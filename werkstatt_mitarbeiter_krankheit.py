"""Private illness periods with owned originals, without diagnoses or identifiers.

Reported illness dates are health data and remain access restricted. This
records dates and an optional employer-copy reference, no diagnosis or medical
identifier. It does
not retrieve eAU, decide pay, debit holiday balances or create attendance stamps.
Every write rechecks ownership inside the shared destructive-restore lock.
"""
from contextlib import contextmanager
from datetime import date, datetime
import base64
import hashlib
import hmac
import json
import re
import secrets
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, flash, redirect, render_template, request, session

TABLES = ('mitarbeiter_krankmeldungen', 'mitarbeiter_krankheit_audit')
TYPES = {'krank': 'Krankmeldung', 'erst': 'Erstbescheinigung', 'folge': 'Folgebescheinigung'}
SOURCES = {'': 'Nicht angegeben', 'eigene_meldung': 'Eigene Krankmeldung',
           'arbeitgeber_au': 'AU-Arbeitgeberexemplar vorliegend'}
_KEY = re.compile(r'[a-f0-9]{32}')
_ORIGINAL_MIMES = {'application/pdf', 'image/png', 'image/jpeg'}
_MAX_ORIGINAL_BYTES = 10 * 1024 * 1024


def init_illness_schema(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS mitarbeiter_krankmeldungen (
        id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL,
        von TEXT NOT NULL, bis TEXT NOT NULL, typ TEXT NOT NULL DEFAULT 'krank',
        quelle TEXT NOT NULL DEFAULT '', original_id INTEGER,
        original_sha256 TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'gemeldet',
        version INTEGER NOT NULL DEFAULT 1, erstellt_am TEXT NOT NULL,
        zurueckgezogen_am TEXT NOT NULL DEFAULT '');
        CREATE INDEX IF NOT EXISTS idx_krankheit_owner_date
        ON mitarbeiter_krankmeldungen(mitarbeiter_id,von,bis);
        CREATE TABLE IF NOT EXISTS mitarbeiter_krankheit_audit (
        id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL, meldung_id TEXT NOT NULL,
        actor TEXT NOT NULL, aktion TEXT NOT NULL, details TEXT NOT NULL, zeit TEXT NOT NULL);''')


def _now():
    return datetime.now(ZoneInfo('Europe/Berlin')).isoformat()


def _fields(payload):
    allowed = {'von', 'bis', 'typ', 'quelle', 'original_id', 'request_id', 'csrf_token', 'confirmed'}
    if not isinstance(payload, dict) or set(payload) - allowed:
        raise ValueError('Nur Zeitraum, Bescheinigungsart und private Originalreferenz angeben.')
    start, end = payload.get('von', ''), payload.get('bis') or payload.get('von', '')
    dates = []
    for raw in (start, end):
        try:
            day = date.fromisoformat(raw)
        except (TypeError, ValueError):
            raise ValueError('Gültiges Datum im Format JJJJ-MM-TT angeben.') from None
        if day.isoformat() != raw or not 2000 <= day.year <= 2099:
            raise ValueError('Gültiges Datum zwischen 2000 und 2099 angeben.')
        dates.append(day)
    if dates[1] < dates[0] or (dates[1] - dates[0]).days > 366:
        raise ValueError('Bis muss nach oder an Von liegen; höchstens ein Jahr je Zeitraum angeben.')
    typ, source = payload.get('typ', 'krank'), payload.get('quelle', '')
    if not isinstance(typ, str) or not isinstance(source, str) or typ not in TYPES or source not in SOURCES:
        raise ValueError('Vorgesehene Bescheinigungsart und Quellenart auswählen.')
    original = payload.get('original_id', '')
    if original in ('', None):
        original = None
    elif not isinstance(original, str) or not re.fullmatch(r'[1-9][0-9]{0,9}', original) or int(original) > 2147483647:
        raise ValueError('Vorhandenes eigenes AU-Original auswählen.')
    else:
        original = int(original)
    if original is not None and source != 'arbeitgeber_au':
        raise ValueError('Ein verknüpftes AU-Original als vorliegendes Arbeitgeberexemplar kennzeichnen.')
    return dict(von=start, bis=end, typ=typ, quelle=source, original_id=original)


class EmployeeIllness:
    def __init__(self, portal):
        self.p = portal
        self.init_schema()

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def init_schema(self):
        with self.db() as db:
            init_illness_schema(db)

    def _personal(self, db, *, lock=False):
        mid = session.get('assistent_mid')
        if lock and type(mid) is int and mid > 0:
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (mid,))
            db.execute('UPDATE assistent_rechte SET version=version WHERE mitarbeiter_id=?', (mid,))
        who = self.p.employee_portal.identity(db)
        if not who or session.get('admin'):
            raise PermissionError('Mit dem aktuellen persönlichen Mitarbeiterzugang anmelden.')
        return who

    def _admin_owner(self, db, mid, *, lock=False):
        if not session.get('admin'):
            raise PermissionError('Nur die Werkstattleitung darf fremde Krankmeldungen pflegen.')
        if type(mid) is not int or not 1 <= mid <= 2147483647:
            raise ValueError('Mitarbeiter auswählen.')
        if lock:
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (mid,))
        employee = db.execute('SELECT id,name,aktiv FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
        if not employee:
            raise ValueError('Mitarbeiter auswählen.')
        return dict(actor='admin', mitarbeiter_id=mid, aktiv=employee['aktiv'])

    @staticmethod
    def _original(db, mid, original_id, expected_hash=None):
        if original_id is None:
            return ''
        row = db.execute('''SELECT id,sha256,mime,size_bytes,original_base64 FROM mitarbeiter_arbeitsvertraege
            WHERE id=? AND mitarbeiter_id=?''', (original_id, mid)).fetchone()
        if (not row or row['mime'] not in _ORIGINAL_MIMES or not re.fullmatch(r'[a-f0-9]{64}', row['sha256'] or '')
                or (expected_hash is not None and row['sha256'] != expected_hash)):
            raise ValueError('Das AU-Original gehört nicht unverändert zu diesem Mitarbeiter.')
        try:
            raw = base64.b64decode(row['original_base64'], validate=True)
            if (not 0 < len(raw) <= _MAX_ORIGINAL_BYTES or len(raw) != row['size_bytes']
                    or hashlib.sha256(raw).hexdigest() != row['sha256']):
                raise ValueError()
        except (ValueError, TypeError):
            raise ValueError('Das AU-Original gehört nicht unverändert zu diesem Mitarbeiter.') from None
        return row['sha256']

    def _private_view(self, db, row, *, admin=False):
        item = {key: row[key] for key in ('id', 'von', 'bis', 'typ', 'quelle', 'status', 'version', 'erstellt_am')}
        item.update(typ_label=TYPES.get(item['typ'], 'Bescheinigungsart prüfen'),
                    quelle_label=SOURCES.get(item['quelle'], 'Quellenart prüfen'),
                    status_label='Zurückgezogen' if item['status'] == 'zurueckgezogen' else 'Erfasst',
                    original_id=None, original_url='', original_status='')
        if row['original_id'] is not None:
            try:
                self._original(db, row['mitarbeiter_id'], row['original_id'], row['original_sha256'])
            except ValueError:
                item['original_status'] = 'Originalzuordnung prüfen'
            else:
                prefix = (f'/admin/mitarbeiter/{row["mitarbeiter_id"]}/portal/arbeitsvertrag/'
                          if admin else '/werkstatt/mein-konto/arbeitsvertrag/')
                item.update(original_id=row['original_id'], original_url=prefix + str(row['original_id']))
        return item

    def _entries(self, db, mid, *, admin=False):
        rows = db.execute('SELECT * FROM mitarbeiter_krankmeldungen WHERE mitarbeiter_id=? ORDER BY von DESC,erstellt_am DESC,id', (mid,)).fetchall()
        return [self._private_view(db, row, admin=admin) for row in rows]

    def personal(self, who):
        with self.db() as db:
            current = self._personal(db)
            if not isinstance(who, dict) or who.get('actor') != current['actor']:
                raise PermissionError('Eigenen Mitarbeiterzugang verwenden.')
            return dict(enabled=True, eintraege=self._entries(db, current['mitarbeiter_id']),
                        request_id=secrets.token_hex(16), originale=[])

    def admin_profile(self, mid):
        with self.db() as db:
            who = self._admin_owner(db, mid)
            originals = [dict(row) for row in db.execute('''SELECT id,titel,filename FROM mitarbeiter_arbeitsvertraege
                WHERE mitarbeiter_id=? AND mime IN (?,?,?) ORDER BY id DESC''',
                (mid, 'application/pdf', 'image/png', 'image/jpeg')).fetchall()]
            return dict(enabled=bool(who['aktiv']), eintraege=self._entries(db, mid, admin=True),
                        request_id=secrets.token_hex(16), originale=originals)

    def _audit(self, db, who, key, action):
        record = dict(db.execute('SELECT * FROM mitarbeiter_krankmeldungen WHERE id=?', (key,)).fetchone())
        details = dict(version=record['version'], record_sha256=hashlib.sha256(
            json.dumps(record, ensure_ascii=False, sort_keys=True).encode()).hexdigest())
        db.execute('INSERT INTO mitarbeiter_krankheit_audit(id,mitarbeiter_id,meldung_id,actor,aktion,details,zeit) VALUES(?,?,?,?,?,?,?)',
                   (secrets.token_hex(16), who['mitarbeiter_id'], key, who['actor'], action,
                    json.dumps(details, sort_keys=True), _now()))

    def _backup(self):
        hook = getattr(self.p, 'schedule_change_backup', None)
        if callable(hook):
            hook('mitarbeiter-krankheit')

    def create(self, payload, *, admin_mid=None):
        fields = _fields(payload)
        key = payload.get('request_id')
        if not isinstance(key, str) or not _KEY.fullmatch(key):
            raise ValueError('Formular neu öffnen und Krankmeldungszeitraum erneut prüfen.')
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self._personal(db, lock=True) if admin_mid is None else self._admin_owner(db, admin_mid, lock=True)
            if admin_mid is not None and not who['aktiv']:
                raise ValueError('Neue Krankmeldung nur für einen aktiven Mitarbeiter erfassen.')
            fields['original_sha256'] = self._original(db, who['mitarbeiter_id'], fields['original_id'])
            old = db.execute('SELECT * FROM mitarbeiter_krankmeldungen WHERE id=?', (key,)).fetchone()
            if old:
                if old['mitarbeiter_id'] != who['mitarbeiter_id'] or old['status'] != 'gemeldet' or any(old[k] != value for k, value in fields.items()):
                    raise ValueError('Diese Formularbestätigung wurde bereits mit anderen Angaben verwendet.')
                return self._private_view(db, old, admin=admin_mid is not None)
            existing = db.execute("SELECT * FROM mitarbeiter_krankmeldungen WHERE mitarbeiter_id=? AND status='gemeldet' AND bis>=? AND von<=?",
                                  (who['mitarbeiter_id'], fields['von'], fields['bis'])).fetchall()
            for row in existing:
                if all(row[k] == value for k, value in fields.items()):
                    return self._private_view(db, row, admin=admin_mid is not None)
            if existing:
                raise ValueError('Für diese Tage besteht bereits eine Krankmeldung. Vorhandenen Zeitraum prüfen oder zurückziehen.')
            db.execute('''INSERT INTO mitarbeiter_krankmeldungen
                (id,mitarbeiter_id,von,bis,typ,quelle,original_id,original_sha256,erstellt_am)
                VALUES(?,?,?,?,?,?,?,?,?)''',
                (key, who['mitarbeiter_id'], *(fields[k] for k in ('von', 'bis', 'typ', 'quelle', 'original_id', 'original_sha256')), _now()))
            self._audit(db, who, key, 'krankheit_erfasst')
            result = self._private_view(db, db.execute('SELECT * FROM mitarbeiter_krankmeldungen WHERE id=?', (key,)).fetchone(), admin=admin_mid is not None)
        self._backup()
        return result

    def withdraw(self, key, version, *, admin_mid=None):
        if not isinstance(key, str) or not _KEY.fullmatch(key):
            raise ValueError('Krankmeldung nicht gefunden.')
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self._personal(db, lock=True) if admin_mid is None else self._admin_owner(db, admin_mid, lock=True)
            row = db.execute('SELECT * FROM mitarbeiter_krankmeldungen WHERE id=? AND mitarbeiter_id=?', (key, who['mitarbeiter_id'])).fetchone()
            if not row:
                raise PermissionError('Eigene Krankmeldung nicht gefunden.')
            if row['status'] != 'gemeldet' or str(row['version']) != str(version):
                raise ValueError('Die Krankmeldung wurde bereits geändert. Bitte neu laden.')
            changed = db.execute("UPDATE mitarbeiter_krankmeldungen SET status='zurueckgezogen',version=version+1,zurueckgezogen_am=? WHERE id=? AND mitarbeiter_id=? AND status='gemeldet' AND version=?",
                                 (_now(), key, who['mitarbeiter_id'], row['version'])).rowcount
            if changed != 1:
                raise ValueError('Die Krankmeldung wurde bereits geändert. Bitte neu laden.')
            self._audit(db, who, key, 'krankheit_zurueckgezogen')
        self._backup()

    @staticmethod
    def _chef_row(row, today):
        try:
            start, end = date.fromisoformat(row['von']), date.fromisoformat(row['bis'])
            if (start.isoformat() != row['von'] or end.isoformat() != row['bis']
                    or not 2000 <= start.year <= end.year <= 2099 or end < start
                    or (end - start).days > 366):
                raise ValueError()
            day_label = start.strftime('%d.%m.%Y') if start == end else start.strftime('%d.%m.%Y') + ' – ' + end.strftime('%d.%m.%Y')
        except (TypeError, ValueError):
            start = end = None
            day_label = 'Zeitraum prüfen'
        if start is None or row['status'] not in ('gemeldet', 'zurueckgezogen'):
            key, label = 'pruefen', 'Krankmeldung prüfen'
        elif row['status'] == 'zurueckgezogen':
            key, label = 'zurueckgezogen', 'Zurückgezogen'
        elif end < today:
            key, label = 'vergangen', 'Zeitraum beendet'
        elif not row['aktiv']:
            key, label = 'inaktiv', 'Zeitraum bei inaktivem Mitarbeiter'
        elif start > today:
            key, label = 'geplant', 'Krankmeldung für späteren Zeitraum'
        else:
            key, label = 'aktuell', 'Aktuell krankgemeldet'
        return dict(id=row['id'], mitarbeiter_id=row['mitarbeiter_id'], name=row['name'],
                    datum_label=day_label, status_key=key, status_label=label,
                    url=f'/admin/mitarbeiter/{row["mitarbeiter_id"]}/portal#krankmeldungen')

    def admin_history(self, who, today=None):
        if not isinstance(who, dict) or who.get('actor') != 'admin':
            raise PermissionError('Nur die Werkstattleitung darf die Krankmeldungsübersicht öffnen.')
        today = today or datetime.now(ZoneInfo('Europe/Berlin')).date()
        with self.db() as db:
            rows = db.execute('''SELECT k.id,k.mitarbeiter_id,k.von,k.bis,k.status,k.erstellt_am,m.name,m.aktiv
                FROM mitarbeiter_krankmeldungen k JOIN mitarbeiter m ON m.id=k.mitarbeiter_id
                ORDER BY k.erstellt_am DESC,k.id DESC''').fetchall()
        return [self._chef_row(row, today) for row in rows]

    def admin_briefing(self, who, today):
        if not isinstance(who, dict) or who.get('actor') != 'admin':
            raise PermissionError('Nur die Werkstattleitung darf die Krankmeldungsübersicht öffnen.')
        with self.db() as db:
            base = '''SELECT k.id,k.mitarbeiter_id,k.von,k.bis,k.status,k.erstellt_am,m.name,m.aktiv
                FROM mitarbeiter_krankmeldungen k JOIN mitarbeiter m ON m.id=k.mitarbeiter_id'''
            current = db.execute(base + " WHERE k.status='gemeldet' AND m.aktiv=1 AND k.von<=? AND k.bis>=? ORDER BY m.name,k.id",
                                 (today.isoformat(), today.isoformat())).fetchall()
            recent = db.execute(base + ' ORDER BY k.erstellt_am DESC,k.id DESC LIMIT 8').fetchall()
        keys = {row['id'] for row in current}
        return dict(rows=[self._chef_row(row, today) for row in current] +
                    [self._chef_row(row, today) for row in recent if row['id'] not in keys],
                    error='', history_url='/admin/mitarbeiter/krankmeldungen')


def register_illness(portal):
    service = EmployeeIllness(portal)
    portal.employee_illness = service
    portal.employee_illness_init_schema = service.init_schema
    bp = Blueprint('employee_illness', __name__)

    def submit(action, *, admin_mid=None):
        expected, supplied = session.get('csrf_token'), request.form.getlist('csrf_token')
        if not expected or not supplied or any(not hmac.compare_digest(str(expected), str(token)) for token in supplied):
            abort(400)
        if request.form.get('confirmed') != 'ja':
            abort(400)
        try:
            action()
            flash('Krankmeldung gespeichert. Urlaub und Zeitstempel bleiben unverändert.', 'success')
        except PermissionError:
            abort(403)
        except ValueError as exc:
            flash(str(exc), 'warning')
        target = f'/admin/mitarbeiter/{admin_mid}/portal' if admin_mid is not None else '/werkstatt/mein-konto'
        return redirect(target + '#krankmeldungen', code=303)

    def allowed(fields):
        if request.files or set(request.form) - fields or any(len(request.form.getlist(key)) != 1 for key in request.form if key != 'csrf_token'):
            abort(400)

    create_fields = {'csrf_token', 'confirmed', 'von', 'bis', 'typ', 'quelle', 'original_id', 'request_id'}

    @bp.post('/werkstatt/mein-konto/abwesenheiten/krankheit')
    def create():
        allowed(create_fields)
        return submit(lambda: service.create(dict(request.form)))

    @bp.post('/werkstatt/mein-konto/abwesenheiten/krankheit/<key>/zurueckziehen')
    def withdraw(key):
        allowed({'csrf_token', 'confirmed', 'version'})
        return submit(lambda: service.withdraw(key, request.form.get('version')))

    @bp.post('/admin/mitarbeiter/<int:mid>/krankheit')
    @portal.admin_required
    def admin_create(mid):
        allowed(create_fields)
        return submit(lambda: service.create(dict(request.form), admin_mid=mid), admin_mid=mid)

    @bp.post('/admin/mitarbeiter/<int:mid>/krankheit/<key>/zurueckziehen')
    @portal.admin_required
    def admin_withdraw(mid, key):
        allowed({'csrf_token', 'confirmed', 'version'})
        return submit(lambda: service.withdraw(key, request.form.get('version'), admin_mid=mid), admin_mid=mid)

    @bp.get('/admin/mitarbeiter/krankmeldungen')
    @portal.admin_required
    def history():
        return render_template('mitarbeiter_krankmeldungen_admin.html', data=dict(
            rows=service.admin_history({'actor': 'admin'}), error='', history_url='/admin/mitarbeiter/krankmeldungen'))

    @bp.after_request
    def privacy(response):
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive'
        response.vary.add('Cookie')
        return response

    portal.app.register_blueprint(bp)
    return service
