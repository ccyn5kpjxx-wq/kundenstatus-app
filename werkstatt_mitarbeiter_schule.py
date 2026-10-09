"""Own school absences, separate from holiday balances and actual time stamps.

Every write rechecks the personal session inside the shared originals lock.
The employee reports school dates; this is neither a holiday approval
nor an attendance stamp. No dates are inferred and no external message is sent.
"""
from contextlib import contextmanager
from datetime import date, datetime
import hmac
import hashlib
import json
import re
import secrets
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, flash, redirect, request, session, url_for

TABLES = ('mitarbeiter_schulabwesenheiten', 'mitarbeiter_schule_audit')
_KEY = re.compile(r'[a-f0-9]{32}')


def init_school_schema(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS mitarbeiter_schulabwesenheiten (
        id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL, von TEXT NOT NULL, bis TEXT NOT NULL,
        ganztag INTEGER NOT NULL, start_zeit TEXT NOT NULL DEFAULT '',
        end_zeit TEXT NOT NULL DEFAULT '', notiz TEXT NOT NULL DEFAULT '', quelle TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'gemeldet', version INTEGER NOT NULL DEFAULT 1,
        erstellt_am TEXT NOT NULL, zurueckgezogen_am TEXT NOT NULL DEFAULT '');
        CREATE INDEX IF NOT EXISTS idx_schule_owner_date
        ON mitarbeiter_schulabwesenheiten(mitarbeiter_id,von,bis);
        CREATE TABLE IF NOT EXISTS mitarbeiter_schule_audit (
        id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL,
        abwesenheit_id TEXT NOT NULL, actor TEXT NOT NULL,
        aktion TEXT NOT NULL, details TEXT NOT NULL, zeit TEXT NOT NULL);''')


def _now():
    return datetime.now(ZoneInfo('Europe/Berlin')).isoformat()


def _year(value):
    if not re.fullmatch(r'20\d{2}', str(value)):
        raise ValueError('Jahr zwischen 2000 und 2099 angeben.')
    return int(value)


def _fields(payload):
    start_day = payload.get('von', '')
    end_day = payload.get('bis') or start_day
    dates = []
    for raw in (start_day, end_day):
        try:
            day = date.fromisoformat(raw)
        except (ValueError, TypeError):
            raise ValueError('Ein gültiges Schuldatum im Format JJJJ-MM-TT angeben.') from None
        if day.isoformat() != raw or not 2000 <= day.year <= 2099:
            raise ValueError('Ein gültiges Schuldatum zwischen 2000 und 2099 angeben.')
        dates.append(day)
    if dates[1] < dates[0]:
        raise ValueError('Bis darf nicht vor Von liegen.')
    if (dates[1] - dates[0]).days > 366:
        raise ValueError('Schulzeiträume mit höchstens einem Jahr angeben.')
    if payload.get('ganztag', '') not in ('', 'ja'):
        raise ValueError('Ganztägig ausdrücklich auswählen oder beide Uhrzeiten angeben.')
    whole = payload.get('ganztag') == 'ja'
    start, end = payload.get('start_zeit', ''), payload.get('end_zeit', '')
    if whole:
        if start or end:
            raise ValueError('Ganztägig oder einen Zeitraum mit Uhrzeiten auswählen.')
    else:
        pattern = r'(?:[01]\d|2[0-3]):[0-5]\d'
        if not isinstance(start, str) or not isinstance(end, str) or not re.fullmatch(pattern, start) or not re.fullmatch(pattern, end) or end <= start:
            raise ValueError('Beide Uhrzeiten im Format HH:MM angeben; das Ende muss nach dem Beginn liegen.')
    note = payload.get('notiz', '')
    if not isinstance(note, str) or len(note.strip()) > 300 or any(ord(c) < 32 and c not in '\n\r\t' for c in note):
        raise ValueError('Die Schulnotiz darf höchstens 300 Zeichen enthalten.')
    source = payload.get('quelle', '')
    if not isinstance(source, str) or len(source.strip()) > 300 or any(ord(c) < 32 for c in source):
        raise ValueError('Die Quellenangabe darf höchstens 300 Zeichen enthalten.')
    return dict(von=start_day, bis=end_day, ganztag=int(whole), start_zeit=start, end_zeit=end, notiz=note.strip(), quelle=source.strip())


class EmployeeSchool:
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
            init_school_schema(db)

    def _identity(self, db, expected=None, *, lock=False):
        # The mid, rights version and auth version always come from the session.
        # Lock the employee and rights rows before rechecking a write on PG.
        mid = session.get('assistent_mid')
        if lock and type(mid) is int and mid > 0:
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (mid,))
            db.execute('UPDATE assistent_rechte SET version=version WHERE mitarbeiter_id=?', (mid,))
        who = self.p.employee_portal.identity(db)
        if not who or session.get('admin') or (expected is not None and expected.get('actor') != who['actor']):
            raise PermissionError('Mit deinem aktuellen persönlichen Mitarbeiterzugang anmelden.')
        return who

    def _admin_identity(self, db, mid):
        if not session.get('admin'):
            raise PermissionError('Nur die Werkstattleitung darf fremde Schulpläne pflegen.')
        if type(mid) is not int or not 1 <= mid <= 2147483647:
            raise ValueError('Aktiven Mitarbeiter auswählen.')
        if db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=? AND aktiv=1', (mid,)).rowcount != 1:
            raise ValueError('Aktiven Mitarbeiter auswählen.')
        return dict(actor='admin', mitarbeiter_id=mid)

    @staticmethod
    def _view(row):
        value = dict(row)
        return {key: value[key] for key in ('id', 'von', 'bis', 'ganztag', 'start_zeit', 'end_zeit', 'notiz', 'quelle', 'status', 'version')}

    def personal(self, who, year):
        year = _year(year)
        with self.db() as db:
            current = self._identity(db, who)
            rows = db.execute('SELECT * FROM mitarbeiter_schulabwesenheiten WHERE mitarbeiter_id=? AND bis>=? AND von<? ORDER BY von,id',
                              (current['mitarbeiter_id'], f'{year}-01-01', f'{year + 1}-01-01')).fetchall()
        return {'enabled': True, 'eintraege': [self._view(row) for row in rows], 'request_id': secrets.token_hex(16)}

    def admin_rows(self, who, year):
        if not isinstance(who, dict) or who.get('actor') != 'admin':
            raise PermissionError('Nur die Werkstattleitung darf die Gesamtübersicht öffnen.')
        year = _year(year)
        with self.db() as db:
            rows = db.execute('SELECT * FROM mitarbeiter_schulabwesenheiten WHERE bis>=? AND von<? ORDER BY von,mitarbeiter_id,id',
                              (f'{year}-01-01', f'{year + 1}-01-01')).fetchall()
        return [dict(self._view(row), mitarbeiter_id=row['mitarbeiter_id']) for row in rows]

    def _audit(self, db, who, key, action):
        record = dict(db.execute('SELECT * FROM mitarbeiter_schulabwesenheiten WHERE id=?', (key,)).fetchone())
        # The immutable reference binds source and exact plan without copying
        # the optional employee note into audit logs.
        details = dict(quelle=record['quelle'], version=record['version'],
                       plan_sha256=hashlib.sha256(json.dumps(record, sort_keys=True, ensure_ascii=False).encode()).hexdigest())
        db.execute('INSERT INTO mitarbeiter_schule_audit(id,mitarbeiter_id,abwesenheit_id,actor,aktion,details,zeit) VALUES(?,?,?,?,?,?,?)',
                   (secrets.token_hex(16), who['mitarbeiter_id'], key, who['actor'], action,
                    json.dumps(details, ensure_ascii=False, sort_keys=True), _now()))

    def _backup(self):
        hook = getattr(self.p, 'schedule_change_backup', None)
        if callable(hook):
            hook('mitarbeiter-schule')

    def create(self, payload):
        return self._create(payload)

    def create_for_admin(self, mid, payload):
        return self._create(payload, admin_mid=mid)

    def _create(self, payload, *, admin_mid=None):
        fields = _fields(payload)
        key = payload.get('request_id')
        if not isinstance(key, str) or not _KEY.fullmatch(key):
            raise ValueError('Formular neu öffnen und den Schultermin erneut prüfen.')
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self._identity(db, lock=True) if admin_mid is None else self._admin_identity(db, admin_mid)
            previous = db.execute('SELECT * FROM mitarbeiter_schulabwesenheiten WHERE id=?', (key,)).fetchone()
            if previous:
                if previous['mitarbeiter_id'] != who['mitarbeiter_id'] or previous['status'] != 'gemeldet' or any(previous[k] != v for k, v in fields.items()):
                    raise ValueError('Dieses Formular wurde bereits mit anderen Angaben verwendet. Bitte neu öffnen.')
                return self._view(previous)
            # Reopening the form must not duplicate an identical active plan.
            same = db.execute('''SELECT * FROM mitarbeiter_schulabwesenheiten
                WHERE mitarbeiter_id=? AND status='gemeldet' AND von=? AND bis=?
                AND ganztag=? AND start_zeit=? AND end_zeit=? AND notiz=? AND quelle=?''',
                (who['mitarbeiter_id'], *(fields[k] for k in ('von', 'bis', 'ganztag', 'start_zeit', 'end_zeit', 'notiz', 'quelle')))).fetchone()
            if same:
                return self._view(same)
            overlapping = db.execute('''SELECT ganztag,start_zeit,end_zeit FROM mitarbeiter_schulabwesenheiten
                WHERE mitarbeiter_id=? AND status='gemeldet' AND bis>=? AND von<=?''',
                (who['mitarbeiter_id'], fields['von'], fields['bis'])).fetchall()
            if any(fields['ganztag'] or row['ganztag'] or
                   (fields['start_zeit'] < row['end_zeit'] and fields['end_zeit'] > row['start_zeit'])
                   for row in overlapping):
                raise ValueError('Für diesen Zeitraum besteht bereits eine überlappende Schulmeldung. Zuerst den vorhandenen Eintrag prüfen oder zurückziehen.')
            db.execute('''INSERT INTO mitarbeiter_schulabwesenheiten
                (id,mitarbeiter_id,von,bis,ganztag,start_zeit,end_zeit,notiz,quelle,erstellt_am)
                VALUES(?,?,?,?,?,?,?,?,?,?)''',
                       (key, who['mitarbeiter_id'], *(fields[k] for k in ('von', 'bis', 'ganztag', 'start_zeit', 'end_zeit', 'notiz', 'quelle')), _now()))
            self._audit(db, who, key, 'schule_gemeldet')
            result = db.execute('SELECT * FROM mitarbeiter_schulabwesenheiten WHERE id=?', (key,)).fetchone()
        self._backup()
        return self._view(result)

    def withdraw(self, key, version):
        return self._withdraw(key, version)

    def withdraw_for_admin(self, mid, key, version):
        return self._withdraw(key, version, admin_mid=mid)

    def _withdraw(self, key, version, *, admin_mid=None):
        if not isinstance(key, str) or not _KEY.fullmatch(key):
            raise ValueError('Eigene Schulmeldung nicht gefunden.')
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self._identity(db, lock=True) if admin_mid is None else self._admin_identity(db, admin_mid)
            row = db.execute('SELECT * FROM mitarbeiter_schulabwesenheiten WHERE id=? AND mitarbeiter_id=?', (key, who['mitarbeiter_id'])).fetchone()
            if not row:
                raise PermissionError('Eigene Schulmeldung nicht gefunden.')
            if row['status'] != 'gemeldet' or str(row['version']) != str(version):
                raise ValueError('Diese Schulmeldung wurde bereits geändert. Bitte neu laden.')
            db.execute("UPDATE mitarbeiter_schulabwesenheiten SET status='zurueckgezogen',version=version+1,zurueckgezogen_am=? WHERE id=? AND mitarbeiter_id=? AND version=?",
                       (_now(), key, who['mitarbeiter_id'], row['version']))
            self._audit(db, who, key, 'schule_zurueckgezogen')
        self._backup()


def register_school(portal):
    service = EmployeeSchool(portal)
    portal.employee_school = service
    portal.employee_school_init_schema = service.init_schema
    bp = Blueprint('employee_school', __name__)

    def csrf():
        expected, supplied = session.get('csrf_token'), request.form.getlist('csrf_token')
        if not expected or not supplied or any(not hmac.compare_digest(str(expected), str(value)) for value in supplied):
            abort(400)

    def submit(action, *, admin_mid=None):
        csrf()
        if request.form.get('confirmed') != 'ja':
            abort(400)
        try:
            action()
            flash('Schulabwesenheit gespeichert. Sie verändert weder Urlaubstage noch Zeitstempel.', 'success')
        except PermissionError:
            abort(403)
        except ValueError as exc:
            flash(str(exc), 'warning')
        endpoint = 'assistent.vacation_admin' if admin_mid is not None else 'assistent.vacation_page'
        target = url_for(endpoint)
        if admin_mid is not None:
            target += '#mitarbeiter-' + str(admin_mid)
        return redirect(target, code=303)

    create_fields = {'csrf_token', 'confirmed', 'von', 'bis', 'ganztag', 'start_zeit', 'end_zeit', 'notiz', 'quelle', 'request_id'}

    def form_fields(allowed):
        # app.add_csrf_fields injects a second identical token into explicit
        # template forms. All CSRF copies must match; business fields stay single.
        if set(request.form) - allowed or any(len(request.form.getlist(key)) != 1 for key in request.form if key != 'csrf_token'):
            abort(400)

    @bp.post('/werkstatt/mein-konto/abwesenheiten/schule')
    def create():
        form_fields(create_fields)
        return submit(lambda: service.create(dict(request.form)))

    @bp.post('/werkstatt/mein-konto/abwesenheiten/schule/<key>/zurueckziehen')
    def withdraw(key):
        form_fields({'csrf_token', 'confirmed', 'version'})
        return submit(lambda: service.withdraw(key, request.form.get('version')))

    @bp.post('/admin/mitarbeiter/<int:mid>/schule')
    @portal.admin_required
    def admin_create(mid):
        form_fields(create_fields)
        return submit(lambda: service.create_for_admin(mid, dict(request.form)), admin_mid=mid)

    @bp.post('/admin/mitarbeiter/<int:mid>/schule/<key>/zurueckziehen')
    @portal.admin_required
    def admin_withdraw(mid, key):
        form_fields({'csrf_token', 'confirmed', 'version'})
        return submit(lambda: service.withdraw_for_admin(mid, key, request.form.get('version')), admin_mid=mid)

    @bp.after_request
    def privacy(response):
        response.headers['Cache-Control'] = 'private, no-store'
        response.headers['Referrer-Policy'] = 'no-referrer'
        return response

    portal.app.register_blueprint(bp)
    return service
