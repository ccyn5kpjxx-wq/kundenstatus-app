"""Personal leave requests and explicitly reviewed annual balances.

Integrate with register_selfservice(portal, assistant_blueprint, protected).
The caller supplies the existing authenticated identity, never a model-provided
employee id. Add the returned service's init_schema to restore hooks and all
three TABLES to backup/export. No entitlement is inferred from legacy text.
Only an admin's explicit review can approve a request. Approval and the existing
calendar entry commit together. No mail, background task or personnel preload.
"""
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import re
import secrets
from zoneinfo import ZoneInfo

from flask import abort, flash, jsonify, redirect, render_template, request, session, url_for

TABLES = ('mitarbeiter_urlaubskonten', 'mitarbeiter_urlaubsantraege', 'mitarbeiter_urlaub_audit')
WEEKDAYS = ('Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag')
STATES = {'beantragt': 'Beantragt', 'genehmigt': 'Genehmigt', 'abgelehnt': 'Abgelehnt',
          'zurueckgezogen': 'Zurückgezogen'}


def _today():
    return datetime.now(ZoneInfo('Europe/Berlin')).date()


def _now():
    return datetime.now(ZoneInfo('Europe/Berlin')).isoformat()


def _date(value):
    if not isinstance(value, str):
        raise ValueError('Datum im Format JJJJ-MM-TT angeben.')
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise ValueError('Datum im Format JJJJ-MM-TT angeben.') from None
    if result.isoformat() != value:
        raise ValueError('Datum im Format JJJJ-MM-TT angeben.')
    return result


def _year(value):
    if isinstance(value, bool) or not re.fullmatch(r'20\d{2}', str(value)):
        raise ValueError('Urlaubsjahr zwischen 2000 und 2099 angeben.')
    return int(value)


def _days(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError('Geprüfte Resttage als Zahl in halben Tagen angeben.')
    raw = str(value).replace(',', '.')
    if not re.fullmatch(r'\d{1,3}(?:\.\d)?', raw):
        raise ValueError('Geprüfte Resttage als Zahl in halben Tagen angeben.')
    try:
        number = Decimal(raw)
    except InvalidOperation:
        raise ValueError('Resttage ungültig.') from None
    if not 0 <= number <= 366 or number * 2 != (number * 2).to_integral_value():
        raise ValueError('Geprüfte Resttage zwischen 0 und 366 in halben Tagen angeben.')
    return int(number * 2)


def _display(half_days):
    return str(half_days // 2) if half_days % 2 == 0 else str(half_days // 2) + ',5'


class EmployeeSelfService:
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
            db.executescript('''
                CREATE TABLE IF NOT EXISTS mitarbeiter_urlaubskonten (
                  mitarbeiter_id INTEGER NOT NULL, jahr INTEGER NOT NULL,
                  rest_halbtage INTEGER NOT NULL, stichtag TEXT NOT NULL,
                  arbeitstage_json TEXT NOT NULL, feiertage TEXT NOT NULL,
                  basis_json TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
                  geprueft_von TEXT NOT NULL, geprueft_am TEXT NOT NULL,
                  PRIMARY KEY(mitarbeiter_id,jahr));
                CREATE TABLE IF NOT EXISTS mitarbeiter_urlaubsantraege (
                  id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL, actor TEXT NOT NULL,
                  request_id TEXT NOT NULL, start_datum TEXT NOT NULL, end_datum TEXT NOT NULL,
                  jahr INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'beantragt',
                  belastung_halbtage INTEGER, kalender_id INTEGER,
                  erstellt_am TEXT NOT NULL, entschieden_am TEXT NOT NULL DEFAULT '',
                  entschieden_von TEXT NOT NULL DEFAULT '', version INTEGER NOT NULL DEFAULT 1,
                  UNIQUE(actor,request_id));
                CREATE TABLE IF NOT EXISTS mitarbeiter_urlaub_audit (
                  id TEXT PRIMARY KEY, mitarbeiter_id INTEGER NOT NULL,
                  actor TEXT NOT NULL, aktion TEXT NOT NULL, referenz TEXT NOT NULL,
                  details TEXT NOT NULL, zeit TEXT NOT NULL);
            ''')

    def _employee(self, db, who):
        who = who if isinstance(who, dict) else {}
        mid = who.get('mitarbeiter_id')
        if type(mid) is not int or mid <= 0 or who.get('actor') != f'mitarbeiter:{mid}' or not who.get('lesen'):
            raise PermissionError('Für den eigenen Urlaub bitte persönlich als Mitarbeiter anmelden.')
        row = db.execute('SELECT id,name,aktiv FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
        if not row or not row['aktiv']:
            raise PermissionError('Persönlicher Mitarbeiterzugang ist nicht aktiv.')
        return dict(row)

    @staticmethod
    def _admin(who):
        if not isinstance(who, dict) or who.get('actor') != 'admin':
            raise PermissionError('Nur die Werkstattleitung darf Urlaub prüfen.')

    def _audit(self, db, mid, actor, action, reference, details):
        db.execute('INSERT INTO mitarbeiter_urlaub_audit(id,mitarbeiter_id,actor,aktion,referenz,details,zeit) VALUES(?,?,?,?,?,?,?)',
                   (secrets.token_hex(16), mid, actor, action, str(reference), json.dumps(details, ensure_ascii=False), _now()))

    def _backup(self):
        hook = getattr(self.p, 'schedule_change_backup', None)
        if callable(hook):
            hook('mitarbeiter-selfservice')

    def _legacy(self, db, mid, year):
        rows = db.execute('SELECT id,start_datum,end_datum,notiz FROM mitarbeiter_urlaub WHERE mitarbeiter_id=? ORDER BY id', (mid,)).fetchall()
        result = {}
        for row in rows:
            # Legacy storage uses DD.MM.YYYY. A malformed interval cannot be
            # quietly omitted from the balance basis.
            try:
                start = datetime.strptime(row['start_datum'], '%d.%m.%Y').date()
                end = datetime.strptime(row['end_datum'] or row['start_datum'], '%d.%m.%Y').date()
                if end < start:
                    raise ValueError()
            except (ValueError, TypeError):
                result[str(row['id'])] = {'ungueltig': True}
                continue
            if start.year <= year <= end.year:
                result[str(row['id'])] = {'von': start.isoformat(), 'bis': end.isoformat(),
                                         'notiz_hash': hashlib.sha256(str(row['notiz'] or '').encode()).hexdigest()}
        return result

    @staticmethod
    def _calendar_note(request_id):
        return 'Persönlicher Urlaubsantrag ' + request_id + ' · durch Werkstattleitung genehmigt'

    def _snapshot(self, db, mid, year):
        approved = db.execute("SELECT id FROM mitarbeiter_urlaubsantraege WHERE mitarbeiter_id=? AND jahr=? AND status='genehmigt' ORDER BY id", (mid, year)).fetchall()
        basis = {'kalender': self._legacy(db, mid, year), 'antraege': [r['id'] for r in approved]}
        token = hashlib.sha256(json.dumps(basis, sort_keys=True).encode()).hexdigest()
        return basis, token

    def _account(self, db, mid, year):
        row = db.execute('SELECT * FROM mitarbeiter_urlaubskonten WHERE mitarbeiter_id=? AND jahr=?', (mid, year)).fetchone()
        if not row:
            return None, None, 'Ein geprüftes Resturlaubskonto für dieses Jahr fehlt. Die Werkstattleitung muss es einrichten.'
        account = dict(row)
        approved = [dict(r) for r in db.execute("SELECT * FROM mitarbeiter_urlaubsantraege WHERE mitarbeiter_id=? AND jahr=? AND status='genehmigt'", (mid, year)).fetchall()]
        try:
            basis = json.loads(account['basis_json'])
            baseline = basis['antraege']
            current = self._legacy(db, mid, year)
            spent = 0
            for item in approved:
                if item['id'] in baseline:
                    continue
                expected = {'von': item['start_datum'], 'bis': item['end_datum'],
                            'notiz_hash': hashlib.sha256(self._calendar_note(item['id']).encode()).hexdigest()}
                if current.pop(str(item['kalender_id']), None) != expected or type(item['belastung_halbtage']) is not int:
                    raise ValueError()
                spent += item['belastung_halbtage']
            if current != basis['kalender'] or not set(baseline).issubset({r['id'] for r in approved}):
                raise ValueError()
            left = account['rest_halbtage'] - spent
            if left < 0:
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            return account, None, 'Urlaubseinträge wurden seit der letzten Kontoprüfung geändert. Resturlaub bis zur Neuprüfung unbekannt.'
        return account, left, ''

    def _range(self, start, end):
        start, end = _date(start), _date(end)
        if end < start:
            raise ValueError('Bis darf nicht vor Von liegen.')
        if start.year != end.year:
            raise ValueError('Urlaub über den Jahreswechsel bitte in zwei Anträge je Urlaubsjahr aufteilen.')
        _year(start.year)
        if start < _today():
            raise ValueError('Neue Urlaubsanträge beginnen frühestens heute. Vergangenes bitte mit der Werkstattleitung klären.')
        return start, end

    def _count(self, account, start, end):
        try:
            days = json.loads(account['arbeitstage_json'])
            if not isinstance(days, list) or not days or any(type(d) is not int or not 0 <= d <= 6 for d in days):
                raise ValueError()
            holidays = self.p.bw_feiertage(start.year) if account['feiertage'] == 'BW' else {}
            if account['feiertage'] not in ('BW', 'keine'):
                raise ValueError()
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ValueError('Arbeitstage oder Feiertagsbasis sind noch nicht verlässlich eingerichtet.') from None
        cursor, count = start, 0
        while cursor <= end:
            if cursor.weekday() in days and cursor not in holidays:
                count += 2
            cursor += timedelta(days=1)
        return count

    def _overlaps(self, db, mid, start, end, ignore=None):
        rows = db.execute("SELECT id,start_datum,end_datum FROM mitarbeiter_urlaubsantraege WHERE mitarbeiter_id=? AND status IN ('beantragt','genehmigt')", (mid,)).fetchall()
        for row in rows:
            if row['id'] != ignore and row['start_datum'] <= end.isoformat() and row['end_datum'] >= start.isoformat():
                return True
        legacy = self._legacy(db, mid, start.year)
        return any('ungueltig' in item or (item['von'] <= end.isoformat() and item['bis'] >= start.isoformat()) for item in legacy.values())

    def _preview(self, db, mid, start, end):
        account, left, need = self._account(db, mid, start.year)
        days = self._count(account, start, end) if account and left is not None else None
        if days == 0:
            raise ValueError('Der Zeitraum enthält laut eingerichteten Arbeitstagen keinen Urlaubstag.')
        return {'von': start.isoformat(), 'bis': end.isoformat(), 'jahr': start.year,
                'tage': _display(days) if days is not None else None,
                'resttage': _display(left) if left is not None else None,
                'konto_version': account['version'] if account else None,
                'bedarf': need, 'saldo_ausreichend': left >= days if left is not None and days is not None else None,
                'status': 'vorschau', 'hinweis': 'Dies ist ein Antrag auf ganze Urlaubstage, noch keine Genehmigung. Beantragte Tage sind nicht vom Resturlaub abgezogen.'}

    def preview(self, who, start, end):
        start, end = self._range(start, end)
        with self.db() as db:
            person = self._employee(db, who)
            if self._overlaps(db, person['id'], start, end):
                raise ValueError('Dieser Zeitraum überschneidet sich mit einem vorhandenen Antrag oder Urlaubseintrag. Bitte den eigenen Verlauf prüfen.')
            return self._preview(db, person['id'], start, end)

    @staticmethod
    def _view(row):
        item = dict(row)
        return {key: item.get(key) for key in ('id', 'start_datum', 'end_datum', 'jahr', 'status', 'erstellt_am', 'entschieden_am', 'version')} | {
            'status_label': STATES.get(item['status'], 'Unbekannt'),
            'tage': _display(item['belastung_halbtage']) if item.get('belastung_halbtage') is not None else None}

    def apply(self, who, start, end, request_id):
        if not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9-]{16,100}', request_id):
            raise ValueError('Eindeutige Antragskennung fehlt.')
        with self.db() as db:
            person = self._employee(db, who)
            # Lock an existing parent row before idempotency and overlap checks.
            # This serializes concurrent requests on SQLite and PostgreSQL.
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (person['id'],))
            previous = db.execute('SELECT * FROM mitarbeiter_urlaubsantraege WHERE actor=? AND request_id=?', (who['actor'], request_id)).fetchone()
            if previous:
                if previous['start_datum'] != start or previous['end_datum'] != end:
                    raise ValueError('Diese Antragskennung gehört bereits zu einem anderen Zeitraum.')
                return self._view(previous)
            start_date, end_date = self._range(start, end)
            if self._overlaps(db, person['id'], start_date, end_date):
                raise ValueError('Dieser Zeitraum überschneidet sich mit einem vorhandenen Antrag oder Urlaubseintrag.')
            self._preview(db, person['id'], start_date, end_date)
            rid = secrets.token_hex(16)
            db.execute('INSERT INTO mitarbeiter_urlaubsantraege(id,mitarbeiter_id,actor,request_id,start_datum,end_datum,jahr,erstellt_am) VALUES(?,?,?,?,?,?,?,?)',
                       (rid, person['id'], who['actor'], request_id, start, end, start_date.year, _now()))
            self._audit(db, person['id'], who['actor'], 'beantragt', rid, {'von': start, 'bis': end})
            row = db.execute('SELECT * FROM mitarbeiter_urlaubsantraege WHERE id=?', (rid,)).fetchone()
        self._backup()
        return self._view(row)

    def summary(self, who, year=None):
        year = _year(year if year is not None else _today().year)
        with self.db() as db:
            person = self._employee(db, who)
            return self._summary(db, person['id'], year)

    def _summary(self, db, mid, year):
        account, left, need = self._account(db, mid, year)
        rows = db.execute('SELECT * FROM mitarbeiter_urlaubsantraege WHERE mitarbeiter_id=? AND jahr=? ORDER BY erstellt_am DESC,id', (mid, year)).fetchall()
        result = {'jahr': year, 'resttage': _display(left) if left is not None else None,
                  'bekannt': left is not None, 'bedarf': need,
                  'antraege': [self._view(row) for row in rows],
                  'hinweis': 'Resturlaub aus dem von der Werkstattleitung geprüften Konto. Bereits im Ausgangsstand berücksichtigter Urlaub wird nicht erneut abgezogen. Offene Anträge sind noch nicht genehmigt.'}
        managed = {str(row['kalender_id']) for row in rows if row['kalender_id'] is not None}
        result['kalendereintraege'] = [{'von': item.get('von'), 'bis': item.get('bis'), 'status': 'Altbestand ohne erfassten Genehmigungsstatus'}
                                      for key, item in self._legacy(db, mid, year).items() if key not in managed]
        if account:
            result.update(stichtag=account['stichtag'], konto_version=account['version'], geprueft_am=account['geprueft_am'])
        return result

    def withdraw(self, who, request_id, version):
        with self.db() as db:
            person = self._employee(db, who)
            changed = db.execute("UPDATE mitarbeiter_urlaubsantraege SET status='zurueckgezogen',version=version+1,entschieden_am=?,entschieden_von=? WHERE id=? AND mitarbeiter_id=? AND status='beantragt' AND version=?",
                                 (_now(), who['actor'], request_id, person['id'], version))
            if changed.rowcount != 1:
                raise ValueError('Nur ein eigener noch offener Antrag kann zurückgezogen werden. Bitte neu laden.')
            self._audit(db, person['id'], who['actor'], 'zurueckgezogen', request_id, {})
        self._backup()

    def save_account(self, who, mid, payload):
        self._admin(who)
        if not isinstance(payload, dict) or payload.get('confirmed') is not True:
            raise ValueError('Reststand und bereits berücksichtigte Urlaube ausdrücklich bestätigen.')
        year, balance, cutoff = _year(payload.get('jahr')), _days(payload.get('resttage')), _date(payload.get('stichtag'))
        if cutoff.year != year or cutoff > _today():
            raise ValueError('Stichtag muss im Urlaubsjahr liegen und darf nicht in der Zukunft liegen.')
        weekdays = payload.get('arbeitstage')
        if not isinstance(weekdays, list) or not weekdays or len(set(weekdays)) != len(weekdays) or any(type(d) is not int or not 0 <= d <= 6 for d in weekdays):
            raise ValueError('Tatsächliche regelmäßige Arbeitstage ausdrücklich auswählen.')
        holiday_basis = payload.get('feiertage')
        if holiday_basis not in ('BW', 'keine'):
            raise ValueError('Feiertagsbasis ausdrücklich festlegen.')
        with self.db() as db:
            person = db.execute('SELECT id,aktiv FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
            if not person or not person['aktiv']:
                raise ValueError('Aktiver Mitarbeiter fehlt.')
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (mid,))
            old = db.execute('SELECT version FROM mitarbeiter_urlaubskonten WHERE mitarbeiter_id=? AND jahr=?', (mid, year)).fetchone()
            if str(payload.get('version', '0')) != str(old['version'] if old else 0):
                raise ValueError('Das Urlaubskonto wurde geändert. Bitte neu laden und prüfen.')
            basis, current_token = self._snapshot(db, mid, year)
            if payload.get('basis_token') != current_token:
                raise ValueError('Urlaubseinträge wurden seit dem Öffnen geändert. Bitte neu laden und den Reststand erneut prüfen.')
            snapshot = basis['kalender']
            if any('ungueltig' in item for item in snapshot.values()):
                raise ValueError('Vorhandene Urlaubszeiträume sind ungültig; zuerst in der Mitarbeiterverwaltung korrigieren.')
            db.execute('''INSERT INTO mitarbeiter_urlaubskonten(mitarbeiter_id,jahr,rest_halbtage,stichtag,arbeitstage_json,feiertage,basis_json,geprueft_von,geprueft_am)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(mitarbeiter_id,jahr) DO UPDATE SET rest_halbtage=excluded.rest_halbtage,
                stichtag=excluded.stichtag,arbeitstage_json=excluded.arbeitstage_json,feiertage=excluded.feiertage,
                basis_json=excluded.basis_json,version=mitarbeiter_urlaubskonten.version+1,
                geprueft_von=excluded.geprueft_von,geprueft_am=excluded.geprueft_am RETURNING mitarbeiter_id''',
                       (mid, year, balance, cutoff.isoformat(), json.dumps(sorted(weekdays)), holiday_basis, json.dumps(basis, sort_keys=True), who['actor'], _now())).fetchall()
            self._audit(db, mid, who['actor'], 'konto_geprueft', year, {'resttage': _display(balance), 'stichtag': cutoff.isoformat()})
        self._backup()

    def review(self, who, request_id, decision, version, account_version=None):
        self._admin(who)
        if decision not in ('genehmigt', 'abgelehnt'):
            raise ValueError('Genehmigen oder ablehnen auswählen.')
        with self.db() as db:
            initial = db.execute('SELECT mitarbeiter_id FROM mitarbeiter_urlaubsantraege WHERE id=?', (request_id,)).fetchone()
            if not initial:
                raise ValueError('Antrag nicht gefunden.')
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (initial['mitarbeiter_id'],))
            row = db.execute('SELECT * FROM mitarbeiter_urlaubsantraege WHERE id=?', (request_id,)).fetchone()
            if row['status'] != 'beantragt' or str(row['version']) != str(version):
                raise ValueError('Antrag wurde bereits geprüft oder geändert. Bitte neu laden.')
            days, calendar_id = None, None
            if decision == 'genehmigt':
                person = db.execute('SELECT aktiv FROM mitarbeiter WHERE id=?', (row['mitarbeiter_id'],)).fetchone()
                if not person or not person['aktiv']:
                    raise ValueError('Mitarbeiter ist nicht aktiv.')
                account, left, need = self._account(db, row['mitarbeiter_id'], row['jahr'])
                if not account or left is None:
                    raise ValueError(need)
                if str(account['version']) != str(account_version):
                    raise ValueError('Kontobasis wurde geändert. Bitte die aktuelle Tagesberechnung erneut prüfen.')
                start, end = _date(row['start_datum']), _date(row['end_datum'])
                if self._overlaps(db, row['mitarbeiter_id'], start, end, ignore=request_id):
                    raise ValueError('Zeitraum überschneidet sich inzwischen mit einem anderen Antrag oder Urlaubseintrag.')
                days = self._count(account, start, end)
                if days <= 0 or days > left:
                    raise ValueError('Kein ausreichender geprüfter Resturlaub für diese Genehmigung.')
                now = _now()
                result = db.execute('INSERT INTO mitarbeiter_urlaub(mitarbeiter_id,start_datum,end_datum,notiz,erstellt_am,geaendert_am) VALUES(?,?,?,?,?,?) RETURNING id',
                                    (row['mitarbeiter_id'], start.strftime('%d.%m.%Y'), end.strftime('%d.%m.%Y'), self._calendar_note(request_id), now, now)).fetchone()
                calendar_id = result['id']
            changed = db.execute('''UPDATE mitarbeiter_urlaubsantraege SET status=?,belastung_halbtage=?,kalender_id=?,
                entschieden_am=?,entschieden_von=?,version=version+1 WHERE id=? AND status='beantragt' AND version=?''',
                                 (decision, days, calendar_id, _now(), who['actor'], request_id, version))
            if changed.rowcount != 1:
                raise ValueError('Antrag wurde inzwischen geändert. Bitte neu laden.')
            self._audit(db, row['mitarbeiter_id'], who['actor'], decision, request_id, {'tage': _display(days) if days is not None else None})
        self._backup()

    def admin_view(self, who, year=None):
        self._admin(who)
        year = _year(year if year is not None else _today().year)
        with self.db() as db:
            employees = [dict(r) for r in db.execute('SELECT id,name,aktiv FROM mitarbeiter ORDER BY aktiv DESC,name,id').fetchall()]
            for person in employees:
                person['konto'] = self._summary(db, person['id'], year)
                row = db.execute('SELECT * FROM mitarbeiter_urlaubskonten WHERE mitarbeiter_id=? AND jahr=?', (person['id'], year)).fetchone()
                person['basis'] = {'version': row['version'], 'arbeitstage': json.loads(row['arbeitstage_json']), 'feiertage': row['feiertage']} if row else {'version': 0, 'arbeitstage': [], 'feiertage': ''}
                snapshot, person['basis_token'] = self._snapshot(db, person['id'], year)
                person['kalender'] = snapshot['kalender']
                for item in person['konto']['antraege']:
                    if item['status'] == 'beantragt':
                        try:
                            item['vorschau'] = self._preview(db, person['id'], _date(item['start_datum']), _date(item['end_datum']))
                        except ValueError as exc:
                            item['vorschau'] = {'bedarf': str(exc), 'tage': None, 'saldo_ausreichend': False}
            return {'jahr': year, 'mitarbeiter': employees}


def register_selfservice(portal, bp, protected):
    """Register personal routes on the existing assistant blueprint.

    READ_ONLY allowlist: vacation_page, vacation_summary, vacation_apply,
    vacation_withdraw. The admin routes use admin_required separately.
    Mutations have their own CSRF guard as well as the portal's global guard.
    """
    service = EmployeeSelfService(portal)
    portal.assistant_selfservice = service
    portal.assistant_selfservice_init_schema = service.init_schema

    def csrf():
        expected = session.get('csrf_token')
        supplied = request.form.get('csrf_token') or request.headers.get('X-CSRF-Token')
        if not expected or not supplied or not hmac.compare_digest(str(expected), str(supplied)):
            abort(400)

    def token():
        if not session.get('csrf_token'):
            session['csrf_token'] = secrets.token_urlsafe(32)
        return session['csrf_token']

    def actor():
        return {'actor': 'admin'}

    @bp.get('/urlaub')
    @protected
    def vacation_page(who):
        if who.get('actor') == 'admin':
            return redirect(url_for('assistent.vacation_admin'))
        try:
            summary = service.summary(who, request.args.get('jahr', _today().year))
        except (ValueError, PermissionError) as exc:
            return str(exc), 400
        employee_portal = getattr(portal, 'employee_portal', None)
        return render_template('assistent_urlaub.html', urlaub=summary, csrf=token(), request_id=secrets.token_hex(16), today=_today().isoformat(),
                               betriebsurlaub=employee_portal.company_holidays(summary['jahr']) if employee_portal else [])

    @bp.get('/urlaub/stand')
    @protected
    def vacation_summary(who):
        try:
            return jsonify(service.summary(who, request.args.get('jahr', _today().year)))
        except PermissionError as exc:
            return jsonify(error=str(exc)), 403

    @bp.post('/urlaub/antrag')
    @protected
    def vacation_apply(who):
        csrf()
        year = _today().year
        try:
            if request.form.get('confirmed') != 'ja':
                raise ValueError('Zeitraum prüfen und den Antrag ausdrücklich bestätigen.')
            result = service.apply(who, request.form.get('von'), request.form.get('bis'), request.form.get('request_id'))
            year = result['jahr']
            flash('Urlaubsantrag eingereicht. Noch nicht genehmigt.', 'success')
        except (ValueError, PermissionError) as exc:
            flash(str(exc), 'warning')
        return redirect(url_for('assistent.vacation_page', jahr=year), code=303)

    @bp.post('/urlaub/antrag/<request_id>/zurueckziehen')
    @protected
    def vacation_withdraw(who, request_id):
        csrf()
        try:
            service.withdraw(who, request_id, request.form.get('version'))
            flash('Eigener offener Antrag zurückgezogen.', 'success')
        except (ValueError, PermissionError) as exc:
            flash(str(exc), 'warning')
        return redirect(url_for('assistent.vacation_page'), code=303)

    @bp.get('/urlaub/verwaltung')
    @portal.admin_required
    def vacation_admin():
        return render_template('assistent_urlaub_admin.html', data=service.admin_view(actor(), request.args.get('jahr', _today().year)),
                               csrf=token(), weekdays=WEEKDAYS, today=_today().isoformat())

    @bp.post('/urlaub/verwaltung/konto/<int:mid>')
    @portal.admin_required
    def vacation_account(mid):
        csrf()
        try:
            payload = dict(request.form)
            payload['arbeitstage'] = [int(x) for x in request.form.getlist('arbeitstage')]
            payload['confirmed'] = request.form.get('confirmed') == 'ja'
            service.save_account(actor(), mid, payload)
            flash('Geprüftes Urlaubskonto gespeichert.', 'success')
        except ValueError as exc:
            flash(str(exc), 'warning')
        return redirect(url_for('assistent.vacation_admin', jahr=request.form.get('jahr', _today().year)), code=303)

    @bp.post('/urlaub/verwaltung/antrag/<request_id>')
    @portal.admin_required
    def vacation_review(request_id):
        csrf()
        try:
            if request.form.get('confirmed') != 'ja':
                raise ValueError('Chefentscheidung ausdrücklich bestätigen.')
            service.review(actor(), request_id, request.form.get('entscheidung'), request.form.get('version'), request.form.get('konto_version'))
            flash('Entscheidung gespeichert.', 'success')
        except ValueError as exc:
            flash(str(exc), 'warning')
        return redirect(url_for('assistent.vacation_admin', jahr=request.form.get('jahr', _today().year)), code=303)

    return service
