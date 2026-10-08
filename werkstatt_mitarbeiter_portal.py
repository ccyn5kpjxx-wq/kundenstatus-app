"""Private employee profiles and original payslips, without AI/OCR or banking.

The personal session identifies the employee; request IDs never do. Original
documents stay in database blobs and are served as protected attachments only.
Profile and payroll writes share the destructive-restore originals lock.
"""
import base64
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import hmac
import io
import json
from pathlib import Path
import re
import secrets
import sqlite3
import time
import warnings
from zoneinfo import ZoneInfo

import fitz
from flask import Blueprint, abort, flash, redirect, render_template, request, send_file, session
from PIL import Image
from werkzeug.utils import secure_filename


TABLES = ('mitarbeiter_portal_profile', 'mitarbeiter_lohnzettel', 'mitarbeiter_betriebsurlaub')
PROFILE_FIELDS = ('personalnummer', 'steuer_id', 'steuernummer', 'adresse', 'geburtsdatum', 'email', 'telefon')
WORK_PLAN_FIELDS = ('wochenstunden', 'tagesstunden', 'pausenminuten', 'beginn', 'arbeitstage')
WORK_PLAN_COLUMNS = {
    'arbeitsplan_wochenminuten': 'INTEGER NOT NULL DEFAULT 0',
    'arbeitsplan_tagesminuten': 'INTEGER NOT NULL DEFAULT 0',
    'arbeitsplan_pausenminuten': 'INTEGER NOT NULL DEFAULT 0',
    'arbeitsplan_beginn': "TEXT NOT NULL DEFAULT ''",
    'arbeitsplan_tage_json': "TEXT NOT NULL DEFAULT '[]'",
}
_WEEKDAYS = ('Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag', 'Sonntag')
_PLAN_NOTE = ('Geplante Sollzeiten, keine erfassten Stempel. Pausen werden nur nach '
              'eigenem Stempel abgezogen; der Arbeitsplan bucht keine Arbeitszeit.')
MAX_DOCUMENT_BYTES = 10 * 1024 * 1024
_PERIOD = re.compile(r'20\d{2}-(?:0[1-9]|1[0-2])')
_BERLIN = ZoneInfo('Europe/Berlin')
_RESTORE_ERROR = ('Datenimport gesperrt: Die Sicherung enthält vorhandene persönliche '
                  'Profile, Lohnzettel oder Betriebsurlaubstermine nicht unverändert. '
                  'Bitte eine aktuelle Sicherung verwenden.')


def _text(value, maximum, label, *, multiline=False):
    if not isinstance(value, str) or len(value) > maximum:
        raise ValueError(f'{label}: höchstens {maximum} Zeichen angeben.')
    if any(ord(char) < 32 and not (multiline and char in '\n\r') for char in value):
        raise ValueError(f'{label}: ungültige Steuerzeichen.')
    if any(char in '<>' for char in value):
        raise ValueError(f'{label}: nur normalen Text angeben.')
    return value.strip()


def _iso_date(value, label):
    if not isinstance(value, str) or not re.fullmatch(r'20\d{2}-\d{2}-\d{2}|19\d{2}-\d{2}-\d{2}', value):
        raise ValueError(f'{label}: Datum im Format JJJJ-MM-TT angeben.')
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f'{label}: gültiges Datum angeben.') from None


def _plan_minutes(value, label, maximum):
    if not isinstance(value, str) or not re.fullmatch(r'(?:0|[1-9][0-9]{0,2})(?:[.,][0-9]{1,2})?', value):
        raise ValueError(f'{label}: Stunden als Zahl angeben.')
    minutes = Decimal(value.replace(',', '.')) * 60
    if minutes != minutes.to_integral_value() or not 0 < minutes <= maximum:
        raise ValueError(f'{label}: positive Stunden mit minutengenauer Dauer angeben.')
    return int(minutes)


def _plan_data(payload):
    if not isinstance(payload, dict) or set(payload) != set(WORK_PLAN_FIELDS):
        raise ValueError('Nur die vorgesehenen Arbeitsplanfelder angeben.')
    weekly = _plan_minutes(payload['wochenstunden'], 'Wochenstunden', 7 * 24 * 60)
    daily = _plan_minutes(payload['tagesstunden'], 'Tagesstunden', 24 * 60)
    pause = payload['pausenminuten']
    days = payload['arbeitstage']
    beginning = payload['beginn']
    if (not isinstance(pause, str) or not re.fullmatch(r'0|[1-9][0-9]{0,3}', pause)
            or int(pause) > 24 * 60):
        raise ValueError('Geplante Pause in ganzen Minuten angeben.')
    if (not isinstance(days, list) or not days or len(days) > 7
            or any(type(day) is not int or not 0 <= day <= 6 for day in days)
            or len(set(days)) != len(days)):
        raise ValueError('Arbeitstage von Montag bis Sonntag eindeutig auswählen.')
    if weekly != daily * len(days):
        raise ValueError('Wochenstunden müssen Tagesstunden mal Anzahl der Arbeitstage entsprechen.')
    if not isinstance(beginning, str) or not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]', beginning):
        raise ValueError('Geplanten Beginn im Format HH:MM angeben.')
    start = int(beginning[:2]) * 60 + int(beginning[3:])
    if start + daily + int(pause) > 24 * 60:
        raise ValueError('Der tägliche Arbeitsplan muss innerhalb desselben Kalendertages enden.')
    return dict(arbeitsplan_wochenminuten=weekly, arbeitsplan_tagesminuten=daily,
                arbeitsplan_pausenminuten=int(pause), arbeitsplan_beginn=beginning,
                arbeitsplan_tage_json=json.dumps(sorted(days), separators=(',', ':')))


def _plan_view(row):
    unknown = dict(bekannt=False, tage=[], tage_label='', wochenstunden='', tagesstunden='',
                   pausenminuten=None, beginn='', ende='', hinweis='Noch kein persönlicher Arbeitsplan hinterlegt. ' + _PLAN_NOTE)
    if not row or not row.get('arbeitsplan_beginn'):
        return unknown
    try:
        weekly, daily, pause = (row[key] for key in ('arbeitsplan_wochenminuten', 'arbeitsplan_tagesminuten', 'arbeitsplan_pausenminuten'))
        if any(type(value) is not int for value in (weekly, daily, pause)):
            raise ValueError()
        days = json.loads(row['arbeitsplan_tage_json'])
        values = _plan_data(dict(wochenstunden=str(Decimal(weekly) / 60), tagesstunden=str(Decimal(daily) / 60),
                                 pausenminuten=str(pause), beginn=row['arbeitsplan_beginn'], arbeitstage=days))
        beginning = values['arbeitsplan_beginn']
        end = int(beginning[:2]) * 60 + int(beginning[3:]) + daily + pause
        hours = lambda minutes: format(Decimal(minutes) / 60, 'f').rstrip('0').rstrip('.') if minutes % 60 else str(minutes // 60)
        return dict(bekannt=True, tage=sorted(days),
                    tage_label='Montag–Freitag' if sorted(days) == [0, 1, 2, 3, 4] else ', '.join(_WEEKDAYS[day] for day in sorted(days)),
                    wochenstunden=hours(weekly), tagesstunden=hours(daily), pausenminuten=pause,
                    beginn=beginning, ende=f'{end // 60:02d}:{end % 60:02d}', hinweis=_PLAN_NOTE)
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        return dict(unknown, hinweis='Der hinterlegte Arbeitsplan muss intern geprüft werden. ' + _PLAN_NOTE)


def _document(file):
    if file is None or not file.filename:
        raise ValueError('Einen Lohnzettel als PDF, JPEG oder PNG auswählen.')
    raw = file.stream.read(MAX_DOCUMENT_BYTES + 1)
    if not raw or len(raw) > MAX_DOCUMENT_BYTES:
        raise ValueError('Lohnzettel darf höchstens 10 MB groß sein.')
    supplied = str(file.filename).replace('\\', '/').rsplit('/', 1)[-1]
    filename = secure_filename(supplied)[:160]
    extension = Path(filename).suffix.lower()
    mime = ''
    try:
        if raw.startswith(b'%PDF-') and extension == '.pdf':
            with fitz.open(stream=raw, filetype='pdf') as document:
                if document.is_encrypted or document.is_repaired or not 1 <= document.page_count <= 100:
                    raise ValueError('PDF muss vollständig lesbar sein und 1 bis 100 Seiten enthalten.')
                for number in range(document.page_count):
                    page = document.load_page(number)
                    if page.rect.is_empty or page.rect.is_infinite:
                        raise ValueError('PDF enthält eine ungültige Seite.')
                mime = 'application/pdf'
        elif extension in ('.png', '.jpg', '.jpeg'):
            with warnings.catch_warnings():
                warnings.simplefilter('error', Image.DecompressionBombWarning)
                with Image.open(io.BytesIO(raw)) as image:
                    kind = image.format
                    if (kind not in ('PNG', 'JPEG') or getattr(image, 'n_frames', 1) != 1
                            or image.width < 1 or image.height < 1
                            or image.width * image.height > 25_000_000
                            or (extension == '.png') != (kind == 'PNG')):
                        raise ValueError('Nur ein einzelnes JPEG- oder PNG-Bild verwenden.')
                    image.verify()
                with Image.open(io.BytesIO(raw)) as image:
                    image.load()
                mime = 'image/png' if kind == 'PNG' else 'image/jpeg'
        if not mime or not filename:
            raise ValueError('Dateityp und Dateiendung müssen zu PDF, JPEG oder PNG passen.')
    except (fitz.FileDataError, OSError, SyntaxError, Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise ValueError('Der Lohnzettel ist beschädigt oder kein lesbares PDF/JPEG/PNG.') from None
    return raw, filename, mime


class EmployeePortal:
    def __init__(self, portal):
        self.p = portal

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
            db.executescript('''CREATE TABLE IF NOT EXISTS mitarbeiter_portal_profile (
                mitarbeiter_id INTEGER PRIMARY KEY, personalnummer TEXT NOT NULL DEFAULT '',
                steuer_id TEXT NOT NULL DEFAULT '', steuernummer TEXT NOT NULL DEFAULT '',
                adresse TEXT NOT NULL DEFAULT '', geburtsdatum TEXT NOT NULL DEFAULT '',
                email TEXT NOT NULL DEFAULT '', telefon TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL, updated_by TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS mitarbeiter_lohnzettel (
                id INTEGER PRIMARY KEY AUTOINCREMENT, mitarbeiter_id INTEGER NOT NULL,
                period TEXT NOT NULL, filename TEXT NOT NULL, mime TEXT NOT NULL,
                size_bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, original_base64 TEXT NOT NULL,
                created_at TEXT NOT NULL, created_by TEXT NOT NULL,
                UNIQUE(mitarbeiter_id,period,sha256));
                CREATE INDEX IF NOT EXISTS idx_mitarbeiter_lohnzettel_owner
                ON mitarbeiter_lohnzettel(mitarbeiter_id,period,id);
                CREATE TABLE IF NOT EXISTS mitarbeiter_betriebsurlaub (
                id INTEGER PRIMARY KEY AUTOINCREMENT, start_datum TEXT NOT NULL,
                end_datum TEXT NOT NULL, notiz TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL, created_by TEXT NOT NULL,
                UNIQUE(start_datum,end_datum,notiz));''')
            for column, definition in WORK_PLAN_COLUMNS.items():
                self.p.ensure_column(db, 'mitarbeiter_portal_profile', column, definition)

    def identity(self, db=None):
        mid, version = session.get('assistent_mid'), session.get('assistent_version')
        auth = session.get('assistent_auth_version', 1)
        if (type(mid) is not int or mid < 1 or type(version) is not int
                or type(auth) is not int or not self.p.app.config.get('ASSISTANT_NATIVE_COCKPIT', True)):
            return None
        if db is None:
            with self.db() as connection:
                return self.identity(connection)
        row = db.execute('''SELECT r.*,m.name AS mitarbeiter_name,m.aktiv
            FROM assistent_rechte r JOIN mitarbeiter m ON m.id=r.mitarbeiter_id
            WHERE r.mitarbeiter_id=?''', (mid,)).fetchone()
        if (not row or not row['aktiv'] or not row['lesen'] or row['version'] != version
                or dict(row).get('auth_version', 1) != auth):
            return None
        who = {key: row[key] for key in ('mitarbeiter_id', 'mitarbeiter_name', 'aktiv', 'version',
                                         'lesen', 'dokumentieren', 'einkaufen', 'limit_cent')}
        return dict(who, auth_version=auth, actor='mitarbeiter:' + str(mid))

    @staticmethod
    def _admin():
        if not session.get('admin'):
            raise PermissionError('Nur die Werkstattleitung darf diese Daten ändern.')

    def _employee(self, db, mid):
        if type(mid) is not int or not 1 <= mid <= 2147483647:
            raise LookupError('Mitarbeiter nicht gefunden.')
        row = db.execute('SELECT id,name,aktiv FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
        if not row:
            raise LookupError('Mitarbeiter nicht gefunden.')
        return dict(row)

    def _audit(self, db, action, mid, **details):
        # No tax numbers, private address, filename or payroll bytes in audit.
        db.execute('INSERT INTO assistent_audit(actor,auftrag_id,aktion,details,zeit) VALUES(?,?,?,?,?)',
                   ('admin', None, action, json.dumps({'mitarbeiter': mid, **details}), self.p.now_str()))

    def _backup(self):
        hook = getattr(self.p, 'schedule_change_backup', None)
        if hook:
            hook('mitarbeiter-portal')

    def _profile(self, db, mid):
        row = db.execute('SELECT * FROM mitarbeiter_portal_profile WHERE mitarbeiter_id=?', (mid,)).fetchone()
        if row:
            return dict(row)
        previous = db.execute('SELECT adresse,geburtsdatum,email,telefon FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
        return {key: (previous[key] or '') if key in dict(previous) else '' for key in PROFILE_FIELDS}

    def _payrolls(self, db, mid, *, admin=False):
        rows = db.execute('''SELECT id,period,filename,mime,size_bytes,created_at
            FROM mitarbeiter_lohnzettel WHERE mitarbeiter_id=? ORDER BY period DESC,id DESC''', (mid,)).fetchall()
        return [dict(row, bytes=row['size_bytes'], url=(f'/admin/mitarbeiter/{mid}/portal/lohnzettel/'
                    if admin else '/werkstatt/mein-konto/lohnzettel/') + str(row['id'])) for row in rows]

    def admin_view(self, mid):
        self._admin()
        with self.db() as db:
            employee = self._employee(db, mid)
            profile = self._profile(db, mid)
            return {'employee': employee, 'profile': profile, 'arbeitsplan': _plan_view(profile),
                    'payrolls': self._payrolls(db, mid, admin=True)}

    def work_plan(self, mid):
        """Read only; the caller supplies its already authorized employee ID."""
        with self.db() as db:
            self._employee(db, mid)
            return _plan_view(self._profile(db, mid))

    def personal_view(self):
        with self.db() as db:
            who = self.identity(db)
            if not who:
                raise PermissionError('Mit deinem persönlichen Mitarbeiterzugang anmelden.')
            mid = who['mitarbeiter_id']
            profile = self._profile(db, mid)
            result = {'who': who, 'employee': {'id': mid, 'name': who['mitarbeiter_name']},
                      'profile': profile, 'arbeitsplan': _plan_view(profile), 'payrolls': self._payrolls(db, mid)}
        result['urlaub'] = self.p.assistant_selfservice.summary(who)
        try:
            report = self.p.assistant_time.summary(who)
            result['arbeitszeit'] = {
                'status_label': {'abwesend': 'Nicht eingestempelt', 'arbeitet': 'Bei der Arbeit',
                                 'pause': 'In Pause'}.get(report['status']['zustand'], 'Zeitstatus prüfen'),
                'monat_stunden': report['abgeschlossene_arbeitszeit'], 'heute_stunden': None,
                'nachricht': 'Die Zeitübersicht zeigt deine erfassten Stempel und abgeschlossenen Arbeitszeiten.'}
        except ValueError as exc:
            result['arbeitszeit'] = {'status_label': 'Zeitstatus prüfen', 'monat_stunden': None,
                                    'heute_stunden': None, 'nachricht': str(exc)}
        result['betriebsurlaub'] = self.company_holidays(datetime.now(_BERLIN).year)
        return result

    def save_profile(self, mid, payload):
        self._admin()
        if not isinstance(payload, dict) or set(payload) != set(PROFILE_FIELDS):
            raise ValueError('Nur die vorgesehenen persönlichen Profilfelder angeben.')
        limits = dict(personalnummer=40, steuer_id=11, steuernummer=30, adresse=300,
                      geburtsdatum=10, email=254, telefon=40)
        data = {key: _text(payload[key], limits[key], key, multiline=key == 'adresse') for key in PROFILE_FIELDS}
        if data['steuer_id'] and not re.fullmatch(r'[0-9]{11}', data['steuer_id']):
            raise ValueError('Steuer-ID muss aus genau 11 Ziffern bestehen.')
        if data['steuernummer'] and not re.fullmatch(r'[0-9 /-]{3,30}', data['steuernummer']):
            raise ValueError('Steuernummer nur mit Ziffern, Leerzeichen, Bindestrich oder Schrägstrich angeben.')
        if data['geburtsdatum'] and _iso_date(data['geburtsdatum'], 'Geburtsdatum') > datetime.now(_BERLIN).date():
            raise ValueError('Geburtsdatum darf nicht in der Zukunft liegen.')
        if data['email'] and not re.fullmatch(r'[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+', data['email']):
            raise ValueError('Eine gültige E-Mail-Adresse angeben.')
        if data['telefon'] and not re.fullmatch(r'[0-9+()/ .-]{3,40}', data['telefon']):
            raise ValueError('Telefonnummer nur mit Ziffern und üblichen Trennzeichen angeben.')
        with self.p.portal_originals_operation_lock(), self.db() as db:
            self._employee(db, mid)
            columns = ','.join(PROFILE_FIELDS)
            updates = ','.join(key + '=excluded.' + key for key in PROFILE_FIELDS)
            db.execute('INSERT INTO mitarbeiter_portal_profile(mitarbeiter_id,' + columns + ',updated_at,updated_by) '
                       'VALUES(' + ','.join('?' for _ in range(10)) + ') ON CONFLICT(mitarbeiter_id) DO UPDATE SET '
                       + updates + ',updated_at=excluded.updated_at,updated_by=excluded.updated_by RETURNING mitarbeiter_id',
                       (mid, *(data[key] for key in PROFILE_FIELDS), self.p.now_str(), 'admin')).fetchall()
            self._audit(db, 'mitarbeiter_portal_profil_gespeichert', mid)
        self._backup()

    def save_work_plan(self, mid, payload):
        self._admin()
        data = _plan_data(payload)
        with self.p.portal_originals_operation_lock(), self.db() as db:
            self._employee(db, mid)
            # Creating the first private row must preserve proven old contact
            # fields; updating an existing row changes only its plan columns.
            previous = self._profile(db, mid)
            columns = (*PROFILE_FIELDS, *WORK_PLAN_COLUMNS)
            updates = ','.join(key + '=excluded.' + key for key in WORK_PLAN_COLUMNS)
            db.execute('INSERT INTO mitarbeiter_portal_profile(mitarbeiter_id,' + ','.join(columns) + ',updated_at,updated_by) '
                       'VALUES(' + ','.join('?' for _ in range(len(columns) + 3)) + ') ON CONFLICT(mitarbeiter_id) DO UPDATE SET '
                       + updates + ',updated_at=excluded.updated_at,updated_by=excluded.updated_by RETURNING mitarbeiter_id',
                       (mid, *(previous[key] for key in PROFILE_FIELDS), *(data[key] for key in WORK_PLAN_COLUMNS),
                        self.p.now_str(), 'admin')).fetchall()
            self._audit(db, 'mitarbeiter_arbeitsplan_gespeichert', mid)
        self._backup()

    def upload_payroll(self, mid, period, file):
        self._admin()
        if not isinstance(period, str) or not _PERIOD.fullmatch(period):
            raise ValueError('Abrechnungsmonat im Format JJJJ-MM angeben.')
        raw, filename, mime = _document(file)
        digest = hashlib.sha256(raw).hexdigest()
        with self.p.portal_originals_operation_lock(), self.db() as db:
            self._employee(db, mid)
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (mid,))
            prior = db.execute('SELECT id FROM mitarbeiter_lohnzettel WHERE mitarbeiter_id=? AND period=? AND sha256=?',
                               (mid, period, digest)).fetchone()
            if prior:
                return prior['id']
            row = db.execute('''INSERT INTO mitarbeiter_lohnzettel
                (mitarbeiter_id,period,filename,mime,size_bytes,sha256,original_base64,created_at,created_by)
                VALUES(?,?,?,?,?,?,?,?,?) RETURNING id''',
                (mid, period, filename, mime, len(raw), digest, base64.b64encode(raw).decode('ascii'),
                 self.p.now_str(), 'admin')).fetchone()
            self._audit(db, 'mitarbeiter_portal_lohnzettel_hinterlegt', mid, lohnzettel=row['id'], monat=period)
            payroll_id = row['id']
        self._backup()
        return payroll_id

    def payroll(self, payroll_id, *, admin_mid=None):
        with self.db() as db:
            if admin_mid is not None:
                self._admin()
                mid = self._employee(db, admin_mid)['id']
            else:
                who = self.identity(db)
                if not who:
                    raise LookupError('Lohnzettel nicht gefunden.')
                mid = who['mitarbeiter_id']
            row = db.execute('SELECT * FROM mitarbeiter_lohnzettel WHERE id=? AND mitarbeiter_id=?',
                             (payroll_id, mid)).fetchone()
            if not row:
                raise LookupError('Lohnzettel nicht gefunden.')
            row = dict(row)
        try:
            raw = base64.b64decode(row['original_base64'], validate=True)
            if (not 0 < len(raw) <= MAX_DOCUMENT_BYTES or len(raw) != row['size_bytes']
                    or hashlib.sha256(raw).hexdigest() != row['sha256']
                    or row['mime'] not in ('application/pdf', 'image/png', 'image/jpeg')):
                raise ValueError()
        except (ValueError, TypeError):
            raise LookupError('Lohnzettel nicht gefunden.') from None
        return row, raw

    def company_holidays(self, jahr=None):
        if jahr is not None and (type(jahr) is not int or not 1900 <= jahr <= 2199):
            raise ValueError('Gültiges Jahr angeben.')
        with self.db() as db:
            if jahr is None:
                rows = db.execute('SELECT * FROM mitarbeiter_betriebsurlaub ORDER BY start_datum,id').fetchall()
            else:
                rows = db.execute('''SELECT * FROM mitarbeiter_betriebsurlaub
                    WHERE start_datum<=? AND end_datum>=? ORDER BY start_datum,id''',
                    (f'{jahr}-12-31', f'{jahr}-01-01')).fetchall()
        return [dict(row) for row in rows]

    def add_company_holiday(self, start, end, note):
        self._admin()
        first, last = _iso_date(start, 'Beginn'), _iso_date(end, 'Ende')
        if first.year < 2000 or last < first or (last - first).days > 366:
            raise ValueError('Betriebsurlaub mit gültigem Beginn und Ende, höchstens 367 Kalendertagen angeben.')
        note = _text(note, 500, 'Notiz', multiline=True)
        with self.p.portal_originals_operation_lock(), self.db() as db:
            db.execute('''INSERT INTO mitarbeiter_betriebsurlaub(start_datum,end_datum,notiz,created_at,created_by)
                VALUES(?,?,?,?,?) ON CONFLICT(start_datum,end_datum,notiz) DO NOTHING RETURNING id''',
                (start, end, note, self.p.now_str(), 'admin')).fetchall()
            self._audit(db, 'mitarbeiter_betriebsurlaub_hinterlegt', None)
        self._backup()

    def new_time_form(self, who, revision):
        current = self.identity()
        if (not current or current['actor'] != who.get('actor')
                or current['version'] != who.get('version')
                or type(revision) is not int or revision < 0):
            raise PermissionError('Persönlichen Zeitstatus erneut öffnen.')
        now = int(time.time())
        forms = session.get('employee_time_forms', {})
        forms = {key: value for key, value in forms.items()
                 if isinstance(value, dict) and type(value.get('issued')) is int and value['issued'] > now - 1800}
        forms = dict(list(forms.items())[-7:])
        identifier = secrets.token_urlsafe(18)
        forms[identifier] = {key: current[key] for key in ('mitarbeiter_id', 'version', 'auth_version')}
        forms[identifier].update(revision=revision, issued=now)
        session['employee_time_forms'] = forms
        return identifier

    def stamp_time(self, payload):
        if (not isinstance(payload, dict) or set(payload) != {'aktion','revision','request_id','confirmed'}
                or payload['confirmed'] != 'ja' or not isinstance(payload['revision'], str)
                or not re.fullmatch(r'0|[1-9][0-9]{0,9}', payload['revision'])
                or not isinstance(payload['request_id'], str)):
            raise ValueError('Persönlichen Zeitstatus erneut öffnen und den passenden Stempel wählen.')
        evidence = session.get('employee_time_forms', {}).get(payload['request_id'])
        with self.p.portal_originals_operation_lock():
            who = self.identity()
            if (not who or not isinstance(evidence, dict)
                    or any(evidence.get(key) != who[key] for key in ('mitarbeiter_id', 'version', 'auth_version'))
                    or evidence.get('revision') != int(payload['revision'])
                    or type(evidence.get('issued')) is not int or evidence['issued'] <= time.time() - 1800):
                raise PermissionError('Die Stempelfreigabe gehört zu einem anderen oder veralteten Zugang. Zeitstatus neu öffnen.')
            return self.p.assistant_time.stamp(who, payload['aktion'], payload['request_id'], int(payload['revision']))


def register_employee_portal(p):
    service = EmployeePortal(p)
    service.init_schema()
    p.employee_portal = service
    p.employee_portal_init_schema = service.init_schema
    bp = Blueprint('employee_portal', __name__)

    def token():
        if not session.get('csrf_token'):
            session['csrf_token'] = secrets.token_urlsafe(32)
        return session['csrf_token']

    def csrf():
        expected, supplied = session.get('csrf_token'), request.form.get('csrf_token') or request.headers.get('X-CSRF-Token')
        if not expected or not supplied or not hmac.compare_digest(str(expected), str(supplied)):
            abort(400)

    @bp.get('/werkstatt/mein-konto')
    def personal():
        try:
            data = service.personal_view()
        except PermissionError:
            return redirect('/werkstatt/materialbestellung')
        return render_template('mitarbeiter_portal.html', **data, csrf_token=token())

    @bp.route('/admin/mitarbeiter/<int:mid>/portal', methods=['GET', 'POST'])
    @p.admin_required
    def admin_profile(mid):
        error = None
        if request.method == 'POST':
            csrf()
            try:
                if set(request.form) - set(PROFILE_FIELDS) - {'csrf_token'}:
                    raise ValueError('Nur die vorgesehenen persönlichen Profilfelder angeben.')
                service.save_profile(mid, {key: request.form.get(key, '') for key in PROFILE_FIELDS})
                flash('Persönliches Profil gespeichert.', 'success')
                return redirect(f'/admin/mitarbeiter/{mid}/portal', code=303)
            except LookupError:
                abort(404)
            except ValueError as exc:
                error = str(exc)
        try:
            data = service.admin_view(mid)
        except LookupError:
            abort(404)
        return render_template('mitarbeiter_portal_admin.html', **data, csrf_token=token(), error=error), 400 if error else 200

    @bp.post('/admin/mitarbeiter/<int:mid>/portal/lohnzettel')
    @p.admin_required
    def admin_upload(mid):
        csrf()
        try:
            if set(request.files) != {'file'} or len(request.files.getlist('file')) != 1:
                raise ValueError('Genau einen Lohnzettel auswählen.')
            service.upload_payroll(mid, request.form.get('period'), request.files.get('file'))
            flash('Lohnzettel im persönlichen Profil hinterlegt.', 'success')
            return redirect(f'/admin/mitarbeiter/{mid}/portal', code=303)
        except LookupError:
            abort(404)
        except ValueError as exc:
            try:
                data = service.admin_view(mid)
            except LookupError:
                abort(404)
            return render_template('mitarbeiter_portal_admin.html', **data, csrf_token=token(), error=str(exc)), 400

    @bp.post('/admin/mitarbeiter/<int:mid>/portal/arbeitsplan')
    @p.admin_required
    def admin_work_plan(mid):
        csrf()
        try:
            if (set(request.form) - set(WORK_PLAN_FIELDS) - {'csrf_token'}
                    or any(len(request.form.getlist(key)) != 1 for key in WORK_PLAN_FIELDS if key != 'arbeitstage')):
                raise ValueError('Nur die vorgesehenen persönlichen Arbeitsplanfelder angeben.')
            days = request.form.getlist('arbeitstage')
            if any(not re.fullmatch('[0-6]', day) for day in days):
                raise ValueError('Arbeitstage von Montag bis Sonntag auswählen.')
            payload = {key: request.form.get(key, '') for key in WORK_PLAN_FIELDS if key != 'arbeitstage'}
            service.save_work_plan(mid, dict(payload, arbeitstage=[int(day) for day in days]))
            flash('Persönlicher Soll-Arbeitsplan gespeichert. Erfasste Stempel bleiben unverändert.', 'success')
            return redirect(f'/admin/mitarbeiter/{mid}/portal', code=303)
        except LookupError:
            abort(404)
        except ValueError as exc:
            try:
                data = service.admin_view(mid)
            except LookupError:
                abort(404)
            return render_template('mitarbeiter_portal_admin.html', **data, csrf_token=token(), error=str(exc)), 400

    def download(payroll_id, admin_mid=None):
        try:
            row, raw = service.payroll(payroll_id, admin_mid=admin_mid)
        except LookupError:
            abort(404)
        return send_file(io.BytesIO(raw), mimetype=row['mime'], as_attachment=True,
                         download_name=secure_filename(row['filename']) or 'Lohnzettel', etag=False, conditional=False)

    @bp.get('/werkstatt/mein-konto/lohnzettel/<int:payroll_id>')
    def personal_payroll(payroll_id):
        return download(payroll_id)

    @bp.post('/werkstatt/mein-konto/zeit')
    def personal_stamp():
        csrf()
        if not service.identity():
            return redirect('/werkstatt/materialbestellung')
        try:
            if set(request.form) - {'csrf_token','aktion','revision','request_id','confirmed'}:
                raise ValueError('Nur deinen eigenen aktuellen Zeitstempel bestätigen.')
            result = service.stamp_time({key: request.form.get(key) for key in ('aktion','revision','request_id','confirmed')})
            flash(result['hinweis'], 'success')
        except (ValueError, PermissionError) as exc:
            flash(str(exc), 'warning')
        return redirect('/werkstatt/assistent/arbeitszeit', code=303)

    @bp.get('/admin/mitarbeiter/<int:mid>/portal/lohnzettel/<int:payroll_id>')
    @p.admin_required
    def admin_payroll(mid, payroll_id):
        return download(payroll_id, mid)

    @bp.route('/admin/mitarbeiter/betriebsurlaub', methods=['GET', 'POST'])
    @p.admin_required
    def company_holidays_admin():
        error = None
        if request.method == 'POST':
            csrf()
            try:
                service.add_company_holiday(request.form.get('start_datum'), request.form.get('end_datum'), request.form.get('notiz', ''))
                flash('Betriebsurlaub hinterlegt. Persönliche Resttage wurden nicht verändert.', 'success')
                return redirect('/admin/mitarbeiter/betriebsurlaub', code=303)
            except ValueError as exc:
                error = str(exc)
        return render_template('mitarbeiter_betriebsurlaub.html', items=service.company_holidays(),
                               error=error, csrf_token=token()), 400 if error else 200

    @bp.after_request
    def private_response(response):
        response.headers['Cache-Control'] = 'private, no-store, max-age=0'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive'
        response.vary.add('Cookie')
        return response

    p.app.register_blueprint(bp)
    return service


def ensure_employee_private_state_for_import(p, *, export=None, imported_db=None, target=None, archive=None, names=None):
    """Caller holds Originals-Lock until import ends; preserve owner and bytes."""
    own_target, source = target is None, None
    if own_target:
        target = p.get_db()
    try:
        protected = {table: [dict(row) for row in target.execute('SELECT * FROM ' + table).fetchall()]
                     for table in TABLES if p.get_table_columns(target, table)}
        if not any(protected.values()):
            return
        mids = {row['mitarbeiter_id'] for table in TABLES[:2] for row in protected.get(table, [])}
        protected['mitarbeiter'] = []
        protected['assistent_rechte'] = []
        absent_rights = set()
        for mid in sorted(mids):
            employee = target.execute('SELECT id,name,aktiv FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
            if not employee:
                raise ValueError(_RESTORE_ERROR)
            protected['mitarbeiter'].append(dict(employee))
            rights = target.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=?', (mid,)).fetchone()
            if rights:
                protected['assistent_rechte'].append(dict(rights))
            else:
                absent_rights.add(mid)
        if imported_db is not None:
            source = sqlite3.connect(Path(imported_db).resolve().as_uri() + '?mode=ro', uri=True)
            source.row_factory = sqlite3.Row
            tables = {row['name'] for row in source.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            incoming = {table: [dict(row) for row in source.execute('SELECT * FROM ' + table).fetchall()]
                        if table in tables else [] for table in protected}
        else:
            incoming = (export or {}).get('tables')
        if not isinstance(incoming, dict):
            raise ValueError(_RESTORE_ERROR)
        incoming_rights = incoming.get('assistent_rechte', [])
        if (not isinstance(incoming_rights, list)
                or any(not isinstance(row, dict) or type(row.get('mitarbeiter_id')) is not int
                       or row['mitarbeiter_id'] < 1 or row['mitarbeiter_id'] in absent_rights
                       for row in incoming_rights)):
            raise ValueError(_RESTORE_ERROR)
        for table, rows in protected.items():
            key = 'mitarbeiter_id' if table in (TABLES[0], 'assistent_rechte') else 'id'
            candidates = incoming.get(table, [])
            if not isinstance(candidates, list) or any(not isinstance(row, dict) for row in candidates):
                raise ValueError(_RESTORE_ERROR)
            for row in rows:
                matches = [candidate for candidate in candidates if candidate.get(key) == row[key]]
                if len(matches) != 1:
                    raise ValueError(_RESTORE_ERROR)
                restored = matches[0]
                for column, original in row.items():
                    value = restored.get(column)
                    if column == 'original_base64' and imported_db is None:
                        reference = p.backup_binary_reference_map(export).get((table, row['id'], column))
                        if reference is not None:
                            if archive is None or names is None:
                                raise ValueError(_RESTORE_ERROR)
                            value = base64.b64encode(p.read_backup_binary_blob(archive, names, reference)).decode('ascii')
                    if column not in restored or value != original:
                        raise ValueError(_RESTORE_ERROR)
    except (sqlite3.Error, OSError, TypeError, KeyError, AttributeError):
        raise ValueError(_RESTORE_ERROR) from None
    finally:
        if source is not None:
            source.close()
        if own_target:
            target.close()
