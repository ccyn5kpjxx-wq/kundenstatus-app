"""Private employee profiles, payslips and contracts, without AI/OCR or banking.

The personal session identifies the employee; request IDs never do. Original
documents stay in database blobs and are served as protected attachments only.
Profile and document writes share the destructive-restore originals lock.
"""
import base64
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import hmac
import io
import json
import math
from pathlib import Path
import re
import secrets
import sqlite3
import struct
import time
import warnings
from xml.etree import ElementTree
import zipfile
import zlib
from zoneinfo import ZoneInfo

import fitz
from flask import Blueprint, abort, flash, redirect, render_template, request, send_file, session
from PIL import Image
from pypdf import PdfReader
from pypdf.generic import ArrayObject, DictionaryObject, StreamObject
from werkzeug.utils import secure_filename


TABLES = ('mitarbeiter_portal_profile', 'mitarbeiter_lohnzettel', 'mitarbeiter_betriebsurlaub',
          'mitarbeiter_arbeitsvertraege')
OWNER_TABLES = ('mitarbeiter_portal_profile', 'mitarbeiter_lohnzettel', 'mitarbeiter_arbeitsvertraege')
LEGACY_PROFILE_FIELDS = ('personalnummer', 'steuer_id', 'steuernummer', 'adresse', 'geburtsdatum', 'email', 'telefon')
PRIVATE_PROFILE_COLUMNS = {
    'sozialversicherungsnummer': "TEXT NOT NULL DEFAULT ''",
    'krankenkasse': "TEXT NOT NULL DEFAULT ''",
    'steuerklasse': "TEXT NOT NULL DEFAULT ''",
    'eintrittsdatum': "TEXT NOT NULL DEFAULT ''",
}
PROFILE_FIELDS = (*LEGACY_PROFILE_FIELDS, *PRIVATE_PROFILE_COLUMNS)
PROFILE_LIMITS = dict(personalnummer=40, steuer_id=11, steuernummer=30, adresse=300,
                      geburtsdatum=10, email=254, telefon=40, sozialversicherungsnummer=20,
                      krankenkasse=120, steuerklasse=1, eintrittsdatum=10)
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
DOCX_MIME = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
_DOCX_MAIN_TYPE = DOCX_MIME + '.main+xml'
_OOXML_TYPES = 'http://schemas.openxmlformats.org/package/2006/content-types'
_OOXML_RELS = 'http://schemas.openxmlformats.org/package/2006/relationships'
_PERIOD = re.compile(r'20\d{2}-(?:0[1-9]|1[0-2])')
_BERLIN = ZoneInfo('Europe/Berlin')
_RESTORE_ERROR = ('Datenimport gesperrt: Die Sicherung enthält vorhandene persönliche '
                  'Profile, Lohnzettel, Arbeitsverträge oder Betriebsurlaubstermine nicht unverändert. '
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


class _ContractXmlBuilder(ElementTree.TreeBuilder):
    """Package structure only; no DTD/entities or unbounded XML trees."""
    def __init__(self):
        super().__init__()
        self.depth = self.nodes = 0

    def start(self, tag, attrs):
        self.depth += 1
        self.nodes += 1
        if self.depth > 256 or self.nodes > 100_000:
            raise ValueError('DOCX-XML ist zu umfangreich.')
        return super().start(tag, attrs)

    def end(self, tag):
        result = super().end(tag)
        self.depth -= 1
        return result

    def doctype(self, *_):
        raise ValueError('DOCX darf keine XML-Dokumenttypdefinition enthalten.')


def _contract_xml(raw):
    return ElementTree.fromstring(raw, parser=ElementTree.XMLParser(target=_ContractXmlBuilder()))


def _validate_docx(raw):
    """Inspect a bounded OOXML package in memory; never extract, execute or fetch."""
    error = 'Nur vollständige DOCX-Dateien ohne Makros oder eingebettete Programme verwenden.'
    try:
        if not raw.startswith(b'PK\x03\x04'):
            raise ValueError(error)
        # Reject excessive central-directory counts before ZipFile allocates
        # one ZipInfo per member. Ten-MiB DOCX originals never need ZIP64.
        end = raw.rfind(b'PK\x05\x06', max(0, len(raw) - 65557))
        if end < 0 or end + 22 > len(raw):
            raise ValueError(error)
        _, disk, central_disk, disk_count, count, central_size, central_offset, comment_size = struct.unpack_from('<4s4H2LH', raw, end)
        if (disk or central_disk or disk_count != count or not 2 <= count <= 512
                or central_size > 512 * 1024 or central_offset + central_size != end
                or end + 22 + comment_size != len(raw)):
            raise ValueError(error)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            entries = archive.infolist()
            if len(entries) != count or sum(item.file_size for item in entries) > 40 * 1024 * 1024:
                raise ValueError(error)
            seen, parts = set(), {}
            for item in entries:
                name = item.filename
                lower = name.casefold()
                components = name.rstrip('/').split('/')
                if (not name or item.orig_filename != name or name.startswith('/') or '\\' in name or '\x00' in name or ':' in name
                        or any(part in ('', '.', '..') for part in components) or lower in seen
                        or item.flag_bits & 1 or item.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED)
                        or ((item.external_attr >> 16) & 0o170000) == 0o120000
                        or item.file_size > 16 * 1024 * 1024
                        or item.file_size > max(1, item.compress_size) * 1000
                        or 'vba' in lower or '/activex/' in lower or '/embeddings/' in lower
                        or (lower.endswith('.bin') and not lower.startswith('word/printersettings/'))):
                    raise ValueError(error)
                seen.add(lower)
                if item.is_dir():
                    continue
                limit = (4 * 1024 * 1024 if name == 'word/document.xml' else
                         1024 * 1024 if name == '[Content_Types].xml' or lower.endswith('.rels') else
                         16 * 1024 * 1024)
                if item.file_size > limit:
                    raise ValueError(error)
                with archive.open(item) as member:
                    content = member.read(limit + 1)
                if len(content) != item.file_size or len(content) > limit:
                    raise ValueError(error)
                if name in ('[Content_Types].xml', 'word/document.xml'):
                    parts[name] = content
                if lower.endswith('.rels'):
                    relationships = _contract_xml(content)
                    if relationships.tag != '{' + _OOXML_RELS + '}Relationships':
                        raise ValueError(error)
                    if any('vba' in child.get('Type', '').casefold()
                           or 'macro' in child.get('Type', '').casefold()
                           or 'activex' in child.get('Type', '').casefold()
                           or 'oleobject' in child.get('Type', '').casefold()
                           for child in relationships):
                        raise ValueError(error)
            types = _contract_xml(parts['[Content_Types].xml'])
            if types.tag != '{' + _OOXML_TYPES + '}Types':
                raise ValueError(error)
            main = []
            for child in types:
                content_type = child.get('ContentType', '')
                if any(marker in content_type.casefold() for marker in ('macro', 'vba', 'activex', 'oleobject')):
                    raise ValueError(error)
                if child.get('PartName') == '/word/document.xml':
                    if child.tag != '{' + _OOXML_TYPES + '}Override':
                        raise ValueError(error)
                    main.append(content_type)
            if main != [_DOCX_MAIN_TYPE]:
                raise ValueError(error)
            document = _contract_xml(parts['word/document.xml'])
            namespaces = ('http://schemas.openxmlformats.org/wordprocessingml/2006/main',
                          'http://purl.oclc.org/ooxml/wordprocessingml/main')
            if not any(document.tag == '{' + ns + '}document'
                       and document.find('{' + ns + '}body') is not None for ns in namespaces):
                raise ValueError(error)
    except (ValueError, KeyError, zipfile.BadZipFile, zipfile.LargeZipFile, RuntimeError,
            NotImplementedError, OSError, ElementTree.ParseError, zlib.error):
        raise ValueError(error) from None


def _validate_repaired_contract_pdf(raw, document):
    """Accept an intact original only when a second strict parser agrees.

    MuPDF can flag historical xref numbering as repaired even though pypdf can
    read every original page strictly. No repaired/rewritten bytes are saved.
    """
    try:
        with io.BytesIO(raw) as stream:
            reader = PdfReader(stream, strict=True)
            if reader.is_encrypted:
                raise ValueError()
            root = reader.root_object
            tree = root.get('/Pages').get_object()
            pages = reader.pages
            if (root.get('/Type') != '/Catalog' or not isinstance(tree, DictionaryObject)
                    or tree.get('/Type') != '/Pages' or tree.get('/Count') != document.page_count
                    or len(pages) != document.page_count or not 1 <= len(pages) <= 100):
                raise ValueError()
            total_contents = 0
            for number, page in enumerate(pages):
                box = [float(value) for value in page.mediabox]
                if (page.get('/Type') != '/Page' or len(box) != 4
                        or not all(math.isfinite(value) for value in box)
                        or not 0 < box[2] - box[0] <= 14400
                        or not 0 < box[3] - box[1] <= 14400):
                    raise ValueError()
                resources = page.get('/Resources')
                if resources is not None and not isinstance(resources.get_object(), DictionaryObject):
                    raise ValueError()
                contents = page.get('/Contents')
                if contents is not None:
                    contents = contents.get_object()
                    members = contents if isinstance(contents, ArrayObject) else [contents]
                    if len(members) > 128:
                        raise ValueError()
                    page_contents = 0
                    for member in members:
                        member = member.get_object()
                        if not isinstance(member, StreamObject):
                            raise ValueError()
                        data = member.get_data()
                        page_contents += len(data)
                        total_contents += len(data)
                        if page_contents > 1024 * 1024 or total_contents > 8 * 1024 * 1024:
                            raise ValueError()
                    # Resolve and parse every page's operators, without executing
                    # PDF actions, OCR or extracting private text.
                    parsed = page.get_contents()
                    if parsed is None or len(parsed.operations) > 100_000:
                        raise ValueError()
                mupdf_page = document.load_page(number)
                scale = 128 / max(mupdf_page.rect.width, mupdf_page.rect.height)
                thumbnail = mupdf_page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csGRAY, alpha=False)
                if (not 0 < thumbnail.width <= 129 or not 0 < thumbnail.height <= 129
                        or not thumbnail.samples):
                    raise ValueError()
    except Exception:
        raise ValueError('Das PDF-Original ist nicht mit zwei unabhängigen Prüfungen vollständig lesbar.') from None


def _document(file, *, label='Lohnzettel', allow_docx=False, allow_repaired_pdf=False):
    formats = 'PDF, DOCX, JPEG oder PNG' if allow_docx else 'PDF, JPEG oder PNG'
    if file is None or not file.filename:
        selection = 'Einen Lohnzettel' if label == 'Lohnzettel' else label
        raise ValueError(f'{selection} als {formats} auswählen.')
    raw = file.stream.read(MAX_DOCUMENT_BYTES + 1)
    if not raw or len(raw) > MAX_DOCUMENT_BYTES:
        raise ValueError(f'{label} darf höchstens 10 MB groß sein.')
    supplied = str(file.filename).replace('\\', '/').rsplit('/', 1)[-1]
    filename = secure_filename(supplied)[:160]
    extension = Path(filename).suffix.lower()
    mime = ''
    try:
        if allow_docx and extension == '.docx':
            _validate_docx(raw)
            mime = DOCX_MIME
        elif raw.startswith(b'%PDF-') and extension == '.pdf':
            with fitz.open(stream=raw, filetype='pdf') as document:
                if (document.is_encrypted or not 1 <= document.page_count <= 100
                        or (document.is_repaired and not allow_repaired_pdf)):
                    raise ValueError('PDF muss vollständig lesbar sein und 1 bis 100 Seiten enthalten.')
                for number in range(document.page_count):
                    page = document.load_page(number)
                    if page.rect.is_empty or page.rect.is_infinite:
                        raise ValueError('PDF enthält eine ungültige Seite.')
                if document.is_repaired:
                    _validate_repaired_contract_pdf(raw, document)
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
            raise ValueError(f'Dateityp und Dateiendung müssen zu {formats} passen.')
    except (fitz.FileDataError, OSError, SyntaxError, Image.DecompressionBombError, Image.DecompressionBombWarning):
        raise ValueError(f'{label} ist beschädigt oder keine lesbare Datei ({formats}).') from None
    return raw, filename, mime


def _contract_document(file):
    return _document(file, label='Personalunterlage', allow_docx=True, allow_repaired_pdf=True)


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
                UNIQUE(start_datum,end_datum,notiz));
                CREATE TABLE IF NOT EXISTS mitarbeiter_arbeitsvertraege (
                id INTEGER PRIMARY KEY AUTOINCREMENT, mitarbeiter_id INTEGER NOT NULL,
                titel TEXT NOT NULL DEFAULT '', filename TEXT NOT NULL, mime TEXT NOT NULL,
                size_bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, original_base64 TEXT NOT NULL,
                created_at TEXT NOT NULL, created_by TEXT NOT NULL,
                UNIQUE(mitarbeiter_id,sha256));
                CREATE INDEX IF NOT EXISTS idx_mitarbeiter_arbeitsvertraege_owner
                ON mitarbeiter_arbeitsvertraege(mitarbeiter_id,id);''')
            for column, definition in {**PRIVATE_PROFILE_COLUMNS, **WORK_PLAN_COLUMNS}.items():
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
        # No tax numbers, private address, filename or document bytes in audit.
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

    def _contracts(self, db, mid, *, admin=False):
        rows = db.execute('''SELECT id,titel,filename,mime,size_bytes,created_at
            FROM mitarbeiter_arbeitsvertraege WHERE mitarbeiter_id=? ORDER BY id DESC''', (mid,)).fetchall()
        return [dict(row, bytes=row['size_bytes'], url=(f'/admin/mitarbeiter/{mid}/portal/arbeitsvertrag/'
                    if admin else '/werkstatt/mein-konto/arbeitsvertrag/') + str(row['id'])) for row in rows]

    def admin_view(self, mid):
        self._admin()
        with self.db() as db:
            employee = self._employee(db, mid)
            profile = self._profile(db, mid)
            return {'employee': employee, 'profile': profile, 'arbeitsplan': _plan_view(profile),
                    'payrolls': self._payrolls(db, mid, admin=True),
                    'contracts': self._contracts(db, mid, admin=True)}

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
                      'profile': profile, 'arbeitsplan': _plan_view(profile), 'payrolls': self._payrolls(db, mid),
                      'contracts': self._contracts(db, mid)}
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
        if (not isinstance(payload, dict) or not set(LEGACY_PROFILE_FIELDS).issubset(payload)
                or set(payload) - set(PROFILE_FIELDS)):
            raise ValueError('Nur die vorgesehenen persönlichen Profilfelder angeben.')
        data = {key: _text(value, PROFILE_LIMITS[key], key, multiline=key == 'adresse')
                for key, value in payload.items()}
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
        if data.get('sozialversicherungsnummer') and not re.fullmatch(r'[0-9A-Za-z /-]{1,20}', data['sozialversicherungsnummer']):
            raise ValueError('Sozialversicherungsnummer nur mit Buchstaben, Ziffern und üblichen Trennzeichen angeben.')
        if data.get('steuerklasse') and not re.fullmatch(r'[1-6]', data['steuerklasse']):
            raise ValueError('Steuerklasse muss 1 bis 6 sein oder leer bleiben.')
        if data.get('eintrittsdatum'):
            _iso_date(data['eintrittsdatum'], 'Eintrittsdatum')
        with self.p.portal_originals_operation_lock(), self.db() as db:
            self._employee(db, mid)
            previous = self._profile(db, mid)
            # A previously opened seven-field form must not erase newly saved data.
            data = {key: data[key] if key in data else previous.get(key, '') for key in PROFILE_FIELDS}
            columns = ','.join(PROFILE_FIELDS)
            updates = ','.join(key + '=excluded.' + key for key in PROFILE_FIELDS if key in payload)
            db.execute('INSERT INTO mitarbeiter_portal_profile(mitarbeiter_id,' + columns + ',updated_at,updated_by) '
                       'VALUES(' + ','.join('?' for _ in range(len(PROFILE_FIELDS) + 3)) + ') ON CONFLICT(mitarbeiter_id) DO UPDATE SET '
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

    def upload_contract(self, mid, titel, file):
        """Keep each contract/addendum original, without a fabricated date or month."""
        self._admin()
        titel = _text(titel, 120, 'Dokumentname')
        raw, filename, mime = _contract_document(file)
        digest = hashlib.sha256(raw).hexdigest()
        with self.p.portal_originals_operation_lock(), self.db() as db:
            self._employee(db, mid)
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (mid,))
            prior = db.execute('SELECT id FROM mitarbeiter_arbeitsvertraege WHERE mitarbeiter_id=? AND sha256=?',
                               (mid, digest)).fetchone()
            if prior:
                return prior['id']
            row = db.execute('''INSERT INTO mitarbeiter_arbeitsvertraege
                (mitarbeiter_id,titel,filename,mime,size_bytes,sha256,original_base64,created_at,created_by)
                VALUES(?,?,?,?,?,?,?,?,?) RETURNING id''',
                (mid, titel, filename, mime, len(raw), digest, base64.b64encode(raw).decode('ascii'),
                 self.p.now_str(), 'admin')).fetchone()
            contract_id = row['id']
            self._audit(db, 'mitarbeiter_portal_arbeitsvertrag_hinterlegt', mid, arbeitsvertrag=contract_id)
        self._backup()
        return contract_id

    def contract(self, contract_id, *, admin_mid=None):
        if type(contract_id) is not int or not 1 <= contract_id <= 2147483647:
            raise LookupError('Personalunterlage nicht gefunden.')
        with self.db() as db:
            if admin_mid is not None:
                self._admin()
                mid = self._employee(db, admin_mid)['id']
            else:
                who = self.identity(db)
                if not who:
                    raise LookupError('Personalunterlage nicht gefunden.')
                mid = who['mitarbeiter_id']
            row = db.execute('SELECT * FROM mitarbeiter_arbeitsvertraege WHERE id=? AND mitarbeiter_id=?',
                             (contract_id, mid)).fetchone()
            if not row:
                raise LookupError('Personalunterlage nicht gefunden.')
            row = dict(row)
        try:
            raw = base64.b64decode(row['original_base64'], validate=True)
            if (not 0 < len(raw) <= MAX_DOCUMENT_BYTES or len(raw) != row['size_bytes']
                    or hashlib.sha256(raw).hexdigest() != row['sha256']
                    or row['mime'] not in ('application/pdf', 'image/png', 'image/jpeg', DOCX_MIME)):
                raise ValueError()
        except (ValueError, TypeError):
            raise LookupError('Personalunterlage nicht gefunden.') from None
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
                service.save_profile(mid, {key: request.form.get(key, '') for key in PROFILE_FIELDS
                                          if key in LEGACY_PROFILE_FIELDS or key in request.form})
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

    @bp.post('/admin/mitarbeiter/<int:mid>/portal/arbeitsvertrag')
    @p.admin_required
    def admin_contract_upload(mid):
        csrf()
        try:
            if (set(request.form) - {'csrf_token', 'titel'} or len(request.form.getlist('titel')) > 1
                    or set(request.files) != {'file'} or len(request.files.getlist('file')) != 1):
                raise ValueError('Genau eine Personalunterlage und einen optionalen Dokumentnamen angeben.')
            service.upload_contract(mid, request.form.get('titel', ''), request.files.get('file'))
            flash('Personalunterlage im persönlichen Profil hinterlegt.', 'success')
            return redirect(f'/admin/mitarbeiter/{mid}/portal#arbeitsvertraege', code=303)
        except LookupError:
            abort(404)
        except ValueError as exc:
            try:
                data = service.admin_view(mid)
            except LookupError:
                abort(404)
            return render_template('mitarbeiter_portal_admin.html', **data, csrf_token=token(), error=str(exc)), 400

    def download_contract(contract_id, admin_mid=None):
        try:
            row, raw = service.contract(contract_id, admin_mid=admin_mid)
        except LookupError:
            abort(404)
        return send_file(io.BytesIO(raw), mimetype=row['mime'], as_attachment=True,
                         download_name=secure_filename(row['filename']) or 'Personalunterlage', etag=False, conditional=False)

    @bp.get('/werkstatt/mein-konto/arbeitsvertrag/<int:contract_id>')
    def personal_contract(contract_id):
        return download_contract(contract_id)

    @bp.get('/admin/mitarbeiter/<int:mid>/portal/arbeitsvertrag/<int:contract_id>')
    @p.admin_required
    def admin_contract(mid, contract_id):
        return download_contract(contract_id, mid)

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
        mids = {row['mitarbeiter_id'] for table in OWNER_TABLES for row in protected.get(table, [])}
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
                    # Older backups predate these optional private columns. A
                    # missing empty migration default cannot discard real data.
                    if (table == 'mitarbeiter_portal_profile' and column in PRIVATE_PROFILE_COLUMNS
                            and column not in restored and original == ''):
                        continue
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
