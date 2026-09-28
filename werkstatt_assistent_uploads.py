"""Private, durable avatar uploads; analysis never edits an order.

The caller must obtain actor_id from current authenticated assistant identity,
check current capabilities and CSRF, and require deliberate human confirmation
before attach(..., confirmed=True). Never expose file_base64 or OCR raw text to
the model. An external db lets confirmed order creation and attachment share a
transaction; the caller owns commit/rollback. Original bytes survive restarts in
the staging table and datei_backups, even if a filesystem write is interrupted.
"""
import base64
import hashlib
import io
import json
import re
import secrets
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from PIL import Image, UnidentifiedImageError
from werkzeug.utils import secure_filename

MAX_BYTES = 8 * 1024 * 1024
PURPOSES = {'fahrzeugschein', 'schaden', 'angebot', 'sonstiges'}
FORMATS = {'JPEG': ('.jpg', 'image/jpeg'), 'PNG': ('.png', 'image/png'),
           'WEBP': ('.webp', 'image/webp'), 'GIF': ('.gif', 'image/gif'),
           'TIFF': ('.tif', 'image/tiff'), 'BMP': ('.bmp', 'image/bmp')}
FIELD_LIMITS = {'fahrzeug': 120, 'kennzeichen': 24, 'fin_nummer': 17,
                'hsn_nummer': 4, 'tsn_nummer': 8, 'farbcode': 80,
                'farbton': 120, 'farbton_2': 120, 'beschreibung': 1600,
                'analyse_text': 600, 'bauteile_override': 600}
PRIVATE_CATEGORY = 'assistent'
COMMERCIAL_EXCLUDED = re.compile(r'\b(?:rechnung\w*|invoice|gutschrift\w*|kontoauszug\w*|'
                                 r'bilanz\w*|buchhaltung\w*|gewinn\w*|liquidität\w*|lohn\w*|banking)\b', re.I)
SENSITIVE = re.compile(
    r'\b(?:iban|bic|swift|bank\w*|konto\w*|blz|sepa|lastschrift|mandatsreferenz|'
    r'kreditkart\w*|gesamt\w*|netto|brutto|umsatz\w*|steuer\w*|mehrwertsteuer|'
    r'rechnungsbetrag|zahlung\w*|fälliger?\s+betrag|saldo|gewinn|liquidität)\b|'
    r'\b[A-Z]{2}\s*\d{2}(?:[ \t]?[A-Z0-9]){11,30}\b|€|\bEUR\b', re.I)


def is_assistant_private(document):
    return str((document or {}).get('kategorie') or '').strip().lower() == PRIVATE_CATEGORY


def ensure_upload_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS assistent_uploads (
        id INTEGER PRIMARY KEY AUTOINCREMENT, upload_id TEXT NOT NULL UNIQUE,
        actor TEXT NOT NULL, request_id TEXT NOT NULL, original_name TEXT NOT NULL,
        mime_type TEXT NOT NULL, suffix TEXT NOT NULL, size INTEGER NOT NULL,
        file_sha256 TEXT NOT NULL, file_base64 TEXT NOT NULL, zweck TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'bereit', analyse_json TEXT NOT NULL DEFAULT '{}',
        lease_token TEXT NOT NULL DEFAULT '', lease_until REAL NOT NULL DEFAULT 0,
        auftrag_id INTEGER, datei_id INTEGER, erstellt_am TEXT NOT NULL,
        UNIQUE(actor, request_id))''')


def _safe_text(value, limit):
    if not isinstance(value, str):
        return ''
    return '\n'.join(line for line in value.splitlines() if not SENSITIVE.search(line))[:limit].strip()


def _offer_text(text, structured, filename):
    # A consciously labelled quote may contain prices, but invoice/accounting
    # documents must go through their separate authorized product reader.
    from werkstatt_cockpit_api import _without_bank_lines
    candidate = text or '\n'.join(value for key in ('beschreibung', 'analyse_text')
                                 if isinstance(value := structured.get(key), str))
    if COMMERCIAL_EXCLUDED.search(filename + '\n' + candidate):
        return ''
    return _without_bank_lines(candidate)[:12000].strip()


def _validated_file(raw, name):
    suffix = Path(name).suffix.lower()
    if suffix == '.pdf':
        import fitz
        try:
            with fitz.open(stream=raw, filetype='pdf') as document:
                if document.needs_pass or not 1 <= document.page_count <= 30:
                    raise ValueError('PDF muss unverschlüsselt sein und darf höchstens 30 Seiten haben.')
                for index in range(1, document.xref_length()):
                    obj = document.xref_object(index)
                    if re.search(r'/(?:JavaScript|JS|Launch|EmbeddedFile|OpenAction|AA)\b', obj):
                        raise ValueError('PDF mit aktiven Inhalten oder eingebetteten Dateien ist nicht erlaubt.')
        except (RuntimeError, ValueError) as exc:
            raise ValueError('Keine geeignete PDF-Datei. Bitte ein normales PDF oder Bild verwenden.') from exc
        return '.pdf', 'application/pdf'
    if suffix not in {'.jpg', '.jpeg', '.png', '.webp', '.gif', '.tif', '.tiff', '.bmp'}:
        raise ValueError('Bitte JPEG, PNG, WebP oder PDF auswählen; Office-Dateien werden nicht übernommen.')
    try:
        with Image.open(io.BytesIO(raw)) as image:
            if image.format not in FORMATS or image.width * image.height > 20_000_000:
                raise ValueError('Bildformat oder Bildauflösung nicht unterstützt.')
            if getattr(image, 'n_frames', 1) != 1:
                raise ValueError('Bitte ein einzelnes, nicht animiertes Bild auswählen.')
            actual = FORMATS[image.format]
            image.verify()
        with Image.open(io.BytesIO(raw)) as image:
            image.load()
        return actual
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError) as exc:
        raise ValueError('Keine gültige Bilddatei.') from exc


class AssistantUploads:
    def __init__(self, portal):
        self.p = portal
        self.init_schema()

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def init_schema(self):
        with self.db() as db:
            ensure_upload_schema(db)

    def _actor(self, actor):
        if not isinstance(actor, str) or not re.fullmatch(r'admin|mitarbeiter:[1-9][0-9]{0,12}', actor):
            raise ValueError('Persönlicher Zugang fehlt.')
        return actor

    def _row(self, db, actor, upload_id):
        self._actor(actor)
        if not isinstance(upload_id, str) or not re.fullmatch(r'[0-9a-f]{32}', upload_id):
            raise ValueError('Upload nicht gefunden.')
        row = db.execute('SELECT * FROM assistent_uploads WHERE upload_id=? AND actor=?',
                         (upload_id, actor)).fetchone()
        if not row:
            raise ValueError('Upload nicht gefunden.')
        return dict(row)

    def _view(self, row):
        try:
            analysis = json.loads(row['analyse_json'])
        except (ValueError, TypeError):
            analysis = {}
        return {'id': row['upload_id'], 'original_name': row['original_name'],
                'mime_type': row['mime_type'], 'size': row['size'], 'zweck': row['zweck'],
                'status': row['status'], 'auftrag_id': row.get('auftrag_id'),
                'datei_id': row.get('datei_id'), 'erstellt_am': row['erstellt_am'],
                'analyse': analysis or {'felder': {}, 'hinweise': ['Noch nicht ausgewertet.']}}

    def stage(self, actor_id, file, request_id, purpose='sonstiges'):
        self._actor(actor_id)
        if not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9-]{16,80}', request_id):
            raise ValueError('Eindeutige Upload-Vorgangsnummer fehlt.')
        if purpose not in PURPOSES:
            raise ValueError('Bitte den Zweck des Uploads auswählen.')
        if not file or not getattr(file, 'filename', ''):
            raise ValueError('Datei fehlt.')
        raw = file.read(MAX_BYTES + 1)
        if not raw or len(raw) > MAX_BYTES:
            raise ValueError('Datei leer oder größer als 8 MB.')
        name = secure_filename(file.filename)[:160]
        suffix, mime = _validated_file(raw, name)
        # Canonical extension avoids interpreting a disguised image as another format.
        name = (Path(name).stem or 'Upload')[:120] + suffix
        digest = hashlib.sha256(raw).hexdigest()
        with self.db() as db:
            existing = db.execute('SELECT * FROM assistent_uploads WHERE actor=? AND request_id=?',
                                  (actor_id, request_id)).fetchone()
            if existing:
                if existing['file_sha256'] != digest or existing['zweck'] != purpose:
                    raise ValueError('Diese Vorgangsnummer gehört zu einer anderen Datei.')
                return self._view(dict(existing))
            pending = db.execute('SELECT COUNT(*) AS n FROM assistent_uploads WHERE actor=? AND datei_id IS NULL',
                                 (actor_id,)).fetchone()['n']
            if pending >= 20:
                raise ValueError('Bitte vorhandene Uploads zuerst zuordnen; maximal 20 offene Dateien.')
            token = secrets.token_hex(16)
            db.execute('''INSERT INTO assistent_uploads(upload_id,actor,request_id,original_name,
                mime_type,suffix,size,file_sha256,file_base64,zweck,erstellt_am)
                VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(actor,request_id) DO NOTHING''',
                (token, actor_id, request_id, name, mime, suffix, len(raw), digest,
                 base64.b64encode(raw).decode('ascii'), purpose, self.p.now_str()))
            row = dict(db.execute('SELECT * FROM assistent_uploads WHERE actor=? AND request_id=?',
                                  (actor_id, request_id)).fetchone())
            if row['file_sha256'] != digest or row['zweck'] != purpose:
                raise ValueError('Diese Vorgangsnummer gehört zu einer anderen Datei.')
            return self._view(row)

    def get(self, actor_id, upload_id):
        with self.db() as db:
            return self._view(self._row(db, actor_id, upload_id))

    def list(self, actor_id, limit=5):
        self._actor(actor_id)
        limit = min(max(int(limit), 1), 20)
        with self.db() as db:
            return [self._view(dict(row)) for row in db.execute(
                'SELECT * FROM assistent_uploads WHERE actor=? ORDER BY id DESC LIMIT ?',
                (actor_id, limit)).fetchall()]

    def _bytes(self, row):
        try:
            raw = base64.b64decode(row['file_base64'], validate=True)
        except (ValueError, TypeError) as exc:
            raise ValueError('Gespeichertes Original ist nicht verfügbar.') from exc
        if not raw or len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != row['file_sha256']:
            raise ValueError('Gespeichertes Original ist beschädigt; bitte erneut hochladen.')
        return raw

    def read_content(self, actor_id, upload_id):
        with self.db() as db:
            row = self._row(db, actor_id, upload_id)
            return self._bytes(row), row['mime_type'], row['original_name']

    def analyze(self, actor_id, upload_id):
        lease = secrets.token_hex(16)
        with self.db() as db:
            row = self._row(db, actor_id, upload_id)
            if row['status'] in {'pruefen', 'zugeordnet'}:
                return self._view(row)
            cursor = db.execute('''UPDATE assistent_uploads SET status='analyse',lease_token=?,lease_until=?
                WHERE upload_id=? AND actor=? AND datei_id IS NULL AND lease_until<?''',
                (lease, time.time() + 300, upload_id, actor_id, time.time()))
            if cursor.rowcount != 1:
                return self._view(self._row(db, actor_id, upload_id))
        try:
            raw = self._bytes(row)
            with tempfile.TemporaryDirectory(prefix='avatar-ocr-') as temporary:
                path = Path(temporary) / ('original' + row['suffix'])
                path.write_bytes(raw)
                bundle = self.p.build_document_analysis_bundle_safe(path, row['original_name'])
            if not isinstance(bundle, dict):
                bundle = {}
            text = bundle.get('text') if isinstance(bundle.get('text'), str) else ''
            structured = bundle.get('structured') if isinstance(bundle.get('structured'), dict) else {}
            local = self.p.parse_document_fields(_safe_text(text, 100000), row['original_name']) if text else {}
            fields = {}
            for key, length in FIELD_LIMITS.items():
                value = _safe_text(structured.get(key) or local.get(key), length)
                if key == 'fin_nummer' and not re.fullmatch(r'[A-HJ-NPR-Z0-9]{17}', value.upper()):
                    value = ''
                if key == 'hsn_nummer' and not re.fullmatch(r'[0-9]{4}', value):
                    value = ''
                if key == 'tsn_nummer' and not re.fullmatch(r'[A-Za-z0-9]{3,8}', value):
                    value = ''
                if value:
                    fields[key] = value
            if row['zweck'] == 'fahrzeugschein':
                # Merely propose the explicitly printed holder. The confirmed
                # intake remains the place to decide who actually orders work.
                name = ''
                for source in (structured, local):
                    for key in ('kunde_name', 'anspruchsteller_name', 'halter_name', 'halter',
                                'fahrzeughalter', 'zulassungsinhaber', 'versicherungsnehmer'):
                        name = _safe_text(source.get(key), 180)
                        if name:
                            break
                    if name:
                        break
                parser = getattr(self.p, 'fahrzeugverkauf_extract_customer_name', None)
                if not name and callable(parser):
                    name = _safe_text(parser(_safe_text(text, 100000)), 180)
                if name and '\n' not in name and not any(ord(char) < 32 for char in name):
                    fields['kunde_name'] = name
            redacted = bool(SENSITIVE.search(text) or any(
                isinstance(value, str) and SENSITIVE.search(value) for value in structured.values()))
            analysis = {'felder': fields, 'hinweise': [
                'Automatische Erkennung ist ein Vorschlag. Bitte am Original prüfen; noch keine Auftragsdaten geändert.'],
                'quelle': 'Upload ' + upload_id, 'bankdaten_entfernt': redacted, 'pruefen': True}
            if row['zweck'] == 'fahrzeugschein':
                analysis['hinweise'].append('Halter ist nicht automatisch Auftraggeber. Kundendaten und Auftrag müssen gesondert bestätigt werden.')
                if fields.get('kunde_name'):
                    analysis['hinweise'].append('Vorgeschlagener Auftraggeber stammt aus der Halterangabe im Fahrzeugschein. Namen am Original prüfen und tatsächlichen Auftraggeber bestätigen.')
            if row['zweck'] == 'angebot':
                analysis['angebotsinhalt'] = _offer_text(text, structured, row['original_name'])
                analysis['hinweise'].append('Angebotsauslese ungeprüft; Original und Umfang kontrollieren. Kein bestätigter Verkaufspreis.')
                if not analysis['angebotsinhalt']:
                    analysis['hinweise'].append('Kein zulässiger Angebotsinhalt erkannt. Rechnungs- und Buchhaltungsdokumente werden hier nicht ausgegeben.')
            if redacted:
                analysis['hinweise'].append('Bank- und Buchhaltungsangaben wurden nicht in den Vorschlag übernommen.')
            if not fields:
                analysis['hinweise'].append('Keine sicheren Fahrzeug- oder Arbeitsangaben erkannt. Bitte Angaben selbst ergänzen.')
        except Exception:
            analysis = {'felder': {}, 'hinweise': ['Auswertung nicht verfügbar. Original ist gespeichert; Angaben bitte selbst prüfen.'], 'pruefen': True}
        with self.db() as db:
            db.execute('''UPDATE assistent_uploads SET status='pruefen',analyse_json=?,lease_token='',lease_until=0
                WHERE upload_id=? AND actor=? AND lease_token=? AND datei_id IS NULL''',
                (json.dumps(analysis, ensure_ascii=False), upload_id, actor_id, lease))
            return self._view(self._row(db, actor_id, upload_id))

    def attach(self, actor_id, upload_id, auftrag_id, *, confirmed=False, db=None):
        if confirmed is not True:
            raise ValueError('Die Zuordnung muss ausdrücklich bestätigt werden.')
        if not isinstance(auftrag_id, int) or isinstance(auftrag_id, bool) or auftrag_id <= 0:
            raise ValueError('Bitte einen gespeicherten Auftrag auswählen.')
        if db is None:
            with self.db() as connection:
                return self.attach(actor_id, upload_id, auftrag_id, confirmed=True, db=connection)
        row = self._row(db, actor_id, upload_id)
        # Lock this row portably before checking idempotency/creating any file record.
        db.execute('UPDATE assistent_uploads SET upload_id=upload_id WHERE upload_id=? AND actor=?',
                   (upload_id, actor_id))
        row = self._row(db, actor_id, upload_id)
        if row['datei_id']:
            existing = db.execute('SELECT id FROM dateien WHERE id=? AND auftrag_id=?',
                                  (row['datei_id'], auftrag_id)).fetchone()
            if row['auftrag_id'] != auftrag_id or not existing:
                raise ValueError('Upload bereits anders zugeordnet oder Anhang gelöscht. Bitte neu hochladen.')
            return {'upload_id': upload_id, 'datei_id': row['datei_id'], 'auftrag_id': auftrag_id, 'duplicate': True}
        order = db.execute('SELECT id,archiviert FROM auftraege WHERE id=?', (auftrag_id,)).fetchone()
        if not order or order['archiviert']:
            raise ValueError('Aktiver Auftrag nicht gefunden.')
        if row['status'] != 'pruefen':
            raise ValueError('Bitte zuerst die Auswertung abwarten und prüfen.')
        raw = self._bytes(row)
        name = 'assistant-' + upload_id + row['suffix']
        target = self.p.UPLOAD_DIR / name
        target.parent.mkdir(parents=True, exist_ok=True)
        # The generated path is never supplied by the client. No data is exposed
        # without the committed dateien row; rollback may leave only an orphan.
        target.write_bytes(raw)
        analysis = json.loads(row['analyse_json'])
        fields = analysis.get('felder', {})
        offer_text = analysis.get('angebotsinhalt', '') if row['zweck'] == 'angebot' else ''
        cursor = db.execute('''INSERT INTO dateien(auftrag_id,original_name,stored_name,mime_type,size,
            quelle,kategorie,dokument_zweck,kunde_sichtbar,partner_sichtbar,versicherung_sichtbar,
            sichtbarkeit_geprueft,dokument_typ,notiz,extrahierter_text,extrakt_kurz,analyse_quelle,analyse_json,analyse_hinweis,hochgeladen_am)
            VALUES(?,?,?,?,?,'intern','assistent',?,0,0,0,1,?,?,?,?,?,?,?,?)''',
            (auftrag_id, row['original_name'], name, row['mime_type'], len(raw), row['zweck'],
             'Angebot (ungeprüft)' if offer_text else 'Assistent-Unterlage',
             'Intern; vom Mitarbeiter bewusst zugeordnet. Keine Reparaturfreigabe.', offer_text,
             fields.get('analyse_text', ''), 'assistent_upload_v1', json.dumps({'assistent_vorschlag': fields}, ensure_ascii=False),
             'Ungeprüfte Auslese; keine automatische Feldübernahme.', self.p.now_str()))
        file_id = cursor.lastrowid
        if not self.p.store_datei_backup(db, file_id, target):
            raise ValueError('Original konnte nicht dauerhaft gesichert werden. Zuordnung nicht gespeichert.')
        db.execute('UPDATE assistent_uploads SET status=\'zugeordnet\',auftrag_id=?,datei_id=?,lease_token=\'\',lease_until=0 WHERE upload_id=? AND actor=?',
                   (auftrag_id, file_id, upload_id, actor_id))
        return {'upload_id': upload_id, 'datei_id': file_id, 'auftrag_id': auftrag_id, 'duplicate': False}

    def attachment_resolver(self, actor_id, order_id, file_id, kind):
        self._actor(actor_id)
        if kind not in {'kunde', 'lieferant'} or not isinstance(file_id, int) or isinstance(file_id, bool):
            raise ValueError('Ungültige Anhangsauswahl.')
        with self.db() as db:
            row = db.execute('''SELECT u.* FROM assistent_uploads u JOIN dateien d ON d.id=u.datei_id
                WHERE u.actor=? AND u.auftrag_id=? AND u.datei_id=? AND d.auftrag_id=u.auftrag_id''',
                (actor_id, order_id, file_id)).fetchone()
            if not row or row['zweck'] != 'schaden' or not row['mime_type'].startswith('image/'):
                raise ValueError('Nur eigene, zugeordnete Schadenfotos sind als Mailanhang freigegeben.')
            row = dict(row)
            return {'datei_id': file_id, 'original_name': row['original_name'], 'mime_type': row['mime_type'],
                    'content': self._bytes(row), 'sha256': row['file_sha256'], 'size': row['size']}
