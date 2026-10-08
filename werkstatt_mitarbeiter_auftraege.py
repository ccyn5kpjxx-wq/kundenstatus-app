"""Personal employee orders: exact IDs, scoped progress and internal work photos.

No shared workshop login, notification, AI or financial hydration is used. The
separate operations gate authorizes only these routes; it never widens stored
assistant permissions. Confirmations use server-held state and durable audit
keys. Work documents are inspected locally and private originals remain intact.
"""
import base64
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

import fitz
from flask import Blueprint, abort, flash, redirect, render_template, request, send_file, session
from PIL import Image, ImageOps
from werkzeug.utils import secure_filename

from werkstatt_auftrag_ausdruck import _date, _time, _work_text
from werkstatt_cockpit_api import _BANK_DATA, _without_bank_lines
from werkstatt_fortschritt import NATIVE_ACTION_LABELS, ProgressError, WorkshopProgress, _snapshot


MAX_PHOTO_BYTES = 8 * 1024 * 1024
MAX_DOCUMENT_BYTES = 20 * 1024 * 1024
MAX_PHOTOS = 6
_ID = re.compile(r'[1-9][0-9]{0,12}')
_EXCLUDED = re.compile(r'rechnung|invoice|gutschrift|kontoauszug|banking|buchhaltung|bilanz|lohn|gehalt|payroll|steuerbescheid', re.I)
_MONEY = re.compile(r'\b(?:netto|brutto|gesamtbetrag|umsatz|saldo|gewinn|rechnungsbetrag)\b|€|\bEUR\b|\b(?:Preis|Kosten|Betrag)\s*[:\d]', re.I)
_FIELDS = ('id', 'fahrzeug', 'kennzeichen', 'auftragsnummer', 'beschreibung', 'analyse_text',
           'analyse_pruefen', 'analyse_hinweis', 'status', 'produktion_schritt', 'farbcode', 'farbton',
           'farbton_2', 'lackierbereit', 'annahme_datum', 'annahme_uhrzeit', 'start_datum',
           'fertig_datum', 'fertig_uhrzeit', 'abholtermin', 'abhol_uhrzeit', 'transport_art',
           'archiviert', 'geaendert_am', 'versicherung_id', 'versicherung_freigabe_status', 'schaden_eigenauftrag',
           'lackierbereit_am', 'fahrzeug_abholbereit', 'fahrzeug_abholbereit_am')
_FILE_FIELDS = ('id', 'auftrag_id', 'original_name', 'stored_name', 'mime_type', 'size', 'quelle',
                'kategorie', 'dokument_typ', 'dokument_zweck', 'extrahierter_text', 'extrakt_kurz',
                'analyse_json', 'analyse_hinweis', 'sichtbarkeit_geprueft', 'hochgeladen_am')
_ACTIONS = tuple(NATIVE_ACTION_LABELS)


def _id(value):
    if type(value) is int and 1 <= value <= 9999999999999:
        return value
    if isinstance(value, str) and _ID.fullmatch(value):
        return int(value)
    raise ValueError('Bitte nur die interne Auftragsnummer eingeben, zum Beispiel 102.')


def _safe(value, maximum=3000):
    value = '\n'.join(line for line in str(value or '').splitlines() if not _MONEY.search(line))
    return _work_text(_without_bank_lines(value))[:maximum]


def _photo(file):
    """Reencode pixels: no EXIF/GPS, active payload or misleading MIME survives."""
    if not file or not file.filename:
        raise ValueError('Ein Foto als JPEG oder PNG auswählen.')
    raw = file.stream.read(MAX_PHOTO_BYTES + 1)
    if not raw or len(raw) > MAX_PHOTO_BYTES:
        raise ValueError('Jedes Foto darf höchstens 8 MB groß sein.')
    name = secure_filename(str(file.filename).replace('\\', '/').rsplit('/', 1)[-1])[:160]
    if Path(name).suffix.lower() not in {'.jpg', '.jpeg', '.png'} or _EXCLUDED.search(name):
        raise ValueError('Nur Arbeitsfotos als JPEG oder PNG hinzufügen; keine Rechnungen oder Personalbelege.')
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error', Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as image:
                if image.format not in {'JPEG', 'PNG'} or getattr(image, 'n_frames', 1) != 1 or image.width * image.height > 20_000_000:
                    raise ValueError('Nur ein einzelnes JPEG- oder PNG-Foto mit höchstens 20 Megapixeln verwenden.')
                if (Path(name).suffix.lower() == '.png') != (image.format == 'PNG'):
                    raise ValueError('Dateiendung und Bildformat müssen zusammenpassen.')
                image.verify()
            with Image.open(io.BytesIO(raw)) as image:
                clean = ImageOps.exif_transpose(image).convert('RGB')
                target = io.BytesIO(); clean.save(target, 'JPEG', quality=90)
    except (OSError, SyntaxError, Image.DecompressionBombWarning, Image.DecompressionBombError):
        raise ValueError('Das Foto ist beschädigt oder nicht lesbar.') from None
    if len(target.getvalue()) > MAX_PHOTO_BYTES:
        raise ValueError('Das bereinigte Foto ist zu groß. Bitte eine kleinere Aufnahme verwenden.')
    return target.getvalue(), (Path(name).stem or 'Arbeitsfoto')[:120] + '.jpg', hashlib.sha256(raw).hexdigest()


def _pdf_work_copy(raw):
    """Fail closed for scans/embedded images; remove bank/cost lines locally.

    Text-only vector documents can be inspected reliably. A page image (including
    OCR under a scanned page) could contain uninspectable bank data, so such pages
    are deliberately not returned. Active content and attachments are rejected.
    Originals are neither overwritten nor cached under a public URL.
    """
    try:
        with fitz.open(stream=raw, filetype='pdf') as document:
            if document.is_encrypted or document.is_repaired or not 1 <= document.page_count <= 30:
                raise ValueError('PDF nicht verlässlich lesbar.')
            for index in range(1, document.xref_length()):
                obj = document.xref_object(index)
                if re.search(r'/(?:JavaScript|JS|Launch|EmbeddedFile|OpenAction|AA|AcroForm)\b', obj):
                    raise ValueError('PDF enthält aktive oder eingebettete Inhalte.')
            texts = []
            for page in document:
                if page.get_images(full=True) or not page.get_text().strip():
                    raise ValueError('Gescanntes PDF oder Bildanteile: vor Anzeige intern prüfen.')
                if list(page.annots() or []) or list(page.widgets() or []):
                    raise ValueError('PDF mit unprüfbaren Anmerkungen: vor Anzeige intern prüfen.')
                texts.append(page.get_text())
            # Never turn an invoice into a seemingly permissible work document.
            if _EXCLUDED.search('\n'.join(texts)):
                raise ValueError('Kaufmännischer oder persönlicher Beleg ist keine Arbeitsunterlage.')
            # Outlined characters, drawings and QR graphics are not searchable
            # text. Copy none of the original page objects; render only sanitized
            # extracted text into a new, clearly marked working document.
            if any(len(text) > 100000 for text in texts):
                raise ValueError('PDF-Seite enthält zu viel Text für eine verlässliche Arbeitskopie.')
            safe_pages = [_safe(text, 100000) for text in texts]
            if any(not text.strip() for text in safe_pages):
                raise ValueError('Keine verlässlich lesbaren Arbeitsangaben auf jeder Seite.')
        with fitz.open() as output:
            for source_index, text in enumerate(safe_pages, 1):
                lines = []
                for original in text.splitlines():
                    words = original.split()
                    current = ''
                    for word in words:
                        # Bound even a single long token by actual glyph width;
                        # no clipped/truncated repair instruction is acceptable.
                        parts, part = [], ''
                        for char in word:
                            if fitz.get_text_length(part + char, fontname='helv', fontsize=10) > 500:
                                parts.append(part); part = char
                            else:
                                part += char
                        if part:
                            parts.append(part)
                        for part in parts:
                            candidate = (current + ' ' + part).strip()
                            if fitz.get_text_length(candidate, fontname='helv', fontsize=10) > 500 and current:
                                lines.append(current); current = part
                            else:
                                current = candidate
                    lines.append(current)
                for offset in range(0, len(lines), 48):
                    page = output.new_page(width=595, height=842)
                    page.insert_text((40, 40), f'Bereinigte Arbeitskopie - Originalseite {source_index}', fontsize=12)
                    page.insert_text((40, 75), '\n'.join(lines[offset:offset + 48]), fontsize=10, lineheight=1.35)
                    page.insert_text((40, 800), 'Nur Arbeitsangaben. Original bleibt bei der Werkstattleitung.', fontsize=9)
            clean = output.tobytes(garbage=4, deflate=True)
        with fitz.open(stream=clean, filetype='pdf') as checked:
            text = '\n'.join(page.get_text() for page in checked)
            if _BANK_DATA.search(text) or _MONEY.search(text):
                raise ValueError('Bank- oder Kostenangaben konnten nicht sicher entfernt werden.')
        return clean, True
    except (fitz.FileDataError, RuntimeError):
        raise ValueError('PDF nicht verlässlich lesbar.') from None


class EmployeeOrders:
    def __init__(self, portal):
        self.p = portal
        self.progress = WorkshopProgress(portal, initialize=False)

    def identity(self, db=None):
        who = self.p.employee_portal.identity(db)
        if who is None:
            raise PermissionError('Bitte mit dem persönlichen Mitarbeiterzugang anmelden.')
        return who

    def can_edit(self):
        return self.p.app.config.get('EMPLOYEE_ORDER_OPERATIONS_ENABLED') is True

    def _writer(self):
        who = self.identity()
        if not self.can_edit():
            raise PermissionError('Auftragsänderungen sind hier noch nicht freigegeben.')
        # A route-scoped grant for these fixed actions only. Never store or expose
        # this synthetic permission, and never pass it to broader assistant tools.
        return dict(who, dokumentieren=1)

    def _order(self, oid, db=None, *, for_update=False):
        oid = _id(oid)
        if db is None:
            connection = self.p.get_db()
            try:
                return self._order(oid, connection, for_update=for_update)
            finally:
                connection.close()
        sql = 'SELECT ' + ','.join(_FIELDS) + ' FROM auftraege WHERE id=?'
        if for_update and getattr(self.p, 'USE_POSTGRES', False):
            sql += ' FOR UPDATE'
        row = db.execute(sql, (oid,)).fetchone()
        if row is None:
            raise LookupError('Auftrag nicht gefunden.')
        row = dict(row)
        if row.get('archiviert') or not self.p.werkstatt_tafel_auftrag_sichtbar(row):
            raise LookupError('Auftrag ist hier nicht als aktueller Werkstattauftrag freigegeben.')
        return row

    def _order_view(self, row):
        transport = row.get('transport_art')
        pickup, departure = ('Abholung durch uns', 'Rückbringung') if transport == 'hol_und_bring' else ('Kunde bringt', 'Kunde holt') if transport == 'standard' else ('Annahme', 'Rückgabe')
        def event(label, day, clock=''):
            return {'label': label, 'datum': _date(day), 'uhrzeit': _time(clock) if _date(day) else ''}
        status = int(row['status'] or 1)
        from werkstatt_fortschritt import STATUS_LABELS, STAGE_LABELS
        return {
            'id': row['id'], 'nummer': row['id'], 'fahrzeug': _safe(row['fahrzeug'], 120),
            'kennzeichen': _safe(row['kennzeichen'], 24), 'externe_referenz': _safe(row['auftragsnummer'], 100),
            'arbeit': _safe(row['beschreibung'], 100000), 'analyse_text': _safe(row['analyse_text'], 100000),
            'text_gekuerzt': any(len(str(row.get(key) or '')) > 100000 for key in ('beschreibung', 'analyse_text')),
            'analyse_pruefen': bool(row['analyse_pruefen']), 'analyse_hinweis': _safe(row['analyse_hinweis'], 500),
            'status': status, 'status_label': STATUS_LABELS.get(status, 'Status offen'),
            'produktion_schritt': row['produktion_schritt'] or '', 'produktion_label': STAGE_LABELS.get(row['produktion_schritt'] or '', 'Nicht hinterlegt'),
            'farbcode': _safe(row['farbcode'], 80), 'farbton': _safe(row['farbton'], 120), 'farbton_2': _safe(row['farbton_2'], 120),
            'lackierbereit': bool(row['lackierbereit']), 'geaendert_am': row['geaendert_am'],
            'termine': [event(pickup, row['annahme_datum'], row['annahme_uhrzeit']), event('Arbeitsbeginn', row['start_datum']),
                        event(departure, row['abholtermin'], row['abhol_uhrzeit'])],
            'fertig': event('Geplante Fertigstellung', row['fertig_datum'], row['fertig_uhrzeit']),
            'transport': 'Hol- und Bringservice' if transport == 'hol_und_bring' else 'Kunde bringt und holt' if transport == 'standard' else 'Transportart noch offen',
        }

    def _file(self, oid, did):
        db = self.p.get_db()
        try:
            self.identity(db); self._order(oid, db)
            row = db.execute('SELECT ' + ','.join(_FILE_FIELDS) + ' FROM dateien WHERE id=? AND auftrag_id=?', (_id(did), _id(oid))).fetchone()
        finally:
            db.close()
        if row is None:
            raise LookupError('Arbeitsunterlage nicht gefunden.')
        return dict(row)

    def _raw(self, row):
        # Existing integrity-checked DB backup is preferred. No filesystem restore
        # occurs on GET, and no client path or filename controls disk selection.
        raw = self.p.load_datei_backup_bytes(row)
        if raw is None:
            path = self.p.upload_file_path(row)
            if path is None or not path.is_file() or path.stat().st_size > MAX_DOCUMENT_BYTES:
                raise LookupError('Originaldatei nicht mehr verfügbar.')
            raw = path.read_bytes()
        if not raw or len(raw) > MAX_DOCUMENT_BYTES:
            raise ValueError('Unterlage zu groß oder leer.')
        return raw

    def content(self, oid, did):
        row = self._file(oid, did)
        metadata = '\n'.join(str(row.get(key) or '') for key in
                             ('original_name', 'kategorie', 'dokument_typ', 'extrahierter_text', 'extrakt_kurz', 'analyse_json', 'analyse_hinweis'))
        if _EXCLUDED.search(metadata) or not self.p.werkstatt_datei_sichtbar(row):
            raise LookupError('Diese Unterlage ist für das persönliche Werkstattportal gesperrt.')
        raw = self._raw(row)
        if raw.startswith(b'%PDF-') and row['mime_type'] == 'application/pdf':
            clean, changed = _pdf_work_copy(raw)
            return clean, 'application/pdf', 'Arbeitskopie-' + str(row['id']) + '.pdf' if changed else secure_filename(row['original_name']), changed
        # Standard document scans are not automatically reclassified as photos.
        if row['kategorie'] not in {'assistent', 'fertigbild', 'reklamation'} or not row['sichtbarkeit_geprueft']:
            raise ValueError('Bildunterlage vor Anzeige intern als Arbeitsfoto prüfen.')
        if row['kategorie'] == 'assistent' and row['dokument_typ'] != 'Arbeitsfoto':
            raise ValueError('Bildunterlage ist noch nicht als Arbeitsfoto geprüft.')
        if _BANK_DATA.search(metadata) or _MONEY.search(metadata):
            raise ValueError('Bildunterlage mit Bank- oder Kostenangaben wird nicht angezeigt.')
        name = row['original_name']
        class PhotoFile:
            filename = name
            stream = io.BytesIO(raw)
        clean, name, _ = _photo(PhotoFile())
        return clean, 'image/jpeg', name, False

    def _files(self, oid):
        db = self.p.get_db()
        try:
            rows = [dict(row) for row in db.execute('SELECT ' + ','.join(_FILE_FIELDS) + ' FROM dateien WHERE auftrag_id=? ORDER BY id DESC', (oid,)).fetchall()]
        finally:
            db.close()
        documents, photos = [], []
        for row in rows:
            metadata = ' '.join(str(row.get(key) or '') for key in ('original_name', 'kategorie', 'dokument_typ', 'extrahierter_text', 'extrakt_kurz', 'analyse_json'))
            # Do not even disclose private payroll/accounting titles.
            if _EXCLUDED.search(metadata) or not self.p.werkstatt_datei_sichtbar(row):
                continue
            item = {'id': row['id'], 'name': _safe(row['original_name'], 160), 'mime': '', 'url': '',
                    'download_url': '', 'arbeitskopie': False, 'gesperrt': False, 'hinweis': ''}
            try:
                _, mime, name, changed = self.content(oid, row['id'])
                item.update(mime=mime, arbeitskopie=changed,
                            url=f'/werkstatt/mein-konto/auftraege/{oid}/datei/{row["id"]}',
                            download_url=f'/werkstatt/mein-konto/auftraege/{oid}/datei/{row["id"]}?download=1',
                            hinweis='Bereinigte Arbeitskopie; Original bleibt bei der Werkstattleitung.' if changed else '')
            except (LookupError, ValueError):
                item.update(gesperrt=True, name='Unterlage intern prüfen', hinweis='Original fehlt oder kann hier noch nicht sicher angezeigt werden.')
            (photos if item['mime'].startswith('image/') else documents).append(item)
        return documents, photos

    def _form(self, who, oid, kind, values):
        forms = session.get('employee_order_forms', {})
        forms = {key: value for key, value in forms.items() if isinstance(value, dict) and value.get('expires', 0) > time.time()}
        if len(forms) >= 8:
            forms.pop(next(iter(forms)))
        token = secrets.token_urlsafe(18)
        forms[token] = dict(values, kind=kind, mid=who['mitarbeiter_id'], version=who['version'],
                            auth=who['auth_version'], oid=oid, expires=time.time() + 1800)
        session['employee_order_forms'] = forms
        return token

    def page(self, number=''):
        who = self.identity()
        data = {'employee': {'id': who['mitarbeiter_id'], 'name': who['mitarbeiter_name']}, 'nummer': number,
                'order': None, 'documents': [], 'photos': [], 'can_edit': self.can_edit(),
                'action_forms': [], 'photo_request_id': '', 'error': ''}
        if not number:
            return data
        row = self._order(number)
        data['order'] = self._order_view(row)
        data['documents'], data['photos'] = self._files(row['id'])
        if self.can_edit():
            writer = dict(who, dokumentieren=1)
            plans = []
            for action in _ACTIONS:
                try:
                    preview = self.progress.preview(row['id'], action, writer)
                    if preview['expected_snapshot'] != _snapshot(row):
                        data.update(can_edit=False, error='Der Auftrag wurde während der Anzeige geändert. Bitte neu öffnen.')
                        return data
                    if not preview['unveraendert']:
                        plans.append(preview)
                except ProgressError:
                    continue
            if plans:
                preview = plans[0]
                saved = {key: preview[key] for key in ('version', 'actor', 'auftrag_id', 'expected_status', 'expected_changed_at', 'expected_snapshot', 'werkstatttag')}
                request_id = self._form(who, row['id'], 'status', {'preview': saved})
                data['action_forms'] = [{'aktion': plan['aktion'], 'label': plan['aktion_label'], 'request_id': request_id} for plan in plans]
            if row['status'] in (2, 3, 4):
                data['photo_request_id'] = self._form(who, row['id'], 'fotos', {'changed_at': row['geaendert_am']})
        return data

    def _confirmed(self, who, oid, kind, payload):
        token = payload.get('request_id', '')
        if payload.get('confirmed') != 'ja' or not isinstance(token, str):
            raise ValueError('Bitte die gewünschte Änderung bewusst bestätigen.')
        form = session.get('employee_order_forms', {}).get(token)
        expected = (who['mitarbeiter_id'], who['version'], who['auth_version'], _id(oid), kind)
        if (not isinstance(form, dict) or form.get('expires', 0) <= time.time()
                or tuple(form.get(key) for key in ('mid', 'version', 'auth', 'oid', 'kind')) != expected):
            raise ValueError('Der Formularstand ist abgelaufen oder gehört zu einem anderen Zugang. Auftrag neu öffnen.')
        return token, form

    def status(self, oid, payload):
        if set(payload) - {'aktion', 'request_id', 'confirmed', 'csrf_token'}:
            raise ValueError('Nur die vorgesehenen Statusfelder übergeben.')
        action = payload.get('aktion')
        if action not in _ACTIONS:
            raise ValueError('Diese Statusaktion ist nicht erlaubt.')
        with self.p.portal_originals_operation_lock():
            who = self._writer()
            token, form = self._confirmed(who, oid, 'status', payload)
            self._order(oid)
            def authorize(db, order_id):
                self.identity(db)
                self._order(order_id, db, for_update=True)
            return self.progress.confirm(dict(form['preview'], aktion=action), who, 'employee-order:' + token, authorize=authorize)

    def upload_photos(self, oid, payload, files):
        if set(payload) - {'request_id', 'confirmed', 'csrf_token'}:
            raise ValueError('Nur die vorgesehenen Fotofelder übergeben.')
        who = self._writer()
        self._confirmed(who, oid, 'fotos', payload)
        files = [file for file in files if file and file.filename]
        if not 1 <= len(files) <= MAX_PHOTOS:
            raise ValueError('Bitte ein bis sechs Arbeitsfotos auswählen.')
        prepared = [_photo(file) for file in files]
        with self.p.portal_originals_operation_lock():
            who = self._writer()
            token, form = self._confirmed(who, oid, 'fotos', payload)
            fingerprint = hashlib.sha256(json.dumps([_id(oid), [item[2] for item in prepared]], separators=(',', ':')).encode()).hexdigest()
            key = 'employee-photo:' + token
            db = self.p.get_db()
            created_paths = []
            try:
                if not getattr(self.p, 'USE_POSTGRES', False):
                    db.execute('BEGIN IMMEDIATE')
                self.identity(db)
                previous = db.execute('SELECT payload_fingerprint,result_json FROM assistent_fortschritt_audit WHERE actor=? AND request_id=?', (who['actor'], key)).fetchone()
                if previous:
                    if previous['payload_fingerprint'] != fingerprint:
                        raise ValueError('Diese Fotofreigabe wurde bereits für andere Bilder verwendet.')
                    return dict(json.loads(previous['result_json']), wiederholt=True)
                row = self._order(oid, db, for_update=True)
                if row['status'] not in (2, 3, 4) or row['geaendert_am'] != form['changed_at']:
                    raise ValueError('Der Auftrag wurde inzwischen geändert. Auftrag neu öffnen und Fotos erneut bestätigen.')
                inserted = db.execute('''INSERT INTO assistent_fortschritt_audit
                    (actor,request_id,payload_fingerprint,order_id,action,expected_status,expected_changed_at,created_at)
                    VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(actor,request_id) DO NOTHING''',
                    (who['actor'], key, fingerprint, _id(oid), 'arbeitsfotos', row['status'], row['geaendert_am'], self.p.now_str()))
                if inserted.rowcount != 1:
                    raise ValueError('Die Fotofreigabe wird bereits bearbeitet. Bitte erneut versuchen.')
                ids = []
                for index, (raw, name, _) in enumerate(prepared):
                    stored = 'employee-order-' + token + '-' + str(index) + '.jpg'
                    # Regular uploads are included in both SQLite and PostgreSQL
                    # ZIP backups. DB-only copies alone are intentionally omitted
                    # from JSON exports, so keep the proven double-original path.
                    target = self.p.UPLOAD_DIR / stored
                    try:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        if target.is_symlink():
                            raise ValueError('Unsicherer Speicherpfad für Arbeitsfoto.')
                        if target.exists():
                            if hashlib.sha256(target.read_bytes()).hexdigest() != hashlib.sha256(raw).hexdigest():
                                raise ValueError('Vorhandenes Arbeitsfoto stimmt nicht mit dieser Freigabe überein.')
                        else:
                            with target.open('xb') as output:
                                created_paths.append(target)
                                output.write(raw)
                    except OSError:
                        raise ValueError('Arbeitsfoto konnte nicht dauerhaft gespeichert werden. Bitte erneut versuchen.') from None
                    cursor = db.execute('''INSERT INTO dateien(auftrag_id,original_name,stored_name,mime_type,size,quelle,kategorie,
                        dokument_zweck,kunde_sichtbar,partner_sichtbar,versicherung_sichtbar,sichtbarkeit_geprueft,dokument_typ,
                        notiz,hochgeladen_am) VALUES(?,?,?,?,?,'intern','assistent','schaden',0,0,0,1,'Arbeitsfoto',?,?)''',
                        (_id(oid), name, stored, 'image/jpeg', len(raw), 'Internes Arbeitsfoto von ' + who['mitarbeiter_name'], self.p.now_str()))
                    did = cursor.lastrowid
                    if not self.p.store_datei_backup(db, did, target):
                        raise ValueError('Arbeitsfoto konnte nicht zusätzlich gesichert werden. Zuordnung nicht gespeichert.')
                    ids.append(did)
                result = {'datei_ids': ids, 'wiederholt': False, 'anzahl': len(ids)}
                db.execute('UPDATE assistent_fortschritt_audit SET result_json=? WHERE actor=? AND request_id=?',
                           (json.dumps(result, separators=(',', ':')), who['actor'], key))
                db.commit()
                return result
            except BaseException:
                db.rollback()
                # Remove only files created by this failed transaction. A fresh
                # lookup avoids deleting an original after an uncertain commit;
                # unavailable DB means conservatively keep the unexposed orphan.
                for target in created_paths:
                    try:
                        check = self.p.get_db()
                        try:
                            committed = check.execute('SELECT 1 FROM dateien WHERE stored_name=? LIMIT 1', (target.name,)).fetchone()
                        finally:
                            check.close()
                        if (not committed and not target.is_symlink()
                                and target.resolve().parent == self.p.UPLOAD_DIR.resolve()):
                            target.unlink(missing_ok=True)
                    except Exception:
                        pass
                raise
            finally:
                db.close()


def ensure_employee_orders_for_import(p, *, export=None, imported_db=None, target=None, archive=None, names=None):
    """An older restore must not forget a consumed request or private photo.

    Called under the same originals lock as status/photo commits. Protect native
    audit results, current operational fields and complete internal photo blobs;
    a current JSON/SQLite snapshot remains importable without triggering work.
    """
    error = ('Datenimport gesperrt: Die Sicherung enthält vorhandene persönliche '
             'Auftragsänderungen, interne Arbeitsfotos oder deren Wiederholungsschutz '
             'nicht unverändert. Bitte eine aktuelle Sicherung verwenden.')
    own_target = target is None
    target = target if target is not None else p.get_db()
    source = None
    try:
        if not p.get_table_columns(target, 'assistent_fortschritt_audit'):
            return
        audits = [dict(row) for row in target.execute("SELECT * FROM assistent_fortschritt_audit WHERE request_id LIKE 'employee-order:%' OR request_id LIKE 'employee-photo:%'").fetchall()]
        if not audits:
            return
        protected = {'assistent_fortschritt_audit': audits, 'auftraege': [], 'dateien': [], 'datei_backups': [],
                     'mitarbeiter': [], 'assistent_rechte': []}
        mids, absent_rights = set(), set()
        for audit in audits:
            actor = audit.get('actor')
            if not isinstance(actor, str) or not re.fullmatch(r'mitarbeiter:[1-9][0-9]{0,12}', actor):
                raise ValueError(error)
            mids.add(int(actor.split(':')[1]))
        for mid in mids:
            employee = target.execute('SELECT id,name,aktiv FROM mitarbeiter WHERE id=?', (mid,)).fetchone()
            if employee is None:
                raise ValueError(error)
            protected['mitarbeiter'].append(dict(employee))
            rights = target.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=?', (mid,)).fetchone()
            if rights:
                protected['assistent_rechte'].append(dict(rights))
            else:
                absent_rights.add(mid)
        oids = {row['order_id'] for row in audits}
        for oid in oids:
            row = target.execute('SELECT ' + ','.join(_FIELDS) + ' FROM auftraege WHERE id=?', (oid,)).fetchone()
            if row:
                protected['auftraege'].append(dict(row))
        dids = set()
        for row in audits:
            if row['action'] == 'arbeitsfotos':
                dids.update(json.loads(row['result_json']).get('datei_ids', []))
        for did in dids:
            for table, key in (('dateien', 'id'), ('datei_backups', 'datei_id')):
                row = target.execute(f'SELECT * FROM {table} WHERE {key}=?', (did,)).fetchone()
                if row:
                    protected[table].append(dict(row))
        if imported_db is not None:
            source = sqlite3.connect(Path(imported_db).resolve().as_uri() + '?mode=ro', uri=True)
            source.row_factory = sqlite3.Row
            tables = {row['name'] for row in source.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            incoming = {table: [dict(row) for row in source.execute('SELECT * FROM ' + table)] if table in tables else [] for table in protected}
        else:
            incoming = (export or {}).get('tables')
        if not isinstance(incoming, dict):
            raise ValueError(error)
        incoming_rights = incoming.get('assistent_rechte', [])
        if (not isinstance(incoming_rights, list)
                or any(not isinstance(row, dict) or type(row.get('mitarbeiter_id')) is not int
                       or row['mitarbeiter_id'] < 1 or row['mitarbeiter_id'] in absent_rights
                       for row in incoming_rights)):
            raise ValueError(error)
        for table, rows in protected.items():
            if table == 'datei_backups' and table not in incoming and imported_db is None:
                # Normal ZIP export deliberately omits this high-volume table.
                # Verify the exact original upload member instead of demanding
                # fictitious DB rows or accepting a metadata-only checksum.
                if rows and (archive is None or names is None):
                    raise ValueError(error)
                for row in rows:
                    original_file = next((item for item in protected['dateien'] if item['id'] == row['datei_id']), None)
                    stored = (original_file or {}).get('stored_name')
                    if not isinstance(stored, str) or Path(stored).name != stored:
                        raise ValueError(error)
                    member = 'uploads/' + stored
                    if member not in names or archive.namelist().count(member) != 1:
                        raise ValueError(error)
                    with archive.open(member, 'r') as stream:
                        raw = stream.read(MAX_PHOTO_BYTES + 1)
                    if (len(raw) != row['size'] or len(raw) > MAX_PHOTO_BYTES
                            or hashlib.sha256(raw).hexdigest() != row['file_sha256']):
                        raise ValueError(error)
                continue
            candidates = incoming.get(table, [])
            if not isinstance(candidates, list) or any(not isinstance(row, dict) for row in candidates):
                raise ValueError(error)
            for row in rows:
                id_key = 'mitarbeiter_id' if table == 'assistent_rechte' else 'id'
                matches = [item for item in candidates if type(item.get(id_key)) is int and item[id_key] == row[id_key]]
                if len(matches) != 1:
                    raise ValueError(error)
                restored = matches[0]
                for key, original in row.items():
                    value = restored.get(key)
                    if table == 'datei_backups' and key == 'file_base64' and imported_db is None:
                        reference = p.backup_binary_reference_map(export).get((table, row['id'], key))
                        if reference is not None:
                            if archive is None or names is None:
                                raise ValueError(error)
                            value = base64.b64encode(p.read_backup_binary_blob(archive, names, reference)).decode('ascii')
                    if key not in restored or value != original:
                        raise ValueError(error)
    except (sqlite3.Error, OSError, KeyError, TypeError, json.JSONDecodeError, AttributeError):
        raise ValueError(error) from None
    finally:
        if source is not None:
            source.close()
        if own_target:
            target.close()


def register_employee_orders(p):
    service = EmployeeOrders(p)
    p.employee_orders = service
    p.app.config.setdefault('EMPLOYEE_ORDER_OPERATIONS_ENABLED', False)
    bp = Blueprint('employee_orders', __name__)

    def token():
        if not session.get('csrf_token'):
            session['csrf_token'] = secrets.token_urlsafe(32)
        return session['csrf_token']

    def csrf():
        expected, supplied = session.get('csrf_token'), request.form.get('csrf_token') or request.headers.get('X-CSRF-Token')
        if not expected or not supplied or not hmac.compare_digest(str(expected), str(supplied)):
            abort(400)

    @bp.get('/werkstatt/mein-konto/auftraege')
    def personal_orders():
        number = request.args.get('nummer', '')
        try:
            data = service.page(number)
        except PermissionError:
            return redirect('/werkstatt/materialbestellung')
        except (ValueError, LookupError) as exc:
            data = service.page()
            data.update(nummer=number[:40], error=str(exc))
            return render_template('mitarbeiter_auftraege.html', **data, csrf_token=token()), 404 if isinstance(exc, LookupError) else 400
        return render_template('mitarbeiter_auftraege.html', **data, csrf_token=token())

    @bp.post('/werkstatt/mein-konto/auftraege/<int:oid>/status')
    def status(oid):
        csrf()
        try:
            result = service.status(oid, request.form.to_dict())
            flash(result['hinweis'], 'success')
            return redirect(f'/werkstatt/mein-konto/auftraege?nummer={oid}', code=303)
        except PermissionError:
            abort(403)
        except LookupError:
            abort(404)
        except (ValueError, ProgressError) as exc:
            flash(str(exc), 'warning')
            return redirect(f'/werkstatt/mein-konto/auftraege?nummer={oid}', code=303)

    @bp.post('/werkstatt/mein-konto/auftraege/<int:oid>/fotos')
    def photos(oid):
        csrf()
        if set(request.files) - {'fotos'}:
            abort(400)
        try:
            result = service.upload_photos(oid, request.form.to_dict(), request.files.getlist('fotos'))
            flash(f'{result["anzahl"]} Arbeitsfoto(s) intern am Auftrag gespeichert.', 'success')
            return redirect(f'/werkstatt/mein-konto/auftraege?nummer={oid}', code=303)
        except PermissionError:
            abort(403)
        except LookupError:
            abort(404)
        except ValueError as exc:
            flash(str(exc), 'warning')
            return redirect(f'/werkstatt/mein-konto/auftraege?nummer={oid}', code=303)

    @bp.get('/werkstatt/mein-konto/auftraege/<int:oid>/datei/<int:did>')
    def original(oid, did):
        try:
            raw, mime, name, changed = service.content(oid, did)
        except (PermissionError, LookupError, ValueError):
            abort(404)
        response = send_file(io.BytesIO(raw), mimetype=mime, download_name=name or 'Arbeitsunterlage',
                             as_attachment=request.args.get('download') == '1', conditional=False, etag=False)
        if changed:
            response.headers['X-Workshop-Work-Copy'] = '1'
        return response

    @bp.after_request
    def private(response):
        response.headers['Cache-Control'] = 'private, no-store, max-age=0'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    p.app.register_blueprint(bp)
    return service
