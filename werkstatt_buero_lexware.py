"""Scan suggestions and approved original-file handoff to Lexware.

Only the files endpoint can write externally. No voucher finalization, posting
category IDs, tax rates, payments or bank calls are implemented. Optional AI
extraction is injected by the host during draft analysis; transfer checks remain
local. Registration is inert and schema initialization is explicit.
The original bytes are the retry identity for the Lexware handoff.
"""
from contextlib import contextmanager
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import tempfile
import time
import unicodedata
from urllib.parse import urlsplit
from uuid import UUID

from flask import Blueprint, abort, flash, redirect, request, session


TABLES = ('buero_lexware_transfers',)
MAX_UPLOAD_BYTES = 5_000_000
MAX_SCAN_BYTES = 10 * 1024 * 1024
LEASE_SECONDS = 180
MIMES = {'application/pdf': '.pdf', 'image/jpeg': '.jpg', 'image/png': '.png'}
KEY = re.compile(r'[a-f0-9]{32}')
REMOTE_STATES = frozenset({'blank', 'unchecked', 'open', 'overdue', 'paid', 'paidoff',
                           'voided', 'transferred', 'sepadebit'})


class LexwareConflict(ValueError):
    pass


class LexwareUnavailable(RuntimeError):
    pass


_UPLOAD_COLUMNS = {
    'upload_base64': "TEXT NOT NULL DEFAULT ''", 'upload_sha256': "TEXT NOT NULL DEFAULT ''",
    'upload_mime': "TEXT NOT NULL DEFAULT ''", 'upload_filename': "TEXT NOT NULL DEFAULT ''",
    'upload_meta_json': "TEXT NOT NULL DEFAULT '{}'",
}


def init_office_lexware_schema(db, ensure_column=None, *, postgres=False):
    db.execute('''CREATE TABLE IF NOT EXISTS buero_lexware_transfers (
        beleg_id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, approved_revision INTEGER NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending', lease_id TEXT NOT NULL DEFAULT '',
        lease_until BIGINT NOT NULL DEFAULT 0, file_id TEXT NOT NULL DEFAULT '',
        voucher_id TEXT NOT NULL DEFAULT '', remote_status TEXT NOT NULL DEFAULT '',
        api_fingerprint TEXT NOT NULL, last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        CHECK(state IN ('pending','sending','uncertain','uploaded','linked','error')),
        CHECK(approved_revision>0))''')
    if callable(ensure_column):
        for column, definition in _UPLOAD_COLUMNS.items():
            ensure_column(db, 'buero_lexware_transfers', column, definition)
    else:
        columns = ({row['column_name'] for row in db.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='buero_lexware_transfers' AND table_schema=current_schema()").fetchall()}
            if postgres else {row[1] for row in db.execute('PRAGMA table_info(buero_lexware_transfers)').fetchall()})
        for column, definition in _UPLOAD_COLUMNS.items():
            if column not in columns:
                db.execute('ALTER TABLE buero_lexware_transfers ADD COLUMN ' + column + ' ' + definition)


def _key(value):
    if not isinstance(value, str) or not KEY.fullmatch(value):
        raise LookupError('Beleg nicht gefunden.')
    return value


def _revision(value):
    if type(value) is bool or not re.fullmatch(r'[1-9]\d{0,9}', str(value)):
        raise LexwareConflict('Aktuellen Belegstand neu öffnen.')
    return int(value)


def _uuid(value):
    if not isinstance(value, str):
        raise ValueError('Keine gültige Lexware-Belegreferenz erhalten.')
    try:
        result = str(UUID(value))
    except (ValueError, AttributeError):
        raise ValueError('Keine gültige Lexware-Belegreferenz erhalten.') from None
    if value.lower() != result:
        raise ValueError('Keine gültige Lexware-Belegreferenz erhalten.')
    return result


def voucher_url(voucher_id):
    return 'https://app.lexware.de/permalink/vouchers/edit/' + _uuid(voucher_id)


def classify_scan(text):
    """Text is evidence for a suggestion only, never executable instructions."""
    text = unicodedata.normalize('NFKC', text if isinstance(text, str) else '')[:200000].casefold()
    flat = ' '.join(text.split())
    mixed = bool(re.search(r'(?<!\d)7(?:[.,]0{1,2})?\s*%', flat)
                 and re.search(r'(?<!\d)19(?:[.,]0{1,2})?\s*%', flat))
    result = {'category': 'sonstige', 'suggested_kind': '', 'blocked_auto': False,
              'mixed_vat': mixed, 'hint': 'Dokumentart ist unklar. Original vor Freigabe prüfen.'}
    if not flat:
        result['hint'] = 'Keine verlässliche lokale Texterkennung. Angaben am Original prüfen.'
    elif re.search(r'zeugenfragebogen|anh[öo]rung\s+als\s+zeuge|zeugenaussage|bu[ßs]geld|bussgeld|verwarnungsgeld', flat):
        result.update(suggested_kind='post', blocked_auto=True,
                      hint='Anhörung, Zeugenfragebogen oder Verwarnung erkannt: keine Rechnung und keine automatische Lexware-Übergabe.')
    elif re.search(r'einkommensteuer|kirchensteuer|solidarit[äa]tszuschlag|privatrechnung|privateinkauf|nur\s+privat|privater\s+(?:beleg|einkauf)', flat):
        result.update(category='steuern' if 'steuer' in flat else 'sonstige', suggested_kind='post',
                      blocked_auto=True, hint='Privater oder persönlicher Steuerbezug erkannt. Keine automatische Übergabe an die Geschäftsbelege.')
    else:
        headings = []
        for kind, pattern in (
            ('lieferschein', r'(?m)^\s*(?:lieferschein|liefernachweis)\b'),
            ('post', r'(?m)^\s*(?:mahnung|zahlungserinnerung|angebot|auftragsbest[äa]tigung)\b'),
            ('bescheid', r'(?m)^\s*(?:(?:gewerbe|umsatz|grund|k[öo]rperschaft)steuer(?:bescheid|[- ]bescheid)|geb[üu]hrenbescheid|beitragsbescheid|abgabenbescheid|kostenbescheid|steuerbescheid)\b'),
            ('rechnung', r'(?m)^\s*(?:rechnung|invoice)\b'),
        ):
            match = re.search(pattern, text[:4000])
            if match:
                headings.append((match.start(), kind))
        kind = min(headings)[1] if headings else ''
        if kind == 'rechnung':
            result.update(category='rechnung', suggested_kind=kind,
                          hint='Rechnungsüberschrift erkannt. Art und Angaben sind ein Vorschlag; Chef prüft das Original.')
        elif kind == 'bescheid':
            result.update(category='steuern' if 'steuer' in flat else 'gebuehren', suggested_kind=kind,
                          hint='Bescheid erkannt. Betrieblicher Bezug und Zahlungspflicht müssen ausdrücklich geprüft werden.')
        elif kind:
            result.update(suggested_kind=kind, blocked_auto=True,
                          hint='Lieferschein oder sonstige Post erkannt. Keine automatische Übergabe als zahlbarer Lexware-Beleg.')
    if mixed:
        result['hint'] += ' Mehrere Umsatzsteuersätze erkannt: Aufteilung in Lexware prüfen; es wird kein einheitlicher Steuersatz gesetzt.'
    return result


class OfficeLexware:
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
        with self.p.portal_originals_operation_lock(), self.db() as db:
            init_office_lexware_schema(db, getattr(self.p, 'ensure_column', None), postgres=bool(getattr(self.p, 'USE_POSTGRES', False)))

    def _identity(self, db=None, *, lock=False):
        if db is not None:
            return self.p.office_documents.identity(admin=bool(session.get('admin')), db=db, lock=lock)
        return self.p.office_documents.identity(admin=bool(session.get('admin')))

    def _detail(self, key):
        self._identity()
        return self.p.office_documents.detail(_key(key), admin=bool(session.get('admin')))

    def _original(self, key, detail):
        meta, raw = self.p.office_documents.original(_key(key), admin=bool(session.get('admin')))
        if (meta.get('revision') != detail['revision'] or not isinstance(raw, bytes)
                or meta.get('sha256') != detail.get('sha256')
                or len(raw) != meta.get('size_bytes') or not 0 < len(raw) <= MAX_SCAN_BYTES
                or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), meta['sha256'])
                or meta.get('mime') not in MIMES):
            raise LexwareConflict('Original oder Belegstand hat sich geändert. Beleg neu prüfen.')
        return meta, raw

    def _scan(self, meta, raw, *, allow_ai=True):
        # Private, random directory; generated filename, no user-controlled path.
        # Existing local OCR always runs first. Optional online analysis is only
        # an explicitly enabled host callback; no provider client lives here.
        parent = getattr(self.p, 'DATA_DIR', None)
        if parent is not None:
            parent = Path(parent)
            if not parent.is_dir():
                raise ValueError('Geschützter Datenordner ist nicht verfügbar.')
        with tempfile.TemporaryDirectory(prefix='buero-scan-', dir=parent) as folder:
            path = Path(folder) / ('scan' + MIMES[meta['mime']])
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0), 0o600)
            with os.fdopen(descriptor, 'wb') as target:
                target.write(raw)
            try:
                text = self.p.extract_document_text_local(path, path.name)
            except Exception:
                text = ''
            from werkstatt_buero_analyse import analyse_document
            enabled = allow_ai and getattr(self.p, 'app', None) is not None and self.p.app.config.get('OFFICE_AI_ANALYSIS_ENABLED', False) is True
            return analyse_document(text, ai_hook=getattr(self.p, 'office_extract_fields_ai', None),
                                    ai_enabled=enabled, path=path, filename=path.name)

    def analyse_scan(self, key, *, intake=False):
        if intake:
            detail, raw = self.p.office_documents.intake_snapshot(_key(key))
            meta = detail
            if (not isinstance(raw, bytes) or len(raw) != detail.get('size_bytes')
                    or not 0 < len(raw) <= MAX_SCAN_BYTES or detail.get('mime') not in MIMES
                    or hashlib.sha256(raw).hexdigest() != detail.get('sha256')):
                raise LexwareConflict('Original oder Belegstand hat sich geändert. Beleg neu prüfen.')
        else:
            detail = self._detail(key)
            meta, raw = self._original(key, detail)
        if detail['status'] not in ('entwurf', 'zurueckgewiesen', 'eingereicht'):
            raise LexwareConflict('Bereits geprüfte Belege nicht ungefragt neu klassifizieren.')
        suggestion = self._scan(meta, raw)
        revision = detail['revision']
        if intake or detail['status'] in ('entwurf', 'zurueckgewiesen'):
            revision = self.p.office_documents.record_extracted_fields(
                key, str(revision), suggestion['fields'], suggestion['hint'], suggestion['source'], intake=intake)
        self.p.office_documents.record_scan_suggestion(key, str(revision), suggestion['category'],
                                                     suggestion['hint'], suggestion['suggested_kind'], intake=intake)
        return suggestion

    def _config(self):
        base = getattr(self.p, 'LEXWARE_API_BASE_URL', '').rstrip('/')
        parsed = urlsplit(base)
        if (parsed.scheme != 'https' or parsed.hostname != 'api.lexware.io'
                or parsed.username or parsed.password or parsed.port not in (None, 443)
                or parsed.path or parsed.query or parsed.fragment):
            raise LexwareUnavailable('Für die Übergabe muss der offizielle Lexware-API-Endpunkt konfiguriert sein.')
        key = getattr(self.p, 'LEXWARE_API_KEY', '')
        if not isinstance(key, str) or not key.strip():
            raise LexwareUnavailable('Der bestehende Lexware-API-Zugang ist nicht eingerichtet.')
        fingerprint = hashlib.sha256((base + '\0' + key).encode()).hexdigest()
        return base, key, fingerprint

    @staticmethod
    def _approved(detail):
        if (detail.get('status') not in ('freigegeben', 'zahlung_in_bearbeitung')
                or detail.get('typ') not in ('rechnung', 'bescheid') or detail.get('payable') != 1
                or detail.get('business_confirmed') != 1 or detail.get('decided_by') != 'admin'):
            raise ValueError('Nur chefgeprüfte, ausdrücklich betriebliche und zahlungsfreigegebene Rechnungen oder Bescheide übergeben.')

    def _receipt(self, key):
        with self.db() as db:
            row = db.execute('SELECT * FROM buero_lexware_transfers WHERE beleg_id=?', (_key(key),)).fetchone()
        return dict(row) if row else None

    def _backup(self):
        hook = getattr(self.p, 'schedule_change_backup', None)
        if callable(hook):
            hook('buero-lexware')

    @staticmethod
    def _view(receipt):
        voucher = _uuid(receipt['voucher_id'])
        return {'state': receipt['state'], 'file_id': receipt.get('file_id') or '', 'voucher_id': voucher,
                'url': voucher_url(voucher), 'lexware_status': receipt.get('remote_status') or 'unbekannt',
                'hinweis': 'Datei in Lexware zur Prüfung bereitgestellt. Erfassung, private oder geschäftliche Zuordnung und Überweisung dort prüfen; Bankfreigabe bleibt persönlich.'}

    def _claim(self, key, detail, fingerprint, *, manual=False, upload=None):
        lease, now = secrets.token_hex(16), int(time.time())
        with self.p.portal_originals_operation_lock(), self.db() as db:
            who = self._identity(db, lock=True)
            current = db.execute('SELECT revision,status,typ,payable,business_confirmed,decided_by,sha256,created_by,bereich '
                                 'FROM buero_belege WHERE id=?', (_key(key),)).fetchone()
            if not current or current['revision'] != detail['revision'] or current['sha256'] != detail['sha256']:
                raise LexwareConflict('Der Chef-Freigabestand wurde geändert. Bitte neu öffnen.')
            self.p.office_documents._visibility(dict(current), who, admin=bool(session.get('admin')))
            if not manual:
                self._approved(dict(current))
            timestamp = self.p.now_str()
            db.execute('''INSERT INTO buero_lexware_transfers
                (beleg_id,sha256,approved_revision,api_fingerprint,created_at,updated_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(beleg_id) DO NOTHING RETURNING beleg_id''',
                (key, detail['sha256'], detail['revision'], fingerprint, timestamp, timestamp))
            row = db.execute('SELECT * FROM buero_lexware_transfers WHERE beleg_id=?', (key,)).fetchone()
            self._receipt_matches(dict(row), detail, fingerprint)
            if row['voucher_id']:
                return '', dict(row)
            if not row['upload_base64'] and upload is not None:
                db.execute('''UPDATE buero_lexware_transfers SET upload_base64=?,upload_sha256=?,upload_mime=?,
                    upload_filename=?,upload_meta_json=? WHERE beleg_id=? AND upload_base64='' ''',
                    (base64.b64encode(upload['bytes']).decode('ascii'), upload['sha256'], upload['mime'], upload['filename'],
                     json.dumps(upload['metadata'], sort_keys=True, ensure_ascii=False), key))
            claimed = db.execute('''UPDATE buero_lexware_transfers SET state='sending',lease_id=?,lease_until=?,
                approved_revision=?,last_error='',updated_at=? WHERE beleg_id=?
                AND (state<>'sending' OR lease_until<=?) RETURNING beleg_id''',
                (lease, now + LEASE_SECONDS, detail['revision'], timestamp, key, now)).fetchone()
            if not claimed:
                raise LexwareConflict('Dieser Beleg wird gerade übertragen. Bitte den laufenden Versuch abwarten.')
        self._backup()
        return lease, None

    def _mark_attempt(self, key, lease, state, *, file_id='', voucher_id='', error=''):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            changed = db.execute('''UPDATE buero_lexware_transfers SET state=?,file_id=?,voucher_id=?,
                last_error=?,lease_until=0,updated_at=? WHERE beleg_id=? AND lease_id=? RETURNING beleg_id''',
                (state, file_id, voucher_id, error, self.p.now_str(), key, lease)).fetchone()
            if not changed:
                raise LexwareConflict('Der Übertragungsversuch wurde inzwischen übernommen. Belegstatus neu prüfen.')
        self._backup()

    def _multipart(self, key, meta, raw, base, api_key):
        http = self.p.get_requests()
        if http is None:
            raise LexwareUnavailable('Das vorhandene HTTP-Modul ist nicht verfügbar.')
        response = http.request('POST', base + '/v1/files',
            headers={'Authorization': 'Bearer ' + api_key, 'Accept': 'application/json'},
            data={'type': 'voucher'}, files={'file': (meta.get('upload_filename') or 'beleg-' + key + MIMES[meta['mime']], raw, meta['mime'])},
            timeout=(10, 45), allow_redirects=False)
        if response.status_code != 202:
            raise LexwareUnavailable('Lexware hat die Datei nicht mit einer bestätigten Belegreferenz angenommen. Status prüfen und denselben Beleg später erneut versuchen.')
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError('Keine gültige Lexware-Antwort erhalten.')
        return _uuid(payload.get('id')), _uuid(payload.get('voucherId'))

    def _link(self, key, detail, receipt, *, manual=False):
        current = self._detail(key)
        if not manual:
            self._approved(current)
        if current['sha256'] != detail['sha256']:
            raise LexwareConflict('Das lokale Original wurde geändert. Übergabe intern prüfen.')
        voucher = _uuid(receipt['voucher_id'])
        if current.get('lexware_beleg_id'):
            if _uuid(current['lexware_beleg_id']) != voucher:
                raise LexwareConflict('Der Beleg hat bereits eine andere Lexware-Referenz.')
        else:
            if current['revision'] != detail['revision']:
                raise LexwareConflict('Der Freigabestand wurde während der Übergabe geändert. Die gespeicherte Lexware-Referenz intern abgleichen.')
            record = (self.p.office_documents.record_lexware_filing_reference if manual
                      else self.p.office_documents.record_lexware_reference)
            record(key, str(current['revision']), voucher, voucher_url(voucher), status='uebertragen')
        with self.db() as db:
            db.execute("UPDATE buero_lexware_transfers SET state='linked',last_error='',updated_at=? WHERE beleg_id=? AND voucher_id=?",
                       (self.p.now_str(), key, voucher))
        self._backup()
        return self._view(dict(receipt, state='linked'))

    @staticmethod
    def _receipt_matches(receipt, detail, fingerprint):
        if receipt and (receipt['sha256'] != detail['sha256'] or receipt['api_fingerprint'] != fingerprint):
            raise LexwareConflict('Original oder Lexware-Zugang unterscheidet sich vom bisherigen Übertragungsversuch. Intern abgleichen.')
        if receipt and not receipt.get('upload_base64') and (
                receipt.get('upload_sha256') or receipt.get('upload_mime') or receipt.get('upload_filename')
                or receipt.get('upload_meta_json') != '{}'):
            raise LexwareConflict('Die gespeicherte Lexware-Uploadkopie ist unvollständig. Intern abgleichen; keine andere Datei senden.')
        if receipt and receipt.get('upload_base64'):
            OfficeLexware._frozen_upload(receipt, detail)

    @staticmethod
    def _frozen_upload(receipt, detail):
        try:
            raw = base64.b64decode(receipt['upload_base64'], validate=True)
            metadata = json.loads(receipt['upload_meta_json'])
            mime, filename = receipt['upload_mime'], receipt['upload_filename']
            if (not 0 < len(raw) <= MAX_UPLOAD_BYTES or mime not in MIMES
                    or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,179}', filename)
                    or not filename.lower().endswith(MIMES[mime])
                    or hashlib.sha256(raw).hexdigest() != receipt['upload_sha256']
                    or not isinstance(metadata, dict) or metadata.get('original_sha256') != detail['sha256']):
                raise ValueError
        except (KeyError, ValueError, TypeError):
            raise LexwareConflict('Die gespeicherte Lexware-Uploadkopie ist nicht unverändert prüfbar. Intern abgleichen; keine neue Kopie senden.') from None
        return {'bytes': raw, 'mime': mime, 'filename': filename, 'sha256': receipt['upload_sha256'], 'metadata': metadata}

    def _prepare_upload(self, key, detail, meta, raw, receipt, *, manual):
        if receipt and receipt.get('upload_base64'):
            return self._frozen_upload(receipt, detail)
        # A legacy attempt may already have reached Lexware with these exact
        # original bytes. Never replace its retry identity with a new cover.
        if not manual or receipt:
            if len(raw) > MAX_UPLOAD_BYTES:
                raise ValueError('Lexware-Datei muss höchstens 5 MB groß sein. Original nicht automatisch komprimiert oder verändert.')
            return dict(bytes=raw, mime=meta['mime'], filename='beleg-' + key + MIMES[meta['mime']],
                        sha256=hashlib.sha256(raw).hexdigest(), metadata={
                            'original_sha256': detail['sha256'], 'source_revision': detail['revision'],
                            'scope': 'legacy_original' if receipt else 'approved_original', 'transformed': False})
        from werkstatt_buero_lexware_kopie import prepare_lexware_review_copy
        prepared = prepare_lexware_review_copy(detail, raw, max_bytes=MAX_UPLOAD_BYTES)
        if not isinstance(prepared, dict) or prepared.get('original_sha256') != detail['sha256']:
            raise ValueError('Lexware-Prüfkopie gehört nicht zum unveränderten Original.')
        upload = {'bytes': prepared.get('bytes'), 'mime': prepared.get('mime'), 'filename': prepared.get('filename'),
                  'sha256': prepared.get('sha256'), 'metadata': {
                      'scope': 'manual_review', 'source_revision': detail['revision'], 'original_sha256': detail['sha256'],
                      'transformed': prepared.get('transformed'), 'source_pages': prepared.get('source_pages')}}
        if not isinstance(upload['bytes'], bytes):
            raise ValueError('Keine gültige Lexware-Prüfkopie erhalten.')
        # Apply the same content/hash/name checks before anything is persisted.
        self._frozen_upload(dict(upload_base64=base64.b64encode(upload['bytes']).decode('ascii'),
            upload_meta_json=json.dumps(upload['metadata']), upload_mime=upload['mime'],
            upload_filename=upload['filename'], upload_sha256=upload['sha256']), detail)
        return upload

    def send_approved(self, key, version):
        """The existing automatic handoff retains its stricter payment gates."""
        return self._send(key, version, manual=False)

    def file_for_review(self, key, version):
        """Explicit user-requested filing, independent of payment approval."""
        return self._send(key, version, manual=True)

    def _send(self, key, version, *, manual):
        detail = self._detail(key)
        if not manual:
            self._approved(detail)
        if detail['revision'] != _revision(version):
            raise LexwareConflict('Der Belegstand wurde geändert. Beleg neu öffnen.')
        base, api_key, fingerprint = self._config()
        receipt = self._receipt(key)
        self._receipt_matches(receipt, detail, fingerprint)
        # A successful replay returns its existing reference without another POST.
        if detail.get('lexware_beleg_id'):
            voucher = _uuid(detail['lexware_beleg_id'])
            if receipt and (receipt['sha256'] != detail['sha256'] or receipt['voucher_id'] and receipt['voucher_id'] != voucher):
                raise LexwareConflict('Lokaler Beleg und Übertragungsreferenz widersprechen sich.')
            result = self._view(dict(receipt or {}, state='linked', voucher_id=voucher))
            if not receipt:
                result['hinweis'] = 'Vorhandener Lexware-Verweis geöffnet. Übertragungsnachweis und Belegstatus sind hier unbekannt; keine neue Datei gesendet.'
            return result
        meta, raw = self._original(key, detail)
        if not manual:
            if len(raw) > MAX_UPLOAD_BYTES:
                raise ValueError('Lexware-Datei muss höchstens 5 MB groß sein. Original nicht automatisch komprimiert oder verändert.')
            suggestion = self._scan(meta, raw, allow_ai=False)
            if suggestion['blocked_auto']:
                raise ValueError(suggestion['hint'])
            metadata_hint = classify_scan('\n'.join(str(detail.get(field) or '') for field in
                                                   ('lieferant', 'belegnummer', 'notiz')))
            if metadata_hint['blocked_auto']:
                raise ValueError(metadata_hint['hint'])
            if suggestion['suggested_kind'] and suggestion['suggested_kind'] != detail['typ']:
                raise ValueError('Erkannte Dokumentart und geprüfte Art widersprechen sich. Chef muss den Beleg erneut prüfen.')
        if self.p.app.config.get('OFFICE_LEXWARE_UPLOAD_ENABLED') is not True:
            raise LexwareUnavailable('Die Lexware-Übergabe ist noch nicht aktiviert.')
        upload = None if receipt and receipt.get('voucher_id') else self._prepare_upload(key, detail, meta, raw, receipt, manual=manual)
        lease, prior = self._claim(key, detail, fingerprint, manual=manual, upload=upload)
        if prior:
            return self._link(key, detail, prior, manual=manual)
        try:
            # Recheck immediately before the external write, after local claim.
            current = self._detail(key)
            if not manual:
                self._approved(current)
            if current['revision'] != detail['revision'] or current['sha256'] != detail['sha256']:
                raise LexwareConflict('Freigabestand wurde vor der Übertragung geändert.')
            frozen = self._frozen_upload(self._receipt(key), detail)
            file_id, voucher = self._multipart(key, {'mime': frozen['mime'], 'upload_filename': frozen['filename']}, frozen['bytes'], base, api_key)
        except Exception as exc:
            # Timeouts and malformed replies may follow a successful remote write.
            # Never manufacture success, never change bytes or create a voucher.
            self._mark_attempt(key, lease, 'uncertain', error='Ergebnis unbestätigt; nur mit identischem Original erneut versuchen.')
            if isinstance(exc, (PermissionError, ValueError, LexwareUnavailable)):
                raise
            raise LexwareUnavailable('Übertragungsergebnis unbestätigt. Derselbe Originalbeleg kann nach Statusprüfung erneut versucht werden.') from None
        self._mark_attempt(key, lease, 'uploaded', file_id=file_id, voucher_id=voucher)
        return self._link(key, detail, self._receipt(key), manual=manual)

    def sync_status(self, key):
        detail = self._detail(key)
        receipt = self._receipt(key)
        voucher = _uuid(detail.get('lexware_beleg_id') or (receipt or {}).get('voucher_id'))
        if receipt and (receipt['sha256'] != detail['sha256']
                        or receipt['voucher_id'] and receipt['voucher_id'] != voucher):
            raise LexwareConflict('Lokaler Beleg und gespeicherte Lexware-Referenz widersprechen sich.')
        _, _, fingerprint = self._config()
        if receipt and receipt['api_fingerprint'] != fingerprint:
            raise LexwareConflict('Lexware-Zugang wurde seit der Übergabe geändert. Intern abgleichen.')
        # The established JSON helper is used exclusively with GET here.
        remote = self.p.lexware_request('GET', '/v1/vouchers/' + voucher)
        if not isinstance(remote, dict) or remote.get('id') and _uuid(remote['id']) != voucher:
            raise LexwareUnavailable('Lexware-Belegstatus konnte nicht sicher zugeordnet werden.')
        status = remote.get('voucherStatus')
        status = status if isinstance(status, str) and status in REMOTE_STATES else 'unbekannt'
        if receipt:
            with self.db() as db:
                db.execute('UPDATE buero_lexware_transfers SET remote_status=?,updated_at=? WHERE beleg_id=? AND voucher_id=?',
                           (status, self.p.now_str(), key, voucher))
        return dict(self._view(dict(receipt or {}, state=(receipt or {}).get('state', 'linked'), voucher_id=voucher)),
                    lexware_status=status,
                    hinweis='Lexware-Status gelesen. Keine lokale Zahlungsbestätigung und keine Überweisung ausgelöst.')


def register_office_lexware(p):
    service = OfficeLexware(p)
    p.office_lexware = service
    p.office_lexware_init_schema = service.init_schema
    bp = Blueprint('office_lexware', __name__)

    @bp.before_request
    def protected():
        try:
            service._identity()
        except PermissionError:
            abort(403)
        expected, supplied = session.get('csrf_token'), request.form.get('csrf_token')
        if not expected or not supplied or not hmac.compare_digest(str(expected), str(supplied)):
            abort(400)

    @bp.after_request
    def private(response):
        response.headers['Cache-Control'] = 'private, no-store, max-age=0'
        response.headers['Referrer-Policy'] = 'no-referrer'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response

    def perform(key, action):
        if set(request.form) - {'csrf_token', 'revision'}:
            abort(400)
        try:
            if service._detail(key)['revision'] != _revision(request.form.get('revision')):
                raise LexwareConflict('Beleg wurde geändert. Neu öffnen.')
            action()
        except PermissionError:
            abort(403)
        except LookupError:
            abort(404)
        except LexwareConflict as exc:
            return str(exc), 409
        except ValueError as exc:
            return str(exc), 400
        except RuntimeError:
            flash('Lexware-Übergabe oder Statusprüfung ist noch nicht bestätigt. Original und Freigabe bleiben im Büro erhalten.', 'warning')
        prefix = '/admin' if session.get('admin') else '/werkstatt'
        return redirect(prefix + '/buero/belege/' + _key(key), code=303)

    @bp.post('/werkstatt/buero/belege/<key>/analysieren')
    def analyse(key):
        return perform(key, lambda: service.analyse_scan(key))

    @bp.post('/werkstatt/buero/belege/<key>/lexware')
    @bp.post('/admin/buero/belege/<key>/lexware')
    def transfer(key):
        return perform(key, lambda: service.send_approved(key, request.form.get('revision')))

    @bp.post('/werkstatt/buero/belege/<key>/lexware/ablegen')
    @bp.post('/admin/buero/belege/<key>/lexware/ablegen')
    def filing(key):
        return perform(key, lambda: service.file_for_review(key, request.form.get('revision')))

    @bp.post('/werkstatt/buero/belege/<key>/lexware/status')
    @bp.post('/admin/buero/belege/<key>/lexware/status')
    def status(key):
        return perform(key, lambda: service.sync_status(key))

    p.app.register_blueprint(bp)
    return service
