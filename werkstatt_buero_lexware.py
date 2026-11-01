"""Scan suggestions and approved original-file handoff to Lexware.

Only the files endpoint can write externally. No voucher finalization, posting
category IDs, tax rates, payments or bank calls are implemented. Optional AI
extraction is injected by the host during draft analysis; transfer checks remain
local. Registration is inert and schema initialization is explicit.
The original bytes are the retry identity for the Lexware handoff.
"""
from contextlib import contextmanager
import hashlib
import hmac
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


def init_office_lexware_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS buero_lexware_transfers (
        beleg_id TEXT PRIMARY KEY, sha256 TEXT NOT NULL, approved_revision INTEGER NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending', lease_id TEXT NOT NULL DEFAULT '',
        lease_until BIGINT NOT NULL DEFAULT 0, file_id TEXT NOT NULL DEFAULT '',
        voucher_id TEXT NOT NULL DEFAULT '', remote_status TEXT NOT NULL DEFAULT '',
        api_fingerprint TEXT NOT NULL, last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        CHECK(state IN ('pending','sending','uncertain','uploaded','linked','error')),
        CHECK(approved_revision>0))''')


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
            init_office_lexware_schema(db)

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
                'url': voucher_url(voucher), 'lexware_status': receipt.get('remote_status') or 'blank',
                'hinweis': 'Original in Lexware bereitgestellt. Erfassung, steuerliche Zuordnung und Überweisung dort prüfen; Bankfreigabe bleibt persönlich.'}

    def _claim(self, key, detail, fingerprint):
        lease, now = secrets.token_hex(16), int(time.time())
        with self.p.portal_originals_operation_lock(), self.db() as db:
            self._identity(db, lock=True)
            current = db.execute('SELECT revision,status,typ,payable,business_confirmed,decided_by,sha256 '
                                 'FROM buero_belege WHERE id=?', (_key(key),)).fetchone()
            if not current or current['revision'] != detail['revision'] or current['sha256'] != detail['sha256']:
                raise LexwareConflict('Der Chef-Freigabestand wurde geändert. Bitte neu öffnen.')
            self._approved(dict(current))
            timestamp = self.p.now_str()
            db.execute('''INSERT INTO buero_lexware_transfers
                (beleg_id,sha256,approved_revision,api_fingerprint,created_at,updated_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(beleg_id) DO NOTHING RETURNING beleg_id''',
                (key, detail['sha256'], detail['revision'], fingerprint, timestamp, timestamp))
            row = db.execute('SELECT * FROM buero_lexware_transfers WHERE beleg_id=?', (key,)).fetchone()
            if row['sha256'] != detail['sha256'] or row['api_fingerprint'] != fingerprint:
                raise LexwareConflict('Original oder Lexware-Zugang unterscheidet sich vom bisherigen Übertragungsversuch. Intern abgleichen.')
            if row['voucher_id']:
                return '', dict(row)
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
            data={'type': 'voucher'}, files={'file': ('beleg-' + key + MIMES[meta['mime']], raw, meta['mime'])},
            timeout=(10, 45), allow_redirects=False)
        if response.status_code != 202:
            raise LexwareUnavailable('Lexware hat die Datei nicht mit einer bestätigten Belegreferenz angenommen. Status prüfen und denselben Beleg später erneut versuchen.')
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError('Keine gültige Lexware-Antwort erhalten.')
        return _uuid(payload.get('id')), _uuid(payload.get('voucherId'))

    def _link(self, key, detail, receipt):
        current = self._detail(key)
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
            self.p.office_documents.record_lexware_reference(key, str(current['revision']), voucher,
                                                            voucher_url(voucher), status='uebertragen')
        with self.db() as db:
            db.execute("UPDATE buero_lexware_transfers SET state='linked',last_error='',updated_at=? WHERE beleg_id=? AND voucher_id=?",
                       (self.p.now_str(), key, voucher))
        self._backup()
        return self._view(dict(receipt, state='linked'))

    def send_approved(self, key, version):
        detail = self._detail(key)
        self._approved(detail)
        receipt = self._receipt(key)
        # A successful replay returns its existing reference without another POST.
        if detail.get('lexware_beleg_id'):
            voucher = _uuid(detail['lexware_beleg_id'])
            if receipt and (receipt['sha256'] != detail['sha256'] or receipt['voucher_id'] and receipt['voucher_id'] != voucher):
                raise LexwareConflict('Lokaler Beleg und Übertragungsreferenz widersprechen sich.')
            return self._view(dict(receipt or {}, state='linked', voucher_id=voucher))
        if detail['revision'] != _revision(version):
            raise LexwareConflict('Der Belegstand wurde geändert. Freigabe neu prüfen.')
        meta, raw = self._original(key, detail)
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
        base, api_key, fingerprint = self._config()
        if self.p.app.config.get('OFFICE_LEXWARE_UPLOAD_ENABLED') is not True:
            raise LexwareUnavailable('Die automatische Lexware-Übergabe ist noch nicht aktiviert.')
        lease, prior = self._claim(key, detail, fingerprint)
        if prior:
            return self._link(key, detail, prior)
        try:
            # Recheck immediately before the external write, after local claim.
            current = self._detail(key)
            self._approved(current)
            if current['revision'] != detail['revision'] or current['sha256'] != detail['sha256']:
                raise LexwareConflict('Freigabestand wurde vor der Übertragung geändert.')
            file_id, voucher = self._multipart(key, meta, raw, base, api_key)
        except Exception as exc:
            # Timeouts and malformed replies may follow a successful remote write.
            # Never manufacture success, never change bytes or create a voucher.
            self._mark_attempt(key, lease, 'uncertain', error='Ergebnis unbestätigt; nur mit identischem Original erneut versuchen.')
            if isinstance(exc, (PermissionError, ValueError, LexwareUnavailable)):
                raise
            raise LexwareUnavailable('Übertragungsergebnis unbestätigt. Derselbe Originalbeleg kann nach Statusprüfung erneut versucht werden.') from None
        self._mark_attempt(key, lease, 'uploaded', file_id=file_id, voucher_id=voucher)
        return self._link(key, detail, self._receipt(key))

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

    @bp.post('/werkstatt/buero/belege/<key>/lexware/status')
    @bp.post('/admin/buero/belege/<key>/lexware/status')
    def status(key):
        return perform(key, lambda: service.sync_status(key))

    p.app.register_blueprint(bp)
    return service
