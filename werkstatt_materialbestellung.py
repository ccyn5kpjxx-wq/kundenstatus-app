"""Personal photo/count requests joined to the existing guarded material chain.

Portal originals, the immutable batch/item references and employee audit live
in the already backed-up intake/material tables. Portal references are their
own source, never Meta messages or WhatsApp sender grants. Registration starts
no worker and grants no login, purchase right, price or supplier approval.
"""
import copy
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import io
import json
import re
import secrets
import threading
import time
from collections import OrderedDict
from types import SimpleNamespace

from flask import Blueprint, jsonify, render_template, request, session
from werkzeug.datastructures import FileStorage
from werkzeug.exceptions import RequestEntityTooLarge
from PIL import Image

from werkstatt_materialfoto import _image, _code_view, _CODE_KEY, _search_code, LabelPreviewBusy
from werkstatt_materialkanal import _BorrowedConnection, _fingerprint, _json, _note


PORTAL_SOURCE = 'portal:personal'
MAX_PHOTOS = 10
MAX_PHOTO_BYTES = 8 * 1024 * 1024
MAX_TOTAL_BYTES = 50 * 1024 * 1024
MAX_BODY_BYTES = MAX_TOTAL_BYTES + 512 * 1024
MAX_PREVIEW_BODY_BYTES = MAX_PHOTO_BYTES + 512 * 1024
PREVIEW_RATE_LIMIT = 30
_UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
_PHOTO_REF = re.compile(r'portal\.([1-9][0-9]*)\.(' + _UUID.pattern + r')\.(' + _UUID.pattern + r')')
_REPLY_REF = re.compile(r'portalreply\.([1-9][0-9]*)\.(' + _UUID.pattern + r')\.([1-9][0-9]*)')
_REQUEST_PREFIX = 'portal-request:v1:'


class SubmissionConflict(ValueError):
    pass


def _uuid(value):
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise ValueError('Eindeutige Vorgangsnummer fehlt. Bitte das Formular neu öffnen.')
    return value


def _description(value):
    if (not isinstance(value, str) or len(value) > 500
            or _note(value, 500) != value.strip()):
        raise ValueError('Beschreibung mit höchstens 500 Zeichen ohne sensible Konto- oder Zugangsdaten angeben.')
    return value.strip()


def _rejected_codes(value):
    if (not isinstance(value, list) or len(value) > 8
            or any(not isinstance(code, str) or _search_code(code) != code for code in value)):
        raise ValueError('Abgelehnte Artikelcodes eindeutig angeben.')
    return list(dict.fromkeys(value))


def portal_request_details(source):
    """Read immutable form metadata; free descriptions are never commands.

    Legacy personal photos retain their original plain quantity caption and
    therefore their exact replay hashes. Only this form can create the structured
    image source; reply texts and other channels cannot impersonate its mode.
    """
    if (source['phone_number_id'] != PORTAL_SOURCE or not _PHOTO_REF.fullmatch(source['wamid'])
            or not source['mime'].startswith('image/') or not source['caption'].startswith(_REQUEST_PREFIX)):
        return None
    try:
        details = json.loads(source['caption'][len(_REQUEST_PREFIX):])
    except (ValueError, TypeError):
        raise ValueError('Gespeicherte Bildanforderung benötigt eine interne Prüfung.') from None
    required = {'menge', 'dringend', 'vorgang', 'beschreibung'}
    if (not isinstance(details, dict) or not required <= set(details)
            or set(details) - required - {'artikelkorrektur', 'abgelehnte_codes', 'etikett_sha256'}
            or type(details['menge']) is not int or not 1 <= details['menge'] <= 999
            or type(details['dringend']) is not bool or not isinstance(details['vorgang'], str)
            or details['vorgang'] not in {'bestellung', 'anfrage'}
            or _description(details['beschreibung']) != details['beschreibung']):
        raise ValueError('Gespeicherte Bildanforderung benötigt eine interne Prüfung.')
    if set(details) - required:
        if (details.get('artikelkorrektur') is not True or 'abgelehnte_codes' not in details
                or _rejected_codes(details['abgelehnte_codes']) != details['abgelehnte_codes']
                or 'etikett_sha256' in details and not re.fullmatch(r'[0-9a-f]{64}', str(details['etikett_sha256']))):
            raise ValueError('Gespeicherte Artikelkorrektur benötigt eine interne Prüfung.')
    return details


def _position_evidence(row):
    result = {key: row[key] for key in ('client_id', 'quantity', 'urgent', 'sha256')}
    # Empty additions preserve the original three-field client's fingerprint.
    if row['vorgang'] != 'bestellung' or row['beschreibung']:
        result.update(vorgang=row['vorgang'], beschreibung=row['beschreibung'])
    if row.get('artikelkorrektur'):
        result.update(artikelkorrektur=True, abgelehnte_codes=row['abgelehnte_codes'])
        if row.get('label'):
            result['etikett_sha256'] = row['label']['sha256']
    return result


class MaterialOrderPortal:
    def __init__(self, portal):
        self.p = portal
        self.clock = portal.material_channel.clock
        self._preview_requests = OrderedDict()
        self._preview_lock = threading.Lock()

    def preview_allowed(self, actor):
        """Bound decoder/catalog work per person; no persisted order state."""
        now = time.monotonic()
        with self._preview_lock:
            for key, times in list(self._preview_requests.items()):
                if not times or times[-1] <= now - 60:
                    del self._preview_requests[key]
            times = [stamp for stamp in self._preview_requests.pop(actor, []) if stamp > now - 60]
            allowed = len(times) < PREVIEW_RATE_LIMIT
            if allowed:
                times.append(now)
            self._preview_requests[actor] = times
            while len(self._preview_requests) > 1024:
                self._preview_requests.popitem(last=False)
            return allowed

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

    def identity(self):
        """Only the existing personal session identifies the requesting person."""
        mid = session.get('assistent_mid')
        version = session.get('assistent_version')
        auth_version = session.get('assistent_auth_version', 1)
        if (type(mid) is not int or mid <= 0 or type(version) is not int
                or type(auth_version) is not int
                or not self.p.app.config.get('ASSISTANT_NATIVE_COCKPIT', True)):
            return None
        with self.db() as db:
            row = db.execute('''SELECT r.*,m.name AS mitarbeiter_name,m.aktiv
                FROM assistent_rechte r JOIN mitarbeiter m ON m.id=r.mitarbeiter_id
                WHERE r.mitarbeiter_id=?''', (mid,)).fetchone()
        if (not row or not row['aktiv'] or row['version'] != version or not row['lesen']
                or dict(row).get('auth_version', 1) != auth_version):
            return None
        return dict(row, actor='mitarbeiter:' + str(mid))

    def can_order(self, who):
        return bool(who and who.get('lesen') and who.get('einkaufen')
                    and type(who.get('limit_cent')) is int and who['limit_cent'] > 0)

    def active(self, db, row, *, lock=False):
        """Worker/dispatch revalidate the persisted personal portal source."""
        reference = _PHOTO_REF.fullmatch(row['wamid']) or _REPLY_REF.fullmatch(row['wamid'])
        if (row['phone_number_id'] != PORTAL_SOURCE or not reference
                or int(reference[1]) != row['employee_id'] or row['sender_id'] != 0
                or row['sender_revision'] != row['rights_version'] or row['forwarded']
                or not self.p.app.config.get('ASSISTANT_NATIVE_COCKPIT', True)):
            raise PermissionError('Persönliche Portalquelle ist nicht gültig.')
        portal_request_details(row)
        if lock:
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (row['employee_id'],))
            db.execute('UPDATE assistent_rechte SET version=version WHERE mitarbeiter_id=?', (row['employee_id'],))
        employee = self.p.material_channel._employee(db, row['employee_id'])
        rights = db.execute('SELECT limit_cent FROM assistent_rechte WHERE mitarbeiter_id=?',
                            (row['employee_id'],)).fetchone()
        if (employee['version'] != row['rights_version'] or not rights
                or type(rights['limit_cent']) is not int or rights['limit_cent'] <= 0):
            raise PermissionError('Persönliche Materialfreigabe wurde seit dem Eingang geändert.')
        return employee

    def _person(self, db, who, *, lock=False):
        if not self.can_order(who):
            raise PermissionError('Persönlicher Materialzugang mit Einkaufrecht erforderlich.')
        mid = who['mitarbeiter_id']
        if who.get('actor') != 'mitarbeiter:' + str(mid):
            raise PermissionError('Persönlicher Materialzugang erforderlich.')
        if lock:
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (mid,))
            db.execute('UPDATE assistent_rechte SET version=version WHERE mitarbeiter_id=?', (mid,))
        employee = self.p.material_channel._employee(db, mid)
        rights = db.execute('SELECT limit_cent FROM assistent_rechte WHERE mitarbeiter_id=?', (mid,)).fetchone()
        if employee['version'] != who['version'] or not rights or rights['limit_cent'] <= 0:
            raise PermissionError('Persönliche Rechte wurden geändert. Bitte erneut anmelden.')
        return employee

    def _audit(self, db, who, action, details):
        db.execute('INSERT INTO assistent_audit(actor,auftrag_id,aktion,details,zeit) VALUES(?,?,?,?,?)',
                   (who['actor'], None, action, _json(details), self.p.now_str()))

    def _validate(self, request_id, positions, files):
        request_id = _uuid(request_id)
        if not isinstance(positions, list) or not 1 <= len(positions) <= MAX_PHOTOS:
            raise ValueError('Ein bis zehn Fotos mit zugehöriger Stückzahl auswählen.')
        ids, rows, total = set(), [], 0
        expected_fields = {'foto_' + str(item.get('id')) for item in positions if isinstance(item, dict)}
        expected_fields.update('etikett_' + str(item.get('id')) for item in positions
                               if isinstance(item, dict) and item.get('etikett_datei') is True)
        if set(files.keys()) != expected_fields or any(len(files.getlist(key)) != 1 for key in files.keys()):
            raise ValueError('Jedes Foto muss genau einem Eintrag zugeordnet sein.')
        for position in positions:
            if (not isinstance(position, dict) or not {'id', 'menge', 'dringend'} <= set(position)
                    or set(position) - {'id', 'menge', 'dringend', 'vorgang', 'beschreibung',
                                        'artikelkorrektur', 'abgelehnte_codes', 'etikett_datei'}):
                raise ValueError('Zu jedem Bild Vorgangsnummer, Stückzahl, Dringlichkeit und optional Beschreibung oder Anfrage angeben.')
            client_id = _uuid(position['id'])
            if client_id in ids:
                raise ValueError('Fotoreferenz wurde mehrfach verwendet. Bitte neu auswählen.')
            ids.add(client_id)
            quantity, urgent = position['menge'], position['dringend']
            if type(quantity) is not int or not 1 <= quantity <= 999 or type(urgent) is not bool:
                raise ValueError('Stückzahl muss eine ganze Zahl von 1 bis 999 sein; Dringlichkeit muss ja oder nein sein.')
            kind = position.get('vorgang', 'bestellung')
            if not isinstance(kind, str) or kind not in {'bestellung', 'anfrage'}:
                raise ValueError('Bestellung oder Teileanfrage wählen.')
            description = _description(position.get('beschreibung', ''))
            correction = position.get('artikelkorrektur', False)
            rejected = _rejected_codes(position.get('abgelehnte_codes', []))
            has_label = position.get('etikett_datei', False)
            if (type(correction) is not bool or type(has_label) is not bool
                    or (rejected or has_label) and not correction):
                raise ValueError('Etikettfoto und abgelehnte Codes benötigen eine Artikelkorrektur.')
            file = files.get('foto_' + client_id)
            if not file or not getattr(file, 'filename', ''):
                raise ValueError('Bitte zu jedem Eintrag ein Foto auswählen.')
            raw = file.read(MAX_PHOTO_BYTES + 1)
            if not raw or len(raw) > MAX_PHOTO_BYTES:
                raise ValueError('Ein Foto ist leer oder größer als 8 MB.')
            total += len(raw)
            if total > MAX_TOTAL_BYTES:
                raise ValueError('Alle Fotos zusammen dürfen höchstens 50 MB groß sein.')
            # Validate before the write transaction; preserve original bytes in
            # the intake and use the existing metadata-free copy for analysis.
            _image(raw)
            with Image.open(io.BytesIO(raw)) as image:
                mime, suffix = {'JPEG': ('image/jpeg', '.jpg'), 'PNG': ('image/png', '.png'),
                                'WEBP': ('image/webp', '.webp')}[image.format]
            label = None
            if has_label:
                label_file = files.get('etikett_' + client_id)
                label_raw = label_file.read(MAX_PHOTO_BYTES + 1)
                if not label_raw or len(label_raw) > MAX_PHOTO_BYTES:
                    raise ValueError('Etikettfoto leer oder größer als 8 MB.')
                total += len(label_raw)
                if total > MAX_TOTAL_BYTES:
                    raise ValueError('Alle Fotos zusammen dürfen höchstens 50 MB groß sein.')
                _image(label_raw)
                with Image.open(io.BytesIO(label_raw)) as image:
                    label_mime, label_suffix = {'JPEG': ('image/jpeg', '.jpg'), 'PNG': ('image/png', '.png'),
                                               'WEBP': ('image/webp', '.webp')}[image.format]
                label = dict(raw=label_raw, mime=label_mime, suffix=label_suffix, sha256=hashlib.sha256(label_raw).hexdigest())
            rows.append({'client_id': client_id, 'quantity': quantity, 'urgent': urgent,
                         'vorgang': kind, 'beschreibung': description,
                         'artikelkorrektur': correction, 'abgelehnte_codes': rejected, 'label': label,
                         'sha256': hashlib.sha256(raw).hexdigest(), 'raw': raw, 'mime': mime, 'suffix': suffix})
        return request_id, rows

    def submit(self, who, request_id, positions, files):
        # Access must precede reading any uploaded bytes. Check again after all
        # potentially slow validation and hold the rights lock through commit.
        with self.db() as db:
            self._person(db, who)
        request_id, rows = self._validate(request_id, positions, files)
        evidence = [_position_evidence(row) for row in rows]
        batch_hash = _fingerprint(sorted(evidence, key=lambda row: row['client_id']))
        now = self.clock()
        stamp = datetime.fromtimestamp(now, timezone.utc).isoformat()
        with self.p.portal_originals_operation_lock(), self.db() as db:
            employee = self._person(db, who, lock=True)
            mid = employee['id']
            prefix = 'portal.' + str(mid) + '.' + request_id + '.'
            existing = db.execute('''SELECT * FROM einkauf_material_nachrichten
                WHERE phone_number_id=? AND employee_id=? AND wamid LIKE ? ORDER BY id''',
                (PORTAL_SOURCE, mid, prefix + '%')).fetchall()
            if existing:
                expected = {prefix + row['client_id']: _fingerprint(dict(batch_hash=batch_hash,
                            **_position_evidence(row))) for row in rows}
                if len(existing) != len(rows) or any(expected.get(row['wamid']) != row['canonical_hash'] for row in existing):
                    raise SubmissionConflict('Diese Abgabe wurde bereits mit anderen Fotos oder Mengen erfasst. Bitte den gespeicherten Vorgang prüfen.')
                views = []
                for source in existing:
                    self.active(db, source)
                    draft = db.execute('SELECT id FROM einkauf_material_dialoge WHERE message_id=?', (source['id'],)).fetchone()
                    if not draft:
                        raise SubmissionConflict('Die gespeicherte Abgabe benötigt eine interne Prüfung. Bitte nicht erneut bestellen.')
                    views.append(self._view(db, draft['id']))
                return {'request_id': request_id, 'anforderungen': views}
            intake = copy.copy(self.p.workshop_intake)
            intake.p = SimpleNamespace(get_db=lambda: _BorrowedConnection(db))
            photos = copy.copy(self.p.assistant_material_photos)
            photos.p = SimpleNamespace(get_db=lambda: _BorrowedConnection(db), app=self.p.app, now_str=self.p.now_str)
            dialog = copy.copy(self.p.material_dialog)
            dialog.p = SimpleNamespace(**vars(self.p))
            dialog.p.get_db = lambda: _BorrowedConnection(db)
            views = []
            for row in rows:
                reference = prefix + row['client_id']
                source_key = 'portal-photo:' + str(mid) + ':' + hashlib.sha256(reference.encode()).hexdigest()
                caption = str(row['quantity']) + ' Stück' + (', dringend' if row['urgent'] else '')
                if row['vorgang'] != 'bestellung' or row['beschreibung'] or row['artikelkorrektur']:
                    details = {'menge': row['quantity'], 'dringend': row['urgent'],
                               'vorgang': row['vorgang'], 'beschreibung': row['beschreibung']}
                    if row['artikelkorrektur']:
                        details.update(artikelkorrektur=True, abgelehnte_codes=row['abgelehnte_codes'])
                        if row['label']:
                            details['etikett_sha256'] = row['label']['sha256']
                    caption = _REQUEST_PREFIX + _json(details)
                group = intake.create({'supplier': 'Lieferant ungeklärt', 'source_key': source_key,
                    'external_ref': 'Persönliche Foto-Bestellmaske; Mitarbeiter: ' + employee['name'] + '; Abgabe: ' + request_id,
                    'source_at': stamp, 'already_ordered': False, 'original_author': employee['name'],
                    'lines': [{'product': re.sub(r'\s+',' ',row['beschreibung']) or ('Teileanfrage anhand Bild' if row['vorgang']=='anfrage' else 'Materialfoto – Artikelzuordnung prüfen'), 'quantity': str(row['quantity']),
                               'unit': 'Stück', 'urgent': row['urgent'], 'category': 'ungeklaert',
                               'original_author': employee['name']}]})
                db.execute('UPDATE einkauf_eingang SET created_by=? WHERE id=?', (who['actor'], group['id']))
                # _image validated the actual original, including WebP. Store
                # it losslessly; the photo service makes its own clean JPEG.
                original = db.execute('''INSERT INTO einkauf_eingang_dateien
                    (eingang_id,kind,original_name,mime,suffix,sha256,original_base64,created_at,created_by)
                    VALUES(?,'materialfoto',?,?,?,?,?,?,?) RETURNING id''',
                    (group['id'], 'materialfoto' + row['suffix'], row['mime'], row['suffix'], row['sha256'],
                     base64.b64encode(row['raw']).decode('ascii'), stamp, who['actor'])).fetchone()
                intake._bump(db, group['id'])
                analysis_file = row
                if row['label']:
                    analysis_file = row['label']
                    original = db.execute('''INSERT INTO einkauf_eingang_dateien
                        (eingang_id,kind,original_name,mime,suffix,sha256,original_base64,created_at,created_by)
                        VALUES(?,'materialfoto',?,?,?,?,?,?,?) RETURNING id''',
                        (group['id'], 'produktetikett' + analysis_file['suffix'], analysis_file['mime'], analysis_file['suffix'],
                         analysis_file['sha256'], base64.b64encode(analysis_file['raw']).decode('ascii'), stamp, who['actor'])).fetchone()
                    intake._bump(db, group['id'])
                photo = photos.stage(who, FileStorage(stream=io.BytesIO(analysis_file['raw']), filename='materialfoto.jpg'),
                                     'portal-' + hashlib.sha256(reference.encode()).hexdigest())
                canonical = _fingerprint(dict(batch_hash=batch_hash,
                    **_position_evidence(row)))
                db.execute('''INSERT INTO einkauf_material_nachrichten
                    (phone_number_id,wamid,canonical_hash,sender_id,sender_revision,employee_id,employee_name,
                     rights_version,media_id,mime,expected_sha256,caption,source_at,received_at,state,
                     intake_id,file_id,assistant_photo_id,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'ready',?,?,?,?)''',
                    (PORTAL_SOURCE, reference, canonical, 0, employee['version'], mid, employee['name'],
                     employee['version'], row['client_id'], analysis_file['mime'], analysis_file['sha256'], caption, stamp, now,
                     group['id'], original['id'], photo['id'], now))
                source = db.execute('SELECT * FROM einkauf_material_nachrichten WHERE phone_number_id=? AND wamid=?',
                                    (PORTAL_SOURCE, reference)).fetchone()
                view = dialog._ensure(db, source)
                views.append(self._view(db, view['id']))
            self._audit(db, who, 'material_portal_submitted', {'request_id': request_id, 'batch_hash': batch_hash,
                'rights_version': employee['version'], 'bindings': [{'client_id': view['client_id'], 'draft_id': view['id']} for view in views]})
            return {'request_id': request_id, 'anforderungen': views}

    def _view(self, db, draft_id):
        view = self.p.material_dialog._view(db, draft_id)
        source = db.execute('SELECT * FROM einkauf_material_nachrichten WHERE id=?', (view['message_id'],)).fetchone()
        reference = _PHOTO_REF.fullmatch(source['wamid']) if source else None
        details = portal_request_details(source) if source else None
        kind = details['vorgang'] if details else 'bestellung'
        description = details['beschreibung'] if details else ''
        code = _code_view(None)
        if source and source['assistant_photo_id']:
            # Read only the server decoder's image-bound evidence. Client code,
            # dialog text and model-generated labels cannot establish this value.
            photo = db.execute('SELECT merkmale_json,file_sha256 FROM assistent_materialfotos WHERE foto_id=? AND actor=?',
                (source['assistant_photo_id'], 'mitarbeiter:' + str(source['employee_id']))).fetchone()
            if photo:
                try:
                    stored = json.loads(photo['merkmale_json'])
                    code = _code_view(stored.get(_CODE_KEY) if isinstance(stored, dict) else None, photo['file_sha256'])
                except (ValueError, TypeError):
                    pass
        decoded = code['codes'][0]['suchwert'] if code['status'] == 'erkannt' else ''
        labels = view['analysis'].get('merkmale', {})
        selected = view['fields'].get('selected_article', {}).get('value', {})
        manual = view['fields'].get('manual_article', {}).get('value', {})
        product = manual.get('product_name') or manual.get('produkt_name') or selected.get('produkt_name') or view['review'].get('product_name') or labels.get('produkt')
        if not product:
            product = description or ' '.join(str(labels.get(key) or '') for key in ('marke', 'materialtyp', 'masse')).strip() or 'Materialfoto'
        state = view['state']
        dispatch_state = view['dispatch_state']
        dispatch = getattr(getattr(self.p, 'workshop_orders', None), 'dispatch', None)
        if callable(getattr(dispatch, 'status', None)) and state not in {'external_pending', 'external_sent'}:
            durable = db.execute('SELECT id FROM assistent_bestellanforderungen WHERE request_id=?',
                                 ('material:' + str(draft_id),)).fetchone()
            if durable:
                state = 'accepted'
                try:
                    delivery = dispatch.status(durable['id'])
                    dispatch_state = delivery['state']
                except (ValueError, OSError, RuntimeError):
                    dispatch_state = 'uncertain'
        label = ('Foto wird ausgelesen' if view['analysis_state'] in {'pending', 'processing'} else
                 'Zusätzliche Bestellung bestätigen' if view['duplicate_of'] and view['employee_reply_required'] else
                 'Angabe zum Artikel erforderlich' if view['employee_reply_required'] else
                 {'review': 'Interne Bestellprüfung', 'approved': 'Zur Bestellung vorgemerkt',
                  'accepted': {'sent': 'Bestellt', 'queued': 'Sammelbestellung Montag 14 Uhr',
                               'copy_pending': 'Bestellt – Ablage wird ergänzt',
                               'uncertain': 'Versandstatus wird geprüft', 'partial': 'Versandstatus wird geprüft',
                               'blocked': 'Bestellversand gesperrt', 'not_sent': 'Versand noch offen'
                               }.get(dispatch_state, 'Bestellübergabe wird geprüft'),
                  'cancelled': 'Abgebrochen', 'external_pending': 'Extern reserviert',
                  'external_sent': 'Extern bestellt'}.get(state, 'Bedarf erfasst'))
        if view['error_code'] and state not in {'accepted', 'external_pending', 'external_sent', 'cancelled'}:
            label = 'Interne Prüfung erforderlich'
        if kind == 'anfrage' and state != 'cancelled':
            label = 'Bild wird geprüft' if view['analysis_state'] in {'pending', 'processing'} else 'Teileanfrage zur internen Prüfung'
        return {'client_id': reference[3] if reference else None, 'request_id': reference[2] if reference else None,
                'id': view['id'], 'code': view['code'], 'revision': view['revision'], 'product': product,
                'quantity': view['fields'].get('quantity', {}).get('value'),
                'unit': view['fields'].get('unit', {}).get('value'), 'urgent': view['fields'].get('urgent', {}).get('value'),
                'vorgang': kind, 'beschreibung': description,
                'code_erkennung': code, 'decodedCode': decoded,
                'state': state, 'dispatch_state': dispatch_state, 'label': label, 'duplicate_id': view['duplicate_of'],
                'analysis_state': view['analysis_state'], 'employee_reply_required': view['employee_reply_required'] if kind=='bestellung' else False,
                'questions': [{'field': q['field'], 'body': re.sub(r'^M-\d+\s+R\d+:\s*', '', q['body'])
                              .removesuffix(' Antworte bitte zitiert oder mit diesem Vorgangscode.')} for q in view['questions']
                              if q['revision'] == view['revision'] and q['state'] != 'superseded']}

    def list(self, who, request_id=None):
        if request_id is not None:
            request_id = _uuid(request_id)
        with self.db() as db:
            self._person(db, who)
            query = '''SELECT d.id FROM einkauf_material_dialoge d
                JOIN einkauf_material_nachrichten n ON n.id=d.message_id
                WHERE n.employee_id=? AND n.phone_number_id=?'''
            args = [who['mitarbeiter_id'], PORTAL_SOURCE]
            if request_id:
                query += ' AND n.wamid LIKE ? ORDER BY d.id'
                args.append('portal.' + str(who['mitarbeiter_id']) + '.' + request_id + '.%')
            else:
                query += ' ORDER BY d.id DESC LIMIT 100'
            rows = db.execute(query, args).fetchall()
            result = {'anforderungen': [self._view(db, row['id']) for row in rows]}
            if request_id:
                result['request_id'] = request_id
            return result

    def answer(self, who, draft_id, data):
        with self.p.portal_originals_operation_lock():
            return self._answer(who, draft_id, data)

    def _answer(self, who, draft_id, data):
        if not isinstance(data, dict) or set(data) != {'revision', 'antwort', 'request_id'}:
            raise ValueError('Antwort mit Vorgang, aktuellem Bearbeitungsstand und eindeutiger Referenz senden.')
        request_id = _uuid(data['request_id'])
        revision, answer = data['revision'], data['antwort']
        if (type(revision) is not int or revision <= 0 or not isinstance(answer, str)
                or not 1 <= len(answer.strip()) <= 200 or _note(answer, 200) != answer.strip()
                or re.search(r'\bM-\d+\s+R\d+\b', answer, re.I)):
            raise ValueError('Eine kurze eindeutige Antwort zum angezeigten Vorgang angeben.')
        with self.db() as db:
            employee = self._person(db, who, lock=True)
            draft = self.p.material_dialog._draft(db, draft_id)
            source = self.p.material_dialog._source(db, draft, lock=True)
            if source['employee_id'] != employee['id'] or source['phone_number_id'] != PORTAL_SOURCE:
                raise PermissionError('Dieser Vorgang gehört nicht zu deinem persönlichen Formular.')
            reference = 'portalreply.' + str(employee['id']) + '.' + request_id + '.' + str(draft_id)
            existing = db.execute('SELECT * FROM einkauf_material_texte WHERE phone_number_id=? AND wamid=?',
                                  (PORTAL_SOURCE, reference)).fetchone()
            body = 'M-' + str(draft_id) + ' R' + str(revision) + ': ' + answer.strip()
            if existing:
                if existing['body'] != body:
                    raise SubmissionConflict('Diese Antwortreferenz gehört bereits zu einer anderen Antwort.')
                if existing['state'] == 'applied':
                    view = self._view(db, draft_id)
                    return {'anforderung': view, 'anforderungen': [view], 'request_id': request_id}
                if existing['state'] != 'queued':
                    raise SubmissionConflict('Der Antwortstatus wird geprüft. Bitte den Vorgang neu laden.')
            else:
                if draft['revision'] != revision or self.p.material_dialog._accepted(db, draft) or draft['state'] == 'cancelled':
                    raise SubmissionConflict('Dieser Vorgang wurde bereits geändert oder übergeben. Bitte neu laden.')
                event = {'phone_number_id': PORTAL_SOURCE, 'wamid': reference, 'reply_to': source['wamid'],
                         'body': body, 'forwarded': False, 'source_at': datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()}
                self.p.material_dialog.receive_text(db, event, {'id': 0, 'revision': employee['version']}, employee)
            text = db.execute('SELECT id FROM einkauf_material_texte WHERE phone_number_id=? AND wamid=?',
                              (PORTAL_SOURCE, reference)).fetchone()
            text_id = text['id']
            self._audit(db, who, 'material_portal_answer', {'draft_id': draft_id, 'revision': revision,
                'request_id': request_id, 'text_id': text_id})
        result = self.p.material_dialog.process_text(text_id=text_id)
        if not result or result['state'] != 'applied':
            raise SubmissionConflict('Die Antwort konnte nicht eindeutig übernommen werden. Bitte den Vorgang neu laden.')
        with self.db() as db:
            view = self._view(db, draft_id)
            return {'anforderung': view, 'anforderungen': [view], 'request_id': request_id}

    def process_next(self):
        """One original analysis or guarded handoff in the existing worker tick."""
        return self._process_next()

    def _process_next(self):
        with self.p.portal_originals_operation_lock(), self.db() as db:
            text = db.execute("SELECT id FROM einkauf_material_texte WHERE phone_number_id=? AND state='queued' ORDER BY id LIMIT 1",
                              (PORTAL_SOURCE,)).fetchone()
        if text:
            return self.p.material_dialog.process_text(text_id=text['id'])
        with self.p.portal_originals_operation_lock(), self.db() as db:
            row = db.execute('''SELECT d.id,d.revision,d.state,d.analysis_lease,d.message_id FROM einkauf_material_dialoge d
                JOIN einkauf_material_nachrichten n ON n.id=d.message_id
                WHERE n.phone_number_id=? AND d.state NOT IN ('accepted','cancelled','review','external_pending','external_sent')
                AND (d.state='approved' OR d.analysis_state='pending'
                     OR (d.analysis_state='processing' AND d.analysis_until<?))
                ORDER BY d.id LIMIT 1''', (PORTAL_SOURCE, self.clock())).fetchone()
        if not row:
            return None
        dialog = self.p.material_dialog
        try:
            if row['state'] == 'approved':
                # Dispatch and its acknowledgement remain one protected mutation.
                with self.p.portal_originals_operation_lock():
                    result = self.p.workshop_orders.submit_material_request(row['id'], row['revision'])
                    dialog.order_attempt(row['id'], row['revision'], result)
                    return {'id': row['id'], 'state': result['state']}
            view = dialog.analyze(row['id'])
            return {'id': row['id'], 'state': view['state']}
        except (ValueError, PermissionError):
            with self.p.portal_originals_operation_lock(), self.db() as db:
                db.execute('''UPDATE einkauf_material_dialoge SET state='review',error_code='portal_bedarf_intern_pruefen',updated_at=?
                    WHERE id=? AND revision=? AND state=? AND analysis_lease=? AND message_id=?
                    AND state NOT IN ('accepted','cancelled','external_pending','external_sent')
                    AND NOT EXISTS (SELECT 1 FROM assistent_bestellanforderungen WHERE request_id=?)''',
                    (self.clock(), row['id'], row['revision'], row['state'], row['analysis_lease'],
                     row['message_id'], 'material:' + str(row['id'])))
                current = db.execute('SELECT state FROM einkauf_material_dialoge WHERE id=?', (row['id'],)).fetchone()
            return {'id': row['id'], 'state': current['state'] if current else 'missing'}


def register_material_order_portal(p):
    if 'werkstatt_materialbestellung' in p.app.extensions:
        return p.app.extensions['werkstatt_materialbestellung']
    service = MaterialOrderPortal(p)
    p.material_order_portal = service
    p.app.extensions['werkstatt_materialbestellung'] = service
    bp = Blueprint('werkstatt_materialbestellung', __name__, url_prefix='/werkstatt/materialbestellung')

    def upload_limit():
        # Must precede the app-wide CSRF parser: the global 25 MB limit remains
        # unchanged for all other endpoints. Only personal uploads get 50 MB.
        if request.endpoint in {'werkstatt_materialbestellung.submit', 'werkstatt_materialbestellung.answer',
                                'werkstatt_materialbestellung.preview'}:
            who = service.identity()
            if not who:
                return jsonify(error='Persönliche Anmeldung erforderlich.', accepted=False), 401
            if not service.can_order(who):
                return jsonify(error='Persönliche Einkaufrechte fehlen.', accepted=False), 403
            supplied = request.headers.get('X-CSRF-Token')
            expected = session.get('csrf_token')
            if request.endpoint == 'werkstatt_materialbestellung.preview':
                request.max_content_length = MAX_PREVIEW_BODY_BYTES
                if supplied is None:
                    return jsonify(error='Sicherheitsprüfung fehlgeschlagen. Bitte die Seite neu laden.', accepted=False), 403
            if supplied is not None and (not isinstance(expected, str) or not hmac.compare_digest(expected, supplied)):
                return jsonify(error='Dein persönlicher Zugang hat sich geändert. Bitte die Seite neu laden.',
                               accepted=False, reload_required=True), 403
            if request.endpoint == 'werkstatt_materialbestellung.submit' and request.method == 'POST':
                request.max_content_length = MAX_BODY_BYTES
    p.app.before_request_funcs.setdefault(None, []).insert(0, upload_limit)

    @bp.after_request
    def privacy(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['Referrer-Policy'] = 'same-origin'
        response.headers['Permissions-Policy'] = 'camera=(self), microphone=(), geolocation=(), payment=()'
        return response

    @bp.errorhandler(ValueError)
    def invalid(exc):
        data = {'error': str(exc)}
        if request.endpoint == 'werkstatt_materialbestellung.submit' or not isinstance(exc, SubmissionConflict):
            data['accepted'] = False
        return jsonify(data), 409 if isinstance(exc, SubmissionConflict) else 400

    @bp.errorhandler(PermissionError)
    def denied(exc):
        return jsonify(error=str(exc), accepted=False), 403

    @bp.errorhandler(LabelPreviewBusy)
    def label_busy(exc):
        response = jsonify(error=str(exc), accepted=False)
        response.status_code = 429
        response.headers['Retry-After'] = '5'
        return response

    @bp.errorhandler(RequestEntityTooLarge)
    def oversized(exc):
        return jsonify(error='Alle Fotos zusammen dürfen höchstens 50 MB groß sein; je Foto höchstens 8 MB.', accepted=False), 413

    def protected():
        who = service.identity()
        if not who:
            return None, (jsonify(error='Persönliche Anmeldung erforderlich.', accepted=False), 401)
        if not service.can_order(who):
            return None, (jsonify(error='Persönliche Einkaufrechte fehlen.', accepted=False), 403)
        # Bind reads/reconciliation as well as mutations to the personal page
        # opened before a possible account change in another browser tab.
        if request.method in {'GET', 'POST'}:
            expected = session.get('csrf_token')
            supplied = request.headers.get('X-CSRF-Token') or request.form.get('csrf_token')
            if not isinstance(expected, str) or not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied):
                return None, (jsonify(error='Sicherheitsprüfung fehlgeschlagen. Bitte die Seite neu laden.', accepted=False), 403)
        return who, None

    @bp.post('/artikelscan-vorschau')
    def preview():
        who, error = protected()
        if error:
            return error
        if not service.preview_allowed(who['actor']):
            response = jsonify(error='Viele Fotos geprüft. Bitte kurz warten; du kannst den Bestellwunsch bereits senden.', accepted=False)
            response.status_code = 429
            response.headers['Retry-After'] = '60'
            return response
        if (set(request.form.keys()) - {'csrf_token', 'modus', 'abgelehnte_codes'} or set(request.files.keys()) != {'foto'}
                or len(request.files.getlist('foto')) != 1):
            raise ValueError('Für die Artikelvorschau genau ein Foto übermitteln.')
        mode = request.form.get('modus', 'code')
        if mode not in {'code', 'etikett'}:
            raise ValueError('Code- oder Etikettvorschau auswählen.')
        try:
            rejected = _rejected_codes(json.loads(request.form.get('abgelehnte_codes', '[]')))
        except (ValueError, TypeError):
            raise ValueError('Abgelehnte Artikelcodes konnten nicht gelesen werden.') from None
        result = p.assistant_material_photos.preview(who, request.files.get('foto'), read_label=mode == 'etikett', rejected_codes=rejected)
        current = service.identity()
        if (not service.can_order(current) or current['actor'] != who['actor']
                or current['version'] != who['version']):
            raise PermissionError('Deine persönlichen Rechte haben sich geändert. Bitte die Seite neu laden.')
        return jsonify(result)

    @bp.get('')
    def page():
        who = service.identity()
        login_next = '/werkstatt/mein-konto' if request.args.get('next') == 'profil' else '/werkstatt/materialbestellung'
        token = session.get('csrf_token')
        if not token:
            token = secrets.token_urlsafe(32)
            session['csrf_token'] = token
        employees = []
        if not who:
            with service.db() as db:
                employees = [dict(row) for row in db.execute('''SELECT m.id,m.name FROM mitarbeiter m
                    JOIN assistent_rechte r ON r.mitarbeiter_id=m.id
                    WHERE m.aktiv=1 AND r.lesen=1 AND r.einkaufen=1 AND r.limit_cent>0
                    AND LENGTH(COALESCE(r.passwort_hash,''))>0 ORDER BY m.name''').fetchall()]
        order_limit_cent = 0
        if service.can_order(who):
            try:
                cap = getattr(getattr(p, 'workshop_orders', None), 'cap', None)
                global_limit = cap() if callable(cap) else 0
                if type(global_limit) is int and global_limit > 0:
                    order_limit_cent = min(25000, who['limit_cent'], global_limit)
            except Exception:
                # A display limit is no purchase permit. Missing configuration
                # must not invent a 250 EUR permission or prevent login display.
                order_limit_cent = 0
        return render_template('materialbestellung.html', who=who, auth=bool(who), can_order=service.can_order(who),
            csrf_token=token, employees=employees, login_next=login_next, max_photos=MAX_PHOTOS, max_photo_bytes=MAX_PHOTO_BYTES,
            max_total_bytes=MAX_TOTAL_BYTES, order_limit_cent=order_limit_cent)

    @bp.route('/anforderungen', methods=['GET', 'POST'])
    def submit():
        who, error = protected()
        if error:
            return error
        if request.method == 'GET':
            return jsonify(service.list(who, request.args.get('request_id')))
        if set(request.form.keys()) - {'request_id', 'positionen', 'csrf_token'}:
            raise ValueError('Nur Bilder mit Stückzahlen, Dringlichkeit und optional Beschreibung oder Anfrage übermitteln.')
        try:
            positions = json.loads(request.form.get('positionen', ''))
        except (ValueError, TypeError):
            raise ValueError('Fotoeinträge konnten nicht gelesen werden. Bitte das Formular neu öffnen.') from None
        return jsonify(service.submit(who, request.form.get('request_id'), positions, request.files))

    @bp.post('/anforderungen/<int:draft_id>/antwort')
    def answer(draft_id):
        who, error = protected()
        if error:
            return error
        return jsonify(service.answer(who, draft_id, request.get_json(silent=True)))

    p.app.register_blueprint(bp)
    return service
