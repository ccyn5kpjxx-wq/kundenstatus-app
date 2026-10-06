"""Opt-in, signed WhatsApp direct-image intake; no messages or orders sent.

Registration never starts a worker. Root owns the existing webhook and must
keep non-text messages out of the legacy vehicle-chat handler. This receiver
does not consume normal WhatsApp groups or claim Business-App synchronization.
References: Meta's official Postman WhatsApp Business Platform / Media and
Media Object; github.com/fbsamples/whatsapp-api-examples signature validation.
"""
import base64
from contextlib import contextmanager
import copy
from datetime import datetime, timezone
import hashlib
import hmac
import io
import json
import os
import re
import secrets
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import click
import requests
from werkzeug.datastructures import FileStorage

from werkstatt_einkaufseingang import _has_bank_data, _SECRET, _image_or_pdf


TABLES = ('einkauf_material_absender', 'einkauf_material_nachrichten', 'einkauf_material_worker')
MAX_WEBHOOK_BYTES = 256 * 1024
MAX_IMAGE_BYTES = 5 * 1024 * 1024
MAX_METADATA_BYTES = 64 * 1024
MAX_ATTEMPTS = 5
MEDIA_HOSTS = frozenset({'lookaside.fbsbx.com', 'lookaside.facebook.com', 'graph.facebook.com'})
IMAGE_TYPES = {'image/jpeg': '.jpg', 'image/png': '.png'}
_WORKER_LOCK = threading.Lock()
_WORKER_THREAD = None


class ChannelError(ValueError):
    def __init__(self, code, retry=False):
        self.code, self.retry = code, retry
        super().__init__(code)


def _key(value, maximum=200):
    return isinstance(value, str) and 0 < len(value) <= maximum and not any(ord(c) < 33 or ord(c) > 126 for c in value)


def _phone(value):
    if not isinstance(value, str):
        raise ValueError('Telefonnummer mit internationaler Vorwahl angeben.')
    value = value.strip().removeprefix('+')
    if not re.fullmatch(r'[1-9][0-9]{6,14}', value):
        raise ValueError('Telefonnummer eindeutig im internationalen Format angeben, z. B. +49…; keine Ortsnummer ergänzen.')
    return value


def _numeric_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[1-9][0-9]{0,63}', value):
        raise ValueError('Ungültige Meta-Referenz.')
    return value


def _row_id(value):
    if type(value) is int and value > 0:
        return value
    raise ValueError('Ungültige Mitarbeiter-/Zuordnungsreferenz.')


def _digest(value):
    if isinstance(value, str) and re.fullmatch(r'[a-fA-F0-9]{64}', value):
        return value.lower()
    try:
        raw = base64.b64decode(value, validate=True) if isinstance(value, str) else b''
    except (ValueError, TypeError):
        raw = b''
    if len(raw) != 32:
        raise ValueError('Bild-Prüfsumme fehlt oder ist ungültig.')
    return raw.hex()


def _note(value, maximum=1200):
    if not isinstance(value, str):
        return ''
    # Captions remain text evidence, never instructions, permissions or fields.
    clean = '\n'.join(line for line in value.splitlines() if not _has_bank_data(line) and not _SECRET.search(line))
    return ''.join(c for c in clean if (ord(c) >= 32 or c in '\n\t') and not 0xD800 <= ord(c) <= 0xDFFF)[:maximum].strip()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _fingerprint(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


class _BorrowedConnection:
    """Nested intake services share the caller's atomic transaction."""
    def __init__(self, connection):
        self.connection = connection
    def execute(self, *args, **kwargs):
        return self.connection.execute(*args, **kwargs)
    def executescript(self, *args, **kwargs):
        return self.connection.executescript(*args, **kwargs)
    def commit(self):
        pass
    def rollback(self):
        pass
    def close(self):
        pass


class MaterialChannel:
    def __init__(self, portal, transport=None, clock=None):
        self.p = portal
        self.transport = transport or requests
        self.clock = clock or time.time
        for key in ('MATERIAL_WHATSAPP_ENABLED', 'MATERIAL_WHATSAPP_WORKER_ENABLED', 'MATERIAL_WHATSAPP_SHARE_PORTAL_NUMBER'):
            portal.app.config.setdefault(key, getattr(portal, key, os.environ.get(key, '').lower() in {'1', 'true', 'yes'}))
        portal.app.config.setdefault('MATERIAL_WHATSAPP_PHONE_IDS', getattr(portal, 'MATERIAL_WHATSAPP_PHONE_IDS', os.environ.get('MATERIAL_WHATSAPP_PHONE_IDS', '')))
        self.worker_thread = None
        self.worker_stop = threading.Event()
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
                CREATE TABLE IF NOT EXISTS einkauf_material_absender (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, phone_e164 TEXT NOT NULL UNIQUE,
                    employee_id INTEGER NOT NULL, source_note TEXT NOT NULL, verified_at TEXT NOT NULL,
                    verified_by TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1,
                    revision INTEGER NOT NULL DEFAULT 1);
                CREATE TABLE IF NOT EXISTS einkauf_material_nachrichten (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, phone_number_id TEXT NOT NULL,
                    wamid TEXT NOT NULL, canonical_hash TEXT NOT NULL, sender_id INTEGER NOT NULL,
                    sender_revision INTEGER NOT NULL, employee_id INTEGER NOT NULL, employee_name TEXT NOT NULL,
                    rights_version INTEGER NOT NULL, media_id TEXT NOT NULL, mime TEXT NOT NULL,
                    expected_sha256 TEXT NOT NULL, caption TEXT NOT NULL DEFAULT '', forwarded INTEGER NOT NULL DEFAULT 0,
                    source_at TEXT NOT NULL, received_at DOUBLE PRECISION NOT NULL,
                    state TEXT NOT NULL DEFAULT 'queued', lease_token TEXT NOT NULL DEFAULT '',
                    lease_until DOUBLE PRECISION NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                    retry_at DOUBLE PRECISION NOT NULL DEFAULT 0, intake_id INTEGER, file_id INTEGER,
                    assistant_photo_id TEXT NOT NULL DEFAULT '',
                    error_code TEXT NOT NULL DEFAULT '', updated_at DOUBLE PRECISION NOT NULL,
                    UNIQUE(phone_number_id,wamid));
                CREATE TABLE IF NOT EXISTS einkauf_material_worker (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, worker_key TEXT NOT NULL UNIQUE,
                    heartbeat_at DOUBLE PRECISION NOT NULL, last_error TEXT NOT NULL DEFAULT '');
            ''')

    def _config(self):
        value = self.p.app.config.get('MATERIAL_WHATSAPP_PHONE_IDS')
        values = value.split(',') if isinstance(value, str) else value if isinstance(value, (list, tuple)) else []
        try:
            ids = {_numeric_id(item.strip()) for item in values if isinstance(item, str) and item.strip()}
        except ValueError:
            ids = set()
        version = getattr(self.p, 'WHATSAPP_GRAPH_VERSION', '')
        if not isinstance(version, str) or not re.fullmatch(r'v[0-9]{1,3}\.[0-9]', version):
            version = ''
        portal_receiver = getattr(self.p, 'WHATSAPP_PHONE_NUMBER_ID', '')
        shared_receiver = (portal_receiver if self.p.app.config.get('MATERIAL_WHATSAPP_SHARE_PORTAL_NUMBER') is True
                           and isinstance(portal_receiver, str) and portal_receiver in ids else '')
        return {'enabled': self.p.app.config.get('MATERIAL_WHATSAPP_ENABLED') is True,
                'ids': ids, 'secret': getattr(self.p, 'WHATSAPP_APP_SECRET', '') or '',
                'token': getattr(self.p, 'WHATSAPP_ACCESS_TOKEN', '') or '', 'version': version,
                'shared_receiver_id': shared_receiver}

    def vehicle_routing_exclusions(self):
        """Keep material senders out of vehicle conversations, also after revocation.

        Sharing is an explicit opt-in for exactly the existing portal receiver.
        All other material receivers stay exclusive. Registry ownership, rather
        than current employee rights or intake activation, determines routing.
        """
        config = self._config()
        shared_receiver = config['shared_receiver_id']
        # Invalid intake configuration must not release an already reserved
        # receiver to the vehicle chat. Match the webhook's previous raw list
        # exclusion even when _config rejects the allowlist as a whole.
        value = self.p.app.config.get('MATERIAL_WHATSAPP_PHONE_IDS', '')
        values = value.split(',') if isinstance(value, str) else value if isinstance(value, (list, tuple)) else []
        reserved_receivers = {item.strip() for item in values if isinstance(item, str) and item.strip()}
        reserved_pairs = []
        if shared_receiver:
            with self.db() as db:
                rows = db.execute('SELECT phone_e164 FROM einkauf_material_absender ORDER BY phone_e164').fetchall()
            reserved_pairs = [(shared_receiver, row['phone_e164']) for row in rows]
        return {'exclusive_receiver_ids': sorted(reserved_receivers - {shared_receiver}),
                'reserved_sender_pairs': reserved_pairs}

    @staticmethod
    def _employee(db, employee_id):
        row = db.execute('''SELECT m.id,m.name,m.aktiv,r.lesen,r.einkaufen,r.version
            FROM mitarbeiter m LEFT JOIN assistent_rechte r ON r.mitarbeiter_id=m.id WHERE m.id=?''', (employee_id,)).fetchone()
        if not row or not row['aktiv'] or not row['lesen'] or not row['einkaufen'] or type(row['version']) is not int:
            raise PermissionError('Mitarbeiter ist nicht aktiv oder Materialrechte fehlen.')
        return dict(row)

    def readiness(self):
        config = self._config()
        with self.db() as db:
            rows = db.execute('SELECT * FROM einkauf_material_absender WHERE active=1').fetchall()
            heartbeat = db.execute("SELECT heartbeat_at,last_error FROM einkauf_material_worker WHERE worker_key='material' ORDER BY id DESC LIMIT 1").fetchone()
            eligible = 0
            for row in rows:
                try:
                    self._employee(db, row['employee_id'])
                    eligible += 1
                except PermissionError:
                    pass
        blockers = []
        for okay, label in ((config['enabled'], 'Materialkanal ist deaktiviert.'), (bool(config['secret']), 'Webhook-App-Secret fehlt.'),
                            (bool(config['token']), 'Meta-Medienzugang fehlt.'), (bool(config['ids']), 'Bestätigte Phone-Number-ID-Allowlist fehlt.'),
                            (bool(config['version']), 'Graph-API-Version fehlt.'), (eligible > 0, 'Verifizierte Mitarbeiterzuordnung mit Materialrechten fehlt.'),
                            (bool(getattr(self.p, 'assistant_material_photos', None)), 'Persönlicher Fotodienst fehlt.'),
                            (bool(getattr(self.p, 'workshop_intake', None)), 'Materialeingang fehlt.')):
            if not okay:
                blockers.append(label)
        vehicle_receiver = getattr(self.p, 'WHATSAPP_PHONE_NUMBER_ID', '')
        routing_conflict = bool(isinstance(vehicle_receiver, str) and vehicle_receiver.strip()
                                and vehicle_receiver.strip() in config['ids'] and not config['shared_receiver_id'])
        if routing_conflict:
            blockers.append('Nummernkonflikt: Die Bestellnummer ist zugleich für den Fahrzeugchat hinterlegt. '
                            'Der Bestellkanal schließt diese Nummer auch während einer Pause vom Fahrzeugchat aus. '
                            'Eine separate Nummer zuordnen oder die bestehende Nutzung ausdrücklich klären.')
        worker_signal = bool(heartbeat and 0 <= self.clock() - heartbeat['heartbeat_at'] < 90)
        worker_last_error = heartbeat['last_error'] if heartbeat else ''
        worker_healthy = bool(worker_signal and not worker_last_error)
        replies_enabled = self.p.app.config.get('MATERIAL_WHATSAPP_REPLIES_ENABLED') is True
        dialog_ready = bool(getattr(self.p, 'material_dialog', None))
        operation_blockers = list(blockers)
        for okay, label in ((dialog_ready, 'Bestelldialog ist noch nicht eingerichtet.'),
                            (replies_enabled, 'Rückfragen per WhatsApp sind noch nicht eingeschaltet.'),
                            (worker_signal, 'Ein aktueller Hintergrundlauf ist noch nicht bestätigt.'),
                            (not worker_last_error, 'Der letzte Hintergrundlauf meldet einen Verarbeitungsfehler.')):
            if not okay:
                operation_blockers.append(label)
        return {'enabled': config['enabled'], 'configured': bool(config['secret'] and config['token'] and config['ids'] and config['version']),
                'ready': not blockers, 'secret_present': bool(config['secret']), 'token_present': bool(config['token']),
                'operational_ready': not operation_blockers, 'operation_blockers': operation_blockers,
                'routing_conflict': routing_conflict, 'replies_enabled': replies_enabled, 'dialog_ready': dialog_ready,
                'routing_mode': 'shared_portal' if config['shared_receiver_id'] else 'exclusive',
                'portal_number_shared': bool(config['shared_receiver_id']),
                'phone_ids_count': len(config['ids']), 'verified_senders': len(rows), 'eligible_senders': eligible,
                'direct_messages_only': True, 'automatic_worker_started': bool(self.worker_thread and self.worker_thread.is_alive()),
                'worker_enabled': self.p.app.config.get('MATERIAL_WHATSAPP_WORKER_ENABLED') is True,
                'worker_signal': worker_signal, 'worker_healthy': worker_healthy,
                'worker_heartbeat_at': heartbeat['heartbeat_at'] if heartbeat else None,
                'worker_last_error': worker_last_error, 'blockers': blockers}

    def list_senders(self):
        with self.db() as db:
            rows = db.execute('SELECT s.*,m.name FROM einkauf_material_absender s LEFT JOIN mitarbeiter m ON m.id=s.employee_id ORDER BY s.id').fetchall()
        return [dict(row) for row in rows]

    def verify_sender(self, employee_id, phone_e164, source_note, *, confirmed=False):
        employee_id, phone = _row_id(employee_id), _phone(phone_e164)
        note = _note(source_note, 500)
        if confirmed is not True or not note:
            raise ValueError('Persönliche Nummer und Zuordnungsnachweis ausdrücklich bestätigen.')
        with self.db() as db:
            self._employee(db, employee_id)
            inserted = db.execute('''INSERT INTO einkauf_material_absender
                (phone_e164,employee_id,source_note,verified_at,verified_by) VALUES(?,?,?,?,?)
                ON CONFLICT(phone_e164) DO NOTHING RETURNING id''',
                (phone, employee_id, note, datetime.now(timezone.utc).isoformat(), 'admin')).fetchone()
            row = dict(db.execute('SELECT * FROM einkauf_material_absender WHERE phone_e164=?', (phone,)).fetchone())
            if not inserted and (row['employee_id'] != employee_id or not row['active']):
                raise ValueError('Diese Nummer ist bereits fest zugeordnet oder widerrufen. Keine automatische Neuzuordnung.')
        return row

    def revoke_sender(self, sender_id, revision):
        sender_id, revision = _row_id(sender_id), _row_id(revision)
        with self.db() as db:
            changed = db.execute('UPDATE einkauf_material_absender SET active=0,revision=revision+1 WHERE id=? AND revision=? AND active=1', (sender_id, revision)).rowcount
            if changed != 1:
                raise ValueError('Zuordnung wurde bereits geändert. Bitte neu laden.')
        return {'revoked': True}

    def ingest_webhook(self, raw_bytes, signature):
        config = self._config()
        result = {'enabled': config['enabled'], 'accepted': 0, 'duplicates': 0, 'ignored': 0}
        if not config['enabled']:
            return result
        if not isinstance(raw_bytes, bytes) or len(raw_bytes) > MAX_WEBHOOK_BYTES:
            raise ValueError('Webhook ist zu groß oder ungültig.')
        if not config['secret'] or not isinstance(signature, str) or not re.fullmatch(r'sha256=[a-fA-F0-9]{64}', signature):
            raise PermissionError('Signierter Meta-Webhook erforderlich.')
        expected = 'sha256=' + hmac.new(config['secret'].encode(), raw_bytes, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature.lower()):
            raise PermissionError('Signierter Meta-Webhook erforderlich.')
        try:
            payload = json.loads(raw_bytes)
        except (ValueError, UnicodeError) as exc:
            raise ValueError('Ungültiger Webhook.') from exc
        if not isinstance(payload, dict) or payload.get('object') != 'whatsapp_business_account' or not isinstance(payload.get('entry'), list) or len(payload['entry']) > 20:
            raise ValueError('Unerwarteter Webhook-Typ.')
        events = []
        for entry in payload['entry']:
            changes = entry.get('changes') if isinstance(entry, dict) else None
            if not isinstance(changes, list) or len(changes) > 20:
                raise ValueError('Ungültige Webhook-Änderungen.')
            for change in changes:
                value = change.get('value') if isinstance(change, dict) else None
                if not isinstance(value, dict) or change.get('field') != 'messages' or value.get('messaging_product') != 'whatsapp':
                    result['ignored'] += 1
                    continue
                meta = value.get('metadata') if isinstance(value.get('metadata'), dict) else {}
                if not isinstance(meta.get('phone_number_id'), str) or meta['phone_number_id'] not in config['ids']:
                    result['ignored'] += 1
                    continue
                messages = value.get('messages', [])
                if not isinstance(messages, list) or len(messages) > 100:
                    raise ValueError('Ungültige Nachrichtenliste.')
                for message in messages:
                    context = message.get('context') if isinstance(message, dict) and isinstance(message.get('context'), dict) else {}
                    if (not isinstance(message, dict) or message.get('type') not in {'image','text'} or message.get('group_id') or value.get('group_id')
                            or context.get('group_id') or message.get('recipient_type') == 'group' or value.get('recipient_type') == 'group'):
                        result['ignored'] += 1
                        continue
                    try:
                        if message['type'] == 'text':
                            if not getattr(self.p,'material_dialog',None):
                                result['ignored'] += 1
                                continue
                            event = self._text_event(meta['phone_number_id'], message)
                        else:
                            event = self._event(meta['phone_number_id'], message)
                    except ValueError:
                        result['ignored'] += 1
                        continue
                    events.append(event)
                    if len(events) > 100:
                        raise ValueError('Zu viele Bildnachrichten im Webhook.')
        # Do not partially accept a malformed batch before validation finished.
        with self.db() as db:
            for event in events:
                sender = db.execute('SELECT * FROM einkauf_material_absender WHERE phone_e164=? AND active=1', (event['phone'],)).fetchone()
                if not sender:
                    result['ignored'] += 1
                    continue
                try:
                    employee = self._employee(db, sender['employee_id'])
                except PermissionError:
                    result['ignored'] += 1
                    continue
                if event.get('message_type') == 'text':
                    inserted = self.p.material_dialog.receive_text(db,event,sender,employee)
                    result['accepted' if inserted else 'duplicates'] += 1
                    continue
                fingerprint = _fingerprint(event)
                old = db.execute('SELECT canonical_hash FROM einkauf_material_nachrichten WHERE phone_number_id=? AND wamid=?', (event['phone_number_id'], event['wamid'])).fetchone()
                if old:
                    if old['canonical_hash'] != fingerprint:
                        raise ValueError('Nachrichtenkennung wurde mit anderem Inhalt wiederholt.')
                    result['duplicates'] += 1
                    continue
                row = db.execute('''INSERT INTO einkauf_material_nachrichten
                    (phone_number_id,wamid,canonical_hash,sender_id,sender_revision,employee_id,employee_name,rights_version,
                     media_id,mime,expected_sha256,caption,forwarded,source_at,received_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(phone_number_id,wamid) DO NOTHING RETURNING id''',
                    (event['phone_number_id'], event['wamid'], fingerprint, sender['id'], sender['revision'], employee['id'], _note(employee['name'], 200), employee['version'],
                     event['media_id'], event['mime'], event['expected_sha256'], event['caption'], int(event['forwarded']), event['source_at'], self.clock(), self.clock())).fetchone()
                if row:
                    result['accepted'] += 1
                else:
                    existing = db.execute('SELECT canonical_hash FROM einkauf_material_nachrichten WHERE phone_number_id=? AND wamid=?', (event['phone_number_id'], event['wamid'])).fetchone()
                    if not existing or existing['canonical_hash'] != fingerprint:
                        raise ValueError('Nachrichtenkennung wurde mit anderem Inhalt wiederholt.')
                    result['duplicates'] += 1
        return result

    @staticmethod
    def _text_event(phone_id, message):
        text = message.get('text')
        body = _note(text.get('body')) if isinstance(text,dict) else ''
        if not body or not _key(message.get('id'),400) or not message['id'].startswith('wamid.'):
            raise ValueError('Eindeutige Textnachricht fehlt.')
        stamp = message.get('timestamp')
        if not isinstance(stamp,str) or not re.fullmatch(r'[0-9]{1,12}',stamp):
            raise ValueError('Nachrichtenzeitpunkt fehlt.')
        try:
            source_at = datetime.fromtimestamp(int(stamp),timezone.utc).isoformat()
        except (ValueError,OverflowError,OSError) as exc:
            raise ValueError('Nachrichtenzeitpunkt ungültig.') from exc
        context = message.get('context') if isinstance(message.get('context'),dict) else {}
        reply = context.get('id','')
        if reply and (not _key(reply,400) or not reply.startswith('wamid.')):
            raise ValueError('Ungültiger Antwortbezug.')
        return {'message_type':'text','phone_number_id':phone_id,'phone':_phone(message.get('from')),
            'wamid':message['id'],'body':body,'source_at':source_at,'reply_to':reply,
            'forwarded':context.get('forwarded') is True or context.get('frequently_forwarded') is True}

    @staticmethod
    def _event(phone_id, message):
        media = message.get('image')
        if not isinstance(media, dict) or media.get('mime_type') not in IMAGE_TYPES:
            raise ValueError('Nur JPEG-/PNG-Materialbilder erlaubt.')
        wamid = message.get('id')
        if not _key(wamid, 400) or not wamid.startswith('wamid.'):
            raise ValueError('Nachrichtenreferenz fehlt.')
        stamp = message.get('timestamp')
        if not isinstance(stamp, str) or not re.fullmatch(r'[0-9]{1,12}', stamp):
            raise ValueError('Nachrichtenzeitpunkt fehlt.')
        try:
            source_at = datetime.fromtimestamp(int(stamp), timezone.utc).isoformat()
        except (ValueError, OverflowError, OSError) as exc:
            raise ValueError('Nachrichtenzeitpunkt ungültig.') from exc
        context = message.get('context') if isinstance(message.get('context'), dict) else {}
        return {'phone_number_id': phone_id, 'wamid': wamid, 'phone': _phone(message.get('from')),
                'media_id': _numeric_id(media.get('id')), 'mime': media['mime_type'], 'expected_sha256': _digest(media.get('sha256')),
                'caption': _note(media.get('caption')), 'source_at': source_at,
                'forwarded': context.get('forwarded') is True or context.get('frequently_forwarded') is True}

    def status(self, limit=100):
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError('Ungültige Listengröße.')
        with self.db() as db:
            return [dict(row) for row in db.execute('''SELECT id,employee_id,employee_name,caption,mime,forwarded,source_at,
                received_at,state,attempts,retry_at,intake_id,file_id,assistant_photo_id,error_code FROM einkauf_material_nachrichten
                ORDER BY id DESC LIMIT ?''', (limit,)).fetchall()]

    def _active(self, db, row, *, lock=False):
        config = self._config()
        if not config['enabled'] or row['phone_number_id'] not in config['ids'] or not config['secret'] or not config['token'] or not config['version']:
            raise PermissionError('Materialkanal pausiert oder nicht vollständig eingerichtet.')
        if lock:
            db.execute('UPDATE einkauf_material_absender SET revision=revision WHERE id=?', (row['sender_id'],))
            db.execute('UPDATE mitarbeiter SET aktiv=aktiv WHERE id=?', (row['employee_id'],))
            db.execute('UPDATE assistent_rechte SET version=version WHERE mitarbeiter_id=?', (row['employee_id'],))
        sender = db.execute('SELECT * FROM einkauf_material_absender WHERE id=?', (row['sender_id'],)).fetchone()
        employee = self._employee(db, row['employee_id'])
        if (not sender or not sender['active'] or sender['employee_id'] != row['employee_id'] or
                sender['revision'] != row['sender_revision'] or employee['version'] != row['rights_version']):
            raise PermissionError('Persönliche Materialfreigabe wurde seit dem Eingang geändert.')
        return employee

    @staticmethod
    def _response_bytes(response, maximum):
        started = time.monotonic()
        try:
            status = response.status_code
            if status != 200:
                raise ChannelError('meta_voruebergehend_nicht_erreichbar' if status == 429 or status >= 500 else 'meta_zugriff_abgewiesen', retry=status == 429 or status >= 500)
            length = response.headers.get('Content-Length')
            if length is not None and (not str(length).isdigit() or int(length) > maximum):
                raise ChannelError('medium_zu_gross_oder_ungueltige_laenge')
            result = bytearray()
            for block in response.iter_content(chunk_size=65536):
                if time.monotonic() - started > 30:
                    raise ChannelError('meta_voruebergehend_nicht_erreichbar', retry=True)
                if not block:
                    continue
                result.extend(block)
                if len(result) > maximum:
                    raise ChannelError('medium_zu_gross_oder_ungueltige_laenge')
            if not result or length is not None and len(result) != int(length):
                raise ChannelError('medium_unvollstaendig')
            return bytes(result)
        finally:
            response.close()

    def _download(self, row):
        config = self._config()
        headers = {'Authorization': 'Bearer ' + config['token']}
        try:
            response = self.transport.get('https://graph.facebook.com/' + config['version'] + '/' + row['media_id'],
                params={'phone_number_id': row['phone_number_id']}, headers=headers, timeout=(5, 20), allow_redirects=False, stream=True)
            meta = json.loads(self._response_bytes(response, MAX_METADATA_BYTES))
            if not isinstance(meta, dict) or str(meta.get('id')) != row['media_id'] or meta.get('mime_type') != row['mime'] or meta.get('messaging_product', 'whatsapp') != 'whatsapp':
                raise ChannelError('medienmetadaten_passen_nicht')
            size = meta.get('file_size')
            if isinstance(size, bool) or not isinstance(size, (int, str)) or not str(size).isdigit() or not 0 < int(size) <= MAX_IMAGE_BYTES:
                raise ChannelError('medium_zu_gross_oder_ungueltige_laenge')
            if _digest(meta.get('sha256')) != row['expected_sha256']:
                raise ChannelError('bild_pruefsumme_passt_nicht')
            url = meta.get('url')
            parsed = urlsplit(url) if isinstance(url, str) and len(url) <= 4096 else None
            if (not parsed or parsed.scheme != 'https' or parsed.hostname not in MEDIA_HOSTS or parsed.username or parsed.password or
                    parsed.port not in (None, 443) or parsed.fragment):
                raise ChannelError('medienadresse_nicht_freigegeben')
            response = self.transport.get(url, headers=headers, timeout=(5, 20), allow_redirects=False, stream=True)
            if response.status_code != 200:
                self._response_bytes(response, MAX_IMAGE_BYTES)
            content_type = (response.headers.get('Content-Type') or '').split(';', 1)[0].strip().lower()
            if content_type != row['mime']:
                response.close()
                raise ChannelError('bildformat_passt_nicht')
            raw = self._response_bytes(response, MAX_IMAGE_BYTES)
            if len(raw) != int(size) or hashlib.sha256(raw).hexdigest() != row['expected_sha256']:
                raise ChannelError('bild_pruefsumme_passt_nicht')
            actual_mime, _ = _image_or_pdf(raw)
            if actual_mime != row['mime']:
                raise ChannelError('bildformat_passt_nicht')
            return raw
        except ChannelError:
            raise
        except requests.RequestException as exc:
            raise ChannelError('meta_voruebergehend_nicht_erreichbar', retry=True) from exc
        except (ValueError, TypeError, KeyError) as exc:
            raise ChannelError('medienantwort_ungueltig') from exc

    def process_next(self):
        config = self._config()
        if not (config['enabled'] and config['ids'] and config['secret'] and config['token'] and config['version']):
            return None
        lease, now = secrets.token_hex(16), self.clock()
        with self.db() as db:
            db.execute("""UPDATE einkauf_material_nachrichten SET state='review',error_code='wiederholungen_erschoepft',
                lease_token='',lease_until=0,updated_at=? WHERE attempts>=? AND
                (state IN ('queued','retry') OR (state='processing' AND lease_until<?))""", (now, MAX_ATTEMPTS, now))
            row = db.execute('''SELECT * FROM einkauf_material_nachrichten WHERE
                (state IN ('queued','retry') OR (state='processing' AND lease_until<?))
                AND retry_at<=? AND attempts<? ORDER BY id LIMIT 1''', (now, now, MAX_ATTEMPTS)).fetchone()
            if not row:
                return None
            changed = db.execute('''UPDATE einkauf_material_nachrichten SET state='processing',lease_token=?,lease_until=?,attempts=attempts+1,updated_at=?
                WHERE id=? AND (state IN ('queued','retry') OR (state='processing' AND lease_until<?)) AND attempts<?''',
                (lease, now + 120, now, row['id'], now, MAX_ATTEMPTS)).rowcount
            if changed != 1:
                return None
            row = dict(db.execute('SELECT * FROM einkauf_material_nachrichten WHERE id=?', (row['id'],)).fetchone())
        try:
            if not getattr(self.p, 'assistant_material_photos', None):
                raise ChannelError('persoenlicher_fotodienst_fehlt')
            with self.db() as db:
                self._active(db, row)
            raw = self._download(row)
            with self.db() as db:
                # Serialization against revoke/inactive/rights updates and one
                # shared transaction prevent a partial or unauthorized intake.
                self._active(db, row, lock=True)
                owned = db.execute('''UPDATE einkauf_material_nachrichten SET updated_at=updated_at
                    WHERE id=? AND state='processing' AND lease_token=? AND lease_until>=?''',
                    (row['id'], lease, self.clock())).rowcount
                if owned != 1:
                    raise ChannelError('verarbeitung_erneut_pruefen')
                intake = copy.copy(self.p.workshop_intake)
                intake.p = SimpleNamespace(get_db=lambda: _BorrowedConnection(db))
                source_key = 'whatsapp:' + row['phone_number_id'] + ':' + hashlib.sha256(row['wamid'].encode()).hexdigest()
                caption = ' '.join(row['caption'].split())
                reference = ('WhatsApp-Direktfoto; Absender: ' + row['employee_name'] + '; '
                             + ('weitergeleitet, ursprünglicher Autor ungeklärt; ' if row['forwarded'] else '')
                             + 'Nachricht: ' + row['wamid'][:90] + ('; Bildunterschrift (ungeprüft): ' + caption[:180] if caption else ''))[:500]
                author = None if row['forwarded'] else row['employee_name']
                group = intake.create({'supplier': 'Lieferant ungeklärt', 'source_key': source_key, 'external_ref': reference,
                    'source_at': row['source_at'], 'already_ordered': False, 'original_author': author,
                    'lines': [{'product': 'Materialfoto – Artikelzuordnung prüfen', 'quantity': None, 'unit': '', 'sku': '',
                               'variant': '', 'pack': '', 'urgent': None, 'category': 'ungeklaert', 'original_author': author}]})
                file = intake.attach(group['id'], FileStorage(stream=io.BytesIO(raw), filename='whatsapp-' + str(row['id']) + IMAGE_TYPES[row['mime']]), 'materialfoto')
                photos = copy.copy(self.p.assistant_material_photos)
                photos.p = SimpleNamespace(get_db=lambda: _BorrowedConnection(db), app=self.p.app,
                    now_str=getattr(self.p, 'now_str', lambda: datetime.now(timezone.utc).isoformat()))
                photo = photos.stage({'actor': 'mitarbeiter:' + str(row['employee_id']), 'lesen': True, 'einkaufen': True},
                    FileStorage(stream=io.BytesIO(raw), filename='material' + IMAGE_TYPES[row['mime']]),
                    'wa-' + hashlib.sha256(source_key.encode()).hexdigest())
                saved = db.execute('''UPDATE einkauf_material_nachrichten SET state='ready',intake_id=?,file_id=?,assistant_photo_id=?,lease_token='',lease_until=0,
                    error_code='',updated_at=? WHERE id=? AND lease_token=?''', (group['id'], file['id'], photo['id'], self.clock(), row['id'], lease)).rowcount
                if saved != 1:
                    raise ChannelError('verarbeitung_erneut_pruefen')
            return {'id': row['id'], 'state': 'ready', 'intake_id': group['id'], 'file_id': file['id'], 'assistant_photo_id': photo['id'],
                    'next_step': 'Im persönlichen Assistenten das Foto auslesen lassen; noch keine Artikel- oder Bestellfreigabe.'}
        except (ChannelError, PermissionError, ValueError) as exc:
            code = exc.code if isinstance(exc, ChannelError) else 'berechtigung_entzogen' if isinstance(exc, PermissionError) else 'eingang_muss_geprueft_werden'
            retry = isinstance(exc, ChannelError) and exc.retry and row['attempts'] < MAX_ATTEMPTS
            state = 'retry' if retry else 'review'
            with self.db() as db:
                db.execute('''UPDATE einkauf_material_nachrichten SET state=?,error_code=?,retry_at=?,lease_token='',lease_until=0,updated_at=?
                    WHERE id=? AND lease_token=?''', (state, code, self.clock() + min(3600, 30 * 2 ** row['attempts']) if retry else 0,
                    self.clock(), row['id'], lease))
            return {'id': row['id'], 'state': state, 'error_code': code}

    def worker_tick(self):
        """One bounded attempt plus a durable signal; never reports credentials."""
        error = ''
        try:
            result = self.process_next()
            dialog = getattr(self.p,'material_dialog',None)
            if dialog:
                followup = dialog.process_next()
                return result or followup
            return result
        except Exception:
            error = 'worker_verarbeitung_fehlgeschlagen'
            return None
        finally:
            with self.db() as db:
                db.execute("""INSERT INTO einkauf_material_worker(worker_key,heartbeat_at,last_error) VALUES('material',?,?)
                    ON CONFLICT(worker_key) DO UPDATE SET heartbeat_at=excluded.heartbeat_at,last_error=excluded.last_error""",
                    (self.clock(), error))


def register_material_channel(p):
    if 'werkstatt_materialkanal' in p.app.extensions:
        return p.app.extensions['werkstatt_materialkanal']
    service = MaterialChannel(p)
    p.material_channel = service
    p.material_channel_init_schema = service.init_schema
    p.app.extensions['werkstatt_materialkanal'] = service

    @p.app.cli.command('werkstatt-materialeingang-worker')
    @click.option('--once', is_flag=True, help='Höchstens eine gespeicherte Bildnachricht verarbeiten.')
    @click.option('--interval', default=30, type=click.IntRange(10, 120))
    def worker(once, interval):
        while True:
            result = service.worker_tick()
            if result:
                click.echo('Materialeingang #' + str(result['id']) + ': ' + result['state'])
            if once:
                return
            time.sleep(interval)
    return service


def start_material_worker(p):
    """Explicit app-start hook, default off; database leases coordinate processes."""
    global _WORKER_THREAD
    service = getattr(p, 'material_channel', None)
    if not service or p.app.config.get('MATERIAL_WHATSAPP_WORKER_ENABLED') is not True:
        return False
    with _WORKER_LOCK:
        if _WORKER_THREAD and _WORKER_THREAD.is_alive():
            return False
        service.worker_stop.clear()
        def loop():
            while not service.worker_stop.is_set():
                try:
                    service.worker_tick()
                except Exception:
                    # Database failure must not leak data or silently kill the loop.
                    # A missing/stale durable signal remains visible in readiness.
                    pass
                service.worker_stop.wait(30)
        service.worker_thread = threading.Thread(target=loop, name='werkstatt-materialkanal', daemon=True)
        _WORKER_THREAD = service.worker_thread
        service.worker_thread.start()
        return True
