"""Private immutable shop receipts using the existing durable MailOutbox.

No SMTP implementation, invoice issuance, supplier operation or automatic start.
Queue records and legal packs live in the explicitly injected shop SQLite store.
Only deliver_once (explicit delivery_enabled) calls the injected MailOutbox.send;
its stable UUID and retry_not_sent=False preserve at-most-once SMTP submission.
Unknown sends are reconciled through status only, never silently resubmitted.
"""
import base64
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import format_datetime
from hashlib import sha256
import json
import re
import sqlite3
from uuid import UUID, uuid5
from uuid import uuid4

from shop_buyer import validate_context
from shop_order_store import _validate_snapshot, _delivery_context
from werkstatt_mail_identity import FOOTER_TEXT

NAMESPACE = UUID('9325b13f-f90f-45a1-a423-ddcf2086c4fc')
KINDS = {'order_received', 'order_confirmed', 'order_cancelled'}
LABELS = {'mounting': 'Montage', 'painting': 'Lackierung', 'installation': 'Einbau',
          'oil_service': 'Ölservice', 'care': 'Aufbereitung'}
WORKSHOP_DELIVERY_LABELS = {
    'collection': 'Abholung nach Werkstattleistung',
    'customer_reshipment': 'Weiterversand an die Lieferadresse nach Werkstattleistung (2 Versandwege)',
}


class ReceiptError(ValueError):
    pass


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _stamp(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise ReceiptError('receipt_unavailable') from None


def _email(value):
    return (isinstance(value, str) and len(value) <= 254
            and re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}", value)
            and '..' not in value and not any(c in value for c in '\r\n\x00'))


def _money(value):
    if type(value) is not int or not 0 <= value <= 100_000_000:
        raise ReceiptError('receipt_binding_mismatch')
    return f'{value // 100:,}'.replace(',', '.') + ',' + f'{value % 100:02d}' + ' EUR'


def _receipt_context(value):
    """Keep only validated buyer fields, including the explicit delivery choice."""
    try:
        selected = {key: value[key] for key in ('contact', 'billing', 'delivery')}
        if 'workshop_delivery' in value:
            selected['workshop_delivery'] = value['workshop_delivery']
        return validate_context(selected)
    except Exception:
        raise ReceiptError('receipt_binding_mismatch') from None


def init_order_receipt_schema(connection):
    connection.executescript('''
      CREATE TABLE IF NOT EXISTS shop_receipt_legal_v2 (
        hash TEXT PRIMARY KEY, snapshot_json TEXT NOT NULL);
      CREATE TRIGGER IF NOT EXISTS shop_receipt_legal_immutable_v2
        BEFORE UPDATE ON shop_receipt_legal_v2 BEGIN SELECT RAISE(ABORT,'immutable_receipt_legal'); END;
      CREATE TABLE IF NOT EXISTS shop_order_receipts_v2 (
        order_id TEXT NOT NULL, kind TEXT NOT NULL, customer_id TEXT NOT NULL, mode TEXT NOT NULL,
        token TEXT UNIQUE NOT NULL, binding_json TEXT NOT NULL, recipient TEXT NOT NULL,
        sender TEXT NOT NULL, subject TEXT NOT NULL, body TEXT NOT NULL, legal_hash TEXT NOT NULL,
        fingerprint TEXT NOT NULL, created_at TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
        lease_token TEXT, lease_until TEXT,
        PRIMARY KEY(order_id,kind));
      CREATE TRIGGER IF NOT EXISTS shop_order_receipt_immutable_v2
        BEFORE UPDATE OF order_id,kind,customer_id,mode,token,binding_json,recipient,sender,subject,body,legal_hash,fingerprint,created_at
        ON shop_order_receipts_v2 BEGIN SELECT RAISE(ABORT,'immutable_order_receipt'); END;
    ''')
    connection.commit()


class ShopOrderReceipts:
    def __init__(self, store, connection_factory, *, mode, sender_address, enabled=False,
                 delivery_enabled=False, outbox=None, smtp_config=None, legal_snapshot=None,
                 require_legal=True, clock=None):
        if (not isinstance(mode, str) or mode not in {'test', 'live'} or not _email(sender_address)
                or any(type(v) is not bool for v in (enabled, delivery_enabled, require_legal))
                or not callable(connection_factory) or legal_snapshot is not None and not callable(legal_snapshot)):
            raise ReceiptError('invalid_receipt_configuration')
        self.store, self.factory, self.mode, self.sender = store, connection_factory, mode, sender_address
        self.enabled, self.delivery_enabled, self.outbox, self.smtp_config = enabled, delivery_enabled, outbox, smtp_config
        self.legal_snapshot, self.require_legal = legal_snapshot, require_legal
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def readiness(self):
        queued = self.enabled and (not self.require_legal or callable(self.legal_snapshot))
        delivery = queued and self.delivery_enabled and callable(self.smtp_config) and all(
            callable(getattr(self.outbox, name, None)) for name in ('send', 'status'))
        return {'queue_ready': queued, 'delivery_ready': delivery, 'mode': self.mode}

    @contextmanager
    def _db(self, *, write=False):
        connection = None
        try:
            connection = self.factory()
            if not isinstance(connection, sqlite3.Connection):
                raise ValueError()
            connection.row_factory = sqlite3.Row
            if write:
                connection.execute('BEGIN IMMEDIATE')
            yield connection
            if write:
                connection.commit()
        except Exception:
            if isinstance(connection, sqlite3.Connection):
                connection.rollback()
            raise ReceiptError('receipt_unavailable') from None
        finally:
            if isinstance(connection, sqlite3.Connection):
                connection.close()

    def _load(self, order_id, customer_id):
        try:
            order = self.store.get_order(order_id, customer_id=customer_id, mode=self.mode)
            snapshot = self.store.snapshot_for_order(order_id, customer_id=customer_id, mode=self.mode)
            context = self.store.delivery_context_for_order(order_id, customer_id=customer_id, mode=self.mode)
            _validate_snapshot(snapshot, order['quote_hash'], customer_id, self.mode)
            _delivery_context(snapshot, context)
            return order, snapshot, context
        except Exception:
            raise ReceiptError('receipt_not_found') from None

    def _legal(self, order_id, customer_id):
        value = self.legal_snapshot(order_id, customer_id=customer_id, mode=self.mode) if self.legal_snapshot else None
        if value is None and not self.require_legal:
            return {'version': 'not-provided', 'texts': {}, 'attachments': []}
        if (not isinstance(value, Mapping) or set(value) != {'version', 'texts', 'attachments'}
                or not isinstance(value['version'], str) or not 1 <= len(value['version']) <= 160
                or not isinstance(value['texts'], dict)
                or not {'agb', 'widerruf', 'lieferung', 'datenschutz'} <= value['texts'].keys()
                or len(value['texts']) > 10 or not isinstance(value['attachments'], list) or len(value['attachments']) > 10):
            raise ReceiptError('receipt_legal_unavailable')
        texts = {}
        for key, text in value['texts'].items():
            if (not isinstance(key, str) or not re.fullmatch(r'[a-z_]{1,40}', key)
                    or not isinstance(text, str) or not 1 <= len(text) <= 50000 or '\x00' in text):
                raise ReceiptError('receipt_legal_unavailable')
            texts[key] = text
        attachments, size = [], 0
        for item in value['attachments']:
            if (not isinstance(item, dict) or set(item) != {'filename', 'mimetype', 'content'}
                    or not isinstance(item['filename'], str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,150}', item['filename'])
                    or item['mimetype'] not in {'application/pdf', 'image/jpeg', 'image/png', 'text/plain'}
                    or not isinstance(item['content'], bytes) or not 0 < len(item['content']) <= 5 * 1024 * 1024):
                raise ReceiptError('receipt_legal_unavailable')
            size += len(item['content'])
            if size > 12 * 1024 * 1024:
                raise ReceiptError('receipt_legal_unavailable')
            attachments.append({'filename': item['filename'], 'mimetype': item['mimetype'],
                                'sha256': sha256(item['content']).hexdigest(),
                                'content_base64': base64.b64encode(item['content']).decode('ascii')})
        return {'version': value['version'], 'texts': texts, 'attachments': attachments}

    def prepare(self, order_id, *, customer_id, kind):
        if not self.readiness()['queue_ready'] or kind not in KINDS:
            raise ReceiptError('receipt_unavailable')
        order, snapshot, context = self._load(order_id, customer_id)
        context = _receipt_context(context)
        if (kind == 'order_received' and order['status'] != 'authorized'
                or kind == 'order_confirmed' and order['status'] != 'paid'
                or kind == 'order_cancelled' and order['status'] != 'cancelled'):
            raise ReceiptError('receipt_state_unconfirmed')
        legal = self._legal(order_id, customer_id)
        legal_json = _json(legal)
        binding = {key: order[key] for key in ('id', 'customer_id', 'mode', 'quote_hash', 'amount_gross_cent', 'currency')}
        labels = {'order_received': ('Eingang Ihrer Bestellung', 'Ihre Bestellung ist eingegangen. Bei Eingang wurde der Kartenbetrag reserviert. Diese Eingangsbestätigung bestätigt noch keinen Einzug und keine Lieferantenannahme.'),
                  'order_confirmed': ('Bestätigung Ihrer Bestellung', 'Alle Bestellpositionen wurden vom Lieferanten angenommen. Die Zahlung wurde eingezogen. Dies ist noch keine Versand- oder Zustellbestätigung.'),
                  'order_cancelled': ('Status Ihrer Bestellung', 'Die Kartenreservierung wurde beendet. Es wurde kein Kartenbetrag eingezogen.')}
        subject, intro = labels[kind]
        lines = [('TESTVORGANG · keine echte Bestellung oder Zahlung\n' if self.mode == 'test' else '') + intro,
                 '', 'Bestellung: ' + order_id, 'Bestelleingang: ' + order['created_at'], '']
        for line in snapshot['lines']:
            lines.append(f"{line['quantity']} × {line['name']}: {_money(line['items_gross_cent'])}")
            for service in line['services']:
                lines.append('  ' + LABELS.get(service['code'], 'Erforderliches Zubehör') + ': ' + _money(service['gross_cent']))
        for shipment in snapshot['shipments']:
            label = 'Versand zur Werkstatt' if shipment['destination'] == 'workshop' else 'Direktversand zum Kunden'
            if shipment['destination'] == 'workshop' and 'workshop_delivery' in context:
                label += '; ' + WORKSHOP_DELIVERY_LABELS[context['workshop_delivery']]
            lines.append(label + ': ' + _money(shipment['gross_cent']))
        lines += ['', 'Gesamt inkl. 19 % MwSt.: ' + _money(order['amount_gross_cent']),
                  '', 'Montage-/Servicetermin vereinbaren wir nach Wareneingang; hier ist kein Termin reserviert.',
                  '', 'RECHNUNGSANSCHRIFT']
        for key in ('billing', 'delivery'):
            if key == 'delivery':
                title = ('LIEFERANSCHRIFT FÜR DIREKTVERSAND UND WEITERVERSAND NACH WERKSTATTLEISTUNG'
                         if context.get('workshop_delivery') == 'customer_reshipment'
                         and any(s['destination'] == 'workshop' for s in snapshot['shipments'])
                         else 'LIEFERANSCHRIFT FÜR DIREKTVERSAND')
                lines += ['', title]
            address = context[key]
            lines += [address['recipient'], address['street'], address['postcode'] + ' ' + address['city'], address['country']]
        lines += ['', 'Dies ist eine Bestell-/Statusbestätigung, keine Rechnung.', '', FOOTER_TEXT]
        record = {'order_id': order_id, 'kind': kind, 'customer_id': customer_id, 'mode': self.mode,
            'binding': binding, 'recipient': context['contact']['email'], 'sender': self.sender,
            'subject': ('TEST · ' if self.mode == 'test' else '') + subject + ' ' + order_id,
            'body': '\n'.join(lines), 'legal_hash': sha256(legal_json.encode()).hexdigest(), 'legal_json': legal_json}
        record['token'] = str(uuid5(NAMESPACE, self.mode + ':' + order_id + ':' + kind))
        record['fingerprint'] = sha256(_json(record).encode()).hexdigest()
        return record

    def enqueue_in_transaction(self, connection, prepared):
        """Caller owns a write transaction; prepared comes only from prepare()."""
        if not self.readiness()['queue_ready'] or not connection.in_transaction:
            raise ReceiptError('receipt_unavailable')
        prior = connection.execute('SELECT fingerprint FROM shop_order_receipts_v2 WHERE order_id=? AND kind=?',
                                   (prepared['order_id'], prepared['kind'])).fetchone()
        if prior:
            if prior[0] != prepared['fingerprint']:
                raise ReceiptError('receipt_conflict')
            return
        connection.execute('INSERT OR IGNORE INTO shop_receipt_legal_v2 VALUES (?,?)', (prepared['legal_hash'], prepared['legal_json']))
        connection.execute('''INSERT INTO shop_order_receipts_v2
          (order_id,kind,customer_id,mode,token,binding_json,recipient,sender,subject,body,legal_hash,fingerprint,created_at)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', (prepared['order_id'], prepared['kind'], prepared['customer_id'], self.mode,
          prepared['token'], _json(prepared['binding']), prepared['recipient'], prepared['sender'], prepared['subject'], prepared['body'],
          prepared['legal_hash'], prepared['fingerprint'], _stamp(self.clock()).isoformat()))

    def enqueue(self, order_id, *, customer_id, kind):
        prepared = self.prepare(order_id, customer_id=customer_id, kind=kind)
        with self._db(write=True) as connection:
            self.enqueue_in_transaction(connection, prepared)

    def _mail_state(self, result, row):
        if not isinstance(result, Mapping) or result.get('token') != row['token']:
            return 'unknown'
        accepted = result.get('accepted')
        if (result.get('state') in {'sent', 'copy_pending', 'partial'} and isinstance(accepted, list)
                and row['recipient'].casefold() in [v.casefold() for v in accepted if isinstance(v, str)]):
            return 'submitted'
        return 'not_sent' if result.get('state') == 'not_sent' else 'unknown'

    def deliver_once(self):
        if not self.readiness()['delivery_ready']:
            return {'processed': False, 'state': 'disabled'}
        with self._db(write=True) as connection:
            now = _stamp(self.clock())
            raw = connection.execute("SELECT * FROM shop_order_receipts_v2 WHERE mode=? AND (state='queued' OR (state='sending' AND lease_until<=?)) ORDER BY created_at,kind LIMIT 1", (self.mode, now.isoformat())).fetchone()
            if raw is None:
                return {'processed': False, 'state': 'idle'}
            row = dict(raw)
            first = row['state'] == 'queued'
            owner = uuid4().hex
            connection.execute("UPDATE shop_order_receipts_v2 SET state='sending',lease_token=?,lease_until=? WHERE token=?", (owner, (now + timedelta(seconds=120)).isoformat(), row['token']))
        try:
            if not first:
                state = self._mail_state(self.outbox.status(row['token']), row)
            else:
                with self._db() as connection:
                    packed = connection.execute('SELECT snapshot_json FROM shop_receipt_legal_v2 WHERE hash=?', (row['legal_hash'],)).fetchone()
                legal = json.loads(packed[0])
                if sha256(_json(legal).encode()).hexdigest() != row['legal_hash']:
                    raise ValueError()
                material = {key: row[key] for key in ('order_id', 'kind', 'customer_id', 'mode', 'recipient', 'sender', 'subject', 'body', 'legal_hash', 'token')}
                material.update(binding=json.loads(row['binding_json']), legal_json=packed[0])
                if sha256(_json(material).encode()).hexdigest() != row['fingerprint']:
                    raise ValueError()
                message = EmailMessage()
                message['From'], message['To'], message['Subject'] = row['sender'], row['recipient'], row['subject']
                message['Message-ID'] = '<shop-' + sha256(row['token'].encode()).hexdigest() + '@' + row['sender'].rsplit('@', 1)[1] + '>'
                message['Date'] = format_datetime(_stamp(row['created_at']))
                body = row['body']
                for key, text in legal['texts'].items():
                    body += '\n\n' + key.upper() + '\n' + text
                message.set_content(body)
                for item in legal['attachments']:
                    data = base64.b64decode(item['content_base64'], validate=True)
                    if sha256(data).hexdigest() != item['sha256']:
                        raise ValueError()
                    main, sub = item['mimetype'].split('/', 1)
                    message.add_attachment(data, maintype=main, subtype=sub, filename=item['filename'])
                config = self.smtp_config()
                if not isinstance(config, Mapping) or config.get('from_address') != row['sender']:
                    raise ValueError()
                state = self._mail_state(self.outbox.send(row['token'], message, dict(config), retry_not_sent=False), row)
        except Exception:
            state = 'unknown'
        with self._db(write=True) as connection:
            connection.execute("UPDATE shop_order_receipts_v2 SET state=? WHERE token=? AND state='sending' AND lease_token=?", (state, row['token'], owner))
        return {'processed': True, 'state': state}

    def customer_receipt(self, order_id, *, customer_id, include_addresses=False):
        order, snapshot, context = self._load(order_id, customer_id)
        parsed_context = (_receipt_context(context) if include_addresses is True
                          or isinstance(context, Mapping) and 'workshop_delivery' in context else None)
        with self._db() as connection:
            rows = connection.execute('SELECT kind,state FROM shop_order_receipts_v2 WHERE order_id=? AND customer_id=? AND mode=?',
                                      (order_id, customer_id, self.mode)).fetchall()
        result = {'order_id': order_id, 'status': order['status'], 'mode': self.mode, 'currency': 'eur',
            'amount_gross_cent': order['amount_gross_cent'],
            'lines': [{key: line[key] for key in ('id', 'name', 'quantity', 'unit_gross_cent', 'items_gross_cent', 'services', 'destination')} for line in snapshot['lines']],
            'shipments': [{key: group[key] for key in ('destination', 'gross_cent')} for group in snapshot['shipments']],
            'email_state': {row['kind']: row['state'] for row in rows}, 'invoice_status': 'not_issued',
            'invoice': None, 'returns_enabled': False}
        if (parsed_context is not None and 'workshop_delivery' in parsed_context
                and any(group['destination'] == 'workshop' for group in snapshot['shipments'])):
            choice = parsed_context['workshop_delivery']
            result.update(workshop_delivery=choice, workshop_delivery_label=WORKSHOP_DELIVERY_LABELS[choice])
        if include_addresses is True:
            result['billing'], result['delivery'] = parsed_context['billing'], parsed_context['delivery']
        return result

    def reconcile(self, order_id, *, customer_id, kind):
        """Read existing outbox outcome only. Never retry SMTP or alter content."""
        self._load(order_id, customer_id)
        if kind not in KINDS or not self.readiness()['delivery_ready']:
            raise ReceiptError('receipt_unavailable')
        with self._db() as connection:
            raw = connection.execute('SELECT * FROM shop_order_receipts_v2 WHERE order_id=? AND customer_id=? AND mode=? AND kind=?',
                                     (order_id, customer_id, self.mode, kind)).fetchone()
        if raw is None:
            raise ReceiptError('receipt_not_found')
        row = dict(raw)
        state = self._mail_state(self.outbox.status(row['token']), row)
        if state == 'submitted':
            with self._db(write=True) as connection:
                connection.execute("UPDATE shop_order_receipts_v2 SET state='submitted' WHERE token=? AND state IN ('sending','unknown','not_sent')", (row['token'],))
        return {'kind': kind, 'state': state}
