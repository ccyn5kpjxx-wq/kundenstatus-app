"""Durable dispatch for deliberately requested employee supplier orders.

Integration constraints:
* No routes, scheduler, background threads or network work run on import/enqueue.
  After accepting an urgent order the trusted caller must invoke dispatch_due;
  a persistent scheduler must invoke it for weekly and overdue orders.
* authorize_order must verify the authenticated actor's current purchasing right,
  a deliberate employee order action, the exact approved product/variant/price
  and spending cap. Model output or a client-side checkbox is not authorization.
* supplier_resolver must return a server-verified supplier recipient. Recipient
  ownership and order-taking permission cannot be inferred from invoice text.
* The DB and MailOutbox private directory must survive restarts and be shared by
  every worker. Use the existing MailOutbox/Mailbox boundary; never raw SMTP.
* Route wrappers must authenticate/authorize status reads and require CSRF for
  acceptance. Do not expose enqueue directly as an autonomous model tool.
* Accepted orders and batch contents are immutable. Corrections require a new
  explicit workflow; uncertain delivery is NEVER retried as a new message.

Payload keys follow werkstatt_bestellplan, plus product_name, verified gross
unit_price_cents, explicit shipping_cents, price_verified=True, price_basis='gross'
and currency='EUR'. Historical invoice prices alone are not verified prices.
Each request's cap includes its shipping allowance. No unknown charge is added.
New avatar requests also require explicit extra_costs_cents and price_source.
The management layer supplies transactional reservation and pre-send batch guards
to enforce the shared supplier budget and current verified contact.
"""
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from email.message import EmailMessage
from email.utils import format_datetime, getaddresses
import hashlib
import json
import re
import uuid

from mailbox_client import Mailbox
from mailbox_outbox import MailOutbox
from werkstatt_bestellplan import group_orders, next_dispatch_at, migrated_weekly_dispatch_at, valid_saved_dispatch_at


_STATES = {'queued', 'ready', 'sending', 'sent', 'copy_pending', 'partial',
           'uncertain', 'not_sent', 'blocked'}
_MESSAGES = {
    'queued': 'Bestellung gespeichert; Versand zum festgelegten Termin ausstehend.',
    'ready': 'Bestellmail unveränderlich vorbereitet; Versand ausstehend.',
    'sending': 'Versand läuft oder wird abgeglichen. Nicht erneut bestellen.',
    'sent': 'Vom Mailserver angenommen und in Gesendet abgelegt.',
    'copy_pending': 'Vom Mailserver angenommen. Nur die Gesendet-Kopie fehlt noch.',
    'partial': 'Empfängerannahme unvollständig. Nicht erneut senden; manuell prüfen.',
    'uncertain': 'Versandstatus unklar. Nicht erneut senden; im Postfach prüfen.',
    'not_sent': 'Der Mailserver hat die Bestellmail nicht angenommen.',
    'blocked': 'Versand gesperrt; Postfach oder unveränderten Absender prüfen.',
}


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _digest(value):
    return hashlib.sha256(_canonical(value).encode('utf-8')).hexdigest()


def _text(value, field, limit=300):
    if type(value) is int:
        value = str(value)
    if (not isinstance(value, str) or not value.strip() or len(value) > limit
            or any(ord(char) < 32 for char in value)):
        raise ValueError(f'{field} fehlt oder ist ungültig.')
    return value.strip()


def _aware(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('Versandzeit braucht eine Zeitzone.')
    return value.astimezone(timezone.utc)


def _sender(config):
    if not config.get('smtp_configured') or not (config.get('smtp_ssl') or config.get('smtp_tls')):
        raise ValueError('Verschlüsselter Bestellversand ist noch nicht eingerichtet.')
    header = _text(config.get('from_address'), 'Absender', 320)
    addresses = getaddresses([header])
    if len(addresses) != 1 or not re.fullmatch(r'[^\s<>@]+@[^\s<>@]+\.[A-Za-z]{2,63}', addresses[0][1]):
        raise ValueError('Ein eindeutiger Absender ist erforderlich.')
    account = _text(config.get('smtp_user'), 'Postfach', 254).casefold()
    return header, account


class OrderDispatch:
    """Application service; trusted callbacks are mandatory for accepting orders."""
    LEASE_SECONDS = 300
    RETRY_SECONDS = 300
    MAX_SEND_ATTEMPTS = 3

    def __init__(self, get_db, outbox, smtp_config, authorize_order=None,
                 supplier_resolver=None, clock=None, reservation_guard=None, batch_guard=None, schedule_guard=None):
        self.get_db = get_db
        self.outbox = outbox
        self.smtp_config = smtp_config
        self.authorize_order = authorize_order
        self.supplier_resolver = supplier_resolver
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.reservation_guard = reservation_guard
        self.batch_guard = batch_guard
        self.schedule_guard = schedule_guard
        self.init_schema()

    def init_schema(self):
        """Restore-safe schema setup, without routes, threads or transport."""
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS assistent_bestellanforderungen (
                id TEXT PRIMARY KEY, actor_id TEXT NOT NULL, request_id TEXT NOT NULL,
                request_fingerprint TEXT NOT NULL, snapshot_json TEXT NOT NULL,
                due_at REAL NOT NULL, created_at REAL NOT NULL,
                batch_id TEXT NOT NULL DEFAULT '', UNIQUE(actor_id,request_id))''')
            db.execute('''CREATE TABLE IF NOT EXISTS assistent_bestellpakete (
                id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, fingerprint TEXT NOT NULL,
                due_at REAL NOT NULL, created_at REAL NOT NULL, state TEXT NOT NULL,
                lease TEXT NOT NULL DEFAULT '', lease_until REAL NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                result_json TEXT NOT NULL DEFAULT '{}')''')
            db.commit()
        self.init_schedule_schema()

    def init_schedule_schema(self):
        """Add versioned schedule metadata to old DBs, also after a restore."""
        for table, column, definition in (
            ('assistent_bestellanforderungen','schedule_version','INTEGER NOT NULL DEFAULT 1'),
            ('assistent_bestellanforderungen','legacy_due_at','DOUBLE PRECISION'),
            ('assistent_bestellpakete','not_before_at','DOUBLE PRECISION NOT NULL DEFAULT 0'),
        ):
            with self._db() as db:
                try:
                    db.execute(f'SELECT {column} FROM {table} WHERE 1=0')
                except Exception:
                    db.rollback()
                    try:
                        db.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')
                        db.commit()
                    except Exception:
                        # Two starting processes can race the same migration.
                        # Clear PostgreSQL's aborted transaction, then prove
                        # that the exact column is available instead of hiding
                        # any unrelated DDL error.
                        db.rollback()
                        db.execute(f'SELECT {column} FROM {table} WHERE 1=0')

    def migrate_weekly_schedule(self, db):
        """Keep the original Monday, and never rewrite frozen mail payloads."""
        if self.schedule_guard:
            self.schedule_guard(db)
        db.execute("UPDATE assistent_bestellanforderungen SET due_at=due_at WHERE schedule_version=1 AND batch_id=''")
        rows = db.execute("SELECT * FROM assistent_bestellanforderungen WHERE schedule_version=1 AND batch_id='' ORDER BY id").fetchall()
        for row in rows:
            snapshot = json.loads(row['snapshot_json'])
            order = snapshot['order']
            created = datetime.fromtimestamp(row['created_at'], timezone.utc)
            due = datetime.fromtimestamp(row['due_at'], timezone.utc)
            if (_digest(order) != row['request_fingerprint'] or snapshot['actor_id'] != row['actor_id']
                    or not valid_saved_dispatch_at(created,order['urgent'],due,1)):
                raise ValueError('Alte Bestellfrist oder Freigabe muss geprüft werden; keine automatische Verschiebung.')
            if order['urgent']:
                db.execute("UPDATE assistent_bestellanforderungen SET schedule_version=2 WHERE id=? AND schedule_version=1 AND batch_id=''", (row['id'],))
            else:
                shifted = migrated_weekly_dispatch_at(created,due).timestamp()
                db.execute("UPDATE assistent_bestellanforderungen SET legacy_due_at=due_at,due_at=?,schedule_version=2 WHERE id=? AND schedule_version=1 AND batch_id='' AND due_at=?",
                           (shifted,row['id'],row['due_at']))
        # Already frozen but provably unsent batches retain their payload/hash
        # and original slot. A separate not-before time postpones transport.
        batches = db.execute("SELECT * FROM assistent_bestellpakete WHERE state IN ('ready','blocked','not_sent') AND not_before_at=0 ORDER BY id").fetchall()
        for batch in batches:
            payload = json.loads(batch['payload_json'])
            if payload.get('urgent') is not False:
                continue
            originals = db.execute('SELECT * FROM assistent_bestellanforderungen WHERE batch_id=?', (batch['id'],)).fetchall()
            if not originals or not any(row['schedule_version']==1 for row in originals):
                continue
            entries = {entry['id']:entry for entry in payload.get('orders',[])}
            if (_digest(payload) != batch['fingerprint'] or len(entries) != len(originals)
                    or set(entries) != {row['id'] for row in originals}
                    or not all(row['schedule_version']==1 for row in originals)):
                raise ValueError('Eingefrorene Altbestellung stimmt nicht mit ihren Bestellbelegen überein.')
            gates = []
            for row in originals:
                snapshot = json.loads(row['snapshot_json'])
                if (entries[row['id']] != dict(snapshot,id=row['id'])
                        or _digest(snapshot['order']) != row['request_fingerprint']
                        or snapshot['actor_id'] != row['actor_id'] or row['due_at'] != batch['due_at']):
                    raise ValueError('Eingefrorene Altbestellung wurde verändert; Versand gesperrt.')
                created = datetime.fromtimestamp(row['created_at'],timezone.utc)
                due = datetime.fromtimestamp(row['due_at'],timezone.utc)
                gates.append(migrated_weekly_dispatch_at(created,due).timestamp())
            db.execute("UPDATE assistent_bestellpakete SET not_before_at=? WHERE id=? AND not_before_at=0 AND state IN ('ready','blocked','not_sent')",
                       (max(gates),batch['id']))

    @contextmanager
    def _db(self):
        db = self.get_db()
        try:
            yield db
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _intent(payload, request_id):
        if not isinstance(payload, dict):
            raise ValueError('Vollständige ausdrückliche Bestellung erforderlich.')
        quantity = payload.get('quantity')
        if isinstance(quantity, Decimal):
            if not quantity.is_finite() or quantity.adjusted() > 8 or quantity.as_tuple().exponent < -6:
                raise ValueError('Bestellmenge ist ungültig.')
        elif len(str(quantity)) > 60:
            raise ValueError('Bestellmenge ist ungültig.')
        validation = group_orders([dict(payload, id=request_id, recipient_verified=True)])
        if validation['invalid']:
            raise ValueError(' '.join(validation['invalid'][0]['errors'].values()))
        group = validation['groups'][0]
        item = group['orders'][0]
        if Decimal(item['quantity']) > 100000000 or Decimal(item['quantity']).as_tuple().exponent < -6:
            raise ValueError('Bestellmenge ist ungültig.')
        for key, limit in (('supplier_id', 128), ('recipient', 254)):
            _text(group[key], key, limit)
        for key, limit in (('variant', 500), ('unit', 60)):
            _text(item[key], key, limit)
        for key in ('product_id', 'article_number'):
            if item[key] is not None:
                _text(item[key], key, 128)
        if payload.get('price_verified') is not True or payload.get('price_basis') != 'gross' or payload.get('currency') != 'EUR':
            raise ValueError('Bestätigter Brutto-Stückpreis in EUR erforderlich; Rechnungsfunde allein reichen nicht.')
        price, shipping, cap = payload.get('unit_price_cents'), payload.get('shipping_cents'), item['max_total_cents']
        extra = payload.get('extra_costs_cents', 0)
        if any(type(value) is not int or value < 0 or value > 10**12 for value in (price, shipping, extra, cap)):
            raise ValueError('Bestätigter Preis, Versandkosten und Kostenrahmen müssen in Cent vorliegen.')
        expected = int((Decimal(item['quantity']) * price).to_integral_value(rounding=ROUND_CEILING)) + shipping + extra
        if expected > cap:
            raise ValueError('Bestellung einschließlich Versand überschreitet den erlaubten Kostenrahmen.')
        return dict(item, supplier_id=group['supplier_id'], recipient=group['recipient'],
                    urgent=group['urgent'], order_requested=True,
                    product_name=_text(payload.get('product_name'), 'Produktname'),
                    unit_price_cents=price, shipping_cents=shipping,
                    extra_costs_cents=extra,
                    price_source=_text(payload.get('price_source', 'Manuell bestätigter Brutto-Stückpreis'), 'Preisquelle', 500),
                    expected_total_cents=expected, price_verified=True, price_basis='gross', currency='EUR')

    def enqueue(self, payload, actor_id, request_id):
        actor = _text(actor_id, 'Mitarbeiter', 128)
        request_key = _text(request_id, 'Anforderungs-ID', 128)
        intent = self._intent(payload, request_key)
        if not callable(self.authorize_order) or self.authorize_order(actor, dict(intent)) is not True:
            raise PermissionError('Eine bewusst ausgelöste Bestellung eines berechtigten Mitarbeiters ist erforderlich.')
        fingerprint = _digest(intent)
        with self._db() as db:
            existing = db.execute('SELECT * FROM assistent_bestellanforderungen WHERE actor_id=? AND request_id=?',
                                  (actor, request_key)).fetchone()
        if existing:
            if existing['request_fingerprint'] != fingerprint:
                raise ValueError('Diese Anforderungs-ID gehört bereits zu einer anderen Bestellung.')
            return self.status(existing['id'])
        if not callable(self.supplier_resolver):
            raise ValueError('Geprüfte Lieferantenzuordnung fehlt.')
        supplier = self.supplier_resolver(intent['supplier_id'])
        if not isinstance(supplier, dict) or supplier.get('verified') is not True:
            raise ValueError('Bestelladresse des Lieferanten ist nicht bestätigt.')
        recipient = supplier.get('recipient') or supplier.get('email')
        checked = group_orders([dict(intent, recipient=recipient, recipient_verified=True)])
        if checked['invalid'] or checked['groups'][0]['recipient'] != intent['recipient']:
            raise ValueError('Bestelladresse stimmt nicht mit dem geprüften Lieferanten überein.')
        if supplier.get('id') is not None and str(supplier['id']) != intent['supplier_id']:
            raise ValueError('Lieferantenzuordnung ist nicht eindeutig.')
        sender, account = _sender(self.smtp_config())
        now = _aware(self.clock())
        due_at = next_dispatch_at(now, intent['urgent']).timestamp()
        order_id = str(uuid.uuid4())
        snapshot = {'order': intent, 'actor_id': actor,
                    'supplier_name': _text(supplier.get('name') or intent['supplier_id'], 'Lieferant'),
                    'recipient_verified': True, 'from_address': sender, 'sender_account': account}
        with self._db() as db:
            if self.reservation_guard:
                self.reservation_guard(db, actor, request_key, intent, due_at)
            db.execute('''INSERT INTO assistent_bestellanforderungen
                (id,actor_id,request_id,request_fingerprint,snapshot_json,due_at,created_at,schedule_version)
                VALUES(?,?,?,?,?,?,?,2) ON CONFLICT(actor_id,request_id) DO NOTHING''',
                (order_id, actor, request_key, fingerprint, _canonical(snapshot),
                 due_at, now.timestamp()))
            stored = db.execute('SELECT id,request_fingerprint FROM assistent_bestellanforderungen WHERE actor_id=? AND request_id=?',
                                (actor, request_key)).fetchone()
            if stored['request_fingerprint'] != fingerprint:
                raise ValueError('Diese Anforderungs-ID gehört bereits zu einer anderen Bestellung.')
            db.commit()
        return self.status(stored['id'])

    def _freeze_due_batches(self, now):
        with self._db() as db:
            self.migrate_weekly_schedule(db)
            rows = db.execute("SELECT * FROM assistent_bestellanforderungen WHERE batch_id='' AND due_at<=? ORDER BY due_at,id",
                              (now.timestamp(),)).fetchall()
            groups = defaultdict(list)
            for row in rows:
                snapshot = json.loads(row['snapshot_json'])
                order = snapshot['order']
                if (_digest(order) != row['request_fingerprint'] or snapshot['actor_id'] != row['actor_id']
                        or not valid_saved_dispatch_at(datetime.fromtimestamp(row['created_at'], timezone.utc),
                            order['urgent'],datetime.fromtimestamp(row['due_at'], timezone.utc),row['schedule_version'],
                            datetime.fromtimestamp(row['legacy_due_at'], timezone.utc) if row['legacy_due_at'] is not None else None)):
                    raise ValueError('Gespeicherte Bestellfreigabe wurde verändert; Versand gesperrt.')
                key = (order['supplier_id'], order['recipient'], order['urgent'], row['due_at'],
                       snapshot['from_address'], snapshot['sender_account'], row['id'] if order['urgent'] else '')
                groups[key].append((dict(row), snapshot))
            for group in groups.values():
                batch_id = str(uuid.uuid4())
                claimed = []
                for row, snapshot in group:
                    changed = db.execute("UPDATE assistent_bestellanforderungen SET batch_id=? WHERE id=? AND batch_id=''",
                                         (batch_id, row['id']))
                    if changed.rowcount == 1:
                        claimed.append({'id': row['id'], **snapshot})
                if not claimed:
                    continue
                payload = {'orders': claimed, 'created_at': now.isoformat(),
                           'from_address': claimed[0]['from_address'], 'sender_account': claimed[0]['sender_account'],
                           'recipient': claimed[0]['order']['recipient'], 'urgent': claimed[0]['order']['urgent'],
                           'max_total_cents': sum(entry['order']['max_total_cents'] for entry in claimed)}
                db.execute('''INSERT INTO assistent_bestellpakete
                    (id,payload_json,fingerprint,due_at,created_at,state) VALUES(?,?,?,?,?,'ready')''',
                    (batch_id, _canonical(payload), _digest(payload), group[0][0]['due_at'], now.timestamp()))
            db.commit()

    @staticmethod
    def _message(batch):
        payload = json.loads(batch['payload_json'])
        if _digest(payload) != batch['fingerprint']:
            raise ValueError('Gespeicherter Bestellinhalt wurde verändert.')
        message = EmailMessage()
        message['From'] = payload['from_address']
        message['To'] = payload['recipient']
        message['Subject'] = ('Dringende Bestellung' if payload['urgent'] else 'Sammelbestellung') + ' – Gärtner Werkstatt'
        message['Date'] = format_datetime(datetime.fromisoformat(payload['created_at']))
        message['Message-ID'] = f'<werkstatt-order-{batch["id"]}@{getaddresses([payload["from_address"]])[0][1].rsplit("@",1)[1]}>'
        money = lambda cents: f'{Decimal(cents) / 100:.2f}'.replace('.', ',') + ' EUR'
        lines = ['Guten Tag,', '', 'hiermit bestellen wir die folgenden ausdrücklich freigegebenen Positionen.',
                 'Bitte keine abweichenden Artikel oder Varianten liefern. Die genannten Brutto-Höchstbeträge',
                 'einschließlich Versand dürfen nicht überschritten werden; andernfalls bitte vor Lieferung rückfragen.', '']
        lines += ['Verbindlicher Brutto-Höchstbetrag der gesamten Bestellmail einschließlich aller Versand- und Nebenkosten: '
                  + money(payload.get('max_total_cents', sum(entry['order']['max_total_cents'] for entry in payload['orders']))), '']
        for number, entry in enumerate(payload['orders'], 1):
            order = entry['order']
            lines += [f'{number}. {order["product_name"]}',
                      f'Artikelnummer: {order["article_number"] or "Katalogartikel " + order["product_id"]}',
                      f'Variante: {order["variant"]}', f'Menge: {order["quantity"]} {order["unit"]}',
                      f'Bestätigter Brutto-Stückpreis: {money(order["unit_price_cents"])}',
                      f'Freigegebener Versandanteil: {money(order["shipping_cents"])}',
                      f'Freigegebene Nebenkosten: {money(order.get("extra_costs_cents", 0))}',
                      f'Preisquelle: {order.get("price_source", "Manuell bestätigter Brutto-Stückpreis")}',
                      f'Höchstbetrag dieser Anforderung inklusive Versand: {money(order["max_total_cents"])}',
                      f'Bestellreferenz: {entry["id"]}', '']
        lines += ['Bitte bestätigen Sie die Bestellung und den Liefertermin.', '', 'Gärtner GmbH Karosserie + Lack']
        message.set_content('\n'.join(lines))
        return message

    def _record_result(self, batch_id, owner, result, now, blocked_message=None):
        state = result.get('state') if isinstance(result, dict) else None
        if state not in _STATES or state in {'queued', 'ready'}:
            state = 'uncertain'
        public = {'state': state, 'message': _MESSAGES[state]}
        if state == 'blocked' and blocked_message:
            public['message'] = str(blocked_message)[:500]
        with self._db() as db:
            # Preserve the first evidenced acceptance time across later IMAP
            # copy retries. Never infer it from a legacy outbox update time.
            previous = db.execute('SELECT result_json FROM assistent_bestellpakete WHERE id=? AND lease=?',(batch_id,owner)).fetchone()
            saved = json.loads(previous['result_json']) if previous else {}
            if saved.get('sent_at'):
                public['sent_at'] = saved['sent_at']
            elif isinstance(result,dict) and result.get('sent_at') and state in {'sent','copy_pending','partial'}:
                public['sent_at'] = result['sent_at']
            db.execute('''UPDATE assistent_bestellpakete SET state=?,result_json=?,lease='',lease_until=0,next_attempt_at=?
                WHERE id=? AND lease=?''', (state, _canonical(public), now.timestamp()+self.RETRY_SECONDS, batch_id, owner))
            db.commit()
        return public

    def _deliver(self, batch, owner, now):
        try:
            with self._db() as db:
                gate = db.execute('SELECT due_at,not_before_at FROM assistent_bestellpakete WHERE id=?', (batch['id'],)).fetchone()
                effective = max(gate['due_at'],gate['not_before_at'])
                if effective > now.timestamp():
                    db.execute("UPDATE assistent_bestellpakete SET lease='',lease_until=0,next_attempt_at=? WHERE id=? AND lease=?", (effective,batch['id'],owner))
                    db.commit()
                    return {'state':batch['state'],'message':'Sammelversand frühestens Montag um 14 Uhr (Europe/Berlin).'}
            existing = self.outbox.status(batch['id'])
            if existing and existing.get('state') in {'sent', 'uncertain', 'sending'}:
                return self._record_result(batch['id'], owner, existing, now)
            if existing and existing.get('state') in {'copy_pending', 'partial'}:
                result = self.outbox.retry_copy(batch['id']) if existing.get('can_retry_copy') else existing
                return self._record_result(batch['id'], owner, result, now)
            # Only absence or a known SMTP rejection permits submission.
            if existing and existing.get('state') != 'not_sent':
                return self._record_result(batch['id'], owner, {'state': 'uncertain'}, now)
            payload = json.loads(batch['payload_json'])
            try:
                config = self.smtp_config()
                sender, account = _sender(config)
                if sender != payload['from_address'] or account != payload['sender_account']:
                    raise ValueError('Sender changed')
                message = self._message(batch)
            except Exception:
                return self._record_result(batch['id'], owner, {'state': 'blocked'}, now)
            if self.batch_guard:
                try:
                    self.batch_guard(payload)
                except ValueError as exc:
                    return self._record_result(batch['id'], owner, {'state': 'blocked'}, now, str(exc))
            with self._db() as db:
                changed = db.execute("UPDATE assistent_bestellpakete SET state='sending',attempts=attempts+1 WHERE id=? AND lease=? AND attempts<?",
                                     (batch['id'], owner, self.MAX_SEND_ATTEMPTS))
                db.commit()
            if changed.rowcount != 1:
                return self._record_result(batch['id'], owner, {'state': 'not_sent'}, now)
            result = self.outbox.send(batch['id'], message, config)
            if isinstance(result,dict) and result.get('state') in {'sent','copy_pending','partial'}:
                result = dict(result,sent_at=_aware(self.clock()).isoformat())
            return self._record_result(batch['id'], owner, result, now)
        except Exception:
            # MailOutbox alone knows whether DATA was accepted; never guess retry.
            try:
                result = self.outbox.status(batch['id']) or {'state': 'uncertain'}
            except Exception:
                result = {'state': 'uncertain'}
            return self._record_result(batch['id'], owner, result, now)

    def dispatch_due(self, now=None, limit=20):
        now = _aware(now or self.clock())
        self._freeze_due_batches(now)
        with self._db() as db:
            rows = db.execute('''SELECT * FROM assistent_bestellpakete
                WHERE state IN ('ready','not_sent','blocked','copy_pending','partial','sending')
                AND next_attempt_at<=? AND lease_until<=?
                AND due_at<=? AND not_before_at<=?
                AND (attempts<? OR state IN ('copy_pending','partial','sending'))
                ORDER BY due_at,id LIMIT ?''',
                (now.timestamp(), now.timestamp(), now.timestamp(), now.timestamp(), self.MAX_SEND_ATTEMPTS, max(1, min(int(limit), 100)))).fetchall()
        results = []
        for row in rows:
            owner = uuid.uuid4().hex
            with self._db() as db:
                claimed = db.execute('''UPDATE assistent_bestellpakete SET lease=?,lease_until=?
                    WHERE id=? AND lease_until<=? AND next_attempt_at<=?
                    AND due_at<=? AND not_before_at<=?
                    AND (attempts<? OR state IN ('copy_pending','partial','sending'))
                    AND state IN ('ready','not_sent','blocked','copy_pending','partial','sending')''',
                    (owner, now.timestamp()+self.LEASE_SECONDS, row['id'], now.timestamp(),
                     now.timestamp(), now.timestamp(), now.timestamp(), self.MAX_SEND_ATTEMPTS))
                db.commit()
            if claimed.rowcount == 1:
                results.append(dict(self._deliver(dict(row), owner, now), batch_id=row['id']))
        return {'batches': results, 'orders': self.list_orders(limit=100)}

    def status(self, order_id):
        with self._db() as db:
            row = db.execute('''SELECT o.*,b.state AS batch_state,b.attempts,b.result_json,b.not_before_at FROM assistent_bestellanforderungen o
                LEFT JOIN assistent_bestellpakete b ON b.id=o.batch_id WHERE o.id=?''', (order_id,)).fetchone()
        if not row:
            raise ValueError('Bestellanforderung nicht gefunden.')
        state = row['batch_state'] or 'queued'
        if row['batch_id']:
            try:
                actual = self.outbox.status(row['batch_id'])
            except Exception:
                actual = None  # Retain the last durably recorded result.
                if state in {'ready', 'sending'}:
                    state = 'uncertain'
            if (actual and actual.get('state') in _STATES
                    and not (state == 'blocked' and actual.get('state') == 'not_sent')):
                state = actual['state']
        snapshot = json.loads(row['snapshot_json'])
        message = _MESSAGES[state]
        if state == 'blocked':
            try:
                message = json.loads(row['result_json'] or '{}').get('message') or message
            except (ValueError, TypeError):
                pass
        return {'id': row['id'], 'actor_id': row['actor_id'], 'state': state, 'message': message,
                'due_at': datetime.fromtimestamp(max(row['due_at'],row['not_before_at'] or 0), timezone.utc).isoformat(),
                'original_due_at': datetime.fromtimestamp(row['legacy_due_at'] if row['legacy_due_at'] is not None else row['due_at'], timezone.utc).isoformat(),
                'order': snapshot['order'], 'batch_id': row['batch_id'] or None,
                'needs_review': state in {'uncertain', 'partial', 'blocked'} or
                                (state == 'not_sent' and (row['attempts'] or 0) >= self.MAX_SEND_ATTEMPTS)}

    def list_orders(self, actor_id=None, limit=100):
        with self._db() as db:
            where, args = (' WHERE actor_id=?', [str(actor_id)]) if actor_id is not None else ('', [])
            rows = db.execute('SELECT id FROM assistent_bestellanforderungen'+where+' ORDER BY created_at DESC,id DESC LIMIT ?',
                              (*args, max(1, min(int(limit), 100)))).fetchall()
        return [self.status(row['id']) for row in rows]


def build_order_dispatch(get_db, storage_dir, imap_config, smtp_config,
                         authorize_order=None, supplier_resolver=None, clock=None,
                         reservation_guard=None, batch_guard=None, schedule_guard=None):
    """Construct the real durable mailbox boundary; does not send or connect."""
    mailbox = Mailbox(imap_config)
    outbox = MailOutbox(get_db, storage_dir, mailbox)
    return OrderDispatch(get_db, outbox, smtp_config, authorize_order, supplier_resolver, clock,
                         reservation_guard, batch_guard, schedule_guard)
