"""Durable order-to-Lexware handoff. Only a first, committed claim can issue an invoice."""
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from hashlib import sha256
import json
import logging
import re
import sqlite3
import threading
import time
from uuid import UUID
from zoneinfo import ZoneInfo

from shop_lexware_invoices import build_shop_invoice_payload, ShopLexwareError
from shop_measurement import purchase_projection
from shop_order_receipts import _receipt_context
from shop_order_store import ORDER_ID, _delivery_context, _validate_snapshot


class ShopInvoiceError(ValueError):
    pass


_PROOF = """o.mode='live' AND o.status='paid' AND
    EXISTS (SELECT 1 FROM shop_payment_attempts_v2 a
      JOIN shop_payment_events_v2 e ON e.order_id=a.order_id AND e.attempt_id=a.id AND e.kind='paid'
      JOIN shop_payment_actions_v2 c ON c.order_id=a.order_id AND c.intent_id=a.intent_id
      WHERE a.order_id=o.id AND a.state='paid' AND a.session_id IS NOT NULL AND a.intent_id IS NOT NULL
        AND c.operation='capture' AND c.state='captured')
    AND EXISTS (SELECT 1 FROM shop_order_workflow_v2 w JOIN shop_fulfillment_v2 f
      ON f.order_id=w.order_id AND f.customer_id=w.customer_id AND f.mode=w.mode
      WHERE w.order_id=o.id AND w.customer_id=o.customer_id AND w.mode=o.mode
        AND w.state='fulfilled' AND f.state='accepted')
    AND EXISTS (SELECT 1 FROM shop_fulfillment_positions_v3 p WHERE p.order_id=o.id AND p.state='accepted')
    AND NOT EXISTS (SELECT 1 FROM shop_fulfillment_positions_v3 p WHERE p.order_id=o.id AND p.state!='accepted')"""


class ShopInvoiceService:
    def __init__(self, store, adapter, *, clock=time.time, mailer=None):
        self.connect = store._connect
        self.adapter = adapter
        self.mailer = mailer
        self.clock = clock
        self._stop = threading.Event()
        self._start_lock = threading.Lock()
        self._thread = None
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS shop_invoice_activation_v1 (
                    mode TEXT PRIMARY KEY, starts_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS shop_lexware_invoices_v1 (
                    order_id TEXT PRIMARY KEY REFERENCES shop_orders_v2(id),
                    state TEXT NOT NULL, payload_json TEXT, remote_id TEXT UNIQUE,
                    voucher_number TEXT, updated_at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS shop_lexware_invoice_recovery_idx
                    ON shop_lexware_invoices_v1(state,updated_at);
                CREATE TABLE IF NOT EXISTS shop_invoice_mail_v1 (
                    order_id TEXT PRIMARY KEY REFERENCES shop_orders_v2(id) ON DELETE CASCADE,
                    state TEXT NOT NULL DEFAULT 'pending', next_at REAL NOT NULL DEFAULT 0,
                    recipient TEXT, voucher_number TEXT, pdf_sha256 TEXT, attempts INTEGER NOT NULL DEFAULT 0);
                CREATE INDEX IF NOT EXISTS shop_invoice_mail_next_idx ON shop_invoice_mail_v1(state,next_at);
                """)
            if mailer is not None:
                # The trigger itself is the durable activation boundary, including for
                # concurrent checkouts and orders created in the very same clock tick.
                db.execute("""CREATE TRIGGER IF NOT EXISTS shop_invoice_mail_new_order_v1
                    AFTER INSERT ON shop_orders_v2 WHEN NEW.mode='live'
                    BEGIN INSERT INTO shop_invoice_mail_v1(order_id) VALUES(NEW.id); END""")
            db.execute("INSERT OR IGNORE INTO shop_invoice_activation_v1 VALUES ('live',?)",
                       (self._now().isoformat(),))
            db.commit()

    def _now(self):
        value = self.clock()
        if isinstance(value, datetime):
            if value.utcoffset() is None:
                raise ShopInvoiceError('invalid_clock')
            return value.astimezone(timezone.utc)
        return datetime.fromtimestamp(value, timezone.utc)

    @contextmanager
    def _db(self, *, write=False):
        with closing(self.connect()) as db:
            db.row_factory = sqlite3.Row
            with db:
                if write:
                    db.execute('BEGIN IMMEDIATE')
                yield db

    def statuses(self, order_ids):
        ids = list(dict.fromkeys(key for key in order_ids if isinstance(key, str) and ORDER_ID.fullmatch(key)))
        if len(ids) > 100:
            raise ShopInvoiceError('too_many_orders')
        if not ids:
            return {}
        with self._db() as db:
            rows = db.execute(f"""SELECT o.id, ({_PROOF}) eligible, j.state, j.voucher_number,
                    j.remote_id, j.updated_at, m.state mail_state, m.next_at FROM shop_orders_v2 o
                    LEFT JOIN shop_lexware_invoices_v1 j ON j.order_id=o.id
                    LEFT JOIN shop_invoice_mail_v1 m ON m.order_id=o.id
                    WHERE o.id IN ({','.join('?' for _ in ids)})""", ids).fetchall()
        configured = self.adapter.configured()
        now = self._now().timestamp()
        result = {}
        for row in rows:
            state = row['state']
            if state is None:
                state = 'ineligible' if not row['eligible'] else ('available' if configured else 'unavailable')
            elif state == 'preparing' and now - row['updated_at'] > 180:
                state = 'unknown'
            elif state == 'retry':
                state = 'available' if configured else 'unavailable'
            mail_state = row['mail_state'] if state == 'ready' else None
            if mail_state == 'sending' and row['next_at'] <= now:
                mail_state = 'unknown'
            record = {'state': state, 'voucher_number': row['voucher_number'] or '', 'mail_state': mail_state}
            if self.mailer is not None and state == 'ready' and mail_state is None:
                record['predates_automatic_mail'] = True
            if state == 'draft' and row['remote_id']:
                record['edit_url'] = 'https://app.lexware.de/permalink/invoices/edit/' + row['remote_id']
            result[row['id']] = record
        return result

    def status(self, order_id):
        try:
            return self.statuses([order_id])[order_id]
        except KeyError:
            raise ShopInvoiceError('order_not_found') from None

    def _claim(self, order_id):
        """Freeze invoice contents and commit the unique right to POST before any external IO."""
        now = self._now()
        with self._db(write=True) as db:
            order = db.execute(f'SELECT o.* FROM shop_orders_v2 o WHERE o.id=? AND {_PROOF}', (order_id,)).fetchone()
            if not order or not purchase_projection(db, order):
                raise ShopInvoiceError('payment_or_fulfillment_unconfirmed')
            prior = db.execute('SELECT * FROM shop_lexware_invoices_v1 WHERE order_id=?', (order_id,)).fetchone()
            if prior:
                if prior['state'] != 'retry':
                    return None
                # A failed preflight is the only retryable state. No POST took place.
                db.execute("UPDATE shop_lexware_invoices_v1 SET state='preparing', updated_at=? WHERE order_id=?",
                           (now.timestamp(), order_id))
                return json.loads(prior['payload_json'])
            payment = db.execute("""SELECT last_event_at FROM shop_payment_attempts_v2
                WHERE order_id=? AND state='paid'""", (order_id,)).fetchone()
            try:
                if not payment['last_event_at']:
                    raise ValueError()
                snapshot = json.loads(order['snapshot_json'])
                snapshot['quote_hash'] = order['quote_hash']
                payload = build_shop_invoice_payload(snapshot, {**dict(order), 'paid_at': payment['last_event_at']},
                    json.loads(order['delivery_context_json']), voucher_date=now.astimezone(ZoneInfo('Europe/Berlin')).date())
                frozen = json.dumps(payload, allow_nan=False, ensure_ascii=False)
                state = 'preparing'
            except (ValueError, TypeError, KeyError):
                frozen, state, payload = None, 'blocked', None
            db.execute('INSERT INTO shop_lexware_invoices_v1(order_id,state,payload_json,updated_at) VALUES(?,?,?,?)',
                       (order_id, state, frozen, now.timestamp()))
            return payload

    def _set_state(self, order_id, state, *, expected, number=None):
        with self._db(write=True) as db:
            db.execute(f"""UPDATE shop_lexware_invoices_v1 SET state=?,updated_at=?,
                       voucher_number=COALESCE(?,voucher_number) WHERE order_id=?
                       AND state IN ({','.join('?' for _ in expected)})""",
                       (state, self._now().timestamp(), number, order_id, *expected))

    def _link(self, order_id, remote_id):
        remote_id = str(UUID(remote_id))
        with self._db(write=True) as db:
            cursor = db.execute("""UPDATE shop_lexware_invoices_v1 SET remote_id=? WHERE order_id=?
                AND (remote_id IS NULL OR remote_id=?)""", (remote_id, order_id, remote_id))
            if cursor.rowcount != 1:
                raise ShopInvoiceError('invoice_identity_conflict')
        return remote_id

    def _verify(self, order_id, remote_id, payload):
        info = self.adapter.inspect(remote_id, payload)
        state = {'draft': 'draft', 'final': 'ready'}[info['state']]
        self._set_state(order_id, state, expected=('preparing', 'unknown', 'draft'), number=info['voucher_number'])

    def prepare(self, order_id):
        if not self.adapter.configured():
            raise ShopInvoiceError('lexware_unavailable')
        payload = self._claim(order_id)
        if payload is not None:
            try:
                remote_id = self.adapter.create_invoice(payload, finalize=True)
                self._verify(order_id, self._link(order_id, remote_id), payload)
            except Exception as error:
                # Ambiguous writes are only ever followed by a read-only search.
                safe_retry = isinstance(error, ShopLexwareError) and str(error) == 'lexware_invoice_preflight_unavailable'
                self._set_state(order_id, 'retry' if safe_retry else 'unknown', expected=('preparing',))
        return self.status(order_id)

    def refresh(self, order_id):
        with self._db() as db:
            row = db.execute('SELECT * FROM shop_lexware_invoices_v1 WHERE order_id=?', (order_id,)).fetchone()
        if not row or row['state'] not in ('unknown', 'draft', 'preparing', 'ready'):
            raise ShopInvoiceError('no_invoice_attempt')
        if row['state'] == 'ready':
            return self.status(order_id)
        if row['state'] == 'preparing' and self._now().timestamp() - row['updated_at'] <= 180:
            return self.status(order_id)
        try:
            payload = json.loads(row['payload_json'])
            remote_id = row['remote_id'] or self.adapter.recover(payload)
            if remote_id:
                self._verify(order_id, self._link(order_id, remote_id), payload)
            else:
                self._set_state(order_id, 'unknown', expected=('unknown','preparing'))
        except Exception:
            self._set_state(order_id, row['state'] if row['state'] == 'draft' else 'unknown',
                            expected=('unknown','preparing','draft'))
        return self.status(order_id)

    def pdf(self, order_id):
        with self._db() as db:
            row = db.execute("""SELECT remote_id,payload_json,voucher_number FROM shop_lexware_invoices_v1
                                WHERE order_id=? AND state='ready'""", (order_id,)).fetchone()
        if not row:
            raise ShopInvoiceError('invoice_not_ready')
        return self._original_pdf(order_id, row)

    def _original_pdf(self, order_id, row):
        try:
            original = self.adapter.pdf(row['remote_id'], json.loads(row['payload_json']),
                                        voucher_number=row['voucher_number'])
        except ShopLexwareError:
            raise ShopInvoiceError('invoice_pdf_unavailable') from None
        number = re.sub(r'[^\w.-]+', '-', row['voucher_number']).strip('.-') or order_id
        return original, 'Rechnung-' + number + '.pdf'

    def _deliver_mail(self):
        now = self._now().timestamp()
        with self._db(write=True) as db:
            row = db.execute(f"""SELECT o.*, j.remote_id, j.payload_json, j.voucher_number, m.attempts,
                m.pdf_sha256, m.recipient sent_to, m.voucher_number sent_number
                FROM shop_invoice_mail_v1 m JOIN shop_orders_v2 o ON o.id=m.order_id
                JOIN shop_lexware_invoices_v1 j ON j.order_id=o.id
                WHERE m.state IN ('pending','not_sent') AND m.next_at<=?
                AND j.state='ready' AND {_PROOF} ORDER BY m.next_at,o.created_at,o.id LIMIT 1""",
                (now,)).fetchone()
            if row is None:
                return False
            order_id = row['id']
            try:
                snapshot = json.loads(row['snapshot_json'])
                snapshot['quote_hash'] = row['quote_hash']
                _validate_snapshot(snapshot, row['quote_hash'], row['customer_id'], row['mode'])
                context = json.loads(row['delivery_context_json'])
                _delivery_context(snapshot, context)
                recipient = _receipt_context(context)['contact']['email']
                payload = json.loads(row['payload_json'])
                if (payload['introduction'] != 'Ihre Shopbestellung ' + order_id
                        or not row['voucher_number'] or not row['remote_id']
                        or not purchase_projection(db, row)):
                    raise ValueError()
            except (ValueError, KeyError, TypeError):
                db.execute("UPDATE shop_invoice_mail_v1 SET state='blocked' WHERE order_id=?", (order_id,))
                return True
            # Defer the next read attempt before fetching the PDF. A crash or
            # transient Lexware failure here has never acquired the right to send.
            db.execute("UPDATE shop_invoice_mail_v1 SET next_at=? WHERE order_id=?", (now + 300, order_id))

        try:
            content, filename = self._original_pdf(order_id, row)
        except ShopInvoiceError:
            return True
        digest = sha256(content).hexdigest()
        attempt = row['attempts'] + 1
        with self._db(write=True) as db:
            if row['attempts'] and (row['pdf_sha256'], row['sent_to'], row['sent_number']) != (
                    digest, recipient, row['voucher_number']):
                db.execute("""UPDATE shop_invoice_mail_v1 SET state='blocked' WHERE order_id=?
                           AND state='not_sent' AND attempts=? AND next_at=?""", (order_id, row['attempts'], now + 300))
                return True
            # Recheck eligibility and the frozen invoice after the provider read.
            # Only the committed winner may cross the SMTP boundary.
            if not purchase_projection(db, row):
                return True
            claimed = db.execute(f"""UPDATE shop_invoice_mail_v1 SET state='sending', recipient=?,
                    voucher_number=?,pdf_sha256=?,attempts=?,next_at=? WHERE order_id=? AND state IN ('pending','not_sent')
                    AND next_at=? AND EXISTS (SELECT 1 FROM shop_orders_v2 o
                        JOIN shop_lexware_invoices_v1 j ON j.order_id=o.id WHERE o.id=? AND {_PROOF}
                        AND j.state='ready' AND j.remote_id=? AND j.voucher_number=? AND j.payload_json=?)""",
                (recipient, row['voucher_number'], digest, attempt, self._now().timestamp() + 900,
                 order_id, now + 300, order_id, row['remote_id'], row['voucher_number'], row['payload_json']))
            if claimed.rowcount != 1:
                return True
        try:
            state = self.mailer.send(order_id=order_id, recipient=recipient, voucher_number=row['voucher_number'],
                                     content=content, filename=filename)
        except Exception:
            state = 'unknown'
        if state not in ('submitted', 'not_sent', 'unknown'):
            state = 'unknown'
        retry_delay = min(86400, 900 * 2 ** min(attempt - 1, 7)) if state == 'not_sent' else 900
        with self._db(write=True) as db:
            db.execute("""UPDATE shop_invoice_mail_v1 SET state=?,next_at=?
                WHERE order_id=? AND state IN ('sending','unknown') AND attempts=?""",
                (state, self._now().timestamp() + retry_delay, order_id, attempt))
        return True

    def _reconcile_mail(self):
        now = self._now().timestamp()
        with self._db(write=True) as db:
            row = db.execute("""SELECT order_id,recipient,attempts FROM shop_invoice_mail_v1
                WHERE state IN ('unknown','sending') AND next_at<=? ORDER BY next_at LIMIT 1""", (now,)).fetchone()
            if row is None:
                return False
            db.execute("""UPDATE shop_invoice_mail_v1 SET state='unknown',next_at=? WHERE order_id=?""",
                       (now + 900, row['order_id']))
        # Reconciliation is read-only. An absent Outbox entry is ambiguous.
        try:
            state = self.mailer.status(order_id=row['order_id'], recipient=row['recipient'])
        except Exception:
            return True
        if state in ('submitted', 'not_sent'):
            with self._db(write=True) as db:
                db.execute("""UPDATE shop_invoice_mail_v1 SET state=? WHERE order_id=? AND state='unknown'
                           AND attempts=?""", (state, row['order_id'], row['attempts']))
        return True

    def _process_invoice(self):
        now = self._now().timestamp()
        with self._db() as db:
            order = db.execute(f"""SELECT o.id FROM shop_orders_v2 o LEFT JOIN shop_lexware_invoices_v1 j
                ON j.order_id=o.id WHERE {_PROOF}
                AND ((j.order_id IS NULL AND o.created_at >=
                      (SELECT starts_at FROM shop_invoice_activation_v1 WHERE mode='live'))
                     OR (j.state='retry' AND j.updated_at < ?))
                ORDER BY COALESCE(j.updated_at,0),o.created_at,o.id LIMIT 1""", (now - 300,)).fetchone()
            pending = db.execute("""SELECT j.order_id FROM shop_lexware_invoices_v1 j
                WHERE (j.state IN ('preparing','unknown') OR (j.state='draft' AND ? AND EXISTS
                    (SELECT 1 FROM shop_invoice_mail_v1 m WHERE m.order_id=j.order_id)))
                AND j.updated_at < ? ORDER BY j.updated_at LIMIT 1""",
                (self.mailer is not None, now - 1800)).fetchone()
        if order:
            try:
                self.prepare(order['id'])
            except ShopInvoiceError:
                # A structurally inconsistent capture must not starve later orders.
                with self._db(write=True) as db:
                    db.execute("""INSERT OR IGNORE INTO shop_lexware_invoices_v1(order_id,state,updated_at)
                                  VALUES(?,'blocked',?)""", (order['id'], now))
            return True
        if pending:
            self.refresh(pending['order_id'])
            return True
        return False

    def process_next(self):
        if not self.adapter.configured():
            return False
        issued = self._process_invoice()
        if self.mailer is None:
            return issued
        delivered = self._deliver_mail()
        reconciled = self._reconcile_mail()
        return issued or delivered or reconciled

    def start(self):
        if not self.adapter.configured():
            return False
        with self._start_lock:
            if self._thread and self._thread.is_alive():
                return False
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, daemon=True, name='shop-lexware-invoices')
            self._thread.start()
            return True

    def _run(self):
        while not self._stop.is_set():
            try:
                worked = self.process_next()
            except Exception:
                logging.getLogger(__name__).error('Shop-Rechnungsabgleich vorübergehend nicht verfügbar.')
                worked = False
            self._stop.wait(1 if worked else 15)

    def stop(self, timeout=5):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)
