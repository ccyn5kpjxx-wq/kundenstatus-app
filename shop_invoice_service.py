"""Durable order-to-Lexware handoff. Only a first, committed claim can issue an invoice."""
from contextlib import closing, contextmanager
from datetime import datetime, timezone
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
from shop_order_store import ORDER_ID


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
    def __init__(self, store, adapter, *, clock=time.time):
        self.connect = store._connect
        self.adapter = adapter
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
                """)
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
                    j.remote_id, j.updated_at FROM shop_orders_v2 o
                    LEFT JOIN shop_lexware_invoices_v1 j ON j.order_id=o.id
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
            record = {'state': state, 'voucher_number': row['voucher_number'] or ''}
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
        try:
            original = self.adapter.pdf(row['remote_id'], json.loads(row['payload_json']))
        except ShopLexwareError:
            raise ShopInvoiceError('invoice_pdf_unavailable') from None
        number = re.sub(r'[^\w.-]+', '-', row['voucher_number']).strip('.-') or order_id
        return original, 'Rechnung-' + number + '.pdf'

    def process_next(self):
        if not self.adapter.configured():
            return False
        now = self._now().timestamp()
        with self._db() as db:
            order = db.execute(f"""SELECT o.id FROM shop_orders_v2 o LEFT JOIN shop_lexware_invoices_v1 j
                ON j.order_id=o.id WHERE {_PROOF}
                AND ((j.order_id IS NULL AND o.created_at >=
                      (SELECT starts_at FROM shop_invoice_activation_v1 WHERE mode='live'))
                     OR (j.state='retry' AND j.updated_at < ?))
                ORDER BY COALESCE(j.updated_at,0),o.created_at,o.id LIMIT 1""", (now - 300,)).fetchone()
            pending = db.execute("""SELECT order_id FROM shop_lexware_invoices_v1
                WHERE state IN ('preparing','unknown') AND updated_at < ?
                ORDER BY updated_at LIMIT 1""", (now - 1800,)).fetchone()
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
