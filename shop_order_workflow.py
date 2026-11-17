"""Explicit durable authorization-to-fulfillment worker; no import/startup IO.

The only automatic entry is the verified Checkout wrapper or a bounded repair
scan of the same immutable payment ledger. Browser returns/status never enqueue.
The saga and payment-action journals remain the final at-most-once side-effect
fences. A stale queue lease may resume a saga; it never grants a new supplier or
capture POST itself. All adapters/storage/start/activation are explicitly injected.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
import threading
from uuid import uuid4


class WorkflowError(ValueError):
    pass


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        raise WorkflowError('workflow_unavailable') from None


def init_order_workflow_schema(connection):
    connection.executescript('''
      CREATE TABLE IF NOT EXISTS shop_order_workflow_v2 (
        order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, mode TEXT NOT NULL,
        binding_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'queued',
        lease_token TEXT, lease_until TEXT, attempts INTEGER NOT NULL DEFAULT 0,
        result_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
      CREATE TRIGGER IF NOT EXISTS shop_order_workflow_identity_immutable_v2
        BEFORE UPDATE OF order_id,customer_id,mode,binding_json,created_at ON shop_order_workflow_v2
        BEGIN SELECT RAISE(ABORT,'immutable_order_workflow'); END;
    ''')
    connection.commit()


class WorkflowJournal:
    def __init__(self, connection_factory, *, clock=None, lease_seconds=300):
        if not callable(connection_factory) or type(lease_seconds) is not int or not 60 <= lease_seconds <= 3600:
            raise WorkflowError('invalid_workflow_configuration')
        self.factory, self.clock, self.lease_seconds = connection_factory, clock or (lambda: datetime.now(timezone.utc)), lease_seconds

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
            raise WorkflowError('workflow_unavailable') from None
        finally:
            if isinstance(connection, sqlite3.Connection):
                connection.close()

    def existing(self, order_id, *, mode, customer_id=None):
        with self._db() as connection:
            row = connection.execute('SELECT * FROM shop_order_workflow_v2 WHERE order_id=? AND mode=?', (order_id, mode)).fetchone()
        if row is not None and customer_id is not None and row['customer_id'] != customer_id:
            return None
        return dict(row) if row else None

    def eligible(self, *, mode, limit=20):
        with self._db() as connection:
            return [row[0] for row in connection.execute('''SELECT o.id FROM shop_orders_v2 o
              JOIN shop_payment_attempts_v2 a ON a.order_id=o.id
              WHERE o.mode=? AND o.status='authorized' AND a.session_id IS NOT NULL AND a.intent_id IS NOT NULL
                AND EXISTS (SELECT 1 FROM shop_payment_events_v2 e WHERE e.order_id=o.id AND e.attempt_id=a.id AND e.kind='authorized')
                AND NOT EXISTS (SELECT 1 FROM shop_order_workflow_v2 j WHERE j.order_id=o.id)
              ORDER BY o.created_at,o.id LIMIT ?''', (mode, limit))]

    def enqueue_authorized(self, order_id, *, mode, receipt_service, prepared):
        """Qualify the retained authorization and persist job+receipt atomically."""
        if not isinstance(order_id, str) or not re.fullmatch(r'sho_[a-f0-9]{32}', order_id):
            raise WorkflowError('invalid_workflow_order')
        with self._db(write=True) as connection:
            prior = connection.execute('SELECT * FROM shop_order_workflow_v2 WHERE order_id=?', (order_id,)).fetchone()
            if prior:
                if prior['mode'] != mode:
                    raise ValueError()
                return dict(prior)
            row = connection.execute('''SELECT o.*,a.id AS payment_attempt,a.session_id,a.intent_id
              FROM shop_orders_v2 o JOIN shop_payment_attempts_v2 a ON a.order_id=o.id
              WHERE o.id=? AND o.mode=? AND o.status='authorized' AND a.session_id IS NOT NULL AND a.intent_id IS NOT NULL
                AND EXISTS (SELECT 1 FROM shop_payment_events_v2 e WHERE e.order_id=o.id AND e.attempt_id=a.id AND e.kind='authorized')''',
                (order_id, mode)).fetchone()
            if row is None:
                return None
            expected = {key: row[key] for key in ('id', 'customer_id', 'mode', 'quote_hash', 'amount_gross_cent', 'currency')}
            if prepared['binding'] != expected or prepared['kind'] != 'order_received':
                raise ValueError()
            receipt_service.enqueue_in_transaction(connection, prepared)
            binding = {**expected, **{key: row[key] for key in ('payment_attempt', 'session_id', 'intent_id')}}
            now = _stamp(self.clock()).isoformat()
            connection.execute('''INSERT INTO shop_order_workflow_v2
              (order_id,customer_id,mode,binding_json,created_at,updated_at) VALUES (?,?,?,?,?,?)''',
              (order_id, row['customer_id'], mode, _json(binding), now, now))
            return dict(connection.execute('SELECT * FROM shop_order_workflow_v2 WHERE order_id=?', (order_id,)).fetchone())

    def claim(self, *, mode):
        now = _stamp(self.clock())
        with self._db(write=True) as connection:
            row = connection.execute('''SELECT * FROM shop_order_workflow_v2 WHERE mode=?
              AND (state='queued' OR (state='running' AND lease_until<=?)) ORDER BY created_at,order_id LIMIT 1''',
              (mode, now.isoformat())).fetchone()
            if row is None:
                return None
            token = uuid4().hex
            connection.execute("UPDATE shop_order_workflow_v2 SET state='running',lease_token=?,lease_until=?,attempts=attempts+1,updated_at=? WHERE order_id=?",
                (token, (now + timedelta(seconds=self.lease_seconds)).isoformat(), now.isoformat(), row['order_id']))
            return dict(connection.execute('SELECT * FROM shop_order_workflow_v2 WHERE order_id=?', (row['order_id'],)).fetchone())

    def finish(self, job, *, state, receipt_service=None, prepared=None):
        if state not in {'fulfilled', 'cancelled', 'manual_review'}:
            raise WorkflowError('invalid_workflow_result')
        with self._db(write=True) as connection:
            row = connection.execute("SELECT * FROM shop_order_workflow_v2 WHERE order_id=? AND state='running' AND lease_token=?",
                                     (job['order_id'], job['lease_token'])).fetchone()
            if row is None:
                return False
            if prepared is not None:
                if (prepared['order_id'] != row['order_id'] or prepared['customer_id'] != row['customer_id']
                        or prepared['mode'] != row['mode']):
                    raise ValueError()
                receipt_service.enqueue_in_transaction(connection, prepared)
            connection.execute('UPDATE shop_order_workflow_v2 SET state=?,result_code=?,updated_at=? WHERE order_id=? AND lease_token=?',
                (state, state, _stamp(self.clock()).isoformat(), job['order_id'], job['lease_token']))
            return True


class ShopOrderWorkflow:
    def __init__(self, store, journal, fulfillment, payment_actions=None, *, mode, enabled=False,
                 receipt_service=None, clock=None, poll_seconds=2):
        if (not isinstance(mode, str) or mode not in {'test', 'live'} or type(enabled) is not bool
                or type(poll_seconds) not in (int, float) or not 0.5 <= poll_seconds <= 60):
            raise WorkflowError('invalid_workflow_configuration')
        self.store, self.journal, self.fulfillment, self.payment = store, journal, fulfillment, payment_actions
        self.mode, self.enabled, self.receipts = mode, enabled, receipt_service
        self.clock, self.poll_seconds = clock or (lambda: datetime.now(timezone.utc)), poll_seconds
        self._stop, self._thread, self._run_lock, self._start_lock = threading.Event(), None, threading.Lock(), threading.Lock()

    def readiness(self):
        try:
            fulfillment = self.fulfillment.readiness()
            receipt = self.receipts.readiness()
            ready = (self.enabled and fulfillment.get('ready') is True and fulfillment.get('mode') == self.mode
                     and receipt.get('queue_ready') is True and receipt.get('mode') == self.mode
                     and all(callable(getattr(self.journal, method, None)) for method in ('existing', 'eligible', 'enqueue_authorized', 'claim', 'finish'))
                     and all(callable(getattr(self.store, method, None)) for method in ('get_order', 'lookup_payment_owner')))
        except Exception:
            ready, receipt = False, {}
        return {'ready': ready, 'mode': self.mode, 'mail_delivery_ready': receipt.get('delivery_ready') is True}

    def _active(self):
        if not self.readiness()['ready']:
            raise WorkflowError('workflow_disabled')

    def consider(self, order_id):
        self._active()
        existing = self.journal.existing(order_id, mode=self.mode)
        if existing is not None:
            return existing
        identity = self.store.lookup_payment_owner(order_id, mode=self.mode)
        if identity is None:
            return None
        order = self.store.get_order(order_id, customer_id=identity['customer_id'], mode=self.mode)
        if order['status'] != 'authorized':
            return None
        prepared = self.receipts.prepare(order_id, customer_id=identity['customer_id'], kind='order_received')
        return self.journal.enqueue_authorized(order_id, mode=self.mode, receipt_service=self.receipts, prepared=prepared)

    def repair(self, limit=20):
        self._active()
        if type(limit) is not int or not 1 <= limit <= 50:
            raise WorkflowError('invalid_workflow_limit')
        queued = 0
        for order_id in self.journal.eligible(mode=self.mode, limit=limit):
            try:
                if self.consider(order_id) is not None:
                    queued += 1
            except Exception:
                # A missing frozen legal pack/address must not execute goods,
                # nor prevent other independently valid orders being queued.
                continue
        return queued

    def run_once(self):
        self._active()
        if not self._run_lock.acquire(blocking=False):
            return {'processed': False, 'state': 'busy'}
        try:
            self.repair()
            job = self.journal.claim(mode=self.mode)
            if job is None:
                self.receipts.deliver_once()
                return {'processed': False, 'state': 'idle'}
            try:
                current = self.store.get_order(job['order_id'], customer_id=job['customer_id'], mode=self.mode)
                if current['status'] not in {'authorized', 'paid', 'cancelled'}:
                    self.journal.finish(job, state='manual_review')
                    return {'processed': True, 'state': 'manual_review'}
                # The saga fresh-checks authorization before new procurement,
                # but can reconcile a prior capture when the PI is now paid.
                result = self.fulfillment.run(job['order_id'], customer_id=job['customer_id'])
                if not isinstance(result, dict) or result.get('order_id') != job['order_id'] or result.get('mode') != self.mode:
                    raise ValueError()
                state, prepared = 'manual_review', None
                if result.get('state') == 'captured' and result.get('payment_confirmed') is True and result.get('authorization_released') is False:
                    state = 'fulfilled'
                    prepared = self.receipts.prepare(job['order_id'], customer_id=job['customer_id'], kind='order_confirmed')
                elif result.get('state') == 'cancelled' and result.get('authorization_released') is True and result.get('payment_confirmed') is False:
                    state = 'cancelled'
                    prepared = self.receipts.prepare(job['order_id'], customer_id=job['customer_id'], kind='order_cancelled')
                self.journal.finish(job, state=state, receipt_service=self.receipts, prepared=prepared)
                self.receipts.deliver_once()
                return {'processed': True, 'state': state}
            except Exception:
                # Keep the claim for stale-lease recovery. The independent saga
                # journals prevent another POST after an uncertain operation.
                return {'processed': True, 'state': 'deferred'}
        finally:
            self._run_lock.release()

    def start(self):
        self._active()
        with self._start_lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            def loop():
                while not self._stop.is_set():
                    try:
                        self.run_once()
                    except Exception:
                        pass
                    self._stop.wait(self.poll_seconds)
            self._thread = threading.Thread(target=loop, name='shop-order-workflow', daemon=True)
            self._thread.start()
            return True

    def stop(self, timeout=5):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(min(max(float(timeout), 0), 10))

    def customer_status(self, order_id, *, customer_id):
        self.store.get_order(order_id, customer_id=customer_id, mode=self.mode)
        job = self.journal.existing(order_id, mode=self.mode, customer_id=customer_id)
        return {'state': job['state'] if job else 'not_queued', 'delivery_confirmed': False}


class WorkflowCheckout:
    """Drop-in Checkout facade; only the real checked result can enqueue work."""
    def __init__(self, checkout, workflow):
        self.checkout, self.workflow, self.mode = checkout, workflow, checkout.mode
        if workflow.mode != self.mode:
            raise WorkflowError('workflow_mode_mismatch')

    def readiness(self):
        result = dict(self.checkout.readiness())
        result['ready'] = result.get('ready') is True and self.workflow.readiness()['ready'] is True
        return result

    def start(self, order_id, *, customer_id):
        result = self.checkout.start(order_id, customer_id=customer_id)
        if result.get('status') == 'authorized':
            self.workflow.consider(order_id)
        return result

    def status(self, order_id, *, customer_id):
        return self.checkout.status(order_id, customer_id=customer_id)

    def reconcile(self, order_id, *, customer_id):
        result = self.checkout.reconcile(order_id, customer_id=customer_id)
        if result.get('status') == 'authorized':
            self.workflow.consider(order_id)
        return result

    def handle_webhook(self, raw_body, signature):
        result = self.checkout.handle_webhook(raw_body, signature)
        if result.get('ignored') is False and result.get('status') == 'authorized':
            self.workflow.consider(result['order_id'])
        return result
