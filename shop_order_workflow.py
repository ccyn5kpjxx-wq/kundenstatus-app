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
import logging
import re
import sqlite3
import threading
from uuid import uuid4
from shop_order_review import requires_review
from shop_order_store import _delivery_context, _validate_snapshot, requires_workshop_order


log = logging.getLogger(__name__)


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
    # Only an already final verified payment may recover an uncertain payment
    # action. Unknown/claimed supplier outcomes can never become replay jobs.
    _PAYMENT_RECOVERY = '''FROM shop_order_workflow_v2 j
      JOIN shop_orders_v2 o ON o.id=j.order_id
      JOIN shop_payment_attempts_v2 a ON a.order_id=o.id
      JOIN shop_fulfillment_v2 f ON f.order_id=o.id AND f.mode=o.mode
      WHERE o.mode=? AND j.state IN ('manual_review','awaiting_review') AND f.action_state='unknown'
        AND ((o.status='paid' AND f.state='accepted' AND NOT EXISTS
            (SELECT 1 FROM shop_fulfillment_positions_v3 p WHERE p.order_id=o.id AND p.state!='accepted'))
          OR (o.status='cancelled' AND f.state='rejected' AND NOT EXISTS
            (SELECT 1 FROM shop_fulfillment_positions_v3 p WHERE p.order_id=o.id AND p.state NOT IN ('pending','rejected'))))
        AND EXISTS (SELECT 1 FROM shop_payment_events_v2 e WHERE e.order_id=o.id AND e.attempt_id=a.id
            AND e.kind=CASE WHEN o.status='paid' THEN 'paid' ELSE 'cancelled' END)'''
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
              LEFT JOIN shop_order_workflow_v2 j ON j.order_id=o.id
              WHERE o.mode=? AND o.status='authorized' AND a.session_id IS NOT NULL AND a.intent_id IS NOT NULL
                AND EXISTS (SELECT 1 FROM shop_payment_events_v2 e WHERE e.order_id=o.id AND e.attempt_id=a.id AND e.kind='authorized')
                AND (j.order_id IS NULL OR j.state='awaiting_review')
              ORDER BY COALESCE(j.updated_at,o.created_at),o.id LIMIT ?''', (mode, limit))]

    def paid_workshop_candidates(self, *, after='', order_id=None, limit=20):
        """Only financially verified live purchases still open for workshop processing."""
        target = ' AND o.id=?' if order_id is not None else ''
        params = (after, order_id, limit) if order_id is not None else (after, limit)
        with self._db() as connection:
            return [dict(row) for row in connection.execute('''SELECT o.id,o.customer_id,o.snapshot_json
                FROM shop_orders_v2 o JOIN shop_payment_attempts_v2 a ON a.order_id=o.id
                WHERE o.id>? AND o.mode='live' AND o.status='paid'
                AND EXISTS (SELECT 1 FROM shop_payment_events_v2 e
                    WHERE e.order_id=o.id AND e.attempt_id=a.id AND e.kind='paid')
                AND NOT EXISTS (SELECT 1 FROM shop_workshop_handled_v1 h WHERE h.order_id=o.id)
                ''' + target + ' ORDER BY o.id LIMIT ?', params)]

    def reconciliation_candidates(self, *, mode, limit=20):
        with self._db() as connection:
            return [r[0] for r in connection.execute('SELECT j.order_id ' + self._PAYMENT_RECOVERY
                + ' ORDER BY j.updated_at,j.order_id LIMIT ?', (mode, limit))]

    def resume_reconciliation(self, order_id, *, mode):
        with self._db(write=True) as connection:
            row = connection.execute('SELECT j.* ' + self._PAYMENT_RECOVERY + ' AND o.id=?', (mode, order_id)).fetchone()
            if row is None:
                return None
            connection.execute("UPDATE shop_order_workflow_v2 SET state='queued',lease_token=NULL,lease_until=NULL,updated_at=? WHERE order_id=?",
                               (_stamp(self.clock()).isoformat(), order_id))
            return dict(connection.execute('SELECT * FROM shop_order_workflow_v2 WHERE order_id=?', (order_id,)).fetchone())

    def enqueue_authorized(self, order_id, *, mode, receipt_service, prepared, state='queued'):
        """Qualify the retained authorization and persist job+receipt atomically."""
        if (not isinstance(order_id, str) or not re.fullmatch(r'sho_[a-f0-9]{32}', order_id)
                or state not in {'queued', 'awaiting_review'}):
            raise WorkflowError('invalid_workflow_order')
        with self._db(write=True) as connection:
            prior = connection.execute('SELECT * FROM shop_order_workflow_v2 WHERE order_id=?', (order_id,)).fetchone()
            if prior:
                if prior['mode'] != mode:
                    raise ValueError()
                if prior['state'] in {'queued', 'awaiting_review'}:
                    connection.execute('''UPDATE shop_order_workflow_v2 SET state=?,lease_token=NULL,lease_until=NULL,
                        updated_at=? WHERE order_id=?''', (state, _stamp(self.clock()).isoformat(), order_id))
                return dict(connection.execute('SELECT * FROM shop_order_workflow_v2 WHERE order_id=?', (order_id,)).fetchone())
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
              (order_id,customer_id,mode,binding_json,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?)''',
              (order_id, row['customer_id'], mode, _json(binding), state, now, now))
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
        if state not in {'fulfilled', 'cancelled', 'manual_review', 'awaiting_review'}:
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
                 receipt_service=None, clock=None, poll_seconds=2, review_service=None,
                 workshop_order_create=None, workshop_order_lookup=None):
        if (not isinstance(mode, str) or mode not in {'test', 'live'} or type(enabled) is not bool
                or type(poll_seconds) not in (int, float) or not 0.5 <= poll_seconds <= 60):
            raise WorkflowError('invalid_workflow_configuration')
        if ((workshop_order_create is None) != (workshop_order_lookup is None)
                or workshop_order_create is not None and not all(map(callable, (workshop_order_create, workshop_order_lookup)))):
            raise WorkflowError('invalid_workshop_order_configuration')
        self.store, self.journal, self.fulfillment, self.payment = store, journal, fulfillment, payment_actions
        self.mode, self.enabled, self.receipts = mode, enabled, receipt_service
        self.review_service = review_service
        self.workshop_order_create, self.workshop_order_lookup = workshop_order_create, workshop_order_lookup
        self._workshop_after, self._workshop_next_scan = '', None
        self.clock, self.poll_seconds = clock or (lambda: datetime.now(timezone.utc)), poll_seconds
        self._stop, self._thread, self._run_lock, self._start_lock = threading.Event(), None, threading.Lock(), threading.Lock()

    def readiness(self):
        try:
            fulfillment = self.fulfillment.readiness()
            receipt = self.receipts.readiness()
            ready = (self.enabled and fulfillment.get('ready') is True and fulfillment.get('mode') == self.mode
                     and receipt.get('queue_ready') is True and receipt.get('mode') == self.mode
                     and all(callable(getattr(self.journal, method, None)) for method in
                         ('existing', 'eligible', 'enqueue_authorized', 'claim', 'finish',
                          'reconciliation_candidates', 'resume_reconciliation'))
                     and all(callable(getattr(self.store, method, None)) for method in ('get_order', 'lookup_payment_owner')))
        except Exception:
            ready, receipt = False, {}
        return {'ready': ready, 'mode': self.mode, 'mail_delivery_ready': receipt.get('delivery_ready') is True}

    def _active(self):
        if not self.readiness()['ready']:
            raise WorkflowError('workflow_disabled')

    def _ensure_workshop_order(self, order_id, customer_id):
        identity = {'customer_id': customer_id, 'mode': 'live'}
        order = self.store.get_order(order_id, **identity)
        if order['status'] != 'paid':
            raise WorkflowError('workshop_payment_unconfirmed')
        snapshot = self.store.snapshot_for_order(order_id, **identity)
        _validate_snapshot(snapshot, order['quote_hash'], customer_id, 'live')
        if not requires_workshop_order(snapshot):
            return None
        context = self.store.delivery_context_for_order(order_id, **identity)
        _delivery_context(snapshot, context)
        number = self.workshop_order_create(order, snapshot, context)
        if type(number) is not int or number < 1:
            raise WorkflowError('workshop_order_unavailable')
        return number

    def consider_paid(self, order_id):
        """A verified payment may arrive before the fulfillment worker finishes."""
        if self.mode == 'live' and self.enabled and self.workshop_order_create is not None:
            try:
                for row in self.journal.paid_workshop_candidates(order_id=order_id, limit=1):
                    self._ensure_workshop_order(row['id'], row['customer_id'])
            except Exception:
                # Payment has already been persisted. The worker recovers the portal write.
                log.warning('Shopauftrag wird nach bestätigter Zahlung erneut abgeglichen: %s', order_id)

    def _repair_workshop_orders(self, limit=20):
        if self.mode != 'live' or self.workshop_order_create is None:
            return
        now = _stamp(self.clock())
        if self._workshop_next_scan is not None and now < self._workshop_next_scan:
            return
        try:
            rows = self.journal.paid_workshop_candidates(after=self._workshop_after, limit=limit)
            service_orders = []
            for row in rows:
                try:
                    if requires_workshop_order(json.loads(row['snapshot_json'])):
                        service_orders.append(row)
                except (TypeError, ValueError):
                    log.warning('Bestellpositionen für den Werkstattabgleich unlesbar: %s', row['id'])
            if service_orders:
                linked = self.workshop_order_lookup([row['id'] for row in service_orders])
                for row in service_orders:
                    if row['id'] not in linked:
                        try:
                            self._ensure_workshop_order(row['id'], row['customer_id'])
                        except Exception:
                            log.warning('Bezahlter Shopauftrag konnte nicht übernommen werden: %s', row['id'])
            self._workshop_after = rows[-1]['id'] if len(rows) == limit else ''
            self._workshop_next_scan = None if self._workshop_after else now + timedelta(minutes=1)
        except Exception:
            self._workshop_next_scan = now + timedelta(minutes=1)
            log.warning('Abgleich der bezahlten Shopaufträge wird wiederholt.')

    def consider(self, order_id):
        self._active()
        existing = self.journal.existing(order_id, mode=self.mode)
        if existing is not None and existing['state'] in {'manual_review', 'awaiting_review'}:
            reconciled = self.journal.resume_reconciliation(order_id, mode=self.mode)
            if reconciled is not None:
                return reconciled
        if existing is not None and existing['state'] not in {'queued', 'awaiting_review'}:
            return existing
        identity = self.store.lookup_payment_owner(order_id, mode=self.mode)
        if identity is None:
            return None
        order = self.store.get_order(order_id, customer_id=identity['customer_id'], mode=self.mode)
        if order['status'] != 'authorized':
            return None
        state = self._review_queue_state(order_id, identity['customer_id'])
        prepared = self.receipts.prepare(order_id, customer_id=identity['customer_id'], kind='order_received')
        return self.journal.enqueue_authorized(order_id, mode=self.mode, receipt_service=self.receipts, prepared=prepared, state=state)

    def _review_queue_state(self, order_id, customer_id):
        snapshot = self.store.snapshot_for_order(order_id, customer_id=customer_id, mode=self.mode)
        if not requires_review(snapshot):
            return 'queued'
        try:
            if self.review_service is None or self.review_service.readiness().get('ready') is not True or self.review_service.mode != self.mode:
                return 'awaiting_review'
            status = self.review_service.ensure(order_id, customer_id=customer_id)
            return 'queued' if status['state'] in {'approved', 'rejected', 'expired'} else 'awaiting_review'
        except Exception:
            return 'awaiting_review'

    def repair(self, limit=20):
        self._active()
        if type(limit) is not int or not 1 <= limit <= 50:
            raise WorkflowError('invalid_workflow_limit')
        queued = 0
        for order_id in self.journal.eligible(mode=self.mode, limit=limit):
            try:
                result = self.consider(order_id)
                if result is not None and result['state'] == 'queued':
                    queued += 1
            except Exception:
                # A missing frozen legal pack/address must not execute goods,
                # nor prevent other independently valid orders being queued.
                continue
        for order_id in self.journal.reconciliation_candidates(mode=self.mode, limit=limit):
            try:
                if self.journal.resume_reconciliation(order_id, mode=self.mode) is not None:
                    queued += 1
            except Exception:
                continue
        return queued

    def run_once(self):
        self._active()
        if not self._run_lock.acquire(blocking=False):
            return {'processed': False, 'state': 'busy'}
        try:
            self.repair()
            self._repair_workshop_orders()
            job = self.journal.claim(mode=self.mode)
            if job is None:
                self.receipts.deliver_once()
                return {'processed': False, 'state': 'idle'}
            try:
                current = self.store.get_order(job['order_id'], customer_id=job['customer_id'], mode=self.mode)
                if current['status'] not in {'authorized', 'paid', 'cancelled'}:
                    self.journal.finish(job, state='manual_review')
                    return {'processed': True, 'state': 'manual_review'}
                if current['status'] == 'authorized' and self._review_queue_state(job['order_id'], job['customer_id']) == 'awaiting_review':
                    self.journal.finish(job, state='awaiting_review')
                    return {'processed': True, 'state': 'awaiting_review'}
                # The saga fresh-checks authorization before new procurement,
                # but can reconcile a prior capture when the PI is now paid.
                result = self.fulfillment.run(job['order_id'], customer_id=job['customer_id'])
                if not isinstance(result, dict) or result.get('order_id') != job['order_id'] or result.get('mode') != self.mode:
                    raise ValueError()
                state, prepared = 'manual_review', None
                if result.get('state') == 'captured' and result.get('payment_confirmed') is True and result.get('authorization_released') is False:
                    state = 'fulfilled'
                    if self.mode == 'live' and self.workshop_order_create is not None:
                        self._ensure_workshop_order(job['order_id'], job['customer_id'])
                    prepared = self.receipts.prepare(job['order_id'], customer_id=job['customer_id'], kind='order_confirmed')
                elif result.get('state') == 'cancelled' and result.get('authorization_released') is True and result.get('payment_confirmed') is False:
                    state = 'cancelled'
                    prepared = self.receipts.prepare(job['order_id'], customer_id=job['customer_id'], kind='order_cancelled')
                elif result.get('state') == 'awaiting_review':
                    state = 'awaiting_review'
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
        elif result.get('status') == 'paid':
            self.workflow.consider_paid(order_id)
        return result

    def handle_webhook(self, raw_body, signature):
        result = self.checkout.handle_webhook(raw_body, signature)
        if result.get('ignored') is False and result.get('status') == 'authorized':
            self.workflow.consider(result['order_id'])
        elif result.get('ignored') is False and result.get('status') == 'paid':
            self.workflow.consider_paid(result['order_id'])
        return result
