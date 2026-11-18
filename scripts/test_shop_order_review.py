"""Real isolated SQLite, synthetic holds/RSU/mail; no provider or app IO."""
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import sys
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shop_order_review import (OrderReviewError, OrderReviewJournal, ShopOrderReview,
                               init_order_review_schema, requires_review)
from shop_order_store import OrderStore, init_schema
from shop_fulfillment import FulfillmentJournal, init_fulfillment_schema, FulfillmentError
from shop_order_workflow import ShopOrderWorkflow, WorkflowJournal, init_order_workflow_schema
from shop_order_receipts import ShopOrderReceipts, init_order_receipt_schema
import test_shop_fulfillment as saga_fixtures
from test_shop_fulfillment import Payment, Transport
from test_shop_cart_quotes import TYRE, RIM, line
from test_shop_complete_wheels import proposal
from test_shop_order_workflow import CONTEXT, LEGAL, FakeOutbox

EVIDENCE = {'vehicle_ref': 'CAR-1', 'certificate_references': ['SYNTHETIC-ABE-1'],
            'conditions_confirmed': True, 'full_order_confirmed': True}


class ReviewFixture:
    def __init__(self):
        self.f = f = saga_fixtures.SagaChecks('test_all_shipments_bound_once_then_full_capture_and_restart_safe')
        f.setUp()
        f.path = Path(f.temp.name) / 'review.sqlite'
        f.factory = lambda: sqlite3.connect(f.path, timeout=10)
        with closing(f.factory()) as db:
            init_schema(db)
            init_fulfillment_schema(db)
            init_order_review_schema(db)
            init_order_workflow_schema(db)
            init_order_receipt_schema(db)
        f.store = OrderStore(f.factory, clock=lambda: f.now)
        f.journal = FulfillmentJournal(f.factory, clock=lambda: f.now)
        f.context = deepcopy(CONTEXT)
        f.rows = [line(RIM, 4, group='set-1'),
                  line(TYRE, 4, services={'mounting': True}, group='set-1')]
        f.quantities = {RIM: 4, TYRE: 4}
        f.f.products[RIM]['fitment'] = None
        self.quote = self.quote_now(f.rows)
        assert self.quote.snapshot()['version'] == 2
        f.store.save_quote(self.quote.snapshot(), expected_quote_hash=self.quote.quote_hash,
                          customer_id='customer-a', mode='test', delivery_context=f.context)
        f.order = f.store.create_order(self.quote.quote_hash, customer_id='customer-a', mode='test',
                                       idempotency_key='SYNTHETIC-complete-wheel-review')
        f.attempt = f.store.claim_payment(f.order['id'], customer_id='customer-a', mode='test')
        f.store.bind_payment(f.order['id'], f.attempt['id'], customer_id='customer-a', mode='test',
            session_id='cs_test_SYNTHETIC', intent_id='pi_SYNTHETIC',
            amount_gross_cent=f.order['amount_gross_cent'], currency='eur')
        f.store.apply_verified_event(f.event('authorized', 'evt_SYNTHETIC_REVIEW_AUTH'), verified=True)
        f.payment = Payment(f)
        f.transport = Transport(f)
        self.journal = OrderReviewJournal(f.factory, clock=lambda: f.now)
        self.review = self.service()
        def fresh(snapshot, context, *, destinations):
            requests = [request for request, row in zip(snapshot['request'], snapshot['lines'])
                        if row['destination'] in destinations]
            return self.quote_now(requests).snapshot()
        f.revalidate = fresh
        f.saga = f.make_saga(review_service=self.review)
        self.outbox = FakeOutbox()
        self.receipts = ShopOrderReceipts(f.store, f.factory, mode='test', sender_address='shop@example.test',
            enabled=True, delivery_enabled=True, outbox=self.outbox, smtp_config=lambda: {'from_address': 'shop@example.test'},
            legal_snapshot=lambda oid, **kw: deepcopy(LEGAL), clock=lambda: f.now)
        self.workflow_journal = WorkflowJournal(f.factory, clock=lambda: f.now, lease_seconds=60)
        self.workflow = ShopOrderWorkflow(f.store, self.workflow_journal, f.saga, f.payment,
            mode='test', enabled=True, receipt_service=self.receipts, review_service=self.review, clock=lambda: f.now)

    def quote_now(self, rows):
        f = self.f
        return f.f.service_instance(clock=lambda: f.now, manual_mixed_orders=True,
            assembly_fitment=lambda *a: None, complete_wheel_review=True, assembly_proposal=proposal).quote(
                {'lines': rows}, customer_id='customer-a', vehicle_ref='CAR-1', delivery_context=f.context)

    def service(self, **extra):
        f = self.f
        return ShopOrderReview(f.store, self.journal, f.payment, **{'mode': 'test', 'enabled': True,
            'clock': lambda: f.now, **extra})

    def ensure(self):
        return self.review.ensure(self.f.order['id'], customer_id='customer-a')

    def approve(self, **extra):
        return self.review.approve(self.f.order['id'], **{'customer_id': 'customer-a',
            'expected_quote_hash': self.f.order['quote_hash'], 'actor_id': 'SYNTHETIC_ADMIN',
            'evidence': deepcopy(EVIDENCE), **extra})

    def raw(self):
        with closing(self.f.factory()) as db:
            db.row_factory = sqlite3.Row
            row = db.execute('SELECT * FROM shop_order_reviews_v1 WHERE order_id=?', (self.f.order['id'],)).fetchone()
            return dict(row) if row else None

    def close(self):
        self.workflow.stop()
        self.f.tearDown()


class ReviewChecks(unittest.TestCase):
    def setUp(self):
        self.x = ReviewFixture()
        self.addCleanup(self.x.close)
        self.f, self.review = self.x.f, self.x.review
        self.oid = self.f.order['id']

    def test_disabled_construction_performs_no_database_or_payment_io(self):
        calls = []
        service = ShopOrderReview(None, OrderReviewJournal(lambda: calls.append('db')), mode='test')
        self.assertFalse(service.readiness()['ready'])
        with self.assertRaises(OrderReviewError):
            service.ensure(self.oid, customer_id='customer-a')
        self.assertEqual(calls, [])

    def test_only_verified_actual_hold_can_create_pending_and_public_state_is_private(self):
        before = self.review.status(self.oid, customer_id='customer-a')
        self.assertFalse(before['authorization_verified'])
        self.assertIsNone(self.x.raw())
        result = self.x.ensure()
        self.assertEqual(result['state'], 'pending')
        self.assertEqual(self.f.payment.authorizations, 1)
        self.assertEqual(result['expires_at'], (self.f.now + timedelta(hours=3)).isoformat())
        self.assertFalse(self.f.transport.calls)
        self.assertFalse(self.f.payment.actions)
        for secret in ('pi_SYNTHETIC', 'cs_test_SYNTHETIC', 'customer-a', 'shop_order_reviews'):
            self.assertNotIn(secret, json.dumps(result))
        raw = self.x.raw()
        self.assertEqual(set(json.loads(raw['binding_json'])),
            {'order_id', 'customer_id', 'mode', 'quote_hash', 'amount_gross_cent', 'currency',
             'payment_attempt', 'session_id', 'intent_id', 'context_hash', 'cart_hash', 'requirements_hash', 'proposals_hash'})

    def test_status_and_admin_decisions_are_provider_free_and_status_does_not_write(self):
        self.x.ensure()
        raw = self.x.raw()
        with patch.object(self.f.payment, 'authorization', side_effect=AssertionError('No provider reads on status/admin')):
            self.assertEqual(self.review.status(self.oid, customer_id='customer-a')['state'], 'pending')
            self.assertEqual(self.x.raw(), raw)
            self.assertEqual(self.x.approve()['state'], 'approved')
            approved = self.x.raw()
            self.f.now += timedelta(hours=3)
            self.assertEqual(self.review.status(self.oid, customer_id='customer-a')['state'], 'expired')
            self.assertEqual(self.x.raw(), approved)
        # Explicit worker path may persist expiry, without refreshing the hold.
        self.assertEqual(self.x.ensure()['state'], 'expired')
        self.assertEqual(self.x.raw()['state'], 'expired')

    def test_forged_owner_quote_funds_context_or_hold_cannot_approve(self):
        self.x.ensure()
        for extra in ({'customer_id': 'other'}, {'expected_quote_hash': '0'*64}):
            with self.subTest(extra=extra), self.assertRaises(OrderReviewError):
                self.x.approve(**extra)
        for key, bad in (('quote_hash', '0'*64), ('intent_id', 'pi_OTHER'), ('amount_gross_cent', 1)):
            binding = {**self.f.binding(), key: bad}
            with self.subTest(key=key), self.assertRaises(OrderReviewError):
                self.review.require_approved(self.oid, customer_id='customer-a', binding=binding)
        with patch.object(self.f.store, 'delivery_context_for_order', return_value={'different_address': True}):
            with self.assertRaises(OrderReviewError):
                self.review.status(self.oid, customer_id='customer-a')
        self.assertEqual(self.x.raw()['state'], 'pending')

    def test_missing_auth_event_or_bad_live_binding_cannot_create_record(self):
        with closing(self.f.factory()) as db:
            db.execute('DELETE FROM shop_payment_events_v2')
            db.commit()
        with self.assertRaises(OrderReviewError):
            self.x.ensure()
        self.assertIsNone(self.x.raw())
        self.assertEqual(self.f.payment.authorizations, 0)

    def test_bad_gateway_hold_is_not_relabelled_verified(self):
        original = self.f.payment.authorization
        with patch.object(self.f.payment, 'authorization', side_effect=lambda oid: {**original(oid), 'mode': 'live'}):
            with self.assertRaises(OrderReviewError):
                self.x.ensure()
        self.assertIsNone(self.x.raw())

    def test_technical_evidence_is_explicit_exact_vehicle_and_does_not_mutate_catalogue(self):
        self.x.ensure()
        original = self.f.store.snapshot_for_order(self.oid, customer_id='customer-a', mode='test')
        for bad in (None, {}, {**EVIDENCE, 'vehicle_ref': 'OTHER-CAR'},
                    {**EVIDENCE, 'conditions_confirmed': False}, {**EVIDENCE, 'certificate_references': [{}]},
                    {**EVIDENCE, 'certificate_references': ['one', 'one']}):
            with self.subTest(evidence=bad), self.assertRaises(OrderReviewError):
                self.x.approve(evidence=bad)
        result = self.x.approve()
        self.assertEqual(result['review_evidence'], EVIDENCE)
        self.assertEqual(self.f.store.snapshot_for_order(self.oid, customer_id='customer-a', mode='test'), original)
        self.assertEqual(original['assembly_evidence'], {})
        self.assertIsNone(original['product_checks'][RIM]['fitment_evidence_id'])

    def test_decision_is_durable_immutable_and_replay_does_not_reopen_it(self):
        self.x.ensure()
        self.x.approve()
        self.assertEqual(self.x.approve()['state'], 'approved')
        restarted = self.x.service()
        self.assertEqual(restarted.require_approved(self.oid, customer_id='customer-a')['state'], 'approved')
        with self.assertRaises(OrderReviewError):
            self.x.approve(evidence={**EVIDENCE, 'certificate_references': ['SYNTHETIC-OTHER']})
        with self.assertRaises(OrderReviewError):
            restarted.reject(self.oid, customer_id='customer-a', expected_quote_hash=self.f.order['quote_hash'], actor_id='SYNTHETIC_ADMIN')
        with closing(self.f.factory()) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE shop_order_reviews_v1 SET expires_at='2099-01-01' WHERE order_id=?", (self.oid,))

    def test_concurrent_approve_and_reject_commit_exactly_one_decision(self):
        self.x.ensure()
        barrier, results = threading.Barrier(2), []
        def action(approve):
            barrier.wait()
            try:
                service = self.x.service()
                args = dict(customer_id='customer-a', expected_quote_hash=self.f.order['quote_hash'], actor_id='SYNTHETIC_ADMIN')
                result = service.approve(self.oid, evidence=EVIDENCE, **args) if approve else service.reject(self.oid, **args)
                results.append(result['state'])
            except OrderReviewError:
                results.append('conflict')
        threads = [threading.Thread(target=action, args=(flag,)) for flag in (True, False)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10)
        self.assertEqual(results.count('conflict'), 1)
        self.assertIn(self.x.raw()['state'], {'approved', 'rejected'})
        self.assertFalse(self.f.transport.calls)

    def test_pending_workflow_waits_durably_then_repair_after_approval_fulfills_once(self):
        job = self.x.workflow.consider(self.oid)
        self.assertEqual(job['state'], 'awaiting_review')
        self.assertEqual(self.x.workflow.run_once()['state'], 'idle')
        self.assertFalse(self.f.transport.calls)
        self.assertFalse(self.f.payment.actions)
        self.x.approve()
        result = self.x.workflow.run_once()
        self.assertEqual(result['state'], 'fulfilled')
        self.assertEqual(len(self.f.transport.calls), 2)
        self.assertEqual(self.f.payment.actions, ['capture'])
        self.assertEqual(self.x.workflow.run_once()['state'], 'idle')
        self.assertEqual(len(self.f.transport.calls), 2)

    def test_direct_saga_missing_or_disabled_review_service_cannot_bypass_gate(self):
        for service in (None, self.x.service(enabled=False), self.review):
            self.f.saga = self.f.make_saga(review_service=service)
            self.assertEqual(self.f.run_saga()['state'], 'awaiting_review')
        self.assertFalse(self.f.transport.calls)
        self.assertFalse(self.f.inquiry_calls)
        self.assertFalse(self.f.payment.actions)

    def test_directly_queued_pending_job_is_fenced_again_in_worker(self):
        self.x.workflow.consider(self.oid)
        with closing(self.f.factory()) as db:
            db.execute("UPDATE shop_order_workflow_v2 SET state='queued' WHERE order_id=?", (self.oid,))
            db.commit()
        self.assertEqual(self.x.workflow.run_once()['state'], 'awaiting_review')
        self.assertFalse(self.f.transport.calls)
        self.assertFalse(self.f.payment.actions)

    def test_changed_nominal_proposal_requires_new_approval_even_with_same_money(self):
        self.x.ensure(); self.x.approve()
        original = self.f.revalidate
        def changed(*args, **kwargs):
            value = original(*args, **kwargs)
            value['assembly_proposals']['set-1']['evidence_id'] = 'SYNTHETIC-OTHER-DIMENSIONAL-SOURCE'
            value.pop('quote_hash')
            value['quote_hash'] = sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                separators=(',', ':'), allow_nan=False).encode()).hexdigest()
            return value
        self.f.revalidate = changed
        self.f.saga = self.f.make_saga(review_service=self.review)
        self.assertEqual(self.f.run_saga()['state'], 'cancelled')
        self.assertFalse(self.f.transport.calls)
        self.assertEqual(self.f.payment.actions, ['cancel'])

    def test_rejected_or_expired_review_releases_only_unordered_authorization(self):
        self.x.ensure()
        self.review.reject(self.oid, customer_id='customer-a', expected_quote_hash=self.f.order['quote_hash'], actor_id='SYNTHETIC_ADMIN')
        self.assertEqual(self.f.run_saga()['state'], 'cancelled')
        self.assertFalse(self.f.transport.calls)
        self.assertEqual(self.f.payment.actions, ['cancel'])

    def test_expired_pending_review_is_cancelled_without_new_supplier_command(self):
        self.x.ensure()
        self.f.now += timedelta(hours=4)
        self.assertEqual(self.f.run_saga()['state'], 'cancelled')
        self.assertFalse(self.f.transport.calls)
        self.assertEqual(self.f.payment.actions, ['cancel'])

    def test_turning_review_service_off_does_not_consume_a_past_approval(self):
        self.x.ensure(); self.x.approve()
        self.review.enabled = False
        self.assertEqual(self.f.run_saga()['state'], 'awaiting_review')
        self.assertFalse(self.f.transport.calls)
        self.assertFalse(self.f.payment.actions)

    def test_expiry_before_actual_transport_grant_blocks_supplier_and_capture(self):
        self.x.ensure(); self.x.approve()
        self.f.transport.before_grant = lambda: setattr(self.f, 'now', self.f.now + timedelta(hours=4))
        self.assertEqual(self.f.run_saga()['state'], 'manual_review')
        self.assertFalse(self.f.transport.calls)
        self.assertFalse(self.f.payment.actions)
        with self.assertRaises(FulfillmentError):
            self.f.saga.proof_resolver(self.oid, operation='capture')

    def test_review_service_off_at_transport_grant_cannot_reuse_prepared_command(self):
        self.x.ensure(); self.x.approve()
        self.f.transport.before_grant = lambda: setattr(self.review, 'enabled', False)
        self.assertEqual(self.f.run_saga()['state'], 'manual_review')
        self.assertFalse(self.f.transport.calls)
        self.assertFalse(self.f.payment.actions)

    def test_expiry_after_first_acceptance_never_cancels_or_captures_partial_goods(self):
        self.x.ensure(); self.x.approve()
        self.f.transport.after_grant = lambda: setattr(self.f, 'now', self.f.now + timedelta(hours=4))
        self.assertEqual(self.f.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.f.transport.calls), 1)
        self.assertFalse(self.f.payment.actions)

    def test_capture_proof_requires_unexpired_approval_even_after_all_goods_accepted(self):
        self.x.ensure(); self.x.approve()
        def after():
            if len(self.f.transport.calls) == 2:
                self.f.now += timedelta(hours=4)
        self.f.transport.after_grant = after
        self.assertEqual(self.f.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.f.transport.calls), 2)
        self.assertFalse(self.f.payment.actions)
        with self.assertRaises(FulfillmentError):
            self.f.saga.proof_resolver(self.oid, operation='capture')

    def _reconcile_result(self, operation):
        # Model the existing PaymentActions GET-only recovery of an already
        # final signed payment; do not issue a second synthetic financial action.
        proof = self.f.saga.proof_resolver(self.oid, operation=operation)
        self.assertTrue(proof['verified'])
        return {'order_id': self.oid, 'operation': operation, 'mode': 'test', 'currency': 'eur',
            'amount_gross_cent': self.f.order['amount_gross_cent'],
            'state': 'captured' if operation == 'capture' else 'cancelled',
            'payment_confirmed': operation == 'capture', 'authorization_released': operation == 'cancel'}

    def test_capture_timeout_then_verified_paid_worker_replay_only_reconciles(self):
        self.x.workflow.consider(self.oid)
        self.x.approve()
        self.f.payment.result_state = 'unknown'
        self.assertEqual(self.x.workflow.run_once()['state'], 'manual_review')
        self.assertEqual(self.f.payment.actions, ['capture'])
        self.assertEqual(len(self.f.transport.calls), 2)
        self.f.store.apply_verified_event(self.f.event('paid', 'evt_SYNTHETIC_PAID_WEBHOOK'), verified=True)
        self.f.now += timedelta(hours=4)
        with patch.object(self.f.payment, 'capture', side_effect=lambda oid: self._reconcile_result('capture')) as reconcile:
            self.assertEqual(self.x.workflow.run_once()['state'], 'fulfilled')
            reconcile.assert_called_once_with(self.oid)
        self.assertEqual(self.f.payment.actions, ['capture'])
        self.assertEqual(len(self.f.transport.calls), 2)
        self.assertEqual(self.review.status(self.oid, customer_id='customer-a')['state'], 'approved')

    def test_cancel_timeout_then_verified_cancelled_worker_replay_only_reconciles(self):
        self.x.workflow.consider(self.oid)
        self.review.reject(self.oid, customer_id='customer-a', expected_quote_hash=self.f.order['quote_hash'], actor_id='SYNTHETIC_ADMIN')
        self.f.payment.result_state = 'unknown'
        self.assertEqual(self.x.workflow.run_once()['state'], 'manual_review')
        self.assertEqual(self.f.payment.actions, ['cancel'])
        self.f.store.apply_verified_event(self.f.event('cancelled', 'evt_SYNTHETIC_CANCEL_WEBHOOK'), verified=True)
        self.f.now += timedelta(hours=4)
        with patch.object(self.f.payment, 'cancel', side_effect=lambda oid: self._reconcile_result('cancel')) as reconcile:
            self.assertEqual(self.x.workflow.run_once()['state'], 'cancelled')
            reconcile.assert_called_once_with(self.oid)
        self.assertEqual(self.f.payment.actions, ['cancel'])
        self.assertFalse(self.f.transport.calls)

    def test_paid_order_retains_historical_approval_after_hold_deadline(self):
        self.x.ensure(); self.x.approve()
        self.assertEqual(self.f.run_saga()['state'], 'captured')
        self.f.now += timedelta(days=10)
        self.assertEqual(self.review.status(self.oid, customer_id='customer-a')['state'], 'approved')


if __name__ == '__main__':
    unittest.main()
