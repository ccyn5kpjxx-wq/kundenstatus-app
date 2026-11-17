"""Synthetic action SDK; real temporary durable stores; all network forbidden."""
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shop_checkout import ShopCheckout
from shop_order_store import OrderStore, init_schema
from shop_payment_actions import PaymentActionError, PaymentActionJournal, PaymentActions, init_payment_action_schema
from test_shop_checkout import FakeGateway
from test_shop_order_store import NOW, CONTEXT, quote


class PaymentActionsChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'synthetic-actions.sqlite'
        self.factory = lambda: sqlite3.connect(self.path, timeout=5)
        with closing(self.factory()) as db:
            init_schema(db)
            init_payment_action_schema(db)
        self.now = NOW
        self.store = OrderStore(self.factory, clock=lambda: self.now)
        self.snapshot = quote()
        self.store.save_quote(self.snapshot, expected_quote_hash=self.snapshot['quote_hash'], customer_id='customer-a',
                              mode='test', delivery_context=CONTEXT)
        self.order = self.store.create_order(self.snapshot['quote_hash'], customer_id='customer-a',
                                             mode='test', idempotency_key='synthetic-payment-actions')
        self.gateway = FakeGateway()
        self.gateway.public_origin = 'https://shop.example.test'
        self.posts, self.charge_reads, self.proof_reads = [], [], []
        self.timeout, self.before_timeout, self.bad_response = False, False, False
        self.charges = {}
        client_intents = SimpleNamespace(capture=self.capture_sdk, cancel=self.cancel_sdk)
        self.gateway.client = SimpleNamespace(v1=SimpleNamespace(payment_intents=client_intents,
                                                                charges=SimpleNamespace(retrieve=self.charge_sdk)))
        checkout = ShopCheckout(self.store, self.gateway, mode='test', public_origin=self.gateway.public_origin,
                                enabled=True, clock=lambda: self.now,
                                order_lookup=lambda oid: self.store.lookup_payment_owner(oid, mode='test'))
        checkout.start(self.order['id'], customer_id='customer-a')
        session, intent = self.gateway.complete()
        intent.update(payment_method_types=['card'], latest_charge='ch_synthetic')
        self.gateway.event(session, 'checkout.session.completed')
        checkout.handle_webhook(b'synthetic-auth', 'synthetic-verified')
        self.charges['ch_synthetic'] = {'id': 'ch_synthetic', 'object': 'charge', 'livemode': False,
                'payment_intent': intent['id'], 'currency': 'eur', 'amount': intent['amount'],
                'paid': True, 'captured': False, 'amount_captured': 0, 'refunded': False, 'amount_refunded': 0,
                'payment_method_details': {'type': 'card', 'card': {'capture_before': int((NOW + timedelta(days=5)).timestamp())}}}
        self.journal = PaymentActionJournal(self.factory, clock=lambda: self.now)
        self.proofs = {}
        attempt = self.store.claim_payment(self.order['id'], customer_id='customer-a', mode='test')
        self.binding = {'order_id': self.order['id'], 'customer_id': 'customer-a', 'mode': 'test', 'currency': 'eur',
                'amount_gross_cent': self.order['amount_gross_cent'], 'quote_hash': self.order['quote_hash'],
                'payment_attempt': attempt['id'], 'session_id': attempt['session_id'], 'intent_id': attempt['intent_id']}
        common = {key: value for key, value in self.binding.items() if key != 'customer_id'}
        self.proofs['capture'] = {**common, 'proof_id': 'sfp_syntheticcapture', 'verified': True, 'persisted': True,
                'all_goods_accepted': True, 'accepted_lines': [{'id': line['id'], 'quantity': line['quantity']} for line in self.snapshot['lines']]}
        self.proofs['cancel'] = {**common, 'proof_id': 'sfp_syntheticrejection', 'verified': True, 'persisted': True,
                'all_goods_accepted': False, 'cancel_allowed': True}
        self.actions = self.service(enabled=True)

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()

    def service(self, **kwargs):
        return PaymentActions(self.store, self.gateway, self.journal, self.resolve_proof,
                              mode='test', clock=lambda: self.now, **kwargs)

    def resolve_proof(self, order_id, *, operation):
        self.proof_reads.append((order_id, operation))
        return deepcopy(self.proofs.get(operation))

    def charge_sdk(self, identifier):
        self.charge_reads.append(identifier)
        return deepcopy(self.charges[identifier])

    def capture_sdk(self, identifier, params, *, options):
        self.posts.append(('capture', identifier, deepcopy(params), deepcopy(options)))
        if self.before_timeout:
            raise TimeoutError('PRIVATE_PROVIDER_CANARY')
        intent = self.gateway.intents[identifier]
        intent.update(status='succeeded', amount_capturable=0, amount_received=intent['amount'])
        if self.timeout:
            raise TimeoutError('PRIVATE_PROVIDER_CANARY')
        response = deepcopy(intent)
        if self.bad_response:
            response['amount_received'] = 1
        return response

    def cancel_sdk(self, identifier, params, *, options):
        self.posts.append(('cancel', identifier, deepcopy(params), deepcopy(options)))
        if self.before_timeout:
            raise TimeoutError('PRIVATE_PROVIDER_CANARY')
        intent = self.gateway.intents[identifier]
        intent.update(status='canceled', amount_capturable=0, amount_received=0)
        if self.timeout:
            raise TimeoutError('PRIVATE_PROVIDER_CANARY')
        return deepcopy(intent)

    def status(self):
        return self.store.get_order(self.order['id'], customer_id='customer-a', mode='test')['status']

    def test_default_disabled_never_reads_or_posts_or_resolves_proof(self):
        self.gateway.reads.clear()
        service = self.service()
        self.assertFalse(service.readiness()['ready'])
        for method in ('authorization', 'capture', 'cancel', 'reconcile'):
            with self.subTest(method=method), self.assertRaisesRegex(PaymentActionError, 'payment_actions_disabled'):
                getattr(service, method)(self.order['id'])
        self.assertEqual(self.posts + self.charge_reads + self.proof_reads + self.gateway.reads, [])

    def test_authorization_is_read_only_and_returns_actual_card_capture_deadline(self):
        result = self.actions.authorization(self.order['id'])
        for key, value in self.binding.items():
            self.assertEqual(result[key], value)
        self.assertTrue(result['verified'])
        self.assertEqual(result['state'], 'authorized')
        self.assertEqual(result['authorized_until'], (NOW + timedelta(days=5)).isoformat())
        self.assertEqual(self.posts + self.proof_reads, [])
        self.assertIsNone(self.journal.lookup(self.order['id'], mode='test'))

    def test_full_capture_exact_frozen_amount_and_proof_uses_one_durable_post(self):
        result = self.actions.capture(self.order['id'])
        self.assertEqual(result['state'], 'captured')
        self.assertTrue(result['payment_confirmed'])
        self.assertFalse(result['authorization_released'])
        self.assertEqual(self.status(), 'paid')
        self.assertEqual(self.posts[0][2], {'amount_to_capture': self.order['amount_gross_cent']})
        self.assertNotIn('final_capture', self.posts[0][2])
        self.assertTrue(self.posts[0][3]['idempotency_key'].startswith('shop-capture-v1:'))
        self.assertEqual(self.actions.capture(self.order['id'])['state'], 'captured')
        self.assertEqual(len(self.posts), 1)

    def test_capture_requires_exact_persisted_all_goods_acceptance_and_amount(self):
        for mutation in ('partial', 'not-persisted', 'not-verified', 'wrong-amount', 'duplicate-lines', 'extra-id'):
            original = deepcopy(self.proofs['capture'])
            value = self.proofs['capture']
            if mutation == 'partial':
                value['accepted_lines'].pop()
            elif mutation == 'not-persisted':
                value['persisted'] = False
            elif mutation == 'not-verified':
                value['verified'] = False
            elif mutation == 'wrong-amount':
                value['amount_gross_cent'] += 1
            elif mutation == 'duplicate-lines':
                value['accepted_lines'].append(deepcopy(value['accepted_lines'][0]))
            else:
                value['accepted_lines'][0]['id'] = 'parts:' + 'f' * 24
            with self.subTest(mutation=mutation), self.assertRaisesRegex(PaymentActionError, 'fulfillment_unconfirmed'):
                self.actions.capture(self.order['id'])
            self.proofs['capture'] = original
        self.assertEqual(self.posts, [])
        self.assertIsNone(self.journal.lookup(self.order['id'], mode='test'))

    def test_wrong_mode_customer_order_pi_and_manual_binding_fail_before_post(self):
        self.gateway.mode = 'live'
        with self.assertRaisesRegex(PaymentActionError, 'payment_action_unavailable'):
            self.actions.capture(self.order['id'])
        self.gateway.mode = 'test'
        for key, bad in (('currency', 'usd'), ('capture_method', 'automatic'), ('livemode', True),
                         ('amount', self.order['amount_gross_cent'] + 1), ('payment_method_types', ['card', 'klarna'])):
            intent = self.gateway.intents['pi_synthetic']
            original = deepcopy(intent[key])
            intent[key] = bad
            with self.subTest(key=key), self.assertRaisesRegex(PaymentActionError, 'payment_state_unconfirmed'):
                self.actions.capture(self.order['id'])
            intent[key] = original
        with self.assertRaisesRegex(PaymentActionError, 'payment_binding_mismatch'):
            self.actions.capture('sho_' + 'f' * 32)
        self.assertEqual(self.posts, [])

    def test_expired_or_mismatched_charge_prevents_capture(self):
        charge = self.charges['ch_synthetic']
        for key, bad in (('payment_intent', 'pi_other'), ('amount_captured', 1), ('captured', True), ('livemode', True)):
            original = deepcopy(charge[key])
            charge[key] = bad
            with self.subTest(key=key), self.assertRaisesRegex(PaymentActionError, 'authorization_unconfirmed'):
                self.actions.capture(self.order['id'])
            charge[key] = original
        charge['payment_method_details']['card']['capture_before'] = int(NOW.timestamp())
        with self.assertRaisesRegex(PaymentActionError, 'authorization_unconfirmed'):
            self.actions.authorization(self.order['id'])
        with self.assertRaisesRegex(PaymentActionError, 'authorization_unconfirmed'):
            self.actions.capture(self.order['id'])
        self.assertEqual(self.posts, [])

    def test_unknown_capture_response_recovers_by_get_with_no_second_post_after_restart(self):
        self.timeout = True
        with self.assertRaisesRegex(PaymentActionError, '^payment_action_reconciliation_required$'):
            self.actions.capture(self.order['id'])
        self.assertEqual(self.journal.lookup(self.order['id'], mode='test')['state'], 'unknown')
        self.journal = PaymentActionJournal(self.factory, clock=lambda: self.now)
        restarted = self.service(enabled=True)
        self.assertEqual(restarted.capture(self.order['id'])['state'], 'captured')
        self.assertEqual(len(self.posts), 1)
        self.assertEqual(self.status(), 'paid')

    def test_unknown_pre_send_outcome_never_retries_even_if_authorization_still_open(self):
        self.before_timeout = True
        with self.assertRaisesRegex(PaymentActionError, 'payment_action_reconciliation_required'):
            self.actions.capture(self.order['id'])
        self.before_timeout = False
        self.assertEqual(self.actions.capture(self.order['id'])['state'], 'unknown')
        self.assertEqual(self.actions.reconcile(self.order['id'])['state'], 'unknown')
        with self.assertRaisesRegex(PaymentActionError, 'payment_action_conflict'):
            self.actions.cancel(self.order['id'])
        self.assertEqual(len(self.posts), 1)

    def test_crash_after_durable_claim_and_deadline_expiry_before_post_never_send_again(self):
        claim = self.journal.claim
        def delayed(*args):
            action = claim(*args)
            self.now += timedelta(days=6)
            return action
        with patch.object(self.journal, 'claim', delayed):
            with self.assertRaisesRegex(PaymentActionError, 'payment_action_reconciliation_required'):
                self.actions.capture(self.order['id'])
        self.assertEqual(self.posts, [])
        self.assertEqual(self.actions.capture(self.order['id'])['state'], 'unknown')
        self.assertEqual(self.posts, [])

    def test_malformed_capture_response_is_unknown_until_current_full_amount_confirmed(self):
        self.bad_response = True
        with self.assertRaisesRegex(PaymentActionError, 'payment_action_reconciliation_required'):
            self.actions.capture(self.order['id'])
        self.assertEqual(self.status(), 'authorized')
        self.assertEqual(self.actions.reconcile(self.order['id'])['state'], 'captured')
        self.assertEqual(len(self.posts), 1)

    def test_cancel_releases_only_an_uncaptured_authorization_and_never_calls_refund(self):
        result = self.actions.cancel(self.order['id'])
        self.assertEqual(result['state'], 'cancelled')
        self.assertTrue(result['authorization_released'])
        self.assertFalse(result['payment_confirmed'])
        self.assertEqual(self.status(), 'cancelled')
        self.assertEqual(self.posts[0][2], {'cancellation_reason': 'abandoned'})
        self.assertEqual(self.actions.cancel(self.order['id'])['state'], 'cancelled')
        self.assertEqual(len(self.posts), 1)
        with self.assertRaisesRegex(PaymentActionError, 'payment_action_conflict'):
            self.actions.capture(self.order['id'])

    def test_cancel_timeout_after_release_is_reconciled_without_second_post(self):
        self.timeout = True
        with self.assertRaisesRegex(PaymentActionError, 'payment_action_reconciliation_required'):
            self.actions.cancel(self.order['id'])
        self.assertEqual(self.actions.reconcile(self.order['id'])['state'], 'cancelled')
        self.assertEqual(len(self.posts), 1)

    def test_captured_or_partly_captured_money_cannot_be_cancelled_as_a_refund(self):
        intent = self.gateway.intents['pi_synthetic']
        intent.update(status='succeeded', amount_capturable=0, amount_received=intent['amount'])
        with self.assertRaisesRegex(PaymentActionError, 'captured_payment_requires_refund'):
            self.actions.cancel(self.order['id'])
        intent['amount_received'] = 1
        with self.assertRaisesRegex(PaymentActionError, 'payment_state_unconfirmed'):
            self.actions.cancel(self.order['id'])
        self.assertEqual(self.posts, [])

    def test_unknown_or_partial_supplier_outcome_never_allows_cancellation(self):
        self.proofs['cancel']['cancel_allowed'] = False
        with self.assertRaisesRegex(PaymentActionError, 'fulfillment_unconfirmed'):
            self.actions.cancel(self.order['id'])
        self.proofs['cancel'].update(cancel_allowed=True, all_goods_accepted=True)
        with self.assertRaisesRegex(PaymentActionError, 'fulfillment_unconfirmed'):
            self.actions.cancel(self.order['id'])
        self.assertEqual(self.posts, [])

    def test_concurrent_capture_and_cancel_claims_have_one_winner(self):
        barrier, outcomes = threading.Barrier(2), []
        def claim(operation):
            barrier.wait(5)
            try:
                outcomes.append(self.journal.claim(self.binding, operation, self.proofs[operation]))
            except PaymentActionError as error:
                outcomes.append(error.code)
        workers = [threading.Thread(target=claim, args=(operation,)) for operation in ('capture', 'cancel')]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(5)
            self.assertFalse(worker.is_alive())
        self.assertEqual(sum(isinstance(value, dict) and value['can_create'] for value in outcomes), 1)
        self.assertIn('payment_action_conflict', outcomes)
        self.assertEqual(self.posts, [])

    def test_action_binding_amount_proof_and_operation_are_immutable(self):
        action = self.journal.claim(self.binding, 'capture', self.proofs['capture'])
        self.assertFalse(self.journal.claim(self.binding, 'capture', self.proofs['capture'])['can_create'])
        for mutation in ('amount', 'proof', 'operation'):
            binding, proof, operation = deepcopy(self.binding), deepcopy(self.proofs['capture']), 'capture'
            if mutation == 'amount':
                binding['amount_gross_cent'] += 1
            elif mutation == 'proof':
                proof['proof_id'] += 'changed'
            else:
                operation = 'cancel'
            with self.subTest(mutation=mutation), self.assertRaisesRegex(PaymentActionError, 'payment_action_conflict'):
                self.journal.claim(binding, operation, proof)
        with closing(self.factory()) as connection, self.assertRaises(sqlite3.IntegrityError):
            connection.execute("UPDATE shop_payment_actions_v2 SET operation='cancel' WHERE id=?", (action['id'],))

    def test_quote_expiry_does_not_replace_actual_card_deadline_or_allow_unproven_capture(self):
        self.now += timedelta(hours=2)
        self.assertEqual(self.actions.authorization(self.order['id'])['state'], 'authorized')
        self.proofs['capture']['all_goods_accepted'] = False
        with self.assertRaisesRegex(PaymentActionError, 'fulfillment_unconfirmed'):
            self.actions.capture(self.order['id'])
        self.assertEqual(self.posts, [])


if __name__ == '__main__':
    unittest.main()
