"""Synthetic mixed Checkout integration. Temporary SQLite; network forbidden."""
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
import json
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
from shop_checkout import CheckoutError, ShopCheckout
from shop_order_store import OrderStore, init_schema
from shop_stripe_gateway import StripeShopGateway
from test_shop_order_store import NOW, CONTEXT, quote


class FakeGateway:
    def __init__(self, mode='test'):
        self.mode, self.sessions, self.intents = mode, {}, {}
        self.created, self.reads, self.verified = [], [], []
        self.timeout, self.recover, self.ambiguous = False, False, False
        self.signed_event, self.hook = None, None

    def create_session(self, params, *, idempotency_key):
        self.created.append((deepcopy(params), idempotency_key))
        sid = 'cs_' + self.mode + '_synthetic'
        value = {'object': 'checkout.session', 'id': sid, 'livemode': self.mode == 'live',
                 'mode': 'payment', 'currency': 'eur', 'client_reference_id': params['client_reference_id'],
                 'metadata': deepcopy(params['metadata']), 'amount_total': sum(
                     row['quantity'] * row['price_data']['unit_amount'] for row in params['line_items']),
                 'expires_at': params['expires_at'], 'success_url': params['success_url'],
                 'cancel_url': params['cancel_url'], 'status': 'open', 'payment_status': 'unpaid',
                 'payment_intent': None, 'url': 'https://checkout.stripe.com/c/pay/' + sid + '#synthetic'}
        self.sessions[sid] = value
        stale = deepcopy(value)
        if self.hook:
            self.hook(value)
        if self.timeout:
            raise RuntimeError('PRIVATE_PROVIDER_RESPONSE_CANARY')
        return stale

    def retrieve_session(self, identifier):
        self.reads.append(('session', identifier))
        return deepcopy(self.sessions[identifier])

    def retrieve_intent(self, identifier):
        self.reads.append(('intent', identifier))
        return deepcopy(self.intents[identifier])

    def find_session(self, expected):
        self.reads.append(('find', deepcopy(expected)))
        values = list(self.sessions.values())
        if self.ambiguous:
            return deepcopy(values * 2)
        return deepcopy(values[0]) if values and self.recover else None

    def verify_webhook(self, raw, signature):
        self.verified.append((raw, signature))
        if signature != 'synthetic-verified' or self.signed_event is None:
            raise RuntimeError('PRIVATE_SIGNATURE_CANARY')
        return deepcopy(self.signed_event)

    def complete(self, state='requires_capture'):
        session = next(iter(self.sessions.values()))
        amount = session['amount_total']
        intent = {'object': 'payment_intent', 'id': 'pi_synthetic', 'livemode': self.mode == 'live',
                  'currency': 'eur', 'amount': amount, 'capture_method': 'manual',
                  'metadata': deepcopy(session['metadata']), 'status': state,
                  'amount_capturable': amount if state == 'requires_capture' else 0,
                  'amount_received': amount if state == 'succeeded' else 0}
        self.intents[intent['id']] = intent
        session.update(status='complete', payment_status='paid', payment_intent=intent['id'], url=None)
        return session, intent

    def event(self, obj, event_type, *, created=NOW, identifier='evt_synthetic'):
        self.signed_event = {'object': 'event', 'id': identifier, 'livemode': self.mode == 'live',
                             'created': int(created.timestamp()), 'type': event_type,
                             'data': {'object': deepcopy(obj)}}
        return deepcopy(self.signed_event)


class CheckoutChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.temp = tempfile.TemporaryDirectory()
        path = Path(self.temp.name) / 'synthetic.sqlite'
        self.factory = lambda: sqlite3.connect(path, timeout=5)
        with closing(self.factory()) as db:
            init_schema(db)
        self.now = NOW
        self.store = OrderStore(self.factory, clock=lambda: self.now)
        self.snapshot = quote()
        self.store.save_quote(self.snapshot, expected_quote_hash=self.snapshot['quote_hash'],
            customer_id='customer-a', mode='test', delivery_context=CONTEXT)
        self.order = self.store.create_order(self.snapshot['quote_hash'], customer_id='customer-a',
                                             mode='test', idempotency_key='synthetic-checkout-1')
        self.gateway = FakeGateway()
        self.checkout = self.service()

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()

    def service(self, **changes):
        params = dict(mode='test', public_origin='https://shop.example.test', enabled=True,
                      order_lookup=lambda oid: self.store.lookup_payment_owner(oid, mode='test'), clock=lambda: self.now)
        params.update(changes)
        return ShopCheckout(self.store, self.gateway, **params)

    def start(self, service=None):
        return (service or self.checkout).start(self.order['id'], customer_id='customer-a')

    def count_events(self):
        with closing(self.factory()) as db:
            return db.execute('SELECT COUNT(*) FROM shop_payment_events_v2').fetchone()[0]

    def test_disabled_default_and_bad_mode_do_not_create_or_contact_gateway(self):
        service = ShopCheckout(self.store, self.gateway, mode='test', public_origin='https://shop.example.test')
        self.assertFalse(service.readiness()['ready'])
        with self.assertRaisesRegex(CheckoutError, 'checkout_disabled'):
            self.start(service)
        with self.assertRaisesRegex(CheckoutError, 'checkout_disabled'):
            service.handle_webhook(b'{}', 'synthetic-verified')
        self.gateway.mode = 'live'
        with self.assertRaisesRegex(CheckoutError, 'checkout_unavailable'):
            self.start()
        self.assertEqual(self.gateway.created, [])
        self.assertEqual(self.gateway.verified, [])

    def test_frozen_mixed_cart_produces_exact_manual_card_checkout_and_config_return_urls(self):
        result = self.start()
        params, key = self.gateway.created[0]
        self.assertEqual(params['payment_method_types'], ['card'])
        self.assertEqual(params['payment_intent_data']['capture_method'], 'manual')
        self.assertEqual(params['metadata'], params['payment_intent_data']['metadata'])
        self.assertEqual(sum(row['quantity'] * row['price_data']['unit_amount'] for row in params['line_items']), 53200)
        self.assertEqual(len([r for r in params['line_items'] if r['price_data']['product_data']['name'].startswith('Montage')]), 1)
        self.assertEqual(params['expires_at'], int(NOW.timestamp()) + 3600)
        self.assertGreater(params['expires_at'], int(__import__('datetime').datetime.fromisoformat(self.snapshot['expires_at']).timestamp()))
        self.assertTrue(params['success_url'].startswith('https://shop.example.test/shop/orders/' + self.order['id']))
        self.assertEqual(key, 'shop-checkout-v1:' + self.order['id'])
        self.assertEqual(result['payment_state'], 'pending')
        self.assertIsNotNone(result['checkout_url'])
        self.assertFalse(result['payment_confirmed'])
        self.assertFalse(result['capture_triggered'])
        self.assertFalse(result['supplier_order_created'])
        self.assertNotIn('mapping', json.dumps(params))

    def test_duplicate_start_reuses_existing_session_and_never_posts_again(self):
        first = self.start()
        again = self.start()
        self.assertEqual(first['session_id'], again['session_id'])
        self.assertEqual(len(self.gateway.created), 1)

    def test_unknown_creation_zero_or_ambiguous_lookup_never_allows_a_new_post(self):
        self.gateway.timeout = True
        with self.assertRaisesRegex(CheckoutError, '^checkout_reconciliation_required$'):
            self.start()
        self.gateway.timeout = False
        for ambiguous in (False, True):
            self.gateway.ambiguous = ambiguous
            with self.assertRaisesRegex(CheckoutError, 'checkout_reconciliation_required'):
                self.start()
        self.gateway.ambiguous, self.gateway.recover = False, True
        self.assertEqual(self.start()['session_id'], 'cs_test_synthetic')
        self.assertEqual(len(self.gateway.created), 1)

    def test_foreign_redirect_or_wrong_mode_response_is_unknown_not_a_second_charge(self):
        self.gateway.hook = lambda value: value.update(url='https://attacker.invalid/pay')
        # The first response deliberately stays its original copy; inspect a bad
        # recovered URL after the response was lost instead.
        self.gateway.timeout = True
        with self.assertRaises(CheckoutError):
            self.start()
        self.gateway.timeout, self.gateway.recover = False, True
        with self.assertRaisesRegex(CheckoutError, 'checkout_url_rejected'):
            self.start()
        self.assertEqual(len(self.gateway.created), 1)

    def test_expiry_is_rechecked_after_claim_immediately_before_first_post(self):
        claim = self.store.claim_payment
        def delayed(*args, **kwargs):
            result = claim(*args, **kwargs)
            self.now += timedelta(minutes=20)
            return result
        with patch.object(self.store, 'claim_payment', delayed):
            with self.assertRaisesRegex(CheckoutError, 'quote_expired'):
                self.start()
        self.assertEqual(len(self.gateway.created), 0)

    def test_browser_status_reads_cannot_accept_payment_or_run_provider_requests(self):
        before = self.checkout.status(self.order['id'], customer_id='customer-a')
        self.assertEqual(before['status'], 'draft')
        self.assertFalse(before['payment_confirmed'])
        self.assertIsNone(before['checkout_url'])
        self.assertEqual(self.gateway.created, [])
        self.assertEqual(self.gateway.reads, [])
        with self.assertRaises(CheckoutError):
            self.checkout.status(self.order['id'], customer_id='customer-b')

    def test_session_completion_with_manual_hold_is_authorized_not_paid_and_replay_is_safe(self):
        self.start()
        session, _ = self.gateway.complete()
        self.gateway.event(session, 'checkout.session.completed')
        first = self.checkout.handle_webhook(b'synthetic-body', 'synthetic-verified')
        again = self.checkout.handle_webhook(b'synthetic-body', 'synthetic-verified')
        self.assertEqual(first['status'], 'authorized')
        self.assertEqual(again['status'], 'authorized')
        self.assertEqual(self.count_events(), 1)
        result = self.start()
        self.assertTrue(result['authorization_confirmed'])
        self.assertFalse(result['payment_confirmed'])
        self.assertIsNone(result['checkout_url'])
        self.assertEqual(len(self.gateway.created), 1)

    def test_replayed_immutable_intent_event_after_capture_does_not_change_its_fingerprint(self):
        self.start()
        _, intent = self.gateway.complete()
        immutable = self.gateway.event(intent, 'payment_intent.amount_capturable_updated')
        self.assertEqual(self.checkout.handle_webhook(b'auth', 'synthetic-verified')['status'], 'authorized')
        self.now += timedelta(seconds=30)
        self.gateway.complete('succeeded')
        self.gateway.signed_event = immutable
        self.assertEqual(self.checkout.handle_webhook(b'auth', 'synthetic-verified')['status'], 'paid')
        self.assertEqual(self.checkout.handle_webhook(b'auth', 'synthetic-verified')['status'], 'paid')
        self.assertEqual(self.count_events(), 2)
        self.assertTrue(self.start()['payment_confirmed'])

    def test_signature_wrong_mode_and_other_integration_are_checked_before_reconciliation(self):
        self.start()
        session, _ = self.gateway.complete()
        self.gateway.event(session, 'checkout.session.completed')
        with self.assertRaisesRegex(CheckoutError, 'invalid_webhook'):
            self.checkout.handle_webhook(b'body', 'unsigned')
        self.gateway.signed_event['livemode'] = True
        with self.assertRaisesRegex(CheckoutError, 'invalid_webhook'):
            self.checkout.handle_webhook(b'body', 'synthetic-verified')
        self.gateway.signed_event['livemode'] = False
        self.gateway.signed_event['data']['object']['metadata']['integration'] = 'other-product'
        self.assertTrue(self.checkout.handle_webhook(b'body', 'synthetic-verified')['ignored'])
        self.assertEqual(self.gateway.reads, [])

    def test_different_intent_or_partial_capture_cannot_bind_or_become_paid(self):
        self.start()
        _, intent = self.gateway.complete()
        wrong = deepcopy(intent)
        wrong['id'] = 'pi_other'
        self.gateway.event(wrong, 'payment_intent.amount_capturable_updated')
        with self.assertRaisesRegex(CheckoutError, 'payment_binding_mismatch'):
            self.checkout.handle_webhook(b'body', 'synthetic-verified')
        self.gateway.intents['pi_synthetic'].update(status='succeeded', amount_capturable=0, amount_received=1)
        with self.assertRaisesRegex(CheckoutError, 'payment_state_unconfirmed'):
            self.checkout.reconcile(self.order['id'], customer_id='customer-a')
        self.assertEqual(self.count_events(), 0)

    def test_signed_event_type_and_object_state_must_agree_before_status_writes(self):
        self.start()
        _, authorized = self.gateway.complete()
        for event_type in ('payment_intent.succeeded', 'payment_intent.canceled', 'payment_intent.payment_failed'):
            with self.subTest(event_type=event_type):
                self.gateway.event(authorized, event_type)
                with self.assertRaisesRegex(CheckoutError, 'payment_state_unconfirmed'):
                    self.checkout.handle_webhook(b'contradictory-type', 'synthetic-verified')
        paid = deepcopy(authorized)
        paid.update(status='succeeded', amount_capturable=0, amount_received=paid['amount'])
        for event_type in ('payment_intent.canceled', 'payment_intent.succeeded'):
            with self.subTest(event_type=event_type):
                self.gateway.event(paid, event_type)
                with self.assertRaisesRegex(CheckoutError, 'payment_state_unconfirmed'):
                    self.checkout.handle_webhook(b'contradictory-paid', 'synthetic-verified')
        self.assertEqual(self.count_events(), 0)
        self.assertEqual(self.checkout.status(self.order['id'], customer_id='customer-a')['status'], 'payment_pending')

    def test_expired_open_session_without_intent_closes_and_cannot_restart(self):
        self.start()
        session = self.gateway.sessions['cs_test_synthetic']
        session.update(status='expired', url=None)
        self.gateway.event(session, 'checkout.session.expired')
        self.assertEqual(self.checkout.handle_webhook(b'expired', 'synthetic-verified')['status'], 'cancelled')
        self.assertEqual(self.start()['status'], 'cancelled')
        self.assertIsNone(self.start()['checkout_url'])
        self.assertEqual(len(self.gateway.created), 1)

    def test_session_expiry_hint_cannot_erase_current_manual_authorization(self):
        self.start()
        session, _ = self.gateway.complete()
        stale = deepcopy(session)
        stale.update(status='expired', payment_status='unpaid', url=None)
        self.gateway.event(stale, 'checkout.session.expired')
        result = self.checkout.handle_webhook(b'stale-expiry', 'synthetic-verified')
        self.assertEqual(result['status'], 'authorized')
        self.assertEqual(self.checkout.handle_webhook(b'stale-expiry', 'synthetic-verified')['status'], 'authorized')
        self.assertEqual(self.count_events(), 1)
        self.assertTrue(self.start()['authorization_confirmed'])
        self.assertIsNone(self.start()['checkout_url'])

    def test_session_expiry_with_processing_intent_remains_pending_until_authorization(self):
        self.start()
        session, _ = self.gateway.complete('processing')
        session.update(status='expired', payment_status='unpaid')
        self.gateway.event(session, 'checkout.session.expired')
        result = self.checkout.handle_webhook(b'pending-expiry', 'synthetic-verified')
        self.assertEqual(result['status'], 'payment_pending')
        self.assertEqual(self.count_events(), 0)
        self.now += timedelta(seconds=30)
        self.gateway.complete()
        result = self.checkout.reconcile(self.order['id'], customer_id='customer-a')
        self.assertEqual(result['status'], 'authorized')
        self.assertTrue(result['authorization_confirmed'])
        self.assertEqual(len(self.gateway.created), 1)

    def test_delayed_older_failed_get_cannot_supersede_newer_authorization(self):
        self.start()
        _, intent = self.gateway.complete('requires_payment_method')
        intent['last_payment_error'] = {'code': 'card_declined'}
        blocked, resume, outcomes = threading.Event(), threading.Event(), []
        retrieve = self.gateway.retrieve_intent

        def delayed(identifier):
            value = retrieve(identifier)
            if threading.current_thread().name == 'old-payment-read':
                blocked.set()
                if not resume.wait(5):
                    raise AssertionError('Synthetic read did not resume')
            return value

        def reconcile_old():
            try:
                outcomes.append(self.checkout.reconcile(self.order['id'], customer_id='customer-a'))
            except Exception as error:
                outcomes.append(error)

        with patch.object(self.gateway, 'retrieve_intent', delayed):
            worker = threading.Thread(target=reconcile_old, name='old-payment-read')
            worker.start()
            try:
                self.assertTrue(blocked.wait(5))
                self.now += timedelta(seconds=30)
                self.gateway.complete()
                newer = self.checkout.reconcile(self.order['id'], customer_id='customer-a')
                self.assertEqual(newer['status'], 'authorized')
                self.now += timedelta(seconds=30)
            finally:
                resume.set()
                worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], dict)
        self.assertEqual(outcomes[0]['status'], 'authorized')
        self.assertEqual(self.checkout.status(self.order['id'], customer_id='customer-a')['status'], 'authorized')

    def test_signed_session_can_recover_lost_creation_response_without_finder_or_new_post(self):
        self.gateway.timeout = True
        with self.assertRaises(CheckoutError):
            self.start()
        session, _ = self.gateway.complete()
        self.gateway.event(session, 'checkout.session.completed')
        self.assertEqual(self.checkout.handle_webhook(b'recovered', 'synthetic-verified')['status'], 'authorized')
        self.assertEqual(len(self.gateway.created), 1)

    def test_concurrent_webhook_status_prevents_returning_a_stale_open_checkout_url(self):
        def paid_before_response(_):
            session, intent = self.gateway.complete()
            self.gateway.event(session, 'checkout.session.completed')
            self.checkout.handle_webhook(b'early-webhook', 'synthetic-verified')
        self.gateway.hook = paid_before_response
        result = self.start()
        self.assertEqual(result['status'], 'authorized')
        self.assertIsNone(result['checkout_url'])
        self.assertEqual(len(self.gateway.created), 1)

    def test_return_origins_and_missing_owner_lookup_are_not_implicitly_accepted(self):
        for origin in ('http://shop.example.test', 'https://attacker@shop.example.test',
                       'https://shop.example.test:444', 'https://shop.example.test/path',
                       'https://shop.example.test/?next=evil'):
            with self.subTest(origin=origin), self.assertRaisesRegex(CheckoutError, 'invalid_configuration'):
                self.service(public_origin=origin)
        with self.assertRaisesRegex(CheckoutError, 'checkout_unavailable'):
            self.start(self.service(order_lookup=None))

    def test_actual_stripe_transport_whitelist_accepts_the_frozen_mixed_checkout_parameters(self):
        fake = self.gateway
        sessions = SimpleNamespace(create=lambda params, options: fake.create_session(params, idempotency_key=options['idempotency_key']),
                                   retrieve=fake.retrieve_session)
        client = SimpleNamespace(v1=SimpleNamespace(checkout=SimpleNamespace(sessions=sessions),
                                                   payment_intents=SimpleNamespace(retrieve=fake.retrieve_intent)))
        sdk = SimpleNamespace(Webhook=SimpleNamespace(construct_event=lambda raw, sig, secret, tolerance: fake.verify_webhook(raw, sig)))
        gateway = StripeShopGateway({'SHOP_STRIPE_MODE': 'test', 'SHOP_STRIPE_SECRET_KEY': 'sk_test_SYNTHETIC_NOT_REAL_1234',
                    'SHOP_STRIPE_WEBHOOK_SECRET': 'whsec_SYNTHETIC_NOT_REAL_1234',
                    'SHOP_STRIPE_PUBLIC_ORIGIN': 'https://shop.example.test'}, client=client, sdk=sdk, clock=lambda: self.now)
        service = self.service()
        service.gateway = gateway
        self.assertIsNotNone(self.start(service)['checkout_url'])
        self.assertEqual(len(fake.created), 1)


if __name__ == '__main__':
    unittest.main()
