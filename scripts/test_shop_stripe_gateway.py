"""Stripe SDK transport checks using fakes and real synthetic HMACs, no IO."""
from copy import deepcopy
import hashlib
import hmac
import json
from pathlib import Path
import socket
import sys
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import stripe
from shop_stripe_gateway import StripeShopGateway, StripeShopError


class StripeGatewayChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.client = Mock()
        self.now = int(time.time())
        self.config = {'SHOP_STRIPE_MODE': 'test', 'SHOP_STRIPE_SECRET_KEY': 'rk_test_SYNTHETIC_ONLY',
                       'SHOP_STRIPE_WEBHOOK_SECRET': 'whsec_SYNTHETIC_ONLY',
                       'SHOP_STRIPE_PUBLIC_ORIGIN': 'https://shop.example.test'}
        self.gateway = StripeShopGateway(self.config, client=self.client, clock=lambda: self.now)
        self.meta = {'integration': 'gaertner_shop_v2', 'order_id': 'sho_' + 'a' * 32,
                     'quote_hash': 'b' * 64, 'payment_attempt': 'spa_' + 'c' * 32, 'capture_method': 'manual'}
        self.params = {'mode': 'payment', 'ui_mode': 'hosted_page', 'payment_method_types': ['card'],
                       'client_reference_id': self.meta['order_id'], 'metadata': self.meta,
                       'payment_intent_data': {'metadata': dict(self.meta), 'capture_method': 'manual'},
                       'line_items': [{'quantity': 1, 'price_data': {'currency': 'eur', 'unit_amount': 148449,
                                      'product_data': {'name': 'SYNTHETIC mixed order'}}}],
                       'success_url': 'https://shop.example.test/shop/zahlung/erfolg',
                       'cancel_url': 'https://shop.example.test/shop/zahlung/abgebrochen',
                       'expires_at': self.now + 3600}

    def tearDown(self):
        self.network.stop()

    def test_no_network_on_construction_and_explicit_mode_credentials_only(self):
        self.client.assert_not_called()
        for field, value in [('SHOP_STRIPE_MODE', 'off'), ('SHOP_STRIPE_SECRET_KEY', 'sk_live_SYNTHETIC_ONLY'),
                             ('SHOP_STRIPE_WEBHOOK_SECRET', ''),
                             ('SHOP_STRIPE_PUBLIC_ORIGIN', 'http://shop.example.test'),
                             ('SHOP_STRIPE_PUBLIC_ORIGIN', 'https://user:secret@shop.example.test')]:
            with self.subTest(field=field, value=value):
                with self.assertRaises(StripeShopError):
                    StripeShopGateway({**self.config, field: value}, client=self.client)

    def test_manual_creation_uses_exact_key_no_connect_or_customer_bank_data(self):
        self.client.v1.checkout.sessions.create.return_value = {'id': 'cs_test_SYNTHETIC'}
        result = self.gateway.create_session(self.params, idempotency_key='synthetic-stable-key')
        self.assertEqual(result['id'], 'cs_test_SYNTHETIC')
        self.client.v1.checkout.sessions.create.assert_called_once_with(self.params,
            options={'idempotency_key': 'synthetic-stable-key'})
        self.client.v1.payment_intents.capture.assert_not_called()
        self.assertFalse(hasattr(self.gateway, 'capture'))
        self.assertFalse(hasattr(self.gateway, 'refund'))

    def test_checkout_amount_mutations_extra_charges_and_unapproved_payment_methods_reject_before_io(self):
        cases = [lambda p: p.update(payment_method_types=['paypal']),
                 lambda p: p.update(discounts=[{'coupon': 'SYNTHETIC'}]),
                 lambda p: p.update(automatic_tax={'enabled': True}),
                 lambda p: p.update(allow_promotion_codes=True),
                 lambda p: p.update(shipping_options=[]),
                 lambda p: p['payment_intent_data'].update(transfer_data={'destination': 'acct_SYNTHETIC'}),
                 lambda p: p['payment_intent_data'].update(capture_method='automatic'),
                 lambda p: p['line_items'][0]['price_data'].update(currency='usd'),
                 lambda p: p['line_items'][0]['price_data'].update(tax_behavior='exclusive'),
                 lambda p: p['line_items'][0]['price_data'].update(unit_amount=True),
                 lambda p: p['line_items'][0]['price_data'].update(unit_amount=-1),
                 lambda p: p.update(client_reference_id='sho_' + 'd' * 32),
                 lambda p: p.update(expires_at=self.now + 900)]
        for mutate in cases:
            p = deepcopy(self.params)
            mutate(p)
            with self.assertRaises(StripeShopError):
                self.gateway.create_session(p, idempotency_key='synthetic-stable-key')
        self.client.v1.checkout.sessions.create.assert_not_called()

    def test_return_urls_are_exact_https_server_origin_and_shop_path(self):
        for url in ('https://attacker.test/shop/return', '//shop.example.test/shop/return',
                    'http://shop.example.test/shop/return', 'https://shop.example.test.evil/shop/return',
                    'https://shop.example.test/admin', 'https://shop.example.test/shop/return#secret',
                    'https://x@shop.example.test/shop/return'):
            with self.subTest(url=url):
                with self.assertRaises(StripeShopError):
                    self.gateway.create_session({**self.params, 'success_url': url}, idempotency_key='synthetic-stable-key')
        self.client.v1.checkout.sessions.create.assert_not_called()

    def test_provider_failure_is_unknown_no_hidden_retry_or_exception_text(self):
        self.client.v1.checkout.sessions.create.side_effect = TimeoutError('PRIVATE_CANARY')
        with self.assertRaises(StripeShopError) as caught:
            self.gateway.create_session(self.params, idempotency_key='synthetic-stable-key')
        self.assertEqual(caught.exception.code, 'creation_outcome_unknown')
        self.assertNotIn('PRIVATE_CANARY', str(caught.exception))
        self.assertEqual(self.client.v1.checkout.sessions.create.call_count, 1)

    def test_reads_reject_arbitrary_ids_before_sdk_and_return_safe_failures(self):
        for session_id in ('cs_live_SYNTHETIC', 'https://attacker.test/', '../x', None):
            with self.assertRaises(StripeShopError):
                self.gateway.retrieve_session(session_id)
        self.client.v1.checkout.sessions.retrieve.assert_not_called()
        self.client.v1.checkout.sessions.retrieve.return_value = {'id': 'cs_test_SYNTHETIC'}
        self.assertEqual(self.gateway.retrieve_session('cs_test_SYNTHETIC')['id'], 'cs_test_SYNTHETIC')
        self.client.v1.payment_intents.retrieve.return_value = {'id': 'pi_SYNTHETIC'}
        self.assertEqual(self.gateway.retrieve_intent('pi_SYNTHETIC')['id'], 'pi_SYNTHETIC')
        self.client.v1.payment_intents.retrieve.side_effect = RuntimeError('PRIVATE_CANARY')
        with self.assertRaises(StripeShopError) as caught:
            self.gateway.retrieve_intent('pi_SYNTHETIC')
        self.assertEqual(caught.exception.code, 'provider_unavailable')

    def signed(self, body, *, timestamp=None):
        ts = self.now if timestamp is None else timestamp
        signature = hmac.new(self.config['SHOP_STRIPE_WEBHOOK_SECRET'].encode(),
                             str(ts).encode() + b'.' + body, hashlib.sha256).hexdigest()
        return f't={ts},v1={signature}'

    def event(self, **kwargs):
        return json.dumps({'id': 'evt_SYNTHETIC', 'type': 'payment_intent.succeeded',
                           'livemode': False, 'data': {'object': {'id': 'pi_SYNTHETIC'}}, **kwargs}).encode()

    def test_real_sdk_verifies_raw_signed_bytes_without_provider_reads(self):
        body = self.event()
        self.assertEqual(self.gateway.verify_webhook(body, self.signed(body))['id'], 'evt_SYNTHETIC')
        self.client.v1.checkout.sessions.retrieve.assert_not_called()

    def test_changed_expired_future_duplicate_timestamps_and_cross_account_events_reject(self):
        body = self.event()
        for raw, signature in [(body + b' ', self.signed(body)),
                               (body, self.signed(body, timestamp=self.now - 301)),
                               (body, self.signed(body, timestamp=self.now + 301)),
                               (body, self.signed(body) + f',t={self.now}')]:
            with self.assertRaises(StripeShopError):
                self.gateway.verify_webhook(raw, signature)
        for values in ({'livemode': True}, {'account': 'acct_SYNTHETIC'}, {'context': 'acct_SYNTHETIC'}):
            raw = self.event(**values)
            with self.assertRaises(StripeShopError):
                self.gateway.verify_webhook(raw, self.signed(raw))

    def test_sdk_client_disables_automatic_network_retries(self):
        sdk = Mock()
        StripeShopGateway(self.config, sdk=sdk)
        self.assertEqual(sdk.StripeClient.call_args.kwargs['max_network_retries'], 0)
        sdk.RequestsClient.assert_called_once_with(timeout=(5.0, 15.0))


if __name__ == '__main__':
    unittest.main()
