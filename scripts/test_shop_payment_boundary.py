"""Synthetic quote -> journal -> real SDK transport -> signed HTTP webhook.

Stripe HTTP is replaced with a fake client. Only the local SDK signature
verifier runs. No live app, credentials, orders, mail or actual payments.
"""
from contextlib import closing
from copy import deepcopy
import hashlib
import hmac
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
from flask import Flask
from shop_checkout import ShopCheckout
from shop_order_store import OrderStore, init_schema
from shop_public_api import create_shop_public_blueprint
from shop_stripe_gateway import StripeShopGateway
from test_shop_cart_quotes import Fixture, NOW, TYRE, RIM, OIL, BODY, line


class PaymentBoundaryChecks(unittest.TestCase):
    origin = 'https://shop.example.test'
    secret = 'whsec_SYNTHETIC_ONLY'

    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.temp = tempfile.TemporaryDirectory()
        path = Path(self.temp.name) / 'synthetic.sqlite'
        factory = lambda: sqlite3.connect(path)
        with closing(factory()) as db:
            init_schema(db)
        self.store = OrderStore(factory, clock=lambda: NOW)
        fixture = Fixture()
        context = {'postcode': '74821', 'address_reference': 'SYNTHETIC_ONLY'}
        quote = fixture.service_instance().quote({'lines': [
            line(TYRE, 4, services={'mounting': True}, group='set-1'),
            line(RIM, 4, services={'mounting': True}, group='set-1'), line(OIL, 2),
            line(BODY, services={'painting': True, 'paint_color': 'LY7W'})]},
            customer_id='customer-a', vehicle_ref='CAR-1', delivery_context=context)
        self.store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash,
            customer_id='customer-a', mode='test', delivery_context=context)
        self.order = self.store.create_order(quote.quote_hash, customer_id='customer-a',
            mode='test', idempotency_key='synthetic-mixed-payment-1')
        self.sdk_client = Mock()
        self.gateway = StripeShopGateway({'SHOP_STRIPE_MODE': 'test',
            'SHOP_STRIPE_SECRET_KEY': 'rk_test_SYNTHETIC_ONLY',
            'SHOP_STRIPE_WEBHOOK_SECRET': self.secret, 'SHOP_STRIPE_PUBLIC_ORIGIN': self.origin},
            client=self.sdk_client, clock=lambda: NOW)
        self.checkout = ShopCheckout(self.store, self.gateway, mode='test', public_origin=self.origin,
            enabled=True, order_lookup=lambda oid: self.store.lookup_payment_owner(oid, mode='test'), clock=lambda: NOW)
        self.app = Flask(__name__)
        self.app.register_blueprint(create_shop_public_blueprint(checkout_service=self.checkout,
            order_store=self.store, authorize_quote=lambda request: 'customer-a'))
        self.app.config.update(SHOP_PUBLIC_CATALOGUE_ENABLED=True, SHOP_ORDER_DRAFT_ENABLED=True,
            SHOP_CHECKOUT_ENABLED=True, SHOP_ORDER_MODE='test', SHOP_PUBLIC_ORIGIN=self.origin)
        self.client = self.app.test_client()
        self.session = None
        self.intent = None
        self.sdk_client.v1.checkout.sessions.create.side_effect = self.create_session
        self.sdk_client.v1.checkout.sessions.retrieve.side_effect = lambda sid: deepcopy(self.session)
        self.sdk_client.v1.payment_intents.retrieve.side_effect = lambda iid: deepcopy(self.intent)

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()

    def create_session(self, params, *, options):
        self.sent_params = deepcopy(params)
        amount = sum(x['quantity'] * x['price_data']['unit_amount'] for x in params['line_items'])
        self.session = {'object': 'checkout.session', 'id': 'cs_test_SYNTHETIC', 'livemode': False,
            'mode': 'payment', 'currency': 'eur', 'amount_total': amount,
            'client_reference_id': params['client_reference_id'], 'metadata': deepcopy(params['metadata']),
            'status': 'open', 'payment_status': 'unpaid', 'expires_at': params['expires_at'],
            'success_url': params['success_url'], 'cancel_url': params['cancel_url'], 'payment_link': None,
            'payment_intent': None, 'url': 'https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC#stripe-fragment'}
        return deepcopy(self.session)

    def start(self):
        return self.client.post('/shop/api/v2/orders/' + self.order['id'] + '/checkout',
            base_url=self.origin, headers={'Origin': self.origin}, json={})

    def webhook(self, event):
        raw = json.dumps(event, separators=(',', ':')).encode()
        timestamp = int(NOW.timestamp())
        signature = hmac.new(self.secret.encode(), str(timestamp).encode() + b'.' + raw, hashlib.sha256).hexdigest()
        with patch('time.time', return_value=timestamp):
            return self.client.post('/shop/api/v2/payments/stripe/webhook', base_url=self.origin,
                headers={'Stripe-Signature': f't={timestamp},v1={signature}'},
                data=raw, content_type='application/json')

    def event(self, event_id, event_type, obj):
        return {'object': 'event', 'id': event_id, 'livemode': False, 'created': int(NOW.timestamp()),
                'type': event_type, 'data': {'object': deepcopy(obj)}}

    def authorize(self):
        self.session.update(status='complete', url=None, payment_intent='pi_SYNTHETIC')
        self.intent = {'object': 'payment_intent', 'id': 'pi_SYNTHETIC', 'livemode': False,
            'currency': 'eur', 'amount': 153209, 'amount_capturable': 153209, 'amount_received': 0,
            'capture_method': 'manual', 'metadata': deepcopy(self.session['metadata']), 'status': 'requires_capture'}

    def test_mixed_total_authorization_capture_evidence_and_replay_with_real_sdk_verification(self):
        result = self.start()
        self.assertEqual(result.status_code, 200, result.get_json())
        self.assertEqual(result.get_json()['checkout']['amount_gross_cent'], 153209)
        self.assertFalse(result.get_json()['checkout']['payment_confirmed'])
        self.assertEqual(sum(x['quantity'] * x['price_data']['unit_amount'] for x in self.sent_params['line_items']), 153209)
        self.assertEqual(len([x for x in self.sent_params['line_items'] if x['price_data']['product_data']['name'].startswith('Montage')]), 1)
        self.assertEqual(self.sent_params['payment_intent_data']['capture_method'], 'manual')
        self.assertEqual(self.start().status_code, 200)
        self.assertEqual(self.sdk_client.v1.checkout.sessions.create.call_count, 1)
        self.authorize()
        completed = self.webhook(self.event('evt_SYNTHETICCOMPLETE', 'checkout.session.completed', self.session))
        self.assertEqual(completed.status_code, 200, completed.get_json())
        state = self.store.get_order(self.order['id'], customer_id='customer-a', mode='test')
        self.assertEqual(state['status'], 'authorized')
        self.assertFalse(self.checkout.status(self.order['id'], customer_id='customer-a')['payment_confirmed'])
        expired_hint = deepcopy(self.session)
        expired_hint['status'] = 'expired'
        # A stale Session hint cannot erase the currently verified full PI hold.
        result = self.webhook(self.event('evt_SYNTHETICEXPIRY', 'checkout.session.expired', expired_hint))
        self.assertEqual(result.status_code, 200, result.get_json())
        self.assertEqual(self.store.get_order(self.order['id'], customer_id='customer-a', mode='test')['status'], 'authorized')
        authorization = self.event('evt_SYNTHETICAUTH', 'payment_intent.amount_capturable_updated', self.intent)
        self.assertEqual(self.webhook(authorization).status_code, 200)
        self.intent.update(status='succeeded', amount_capturable=0, amount_received=153209)
        self.assertEqual(self.webhook(self.event('evt_SYNTHETICPAID', 'payment_intent.succeeded', self.intent)).status_code, 200)
        self.assertTrue(self.checkout.status(self.order['id'], customer_id='customer-a')['payment_confirmed'])
        self.assertEqual(self.webhook(authorization).status_code, 200)
        self.assertEqual(self.store.get_order(self.order['id'], customer_id='customer-a', mode='test')['status'], 'paid')
        self.sdk_client.v1.payment_intents.capture.assert_not_called()
        self.sdk_client.v1.payment_intents.cancel.assert_not_called()

    def test_uncertain_creation_never_posts_again_or_accepts_return_query_as_payment(self):
        self.sdk_client.v1.checkout.sessions.create.side_effect = TimeoutError('PRIVATE_CANARY')
        self.assertEqual(self.start().status_code, 503)
        self.assertEqual(self.start().status_code, 503)
        self.assertEqual(self.sdk_client.v1.checkout.sessions.create.call_count, 1)
        status = self.client.get('/shop/api/v2/orders/' + self.order['id'], base_url=self.origin)
        self.assertEqual(status.get_json()['order']['status'], 'payment_pending')
        self.assertEqual(self.client.get('/shop/api/v2/orders/' + self.order['id'] + '?paid=1', base_url=self.origin).status_code, 400)


if __name__ == '__main__':
    unittest.main()
