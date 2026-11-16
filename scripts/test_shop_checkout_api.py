"""HTTP payment boundary with fake service, no SDK, portal or provider IO."""
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask
from shop_public_api import create_shop_public_blueprint


class SafeError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__('PRIVATE_CANARY')


class CheckoutApiChecks(unittest.TestCase):
    origin = 'https://shop.example.test'
    order_id = 'sho_' + 'a' * 32

    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.checkout = Mock()
        self.checkout.mode = 'test'
        self.checkout.readiness.return_value = {'ready': True, 'mode': 'test',
                                               'capture_method': 'manual', 'payment_methods': ['card']}
        self.checkout.start.return_value = {'order_id': self.order_id, 'status': 'payment_pending',
            'mode': 'test', 'currency': 'eur', 'amount_gross_cent': 148449,
            'session_id': 'cs_test_PRIVATE_CANARY', 'checkout_url': 'https://checkout.stripe.com/c/pay/SYNTHETIC#fragment',
            'payment_state': 'pending', 'authorization_confirmed': False, 'payment_confirmed': False,
            'capture_triggered': False, 'supplier_order_created': False, 'PRIVATE_CANARY': 'PRIVATE_CANARY'}
        self.auth = Mock(return_value='customer-a')
        self.orders = Mock()
        self.orders.get_order.return_value = {'id': self.order_id, 'mode': 'test', 'quote_hash': 'b' * 64,
            'amount_gross_cent': 148449, 'currency': 'eur', 'created_at': '2026-11-16T12:00:00Z',
            'quote_expires_at': '2026-11-16T12:15:00Z', 'status': 'payment_pending'}
        self.app = Flask(__name__)
        self.app.register_blueprint(create_shop_public_blueprint(checkout_service=self.checkout, authorize_quote=self.auth,
            order_store=self.orders))
        self.client = self.app.test_client()

    def tearDown(self):
        self.network.stop()

    def enable(self):
        self.app.config.update(SHOP_PUBLIC_CATALOGUE_ENABLED=True, SHOP_ORDER_DRAFT_ENABLED=True,
            SHOP_CHECKOUT_ENABLED=True, SHOP_ORDER_MODE='test', SHOP_PUBLIC_ORIGIN=self.origin)

    def start(self, **kwargs):
        return self.client.post('/shop/api/v2/orders/' + self.order_id + '/checkout',
            base_url=self.origin, headers={'Origin': self.origin}, json=kwargs)

    def webhook(self, *, headers=None, data=b'{"synthetic":true}', base=None):
        return self.client.post('/shop/api/v2/payments/stripe/webhook', base_url=base or self.origin,
            headers={'Stripe-Signature': 't=123,v1=SYNTHETIC', **(headers or {})}, data=data,
            content_type='application/json')

    def test_all_checkout_paths_default_off(self):
        self.assertEqual(self.start().status_code, 404)
        self.assertEqual(self.webhook().status_code, 404)
        self.checkout.start.assert_not_called()
        self.checkout.handle_webhook.assert_not_called()

    def test_start_uses_owner_and_stored_order_only_projects_safe_fields(self):
        self.enable()
        response = self.start()
        self.assertEqual(response.status_code, 200)
        self.checkout.start.assert_called_once_with(self.order_id, customer_id='customer-a')
        self.assertNotIn(b'PRIVATE_CANARY', response.data)
        self.assertFalse(response.get_json()['checkout']['payment_confirmed'])
        self.assertTrue(response.get_json()['checkout']['checkout_url'].endswith('#fragment'))

    def test_caller_amount_mode_url_snapshot_or_missing_authorization_cannot_start(self):
        self.enable()
        for extra in ({'mode': 'live'}, {'amount': 1}, {'url': 'https://attacker.test'}, {'snapshot': {}}):
            self.assertEqual(self.start(**extra).status_code, 400)
        self.auth.return_value = None
        self.assertEqual(self.start().status_code, 403)
        self.checkout.start.assert_not_called()

    def test_mode_and_readiness_fail_before_start(self):
        self.enable()
        self.checkout.mode = 'live'
        self.assertEqual(self.start().status_code, 503)
        self.checkout.mode = 'test'
        self.checkout.readiness.return_value['capture_method'] = 'automatic'
        self.assertEqual(self.start().status_code, 503)
        self.checkout.start.assert_not_called()

    def test_order_status_only_advertises_payment_when_the_service_is_actually_ready(self):
        self.enable()
        path = '/shop/api/v2/orders/' + self.order_id
        self.assertTrue(self.client.get(path, base_url=self.origin).get_json()['payment_enabled'])
        self.checkout.readiness.return_value['ready'] = False
        response = self.client.get(path, base_url=self.origin)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()['payment_enabled'])
        self.checkout.readiness.return_value['ready'] = True
        self.checkout.mode = 'live'
        self.assertFalse(self.client.get(path, base_url=self.origin).get_json()['payment_enabled'])
        self.checkout.mode = 'test'
        self.checkout.readiness.side_effect = RuntimeError('PRIVATE_CANARY')
        response = self.client.get(path, base_url=self.origin)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()['payment_enabled'])
        self.assertNotIn(b'PRIVATE_CANARY', response.data)

    def test_malformed_or_cross_origin_checkout_url_and_provider_exceptions_are_not_exposed(self):
        self.enable()
        self.checkout.start.return_value['checkout_url'] = 'https://attacker.test/pay'
        self.assertEqual(self.start().status_code, 503)
        self.checkout.start.side_effect = SafeError('creation_outcome_unknown')
        result = self.start()
        self.assertEqual(result.status_code, 503)
        self.assertNotIn(b'PRIVATE_CANARY', result.data)

    def test_webhook_passes_exact_bytes_signature_without_browser_auth_or_trusting_return(self):
        self.enable()
        self.checkout.handle_webhook.return_value = {'received': True, 'ignored': False, 'order_id': 'PRIVATE_CANARY'}
        raw = b'{ "synthetic" : true }\n'
        result = self.webhook(data=raw)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.get_json(), {'received': True, 'ignored': False})
        self.checkout.handle_webhook.assert_called_once_with(raw, 't=123,v1=SYNTHETIC')
        self.auth.assert_not_called()

    def test_invalid_or_large_unsigned_and_wrong_host_webhooks_do_not_reach_service(self):
        self.enable()
        self.assertEqual(self.webhook(headers={'Stripe-Signature': ''}).status_code, 400)
        self.assertEqual(self.webhook(data=b'x' * 262145).status_code, 400)
        self.assertEqual(self.webhook(base='http://shop.example.test').status_code, 403)
        self.assertEqual(self.webhook(base='https://attacker.test').status_code, 403)
        self.checkout.handle_webhook.assert_not_called()

    def test_bad_signature_is_rejected_and_transient_failure_retried_by_provider(self):
        self.enable()
        self.checkout.handle_webhook.side_effect = SafeError('invalid_webhook')
        self.assertEqual(self.webhook().status_code, 400)
        self.checkout.handle_webhook.side_effect = SafeError('provider_unavailable')
        result = self.webhook()
        self.assertEqual(result.status_code, 503)
        self.assertNotIn(b'PRIVATE_CANARY', result.data)


if __name__ == '__main__':
    unittest.main()
