"""Synthetic Flask boundary tests; no portal import, network or real records."""
import sys
from pathlib import Path
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask
from shop_public_api import create_shop_public_blueprint
from shop_order_store import OrderStoreError


class PublicApiChecks(unittest.TestCase):
    origin = 'https://shop.example.test'

    def test_price_completed_during_warmup_is_reprojected_before_polling_stops(self):
        identifier = 'tyre:' + 'a' * 24
        for detail in (False, True):
            with self.subTest(detail=detail):
                app = Flask('cache-completion-race')
                state = {'complete': False}
                def item():
                    return {'id': identifier, 'category': 'tyre',
                            'price': {'unit_gross_cent': 12345} if state['complete'] else None}
                cat = Mock()
                cat.page.side_effect = lambda **kwargs: {'items': [item()], 'page': 1, 'total': 1}
                cat.get.side_effect = lambda value: item()
                def warmup(ids, *, quantities):
                    state['complete'] = True
                    return False
                app.register_blueprint(create_shop_public_blueprint(catalogue=cat, catalogue_warmup=warmup))
                app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
                path = '/shop/api/v2/products' + ('/' + identifier + '?quantity=2' if detail else '')
                result = app.test_client().get(path).get_json()
                self.assertIs(result['prices_refreshing'], False)
                projected = result['item'] if detail else result['items'][0]
                self.assertEqual(projected['price']['unit_gross_cent'], 12345)

    def test_public_warmup_only_queues_returned_ids_and_exact_bounded_quantity(self):
        app = Flask('price-warmup')
        cat = Mock()
        identifier = 'tyre:' + 'a' * 24
        cat.page.return_value = {'items': [{'id': identifier}], 'page': 1, 'total': 1}
        cat.get.return_value = {'id': identifier, 'category': 'tyre'}
        warmup = Mock(return_value=1)
        app.register_blueprint(create_shop_public_blueprint(catalogue=cat, catalogue_warmup=warmup))
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        client = app.test_client()
        result = client.get('/shop/api/v2/products')
        self.assertTrue(result.get_json()['prices_refreshing'])
        warmup.assert_called_once_with([identifier], quantities=(1, 4))
        warmup.reset_mock()
        result = client.get('/shop/api/v2/products/' + identifier + '?quantity=2')
        self.assertEqual(result.status_code, 200)
        warmup.assert_called_once_with([identifier], quantities=(2,))
        warmup.reset_mock()
        for query in ('quantity=5', 'quantity=0', 'quantity=1&quantity=2', 'quantity=2&provider_url=x'):
            self.assertEqual(client.get('/shop/api/v2/products/' + identifier + '?' + query).status_code, 400)
        warmup.assert_not_called()
        warmup.side_effect = RuntimeError('PRIVATE_CANARY')
        result = client.get('/shop/api/v2/products/' + identifier)
        self.assertEqual(result.status_code, 200)
        self.assertFalse(result.get_json()['prices_refreshing'])
        self.assertNotIn(b'PRIVATE_CANARY', result.data)

    def setUp(self):
        self.catalogue = Mock()
        self.catalogue.page.return_value = {'items': [], 'page': 1, 'page_size': 24, 'total': 0}
        self.catalogue.get.return_value = None
        self.service = Mock()
        self.service.quote.return_value.public.return_value = {
            'ready': False, 'blocked_reasons': ['freight_unconfirmed'], 'total_gross_cent': None}
        self.auth = Mock(return_value='guest:synthetic-session')
        self.context = Mock(return_value={})
        self.sink = Mock()
        self.orders = Mock()
        self.app = Flask(__name__)
        self.app.register_blueprint(create_shop_public_blueprint(
            catalogue=self.catalogue, quote_service=self.service, authorize_quote=self.auth,
            quote_context=self.context, quote_sink=self.sink, order_store=self.orders))
        self.client = self.app.test_client()

    def enabled(self):
        self.app.config.update(SHOP_PUBLIC_CATALOGUE_ENABLED=True,
                               SHOP_CART_QUOTE_ENABLED=True, SHOP_PUBLIC_ORIGIN=self.origin)

    def post(self, body='{"lines":[]}', **kwargs):
        return self.client.post('/shop/api/v2/cart/quote', base_url=self.origin,
                                headers={'Origin': self.origin}, data=body,
                                content_type='application/json', **kwargs)

    def test_all_routes_default_off_even_with_dependencies_present(self):
        self.assertEqual(self.client.get('/shop/api/v2/products').status_code, 404)
        self.assertEqual(self.post().status_code, 404)
        self.assertEqual(self.client.post('/shop/api/v2/orders', json={}).status_code, 404)
        self.assertEqual(self.client.get('/shop/api/v2/orders/sho_' + 'a' * 32).status_code, 404)
        self.catalogue.page.assert_not_called()
        self.service.quote.assert_not_called()

    def test_bounded_query_preserves_only_explicit_catalogue_filters(self):
        self.enabled()
        result = self.client.get('/shop/api/v2/products?page=2&page_size=24&q=Audi&year=2016')
        self.assertEqual(result.status_code, 200)
        self.catalogue.page.assert_called_once_with(page=2, page_size=24, q='Audi', year=2016)
        self.assertEqual(result.headers['Cache-Control'], 'no-store')
        for query in ('page=1&page=2', 'provider_url=https://example.test', 'page=-1', 'q=' + 'x' * 161):
            with self.subTest(query=query):
                self.assertEqual(self.client.get('/shop/api/v2/products?' + query).status_code, 400)

    def test_exact_lookup_404_and_fixed_errors_do_not_echo_internal_failures(self):
        self.enabled()
        self.assertEqual(self.client.get('/shop/api/v2/products/tyre:' + 'a' * 24).status_code, 404)
        self.catalogue.get.side_effect = RuntimeError('PRIVATE_PROVIDER_CANARY')
        response = self.client.get('/shop/api/v2/products/tyre:' + 'b' * 24)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(b'PRIVATE_PROVIDER_CANARY', response.data)

    def test_quotes_require_exact_https_origin_and_host_session_csrf_authorization(self):
        self.enabled()
        for origin, base in (('https://attacker.example', self.origin), (None, self.origin),
                             (self.origin, 'http://shop.example.test')):
            response = self.client.post('/shop/api/v2/cart/quote', base_url=base,
                headers={'Origin': origin} if origin else {}, json={'lines': []})
            self.assertEqual(response.status_code, 403)
        self.service.quote.assert_not_called()
        self.auth.return_value = None
        self.assertEqual(self.post().status_code, 403)
        self.auth.return_value = 'x' * 101
        self.assertEqual(self.post().status_code, 403)
        self.service.quote.assert_not_called()

    def test_duplicate_fields_nonfinite_and_oversized_bodies_never_reach_the_service(self):
        self.enabled()
        for body in ('{"lines":[],"lines":[]}', '{"lines":NaN}', '[]', 'x' * 16385):
            with self.subTest(body=body[:40]):
                self.assertEqual(self.post(body).status_code, 400)
        self.service.quote.assert_not_called()

    def test_quote_calls_the_server_identity_and_never_claims_a_payment_or_order(self):
        self.enabled()
        result = self.post()
        self.assertEqual(result.status_code, 200)
        self.service.quote.assert_called_once_with({'lines': []}, customer_id='guest:synthetic-session')
        body = result.get_json()
        self.assertFalse(body['order_created'])
        self.assertFalse(body['payment_enabled'])
        self.assertEqual(body['quote']['blocked_reasons'], ['freight_unconfirmed'])

    def test_misconfigured_dependencies_and_nonfinite_responses_fail_closed(self):
        self.enabled()
        self.catalogue.page.return_value = {'price': float('nan')}
        self.assertEqual(self.client.get('/shop/api/v2/products').status_code, 503)
        self.service.quote.side_effect = RuntimeError('PRIVATE_TOKEN_CANARY')
        response = self.post()
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(b'PRIVATE_TOKEN_CANARY', response.data)

    def test_vehicle_and_delivery_context_come_only_from_the_host_session(self):
        self.enabled()
        self.context.return_value = {'vehicle_ref': 'reviewed-vehicle',
                                     'delivery_context': {'postcode': '74821'}}
        self.assertEqual(self.post().status_code, 200)
        self.service.quote.assert_called_once_with({'lines': []}, customer_id='guest:synthetic-session',
            vehicle_ref='reviewed-vehicle', delivery_context={'postcode': '74821'})
        self.service.quote.reset_mock()
        self.context.return_value = {'mode': 'live'}
        self.assertEqual(self.post().status_code, 503)
        self.service.quote.assert_not_called()

    def test_complete_unicode_fitment_details_have_their_own_bounded_budget(self):
        self.enabled()
        self.catalogue.get.return_value = {'conditions': ['Ä' * 300] * 2560}
        self.assertEqual(self.client.get('/shop/api/v2/products/tyre:' + 'a' * 24).status_code, 200)
        self.catalogue.get.return_value = {'conditions': 'Ä' * (3 * 1024 * 1024)}
        self.assertEqual(self.client.get('/shop/api/v2/products/tyre:' + 'a' * 24).status_code, 503)

    def test_only_ready_quotes_are_persisted_with_the_exact_trusted_delivery_context(self):
        self.enabled()
        self.assertEqual(self.post().status_code, 200)
        self.sink.assert_not_called()
        self.context.return_value = {'delivery_context': {'postcode': '74821'}}
        self.service.quote.return_value.public.return_value = {'ready': True, 'total_gross_cent': 12345}
        response = self.post()
        self.assertTrue(response.get_json()['quote_retained'])
        self.sink.assert_called_once_with(self.service.quote.return_value,
            customer_id='guest:synthetic-session', delivery_context={'postcode': '74821'})
        self.sink.side_effect = RuntimeError('PRIVATE_STORAGE_CANARY')
        response = self.post()
        self.assertEqual(response.status_code, 503)
        self.assertNotIn(b'PRIVATE_STORAGE_CANARY', response.data)
        self.sink.side_effect = OrderStoreError('storage_unavailable')
        self.assertEqual(self.post().status_code, 503)

    def test_storage_outage_is_unavailable_instead_of_missing_order_or_quote_conflict(self):
        self.enabled()
        self.app.config.update(SHOP_ORDER_DRAFT_ENABLED=True, SHOP_ORDER_MODE='test')
        self.orders.create_order.side_effect = OrderStoreError('storage_unavailable')
        self.orders.get_order.side_effect = OrderStoreError('storage_unavailable')
        result = self.client.post('/shop/api/v2/orders', base_url=self.origin,
            headers={'Origin': self.origin}, json={'quote_hash': 'a' * 64, 'idempotency_key': 'synthetic-key-1'})
        self.assertEqual(result.status_code, 503)
        result = self.client.get('/shop/api/v2/orders/sho_' + 'a' * 32, base_url=self.origin)
        self.assertEqual(result.status_code, 503)
        self.orders.get_order.side_effect = OrderStoreError('order_not_found')
        self.assertEqual(self.client.get('/shop/api/v2/orders/sho_' + 'a' * 32, base_url=self.origin).status_code, 404)


if __name__ == '__main__':
    unittest.main()
