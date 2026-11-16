"""Real catalogue/quote/store integration with synthetic goods and temporary DB.

The HTTP boundary is exercised without importing or starting the live portal.
No network, payment adapter, mail worker or supplier order is available.
"""
from contextlib import closing
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
from flask import Flask
from shop_catalog_public import PublicCatalog
from shop_order_store import OrderStore, OrderStoreError, init_schema
from shop_public_api import create_shop_public_blueprint
from test_shop_cart_quotes import Fixture, NOW, TYRE, RIM, OIL, BODY, line


class ProductionBoundaryChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.temp = tempfile.TemporaryDirectory()
        path = Path(self.temp.name) / 'synthetic-shop.sqlite'
        self.factory = lambda: sqlite3.connect(path, timeout=5)
        with closing(self.factory()) as db:
            init_schema(db)
        self.store = OrderStore(self.factory, clock=lambda: NOW)
        self.fixture = Fixture()
        self.service = self.fixture.service_instance()
        self.principal = 'customer-a'
        self.context = {'postcode': '74821', 'address_reference': 'SYNTHETIC_ONLY'}
        products = [dict(row, department='accessories') for row in self.fixture.products.values()]
        self.catalogue = PublicCatalog(products, released_ids={TYRE, RIM, OIL, BODY}, clock=lambda: NOW)
        self.app = Flask(__name__)

        def save(quote, *, customer_id, delivery_context):
            self.store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash,
                customer_id=customer_id, mode='test', delivery_context=delivery_context)

        self.app.register_blueprint(create_shop_public_blueprint(
            catalogue=self.catalogue, quote_service=self.service,
            authorize_quote=lambda request: self.principal,
            quote_context=lambda request, principal: {'vehicle_ref': 'CAR-1', 'delivery_context': self.context},
            quote_sink=save, order_store=self.store))
        self.app.config.update(SHOP_PUBLIC_CATALOGUE_ENABLED=True, SHOP_CART_QUOTE_ENABLED=True,
                               SHOP_PUBLIC_ORIGIN='https://shop.example.test',
                               SHOP_ORDER_DRAFT_ENABLED=True, SHOP_ORDER_MODE='test')
        self.client = self.app.test_client()

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()

    def post(self, payload):
        return self.client.post('/shop/api/v2/cart/quote', base_url='https://shop.example.test',
            headers={'Origin': 'https://shop.example.test'}, json=payload)

    def mixed(self):
        return {'lines': [line(TYRE, 4, services={'mounting': True}, group='set-1'),
                          line(RIM, 4, services={'mounting': True}, group='set-1'),
                          line(OIL, 2), line(BODY, services={'painting': True, 'paint_color': 'LY7W'})]}

    def post_order(self, payload):
        return self.client.post('/shop/api/v2/orders', base_url='https://shop.example.test',
            headers={'Origin': 'https://shop.example.test'}, json=payload)

    def test_bounded_browse_never_calls_quote_dependencies_or_releases_private_data(self):
        response = self.client.get('/shop/api/v2/products?page_size=2')
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(len(body['items']), 2)
        self.assertEqual(body['total'], 4)
        self.assertTrue(body['has_more'])
        self.assertFalse(any(self.fixture.calls.values()))
        self.assertNotIn('PRIVATE_CANARY', json.dumps(body))
        self.assertTrue(all(row['order_enabled'] is False for row in body['items']))

    def test_mixed_http_quote_is_retained_then_creates_one_bound_synthetic_order(self):
        response = self.post(self.mixed())
        self.assertEqual(response.status_code, 200, response.get_json())
        body = response.get_json()
        self.assertTrue(body['quote_retained'])
        self.assertTrue(body['quote']['ready'])
        self.assertFalse(body['order_created'])
        self.assertFalse(body['payment_enabled'])
        self.assertEqual(body['quote']['total_gross_cent'], 148449)
        self.assertEqual(len([v for v in self.fixture.calls['services'] if v[0] == 'mounting']), 1)
        self.assertEqual(len(body['quote']['shipments']), 2)
        self.assertNotIn('PRIVATE_CANARY', json.dumps(body))
        quote_hash = body['quote']['quote_hash']
        args = dict(customer_id='customer-a', mode='test', idempotency_key='synthetic-checkout-1')
        payload = {'quote_hash': quote_hash, 'idempotency_key': args['idempotency_key']}
        result = self.post_order(payload)
        self.assertEqual(result.status_code, 200, result.get_json())
        self.assertFalse(result.get_json()['payment_enabled'])
        self.assertFalse(result.get_json()['supplier_order_created'])
        order = result.get_json()['order']
        again = self.post_order(payload).get_json()['order']
        self.assertEqual(order['id'], again['id'])
        self.assertNotIn('customer_id', order)
        self.assertEqual(order['amount_gross_cent'], 148449)
        self.assertEqual(self.store.delivery_context_for_order(order['id'], customer_id='customer-a', mode='test'), self.context)
        with self.assertRaises(OrderStoreError):
            self.store.create_order(quote_hash, customer_id='customer-b', mode='test', idempotency_key='synthetic-checkout-1')
        with self.assertRaises(OrderStoreError):
            self.store.create_order(quote_hash, customer_id='customer-a', mode='live', idempotency_key='synthetic-checkout-1')
        self.principal = 'customer-b'
        self.assertEqual(self.post_order(payload).status_code, 409)
        self.assertEqual(self.client.get('/shop/api/v2/orders/' + order['id'], base_url='https://shop.example.test').status_code, 404)
        self.principal = 'customer-a'
        self.assertEqual(self.client.get('/shop/api/v2/orders/' + order['id'], base_url='https://shop.example.test').status_code, 200)

    def test_unconfirmed_freight_and_spoofed_prices_never_create_a_retained_quote(self):
        self.service.freight_quote = lambda shipment, context: None
        response = self.post(self.mixed())
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertFalse(body['quote']['ready'])
        self.assertFalse(body['quote_retained'])
        self.assertIsNone(body['quote']['total_gross_cent'])
        self.assertEqual(self.post_order({'quote_hash': body['quote']['quote_hash'],
                         'idempotency_key': 'synthetic-checkout-1'}).status_code, 409)
        self.assertEqual(self.post({**self.mixed(), 'total_gross_cent': 1}).status_code, 400)
        with closing(self.factory()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_quotes_v2').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_orders_v2').fetchone()[0], 0)

    def test_client_amounts_snapshots_and_mode_cannot_create_an_order(self):
        quote = self.post(self.mixed()).get_json()['quote']
        payload = {'quote_hash': quote['quote_hash'], 'idempotency_key': 'synthetic-checkout-1'}
        for field, value in (('amount_gross_cent', 1), ('mode', 'live'), ('snapshot', quote)):
            with self.subTest(field=field):
                self.assertEqual(self.post_order({**payload, field: value}).status_code, 400)
        self.app.config['SHOP_ORDER_DRAFT_ENABLED'] = False
        self.assertEqual(self.post_order(payload).status_code, 404)
        with closing(self.factory()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_orders_v2').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
