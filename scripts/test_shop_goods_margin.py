"""Owner-only costing against temporary, synthetic quote, order and RSU journals."""
from contextlib import closing
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from flask import Flask
from shop_admin import register_shop_admin
from shop_goods_margin import admin_goods_margins, retained_goods_cost
from shop_public_api import create_shop_public_blueprint
from shop_storage import ShopStorage
from test_shop_fulfillment import SagaFixture
from test_shop_order_store import CONTEXT, NOW, OIL, TYRE, quote


def retained(snapshot, prices, **edits):
    def lookup(item, quantity):
        if item['id'] not in prices:
            return None
        return {'confirmed': True, 'product_id': item['id'], 'identity_hash': item['identity_hash'],
                'supplier_article_id': item['mapping']['supplier_article_id'],
                'mapping_evidence_id': item['mapping']['evidence_id'], 'quantity': quantity,
                'currency': 'EUR', 'mode': snapshot['mode'], 'unit_net': prices[item['id']], **edits}
    return retained_goods_cost(snapshot, lookup)


class OwnerMarginTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('Network forbidden')))
        self.storage = ShopStorage(Path(folder.name), clock=lambda: NOW).provision(create=True)
        self.store = self.storage.order_store()
        app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        app.config.update(TESTING=True, SECRET_KEY='isolated-margin', SHOP_PUBLIC_CATALOGUE_ENABLED=True,
                          SHOP_ORDER_DRAFT_ENABLED=True, SHOP_ORDER_MODE='live', SHOP_PUBLIC_ORIGIN='https://shop.example')
        app.extensions['shop_production'] = {'storage': self.storage, 'store': self.store}
        register_shop_admin(app)
        app.register_blueprint(create_shop_public_blueprint(order_store=self.store,
            authorize_quote=lambda request: 'customer-a'))
        self.client = app.test_client()

    def login(self, **role):
        with self.client.session_transaction() as session:
            session.clear()
            session.update(role)

    def order(self, cost=None, status='paid'):
        self.snapshot = quote(mode='live')
        key = self.snapshot['quote_hash']
        self.store.save_quote(self.snapshot, expected_quote_hash=key, customer_id='customer-a', mode='live',
                              delivery_context=CONTEXT, goods_cost_net_cent=cost)
        order = self.store.create_order(key, customer_id='customer-a', mode='live', idempotency_key='synthetic-order')
        with closing(self.storage.connect()) as db:
            db.execute('UPDATE shop_orders_v2 SET status=? WHERE id=?', (status, order['id']))
            db.commit()
        return order['id']

    def admin_pages(self, order_id):
        self.login(admin=True)
        listing = self.client.get('/admin/shop')
        detail = self.client.get('/admin/shop/' + order_id)
        self.assertEqual((listing.status_code, detail.status_code), (200, 200))
        return listing.text, detail.text

    def test_costed_goods_only_in_owner_list_and_detail(self):
        # 4 tyres and 2 parts worth 440.00 gross, plus 80.00 mounting and 12.00 freight.
        cost = retained(quote(mode='live'), {TYRE: '55.125', OIL: '7.00'})
        self.assertEqual(cost, 23450)
        oid = self.order(cost)
        listing, detail = self.admin_pages(oid)
        for page in (listing, detail):
            self.assertIn('Warenmarge', page)
            self.assertIn('135,25 €', page)
            self.assertIn('36,6 %', page)
        for text in ('369,75 €', '234,50 €', 'EK bei Preisprüfung'):
            self.assertIn(text, detail)
        self.login()
        public = self.client.get('/shop/api/v2/orders/' + oid, base_url='https://shop.example')
        self.assertEqual(public.status_code, 200)
        self.assertEqual(public.json['order']['amount_gross_cent'], 53200)
        for private in ('unit_net', 'goods_cost', 'cost_net', 'margin', '23450', '13525'):
            self.assertNotIn(private, public.text)
            self.assertNotIn(private, str(self.store.get_order(oid, customer_id='customer-a', mode='live')))
        for identity in ({}, {'office': True, 'office_mid': 1}, {'assistent_mid': 1, 'assistent_version': 1},
                         {'werkstatt_tafel': True}, {'partner_autohaus_id': 1}, {'admin': 1}):
            with self.subTest(identity=identity):
                self.login(**identity)
                for url in ('/admin/shop', '/admin/shop/' + oid):
                    response = self.client.get(url)
                    self.assertEqual(response.status_code, 403)
                    self.assertNotIn('Warenmarge', response.text)
                    self.assertNotIn('234,50', response.text)

    def test_loss_shown_in_euros_and_percent(self):
        cost = retained(quote(mode='live'), {TYRE: '100', OIL: '10'})
        self.assertEqual(cost, 42000)
        for page in self.admin_pages(self.order(cost)):
            self.assertIn('−50,25 €', page)
            self.assertRegex(page, '[−-]13,6 %')

    def test_cancelled_with_known_purchase_does_not_count_as_revenue(self):
        for page in self.admin_pages(self.order(23450, status='cancelled')):
            self.assertIn('Kein Umsatz', page)
            self.assertNotIn('135,25 €', page)

    def test_absent_or_unbound_purchase_never_fabricates_zero_cost(self):
        snapshot = quote(mode='live')
        for prices, edits in (({TYRE: '55'}, {}), ({TYRE: '-5', OIL: '7'}, {}),
                ({TYRE: '0', OIL: '7'}, {}), ({TYRE: '55', OIL: '7'}, {'quantity': 1}),
                ({TYRE: '55', OIL: '7'}, {'mapping_evidence_id': 'wrong-mapping'})):
            with self.subTest(prices=prices, edits=edits):
                self.assertIsNone(retained(snapshot, prices, **edits))
        oid = self.order()
        # An older journal may not even contain the new side table yet.
        with closing(self.storage.connect()) as db:
            db.execute('DROP TABLE shop_quote_goods_cost_v1')
            db.commit()
        for page in self.admin_pages(oid):
            self.assertIn('EK fehlt', page)
            self.assertNotIn('100,0 %', page)


class HistoricalPurchases(SagaFixture, unittest.TestCase):
    def test_only_all_accepted_journal_positions_can_supply_historic_cost(self):
        def result():
            with closing(self.factory()) as db:
                db.row_factory = sqlite3.Row
                return admin_goods_margins(db, {'shop_fulfillment_positions_v3'}, [self.order['id']])[self.order['id']]

        self.assertEqual(result()['status'], 'EK fehlt')
        self.assertEqual(self.run_saga()['state'], 'captured')
        actual = result()
        # 4 tyres and 4 rims at 100 net each, 2 oils and one part at 50 each.
        self.assertEqual((actual['status'], actual['source'], actual['purchase']),
                         ('Kalkuliert', 'Lieferantenbestellung', '950,00 €'))
        with closing(self.factory()) as db:
            db.execute("""UPDATE shop_fulfillment_positions_v3 SET state='unknown' WHERE child_key=(
                SELECT child_key FROM shop_fulfillment_positions_v3 WHERE order_id=? LIMIT 1)""", (self.order['id'],))
            db.commit()
        self.assertEqual(result()['status'], 'EK fehlt')


if __name__ == '__main__':
    unittest.main()
