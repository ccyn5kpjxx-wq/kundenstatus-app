"""Internal delivery-date edits against an isolated SQLite journal without network."""
from contextlib import closing
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

from flask import Flask

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from shop_admin import register_shop_admin
from shop_fulfillment import ShopFulfillment, init_fulfillment_schema
from shop_storage import ShopStorage
from shop_supplier_delivery_dates import DATE_TABLE, delivery_dates
from test_shop_cart_quotes import Fixture, NOW, OIL, line


class SupplierDateChecks(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('offline fixture')))
        temp = self.enterContext(tempfile.TemporaryDirectory())
        self.storage = ShopStorage(Path(temp), clock=lambda: NOW).provision(create=True)
        self.store = self.storage.order_store()
        fixture = Fixture()
        defaults = fixture.current()
        fixture.current = lambda: dict(defaults, mode='live')
        context = {'postcode':'74821','address_version':'synthetic-1','delivery':{'salutation':'Firma'}}
        quote = fixture.service_instance(mode='live').quote({'lines':[line(OIL)]}, customer_id='23',
            vehicle_ref='CAR-1', delivery_context=context)
        self.store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash, customer_id='23',
            mode='live', delivery_context=context)
        order = self.store.create_order(quote.quote_hash, customer_id='23', mode='live', idempotency_key='test-shop-eta')
        self.order = order['id']
        child, = ShopFulfillment._children(quote.snapshot())
        self.key = child['child_key']
        item = child['item']
        reference = 'SHOP-' + self.order[4:] + '-C-' + self.key[:24]
        digest = 'a' * 64
        position = dict(pos=1, quantity=item['quantity'], outcome='accepted', request_bound=True,
            order_number='1234', supplier_article_id=item['mapping']['supplier_article_id'],
            echo_confirmed=False, facts_source='submitted_request_under_external_policy')
        receipt = dict(reference=reference, request_hash=digest, outcome='accepted', positions=[position],
            echo_confirmed=False, fulfillment_confirmed=False)
        with closing(self.storage.connect()) as db, db:
            init_fulfillment_schema(db)
            db.execute("""INSERT INTO shop_fulfillment_v2
                (order_id,customer_id,mode,binding_json,plan_hash,lines_json,policy_json,created_at,updated_at)
                VALUES (?,'23','live','{}',?,'[]','{}',?,?)""", (self.order,digest,NOW.isoformat(),NOW.isoformat()))
            db.execute("""INSERT INTO shop_fulfillment_positions_v3
                (order_id,child_key,destination,item_json,reference,state,grant_used,request_hash,receipt_json,updated_at)
                VALUES (?,?,'customer',?,?,'accepted',1,?,?,?)""",
                (self.order,self.key,json.dumps(item),reference,digest,json.dumps(receipt),NOW.isoformat()))

        app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        app.config.update(TESTING=True, SECRET_KEY='only-for-tests')
        app.extensions['shop_production'] = {'storage':self.storage, 'store':self.store}
        register_shop_admin(app)
        self.client = app.test_client()
        self.token = 'A' * 40
        with self.client.session_transaction() as session:
            session.update(admin=True, csrf_token=self.token)
        self.url = f'/admin/shop/{self.order}/liefertermin/{self.key}'

    def save(self, day, version, **options):
        return self.client.post(self.url,
            data={'delivery_date':day, 'version':str(version), 'csrf_token':self.token}, **options)

    def dates(self):
        with closing(self.storage.connect()) as db:
            return delivery_dates(db, {DATE_TABLE}, self.order)

    def test_save_update_clear_and_stale_open_tab(self):
        self.assertEqual(self.dates(), {})
        self.assertEqual(self.save('2026-11-24', 0).status_code, 303)
        details = self.client.get('/admin/shop/' + self.order).get_data(as_text=True)
        self.assertIn('24.11.2026', details)
        self.assertIn('Bei TYREsystem bestellt', details)
        self.assertEqual(self.save('2026-11-25', 1).status_code, 303)
        self.assertEqual(self.save('2026-11-30', 1).status_code, 409)
        self.assertEqual(self.dates()[self.key], {'date':'2026-11-25', 'version':2})
        self.assertEqual(self.save('', 2).status_code, 303)
        self.assertEqual(self.save('2026-11-30', 0).status_code, 409)
        self.assertEqual(self.dates()[self.key], {'date':'', 'version':3})

    def test_invalid_stale_unbound_or_unauthorized_form_never_saves(self):
        self.assertEqual(self.save('2026-02-31', 0).status_code, 400)
        self.assertEqual(self.save('2026W145', 0).status_code, 400)
        self.assertEqual(self.save('2026-12-01', 0, headers={'Origin':'https://other.invalid'}).status_code, 403)
        with closing(self.storage.connect()) as db, db:
            db.execute("UPDATE shop_fulfillment_positions_v3 SET receipt_json='{}' WHERE order_id=?", (self.order,))
        self.assertEqual(self.save('2026-12-01', 0).status_code, 409)
        with self.client.session_transaction() as session:
            session.pop('csrf_token')
        self.assertEqual(self.save('2026-12-01', 0).status_code, 403)
        with self.client.session_transaction() as session:
            session.update(admin=False, csrf_token=self.token)
        self.assertEqual(self.save('2026-12-01', 0).status_code, 403)
        self.assertEqual(self.dates(), {})


if __name__ == '__main__':
    unittest.main()
