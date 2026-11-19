"""Real order/quote/HTTP/admin integration on temporary synthetic shop databases."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import sys
import tempfile
from threading import Event
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from flask import Flask
from shop_admin import register_shop_admin
from shop_public_api import create_shop_public_blueprint
from shop_storage import ShopStorage
from test_shop_cart_quotes import Fixture, NOW, OIL, RIM, TYRE, line

ORIGIN = 'https://shop.example'
DAY = '2026-11-20'
WISH = {'date': DAY, 'time': '10:00'}
REQUESTED = {**WISH, 'status': 'requested'}
TYRE_MOUNT = line(TYRE, 4, services={'mounting': True})


class ShopAppointmentChecks(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.storage = ShopStorage(Path(folder.name), clock=lambda: NOW).provision(create=True)
        self.store = self.storage.order_store()
        self.calendar = Mock(return_value=['09:00', '10:00'])
        self.buyer = 'test-buyer'
        self.app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        self.app.config.update(TESTING=True, SECRET_KEY='shop-test', SHOP_PUBLIC_CATALOGUE_ENABLED=True,
                               SHOP_ORDER_DRAFT_ENABLED=True, SHOP_PUBLIC_ORIGIN=ORIGIN, SHOP_ORDER_MODE='test')
        self.app.extensions['shop_production'] = {'storage': self.storage, 'store': self.store}
        self.app.register_blueprint(create_shop_public_blueprint(order_store=self.store,
            authorize_quote=lambda req: self.buyer, appointment_calendar=self.calendar))
        register_shop_admin(self.app)
        self.client = self.app.test_client()

    def cart(self, *lines, workshop_delivery='collection'):
        context = {'postcode': '74821', 'contact': {'name': 'Testkunde', 'email': 'buyer@example.invalid'}}
        context['workshop_delivery'] = workshop_delivery
        quote = Fixture().service_instance().quote({'lines': list(lines)}, customer_id=self.buyer,
            delivery_context=context, vehicle_ref='CAR-1')
        self.store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash, customer_id=self.buyer,
                              mode='test', delivery_context=context)
        return {'quote_hash': quote.quote_hash, 'idempotency_key': 'order-' + quote.quote_hash}

    def submit(self, payload):
        return self.client.post('/shop/api/v2/orders', json=payload, base_url=ORIGIN, headers={'Origin': ORIGIN})

    def test_requested_order_is_visible_after_retry_to_customer_and_workshop(self):
        payload = self.cart(TYRE_MOUNT, line(OIL))
        response = self.submit({**payload, 'appointment_preference': WISH})
        self.assertEqual(response.status_code, 200, response.text)
        order = response.json['order']
        self.assertEqual(order['appointment_preference'], REQUESTED)
        self.calendar.return_value = []
        for retry in (payload, {**payload, 'appointment_preference': WISH}):
            self.assertEqual(self.submit(retry).json['order'], order)
        self.assertEqual(self.submit({**payload, 'appointment_preference': {**WISH, 'time': '09:00'}}).json['error'],
                         'appointment_preference_conflict')
        self.calendar.assert_called_once_with(DAY)
        path = '/shop/api/v2/orders/' + order['id']
        self.assertEqual(self.client.get(path, base_url=ORIGIN).json['order'], order)
        self.buyer = 'other-buyer'
        self.assertEqual(self.client.get(path, base_url=ORIGIN).status_code, 404)
        self.assertEqual(self.submit({**payload, 'appointment_preference': WISH}).status_code, 409)
        self.assertNotEqual(self.client.get('/admin/shop/' + order['id']).status_code, 200)
        with self.client.session_transaction() as session:
            session['admin'] = True
        for page in ('/admin/shop', '/admin/shop/' + order['id']):
            self.assertIn('20.11.2026, 10:00 Uhr', self.client.get(page).text)
        detail = self.client.get('/admin/shop/' + order['id']).text
        self.assertIn('Montagetermin angefragt', detail)
        self.assertIn('buyer@example.invalid', detail)
        self.assertNotIn('buyer@example.invalid', self.client.get('/admin/shop').text)
        with closing(self.storage.connect()) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM shop_payment_attempts_v2').fetchone()[0], 0)

    def test_slot_lookup_and_order_failure_leave_no_draft_or_request(self):
        lookup = '/shop/api/v2/appointment-slots?datum='
        self.assertEqual(self.client.get(lookup + DAY).json, {'zeiten': ['09:00', '10:00']})
        self.assertEqual(self.client.get(lookup + DAY + '&datum=' + DAY).status_code, 400)
        self.assertEqual(self.client.get(lookup + '2026-02-30').status_code, 400)
        payload = self.cart(TYRE_MOUNT)
        for wish in (None, {'date': DAY, 'time': '09:30'}, {**WISH, 'date': '2026-02-30'},
                     {**WISH, 'status': 'confirmed'}):
            self.assertEqual(self.submit({**payload, 'appointment_preference': wish}).status_code, 400)
        self.calendar.return_value = []
        self.assertEqual(self.submit({**payload, 'appointment_preference': WISH}).json['error'],
                         'appointment_slot_unavailable')
        self.calendar.side_effect = RuntimeError('synthetic calendar unavailable')
        self.assertEqual(self.submit({**payload, 'appointment_preference': WISH}).status_code, 503)
        self.assertEqual(self.client.get(lookup + DAY).json['error'], 'appointment_service_unavailable')
        with closing(self.storage.connect()) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM shop_orders_v2').fetchone()[0], 0)
        # Buying without a request remains possible even if the calendar is offline.
        order = self.submit(payload).json['order']
        self.assertNotIn('appointment_preference', order)
        self.assertEqual(self.submit({**payload, 'appointment_preference': WISH}).json['error'],
                         'appointment_preference_conflict')

    def test_only_server_priced_workshop_mounting_is_eligible(self):
        for lines in ([line(TYRE, 4)], [line(OIL)]):
            payload = self.cart(*lines)
            response = self.submit({**payload, 'appointment_preference': WISH})
            self.assertEqual((response.status_code, response.json['error']), (400, 'appointment_not_eligible'))
        self.calendar.assert_not_called()
        reshipment = self.cart(TYRE_MOUNT, workshop_delivery='customer_reshipment')
        response = self.submit({**reshipment, 'appointment_preference': WISH})
        self.assertEqual((response.status_code, response.json['error']), (400, 'appointment_not_eligible'))
        self.calendar.assert_not_called()
        group = self.cart(line(TYRE, 4, group='set', services={'mounting': True}), line(RIM, 4, group='set'))
        self.assertEqual(self.submit({**group, 'appointment_preference': WISH}).json['order']['appointment_preference'],
                         REQUESTED)

    def test_existing_storage_is_migrated_and_old_orders_remain_readable(self):
        payload = self.cart(TYRE_MOUNT)
        original = self.submit(payload).json['order']
        with closing(self.storage.connect()) as db:
            db.execute('DROP TABLE shop_order_appointment_requests_v2')
            db.commit()
        # Read-only admin also supports old journals when the selling release is disabled.
        with self.client.session_transaction() as session:
            session['admin'] = True
        self.assertEqual(self.client.get('/admin/shop/' + original['id']).status_code, 200)
        self.storage.provision(create=False)
        self.assertEqual(self.submit(payload).json['order'], original)
        second = self.cart(TYRE_MOUNT, line(OIL))
        self.assertEqual(self.submit({**second, 'appointment_preference': WISH}).json['order']['appointment_preference'],
                         REQUESTED)

    def test_concurrent_submissions_preserve_one_order_and_preference(self):
        payload = self.cart(TYRE_MOUNT)
        def create(_):
            return self.store.create_order(**payload, customer_id=self.buyer, mode='test',
                appointment_preference=WISH, appointment_calendar=self.calendar)
        with ThreadPoolExecutor(4) as pool:
            all_orders = list(pool.map(create, range(8)))
        self.assertTrue(all(order == all_orders[0] for order in all_orders))
        self.calendar.assert_called_with(DAY)
        with closing(self.storage.connect()) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM shop_order_appointment_requests_v2').fetchone()[0], 1)

    def test_slow_calendar_does_not_block_an_unrelated_order(self):
        with_wish, without = self.cart(TYRE_MOUNT), self.cart(line(OIL))
        reading, release = Event(), Event()
        def calendar(day):
            reading.set()
            if not release.wait(20):
                raise TimeoutError()
            return ['10:00']
        with ThreadPoolExecutor(2) as pool:
            future = pool.submit(self.store.create_order, **with_wish, customer_id=self.buyer, mode='test',
                                 appointment_preference=WISH, appointment_calendar=calendar)
            try:
                self.assertTrue(reading.wait(10))
                other = pool.submit(self.store.create_order, **without, customer_id=self.buyer, mode='test')
                self.assertNotIn('appointment_preference', other.result(timeout=8))
            finally:
                release.set()
            self.assertEqual(future.result(timeout=8)['appointment_preference'], REQUESTED)

    def test_independent_customers_can_request_the_same_hour_without_reserving_it(self):
        first = self.submit({**self.cart(TYRE_MOUNT), 'appointment_preference': WISH}).json['order']
        self.buyer = 'second-buyer'
        second = self.submit({**self.cart(TYRE_MOUNT), 'appointment_preference': WISH}).json['order']
        self.assertNotEqual(first['id'], second['id'])
        self.assertEqual(first['appointment_preference'], second['appointment_preference'])
        self.assertIn('10:00', self.client.get('/shop/api/v2/appointment-slots?datum=' + DAY).json['zeiten'])

    def test_failed_wish_insert_rolls_back_order_before_retry(self):
        payload = {**self.cart(TYRE_MOUNT), 'appointment_preference': WISH}
        with closing(self.storage.connect()) as db:
            db.execute("""CREATE TRIGGER synthetic_failure BEFORE INSERT ON shop_order_appointment_requests_v2
                          BEGIN SELECT RAISE(ABORT,'synthetic_failure'); END;""")
            db.commit()
        self.assertEqual(self.submit(payload).status_code, 503)
        with closing(self.storage.connect()) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM shop_orders_v2').fetchone()[0], 0)
            db.execute('DROP TRIGGER synthetic_failure')
            db.commit()
        self.assertEqual(self.submit(payload).json['order']['appointment_preference'], REQUESTED)


if __name__ == '__main__':
    unittest.main()
