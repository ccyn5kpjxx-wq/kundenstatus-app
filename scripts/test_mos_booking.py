"""Offline tests only: isolated SQLite, synthetic IDs, blocked external sockets."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mos_booking.app import create_app
from mos_booking.gateway import OfflineGateway, StripeTestGateway, verified_event
from mos_booking.service import BookingService, BookingError, Conflict, ProviderUnavailable

BASE = 'http://127.0.0.1:5084'


class BookingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'test.mos-test.sqlite3'
        self.now = time.time()
        self.secret = 'whsec_only_a_synthetic_test_secret'
        self.gateway = OfflineGateway(self.path, BASE, self.secret, clock=lambda: self.now)
        self.service = BookingService(self.path, self.gateway, BASE, clock=lambda: self.now)
        self.service.seed_demo()
        first = date.today() + timedelta(days=7)
        self.start, self.end = first.isoformat(), (first + timedelta(days=3)).isoformat()

    def create(self, owner='owner', key='request-00000001', vehicle_id='test-c3', **kw):
        return self.service.create(owner, key, vehicle_id, kw.get('start', self.start), kw.get('end', self.end), kw.get('km', 501))

    def sid(self, bid):
        with self.service.db() as db:
            return db.execute('SELECT session_id FROM bookings WHERE id=?', (bid,)).fetchone()[0]

    def paid(self, b, eid=None):
        sid = self.sid(b['id'])
        self.gateway.pay(sid)
        body, sig = self.gateway.signed_event(sid, event_id=eid)
        event = verified_event(body, sig, self.secret)
        self.service.handle_event(event)
        return event

    def test_quote_uses_server_tariff_and_integer_cents(self):
        b = self.create()
        self.assertEqual(b['quote']['total_cents'], 12975)  # 3*39 EUR + 51*0.25
        kona = self.create(owner='other', vehicle_id='test-kona')
        self.assertEqual(kona['quote']['total_cents'], 15975)

    def test_unknown_vehicle_and_abo_not_bookable(self):
        for vehicle in ('Hyundai i10', '123', 'auto-abo'):
            with self.assertRaises(BookingError):
                self.create(vehicle_id=vehicle)

    def test_changed_quote_requires_new_confirmation(self):
        q = self.service.preview_quote('test-c3', self.start, self.end, 0)
        with self.service.db(True) as db:
            db.execute("UPDATE vehicles SET day_cents=4000,long_cents=4000 WHERE id='test-c3'")
        with self.assertRaises(Conflict):
            self.service.create('owner', 'request-00000001', 'test-c3', self.start, self.end, 0, q['quote_hash'])
        with self.service.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM bookings').fetchone()[0], 0)

    def test_invalid_dates_kilometres(self):
        for start, end, km in [(self.end, self.start, 0), ('bad', self.end, 0),
                               ('2000-01-01', '2000-01-02', 0), (self.start, self.end, -1),
                               (self.start, self.end, 1.5), (self.start, self.end, True)]:
            with self.assertRaises(BookingError):
                self.create(start=start, end=end, km=km)

    def test_one_day_discount_and_dst(self):
        with self.service.db() as db:
            vehicle = dict(db.execute("SELECT * FROM vehicles WHERE id='test-kona'").fetchone())
        self.now = 1767225600  # 2026-01-01
        self.assertEqual(self.service.quote(vehicle, '2026-10-24', '2026-10-26', 0)['total_cents'], 11800)
        self.assertEqual(self.service.quote(vehicle, '2026-03-28', '2026-03-30', 0)['days'], 2)
        self.assertEqual(self.service.quote(vehicle, '2026-10-24', '2026-10-24', 0)['total_cents'], 5900)

    def test_concurrent_competing_bookings_only_one_succeeds(self):
        def attempt(i):
            try:
                return self.create(owner=str(i))['id']
            except Conflict:
                return None
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(attempt, range(8)))
        self.assertEqual(sum(x is not None for x in results), 1)

    def test_concurrent_same_request_one_session(self):
        with ThreadPoolExecutor(max_workers=5) as pool:
            results = list(pool.map(lambda _: self.create(), range(5)))
        self.assertEqual(len({r['id'] for r in results}), 1)
        self.assertEqual(len({r['checkout_url'] for r in results}), 1)

    def test_request_key_cannot_change_and_owner_cannot_read_other_booking(self):
        b = self.create()
        with self.assertRaises(Conflict):
            self.create(vehicle_id='test-i10')
        with self.assertRaises(BookingError):
            self.service.get(b['id'], 'intruder')

    def test_return_day_occupied_next_day_free(self):
        self.create()
        later = (date.fromisoformat(self.end) + timedelta(days=1)).isoformat()
        with self.assertRaises(Conflict):
            self.create(owner='b', start=self.end, end=later)
        self.create(owner='c', start=later, end=later)

    def test_provider_timeout_keeps_hold_and_safe_retry(self):
        real = self.gateway.create
        def lost_response(params, key):
            real(params, key)
            raise TimeoutError('response lost after provider create')
        with patch.object(self.gateway, 'create', side_effect=lost_response):
            with self.assertRaises(ProviderUnavailable):
                self.create()
        with self.assertRaises(Conflict):
            self.create(owner='other')
        b = self.create()
        self.assertEqual(b['state'], 'checkout_open')
        with self.service.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM demo_sessions').fetchone()[0], 1)

    def test_unknown_creation_after_timeout_does_not_auto_release(self):
        with patch.object(self.gateway, 'create', side_effect=TimeoutError):
            with self.assertRaises(ProviderUnavailable):
                self.create()
        self.now += 4000
        self.service.sweep()
        with self.assertRaises(Conflict):
            self.create(owner='other')

    def test_local_expiry_alone_never_frees_stock(self):
        self.create()
        self.now += 4000
        with patch.object(self.gateway, 'retrieve', side_effect=TimeoutError):
            self.service.sweep()
        with self.assertRaises(Conflict):
            self.create(owner='other')

    def test_provider_confirmed_expiry_releases_stock(self):
        b = self.create()
        self.now += 4000
        self.service.sweep()
        self.assertEqual(self.service.get(b['id'], 'owner')['state'], 'expired')
        self.create(owner='other')

    def test_cancel_expire_before_release_and_no_cancel_of_paid(self):
        b = self.create()
        self.service.reconcile(b['id'], cancel=True)
        self.assertEqual(self.gateway.retrieve(self.sid(b['id']))['status'], 'expired')
        self.assertEqual(self.service.get(b['id'], 'owner')['state'], 'expired')
        b2 = self.create(owner='other')
        self.paid(b2)
        self.service.reconcile(b2['id'], cancel=True)
        self.assertEqual(self.service.get(b2['id'], 'other')['state'], 'confirmed')

    def test_payment_signed_idempotent_parallel_and_order_independent(self):
        b = self.create()
        event = self.paid(b, 'evt_same')
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.service.handle_event(event), range(8)))
        body, sig = self.gateway.signed_event(self.sid(b['id']), 'checkout.session.expired')
        self.service.handle_event(verified_event(body, sig, self.secret))
        got = self.service.get(b['id'], 'owner')
        self.assertEqual((got['state'], got['paid']), ('confirmed', 1))
        with self.service.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM bookings').fetchone()[0], 1)

    def test_webhook_before_checkout_creation_response(self):
        real = self.gateway.create
        def early_event(params, key):
            result = real(params, key)
            self.gateway.pay(result['id'])
            body, sig = self.gateway.signed_event(result['id'])
            self.service.handle_event(verified_event(body, sig, self.secret))
            return result
        with patch.object(self.gateway, 'create', side_effect=early_event):
            b = self.create()
        self.assertEqual((b['state'], b['paid']), ('confirmed', 1))

    def test_same_payment_intent_never_fulfils_two_bookings(self):
        first, second = self.create(), self.create(owner='second', vehicle_id='test-i10')
        self.paid(first)
        pi = self.gateway.retrieve(self.sid(first['id']))['payment_intent']
        sid = self.sid(second['id'])
        self.gateway.change(sid, lambda d: d.update(status='complete', payment_status='paid', payment_intent=pi))
        body, sig = self.gateway.signed_event(sid)
        self.service.handle_event(verified_event(body, sig, self.secret))
        self.assertEqual(self.service.get(second['id'], 'second')['state'], 'review')

    def test_payment_wins_cancel_race(self):
        b = self.create()
        original = self.gateway.expire
        def racing_expire(sid):
            self.gateway.pay(sid)
            return original(sid)
        with patch.object(self.gateway, 'expire', side_effect=racing_expire):
            self.service.reconcile(b['id'], cancel=True)
        self.assertNotEqual(self.service.get(b['id'], 'owner')['state'], 'expired')
        body, sig = self.gateway.signed_event(self.sid(b['id']))
        self.service.handle_event(verified_event(body, sig, self.secret))
        self.assertEqual(self.service.get(b['id'], 'owner')['state'], 'confirmed')

    def test_unpaid_complete_is_not_confirmed(self):
        b = self.create()
        sid = self.sid(b['id'])
        self.gateway.change(sid, lambda d: d.update(status='complete'))
        body, sig = self.gateway.signed_event(sid)
        self.service.handle_event(verified_event(body, sig, self.secret))
        got = self.service.get(b['id'], 'owner')
        self.assertEqual((got['state'], got['paid']), ('payment_pending', 0))
        self.now += 4000
        self.service.sweep()
        with self.assertRaises(Conflict):
            self.create(owner='other')

    def test_mismatched_amount_currency_metadata_live_mode_rejected(self):
        b = self.create()
        sid = self.sid(b['id'])
        original = self.gateway.retrieve(sid)
        for update in ({'amount_total': 1}, {'currency': 'usd'}, {'metadata': {}}, {'livemode': True}):
            self.gateway.change(sid, lambda d: d.update(update))
            body, sig = self.gateway.signed_event(sid)
            with self.assertRaises(BookingError):
                self.service.handle_event(verified_event(body, sig, self.secret))
            self.gateway.change(sid, lambda d: d.update(original))
        self.assertEqual(self.service.get(b['id'], 'owner')['paid'], 0)

    def test_late_paid_event_after_release_goes_to_review(self):
        b = self.create()
        sid = self.sid(b['id'])
        self.service.reconcile(b['id'], cancel=True)
        other = self.create(owner='other')
        # Deliberately adversarial provider state: must never double-confirm.
        self.gateway.change(sid, lambda d: d.update(status='complete', payment_status='paid', payment_intent='pi_late'))
        body, sig = self.gateway.signed_event(sid)
        self.service.handle_event(verified_event(body, sig, self.secret))
        self.assertEqual(self.service.get(b['id'], 'owner')['state'], 'review')
        self.assertEqual(self.service.get(b['id'], 'owner')['paid'], 1)
        self.assertEqual(self.service.get(other['id'], 'other')['state'], 'checkout_open')

    def test_maintenance_after_hold_requires_review(self):
        b = self.create()
        with self.service.db(True) as db:
            db.execute("UPDATE vehicles SET active=0 WHERE id='test-c3'")
        self.paid(b)
        self.assertEqual(self.service.get(b['id'], 'owner')['state'], 'review')

    def test_signature_invalid_stale_tampered_and_webhook_retry(self):
        b = self.create()
        sid = self.sid(b['id'])
        body, sig = self.gateway.signed_event(sid)
        for raw, signature in [(body + b' ', sig), (body, 't=1,v1=wrong'), (body, '')]:
            with self.assertRaises(Exception):
                verified_event(raw, signature, self.secret)
        with patch('stripe._webhook.time.time', return_value=time.time()+400):
            with self.assertRaises(Exception):
                verified_event(body, sig, self.secret)
        event = verified_event(body, sig, self.secret)
        with patch.object(self.gateway, 'retrieve', side_effect=TimeoutError):
            with self.assertRaises(ProviderUnavailable):
                self.service.handle_event(event)
        with self.service.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM events').fetchone()[0], 0)

    def test_stripe_adapter_rejects_live_and_uses_idempotency(self):
        with self.assertRaises(ValueError):
            StripeTestGateway('sk_live_forbidden')
        with patch('mos_booking.gateway.stripe.StripeClient') as constructor:
            adapter = StripeTestGateway('sk_test_synthetic_no_network')
            api = constructor.return_value.v1.checkout.sessions
            api.create.return_value.to_dict.return_value = {'url': 'https://checkout.stripe.com/c/pay/cs_test_fake'}
            adapter.create({'mode': 'payment'}, 'request-1')
            api.create.assert_called_once_with({'mode': 'payment'}, options={'idempotency_key': 'request-1'})


class FlaskTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = {'TESTING': True, 'MODE': 'offline', 'DATABASE': str(Path(self.tmp.name)/'app.mos-test.sqlite3'),
                       'BASE_URL': BASE, 'SECRET_KEY': 'unit-session', 'ADMIN_TOKEN': 'unit-admin'}
        self.app = create_app(self.config)
        self.client = self.app.test_client()
        self.client.get('/', base_url=BASE)
        with self.client.session_transaction(base_url=BASE) as s:
            self.csrf = s['csrf']
        self.headers = {'X-CSRF-Token': self.csrf, 'Idempotency-Key': 'request-00000001'}
        first = date.today()+timedelta(days=7)
        self.data = {'vehicle_id': 'test-i10', 'start': first.isoformat(), 'end': (first+timedelta(days=2)).isoformat(), 'km': 0}
        quote = self.post('/api/quote', json=self.data).json
        self.data['expected_quote_hash'] = quote['quote_hash']

    def post(self, url, **kwargs):
        return self.client.post(url, base_url=BASE, headers=self.headers, **kwargs)

    def test_disabled_live_foreign_host_and_wrong_database(self):
        self.assertEqual(create_app({'MODE': 'off'}).test_client().get('/').status_code, 404)
        for patch_config in ({'MODE': 'live'}, {'DATABASE': 'data/auftraege.db'}, {'BASE_URL': 'https://example.com'}):
            with self.assertRaises(ValueError):
                create_app({**self.config, **patch_config})
        self.assertEqual(self.client.get('/', base_url='http://attacker.example').status_code, 403)
        self.assertEqual(self.client.get('/', base_url=BASE, environ_overrides={'REMOTE_ADDR': '192.168.1.1'}).status_code, 403)

    def test_csrf_client_price_and_admin_access(self):
        self.assertEqual(self.client.post('/api/checkout', base_url=BASE, json=self.data).status_code, 403)
        self.assertEqual(self.post('/api/checkout', json={**self.data, 'amount': 1}).status_code, 400)
        self.assertEqual(self.post('/api/checkout', json={'vehicle_id': []}).status_code, 400)
        page = self.client.get('/admin', base_url=BASE).get_data(as_text=True)
        self.assertIn('Test-Admin-Passwort', page)
        self.assertEqual(self.post('/admin', data={'password': 'wrong'}).status_code, 403)
        self.assertEqual(self.post('/admin', data={'password': 'unit-admin'}).status_code, 303)
        self.assertIn('Noch keine Testbuchungen', self.client.get('/admin', base_url=BASE).get_data(as_text=True))

    def test_redirect_does_not_confirm_and_signed_webhook_does(self):
        response = self.post('/api/checkout', json=self.data)
        self.assertEqual(response.status_code, 200)
        b = response.json
        self.client.get('/booking/'+b['id']+'?paid=true', base_url=BASE)
        self.assertEqual(self.client.get('/api/bookings/'+b['id'], base_url=BASE).json['paid'], 0)
        outsider = self.app.test_client()
        self.assertEqual(outsider.get('/api/bookings/'+b['id'], base_url=BASE).status_code, 400)
        service = self.app.extensions['booking']
        sid = b['checkout_url'].split('/')[-1]
        service.gateway.pay(sid)
        body, sig = service.gateway.signed_event(sid)
        self.assertEqual(self.client.post('/webhooks/stripe', base_url=BASE, data=body, headers={'Stripe-Signature': 'bad'}).status_code, 400)
        for _ in range(2):
            self.assertEqual(self.client.post('/webhooks/stripe', base_url=BASE, data=body, headers={'Stripe-Signature': sig}).status_code, 204)
        self.assertEqual(self.client.get('/api/bookings/'+b['id'], base_url=BASE).json['state'], 'confirmed')

    def test_simulation_flow_and_cancel_redirect(self):
        b = self.post('/api/checkout', json=self.data).json
        path = '/simulate/'+b['checkout_url'].split('/')[-1]
        self.assertIn('keine Kartendaten', self.client.get('/', base_url=BASE).get_data(as_text=True))
        self.assertEqual(self.post(path).status_code, 303)
        self.assertEqual(self.client.get('/api/bookings/'+b['id'], base_url=BASE).json['state'], 'confirmed')


if __name__ == '__main__':
    def denied(*args, **kwargs):
        raise AssertionError('External network is forbidden in booking tests')
    with patch('socket.socket.connect', denied), patch('socket.create_connection', denied):
        unittest.main(verbosity=2)
