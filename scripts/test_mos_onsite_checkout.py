"""Exercise the public pay-at-pickup path against the isolated portal inventory."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from html import unescape
from io import BytesIO
import json
import re
from tempfile import TemporaryDirectory
from threading import Barrier
import unittest
from unittest.mock import patch

from itsdangerous import URLSafeTimedSerializer
from pypdf import PdfReader

import test_mos_public_booking as public
from mos_public_contract import presign_quote, signed_payload
from mietwagen_checkout import validate_onsite_quote


class OnsiteCheckoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        directory = TemporaryDirectory(prefix='mos-onsite-checkout-')
        cls.addClassCleanup(directory.cleanup)
        portal = public.build_test_app(directory.name, origin='http://localhost')
        portal.app.extensions.pop('mos_public_booking', None)
        portal.app.config['MOS_PUBLIC_BOOKING']['slots'] = [
            (public.first_slot + timedelta(days=day)).astimezone(public.ZoneInfo('Europe/Berlin')).isoformat()
            for day in range(5)
        ]

    def setUp(self):
        self.public = public.PublicTests()
        self.public.setUp()
        self.slots = self.public.cfg['slots']
        self.service = public.portal.app.extensions['mos_public_booking']['service']

    def offer(self, method='onsite'):
        return self.public.post('/mietwagen-test/quote', {
            'vehicle': 'kona', 'start': self.slots[0], 'end': self.slots[3], 'payment_method': method})

    def token(self, method='onsite'):
        page = self.offer(method)
        self.assertEqual(page.status_code, 200)
        return unescape(re.search(r'name="quote_token" value="([^"]+)"', page.get_data(as_text=True))[1])

    def quote(self, token):
        signer = URLSafeTimedSerializer(public.portal.app.secret_key, salt='mos-public-quote-v1')
        return signer.loads(token)['quote']

    def test_direct_booking_creates_an_onsite_reservation_and_updates_public_availability(self):
        fields = {'vehicle':'kona', 'start':self.slots[0], 'end':self.slots[3], 'payment_method':'onsite'}
        page = self.public.client.get('/mietwagen-test/buchen', query_string=fields)
        self.assertEqual(page.status_code, 200)
        token = unescape(re.search(r'name="quote_token" value="([^"]+)"', page.get_data(as_text=True))[1])
        self.assertEqual(self.public.client.get('/mietwagen-test/availability', query_string=fields).json, {'available':True})
        reservation = self.public.checkout(token)
        self.assertEqual(reservation.status_code, 303)
        self.assertEqual(self.service.read(reservation.location.rsplit('/', 1)[1])['status'], 'confirmed')
        fields['payment_method'] = 'online'
        availability = self.public.client.get('/mietwagen-test/availability', query_string=fields)
        self.assertEqual(availability.json['reason'], 'occupied')
        self.assertEqual(self.public.client.get('/mietwagen-test/buchen', query_string=fields).status_code, 409)

    def test_online_checkout_hold_blocks_then_creates_calendar_reservation(self):
        fields = {'vehicle':'kona', 'start':self.slots[0], 'end':self.slots[3], 'payment_method':'online'}
        page = self.public.client.get('/mietwagen-test/buchen', query_string=fields)
        token = unescape(re.search(r'name="quote_token" value="([^"]+)"', page.get_data(as_text=True))[1])
        checkout = self.public.checkout(token)
        self.assertEqual(checkout.status_code, 303)
        check_url = '/mietwagen-test/availability'
        self.assertEqual(self.public.client.get(check_url, query_string=fields).json['reason'], 'occupied')
        confirmed = self.public.post(checkout.location)
        self.assertEqual(confirmed.status_code, 303)
        self.assertEqual(len(self.public.rows()), 1)
        self.assertEqual(self.public.client.get(check_url, query_string=fields).json['reason'], 'occupied')

    def test_confirms_once_without_stripe_and_blocks_both_payment_paths(self):
        token = self.token()
        q = self.quote(token)
        self.assertEqual((q['days'], q['rental_cents'], q['deposit_cents']), (3, 14700, 50000))
        self.assertEqual((q['amount_cents'], q['deposit_authorized_cents'], q['deposit_charged_cents']), (0, 0, 0))
        self.assertIn('Kartenterminal', q['pickup_payment_note'])
        self.assertIn(q['pickup_payment_note'], q['terms_text'])
        self.assertEqual(q['rules_version'], self.public.cfg['terms_version']+' + Vor-Ort-Zahlung v3')
        with (patch.object(self.service.gateway, 'create', side_effect=AssertionError('Unexpected Checkout')),
              patch.object(self.service.gateway, 'create_deposit_intent', side_effect=AssertionError('Unexpected deposit'))):
            first = self.public.checkout(token)
            self.assertEqual(first.status_code, 303)
            self.assertRegex(first.location, r'/status/[^/]+$')
            hold = self.service.read(first.location.rsplit('/', 1)[1])
            self.assertEqual((hold['status'], hold['session_id'], hold['payment_intent']), ('confirmed', None, None))
            self.assertEqual(self.public.rows()[0]['id'], hold['mietvorgang_id'])
            signed, _, _ = signed_payload(json.loads(hold['payload']))
            self.assertEqual(signed['payment_method'], 'onsite')
            self.assertEqual(signed['pickup_payment_note'], q['pickup_payment_note'])
            receipt = self.public.client.get(first.location + '/bestaetigung.txt')
            self.assertEqual(receipt.status_code, 200)
            self.assertIn(q['pickup_payment_note'], receipt.get_data(as_text=True))
            pdf = self.public.client.get(first.location + '/vertrag.pdf')
            self.assertEqual(pdf.status_code, 200)
            text = ' '.join(page.extract_text() or '' for page in PdfReader(BytesIO(pdf.data)).pages)
            self.assertIn('Kartenterminal', text)
            self.assertEqual(self.public.checkout(token).location, first.location)
            self.assertEqual(len(self.public.rows()), 1)
            self.service.cancel_or_reconcile(hold['id'])
            self.assertEqual(self.service.read(hold['id'])['status'], 'confirmed')
            with self.assertRaises(ValueError):
                self.service.create_checkout(hold['id'])
            with self.assertRaises(ValueError):
                self.service.prepare_deposit(hold['id'])
        self.assertEqual(self.offer('online').status_code, 409)
        self.assertEqual(self.offer().status_code, 409)

    def test_unavailable_slot_changed_method_and_tampered_amount(self):
        self.assertEqual(self.offer('unexpected').status_code, 409)
        token = self.token()
        q = self.quote(token)
        for altered in ({'amount_cents': 1}, {'deposit_authorized_cents': 50000},
                        {'deposit_charged_cents': 50000}, {'deposit_cents': 0}, {'deposit_timing':'before_pickup'}):
            with self.subTest(altered=altered), self.assertRaises(ValueError):
                validate_onsite_quote({**q, **altered})
        mismatch = self.public.post('/mietwagen-test/checkout', {
            'quote_token': token, 'payment_method': 'online', 'accept':'yes', 'sign_confirm':'yes',
            'name':'Test','email':'test@example.invalid','signature_data':self.public.signature()})
        self.assertEqual(mismatch.status_code, 409)
        db = public.portal.get_db()
        try:
            db.execute('UPDATE miet_checkout_slots SET active=0 WHERE slot=?', (self.slots[0],))
            db.commit()
            self.assertEqual(self.public.checkout(token).status_code, 409)
            self.assertEqual(self.public.rows(), [])
        finally:
            db.execute('UPDATE miet_checkout_slots SET active=1 WHERE slot=?', (self.slots[0],))
            db.commit()
            db.close()

    def test_overlapping_submissions_share_the_same_vehicle_lock(self):
        customer = {'name':'Test','email':'test@example.invalid','telefon':''}
        offers = [presign_quote(self.quote(self.token(method)), customer, self.public.signature())
                  for method in ('onsite', 'online')]
        start_both = Barrier(2)
        def submit(q):
            start_both.wait(timeout=15)
            try:
                return self.service.reserve('parallel-request-' + q.get('payment_method','online'),
                                            q['vehicle_id'], q['start_slot'][:10], q['end_slot'][:10], customer, q)
            except ValueError as error:
                self.assertIn('belegt', str(error))
                return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(submit, offers))
        self.assertEqual(sum(r is not None for r in results), 1)
        self.assertEqual(len(self.public.rows()), int(results[0] is not None))


if __name__ == '__main__':
    unittest.main()
