"""Synthetic live-mode shop orders and the real workshop HTTP routes on isolated databases."""
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from flask import template_rendered
from werkzeug.datastructures import MultiDict

import smoke_test
from shop_consumer import ShopConsumer, init_consumer_schema
from shop_production_payments import ConsumerOrderStore
from shop_storage import ShopStorage
from test_shop_order_store import NOW, quote
from test_shop_production_payments import pack


portal = smoke_test.portal
PORTAL = 'https://kundenstatus-app.onrender.com'
SHOP = 'https://auto-lackierzentrum.de'


class ShopAppointmentConfirmation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING'] = True
        portal.init_db()

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        storage = ShopStorage(Path(folder.name), clock=lambda: NOW).provision(create=True)
        self.store = storage.order_store()
        with closing(storage.connect()) as db:
            init_consumer_schema(db)
        self.consumer = ShopConsumer(self.store, storage.connect, mode='live', public_origin=SHOP,
                                     pack=pack(), enabled=True, clock=lambda: NOW)
        self.enterContext(patch.dict(portal.app.extensions, shop_production={'storage': storage, 'store': self.store}))
        self.client = portal.app.test_client()
        with self.client.session_transaction(base_url=PORTAL) as session:
            session['admin'] = True
        day = date.today() + timedelta(days=10)
        if day.weekday() == 6:
            day += timedelta(days=1)
        self.day = day.isoformat()

    def order(self, customer, time, payment='paid'):
        context = {'contact': {'name': 'Montage ' + customer, 'email': customer + '@example.invalid'}}
        value = quote(customer=customer, mode='live', context=context)
        key = value['quote_hash']
        self.store.save_quote(value, expected_quote_hash=key, customer_id=customer, mode='live',
                              delivery_context=context)
        self.consumer.accept_quote(key, customer_id=customer, pack_hash=self.consumer.pack_hash, terms_accepted=True)
        guarded = ConsumerOrderStore(self.store, self.consumer)
        order = guarded.create_order(key, customer_id=customer, mode='live', idempotency_key='order-' + customer,
                                     appointment_preference={'date': self.day, 'time': time} if time else None,
                                     appointment_calendar=portal.freie_reifen_slots)
        if payment:
            attempt = self.store.claim_payment(order['id'], customer_id=customer, mode='live')
            binding = dict(session_id='cs_live_' + customer, intent_id='pi_' + customer,
                           currency='eur', amount_gross_cent=order['amount_gross_cent'], mode='live')
            self.store.bind_payment(order['id'], attempt['id'], customer_id=customer, **binding)
            self.store.apply_verified_event(dict(binding, order_id=order['id'], attempt_id=attempt['id'],
                event_id='evt_' + customer, kind=payment, occurred_at=NOW.isoformat()), verified=True)
        return '/admin/shop/' + order['id']

    def form(self, path):
        response = self.client.get(path, base_url=PORTAL)
        self.assertEqual(response.status_code, 200)
        action = path + '/termin-bestaetigen'
        form = re.search(r'<form\b[^>]*\baction="' + re.escape(action) + r'"[^>]*>(.*?)</form>',
                         response.text, flags=re.DOTALL)
        self.assertIsNotNone(form, 'Termin bestätigen fehlt im bezahlten Auftrag')
        self.assertIn('Termin bestätigen', form[1])
        tokens = re.findall(r'<input\b[^>]*\bname="csrf_token"[^>]*\bvalue="([^"]+)"', form[1])
        self.assertEqual(len(tokens), 1, 'Das gerenderte Formular muss genau einen gültigen Token senden')
        return action, MultiDict([('csrf_token', tokens[0])])

    def post(self, path, data, client=None):
        return (client or self.client).post(path, data=data, base_url=PORTAL,
                                           headers={'Origin': PORTAL, 'Sec-Fetch-Site': 'same-origin'})

    def context(self, path):
        result = {}
        def capture(sender, template, context, **extra):
            result.update(context)
        with template_rendered.connected_to(capture, portal.app):
            response = self.client.get(path, base_url=PORTAL)
        self.assertEqual(response.status_code, 200)
        return result

    def calendar_entries(self, path):
        days = self.context('/admin/kalender')['kalender_items']
        return [item for day in days if day['datum'].isoformat() == self.day
                for item in day['system'] if item.get('url') == path]

    def test_paid_order_confirmed_once_in_detail_list_calendar_and_cockpit(self):
        path = self.order('calendarpaid', '09:00')
        action, data = self.form(path)
        self.assertEqual(self.calendar_entries(path), [])
        for _ in range(2):
            result = self.post(action, data)
            self.assertIn(result.status_code, (302, 303))
            self.assertTrue(result.location.endswith(path))
        entries = self.calendar_entries(path)
        self.assertEqual(len(entries), 1)
        self.assertIn('09:00', entries[0]['titel'])
        month = self.context('/admin/cockpit?tag=' + self.day)['mini_calendar']
        self.assertEqual(sum(item.get('url') == path for item in month['selected_day']['items']), 1)
        self.assertNotIn('09:00', portal.freie_reifen_slots(self.day))
        detail = self.client.get(path, base_url=PORTAL).text
        self.assertNotIn(action, detail)
        self.assertRegex(detail, '(?i)termin bestätigt')
        self.assertRegex(self.client.get('/admin/shop', base_url=PORTAL).text, '(?i)termin bestätigt')

    def test_second_customer_cannot_take_another_confirmed_time(self):
        first = self.order('firstbuyer', '10:00')
        other = self.order('secondbuyer', '10:00')
        first_action, first_fields = self.form(first)
        other_action, other_fields = self.form(other)
        self.assertIn(self.post(first_action, first_fields).status_code, (302, 303))
        blocked = self.post(other_action, other_fields)
        if blocked.status_code in (302, 303):
            blocked = self.client.get(blocked.location, base_url=PORTAL)
        self.assertIn(blocked.status_code, (200, 409))
        self.assertRegex(blocked.text, '(?i)belegt|nicht mehr frei|bereits vergeben')
        self.assertEqual(len(self.calendar_entries(first)), 1)
        self.assertEqual(self.calendar_entries(other), [])

    def test_admin_csrf_and_order_eligibility_checked_before_booking(self):
        path = self.order('secretbuyer', '11:00')
        action, data = self.form(path)
        foreign = portal.app.test_client()
        with foreign.session_transaction(base_url=PORTAL) as session:
            session['partner_autohaus_id'] = portal.get_autohaus_by_slug('kaesmann')['id']
            session['csrf_token'] = data['csrf_token']
        self.assertEqual(self.post(action, data, client=foreign).status_code, 403)
        self.assertIn(self.post(action, {'csrf_token': 'invalid'}).status_code, (400, 403))
        self.assertEqual(self.calendar_entries(path), [])
        for customer, time, status in [('draftbuyer', '12:00', None), ('cancelbuyer', '13:00', 'cancelled'),
                                       ('withoutwish', None, 'paid')]:
            with self.subTest(customer=customer):
                path = self.order(customer, time, status)
                action = path + '/termin-bestaetigen'
                self.assertNotIn(action, self.client.get(path, base_url=PORTAL).text)
                self.assertEqual(self.post(action, data).status_code, 409)
                self.assertEqual(self.calendar_entries(path), [])


if __name__ == '__main__':
    unittest.main()
