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
from shop_fulfillment import init_fulfillment_schema
from shop_order_review import init_order_review_schema
from shop_order_workflow import init_order_workflow_schema
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
        self.storage = storage
        self.enterContext(patch.dict(portal.app.extensions, shop_production={'storage': storage, 'store': self.store}))
        self.client = portal.app.test_client()
        with self.client.session_transaction(base_url=PORTAL) as session:
            session['admin'] = True
        day = date.today() + timedelta(days=10)
        if day.weekday() == 6:
            day += timedelta(days=1)
        self.day = day.isoformat()

    def order(self, customer, time, payment='paid', mode='live'):
        context = {'contact': {'name': 'Montage ' + customer, 'email': customer + '@example.invalid'}}
        value = quote(customer=customer, mode=mode, context=context)
        key = value['quote_hash']
        self.store.save_quote(value, expected_quote_hash=key, customer_id=customer, mode=mode,
                              delivery_context=context)
        consumer = ShopConsumer(self.store, self.storage.connect, mode=mode, public_origin=SHOP,
                                pack=pack(), enabled=True, clock=lambda: NOW)
        consumer.accept_quote(key, customer_id=customer, pack_hash=consumer.pack_hash, terms_accepted=True)
        guarded = ConsumerOrderStore(self.store, consumer)
        order = guarded.create_order(key, customer_id=customer, mode=mode, idempotency_key='order-' + customer,
                                     appointment_preference={'date': self.day, 'time': time} if time else None,
                                     appointment_calendar=portal.freie_reifen_slots)
        if payment:
            attempt = self.store.claim_payment(order['id'], customer_id=customer, mode=mode)
            binding = dict(session_id='cs_' + mode + '_' + customer, intent_id='pi_' + customer,
                           currency='eur', amount_gross_cent=order['amount_gross_cent'], mode=mode)
            self.store.bind_payment(order['id'], attempt['id'], customer_id=customer, **binding)
            self.store.apply_verified_event(dict(binding, order_id=order['id'], attempt_id=attempt['id'],
                event_id='evt_' + customer, kind=payment, occurred_at=NOW.isoformat()), verified=True)
        return '/admin/shop/' + order['id']

    def form(self, path, suffix='/termin-bestaetigen', label='Termin bestätigen'):
        response = self.client.get(path, base_url=PORTAL)
        self.assertEqual(response.status_code, 200)
        action = path + suffix
        form = re.search(r'<form\b[^>]*\baction="' + re.escape(action) + r'"[^>]*>(.*?)</form>',
                         response.text, flags=re.DOTALL)
        self.assertIsNotNone(form, f'{label} fehlt im Auftrag')
        self.assertIn(label, form[1])
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

    def assert_shop_count(self, count, path='/admin/cockpit'):
        page = self.client.get(path, base_url=PORTAL)
        self.assertEqual(page.status_code, 200)
        nav = re.search(r'<nav\b[^>]*\bid="werkstattNavigation"[^>]*>.*?</nav>', page.text, re.DOTALL)
        self.assertIsNotNone(nav)
        link = re.search(r'<a\b[^>]*\bhref="/admin/shop"[^>]*>(.*?)</a>', nav[0], re.DOTALL)
        self.assertIsNotNone(link)
        badge = re.search(r'<span\b([^>]*\bdata-shop-pending-count\b[^>]*)>\s*(\d+|–)\s*</span>', link[1])
        self.assertIsNotNone(badge)
        self.assertEqual(badge[2], str(count) if count is not None else '–')
        self.assertIn('data-count-url="/admin/shop/offen/anzahl"', badge[1])
        refresh = self.client.get('/admin/shop/offen/anzahl', base_url=PORTAL)
        self.assertIn('no-store', refresh.headers['Cache-Control'])
        if count is None:
            self.assertIn('nicht verfügbar', badge[1])
            self.assertEqual(refresh.status_code, 503)
        else:
            self.assertEqual('is-alert' in badge[1], count > 0)
            self.assertEqual(refresh.status_code, 200)
            self.assertEqual(refresh.json, {'count': count})

    def test_owner_opens_shop_from_second_sidebar_link(self):
        cockpit = self.client.get('/admin/cockpit', base_url=PORTAL)
        self.assertEqual(cockpit.status_code, 200)
        menu = re.search(r'<nav\b[^>]*\bid="werkstattNavigation"[^>]*>.*?</nav>', cockpit.text, re.DOTALL)
        self.assertIsNotNone(menu)
        direct = menu[0].split('id="hauptNavigation"', 1)[1].split('<details', 1)[0]
        links = re.findall(r'<a\b[^>]*\bhref="([^"]+)"[^>]*>([^<]+)', direct)
        self.assertEqual(links[0][1], 'Tagesübersicht')
        self.assertEqual((links[1][0], links[1][1].strip()), ('/admin/shop', 'Shopbestellungen'))
        self.assertEqual(menu[0].count('href="/admin/shop"'), 1)
        opened = self.client.get(links[1][0], base_url=PORTAL)
        self.assertEqual(opened.status_code, 200)
        self.assertIn('<h1>Shopbestellungen</h1>', opened.text)

        # Existing orders remain reachable from the calendar when sales are paused.
        with patch.dict(portal.app.config, SHOP_BUYER_ENABLED=False):
            calendar = self.client.get('/admin/kalender', base_url=PORTAL)
            self.assertEqual(calendar.status_code, 200)
            self.assertRegex(calendar.text, r'href="/admin/shop">\s*Shopbestellungen')

        partner = portal.get_autohaus_by_slug('kaesmann')['id']
        office = {'actor': 'employee', 'mitarbeiter_name': 'Testbüro', 'office_categories': ('preise',)}
        with patch.object(portal.office_access, 'identity', return_value=office):
            for path, identity in (('/werkstatt/buero', {'assistent_mid': 99}),
                                   ('/partner/kaesmann/dashboard', {'partner_autohaus_id': partner})):
                with self.subTest(path=path):
                    client = portal.app.test_client()
                    with client.session_transaction(base_url=PORTAL) as session:
                        session.update(identity)
                    page = client.get(path, base_url=PORTAL)
                    self.assertEqual(page.status_code, 200)
                    self.assertNotIn('href="/admin/shop"', page.text)
                    self.assertNotIn('data-shop-pending-count', page.text)
                    self.assertEqual(client.get('/admin/shop', base_url=PORTAL).status_code, 403)
                    self.assertEqual(client.get('/admin/shop/offen/anzahl', base_url=PORTAL).status_code, 403)

    def test_sidebar_counts_orders_until_workshop_marks_each_as_handled(self):
        # Use a different week from the calendar reservation tests sharing the portal fixture.
        self.day = (date.fromisoformat(self.day) + timedelta(days=7)).isoformat()
        authorized = self.order('queueauthorized', None, payment='authorized')
        paid = self.order('queuepaid', '10:00')
        self.order('queuecancel', None, payment='cancelled')
        self.order('queuedraft', None, payment=None)
        self.order('queuetest', None, mode='test')
        # Simulate an existing order journal predating the manual completion marker.
        oid = paid.rsplit('/', 1)[-1]
        with closing(self.storage.connect()) as db:
            db.execute('DROP TABLE IF EXISTS shop_workshop_handled_v1')
            init_fulfillment_schema(db)
            init_order_workflow_schema(db)
            db.execute('''INSERT INTO shop_order_workflow_v2
                (order_id,customer_id,mode,binding_json,state,created_at,updated_at)
                VALUES (?,?,'live','{}','fulfilled',?,?)''', (oid, 'queuepaid', NOW.isoformat(), NOW.isoformat()))
            db.execute('''INSERT INTO shop_fulfillment_v2
                (order_id,customer_id,mode,binding_json,plan_hash,lines_json,policy_json,
                 state,action_state,created_at,updated_at)
                VALUES (?,?,'live','{}','test-plan','[]','{}','accepted','captured',?,?)''',
                (oid, 'queuepaid', NOW.isoformat(), NOW.isoformat()))
            db.commit()
        self.assert_shop_count(2)
        for path in ('/admin/shop?per_page=1', paid, authorized):
            self.assertEqual(self.client.get(path, base_url=PORTAL).status_code, 200)
        self.assert_shop_count(2, '/admin/kalender')
        with closing(self.storage.connect()) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='shop_workshop_handled_v1'").fetchone())

        appointment, fields = self.form(paid)
        self.assertEqual(self.post(paid + '/bearbeitet', fields).status_code, 409)
        self.assertIn(self.post(appointment, fields).status_code, (302, 303))
        self.assert_shop_count(2)
        complete, fields = self.form(paid, '/bearbeitet', 'Als bearbeitet markieren')
        for _ in range(2):
            self.assertEqual(self.post(complete, fields).status_code, 303)
            self.assert_shop_count(1)
        with closing(self.storage.connect()) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM shop_workshop_handled_v1 WHERE order_id=?',
                                        (oid,)).fetchone()[0], 1)
        self.assert_shop_count(1)

    def test_queue_completion_requires_eligible_order_owner_and_valid_form(self):
        self.assert_shop_count(0)
        order = self.order('queueprotected', None)
        action, fields = self.form(order, '/bearbeitet', 'Als bearbeitet markieren')
        self.assertEqual(self.client.get(action, base_url=PORTAL).status_code, 405)
        self.assertIn(self.post(action, {'csrf_token': 'wrong'}).status_code, (400, 403))
        for role in ({'assistent_mid': 123}, {'partner_autohaus_id': portal.get_autohaus_by_slug('kaesmann')['id']}):
            with self.subTest(role=role):
                foreign = portal.app.test_client()
                with foreign.session_transaction(base_url=PORTAL) as session:
                    session.update(role, csrf_token=fields['csrf_token'])
                self.assertEqual(self.post(action, fields, client=foreign).status_code, 403)
        for customer, payment, mode in (('queueignoreddraft', None, 'live'),
                                        ('queueignoredcancel', 'cancelled', 'live'),
                                        ('queueignoredtest', 'paid', 'test')):
            with self.subTest(customer=customer):
                path = self.order(customer, None, payment=payment, mode=mode)
                self.assertNotIn(path + '/bearbeitet', self.client.get(path, base_url=PORTAL).text)
                self.assertIn(self.post(path + '/bearbeitet', fields).status_code, (404, 409))
        self.assert_shop_count(1)
        with patch.object(self.storage, '_paths', side_effect=OSError('synthetic shop journal unavailable')):
            self.assert_shop_count(None)
        self.assertEqual(self.post(action, fields).status_code, 303)
        self.assert_shop_count(0)

    def test_pending_review_and_supplier_workflow_count_the_same_order_once(self):
        path = self.order('queuereview', None, payment='authorized')
        oid = path.rsplit('/', 1)[-1]
        action, fields = self.form(path, '/bearbeitet', 'Als bearbeitet markieren')
        with closing(self.storage.connect()) as db:
            init_order_review_schema(db)
            init_order_workflow_schema(db)
            db.execute('''INSERT INTO shop_order_reviews_v1
                (order_id,customer_id,mode,binding_json,requirements_json,state,expires_at,created_at,updated_at)
                VALUES (?,'queuereview','live','{}','[]','pending',datetime('now','+2 days'),?,?)''',
                (oid, NOW.isoformat(), NOW.isoformat()))
            db.execute('''INSERT INTO shop_order_workflow_v2
                (order_id,customer_id,mode,binding_json,state,created_at,updated_at)
                VALUES (?,'queuereview','live','{}','awaiting_review',?,?)''', (oid, NOW.isoformat(), NOW.isoformat()))
            db.commit()
        self.assert_shop_count(1)
        self.assertNotIn(action, self.client.get(path, base_url=PORTAL).text)
        self.assertEqual(self.post(action, fields).status_code, 409)
        self.assert_shop_count(1)

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
