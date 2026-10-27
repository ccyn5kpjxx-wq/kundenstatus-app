"""Render current customer contacts using a temporary DB and denied network."""
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import smoke_test as isolated
from werkstatt_kontakt import BUSINESS_ADDRESS, DIRECTIONS_URL, PHONE, PHONE_E164, PHONE_URL

p = isolated.portal


class WorkshopContactTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        p.app.config['TESTING'] = True
        p.init_db()

    def setUp(self):
        self.client = p.app.test_client()

    def test_homepage_schema_display_and_call_target_agree(self):
        response = self.client.get('/homepage')
        self.assertEqual(response.status_code, 200)
        text = response.get_data(as_text=True)
        schema = json.loads(text.split('<script type="application/ld+json">', 1)[1].split('</script>', 1)[0])
        self.assertEqual(schema['telephone'], PHONE_E164)
        self.assertEqual(schema['address']['streetAddress'], 'Alte Neckarelzer Straße 18/1')
        self.assertEqual(schema['address']['postalCode'], '74821')
        self.assertEqual(schema['address']['addressLocality'], 'Mosbach')
        self.assertIn(f'href="{PHONE_URL}"', text)
        self.assertIn(PHONE, text)
        self.assertNotIn('Binauer', text)
        self.assertNotIn('1. November 2026', text)

    def test_rental_homepage_and_portal_preview_show_confirmed_contact(self):
        from rental_app import app as rental
        with patch.dict(rental.config, MOS_PUBLIC_LIVE_ENTRY_URL=None, MOS_PUBLIC_TEST_ENTRY_URL=None):
            responses = (rental.test_client().get('/'), self.client.get('/mietwagen-vorschau/'))
        for response in responses:
            self.addCleanup(response.close)
            self.assertEqual(response.status_code, 200)
            text = response.get_data(as_text=True)
            self.assertIn(PHONE, text)
            self.assertIn(PHONE_URL, text)
            self.assertIn(BUSINESS_ADDRESS, text)
            self.assertNotIn('Binauer', text)
            self.assertNotIn('Mosbach-Lohrbach', text)
            self.assertNotIn('tel:+4915227706694', text)
            self.assertNotIn('wa.me/4962611809260', text)

    def test_public_entrypoints_render_with_current_call_links(self):
        routes = ('/homepage', '/team', '/portale', '/privat', '/impressum', '/datenschutz',
                  '/leistungen/reifenservice', '/kunden-app/demo', '/mietwagen-info')
        for route in routes:
            with self.subTest(route=route):
                response = self.client.get(route)
                self.assertEqual(response.status_code, 200)
                text = response.get_data(as_text=True)
                self.assertIn(PHONE_URL if route not in ('/impressum', '/datenschutz') else PHONE, text)
                self.assertNotIn('tel:+4915227706694', text)
                self.assertNotIn('01522 7706694', text)
                self.assertNotIn('Binauer Höhe', text)
                self.assertNotIn('werkstatt_oeffentlich.', text)

    def test_anfrage_success_template_uses_current_number(self):
        with p.app.test_request_context('/anfrage'):
            text = p.render_template('homepage_anfrage.html', gesendet=True, formdata={},
                                     portal_links=p.homepage_portal_links())
        self.assertIn('Ihre Anfrage ist angekommen.', text)
        self.assertIn(f'href="{PHONE_URL}"', text)
        self.assertIn(PHONE, text)
        self.assertNotIn('tel:+4915227706694', text)

    def test_member_contact_and_whatsapp_are_preserved(self):
        for route in ('/team', '/geschaeftspartner'):
            with self.subTest(route=route):
                text = self.client.get(route).get_data(as_text=True)
                self.assertIn('tel:+4917660477486', text)
        for route in ('/privat', '/mietwagen-info'):
            with self.subTest(route=route):
                text = self.client.get(route).get_data(as_text=True)
                self.assertIn('https://wa.me/4915227706694', text)
                self.assertNotIn('wa.me/4962611809260', text)

    def test_customer_contact_keeps_mobile_whatsapp_independent(self):
        with patch.object(p, 'WHATSAPP_WORKSHOP_NUMBERS', ''), p.app.test_request_context('/'):
            contact = p.werkstatt_kundenkontakt({'fahrzeug': 'Synthetic car'})
        self.assertEqual(contact['adresse'], BUSINESS_ADDRESS)
        self.assertEqual(contact['telefon_url'], PHONE_URL)
        self.assertEqual(contact['telefon'], PHONE)
        self.assertTrue(contact['whatsapp_url'].startswith('https://wa.me/4915227706694?'))

    def test_directions_and_existing_google_profile_are_independent(self):
        self.assertEqual(parse_qs(urlparse(DIRECTIONS_URL).query)['destination'], [BUSINESS_ADDRESS])
        self.assertEqual(p.WERKSTATT_ANFAHRT_URL, DIRECTIONS_URL)
        profile = p.GOOGLE_MAPS_FALLBACK_URL
        self.assertIn('0x4797f50077c5e48b:0xb9c4f6b6b41be3b3', profile)
        with patch.object(p, 'get_app_setting', return_value=''):
            text = p.customer_fertig_whatsapp_message({'fahrzeug': 'Synthetic car'})
        self.assertIn(profile, text)
        self.assertIn(PHONE, text)
        self.assertNotIn('Binauer', text)

    def test_explicit_review_url_is_preserved(self):
        url = 'https://example.invalid/existing-review-profile'
        with patch.object(p, 'get_app_setting', return_value=url):
            text = p.customer_fertig_whatsapp_message({'fahrzeug': 'Synthetic car'})
        self.assertIn(url, text)

    def test_customer_error_page_keeps_call_and_whatsapp_channels(self):
        with p.app.test_request_context('/'):
            text = p.render_template_string(p.FEHLERSEITE_PORTAL_TEMPLATE, titel='Test', text='Test')
        self.assertIn(PHONE_URL, text)
        self.assertIn('https://wa.me/4915227706694', text)


if __name__ == '__main__':
    unittest.main(verbosity=2)
