"""Public prototype stays fictional, read-only and separate from private access."""
from unittest.mock import patch
import unittest
import smoke_test as isolated

portal = isolated.portal


class PublicDemoTests(unittest.TestCase):
    def setUp(self):
        portal.app.config['TESTING'] = True
        self.client = portal.app.test_client()

    def test_public_demo_uses_no_company_driver_or_order_lookup(self):
        with patch.object(portal.kundenflotten, 'company', side_effect=AssertionError('private lookup')), \
             patch.object(portal.kundenflotten, 'vehicle', side_effect=AssertionError('private lookup')), \
             patch.object(portal.kundenflotten, 'linked_cases', side_effect=AssertionError('private lookup')), \
             patch.object(portal, 'get_auftrag_by_kunden_status_token', side_effect=AssertionError('private lookup')):
            response = self.client.get('/kunden-app/demo?company_id=secret&token=secret',
                                       environ_base={'REMOTE_ADDR': '192.0.2.25'})
        self.assertEqual(response.status_code, 200)
        self.assertIn('Öffentliche App-Vorschau', response.text)
        self.assertIn('Hallo, Alex.', response.text)
        self.assertIn('MOS · DEMO', response.text)
        self.assertIn('kein Speichern oder Versand', response.text)
        self.assertNotIn('value="secret"', response.text)
        self.assertIn('noindex', response.headers['X-Robots-Tag'])
        self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')

    def test_public_demo_rejects_writes(self):
        self.client.get('/kunden-app/demo')
        for data in ({'beschreibung': 'must not be stored'},
                     isolated.csrf_data(self.client, {'beschreibung': 'must not be stored'})):
            response = self.client.post('/kunden-app/demo', data=data)
            self.assertIn(response.status_code, (400, 405))

    def test_existing_local_demo_stays_gated(self):
        old = portal.app.config.get('KUNDENFLOTTEN_DEMO_ENABLED', False)
        try:
            for enabled, remote, status in [(False, '127.0.0.1', 404),
                                             (True, '192.0.2.25', 404),
                                             (True, '127.0.0.1', 200)]:
                portal.app.config['KUNDENFLOTTEN_DEMO_ENABLED'] = enabled
                self.assertEqual(self.client.get('/kunden-app/vorschau',
                    environ_base={'REMOTE_ADDR': remote}).status_code, status)
        finally:
            portal.app.config['KUNDENFLOTTEN_DEMO_ENABLED'] = old

    def test_public_site_links_to_demo_on_portal_host(self):
        with patch.multiple(portal, PUBLIC_SITE_ONLY=True, PUBLIC_HOSTS={'site.example.test'},
                            PUBLIC_BASE_URL='https://site.example.test',
                            PORTAL_BASE_URL='https://portal.example.test'):
            response = self.client.get('/geschaeftspartner', base_url='https://site.example.test',
                                       environ_base={'REMOTE_ADDR': '192.0.2.25'})
            self.assertEqual(response.status_code, 200)
            self.assertIn('href="https://portal.example.test/kunden-app/demo"', response.text)
            self.assertNotIn('href="/kunden-app/vorschau"', response.text)
            self.assertEqual(self.client.get('/kunden-app/demo', base_url='https://site.example.test').status_code, 404)

    def test_public_demo_does_not_unlock_private_fleet_or_cases(self):
        self.assertEqual(self.client.get('/admin/kundenflotten').status_code, 302)
        self.assertEqual(self.client.get('/kunden-app/fall/not-a-real-token').status_code, 404)


if __name__ == '__main__':
    unittest.main(verbosity=2)
