"""Personal material login through the real app, isolated DB and no network."""
import unittest
from unittest.mock import patch

import test_assistent as assistant_fixture

p = assistant_fixture.p


class MaterialLoginTests(unittest.TestCase):
    def setUp(self):
        self.fixture = assistant_fixture.AssistantTests('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.client = p.app.test_client()
        with self.client.session_transaction() as state:
            state['csrf_token'] = 'synthetic-form-token'

    def login(self, **changes):
        data = dict(mitarbeiter_id='1', password='test-passwort-123',
                    next='/werkstatt/materialbestellung', csrf_token='synthetic-form-token')
        data.update(changes)
        return self.client.post('/werkstatt/assistent/login', data=data)

    def test_expired_form_returns_get_page_with_new_token_and_visible_message(self):
        response = self.login(csrf_token='stale-token')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/werkstatt/materialbestellung')
        with self.client.session_transaction() as state:
            self.assertNotIn('assistent_mid', state)
        page = self.client.get(response.location)
        self.assertEqual(page.status_code, 200)
        self.assertIn('Anmeldeseite war veraltet', page.text)
        self.assertIn('role="alert"', page.text)
        with self.client.session_transaction() as state:
            self.assertNotEqual(state['csrf_token'], 'synthetic-form-token')

    def test_wrong_password_returns_personal_form_and_keeps_identity_absent(self):
        response = self.login(password='synthetic-wrong-password')
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.location, '/werkstatt/materialbestellung')
        page = self.client.get(response.location)
        self.assertEqual(page.status_code, 200)
        self.assertIn('role="alert"', page.text)
        self.assertIn('Persönliches Passwort', page.text)
        self.assertNotEqual(page.mimetype, 'application/json')
        with self.client.session_transaction() as state:
            self.assertNotIn('assistent_mid', state)

    def test_admin_text_is_not_an_employee_or_an_exception_page(self):
        response = self.login(mitarbeiter_id='admin')
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.location, '/werkstatt/materialbestellung')
        self.assertIn('role="alert"', self.client.get(response.location).text)
        with self.client.session_transaction() as state:
            self.assertNotIn('assistent_mid', state)

    def test_real_personal_login_opens_form_without_admin_impersonation(self):
        response = self.login()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/werkstatt/materialbestellung')
        page = self.client.get(response.location)
        self.assertEqual(page.status_code, 200)
        self.assertIn('id="materialbestellung"', page.text)
        with self.client.session_transaction() as state:
            self.assertEqual(state['assistent_mid'], 1)
            self.assertNotIn('admin', state)
            self.assertNotEqual(state['csrf_token'], 'synthetic-form-token')

    def test_expired_external_next_does_not_redirect_or_authenticate(self):
        response = self.login(csrf_token='stale-token', next='https://untrusted.example')
        self.assertEqual(response.status_code, 400)
        self.assertIsNone(response.location)
        with self.client.session_transaction() as state:
            self.assertNotIn('assistent_mid', state)

    def test_rate_limit_has_a_visible_form_message_and_no_identity(self):
        with patch.object(p, 'login_rate_limit_status', return_value=(True, None)):
            response = self.login()
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.location, '/werkstatt/materialbestellung')
        self.assertIn('role="alert"', self.client.get(response.location).text)
        with self.client.session_transaction() as state:
            self.assertNotIn('assistent_mid', state)


if __name__ == '__main__':
    unittest.main()
