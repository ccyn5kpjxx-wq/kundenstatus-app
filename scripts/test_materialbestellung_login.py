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

    def test_personal_app_start_keeps_profile_destination_through_login(self):
        response = self.client.get('/werkstatt/mein-konto')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/werkstatt/materialbestellung?next=profil')
        page = self.client.get(response.location)
        self.assertEqual(page.status_code, 200)
        self.assertIn('name="next" value="/werkstatt/mein-konto"', page.text)
        self.assertIn('Anmelden und Portal öffnen', page.text)
        self.assertIn('DEINE WERKSTATT. DEIN PORTAL.', page.text)
        response = self.login(next='/werkstatt/mein-konto')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/werkstatt/mein-konto')
        with patch('werkstatt_mitarbeiter_portal.render_template', return_value='synthetic-personal-profile') as render:
            self.assertEqual(self.client.get(response.location).status_code, 200)
        self.assertEqual(render.call_args.kwargs['employee']['id'], 1)
        with self.client.session_transaction() as state:
            self.assertEqual(state['assistent_mid'], 1)
            self.assertNotIn('admin', state)

    def test_profile_password_error_returns_to_profile_login_not_material(self):
        response = self.login(next='/werkstatt/mein-konto', password='synthetic-wrong-password')
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.location, '/werkstatt/materialbestellung?next=profil')
        page = self.client.get(response.location)
        self.assertIn('role="alert"', page.text)
        self.assertIn('name="next" value="/werkstatt/mein-konto"', page.text)
        with self.client.session_transaction() as state:
            self.assertNotIn('assistent_mid', state)

    def test_expired_profile_form_preserves_destination_and_renews_csrf(self):
        response = self.login(next='/werkstatt/mein-konto', csrf_token='stale-token')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/werkstatt/materialbestellung?next=profil')
        page = self.client.get(response.location)
        self.assertIn('Anmeldeseite war veraltet', page.text)
        self.assertIn('name="next" value="/werkstatt/mein-konto"', page.text)
        with self.client.session_transaction() as state:
            self.assertNotIn('assistent_mid', state)
            self.assertNotEqual(state['csrf_token'], 'synthetic-form-token')

    def test_profile_rate_limit_returns_visible_profile_form(self):
        with patch.object(p, 'login_rate_limit_status', return_value=(True, None)):
            response = self.login(next='/werkstatt/mein-konto')
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.location, '/werkstatt/materialbestellung?next=profil')
        self.assertIn('Zu viele Fehlversuche', self.client.get(response.location).text)

    def test_login_destination_is_a_fixed_whitelist(self):
        for target in ('https://untrusted.example', '//untrusted.example',
                       '/admin', '/werkstatt/mein-konto?mitarbeiter_id=2', 'profil'):
            with self.subTest(target=target):
                fresh = p.app.test_client()
                with fresh.session_transaction() as state:
                    state['csrf_token'] = 'synthetic-form-token'
                page = fresh.get('/werkstatt/materialbestellung', query_string={'next': target})
                expected = '/werkstatt/mein-konto' if target == 'profil' else '/werkstatt/materialbestellung'
                self.assertIn(f'name="next" value="{expected}"', page.text)
                response = fresh.post('/werkstatt/assistent/login', data={
                    'mitarbeiter_id': '1', 'password': 'test-passwort-123',
                    'csrf_token': 'synthetic-form-token', 'next': target})
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.location, '/werkstatt/assistent')

    def test_assistant_login_without_next_keeps_existing_json_errors_and_success_target(self):
        data = dict(mitarbeiter_id='1', password='synthetic-wrong-password',
                    csrf_token='synthetic-form-token')
        response = self.client.post('/werkstatt/assistent/login', data=data)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.mimetype, 'application/json')
        self.assertIsNone(response.location)
        data['password'] = 'test-passwort-123'
        response = self.client.post('/werkstatt/assistent/login', data=data)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/werkstatt/assistent')

    def test_shared_admin_and_remembered_logins_cannot_survive_personal_login(self):
        with p.app.test_request_context('/'):
            admin_token = p.create_remember_login_token('admin')
            partner_token = p.create_remember_login_token('partner', autohaus_id=7)
        self.client.set_cookie(p.ADMIN_REMEMBER_COOKIE, admin_token, domain='localhost')
        self.client.set_cookie(p.PARTNER_REMEMBER_COOKIE, partner_token, domain='localhost')
        with self.client.session_transaction() as state:
            state.update(admin=True, partner_autohaus_id=7, assistent_mid=2,
                         assistent_version=4, assistent_auth_version=3,
                         assistent_bestaetigung={'id': 999, 'actor': 'admin'})
        response = self.login()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, '/werkstatt/materialbestellung')
        cookies = response.headers.getlist('Set-Cookie')
        for name in (p.ADMIN_REMEMBER_COOKIE, p.PARTNER_REMEMBER_COOKIE):
            self.assertTrue(any(item.startswith(name + '=') and 'Max-Age=0' in item for item in cookies))
            self.assertIsNone(self.client.get_cookie(name))
        with assistant_fixture.database() as db:
            count = db.execute('SELECT COUNT(*) FROM login_tokens WHERE token_hash IN (?,?)',
                (p.remember_login_token_hash(admin_token), p.remember_login_token_hash(partner_token))).fetchone()[0]
        self.assertEqual(count, 0)
        self.assertEqual(self.client.get('/werkstatt/materialbestellung').status_code, 200)
        with patch('werkstatt_assistent.render_template', return_value='synthetic-own-profile') as render:
            self.assertEqual(self.client.get('/werkstatt/assistent').status_code, 200)
        self.assertEqual(render.call_args.kwargs['who']['actor'], 'mitarbeiter:1')
        self.assertEqual(self.client.get('/werkstatt/assistent/rechte').status_code, 302)
        with self.client.session_transaction() as state:
            self.assertEqual(state['assistent_mid'], 1)
            self.assertEqual(state['assistent_version'], 1)
            self.assertEqual(state['assistent_auth_version'], 1)
            self.assertNotEqual(state['csrf_token'], 'synthetic-form-token')
            for key in ('admin', 'partner_autohaus_id', 'assistent_bestaetigung'):
                self.assertNotIn(key, state)

    def test_stale_auth_session_cannot_read_or_submit_material_after_password_version_changes(self):
        self.assertEqual(self.login().status_code, 302)
        cookie_name = p.app.config['SESSION_COOKIE_NAME']
        stale = p.app.test_client()
        stale.set_cookie(cookie_name, self.client.get_cookie(cookie_name).value, domain='localhost')
        with self.client.session_transaction() as state:
            token = state['csrf_token']
        with assistant_fixture.database() as db:
            db.execute('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1')
            self.assertEqual(db.execute('SELECT version FROM assistent_rechte WHERE mitarbeiter_id=1').fetchone()[0], 1)
        self.assertEqual(stale.get('/werkstatt/materialbestellung/anforderungen').status_code, 401)
        self.assertEqual(stale.post('/werkstatt/materialbestellung/anforderungen',
            data={'csrf_token': token, 'request_id': 'synthetic-stale-request', 'positionen': '[]'}).status_code, 401)
        self.assertEqual(stale.get('/werkstatt/assistent/auftrag/156').status_code, 401)
        self.assertIn('name="mitarbeiter_id"', stale.get('/werkstatt/materialbestellung').text)
        response = self.login(csrf_token=token)
        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as state:
            self.assertEqual(state['assistent_mid'], 1)
            self.assertEqual(state['assistent_version'], 1)
            self.assertEqual(state['assistent_auth_version'], 2)
            self.assertNotEqual(state['csrf_token'], token)
            fresh_token = state['csrf_token']
        self.assertEqual(self.client.get('/werkstatt/materialbestellung/anforderungen',
            headers={'X-CSRF-Token': fresh_token}).status_code, 200)
        self.assertEqual(stale.get('/werkstatt/materialbestellung/anforderungen').status_code, 401)

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

    def test_order_limit_display_uses_minimum_of_personal_global_and_250_eur(self):
        self.login()
        for personal, global_limit, expected in ((30000,30000,25000),(9000,25000,9000),
                                                (12000,8000,8000),(12000,0,0),
                                                (12000,-1,0),(12000,'25000',0),(0,25000,0)):
            with self.subTest(personal=personal, global_limit=global_limit):
                with assistant_fixture.database() as db:
                    db.execute('UPDATE assistent_rechte SET limit_cent=? WHERE mitarbeiter_id=1', (personal,))
                with patch.object(p.workshop_orders,'cap',return_value=global_limit), \
                     patch('werkstatt_materialbestellung.render_template',return_value='synthetic-page') as render:
                    response = self.client.get('/werkstatt/materialbestellung')
                self.assertEqual(response.status_code,200)
                self.assertEqual(render.call_args.kwargs['order_limit_cent'],expected)

    def test_missing_global_configuration_or_personal_identity_never_invents_display_limit(self):
        self.login()
        with patch.object(p.workshop_orders,'cap',side_effect=RuntimeError('synthetic configuration unavailable')), \
             patch('werkstatt_materialbestellung.render_template',return_value='synthetic-page') as render:
            self.assertEqual(self.client.get('/werkstatt/materialbestellung').status_code,200)
            self.assertEqual(render.call_args.kwargs['order_limit_cent'],0)
        with self.client.session_transaction() as state:
            state.pop('assistent_mid',None)
            state.pop('assistent_version',None)
        with patch.object(p.workshop_orders,'cap') as cap, \
             patch('werkstatt_materialbestellung.render_template',return_value='synthetic-page') as render:
            self.assertEqual(self.client.get('/werkstatt/materialbestellung').status_code,200)
            self.assertEqual(render.call_args.kwargs['order_limit_cent'],0)
            cap.assert_not_called()


if __name__ == '__main__':
    unittest.main()
