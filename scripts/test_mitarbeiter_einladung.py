"""Synthetic personal setup link tests, no real accounts or outbound calls."""
import concurrent.futures
import json
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import patch
from urllib.parse import urlsplit, parse_qs

import test_assistent as assistant_fixture
from werkzeug.security import check_password_hash
from werkstatt_mitarbeiter_einladung import EmployeeInvitations, register_employee_invitations, INVALID_LINK

p = assistant_fixture.p
database = assistant_fixture.database

if 'employee_invitations' not in p.app.blueprints:
    p.employee_invitations = register_employee_invitations(p)


class EmployeeInvitationTests(TestCase):
    def setUp(self):
        self.fixture = assistant_fixture.AssistantTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.service = p.employee_invitations
        self.clock = patch.object(self.service, 'clock', return_value=1791370800)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.config = patch.dict(p.app.config, EMPLOYEE_INVITE_PUBLIC_BASE_URL='https://portal.test')
        self.config.start()
        self.addCleanup(self.config.stop)
        with database() as db:
            db.execute('DELETE FROM assistent_einladungen')
            db.execute('''INSERT INTO mitarbeiter(id,name,aktiv,erstellt_am,geaendert_am)
                VALUES(2,'Second Testperson',1,?,?)''', (p.now_str(), p.now_str()))
            db.execute('UPDATE assistent_rechte SET auth_version=1 WHERE mitarbeiter_id=1')
        self.admin = self.fixture.make_client(admin=True)
        self.client = p.app.test_client()
        with self.client.session_transaction() as state:
            state['csrf_token'] = 'setup-test-csrf'
        with p.LOGIN_ATTEMPTS_LOCK:
            p.LOGIN_ATTEMPTS.clear()

    def rights(self, mid=1):
        with database() as db:
            return dict(db.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=?', (mid,)).fetchone())

    def invite(self, mid=1, **kwargs):
        value = self.service.issue(mid, **kwargs)
        return parse_qs(urlsplit(value['url']).fragment)['token'][0]

    def redeem(self, token, **overrides):
        data = dict(token=token, password='self-chosen-password-123',
                    password_confirm='self-chosen-password-123', csrf_token='setup-test-csrf')
        data.update(overrides)
        return self.client.post('/werkstatt/zugang/einrichten', data=data)

    def test_strong_fragment_link_only_hash_persisted_and_get_cannot_consume(self):
        before = self.rights()
        value = self.service.issue(1)
        parsed = urlsplit(value['url'])
        token = parse_qs(parsed.fragment)['token'][0]
        self.assertEqual(len(token), 64)
        self.assertEqual(parsed.path, '/werkstatt/zugang/einrichten')
        self.assertFalse(parsed.query)
        with database() as db:
            invite = dict(db.execute('SELECT * FROM assistent_einladungen').fetchone())
            audit = [dict(row) for row in db.execute('SELECT * FROM assistent_audit').fetchall()]
        self.assertNotIn(token, json.dumps(invite))
        self.assertNotIn(token, json.dumps(audit))
        self.assertNotIn(value['url'], json.dumps(audit))
        self.assertIsNone(invite['used_at'])
        self.assertEqual(before, self.rights())
        with patch('werkstatt_mitarbeiter_einladung.render_template', return_value='neutral-setup') as renderer:
            response = self.client.get('/werkstatt/zugang/einrichten?token=' + token)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(renderer.call_args.kwargs['valid'])
        self.assertIsNone(renderer.call_args.kwargs['setup_token'])
        self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
        self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertEqual(before, self.rights())
        self.assertTrue(self.service.inspect(token)['valid'])

    def test_host_header_cannot_change_configured_link(self):
        with p.app.test_request_context('/', headers={'Host':'attacker.test','X-Forwarded-Host':'attacker.test'}):
            value = self.service.issue(1)
        self.assertTrue(value['url'].startswith('https://portal.test/'))
        self.assertNotIn('attacker', value['url'])

    def test_missing_or_unsafe_canonical_origin_fails_before_creating(self):
        for value in ('https://a.test/path', 'https://u:p@a.test', 'http://a.test', 'https://a.test?x=1'):
            with self.subTest(value=value), patch.dict(p.app.config, EMPLOYEE_INVITE_PUBLIC_BASE_URL=value):
                with self.assertRaisesRegex(ValueError, 'HTTPS-Portaladresse'):
                    self.service.issue(1)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_einladungen').fetchone()[0], 0)

    def test_rights_missing_requires_explicit_narrow_material_grant(self):
        with self.assertRaisesRegex(ValueError, 'ausdrücklich'):
            self.service.issue(2)
        with patch.object(p.workshop_orders, 'cap', return_value=9000):
            self.service.issue(2, grant_material=True)
        rights = self.rights(2)
        self.assertEqual((rights['lesen'],rights['dokumentieren'],rights['einkaufen'],rights['limit_cent']), (1,0,1,9000))
        self.assertEqual(rights['passwort_hash'], '')

    def test_grant_never_widens_existing_permissions_or_exceeds_250(self):
        with database() as db:
            db.execute('UPDATE assistent_rechte SET dokumentieren=0,einkaufen=0,limit_cent=0 WHERE mitarbeiter_id=1')
        before = self.rights()
        self.service.issue(1, grant_material=True)
        self.assertEqual(before, self.rights())
        with patch.object(p.workshop_orders, 'cap', return_value=99000):
            self.service.issue(2, grant_material=True)
        self.assertEqual(self.rights(2)['limit_cent'], 25000)

    def test_issue_all_is_atomic_when_any_required_grant_missing(self):
        with self.assertRaises(ValueError):
            self.service.issue_all()
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_einladungen').fetchone()[0], 0)
        with patch.object(p.workshop_orders, 'cap', return_value=25000):
            values = self.service.issue_all(grant_material=True)
        self.assertEqual([row['employee_id'] for row in values], [1,2])

    def test_new_link_revokes_previous_and_does_not_change_password(self):
        before = self.rights()
        old = self.invite()
        new = self.invite()
        self.assertNotEqual(old, new)
        with self.assertRaisesRegex(ValueError, 'ungültig'):
            self.service.inspect(old)
        self.assertTrue(self.service.inspect(new)['valid'])
        self.assertEqual(before, self.rights())
        self.assertTrue(check_password_hash(self.rights()['passwort_hash'], 'test-passwort-123'))

    def test_expiry_inactive_and_rights_or_auth_change_invalidate(self):
        token = self.invite()
        with patch.object(self.service, 'clock', return_value=1791370800 + 72*3600):
            with self.assertRaises(ValueError): self.service.inspect(token)
        for query in ('UPDATE mitarbeiter SET aktiv=0 WHERE id=1',
                      'UPDATE assistent_rechte SET version=version+1 WHERE mitarbeiter_id=1',
                      'UPDATE assistent_rechte SET auth_version=auth_version+1 WHERE mitarbeiter_id=1',
                      "UPDATE assistent_rechte SET passwort_hash='changed' WHERE mitarbeiter_id=1"):
            with self.subTest(query=query):
                with database() as db:
                    db.execute('UPDATE mitarbeiter SET aktiv=1 WHERE id=1')
                token = self.invite()
                with database() as db: db.execute(query)
                with self.assertRaises(ValueError): self.service.inspect(token)

    def test_password_repeat_failure_keeps_valid_token_and_no_password_reflection(self):
        token = self.invite()
        before = self.rights()
        with patch('werkstatt_mitarbeiter_einladung.render_template', return_value='setup-form') as renderer:
            response = self.redeem(token, password_confirm='wrong-repeat')
        self.assertEqual(response.status_code, 400)
        self.assertTrue(renderer.call_args.kwargs['valid'])
        self.assertEqual(renderer.call_args.kwargs['setup_token'], token)
        self.assertNotIn('password', renderer.call_args.kwargs)
        self.assertEqual(before, self.rights())
        self.assertTrue(self.service.inspect(token)['valid'])

    def test_csrf_and_rate_guards_apply_before_consumption(self):
        token = self.invite()
        response = self.redeem(token, csrf_token='wrong')
        self.assertEqual(response.status_code, 400)
        self.assertTrue(self.service.inspect(token)['valid'])
        with patch.object(p, 'login_rate_limit_status', return_value=(True, 60)), \
             patch('werkstatt_mitarbeiter_einladung.render_template', return_value='limited'):
            self.assertEqual(self.redeem(token).status_code, 429)
            response = self.client.post('/werkstatt/zugang/einrichten/pruefen', json={'token':token}, headers={'X-CSRF-Token':'setup-test-csrf'})
        self.assertEqual(response.status_code, 429)
        self.assertTrue(self.service.inspect(token)['valid'])

    def test_inspection_returns_only_intended_identity_and_invalid_token_is_generic(self):
        token = self.invite()
        response = self.client.post('/werkstatt/zugang/einrichten/pruefen', json={'token':token,'employee_id':2}, headers={'X-CSRF-Token':'setup-test-csrf'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['employee'], {'id':1,'name':'Testperson'})
        self.assertNotIn('hash', response.text)
        response = self.client.post('/werkstatt/zugang/einrichten/pruefen', json={'token':'not-an-invite'}, headers={'X-CSRF-Token':'setup-test-csrf'})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json, {'valid':False,'error':INVALID_LINK})

    def test_success_authenticates_only_invited_employee_with_no_admin_or_old_draft(self):
        token = self.invite()
        with self.client.session_transaction() as state:
            state.update(admin=True, assistent_mid=2, assistent_version=99,
                         assistent_bestaetigung={'id':'old'}, partner_autohaus_id=7)
        before = self.rights()
        response = self.redeem(token, mitarbeiter_id='2', next='https://attacker.test')
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.location, '/werkstatt/mein-konto')
        with self.client.session_transaction() as state:
            self.assertEqual(state['assistent_mid'], 1)
            self.assertEqual(state['assistent_version'], before['version'])
            self.assertEqual(state['assistent_auth_version'], 2)
            self.assertNotEqual(state['csrf_token'], 'setup-test-csrf')
            for key in ('admin','partner_autohaus_id','assistent_bestaetigung'):
                self.assertNotIn(key, state)
        cookies = response.headers.getlist('Set-Cookie')
        self.assertTrue(any(p.ADMIN_REMEMBER_COOKIE in value and 'Max-Age=0' in value for value in cookies))
        self.assertTrue(any(p.PARTNER_REMEMBER_COOKIE in value and 'Max-Age=0' in value for value in cookies))
        after = self.rights()
        self.assertEqual(before['version'], after['version'])
        self.assertEqual(after['auth_version'], 2)
        self.assertTrue(check_password_hash(after['passwort_hash'], 'self-chosen-password-123'))
        self.assertFalse(check_password_hash(after['passwort_hash'], 'test-passwort-123'))
        with self.assertRaises(ValueError): self.service.inspect(token)

    def test_one_use_and_old_login_revoked_without_invalidating_pending_material(self):
        token = self.invite()
        old_session = self.fixture.make_client()
        material = dict(phone_number_id='portal:personal', wamid='portal.1.00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000002',
                        employee_id=1,sender_id=0,sender_revision=1,rights_version=1,forwarded=0,
                        mime='image/jpeg', caption='1 Stück')
        with database() as db:
            self.assertEqual(p.material_order_portal.active(db, material)['id'], 1)
        self.service.redeem(token, 'new-personal-password', 'new-personal-password')
        self.assertEqual(old_session.get('/werkstatt/assistent/auftrag/156').status_code, 401)
        with database() as db:
            self.assertEqual(p.material_order_portal.active(db, material)['id'], 1)
        with self.assertRaises(ValueError):
            self.service.redeem(token, 'again-new-password', 'again-new-password')
        self.assertEqual(self.rights()['auth_version'], 2)

    def test_concurrent_redemption_succeeds_exactly_once(self):
        token = self.invite()
        def redeem(index):
            try:
                return self.service.redeem(token, 'concurrent-password-' + str(index), 'concurrent-password-' + str(index))
            except ValueError:
                return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(redeem, [1,2]))
        self.assertEqual(sum(value is not None for value in results), 1)
        self.assertEqual(self.rights()['auth_version'], 2)

    def test_employee_cannot_issue_links_and_admin_post_requires_csrf(self):
        response = self.fixture.client.post('/admin/mitarbeiter/1/einrichtungslink', data={'csrf_token':'test-csrf'})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.admin.post('/admin/mitarbeiter/1/einrichtungslink', data={}).status_code, 400)
        with patch('werkstatt_mitarbeiter_einladung.render_template', return_value='admin-links') as renderer:
            response = self.admin.post('/admin/mitarbeiter/1/einrichtungslink', data={'csrf_token':'test-csrf'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(renderer.call_args.kwargs['created_invitations']), 1)
        self.assertEqual(renderer.call_args.kwargs['created_invitation']['employee_id'], 1)
        self.assertNotIn('token_hash', str(renderer.call_args.kwargs['employees']))

    def test_postgres_adapter_keeps_explicit_natural_primary_key_returning(self):
        # Exercise the actual production SQL adapter over a synthetic cursor.
        # It would append RETURNING id to either old natural-key INSERT.
        statements = []
        original_db = p.get_db
        class Cursor:
            def __init__(self, connection): self.cursor = connection.cursor()
            def __enter__(self): return self
            def __exit__(self, *args): self.cursor.close()
            def execute(self, statement, params):
                statements.append(statement)
                if p.get_insert_table_name(statement) in {'assistent_einladungen','assistent_rechte'}:
                    self.assert_natural_key(statement)
                self.cursor.execute(statement.replace('%s','?'), params)
            @staticmethod
            def assert_natural_key(statement):
                if 'RETURNING id' in statement:
                    raise AssertionError('PostgreSQL requires the natural employee key.')
            @property
            def rowcount(self): return self.cursor.rowcount
            @property
            def description(self):
                return [SimpleNamespace(name=value[0]) for value in self.cursor.description] if self.cursor.description else None
            def fetchall(self): return self.cursor.fetchall()
        class Connection:
            def __init__(self): self.connection = original_db()
            def cursor(self): return Cursor(self.connection)
            def commit(self): self.connection.commit()
            def rollback(self): self.connection.rollback()
            def close(self): self.connection.close()
        with patch.object(p, 'get_db', side_effect=lambda: p.PostgresConnection(Connection())), \
             patch.object(p.workshop_orders, 'cap', return_value=25000):
            value = self.service.issue(2, grant_material=True)
        token = parse_qs(urlsplit(value['url']).fragment)['token'][0]
        self.assertTrue(self.service.inspect(token)['valid'])
        natural_inserts = [sql for sql in statements if p.get_insert_table_name(sql) in {'assistent_einladungen','assistent_rechte'}]
        self.assertEqual(len(natural_inserts), 2)
        self.assertTrue(all('RETURNING mitarbeiter_id' in sql for sql in natural_inserts))


if __name__ == '__main__':
    main()
