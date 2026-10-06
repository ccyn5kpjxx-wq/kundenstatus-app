"""Personal material rights never imply a portal password; offline HTTP tests."""
import unittest
from unittest.mock import patch

import test_assistent as fixture

p, database = fixture.p, fixture.database


class WhatsappOnlyAccessTests(unittest.TestCase):
    def setUp(self):
        self.legacy = fixture.AssistantTests(methodName='runTest')
        self.legacy.setUp()
        self.addCleanup(self.legacy.tearDown)
        self.admin = self.legacy.make_client(admin=True)
        self.enterContext(patch('requests.sessions.Session.request', side_effect=AssertionError('No network')))
        self.enterContext(patch('smtplib.SMTP', side_effect=AssertionError('No SMTP')))
        self.enterContext(patch('smtplib.SMTP_SSL', side_effect=AssertionError('No SMTP')))
        with database() as db:
            db.execute("UPDATE assistent_rechte SET passwort_hash='',lesen=1,einkaufen=1,dokumentieren=0,limit_cent=25000 WHERE mitarbeiter_id=1")

    def login(self, password):
        client = p.app.test_client()
        client.get('/werkstatt/assistent')
        with client.session_transaction() as session:
            token = session['csrf_token']
        response = client.post('/werkstatt/assistent/login', data={
            'csrf_token':token,'mitarbeiter_id':'1','password':password})
        return client, response

    def rights(self, **kwargs):
        data = dict(csrf_token='test-csrf',mitarbeiter_id='1',password='',lesen='on',einkaufen='on',limit='250')
        data.update(kwargs)
        return self.admin.post('/werkstatt/assistent/rechte',data=data)

    def record(self):
        with database() as db:
            return dict(db.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=1').fetchone())

    def test_empty_hash_never_authenticates(self):
        for password in ('', 'random-password-123', '!whatsapp-only-login-disabled!'):
            with self.subTest(password=password):
                client, response = self.login(password)
                self.assertEqual(response.status_code,401)
                with client.session_transaction() as session:
                    self.assertFalse(session.get('assistent_mid'))

    def test_page_distinguishes_no_password_and_never_exposes_hash(self):
        response = self.admin.get('/werkstatt/assistent/rechte')
        self.assertEqual(response.status_code,200)
        self.assertIn('Portal-Anmeldung deaktiviert',response.text)
        self.assertIn('WhatsApp-Rechte speichern',response.text)
        self.assertNotIn('Persönlicher Zugang eingerichtet.',response.text)
        with patch('werkstatt_assistent.render_template',return_value='safe') as render:
            self.admin.get('/werkstatt/assistent/rechte')
        employee = render.call_args.kwargs['employees'][0]
        self.assertIs(employee['has_password'],False)
        self.assertNotIn('passwort_hash',employee)

    def test_edit_material_rights_preserves_disabled_portal_and_increments_version(self):
        version=self.record()['version']
        self.assertEqual(self.rights(limit='125').status_code,200)
        record=self.record()
        self.assertEqual(record['passwort_hash'],'')
        self.assertEqual(record['limit_cent'],12500)
        self.assertEqual(record['version'],version+1)
        self.assertEqual(record['dokumentieren'],0)

    def test_explicit_portal_activation_requires_real_password(self):
        before=self.record()
        for password in ('','short'):
            response=self.rights(enable_portal_login='on',password=password)
            self.assertEqual(response.status_code,400)
            self.assertEqual(self.record(),before)
        response=self.rights(enable_portal_login='on',password='synthetic-new-password-123')
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.login('synthetic-new-password-123')[1].status_code,302)
        self.assertIn('Portal-Anmeldung aktiviert',response.text)

    def test_existing_password_is_not_changed_by_rights_edit(self):
        self.rights(enable_portal_login='on',password='synthetic-existing-password-123')
        hashed=self.record()['passwort_hash']
        self.assertEqual(self.rights(limit='200',dokumentieren='on').status_code,200)
        self.assertEqual(self.record()['passwort_hash'],hashed)
        self.assertEqual(self.login('synthetic-existing-password-123')[1].status_code,302)

    def test_material_save_does_not_implicitly_enable_portal_login(self):
        before=self.record()
        response=self.rights(password='synthetic-unconfirmed-password-123')
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.record(),before)
        self.assertEqual(self.login('synthetic-unconfirmed-password-123')[1].status_code,401)

    def test_new_portal_form_still_requires_password(self):
        with database() as db:
            db.execute('DELETE FROM assistent_rechte WHERE mitarbeiter_id=1')
        self.assertEqual(self.rights().status_code,400)
        with database() as db:
            self.assertIsNone(db.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=1').fetchone())


if __name__=='__main__':
    unittest.main()
