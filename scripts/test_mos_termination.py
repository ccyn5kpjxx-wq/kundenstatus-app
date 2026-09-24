"""Synthetic, isolated tests for the opt-in public termination intake."""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from unittest.mock import patch
import sqlite3
import sys
import os

from flask import Flask

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mos_termination import HOURLY_IP_LIMIT, _declaration, init_schema, register, submit, unresolved_count


MAIL = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
        'smtp_host': 'example.invalid', 'smtp_port': 465, 'smtp_user': 'sender@example.invalid',
        '_smtp_password': 'synthetic', 'from_address': 'sender@example.invalid',
        'display_name': 'Synthetic MOS'}


class Portal:
    def __init__(self, path):
        self.path = path
        self.USE_POSTGRES = False
        self.app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        self.app.secret_key = 'synthetic-secret-only-for-tests'
        self.app.config['TESTING'] = True
        self.app.config['MOS_TERMINATION_TEST_MAIL'] = MAIL
        self.mail_cfg = MAIL

    def get_db(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db

    def get_werkstatt_smtp_config(self):
        return self.mail_cfg

    def admin_required(self, function):
        return function  # Synthetic portal only; application integration keeps real admin guard.


class TerminationTests(TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.portal = Portal(str(Path(self.tmp.name) / 'synthetic.sqlite3'))
        register(self.portal, enabled=True, test_mode=True)
        self.client = self.portal.app.test_client()

    def form_data(self):
        response = self.client.get('/mieten/kuendigen')
        self.assertEqual(response.status_code, 200)
        with self.client.session_transaction() as s:
            return {'csrf_token': s['csrf_token'], 'form_token': s['mos_termination_form'],
                    'kind': 'ordinary', 'name': 'Erika Muster',
                    'contract_reference': 'TEST-MOS-1', 'email': 'erika@example.invalid',
                    'timing': 'earliest', 'desired_date': '', 'reason': ''}

    def test_direct_route_and_receipt_does_not_cancel_any_rental(self):
        data = self.form_data()
        self.assertIn(b'jetzt k\xc3\xbcndigen', self.client.get('/mieten/kuendigen').data)
        data = self.form_data()
        with patch('mos_termination._smtp_send', return_value='sent') as send:
            response = self.client.post('/mieten/kuendigen', data=data)
        self.assertEqual(response.status_code, 200)
        self.assertIn('Eingang Ihrer', send.call_args.args[0]['Subject'])
        db = self.portal.get_db()
        row = db.execute('SELECT * FROM miet_checkout_terminations').fetchone()
        self.assertEqual(row['mail_status'], 'sent')
        self.assertIn('frühestmöglichen Zeitpunkt', row['declaration_text'])
        self.assertEqual(unresolved_count(self.portal), 0)
        db.close()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.data, 'html.parser')
        href = soup.find('a', string='Kündigungserklärung als Textdatei speichern')['href']
        saved = self.client.get(href)
        self.assertEqual(saved.status_code, 200)
        self.assertEqual(saved.mimetype, 'text/plain')
        self.assertIn('Abgabe über die Schaltfläche', saved.get_data(as_text=True))
        self.assertIn('Zugang beim Vermieter:', saved.get_data(as_text=True))

    def test_repeat_is_idempotent_and_different_payload_is_rejected(self):
        data = self.form_data()
        with patch('mos_termination._smtp_send', return_value='sent') as send:
            first = self.client.post('/mieten/kuendigen', data=data)
            second = self.client.post('/mieten/kuendigen', data=data)
            changed = self.client.post('/mieten/kuendigen', data={**data, 'name': 'Andere Person'})
        self.assertEqual((first.status_code, second.status_code, changed.status_code), (200, 200, 400))
        self.assertEqual(send.call_count, 1)
        db = self.portal.get_db()
        self.assertEqual(db.execute('SELECT COUNT(*) FROM miet_checkout_terminations').fetchone()[0], 1)
        db.close()

    def test_unverified_email_is_flagged_without_losing_declaration(self):
        data = self.form_data()
        with patch('mos_termination._smtp_send', return_value='uncertain'):
            response = self.client.post('/mieten/kuendigen', data=data)
        self.assertEqual(response.status_code, 202)
        self.assertIn('noch ungeklärt', response.get_data(as_text=True))
        self.assertEqual(unresolved_count(self.portal), 1)
        self.assertEqual(self.portal.app.test_cli_runner().invoke(args=['mos-termination-check']).exit_code, 1)
        admin = self.client.get('/mieten/admin/kuendigungen')
        self.assertEqual(admin.status_code, 200)
        self.assertIn('1 Bestätigung(en) offen', admin.get_data(as_text=True))
        db = self.portal.get_db()
        self.assertEqual(db.execute('SELECT mail_status FROM miet_checkout_terminations').fetchone()[0], 'review')
        db.close()

    def test_extraordinary_requires_reason_and_valid_date(self):
        data = self.form_data()
        self.assertEqual(self.client.post('/mieten/kuendigen', data={**data, 'kind': 'extraordinary'}).status_code, 400)
        self.assertEqual(self.client.post('/mieten/kuendigen', data={**data, 'timing': 'date',
                        'desired_date': '2026-02-30'}).status_code, 400)
        with patch('mos_termination._smtp_send', return_value='sent'):
            response = self.client.post('/mieten/kuendigen', data={**data, 'kind': 'extraordinary',
                                        'reason': 'Fahrzeug nicht nutzbar', 'timing': 'date',
                                        'desired_date': '2026-11-01'})
        self.assertEqual(response.status_code, 200)
        self.assertIn('Fahrzeug nicht nutzbar', response.get_data(as_text=True))

    def test_form_csrf_receipt_tampering_and_headers(self):
        data = self.form_data()
        self.assertEqual(self.client.post('/mieten/kuendigen', data={**data, 'csrf_token': 'bad'}).status_code, 400)
        with patch('mos_termination._smtp_send', return_value='sent'):
            response = self.client.post('/mieten/kuendigen', data=data)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(self.client.get('/mieten/kuendigen/beleg/not-a-token').status_code, 404)

    def test_rate_limit_persisted_and_no_plain_ip(self):
        db = self.portal.get_db()
        init_schema(db)
        db.commit()
        db.close()
        data = _declaration(self.form_data())
        for n in range(HOURLY_IP_LIMIT):
            row, fresh = submit(self.portal, data, 'key-' + str(n).zfill(32), ip='203.0.113.12')
            self.assertTrue(fresh)
        with self.assertRaises(OverflowError):
            submit(self.portal, data, 'key-' + 'x' * 32, ip='203.0.113.12')
        db = self.portal.get_db()
        buckets = [row['bucket'] for row in db.execute('SELECT bucket FROM miet_checkout_termination_limits')]
        self.assertTrue(buckets)
        self.assertNotIn('203.0.113.12', ' '.join(buckets))
        self.assertEqual(db.execute('SELECT COUNT(*) FROM miet_checkout_terminations').fetchone()[0], HOURLY_IP_LIMIT)
        db.close()

    def test_unverified_email_cannot_be_used_to_block_another_sender(self):
        data = _declaration(self.form_data())
        for n in range(12):
            _, fresh = submit(self.portal, data, 'sender-' + str(n).zfill(32),
                              ip='198.51.100.' + str(n + 1))
            self.assertTrue(fresh)
        _, fresh = submit(self.portal, data, 'actual-customer-' + 'x' * 24,
                          ip='203.0.113.219')
        self.assertTrue(fresh)

    def test_disabled_route_and_live_preflight(self):
        other = Portal(str(Path(self.tmp.name) / 'off.sqlite3'))
        register(other, enabled=False)
        self.assertEqual(other.app.test_client().get('/mieten/kuendigen').status_code, 404)
        live = Portal(str(Path(self.tmp.name) / 'live.sqlite3'))
        with self.assertRaisesRegex(ValueError, 'PostgreSQL'):
            register(live, enabled=True, test_mode=False)

    def test_running_route_survives_smtp_outage_and_preserves_receipt(self):
        live = Portal(str(Path(self.tmp.name) / 'active.sqlite3'))
        live.USE_POSTGRES = True  # Only simulates the startup gate, not a PostgreSQL acceptance.
        live.USING_EPHEMERAL_SECRET_KEY = False
        live.USING_GENERATED_FLASK_SECRET_KEY = False
        live.app.secret_key = 'synthetic-secret-only-for-tests-' + 'x' * 32
        live.app.config['SESSION_COOKIE_SECURE'] = True
        origin = 'https://test.example.invalid'
        with patch.dict(os.environ, {'MOS_TERMINATION_EMAIL_ENABLED': '1',
                                     'MOS_TERMINATION_ORIGIN': origin}):
            register(live, enabled=True, test_mode=False)
        live.mail_cfg = {**MAIL, 'smtp_configured': False}
        client = live.app.test_client()
        self.assertEqual(client.get('/mieten/kuendigen').status_code, 403)
        page = client.get('/mieten/kuendigen', base_url=origin)
        self.assertEqual(page.status_code, 200)
        from bs4 import BeautifulSoup
        html = BeautifulSoup(page.data, 'html.parser')
        data = {'csrf_token': html.find('input', {'name': 'csrf_token'})['value'],
                'form_token': html.find('input', {'name': 'form_token'})['value'],
                'kind': 'ordinary', 'name': 'Erika Muster',
                'contract_reference': 'TEST-MOS-2', 'email': 'erika@example.invalid',
                'timing': 'earliest'}
        response = client.post('/mieten/kuendigen', data=data, base_url=origin)
        self.assertEqual(response.status_code, 202)
        self.assertIn('noch ungeklärt', response.get_data(as_text=True))
        self.assertEqual(unresolved_count(live), 1)

    def test_live_gate_rejects_generated_key_and_insecure_cookie(self):
        live = Portal(str(Path(self.tmp.name) / 'weak.sqlite3'))
        live.USE_POSTGRES = True
        with patch.dict(os.environ, {'MOS_TERMINATION_EMAIL_ENABLED': '1',
                                     'MOS_TERMINATION_ORIGIN': 'https://test.example.invalid'}):
            with self.assertRaisesRegex(ValueError, 'Flask-Schlüssel'):
                register(live, enabled=True, test_mode=False)


if __name__ == '__main__':
    main()
