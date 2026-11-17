"""Run the mail CLI with the real starter's stripped environment and test DB."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
from run_mos_public_test import build_test_app
from scripts.run_mos_render_contract_delivery import run as run_mail_starter
from mietwagen_checkout import init_schema as init_checkout_schema
from mos_booking.production import init_refund_schema
from mos_public_contract import init_schema as init_contract_schema
from mos_order_receipt import init_schema as init_order_receipt_schema


class ReminderWorkerCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='mos-mail-cli-')
        cls.portal = build_test_app(cls.temp.name, origin='http://localhost')
        assert str(cls.portal.DB).endswith('.mos-public-test.sqlite3')

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def db(self):
        connection = sqlite3.connect(self.portal.DB)
        connection.row_factory = sqlite3.Row
        return connection

    def setUp(self):
        self.portal.app.extensions.pop('mos_public_booking', None)
        self.portal.app.config['MOS_PUBLIC_BOOKING'] = {
            'mode': 'live', 'enabled': False, 'origin': 'https://mos.example.test',
            'deposit_method': 'card_authorization_at_booking', 'deposit_timing': 'before_pickup'}
        for key in ('MOS_PUBLIC_STRIPE_TEST_KEY', 'MOS_PUBLIC_STRIPE_LIVE_KEY',
                    'MOS_PUBLIC_WEBHOOK_SECRET', 'MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY'):
            self.portal.app.config.pop(key, None)
        db = self.db()
        try:
            for name in ('miet_checkout_deposit_reminders', 'miet_checkout_order_receipts',
                         'miet_checkout_contract_delivery', 'miet_checkout_contracts',
                         'miet_checkout_cancellations', 'miet_checkout_refunds',
                         'miet_checkout_review_refunds', 'miet_checkout_limits',
                         'miet_checkout_deposit_auths', 'miet_checkout_creation_attempts',
                         'miet_checkout_events', 'miet_checkout_holds'):
                db.execute('DROP TABLE IF EXISTS ' + name)
            db.commit()
        finally:
            db.close()
        self.mail_cfg = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                         'from_address': 'workshop@example.test', '_smtp_password': 'synthetic',
                         'display_name': 'Synthetische Werkstatt'}
        self.config_file = Path(self.temp.name) / 'mail-worker-config.json'
        self.config_file.write_text(json.dumps(self.portal.app.config['MOS_PUBLIC_BOOKING']), encoding='utf-8')
        self.env = {'RENDER': 'true', 'PUBLIC_SITE_ONLY': 'false',
                    'REQUIRE_POSTGRES_ON_RENDER': 'true',
                    'DATABASE_URL': 'postgresql://synthetic:synthetic@db.example.test/synthetic',
                    'MOS_BOOKING_CONFIG_FILE': str(self.config_file), 'MOS_CONTRACT_EMAIL_ENABLED': '1',
                    'FLASK_SECRET_KEY': 'synthetic-persistent-shared-key-' * 2,
                    'MAIL_SMTP_HOST': 'smtp.example.test', 'MAIL_SMTP_PORT': '465',
                    'MAIL_SMTP_USER': 'workshop@example.test', 'MAIL_IMAP_USER': 'workshop@example.test',
                    'MAIL_SMTP_PASS': 'synthetic', 'MAIL_SMTP_SSL': '1', 'MAIL_SMTP_TLS': '0',
                    # The real starter must remove all three before its CLI.
                    'MOS_STRIPE_LIVE_KEY': 'rk_live_synthetic_never_used',
                    'MOS_STRIPE_PUBLISHABLE_KEY': 'pk_live_synthetic_never_used',
                    'MOS_STRIPE_WEBHOOK_SECRET': 'whsec_synthetic_never_used'}

    def run_sanitized_cli(self, sent):
        results = []

        def virtual_child(command, *, cwd, env, timeout, check):
            self.assertEqual(command[-3:], ['-m', 'scripts.run_mos_render_cli', 'mos-contract-delivery'])
            self.assertEqual(cwd, ROOT)
            for name in ('MOS_STRIPE_LIVE_KEY', 'MOS_STRIPE_PUBLISHABLE_KEY', 'MOS_STRIPE_WEBHOOK_SECRET'):
                self.assertNotIn(name, env)
            self.assertEqual(env['MAILBOX_SEND_ENABLED'], 'false')
            self.assertEqual(env['MOS_CONTRACT_EMAIL_ENABLED'], '1')
            # PostgreSQL is replaced only at the DB driver boundary by this
            # already initialized synthetic portal. The registered CLI and
            # reminder/outbox implementation themselves execute unchanged.
            with patch.dict(os.environ, env, clear=True), \
                    patch.object(self.portal, 'USE_POSTGRES', True), \
                    patch.object(self.portal, 'get_db', side_effect=self.db), \
                    patch.object(self.portal, 'get_werkstatt_smtp_config', return_value=self.mail_cfg), \
                    patch('mos_deposit_reminder._smtp_send', side_effect=lambda message, _: sent.append(message) or 'sent'), \
                    patch('stripe.StripeClient', side_effect=AssertionError('Mail worker must not initialize Stripe')), \
                    patch('mos_booking.gateway.OfflineGateway', side_effect=AssertionError('Mail worker must not create a gateway')), \
                    patch('mietwagen_checkout.SharedCheckout.__init__', side_effect=AssertionError('Mail worker must not initialize Checkout')):
                result = self.portal.app.test_cli_runner().invoke(args=['mos-contract-delivery'])
            results.append(result)
            return SimpleNamespace(returncode=result.exit_code)

        with patch('scripts.run_mos_render_contract_delivery.subprocess.run', side_effect=virtual_child):
            code = run_mail_starter(self.env, secret_root=self.config_file.parent)
        self.assertEqual(code, 0, results[0].output + repr(results[0].exception))
        self.assertNotIn('mos_public_booking', self.portal.app.extensions)
        return results[0]

    def test_mail_cli_initializes_new_outbox_without_stripe_or_booking_setup(self):
        sent = []
        result = self.run_sanitized_cli(sent)
        self.assertIn('Kautionserinnerungen versandt: 0', result.output)
        self.assertEqual(sent, [])
        db = self.db()
        try:
            names = {row['name'] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        finally:
            db.close()
        self.assertTrue({'miet_checkout_holds', 'miet_checkout_deposit_auths',
                         'miet_checkout_cancellations', 'miet_checkout_deposit_reminders',
                         'miet_checkout_contracts', 'miet_checkout_contract_delivery',
                         'miet_checkout_order_receipts'} <= names)

    def test_mail_cli_sends_due_deposit_link_for_existing_paid_booking_without_stripe(self):
        db = self.db()
        now = datetime.now(timezone.utc)
        start = now + timedelta(hours=6)
        end = start + timedelta(days=1)
        hold_id = 'synthetic-existing-paid-hold'
        email = 'customer@example.test'
        try:
            init_checkout_schema(db); init_refund_schema(db)
            init_contract_schema(db); init_order_receipt_schema(db)
            vehicle = db.execute('SELECT id FROM mietfahrzeuge ORDER BY id LIMIT 1').fetchone()['id']
            rental_id = db.execute('''INSERT INTO mietvorgaenge
                (mietfahrzeug_id,kunde_name,kunde_email,start_datum,end_datum,status,erstellt_am,geaendert_am)
                VALUES (?,?,?,?,?,'aktiv',?,?)''',
                (vehicle, 'Synthetischer Testkunde', email, start.strftime('%d.%m.%Y'),
                 end.strftime('%d.%m.%Y'), now.isoformat(), now.isoformat())).lastrowid
            payload = json.dumps({'customer': {'email': email}, 'quote': {
                'test_only': False, 'deposit_timing': 'before_pickup',
                'deposit_method': 'card_authorization_at_booking', 'owner_hash': 'a' * 64,
                'start_slot': start.isoformat(), 'end_slot': end.isoformat()}})
            db.execute('''INSERT INTO miet_checkout_holds
                (id,request_key,mietfahrzeug_id,start_datum,end_datum,payload,fingerprint,status,
                 expires_at,session_id,payment_intent,mietvorgang_id)
                VALUES (?,?,?,?,?,?,?,'confirmed',1,?,?,?)''',
                (hold_id, 'synthetic-request', vehicle, start.date().isoformat(), end.date().isoformat(),
                 payload, 'synthetic-fingerprint', 'cs_live_synthetic', 'pi_synthetic', rental_id))
            # This fixture represents a contract already delivered by the prior
            # worker, so this cycle has only the later reminder left to send.
            db.execute('INSERT INTO miet_checkout_contracts VALUES (?,?,?,?,?,?,?,?)',
                       (hold_id, '{}', 'synthetic', 'Synthetisch', now.isoformat(), '', '', 'synthetic'))
            db.execute('''INSERT INTO miet_checkout_contract_delivery
                (hold_id,pdf_sha256,recipient,status,enqueued_at,accepted_at)
                VALUES (?,?,?,'sent',?,?)''', (hold_id, 'synthetic', email, now.isoformat(), now.isoformat()))
            db.commit()
        finally:
            db.close()
        sent = []
        result = self.run_sanitized_cli(sent)
        self.assertIn('Kautionserinnerungen versandt: 1', result.output)
        self.assertEqual(len(sent), 1)
        self.assertIn('/mieten/buchung-oeffnen#', sent[0].get_content())
        db = self.db()
        try:
            self.assertEqual(db.execute('SELECT status FROM miet_checkout_deposit_reminders WHERE hold_id=?',
                                        (hold_id,)).fetchone()['status'], 'sent')
            self.assertEqual(db.execute('SELECT COUNT(*) AS n FROM miet_checkout_deposit_auths').fetchone()['n'], 0)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
