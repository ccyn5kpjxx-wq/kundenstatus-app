"""Render mail-worker preflight; all process and network calls are mocked."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.run_mos_render_contract_delivery import DeliveryPreflightError, preflight, run


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='mos-mail-preflight-')
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.file = root / 'mos-booking.json'
        self.file.write_text(json.dumps({'mode': 'live', 'enabled': False,
                                         'origin': 'https://rental.example.test'}), encoding='utf-8')
        self.env = {
            'RENDER': 'true', 'PUBLIC_SITE_ONLY': 'false',
            'REQUIRE_POSTGRES_ON_RENDER': 'true',
            'DATABASE_URL': 'postgresql://user:synthetic@db.example.test/mos',
            'MOS_BOOKING_CONFIG_FILE': str(self.file),
            'MOS_CONTRACT_EMAIL_ENABLED': '1',
            'FLASK_SECRET_KEY': 'synthetic-persistent-key-with-32-chars',
            'MAIL_SMTP_HOST': 'smtp.example.test', 'MAIL_SMTP_PORT': '465',
            'MAIL_SMTP_USER': 'workshop@example.test',
            'MAIL_IMAP_USER': 'workshop@example.test',
            'MAIL_SMTP_PASS': 'synthetic', 'MAIL_SMTP_SSL': '1', 'MAIL_SMTP_TLS': '0',
            'MOS_STRIPE_LIVE_KEY': 'synthetic-never-used',
        }

    def test_disabled_new_bookings_can_still_deliver_existing_contracts(self):
        self.assertEqual(preflight(self.env, secret_root=self.file.parent)['mode'], 'live')
        with patch('scripts.run_mos_render_contract_delivery.subprocess.run') as process:
            process.return_value.returncode = 0
            self.assertEqual(run(self.env, secret_root=self.file.parent), 0)
        args, kwargs = process.call_args
        self.assertEqual(args[0][-1], 'mos-contract-delivery')
        self.assertNotIn('MOS_STRIPE_LIVE_KEY', kwargs['env'])
        self.assertEqual(kwargs['env']['MAIL_SMTP_PASS'], 'synthetic')

    def test_no_mail_authority_or_untrusted_runtime_never_starts_worker(self):
        cases = [
            ('mail off', {'MOS_CONTRACT_EMAIL_ENABLED': '0'}),
            ('local database', {'DATABASE_URL': 'postgresql://user@localhost/mos'}),
            ('no TLS', {'MAIL_SMTP_SSL': '0', 'MAIL_SMTP_TLS': '0'}),
            ('SMTP missing', {'MAIL_SMTP_PASS': ''}),
            ('test key', {'MOS_STRIPE_TEST_KEY': 'sk_test_synthetic'}),
            ('bad file', {'MOS_BOOKING_CONFIG_FILE': str(self.file.parent / 'absent.json')}),
        ]
        for label, changes in cases:
            with self.subTest(label=label), patch('scripts.run_mos_render_contract_delivery.subprocess.run') as process:
                self.assertEqual(run({**self.env, **changes}, secret_root=self.file.parent), 2)
                process.assert_not_called()

    def test_test_mode_secret_file_is_rejected(self):
        self.file.write_text(json.dumps({'mode': 'stripe_test', 'enabled': True,
                                         'origin': 'https://rental.example.test'}), encoding='utf-8')
        with self.assertRaises(DeliveryPreflightError):
            preflight(self.env, secret_root=self.file.parent)


if __name__ == '__main__':
    unittest.main()
