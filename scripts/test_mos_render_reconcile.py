"""Offline checks for the future Render reconciliation entry point."""

import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))

from mos_public_contract import LESSOR_ADDRESS, LESSOR_NAME
import run_mos_render_reconcile as runner


def fixture_config():
    fleet = {'kona': {'id': 101, 'expected_name': 'TEST-KONA'},
             'i10': {'id': 102, 'expected_name': 'TEST-i10'}}
    review = {'approved_by': 'TEST ONLY', 'approved_at': '2026-09-24', 'evidence': 'TEST ONLY'}
    launch = {name: dict(review) for name in
              ('business_review', 'legal_review', 'finance_review', 'privacy_review',
               'sandbox_acceptance', 'postgres_acceptance')}
    launch['insurance'] = {
        name: {'verified': True, 'use': 'paid_self_drive',
               'evidence': 'TEST ONLY', 'vehicle_id': vehicle['id']}
        for name, vehicle in fleet.items()
    }
    return {
        'mode': 'live', 'enabled': True, 'live_enabled': True,
        'origin': 'https://booking.example.invalid', 'fleet': fleet, 'launch': launch,
        'deposit_method': 'card_authorization_at_booking',
        'cancellation_policy': 'free_48h_then_10pct_rent',
        'terms_version': 'test-fixture-final-v1', 'terms_text': 'TEST ONLY',
        'privacy_url': 'https://booking.example.invalid/privacy',
        'merchant_name': LESSOR_NAME, 'merchant_address': LESSOR_ADDRESS,
        'merchant_email': 'test@example.invalid', 'merchant_phone': 'TEST',
    }


class RenderReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='mos-render-reconcile-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'secrets'
        self.root.mkdir()
        self.file = self.root / 'mos-booking.json'
        self.config = fixture_config()
        self.write_config()
        self.env = {
            'RENDER': 'true', 'PUBLIC_SITE_ONLY': 'false',
            'REQUIRE_POSTGRES_ON_RENDER': 'true',
            'DATABASE_URL': 'postgresql://user:TEST@render-db.example.invalid/mos',
            'MOS_BOOKING_CONFIG_FILE': str(self.file),
            'MOS_STRIPE_LIVE_KEY': 'rk_live_TEST_ONLY',
            'MOS_STRIPE_PUBLISHABLE_KEY': 'pk_live_TEST_ONLY',
            'MOS_STRIPE_WEBHOOK_SECRET': 'whsec_TEST_ONLY',
            'FLASK_SECRET_KEY': 'TEST_ONLY_' + 'x' * 40,
        }

    def write_config(self):
        self.file.write_text(json.dumps(self.config), encoding='utf-8')

    def test_valid_runtime_passes_without_portal_import_or_network(self):
        self.assertEqual(runner.preflight(self.env, secret_root=self.root), self.config)
        with patch.object(runner.subprocess, 'run', return_value=SimpleNamespace(returncode=0)) as process:
            self.assertEqual(runner.run(self.env, secret_root=self.root), 0)
        args, kwargs = process.call_args
        self.assertEqual(args[0][-1], 'mos-booking-reconcile')
        self.assertEqual(kwargs['timeout'], runner.TIMEOUT_SECONDS)
        self.assertEqual(kwargs['env']['DATABASE_URL'], self.env['DATABASE_URL'])
        self.assertEqual(kwargs['env']['AUTO_BACKUP_ON_STARTUP'], 'false')
        self.assertEqual(kwargs['env']['MAILBOX_SEND_ENABLED'], 'false')
        self.assertNotIn('DATA_DIR', self.env)

    def test_missing_insurance_or_live_activation_does_not_start_job(self):
        with patch.object(runner.subprocess, 'run') as process:
            del self.config['launch']['insurance']['kona']
            self.write_config()
            self.assertEqual(runner.run(self.env, secret_root=self.root), 2)
            process.assert_not_called()
            self.config = fixture_config()
            self.config['live_enabled'] = False
            self.write_config()
            self.assertEqual(runner.run(self.env, secret_root=self.root), 2)
            process.assert_not_called()

    def test_wrong_database_secret_location_or_key_mode_fails_closed(self):
        bad = (
            {'DATABASE_URL': 'postgresql://user:TEST@127.0.0.1/mos'},
            {'DATABASE_URL': 'sqlite:///tmp/mos.db'},
            {'MOS_STRIPE_LIVE_KEY': 'sk_test_TEST_ONLY'},
            {'MOS_STRIPE_PUBLISHABLE_KEY': 'pk_test_TEST_ONLY'},
            {'MOS_STRIPE_TEST_KEY': 'sk_test_TEST_ONLY'},
            {'MOS_BOOKING_CONFIG_FILE': str(Path(self.temp.name) / 'outside.json')},
            {'PUBLIC_SITE_ONLY': 'true'},
            {'REQUIRE_POSTGRES_ON_RENDER': 'false'},
        )
        for update in bad:
            with self.subTest(update=next(iter(update))):
                with self.assertRaises(runner.ReconcilePreflightError):
                    runner.preflight({**self.env, **update}, secret_root=self.root)

    def test_invalid_json_does_not_start_job(self):
        self.file.write_text('{', encoding='utf-8')
        with patch.object(runner.subprocess, 'run') as process:
            self.assertEqual(runner.run(self.env, secret_root=self.root), 2)
            process.assert_not_called()

    def test_job_failure_and_timeout_reach_scheduler(self):
        with patch.object(runner.subprocess, 'run', return_value=SimpleNamespace(returncode=1)):
            self.assertEqual(runner.run(self.env, secret_root=self.root), 1)
        with patch.object(runner.subprocess, 'run', side_effect=runner.subprocess.TimeoutExpired('flask', 900)):
            self.assertEqual(runner.run(self.env, secret_root=self.root), 124)


if __name__ == '__main__':
    unittest.main()
