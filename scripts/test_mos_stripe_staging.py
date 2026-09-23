"""Pure preflight tests: no portal import, browser, network, or Stripe API call."""

from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

import run_mos_stripe_staging as launcher
from run_mos_stripe_staging import require_test_credentials, staging_configuration, TEST_ORIGIN


TEST_VALUES = {
    'MOS_STRIPE_TEST_KEY': 'sk_test_dummy',
    'MOS_STRIPE_PUBLISHABLE_KEY': 'pk_test_dummy',
    'MOS_STRIPE_WEBHOOK_SECRET': 'whsec_dummy',
}


class StagingPreflightTests(unittest.TestCase):
    def test_all_test_credentials_required_without_exposing_values(self):
        self.assertEqual(require_test_credentials(TEST_VALUES), TEST_VALUES)
        for name in TEST_VALUES:
            with self.subTest(missing=name):
                with self.assertRaisesRegex(ValueError, name):
                    require_test_credentials({k: v for k, v in TEST_VALUES.items() if k != name})
            with self.subTest(empty=name):
                with self.assertRaisesRegex(ValueError, name):
                    require_test_credentials({**TEST_VALUES, name: ''})

    def test_live_or_mismatched_credentials_fail_closed(self):
        cases = (
            {'MOS_STRIPE_TEST_KEY': 'sk_live_dummy'},
            {'MOS_STRIPE_PUBLISHABLE_KEY': 'pk_live_dummy'},
            {'MOS_STRIPE_WEBHOOK_SECRET': 'wrong_dummy'},
            {'MOS_STRIPE_TEST_KEY': 'sk_test_dummy\n'},
            {'MOS_STRIPE_LIVE_KEY': 'sk_live_dummy'},
        )
        for override in cases:
            with self.subTest(override=next(iter(override))):
                with self.assertRaises(ValueError):
                    require_test_credentials({**TEST_VALUES, **override})
        self.assertEqual(require_test_credentials({**TEST_VALUES, 'MOS_STRIPE_TEST_KEY': 'rk_test_dummy'})
                         ['MOS_STRIPE_TEST_KEY'], 'rk_test_dummy')

    def test_staging_config_retains_synthetic_fleet_and_is_draft(self):
        offline = {'enabled': True, 'test_configuration': True, 'mode': 'offline',
                   'origin': 'http://127.0.0.1:5085',
                   'fleet': {'i10': {'id': 1, 'daily_cents': 3900},
                             'kona': {'id': 2, 'daily_cents': 5900}},
                   'terms_version': 'draft:test', 'terms_text': 'TEST'}
        config = staging_configuration(offline)
        self.assertEqual(config['mode'], 'stripe_test')
        self.assertEqual(config['origin'], TEST_ORIGIN)
        self.assertTrue(config['enabled'])
        self.assertTrue(config['test_configuration'])
        self.assertEqual(config['deposit_method'], 'card_authorization_at_booking')
        self.assertEqual(config['cancellation_policy'], 'free_48h_then_10pct_rent')
        self.assertEqual(config['fleet'], offline['fleet'])
        self.assertEqual(config['terms_version'], 'draft:test')
        self.assertEqual(offline['mode'], 'offline')
        with self.assertRaises(ValueError):
            staging_configuration({**offline, 'mode': 'live'})

    def test_builder_keeps_dummy_credentials_in_isolated_test_app(self):
        with tempfile.TemporaryDirectory(prefix='mos-stripe-preflight-') as directory:
            db_path = Path(directory) / launcher.TEST_DB_NAME
            offline = {'enabled': True, 'test_configuration': True, 'mode': 'offline',
                       'fleet': {'i10': {'id': 1}, 'kona': {'id': 2}},
                       'terms_version': 'draft:test'}
            fake = SimpleNamespace(USE_POSTGRES=False, DB=db_path,
                                   app=SimpleNamespace(config={'MOS_PUBLIC_BOOKING': offline}))
            with patch.object(launcher, 'build_test_app', return_value=fake) as builder:
                result = launcher.build_staging_app(directory, require_test_credentials(TEST_VALUES))
            self.assertIs(result, fake)
            builder.assert_called_once_with(Path(directory).resolve(), origin=TEST_ORIGIN)
            self.assertEqual(fake.app.config['MOS_PUBLIC_BOOKING']['mode'], 'stripe_test')
            self.assertEqual(fake.app.config['MOS_PUBLIC_BOOKING']['deposit_method'],
                             'card_authorization_at_booking')
            self.assertEqual(fake.app.config['MOS_PUBLIC_STRIPE_TEST_KEY'], 'sk_test_dummy')
            self.assertEqual(fake.app.config['MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY'], 'pk_test_dummy')
            self.assertEqual(fake.app.config['MOS_PUBLIC_WEBHOOK_SECRET'], 'whsec_dummy')
            self.assertEqual(fake.app.config['MOS_PUBLIC_STRIPE_LIVE_KEY'], '')


if __name__ == '__main__':
    unittest.main()
