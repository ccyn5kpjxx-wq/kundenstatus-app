"""Long-rental extended requests are explicit, signed and coverage-checked."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
import test_mos_deferred_deposit as deferred_fixture
from mietwagen_checkout import EXTENDED_DEPOSIT_MODE, extended_deposit_quote
from mos_public_contract import signed_payload
from mos_booking.gateway import StripeTestGateway
from mos_booking.production import StripeLiveGateway


class ExtendedDepositTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deferred_fixture.DeferredDepositTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        deferred_fixture.DeferredDepositTests.tearDownClass()

    def setUp(self):
        self.fixture = deferred_fixture.DeferredDepositTests()
        self.fixture.setUp()
        self.service = self.fixture.service
        self.gateway = self.fixture.gateway

    @contextmanager
    def earliest_window(self, paid):
        quote = json.loads(paid['payload'])['quote']
        moment = datetime.fromisoformat(quote['start_slot']) - timedelta(hours=12)
        self.fixture.clock = moment.timestamp()

        class EarliestWindow(datetime):
            @classmethod
            def now(cls, tz=None):
                return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)

        with patch('mietwagen_checkout.datetime', EarliestWindow):
            yield

    def long_paid(self, days=10, *, test_only=True):
        hold = self.fixture.hold(rental_days=days, authorization_mode=EXTENDED_DEPOSIT_MODE,
                                 test_only=test_only)
        return self.fixture.pay(hold)[0]

    def test_ten_day_rental_explicitly_requests_extended_hold_and_verifies_actual_deadline(self):
        paid = self.long_paid()
        calls = []
        create = self.gateway.create_deposit_intent

        def capture(params, key):
            calls.append(params)
            return create(params, key)

        with self.earliest_window(paid), patch.object(self.gateway, 'create_deposit_intent', side_effect=capture):
            intent = self.service.prepare_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=30 * 86400)
            self.assertTrue(self.service.reconcile_deposit(paid['id'])['ready'])
        self.assertEqual(calls[0]['payment_method_options'],
                         {'card': {'request_extended_authorization': 'if_available'}})
        self.assertEqual(self.service.read(paid['id'])['status'], 'confirmed')

    def test_ordinary_seven_day_fallback_does_not_pass_for_ten_day_rental(self):
        paid = self.long_paid()
        with self.earliest_window(paid):
            intent = self.service.prepare_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7 * 86400)
            with self.assertRaisesRegex(ValueError, 'reicht nicht bis nach der Rückgabe'):
                self.service.reconcile_deposit(paid['id'])
        self.assertEqual(self.service._deposit_record(paid['id'])['status'], 'released')
        self.assertEqual(self.service.read(paid['id'])['status'], 'confirmed')
        self.assertEqual(self.fixture.rental_status(paid), 'aktiv')

    def test_twenty_eight_days_fit_visa_extended_window_with_twelve_hour_lead_and_return_buffer(self):
        paid = self.long_paid(28)
        with self.earliest_window(paid):
            intent = self.service.prepare_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=29 * 86400 + 18 * 3600)
            self.assertTrue(self.service.reconcile_deposit(paid['id'])['ready'])

    def test_deadline_equal_to_return_plus_twenty_four_hours_remains_insufficient(self):
        paid = self.long_paid(28)
        with self.earliest_window(paid):
            intent = self.service.prepare_deposit(paid['id'])
            self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=29 * 86400 + 12 * 3600)
            with self.assertRaisesRegex(ValueError, 'reicht nicht bis nach der Rückgabe'):
                self.service.reconcile_deposit(paid['id'])

    def test_five_day_rental_does_not_request_extension_even_when_configuration_flag_is_signed(self):
        paid = self.long_paid(5)
        calls = []
        create = self.gateway.create_deposit_intent

        def capture(params, key):
            calls.append(params)
            return create(params, key)

        with self.earliest_window(paid), patch.object(self.gateway, 'create_deposit_intent', side_effect=capture):
            self.service.prepare_deposit(paid['id'])
        self.assertNotIn('payment_method_options', calls[0])

    def test_long_rental_without_explicit_mode_and_above_cap_are_rejected_before_payment(self):
        with patch.object(self.gateway, 'create') as rent_checkout:
            with self.assertRaisesRegex(ValueError, 'über fünf Tage'):
                self.fixture.hold(rental_days=10)
            with self.assertRaisesRegex(ValueError, 'verlängerte Kreditkartenautorisierung'):
                self.fixture.hold(rental_days=29, authorization_mode=EXTENDED_DEPOSIT_MODE)
            with self.assertRaises(ValueError):
                self.fixture.hold(rental_days=10, authorization_mode='automatic')
            rent_checkout.assert_not_called()

    def test_extension_mode_is_bound_to_signed_snapshot_and_legacy_quotes_keep_their_shape(self):
        hold = self.fixture.hold(rental_days=10, authorization_mode=EXTENDED_DEPOSIT_MODE)
        payload = json.loads(hold['payload'])
        contract, _, _ = signed_payload(payload)
        self.assertEqual(contract['deposit_authorization_mode'], EXTENDED_DEPOSIT_MODE)
        self.assertTrue(extended_deposit_quote(payload['quote']))
        payload['quote'].pop('deposit_authorization_mode')
        with self.assertRaisesRegex(ValueError, 'unterschriebene Vertrag'):
            signed_payload(payload)
        self.fixture.setUp()
        legacy = self.fixture.hold(deferred=False)
        contract, _, _ = signed_payload(json.loads(legacy['payload']))
        self.assertNotIn('deposit_authorization_mode', contract)

    def test_paused_gateway_keeps_extended_option_bound_to_the_frozen_quote(self):
        paid = self.long_paid(test_only=False)
        gateway = object.__new__(StripeLiveGateway)
        gateway.settlement_only = True
        live_hold = {**paid, 'session_id': 'cs_live_synthetic'}
        params = {'amount': 50000, 'currency': 'eur', 'capture_method': 'manual',
                  'payment_method_types': ['card'],
                  'metadata': {'hold_id': paid['id'], 'quote_hash': paid['fingerprint']},
                  'payment_method_options': {'card': {'request_extended_authorization': 'if_available'}}}
        key = 'mos-deposit-' + paid['id']
        with self.earliest_window(paid), patch.object(StripeTestGateway, 'create_deposit_intent', return_value={'id': 'pi_synthetic'}) as create:
            self.assertEqual(gateway.create_existing_deposit_intent(params, key, live_hold)['id'], 'pi_synthetic')
            create.assert_called_once_with(params, key)
            wrong = {name: value for name, value in params.items() if name != 'payment_method_options'}
            with self.assertRaises(ValueError):
                gateway.create_existing_deposit_intent(wrong, key, live_hold)


if __name__ == '__main__':
    unittest.main()
