"""Card-hold gateway regression tests; only synthetic Stripe responses are used."""
from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mos_booking.gateway import OfflineGateway, StripeTestGateway, validate_deposit_intent


PARAMS = {'amount': 50_000, 'currency': 'eur', 'capture_method': 'manual',
          'payment_method_types': ['card'], 'metadata': {'hold_id': 'synthetic-hold'}}


def intent(status='requires_payment_method', **changes):
    data = {'id': 'pi_test_synthetic', 'object': 'payment_intent', 'livemode': False,
            'amount': 50_000, 'currency': 'eur', 'capture_method': 'manual',
            'payment_method_types': ['card'], 'amount_capturable': 0,
            'amount_received': 0, 'status': status,
            'client_secret': 'pi_test_synthetic_secret_synthetic', 'latest_charge': None}
    if status == 'requires_capture':
        data['amount_capturable'] = 50_000
        data['latest_charge'] = {'id': 'ch_test_synthetic', 'object': 'charge',
                                 'payment_intent': data['id'], 'currency': 'eur',
                                 'amount': 50_000, 'captured': False,
                                 'payment_method_details': {'card': {
                                     'funding': 'credit',
                                     'capture_before': int(time.time()) + 86400}}}
    data.update(changes)
    return data


class DepositGatewayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 1_800_000_000
        self.gateway = OfflineGateway(Path(self.tmp.name) / 'synthetic.sqlite',
                                      'http://localhost', 'whsec_synthetic',
                                      clock=lambda: self.now)

    def test_offline_hold_can_be_released_without_charging_card(self):
        created = self.gateway.create_deposit_intent(PARAMS, 'create-1')
        self.assertEqual(created['status'], 'requires_payment_method')
        self.assertEqual(created['amount_capturable'], 0)
        self.assertIsNone(created['capture_before'])
        authorized = self.gateway.authorize_deposit_intent(created['id'], 5*86400)
        self.assertEqual(authorized['status'], 'requires_capture')
        self.assertEqual(authorized['amount_capturable'], 50_000)
        self.assertEqual(authorized['amount_received'], 0)
        self.assertEqual((authorized['card_funding'], authorized['credit_eligible']),
                         ('credit', True))
        self.assertEqual(datetime.fromisoformat(authorized['capture_before']).tzinfo, timezone.utc)
        self.assertEqual(self.gateway.retrieve_deposit_intent(created['id'])['capture_before'],
                         authorized['capture_before'])
        canceled = self.gateway.cancel_deposit_intent(created['id'], 'release-1')
        self.assertEqual((canceled['status'], canceled['amount_received'], canceled['amount_capturable']),
                         ('canceled', 0, 0))
        self.assertEqual(self.gateway.cancel_deposit_intent(created['id'], 'release-1'), canceled)
        with self.assertRaises(ValueError):
            self.gateway.capture_deposit_intent(created['id'], 100, 'capture-after-release')

    def test_expired_hold_is_not_represented_as_active(self):
        created = self.gateway.create_deposit_intent(PARAMS, 'create-2')
        self.gateway.authorize_deposit_intent(created['id'], 60)
        self.now += 60
        expired = self.gateway.retrieve_deposit_intent(created['id'])
        self.assertEqual(expired['status'], 'canceled')
        self.assertIsNone(expired['capture_before'])

    def test_debit_or_prepaid_hold_can_be_released_without_rent_charge(self):
        for funding in ('debit', 'prepaid', 'unknown'):
            with self.subTest(funding=funding):
                created = self.gateway.create_deposit_intent(PARAMS, 'create-'+funding)
                authorized = self.gateway.authorize_deposit_intent(created['id'], funding=funding)
                self.assertEqual(authorized['card_funding'], funding)
                self.assertFalse(authorized['credit_eligible'])
                released = self.gateway.cancel_deposit_intent(created['id'], 'release-'+funding)
                self.assertEqual((released['status'], released['amount_received']), ('canceled', 0))

    def test_partial_damage_capture_is_explicit_and_idempotent(self):
        created = self.gateway.create_deposit_intent(PARAMS, 'create-3')
        self.gateway.authorize_deposit_intent(created['id'])
        captured = self.gateway.capture_deposit_intent(created['id'], 12_345, 'damage-1')
        self.assertEqual((captured['status'], captured['amount_received']), ('succeeded', 12_345))
        self.assertEqual(self.gateway.capture_deposit_intent(created['id'], 12_345, 'damage-1'), captured)
        with self.assertRaises(ValueError):
            self.gateway.capture_deposit_intent(created['id'], 12_346, 'damage-1')
        with self.assertRaises(ValueError):
            self.gateway.cancel_deposit_intent(created['id'], 'late-release')

    def test_idempotency_and_unapproved_payment_methods_fail_closed(self):
        self.gateway.create_deposit_intent(PARAMS, 'create-4')
        with self.assertRaises(ValueError):
            self.gateway.create_deposit_intent({**PARAMS, 'metadata': {'hold_id': 'other'}}, 'create-4')
        for bad in ({**PARAMS, 'capture_method': 'automatic'},
                    {**PARAMS, 'payment_method_types': ['card', 'link']},
                    {**PARAMS, 'amount': 1}, {**PARAMS, 'confirm': True}):
            with self.assertRaises(ValueError):
                self.gateway.create_deposit_intent(bad, 'bad-'+str(len(bad)))

    def test_provider_response_requires_full_matching_hold_and_real_expiry(self):
        valid = intent('requires_capture')
        self.assertTrue(validate_deposit_intent(valid, False)['capture_before'].endswith('+00:00'))
        for bad in (
            {**valid, 'livemode': True},
            {**valid, 'amount': 50_001},
            {**valid, 'capture_method': 'automatic'},
            {**valid, 'payment_method_types': ['card', 'link']},
            {**valid, 'amount_capturable': 49_999},
            {**valid, 'amount_received': 50_000},
            {**valid, 'latest_charge': 'ch_unexpanded'},
            {**valid, 'latest_charge': {**valid['latest_charge'], 'payment_intent': 'pi_other'}},
            {**valid, 'latest_charge': {**valid['latest_charge'],
                                        'payment_method_details': {'card': {}}}},
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                validate_deposit_intent(bad, False)

    def test_stripe_uses_independent_manual_capture_intent(self):
        with patch('mos_booking.gateway.stripe.StripeClient') as constructor:
            gateway = StripeTestGateway('rk_test_synthetic_no_network')
            api = constructor.return_value.v1.payment_intents
            api.create.return_value.to_dict.return_value = intent()
            created = gateway.create_deposit_intent(PARAMS, 'create-stripe')
            self.assertEqual(created['status'], 'requires_payment_method')
            api.create.assert_called_once_with({**PARAMS, 'expand': ['latest_charge']},
                                               options={'idempotency_key': 'create-stripe'})
            api.retrieve.return_value.to_dict.return_value = intent('requires_capture')
            self.assertEqual(gateway.retrieve_deposit_intent('pi_test_synthetic')['amount_capturable'], 50_000)
            api.retrieve.assert_called_once_with('pi_test_synthetic', {'expand': ['latest_charge']})
            api.cancel.return_value.to_dict.return_value = intent('canceled')
            self.assertEqual(gateway.cancel_deposit_intent('pi_test_synthetic', 'release-stripe')['status'],
                             'canceled')
            api.cancel.assert_called_once_with('pi_test_synthetic',
                                               options={'idempotency_key': 'release-stripe'})
            api.capture.return_value.to_dict.return_value = intent('succeeded', amount_received=100)
            self.assertEqual(gateway.capture_deposit_intent('pi_test_synthetic', 100, 'damage-stripe')
                             ['amount_received'], 100)
            api.capture.assert_called_once_with('pi_test_synthetic', {'amount_to_capture': 100},
                                                options={'idempotency_key': 'damage-stripe'})


if __name__ == '__main__':
    unittest.main()
