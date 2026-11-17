"""Exact-size tyre purchase is not an asserted vehicle approval. Offline only."""
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shop_cart_quotes import QuoteInputError
from test_shop_cart_quotes import Fixture, TYRE, RIM, BODY, line


class ExactTyrePurchaseChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('No provider or payment network'))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.fixture = Fixture()
        self.fixture.products[TYRE].update(fitment_required=False, fitment=None)

    def quote(self, rows, *, vehicle='', **changes):
        return self.fixture.service_instance(**changes).quote({'lines': rows}, customer_id='synthetic',
            vehicle_ref=vehicle, delivery_context={'postcode': '74821', 'address_version': 'synthetic'})

    @staticmethod
    def codes(value):
        return {reason['code'] for reason in value.public()['blocked_reasons']}

    def test_exact_standalone_tyre_can_be_quoted_without_vehicle_approval(self):
        value = self.quote([line(TYRE, 4)])
        public, frozen = value.public(), value.snapshot()
        self.assertTrue(public['ready'])
        self.assertEqual(public['lines'][0]['quantity'], 4)
        self.assertEqual(public['lines'][0]['destination'], 'customer')
        self.assertEqual(public['total_gross_cent'], 4 * 13090 + 500)
        self.assertEqual(frozen['vehicle_ref'], '')
        self.assertEqual(frozen['assembly_evidence'], {})
        self.assertNotIn('fitment_unconfirmed', self.codes(value))
        self.assertFalse(public['order_enabled'])
        self.assertFalse(public['payment_enabled'])
        for key in ('vehicle_ref', 'fitment', 'fitment_confirmed', 'assembly_evidence'):
            self.assertNotIn(key, public)
            self.assertNotIn(key, public['lines'][0])

    def test_explicit_tyre_fitment_requirement_still_needs_exact_proof(self):
        self.fixture.products[TYRE]['fitment_required'] = True
        self.assertIn('fitment_unconfirmed', self.codes(self.quote([line(TYRE)])))
        p = self.fixture.products[TYRE]
        p['fitment'] = {'confirmed': True, 'identity_hash': p['identity_hash'], 'vehicle_ref': 'CAR-1', 'evidence_id': 'SYNTHETIC'}
        self.assertIn('fitment_unconfirmed', self.codes(self.quote([line(TYRE)], vehicle='other-car')))
        self.assertTrue(self.quote([line(TYRE)], vehicle='CAR-1').public()['ready'])

    def test_nonboolean_fitment_metadata_never_becomes_size_only_permission(self):
        for malformed in (None, 0, 'false', [], {}):
            self.fixture.products[TYRE]['fitment_required'] = malformed
            with self.subTest(value=malformed):
                self.assertIn('fitment_unconfirmed', self.codes(self.quote([line(TYRE)])))

    def test_rims_and_body_parts_cannot_opt_out_of_confirmed_vehicle_fit(self):
        for identifier in (RIM, BODY):
            self.fixture.products[identifier].update(fitment_required=False, fitment=None)
            with self.subTest(product=identifier):
                self.assertIn('fitment_unconfirmed', self.codes(self.quote([line(identifier)], vehicle='CAR-1')))
        with self.assertRaises(QuoteInputError):
            self.quote([{**line(RIM), 'fitment_confirmed': True}])

    def test_complete_assembly_keeps_independent_proof_and_stock_guards(self):
        rows = [line(TYRE, 4, services={'mounting': True}, group='SYNTHETIC-set'), line(RIM, 4, group='SYNTHETIC-set')]
        missing = self.quote(rows, vehicle='CAR-1', assembly_fitment=None)
        self.assertIn('assembly_fitment_unconfirmed', self.codes(missing))
        self.assertFalse(missing.public()['ready'])
        self.assertTrue(self.quote(rows, vehicle='CAR-1').public()['ready'])
        self.fixture.stock[TYRE] = 3
        insufficient = self.quote([line(TYRE, 4)])
        self.assertFalse(insufficient.public()['ready'])
        self.assertIn('stock_insufficient_or_unconfirmed', self.codes(insufficient))


if __name__ == '__main__':
    unittest.main()
