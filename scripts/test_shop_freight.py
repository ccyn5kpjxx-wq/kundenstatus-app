"""Retail shipping arithmetic and exact shipment/context binding, no IO."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_freight import RSURetailFreight, shipping_gross_cent, _hash

NOW = datetime(2026, 11, 17, tzinfo=timezone.utc)
TYRE, OTHER, RIM, OIL, BODY = ['tyre:' + c * 24 for c in ('a', 'b')] + ['alloy_wheel:' + 'c' * 24] + ['parts:' + c * 24 for c in ('d', 'e')]


class FreightChecks(unittest.TestCase):
    def setUp(self):
        self.context = {'delivery': {'country': 'DE', 'postcode': '74821'}}
        self.classes = {TYRE: 'pkw_tyre', OTHER: 'pkw_tyre', RIM: 'pkw_rim',
                        OIL: 'mounting_supply', BODY: 'parts_bulky'}
        self.provider = self.service()

    def service(self, **kwargs):
        return RSURetailFreight(self.classes, **{'mode': 'test', 'basis': 'net',
            'evidence_id': 'SYNTHETIC_RETAIL_TARIFF', 'checked_at': NOW,
            'destination_eligibility': lambda destination, context: True, 'clock': lambda: NOW, **kwargs})

    def shipment(self, items, destination='workshop'):
        group = {'id': destination, 'destination': destination,
            'private_items': [{'id': key, 'quantity': quantity, 'identity_hash': 'f' * 64,
                              'mapping': {'supplier_article_id': 'SYNTHETIC'}} for key, quantity in items],
            'context_hash': _hash(self.context)}
        return {**group, 'shipment_hash': _hash(group)}

    def test_markup_applies_to_shipping_only_with_explicit_basis_and_rounding(self):
        self.assertEqual(shipping_gross_cent(600, basis='net'), 821)
        self.assertEqual(shipping_gross_cent(6000, basis='net'), 8211)
        self.assertEqual(shipping_gross_cent(1500, basis='net'), 2053)
        self.assertEqual(shipping_gross_cent(600, basis='gross'), 690)
        self.assertEqual(shipping_gross_cent(0, basis='net'), 0)
        for amount in (-1, True, 600.0):
            with self.assertRaises(ValueError):
                shipping_gross_cent(amount, basis='net')
        with self.assertRaises(ValueError):
            shipping_gross_cent(600, basis=None)

    def test_one_supplier_sku_quantity_two_is_free_but_distinct_single_skus_are_not(self):
        single = self.provider(self.shipment([(TYRE, 1)]), self.context)
        self.assertEqual(single['gross_cent'], 821)
        self.assertEqual(self.provider(self.shipment([(TYRE, 2)]), self.context)['gross_cent'], 0)
        self.assertEqual(self.provider(self.shipment([(TYRE, 1), (OTHER, 1)]), self.context)['gross_cent'], 1642)

    def test_mixed_set_and_accessory_shipping_and_bulky_replacement(self):
        self.assertEqual(self.provider(self.shipment([(TYRE, 4), (RIM, 4)]), self.context)['gross_cent'], 0)
        self.assertEqual(self.provider(self.shipment([(TYRE, 4), (OIL, 2)]), self.context)['gross_cent'], 821)
        self.assertEqual(self.provider(self.shipment([(BODY, 1)]), self.context)['gross_cent'], 8211)
        self.assertEqual(self.provider(self.shipment([(BODY, 1), (OIL, 1)]), self.context)['gross_cent'], 9032)

    def test_sensor_ring_and_flat_fees_apply_per_actual_api_order(self):
        for category in ('rdks_sensor', 'centring_ring'):
            self.classes[TYRE] = self.classes[OTHER] = category
            provider = self.service()
            self.assertEqual(provider(self.shipment([(TYRE, 4)]), self.context)['gross_cent'], 0)
            self.assertEqual(provider(self.shipment([(TYRE, 2), (OTHER, 2)]), self.context)['gross_cent'], 1642)
        for category, expected in (('mounting_supply', 1642), ('parts_standard', 1642), ('parts_bulky', 16422)):
            self.classes[TYRE] = self.classes[OTHER] = category
            provider = self.service()
            self.assertEqual(provider(self.shipment([(TYRE, 2), (OTHER, 2)]), self.context)['gross_cent'], expected)
        self.classes[TYRE], self.classes[OTHER] = 'parts_bulky', 'parts_standard'
        self.assertEqual(self.service()(self.shipment([(TYRE, 1), (OTHER, 1)]), self.context)['gross_cent'], 9032)

    def test_shipping_markup_rounds_once_after_summing_order_net_charges(self):
        # Five separate €6 NET supplier orders: €30 *1.15 *1.19 = €41.055
        # -> €41.06. Summing five individually rounded €8.21 charges would
        # incorrectly change our one-shipment retail rounding policy by 1cent.
        ids = ['parts:' + f'{i:024x}' for i in range(1, 6)]
        self.classes.update({pid: 'parts_standard' for pid in ids})
        result = self.service()(self.shipment([(pid, 1) for pid in ids]), self.context)
        self.assertEqual(result['gross_cent'], 4106)
        self.assertNotEqual(result['gross_cent'], 5 * shipping_gross_cent(600, basis='net'))

    def test_each_destination_has_its_own_order_threshold_and_unaggregated_duplicates_block(self):
        for destination in ('workshop', 'customer'):
            self.assertEqual(self.provider(self.shipment([(TYRE, 1)], destination), self.context)['gross_cent'], 821)
        self.assertIsNone(self.provider(self.shipment([(TYRE, 1), (TYRE, 1)]), self.context))

    def test_unknown_tax_class_or_destination_never_becomes_free(self):
        group = self.shipment([(TYRE, 4)])
        self.assertIsNone(self.service(basis=None)(group, self.context))
        without_basis = RSURetailFreight(self.classes, mode='test', evidence_id='SYNTHETIC_RETAIL_TARIFF',
            checked_at=NOW, destination_eligibility=lambda *args: True, clock=lambda: NOW)
        self.assertIsNone(without_basis(group, self.context))
        self.assertIsNone(self.service(destination_eligibility=None)(group, self.context))
        self.assertIsNone(self.service(destination_eligibility=lambda *args: False)(group, self.context))
        # An unconfirmed island/package count is ineligible. One API position
        # never proves one physical parcel, so no inferred island charge.
        self.assertIsNone(self.service(destination_eligibility=lambda *args: None)(group, self.context))
        unknown = 'parts:' + 'f' * 24
        self.assertIsNone(self.provider(self.shipment([(unknown, 1)]), self.context))

    def test_exact_quantities_address_and_destination_are_bound(self):
        group = self.shipment([(TYRE, 1)])
        changed = deepcopy(group)
        changed['private_items'][0]['quantity'] = 2
        self.assertIsNone(self.provider(changed, self.context))
        self.assertIsNone(self.provider(group, {'delivery': {'country': 'DE', 'postcode': '12345'}}))
        changed = {**group, 'destination': 'customer'}
        self.assertIsNone(self.provider(changed, self.context))
        self.assertEqual(self.provider(self.shipment([(TYRE, 1)], 'customer'), self.context)['gross_cent'], 821)

    def test_old_or_future_tariff_review_blocks_new_shipping_commitment(self):
        group = self.shipment([(TYRE, 1)])
        for checked in (NOW - timedelta(days=30), NOW + timedelta(seconds=1)):
            self.assertIsNone(self.service(checked_at=checked)(group, self.context))


if __name__ == '__main__':
    unittest.main()
