"""Explicit merchant prices, exact approved SKU whitelist, cache-only stock."""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
from shop_parts_fixed_freight import FixedPartsRetailFreight, RULE
from shop_parts_freight import PartsFreightError, _hash
from shop_freight import RSURetailFreight
import test_shop_parts_freight as observation_freight_tests
from test_shop_rsu_parts_evidence import SID, IDENTITY, EAN, ARTICLE
from test_shop_cart_quotes import NOW, TYRE


class FixedPartsFreightChecks(unittest.TestCase):
    def setUp(self):
        self.f = observation_freight_tests.PartsFreightChecks(); self.f.setUp(); self.addCleanup(self.f.doCleanups)
        self.record = {'rule': RULE, 'product_id': SID, 'identity_hash': IDENTITY,
            'binding_evidence_id': self.f.binding['mapping']['evidence_id'], 'supplier_article_id': ARTICLE,
            'ean': EAN, 'shipping_class': 'standard', 'direct_gross_cent': 821, 'reshipment_gross_cent': 1642,
            'quantity_min': 1, 'quantity_max': 20, 'merchant_cost_difference_accepted': True,
            'source_ref': self.f.references[0], 'policy_evidence_id': 'user-fixed-parts-freight-20261118'}
        self.seal(self.record)

    def seal(self, value):
        value.pop('proof_sha256', None); value['proof_sha256'] = _hash(value)

    def tariff(self, *, record=None, **changes):
        args = dict(bindings={SID: self.f.binding}, mode='test', destination_eligibility=self.f.eligibility,
            retained_inquiry=self.f.provider.retained_inquiry, source_refs=self.f.references, clock=lambda: self.f.now)
        args.update(changes)
        return FixedPartsRetailFreight({SID: self.record if record is None else record}, **args)

    def warm(self, quantity=1):
        self.assertIsNotNone(self.f.provider.retail_quote(self.f.product, quantity))

    def test_constructor_and_empty_cache_make_no_supplier_calls(self):
        tariff = self.tariff()
        self.assertEqual(self.f.raw_inquiry.calls, [])
        self.assertIsNone(tariff(self.f.shipment(), self.f.context))
        self.assertEqual(self.f.raw_inquiry.calls, [])

    def test_exact_position_direct_and_collection_charge_821_with_no_extra_read(self):
        self.warm(); tariff = self.tariff()
        for destination in ('customer', 'workshop'):
            result = tariff(self.f.shipment(destination=destination), self.f.context)
            self.assertEqual(result['gross_cent'], 821)
            self.assertEqual(result['expires_at'], (NOW + timedelta(minutes=15)).isoformat())
        self.assertEqual(self.f.raw_inquiry.calls, [(ARTICLE, 1)])

    def test_reshipment_is_1642_and_requires_both_delivery_legs(self):
        self.warm(); context = deepcopy(self.f.context); context['workshop_delivery'] = 'customer_reshipment'
        result = self.tariff()(self.f.shipment(destination='workshop', context=context), context)
        self.assertEqual(result['gross_cent'], 1642)
        context['delivery']['postcode'] = '25980'
        self.assertIsNone(self.tariff()(self.f.shipment(destination='workshop', context=context), context))
        self.assertEqual(self.f.raw_inquiry.calls, [(ARTICLE, 1)])

    def test_fee_is_per_sku_position_and_never_multiplied_by_quantity(self):
        self.warm(2); rows = self.f.shipment()['private_items']; rows[0]['quantity'] = 2
        result = self.tariff()(self.f.shipment(rows=rows), self.f.context)
        self.assertEqual(result['gross_cent'], 821)
        self.assertEqual(self.f.raw_inquiry.calls, [(ARTICLE, 2)])
        rows[0]['quantity'] = 1  # no exact q1 retained observation
        self.assertIsNone(self.tariff()(self.f.shipment(rows=rows), self.f.context))

    def test_higher_goods_cost_or_changed_stock_uses_current_verified_retained_observation(self):
        self.warm(); self.f.raw_inquiry.changes = {'price': Decimal('75'), 'stock': 2}
        self.assertIsNotNone(self.f.provider.fresh_inquiry(self.f.shipment()['private_items'][0], 1))
        result = self.tariff()(self.f.shipment(), self.f.context)
        self.assertEqual(result['gross_cent'], 821)  # merchant price, no invented supplier-cost claim
        self.assertEqual(len(self.f.raw_inquiry.calls), 2)
        self.f.raw_inquiry.changes = {'stock': 0}
        self.assertIsNone(self.f.provider.fresh_inquiry(self.f.shipment()['private_items'][0], 1))
        self.assertIsNone(self.tariff()(self.f.shipment(), self.f.context))
        self.assertEqual(len(self.f.raw_inquiry.calls), 3)

    def test_policy_has_no_invented_ui_expiry_but_actual_stock_observation_still_expires(self):
        self.warm(); tariff = self.tariff(); self.f.now += timedelta(minutes=16)
        self.assertIsNone(tariff(self.f.shipment(), self.f.context))
        self.warm()
        result = tariff(self.f.shipment(), self.f.context)
        self.assertEqual(result['gross_cent'], 821)
        self.assertEqual(result['expires_at'], (self.f.now + timedelta(minutes=15)).isoformat())
        self.assertEqual(len(self.f.raw_inquiry.calls), 2)

    def test_explicit_whitelist_identity_and_merchant_risk_acceptance_are_mandatory(self):
        for changes in ({'shipping_class': 'parts_standard'}, {'shipping_class': 'bulky'},
                        {'merchant_cost_difference_accepted': False}, {'merchant_cost_difference_accepted': 1},
                        {'direct_gross_cent': 0}, {'direct_gross_cent': 822}, {'reshipment_gross_cent': 1643},
                        {'quantity_max': 48}, {'quantity_min': True}, {'supplier_article_id': '900002'},
                        {'binding_evidence_id': self.f.mapping['evidence_id']}, {'identity_hash': 'f' * 64},
                        {'ean': '4046283759835'}, {'expires_at': (NOW + timedelta(days=5)).isoformat()}):
            record = {**deepcopy(self.record), **changes}; self.seal(record)
            with self.subTest(changes=changes), self.assertRaises(PartsFreightError): self.tariff(record=record)
        with self.assertRaises(PartsFreightError): self.tariff(source_refs=[])

    def test_unknown_part_or_wrong_snapshot_binding_never_falls_through_as_standard(self):
        self.warm(); rows = self.f.shipment()['private_items']
        for changes in ({'id': 'parts:' + '2' * 24}, {'identity_hash': 'f' * 64},
                        {'mapping': {**rows[0]['mapping'], 'supplier_article_id': '900002'}}):
            changed = deepcopy(rows); changed[0].update(changes)
            self.assertIsNone(self.tariff()(self.f.shipment(rows=changed), self.f.context))
        self.assertEqual(self.f.raw_inquiry.calls, [(ARTICLE, 1)])

    def test_unknown_mixed_shipping_returns_no_partial_total(self):
        self.warm(); rows = self.f.shipment()['private_items'] + [{'id': TYRE, 'identity_hash': 'c' * 64,
            'quantity': 1, 'mapping': {'supplier_article_id': '123', 'evidence_id': 'f' * 64}}]
        self.assertIsNone(self.tariff()(self.f.shipment(rows=rows), self.f.context))
        fallback = RSURetailFreight({TYRE: 'pkw_tyre'}, mode='test', basis='net', evidence_id='SYNTHETIC-tariff',
            checked_at=NOW.isoformat(), destination_eligibility=self.f.eligibility, clock=lambda: self.f.now)
        self.assertEqual(self.tariff(fallback=fallback)(self.f.shipment(rows=rows), self.f.context)['gross_cent'], 1642)
        self.assertEqual(self.f.raw_inquiry.calls, [(ARTICLE, 1)])

    def test_no_cross_country_island_packstation_or_changed_context_price(self):
        self.warm(); tariff = self.tariff()
        for changes in ({'country': 'AT'}, {'postcode': '25980'}, {'street': 'Packstation 123'}):
            context = deepcopy(self.f.context); context['delivery'].update(changes)
            self.assertIsNone(tariff(self.f.shipment(context=context), context))
        changed = deepcopy(self.f.context); changed['delivery']['street'] = 'Changed Street 1'
        self.assertIsNone(tariff(self.f.shipment(), changed))

    def test_untrusted_retained_callback_is_still_checked_without_refreshing_it(self):
        self.warm(); result = self.f.provider.retained_inquiry(self.f.shipment()['private_items'][0], 1)
        for change in ({'quantity': 2}, {'mapping_evidence_id': 'f' * 64}, {'unit_net': 'unknown'}, {'stock': 0},
                       {'mode': 'live'}, {'expected_ean': '4046283759835'}, {'confirmed': 1},
                       {'observed_at': (NOW + timedelta(seconds=1)).isoformat()}):
            value = {**deepcopy(result), **change}
            tariff = self.tariff(retained_inquiry=lambda *args: value)
            self.assertIsNone(tariff(self.f.shipment(), self.f.context), change)
        self.assertEqual(self.f.raw_inquiry.calls, [(ARTICLE, 1)])


if __name__ == '__main__':
    unittest.main(verbosity=2)
