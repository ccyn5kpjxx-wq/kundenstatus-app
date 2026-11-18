"""Retail shipping arithmetic and exact shipment/context binding, no IO."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shop_freight import (RSURetailFreight, MainlandDeliveryEligibility,
                         KNOWN_ISLAND_POSTCODES, shipping_gross_cent, _hash)

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

    @staticmethod
    def customer_context(postcode='10115', **extra):
        return {'delivery': {'recipient': 'Synthetic Buyer', 'street': 'Teststraße 12',
                            'postcode': postcode, 'city': 'Berlin', 'country': 'DE'}, **extra}

    def test_mainland_extension_requires_both_literal_server_optin_and_saved_true(self):
        for flag in (None, False, 'true', 1, True):
            policy = MainlandDeliveryEligibility(['74821'], mainland_delivery_approved=flag)
            for declaration in (None, False, True):
                context = self.customer_context()
                if declaration is not None:
                    context['delivery_mainland'] = declaration
                self.assertIs(policy('customer', context), flag is True and declaration is True)
            # The existing area remains available without new declarations.
            self.assertTrue(policy('customer', self.customer_context('74821')))
            self.assertTrue(policy('customer', self.customer_context('74821', delivery_mainland=False)))
        for value in ('true', 1, [], None):
            self.assertFalse(MainlandDeliveryEligibility(['74821'], mainland_delivery_approved=True)(
                'customer', self.customer_context('74821', delivery_mainland=value)))

    def test_islands_foreign_and_unsupported_addresses_override_declaration_and_legacy_area(self):
        policy = MainlandDeliveryEligibility(['74821', *KNOWN_ISLAND_POSTCODES], mainland_delivery_approved=True)
        for postcode in KNOWN_ISLAND_POSTCODES:
            self.assertFalse(policy('customer', self.customer_context(postcode, delivery_mainland=True)))
        for field, value in (('country', 'AT'), ('street', 'Packstation 123'),
                             ('recipient', 'Packstation 123'), ('city', 'Postfach'),
                             ('postcode', '00000'), ('street', 'Ohne Hausnummer')):
            context = self.customer_context('74821', delivery_mainland=True)
            context['delivery'][field] = value
            with self.subTest(field=field, value=value):
                self.assertFalse(policy('customer', context))
        context = self.customer_context(delivery_mainland=True)
        del context['delivery']['city']
        self.assertFalse(policy('customer', context))

    def test_mainland_selection_changes_freight_context_hash_without_changing_tariff(self):
        self.context = self.customer_context(delivery_mainland=True)
        policy = MainlandDeliveryEligibility([], mainland_delivery_approved=True)
        provider = self.service(destination_eligibility=policy)
        group = self.shipment([(TYRE, 1)], destination='customer')
        self.assertEqual(provider(group, self.context)['gross_cent'], 821)
        self.assertEqual(provider(self.shipment([(TYRE, 4)], 'customer'), self.context)['gross_cent'], 0)
        self.context['delivery_mainland'] = False
        self.assertIsNone(provider(group, self.context))
        self.assertIsNone(provider(self.shipment([(TYRE, 1)], 'customer'), self.context))
        self.context['delivery_mainland'] = True
        self.context['delivery']['street'] = 'Andere Straße 99'
        self.assertIsNone(provider(group, self.context))
        self.assertEqual(provider(self.shipment([(TYRE, 1)], 'customer'), self.context)['gross_cent'], 821)

    def test_workshop_collection_and_reshipment_keep_their_independent_guards(self):
        policy = MainlandDeliveryEligibility([], mainland_delivery_approved=True)
        provider = self.service(destination_eligibility=policy)
        self.context = self.customer_context('27498', delivery_mainland=False)
        self.assertEqual(provider(self.shipment([(TYRE, 4)]), self.context)['gross_cent'], 0)
        self.context['workshop_delivery'] = 'customer_reshipment'
        self.assertIsNone(provider(self.shipment([(TYRE, 1)]), self.context))
        self.context = self.customer_context(delivery_mainland=True, workshop_delivery='customer_reshipment')
        self.assertEqual(provider(self.shipment([(TYRE, 1)]), self.context)['gross_cent'], 1642)
        # Free supplier inbound freight proves no price for workshop outbound.
        self.assertIsNone(provider(self.shipment([(TYRE, 4)]), self.context))

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

    def test_two_shipping_legs_only_for_explicit_workshop_reshipment(self):
        for choice in (None, 'collection', 'customer_reshipment'):
            if choice is None:self.context.pop('workshop_delivery', None)
            else:self.context['workshop_delivery'] = choice
            customer = self.provider(self.shipment([(TYRE, 1)], 'customer'), self.context)
            workshop = self.provider(self.shipment([(TYRE, 1)], 'workshop'), self.context)
            self.assertEqual(customer['gross_cent'], 821)
            self.assertEqual(workshop['gross_cent'], 1642 if choice == 'customer_reshipment' else 821)
        # Free supplier inbound freight does not prove free workshop outbound.
        self.assertIsNone(self.provider(self.shipment([(TYRE, 2)]), self.context))
        self.context['workshop_delivery'] = 'collection'
        self.assertEqual(self.provider(self.shipment([(TYRE, 2)]), self.context)['gross_cent'], 0)
        self.context['workshop_delivery'] = 'customer_reshipment'
        self.assertEqual(self.provider(self.shipment([(TYRE, 2)], 'customer'), self.context)['gross_cent'], 0)

    def test_workshop_reshipment_doubles_base_before_markup_vat_and_final_rounding(self):
        ids = ['parts:' + f'{i:024x}' for i in range(1, 6)]
        self.classes.update({pid: 'parts_standard' for pid in ids})
        self.context['workshop_delivery'] = 'customer_reshipment'
        quoted = self.service()(self.shipment([(pid, 1) for pid in ids]), self.context)
        # Five €6 NET orders, two legs: €60 * 1.15 * 1.19 = €82.11.
        self.assertEqual(quoted['gross_cent'], 8211)
        self.assertNotEqual(quoted['gross_cent'], 2 * 4106)
        self.assertEqual(self.service()(self.shipment([(BODY, 1)]), self.context)['gross_cent'], 16422)

    def test_workshop_choice_is_bound_to_context_and_invalid_enum_cannot_reduce_fee(self):
        original = self.shipment([(TYRE, 1)])
        self.context['workshop_delivery'] = 'customer_reshipment'
        self.assertIsNone(self.provider(original, self.context))
        valid = self.shipment([(TYRE, 1)])
        self.assertEqual(self.provider(valid, self.context)['gross_cent'], 1642)
        for invalid in (None, '', True, [], 'customer', 'pickup'):
            self.context['workshop_delivery'] = invalid
            with self.subTest(choice=invalid):
                self.assertIsNone(self.provider(self.shipment([(TYRE, 1)]), self.context))

    def test_reshipment_requires_customer_eligibility_in_addition_to_workshop(self):
        calls=[]
        def eligible(destination, context):
            calls.append(destination)
            return destination == 'workshop'
        provider=self.service(destination_eligibility=eligible)
        self.context['workshop_delivery']='customer_reshipment'
        self.assertIsNone(provider(self.shipment([(TYRE, 1)]), self.context))
        self.assertEqual(calls,['workshop','customer'])
        self.context['workshop_delivery']='collection'
        self.assertEqual(provider(self.shipment([(TYRE, 1)]), self.context)['gross_cent'],821)

    def test_actual_cart_quote_preserves_buyer_choice_in_shipping_price_and_frozen_hash(self):
        from shop_buyer import validate_context
        from test_shop_cart_quotes import Fixture, TYRE as CART_TYRE, NOW as CART_NOW
        fixture=Fixture()
        freight=RSURetailFreight({CART_TYRE:'pkw_tyre'},mode='test',basis='net',
            evidence_id='SYNTHETIC_TWO_LEG_POLICY',checked_at=CART_NOW,
            destination_eligibility=lambda *args:True,clock=lambda:CART_NOW)
        service=fixture.service_instance(freight_quote=freight,clock=lambda:CART_NOW)
        address={'recipient':'Synthetic Buyer','street':'Teststraße 12','postcode':'74821','city':'Mosbach','country':'DE'}
        payload={'contact':{'name':'Synthetic Buyer','email':'buyer@example.test'},'billing':address,'delivery':None}
        one=validate_context({**payload,'workshop_delivery':'collection'})
        two=validate_context({**payload,'workshop_delivery':'customer_reshipment'})
        cart={'lines':[{'id':CART_TYRE,'quantity':1,'services':{'mounting':True}}]}
        first=service.quote(cart,customer_id='synthetic-customer',vehicle_ref='CAR-1',delivery_context=one)
        second=service.quote(cart,customer_id='synthetic-customer',vehicle_ref='CAR-1',delivery_context=two)
        self.assertTrue(first.snapshot()['ready']);self.assertTrue(second.snapshot()['ready'])
        self.assertEqual(first.snapshot()['shipments'][0]['gross_cent'],821)
        self.assertEqual(second.snapshot()['shipments'][0]['gross_cent'],1642)
        self.assertEqual(second.snapshot()['total_gross_cent']-first.snapshot()['total_gross_cent'],821)
        self.assertNotEqual(first.quote_hash,second.quote_hash)

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
