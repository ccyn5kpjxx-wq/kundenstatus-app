"""Existing authorizations keep their original amount across the tyre pricing change."""
import json
import unittest

from test_shop_fulfillment import SagaFixture, TYRE
from test_shop_order_store import rehash
from shop_fulfillment import FulfillmentError
from shop_order_store import OrderStoreError, _validate_snapshot
from shop_production_payments import _frozen_price_service, requote_remaining_goods
from shop_tyre_pricing import tyre_unit_gross_cent


def adjusted(snapshot, base_change=0):
    for line in snapshot['lines']:
        if line['category'] != 'tyre':
            continue
        base = line['unit_gross_cent'] + base_change
        line['base_unit_gross_cent'] = base
        gross = tyre_unit_gross_cent(base)
        difference = (gross - line['unit_gross_cent']) * line['quantity']
        line['unit_gross_cent'] = gross
        line['items_gross_cent'] += difference
        snapshot['known_subtotal_gross_cent'] += difference
        snapshot['total_gross_cent'] += difference
    return rehash(snapshot)


class FrozenTyrePriceChecks(SagaFixture, unittest.TestCase):
    def saved(self):
        return self.store.snapshot_for_order(self.order['id'], customer_id='customer-a', mode='test')

    def refresh_with_current_policy(self, base_change=0):
        def price(product, quantity):
            result = self.f.retail(product, quantity)
            if product['category'] == 'tyre':
                result['unit_gross_cent'] += base_change
            return result
        current = self.f.service_instance(clock=lambda: self.now, tyre_payment_costs=True, retail_quote=price)
        def revalidate(snapshot, context, *, destinations):
            requests = [request for request, line in zip(snapshot['request'], snapshot['lines'])
                        if line['destination'] in destinations]
            return current.quote({'lines': requests}, customer_id=snapshot['customer_id'],
                                 vehicle_ref=snapshot['vehicle_ref'], delivery_context=context).snapshot()
        self.saga.revalidate = revalidate

    def test_old_authorization_fulfills_at_exact_original_amount_after_policy_change(self):
        original = self.saved()
        self.assertNotIn('base_unit_gross_cent', next(row for row in original['lines'] if row['id'] == TYRE))
        self.refresh_with_current_policy()
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(self.saved(), original)
        self.assertEqual(self.order['amount_gross_cent'], original['total_gross_cent'])

    def test_base_merchandise_increase_still_rejects_legacy_order(self):
        self.refresh_with_current_policy(base_change=1)
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertEqual(self.transport.calls, [])

    def test_new_tyre_price_allowance_cannot_subsidize_increased_procurement_cost(self):
        original = adjusted(self.saved())
        self.refresh_with_current_policy()
        children = [dict(child, item_json=json.dumps(child['item'])) for child in self.saga._children(original)]
        self.inquiry_edits = lambda result: result.update(unit_net='100.10') if result['product_id'] == TYRE else None
        with self.assertRaisesRegex(FulfillmentError, 'procurement_revalidation_failed'):
            self.saga._fresh_plan(original, self.context, children)
        self.assertEqual(self.transport.calls, [])

    def test_corrupt_frozen_pricing_basis_is_rejected_before_procurement(self):
        original = adjusted(self.saved())
        row = next(line for line in original['lines'] if line['id'] == TYRE)
        for bad_base in (True, -1, row['unit_gross_cent'], row['base_unit_gross_cent'] + 1):
            row['base_unit_gross_cent'] = bad_base
            corrupt = rehash(original)
            with self.assertRaisesRegex(OrderStoreError, 'invalid_quote_lines'):
                _validate_snapshot(corrupt, corrupt['quote_hash'], 'customer-a', 'test')

    def test_legacy_no_pricebook_path_uses_purchased_quantity_and_preserves_shared_service(self):
        frozen = self.saved()
        common = self.f.service_instance(clock=lambda: self.now, tyre_payment_costs=True)
        quantity_price = self.f.retail
        calls = []
        def legacy_lookup(product, quantity):
            calls.append((product['id'], quantity))
            return quantity_price(product, quantity)
        selected = _frozen_price_service(common, frozen, legacy_lookup)
        self.assertIsNot(selected, common)
        self.assertFalse(selected.tyre_payment_costs)
        self.assertTrue(common.tyre_payment_costs)
        self.assertIs(_frozen_price_service(common, frozen, None), common)
        self.assertIs(_frozen_price_service(common, adjusted(self.saved()), legacy_lookup), common)
        remaining, _ = requote_remaining_goods(common, frozen['request'], frozen, self.context,
                                                frozen['shipments'], legacy_retail_quote=legacy_lookup)
        self.assertEqual(remaining['lines'], frozen['lines'])
        self.assertIn((TYRE, 4), calls)


if __name__ == '__main__':
    unittest.main()
