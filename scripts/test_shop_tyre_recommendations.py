"""Offline same-size tyre price comparison; no compatibility authority."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_tyre_recommendations import select_priced_tyres, tyre_size_key

NOW = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)


def tyre(number=1, unit=1000, *, brand='Synthetic Brand', model=None, size=('225', '40', '18'), stock=12):
    return {'id': 'tyre:' + f'{number:024x}', 'category': 'tyre', 'department': 'tyres',
            'brand': brand, 'name': model or f'Synthetic Model {number}',
            'geometry': {'width': size[0], 'ratio': size[1], 'diameter': size[2],
                         'load_index': '92', 'speed_index': 'Y'},
            'size': f'{size[0]}/{size[1]} R{size[2]} 92Y',
            'purchase_status': 'supplier_check_required', 'order_enabled': False,
            'fitment': {'status': 'not_confirmed'}, 'price_status': 'current',
            'price': {'kind': 'merchant_pricebook', 'source': 'RSU', 'currency': 'EUR', 'vat_percent': '19.00',
                      'observed_at': (NOW - timedelta(days=1)).isoformat(),
                      'expires_at': (NOW + timedelta(days=6)).isoformat(),
                      'stock_reported': stock, 'stock_current': False,
                      'prices_by_quantity': {str(q): {'quantity': q, 'unit_gross_cent': unit,
                          'items_gross_cent': unit * q} for q in range(1, 5)}}}


class RecommendationChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('No network'))
        self.network.start(); self.addCleanup(self.network.stop)

    def choose(self, rows, **kwargs):
        return select_priced_tyres(rows, now=NOW, **kwargs)

    def test_structured_nominal_size_only_never_title_inference(self):
        product = tyre(); self.assertEqual(tyre_size_key(product), '225/40 R18')
        product['geometry']['width'] = '225.0'; self.assertEqual(tyre_size_key(product), '225/40 R18')
        for geometry in (None, {}, {'width': True, 'ratio': '40', 'diameter': '18'},
                         {'width': '225', 'ratio': 'forty', 'diameter': '18'},
                         {'width': '999', 'ratio': '40', 'diameter': '18'}):
            self.assertIsNone(tyre_size_key({**product, 'geometry': geometry}))
        self.assertIsNone(tyre_size_key({**product, 'category': 'alloy_wheel'}))

    def test_price_quantiles_include_extremes_and_are_sorted_cheap_to_expensive(self):
        rows = [tyre(n, n * 1000) for n in range(9, 0, -1)]
        result = self.choose(rows)
        self.assertEqual(result['available_sizes'], ['225/40 R18'])
        self.assertEqual(result['selected_size'], '225/40 R18')
        self.assertEqual([p['id'] for p in result['items']], [tyre(n)['id'] for n in (1, 3, 5, 7, 9)])
        self.assertEqual(self.choose(rows, limit=4)['items'][0]['id'], tyre(1)['id'])
        self.assertEqual(self.choose(rows, limit=4)['items'][-1]['id'], tyre(9)['id'])

    def test_same_model_variants_do_not_fill_comparison_and_keep_actual_specs(self):
        expensive = tyre(1, 2000, brand=' SYNTHETIC  Brand ', model=' Touring  Model ')
        cheap = tyre(2, 1500, brand='synthetic brand', model='touring model')
        cheap['geometry'].update(load_index='94', speed_index='W')
        rows = [expensive, cheap, tyre(3, 3000), tyre(4, 4000)]
        result = self.choose(rows)
        self.assertEqual(len(result['items']), 3)
        self.assertEqual(result['items'][0]['id'], cheap['id'])
        self.assertEqual(result['items'][0]['geometry']['load_index'], '94')
        self.assertEqual(result['items'][0]['geometry']['speed_index'], 'W')

    def test_exact_size_cohorts_are_not_mixed_to_reach_five(self):
        rows = [tyre(1, 1000), tyre(2, 2000), tyre(3, 500, size=('235', '45', '18'))]
        result = self.choose(rows)
        self.assertEqual(result['selected_size'], '225/40 R18')
        self.assertEqual(len(result['items']), 2)
        self.assertEqual(result['available_sizes'], ['225/40 R18', '235/45 R18'])
        explicit = self.choose(rows, size_key='235/45 R18')
        self.assertEqual([p['id'] for p in explicit['items']], [tyre(3)['id']])
        unknown = self.choose(rows, size_key='205/45 R18')
        self.assertEqual(unknown['items'], [])
        self.assertEqual(unknown['selected_size'], '205/45 R18')

    def test_default_cohort_ties_prefer_brand_diversity_then_price_then_numeric_size(self):
        rows = [tyre(1, 1000, brand='A'), tyre(2, 2000, brand='A'),
                tyre(3, 5000, brand='B', size=('235', '45', '18')),
                tyre(4, 6000, brand='C', size=('235', '45', '18'))]
        self.assertEqual(self.choose(rows)['selected_size'], '235/45 R18')
        rows[3]['brand'] = 'B'
        self.assertEqual(self.choose(rows)['selected_size'], '225/40 R18')
        rows[2]['price'] = deepcopy(rows[0]['price']); rows[3]['price'] = deepcopy(rows[1]['price'])
        self.assertEqual(self.choose(rows)['selected_size'], '225/40 R18')

    def test_no_unpriced_inquiry_unknown_source_or_wrong_stock_placeholders(self):
        base = tyre()
        bad = [None, {}, {**base, 'category': 'body_part'}, {**base, 'department': 'parts'},
               {**base, 'purchase_status': 'inquiry_only'}, {**base, 'price_status': 'update_required'},
               {**base, 'price': None}, {**base, 'brand': ''}, {**base, 'name': ''}]
        for change in ({'source': 'ALZURA'}, {'kind': 'partner_inquiry_pricebook'}, {'currency': 'USD'},
                       {'vat_percent': '7.00'}, {'stock_reported': 3}, {'stock_reported': True}):
            bad.append({**base, 'price': {**base['price'], **change}})
        self.assertEqual(self.choose(bad)['items'], [])
        self.assertIsNone(self.choose(bad)['selected_size'])

    def test_expired_future_or_retimed_prices_are_not_current(self):
        for change in ({'expires_at': NOW.isoformat()},
                       {'observed_at': (NOW+timedelta(seconds=1)).isoformat()},
                       {'expires_at': (NOW+timedelta(days=7)).isoformat()}, {'observed_at': 'no date'}):
            row = tyre(); row['price'].update(change)
            with self.subTest(change=change): self.assertEqual(self.choose([row])['items'], [])

    def test_requested_quantity_uses_its_actual_amount_and_stock(self):
        two_only = tyre(1, 500, stock=2); four = tyre(2, 2000, stock=4)
        self.assertEqual([p['id'] for p in self.choose([two_only, four])['items']], [four['id']])
        self.assertEqual([p['id'] for p in self.choose([two_only, four], quantity=2)['items']],
                         [two_only['id'], four['id']])
        for line in ({'quantity': True, 'unit_gross_cent': 1, 'items_gross_cent': 4},
                     {'quantity': 4, 'unit_gross_cent': 0, 'items_gross_cent': 0},
                     {'quantity': 4, 'unit_gross_cent': 1000, 'items_gross_cent': 3999},
                     {'quantity': 4, 'unit_gross_cent': '1000', 'items_gross_cent': 4000}):
            row = tyre(); row['price']['prices_by_quantity']['4'] = line
            self.assertEqual(self.choose([row])['items'], [])

    def test_inputs_outputs_and_authority_flags_are_independent(self):
        rows = [tyre(1, 1000), tyre(2, 2000)]; before = deepcopy(rows)
        first = self.choose(rows)
        self.assertEqual(rows, before)
        first['items'][0]['price']['prices_by_quantity']['4']['unit_gross_cent'] = 1
        first['items'][0]['geometry']['load_index'] = '150'
        second = self.choose(rows)
        self.assertEqual(second['items'], before)
        self.assertFalse(second['items'][0]['order_enabled'])
        self.assertEqual(second['items'][0]['fitment']['status'], 'not_confirmed')
        self.assertNotIn('quality_tier', second['items'][0])

    def test_order_ties_duplicates_and_conflicts_do_not_depend_on_input_order(self):
        rows = [tyre(n, 1000) for n in range(1, 9)]
        expected = self.choose(rows)
        self.assertEqual(self.choose(list(reversed(rows))), expected)
        self.assertEqual(self.choose([*rows, deepcopy(rows[0])]), expected)
        conflict = deepcopy(rows[0]); conflict['price']['stock_reported'] = 0
        self.assertNotIn(rows[0]['id'], [p['id'] for p in self.choose([*rows, conflict])['items']])

    def test_short_valid_pool_is_not_padded_and_limit_one_keeps_cheapest(self):
        rows = [tyre(2, 2000), tyre(1, 1000)]
        self.assertEqual(len(self.choose(rows)['items']), 2)
        self.assertEqual(self.choose(rows, limit=1)['items'][0]['id'], rows[1]['id'])
        self.assertEqual(self.choose([]), {'items': [], 'available_sizes': [], 'selected_size': None})

    def test_bad_bounds_clocks_and_size_syntax_rejected(self):
        for kwargs in ({'quantity': True}, {'quantity': 0}, {'quantity': 5}, {'limit': 0},
                       {'limit': 6}, {'limit': True}, {'size_key': '225/40R18'},
                       {'size_key': '225/40 R31'}, {'size_key': '225/40 R18\n'}, {'size_key': []}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError): self.choose([], **kwargs)
        for clock in (None, datetime(2026, 11, 18), 'bad'):
            with self.subTest(clock=clock), self.assertRaises(ValueError):
                select_priced_tyres([], now=clock)
        with self.assertRaises(ValueError): self.choose({})
        with self.assertRaises(ValueError): self.choose([None]*10001)


if __name__ == '__main__': unittest.main(verbosity=2)
