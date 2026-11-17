"""Offline retail publication contract. Synthetic rows; no providers or files."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_catalog_public import CatalogError, PublicCatalog, variant_fingerprint

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
PHOTO = 'bilder/' + 'a' * 64 + '.png'
IMAGE = '/shop/media/' + 'a' * 64 + '.png'


def wheel(letter='a', **changes):
    row = {
        'selection_id': 'alloy_wheel:' + letter * 24,
        'category': 'alloy_wheel', 'manufacturer_name': 'SYNTHETIC',
        'name': 'Synthetic Schwarz 18', 'size': '8 × 18 Zoll', 'diameter': '18',
        'photo': PHOTO, 'description': 'Public description',
        'details': {'Farbe': 'Black', 'Ausführung': '8x18 ET45 5x112', 'private_note': 'PRIVATE_CANARY'},
        'quote_public': {
            'source': 'ALZURA', 'observed_at': (NOW - timedelta(minutes=1)).isoformat(),
            'expires_at': (NOW + timedelta(minutes=14)).isoformat(), 'stock_reported': 20,
            'prices_by_quantity': {str(q): {'quantity': q, 'unit_gross_cent': 12345,
                                          'items_gross_cent': 12345 * q, 'vat_percent': '19.00',
                                          'net_purchase_cent': 1} for q in range(1, 5)},
            'supplier_secret': 'PRIVATE_CANARY'},
        'provider_id': 'PRIVATE_CANARY', 'net_price': 'PRIVATE_CANARY',
        'private_evidence': {'token': 'PRIVATE_CANARY'},
        'vehicle_candidates': [{'make': 'Audi', 'model': 'A3', 'year': 2016}],
        'fitment_status': 'approved'
    }
    row.update(changes)
    return row


def part(letter='f', **changes):
    row = {'id': 'parts:' + letter * 24, 'category': 'oil', 'name': 'Synthetic Motoröl',
           'department': 'accessories', 'brand': 'SYNTHETIC', 'photo': PHOTO, 'unit_label': '5 Liter',
           'price': {'unit_gross_cent': 4567, 'stock': 30, 'base_price_unit': 'L',
                     'base_price_gross_cent': 913, 'observed_at': (NOW - timedelta(seconds=1)).isoformat(),
                     'expires_at': (NOW + timedelta(minutes=10)).isoformat(), 'net_price': 'PRIVATE_CANARY'},
           'provider_id': 'PRIVATE_CANARY'}
    row.update(changes)
    return row


def catalogue(rows=None, **kwargs):
    rows = [wheel()] if rows is None else list(rows)
    ids = [row.get('selection_id', row.get('id')) for row in rows if isinstance(row, dict)]
    return PublicCatalog(rows, released_ids=kwargs.pop('released_ids', ids), clock=lambda: NOW, **kwargs)


def verified(row):
    return {'reviewed': True, 'variant_fingerprint': variant_fingerprint(row), 'report_keys': ['REPORT'],
            'vehicles': [{'make': 'Audi', 'model': 'A3', 'year_from': 2013, 'year_to': 2020,
                          'hsn': '0588', 'tsn': 'BHT', 'conditions': ['RDKS and exact variant conditions apply.']}],
            'private_vin': 'PRIVATE_CANARY'}


class PublicCatalogueTests(unittest.TestCase):
    def test_cache_only_rsu_retail_projection_is_current_and_never_exposes_private_fields(self):
        row = wheel(quote_public=None)
        quote = wheel()['quote_public']
        quote['source'] = 'RSU'
        cached = lambda identifier: quote
        c = catalogue([row], quote_lookup=cached)
        result = c.get(row['selection_id'])
        self.assertEqual(result['price_status'], 'current')
        self.assertEqual(result['price']['prices_by_quantity']['1']['unit_gross_cent'], 12345)
        self.assertNotIn('PRIVATE_CANARY', json.dumps(result))
        self.assertNotIn('net_purchase', json.dumps(result))
        quote['expires_at'] = NOW.isoformat()
        self.assertEqual(c.get(row['selection_id'])['price_status'], 'update_required')

    def test_publication_allowlist_is_exact_and_hides_counts_and_search(self):
        first, private = wheel(), wheel('b', name='Unreleased private search term')
        c = catalogue([first, private], released_ids={first['selection_id']})
        self.assertEqual(c.page()['total'], 1)
        self.assertEqual(c.page(q='Unreleased')['total'], 0)
        self.assertIsNone(c.get(private['selection_id']))
        self.assertEqual(catalogue([first], released_ids=[]).page()['total'], 0)
        with self.assertRaises(CatalogError):
            catalogue([first], released_ids={first['selection_id'] + 'x'})

    def test_recursive_projection_does_not_echo_private_data(self):
        c = catalogue([wheel(), part()], image_urls={PHOTO: IMAGE})
        output = json.dumps(c.page(), ensure_ascii=False)
        self.assertNotIn('PRIVATE_CANARY', output)
        for forbidden in ('net_price', 'net_purchase', 'provider_id', 'private_evidence', 'vehicle_candidates', 'token'):
            self.assertNotIn(forbidden, output)
        self.assertEqual(c.get(wheel()['selection_id'])['images'], [IMAGE])
        self.assertFalse(c.get(wheel()['selection_id'])['order_enabled'])

    def test_input_and_output_mutation_cannot_change_snapshot(self):
        row = wheel()
        c = catalogue([row])
        row['quote_public']['prices_by_quantity']['1']['unit_gross_cent'] = 9
        row['name'] = 'Changed source'
        out = c.get(row['selection_id'])
        out['price']['prices_by_quantity']['1']['unit_gross_cent'] = 1
        out['fitment']['vehicles'].append({'make': 'INJECTED'})
        fresh = c.get(row['selection_id'])
        self.assertEqual(fresh['name'], 'Synthetic Schwarz 18')
        self.assertEqual(fresh['price']['prices_by_quantity']['1']['unit_gross_cent'], 12345)
        self.assertEqual(fresh['fitment']['vehicles'], [])

    def test_freshness_is_rechecked_and_expired_amounts_are_not_published(self):
        row = wheel()
        c = catalogue([row])
        self.assertEqual(c.get(row['selection_id'])['price_status'], 'current')
        deadline = NOW + timedelta(minutes=14)
        self.assertIsNone(c.get(row['selection_id'], now=deadline)['price'])
        expired = c.page(now=deadline)['items'][0]
        self.assertEqual(expired['price_status'], 'update_required')
        self.assertNotIn('12345', json.dumps(expired))

    def test_future_naive_or_overlong_quotes_are_not_current(self):
        variants = [
            {'observed_at': (NOW + timedelta(seconds=1)).isoformat(), 'expires_at': (NOW + timedelta(minutes=10)).isoformat()},
            {'observed_at': '2026-11-16T11:59:00'},
            {'expires_at': (NOW + timedelta(minutes=14, microseconds=1)).isoformat()},
            {'expires_at': (NOW - timedelta(minutes=2)).isoformat()}
        ]
        for change in variants:
            with self.subTest(change=change):
                row = wheel()
                row['quote_public'].update(change)
                self.assertIsNone(catalogue([row]).get(row['selection_id'])['price'])
        row = wheel()
        row['quote_public']['observed_at'] = NOW.isoformat()
        row['quote_public']['expires_at'] = (NOW + timedelta(minutes=15)).isoformat()
        self.assertEqual(catalogue([row]).get(row['selection_id'])['price_status'], 'current')

    def test_invalid_cents_stock_and_quantity_lines_cannot_be_published(self):
        for value in (True, 0, -1, 12.5, float('nan'), float('inf'), 10_000_001):
            with self.subTest(value=value):
                row = wheel()
                for line in row['quote_public']['prices_by_quantity'].values():
                    line['unit_gross_cent'] = value
                self.assertIsNone(catalogue([row]).get(row['selection_id'])['price'])
        row = wheel()
        row['quote_public']['stock_reported'] = True
        self.assertIsNone(catalogue([row]).get(row['selection_id'])['price'])
        row = wheel()
        row['quote_public']['prices_by_quantity']['1']['items_gross_cent'] += 1
        out = catalogue([row]).get(row['selection_id'])['price']['prices_by_quantity']
        self.assertNotIn('1', out)
        self.assertIn('4', out)

    def test_parts_retail_base_price_and_review_pending(self):
        row = part()
        out = catalogue([row]).get(row['id'])
        self.assertEqual(out['department'], 'accessories')
        self.assertEqual(out['price']['base_price_gross_cent'], 913)
        self.assertEqual(out['price']['prices_by_quantity']['20']['items_gross_cent'], 4567 * 20)
        row['price_status'] = 'review_pending'
        self.assertIsNone(catalogue([row]).get(row['id'])['price'])

    def test_explicit_currency_or_vat_conflicts_are_not_relabelled_as_euro_retail(self):
        for factory, field in ((wheel, 'quote_public'), (part, 'price')):
            for change in ({'currency': 'USD'}, {'vat_percent': '0.00'}, {'currency': None}, {'vat_percent': True}):
                with self.subTest(category=factory()['category'], change=change):
                    row = factory()
                    row[field].update(change)
                    key = row.get('selection_id', row.get('id'))
                    self.assertIsNone(catalogue([row]).get(key)['price'])
            row = factory()
            row[field].update(currency='eur', vat_percent='19.00')
            key = row.get('selection_id', row.get('id'))
            self.assertEqual(catalogue([row]).get(key)['price']['currency'], 'EUR')

    def test_quantity_line_currency_conflict_cannot_become_a_euro_price(self):
        row = wheel()
        row['quote_public']['prices_by_quantity']['4']['currency'] = 'USD'
        price = catalogue([row]).get(row['selection_id'])['price']
        self.assertNotIn('4', price['prices_by_quantity'])
        self.assertIn('1', price['prices_by_quantity'])
        for line in row['quote_public']['prices_by_quantity'].values():
            line['currency'] = 'USD'
        self.assertIsNone(catalogue([row]).get(row['selection_id'])['price'])

    def test_media_requires_explicit_safe_public_mapping(self):
        row = wheel()
        for value in ('https://example.invalid/photo.png?token=PRIVATE_CANARY', '//example.invalid/a.png',
                      '/shop/../private/a.png', 'file:///C:/a.png', 'data:image/png,abc', '/shop/a.png?token=x'):
            with self.subTest(value=value):
                self.assertEqual(catalogue([row], image_urls={PHOTO: value}).get(row['selection_id'])['images'], [])
        self.assertEqual(catalogue([row]).get(row['selection_id'])['images'], [])

    def test_pagination_is_bounded_stable_and_does_not_overlap(self):
        rows = [wheel(format(i, 'x'), name='Same name') for i in range(12)]
        c = catalogue(reversed(rows), released_ids=[r['selection_id'] for r in rows])
        first, second = c.page(page_size=5), c.page(page=2, page_size=5)
        self.assertEqual(first['total'], 12)
        self.assertTrue(first['has_more'])
        self.assertEqual(len(first['items']), 5)
        self.assertFalse(set(r['id'] for r in first['items']) & set(r['id'] for r in second['items']))
        self.assertEqual(c.page(page=4, page_size=5)['items'], [])
        self.assertFalse(c.page(page=4, page_size=5)['has_more'])
        for params in ({'page': True}, {'page': 0}, {'page': 1.0}, {'page_size': 49}, {'page_size': -1}):
            with self.subTest(params=params), self.assertRaises(CatalogError):
                c.page(**params)

    def test_search_only_indexes_public_fields_and_matches_all_terms(self):
        c = catalogue([wheel(), wheel('b', name='Other red model'), part()])
        self.assertEqual(c.page(q='SYNTHETIC schwarz')['total'], 1)
        self.assertEqual(c.page(q='PRIVATE_CANARY')['total'], 0)
        self.assertEqual(c.page(category='oil')['total'], 1)
        self.assertEqual(c.page(department='wheels')['total'], 2)
        self.assertEqual(c.page(brand='synthetic')['total'], 3)
        for params in ({'q': 'x' * 101}, {'category': {}}, {'department': []}, {'brand': True}):
            with self.subTest(params=params), self.assertRaises(CatalogError):
                c.page(**params)

    def test_candidates_are_never_vehicle_approval_or_filter_matches(self):
        row = wheel()
        c = catalogue([row])
        self.assertEqual(c.get(row['selection_id'])['fitment']['status'], 'not_confirmed')
        self.assertEqual(c.page(make='Audi')['total'], 0)
        self.assertEqual(c.page(hsn='0588', tsn='BHT')['total'], 0)

    def test_exact_reviewed_variant_and_released_report_enable_only_documented_vehicles(self):
        row = wheel()
        evidence = verified(row)
        c = catalogue([row], verified_fitments={row['selection_id']: evidence}, document_urls={'REPORT': '/shop/reports/report.pdf'})
        self.assertEqual(c.page(make='Audi')['total'], 1)
        self.assertEqual(c.page(make='Audi', model='A3', year=2016)['total'], 1)
        self.assertEqual(c.page(make='Audi', model='A3', year=2012)['total'], 0)
        self.assertEqual(c.page(hsn='0588', tsn='BHT')['total'], 1)
        self.assertEqual(c.page(hsn='0588', tsn='ZZZ')['total'], 0)
        out = c.get(row['selection_id'])['fitment']
        self.assertEqual(out['reports'], ['/shop/reports/report.pdf'])
        self.assertEqual(out['status'], 'reviewed_with_conditions')
        self.assertNotIn('PRIVATE_CANARY', json.dumps(out))
        self.assertEqual(out['vehicles'][0]['conditions'], evidence['vehicles'][0]['conditions'])

    def test_fitment_fails_closed_for_wrong_geometry_unreviewed_or_unreleased_reports(self):
        row = wheel()
        evidence = verified(row)
        changed = deepcopy(row)
        changed['details']['Ausführung'] = '8x18 ET35 5x112'
        for supplied, documents in ((evidence, {}), ({**evidence, 'reviewed': 'true'}, {'REPORT': '/a.pdf'}),
                                    ({**evidence, 'variant_fingerprint': '0' * 64}, {'REPORT': '/a.pdf'})):
            c = catalogue([row], verified_fitments={row['selection_id']: supplied}, document_urls=documents)
            self.assertEqual(c.page(make='Audi')['total'], 0)
        c = catalogue([changed], verified_fitments={row['selection_id']: evidence}, document_urls={'REPORT': '/a.pdf'})
        self.assertEqual(c.page(make='Audi')['total'], 0)
        self.assertNotEqual(variant_fingerprint(row), variant_fingerprint(changed))
        rebranded = deepcopy(row)
        rebranded['manufacturer_name'] = 'Different manufacturer'
        self.assertNotEqual(variant_fingerprint(row), variant_fingerprint(rebranded))

    def test_conditions_are_never_silently_shortened_and_bad_detail_labels_are_ignored(self):
        row = wheel(detail_fields=[{'label': [], 'value': 'bad label'}])
        evidence = verified(row)
        evidence['vehicles'][0]['conditions'] = ['A' * 300 + ' EXCEPT ALL WHEEL DRIVE']
        c = catalogue([row], verified_fitments={row['selection_id']: evidence}, document_urls={'REPORT': '/report.pdf'})
        self.assertEqual(c.page()['total'], 1)
        self.assertEqual(c.page(make='Audi')['total'], 0)
        evidence['vehicles'][0]['conditions'] = ['A' * 270 + ' EXCEPT ALL WHEEL DRIVE']
        c = catalogue([row], verified_fitments={row['selection_id']: evidence}, document_urls={'REPORT': '/report.pdf'})
        self.assertTrue(c.page()['items'][0]['fitment']['vehicles'][0]['conditions'][0].endswith('EXCEPT ALL WHEEL DRIVE'))

    def test_partial_or_inconsistent_vehicle_queries_are_rejected(self):
        c = catalogue()
        for params in ({'model': 'A3'}, {'make': 'Audi', 'year': 2016}, {'hsn': '0588'}, {'tsn': 'BHT'},
                       {'hsn': '588', 'tsn': 'BHT'}, {'make': 'Audi', 'model': 'A3', 'year': True}):
            with self.subTest(params=params), self.assertRaises(CatalogError):
                c.page(**params)

    def test_pages_do_not_repeat_full_detail_records_for_large_fitment_sets(self):
        row = wheel(description='x' * 2000)
        evidence = verified(row)
        evidence['vehicles'] = [dict(evidence['vehicles'][0], model='Model ' + str(n),
                                     conditions=['condition ' + str(i) + 'x' * 280 for i in range(20)]) for n in range(128)]
        c = catalogue([row], verified_fitments={row['selection_id']: evidence}, document_urls={'REPORT': '/report.pdf'})
        listed = c.page()['items'][0]
        self.assertLessEqual(len(listed['description']), 280)
        self.assertEqual(len(listed['fitment']['vehicles']), 3)
        self.assertEqual(listed['fitment']['more_vehicles'], 125)
        self.assertEqual(listed['fitment']['vehicles'][0]['more_conditions'], 17)
        self.assertLess(len(json.dumps(listed)), 10_000)
        self.assertEqual(len(c.get(row['selection_id'])['fitment']['vehicles']), 128)

    def test_duplicate_or_mismatched_ids_fail_closed(self):
        row = wheel()
        with self.assertRaises(CatalogError):
            catalogue([row, deepcopy(row)])
        wrong = wheel(category='tyre')
        self.assertEqual(catalogue([wrong]).page()['total'], 0)
        for value in ('alloy_wheel:' + 'a' * 23, row['selection_id'] + 'x', True, {}):
            with self.subTest(value=value), self.assertRaises(CatalogError):
                catalogue().get(value)

    def test_module_does_not_read_paths_or_connect_to_network(self):
        row = wheel()
        with patch('builtins.open', side_effect=AssertionError('file access forbidden')), \
                patch.object(socket, 'socket', side_effect=AssertionError('network forbidden')):
            c = catalogue([row])
            self.assertEqual(c.page()['total'], 1)
            self.assertIsNotNone(c.get(row['selection_id']))


if __name__ == '__main__':
    unittest.main()
