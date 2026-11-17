"""Synthetic source-bound enrichment and public cache warming boundaries."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_catalog_public import CatalogError, variant_fingerprint
from shop_public_api import create_shop_public_blueprint
from test_shop_catalog_public import NOW, PHOTO, IMAGE, wheel, part


def tyre(letter='d', season='Sommerreifen'):
    return {'selection_id': 'tyre:' + letter * 24, 'category': 'tyre', 'name': 'SYNTHETIC tyre',
            'manufacturer_name': 'SYNTHETIC', 'size': '205/55 R18 91V', 'width': '205', 'ratio': '55',
            'diameter': '18', 'load_index': '91', 'speed_index': 'V', 'season': season,
            'dimensions_valid': True, 'photo': PHOTO, 'quote_public': None}


def pair(rim, tire):
    return {'reviewed': True, 'tyre_id': tire.get('selection_id', tire.get('id')),
            'rim_identity': variant_fingerprint(rim), 'tyre_identity': variant_fingerprint(tire)}


def catalogue(rows, **kwargs):
    return EnrichedPublicCatalog(rows, released_ids=kwargs.pop('released_ids', [r.get('selection_id', r.get('id')) for r in rows]),
                                  image_urls={PHOTO: IMAGE}, clock=lambda: NOW, **kwargs)


class EnrichmentChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()

    def tearDown(self):
        self.network.stop()

    def test_only_exact_procurement_membership_removes_market_price_label(self):
        a, b = wheel(), wheel('b')
        result = catalogue([a, b], procurement_ids={a['selection_id']})
        self.assertEqual(result.get(a['selection_id'])['purchase_status'], 'supplier_check_required')
        inquiry = result.get(b['selection_id'])
        self.assertEqual(inquiry['purchase_status'], 'inquiry_only')
        self.assertEqual(inquiry['price_label'], 'Richtpreis')
        self.assertIsNotNone(inquiry['price'])
        self.assertEqual(catalogue([a]).get(a['selection_id'])['purchase_status'], 'inquiry_only')

    def test_rsu_only_policy_hides_unbound_imports_and_never_consults_their_cache(self):
        bound, unbound, oil = wheel(), wheel('b'), part()
        bound['quote_public']['source'] = 'RSU'
        calls = []
        def cached(identifier):
            calls.append(identifier)
            # A market cache might still contain a recent numeric value from
            # an older release. Membership, rather than this value, controls it.
            return deepcopy(bound['quote_public'] if identifier == bound['selection_id']
                            else unbound['quote_public'])
        catalog = catalogue([bound, unbound, oil], procurement_ids={bound['selection_id']},
                            rsu_only_prices=True, quote_lookup=cached)
        for product in (catalog.get(unbound['selection_id']), catalog.get(oil['id']),
                        *catalog.page()['items'], *catalog.variants(bound['selection_id'])['items']):
            if product['id'] == bound['selection_id']:
                self.assertEqual(product['price_status'], 'current')
                self.assertEqual(product['price']['prices_by_quantity']['1']['unit_gross_cent'], 12345)
            else:
                self.assertIsNone(product['price'])
                self.assertEqual(product['price_status'], 'not_offered')
                self.assertEqual(product['price_label'], '')
                self.assertFalse(product['order_enabled'])
        self.assertTrue(calls)
        self.assertEqual(set(calls), {bound['selection_id']})
        # Without any cache callback, imported market prices are still hidden.
        imported = catalogue([unbound], rsu_only_prices=True).get(unbound['selection_id'])
        self.assertIsNone(imported['price'])
        self.assertEqual(imported['price_status'], 'not_offered')

    def test_rsu_only_policy_applies_to_tyre_recommendations_without_changing_scope(self):
        rim, tire = wheel(), tyre()
        tire['quote_public'] = deepcopy(rim['quote_public'])
        catalog = catalogue([rim, tire], procurement_ids={rim['selection_id']}, rsu_only_prices=True,
                            compatible_pairs={rim['selection_id']: [pair(rim, tire)]})
        result = catalog.compatible_tyres(rim['selection_id'], season='summer')
        self.assertEqual(result['scope'], 'rim_width_candidate')
        self.assertEqual(len(result['items']), 1)
        self.assertEqual(result['items'][0]['price_status'], 'not_offered')
        self.assertIsNone(result['items'][0]['price'])
        for invalid in (None, 'true', 1):
            with self.subTest(invalid=invalid), self.assertRaises(CatalogError):
                catalogue([rim], rsu_only_prices=invalid)

    def test_exact_rim_dimensions_are_public_without_private_proof_or_vehicle_promise(self):
        row = wheel()
        identifier = row['selection_id']
        geometry = {'width': '8', 'diameter': '18', 'offset': '45',
                    'bolt_count': '5', 'pcd': '112', 'centre_bore': '66.6'}
        proof = {'confirmed': True, 'product_id': identifier,
                 'identity_hash': variant_fingerprint(row), 'supplier': 'RSU',
                 'evidence_id': 'PRIVATE_PROOF_CANARY', 'geometry': geometry,
                 'model_photo_only': True,
                 'supplier_article_id': 'PRIVATE_SUPPLIER_CANARY'}
        catalog = catalogue([row], procurement_ids={identifier}, standalone_rims={identifier: proof})
        product = catalog.get(identifier)
        self.assertEqual(product['sale_dimensions'], geometry)
        self.assertEqual(product['image_note'], 'Modellfoto. Größe und genaue Ausführung können von der Abbildung abweichen.')
        self.assertEqual(product['fitment']['status'], 'not_confirmed')
        self.assertNotIn('PRIVATE_PROOF_CANARY', json.dumps(product))
        self.assertNotIn('PRIVATE_SUPPLIER_CANARY', json.dumps(product))
        for changed in ({'identity_hash': '0' * 64}, {'product_id': 'alloy_wheel:' + 'b' * 24},
                        {'confirmed': False}, {'supplier': 'OTHER'}, {'geometry': {'diameter': '18'}}):
            rejected = catalogue([row], procurement_ids={identifier},
                                 standalone_rims={identifier: {**proof, **changed}})
            self.assertNotIn('sale_dimensions', rejected.get(identifier))
        self.assertNotIn('sale_dimensions', catalogue([row], standalone_rims={identifier: proof}).get(identifier))

    def test_grouped_design_prefers_exact_procurement_variant_and_keeps_its_own_price(self):
        first, sourced = wheel(), wheel('b')
        sourced['quote_public']['prices_by_quantity']['1']['unit_gross_cent'] = 25000
        sourced['quote_public']['prices_by_quantity']['1']['items_gross_cent'] = 25000
        identifier = sourced['selection_id']
        proof = {'confirmed': True, 'product_id': identifier,
                 'identity_hash': variant_fingerprint(sourced), 'supplier': 'RSU',
                 'evidence_id': 'synthetic-proof', 'geometry': {'width': '8', 'diameter': '18',
                 'offset': '45', 'bolt_count': '5', 'pcd': '112', 'centre_bore': '66.6'}}
        catalog = catalogue([first, sourced], procurement_ids={identifier}, standalone_rims={identifier: proof})
        page = catalog.page(group='family')
        self.assertEqual(page['total'], 1)
        self.assertEqual(page['items'][0]['id'], identifier)
        self.assertEqual(page['items'][0]['family']['variant_count'], 2)
        self.assertEqual(page['items'][0]['price']['prices_by_quantity']['1']['unit_gross_cent'], 25000)
        self.assertEqual(catalog.variants(first['selection_id'])['items'][0]['id'], identifier)

    def test_authoritative_cache_never_falls_back_to_imported_price(self):
        rim = wheel()
        for lookup in (None, lambda i: None, lambda i: {}, lambda i: 1 / 0):
            with self.subTest(lookup=lookup):
                result = catalogue([rim], procurement_ids={rim['selection_id']},
                    quote_lookup=lookup, quote_lookup_authoritative=True).get(rim['selection_id'])
                self.assertIsNone(result['price'])
                self.assertEqual(result['price_status'], 'update_required')

    def test_complete_browse_and_family_variants_only_include_exact_documented_candidates(self):
        a, no_pair, native = wheel(diameter='17'), wheel('b', diameter='20'), wheel('c', diameter='18')
        summer, winter = tyre('d'), tyre('e', 'Winterreifen')
        identifier = native['selection_id']
        proof = {'confirmed': True, 'product_id': identifier, 'identity_hash': variant_fingerprint(native),
                 'supplier': 'RSU', 'evidence_id': 'synthetic-proof', 'geometry': {'width': '8', 'diameter': '18',
                 'offset': '45', 'bolt_count': '5', 'pcd': '112', 'centre_bore': '66.6'}}
        catalog = catalogue([a, no_pair, native, summer, winter], procurement_ids={identifier},
                            standalone_rims={identifier: proof}, compatible_pairs={
                                a['selection_id']: [pair(a, summer)], identifier: [pair(native, winter)]})
        self.assertEqual(catalog.page(department='wheels')['total'], 3)
        self.assertEqual(catalog.page(department='wheels', group='family')['items'][0]['family']['variant_count'], 3)
        result = catalog.page(department='wheels', group='family', complete='1')
        self.assertEqual(result['total'], 1)
        self.assertEqual(result['items'][0]['id'], identifier)
        self.assertEqual(result['items'][0]['family'], {'variant_count': 2, 'diameters': ['17', '18']})
        self.assertEqual(result['items'][0]['tyre_seasons'], ['winter'])
        variants = catalog.variants(no_pair['selection_id'], complete='1')
        self.assertEqual(variants['total'], 2)
        self.assertEqual([item['id'] for item in variants['items']], [identifier, a['selection_id']])
        self.assertEqual(catalog.variants(identifier, complete='1', diameter='20')['total'], 0)
        self.assertEqual(catalog.variants(identifier)['total'], 3)

    def test_complete_seasons_exclude_stale_unreleased_wrong_category_and_unknown_classification(self):
        rim, stale, unknown, oil = wheel(), tyre(), tyre('e', None), part()
        good = tyre('c', 'Ganzjahresreifen')
        stale_pair = {**pair(rim, stale), 'tyre_identity': '0' * 64}
        hidden = tyre('b', 'Winterreifen')
        catalog = catalogue([rim, stale, unknown, oil, good, hidden],
            released_ids=[r.get('selection_id', r.get('id')) for r in (rim, stale, unknown, oil, good)],
            compatible_pairs={rim['selection_id']: [stale_pair, pair(rim, oil), pair(rim, hidden),
                                                   pair(rim, unknown), pair(rim, good), pair(rim, good)]})
        self.assertEqual(catalog.get(rim['selection_id'])['tyre_seasons'], ['allseason'])
        self.assertEqual(len(catalog.compatible_tyres(rim['selection_id'], season='allseason')['items']), 1)
        self.assertEqual(catalog.compatible_tyres(rim['selection_id'], season='summer')['items'], [])
        without = catalogue([rim, stale, unknown], compatible_pairs={rim['selection_id']: [stale_pair, pair(rim, unknown)]})
        self.assertEqual(without.page(department='wheels', complete='1')['total'], 0)
        self.assertEqual(without.page(department='wheels')['total'], 1)
        for value in ('0', '', True, 1):
            with self.assertRaises(CatalogError):
                catalog.page(complete=value)
            with self.assertRaises(CatalogError):
                catalog.variants(rim['selection_id'], complete=value)

    def test_original_tyre_documents_require_exact_variant_and_release_paths(self):
        tire = tyre()
        label, sheet = '/shop/documents/' + 'c' * 64 + '.jpg', '/shop/documents/' + 'd' * 64 + '.pdf'
        doc = {'identity_hash': variant_fingerprint(tire), 'tyre_label_url': label,
               'tyre_sheet_url': sheet, 'eprel_id': '12345', 'private': 'PRIVATE_CANARY'}
        metadata = {tire['selection_id']: doc}
        result = catalogue([tire], product_documents=metadata, document_urls={'label': label, 'sheet': sheet})
        self.assertEqual(result.get(tire['selection_id'])['product_documents'], {
            'tyre_label_url': label, 'tyre_sheet_url': sheet, 'eprel_id': '12345'})
        self.assertNotIn('PRIVATE_CANARY', json.dumps(result.get(tire['selection_id'])))
        metadata[tire['selection_id']]['identity_hash'] = 'f' * 64
        self.assertNotIn('product_documents', catalogue([tire], product_documents=metadata,
            document_urls={'label': label, 'sheet': sheet}).get(tire['selection_id']))

    def test_families_merge_dimensions_but_keep_finish_and_full_model_separate(self):
        a = wheel(name='SYNTHETIC Model Black')
        b = wheel('b', name=a['name'], diameter='19', size='8 × 19 Zoll')
        c = wheel('c', name=a['name'], details={'Farbe': 'Silver'})
        d = wheel('d', name='SYNTHETIC Other Model Black')
        catalog = catalogue([a, b, c, d])
        page = catalog.page(department='wheels', group='family')
        self.assertEqual(page['total'], 3)
        family = next(p for p in page['items'] if p['id'] == a['selection_id'])
        self.assertEqual(family['family'], {'variant_count': 2, 'diameters': ['18', '19']})
        self.assertEqual(catalog.variants(a['selection_id'])['total'], 2)
        self.assertNotIn('PRIVATE_CANARY', json.dumps(page))

    def test_unreleased_variant_does_not_inflate_family_or_variants(self):
        a, b = wheel(), wheel('b')
        catalog = catalogue([a, b], released_ids=[a['selection_id']])
        self.assertEqual(catalog.variants(a['selection_id'])['total'], 1)
        self.assertIsNone(catalog.variants(b['selection_id']))

    def test_photo_geometry_requires_review_and_exact_variant_and_photo(self):
        rim = wheel()
        face = {'reviewed': True, 'width': 800, 'height': 800, 'cx': 400, 'cy': 400,
                'rx': 350, 'ry': 350, 'rotation': 0, 'hub': {'x': 400, 'y': 400},
                'openings': [[[200, 200], [220, 200], [200, 230]]]}
        evidence = {rim['selection_id']: {'variant_fingerprint': variant_fingerprint(rim), 'photo': PHOTO, 'photo_face': face}}
        catalog = catalogue([rim], browsing_evidence=evidence)
        self.assertEqual(catalog.page(studio='1')['total'], 1)
        self.assertEqual(catalog.get(rim['selection_id'])['photo_face']['width'], 800)
        for key, value in (('variant_fingerprint', 'f' * 64), ('photo', 'bilder/' + 'f' * 64 + '.png')):
            bad = deepcopy(evidence)
            bad[rim['selection_id']][key] = value
            self.assertEqual(catalogue([rim], browsing_evidence=bad).page(studio='1')['total'], 0)

    def test_provider_vehicle_metadata_remains_distinct_from_reviewed_gutachten(self):
        body = part(category='body_part', department='parts', name='SYNTHETIC body')
        evidence = {body['id']: {'variant_fingerprint': variant_fingerprint(body), 'provider_reviewed': True,
            'provider_vehicles': [{'make': 'Audi', 'model': 'A3', 'year_from': 2013, 'year_to': 2020,
                                   'hsn': '0588', 'tsn': 'BHT', 'label': 'SYNTHETIC engine', 'private': 'PRIVATE_CANARY'}]}}
        catalog = catalogue([body], browsing_evidence=evidence)
        result = catalog.page(make='Audi', model='A3', year=2016)
        self.assertEqual(result['total'], 1)
        self.assertEqual(result['items'][0]['fitment']['status'], 'provider_vehicle')
        self.assertEqual(result['items'][0]['fitment']['reports'], [])
        self.assertEqual(catalog.page(make='BMW')['total'], 0)
        self.assertNotIn('PRIVATE_CANARY', json.dumps(result))

    def test_normalized_german_seasons_match_api_season_keys_without_vehicle_approval(self):
        rim, tire = wheel(), tyre()
        catalog = catalogue([rim, tire], compatible_pairs={rim['selection_id']: [pair(rim, tire)]})
        result = catalog.compatible_tyres(rim['selection_id'], season='summer', quantity=3)
        self.assertEqual([p['id'] for p in result['items']], [tire['selection_id']])
        self.assertEqual(result['scope'], 'rim_width_candidate')
        self.assertEqual(catalog.compatible_tyres(rim['selection_id'], season='winter')['items'], [])
        with self.assertRaises(CatalogError):
            catalog.compatible_tyres(rim['selection_id'], model='A3')

    def test_exact_pair_binding_and_tyre_category_are_required(self):
        rim, tire, oil = wheel(), tyre(), part()
        good = pair(rim, tire)
        bad = deepcopy(good)
        bad['tyre_identity'] = 'f' * 64
        catalog = catalogue([rim, tire, oil], compatible_pairs={rim['selection_id']: [bad, pair(rim, oil), good]})
        self.assertEqual([p['id'] for p in catalog.compatible_tyres(rim['selection_id'])['items']], [tire['selection_id']])

    def test_full_export_group_supplies_exact_season_without_changing_pair_identity_or_approval(self):
        rim = wheel()
        tyres = [tyre(letter, None) for letter in ('d', 'e', 'f')]
        for row, label in zip(tyres, ('Sommerreifen', 'Winterreifen', 'Ganzjahresreifen')):
            row['group'] = label
        originals = {row['selection_id']: variant_fingerprint(row) for row in tyres}
        catalog = catalogue([rim, *tyres], compatible_pairs={rim['selection_id']: [pair(rim, row) for row in tyres]})
        for row, season in zip(tyres, ('summer', 'winter', 'allseason')):
            result = catalog.compatible_tyres(rim['selection_id'], season=season)
            self.assertEqual([item['id'] for item in result['items']], [row['selection_id']])
            self.assertEqual(result['scope'], 'rim_width_candidate')
            self.assertNotIn('season', result['items'][0]['geometry'])
            self.assertEqual(variant_fingerprint(catalog._source_row(row['selection_id'])), originals[row['selection_id']])
            self.assertFalse(result['items'][0]['order_enabled'])

    def test_missing_unknown_or_conflicting_classification_never_infers_season_from_model_name(self):
        rim = wheel()
        missing = tyre('d', None)
        missing['name'] = 'Synthetic winter summer allseason model'
        unknown = tyre('e', None)
        unknown['group'] = 'Unknown source class'
        conflict = tyre('f', 'Winterreifen')
        conflict['group'] = 'Sommerreifen'
        catalog = catalogue([rim, missing, unknown, conflict],
                            compatible_pairs={rim['selection_id']: [pair(rim, row) for row in (missing, unknown, conflict)]})
        for season in ('summer', 'winter', 'allseason'):
            self.assertEqual(catalog.compatible_tyres(rim['selection_id'], season=season)['items'], [])
        self.assertEqual(len(catalog.compatible_tyres(rim['selection_id'])['items']), 3)

    def test_compatibility_input_is_detached_from_later_caller_mutation(self):
        rim, tire = wheel(), tyre()
        pairs = {rim['selection_id']: [pair(rim, tire)]}
        catalog = catalogue([rim, tire], compatible_pairs=pairs)
        pairs[rim['selection_id']].clear()
        rim['diameter'] = '22'
        self.assertEqual([p['id'] for p in catalog.compatible_tyres(rim['selection_id'])['items']], [tire['selection_id']])

    def test_group_projection_rechecks_expiry_and_never_enables_orders(self):
        rim = wheel()
        time = [NOW]
        catalog = EnrichedPublicCatalog([rim], released_ids=[rim['selection_id']], clock=lambda: time[0])
        self.assertEqual(catalog.page(group='family')['items'][0]['price_status'], 'current')
        time[0] += timedelta(minutes=15)
        current = catalog.page(group='family')['items'][0]
        self.assertIsNone(current['price'])
        self.assertFalse(current['order_enabled'])

    def test_variants_reprojects_when_warmup_completed_before_return(self):
        rim = wheel(quote_public=None)
        cache = {}
        catalog = catalogue([rim], quote_lookup=lambda pid: cache.get(pid))
        def warm(ids, **kwargs):
            cache[rim['selection_id']] = wheel()['quote_public']
            return False
        app = Flask('variants-cache-race')
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        app.register_blueprint(create_shop_public_blueprint(catalogue=catalog, catalogue_warmup=warm))
        result = app.test_client().get('/shop/api/v2/products/' + rim['selection_id'] + '/variants').get_json()
        self.assertFalse(result['prices_refreshing'])
        self.assertEqual(result['items'][0]['price_status'], 'current')

    def test_compatible_warmup_binds_requested_quantity_and_reprojects(self):
        rim, tire = wheel(), tyre()
        cache, calls = {}, []
        catalog = catalogue([rim, tire], compatible_pairs={rim['selection_id']: [pair(rim, tire)]}, quote_lookup=lambda pid: cache.get(pid))
        def warm(ids, *, quantities):
            calls.append(quantities)
            price = wheel()['quote_public']
            price['prices_by_quantity'] = {'3': {'quantity': 3, 'unit_gross_cent': 10000,
                                                 'items_gross_cent': 30000, 'vat_percent': '19.00'}}
            cache[tire['selection_id']] = price
            return False
        app = Flask('compatible-cache-race')
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        app.register_blueprint(create_shop_public_blueprint(catalogue=catalog, catalogue_warmup=warm))
        result = app.test_client().get('/shop/api/v2/products/' + rim['selection_id'] + '/compatible-tyres?quantity=3').get_json()
        self.assertEqual(calls, [(3,)])
        self.assertEqual(result['items'][0]['price_status'], 'current')

    def test_cached_parts_keep_observed_quantity_tiers_without_inventing_unqueried_prices(self):
        oil = part(price=None)
        cached = deepcopy(wheel()['quote_public'])
        cached['prices_by_quantity'] = {
            '1': {'quantity': 1, 'unit_gross_cent': 1000, 'items_gross_cent': 1000, 'vat_percent': '19.00'},
            '3': {'quantity': 3, 'unit_gross_cent': 800, 'items_gross_cent': 2400, 'vat_percent': '19.00'}}
        catalog = catalogue([oil], quote_lookup=lambda identifier: cached)
        price = catalog.get(oil['id'])['price']
        self.assertEqual(price['prices_by_quantity']['3']['items_gross_cent'], 2400)
        self.assertEqual(set(price['prices_by_quantity']), {'1', '3'})
        cached['prices_by_quantity'].pop('1')
        self.assertEqual(set(catalog.get(oil['id'])['price']['prices_by_quantity']), {'3'})


if __name__ == '__main__':
    unittest.main()
