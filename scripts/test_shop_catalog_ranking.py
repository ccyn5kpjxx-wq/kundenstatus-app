"""Offline offer ordering across full filters, families and pagination."""
from collections import Counter
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask
from shop_catalog_public import CatalogError, PublicCatalog, variant_fingerprint
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_tyre_tests import ADAC_TYRE_TESTS, TyreTest
from shop_public_api import create_shop_public_blueprint
from test_shop_catalog_public import NOW, wheel, part
from test_shop_catalog_enriched import tyre, pair


LABEL = '/shop/documents/' + '1' * 64 + '.jpg'
SHEET = '/shop/documents/' + '2' * 64 + '.pdf'


def identifier(row):
    return row.get('selection_id', row.get('id'))


def quote(stock=20):
    result = deepcopy(wheel()['quote_public'])
    result.update(source='RSU', stock_reported=stock)
    return result


def documented(row):
    return {'identity_hash': variant_fingerprint(row), 'eprel_id': '123456',
            'tyre_label_url': LABEL, 'tyre_sheet_url': SHEET}


def rim_proof(row):
    return {'confirmed': True, 'product_id': identifier(row),
            'identity_hash': variant_fingerprint(row), 'supplier': 'RSU',
            'evidence_id': 'synthetic-reviewed-dimensions',
            'geometry': {'width': '8', 'diameter': '18', 'offset': '45',
                         'bolt_count': '5', 'pcd': '112', 'centre_bore': '66.6'}}


def catalog(rows, **kwargs):
    return EnrichedPublicCatalog(rows, released_ids=[identifier(r) for r in rows],
        document_urls={'label': LABEL, 'sheet': SHEET}, clock=lambda: NOW, **kwargs)


class OfferRankingChecks(unittest.TestCase):
    def test_stock_alternatives_require_exact_dimensions_evidence_price_and_requested_stock(self):
        rows = [wheel(letter) for letter in 'abcdef']
        proofs = {identifier(row): rim_proof(row) for row in rows}
        proofs[identifier(rows[2])]['geometry']['offset'] = '46'
        del proofs[identifier(rows[3])]
        for index, row in enumerate(rows):
            observed = NOW - timedelta(days=1)
            seen = NOW - (timedelta(minutes=1) if index == 5 else timedelta(hours=1))
            q = quote(3 if index in (0, 4) else 6)
            q.update(kind='merchant_pricebook', observed_at=observed.isoformat(),
                     expires_at=(observed + timedelta(days=7)).isoformat(), review_at=(observed + timedelta(days=1)).isoformat(),
                     stock_observed_at=seen.isoformat(), stock_expires_at=(seen + timedelta(minutes=15)).isoformat())
            row['quote_public'] = q
        approved = {identifier(r) for r in rows}
        store = catalog(rows, procurement_ids=approved, shipping_ready_ids=approved, standalone_rims=proofs, allow_published_prices=True)
        app = Flask('stock-options')
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        calls = []
        app.register_blueprint(create_shop_public_blueprint(catalogue=store, catalogue_warmup=lambda *a, **k: calls.append((a,k))))
        client = app.test_client()
        path = '/shop/api/v2/products/' + identifier(rows[0])
        result = client.get(path + '/stock-alternatives?quantity=4')
        self.assertEqual(result.status_code, 200)
        self.assertEqual([p['id'] for p in result.json['items']], [identifier(rows[5]), identifier(rows[1])])
        self.assertFalse(result.json['items'][1]['price']['stock_current'])
        self.assertEqual(calls, [], 'alternatives must use only local stock observations')
        self.assertEqual(client.get(path + '/stock-alternatives?quantity=4&quantity=3').status_code, 400)
        self.assertEqual(client.get(path + '?quantity=4&recheck_stock=1').status_code, 200)
        self.assertEqual(sum(kw.get('recheck_stock', False) for _, kw in calls), 1)
        self.assertEqual(client.get(path + '?quantity=5&recheck_stock=1').status_code, 400)

    def test_tyre_stock_alternatives_preserve_season_dimensions_and_service_description(self):
        src = tyre()
        original = catalog([src]).get(identifier(src))
        key = EnrichedPublicCatalog._stock_peer_key(original)
        self.assertIsNotNone(key)
        for mutation in ({'tyre_season':'winter'}, {'size': '205/55 R18 91V XL'},
                         {'geometry': {**original['geometry'], 'load_index':'88'}},
                         {'geometry': {**original['geometry'], 'runflat':'1'}},
                         {'geometry': {**original['geometry'], 'width':'225'}}):
            self.assertNotEqual(key, EnrichedPublicCatalog._stock_peer_key({**original, **mutation}))

    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()

    def tearDown(self):
        self.network.stop()

    def test_exact_test_rating_distinguishes_zero_from_unknown_across_pages(self):
        rows = [tyre(letter) for letter in ('a', 'b', 'c')]
        records = {}
        for index, row in enumerate(rows):
            ean = f'{index:013d}'
            row.update(identifiers={'ean': ean}, quality_facts={
                'fuel_efficiency': 'C', 'wet_grip': 'B', 'noise_db': '71'})
            if index:
                records[ean] = TyreTest(variant_fingerprint(row), ('C', 'B', '71'), 2025,
                    5.5 if index == 1 else 2.4, 'sommerreifen/205-55-r18/test-id-1/')
        with patch.dict(ADAC_TYRE_TESTS, records, clear=True):
            c = catalog(rows)
            results = [c.page(department='tyres', sort='rating', page_size=1, page=page)['items'][0]
                       for page in (1, 2, 3)]
            self.assertEqual([item['id'] for item in results], [identifier(r) for r in reversed(rows)])
            self.assertEqual(results[0]['test_rating']['stars'], 4)
            self.assertEqual(results[1]['test_rating']['stars'], 0)
            self.assertNotIn('test_rating', results[2])
            for changed in ({'speed_index': 'W'}, {'group': 'Winterreifen'}, {'approval': 'OEM'},
                            {'quality_facts': {'wet_grip': 'B'}}, {'identifiers': {'ean': 'other'}}):
                with self.subTest(changed=changed):
                    edited = {**rows[1], **changed}
                    self.assertNotIn('test_rating', catalog([edited]).get(identifier(edited)))

    def test_wet_grip_sort_is_independent_of_stars_and_puts_unknown_last(self):
        rows = [tyre(letter) for letter in ('a', 'b', 'c')]
        rows[0]['quality_facts'] = {'wet_grip': 'unknown'}
        rows[1]['quality_facts'] = {'wet_grip': 'D'}
        rows[2]['quality_facts'] = {'wet_grip': 'A'}
        c = catalog(rows)
        results = c.page(category='tyre', sort='wet_grip')['items']
        self.assertEqual([r['id'] for r in results], [identifier(r) for r in reversed(rows)])
        self.assertTrue(all('test_rating' not in row for row in results))
        with self.assertRaises(CatalogError):
            c.page(department='wheels', sort='rating')

    def test_base_local_price_and_stock_rank_before_every_page_without_hiding_rows(self):
        missing = part('a', name='AAA query missing', price=None)
        zero = part('b', name='BBB query zero')
        zero['price']['stock'] = 0
        ready = part('c', name='ZZZ query offer')
        rows = [missing, zero, ready]
        c = PublicCatalog(rows, released_ids=[identifier(r) for r in rows], clock=lambda: NOW)
        combined = []
        for page in range(1, 4):
            result = c.page(department='accessories', q='query', brand='SYNTHETIC', page=page, page_size=1)
            self.assertEqual(result['total'], 3)
            self.assertEqual(result['has_more'], page < 3)
            combined.extend(p['id'] for p in result['items'])
            self.assertFalse(result['items'][0]['order_enabled'])
        self.assertEqual(combined, [identifier(ready), identifier(zero), identifier(missing)])

    def test_static_procurement_obligations_precede_zero_expired_and_unbound(self):
        good = tyre('1'); good.update(name='ZZZ complete offer', quote_public=quote())
        missing_docs = tyre('2'); missing_docs.update(name='AAA missing documents', quote_public=quote())
        missing_shipping = tyre('3'); missing_shipping.update(name='BBB missing shipping', quote_public=quote())
        zero = tyre('4'); zero.update(name='AAA reported zero', quote_public=quote(0))
        expired = tyre('5'); expired.update(name='000 expired preferred', quote_public=quote())
        expired['quote_public']['expires_at'] = NOW.isoformat()
        market = tyre('6'); market.update(name='000 market first alphabetically', quote_public=quote())
        rows = [market, expired, zero, missing_docs, missing_shipping, good]
        bound = {identifier(r) for r in rows if r is not market}
        docs = {identifier(r): documented(r) for r in rows if r is not missing_docs}
        c = catalog(rows, procurement_ids=bound, product_documents=docs,
            shipping_ready_ids=bound - {identifier(missing_shipping)}, preferred_ids={identifier(expired)})
        result = c.page(page_size=48)
        self.assertEqual(result['total'], len(rows))
        self.assertEqual([p['id'] for p in result['items']], [identifier(r) for r in
            [good, missing_docs, missing_shipping, zero, expired, market]])
        self.assertIsNotNone(c.get(identifier(market))['price'])
        self.assertEqual(c.get(identifier(market))['purchase_status'], 'inquiry_only')
        self.assertTrue(all(p['order_enabled'] is False for p in result['items']))

    def test_vehicle_specific_accessory_is_not_ranked_as_universal(self):
        vehicle = part('a', category='accessory', department='parts', name='AAA vehicle sensor')
        universal = part('b', category='accessory', name='ZZZ general item')
        ids = {identifier(vehicle), identifier(universal)}
        c = catalog([vehicle, universal], procurement_ids=ids, shipping_ready_ids=ids)
        self.assertEqual([p['id'] for p in c.page()['items']], [identifier(universal), identifier(vehicle)])

    def test_rim_without_exact_sale_geometry_follows_valid_dimension_offer(self):
        unknown = wheel('1', name='AAA geometry missing', quote_public=quote())
        exact = wheel('2', name='ZZZ exact geometry', quote_public=quote())
        ids = {identifier(unknown), identifier(exact)}
        c = catalog([unknown, exact], procurement_ids=ids, shipping_ready_ids=ids,
                    standalone_rims={identifier(exact): rim_proof(exact)})
        first = c.page(department='wheels', page_size=1)
        self.assertEqual(first['items'][0]['id'], identifier(exact))
        self.assertEqual(first['total'], 2)
        self.assertNotIn('sale_dimensions', c.get(identifier(unknown)))

    def test_stale_positive_stock_does_not_erase_valid_fixed_merchant_price(self):
        fixed = tyre('1'); fixed.update(name='ZZZ published offer', quote_public=quote())
        published = fixed['quote_public']
        observed = NOW - timedelta(days=1)
        published.update(kind='merchant_pricebook', observed_at=observed.isoformat(),
            expires_at=(observed + timedelta(days=7)).isoformat(), review_at=(observed + timedelta(days=5)).isoformat(),
            stock_observed_at=(NOW - timedelta(hours=1)).isoformat(),
            stock_expires_at=(NOW - timedelta(minutes=45)).isoformat())
        zero = tyre('2'); zero.update(name='AAA current zero', quote_public=quote(0))
        ids = {identifier(fixed), identifier(zero)}
        c = catalog([zero, fixed], procurement_ids=ids, shipping_ready_ids=ids,
            product_documents={identifier(r): documented(r) for r in [zero, fixed]}, allow_published_prices=True)
        result = c.page()
        self.assertEqual(result['items'][0]['id'], identifier(fixed))
        self.assertEqual(result['items'][0]['price_status'], 'current')
        self.assertFalse(result['items'][0]['price']['stock_current'])
        self.assertEqual(result['items'][1]['price']['stock_reported'], 0)

    def test_cache_state_overrides_old_seed_membership_and_preserves_ties(self):
        a = part('a', name='AAA expired old seed', price=None)
        b = part('b', name='ZZZ current offer', price=None)
        values = {identifier(a): quote(), identifier(b): quote()}
        values[identifier(a)]['expires_at'] = NOW.isoformat()
        calls = Counter()
        def cached(pid):
            calls[pid] += 1
            return values.get(pid)
        ids = {identifier(a), identifier(b)}
        c = catalog([a, b], procurement_ids=ids, shipping_ready_ids=ids,
            preferred_ids={identifier(a)}, quote_lookup=cached, quote_lookup_authoritative=True)
        self.assertEqual(c.page(page_size=1)['items'][0]['id'], identifier(b))
        self.assertEqual(calls, Counter({identifier(a): 1, identifier(b): 1}))
        values[identifier(a)] = quote()
        values[identifier(b)] = quote(0)
        self.assertEqual(c.page(page_size=1)['items'][0]['id'], identifier(a))
        calls.clear()
        first = [p['id'] for p in c.page()['items']]
        self.assertEqual(first, [p['id'] for p in c.page()['items']])
        self.assertEqual(calls, Counter({identifier(a): 2, identifier(b): 2}))

    def test_family_representative_and_variant_order_are_ranked_before_pagination(self):
        unavailable = wheel('1', name='AAA unavailable design', quote_public=None)
        displayed = wheel('2', name='ZZZ sourced design', quote_public=None)
        sourced = wheel('3', name='ZZZ sourced design', quote_public=quote())
        rows = [unavailable, displayed, sourced]
        sid = identifier(sourced)
        c = catalog(rows, procurement_ids={sid}, shipping_ready_ids={sid}, standalone_rims={sid: rim_proof(sourced)})
        first = c.page(group='family', page=1, page_size=1)
        second = c.page(group='family', page=2, page_size=1)
        self.assertEqual(first['total'], 2)
        self.assertEqual(first['items'][0]['id'], sid)
        self.assertEqual(first['items'][0]['family']['variant_count'], 2)
        self.assertEqual(second['items'][0]['id'], identifier(unavailable))
        self.assertEqual(c.variants(identifier(displayed), page=1, page_size=1)['items'][0]['id'], sid)
        self.assertEqual(c.variants(identifier(displayed))['total'], 2)
        self.assertEqual(c.page(group='family', diameter='20')['total'], 0)

    def test_grouped_filter_reads_each_local_cache_entry_once_instead_of_scanning_pages(self):
        rows = [wheel(name='Design ' + str(n % 4)) for n in range(121)]
        for n, row in enumerate(rows):
            row['selection_id'] = 'alloy_wheel:' + format(n, '024x')
        ids = {identifier(r) for r in rows}
        calls = Counter()
        def cached(pid):
            calls[pid] += 1
            return quote()
        c = catalog(rows, procurement_ids=ids, quote_lookup=cached, quote_lookup_authoritative=True)
        result = c.page(group='family', page_size=1)
        self.assertEqual(result['total'], 4)
        self.assertEqual(len(result['items']), 1)
        self.assertEqual(calls, Counter({pid: 1 for pid in ids}))

    def test_recommendations_keep_real_priced_candidates_without_unbound_padding(self):
        rim = wheel()
        inquiry = tyre('1'); inquiry.update(name='AAA inquiry', quote_public=quote())
        available = tyre('2'); available.update(name='ZZZ available', quote_public=quote())
        tid = identifier(available)
        c = catalog([rim, inquiry, available], procurement_ids={tid}, shipping_ready_ids={tid},
            product_documents={tid: documented(available)}, compatible_pairs={identifier(rim): [pair(rim, inquiry), pair(rim, available)]})
        result = c.compatible_tyres(identifier(rim), season='summer')
        self.assertEqual([p['id'] for p in result['items']], [tid])
        self.assertEqual(result['scope'], 'rim_width_candidate')
        self.assertEqual(c.get(identifier(inquiry))['purchase_status'], 'inquiry_only')
        self.assertEqual(c.page(department='tyres')['total'], 2)

    def test_shipping_rank_input_rejects_unreleased_or_unbound_identity(self):
        row = part()
        for bad in ('string', True, {identifier(row)}, {'parts:' + '0' * 24}):
            with self.subTest(value=bad), self.assertRaises(CatalogError):
                catalog([row], procurement_ids=set(), shipping_ready_ids=bad)

    def test_http_lists_and_family_pages_share_ranking_and_counts(self):
        a = wheel('1', name='AAA inquiry', quote_public=None)
        b = wheel('2', name='ZZZ actual offer', quote_public=quote())
        sid = identifier(b)
        c = catalog([a, b], procurement_ids={sid}, shipping_ready_ids={sid}, standalone_rims={sid: rim_proof(b)})
        app = Flask(__name__)
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        app.register_blueprint(create_shop_public_blueprint(catalogue=c))
        with app.test_client() as client:
            for suffix in ('', '&group=family'):
                response = client.get('/shop/api/v2/products?department=wheels&page_size=1' + suffix)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json['items'][0]['id'], sid)
                self.assertEqual(response.json['total'], 2)


if __name__ == '__main__':
    unittest.main()
