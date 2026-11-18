"""Compare the anonymous display path with the synthetic checkout, without I/O."""
from datetime import timedelta
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask
from shop_freight import MainlandDeliveryEligibility, RSURetailFreight
from shop_production import _service_quote
from shop_public_api import create_shop_public_blueprint
from test_shop_cart_quotes import RIM, TYRE, NOW, line
from test_shop_complete_wheels import fixture, make_quote
from test_shop_catalog_enriched import catalogue, pair, tyre, wheel


class SetPreviewChecks(unittest.TestCase):
    def setUp(self):
        network = patch.object(socket, 'socket', side_effect=AssertionError('No network in preview'))
        network.start()
        self.addCleanup(network.stop)
        self.rim, self.tyre = wheel('b'), tyre('a')
        self.fees = RSURetailFreight({RIM: 'pkw_rim', TYRE: 'pkw_tyre'}, mode='test', basis='net',
            evidence_id='synthetic', checked_at=NOW - timedelta(days=1),
            destination_eligibility=MainlandDeliveryEligibility(), clock=lambda: NOW)
        observed = NOW - timedelta(hours=1)
        self.book = {}
        for identifier, cents in ((RIM, 14865), (TYRE, 10858)):
            self.book[identifier] = {'kind': 'merchant_pricebook', 'source': 'RSU',
                'stock_reported': 4, 'observed_at': observed.isoformat(),
                'expires_at': (observed + timedelta(days=7)).isoformat(),
                'review_at': (observed + timedelta(days=5)).isoformat(),
                'stock_observed_at': observed.isoformat(),
                'stock_expires_at': (observed + timedelta(minutes=15)).isoformat(),
                'prices_by_quantity': {str(q): {'quantity': q, 'vat_percent': '19.00',
                    'unit_gross_cent': cents + 4 - q, 'items_gross_cent': (cents + 4 - q) * q}
                    for q in range(1, 5)}}
        self.url = '/shop/api/v2/products/' + RIM + '/complete-wheel-price'

    def client(self, **overrides):
        cat = catalogue([self.rim, self.tyre], **{
            'compatible_pairs': {RIM: [pair(self.rim, self.tyre)]},
            'procurement_ids': {RIM, TYRE}, 'rsu_only_prices': True,
            'quote_lookup_authoritative': True, 'allow_published_prices': True, 'quote_lookup': self.book.get,
            'wheel_pickup_freight': self.fees.workshop_collection_gross_cent, **overrides})
        app = Flask('wheel-preview')
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        def forbidden(*args, **kwargs):
            raise AssertionError('A preview cannot reach financial or refresh operations')
        app.register_blueprint(create_shop_public_blueprint(catalogue=cat, catalogue_warmup=forbidden,
            authorize_quote=forbidden, quote_sink=forbidden))
        return app.test_client()

    def preview(self, quantity, **kwargs):
        result = self.client(**kwargs).get(self.url, query_string={'quantity': quantity, 'tyre_id': TYRE})
        self.assertEqual(result.status_code, 200)
        return result.get_json()

    def test_selected_quantity_prices_montage_and_one_tariff_match_checkout(self):
        f = fixture()
        def retail(product, quantity):
            unit = self.book[product['id']]['prices_by_quantity'][str(quantity)]['unit_gross_cent']
            return {**f.retail(product, quantity), 'unit_gross_cent': unit}
        def checkout_tariff(code, product, quantity, options):
            return {**_service_quote(code, product, quantity, options), **f.current()}
        for q in (4, 3, 2, 1):
            with self.subTest(q=q):
                rows = [line(RIM, q, group='set-1'), line(TYRE, q, group='set-1', services={'mounting': True})]
                snapshot = make_quote(f, rows=rows, retail_quote=retail, freight_quote=self.fees,
                                      service_quote=checkout_tariff).public()
                self.assertTrue(snapshot['ready'], snapshot['blocked_reasons'])
                p = self.preview(q)
                self.assertEqual(p['total_gross_cent'], snapshot['total_gross_cent'])
                self.assertEqual(p['goods_gross_cent'], sum(l['items_gross_cent'] for l in snapshot['lines']))
                self.assertEqual(p['mounting_gross_cent'], q * 2500)
                self.assertEqual(p['freight_gross_cent'], 1642 if q == 1 else 0)
                self.assertEqual(p['currency'], 'EUR')
                self.assertEqual(set(p), {'rim_id','tyre_id','quantity','currency','goods_gross_cent',
                                          'mounting_gross_cent','freight_gross_cent','total_gross_cent'})
        self.assertEqual(self.preview(4)['total_gross_cent'], 112892)

    def test_options_charge_each_tyre_once_across_tariff_bands(self):
        for inch, unit in (('13',2250),('16',2250),('17',2500),('18',2500),('19',3000),('20',3000),('21',None)):
            with self.subTest(inch=inch):
                self.tyre['diameter'] = self.rim['diameter'] = inch
                result = self.client().get(self.url, query_string={'quantity': 3, 'tyre_id': TYRE,
                    'runflat': '1', 'disposal': '1'}).get_json()
                self.assertEqual(result['mounting_gross_cent'], (unit + 900) * 3 if unit is not None else None)
                if unit is None:
                    self.assertIsNone(result['total_gross_cent'])

    def test_unavailable_or_expired_freight_is_never_promoted_to_free(self):
        self.assertIsNone(self.preview(4, wheel_pickup_freight=None)['freight_gross_cent'])
        self.fees.checked_at = NOW - timedelta(days=30)
        result = self.preview(4)
        self.assertIsNone(result['freight_gross_cent'])
        self.assertIsNone(result['total_gross_cent'])
        self.assertEqual(result['mounting_gross_cent'], 10000)

    def test_mismatched_or_missing_diameters_do_not_quote_a_mounted_total(self):
        for diameter in ('17', ''):
            with self.subTest(diameter=diameter):
                self.rim['diameter'] = diameter
                price = self.preview(3)
                self.assertIsNone(price['mounting_gross_cent'])
                self.assertIsNone(price['total_gross_cent'])

    def test_missing_tier_insufficient_stock_future_stock_and_inquiry_do_not_get_a_total(self):
        missing_tier = self.book[RIM]['prices_by_quantity'].pop('3')
        self.assertIsNone(self.preview(3)['total_gross_cent'])
        self.book[RIM]['prices_by_quantity']['3'] = missing_tier
        self.book[TYRE]['stock_reported'] = 2
        self.assertIsNone(self.preview(3)['total_gross_cent'])
        self.book[TYRE]['stock_reported'] = 3
        self.assertIsNone(self.preview(3, procurement_ids={TYRE})['total_gross_cent'])
        self.book[TYRE]['stock_observed_at'] = (NOW + timedelta(minutes=1)).isoformat()
        self.book[TYRE]['stock_expires_at'] = (NOW + timedelta(minutes=10)).isoformat()
        self.assertIsNone(self.preview(3)['total_gross_cent'])

    def test_only_exact_documented_pairs_and_bounded_read_parameters(self):
        client = self.client()
        base = self.url + '?tyre_id=' + TYRE + '&quantity=3'
        for suffix in ('&quantity=2', '&unit_gross_cent=1', '&runflat=2', '&disposal=false', '&workshop_delivery=customer_reshipment'):
            self.assertEqual(client.get(base + suffix).status_code, 400)
        for q in ('', '01', '5', '0', '1.5'):
            self.assertEqual(client.get(self.url, query_string={'quantity': q, 'tyre_id': TYRE}).status_code, 400)
        self.assertEqual(client.post(base).status_code, 405)
        self.assertEqual(client.get(base.replace(TYRE, 'tyre:' + 'd'*24)).status_code, 404)
        self.assertEqual(self.client(compatible_pairs={}).get(base).status_code, 404)


if __name__ == '__main__':
    unittest.main()
