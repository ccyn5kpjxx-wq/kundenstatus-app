"""Synthetic browsing, procurement and quoting using detached observations."""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_catalog_public import PublicCatalog
from shop_price_book import PublishedPriceBook
from shop_tyre_pricing import cached_tyre_checkout_base, tyre_unit_gross_cent
from test_shop_cart_quotes import Fixture, NOW, RIM, TYRE, line
from test_shop_catalog_enriched import tyre
from test_shop_catalog_public import wheel


class TyrePricingTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('offline suite')))
        self.f = Fixture()
        self.t = tyre('a')
        self.r = wheel('b')
        self.raw = {**self.r['quote_public'], 'source':'RSU'}
        self.fixed = 6899
        self.clock = NOW
        for tier in self.raw['prices_by_quantity'].values():
            unit = self.fixed if tier['quantity'] == 1 else self.fixed - 50
            tier.update(unit_gross_cent=unit, items_gross_cent=unit * tier['quantity'])

    def catalogue(self, quote_lookup, **changes):
        return PublicCatalog([self.t, self.r], released_ids={TYRE, RIM}, clock=lambda:self.clock,
            quote_lookup=quote_lookup, quote_lookup_authoritative=True, tyre_payment_costs=True, **changes)

    def current(self, product, qty):
        return dict(self.f.retail(product, qty), unit_gross_cent=self.fixed - (50 if qty > 1 else 0))

    def check_catalogue_and_quote(self, catalogue, retail_quote, expected_bases):
        for quantity in (1, 2, 4):
            quote = self.f.quote([line(TYRE, quantity)], retail_quote=retail_quote, tyre_payment_costs=True)
            self.assertTrue(quote.snapshot()['ready'])
            result = quote.public()['lines'][0]
            self.assertEqual(quote.snapshot()['lines'][0]['base_unit_gross_cent'], expected_bases[quantity])
            self.assertNotIn('base_unit_gross_cent', result)
            offered = catalogue.get(TYRE)['price']['prices_by_quantity'][str(quantity)]
            self.assertEqual(result['unit_gross_cent'], tyre_unit_gross_cent(expected_bases[quantity]))
            self.assertEqual(result['items_gross_cent'], quantity * result['unit_gross_cent'])
            self.assertEqual(result['unit_gross_cent'], offered['unit_gross_cent'])
            self.assertEqual(result['items_gross_cent'], offered['items_gross_cent'])

    def test_pricebook_projection_never_updates_or_accumulates_in_stored_prices(self):
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        book = PublishedPriceBook(Path(tmp) / 'prices.db', products=self.f.products, approved_ids={TYRE},
                                  clock=lambda:self.clock, mode='test').provision(create=True)
        seed = {'source':'RSU', 'identity_hash':self.f.products[TYRE]['identity_hash'],
                'observed_at':self.clock.isoformat(), 'unit_gross_cent':self.fixed, 'stock_reported':9}
        book.seed({TYRE:seed}, evidence_id='a'*64)
        catalogue = self.catalogue(book.get, allow_published_prices=True)
        self.check_catalogue_and_quote(catalogue, lambda product,qty:book.checkout_quote(product, qty, self.current),
                                      dict.fromkeys((1, 2, 4), self.fixed))
        self.assertEqual(book.get(TYRE)['prices_by_quantity']['1']['unit_gross_cent'], self.fixed)
        self.clock += timedelta(days=4)
        self.assertEqual(catalogue.page(department='tyres')['items'][0]['price']['prices_by_quantity']['1']['unit_gross_cent'], 7040)
        self.assertEqual(book.due_ids(), [])

    def test_cache_preserves_quantity_tiers_and_only_offers_supported_quantities(self):
        original = deepcopy(self.raw)
        get = lambda _:self.raw
        catalogue = self.catalogue(get)
        self.check_catalogue_and_quote(catalogue,
            lambda product,qty:cached_tyre_checkout_base(product, qty, self.current, get),
            {1: self.fixed, 2: self.fixed - 50, 4: self.fixed - 50})
        self.assertEqual(self.raw, original)
        for key in ('1','4'):
            self.assertEqual(catalogue.get(RIM)['price']['prices_by_quantity'][key]['unit_gross_cent'],
                             self.raw['prices_by_quantity'][key]['unit_gross_cent'])
        bad = lambda product,qty:dict(self.current(product,qty),unit_gross_cent=self.fixed-49)
        self.assertIsNone(cached_tyre_checkout_base(self.f.products[TYRE],4,bad,get))
        del self.raw['prices_by_quantity']['1']
        offered = catalogue.get(TYRE)['price']['prices_by_quantity']
        self.assertNotIn('1', offered)
        self.assertIn('4', offered)
        self.assertIsNone(cached_tyre_checkout_base(self.f.products[TYRE],1,self.current,get))
        self.assertEqual(cached_tyre_checkout_base(self.f.products[TYRE],4,self.current,get)['unit_gross_cent'],
                         self.fixed - 50)

    def test_mixed_cart_total_changes_only_by_tyre_price_difference(self):
        cart = [line(TYRE, 4, group='set1', services={'mounting': True}), line(RIM, 4, group='set1')]
        old = self.f.quote(cart).public()
        new = self.f.quote(cart, tyre_payment_costs=True).public()
        self.assertTrue(new['ready'])
        tyre_line = next(item for item in old['lines'] if item['id'] == TYRE)
        extra = 4 * (tyre_unit_gross_cent(tyre_line['unit_gross_cent']) - tyre_line['unit_gross_cent'])
        self.assertEqual(new['total_gross_cent'], old['total_gross_cent'] + extra)
        self.assertEqual(new['shipments'], old['shipments'])
        self.assertEqual(next(item for item in new['lines'] if item['id'] == RIM),
                         next(item for item in old['lines'] if item['id'] == RIM))

    def test_round_up_preserves_original_net_revenue_for_reference_purchase(self):
        for base in (1, 39, 6899, 14999):
            unit = tyre_unit_gross_cent(base)
            before = Decimal(2 * base) / Decimal('1.19')
            def net_after_fees(price):
                return 2 * price / Decimal('1.19') - (2 * price * Decimal('.015') + 25)
            self.assertGreaterEqual(net_after_fees(unit), before)
            self.assertLess(net_after_fees(unit - 1), before)


if __name__ == '__main__':
    unittest.main()
