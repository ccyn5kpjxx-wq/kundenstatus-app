"""Source-exact caches with synthetic providers, clocks and no network."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_price_cache import PublicPriceCache, PriceCacheError
from shop_price_hybrid import HybridPriceCache

BOUND, MARKET, PART = 'tyre:'+'a'*24, 'tyre:'+'b'*24, 'parts:'+'c'*24
PRODUCTS = {pid: {'id': pid, 'category': category, 'identity_hash': key*64}
            for pid, category, key in ((BOUND, 'tyre', 'a'), (MARKET, 'tyre', 'b'), (PART, 'oil', 'c'))}


class HybridChecks(unittest.TestCase):
    def setUp(self):
        forbidden = patch.object(socket, 'socket', side_effect=AssertionError('No network'))
        forbidden.start()
        self.addCleanup(forbidden.stop)
        self.wall, self.mono = datetime(2026, 11, 17, 12, tzinfo=timezone.utc), 0.0
        self.calls, self.failure = {'RSU': [], 'ALZURA': []}, set()
        self.caches = []
        self.addCleanup(lambda: [cache.stop() for cache in self.caches])

    def cache(self, source, ids, *, budget=20, enabled=True):
        def retail(product, quantity):
            self.calls[source].append((product['id'], quantity))
            if source in self.failure:
                raise ValueError('PRIVATE_PROVIDER_CANARY')
            return {'confirmed': True, 'source': source, 'procurement_confirmed': source == 'RSU',
                'product_id': product['id'], 'identity_hash': product['identity_hash'], 'mode': 'live',
                'currency': 'eur', 'vat_percent': '19.00', 'quantity': quantity,
                'unit_gross_cent': (10000 if source == 'RSU' else 20000) + quantity,
                'stock': 100, 'observed_at': self.wall.isoformat(), 'expires_at': (self.wall+timedelta(minutes=15)).isoformat()}
        provider = type('Provider', (), {'retail_quote': staticmethod(retail)})()
        cache = PublicPriceCache(PRODUCTS, approved_ids=set(ids), provider=provider, mode='live',
            enabled=enabled, reads_approved=enabled, source=source, require_procurement=source == 'RSU',
            max_provider_reads_per_period=budget, spacing_seconds=1,
            clock=lambda: self.wall, monotonic=lambda: self.mono)
        self.caches.append(cache)
        return cache

    def hybrid(self, **changes):
        options = {'released_ids': set(PRODUCTS), 'procurement_ids': {BOUND},
            'rsu_cache': self.cache('RSU', {BOUND}), 'market_cache': self.cache('ALZURA', {MARKET, PART})}
        options.update(changes)
        return HybridPriceCache(PRODUCTS, **options)

    def advance(self, seconds=2):
        self.wall += timedelta(seconds=seconds)
        self.mono += seconds

    def drain(self, hybrid, count=10):
        for _ in range(count):
            hybrid.run_once()
            self.advance()

    def test_construction_and_unstarted_reads_never_call_providers(self):
        with patch('threading.Thread', side_effect=AssertionError('No implicit thread')):
            hybrid = self.hybrid()
            self.assertIsNone(hybrid.get(BOUND))
            self.assertFalse(hybrid.warmup([BOUND, MARKET]))
            self.assertFalse(hybrid.status()['running'])
        self.assertEqual(self.calls, {'RSU': [], 'ALZURA': []})

    def test_both_sources_warm_without_shortcircuit_and_keep_every_quantity_tier(self):
        hybrid = self.hybrid()
        self.assertTrue(hybrid.start(background=False))
        self.assertTrue(hybrid.warmup([BOUND, MARKET], quantities=(2, 3)))
        self.assertEqual(hybrid.status()['queued'], 4)
        self.assertEqual(self.calls, {'RSU': [], 'ALZURA': []})
        self.drain(hybrid)
        self.assertEqual(self.calls['RSU'], [(BOUND, 2), (BOUND, 3)])
        self.assertEqual(self.calls['ALZURA'], [(MARKET, 2), (MARKET, 3)])
        for pid, source, base in ((BOUND, 'RSU', 10000), (MARKET, 'ALZURA', 20000)):
            value = hybrid.get(pid)
            self.assertEqual(value['source'], source)
            self.assertEqual(set(value['prices_by_quantity']), {'2', '3'})
            self.assertEqual(value['prices_by_quantity']['3']['items_gross_cent'], (base+3)*3)
        self.assertFalse(hybrid.warmup([BOUND, MARKET], quantities=(2, 3)))

    def test_rsu_failure_or_absence_never_falls_back_to_market(self):
        self.failure.add('RSU')
        hybrid = self.hybrid()
        hybrid.start(background=False)
        hybrid.warmup([BOUND, MARKET], quantities=(1,))
        self.drain(hybrid)
        self.assertIsNone(hybrid.get(BOUND))
        self.assertEqual(hybrid.get(MARKET)['source'], 'ALZURA')
        self.assertTrue(all(pid != BOUND for pid, _ in self.calls['ALZURA']))
        absent = self.hybrid(rsu_cache=None)
        absent.start(background=False)
        self.assertFalse(absent.warmup([BOUND], quantities=(1,)))
        self.assertIsNone(absent.get(BOUND))
        self.assertNotIn('PRIVATE_PROVIDER_CANARY', json.dumps(hybrid.status()))

    def test_source_budgets_are_independent_and_unreleased_never_gets_queried(self):
        hybrid = self.hybrid(rsu_cache=self.cache('RSU', {BOUND}, budget=1),
                             market_cache=self.cache('ALZURA', {MARKET, PART}, budget=2))
        hybrid.start(background=False)
        hybrid.warmup([BOUND, MARKET], quantities=(1, 4))
        self.drain(hybrid)
        self.assertEqual(len(self.calls['RSU']), 1)
        self.assertEqual(len(self.calls['ALZURA']), 2)
        self.assertEqual(hybrid.status()['sources']['RSU']['reserved_reads_in_period'], 1)
        self.assertFalse(hybrid.warmup(['tyre:'+'d'*24], quantities=(1,)))
        self.assertIsNone(hybrid.get('tyre:'+'d'*24))
        self.assertFalse(hybrid.run_once())

    def test_source_scope_and_malformed_quantity_fail_before_queuing(self):
        with self.assertRaises(PriceCacheError):
            self.hybrid(market_cache=self.cache('ALZURA', {BOUND, MARKET}))
        hybrid = self.hybrid()
        hybrid.start(background=False)
        for ids, quantities in (([BOUND], (5,)), ([PART], (True,)), ([BOUND, MARKET], (1, 1)),
                                ([BOUND]*25, (1,)), (['PRIVATE_CANARY'], (1,))):
            with self.subTest(quantities=quantities), self.assertRaises(PriceCacheError):
                hybrid.warmup(ids, quantities=quantities)
        self.assertEqual(hybrid.status()['queued'], 0)
        self.assertTrue(hybrid.warmup([PART], quantities=(20,)))
        self.drain(hybrid)
        self.assertEqual(self.calls['RSU'], [])
        self.assertEqual(self.calls['ALZURA'], [(PART, 20)])

    def test_expiry_and_returned_mutation_cannot_mix_or_resurrect_sources(self):
        hybrid = self.hybrid()
        hybrid.start(background=False)
        hybrid.warmup([BOUND, MARKET], quantities=(1,))
        self.drain(hybrid, 2)
        value = hybrid.get(BOUND)
        value['prices_by_quantity']['1']['unit_gross_cent'] = 1
        self.assertEqual(hybrid.get(BOUND)['prices_by_quantity']['1']['unit_gross_cent'], 10001)
        self.advance(901)
        self.assertIsNone(hybrid.get(BOUND))
        self.assertIsNone(hybrid.get(MARKET))
        hybrid.stop()
        self.assertFalse(hybrid.status()['running'])
        self.assertFalse(hybrid.warmup([BOUND, MARKET], quantities=(1,)))


if __name__ == '__main__':
    unittest.main()
