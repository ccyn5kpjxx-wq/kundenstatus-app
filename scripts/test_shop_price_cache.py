"""Bounded fake-provider cache; no HTTP, credentials or real supplier data."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import socket
import sys
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_price_cache import PriceCacheError, PublicPriceCache

NOW = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
IDS = ['tyre:' + f'{i:024x}' for i in range(1, 31)]
PRODUCTS = {pid: {'id': pid, 'identity_hash': f'{i:064x}', 'category': 'tyre', 'private_field': 'PRIVATE_CANARY'}
            for i, pid in enumerate(IDS, 1)}


class Time:
    def __init__(self):
        self.wall, self.mono = NOW, 0.0

    def advance(self, seconds):
        self.wall += timedelta(seconds=seconds)
        self.mono += seconds


class Provider:
    def __init__(self, clock):
        self.clock, self.calls, self.changes, self.fail = clock, [], {}, False

    def retail_quote(self, product, quantity):
        self.calls.append((product, quantity))
        if self.fail:
            raise ValueError('PRIVATE_CANARY_PROVIDER_PRICE_PASSWORD')
        return {'confirmed': True, 'procurement_confirmed': True, 'product_id': product['id'],
                'identity_hash': product['identity_hash'], 'mode': 'test', 'currency': 'eur',
                'vat_percent': '19.00', 'quantity': quantity, 'unit_gross_cent': 10000 if quantity == 1 else 9000,
                'stock': 20, 'observed_at': self.clock.wall.isoformat(),
                'expires_at': (self.clock.wall + timedelta(minutes=15)).isoformat(),
                'unit_net': 'PRIVATE_CANARY', 'source_binding': 'PRIVATE_CANARY', **self.changes}


class CacheTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.network = patch.object(socket, 'socket', side_effect=AssertionError('network forbidden'))
        cls.network.start()

    @classmethod
    def tearDownClass(cls):
        cls.network.stop()

    def setUp(self):
        self.time = Time()
        self.provider = Provider(self.time)
        self.caches = []

    def tearDown(self):
        for cache in self.caches:
            cache.stop()

    def cache(self, *, products=PRODUCTS, **changes):
        settings = dict(approved_ids=set(IDS), provider=self.provider, mode='test', enabled=True,
            reads_approved=True, max_provider_reads_per_period=120, period_seconds=60,
            spacing_seconds=2, failure_backoff_seconds=30,
            clock=lambda: self.time.wall, monotonic=lambda: self.time.mono)
        settings.update(changes)
        value = PublicPriceCache(products, **settings)
        self.caches.append(value)
        return value

    def test_import_construction_and_unarmed_start_never_spawn_or_read(self):
        with patch('threading.Thread', side_effect=AssertionError('thread forbidden')):
            for change in ({'enabled': False}, {'reads_approved': False}, {'max_provider_reads_per_period': 0}):
                cache = self.cache(**change)
                self.assertFalse(cache.start())
                self.assertEqual(cache.enqueue([IDS[0]]), 0)
                self.assertFalse(cache.run_once())
                self.assertIsNone(cache.get(IDS[0]))
        self.assertEqual(self.provider.calls, [])

    def test_gross_projection_is_cache_only_detached_and_exact_quantities(self):
        cache = self.cache()
        self.assertTrue(cache.start(background=False))
        self.assertEqual(cache.enqueue([IDS[0]]), 2)
        self.assertTrue(cache.run_once())
        self.time.advance(2)
        self.assertTrue(cache.run_once())
        quote = cache.get(IDS[0])
        self.assertEqual(quote['source'], 'RSU')
        self.assertEqual(set(quote['prices_by_quantity']), {'1', '4'})
        self.assertEqual(quote['prices_by_quantity']['4']['items_gross_cent'], 36000)
        self.assertEqual(quote['observed_at'], NOW.isoformat())
        self.assertEqual(quote['expires_at'], (NOW + timedelta(minutes=15)).isoformat())
        encoded = json.dumps(quote)
        for private in ('PRIVATE_CANARY', 'unit_net', 'source_binding', 'identity_hash', 'policy'):
            self.assertNotIn(private, encoded)
        quote['prices_by_quantity']['1']['unit_gross_cent'] = 1
        self.assertEqual(cache.get(IDS[0])['prices_by_quantity']['1']['unit_gross_cent'], 10000)
        self.assertEqual(len(self.provider.calls), 2)
        self.assertNotIn('private_field', self.provider.calls[0][0])
        self.assertEqual(cache.enqueue([IDS[0]]), 0)

    def test_page_allowlist_queue_bound_and_singleflight(self):
        cache = self.cache(max_queue=2)
        cache.start(background=False)
        self.assertEqual(cache.enqueue(['tyre:' + 'f' * 24]), 0)
        self.assertEqual(cache.enqueue([IDS[0], IDS[0], IDS[1]]), 2)
        self.assertEqual(cache.enqueue([IDS[0], IDS[1]]), 0)
        self.assertEqual(cache.status()['queued'], 2)
        for values in (IDS[:25], [None]):
            with self.subTest(values=values), self.assertRaises(PriceCacheError):
                cache.enqueue(values)
        for quantities in ((5,), (21,), (1, 1), (True,), [1]):
            with self.subTest(quantities=quantities), self.assertRaises(PriceCacheError):
                cache.enqueue([IDS[0]], quantities=quantities)
        self.assertEqual(self.provider.calls, [])

    def test_global_spacing_and_rolling_budget_limit_even_many_anonymous_pages(self):
        cache = self.cache(max_provider_reads_per_period=2)
        cache.start(background=False)
        # Only currently affordable work is queued; the next period is not
        # promised to an abandoned anonymous page request.
        self.assertEqual(cache.enqueue(IDS[:24]), 2)
        self.assertTrue(cache.run_once())
        self.assertFalse(cache.run_once())
        self.time.advance(2)
        self.assertTrue(cache.run_once())
        self.time.advance(2)
        self.assertFalse(cache.run_once())
        self.assertEqual(len(self.provider.calls), 2)
        self.assertIs(cache.warmup([IDS[1]]), False)
        self.assertEqual(cache.status()['queued'], 0)
        self.time.advance(56)
        self.assertIs(cache.warmup([IDS[1]]), True)
        self.assertTrue(cache.run_once())
        self.assertEqual(len(self.provider.calls), 3)

    def test_exact_wheel_quantities_two_three_and_parts_one_to_twenty(self):
        cache = self.cache()
        cache.start(background=False)
        self.assertIs(cache.warmup([IDS[0]], quantities=(2, 3)), True)
        self.assertEqual(self.provider.calls, [])
        cache.run_once()
        self.time.advance(2)
        cache.run_once()
        self.assertEqual([q for _, q in self.provider.calls], [2, 3])
        self.assertEqual(set(cache.get(IDS[0])['prices_by_quantity']), {'2', '3'})
        self.assertIs(cache.warmup([IDS[0]], quantities=(2, 3)), False)
        pid = 'parts:' + 'a' * 24
        products = {pid: {'id': pid, 'identity_hash': 'f' * 64, 'category': 'oil'}}
        parts = self.cache(products=products, approved_ids={pid})
        parts.start(background=False)
        self.assertEqual(parts.enqueue([pid], quantities=(1, 20)), 2)
        parts.run_once()
        self.time.advance(2)
        parts.run_once()
        self.assertEqual(set(parts.get(pid)['prices_by_quantity']), {'1', '20'})
        self.assertEqual([q for _, q in self.provider.calls][-2:], [1, 20])

    def test_warmup_is_literal_pending_bool_not_newly_queued_count(self):
        cache = self.cache()
        self.assertIs(cache.warmup([IDS[0]], quantities=(2,)), False)
        cache.start(background=False)
        self.assertIs(cache.warmup([IDS[0]], quantities=(2,)), True)
        self.assertEqual(cache.enqueue([IDS[0]], quantities=(2,)), 0)
        self.assertIs(cache.warmup([IDS[0]], quantities=(2,)), True)
        self.assertEqual(self.provider.calls, [])
        cache.run_once()
        self.assertIs(cache.warmup([IDS[0]], quantities=(2,)), False)
        self.assertIs(cache.warmup(['tyre:' + 'f' * 24]), False)
        self.assertEqual(len(self.provider.calls), 1)

    def test_abandoned_queued_jobs_expire_without_spending_later_budget(self):
        cache = self.cache()
        cache.start(background=False)
        self.assertIs(cache.warmup([IDS[0]], quantities=(3,)), True)
        self.time.advance(31)
        self.assertFalse(cache.run_once())
        self.assertEqual(cache.status()['queued'], 0)
        self.assertEqual(self.provider.calls, [])
        self.assertIs(cache.warmup([IDS[0]], quantities=(3,)), True)
        self.assertTrue(cache.run_once())
        self.assertEqual(len(self.provider.calls), 1)

    def test_failure_backoff_and_error_privacy(self):
        cache = self.cache()
        cache.start(background=False)
        self.provider.fail = True
        cache.enqueue([IDS[0]], quantities=(1,))
        cache.run_once()
        self.assertIsNone(cache.get(IDS[0]))
        self.time.advance(2)
        self.assertEqual(cache.enqueue([IDS[0]], quantities=(1,)), 0)
        self.assertNotIn('PRIVATE_CANARY', repr(cache._backoffs) + repr(cache))
        self.time.advance(28)
        self.assertEqual(cache.enqueue([IDS[0]], quantities=(1,)), 1)
        cache.run_once()
        self.assertEqual(len(self.provider.calls), 2)

    def test_failure_backoff_starts_after_slow_failed_read(self):
        def slow_failure(product, quantity):
            self.time.advance(40)
            raise RuntimeError('PRIVATE_CANARY')
        self.provider.retail_quote = slow_failure
        cache = self.cache()
        cache.start(background=False)
        cache.enqueue([IDS[0]], quantities=(1,))
        cache.run_once()
        self.time.advance(4)
        self.assertEqual(cache.enqueue([IDS[0]], quantities=(1,)), 0)
        self.time.advance(26)
        self.assertEqual(cache.enqueue([IDS[0]], quantities=(1,)), 1)

    def test_stock_changes_use_conservative_envelope_and_never_invent_tiers(self):
        cache = self.cache()
        cache.start(background=False)
        self.provider.changes = {'stock': 2}
        cache.enqueue([IDS[0]], quantities=(1,))
        cache.run_once()
        self.time.advance(2)
        self.provider.changes = {'stock': 20}
        cache.enqueue([IDS[0]], quantities=(4,))
        cache.run_once()
        value = cache.get(IDS[0])
        self.assertEqual(value['stock_reported'], 2)
        self.assertEqual(set(value['prices_by_quantity']), {'1'})
        separate = self.cache()
        separate.start(background=False)
        separate.enqueue([IDS[0]], quantities=(4,))
        separate.run_once()
        self.assertEqual(set(separate.get(IDS[0])['prices_by_quantity']), {'4'})

    def test_stale_future_wrong_identity_and_mode_quotes_never_enter_cache(self):
        for changes in ({'product_id': IDS[1]}, {'identity_hash': 'f' * 64}, {'quantity': 4},
                        {'mode': 'live'}, {'currency': 'usd'}, {'stock': True}, {'stock': 0},
                        {'unit_gross_cent': True}, {'observed_at': (NOW + timedelta(seconds=1)).isoformat()},
                        {'expires_at': NOW.isoformat()}, {'expires_at': (NOW + timedelta(minutes=16)).isoformat()}):
            with self.subTest(changes=changes):
                cache = self.cache()
                cache.start(background=False)
                self.provider.changes = changes
                cache.enqueue([IDS[0]], quantities=(1,))
                self.assertTrue(cache.run_once())
                self.assertIsNone(cache.get(IDS[0]))

    def test_expiry_not_extended_and_clock_rollback_fails_closed(self):
        cache = self.cache()
        cache.start(background=False)
        cache.enqueue([IDS[0]], quantities=(1,))
        cache.run_once()
        self.time.advance(900)
        self.assertIsNone(cache.get(IDS[0]))
        self.assertEqual(cache.enqueue([IDS[0]], quantities=(1,)), 1)
        cache.run_once()
        self.assertEqual(cache.get(IDS[0])['observed_at'], self.time.wall.isoformat())
        self.time.wall -= timedelta(seconds=1)
        self.assertIsNone(cache.get(IDS[0]))
        self.assertEqual(cache.enqueue([IDS[1]]), 0)
        self.assertFalse(cache.run_once())

    def test_inflight_singleflight_and_worker_cap_with_controlled_provider(self):
        started, release = threading.Event(), threading.Event()
        original = self.provider.retail_quote
        def blocked(product, quantity):
            started.set()
            self.assertTrue(release.wait(timeout=2))
            return original(product, quantity)
        self.provider.retail_quote = blocked
        cache = self.cache(max_workers=1, max_provider_reads_per_period=1)
        cache.start(background=False)
        self.assertIs(cache.warmup([IDS[0]], quantities=(3,)), True)
        worker = threading.Thread(target=cache.run_once)
        worker.start()
        try:
            self.assertTrue(started.wait(timeout=2))
            self.assertEqual(cache.enqueue([IDS[0]], quantities=(3,)), 0)
            # The single read budget is reserved by the active job. Its own
            # polling hint stays true; unrelated work cannot promise a refresh.
            self.assertIs(cache.warmup([IDS[0]], quantities=(3,)), True)
            self.assertIs(cache.warmup([IDS[1]], quantities=(1,)), False)
            self.assertEqual(cache.enqueue([IDS[1]], quantities=(1,)), 0)
            self.time.advance(2)
            self.assertFalse(cache.run_once())
            self.assertEqual(cache.status()['inflight'], 1)
        finally:
            release.set()
            worker.join(timeout=2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(self.provider.calls), 1)
        self.assertIs(cache.warmup([IDS[0]], quantities=(3,)), False)

    def test_explicit_background_start_and_stop_are_controlled(self):
        cache = self.cache(max_workers=1)
        self.assertTrue(cache.start())
        self.assertEqual(len(cache._threads), 1)
        self.assertEqual(cache.enqueue([IDS[0]], quantities=(1,)), 1)
        with cache._condition:
            self.assertTrue(cache._condition.wait_for(lambda: cache.get(IDS[0]) is not None, timeout=2))
        cache.stop()
        self.assertFalse(cache.status()['running'])
        self.assertEqual(cache.enqueue([IDS[1]]), 0)
        self.assertEqual(len(self.provider.calls), 1)


if __name__ == '__main__':
    unittest.main()
