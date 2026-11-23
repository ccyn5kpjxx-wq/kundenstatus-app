"""Bounded daily refresh scheduling, synthetic books/providers only."""
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_pricebook_runtime import PriceBookRuntime


class Book:
    def __init__(self, identifiers, *, success=True):
        self.due = set(identifiers)
        self.published = set(identifiers)
        self.success = success
        self.publications = []

    def due_ids(self):
        return sorted(self.due)

    def review_due(self, identifier):
        return identifier in self.published and identifier in self.due

    def get(self, identifier):
        return {'id': identifier} if identifier not in self.due else None

    def publish_uniform(self, product, response):
        self.publications.append((product, response))
        if self.success:
            self.due.remove(product['id'])
        return self.success


class PriceBookRuntimeChecks(unittest.TestCase):
    def test_manual_old_shortage_uses_one_shared_slot_without_consuming_checkout_reserve(self):
        runtime, book, provider, mono, products = self.fixture()
        pid = next(iter(products))
        runtime.start(background=False)
        public = {'stock_current': False, 'stock_reported': 3}
        book.get = Mock(return_value=public)
        runtime.warmup([pid], quantities=(3,), recheck_stock=True)
        self.assertEqual(provider.retail_quote.call_count, 0)
        runtime.warmup([pid], quantities=(4,), recheck_stock=True)
        provider.retail_quote.assert_called_once_with(products[pid], 1)
        runtime.warmup([pid], quantities=(4,), recheck_stock=True)
        self.assertEqual(provider.retail_quote.call_count, 1)
        mono[0] += 60
        public['stock_current'] = True
        runtime.warmup([pid], quantities=(4,), recheck_stock=True)
        public['stock_current'] = False
        runtime._attempts.extend([mono[0]] * (runtime._budget - runtime._checkout_reserve))
        runtime.warmup([pid], quantities=(4,), recheck_stock=True)
        self.assertEqual(provider.retail_quote.call_count, 1)

    def fixture(self, count=3, *, success=True, budget=300):
        products = {'tyre:' + f'{i:024x}': {'id': 'tyre:' + f'{i:024x}', 'identity_hash': 'a' * 64,
                                          'category': 'tyre'} for i in range(count)}
        book = Book(products, success=success)
        provider = Mock()
        provider.retail_quote.return_value = {'synthetic': True}
        mono = [0.]
        runtime = PriceBookRuntime(book, products, approved_ids=frozenset(products), provider=provider,
                                   max_reads_per_hour=budget, monotonic=lambda: mono[0])
        return runtime, book, provider, mono, products

    def test_daily_file_is_polled_by_worker_once_a_minute_and_failure_allows_rsu_fallback(self):
        _, book, provider, mono, products = self.fixture()
        importer = Mock(side_effect=[ValueError('synthetic private error'), None])
        runtime = PriceBookRuntime(book, products, approved_ids=set(products), provider=provider,
                                   daily_import=importer, monotonic=lambda: mono[0])
        runtime.start(background=False)
        self.assertEqual(runtime.status()['price_refresh_days'], 1)
        self.assertTrue(runtime.refresh_for_checkout(next(iter(products))))
        importer.assert_not_called()
        with self.assertLogs('shop_pricebook_runtime', level='WARNING') as messages:
            runtime._import_if_due()
        self.assertNotIn('synthetic private error', ''.join(messages.output))
        mono[0] += 3
        self.assertTrue(runtime.run_once())
        runtime._import_if_due()
        self.assertEqual(importer.call_count, 1)
        mono[0] += 57
        runtime._import_if_due()
        self.assertEqual(importer.call_count, 2)

    def test_active_catalogue_size_can_be_reviewed_in_one_day_with_checkout_reserve(self):
        runtime, book, provider, mono, products = self.fixture(count=4098)
        runtime.start(background=False)
        # 288 background slots per hour leave twelve of the hourly 300 for checkout.
        for _ in range(len(products)):
            self.assertTrue(runtime.run_once())
            mono[0] += 12.5
        self.assertLess(mono[0], 86400)
        self.assertFalse(book.due)
        self.assertEqual(provider.retail_quote.call_count, len(products))

    def test_construction_and_page_hints_never_call_supplier_and_refresh_uses_quantity_one_only(self):
        runtime, book, provider, mono, products = self.fixture()
        provider.retail_quote.assert_not_called()
        self.assertIsNone(runtime.get([]))
        self.assertFalse(runtime.warmup(list(products), quantities=(4,)))
        runtime.start(background=False)
        self.assertTrue(runtime.warmup(list(products), quantities=(1, 4)))
        provider.retail_quote.assert_not_called()
        self.assertTrue(runtime.run_once())
        self.assertEqual(provider.retail_quote.call_args.args[1], 1)
        self.assertEqual(len(book.publications), 1)
        published = provider.retail_quote.call_args.args[0]['id']
        self.assertEqual(runtime.get(published), {'id': published})
        self.assertFalse(runtime.warmup([published], quantities=(4,)))

    def test_spacing_budget_and_round_robin_do_not_depend_on_anonymous_page_traffic(self):
        runtime, book, provider, mono, products = self.fixture(budget=2)
        runtime.start(background=False)
        self.assertTrue(runtime.run_once())
        mono[0] = 1
        self.assertFalse(runtime.run_once())
        mono[0] = 3
        self.assertTrue(runtime.run_once())
        mono[0] = 10
        self.assertFalse(runtime.run_once())
        mono[0] = 3600
        self.assertTrue(runtime.run_once())
        self.assertEqual([call.args[0]['id'] for call in provider.retail_quote.call_args_list], sorted(products))

    def test_thirty_seven_unavailable_articles_back_off_for_an_hour_not_every_five_minutes(self):
        runtime, book, provider, mono, products = self.fixture(37, success=False)
        runtime.start(background=False)
        for _ in products:
            self.assertTrue(runtime.run_once())
            mono[0] += 2.2
        self.assertEqual(provider.retail_quote.call_count, 37)
        for time in (100, 300, 600, 1800, 3599):
            mono[0] = time
            self.assertFalse(runtime.run_once())
        self.assertEqual(provider.retail_quote.call_count, 37)
        mono[0] = 3600
        self.assertTrue(runtime.run_once())
        self.assertEqual(provider.retail_quote.call_count, 38)

    def test_only_one_provider_read_can_be_in_flight_and_stop_prevents_new_work(self):
        runtime, book, provider, mono, products = self.fixture()
        entered, release = threading.Event(), threading.Event()
        def wait(*args):
            entered.set()
            if not release.wait(2):
                raise AssertionError('Synthetic provider was not released')
            return {'synthetic': True}
        provider.retail_quote.side_effect = wait
        runtime.start(background=False)
        worker = threading.Thread(target=runtime.run_once)
        worker.start()
        try:
            self.assertTrue(entered.wait(1))
            mono[0] = 5
            self.assertFalse(runtime.run_once())
            runtime.stop()
            self.assertFalse(runtime.run_once())
        finally:
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(provider.retail_quote.call_count, 1)

    def test_checkout_reviews_only_the_exact_due_published_identifier(self):
        runtime, book, provider, _, products = self.fixture()
        fresh, unpublished, requested = sorted(products)
        book.due.remove(fresh)
        book.published.remove(unpublished)
        self.assertFalse(runtime.refresh_for_checkout(requested))
        runtime.start(background=False)
        for identifier in (fresh, unpublished, None, 'not-a-product'):
            self.assertFalse(runtime.refresh_for_checkout(identifier))
        provider.retail_quote.assert_not_called()
        self.assertTrue(runtime.refresh_for_checkout(requested))
        provider.retail_quote.assert_called_once_with(products[requested], 1)
        self.assertFalse(runtime.refresh_for_checkout(requested))
        self.assertEqual(runtime.status()['reserved_reads_in_period'], 1)

    def test_checkout_prioritizes_one_slot_after_spacing_and_shares_failure_backoff(self):
        runtime, book, provider, mono, products = self.fixture()
        first, second, requested = sorted(products)
        runtime.start(background=False)
        self.assertTrue(runtime.run_once())
        for _ in range(5):
            self.assertFalse(runtime.refresh_for_checkout(requested))
        self.assertEqual(runtime.status()['queued'], 1)
        self.assertFalse(runtime.run_once())
        book.success = False
        mono[0] = 3
        self.assertTrue(runtime.run_once())
        self.assertEqual([call.args[0]['id'] for call in provider.retail_quote.call_args_list], [first, requested])
        self.assertEqual(runtime.status()['queued'], 0)
        mono[0] = 6
        self.assertFalse(runtime.refresh_for_checkout(requested))
        self.assertTrue(runtime.run_once())
        self.assertEqual(provider.retail_quote.call_args.args[0]['id'], second)
        mono[0] = 3603
        book.success = True
        self.assertTrue(runtime.refresh_for_checkout(requested))

    def test_hourly_limit_includes_checkout_with_room_reserved_from_background(self):
        runtime, book, provider, mono, products = self.fixture(count=302)
        runtime.start(background=False)
        for _ in range(288):
            self.assertTrue(runtime.run_once())
            mono[0] += 3
        self.assertFalse(runtime.run_once())
        remaining = sorted(book.due)
        queued = remaining[-1]
        for requested in remaining[:12]:
            self.assertTrue(runtime.refresh_for_checkout(requested))
            mono[0] += 3
        self.assertFalse(runtime.refresh_for_checkout(queued))
        self.assertFalse(runtime.run_once())
        self.assertEqual(provider.retail_quote.call_count, 300)
        mono[0] = 3600
        self.assertTrue(runtime.run_once())
        self.assertEqual(provider.retail_quote.call_args.args[0]['id'], queued)

    def test_checkout_joins_inflight_review_without_a_second_read(self):
        runtime, book, provider, mono, products = self.fixture(count=1)
        identifier = next(iter(products))
        entered, release = threading.Event(), threading.Event()
        def fetch(*args):
            entered.set()
            release.wait(2)
            return {'synthetic': True}
        provider.retail_quote.side_effect = fetch
        runtime.start(background=False)
        worker = threading.Thread(target=runtime.run_once)
        worker.start()
        try:
            self.assertTrue(entered.wait(1))
            self.assertFalse(runtime.refresh_for_checkout(identifier, wait_seconds=.01))
            self.assertEqual(runtime.status()['queued'], 0)
            joined = threading.Timer(.1, release.set)
            joined.start()
            try:
                self.assertTrue(runtime.refresh_for_checkout(identifier, wait_seconds=1))
            finally:
                joined.join()
        finally:
            release.set()
            worker.join()
        self.assertEqual(provider.retail_quote.call_count, 1)


if __name__ == '__main__':
    unittest.main()
