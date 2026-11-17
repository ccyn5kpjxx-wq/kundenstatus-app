"""Bounded five-day refresh scheduling, synthetic books/providers only."""
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
        self.success = success
        self.publications = []

    def due_ids(self):
        return sorted(self.due)

    def get(self, identifier):
        return {'id': identifier} if identifier not in self.due else None

    def publish_uniform(self, product, response):
        self.publications.append((product, response))
        if self.success:
            self.due.remove(product['id'])
        return self.success


class PriceBookRuntimeChecks(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
