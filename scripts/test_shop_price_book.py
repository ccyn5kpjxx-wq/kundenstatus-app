"""Daily merchant publication, restart recovery and fresh-cost boundaries."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
from shop_price_book import PublishedPriceBook, PriceBookError
from shop_catalog_public import PublicCatalog, variant_fingerprint
from test_shop_catalog_public import wheel

NOW = datetime(2026, 11, 17, 17, 17, tzinfo=timezone.utc)


class PriceBookChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name).resolve() / 'shop-pricebook/prices.sqlite3'
        self.row = wheel(quote_public=None)
        self.id = self.row['selection_id']
        self.product = {'id': self.id, 'identity_hash': variant_fingerprint(self.row), 'category': 'alloy_wheel'}
        self.products = {self.id: self.product}
        self.time = [NOW]
        self.network = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def book(self, **changes):
        return PublishedPriceBook(self.path, products=self.products, approved_ids=frozenset(self.products),
                                  mode=changes.get('mode', 'live'), clock=lambda: self.time[0])

    def seed(self, **changes):
        return {'source': 'RSU', 'identity_hash': self.product['identity_hash'],
                'observed_at': NOW.isoformat(), 'unit_gross_cent': 15000, 'stock_reported': 20, **changes}

    def fresh(self, quantity=1, **changes):
        now = self.time[0]
        return {'source': 'RSU', 'confirmed': True, 'procurement_confirmed': True,
                'product_id': self.id, 'identity_hash': self.product['identity_hash'], 'mode': 'live',
                'currency': 'eur', 'vat_percent': '19.00', 'quantity': quantity, 'stock': 12,
                'unit_gross_cent': 12000, 'observed_at': now.isoformat(),
                'expires_at': (now + timedelta(minutes=15)).isoformat(), **changes}

    def ready(self):
        book = self.book().provision(create=True)
        self.assertEqual(book.seed({self.id: self.seed()}, evidence_id='a' * 64), 1)
        return book

    def test_inert_construction_explicit_creation_and_missing_file_not_recreated(self):
        book = self.book()
        self.assertFalse(self.path.parent.exists())
        with self.assertRaises(PriceBookError):
            book.provision()
        book.provision(create=True)
        self.assertTrue(book.ready)
        self.path.unlink()
        self.assertIsNone(book.get(self.id))
        with self.assertRaises(PriceBookError):
            book.seed({self.id: self.seed()}, evidence_id='a' * 64)
        self.assertFalse(self.path.exists())

    def test_initialized_marker_prevents_empty_recreation_after_restart(self):
        book = self.ready()
        marker = book.marker_path.read_bytes()
        marker_stat = book.marker_path.stat()
        self.book().provision(create=True)
        self.assertEqual(book.marker_path.read_bytes(), marker)
        self.assertEqual(book.marker_path.stat().st_mtime_ns, marker_stat.st_mtime_ns)
        self.path.unlink()
        with self.assertRaisesRegex(PriceBookError, 'initialized_pricebook_missing'):
            self.book().provision(create=True)
        self.assertFalse(self.path.exists())
        self.assertEqual(book.marker_path.read_bytes(), marker)

    def test_missing_old_marker_adopted_only_after_valid_database_and_corrupt_marker_rejected(self):
        book = self.ready()
        book.marker_path.unlink()
        self.book().provision()
        self.assertTrue(book.marker_path.is_file())
        book.marker_path.write_bytes(b'wrong marker')
        with self.assertRaisesRegex(PriceBookError, 'foreign_pricebook_marker'):
            self.book().provision(create=True)
        self.assertEqual(book.marker_path.read_bytes(), b'wrong marker')
        book.marker_path.unlink()
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('DROP TABLE shop_public_pricebook_meta')
        with self.assertRaises(PriceBookError):
            self.book().provision(create=True)
        self.assertFalse(book.marker_path.exists())

    def test_private_snapshot_recovers_latest_committed_publication_without_reseed(self):
        book = self.ready()
        self.time[0] += timedelta(days=5)
        book.publish_uniform(self.product, self.fresh(unit_gross_cent=16000, stock=7))
        snapshot = book.snapshot()
        self.assertIsInstance(snapshot, bytes)
        self.assertTrue(snapshot.startswith(b'SQLite format 3\0'))
        recovered = self.path.parent.parent / 'recovered'
        recovered.mkdir(mode=0o700)
        restore_path = recovered / 'prices.sqlite3'
        restore_path.write_bytes(snapshot)
        restored = PublishedPriceBook(restore_path, products=self.products,
            approved_ids=frozenset(self.products), clock=lambda: self.time[0]).provision()
        self.assertEqual(restored.seed({self.id: self.seed()}, evidence_id='a' * 64), 0)
        self.assertEqual(restored.get(self.id)['prices_by_quantity']['4']['items_gross_cent'], 64000)
        self.assertEqual(restored.get(self.id)['stock_reported'], 7)
        self.assertEqual(restored.get(self.id)['review_at'], (self.time[0] + timedelta(days=1)).isoformat())
        self.path.unlink()
        with self.assertRaises(PriceBookError):
            book.snapshot()

    def test_snapshot_requires_valid_marker_and_honours_private_size_bound(self):
        book = self.ready()
        with patch('shop_price_book.SNAPSHOT_MAX_BYTES', 1):
            with self.assertRaisesRegex(PriceBookError, 'pricebook_snapshot_too_large'):
                book.snapshot()
        book.marker_path.unlink()
        with self.assertRaises(PriceBookError):
            book.snapshot()

    def test_seed_is_reviewed_daily_and_stock_has_its_own_expiry(self):
        book = self.ready()
        self.time[0] += timedelta(hours=23, minutes=59)
        price = book.get(self.id)
        self.assertEqual(price['kind'], 'merchant_pricebook')
        self.assertFalse(price['stock_current'])
        self.assertEqual(price['expires_at'], (NOW + timedelta(days=7)).isoformat())
        self.assertEqual(price['review_at'], (NOW + timedelta(days=1)).isoformat())
        self.assertEqual(price['prices_by_quantity']['4']['items_gross_cent'], 60000)
        self.assertNotIn('evidence_id', price)
        self.assertEqual(book.due_ids(), [])
        self.assertFalse(book.review_due(self.id))
        self.time[0] = NOW + timedelta(days=1)
        self.assertIsNotNone(book.get(self.id))
        self.assertEqual(book.due_ids(), [self.id])
        self.assertTrue(book.review_due(self.id))
        self.assertFalse(book.review_due('tyre:' + 'f' * 24))
        self.time[0] = NOW + timedelta(days=7)
        self.assertIsNone(book.get(self.id))

    def test_restart_retains_publication_and_old_seed_never_overwrites_newer_record(self):
        book = self.ready()
        self.time[0] += timedelta(days=5)
        self.assertIs(book.publish_uniform(self.product, self.fresh(unit_gross_cent=16000)), True)
        restarted = self.book().provision()
        self.assertEqual(restarted.seed({self.id: self.seed()}, evidence_id='a' * 64), 0)
        self.assertEqual(restarted.get(self.id)['prices_by_quantity']['1']['unit_gross_cent'], 16000)
        self.assertEqual(restarted.get(self.id)['expires_at'], (self.time[0] + timedelta(days=7)).isoformat())

    def test_background_quote_within_window_changes_stock_not_merchant_amount_or_expiry(self):
        book = self.ready()
        self.time[0] += timedelta(hours=2)
        book.publish_uniform(self.product, self.fresh(unit_gross_cent=18000, stock=7))
        price = book.get(self.id)
        self.assertEqual(price['prices_by_quantity']['1']['unit_gross_cent'], 15000)
        self.assertEqual(price['expires_at'], (NOW + timedelta(days=7)).isoformat())
        self.assertEqual(price['stock_reported'], 7)
        self.assertTrue(price['stock_current'])

    def test_legacy_rows_become_due_after_one_day_and_lower_price_is_published(self):
        self.ready()
        self.time[0] += timedelta(days=1, minutes=1)
        book = self.book().provision()
        self.assertEqual(book.get(self.id)['observed_at'], NOW.isoformat())
        self.assertEqual(book.due_ids(), [self.id])
        book.publish_uniform(self.product, self.fresh(unit_gross_cent=14000))
        self.assertEqual(book.get(self.id)['prices_by_quantity']['4']['items_gross_cent'], 56000)
        self.assertFalse(book.review_due(self.id))

    def test_day_boundary_preserves_a_newer_stock_observation(self):
        book = self.ready()
        self.time[0] += timedelta(hours=23, minutes=58)
        price_quote = self.fresh(unit_gross_cent=15300, stock=9)
        self.time[0] += timedelta(minutes=1)
        newer_stock_at = self.time[0].isoformat()
        book.publish_uniform(self.product, self.fresh(unit_gross_cent=15300, stock=2))
        self.time[0] += timedelta(minutes=1)
        book.publish_uniform(self.product, price_quote)
        price = book.get(self.id)
        self.assertEqual(price['prices_by_quantity']['1']['unit_gross_cent'], 15300)
        self.assertEqual((price['stock_reported'], price['stock_observed_at']), (2, newer_stock_at))

    def test_early_increase_needs_single_unit_and_exact_quantity_confirmation(self):
        book = self.ready()
        self.time[0] += timedelta(hours=1)
        def quote(q, unit, stock=12):
            return book.checkout_quote(self.product, q, lambda *args: self.fresh(q, unit_gross_cent=unit, stock=stock),
                                       report_price_change=True)
        self.assertFalse(book.review_due(self.id))
        self.assertEqual(quote(4, 16000)['reason'], 'published_price_review_required')
        self.assertTrue(book.review_due(self.id))
        with self.assertRaises(PriceBookError):
            book.publish_uniform(self.product, self.fresh(4, unit_gross_cent=16000))
        self.assertEqual(book.get(self.id)['prices_by_quantity']['1']['unit_gross_cent'], 15000)
        book.publish_uniform(self.product, self.fresh(1, unit_gross_cent=16000))
        self.assertFalse(book.review_due(self.id))
        self.assertEqual(quote(4, 16000)['unit_gross_cent'], 16000)
        self.assertIsNone(quote(4, 16000, stock=3))
        # A quantity-only premium never changes the merchant unit or loops while
        # the just-confirmed Q1 observation is valid.
        self.assertEqual(quote(4, 16500)['reason'], 'published_price_review_required')
        self.assertFalse(book.review_due(self.id))

    def test_atomic_seed_rejects_alzura_wrong_identity_and_future_initial_observations(self):
        book = self.book().provision(create=True)
        for changes in ({'source': 'ALZURA'}, {'identity_hash': '0' * 64},
                        {'observed_at': (NOW + timedelta(seconds=1)).isoformat()}, {'unit_gross_cent': True}):
            with self.subTest(changes=changes), self.assertRaises(PriceBookError):
                book.seed({self.id: self.seed(**changes)}, evidence_id='a' * 64)
        self.assertIsNone(book.get(self.id))
        self.assertEqual(book.due_ids(), [self.id])

    def test_expired_seed_on_fresh_or_partial_recovery_is_skipped_without_retiming(self):
        book = self.book().provision(create=True)
        self.time[0] = NOW + timedelta(days=7)
        self.assertEqual(book.seed({self.id: self.seed()}, evidence_id='a' * 64), 0)
        self.assertIsNone(book.get(self.id))
        self.assertEqual(book.due_ids(), [self.id])
        self.assertIs(book.publish_uniform(self.product, self.fresh(unit_gross_cent=17000)), True)
        self.assertEqual(book.seed({self.id: self.seed()}, evidence_id='a' * 64), 0)
        self.assertEqual(book.get(self.id)['prices_by_quantity']['1']['unit_gross_cent'], 17000)

    def test_checkout_fresh_exact_quantity_keeps_pinned_price_until_increase_verified(self):
        book = self.ready()
        self.time[0] += timedelta(days=2)
        provider = Mock(return_value=self.fresh(quantity=4))
        result = book.checkout_quote(self.product, 4, provider)
        provider.assert_called_once_with(self.product, 4)
        self.assertEqual(result['unit_gross_cent'], 15000)
        self.assertEqual(result['observed_at'], self.time[0].isoformat())
        for changes in ({'unit_gross_cent': 15001}, {'stock': 3}, {'quantity': 1},
                        {'identity_hash': '0' * 64}, {'procurement_confirmed': False}, {'source': 'ALZURA'},
                        {'expires_at': (self.time[0] + timedelta(days=5)).isoformat()}):
            with self.subTest(changes=changes):
                self.assertIsNone(book.checkout_quote(self.product, 4, lambda *args: self.fresh(quantity=4, **changes)))
        self.assertEqual(book.get(self.id)['prices_by_quantity']['4']['unit_gross_cent'], 15000)
        self.assertEqual(book.checkout_quote(self.product, 4, lambda *args: self.fresh(quantity=4, unit_gross_cent=15001),
                                              report_price_change=True),
                         {'confirmed': False, 'reason': 'published_price_review_required'})
        self.assertIsNone(book.checkout_quote(self.product, 4, lambda *args: self.fresh(quantity=4, stock=1),
                                              report_price_change=True))

    def test_checkout_uses_price_published_while_fetching_stock(self):
        book = self.ready()
        self.time[0] += timedelta(days=5)
        def fresh_after_publication(product, quantity):
            book.publish_uniform(product, self.fresh(unit_gross_cent=15005))
            return self.fresh(quantity=quantity, unit_gross_cent=15005)
        self.assertEqual(book.checkout_quote(self.product, 4, fresh_after_publication)['unit_gross_cent'], 15005)

    def test_mode_identity_and_corrupt_or_foreign_store_are_not_silently_adopted(self):
        self.ready()
        with self.assertRaises(PriceBookError):
            self.book(mode='test').provision()
        wrong = {**self.product, 'identity_hash': '0' * 64}
        self.assertIsNone(self.book().provision().checkout_quote(wrong, 1, Mock()))

    def test_public_catalogue_requires_explicit_merchant_kind_permission_and_separate_stock_dates(self):
        book = self.ready()
        self.time[0] += timedelta(days=2)
        options = dict(released_ids=[self.id], quote_lookup=book.get, quote_lookup_authoritative=True,
                       clock=lambda: self.time[0])
        self.assertIsNone(PublicCatalog([self.row], **options).get(self.id)['price'])
        catalogue = PublicCatalog([self.row], allow_published_prices=True, **options)
        displayed = catalogue.get(self.id)['price']
        self.assertEqual(displayed['source'], 'RSU')
        self.assertEqual(displayed['prices_by_quantity']['1']['unit_gross_cent'], 15000)
        self.assertFalse(displayed['stock_current'])
        self.assertNotIn('source_evidence', displayed)
        for changes in ({'source': 'ALZURA'}, {'kind': 'untyped'},
                        {'expires_at': (NOW + timedelta(days=8)).isoformat()},
                        {'stock_expires_at': (NOW + timedelta(days=5)).isoformat()}):
            fake = {**book.get(self.id), **changes}
            projected = PublicCatalog([self.row], released_ids=[self.id], allow_published_prices=True,
                quote_lookup=lambda _: fake, quote_lookup_authoritative=True, clock=lambda: self.time[0])
            self.assertIsNone(projected.get(self.id)['price'])

    def test_display_and_due_reads_use_detached_l1_without_filesystem_or_sqlite_calls(self):
        book = self.ready()
        with (patch('sqlite3.connect', side_effect=AssertionError('No SQLite on display')),
              patch.object(Path, 'is_file', side_effect=AssertionError('No filesystem on display')),
              patch.object(Path, 'resolve', side_effect=AssertionError('No filesystem on display'))):
            self.assertEqual(book.get(self.id)['prices_by_quantity']['4']['unit_gross_cent'], 15000)
            self.assertEqual(book.due_ids(), [])
            changed = book.get(self.id)
            changed['prices_by_quantity']['1']['unit_gross_cent'] = 1
            self.assertEqual(book.get(self.id)['prices_by_quantity']['1']['unit_gross_cent'], 15000)
        self.path.unlink()
        self.assertEqual(book.get(self.id)['prices_by_quantity']['1']['unit_gross_cent'], 15000)
        provider = Mock(return_value=self.fresh())
        self.assertIsNone(book.checkout_quote(self.product, 1, provider))
        provider.assert_not_called()

    def test_failed_seed_transaction_cannot_publish_partial_l1_or_database_updates(self):
        book = self.book().provision(create=True)
        with patch.object(book, '_insert', side_effect=PriceBookError('synthetic_rollback')):
            with self.assertRaises(PriceBookError):
                book.seed({self.id: self.seed()}, evidence_id='a' * 64)
        self.assertIsNone(book.get(self.id))
        self.assertIsNone(self.book().provision().get(self.id))


if __name__ == '__main__':
    unittest.main()
