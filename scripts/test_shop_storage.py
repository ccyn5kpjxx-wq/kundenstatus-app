"""Persistent shop provisioning and consistent recovery copy, temporary IO only."""
from contextlib import closing
from io import BytesIO
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_storage import ShopStorage, StorageError


class ShopStorageChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_construction_is_idle_and_unprovisioned_storage_cannot_open(self):
        storage = ShopStorage(self.root)
        self.assertFalse(storage.directory.exists())
        with self.assertRaises(StorageError):
            storage.connect()
        with self.assertRaises(StorageError):
            ShopStorage('relative-data')
        with self.assertRaises(StorageError):
            ShopStorage(self.root / 'missing').provision(create=True)

    def test_explicit_provisioning_reopens_same_tables_and_preserves_committed_data(self):
        storage = ShopStorage(self.root).provision(create=True)
        with closing(storage.connect()) as db:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertIn('shop_orders_v2', tables)
            self.assertIn('shop_buyer_sessions_v2', tables)
            db.execute('CREATE TABLE synthetic_recovery (value TEXT)')
            db.execute("INSERT INTO synthetic_recovery VALUES ('SYNTHETIC_ONLY')")
            db.commit()
        again = ShopStorage(self.root).provision()
        with closing(again.connect()) as db:
            self.assertEqual(db.execute('SELECT value FROM synthetic_recovery').fetchone()[0], 'SYNTHETIC_ONLY')

    def test_deleted_database_is_not_silently_recreated(self):
        storage = ShopStorage(self.root).provision(create=True)
        storage.path.unlink()
        with self.assertRaises(StorageError):
            storage.connect()
        self.assertFalse(storage.path.exists())

    def test_snapshot_has_committed_wal_record_and_can_be_opened_without_original_store(self):
        storage = ShopStorage(self.root).provision(create=True)
        with closing(storage.connect()) as writer:
            writer.execute('CREATE TABLE synthetic_recovery (value TEXT)')
            writer.execute("INSERT INTO synthetic_recovery VALUES ('SYNTHETIC_ONLY')")
            writer.commit()
            data = storage.snapshot()
        recovered = self.root / 'recovered.sqlite3'
        recovered.write_bytes(data)
        with closing(sqlite3.connect(recovered)) as db:
            self.assertEqual(db.execute('PRAGMA quick_check').fetchone(), ('ok',))
            self.assertEqual(db.execute('SELECT value FROM synthetic_recovery').fetchone()[0], 'SYNTHETIC_ONLY')
        self.assertFalse(list(storage.directory.glob('.snapshot-*')))

    def test_backup_package_contains_exact_snapshot_and_hash_metadata(self):
        storage = ShopStorage(self.root).provision(create=True)
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            info = storage.write_backup(archive)
        with zipfile.ZipFile(BytesIO(buffer.getvalue())) as archive:
            self.assertEqual(archive.namelist(), ['shop-v2/shop.sqlite3'])
            data = archive.read(info['path'])
            self.assertEqual(len(data), info['bytes'])
        self.assertEqual(info['format'], 'shop_sqlite_v2')

    def test_restart_after_database_loss_cannot_become_an_empty_healthy_store(self):
        storage = ShopStorage(self.root).provision(create=True)
        storage.path.unlink()
        for create in (False, True):
            with self.assertRaises(StorageError):
                ShopStorage(self.root).provision(create=create)
        self.assertFalse(storage.path.exists())

    def test_first_creation_is_explicit_and_unknown_existing_directory_is_not_adopted(self):
        with self.assertRaises(StorageError):
            ShopStorage(self.root).provision()
        self.assertFalse((self.root / 'shop-v2').exists())
        (self.root / 'shop-v2').mkdir()
        with self.assertRaises(StorageError):
            ShopStorage(self.root).provision(create=True)

    def test_nonempty_foreign_or_incomplete_database_is_not_adopted_after_restart(self):
        storage = ShopStorage(self.root).provision(create=True)
        with closing(storage.connect()) as db:
            db.execute('DROP TABLE shop_buyer_sessions_v2')
            db.execute('CREATE TABLE unrelated_records (value TEXT)')
            db.commit()
        self.assertGreater(storage.path.stat().st_size, 0)
        with self.assertRaisesRegex(StorageError, 'initialized_shop_database_missing_or_invalid'):
            ShopStorage(self.root).provision(create=True)
        with closing(sqlite3.connect(storage.path)) as db:
            names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn('shop_buyer_sessions_v2', names)


if __name__ == '__main__':
    unittest.main()
