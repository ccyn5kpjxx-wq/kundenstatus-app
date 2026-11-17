"""Persistent shop provisioning and consistent recovery copy, temporary IO only."""
from contextlib import closing
from email.message import EmailMessage
from hashlib import sha256
from io import BytesIO
from pathlib import Path
import json
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_storage import ShopStorage, StorageError
from mailbox_outbox import MailOutbox


class ShopStorageChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)
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
            self.assertEqual(archive.namelist(), ['shop-v2/shop.sqlite3', 'shop-v2/initialized'])
            data = archive.read(info['path'])
            self.assertEqual(len(data), info['bytes'])
            self.assertEqual(archive.read(info['marker']['path']), b'shop_storage_v2\n')
            self.assertEqual(info['outbox'], {'records': 0, 'bytes': 0, 'files': []})
        self.assertEqual(info['format'], 'shop_sqlite_v2')

    def pending_mail(self):
        storage = ShopStorage(self.root).provision(create=True)
        outbox = MailOutbox(storage.connect, storage.directory / 'mail-outbox', Mock())
        message = EmailMessage()
        message['From'], message['To'] = 'sender@example.invalid', 'customer@example.invalid'
        message['Subject'] = 'SYNTHETIC legal confirmation'
        message['Message-ID'] = '<synthetic-storage-test@example.invalid>'
        message.set_content('SYNTHETIC frozen terms and original declaration')
        token = str(uuid4())
        with (patch.object(outbox, '_preflight', side_effect=ValueError('SYNTHETIC offline preflight')),
                patch.object(outbox, '_smtp', side_effect=AssertionError('SMTP forbidden'))):
            result = outbox.send(token, message, {'smtp_user': 'sender@example.invalid'})
        self.assertEqual(result['state'], 'not_sent')
        return storage, outbox, token

    def backup(self, storage):
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, 'w') as archive:
            manifest = storage.write_backup(archive)
        return buffer.getvalue(), manifest

    def test_mail_original_journal_and_marker_restore_with_real_outbox_without_provider_calls(self):
        storage, outbox, token = self.pending_mail()
        row = outbox._read(token)
        original = outbox.payload(token)
        (storage.directory / 'mail-outbox' / 'do-not-include-private.txt').write_bytes(b'PRIVATE_CANARY')
        raw, manifest = self.backup(storage)
        restored_root = self.root / 'restored'
        restored_root.mkdir()
        with zipfile.ZipFile(BytesIO(raw)) as archive:
            self.assertNotIn('PRIVATE_CANARY', repr(archive.namelist()))
            self.assertNotIn('shop-v2/mail-outbox/do-not-include-private.txt',archive.namelist())
            self.assertEqual(manifest['outbox']['records'], 1)
            self.assertEqual(len(manifest['outbox']['files']), 3)
            for item in manifest['outbox']['files']:
                content = archive.read(item['path'])
                self.assertEqual(sha256(content).hexdigest(), item['sha256'])
                self.assertEqual(len(content), item['bytes'])
            for name in archive.namelist():
                target = restored_root / name
                self.assertTrue(target.resolve().is_relative_to(restored_root.resolve()))
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.read(name))
        restored = ShopStorage(restored_root).provision(create=False)
        recovered_outbox = MailOutbox(restored.connect, restored.directory / 'mail-outbox', Mock())
        self.assertEqual(recovered_outbox.payload(token), original)
        self.assertEqual(recovered_outbox.status(token)['state'], 'not_sent')
        self.assertEqual(recovered_outbox._read(token)['attempt'], row['attempt'])
        self.assertEqual(self.backup(restored)[1]['outbox']['records'], 1)

    def test_active_mail_lock_fails_backup_instead_of_mixing_smtp_state(self):
        storage, outbox, token = self.pending_mail()
        buffer = BytesIO()
        with outbox._lock(token) as locked:
            self.assertTrue(locked)
            with zipfile.ZipFile(buffer, 'w') as archive:
                with self.assertRaisesRegex(StorageError, 'shop_backup_unavailable'):
                    storage.write_backup(archive)
                self.assertEqual(archive.namelist(), [])

    def test_newer_durable_journal_is_preserved_when_database_state_lags(self):
        storage, outbox, token = self.pending_mail()
        with closing(storage.connect()) as db:
            db.execute("UPDATE mailbox_outbox SET state='sending',revision=0 WHERE token=?", (token,))
            db.commit()
        raw, manifest = self.backup(storage)
        with zipfile.ZipFile(BytesIO(raw)) as archive:
            item = next(v for v in manifest['outbox']['files'] if v['path'].endswith('.json'))
            journal = json.loads(archive.read(item['path']))
            self.assertEqual(journal['state'], 'not_sent')
            self.assertEqual(journal['revision'], 1)

    def test_completed_sent_copy_can_have_no_remaining_mime_payload(self):
        storage, outbox, token = self.pending_mail()
        with outbox._lock(token) as locked:
            self.assertTrue(locked)
            row = outbox._read(token)
            self.assertTrue(outbox._record(row, state='sent', accepted=['customer@example.invalid'], copy_done=1))
            outbox._path(row['payload_name']).unlink()
        raw, manifest = self.backup(storage)
        self.assertEqual(len(manifest['outbox']['files']), 2)
        self.assertFalse(any(v['path'].endswith('.eml') for v in manifest['outbox']['files']))

    def test_missing_or_tampered_pending_original_and_malformed_journal_are_rejected(self):
        storage, outbox, token = self.pending_mail()
        row = outbox._read(token)
        path = outbox._path(row['payload_name'])
        raw = path.read_bytes()
        path.unlink()
        with self.assertRaises(StorageError):
            self.backup(storage)
        path.write_bytes(raw.replace(b'SYNTHETIC legal confirmation', b'ALTERED message subject'))
        with self.assertRaises(StorageError):
            self.backup(storage)
        path.write_bytes(raw)
        outbox._journal_path(row).write_bytes(b'{"state":"sent","state":"not_sent"}')
        with self.assertRaises(StorageError):
            self.backup(storage)

    def test_db_change_during_file_copy_fails_before_archive_is_written(self):
        storage, outbox, token = self.pending_mail()
        original = storage._checked_file
        mutated = False
        def changing(path, folder, **kwargs):
            nonlocal mutated
            result = original(path, folder, **kwargs)
            if path.suffix == '.eml' and not mutated:
                mutated = True
                with closing(storage.connect()) as db:
                    db.execute('UPDATE mailbox_outbox SET updated_at=updated_at+1 WHERE token=?', (token,))
                    db.commit()
            return result
        buffer = BytesIO()
        with patch.object(storage, '_checked_file', side_effect=changing), zipfile.ZipFile(buffer, 'w') as archive:
            with self.assertRaises(StorageError):
                storage.write_backup(archive)
            self.assertEqual(archive.namelist(), [])
        self.assertTrue(mutated)

    def test_payload_path_and_backup_size_bounds_reject_unreviewed_files(self):
        storage, outbox, token = self.pending_mail()
        with patch('shop_storage.MAX_OUTBOX_FILE', 10), self.assertRaises(StorageError):
            self.backup(storage)
        with patch('shop_storage.MAX_OUTBOX_RECORDS', 0), self.assertRaises(StorageError):
            self.backup(storage)
        with closing(storage.connect()) as db:
            db.execute("UPDATE mailbox_outbox SET payload_name='../secret.eml' WHERE token=?", (token,))
            db.commit()
        with self.assertRaises(StorageError):
            self.backup(storage)

    def test_referenced_symlink_cannot_include_private_external_file(self):
        storage, outbox, token = self.pending_mail()
        row = outbox._read(token)
        payload = outbox._path(row['payload_name'])
        external = self.root / 'SYNTHETIC-secret.eml'
        external.write_bytes(payload.read_bytes())
        payload.unlink()
        try:
            payload.symlink_to(external)
        except (OSError, NotImplementedError):
            self.skipTest('OS does not permit creating test symlinks')
        with self.assertRaises(StorageError):
            self.backup(storage)

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
