"""Storage regressions on disposable files and a separate synthetic SQLite DB."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import random
import shutil
import stat
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backup_storage import BackupStorage, BackupStorageError, LimitedBackupWriter


def make_archive(directory, number, *, payload=b"x" * 1024, invalid_json=False, wrong_manifest=False):
    path = directory / f"kundenstatus-backup-20260101-0000{number:02d}.zip"
    manifest = {
        "format_version": 4, "backup_file": "another.zip" if wrong_manifest else path.name,
        "upload_count": 1,
    }
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as archive:
        archive.writestr("backup.json", "broken" if invalid_json else json.dumps({
            "format_version": 4, "tables": {"auftraege": [], "autohaeuser": []},
        }))
        archive.writestr("uploads/example.bin", payload)
        archive.writestr("manifest.json", json.dumps(manifest))
    return path


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="backup-storage-")
        self.directory = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_byte_budget_and_count_preserve_newest_two(self):
        paths = [make_archive(self.directory, i) for i in range(7)]
        size = paths[0].stat().st_size
        storage = BackupStorage(self.directory, keep=168, max_bytes=6 * size)
        limit = storage.prepare()
        self.assertEqual(limit, 2 * size)
        self.assertEqual(sum(p.stat().st_size for p in self.directory.iterdir()), 4 * size)
        self.assertTrue(paths[-1].exists() and paths[-2].exists())
        latest = make_archive(self.directory, 8)
        storage.validate_created(latest)
        storage.finish()
        self.assertLessEqual(sum(p.stat().st_size for p in self.directory.iterdir()), 6 * size)
        BackupStorage(self.directory, keep=3).finish()
        self.assertEqual(len(list(self.directory.iterdir())), 3)

    def test_corrupt_unknown_partial_and_mismatched_archives_are_not_deleted(self):
        paths = [make_archive(self.directory, i) for i in range(4)]
        invalid = make_archive(self.directory, 9, invalid_json=True)
        wrong = make_archive(self.directory, 8, wrong_manifest=True)
        broken = self.directory / "kundenstatus-backup-20260101-000010.zip"
        broken.write_bytes(b"broken ZIP")
        unknown = self.directory / "original-document.bin"
        unknown.write_bytes(b"original")
        partial = self.directory / ".kundenstatus-backup-20260101-000011.zip.stale.part"
        partial.write_bytes(b"partial")
        keep_bytes = {p: p.read_bytes() for p in (invalid, wrong, broken, unknown, partial)}
        BackupStorage(self.directory, keep=2).finish()
        self.assertTrue(paths[-1].exists() and paths[-2].exists())
        self.assertFalse(paths[0].exists() or paths[1].exists())
        for path, content in keep_bytes.items():
            self.assertEqual(path.read_bytes(), content)

    def test_impossible_budget_does_not_delete_any_recovery_copy(self):
        paths = [make_archive(self.directory, i) for i in range(3)]
        size = paths[0].stat().st_size
        with self.assertRaises(BackupStorageError):
            BackupStorage(self.directory, keep=3, max_bytes=2 * size).prepare()
        self.assertTrue(all(path.exists() for path in paths))

    def test_no_space_refuses_before_deleting_or_creating_files(self):
        paths = [make_archive(self.directory, i) for i in range(3)]
        size = paths[0].stat().st_size
        with patch("backup_storage.shutil.disk_usage", return_value=SimpleNamespace(free=0)):
            with self.assertRaises(BackupStorageError):
                BackupStorage(self.directory, keep=3, max_bytes=6 * size, reserve_bytes=5 * size).prepare()
        self.assertEqual(set(self.directory.iterdir()), set(paths))

    def test_unknown_file_consumes_budget_and_unknown_subdirectory_refuses(self):
        unknown = self.directory / "keep-me.bin"
        unknown.write_bytes(b"x" * 1000)
        with self.assertRaises(BackupStorageError):
            BackupStorage(self.directory, keep=5, max_bytes=1000).prepare()
        self.assertEqual(unknown.stat().st_size, 1000)
        unknown.unlink()
        (self.directory / "unmeasured-folder").mkdir()
        with self.assertRaises(BackupStorageError):
            BackupStorage(self.directory, keep=5, max_bytes=1000).prepare()

    def test_local_count_one_remains_supported_without_byte_budget(self):
        paths = [make_archive(self.directory, i) for i in range(3)]
        BackupStorage(self.directory, keep=1).finish()
        self.assertEqual(list(self.directory.iterdir()), [paths[-1]])

    def test_symlink_is_not_followed_or_deleted(self):
        originals = self.directory / "originals"
        originals.mkdir()
        original = make_archive(originals, 0)
        target_bytes = original.read_bytes()
        backups = self.directory / "backups"
        backups.mkdir()
        link = backups / original.name
        try:
            link.symlink_to(original)
        except OSError as exc:
            self.skipTest(f"OS cannot create a symlink: {exc}")
        for i in range(1, 5):
            make_archive(backups, i)
        BackupStorage(backups, keep=2).finish()
        self.assertTrue(link.is_symlink())
        self.assertEqual(original.read_bytes(), target_bytes)

    def test_hardlink_is_not_deleted(self):
        original = make_archive(self.directory, 0)
        alias = self.directory / "original-alias.bin"
        os.link(original, alias)
        for i in range(1, 5):
            make_archive(self.directory, i)
        BackupStorage(self.directory, keep=2).finish()
        self.assertTrue(original.exists() and alias.exists())

    def test_symlink_metadata_is_excluded_without_opening_target(self):
        # Exercises link refusal even on Windows without CreateSymbolicLink privilege.
        linked = make_archive(self.directory, 0)
        for i in range(1, 5):
            make_archive(self.directory, i)
        original_lstat = Path.lstat
        original_open = Path.open

        def lstat_result(path):
            if path == linked:
                return SimpleNamespace(st_mode=stat.S_IFLNK | 0o777, st_file_attributes=0)
            return original_lstat(path)

        def open_file(path, *args, **kwargs):
            self.assertNotEqual(path, linked, "Linked target must never be opened")
            return original_open(path, *args, **kwargs)

        with patch.object(Path, "lstat", lstat_result), patch.object(Path, "open", open_file):
            BackupStorage(self.directory, keep=2).finish()
        self.assertTrue(linked.exists())

    def test_damaged_compressed_member_does_not_abort_or_replace_valid_copies(self):
        first = make_archive(self.directory, 0)
        second = make_archive(self.directory, 1)
        damaged = self.directory / "kundenstatus-backup-20260101-000009.zip"
        with zipfile.ZipFile(damaged, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("backup.json", json.dumps({
                "format_version": 4, "tables": {"auftraege": [], "autohaeuser": []},
            }))
            archive.writestr("manifest.json", json.dumps({
                "format_version": 4, "backup_file": damaged.name, "upload_count": 1,
            }))
            archive.writestr("uploads/broken.bin", b"x" * 1024)
        with zipfile.ZipFile(damaged) as archive:
            info = archive.getinfo("uploads/broken.bin")
            offset = info.header_offset + 30 + len(info.filename.encode()) + len(info.extra)
        data = bytearray(damaged.read_bytes())
        data[offset] = 0x07  # Reserved deflate block type; central directory is intact.
        damaged.write_bytes(data)
        BackupStorage(self.directory, keep=2).finish()
        self.assertTrue(first.exists() and second.exists() and damaged.exists())

    def test_writer_enforces_actual_size_including_seek_and_zip_footer(self):
        raw = io.BytesIO()
        writer = LimitedBackupWriter(raw, self.directory, 10, 0)
        writer.write(b"12345678")
        writer.seek(0)
        writer.write(b"abcdefgh")
        self.assertEqual(len(raw.getvalue()), 8)
        writer.seek(8)
        with self.assertRaises(BackupStorageError):
            writer.write(b"XYZ")
        self.assertEqual(len(raw.getvalue()), 8)
        raw = io.BytesIO()
        with self.assertRaises(BackupStorageError):
            with zipfile.ZipFile(LimitedBackupWriter(raw, self.directory, 80, 0), "w") as archive:
                archive.writestr("a", b"payload")
        self.assertLessEqual(len(raw.getvalue()), 80)

    def test_writer_detects_falling_disk_space(self):
        raw = io.BytesIO()
        writer = LimitedBackupWriter(raw, self.directory, 100, 10)
        with patch("backup_storage.shutil.disk_usage", return_value=SimpleNamespace(free=20)):
            writer.write(b"12345")
        with patch("backup_storage.shutil.disk_usage", return_value=SimpleNamespace(free=12)):
            with self.assertRaises(BackupStorageError):
                writer.write(b"67890")
        self.assertEqual(raw.getvalue(), b"12345")

    def test_twenty_mib_compressed_write_and_crc(self):
        path = self.directory / "benchmark.part"
        payload = random.Random(41).randbytes(20 * 1024 * 1024)
        started = time.monotonic()
        with path.open("xb", buffering=0) as raw:
            writer = LimitedBackupWriter(raw, self.directory, 21 * 1024 * 1024, 0)
            with zipfile.ZipFile(writer, "w", zipfile.ZIP_DEFLATED) as archive:
                # Match ZipFile.write's chunked upload compression path.
                with archive.open("synthetic.bin", "w") as target:
                    for offset in range(0, len(payload), 8192):
                        target.write(payload[offset:offset + 8192])
        with zipfile.ZipFile(path) as archive:
            self.assertIsNone(archive.testzip())
            self.assertEqual(archive.getinfo("synthetic.bin").file_size, len(payload))
        print(f"20 MiB compressed archive + CRC: {time.monotonic() - started:.2f}s")


class ApplicationBackupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="backup-app-test-")
        cls.root = Path(cls.temp.name)
        os.environ.update({
            "RENDER": "isolated-backup-test", "DATABASE_URL": "", "REQUIRE_POSTGRES_ON_RENDER": "0",
            "DATA_DIR": str(cls.root), "SQLITE_DB_PATH": str(cls.root / "test.db"),
            "UPLOAD_DIR": str(cls.root / "uploads"), "BACKUP_DIR": str(cls.root / "backups"),
            "DELETED_UPLOAD_DIR": str(cls.root / "deleted"), "AUTO_BACKUP_ENABLED": "0",
            "AUTO_CHANGE_BACKUP_ENABLED": "0", "GOOGLE_ADS_AUTO_SYNC_ENABLED": "0",
            "LEXWARE_API_KEY": "", "OPENAI_API_KEY": "", "FLASK_SECRET_KEY": "isolated-test-secret",
            "ADMIN_PASS": "isolated-test-password", "PUBLIC_SITE_ONLY": "0",
        })
        import app as portal
        cls.portal = portal
        portal.app.config["TESTING"] = True

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_real_sqlite_backup_remains_importable_and_failed_growth_keeps_two(self):
        portal = self.portal
        portal.AUTO_BACKUP_MAX_BYTES = 6 * 1024 * 1024
        portal.AUTO_BACKUP_RESERVE_BYTES = 0
        portal.AUTO_BACKUP_KEEP = 5
        source = portal.UPLOAD_DIR / "synthetic.bin"
        original = random.Random(7).randbytes(100 * 1024)
        source.write_bytes(original)
        first = portal.create_backup_package("isolated-test")
        second = portal.create_backup_package("isolated-test")
        self.assertNotEqual(first, second)
        with zipfile.ZipFile(second) as archive:
            names, _ = portal.validate_import_package_archive(archive)
            self.assertTrue({"backup.json", "manifest.json", "auftraege.db"}.issubset(names))
            self.assertEqual(archive.read("uploads/synthetic.bin"), original)
            self.assertIsNone(archive.testzip())
        hashes = {path: path.read_bytes() for path in (first, second)}
        source.write_bytes(random.Random(8).randbytes(3 * 1024 * 1024))
        with self.assertRaises(BackupStorageError):
            portal.create_backup_package("isolated-oversize")
        for path, content in hashes.items():
            self.assertEqual(path.read_bytes(), content)
        self.assertFalse(list(portal.BACKUP_DIR.glob("*.part")))
        self.assertEqual(source.stat().st_size, 3 * 1024 * 1024)

    def test_analytics_excluded_but_business_mutations_still_schedule(self):
        for path in ("/api/besucher", "/api/klick"):
            with self.portal.app.test_request_context(path, method="POST"):
                self.assertFalse(self.portal.should_backup_after_request(), path)
        with self.portal.app.test_request_context("/admin/daten-import", method="POST"):
            self.assertTrue(self.portal.should_backup_after_request())
        with self.portal.app.test_request_context("/admin", method="GET"):
            self.assertFalse(self.portal.should_backup_after_request())


if __name__ == "__main__":
    unittest.main(verbosity=2)
