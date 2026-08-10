# -*- coding: utf-8 -*-
"""Regressionstest fuer die Begrenzung von Datenimport-ZIPs.

Der Test verwendet nur ein eigenes Temp-Verzeichnis. Er prueft, dass ein
normales Backup akzeptiert wird und dass stark komprimierte Grossdaten,
doppelte Namen sowie unsichere Pfade vor dem Entpacken abgewiesen werden.
"""

from __future__ import annotations

import io
import os
import pathlib
import sys
import tempfile
import zipfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="import_package_guard_"))
os.environ.update(
    {
        "RENDER": "local-import-guard-test",
        "DATABASE_URL": "",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEMP_DIR),
        "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
        "UPLOAD_DIR": str(TEMP_DIR / "uploads"),
        "BACKUP_DIR": str(TEMP_DIR / "backups"),
        "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "LEXWARE_AUTO_SYNC_ENABLED": "0",
        "GOOGLE_ADS_AUTO_SYNC_ENABLED": "0",
        "OPENAI_API_KEY": "",
        "FLASK_SECRET_KEY": "import-package-guard-test-secret",
        "ADMIN_PASS": "import-package-guard-test-pass",
        "PUBLIC_SITE_ONLY": "0",
        "PUBLIC_SITE_INDEXABLE": "0",
    }
)

import app as portal  # noqa: E402


def archive_with(entries):
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in entries:
            archive.writestr(name, content)
    payload.seek(0)
    return zipfile.ZipFile(payload), payload


def expect_rejected(label, entries, expected_text):
    archive, payload = archive_with(entries)
    try:
        try:
            portal.validate_import_package_archive(archive)
        except ValueError as exc:
            passed = expected_text.lower() in str(exc).lower()
            detail = str(exc)
        else:
            passed = False
            detail = "kein ValueError ausgelöst"
    finally:
        archive.close()
        payload.close()
    print(f"[{'OK' if passed else 'FEHLER'}] {label}" + (f" – {detail}" if detail else ""))
    return passed


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    checks = []

    backup_path = portal.create_backup_package("import-package-guard-test")
    with zipfile.ZipFile(backup_path) as archive:
        names, stats = portal.validate_import_package_archive(archive)
    checks.append(
        (
            "backup.json" in names
            and "manifest.json" in names
            and stats["member_count"] >= 2
            and stats["uncompressed_bytes"] > 0
        )
    )
    print(f"[{'OK' if checks[-1] else 'FEHLER'}] Eigenes Backup bleibt importierbar")

    original_limit = portal.IMPORT_PACKAGE_MAX_UNCOMPRESSED_MB
    try:
        portal.IMPORT_PACKAGE_MAX_UNCOMPRESSED_MB = 1
        checks.append(
            expect_rejected(
                "Komprimierte Grossdaten werden vor dem Entpacken begrenzt",
                [("uploads/zu-gross.txt", b"0" * (2 * 1024 * 1024))],
                "nach dem Entpacken zu groß",
            )
        )
    finally:
        portal.IMPORT_PACKAGE_MAX_UNCOMPRESSED_MB = original_limit

    checks.append(
        expect_rejected(
            "Doppelte Dateinamen werden abgewiesen",
            [("uploads/doppelt.txt", b"eins"), ("uploads/doppelt.txt", b"zwei")],
            "doppelte Dateinamen",
        )
    )
    checks.append(
        expect_rejected(
            "Unsichere Pfade werden abgewiesen",
            [("../ausserhalb.txt", b"nein")],
            "unzulässigen Dateipfad",
        )
    )
    checks.append(
        expect_rejected(
            "Verschachtelte Upload-Pfade werden abgewiesen",
            [("uploads/unterordner/foto.jpg", b"nein")],
            "unzulässigen Upload-Pfad",
        )
    )

    failed = len([check for check in checks if not check])
    print(f"== ERGEBNIS: {len(checks) - failed}/{len(checks)} Checks bestanden ==")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        import shutil

        shutil.rmtree(TEMP_DIR, ignore_errors=True)
