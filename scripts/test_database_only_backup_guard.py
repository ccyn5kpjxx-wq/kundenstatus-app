# -*- coding: utf-8 -*-
"""Regression tests for DB-only original protection during backup/import."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import pathlib
import shutil
import sqlite3
import sys
import tempfile
import zipfile
from unittest.mock import MagicMock, patch


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="database_only_backup_guard_"))
os.environ.update(
    {
        "RENDER": "local-database-only-backup-guard-test",
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
        "FLASK_SECRET_KEY": "database-only-backup-guard-secret",
        "ADMIN_PASS": "database-only-backup-guard-pass",
        "PUBLIC_SITE_ONLY": "0",
        "PUBLIC_SITE_INDEXABLE": "0",
    }
)

import app as portal  # noqa: E402


def report(checks, label, passed):
    checks.append(bool(passed))
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")


def insert_database_only_original(stored_name, raw):
    connection = sqlite3.connect(portal.DB)
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        datei_id = int(
            connection.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM dateien").fetchone()[0]
        )
        connection.execute(
            """
            INSERT INTO dateien
            (id, auftrag_id, original_name, stored_name, mime_type, size,
             quelle, dokument_typ, hochgeladen_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datei_id,
                99999999,
                "synthetic-original.bin",
                stored_name,
                "application/octet-stream",
                len(raw),
                "test",
                "",
                "2026-10-03 12:00:00",
            ),
        )
        connection.execute(
            """
            INSERT INTO datei_backups
            (datei_id, file_base64, file_sha256, size, erstellt_am)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                datei_id,
                base64.b64encode(raw).decode("ascii"),
                hashlib.sha256(raw).hexdigest(),
                len(raw),
                "2026-10-03 12:00:00",
            ),
        )
        connection.commit()
        return datei_id
    finally:
        connection.close()


def rows_for(datei_id):
    connection = sqlite3.connect(portal.DB)
    connection.row_factory = sqlite3.Row
    try:
        return {
            "datei": dict(
                connection.execute("SELECT * FROM dateien WHERE id=?", (datei_id,)).fetchone()
            ),
            "backup": dict(
                connection.execute(
                    "SELECT * FROM datei_backups WHERE datei_id=?", (datei_id,)
                ).fetchone()
            ),
        }
    finally:
        connection.close()


def import_archive(admin, payload):
    with admin.session_transaction() as session:
        session[portal.CSRF_FIELD_NAME] = "database-only-backup-guard-csrf"
    response = admin.post(
        "/admin/daten-import",
        data={
            portal.CSRF_FIELD_NAME: "database-only-backup-guard-csrf",
            "datenpaket": (io.BytesIO(payload), "backup-v4.zip"),
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    with admin.session_transaction() as session:
        flashes = [message for _, message in session.get("_flashes", [])]
        session.pop("_flashes", None)
    return response, flashes


def rewrite_package(
    payload,
    *,
    manifest_values=None,
    export_values=None,
    replacement_dateien=None,
    replacement_members=None,
    drop_names=(),
):
    source = io.BytesIO(payload)
    target = io.BytesIO()
    dropped = set(drop_names)
    with zipfile.ZipFile(source) as archive, zipfile.ZipFile(
        target, "w", zipfile.ZIP_DEFLATED
    ) as rewritten:
        for info in archive.infolist():
            if info.filename in dropped:
                continue
            raw = (replacement_members or {}).get(
                info.filename, archive.read(info.filename)
            )
            if info.filename == "manifest.json":
                manifest = json.loads(raw.decode("utf-8"))
                manifest.update(manifest_values or {})
                raw = json.dumps(manifest, ensure_ascii=False).encode("utf-8")
            elif info.filename == "backup.json":
                export = json.loads(raw.decode("utf-8"))
                export.update(export_values or {})
                if replacement_dateien is not None:
                    export["tables"]["dateien"] = replacement_dateien
                raw = json.dumps(export, ensure_ascii=False).encode("utf-8")
            rewritten.writestr(info, raw)
    return target.getvalue()


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    portal.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    checks = []

    clean_backup = portal.create_backup_package("database-only-guard-clean")
    clean_archive_bytes = clean_backup.read_bytes()

    raw = b"synthetic database-only original"
    stored_name = "synthetic-database-only-original.bin"
    datei_id = insert_database_only_original(stored_name, raw)
    before = rows_for(datei_id)

    summary = portal.database_only_datei_backup_summary()
    report(
        checks,
        "Fehlendes Original mit DB-Kopie wird aggregiert erkannt",
        summary == {"count": 1, "bytes": len(raw)},
    )
    try:
        portal.ensure_no_database_only_originals_for_import()
    except ValueError as exc:
        blocked = "nur in der Datenbank-Sicherung" in str(exc) and stored_name not in str(exc)
    else:
        blocked = False
    report(checks, "Import-Guard sperrt ohne Dateinamen-Leak", blocked)

    postgres_coverage = portal.database_only_backup_coverage(
        summary, includes_database_snapshot=False
    )
    sqlite_coverage = portal.database_only_backup_coverage(
        summary, includes_database_snapshot=True
    )
    report(
        checks,
        "Postgres-ZIP wird als nicht eigenständig vollständig markiert",
        postgres_coverage["database_only_datei_backups_excluded_count"] == 1
        and postgres_coverage["database_only_datei_backups_excluded_bytes"] == len(raw)
        and postgres_coverage["standalone_restore_contains_all_originals"] is False
        and bool(postgres_coverage["warnings"]),
    )
    report(
        checks,
        "SQLite-Snapshot bleibt als vollständige Wiederherstellung markiert",
        sqlite_coverage["database_only_datei_backups_excluded_count"] == 0
        and sqlite_coverage["standalone_restore_contains_all_originals"] is True
        and sqlite_coverage["warnings"] == [],
    )

    fake_lock_connection = MagicMock()
    fake_lock_connection.execute.return_value.fetchone.return_value = {}
    with patch.object(portal, "USE_POSTGRES", True), patch.object(
        portal, "open_fresh_db", return_value=fake_lock_connection
    ):
        with portal.portal_originals_operation_lock():
            with portal.portal_originals_operation_lock():
                pass
    lock_sql = [call.args[0] for call in fake_lock_connection.execute.call_args_list]
    report(
        checks,
        "Postgres-Sperre ist reentrant und nutzt denselben Advisory-Key zum Sperren und Freigeben",
        len(lock_sql) == 2
        and "pg_advisory_lock" in lock_sql[0]
        and "pg_advisory_unlock" in lock_sql[1]
        and fake_lock_connection.execute.call_args_list[0].args[1]
        == (portal.PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,)
        and fake_lock_connection.execute.call_args_list[1].args[1]
        == (portal.PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,)
        and fake_lock_connection.close.call_count == 1,
    )

    with zipfile.ZipFile(io.BytesIO(clean_archive_bytes)) as archive:
        names = set(archive.namelist())
        export = json.loads(archive.read("backup.json"))
        with tempfile.TemporaryDirectory() as extracted:
            imported_db, _uploads, _backup_export = portal.extract_import_package_files(
                archive, names, pathlib.Path(extracted)
            )
            try:
                portal.import_sqlite_rows_into_current_database(imported_db)
            except ValueError as exc:
                sqlite_import_blocked = "nur in der Datenbank-Sicherung" in str(exc)
            else:
                sqlite_import_blocked = False
        try:
            portal.import_backup_json_rows_into_current_database(export, archive, names)
        except ValueError as exc:
            json_import_blocked = "nur in der Datenbank-Sicherung" in str(exc)
        else:
            json_import_blocked = False
    report(
        checks,
        "Beide destruktiven Zeilenimporte prüfen den Guard selbst",
        sqlite_import_blocked and json_import_blocked and rows_for(datei_id) == before,
    )

    admin = portal.app.test_client()
    with admin.session_transaction() as session:
        session["admin"] = True
    with patch.object(portal, "create_backup_package") as safety_backup, patch.object(
        portal, "import_backup_json_rows_into_current_database"
    ) as json_importer, patch.object(
        portal, "import_sqlite_rows_into_current_database"
    ) as sqlite_importer, patch.object(
        portal, "copy_sqlite_database_snapshot"
    ) as sqlite_replacer, patch.object(
        portal, "replace_uploads_from_import"
    ) as upload_replacer, patch.object(portal, "init_db") as initializer:
        response, messages = import_archive(admin, clean_archive_bytes)
    report(
        checks,
        "Admin-Import stoppt vor Sicherheitsbackup und jeder Mutation",
        response.status_code == 302
        and not safety_backup.called
        and not json_importer.called
        and not sqlite_importer.called
        and not sqlite_replacer.called
        and not upload_replacer.called
        and not initializer.called
        and rows_for(datei_id) == before
        and any("nur in der Datenbank-Sicherung" in message for message in messages),
    )

    incomplete_values = {
        "database_only_datei_backups_excluded_count": 1,
        "database_only_datei_backups_excluded_bytes": len(raw),
        "standalone_restore_contains_all_originals": False,
        "warnings": ["synthetic"],
    }
    incomplete_payload = rewrite_package(
        clean_archive_bytes,
        manifest_values=incomplete_values,
        export_values=incomplete_values,
    )
    with zipfile.ZipFile(io.BytesIO(incomplete_payload)) as archive:
        names, _stats = portal.validate_import_package_archive(archive)
        try:
            with tempfile.TemporaryDirectory() as extracted:
                portal.extract_import_package_files(
                    archive, names, pathlib.Path(extracted)
                )
        except ValueError as exc:
            incomplete_blocked = "DB-only Originaldateien fehlen" in str(exc)
        else:
            incomplete_blocked = False
    report(
        checks,
        "Als unvollständig markiertes ZIP wird auch auf einem anderen System abgewiesen",
        incomplete_blocked,
    )

    contradictory_payload = rewrite_package(
        clean_archive_bytes,
        manifest_values=incomplete_values,
    )
    with zipfile.ZipFile(io.BytesIO(contradictory_payload)) as archive:
        names, _stats = portal.validate_import_package_archive(archive)
        try:
            with tempfile.TemporaryDirectory() as extracted:
                portal.extract_import_package_files(
                    archive, names, pathlib.Path(extracted)
                )
        except ValueError as exc:
            contradiction_blocked = "widersprechen" in str(exc)
        else:
            contradiction_blocked = False
    report(
        checks,
        "Widerspruch zwischen Manifest und backup.json wird abgewiesen",
        contradiction_blocked,
    )

    inferred_payload = rewrite_package(
        clean_archive_bytes,
        replacement_dateien=[{"id": 1, "stored_name": "missing-original.bin"}],
        drop_names={"auftraege.db"},
    )
    with zipfile.ZipFile(io.BytesIO(inferred_payload)) as archive:
        names, _stats = portal.validate_import_package_archive(archive)
        try:
            with tempfile.TemporaryDirectory() as extracted:
                portal.extract_import_package_files(
                    archive, names, pathlib.Path(extracted)
                )
        except ValueError as exc:
            inferred_blocked = "Datei-Originale fehlen" in str(exc)
        else:
            inferred_blocked = False
    report(
        checks,
        "JSON-only-Paket wird unabhängig von optionalen Metadaten gegen ZIP-Uploads geprüft",
        inferred_blocked,
    )

    tampered_db_path = TEMP_DIR / "tampered-import.db"
    tampered = sqlite3.connect(tampered_db_path)
    try:
        tampered.executescript(
            """
            CREATE TABLE auftraege (id INTEGER PRIMARY KEY);
            CREATE TABLE autohaeuser (id INTEGER PRIMARY KEY);
            CREATE TABLE dateien (id INTEGER PRIMARY KEY, stored_name TEXT, size INTEGER);
            INSERT INTO dateien (id, stored_name, size)
            VALUES (1, 'missing-from-sqlite-package.bin', 5);
            """
        )
        tampered.commit()
    finally:
        tampered.close()
    sqlite_bypass_payload = rewrite_package(
        clean_archive_bytes,
        replacement_members={"auftraege.db": tampered_db_path.read_bytes()},
    )
    with zipfile.ZipFile(io.BytesIO(sqlite_bypass_payload)) as archive:
        names, _stats = portal.validate_import_package_archive(archive)
        try:
            with tempfile.TemporaryDirectory() as extracted:
                portal.extract_import_package_files(
                    archive, names, pathlib.Path(extracted)
                )
        except ValueError as exc:
            sqlite_bypass_blocked = "Datenbankkopie" in str(exc)
        else:
            sqlite_bypass_blocked = False
    report(
        checks,
        "Beigefügte SQLite-Datei umgeht die Originalabdeckung nicht",
        sqlite_bypass_blocked,
    )

    upload_path = portal.UPLOAD_DIR / stored_name
    upload_path.write_bytes(raw)
    report(
        checks,
        "Physisch vorhandene reguläre Datei löst den Guard nicht aus",
        portal.database_only_datei_backup_summary() == {"count": 0, "bytes": 0},
    )
    upload_path.unlink()
    upload_path.mkdir()
    report(
        checks,
        "Verzeichnis statt Upload-Datei bleibt geschützt",
        portal.database_only_datei_backup_summary() == {"count": 1, "bytes": len(raw)},
    )
    upload_path.rmdir()

    connection = sqlite3.connect(portal.DB)
    try:
        connection.execute("DELETE FROM datei_backups WHERE datei_id=?", (datei_id,))
        connection.commit()
    finally:
        connection.close()
    report(
        checks,
        "Fehlende Datei ohne DB-Original wird von diesem speziellen Guard nicht gezählt",
        portal.database_only_datei_backup_summary() == {"count": 0, "bytes": 0},
    )

    with patch.object(
        portal,
        "ensure_no_database_only_originals_for_import",
        side_effect=[None, ValueError("synthetic second guard")],
    ) as guard, patch.object(portal, "create_backup_package") as safety_backup, patch.object(
        portal, "copy_sqlite_database_snapshot"
    ) as sqlite_replacer, patch.object(
        portal, "replace_uploads_from_import"
    ) as upload_replacer:
        response, messages = import_archive(admin, clean_archive_bytes)
    report(
        checks,
        "Zweite Prüfung schließt das Zeitfenster während des Sicherheitsbackups",
        response.status_code == 302
        and guard.call_count == 2
        and safety_backup.call_count == 1
        and not sqlite_replacer.called
        and not upload_replacer.called
        and any("synthetic second guard" in message for message in messages),
    )

    failed = sum(not check for check in checks)
    print(f"== ERGEBNIS: {len(checks) - failed}/{len(checks)} Checks bestanden ==")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
