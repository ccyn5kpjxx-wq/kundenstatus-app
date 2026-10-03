import base64
import hashlib
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from scripts import render_upload_blob_audit as audit


class FakeDatabase:
    def __init__(self, reference_rows=None, backup_rows=None, on_backup_rows=None):
        self.reference_rows = reference_rows or {}
        self.backup_results = backup_rows or {}
        self.on_backup_rows = on_backup_rows
        self.reference_columns_calls = 0
        self.reference_values_calls = []
        self.backup_row_calls = []

    def reference_columns(self):
        self.reference_columns_calls += 1
        return [
            {
                "table_name": table,
                "column_name": column,
                "has_id": has_id,
            }
            for table, column, has_id in self.reference_rows
        ]

    def reference_values(self, table, column, has_id):
        self.reference_values_calls.append((table, column, has_id))
        return self.reference_rows[(table, column, has_id)]

    def backup_rows(self, datei_id):
        self.backup_row_calls.append(datei_id)
        rows = self.backup_results.get(datei_id, [])
        if self.on_backup_rows:
            self.on_backup_rows(datei_id, len(self.backup_row_calls))
        return rows


def backup_row(
    name,
    disk_data,
    *,
    blob_data=None,
    encoded=None,
    stored_hash=None,
    datei_size=None,
    backup_size=None,
    backup_id=1,
):
    blob_data = disk_data if blob_data is None else blob_data
    if encoded is None:
        encoded = base64.b64encode(blob_data).decode("ascii")
    return {
        "stored_name": name,
        "datei_size": len(disk_data) if datei_size is None else datei_size,
        "backup_id": backup_id,
        "backup_size": len(disk_data) if backup_size is None else backup_size,
        "file_sha256": (
            hashlib.sha256(blob_data).hexdigest()
            if stored_hash is None
            else stored_hash
        ),
        "file_base64": encoded,
    }


class RenderUploadBlobAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp.name)
        self.root = self.temp_path / "uploads"
        self.root.mkdir()
        self.audit_root = self.temp_path / "audit"
        self.original_audit_root = audit.AUDIT_ROOT
        audit.AUDIT_ROOT = self.audit_root

    def tearDown(self):
        audit.AUDIT_ROOT = self.original_audit_root
        self.temp.cleanup()

    @staticmethod
    def candidate_index_entry(name, data, *datei_ids):
        return [
            name,
            len(data),
            hashlib.sha256(data).hexdigest(),
            [["dateien", "stored_name", datei_id] for datei_id in datei_ids],
        ]

    @staticmethod
    def args(inventory, coverage, candidate_index, **overrides):
        values = {
            "expected_files": inventory["file_count"],
            "expected_bytes": inventory["total_file_bytes"],
            "expected_inventory_sha256": inventory["inventory_sha256"],
            "master_manifest_sha256": "a" * 64,
            "verification_report_sha256": "b" * 64,
            "expected_coverage_sha256": audit.canonical_sha256(sorted(coverage)),
            "expected_candidates": len(candidate_index),
            "expected_candidate_bytes": sum(item[1] for item in candidate_index),
            "expected_candidate_blobs": sum(len(item[3]) for item in candidate_index),
            "expected_candidate_index_sha256": audit.canonical_sha256(
                candidate_index
            ),
        }
        values.update(overrides)
        return types.SimpleNamespace(**values)

    def test_audit_decodes_and_rehashes_real_blobs_but_protects_bad_cases(self):
        files = {
            "bad-base64.bin": b"disk bytes for malformed base64",
            "bad-hash.bin": b"disk bytes whose backup hash is wrong",
            "bad-size.bin": b"disk bytes whose size metadata is wrong",
            "extra-reference.bin": b"also referenced outside dateien",
            "good.bin": b"verified database copy",
            "multiple-dateien.bin": b"two dateien rows must both be checked",
        }
        for name, data in files.items():
            (self.root / name).write_bytes(data)

        datei_rows = [
            {"id": 1, "stored_value": "bad-base64.bin"},
            {"id": 2, "stored_value": "bad-hash.bin"},
            {"id": 3, "stored_value": "bad-size.bin"},
            {"id": 4, "stored_value": "extra-reference.bin"},
            {"id": 5, "stored_value": "good.bin"},
            {"id": 6, "stored_value": "multiple-dateien.bin"},
            {"id": 7, "stored_value": "multiple-dateien.bin"},
        ]
        coverage = ["dateien.stored_name", "lead_dateien.stored_name"]
        database = FakeDatabase(
            reference_rows={
                ("dateien", "stored_name", True): datei_rows,
                ("lead_dateien", "stored_name", True): [
                    {"id": 91, "stored_value": "extra-reference.bin"}
                ],
            },
            backup_rows={
                1: [
                    backup_row(
                        "bad-base64.bin",
                        files["bad-base64.bin"],
                        encoded="%%% definitely not base64 %%%",
                        backup_id=1,
                    )
                ],
                2: [
                    backup_row(
                        "bad-hash.bin",
                        files["bad-hash.bin"],
                        stored_hash="0" * 64,
                        backup_id=2,
                    )
                ],
                3: [
                    backup_row(
                        "bad-size.bin",
                        files["bad-size.bin"],
                        backup_size=len(files["bad-size.bin"]) + 1,
                        backup_id=3,
                    )
                ],
                4: [
                    backup_row(
                        "extra-reference.bin",
                        files["extra-reference.bin"],
                        backup_id=4,
                    )
                ],
                5: [backup_row("good.bin", files["good.bin"], backup_id=5)],
                6: [
                    backup_row(
                        "multiple-dateien.bin",
                        files["multiple-dateien.bin"],
                        encoded="invalid base64 for first row",
                        backup_id=6,
                    )
                ],
                7: [
                    backup_row(
                        "multiple-dateien.bin",
                        files["multiple-dateien.bin"],
                        backup_id=7,
                    )
                ],
            },
        )
        inventory = audit.scan_inventory(self.root)
        candidate_index = [
            self.candidate_index_entry("good.bin", files["good.bin"], 5)
        ]

        report, report_path = audit.run_audit(
            database,
            self.root,
            self.args(inventory, coverage, candidate_index),
        )

        self.assertTrue(report_path.is_file())
        self.assertEqual(report["candidate_count"], 1)
        self.assertEqual(report["candidate_bytes"], len(files["good.bin"]))
        self.assertEqual(report["candidate_blob_count"], 1)
        self.assertEqual(report["candidates"][0]["relative_path"], "good.bin")
        self.assertEqual(
            report["protected_reason_counts"],
            {
                "blob_verification_failed": 4,
                "non_dateien_reference": 1,
            },
        )
        self.assertEqual(
            report["blob_failure_reason_counts"],
            {
                "backup_base64_invalid": 2,
                "backup_metadata_hash_mismatch": 1,
                "size_metadata_mismatch": 1,
            },
        )
        decoded_bytes = sum(
            len(files[name])
            for name in (
                "bad-hash.bin",
                "bad-size.bin",
                "extra-reference.bin",
                "good.bin",
                "multiple-dateien.bin",
            )
        )
        self.assertEqual(
            report["physical_datei_blob_stats"],
            {
                "rows_examined": 7,
                "decode_attempts": 7,
                "decoded": 5,
                "decoded_bytes": decoded_bytes,
                "rehashed": 5,
                "rehashed_bytes": decoded_bytes,
                "fully_verified": 3,
                "fully_verified_bytes": (
                    len(files["extra-reference.bin"])
                    + len(files["good.bin"])
                    + len(files["multiple-dateien.bin"])
                ),
            },
        )
        # The valid second row is checked even though the first row for the same
        # physical file contains malformed Base64.
        self.assertEqual(database.backup_row_calls, [1, 2, 3, 4, 5, 6, 7])
        self.assertTrue(report["candidate_baseline_matches"])
        self.assertTrue(report["reference_columns_coverage_matches"])
        self.assertTrue(report["inventory_rechecked_unchanged"])
        self.assertTrue(report["deletion_evidence_complete"])
        self.assertEqual(
            report["candidate_index_sha256"], audit.canonical_sha256(candidate_index)
        )
        self.assertEqual(report["database_writes"], 0)
        self.assertEqual(report["server_files_deleted"], 0)
        self.assertFalse(report["delete_approved"])
        hash_input = dict(report)
        stored_audit_hash = hash_input.pop("audit_sha256")
        self.assertEqual(stored_audit_hash, audit.canonical_sha256(hash_input))

    def test_inventory_mismatch_aborts_before_any_database_lookup(self):
        (self.root / "one.bin").write_bytes(b"one")
        inventory = audit.scan_inventory(self.root)
        database = FakeDatabase(
            reference_rows={
                ("dateien", "stored_name", True): [
                    {"id": 1, "stored_value": "one.bin"}
                ]
            },
            backup_rows={1: [backup_row("one.bin", b"one")]},
        )
        candidate_index = [self.candidate_index_entry("one.bin", b"one", 1)]
        args = self.args(
            inventory,
            ["dateien.stored_name"],
            candidate_index,
            expected_bytes=inventory["total_file_bytes"] + 1,
        )

        with self.assertRaisesRegex(audit.AuditError, "Live-Inventar"):
            audit.run_audit(database, self.root, args)

        self.assertEqual(database.reference_columns_calls, 0)
        self.assertEqual(database.reference_values_calls, [])
        self.assertEqual(database.backup_row_calls, [])
        self.assertFalse(self.audit_root.exists())

    def test_second_inventory_scan_aborts_if_a_file_changes_during_blob_audit(self):
        original = b"unchanged at the first scan"
        changed = b"changed while database blobs are checked"
        path = self.root / "one.bin"
        path.write_bytes(original)
        inventory = audit.scan_inventory(self.root)

        def mutate_upload(_datei_id, call_count):
            if call_count == 1:
                path.write_bytes(changed)

        database = FakeDatabase(
            reference_rows={
                ("dateien", "stored_name", True): [
                    {"id": 1, "stored_value": "one.bin"}
                ]
            },
            backup_rows={1: [backup_row("one.bin", original)]},
            on_backup_rows=mutate_upload,
        )
        args = self.args(
            inventory,
            ["dateien.stored_name"],
            [self.candidate_index_entry("one.bin", original, 1)],
        )

        with self.assertRaisesRegex(audit.AuditError, "waehrend des Audits"):
            audit.run_audit(database, self.root, args)

        self.assertEqual(database.backup_row_calls, [1])
        self.assertFalse(self.audit_root.exists())

    def test_wrong_baseline_or_coverage_fingerprint_cannot_complete_evidence(self):
        data = b"valid redundant blob"
        (self.root / "one.bin").write_bytes(data)
        inventory = audit.scan_inventory(self.root)
        reference_rows = {
            ("dateien", "stored_name", True): [
                {"id": 1, "stored_value": "one.bin"}
            ]
        }
        candidate_index = [self.candidate_index_entry("one.bin", data, 1)]

        cases = (
            {
                "expected_coverage_sha256": "c" * 64,
                "coverage_matches": False,
                "baseline_matches": True,
            },
            {
                "expected_candidate_index_sha256": "d" * 64,
                "coverage_matches": True,
                "baseline_matches": False,
            },
        )
        for case in cases:
            expected = dict(case)
            coverage_matches = expected.pop("coverage_matches")
            baseline_matches = expected.pop("baseline_matches")
            database = FakeDatabase(
                reference_rows=reference_rows,
                backup_rows={1: [backup_row("one.bin", data)]},
            )
            args = self.args(
                inventory,
                ["dateien.stored_name"],
                candidate_index,
                **expected,
            )

            with self.subTest(case=case):
                report, _ = audit.run_audit(database, self.root, args)
                self.assertIs(
                    report["reference_columns_coverage_matches"], coverage_matches
                )
                self.assertIs(
                    report["candidate_baseline_matches"], baseline_matches
                )
                self.assertFalse(report["deletion_evidence_complete"])

    def test_import_does_not_import_flask_app_or_run_database_code(self):
        marker = self.temp_path / "app-imported.txt"
        trap = self.temp_path / "app.py"
        trap.write_text(
            "import os\n"
            "from pathlib import Path\n"
            "Path(os.environ['AUDIT_IMPORT_MARKER']).write_text('imported')\n",
            encoding="utf-8",
        )
        repository_root = Path(__file__).resolve().parent.parent
        code = (
            "import sys; "
            f"sys.path.insert(0, {str(repository_root)!r}); "
            f"sys.path.insert(0, {str(self.temp_path)!r}); "
            "import scripts.render_upload_blob_audit"
        )
        environment = os.environ.copy()
        environment["AUDIT_IMPORT_MARKER"] = str(marker)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"

        completed = subprocess.run(
            [sys.executable, "-c", code],
            cwd=self.temp_path,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertFalse(marker.exists(), "Import hat unerwartet app.py geladen.")
        self.assertEqual(completed.stdout, "")
        self.assertEqual(completed.stderr, "")

    def test_postgres_connection_enforces_read_only_session(self):
        connection_arguments = {}

        class FakeResult:
            def __init__(self, row=None):
                self.row = row

            def fetchone(self):
                return self.row

        class FakeConnection:
            def __init__(self):
                self.closed = False
                self.rollback_called = False
                self.statements = []
                self.events = []
                self._isolation_level = None
                self._read_only = False

            @property
            def isolation_level(inner_self):
                return inner_self._isolation_level

            @isolation_level.setter
            def isolation_level(inner_self, value):
                inner_self._isolation_level = value
                inner_self.events.append(("isolation_level", value))

            @property
            def read_only(inner_self):
                return inner_self._read_only

            @read_only.setter
            def read_only(inner_self, value):
                inner_self._read_only = value
                inner_self.events.append(("read_only", value))

            def execute(inner_self, statement):
                inner_self.statements.append(statement)
                inner_self.events.append(("execute", statement))
                if statement == "SHOW transaction_read_only":
                    return FakeResult({"transaction_read_only": "on"})
                if statement == "SHOW transaction_isolation":
                    return FakeResult({"transaction_isolation": "repeatable read"})
                return FakeResult()

            def rollback(inner_self):
                inner_self.rollback_called = True

            def close(inner_self):
                inner_self.closed = True

        connection = FakeConnection()
        psycopg_module = types.ModuleType("psycopg")
        psycopg_module.sql = object()

        class FakeIsolationLevel:
            REPEATABLE_READ = object()

        psycopg_module.IsolationLevel = FakeIsolationLevel

        def connect(database_url, **kwargs):
            connection_arguments["database_url"] = database_url
            connection_arguments.update(kwargs)
            return connection

        psycopg_module.connect = connect
        rows_module = types.ModuleType("psycopg.rows")
        rows_module.dict_row = object()

        with mock.patch.dict(
            sys.modules,
            {"psycopg": psycopg_module, "psycopg.rows": rows_module},
        ):
            database = audit.ReadOnlyPostgres("postgresql://example.invalid/audit")

        self.assertIs(database.connection, connection)
        self.assertEqual(
            connection_arguments,
            {
                "database_url": "postgresql://example.invalid/audit",
                "autocommit": False,
                "options": "-c default_transaction_read_only=on",
                "row_factory": rows_module.dict_row,
            },
        )
        self.assertEqual(
            connection.statements,
            [
                "SHOW transaction_read_only",
                "SHOW transaction_isolation",
            ],
        )
        self.assertEqual(
            connection.events,
            [
                ("isolation_level", FakeIsolationLevel.REPEATABLE_READ),
                ("read_only", True),
                ("execute", "SHOW transaction_read_only"),
                ("execute", "SHOW transaction_isolation"),
            ],
        )
        self.assertIs(
            connection.isolation_level, FakeIsolationLevel.REPEATABLE_READ
        )
        self.assertTrue(connection.read_only)
        database.close()
        self.assertTrue(connection.rollback_called)
        self.assertTrue(connection.closed)


if __name__ == "__main__":
    unittest.main()
