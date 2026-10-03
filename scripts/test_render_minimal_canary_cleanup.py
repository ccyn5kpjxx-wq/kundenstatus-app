from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import pathlib
import stat
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import render_minimal_canary_cleanup as canary  # noqa: E402


def fake_candidate(mtime_ns: int = 1) -> dict:
    return {
        "relative_path": canary.BOOTSTRAP_CANDIDATE_PATH,
        "size": canary.BOOTSTRAP_CANDIDATE_SIZE,
        "mtime_ns": mtime_ns,
        "sha256": canary.BOOTSTRAP_CANDIDATE_SHA256,
        "references": [dict(canary.BOOTSTRAP_CANDIDATE_REFERENCE)],
    }


class FakeDatabase:
    def __init__(self):
        self.lock_trace = ["advisory", "tables"]
        self.database_writes = 0
        self.closed = False

    def acquire(self):
        return None

    def close(self):
        self.closed = True


class MinimalCanaryTests(unittest.TestCase):
    def test_fixed_scope_constants(self):
        self.assertEqual(
            canary.BOOTSTRAP_CANDIDATE_PATH,
            "375b778a6df149ad963b1e42bb8de204.pdf",
        )
        self.assertEqual(canary.EXPECTED_DATEI_ID, 1101)
        self.assertEqual(canary.BOOTSTRAP_CANDIDATE_SIZE, 15_281_466)
        self.assertEqual(
            canary.BOOTSTRAP_CANDIDATE_SHA256,
            "3e8bb872dd8845b0ffe8981912242b2943117ffa7dfcbbcc72d8cd274e2af594",
        )
        self.assertEqual(
            canary.BOOTSTRAP_LOCAL_EVIDENCE["verification_report_sha256"],
            "840e550058a6ab46b4afffad5f858dd937a7052994cdc89f1f94b5b20992119a",
        )

    def test_main_refuses_execute_without_exact_approval_and_plan(self):
        base = ["--audit-path", "x", "--expected-audit-sha256", "0" * 64]
        with self.assertRaises(canary.CanaryError):
            canary.main(base + ["--execute"])
        with self.assertRaises(canary.CanaryError):
            canary.main(
                base
                + [
                    "--execute",
                    "--approval",
                    "wrong",
                    "--run-id",
                    "a" * 32,
                    "--expected-plan-sha256",
                    "b" * 64,
                ]
            )

    def test_prepare_rejects_execute_arguments(self):
        with self.assertRaises(canary.CanaryError):
            canary.main(
                [
                    "--audit-path",
                    "x",
                    "--expected-audit-sha256",
                    "0" * 64,
                    "--approval",
                    canary.APPROVAL_PHRASE,
                ]
            )

    def test_plan_is_hash_bound_fresh_and_one_shot(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            run_id = "a" * 32
            run_dir = root / f"run-{run_id}"
            run_dir.mkdir()
            plan = {
                "format": canary.PLAN_FORMAT,
                "run_id": run_id,
                "created_at": canary.utc_now(),
                "value": 1,
            }
            plan["plan_sha256"] = canary.canonical_sha256(plan)
            (run_dir / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
            with mock.patch.object(canary, "PLAN_ROOT", root):
                loaded = canary.read_and_claim_plan(run_id, plan["plan_sha256"])
                self.assertEqual(loaded, plan)
                self.assertFalse((run_dir / "plan.json").exists())
                self.assertTrue((run_dir / "claimed.json").exists())
                with self.assertRaises(canary.CanaryError):
                    canary.read_and_claim_plan(run_id, plan["plan_sha256"])

    def test_plan_rejects_expired_and_self_rehashed_wrong_expected_hash(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            for index, created_at in enumerate(
                [
                    (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat(),
                    (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)).isoformat(),
                ]
            ):
                run_id = f"{index + 1:032x}"
                run_dir = root / f"run-{run_id}"
                run_dir.mkdir()
                plan = {
                    "format": canary.PLAN_FORMAT,
                    "run_id": run_id,
                    "created_at": created_at,
                }
                plan["plan_sha256"] = canary.canonical_sha256(plan)
                (run_dir / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
                with mock.patch.object(canary, "PLAN_ROOT", root):
                    with self.assertRaises(canary.CanaryError):
                        canary.read_and_claim_plan(run_id, plan["plan_sha256"])
            run_id = "f" * 32
            run_dir = root / f"run-{run_id}"
            run_dir.mkdir()
            plan = {"run_id": run_id, "created_at": canary.utc_now(), "value": 2}
            plan["plan_sha256"] = canary.canonical_sha256(plan)
            (run_dir / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
            with mock.patch.object(canary, "PLAN_ROOT", root):
                with self.assertRaises(canary.CanaryError):
                    canary.read_and_claim_plan(run_id, "0" * 64)

    def test_plan_rejects_embedded_run_id_mismatch_before_claim(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            requested_run_id = "a" * 32
            embedded_run_id = "b" * 32
            run_dir = root / f"run-{requested_run_id}"
            run_dir.mkdir()
            plan = {
                "format": canary.PLAN_FORMAT,
                "run_id": embedded_run_id,
                "created_at": canary.utc_now(),
            }
            plan["plan_sha256"] = canary.canonical_sha256(plan)
            plan_path = run_dir / "plan.json"
            plan_path.write_text(json.dumps(plan), encoding="utf-8")
            with mock.patch.object(canary, "PLAN_ROOT", root):
                with self.assertRaises(canary.CanaryError):
                    canary.read_and_claim_plan(
                        requested_run_id, plan["plan_sha256"]
                    )
            self.assertTrue(plan_path.exists())
            self.assertFalse((run_dir / "claimed.json").exists())

    def test_atomic_json_removes_complete_link_when_directory_fsync_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            target = pathlib.Path(temp) / "run" / "receipt.json"
            with mock.patch.object(
                canary,
                "fsync_directory",
                side_effect=[None, OSError("simulated directory fsync failure"), None],
            ):
                with self.assertRaises(OSError):
                    canary.atomic_json(target, {"status": "complete"})
            self.assertFalse(target.exists())

    def test_atomic_json_ignores_partial_cleanup_error_after_durable_publish(self):
        with tempfile.TemporaryDirectory() as temp:
            target = pathlib.Path(temp) / "run" / "receipt.json"
            original_unlink = pathlib.Path.unlink

            def unlink(path, *args, **kwargs):
                if path.name.startswith(".receipt.") and path.name.endswith(".part"):
                    raise OSError("simulated partial cleanup failure")
                return original_unlink(path, *args, **kwargs)

            with (
                mock.patch.object(canary, "fsync_directory"),
                mock.patch.object(pathlib.Path, "unlink", new=unlink),
            ):
                canary.atomic_json(target, {"status": "complete"})
            self.assertTrue(target.is_file())

    def test_audit_validation_arguments_are_exact(self):
        args = canary.audit_validation_args("1" * 64)
        self.assertEqual(args.expected_files, 557)
        self.assertEqual(args.expected_bytes, 953_348_048)
        self.assertEqual(args.expected_candidates, 480)
        self.assertEqual(args.expected_candidate_bytes, 879_034_032)
        self.assertEqual(args.expected_candidate_blobs, 480)
        self.assertEqual(
            args.expected_verification_report_sha256,
            canary.BOOTSTRAP_LOCAL_EVIDENCE["verification_report_sha256"],
        )

    def test_lock_order_is_advisory_then_share_tables(self):
        events = []
        columns = [
            {"table_name": "dateien", "column_name": "stored_name", "has_id": True},
            {
                "table_name": "reklamationen",
                "column_name": "datei_stored_name",
                "has_id": True,
            },
        ]

        class Result:
            def fetchone(self):
                return {"locked": True}

            def fetchall(self):
                return columns

        class Connection:
            def execute(self, query, params=None):
                events.append((str(query), params))
                return Result()

        class SQL:
            @staticmethod
            def SQL(value):
                return value

            @staticmethod
            def Identifier(value):
                return f'"{value}"'

        database = canary.LockedPostgres.__new__(canary.LockedPostgres)
        database.connection = Connection()
        database.sql = SQL()
        database.lock_trace = []
        database.database_writes = 0
        database._locked_reference_columns = None
        database.acquire()
        self.assertEqual(database.lock_trace, ["advisory", "tables"])
        self.assertIn("pg_try_advisory_xact_lock", events[0][0])
        lock_statements = [query for query, _params in events if "LOCK TABLE" in query]
        self.assertEqual(len(lock_statements), 1)
        self.assertIn('"datei_backups"', lock_statements[0])
        self.assertIn('"dateien"', lock_statements[0])
        self.assertIn('"reklamationen"', lock_statements[0])
        self.assertEqual(database.reference_columns(), columns)

    @unittest.skipUnless(os.name == "posix", "dir_fd and /proc test requires POSIX")
    def test_file_verifier_rejects_symlink_and_hardlink(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            name = "candidate.bin"
            target = root / "target.bin"
            target.write_bytes(b"exact")
            digest = hashlib.sha256(b"exact").hexdigest()
            root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with (
                    mock.patch.object(canary, "BOOTSTRAP_CANDIDATE_PATH", name),
                    mock.patch.object(canary, "BOOTSTRAP_CANDIDATE_SIZE", 5),
                    mock.patch.object(canary, "BOOTSTRAP_CANDIDATE_SHA256", digest),
                ):
                    (root / name).symlink_to(target)
                    candidate = fake_candidate(target.stat().st_mtime_ns)
                    candidate.update(relative_path=name, size=5, sha256=digest)
                    with self.assertRaises(canary.CanaryError):
                        canary.open_verified_candidate(root_fd, candidate)
                    (root / name).unlink()
                    os.link(target, root / name)
                    candidate["mtime_ns"] = target.stat().st_mtime_ns
                    with self.assertRaises(canary.CanaryError):
                        canary.open_verified_candidate(root_fd, candidate)
            finally:
                os.close(root_fd)

    @unittest.skipUnless(os.name == "posix", "dir_fd and /proc test requires POSIX")
    def test_final_path_check_rejects_inode_swap(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            name = "candidate.bin"
            path = root / name
            path.write_bytes(b"first")
            root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            held_fd = os.open(path, os.O_RDONLY)
            expected = canary.identity(os.fstat(held_fd))
            path.unlink()
            path.write_bytes(b"other")
            try:
                with mock.patch.object(canary, "BOOTSTRAP_CANDIDATE_PATH", name):
                    with self.assertRaises(canary.CanaryError):
                        canary.final_path_check(root_fd, held_fd, expected)
            finally:
                os.close(held_fd)
                os.close(root_fd)

    @unittest.skipUnless(os.name == "posix", "renameat2 staging requires Linux")
    def test_staging_moves_and_unlinks_only_verified_hidden_name(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            original_name = "a" * 32 + ".pdf"
            original = root / original_name
            original.write_bytes(b"verified")
            root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            candidate_fd = os.open(original, os.O_RDONLY)
            expected = canary.identity(os.fstat(candidate_fd))
            try:
                with mock.patch.object(
                    canary, "BOOTSTRAP_CANDIDATE_PATH", original_name
                ):
                    hidden = canary.stage_candidate(root_fd, "b" * 32)
                    self.assertFalse(original.exists())
                    self.assertTrue((root / hidden).is_file())
                    canary.staged_path_check(
                        root_fd, candidate_fd, expected, hidden
                    )
                    canary.strict_unlink_staged(
                        root_fd, candidate_fd, expected, hidden
                    )
                    self.assertFalse(original.exists())
                    self.assertFalse((root / hidden).exists())
            finally:
                os.close(candidate_fd)
                os.close(root_fd)

    @unittest.skipUnless(os.name == "posix", "renameat2 staging requires Linux")
    def test_staging_inode_swap_is_never_unlinked(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            original_name = "a" * 32 + ".pdf"
            original = root / original_name
            original.write_bytes(b"verified")
            root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            candidate_fd = os.open(original, os.O_RDONLY)
            expected = canary.identity(os.fstat(candidate_fd))
            original.unlink()
            original.write_bytes(b"replacement")
            try:
                with mock.patch.object(
                    canary, "BOOTSTRAP_CANDIDATE_PATH", original_name
                ):
                    hidden = canary.stage_candidate(root_fd, "c" * 32)
                    with self.assertRaises(canary.CanaryError):
                        canary.staged_path_check(
                            root_fd, candidate_fd, expected, hidden
                        )
                    self.assertTrue((root / hidden).is_file())
                    self.assertEqual((root / hidden).read_bytes(), b"replacement")
            finally:
                os.close(candidate_fd)
                os.close(root_fd)

    @unittest.skipUnless(os.name == "posix", "/proc fd scan requires POSIX")
    def test_foreign_fd_scan_detects_second_open_descriptor(self):
        with tempfile.NamedTemporaryFile() as source:
            own_fd = os.open(source.name, os.O_RDONLY)
            other_fd = os.open(source.name, os.O_RDONLY)
            try:
                file_identity = canary.identity(os.fstat(own_fd))
                matches = canary.foreign_open_fds(file_identity, own_fd)
                self.assertTrue(
                    any(item.endswith(f"/{other_fd}") for item in matches), matches
                )
            finally:
                os.close(other_fd)
                os.close(own_fd)

    @unittest.skipUnless(os.name == "posix", "/proc fd scan requires POSIX")
    def test_foreign_fd_scan_still_checks_readable_outside_cgroup(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            proc_root = root / "proc"
            source = root / "candidate"
            source.write_bytes(b"verified")
            (proc_root / "self").mkdir(parents=True)
            (proc_root / "self" / "cgroup").write_bytes(b"0::/service\n")

            same = proc_root / "101"
            (same / "fd").mkdir(parents=True)
            (same / "cgroup").write_bytes(b"0::/service\n")
            os.link(source, same / "fd" / "7")

            outside = proc_root / "202"
            (outside / "fd").mkdir(parents=True)
            (outside / "cgroup").write_bytes(b"0::/platform\n")
            os.link(source, outside / "fd" / "8")

            own_fd = os.open(source, os.O_RDONLY)
            try:
                file_identity = canary.identity(os.fstat(own_fd))
                with mock.patch.object(canary, "PROC_ROOT", proc_root):
                    matches = canary.foreign_open_fds(file_identity, own_fd)
                self.assertEqual(matches, ["101/7", "202/8"])
            finally:
                os.close(own_fd)

    @unittest.skipUnless(os.name == "posix", "/proc fd scan requires POSIX")
    def test_foreign_fd_scan_skips_opaque_outside_cgroup_only(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            proc_root = root / "proc"
            source = root / "candidate"
            source.write_bytes(b"verified")
            (proc_root / "self").mkdir(parents=True)
            (proc_root / "self" / "cgroup").write_bytes(b"0::/service\n")

            outside = proc_root / "202"
            (outside / "fd").mkdir(parents=True)
            (outside / "cgroup").write_bytes(b"0::/platform\n")

            real_iterdir = pathlib.Path.iterdir

            def controlled_iterdir(path):
                if path == outside / "fd":
                    raise PermissionError("opaque platform fd directory")
                return real_iterdir(path)

            own_fd = os.open(source, os.O_RDONLY)
            try:
                file_identity = canary.identity(os.fstat(own_fd))
                with (
                    mock.patch.object(canary, "PROC_ROOT", proc_root),
                    mock.patch.object(
                        pathlib.Path,
                        "iterdir",
                        autospec=True,
                        side_effect=controlled_iterdir,
                    ),
                ):
                    self.assertEqual(
                        canary.foreign_open_fds(file_identity, own_fd), []
                    )
            finally:
                os.close(own_fd)

    @unittest.skipUnless(os.name == "posix", "/proc fd scan requires POSIX")
    def test_foreign_fd_scan_fails_closed_for_opaque_same_cgroup(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            proc_root = root / "proc"
            source = root / "candidate"
            source.write_bytes(b"verified")
            (proc_root / "self").mkdir(parents=True)
            (proc_root / "self" / "cgroup").write_bytes(b"0::/service\n")

            same = proc_root / "101"
            (same / "fd").mkdir(parents=True)
            (same / "cgroup").write_bytes(b"0::/service\n")

            real_iterdir = pathlib.Path.iterdir

            def controlled_iterdir(path):
                if path == same / "fd":
                    raise PermissionError("opaque service fd directory")
                return real_iterdir(path)

            own_fd = os.open(source, os.O_RDONLY)
            try:
                file_identity = canary.identity(os.fstat(own_fd))
                with (
                    mock.patch.object(canary, "PROC_ROOT", proc_root),
                    mock.patch.object(
                        pathlib.Path,
                        "iterdir",
                        autospec=True,
                        side_effect=controlled_iterdir,
                    ),
                ):
                    with self.assertRaises(canary.CanaryError):
                        canary.foreign_open_fds(file_identity, own_fd)
            finally:
                os.close(own_fd)

    def _execute_fixture(
        self,
        *,
        foreign=None,
        foreign_calls=None,
        locked=None,
        after=None,
        atomic_side_effect=None,
    ):
        temporary = tempfile.TemporaryDirectory()
        root = pathlib.Path(temporary.name)
        path = root / canary.BOOTSTRAP_CANDIDATE_PATH
        path.write_bytes(b"placeholder")
        held_path = root / "held.placeholder"
        held_path.write_bytes(b"held")
        opened_fd = os.open(held_path, os.O_RDONLY)
        current_identity = canary.identity(os.fstat(opened_fd))
        os.close(opened_fd)
        candidate = fake_candidate()
        report = {"audit_sha256": "a" * 64, "created_at": canary.utc_now()}
        database_binding = {
            "datei_id": 1101,
            "backup_id": 7,
            "stored_name": canary.BOOTSTRAP_CANDIDATE_PATH,
            "size": canary.BOOTSTRAP_CANDIDATE_SIZE,
            "sha256": canary.BOOTSTRAP_CANDIDATE_SHA256,
            "reference_columns_sha256": "c" * 64,
        }
        plan = {
            "plan_sha256": "b" * 64,
            "created_at": canary.utc_now(),
            "pre_capacity": {"free_bytes": 0, "free_inodes": 1},
            "candidate_file_identity": current_identity,
            "database_binding": database_binding,
        }
        baseline = dict(canary.BOOTSTRAP_SOURCE_INVENTORY)
        locked_inventory = dict(baseline if locked is None else locked)
        expected_after = dict(canary.BOOTSTRAP_EXPECTED_AFTER)
        if after is not None:
            expected_after = after
        fake_database = FakeDatabase()

        def open_candidate(_root_fd, _candidate):
            return os.open(held_path, os.O_RDONLY), current_identity

        def open_root(_root):
            return os.open(held_path, os.O_RDONLY)

        hidden_holder = {"path": None}

        def stage_exact(_root_fd, _run_id):
            hidden_path = root / canary.canary_hidden_name(_run_id)
            hidden_holder["path"] = hidden_path
            path.rename(hidden_path)
            return hidden_path.name

        def unlink_staged(_root_fd, _candidate_fd, _identity, _hidden_name):
            hidden_holder["path"].unlink()

        def restore_staged(_root_fd, _candidate_fd, _identity, _hidden_name):
            hidden_holder["path"].rename(path)

        def fake_namespace(_root_fd, expected_identity, _hidden_name):
            hidden_path = hidden_holder["path"]
            return {
                "original": {
                    "exists": path.exists(),
                    "matches_expected": path.exists(),
                },
                "hidden": {
                    "exists": bool(hidden_path and hidden_path.exists()),
                    "matches_expected": bool(hidden_path and hidden_path.exists()),
                },
            }

        def fake_capacity(_root):
            hidden_path = hidden_holder["path"]
            free = (
                0
                if path.exists() or (hidden_path and hidden_path.exists())
                else 2 * canary.POST_MIN_FREE_BYTES
            )
            return {"free_bytes": free, "free_inodes": 2}

        patches = [
            mock.patch.object(canary, "validate_runtime", return_value=(root, "postgresql://test")),
            mock.patch.object(canary, "load_fresh_audit", return_value=(report, candidate)),
            mock.patch.object(canary, "read_and_claim_plan", return_value=plan),
            mock.patch.object(canary, "plan_bindings"),
            mock.patch.object(
                canary,
                "scan_inventory",
                side_effect=[baseline, locked_inventory, expected_after],
            ),
            mock.patch.object(canary, "open_verified_candidate", side_effect=open_candidate),
            mock.patch.object(canary, "open_root_fd", side_effect=open_root),
            mock.patch.object(canary, "hash_fd", return_value=canary.BOOTSTRAP_CANDIDATE_SHA256),
            mock.patch.object(canary, "stage_candidate", side_effect=stage_exact),
            mock.patch.object(canary, "staged_path_check"),
            mock.patch.object(canary, "namespace_state", side_effect=fake_namespace),
            mock.patch.object(canary, "strict_unlink_staged", side_effect=unlink_staged),
            mock.patch.object(canary, "restore_staged_candidate", side_effect=restore_staged),
            mock.patch.object(canary, "LockedPostgres", return_value=fake_database),
            mock.patch.object(canary, "verify_database", return_value=database_binding),
            mock.patch.object(
                canary,
                "foreign_open_fds",
                side_effect=foreign_calls,
                return_value=foreign or [],
            ),
            mock.patch.object(canary, "final_path_check"),
            mock.patch.object(canary, "capacity", side_effect=fake_capacity),
            mock.patch.object(canary, "RECEIPT_ROOT", root / "receipts"),
            mock.patch.object(canary.os, "fsync", return_value=None),
        ]
        if atomic_side_effect is not None:
            patches.append(mock.patch.object(canary, "atomic_json", side_effect=atomic_side_effect))
        for patcher in patches:
            patcher.start()
        self.addCleanup(lambda: [patcher.stop() for patcher in reversed(patches)])
        self.addCleanup(temporary.cleanup)
        return root, path, fake_database

    def test_execute_success_unlinks_exact_file_and_writes_receipt(self):
        root, path, database = self._execute_fixture()
        result = canary.execute(pathlib.Path("audit"), "a" * 64, "1" * 32, "b" * 64)
        self.assertFalse(path.exists())
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["deleted_count"], 1)
        self.assertEqual(result["database_writes"], 0)
        self.assertEqual(result["lock_trace"], ["advisory", "tables"])
        self.assertTrue(pathlib.Path(result["receipt_path"]).is_file())
        persisted = json.loads(
            pathlib.Path(result["receipt_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(persisted["deleted_count"], 1)
        self.assertEqual(persisted["deleted_bytes"], canary.BOOTSTRAP_CANDIDATE_SIZE)
        persisted_hash = persisted.pop("receipt_sha256")
        self.assertEqual(persisted_hash, canary.canonical_sha256(persisted))
        self.assertTrue(database.closed)

    def test_foreign_fd_aborts_before_unlink(self):
        _root, path, database = self._execute_fixture(foreign=["123/9"])
        with self.assertRaises(canary.CanaryError):
            canary.execute(pathlib.Path("audit"), "a" * 64, "2" * 32, "b" * 64)
        self.assertTrue(path.exists())
        self.assertTrue(database.closed)

    def test_foreign_fd_opened_before_staging_is_detected_and_restored(self):
        _root, path, database = self._execute_fixture(
            foreign_calls=[[], ["123/9"]]
        )
        with self.assertRaises(canary.CanaryError):
            canary.execute(pathlib.Path("audit"), "a" * 64, "8" * 32, "b" * 64)
        self.assertTrue(path.exists())
        self.assertTrue(database.closed)

    def test_locked_inventory_change_aborts_before_staging(self):
        wrong_locked = dict(canary.BOOTSTRAP_SOURCE_INVENTORY)
        wrong_locked["file_count"] += 1
        _root, path, database = self._execute_fixture(locked=wrong_locked)
        with self.assertRaises(canary.CanaryError):
            canary.execute(pathlib.Path("audit"), "a" * 64, "9" * 32, "b" * 64)
        self.assertTrue(path.exists())
        self.assertTrue(database.closed)

    def test_locked_fd_hash_change_aborts_before_staging(self):
        _root, path, database = self._execute_fixture()
        with mock.patch.object(canary, "hash_fd", return_value="0" * 64):
            with self.assertRaises(canary.CanaryError):
                canary.execute(
                    pathlib.Path("audit"), "a" * 64, "a" * 32, "b" * 64
                )
        self.assertTrue(path.exists())
        self.assertTrue(database.closed)

    def test_final_binding_error_aborts_before_unlink(self):
        _root, path, _database = self._execute_fixture()
        with mock.patch.object(
            canary, "final_path_check", side_effect=canary.CanaryError("swapped")
        ):
            with self.assertRaises(canary.CanaryError):
                canary.execute(pathlib.Path("audit"), "a" * 64, "3" * 32, "b" * 64)
        self.assertTrue(path.exists())

    def test_unlink_enoent_is_not_reported_as_success(self):
        _root, path, _database = self._execute_fixture()
        with mock.patch.object(
            canary,
            "strict_unlink_staged",
            side_effect=FileNotFoundError("missing"),
        ):
            with self.assertRaises(canary.CanaryError):
                canary.execute(pathlib.Path("audit"), "a" * 64, "4" * 32, "b" * 64)
        self.assertTrue(path.exists())

    def test_staged_binding_error_restores_without_unlink(self):
        _root, path, _database = self._execute_fixture()
        original_check = canary.staged_path_check
        calls = 0

        def fail_first_staged_check(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise canary.CanaryError("staged inode mismatch")
            return original_check(*args, **kwargs)

        with mock.patch.object(
            canary, "staged_path_check", side_effect=fail_first_staged_check
        ):
            with self.assertRaises(canary.CanaryError):
                canary.execute(
                    pathlib.Path("audit"), "a" * 64, "c" * 32, "b" * 64
                )
        self.assertTrue(path.exists())

    def test_interrupt_after_stage_syscall_is_reconstructed_and_restored(self):
        _root, path, _database = self._execute_fixture()
        stage = canary.stage_candidate.side_effect

        def stage_then_interrupt(*args, **kwargs):
            stage(*args, **kwargs)
            raise KeyboardInterrupt()

        canary.stage_candidate.side_effect = stage_then_interrupt
        with self.assertRaises(canary.CanaryError):
            canary.execute(pathlib.Path("audit"), "a" * 64, "d" * 32, "b" * 64)
        self.assertTrue(path.exists())

    def test_interrupt_after_unlink_syscall_is_reconstructed_as_partial(self):
        _root, path, _database = self._execute_fixture()
        unlink = canary.strict_unlink_staged.side_effect

        def unlink_then_interrupt(*args, **kwargs):
            unlink(*args, **kwargs)
            raise KeyboardInterrupt()

        canary.strict_unlink_staged.side_effect = unlink_then_interrupt
        with self.assertRaises(canary.CanaryRunError) as raised:
            canary.execute(pathlib.Path("audit"), "a" * 64, "e" * 32, "b" * 64)
        self.assertFalse(path.exists())
        self.assertEqual(raised.exception.details["status"], "partial_after_unlink")
        self.assertEqual(raised.exception.details["deleted_count"], 1)
        receipt = dict(raised.exception.details)
        receipt_hash = receipt.pop("receipt_sha256")
        self.assertEqual(receipt_hash, canary.canonical_sha256(receipt))

    def test_postcheck_failure_after_unlink_is_partial_and_reconstructable(self):
        wrong_after = dict(canary.BOOTSTRAP_EXPECTED_AFTER)
        wrong_after["file_count"] = 999
        _root, path, _database = self._execute_fixture(after=wrong_after)
        with self.assertRaises(canary.CanaryRunError) as raised:
            canary.execute(pathlib.Path("audit"), "a" * 64, "5" * 32, "b" * 64)
        self.assertFalse(path.exists())
        self.assertEqual(raised.exception.details["status"], "partial_after_unlink")
        self.assertTrue(raised.exception.details["path_missing"])
        self.assertEqual(raised.exception.details["post_database"]["datei_id"], 1101)
        self.assertEqual(raised.exception.details["database_writes"], 0)

    def test_receipt_failure_never_claims_success(self):
        _root, path, _database = self._execute_fixture(
            atomic_side_effect=OSError("simulated receipt ENOSPC")
        )
        with self.assertRaises(canary.CanaryRunError) as raised:
            canary.execute(pathlib.Path("audit"), "a" * 64, "6" * 32, "b" * 64)
        self.assertFalse(path.exists())
        self.assertEqual(raised.exception.details["status"], "partial_receipt_error")
        self.assertNotEqual(raised.exception.details["status"], "complete")
        self.assertEqual(raised.exception.details["database_writes"], 0)
        receipt = dict(raised.exception.details)
        receipt_hash = receipt.pop("receipt_sha256")
        self.assertEqual(receipt_hash, canary.canonical_sha256(receipt))

    def test_insufficient_real_free_space_is_partial_not_success(self):
        _root, path, _database = self._execute_fixture()
        with mock.patch.object(
            canary, "capacity", return_value={"free_bytes": 0, "free_inodes": 1}
        ):
            with self.assertRaises(canary.CanaryRunError) as raised:
                canary.execute(pathlib.Path("audit"), "a" * 64, "7" * 32, "b" * 64)
        self.assertFalse(path.exists())
        self.assertEqual(raised.exception.details["status"], "partial_after_unlink")


if __name__ == "__main__":
    unittest.main(verbosity=2)
