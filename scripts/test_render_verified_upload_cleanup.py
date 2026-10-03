"""Offline safety tests for the narrowly scoped Render cleanup executor."""

import errno
import hashlib
import json
import os
import pathlib
import sys
import tempfile
from types import SimpleNamespace
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import render_verified_upload_cleanup as cleanup  # noqa: E402


def build_report(files, candidates):
    inventory = {
        "file_count": len(files),
        "total_file_bytes": sum(item["size"] for item in files),
        "inventory_sha256": cleanup.inventory_digest(files),
        "files": files,
    }
    candidate_values = cleanup.candidate_summary(candidates)
    covered = ["dateien.stored_name"]
    coverage_sha256 = cleanup.canonical_sha256(covered)
    report = {
        "format": cleanup.AUDIT_FORMAT,
        "audit_id": "synthetic-audit",
        "created_at": "2026-10-03T00:00:00+00:00",
        "mode": "strictly_read_only",
        "upload_root": "/var/data/uploads",
        "source_inventory": inventory,
        "local_evidence": {
            "master_manifest_sha256": hashlib.sha256(b"manifest").hexdigest(),
            "verification_report_sha256": hashlib.sha256(b"verification").hexdigest(),
            "expected_coverage_sha256": coverage_sha256,
            "expected_candidate_count": candidate_values["candidate_count"],
            "expected_candidate_bytes": candidate_values["candidate_bytes"],
            "expected_candidate_blob_count": candidate_values["candidate_blob_count"],
            "expected_candidate_index_sha256": candidate_values[
                "candidate_index_sha256"
            ],
        },
        "reference_columns_covered": covered,
        "reference_columns_coverage_sha256": coverage_sha256,
        "reference_columns_coverage_matches": True,
        "candidate_baseline_matches": True,
        "inventory_rechecked_unchanged": True,
        "deletion_evidence_complete": True,
        "candidates": candidates,
        "database_writes": 0,
        "server_files_deleted": 0,
        "delete_approved": False,
        **candidate_values,
    }
    report["audit_sha256"] = cleanup.canonical_sha256(report)
    return report


def args_for(report):
    inventory = report["source_inventory"]
    local_evidence = report["local_evidence"]
    return SimpleNamespace(
        expected_audit_sha256=report["audit_sha256"],
        expected_inventory_sha256=inventory["inventory_sha256"],
        expected_files=inventory["file_count"],
        expected_bytes=inventory["total_file_bytes"],
        expected_candidates=report["candidate_count"],
        expected_candidate_bytes=report["candidate_bytes"],
        expected_candidate_blobs=report["candidate_blob_count"],
        expected_candidate_index_sha256=report["candidate_index_sha256"],
        expected_master_manifest_sha256=local_evidence["master_manifest_sha256"],
        expected_verification_report_sha256=local_evidence[
            "verification_report_sha256"
        ],
        expected_coverage_sha256=local_evidence["expected_coverage_sha256"],
    )


def write_audit(report, suffix):
    audit_root = pathlib.Path(tempfile.gettempdir()) / "gaertner-storage-audit-v1"
    audit_root.mkdir(parents=True, exist_ok=True)
    path = audit_root / f"synthetic-cleanup-test-{suffix}.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return path


def rehash_report(report):
    report["audit_sha256"] = cleanup.canonical_sha256(
        {key: value for key, value in report.items() if key != "audit_sha256"}
    )
    return report


def check(results, label, passed):
    results.append(bool(passed))
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")


def main():
    results = []
    skipped = 0
    raw_a = b"abc"
    raw_b = b"protected"
    files = [
        {
            "relative_path": "a.bin",
            "size": len(raw_a),
            "mtime_ns": 100,
            "sha256": __import__("hashlib").sha256(raw_a).hexdigest(),
        },
        {
            "relative_path": "b.bin",
            "size": len(raw_b),
            "mtime_ns": 200,
            "sha256": __import__("hashlib").sha256(raw_b).hexdigest(),
        },
    ]
    candidates = [
        {
            **files[0],
            "references": [
                {"table": "dateien", "column": "stored_name", "row_id": 42}
            ],
        }
    ]
    report = build_report(files, candidates)
    audit_path = write_audit(report, "valid")
    loaded = cleanup.load_and_validate_audit(audit_path, args_for(report))
    check(results, "Hashgebundener Audit wird akzeptiert", loaded == report)

    audit_contract_blocked = True
    contract_paths = []
    contract_mutations = (
        ("mode", lambda value: value.__setitem__("mode", "write_enabled")),
        ("upload_root", lambda value: value.__setitem__("upload_root", "/tmp/uploads")),
        ("local_evidence", lambda value: value.pop("local_evidence")),
        (
            "candidate_blob_bytes",
            lambda value: value.__setitem__(
                "candidate_blob_bytes", value["candidate_blob_bytes"] + 1
            ),
        ),
    )
    for field, mutate in contract_mutations:
        broken = json.loads(json.dumps(report))
        mutate(broken)
        rehash_report(broken)
        broken_path = write_audit(broken, f"contract-{field}")
        contract_paths.append(broken_path)
        try:
            cleanup.load_and_validate_audit(broken_path, args_for(report))
        except cleanup.CleanupError:
            continue
        audit_contract_blocked = False
    check(
        results,
        "Auditvertrag erzwingt mode, upload_root, local_evidence und candidate_blob_bytes",
        audit_contract_blocked,
    )

    evidence_args = args_for(report)
    evidence_args.expected_master_manifest_sha256 = "0" * 64
    try:
        cleanup.load_and_validate_audit(audit_path, evidence_args)
    except cleanup.CleanupError:
        evidence_cli_bound = True
    else:
        evidence_cli_bound = False
    check(
        results,
        "Lokale Archiv-Evidenz ist an explizite CLI-Pruefsummen gebunden",
        evidence_cli_bound,
    )

    selected = cleanup.select_candidates(loaded, 1)
    synthetic_after = cleanup.expected_inventory_after(report, selected)
    with mock.patch.multiple(
        cleanup,
        BOOTSTRAP_CANDIDATE_PATH="a.bin",
        BOOTSTRAP_CANDIDATE_SHA256=selected[0]["sha256"],
        BOOTSTRAP_CANDIDATE_SIZE=selected[0]["size"],
        BOOTSTRAP_CANDIDATE_REFERENCE=selected[0]["references"][0],
        BOOTSTRAP_SOURCE_INVENTORY={
            key: report["source_inventory"][key]
            for key in ("file_count", "total_file_bytes", "inventory_sha256")
        },
        BOOTSTRAP_EXPECTED_AFTER={
            key: synthetic_after[key]
            for key in ("file_count", "total_file_bytes", "inventory_sha256")
        },
        BOOTSTRAP_REMAINING_CANDIDATES=cleanup.candidate_summary([]),
        BOOTSTRAP_LOCAL_EVIDENCE=dict(report["local_evidence"]),
    ):
        bound_candidate, bound_remaining = cleanup.validate_bootstrap_candidate(
            report, "a.bin"
        )
        try:
            cleanup.validate_bootstrap_candidate(report, "b.bin")
        except cleanup.CleanupError:
            alternate_bootstrap_blocked = True
        else:
            alternate_bootstrap_blocked = False
    check(
        results,
        "Bootstrap-Modus akzeptiert exakt einen hart gebundenen Canary-Kandidaten",
        bound_candidate == selected[0]
        and bound_remaining == []
        and alternate_bootstrap_blocked,
    )

    fixed_hidden = cleanup.bootstrap_hidden_name(
        "1" * 32,
        {"relative_path": cleanup.BOOTSTRAP_CANDIDATE_PATH},
    )
    check(
        results,
        "Bootstrap-Hidden-Name ist run-id-gebunden und exakt so lang wie das Original",
        fixed_hidden == "." + "1" * 31 + ".pdf"
        and len(os.fsencode(fixed_hidden)) == 36
        and len(os.fsencode(fixed_hidden))
        == len(os.fsencode(cleanup.BOOTSTRAP_CANDIDATE_PATH)),
    )

    plan_run_id = "2" * 32
    plan_script_hash = "a" * 64
    plan_commit = "b" * 40
    plan_boot_id = "11111111-2222-3333-4444-555555555555"
    plan_processes = {
        "master": {
            "pid": 100,
            "ppid": 1,
            "starttime": 1000,
            "uid": 1000,
            "cmdline_sha256": "c" * 64,
            "cgroup_sha256": "d" * 64,
            "state": "S",
        },
        "worker": {
            "pid": 101,
            "ppid": 100,
            "starttime": 1001,
            "uid": 1000,
            "cmdline_sha256": "e" * 64,
            "cgroup_sha256": "f" * 64,
            "state": "S",
        },
    }
    plan_capacity, plan_required_bytes = cleanup.bootstrap_capacity_plan(report, [])
    plan_expected_after = cleanup.expected_inventory_after(report, selected)
    prepared_plan = {
        "format": cleanup.BOOTSTRAP_PLAN_FORMAT,
        "run_id": plan_run_id,
        "created_at": "2026-10-03T00:00:00+00:00",
        "status": "bootstrap_prepared_read_only",
        "script_sha256": plan_script_hash,
        "render_git_commit": plan_commit,
        "boot_id": plan_boot_id,
        "upload_root": str(cleanup.EXPECTED_UPLOAD_ROOT),
        "upload_root_identity": [1, 10],
        "audit_sha256": report["audit_sha256"],
        "master_manifest_sha256": report["local_evidence"]["master_manifest_sha256"],
        "database_writes": 0,
        "candidate": selected[0],
        "candidate_file_identity": [1, 100, 3, 100, 200, 1],
        "candidate_allocated_bytes": 10_240_000,
        "hidden_name": "x.bin",
        "expected_after": {
            key: plan_expected_after[key]
            for key in ("file_count", "total_file_bytes", "inventory_sha256")
        },
        "remaining_candidates": cleanup.candidate_summary([]),
        "required_remaining_ledger_bytes": plan_required_bytes,
        "remaining_capacity_plan_sha256": plan_capacity["plan_sha256"],
        "pre_capacity": {
            "available_bytes": 0,
            "available_inodes": 100,
            "required_ledger_bytes": plan_required_bytes,
            "required_ledger_inodes": cleanup.PERSISTENT_LEDGER_MIN_INODES,
            "sufficient_for_regular_cleanup": False,
        },
        "database_preflight": {
            "row_signature_count": 1,
            "row_signatures_sha256": "6" * 64,
        },
        "processes": plan_processes,
        "watchdog_contract": cleanup.bootstrap_watchdog_contract(plan_script_hash),
    }
    prepared_plan["plan_sha256"] = cleanup.canonical_sha256(prepared_plan)
    with (
        mock.patch.object(cleanup, "sha256_regular_file", return_value=plan_script_hash),
        mock.patch.object(cleanup, "read_boot_id", return_value=plan_boot_id),
        mock.patch.object(cleanup, "bootstrap_hidden_name", return_value="x.bin"),
        mock.patch.object(
            cleanup,
            "BOOTSTRAP_CANDIDATE_ALLOCATED_BYTES",
            10_240_000,
        ),
        mock.patch.dict(os.environ, {"RENDER_GIT_COMMIT": plan_commit}),
    ):
        validated_plan = cleanup.validate_prepared_bootstrap_plan(
            prepared_plan,
            run_id=plan_run_id,
            expected_plan_sha256=prepared_plan["plan_sha256"],
            report=report,
            candidate=selected[0],
            remaining_candidates=[],
            root_identity=(1, 10),
        )
        forged_plan = json.loads(json.dumps(prepared_plan))
        forged_plan["processes"]["worker"]["starttime"] += 1
        forged_plan["plan_sha256"] = cleanup.canonical_sha256(
            {key: value for key, value in forged_plan.items() if key != "plan_sha256"}
        )
        try:
            cleanup.validate_prepared_bootstrap_plan(
                forged_plan,
                run_id=plan_run_id,
                expected_plan_sha256=prepared_plan["plan_sha256"],
                report=report,
                candidate=selected[0],
                remaining_candidates=[],
                root_identity=(1, 10),
            )
        except cleanup.CleanupError:
            forged_plan_blocked = True
        else:
            forged_plan_blocked = False
    with mock.patch.object(
        cleanup, "scan_portal_processes", return_value=plan_processes
    ):
        forged_topology = json.loads(json.dumps(plan_processes))
        forged_topology["worker"]["starttime"] += 1
        try:
            cleanup.assert_process_topology(forged_topology)
        except cleanup.CleanupError:
            forged_process_blocked = True
        else:
            forged_process_blocked = False
    check(
        results,
        "PREPARE-Plan bindet SHA und exakte /proc-Prozessidentitaeten gegen Faelschung",
        validated_plan == prepared_plan
        and forged_plan_blocked
        and forged_process_blocked,
    )

    prepare_writes = []
    prepare_rename = mock.Mock()
    prepare_unlink = mock.Mock()
    prepare_stat = SimpleNamespace(
        st_mode=__import__("stat").S_IFREG | 0o600,
        st_dev=1,
        st_ino=100,
        st_size=len(raw_a),
        st_mtime_ns=100,
        st_ctime_ns=200,
        st_nlink=1,
        st_blocks=20_000,
    )

    def capture_prepare_json(_directory_fd, name, value):
        prepare_writes.append((name, json.loads(json.dumps(value))))

    with (
        mock.patch.object(cleanup, "require_pidfd_support"),
        mock.patch.object(cleanup, "bootstrap_hidden_name", return_value="x.bin"),
        mock.patch.object(cleanup, "open_upload_root", return_value=(100, (1, 10))),
        mock.patch.object(cleanup, "scan_inventory", return_value=report["source_inventory"]),
        mock.patch.object(cleanup, "assert_root_binding"),
        mock.patch.object(
            cleanup,
            "preflight_database",
            return_value={
                "row_signatures": [{"datei_id": 42}],
                "row_signatures_sha256": "6" * 64,
                "row_lock_projection": [{"datei_id": 42}],
            },
        ),
        mock.patch.object(
            cleanup,
            "open_verified_candidate",
            return_value=(200, cleanup.file_identity(prepare_stat)),
        ),
        mock.patch.object(cleanup.os, "fstat", return_value=prepare_stat),
        mock.patch.object(
            cleanup,
            "bootstrap_path_state",
            return_value={"exists": False, "matches_held_fd": False},
        ),
        mock.patch.object(cleanup, "scan_portal_processes", return_value=plan_processes),
        mock.patch.object(cleanup, "sha256_regular_file", return_value=plan_script_hash),
        mock.patch.object(cleanup, "read_boot_id", return_value=plan_boot_id),
        mock.patch.object(
            cleanup,
            "capacity_snapshot",
            return_value=prepared_plan["pre_capacity"],
        ),
        mock.patch.object(
            cleanup,
            "create_private_run",
            return_value=(pathlib.Path("/tmp/bootstrap-prepare"), 201, (2, 20)),
        ) as prepare_create_run,
        mock.patch.object(cleanup, "atomic_json_at", side_effect=capture_prepare_json),
        mock.patch.object(cleanup, "BOOTSTRAP_CANDIDATE_ALLOCATED_BYTES", 10_240_000),
        mock.patch.object(cleanup.os, "rename", prepare_rename),
        mock.patch.object(cleanup.os, "unlink", prepare_unlink),
        mock.patch.object(cleanup.os, "close"),
        mock.patch.dict(os.environ, {"RENDER_GIT_COMMIT": plan_commit}),
    ):
        prepared_read_only = cleanup.prepare_bootstrap_canary(
            report,
            selected[0],
            [],
            cleanup.EXPECTED_UPLOAD_ROOT,
            "postgresql://test",
        )
    check(
        results,
        "Bootstrap-PREPARE liest /var/data und DB nur und schreibt ausschliesslich den /tmp-Plan",
        prepared_read_only["status"] == "bootstrap_prepared_read_only"
        and prepared_read_only["database_writes"] == 0
        and prepare_create_run.call_args.args[0] == cleanup.BOOTSTRAP_TEMP_ROOT
        and len(prepare_writes) == 1
        and prepare_writes[0][0] == ".bootstrap-plan.json"
        and prepare_writes[0][1]["plan_sha256"] == prepared_read_only["plan_sha256"]
        and prepare_rename.call_count == 0
        and prepare_unlink.call_count == 0,
    )

    watchdog_identity = {
        "pid": 900,
        "ppid": 1,
        "starttime": 9000,
        "uid": 1000,
        "cmdline_sha256": "7" * 64,
        "cgroup_sha256": "8" * 64,
        "state": "S",
    }
    watchdog_token_sha256 = "9" * 64
    fired_marker = cleanup.marker_with_hash(
        {
            "format": cleanup.BOOTSTRAP_MARKER_FORMAT,
            "run_id": plan_run_id,
            "event": "fired",
            "created_at": "2026-10-03T00:00:01+00:00",
            "token_sha256": watchdog_token_sha256,
            "deadline_monotonic": 20.0,
            "watchdog_identity": watchdog_identity,
            "resume_order": ["worker", "master"],
        }
    )
    watchdog_handle = {
        "process": SimpleNamespace(poll=lambda: None),
        "process_identity": watchdog_identity,
        "token_sha256": watchdog_token_sha256,
        "deadline_monotonic": 20.0,
        "cutoff_monotonic": 8.0,
        "ready_marker": {"run_id": plan_run_id},
    }
    with (
        mock.patch.object(cleanup, "marker_exists", return_value=True),
        mock.patch.object(cleanup, "read_small_json_at", return_value=fired_marker),
    ):
        try:
            cleanup.assert_watchdog_window(
                watchdog_handle,
                201,
                plan_processes,
                require_mutation_window=True,
            )
        except cleanup.WatchdogUnsafeError:
            fired_window_blocked = True
        else:
            fired_window_blocked = False
    cutoff_topology = mock.Mock()
    with (
        mock.patch.object(cleanup, "marker_exists", return_value=False),
        mock.patch.object(
            cleanup,
            "capture_process_identity",
            return_value=watchdog_identity,
        ),
        mock.patch.object(cleanup.time, "monotonic", return_value=8.01),
        mock.patch.object(
            cleanup, "assert_process_topology", cutoff_topology
        ),
    ):
        try:
            cleanup.assert_watchdog_window(
                watchdog_handle,
                201,
                plan_processes,
                require_mutation_window=True,
            )
        except cleanup.WatchdogUnsafeError:
            cutoff_window_blocked = True
        else:
            cutoff_window_blocked = False
    check(
        results,
        "Watchdog-fired und T+8s-Cutoff sperren jede weitere Bootstrap-Mutation",
        fired_window_blocked
        and cutoff_window_blocked
        and cutoff_topology.call_count == 0,
    )

    pidfd_zero_signals = []
    with (
        mock.patch.object(
            cleanup,
            "pidfd_send",
            side_effect=lambda fd, sig: pidfd_zero_signals.append((fd, sig)),
        ),
        mock.patch.object(
            cleanup,
            "capture_process_identity",
            return_value=plan_processes["master"],
        ),
    ):
        bound_pidfd_identity = cleanup.assert_pidfd_identity(
            "master", plan_processes["master"], 301
        )
    replaced_identity = {
        **plan_processes["master"],
        "starttime": plan_processes["master"]["starttime"] + 1,
    }
    replaced_pidfd_signals = []
    with (
        mock.patch.object(
            cleanup,
            "pidfd_send",
            side_effect=lambda fd, sig: replaced_pidfd_signals.append((fd, sig)),
        ),
        mock.patch.object(
            cleanup,
            "capture_process_identity",
            return_value=replaced_identity,
        ),
    ):
        try:
            cleanup.assert_pidfd_identity(
                "master", plan_processes["master"], 301
            )
        except cleanup.CleanupError:
            replaced_pidfd_blocked = True
        else:
            replaced_pidfd_blocked = False
    crossing_topology = mock.Mock(return_value=plan_processes)
    with (
        mock.patch.object(cleanup, "marker_exists", return_value=False),
        mock.patch.object(
            cleanup,
            "capture_process_identity",
            return_value=watchdog_identity,
        ),
        mock.patch.object(cleanup.time, "monotonic", side_effect=[7.9, 8.01]),
        mock.patch.object(
            cleanup, "assert_process_topology", crossing_topology
        ),
    ):
        try:
            cleanup.assert_watchdog_window(
                watchdog_handle,
                201,
                plan_processes,
                require_mutation_window=True,
            )
        except cleanup.WatchdogUnsafeError:
            scan_crossing_blocked = True
        else:
            scan_crossing_blocked = False
    check(
        results,
        "PIDFD wird nach open an /proc gebunden und langsamer T-Scan darf den Cutoff nicht ueberlaufen",
        bound_pidfd_identity == plan_processes["master"]
        and pidfd_zero_signals == [(301, 0), (301, 0)]
        and replaced_pidfd_blocked
        and replaced_pidfd_signals == [(301, 0)]
        and scan_crossing_blocked
        and crossing_topology.call_count == 1,
    )
    after = cleanup.expected_inventory_after(loaded, selected)
    check(
        results,
        "Erwartetes Nachher-Inventar entfernt ausschließlich den Kandidaten",
        selected[0]["relative_path"] == "a.bin"
        and after["file_count"] == 1
        and after["total_file_bytes"] == len(raw_b)
        and after["files"][0]["relative_path"] == "b.bin"
        and after["inventory_sha256"] == cleanup.inventory_digest([files[1]]),
    )

    tampered = json.loads(json.dumps(report))
    tampered["source_inventory"]["files"][0]["size"] += 1
    tampered_path = write_audit(tampered, "tampered")
    try:
        cleanup.load_and_validate_audit(tampered_path, args_for(report))
    except cleanup.CleanupError:
        tamper_blocked = True
    else:
        tamper_blocked = False
    check(results, "Veränderte Audit-Nutzlast wird abgewiesen", tamper_blocked)

    unsafe = json.loads(json.dumps(report))
    unsafe["source_inventory"]["files"][0]["relative_path"] = "../a.bin"
    unsafe["candidates"][0]["relative_path"] = "../a.bin"
    unsafe["source_inventory"]["inventory_sha256"] = cleanup.inventory_digest(
        unsafe["source_inventory"]["files"]
    )
    unsafe["audit_sha256"] = cleanup.canonical_sha256(
        {key: value for key, value in unsafe.items() if key != "audit_sha256"}
    )
    unsafe_path = write_audit(unsafe, "unsafe")
    try:
        cleanup.load_and_validate_audit(unsafe_path, args_for(unsafe))
    except cleanup.CleanupError:
        unsafe_blocked = True
    else:
        unsafe_blocked = False
    check(results, "Pfadnavigation im Audit wird abgewiesen", unsafe_blocked)

    not_read_only = json.loads(json.dumps(report))
    not_read_only["database_writes"] = 1
    not_read_only["audit_sha256"] = cleanup.canonical_sha256(
        {key: value for key, value in not_read_only.items() if key != "audit_sha256"}
    )
    not_read_only_path = write_audit(not_read_only, "write")
    try:
        cleanup.load_and_validate_audit(not_read_only_path, args_for(not_read_only))
    except cleanup.CleanupError:
        write_blocked = True
    else:
        write_blocked = False
    check(results, "Nicht-read-only Audit wird abgewiesen", write_blocked)

    class FakeLockResult:
        def __init__(self, acquired):
            self.acquired = acquired

        def fetchone(self):
            return {
                "acquired": self.acquired,
                "locked": self.acquired,
                "pg_try_advisory_lock": self.acquired,
            }

    class FakeLockConnection:
        def __init__(self, acquired):
            self.acquired = acquired
            self.queries = []
            self.rollbacks = 0

        def execute(self, query, parameters=None):
            self.queries.append((str(query), parameters))
            return FakeLockResult(self.acquired)

        def rollback(self):
            self.rollbacks += 1

    lock_database = cleanup.LockedPostgres.__new__(cleanup.LockedPostgres)
    lock_database.connection = FakeLockConnection(True)
    lock_database.advisory_locked = False
    lock_database.acquire_advisory_lock()
    check(
        results,
        "Advisory-Lock wird nicht blockierend vor Tabellenlocks angefordert",
        lock_database.advisory_locked
        and len(lock_database.connection.queries) == 1
        and "pg_try_advisory_lock" in lock_database.connection.queries[0][0],
    )

    busy_database = cleanup.LockedPostgres.__new__(cleanup.LockedPostgres)
    busy_database.connection = FakeLockConnection(False)
    busy_database.advisory_locked = False
    try:
        busy_database.acquire_advisory_lock()
    except cleanup.CleanupError:
        busy_lock_blocked = not busy_database.advisory_locked
    else:
        busy_lock_blocked = False
    check(results, "Belegter Advisory-Lock bricht sofort ab", busy_lock_blocked)

    class FakeSqlToken:
        def __init__(self, value):
            self.value = value

        def join(self, values):
            return FakeSqlToken(self.value.join(str(value) for value in values))

        def format(self, *values):
            result = self.value
            for value in values:
                result = result.replace("{}", str(value), 1)
            return FakeSqlToken(result)

        def __str__(self):
            return self.value

    fake_sql = SimpleNamespace(
        SQL=lambda value: FakeSqlToken(value),
        Identifier=lambda value: FakeSqlToken(value),
    )
    ordered_database = cleanup.LockedPostgres.__new__(cleanup.LockedPostgres)
    ordered_database.connection = FakeLockConnection(True)
    ordered_database.sql = fake_sql
    ordered_database.advisory_locked = False
    ordered_database.acquire_advisory_lock()
    ordered_database.lock_reference_tables([])
    ordered_queries = [query for query, _parameters in ordered_database.connection.queries]
    check(
        results,
        "Try-Advisory steht im Event-Trace vor SHARE-NOWAIT-Tabellenlock",
        len(ordered_queries) == 2
        and "pg_try_advisory_lock" in ordered_queries[0]
        and "LOCK TABLE" in ordered_queries[1]
        and "SHARE MODE NOWAIT" in ordered_queries[1],
    )

    fake_database = cleanup.LockedPostgres.__new__(cleanup.LockedPostgres)
    fake_database.advisory_locked = False
    try:
        fake_database.lock_reference_tables([])
    except cleanup.CleanupError:
        order_blocked = True
    else:
        order_blocked = False
    check(results, "Tabellenlock ohne vorherigen Advisory-Lock wird verweigert", order_blocked)

    class FakeSignatureCursor:
        def fetchall(self):
            return [
                {
                    "datei_id": 42,
                    "datei_xmin": "101",
                    "datei_ctid": "(0,1)",
                    "stored_name": "a.bin",
                    "datei_size": len(raw_a),
                    "backup_id": 99,
                    "backup_xmin": "102",
                    "backup_ctid": "(0,2)",
                    "backup_sha256": hashlib.sha256(raw_a).hexdigest(),
                    "backup_size": len(raw_a),
                }
            ]

    class FakeSignatureConnection:
        def __init__(self):
            self.query = ""

        def execute(self, query, parameters=None):
            self.query = str(query)
            return FakeSignatureCursor()

    signature_connection = FakeSignatureConnection()
    signature_database = SimpleNamespace(connection=signature_connection)
    signatures = cleanup.candidate_row_signatures(
        signature_database, [42], include_blob_length=False
    )
    check(
        results,
        "Kurze DB-Signatur unter Tabellenlock beruehrt kein Base64-TOAST-Feld",
        len(signatures) == 1
        and signatures[0]["datei_xmin"] == "101"
        and "char_length" not in signature_connection.query.lower()
        and "file_base64" not in signature_connection.query.lower(),
    )

    with tempfile.TemporaryDirectory(prefix="verified_cleanup_journal_") as tmp:
        journal_path = pathlib.Path(tmp) / "journal.jsonl"
        writer = cleanup.JournalWriter(journal_path, "unit-run")
        first_record = writer.append(
            {
                "event": "run_started",
                "audit_sha256": report["audit_sha256"],
                "plan_sha256": "1" * 64,
            }
        )
        second_record = writer.append({"event": "stage_intent", "relative_path": "a.bin"})
        writer.close()
        journal_summary = writer.summary()
        journal_bytes = journal_path.read_bytes()
        journal_records = [
            json.loads(line) for line in journal_bytes.decode("utf-8").splitlines()
        ]
        chain_valid = True
        previous_hash = "0" * 64
        for sequence, record in enumerate(journal_records, start=1):
            claimed_hash = record.pop("event_sha256")
            chain_valid = chain_valid and (
                record["sequence"] == sequence
                and record["previous_event_sha256"] == previous_hash
                and claimed_hash == cleanup.canonical_sha256(record)
            )
            previous_hash = claimed_hash
        check(
            results,
            "Journal ist monoton hashverkettet und bindet Audit sowie Plan",
            chain_valid
            and first_record["event"] == "run_started"
            and first_record["audit_sha256"] == report["audit_sha256"]
            and first_record["plan_sha256"] == "1" * 64
            and second_record["previous_event_sha256"] == first_record["event_sha256"]
            and journal_summary["event_count"] == 2
            and journal_summary["last_event_sha256"] == second_record["event_sha256"]
            and journal_summary["journal_sha256"]
            == hashlib.sha256(journal_bytes).hexdigest(),
        )

    setup_run_id = "1" * 32
    setup_plan = {
        "format": cleanup.CLEANUP_FORMAT,
        "run_id": setup_run_id,
        "created_at": "2026-10-03T00:00:00+00:00",
        "status": "authorized_scope",
        "audit_sha256": report["audit_sha256"],
        "advisory_lock_key": cleanup.PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
        "database_writes": 0,
        "selected_count": 1,
        "selected_bytes": selected[0]["size"],
        "selected": selected,
        "expected_after": {
            key: cleanup.expected_inventory_after(report, selected)[key]
            for key in ("file_count", "total_file_bytes", "inventory_sha256")
        },
        "expected_remaining_candidates": cleanup.candidate_summary([]),
    }
    setup_plan["plan_sha256"] = cleanup.canonical_sha256(setup_plan)
    setup_receipt = cleanup.receipt_with_hash(
        {
            "format": cleanup.CLEANUP_FORMAT,
            "run_id": setup_run_id,
            "created_at": "2026-10-03T00:00:00+00:00",
            "status": "cleanup_setup_incomplete",
            "audit_sha256": report["audit_sha256"],
            "plan_sha256": setup_plan["plan_sha256"],
            "database_writes": 0,
            "selected_count": 1,
            "selected_bytes": selected[0]["size"],
            "deleted_count": 0,
            "restored_count": 0,
            "staged_remaining_count": 0,
            "uncertain": [dict(cleanup.SETUP_UNCERTAINTY)],
            "ledger_capacity": {
                "available_bytes_before_run": 16 * 1024 * 1024,
                "available_inodes_before_run": 100,
                "required_ledger_bytes": cleanup.PERSISTENT_LEDGER_MIN_BYTES,
                "required_ledger_inodes": cleanup.PERSISTENT_LEDGER_MIN_INODES,
            },
        }
    )
    setup_files = [
        ".cleanup-receipt.json",
        ".cleanup-plan.json",
        ".cleanup-journal.jsonl",
    ]

    def read_setup_fixture(_fd, name):
        return setup_receipt if name == ".cleanup-receipt.json" else setup_plan

    with (
        mock.patch.object(cleanup, "read_small_json_at", side_effect=read_setup_fixture),
        mock.patch.object(
            cleanup,
            "setup_file_identity_at",
            side_effect=lambda _fd, name: (1, name),
        ),
        mock.patch.object(cleanup, "read_setup_journal_at") as setup_journal_check,
    ):
        setup_identities = cleanup.validate_setup_run(
            77, setup_run_id, setup_files
        )
    check(
        results,
        "Authentischer Setup-Run ist vor jeder Mutation streng validierbar",
        set(setup_identities) == set(setup_files)
        and setup_journal_check.call_args.args == (77, setup_run_id),
    )

    armed_receipt = cleanup.receipt_with_hash(
        {
            "format": cleanup.CLEANUP_FORMAT,
            "run_id": setup_run_id,
            "created_at": "2026-10-03T00:00:01+00:00",
            "status": "cleanup_mutation_armed",
            "audit_sha256": report["audit_sha256"],
            "plan_sha256": setup_plan["plan_sha256"],
            "database_writes": 0,
        }
    )
    base_info = SimpleNamespace(
        st_mode=__import__("stat").S_IFDIR | 0o700,
        st_dev=1,
        st_ino=10,
    )
    prior_info = SimpleNamespace(
        st_mode=__import__("stat").S_IFDIR | 0o700,
        st_dev=1,
        st_ino=11,
    )
    armed_rmdir = mock.Mock()
    with (
        mock.patch.object(cleanup, "require_secure_directory_flags", return_value=0),
        mock.patch.object(
            cleanup.os,
            "listdir",
            side_effect=lambda fd: (
                [f"run-{setup_run_id}"]
                if fd == 70
                else [".cleanup-receipt.json"]
            ),
        ),
        mock.patch.object(cleanup.os, "fstat", side_effect=lambda fd: base_info if fd == 70 else prior_info),
        mock.patch.object(cleanup.os, "stat", return_value=prior_info),
        mock.patch.object(cleanup.os, "open", return_value=71),
        mock.patch.object(cleanup.os, "close"),
        mock.patch.object(cleanup.os, "rmdir", armed_rmdir),
        mock.patch.object(cleanup, "read_small_json_at", return_value=armed_receipt),
    ):
        try:
            cleanup.reconcile_prior_runs_at(70)
        except cleanup.CleanupError as exc:
            armed_blocks = "Mutationsbereiter" in str(exc)
        else:
            armed_blocks = False
    check(
        results,
        "Durabler mutation_armed-Beleg wird niemals automatisch entfernt",
        armed_blocks and armed_rmdir.call_count == 0,
    )

    empty_rmdir = mock.Mock()
    with (
        mock.patch.object(cleanup, "require_secure_directory_flags", return_value=0),
        mock.patch.object(
            cleanup.os,
            "listdir",
            side_effect=lambda fd: [f"run-{setup_run_id}"] if fd == 70 else [],
        ),
        mock.patch.object(cleanup.os, "fstat", side_effect=lambda fd: base_info if fd == 70 else prior_info),
        mock.patch.object(cleanup.os, "stat", return_value=prior_info),
        mock.patch.object(cleanup.os, "open", return_value=71),
        mock.patch.object(cleanup.os, "close"),
        mock.patch.object(cleanup.os, "rmdir", empty_rmdir),
        mock.patch.object(cleanup, "fsync_directory"),
    ):
        empty_removed = cleanup.reconcile_prior_runs_at(70)
    check(
        results,
        "Crash zwischen mkdir und Setup-Receipt hinterlaesst nur entfernbaren leeren Run",
        empty_removed == [f"run-{setup_run_id}"]
        and empty_rmdir.call_args.args[0] == f"run-{setup_run_id}",
    )

    foreign_rmdir = mock.Mock()
    with (
        mock.patch.object(cleanup, "require_secure_directory_flags", return_value=0),
        mock.patch.object(
            cleanup.os,
            "listdir",
            side_effect=lambda fd: (
                [f"run-{setup_run_id}"] if fd == 70 else ["foreign.bin"]
            ),
        ),
        mock.patch.object(cleanup.os, "fstat", side_effect=lambda fd: base_info if fd == 70 else prior_info),
        mock.patch.object(cleanup.os, "stat", return_value=prior_info),
        mock.patch.object(cleanup.os, "open", return_value=71),
        mock.patch.object(cleanup.os, "close"),
        mock.patch.object(cleanup.os, "rmdir", foreign_rmdir),
    ):
        try:
            cleanup.reconcile_prior_runs_at(70)
        except cleanup.CleanupError:
            foreign_blocks = True
        else:
            foreign_blocks = False
    check(
        results,
        "Fremder Inhalt in einem Run-Verzeichnis blockiert die Folgebereinigung",
        foreign_blocks and foreign_rmdir.call_count == 0,
    )

    check(
        results,
        "Nichtterminaler persistenter Lauf blockiert einen Folgelauf",
        not cleanup.prior_run_is_resolved({"status": "cleanup_partial"})
        and not cleanup.prior_run_is_resolved(
            {"status": "cleanup_failed_no_deletion", "uncertain": [{"x": 1}]}
        )
        and cleanup.prior_run_is_resolved({"status": "cleanup_complete"})
        and cleanup.prior_run_is_resolved(
            {
                "status": "cleanup_failed_no_deletion",
                "staged_remaining": [],
                "uncertain": [],
            }
        ),
    )

    with tempfile.TemporaryDirectory(prefix="verified_cleanup_file_") as tmp:
        root = pathlib.Path(tmp)
        path = root / "a.bin"
        path.write_bytes(raw_a)
        info = path.stat()
        candidate = dict(
            candidates[0],
            mtime_ns=info.st_mtime_ns,
        )
        if os.name == "nt":
            verified = path.read_bytes() == raw_a
        else:
            root_fd = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                fd, _identity = cleanup.open_verified_candidate(root_fd, candidate)
                os.close(fd)
                verified = path.read_bytes() == raw_a
            finally:
                os.close(root_fd)
        check(results, "Dateiprüfung verändert oder löscht die Quelldatei nicht", verified)

    posix_fd_release_verified = None
    if os.name == "posix":
        with tempfile.TemporaryDirectory(prefix="verified_cleanup_fd_blocks_") as tmp:
            root = pathlib.Path(tmp)
            path = root / "allocated.bin"
            directory_fd = os.open(
                root,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            held_fd = os.open(
                path,
                os.O_RDWR | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            try:
                allocation_size = 16 * 1024 * 1024
                if callable(getattr(os, "posix_fallocate", None)):
                    os.posix_fallocate(held_fd, 0, allocation_size)
                else:
                    block = b"x" * (1024 * 1024)
                    for _ in range(allocation_size // len(block)):
                        os.write(held_fd, block)
                os.fsync(held_fd)
                allocated_bytes = int(os.fstat(held_fd).st_blocks) * 512
                os.unlink(path)
                os.fsync(directory_fd)
                held_after_unlink = os.fstat(held_fd)
                held_vfs = os.statvfs(root)
                held_available = int(held_vfs.f_bavail) * int(held_vfs.f_frsize)
                os.close(held_fd)
                held_fd = None
                os.fsync(directory_fd)
                released_available = held_available
                for _ in range(100):
                    released_vfs = os.statvfs(root)
                    released_available = (
                        int(released_vfs.f_bavail) * int(released_vfs.f_frsize)
                    )
                    if released_available > held_available:
                        break
                    __import__("time").sleep(0.01)
                posix_fd_release_verified = (
                    held_after_unlink.st_nlink == 0
                    and allocated_bytes > 0
                    and released_available
                    >= held_available + max(4096, allocated_bytes // 2)
                )
            finally:
                if held_fd is not None:
                    os.close(held_fd)
                os.close(directory_fd)
    if posix_fd_release_verified is None:
        skipped += 1
        print(
            "[SKIP] POSIX haelt geloeschte Dateibloecke bis zum Schliessen des letzten FD "
            "(Linux-Laufzeit erforderlich)"
        )
    else:
        check(
            results,
            "POSIX haelt geloeschte Dateibloecke bis zum Schliessen des letzten FD",
            posix_fd_release_verified,
        )

    expected_stat = SimpleNamespace(
        st_mode=__import__("stat").S_IFREG | 0o600,
        st_dev=1,
        st_ino=100,
        st_size=len(raw_a),
        st_mtime_ns=100,
        st_ctime_ns=200,
        st_nlink=1,
    )
    swapped_stat = SimpleNamespace(
        st_mode=expected_stat.st_mode,
        st_dev=1,
        st_ino=999,
        st_size=len(raw_a),
        st_mtime_ns=100,
        st_ctime_ns=201,
        st_nlink=1,
    )
    staged_entry = {
        "candidate": candidates[0],
        "fd": 123,
        "identity": cleanup.file_identity(expected_stat),
        "state": "staged",
    }
    unlink_spy = mock.Mock()
    race_journal = SimpleNamespace(append=mock.Mock())
    with (
        mock.patch.object(cleanup.os, "fstat", return_value=expected_stat),
        mock.patch.object(cleanup.os, "stat", return_value=swapped_stat),
        mock.patch.object(cleanup.os, "unlink", unlink_spy),
    ):
        try:
            cleanup.unlink_staged_files([staged_entry], 456, race_journal)
        except cleanup.CleanupError:
            swapped_inode_blocked = True
        else:
            swapped_inode_blocked = False
    check(
        results,
        "Ausgetauschte Pending-Inode wird niemals geloescht",
        swapped_inode_blocked
        and unlink_spy.call_count == 0
        and staged_entry["state"] == "staged"
        and race_journal.append.call_count == 0,
    )

    class FakeRenameAt2:
        def __init__(self):
            self.argtypes = None
            self.restype = None
            self.calls = []

        def __call__(self, *args):
            self.calls.append(args)
            return -1

    fake_renameat2 = FakeRenameAt2()
    with (
        mock.patch.object(cleanup.os, "name", "posix"),
        mock.patch.object(
            cleanup.ctypes,
            "CDLL",
            return_value=SimpleNamespace(renameat2=fake_renameat2),
        ),
        mock.patch.object(cleanup.ctypes, "get_errno", return_value=errno.EEXIST),
    ):
        try:
            cleanup.rename_noreplace(
                "a.bin",
                "a.bin",
                source_dir_fd=10,
                target_dir_fd=11,
            )
        except OSError as exc:
            kernel_collision_blocked = exc.errno == errno.EEXIST
        else:
            kernel_collision_blocked = False

    collision_entry = {
        "candidate": candidates[0],
        "fd": 123,
        "identity": cleanup.file_identity(expected_stat),
        "state": "opened",
    }
    collision_journal = SimpleNamespace(append=mock.Mock())
    collision_noreplace = mock.Mock(
        side_effect=FileExistsError(errno.EEXIST, "target exists", "a.bin")
    )
    collision_plain_rename = mock.Mock()
    collision_unlink = mock.Mock()
    with (
        mock.patch.object(cleanup.os, "fstat", return_value=expected_stat),
        mock.patch.object(cleanup.os, "stat", return_value=expected_stat),
        mock.patch.object(cleanup, "rename_noreplace", collision_noreplace),
        mock.patch.object(cleanup.os, "rename", collision_plain_rename),
        mock.patch.object(cleanup.os, "unlink", collision_unlink),
    ):
        try:
            cleanup.stage_selected_files(
                [collision_entry], 10, 11, collision_journal
            )
        except FileExistsError as exc:
            stage_collision_blocked = exc.errno == errno.EEXIST
        else:
            stage_collision_blocked = False
    check(
        results,
        "Staging-Zielkollision wird atomar ohne Ueberschreiben oder Loeschen abgewiesen",
        kernel_collision_blocked
        and len(fake_renameat2.calls) == 1
        and fake_renameat2.calls[0][-1] == cleanup.RENAME_NOREPLACE
        and stage_collision_blocked
        and collision_noreplace.call_count == 1
        and collision_noreplace.call_args.kwargs
        == {"source_dir_fd": 10, "target_dir_fd": 11}
        and collision_plain_rename.call_count == 0
        and collision_unlink.call_count == 0
        and collision_entry["state"] == "opened"
        and collision_journal.append.call_count == 1
        and collision_journal.append.call_args.args[0]["event"] == "stage_intent",
    )

    restore_state = {"linked": False, "pending_removed": False}
    linked_stat = SimpleNamespace(**vars(expected_stat))
    linked_stat.st_nlink = 2
    restore_entry = {
        "candidate": candidates[0],
        "fd": 123,
        "identity": cleanup.file_identity(expected_stat),
        "state": "staged",
    }

    def restore_stat(_name, *, dir_fd, follow_symlinks=False):
        del follow_symlinks
        if dir_fd == 10:
            if not restore_state["linked"]:
                raise FileNotFoundError("original absent")
            return expected_stat if restore_state["pending_removed"] else linked_stat
        if dir_fd == 11 and not restore_state["pending_removed"]:
            return linked_stat if restore_state["linked"] else expected_stat
        raise FileNotFoundError("pending absent")

    def restore_fstat(_fd):
        if restore_state["linked"] and not restore_state["pending_removed"]:
            return linked_stat
        return expected_stat

    def restore_link(*_args, **_kwargs):
        restore_state["linked"] = True

    def restore_unlink(*_args, **_kwargs):
        restore_state["pending_removed"] = True

    full_journal = SimpleNamespace(
        append=mock.Mock(side_effect=OSError(errno.ENOSPC, "journal full"))
    )
    with (
        mock.patch.object(cleanup.os, "stat", side_effect=restore_stat),
        mock.patch.object(cleanup.os, "fstat", side_effect=restore_fstat),
        mock.patch.object(cleanup.os, "link", side_effect=restore_link) as link_spy,
        mock.patch.object(cleanup.os, "unlink", side_effect=restore_unlink) as restore_unlink_spy,
        mock.patch.object(cleanup, "fsync_directory", return_value=None),
    ):
        journal_full_restored, journal_full_errors = cleanup.restore_staged_files(
            [restore_entry], 10, 11, full_journal
        )
    check(
        results,
        "ENOSPC im Journal verhindert den Restore einer gestagten Datei nicht",
        restore_entry["state"] == "restored"
        and restore_state == {"linked": True, "pending_removed": True}
        and link_spy.call_count == 1
        and restore_unlink_spy.call_count == 1
        and len(journal_full_restored) == 1
        and journal_full_restored[0]["relative_path"] == "a.bin"
        and {item.get("phase") for item in journal_full_errors}
        == {
            "journal_restore_linked",
            "journal_restored",
        },
    )

    bootstrap_restore_state = {"hidden": True, "original": False}
    bootstrap_restore_entry = {
        "candidate": candidates[0],
        "fd": 123,
        "identity": cleanup.file_identity(expected_stat),
        "state": "hidden",
    }

    def bootstrap_restore_stat(name, *, dir_fd, follow_symlinks=False):
        del dir_fd, follow_symlinks
        if name == "hidden.pending" and bootstrap_restore_state["hidden"]:
            return expected_stat
        if name == "a.bin" and bootstrap_restore_state["original"]:
            return expected_stat
        raise FileNotFoundError(name)

    def bootstrap_restore_rename(source, target, **_kwargs):
        if source != "hidden.pending" or target != "a.bin":
            raise AssertionError("unerwartetes Bootstrap-Restore-Ziel")
        bootstrap_restore_state.update(hidden=False, original=True)

    bootstrap_full_journal = SimpleNamespace(
        append=mock.Mock(side_effect=OSError(errno.ENOSPC, "journal full"))
    )
    with (
        mock.patch.object(cleanup.os, "stat", side_effect=bootstrap_restore_stat),
        mock.patch.object(cleanup.os, "fstat", return_value=expected_stat),
        mock.patch.object(
            cleanup, "rename_noreplace", side_effect=bootstrap_restore_rename
        ) as bootstrap_restore_rename_spy,
        mock.patch.object(cleanup, "fsync_directory", return_value=None),
    ):
        bootstrap_restored, bootstrap_restore_errors = (
            cleanup.restore_bootstrap_hidden(
                bootstrap_restore_entry,
                10,
                "hidden.pending",
                bootstrap_full_journal,
            )
        )
    check(
        results,
        "Bootstrap-Restore per NOREPLACE laeuft unabhaengig von vollem Journal",
        bootstrap_restore_entry["state"] == "restored"
        and bootstrap_restore_state == {"hidden": False, "original": True}
        and bootstrap_restore_rename_spy.call_count == 1
        and len(bootstrap_restored) == 1
        and {item.get("phase") for item in bootstrap_restore_errors}
        == {"bootstrap_journal_restored"}
        and bootstrap_restore_rename_spy.call_count == 1,
    )

    execution_events = []
    receipt_writes = []
    expected_after = cleanup.expected_inventory_after(report, selected)

    class FakeExecutionDatabase:
        def __init__(self, _database_url):
            self.advisory_locked = False

        def acquire_advisory_lock(self):
            execution_events.append("advisory_lock")
            self.advisory_locked = True

        def release_table_locks(self):
            execution_events.append("release_table_locks")

        def close(self):
            execution_events.append("database_close")

    class FakeJournal:
        def __init__(self, _path, run_id, *, directory_fd=None):
            self.run_id = run_id
            self.directory_fd = directory_fd
            self.closed = False
            self.records = []

        def append(self, event):
            record = dict(event, run_id=self.run_id)
            self.records.append(record)
            return record

        def close(self):
            self.closed = True

        def checkpoint(self):
            if self.closed:
                raise cleanup.CleanupError("Journal geschlossen")
            return {
                "event_count": len(self.records),
                "last_event_sha256": "4" * 64,
                "journal_sha256": "5" * 64,
            }

        def summary(self):
            if not self.closed:
                raise cleanup.CleanupError("Journal noch offen")
            return {
                "event_count": len(self.records),
                "last_event_sha256": "4" * 64,
                "journal_sha256": "5" * 64,
            }

    def run_synthetic_bootstrap(
        *,
        fail_window_index=None,
        fired_after_failure=False,
        failure_exception=None,
    ):
        events = []
        signals = []
        writes = []
        persistent_receipts = []
        guarded_receipts = []
        state = {"original": True, "hidden": False, "candidate_fd_closed": False}
        window_calls = 0
        bootstrap_stat = SimpleNamespace(
            st_mode=__import__("stat").S_IFREG | 0o600,
            st_dev=1,
            st_ino=100,
            st_size=len(raw_a),
            st_mtime_ns=100,
            st_ctime_ns=200,
            st_nlink=1,
            st_blocks=20_000,
        )
        bootstrap_identity = cleanup.file_identity(bootstrap_stat)
        bootstrap_plan = json.loads(json.dumps(prepared_plan))
        bootstrap_plan["candidate_file_identity"] = list(bootstrap_identity)
        bootstrap_plan["candidate_allocated_bytes"] = 10_240_000
        bootstrap_plan["hidden_name"] = "x.bin"
        bootstrap_plan["plan_sha256"] = cleanup.canonical_sha256(
            {
                key: value
                for key, value in bootstrap_plan.items()
                if key != "plan_sha256"
            }
        )
        sufficient_capacity = {
            "available_bytes": 20_000_000,
            "available_inodes": 100,
            "required_ledger_bytes": plan_required_bytes,
            "required_ledger_inodes": cleanup.PERSISTENT_LEDGER_MIN_INODES,
            "sufficient_for_regular_cleanup": True,
        }
        insufficient_capacity = {
            **sufficient_capacity,
            "available_bytes": 0,
            "sufficient_for_regular_cleanup": False,
        }
        watchdog_process_identity = {
            "pid": 900,
            "ppid": os.getpid(),
            "starttime": 9000,
            "uid": 1000,
            "cmdline_sha256": "7" * 64,
            "cgroup_sha256": "8" * 64,
            "state": "S",
        }
        watchdog = {
            "process": SimpleNamespace(poll=lambda: None),
            "process_identity": watchdog_process_identity,
            "parent_identity": {
                **watchdog_process_identity,
                "pid": os.getpid(),
            },
            "token_sha256": "9" * 64,
            "started_monotonic": 0.0,
            "deadline_monotonic": 20.0,
            "cutoff_monotonic": 8.0,
            "ready_marker": {
                "run_id": plan_run_id,
                "marker_sha256": "a" * 64,
            },
        }

        class FakeBootstrapDatabase:
            def __init__(self, _database_url):
                self.advisory_locked = False

            def acquire_advisory_lock(self):
                events.append("advisory_lock")
                self.advisory_locked = True

            def release_table_locks(self):
                events.append("release_table_locks")

            def close(self):
                events.append("database_close")

        def fake_scan(_root):
            events.append("post_scan" if state["hidden"] or not state["original"] else "source_scan")
            return expected_after if state["hidden"] or not state["original"] else report["source_inventory"]

        def fake_preflight(_database_url, _report, _selected):
            events.append("database_preflight")
            return {
                "row_signatures": [{"datei_id": 42}],
                "row_signatures_sha256": "6" * 64,
                "row_lock_projection": [{"datei_id": 42}],
            }

        def fake_path_state(_root_fd, name, held_fd):
            if state["candidate_fd_closed"]:
                events.append("closed_fd_path_state")
            exists = state["hidden"] if name == "x.bin" else state["original"]
            return {
                "relative_path": name,
                "exists": exists,
                "regular_file": exists,
                "file_identity": list(bootstrap_identity) if exists else None,
                "matches_held_fd": bool(exists and held_fd is not None),
            }

        def fake_window(*_args, **_kwargs):
            nonlocal window_calls
            current = window_calls
            window_calls += 1
            events.append(f"watchdog_window_{current}")
            if fail_window_index == current:
                raise (
                    failure_exception
                    if failure_exception is not None
                    else cleanup.WatchdogUnsafeError(
                        "simulierter Watchdog/Cutoff-Abbruch"
                    )
                )
            stopped = {
                role: {**identity, "state": "T"}
                for role, identity in plan_processes.items()
            }
            return {
                "checked_monotonic": float(current),
                "remaining_seconds": 15.0,
                "watchdog_fired": False,
                "processes": stopped,
            }

        def fake_pidfd_send(pidfd, sig):
            signals.append((pidfd, int(sig)))

        def fake_rename(source, target, **kwargs):
            if (
                source != "a.bin"
                or target != "x.bin"
                or kwargs
                != {"source_dir_fd": 100, "target_dir_fd": 100}
                or not state["original"]
                or state["hidden"]
            ):
                raise AssertionError("unerwarteter synthetischer Bootstrap-Rename")
            events.append("rename_hidden")
            state.update(original=False, hidden=True)

        def fake_unlink(name, *, dir_fd):
            if name != "x.bin" or dir_fd != 100 or not state["hidden"]:
                raise AssertionError("unerwarteter synthetischer Bootstrap-Unlink")
            events.append("unlink_hidden")
            state["hidden"] = False

        def fake_close_entry(entry, *, strict=False):
            del strict
            if entry.get("fd") is not None:
                events.append("candidate_close")
                entry["fd"] = None
                state["candidate_fd_closed"] = True

        def fake_capacity(_root_fd, _required_bytes):
            if not state["hidden"] and not state["original"]:
                events.append(
                    "capacity_after_fd_close"
                    if state["candidate_fd_closed"]
                    else "capacity_before_fd_close"
                )
                return sufficient_capacity
            events.append("capacity_before_mutation")
            return insufficient_capacity

        def fake_atomic(_directory_fd, name, value):
            writes.append((name, json.loads(json.dumps(value))))

        def fake_persistent(run_id, root_identity, receipt):
            if not state["candidate_fd_closed"]:
                raise AssertionError("Persistenter Beleg wurde vor Kandidaten-FD-close angelegt")
            events.append("persistent_receipt")
            persistent_receipts.append(json.loads(json.dumps(receipt)))
            return (
                cleanup.BOOTSTRAP_PERSISTENT_ROOT / f"run-{run_id}",
                202,
                (root_identity[0], 12),
            )

        def fake_guard(_directory_fd, name, value, **kwargs):
            events.append("guarded_terminal_receipt")
            guarded_receipts.append((name, json.loads(json.dumps(value)), kwargs))
            return sufficient_capacity

        def fake_restore(entry, _root_fd, _hidden_name, _journal):
            events.append("restore_hidden")
            state.update(original=True, hidden=False)
            entry["state"] = "restored"
            return ([{"relative_path": "a.bin", "size": len(raw_a)}], [])

        restore_spy = mock.Mock(side_effect=fake_restore)
        with (
            mock.patch.multiple(
                cleanup,
                require_pidfd_support=lambda: None,
                bootstrap_hidden_name=lambda *_args: "x.bin",
                open_upload_root=lambda _root: (100, (1, 10)),
                open_prepared_bootstrap_plan=lambda *_args: (
                    bootstrap_plan,
                    pathlib.Path("/tmp/fake-bootstrap") / f"run-{plan_run_id}",
                    101,
                    (2, 20),
                ),
                ensure_persistent_ledger_capacity=lambda *_args: {},
                JournalWriter=FakeJournal,
                LockedPostgres=FakeBootstrapDatabase,
                reconcile_pending_runs=lambda *_args: [],
                capacity_snapshot=fake_capacity,
                assert_root_binding=lambda *_args: None,
                scan_inventory=fake_scan,
                preflight_database=fake_preflight,
                open_verified_candidate=lambda *_args: (200, bootstrap_identity),
                bootstrap_path_state=fake_path_state,
                assert_process_topology=lambda *_args, **_kwargs: plan_processes,
                assert_pidfd_identity=lambda role, identity, pidfd: identity,
                pidfd_send=fake_pidfd_send,
                start_bootstrap_watchdog=lambda *_args: watchdog,
                wait_for_stopped_process=lambda identity: identity,
                assert_watchdog_window=fake_window,
                verify_locked_database=lambda *_args: events.append("share_nowait"),
                quick_inventory_metadata_fd=lambda _fd: cleanup.expected_inventory_metadata(report),
                rename_noreplace=fake_rename,
                fsync_directory=lambda *_args: None,
                close_entry=fake_close_entry,
                write_bootstrap_persistent_receipt=fake_persistent,
                assert_pending_binding=lambda *_args: None,
                atomic_json_at=fake_atomic,
                atomic_json_at_with_capacity_guard=fake_guard,
                watchdog_fired=lambda *_args: bool(
                    fired_after_failure and fail_window_index is not None
                ),
                restore_bootstrap_hidden=restore_spy,
            ),
            mock.patch.object(cleanup, "BOOTSTRAP_CANDIDATE_ALLOCATED_BYTES", 10_240_000),
            mock.patch.object(
                cleanup,
                "BOOTSTRAP_CANDIDATE_REFERENCE",
                selected[0]["references"][0],
            ),
            mock.patch.object(cleanup.os, "fstat", return_value=bootstrap_stat),
            mock.patch.object(cleanup.os, "stat", return_value=bootstrap_stat),
            mock.patch.object(
                cleanup.os,
                "pidfd_open",
                side_effect=lambda pid, _flags: 301 if pid == 100 else 302,
                create=True,
            ),
            mock.patch.object(cleanup.os, "unlink", side_effect=fake_unlink),
            mock.patch.object(cleanup.os, "close"),
            mock.patch.object(cleanup.signal, "SIGSTOP", 19, create=True),
            mock.patch.object(cleanup.signal, "SIGCONT", 18, create=True),
        ):
            try:
                receipt = cleanup.execute_bootstrap_canary(
                    report,
                    selected[0],
                    [],
                    cleanup.EXPECTED_UPLOAD_ROOT,
                    "postgresql://test",
                    run_id=plan_run_id,
                    expected_plan_sha256=bootstrap_plan["plan_sha256"],
                )
            except cleanup.CleanupRunError as exc:
                receipt = exc.details
                error = exc
            else:
                error = None
        return {
            "receipt": receipt,
            "error": error,
            "events": events,
            "signals": signals,
            "writes": writes,
            "persistent_receipts": persistent_receipts,
            "guarded_receipts": guarded_receipts,
            "state": state,
            "restore_calls": restore_spy.call_count,
        }

    bootstrap_success = run_synthetic_bootstrap()
    success_stop_resume_signals = [
        item
        for item in bootstrap_success["signals"]
        if item[1] in (19, 18)
    ]
    check(
        results,
        "Bootstrap-EXECUTE schliesst FD vor statvfs, persistiert sofort und setzt Worker/Master frei",
        bootstrap_success["error"] is None
        and bootstrap_success["receipt"]["status"] == "bootstrap_complete"
        and bootstrap_success["receipt"]["candidate_fd_closed"] is True
        and bootstrap_success["receipt"]["deleted_count"] == 1
        and bootstrap_success["state"]
        == {"original": False, "hidden": False, "candidate_fd_closed": True}
        and success_stop_resume_signals
        == [(301, 19), (302, 19), (302, 18), (301, 18)]
        and bootstrap_success["events"].index("candidate_close")
        < bootstrap_success["events"].index("capacity_after_fd_close")
        < bootstrap_success["events"].index("persistent_receipt")
        < bootstrap_success["events"].index("closed_fd_path_state")
        < bootstrap_success["events"].index("post_scan")
        and "capacity_before_fd_close" not in bootstrap_success["events"]
        and len(bootstrap_success["persistent_receipts"]) == 1
        and bootstrap_success["persistent_receipts"][0]["status"]
        == "bootstrap_storage_released"
        and len(bootstrap_success["guarded_receipts"]) == 1
        and bootstrap_success["guarded_receipts"][0][0]
        == ".bootstrap-receipt.json"
        and bootstrap_success["guarded_receipts"][0][1]["status"]
        == "bootstrap_complete"
        and bootstrap_success["receipt"]["persistent_receipt_path"]
        == str(
            cleanup.BOOTSTRAP_PERSISTENT_ROOT
            / f"run-{plan_run_id}"
            / ".bootstrap-receipt.json"
        ),
    )

    bootstrap_cutoff = run_synthetic_bootstrap(fail_window_index=0)
    cutoff_stop_resume_signals = [
        item for item in bootstrap_cutoff["signals"] if item[1] in (19, 18)
    ]
    check(
        results,
        "Cutoff vor Rename mutiert keinen Upload und SIGCONT laeuft trotzdem im finally",
        bootstrap_cutoff["error"] is not None
        and bootstrap_cutoff["receipt"]["status"] == "bootstrap_failed_no_deletion"
        and bootstrap_cutoff["state"]["original"] is True
        and bootstrap_cutoff["state"]["hidden"] is False
        and "rename_hidden" not in bootstrap_cutoff["events"]
        and "unlink_hidden" not in bootstrap_cutoff["events"]
        and cutoff_stop_resume_signals
        == [(301, 19), (302, 19), (302, 18), (301, 18)],
    )

    bootstrap_fired = run_synthetic_bootstrap(
        fail_window_index=3,
        fired_after_failure=True,
    )
    fired_stop_resume_signals = [
        item for item in bootstrap_fired["signals"] if item[1] in (19, 18)
    ]
    check(
        results,
        "Ausgeloester Watchdog nach Rename verhindert Unlink und Restore, belegt Hidden-Zustand und resumed",
        bootstrap_fired["error"] is not None
        and bootstrap_fired["receipt"]["status"]
        == "bootstrap_recovery_required"
        and bootstrap_fired["state"]["original"] is False
        and bootstrap_fired["state"]["hidden"] is True
        and "rename_hidden" in bootstrap_fired["events"]
        and "unlink_hidden" not in bootstrap_fired["events"]
        and bootstrap_fired["restore_calls"] == 0
        and bootstrap_fired["receipt"]["hidden_state"]["exists"] is True
        and bootstrap_fired["receipt"]["original_state"]["exists"] is False
        and fired_stop_resume_signals
        == [(301, 19), (302, 19), (302, 18), (301, 18)],
    )

    bootstrap_interrupt = run_synthetic_bootstrap(
        fail_window_index=3,
        failure_exception=KeyboardInterrupt(),
    )
    interrupt_stop_resume_signals = [
        item for item in bootstrap_interrupt["signals"] if item[1] in (19, 18)
    ]
    check(
        results,
        "KeyboardInterrupt nach Rename wird restauriert, terminal belegt und pidfd-resumed",
        bootstrap_interrupt["error"] is not None
        and bootstrap_interrupt["receipt"]["status"]
        == "bootstrap_failed_no_deletion"
        and bootstrap_interrupt["receipt"]["error_type"] == "KeyboardInterrupt"
        and bootstrap_interrupt["state"]["original"] is True
        and bootstrap_interrupt["state"]["hidden"] is False
        and bootstrap_interrupt["restore_calls"] == 1
        and "restore_hidden" in bootstrap_interrupt["events"]
        and any(
            value.get("status") == "bootstrap_failed_no_deletion"
            for _name, value in bootstrap_interrupt["writes"]
        )
        and interrupt_stop_resume_signals
        == [(301, 19), (302, 19), (302, 18), (301, 18)],
    )

    zero_open_pending = mock.Mock()
    zero_open_fallback = mock.Mock(
        return_value=(pathlib.Path("/tmp/fake-zero-space-run"), 101, (1, 11))
    )
    zero_database_instance = mock.Mock()
    zero_database = mock.Mock(return_value=zero_database_instance)
    zero_stage = mock.Mock()
    zero_unlink = mock.Mock()
    zero_rename = mock.Mock()
    zero_os_unlink = mock.Mock()
    zero_receipt_writes = []

    def fake_zero_atomic(_directory_fd, name, value):
        zero_receipt_writes.append((name, json.loads(json.dumps(value))))

    with (
        mock.patch.multiple(
            cleanup,
            LockedPostgres=zero_database,
            JournalWriter=FakeJournal,
            open_upload_root=lambda _root: (100, (1, 10)),
            reconcile_pending_runs=lambda *_args: [],
            open_pending_run=zero_open_pending,
            open_fallback_run=zero_open_fallback,
            assert_root_binding=lambda *_args: None,
            atomic_json_at=fake_zero_atomic,
            scan_inventory=lambda _root: report["source_inventory"],
            stage_selected_files=zero_stage,
            unlink_staged_files=zero_unlink,
        ),
        mock.patch.object(
            cleanup.os,
            "fstatvfs",
            return_value=SimpleNamespace(f_bavail=0, f_frsize=4096, f_favail=0),
            create=True,
        ),
        mock.patch.object(cleanup.os, "rename", zero_rename),
        mock.patch.object(cleanup.os, "unlink", zero_os_unlink),
        mock.patch.object(cleanup.os, "close", return_value=None),
    ):
        try:
            cleanup.execute_cleanup(
                report,
                selected,
                pathlib.Path("/var/data/uploads"),
                "postgresql://test",
            )
        except cleanup.CleanupRunError as exc:
            zero_space_details = exc.details
        else:
            zero_space_details = None
    check(
        results,
        "Null freier Persistenzspeicher stoppt vor Halb-Run und Upload-Mutation",
        zero_space_details is not None
        and zero_space_details["status"] == "cleanup_failed_no_deletion"
        and zero_space_details["deleted_count"] == 0
        and zero_space_details["staged_remaining_count"] == 0
        and zero_space_details["receipt_storage"] == "temporary_fallback"
        and zero_space_details["ledger_capacity"] is None
        and "available_bytes=0" in zero_space_details["error"]
        and zero_open_pending.call_count == 0
        and zero_open_fallback.call_count == 1
        and zero_database.call_count == 1
        and zero_database_instance.acquire_advisory_lock.call_count == 1
        and zero_database_instance.close.call_count == 1
        and zero_stage.call_count == 0
        and zero_unlink.call_count == 0
        and zero_rename.call_count == 0
        and zero_os_unlink.call_count == 0
        and zero_receipt_writes[-1][0] == ".cleanup-receipt.json",
    )

    with mock.patch.object(
        cleanup.os,
        "fstatvfs",
        return_value=SimpleNamespace(f_bavail=2047, f_frsize=4096, f_favail=8),
        create=True,
    ):
        insufficient_physical_capacity = cleanup.capacity_snapshot(
            100, cleanup.PERSISTENT_LEDGER_MIN_BYTES
        )
    check(
        results,
        "Bootstrap-Postcheck bewertet reale statvfs-Bytes statt logischer Dateigroesse",
        insufficient_physical_capacity["available_bytes"]
        == 2047 * 4096
        and not insufficient_physical_capacity["sufficient_for_regular_cleanup"],
    )

    scan_values = iter([report["source_inventory"], expected_after])
    quick_values = iter(
        [
            cleanup.expected_inventory_metadata(report),
            cleanup.expected_inventory_metadata({"source_inventory": expected_after}),
            cleanup.expected_inventory_metadata({"source_inventory": expected_after}),
        ]
    )
    next_fd = iter(range(200, 200 + len(selected)))

    def fake_scan(_root):
        value = next(scan_values)
        execution_events.append(
            "source_scan" if value == report["source_inventory"] else "post_scan"
        )
        return value

    def fake_preflight(_database_url, _report, _selected):
        execution_events.append("heavy_preflight")
        return {
            "row_signatures": [{"datei_id": 42}],
            "row_signatures_sha256": "2" * 64,
            "row_lock_projection": [{"datei_id": 42}],
        }

    def fake_open_candidate(_root_fd, candidate):
        execution_events.append("file_hash_preflight")
        return next(next_fd), (
            1,
            100 + int(candidate["references"][0]["row_id"]),
            int(candidate["size"]),
            int(candidate["mtime_ns"]),
            300,
            1,
        )

    def fake_locked_verify(database, _report, _selected, _preflight):
        if not database.advisory_locked:
            raise AssertionError("Tabellenpruefung ohne Advisory-Lock")
        execution_events.append("share_nowait_verify")

    def fake_stage(opened, _root_fd, _pending_fd, journal):
        execution_events.append("stage")
        for entry in opened:
            entry["state"] = "staged"
            journal.append({"event": "staged", **cleanup.evidence_for(entry)})
        return list(opened)

    def fake_unlink(staged, _pending_fd, journal):
        execution_events.append("unlink_pending")
        deleted = []
        for entry in staged:
            evidence = cleanup.evidence_for(entry)
            entry["state"] = "deleted"
            entry["fd"] = None
            deleted.append(evidence)
            journal.append({"event": "unlinked_pending", **evidence})
        return deleted

    def fake_atomic_json(_directory_fd, name, value):
        receipt_writes.append((name, json.loads(json.dumps(value))))
        if value.get("status") == "cleanup_mutation_armed":
            execution_events.append("mutation_armed")

    with mock.patch.multiple(
        cleanup,
        LockedPostgres=FakeExecutionDatabase,
        JournalWriter=FakeJournal,
        open_upload_root=lambda _root: (100, (1, 10)),
        reconcile_pending_runs=lambda *_args: [],
        ensure_persistent_ledger_capacity=lambda *_args: {
            "available_bytes_before_run": 10_000_000,
            "available_inodes_before_run": 100,
            "required_ledger_bytes": 1_000_000,
            "required_ledger_inodes": 8,
        },
        open_pending_run=lambda _root, _identity, _run_id, _setup_receipt: (
            pathlib.Path("/fake/pending") / f"run-{_run_id}",
            101,
            (1, 11),
        ),
        assert_root_binding=lambda *_args: None,
        assert_pending_binding=lambda *_args: None,
        atomic_json_at=fake_atomic_json,
        scan_inventory=fake_scan,
        preflight_database=fake_preflight,
        open_verified_candidate=fake_open_candidate,
        verify_locked_database=fake_locked_verify,
        quick_inventory_metadata_fd=lambda _fd: next(quick_values),
        stage_selected_files=fake_stage,
        unlink_staged_files=fake_unlink,
        close_entry=lambda entry: entry.update(fd=None),
    ):
        success_receipt = cleanup.execute_cleanup(
            report, selected, pathlib.Path("/var/data/uploads"), "postgresql://test"
        )
    check(
        results,
        "Execute-Erfolg loescht exakt den freigegebenen Kandidaten nach allen Preflights",
        success_receipt["status"] == "cleanup_complete"
        and success_receipt["deleted_count"] == 1
        and success_receipt["deleted"][0]["relative_path"] == "a.bin"
        and success_receipt["database_writes"] == 0
        and success_receipt["after_inventory"]["inventory_sha256"]
        == expected_after["inventory_sha256"]
        and success_receipt["journal_sha256"] == "5" * 64
        and receipt_writes[-1][0] == ".cleanup-receipt.json"
        and execution_events.index("advisory_lock")
        < execution_events.index("heavy_preflight")
        < execution_events.index("file_hash_preflight")
        < execution_events.index("share_nowait_verify")
        < execution_events.index("mutation_armed")
        < execution_events.index("stage")
        < execution_events.index("unlink_pending")
        < execution_events.index("release_table_locks")
        < execution_events.index("post_scan")
        < execution_events.index("database_close"),
    )

    partial_candidates = [
        candidates[0],
        {
            **files[1],
            "references": [
                {"table": "dateien", "column": "stored_name", "row_id": 43}
            ],
        },
    ]
    partial_report = build_report(files, partial_candidates)
    partial_selected = cleanup.select_candidates(partial_report, 0)
    partial_after_first = cleanup.expected_inventory_after(
        partial_report, [partial_selected[0]]
    )
    partial_writes = []
    execution_events = []
    partial_scan_values = iter(
        [partial_report["source_inventory"], partial_after_first]
    )
    partial_quick_values = iter(
        [
            cleanup.expected_inventory_metadata(partial_report),
            cleanup.expected_inventory_metadata(
                {
                    "source_inventory": cleanup.expected_inventory_after(
                        partial_report, partial_selected
                    )
                }
            ),
        ]
    )
    partial_fd_values = iter([300, 301])

    def fake_partial_scan(_root):
        return next(partial_scan_values)

    def fake_partial_preflight(_database_url, _report, _selected):
        execution_events.append("heavy_preflight")
        rows = [{"datei_id": value} for value in (42, 43)]
        return {
            "row_signatures": rows,
            "row_signatures_sha256": "6" * 64,
            "row_lock_projection": rows,
        }

    def fake_partial_open(_root_fd, candidate):
        return next(partial_fd_values), (
            1,
            100 + int(candidate["references"][0]["row_id"]),
            int(candidate["size"]),
            int(candidate["mtime_ns"]),
            300,
            1,
        )

    def fake_partial_unlink(staged, _pending_fd, journal):
        first = staged[0]
        first_evidence = cleanup.evidence_for(first)
        first["state"] = "deleted"
        first["fd"] = None
        journal.append({"event": "unlinked_pending", **first_evidence})
        raise OSError("simulierter Fehler nach erstem unlink")

    def fake_partial_restore(staged, _root_fd, _pending_fd, journal):
        restored_items = []
        for entry in staged:
            if entry.get("state") != "staged":
                continue
            evidence = cleanup.evidence_for(entry)
            entry["state"] = "restored"
            restored_items.append(evidence)
            journal.append({"event": "restored", **evidence})
        return restored_items, []

    def fake_partial_atomic(_directory_fd, name, value):
        partial_writes.append((name, json.loads(json.dumps(value))))

    with mock.patch.multiple(
        cleanup,
        LockedPostgres=FakeExecutionDatabase,
        JournalWriter=FakeJournal,
        open_upload_root=lambda _root: (100, (1, 10)),
        reconcile_pending_runs=lambda *_args: [],
        ensure_persistent_ledger_capacity=lambda *_args: {
            "available_bytes_before_run": 10_000_000,
            "available_inodes_before_run": 100,
            "required_ledger_bytes": 1_000_000,
            "required_ledger_inodes": 8,
        },
        open_pending_run=lambda _root, _identity, _run_id, _setup_receipt: (
            pathlib.Path("/fake/pending") / f"run-{_run_id}",
            101,
            (1, 11),
        ),
        assert_root_binding=lambda *_args: None,
        assert_pending_binding=lambda *_args: None,
        atomic_json_at=fake_partial_atomic,
        scan_inventory=fake_partial_scan,
        preflight_database=fake_partial_preflight,
        open_verified_candidate=fake_partial_open,
        verify_locked_database=fake_locked_verify,
        quick_inventory_metadata_fd=lambda _fd: next(partial_quick_values),
        stage_selected_files=fake_stage,
        unlink_staged_files=fake_partial_unlink,
        restore_staged_files=fake_partial_restore,
        pending_evidence=lambda _staged, _pending_fd: ([], []),
        close_entry=lambda entry: entry.update(fd=None),
    ):
        try:
            cleanup.execute_cleanup(
                partial_report,
                partial_selected,
                pathlib.Path("/var/data/uploads"),
                "postgresql://test",
            )
        except cleanup.CleanupRunError as exc:
            partial_details = exc.details
        else:
            partial_details = None
    partial_receipt = partial_writes[-1][1] if partial_writes else {}
    partial_deleted = partial_details.get("deleted", []) if partial_details else []
    partial_restored = partial_details.get("restored", []) if partial_details else []
    partial_contract = (
        partial_details is not None
        and partial_details == partial_receipt
        and partial_details["status"] == "cleanup_partial"
        and partial_details["database_writes"] == 0
        and partial_details.get("selected_count") == 2
        and partial_details.get("deleted_count") == 1
        and len(partial_deleted) == 1
        and partial_deleted[0]["relative_path"]
        == partial_selected[0]["relative_path"]
        and partial_details.get("restored_count") == 1
        and len(partial_restored) == 1
        and partial_restored[0]["relative_path"]
        == partial_selected[1]["relative_path"]
        and partial_details.get("staged_remaining_count") == 0
        and partial_details["staged_remaining"] == []
        and partial_details.get("uncertain_count") == 0
        and partial_details["uncertain"] == []
        and partial_details["event_count"] > 0
        and partial_details["last_event_sha256"] == "4" * 64
        and partial_details["journal_sha256"] == "5" * 64
        and partial_details["error_type"] == "OSError"
        and partial_writes[-1][0] == ".cleanup-receipt.json"
    )
    if not partial_contract:
        print("PARTIAL-DETAILS " + json.dumps(partial_details, sort_keys=True))
    check(
        results,
        "Fehler nach erstem Unlink erzeugt vollstaendigen Partial-Receipt",
        partial_contract,
    )

    for path in (
        audit_path,
        tampered_path,
        unsafe_path,
        not_read_only_path,
        *contract_paths,
    ):
        path.unlink(missing_ok=True)
    failed = sum(not item for item in results)
    print(
        f"== ERGEBNIS: {len(results) - failed}/{len(results)} Checks bestanden; "
        f"{skipped} uebersprungen =="
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
