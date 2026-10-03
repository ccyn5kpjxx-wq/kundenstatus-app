"""Delete only disk uploads proven identical to PostgreSQL ``datei_backups``.

This is a deliberately narrow one-shot Render maintenance tool. It consumes a
fresh report from ``render_upload_blob_audit.py`` and acquires the same
PostgreSQL advisory lock as the application's destructive import path. Slow
blob and file hashing happens before the short ``SHARE NOWAIT`` table-lock
window. Exact files are then atomically moved into a private same-filesystem
directory, checked by inode, and unlinked only from there. No database row is
changed.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import hmac
import json
import os
import pathlib
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone

from render_upload_blob_audit import (
    AUDIT_FORMAT,
    AuditError,
    ReadOnlyPostgres,
    canonical_sha256,
    discover_references,
    scan_inventory,
    verify_blob,
)


CLEANUP_FORMAT = "gaertner-render-upload-cleanup-v1"
CLEANUP_ROOT = pathlib.Path(tempfile.gettempdir()) / "gaertner-storage-cleanup-v1"
EXPECTED_UPLOAD_ROOT = pathlib.Path("/var/data/uploads")
PENDING_ROOT = EXPECTED_UPLOAD_ROOT.parent / ".gaertner-upload-cleanup-v1"
PORTAL_ORIGINALS_ADVISORY_LOCK_KEY = 657274620261003
APPROVAL_PHRASE = "DELETE_VERIFIED_DATEI_DUPLICATES"
PERSISTENT_LEDGER_MIN_BYTES = 8 * 1024 * 1024
PERSISTENT_LEDGER_MIN_INODES = 8
RENAME_NOREPLACE = 1
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
IDENTIFIER_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")
RUN_DIRECTORY_PATTERN = re.compile(r"run-([0-9a-f]{32})\Z")
ATOMIC_RECEIPT_PART_PATTERN = re.compile(
    r"\.\.cleanup-receipt\.json\.[0-9a-f]{32}\.part\Z"
)
ATOMIC_PLAN_PART_PATTERN = re.compile(
    r"\.\.cleanup-plan\.json\.[0-9a-f]{32}\.part\Z"
)
SETUP_UNCERTAINTY = {
    "phase": "setup",
    "error": "Terminaler Cleanup-Beleg wurde noch nicht geschrieben.",
}
BOOTSTRAP_PLAN_FORMAT = "gaertner-zero-space-bootstrap-plan-v1"
BOOTSTRAP_MARKER_FORMAT = "gaertner-zero-space-bootstrap-watchdog-v1"
BOOTSTRAP_RECEIPT_FORMAT = "gaertner-zero-space-bootstrap-receipt-v1"
BOOTSTRAP_APPROVAL_PHRASE = "DELETE_ONE_VERIFIED_DATEI_DUPLICATE_1101"
BOOTSTRAP_TEMP_ROOT = (
    pathlib.Path(tempfile.gettempdir()) / "gaertner-zero-space-bootstrap-v1"
)
BOOTSTRAP_PERSISTENT_ROOT = (
    EXPECTED_UPLOAD_ROOT.parent / ".gaertner-zero-space-bootstrap-v1"
)
BOOTSTRAP_WATCHDOG_SECONDS = 20.0
BOOTSTRAP_MUTATION_CUTOFF_SECONDS = 8.0
BOOTSTRAP_MIN_REMAINING_SECONDS = 12.0
BOOTSTRAP_CANDIDATE_PATH = "375b778a6df149ad963b1e42bb8de204.pdf"
BOOTSTRAP_CANDIDATE_SHA256 = (
    "3e8bb872dd8845b0ffe8981912242b2943117ffa7dfcbbcc72d8cd274e2af594"
)
BOOTSTRAP_CANDIDATE_SIZE = 15_281_466
BOOTSTRAP_CANDIDATE_ALLOCATED_BYTES = 15_282_176
BOOTSTRAP_CANDIDATE_REFERENCE = {
    "table": "dateien",
    "column": "stored_name",
    "row_id": 1101,
}
BOOTSTRAP_SOURCE_INVENTORY = {
    "file_count": 557,
    "total_file_bytes": 953_348_048,
    "inventory_sha256": "08bac282e2bc8061309f00d2605be9c0018420dbd236c9f056d097694f00e5a5",
}
BOOTSTRAP_EXPECTED_AFTER = {
    "file_count": 556,
    "total_file_bytes": 938_066_582,
    "inventory_sha256": "23fd8edd443a85cf329878b6b8e7a25a8349f8f1d6c37a09128c59ca07331837",
}
BOOTSTRAP_REMAINING_CANDIDATES = {
    "candidate_count": 479,
    "candidate_bytes": 863_752_566,
    "candidate_blob_count": 479,
    "candidate_index_sha256": "83af66fa382797fcb53a0649e867867e427e6effe0a72205f29e3a65b8aea71f",
}
BOOTSTRAP_LOCAL_EVIDENCE = {
    "master_manifest_sha256": "7cca1e62d44188faee5ddfecaebce01b8ddd2f7ab3f7fe29abe3d2a5dc96a6a2",
    "verification_report_sha256": "840e550058a6ab46b4afffad5f858dd937a7052994cdc89f1f94b5b20992119a",
    "expected_coverage_sha256": "e7575bf8e1b0542c4587e1a322233d7517c0ba3fa74ef32a6ee749bb64a63f40",
}


class CleanupError(RuntimeError):
    pass


class CleanupRunError(CleanupError):
    def __init__(self, message: str, details: dict):
        super().__init__(message)
        self.details = details


class WatchdogUnsafeError(CleanupError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_hash(label: str, value: str) -> str:
    cleaned = str(value or "").strip().lower()
    if not SHA256_PATTERN.fullmatch(cleaned):
        raise CleanupError(f"{label} ist keine SHA-256-Pruefsumme.")
    return cleaned


def inventory_digest(files: list[dict]) -> str:
    rows = [
        [item["relative_path"], int(item["size"]), item["sha256"]]
        for item in files
    ]
    return hashlib.sha256(
        json.dumps(rows, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def candidate_index(candidates: list[dict]) -> list[list]:
    return [
        [
            item["relative_path"],
            int(item["size"]),
            item["sha256"],
            [
                [reference["table"], reference["column"], int(reference["row_id"])]
                for reference in item["references"]
            ],
        ]
        for item in candidates
    ]


def candidate_summary(candidates: list[dict]) -> dict:
    index = candidate_index(candidates)
    return {
        "candidate_count": len(candidates),
        "candidate_bytes": sum(int(item["size"]) for item in candidates),
        "candidate_blob_count": sum(len(item["references"]) for item in candidates),
        "candidate_blob_bytes": sum(
            int(item["size"]) * len(item["references"]) for item in candidates
        ),
        "candidate_index_sha256": canonical_sha256(index),
    }


def load_and_validate_audit(path: pathlib.Path, args) -> dict:
    expected_audit_root = pathlib.Path(tempfile.gettempdir()) / "gaertner-storage-audit-v1"
    resolved = path.resolve()
    if resolved.parent != expected_audit_root.resolve() or not resolved.is_file():
        raise CleanupError("Auditdatei liegt nicht im erwarteten temporaeren Verzeichnis.")
    try:
        report = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise CleanupError("Auditdatei ist nicht lesbar.") from exc
    if not isinstance(report, dict) or report.get("format") != AUDIT_FORMAT:
        raise CleanupError("Auditformat ist ungueltig.")
    if report.get("mode") != "strictly_read_only" or pathlib.Path(
        str(report.get("upload_root") or "")
    ) != EXPECTED_UPLOAD_ROOT:
        raise CleanupError("Audit stammt nicht aus dem freigegebenen Read-only-Upload-Root.")
    claimed_audit_hash = validate_hash("Audit-Hash", report.get("audit_sha256"))
    canonical = dict(report)
    canonical.pop("audit_sha256", None)
    actual_audit_hash = canonical_sha256(canonical)
    expected_audit_hash = validate_hash(
        "Erwarteter Audit-Hash", args.expected_audit_sha256
    )
    if not (
        hmac.compare_digest(claimed_audit_hash, actual_audit_hash)
        and hmac.compare_digest(actual_audit_hash, expected_audit_hash)
    ):
        raise CleanupError("Audit-Pruefsumme stimmt nicht.")

    required_true = (
        "candidate_baseline_matches",
        "reference_columns_coverage_matches",
        "inventory_rechecked_unchanged",
        "deletion_evidence_complete",
    )
    if any(report.get(key) is not True for key in required_true):
        raise CleanupError("Audit hat nicht alle vier Sicherheitsfreigaben.")
    try:
        database_writes = int(report.get("database_writes"))
        server_files_deleted = int(report.get("server_files_deleted"))
    except (TypeError, ValueError) as exc:
        raise CleanupError("Audit-Ausgangszustand ist nicht strikt read-only.") from exc
    if (
        database_writes != 0
        or server_files_deleted != 0
        or report.get("delete_approved") is not False
    ):
        raise CleanupError("Audit-Ausgangszustand ist nicht strikt read-only.")

    inventory = report.get("source_inventory")
    if not isinstance(inventory, dict) or not isinstance(inventory.get("files"), list):
        raise CleanupError("Audit-Inventar fehlt.")
    files = inventory["files"]
    names = []
    for item in files:
        if not isinstance(item, dict):
            raise CleanupError("Audit-Inventar enthaelt eine fehlerhafte Zeile.")
        name = str(item.get("relative_path") or "")
        digest = validate_hash("Datei-Hash", item.get("sha256"))
        if pathlib.PurePath(name).name != name or not name or "/" in name or "\\" in name:
            raise CleanupError("Audit-Inventar enthaelt einen unsicheren Dateinamen.")
        try:
            size = int(item.get("size"))
            mtime_ns = int(item.get("mtime_ns"))
        except (TypeError, ValueError) as exc:
            raise CleanupError("Audit-Inventar enthaelt ungueltige Metadaten.") from exc
        if size < 0 or mtime_ns < 0:
            raise CleanupError("Audit-Inventar enthaelt ungueltige Metadaten.")
        item.update(size=size, mtime_ns=mtime_ns, sha256=digest)
        names.append(name)
    if names != sorted(names) or len(names) != len(set(names)):
        raise CleanupError("Audit-Inventar ist nicht eindeutig sortiert.")
    actual_inventory_hash = inventory_digest(files)
    expected_inventory_hash = validate_hash(
        "Erwarteter Inventar-Hash", args.expected_inventory_sha256
    )
    if (
        int(inventory.get("file_count") or -1) != int(args.expected_files)
        or int(inventory.get("total_file_bytes") or -1) != int(args.expected_bytes)
        or not hmac.compare_digest(
            validate_hash("Inventar-Hash", inventory.get("inventory_sha256")),
            actual_inventory_hash,
        )
        or not hmac.compare_digest(actual_inventory_hash, expected_inventory_hash)
    ):
        raise CleanupError("Audit-Inventar weicht von der freigegebenen Baseline ab.")

    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        raise CleanupError("Audit-Kandidaten fehlen.")
    files_by_name = {item["relative_path"]: item for item in files}
    seen_candidates = set()
    for item in candidates:
        if not isinstance(item, dict):
            raise CleanupError("Audit-Kandidaten enthalten eine fehlerhafte Zeile.")
        name = str(item.get("relative_path") or "")
        source = files_by_name.get(name)
        if not source or name in seen_candidates:
            raise CleanupError("Audit-Kandidat ist nicht eindeutig im Inventar enthalten.")
        seen_candidates.add(name)
        digest = validate_hash("Kandidaten-Hash", item.get("sha256"))
        if int(item.get("size") or -1) != source["size"] or not hmac.compare_digest(
            digest, source["sha256"]
        ):
            raise CleanupError("Audit-Kandidat stimmt nicht mit dem Inventar ueberein.")
        references = item.get("references")
        if not isinstance(references, list) or not references:
            raise CleanupError("Audit-Kandidat hat keine gepruefte DB-Referenz.")
        normalized_references = []
        for reference in references:
            if not isinstance(reference, dict):
                raise CleanupError("Audit-Kandidat hat eine fehlerhafte DB-Referenz.")
            if reference.get("table") != "dateien" or reference.get("column") != "stored_name":
                raise CleanupError("Audit-Kandidat hat eine unzulaessige DB-Referenz.")
            try:
                row_id = int(reference.get("row_id"))
            except (TypeError, ValueError) as exc:
                raise CleanupError("Audit-Kandidat hat keine gueltige Datei-ID.") from exc
            if row_id <= 0:
                raise CleanupError("Audit-Kandidat hat keine gueltige Datei-ID.")
            normalized_references.append(
                {"table": "dateien", "column": "stored_name", "row_id": row_id}
            )
        item.update(
            size=source["size"],
            mtime_ns=source["mtime_ns"],
            sha256=digest,
            references=normalized_references,
        )

    summary = candidate_summary(candidates)
    expected_candidate_hash = validate_hash(
        "Erwarteter Kandidatenindex-Hash", args.expected_candidate_index_sha256
    )
    expected_values = {
        "candidate_count": int(args.expected_candidates),
        "candidate_bytes": int(args.expected_candidate_bytes),
        "candidate_blob_count": int(args.expected_candidate_blobs),
    }
    for key, expected in expected_values.items():
        if summary[key] != expected or int(report.get(key) or -1) != expected:
            raise CleanupError("Audit-Kandidaten weichen von der freigegebenen Baseline ab.")
    if int(report.get("candidate_blob_bytes") or -1) != summary["candidate_blob_bytes"]:
        raise CleanupError("Audit-Kandidatenbytes der DB-Originale stimmen nicht.")
    if not (
        hmac.compare_digest(summary["candidate_index_sha256"], expected_candidate_hash)
        and hmac.compare_digest(
            validate_hash("Kandidatenindex-Hash", report.get("candidate_index_sha256")),
            expected_candidate_hash,
        )
    ):
        raise CleanupError("Audit-Kandidatenindex stimmt nicht.")

    covered = report.get("reference_columns_covered")
    if not isinstance(covered, list) or any(not isinstance(item, str) for item in covered):
        raise CleanupError("Audit-Referenzabdeckung fehlt.")
    if covered != sorted(set(covered)):
        raise CleanupError("Audit-Referenzabdeckung ist nicht eindeutig sortiert.")
    coverage_hash = canonical_sha256(covered)
    if not hmac.compare_digest(
        coverage_hash,
        validate_hash(
            "Referenzabdeckungs-Hash",
            report.get("reference_columns_coverage_sha256"),
        ),
    ):
        raise CleanupError("Audit-Referenzabdeckung stimmt nicht.")
    local_evidence = report.get("local_evidence")
    if not isinstance(local_evidence, dict):
        raise CleanupError("Lokale Audit-Evidenz fehlt.")
    for key in (
        "master_manifest_sha256",
        "verification_report_sha256",
        "expected_coverage_sha256",
        "expected_candidate_index_sha256",
    ):
        validate_hash(f"Lokale Evidenz {key}", local_evidence.get(key))
    expected_local_hashes = {
        "master_manifest_sha256": validate_hash(
            "Erwarteter Master-Manifest-Hash", args.expected_master_manifest_sha256
        ),
        "verification_report_sha256": validate_hash(
            "Erwarteter Verifikationsbericht-Hash",
            args.expected_verification_report_sha256,
        ),
        "expected_coverage_sha256": validate_hash(
            "Erwarteter Referenzabdeckungs-Hash", args.expected_coverage_sha256
        ),
        "expected_candidate_index_sha256": expected_candidate_hash,
    }
    if (
        int(local_evidence.get("expected_candidate_count") or -1)
        != summary["candidate_count"]
        or int(local_evidence.get("expected_candidate_bytes") or -1)
        != summary["candidate_bytes"]
        or int(local_evidence.get("expected_candidate_blob_count") or -1)
        != summary["candidate_blob_count"]
        or not hmac.compare_digest(
            local_evidence["expected_coverage_sha256"], coverage_hash
        )
        or not hmac.compare_digest(
            local_evidence["expected_candidate_index_sha256"],
            summary["candidate_index_sha256"],
        )
        or any(
            not hmac.compare_digest(local_evidence[key], expected)
            for key, expected in expected_local_hashes.items()
        )
    ):
        raise CleanupError("Lokale Audit-Evidenz stimmt nicht mit dem Bericht ueberein.")
    return report


def select_candidates(report: dict, limit: int) -> list[dict]:
    candidates = sorted(
        report["candidates"],
        key=lambda item: (int(item["size"]), item["relative_path"]),
    )
    if limit < 0:
        raise CleanupError("Limit darf nicht negativ sein.")
    return candidates[:limit] if limit else candidates


def validate_bootstrap_candidate(report: dict, requested_name: str) -> tuple[dict, list[dict]]:
    if requested_name != BOOTSTRAP_CANDIDATE_PATH:
        raise CleanupError("Zero-Space-Canary ist nur fuer den fest freigegebenen Kandidaten erlaubt.")
    source = report.get("source_inventory") or {}
    if any(source.get(key) != value for key, value in BOOTSTRAP_SOURCE_INVENTORY.items()):
        raise CleanupError("Zero-Space-Canary-Audit hat nicht das fest gebundene Ausgangsinventar.")
    local_evidence = report.get("local_evidence") or {}
    if any(local_evidence.get(key) != value for key, value in BOOTSTRAP_LOCAL_EVIDENCE.items()):
        raise CleanupError("Zero-Space-Canary ist nicht an das freigegebene lokale Archiv gebunden.")
    matches = [
        candidate
        for candidate in report.get("candidates") or []
        if candidate.get("relative_path") == requested_name
    ]
    if len(matches) != 1:
        raise CleanupError("Zero-Space-Canary-Kandidat fehlt oder ist nicht eindeutig.")
    candidate = matches[0]
    if (
        int(candidate.get("size") or -1) != BOOTSTRAP_CANDIDATE_SIZE
        or candidate.get("sha256") != BOOTSTRAP_CANDIDATE_SHA256
        or candidate.get("references") != [BOOTSTRAP_CANDIDATE_REFERENCE]
    ):
        raise CleanupError("Zero-Space-Canary-Kandidat weicht von der Freigabe ab.")
    expected_after = expected_inventory_after(report, [candidate])
    if any(
        expected_after.get(key) != value
        for key, value in BOOTSTRAP_EXPECTED_AFTER.items()
    ):
        raise CleanupError("Zero-Space-Canary-Nachherinventar weicht von der Freigabe ab.")
    remaining = [
        item
        for item in report["candidates"]
        if item["relative_path"] != requested_name
    ]
    remaining_summary = candidate_summary(remaining)
    if any(
        remaining_summary.get(key) != value
        for key, value in BOOTSTRAP_REMAINING_CANDIDATES.items()
    ):
        raise CleanupError("Zero-Space-Canary-Restmenge weicht von der Freigabe ab.")
    return candidate, remaining


def bootstrap_hidden_name(run_id: str, candidate: dict) -> str:
    if not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise CleanupError("Bootstrap-Run-ID ist ungueltig.")
    suffix = pathlib.PurePath(candidate["relative_path"]).suffix
    hidden = f".{run_id[:31]}{suffix}"
    if (
        len(os.fsencode(hidden)) != len(os.fsencode(candidate["relative_path"]))
        or len(os.fsencode(hidden)) != 36
        or pathlib.PurePath(hidden).name != hidden
    ):
        raise CleanupError("Bootstrap-Hidden-Name hat nicht exakt die Original-Laenge.")
    return hidden


def read_boot_id() -> str:
    try:
        value = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="ascii"
        ).strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise CleanupError("Linux-Boot-ID fuer Quiescence-Bindung ist nicht lesbar.") from exc
    if not re.fullmatch(r"[0-9a-f-]{36}", value):
        raise CleanupError("Linux-Boot-ID fuer Quiescence-Bindung ist ungueltig.")
    return value


PROCESS_IDENTITY_FIELDS = (
    "pid",
    "ppid",
    "starttime",
    "uid",
    "cmdline_sha256",
    "cgroup_sha256",
)


def require_pidfd_support() -> None:
    if (
        os.name != "posix"
        or not callable(getattr(os, "pidfd_open", None))
        or not callable(getattr(signal, "pidfd_send_signal", None))
    ):
        raise CleanupError("Linux-pidfd APIs fehlen; Bootstrap wird verweigert.")


def read_proc_bytes(pid: int, name: str, maximum: int = 1024 * 1024) -> bytes:
    path = pathlib.Path("/proc") / str(int(pid)) / name
    try:
        value = path.read_bytes()
    except OSError as exc:
        raise CleanupError(f"Prozessbeleg {path} ist nicht lesbar.") from exc
    if len(value) > maximum:
        raise CleanupError(f"Prozessbeleg {path} ist unerwartet gross.")
    return value


def parse_proc_stat(raw: bytes) -> tuple[str, int, int]:
    try:
        text = raw.decode("ascii")
        closing = text.rindex(")")
        fields = text[closing + 2 :].split()
        state = fields[0]
        ppid = int(fields[1])
        starttime = int(fields[19])
    except (UnicodeDecodeError, ValueError, IndexError) as exc:
        raise CleanupError("/proc/<pid>/stat ist ungueltig.") from exc
    return state, ppid, starttime


def capture_process_identity(pid: int) -> dict:
    before = read_proc_bytes(pid, "stat")
    state, ppid, starttime = parse_proc_stat(before)
    cmdline = read_proc_bytes(pid, "cmdline")
    cgroup = read_proc_bytes(pid, "cgroup")
    status = read_proc_bytes(pid, "status")
    uid_match = re.search(rb"(?m)^Uid:\s+(\d+)\s+", status)
    if uid_match is None:
        raise CleanupError("Prozess-UID fehlt im /proc-Beleg.")
    after = read_proc_bytes(pid, "stat")
    after_state, after_ppid, after_starttime = parse_proc_stat(after)
    if (ppid, starttime) != (after_ppid, after_starttime):
        raise CleanupError("Prozessidentitaet hat sich waehrend der Erfassung geaendert.")
    return {
        "pid": int(pid),
        "ppid": ppid,
        "starttime": starttime,
        "uid": int(uid_match.group(1)),
        "cmdline_sha256": hashlib.sha256(cmdline).hexdigest(),
        "cgroup_sha256": hashlib.sha256(cgroup).hexdigest(),
        "state": after_state or state,
    }


def process_identity_projection(value: dict) -> dict:
    return {key: value.get(key) for key in PROCESS_IDENTITY_FIELDS}


def process_identity_sha256(value: dict) -> str:
    return canonical_sha256(process_identity_projection(value))


def portal_cmdline_matches(raw: bytes) -> bool:
    tokens = [token for token in raw.split(b"\0") if token]
    has_gunicorn = any(pathlib.PurePosixPath(os.fsdecode(token)).name == "gunicorn" for token in tokens)
    return has_gunicorn and b"app:app" in tokens


def scan_portal_processes() -> dict:
    require_pidfd_support()
    matches = []
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            cmdline = read_proc_bytes(pid, "cmdline")
        except CleanupError:
            continue
        if not portal_cmdline_matches(cmdline):
            continue
        identity = capture_process_identity(pid)
        if identity["uid"] != os.getuid():
            raise CleanupError("Passender Portalprozess laeuft unter unerwarteter UID.")
        matches.append(identity)
    if len(matches) != 2:
        raise CleanupError(
            f"Bootstrap verlangt exakt einen Gunicorn-Master und einen Worker; gefunden={len(matches)}."
        )
    master_candidates = [
        item for item in matches if all(other["pid"] != item["ppid"] for other in matches)
    ]
    if len(master_candidates) != 1:
        raise CleanupError("Gunicorn-Mastertopologie ist nicht eindeutig.")
    master = master_candidates[0]
    workers = [item for item in matches if item["ppid"] == master["pid"]]
    if len(workers) != 1:
        raise CleanupError("Gunicorn-Worker ist nicht eindeutig dem Master zugeordnet.")
    return {"master": master, "worker": workers[0]}


def assert_process_topology(
    expected: dict,
    *,
    required_state: str | None = None,
) -> dict:
    current = scan_portal_processes()
    for role in ("master", "worker"):
        if process_identity_projection(current[role]) != process_identity_projection(
            expected[role]
        ):
            raise CleanupError(f"Portal-{role}-Identitaet weicht vom vorbereiteten Plan ab.")
        if required_state is not None and current[role]["state"] != required_state:
            raise CleanupError(f"Portal-{role} ist nicht im Zustand {required_state}.")
    if current["worker"]["ppid"] != current["master"]["pid"]:
        raise CleanupError("Portal-Prozesstopologie wurde veraendert.")
    return current


def pidfd_send(pidfd: int, sig: int) -> None:
    require_pidfd_support()
    signal.pidfd_send_signal(int(pidfd), sig, None, 0)


def assert_pidfd_identity(role: str, identity: dict, pidfd: int) -> dict:
    """Sandwich /proc identity capture between pidfd liveness checks.

    A pidfd is stable after it has been opened, but a numeric PID could have
    been recycled between the earlier topology scan and pidfd_open().  The two
    signal-0 checks prove that this pidfd stayed live around a fresh exact
    identity capture.  A later exit can only make the pidfd signal fail; it
    cannot retarget the descriptor to a recycled PID.
    """
    pidfd_send(pidfd, 0)
    current = capture_process_identity(int(identity["pid"]))
    if process_identity_projection(current) != process_identity_projection(identity):
        raise CleanupError(f"Portal-{role}-pidfd ist nicht an die vorbereitete Identitaet gebunden.")
    pidfd_send(pidfd, 0)
    return current


def wait_for_stopped_process(identity: dict, timeout_seconds: float = 1.5) -> dict:
    deadline = time.monotonic() + timeout_seconds
    while True:
        current = capture_process_identity(int(identity["pid"]))
        if process_identity_projection(current) != process_identity_projection(identity):
            raise CleanupError("Prozessidentitaet wechselte waehrend SIGSTOP.")
        if current["state"] == "T":
            return current
        if time.monotonic() >= deadline:
            raise CleanupError("Portalprozess erreichte den SIGSTOP-Zustand nicht rechtzeitig.")
        time.sleep(0.01)


def expected_inventory_after(report: dict, selected: list[dict]) -> dict:
    removed = {item["relative_path"] for item in selected}
    files = [
        dict(item)
        for item in report["source_inventory"]["files"]
        if item["relative_path"] not in removed
    ]
    return {
        "file_count": len(files),
        "total_file_bytes": sum(int(item["size"]) for item in files),
        "inventory_sha256": inventory_digest(files),
        "files": files,
    }


def require_secure_directory_flags() -> int:
    directory = getattr(os, "O_DIRECTORY", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not directory or not nofollow:
        raise CleanupError(
            "O_DIRECTORY/O_NOFOLLOW fehlt; sichere Render-Loeschung wird verweigert."
        )
    return os.O_RDONLY | directory | nofollow | getattr(os, "O_CLOEXEC", 0)


def directory_identity(info) -> tuple[int, int]:
    return int(info.st_dev), int(info.st_ino)


def open_upload_root(root: pathlib.Path) -> tuple[int, tuple[int, int]]:
    fd = os.open(str(root), require_secure_directory_flags())
    try:
        info = os.fstat(fd)
        path_info = os.stat(root, follow_symlinks=False)
        if not stat.S_ISDIR(info.st_mode) or not stat.S_ISDIR(path_info.st_mode):
            raise CleanupError("Upload-Root ist kein regulaeres Verzeichnis.")
        identity = directory_identity(info)
        if directory_identity(path_info) != identity:
            raise CleanupError("Upload-Root-Pfad verweist nicht auf das geoeffnete Verzeichnis.")
        return fd, identity
    except Exception:
        os.close(fd)
        raise


def assert_root_binding(
    root: pathlib.Path, root_fd: int, expected_identity: tuple[int, int]
) -> None:
    opened = os.fstat(root_fd)
    current = os.stat(root, follow_symlinks=False)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or directory_identity(opened) != expected_identity
        or directory_identity(current) != expected_identity
    ):
        raise CleanupError("Upload-Root wurde waehrend des Cleanup-Laufs ersetzt.")


def quick_inventory_metadata_fd(root_fd: int) -> list[tuple[str, int, int]]:
    result = []
    for name in sorted(os.listdir(root_fd)):
        if pathlib.PurePath(name).name != name or "/" in name or "\\" in name:
            raise CleanupError("Upload-Root enthaelt einen unsicheren Dateinamen.")
        info = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode):
            raise CleanupError("Upload-Root enthaelt einen unzulaessigen Eintrag.")
        result.append((name, int(info.st_size), int(info.st_mtime_ns)))
    return result


def expected_inventory_metadata(report: dict) -> list[tuple[str, int, int]]:
    return [
        (item["relative_path"], int(item["size"]), int(item["mtime_ns"]))
        for item in report["source_inventory"]["files"]
    ]


class LockedPostgres:
    def __init__(self, database_url: str):
        try:
            import psycopg
            from psycopg import IsolationLevel
            from psycopg import sql
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise CleanupError("psycopg ist nicht installiert.") from exc
        self.sql = sql
        self.connection = psycopg.connect(
            database_url,
            autocommit=False,
            row_factory=dict_row,
        )
        self.connection.isolation_level = IsolationLevel.READ_COMMITTED
        self.advisory_locked = False

    def acquire_advisory_lock(self) -> None:
        row = self.connection.execute(
            "SELECT pg_try_advisory_lock(%s) AS locked",
            (PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,),
        ).fetchone()
        if not row or not bool(row.get("locked")):
            self.connection.rollback()
            raise CleanupError(
                "Portal-Originale sind bereits durch einen anderen Lauf gesperrt."
            )
        self.advisory_locked = True

    def reference_columns(self) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT c.table_name, c.column_name,
                   EXISTS (
                     SELECT 1 FROM information_schema.columns i
                     WHERE i.table_schema=c.table_schema
                       AND i.table_name=c.table_name
                       AND i.column_name='id'
                   ) AS has_id
            FROM information_schema.columns c
            JOIN information_schema.tables t
              ON t.table_schema=c.table_schema AND t.table_name=c.table_name
            WHERE c.table_schema=current_schema()
              AND t.table_type='BASE TABLE'
              AND c.column_name = ANY(%s)
            ORDER BY c.table_name, c.column_name
            """,
            (["stored_name", "datei_stored_name", "pdf_stored_name", "unterschrift_stored"],),
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def covered_columns(rows: list[dict]) -> list[str]:
        return sorted(
            f"{row['table_name']}.{row['column_name']}" for row in rows
        )

    def lock_reference_tables(self, rows: list[dict]) -> None:
        if not self.advisory_locked:
            raise CleanupError("Advisory-Lock muss vor Tabellenlocks erworben werden.")
        tables = sorted({str(row["table_name"]) for row in rows} | {"datei_backups"})
        if any(not IDENTIFIER_PATTERN.fullmatch(table) for table in tables):
            raise CleanupError("Unzulaessiger Tabellenname in Referenzabdeckung.")
        identifiers = self.sql.SQL(", ").join(
            self.sql.Identifier(table) for table in tables
        )
        self.connection.execute(
            self.sql.SQL("LOCK TABLE {} IN SHARE MODE NOWAIT").format(identifiers)
        )

    def release_table_locks(self) -> None:
        """Release transaction-scoped locks without releasing the session lock."""
        self.connection.rollback()

    def reference_values(self, table: str, column: str, has_id: bool):
        identifier = self.sql.Identifier
        if has_id:
            query = self.sql.SQL(
                "SELECT id, {column}::text AS stored_value "
                "FROM {table} WHERE COALESCE({column}::text, '') <> ''"
            ).format(column=identifier(column), table=identifier(table))
        else:
            query = self.sql.SQL(
                "SELECT NULL::bigint AS id, {column}::text AS stored_value "
                "FROM {table} WHERE COALESCE({column}::text, '') <> ''"
            ).format(column=identifier(column), table=identifier(table))
        return self.connection.execute(query).fetchall()

    def backup_rows(self, datei_id: int):
        return self.connection.execute(
            """
            SELECT d.id, d.stored_name, d.size AS datei_size,
                   b.id AS backup_id, b.file_base64,
                   b.file_sha256, b.size AS backup_size
            FROM dateien d
            LEFT JOIN datei_backups b ON b.datei_id=d.id
            WHERE d.id=%s
            ORDER BY b.id
            """,
            (datei_id,),
        ).fetchall()

    def close(self) -> None:
        try:
            self.connection.rollback()
            if self.advisory_locked:
                unlocked = self.connection.execute(
                    "SELECT pg_advisory_unlock(%s) AS unlocked",
                    (PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,),
                ).fetchone()
                self.connection.commit()
                if not unlocked or not bool(unlocked.get("unlocked")):
                    raise CleanupError("PostgreSQL-Advisory-Lock wurde nicht freigegeben.")
        finally:
            self.connection.close()


def covered_columns(rows: list[dict]) -> list[str]:
    return sorted(f"{row['table_name']}.{row['column_name']}" for row in rows)


def candidate_row_signatures(
    database, row_ids: list[int], *, include_blob_length: bool
) -> list[dict]:
    """Bind verified blobs to immutable PostgreSQL row versions without fetching blobs."""
    if not row_ids:
        return []
    blob_length_projection = (
        ", char_length(b.file_base64) AS backup_base64_length"
        if include_blob_length
        else ""
    )
    rows = database.connection.execute(
        """
        SELECT d.id AS datei_id,
               d.xmin::text AS datei_xmin,
               d.ctid::text AS datei_ctid,
               d.stored_name::text AS stored_name,
               d.size AS datei_size,
               b.id AS backup_id,
               b.datei_id AS backup_datei_id,
               b.xmin::text AS backup_xmin,
               b.ctid::text AS backup_ctid,
               b.file_sha256::text AS backup_sha256,
               b.size AS backup_size
               {blob_length_projection}
        FROM dateien d
        LEFT JOIN datei_backups b ON b.datei_id=d.id
        WHERE d.id = ANY(%s)
        ORDER BY d.id, b.id
        """.format(blob_length_projection=blob_length_projection),
        (sorted(set(int(value) for value in row_ids)),),
    ).fetchall()
    normalized = []
    for row in rows:
        normalized.append(
            {
                "datei_id": int(row["datei_id"]),
                "datei_xmin": str(row.get("datei_xmin") or ""),
                "datei_ctid": str(row.get("datei_ctid") or ""),
                "stored_name": str(row.get("stored_name") or ""),
                "datei_size": (
                    int(row["datei_size"]) if row.get("datei_size") is not None else None
                ),
                "backup_id": (
                    int(row["backup_id"]) if row.get("backup_id") is not None else None
                ),
                "backup_datei_id": (
                    int(row["backup_datei_id"])
                    if row.get("backup_datei_id") is not None
                    else None
                ),
                "backup_xmin": str(row.get("backup_xmin") or ""),
                "backup_ctid": str(row.get("backup_ctid") or ""),
                "backup_sha256": str(row.get("backup_sha256") or "").strip().lower(),
                "backup_size": (
                    int(row["backup_size"]) if row.get("backup_size") is not None else None
                ),
                "backup_base64_length": (
                    int(row["backup_base64_length"])
                    if row.get("backup_base64_length") is not None
                    else None
                ),
            }
        )
    return normalized


def signature_lock_projection(signatures: list[dict]) -> list[dict]:
    """Project fields safe to query under lock; xmin/ctid bind the blob length."""
    return [
        {key: value for key, value in row.items() if key != "backup_base64_length"}
        for row in signatures
    ]


def selected_row_ids(selected: list[dict]) -> list[int]:
    return sorted(
        {
            int(reference["row_id"])
            for candidate in selected
            for reference in candidate["references"]
        }
    )


def preflight_database(database_url: str, report: dict, selected: list[dict]) -> dict:
    """Perform expensive Base64 decoding and hashing in a read-only snapshot."""
    database = ReadOnlyPostgres(database_url)
    try:
        columns = database.reference_columns()
        if covered_columns(columns) != report["reference_columns_covered"]:
            raise CleanupError("Live-Referenzspalten weichen vor dem Cleanup vom Audit ab.")
        references, covered = discover_references(database)
        if covered != report["reference_columns_covered"]:
            raise CleanupError("Live-Referenzabdeckung weicht vor dem Cleanup vom Audit ab.")
        for candidate in selected:
            if references.get(candidate["relative_path"], []) != candidate["references"]:
                raise CleanupError("Live-Referenzen eines Kandidaten weichen vom Audit ab.")
            for reference in candidate["references"]:
                result = verify_blob(database, candidate, int(reference["row_id"]))
                if not result.get("verified"):
                    raise CleanupError(
                        "DB-Original eines Kandidaten ist nicht mehr exakt verifiziert."
                    )
        signatures = candidate_row_signatures(
            database, selected_row_ids(selected), include_blob_length=True
        )
        expected_signature_rows = sum(len(item["references"]) for item in selected)
        if len(signatures) != expected_signature_rows:
            raise CleanupError("DB-Zeilensignaturen sind nicht eindeutig vollstaendig.")
        return {
            "reference_columns": covered,
            "row_signatures": signatures,
            "row_signatures_sha256": canonical_sha256(signatures),
            "row_lock_projection": signature_lock_projection(signatures),
        }
    finally:
        database.close()


def verify_locked_database(
    database: LockedPostgres,
    report: dict,
    selected: list[dict],
    preflight: dict,
) -> None:
    """Do only short metadata/reference checks while SHARE NOWAIT locks are held."""
    columns_before = database.reference_columns()
    if covered_columns(columns_before) != report["reference_columns_covered"]:
        raise CleanupError("Live-Referenzspalten weichen vor dem Tabellenlock vom Audit ab.")
    database.lock_reference_tables(columns_before)
    columns_after = database.reference_columns()
    if covered_columns(columns_after) != report["reference_columns_covered"]:
        raise CleanupError("Live-Referenzspalten haben sich beim Tabellenlock veraendert.")
    references, covered = discover_references(database)
    if covered != report["reference_columns_covered"]:
        raise CleanupError("Live-Referenzabdeckung weicht vom Audit ab.")
    for candidate in selected:
        expected_refs = candidate["references"]
        live_refs = references.get(candidate["relative_path"], [])
        if live_refs != expected_refs:
            raise CleanupError("Live-Referenzen eines Kandidaten weichen vom Audit ab.")
    signatures = candidate_row_signatures(
        database, selected_row_ids(selected), include_blob_length=False
    )
    if signature_lock_projection(signatures) != preflight["row_lock_projection"]:
        raise CleanupError("DB-Zeilenversionen haben sich seit dem Preflight veraendert.")


def file_identity(info) -> tuple:
    return (
        int(info.st_dev),
        int(info.st_ino),
        int(info.st_size),
        int(info.st_mtime_ns),
        int(info.st_ctime_ns),
        int(info.st_nlink),
    )


def post_rename_identity(info) -> tuple:
    """Identity fields that stay stable across a same-filesystem rename."""
    return (
        stat.S_IFMT(info.st_mode),
        int(info.st_dev),
        int(info.st_ino),
        int(info.st_size),
        int(info.st_mtime_ns),
        int(info.st_nlink),
    )


def open_verified_candidate(root_fd: int, candidate: dict) -> tuple[int, tuple]:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow:
        raise CleanupError("O_NOFOLLOW fehlt; sichere Render-Loeschung wird verweigert.")
    flags |= nofollow
    fd = os.open(candidate["relative_path"], flags, dir_fd=root_fd)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise CleanupError("Kandidat ist keine einzelne regulaere Datei.")
        if (
            int(before.st_size) != int(candidate["size"])
            or int(before.st_mtime_ns) != int(candidate["mtime_ns"])
        ):
            raise CleanupError("Kandidaten-Metadaten weichen vom Audit ab.")
        digest = hashlib.sha256()
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            digest.update(block)
        after = os.fstat(fd)
        if file_identity(before) != file_identity(after):
            raise CleanupError("Kandidat wurde waehrend der Dateipruefung veraendert.")
        if not hmac.compare_digest(digest.hexdigest(), candidate["sha256"]):
            raise CleanupError("Kandidaten-Pruefsumme weicht vom Audit ab.")
        path_info = os.stat(
            candidate["relative_path"],
            dir_fd=root_fd,
            follow_symlinks=False,
        )
        if file_identity(path_info) != file_identity(after):
            raise CleanupError("Kandidaten-Pfad wurde waehrend der Pruefung ersetzt.")
        return fd, file_identity(after)
    except Exception:
        os.close(fd)
        raise


def fsync_directory(fd: int) -> None:
    os.fsync(fd)


def estimated_persistent_ledger_bytes(plan: dict) -> int:
    """Conservative space floor for plan, journal, terminal receipt and temp copies."""
    plan_bytes = len(
        (json.dumps(plan, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    )
    selected_bytes = len(
        json.dumps(
            plan.get("selected") or [],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    # The journal repeats evidence for intent/result events, while a partial
    # receipt can contain selected, deleted, restored and staged copies at once.
    calculated = plan_bytes * 3 + selected_bytes * 12 + 512 * 1024
    return max(PERSISTENT_LEDGER_MIN_BYTES, calculated)


def ensure_persistent_ledger_capacity(root_fd: int, plan: dict) -> dict:
    """Fail before creating a run directory when its evidence cannot be durable."""
    info = os.fstatvfs(root_fd)
    available_bytes = int(info.f_bavail) * int(info.f_frsize)
    available_inodes = int(info.f_favail)
    required_bytes = estimated_persistent_ledger_bytes(plan)
    if (
        available_bytes < required_bytes
        or available_inodes < PERSISTENT_LEDGER_MIN_INODES
    ):
        raise CleanupError(
            "Persistenter Speicher reicht nicht fuer einen crash-sicheren "
            f"Cleanup-Beleg: available_bytes={available_bytes}, "
            f"required_bytes={required_bytes}, available_inodes={available_inodes}. "
            "Keine Upload-Datei wurde veraendert. AUTO_BACKUP_RESERVE_BYTES ist "
            "keine vorallokierte Reserve-Datei; Zero-Space-Cleanup wird verweigert."
        )
    return {
        "available_bytes_before_run": available_bytes,
        "available_inodes_before_run": available_inodes,
        "required_ledger_bytes": required_bytes,
        "required_ledger_inodes": PERSISTENT_LEDGER_MIN_INODES,
    }


def capacity_snapshot(root_fd: int, required_bytes: int) -> dict:
    info = os.fstatvfs(root_fd)
    available_bytes = int(info.f_bavail) * int(info.f_frsize)
    available_inodes = int(info.f_favail)
    return {
        "available_bytes": available_bytes,
        "available_inodes": available_inodes,
        "required_ledger_bytes": int(required_bytes),
        "required_ledger_inodes": PERSISTENT_LEDGER_MIN_INODES,
        "sufficient_for_regular_cleanup": (
            available_bytes >= int(required_bytes)
            and available_inodes >= PERSISTENT_LEDGER_MIN_INODES
        ),
    }


def rename_noreplace(
    source_name: str,
    target_name: str,
    *,
    source_dir_fd: int,
    target_dir_fd: int,
) -> None:
    """Linux renameat2(RENAME_NOREPLACE); never emulate with overwrite-prone rename."""
    if os.name != "posix":
        raise CleanupError("Atomisches RENAME_NOREPLACE ist nur unter Linux freigegeben.")
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise CleanupError("renameat2(RENAME_NOREPLACE) ist nicht verfuegbar.")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        int(source_dir_fd),
        os.fsencode(source_name),
        int(target_dir_fd),
        os.fsencode(target_name),
        RENAME_NOREPLACE,
    )
    if result != 0:
        error_number = ctypes.get_errno() or errno.EIO
        raise OSError(error_number, os.strerror(error_number), target_name)


def read_small_json_at(directory_fd: int, name: str) -> dict:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(name, flags, dir_fd=directory_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 2 * 1024 * 1024:
            raise CleanupError("Persistenter Cleanup-Beleg ist ungueltig.")
        chunks = []
        while True:
            block = os.read(fd, 64 * 1024)
            if not block:
                break
            chunks.append(block)
        value = json.loads(b"".join(chunks).decode("utf-8"))
        if not isinstance(value, dict):
            raise CleanupError("Persistenter Cleanup-Beleg ist ungueltig.")
        return value
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise CleanupError("Persistenter Cleanup-Beleg ist nicht lesbar.") from exc
    finally:
        os.close(fd)


def verify_prior_journal(directory_fd: int, receipt: dict, run_id: str) -> None:
    expected_count = int(receipt.get("event_count") or 0)
    expected_tail = str(receipt.get("last_event_sha256") or "")
    expected_raw = str(receipt.get("journal_sha256") or "")
    if expected_count == 0:
        if expected_tail != "0" * 64 or expected_raw:
            raise CleanupError("Leerer Cleanup-Journalbeleg ist inkonsistent.")
        return
    validate_hash("Persistenter Journal-Hash", expected_raw)
    validate_hash("Persistente Journal-Kettenspitze", expected_tail)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(".cleanup-journal.jsonl", flags, dir_fd=directory_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 32 * 1024 * 1024:
            raise CleanupError("Persistentes Cleanup-Journal ist ungueltig.")
        chunks = []
        while True:
            block = os.read(fd, 64 * 1024)
            if not block:
                break
            chunks.append(block)
        raw = b"".join(chunks)
    finally:
        os.close(fd)
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected_raw):
        raise CleanupError("Persistenter Cleanup-Journal-Hash stimmt nicht.")
    previous = "0" * 64
    records = raw.decode("utf-8").splitlines()
    if len(records) != expected_count:
        raise CleanupError("Persistente Cleanup-Journalanzahl stimmt nicht.")
    for sequence, line in enumerate(records, 1):
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise CleanupError("Persistentes Cleanup-Journal ist nicht lesbar.") from exc
        claimed = validate_hash("Journal-Ereignis-Hash", record.pop("event_sha256", ""))
        if (
            record.get("run_id") != run_id
            or int(record.get("sequence") or 0) != sequence
            or record.get("previous_event_sha256") != previous
            or not hmac.compare_digest(canonical_sha256(record), claimed)
        ):
            raise CleanupError("Persistente Cleanup-Journalkette ist ungueltig.")
        previous = claimed
    if not hmac.compare_digest(previous, expected_tail):
        raise CleanupError("Persistente Cleanup-Journalkettenspitze stimmt nicht.")


def validate_receipt_envelope(receipt: dict, run_id: str) -> str:
    if not isinstance(receipt, dict):
        raise CleanupError("Persistenter Cleanup-Receipt ist ungueltig.")
    claimed = validate_hash("Persistenter Receipt-Hash", receipt.get("receipt_sha256"))
    canonical = dict(receipt)
    canonical.pop("receipt_sha256", None)
    if (
        receipt.get("format") != CLEANUP_FORMAT
        or receipt.get("run_id") != run_id
        or not hmac.compare_digest(canonical_sha256(canonical), claimed)
    ):
        raise CleanupError("Persistenter Cleanup-Receipt ist ungueltig.")
    return str(receipt.get("status") or "")


def validate_setup_receipt(receipt: dict, run_id: str) -> None:
    expected_keys = {
        "format",
        "run_id",
        "created_at",
        "status",
        "audit_sha256",
        "plan_sha256",
        "database_writes",
        "selected_count",
        "selected_bytes",
        "deleted_count",
        "restored_count",
        "staged_remaining_count",
        "uncertain",
        "ledger_capacity",
        "receipt_sha256",
    }
    if set(receipt) != expected_keys:
        raise CleanupError("Setup-Receipt hat unerwartete Felder.")
    if validate_receipt_envelope(receipt, run_id) != "cleanup_setup_incomplete":
        raise CleanupError("Setup-Receipt hat einen unerwarteten Status.")
    validate_hash("Setup-Audit-Hash", receipt.get("audit_sha256"))
    validate_hash("Setup-Plan-Hash", receipt.get("plan_sha256"))
    try:
        created_at = datetime.fromisoformat(str(receipt.get("created_at") or ""))
        values = {
            "database_writes": int(receipt.get("database_writes")),
            "selected_count": int(receipt.get("selected_count")),
            "selected_bytes": int(receipt.get("selected_bytes")),
            "deleted_count": int(receipt.get("deleted_count")),
            "restored_count": int(receipt.get("restored_count")),
            "staged_remaining_count": int(receipt.get("staged_remaining_count")),
        }
    except (TypeError, ValueError) as exc:
        raise CleanupError("Setup-Receipt hat ungueltige Werte.") from exc
    ledger = receipt.get("ledger_capacity")
    if (
        created_at.utcoffset() is None
        or values["database_writes"] != 0
        or values["selected_count"] < 1
        or values["selected_bytes"] < 0
        or values["deleted_count"] != 0
        or values["restored_count"] != 0
        or values["staged_remaining_count"] != 0
        or receipt.get("uncertain") != [SETUP_UNCERTAINTY]
        or not isinstance(ledger, dict)
        or set(ledger)
        != {
            "available_bytes_before_run",
            "available_inodes_before_run",
            "required_ledger_bytes",
            "required_ledger_inodes",
        }
    ):
        raise CleanupError("Setup-Receipt ist strukturell ungueltig.")
    try:
        ledger_values = {key: int(value) for key, value in ledger.items()}
    except (TypeError, ValueError) as exc:
        raise CleanupError("Setup-Receipt hat ungueltige Kapazitaetswerte.") from exc
    if (
        ledger_values["available_bytes_before_run"] < 0
        or ledger_values["available_inodes_before_run"] < 0
        or ledger_values["required_ledger_bytes"] < PERSISTENT_LEDGER_MIN_BYTES
        or ledger_values["required_ledger_inodes"] < PERSISTENT_LEDGER_MIN_INODES
    ):
        raise CleanupError("Setup-Receipt hat ungueltige Kapazitaetswerte.")


def validate_setup_plan(plan: dict, receipt: dict, run_id: str) -> None:
    expected_keys = {
        "format",
        "run_id",
        "created_at",
        "status",
        "audit_sha256",
        "advisory_lock_key",
        "database_writes",
        "selected_count",
        "selected_bytes",
        "selected",
        "expected_after",
        "expected_remaining_candidates",
        "plan_sha256",
    }
    if not isinstance(plan, dict) or set(plan) != expected_keys:
        raise CleanupError("Persistenter Setup-Plan ist strukturell ungueltig.")
    claimed = validate_hash("Persistenter Plan-Hash", plan.get("plan_sha256"))
    canonical = dict(plan)
    canonical.pop("plan_sha256", None)
    if (
        plan.get("format") != CLEANUP_FORMAT
        or plan.get("run_id") != run_id
        or plan.get("status") != "authorized_scope"
        or plan.get("audit_sha256") != receipt.get("audit_sha256")
        or receipt.get("plan_sha256") != claimed
        or not hmac.compare_digest(canonical_sha256(canonical), claimed)
    ):
        raise CleanupError("Persistenter Setup-Plan ist ungueltig.")
    selected = plan.get("selected")
    try:
        database_writes = int(plan.get("database_writes"))
        selected_count = int(plan.get("selected_count"))
        selected_bytes = int(plan.get("selected_bytes"))
    except (TypeError, ValueError) as exc:
        raise CleanupError("Persistenter Setup-Plan hat ungueltige Werte.") from exc
    if (
        database_writes != 0
        or int(plan.get("advisory_lock_key") or 0)
        != PORTAL_ORIGINALS_ADVISORY_LOCK_KEY
        or not isinstance(selected, list)
        or selected_count != len(selected)
        or selected_count < 1
        or selected_bytes != sum(int(item.get("size") or -1) for item in selected)
        or selected_count != int(receipt.get("selected_count"))
        or selected_bytes != int(receipt.get("selected_bytes"))
    ):
        raise CleanupError("Persistenter Setup-Plan ist strukturell ungueltig.")


def read_setup_journal_at(directory_fd: int, run_id: str) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(".cleanup-journal.jsonl", flags, dir_fd=directory_fd)
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or int(info.st_nlink) != 1
            or info.st_size > 32 * 1024 * 1024
        ):
            raise CleanupError("Persistentes Setup-Journal ist ungueltig.")
        chunks = []
        while True:
            block = os.read(fd, 64 * 1024)
            if not block:
                break
            chunks.append(block)
        raw = b"".join(chunks)
    finally:
        os.close(fd)

    previous = "0" * 64
    event_names = []
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise CleanupError("Persistentes Setup-Journal ist nicht lesbar.") from exc
    for sequence, line in enumerate(lines, 1):
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise CleanupError("Persistentes Setup-Journal ist nicht lesbar.") from exc
        if not isinstance(record, dict):
            raise CleanupError("Persistentes Setup-Journal ist ungueltig.")
        claimed = validate_hash("Setup-Journal-Ereignis-Hash", record.pop("event_sha256", ""))
        if (
            record.get("run_id") != run_id
            or int(record.get("sequence") or 0) != sequence
            or record.get("previous_event_sha256") != previous
            or not hmac.compare_digest(canonical_sha256(record), claimed)
        ):
            raise CleanupError("Persistente Setup-Journalkette ist ungueltig.")
        previous = claimed
        event_names.append(str(record.get("event") or ""))

    normal_prefix = [
        "run_started",
        "advisory_lock_acquired",
        "database_preflight_complete",
        "file_preflight_complete",
        "mutation_arm_intent",
    ]
    prefix_count = 0
    while (
        prefix_count < len(event_names)
        and prefix_count < len(normal_prefix)
        and event_names[prefix_count] == normal_prefix[prefix_count]
    ):
        prefix_count += 1
    tail = event_names[prefix_count:]
    if tail and tail[0] == "table_locks_released_after_failure":
        if prefix_count < 4:
            raise CleanupError("Setup-Journal enthaelt eine unplausible Lock-Freigabe.")
        tail = tail[1:]
    if tail and tail[0] == "run_failed":
        tail = tail[1:]
    if tail:
        raise CleanupError("Setup-Journal enthaelt ein Ereignis nach Mutationsbeginn.")


def setup_file_identity_at(directory_fd: int, name: str) -> tuple:
    info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode) or int(info.st_nlink) != 1:
        raise CleanupError("Setup-Lauf enthaelt keinen einzelnen regulaeren Beleg.")
    return file_identity(info)


def validate_setup_run(directory_fd: int, run_id: str, names: list[str]) -> dict[str, tuple]:
    name_set = set(names)
    if len(name_set) != len(names):
        raise CleanupError("Setup-Lauf enthaelt doppelte Eintraege.")
    receipt_parts = [name for name in names if ATOMIC_RECEIPT_PART_PATTERN.fullmatch(name)]
    plan_parts = [name for name in names if ATOMIC_PLAN_PART_PATTERN.fullmatch(name)]
    recognized = {
        ".cleanup-receipt.json",
        ".cleanup-plan.json",
        ".cleanup-journal.jsonl",
        *receipt_parts,
        *plan_parts,
    }
    if (
        name_set - recognized
        or len(receipt_parts) > 1
        or len(plan_parts) > 1
        or ".cleanup-receipt.json" not in name_set
    ):
        raise CleanupError("Setup-Lauf enthaelt einen fremden oder unvollstaendigen Eintrag.")

    receipt = read_small_json_at(directory_fd, ".cleanup-receipt.json")
    validate_setup_receipt(receipt, run_id)
    identities = {
        name: setup_file_identity_at(directory_fd, name) for name in names
    }

    plan_names = [
        name
        for name in (".cleanup-plan.json", *plan_parts)
        if name in name_set
    ]
    if len(plan_names) > 1:
        raise CleanupError("Setup-Lauf enthaelt mehrere Planfassungen.")
    for plan_name in plan_names:
        validate_setup_plan(read_small_json_at(directory_fd, plan_name), receipt, run_id)
    if ".cleanup-journal.jsonl" in name_set:
        if ".cleanup-plan.json" not in name_set:
            raise CleanupError("Setup-Journal liegt ohne dauerhaftes Plan-Artefakt vor.")
        read_setup_journal_at(directory_fd, run_id)

    for receipt_part in receipt_parts:
        partial = read_small_json_at(directory_fd, receipt_part)
        status = validate_receipt_envelope(partial, run_id)
        if (
            partial.get("audit_sha256") != receipt.get("audit_sha256")
            or partial.get("plan_sha256") != receipt.get("plan_sha256")
            or int(partial.get("database_writes") or 0) != 0
        ):
            raise CleanupError("Temporärer Setup-Receipt ist nicht an den Lauf gebunden.")
        if status == "cleanup_setup_incomplete":
            validate_setup_receipt(partial, run_id)
        elif status == "cleanup_mutation_armed":
            pass
        elif status in {"cleanup_failed_no_deletion", "cleanup_partial"}:
            if (
                int(partial.get("deleted_count") or 0) != 0
                or int(partial.get("restored_count") or 0) != 0
                or int(partial.get("staged_remaining_count") or 0) != 0
                or partial.get("deleted")
                or partial.get("restored")
                or partial.get("staged_remaining")
            ):
                raise CleanupError("Temporärer Fehler-Receipt behauptet eine Mutation.")
        else:
            raise CleanupError("Temporärer Setup-Receipt hat einen unbekannten Status.")
    return identities


def remove_validated_setup_run(directory_fd: int, identities: dict[str, tuple]) -> None:
    current_names = sorted(os.listdir(directory_fd))
    if set(current_names) != set(identities):
        raise CleanupError("Setup-Lauf wurde waehrend der Bereinigung veraendert.")
    for name in current_names:
        if setup_file_identity_at(directory_fd, name) != identities[name]:
            raise CleanupError("Setup-Beleg wurde waehrend der Bereinigung veraendert.")
    deletion_order = sorted(
        identities,
        key=lambda name: (name == ".cleanup-receipt.json", name),
    )
    for name in deletion_order:
        if setup_file_identity_at(directory_fd, name) != identities[name]:
            raise CleanupError("Setup-Beleg wurde vor dem Entfernen ersetzt.")
        os.unlink(name, dir_fd=directory_fd)
        fsync_directory(directory_fd)


def reconcile_prior_runs_at(base_fd: int) -> list[str]:
    """Remove only proven pre-mutation setup debris; preserve every armed run."""
    flags = require_secure_directory_flags()
    base_info = os.fstat(base_fd)
    removed = []
    for prior_name in sorted(os.listdir(base_fd)):
        match = RUN_DIRECTORY_PATTERN.fullmatch(prior_name)
        prior_info = os.stat(prior_name, dir_fd=base_fd, follow_symlinks=False)
        if (
            match is None
            or not stat.S_ISDIR(prior_info.st_mode)
            or int(prior_info.st_dev) != int(base_info.st_dev)
        ):
            raise CleanupError("Persistenter Cleanup-Bereich enthaelt einen fremden Eintrag.")
        prior_fd = os.open(prior_name, flags, dir_fd=base_fd)
        remove_directory = False
        try:
            opened_info = os.fstat(prior_fd)
            if directory_identity(opened_info) != directory_identity(prior_info):
                raise CleanupError("Persistenter Cleanup-Lauf wurde ersetzt.")
            names = sorted(os.listdir(prior_fd))
            if not names:
                # mkdir is fsynced before the setup receipt; an empty exact run
                # directory is therefore the only unauthenticated state we remove.
                remove_directory = True
            elif ".cleanup-receipt.json" not in names:
                receipt_parts = [
                    name for name in names if ATOMIC_RECEIPT_PART_PATTERN.fullmatch(name)
                ]
                if len(names) != 1 or len(receipt_parts) != 1:
                    raise CleanupError(
                        f"Nicht abgeschlossener Cleanup-Lauf blockiert: {prior_name}"
                    )
                partial = read_small_json_at(prior_fd, receipt_parts[0])
                validate_setup_receipt(partial, match.group(1))
                identities = {
                    receipt_parts[0]: setup_file_identity_at(prior_fd, receipt_parts[0])
                }
                remove_validated_setup_run(prior_fd, identities)
                remove_directory = True
            else:
                receipt = read_small_json_at(prior_fd, ".cleanup-receipt.json")
                status = validate_receipt_envelope(receipt, match.group(1))
                if status == "cleanup_setup_incomplete":
                    identities = validate_setup_run(
                        prior_fd, match.group(1), names
                    )
                    remove_validated_setup_run(prior_fd, identities)
                    remove_directory = True
                elif status == "cleanup_mutation_armed":
                    raise CleanupError(
                        f"Mutationsbereiter Cleanup-Lauf blockiert: {prior_name}"
                    )
                elif not prior_run_is_resolved(receipt, prior_name, prior_fd):
                    raise CleanupError(
                        f"Nicht aufgeloester Cleanup-Lauf blockiert: {prior_name}"
                    )
        finally:
            os.close(prior_fd)
        if remove_directory:
            os.rmdir(prior_name, dir_fd=base_fd)
            fsync_directory(base_fd)
            removed.append(prior_name)
    return removed


def reconcile_pending_runs(
    root: pathlib.Path, root_identity: tuple[int, int]
) -> list[str]:
    """Reconcile setup-only debris while the caller holds the advisory lock."""
    if PENDING_ROOT.parent != root.parent:
        raise CleanupError("Persistenter Cleanup-Bereich liegt nicht neben dem Upload-Root.")
    flags = require_secure_directory_flags()
    parent_fd = os.open(str(root.parent), flags)
    base_fd = None
    try:
        parent_info = os.fstat(parent_fd)
        parent_path_info = os.stat(root.parent, follow_symlinks=False)
        if (
            directory_identity(parent_info) != directory_identity(parent_path_info)
            or int(parent_info.st_dev) != root_identity[0]
        ):
            raise CleanupError("Cleanup-Elternverzeichnis wurde ersetzt.")
        try:
            base_fd = os.open(PENDING_ROOT.name, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            return []
        base_info = os.fstat(base_fd)
        if (
            not stat.S_ISDIR(base_info.st_mode)
            or int(base_info.st_dev) != root_identity[0]
            or stat.S_IMODE(base_info.st_mode) & 0o077
        ):
            raise CleanupError("Persistenter Cleanup-Bereich ist nicht privat gebunden.")
        return reconcile_prior_runs_at(base_fd)
    finally:
        if base_fd is not None:
            os.close(base_fd)
        os.close(parent_fd)


def prior_run_is_resolved(
    receipt: dict,
    prior_name: str | None = None,
    directory_fd: int | None = None,
) -> bool:
    strict_context = prior_name is not None or directory_fd is not None
    if strict_context:
        if prior_name is None or directory_fd is None:
            raise CleanupError("Persistenter Cleanup-Receipt-Kontext ist unvollstaendig.")
        run_id = prior_name.removeprefix("run-")
        validate_receipt_envelope(receipt, run_id)
        plan = read_small_json_at(directory_fd, ".cleanup-plan.json")
        claimed_plan_hash = validate_hash(
            "Persistenter Plan-Hash", plan.get("plan_sha256")
        )
        canonical_plan = dict(plan)
        canonical_plan.pop("plan_sha256", None)
        if (
            plan.get("format") != CLEANUP_FORMAT
            or plan.get("run_id") != run_id
            or receipt.get("plan_sha256") != claimed_plan_hash
            or not hmac.compare_digest(canonical_sha256(canonical_plan), claimed_plan_hash)
        ):
            raise CleanupError("Persistenter Cleanup-Plan ist ungueltig.")
        verify_prior_journal(directory_fd, receipt, run_id)
    status = str(receipt.get("status") or "")
    if status == "cleanup_complete":
        if not strict_context:
            return True
        return (
            int(receipt.get("selected_count") or -1)
            == int(receipt.get("deleted_count") or -2)
            and not receipt.get("restored")
            and not receipt.get("staged_remaining")
            and not receipt.get("uncertain")
            and (receipt.get("post_unlink_database_verification") or {}).get(
                "verified"
            )
            is True
        )
    return (
        status == "cleanup_failed_no_deletion"
        and int(receipt.get("deleted_count") or 0) == 0
        and not receipt.get("staged_remaining")
        and not receipt.get("uncertain")
    )


def open_pending_run(
    root: pathlib.Path,
    root_identity: tuple[int, int],
    run_id: str,
    setup_receipt: dict | None = None,
) -> tuple[pathlib.Path, int, tuple[int, int]]:
    if PENDING_ROOT.parent != root.parent:
        raise CleanupError("Persistenter Cleanup-Bereich liegt nicht neben dem Upload-Root.")
    flags = require_secure_directory_flags()
    parent_fd = os.open(str(root.parent), flags)
    base_fd = None
    run_fd = None
    run_name = f"run-{run_id}"
    run_created = False
    try:
        parent_info = os.fstat(parent_fd)
        if int(parent_info.st_dev) != root_identity[0]:
            raise CleanupError("Upload-Root und Cleanup-Elternverzeichnis liegen nicht gleich.")
        try:
            os.mkdir(PENDING_ROOT.name, 0o700, dir_fd=parent_fd)
            fsync_directory(parent_fd)
        except FileExistsError:
            pass
        base_fd = os.open(PENDING_ROOT.name, flags, dir_fd=parent_fd)
        base_info = os.fstat(base_fd)
        if not stat.S_ISDIR(base_info.st_mode) or int(base_info.st_dev) != root_identity[0]:
            raise CleanupError("Persistenter Cleanup-Bereich liegt nicht auf dem Upload-Dateisystem.")
        os.fchmod(base_fd, 0o700)

        reconcile_prior_runs_at(base_fd)

        os.mkdir(run_name, 0o700, dir_fd=base_fd)
        run_created = True
        fsync_directory(base_fd)
        run_fd = os.open(run_name, flags, dir_fd=base_fd)
        run_info = os.fstat(run_fd)
        if not stat.S_ISDIR(run_info.st_mode) or int(run_info.st_dev) != root_identity[0]:
            raise CleanupError("Cleanup-Lauf liegt nicht auf dem Upload-Dateisystem.")
        os.fchmod(run_fd, 0o700)
        fsync_directory(run_fd)
        if setup_receipt is not None:
            atomic_json_at(run_fd, ".cleanup-receipt.json", setup_receipt)
        result = PENDING_ROOT / run_name, run_fd, directory_identity(run_info)
        run_fd = None
        return result
    except Exception:
        if run_fd is not None:
            os.close(run_fd)
            run_fd = None
        if run_created and base_fd is not None:
            try:
                os.rmdir(run_name, dir_fd=base_fd)
                fsync_directory(base_fd)
            except OSError:
                # No upload was touched. A non-empty or externally changed
                # directory must remain visible and block the next run.
                pass
        raise
    finally:
        if run_fd is not None:
            os.close(run_fd)
        if base_fd is not None:
            os.close(base_fd)
        os.close(parent_fd)


def open_fallback_run(run_id: str) -> tuple[pathlib.Path, int, tuple[int, int]]:
    """Create a /tmp evidence location only when persistent setup itself failed."""
    CLEANUP_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    base_info = os.stat(CLEANUP_ROOT, follow_symlinks=False)
    if not stat.S_ISDIR(base_info.st_mode):
        raise CleanupError("Temporaerer Cleanup-Belegbereich ist ungueltig.")
    os.chmod(CLEANUP_ROOT, 0o700)
    base_fd = os.open(str(CLEANUP_ROOT), require_secure_directory_flags())
    try:
        run_name = f"run-{run_id}"
        os.mkdir(run_name, 0o700, dir_fd=base_fd)
        fsync_directory(base_fd)
        run_fd = os.open(run_name, require_secure_directory_flags(), dir_fd=base_fd)
        info = os.fstat(run_fd)
        os.fchmod(run_fd, 0o700)
        fsync_directory(run_fd)
        return CLEANUP_ROOT / run_name, run_fd, directory_identity(info)
    finally:
        os.close(base_fd)


def assert_pending_binding(
    path: pathlib.Path, directory_fd: int, expected_identity: tuple[int, int]
) -> None:
    opened = os.fstat(directory_fd)
    current = os.stat(path, follow_symlinks=False)
    if (
        not stat.S_ISDIR(opened.st_mode)
        or not stat.S_ISDIR(current.st_mode)
        or directory_identity(opened) != expected_identity
        or directory_identity(current) != expected_identity
    ):
        raise CleanupError("Persistenter Cleanup-Lauf wurde ersetzt.")


def atomic_json_at(directory_fd: int, name: str, value) -> None:
    if pathlib.PurePath(name).name != name or "/" in name or "\\" in name:
        raise CleanupError("Cleanup-Belegname ist unzulaessig.")
    partial = f".{name}.{uuid.uuid4().hex}.part"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = None
    try:
        fd = os.open(partial, flags, 0o600, dir_fd=directory_fd)
        payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        offset = 0
        while offset < len(payload):
            offset += os.write(fd, payload[offset:])
        os.fsync(fd)
        os.close(fd)
        fd = None
        os.rename(partial, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        fsync_directory(directory_fd)
    except Exception:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(partial, dir_fd=directory_fd)
            fsync_directory(directory_fd)
        except OSError:
            pass
        raise


def atomic_json_at_with_capacity_guard(
    directory_fd: int,
    name: str,
    value: dict,
    *,
    capacity_fd: int,
    required_bytes: int,
) -> dict:
    """Commit JSON only after its fully allocated temp copy leaves batch capacity."""
    if pathlib.PurePath(name).name != name or "/" in name or "\\" in name:
        raise CleanupError("Cleanup-Belegname ist unzulaessig.")
    partial = f".{name}.{uuid.uuid4().hex}.guarded-part"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = None
    try:
        fd = os.open(partial, flags, 0o600, dir_fd=directory_fd)
        payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode(
            "utf-8"
        )
        offset = 0
        while offset < len(payload):
            offset += os.write(fd, payload[offset:])
        os.fsync(fd)
        os.close(fd)
        fd = None
        fsync_directory(directory_fd)
        guarded_capacity = capacity_snapshot(capacity_fd, required_bytes)
        if not guarded_capacity["sufficient_for_regular_cleanup"]:
            raise CleanupError(
                "Persistenter Erfolgsbeleg wuerde nicht genug reale Batch-Kapazitaet lassen."
            )
        os.rename(partial, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        fsync_directory(directory_fd)
        return guarded_capacity
    except Exception:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(partial, dir_fd=directory_fd)
            fsync_directory(directory_fd)
        except OSError:
            pass
        raise


def sha256_regular_file(path: pathlib.Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    if not getattr(os, "O_NOFOLLOW", 0):
        raise CleanupError("O_NOFOLLOW fehlt fuer den Skriptbeleg.")
    fd = os.open(str(path), flags)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise CleanupError("Bootstrap-Skriptbeleg ist keine regulaere Datei.")
        digest = hashlib.sha256()
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            digest.update(block)
        after = os.fstat(fd)
        if file_identity(before) != file_identity(after):
            raise CleanupError("Bootstrap-Skript wurde waehrend des Hashings veraendert.")
        return digest.hexdigest()
    finally:
        os.close(fd)


def create_private_run(
    base_path: pathlib.Path,
    run_id: str,
    *,
    required_device: int | None = None,
) -> tuple[pathlib.Path, int, tuple[int, int]]:
    if not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise CleanupError("Bootstrap-Run-ID ist ungueltig.")
    flags = require_secure_directory_flags()
    parent_fd = os.open(str(base_path.parent), flags)
    base_fd = None
    run_fd = None
    try:
        parent_info = os.fstat(parent_fd)
        parent_path_info = os.stat(base_path.parent, follow_symlinks=False)
        if directory_identity(parent_info) != directory_identity(parent_path_info):
            raise CleanupError("Bootstrap-Elternverzeichnis wurde ersetzt.")
        if required_device is not None and int(parent_info.st_dev) != required_device:
            raise CleanupError("Bootstrap-Beleg liegt nicht auf dem erwarteten Dateisystem.")
        try:
            os.mkdir(base_path.name, 0o700, dir_fd=parent_fd)
            fsync_directory(parent_fd)
        except FileExistsError:
            pass
        base_fd = os.open(base_path.name, flags, dir_fd=parent_fd)
        base_info = os.fstat(base_fd)
        if (
            not stat.S_ISDIR(base_info.st_mode)
            or (required_device is not None and int(base_info.st_dev) != required_device)
        ):
            raise CleanupError("Bootstrap-Belegbasis ist ungueltig.")
        os.fchmod(base_fd, 0o700)
        run_name = f"run-{run_id}"
        os.mkdir(run_name, 0o700, dir_fd=base_fd)
        fsync_directory(base_fd)
        run_fd = os.open(run_name, flags, dir_fd=base_fd)
        run_info = os.fstat(run_fd)
        if (
            not stat.S_ISDIR(run_info.st_mode)
            or (required_device is not None and int(run_info.st_dev) != required_device)
        ):
            raise CleanupError("Bootstrap-Run-Verzeichnis ist ungueltig.")
        os.fchmod(run_fd, 0o700)
        fsync_directory(run_fd)
        result = base_path / run_name, run_fd, directory_identity(run_info)
        run_fd = None
        return result
    finally:
        if run_fd is not None:
            os.close(run_fd)
        if base_fd is not None:
            os.close(base_fd)
        os.close(parent_fd)


def open_existing_private_run(
    base_path: pathlib.Path, run_id: str
) -> tuple[pathlib.Path, int, tuple[int, int]]:
    if not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise CleanupError("Bootstrap-Run-ID ist ungueltig.")
    flags = require_secure_directory_flags()
    base_fd = os.open(str(base_path), flags)
    try:
        base_info = os.fstat(base_fd)
        base_path_info = os.stat(base_path, follow_symlinks=False)
        if (
            directory_identity(base_info) != directory_identity(base_path_info)
            or stat.S_IMODE(base_info.st_mode) & 0o077
        ):
            raise CleanupError("Bootstrap-Belegbasis ist nicht privat gebunden.")
        run_name = f"run-{run_id}"
        run_fd = os.open(run_name, flags, dir_fd=base_fd)
        run_info = os.fstat(run_fd)
        run_path = base_path / run_name
        run_path_info = os.stat(run_path, follow_symlinks=False)
        if (
            directory_identity(run_info) != directory_identity(run_path_info)
            or stat.S_IMODE(run_info.st_mode) & 0o077
        ):
            os.close(run_fd)
            raise CleanupError("Bootstrap-Run ist nicht privat gebunden.")
        return run_path, run_fd, directory_identity(run_info)
    finally:
        os.close(base_fd)


def marker_with_hash(marker: dict) -> dict:
    result = dict(marker)
    result["marker_sha256"] = canonical_sha256(result)
    return result


def validate_marker(marker: dict, run_id: str, event: str, token_sha256: str) -> dict:
    claimed = validate_hash("Watchdog-Marker-Hash", marker.get("marker_sha256"))
    canonical = dict(marker)
    canonical.pop("marker_sha256", None)
    if (
        marker.get("format") != BOOTSTRAP_MARKER_FORMAT
        or marker.get("run_id") != run_id
        or marker.get("event") != event
        or marker.get("token_sha256") != token_sha256
        or not hmac.compare_digest(canonical_sha256(canonical), claimed)
    ):
        raise WatchdogUnsafeError("Watchdog-Marker ist ungueltig.")
    return marker


def marker_exists(directory_fd: int, name: str) -> bool:
    try:
        info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(info.st_mode) or int(info.st_nlink) != 1:
        raise WatchdogUnsafeError("Watchdog-Markerpfad ist kein einzelner regulaerer Beleg.")
    return True


def bootstrap_watchdog_main(argv: list[str]) -> int:
    watchdog_parser = argparse.ArgumentParser(add_help=False)
    watchdog_parser.add_argument("--run-id", required=True)
    watchdog_parser.add_argument("--master-pidfd", required=True, type=int)
    watchdog_parser.add_argument("--worker-pidfd", required=True, type=int)
    watchdog_parser.add_argument("--deadline-monotonic", required=True, type=float)
    watchdog_parser.add_argument("--parent-identity-sha256", required=True)
    watchdog_parser.add_argument("--master-identity-sha256", required=True)
    watchdog_parser.add_argument("--worker-identity-sha256", required=True)
    args = watchdog_parser.parse_args(argv)
    require_pidfd_support()
    token = os.getenv("GAERTNER_BOOTSTRAP_WATCHDOG_TOKEN", "")
    if not re.fullmatch(r"[0-9a-f]{64}", token):
        raise CleanupError("Watchdog-Starttoken fehlt.")
    token_sha256 = hashlib.sha256(token.encode("ascii")).hexdigest()
    run_path, run_fd, _identity = open_existing_private_run(
        BOOTSTRAP_TEMP_ROOT, args.run_id
    )
    del run_path
    try:
        pidfd_send(args.master_pidfd, 0)
        pidfd_send(args.worker_pidfd, 0)
        watchdog_identity = capture_process_identity(os.getpid())
        ready = marker_with_hash(
            {
                "format": BOOTSTRAP_MARKER_FORMAT,
                "run_id": args.run_id,
                "event": "ready",
                "created_at": utc_now(),
                "token_sha256": token_sha256,
                "deadline_monotonic": args.deadline_monotonic,
                "watchdog_identity": watchdog_identity,
                "parent_identity_sha256": validate_hash(
                    "Watchdog-Parentidentitaet", args.parent_identity_sha256
                ),
                "master_identity_sha256": validate_hash(
                    "Watchdog-Masteridentitaet", args.master_identity_sha256
                ),
                "worker_identity_sha256": validate_hash(
                    "Watchdog-Workeridentitaet", args.worker_identity_sha256
                ),
                "self_test": "pidfd_signal_0_ok",
            }
        )
        atomic_json_at(run_fd, ".watchdog-ready.json", ready)
        remaining = args.deadline_monotonic - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        resume_errors = []
        fired = marker_with_hash(
            {
                "format": BOOTSTRAP_MARKER_FORMAT,
                "run_id": args.run_id,
                "event": "fired",
                "created_at": utc_now(),
                "token_sha256": token_sha256,
                "deadline_monotonic": args.deadline_monotonic,
                "watchdog_identity": watchdog_identity,
                "resume_order": ["worker", "master"],
            }
        )
        try:
            atomic_json_at(run_fd, ".watchdog-fired.json", fired)
        finally:
            for role, pidfd in (
                ("worker", args.worker_pidfd),
                ("master", args.master_pidfd),
            ):
                try:
                    pidfd_send(pidfd, signal.SIGCONT)
                except Exception as exc:
                    resume_errors.append(
                        {"role": role, "error_type": type(exc).__name__, "error": str(exc)}
                    )
        return 0 if not resume_errors else 3
    finally:
        os.close(run_fd)


def start_bootstrap_watchdog(
    run_id: str,
    run_fd: int,
    process_plan: dict,
    master_pidfd: int,
    worker_pidfd: int,
) -> dict:
    require_pidfd_support()
    if marker_exists(run_fd, ".watchdog-ready.json") or marker_exists(
        run_fd, ".watchdog-fired.json"
    ):
        raise CleanupError("Watchdog-Marker existiert bereits.")
    started_monotonic = time.monotonic()
    deadline = started_monotonic + BOOTSTRAP_WATCHDOG_SECONDS
    cutoff = started_monotonic + BOOTSTRAP_MUTATION_CUTOFF_SECONDS
    token = uuid.uuid4().hex + uuid.uuid4().hex
    token_sha256 = hashlib.sha256(token.encode("ascii")).hexdigest()
    parent_identity = capture_process_identity(os.getpid())
    command = [
        sys.executable,
        str(pathlib.Path(__file__).resolve()),
        "--internal-bootstrap-watchdog",
        "--run-id",
        run_id,
        "--master-pidfd",
        str(master_pidfd),
        "--worker-pidfd",
        str(worker_pidfd),
        "--deadline-monotonic",
        repr(deadline),
        "--parent-identity-sha256",
        process_identity_sha256(parent_identity),
        "--master-identity-sha256",
        process_identity_sha256(process_plan["master"]),
        "--worker-identity-sha256",
        process_identity_sha256(process_plan["worker"]),
    ]
    environment = dict(os.environ)
    environment["GAERTNER_BOOTSTRAP_WATCHDOG_TOKEN"] = token
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        pass_fds=(master_pidfd, worker_pidfd),
        start_new_session=True,
        env=environment,
    )
    ready_deadline = min(cutoff, time.monotonic() + 2.0)
    while not marker_exists(run_fd, ".watchdog-ready.json"):
        if process.poll() is not None:
            raise WatchdogUnsafeError("Watchdog endete vor dem Ready-Self-Test.")
        if time.monotonic() >= ready_deadline:
            raise WatchdogUnsafeError("Watchdog-Ready-Self-Test kam nicht rechtzeitig.")
        time.sleep(0.01)
    ready = validate_marker(
        read_small_json_at(run_fd, ".watchdog-ready.json"),
        run_id,
        "ready",
        token_sha256,
    )
    watchdog_identity = capture_process_identity(process.pid)
    if (
        process_identity_projection(ready.get("watchdog_identity") or {})
        != process_identity_projection(watchdog_identity)
        or ready.get("parent_identity_sha256")
        != process_identity_sha256(parent_identity)
        or ready.get("master_identity_sha256")
        != process_identity_sha256(process_plan["master"])
        or ready.get("worker_identity_sha256")
        != process_identity_sha256(process_plan["worker"])
        or float(ready.get("deadline_monotonic") or 0.0) != deadline
        or ready.get("self_test") != "pidfd_signal_0_ok"
    ):
        raise WatchdogUnsafeError("Watchdog-Ready-Beleg ist nicht an Prozess und pidfds gebunden.")
    return {
        "process": process,
        "process_identity": watchdog_identity,
        "parent_identity": parent_identity,
        "token_sha256": token_sha256,
        "started_monotonic": started_monotonic,
        "deadline_monotonic": deadline,
        "cutoff_monotonic": cutoff,
        "ready_marker": ready,
    }


def assert_watchdog_window(
    handle: dict,
    run_fd: int,
    process_plan: dict,
    *,
    require_mutation_window: bool,
) -> dict:
    def live_window() -> tuple[float, float]:
        process = handle["process"]
        if process.poll() is not None:
            raise WatchdogUnsafeError("Watchdog ist vorzeitig beendet.")
        if marker_exists(run_fd, ".watchdog-fired.json"):
            validate_marker(
                read_small_json_at(run_fd, ".watchdog-fired.json"),
                handle["ready_marker"]["run_id"],
                "fired",
                handle["token_sha256"],
            )
            raise WatchdogUnsafeError("Watchdog hat bereits ausgeloest.")
        current_watchdog = capture_process_identity(
            handle["process_identity"]["pid"]
        )
        if process_identity_projection(
            current_watchdog
        ) != process_identity_projection(handle["process_identity"]):
            raise WatchdogUnsafeError("Watchdog-Prozessidentitaet wurde ersetzt.")
        now = time.monotonic()
        remaining = handle["deadline_monotonic"] - now
        if require_mutation_window and (
            now >= handle["cutoff_monotonic"]
            or remaining < BOOTSTRAP_MIN_REMAINING_SECONDS
        ):
            raise WatchdogUnsafeError("Bootstrap-Mutationsfenster ist abgelaufen.")
        return now, remaining

    # Recheck both the full topology/T state and the live window at the tail.
    # This closes both directions of the race: a slow /proc scan may not cross
    # T+8, and a process resumed during the intervening marker/time check may
    # not leave us with a stale stopped-state result.
    live_window()
    assert_process_topology(process_plan, required_state="T")
    live_window()
    stopped = assert_process_topology(process_plan, required_state="T")
    now, remaining = live_window()
    return {
        "checked_monotonic": now,
        "remaining_seconds": remaining,
        "watchdog_fired": False,
        "processes": stopped,
    }


class JournalWriter:
    """Append-only, fsynced JSONL journal with a SHA-256 event chain."""

    def __init__(
        self,
        path: pathlib.Path,
        run_id: str,
        *,
        directory_fd: int | None = None,
    ):
        self.path = path
        self.run_id = run_id
        self.directory_fd = directory_fd
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        if directory_fd is None:
            fd = os.open(str(path), flags, 0o600)
        else:
            fd = os.open(path.name, flags, 0o600, dir_fd=directory_fd)
        self.target = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
        self.event_count = 0
        self.last_event_sha256 = "0" * 64
        self.raw_digest = hashlib.sha256()
        self.closed = False
        if directory_fd is not None:
            fsync_directory(directory_fd)

    def append(self, event: dict) -> dict:
        if self.closed:
            raise CleanupError("Cleanup-Journal ist bereits geschlossen.")
        record = dict(event)
        record.update(
            {
                "run_id": self.run_id,
                "sequence": self.event_count + 1,
                "recorded_at": utc_now(),
                "previous_event_sha256": self.last_event_sha256,
            }
        )
        record["event_sha256"] = canonical_sha256(record)
        encoded = (
            json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
        self.target.write(encoded.decode("utf-8"))
        self.target.flush()
        os.fsync(self.target.fileno())
        self.raw_digest.update(encoded)
        self.event_count += 1
        self.last_event_sha256 = record["event_sha256"]
        return record

    def close(self) -> None:
        if self.closed:
            return
        self.target.flush()
        os.fsync(self.target.fileno())
        self.target.close()
        self.closed = True
        if self.directory_fd is not None:
            fsync_directory(self.directory_fd)

    def checkpoint(self) -> dict:
        """Return a durable chain checkpoint without closing the journal."""
        if self.closed:
            raise CleanupError("Cleanup-Journal ist bereits geschlossen.")
        self.target.flush()
        os.fsync(self.target.fileno())
        if self.directory_fd is not None:
            fsync_directory(self.directory_fd)
        return {
            "event_count": self.event_count,
            "last_event_sha256": self.last_event_sha256,
            "journal_sha256": self.raw_digest.hexdigest(),
        }

    def summary(self) -> dict:
        if not self.closed:
            raise CleanupError("Cleanup-Journal muss vor der Zusammenfassung geschlossen sein.")
        return {
            "event_count": self.event_count,
            "last_event_sha256": self.last_event_sha256,
            "journal_sha256": self.raw_digest.hexdigest(),
        }


def evidence_for(entry: dict) -> dict:
    candidate = entry["candidate"]
    return {
        "relative_path": candidate["relative_path"],
        "size": int(candidate["size"]),
        "sha256": candidate["sha256"],
        "datei_ids": [int(ref["row_id"]) for ref in candidate["references"]],
        "preflight_file_identity": list(entry["identity"]),
    }


def close_entry(entry: dict, *, strict: bool = False) -> None:
    fd = entry.get("fd")
    if fd is None:
        return
    entry["fd"] = None
    try:
        os.close(fd)
    except OSError:
        if strict:
            raise


def stage_selected_files(
    opened: list[dict], root_fd: int, pending_fd: int, journal: JournalWriter
) -> list[dict]:
    staged = []
    for entry in opened:
        candidate = entry["candidate"]
        name = candidate["relative_path"]
        held = os.fstat(entry["fd"])
        current = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        if file_identity(held) != entry["identity"] or file_identity(current) != entry["identity"]:
            raise CleanupError("Kandidat wurde vor dem Staging ersetzt oder veraendert.")
        evidence = evidence_for(entry)
        journal.append({"event": "stage_intent", **evidence})
        rename_noreplace(
            name,
            name,
            source_dir_fd=root_fd,
            target_dir_fd=pending_fd,
        )
        entry["state"] = "staged"
        staged.append(entry)
        fsync_directory(root_fd)
        fsync_directory(pending_fd)
        pending = os.stat(name, dir_fd=pending_fd, follow_symlinks=False)
        held_after = os.fstat(entry["fd"])
        if (
            not stat.S_ISREG(pending.st_mode)
            or post_rename_identity(pending) != post_rename_identity(held_after)
        ):
            raise CleanupError("Gestagter Kandidat ist nicht mehr das gepruefte Dateiobjekt.")
        try:
            os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise CleanupError("Kandidatenname wurde waehrend des Staging neu belegt.")
        journal.append(
            {
                "event": "staged",
                **evidence,
                "staged_file_identity": list(file_identity(pending)),
            }
        )
    return staged


def append_restore_journal_best_effort(
    journal: JournalWriter | None,
    event: dict,
    errors: list[dict],
    *,
    phase: str,
    relative_path: str,
) -> bool:
    """Journal restore evidence without ever gating the filesystem recovery."""
    if journal is None:
        return True
    try:
        journal.append(event)
        return True
    except Exception as exc:
        errors.append(
            {
                "phase": phase,
                "relative_path": relative_path,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        )
        return False


def restore_staged_files(
    staged: list[dict],
    root_fd: int,
    pending_fd: int,
    journal: JournalWriter | None,
) -> tuple[list[dict], list[dict]]:
    restored = []
    errors = []
    for entry in reversed(staged):
        if entry.get("state") != "staged":
            continue
        name = entry["candidate"]["relative_path"]
        evidence = evidence_for(entry)
        try:
            pending = os.stat(name, dir_fd=pending_fd, follow_symlinks=False)
            held = os.fstat(entry["fd"])
            if post_rename_identity(pending) != post_rename_identity(held):
                raise CleanupError("Gestagte Datei stimmt beim Restore nicht mit dem FD ueberein.")
            try:
                os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise CleanupError("Restore wuerde einen vorhandenen Upload-Pfad ueberschreiben.")
            # link(2) is an atomic no-replace operation. A plain rename could
            # overwrite an upload created between the EEXIST check and restore.
            # Recovery must happen before any best-effort journal write: when
            # the filesystem is full, evidence I/O must never keep the only
            # verified copy stranded in the private pending directory.
            os.link(
                name,
                name,
                src_dir_fd=pending_fd,
                dst_dir_fd=root_fd,
                follow_symlinks=False,
            )
            fsync_directory(root_fd)
            append_restore_journal_best_effort(
                journal,
                {"event": "restore_linked", **evidence},
                errors,
                phase="journal_restore_linked",
                relative_path=name,
            )
            restored_info = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            pending_after_link = os.stat(
                name, dir_fd=pending_fd, follow_symlinks=False
            )
            held_after = os.fstat(entry["fd"])
            if (
                post_rename_identity(restored_info) != post_rename_identity(held_after)
                or post_rename_identity(pending_after_link)
                != post_rename_identity(held_after)
            ):
                raise CleanupError("Wiederhergestellter Upload stimmt nicht mit dem FD ueberein.")
            os.unlink(name, dir_fd=pending_fd)
            entry["state"] = "restored"
            restored.append(evidence)
            fsync_directory(pending_fd)
            fsync_directory(root_fd)
            restored_info = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            held_after = os.fstat(entry["fd"])
            if post_rename_identity(restored_info) != post_rename_identity(held_after):
                raise CleanupError("Wiederhergestellter Upload stimmt nicht mit dem FD ueberein.")
            append_restore_journal_best_effort(
                journal,
                {"event": "restored", **evidence},
                errors,
                phase="journal_restored",
                relative_path=name,
            )
        except Exception as exc:
            errors.append(
                {
                    "relative_path": name,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            append_restore_journal_best_effort(
                journal,
                {"event": "restore_failed", **evidence, "error": str(exc)},
                errors,
                phase="journal_restore_failed",
                relative_path=name,
            )
    return restored, errors


def bootstrap_path_state(root_fd: int, name: str, held_fd: int | None) -> dict:
    try:
        info = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError:
        return {"relative_path": name, "exists": False, "matches_held_fd": False}
    result = {
        "relative_path": name,
        "exists": True,
        "regular_file": stat.S_ISREG(info.st_mode),
        "file_identity": list(file_identity(info)),
        "matches_held_fd": False,
    }
    if held_fd is not None:
        try:
            result["matches_held_fd"] = (
                post_rename_identity(info)
                == post_rename_identity(os.fstat(held_fd))
            )
        except OSError as exc:
            result["identity_error"] = {
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
    return result


def restore_bootstrap_hidden(
    entry: dict,
    root_fd: int,
    hidden_name: str,
    journal: JournalWriter | None,
) -> tuple[list[dict], list[dict]]:
    restored = []
    errors = []
    if entry.get("state") != "hidden":
        return restored, errors
    original_name = entry["candidate"]["relative_path"]
    evidence = evidence_for(entry)
    try:
        hidden = os.stat(hidden_name, dir_fd=root_fd, follow_symlinks=False)
        held = os.fstat(entry["fd"])
        if post_rename_identity(hidden) != post_rename_identity(held):
            raise CleanupError("Bootstrap-Hidden-Datei stimmt nicht mit dem gehaltenen FD ueberein.")
        try:
            os.stat(original_name, dir_fd=root_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise CleanupError("Bootstrap-Restore wuerde einen vorhandenen Originalpfad ersetzen.")
        rename_noreplace(
            hidden_name,
            original_name,
            source_dir_fd=root_fd,
            target_dir_fd=root_fd,
        )
        entry["state"] = "restored"
        fsync_directory(root_fd)
        restored_info = os.stat(original_name, dir_fd=root_fd, follow_symlinks=False)
        held_after = os.fstat(entry["fd"])
        if post_rename_identity(restored_info) != post_rename_identity(held_after):
            raise CleanupError("Bootstrap-Restore stimmt nicht mit dem gehaltenen FD ueberein.")
        try:
            os.stat(hidden_name, dir_fd=root_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise CleanupError("Bootstrap-Hidden-Pfad blieb nach Restore bestehen.")
        restored.append(evidence)
        append_restore_journal_best_effort(
            journal,
            {"event": "bootstrap_restored", "hidden_name": hidden_name, **evidence},
            errors,
            phase="bootstrap_journal_restored",
            relative_path=original_name,
        )
    except Exception as exc:
        errors.append(
            {
                "phase": "bootstrap_restore",
                "relative_path": original_name,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        )
    return restored, errors


def unlink_staged_files(
    staged: list[dict], pending_fd: int, journal: JournalWriter
) -> list[dict]:
    deleted = []
    for entry in staged:
        if entry.get("state") != "staged":
            raise CleanupError("Nur vollstaendig gestagte Kandidaten duerfen geloescht werden.")
        name = entry["candidate"]["relative_path"]
        evidence = evidence_for(entry)
        pending = os.stat(name, dir_fd=pending_fd, follow_symlinks=False)
        held = os.fstat(entry["fd"])
        if post_rename_identity(pending) != post_rename_identity(held):
            raise CleanupError("Pending-Datei stimmt vor unlink nicht mit dem FD ueberein.")
        journal.append({"event": "unlink_pending_intent", **evidence})
        os.unlink(name, dir_fd=pending_fd)
        entry["state"] = "deleted"
        deleted.append(evidence)
        fsync_directory(pending_fd)
        close_entry(entry, strict=True)
        try:
            os.stat(name, dir_fd=pending_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise CleanupError("Pending-Datei ist nach unlink weiterhin vorhanden.")
        journal.append({"event": "unlinked_pending", **evidence})
    return deleted


def pending_evidence(staged: list[dict], pending_fd: int) -> tuple[list[dict], list[dict]]:
    remaining = []
    uncertain = []
    for entry in staged:
        if entry.get("state") != "staged":
            continue
        evidence = evidence_for(entry)
        try:
            info = os.stat(
                entry["candidate"]["relative_path"],
                dir_fd=pending_fd,
                follow_symlinks=False,
            )
            if not stat.S_ISREG(info.st_mode):
                raise CleanupError("Verbliebener Pending-Eintrag ist keine regulaere Datei.")
            held = os.fstat(entry["fd"])
            if post_rename_identity(info) != post_rename_identity(held):
                raise CleanupError("Verbliebene Pending-Datei stimmt nicht mit dem FD ueberein.")
            remaining.append(dict(evidence, current_file_identity=list(file_identity(info))))
        except Exception as exc:
            uncertain.append(
                {
                    "relative_path": entry["candidate"]["relative_path"],
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
    return remaining, uncertain


def receipt_with_hash(receipt: dict) -> dict:
    result = dict(receipt)
    result["receipt_sha256"] = canonical_sha256(result)
    return result


def execute_cleanup(report: dict, selected: list[dict], root: pathlib.Path, database_url: str) -> dict:
    if not selected:
        raise CleanupError("Kein freigegebener Kandidat ausgewaehlt.")

    run_id = uuid.uuid4().hex
    audit_hash = report["audit_sha256"]
    expected_after = expected_inventory_after(report, selected)
    removed_names = {item["relative_path"] for item in selected}
    remaining_candidates = [
        item for item in report["candidates"] if item["relative_path"] not in removed_names
    ]
    remaining_summary = candidate_summary(remaining_candidates)
    plan = {
        "format": CLEANUP_FORMAT,
        "run_id": run_id,
        "created_at": utc_now(),
        "status": "authorized_scope",
        "audit_sha256": audit_hash,
        "advisory_lock_key": PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
        "database_writes": 0,
        "selected_count": len(selected),
        "selected_bytes": sum(int(item["size"]) for item in selected),
        "selected": selected,
        "expected_after": {
            key: expected_after[key]
            for key in ("file_count", "total_file_bytes", "inventory_sha256")
        },
        "expected_remaining_candidates": remaining_summary,
    }
    plan["plan_sha256"] = canonical_sha256(plan)

    database = None
    root_fd = None
    root_identity = None
    pending_fd = None
    pending_identity = None
    pending_path = None
    journal = None
    opened: list[dict] = []
    staged: list[dict] = []
    deleted: list[dict] = []
    restored: list[dict] = []
    recovery_errors: list[dict] = []
    source_inventory = None
    actual_after = None
    post_unlink_database_verification = None
    plan_path = None
    journal_path = None
    receipt_path = None
    receipt_storage = "persistent_same_filesystem"
    tables_may_be_locked = False
    ledger_capacity = None
    reconciled_setup_runs: list[str] = []

    try:
        root_fd, root_identity = open_upload_root(root)
        # This lock serializes setup reconciliation as well as the cleanup. It
        # must be held before a setup-only run can be classified as abandoned;
        # otherwise a second executor could remove a live first executor's
        # pre-mutation evidence directory.
        database = LockedPostgres(database_url)
        database.acquire_advisory_lock()
        reconciled_setup_runs = reconcile_pending_runs(root, root_identity)
        ledger_capacity = ensure_persistent_ledger_capacity(root_fd, plan)
        setup_receipt = receipt_with_hash(
            {
                "format": CLEANUP_FORMAT,
                "run_id": run_id,
                "created_at": utc_now(),
                "status": "cleanup_setup_incomplete",
                "audit_sha256": audit_hash,
                "plan_sha256": plan["plan_sha256"],
                "database_writes": 0,
                "selected_count": len(selected),
                "selected_bytes": sum(int(item["size"]) for item in selected),
                "deleted_count": 0,
                "restored_count": 0,
                "staged_remaining_count": 0,
                "uncertain": [dict(SETUP_UNCERTAINTY)],
                "ledger_capacity": ledger_capacity,
            }
        )
        pending_path, pending_fd, pending_identity = open_pending_run(
            root, root_identity, run_id, setup_receipt
        )
        assert_root_binding(root, root_fd, root_identity)
        assert_pending_binding(pending_path, pending_fd, pending_identity)
        reserved_names = {
            ".cleanup-plan.json",
            ".cleanup-journal.jsonl",
            ".cleanup-receipt.json",
        }
        if removed_names & reserved_names:
            raise CleanupError("Kandidatenname kollidiert mit persistentem Cleanup-Beleg.")
        plan_path = pending_path / ".cleanup-plan.json"
        journal_path = pending_path / ".cleanup-journal.jsonl"
        receipt_path = pending_path / ".cleanup-receipt.json"
        atomic_json_at(pending_fd, plan_path.name, plan)
        journal = JournalWriter(journal_path, run_id, directory_fd=pending_fd)
        journal.append(
            {
                "event": "run_started",
                "audit_sha256": audit_hash,
                "plan_sha256": plan["plan_sha256"],
                "pending_path": str(pending_path),
                "ledger_capacity": ledger_capacity,
                "reconciled_setup_runs": reconciled_setup_runs,
            }
        )

        journal.append(
            {
                "event": "advisory_lock_acquired",
                "advisory_lock_key": PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
            }
        )

        assert_root_binding(root, root_fd, root_identity)
        source_inventory = scan_inventory(root)
        assert_root_binding(root, root_fd, root_identity)
        if source_inventory != report["source_inventory"]:
            raise CleanupError("Live-Inventar weicht vor dem Preflight vom Audit ab.")

        preflight = preflight_database(database_url, report, selected)
        journal.append(
            {
                "event": "database_preflight_complete",
                "row_signatures_sha256": preflight["row_signatures_sha256"],
                "row_signature_count": len(preflight["row_signatures"]),
            }
        )
        for candidate in selected:
            fd, identity = open_verified_candidate(root_fd, candidate)
            opened.append(
                {
                    "candidate": candidate,
                    "fd": fd,
                    "identity": identity,
                    "state": "opened",
                }
            )
        assert_root_binding(root, root_fd, root_identity)
        journal.append({"event": "file_preflight_complete", "count": len(opened)})

        tables_may_be_locked = True
        verify_locked_database(database, report, selected, preflight)
        assert_root_binding(root, root_fd, root_identity)
        assert_pending_binding(pending_path, pending_fd, pending_identity)
        if quick_inventory_metadata_fd(root_fd) != expected_inventory_metadata(report):
            raise CleanupError("Live-Dateimetadaten weichen unter Tabellenlock vom Audit ab.")

        journal.append(
            {
                "event": "mutation_arm_intent",
                "selected_count": len(selected),
                "selected_bytes": sum(int(item["size"]) for item in selected),
            }
        )
        armed_journal = journal.checkpoint()
        mutation_armed_receipt = receipt_with_hash(
            {
                "format": CLEANUP_FORMAT,
                "run_id": run_id,
                "created_at": utc_now(),
                "status": "cleanup_mutation_armed",
                "audit_sha256": audit_hash,
                "plan_sha256": plan["plan_sha256"],
                "plan_path": str(plan_path),
                "journal_path": str(journal_path),
                **armed_journal,
                "pending_path": str(pending_path),
                "receipt_storage": receipt_storage,
                "ledger_capacity": ledger_capacity,
                "advisory_lock_key": PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
                "database_writes": 0,
                "selected_count": len(selected),
                "selected_bytes": sum(int(item["size"]) for item in selected),
                "selected": selected,
                "deleted_count": 0,
                "deleted_bytes": 0,
                "deleted": [],
                "restored_count": 0,
                "restored_bytes": 0,
                "restored": [],
                "staged_remaining_count": 0,
                "staged_remaining_bytes": 0,
                "staged_remaining": [],
                "uncertain_count": 1,
                "uncertain": [
                    {
                        "phase": "mutation_armed",
                        "error": (
                            "Upload-Mutation kann nach diesem dauerhaft fsyncten "
                            "Beleg begonnen haben; Lauf niemals automatisch entfernen."
                        ),
                    }
                ],
                "after_inventory": None,
                "remaining_candidates": remaining_summary,
                "post_unlink_database_verification": None,
            }
        )
        assert_root_binding(root, root_fd, root_identity)
        assert_pending_binding(pending_path, pending_fd, pending_identity)
        atomic_json_at(pending_fd, receipt_path.name, mutation_armed_receipt)
        assert_pending_binding(pending_path, pending_fd, pending_identity)

        staged = stage_selected_files(opened, root_fd, pending_fd, journal)
        if len(staged) != len(selected):
            raise CleanupError("Nicht alle Kandidaten wurden vollstaendig gestagt.")
        if quick_inventory_metadata_fd(root_fd) != expected_inventory_metadata(
            {"source_inventory": expected_after}
        ):
            raise CleanupError("Upload-Metadaten stimmen nach dem Staging nicht.")
        journal.append({"event": "all_selected_staged", "count": len(staged)})

        deleted = unlink_staged_files(staged, pending_fd, journal)
        if len(deleted) != len(selected):
            raise CleanupError("Nicht alle gestagten Kandidaten wurden geloescht.")
        if quick_inventory_metadata_fd(root_fd) != expected_inventory_metadata(
            {"source_inventory": expected_after}
        ):
            raise CleanupError("Upload-Metadaten stimmen nach unlink nicht.")
        journal.append({"event": "all_selected_unlinked", "count": len(deleted)})
        assert_pending_binding(pending_path, pending_fd, pending_identity)

        # Full hashing is intentionally outside the transaction-scoped table locks.
        database.release_table_locks()
        tables_may_be_locked = False
        journal.append({"event": "table_locks_released"})
        assert_root_binding(root, root_fd, root_identity)
        actual_after = scan_inventory(root)
        assert_root_binding(root, root_fd, root_identity)
        if actual_after != expected_after:
            raise CleanupError(
                "Live-Inventar stimmt nach unlink nicht mit dem erwarteten Zustand ueberein."
            )
        journal.append(
            {
                "event": "postscan_complete",
                "inventory_sha256": actual_after["inventory_sha256"],
                "file_count": actual_after["file_count"],
                "total_file_bytes": actual_after["total_file_bytes"],
            }
        )
        assert_pending_binding(pending_path, pending_fd, pending_identity)

        postflight = preflight_database(database_url, report, selected)
        if (
            postflight["row_signatures"] != preflight["row_signatures"]
            or postflight["row_signatures_sha256"]
            != preflight["row_signatures_sha256"]
        ):
            raise CleanupError(
                "DB-Originale oder Zeilenversionen weichen nach unlink vom Preflight ab."
            )
        post_unlink_database_verification = {
            "verified": True,
            "row_signature_count": len(postflight["row_signatures"]),
            "row_signatures_sha256": postflight["row_signatures_sha256"],
            "matches_preflight": True,
        }
        journal.append(
            {
                "event": "post_unlink_database_verification_complete",
                **post_unlink_database_verification,
            }
        )

        closing_database = database
        database = None
        closing_database.close()
        journal.append({"event": "advisory_lock_released"})
        journal.append({"event": "run_complete", "deleted_count": len(deleted)})
        journal.close()
        journal_summary = journal.summary()
        receipt = receipt_with_hash(
            {
                "format": CLEANUP_FORMAT,
                "run_id": run_id,
                "created_at": utc_now(),
                "status": "cleanup_complete",
                "audit_sha256": audit_hash,
                "plan_sha256": plan["plan_sha256"],
                "plan_path": str(plan_path),
                "journal_path": str(journal_path),
                **journal_summary,
                "pending_path": str(pending_path),
                "receipt_storage": receipt_storage,
                "ledger_capacity": ledger_capacity,
                "advisory_lock_key": PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
                "database_writes": 0,
                "selected_count": len(selected),
                "selected_bytes": sum(int(item["size"]) for item in selected),
                "selected": selected,
                "deleted_count": len(deleted),
                "deleted_bytes": sum(item["size"] for item in deleted),
                "deleted": deleted,
                "restored_count": 0,
                "restored_bytes": 0,
                "restored": [],
                "staged_remaining_count": 0,
                "staged_remaining_bytes": 0,
                "staged_remaining": [],
                "uncertain_count": 0,
                "uncertain": [],
                "after_inventory": {
                    key: actual_after[key]
                    for key in ("file_count", "total_file_bytes", "inventory_sha256")
                },
                "remaining_candidates": remaining_summary,
                "post_unlink_database_verification": post_unlink_database_verification,
            }
        )
        assert_pending_binding(pending_path, pending_fd, pending_identity)
        atomic_json_at(pending_fd, receipt_path.name, receipt)
        return dict(receipt, receipt_path=str(receipt_path))
    except Exception as exc:
        primary_error = exc
        # Helper-local lists are unavailable when a staged rename or unlink raises.
        # Reconstruct the authoritative state from each entry immediately updated
        # after its filesystem mutation.
        staged = [entry for entry in opened if entry.get("state") != "opened"]
        known_deleted_names = {item["relative_path"] for item in deleted}
        for entry in opened:
            if entry.get("state") == "deleted":
                evidence = evidence_for(entry)
                if evidence["relative_path"] not in known_deleted_names:
                    deleted.append(evidence)
                    known_deleted_names.add(evidence["relative_path"])
        if staged and root_fd is not None and pending_fd is not None:
            newly_restored, restore_errors = restore_staged_files(
                staged, root_fd, pending_fd, journal
            )
            restored.extend(newly_restored)
            recovery_errors.extend(restore_errors)

        if database is not None and tables_may_be_locked:
            try:
                database.release_table_locks()
                tables_may_be_locked = False
                if journal is not None:
                    journal.append({"event": "table_locks_released_after_failure"})
            except Exception as release_exc:
                recovery_errors.append(
                    {
                        "phase": "release_table_locks",
                        "error_type": type(release_exc).__name__,
                        "error": str(release_exc),
                    }
                )

        upload_mutated = any(
            entry.get("state") != "opened" for entry in opened
        )
        if root_fd is not None and root_identity is not None and upload_mutated:
            try:
                assert_root_binding(root, root_fd, root_identity)
                actual_after = scan_inventory(root)
                assert_root_binding(root, root_fd, root_identity)
            except Exception as scan_exc:
                recovery_errors.append(
                    {
                        "phase": "failure_postscan",
                        "error_type": type(scan_exc).__name__,
                        "error": str(scan_exc),
                    }
                )
                actual_after = None
        elif source_inventory is not None:
            actual_after = source_inventory

        if database is not None:
            closing_database = database
            database = None
            try:
                closing_database.close()
            except Exception as close_exc:
                recovery_errors.append(
                    {
                        "phase": "release_advisory_lock",
                        "error_type": type(close_exc).__name__,
                        "error": str(close_exc),
                    }
                )

        if pending_fd is None:
            try:
                pending_path, pending_fd, pending_identity = open_fallback_run(run_id)
                receipt_storage = "temporary_fallback"
                plan_path = pending_path / ".cleanup-plan.json"
                journal_path = pending_path / ".cleanup-journal.jsonl"
                receipt_path = pending_path / ".cleanup-receipt.json"
                atomic_json_at(pending_fd, plan_path.name, plan)
                journal = JournalWriter(journal_path, run_id, directory_fd=pending_fd)
                journal.append(
                    {
                        "event": "fallback_receipt_started",
                        "audit_sha256": audit_hash,
                        "plan_sha256": plan["plan_sha256"],
                        "original_error_type": type(primary_error).__name__,
                        "original_error": str(primary_error),
                    }
                )
            except Exception as fallback_exc:
                recovery_errors.append(
                    {
                        "phase": "create_fallback_receipt",
                        "error_type": type(fallback_exc).__name__,
                        "error": str(fallback_exc),
                    }
                )

        staged_remaining = []
        uncertain = list(recovery_errors)
        if pending_fd is not None:
            remaining, pending_uncertain = pending_evidence(staged, pending_fd)
            staged_remaining.extend(remaining)
            uncertain.extend(pending_uncertain)
        absent_candidate_names = {
            item["relative_path"] for item in deleted + staged_remaining
        }
        failure_remaining_summary = candidate_summary(
            [
                item
                for item in report["candidates"]
                if item["relative_path"] not in absent_candidate_names
            ]
        )
        status = (
            "cleanup_partial"
            if deleted or staged_remaining or uncertain
            else "cleanup_failed_no_deletion"
        )

        journal_summary = {
            "event_count": 0,
            "last_event_sha256": "0" * 64,
            "journal_sha256": "",
        }
        if journal is not None:
            try:
                journal.append(
                    {
                        "event": "run_failed",
                        "status": status,
                        "error_type": type(primary_error).__name__,
                        "error": str(primary_error),
                        "deleted_count": len(deleted),
                        "restored_count": len(restored),
                        "staged_remaining_count": len(staged_remaining),
                    }
                )
            except Exception as journal_exc:
                uncertain.append(
                    {
                        "phase": "terminal_journal_event",
                        "error_type": type(journal_exc).__name__,
                        "error": str(journal_exc),
                    }
                )
            try:
                journal.close()
                journal_summary = journal.summary()
            except Exception as journal_close_exc:
                uncertain.append(
                    {
                        "phase": "close_journal",
                        "error_type": type(journal_close_exc).__name__,
                        "error": str(journal_close_exc),
                    }
                )

        failure_receipt = receipt_with_hash(
            {
                "format": CLEANUP_FORMAT,
                "run_id": run_id,
                "created_at": utc_now(),
                "status": status,
                "audit_sha256": audit_hash,
                "plan_sha256": plan["plan_sha256"],
                "plan_path": str(plan_path) if plan_path else None,
                "journal_path": str(journal_path) if journal_path else None,
                **journal_summary,
                "receipt_path": str(receipt_path) if receipt_path else None,
                "pending_path": str(pending_path) if pending_path else None,
                "receipt_storage": receipt_storage,
                "ledger_capacity": ledger_capacity,
                "advisory_lock_key": PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
                "database_writes": 0,
                "error_type": type(primary_error).__name__,
                "error": str(primary_error),
                "selected_count": len(selected),
                "selected_bytes": sum(int(item["size"]) for item in selected),
                "selected": selected,
                "deleted_count": len(deleted),
                "deleted_bytes": sum(item["size"] for item in deleted),
                "deleted": deleted,
                "restored_count": len(restored),
                "restored_bytes": sum(item["size"] for item in restored),
                "restored": restored,
                "staged_remaining_count": len(staged_remaining),
                "staged_remaining_bytes": sum(
                    item["size"] for item in staged_remaining
                ),
                "staged_remaining": staged_remaining,
                "uncertain_count": len(uncertain),
                "uncertain": uncertain,
                "after_inventory": (
                    {
                        key: actual_after[key]
                        for key in ("file_count", "total_file_bytes", "inventory_sha256")
                    }
                    if actual_after is not None
                    else None
                ),
                "remaining_candidates": failure_remaining_summary,
                "post_unlink_database_verification": post_unlink_database_verification,
            }
        )
        receipt_error = None
        if pending_fd is not None and receipt_path is not None:
            try:
                atomic_json_at(pending_fd, receipt_path.name, failure_receipt)
            except Exception as write_exc:
                receipt_error = {
                    "error_type": type(write_exc).__name__,
                    "error": str(write_exc),
                }
        details = dict(failure_receipt)
        if receipt_error is not None:
            details["receipt_write_error"] = receipt_error
        raise CleanupRunError(str(primary_error), details) from primary_error
    finally:
        for entry in opened:
            close_entry(entry)
        if database is not None:
            try:
                database.close()
            except Exception:
                pass
        if journal is not None and not journal.closed:
            try:
                journal.close()
            except Exception:
                pass
        if pending_fd is not None:
            try:
                os.close(pending_fd)
            except OSError:
                pass
        if root_fd is not None:
            try:
                os.close(root_fd)
            except OSError:
                pass


def bootstrap_capacity_plan(
    report: dict, remaining_candidates: list[dict]
) -> tuple[dict, int]:
    final_expected = expected_inventory_after(report, report["candidates"])
    capacity_plan = {
        "format": CLEANUP_FORMAT,
        "run_id": "remaining-batch-capacity-estimate",
        "created_at": report["created_at"],
        "status": "authorized_scope",
        "audit_sha256": report["audit_sha256"],
        "advisory_lock_key": PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
        "database_writes": 0,
        "selected_count": len(remaining_candidates),
        "selected_bytes": sum(int(item["size"]) for item in remaining_candidates),
        "selected": remaining_candidates,
        "expected_after": {
            key: final_expected[key]
            for key in ("file_count", "total_file_bytes", "inventory_sha256")
        },
        "expected_remaining_candidates": candidate_summary([]),
    }
    capacity_plan["plan_sha256"] = canonical_sha256(capacity_plan)
    return capacity_plan, estimated_persistent_ledger_bytes(capacity_plan)


def bootstrap_watchdog_contract(script_sha256: str) -> dict:
    return {
        "script_sha256": script_sha256,
        "duration_seconds": BOOTSTRAP_WATCHDOG_SECONDS,
        "mutation_cutoff_seconds": BOOTSTRAP_MUTATION_CUTOFF_SECONDS,
        "minimum_remaining_seconds": BOOTSTRAP_MIN_REMAINING_SECONDS,
        "stop_order": ["master", "worker"],
        "resume_order": ["worker", "master"],
        "pidfd_only": True,
        "detached": True,
    }


def prepare_bootstrap_canary(
    report: dict,
    candidate: dict,
    remaining_candidates: list[dict],
    root: pathlib.Path,
    database_url: str,
) -> dict:
    """Read-only /var/data+DB preparation; only its /tmp plan is written."""
    require_pidfd_support()
    run_id = uuid.uuid4().hex
    hidden_name = bootstrap_hidden_name(run_id, candidate)
    expected_after = expected_inventory_after(report, [candidate])
    remaining_summary = candidate_summary(remaining_candidates)
    capacity_plan, required_ledger_bytes = bootstrap_capacity_plan(
        report, remaining_candidates
    )
    root_fd = None
    candidate_fd = None
    temp_fd = None
    try:
        root_fd, root_identity = open_upload_root(root)
        source_inventory = scan_inventory(root)
        assert_root_binding(root, root_fd, root_identity)
        if source_inventory != report["source_inventory"]:
            raise CleanupError("Bootstrap-PREPARE-Inventar weicht vom Audit ab.")
        database_preflight = preflight_database(database_url, report, [candidate])
        candidate_fd, candidate_identity = open_verified_candidate(root_fd, candidate)
        held = os.fstat(candidate_fd)
        allocated_bytes = int(getattr(held, "st_blocks", -1)) * 512
        if (
            allocated_bytes != BOOTSTRAP_CANDIDATE_ALLOCATED_BYTES
            or allocated_bytes < required_ledger_bytes
        ):
            raise CleanupError("Bootstrap-PREPARE-Blockallokation ist nicht exakt freigegeben.")
        if len(os.fsencode(hidden_name)) != len(os.fsencode(candidate["relative_path"])):
            raise CleanupError("Bootstrap-Hidden-Name ist nicht laengengleich.")
        if bootstrap_path_state(root_fd, hidden_name, candidate_fd)["exists"]:
            raise CleanupError("Vorbereiteter Bootstrap-Hidden-Pfad existiert bereits.")
        processes = scan_portal_processes()
        script_sha256 = sha256_regular_file(pathlib.Path(__file__).resolve())
        render_commit = str(os.getenv("RENDER_GIT_COMMIT") or "").strip().lower()
        if not re.fullmatch(r"[0-9a-f]{40,64}", render_commit):
            raise CleanupError("RENDER_GIT_COMMIT fehlt fuer den Bootstrap-Plan.")
        pre_capacity = capacity_snapshot(root_fd, required_ledger_bytes)
        if pre_capacity["available_bytes"] >= required_ledger_bytes:
            raise CleanupError("Bootstrap-PREPARE wird bei ausreichendem persistentem Speicher verweigert.")
        plan = {
            "format": BOOTSTRAP_PLAN_FORMAT,
            "run_id": run_id,
            "created_at": utc_now(),
            "status": "bootstrap_prepared_read_only",
            "script_sha256": script_sha256,
            "render_git_commit": render_commit,
            "boot_id": read_boot_id(),
            "upload_root": str(root),
            "upload_root_identity": list(root_identity),
            "audit_sha256": report["audit_sha256"],
            "master_manifest_sha256": report["local_evidence"][
                "master_manifest_sha256"
            ],
            "database_writes": 0,
            "candidate": candidate,
            "candidate_file_identity": list(candidate_identity),
            "candidate_allocated_bytes": allocated_bytes,
            "hidden_name": hidden_name,
            "expected_after": {
                key: expected_after[key]
                for key in ("file_count", "total_file_bytes", "inventory_sha256")
            },
            "remaining_candidates": remaining_summary,
            "required_remaining_ledger_bytes": required_ledger_bytes,
            "remaining_capacity_plan_sha256": capacity_plan["plan_sha256"],
            "pre_capacity": pre_capacity,
            "database_preflight": {
                "row_signature_count": len(database_preflight["row_signatures"]),
                "row_signatures_sha256": database_preflight[
                    "row_signatures_sha256"
                ],
            },
            "processes": processes,
            "watchdog_contract": bootstrap_watchdog_contract(script_sha256),
        }
        plan["plan_sha256"] = canonical_sha256(plan)
        temp_path, temp_fd, _temp_identity = create_private_run(
            BOOTSTRAP_TEMP_ROOT, run_id
        )
        atomic_json_at(temp_fd, ".bootstrap-plan.json", plan)
        return dict(
            plan,
            plan_path=str(temp_path / ".bootstrap-plan.json"),
        )
    finally:
        if candidate_fd is not None:
            os.close(candidate_fd)
        if temp_fd is not None:
            os.close(temp_fd)
        if root_fd is not None:
            os.close(root_fd)


def validate_prepared_bootstrap_plan(
    plan: dict,
    *,
    run_id: str,
    expected_plan_sha256: str,
    report: dict,
    candidate: dict,
    remaining_candidates: list[dict],
    root_identity: tuple[int, int],
) -> dict:
    expected_keys = {
        "format",
        "run_id",
        "created_at",
        "status",
        "script_sha256",
        "render_git_commit",
        "boot_id",
        "upload_root",
        "upload_root_identity",
        "audit_sha256",
        "master_manifest_sha256",
        "database_writes",
        "candidate",
        "candidate_file_identity",
        "candidate_allocated_bytes",
        "hidden_name",
        "expected_after",
        "remaining_candidates",
        "required_remaining_ledger_bytes",
        "remaining_capacity_plan_sha256",
        "pre_capacity",
        "database_preflight",
        "processes",
        "watchdog_contract",
        "plan_sha256",
    }
    if not isinstance(plan, dict) or set(plan) != expected_keys:
        raise CleanupError("Bootstrap-Plan hat unerwartete Felder.")
    claimed = validate_hash("Bootstrap-Plan-Hash", plan.get("plan_sha256"))
    expected = validate_hash("Erwarteter Bootstrap-Plan-Hash", expected_plan_sha256)
    canonical = dict(plan)
    canonical.pop("plan_sha256", None)
    script_sha256 = sha256_regular_file(pathlib.Path(__file__).resolve())
    capacity_plan, required_bytes = bootstrap_capacity_plan(
        report, remaining_candidates
    )
    expected_after = expected_inventory_after(report, [candidate])
    if not (
        hmac.compare_digest(canonical_sha256(canonical), claimed)
        and hmac.compare_digest(claimed, expected)
        and plan.get("format") == BOOTSTRAP_PLAN_FORMAT
        and plan.get("run_id") == run_id
        and plan.get("status") == "bootstrap_prepared_read_only"
        and plan.get("script_sha256") == script_sha256
        and plan.get("render_git_commit")
        == str(os.getenv("RENDER_GIT_COMMIT") or "").strip().lower()
        and plan.get("boot_id") == read_boot_id()
        and plan.get("upload_root") == str(EXPECTED_UPLOAD_ROOT)
        and plan.get("upload_root_identity") == list(root_identity)
        and plan.get("audit_sha256") == report["audit_sha256"]
        and plan.get("master_manifest_sha256")
        == report["local_evidence"]["master_manifest_sha256"]
        and int(plan.get("database_writes", -1)) == 0
        and plan.get("candidate") == candidate
        and int(plan.get("candidate_allocated_bytes") or -1)
        == BOOTSTRAP_CANDIDATE_ALLOCATED_BYTES
        and plan.get("hidden_name") == bootstrap_hidden_name(run_id, candidate)
        and plan.get("expected_after")
        == {
            key: expected_after[key]
            for key in ("file_count", "total_file_bytes", "inventory_sha256")
        }
        and plan.get("remaining_candidates") == candidate_summary(remaining_candidates)
        and int(plan.get("required_remaining_ledger_bytes") or -1) == required_bytes
        and plan.get("remaining_capacity_plan_sha256")
        == capacity_plan["plan_sha256"]
        and plan.get("watchdog_contract")
        == bootstrap_watchdog_contract(script_sha256)
    ):
        raise CleanupError("Bootstrap-Plan ist nicht exakt an Lauf, Audit und Skript gebunden.")
    processes = plan.get("processes")
    if not isinstance(processes, dict) or set(processes) != {"master", "worker"}:
        raise CleanupError("Bootstrap-Plan-Prozesstopologie fehlt.")
    for role in ("master", "worker"):
        identity = processes[role]
        if set(identity) != {*PROCESS_IDENTITY_FIELDS, "state"}:
            raise CleanupError("Bootstrap-Plan-Prozessidentitaet ist unvollstaendig.")
        validate_hash("Prozess-cmdline-Hash", identity["cmdline_sha256"])
        validate_hash("Prozess-cgroup-Hash", identity["cgroup_sha256"])
    if processes["worker"]["ppid"] != processes["master"]["pid"]:
        raise CleanupError("Bootstrap-Plan-Prozesstopologie ist ungueltig.")
    return plan


def open_prepared_bootstrap_plan(
    run_id: str,
    expected_plan_sha256: str,
    report: dict,
    candidate: dict,
    remaining_candidates: list[dict],
    root_identity: tuple[int, int],
) -> tuple[dict, pathlib.Path, int, tuple[int, int]]:
    temp_path, temp_fd, temp_identity = open_existing_private_run(
        BOOTSTRAP_TEMP_ROOT, run_id
    )
    try:
        if sorted(os.listdir(temp_fd)) != [".bootstrap-plan.json"]:
            raise CleanupError("Bootstrap-PREPARE-Run ist nicht jungfraeulich.")
        plan = read_small_json_at(temp_fd, ".bootstrap-plan.json")
        validate_prepared_bootstrap_plan(
            plan,
            run_id=run_id,
            expected_plan_sha256=expected_plan_sha256,
            report=report,
            candidate=candidate,
            remaining_candidates=remaining_candidates,
            root_identity=root_identity,
        )
        return plan, temp_path, temp_fd, temp_identity
    except Exception:
        os.close(temp_fd)
        raise


def write_bootstrap_persistent_receipt(
    run_id: str,
    root_identity: tuple[int, int],
    receipt: dict,
) -> tuple[pathlib.Path, int, tuple[int, int]]:
    run_path, run_fd, run_identity = create_private_run(
        BOOTSTRAP_PERSISTENT_ROOT,
        run_id,
        required_device=root_identity[0],
    )
    try:
        atomic_json_at(run_fd, ".bootstrap-receipt.json", receipt)
        return run_path, run_fd, run_identity
    except Exception:
        os.close(run_fd)
        raise


def watchdog_fired(handle: dict | None, run_fd: int | None) -> bool:
    if handle is None or run_fd is None:
        return False
    if not marker_exists(run_fd, ".watchdog-fired.json"):
        return False
    validate_marker(
        read_small_json_at(run_fd, ".watchdog-fired.json"),
        handle["ready_marker"]["run_id"],
        "fired",
        handle["token_sha256"],
    )
    return True


def execute_bootstrap_canary(
    report: dict,
    candidate: dict,
    remaining_candidates: list[dict],
    root: pathlib.Path,
    database_url: str,
    *,
    run_id: str,
    expected_plan_sha256: str,
) -> dict:
    """Execute one prepared, pidfd-guarded zero-space canary."""
    require_pidfd_support()
    root_fd = None
    root_identity = None
    temp_fd = None
    temp_path = None
    temp_identity = None
    persistent_fd = None
    persistent_path = None
    persistent_identity = None
    database = None
    journal = None
    entry = None
    plan = None
    preflight = None
    tables_may_be_locked = False
    watchdog = None
    pidfds: dict[str, int] = {}
    process_plan = None
    source_inventory = None
    actual_after = None
    post_database_verification = None
    pre_capacity = None
    freed_capacity = None
    post_receipt_capacity = None
    allocated_bytes = None
    restored: list[dict] = []
    recovery_errors: list[dict] = []
    journal_errors: list[dict] = []
    resume_results: list[dict] = []
    primary_error: BaseException | None = None
    operation_complete = False
    preliminary_persistent_receipt = None
    expected_after = expected_inventory_after(report, [candidate])
    remaining_summary = candidate_summary(remaining_candidates)
    _capacity_plan, required_ledger_bytes = bootstrap_capacity_plan(
        report, remaining_candidates
    )
    hidden_name = bootstrap_hidden_name(run_id, candidate)
    plan_path = None
    journal_path = None
    temp_receipt_path = None

    try:
        root_fd, root_identity = open_upload_root(root)
        plan, temp_path, temp_fd, temp_identity = open_prepared_bootstrap_plan(
            run_id,
            expected_plan_sha256,
            report,
            candidate,
            remaining_candidates,
            root_identity,
        )
        del temp_identity
        plan_path = temp_path / ".bootstrap-plan.json"
        journal_path = temp_path / ".bootstrap-journal.jsonl"
        temp_receipt_path = temp_path / ".bootstrap-receipt.json"
        ensure_persistent_ledger_capacity(temp_fd, plan)
        journal = JournalWriter(journal_path, run_id, directory_fd=temp_fd)
        journal.append(
            {
                "event": "bootstrap_execute_started",
                "plan_sha256": plan["plan_sha256"],
                "audit_sha256": report["audit_sha256"],
                "hidden_name": hidden_name,
            }
        )

        database = LockedPostgres(database_url)
        database.acquire_advisory_lock()
        journal.append(
            {
                "event": "advisory_lock_acquired",
                "advisory_lock_key": PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
            }
        )
        reconcile_pending_runs(root, root_identity)
        pre_capacity = capacity_snapshot(root_fd, required_ledger_bytes)
        if pre_capacity["available_bytes"] >= required_ledger_bytes:
            raise CleanupError("Bootstrap-EXECUTE wird bei ausreichendem Speicher verweigert.")

        assert_root_binding(root, root_fd, root_identity)
        source_inventory = scan_inventory(root)
        assert_root_binding(root, root_fd, root_identity)
        if source_inventory != report["source_inventory"]:
            raise CleanupError("Bootstrap-EXECUTE-Inventar weicht vom Audit ab.")
        preflight = preflight_database(database_url, report, [candidate])
        if (
            len(preflight["row_signatures"])
            != int(plan["database_preflight"]["row_signature_count"])
            or preflight["row_signatures_sha256"]
            != plan["database_preflight"]["row_signatures_sha256"]
        ):
            raise CleanupError("Bootstrap-DB-Preflight weicht vom vorbereiteten Plan ab.")
        candidate_fd, candidate_identity = open_verified_candidate(root_fd, candidate)
        entry = {
            "candidate": candidate,
            "fd": candidate_fd,
            "identity": candidate_identity,
            "state": "opened",
        }
        held = os.fstat(candidate_fd)
        allocated_bytes = int(getattr(held, "st_blocks", -1)) * 512
        if (
            list(candidate_identity) != plan["candidate_file_identity"]
            or allocated_bytes != int(plan["candidate_allocated_bytes"])
            or allocated_bytes != BOOTSTRAP_CANDIDATE_ALLOCATED_BYTES
            or allocated_bytes < required_ledger_bytes
        ):
            raise CleanupError("Bootstrap-Dateiidentitaet oder Live-Blockallokation weicht ab.")
        if len(os.fsencode(hidden_name)) != len(os.fsencode(candidate["relative_path"])):
            raise CleanupError("Bootstrap-Hidden-Name ist nicht laengengleich.")
        if bootstrap_path_state(root_fd, hidden_name, candidate_fd)["exists"]:
            raise CleanupError("Bootstrap-Hidden-Pfad ist bereits belegt.")

        process_plan = plan["processes"]
        assert_process_topology(process_plan)
        pidfds["master"] = os.pidfd_open(int(process_plan["master"]["pid"]), 0)
        pidfds["worker"] = os.pidfd_open(int(process_plan["worker"]["pid"]), 0)
        assert_pidfd_identity("master", process_plan["master"], pidfds["master"])
        assert_pidfd_identity("worker", process_plan["worker"], pidfds["worker"])
        watchdog = start_bootstrap_watchdog(
            run_id,
            temp_fd,
            process_plan,
            pidfds["master"],
            pidfds["worker"],
        )
        journal.append(
            {
                "event": "watchdog_ready",
                "watchdog_identity": watchdog["process_identity"],
                "deadline_monotonic": watchdog["deadline_monotonic"],
                "cutoff_monotonic": watchdog["cutoff_monotonic"],
                "ready_marker_sha256": watchdog["ready_marker"]["marker_sha256"],
            }
        )

        # Required order: stop the accepting master first, then its sole worker.
        assert_pidfd_identity("master", process_plan["master"], pidfds["master"])
        pidfd_send(pidfds["master"], signal.SIGSTOP)
        wait_for_stopped_process(process_plan["master"])
        assert_pidfd_identity("worker", process_plan["worker"], pidfds["worker"])
        pidfd_send(pidfds["worker"], signal.SIGSTOP)
        wait_for_stopped_process(process_plan["worker"])
        stopped_check = assert_watchdog_window(
            watchdog,
            temp_fd,
            process_plan,
            require_mutation_window=True,
        )
        journal.append(
            {
                "event": "portal_processes_stopped",
                "processes": stopped_check["processes"],
                "remaining_seconds": stopped_check["remaining_seconds"],
            }
        )

        tables_may_be_locked = True
        verify_locked_database(database, report, [candidate], preflight)
        assert_root_binding(root, root_fd, root_identity)
        if quick_inventory_metadata_fd(root_fd) != expected_inventory_metadata(report):
            raise CleanupError("Bootstrap-Inventar weicht unter SHARE-NOWAIT vom Audit ab.")
        held_now = os.fstat(candidate_fd)
        original_now = os.stat(
            candidate["relative_path"], dir_fd=root_fd, follow_symlinks=False
        )
        if (
            file_identity(held_now) != entry["identity"]
            or file_identity(original_now) != entry["identity"]
        ):
            raise CleanupError("Bootstrap-Kandidat wurde vor der Mutation ersetzt.")
        rename_window = assert_watchdog_window(
            watchdog,
            temp_fd,
            process_plan,
            require_mutation_window=True,
        )
        journal.append(
            {
                "event": "bootstrap_mutation_armed",
                "remaining_seconds": rename_window["remaining_seconds"],
                "hidden_name": hidden_name,
            }
        )
        armed_summary = journal.checkpoint()
        armed_receipt = receipt_with_hash(
            {
                "format": BOOTSTRAP_RECEIPT_FORMAT,
                "run_id": run_id,
                "created_at": utc_now(),
                "status": "bootstrap_mutation_armed",
                "plan_sha256": plan["plan_sha256"],
                "audit_sha256": report["audit_sha256"],
                "hidden_name": hidden_name,
                "processes": process_plan,
                "watchdog_identity": watchdog["process_identity"],
                "watchdog_ready_marker_sha256": watchdog["ready_marker"][
                    "marker_sha256"
                ],
                "deadline_monotonic": watchdog["deadline_monotonic"],
                "cutoff_monotonic": watchdog["cutoff_monotonic"],
                "database_writes": 0,
                "candidate": {
                    "relative_path": candidate["relative_path"],
                    "size": int(candidate["size"]),
                    "sha256": candidate["sha256"],
                    "row_id": BOOTSTRAP_CANDIDATE_REFERENCE["row_id"],
                },
                "candidate_file_identity": list(entry["identity"]),
                "candidate_allocated_bytes": allocated_bytes,
                "pre_capacity": pre_capacity,
                **armed_summary,
            }
        )
        atomic_json_at(temp_fd, temp_receipt_path.name, armed_receipt)

        # Live stopped/fired/deadline checks are repeated immediately before
        # each of the only two upload mutations.
        assert_watchdog_window(
            watchdog,
            temp_fd,
            process_plan,
            require_mutation_window=True,
        )
        assert_root_binding(root, root_fd, root_identity)
        rename_noreplace(
            candidate["relative_path"],
            hidden_name,
            source_dir_fd=root_fd,
            target_dir_fd=root_fd,
        )
        entry["state"] = "hidden"
        fsync_directory(root_fd)
        hidden_after = bootstrap_path_state(root_fd, hidden_name, candidate_fd)
        original_after = bootstrap_path_state(
            root_fd, candidate["relative_path"], candidate_fd
        )
        if (
            not hidden_after["exists"]
            or not hidden_after["matches_held_fd"]
            or original_after["exists"]
        ):
            raise CleanupError("Bootstrap-Rename hat keinen eindeutigen Hidden-Inode-Zustand.")

        unlink_window = assert_watchdog_window(
            watchdog,
            temp_fd,
            process_plan,
            require_mutation_window=True,
        )
        hidden_before_unlink = bootstrap_path_state(
            root_fd, hidden_name, candidate_fd
        )
        original_before_unlink = bootstrap_path_state(
            root_fd, candidate["relative_path"], candidate_fd
        )
        if (
            not hidden_before_unlink["exists"]
            or not hidden_before_unlink["matches_held_fd"]
            or original_before_unlink["exists"]
        ):
            raise CleanupError("Bootstrap-Pfade wurden unmittelbar vor unlink veraendert.")
        journal.append(
            {
                "event": "bootstrap_unlink_armed",
                "remaining_seconds": unlink_window["remaining_seconds"],
                "hidden_name": hidden_name,
            }
        )
        assert_watchdog_window(
            watchdog,
            temp_fd,
            process_plan,
            require_mutation_window=True,
        )
        hidden_before_unlink = bootstrap_path_state(
            root_fd, hidden_name, candidate_fd
        )
        if (
            not hidden_before_unlink["exists"]
            or not hidden_before_unlink["matches_held_fd"]
        ):
            raise CleanupError("Bootstrap-Hidden-Inode wechselte vor unlink.")
        os.unlink(hidden_name, dir_fd=root_fd)
        entry["state"] = "deleted"
        fsync_directory(root_fd)
        if bootstrap_path_state(root_fd, hidden_name, candidate_fd)["exists"]:
            raise CleanupError("Bootstrap-Hidden-Pfad blieb nach unlink bestehen.")
        if bootstrap_path_state(
            root_fd, candidate["relative_path"], candidate_fd
        )["exists"]:
            raise CleanupError("Bootstrap-Originalpfad erschien nach unlink erneut.")

        # Closing the final open description is what actually releases blocks.
        close_entry(entry, strict=True)
        if entry.get("fd") is not None:
            raise CleanupError("Bootstrap-Kandidaten-FD blieb nach close gesetzt.")
        freed_capacity = capacity_snapshot(root_fd, required_ledger_bytes)
        preliminary_persistent_receipt = receipt_with_hash(
            {
                "format": BOOTSTRAP_RECEIPT_FORMAT,
                "run_id": run_id,
                "created_at": utc_now(),
                "status": "bootstrap_storage_released",
                "plan_sha256": plan["plan_sha256"],
                "audit_sha256": report["audit_sha256"],
                "script_sha256": plan["script_sha256"],
                "database_writes": 0,
                "candidate": {
                    "relative_path": candidate["relative_path"],
                    "size": int(candidate["size"]),
                    "sha256": candidate["sha256"],
                    "row_id": BOOTSTRAP_CANDIDATE_REFERENCE["row_id"],
                },
                "hidden_name": hidden_name,
                "hidden_name_bytes": len(os.fsencode(hidden_name)),
                "original_name_bytes": len(os.fsencode(candidate["relative_path"])),
                "candidate_file_identity": list(entry["identity"]),
                "candidate_allocated_bytes": allocated_bytes,
                "candidate_fd_closed": True,
                "processes": process_plan,
                "watchdog_identity": watchdog["process_identity"],
                "watchdog_ready_marker_sha256": watchdog["ready_marker"][
                    "marker_sha256"
                ],
                "deadline_monotonic": watchdog["deadline_monotonic"],
                "cutoff_monotonic": watchdog["cutoff_monotonic"],
                "pre_capacity": pre_capacity,
                "freed_capacity_before_receipt": freed_capacity,
                "required_remaining_ledger_bytes": required_ledger_bytes,
            }
        )
        persistent_path, persistent_fd, persistent_identity = (
            write_bootstrap_persistent_receipt(
                run_id, root_identity, preliminary_persistent_receipt
            )
        )
        assert_pending_binding(
            persistent_path, persistent_fd, persistent_identity
        )
        # Only after this compact atomic+fsynced persistent fact exists may
        # additional filesystem probes enrich it.  A failed stat below can no
        # longer erase the evidence that unlink+final-FD-close released space.
        persistent_original_state = bootstrap_path_state(
            root_fd, candidate["relative_path"], None
        )
        persistent_hidden_state = bootstrap_path_state(
            root_fd, hidden_name, None
        )
        post_receipt_capacity = capacity_snapshot(root_fd, required_ledger_bytes)
        storage_receipt = receipt_with_hash(
            {
                **{
                    key: value
                    for key, value in preliminary_persistent_receipt.items()
                    if key != "receipt_sha256"
                },
                "original_state": persistent_original_state,
                "hidden_state": persistent_hidden_state,
                "post_receipt_capacity": post_receipt_capacity,
                "persistent_receipt_path": str(
                    persistent_path / ".bootstrap-receipt.json"
                ),
            }
        )
        atomic_json_at(persistent_fd, ".bootstrap-receipt.json", storage_receipt)
        preliminary_persistent_receipt = storage_receipt

        append_restore_journal_best_effort(
            journal,
            {
                "event": "bootstrap_hidden_unlinked_and_fd_closed",
                "hidden_name": hidden_name,
                "freed_capacity": freed_capacity,
                "post_receipt_capacity": post_receipt_capacity,
            },
            journal_errors,
            phase="bootstrap_post_unlink_journal",
            relative_path=candidate["relative_path"],
        )
        database.release_table_locks()
        tables_may_be_locked = False
        assert_root_binding(root, root_fd, root_identity)
        actual_after = scan_inventory(root)
        assert_root_binding(root, root_fd, root_identity)
        if actual_after != expected_after:
            raise CleanupError("Bootstrap-Nachherinventar stimmt nicht exakt.")
        postflight = preflight_database(database_url, report, [candidate])
        matches_preflight = (
            postflight["row_signatures"] == preflight["row_signatures"]
            and postflight["row_signatures_sha256"]
            == preflight["row_signatures_sha256"]
        )
        post_database_verification = {
            "verified": matches_preflight,
            "matches_preflight": matches_preflight,
            "row_signature_count": len(postflight["row_signatures"]),
            "row_signatures_sha256": postflight["row_signatures_sha256"],
        }
        if not matches_preflight:
            raise CleanupError("Bootstrap-DB-Blob oder Zeilenversion weicht nach unlink ab.")
        post_receipt_capacity = capacity_snapshot(root_fd, required_ledger_bytes)
        if not post_receipt_capacity["sufficient_for_regular_cleanup"]:
            raise CleanupError("Freier Speicher reicht nach persistentem Bootstrap-Beleg nicht aus.")
        if journal_errors:
            raise CleanupError("Bootstrap-Journal ist nach unlink unvollstaendig.")
        operation_complete = True
    except BaseException as exc:
        primary_error = exc
        # Restore is the first recovery action and never depends on journal I/O.
        if entry is not None and entry.get("state") == "hidden" and root_fd is not None:
            safe_to_restore = False
            try:
                if not watchdog_fired(watchdog, temp_fd):
                    assert_watchdog_window(
                        watchdog,
                        temp_fd,
                        process_plan,
                        require_mutation_window=True,
                    )
                    safe_to_restore = True
            except BaseException as restore_guard_exc:
                recovery_errors.append(
                    {
                        "phase": "bootstrap_restore_guard",
                        "error_type": type(restore_guard_exc).__name__,
                        "error": str(restore_guard_exc),
                    }
                )
            if safe_to_restore:
                try:
                    newly_restored, restore_errors = restore_bootstrap_hidden(
                        entry, root_fd, hidden_name, journal
                    )
                    restored.extend(newly_restored)
                    recovery_errors.extend(restore_errors)
                except BaseException as restore_exc:
                    recovery_errors.append(
                        {
                            "phase": "bootstrap_restore_interrupted",
                            "error_type": type(restore_exc).__name__,
                            "error": str(restore_exc),
                        }
                    )
            else:
                recovery_errors.append(
                    {
                        "phase": "bootstrap_restore_skipped",
                        "error": "Watchdog/Prozesszustand erlaubt keine weitere Upload-Mutation.",
                    }
                )
        if entry is not None and entry.get("fd") is not None:
            try:
                close_entry(entry, strict=True)
            except BaseException as close_exc:
                recovery_errors.append(
                    {
                        "phase": "bootstrap_close_candidate_fd",
                        "error_type": type(close_exc).__name__,
                        "error": str(close_exc),
                    }
                )
        if database is not None and tables_may_be_locked:
            try:
                database.release_table_locks()
                tables_may_be_locked = False
            except BaseException as release_exc:
                recovery_errors.append(
                    {
                        "phase": "bootstrap_release_table_locks",
                        "error_type": type(release_exc).__name__,
                        "error": str(release_exc),
                    }
                )
        if root_fd is not None and root_identity is not None:
            try:
                actual_after = scan_inventory(root)
            except BaseException as scan_exc:
                recovery_errors.append(
                    {
                        "phase": "bootstrap_failure_postscan",
                        "error_type": type(scan_exc).__name__,
                        "error": str(scan_exc),
                    }
                )
    finally:
        # Required unconditional pidfd-only order, even if SIGSTOP was partial.
        for role in ("worker", "master"):
            pidfd = pidfds.get(role)
            if pidfd is None:
                continue
            result = {"role": role, "signal": "SIGCONT", "attempted": True}
            try:
                pidfd_send(pidfd, signal.SIGCONT)
                result["sent"] = True
            except BaseException as resume_exc:
                result.update(
                    sent=False,
                    error_type=type(resume_exc).__name__,
                    error=str(resume_exc),
                )
                recovery_errors.append(
                    {
                        "phase": f"bootstrap_resume_{role}",
                        "error_type": type(resume_exc).__name__,
                        "error": str(resume_exc),
                    }
                )
            resume_results.append(result)
        for pidfd in pidfds.values():
            try:
                os.close(pidfd)
            except OSError:
                pass

    if primary_error is None and recovery_errors:
        primary_error = CleanupError("Bootstrap-Prozessfreigabe war nicht vollstaendig.")
    if database is not None:
        closing_database = database
        database = None
        try:
            closing_database.close()
        except BaseException as close_exc:
            recovery_errors.append(
                {
                    "phase": "bootstrap_release_advisory_lock",
                    "error_type": type(close_exc).__name__,
                    "error": str(close_exc),
                }
            )
            if primary_error is None:
                primary_error = close_exc

    journal_summary = {
        "event_count": 0,
        "last_event_sha256": "0" * 64,
        "journal_sha256": "",
    }
    if journal is not None:
        if not journal.closed:
            try:
                journal.append(
                    {
                        "event": (
                            "bootstrap_complete"
                            if primary_error is None and operation_complete
                            else "bootstrap_failed"
                        ),
                        "state": entry.get("state") if entry else "not_opened",
                        "resume_results": resume_results,
                    }
                )
                journal.close()
            except BaseException as journal_exc:
                journal_errors.append(
                    {
                        "phase": "bootstrap_terminal_journal",
                        "error_type": type(journal_exc).__name__,
                        "error": str(journal_exc),
                    }
                )
                if primary_error is None:
                    primary_error = journal_exc
        if journal.closed:
            journal_summary = journal.summary()

    state = entry.get("state") if entry else "not_opened"
    try:
        fired_at_finish = watchdog_fired(watchdog, temp_fd)
    except BaseException as fired_exc:
        fired_at_finish = None
        recovery_errors.append(
            {
                "phase": "bootstrap_watchdog_final_state",
                "error_type": type(fired_exc).__name__,
                "error": str(fired_exc),
            }
        )
        if primary_error is None:
            primary_error = fired_exc
    if primary_error is None and operation_complete:
        status = "bootstrap_complete"
    elif state == "deleted":
        status = "bootstrap_partial_deleted"
    elif state == "hidden":
        status = "bootstrap_recovery_required"
    else:
        status = "bootstrap_failed_no_deletion"
    uncertainty = recovery_errors + journal_errors
    original_state = (
        bootstrap_path_state(root_fd, candidate["relative_path"], None)
        if root_fd is not None
        else None
    )
    hidden_state = (
        bootstrap_path_state(root_fd, hidden_name, None)
        if root_fd is not None
        else None
    )
    terminal_receipt = receipt_with_hash(
        {
            "format": BOOTSTRAP_RECEIPT_FORMAT,
            "run_id": run_id,
            "created_at": utc_now(),
            "status": status,
            "plan_sha256": plan.get("plan_sha256") if plan else expected_plan_sha256,
            "audit_sha256": report["audit_sha256"],
            "script_sha256": plan.get("script_sha256") if plan else None,
            "plan_path": str(plan_path) if plan_path else None,
            "journal_path": str(journal_path) if journal_path else None,
            **journal_summary,
            "database_writes": 0,
            "error_type": type(primary_error).__name__ if primary_error else None,
            "error": str(primary_error) if primary_error else None,
            "candidate": {
                "relative_path": candidate["relative_path"],
                "size": int(candidate["size"]),
                "sha256": candidate["sha256"],
                "row_id": BOOTSTRAP_CANDIDATE_REFERENCE["row_id"],
            },
            "deleted_count": 1 if state == "deleted" else 0,
            "deleted_bytes": int(candidate["size"]) if state == "deleted" else 0,
            "restored_count": len(restored),
            "restored": restored,
            "hidden_name": hidden_name,
            "hidden_name_bytes": len(os.fsencode(hidden_name)),
            "original_name_bytes": len(os.fsencode(candidate["relative_path"])),
            "candidate_fd_closed": entry is None or entry.get("fd") is None,
            "original_state": original_state,
            "hidden_state": hidden_state,
            "processes": process_plan,
            "watchdog_identity": watchdog.get("process_identity") if watchdog else None,
            "watchdog_ready_marker_sha256": (
                watchdog["ready_marker"]["marker_sha256"] if watchdog else None
            ),
            "watchdog_fired_at_finish": fired_at_finish,
            "deadline_monotonic": (
                watchdog.get("deadline_monotonic") if watchdog else None
            ),
            "cutoff_monotonic": watchdog.get("cutoff_monotonic") if watchdog else None,
            "resume_results": resume_results,
            "pre_capacity": pre_capacity,
            "freed_capacity_before_receipt": freed_capacity,
            "post_receipt_capacity": post_receipt_capacity,
            "required_remaining_ledger_bytes": required_ledger_bytes,
            "persistent_receipt_path": (
                str(persistent_path / ".bootstrap-receipt.json")
                if persistent_path is not None
                else None
            ),
            "temporary_receipt_path": (
                str(temp_receipt_path) if temp_receipt_path is not None else None
            ),
            "after_inventory": (
                {
                    key: actual_after[key]
                    for key in ("file_count", "total_file_bytes", "inventory_sha256")
                }
                if actual_after is not None
                else None
            ),
            "remaining_candidates": remaining_summary,
            "post_unlink_database_verification": post_database_verification,
            "uncertain_count": len(uncertainty),
            "uncertain": uncertainty,
        }
    )

    final_write_error = None
    if temp_fd is not None and temp_receipt_path is not None:
        try:
            atomic_json_at(temp_fd, temp_receipt_path.name, terminal_receipt)
        except BaseException as exc:
            final_write_error = exc
    if persistent_fd is not None:
        try:
            assert_pending_binding(persistent_path, persistent_fd, persistent_identity)
            if status == "bootstrap_complete":
                atomic_json_at_with_capacity_guard(
                    persistent_fd,
                    ".bootstrap-receipt.json",
                    terminal_receipt,
                    capacity_fd=root_fd,
                    required_bytes=required_ledger_bytes,
                )
            else:
                atomic_json_at(
                    persistent_fd, ".bootstrap-receipt.json", terminal_receipt
                )
        except BaseException as exc:
            final_write_error = final_write_error or exc
    elif status == "bootstrap_complete":
        final_write_error = CleanupError("Persistenter Bootstrap-Erfolgsbeleg fehlt.")
    if final_write_error is not None and primary_error is None:
        primary_error = final_write_error
        terminal_receipt = receipt_with_hash(
            {
                **{
                    key: value
                    for key, value in terminal_receipt.items()
                    if key != "receipt_sha256"
                },
                "status": "bootstrap_partial_deleted",
                "error_type": type(final_write_error).__name__,
                "error": str(final_write_error),
            }
        )
        if persistent_fd is not None:
            try:
                atomic_json_at(
                    persistent_fd, ".bootstrap-receipt.json", terminal_receipt
                )
            except Exception:
                pass

    for entry_to_close in [entry] if entry is not None else []:
        close_entry(entry_to_close)
    if journal is not None and not journal.closed:
        try:
            journal.close()
        except Exception:
            pass
    if persistent_fd is not None:
        os.close(persistent_fd)
    if temp_fd is not None:
        os.close(temp_fd)
    if root_fd is not None:
        os.close(root_fd)

    if primary_error is not None or not operation_complete:
        raise CleanupRunError(
            str(primary_error or "Bootstrap wurde nicht vollstaendig abgeschlossen."),
            terminal_receipt,
        ) from primary_error
    return terminal_receipt


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--execute", action="store_true")
    result.add_argument("--approval")
    result.add_argument("--audit-path", required=True)
    result.add_argument("--expected-root", required=True)
    result.add_argument("--expected-audit-sha256", required=True)
    result.add_argument("--expected-files", required=True, type=int)
    result.add_argument("--expected-bytes", required=True, type=int)
    result.add_argument("--expected-inventory-sha256", required=True)
    result.add_argument("--expected-candidates", required=True, type=int)
    result.add_argument("--expected-candidate-bytes", required=True, type=int)
    result.add_argument("--expected-candidate-blobs", required=True, type=int)
    result.add_argument("--expected-candidate-index-sha256", required=True)
    result.add_argument("--expected-master-manifest-sha256", required=True)
    result.add_argument("--expected-verification-report-sha256", required=True)
    result.add_argument("--expected-coverage-sha256", required=True)
    result.add_argument("--limit", type=int, default=0)
    result.add_argument("--bootstrap-candidate")
    bootstrap_mode = result.add_mutually_exclusive_group()
    bootstrap_mode.add_argument("--bootstrap-prepare", action="store_true")
    bootstrap_mode.add_argument("--bootstrap-execute", action="store_true")
    result.add_argument("--bootstrap-run-id")
    result.add_argument("--expected-bootstrap-plan-sha256")
    return result


def main(argv=None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    if raw_argv and raw_argv[0] == "--internal-bootstrap-watchdog":
        return bootstrap_watchdog_main(raw_argv[1:])
    args = parser().parse_args(raw_argv)
    root = pathlib.Path(args.expected_root).resolve()
    configured_root = pathlib.Path(os.getenv("UPLOAD_DIR", str(EXPECTED_UPLOAD_ROOT))).resolve()
    if root != EXPECTED_UPLOAD_ROOT or configured_root != EXPECTED_UPLOAD_ROOT:
        raise CleanupError("Cleanup ist ausschliesslich fuer /var/data/uploads freigegeben.")
    if not os.getenv("RENDER"):
        raise CleanupError("Cleanup darf nur in der Render-Laufzeit ausgefuehrt werden.")
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url.startswith(("postgres://", "postgresql://")):
        raise CleanupError("DATABASE_URL ist keine PostgreSQL-Verbindung.")
    report = load_and_validate_audit(pathlib.Path(args.audit_path), args)
    bootstrap_requested = args.bootstrap_prepare or args.bootstrap_execute
    if bootstrap_requested:
        raise CleanupError(
            "Der alte Zero-Space-Bootstrap ist deaktiviert; "
            "dafuer ausschliesslich render_minimal_canary_cleanup.py verwenden."
        )
        if args.limit != 0 or not args.bootstrap_candidate:
            raise CleanupError(
                "Bootstrap verlangt den exakten Canary-Kandidaten; --limit bleibt 0."
            )
        candidate, remaining = validate_bootstrap_candidate(
            report, args.bootstrap_candidate
        )
        if args.bootstrap_prepare:
            if args.execute or args.bootstrap_run_id or args.expected_bootstrap_plan_sha256:
                raise CleanupError("Bootstrap-PREPARE ist strikt read-only und akzeptiert keine Execute-Argumente.")
            prepared = prepare_bootstrap_canary(
                report, candidate, remaining, root, database_url
            )
            summary = {
                "status": prepared["status"],
                "run_id": prepared["run_id"],
                "plan_path": prepared["plan_path"],
                "plan_sha256": prepared["plan_sha256"],
                "audit_sha256": prepared["audit_sha256"],
                "hidden_name": prepared["hidden_name"],
                "hidden_name_bytes": len(os.fsencode(prepared["hidden_name"])),
                "processes": prepared["processes"],
                "watchdog_contract": prepared["watchdog_contract"],
                "database_writes": 0,
            }
            print(
                "RESULT " + json.dumps(summary, ensure_ascii=False, sort_keys=True),
                flush=True,
            )
            return 0
        if (
            not args.execute
            or args.approval != BOOTSTRAP_APPROVAL_PHRASE
            or not args.bootstrap_run_id
            or not args.expected_bootstrap_plan_sha256
        ):
            raise CleanupError("Explizite feste Bootstrap-EXECUTE-Freigabe oder Planbindung fehlt.")
        receipt = execute_bootstrap_canary(
            report,
            candidate,
            remaining,
            root,
            database_url,
            run_id=args.bootstrap_run_id,
            expected_plan_sha256=args.expected_bootstrap_plan_sha256,
        )
    else:
        if (
            args.bootstrap_candidate
            or args.bootstrap_run_id
            or args.expected_bootstrap_plan_sha256
        ):
            raise CleanupError("Bootstrap-Argumente sind ohne PREPARE/EXECUTE-Modus unzulaessig.")
        if not args.execute or args.approval != APPROVAL_PHRASE:
            raise CleanupError("Explizite Cleanup-Freigabe fehlt.")
        selected = select_candidates(report, args.limit)
        receipt = execute_cleanup(report, selected, root, database_url)
    representative_ids = (
        [BOOTSTRAP_CANDIDATE_REFERENCE["row_id"]]
        if receipt["status"].startswith("bootstrap_")
        else sorted(
            {
                row_id
                for item in receipt["deleted"]
                for row_id in item["datei_ids"]
            }
        )
    )
    summary = {
        "status": receipt["status"],
        "receipt_path": receipt.get("persistent_receipt_path")
        or receipt.get("receipt_path"),
        "receipt_sha256": receipt["receipt_sha256"],
        "journal_path": receipt["journal_path"],
        "journal_sha256": receipt["journal_sha256"],
        "last_event_sha256": receipt["last_event_sha256"],
        "database_writes": 0,
        "deleted_count": receipt["deleted_count"],
        "deleted_bytes": receipt["deleted_bytes"],
        "after_inventory": receipt["after_inventory"],
        "remaining_candidates": receipt["remaining_candidates"],
        "post_unlink_database_verification": receipt[
            "post_unlink_database_verification"
        ],
        "representative_datei_ids": representative_ids[:5],
    }
    if receipt["status"].startswith("bootstrap_"):
        summary.update(
            {
                "original_state": receipt.get("original_state"),
                "hidden_state": receipt.get("hidden_state"),
                "pre_capacity": receipt.get("pre_capacity"),
                "post_capacity": receipt.get("post_receipt_capacity"),
                "required_remaining_ledger_bytes": receipt.get(
                    "required_remaining_ledger_bytes"
                ),
            }
        )
    print("RESULT " + json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CleanupRunError as exc:
        details = dict(exc.details)
        details.setdefault("error", str(exc))
        print("ERROR " + json.dumps(details, ensure_ascii=False, sort_keys=True), flush=True)
        raise SystemExit(2)
    except (AuditError, CleanupError) as exc:
        print("ERROR " + json.dumps({"error": str(exc)}, ensure_ascii=False), flush=True)
        raise SystemExit(2)
