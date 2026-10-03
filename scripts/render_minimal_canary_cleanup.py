"""Release one exact Render upload after proving its PostgreSQL copy.

This one-shot tool exists only to bootstrap free space on the full persistent
disk.  It cannot select a different file and it never writes the database.
The default mode prepares a short-lived plan in ``/tmp``.  Execution requires
that exact plan hash plus a fixed approval phrase, then repeats every material
check under PostgreSQL advisory and table locks.  The public upload name is
atomically moved to a run-specific hidden name and reverified before unlink.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import pathlib
import re
import stat
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from render_upload_blob_audit import (
    AuditError,
    ReadOnlyPostgres,
    canonical_sha256,
    discover_references,
    scan_inventory,
    verify_blob,
)
from render_verified_upload_cleanup import (
    BOOTSTRAP_CANDIDATE_PATH,
    BOOTSTRAP_CANDIDATE_REFERENCE,
    BOOTSTRAP_CANDIDATE_SHA256,
    BOOTSTRAP_CANDIDATE_SIZE,
    BOOTSTRAP_EXPECTED_AFTER,
    BOOTSTRAP_LOCAL_EVIDENCE,
    BOOTSTRAP_SOURCE_INVENTORY,
    CleanupError,
    EXPECTED_UPLOAD_ROOT,
    PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,
    bootstrap_hidden_name,
    load_and_validate_audit,
    rename_noreplace,
    validate_bootstrap_candidate,
)


APPROVAL_PHRASE = "DELETE_ONE_VERIFIED_DATEI_DUPLICATE_1101"
PLAN_FORMAT = "gaertner-render-minimal-canary-plan-v1"
RECEIPT_FORMAT = "gaertner-render-minimal-canary-receipt-v1"
PLAN_ROOT = pathlib.Path(tempfile.gettempdir()) / "gaertner-minimal-canary-v1"
RECEIPT_ROOT = EXPECTED_UPLOAD_ROOT.parent / ".gaertner-minimal-canary-v1"
PROC_ROOT = pathlib.Path("/proc")
PLAN_MAX_AGE = timedelta(minutes=10)
AUDIT_MAX_AGE = timedelta(minutes=20)
FUTURE_SKEW = timedelta(seconds=30)
POST_MIN_FREE_BYTES = 8 * 1024 * 1024
RUN_ID_PATTERN = re.compile(r"[0-9a-f]{32}\Z")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
EXPECTED_CANDIDATES = 480
EXPECTED_CANDIDATE_BYTES = 879_034_032
EXPECTED_CANDIDATE_BLOBS = 480
EXPECTED_CANDIDATE_INDEX_SHA256 = (
    "6016f9a8403844a2e0e04cda4edc52b575bc3d92a8cb15fc95c08207c4c96a63"
)
EXPECTED_DATEI_ID = 1101
O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


class CanaryError(RuntimeError):
    pass


class CanaryRunError(CanaryError):
    def __init__(self, message: str, details: dict):
        super().__init__(message)
        self.details = details


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_time(label: str, value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise CanaryError(f"{label} ist kein gueltiger UTC-Zeitpunkt.") from exc
    if parsed.tzinfo is None:
        raise CanaryError(f"{label} hat keine Zeitzone.")
    return parsed.astimezone(timezone.utc)


def validate_fresh(label: str, value: object, maximum_age: timedelta) -> datetime:
    parsed = parse_time(label, value)
    now = datetime.now(timezone.utc)
    if parsed > now + FUTURE_SKEW or now - parsed > maximum_age:
        raise CanaryError(f"{label} ist abgelaufen oder liegt in der Zukunft.")
    return parsed


def validate_sha(label: str, value: object) -> str:
    cleaned = str(value or "").strip().lower()
    if not SHA256_PATTERN.fullmatch(cleaned):
        raise CanaryError(f"{label} ist keine SHA-256-Pruefsumme.")
    return cleaned


def script_sha256() -> str:
    return hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()


def read_boot_id() -> str:
    try:
        value = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="ascii"
        ).strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise CanaryError("Linux-Boot-ID ist nicht lesbar.") from exc
    if not re.fullmatch(r"[0-9a-f-]{36}", value):
        raise CanaryError("Linux-Boot-ID ist ungueltig.")
    return value


def render_git_commit() -> str:
    value = os.getenv("RENDER_GIT_COMMIT", "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40,64}", value):
        raise CanaryError("RENDER_GIT_COMMIT fehlt oder ist ungueltig.")
    return value


def validate_runtime() -> tuple[pathlib.Path, str]:
    root = pathlib.Path(os.getenv("UPLOAD_DIR", str(EXPECTED_UPLOAD_ROOT))).resolve()
    if not os.getenv("RENDER") or root != EXPECTED_UPLOAD_ROOT:
        raise CanaryError("Canary ist nur auf Render fuer /var/data/uploads erlaubt.")
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url.startswith(("postgres://", "postgresql://")):
        raise CanaryError("DATABASE_URL ist keine PostgreSQL-Verbindung.")
    if not O_NOFOLLOW or not pathlib.Path("/proc").is_dir():
        raise CanaryError("Linux O_NOFOLLOW und /proc werden zwingend benoetigt.")
    return root, database_url


def audit_validation_args(expected_audit_sha256: str) -> SimpleNamespace:
    return SimpleNamespace(
        expected_audit_sha256=validate_sha(
            "Erwarteter Audit-Hash", expected_audit_sha256
        ),
        expected_files=BOOTSTRAP_SOURCE_INVENTORY["file_count"],
        expected_bytes=BOOTSTRAP_SOURCE_INVENTORY["total_file_bytes"],
        expected_inventory_sha256=BOOTSTRAP_SOURCE_INVENTORY["inventory_sha256"],
        expected_candidates=EXPECTED_CANDIDATES,
        expected_candidate_bytes=EXPECTED_CANDIDATE_BYTES,
        expected_candidate_blobs=EXPECTED_CANDIDATE_BLOBS,
        expected_candidate_index_sha256=EXPECTED_CANDIDATE_INDEX_SHA256,
        expected_master_manifest_sha256=BOOTSTRAP_LOCAL_EVIDENCE[
            "master_manifest_sha256"
        ],
        expected_verification_report_sha256=BOOTSTRAP_LOCAL_EVIDENCE[
            "verification_report_sha256"
        ],
        expected_coverage_sha256=BOOTSTRAP_LOCAL_EVIDENCE[
            "expected_coverage_sha256"
        ],
    )


def load_fresh_audit(path: pathlib.Path, expected_hash: str) -> tuple[dict, dict]:
    try:
        report = load_and_validate_audit(path, audit_validation_args(expected_hash))
        candidate, remaining = validate_bootstrap_candidate(
            report, BOOTSTRAP_CANDIDATE_PATH
        )
    except (AuditError, CleanupError, ValueError) as exc:
        raise CanaryError(str(exc)) from exc
    validate_fresh("Audit", report.get("created_at"), AUDIT_MAX_AGE)
    if len(remaining) != EXPECTED_CANDIDATES - 1:
        raise CanaryError("Audit-Restmenge ist nicht exakt gebunden.")
    return report, candidate


def identity(st: os.stat_result) -> dict:
    return {
        "dev": int(st.st_dev),
        "ino": int(st.st_ino),
        "mode": int(st.st_mode),
        "nlink": int(st.st_nlink),
        "size": int(st.st_size),
        "mtime_ns": int(st.st_mtime_ns),
    }


def same_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return identity(left) == identity(right)


def hash_fd(fd: int) -> str:
    os.lseek(fd, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    while True:
        block = os.read(fd, 1024 * 1024)
        if not block:
            break
        digest.update(block)
    os.lseek(fd, 0, os.SEEK_SET)
    return digest.hexdigest()


def open_verified_candidate(root_fd: int, candidate: dict) -> tuple[int, dict]:
    flags = os.O_RDONLY | O_CLOEXEC | O_NOFOLLOW
    try:
        fd = os.open(BOOTSTRAP_CANDIDATE_PATH, flags, dir_fd=root_fd)
    except OSError as exc:
        raise CanaryError("Canary-Datei ist nicht sicher zu oeffnen.") from exc
    try:
        before = os.fstat(fd)
        path_before = os.stat(
            BOOTSTRAP_CANDIDATE_PATH, dir_fd=root_fd, follow_symlinks=False
        )
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not same_identity(before, path_before)
            or before.st_size != BOOTSTRAP_CANDIDATE_SIZE
            or before.st_mtime_ns != int(candidate["mtime_ns"])
        ):
            raise CanaryError("Canary-Datei hat unsichere Metadaten.")
        digest = hash_fd(fd)
        after = os.fstat(fd)
        path_after = os.stat(
            BOOTSTRAP_CANDIDATE_PATH, dir_fd=root_fd, follow_symlinks=False
        )
        if not same_identity(before, after) or not same_identity(after, path_after):
            raise CanaryError("Canary-Datei wurde waehrend der Pruefung ausgetauscht.")
        if not hmac.compare_digest(digest, BOOTSTRAP_CANDIDATE_SHA256):
            raise CanaryError("Canary-Datei-Hash stimmt nicht.")
        return fd, identity(after)
    except BaseException:
        os.close(fd)
        raise


def capacity(root: pathlib.Path) -> dict:
    values = os.statvfs(root)
    return {
        "free_bytes": int(values.f_bavail * values.f_frsize),
        "free_inodes": int(values.f_favail),
    }


def verify_database(database, candidate: dict) -> dict:
    references, covered = discover_references(database)
    exact_references = references.get(BOOTSTRAP_CANDIDATE_PATH) or []
    if exact_references != [BOOTSTRAP_CANDIDATE_REFERENCE]:
        raise CanaryError("Live-DB-Referenzen des Canary sind nicht exakt freigegeben.")
    result = verify_blob(database, candidate, EXPECTED_DATEI_ID)
    if (
        result.get("verified") is not True
        or int(result.get("decoded_size") or -1) != BOOTSTRAP_CANDIDATE_SIZE
        or not hmac.compare_digest(
            str(result.get("actual_sha256") or ""), BOOTSTRAP_CANDIDATE_SHA256
        )
    ):
        raise CanaryError("PostgreSQL-Original des Canary ist nicht bytegenau.")
    rows = list(database.backup_rows(EXPECTED_DATEI_ID))
    if len(rows) != 1 or rows[0].get("backup_id") is None:
        raise CanaryError("PostgreSQL-Backupzeile ist nicht eindeutig.")
    row = rows[0]
    return {
        "datei_id": EXPECTED_DATEI_ID,
        "backup_id": int(row["backup_id"]),
        "stored_name": pathlib.PurePath(str(row["stored_name"])).name,
        "size": int(row["backup_size"]),
        "sha256": result["actual_sha256"],
        "reference_columns_sha256": canonical_sha256(sorted(covered)),
    }


class LockedPostgres:
    """Small read-only-by-construction facade over a locked write-capable txn."""

    def __init__(self, database_url: str):
        try:
            import psycopg
            from psycopg import sql
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise CanaryError("psycopg ist nicht installiert.") from exc
        self.sql = sql
        self.connection = psycopg.connect(
            database_url, autocommit=False, row_factory=dict_row
        )
        self.lock_trace: list[str] = []
        self.database_writes = 0
        self._locked_reference_columns: list[dict] | None = None

    def _query_reference_columns(self) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT c.table_name, c.column_name,
                   EXISTS (
                     SELECT 1 FROM information_schema.columns i
                     WHERE i.table_schema=c.table_schema
                       AND i.table_name=c.table_name AND i.column_name='id'
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

    def acquire(self) -> None:
        row = self.connection.execute(
            "SELECT pg_try_advisory_xact_lock(%s) AS locked",
            (PORTAL_ORIGINALS_ADVISORY_LOCK_KEY,),
        ).fetchone()
        self.lock_trace.append("advisory")
        if not bool((row or {}).get("locked")):
            raise CanaryError("Portal-Dateisperre ist bereits belegt.")
        self.connection.execute("SET LOCAL lock_timeout = '3000ms'")
        columns_before = self._query_reference_columns()
        tables = sorted(
            {str(row["table_name"]) for row in columns_before}
            | {"dateien", "datei_backups"}
        )
        if any(not IDENTIFIER_PATTERN.fullmatch(table) for table in tables):
            raise CanaryError("Ungueltiger Tabellenname in der Referenzabdeckung.")
        identifiers = self.sql.SQL(", ").join(
            self.sql.Identifier(table) for table in tables
        )
        self.connection.execute(
            self.sql.SQL("LOCK TABLE {} IN SHARE MODE NOWAIT").format(identifiers)
        )
        columns_after = self._query_reference_columns()
        if columns_after != columns_before:
            raise CanaryError("Referenztabellen aenderten sich beim Sperren.")
        self._locked_reference_columns = columns_after
        self.lock_trace.append("tables")

    def reference_columns(self):
        if self._locked_reference_columns is None:
            raise CanaryError("Referenztabellen wurden noch nicht vollstaendig gesperrt.")
        return [dict(row) for row in self._locked_reference_columns]

    def reference_values(self, table: str, column: str, has_id: bool):
        identifier = self.sql.Identifier
        if has_id:
            query = self.sql.SQL(
                "SELECT id, {column}::text AS stored_value FROM {table} "
                "WHERE COALESCE({column}::text, '') <> ''"
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
            WHERE d.id=%s ORDER BY b.id
            """,
            (datei_id,),
        ).fetchall()

    def close(self) -> None:
        try:
            self.connection.rollback()
        finally:
            self.connection.close()


def foreign_open_fds(file_identity: dict, own_fd: int) -> list[str]:
    matches: list[str] = []
    own_pid = os.getpid()
    try:
        own_cgroup = (PROC_ROOT / "self" / "cgroup").read_bytes()
        processes = list(PROC_ROOT.iterdir())
    except OSError as exc:
        raise CanaryError("/proc kann nicht fuer offene FDs geprueft werden.") from exc
    for process in processes:
        if not process.name.isdigit():
            continue
        try:
            process_owner = int(process.stat().st_uid)
        except (FileNotFoundError, OSError):
            continue
        # Render exposes platform sidecars in /proc.  Some use the service UID
        # but live in a different cgroup and keep their FD metadata opaque.
        # Cgroups are not treated as an access boundary: scan every readable
        # same-UID FD and use the cgroup difference only to classify an opaque
        # platform process.  Same-cgroup opacity remains fail-closed.
        if process_owner != os.getuid():
            continue
        try:
            process_cgroup = (process / "cgroup").read_bytes()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise CanaryError(
                "Prozessgrenzen sind nicht vollstaendig lesbar."
            ) from exc
        same_cgroup = hmac.compare_digest(process_cgroup, own_cgroup)
        fd_root = process / "fd"
        try:
            descriptors = list(fd_root.iterdir())
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            if not same_cgroup:
                continue
            raise CanaryError("Offene Prozess-FDs sind nicht vollstaendig lesbar.") from exc
        for descriptor in descriptors:
            try:
                descriptor_number = int(descriptor.name)
                if int(process.name) == own_pid and descriptor_number == own_fd:
                    continue
                current = descriptor.stat()
            except PermissionError as exc:
                if not same_cgroup:
                    continue
                raise CanaryError(
                    "Offene Prozess-FDs sind nicht vollstaendig lesbar."
                ) from exc
            except (FileNotFoundError, OSError, ValueError):
                continue
            if (
                int(current.st_dev) == file_identity["dev"]
                and int(current.st_ino) == file_identity["ino"]
            ):
                matches.append(f"{process.name}/{descriptor.name}")
    return sorted(matches)


def root_identity(root_fd: int) -> list[int]:
    current = os.fstat(root_fd)
    if not stat.S_ISDIR(current.st_mode):
        raise CanaryError("Upload-Root-FD ist kein Verzeichnis.")
    return [int(current.st_dev), int(current.st_ino)]


def fsync_directory(path: pathlib.Path) -> None:
    if os.name != "posix":
        return
    directory_fd = os.open(path, os.O_RDONLY | O_DIRECTORY | O_CLOEXEC)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def write_all(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("Dateischreiben blieb ohne Fortschritt.")
        view = view[written:]


def atomic_json(path: pathlib.Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if (
        path.parent.parent.is_symlink()
        or not path.parent.parent.is_dir()
        or path.parent.is_symlink()
        or not path.parent.is_dir()
        or path.exists()
    ):
        raise CanaryError("Receipt-Ziel ist unsicher oder bereits belegt.")
    fsync_directory(path.parent.parent)
    payload = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
        "utf-8"
    )
    partial = path.with_name(f".receipt.{uuid.uuid4().hex}.part")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_CLOEXEC | O_NOFOLLOW
    fd = os.open(partial, flags, 0o600)
    try:
        write_all(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    published = False
    try:
        os.link(partial, path, follow_symlinks=False)
        published = True
        fsync_directory(path.parent)
    except BaseException:
        if published:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            try:
                fsync_directory(path.parent)
            except OSError:
                pass
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    else:
        # The complete receipt is durable now.  Cleanup must never turn that
        # success into a reported failure or invalidate the published receipt.
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            fsync_directory(path.parent)
        except OSError:
            pass


def ensure_receipt_root() -> None:
    RECEIPT_ROOT.mkdir(mode=0o700, exist_ok=True)
    if (
        RECEIPT_ROOT.parent.is_symlink()
        or not RECEIPT_ROOT.parent.is_dir()
        or RECEIPT_ROOT.is_symlink()
        or not RECEIPT_ROOT.is_dir()
    ):
        raise CanaryError("Persistentes Receipt-Verzeichnis ist unsicher.")
    # Persist the RECEIPT_ROOT directory entry in /var/data as well.  This is
    # harmless when it existed already and required when this run created it.
    fsync_directory(RECEIPT_ROOT.parent)


def write_plan(plan: dict) -> pathlib.Path:
    PLAN_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    run_dir = PLAN_ROOT / f"run-{plan['run_id']}"
    run_dir.mkdir(mode=0o700)
    path = run_dir / "plan.json"
    payload = (json.dumps(plan, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
        "utf-8"
    )
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_CLOEXEC, 0o600)
    try:
        write_all(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    fsync_directory(run_dir)
    return path


def read_and_claim_plan(run_id: str, expected_sha256: str) -> dict:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise CanaryError("Run-ID ist ungueltig.")
    expected_sha256 = validate_sha("Erwarteter Plan-Hash", expected_sha256)
    run_dir = PLAN_ROOT / f"run-{run_id}"
    path = run_dir / "plan.json"
    claimed = run_dir / "claimed.json"
    if run_dir.is_symlink() or path.is_symlink() or claimed.exists():
        raise CanaryError("Plan ist unsicher oder bereits verbraucht.")
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise CanaryError("Plan ist nicht lesbar.") from exc
    claimed_hash = validate_sha("Plan-Hash", plan.get("plan_sha256"))
    canonical = dict(plan)
    canonical.pop("plan_sha256", None)
    if not (
        hmac.compare_digest(canonical_sha256(canonical), claimed_hash)
        and hmac.compare_digest(claimed_hash, expected_sha256)
    ):
        raise CanaryError("Plan-Pruefsumme stimmt nicht.")
    if plan.get("run_id") != run_id:
        raise CanaryError("Plan-Run-ID stimmt nicht mit dem beanspruchten Lauf ueberein.")
    validate_fresh("Plan", plan.get("created_at"), PLAN_MAX_AGE)
    try:
        os.link(path, claimed, follow_symlinks=False)
        os.unlink(path)
    except OSError as exc:
        raise CanaryError("Plan konnte nicht atomar beansprucht werden.") from exc
    fsync_directory(run_dir)
    return plan


def plan_bindings(plan: dict, report: dict, candidate: dict, root_fd: int) -> None:
    expected = {
        "format": PLAN_FORMAT,
        "run_id": plan.get("run_id"),
        "status": "prepared_read_only",
        "audit_sha256": report["audit_sha256"],
        "script_sha256": script_sha256(),
        "render_git_commit": render_git_commit(),
        "boot_id": read_boot_id(),
        "upload_root": str(EXPECTED_UPLOAD_ROOT),
        "upload_root_identity": root_identity(root_fd),
        "source_inventory": BOOTSTRAP_SOURCE_INVENTORY,
        "expected_after": BOOTSTRAP_EXPECTED_AFTER,
        "candidate": candidate,
        "database_writes": 0,
    }
    for key, value in expected.items():
        if plan.get(key) != value:
            raise CanaryError(f"Planbindung stimmt nicht: {key}.")


def open_root_fd(root: pathlib.Path) -> int:
    return os.open(root, os.O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW)


def canary_hidden_name(run_id: str) -> str:
    try:
        return bootstrap_hidden_name(
            run_id, {"relative_path": BOOTSTRAP_CANDIDATE_PATH}
        )
    except CleanupError as exc:
        raise CanaryError(str(exc)) from exc


def stage_candidate(root_fd: int, run_id: str) -> str:
    hidden_name = canary_hidden_name(run_id)
    try:
        os.stat(hidden_name, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError:
        pass
    else:
        raise CanaryError("Run-spezifischer Canary-Stagingname ist bereits belegt.")
    try:
        rename_noreplace(
            BOOTSTRAP_CANDIDATE_PATH,
            hidden_name,
            source_dir_fd=root_fd,
            target_dir_fd=root_fd,
        )
    except OSError as exc:
        raise CanaryError("Canary konnte nicht atomar gestagt werden.") from exc
    return hidden_name


def staged_path_check(
    root_fd: int, candidate_fd: int, expected_identity: dict, hidden_name: str
) -> None:
    held = os.fstat(candidate_fd)
    try:
        staged = os.stat(hidden_name, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise CanaryError("Gestagter Canary-Pfad fehlt.") from exc
    if (
        identity(held) != expected_identity
        or identity(staged) != expected_identity
        or not stat.S_ISREG(staged.st_mode)
        or staged.st_nlink != 1
    ):
        raise CanaryError("Gestagter Canary stimmt nicht mit dem gehaltenen FD ueberein.")
    try:
        os.stat(
            BOOTSTRAP_CANDIDATE_PATH, dir_fd=root_fd, follow_symlinks=False
        )
    except FileNotFoundError:
        return
    raise CanaryError("Oeffentlicher Canary-Pfad wurde nach dem Staging neu belegt.")


def restore_staged_candidate(
    root_fd: int, candidate_fd: int, expected_identity: dict, hidden_name: str
) -> None:
    staged_path_check(root_fd, candidate_fd, expected_identity, hidden_name)
    try:
        rename_noreplace(
            hidden_name,
            BOOTSTRAP_CANDIDATE_PATH,
            source_dir_fd=root_fd,
            target_dir_fd=root_fd,
        )
    except OSError as exc:
        raise CanaryError("Gestagter Canary konnte nicht sicher restauriert werden.") from exc
    try:
        os.fsync(root_fd)
    except OSError:
        # Runtime restoration is already complete.  A failed durability flush
        # must not trigger a second rename or an unlink.
        pass
    final_path_check(root_fd, candidate_fd, expected_identity)
    try:
        os.stat(hidden_name, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise CanaryError("Canary-Stagingname blieb nach der Restaurierung bestehen.")


def strict_unlink_staged(
    root_fd: int, candidate_fd: int, expected_identity: dict, hidden_name: str
) -> None:
    staged_path_check(root_fd, candidate_fd, expected_identity, hidden_name)
    os.unlink(hidden_name, dir_fd=root_fd)


def namespace_state(root_fd: int, expected_identity: dict, hidden_name: str) -> dict:
    result = {}
    for label, name in (
        ("original", BOOTSTRAP_CANDIDATE_PATH),
        ("hidden", hidden_name),
    ):
        try:
            info = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        except FileNotFoundError:
            result[label] = {"exists": False, "matches_expected": False}
            continue
        current_identity = identity(info)
        result[label] = {
            "exists": True,
            "matches_expected": current_identity == expected_identity,
            "regular_file": stat.S_ISREG(info.st_mode),
            "identity": current_identity,
        }
    return result


def prepare(audit_path: pathlib.Path, expected_audit_sha256: str) -> dict:
    root, database_url = validate_runtime()
    report, candidate = load_fresh_audit(audit_path, expected_audit_sha256)
    inventory = scan_inventory(root)
    if any(
        inventory.get(key) != value
        for key, value in BOOTSTRAP_SOURCE_INVENTORY.items()
    ):
        raise CanaryError("PREPARE-Inventar weicht von der Baseline ab.")
    root_fd = open_root_fd(root)
    candidate_fd = None
    database = ReadOnlyPostgres(database_url)
    try:
        candidate_fd, file_binding = open_verified_candidate(root_fd, candidate)
        database_binding = verify_database(database, candidate)
        run_id = uuid.uuid4().hex
        plan = {
            "format": PLAN_FORMAT,
            "run_id": run_id,
            "created_at": utc_now(),
            "status": "prepared_read_only",
            "audit_sha256": report["audit_sha256"],
            "script_sha256": script_sha256(),
            "render_git_commit": render_git_commit(),
            "boot_id": read_boot_id(),
            "upload_root": str(root),
            "upload_root_identity": root_identity(root_fd),
            "source_inventory": BOOTSTRAP_SOURCE_INVENTORY,
            "expected_after": BOOTSTRAP_EXPECTED_AFTER,
            "candidate": candidate,
            "candidate_file_identity": file_binding,
            "database_binding": database_binding,
            "pre_capacity": capacity(root),
            "database_writes": 0,
        }
        plan["plan_sha256"] = canonical_sha256(plan)
        path = write_plan(plan)
        return {
            "status": "prepared_read_only",
            "run_id": run_id,
            "plan_path": str(path),
            "plan_sha256": plan["plan_sha256"],
            "audit_sha256": report["audit_sha256"],
            "database_writes": 0,
            "server_files_deleted": 0,
        }
    finally:
        if candidate_fd is not None:
            os.close(candidate_fd)
        database.close()
        os.close(root_fd)


def final_path_check(root_fd: int, candidate_fd: int, expected_identity: dict) -> None:
    held = os.fstat(candidate_fd)
    try:
        current = os.stat(
            BOOTSTRAP_CANDIDATE_PATH, dir_fd=root_fd, follow_symlinks=False
        )
    except FileNotFoundError as exc:
        raise CanaryError("Canary-Pfad fehlt unmittelbar vor unlinkat.") from exc
    if (
        identity(held) != expected_identity
        or identity(current) != expected_identity
        or not stat.S_ISREG(current.st_mode)
        or current.st_nlink != 1
    ):
        raise CanaryError("Canary-Pfad/Inode wechselte unmittelbar vor unlinkat.")


def receipt_payload(base: dict, status_value: str, error: BaseException | None = None) -> dict:
    payload = {
        **base,
        "format": RECEIPT_FORMAT,
        "status": status_value,
        "finished_at": utc_now(),
        "database_writes": 0,
    }
    if error is not None:
        payload.update(error_type=type(error).__name__, error=str(error))
    refresh_receipt_hash(payload)
    return payload


def refresh_receipt_hash(payload: dict) -> None:
    payload.pop("receipt_sha256", None)
    payload["receipt_sha256"] = canonical_sha256(payload)


def execute(
    audit_path: pathlib.Path,
    expected_audit_sha256: str,
    run_id: str,
    expected_plan_sha256: str,
) -> dict:
    root, database_url = validate_runtime()
    report, candidate = load_fresh_audit(audit_path, expected_audit_sha256)
    plan = read_and_claim_plan(run_id, expected_plan_sha256)
    root_fd = open_root_fd(root)
    candidate_fd = None
    database = None
    deleted = False
    current_identity = None
    hidden_name = canary_hidden_name(run_id)
    locked_pre_capacity = None
    base_receipt = {
        "run_id": run_id,
        "plan_sha256": plan["plan_sha256"],
        "audit_sha256": report["audit_sha256"],
        "candidate": {
            "relative_path": BOOTSTRAP_CANDIDATE_PATH,
            "size": BOOTSTRAP_CANDIDATE_SIZE,
            "sha256": BOOTSTRAP_CANDIDATE_SHA256,
            "datei_id": EXPECTED_DATEI_ID,
        },
    }
    try:
        plan_bindings(plan, report, candidate, root_fd)
        before_inventory = scan_inventory(root)
        if any(
            before_inventory.get(key) != value
            for key, value in BOOTSTRAP_SOURCE_INVENTORY.items()
        ):
            raise CanaryError("EXECUTE-Inventar weicht vor Locks von der Baseline ab.")
        candidate_fd, current_identity = open_verified_candidate(root_fd, candidate)
        if current_identity != plan.get("candidate_file_identity"):
            raise CanaryError("Canary-Inode weicht vom PREPARE-Plan ab.")
        database = LockedPostgres(database_url)
        database.acquire()
        if database.lock_trace != ["advisory", "tables"]:
            raise CanaryError("Datenbanksperren wurden nicht in sicherer Reihenfolge gesetzt.")
        locked_inventory = scan_inventory(root)
        if any(
            locked_inventory.get(key) != value
            for key, value in BOOTSTRAP_SOURCE_INVENTORY.items()
        ):
            raise CanaryError("EXECUTE-Inventar weicht unter Locks von der Baseline ab.")
        final_path_check(root_fd, candidate_fd, current_identity)
        locked_digest = hash_fd(candidate_fd)
        final_path_check(root_fd, candidate_fd, current_identity)
        if not hmac.compare_digest(locked_digest, BOOTSTRAP_CANDIDATE_SHA256):
            raise CanaryError("Canary-Datei-Hash stimmt unter Locks nicht.")
        pre_database = verify_database(database, candidate)
        if pre_database != plan.get("database_binding"):
            raise CanaryError("PostgreSQL-Bindung weicht vom PREPARE-Plan ab.")
        foreign_before_stage = foreign_open_fds(current_identity, candidate_fd)
        if foreign_before_stage:
            raise CanaryError(
                f"Canary ist vor dem Staging in fremden FDs offen: {foreign_before_stage}"
            )
        validate_fresh("Audit", report.get("created_at"), AUDIT_MAX_AGE)
        validate_fresh("Plan", plan.get("created_at"), PLAN_MAX_AGE)
        locked_pre_capacity = capacity(root)
        if int(locked_pre_capacity["free_bytes"]) >= POST_MIN_FREE_BYTES:
            raise CanaryError(
                "Canary ist nur fuer den belegten Datentraeger unter 8 MiB freigegeben."
            )
        final_path_check(root_fd, candidate_fd, current_identity)
        staged_result = stage_candidate(root_fd, run_id)
        if staged_result != hidden_name:
            raise CanaryError("Canary-Stagingname weicht von der Planbindung ab.")
        os.fsync(root_fd)
        staged_path_check(root_fd, candidate_fd, current_identity, hidden_name)
        staged_digest = hash_fd(candidate_fd)
        staged_path_check(root_fd, candidate_fd, current_identity, hidden_name)
        if not hmac.compare_digest(staged_digest, BOOTSTRAP_CANDIDATE_SHA256):
            raise CanaryError("Gestagter Canary-Hash stimmt nicht.")
        foreign_after_stage = foreign_open_fds(current_identity, candidate_fd)
        if foreign_after_stage:
            raise CanaryError(
                f"Canary ist nach dem Staging in fremden FDs offen: {foreign_after_stage}"
            )
        try:
            strict_unlink_staged(
                root_fd, candidate_fd, current_identity, hidden_name
            )
        except FileNotFoundError as exc:
            raise CanaryError(
                "unlinkat des gestagten Canary meldet ENOENT; der Lauf wird nicht gewertet."
            ) from exc
        deleted = True
        base_receipt.update(
            deleted_count=1,
            deleted_bytes=BOOTSTRAP_CANDIDATE_SIZE,
        )
        os.fsync(root_fd)
        fd_to_close = candidate_fd
        candidate_fd = None
        os.close(fd_to_close)
        post_capacity = capacity(root)
        if (
            int(post_capacity["free_bytes"]) < POST_MIN_FREE_BYTES
            or int(post_capacity["free_bytes"])
            - int(locked_pre_capacity["free_bytes"])
            < POST_MIN_FREE_BYTES
        ):
            raise CanaryError(
                "Canary hat nicht mindestens 8 MiB zusaetzlichen realen Speicher freigegeben."
            )
        after_inventory = scan_inventory(root)
        if any(
            after_inventory.get(key) != value
            for key, value in BOOTSTRAP_EXPECTED_AFTER.items()
        ):
            raise CanaryError("Nachher-Inventar ist nicht exakt der erwartete Zustand.")
        post_database = verify_database(database, candidate)
        if post_database != pre_database:
            raise CanaryError("PostgreSQL-Original/Referenz aenderte sich nach unlinkat.")
        if pathlib.Path(root, BOOTSTRAP_CANDIDATE_PATH).exists():
            raise CanaryError("Canary-Pfad existiert nach unlinkat weiterhin.")
        base_receipt.update(
            path_missing=True,
            prepared_capacity=plan["pre_capacity"],
            pre_capacity=locked_pre_capacity,
            post_capacity=post_capacity,
            after_inventory={
                key: after_inventory[key]
                for key in ("file_count", "total_file_bytes", "inventory_sha256")
            },
            post_database=post_database,
            lock_trace=database.lock_trace,
        )
        receipt = receipt_payload(base_receipt, "complete")
        receipt_path = RECEIPT_ROOT / f"run-{run_id}" / "receipt.json"
        try:
            ensure_receipt_root()
            atomic_json(receipt_path, receipt)
        except BaseException as exc:
            partial = receipt_payload(base_receipt, "partial_receipt_error", exc)
            raise CanaryRunError(
                "Datei ist sicher entfernt, aber persistenter Receipt schlug fehl.", partial
            ) from exc
        return {
            **receipt,
            "receipt_path": str(receipt_path),
        }
    except BaseException as exc:
        if isinstance(exc, CanaryRunError):
            raise
        namespace = None
        namespace_error = None
        if current_identity is not None:
            try:
                namespace = namespace_state(root_fd, current_identity, hidden_name)
            except BaseException as caught_namespace_error:
                namespace_error = caught_namespace_error
        if not deleted and namespace is not None:
            original = namespace["original"]
            hidden = namespace["hidden"]
            if (
                hidden["matches_expected"]
                and not original["exists"]
                and candidate_fd is not None
            ):
                try:
                    restore_staged_candidate(
                        root_fd, candidate_fd, current_identity, hidden_name
                    )
                    namespace = namespace_state(
                        root_fd, current_identity, hidden_name
                    )
                    original = namespace["original"]
                    hidden = namespace["hidden"]
                except BaseException as restore_error:
                    details = {
                        **base_receipt,
                        "namespace": namespace,
                        "original_error": str(exc),
                        "restore_error": str(restore_error),
                        "database_writes": 0,
                    }
                    partial = receipt_payload(
                        details, "partial_staged_not_deleted", exc
                    )
                    raise CanaryRunError(
                        "Canary wurde gestagt, aber weder geloescht noch sicher restauriert.",
                        partial,
                    ) from exc
            if not original["exists"] and not hidden["exists"]:
                deleted = True
            elif not (
                original["matches_expected"] and not hidden["exists"]
            ):
                details = {
                    **base_receipt,
                    "namespace": namespace,
                    "original_error": str(exc),
                    "database_writes": 0,
                }
                partial = receipt_payload(details, "partial_namespace_uncertain", exc)
                raise CanaryRunError(
                    "Canary-Namensraum ist nach dem Abbruch nicht eindeutig.",
                    partial,
                ) from exc
        elif not deleted and namespace_error is not None:
            details = {
                **base_receipt,
                "original_error": str(exc),
                "namespace_error": str(namespace_error),
                "database_writes": 0,
            }
            partial = receipt_payload(details, "partial_namespace_uncertain", exc)
            raise CanaryRunError(
                "Canary-Namensraum konnte nach dem Abbruch nicht rekonstruiert werden.",
                partial,
            ) from exc
        if deleted:
            if candidate_fd is not None:
                fd_to_close = candidate_fd
                candidate_fd = None
                try:
                    os.close(fd_to_close)
                except OSError:
                    pass
            base_receipt.update(
                deleted_count=1,
                deleted_bytes=BOOTSTRAP_CANDIDATE_SIZE,
            )
            details = {
                **base_receipt,
                "path_missing": not pathlib.Path(
                    root, BOOTSTRAP_CANDIDATE_PATH
                ).exists(),
                "database_writes": 0,
            }
            if namespace is not None:
                details["namespace"] = namespace
            try:
                details["post_capacity"] = capacity(root)
            except BaseException as capacity_error:
                details["post_capacity_error"] = str(capacity_error)
            try:
                if database is not None:
                    details["post_database"] = verify_database(database, candidate)
            except BaseException as verify_error:
                details["post_database_error"] = str(verify_error)
            partial = receipt_payload(details, "partial_after_unlink", exc)
            try:
                ensure_receipt_root()
                atomic_json(
                    RECEIPT_ROOT / f"run-{run_id}" / "receipt.json", partial
                )
            except BaseException as receipt_error:
                partial["receipt_error"] = str(receipt_error)
                refresh_receipt_hash(partial)
            raise CanaryRunError(
                "Canary wurde entfernt, Nachpruefung ist aber nicht vollstaendig.",
                partial,
            ) from exc
        if isinstance(exc, CanaryError):
            raise
        raise CanaryError(str(exc)) from exc
    finally:
        if candidate_fd is not None:
            fd_to_close = candidate_fd
            candidate_fd = None
            try:
                os.close(fd_to_close)
            except OSError:
                pass
        if database is not None:
            database.close()
        os.close(root_fd)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--audit-path", required=True)
    result.add_argument("--expected-audit-sha256", required=True)
    result.add_argument("--execute", action="store_true")
    result.add_argument("--approval")
    result.add_argument("--run-id")
    result.add_argument("--expected-plan-sha256")
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    if args.execute:
        if (
            args.approval != APPROVAL_PHRASE
            or not args.run_id
            or not args.expected_plan_sha256
        ):
            raise CanaryError("Explizite Canary-Freigabe und Planbindung fehlen.")
        result = execute(
            pathlib.Path(args.audit_path),
            args.expected_audit_sha256,
            args.run_id,
            args.expected_plan_sha256,
        )
    else:
        if args.approval or args.run_id or args.expected_plan_sha256:
            raise CanaryError("PREPARE akzeptiert keine Execute-Freigaben.")
        result = prepare(
            pathlib.Path(args.audit_path), args.expected_audit_sha256
        )
    print("RESULT " + json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CanaryRunError as exc:
        details = dict(exc.details)
        details.setdefault("error", str(exc))
        print("ERROR " + json.dumps(details, ensure_ascii=False, sort_keys=True), flush=True)
        raise SystemExit(2)
    except (AuditError, CanaryError) as exc:
        print("ERROR " + json.dumps({"error": str(exc)}, ensure_ascii=False), flush=True)
        raise SystemExit(2)
