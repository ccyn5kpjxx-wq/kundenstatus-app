"""Read-only Render audit for disk uploads and PostgreSQL file backups.

The script has no deletion mode and does not import the Flask application. It
opens PostgreSQL with ``default_transaction_read_only=on``, inventories the
physical upload directory, discovers every database column that can reference
an upload, and strictly decodes/re-hashes actual ``datei_backups.file_base64``
values. Only aggregate results are printed; the detailed hash-bound audit file
is written to the ephemeral system temp directory.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import hmac
import json
import os
import pathlib
import re
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime, timezone


AUDIT_FORMAT = "gaertner-render-upload-blob-audit-v1"
AUDIT_ROOT = pathlib.Path(tempfile.gettempdir()) / "gaertner-storage-audit-v1"
REFERENCE_COLUMNS = (
    "stored_name",
    "datei_stored_name",
    "pdf_stored_name",
    "unterschrift_stored",
)
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class AuditError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_sha256(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def atomic_json(path: pathlib.Path, value) -> None:
    AUDIT_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.resolve() != AUDIT_ROOT.resolve():
        raise AuditError("Auditdatei muss im temporaeren Auditverzeichnis liegen.")
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        with partial.open("x", encoding="utf-8") as target:
            json.dump(value, target, ensure_ascii=False, indent=2)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        try:
            partial.chmod(0o600)
        except OSError:
            pass
        partial.replace(path)
    except Exception:
        partial.unlink(missing_ok=True)
        raise


def stable_digest(path: pathlib.Path) -> tuple[str, int, int]:
    if path.is_symlink() or not path.is_file():
        raise AuditError(f"Kein regulaerer Upload: {path.name}")
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise AuditError(f"Upload wurde waehrend der Messung veraendert: {path.name}")
    return digest.hexdigest(), before.st_size, before.st_mtime_ns


def scan_inventory(root: pathlib.Path) -> dict:
    if root.is_symlink() or not root.is_dir():
        raise AuditError("Upload-Root ist kein regulaeres Verzeichnis.")
    paths = sorted(root.iterdir(), key=lambda item: item.name)
    entries = []
    for index, path in enumerate(paths, 1):
        if path.is_dir() or path.is_symlink() or not path.is_file():
            raise AuditError(f"Unbekannter Eintrag im Upload-Root: {path.name}")
        digest, size, mtime_ns = stable_digest(path)
        entries.append(
            {
                "relative_path": path.name,
                "size": size,
                "mtime_ns": mtime_ns,
                "sha256": digest,
            }
        )
        if index % 50 == 0 or index == len(paths):
            print(f"INVENTORY {index}/{len(paths)}", flush=True)
    rows = [[entry["relative_path"], entry["size"], entry["sha256"]] for entry in entries]
    return {
        "file_count": len(entries),
        "total_file_bytes": sum(entry["size"] for entry in entries),
        "inventory_sha256": hashlib.sha256(
            json.dumps(rows, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "files": entries,
    }


def normalize_stored_name(value) -> str:
    text = str(value or "").strip().replace("\\", "/")
    return text.rsplit("/", 1)[-1] if text else ""


class ReadOnlyPostgres:
    def __init__(self, database_url: str):
        try:
            import psycopg
            from psycopg import IsolationLevel
            from psycopg import sql
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise AuditError("psycopg ist nicht installiert.") from exc
        self.sql = sql
        self.connection = psycopg.connect(
            database_url,
            autocommit=False,
            options="-c default_transaction_read_only=on",
            row_factory=dict_row,
        )
        self.connection.isolation_level = IsolationLevel.REPEATABLE_READ
        self.connection.read_only = True
        state = self.connection.execute("SHOW transaction_read_only").fetchone()
        if str((state or {}).get("transaction_read_only") or "").lower() != "on":
            self.connection.close()
            raise AuditError("PostgreSQL-Verbindung ist nicht read-only.")
        isolation = self.connection.execute("SHOW transaction_isolation").fetchone()
        if str((isolation or {}).get("transaction_isolation") or "").lower() != "repeatable read":
            self.connection.close()
            raise AuditError("PostgreSQL-Verbindung nutzt keinen stabilen Snapshot.")

    def close(self) -> None:
        try:
            self.connection.rollback()
        finally:
            self.connection.close()

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
            (list(REFERENCE_COLUMNS),),
        ).fetchall()
        return [dict(row) for row in rows]

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


def discover_references(database) -> tuple[dict[str, list[dict]], list[str]]:
    references: dict[str, list[dict]] = defaultdict(list)
    covered = []
    found_dateien_stored_name = False
    for item in database.reference_columns():
        table = str(item["table_name"])
        column = str(item["column_name"])
        has_id = bool(item["has_id"])
        covered.append(f"{table}.{column}")
        if table == "dateien" and column == "stored_name" and has_id:
            found_dateien_stored_name = True
        for row in database.reference_values(table, column, has_id):
            stored_name = normalize_stored_name(row.get("stored_value"))
            if not stored_name:
                continue
            references[stored_name].append(
                {
                    "table": table,
                    "column": column,
                    "row_id": int(row["id"]) if row.get("id") is not None else None,
                }
            )
    if not found_dateien_stored_name:
        raise AuditError("dateien.stored_name mit id wurde nicht gefunden.")
    for stored_name in references:
        unique = {
            (item["table"], item["column"], item["row_id"]): item
            for item in references[stored_name]
        }
        references[stored_name] = sorted(
            unique.values(),
            key=lambda item: (item["table"], item["column"], item["row_id"] or 0),
        )
    return references, covered


def verify_blob(database, entry: dict, datei_id: int) -> dict:
    """Decode and hash one live blob while keeping counters unambiguous."""
    result = {
        "datei_id": datei_id,
        "decode_attempted": False,
        "decoded": False,
        "decoded_size": 0,
        "rehashed": False,
        "actual_sha256": "",
        "verified": False,
        "reasons": [],
    }
    rows = list(database.backup_rows(datei_id))
    if not rows:
        result["reasons"].append("datei_row_missing")
        return result
    if len(rows) != 1:
        result["reasons"].append("backup_row_count_mismatch")
        return result
    row = rows[0]
    if row.get("backup_id") is None:
        result["reasons"].append("backup_row_missing")
    if normalize_stored_name(row.get("stored_name")) != entry["relative_path"]:
        result["reasons"].append("stored_name_mismatch")
    try:
        datei_size = int(row.get("datei_size") or 0)
        backup_size = int(row.get("backup_size") or 0)
    except (TypeError, ValueError):
        result["reasons"].append("invalid_size_metadata")
        datei_size = -1
        backup_size = -1
    if datei_size != entry["size"] or backup_size != entry["size"]:
        result["reasons"].append("size_metadata_mismatch")
    stored_hash = str(row.get("file_sha256") or "").strip().lower()
    if not SHA256_PATTERN.fullmatch(stored_hash):
        result["reasons"].append("invalid_backup_hash")
    encoded = row.get("file_base64")
    if not isinstance(encoded, str) or not encoded:
        result["reasons"].append("backup_blob_missing")
        return result
    result["decode_attempted"] = True
    try:
        data = base64.b64decode(encoded.strip(), validate=True)
    except (binascii.Error, TypeError, ValueError):
        result["reasons"].append("backup_base64_invalid")
        return result
    decoded_size = len(data)
    actual_hash = hashlib.sha256(data).hexdigest()
    del data
    result.update(
        {
            "decoded": True,
            "decoded_size": decoded_size,
            "rehashed": True,
            "actual_sha256": actual_hash,
        }
    )
    if decoded_size != entry["size"]:
        result["reasons"].append("backup_blob_size_mismatch")
    if SHA256_PATTERN.fullmatch(stored_hash) and not hmac.compare_digest(
        actual_hash, stored_hash
    ):
        result["reasons"].append("backup_metadata_hash_mismatch")
    if not hmac.compare_digest(actual_hash, entry["sha256"]):
        result["reasons"].append("backup_disk_hash_mismatch")
    result["verified"] = not result["reasons"]
    return result


def validate_hex_hash(label: str, value: str) -> str:
    cleaned = str(value or "").strip().lower()
    if not SHA256_PATTERN.fullmatch(cleaned):
        raise AuditError(f"{label} ist keine SHA-256-Pruefsumme.")
    return cleaned


def run_audit(database, root: pathlib.Path, args) -> tuple[dict, pathlib.Path]:
    inventory = scan_inventory(root)
    expected_inventory = validate_hex_hash("Inventar-Hash", args.expected_inventory_sha256)
    actual_tuple = (
        inventory["file_count"],
        inventory["total_file_bytes"],
        inventory["inventory_sha256"],
    )
    expected_tuple = (args.expected_files, args.expected_bytes, expected_inventory)
    if actual_tuple != expected_tuple:
        raise AuditError(f"Live-Inventar weicht vom lokalen Archiv ab: {actual_tuple} != {expected_tuple}")

    references, covered = discover_references(database)
    covered = sorted(covered)
    coverage_sha256 = canonical_sha256(covered)
    expected_coverage_sha256 = validate_hex_hash(
        "Coverage-Hash", args.expected_coverage_sha256
    )
    physical_names = {entry["relative_path"] for entry in inventory["files"]}
    failures: dict[str, int] = defaultdict(int)
    blob_failures: dict[str, int] = defaultdict(int)
    candidates = []
    blob_stats = {
        "rows_examined": 0,
        "decode_attempts": 0,
        "decoded": 0,
        "decoded_bytes": 0,
        "rehashed": 0,
        "rehashed_bytes": 0,
        "fully_verified": 0,
        "fully_verified_bytes": 0,
    }
    for index, entry in enumerate(inventory["files"], 1):
        refs = references.get(entry["relative_path"], [])
        datei_refs = [
            ref
            for ref in refs
            if ref["table"] == "dateien"
            and ref["column"] == "stored_name"
            and ref["row_id"] is not None
        ]
        verified_refs = []
        blob_results = []
        for ref in datei_refs:
            result = verify_blob(database, entry, ref["row_id"])
            blob_results.append(result)
            blob_stats["rows_examined"] += 1
            if result["decode_attempted"]:
                blob_stats["decode_attempts"] += 1
            if result["decoded"]:
                blob_stats["decoded"] += 1
                blob_stats["decoded_bytes"] += result["decoded_size"]
            if result["rehashed"]:
                blob_stats["rehashed"] += 1
                blob_stats["rehashed_bytes"] += result["decoded_size"]
            if result["verified"]:
                blob_stats["fully_verified"] += 1
                blob_stats["fully_verified_bytes"] += result["decoded_size"]
                verified_refs.append(ref)
            else:
                for blob_reason in result["reasons"]:
                    blob_failures[blob_reason] += 1

        reason = ""
        if entry["size"] <= 0:
            reason = "zero_size"
        elif not refs:
            reason = "unreferenced"
        elif len(datei_refs) != len(refs):
            reason = "non_dateien_reference"
        elif not datei_refs:
            reason = "dateien_reference_missing"
        elif len(verified_refs) != len(datei_refs):
            reason = "blob_verification_failed"
        if reason:
            failures[reason] += 1
        else:
            candidates.append(
                {
                    "relative_path": entry["relative_path"],
                    "size": entry["size"],
                    "mtime_ns": entry["mtime_ns"],
                    "sha256": entry["sha256"],
                    "references": verified_refs,
                }
            )
        if index % 25 == 0 or index == inventory["file_count"]:
            print(
                f"BLOBS {index}/{inventory['file_count']} "
                f"decoded={blob_stats['decoded']} "
                f"rehashed={blob_stats['rehashed']} "
                f"verified_files={len(candidates)}",
                flush=True,
            )

    candidate_index = [
        [
            candidate["relative_path"],
            candidate["size"],
            candidate["sha256"],
            [
                [ref["table"], ref["column"], ref["row_id"]]
                for ref in candidate["references"]
            ],
        ]
        for candidate in candidates
    ]
    candidate_count = len(candidates)
    candidate_bytes = sum(candidate["size"] for candidate in candidates)
    candidate_blob_count = sum(len(candidate["references"]) for candidate in candidates)
    candidate_blob_bytes = sum(
        candidate["size"] * len(candidate["references"])
        for candidate in candidates
    )
    candidate_index_sha256 = canonical_sha256(candidate_index)
    expected_candidate_index_sha256 = validate_hex_hash(
        "Kandidatenindex-Hash", args.expected_candidate_index_sha256
    )
    candidate_baseline_matches = (
        candidate_count == args.expected_candidates
        and candidate_bytes == args.expected_candidate_bytes
        and candidate_blob_count == args.expected_candidate_blobs
        and hmac.compare_digest(
            candidate_index_sha256, expected_candidate_index_sha256
        )
    )

    final_inventory = scan_inventory(root)
    if final_inventory != inventory:
        raise AuditError("Das Live-Inventar hat sich waehrend des Audits veraendert.")

    missing_by_table: dict[str, int] = defaultdict(int)
    missing_physical_names = 0
    for stored_name, refs in references.items():
        if stored_name in physical_names:
            continue
        missing_physical_names += 1
        for ref in refs:
            missing_by_table[ref["table"]] += 1

    report = {
        "format": AUDIT_FORMAT,
        "audit_id": uuid.uuid4().hex,
        "created_at": utc_now(),
        "mode": "strictly_read_only",
        "upload_root": str(root),
        "source_inventory": inventory,
        "local_evidence": {
            "master_manifest_sha256": validate_hex_hash(
                "Mastermanifest-Hash", args.master_manifest_sha256
            ),
            "verification_report_sha256": validate_hex_hash(
                "Verifikationsbericht-Hash", args.verification_report_sha256
            ),
            "expected_coverage_sha256": expected_coverage_sha256,
            "expected_candidate_count": args.expected_candidates,
            "expected_candidate_bytes": args.expected_candidate_bytes,
            "expected_candidate_blob_count": args.expected_candidate_blobs,
            "expected_candidate_index_sha256": expected_candidate_index_sha256,
        },
        "reference_columns_covered": covered,
        "reference_columns_coverage_sha256": coverage_sha256,
        "reference_columns_coverage_matches": hmac.compare_digest(
            coverage_sha256, expected_coverage_sha256
        ),
        "candidate_count": candidate_count,
        "candidate_bytes": candidate_bytes,
        "candidate_blob_count": candidate_blob_count,
        "candidate_blob_bytes": candidate_blob_bytes,
        "candidate_index_sha256": candidate_index_sha256,
        "candidate_baseline_matches": candidate_baseline_matches,
        "candidates": candidates,
        "physical_datei_blob_stats": blob_stats,
        "blob_failure_reason_counts": dict(sorted(blob_failures.items())),
        "protected_file_count": inventory["file_count"] - candidate_count,
        "protected_file_bytes": inventory["total_file_bytes"]
        - candidate_bytes,
        "protected_reason_counts": dict(sorted(failures.items())),
        "unreferenced_current_file_count": failures.get("unreferenced", 0),
        "missing_physical_name_count": missing_physical_names,
        "missing_physical_reference_tables": dict(sorted(missing_by_table.items())),
        "inventory_rechecked_unchanged": True,
        "deletion_evidence_complete": bool(
            candidate_baseline_matches
            and hmac.compare_digest(coverage_sha256, expected_coverage_sha256)
        ),
        "database_writes": 0,
        "server_files_deleted": 0,
        "delete_approved": False,
    }
    report["audit_sha256"] = canonical_sha256(report)
    AUDIT_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = AUDIT_ROOT / f"audit-{report['audit_id']}.json"
    atomic_json(path, report)
    return report, path


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--expected-root", required=True)
    result.add_argument("--expected-files", required=True, type=int)
    result.add_argument("--expected-bytes", required=True, type=int)
    result.add_argument("--expected-inventory-sha256", required=True)
    result.add_argument("--master-manifest-sha256", required=True)
    result.add_argument("--verification-report-sha256", required=True)
    result.add_argument("--expected-coverage-sha256", required=True)
    result.add_argument("--expected-candidates", required=True, type=int)
    result.add_argument("--expected-candidate-bytes", required=True, type=int)
    result.add_argument("--expected-candidate-blobs", required=True, type=int)
    result.add_argument("--expected-candidate-index-sha256", required=True)
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    expected_root = pathlib.Path(args.expected_root).resolve()
    configured_root = pathlib.Path(
        os.getenv("UPLOAD_DIR", "/var/data/uploads")
    ).resolve()
    if configured_root != expected_root:
        raise AuditError(f"UPLOAD_DIR stimmt nicht: {configured_root} != {expected_root}")
    if os.getenv("RENDER") and expected_root != pathlib.Path("/var/data/uploads"):
        raise AuditError("Auf Render ist ausschliesslich /var/data/uploads erlaubt.")
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url.startswith(("postgres://", "postgresql://")):
        raise AuditError("DATABASE_URL ist keine PostgreSQL-Verbindung.")
    database = ReadOnlyPostgres(database_url)
    try:
        report, path = run_audit(database, expected_root, args)
    finally:
        database.close()
    summary = {
        "status": "audit_complete",
        "audit_path": str(path),
        "audit_sha256": report["audit_sha256"],
        "source_file_count": report["source_inventory"]["file_count"],
        "source_file_bytes": report["source_inventory"]["total_file_bytes"],
        "source_inventory_sha256": report["source_inventory"]["inventory_sha256"],
        "physical_datei_blob_stats": report["physical_datei_blob_stats"],
        "verified_candidate_count": report["candidate_count"],
        "verified_candidate_bytes": report["candidate_bytes"],
        "verified_candidate_blob_count": report["candidate_blob_count"],
        "verified_candidate_blob_bytes": report["candidate_blob_bytes"],
        "candidate_index_sha256": report["candidate_index_sha256"],
        "candidate_baseline_matches": report["candidate_baseline_matches"],
        "reference_columns_coverage_sha256": report[
            "reference_columns_coverage_sha256"
        ],
        "reference_columns_coverage_matches": report[
            "reference_columns_coverage_matches"
        ],
        "inventory_rechecked_unchanged": report[
            "inventory_rechecked_unchanged"
        ],
        "deletion_evidence_complete": report["deletion_evidence_complete"],
        "protected_file_count": report["protected_file_count"],
        "protected_file_bytes": report["protected_file_bytes"],
        "protected_reason_counts": report["protected_reason_counts"],
        "database_writes": 0,
        "server_files_deleted": 0,
        "delete_approved": False,
    }
    print("RESULT " + json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AuditError as exc:
        print("ERROR " + json.dumps({"error": str(exc)}, ensure_ascii=False), flush=True)
        raise SystemExit(2)
