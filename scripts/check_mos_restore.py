"""Read-only integrity comparison for a PostgreSQL restore and its upload disk.

The manifest contains counts and hashes, not customer rows or file names. Run
once on a quiet source snapshot and again on its isolated restore. This never
creates, drops, or updates a database, and never restores a Render disk.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import stat
import sys


FORMAT_VERSION = 1
REQUIRED_MOS_TABLES = {
    "miet_checkout_holds", "miet_checkout_events", "miet_checkout_deposit_auths",
    "miet_checkout_creation_attempts", "miet_checkout_contracts",
    "miet_checkout_contract_delivery", "miet_checkout_order_receipts",
    "miet_checkout_handovers", "miet_checkout_vehicle_blocks",
    "miet_checkout_vehicle_readiness", "miet_checkout_returns",
    "miet_checkout_return_clearances", "miet_checkout_cancellations",
    "miet_checkout_refunds", "miet_checkout_review_refunds",
    "miet_checkout_limits", "miet_checkout_retention_blocks",
    "miet_checkout_retention_seen", "miet_checkout_terminations",
    "miet_checkout_termination_limits", "miet_checkout_slots",
}
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class RestoreAuditError(ValueError):
    pass


def _regular_file(path: Path) -> bool:
    info = path.lstat()
    return stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode) and not (
        getattr(info, "st_file_attributes", 0) & 0x400
    )


def _hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def upload_inventory(directory: Path) -> tuple[dict, set[str]]:
    if not directory.is_dir() or directory.is_symlink():
        raise RestoreAuditError("Upload-Verzeichnis fehlt oder ist verlinkt.")
    digest = hashlib.sha256()
    count = 0
    total_bytes = 0
    names = set()
    for path in sorted(directory.iterdir(), key=lambda item: item.name):
        if not _regular_file(path):
            raise RestoreAuditError("Upload-Verzeichnis enthält Nicht-Dateien oder Verknüpfungen.")
        if path.name in names:
            raise RestoreAuditError("Doppelter Upload-Dateiname.")
        names.add(path.name)
        size, checksum = _hash_file(path)
        record = json.dumps([path.name, size, checksum], ensure_ascii=False, separators=(",", ":"))
        digest.update(record.encode("utf-8") + b"\n")
        count += 1
        total_bytes += size
    return {"count": count, "bytes": total_bytes, "sha256": digest.hexdigest()}, names


def _validate_digest(value, description: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value.lower()) is None:
        raise RestoreAuditError(f"Ungültige Prüfsumme: {description}.")
    return value.lower()


def _decode_checked(encoded, expected_hash, description: str, *, prefix: bytes | None = None) -> bytes:
    if not isinstance(encoded, str) or not encoded:
        raise RestoreAuditError(f"Fehlende Binärdaten: {description}.")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise RestoreAuditError(f"Ungültige Base64-Daten: {description}.") from exc
    if not raw or (prefix and not raw.startswith(prefix)):
        raise RestoreAuditError(f"Ungültiges Dateiformat: {description}.")
    expected = _validate_digest(expected_hash, description)
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected):
        raise RestoreAuditError(f"Prüfsumme stimmt nicht: {description}.")
    return raw


def verify_signed_documents(conn, tables: set[str]) -> dict:
    if "miet_checkout_contracts" not in tables:
        raise RestoreAuditError("MOS-Vertragstabelle fehlt.")
    mos_count = 0
    for row in conn.execute("""
        SELECT c.contract_json,c.contract_sha256,c.signature_png_base64,
               c.pdf_base64,c.pdf_sha256,h.id
        FROM miet_checkout_contracts c
        LEFT JOIN miet_checkout_holds h ON h.id=c.hold_id
    """):
        contract_json, contract_hash, signature, pdf, pdf_hash, hold_id = row
        if hold_id is None:
            raise RestoreAuditError("MOS-Vertrag ohne Buchung gefunden.")
        expected = _validate_digest(contract_hash, "MOS-Vertrag")
        if not isinstance(contract_json, str) or not hmac.compare_digest(
            hashlib.sha256(contract_json.encode("utf-8")).hexdigest(), expected
        ):
            raise RestoreAuditError("MOS-Vertragssnapshot beschädigt.")
        if not signature:
            raise RestoreAuditError("MOS-Vertrag ohne gespeicherte PNG-Unterschrift.")
        try:
            signature_bytes = base64.b64decode(signature, validate=True)
        except (ValueError, TypeError, base64.binascii.Error) as exc:
            raise RestoreAuditError("MOS-Unterschrift beschädigt.") from exc
        if not signature_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            raise RestoreAuditError("MOS-Unterschrift ist kein PNG.")
        _decode_checked(pdf, pdf_hash, "MOS-Vertrags-PDF", prefix=b"%PDF")
        mos_count += 1

    legacy_count = 0
    if "mietvertrag_versionen" in tables:
        for pdf, pdf_hash, signature, signature_hash in conn.execute("""
            SELECT pdf_base64,pdf_sha256,unterschrift_base64,unterschrift_sha256
            FROM mietvertrag_versionen
        """):
            if pdf:
                _decode_checked(pdf, pdf_hash, "Mietvertrags-PDF", prefix=b"%PDF")
                legacy_count += 1
            elif pdf_hash:
                raise RestoreAuditError("Mietvertrags-PDF fehlt trotz Prüfsumme.")
            if signature:
                _decode_checked(signature, signature_hash, "Mietvertrags-Unterschrift")
            elif signature_hash:
                raise RestoreAuditError("Mietvertrags-Unterschrift fehlt trotz Prüfsumme.")
    return {"mos_contract_pdfs": mos_count, "legacy_contract_pdfs": legacy_count}


def verify_upload_references(conn, files: set[str]) -> int:
    from psycopg import sql

    rows = conn.execute("""
        SELECT table_name,column_name FROM information_schema.columns
        WHERE table_schema='public' AND column_name IN
          ('stored_name','datei_stored_name','pdf_stored_name')
        ORDER BY table_name,column_name
    """).fetchall()
    checked = 0
    missing = 0
    unsafe = 0
    for table, column in rows:
        query = sql.SQL("SELECT {} FROM public.{} WHERE {} IS NOT NULL AND {} <> ''").format(
            sql.Identifier(column), sql.Identifier(table),
            sql.Identifier(column), sql.Identifier(column),
        )
        for (filename,) in conn.execute(query):
            checked += 1
            if not isinstance(filename, str) or filename in {".", ".."} or (
                Path(filename).name != filename or "/" in filename or "\\" in filename
            ):
                unsafe += 1
            elif filename not in files:
                missing += 1
    if unsafe or missing:
        raise RestoreAuditError(
            f"Upload-Referenzen unvollständig: {missing} fehlen, {unsafe} unsicher ({checked} geprüft)."
        )
    return checked


def database_inventory(conn) -> tuple[dict, set[str]]:
    from psycopg import sql

    tables = [row[0] for row in conn.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name
    """)]
    missing = REQUIRED_MOS_TABLES - set(tables)
    if missing:
        raise RestoreAuditError("MOS-Tabellen fehlen: " + ", ".join(sorted(missing)))
    result = {}
    for index, table in enumerate(tables):
        columns = conn.execute("""
            SELECT column_name,data_type,is_nullable FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position
        """, (table,)).fetchall()
        schema_hash = hashlib.sha256(json.dumps(columns, separators=(",", ":")).encode()).hexdigest()
        row_hashes = []
        query = sql.SQL("SELECT row_to_json(t)::text FROM public.{} AS t").format(sql.Identifier(table))
        with conn.cursor(name=f"mos_restore_audit_{index}") as cursor:
            cursor.execute(query)
            for (record,) in cursor:
                row_hashes.append(hashlib.sha256(record.encode("utf-8")).digest())
                if len(row_hashes) > 1_000_000:
                    raise RestoreAuditError("Tabelle zu groß für diesen Restore-Audit.")
        digest = hashlib.sha256()
        for row_hash in sorted(row_hashes):
            digest.update(row_hash)
        result[table] = {"rows": len(row_hashes), "schema_sha256": schema_hash,
                         "content_sha256": digest.hexdigest()}
    return result, set(tables)


def sequence_inventory(conn) -> dict:
    rows = conn.execute("""
        SELECT sequencename,start_value,increment_by,last_value
        FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename
    """).fetchall()
    return {name: {"start": start, "increment": increment, "last": last}
            for name, start, increment, last in rows}


def audit(conn, upload_dir: Path) -> dict:
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        db_tables, table_names = database_inventory(conn)
        sequences = sequence_inventory(conn)
        signed = verify_signed_documents(conn, table_names)
        uploads, filenames = upload_inventory(upload_dir)
        reference_count = verify_upload_references(conn, filenames)
    return {"format_version": FORMAT_VERSION, "tables": db_tables, "sequences": sequences,
            "signed_documents": signed, "uploads": uploads,
            "upload_references_checked": reference_count}


def compare_manifest(actual: dict, baseline: dict) -> None:
    if not isinstance(baseline, dict) or baseline.get("format_version") != FORMAT_VERSION:
        raise RestoreAuditError("Unbekanntes Restore-Manifest.")
    if actual != baseline:
        differing = [key for key in ("tables", "sequences", "signed_documents", "uploads",
                                      "upload_references_checked") if actual.get(key) != baseline.get(key)]
        raise RestoreAuditError("Restore weicht ab: " + ", ".join(differing))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-database", required=True,
                        help="Exakter Name der ausdrücklich gewählten Quell- oder Restore-Datenbank")
    parser.add_argument("--baseline", type=Path, help="Gesichertes Manifest zum Vergleich")
    parser.add_argument("--output", type=Path, help="Neues Manifest anlegen, vorhandene Datei nie ersetzen")
    parser.add_argument("--min-mos-contracts", type=int, default=1)
    parser.add_argument("--min-uploads", type=int, default=1)
    args = parser.parse_args(argv)
    if not args.baseline and not args.output:
        parser.error("--output für die Quelle oder --baseline für den Restore ist erforderlich.")
    if args.min_mos_contracts < 0 or args.min_uploads < 0:
        parser.error("Mindestzahlen dürfen nicht negativ sein.")
    url = os.environ.get("MOS_RESTORE_AUDIT_DATABASE_URL")
    upload_dir = os.environ.get("MOS_RESTORE_AUDIT_UPLOAD_DIR")
    if not url or not upload_dir:
        parser.error("MOS_RESTORE_AUDIT_DATABASE_URL und MOS_RESTORE_AUDIT_UPLOAD_DIR fehlen.")
    try:
        import psycopg

        with psycopg.connect(url, connect_timeout=5, options="-c default_transaction_read_only=on") as conn:
            actual_name = conn.execute("SELECT current_database()").fetchone()[0]
            conn.rollback()
            if actual_name != args.expected_database:
                raise RestoreAuditError("Datenbankname stimmt nicht mit --expected-database überein.")
            manifest = audit(conn, Path(upload_dir))
        if manifest["signed_documents"]["mos_contract_pdfs"] < args.min_mos_contracts:
            raise RestoreAuditError("Zu wenige signierte MOS-Verträge im geprüften Stand.")
        if manifest["uploads"]["count"] < args.min_uploads:
            raise RestoreAuditError("Zu wenige Uploads im geprüften Stand.")
        if args.baseline:
            compare_manifest(manifest, json.loads(args.baseline.read_text(encoding="utf-8")))
        if args.output:
            with args.output.open("x", encoding="utf-8") as target:
                json.dump(manifest, target, ensure_ascii=False, sort_keys=True, indent=2)
                target.write("\n")
        print(json.dumps({"result": "RESTORE_MATCH" if args.baseline else "SOURCE_AUDIT_PASS",
                          "tables": len(manifest["tables"]), "sequences": len(manifest["sequences"]),
                          "mos_contract_pdfs": manifest["signed_documents"]["mos_contract_pdfs"],
                          "uploads": manifest["uploads"]["count"],
                          "upload_references": manifest["upload_references_checked"],
                          "compared": bool(args.baseline)}, sort_keys=True))
        return 0
    except RestoreAuditError as exc:
        print(f"RESTORE_AUDIT_FAILED: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        # A libpq exception can contain the connection string, host or user.
        # Never echo it or a traceback to an operator log.
        print(f"RESTORE_AUDIT_FAILED: {type(exc).__name__}; Details bewusst nicht ausgegeben.",
              file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
