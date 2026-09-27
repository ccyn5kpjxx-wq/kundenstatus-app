"""Synthetic pg_dump/pg_restore plus signed-PDF and upload recovery acceptance.

Requires the dedicated local 127.0.0.1:55439 test cluster. Creates two new
databases, never drops or reuses one, and contacts no cloud service or Stripe.
"""

import argparse
from base64 import b64encode
from hashlib import sha256
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import quote
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from check_mos_restore import RestoreAuditError, audit, compare_manifest, main as audit_cli_main
from run_mos_stripe_postgres_staging import (
    clean_environment, isolated_process_environment, load_cluster_config,
)

PG_BIN = ROOT / ".agent-hub" / "postgres-runtime" / "pgsql" / "bin"


def database_url(cfg, name):
    if not name.startswith("mos_restore_audit_") or len(name) != len("mos_restore_audit_") + 32:
        raise RuntimeError("Ungültiger synthetischer Datenbankname.")
    return (f"postgresql://{cfg['user']}:{quote(cfg['password'], safe='')}@"
            f"127.0.0.1:55439/{name}")


def fresh_database(cfg):
    import psycopg
    from psycopg import sql

    name = "mos_restore_audit_" + secrets.token_hex(16)
    with psycopg.connect(**cfg, autocommit=True, connect_timeout=5) as admin:
        user, database, address, port = admin.execute(
            "SELECT current_user,current_database(),inet_server_addr()::text,inet_server_port()"
        ).fetchone()
        if (user != "mos_test_admin" or database != "postgres" or port != 55439
                or str(ipaddress.ip_interface(address).ip) != "127.0.0.1"):
            raise RuntimeError("Nur der dedizierte lokale MOS-Testcluster ist zulässig.")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    return name


def initialize_real_schema(cfg, source, directory):
    if "app" in sys.modules:
        raise RuntimeError("Schema-Test braucht einen frischen Python-Prozess.")
    with isolated_process_environment(clean_environment(directory, database_url(cfg, source))):
        import app as portal
        from mos_booking.production import init_refund_schema
        from mos_public_contract import init_schema as init_contract_schema
        from mos_handover import init_schema as init_handover_schema
        from mos_order_receipt import init_schema as init_receipt_schema
        from mos_public_booking import init_slot_schema
        from mos_return import init_schema as init_return_schema
        from mos_signature_retention import init_schema as init_retention_schema
        from mos_termination import init_schema as init_termination_schema

        if not portal.USE_POSTGRES or portal.DATABASE_URL != database_url(cfg, source):
            raise RuntimeError("Portal nutzte nicht die neue synthetische PostgreSQL-Datenbank.")
        db = portal.get_db()
        try:
            init_refund_schema(db)
            init_contract_schema(db)
            init_handover_schema(db)
            init_receipt_schema(db)
            init_slot_schema(db, [])
            init_return_schema(db)
            init_retention_schema(db)
            init_termination_schema(db)
            db.commit()
        finally:
            db.close()


def seed_synthetic_records(cfg, source, uploads):
    import psycopg

    upload_name = "mos-restore-synthetic-document.bin"
    payload = b"Synthetic MOS restore upload; no customer data."
    (uploads / upload_name).write_bytes(payload)
    contract_json = json.dumps({"test_only": True}, sort_keys=True, separators=(",", ":"))
    pdf = b"%PDF-1.4\nSynthetic restore audit only\n%%EOF\n"
    png = b"\x89PNG\r\n\x1a\nsynthetic restore signature"
    with psycopg.connect(**{**cfg, "dbname": source}, connect_timeout=5) as conn:
        conn.execute("""
            INSERT INTO miet_checkout_holds
            (id,request_key,mietfahrzeug_id,start_datum,end_datum,payload,fingerprint,status,expires_at,grund)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, ("restore-synthetic-hold", "restore-synthetic-request", 1,
              "2026-10-01", "2026-10-02", '{"test_only":true}', "synthetic",
              "confirmed", 1, "synthetic-only"))
        conn.execute("""
            INSERT INTO miet_checkout_contracts
            (hold_id,contract_json,contract_sha256,signer_name,signed_at,
             signature_png_base64,pdf_base64,pdf_sha256)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, ("restore-synthetic-hold", contract_json,
              sha256(contract_json.encode("utf-8")).hexdigest(), "Synthetic Only",
              "2026-09-27T00:00:00+00:00", b64encode(png).decode("ascii"),
              b64encode(pdf).decode("ascii"), sha256(pdf).hexdigest()))
        conn.execute("""
            INSERT INTO einkauf_belege (original_name,stored_name,size,erstellt_am)
            VALUES (%s,%s,%s,%s)
        """, ("synthetic.bin", upload_name, len(payload), "2026-09-27T00:00:00+00:00"))


def run_pg_utility(executable, cfg, options):
    if not executable.is_file():
        raise RuntimeError("PostgreSQL-Testwerkzeuge fehlen.")
    safe_env = {key: os.environ[key] for key in ("SystemRoot", "WINDIR", "PATH", "TEMP", "TMP")
                if key in os.environ}
    safe_env["PGPASSWORD"] = cfg["password"]
    command = [str(executable), "--host", cfg["host"], "--port", str(cfg["port"]),
               "--username", cfg["user"], "--no-password", *options]
    result = subprocess.run(command, env=safe_env, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(executable.name + " scheiterte mit Exitcode " + str(result.returncode))


def run(cfg):
    import psycopg

    source = fresh_database(cfg)
    restored = fresh_database(cfg)
    with tempfile.TemporaryDirectory(prefix="mos-restore-audit-") as temporary:
        root = Path(temporary)
        source_uploads = root / "source" / "uploads"
        target_uploads = root / "restored" / "uploads"
        source_uploads.mkdir(parents=True)
        target_uploads.mkdir(parents=True)
        initialize_real_schema(cfg, source, root / "source")
        seed_synthetic_records(cfg, source, source_uploads)

        with psycopg.connect(**{**cfg, "dbname": source}, connect_timeout=5) as db:
            baseline = audit(db, source_uploads)
        if baseline["signed_documents"]["mos_contract_pdfs"] != 1:
            raise AssertionError("Synthetische Vertrags-PDF fehlt im Ausgangszustand.")
        if baseline["uploads"]["count"] != 1 or baseline["upload_references_checked"] != 1:
            raise AssertionError("Synthetischer Upload ist nicht vollständig referenziert.")

        archive = root / "synthetic-postgres.dump"
        run_pg_utility(PG_BIN / "pg_dump.exe", cfg,
                       ["--format=custom", "--no-owner", "--no-acl", "--file", str(archive), source])
        run_pg_utility(PG_BIN / "pg_restore.exe", cfg,
                       ["--format=custom", "--no-owner", "--no-acl", "--exit-on-error",
                        "--dbname", restored, str(archive)])
        shutil.copy2(source_uploads / "mos-restore-synthetic-document.bin",
                     target_uploads / "mos-restore-synthetic-document.bin")
        with psycopg.connect(**{**cfg, "dbname": restored}, connect_timeout=5) as db:
            recovered = audit(db, target_uploads)
        compare_manifest(recovered, baseline)
        baseline_path = root / "synthetic-baseline.json"
        with patch.dict(os.environ, {
                "MOS_RESTORE_AUDIT_DATABASE_URL": database_url(cfg, source),
                "MOS_RESTORE_AUDIT_UPLOAD_DIR": str(source_uploads)}):
            if audit_cli_main(["--expected-database", source, "--output", str(baseline_path)]) != 0:
                raise AssertionError("Read-only CLI-Quellprüfung fehlgeschlagen.")
        with patch.dict(os.environ, {
                "MOS_RESTORE_AUDIT_DATABASE_URL": database_url(cfg, restored),
                "MOS_RESTORE_AUDIT_UPLOAD_DIR": str(target_uploads)}):
            if audit_cli_main(["--expected-database", restored,
                               "--baseline", str(baseline_path)]) != 0:
                raise AssertionError("Read-only CLI-Restore-Vergleich fehlgeschlagen.")

        (target_uploads / "mos-restore-synthetic-document.bin").write_bytes(b"corrupt")
        with psycopg.connect(**{**cfg, "dbname": restored}, connect_timeout=5) as db:
            try:
                compare_manifest(audit(db, target_uploads), baseline)
            except RestoreAuditError:
                pass
            else:
                raise AssertionError("Veränderter Upload wurde nicht erkannt.")
        (target_uploads / "mos-restore-synthetic-document.bin").unlink()
        with psycopg.connect(**{**cfg, "dbname": restored}, connect_timeout=5) as db:
            try:
                audit(db, target_uploads)
            except RestoreAuditError:
                pass
            else:
                raise AssertionError("Fehlender referenzierter Upload wurde nicht erkannt.")
        shutil.copy2(source_uploads / "mos-restore-synthetic-document.bin",
                     target_uploads / "mos-restore-synthetic-document.bin")
        with psycopg.connect(**{**cfg, "dbname": restored}, connect_timeout=5) as db:
            db.execute("UPDATE miet_checkout_contracts SET pdf_sha256=%s WHERE hold_id=%s",
                       ("0" * 64, "restore-synthetic-hold"))
        with psycopg.connect(**{**cfg, "dbname": restored}, connect_timeout=5) as db:
            try:
                audit(db, target_uploads)
            except RestoreAuditError:
                pass
            else:
                raise AssertionError("Beschädigte Vertrags-PDF-Prüfsumme wurde nicht erkannt.")
        with psycopg.connect(**{**cfg, "dbname": restored}, connect_timeout=5) as db:
            db.execute("UPDATE miet_checkout_contracts SET pdf_sha256=%s WHERE hold_id=%s",
                       (sha256(b"%PDF-1.4\nSynthetic restore audit only\n%%EOF\n").hexdigest(),
                        "restore-synthetic-hold"))
        with psycopg.connect(**{**cfg, "dbname": restored}, connect_timeout=5) as db:
            compare_manifest(audit(db, target_uploads), baseline)
        print(json.dumps({"result": "PASS", "source": source, "restored": restored,
                          "tables": len(baseline["tables"]),
                          "rows": sum(item["rows"] for item in baseline["tables"].values()),
                          "mos_contract_pdfs": baseline["signed_documents"]["mos_contract_pdfs"],
                          "uploads": baseline["uploads"]["count"],
                          "upload_references": baseline["upload_references_checked"],
                          "archive_bytes": archive.stat().st_size,
                          "tamper_tests": 3}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection-file", required=True)
    args = parser.parse_args()
    cfg = load_cluster_config(args.connection_file)
    run(cfg)


if __name__ == "__main__":
    main()
