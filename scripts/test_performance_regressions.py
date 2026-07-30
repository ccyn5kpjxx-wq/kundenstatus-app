"""Isolierte Regressionstests fuer die wichtigsten Performance-Schutzregeln."""

from pathlib import Path
import os
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
TEMP_DIR = tempfile.TemporaryDirectory(prefix="kundenstatus-performance-")
TEST_ROOT = Path(TEMP_DIR.name)
(TEST_ROOT / "uploads").mkdir(parents=True, exist_ok=True)

os.environ.update(
    {
        "RENDER": "1",
        "DATABASE_URL": "",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEST_ROOT),
        "SQLITE_DB_PATH": str(TEST_ROOT / "auftraege.db"),
        "UPLOAD_DIR": str(TEST_ROOT / "uploads"),
        "BACKUP_DIR": str(TEST_ROOT / "backups"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "AUTO_BACKUP_ON_STARTUP": "0",
        "GOOGLE_ADS_AUTO_SYNC_ENABLED": "0",
        "LEXWARE_API_KEY": "",
    }
)

sys.path.insert(0, str(ROOT))
import app as portal  # noqa: E402


def main():
    ok = True

    def check(label, condition, detail=""):
        nonlocal ok
        passed = bool(condition)
        suffix = f" ({detail})" if detail else ""
        print(("[OK] " if passed else "[FEHLER] ") + label + suffix)
        ok &= passed

    render_config = (ROOT / "render.yaml").read_text(encoding="utf-8")
    procfile = (ROOT / "Procfile").read_text(encoding="utf-8")
    check(
        "Einzelner Gunicorn-Worker wird nicht request-basiert neu gestartet",
        "--max-requests" not in render_config and "--max-requests" not in procfile,
    )

    with portal.app.test_request_context("/session/ping", method="POST"):
        check(
            "Session-Ping loest kein Aenderungsbackup aus",
            not portal.should_backup_after_request(),
        )
    with portal.app.test_request_context("/admin/erinnerungen/neu", method="POST"):
        check(
            "Echte Datenaenderung behaelt das Sicherheitsbackup",
            portal.should_backup_after_request(),
        )

    original_list_mietfahrzeuge = portal.list_mietfahrzeuge
    try:
        portal.list_mietfahrzeuge = lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("vorab geladene Mietfahrzeuge wurden erneut abgefragt")
        )
        with portal.app.test_request_context("/admin/cockpit"):
            kalender = portal.build_mini_monatskalender(
                [],
                endpoint="betriebs_cockpit",
                include_internal_notes=True,
                miet_fahrzeuge=[],
            )
            heute = portal.mietwagen_heute_uebersicht([])
        check(
            "Cockpit verwendet vorab geladene Mietfahrzeuge nur einmal",
            kalender.get("show_miete") is True
            and heute == {"heute_start": [], "heute_rueckgabe": []},
        )
    finally:
        portal.list_mietfahrzeuge = original_list_mietfahrzeuge

    statements = []
    original_connect = portal.sqlite3.connect

    class CountingConnection(portal.sqlite3.Connection):
        pass

    def counting_connect(*args, **kwargs):
        kwargs["factory"] = CountingConnection
        connection = original_connect(*args, **kwargs)
        connection.set_trace_callback(lambda sql: statements.append(" ".join(sql.split())))
        return connection

    portal.sqlite3.connect = counting_connect
    try:
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        started = time.perf_counter()
        response = client.get("/admin/cockpit")
        elapsed_ms = (time.perf_counter() - started) * 1000
        select_count = sum(
            statement.upper().startswith("SELECT")
            for statement in statements
            if not statement.upper().startswith("PRAGMA")
        )
        check("Cockpit bleibt erreichbar", response.status_code == 200)
        check(
            "Cockpit bleibt unter dem SQL-Abfragebudget",
            select_count <= 50,
            f"{select_count} SELECTs, {elapsed_ms:.1f} ms",
        )

        statements.clear()
        started = time.perf_counter()
        postfach_response = client.get("/admin/postfach")
        postfach_elapsed_ms = (time.perf_counter() - started) * 1000
        postfach_select_count = sum(
            statement.upper().startswith("SELECT")
            for statement in statements
            if not statement.upper().startswith("PRAGMA")
        )
        check("Postfach bleibt erreichbar", postfach_response.status_code == 200)
        check(
            "Postfachbestand wird pro Request nur einmal geladen",
            postfach_select_count <= 25,
            f"{postfach_select_count} SELECTs, {postfach_elapsed_ms:.1f} ms",
        )

        leads_response = client.get("/admin/leads")
        check("Lead-Zentrale bleibt erreichbar", leads_response.status_code == 200)
    finally:
        portal.sqlite3.connect = original_connect

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
