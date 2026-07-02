"""Feature-Test: Fahrzeugeinkauf Scan-Sofort — Status-Endpoint, NEU-Badge, Live-Update."""
from datetime import datetime, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402

TEST_MARKER = "TEST-SCAN-SOFORT"


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    ok = True

    def check(label, cond):
        nonlocal ok
        print(("[OK] " if cond else "[FEHLER] ") + label)
        ok &= bool(cond)

    client = portal.app.test_client()
    with client.session_transaction() as session:
        session["admin"] = True

    # Ausgangszustand herstellen: keine offene Test-Anforderung
    with portal.app.test_request_context("/"):
        db = portal.get_db()
        db.execute("DELETE FROM fahrzeugeinkauf_scan_anfragen WHERE status = 'offen'")
        db.commit()
        db.close()

    # 1) ist_neu-Helper
    jetzt = datetime.now().strftime(portal.DATETIME_FMT)
    alt = (datetime.now() - timedelta(days=5)).strftime(portal.DATETIME_FMT)
    check("ist_neu: frisch + Status neu", portal.fahrzeugeinkauf_ist_neu({"status": "neu", "erstellt_am": jetzt}))
    check("ist_neu: leerer Status zählt als neu", portal.fahrzeugeinkauf_ist_neu({"status": "", "erstellt_am": jetzt}))
    check("ist_neu: 5 Tage alt -> nicht neu", not portal.fahrzeugeinkauf_ist_neu({"status": "neu", "erstellt_am": alt}))
    check("ist_neu: bearbeiteter Status -> nicht neu", not portal.fahrzeugeinkauf_ist_neu({"status": "beobachten", "erstellt_am": jetzt}))
    check("ist_neu: kaputtes Datum -> False", not portal.fahrzeugeinkauf_ist_neu({"status": "neu", "erstellt_am": "quatsch"}))

    # 2) Status-Endpoint ohne offene Anforderung
    response = client.get("/admin/fahrzeugeinkauf/status")
    check("Status-Endpoint antwortet 200", response.status_code == 200)
    daten = response.get_json() or {}
    check("Status: keine Anforderung offen", daten.get("ok") and daten.get("anfrage_offen") is False)

    # 3) Scan anfordern -> Anforderung offen, Seite zeigt Lauf-Hinweis + Polling-Skript
    client.get("/admin/fahrzeugeinkauf")  # setzt csrf_token in der Session
    with client.session_transaction() as session:
        csrf = session.get("csrf_token")
    response = client.post(
        "/admin/fahrzeugeinkauf/scan-anfordern",
        data={"csrf_token": csrf},
        follow_redirects=True,
    )
    check("Scan anfordern lädt (200)", response.status_code == 200)
    html = response.get_data(as_text=True)
    check("Flash nennt Scan-Bereitschaft + Auto-Update", "Scan-Bereitschaft" in html and "aktualisiert sich" in html)
    check("Hinweis 'Scan läuft' sichtbar", "Scan l&auml;uft" in html or "Scan läuft" in html)
    check("Polling-Skript eingebunden", "/admin/fahrzeugeinkauf/status" in html)
    daten = (client.get("/admin/fahrzeugeinkauf/status").get_json() or {})
    check("Status: Anforderung offen", daten.get("anfrage_offen") is True)

    # 4) Scan-Eingang simulieren (wie der Import ihn anlegt) -> Status kippt, NEU-Badge erscheint
    with portal.app.test_request_context("/"):
        db = portal.get_db()
        cursor = db.execute(
            "INSERT INTO fahrzeugeinkauf_scans (titel, scan_datum, quelle, pdf_original_name, pdf_stored_name, notiz, erstellt_am)"
            " VALUES (?, ?, ?, '', '', '', ?)",
            (TEST_MARKER, portal.now_str(), "test", portal.now_str()),
        )
        scan_id = cursor.lastrowid
        db.execute(
            "INSERT INTO fahrzeugeinkauf_fahrzeuge (scan_id, titel, preis, ampel, status, erstellt_am, geaendert_am)"
            " VALUES (?, ?, '9.999 €', 'gruen', 'neu', ?, ?)",
            (scan_id, TEST_MARKER + " Golf 7", portal.now_str(), portal.now_str()),
        )
        db.execute(
            "UPDATE fahrzeugeinkauf_scan_anfragen SET status = 'erledigt', erledigt_am = ? WHERE status = 'offen'",
            (portal.now_str(),),
        )
        db.commit()
        db.close()

    daten = (client.get("/admin/fahrzeugeinkauf/status").get_json() or {})
    check("Status: Anforderung erledigt", daten.get("anfrage_offen") is False)
    check("Status: letzter_scan_id gesetzt", daten.get("letzter_scan_id", 0) >= scan_id)

    response = client.get("/admin/fahrzeugeinkauf?scan_fertig=1")
    html = response.get_data(as_text=True)
    check("Eingetroffen-Banner sichtbar", "Scan eingetroffen" in html)
    check("NEU-Badge am frischen Fahrzeug", 'class="fein-neubadge">NEU' in html)
    check("Testfahrzeug wird gerendert", TEST_MARKER + " Golf 7" in html)

    # 5) Aufräumen (nur eigene Testdaten)
    with portal.app.test_request_context("/"):
        db = portal.get_db()
        db.execute("DELETE FROM fahrzeugeinkauf_fahrzeuge WHERE scan_id = ?", (scan_id,))
        db.execute("DELETE FROM fahrzeugeinkauf_scans WHERE id = ?", (scan_id,))
        db.execute("DELETE FROM fahrzeugeinkauf_scan_anfragen WHERE status = 'erledigt' AND erledigt_am >= ?", (jetzt,))
        db.commit()
        db.close()

    print("\nErgebnis:", "ALLE CHECKS GRÜN" if ok else "MINDESTENS EIN CHECK ROT")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
