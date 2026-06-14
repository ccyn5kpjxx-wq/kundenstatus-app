# -*- coding: utf-8 -*-
"""End-to-End-Test Lieferanten-Verwaltung: anlegen (mit E-Mail), Validierung,
bearbeiten, löschen, und Erscheinen in den Teileangebot-Vorschlägen."""
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import app as portal  # noqa: E402

_tmp = tempfile.mkdtemp(prefix="lief_test_")
portal.DB = Path(_tmp) / "test.db"
portal.UPLOAD_DIR = Path(_tmp) / "uploads"

RESULTS = []


def report(label, ok, detail=""):
    RESULTS.append((label, ok))
    print(f"[{'OK  ' if ok else 'FEHLER'}] {label}" + (f" - {detail}" if detail else ""))
    return ok


def with_csrf(client, data=None):
    payload = dict(data or {})
    with client.session_transaction() as s:
        payload["csrf_token"] = s.get("csrf_token")
    return payload


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    client = portal.app.test_client()
    with client.session_transaction() as s:
        s["admin"] = True
    client.get("/admin/lieferanten")

    # 1) Seite lädt leer
    r = client.get("/admin/lieferanten")
    report("Lieferanten-Seite lädt (leer)", r.status_code == 200 and "Lieferanten" in r.get_data(as_text=True))

    # 2) Anlegen mit E-Mail
    r = client.post("/admin/lieferanten/neu", data=with_csrf(client, {
        "name": "Autohaus Käsmann", "kategorie": "Originalteile", "ort": "Mosbach",
        "email": "info@kaesmann.de", "telefon": "06261 9730-0", "notiz": "VW/Audi",
    }), follow_redirects=True)
    seite = r.get_data(as_text=True)
    report("Lieferant angelegt + sichtbar", "Autohaus Käsmann" in seite and "info@kaesmann.de" in seite)
    lief = portal.list_lieferanten()
    report("1 Lieferant in DB", len(lief) == 1, str(len(lief)))
    lid = lief[0]["id"]

    # 3) Ungültige E-Mail wird abgelehnt
    r = client.post("/admin/lieferanten/neu", data=with_csrf(client, {
        "name": "Falschmail GmbH", "email": "kein-at-zeichen",
    }), follow_redirects=True)
    report("Ungültige E-Mail wird abgelehnt", len(portal.list_lieferanten()) == 1)

    # 4) Bearbeiten
    r = client.post(f"/admin/lieferanten/{lid}/bearbeiten", data=with_csrf(client, {
        "name": "Autohaus Käsmann GmbH", "kategorie": "Originalteile", "email": "info@kaesmann.de",
        "aktiv": "1",
    }), follow_redirects=True)
    report("Bearbeiten übernimmt neuen Namen", "Autohaus Käsmann GmbH" in r.get_data(as_text=True))

    # 5) Integration: gepflegter Lieferant erscheint in Teileangebot-Vorschlägen
    #    (eindeutiger Name, der nicht mit den hardcodierten Vorschlägen kollidiert)
    portal.create_lieferant(name="Teile-Express Mosbach", kategorie="Freie Teile", email="verkauf@teile-express.de")
    vorschlaege = portal.versicherung_regionale_lieferanten({"key": "bmw", "label": "BMW"})
    namen = [v["name"] for v in vorschlaege]
    report("Gepflegter Lieferant erscheint als Teileangebot-Vorschlag",
           "Teile-Express Mosbach" in namen, f"{namen}")
    tx = next((v for v in vorschlaege if v["name"] == "Teile-Express Mosbach"), None)
    report("E-Mail im Vorschlag übernommen (kontakt)", tx and tx.get("kontakt") == "verkauf@teile-express.de")

    # 6) Löschen
    r = client.post(f"/admin/lieferanten/{lid}/loeschen", data=with_csrf(client), follow_redirects=True)
    report("Lieferant gelöscht", all(l["id"] != lid for l in portal.list_lieferanten()))

    print()
    fails = [x for x in RESULTS if not x[1]]
    print(f"== ERGEBNIS: {len(RESULTS) - len(fails)}/{len(RESULTS)} Checks bestanden ==")
    for label, ok in fails:
        print(f"  - {label}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
