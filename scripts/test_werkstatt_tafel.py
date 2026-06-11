from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


FEHLER = []


def check(label, ok, detail=""):
    if ok:
        print(f"[OK] {label}")
    else:
        print(f"[FEHLER] {label} {detail}")
        FEHLER.append(label)


def csrf_data(client, data=None):
    payload = dict(data or {})
    with client.session_transaction() as session:
        token = session.get("csrf_token")
    if token:
        payload["csrf_token"] = token
    return payload


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    client = portal.app.test_client()

    # Ohne Login: Tafel und Detail leiten zur Anmeldung um
    response = client.get("/werkstatt/tafel")
    check("Tafel ohne Login leitet zum Login um", response.status_code == 302 and "/werkstatt" in response.headers.get("Location", ""))
    response = client.get("/werkstatt/auftrag/1")
    check("Auftrags-Detail ohne Login leitet zum Login um", response.status_code == 302)

    # Login-Seite laedt (und setzt CSRF-Token in die Session)
    response = client.get("/werkstatt")
    check("Login-Seite laedt", response.status_code == 200 and "Werkstatt-Tafel" in response.get_data(as_text=True))

    # Code setzen wie ueber Admin -> Zugaenge
    portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "TEST99")

    # Falscher Code wird abgelehnt
    response = client.post("/werkstatt", data=csrf_data(client, {"password": "FALSCH"}))
    check("Falscher Code abgelehnt", response.status_code == 200 and "Falscher Werkstatt-Code" in response.get_data(as_text=True))

    # Richtiger Code meldet an
    response = client.post("/werkstatt", data=csrf_data(client, {"password": "TEST99"}))
    check("Richtiger Code leitet zur Tafel", response.status_code == 302 and "/werkstatt/tafel" in response.headers.get("Location", ""))

    response = client.get("/werkstatt/tafel")
    html = response.get_data(as_text=True)
    check("Tafel laedt mit Spalten", response.status_code == 200 and "In Arbeit" in html and "Geplant" in html)

    # Eingeloggt: Login-Seite leitet direkt zur Tafel weiter
    response = client.get("/werkstatt")
    check("Login-Seite leitet Angemeldete zur Tafel", response.status_code == 302)

    # Auftrags-Detail eines echten Auftrags
    auftraege = [a for a in portal.list_auftraege() if int(a.get("status") or 1) <= 4]
    if auftraege:
        auftrag = auftraege[0]
        response = client.get(f"/werkstatt/auftrag/{auftrag['id']}")
        html = response.get_data(as_text=True)
        check(
            f"Auftrags-Detail laedt (#{auftrag['id']})",
            response.status_code == 200 and "Was zu machen ist" in html and "Zur Tafel" in html,
        )
        check("Detail zeigt keine Preise/Kalkulation", "kalkulation" not in html.lower() and "€" not in html.split("footer")[0][:200])
    else:
        print("[HINWEIS] Keine offenen Auftraege in der Test-DB — Detail-Test uebersprungen")

    response = client.get("/werkstatt/auftrag/999999")
    check("Unbekannter Auftrag liefert 404", response.status_code == 404)

    # Code-Rotation wirft angemeldete Bildschirme raus
    portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "NEU777")
    response = client.get("/werkstatt/tafel")
    check("Code-Aenderung meldet alte Sitzungen ab", response.status_code == 302)

    # Admin sieht Tafel ohne Werkstatt-Code
    admin_client = portal.app.test_client()
    with admin_client.session_transaction() as session:
        session["admin"] = True
    response = admin_client.get("/werkstatt/tafel")
    check("Admin sieht Tafel ohne Extra-Login", response.status_code == 200)

    response = admin_client.get("/admin/zugaenge")
    html = response.get_data(as_text=True)
    check("Zugaenge-Seite zeigt Werkstatt-Block", response.status_code == 200 and "Werkstatt-Tafel" in html and "NEU777" in html)

    # Admin: Code aendern ueber das Formular
    admin_client.get("/admin/zugaenge")
    response = admin_client.post("/admin/werkstatt-tafel/zugang", data=csrf_data(admin_client, {"zugangscode": "halle1"}))
    check("Admin-Code-Aenderung leitet zurueck", response.status_code == 302)
    check("Code wird gross geschrieben gespeichert", portal.get_werkstatt_tafel_code() == "HALLE1")

    # Aufraeumen: frischen Zufallscode hinterlassen
    portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, portal.generate_werkstatt_tafel_code())

    if FEHLER:
        print(f"\n{len(FEHLER)} Test(s) fehlgeschlagen.")
        sys.exit(1)
    print("\nWerkstatt-Tafel-Test erfolgreich.")


if __name__ == "__main__":
    main()
