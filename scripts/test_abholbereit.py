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


def ende():
    if FEHLER:
        print(f"\n{len(FEHLER)} Test(s) fehlgeschlagen.")
        sys.exit(1)
    print("\nAbholbereit-Test erfolgreich.")


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()

    # 1) Migration: neue Spalten existieren (Postgres-sicherer ensure_column-Weg)
    db = portal.get_db()
    cols = portal.get_table_columns(db, "auftraege")
    db.close()
    check("Spalte fahrzeug_abholbereit existiert", "fahrzeug_abholbereit" in cols)
    check("Spalte fahrzeug_abholbereit_am existiert", "fahrzeug_abholbereit_am" in cols)

    # Auftrag eines Autohauses mit Hol-/Bringservice in Status 2 vorbereiten
    kandidaten = [a for a in portal.list_auftraege() if int(a.get("autohaus_id") or 0) > 0]
    if not kandidaten:
        check("Test-Auftrag mit Autohaus vorhanden", False, "keine Autohaus-Auftraege geseedet")
        return ende()
    auftrag = kandidaten[0]
    aid = auftrag["id"]
    haus_id = int(auftrag["autohaus_id"])
    autohaus = portal.get_autohaus(haus_id)
    slug = autohaus["slug"]
    db = portal.get_db()
    db.execute(
        "UPDATE auftraege SET status=2, transport_art='hol_und_bring', "
        "fahrzeug_abholbereit=0, fahrzeug_abholbereit_am='' WHERE id=?",
        (aid,),
    )
    db.commit()
    db.close()

    # 2) Partner-Seite (eingeloggt) zeigt den Button
    pc = portal.app.test_client()
    with pc.session_transaction() as session:
        session["partner_autohaus_id"] = haus_id
    pc.get(f"/partner/{slug}/auftrag/{aid}")  # setzt CSRF-Token in die Session
    html = pc.get(f"/partner/{slug}/auftrag/{aid}").get_data(as_text=True)
    check(
        "Partner-Seite zeigt Abholbereit-Button (Status 2 + Hol/Bring)",
        'value="abholbereit"' in html and "Fahrzeug ist abholbereit" in html,
    )

    # 3) POST abholbereit -> Flag + Zeitstempel gesetzt
    resp = pc.post(f"/partner/{slug}/auftrag/{aid}", data=csrf_data(pc, {"aktion": "abholbereit"}))
    a2 = portal.get_auftrag(aid)
    check(
        "POST abholbereit setzt Flag + Zeitstempel",
        resp.status_code == 302 and a2["fahrzeug_abholbereit"] is True and bool(a2["fahrzeug_abholbereit_am"]),
        f"status={resp.status_code} flag={a2['fahrzeug_abholbereit']} am={a2['fahrzeug_abholbereit_am']!r}",
    )

    # 3b) Partner-Seite zeigt jetzt Bestaetigung statt Button
    html = pc.get(f"/partner/{slug}/auftrag/{aid}").get_data(as_text=True)
    check("Partner-Seite zeigt Bestaetigung statt Button", "Als abholbereit gemeldet" in html)

    # 4) In-App-Benachrichtigung wurde angelegt
    benachrichtigungen = portal.list_benachrichtigungen(aid)
    check(
        "In-App-Benachrichtigung 'abholbereit' angelegt",
        any("abholbereit" in (str(b.get("titel", "")) + str(b.get("nachricht", ""))).lower() for b in benachrichtigungen),
    )

    # 5) Werkstatt-Tafel zeigt den Hinweis auf der Karte
    wc = portal.app.test_client()
    portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "TEST99")
    wc.get("/werkstatt")
    wc.post("/werkstatt", data=csrf_data(wc, {"password": "TEST99"}))
    tafel = wc.get("/werkstatt/tafel").get_data(as_text=True)
    check("Werkstatt-Tafel zeigt Abholbereit-Hinweis", "Fahrzeug ist abholbereit" in tafel)

    # 6) Wechsel auf 'In Arbeit' (3) loescht das Flag -> Hinweis verschwindet
    with wc.session_transaction() as session:
        csrf = session.get("csrf_token")
    wc.post(
        f"/werkstatt/auftrag/{aid}/status/3",
        headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
    )
    a3 = portal.get_auftrag(aid)
    check(
        "Status -> In Arbeit loescht das Abholbereit-Flag",
        a3["status"] == 3 and a3["fahrzeug_abholbereit"] is False,
        f"status={a3['status']} flag={a3['fahrzeug_abholbereit']}",
    )
    tafel2 = wc.get("/werkstatt/tafel").get_data(as_text=True)
    check("Werkstatt-Tafel zeigt den Hinweis nach 'In Arbeit' nicht mehr", "Fahrzeug ist abholbereit" not in tafel2)

    ende()


if __name__ == "__main__":
    main()
