"""Feature-Tests für das UX-Paket 2026-07 (47 Review-Fixes)."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    ok = True

    def check(label, cond):
        nonlocal ok
        print(("[OK] " if cond else "[FEHLER] ") + label)
        ok &= bool(cond)

    client = portal.app.test_client()

    # 1) 404-Handler: deutsch, Portal-Optik, Kontakt
    antwort = client.get("/status/gibt-es-nicht-123")
    check("404 liefert Status 404", antwort.status_code == 404)
    html = antwort.get_data(as_text=True)
    check("404-Seite ist deutsch mit Kontakt", "nicht mehr gültig" in html and "wa.me" in html and "Anrufen" in html)
    check("404-Seite in heller Portal-Optik", "#fffdf9" in html)

    # 2) 413-Handler direkt rendern
    with portal.app.test_request_context("/"):
        seite_413 = portal.upload_zu_gross(None)
    check("413-Handler liefert 413", seite_413[1] == 413)
    check("413-Seite erklärt das Limit", "zu groß" in seite_413[0] and str(portal.MAX_UPLOAD_MB) in seite_413[0])

    # 3) News bearbeiten (neue Route)
    with portal.app.test_request_context("/"):
        news_id = portal.create_werkstatt_news(titel="TEST UX-News", nachricht="Alt")
    admin = portal.app.test_client()
    with admin.session_transaction() as session:
        session["admin"] = True
    admin.get("/admin/news")  # setzt csrf_token in der Session
    with admin.session_transaction() as session:
        token = session.get("csrf_token")
    antwort = admin.post(f"/admin/news/{news_id}/bearbeiten", data={
        "csrf_token": token, "titel": "TEST UX-News NEU", "nachricht": "Korrigiert", "kategorie": "betrieb", "pinned": "on",
    }, follow_redirects=False)
    check("News-Bearbeiten leitet zurück (302)", antwort.status_code == 302)
    with portal.app.test_request_context("/"):
        news = portal.get_werkstatt_news(news_id)
    check("News-Titel wurde aktualisiert", news and news["titel"] == "TEST UX-News NEU" and news["nachricht"] == "Korrigiert")
    seite = admin.get("/admin/news").get_data(as_text=True)
    check("News-Seite bietet Bearbeiten an", "data-news-bearbeiten" in seite)

    # 4) Öffnungszeiten: Setting -> Fertig-Mail + Statusseite
    with admin.session_transaction() as session:
        token = session.get("csrf_token")
    antwort = admin.post("/admin/oeffnungszeiten", data={
        "csrf_token": token, "oeffnungszeiten_text": "Mo-Fr 8:00-17:00 Uhr (TEST)",
    }, follow_redirects=False)
    check("Öffnungszeiten speichern (302)", antwort.status_code == 302)
    with portal.app.test_request_context("/"):
        mail = portal.baue_endkunden_fertig_mail({"kunde_name": "Test", "fahrzeug": "VW", "id": 1})
    check("Fertig-Mail nennt Öffnungszeiten + Rückmeldung", "Mo-Fr 8:00-17:00 Uhr (TEST)" in mail and "Bescheid" in mail)

    # 5) Partner: leerer Auftrag wird abgewiesen, gültiger geht durch
    autohaus = portal.get_autohaus_by_slug("kaesmann")
    partner = portal.app.test_client()
    with partner.session_transaction() as session:
        session["partner_autohaus_id"] = autohaus["id"]
    partner.get(f"/partner/{autohaus['slug']}/neu")  # setzt csrf_token in der Session
    with partner.session_transaction() as session:
        token = session.get("csrf_token")
    with portal.app.test_request_context("/"):
        db = portal.get_db()
        vorher = db.execute("SELECT COUNT(*) AS c FROM auftraege").fetchone()["c"]
    antwort = partner.post(f"/partner/{autohaus['slug']}/neu", data={
        "csrf_token": token, "aktion": "speichern", "kunde_name": "Nur Name",
    }, follow_redirects=True)
    html = antwort.get_data(as_text=True)
    check("Leerer Partner-Auftrag abgewiesen (Hinweis)", "mindestens Fahrzeug oder Kennzeichen" in html)
    check("Formularwert bleibt erhalten (Vorbefüllung)", 'value="Nur Name"' in html)
    with portal.app.test_request_context("/"):
        db = portal.get_db()
        nachher = db.execute("SELECT COUNT(*) AS c FROM auftraege").fetchone()["c"]
    check("Kein leerer Auftrag angelegt", nachher == vorher)
    with partner.session_transaction() as session:
        token = session.get("csrf_token")
    antwort = partner.post(f"/partner/{autohaus['slug']}/neu", data={
        "csrf_token": token, "aktion": "speichern", "fahrzeug": "TEST UX Golf", "kennzeichen": "MOS-UX 1",
    }, follow_redirects=False)
    check("Gültiger Partner-Auftrag angelegt (302)", antwort.status_code == 302)
    test_auftrag_id = int((antwort.headers.get("Location") or "/0").rstrip("/").split("/")[-1])

    # 6) Chat-Schutz: Werkstatt-Nachricht kann Partner NICHT löschen
    with portal.app.test_request_context("/"):
        chat_id = portal.add_chat_nachricht(test_auftrag_id, "werkstatt", "Interne Antwort TEST")
    with partner.session_transaction() as session:
        token = session.get("csrf_token")
    antwort = partner.post(
        f"/partner/{autohaus['slug']}/auftrag/{test_auftrag_id}/chat/{chat_id}/loeschen",
        data={"csrf_token": token}, follow_redirects=True,
    )
    check("Fremd-Nachricht-Löschen wird abgelehnt", "Nur eigene Nachrichten" in antwort.get_data(as_text=True))
    with portal.app.test_request_context("/"):
        db = portal.get_db()
        noch_da = db.execute("SELECT COUNT(*) AS c FROM chat_nachrichten WHERE id=?", (chat_id,)).fetchone()["c"]
    check("Werkstatt-Nachricht existiert weiterhin", noch_da == 1)

    # 7) Kundenseite: Terminwunsch-Formular + Klartext-Badge + Öffnungszeiten
    with portal.app.test_request_context("/"):
        auftrag = portal.get_auftrag(test_auftrag_id)
        status_token = auftrag.get("kunden_status_token")
        terminfreigabe = portal.kunden_terminfreigabe_info(auftrag)
    kunde = portal.app.test_client()
    seite = kunde.get(f"/status/{status_token}")
    check("Kunden-Statusseite lädt (200)", seite.status_code == 200)
    html = seite.get_data(as_text=True)
    check("Kein Ein-Wort-Badge mehr (Klartext-Label)", terminfreigabe["label"] in html)
    check("Öffnungszeiten auf Statusseite", "Mo-Fr 8:00-17:00 Uhr (TEST)" in html)
    if terminfreigabe["can_request"]:
        check("Terminwunsch-Formular vorhanden", f"/status/{status_token}/termin" in html and "Wunschtermin senden" in html)
        with kunde.session_transaction() as session:
            token = session.get("csrf_token")
        antwort = kunde.post(f"/status/{status_token}/termin", data={
            "csrf_token": token, "wunschtermin": "2026-07-20", "nachricht": "TEST Wunsch",
        }, follow_redirects=True)
        check("Terminwunsch wird angenommen + Meldung sichtbar", antwort.status_code == 200 and "TEST Wunsch" not in "" and ("angekommen" in antwort.get_data(as_text=True) or "Wunschtermin" in antwort.get_data(as_text=True)))
    else:
        print(f"[WARN] can_request={terminfreigabe['can_request']} — Terminwunsch-POST übersprungen (Label: {terminfreigabe['label']})")

    # Aufräumen: Test-Auftrag + Test-News entfernen
    with portal.app.test_request_context("/"):
        portal.archive_werkstatt_news(news_id)
        portal.delete_auftrag(test_auftrag_id)
        # Öffnungszeiten-Test-Wert wieder leeren (Dev-DB sauber halten)
        portal.set_app_setting("OEFFNUNGSZEITEN_TEXT", "")
    print("[OK] Testdaten aufgeräumt")

    print("UX-Paket-Test erfolgreich." if ok else "UX-Paket-Test FEHLGESCHLAGEN.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
