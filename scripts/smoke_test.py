from pathlib import Path
from io import BytesIO
from datetime import date, timedelta
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def check(label, response, expected_statuses):
    if response.status_code not in expected_statuses:
        print(f"[FEHLER] {label}: Status {response.status_code}, erwartet {sorted(expected_statuses)}")
        return False
    print(f"[OK] {label}: Status {response.status_code}")
    return True


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
    portal.WEATHER_CACHE["payload"] = {
        "ok": True,
        "location": {"name": "Mosbach", "latitude": 49.3536, "longitude": 9.1517},
        "current": {"temperature_2m": 12.4, "weather_code": 1, "wind_speed_10m": 8.2},
        "hourly": {
            "time": ["2026-05-13T12:00", "2026-05-13T13:00", "2026-05-13T14:00"],
            "temperature_2m": [12.4, 13.0, 13.5],
            "weather_code": [1, 2, 2],
            "wind_speed_10m": [8.2, 9.1, 10.0],
        },
        "units": {},
        "updated_at": "2026-05-13T12:00:00",
    }
    portal.WEATHER_CACHE["expires_at"] = time.time() + 300
    ok = True

    login_response = client.get("/login")
    ok &= check("Login-Seite", login_response, {200})
    login_html = login_response.get_data(as_text=True)
    has_csrf = "name=\"csrf_token\"" in login_html
    print("[OK] Login-Formular enthält CSRF-Token" if has_csrf else "[FEHLER] Login-Formular ohne CSRF-Token")
    ok &= has_csrf
    login_clock_ok = "data-live-clock" not in login_html and "data-live-date" not in login_html
    print(
        "[OK] Login-Seite bleibt ohne Live-Uhr"
        if login_clock_ok
        else "[FEHLER] Login-Seite enthaelt noch die Live-Uhr"
    )
    ok &= login_clock_ok
    login_autocomplete_ok = (
        'autocomplete="username"' in login_html
        and 'autocomplete="current-password"' in login_html
        and 'name="username"' in login_html
        and 'name="password"' in login_html
    )
    print(
        "[OK] Admin-Login erlaubt Passwortmanager"
        if login_autocomplete_ok
        else "[FEHLER] Admin-Login blockiert Passwortmanager-Hinweise"
    )
    ok &= login_autocomplete_ok
    security_headers_ok = (
        login_response.headers.get("X-Content-Type-Options") == "nosniff"
        and login_response.headers.get("X-Frame-Options") == "SAMEORIGIN"
        and "frame-ancestors 'self'" in login_response.headers.get("Content-Security-Policy", "")
    )
    print(
        "[OK] Sicherheitsheader werden gesetzt"
        if security_headers_ok
        else "[FEHLER] Sicherheitsheader fehlen"
    )
    ok &= security_headers_ok
    with portal.app.test_request_context("/", environ_base={"REMOTE_ADDR": "203.0.113.250"}, headers={"Host": "example.test"}):
        external_default_denied = not portal.admin_password_matches(portal.DEFAULT_ADMIN_PASS)
    print(
        "[OK] Admin-Defaultpasswort wird extern nicht akzeptiert"
        if external_default_denied
        else "[FEHLER] Admin-Defaultpasswort ist extern akzeptiert"
    )
    ok &= external_default_denied
    with portal.app.test_request_context("/", environ_base={"REMOTE_ADDR": "203.0.113.251"}, headers={"Host": "example.test"}):
        for _ in range(portal.LOGIN_RATE_LIMIT_MAX):
            portal.record_failed_login("smoke", "audit")
        limited, wait_seconds = portal.login_rate_limit_status("smoke", "audit")
        portal.clear_login_attempts("smoke", "audit")
    rate_limit_ok = not limited and wait_seconds == 0
    print(
        "[OK] Login-Rate-Limit bleibt deaktiviert"
        if rate_limit_ok
        else "[FEHLER] Login-Rate-Limit sperrt noch"
    )
    ok &= rate_limit_ok
    ok &= check(
        "Login-POST ohne CSRF lädt Login neu",
        client.post("/login", data={"password": "falsch"}),
        {302},
    )
    ok &= check(
        "Login-POST mit CSRF verarbeitet",
        client.post("/login", data=csrf_data(client, {"password": "falsch"})),
        {200},
    )
    ok &= check("Admin ohne Login geschuetzt", client.get("/admin"), {302})
    partner_index_response = client.get("/partner")
    ok &= check("Partner-Einstieg", partner_index_response, {200})
    partner_index_html = partner_index_response.get_data(as_text=True)
    central_partner_login = "name=\"portal_key\"" in partner_index_html and "name=\"password\"" in partner_index_html
    print(
        "[OK] Zentraler Partner-Login vorhanden"
        if central_partner_login
        else "[FEHLER] Zentraler Partner-Login fehlt"
    )
    ok &= central_partner_login
    partner_autocomplete_ok = (
        'autocomplete="username"' in partner_index_html
        and 'autocomplete="current-password"' in partner_index_html
        and 'name="username"' in partner_index_html
        and 'name="password"' in partner_index_html
    )
    print(
        "[OK] Partner-Login erlaubt Passwortmanager"
        if partner_autocomplete_ok
        else "[FEHLER] Partner-Login blockiert Passwortmanager-Hinweise"
    )
    ok &= partner_autocomplete_ok
    partner_login_clock_ok = "data-live-clock" not in partner_index_html and "data-live-date" not in partner_index_html
    print(
        "[OK] Partner-Login bleibt ohne Live-Uhr"
        if partner_login_clock_ok
        else "[FEHLER] Partner-Login enthaelt noch die Live-Uhr"
    )
    ok &= partner_login_clock_ok
    old_office_blocked = not portal.allowed_file("altauftrag.doc") and not portal.allowed_file("tabelle.xls")
    modern_office_allowed = portal.allowed_file("auftrag.docx") and portal.allowed_file("kalkulation.xlsx")
    print(
        "[OK] Alte Office-Formate werden nicht als Upload angeboten"
        if old_office_blocked
        else "[FEHLER] Alte Office-Formate sind noch als Upload erlaubt"
    )
    ok &= old_office_blocked
    print(
        "[OK] Moderne Office-Formate bleiben erlaubt"
        if modern_office_allowed
        else "[FEHLER] Moderne Office-Formate sind nicht erlaubt"
    )
    ok &= modern_office_allowed
    ok &= check("Favicon vorhanden", client.get("/favicon.ico"), {200})

    with client.session_transaction() as session:
        session["admin"] = True
    admin_response = client.get("/admin")
    ok &= check("Admin mit Login", admin_response, {200})
    logged_in_login_response = client.get("/login", follow_redirects=False)
    ok &= check("Login-Seite leitet eingeloggte Admins weiter", logged_in_login_response, {302})
    logged_in_login_target_ok = "/admin/cockpit" in (logged_in_login_response.headers.get("Location") or "")
    print(
        "[OK] Eingeloggte Admins bleiben beim Neuladen aus dem Login heraus"
        if logged_in_login_target_ok
        else "[FEHLER] Eingeloggte Admins landen wieder auf der Login-Seite"
    )
    ok &= logged_in_login_target_ok
    admin_html = admin_response.get_data(as_text=True)
    admin_lackierportal_simple_ok = (
        "Alle Aufträge" in admin_html
        and all(text in admin_html for text in ("Auswahl", "Fahrzeug", "Analyse", "Termine", "Status", "Aktion"))
        and "Autohäuser anlegen und Termine zentral steuern" not in admin_html
        and "Alle Fahrzeuge, nach Dringlichkeit sortiert" not in admin_html
        and "Konfiguration bitte prüfen" not in admin_html
        and "data-workshop-row" not in admin_html
        and "Rechnung schreiben in Lexware" not in admin_html
    )
    print(
        "[OK] Lackierportal zeigt wieder die einfache Auftragsliste"
        if admin_lackierportal_simple_ok
        else "[FEHLER] Lackierportal ist noch zu umfangreich"
    )
    ok &= admin_lackierportal_simple_ok
    cockpit_response = client.get("/admin/cockpit")
    ok &= check("Betriebs-Cockpit mit Login", cockpit_response, {200})
    cockpit_html = cockpit_response.get_data(as_text=True)
    zugaenge_ui_slim_ok = (
        "/admin/zugaenge" in cockpit_html
        and "Zugänge öffnen" not in cockpit_html
        and "Autohaus-Übersicht" not in cockpit_html
    )
    print(
        "[OK] Betriebs-Cockpit zeigt nur den schlanken Zugänge-Menüpunkt"
        if zugaenge_ui_slim_ok
        else "[FEHLER] Betriebs-Cockpit zeigt die Zugänge-Verwaltung zu prominent"
    )
    ok &= zugaenge_ui_slim_ok
    zugaenge_response = client.get("/admin/zugaenge")
    ok &= check("Autohaus-Zugaenge mit Login", zugaenge_response, {200})
    zugaenge_html = zugaenge_response.get_data(as_text=True)
    zugaenge_page_slim_ok = (
        "Autohaus-Zugänge" in zugaenge_html
        and "/portal/" in zugaenge_html
        and "Lokal öffnen" in zugaenge_html
        and "Link kopieren" not in zugaenge_html
        and "Lackierauftrag PDF" not in zugaenge_html
        and "Autohaus bearbeiten" not in zugaenge_html
    )
    print(
        "[OK] Autohaus-Zugaenge-Seite bleibt schlank"
        if zugaenge_page_slim_ok
        else "[FEHLER] Autohaus-Zugaenge-Seite enthält wieder zu viele Funktionen"
    )
    ok &= zugaenge_page_slim_ok
    removed_modules_ok = all(
        text not in cockpit_html
        for text in (
            "Mitarbeiter",
            "E-Mail-Zentrale",
            "Rechnungskontrolle",
            "Einkauf",
            "Aktuelle Woche",
            "Nächste Termine",
        )
    )
    print(
        "[OK] Betriebs-Cockpit blendet entfernte Module aus"
        if removed_modules_ok
        else "[FEHLER] Betriebs-Cockpit zeigt entfernte Module noch an"
    )
    ok &= removed_modules_ok
    start_clock_calendar_ok = (
        "data-live-clock" in cockpit_html
        and "data-live-date" in cockpit_html
        and "Monatsblick" in cockpit_html
        and "Kalender öffnen" in cockpit_html
        and "/session/ping" in cockpit_html
        and "Feiertag" in cockpit_html
        and "Betriebsurlaub" in cockpit_html
    )
    print(
        "[OK] Startseite zeigt Uhr, Datum und Kalender"
        if start_clock_calendar_ok
        else "[FEHLER] Startseite zeigt Uhr/Datum/Kalender nicht vollständig"
    )
    ok &= start_clock_calendar_ok
    cockpit_weather_ui_ok = (
        "data-weather-widget" in cockpit_html
        and "Mosbach" in cockpit_html
        and "/api/wetter/mosbach" in cockpit_html
        and "Wetter in den nächsten 3 Stunden" in cockpit_html
    )
    print(
        "[OK] Betriebs-Cockpit zeigt Mosbach-Wetter"
        if cockpit_weather_ui_ok
        else "[FEHLER] Betriebs-Cockpit zeigt den Mosbach-Wetterblock nicht korrekt"
    )
    ok &= cockpit_weather_ui_ok
    weather_response = client.get("/api/wetter/mosbach")
    ok &= check("Mosbach-Wetter API", weather_response, {200})
    weather_data = weather_response.get_json(silent=True) or {}
    weather_api_ok = (
        weather_data.get("ok") is True
        and weather_data.get("location", {}).get("name") == "Mosbach"
        and weather_data.get("current", {}).get("temperature_2m") is not None
    )
    print(
        "[OK] Mosbach-Wetter API liefert aktuelle Werte"
        if weather_api_ok
        else "[FEHLER] Mosbach-Wetter API liefert keine aktuellen Werte"
    )
    ok &= weather_api_ok
    session_timeout_ok = portal.app.permanent_session_lifetime >= timedelta(minutes=5)
    print(
        "[OK] Sitzung haelt mindestens 5 Minuten Inaktivitaet"
        if session_timeout_ok
        else "[FEHLER] Sitzung laeuft zu schnell ab"
    )
    ok &= session_timeout_ok
    session_ping_response = client.post("/session/ping", data=csrf_data(client))
    ok &= check("Sitzung bei Aktivitaet verlaengern", session_ping_response, {200})
    session_ping_data = session_ping_response.get_json(silent=True) or {}
    session_ping_ok = (
        session_ping_data.get("authenticated") is True
        and session_ping_data.get("idle_timeout_seconds", 0) >= 300
    )
    print(
        "[OK] Session-Ping bestaetigt aktiven Login"
        if session_ping_ok
        else "[FEHLER] Session-Ping verlaengert den Login nicht"
    )
    ok &= session_ping_ok
    remember_token = portal.create_remember_login_token("admin")
    remember_row = portal.get_remember_login("admin", remember_token)
    remember_login_ok = bool(
        bool(remember_row)
        and remember_row.get("scope") == "admin"
        and remember_row.get("expires_at")
    )
    portal.delete_remember_login_token(remember_token)
    print(
        "[OK] Dauerlogin-Token wird gespeichert und erkannt"
        if remember_login_ok
        else "[FEHLER] Dauerlogin-Token funktioniert nicht"
    )
    ok &= remember_login_ok
    reminder_form_ok = "Kurz notieren, was später erledigt werden soll" in cockpit_html and "/admin/erinnerungen/neu" in cockpit_html
    print(
        "[OK] Startseite zeigt Erinnerungsfeld"
        if reminder_form_ok
        else "[FEHLER] Startseite zeigt kein Erinnerungsfeld"
    )
    ok &= reminder_form_ok
    quick_search_response = client.get("/admin/cockpit/auftraege-suche?q=MOS-K")
    ok &= check("Cockpit-Auftragssuche nach Kennzeichen", quick_search_response, {200})
    quick_search_data = quick_search_response.get_json(silent=True) or {}
    quick_search_items = quick_search_data.get("items") or []
    quick_search_result_ok = any(
        "MOS-K" in item.get("kennzeichen", "") and "/admin/auftrag/" in item.get("url", "")
        for item in quick_search_items
    )
    print(
        "[OK] Cockpit-Auftragssuche liefert klickbaren Auftrag"
        if quick_search_result_ok
        else "[FEHLER] Cockpit-Auftragssuche liefert keinen klickbaren Auftrag"
    )
    ok &= quick_search_result_ok
    reminder_text = "Smoke-Test Erinnerung erledigen"
    reminder_post = client.post(
        "/admin/erinnerungen/neu",
        data=csrf_data(client, {"text": reminder_text, "next": "/admin/cockpit"}),
        follow_redirects=False,
    )
    ok &= check("Erinnerung speichern", reminder_post, {302})
    reminders = portal.list_erinnerungen(limit=20)
    created_reminder = next((item for item in reminders if item["text"] == reminder_text), None)
    reminder_saved_ok = created_reminder is not None
    print(
        "[OK] Erinnerung ist gespeichert"
        if reminder_saved_ok
        else "[FEHLER] Erinnerung wurde nicht gespeichert"
    )
    ok &= reminder_saved_ok
    if created_reminder:
        done_response = client.post(
            f"/admin/erinnerungen/{created_reminder['id']}/erledigt",
            data=csrf_data(client, {"next": "/admin/cockpit"}),
            follow_redirects=False,
        )
        ok &= check("Erinnerung erledigen", done_response, {302})
    cockpit_news_hidden = "News-Fenster öffnen" not in cockpit_html and "Werkstatt-News" not in cockpit_html
    print(
        "[OK] Betriebs-Cockpit blendet interne Werkstatt-News aus"
        if cockpit_news_hidden
        else "[FEHLER] Betriebs-Cockpit zeigt noch Werkstatt-News"
    )
    ok &= cockpit_news_hidden
    cockpit_hide_response = client.post(
        "/admin/cockpit/eingang/smoke-start-delete/loeschen",
        data=csrf_data(client, {"next": "/admin/cockpit"}),
        follow_redirects=False,
    )
    ok &= check("Cockpit-Meldung ausblenden", cockpit_hide_response, {302})
    hidden_ok = "smoke-start-delete" in portal.postfach_hidden_keys("admin", 0)
    print(
        "[OK] Cockpit blendet gelöschte Meldungen aus"
        if hidden_ok
        else "[FEHLER] Cockpit-Meldung wurde nicht ausgeblendet"
    )
    ok &= hidden_ok
    smoke_email_id = portal.create_werkstatt_email(
        absender_name="Smoke Test",
        betreff="Smoke Cockpit entfernen",
        nachricht="Testnachricht ohne Kundenbezug",
        quelle="smoke",
    )
    cockpit_email_response = client.post(
        f"/admin/cockpit/eingang/admin-email-{smoke_email_id}/loeschen",
        data=csrf_data(client, {"next": "/admin/cockpit"}),
        follow_redirects=False,
    )
    ok &= check("Cockpit-E-Mail entfernen", cockpit_email_response, {302})
    smoke_email = portal.get_werkstatt_email(smoke_email_id)
    email_done_ok = bool(smoke_email and smoke_email["status"] == "erledigt")
    print(
        "[OK] Cockpit-E-Mail wird als erledigt markiert"
        if email_done_ok
        else "[FEHLER] Cockpit-E-Mail bleibt neu"
    )
    ok &= email_done_ok
    db = portal.get_db()
    try:
        db.execute(
            "DELETE FROM postfach_ausblendungen WHERE empfaenger='admin' AND autohaus_id=0 AND item_key=?",
            ("smoke-start-delete",),
        )
        db.execute("DELETE FROM werkstatt_emails WHERE id=?", (smoke_email_id,))
        db.commit()
    finally:
        db.close()
    kalender_response = client.get("/admin/kalender")
    ok &= check("Interner Kalender mit Login", kalender_response, {200})
    kalender_html = kalender_response.get_data(as_text=True)
    kalender_week_ok = "Diese Woche" in kalender_html and "KW " in kalender_html
    print(
        "[OK] Kalender zeigt Wochenübersicht"
        if kalender_week_ok
        else "[FEHLER] Kalender-Wochenübersicht fehlt"
    )
    ok &= kalender_week_ok
    kalender_news_ok = (
        "Betriebsurlaub" in kalender_html
        and "19.08.2026" in kalender_html
        and "04.09.2026" in kalender_html
    )
    print(
        "[OK] Kalender zeigt Werkstatt-News"
        if kalender_news_ok
        else "[FEHLER] Kalender zeigt Werkstatt-News nicht"
    )
    ok &= kalender_news_ok
    mitarbeiter_response = client.get("/admin/mitarbeiter")
    ok &= check("Mitarbeiter mit Login", mitarbeiter_response, {200})
    mitarbeiter_html = mitarbeiter_response.get_data(as_text=True)
    mitarbeiter_page_ok = "Team und Urlaub" in mitarbeiter_html and "Mitarbeiter speichern" in mitarbeiter_html
    print(
        "[OK] Mitarbeiterseite zeigt Team- und Urlaubsverwaltung"
        if mitarbeiter_page_ok
        else "[FEHLER] Mitarbeiterseite zeigt die Urlaubsverwaltung nicht"
    )
    ok &= mitarbeiter_page_ok

    smoke_mitarbeiter_id = None
    smoke_urlaub_id = None
    try:
        smoke_mitarbeiter_id = portal.create_mitarbeiter("Smoke Mitarbeiter", rolle="Test")
        smoke_urlaub_id = portal.create_mitarbeiter_urlaub(
            smoke_mitarbeiter_id,
            date.today(),
            date.today(),
            "Smoke Urlaub",
        )
        mitarbeiter_after_create = client.get("/admin/mitarbeiter")
        ok &= check("Mitarbeiter-Urlaub angelegt", mitarbeiter_after_create, {200})
        mitarbeiter_after_html = mitarbeiter_after_create.get_data(as_text=True)
        mitarbeiter_urlaub_ok = "Smoke Mitarbeiter" in mitarbeiter_after_html and "Smoke Urlaub" in mitarbeiter_after_html
        print(
            "[OK] Mitarbeiterurlaub erscheint beim Mitarbeiter"
            if mitarbeiter_urlaub_ok
            else "[FEHLER] Mitarbeiterurlaub erscheint nicht beim Mitarbeiter"
        )
        ok &= mitarbeiter_urlaub_ok
        kalender_urlaub_response = client.get("/admin/kalender?suche=Smoke%20Mitarbeiter")
        ok &= check("Mitarbeiterurlaub im Kalender", kalender_urlaub_response, {200})
        kalender_urlaub_html = kalender_urlaub_response.get_data(as_text=True)
        kalender_urlaub_ok = "Smoke Mitarbeiter im Urlaub" in kalender_urlaub_html and "Smoke Urlaub" in kalender_urlaub_html
        print(
            "[OK] Mitarbeiterurlaub wird in den Kalender übernommen"
            if kalender_urlaub_ok
            else "[FEHLER] Mitarbeiterurlaub fehlt im Kalender"
        )
        ok &= kalender_urlaub_ok
    finally:
        if smoke_mitarbeiter_id:
            db = portal.get_db()
            try:
                if smoke_urlaub_id:
                    db.execute("DELETE FROM mitarbeiter_urlaub WHERE id=?", (smoke_urlaub_id,))
                db.execute("DELETE FROM mitarbeiter WHERE id=?", (smoke_mitarbeiter_id,))
                db.commit()
            finally:
                db.close()
    news_response = client.get("/admin/news")
    ok &= check("Werkstatt-News mit Login", news_response, {200})
    news_html = news_response.get_data(as_text=True)
    news_ok = (
        "News-Fenster" in news_html
        and "Betriebsurlaub" in news_html
        and "Betriebsurlaub vom 19.08.2026 bis 04.09.2026." in news_html
    )
    print(
        "[OK] Werkstatt-News-Fenster zeigt Betriebsurlaub"
        if news_ok
        else "[FEHLER] Werkstatt-News-Fenster zeigt Betriebsurlaub nicht"
    )
    ok &= news_ok
    email_response = client.get("/admin/emails")
    ok &= check("E-Mail-Zentrale mit Login", email_response, {200})
    email_html = email_response.get_data(as_text=True)
    email_ok = (
        "E-Mail-Zentrale" in email_html
        and "/api/werkstatt/emails" in email_html
        and "Postfach jetzt abrufen" in email_html
        and "IMAP" in email_html
    )
    print(
        "[OK] E-Mail-Zentrale zeigt API- und IMAP-Hinweis"
        if email_ok
        else "[FEHLER] E-Mail-Zentrale zeigt API-/IMAP-Hinweis nicht"
    )
    ok &= email_ok
    ok &= check("Werkstatt-Postfach mit Login", client.get("/admin/postfach"), {200})
    rechnungen_response = client.get("/admin/rechnungen")
    ok &= check("Rechnungskontrolle mit Login", rechnungen_response, {200})
    rechnungen_html = rechnungen_response.get_data(as_text=True)
    rechnungen_ok = (
        "Kontoauszug hochladen" in rechnungen_html
        and "Kontoauszug analysieren" in rechnungen_html
        and "Lexware wird geprüft" in rechnungen_html
        and "Einnahmen" in rechnungen_html
        and "Ausgaben" in rechnungen_html
        and "Umsatz / Saldo" in rechnungen_html
        and "Offene Rechnungen" in rechnungen_html
    )
    print(
        "[OK] Rechnungskontrolle zeigt Kontoauszug-Upload, Ladehinweis und Kennzahlen"
        if rechnungen_ok
        else "[FEHLER] Rechnungskontrolle zeigt Kontoauszug-Upload/Kennzahlen nicht"
    )
    ok &= rechnungen_ok
    einkauf_response = client.get("/admin/einkauf")
    ok &= check("Werkstatt-Einkauf mit Login", einkauf_response, {200})
    einkauf_html = einkauf_response.get_data(as_text=True)
    einkauf_ok = (
        "Topcolor-Liste" in einkauf_html
        and "Offene Einkaufspositionen" in einkauf_html
        and "Angelegte Teile" in einkauf_html
        and "Produkte aus Rechnung" in einkauf_html
        and "Topcolor API und Konditionen" in einkauf_html
        and "Produktbild-URL" in einkauf_html
    )
    print(
        "[OK] Einkauf zeigt Topcolor- und Rechnungsmodul"
        if einkauf_ok
        else "[FEHLER] Einkauf zeigt Topcolor-/Rechnungsmodul nicht"
    )
    ok &= einkauf_ok
    parsed_einkauf = portal.extract_einkauf_beleg_positions(
        "\n".join(
            [
                "Bitte Rechnung über 129,90 EUR zahlen.",
                "1,/KG -% 20,3036,9020. 45,00 EUR",
                "12345 Schleifscheibe P500 2 Stk 12,50 EUR",
                "777 Klarlack 1 Dose 88,90 EUR",
            ]
        ),
        "test.pdf",
    )
    parsed_names = {item["produkt_name"] for item in parsed_einkauf}
    einkauf_parser_ok = (
        parsed_names == {"Schleifscheibe P500", "Klarlack"}
        and all("Rechnung" not in item["produkt_name"] for item in parsed_einkauf)
    )
    print(
        "[OK] Einkauf ignoriert OCR-Rechnungszeilen"
        if einkauf_parser_ok
        else "[FEHLER] Einkauf übernimmt OCR-Rechnungszeilen als Artikel"
    )
    ok &= einkauf_parser_ok
    parsed_calendar_note = portal.parse_kalender_schnelleintrag("14.04 Geburtstag Max jährlich")
    parser_ok = (
        parsed_calendar_note["datum"].startswith("14.04.")
        and parsed_calendar_note["wiederholung"] == "jaehrlich"
        and parsed_calendar_note["kategorie"] == "geburtstag"
    )
    print(
        "[OK] Kalender-Schnelleintrag erkennt Datum und Wiederholung"
        if parser_ok
        else "[FEHLER] Kalender-Schnelleintrag erkennt Datum/Wiederholung nicht"
    )
    ok &= parser_ok
    bonus_beispiel = portal.build_bonusmodell(
        [
            {
                "id": 999001,
                "status": 5,
                "fahrzeug": "Smoke Bonus",
                "kennzeichen": "MOS-B 1",
                "abholtermin_obj": date(2026, 5, 12),
                "abholtermin": "12.05.2026",
                "werkstatt_angebot_preis": "3.670,40 € netto",
                "bonus_netto_betrag": 0,
            }
        ],
        reference_date=date(2026, 5, 20),
    )
    bonus_ok = (
        bonus_beispiel["umsatz_netto_label"] == "3.670,40 €"
        and bonus_beispiel["bonus_satz_label"] == "2 %"
        and bonus_beispiel["bonus_netto_label"] == "73,41 €"
    )
    print(
        "[OK] Bonusmodell berechnet Monatsumsatz und 2-Prozent-Bonus"
        if bonus_ok
        else "[FEHLER] Bonusmodell berechnet das Beispiel nicht korrekt"
    )
    ok &= bonus_ok
    bonus_rechnungen = portal.build_bonusmodell(
        [
            {
                "id": 999101,
                "status": 2,
                "fahrzeug": "Smoke Rechnung 1",
                "kennzeichen": "MOS-R 1",
                "rechnung_status": "geschrieben",
                "rechnung_geschrieben_am": "11.05.2026 22:33",
                "bonus_netto_betrag": "580,00",
                "werkstatt_angebot_preis": "",
            },
            {
                "id": 999102,
                "status": 3,
                "fahrzeug": "Smoke Rechnung 2",
                "kennzeichen": "MOS-R 2",
                "rechnung_status": "geschrieben",
                "rechnung_geschrieben_am": "12.05.2026 09:15",
                "bonus_netto_betrag": "4.500,00",
                "werkstatt_angebot_preis": "",
            },
        ],
        reference_date=date(2026, 5, 20),
    )
    bonus_rechnungen_ok = (
        bonus_rechnungen["umsatz_netto_label"] == "5.080,00 €"
        and bonus_rechnungen["bonus_satz_label"] == "3 %"
        and bonus_rechnungen["bonus_netto_label"] == "152,40 €"
        and bonus_rechnungen["gezaehlte_auftraege_count"] == 2
        and bonus_rechnungen["auftraege"][0]["datum_label"] == "Rechnung vom"
    )
    print(
        "[OK] Bonusmodell addiert Rechnungen aus mehreren Aufträgen"
        if bonus_rechnungen_ok
        else "[FEHLER] Bonusmodell addiert Rechnungen aus mehreren Aufträgen nicht korrekt"
    )
    ok &= bonus_rechnungen_ok
    autohaus = portal.get_autohaus_by_slug("kaesmann")
    if autohaus:
        admin_pdf_response = client.get(f"/admin/autohaus/{autohaus['id']}/lackierauftrag-vorlage.pdf")
        ok &= check("Admin Lackierauftrag-PDF", admin_pdf_response, {200})
        is_pdf = admin_pdf_response.mimetype == "application/pdf"
        print("[OK] Admin Lackierauftrag ist PDF" if is_pdf else "[FEHLER] Admin Lackierauftrag ist kein PDF")
        ok &= is_pdf

        client = portal.app.test_client()
        ok &= check(
            "Käsmann-Dashboard ohne Partner-Login geschuetzt",
            client.get("/partner/kaesmann/dashboard"),
            {302},
        )
        client.get("/partner")
        wrong_login = client.post(
            "/partner",
            data=csrf_data(
                client,
                {
                    "portal_key": autohaus["portal_key"],
                    "zugangscode": "FALSCH",
                },
            ),
            follow_redirects=False,
        )
        ok &= check("Zentraler Partner-Login lehnt falschen Code ab", wrong_login, {200})
        right_login = client.post(
            "/partner",
            data=csrf_data(
                client,
                {
                    "portal_key": autohaus["portal_key"],
                    "zugangscode": autohaus["zugangscode"],
                },
            ),
            follow_redirects=False,
        )
        ok &= check("Zentraler Partner-Login akzeptiert richtigen Code", right_login, {302})
        target_ok = "/portal/" in (right_login.headers.get("Location") or "")
        print(
            "[OK] Zentraler Partner-Login leitet ins Portal"
            if target_ok
            else "[FEHLER] Zentraler Partner-Login leitet nicht ins Portal"
        )
        ok &= target_ok
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["partner_autohaus_id"] = autohaus["id"]
        partner_login_reload_response = client.get("/partner", follow_redirects=False)
        ok &= check("Partner-Login leitet eingeloggte Partner weiter", partner_login_reload_response, {302})
        partner_login_reload_ok = "/partner/kaesmann/dashboard" in (
            partner_login_reload_response.headers.get("Location") or ""
        )
        print(
            "[OK] Eingeloggte Partner bleiben beim Neuladen aus dem Login heraus"
            if partner_login_reload_ok
            else "[FEHLER] Eingeloggte Partner landen wieder auf der Login-Seite"
        )
        ok &= partner_login_reload_ok
        partner_dashboard_response = client.get("/partner/kaesmann/dashboard")
        ok &= check(
            "Käsmann-Dashboard mit Partner-Login",
            partner_dashboard_response,
            {200},
        )
        partner_dashboard_html = partner_dashboard_response.get_data(as_text=True)
        partner_start_reduced_ok = (
            "Fahrzeuge suchen, neue Aufträge anlegen und Hinweise im Blick behalten." in partner_dashboard_html
            and "Nächste Termine" not in partner_dashboard_html
            and "Schneller Tagesblick für Dispo und Annahme." not in partner_dashboard_html
            and "Angebotsanfragen" not in partner_dashboard_html
            and "Ihre Fahrzeuge" in partner_dashboard_html
            and "data-live-clock" in partner_dashboard_html
            and "data-live-date" in partner_dashboard_html
            and "/session/ping" in partner_dashboard_html
        )
        print(
            "[OK] Käsmann-Dashboard zeigt reduzierte Startseite"
            if partner_start_reduced_ok
            else "[FEHLER] Käsmann-Dashboard zeigt entfernte Startseiten-Blöcke noch an"
        )
        ok &= partner_start_reduced_ok
        partner_weather_ui_ok = (
            "data-weather-widget" in partner_dashboard_html
            and "Mosbach" in partner_dashboard_html
            and "/api/wetter/mosbach" in partner_dashboard_html
            and "Wetter in den nächsten 3 Stunden" in partner_dashboard_html
        )
        print(
            "[OK] Partner-Dashboard zeigt Mosbach-Wetter"
            if partner_weather_ui_ok
            else "[FEHLER] Partner-Dashboard zeigt den Mosbach-Wetterblock nicht korrekt"
        )
        ok &= partner_weather_ui_ok
        partner_bonus_link_ok = (
            "Bonusmodell" in partner_dashboard_html
            and "/partner/kaesmann/bonusmodell" in partner_dashboard_html
            and "Bonusmodell klar erklärt" not in partner_dashboard_html
        )
        print(
            "[OK] Partner-Dashboard zeigt Bonusmodell als eigene Rubrik"
            if partner_bonus_link_ok
            else "[FEHLER] Partner-Dashboard zeigt die Bonusmodell-Rubrik nicht korrekt"
        )
        ok &= partner_bonus_link_ok
        partner_bonus_response = client.get("/partner/kaesmann/bonusmodell")
        partner_bonus_html = partner_bonus_response.get_data(as_text=True)
        partner_bonus_page_ok = (
            partner_bonus_response.status_code == 200
            and "Bonusmodell klar erklärt" in partner_bonus_html
            and "Monatsumsatz netto" in partner_bonus_html
            and "Bonus netto" in partner_bonus_html
            and "Verrechnung auf Folgerechnung" in partner_bonus_html
        )
        print(
            "[OK] Bonusmodell-Rubrik zeigt aktuelles Bonusmodell"
            if partner_bonus_page_ok
            else "[FEHLER] Bonusmodell-Rubrik zeigt das Bonusmodell nicht"
        )
        ok &= partner_bonus_page_ok
        partner_search_ui_ok = (
            "data-portal-search-input" in partner_dashboard_html
            and "/partner/kaesmann/auftraege-suche" in partner_dashboard_html
        )
        print(
            "[OK] Partner-Dashboard zeigt Fahrzeug-Suche"
            if partner_search_ui_ok
            else "[FEHLER] Partner-Dashboard zeigt keine Fahrzeug-Suche"
        )
        ok &= partner_search_ui_ok
        with client.session_transaction() as session:
            session["admin"] = True
        mixed_session_dashboard_response = client.get("/partner/kaesmann/dashboard")
        mixed_session_dashboard_html = mixed_session_dashboard_response.get_data(as_text=True)
        partner_ki_endpoint_ok = (
            mixed_session_dashboard_response.status_code == 200
            and 'data-chat-url="/partner/kaesmann/ki/chat"' in mixed_session_dashboard_html
            and 'data-clear-url="/partner/kaesmann/ki/chat/loeschen"' in mixed_session_dashboard_html
            and 'data-chat-url="/admin/ki/chat"' not in mixed_session_dashboard_html
        )
        print(
            "[OK] Partner-Dashboard nutzt Partner-KI-Endpunkte trotz Admin-Session"
            if partner_ki_endpoint_ok
            else "[FEHLER] Partner-Dashboard nutzt falsche KI-Endpunkte"
        )
        ok &= partner_ki_endpoint_ok
        pfaff = portal.get_autohaus_by_slug("autohaus-pfaff")
        if pfaff:
            pfaff_client = portal.app.test_client()
            with pfaff_client.session_transaction() as session:
                session["partner_autohaus_id"] = pfaff["id"]
            pfaff_dashboard_html = pfaff_client.get("/partner/autohaus-pfaff/dashboard").get_data(as_text=True)
            pfaff_search_response = pfaff_client.get("/partner/autohaus-pfaff/auftraege-suche?q=MOS-K")
            ok &= check("Partner-Auftragssuche nach Kennzeichen", pfaff_search_response, {200})
            pfaff_search_items = (pfaff_search_response.get_json(silent=True) or {}).get("items") or []
            pfaff_search_ok = any(
                "MOS-K" in item.get("kennzeichen", "") and "/partner/autohaus-pfaff/auftrag/" in item.get("url", "")
                for item in pfaff_search_items
            )
            print(
                "[OK] Partner-Auftragssuche liefert klickbaren Auftrag"
                if pfaff_search_ok
                else "[FEHLER] Partner-Auftragssuche liefert keinen klickbaren Auftrag"
            )
            ok &= pfaff_search_ok
            pfaff_reduced_start_ok = (
                "Nächste Termine" not in pfaff_dashboard_html
                and "Schneller Tagesblick für Dispo und Annahme." not in pfaff_dashboard_html
                and "Angebotsanfragen" not in pfaff_dashboard_html
                and "Aktuell:" not in pfaff_dashboard_html
                and "Ihre Fahrzeuge" in pfaff_dashboard_html
                and "data-live-clock" in pfaff_dashboard_html
                and "/session/ping" in pfaff_dashboard_html
            )
            print(
                "[OK] Partner-Dashboard blendet Termin- und Angebotsblöcke aus"
                if pfaff_reduced_start_ok
                else "[FEHLER] Partner-Dashboard zeigt Termin- oder Angebotsblöcke noch an"
            )
            ok &= pfaff_reduced_start_ok
        ok &= check(
            "Käsmann-Postfach mit Partner-Login",
            client.get("/partner/kaesmann/postfach"),
            {200},
        )
        partner_pdf_response = client.get("/partner/kaesmann/lackierauftrag-vorlage.pdf")
        ok &= check("Käsmann Lackierauftrag-PDF", partner_pdf_response, {200})
        is_pdf = partner_pdf_response.mimetype == "application/pdf"
        print("[OK] Käsmann Lackierauftrag ist PDF" if is_pdf else "[FEHLER] Käsmann Lackierauftrag ist kein PDF")
        ok &= is_pdf

        upload_client = portal.app.test_client()
        with upload_client.session_transaction() as session:
            session["partner_autohaus_id"] = autohaus["id"]
        upload_client.get("/partner/kaesmann/neu")
        upload_response = upload_client.post(
            "/partner/kaesmann/neu",
            data=csrf_data(
                upload_client,
                {
                    "aktion": "upload_analyze",
                    "kunde_name": "Smoke Test Kunde",
                    "kontakt_telefon": "01234 567890",
                    "fahrzeug": "",
                    "kennzeichen": "",
                    "analyse_text": "",
                    "beschreibung": "",
                    "transport_art": "standard",
                    "dateien": (
                        BytesIO(
                            b"Lackierauftrag Smoke Test\n"
                            b"Fahrzeug: Audi A4\n"
                            b"Kennzeichen: MOS ST 42\n"
                            b"Auftrag: SMOKE-PORTAL-42\n"
                        ),
                        "smoke-kunden-upload.txt",
                    ),
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        ok &= check("Kundenportal Upload leitet zum Auftrag", upload_response, {302})
        auftrag_id = None
        location = upload_response.headers.get("Location", "")
        marker = "/auftrag/"
        if marker in location:
            try:
                auftrag_id = int(location.rsplit(marker, 1)[1].split("?", 1)[0].strip("/"))
            except ValueError:
                auftrag_id = None
        upload_created = auftrag_id is not None and portal.get_auftrag(auftrag_id)
        print(
            "[OK] Kundenportal Upload erstellt Auftrag"
            if upload_created
            else "[FEHLER] Kundenportal Upload erstellt keinen pruefbaren Auftrag"
        )
        ok &= bool(upload_created)
        if auftrag_id:
            try:
                dateien = portal.list_dateien(auftrag_id)
                upload_saved = bool(
                    len(dateien) == 1
                    and dateien[0]["quelle"] == "autohaus"
                    and dateien[0]["original_name"] == "smoke-kunden-upload.txt"
                    and dateien[0]["extrahierter_text"]
                )
                print(
                    "[OK] Kundendatei gespeichert und analysiert"
                    if upload_saved
                    else "[FEHLER] Kundendatei wurde nicht korrekt gespeichert/analysiert"
                )
                ok &= upload_saved
                detail_response = upload_client.get(f"/partner/kaesmann/auftrag/{auftrag_id}")
                detail_html = detail_response.get_data(as_text=True)
                quick_upload_ui_ok = (
                    detail_response.status_code == 200
                    and "Schnell-Upload" in detail_html
                    and 'value="quick_upload_analyze"' in detail_html
                )
                print(
                    "[OK] Partner-Auftrag zeigt Schnell-Upload oben"
                    if quick_upload_ui_ok
                    else "[FEHLER] Partner-Auftrag zeigt keinen Schnell-Upload oben"
                )
                ok &= quick_upload_ui_ok
                quick_upload_response = upload_client.post(
                    f"/partner/kaesmann/auftrag/{auftrag_id}",
                    data=csrf_data(
                        upload_client,
                        {
                            "aktion": "quick_upload_analyze",
                            "upload_notiz": "Schnellupload Smoke Hinweis",
                            "dateien": (
                                BytesIO(b"Schnellupload Smoke Test\nSchadenfoto Hinweis links\n"),
                                "smoke-schnell-upload.txt",
                            ),
                        },
                    ),
                    content_type="multipart/form-data",
                    follow_redirects=False,
                )
                ok &= check("Partner Schnell-Upload leitet zurück", quick_upload_response, {302})
                quick_dateien = portal.list_dateien(auftrag_id)
                quick_upload_saved = any(
                    datei["original_name"] == "smoke-schnell-upload.txt"
                    and datei["quelle"] == "autohaus"
                    and datei["notiz"] == "Schnellupload Smoke Hinweis"
                    and datei["extrahierter_text"]
                    for datei in quick_dateien
                )
                print(
                    "[OK] Partner Schnell-Upload speichert und analysiert Datei"
                    if quick_upload_saved
                    else "[FEHLER] Partner Schnell-Upload speichert/analysiert Datei nicht"
                )
                ok &= quick_upload_saved
                auftrag_after_quick_upload = portal.get_auftrag(auftrag_id)
                quick_upload_preserved_fields = bool(
                    auftrag_after_quick_upload
                    and auftrag_after_quick_upload["kunde_name"] == "Smoke Test Kunde"
                    and auftrag_after_quick_upload["fahrzeug"]
                )
                print(
                    "[OK] Partner Schnell-Upload überschreibt keine Auftragsdaten"
                    if quick_upload_preserved_fields
                    else "[FEHLER] Partner Schnell-Upload überschreibt Auftragsdaten"
                )
                ok &= quick_upload_preserved_fields
                admin_upload_client = portal.app.test_client()
                with admin_upload_client.session_transaction() as session:
                    session["admin"] = True
                admin_detail = admin_upload_client.get(f"/admin/auftrag/{auftrag_id}")
                ok &= check("Admin sieht Kundenauftrag", admin_detail, {200})
                admin_html = admin_detail.get_data(as_text=True)
                admin_shows_file = (
                    "smoke-kunden-upload.txt" in admin_html
                    and "Autohaus/Kundenportal" in admin_html
                )
                print(
                    "[OK] Admin sieht gespeicherte Kundendatei mit Herkunft"
                    if admin_shows_file
                    else "[FEHLER] Admin sieht Kundendatei/Herkunft nicht"
                )
                ok &= admin_shows_file
                if dateien:
                    ok &= check("Admin Originaldatei oeffnet", admin_upload_client.get(f"/admin/datei/{dateien[0]['id']}"), {200})
                    ok &= check(
                        "Admin Originaldatei Download",
                        admin_upload_client.get(f"/admin/datei/{dateien[0]['id']}/download"),
                        {200},
                    )
            finally:
                portal.delete_auftrag(auftrag_id)
    else:
        print("[INFO] Autohaus 'kaesmann' existiert lokal nicht, Partner-Dashboard-Test uebersprungen.")

    if not ok:
        raise SystemExit(1)
    print("Smoke-Test erfolgreich.")


if __name__ == "__main__":
    main()
