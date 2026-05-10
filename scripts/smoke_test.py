from pathlib import Path
from io import BytesIO
from datetime import date
import sys


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
    ok = True

    login_response = client.get("/login")
    ok &= check("Login-Seite", login_response, {200})
    has_csrf = "name=\"csrf_token\"" in login_response.get_data(as_text=True)
    print("[OK] Login-Formular enthält CSRF-Token" if has_csrf else "[FEHLER] Login-Formular ohne CSRF-Token")
    ok &= has_csrf
    mini_calendar = portal.build_mini_monatskalender(
        [
            {
                "annahme_datum_obj": date.today(),
                "abholtermin_obj": date.today(),
                "autohaus_name": "Smoke Autohaus",
                "kunde_name": "Smoke Kunde",
                "fahrzeug": "Audi A4",
                "kennzeichen": "MOS ST 42",
            }
        ],
        date.today().strftime("%Y-%m"),
        include_timeline=True,
    )
    today_text = date.today().strftime(portal.DATE_FMT)
    today_calendar_day = next(
        day
        for week in mini_calendar["weeks"]
        for day in week
        if day["datum_text"] == today_text
    )
    calendar_names_ok = (
        today_calendar_day["events"]
        and today_calendar_day["events"][0]["party_name"] == "Smoke Autohaus"
    )
    print(
        "[OK] Monatskalender liefert Autohausnamen pro Tag"
        if calendar_names_ok
        else "[FEHLER] Monatskalender liefert keinen Autohausnamen pro Tag"
    )
    ok &= calendar_names_ok
    timeline_ok = (
        mini_calendar["timeline_rows"]
        and mini_calendar["timeline_rows"][0]["party_name"] == "Smoke Autohaus"
        and mini_calendar["timeline_rows"][0]["start_col"] < mini_calendar["timeline_rows"][0]["end_col"]
    )
    print(
        "[OK] Monatskalender liefert Von-bis-Zeitstrahl"
        if timeline_ok
        else "[FEHLER] Monatskalender liefert keinen Von-bis-Zeitstrahl"
    )
    ok &= timeline_ok
    ok &= check(
        "Login-POST ohne CSRF blockiert",
        client.post("/login", data={"passwort": "falsch"}),
        {400},
    )
    ok &= check(
        "Login-POST mit CSRF verarbeitet",
        client.post("/login", data=csrf_data(client, {"passwort": "falsch"})),
        {200},
    )
    ok &= check("Admin ohne Login geschuetzt", client.get("/admin"), {302})
    partner_index_response = client.get("/partner")
    ok &= check("Partner-Einstieg", partner_index_response, {200})
    partner_index_html = partner_index_response.get_data(as_text=True)
    central_partner_login = "name=\"portal_key\"" in partner_index_html and "name=\"zugangscode\"" in partner_index_html
    print(
        "[OK] Zentraler Partner-Login vorhanden"
        if central_partner_login
        else "[FEHLER] Zentraler Partner-Login fehlt"
    )
    ok &= central_partner_login
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
    admin_html = admin_response.get_data(as_text=True)
    admin_calendar_ok = (
        "Werkstatt-Kalender" in admin_html
        and "mini-calendar-large" in admin_html
        and "Auftragsplan von bis" in admin_html
    )
    print(
        "[OK] Admin-Dashboard zeigt grossen Kalender"
        if admin_calendar_ok
        else "[FEHLER] Admin-Dashboard zeigt keinen grossen Kalender"
    )
    ok &= admin_calendar_ok
    external_client = portal.app.test_client()
    with external_client.session_transaction(base_url="https://werkstatt.example.test") as session:
        session["admin"] = True
    external_admin = external_client.get(
        "/admin/zugaenge",
        base_url="https://werkstatt.example.test",
        headers={
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "werkstatt.example.test",
        },
    )
    ok &= check("Admin-Zugaenge mit oeffentlichem Host", external_admin, {200})
    external_html = external_admin.get_data(as_text=True)
    expected_public_base_url = portal.PUBLIC_BASE_URL or "https://werkstatt.example.test"
    external_link_ok = f"{expected_public_base_url}/portal/" in external_html
    print(
        "[OK] Kundenlinks nutzen oeffentliche Basisadresse"
        if external_link_ok
        else "[FEHLER] Kundenlinks nutzen nicht die oeffentliche Basisadresse"
    )
    ok &= external_link_ok

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
        partner_dashboard_response = client.get("/partner/kaesmann/dashboard")
        ok &= check(
            "Käsmann-Dashboard mit Partner-Login",
            partner_dashboard_response,
            {200},
        )
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
