from pathlib import Path
from io import BytesIO
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
    ok &= check("Partner-Einstieg", client.get("/partner"), {200})
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
    ok &= check("Admin mit Login", client.get("/admin"), {200})
    external_client = portal.app.test_client()
    with external_client.session_transaction(base_url="https://werkstatt.example.test") as session:
        session["admin"] = True
    external_admin = external_client.get(
        "/admin",
        base_url="https://werkstatt.example.test",
        headers={
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "werkstatt.example.test",
        },
    )
    ok &= check("Admin mit oeffentlichem Host", external_admin, {200})
    external_html = external_admin.get_data(as_text=True)
    external_link_ok = "https://werkstatt.example.test/portal/" in external_html
    print(
        "[OK] Kundenlinks nutzen aktuellen oeffentlichen Host"
        if external_link_ok
        else "[FEHLER] Kundenlinks nutzen nicht den aktuellen oeffentlichen Host"
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
        with client.session_transaction() as session:
            session["partner_autohaus_id"] = autohaus["id"]
        ok &= check(
            "Käsmann-Dashboard mit Partner-Login",
            client.get("/partner/kaesmann/dashboard"),
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
