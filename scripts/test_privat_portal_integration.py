# -*- coding: utf-8 -*-
"""Isolierter Integrationstest fuer Privatkunden-Portal und Flyer-QR-Ziele."""

from __future__ import annotations

import base64
import io
import json
import os
import pathlib
import sys
import tempfile
from datetime import date

from werkzeug.datastructures import FileStorage


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="privat_portal_integration_"))
os.environ.update(
    {
        "RENDER": "local-privat-portal-test",
        "DATABASE_URL": "",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEMP_DIR),
        "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
        "UPLOAD_DIR": str(TEMP_DIR / "uploads"),
        "BACKUP_DIR": str(TEMP_DIR / "backups"),
        "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "LEXWARE_API_KEY": "",
        "OPENAI_API_KEY": "",
        "WHATSAPP_ACCESS_TOKEN": "",
        "WHATSAPP_PHONE_NUMBER_ID": "",
        "SCHADEN_VORSCHAU_AKTIV": "0",
        "FLASK_SECRET_KEY": "privat-portal-integration-test",
        "ADMIN_PASS": "privat-portal-integration-test",
    }
)

import app as portal  # noqa: E402


PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


def check(label, condition, detail=""):
    if not condition:
        raise AssertionError(f"{label}: {detail}")
    print(f"[OK] {label}" + (f" - {detail}" if detail else ""))


def scalar(sql, params=()):
    db = portal.get_db()
    try:
        row = db.execute(sql, params).fetchone()
        return row[0] if row else None
    finally:
        db.close()


def row(sql, params=()):
    db = portal.get_db()
    try:
        item = db.execute(sql, params).fetchone()
        return dict(item) if item else None
    finally:
        db.close()


def session_tokens(client):
    keys = portal.schadenaufnahme_session_keys(public_mode=True)
    with client.session_transaction() as session:
        return session.get(portal.CSRF_FIELD_NAME), session.get(keys["form"])


def base_payload(csrf_token, form_token, **overrides):
    payload = {
        portal.CSRF_FIELD_NAME: csrf_token,
        "schadenaufnahme_form_token": form_token,
        "schadenart": "teilkasko",
        "kunde_name": "Petra Privatportal",
        "kunde_strasse": "Musterstrasse 12",
        "kunde_plz": "74821",
        "kunde_ort": "Mosbach",
        "kontakt_telefon": "0171 2345678",
        "kunde_email": "petra@example.test",
        "kontaktweg": "whatsapp",
        "kennzeichen": "MOS PP 42",
        "fahrzeug": "VW Golf Privat",
        "fin_nummer": "WVWZZZ1KZ6W000001",
        "unfall_datum": date.today().isoformat(),
        "unfall_zeit": "11:20",
        "unfall_ort": "Mosbach Parkplatz",
        "beschreibung": "Ein Steinschlag befindet sich im Sichtbereich der Frontscheibe.",
        "versicherung_name": "Allianz",
        "versicherung_police": "POL-PRIVAT-42",
        "schaden_nummer": "SCH-PRIVAT-42",
        "mobilitaet": "ja",
        "datenschutz_bestaetigt": "1",
    }
    payload.update(overrides)
    return payload


def main():
    portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
    portal.init_db()
    portal.PUBLIC_FORM_ATTEMPTS.clear()
    client = portal.app.test_client()

    home = client.get("/privat")
    home_html = home.get_data(as_text=True)
    check("Privatkunden-Portal ist oeffentlich", home.status_code == 200)
    check(
        "Portal verlinkt alle drei sicheren Wege",
        'href="/privat/schaden"' in home_html
        and 'href="/mietwagen"' in home_html
        and 'href="/privat/status"' in home_html,
    )
    check("Portal nennt Automechanik", "AUTOMECHANIK" in home_html)

    form_response = client.get("/privat/schaden")
    form_html = form_response.get_data(as_text=True)
    check("Oeffentliche Schadenaufnahme laedt", form_response.status_code == 200)
    check("Schadenformular wird nicht gecacht", "no-store" in form_response.headers.get("Cache-Control", ""))
    check(
        "Privatmodus und echte POST-Anbindung vorhanden",
        'data-portal-mode="privat"' in form_html
        and 'method="POST"' in form_html
        and 'action="/privat/schaden"' in form_html,
    )
    check(
        "Anschrift ist fuer Privatkunden Pflicht",
        'name="kunde_strasse"' in form_html
        and 'name="kunde_plz"' in form_html
        and 'name="kunde_ort"' in form_html,
    )
    csrf_token, form_token = session_tokens(client)
    check("CSRF und privater Einmal-Token gesetzt", bool(csrf_token and form_token))

    count_before = int(scalar("SELECT COUNT(*) FROM auftraege") or 0)
    invalid = client.post(
        "/privat/schaden",
        data=base_payload(csrf_token, form_token, kunde_strasse="", kunde_plz="", kunde_ort=""),
    )
    check("Fehlende Anschrift wird serverseitig abgelehnt", invalid.status_code == 400)
    check("Ungueltige Meldung erzeugt keinen Fall", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == count_before)

    client.get("/privat/schaden")
    csrf_token, form_token = session_tokens(client)
    bot = client.post(
        "/privat/schaden",
        data=base_payload(csrf_token, form_token, website="spam.example"),
        follow_redirects=False,
    )
    check("Honeypot wird neutral umgeleitet", bot.status_code == 302 and bot.headers.get("Location", "").endswith("/privat/"))
    check("Honeypot erzeugt keinen Fall", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == count_before)

    client.get("/privat/schaden")
    csrf_token, form_token = session_tokens(client)
    analysis_calls = []
    original_analysis = portal.build_document_analysis_bundle_safe

    def forbidden_public_analysis(path, filename=""):
        analysis_calls.append(filename)
        raise AssertionError("Oeffentliche Kunden-Datei wurde synchron analysiert")

    portal.build_document_analysis_bundle_safe = forbidden_public_analysis
    payload = base_payload(csrf_token, form_token)
    payload["dateien"] = (io.BytesIO(PNG_BYTES), "steinschlag.png", "text/html")
    created = client.post(
        "/privat/schaden",
        data=payload,
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    check("Gueltige Privatmeldung nutzt PRG", created.status_code == 302 and "erstellt=" in created.headers.get("Location", ""))
    auftrag_id = int(scalar("SELECT MAX(id) FROM auftraege") or 0)
    auftrag = portal.get_auftrag(auftrag_id)
    check("Privatfall wurde angelegt", auftrag_id > 0 and auftrag["quelle"] == "privat")
    check("Kein Autohaus kann ueber Browserdaten gesetzt werden", not int(auftrag.get("autohaus_id") or 0))
    check("Persoenlicher Status-Token ist stark", len(auftrag.get("kunden_status_token") or "") >= 18)
    check(
        "Status bleibt bis zur persoenlichen Kontaktpruefung inaktiv",
        not auftrag.get("kunden_status_aktiv")
        and not auftrag.get("kunden_status_url")
        and not portal.customer_status_share(auftrag)["can_whatsapp"],
    )

    intake = json.loads(auftrag.get("schaden_aufnahme_json") or "{}")
    check(
        "Anschrift und Privatquelle sind revisionsfest gespeichert",
        intake.get("kunde_strasse") == "Musterstrasse 12"
        and intake.get("kunde_plz") == "74821"
        and intake.get("kunde_ort") == "Mosbach"
        and intake.get("erfasst_von") == "privatkunden-portal",
    )
    check(
        "WhatsApp bleibt bis zur persoenlichen Rufnummernpruefung gesperrt",
        intake.get("whatsapp_kontakt_gewuenscht") is True
        and intake.get("whatsapp_einwilligung") is False
        and intake.get("whatsapp_verifizierung_status") == "ausstehend"
        and not intake.get("whatsapp_einwilligung_am")
        and not portal.customer_status_share(auftrag)["can_whatsapp"],
    )

    versicherung = portal.get_versicherung(auftrag.get("versicherung_id"))
    check("Freie Versicherungsangabe bleibt beim internen Platzhalter", portal.versicherung_ist_platzhalter(versicherung))
    allianz = next((item for item in portal.list_versicherungen() if "allianz" in item["name"].lower() and not portal.versicherung_ist_platzhalter(item)), None)
    check(
        "Privatfall ist fuer echte Versicherung nicht sichtbar",
        bool(allianz) and not portal.versicherung_auftrag_im_portal_sichtbar(auftrag, allianz["id"]),
    )
    upload = row("SELECT * FROM dateien WHERE auftrag_id=? ORDER BY id DESC LIMIT 1", (auftrag_id,))
    check(
        "Upload ist serverseitig typisiert und dem Kunden zugeordnet",
        upload
        and upload["mime_type"] == "image/png"
        and upload["quelle"] == "kunde"
        and not upload["analyse_quelle"],
    )
    check("Oeffentlicher Upload loest keine synchrone Analyse aus", not analysis_calls)
    check(
        "Pruefaufgaben wurden erzeugt",
        int(scalar("SELECT COUNT(*) FROM versicherung_aufgaben WHERE auftrag_id=?", (auftrag_id,)) or 0) >= 3,
    )

    success = client.get(created.headers["Location"])
    success_html = success.get_data(as_text=True)
    check("Sessiongebundene Erfolgssicht laedt", success.status_code == 200 and "Ihre Schadenmeldung ist angekommen" in success_html)
    check(
        "Erfolgssicht behaelt Bearer-Statuslink bis zur Kontaktpruefung zurueck",
        f'/status/{auftrag["kunden_status_token"]}' not in success_html
        and "Danach senden wir Ihnen Ihren geschützten Statuszugang" in success_html
        and 'href="/mietwagen"' in success_html
        and "/admin/" not in success_html,
    )

    stranger = portal.app.test_client()
    stranger_success = stranger.get(f"/privat/schaden?erstellt={auftrag_id}")
    stranger_html = stranger_success.get_data(as_text=True)
    check(
        "Fremde Session sieht keine gespeicherten Falldaten",
        "Petra Privatportal" not in stranger_html and "const hasCreatedCase = false" in stranger_html,
    )

    alter_token = auftrag["kunden_status_token"]
    inactive_status = client.get(f"/status/{alter_token}")
    check(
        "Inaktiver Bearer-Token gibt vor Kontaktpruefung keine Akte preis",
        inactive_status.status_code == 404
        and "no-store" in inactive_status.headers.get("Cache-Control", "")
        and "noindex" in inactive_status.headers.get("X-Robots-Tag", ""),
    )

    admin = portal.app.test_client()
    with admin.session_transaction() as admin_session:
        admin_session["admin"] = True
    admin_page = admin.get(f"/admin/versicherung/schaden/{auftrag_id}")
    admin_html = admin_page.get_data(as_text=True)
    admin_order_page = admin.get(f"/admin/auftrag/{auftrag_id}")
    admin_order_html = admin_order_page.get_data(as_text=True)
    check(
        "Werkstatt sieht in beiden Fallakten Kontaktpruefung statt Bearer-Link",
        admin_page.status_code == 200
        and admin_order_page.status_code == 200
        and "Kontakt bestätigen" in admin_html
        and "Kontakt bestätigen" in admin_order_html
        and 'name="whatsapp_einwilligung"' in admin_html
        and alter_token not in admin_html
        and alter_token not in admin_order_html,
    )
    with admin.session_transaction() as admin_session:
        admin_csrf = admin_session.get(portal.CSRF_FIELD_NAME)

    csrf_blocked = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenstatus-freigeben",
        data={"ansicht": "versicherung", "kontakt_geprueft": "1"},
    )
    check("Adminfreigabe ist CSRF-geschuetzt", csrf_blocked.status_code == 400)
    check(
        "CSRF-Fehler aktiviert keinen Status",
        not portal.get_auftrag(auftrag_id)["kunden_status_aktiv"],
    )

    unchecked = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenstatus-freigeben",
        data={portal.CSRF_FIELD_NAME: admin_csrf, "ansicht": "versicherung"},
        follow_redirects=False,
    )
    check("Kontaktbestaetigung ist serverseitig Pflicht", unchecked.status_code == 302)
    check(
        "Fehlende Kontaktbestaetigung aktiviert keinen Status",
        not portal.get_auftrag(auftrag_id)["kunden_status_aktiv"],
    )

    activated = admin.post(
        f"/admin/auftrag/{auftrag_id}/kundenstatus-freigeben",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "ansicht": "versicherung",
            "kontakt_geprueft": "1",
            "whatsapp_einwilligung": "1",
        },
        follow_redirects=False,
    )
    auftrag = portal.get_auftrag(auftrag_id)
    token = auftrag["kunden_status_token"]
    freigabe_intake = json.loads(auftrag.get("schaden_aufnahme_json") or "{}")
    check(
        "Persoenliche Pruefung aktiviert und rotiert den Status-Token",
        activated.status_code == 302
        and auftrag["kunden_status_aktiv"]
        and token != alter_token
        and len(token) >= 18,
    )
    check(
        "Verifizierte WhatsApp-Einwilligung ist zeit- und nummerngebunden",
        bool(freigabe_intake.get("kontakt_verifiziert_am"))
        and freigabe_intake.get("kontakt_verifiziert_quelle") == "werkstatt-kontaktpruefung"
        and freigabe_intake.get("whatsapp_einwilligung") is True
        and freigabe_intake.get("whatsapp_verifizierung_status") == "bestaetigt"
        and bool(freigabe_intake.get("whatsapp_einwilligung_am"))
        and freigabe_intake.get("whatsapp_einwilligung_telefon_key")
        == portal.whatsapp_number_key(auftrag["kontakt_telefon"])
        and portal.customer_status_share(auftrag)["can_whatsapp"],
    )
    check(
        "Rotierter alter Token bleibt unbrauchbar",
        client.get(f"/status/{alter_token}").status_code == 404,
    )

    status = client.get(f"/status/{token}")
    check("Freigegebener Kundenstatus laedt", status.status_code == 200 and "Petra Privatportal" in status.get_data(as_text=True))
    check(
        "Kundenstatus ist no-store und noindex",
        "no-store" in status.headers.get("Cache-Control", "")
        and "noindex" in status.headers.get("X-Robots-Tag", ""),
    )
    qr = client.get(f"/status/{token}/qr.svg?target=local")
    check(
        "Individueller Status-QR ist no-store und noindex",
        qr.status_code == 200
        and b"<svg" in qr.data
        and "no-store" in qr.headers.get("Cache-Control", "")
        and "noindex" in qr.headers.get("X-Robots-Tag", ""),
    )

    db = portal.get_db()
    try:
        db.execute("UPDATE dateien SET kunde_sichtbar=1 WHERE id=?", (upload["id"],))
        db.commit()
    finally:
        db.close()
    customer_image = client.get(f"/status/{token}/bild/{upload['id']}")
    check(
        "Kundenbild ist ebenfalls no-store und noindex",
        customer_image.status_code == 200
        and "no-store" in customer_image.headers.get("Cache-Control", "")
        and "noindex" in customer_image.headers.get("X-Robots-Tag", ""),
    )

    with client.session_transaction() as customer_session:
        customer_csrf = customer_session.get(portal.CSRF_FIELD_NAME)
    followup_upload = client.post(
        f"/status/{token}/unterlagen",
        data={
            portal.CSRF_FIELD_NAME: customer_csrf,
            "upload_notiz": "Zweites Foto",
            "dateien": (io.BytesIO(PNG_BYTES), "nachgereicht.png", "image/png"),
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    customer_files = int(
        scalar("SELECT COUNT(*) FROM dateien WHERE auftrag_id=? AND quelle='kunde'", (auftrag_id,))
        or 0
    )
    check(
        "Kundenstatus-Upload wird gespeichert, aber niemals synchron analysiert",
        followup_upload.status_code == 302
        and customer_files == 2
        and not analysis_calls
        and not scalar(
            "SELECT COUNT(*) FROM dateien WHERE auftrag_id=? AND quelle='kunde' AND COALESCE(analyse_quelle, '')!=''",
            (auftrag_id,),
        ),
    )
    reanalyzed_customer, _ = portal.reanalyze_existing_documents(auftrag_id)
    check(
        "Sammel-Reanalyse ueberspringt ungepruefte Kunden-Dateien",
        reanalyzed_customer == 0 and not analysis_calls,
    )

    trusted_file = FileStorage(
        stream=io.BytesIO(PNG_BYTES),
        filename="werkstatt-geprueft.png",
        content_type="image/png",
    )
    saved_trusted, _ = portal.save_uploads(
        auftrag_id,
        [trusted_file],
        "intern",
        "standard",
        analyze=False,
    )
    trusted_analysis_calls = []

    def fake_trusted_analysis(path, filename=""):
        trusted_analysis_calls.append(filename)
        return {
            "text": "Werkstatt gepruefte Unterlage",
            "source": "integration_test",
            "status": "ready",
            "hint": "",
            "structured": {},
            "analysis_json": "",
        }

    portal.build_document_analysis_bundle_safe = fake_trusted_analysis
    reanalyzed_trusted, _ = portal.reanalyze_existing_documents(auftrag_id)
    portal.build_document_analysis_bundle_safe = original_analysis
    check(
        "Interne Werkstatt-Datei bleibt ueber bestehenden Flow analysierbar",
        saved_trusted == 1
        and reanalyzed_trusted == 1
        and trusted_analysis_calls == ["werkstatt-geprueft.png"],
    )

    status_entry = client.get("/privat/status")
    check(
        "Allgemeiner Statuseinstieg zeigt keine Akte",
        status_entry.status_code == 200
        and "Persönlicher Zugangscode" in status_entry.get_data(as_text=True)
        and "noindex" in status_entry.headers.get("X-Robots-Tag", ""),
    )
    with client.session_transaction() as session:
        csrf_token = session.get(portal.CSRF_FIELD_NAME)
    wrong = client.post(
        "/privat/status",
        data={portal.CSRF_FIELD_NAME: csrf_token, "zugangscode": "UNGUELTIGER-CODE"},
    )
    check("Falscher Statuscode gibt keine Auftragsdaten preis", wrong.status_code == 200 and "Petra Privatportal" not in wrong.get_data(as_text=True))
    old_link = client.post(
        "/privat/status",
        data={portal.CSRF_FIELD_NAME: csrf_token, "zugangscode": f"https://portal.example/status/{alter_token}"},
        follow_redirects=False,
    )
    check("Alter Link bleibt auch am Statuseinstieg ungueltig", old_link.status_code == 200)
    valid = client.post(
        "/privat/status",
        data={portal.CSRF_FIELD_NAME: csrf_token, "zugangscode": f"https://portal.example/status/{token}"},
        follow_redirects=False,
    )
    check("Persoenlicher Link wird sicher aufgeloest", valid.status_code == 302 and valid.headers.get("Location", "").endswith(f"/status/{token}"))

    db = portal.get_db()
    try:
        db.execute("UPDATE auftraege SET kontakt_telefon='0179 9999999' WHERE id=?", (auftrag_id,))
        db.commit()
    finally:
        db.close()
    check(
        "Aenderung der Mobilnummer entzieht WhatsApp-Freigabe",
        not portal.customer_status_share(portal.get_auftrag(auftrag_id))["can_whatsapp"],
    )

    duplicate = client.post(
        "/privat/schaden",
        data=base_payload(csrf_token, form_token),
    )
    check("Einmal-Token verhindert Doppelanlage", duplicate.status_code == 400)
    check("Kein doppelter Privatfall", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == count_before + 1)

    print(f"Privatkunden-Portal Integrationstest erfolgreich ({TEMP_DIR})")


if __name__ == "__main__":
    main()
