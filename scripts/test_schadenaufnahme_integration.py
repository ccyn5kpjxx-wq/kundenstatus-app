# -*- coding: utf-8 -*-
"""Isolierter End-to-End-Test fuer die verbundene Schadenaufnahme."""

from __future__ import annotations

import base64
import io
import json
import os
import pathlib
import sys
import tempfile
from datetime import date, timedelta


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="schadenaufnahme_integration_"))
os.environ.update(
    {
        "RENDER": "local-schadenaufnahme-test",
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
        "FLASK_SECRET_KEY": "schadenaufnahme-integration-test",
        "ADMIN_PASS": "schadenaufnahme-integration-test",
    }
)

import app as portal  # noqa: E402


PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)
PDF_BYTES = b"%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\ntrailer<</Root 1 0 R>>\n%%EOF\n"


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


def rows(sql, params=()):
    db = portal.get_db()
    try:
        return [dict(row) for row in db.execute(sql, params).fetchall()]
    finally:
        db.close()


def session_tokens(client):
    with client.session_transaction() as session:
        return session.get(portal.CSRF_FIELD_NAME), session.get("schadenaufnahme_form_token")


def base_payload(csrf_token, form_token, **overrides):
    payload = {
        portal.CSRF_FIELD_NAME: csrf_token,
        "schadenaufnahme_form_token": form_token,
        "schadenart": "kasko",
        "kunde_name": "Anna Integration",
        "kontakt_telefon": "0171 2345678",
        "kunde_email": "anna@example.test",
        "kontaktweg": "email",
        "kennzeichen": "MOS IT 42",
        "fahrzeug": "VW Golf Manuell",
        "fin_nummer": "WVWZZZ1KZ6W000001",
        "unfall_datum": date.today().isoformat(),
        "unfall_zeit": "09:40",
        "unfall_ort": "Mosbach Bahnhof",
        "beschreibung": "Beim Ausparken wurde die linke hintere Seite beschaedigt.",
        "versicherung_name": "Allianz",
        "versicherung_police": "POL-INT-42",
        "schaden_nummer": "SCH-INT-42",
        "mobilitaet": "ja",
        "datenschutz_bestaetigt": "1",
    }
    payload.update(overrides)
    return payload


def fake_analysis(_path, original_name):
    structured = {
        "fahrzeug": "Ferrari OCR",
        "kennzeichen": "OCR X 999",
        "fin_nummer": "WVWZZZ1KZ6WOCR999",
        "analyse_text": "OCR darf nur Vorschlag bleiben",
    }
    return {
        "status": "ok",
        "text": f"OCR-Test aus {original_name}",
        "source": "integration_test",
        "analysis_json": json.dumps(structured),
        "structured": structured,
        "hint": "",
    }


def main():
    portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
    portal.init_db()
    portal.PUBLIC_FORM_ATTEMPTS.clear()
    client = portal.app.test_client()

    count_before = int(scalar("SELECT COUNT(*) FROM auftraege") or 0)
    check("GET ohne Admin ist geschuetzt", client.get("/admin/versicherungsschaden").status_code == 302)
    check(
        "POST ohne Admin ist geschuetzt",
        client.post("/admin/versicherungsschaden", data={}).status_code == 302,
    )
    check("Ohne Admin keine Fallanlage", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == count_before)

    with client.session_transaction() as session:
        session["admin"] = True
    form_response = client.get("/admin/versicherungsschaden")
    html = form_response.get_data(as_text=True)
    check("Verbundenes Formular laedt", form_response.status_code == 200)
    check("Formular sendet multipart POST", 'method="POST"' in html and 'enctype="multipart/form-data"' in html)
    check("Datei- und Datenschutzfelder sind angebunden", 'name="dateien"' in html and 'name="datenschutz_bestaetigt"' in html)
    check("Keine lokale Nicht-Speichern-Warnung im Cockpit", "lokale Arbeitsversion ohne Speicherung" not in html)
    csrf_token, form_token = session_tokens(client)
    check("CSRF und Einmal-Token vorhanden", bool(csrf_token and form_token))

    invalid_before = int(scalar("SELECT COUNT(*) FROM auftraege") or 0)
    invalid = client.post(
        "/admin/versicherungsschaden",
        data={portal.CSRF_FIELD_NAME: csrf_token, "schadenaufnahme_form_token": form_token},
    )
    check("Servervalidierung lehnt leere Meldung ab", invalid.status_code == 400)
    check("Ungueltige Meldung erzeugt keinen Fall", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == invalid_before)

    # Neues, weiterhin gueltiges Formular-Token aus derselben Seite verwenden.
    client.get("/admin/versicherungsschaden")
    csrf_token, form_token = session_tokens(client)
    future_payload = base_payload(
        csrf_token,
        form_token,
        unfall_datum=(date.today() + timedelta(days=1)).isoformat(),
    )
    future = client.post("/admin/versicherungsschaden", data=future_payload)
    check("Zukuenftiges Schadendatum wird abgelehnt", future.status_code == 400)
    check("Zukunftsfehler erzeugt keinen Fall", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == invalid_before)

    client.get("/admin/versicherungsschaden")
    csrf_token, form_token = session_tokens(client)
    allianz_before = int(scalar("SELECT COUNT(*) FROM versicherungen WHERE LOWER(name) LIKE '%allianz%'") or 0)
    original_analysis = portal.build_document_analysis_bundle_safe
    portal.build_document_analysis_bundle_safe = fake_analysis
    try:
        payload = base_payload(csrf_token, form_token)
        payload["dateien"] = [
            (io.BytesIO(PNG_BYTES), "schadenfoto.png", "text/html"),
            (io.BytesIO(PDF_BYTES), "unfallbericht.pdf", "text/html"),
        ]
        created_response = client.post(
            "/admin/versicherungsschaden",
            data=payload,
            content_type="multipart/form-data",
            follow_redirects=False,
        )
    finally:
        portal.build_document_analysis_bundle_safe = original_analysis
    check("Gueltige Meldung nutzt Post/Redirect/Get", created_response.status_code == 302)
    location = created_response.headers["Location"]
    check("Redirect enthaelt echte Auftrag-ID", "erstellt=" in location, location)
    auftrag_id = int(location.rsplit("erstellt=", 1)[1].split("&", 1)[0])
    auftrag = portal.get_auftrag(auftrag_id)
    check("Genau ein echter Schadenfall angelegt", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == invalid_before + 1)
    check("Kasko korrekt auf Vollkasko gemappt", auftrag["schadenart"] == "vollkasko")
    check("Manuelle Fahrzeugwerte bleiben erhalten", auftrag["fahrzeug"] == "VW Golf Manuell" and auftrag["kennzeichen"] == "MOS IT 42")
    check("FIN und Kontaktdaten gespeichert", auftrag["fin_nummer"] == "WVWZZZ1KZ6W000001" and auftrag["kunde_email"] == "anna@example.test")
    check("Operativer Startstatus stimmt", auftrag["quelle"] == "intern" and auftrag["versicherung_freigabe_status"] == "vorbereitet" and auftrag["schaden_station"] == "aufnahme")
    check("Mobilitaetswunsch verbunden", auftrag["schaden_mietwagen"] == "ja")
    check("Reale Vorgangsnummer", auftrag["schaden_aufnahme_ref"] == f"VS-{auftrag_id:06d}")
    check("Sicherer Kundenstatus-Token", len(auftrag["kunden_status_token"]) >= 20)
    check("Originalmeldung strukturiert gespeichert", auftrag["schaden_aufnahme"]["unfall_ort"] == "Mosbach Bahnhof" and auftrag["schaden_aufnahme"]["kontaktweg"] == "email")
    check("Datenschutzaufnahme dokumentiert", bool(auftrag["schaden_datenschutz_bestaetigt_am"]) and auftrag["schaden_aufnahme"]["datenschutz_version"] == portal.SCHADENAUFNAHME_DATENSCHUTZ_VERSION)
    check("Bestehende Versicherung wiederverwendet", int(scalar("SELECT COUNT(*) FROM versicherungen WHERE LOWER(name) LIKE '%allianz%'") or 0) == allianz_before)
    check("Initialer Status-Log vorhanden", int(scalar("SELECT COUNT(*) FROM status_log WHERE auftrag_id=? AND status=1", (auftrag_id,)) or 0) == 1)

    dateien = portal.list_dateien(auftrag_id)
    check("Beide Dateien in derselben Fallakte", len(dateien) == 2)
    check("Kundenunterlagen sauber klassifiziert", all(item["quelle"] == "kunde" and item["kategorie"] == "standard" for item in dateien))
    mime_by_name = {item["original_name"]: item["mime_type"] for item in dateien}
    check(
        "Client-MIME wird serverseitig ersetzt",
        mime_by_name.get("schadenfoto.png") == "image/png"
        and mime_by_name.get("unfallbericht.pdf") == "application/pdf",
    )
    check("Upload-Dateien liegen nur im Temp-Ordner", all(portal.upload_file_path(item).is_file() and portal.upload_file_path(item).parent == pathlib.Path(portal.UPLOAD_DIR) for item in dateien))
    check("OCR-Ergebnis gespeichert", all(item["analyse_quelle"] == "integration_test" for item in dateien))
    check("OCR hat manuelle Daten nicht ueberschrieben", portal.get_auftrag(auftrag_id)["fahrzeug"] == "VW Golf Manuell")
    check("OCR-Vorschlaege verlangen Werkstattpruefung", portal.get_auftrag(auftrag_id)["analyse_pruefen"] is True)
    task_titles = [item["titel"] for item in portal.list_versicherung_aufgaben(auftrag_id, include_done=True)]
    check("Pruef- und Mobilitaetsaufgabe erzeugt", "Neue Schadenaufnahme prüfen" in task_titles and "Ersatzmobilität klären" in task_titles)
    pdf_datei = next(item for item in dateien if item["original_name"] == "unfallbericht.pdf")
    db = portal.get_db()
    db.execute("UPDATE dateien SET mime_type='text/html' WHERE id=?", (pdf_datei["id"],))
    db.commit()
    db.close()
    pdf_response = client.get(f"/admin/datei/{pdf_datei['id']}")
    check(
        "PDF wird trotz manipuliertem DB-MIME sicher ausgeliefert",
        pdf_response.status_code == 200
        and pdf_response.headers.get("Content-Type", "").startswith("application/pdf")
        and "attachment" in pdf_response.headers.get("Content-Disposition", "").lower()
        and pdf_response.headers.get("X-Content-Type-Options") == "nosniff",
    )

    success = client.get(location)
    success_html = success.get_data(as_text=True)
    check("Echte Erfolgssicht zeigt Referenz", auftrag["schaden_aufnahme_ref"] in success_html)
    check("Echte Erfolgssicht verlinkt Fallakte", f"/admin/versicherung/schaden/{auftrag_id}" in success_html)
    check("Echte Erfolgssicht verlinkt Kundenstatus", f"/status/{auftrag['kunden_status_token']}" in success_html)
    check(
        "Ohne Einwilligung kein WhatsApp-Link",
        not auftrag["kunden_whatsapp_status"]["can_whatsapp"]
        and "Status per WhatsApp teilen" not in success_html,
    )

    detail = client.get(f"/admin/versicherung/schaden/{auftrag_id}")
    detail_html = detail.get_data(as_text=True)
    check("Fallakte zeigt strukturierte Aufnahme", detail.status_code == 200 and "Mosbach Bahnhof" in detail_html and auftrag["schaden_aufnahme_ref"] in detail_html)
    check("Fallakte erklaert fehlende WhatsApp-Einwilligung", "Keine gültige WhatsApp-Einwilligung" in detail_html)
    cases = client.get("/admin/versicherung/faelle").get_data(as_text=True)
    check("Versicherungsuebersicht zeigt reale Referenz", auftrag["schaden_aufnahme_ref"] in cases)

    whatsapp_calls = []
    original_whatsapp_config = portal.whatsapp_bridge_config_errors
    original_whatsapp_send = portal.send_whatsapp_notice_with_fallback
    portal.whatsapp_bridge_config_errors = lambda: []
    portal.send_whatsapp_notice_with_fallback = lambda *args, **kwargs: (
        whatsapp_calls.append((args, kwargs)) or True,
        [],
    )
    try:
        whatsapp_ok, whatsapp_reason = portal.notify_customer_whatsapp_fertig(auftrag)
    finally:
        portal.whatsapp_bridge_config_errors = original_whatsapp_config
        portal.send_whatsapp_notice_with_fallback = original_whatsapp_send
    check(
        "Automatische WhatsApp bleibt ohne Einwilligung gesperrt",
        not whatsapp_ok and "Einwilligung" in whatsapp_reason and not whatsapp_calls,
    )

    opt_in_auftrag = dict(auftrag)
    opt_in_aufnahme = dict(auftrag["schaden_aufnahme"])
    opt_in_aufnahme.update(
        {
            "kontaktweg": "whatsapp",
            "whatsapp_einwilligung": True,
            "whatsapp_einwilligung_am": portal.now_str(),
            "whatsapp_einwilligung_telefon_key": portal.whatsapp_number_key(auftrag["kontakt_telefon"]),
            "whatsapp_einwilligung_quelle": "werkstatt-cockpit",
        }
    )
    opt_in_auftrag["schaden_aufnahme"] = opt_in_aufnahme
    check("Explizites WhatsApp-Opt-in schaltet Link frei", portal.customer_status_share(opt_in_auftrag)["can_whatsapp"])
    opt_in_auftrag["kontakt_telefon"] = "0171 9999999"
    check("Telefonnummernaenderung entzieht WhatsApp-Freigabe", not portal.customer_status_share(opt_in_auftrag)["can_whatsapp"])
    opt_in_auftrag["kontakt_telefon"] = auftrag["kontakt_telefon"]
    opt_in_aufnahme["whatsapp_einwilligung"] = "false"
    check("String false ist keine WhatsApp-Einwilligung", not portal.customer_status_share(opt_in_auftrag)["can_whatsapp"])

    status_response = client.get(f"/status/{auftrag['kunden_status_token']}")
    status_html = status_response.get_data(as_text=True)
    check("Echter Kundenstatus ist erreichbar", status_response.status_code == 200)
    check("Kundenstatus bietet Upload und Nachricht", f"/status/{auftrag['kunden_status_token']}/unterlagen" in status_html and f"/status/{auftrag['kunden_status_token']}/nachricht" in status_html)

    # Gleiches Einmal-Token darf keinen zweiten Fall erzeugen.
    duplicate_payload = base_payload(csrf_token, form_token)
    duplicate = client.post("/admin/versicherungsschaden", data=duplicate_payload)
    check("Doppel-Submit wird abgefangen", duplicate.status_code == 400)
    check("Doppel-Submit erzeugt keinen zweiten Fall", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == invalid_before + 1)

    # Unbekannter/falscher Dateityp muss vor der Fallanlage scheitern.
    client.get("/admin/versicherungsschaden?neu=1")
    csrf_token, form_token = session_tokens(client)
    bad_payload = base_payload(csrf_token, form_token)
    bad_payload["dateien"] = (io.BytesIO(b"MZ-not-an-image"), "schaden.exe")
    bad_before = int(scalar("SELECT COUNT(*) FROM auftraege") or 0)
    bad_response = client.post(
        "/admin/versicherungsschaden",
        data=bad_payload,
        content_type="multipart/form-data",
    )
    check("Unerlaubter Dateityp wird verstaendlich abgelehnt", bad_response.status_code == 400 and "nur JPG" in bad_response.get_data(as_text=True))
    check("Dateifehler erzeugt keinen Fall", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == bad_before)

    client.get("/admin/versicherungsschaden?neu=1")
    csrf_token, form_token = session_tokens(client)
    polyglot_payload = base_payload(csrf_token, form_token)
    polyglot_payload["dateien"] = (
        io.BytesIO(b"<!doctype html><script>alert(1)</script>\n%PDF-1.7\n%%EOF"),
        "manipuliert.pdf",
        "text/html",
    )
    polyglot_response = client.post(
        "/admin/versicherungsschaden",
        data=polyglot_payload,
        content_type="multipart/form-data",
    )
    check(
        "HTML-Praefix vor PDF-Signatur wird abgelehnt",
        polyglot_response.status_code == 400
        and "Dateityp und Dateiinhalt" in polyglot_response.get_data(as_text=True),
    )
    check("Manipuliertes PDF erzeugt keinen Fall", int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == bad_before)

    # Kundenstatus-Upload landet am selben Fall und bleibt pruefpflichtig.
    client.get(f"/status/{auftrag['kunden_status_token']}")
    with client.session_transaction() as session:
        status_csrf = session.get(portal.CSRF_FIELD_NAME)
    customer_files_before = len(portal.list_dateien(auftrag_id))
    malicious_status_upload = client.post(
        f"/status/{auftrag['kunden_status_token']}/unterlagen",
        data={
            portal.CSRF_FIELD_NAME: status_csrf,
            "dateien": (
                io.BytesIO(b"<script>alert(1)</script>%PDF-1.4\n%%EOF"),
                "kunden-polyglot.pdf",
                "text/html",
            ),
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    check(
        "Kundenstatus weist manipuliertes PDF ebenfalls ab",
        malicious_status_upload.status_code == 302
        and len(portal.list_dateien(auftrag_id)) == customer_files_before,
    )
    portal.build_document_analysis_bundle_safe = fake_analysis
    try:
        status_upload = client.post(
            f"/status/{auftrag['kunden_status_token']}/unterlagen",
            data={
                portal.CSRF_FIELD_NAME: status_csrf,
                "upload_notiz": "Detailfoto nachgereicht",
                "dateien": (io.BytesIO(PNG_BYTES), "detailfoto.png"),
            },
            content_type="multipart/form-data",
            follow_redirects=False,
        )
    finally:
        portal.build_document_analysis_bundle_safe = original_analysis
    check("Kundenstatus-Upload gespeichert", status_upload.status_code == 302 and len(portal.list_dateien(auftrag_id)) == 3)
    check("Kundenstatus-Upload erzeugt Pruefaufgabe", "Unterlagen vom Kunden prüfen" in [item["titel"] for item in portal.list_versicherung_aufgaben(auftrag_id, include_done=True)])

    status_message = client.post(
        f"/status/{auftrag['kunden_status_token']}/nachricht",
        data={portal.CSRF_FIELD_NAME: status_csrf, "nachricht": "Bitte morgen zurueckrufen.", "kontakt": "0171 2345678"},
        follow_redirects=False,
    )
    check("Kundennachricht landet in der Fallakte", status_message.status_code == 302 and "Nachricht vom Kunden" in [item["titel"] for item in portal.list_versicherung_aufgaben(auftrag_id, include_done=True)])

    versicherung = portal.get_versicherung(auftrag["versicherung_id"])
    anschreiben = portal.get_auftrag(auftrag_id)["versicherung_anschreiben"]
    check("Versicherungslink fuehrt durch echten Login", f"/versicherung/login/{versicherung['portal_key']}?next=" in anschreiben)
    check("Anschreiben nutzt echten Versicherungs-Zugangscode", versicherung["zugangscode"] in anschreiben)

    # Eine noch unbekannte Versicherung bleibt ein rein interner, nicht portalfaehiger Platzhalter.
    client.get("/admin/versicherungsschaden?neu=1")
    csrf_token, form_token = session_tokens(client)
    unknown_payload = base_payload(
        csrf_token,
        form_token,
        versicherung_name="Noch nicht zugeordnete Testversicherung",
        kennzeichen="MOS OFF 1",
        schaden_nummer="SCH-OFFEN-1",
        kontaktweg="whatsapp",
    )
    unknown_response = client.post(
        "/admin/versicherungsschaden",
        data=unknown_payload,
        follow_redirects=False,
    )
    check("Unbekannte Versicherung legt internen Fall an", unknown_response.status_code == 302 and "erstellt=" in unknown_response.headers.get("Location", ""))
    unknown_id = int(unknown_response.headers["Location"].rsplit("erstellt=", 1)[1].split("&", 1)[0])
    unknown_auftrag = portal.get_auftrag(unknown_id)
    check(
        "WhatsApp-Opt-in wird zeitlich und an die Nummer gebunden",
        unknown_auftrag["schaden_aufnahme"]["whatsapp_einwilligung"]
        and bool(unknown_auftrag["schaden_aufnahme"]["whatsapp_einwilligung_am"])
        and unknown_auftrag["schaden_aufnahme"]["whatsapp_einwilligung_telefon_key"]
        == portal.whatsapp_number_key(unknown_auftrag["kontakt_telefon"])
        and unknown_auftrag["kunden_whatsapp_status"]["can_whatsapp"],
    )
    placeholder = portal.get_versicherung(unknown_auftrag["versicherung_id"])
    check("Unbekannte Versicherung nutzt markierten Platzhalter", portal.versicherung_ist_platzhalter(placeholder))
    unknown_tasks = [item["titel"] for item in portal.list_versicherung_aufgaben(unknown_id, include_done=True)]
    check("Versicherungszuordnung wird als Aufgabe angelegt", "Versicherung zuordnen" in unknown_tasks)
    placeholder_letter = unknown_auftrag["versicherung_anschreiben"]
    check(
        "Platzhalter-Anschreiben enthaelt keine Zugangsdaten",
        "INTERNER ENTWURF" in placeholder_letter
        and placeholder["portal_key"] not in placeholder_letter
        and placeholder["zugangscode"] not in placeholder_letter,
    )
    insurance_login_html = client.get("/versicherung").get_data(as_text=True)
    check("Platzhalter fehlt im Versicherungslogin", portal.OFFENE_VERSICHERUNG_NAME not in insurance_login_html)
    check("Platzhalter-Direktlogin ist gesperrt", client.get(f"/versicherung/login/{placeholder['portal_key']}").status_code == 404)
    check("Platzhalter-Sluglogin ist gesperrt", client.get(f"/versicherung/{placeholder['slug']}").status_code == 404)
    check("Platzhalter-Dashboard ist gesperrt", client.get(f"/versicherung/{placeholder['slug']}/dashboard").status_code == 404)
    with portal.app.test_request_context():
        placeholder_process = portal.build_versicherung_prozess(unknown_auftrag)
    check(
        "Versicherungsprozess blockiert Platzhalter-Versand",
        not placeholder_process["ready_for_send"]
        and "Echte Versicherung zuordnen" in placeholder_process["send_blocker"],
    )
    with client.session_transaction() as session:
        admin_csrf = session.get(portal.CSRF_FIELD_NAME)
    blocked_send = client.post(
        f"/admin/versicherung/schaden/{unknown_id}/melden",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "versicherung_email": "irgendwer@example.test",
            "versicherung_anschreiben": placeholder_letter,
        },
        follow_redirects=False,
    )
    blocked_fresh = portal.get_auftrag(unknown_id)
    check(
        "Admin-Versand bleibt fuer Platzhalter gesperrt",
        blocked_send.status_code == 302
        and blocked_fresh["versicherung_freigabe_status"] == "vorbereitet"
        and not blocked_fresh["versicherung_gemeldet_am"]
        and not blocked_fresh["versicherung_sendefreigabe_am"],
    )
    try:
        portal.send_versicherung_schadenmail(
            unknown_auftrag,
            "irgendwer@example.test",
            "",
            placeholder_letter,
            [],
        )
    except ValueError as exc:
        central_gate_ok = "echte Versicherung" in str(exc)
    else:
        central_gate_ok = False
    check("Zentrale Mailfunktion sperrt Platzhalter ebenfalls", central_gate_ok)

    check("Falscher Kundenstatus-Token bleibt 404", client.get("/status/falsch-und-unbekannt").status_code == 404)
    check("Oeffentliche Designvorschau bleibt deaktiviert", client.get("/schaden-vorschau").status_code == 404)
    print("\nSchadenaufnahme-Integration: alle Pruefungen erfolgreich")


if __name__ == "__main__":
    main()
