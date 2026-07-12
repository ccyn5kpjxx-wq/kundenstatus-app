# -*- coding: utf-8 -*-
"""Isolierter Integrationstest fuer die Schadenaufnahme im Autohaus-Portal."""

from __future__ import annotations

import base64
import io
import json
import os
import pathlib
import sys
import tempfile
from datetime import date
from urllib.parse import parse_qs, urlparse


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="partner_schadenaufnahme_"))
os.environ.update(
    {
        "RENDER": "local-partner-schadenaufnahme-test",
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
        "FLASK_SECRET_KEY": "partner-schadenaufnahme-integration-test",
        "ADMIN_PASS": "partner-schadenaufnahme-integration-test",
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


def create_test_autohaus(name, slug):
    db = portal.get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO autohaeuser
              (name, slug, portal_key, zugangscode, portal_titel, erstellt_am)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                slug,
                f"{slug}-portal-key",
                f"{slug}-zugangscode",
                name,
                portal.now_str(),
            ),
        )
        db.commit()
        return portal.get_autohaus(cursor.lastrowid)
    finally:
        db.close()


def login_partner(client, autohaus):
    with client.session_transaction() as session:
        session["partner_autohaus_id"] = int(autohaus["id"])


def session_tokens(client, autohaus):
    keys = portal.schadenaufnahme_session_keys(autohaus)
    with client.session_transaction() as session:
        return session.get(portal.CSRF_FIELD_NAME), session.get(keys["form"])


def base_payload(csrf_token, form_token, forged_autohaus_id, **overrides):
    payload = {
        portal.CSRF_FIELD_NAME: csrf_token,
        "schadenaufnahme_form_token": form_token,
        "schadenart": "kasko",
        "kunde_name": "Paula Partnerfall",
        "kontakt_telefon": "0171 2345678",
        "kunde_email": "paula@example.test",
        "kontaktweg": "email",
        "kennzeichen": "MOS PA 42",
        "fahrzeug": "VW Golf Partner Manuell",
        "fin_nummer": "WVWZZZ1KZ6W000001",
        "unfall_datum": date.today().isoformat(),
        "unfall_zeit": "10:15",
        "unfall_ort": "Mosbach Testparkplatz",
        "beschreibung": "Beim Rangieren wurde die linke Fahrzeugseite beschaedigt.",
        "versicherung_name": "Nicht hinterlegte Partner Testversicherung",
        "versicherung_police": "POL-PARTNER-42",
        "schaden_nummer": "SCH-PARTNER-42",
        "mobilitaet": "ja",
        "datenschutz_bestaetigt": "1",
        # Diese Felder sind absichtliche Manipulationsversuche. Massgeblich darf
        # ausschliesslich das angemeldete Autohaus und der serverseitige Actor sein.
        "autohaus_id": str(forged_autohaus_id),
        "quelle": "intern",
        "erfasst_von": "werkstatt-cockpit",
    }
    payload.update(overrides)
    return payload


def fake_analysis(_path, original_name):
    structured = {
        "fahrzeug": "Ferrari OCR",
        "kennzeichen": "OCR X 999",
        "fin_nummer": "WVWZZZ1KZ6WOCR999",
        "analyse_text": "OCR bleibt ein Pruefvorschlag",
    }
    return {
        "status": "ok",
        "text": f"Partner-OCR-Test aus {original_name}",
        "source": "partner_integration_test",
        "analysis_json": json.dumps(structured),
        "structured": structured,
        "hint": "",
    }


def main():
    portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
    portal.init_db()
    portal.PUBLIC_FORM_ATTEMPTS.clear()

    autohaus_a = create_test_autohaus("Partner Testhaus A", "partner-testhaus-a")
    autohaus_b = create_test_autohaus("Partner Testhaus B", "partner-testhaus-b")
    route_a = f"/partner/{autohaus_a['slug']}/versicherung/neu"
    route_b = f"/partner/{autohaus_b['slug']}/versicherung/neu"
    count_before = int(scalar("SELECT COUNT(*) FROM auftraege") or 0)

    anonymous = portal.app.test_client()
    check("Partner-Wizard ohne Login geschuetzt", anonymous.get(route_a).status_code == 302)
    with anonymous.session_transaction() as session:
        session[portal.CSRF_FIELD_NAME] = "anonymous-test-csrf"
    anonymous_post = anonymous.post(
        route_a,
        data=base_payload(
            "anonymous-test-csrf",
            "anonymous-fake-nonce",
            autohaus_b["id"],
        ),
        follow_redirects=False,
    )
    check("Partner-POST ohne Login geschuetzt", anonymous_post.status_code == 302)
    check(
        "Unauthentifizierte Zugriffe erzeugen keinen Fall",
        int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == count_before,
    )

    partner_a = portal.app.test_client()
    login_partner(partner_a, autohaus_a)
    form_response = partner_a.get(route_a)
    partner_html = form_response.get_data(as_text=True)
    check("Partner-Wizard laedt", form_response.status_code == 200)
    csrf_token, form_token = session_tokens(partner_a, autohaus_a)
    check("CSRF und partnergebundener Einmal-Token vorhanden", bool(csrf_token and form_token))
    check(
        "Partner nutzt denselben verbundenen Fuenf-Schritt-Wizard",
        'id="damage-form"' in partner_html
        and 'data-portal-mode="autohaus"' in partner_html
        and "Schritt 1 von 5" in partner_html
        and "Versicherungsfall für Kunden melden" in partner_html
        and f'action="{route_a}"' in partner_html
        and 'enctype="multipart/form-data"' in partner_html,
    )
    check(
        "Partner-Wizard enthaelt keine Admin-Navigation",
        "/admin" not in partner_html
        and "Werkstatt-Cockpit" not in partner_html
        and "Alle Versicherungsfälle" not in partner_html,
    )

    admin = portal.app.test_client()
    with admin.session_transaction() as session:
        session["admin"] = True
    admin_html = admin.get("/admin/versicherungsschaden").get_data(as_text=True)
    common_markers = (
        'id="damage-form"',
        'data-step="1"',
        'data-step="2"',
        'data-step="3"',
        'data-step="4"',
        'data-step="5"',
        'name="dateien"',
        'name="datenschutz_bestaetigt"',
    )
    check(
        "Werkstatt und Partner teilen dieselbe Wizard-Struktur",
        all(marker in partner_html and marker in admin_html for marker in common_markers),
    )

    slug_count = int(scalar("SELECT COUNT(*) FROM auftraege") or 0)
    wrong_slug = partner_a.post(
        route_b,
        data=base_payload(csrf_token, form_token, autohaus_b["id"]),
        follow_redirects=False,
    )
    check("Partner A darf Route von Partner B nicht benutzen", wrong_slug.status_code == 302)
    check(
        "Falscher Slug erzeugt keinen Fall",
        int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == slug_count,
    )

    missing_csrf_payload = base_payload(csrf_token, form_token, autohaus_b["id"])
    missing_csrf_payload.pop(portal.CSRF_FIELD_NAME)
    missing_csrf = partner_a.post(route_a, data=missing_csrf_payload, follow_redirects=False)
    check("Fehlendes CSRF wird abgefangen", missing_csrf.status_code == 302)
    check(
        "CSRF-Fehler erzeugt keinen Fall",
        int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == slug_count,
    )

    partner_a.get(route_a)
    csrf_token, form_token = session_tokens(partner_a, autohaus_a)
    wrong_nonce = partner_a.post(
        route_a,
        data=base_payload(csrf_token, "falscher-partner-nonce", autohaus_b["id"]),
    )
    check(
        "Falscher Partner-Nonce wird serverseitig abgelehnt",
        wrong_nonce.status_code == 400
        and "Formular war nicht mehr aktuell" in wrong_nonce.get_data(as_text=True),
    )
    check(
        "Nonce-Fehler erzeugt keinen Fall",
        int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == slug_count,
    )

    partner_a.get(route_a)
    csrf_token, form_token = session_tokens(partner_a, autohaus_a)
    original_analysis = portal.build_document_analysis_bundle_safe
    portal.build_document_analysis_bundle_safe = fake_analysis
    try:
        valid_payload = base_payload(csrf_token, form_token, autohaus_b["id"])
        valid_payload["dateien"] = (
            io.BytesIO(PNG_BYTES),
            "partner-schadenfoto.png",
            "text/html",
        )
        created_response = partner_a.post(
            route_a,
            data=valid_payload,
            content_type="multipart/form-data",
            follow_redirects=False,
        )
    finally:
        portal.build_document_analysis_bundle_safe = original_analysis

    check("Gueltige Partnermeldung nutzt Post/Redirect/Get", created_response.status_code == 302)
    location = created_response.headers.get("Location", "")
    query = parse_qs(urlparse(location).query)
    created_id = int((query.get("erstellt") or [0])[0])
    check(
        "PRG bleibt vollstaendig im Autohaus-Portal",
        created_id > 0 and urlparse(location).path == route_a and "/admin" not in location,
        location,
    )
    auftrag = portal.get_auftrag(created_id)
    check(
        "Autohaus-ID und Quelle kommen ausschliesslich aus der Partnersession",
        int(auftrag["autohaus_id"] or 0) == int(autohaus_a["id"])
        and int(auftrag["autohaus_id"] or 0) != int(autohaus_b["id"])
        and auftrag["quelle"] == "autohaus",
    )
    check(
        "Partnerfall startet im richtigen Versicherungsprozess",
        bool(auftrag["angebotsphase"])
        and auftrag["versicherung_freigabe_status"] == "vorbereitet"
        and auftrag["schaden_station"] == "aufnahme",
    )
    check(
        "Partner-Actor und Datenschutz sind nachvollziehbar gespeichert",
        auftrag["schaden_aufnahme"]["erfasst_von"] == "autohaus-portal"
        and int(auftrag["schaden_aufnahme"]["autohaus_id"] or 0) == int(autohaus_a["id"])
        and auftrag["schaden_aufnahme"]["autohaus_name"] == autohaus_a["name"]
        and auftrag["schaden_aufnahme"]["datenschutz_version"]
        == portal.SCHADENAUFNAHME_DATENSCHUTZ_VERSION
        and bool(auftrag["schaden_datenschutz_bestaetigt_am"]),
    )
    check(
        "Ersatzmobilitaet und Kundenstatus sind mit dem Fall verbunden",
        auftrag["schaden_mietwagen"] == "ja"
        and len(auftrag["kunden_status_token"]) >= 20,
    )

    dateien = portal.list_dateien(created_id)
    check("Partner-Upload liegt in derselben Fallakte", len(dateien) == 1)
    check(
        "Uploadquelle und serverseitiger MIME-Typ stimmen",
        dateien[0]["quelle"] == "autohaus"
        and dateien[0]["kategorie"] == "standard"
        and dateien[0]["mime_type"] == "image/png",
    )
    check(
        "OCR bleibt Pruefvorschlag und ueberschreibt keine Partnerdaten",
        dateien[0]["analyse_quelle"] == "partner_integration_test"
        and auftrag["fahrzeug"] == "VW Golf Partner Manuell"
        and auftrag["kennzeichen"] == "MOS PA 42"
        and auftrag["fin_nummer"] == "WVWZZZ1KZ6W000001"
        and auftrag["analyse_pruefen"] is True,
    )

    aufgaben = portal.list_versicherung_aufgaben(created_id, include_done=True)
    aufgaben_nach_titel = {item["titel"]: item for item in aufgaben}
    check(
        "Partner-Pruef-, Zuordnungs- und Mobilitaetsaufgaben wurden erzeugt",
        {
            "Neue Schadenaufnahme prüfen",
            "Versicherung zuordnen",
            "Ersatzmobilität klären",
        }.issubset(aufgaben_nach_titel),
    )
    check(
        "Aufgaben tragen die Partnerquelle",
        all(aufgaben_nach_titel[titel]["quelle"] == "autohaus" for titel in aufgaben_nach_titel),
    )
    benachrichtigungen = portal.list_benachrichtigungen(created_id)
    check(
        "Neue Schadenaufnahme informiert die Werkstatt mit Partnerquelle",
        any(
            item["titel"] == "Neue Schadenaufnahme" and item["quelle"] == "autohaus"
            for item in benachrichtigungen
        ),
    )

    success = partner_a.get(location)
    success_html = success.get_data(as_text=True)
    check(
        "Partner-Erfolg zeigt Referenz und nur Partnerziele",
        success.status_code == 200
        and auftrag["schaden_aufnahme_ref"] in success_html
        and f"/partner/{autohaus_a['slug']}/angebot/{created_id}" in success_html
        and f"/status/{auftrag['kunden_status_token']}" in success_html
        and "/admin" not in success_html,
    )
    admin_postfach_html = admin.get("/admin/postfach").get_data(as_text=True)
    check(
        "Werkstatt sieht den neuen Partnerfall sofort im Postfach",
        auftrag["fahrzeug"] in admin_postfach_html
        and auftrag["kennzeichen"] in admin_postfach_html
        and "Neue Schadenaufnahme prüfen" in admin_postfach_html,
    )

    duplicate_payload = base_payload(csrf_token, form_token, autohaus_b["id"])
    duplicate = partner_a.post(route_a, data=duplicate_payload)
    check("Doppel-Submit mit verbrauchtem Nonce wird abgelehnt", duplicate.status_code == 400)
    check(
        "Doppel-Submit erzeugt keinen zweiten Fall",
        int(scalar("SELECT COUNT(*) FROM auftraege") or 0) == slug_count + 1,
    )

    partner_b = portal.app.test_client()
    login_partner(partner_b, autohaus_b)
    partner_b.get(route_b)
    keys_b = portal.schadenaufnahme_session_keys(autohaus_b)
    with partner_b.session_transaction() as session:
        # Auch eine manipulierte Session-Erfolgs-ID darf keinen fremden Fall zeigen.
        session[keys_b["success"]] = created_id
    foreign_success = partner_b.get(f"{route_b}?erstellt={created_id}")
    foreign_html = foreign_success.get_data(as_text=True)
    check(
        "Fremde Erfolg-ID bleibt trotz manipulierter Session verborgen",
        foreign_success.status_code == 200
        and auftrag["schaden_aufnahme_ref"] not in foreign_html
        and auftrag["kunde_name"] not in foreign_html
        and f"/partner/{autohaus_a['slug']}/angebot/{created_id}" not in foreign_html,
    )

    own_detail = partner_a.get(
        f"/partner/{autohaus_a['slug']}/angebot/{created_id}",
        follow_redirects=False,
    )
    foreign_detail = partner_b.get(
        f"/partner/{autohaus_b['slug']}/angebot/{created_id}",
        follow_redirects=False,
    )
    check("Partner A sieht seinen Schadenfall", own_detail.status_code == 200)
    check("Partner B sieht den Schadenfall von A nicht", foreign_detail.status_code == 404)

    customer = portal.app.test_client()
    status_response = customer.get(f"/status/{auftrag['kunden_status_token']}")
    status_html = status_response.get_data(as_text=True)
    check(
        "Kundenstatus des Partnerfalls ist erreichbar und verbunden",
        status_response.status_code == 200
        and auftrag["fahrzeug"] in status_html
        and f"/status/{auftrag['kunden_status_token']}/unterlagen" in status_html
        and f"/status/{auftrag['kunden_status_token']}/nachricht" in status_html,
    )

    # Eine echte Versicherungszuordnung ist noch keine Freigabe zur Offenlegung.
    # Erst der ausdrueckliche Sende-Schritt des Autohauses darf den Fall im
    # Versicherungslogin sichtbar und bearbeitbar machen.
    bekannte_versicherung = next(
        item
        for item in portal.list_versicherungen()
        if not portal.versicherung_ist_platzhalter(item)
    )
    partner_a.get(f"{route_a}?neu=1")
    csrf_token, form_token = session_tokens(partner_a, autohaus_a)
    gate_payload = base_payload(
        csrf_token,
        form_token,
        autohaus_b["id"],
        kunde_name="Versicherung Gate Testkunde",
        kennzeichen="MOS VG 77",
        fahrzeug="Audi A4 Gate Test",
        fin_nummer="WAUZZZ8K9DA000077",
        schaden_nummer="SCH-GATE-77",
        versicherung_name=bekannte_versicherung["name"],
        mobilitaet="unklar",
    )
    gate_response = partner_a.post(route_a, data=gate_payload, follow_redirects=False)
    gate_query = parse_qs(urlparse(gate_response.headers.get("Location", "")).query)
    gate_id = int((gate_query.get("erstellt") or [0])[0])
    check(
        "Vorbereiteter Partnerfall mit echter Versicherung wird angelegt",
        gate_response.status_code == 302 and gate_id > 0,
    )

    versicherung_client = portal.app.test_client()
    with versicherung_client.session_transaction() as session:
        session["versicherung_id"] = bekannte_versicherung["id"]
        session[portal.CSRF_FIELD_NAME] = "versicherung-gate-csrf"
    gate_dashboard_before = versicherung_client.get(
        f"/versicherung/{bekannte_versicherung['slug']}/dashboard"
    ).get_data(as_text=True)
    check(
        "Vorbereiteter Partnerfall bleibt im Versicherungsdashboard verborgen",
        "Versicherung Gate Testkunde" not in gate_dashboard_before,
    )
    check(
        "Vorbereiteter Partnerfall ist per Versicherungs-Direktlink gesperrt",
        versicherung_client.get(
            f"/versicherung/{bekannte_versicherung['slug']}/auftrag/{gate_id}"
        ).status_code
        == 404,
    )
    check(
        "Versicherung kann vorbereiteten Partnerfall nicht per POST veraendern",
        versicherung_client.post(
            f"/versicherung/{bekannte_versicherung['slug']}/auftrag/{gate_id}/freigabe",
            data={portal.CSRF_FIELD_NAME: "versicherung-gate-csrf", "aktion": "rueckfrage"},
        ).status_code
        == 404,
    )

    freigabe_zeit = portal.now_str()
    db = portal.get_db()
    try:
        db.execute(
            """
            UPDATE auftraege
            SET versicherung_freigabe_status='in_pruefung',
                versicherung_sendefreigabe_am=?,
                versicherung_gemeldet_am=?,
                versicherung_portal_freigabe_id=?,
                geaendert_am=?
            WHERE id=?
            """,
            (
                freigabe_zeit,
                freigabe_zeit,
                bekannte_versicherung["id"],
                freigabe_zeit,
                gate_id,
            ),
        )
        db.commit()
    finally:
        db.close()
    gate_dashboard_after = versicherung_client.get(
        f"/versicherung/{bekannte_versicherung['slug']}/dashboard"
    ).get_data(as_text=True)
    check(
        "Nach dokumentierter Sendefreigabe sieht die Versicherung den Fall",
        "Versicherung Gate Testkunde" in gate_dashboard_after
        and versicherung_client.get(
            f"/versicherung/{bekannte_versicherung['slug']}/auftrag/{gate_id}"
        ).status_code
        == 200,
    )

    andere_versicherung = next(
        item
        for item in portal.list_versicherungen()
        if not portal.versicherung_ist_platzhalter(item)
        and int(item["id"]) != int(bekannte_versicherung["id"])
    )
    with partner_a.session_transaction() as session:
        partner_csrf = session.get(portal.CSRF_FIELD_NAME)
    blocked_partner_switch = partner_a.post(
        f"/partner/{autohaus_a['slug']}/angebot/{gate_id}",
        data={
            portal.CSRF_FIELD_NAME: partner_csrf,
            "aktion": "save",
            "versicherung_id": str(andere_versicherung["id"]),
        },
        follow_redirects=False,
    )
    check(
        "Partner kann eine bereits eingebundene Versicherung nicht direkt wechseln",
        blocked_partner_switch.status_code == 302
        and int(portal.get_auftrag(gate_id)["versicherung_id"])
        == int(bekannte_versicherung["id"]),
    )
    with admin.session_transaction() as session:
        admin_csrf = session.get(portal.CSRF_FIELD_NAME)
    blocked_admin_switch = admin.post(
        f"/admin/versicherung/schaden/{gate_id}",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "versicherung_id": str(andere_versicherung["id"]),
        },
        follow_redirects=False,
    )
    check(
        "Auch die Admin-Bearbeitung uebernimmt keine alte Freigabe auf eine neue Versicherung",
        blocked_admin_switch.status_code == 302
        and int(portal.get_auftrag(gate_id)["versicherung_id"])
        == int(bekannte_versicherung["id"]),
    )

    andere_versicherung_client = portal.app.test_client()
    with andere_versicherung_client.session_transaction() as session:
        session["versicherung_id"] = andere_versicherung["id"]
    db = portal.get_db()
    try:
        db.execute(
            "UPDATE auftraege SET versicherung_id=? WHERE id=?",
            (andere_versicherung["id"], gate_id),
        )
        db.commit()
    finally:
        db.close()
    check(
        "Portal-Freigabe bleibt selbst bei einer direkten ID-Aenderung an Versicherung A gebunden",
        versicherung_client.get(
            f"/versicherung/{bekannte_versicherung['slug']}/auftrag/{gate_id}"
        ).status_code
        == 404
        and andere_versicherung_client.get(
            f"/versicherung/{andere_versicherung['slug']}/auftrag/{gate_id}"
        ).status_code
        == 404,
    )
    db = portal.get_db()
    try:
        db.execute(
            "UPDATE auftraege SET versicherung_id=? WHERE id=?",
            (bekannte_versicherung["id"], gate_id),
        )
        db.commit()
    finally:
        db.close()

    zuteilung_id = portal.create_auftrag(
        "versicherung",
        versicherung_id=bekannte_versicherung["id"],
        kunde_name="Eigene Versicherungszuteilung",
        fahrzeug="BMW Versicherungsfall",
        versicherung_freigabe_status="zugeteilt",
        angebotsphase=1,
    )
    check(
        "Von der Versicherung selbst angelegte Zuteilung bleibt sichtbar",
        versicherung_client.get(
            f"/versicherung/{bekannte_versicherung['slug']}/auftrag/{zuteilung_id}"
        ).status_code
        == 200,
    )

    print("\nPartner-Schadenaufnahme-Integration: alle Pruefungen erfolgreich")


if __name__ == "__main__":
    main()
