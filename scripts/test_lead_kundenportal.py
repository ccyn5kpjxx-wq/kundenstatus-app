# -*- coding: utf-8 -*-
"""Isolierter Regressionstest fuer Lead -> Kundenportal -> Auftrag."""

from __future__ import annotations

import base64
from datetime import date, timedelta
from io import BytesIO
import os
import pathlib
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="lead_kundenportal_"))
os.environ.update(
    {
        "RENDER": "local-lead-kundenportal-test",
        "DATABASE_URL": "",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEMP_DIR),
        "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
        "UPLOAD_DIR": str(TEMP_DIR / "uploads"),
        "BACKUP_DIR": str(TEMP_DIR / "backups"),
        "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "OPENAI_API_KEY": "",
        "FLASK_SECRET_KEY": "lead-kundenportal-test",
        "ADMIN_PASS": "lead-kundenportal-test",
        "SCHADEN_SMTP_PASS": "",
    }
)

import app as portal  # noqa: E402


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


def check(label, condition):
    passed = bool(condition)
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")
    return passed


def csrf_token(client):
    with client.session_transaction() as session:
        return session.get(portal.CSRF_FIELD_NAME)


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    portal.schedule_change_backup = lambda _reason: None
    admin = portal.app.test_client()
    kunde = portal.app.test_client()
    checks = []

    lead_id = portal.create_lead(
        {
            "website": "auto-lackierzentrum",
            "herkunft_typ": "website_formular",
            "quelle": "website",
            "status": "neu",
            "kunde_name": "Klara Kundenportal",
            "kontakt_telefon": "0151 23456789",
            "kunde_email": "klara@example.test",
            "fahrzeug": "VW Golf VII",
            "kennzeichen": "MOS-K 123",
            "beschreibung": "Kratzer an der Beifahrertuer",
        }
    )

    with admin.session_transaction() as session:
        session["admin"] = True
    admin.get("/admin/leads")
    admin_csrf = csrf_token(admin)
    create_case = admin.post(
        f"/admin/leads/{lead_id}/auftrag",
        data={portal.CSRF_FIELD_NAME: admin_csrf},
        follow_redirects=False,
    )
    lead = portal.get_lead(lead_id)
    auftrag = portal.get_auftrag(lead["auftrag_id"])
    checks += [
        check("Fruehe Anfrage-Akte wurde angelegt", create_case.status_code == 302 and auftrag["quelle"] == "lead"),
        check("Lead bleibt offen", lead["status"] == "unterlagen_fehlen" and not lead["is_closed"]),
        check("Anfrage-Akte startet in Angebotsphase", auftrag["angebotsphase"] and auftrag["angebot_status"] == "angefragt"),
        check("Kontaktdaten und starker Kundenlink wurden uebernommen", auftrag["kunde_email"] == "klara@example.test" and len(auftrag["kunden_status_token"]) >= 18),
    ]

    lead_page = admin.get(f"/admin/leads/{lead_id}")
    lead_html = lead_page.get_data(as_text=True)
    checks.append(
        check(
            "Lead-Detail zeigt Kundenportal und E-Mail-Linktext",
            lead_page.status_code == 200
            and "Anfrage-Akte aktiv" in lead_html
            and auftrag["kunden_status_url"] in lead_html
            and "Bilder und Unterlagen nachreichen" in lead_html,
        )
    )

    token = auftrag["kunden_status_token"]
    start_page = kunde.get(f"/status/{token}")
    start_html = start_page.get_data(as_text=True)
    customer_csrf = csrf_token(kunde)
    wunsch_annahme = (date.today() + timedelta(days=10)).isoformat()
    wunsch_abholung = (date.today() + timedelta(days=14)).isoformat()
    wunsch_annahme_db = portal.format_date(wunsch_annahme)
    wunsch_abholung_db = portal.format_date(wunsch_abholung)
    checks.append(
        check(
            "Kundenportal zeigt Anfrageablauf vor dem Angebot",
            start_page.status_code == 200
            and "Von der Anfrage bis zur Reparatur" in start_html
            and "Angebot wird vorbereitet" in start_html
            and "Unterlagen nachreichen" in start_html,
        )
    )

    upload = kunde.post(
        f"/status/{token}/unterlagen",
        data={
            portal.CSRF_FIELD_NAME: customer_csrf,
            "upload_notiz": "Detailfoto Beifahrertuer",
            "dateien": (BytesIO(PNG_1X1), "detail.png"),
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    lead = portal.get_lead(lead_id)
    checks.append(
        check(
            "Kundenbild wird sicher gespeichert und am Lead signalisiert",
            upload.status_code == 302
            and "Kundenunterlage" in lead["naechste_aktion"]
            and any(item["quelle"] == "kunde" for item in portal.list_dateien(auftrag["id"])),
        )
    )

    terminwunsch = kunde.post(
        f"/status/{token}/termin",
        data={
            portal.CSRF_FIELD_NAME: customer_csrf,
            "wunschtermin": wunsch_annahme,
            "nachricht": "Bitte vormittags.",
        },
        follow_redirects=False,
    )
    auftrag = portal.get_auftrag(auftrag["id"])
    checks.append(
        check(
            "Frueher Terminwunsch bleibt fuer die spaetere Angebotsannahme gespeichert",
            terminwunsch.status_code == 302
            and auftrag["schaden_aufnahme"].get("kunden_wunsch_annahme_datum") == wunsch_annahme_db,
        )
    )

    offer = admin.post(
        f"/admin/angebot/{auftrag['id']}/senden",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "werkstatt_angebot_preis": "1.250,00 EUR brutto",
            "werkstatt_angebot_text": "Instandsetzung und Lackierung der Beifahrertuer",
            "werkstatt_angebot_notiz": "Vorbehaltlich Sichtpruefung vor Ort.",
        },
        follow_redirects=False,
    )
    auftrag = portal.get_auftrag(auftrag["id"])
    lead = portal.get_lead(lead_id)
    offer_page = kunde.get(f"/status/{token}")
    offer_html = offer_page.get_data(as_text=True)
    checks += [
        check("Werkstatt-Angebot wird im Kundenportal bereitgestellt", offer.status_code == 302 and auftrag["angebot_status"] == "angebot_abgegeben" and "Ihr Werkstatt-Angebot" in offer_html),
        check("Bereits gesendeter Terminwunsch ist im Annahmeformular vorausgefuellt", f'value="{wunsch_annahme}"' in offer_html),
        check("Lead wechselt automatisch auf Angebot offen", lead["status"] == "angebot_offen"),
    ]

    missing_confirmation = kunde.post(
        f"/status/{token}/angebot-annehmen",
        data={
            portal.CSRF_FIELD_NAME: customer_csrf,
            "wunsch_annahme_datum": wunsch_annahme,
            "transport_art": "standard",
            "ersatzfahrzeug": "nein",
        },
        follow_redirects=False,
    )
    checks.append(
        check(
            "Angebot kann nicht ohne ausdrueckliche Bestaetigung angenommen werden",
            missing_confirmation.status_code == 302
            and portal.get_auftrag(auftrag["id"])["angebot_status"] == "angebot_abgegeben",
        )
    )

    accept = kunde.post(
        f"/status/{token}/angebot-annehmen",
        data={
            portal.CSRF_FIELD_NAME: customer_csrf,
            "angebot_annehmen_bestaetigt": "1",
            "wunsch_annahme_datum": wunsch_annahme,
            "wunsch_abholung_datum": wunsch_abholung,
            "transport_art": "hol_und_bring",
            "ersatzfahrzeug": "ja",
            "abhol_adresse": "Musterstrasse 12, 74821 Mosbach",
            "kunden_wunsch_hinweis": "Bitte vormittags abholen.",
        },
        follow_redirects=False,
    )
    auftrag = portal.get_auftrag(auftrag["id"])
    lead = portal.get_lead(lead_id)
    intake = auftrag["schaden_aufnahme"]
    checks += [
        check("Kunde nimmt Angebot verbindlich an", accept.status_code == 302 and auftrag["angebot_status"] == "angenommen" and not auftrag["angebotsphase"]),
        check("Lead wird erst jetzt automatisch gewonnen", lead["status"] == "gewonnen"),
        check(
            "Termin, Transport und Ersatzfahrzeug bleiben bis Werkstattbestaetigung Wuensche",
            intake.get("kunden_wunsch_annahme_datum") == wunsch_annahme_db
            and intake.get("kunden_wunsch_abholung_datum") == wunsch_abholung_db
            and auftrag["transport_art"] == "hol_und_bring"
            and auftrag["schaden_mietwagen"] == "ja"
            and not auftrag["annahme_datum"],
        ),
    ]

    pending_page = kunde.get(f"/status/{token}").get_data(as_text=True)
    checks.append(check("Kundenportal zeigt angenommene Auftragsbestaetigung mit offener Terminpruefung", "Auftragsbestätigung" in pending_page and "Terminbestätigung folgt" in pending_page))

    confirm = admin.post(
        f"/admin/auftrag/{auftrag['id']}/kundenwuensche-bestaetigen",
        data={portal.CSRF_FIELD_NAME: admin_csrf},
        follow_redirects=False,
    )
    auftrag = portal.get_auftrag(auftrag["id"])
    confirmed_page = kunde.get(f"/status/{token}").get_data(as_text=True)
    checks.append(
        check(
            "Werkstatt bestaetigt Termine und plant Auftrag ein",
            confirm.status_code == 302
            and auftrag["status"] == 2
            and auftrag["annahme_datum"] == wunsch_annahme_db
            and auftrag["abholtermin"] == wunsch_abholung_db
            and bool(auftrag["schaden_aufnahme"].get("kunden_wunsch_bestaetigt_am"))
            and "Termine bestätigt" in confirmed_page,
        )
    )

    print(f"Temporaere Testdaten: {TEMP_DIR}")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
