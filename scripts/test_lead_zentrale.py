# -*- coding: utf-8 -*-
"""Isolierter Integrationstest fuer die gemeinsame Lead-Zentrale."""

from __future__ import annotations

import base64
from io import BytesIO
import os
import pathlib
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="lead_zentrale_"))
os.environ.update(
    {
        "RENDER": "local-lead-zentrale-test",
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
        "FLASK_SECRET_KEY": "lead-zentrale-test",
        "ADMIN_PASS": "lead-zentrale-test",
        "SCHADEN_SMTP_PASS": "",
        "TOMORROWWORKS_SMTP_PASS": "",
        "MIETWAGEN_SMTP_PASS": "",
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


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    portal.schedule_change_backup = lambda _reason: None
    portal.sende_oeffentliche_anfrage_benachrichtigung = lambda *args, **kwargs: None
    client = portal.app.test_client()
    checks = []

    app_source = (ROOT / "app.py").read_text(encoding="utf-8")
    checks.append(check(
        "Lead-Mail-Migration nutzt PostgreSQL-sichere Parameter",
        "LIKE '%@tomorrowworks-agentur.de%'" not in app_source
        and "LIKE '%@autovermietung-mos.de%'" not in app_source,
    ))

    lack_id = portal.create_lead(
        {
            "website": "auto-lackierzentrum",
            "herkunft_typ": "website_formular",
            "quelle": "website",
            "kunde_name": "Lack Kunde",
            "kunde_email": "lack@example.test",
            "fahrzeug": "VW Golf",
            "beschreibung": "Kratzer an der Tuer",
        }
    )
    tw_response = client.post(
        "/api/leads",
        json={
            "name": "Tomorrow Kunde",
            "company": "Beispiel GmbH",
            "email": "tw@example.test",
            "project": "Neue Website",
            "message": "Wir brauchen eine neue Website mit Kontaktformular.",
        },
        headers={"Origin": "https://tomorrowworks-agentur.de"},
    )
    rental_response = client.post(
        "/api/leads",
        json={
            "name": "Miet Kunde",
            "phone": "0171 1234567",
            "vehicle": "Hyundai KONA",
            "abholung": "2026-08-01",
            "rueckgabe": "2026-08-05",
            "message": "Bitte Verfuegbarkeit pruefen.",
        },
        headers={"Origin": "https://autovermietung-mos.de"},
    )
    forbidden_response = client.post(
        "/api/leads",
        json={"name": "Nicht erlaubt", "email": "blocked@example.test", "message": "Test"},
        headers={"Origin": "https://example.invalid"},
    )
    tw_payload = tw_response.get_json() or {}
    rental_payload = rental_response.get_json() or {}
    tw_leads = portal.list_leads(status_filter="alle", website_filter="tomorrowworks")
    rental_leads = portal.list_leads(status_filter="alle", website_filter="autovermietung-mos")
    all_leads = portal.list_leads(status_filter="alle", website_filter="alle")
    checks += [
        check("TomorrowWorks API erzeugt TW-Lead", tw_response.status_code == 201 and tw_payload.get("reference", "").startswith("TW-") and len(tw_leads) == 1),
        check("Mietwagen API erzeugt MW-Lead", rental_response.status_code == 201 and rental_payload.get("reference", "").startswith("MW-") and len(rental_leads) == 1),
        check("Alle drei Websites gemeinsam sichtbar", len(all_leads) == 3 and {lead["website"] for lead in all_leads} == set(portal.LEAD_WEBSITES)),
        check("CORS nur fuer passende Website", tw_response.headers.get("Access-Control-Allow-Origin") == "https://tomorrowworks-agentur.de"),
        check("Fremde Websites werden abgewiesen", forbidden_response.status_code == 403),
    ]

    with client.session_transaction() as session:
        session["admin"] = True
    client.get("/admin/leads")
    with client.session_transaction() as session:
        csrf_token = session.get(portal.CSRF_FIELD_NAME)
    tw_lead = tw_leads[0]
    list_page = client.get("/admin/leads?status=alle&website=alle")
    tw_detail = client.get(f"/admin/leads/{tw_lead['id']}")
    checks += [
        check("Lead-Liste zeigt Website-Umschaltung", list_page.status_code == 200 and all(label.encode("utf-8") in list_page.data for label in ("Alle Websites", "TomorrowWorks", "Autovermietung MOS"))),
        check("Detail zeigt passenden Absender", tw_detail.status_code == 200 and b"info@tomorrowworks-agentur.de" in tw_detail.data and b"KI-Analyse" in tw_detail.data),
    ]

    upload = client.post(
        f"/admin/leads/{tw_lead['id']}/anhaenge",
        data={portal.CSRF_FIELD_NAME: csrf_token, "dateien": (BytesIO(PNG_1X1), "projekt.png")},
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    dateien = portal.list_lead_dateien(tw_lead["id"])
    draft_response = client.post(
        f"/admin/leads/{tw_lead['id']}/ki-entwurf",
        data={portal.CSRF_FIELD_NAME: csrf_token},
        follow_redirects=False,
    )
    tw_after = portal.get_lead(tw_lead["id"])
    checks += [
        check("Lead-Anhang gespeichert", upload.status_code == 302 and len(dateien) == 1 and (portal.UPLOAD_DIR / dateien[0]["stored_name"]).is_file()),
        check("Sicherer Entwurf ohne API-Key", draft_response.status_code == 302 and "TomorrowWorks" in tw_after["mail_text_entwurf"] and tw_after["ki_analyse_text"]),
        check("Nicht-Werkstatt-Lead wird nicht Werkstattauftrag", client.post(f"/admin/leads/{tw_lead['id']}/auftrag", data={portal.CSRF_FIELD_NAME: csrf_token}).status_code == 302 and not portal.get_lead(tw_lead["id"])["auftrag_id"]),
    ]

    portal.save_lead_attachment_bytes(lack_id, "schaden.png", PNG_1X1, "image/png", "website")
    convert = client.post(
        f"/admin/leads/{lack_id}/auftrag",
        data={portal.CSRF_FIELD_NAME: csrf_token},
        follow_redirects=False,
    )
    lack_after = portal.get_lead(lack_id)
    checks += [
        check("Lackierzentrum-Lead wird nicht manuell vorzeitig zum Auftrag", convert.status_code == 302 and not lack_after["auftrag_id"]),
        check("Lackierzentrum-Lead besitzt stattdessen sofort Kundenlink und Anhang", len(lack_after["kunden_status_token"]) >= 18 and len(portal.list_lead_dateien(lack_id)) == 1),
    ]

    print(f"Temporaere Testdaten: {TEMP_DIR}")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
