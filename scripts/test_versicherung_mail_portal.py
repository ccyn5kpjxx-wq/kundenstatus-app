"""Regressionstest: Eine versendete Versicherungs-Mail schaltet den Portalvorgang frei."""

from __future__ import annotations

import os
import pathlib
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="versicherung_mail_portal_"))
os.environ.update(
    {
        "RENDER": "local-versicherung-mail-portal-test",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEMP_DIR),
        "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
        "UPLOAD_DIR": str(TEMP_DIR / "uploads"),
        "BACKUP_DIR": str(TEMP_DIR / "backups"),
        "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "FLASK_SECRET_KEY": "versicherung-mail-portal-test",
        "ADMIN_PASS": "versicherung-mail-portal-test",
    }
)

import app as portal  # noqa: E402


def check(label, condition):
    if not condition:
        raise AssertionError(label)
    print(f"[OK] {label}")


def main():
    portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
    portal.init_db()
    versicherung = portal.create_versicherung(
        "Portal Testversicherung",
        zugangscode="PORTAL26",
        email="schaden@example.test",
    )
    versicherung_id = versicherung["id"]
    auftrag_id = portal.create_auftrag(
        "intern",
        versicherung_id=versicherung_id,
        kunde_name="Simone Test",
        fahrzeug="Opel Corsa",
        kennzeichen="MOS-T 252",
        versicherung_email="schaden@example.test",
        versicherung_freigabe_status="vorbereitet",
    )

    admin = portal.app.test_client()
    with admin.session_transaction() as session:
        session["admin"] = True
        session[portal.CSRF_FIELD_NAME] = "test-csrf"
    versicherung_client = portal.app.test_client()
    with versicherung_client.session_transaction() as session:
        session["versicherung_id"] = versicherung_id

    alter_auftrag_id = portal.create_auftrag(
        "intern",
        versicherung_id=versicherung_id,
        kunde_name="Historischer Mailfall",
        fahrzeug="Opel Corsa",
        kennzeichen="MOS-ALT 1",
        versicherung_email="schaden@example.test",
        versicherung_freigabe_status="vorbereitet",
    )
    portal.add_chat_nachricht(
        alter_auftrag_id,
        "werkstatt",
        "E-Mail an Versicherung gesendet: Historischer Kostenvoranschlag",
    )
    portal.init_db()
    alter_auftrag = portal.get_auftrag(alter_auftrag_id)
    alter_portal_url = (
        f"/versicherung/{versicherung['slug']}/auftrag/{alter_auftrag_id}"
    )
    check(
        "Frueher versendete Versicherungs-Mail wird beim Start nachgezogen",
        alter_auftrag["versicherung_portal_freigabe_id"] == versicherung_id
        and bool(alter_auftrag["versicherung_sendefreigabe_am"])
        and versicherung_client.get(alter_portal_url).status_code == 200,
    )

    portal_url = f"/versicherung/{versicherung['slug']}/auftrag/{auftrag_id}"
    check(
        "Noch nicht versendeter Fall bleibt im Versicherungsportal gesperrt",
        versicherung_client.get(portal_url).status_code == 404,
    )

    original_mail_status = portal.schaden_mail_status
    original_schadenmail = portal.send_versicherung_schadenmail
    portal.schaden_mail_status = lambda: {"smtp_configured": True, "missing": []}
    portal.send_versicherung_schadenmail = lambda *_args, **_kwargs: {
        "sent": True,
        "attachments": 0,
    }
    try:
        response = admin.post(
            f"/admin/versicherung/schaden/{auftrag_id}/mail/senden",
            data={
                portal.CSRF_FIELD_NAME: "test-csrf",
                "versicherung_email": "schaden@example.test",
                "versicherung_mail_betreff": "Rueckfrage zum Schadenfall",
                "versicherung_mail_text": "Bitte pruefen Sie die Unterlagen.",
            },
            follow_redirects=False,
        )
    finally:
        portal.schaden_mail_status = original_mail_status
        portal.send_versicherung_schadenmail = original_schadenmail

    auftrag = portal.get_auftrag(auftrag_id)
    check("Mailroute leitet nach erfolgreichem Versand weiter", response.status_code == 302)
    check(
        "Versicherungszuordnung wird als Portal-Freigabe gespeichert",
        auftrag["versicherung_portal_freigabe_id"] == versicherung_id,
    )
    check(
        "Versandzeit schaltet den Portalzugang frei",
        bool(auftrag["versicherung_sendefreigabe_am"]),
    )
    check(
        "Eine normale Nachricht gilt nicht automatisch als komplette Schadenmeldung",
        not auftrag["versicherung_gemeldet_am"],
    )
    check(
        "Versicherung kann den Vorgang nach der Mail oeffnen",
        versicherung_client.get(portal_url).status_code == 200,
    )


if __name__ == "__main__":
    main()
