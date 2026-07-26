# -*- coding: utf-8 -*-
"""Isolierter Integrationstest für Bilder im öffentlichen Anfrageformular."""

from __future__ import annotations

import base64
from io import BytesIO
import os
import pathlib
import sys
import tempfile

from werkzeug.datastructures import FileStorage, MultiDict


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="homepage_bilder_"))
os.environ.update(
    {
        "RENDER": "local-homepage-bilder-test",
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
        "FLASK_SECRET_KEY": "homepage-bilder-test",
        "ADMIN_PASS": "homepage-bilder-test",
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


def main():
    portal.app.config["TESTING"] = True
    client = portal.app.test_client()
    checks = []

    form = client.get("/anfrage?anliegen=dellenreparatur")
    form_html = form.get_data(as_text=True)
    checks += [
        check("Anfrageformular erreichbar", form.status_code == 200),
        check("Mehrfachauswahl sichtbar", all(marker in form_html for marker in (
            'enctype="multipart/form-data"', 'name="bilder"', 'multiple', "Bis zu 5 Bilder",
        ))),
        check("Datenschutzhinweis nennt Bilder", "optionalen Bilder" in form_html),
    ]

    with client.session_transaction() as session:
        csrf_token = session.get(portal.CSRF_FIELD_NAME)

    response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "dellenreparatur"),
            ("name", "Bildtest Kunde"),
            ("telefon", "0171 1234567"),
            ("email", ""),
            ("fahrzeug", "VW Golf"),
            ("wunschdatum", ""),
            ("nachricht", "Delle an der hinteren Tür"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "schaden-gesamt.png")),
            ("bilder", (BytesIO(PNG_1X1), "schaden-detail.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    lead = portal.get_lead(lead_id) if lead_id else None
    lead_dateien = portal.list_lead_dateien(lead_id) if lead_id else []
    lead_paths = [portal.UPLOAD_DIR / item["stored_name"] for item in lead_dateien]
    checks += [
        check("Anfrage erzeugt Lead", response.status_code == 302 and lead and lead["quelle"] == "website"),
        check("Beide Bilder gespeichert", len(lead_dateien) == 2 and all(
            path.is_file() and path.read_bytes() == PNG_1X1 for path in lead_paths
        )),
        check("Keine Standard-Auftragsdatei vor Umwandlung", scalar("SELECT COUNT(*) FROM dateien") == 0),
    ]

    geschuetzt = client.get(f"/admin/leads/{lead_id}/anhaenge/{lead_dateien[0]['id']}")
    checks.append(check("Bildroute ist geschützt", geschuetzt.status_code == 302))

    with client.session_transaction() as session:
        session["admin"] = True
        admin_csrf = session.get(portal.CSRF_FIELD_NAME)
    detail = client.get(f"/admin/leads/{lead_id}")
    detail_html = detail.get_data(as_text=True)
    bild_antworten = [
        client.get(f"/admin/leads/{lead_id}/anhaenge/{item['id']}") for item in lead_dateien
    ]
    checks += [
        check("Bilder im Lead sichtbar", detail.status_code == 200 and all(
            item["original_name"] in detail_html for item in lead_dateien
        )),
        check("Geschützte Routen liefern Originale", all(
            item.status_code == 200 and item.data == PNG_1X1 for item in bild_antworten
        )),
    ]
    for bild_antwort in bild_antworten:
        bild_antwort.close()

    umwandlung = client.post(
        f"/admin/leads/{lead_id}/auftrag",
        data={portal.CSRF_FIELD_NAME: admin_csrf},
        follow_redirects=False,
    )
    lead_nachher = portal.get_lead(lead_id)
    auftrag_id = int(lead_nachher["auftrag_id"] or 0)
    auftrag_dateien = rows(
        "SELECT * FROM dateien WHERE auftrag_id=? AND kategorie='leadbild' ORDER BY id",
        (auftrag_id,),
    )
    auftrag_paths = [portal.UPLOAD_DIR / item["stored_name"] for item in auftrag_dateien]
    checks += [
        check("Lead wird Auftrag", umwandlung.status_code == 302 and auftrag_id > 0),
        check("Alle Bilder werden unabhängig kopiert", len(auftrag_dateien) == 2 and all(
            path.is_file() and path.read_bytes() == PNG_1X1 for path in auftrag_paths
        ) and not ({item["stored_name"] for item in lead_dateien} & {item["stored_name"] for item in auftrag_dateien})),
        check("Bilder bleiben in Lead-Historie", len(portal.list_lead_dateien(lead_id)) == 2),
    ]

    deleted = portal.delete_lead(lead_id)
    checks += [
        check("Lead-Löschung entfernt nur Lead-Bilder", deleted and all(not path.exists() for path in lead_paths)),
        check("Auftragsbilder bleiben erhalten", all(path.is_file() for path in auftrag_paths)),
    ]

    zu_viele = [
        FileStorage(stream=BytesIO(PNG_1X1), filename=f"bild-{index}.png", content_type="image/png")
        for index in range(6)
    ]
    valid, errors = portal.validate_website_lead_uploads(zu_viele)
    falscher_inhalt = FileStorage(
        stream=BytesIO(b"kein echtes bild"), filename="falsch.jpg", content_type="image/jpeg"
    )
    invalid_valid, invalid_errors = portal.validate_website_lead_uploads([falscher_inhalt])
    checks += [
        check("Mehr als fünf Bilder werden abgewiesen", not valid and any("höchstens 5" in error for error in errors)),
        check("Gefälschte Bilddatei wird abgewiesen", not invalid_valid and any(
            "Dateiinhalt" in error for error in invalid_errors
        )),
    ]

    print(f"Temporäre Testdaten: {TEMP_DIR}")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
