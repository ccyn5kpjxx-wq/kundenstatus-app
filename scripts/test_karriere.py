# -*- coding: utf-8 -*-
"""Isolierter Regressionstest fuer Stellenanzeigen und Rueckrufbewerbungen."""

from __future__ import annotations

import io
import json
import os
import pathlib
import re
import shutil
import sqlite3
import sys
import tempfile
import zipfile

from werkzeug.datastructures import MultiDict


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="karriere_test_"))
os.environ.update(
    {
        "RENDER": "local-karriere-test",
        "DATABASE_URL": "",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEMP_DIR),
        "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
        "UPLOAD_DIR": str(TEMP_DIR / "uploads"),
        "BACKUP_DIR": str(TEMP_DIR / "backups"),
        "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "LEXWARE_AUTO_SYNC_ENABLED": "0",
        "GOOGLE_ADS_AUTO_SYNC_ENABLED": "0",
        "OPENAI_API_KEY": "",
        "SCHADEN_SMTP_PASS": "",
        "WEBSITE_LEAD_NOTIFICATION_EMAIL": "karriere@example.test",
        "FLASK_SECRET_KEY": "karriere-test-secret",
        "ADMIN_PASS": "karriere-test-pass",
        "PUBLIC_SITE_ONLY": "0",
        "PUBLIC_SITE_INDEXABLE": "0",
        "PUBLIC_BASE_URL": "https://www.example.test",
        "PORTAL_BASE_URL": "",
    }
)

import app as portal  # noqa: E402


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


def clear_form_limit():
    with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
        portal.PUBLIC_FORM_ATTEMPTS.clear()


def csrf_from(client):
    with client.session_transaction() as session:
        return session.get(portal.CSRF_FIELD_NAME)


def bewerbungen_nav(html):
    match = re.search(
        r'<a[^>]+href="/admin/karriere#bewerbungen"[^>]*>Bewerbungen.*?</a>',
        html,
        re.DOTALL,
    )
    return match.group(0) if match else ""


def zip_payload(files):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)
    return stream.getvalue()


def backup_without_table(backup_path, table_name):
    stream = io.BytesIO()
    with zipfile.ZipFile(backup_path) as source, zipfile.ZipFile(
        stream, "w", zipfile.ZIP_DEFLATED
    ) as target:
        for info in source.infolist():
            payload = source.read(info.filename)
            if info.filename == "backup.json":
                export = json.loads(payload.decode("utf-8"))
                export["tables"].pop(table_name, None)
                payload = json.dumps(export, ensure_ascii=False).encode("utf-8")
            target.writestr(info, payload)
    return stream.getvalue()


def main():
    portal.app.config["TESTING"] = True
    portal.schedule_change_backup = lambda _reason: None
    portal.PUBLIC_REQUEST_MAIL_TESTLOG.clear()
    portal.init_db()
    checks = []

    client = portal.app.test_client()
    homepage = client.get("/homepage")
    homepage_html = homepage.get_data(as_text=True)
    career = client.get("/karriere")
    career_html = career.get_data(as_text=True)
    default_jobs = portal.list_stellenanzeigen(nur_aktive=True)
    selected_career = client.get(f"/karriere?stelle={default_jobs[0]['slug']}")
    selected_career_html = selected_career.get_data(as_text=True)
    expected_job_titles = (
        "Ausbildung zum Fahrzeuglackierer (m/w/d)",
        "Ausbildung zum Kfz-Mechatroniker (m/w/d)",
        "Kfz-Mechatroniker / Kfz-Fachkraft (m/w/d)",
        "Fahrzeuglackierer (m/w/d)",
        "Lackierhelfer (m/w/d)",
    )
    checks += [
        check(
            "Fuenf freigegebene Startstellen ab sofort vorhanden",
            len(default_jobs) == 5
            and tuple(job["titel"] for job in default_jobs) == expected_job_titles
            and all("sofort" in job["beschreibung"].lower() for job in default_jobs),
        ),
        check(
            "Homepage zeigt alle Stellen nach den Kundeninhalten und verlinkt den Rueckruf",
            homepage.status_code == 200
            and all(title in homepage_html for title in expected_job_titles)
            and all(
                f"?stelle={job['slug']}#bewerbung" in homepage_html
                for job in default_jobs
            )
            and "Stellenangebote · Ab sofort" in homepage_html
            and "Kurz bewerben &amp; Rückruf anfordern" in homepage_html
            and homepage_html.index('id="vertrauen"')
            < homepage_html.index('id="jobs"')
            < homepage_html.index('id="kontakt"'),
        ),
        check(
            "Karriereseite zeigt Stellen und Initiativbewerbung",
            career.status_code == 200
            and all(job["titel"] in career_html for job in default_jobs)
            and "Initiativbewerbung" in career_html
            and 'name="telefon"' in career_html
            and 'name="datenschutz"' in career_html,
        ),
        check(
            "Kartenklick waehlt Stelle vor und zeigt zuerst Rueckrufdaten",
            selected_career.status_code == 200
            and f'value="{default_jobs[0]["id"]}" checked' in selected_career_html
            and "Name und Telefonnummer – wir rufen Sie zurück." in selected_career_html
            and "Ausgewählt" in selected_career_html
            and selected_career_html.index('for="name"')
            < selected_career_html.index("Stelle ändern oder weitere Stelle auswählen"),
        ),
        check(
            "Neue Karrieredaten sind Teil von Backup v4",
            portal.BACKUP_FORMAT_VERSION == 4
            and {"stellenanzeigen", "bewerbungen"} <= set(portal.BACKUP_TABLES),
        ),
    ]

    old_v3_export = {
        "format_version": 3,
        "tables": {
            table: []
            for table in portal.BACKUP_TABLES
            if table not in {"stellenanzeigen", "bewerbungen", "datei_backups"}
        },
    }
    try:
        portal.validate_backup_binary_reference_completeness(old_v3_export, {})
        old_v3_compatible = True
    except Exception:
        old_v3_compatible = False
    checks.append(check("Backups vor Karriere-v4 bleiben importierbar", old_v3_compatible))

    csrf_token = csrf_from(client)
    invalid_cases = (
        ("Keine Stelle", {"name": "Erika Muster", "telefon": "0176 1234567", "datenschutz": "1"}, "mindestens einen Bereich"),
        ("Name fehlt", {"stellen_id": str(default_jobs[0]["id"]), "name": " ", "telefon": "0176 1234567", "datenschutz": "1"}, "Namen"),
        ("Telefon ungueltig", {"stellen_id": str(default_jobs[0]["id"]), "name": "Erika Muster", "telefon": "abc123", "datenschutz": "1"}, "Telefonnummer"),
        ("Datenschutz fehlt", {"stellen_id": str(default_jobs[0]["id"]), "name": "Erika Muster", "telefon": "0176 1234567"}, "Datenschutzhinweise"),
        ("Unbekannte Stelle", {"stellen_id": "999999", "name": "Erika Muster", "telefon": "0176 1234567", "datenschutz": "1"}, "aktuell veröffentlichte"),
    )
    for label, data, marker in invalid_cases:
        clear_form_limit()
        before = int(scalar("SELECT COUNT(*) FROM bewerbungen") or 0)
        payload = dict(data)
        payload[portal.CSRF_FIELD_NAME] = csrf_token
        payload["website"] = ""
        response = client.post("/karriere/bewerben", data=payload)
        after = int(scalar("SELECT COUNT(*) FROM bewerbungen") or 0)
        checks.append(check(label, response.status_code == 400 and marker in response.get_data(as_text=True) and before == after))

    gueltige_basisdaten = {
        "stellen_id": str(default_jobs[0]["id"]),
        "name": "Erika Muster",
        "telefon": "0176 1234567",
        "datenschutz": "1",
        "website": "",
    }
    clear_form_limit()
    before = int(scalar("SELECT COUNT(*) FROM bewerbungen") or 0)
    missing_csrf = client.post("/karriere/bewerben", data=gueltige_basisdaten)
    checks.append(
        check(
            "Bewerbungsformular verlangt CSRF-Schutz",
            missing_csrf.status_code == 400
            and int(scalar("SELECT COUNT(*) FROM bewerbungen") or 0) == before,
        )
    )

    csrf_token = csrf_from(client)
    honeypot_payload = dict(gueltige_basisdaten)
    honeypot_payload[portal.CSRF_FIELD_NAME] = csrf_token
    honeypot_payload["website"] = "https://spam.example"
    honeypot = client.post("/karriere/bewerben", data=honeypot_payload)
    checks.append(
        check(
            "Honeypot speichert keine Bewerbung",
            honeypot.status_code == 302
            and int(scalar("SELECT COUNT(*) FROM bewerbungen") or 0) == before,
        )
    )

    with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
        portal.PUBLIC_FORM_ATTEMPTS["karriere-bewerbung:127.0.0.1"] = {
            "count": portal.PUBLIC_FORM_RATE_LIMIT_MAX,
            "started_at": portal.time.time(),
        }
    rate_payload = dict(gueltige_basisdaten)
    rate_payload[portal.CSRF_FIELD_NAME] = csrf_token
    rate_limited = client.post("/karriere/bewerben", data=rate_payload)
    checks.append(
        check(
            "Rate-Limit blockiert ohne Datenbankeintrag",
            rate_limited.status_code == 429
            and int(scalar("SELECT COUNT(*) FROM bewerbungen") or 0) == before,
        )
    )
    clear_form_limit()

    valid = client.post(
        "/karriere/bewerben",
        data=MultiDict(
            [
                (portal.CSRF_FIELD_NAME, csrf_token),
                ("stellen_id", str(default_jobs[0]["id"])),
                ("stellen_id", str(default_jobs[1]["id"])),
                ("stellen_id", "initiativ"),
                ("name", "  Erika Muster  "),
                ("telefon", "+49 (176) 123 45 67"),
                ("rueckrufwunsch", "Werktags ab 16 Uhr"),
                ("datenschutz", "1"),
                ("website", ""),
            ]
        ),
        follow_redirects=False,
    )
    bewerbung_id = int(scalar("SELECT MAX(id) FROM bewerbungen") or 0)
    bewerbung = portal.get_bewerbung(bewerbung_id)
    success = client.get(valid.headers.get("Location", "")) if valid.status_code == 302 else None
    success_html = success.get_data(as_text=True) if success else ""
    repeated = client.get("/karriere?gesendet=1")
    checks += [
        check(
            "Mehrfachauswahl wird als Rueckrufbewerbung gespeichert",
            valid.status_code == 302
            and bewerbung
            and bewerbung["name"] == "Erika Muster"
            and bewerbung["telefon"] == "+49 (176) 123 45 67"
            and bewerbung["stellen_ids"] == [default_jobs[0]["id"], default_jobs[1]["id"]]
            and "Initiativbewerbung" in bewerbung["bereiche"]
            and bewerbung["datenschutz_bestaetigt_am"],
        ),
        check(
            "Werkstatt wird ueber neue Bewerbung benachrichtigt",
            bool(portal.PUBLIC_REQUEST_MAIL_TESTLOG)
            and portal.PUBLIC_REQUEST_MAIL_TESTLOG[-1]["kategorie"] == "karriere-bewerbung"
            and portal.PUBLIC_REQUEST_MAIL_TESTLOG[-1]["vorgang_id"] == bewerbung_id,
        ),
        check(
            "Erfolg ist einmalig und verrät keine Kontaktdaten",
            success is not None
            and success.status_code == 200
            and "Ihre Rückrufanfrage ist angekommen" in success_html
            and bewerbung["telefon"] not in success_html
            and "Ihre Rückrufanfrage ist angekommen" not in repeated.get_data(as_text=True),
        ),
    ]

    anonymous_admin = client.get("/admin/karriere")
    checks.append(check("Karriere-Admin ist geschuetzt", anonymous_admin.status_code == 302 and bewerbung["telefon"] not in anonymous_admin.get_data(as_text=True)))

    admin = portal.app.test_client()
    with admin.session_transaction() as session:
        session["admin"] = True
    admin_page = admin.get("/admin/karriere")
    admin_html = admin_page.get_data(as_text=True)
    nav_mit_neuer_bewerbung = bewerbungen_nav(admin_html)
    postfach_html = admin.get("/admin/postfach").get_data(as_text=True)
    admin_csrf = csrf_from(admin)
    checks += [
        check(
            "Geschuetztes Cockpit zeigt Rueckrufdaten",
            admin_page.status_code == 200 and bewerbung["telefon"] in admin_html,
        ),
        check(
            "Bewerbungen haben eine eigene rote Cockpit-Rubrik",
            "Bewerbungen" in nav_mit_neuer_bewerbung
            and 'class="nav-badge"' in nav_mit_neuer_bewerbung
            and ">1</span>" in nav_mit_neuer_bewerbung
            and "is-alert" not in nav_mit_neuer_bewerbung
            and 'aria-label="1 neue Bewerbungen"' in nav_mit_neuer_bewerbung,
        ),
        check(
            "Bewerbungseingang steht vor der Stellenverwaltung",
            admin_html.index('id="bewerbungen"')
            < admin_html.index('id="stellenanzeigen"')
            and "getrennt vom Postfach" in admin_html,
        ),
        check(
            "Bewerberdaten landen nicht im Cockpit-Postfach",
            bewerbung["telefon"] not in postfach_html
            and bewerbung["name"] not in postfach_html,
        ),
    ]

    create_job = admin.post(
        "/admin/karriere/stellen",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "titel": "Werkstatt-Allrounder (m/w/d)",
            "kurzbeschreibung": "Unterstuetzung in mehreren Werkstattbereichen.",
            "beschreibung": "Aufgaben werden im persoenlichen Gespraech abgestimmt.",
            "sortierung": "6",
            "aktiv": "1",
        },
        follow_redirects=False,
    )
    new_job_id = int(scalar("SELECT MAX(id) FROM stellenanzeigen") or 0)
    new_job = portal.get_stellenanzeige(new_job_id)
    public_with_new = client.get("/karriere").get_data(as_text=True)
    checks.append(check("Admin kann eine freie Stellenanzeige anlegen", create_job.status_code == 302 and new_job and new_job["aktiv"] and new_job["titel"] in public_with_new))

    deactivate = admin.post(
        f"/admin/karriere/stellen/{new_job_id}",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "titel": new_job["titel"],
            "kurzbeschreibung": new_job["kurzbeschreibung"],
            "beschreibung": new_job["beschreibung"],
            "sortierung": "6",
        },
        follow_redirects=False,
    )
    public_without_new = client.get("/karriere").get_data(as_text=True)
    checks.append(check("Ausgeblendete Stelle ist nicht oeffentlich", deactivate.status_code == 302 and not portal.get_stellenanzeige(new_job_id)["aktiv"] and new_job["titel"] not in public_without_new))

    clear_form_limit()
    inactive_attempt = client.post(
        "/karriere/bewerben",
        data={
            portal.CSRF_FIELD_NAME: csrf_token,
            "stellen_id": str(new_job_id),
            "name": "Manipulierter Test",
            "telefon": "0176 9999999",
            "datenschutz": "1",
            "website": "",
        },
    )
    checks.append(check("Manipulierte Auswahl einer inaktiven Stelle wird blockiert", inactive_attempt.status_code == 400 and int(scalar("SELECT COUNT(*) FROM bewerbungen") or 0) == 1))

    status_update = admin.post(
        f"/admin/karriere/bewerbungen/{bewerbung_id}",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "status": "kontaktiert",
            "notiz_intern": "Rückruf vereinbart.",
        },
        follow_redirects=False,
    )
    updated = portal.get_bewerbung(bewerbung_id)
    admin_after_contact_html = admin.get("/admin/karriere").get_data(as_text=True)
    nav_ohne_neue_bewerbung = bewerbungen_nav(admin_after_contact_html)
    checks += [
        check(
            "Rueckrufstatus und interne Notiz sind pflegbar",
            status_update.status_code == 302
            and updated["status"] == "kontaktiert"
            and updated["notiz_intern"] == "Rückruf vereinbart."
            and portal.admin_bewerbungen_count() == 0,
        ),
        check(
            "Rote Zahl verschwindet erst nach Bearbeitung",
            "Bewerbungen" in nav_ohne_neue_bewerbung
            and "nav-badge" not in nav_ohne_neue_bewerbung
            and "Keine neue Bewerbung" in admin_after_contact_html,
        ),
    ]

    delete = admin.post(
        f"/admin/karriere/bewerbungen/{bewerbung_id}/loeschen",
        data={portal.CSRF_FIELD_NAME: admin_csrf},
        follow_redirects=False,
    )
    checks.append(check("Bewerber-Kontaktdaten koennen geloescht werden", delete.status_code == 302 and portal.get_bewerbung(bewerbung_id) is None))

    original_public_only = portal.PUBLIC_SITE_ONLY
    original_public_base = portal.PUBLIC_BASE_URL
    original_public_hosts = set(portal.PUBLIC_HOSTS)
    original_portal_base = portal.PORTAL_BASE_URL
    original_indexable = portal.PUBLIC_SITE_INDEXABLE
    try:
        portal.PUBLIC_SITE_ONLY = False
        portal.PUBLIC_BASE_URL = "http://localhost"
        portal.PUBLIC_HOSTS = {"localhost"}
        portal.PORTAL_BASE_URL = "http://localhost"
        combined_client = portal.app.test_client()
        combined_get = combined_client.get(
            f"/karriere?stelle={default_jobs[0]['slug']}"
        )
        checks.append(
            check(
                "Lokale gemeinsame Origin erzeugt keine Weiterleitungsschleife",
                combined_get.status_code == 200
                and not combined_get.headers.get("Location")
                and f'value="{default_jobs[0]["id"]}" checked'
                in combined_get.get_data(as_text=True),
            )
        )
        portal.PUBLIC_SITE_ONLY = True
        portal.PORTAL_BASE_URL = "https://portal.example.test"
        split_client = portal.app.test_client()
        split_get = split_client.get("/karriere?stelle=initiativ")
        split_post = split_client.post("/karriere/bewerben", data={})
        checks += [
            check("Datenbanklose Homepage leitet Karriere zum Portal", split_get.status_code == 302 and split_get.headers.get("Location", "").startswith("https://portal.example.test/karriere")),
            check("Datenbanklose Homepage speichert keine Bewerbung", split_post.status_code == 404),
        ]
        portal.PORTAL_BASE_URL = ""
        checks.append(check("Fehlende Portal-Verbindung zeigt klaren Fehler", split_client.get("/karriere").status_code == 503))
    finally:
        portal.PUBLIC_SITE_ONLY = original_public_only
        portal.PUBLIC_BASE_URL = original_public_base
        portal.PUBLIC_HOSTS = original_public_hosts
        portal.PORTAL_BASE_URL = original_portal_base
        portal.PUBLIC_SITE_INDEXABLE = original_indexable

    json_sentinel_id = portal.create_stellenanzeige(
        {
            "titel": "Nur vor v3-JSON-Import vorhanden",
            "kurzbeschreibung": "Dieser Datensatz muss beim Import verschwinden.",
            "beschreibung": "Sentinel für den vollständigen Tabellenersatz.",
            "aktiv": 1,
            "sortierung": 900,
        }
    )
    v3_json_package = zip_payload(
        {
            "backup.json": json.dumps(
                old_v3_export, ensure_ascii=False
            ).encode("utf-8")
        }
    )
    v3_json_import = admin.post(
        "/admin/daten-import",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "datenpaket": (io.BytesIO(v3_json_package), "backup-v3-json.zip"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    json_jobs = portal.list_stellenanzeigen(nur_aktive=True)
    json_import_html = v3_json_import.get_data(as_text=True)
    checks.append(
        check(
            "Echter v3-JSON-Import migriert und setzt Standardstellen neu",
            v3_json_import.status_code == 200
            and "Daten wurden importiert" in json_import_html
            and len(json_jobs) == 5
            and len(portal.list_stellenanzeigen()) == 5
            and portal.get_stellenanzeige(json_sentinel_id) is None
            and client.get("/karriere").status_code == 200
            and admin.get("/admin/karriere").status_code == 200,
        )
    )

    old_sqlite = TEMP_DIR / "backup-v3.db"
    source = sqlite3.connect(portal.DB)
    target = sqlite3.connect(old_sqlite)
    try:
        source.backup(target)
    finally:
        source.close()
        target.close()
    with sqlite3.connect(old_sqlite) as db:
        db.execute("DROP TABLE bewerbungen")
        db.execute("DROP TABLE stellenanzeigen")
        db.commit()
    sqlite_sentinel_id = portal.create_stellenanzeige(
        {
            "titel": "Nur vor v3-SQLite-Import vorhanden",
            "kurzbeschreibung": "Dieser WAL-Datensatz darf nicht zurückkehren.",
            "beschreibung": "Sentinel gegen alte SQLite-WAL-Seitendateien.",
            "aktiv": 1,
            "sortierung": 901,
        }
    )
    v3_sqlite_package = zip_payload({"auftraege.db": old_sqlite.read_bytes()})
    v3_sqlite_import = admin.post(
        "/admin/daten-import",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "datenpaket": (io.BytesIO(v3_sqlite_package), "backup-v3-sqlite.zip"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    sqlite_jobs = portal.list_stellenanzeigen(nur_aktive=True)
    sqlite_import_html = v3_sqlite_import.get_data(as_text=True)
    checks.append(
        check(
            "Echter alter SQLite-Import legt Karriere-Tabellen sofort an",
            v3_sqlite_import.status_code == 200
            and "Daten wurden importiert" in sqlite_import_html
            and len(sqlite_jobs) == 5
            and len(portal.list_stellenanzeigen()) == 5
            and portal.get_stellenanzeige(sqlite_sentinel_id) is None
            and client.get("/karriere").status_code == 200
            and admin.get("/admin/karriere").status_code == 200,
        )
    )

    roundtrip_job_id = portal.create_stellenanzeige(
        {
            "titel": "Inaktive Teststelle (m/w/d)",
            "kurzbeschreibung": "Nur für den Backup-Roundtrip.",
            "beschreibung": "Diese Stelle bleibt beim Wiederherstellen vollständig erhalten.",
            "aktiv": 0,
            "sortierung": 77,
        }
    )
    roundtrip_application_id = portal.create_bewerbung(
        [sqlite_jobs[0]["id"]],
        [sqlite_jobs[0]["titel"]],
        "Backup Bewerber",
        "+49 6261 123456",
        "Bitte vormittags zurückrufen.",
    )
    portal.update_bewerbung(
        roundtrip_application_id,
        "kontaktiert",
        "Telefonat für Montag vereinbart.",
    )
    expected_job = portal.get_stellenanzeige(roundtrip_job_id)
    expected_application = portal.get_bewerbung(roundtrip_application_id)
    v4_backup = portal.create_backup_package("karriere-v4-roundtrip")
    db = portal.get_db()
    try:
        db.execute("DELETE FROM bewerbungen WHERE id=?", (roundtrip_application_id,))
        db.execute("DELETE FROM stellenanzeigen WHERE id=?", (roundtrip_job_id,))
        db.commit()
    finally:
        db.close()
    v4_restore = admin.post(
        "/admin/daten-import",
        data={
            portal.CSRF_FIELD_NAME: admin_csrf,
            "datenpaket": (io.BytesIO(v4_backup.read_bytes()), v4_backup.name),
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    restored_job = portal.get_stellenanzeige(roundtrip_job_id)
    restored_application = portal.get_bewerbung(roundtrip_application_id)
    job_fields = (
        "titel",
        "kurzbeschreibung",
        "beschreibung",
        "aktiv",
        "sortierung",
        "erstellt_am",
        "geaendert_am",
    )
    application_fields = (
        "stellen_ids_json",
        "bereiche",
        "name",
        "telefon",
        "rueckrufwunsch",
        "datenschutz_bestaetigt_am",
        "status",
        "notiz_intern",
        "erstellt_am",
        "geaendert_am",
    )
    checks.append(
        check(
            "Backup v4 stellt Stellen und Bewerbungen vollständig wieder her",
            v4_restore.status_code == 302
            and restored_job
            and restored_application
            and all(restored_job[field] == expected_job[field] for field in job_fields)
            and all(
                restored_application[field] == expected_application[field]
                for field in application_fields
            ),
        )
    )

    sentinel_id = portal.create_bewerbung(
        [],
        ["Initiativbewerbung"],
        "Import Wächter",
        "0152 0000000",
        "Nicht durch ein ungültiges Paket löschen.",
    )
    for missing_table in ("stellenanzeigen", "bewerbungen"):
        rejected = admin.post(
            "/admin/daten-import",
            data={
                portal.CSRF_FIELD_NAME: admin_csrf,
                "datenpaket": (
                    io.BytesIO(backup_without_table(v4_backup, missing_table)),
                    f"backup-v4-ohne-{missing_table}.zip",
                ),
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        rejected_html = rejected.get_data(as_text=True)
        checks.append(
            check(
                f"Manipuliertes Backup v4 ohne {missing_table} wird abgelehnt",
                rejected.status_code == 200
                and "Tabellen fehlen" in rejected_html
                and missing_table in rejected_html
                and portal.get_bewerbung(sentinel_id) is not None,
            )
        )

    return 0 if all(checks) else 1


if __name__ == "__main__":
    try:
        exit_code = main()
    finally:
        shutil.rmtree(TEMP_DIR, ignore_errors=True)
    raise SystemExit(exit_code)
