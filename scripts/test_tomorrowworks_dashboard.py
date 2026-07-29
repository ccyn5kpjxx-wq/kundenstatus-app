from __future__ import annotations

import io
import re
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.request import urlopen

from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.test import Client
from werkzeug.wrappers import Response

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tomorrowworks_dashboard import create_app


def csrf(client, path: str) -> str:
    response = client.get(path, follow_redirects=True)
    if response.status_code != 200:
        response = client.get("/", follow_redirects=True)
    assert response.status_code == 200
    match = re.search(rb'name="_csrf_token" value="([^"]+)"', response.data)
    assert match, f"Kein CSRF-Token auf {path}"
    return match.group(1).decode("utf-8")


def post(client, path: str, data: dict, *, follow_redirects: bool = True, content_type: str | None = None):
    data = dict(data)
    data["_csrf_token"] = csrf(client, path)
    return client.post(path, data=data, follow_redirects=follow_redirects, content_type=content_type)


def run() -> None:
    with tempfile.TemporaryDirectory(prefix="tw-dashboard-test-") as temp_dir:
        temp = Path(temp_dir)
        database = temp / "dashboard.db"
        uploads = temp / "uploads"
        project_dir = temp / "kunde-a-website"
        project_dir.mkdir()
        (project_dir / "index.html").write_text(
            "<!doctype html><title>Kunde A Vorschau</title><h1>Interne Vorschau</h1>",
            encoding="utf-8",
        )
        for command in (
            ["git", "init"],
            ["git", "config", "user.name", "Makita Höfer"],
            ["git", "config", "user.email", "team1@example.test"],
            ["git", "add", "index.html"],
            ["git", "commit", "-m", "feat: add customer preview"],
        ):
            subprocess.run(command, cwd=project_dir, check=True, capture_output=True, text=True)
        app = create_app(
            {
                "TESTING": True,
                "DATABASE": str(database),
                "UPLOAD_FOLDER": str(uploads),
                "SECRET_KEY": "test-secret-not-for-production",
                "GIT_MONITOR_ENABLED": False,
            }
        )
        client = app.test_client()

        with app.app_context():
            from tomorrowworks_dashboard.app import get_db

            configured_db = get_db()
            assert configured_db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            assert configured_db.execute("PRAGMA busy_timeout").fetchone()[0] >= 30_000

        response = client.get("/", follow_redirects=True)
        assert response.status_code == 200
        assert "Ersten Admin anlegen" in response.get_data(as_text=True)
        assert "Passwort anzeigen" in response.get_data(as_text=True)

        response = post(
            client,
            "/einrichtung",
            {"name": "Christopher Gärtner", "email": "admin@example.test", "passwort": "sicher-test-123"},
        )
        assert response.status_code == 200
        assert "Projektübersicht" not in response.get_data(as_text=True)
        assert "Wer arbeitet woran?" in response.get_data(as_text=True)

        for index, name in enumerate(("Makita Höfer", "Marcel Gärtner", "Mara Beispiel"), start=1):
            response = post(
                client,
                "/team/neu",
                {
                    "name": name,
                    "email": f"team{index}@example.test",
                    "passwort": "team-test-123",
                    "rolle": "team",
                    "farbe": "#5b8def",
                },
            )
            assert response.status_code == 200
            assert name in response.get_data(as_text=True)

        invalid_website = post(
            client,
            "/kunden/neu",
            {
                "firma": "Falsche Website GmbH",
                "ansprechpartner": "Anna Beispiel",
                "email": "anna@example.test",
                "telefon": "06261 12345",
                "adresse": "Musterstraße 12, 74821 Mosbach",
                "website": "anna@example.test",
                "status": "aktiv",
            },
        )
        assert invalid_website.status_code == 200
        invalid_body = invalid_website.get_data(as_text=True)
        assert "keine E-Mail-Adresse" in invalid_body
        assert 'type="url"' in invalid_body
        assert 'autocomplete="url"' in invalid_body
        validation_connection = sqlite3.connect(database)
        try:
            assert validation_connection.execute("SELECT COUNT(*) FROM kunden").fetchone()[0] == 0
        finally:
            validation_connection.close()

        response = post(
            client,
            "/kunden/neu",
            {
                "firma": "Kunde A GmbH",
                "ansprechpartner": "Anna Beispiel",
                "email": "anna@example.test",
                "telefon": "+49 171 1234567",
                "adresse": "Musterstraße 12, 74821 Mosbach",
                "website": "https://example.test",
                "whatsapp_freigabe": "1",
                "notizen": "Wünscht eine klare, mobile Website.",
                "status": "aktiv",
            },
            follow_redirects=False,
        )
        assert response.status_code == 302
        kunde_id = int(response.headers["Location"].rstrip("/").split("/")[-1])

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        team = connection.execute("SELECT id, name FROM teammitglieder ORDER BY id").fetchall()
        customer = connection.execute("SELECT * FROM kunden WHERE id = ?", (kunde_id,)).fetchone()
        ticket = connection.execute("SELECT * FROM kunden_tickets WHERE kunde_id = ?", (kunde_id,)).fetchone()
        connection.close()
        assert customer["adresse"] == "Musterstraße 12, 74821 Mosbach"
        assert customer["whatsapp_freigabe"] == 1
        assert ticket is not None
        assert len(ticket["token"]) >= 40
        team_ids = [str(row["id"]) for row in team[:2]]

        response = post(
            client,
            "/projekte/neu",
            {
                "kunde_id": str(kunde_id),
                "titel": "Neue Unternehmenswebsite",
                "typ": "website",
                "beschreibung": "Relaunch mit Kontaktformular.",
                "status": "in_arbeit",
                "prioritaet": "hoch",
                "startdatum": "2026-07-28",
                "zieldatum": "2026-08-15",
                "fortschritt": "35",
                "aktuelle_aufgabe": "Startseite mobil fertigstellen",
                "blockiert_grund": "",
                "vorschau_url": "",
                "repo_url": "",
                "lokaler_pfad": str(project_dir),
                "preview_pfad": ".",
                "teammitglieder": team_ids,
                "hauptverantwortlich": team_ids[1],
            },
            follow_redirects=False,
        )
        assert response.status_code == 302
        projekt_id = int(response.headers["Location"].rstrip("/").split("/")[-1])

        response = client.get(f"/projekte/{projekt_id}")
        body = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "Neue Unternehmenswebsite" in body
        assert "Startseite mobil fertigstellen" in body
        assert "Interne Team-Vorschau" in body
        assert "Noch nicht veröffentlicht" in body
        assert "feat: add customer preview" in body
        assert "Codex-/Claude-Übergabe" in body

        response = post(client, f"/projekte/{projekt_id}/git-synchronisieren", {})
        assert response.status_code == 200
        assert "Git-Stand wurde aktualisiert" in response.get_data(as_text=True)

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        projekt = connection.execute("SELECT * FROM projekte WHERE id = ?", (projekt_id,)).fetchone()
        connection.close()
        assert projekt["git_kurz"]
        assert projekt["git_author"] == "Makita Höfer"
        assert projekt["git_status"] == "kein_upstream"
        agent_token = projekt["agent_token"]

        status_response = client.get("/api/projekte/status")
        assert status_response.status_code == 200
        status_payload = status_response.get_json()
        assert status_payload["ok"] is True
        assert status_payload["projekte"][0]["git_status"] == "kein_upstream"

        denied = client.post(
            f"/api/agent/projekte/{projekt_id}/update",
            headers={"Authorization": "Bearer falsch"},
            json={"status": "in_arbeit"},
        )
        assert denied.status_code == 401

        agent_response = client.post(
            f"/api/agent/projekte/{projekt_id}/update",
            headers={"Authorization": f"Bearer {agent_token}"},
            json={
                "agent": "Codex",
                "status": "in_arbeit",
                "fortschritt": 55,
                "aktuelle_aufgabe": "Mobile Navigation prüfen",
                "notiz": "Startseite und Kontaktformular aktualisiert.",
                "git_branch": "main",
                "git_commit": "a" * 40,
                "git_kurz": "aaaaaaa",
                "git_author": "Makita Höfer",
                "git_author_email": "team1@example.test",
                "git_nachricht": "feat: update mobile navigation",
                "git_geaendert_am": "2026-07-28T12:00:00+02:00",
                "git_dirty": 0,
                "kunden_update": "Die mobile Navigation wurde verbessert und ist jetzt leichter bedienbar.",
            },
        )
        assert agent_response.status_code == 200
        assert agent_response.get_json()["ok"] is True

        preview_started = post(
            client,
            f"/projekte/{projekt_id}/vorschau/starten",
            {"preview_pfad": "."},
            follow_redirects=False,
        )
        assert preview_started.status_code == 302
        connection = sqlite3.connect(database)
        preview_port = connection.execute(
            "SELECT preview_port FROM projekte WHERE id = ?", (projekt_id,)
        ).fetchone()[0]
        connection.close()
        try:
            with urlopen(f"http://127.0.0.1:{preview_port}/", timeout=3) as preview_response:
                assert "Interne Vorschau" in preview_response.read().decode("utf-8")
        finally:
            stopped = post(
                client,
                f"/projekte/{projekt_id}/vorschau/stoppen",
                {},
                follow_redirects=False,
            )
            assert stopped.status_code == 302

        portal_path = f"/portal/{ticket['token']}"
        public_page = client.get(portal_path)
        assert public_page.status_code == 200
        assert "Ihr Projektraum" in public_page.get_data(as_text=True)
        assert "Neue Unternehmenswebsite" in public_page.get_data(as_text=True)
        assert "mobile Navigation wurde verbessert" in public_page.get_data(as_text=True)
        assert public_page.headers["Cache-Control"] == "no-store, private"
        assert public_page.headers["X-Robots-Tag"] == "noindex, nofollow, noarchive"
        assert client.get("/portal/falscher-token").status_code == 404

        customer_message_token = csrf(client, portal_path)
        customer_message = client.post(
            f"{portal_path}/nachricht",
            data={
                "_csrf_token": customer_message_token,
                "projekt_id": str(projekt_id),
                "text": "Bitte verwenden Sie unser neues Logo und machen Sie die Telefonnummer größer.",
                "dateien": (io.BytesIO(b"\x89PNG\r\n\x1a\nlogo"), "logo-neu.png"),
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert customer_message.status_code == 200
        assert "Vielen Dank" in customer_message.get_data(as_text=True)

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        assert connection.execute(
            "SELECT COUNT(*) FROM portal_nachrichten WHERE ticket_id = ? AND absender = 'kunde' AND gelesen_team = 0",
            (ticket["id"],),
        ).fetchone()[0] == 1
        portal_file = connection.execute(
            "SELECT * FROM portal_dateien WHERE ticket_id = ?", (ticket["id"],)
        ).fetchone()
        connection.close()
        assert portal_file is not None
        assert client.get(f"{portal_path}/dateien/{portal_file['id']}").status_code == 200

        dashboard_with_update = client.get("/")
        assert "Neue Nachricht" in dashboard_with_update.get_data(as_text=True)
        assert "Kunde A GmbH" in dashboard_with_update.get_data(as_text=True)

        internal_ticket = client.get(f"/kunden/{kunde_id}/ticket")
        internal_body = internal_ticket.get_data(as_text=True)
        assert internal_ticket.status_code == 200
        assert "Bitte verwenden Sie unser neues Logo" in internal_body
        assert "Kundenlink kopieren" in internal_body
        assert "Lokale Vorschau des Kundenlinks" in internal_body
        assert "WhatsApp noch nicht bereit" in internal_body
        assert "Per E-Mail senden" in internal_body

        team_message_token = csrf(client, f"/kunden/{kunde_id}/ticket")
        team_message = client.post(
            f"/kunden/{kunde_id}/ticket/nachricht",
            data={
                "_csrf_token": team_message_token,
                "projekt_id": str(projekt_id),
                "art": "anforderung",
                "text": "Bitte laden Sie zusätzlich die offene Logo-Datei hoch.",
                "dateien": (io.BytesIO(b"PK\x03\x04docx"), "marken-briefing.docx"),
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert team_message.status_code == 200
        assert "Eintrag wurde veröffentlicht" in team_message.get_data(as_text=True)
        assert "offene Logo-Datei" in client.get(portal_path).get_data(as_text=True)

        offer_page = client.get(f"/kunden/{kunde_id}/ticket/angebot")
        assert offer_page.status_code == 200
        assert "Sicherer Prüfmodus aktiv" in offer_page.get_data(as_text=True)
        offer_saved = post(
            client,
            f"/kunden/{kunde_id}/ticket/angebot",
            {
                "paket_name": "Website mit KI und Betreuung",
                "einmalig": "3000,00",
                "monatlich": "300,00",
                "einrichtung": "0,00",
                "laufzeit_monate": "12",
                "kuendigungsfrist_monate": "3",
                "leistungsumfang": "Website, KI-Funktion, Dashboard, Kalender und laufende Abstimmung.",
                "hinweise": "Externe API-Nutzung nach transparentem Nachweis.",
                "gueltig_bis": "2026-08-31",
            },
        )
        assert "Angebotsentwurf wurde gespeichert" in offer_saved.get_data(as_text=True)
        offer_sent = post(client, f"/kunden/{kunde_id}/ticket/angebot/senden", {})
        assert offer_sent.status_code == 200
        assert "Angebot wurde veröffentlicht" in offer_sent.get_data(as_text=True)

        public_offer = client.get(portal_path)
        public_offer_body = public_offer.get_data(as_text=True)
        assert "Website mit KI und Betreuung" in public_offer_body
        assert "3.000,00 €" in public_offer_body
        assert "300,00 €" in public_offer_body
        assert "Angebot zur Vertragsprüfung freigeben" in public_offer_body
        assert "noch kein rechtsverbindlicher Online-Vertrag" in public_offer_body

        offer_approved = post(
            client,
            f"{portal_path}/angebot/freigeben",
            {"name": "Anna Beispiel", "freigabe": "1"},
        )
        assert offer_approved.status_code == 200
        assert "Freigabe wurde gespeichert" in offer_approved.get_data(as_text=True)

        connection = sqlite3.connect(database)
        assert connection.execute("SELECT status FROM ticket_angebote").fetchone()[0] == "freigegeben"
        assert connection.execute(
            "SELECT COUNT(*) FROM portal_nachrichten WHERE absender = 'kunde' AND gelesen_team = 0"
        ).fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM portal_benachrichtigungen").fetchone()[0] >= 6
        assert connection.execute(
            "SELECT COUNT(*) FROM portal_benachrichtigungen WHERE status IN ('keine_domain', 'nicht_konfiguriert')"
        ).fetchone()[0] >= 4
        connection.close()
        assert client.get("/tickets").status_code == 200
        assert "Kunden-Tickets" in client.get("/tickets").get_data(as_text=True)
        client.get(f"/kunden/{kunde_id}/ticket")

        for path in (
            "/kunden",
            f"/kunden/{kunde_id}",
            f"/kunden/{kunde_id}/bearbeiten",
            f"/kunden/{kunde_id}/ticket",
            f"/kunden/{kunde_id}/ticket/angebot",
            "/tickets",
            "/team",
            f"/team/{team[0]['id']}/bearbeiten",
            f"/projekte/{projekt_id}/bearbeiten",
        ):
            page = client.get(path)
            assert page.status_code == 200, path

        response = post(
            client,
            f"/projekte/{projekt_id}/status",
            {
                "status": "interne_pruefung",
                "fortschritt": "70",
                "aktuelle_aufgabe": "Interne Abnahme durchführen",
                "blockiert_grund": "",
            },
        )
        assert "Interne Prüfung" in response.get_data(as_text=True)

        response = post(
            client,
            f"/projekte/{projekt_id}/notiz",
            {"text": "Kunde erhält die Vorschau nach der internen Prüfung."},
        )
        assert "Kunde erhält die Vorschau" in response.get_data(as_text=True)

        upload_token = csrf(client, f"/projekte/{projekt_id}")
        response = client.post(
            f"/projekte/{projekt_id}/datei",
            data={
                "_csrf_token": upload_token,
                "datei": (io.BytesIO(b"%PDF-1.4\n% test"), "entwurf.pdf"),
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert response.status_code == 200
        assert "entwurf.pdf" in response.get_data(as_text=True)
        assert len(list(uploads.glob("*.pdf"))) == 1

        response = post(
            client,
            "/mein-fokus",
            {"projekt_id": str(projekt_id), "fokus_notiz": "Interne Abnahme"},
        )
        assert "Interne Abnahme" in response.get_data(as_text=True)

        response = post(client, "/abmelden", {})
        assert "Willkommen zurück" in response.get_data(as_text=True)
        response = post(
            client,
            "/anmelden",
            {"email": "team1@example.test", "passwort": "team-test-123"},
        )
        assert response.status_code == 200
        assert "Wer arbeitet woran?" in response.get_data(as_text=True)

        health = client.get("/healthz")
        assert health.status_code == 200
        assert health.get_json()["ok"] is True

        connection = sqlite3.connect(database)
        assert connection.execute("SELECT COUNT(*) FROM kunden").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM projekte").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM teammitglieder").fetchone()[0] == 4
        assert connection.execute("SELECT COUNT(*) FROM kunden_tickets").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM portal_nachrichten").fetchone()[0] >= 5
        assert connection.execute("SELECT COUNT(*) FROM portal_dateien").fetchone()[0] == 2
        assert connection.execute("SELECT fortschritt FROM projekte").fetchone()[0] == 70
        assert connection.execute("SELECT COUNT(*) FROM aktivitaeten").fetchone()[0] >= 8
        assert connection.execute(
            "SELECT COUNT(*) FROM aktivitaeten WHERE aktion = 'Agent-Update · Codex'"
        ).fetchone()[0] == 1
        connection.close()

        mounted_database = temp / "mounted-dashboard.db"
        mounted_app = create_app(
            {
                "TESTING": True,
                "DATABASE": str(mounted_database),
                "UPLOAD_FOLDER": str(temp / "mounted-uploads"),
                "SECRET_KEY": "mounted-test-secret",
                "GIT_MONITOR_ENABLED": False,
                "APPLICATION_ROOT": "/agentur",
                "SESSION_COOKIE_PATH": "/agentur",
                "SESSION_COOKIE_SECURE": True,
            }
        )
        mounted_client = Client(
            DispatcherMiddleware(Response("Werkstatt"), {"/agentur": mounted_app}),
            Response,
        )
        mounted_health = mounted_client.get("/agentur/healthz")
        assert mounted_health.status_code == 200
        mounted_setup = mounted_client.get("/agentur/einrichtung")
        assert mounted_setup.status_code == 200
        assert b'/agentur/static/dashboard.css' in mounted_setup.data
        mounted_cookie = mounted_setup.headers.get("Set-Cookie", "")
        assert mounted_cookie.startswith("tomorrowworks_session=")
        assert "Path=/agentur" in mounted_cookie
        assert "Secure" in mounted_cookie

        migration_source = temp / "migration-source.db"
        source_db = sqlite3.connect(database)
        migration_copy = sqlite3.connect(migration_source)
        try:
            source_db.backup(migration_copy)
        finally:
            migration_copy.close()
            source_db.close()
        migration_target = temp / "migration-target.db"
        migration_app = create_app(
            {
                "TESTING": True,
                "DATABASE": str(migration_target),
                "UPLOAD_FOLDER": str(temp / "migration-uploads"),
                "SECRET_KEY": "migration-test-secret",
                "GIT_MONITOR_ENABLED": False,
                "MIGRATION_TOKEN": "one-time-migration-token",
            }
        )
        migration_client = migration_app.test_client()
        with migration_source.open("rb") as source_file:
            imported = migration_client.post(
                "/migration/import",
                headers={"Authorization": "Bearer one-time-migration-token"},
                data={"database": (io.BytesIO(source_file.read()), "dashboard.db")},
                content_type="multipart/form-data",
            )
        assert imported.status_code == 200
        assert imported.get_json()["counts"]["kunden"] == 1
        imported_db = sqlite3.connect(migration_target)
        try:
            assert imported_db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert imported_db.execute("SELECT COUNT(*) FROM projekte").fetchone()[0] == 1
        finally:
            imported_db.close()

    print("Tomorrow-Works-Dashboard: Kernablauf erfolgreich getestet.")


if __name__ == "__main__":
    run()
