from __future__ import annotations

import re
import sqlite3
import sys
import tempfile
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tomorrowworks_dashboard import create_app
from tomorrowworks_dashboard import app as dashboard_app


def csrf(client, path: str) -> str:
    response = client.get(path, follow_redirects=True)
    if response.status_code != 200:
        response = client.get("/", follow_redirects=True)
    assert response.status_code == 200
    match = re.search(rb'name="_csrf_token" value="([^"]+)"', response.data)
    assert match, f"Kein CSRF-Token auf {path}"
    return match.group(1).decode("utf-8")


def post(client, path: str, data: dict, *, follow_redirects: bool = True):
    payload = dict(data)
    payload["_csrf_token"] = csrf(client, path)
    return client.post(path, data=payload, follow_redirects=follow_redirects)


def kundendaten(firma: str, status: str, **extra) -> dict:
    data = {
        "firma": firma,
        "ansprechpartner": "Anna Beispiel",
        "email": f"{firma.lower().replace(' ', '-')}@example.test",
        "telefon": "+49 171 1234567",
        "adresse": "Musterstraße 12, 74821 Mosbach",
        "website": "https://example.test",
        "status": status,
    }
    data.update(extra)
    return data


def run() -> None:
    with tempfile.TemporaryDirectory(prefix="tw-customer-invitation-") as temp_dir:
        temp = Path(temp_dir)
        app = create_app(
            {
                "TESTING": True,
                "DATABASE": str(temp / "dashboard.db"),
                "UPLOAD_FOLDER": str(temp / "uploads"),
                "SECRET_KEY": "test-secret-not-for-production",
                "GIT_MONITOR_ENABLED": False,
                "PUBLIC_BASE_URL": "https://portal.example.test",
                "SMTP_HOST": "smtp.example.test",
                "SMTP_FROM": "team@example.test",
            }
        )
        client = app.test_client()

        response = post(
            client,
            "/einrichtung",
            {
                "name": "Test Admin",
                "email": "admin@example.test",
                "passwort": "sicher-test-123",
            },
        )
        assert response.status_code == 200

        form = client.get("/kunden/neu")
        body = form.get_data(as_text=True)
        assert form.status_code == 200
        assert "Nur intern speichern / Einladung später senden" in body
        assert 'name="einladungsmodus" value="spaeter" checked' in body
        assert "Jetzt per E-Mail einladen" in body

        mail_calls: list[tuple[str, str, str]] = []
        original_mail_senden = dashboard_app.mail_senden

        def fake_mail_senden(_config, empfaenger: str, betreff: str, text: str):
            mail_calls.append((empfaenger, betreff, text))
            return "gesendet", ""

        dashboard_app.mail_senden = fake_mail_senden
        try:
            internal = post(
                client,
                "/kunden/neu",
                kundendaten(
                    "Interne Interessentin",
                    "interessent",
                    einladungsmodus="spaeter",
                ),
            )
            internal_body = internal.get_data(as_text=True)
            assert internal.status_code == 200
            assert "nur intern angelegt" in internal_body
            assert "Es wurde keine E-Mail gesendet" in internal_body
            assert mail_calls == []

            connection = sqlite3.connect(app.config["DATABASE"])
            connection.row_factory = sqlite3.Row
            interessentin = connection.execute(
                "SELECT * FROM kunden WHERE firma = ?", ("Interne Interessentin",)
            ).fetchone()
            assert interessentin is not None
            assert interessentin["status"] == "interessent"
            assert connection.execute(
                "SELECT COUNT(*) FROM kunden_tickets WHERE kunde_id = ?",
                (interessentin["id"],),
            ).fetchone()[0] == 1
            assert connection.execute(
                "SELECT COUNT(*) FROM portal_benachrichtigungen"
            ).fetchone()[0] == 0
            connection.close()

            later_invitation = post(
                client,
                f"/kunden/{interessentin['id']}/ticket/einladung",
                {},
            )
            assert later_invitation.status_code == 200
            assert "Einladung wurde per E-Mail gesendet" in later_invitation.get_data(as_text=True)
            assert len(mail_calls) == 1
            assert mail_calls[0][0] == "interne-interessentin@example.test"

            invited = post(
                client,
                "/kunden/neu",
                kundendaten(
                    "Aktiver Kunde",
                    "aktiv",
                    einladungsmodus="jetzt",
                ),
            )
            assert invited.status_code == 200
            assert "Einladung wurde per E-Mail gesendet" in invited.get_data(as_text=True)
            assert len(mail_calls) == 2
            assert mail_calls[1][0] == "aktiver-kunde@example.test"

            connection = sqlite3.connect(app.config["DATABASE"])
            assert connection.execute(
                "SELECT COUNT(*) FROM portal_benachrichtigungen WHERE status = 'gesendet'"
            ).fetchone()[0] == 2
            connection.close()

            legacy_interest = post(
                client,
                "/kunden/neu",
                kundendaten("Legacy Interessent", "interessent"),
            )
            assert legacy_interest.status_code == 200
            assert "nur intern angelegt" in legacy_interest.get_data(as_text=True)
            assert len(mail_calls) == 2

            legacy_active = post(
                client,
                "/kunden/neu",
                kundendaten("Legacy Aktiv", "aktiv"),
            )
            assert legacy_active.status_code == 200
            assert "Einladung wurde per E-Mail gesendet" in legacy_active.get_data(as_text=True)
            assert len(mail_calls) == 3
        finally:
            dashboard_app.mail_senden = original_mail_senden

    print("TomorrowWorks customer invitation test passed")


if __name__ == "__main__":
    run()
