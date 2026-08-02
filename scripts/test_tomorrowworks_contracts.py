"""Fokustest fuer versionierte Tomorrow-Works-Vertraege und Preis-PDFs."""

from __future__ import annotations

import hashlib
import io
import json
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

from pypdf import PdfReader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import tomorrowworks_dashboard.app as dashboard_module
from tomorrowworks_dashboard import create_app


def csrf(client, path: str) -> str:
    response = client.get(path, follow_redirects=True)
    assert response.status_code == 200, (path, response.status_code)
    match = re.search(rb'name="_csrf_token" value="([^"]+)"', response.data)
    assert match, f"Kein CSRF-Token auf {path}"
    return match.group(1).decode("utf-8")


def post(
    client,
    path: str,
    data: dict,
    *,
    token_path: str | None = None,
    follow_redirects: bool = True,
):
    values = dict(data)
    values["_csrf_token"] = csrf(client, token_path or path)
    return client.post(path, data=values, follow_redirects=follow_redirects)


def pdf_text(payload: bytes) -> str:
    reader = PdfReader(io.BytesIO(payload))
    assert len(reader.pages) >= 1
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def create_customer(client, firma: str, email: str) -> int:
    response = post(
        client,
        "/kunden/neu",
        {
            "firma": firma,
            "ansprechpartner": "Dagi Goldschmidt",
            "email": email,
            "telefon": "+49 6261 123456",
            "adresse": "Atelierweg 7, 74821 Mosbach",
            "website": "https://example.test",
            "status": "interessent",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    return int(response.headers["Location"].rstrip("/").split("/")[-1])


def create_project(client, customer_id: int, title: str) -> int:
    response = post(
        client,
        "/projekte/neu",
        {
            "kunde_id": str(customer_id),
            "titel": title,
            "typ": "website",
            "beschreibung": "Website, Buchung, Sichtbarkeit und transparente laufende Betreuung.",
            "status": "interne_pruefung",
            "prioritaet": "normal",
            "fortschritt": "85",
            "aktuelle_aufgabe": "Vertrag und Praesentation intern pruefen",
            "vorschau_url": "https://preview.example.test",
            "repo_url": "",
            "lokaler_pfad": "",
            "preview_pfad": ".",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    return int(response.headers["Location"].rstrip("/").split("/")[-1])


def run() -> None:
    with tempfile.TemporaryDirectory(prefix="tw-contract-test-") as temp_name:
        root = Path(temp_name)
        database = root / "dashboard.db"
        uploads = root / "uploads"
        app = create_app(
            {
                "TESTING": True,
                "DATABASE": str(database),
                "UPLOAD_FOLDER": str(uploads),
                "SECRET_KEY": "contract-test-secret",
                "GIT_MONITOR_ENABLED": False,
                "CONTRACT_LEGAL_APPROVED": False,
            }
        )
        admin = app.test_client()

        setup = post(
            admin,
            "/einrichtung",
            {"name": "Test Admin", "email": "admin@example.test", "passwort": "sicher-test-123"},
        )
        assert setup.status_code == 200

        team_created = post(
            admin,
            "/team/neu",
            {
                "name": "Team Mitglied",
                "email": "team@example.test",
                "passwort": "team-test-123",
                "rolle": "team",
                "farbe": "#5b8def",
            },
        )
        assert team_created.status_code == 200

        customer_id = create_customer(admin, "Kunstatelier Goldschmidt", "dagi@example.test")
        project_id = create_project(admin, customer_id, "Kunstatelier Goldschmidt - Website, Logo & Visitenkarten")
        other_customer_id = create_customer(admin, "Anderer Kunde", "anderer@example.test")
        other_project_id = create_project(admin, other_customer_id, "Anderes Kundenprojekt")

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        schema_names = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name IN ('projekt_vertraege', 'projekt_vertragsversionen')
                """
            ).fetchall()
        }
        ticket = connection.execute(
            "SELECT * FROM kunden_tickets WHERE kunde_id = ?", (customer_id,)
        ).fetchone()
        assert ticket is not None
        before_messages = connection.execute("SELECT COUNT(*) FROM portal_nachrichten").fetchone()[0]
        before_notifications = connection.execute("SELECT COUNT(*) FROM portal_benachrichtigungen").fetchone()[0]
        connection.close()
        assert schema_names == {"projekt_vertraege", "projekt_vertragsversionen"}

        contract_center = f"/projekte/{project_id}/vertraege"
        anonymous = app.test_client()
        denied = anonymous.get(contract_center, follow_redirects=False)
        assert denied.status_code == 302
        assert "/anmelden" in denied.headers["Location"]
        denied_prices = anonymous.get("/leistungen-preise.pdf", follow_redirects=False)
        assert denied_prices.status_code == 302
        assert "/anmelden" in denied_prices.headers["Location"]

        team = app.test_client()
        logged_in = post(
            team,
            "/anmelden",
            {"email": "team@example.test", "passwort": "team-test-123"},
        )
        assert logged_in.status_code == 200
        team_center = team.get(contract_center)
        assert team_center.status_code == 200
        forbidden = post(
            team,
            contract_center,
            {
                "titel": "Unzulaessiger Team-Vertrag",
                "package_code": "founder_pilot",
                "start_date": "2026-09-01",
                "media_budget_eur": "0,00",
            },
            token_path=f"/projekte/{project_id}",
            follow_redirects=False,
        )
        assert forbidden.status_code == 403

        no_csrf = admin.post(
            contract_center,
            data={"titel": "Ohne CSRF", "package_code": "founder_pilot"},
            follow_redirects=False,
        )
        assert no_csrf.status_code == 400

        mail_called = False
        original_mail = dashboard_module.mail_senden

        def forbidden_mail(*_args, **_kwargs):
            nonlocal mail_called
            mail_called = True
            raise AssertionError("Eine interne Vertragsfassung darf keine Kundenmail ausloesen")

        dashboard_module.mail_senden = forbidden_mail
        try:
            created = post(
                admin,
                contract_center,
                {
                    "titel": "Founder-Pilot Rahmen- und Betreuungsvertrag",
                    "package_code": "founder_pilot",
                    "addon_codes": [
                        "email_setup",
                        "google_business",
                        "google_ads",
                        "booking",
                        "sales_video",
                        "business_card",
                    ],
                    "start_date": "2026-09-01",
                    "media_budget_eur": "1000,00",
                    "notes": "Kundenspezifischer Entwurf. Vor Versand Firmierung und Leistungsbeginn bestaetigen.",
                },
                token_path=f"/projekte/{project_id}",
                follow_redirects=False,
            )
        finally:
            dashboard_module.mail_senden = original_mail
        assert created.status_code == 302
        assert created.headers["Location"].endswith(contract_center)
        assert mail_called is False

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        contracts = connection.execute(
            "SELECT * FROM projekt_vertraege WHERE projekt_id = ?", (project_id,)
        ).fetchall()
        assert len(contracts) == 1
        contract = contracts[0]
        assert contract["status"] == "entwurf"
        versions = connection.execute(
            "SELECT * FROM projekt_vertragsversionen WHERE vertrag_id = ? ORDER BY version",
            (contract["id"],),
        ).fetchall()
        assert len(versions) == 1
        first = versions[0]
        first_snapshot = json.loads(first["snapshot_json"])
        first_path = uploads / first["gespeichert_name"]
        first_bytes = first_path.read_bytes()
        assert first["version"] == 1
        assert first["mimetype"] == "application/pdf"
        assert first["groesse"] == len(first_bytes)
        assert first["sha256"] == hashlib.sha256(first_bytes).hexdigest()
        assert first_bytes.startswith(b"%PDF-")
        assert first_snapshot["legal_approved"] is False
        assert first_snapshot["package"]["code"] == "founder_pilot"
        assert first_snapshot["pricing"]["setup_cent"] == 333_000
        assert first_snapshot["pricing"]["monthly_cent"] == 64_800
        assert first_snapshot["pricing"]["media_budget_cent"] == 100_000
        assert connection.execute("SELECT COUNT(*) FROM portal_nachrichten").fetchone()[0] == before_messages
        assert connection.execute("SELECT COUNT(*) FROM portal_benachrichtigungen").fetchone()[0] == before_notifications
        connection.close()

        first_text = pdf_text(first_bytes)
        for expected in (
            "Nicht rechtsverbindlicher Vertragsentwurf",
            "Kunstatelier Goldschmidt",
            "Founder-Pilot",
            "Google Ads",
            "Instagram",
            "keine Garantie",
            "3.330,00 EUR",
            "648,00 EUR",
        ):
            assert expected in first_text, expected

        pdf_url = f"/projekte/{project_id}/vertraege/{contract['id']}/versionen/1.pdf"
        denied_pdf = anonymous.get(pdf_url, follow_redirects=False)
        assert denied_pdf.status_code == 302
        assert "/anmelden" in denied_pdf.headers["Location"]
        team_pdf = team.get(pdf_url)
        assert team_pdf.status_code == 200
        assert team_pdf.data == first_bytes
        assert team_pdf.headers["Cache-Control"] == "no-store, private"
        assert team_pdf.headers["X-Content-Type-Options"] == "nosniff"
        team_pdf.close()

        inline = admin.get(pdf_url)
        assert inline.status_code == 200
        assert inline.headers["Content-Disposition"].startswith("inline;")
        assert "V01.pdf" in inline.headers["Content-Disposition"]
        assert inline.data == first_bytes
        inline.close()
        download = admin.get(f"{pdf_url}?download=1")
        assert download.status_code == 200
        assert download.headers["Content-Disposition"].startswith("attachment;")
        assert "V01.pdf" in download.headers["Content-Disposition"]
        download.close()

        dashboard_body = admin.get("/").get_data(as_text=True)
        detail_body = admin.get(f"/projekte/{project_id}").get_data(as_text=True)
        center_body = admin.get(contract_center).get_data(as_text=True)
        assert "Leistungen &amp; Preise" in dashboard_body or "Leistungen & Preise" in dashboard_body
        for body in (dashboard_body, detail_body):
            assert "Vertrag ansehen" in body
            assert "Vertrag herunterladen" in body
            assert pdf_url in body
            assert f"{pdf_url}?download=1" in body
        assert "Founder-Pilot Rahmen- und Betreuungsvertrag" in center_body
        assert "Version 1" in center_body or "V1" in center_body

        portal_body = admin.get(f"/portal/{ticket['token']}").get_data(as_text=True)
        assert "Founder-Pilot Rahmen- und Betreuungsvertrag" not in portal_body
        assert "V01.pdf" not in portal_body

        first_snapshot_json = first["snapshot_json"]
        first_hash = first["sha256"]
        dashboard_module.mail_senden = forbidden_mail
        try:
            updated = post(
                admin,
                contract_center,
                {
                    "vertrag_id": str(contract["id"]),
                    "titel": "Founder-Pilot Rahmen- und Betreuungsvertrag",
                    "package_code": "founder_pilot",
                    "addon_codes": ["google_ads", "dashboard_extension", "sales_video"],
                    "start_date": "2026-10-01",
                    "media_budget_eur": "2000,00",
                    "notes": "Version zwei: Dashboard-Erweiterung aufgenommen.",
                },
                token_path=f"/projekte/{project_id}",
                follow_redirects=False,
            )
        finally:
            dashboard_module.mail_senden = original_mail
        assert updated.status_code == 302
        assert mail_called is False

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        versions = connection.execute(
            "SELECT * FROM projekt_vertragsversionen WHERE vertrag_id = ? ORDER BY version",
            (contract["id"],),
        ).fetchall()
        assert [row["version"] for row in versions] == [1, 2]
        persisted_first, second = versions
        assert persisted_first["snapshot_json"] == first_snapshot_json
        assert persisted_first["sha256"] == first_hash
        assert (uploads / persisted_first["gespeichert_name"]).read_bytes() == first_bytes
        second_snapshot = json.loads(second["snapshot_json"])
        second_path = uploads / second["gespeichert_name"]
        second_bytes = second_path.read_bytes()
        assert second["groesse"] == len(second_bytes)
        assert second["sha256"] == hashlib.sha256(second_bytes).hexdigest()
        assert second_snapshot["version"] == 2
        assert second_snapshot["pricing"]["media_budget_cent"] == 200_000
        assert second_snapshot["pricing"]["monthly_cent"] == 84_800
        assert connection.execute("SELECT COUNT(*) FROM portal_nachrichten").fetchone()[0] == before_messages
        assert connection.execute("SELECT COUNT(*) FROM portal_benachrichtigungen").fetchone()[0] == before_notifications
        connection.close()

        second_text = pdf_text(second_bytes)
        assert "Version zwei: Dashboard-Erweiterung aufgenommen" in second_text
        assert "848,00 EUR" in second_text
        assert admin.get(pdf_url).data == first_bytes

        scoped_idor = admin.get(
            f"/projekte/{other_project_id}/vertraege/{contract['id']}/versionen/1.pdf"
        )
        assert scoped_idor.status_code == 404

        prices = admin.get("/leistungen-preise.pdf")
        assert prices.status_code == 200
        assert prices.data.startswith(b"%PDF-")
        assert prices.headers["Content-Disposition"].startswith("inline;")
        prices_text = pdf_text(prices.data)
        for expected in ("Tomorrow Works", "Website Start", "Digitales Cockpit", "Google Ads", "Interne Preisvorlage"):
            assert expected in prices_text, expected
        prices.close()
        prices_download = admin.get("/leistungen-preise.pdf?download=1")
        assert prices_download.status_code == 200
        assert prices_download.headers["Content-Disposition"].startswith("attachment;")
        prices_download.close()

        second_path.write_bytes(b"%PDF-1.4\nabsichtlich manipuliert")
        tampered = admin.get(
            f"/projekte/{project_id}/vertraege/{contract['id']}/versionen/2.pdf"
        )
        assert tampered.status_code == 409
        assert "stimmt nicht mehr mit der gespeicherten Vertragsfassung" in tampered.get_data(as_text=True)

    print("Tomorrow Works Vertraege: versioniert, intern, geschuetzt und als PDF erfolgreich getestet.")


if __name__ == "__main__":
    run()
