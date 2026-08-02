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


def compact_text(value: str) -> str:
    return " ".join(value.split())


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
        expected_addon_prices = {
            "technical_care": (0, 1_999),
            "email_setup": (4_999, 0),
            "google_business": (0, 0),
            "google_business_care": (0, 1_999),
            "google_ads": (0, 0),
            "meta_ads": (0, 0),
            "google_meta_ads": (0, 0),
            "dashboard_extension": (49_900, 4_999),
            "booking": (20_000, 0),
            "sales_video": (69_000, 0),
            "short_clips": (69_900, 0),
            "flyer": (19_900, 0),
            "business_card": (19_000, 0),
            "print_bundle": (39_000, 0),
        }
        for code, expected in expected_addon_prices.items():
            addon = dashboard_module.ADDON_CATALOG[code]
            assert (addon["setup_cent"], addon["monthly_cent"]) == expected
        for code in ("google_ads", "meta_ads", "google_meta_ads"):
            addon = dashboard_module.ADDON_CATALOG[code]
            assert addon["minimum_monthly_cent"] == 20_000
            assert addon["price_mode"] == "agreed_monthly"
            assert addon["setup_cent"] == 0
            assert "media_fee_percent" not in addon
        assert dashboard_module.ADDON_CATALOG["google_business"]["free_service"] is True

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

        unbound_budget = post(
            admin,
            contract_center,
            {
                "titel": "Budget ohne Werbemodul",
                "package_code": "website_start",
                "media_budget_eur": "250,00",
            },
        )
        assert unbound_budget.status_code == 200
        assert "Ein Medienbudget kann nur zusammen mit Google Ads" in unbound_budget.get_data(as_text=True)

        missing_ad_fee = post(
            admin,
            contract_center,
            {
                "titel": "Werbung ohne vereinbartes Honorar",
                "package_code": "website_start",
                "addon_codes": ["google_ads"],
                "media_budget_eur": "250,00",
            },
        )
        assert missing_ad_fee.status_code == 200
        assert "mit dem Kunden vereinbarte monatliche Agenturhonorar" in missing_ad_fee.get_data(as_text=True)

        too_low_google_fee = post(
            admin,
            contract_center,
            {
                "titel": "Google Ads unter Mindesthonorar",
                "package_code": "website_start",
                "addon_codes": ["google_ads"],
                "agreed_ad_monthly_eur": "199,99",
                "media_budget_eur": "250,00",
            },
        )
        assert too_low_google_fee.status_code == 200
        assert "mindestens 200,00 EUR" in too_low_google_fee.get_data(as_text=True)

        too_low_meta_fee = post(
            admin,
            contract_center,
            {
                "titel": "Meta Ads unter Mindesthonorar",
                "package_code": "website_start",
                "addon_codes": ["meta_ads"],
                "agreed_ad_monthly_eur": "199,99",
                "media_budget_eur": "250,00",
            },
        )
        assert too_low_meta_fee.status_code == 200
        assert "mindestens 200,00 EUR" in too_low_meta_fee.get_data(as_text=True)

        fee_without_ads = post(
            admin,
            contract_center,
            {
                "titel": "Honorar ohne Werbemodul",
                "package_code": "website_start",
                "agreed_ad_monthly_eur": "200,00",
                "media_budget_eur": "0",
            },
        )
        assert fee_without_ads.status_code == 200
        assert "kann nur zusammen mit Google Ads" in fee_without_ads.get_data(as_text=True)

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
                    "agreed_ad_monthly_eur": "200,00",
                    "media_budget_eur": "1000,00",
                    "customer_agreement": "Monatlicher Bericht und eine gemeinsame Optimierungsrunde.",
                    "notes": "INTERN-NICHT-IM-PDF: Firmierung und Leistungsbeginn bestaetigen.",
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
        assert first_snapshot["text_version"] == "tw-agenturvertrag-2026-08-02-v3"
        assert first_snapshot["package"]["code"] == "founder_pilot"
        assert {
            item["code"] for item in first_snapshot["selection_manifest"]["packages"] if item["selected"]
        } == {"founder_pilot"}
        assert {
            item["code"] for item in first_snapshot["selection_manifest"]["addons"] if item["selected"]
        } == {
            "email_setup",
            "google_business",
            "google_ads",
            "booking",
            "sales_video",
            "business_card",
        }
        assert len(first_snapshot["selection_manifest"]["addons"]) == len(dashboard_module.ADDON_CATALOG)
        assert first_snapshot["pricing"]["setup_cent"] == 211_999
        assert first_snapshot["pricing"]["monthly_cent"] == 59_900
        assert first_snapshot["pricing"]["media_budget_cent"] == 100_000
        assert first_snapshot["pricing"]["first_term_cent"] == 571_399
        assert first_snapshot["ad_fee_agreement"] == {
            "addon_code": "google_ads",
            "addon_name": "Google Ads",
            "monthly_cent": 20_000,
        }
        assert first_snapshot["customer_agreement"] == "Monatlicher Bericht und eine gemeinsame Optimierungsrunde."
        assert first_snapshot["internal_notes"].startswith("INTERN-NICHT-IM-PDF")
        assert connection.execute("SELECT COUNT(*) FROM portal_nachrichten").fetchone()[0] == before_messages
        assert connection.execute("SELECT COUNT(*) FROM portal_benachrichtigungen").fetchone()[0] == before_notifications
        connection.close()

        first_text = pdf_text(first_bytes)
        first_compact = compact_text(first_text)
        for expected in (
            "Nicht rechtsverbindlicher Vertragsentwurf",
            "Kunstatelier Goldschmidt",
            "Founder-Pilot",
            "Google Ads",
            "Instagram",
            "keine Garantie",
            "2.119,99 EUR",
            "599,00 EUR",
            "Leistungsauswahl",
            "Grundpaket - genau eine Auswahl",
            "Mit dem Kunden vereinbart",
            "Auswahl bestätigt",
        ):
            assert expected in first_text, expected
        assert "Monatlicher Bericht und eine gemeinsame Optimierungsrunde" in first_compact
        assert "INTERN-NICHT-IM-PDF" not in first_text
        for addon in dashboard_module.ADDON_CATALOG.values():
            assert addon["name"] in first_text, addon["name"]
        for expected in (
            "[X] gewählt Founder-Pilot",
            "[ ] nicht gewählt Website Start",
            "[X] zusätzlich gebucht Google Ads",
            "[ ] nicht zusätzlich gebucht Instagram & Facebook Ads",
            "Leistungen des Grundpakets und projektbezogene Inklusivleistungen gelten unabhängig",
            "Preisangaben bei nicht markierten Optionen sind unverbindliche Orientierung",
        ):
            assert expected in first_compact, expected

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
        assert "Grundpaket ankreuzen" in center_body
        assert "Zusatzmodule ankreuzen" in center_body
        assert "Business-Dashboard einrichten" in center_body
        assert "Mit dem Kunden vereinbartes Ads-Honorar pro Monat" in center_body
        assert "Mit dem Kunden vereinbart – zusätzliche Leistungsvereinbarung" in center_body
        assert 'name="customer_agreement"' in center_body
        assert "Nur intern im Snapshot; dieser Text erscheint nicht in der Vertrags-PDF." in center_body
        assert "49,99 € einmalig" in center_body
        assert "ab 200,00 € monatlich" in center_body
        assert "kostenlos · keine laufende Gebühr" in center_body
        assert 'name="package_code" value="website_growth" checked' not in center_body
        assert center_body.count('name="addon_codes"') == len(dashboard_module.ADDON_CATALOG)

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
                    "agreed_ad_monthly_eur": "200,00",
                    "media_budget_eur": "2000,00",
                    "customer_agreement": "Version zwei: Dashboard-Erweiterung aufgenommen.",
                    "notes": "Interne Kalkulation nicht im Kundenvertrag.",
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
        assert second_snapshot["text_version"] == "tw-agenturvertrag-2026-08-02-v3"
        assert second_snapshot["pricing"]["media_budget_cent"] == 200_000
        assert second_snapshot["pricing"]["setup_cent"] == 217_900
        assert second_snapshot["pricing"]["monthly_cent"] == 64_899
        assert second_snapshot["pricing"]["first_term_cent"] == 607_294
        assert {
            item["code"] for item in second_snapshot["selection_manifest"]["addons"] if item["selected"]
        } == {"google_ads", "dashboard_extension", "sales_video"}
        assert connection.execute("SELECT COUNT(*) FROM portal_nachrichten").fetchone()[0] == before_messages
        assert connection.execute("SELECT COUNT(*) FROM portal_benachrichtigungen").fetchone()[0] == before_notifications
        connection.close()

        second_text = pdf_text(second_bytes)
        second_compact = compact_text(second_text)
        assert "Version zwei: Dashboard-Erweiterung aufgenommen" in second_text
        assert "648,99 EUR" in second_text
        assert "Interne Kalkulation nicht im Kundenvertrag" not in second_text
        assert "[X] zusätzlich gebucht Google Ads" in second_compact
        assert "[X] zusätzlich gebucht Business-Dashboard einrichten" in second_compact
        assert "[ ] nicht zusätzlich gebucht Geschäfts-E-Mail bis drei Postfächer" in second_compact
        assert "Google Ads 0,00 EUR 200,00 EUR mit Kunden vereinbart" in second_compact
        assert "Instagram & Facebook Ads 0,00 EUR variabel ab 200,00 EUR" in second_compact
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
        prices_compact = compact_text(prices_text)
        for expected in (
            "Technische Website-Pflege",
            "19,99 EUR",
            "Geschäfts-E-Mail bis drei Postfächer",
            "49,99 EUR",
            "variabel ab 200,00 EUR",
            "Business-Dashboard einrichten",
            "499,00 EUR",
            "kostenlos",
            "keine laufende Gebühr",
        ):
            assert expected in prices_compact, expected
        for stale in ("249,00 EUR", "15 Prozent", "15 %", "1.490,00 EUR"):
            assert stale not in prices_text, stale
        prices.close()
        prices_download = admin.get("/leistungen-preise.pdf?download=1")
        assert prices_download.status_code == 200
        assert prices_download.headers["Content-Disposition"].startswith("attachment;")
        prices_download.close()

        standard_snapshot = dashboard_module.build_contract_snapshot(
            project={"id": 99, "titel": "Standard-Website", "beschreibung": ""},
            customer={
                "firma": "Standardkunde GmbH",
                "ansprechpartner": "Max Muster",
                "adresse": "Musterweg 1, 74821 Mosbach",
                "email": "standard@example.test",
            },
            provider={
                "name": "Tomorrow Works",
                "address": "Binauer Höhe 4, 74821 Mosbach",
                "representative": "Christopher Gärtner",
                "email": "info@example.test",
            },
            package_code="website_start",
            addon_codes=[],
            agreed_ad_monthly_cent=0,
            media_budget_cent=0,
            start_date="",
            customer_agreement="",
            notes="",
            version=1,
            contract_id=1,
            created_at="2026-08-02 20:00:00",
            legal_approved=False,
        )
        standard_pdf = dashboard_module.create_contract_pdf(standard_snapshot)
        standard_reader = PdfReader(io.BytesIO(standard_pdf))
        assert all((page.extract_text() or "").strip() for page in standard_reader.pages)
        standard_text = compact_text(pdf_text(standard_pdf))
        assert "[X] gewählt Website Start" in standard_text
        assert "Founder-Pilot" not in standard_text
        assert "Google-Unternehmensprofil einrichten oder korrigieren kostenlos keine laufende Gebühr" in standard_text

        for ad_code, ad_name in (
            ("meta_ads", "Instagram & Facebook Ads"),
            ("google_meta_ads", "Google + Meta Ads"),
        ):
            ad_snapshot = dashboard_module.build_contract_snapshot(
                project={"id": 102, "titel": "Ads-Grenztest", "beschreibung": ""},
                customer={
                    "firma": "Ads-Test GmbH",
                    "ansprechpartner": "Max Muster",
                    "adresse": "Musterweg 4, 74821 Mosbach",
                    "email": "ads@example.test",
                },
                provider={
                    "name": "Tomorrow Works",
                    "address": "Binauer Höhe 4, 74821 Mosbach",
                    "representative": "Christopher Gärtner",
                    "email": "info@example.test",
                },
                package_code="website_start",
                addon_codes=[ad_code],
                agreed_ad_monthly_cent=20_000,
                media_budget_cent=50_000,
                start_date="",
                customer_agreement="",
                notes="",
                version=1,
                contract_id=1,
                created_at="2026-08-02 20:00:00",
                legal_approved=False,
            )
            assert ad_snapshot["pricing"]["monthly_cent"] == 20_000
            assert ad_snapshot["ad_fee_agreement"]["addon_name"] == ad_name
            ad_text = compact_text(pdf_text(dashboard_module.create_contract_pdf(ad_snapshot)))
            assert f"[X] zusätzlich gebucht {ad_name} 0,00 EUR 200,00 EUR mit Kunden vereinbart" in ad_text

        escaped_snapshot = dashboard_module.build_contract_snapshot(
            project={"id": 101, "titel": "Sonderzeichen", "beschreibung": ""},
            customer={
                "firma": "Sonderzeichen GmbH",
                "ansprechpartner": "Max Muster",
                "adresse": "Musterweg 3, 74821 Mosbach",
                "email": "sonderzeichen@example.test",
            },
            provider={
                "name": "Tomorrow Works",
                "address": "Binauer Höhe 4, 74821 Mosbach",
                "representative": "Christopher Gärtner",
                "email": "info@example.test",
            },
            package_code="website_start",
            addon_codes=[],
            agreed_ad_monthly_cent=0,
            media_budget_cent=0,
            start_date="",
            customer_agreement="<b>Kunden & Partner</b>\n" + ("Zusatzvereinbarung " * 150),
            notes="INTERNER HTML-Test <script>alert(1)</script>",
            version=1,
            contract_id=1,
            created_at="2026-08-02 20:00:00",
            legal_approved=False,
        )
        assert len(escaped_snapshot["customer_agreement"]) == 2000
        escaped_text = pdf_text(dashboard_module.create_contract_pdf(escaped_snapshot))
        assert "Kunden & Partner" in escaped_text
        assert "INTERNER HTML-Test" not in escaped_text

        full_snapshot = dashboard_module.build_contract_snapshot(
            project={"id": 100, "titel": "Vollauswahl", "beschreibung": ""},
            customer={
                "firma": "Vollauswahl GmbH",
                "ansprechpartner": "Max Muster",
                "adresse": "Musterweg 2, 74821 Mosbach",
                "email": "voll@example.test",
            },
            provider={
                "name": "Tomorrow Works",
                "address": "Binauer Höhe 4, 74821 Mosbach",
                "representative": "Christopher Gärtner",
                "email": "info@example.test",
            },
            package_code="digital_cockpit",
            addon_codes=[
                "technical_care",
                "email_setup",
                "google_business",
                "google_business_care",
                "google_meta_ads",
                "dashboard_extension",
                "booking",
                "sales_video",
                "short_clips",
                "print_bundle",
                "social_content",
            ],
            agreed_ad_monthly_cent=35_000,
            media_budget_cent=500_000,
            start_date="",
            customer_agreement="Individuell abgestimmter Gesamtumfang für den Layouttest.",
            notes="Umfangreicher Layouttest.",
            version=1,
            contract_id=1,
            created_at="2026-08-02 20:00:00",
            legal_approved=False,
        )
        full_pdf = dashboard_module.create_contract_pdf(full_snapshot)
        full_reader = PdfReader(io.BytesIO(full_pdf))
        full_page_texts = [page.extract_text() or "" for page in full_reader.pages]
        assert all(text.strip() for text in full_page_texts)
        assert "Freigaben vor Livegang" in full_page_texts[-1]
        assert "Auswahl bestätigt" in full_page_texts[-1]
        assert "Ort, Datum / Auftragnehmer" in full_page_texts[-1]
        assert "Umfangreicher Layouttest" not in "\n".join(full_page_texts)
        assert "Individuell abgestimmter Gesamtumfang" in "\n".join(full_page_texts)

        second_path.write_bytes(b"%PDF-1.4\nabsichtlich manipuliert")
        tampered = admin.get(
            f"/projekte/{project_id}/vertraege/{contract['id']}/versionen/2.pdf"
        )
        assert tampered.status_code == 409
        assert "stimmt nicht mehr mit der gespeicherten Vertragsfassung" in tampered.get_data(as_text=True)

    print("Tomorrow Works Vertraege: versioniert, intern, geschuetzt und als PDF erfolgreich getestet.")


if __name__ == "__main__":
    run()
