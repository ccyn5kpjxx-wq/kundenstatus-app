from pathlib import Path
from io import BytesIO
import gc
import json
import shutil
import sys
import tempfile
import uuid
import zipfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def check(label, condition, detail=""):
    status = "OK" if condition else "FEHLER"
    message = f"[{status}] {label}"
    if detail:
        message += f" - {detail}"
    print(message)
    if not condition:
        raise AssertionError(f"{label}: {detail}")


def extract_id_from_location(location):
    last = (location or "").rstrip("/").split("/")[-1]
    return int(last) if last.isdigit() else 0


def with_csrf(client, data=None):
    payload = dict(data or {})
    with client.session_transaction() as session:
        token = session.get("csrf_token") or "test-csrf-token"
        session["csrf_token"] = token
    payload["csrf_token"] = token
    return payload


def csrf_headers(client):
    with client.session_transaction() as session:
        token = session.get("csrf_token") or "test-csrf-token"
        session["csrf_token"] = token
    return {"X-CSRF-Token": token}


def main():
    original_db = portal.DB
    original_upload_dir = portal.UPLOAD_DIR
    original_backup_dir = portal.BACKUP_DIR
    original_deleted_upload_dir = portal.DELETED_UPLOAD_DIR
    if not original_db.exists():
        raise SystemExit(f"Datenbank nicht gefunden: {original_db}")

    with tempfile.TemporaryDirectory() as tmp:
        test_db = Path(tmp) / "auftraege-test.db"
        test_uploads = Path(tmp) / "uploads"
        test_backups = Path(tmp) / "backups"
        test_deleted_uploads = Path(tmp) / "deleted_uploads"
        test_uploads.mkdir()
        test_backups.mkdir()
        shutil.copy2(original_db, test_db)

        portal.DB = test_db
        portal.UPLOAD_DIR = test_uploads
        portal.BACKUP_DIR = test_backups
        portal.DELETED_UPLOAD_DIR = test_deleted_uploads
        portal.app.config["TESTING"] = True
        portal.init_db()
        db = portal.get_db()
        busy_timeout = db.execute("PRAGMA busy_timeout").fetchone()[0]
        journal_mode = db.execute("PRAGMA journal_mode").fetchone()[0]
        db.close()
        check(
            "SQLite wartet bei kurzer Datenbanksperre",
            busy_timeout >= portal.SQLITE_BUSY_TIMEOUT_SECONDS * 1000,
            str(busy_timeout),
        )
        check(
            "SQLite WAL-Modus aktiv",
            str(journal_mode).lower() == "wal",
            str(journal_mode),
        )

        gutachten_text = """
        Schadensgutachten
        Schadensfeststellung
        Hauptbeschädigungsbereich
        Schadensbeschreibung
        Durch den Anstoß im Heckbereich rechts wurden die beiden unteren Stoßfängerteile stark eingedrückt.
        Die Heckklappenblende rechts unten wurde gestaucht und verschrammt.
        Vorschäden
        Unreparierte Vorschäden
        An dem Fahrzeug wurden folgende unreparierte Vorschäden festgestellt:
        Tür hinten links etwas verschrammt
        DEKRA-Nr: TEST
        Seite 5 von 7
        Instandsetzung
        ARB.POS.NR/ INSTANDSETZUNGS-/EINZEL-/VERBUNDARBEITEN AW
        REIFEN 235/55 R19 FELGE 7.5 J X 19 ALU NOTRAD 18 X 4 T
        NA02R0 STOSSFAENGER H ERSETZEN 9 148.50
        2581 STOSSFAENGER H NEUTEILLACK ST K1R 9
        """
        gutachten_felder = portal.parse_document_fields(gutachten_text, "schaden-gutachten.pdf")
        gutachten_analyse = portal.normalize_document_text(gutachten_felder.get("analyse_text"))
        check(
            "Gutachten ignoriert Vorschaden",
            "tuer hinten links" not in gutachten_analyse,
            gutachten_felder.get("analyse_text"),
        )
        check(
            "Gutachten erkennt aktuellen Heckschaden",
            "stossstange hinten" in gutachten_analyse,
            gutachten_felder.get("analyse_text"),
        )
        gutachten_mit_ki = portal.merge_document_fields(
            {
                "analyse_text": "Tür hinten links lackieren, Stoßstange hinten lackieren",
                "bauteile_override": "Tür hinten links\nStoßstange hinten",
                "analyse_confidence": 0.9,
            },
            gutachten_felder,
        )
        gutachten_ki_analyse = portal.normalize_document_text(gutachten_mit_ki.get("analyse_text"))
        check(
            "KI-Ergebnis wird um Vorschaden bereinigt",
            "tuer hinten links" not in gutachten_ki_analyse
            and "stossstange hinten" in gutachten_ki_analyse,
            gutachten_mit_ki.get("analyse_text"),
        )
        kosten_gutachten_text = """
        Zusammenfassung und Ergebnis
        ohne MwSt. mit MwSt.
        Reparaturkosten 2.925,95 EUR 3.481,88 EUR
        Merkantile Wertminderung 350,00 EUR
        Wiederbeschaffungswert 10.011,85 EUR
        R E P A R A T U R K O S T E N OHNE MWST 2 925.95
        R E P A R A T U R K O S T E N MIT MWST 3 481.88
        """
        kosten_felder = portal.parse_document_fields(kosten_gutachten_text, "schaden-gutachten.pdf")
        check(
            "Gutachten nutzt Reparaturkosten brutto statt WBW",
            kosten_felder.get("rep_max_kosten") == "3.481,88 EUR",
            kosten_felder.get("rep_max_kosten"),
        )
        kosten_ki = portal.normalize_openai_document_data(
            {
                "document_type": "Gutachten",
                "vehicle_type": "",
                "fin_nummer": "",
                "auftragsnummer": "",
                "kennzeichen": "",
                "auftrags_datum": "",
                "fertig_bis": "",
                "rep_max_kosten": "10.011,85 EUR",
                "farbnummer": "",
                "offene_bauteile": [],
                "erledigte_bauteile": [],
                "kurzanalyse": "",
                "lesefassung": "",
                "confidence": 0.9,
                "needs_review": False,
                "review_reason": "",
            },
            kosten_gutachten_text,
        )
        check(
            "KI-WBW wird durch Reparaturkosten ersetzt",
            kosten_ki.get("rep_max_kosten") == "3.481,88 EUR",
            kosten_ki.get("rep_max_kosten"),
        )
        pflicht_status = portal.versicherung_pflichtunterlagen_status(
            {
                "id": 0,
                "kunde_name": "",
                "versicherungsnehmer": "",
                "fin_nummer": "",
                "schaden_nummer": "",
                "versicherung_police": "",
            },
            [],
        )
        pflicht_kalkulation = next(item for item in pflicht_status["items"] if item["key"] == "kalkulation")
        pflicht_kunde = next(item for item in pflicht_status["items"] if item["key"] == "anspruchsteller")
        check(
            "Pflichtunterlagen nennen genaues Upload-Ziel",
            pflicht_kalkulation["anchor"] == "pflicht-upload"
            and "Gutachten" in pflicht_kalkulation["required"]
            and "Reparaturkosten" in pflicht_kalkulation["required"],
            str(pflicht_kalkulation),
        )
        check(
            "Fehlende Kundendaten haben Klickziel",
            pflicht_kunde["status"] == "missing"
            and pflicht_kunde["anchor"] == "pflicht-kunde"
            and "Versicherungsnehmer" in pflicht_kunde["required"],
            str(pflicht_kunde),
        )
        check(
            "OpenAI-Fehler wird benutzerfreundlich angezeigt",
            "https://api.openai.com" not in portal.friendly_analysis_error(
                "401 Client Error: Unauthorized for url: https://api.openai.com/v1/chat/completions",
                "OpenAI",
            )
            and "OpenAI-Zugang" in portal.friendly_analysis_error("401 Unauthorized", "OpenAI"),
        )

        autohaus = portal.get_autohaus_by_slug("kaesmann")
        check("Autohaus Käsmann vorhanden", bool(autohaus))

        admin = portal.app.test_client()
        partner = portal.app.test_client()
        with admin.session_transaction() as session:
            session["admin"] = True
        with partner.session_transaction() as session:
            session["partner_autohaus_id"] = autohaus["id"]

        route_checks = (
            ("Admin Dashboard", admin, "/admin"),
            ("Admin Kalender", admin, "/admin/kalender"),
            ("Admin News", admin, "/admin/news"),
            ("Admin E-Mail", admin, "/admin/emails"),
            ("Admin Einkauf", admin, "/admin/einkauf"),
            ("Partner Dashboard", partner, "/partner/kaesmann/dashboard"),
            ("Partner Neues Fahrzeug", partner, "/partner/kaesmann/neu"),
            ("Partner Angebot neu", partner, "/partner/kaesmann/angebot/neu"),
        )
        for label, client, url in route_checks:
            response = client.get(url)
            check(label, response.status_code == 200, f"Status {response.status_code}")

        sparkasse_csv = (
            "Kontonummer;DEMO\n"
            "Zeitraum;01.05.2026-31.05.2026\n"
            "Buchungstag;Valuta;Auftraggeber/Empfänger;Verwendungszweck;Soll;Haben;Währung\n"
            "02.05.2026;02.05.2026;Topcolor GmbH;Rechnung RE-2002 Material;99,95;;EUR\n"
            "03.05.2026;03.05.2026;Auto Pfaff;Zahlung RG-3003;;250,00;EUR\n"
        )
        parsed_bank_csv = portal.parse_kontoauszug_buchungen(sparkasse_csv, "sparkasse.csv")
        check(
            "Kontoauszug-CSV erkennt Vorzeilen und Soll/Haben",
            len(parsed_bank_csv) == 2
            and parsed_bank_csv[0]["betrag"] == -99.95
            and parsed_bank_csv[1]["betrag"] == 250.0
            and parsed_bank_csv[0]["buchung_datum"] == "02.05.2026",
            str(parsed_bank_csv),
        )
        check(
            "Kontoauszug-Betrag wertet S/H und Unicode-Minus richtig",
            portal.parse_bank_amount("99,95 S") == -99.95
            and portal.parse_bank_amount("100,00 H") == 100.0
            and portal.parse_bank_amount("−25,20 EUR") == -25.2,
        )
        typed_bank_csv = (
            "Buchungstag;Name;Verwendungszweck;Umsatzart;Betrag\n"
            "04.05.2026;Topcolor GmbH;Materialrechnung;Lastschrift;88,90\n"
            "05.05.2026;Autohaus Test;Rechnung RG-4004;Zahlungseingang;420,00\n"
        )
        typed_bank_rows = portal.parse_kontoauszug_buchungen(typed_bank_csv, "umsatzart.csv")
        check(
            "Kontoauszug wertet Lastschrift als Ausgabe und Zahlungseingang als Einnahme",
            len(typed_bank_rows) == 2
            and typed_bank_rows[0]["betrag"] == -88.9
            and typed_bank_rows[0]["richtung"] == "Ausgang"
            and typed_bank_rows[1]["betrag"] == 420.0
            and typed_bank_rows[1]["richtung"] == "Eingang",
            str(typed_bank_rows),
        )
        check(
            "Kontoauszug-Auswertung korrigiert klare Einnahmen und Ausgaben",
            portal.effective_bank_booking_amount(
                {"betrag": 88.9, "name": "Topcolor GmbH", "verwendungszweck": "SEPA Lastschrift Material"}
            )
            == -88.9
            and portal.effective_bank_booking_amount(
                {"betrag": -420.0, "name": "Autohaus Test", "verwendungszweck": "Zahlungseingang Rechnung"}
            )
            == 420.0,
        )
        volksbank_text = (
            "Gesamtumsatz: 22.306,09 S 10.455,66 H\n"
            "alter Kontostand vom 30.12.2025\n"
            "Wert Vorgang\n"
            "20.01. 20.01. Gutschrift PN:931 357,00 H\n"
            " ATT KFZ-Service UG (haftungsbeschraenkt)\n"
            " RE 0162\n"
            "22.01. 22.01. Gutschrift PN:931 483,46 H\n"
            " BGV AG\n"
            " Doppelzahlung Abbuchung Fahrzeugvers. EREF: 048000431184\n"
            "26.01. 25.01. Echtzeitueberweisung PN:801 845,39 S\n"
            " AOK Baden-Wuerttemberg\n"
            " 72759229 SecureGo plus IBAN: DE90600501017404040834\n"
            "30.01. 31.01. Abschluss lt. Anlage 1 PN:905 20,15 S\n"
            " ------------------------------------------------------------\n"
            " neuer Kontostand vom 30.01.2026 11.387,72 H\n"
            "Anlage 1\n"
            " 0,15 Ueberweisung beleglos 21 bis 31.01. 3,15 S\n"
        )
        volksbank_rows = portal.parse_kontoauszug_buchungen(
            volksbank_text,
            "291307_2026_Nr.001_Kontoauszug_vom_2026.01.31_20260501213057.pdf",
        )
        check(
            "Volksbank-PDF-Struktur nutzt S/H, Jahr aus Dateiname und ignoriert Saldo/Anlage",
            len(volksbank_rows) == 4
            and volksbank_rows[0]["buchung_datum"] == "20.01.2026"
            and volksbank_rows[0]["betrag"] == 357.0
            and volksbank_rows[1]["betrag"] == 483.46
            and volksbank_rows[2]["betrag"] == -845.39
            and volksbank_rows[3]["betrag"] == -20.15,
            str(volksbank_rows),
        )
        check(
            "Kontoauszug-Auswertung respektiert Bankmarker bei gemischtem Text",
            portal.effective_bank_booking_amount(volksbank_rows[1]) == 483.46,
            str(volksbank_rows[1]),
        )
        abgang_zugang_csv = (
            "Buchungstag;Empfänger;Verwendungszweck;Abgang;Zugang\n"
            "06.05.2026;Miete GmbH;Miete Werkstatt;1.250,00;\n"
            "07.05.2026;Kunde Bar;Zahlung Rechnung;;750,00\n"
        )
        abgang_zugang_rows = portal.parse_kontoauszug_buchungen(abgang_zugang_csv, "abgang-zugang.csv")
        check(
            "Kontoauszug wertet Abgang/Zugang-Spalten richtig",
            len(abgang_zugang_rows) == 2
            and abgang_zugang_rows[0]["betrag"] == -1250.0
            and abgang_zugang_rows[1]["betrag"] == 750.0,
            str(abgang_zugang_rows),
        )
        parsed_bank_text = portal.parse_kontoauszug_buchungen(
            "01.05.2026\nFlow Kunde\nZahlung Rechnung RG-1001\n123,45 EUR\nNeuer Saldo 9.999,99 EUR\n",
            "kontoauszug.txt",
        )
        check(
            "Kontoauszug-TXT erkennt mehrzeilige Buchungen",
            len(parsed_bank_text) == 1
            and parsed_bank_text[0]["betrag"] == 123.45
            and "RG-1001" in parsed_bank_text[0]["verwendungszweck"],
            str(parsed_bank_text),
        )
        parsed_lastschrift_text = portal.parse_kontoauszug_buchungen(
            "08.05.2026\nSEPA Lastschrift Topcolor GmbH\nMaterialrechnung RE-5005\n64,20 EUR\n",
            "lastschrift.txt",
        )
        check(
            "Kontoauszug-TXT wertet Lastschrift-Block als Ausgabe",
            len(parsed_lastschrift_text) == 1
            and parsed_lastschrift_text[0]["betrag"] == -64.2
            and parsed_lastschrift_text[0]["richtung"] == "Ausgang",
            str(parsed_lastschrift_text),
        )

        db = portal.get_db()
        try:
            db.execute(
                """
                INSERT INTO lexware_rechnungen
                  (voucher_id, voucher_type, richtung, status, payment_status, voucher_status,
                   voucher_number, contact_name, total_amount, open_amount, currency,
                   voucher_date, due_date, lexware_url, raw_json, zuletzt_synced_am, erstellt_am, geaendert_am)
                VALUES ('flow-konto-1', 'invoice', 'Einnahme', 'offen', '', 'open',
                        'RG-1001', 'Flow Kunde', 123.45, 123.45, 'EUR',
                        '01.05.2026', '15.05.2026', '', '{}', '', ?, ?)
                ON CONFLICT(voucher_id) DO UPDATE SET
                  status='offen',
                  open_amount=123.45,
                  total_amount=123.45,
                  voucher_number='RG-1001',
                  contact_name='Flow Kunde'
                """,
                (portal.now_str(), portal.now_str()),
            )
            db.execute(
                """
                INSERT INTO lexware_rechnungen
                  (voucher_id, voucher_type, richtung, status, payment_status, voucher_status,
                   voucher_number, contact_name, total_amount, open_amount, currency,
                   voucher_date, due_date, lexware_url, raw_json, zuletzt_synced_am, erstellt_am, geaendert_am)
                VALUES ('flow-konto-2', 'purchaseinvoice', 'Ausgabe', 'offen', '', 'open',
                        'RE-2002', 'Topcolor GmbH', 99.95, 99.95, 'EUR',
                        '02.05.2026', '16.05.2026', '', '{}', '', ?, ?)
                ON CONFLICT(voucher_id) DO UPDATE SET
                  status='offen',
                  open_amount=99.95,
                  total_amount=99.95,
                  voucher_number='RE-2002',
                  contact_name='Topcolor GmbH'
                """,
                (portal.now_str(), portal.now_str()),
            )
            db.commit()
        finally:
            db.close()
        wrong_match, wrong_score, wrong_hint = portal.match_kontoauszug_buchung_to_rechnung(
            {"betrag": 50.0, "name": "Flow Kunde", "verwendungszweck": "Zahlung RG-1001"}
        )
        check(
            "Kontoauszug ordnet abweichenden Betrag nicht automatisch zu",
            wrong_match is None and "Betrag weicht ab" in wrong_hint,
            f"score={wrong_score}, hint={wrong_hint}",
        )
        contact_only_match, contact_only_score, contact_only_hint = portal.match_kontoauszug_buchung_to_rechnung(
            {"betrag": 123.45, "name": "Flow Kunde", "verwendungszweck": "Zahlung ohne Nummer"}
        )
        check(
            "Kontoauszug braucht Rechnungsnummer fuer automatische Zuordnung",
            contact_only_match is None and "Rechnungsnummer" in contact_only_hint,
            f"score={contact_only_score}, hint={contact_only_hint}",
        )
        parsed_saldo_noise = portal.parse_kontoauszug_buchungen(
            "01.05.2026 Kontostand 1.870.592,36 EUR\n02.05.2026 Flow Kunde Zahlung RG-1001 123,45 EUR\nNeuer Saldo 1.870.715,81 EUR\n",
            "saldo.pdf",
        )
        check(
            "Kontoauszug ignoriert Saldo- und Kontostand-Zahlen",
            len(parsed_saldo_noise) == 1
            and parsed_saldo_noise[0]["betrag"] == 123.45
            and "RG-1001" in parsed_saldo_noise[0]["verwendungszweck"],
            str(parsed_saldo_noise),
        )
        huge_ok, huge_hint = portal.validate_kontoauszug_buchung(
            {
                "buchung_datum": "01.05.2026",
                "name": "Kontostand",
                "verwendungszweck": "Saldo 1.870.592,36 EUR",
                "betrag": 1870592.36,
            }
        )
        check(
            "Kontoauszug stoppt unrealistische Saldo-Buchungen",
            not huge_ok and "ungewoehnlich hoch" in huge_hint,
            huge_hint,
        )
        konto_response = admin.post(
            "/admin/rechnungen/kontoauszug",
            data=with_csrf(
                admin,
                {
                    "kontoauszug_dateien": (
                        BytesIO(
                            (
                                "Buchungstag;Name;Verwendungszweck;Betrag\n"
                                "01.05.2026;Flow Kunde;Zahlung RG-1001;123,45\n"
                                "02.05.2026;Topcolor GmbH;Rechnung RE-2002 Material;99,95 Soll\n"
                            ).encode("utf-8")
                        ),
                        "flow-konto.csv",
                    ),
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        check("Kontoauszug kann hochgeladen werden", konto_response.status_code == 302)
        duplicate_response = admin.post(
            "/admin/rechnungen/kontoauszug",
            data=with_csrf(
                admin,
                {
                    "kontoauszug_dateien": (
                        BytesIO(
                            (
                                "Buchungstag;Name;Verwendungszweck;Betrag\n"
                                "01.05.2026;Flow Kunde;Zahlung RG-1001;123,45\n"
                                "02.05.2026;Topcolor GmbH;Rechnung RE-2002 Material;99,95 Soll\n"
                            ).encode("utf-8")
                        ),
                        "flow-konto.csv",
                    ),
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        check("Doppelter Kontoauszug wird erkannt", duplicate_response.status_code == 302)
        db = portal.get_db()
        try:
            paid_row = db.execute("SELECT status, open_amount FROM lexware_rechnungen WHERE voucher_id='flow-konto-1'").fetchone()
            booking_row = db.execute("SELECT status, rechnung_id FROM kontoauszug_buchungen ORDER BY id DESC LIMIT 1").fetchone()
            duplicate_imports = db.execute("SELECT COUNT(*) AS count FROM kontoauszug_importe WHERE original_name='flow-konto.csv'").fetchone()["count"]
        finally:
            db.close()
        check("Doppelter Kontoauszug erzeugt keinen zweiten Import", duplicate_imports == 1, str(duplicate_imports))
        check(
            "Kontoauszug markiert sichere Zahlung als erledigt",
            paid_row and paid_row["status"] == "bezahlt" and float(paid_row["open_amount"] or 0) == 0,
            str(dict(paid_row) if paid_row else None),
        )
        db = portal.get_db()
        try:
            paid_supplier_row = db.execute("SELECT status, open_amount FROM lexware_rechnungen WHERE voucher_id='flow-konto-2'").fetchone()
        finally:
            db.close()
        check(
            "Kontoauszug markiert sichere Lieferanten-Ausgabe als erledigt",
            paid_supplier_row and paid_supplier_row["status"] == "bezahlt" and float(paid_supplier_row["open_amount"] or 0) == 0,
            str(dict(paid_supplier_row) if paid_supplier_row else None),
        )
        check(
            "Kontoauszug-Buchung bleibt nachvollziehbar",
            booking_row and booking_row["status"] == "zugeordnet" and int(booking_row["rechnung_id"] or 0) > 0,
            str(dict(booking_row) if booking_row else None),
        )
        rechnung_page = admin.get("/admin/rechnungen")
        rechnung_html = rechnung_page.get_data(as_text=True)
        check(
            "Kontoauszug-Auswertung zeigt nur die wichtigen Kennzahlen",
            rechnung_page.status_code == 200
            and "Auswertung letzter Kontoauszug" in rechnung_html
            and "Einnahmen" in rechnung_html
            and "Ausgaben" in rechnung_html
            and "Umsatz / Saldo" in rechnung_html
            and "123,45 EUR" in rechnung_html
            and "99,95 EUR" in rechnung_html
            and "Einzelbuchungen prüfen" in rechnung_html,
        )

        email_api_response = admin.post(
            "/api/werkstatt/emails",
            json={
                "from": {"name": "Flow Test Autohaus", "email": "flow@example.test"},
                "subject": "Flow Test Mail",
                "text": "Bitte im Werkstattportal sammeln.",
                "source": "flow-test",
            },
            headers=csrf_headers(admin),
        )
        check("E-Mail-API speichert Mail", email_api_response.status_code == 201, str(email_api_response.get_json()))
        email_items = portal.list_werkstatt_emails("aktiv", limit=20)
        email_item = next((item for item in email_items if item["betreff"] == "Flow Test Mail"), None)
        check("E-Mail-Zentrale listet API-Mail", bool(email_item), str(email_items[:2]))
        if email_item:
            response = admin.post(
                f"/admin/emails/{email_item['id']}/erledigt",
                data=with_csrf(admin),
                follow_redirects=False,
            )
            check("E-Mail als erledigt markierbar", response.status_code == 302)

        raw_imap_mail = (
            b"From: Lieferant Test <rechnung@example.test>\r\n"
            b"To: Werkstatt <werkstatt@example.test>\r\n"
            b"Subject: Rechnung IMAP Flow\r\n"
            b"Message-ID: <flow-imap-rechnung@example.test>\r\n"
            b"Date: Sun, 03 May 2026 12:15:00 +0200\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"\r\n"
            b"Bitte Rechnung RE-IMAP-1 ueber 129,90 EUR pruefen.\r\n"
        )
        imap_result = portal.import_werkstatt_email_raw_message(raw_imap_mail, source_uid="INBOX:flow-1")
        check("IMAP-Mail wird importiert", imap_result.get("created") is True, str(imap_result))
        imap_duplicate = portal.import_werkstatt_email_raw_message(raw_imap_mail, source_uid="INBOX:flow-1")
        check("IMAP-Dublette wird uebersprungen", imap_duplicate.get("created") is False, str(imap_duplicate))
        imap_email = portal.get_werkstatt_email(imap_result["id"])
        check(
            "IMAP-Mail enthaelt Absender, Betreff und Message-ID",
            imap_email
            and imap_email["absender_email"] == "rechnung@example.test"
            and imap_email["betreff"] == "Rechnung IMAP Flow"
            and imap_email["message_id"] == "<flow-imap-rechnung@example.test>"
            and imap_email["ziel_modul"] == "rechnungen",
            str(imap_email),
        )

        einkauf_response = admin.post(
            "/admin/einkauf/neu",
            data=with_csrf(
                admin,
                {
                    "titel": "Test Material",
                    "menge": "1 Dose",
                    "produkt_name": "PPG Flow Test Lack",
                    "produktbild_url": "https://example.test/material.png",
                    "auto_color_preis": "180,00",
                    "vergleich_preis": "150,00",
                    "vergleich_lieferant": "Flow Vergleich",
                    "notiz": "Flow-Test",
                    "qr_bilder": (BytesIO(b"flow-test-bild"), "topcolor-test.png"),
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        check("Admin kann Topcolor-Einkauf speichern", einkauf_response.status_code == 302)
        einkauf_items = portal.list_einkauf_items("offen", limit=20)
        einkauf_item = next((item for item in einkauf_items if item["titel"] == "Test Material"), None)
        check("Topcolor-Einkauf wird gelistet", bool(einkauf_item), str(einkauf_items[:2]))
        if einkauf_item:
            datei_response = admin.get(f"/admin/einkauf/datei/{einkauf_item['id']}")
            check("Topcolor-Einkaufsdatei öffnet", datei_response.status_code == 200)
            datei_response.close()
            check(
                "Topcolor-Einkauf speichert Produktbild und Preisradar",
                einkauf_item["produktbild_url"] == "https://example.test/material.png"
                and einkauf_item["preisradar"]["status"] == "danger"
                and einkauf_item["google_images_url"].startswith("https://www.google.com/search?tbm=isch"),
                str(einkauf_item),
            )
            vergleich_response = admin.get(
                f"/admin/einkauf/{einkauf_item['id']}/vergleich/google",
                follow_redirects=False,
            )
            check(
                "Topcolor-Einkauf leitet Google-Suche weiter",
                vergleich_response.status_code in {301, 302}
                and vergleich_response.headers.get("Location", "").startswith("https://www.google.com/search"),
                str(vergleich_response.headers),
            )

        rechnung_token = uuid.uuid4().hex[:8].upper()
        rechnung_artikelnummer = str(90000000 + (uuid.uuid4().int % 9999999))
        rechnung_produkt = f"3M Perfect-It Fast Cut Plus Flow {rechnung_token}"
        rechnung_response = admin.post(
            "/admin/einkauf/rechnung",
            data=with_csrf(
                admin,
                {
                    "lieferant": "Flow Lieferant",
                    "rechnung_dateien": (
                        BytesIO(
                            f"Art. {rechnung_artikelnummer} 2 Dose {rechnung_produkt} 42,50 EUR".encode(
                                "utf-8"
                            )
                        ),
                        f"flow-rechnung-{rechnung_token}.txt",
                    ),
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        check("Admin kann Einkaufsrechnung importieren", rechnung_response.status_code == 302)
        pruef_items = portal.list_einkauf_items(portal.EINKAUF_RECHNUNG_PRUEF_STATUS, limit=50)
        pruef_item = next(
            (
                item
                for item in pruef_items
                if item["artikelnummer"] == rechnung_artikelnummer
            ),
            None,
        )
        check("Rechnungsartikel landet zuerst in der Pruefliste", bool(pruef_item), str(pruef_items[:3]))
        if pruef_item:
            check(
                "Rechnungsartikel bekommt Bildsuche und Preisradar",
                pruef_item["google_images_url"].startswith("https://www.google.com/search?tbm=isch")
                and "status" in pruef_item["preisradar"],
                str(pruef_item),
            )
            anlegen_response = admin.post(
                f"/admin/einkauf/{pruef_item['id']}/artikel-anlegen",
                data=with_csrf(
                    admin,
                    {
                        "lieferant": pruef_item["lieferant"],
                        "kategorie": pruef_item["kategorie"],
                        "artikelnummer": pruef_item["artikelnummer"],
                        "produkt_name": pruef_item["produkt_name"],
                        "produkt_beschreibung": pruef_item["produkt_beschreibung"],
                        "produktbild_url": f"https://example.test/{rechnung_artikelnummer}.png",
                        "titel": pruef_item["titel"],
                        "menge": pruef_item["menge"],
                        "ve": pruef_item["ve"],
                        "stueckzahl": str(pruef_item["stueckzahl"]),
                        "auto_color_preis": pruef_item["auto_color_preis"],
                        "preisquelle": pruef_item["preisquelle"],
                        "angebotsstatus": pruef_item["angebotsstatus"],
                    },
                ),
                follow_redirects=False,
            )
            check("Rechnungsartikel kann freigegeben werden", anlegen_response.status_code == 302)
        artikel_items = portal.list_einkauf_artikel(limit=50)
        artikel_item = next(
            (
                item
                for item in artikel_items
                if item["artikelnummer"] == rechnung_artikelnummer
            ),
            None,
        )
        check("Freigegebener Rechnungsartikel landet im Artikelstamm", bool(artikel_item), str(artikel_items[:3]))
        if artikel_item:
            check(
                "Artikelstamm speichert Produktbild und Bildsuche",
                artikel_item["produktbild_url"] == f"https://example.test/{rechnung_artikelnummer}.png"
                and artikel_item["google_images_url"].startswith("https://www.google.com/search?tbm=isch"),
                str(artikel_item),
            )
            artikel_vergleich_response = admin.get(
                f"/admin/einkauf/artikel/{artikel_item['id']}/vergleich/google",
                follow_redirects=False,
            )
            check(
                "Angelegte Teile leiten Google-Suche weiter",
                artikel_vergleich_response.status_code in {301, 302}
                and artikel_vergleich_response.headers.get("Location", "").startswith("https://www.google.com/search"),
                str(artikel_vergleich_response.headers),
            )
            einkauf_page = admin.get("/admin/einkauf")
            einkauf_html = einkauf_page.get_data(as_text=True)
            check(
                "Einkauf zeigt angelegte Teile nach Kategorie",
                einkauf_page.status_code == 200
                and "Angelegte Teile" in einkauf_html
                and portal.einkauf_kategorie_label(artikel_item["kategorie"]) in einkauf_html
                and f"/admin/einkauf/artikel/{artikel_item['id']}/vergleich/google" in einkauf_html,
                einkauf_html[:500],
            )
            if pruef_item:
                check(
                    "Angelegter Rechnungsartikel ist aus der Produktanlage raus",
                    f"rechnung-position-{pruef_item['id']}" not in einkauf_html,
                    f"rechnung-position-{pruef_item['id']}",
                )

        response = partner.get("/partner/kaesmann/dashboard")
        check(
            "Partner Dashboard zeigt KI-Helfer",
            response.status_code == 200
            and "data-ki-assistent" in response.get_data(as_text=True)
            and "Wie kann ich Ihnen helfen?" in response.get_data(as_text=True),
        )
        original_get_openai_api_key = portal.get_openai_api_key
        portal.get_openai_api_key = lambda: ""
        try:
            response = partner.post(
                "/partner/kaesmann/ki/chat",
                json={"message": "Wie lade ich Bilder hoch?"},
                headers=csrf_headers(partner),
            )
        finally:
            portal.get_openai_api_key = original_get_openai_api_key
        payload = response.get_json() or {}
        check(
            "KI-Helfer antwortet ohne OpenAI mit Fallback",
            response.status_code == 200
            and "hochladen" in portal.normalize_document_text(payload.get("answer")),
            str(payload),
        )
        db = portal.get_db()
        ki_rows = db.execute(
            "SELECT COUNT(*) AS count FROM ki_assistent_nachrichten WHERE autohaus_id=?",
            (autohaus["id"],),
        ).fetchone()["count"]
        db.close()
        check("KI-Helfer speichert Frage und Antwort", ki_rows >= 2, str(ki_rows))
        response = partner.post(
            "/partner/kaesmann/ki/chat/loeschen",
            json={"auftrag_id": ""},
            headers=csrf_headers(partner),
        )
        check("KI-Helfer-Verlauf ist löschbar", response.status_code == 200)

        urlaub_response = partner.post(
            "/partner/kaesmann/neu",
            data=with_csrf(
                partner,
                {
                    "aktion": "speichern",
                    "kunde_name": "Betriebsurlaub Testkunde",
                    "kontakt_telefon": "01234",
                    "fahrzeug": "Urlaubstest Auto",
                    "kennzeichen": "MOS-URLAUB-26",
                    "analyse_text": "Test während Betriebsurlaub",
                    "beschreibung": "Kunde plant im Betriebsurlaub.",
                    "transport_art": "standard",
                    "annahme_datum": "2026-08-20",
                    "abholtermin": "2026-08-21",
                },
            ),
            follow_redirects=True,
        )
        urlaub_html = urlaub_response.get_data(as_text=True)
        check(
            "Kunde bekommt Betriebsurlaub-Hinweis bei Einplanung",
            urlaub_response.status_code == 200
            and "Achtung Betriebsurlaub" in urlaub_html
            and "19.08.2026 bis 04.09.2026" in urlaub_html,
            f"Status {urlaub_response.status_code}",
        )
        db = portal.get_db()
        urlaub_row = db.execute(
            """
            SELECT id
            FROM auftraege
            WHERE kennzeichen='MOS-URLAUB-26'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        db.close()
        if urlaub_row:
            portal.delete_auftrag(urlaub_row["id"])

        response = admin.post(
            "/admin/backup/download",
            data=with_csrf(admin),
            follow_redirects=False,
        )
        check("Admin kann Backup-ZIP herunterladen", response.status_code == 200, f"Status {response.status_code}")
        backup_payload = response.get_data()
        response.close()
        backup_bytes = BytesIO(backup_payload)
        with zipfile.ZipFile(backup_bytes) as archive:
            backup_names = set(archive.namelist())
        check(
            "Backup-ZIP enthält Datenbank und JSON-Export",
            "backup.json" in backup_names and "auftraege.db" in backup_names,
            str(sorted(backup_names)[:5]),
        )

        response = partner.post(
            "/partner/kaesmann/angebot/neu",
            data=with_csrf(partner, {
                "kunde_name": "Testkunde Codex",
                "telefon": "01234",
                "fahrzeug": "Audi Q7",
                "kennzeichen": "MOS-T 123",
                "fin_nummer": "WAUZZZTEST0000001",
                "auftragsnummer": "TEST-ANGEBOT-1",
                "analyse_text": "Kotflügel links lackieren",
                "beschreibung": "Kotflügel links lackieren. Bitte Angebot erstellen.",
                "transport_art": "standard",
                "annahme_datum": "02.05.2026",
                "abholtermin": "04.05.2026",
            }),
            follow_redirects=False,
        )
        check(
            "Angebotsanfrage erstellen leitet weiter",
            response.status_code in {302, 303},
            f"Status {response.status_code}",
        )
        angebot_id = extract_id_from_location(response.headers.get("Location", ""))
        check("Neue Angebots-ID erkannt", angebot_id > 0, response.headers.get("Location", ""))
        png_bytes = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde"
            b"\x00\x00\x00\x0cIDATx\x9cc\xf8\xff\xff?\x00\x05"
            b"\xfe\x02\xfeA\xe2!=\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        stored_image_name = f"angebot-{angebot_id}-schadenfoto.png"
        (test_uploads / stored_image_name).write_bytes(png_bytes)
        db = portal.get_db()
        db.execute(
            """
            INSERT INTO dateien
            (auftrag_id, original_name, stored_name, mime_type, size, quelle,
             kategorie, dokument_typ, extrahierter_text, extrakt_kurz,
             analyse_quelle, analyse_json, analyse_hinweis, hochgeladen_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                angebot_id,
                "schadenfoto.png",
                stored_image_name,
                "image/png",
                len(png_bytes),
                "autohaus",
                "standard",
                "Bilddokument",
                "",
                "Schadenfoto vom Autohaus",
                "",
                "",
                "",
                portal.now_str(),
            ),
        )
        db.commit()
        db.close()

        angebot = portal.get_auftrag(angebot_id)
        check("Neue Anfrage ist Angebotsphase", angebot["angebotsphase"])
        check("Anfrage ist noch nicht abgesendet", not angebot["angebot_abgesendet"])
        check(
            "Preisvorschlag wird nur berechnet",
            angebot["preisvorschlag"]["empfehlung"] == "190 € netto",
            str(angebot["preisvorschlag"]),
        )

        response = partner.post(
            f"/partner/kaesmann/angebot/{angebot_id}",
            data=with_csrf(partner, {
                "aktion": "submit_offer",
                "kunde_name": "Testkunde Codex",
                "fahrzeug": "Audi Q7",
                "kennzeichen": "MOS-T 123",
                "fin_nummer": "WAUZZZTEST0000001",
                "auftragsnummer": "TEST-ANGEBOT-1",
                "analyse_text": "Kotflügel links lackieren",
                "beschreibung": "Kotflügel links lackieren. Bitte Angebot erstellen.",
                "transport_art": "standard",
                "annahme_datum": "02.05.2026",
                "abholtermin": "04.05.2026",
            }),
            follow_redirects=False,
        )
        check("Kunde sendet Angebotsanfrage ab", response.status_code in {302, 303})
        angebot = portal.get_auftrag(angebot_id)
        check(
            "Angebot wartet auf Werkstatt",
            angebot["angebot_abgesendet"] and angebot["angebot_status"] == "angefragt",
            angebot["angebot_status"],
        )

        response = admin.get(f"/admin/auftrag/{angebot_id}")
        html = response.get_data(as_text=True)
        check("Admin-Angebotsseite lädt", response.status_code == 200)
        check(
            "Preisvorschlag im Admin sichtbar",
            "Preisvorschlag nach interner Preisliste" in html and "190 € netto" in html,
        )
        response = admin.get("/admin")
        admin_html = response.get_data(as_text=True)
        check(
            "Admin-Angebotskarte zeigt Unterlage mit Bildvorschau",
            response.status_code == 200
            and "schadenfoto.png" in admin_html
            and f"/admin/datei/" in admin_html
            and "Unterlagen und Bilder" in admin_html,
            f"Status {response.status_code}",
        )

        response = admin.post(
            f"/admin/angebot/{angebot_id}/senden",
            data=with_csrf(admin, {
                "werkstatt_angebot_preis": "190 € netto",
                "werkstatt_angebot_text": "Reparatur gemäß Anfrage: Kotflügel links lackieren.",
                "werkstatt_angebot_notiz": "Preis netto, inklusive Material.",
            }),
            follow_redirects=False,
        )
        check("Werkstatt sendet Angebot", response.status_code in {302, 303})
        angebot = portal.get_auftrag(angebot_id)
        check(
            "Angebot abgegeben gespeichert",
            angebot["angebot_status"] == "angebot_abgegeben"
            and angebot["werkstatt_angebot_preis"] == "190 € netto"
            and angebot["werkstatt_angebot_notiz"] == "Preis netto, inklusive Material.",
            angebot["angebot_status"],
        )

        response = admin.post(
            f"/admin/angebot/{angebot_id}/senden",
            data=with_csrf(admin, {
                "werkstatt_angebot_preis": "210 € netto",
                "werkstatt_angebot_text": "Reparatur gemäß Anfrage: Kotflügel links lackieren.",
                "werkstatt_angebot_notiz": "Preis netto, zusätzlich kleine Beilackierung.",
            }),
            follow_redirects=False,
        )
        check("Werkstatt ändert Angebot", response.status_code in {302, 303})
        angebot = portal.get_auftrag(angebot_id)
        check(
            "Geändertes Angebot gespeichert",
            angebot["angebot_status"] == "angebot_abgegeben"
            and angebot["werkstatt_angebot_preis"] == "210 € netto"
            and angebot["werkstatt_angebot_notiz"] == "Preis netto, zusätzlich kleine Beilackierung.",
            (
                f"Status {angebot['angebot_status']}, "
                f"Preis {angebot['werkstatt_angebot_preis']}, "
                f"Notiz {angebot['werkstatt_angebot_notiz']}"
            ),
        )

        response = partner.get(f"/partner/kaesmann/angebot/{angebot_id}")
        partner_html = response.get_data(as_text=True)
        check(
            "Partner sieht geänderte Angebotsnotiz",
            response.status_code == 200
            and "210 € netto" in partner_html
            and "zusätzlich kleine Beilackierung" in partner_html,
            f"Status {response.status_code}",
        )
        partner_hat_angebotsbild = (
            "schadenfoto.png" in partner_html
            and f"/partner/kaesmann/datei/" in partner_html
            and "Original öffnen" in partner_html
        )
        check(
            "Partner sieht Angebotsbild mit Öffnen-Link",
            partner_hat_angebotsbild,
            "" if partner_hat_angebotsbild else "Angebotsbild fehlt",
        )

        response = partner.post(
            f"/partner/kaesmann/angebot/{angebot_id}/annehmen",
            data=with_csrf(partner),
            follow_redirects=False,
        )
        check("Kunde nimmt Angebot an", response.status_code in {302, 303})
        auftrag = portal.get_auftrag(angebot_id)
        check(
            "Annahme macht normalen Auftrag",
            not auftrag["angebotsphase"] and auftrag["angebot_status"] == "angenommen",
            auftrag["angebot_status"],
        )

        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        check("Partner-Auftrag nach Annahme öffnet", response.status_code == 200)

        response = partner.post(
            f"/partner/kaesmann/auftrag/{angebot_id}/chat",
            data=with_csrf(partner, {"nachricht": "Ist die Rückgabe am Nachmittag möglich?"}),
            follow_redirects=False,
        )
        check("Partner sendet Chat-Nachricht", response.status_code in {302, 303})
        db = portal.get_db()
        chat = db.execute(
            """
            SELECT *
            FROM chat_nachrichten
            WHERE auftrag_id=? AND absender='autohaus'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        db.close()
        check("Chat-Nachricht ist für Admin ungelesen", bool(chat) and chat["gelesen_admin"] == 0, str(chat))
        response = admin.get("/admin")
        admin_dashboard_html = response.get_data(as_text=True)
        check(
            "Admin-Dashboard zeigt neue Chat-Nachricht",
            "Neue Chat-Nachrichten" in admin_dashboard_html
            and "Ist die Rückgabe am Nachmittag möglich?" in admin_dashboard_html,
        )
        response = admin.get("/admin/postfach")
        admin_postfach_html = response.get_data(as_text=True)
        check(
            "Werkstatt-Postfach zeigt Chat-Nachricht",
            response.status_code == 200
            and "Werkstatt-Postfach" in admin_postfach_html
            and "Ist die Rückgabe am Nachmittag möglich?" in admin_postfach_html,
            f"Status {response.status_code}",
        )
        response = admin.post(
            f"/admin/postfach/admin-chat-{chat['id']}/loeschen",
            data=with_csrf(admin, {"next": "/admin/postfach"}),
            follow_redirects=True,
        )
        check(
            "Werkstatt-Postfach-Nachricht ist löschbar",
            response.status_code == 200
            and "Ist die Rückgabe am Nachmittag möglich?" not in response.get_data(as_text=True),
        )
        response = admin.get(f"/admin/auftrag/{angebot_id}")
        admin_auftrag_html = response.get_data(as_text=True)
        check(
            "Admin sieht Chat im Auftrag",
            response.status_code == 200 and "Chat mit Autohaus" in admin_auftrag_html,
            f"Status {response.status_code}",
        )
        db = portal.get_db()
        chat = db.execute("SELECT gelesen_admin FROM chat_nachrichten WHERE id=?", (chat["id"],)).fetchone()
        db.close()
        check("Admin-Öffnen markiert Chat als gelesen", chat["gelesen_admin"] == 1)
        response = admin.post(
            f"/admin/auftrag/{angebot_id}/chat",
            data=with_csrf(admin, {"nachricht": "Ja, Rückgabe am Nachmittag passt."}),
            follow_redirects=False,
        )
        check("Admin antwortet im Chat", response.status_code in {302, 303})
        db = portal.get_db()
        hinweis = db.execute(
            """
            SELECT *
            FROM benachrichtigungen
            WHERE auftrag_id=? AND titel='Neue Chat-Nachricht'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        db.close()
        check("Admin-Chat erzeugt Autohaus-Hinweis", bool(hinweis), str(hinweis))
        response = partner.get("/partner/kaesmann/postfach")
        partner_postfach_html = response.get_data(as_text=True)
        check(
            "Autohaus-Postfach zeigt Werkstatt-Hinweis",
            response.status_code == 200
            and "Postfach" in partner_postfach_html
            and "Neue Chat-Nachricht" in partner_postfach_html,
            f"Status {response.status_code}",
        )
        response = partner.post(
            f"/partner/kaesmann/postfach/autohaus-hinweis-{hinweis['id']}/loeschen",
            data=with_csrf(partner, {"next": "/partner/kaesmann/postfach"}),
            follow_redirects=True,
        )
        check(
            "Autohaus-Postfach-Nachricht ist löschbar",
            response.status_code == 200
            and "Neue Chat-Nachricht" not in response.get_data(as_text=True),
        )
        db = portal.get_db()
        hinweis_status = db.execute(
            "SELECT gelesen FROM benachrichtigungen WHERE id=?",
            (hinweis["id"],),
        ).fetchone()
        db.close()
        check(
            "Autohaus-Hinweis wird beim Löschen als gelesen markiert",
            bool(hinweis_status and hinweis_status["gelesen"] == 1),
            str(hinweis_status),
        )
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        partner_chat_html = response.get_data(as_text=True)
        check(
            "Partner sieht Werkstatt-Antwort im Chat",
            "Ja, Rückgabe am Nachmittag passt." in partner_chat_html,
            partner_chat_html[:300],
        )
        portal.add_benachrichtigung(
            angebot_id,
            "Flow-Test alter Entfernen-Hinweis",
            "Dieser Hinweis prueft den alten Entfernen-Button.",
        )
        db = portal.get_db()
        alter_hinweis = db.execute(
            """
            SELECT *
            FROM benachrichtigungen
            WHERE auftrag_id=? AND titel='Flow-Test alter Entfernen-Hinweis'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        db.close()
        check("Alter Entfernen-Hinweis angelegt", bool(alter_hinweis), str(alter_hinweis))
        response = partner.post(
            f"/partner/kaesmann/hinweis/{alter_hinweis['id']}/entfernen",
            data=with_csrf(partner),
            follow_redirects=True,
        )
        db = portal.get_db()
        alter_hinweis_status = db.execute(
            "SELECT gelesen FROM benachrichtigungen WHERE id=?",
            (alter_hinweis["id"],),
        ).fetchone()
        db.close()
        sichtbare_hinweise = portal.list_autohaus_benachrichtigungen(autohaus["id"], limit=200)
        check(
            "Alter Entfernen-Button entfernt Autohaus-Hinweis",
            response.status_code == 200
            and alter_hinweis_status
            and alter_hinweis_status["gelesen"] == 1
            and all(item["id"] != alter_hinweis["id"] for item in sichtbare_hinweise)
            and f"autohaus-hinweis-{alter_hinweis['id']}" in portal.postfach_hidden_keys("autohaus", autohaus["id"]),
            f"Status {response.status_code}",
        )

        db = portal.get_db()
        db.execute(
            """
            INSERT INTO dateien
            (auftrag_id, original_name, stored_name, mime_type, size, quelle, kategorie,
             dokument_typ, extrahierter_text, extrakt_kurz, analyse_quelle, analyse_json,
             analyse_hinweis, hochgeladen_am)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                angebot_id,
                "lackierauftrag-test.pdf",
                "codex-test.pdf",
                "application/pdf",
                0,
                "autohaus",
                "standard",
                "Lackierauftrag",
                "Audi Q7 FIN WAUZZZTEST0000001 Kennzeichen MOS-T 123 Kotflügel links lackieren",
                "Kotflügel links lackieren",
                "test",
                json.dumps(
                    {
                        "document_type": "Lackierauftrag",
                        "vehicle_type": "Audi Q7",
                        "fin_nummer": "WAUZZZTEST0000001",
                        "auftragsnummer": "TEST-ANGEBOT-1",
                        "kennzeichen": "MOS-T 123",
                        "auftrags_datum": "02.05.2026",
                        "fertig_bis": "04.05.2026",
                        "rep_max_kosten": "",
                        "offene_bauteile": ["Kotflügel links"],
                        "erledigte_bauteile": [],
                        "kurzanalyse": "Kotflügel links lackieren",
                        "lesefassung": "Lackierauftrag Audi Q7, Kotflügel links lackieren.",
                        "needs_review": False,
                        "review_reason": "",
                        "confidence": 0.91,
                    },
                    ensure_ascii=False,
                ),
                "",
                portal.now_str(),
            ),
        )
        db.commit()
        db.close()
        portal.reset_document_review_checks(angebot_id, "Testunterlage zur Prüfung")
        auftrag = portal.get_auftrag(angebot_id)
        pruefung = portal.list_document_review_items(angebot_id, auftrag)
        check("Dokument-Prüfansicht liefert Felder", bool(pruefung and pruefung[0]["items"]))
        check(
            "Dokument-Prüfansicht erkennt übernommene FIN",
            any(item["key"] == "fin_nummer" and item["active"] for item in pruefung[0]["items"]),
            str(pruefung[0]["items"]),
        )
        response = admin.get(f"/admin/auftrag/{angebot_id}")
        check(
            "Admin zeigt Dokument-Prüfansicht",
            "Erkannte Felder aus Unterlagen" in response.get_data(as_text=True),
        )
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        check(
            "Partner-Auftrag verweist auf Dokumentseite",
            "Dokumente anschauen" in response.get_data(as_text=True),
        )
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}/dokumente")
        check(
            "Partner zeigt Dokument-Prüfansicht auf Dokumentseite",
            "Erkannte Felder" in response.get_data(as_text=True),
        )
        response = partner.post(
            f"/partner/kaesmann/auftrag/{angebot_id}/dokumente/geprueft",
            data=with_csrf(partner, {}),
            follow_redirects=False,
        )
        auftrag = portal.get_auftrag(angebot_id)
        check("Autohaus-Prüfung speicherbar", response.status_code in {302, 303}, response.headers.get("Location", ""))
        check(
            "Nach Autohaus-Prüfung bleibt Werkstatt-Prüfung offen",
            auftrag["analyse_autohaus_geprueft"] == 1
            and auftrag["analyse_werkstatt_geprueft"] == 0
            and auftrag["analyse_pruefen"],
            f"autohaus={auftrag['analyse_autohaus_geprueft']} werkstatt={auftrag['analyse_werkstatt_geprueft']} pruefen={auftrag['analyse_pruefen']}",
        )
        response = admin.post(
            f"/admin/auftrag/{angebot_id}/dokumente/geprueft",
            data=with_csrf(admin, {}),
            follow_redirects=False,
        )
        auftrag = portal.get_auftrag(angebot_id)
        check("Werkstatt-Prüfung speicherbar", response.status_code in {302, 303})
        check(
            "Doppelte Dokumentprüfung schließt Prüfwarnung",
            auftrag["analyse_autohaus_geprueft"] == 1
            and auftrag["analyse_werkstatt_geprueft"] == 1
            and not auftrag["analyse_pruefen"],
            f"autohaus={auftrag['analyse_autohaus_geprueft']} werkstatt={auftrag['analyse_werkstatt_geprueft']} pruefen={auftrag['analyse_pruefen']}",
        )

        response = admin.post(
            f"/admin/status/{angebot_id}/2",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        check("Status vorwärts möglich", response.status_code in {302, 303})
        check("Status ist Eingeplant", portal.get_auftrag(angebot_id)["status"] == 2)

        response = admin.post(
            f"/admin/status/{angebot_id}/1",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        check("Status rückwärts möglich", response.status_code in {302, 303})
        check("Status ist wieder Angelegt", portal.get_auftrag(angebot_id)["status"] == 1)

        response = admin.post(
            f"/admin/status/{angebot_id}/4",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        check("Status Fertig möglich", response.status_code in {302, 303})
        auftrag = portal.get_auftrag(angebot_id)
        check("Status ist Fertig", auftrag["status"] == 4)
        autohaus_cockpit = portal.autohaus_dashboard_daten([auftrag])
        check(
            "Partner-Dashboard zählt heute fertig nach Statuswechsel",
            any(a["id"] == angebot_id for a in autohaus_cockpit["heute_fertig"]),
            str([a["id"] for a in autohaus_cockpit["heute_fertig"]]),
        )
        admin_cockpit = portal.dashboard_daten([auftrag])
        check(
            "Admin-Dashboard zählt heute fertig nach Statuswechsel",
            any(a["id"] == angebot_id for a in admin_cockpit["heute_fertig"]),
            str([a["id"] for a in admin_cockpit["heute_fertig"]]),
        )

        response = admin.post(
            f"/admin/status/{angebot_id}/5",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        auftrag = portal.get_auftrag(angebot_id)
        check("Status Zurückgegeben möglich", response.status_code in {302, 303})
        check("Status ist Zurückgegeben", auftrag["status"] == 5)
        check("Rückgabetermin wird gesetzt", bool(auftrag["abholtermin"]), auftrag["abholtermin"])

        response = admin.get(f"/admin/auftrag/{angebot_id}")
        admin_auftrag_html = response.get_data(as_text=True)
        check(
            "Zurückgegebener Auftrag zeigt Rechnungslink",
            response.status_code == 200 and "Rechnung schreiben in Lexware" in admin_auftrag_html,
            f"Status {response.status_code}",
        )
        response = admin.get(f"/admin/auftrag/{angebot_id}/rechnung")
        rechnung_html = response.get_data(as_text=True)
        check(
            "Rechnungsseite nach Rückgabe erreichbar",
            response.status_code == 200 and "Rechnung schreiben" in rechnung_html and "Direkt in Lexware" in rechnung_html,
            f"Status {response.status_code}",
        )
        response = admin.post(
            f"/admin/auftrag/{angebot_id}/rechnung",
            data=with_csrf(
                admin,
                {
                    "lexware_kunde_angelegt": "on",
                    "rechnung_geschrieben": "on",
                    "rechnung_nummer": "FLOW-BONUS-1",
                    "bonus_netto_betrag": "3.670,40",
                },
            ),
            follow_redirects=False,
        )
        auftrag = portal.get_auftrag(angebot_id)
        check("Rechnungsstatus speichert Bonusbetrag", response.status_code in {302, 303})
        check(
            "Zurueckgegebener Auftrag fuehrt Bonusbetrag",
            round(float(auftrag["bonus_netto_betrag"] or 0), 2) == 3670.40
            and bool(auftrag["bonus_preis_aktualisiert_am"]),
            str(
                {
                    "bonus": auftrag["bonus_netto_betrag"],
                    "aktualisiert": auftrag["bonus_preis_aktualisiert_am"],
                }
            ),
        )
        response = admin.post(
            f"/admin/auftrag/{angebot_id}/rechnung/upload",
            data=with_csrf(
                admin,
                {
                    "rechnung_dateien": (
                        BytesIO(
                            b"Rechnung Nr. FALSCH-1\n"
                            b"Kennzeichen: HD-LB 197\n"
                            b"Fahrzeug: Mercedes C300\n"
                            b"Gesamt netto 999,00 EUR\n"
                        ),
                        "falsche-rechnung.txt",
                    ),
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        check("Falsche Rechnung wird verarbeitet", response.status_code in {302, 303})
        db = portal.get_db()
        try:
            falsche_rechnung = db.execute(
                "SELECT COUNT(*) AS count FROM dateien WHERE auftrag_id=? AND original_name='falsche-rechnung.txt'",
                (angebot_id,),
            ).fetchone()["count"]
        finally:
            db.close()
        check("Falsche Rechnung wird nicht gespeichert", falsche_rechnung == 0, str(falsche_rechnung))

        response = admin.post(
            f"/admin/auftrag/{angebot_id}/rechnung/upload",
            data=with_csrf(
                admin,
                {
                    "rechnung_dateien": (
                        BytesIO(
                            (
                                f"Rechnung Nr. FLOW-BONUS-2\n"
                                f"Auftrag #{angebot_id}\n"
                                f"Autohaus: Kaesmann\n"
                                f"Fahrzeug: Audi Q7\n"
                                f"Kennzeichen: MOS-T 123\n"
                                f"FIN: WAUZZZTEST0000001\n"
                                f"Gesamt netto 3.670,40 EUR\n"
                            ).encode("utf-8")
                        ),
                        "passende-rechnung.txt",
                    ),
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        check("Passende Rechnung kann hochgeladen werden", response.status_code in {302, 303})
        auftrag = portal.get_auftrag(angebot_id)
        db = portal.get_db()
        try:
            rechnung_datei = db.execute(
                """
                SELECT *
                FROM dateien
                WHERE auftrag_id=? AND kategorie='rechnung' AND original_name='passende-rechnung.txt'
                ORDER BY id DESC
                """,
                (angebot_id,),
            ).fetchone()
        finally:
            db.close()
        check("Passende Rechnung wird als Rechnung gespeichert", bool(rechnung_datei), str(rechnung_datei))
        check(
            "Passende Rechnung aktualisiert Rechnungsdaten",
            auftrag["rechnung_status"] == "geschrieben"
            and auftrag["rechnung_nummer"] == "FLOW-BONUS-2"
            and round(float(auftrag["bonus_netto_betrag"] or 0), 2) == 3670.40,
            str(
                {
                    "status": auftrag["rechnung_status"],
                    "nummer": auftrag["rechnung_nummer"],
                    "betrag": auftrag["bonus_netto_betrag"],
                }
            ),
        )
        response = admin.get(f"/admin/auftrag/{angebot_id}")
        admin_auftrag_html = response.get_data(as_text=True)
        check(
            "Admin-Auftrag zeigt Rechnungsupload und Pruefung",
            response.status_code == 200
            and "Werkstatt-Rechnung hochladen" in admin_auftrag_html
            and "passende-rechnung.txt" in admin_auftrag_html
            and "Prüfung bestanden" in admin_auftrag_html,
            f"Status {response.status_code}",
        )
        response = partner.get("/partner/kaesmann/dashboard")
        partner_dashboard_html = response.get_data(as_text=True)
        check(
            "Partner-Dashboard verlinkt Bonusmodell als Rubrik",
            response.status_code == 200
            and "/partner/kaesmann/bonusmodell" in partner_dashboard_html
            and "Bonusmodell klar erklärt" not in partner_dashboard_html,
        )
        response = partner.get("/partner/kaesmann/bonusmodell")
        partner_bonus_html = response.get_data(as_text=True)
        check(
            "Bonusmodell-Rubrik aktualisiert Bonus nach Rechnungsbetrag",
            response.status_code == 200
            and "3.670,40" in partner_bonus_html
            and "73,41" in partner_bonus_html
            and "Bonusmodell klar erklärt" in partner_bonus_html,
        )
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        partner_auftrag_html = response.get_data(as_text=True)
        check(
            "Partner-Auftrag zeigt Bonusbetrag nach Aktualisierung",
            response.status_code == 200
            and "3.670,40" in partner_auftrag_html
            and "Monatsbonus" in partner_auftrag_html,
        )

        response = admin.post(
            f"/admin/auftrag/{angebot_id}/fertigbilder",
            data=with_csrf(
                admin,
                {
                    "fertigbilder": (
                        BytesIO(b"codex-test-fertigbild"),
                        "fertigbild-test.jpg",
                    )
                },
            ),
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        check("Admin lädt Fertigbild hoch", response.status_code in {302, 303})
        db = portal.get_db()
        fertigbild = db.execute(
            """
            SELECT *
            FROM dateien
            WHERE auftrag_id=? AND kategorie='fertigbild'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        hinweis = db.execute(
            """
            SELECT *
            FROM benachrichtigungen
            WHERE auftrag_id=? AND titel='Neue Fertigbilder'
            ORDER BY id DESC
            """,
            (angebot_id,),
        ).fetchone()
        db.close()
        check("Fertigbild als Fertigbild gespeichert", bool(fertigbild), str(fertigbild))
        check("Fertigbild-Datei liegt im Upload-Ordner", (test_uploads / fertigbild["stored_name"]).exists())
        check("Fertigbild erzeugt Autohaus-Hinweis", bool(hinweis), str(hinweis))
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}")
        partner_html = response.get_data(as_text=True)
        check(
            "Partner-Auftrag verlinkt Dokumente nach Fertigbild",
            response.status_code == 200 and "Dokumente anschauen" in partner_html,
            f"Status {response.status_code}",
        )
        check(
            "Partner sieht Werkstatt-Hinweis",
            "Neue Fertigbilder" in partner_html,
            partner_html[:300],
        )
        response = partner.get(f"/partner/kaesmann/auftrag/{angebot_id}/dokumente")
        partner_html = response.get_data(as_text=True)
        check(
            "Partner sieht Fertigbild auf Dokumentseite",
            response.status_code == 200 and "fertigbild-test.jpg" in partner_html,
            f"Status {response.status_code}",
        )

        db = portal.get_db()
        datei = db.execute("SELECT id FROM dateien LIMIT 1").fetchone()
        db.close()
        if datei:
            response = admin.get(f"/admin/datei/{datei['id']}")
            check("Admin Originaldatei öffnen Route", response.status_code in {200, 404})
            response.close()
            response = admin.get(f"/admin/datei/{datei['id']}/download")
            check("Admin Originaldatei Download Route", response.status_code in {200, 404})
            response.close()
        else:
            print("[INFO] Keine Datei in Testdatenbank gefunden, Datei-Routen übersprungen.")

        reklamation_id = portal.add_reklamation(angebot_id, "autohaus", "Nacharbeit nötig")
        response = admin.post(
            f"/admin/auftrag/{angebot_id}/reklamation-neu-planen",
            data=with_csrf(
                admin,
                {
                    f"start_datum_{angebot_id}": "2026-05-05",
                    f"fertig_datum_{angebot_id}": "2026-05-06",
                },
            ),
            follow_redirects=False,
        )
        check("Offene Reklamation kann neu eingeplant werden", response.status_code in {302, 303})
        neu_geplant = portal.get_auftrag(angebot_id)
        check(
            "Reklamation setzt Prozess wieder auf eingeplant",
            neu_geplant["status"] == 2
            and neu_geplant["start_datum"] == "05.05.2026"
            and neu_geplant["fertig_datum"] == "06.05.2026",
            f"Status {neu_geplant['status']}, Start {neu_geplant['start_datum']}, Fertig {neu_geplant['fertig_datum']}",
        )
        response = admin.post(
            f"/admin/status/{angebot_id}/5",
            data=with_csrf(admin, {"next": f"/admin/auftrag/{angebot_id}"}),
            follow_redirects=False,
        )
        check("Reklamationsauftrag kann wieder auf zurückgegeben gesetzt werden", response.status_code in {302, 303})
        response = admin.post(
            "/admin/auftraege/zurueckgegeben-archivieren",
            data=with_csrf(admin),
            follow_redirects=False,
        )
        check("Rückgabe mit offener Reklamation bleibt aktiv", response.status_code in {302, 303})
        check(
            "Offene Reklamation verhindert Archivierung",
            portal.get_auftrag(angebot_id)["archiviert"] == 0,
        )
        portal.set_reklamation_status(reklamation_id, True)
        response = admin.post(
            "/admin/auftraege/zurueckgegeben-archivieren",
            data=with_csrf(admin),
            follow_redirects=False,
        )
        check("Erledigte Rückgabe ist archivierbar", response.status_code in {302, 303})
        check(
            "Zurückgegebener Auftrag ohne offene Reklamation wird archiviert",
            portal.get_auftrag(angebot_id)["archiviert"] == 1,
        )

        response = None
        admin = None
        partner = None
        db = None
        gc.collect()

    portal.DB = original_db
    portal.UPLOAD_DIR = original_upload_dir
    portal.BACKUP_DIR = original_backup_dir
    portal.DELETED_UPLOAD_DIR = original_deleted_upload_dir
    print("Flow-Test erfolgreich.")


if __name__ == "__main__":
    main()
