"""Exercise the payment sync and dashboard with synthetic Lexware responses."""

from datetime import datetime
from io import BytesIO
import json
import unittest
from unittest.mock import patch

from smoke_test import portal


class NovemberSeven(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 7, 12, tzinfo=tz)


def voucher(number, kind, status="open", *, archived=False):
    return {
        "id": number, "voucherType": kind, "voucherStatus": status,
        "voucherNumber": number, "contactName": "Testkontakt",
        "voucherDate": "2026-09-20", "totalAmount": 1000, "openAmount": 700,
        "currency": "EUR", "archived": archived,
    }


def item(posting_date, amount, kind="partPaymentFinancialTransaction"):
    return {"postingDate": posting_date, "amount": amount, "paymentItemType": kind, "currency": "EUR"}


class ZahlenTestFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        portal.init_db()

    def setUp(self):
        db = portal.get_db()
        try:
            db.execute("DELETE FROM lexware_rechnungen")
            db.execute("DELETE FROM app_settings WHERE key=?", (portal.LEXWARE_ZAHLUNGSSTAND,))
            db.execute("DELETE FROM app_settings WHERE key=?", (portal.LEXWARE_EUER_MONATSSTAENDE,))
            db.commit()
        finally:
            db.close()
        clock = patch.object(portal, "datetime", NovemberSeven)
        clock.start()
        self.addCleanup(clock.stop)

    def page(self):
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        response = client.get("/admin/zahlen?jahr=2026&monat=11")
        self.assertEqual(response.status_code, 200)
        return response.get_data(as_text=True)


class ZahlenZahlungenTest(ZahlenTestFixture):
    def test_sync_months_by_posting_date_and_include_investment_and_refund(self):
        invoice = voucher("TEST-IN", "invoice")
        investment = voucher("TEST-OUT", "purchaseinvoice", "paid")
        refund = voucher("TEST-REFUND", "creditnote", "paidoff", archived=True)
        offset = voucher("TEST-OFFSET", "creditnote", "paidoff")
        payments = {
            "TEST-IN": {"paymentItems": [
                item("2026-10-05", 100), item("2026-10-31", 200),
                item("2026-11-02", 50), item("2026-11-02", 50),
                item("2026-11-07", 100, "manualPayment"),
                item("2026-11-03", 500, "partPaymentCreditNote"),
                item("2026-11-04", 5, "cashDiscount"),
                item("2026-11-07T23:30:00+00:00", 100),
            ]},
            "TEST-OUT": {"paymentItems": [item("2026-11-03", 80)]},
            "TEST-REFUND": {"paymentItems": [item("2026-11-04", 20)]},
            "TEST-OFFSET": {"error": "Lexware API Fehler 406: linked credit note"},
        }

        def list_vouchers(types, status, *, archived=False):
            if archived:
                return [refund]
            if types == "creditnote,salescreditnote,purchasecreditnote":
                return [offset]
            if status in {"open", "overdue"}:
                return [invoice]
            return [investment] if status == "paid" else []

        with (
            patch.object(portal, "LEXWARE_API_KEY", "synthetic-only"),
            patch.object(portal, "fetch_lexware_voucherlist", side_effect=list_vouchers),
            patch.object(portal, "fetch_lexware_payment", side_effect=payments.__getitem__) as fetch_payment,
            patch.object(portal, "lexware_request", return_value={
                "voucherStatus": "paidoff", "relatedVouchers": [{"id": "TEST-IN", "voucherType": "invoice"}],
            }),
            patch.object(portal.time, "sleep"),
        ):
            self.assertTrue(portal.sync_lexware_rechnungen()["ok"])
        self.assertEqual(fetch_payment.call_count, 4)

        db = portal.get_db()
        try:
            db.execute("UPDATE lexware_rechnungen SET investition=1 WHERE voucher_id=?", ("TEST-OUT",))
            db.commit()
        finally:
            db.close()
        zahlen = portal.umsatz_ziel_daten(2026, 11)
        self.assertFalse(zahlen["zahlungsabgleich_unvollstaendig"])
        self.assertEqual(zahlen["einnahmen_bisher_label"], "200,00 €")
        self.assertEqual(zahlen["ausgaben_bisher_label"], "100,00 €")
        self.assertEqual(zahlen["investitionen_bisher_label"], "80,00 €")
        self.assertEqual(zahlen["differenz_bisher_label"], "100,00 €")
        self.assertEqual(zahlen["vormonat_einnahmen_bisher_label"], "100,00 €")
        self.assertEqual(zahlen["vormonat_einnahmen_total_label"], "300,00 €")
        self.assertEqual([(b["nummer"], b["datum"]) for b in zahlen["alle_ausgaben"]], [
            ("TEST-OUT", "03.11.2026"), ("TEST-REFUND", "04.11.2026"),
        ])
        html = self.page()
        self.assertIn("TEST-REFUND", html)
        self.assertIn("davon 80,00 € Investitionen", html)
        self.assertEqual(html.count('/admin/zahlen/investition/'), 1)

    def test_legacy_cache_uses_items_only_and_failed_sync_keeps_incomplete_banner(self):
        portal.upsert_lexware_rechnung(voucher("KNOWN", "invoice"), {
            "paymentItems": [item("2026-11-02", 19.90)],
        })
        portal.upsert_lexware_rechnung(voucher("MISSING", "invoice", "paid"), {
            "paidDate": "2026-11-03", "openAmount": 0,
        })
        portal.lexware_zahlungsstand_speichern({}, 0, True)
        zahlen = portal.umsatz_ziel_daten(2026, 11)
        self.assertTrue(zahlen["zahlungsabgleich_unvollstaendig"])
        self.assertEqual(zahlen["zahlungsdaten_luecken"], 1)
        self.assertEqual(zahlen["einnahmen_bisher_label"], "19,90 €")
        self.assertEqual(zahlen["ampel"], "neutral")
        self.assertIn("Zahlungsabgleich offen", self.page())

    def test_failed_refresh_preserves_last_complete_payment_totals(self):
        paid = voucher("SNAPSHOT", "invoice", "paid")
        beleg = portal.lexware_zahlungsbeleg(paid, {"paymentItems": [item("2026-11-07", 90.06)]})
        portal.lexware_zahlungsstand_speichern({"SNAPSHOT": beleg}, 0, False)
        vorher = portal.lexware_zahlungsstand_lesen()["letzter_erfolg"]
        portal.lexware_zahlungsstand_speichern({}, 1, True)
        zahlen = portal.umsatz_ziel_daten(2026, 11)
        self.assertTrue(zahlen["zahlungsabgleich_unvollstaendig"])
        self.assertEqual(zahlen["einnahmen_bisher_label"], "90,06 €")
        self.assertEqual(zahlen["last_sync"], vorher)

    def test_cash_items_require_valid_currency_amount_and_known_type(self):
        for invalid in (
            item("2026-11-02", -1),
            item("2026-11-02", "NaN"),
            {**item("2026-11-02", 1), "currency": "USD"},
            item("not-a-date", 1),
            item("2026-11-02", 1, "futurePaymentKind"),
        ):
            with self.subTest(invalid=invalid), self.assertRaises((ValueError, ArithmeticError)):
                portal.lexware_zahlungsposten({"paymentItems": [invalid]})
        self.assertEqual(portal.lexware_zahlungsposten({"paymentItems": [
            item("2026-11-02", 500, "partPaymentCreditNote"),
            item("2026-11-02", 4, "cashDiscount"),
            item("2026-11-02", 7, "irrecoverableReceivable"),
        ]}), [])

    def test_unpaid_invoice_is_not_receipt_and_local_midnight_sets_month(self):
        unpaid = voucher("OPEN", "invoice", "open")
        received = voucher("OLD", "invoice", "paid")
        belege = {
            "OPEN": portal.lexware_zahlungsbeleg(unpaid, {"paymentItems": []}),
            "OLD": portal.lexware_zahlungsbeleg(received, {"paymentItems": [
                item("2026-10-31T23:30:00+00:00", 12.34),
                item("2026-11-08T00:00:00+01:00", 20),
            ]}),
        }
        portal.lexware_zahlungsstand_speichern(belege, 0, False)
        zahlen = portal.umsatz_ziel_daten(2026, 11)
        self.assertEqual(zahlen["einnahmen_bisher_label"], "12,34 €")
        self.assertEqual(zahlen["eintraege"], 1)
        self.assertEqual(zahlen["vormonat_einnahmen_total_label"], "0,00 €")

    def test_first_failed_refresh_does_not_show_unknown_zero_as_received_money(self):
        portal.lexware_zahlungsstand_speichern({}, 1, True)
        zahlen = portal.umsatz_ziel_daten(2026, 11)
        self.assertFalse(zahlen["hat_daten"])
        self.assertTrue(zahlen["zahlungsabgleich_unvollstaendig"])
        self.assertFalse(zahlen["ziel_gesetzt"])
        html = self.page()
        self.assertIn("Zahlungsdaten fehlen.", html)
        self.assertIn('metric-value">—</div>', html)

    def test_voucher_pagination_does_not_accept_truncated_or_malformed_history(self):
        with patch.object(portal.time, "sleep"), patch.object(portal, "lexware_request", return_value={
            "content": [{"id": "ONE"}], "last": False, "totalElements": 2,
        }):
            with self.assertRaisesRegex(RuntimeError, "Seitenlimit"):
                portal.fetch_lexware_voucherlist("invoice", "paid", max_pages=1)
        with patch.object(portal, "lexware_request", return_value={
            "content": [{}], "last": True, "totalElements": 1,
        }):
            with self.assertRaises(RuntimeError):
                portal.fetch_lexware_voucherlist("invoice", "paid")


def report(company="Testbetrieb GmbH", month="November", year=2026, created="07.11.2026 10:02",
           income="1.234,56", expense="34,56", difference="1.200,00"):
    return (f"- generic: {company}, Teststraße 1, 12345 Teststadt\n"
            f"- generic: {created}\n"
            f'- heading "Einnahmen-Überschussrechnung für den Monat {month} {year}" [level=1]\n'
            "- generic: Berechnung nach Zahldatum\n"
            "- generic: Alle Angaben in Euro\n"
            f'- heading "Betriebseinnahmen gesamt {income}" [level=4]:\n'
            f"  - generic: Betriebseinnahmen gesamt\n  - generic: {income}\n"
            f'- heading "Betriebsausgaben gesamt {expense}" [level=4]:\n'
            f"- generic: Differenz\n- generic: {difference}\n").encode("utf-8")


class LexwareBerichtsimportTest(ZahlenTestFixture):
    # Separate scenarios share the isolated Flask fixture and no provider access.
    def parse(self, data):
        return portal.lexware_euer_bericht_lesen(data, "Lexware-November.txt", "Testbetrieb GmbH",
            now=datetime.fromisoformat("2026-11-07T12:00:00+01:00"))

    def test_report_labels_not_entered_totals_drive_snapshot_and_archive(self):
        data = report()
        record = self.parse(data)
        self.assertEqual(record["einnahmen_cent"], 123456)
        self.assertEqual(record["ausgaben_cent"], 3456)
        portal.lexware_euer_monatsstand_speichern(record, data)
        # Deliberately conflicting API data cannot be added to the EÜR snapshot.
        api = portal.lexware_zahlungsbeleg(voucher("CONFLICT", "invoice"), {
            "paymentItems": [item("2026-11-07", 9000)],
        })
        portal.lexware_zahlungsstand_speichern({"CONFLICT": api}, 0, False)
        zahlen = portal.umsatz_ziel_daten(2026, 11)
        self.assertTrue(zahlen["lexware_euer_snapshot"])
        self.assertEqual(zahlen["einnahmen_bisher_label"], "1.234,56 €")
        self.assertEqual(zahlen["differenz_bisher_label"], "1.200,00 €")
        self.assertEqual(zahlen["as_of"], 7)
        self.assertEqual(zahlen["tendenz_pct"], None)
        client = portal.app.test_client()
        self.assertIn(client.get("/admin/zahlen/lexware-bericht/2026/11").status_code, (302, 401, 403))
        with client.session_transaction() as session:
            session["admin"] = True
        response = client.get("/admin/zahlen/lexware-bericht/2026/11")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, data)
        self.assertEqual(response.headers["Cache-Control"], "private, no-store")
        response.close()
        self.assertIn("Lexware-Auswertung nach Zahldatum", self.page())

    def test_wrong_scope_ambiguous_totals_or_bad_equation_are_rejected(self):
        for data in (
            report(company="Anderer Betrieb GmbH"),
            report(difference="1.200,01"),
            report().replace(b"Zahldatum", b"Belegdatum"),
            report() + b"\nBetriebseinnahmen gesamt 9.999,99\n",
            report(created="08.11.2026 10:02"),
            report().replace(b"Alle Angaben in Euro", b"Alle Angaben in Dollar"),
        ):
            with self.subTest(data=data[:60]), self.assertRaises(ValueError):
                self.parse(data)

    def test_printed_pdf_report_extracts_the_same_source_labels(self):
        from reportlab.pdfgen.canvas import Canvas
        buffer = BytesIO()
        document = Canvas(buffer)
        for index, line in enumerate((
            "Testbetrieb GmbH, Teststraße 1, 12345 Teststadt", "07.11.2026 10:02",
            "Einnahmen-Überschussrechnung für den Monat November 2026",
            "Berechnung nach Zahldatum", "Alle Angaben in Euro",
            "Betriebseinnahmen gesamt 1.234,56", "Betriebsausgaben gesamt 34,56",
            "Differenz 1.200,00",
        )):
            document.drawString(35, 800 - index * 24, line)
        document.save()
        data = buffer.getvalue()
        record = portal.lexware_euer_bericht_lesen(data, "Lexware.pdf", "Testbetrieb GmbH",
            now=datetime.fromisoformat("2026-11-07T12:00:00+01:00"))
        self.assertEqual(record["einnahmen_cent"], 123456)
        self.assertEqual(record["ausgaben_cent"], 3456)
        self.assertEqual(record["stichtag"], "2026-11-07")

    def test_signed_month_results_are_accepted(self):
        record = self.parse(report(income="-100,00", expense="-20,00", difference="-80,00"))
        self.assertEqual((record["einnahmen_cent"], record["ausgaben_cent"]), (-10000, -2000))

    def test_failed_or_older_source_does_not_replace_saved_month(self):
        data = report()
        record = self.parse(data)
        portal.lexware_euer_monatsstand_speichern(record, data)
        original = portal.get_app_setting(portal.LEXWARE_EUER_MONATSSTAENDE)
        for payload, source in (
            (record, data + b"changed"),
            ({**record, "quellenzeitpunkt": "2026-11-07T09:02:00+01:00"}, data),
        ):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                portal.lexware_euer_monatsstand_speichern(payload, source)
            self.assertEqual(portal.get_app_setting(portal.LEXWARE_EUER_MONATSSTAENDE), original)

    def test_import_requires_selected_month_and_preserves_previous_on_failure(self):
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        client.get("/admin/zahlen?jahr=2026&monat=11")
        with client.session_transaction() as session:
            csrf = session.get("csrf_token")
        def post(month):
            return client.post("/admin/zahlen/lexware-bericht", data={
                "csrf_token": csrf, "jahr": "2026", "monat": str(month),
                "lexware_bericht": (BytesIO(report(company=portal.WORKSHOP_LEGAL_NAME)), "Lexware.txt"),
            })
        self.assertEqual(post(10).status_code, 302)
        self.assertEqual(portal.lexware_euer_monatsstaende_lesen(), {})
        self.assertEqual(post(11).status_code, 302)
        self.assertIn("2026-11", portal.lexware_euer_monatsstaende_lesen())

    def test_previous_month_goal_uses_euer_only_and_stale_report_has_no_projection(self):
        november = self.parse(report(created="06.11.2026 10:02"))
        october_data = report(month="Oktober", income="2.001,00", expense="1,00", difference="2.000,00")
        october = self.parse(october_data)
        portal.lexware_euer_monatsstand_speichern(october, october_data)
        november_data = report(created="06.11.2026 10:02")
        portal.lexware_euer_monatsstand_speichern(november, november_data)
        zahlen = portal.umsatz_ziel_daten(2026, 11)
        self.assertTrue(zahlen["snapshot_veraltet"])
        self.assertEqual(zahlen["hochrechnung_label"], "—")
        self.assertEqual(zahlen["ziel_label"], "3.000,00 €")
        self.assertIn("Berichtsstand vom 06.11.2026", self.page())

    def test_month_table_keeps_snapshot_visible_without_current_month_data(self):
        data = report()
        portal.lexware_euer_monatsstand_speichern(self.parse(data), data)
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        response = client.get("/admin/zahlen?jahr=2026&monat=12")
        html = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("November 2026", html)
        self.assertIn("1.234,56 €", html)
        self.assertIn("Lexware EÜR", html)
        november = portal.umsatz_ziel_daten(2026, 12)["uebersicht"][0]
        self.assertFalse(november["unvollstaendig"])

    def test_snapshot_does_not_make_partial_api_history_complete(self):
        portal.upsert_lexware_rechnung(voucher("OLD-API", "invoice"), {
            "paymentItems": [item("2026-10-02", 15.50)],
        })
        data = report()
        portal.lexware_euer_monatsstand_speichern(self.parse(data), data)
        zahlen = portal.umsatz_ziel_daten(2026, 11)
        october = next(row for row in zahlen["uebersicht"] if row["monat"] == 10)
        self.assertTrue(october["unvollstaendig"])
        self.assertIn("Teilstand", self.page())


if __name__ == "__main__":
    unittest.main()
