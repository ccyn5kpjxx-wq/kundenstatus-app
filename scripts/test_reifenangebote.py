"""Isolated public tire offers: no workshop reservation and complete offer emails.

Uses the existing disposable smoke-test database. Network access and worker
startup are blocked before importing the application; no real mail is sent.
"""
from __future__ import annotations

import base64
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from werkzeug.datastructures import MultiDict

sys.path.insert(0, str(Path(__file__).resolve().parent))
with patch("threading.Thread.start"):
    import smoke_test as isolated

portal = isolated.portal
REAL_LEAD_NOTICE = portal.lead_notice
PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8A"
    "AQUBAScY42YAAAAASUVORK5CYII="
)


class FormInputs(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.values = {}
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "input" and attrs.get("name"):
            self.values[attrs["name"]] = attrs.get("value", "")


class ReifenangeboteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
        portal.init_db()

    def setUp(self):
        self.client = portal.app.test_client()
        self.mocks = {}
        for name in (
            "sende_website_lead_benachrichtigung", "lead_notice",
            "notify_lead_workshop", "schedule_change_backup",
        ):
            provider = patch.object(portal, name)
            self.mocks[name] = provider.start()
            self.addCleanup(provider.stop)
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()

    def scalar(self, sql, params=()):
        db = portal.get_db()
        try:
            return db.execute(sql, params).fetchone()[0]
        finally:
            db.close()

    def count(self, table):
        return self.scalar("SELECT COUNT(*) FROM " + table)

    def form(self):
        response = self.client.get("/anfrage?anliegen=reifenangebot")
        self.assertEqual(response.status_code, 200)
        inputs = FormInputs(response.text).values
        self.assertTrue(inputs.get("reifenangebot_token"), "Offer form needs its one-use token")
        return MultiDict({
            portal.CSRF_FIELD_NAME: inputs[portal.CSRF_FIELD_NAME],
            "reifenangebot_token": inputs["reifenangebot_token"],
            "anliegen": "reifenangebot",
            "name": "Anna Reifen Test",
            "email": "reifenangebot@example.invalid",
            "telefon": "",
            "fahrzeug": "VW Golf Test",
            "reifen_saison": "winter",
            "reifen_qualitaet": "mittelklasse",
            "reifen_anzahl": "4",
            "reifen_groesse_vorne": "205/55 R16 91H",
            "reifen_groesse_hinten": "",
            "reifen_montage": "nein",
            "nachricht": "Nur Reifen zur Abholung anbieten.",
        })

    def submit(self, changes=None, *, photo=False, payload=None):
        data = payload if payload is not None else self.form()
        for name, value in (changes or {}).items():
            data.setlist(name, value if isinstance(value, list) else [value])
        if photo:
            data.add("bilder", (BytesIO(PNG_BYTES), "reifenbeschriftung.png", "image/png"))
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        return self.client.post("/anfrage", data=data)

    def latest(self):
        return portal.get_lead(self.scalar("SELECT MAX(id) FROM leads"))

    def make_overdue(self, lead_id):
        db = portal.get_db()
        db.execute("UPDATE leads SET reifen_angebot_faellig_am=? WHERE id=?", (
            (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), lead_id
        ))
        db.commit()
        db.close()

    def admin(self):
        with self.client.session_transaction() as session:
            session["admin"] = True
            return session[portal.CSRF_FIELD_NAME]

    def offer(self, lead_id, **changes):
        data = {
            portal.CSRF_FIELD_NAME: self.admin(),
            "angebot_text": "4 Winterreifen 205/55 R16 91H, Hersteller Musterreifen. Abholung.",
            "angebot_preis": "440,00 EUR brutto",
            "angebot_notiz": "Lieferzeit 3 Werktage. Angebot gültig bis Freitag.",
            "email_senden": "1",
        }
        data.update(changes)
        return self.client.post(f"/admin/leads/{lead_id}/angebot", data=data)

    def test_offer_without_phone_or_date_creates_no_slot_or_order(self):
        counts = {table: self.count(table) for table in ("leads", "reifen_termin_slots", "auftraege")}
        before = datetime.now(timezone.utc)
        response = self.submit()
        after = datetime.now(timezone.utc)
        self.assertEqual(response.status_code, 302)
        lead = self.latest()
        self.assertTrue(lead["ist_reifenangebot"])
        self.assertEqual(lead["kontakt_telefon"], "")
        self.assertEqual(self.count("leads"), counts["leads"] + 1)
        self.assertEqual(self.count("reifen_termin_slots"), counts["reifen_termin_slots"])
        self.assertEqual(self.count("auftraege"), counts["auftraege"])
        self.assertFalse(lead["auftrag_id"])
        self.assertFalse(lead["portal_data"].get("kunden_wunsch_annahme_datum"))
        due = datetime.fromisoformat(lead["reifen_angebot_faellig_am"])
        self.assertIsNotNone(due.utcoffset())
        self.assertGreaterEqual(due, before + timedelta(hours=24, seconds=-2))
        self.assertLessEqual(due, after + timedelta(hours=24, seconds=2))
        self.assertTrue(lead["reifenangebot_frist_label"])
        self.assertFalse(lead["reifenangebot_ueberfaellig"])
        notice = self.mocks["lead_notice"].call_args.args
        self.assertEqual(notice[2:4], ("E-Mail", "reifenangebot@example.invalid"))
        self.assertIn("24", notice[4])
        self.assertNotIn("Termin ist reserviert", notice[4])

    def test_structured_tire_selection_persists_including_mixed_sizes(self):
        response = self.submit({
            "reifen_saison": "ganzjahr", "reifen_qualitaet": "premium",
            "reifen_groesse_vorne": "225/40 R18 92Y XL",
            "reifen_groesse_hinten": "255/35 R18 94Y XL",
            "reifen_montage": "ja", "reifen_anzahl": "4",
        })
        self.assertEqual(response.status_code, 302)
        values = self.latest()["portal_data"]["reifenangebot"]
        self.assertEqual(values, {
            "saison": "ganzjahr", "qualitaet": "premium", "anzahl": 4,
            "groesse_vorne": "225/40 R18 92Y XL", "groesse_hinten": "255/35 R18 94Y XL",
            "montage": True,
        })

    def test_valid_photo_replaces_unknown_size_and_is_saved(self):
        response = self.submit({"reifen_groesse_vorne": ""}, photo=True)
        self.assertEqual(response.status_code, 302)
        lead = self.latest()
        self.assertEqual(lead["portal_data"]["reifenangebot"]["groesse_vorne"], "")
        files = portal.list_lead_dateien(lead["id"])
        self.assertTrue(any(item["original_name"] == "reifenbeschriftung.png" for item in files))

    def test_invalid_or_missing_offer_fields_do_not_create_leads(self):
        cases = (
            {"email": "", "telefon": "0171 1234567"},
            {"email": "invalid"}, {"email": "one@example.invalid,two@example.invalid"},
            {"reifen_groesse_vorne": ""}, {"reifen_saison": ""},
            {"reifen_saison": "unsafe"}, {"reifen_qualitaet": ""},
            {"reifen_qualitaet": "unsafe"}, {"reifen_anzahl": "0"},
            {"reifen_anzahl": "9"}, {"reifen_anzahl": "1.5"},
            {"reifen_montage": "unsafe"},
            {portal.CSRF_FIELD_NAME: "invalid"}, {"reifenangebot_token": "invalid"},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                before = self.count("leads")
                response = self.submit(changes)
                self.assertIn(response.status_code, (400, 409))
                self.assertEqual(self.count("leads"), before)

    def test_double_submission_creates_one_lead_and_one_receipt(self):
        payload = self.form()
        before = self.count("leads")
        first = self.submit(payload=payload)
        second = self.submit(payload=payload)
        self.assertEqual(first.status_code, 302)
        self.assertIn(second.status_code, (302, 409))
        self.assertEqual(self.count("leads"), before + 1)
        self.mocks["lead_notice"].assert_called_once()

    def test_two_open_forms_can_submit_two_distinct_vehicle_requests(self):
        first_form = self.form()
        second_form = self.form()
        self.assertNotEqual(first_form["reifenangebot_token"], second_form["reifenangebot_token"])
        first_form["fahrzeug"] = "Testwagen A"
        second_form["fahrzeug"] = "Testwagen B"
        before = self.count("leads")
        self.assertEqual(self.submit(payload=first_form).status_code, 302)
        first = self.latest()
        self.assertEqual(self.submit(payload=second_form).status_code, 302)
        second = self.latest()
        self.assertEqual(self.count("leads"), before + 2)
        self.assertNotEqual(first["id"], second["id"])
        self.assertEqual(first["fahrzeug"], "Testwagen A")
        self.assertEqual(second["fahrzeug"], "Testwagen B")

    def test_changed_replay_is_not_silently_treated_as_the_old_offer(self):
        payload = self.form()
        self.assertEqual(self.submit(payload=payload).status_code, 302)
        first = self.latest()
        replay = payload.copy()
        replay["fahrzeug"] = "Other car using a consumed token"
        before = self.count("leads")
        self.assertEqual(self.submit(payload=replay).status_code, 409)
        self.assertEqual(self.count("leads"), before)
        self.assertEqual(portal.get_lead(first["id"])["fahrzeug"], "VW Golf Test")

    def test_offer_remains_out_of_booking_queue_and_is_visible_to_office(self):
        self.assertEqual(self.submit().status_code, 302)
        lead = self.latest()
        self.assertFalse(any(item["id"] == lead["id"] for item in portal.list_reifentermine()))
        self.admin()
        response = self.client.get(f"/admin/leads/{lead['id']}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("205/55 R16 91H", response.text)
        self.assertIn("24", response.text)

    def test_offer_email_includes_price_scope_and_terms_and_is_not_sent_twice(self):
        self.assertEqual(self.submit().status_code, 302)
        lead = self.latest()
        with patch.object(portal, "lead_notice", REAL_LEAD_NOTICE), patch.object(portal, "send_lead_email") as sender:
            self.assertEqual(self.offer(lead["id"]).status_code, 302)
            self.assertEqual(self.offer(lead["id"]).status_code, 302)
        sender.assert_called_once()
        args = sender.call_args.args
        self.assertEqual(args[1], "reifenangebot@example.invalid")
        self.assertIn("4 Winterreifen 205/55 R16 91H", args[3])
        self.assertIn("440,00 € brutto", args[3])
        self.assertIn("Lieferzeit 3 Werktage", args[3])
        self.assertIn("gültig bis Freitag", args[3])
        self.assertFalse(portal.get_lead(lead["id"])["reifenangebot_ueberfaellig"])

    def test_only_current_reviewed_pdf_reaches_the_customer_email(self):
        from pypdf import PdfWriter

        def pdf_bytes(title):
            output = BytesIO()
            writer = PdfWriter()
            writer.add_blank_page(width=100, height=100)
            writer.add_metadata({"/Title": title})
            writer.write(output)
            return output.getvalue()

        self.assertEqual(self.submit().status_code, 302)
        lead = self.latest()
        old_pdf = pdf_bytes("Old offer must not be attached")
        portal.save_lead_attachment_bytes(
            lead["id"], "altes-angebot.pdf", old_pdf, "application/pdf", quelle="werkstatt_angebot"
        )
        current_pdf = pdf_bytes("Reviewed current tire offer")
        fake_config = {
            "configured": True, "website": "auto-lackierzentrum",
            "display_name": "Testwerkstatt", "address": "werkstatt@example.invalid",
            "host": "smtp.example.invalid", "port": 465, "ssl": True, "tls": False,
            "user": "test-only", "password": "test-only",
        }
        with (
            patch.object(portal, "lead_notice", REAL_LEAD_NOTICE),
            patch.object(portal, "get_lead_mail_config", return_value=fake_config),
            patch.object(portal.smtplib, "SMTP_SSL") as smtp_factory,
            patch.object(portal, "append_lead_message_to_sent", return_value="Sent"),
        ):
            response = self.offer(lead["id"], angebot_pdf=(BytesIO(current_pdf), "reifenangebot.pdf", "application/pdf"))
            self.assertEqual(response.status_code, 302)
            smtp = smtp_factory.return_value.__enter__.return_value
            smtp.send_message.assert_called_once()
            message = smtp.send_message.call_args.args[0]
        self.assertEqual(message["To"], "reifenangebot@example.invalid")
        self.assertIn("440,00 € brutto", message.get_body(preferencelist=("plain",)).get_content())
        attachments = list(message.iter_attachments())
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].get_filename(), "reifenangebot.pdf")
        self.assertEqual(attachments[0].get_payload(decode=True), current_pdf)

    def test_customer_offer_acceptance_cannot_create_a_repair_order_or_slot(self):
        self.assertEqual(self.submit().status_code, 302)
        lead = self.latest()
        self.assertEqual(self.offer(lead["id"]).status_code, 302)
        before = {table: self.count(table) for table in ("reifen_termin_slots", "auftraege")}
        with self.client.session_transaction() as session:
            csrf = session[portal.CSRF_FIELD_NAME]
            session.pop("admin", None)
        response = self.client.post("/status/" + lead["kunden_status_token"] + "/angebot-annehmen", data={
            portal.CSRF_FIELD_NAME: csrf, "angebot_annehmen_bestaetigt": "1",
            "wunsch_annahme_datum": (date.today() + timedelta(days=10)).isoformat(),
            "transport_art": "standard", "ersatzfahrzeug": "nein",
        })
        self.assertEqual(response.status_code, 302)
        for table, count in before.items():
            self.assertEqual(self.count(table), count)
        current = portal.get_lead(lead["id"])
        self.assertFalse(current["auftrag_id"])
        self.assertEqual(current["angebot_status"], "angebot_abgegeben")

    def test_overdue_offer_is_flagged_until_the_office_provides_a_quote(self):
        self.assertEqual(self.submit().status_code, 302)
        lead = self.latest()
        self.make_overdue(lead["id"])
        self.assertTrue(portal.get_lead(lead["id"])["reifenangebot_ueberfaellig"])
        with patch.object(portal, "lead_notice", REAL_LEAD_NOTICE), patch.object(portal, "send_lead_email"):
            self.assertEqual(self.offer(lead["id"]).status_code, 302)
        self.assertFalse(portal.get_lead(lead["id"])["reifenangebot_ueberfaellig"])

    def test_preparing_without_sending_does_not_complete_the_email_deadline(self):
        self.assertEqual(self.submit().status_code, 302)
        lead = self.latest()
        self.make_overdue(lead["id"])
        with patch.object(portal, "lead_notice", REAL_LEAD_NOTICE), patch.object(portal, "send_lead_email") as sender:
            self.assertEqual(self.offer(lead["id"], email_senden="0").status_code, 302)
        sender.assert_not_called()
        current = portal.get_lead(lead["id"])
        self.assertTrue(current["reifenangebot_ueberfaellig"])
        self.assertEqual(current["reifenangebot_email_status"], "offen")

    def test_failed_and_uncertain_email_leave_the_deadline_open(self):
        for error, expected in (
            (ValueError("test sender not configured"), "fehlgeschlagen"),
            (RuntimeError("test provider outcome uncertain"), "unklar"),
        ):
            with self.subTest(status=expected):
                self.assertEqual(self.submit().status_code, 302)
                lead = self.latest()
                self.make_overdue(lead["id"])
                with patch.object(portal, "lead_notice", REAL_LEAD_NOTICE), patch.object(portal, "send_lead_email", side_effect=error) as sender:
                    self.assertEqual(self.offer(lead["id"]).status_code, 302)
                sender.assert_called_once()
                current = portal.get_lead(lead["id"])
                self.assertTrue(current["reifenangebot_ueberfaellig"])
                self.assertEqual(current["reifenangebot_email_status"], expected)

    def test_unsent_changed_quote_reopens_the_email_deadline(self):
        self.assertEqual(self.submit().status_code, 302)
        lead = self.latest()
        self.make_overdue(lead["id"])
        with patch.object(portal, "lead_notice", REAL_LEAD_NOTICE), patch.object(portal, "send_lead_email") as sender:
            self.assertEqual(self.offer(lead["id"]).status_code, 302)
            sent = portal.get_lead(lead["id"])
            self.assertFalse(sent["reifenangebot_ueberfaellig"])
            self.assertEqual(sent["reifenangebot_email_status"], "gesendet")
            self.assertEqual(self.offer(lead["id"], angebot_preis="480,00 € brutto", email_senden="0").status_code, 302)
        sender.assert_called_once()
        current = portal.get_lead(lead["id"])
        self.assertTrue(current["reifenangebot_ueberfaellig"])
        self.assertEqual(current["reifenangebot_email_status"], "offen")

    def test_failed_photo_save_does_not_leave_a_partial_offer(self):
        before = {table: self.count(table) for table in ("leads", "reifen_termin_slots", "lead_dateien")}
        with patch.object(portal, "save_lead_upload_file", side_effect=OSError("test disk unavailable")):
            response = self.submit({"reifen_groesse_vorne": ""}, photo=True)
        self.assertEqual(response.status_code, 503)
        for table, count in before.items():
            self.assertEqual(self.count(table), count)
        self.mocks["lead_notice"].assert_not_called()

    def test_existing_booking_still_requires_a_phone_and_reserves_one_hour(self):
        self.client.get("/anfrage?anliegen=reifenservice")
        with self.client.session_transaction() as session:
            csrf = session[portal.CSRF_FIELD_NAME]
        day = date.today() + timedelta(days=16)
        while day.weekday() == 6:
            day += timedelta(days=1)
        payload = {
            portal.CSRF_FIELD_NAME: csrf, "anliegen": "reifenservice",
            "name": "Termin Test", "email": "termin@example.invalid",
            "reifen_leistung": "radwechsel", "wunschdatum": day.isoformat(),
            "wunschzeit": "09:00", "telefon": "",
        }
        before = self.count("reifen_termin_slots")
        self.assertEqual(self.client.post("/anfrage", data=payload).status_code, 400)
        payload["telefon"] = "0171 1234567"
        self.assertEqual(self.client.post("/anfrage", data=payload).status_code, 302)
        self.assertEqual(self.count("reifen_termin_slots"), before + 1)
        lead = self.latest()
        self.assertFalse(lead["ist_reifenangebot"])
        self.assertNotIn("09:00", portal.freie_reifen_slots(day.isoformat()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
