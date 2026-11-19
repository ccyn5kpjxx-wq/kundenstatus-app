"""End-to-end dealership inquiry checks on disposable data with external network blocked."""

from io import BytesIO
import unittest
from unittest.mock import patch

import smoke_test as isolated
from test_homepage_kurzanfrage import PNG_1X1, scalar


portal = isolated.portal


class AutohausProbeauftragTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        portal.init_db()

    def setUp(self):
        self.client = portal.app.test_client()
        self.form = self.client.get("/anfrage?anliegen=autohaus_probeauftrag")
        with self.client.session_transaction() as session:
            self.data = {
                portal.CSRF_FIELD_NAME: session[portal.CSRF_FIELD_NAME],
                "anliegen": "autohaus_probeauftrag", "betrieb": "Autohaus Beispiel",
                "name": "Ana Muster", "telefon": "06261 000000",
                "nachricht": "Bitte Lackierung der Stoßstange anbieten.",
            }
        self.enterContext(patch.object(portal, "PUBLIC_FORM_RATE_LIMIT_MAX", 50))
        self.enterContext(patch.object(portal, "schedule_change_backup"))
        self.workshop = self.enterContext(patch.object(portal, "sende_website_lead_benachrichtigung"))
        self.enterContext(patch.object(portal, "notify_lead_workshop"))
        self.notice = self.enterContext(patch.object(portal, "lead_notice"))

    def test_direct_entry_and_phone_only_inquiry(self):
        self.assertEqual(self.form.status_code, 200)
        for item in ("Erstes Fahrzeug anfragen", 'name="betrieb"', 'autocomplete="organization"',
                     "Ansprechperson", "Probeauftrag unverbindlich anfragen", "/partner"):
            self.assertIn(item, self.form.text)
        self.assertNotIn('class="intent-grid"', self.form.text)
        orders_before = scalar("SELECT COUNT(*) FROM auftraege")
        result = self.client.post("/anfrage", data=self.data)
        self.assertEqual(result.status_code, 302)
        lead = portal.get_lead(scalar("SELECT MAX(id) FROM leads"))
        self.assertEqual(lead["portal_data"]["betrieb"], self.data["betrieb"])
        self.assertEqual(lead["portal_data"]["anliegen"], "autohaus_probeauftrag")
        self.assertEqual(lead["kunde_name"], self.data["name"])
        self.assertEqual(lead["kontakt_telefon"], self.data["telefon"])
        self.assertIn("Autohaus Beispiel", lead["beschreibung"])
        self.assertIn("Probeauftrag", lead["naechste_aktion"])
        self.assertFalse(lead["auftrag_id"] or lead["autohaus_id"])
        self.assertEqual(scalar("SELECT COUNT(*) FROM auftraege"), orders_before)
        self.assertIn("Autohaus Beispiel", self.workshop.call_args.args[1]["beschreibung"])
        self.notice.assert_not_called()
        receipt = self.client.get(result.location)
        self.assertIn("Ihre Anfrage zum Probeauftrag", receipt.text)
        self.assertIn("Meine Anfrage und Angebote öffnen", receipt.text)
        self.assertNotIn(lead["kunden_status_token"], self.client.get(result.location).text)

    def test_email_photo_and_untrusted_business_name(self):
        result = self.client.post("/anfrage", data={
            **self.data, "telefon": "", "email": "dispo@example.test", "fahrzeug": "Pkw Muster",
            "betrieb": "Muster & Co.", "bilder": (BytesIO(PNG_1X1), "stossfaenger.png"),
            "autohaus_id": "37", "auftrag_id": "50",
        })
        self.assertEqual(result.status_code, 302)
        lead = portal.get_lead(scalar("SELECT MAX(id) FROM leads"))
        self.assertEqual(lead["fahrzeug"], "Pkw Muster")
        self.assertFalse(lead["autohaus_id"] or lead["auftrag_id"])
        photos = portal.list_lead_dateien(lead["id"])
        self.assertEqual([photo["original_name"] for photo in photos], ["stossfaenger.png"])
        self.assertEqual((portal.UPLOAD_DIR / photos[0]["stored_name"]).read_bytes(), PNG_1X1)
        message = self.notice.call_args.args
        self.assertEqual(message[:4], (lead["id"], "Ihre Anfrage zum Probeauftrag ist eingegangen", "E-Mail", "dispo@example.test"))
        self.assertIn(lead["kunden_status_token"], message[4])

    def test_validation_stays_in_form_and_creates_no_lead(self):
        before = scalar("SELECT COUNT(*) FROM leads")
        for changes in ({"betrieb": " "}, {"betrieb": "a" * 161}, {"telefon": ""},
                        {"nachricht": ""}, {portal.CSRF_FIELD_NAME: "invalid"}):
            with self.subTest(changes=changes):
                self.assertEqual(self.client.post("/anfrage", data={**self.data, **changes}).status_code, 400)
        bad = self.client.post("/anfrage", data={**self.data, "betrieb": '<b>Test</b>', "telefon": ""})
        self.assertIn('value="&lt;b&gt;Test&lt;/b&gt;"', bad.text)
        self.assertNotIn('<b>Test</b>', bad.text)
        self.assertIn("Erstes Fahrzeug anfragen", bad.text)
        self.assertEqual(scalar("SELECT COUNT(*) FROM leads"), before)
        self.workshop.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
