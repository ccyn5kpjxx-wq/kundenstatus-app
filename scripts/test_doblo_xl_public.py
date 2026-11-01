"""Doblò XL display and inquiry regression checks; isolated DB, no providers."""

from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path
import re
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import smoke_test as isolated

portal = isolated.portal


def vehicle(identifier, name, basis="verfuegbar", **changes):
    result = {
        "id": identifier, "bezeichnung": name, "fahrzeugklasse": "Transporter",
        "tagessatz": "59", "basis_status": basis, "status": "verfuegbar",
        "bilder": [], "vorgaenge": [],
    }
    result.update(changes)
    return result


class DobloXLPublicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        portal.init_db()

    def test_homepage_card_keeps_tariff_and_compatible_selection(self):
        response = portal.app.test_client().get("/anfrage?anliegen=mietwagenanfrage")
        self.assertEqual(response.status_code, 200)
        cards = re.findall(r'<label class="rental-card">.*?</label>', response.text, re.S)
        xl = next(card for card in cards if 'value="fiat-doblo-maxi"' in card)
        self.assertIn("Fiat Doblò XL", xl)
        self.assertIn("1.5 Diesel · 75 kW", xl)
        self.assertIn("Erhöhte Nutzlast", xl)
        self.assertIn("Jetzt verfügbar", xl)
        self.assertIn("59 € / Tag", xl)
        self.assertIn("Ab 3 Tagen 49 € / Tag", xl)
        self.assertIn("Herstellerabbildung · Symbolfoto", xl)
        self.assertIn("/static/mietwagen/doblo-xl-hersteller.png", xl)
        self.assertNotIn("is-bald", xl)
        self.assertNotIn("5 Sitze", xl)
        smaller = next(card for card in cards if 'value="fiat-doblo"' in card)
        self.assertIn("In Kürze", smaller)
        self.assertIn("55 € / Tag", smaller)
        self.assertIn("Ab 3 Tagen 45 € / Tag", smaller)

    def test_public_fleet_respects_xl_status_and_existing_reservations(self):
        fixtures = [
            vehicle(11, "Fiat Doblò XL 1.5 Diesel"),
            vehicle(12, "Fiat Doblo Maxi"),
            vehicle(13, "Fiat Doblò 1.5 Diesel 75 kW XL", basis="bald"),
            vehicle(14, "Fiat Doblò", basis="verfuegbar"),
            vehicle(15, "Fiat Doblò XL", basis="wartung"),
            vehicle(16, "Fiat Doblò XL", basis="inaktiv"),
            vehicle(17, "Fiat Doblò XL", vorgaenge=[
                {"abgeschlossen": False, "start_datum": "2026-11-04", "end_datum": "2026-11-07"},
                {"abgeschlossen": True, "start_datum": "2026-10-01", "end_datum": "2026-10-02"},
            ]),
            vehicle(18, "Hyundai i10", basis="bald"),
            vehicle(19, "Hyundai KONA", basis="bald"),
        ]
        before = deepcopy(fixtures)
        with patch.object(portal, "list_mietfahrzeuge", return_value=fixtures):
            rows = {item["id"]: item for item in portal.mietwagen_public_fahrzeuge()}
        self.assertEqual(fixtures, before, "rendering must not modify fleet records")
        self.assertFalse(rows[11]["bald"])
        self.assertFalse(rows[12]["bald"])
        self.assertEqual(rows[12]["bezeichnung"], "Fiat Doblo XL")
        self.assertTrue(rows[13]["bald"], "an explicit future XL availability remains respected")
        self.assertTrue(rows[14]["bald"], "smaller Doblò has not been released")
        self.assertNotIn(15, rows)
        self.assertNotIn(16, rows)
        self.assertEqual(rows[17]["belegt"], ["2026-11-04 – 2026-11-07"])
        self.assertFalse(rows[18]["bald"])
        self.assertFalse(rows[19]["bald"])
        self.assertEqual(rows[11]["bild_url"], "/static/mietwagen/doblo-xl-hersteller.png")

    def test_rental_routes_use_labelled_manufacturer_photo(self):
        fixtures = [vehicle(11, "Fiat Doblò XL", bilder=[{"id": 999}])]
        with patch.object(portal, "list_mietfahrzeuge", return_value=fixtures):
            client = portal.app.test_client()
            for path in ("/mietwagen-info", "/mietwagen"):
                response = client.get(path)
                self.assertEqual(response.status_code, 200, path)
                self.assertIn("/static/mietwagen/doblo-xl-hersteller.png", response.text)
                self.assertIn("Herstellerabbildung · Symbolfoto", response.text)
                self.assertNotIn("/mietwagen/bild/999", response.text)
                self.assertNotIn("In Kürze verfügbar", response.text)

    def test_xl_submission_creates_inquiry_not_rental(self):
        client = portal.app.test_client()
        client.get("/anfrage?anliegen=mietwagenanfrage")
        with client.session_transaction() as session:
            csrf = session[portal.CSRF_FIELD_NAME]
        start = date.today() + timedelta(days=7)
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        with patch.object(portal, "sende_website_lead_benachrichtigung"), \
                patch.object(portal, "lead_notice"), patch.object(portal, "notify_lead_workshop"):
            db = portal.get_db()
            before = db.execute("SELECT COUNT(*) FROM auftraege").fetchone()[0]
            db.close()
            response = client.post("/anfrage", data={
                portal.CSRF_FIELD_NAME: csrf, "anliegen": "mietwagenanfrage",
                "name": "XL Testkunde", "telefon": "0171 1234567", "email": "xl@example.invalid",
                "mietwagen_fahrzeug": "fiat-doblo-maxi", "fahrzeug": "",
                "mietbeginn": start.isoformat(), "mietende": (start + timedelta(days=3)).isoformat(),
                "nachricht": "Ich möchte den XL für einen Transport anfragen.", "datenschutz": "1",
            })
        self.assertEqual(response.status_code, 302)
        db = portal.get_db()
        lead_id = db.execute("SELECT MAX(id) FROM leads").fetchone()[0]
        after = db.execute("SELECT COUNT(*) FROM auftraege").fetchone()[0]
        db.close()
        lead = portal.get_lead(lead_id)
        self.assertEqual(lead["fahrzeug"], "Fiat Doblò XL")
        for text in ("59 € / Tag", "Ab 3 Tagen 49 € / Tag", "Jetzt verfügbar", "75 kW"):
            self.assertIn(text, lead["beschreibung"])
        self.assertEqual(before, after)
        self.assertFalse(lead.get("auftrag_id"))


if __name__ == "__main__":
    unittest.main()
