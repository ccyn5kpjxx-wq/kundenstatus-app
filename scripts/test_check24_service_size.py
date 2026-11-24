"""Booking-mail service labels with and without the wheel size."""
from pathlib import Path
import sys
import unittest
from datetime import datetime, timedelta
from io import BytesIO

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from check24_mail import BERLIN, MONTHS, parse_check24_mail
from scripts.test_check24_buchungen import BODY, synthetic_mail


class Check24ServiceSizeTest(unittest.TestCase):
    def test_mounting_with_and_without_size(self):
        for label in ("Reifenmontage für 2 Reifen", "Reifenmontage für 4 Reifen (15 Zoll)"):
            body = BODY.replace("Reifenmontage für 2 Reifen", label)
            for html in (False, True):
                with self.subTest(label=label, html=html):
                    content = "".join(f"<p>{line}</p>" for line in body.splitlines()) if html else body
                    result = parse_check24_mail(synthetic_mail(content, html=html))
                    self.assertTrue(result["valid"], result["errors"])
                    self.assertEqual(result["service"], label)

    def test_conflicting_or_unrecognized_mounting_position_stays_in_review(self):
        first = "Reifenmontage für 4 Reifen (15 Zoll)"
        for second in ("Reifenmontage für 4 Reifen (17 Zoll)", "Reifenmontage für 2 Reifen",
                       "Reifenmontage für 4 Reifen (Größe offen)"):
            with self.subTest(second=second):
                body = BODY.replace("Reifenmontage für 2 Reifen", f"{first}\n{second}")
                result = parse_check24_mail(synthetic_mail(body))
                self.assertFalse(result["valid"])
                self.assertFalse(result["service"])

    def test_mounting_label_outside_booked_services_is_not_an_order(self):
        body = BODY.replace("Reifenmontage für 2 Reifen", "Beratung")
        result = parse_check24_mail(synthetic_mail(body + "\nReifenmontage für 4 Reifen (15 Zoll)"))
        self.assertFalse(result["valid"])
        self.assertFalse(result["service"])

    def test_original_eml_creates_one_order_and_blocks_the_public_slot(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from smoke_test import portal  # Temporary database with all external connections blocked.

        today = datetime.now(BERLIN).date()
        monday = today + timedelta(days=7 - today.weekday())
        month_name = next(name for name, number in MONTHS.items() if number == monday.month)
        label = "Reifenmontage für 4 Reifen (15 Zoll)"
        number = "TEST15ZOLL"
        body = BODY.replace("TEST9001", number).replace("Reifenmontage für 2 Reifen", label)
        body = body.replace("Voraussichtliche Zustellung: Do. 29. Oktober\n", "")
        body = body.replace("Fr. 30. Oktober", f"Mo. {monday.day}. {month_name} {monday.year}")
        body = body.replace("2x Beispielreifen 185/60 R15 88 V, Ganzjahresreifen", "")
        raw = synthetic_mail(body, subject=f"Neue CHECK24 Terminbuchung (Bestellnummer: {number})")
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        client.get("/admin/check24")
        with client.session_transaction() as session:
            csrf = {portal.CSRF_FIELD_NAME: session[portal.CSRF_FIELD_NAME]}

        availability = f"/api/reifenservice/termine?datum={monday.isoformat()}"
        self.assertIn("08:00", client.get(availability).get_json()["zeiten"])
        response = client.post("/admin/check24/import", data={**csrf, "mail": (BytesIO(raw), "test.eml")})
        self.assertEqual(response.status_code, 302)
        event = next(event for event in portal.check24_bookings.rows() if event["provider_id"] == number)
        self.assertEqual(event["state"], "review")
        self.assertIsNone(event["order_id"])
        accepted = client.post(f"/admin/check24/{event['id']}/uebernehmen", data={**csrf, "reviewed": "1"})
        self.assertEqual(accepted.status_code, 302)
        repeated = client.post("/admin/check24/import", data={**csrf, "mail": (BytesIO(raw), "test.eml")})
        self.assertEqual(repeated.status_code, 302)
        events = [event for event in portal.check24_bookings.rows() if event["provider_id"] == number]
        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event["state"], "applied")
        self.assertEqual(accepted.location, f"/admin/auftrag/{event['order_id']}")
        order = portal.get_auftrag(event["order_id"])
        self.assertEqual(order["quelle"], "check24")
        self.assertEqual(order["auftragsnummer"], number)
        self.assertIn(label, order["beschreibung"])
        self.assertIn(number, client.get("/admin/reifenportal").get_data(as_text=True))
        slots = client.get(availability).get_json()["zeiten"]
        self.assertNotIn("08:00", slots)
        self.assertIn("09:00", slots)


if __name__ == "__main__":
    unittest.main()
