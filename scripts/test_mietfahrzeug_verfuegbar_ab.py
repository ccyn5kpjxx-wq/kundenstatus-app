"""Dated rental releases in the real portal with isolated records and no network."""
from datetime import datetime, timedelta
from pathlib import Path
import sys
import unittest
import uuid
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import smoke_test as isolated

from mietwagen_checkout import SharedCheckout
from mos_booking.gateway import OfflineGateway

portal = isolated.portal


class DatedVehicleReleaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config.update(TESTING=True, MOS_SHARED_CHECKOUT_ENABLED=True)
        portal.init_db()

    def setUp(self):
        self.release = datetime.now(ZoneInfo("Europe/Berlin")).date() + timedelta(days=20)
        self.vid = portal.create_mietfahrzeug(
            kennzeichen="TEST-" + uuid.uuid4().hex[:10], bezeichnung="Test Kastenwagen",
            fahrzeugklasse="Transporter", tagessatz="69", verfuegbar_ab=self.release.isoformat())

    def stored(self):
        db = portal.get_db()
        try:
            return dict(db.execute("SELECT * FROM mietfahrzeuge WHERE id=?", (self.vid,)).fetchone())
        finally:
            db.close()

    def update(self, **changes):
        values = self.stored()
        values.pop("verfuegbar_ab", None)
        values.update(changes)
        portal.update_mietfahrzeug(self.vid, **values)

    def rental(self, start=None, end=None):
        start = self.release if start is None else start
        return portal.create_mietvorgang(
            self.vid, kunde_name="Test Person", kunde_email="test@example.invalid",
            start_datum=start.isoformat(), end_datum=(end or start + timedelta(days=1)).isoformat())

    def checkout(self, start=None):
        start = self.release if start is None else start
        gateway = OfflineGateway(isolated.TEMP_DIR / "dated-provider.db", "http://127.0.0.1:5084", "test-only")
        service = SharedCheckout(portal, gateway)
        quote = {"amount_cents": 6900, "currency": "eur", "rules_version": "synthetic-test-only"}
        return service.reserve(uuid.uuid4().hex, self.vid, start.isoformat(),
                               (start + timedelta(days=1)).isoformat(),
                               {"name": "Test Person", "email": "test@example.invalid"}, quote)

    def test_empty_default_keeps_existing_vehicle_unrestricted(self):
        other = portal.create_mietfahrzeug(kennzeichen="TEST-" + uuid.uuid4().hex[:10])
        self.assertTrue(portal.mietfahrzeug_zeitraum_frei(other, "2026-11-01", "2026-11-02"))

    def test_release_day_is_inclusive_but_overlap_from_before_is_rejected(self):
        before = self.release - timedelta(days=1)
        self.assertFalse(portal.mietfahrzeug_zeitraum_frei(self.vid, before.isoformat(), self.release.isoformat()))
        self.assertTrue(portal.mietfahrzeug_zeitraum_frei(self.vid, self.release.isoformat(),
                                                       (self.release + timedelta(days=1)).isoformat()))

    def test_manual_rental_rejects_before_release_with_exact_date(self):
        with self.assertRaisesRegex(ValueError, self.release.strftime(portal.DATE_FMT)):
            self.rental(self.release - timedelta(days=1))
        self.assertIsNotNone(self.rental())

    def test_shared_checkout_rechecks_release_under_inventory_lock(self):
        with self.assertRaisesRegex(ValueError, "Zeitraum bereits belegt"):
            self.checkout(self.release - timedelta(days=1))
        self.assertEqual(self.checkout()["status"], "pending")

    def test_invalid_date_is_rejected_without_changing_vehicle(self):
        original = self.stored()
        with self.assertRaisesRegex(ValueError, "gültiges Datum"):
            self.update(verfuegbar_ab="2026-02-30", bezeichnung="Not saved")
        self.assertEqual(self.stored(), original)
        with self.assertRaisesRegex(ValueError, "gültiges Datum"):
            portal.create_mietfahrzeug(kennzeichen="TEST-INVALID", verfuegbar_ab="not-a-date")

    def test_german_date_is_stored_canonically(self):
        self.update(verfuegbar_ab=self.release.strftime(portal.DATE_FMT))
        self.assertEqual(self.stored()["verfuegbar_ab"], self.release.isoformat())

    def test_missing_field_preserves_date_and_explicit_empty_clears_it(self):
        self.update(bezeichnung="Updated name")
        self.assertEqual(self.stored()["verfuegbar_ab"], self.release.isoformat())
        self.update(verfuegbar_ab="")
        self.assertEqual(self.stored()["verfuegbar_ab"], "")
        self.assertTrue(portal.mietfahrzeug_zeitraum_frei(self.vid, "2026-11-01", "2026-11-02"))

    def test_later_release_cannot_invalidate_open_manual_rental(self):
        self.rental()
        with self.assertRaisesRegex(ValueError, "offenen Vermietung oder Reservierung"):
            self.update(verfuegbar_ab=(self.release + timedelta(days=1)).isoformat())
        self.assertEqual(self.stored()["verfuegbar_ab"], self.release.isoformat())

    def test_later_release_cannot_invalidate_open_checkout(self):
        hold = self.checkout()
        with self.assertRaisesRegex(ValueError, "offenen Vermietung oder Reservierung"):
            self.update(verfuegbar_ab=(self.release + timedelta(days=1)).isoformat())
        db = portal.get_db()
        try:
            self.assertEqual(db.execute("SELECT status FROM miet_checkout_holds WHERE id=?", (hold["id"],)).fetchone()[0], "pending")
        finally:
            db.close()

    def test_completed_rental_history_does_not_block_date_change(self):
        rental_id = self.rental()
        db = portal.get_db()
        db.execute("UPDATE mietvorgaenge SET status='zurueck',rueckgabe_datum=? WHERE id=?",
                   ((self.release + timedelta(days=1)).isoformat(), rental_id))
        db.commit()
        db.close()
        self.update(verfuegbar_ab=(self.release + timedelta(days=2)).isoformat())
        self.assertEqual(len(portal.list_mietvorgaenge(self.vid)), 1)

    def test_cockpit_switches_on_berlin_release_day_without_manual_status_edit(self):
        original = self.stored()

        class AtRelease(datetime):
            @classmethod
            def now(cls, tz=None):
                value = datetime.combine(self.release, datetime.min.time(), tzinfo=ZoneInfo("Europe/Berlin"))
                return value.astimezone(tz) if tz else value.replace(tzinfo=None)

        before = portal.hydrate_mietfahrzeug(original, vorgaenge=[], bilder=[])
        self.assertEqual(before["effektiver_status"], "bald")
        self.assertIn(self.release.strftime(portal.DATE_FMT), before["effektiver_status_label"])
        with patch.object(portal, "datetime", AtRelease):
            after = portal.hydrate_mietfahrzeug(original, vorgaenge=[], bilder=[])
        self.assertTrue(after["ist_verfuegbar"])
        self.assertEqual(after["basis_status"], "verfuegbar")
        self.assertEqual(self.stored(), original, "Rendering cannot mutate the vehicle")

    def admin_client(self):
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
            session["csrf_token"] = "dated-vehicle-test-token"
        return client

    def test_admin_forms_include_release_date_and_old_edit_form_preserves_it(self):
        client = self.admin_client()
        page = client.get("/admin/mietfahrzeuge")
        self.assertEqual(page.status_code, 200)
        self.assertIn('name="verfuegbar_ab"', page.text)
        self.assertEqual(page.text.count('id="mietfahrzeug-neu-verfuegbar-ab"'), 1)
        self.assertEqual(page.text.count(f'id="mietfahrzeug-verfuegbar-ab-{self.vid}"'), 1)
        self.assertIn('value="' + self.release.isoformat() + '"', page.text)
        response = client.post(f"/admin/mietfahrzeuge/{self.vid}/bearbeiten", data={
            "csrf_token": "dated-vehicle-test-token", "kennzeichen": self.stored()["kennzeichen"],
            "bezeichnung": "Older form update", "status": "verfuegbar", "aktiv": "1"})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.stored()["verfuegbar_ab"], self.release.isoformat())

    def test_admin_create_saves_date_and_invalid_edit_keeps_record(self):
        client = self.admin_client()
        plate = "TEST-" + uuid.uuid4().hex[:10].upper()
        response = client.post("/admin/mietfahrzeuge/neu", data={
            "csrf_token": "dated-vehicle-test-token", "kennzeichen": plate,
            "bezeichnung": "Test Kastenwagen", "status": "verfuegbar", "tagessatz": "69",
            "verfuegbar_ab": self.release.isoformat()})
        self.assertEqual(response.status_code, 302)
        db = portal.get_db()
        try:
            row = db.execute("SELECT verfuegbar_ab FROM mietfahrzeuge WHERE kennzeichen=?", (plate,)).fetchone()
            self.assertEqual(row[0], self.release.isoformat())
        finally:
            db.close()
        original = self.stored()
        response = client.post(f"/admin/mietfahrzeuge/{self.vid}/bearbeiten", data={
            "csrf_token": "dated-vehicle-test-token", "kennzeichen": original["kennzeichen"],
            "status": "verfuegbar", "verfuegbar_ab": "2026-13-01"}, follow_redirects=True)
        self.assertIn("gültiges Datum", response.text)
        self.assertEqual(self.stored(), original)

    def test_public_ducato_card_and_fleet_have_labelled_photo_price_and_date(self):
        self.update(bezeichnung="Fiat Ducato", tagessatz="69")
        row = next(f for f in portal.mietwagen_public_fahrzeuge() if f["id"] == self.vid)
        self.assertEqual(row["bild_url"], "/static/mietwagen/ducato-hersteller.jpg")
        self.assertIn("Herstellerabbildung", row["bild_hinweis"])
        self.assertEqual(row["tagessatz"], "69")
        self.assertEqual(row["verfuegbar_ab_label"], self.release.strftime(portal.DATE_FMT))
        self.assertTrue(row["bald"])
        self.assertEqual(portal.mietwagen_symbolfoto_pfad(self.stored()).name, "ducato-hersteller.jpg")
        client = portal.app.test_client()
        for url in ("/mietwagen-info", "/mietwagen"):
            page = client.get(url)
            self.assertEqual(page.status_code, 200)
            self.assertIn("Verfügbar ab " + self.release.strftime(portal.DATE_FMT), page.text)
            self.assertIn("/static/mietwagen/ducato-hersteller.jpg", page.text)
        inquiry = client.get("/anfrage?anliegen=mietwagenanfrage")
        self.assertIn('value="fiat-ducato"', inquiry.text)
        self.assertIn("69 € / Tag", inquiry.text)
        self.assertIn("150 km pro Tag inklusive", inquiry.text)
        self.assertIn("Herstellerabbildung", inquiry.text)


if __name__ == "__main__":
    unittest.main()
