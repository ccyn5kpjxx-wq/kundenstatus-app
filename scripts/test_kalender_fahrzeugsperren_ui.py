"""Admin form, occupancy in the monthly calendar and safe release."""
from datetime import date, timedelta
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run_mos_public_test import build_test_app


class CalendarBlockFormTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        directory = tempfile.TemporaryDirectory(prefix="mos-kalender-form-")
        cls.addClassCleanup(directory.cleanup)
        cls.portal = build_test_app(directory.name, origin="http://localhost")
        cls.portal.app.config["TESTING"] = True

    def setUp(self):
        self.client = self.portal.app.test_client()
        with self.client.session_transaction() as session:
            session["admin"] = True
        self.start = date.today() + timedelta(days=12)
        self.end = self.start + timedelta(days=2)
        self.vehicle = self.portal.app.config["MOS_PUBLIC_BOOKING"]["fleet"]["i10"]["id"]
        self.fields = dict(vehicle_id=str(self.vehicle), start_date=self.start.isoformat(),
                           end_date=self.end.isoformat(), source="vor_ort", note="Abholung vor Ort")
        self.form_url = "/admin/kalender/fahrzeugsperren"
        self.page = self.client.get("/admin/cockpit?mietsperre=1")
        self.assertEqual(self.page.status_code, 200)
        with self.client.session_transaction() as session:
            self.csrf = session["csrf_token"]

    def test_admin_create_conflict_calendar_and_release(self):
        self.assertIn('id="fahrzeug-sperren-formular"', self.page.get_data(as_text=True))
        self.assertIn("Ab diesem Tag blockieren", self.page.get_data(as_text=True))
        fields = {**self.fields, "csrf_token": self.csrf}
        saved = self.client.post(self.form_url, data=fields)
        self.assertEqual(saved.status_code, 303)
        block_id = int(saved.location.rsplit("-", 1)[1])
        html = self.client.get(saved.location).get_data(as_text=True)
        self.assertIn(f'id="fahrzeugsperre-{block_id}"', html)
        self.assertIn("Fahrzeugsperre</span>", html)
        self.assertIn("Vor-Ort-Buchung", html)
        self.assertIn("Abholung vor Ort", html)
        again = self.client.post(self.form_url, data=fields)
        self.assertEqual(again.status_code, 409)
        self.assertIn('role="alert"', again.get_data(as_text=True))
        self.assertIn('value="Abholung vor Ort"', again.get_data(as_text=True))

        with self.portal.app.test_request_context("/"):
            cal = self.portal.build_mini_monatskalender([], selected_day_value=self.end.isoformat(),
                                                       include_internal_notes=True)
            self.assertTrue(any(item["label"] == "Fahrzeugsperre" for item in cal["selected_day"]["items"]))
            partner = self.portal.build_mini_monatskalender([], selected_day_value=self.end.isoformat())
            self.assertFalse(partner["fahrzeugsperren"])
            self.assertFalse(any(item["label"] == "Fahrzeugsperre" for item in partner["selected_day"]["items"]))

        release = f"{self.form_url}/{block_id}/freigeben"
        self.assertEqual(self.client.post(release).status_code, 400)
        response = self.client.post(release, data={"monat": self.start.strftime("%Y-%m"), "csrf_token": self.csrf})
        self.assertEqual(response.status_code, 303)
        self.assertNotIn(f'id="fahrzeugsperre-{block_id}"', self.client.get(response.location).get_data(as_text=True))

    def test_protected_routes_and_navigation(self):
        self.assertEqual(self.client.post(self.form_url, data=self.fields).status_code, 400)
        guest = self.portal.app.test_client()
        guest.get("/login")
        with guest.session_transaction() as session:
            token = session["csrf_token"]
        for path in (self.form_url, self.form_url + "/1/freigeben"):
            res = guest.post(path, data={**self.fields, "csrf_token": token})
            self.assertIn(res.status_code, (302, 303))
            self.assertIn("/login", res.location)
        for path in ("/admin/cockpit", "/admin/kalender", "/admin/mietfahrzeuge",
                     "/mietwagen-test/admin", "/mietwagen-test/admin/termine"):
            with self.subTest(path=path):
                page = self.client.get(path)
                self.assertEqual(page.status_code, 200)
                self.assertIn("mietsperre=1#fahrzeug-sperren", page.get_data(as_text=True))

    def test_calendar_deep_link_survives_login_and_expired_csrf(self):
        guest = self.portal.app.test_client()
        target = "/admin/cockpit?mietsperre=1#fahrzeug-sperren"
        login = "/login?return_to=mietkalender"
        self.assertEqual(guest.get(target).location, login)
        self.assertEqual(guest.get(login).status_code, 200)
        with patch.object(self.portal, "admin_password_matches", return_value=True) as password_check:
            self.assertEqual(guest.post(login, data={"password": "fixture"}).location, login)
            password_check.assert_not_called()
            guest.get(login)
            with guest.session_transaction() as session:
                token = session["csrf_token"]
            result = guest.post(login, data={"password": "fixture", "csrf_token": token})
            password_check.assert_called_once_with("fixture")
        self.assertEqual(result.location, target)
        html = guest.get(result.location).get_data(as_text=True)
        self.assertIn('id="fahrzeug-sperren-formular"', html)
        self.assertIn("data-cockpit-kalender open", html)
        self.assertEqual(guest.get(login).location, target)


if __name__ == "__main__":
    unittest.main()
