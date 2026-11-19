"""Exercise the public booking form against the isolated workshop database."""

from datetime import date, timedelta
from unittest import TestCase, main
from unittest.mock import patch

import smoke_test as isolated


portal = isolated.portal


class ReifenterminDirektTest(TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        portal.init_db()

    def setUp(self):
        self.client = portal.app.test_client()
        for provider in ("sende_website_lead_benachrichtigung", "lead_notice", "notify_lead_workshop", "schedule_change_backup"):
            self.enterContext(patch.object(portal, provider))
        self.url = "/anfrage?anliegen=reifenservice"

    def booking_form(self, response):
        marker = 'class="form-card tire-booking-form"'
        self.assertIn(marker, response.text)
        return response.text.split(marker, 1)[1].split("</form>", 1)[0]

    def test_actions_and_required_fields_precede_optional_details(self):
        form = self.booking_form(self.client.get(self.url))
        elements = ('value="radwechsel"', 'value="montage"', 'class="booking-more-services"',
                    'id="wunschdatum"', 'id="wunschzeit"', 'id="name"', 'id="telefon"',
                    'class="booking-options"', 'id="email"', 'id="bilder"', 'type="submit"')
        offsets = [form.index(element) for element in elements]
        self.assertEqual(offsets, sorted(offsets))
        self.assertIn('<details class="booking-options">', form)
        self.assertIn('<details class="booking-more-services">', form)
        self.assertIn('aria-controls="reifenpreise"', form)
        self.assertIn('id="photo-preview"', form)

    def test_preselected_shop_service_opens_its_group(self):
        form = self.booking_form(self.client.get(self.url + "&shop_leistung=reifencheck"))
        self.assertIn('<details class="booking-more-services" open>', form)
        self.assertIn('value="pruefung" checked', form)
        self.assertIn('name="shop_leistung" value="reifencheck"', form)
        main_service = self.booking_form(self.client.get(self.url + "&shop_leistung=reifenmontage"))
        self.assertIn('value="montage" checked', main_service)
        self.assertIn('<details class="booking-more-services">', main_service)

    def test_existing_fragment_exposes_order_path_before_workshop_form(self):
        with patch.dict(portal.app.config, SHOP_FRONTEND_ENABLED=True, SHOP_ORDER_DRAFT_ENABLED=True,
                        SHOP_CHECKOUT_ENABLED=True):
            page = self.client.get(self.url + "&leistungsart=reifenservice").text
        self.assertEqual(page.count('id="kurzanfrage"'), 1)
        self.assertLess(page.index('id="kurzanfrage"'), page.index('href="/shop/reifen/?montage=1"'))
        self.assertLess(page.index('Reifen mit Montage bestellen'), page.index('id="terminbuchung"'))
        with patch.dict(portal.app.config, SHOP_CHECKOUT_ENABLED=False):
            self.assertIn('Neue Reifen anfragen', self.client.get(self.url).text)

    def test_calendar_on_public_hostname_and_without_javascript(self):
        day = date.today() + timedelta(days=10)
        if day.weekday() == 6:
            day += timedelta(days=1)
        day = day.isoformat()
        with patch.object(portal, "PUBLIC_HOSTS", {"homepage.example"}):
            origin = "https://homepage.example"
            times = self.client.get("/api/reifenservice/termine", query_string={"datum": day}, base_url=origin)
            self.assertEqual(times.status_code, 200)
            self.assertIn("09:00", times.get_json()["zeiten"])
            page = self.client.get(self.url + "&wunschdatum=" + day, base_url=origin)
            self.assertIn('value="09:00"', self.booking_form(page))
            self.assertIn('value="' + day + '"', self.booking_form(page))

    def test_occupied_time_updates_choices_and_preserves_input(self):
        self.client.get(self.url)
        with self.client.session_transaction() as sess:
            csrf = sess[portal.CSRF_FIELD_NAME]
        day = date.today() + timedelta(days=15)
        if day.weekday() == 6:
            day += timedelta(days=1)
        booking = {
            portal.CSRF_FIELD_NAME: csrf, "anliegen": "reifenservice", "reifen_leistung": ["montage", "wuchten"],
            "wunschdatum": day.isoformat(), "wunschzeit": "11:00", "name": "Werkstatt Test",
            "telefon": "06261 000000", "fahrzeug": "Muster-Pkw", "nachricht": "Eigene Reifen.",
        }
        self.assertEqual(self.client.post("/anfrage", data=booking).status_code, 302)
        conflict = self.client.post("/anfrage", data=booking)
        self.assertEqual(conflict.status_code, 409)
        form = self.booking_form(conflict)
        for value in ('value="montage" checked', 'value="wuchten" checked', 'value="Muster-Pkw"',
                      'Eigene Reifen.', '<details class="booking-options" open>', 'role="alert"'):
            self.assertIn(value, form)
        self.assertNotIn('<option value="11:00"', form)
        self.assertIn('<option value="12:00"', form)


if __name__ == "__main__":
    main(verbosity=2)
