"""Shopkauf in der isolierten Werkstattdatenbank, ohne Kontakt zu Kunden oder Zahlungsdiensten."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import date, timedelta
from threading import Barrier
from unittest import TestCase, main
from unittest.mock import Mock, patch
from uuid import uuid4

from smoke_test import portal
from test_shop_order_store import quote
from shop_order_store import requires_workshop_order


class ShopWerkstattauftragTest(TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        portal.init_db()

    def setUp(self):
        day = date.today() + timedelta(days=8)
        if day.weekday() == 6:
            day += timedelta(days=1)
        self.day = day.isoformat()
        self.order_id = "sho_" + uuid4().hex
        self.order = {"id": self.order_id, "status": "paid", "mode": "live",
                      "appointment_preference": {"date": self.day, "time": "11:00"}}
        self.context = {"contact": {"name": "Test Montage", "email": "test@example.invalid", "phone": "01234000"}}
        self.snapshot = quote(mode="live")
        self.snapshot["lines"][0]["name"] = "Testreifen"
        self.snapshot["lines"][1]["name"] = "Testöl"
        with closing(portal.get_db()) as db:
            db.execute("DELETE FROM shop_reifen_termin_slots")
            db.commit()

    def ensure(self):
        return portal.erstelle_shop_montageauftrag(self.order, self.snapshot, self.context)

    def test_concurrent_order_and_one_confirmed_calendar_entry(self):
        start = Barrier(2)

        def create(_):
            start.wait(5)
            return self.ensure()

        with ThreadPoolExecutor(max_workers=2) as workers:
            first, second = workers.map(create, range(2))
        self.assertEqual(first, second)
        self.assertEqual(portal.shop_montageauftraege([self.order_id]), {self.order_id: first})
        job = portal.get_auftrag(first)
        self.assertEqual(job["auftragsnummer"], str(first))
        self.assertEqual(job["shop_order_id"], self.order_id)
        self.assertFalse(job["autohaus_id"])
        self.assertEqual((job["kennzeichen"], job["annahme_datum"]), ("", ""))
        self.assertFalse(job["kunden_status_url"])
        self.assertEqual(job["leistungsart"], "reifenservice")
        self.assertIn("4 × Testreifen", job["beschreibung"])
        self.assertIn("2 × Testöl", job["beschreibung"])
        self.assertNotIn("supplier_article_id", job["beschreibung"])
        self.assertEqual(job["schaden_aufnahme"]["shop_montagewunsch"]["time"], "11:00")
        with portal.app.test_request_context():
            self.assertFalse(any(t["shop_order_id"] == self.order_id for t in portal.gebuchte_reifen_kalendereintraege()))
            self.assertTrue(portal.bestaetige_shop_reifentermin(self.order_id, self.day, "11:00", "Test Montage"))
            self.assertFalse(portal.bestaetige_shop_reifentermin(self.order_id, self.day, "11:00", "Test Montage"))
            job = portal.get_auftrag(first)
            self.assertEqual(job["status"], 2)
            self.assertEqual(job["annahme_datum"], portal.format_date(self.day))
            terms = [t for t in portal.gebuchte_reifen_kalendereintraege() if t["shop_order_id"] == self.order_id]
            self.assertEqual(len(terms), 1)
            self.assertEqual(terms[0]["url"], portal.url_for("auftrag_detail", auftrag_id=first, back="kalender"))
            self.assertTrue(portal.reifen_anlieferung_ersetzt(job, date.fromisoformat(self.day), {first: terms[0]}))

    def test_paid_goods_and_reshipment_without_calendar(self):
        self.context["workshop_delivery"] = "customer_reshipment"
        self.assertNotIn("shop_montagewunsch", portal.get_auftrag(self.ensure())["schaden_aufnahme"])
        self.order_id = self.order["id"] = "sho_" + uuid4().hex
        self.snapshot["lines"][0]["services"] = []
        self.snapshot["lines"][0]["destination"] = "customer"
        self.assertIsNone(self.ensure())
        self.assertEqual(portal.shop_montageauftraege([self.order_id]), {})

    def test_one_operator_notice_per_paid_wheel_service_order(self):
        line, request = self.snapshot["lines"][0], self.snapshot["request"][0]
        line["id"] = request["id"] = "alloy_wheel:" + "a" * 24
        line["name"] = "Beispielfelge"
        request["services"] = {"painting": True}
        line["services"] = [{"code": "painting", "gross_cent": 12000}]
        with patch("workshop_booking_whatsapp.enqueue") as notice:
            order_number = self.ensure()
            self.assertEqual(self.ensure(), order_number)
        notice.assert_called_once()
        data = notice.call_args.kwargs
        self.assertEqual(data["event_key"], "shop-wheel-service:" + self.order_id)
        self.assertEqual(data["path"], f"/admin/auftrag/{order_number}")
        self.assertIn("Beispielfelge", data["message"])
        self.assertNotIn("Terminwunsch", data["message"])

    def test_only_selected_and_costed_work_creates_a_job(self):
        line, request = self.snapshot["lines"][0], self.snapshot["request"][0]
        for options, code, price in (
                ({}, "required:rdks", 4200), ({"disposal": True}, "disposal", 200),
                ({}, "painting", 7000), ({"painting": True}, "required:processing", 100),
                ({"painting": True}, "painting", None)):
            with self.subTest(options=options, code=code):
                request["services"] = options
                line["services"] = [{"code": code, "gross_cent": price}]
                self.assertFalse(requires_workshop_order(self.snapshot))
                with patch.object(portal, "open_fresh_db", side_effect=AssertionError("unexpected write")):
                    self.assertIsNone(self.ensure())

    def test_purchased_work_sets_type_and_describes_product_and_work(self):
        line, request = self.snapshot["lines"][0], self.snapshot["request"][0]
        for code, product, name, extra, kind, expected in (
                ("painting", "body_part", "Kotflügel", {"paint_color": "LY7W"}, "lackierung", "Farbton LY7W"),
                ("installation", "body_part", "Leuchte", {}, "mechanik", "Einbau"),
                ("oil_service", "oil", "Motoröl", {}, "oelwechsel", "Ölservice"),
                ("care", "care_product", "Autopflege", {}, "fahrzeugpflege", "Aufbereitung innen und außen")):
            with self.subTest(code=code):
                self.order["id"] = "sho_" + uuid4().hex
                line["id"] = request["id"] = product + ":sample"
                line["name"] = name
                request["services"] = {code: "complete" if code == "care" else True, **extra}
                line["services"] = [{"code": code, "gross_cent": 9000}]
                self.assertTrue(requires_workshop_order(self.snapshot))
                job = portal.get_auftrag(self.ensure())
                self.assertEqual(job["leistungsart"], kind)
                self.assertIn(name, job["beschreibung"])
                self.assertIn(expected, job["beschreibung"])
                self.assertNotIn("shop_montagewunsch", job["schaden_aufnahme"])

    def test_confirmed_before_backfill_uses_existing_time_and_replays_keep_state(self):
        portal.bestaetige_shop_reifentermin(self.order_id, self.day, "11:00", "Test Montage")
        number = self.ensure()
        self.assertEqual(portal.get_auftrag(number)["annahme_uhrzeit"], "11:00")
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET status=5, archiviert=1 WHERE id=?", (number,))
            db.commit()
        self.assertEqual(self.ensure(), number)
        self.assertEqual(portal.get_auftrag(number)["status"], 5)

    def test_order_visible_in_workshop_with_wish_and_private_shop_link(self):
        number = self.ensure()
        with portal.app.test_request_context():
            url = portal.url_for("auftrag_detail", auftrag_id=number)
            shop_url = portal.url_for("shop_admin.order_detail", order_id=self.order_id)
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        response = client.get(url)
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn(shop_url, html)
        self.assertIn("Montagewunsch · " + portal.format_date(self.day), html)
        self.assertIn(number, [job["id"] for job in portal.list_auftraege()])
        self.assertNotIn(number, [job["id"] for job in portal.list_auftraege(autohaus_id=2)])
        partner = portal.app.test_client()
        with partner.session_transaction() as session:
            session["partner_autohaus_id"] = 2
        self.assertNotIn(shop_url, partner.get(url).get_data(as_text=True))

    def test_linked_workshop_order_opens_existing_invoice_and_keeps_extras_separate(self):
        number = self.ensure()
        client = portal.app.test_client()
        with client.session_transaction() as staff:
            staff["admin"] = True
        service = Mock(spec=["statuses"])
        pdf = f"/admin/shop/{self.order_id}/rechnung.pdf"
        detail = f"/admin/auftrag/{number}"
        with patch.dict(portal.app.extensions, shop_invoice_service=service):
            service.statuses.return_value = {self.order_id: {"state": "ready", "voucher_number": "TEST-101"}}
            html = client.get(detail).get_data(as_text=True)
            self.assertIn(f'href="{pdf}"', html)
            self.assertIn("Shoprechnung TEST-101 öffnen und drucken", html)
            self.assertIn("Zusätzliche Reifenleistungen abrechnen", html)
            self.assertNotIn("Noch keine Rechnungsnummer", html)
            service.statuses.assert_called_once_with([self.order_id])
            service.statuses.return_value = {self.order_id: {"state": "preparing"}}
            html = client.get(detail).get_data(as_text=True)
            self.assertIn(f'/admin/shop/{self.order_id}#rechnung', html)
            self.assertNotIn(pdf, html)
            service.statuses.side_effect = RuntimeError("isolated test")
            response = client.get(detail)
            self.assertEqual(response.status_code, 200)
            self.assertIn("Rechnungsstatus derzeit nicht verfügbar", response.get_data(as_text=True))
            self.assertNotIn(pdf, response.get_data(as_text=True))

    def test_non_admin_or_unlinked_workshop_does_not_read_invoice(self):
        service = Mock(spec=["statuses"])
        with patch.dict(portal.app.extensions, shop_invoice_service=service):
            with portal.app.test_request_context():
                portal.session["werkstatt"] = True
                self.assertIsNone(portal.workshop_shop_invoice(self.order_id))
                portal.session["admin"] = True
                self.assertIsNone(portal.workshop_shop_invoice(None))
            service.statuses.assert_not_called()

    def test_board_print_requires_workshop_session_and_current_operational_release(self):
        number = self.ensure()
        label = f"/werkstatt/auftrag/{number}/reifenetikett"
        client = portal.app.test_client()
        self.assertEqual(client.get(label).status_code, 302)
        with client.session_transaction() as session:
            session["partner_autohaus_id"] = 2
        self.assertEqual(client.get(label).status_code, 302)
        portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "SYNTHETIC99")
        with client.session_transaction() as session:
            session.clear()
            session["werkstatt_tafel"] = portal.werkstatt_tafel_session_token()
        self.assertIn(label, client.get("/werkstatt/tafel").get_data(as_text=True))
        self.assertIn(label, client.get(f"/werkstatt/auftrag/{number}").get_data(as_text=True))
        with patch("shop_admin.shop_admin_reader") as reader:
            reader.return_value.detail.return_value = {"lines": [{"quantity": 4, "name": "Testreifen",
                "tyre_size": "205/55 R16", "category": "tyre", "supplier": {"eta_date": ""}}]}
            result = client.get(label)
            self.assertEqual(result.status_code, 200)
            self.assertIn("205/55 R16", result.get_data(as_text=True))
            self.assertIn(f'/werkstatt/auftrag/{number}/ausdrucken', result.text)
            self.assertNotIn(f'/admin/auftrag/{number}/ausdrucken', result.text)
            self.assertIn("no-store", result.headers["Cache-Control"])
            self.assertEqual(result.headers["Referrer-Policy"], "no-referrer")
            self.assertNotEqual(client.get(f"/admin/auftrag/{number}/reifenetikett").status_code, 200)
            reader.reset_mock()
            for status, archived, proposal, insurer in ((5, 0, 0, 0), (2, 1, 0, 0), (1, 0, 1, 0), (2, 0, 0, 1)):
                with closing(portal.get_db()) as db:
                    db.execute("UPDATE auftraege SET status=?,archiviert=?,angebotsphase=?,versicherung_id=? WHERE id=?",
                               (status, archived, proposal, insurer, number))
                    db.commit()
                self.assertEqual(client.get(label).status_code, 404)
                self.assertNotIn(label, client.get("/werkstatt/tafel").get_data(as_text=True))
            reader.assert_not_called()

    def test_failed_transaction_can_retry_and_unpaid_or_test_never_creates(self):
        for mode, status in (("test", "paid"), ("live", "payment_pending")):
            self.order.update(mode=mode, status=status)
            with self.assertRaises(ValueError):
                self.ensure()
        self.order.update(mode="live", status="paid")
        original_create = portal.create_auftrag

        def interrupted(*args, **kwargs):
            original_create(*args, **kwargs)
            raise RuntimeError("interrupted")

        with patch.object(portal, "create_auftrag", interrupted), self.assertRaises(RuntimeError):
            self.ensure()
        self.assertEqual(portal.shop_montageauftraege([self.order_id]), {})
        with closing(portal.get_db()) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM auftraege WHERE schaden_aufnahme_json LIKE ?",
                                        ("%" + self.order_id + "%",)).fetchone()[0], 0)
        self.assertGreater(self.ensure(), 0)


if __name__ == "__main__":
    main()
