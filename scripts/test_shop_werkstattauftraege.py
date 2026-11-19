"""Shopkauf in der isolierten Werkstattdatenbank, ohne Kontakt zu Kunden oder Zahlungsdiensten."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import date, timedelta
from threading import Barrier
from unittest import TestCase, main
from unittest.mock import patch
from uuid import uuid4

from smoke_test import portal
from test_shop_order_store import quote


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
        job = portal.get_auftrag(self.ensure())
        self.assertEqual(job["leistungsart"], "sonstiges")
        self.assertEqual(job["annahme_datum"], "")
        self.assertNotIn("shop_montagewunsch", job["schaden_aufnahme"])
        self.assertNotIn("Reifenmontage", job["beschreibung"])
        self.assertIn("Testreifen", job["beschreibung"])

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
