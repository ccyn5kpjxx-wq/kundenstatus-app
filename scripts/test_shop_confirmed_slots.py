"""Isolated integration tests for the workshop's shared tyre appointment capacity."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import date, datetime, timedelta
from threading import Barrier
import unittest
from zoneinfo import ZoneInfo

from smoke_test import portal


class ConfirmedSlotsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        portal.init_db()

    def setUp(self):
        with closing(portal.get_db()) as db:
            for table in ("shop_reifen_termin_slots", "reifen_termin_slots", "check24_bookings"):
                db.execute("DELETE FROM " + table)
            self.lead = db.execute("""INSERT INTO leads(kunde_name,erstellt_am,geaendert_am)
                                      VALUES('Montage Test','','')""").lastrowid
            db.commit()
        day = date.today() + timedelta(days=8)
        if day.weekday() == 6:
            day += timedelta(days=1)
        self.day = day.isoformat()
        self.order = "sho_" + "1" * 32

    def reserve_website(self):
        with closing(portal.get_db()) as db:
            portal.reserviere_reifen_slot(db, self.lead, self.day, "11:00")
            db.commit()

    def confirm_shop(self):
        return portal.bestaetige_shop_reifentermin(self.order, self.day, "11:00", "Shop Test")

    def test_website_and_shop_block_each_other(self):
        self.reserve_website()
        with self.assertRaises(portal.ReifenSlotBelegt):
            self.confirm_shop()
        with closing(portal.get_db()) as db:
            db.execute("DELETE FROM reifen_termin_slots WHERE lead_id=?", (self.lead,))
            db.commit()
        self.assertTrue(self.confirm_shop())
        self.assertFalse(self.confirm_shop())
        with self.assertRaises(portal.ReifenSlotBelegt):
            self.reserve_website()
        self.assertNotIn("11:00", portal.freie_reifen_slots(self.day))
        self.assertIn("12:00", portal.freie_reifen_slots(self.day))

    def test_partial_check24_overlap_in_both_directions(self):
        start = datetime.fromisoformat(self.day + "T11:30").replace(tzinfo=ZoneInfo("Europe/Berlin"))
        end = start + timedelta(minutes=30)
        with closing(portal.get_db()) as db:
            db.execute("""INSERT INTO check24_bookings (provider_id,order_id,start_at,end_at,event_id)
                          VALUES('TEST',1,?,?,1)""", (start.isoformat(), end.isoformat()))
            db.commit()
        with self.assertRaises(portal.ReifenSlotBelegt):
            self.confirm_shop()
        with closing(portal.get_db()) as db:
            db.execute("UPDATE check24_bookings SET active=0 WHERE provider_id='TEST'")
            db.commit()
        self.confirm_shop()
        with closing(portal.get_db()) as db:
            self.assertTrue(portal.check24_bookings._conflict(db, start, end))
            self.assertFalse(portal.check24_bookings._conflict(db, end, end + timedelta(minutes=30)))

    def test_concurrent_website_and_shop_writers_have_one_winner(self):
        start = Barrier(2)

        def book(action):
            start.wait(timeout=5)
            try:
                action()
                return True
            except portal.ReifenSlotBelegt:
                return False

        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(book, (self.confirm_shop, self.reserve_website)))
        self.assertEqual(sorted(results), [False, True])
        with closing(portal.get_db()) as db:
            self.assertEqual(sum(db.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                                 for table in ("shop_reifen_termin_slots", "reifen_termin_slots")), 1)


if __name__ == "__main__":
    unittest.main()
