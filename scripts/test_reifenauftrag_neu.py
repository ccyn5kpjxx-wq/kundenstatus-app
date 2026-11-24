"""Customer-free integration checks for the tire order print queue."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from io import BytesIO
import json
import re
import sqlite3
from threading import Barrier, Event
import unittest
from unittest.mock import patch

import fitz
from smoke_test import portal
from werkzeug.datastructures import FileStorage
import werkstatt_druckpaket


class TirePrintQueueTests(unittest.TestCase):
    def setUp(self):
        self.order = self.new_tire_order("Druckauftrag A")
        self.other = self.new_tire_order("Druckauftrag B")
        self.client = self.login()

    def new_tire_order(self, label):
        return portal.create_auftrag("intern", leistungsart="reifenservice",
                                     kunde_name=label, fahrzeug="Testfahrzeug", beschreibung="Reifenmontage")

    def login(self):
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        return client

    def listing(self, client=None):
        response = (client or self.client).get("/admin/reifenportal")
        self.assertEqual(response.status_code, 200)
        return response.get_data(as_text=True)

    def card(self, html, order_id):
        match = re.search(rf'<article\b[^>]*\bdata-reifenauftrag="{order_id}"[^>]*>.*?</article>', html, re.S)
        self.assertIsNotNone(match, f"Reifenauftrag {order_id} fehlt im Portal")
        return match.group()

    def printed_at(self, order_id):
        with closing(portal.get_db()) as db:
            return db.execute("SELECT reifen_auftrag_gedruckt_am FROM auftraege WHERE id=?",
                              (order_id,)).fetchone()[0]

    def csrf(self):
        with self.client.session_transaction() as session:
            return {portal.CSRF_FIELD_NAME: session[portal.CSRF_FIELD_NAME]}

    def assert_order_pdf(self, response, order_id):
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/pdf")
        with fitz.open(stream=response.data, filetype="pdf") as pdf:
            self.assertIn(f"Auftrag {order_id}", pdf[0].get_text())

    def test_only_completed_print_request_acknowledges_its_own_order(self):
        for section in ("admin", "werkstatt"):
            with self.subTest(section=section):
                oid = self.new_tire_order(section)
                listing = self.listing()
                self.assertRegex(listing, r'<option\b[^>]*value="neu"')
                self.assertIn('id="reifen-neu-filter"', listing)
                card = self.card(listing, oid)
                self.assertIn("data-reifen-neu", card)
                form = re.search(r'<form\b[^>]*\bdata-reifen-druck\b[^>]*>.*?</form>', card, re.S)
                self.assertIsNotNone(form)
                self.assertIn(f'action="/admin/auftrag/{oid}/ausdrucken"', form.group())
                self.assertIn('method="post"', form.group().lower())
                self.assertIn(f'name="{portal.CSRF_FIELD_NAME}"', form.group())
                self.assertIn("data-reifen-neu", self.card(listing, self.other))
                path = f"/{section}/auftrag/{oid}/ausdrucken"
                for _ in range(2):
                    self.assert_order_pdf(self.client.get(path), oid)
                self.assertEqual(self.client.post(path).status_code, 400)
                self.assertFalse(self.printed_at(oid))
                self.assert_order_pdf(self.client.post(path, data=self.csrf()), oid)
                self.assertTrue(self.printed_at(oid))
                self.assertEqual(portal.get_auftrag(oid)["status"], 1)

                # Preserve the first receipt even when a reprint happens on a later day.
                first_print = "15.11.2026 09:10"
                with closing(portal.get_db()) as db:
                    db.execute("UPDATE auftraege SET reifen_auftrag_gedruckt_am=? WHERE id=?", (first_print, oid))
                    db.commit()
                self.assert_order_pdf(self.client.post(path, data=self.csrf()), oid)
                self.assertEqual(self.printed_at(oid), first_print)
                fresh_session = self.listing(self.login())
                self.assertNotIn("data-reifen-neu", self.card(fresh_session, oid))
                self.assertIn("data-reifen-neu", self.card(fresh_session, self.other))
                self.assertFalse(self.printed_at(self.other))

    def test_opening_details_labels_or_invoicing_does_not_acknowledge(self):
        self.listing()
        calculation = portal.berechne_reifenservice([{"preis_id": "montage_17_18", "menge": 4}])
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET reifen_rechnung_json=? WHERE id=?",
                       (json.dumps({"kalkulation": calculation}), self.order))
            db.commit()
        for path in (f"/admin/auftrag/{self.order}", f"/werkstatt/auftrag/{self.order}",
                     f"/admin/auftrag/{self.order}/reifenetikett", f"/werkstatt/auftrag/{self.order}/reifenetikett",
                     f"/admin/auftrag/{self.order}/reifen-rechnung", f"/admin/auftrag/{self.order}/reifen-rechnung/druck"):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 200)
                self.assertFalse(self.printed_at(self.order))
        self.assertIn("data-reifen-neu", self.card(self.listing(), self.order))

    def test_failed_pdf_or_unauthorized_request_does_not_acknowledge(self):
        self.listing()
        path = f"/admin/auftrag/{self.order}/ausdrucken"
        stranger = portal.app.test_client()
        stranger.get("/login")
        with stranger.session_transaction() as session:
            token = session[portal.CSRF_FIELD_NAME]
        unauthenticated = stranger.post(path, data={portal.CSRF_FIELD_NAME: token})
        self.assertEqual(unauthenticated.status_code, 302)
        self.assertFalse(self.printed_at(self.order))
        with patch("werkstatt_druckpaket._cover", side_effect=RuntimeError("PDF generation failed")), \
                patch.dict(portal.app.config, {"PROPAGATE_EXCEPTIONS": True}):
            with self.assertRaisesRegex(RuntimeError, "PDF generation failed"):
                self.client.post(path, data=self.csrf())
        self.assertFalse(self.printed_at(self.order))
        self.assertIn("data-reifen-neu", self.card(self.listing(), self.order))

    def test_missing_original_keeps_badge_until_complete_package_is_printed(self):
        with fitz.open() as document:
            document.new_page().insert_text((50, 70), "SYNTHETISCHE MONTAGEANLEITUNG")
            attachment = FileStorage(stream=BytesIO(document.tobytes()), filename="Werkstattauftrag.pdf")
        saved, _ = portal.save_uploads(self.order, [attachment], "autohaus", analyze=False)
        self.assertEqual(saved, 1)
        self.listing()
        path = f"/admin/auftrag/{self.order}/ausdrucken"
        with patch.object(werkstatt_druckpaket, "lade_druckquelle", return_value=None):
            incomplete = self.client.post(path, data=self.csrf())
        self.assert_order_pdf(incomplete, self.order)
        with fitz.open(stream=incomplete.data, filetype="pdf") as pdf:
            self.assertIn("Fehlende Druckseiten", pdf[0].get_text())
        self.assertIn("data-reifen-neu", self.card(self.listing(), self.order))
        complete = self.client.post(path, data=self.csrf())
        self.assert_order_pdf(complete, self.order)
        with fitz.open(stream=complete.data, filetype="pdf") as pdf:
            self.assertIn("SYNTHETISCHE MONTAGEANLEITUNG", pdf[-1].get_text())
        self.assertTrue(self.printed_at(self.order))

    def test_simultaneous_print_requests_write_the_first_receipt_once(self):
        first_pdf_ready = Event()
        both_pdfs_ready = Barrier(2, timeout=15)
        record_print = werkstatt_druckpaket._record_tyre_print

        def synchronized_record(*args):
            first_pdf_ready.set()
            both_pdfs_ready.wait()
            return record_print(*args)

        # Fail either HTTP request if it attempts to write the already issued receipt.
        trigger = f"assert_first_print_only_{self.order}"
        with closing(portal.get_db()) as db:
            db.execute(f"""CREATE TRIGGER {trigger} BEFORE UPDATE OF reifen_auftrag_gedruckt_am ON auftraege
                WHEN OLD.id={self.order} AND length(OLD.reifen_auftrag_gedruckt_am)>0
                BEGIN SELECT RAISE(ABORT, 'duplicate receipt'); END""")

        def remove_trigger():
            with closing(portal.get_db()) as db:
                db.execute(f"DROP TRIGGER IF EXISTS {trigger}")

        self.addCleanup(remove_trigger)
        self.listing()
        second_client = self.login()
        self.listing(second_client)
        with second_client.session_transaction() as session:
            second_token = {portal.CSRF_FIELD_NAME: session[portal.CSRF_FIELD_NAME]}
        path = f"/admin/auftrag/{self.order}/ausdrucken"
        with patch.object(werkstatt_druckpaket, "_record_tyre_print", synchronized_record), ThreadPoolExecutor(2) as workers:
            first = workers.submit(self.client.post, path, data=self.csrf())
            # Start the second PDF only when the first has closed its PDF objects.
            self.assertTrue(first_pdf_ready.wait(timeout=15))
            second = workers.submit(second_client.post, path, data=second_token)
            responses = [first.result(timeout=20), second.result(timeout=20)]
        for response in responses:
            self.assert_order_pdf(response, self.order)
        self.assertTrue(self.printed_at(self.order))
        self.assertFalse(self.printed_at(self.other))

    def test_new_order_remains_unprinted_after_completion_and_leaves_queue_on_archive(self):
        self.assertEqual(self.printed_at(self.order), "")
        self.assertIn("data-reifen-neu", self.card(self.listing(), self.order))
        response = self.client.post(f"/admin/status/{self.order}/4", data=self.csrf())
        self.assertEqual(response.status_code, 302)
        self.assertEqual(portal.get_auftrag(self.order)["status"], 4)
        self.assertFalse(self.printed_at(self.order))
        self.assertIn("data-reifen-neu", self.card(self.listing(), self.order))
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET archiviert=1 WHERE id=?", (self.order,))
            db.commit()
        self.assertNotIn(f'data-reifenauftrag="{self.order}"', self.listing())

    def test_one_time_migration_leaves_completed_legacy_orders_out_of_new_queue(self):
        with closing(sqlite3.connect(":memory:")) as db:
            db.row_factory = sqlite3.Row
            db.executescript("""
                CREATE TABLE app_settings(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT);
                CREATE TABLE auftraege(id INTEGER PRIMARY KEY,leistungsart TEXT,schaden_aufnahme_json TEXT,
                                       status INTEGER,archiviert INTEGER);
                INSERT INTO auftraege VALUES(1,'reifenservice','{}',4,0),(2,'reifenservice','{}',2,1),
                                            (3,'reifenservice','{}',2,0);
            """)
            portal.ensure_reifen_auftragsdruck(db)
            self.assertEqual([row[0] for row in db.execute(
                "SELECT reifen_auftrag_gedruckt_am FROM auftraege ORDER BY id")], [None, None, ""])
            db.execute("UPDATE auftraege SET status=4 WHERE id=3")
            portal.ensure_reifen_auftragsdruck(db)
            self.assertEqual(db.execute("SELECT reifen_auftrag_gedruckt_am FROM auftraege WHERE id=3").fetchone()[0], "")


if __name__ == "__main__":
    unittest.main()
