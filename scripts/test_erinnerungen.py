"""Synthetic reminder tests: temporary DB, no application import or network."""
from contextlib import closing, contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from threading import RLock
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4
import sqlite3
import sys
import tempfile
import unittest

from flask import Flask

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_kundenflotten import Kundenflotten
from werkstatt_erinnerungen import BACKUP_TABLES, SCHEMA_FEATURE, ReminderService, StaleReminder, ensure_schema


class ReminderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "synthetic.db"
        self.lock = RLock()
        self.lock_depth = 0
        self.app = Flask(__name__)
        self.app.config.update(TESTING=True, SERVICE_APP_TEST_TRANSPORT_ENABLED=True)
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
            guard = patch(target, side_effect=AssertionError("Network is forbidden in reminder tests"))
            guard.start()
            self.addCleanup(guard.stop)

        def get_db():
            db = sqlite3.connect(self.path)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            return db

        @contextmanager
        def operation_lock():
            with self.lock:
                self.lock_depth += 1
                try:
                    yield
                finally:
                    self.lock_depth -= 1

        self.p = SimpleNamespace(app=self.app, get_db=get_db, portal_originals_operation_lock=operation_lock)
        self.fleet = Kundenflotten(self.p)
        self.fleet.init_schema()
        with closing(get_db()) as db:
            ensure_schema(db)
            ensure_schema(db)
            db.commit()
        self.company = self.fleet.create_company({"name": "Synthetic fleet", "email": "private-company@example.invalid"})
        self.other = self.fleet.create_company({"name": "Other synthetic fleet"})
        self.today = date.today()
        self.vehicle = self.fleet.create_vehicle(self.company, {"kennzeichen": "MOS TEST 1", "modell": "Synthetic car", "km": "10000", "km_datum": self.today.isoformat()})
        self.other_vehicle = self.fleet.create_vehicle(self.other, {"kennzeichen": "MOS TEST 2", "modell": "Other car", "km": "10000", "km_datum": self.today.isoformat()})
        self.driver()
        self.service = ReminderService(self.p, self.fleet)

    def sql(self, sql, args=()):
        with closing(self.p.get_db()) as db:
            rows = db.execute(sql, args).fetchall()
            db.commit()
            return rows

    def driver(self, **changes):
        data = {"name": "Synthetic driver", "kanal": "email", "email": "synthetic-driver@example.invalid", "einwilligung": "Synthetic email reminder approval"}
        data.update(changes)
        self.fleet.change_vehicle(self.company, self.vehicle, "fahrer", data)

    def plan(self, **changes):
        data = {"id": uuid4().hex, "fahrzeug_id": self.vehicle, "art": "service", "titel": "Synthetic service",
                "faellig_datum": self.today.isoformat(), "faellig_km": None, "intervall_monate": None,
                "intervall_km": None, "quelle": "Recorded synthetic maintenance plan", "status": "offen", "version": 1, "erstellt_am": self.today.isoformat()}
        data.update(changes)
        columns = list(data)
        self.sql("INSERT INTO kundenflotten_plaene (" + ",".join(columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")", tuple(data[key] for key in columns))
        return "plan:" + data["id"]

    def appointment(self, **changes):
        data = {"id": uuid4().hex, "fahrzeug_id": self.vehicle, "datum": self.today.isoformat(), "art": "reifen",
                "transport": "offen", "ersatzwagen": 0, "status": "bestaetigt", "notiz": "INTERNAL-NOT-PUBLIC", "erstellt_am": self.today.isoformat()}
        data.update(changes)
        keys = list(data)
        self.sql("INSERT INTO kundenflotten_termine (" + ",".join(keys) + ") VALUES (" + ",".join("?" for _ in keys) + ")", tuple(data[key] for key in keys))
        return "termin:" + data["id"]

    def preview(self, item):
        return self.service.preview(self.company, self.vehicle, item, today=self.today)

    def record(self, item, preview=None, **changes):
        data = {"snapshot": (preview or self.preview(item))["snapshot"], "request_id": uuid4().hex, "today": self.today}
        data.update(changes)
        return self.service.record_test(self.company, self.vehicle, item, **data)

    def count(self):
        return self.sql("SELECT COUNT(*) AS n FROM kundenflotten_erinnerung_tests")[0]["n"]

    def test_date_or_mileage_existing_rules_and_no_invented_interval(self):
        overdue = self.plan(faellig_datum=(self.today - timedelta(days=1)).isoformat())
        mileage = self.plan(faellig_datum="", faellig_km=10500)
        future = self.plan(faellig_datum=(self.today + timedelta(days=31)).isoformat(), faellig_km=12000)
        closed = self.plan(status="abgeschlossen")
        items = self.service.list_due(self.company, today=self.today)
        self.assertEqual([item["id"] for item in items], [overdue, mileage])
        self.assertNotIn(future, str(items))
        self.assertNotIn(closed, str(items))
        self.assertEqual(items[1]["due_km"], 10500)

    def test_only_explicit_confirmed_appointments_within_horizon(self):
        confirmed = self.appointment()
        request = self.appointment(status="angefragt")
        cancelled = self.appointment(status="storniert")
        future = self.appointment(datum=(self.today + timedelta(days=31)).isoformat())
        items = self.service.vehicle_inbox(self.company, self.vehicle, today=self.today)
        self.assertEqual([item["id"] for item in items], [confirmed])
        self.assertNotIn("INTERNAL-NOT-PUBLIC", str(items))
        self.assertTrue(all(item not in str(items) for item in (request, cancelled, future)))

    def test_reads_never_write_and_do_not_expose_contact_or_internal_source(self):
        item = self.plan(quelle="PRIVATE-SOURCE")
        with closing(self.p.get_db()) as db:
            before = list(db.iterdump())
        for _ in range(2):
            public = self.service.list_due(self.company, today=self.today)
            public += self.service.vehicle_inbox(self.company, self.vehicle, today=self.today)
            public += [self.preview(item)]
            self.assertNotIn("synthetic-driver@example.invalid", str(public))
            self.assertNotIn("private-company@example.invalid", str(public))
            self.assertNotIn("PRIVATE-SOURCE", str(public))
        with closing(self.p.get_db()) as db:
            self.assertEqual(list(db.iterdump()), before)

    def test_company_vehicle_and_item_scope(self):
        item = self.plan()
        other_item = self.plan(fahrzeug_id=self.other_vehicle)
        for call in (lambda: self.service.vehicle_inbox(self.other, self.vehicle),
                     lambda: self.service.preview(self.other, self.vehicle, item),
                     lambda: self.service.preview(self.company, self.vehicle, other_item)):
            with self.assertRaises((LookupError, StaleReminder)):
                call()
        self.assertEqual(self.service.list_due(self.other, today=self.today)[0]["id"], other_item)

    def test_local_receipt_is_explicitly_not_sent_and_idempotent(self):
        item = self.plan()
        request_id = uuid4().hex
        first = self.record(item, request_id=request_id)
        self.assertEqual(first, self.record(item, request_id=request_id))
        self.assertEqual(self.count(), 1)
        self.assertEqual(first["status"], "lokal_getestet")
        self.assertEqual(first["transport"], "lokaler_test")
        self.assertFalse(first["sent"])
        self.assertEqual(first["recipient"], "test@example.invalid")

    def test_product_mode_and_missing_flag_disable_transport_even_for_invalid_domain(self):
        item = self.plan()
        for config in ({"TESTING": False}, {"SERVICE_APP_TEST_TRANSPORT_ENABLED": False}, {"TESTING": "true"}):
            with patch.dict(self.app.config, config), self.assertRaises(ValueError):
                self.record(item)
        self.assertEqual(self.count(), 0)

    def test_real_malformed_and_multiple_recipients_rejected(self):
        item = self.plan()
        for recipient in ("driver@example.com", "test@example.invalid.attacker.test", "test@example.invalid\r\nBcc:x@y.de", "test@example.invalid,another@example.invalid", "invalid", None):
            with self.subTest(recipient=recipient), self.assertRaises(ValueError):
                self.record(item, recipient=recipient)
        self.assertEqual(self.count(), 0)

    def test_driver_change_and_consent_revocation_invalidate_old_preview(self):
        item = self.plan()
        for changes in ({"name": "New synthetic driver"}, {"kanal": "keine", "einwilligung": ""}):
            previous = self.preview(item)
            self.driver(**changes)
            with self.assertRaises(StaleReminder):
                self.record(item, previous)
        self.assertEqual(self.count(), 0)

    def test_current_unapproved_or_manual_contact_cannot_be_tested(self):
        item = self.plan()
        for changes in ({"kanal": "keine", "einwilligung": ""}, {"kanal": "telefon", "telefon": "0171 1234567"}):
            self.driver(**changes)
            self.assertFalse(self.preview(item)["contact_ready"])
            with self.assertRaises(ValueError):
                self.record(item)
        self.assertEqual(self.count(), 0)

    def test_channel_change_invalidates_snapshot_and_whatsapp_stays_local(self):
        item = self.plan()
        previous = self.preview(item)
        self.driver(kanal="whatsapp", telefon="0171 1234567", einwilligung="Synthetic WhatsApp reminder approval")
        with self.assertRaises(StaleReminder):
            self.record(item, previous)
        current = self.preview(item)
        self.assertEqual(current["channel"], "whatsapp")
        self.assertEqual(current["transport"], "deaktiviert")
        self.assertEqual(self.record(item, current)["recipient"], "test@example.invalid")

    def test_kilometre_update_invalidates_preview_even_if_plan_version_unchanged(self):
        item = self.plan(faellig_datum="", faellig_km=10500)
        previous = self.preview(item)
        self.fleet.change_vehicle(self.company, self.vehicle, "km", {"km": "10100", "km_datum": self.today.isoformat()})
        with self.assertRaises(StaleReminder):
            self.record(item, previous)
        self.assertEqual(self.count(), 0)

    def test_plan_version_due_change_completion_and_cancellation_reject_stale_preview(self):
        item = self.plan()
        previous = self.preview(item)
        self.sql("UPDATE kundenflotten_plaene SET version=version+1 WHERE id=?", (item[5:],))
        with self.assertRaises(StaleReminder):
            self.record(item, previous)
        previous = self.preview(item)
        self.sql("UPDATE kundenflotten_plaene SET status='abgeschlossen' WHERE id=?", (item[5:],))
        with self.assertRaises(StaleReminder):
            self.record(item, previous)
        appointment = self.appointment()
        previous = self.preview(appointment)
        self.sql("UPDATE kundenflotten_termine SET status='storniert' WHERE id=?", (appointment[7:],))
        with self.assertRaises(StaleReminder):
            self.record(appointment, previous)
        self.assertEqual(self.count(), 0)

    def test_assignment_revision_changes_even_with_same_contact_and_inactive_blocks(self):
        item = self.plan()
        current = {"id": uuid4().hex, "version": 1, "active": True}
        def assignment(db, company, vehicle):
            self.assertGreater(self.lock_depth, 0)
            return dict(current)
        self.service = ReminderService(self.p, self.fleet, assignment_context=assignment)
        previous = self.preview(item)
        current["version"] += 1
        with self.assertRaises(StaleReminder):
            self.record(item, previous)
        current["active"] = False
        with self.assertRaises(ValueError):
            self.record(item)
        self.assertEqual(self.count(), 0)

    def test_assignment_hook_without_explicit_active_assignment_blocks_contact(self):
        item = self.plan()
        for state in (None, {}, {"active": "true"}, {"active": False}):
            self.service = ReminderService(self.p, self.fleet, assignment_context=lambda *args: state)
            self.assertFalse(self.preview(item)["contact_ready"])
            with self.assertRaises(ValueError):
                self.record(item)
        self.assertEqual(self.count(), 0)

    def test_parallel_replays_record_exactly_one_test_receipt(self):
        item = self.plan()
        previous = self.preview(item)
        key = uuid4().hex
        with ThreadPoolExecutor(max_workers=2) as pool:
            records = list(pool.map(lambda _: self.record(item, previous, request_id=key), range(2)))
        self.assertEqual(records[0], records[1])
        self.assertEqual(self.count(), 1)

    def test_restore_lock_and_current_authorization_precede_source_read_and_write(self):
        item = self.plan()
        events = []
        def authorized():
            self.assertGreater(self.lock_depth, 0)
            events.append("checked")
        self.record(item, authorize=authorized)
        self.assertEqual(events, ["checked"])
        def revoked():
            raise PermissionError("Current assignment revoked")
        with self.assertRaises(PermissionError):
            self.record(item, authorize=revoked)
        self.assertEqual(self.count(), 1)

    def test_receipt_key_cannot_be_reused_for_different_item_or_test_recipient(self):
        first, second = self.plan(), self.plan()
        key = uuid4().hex
        self.record(first, request_id=key)
        for item, recipient in ((second, "test@example.invalid"), (first, "another@example.invalid")):
            with self.assertRaises(ValueError):
                self.record(item, request_id=key, recipient=recipient)
        self.assertEqual(self.count(), 1)

    def test_schema_metadata_and_invalid_client_tokens(self):
        self.assertEqual(BACKUP_TABLES, ("kundenflotten_erinnerung_tests",))
        self.assertEqual(SCHEMA_FEATURE, "kundenflotten_erinnerungen_v1")
        item = self.plan()
        for changes in ({"snapshot": "bad"}, {"request_id": "bad"}, {"snapshot": "0" * 64}):
            with self.assertRaises(ValueError):
                self.record(item, **changes)
        self.assertEqual(self.count(), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
