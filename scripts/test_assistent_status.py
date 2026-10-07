"""Native status previews/confirmation on synthetic SQLite; no app or network."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import pathlib
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from werkstatt_fortschritt import NATIVE_ACTION_LABELS, ProgressError, WorkshopProgress


class FixedClock(datetime):
    instant = datetime(2026, 9, 28, 23, 30, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls.instant.astimezone(tz) if tz else cls.instant.replace(tzinfo=None)


class NativeStatusTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.temp.name) / "synthetic.db")
        self.clock = patch("werkstatt_fortschritt.datetime", FixedClock)
        FixedClock.instant = datetime(2026, 9, 28, 23, 30, tzinfo=timezone.utc)
        self.clock.start()

        def get_db():
            db = sqlite3.connect(self.path, timeout=10)
            db.row_factory = sqlite3.Row
            return db

        def ensure_column(db, table, column, definition):
            columns = [row["name"] for row in db.execute(f"PRAGMA table_info({table})")]
            if column not in columns:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

        self.portal = SimpleNamespace(get_db=get_db, ensure_column=ensure_column, USE_POSTGRES=False,
                                      fuehre_auftrag_status_wechsel_aus=Mock(side_effect=AssertionError("external helper")),
                                      sende_autohaus_benachrichtigung_mail=Mock(side_effect=AssertionError("mail")))
        db = get_db()
        db.executescript("""CREATE TABLE auftraege (
            id INTEGER PRIMARY KEY, fahrzeug TEXT, kennzeichen TEXT, status INTEGER,
            archiviert INTEGER DEFAULT 0, produktion_schritt TEXT DEFAULT '', geaendert_am TEXT,
            start_datum TEXT DEFAULT '', fertig_datum TEXT DEFAULT '',
            fahrzeug_abholbereit INTEGER DEFAULT 0, fahrzeug_abholbereit_am TEXT DEFAULT '',
            abholtermin TEXT DEFAULT '', preis_netto TEXT DEFAULT 'private-financial-value');
            CREATE TABLE status_log (id INTEGER PRIMARY KEY, auftrag_id INTEGER, status INTEGER, zeitstempel TEXT);
            INSERT INTO auftraege (id,fahrzeug,kennzeichen,status,produktion_schritt,geaendert_am,abholtermin)
            VALUES(156,'Testfahrzeug','TEST-156',3,'vorarbeit','28.09.2026 09:30','30.09.2026');
        """)
        db.commit()
        db.close()
        self.service = WorkshopProgress(self.portal)
        self.who = {"actor": "mitarbeiter:7", "lesen": 1, "dokumentieren": 1}

    def tearDown(self):
        self.clock.stop()
        self.temp.cleanup()

    def execute(self, sql, args=()):
        db = self.portal.get_db()
        try:
            rows = db.execute(sql, args).fetchall()
            db.commit()
            return [dict(row) for row in rows]
        finally:
            db.close()

    def preview(self, action="lackierbereit"):
        return self.service.preview(156, action, self.who)

    def confirm(self, action="lackierbereit", key="assistant:one", preview=None):
        return self.service.confirm(preview or self.preview(action), self.who, key)

    def count(self, table):
        return self.execute(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]

    def test_preview_is_deterministic_read_only_and_contains_no_financial_fields(self):
        before = self.execute("SELECT * FROM auftraege")
        preview = self.preview()
        self.assertEqual(preview, json.loads(json.dumps(self.preview())))
        self.assertEqual(preview["aktion_label"], "Lackierbereit")
        self.assertIn("nicht fertiggemeldet", preview["zusammenfassung"])
        self.assertIn("Kein E-Mail- oder WhatsApp", preview["zusammenfassung"])
        self.assertNotIn("private-financial-value", json.dumps(preview))
        self.assertNotIn("preis_netto", json.dumps(preview))
        self.assertEqual(before, self.execute("SELECT * FROM auftraege"))
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)

    def test_native_workflow_preserves_transport_and_logs_only_overall_status_changes(self):
        self.execute("UPDATE auftraege SET status=2,fahrzeug_abholbereit=1,fahrzeug_abholbereit_am='previous'")
        start = self.preview("in_arbeit_starten")
        self.assertEqual(start["werkstatttag"], "29.09.2026")
        self.assertIn("29.09.2026", start["zusammenfassung"])
        self.confirm(preview=start)
        for action, key in (("lackierbereit", "ready"), ("lackierung_starten", "paint"), ("finish_starten", "finish")):
            result = self.confirm(action, key)
            self.assertEqual(result["fortschritt"]["status"], 3)
            self.assertFalse(result["fortschritt"]["fahrzeug_fertig"])
        result = self.confirm("fertig_melden", "done")
        self.assertEqual(result["fortschritt"]["status"], 4)
        self.assertTrue(result["fortschritt"]["fahrzeug_fertig"])
        self.assertFalse(result["fortschritt"]["lackierbereit"])
        self.assertIn("Fahrzeug fertig gespeichert", result["hinweis"])
        row = self.execute("SELECT * FROM auftraege")[0]
        self.assertEqual(row["start_datum"], "29.09.2026")
        self.assertEqual(row["fertig_datum"], "29.09.2026")
        self.assertEqual(row["abholtermin"], "30.09.2026")
        self.assertEqual(row["fahrzeug_abholbereit"], 0)
        self.assertEqual(row["fahrzeug_abholbereit_am"], "")
        self.assertEqual(row["preis_netto"], "private-financial-value")
        self.assertEqual([r["status"] for r in self.execute("SELECT * FROM status_log ORDER BY id")], [3, 4])
        self.assertEqual(self.count("assistent_fortschritt_audit"), 5)
        self.portal.fuehre_auftrag_status_wechsel_aus.assert_not_called()
        self.portal.sende_autohaus_benachrichtigung_mail.assert_not_called()

    def test_fertig_preserves_known_dates_and_never_marks_returned(self):
        self.execute("UPDATE auftraege SET start_datum='25.09.2026',fertig_datum='30.09.2026'")
        preview = self.preview("fertig_melden")
        self.assertIn("nicht als zurückgegeben", preview["zusammenfassung"])
        self.assertNotIn("Fehlendes Fertigdatum", preview["zusammenfassung"])
        self.confirm(preview=preview)
        row = self.execute("SELECT * FROM auftraege")[0]
        self.assertEqual((row["status"], row["start_datum"], row["fertig_datum"]), (4, "25.09.2026", "30.09.2026"))

    def test_lackierbereit_is_not_lackierung_started_or_vehicle_finished(self):
        result = self.confirm()["fortschritt"]
        self.assertTrue(result["lackierbereit"])
        self.assertEqual(result["produktion_schritt"], "vorarbeit")
        self.assertEqual(result["status"], 3)
        self.assertFalse(result["fahrzeug_fertig"])
        self.assertEqual(self.count("status_log"), 0)

    def test_vorarbeit_and_karosserie_are_internal_idempotent_steps_requiring_work(self):
        for action, stage in (("vorarbeit_starten", "vorarbeit"), ("karosserie_starten", "karosserie")):
            result = self.confirm(action, "first:" + action)
            self.assertEqual(result["fortschritt"]["produktion_schritt"], stage)
            self.assertEqual(result["fortschritt"]["status"], 3)
            self.assertTrue(self.confirm(action, "second:" + action)["unveraendert"])
            self.assertEqual(self.count("status_log"), 0)
        self.execute("UPDATE auftraege SET status=2")
        for action in ("vorarbeit_starten", "karosserie_starten"):
            with self.assertRaises(ProgressError):
                self.preview(action)

    def test_repeated_commands_do_not_toggle_or_rewrite_state(self):
        for action in ("in_arbeit_starten", "lackierbereit", "lackierung_starten", "finish_starten", "fertig_melden"):
            with self.subTest(action=action):
                self.confirm(action, "first:" + action)
                before = self.execute("SELECT * FROM auftraege")
                logs = self.count("status_log")
                preview = self.preview(action)
                self.assertTrue(preview["unveraendert"])
                result = self.confirm(action, "second:" + action, preview)
                self.assertTrue(result["unveraendert"])
                self.assertEqual(before, self.execute("SELECT * FROM auftraege"))
                self.assertEqual(logs, self.count("status_log"))

    def test_status4_cannot_reactivate_and_status5_never_changes(self):
        for status in (1, 4, 5):
            self.execute("UPDATE auftraege SET status=?", (status,))
            for action in NATIVE_ACTION_LABELS:
                if status == 4 and action == "fertig_melden":
                    continue
                with self.subTest(status=status, action=action), self.assertRaises(ProgressError) as error:
                    self.preview(action)
                self.assertEqual(error.exception.code, "status_not_allowed")
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)

    def test_scheduled_order_requires_start_before_paint_finish_or_complete(self):
        self.execute("UPDATE auftraege SET status=2")
        self.assertEqual(self.preview()["expected_status"], 2)
        for action in ("lackierung_starten", "finish_starten", "fertig_melden"):
            with self.subTest(action=action), self.assertRaises(ProgressError) as error:
                self.preview(action)
            self.assertEqual(error.exception.code, "status_not_allowed")

    def test_archive_before_preview_or_after_preview_is_rejected(self):
        preview = self.preview()
        self.execute("UPDATE auftraege SET archiviert=1")
        for operation in (self.preview, lambda: self.confirm(preview=preview)):
            with self.assertRaises(ProgressError) as error:
                operation()
            self.assertEqual(error.exception.code, "archived")
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)

    def test_permissions_and_actor_binding_checked_before_writes_and_replay(self):
        preview = self.preview()
        self.confirm(preview=preview)
        for who in (None, {}, dict(self.who, dokumentieren=0), dict(self.who, dokumentieren="1")):
            for operation in (lambda: self.service.preview(156, "lackierbereit", who),
                              lambda: self.service.confirm(preview, who, "assistant:one")):
                with self.subTest(who=who), self.assertRaises(ProgressError) as error:
                    operation()
                self.assertEqual(error.exception.code, "permission_denied")
        with self.assertRaises(ProgressError) as error:
            self.service.confirm(preview, dict(self.who, actor="mitarbeiter:8"), "other")
        self.assertEqual(error.exception.code, "permission_denied")
        self.assertEqual(self.count("assistent_fortschritt_audit"), 1)

    def test_stale_snapshot_catches_legacy_same_minute_writes(self):
        cases = (("produktion_schritt", "karosserie"), ("lackierbereit", 1),
                 ("fertig_datum", "01.10.2026"), ("kennzeichen", "TEST-CHANGED"))
        for field, value in cases:
            preview = self.preview()
            self.execute(f"UPDATE auftraege SET {field}=?", (value,))
            with self.subTest(field=field), self.assertRaises(ProgressError) as error:
                self.confirm(preview=preview)
            self.assertEqual(error.exception.code, "stale_state")
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)

    def test_new_workshop_day_requires_new_preview(self):
        preview = self.preview("fertig_melden")
        FixedClock.instant = datetime(2026, 9, 29, 22, 1, tzinfo=timezone.utc)
        with self.assertRaises(ProgressError) as error:
            self.confirm(preview=preview)
        self.assertEqual(error.exception.code, "stale_state")
        self.assertEqual(self.count("status_log"), 0)

    def test_replay_returns_saved_result_without_reverting_later_state(self):
        preview = self.preview()
        first = self.confirm(preview=preview)
        later = self.confirm("lackierung_starten", "later")["fortschritt"]
        replay = self.confirm(preview=preview)
        self.assertTrue(replay["wiederholt"])
        self.assertEqual(replay["fortschritt"], first["fortschritt"])
        self.assertEqual(self.service.read(156), later)
        self.assertEqual(self.count("assistent_fortschritt_audit"), 2)

    def test_reused_request_id_with_new_payload_is_conflict(self):
        self.confirm()
        with self.assertRaises(ProgressError) as error:
            self.confirm("lackierung_starten")
        self.assertEqual(error.exception.code, "request_conflict")

    def test_audit_or_log_failure_rolls_back_order_and_reservation(self):
        for target, timing in (("assistent_fortschritt_audit", "UPDATE OF result_json"), ("status_log", "INSERT")):
            before = self.execute("SELECT * FROM auftraege")
            self.execute(f"CREATE TRIGGER reject_write BEFORE {timing} ON {target} BEGIN SELECT RAISE(ABORT,'synthetic failure'); END")
            with self.subTest(target=target), self.assertRaises(sqlite3.IntegrityError):
                self.confirm("fertig_melden")
            self.assertEqual(before, self.execute("SELECT * FROM auftraege"))
            self.assertEqual(self.count("status_log"), 0)
            self.assertEqual(self.count("assistent_fortschritt_audit"), 0)
            self.execute("DROP TRIGGER reject_write")

    def test_concurrent_confirmations_replay_one_write(self):
        preview = self.preview("fertig_melden")
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.confirm(preview=preview), range(2)))
        self.assertEqual(sorted(r["wiederholt"] for r in results), [False, True])
        self.assertEqual(self.count("status_log"), 1)
        self.assertEqual(self.count("assistent_fortschritt_audit"), 1)

    def test_concurrent_distinct_previews_only_one_applies(self):
        previews = [self.preview("lackierung_starten"), self.preview("finish_starten")]

        def submit(pair):
            key, preview = pair
            try:
                return self.confirm(key=str(key), preview=preview)
            except ProgressError as error:
                return error.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(submit, enumerate(previews)))
        self.assertEqual(sum(isinstance(r, dict) for r in results), 1)
        self.assertIn("stale_state", results)
        self.assertEqual(self.count("assistent_fortschritt_audit"), 1)

    def test_invalid_actions_ids_and_malformed_previews_fail_without_writes(self):
        for order in (True, 156.0, "156", 0, [], None):
            with self.subTest(order=order), self.assertRaises(ProgressError):
                self.service.preview(order, "lackierbereit", self.who)
        for action in ("fertig", "zurueckgegeben", "archivieren", "loeschen", "rechnung", {}, None):
            with self.subTest(action=action), self.assertRaises(ProgressError):
                self.preview(action)
        preview = self.preview()
        for malformed in (None, {}, dict(preview, version=True), dict(preview, expected_snapshot="bad"),
                          dict(preview, expected_status=True), dict(preview, auftrag_id="156")):
            with self.subTest(preview=malformed), self.assertRaises(ProgressError):
                self.service.confirm(malformed, self.who, "invalid")
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)

    def test_existing_bearer_update_does_not_gain_native_status_actions(self):
        for action in ("in_arbeit_starten", "fertig_melden"):
            with self.subTest(action=action), self.assertRaises(ProgressError) as error:
                self.service.update(156, action, 3, "28.09.2026 09:30", "avatar:existing-grant", "old-api")
            self.assertEqual(error.exception.code, "invalid_action")
        self.assertEqual(self.count("status_log"), 0)

    def test_missing_order_or_missing_timestamp_fail_closed(self):
        with self.assertRaises(ProgressError) as error:
            self.service.preview(999, "lackierbereit", self.who)
        self.assertEqual(error.exception.status_code, 404)
        self.execute("UPDATE auftraege SET geaendert_am=NULL")
        with self.assertRaises(ProgressError) as error:
            self.preview()
        self.assertEqual(error.exception.code, "invalid_request")


if __name__ == "__main__":
    unittest.main()
