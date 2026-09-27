"""Run with python scripts/test_fortschritt.py; isolated SQLite, no app/network."""

from concurrent.futures import ThreadPoolExecutor
import pathlib
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from werkstatt_fortschritt import ProgressError, WorkshopProgress, init_schema


class ProgressTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.temp.name) / "test.db")
        def get_db():
            db = sqlite3.connect(self.path, timeout=10)
            db.row_factory = sqlite3.Row
            return db
        def ensure_column(db, table, column, definition):
            names = [row["name"] for row in db.execute(f"PRAGMA table_info({table})")]
            if column not in names:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        self.portal = SimpleNamespace(get_db=get_db, ensure_column=ensure_column, USE_POSTGRES=False)
        db = get_db()
        db.executescript("""CREATE TABLE auftraege (
            id INTEGER PRIMARY KEY, fahrzeug TEXT, kennzeichen TEXT,
            status INTEGER, archiviert INTEGER DEFAULT 0,
            produktion_schritt TEXT DEFAULT '', geaendert_am TEXT NOT NULL);
            CREATE TABLE status_log (id INTEGER PRIMARY KEY, auftrag_id INTEGER, status INTEGER, zeitstempel TEXT);
            INSERT INTO auftraege VALUES(156,'Audi A4','TEST-1',3,0,'vorarbeit','28.09.2026 09:30');
        """)
        db.commit()
        db.close()
        self.service = WorkshopProgress(self.portal)

    def tearDown(self):
        self.temp.cleanup()

    def update(self, action="lackierbereit", request_id="request-1", expected=None, actor="employee:1"):
        state = expected or self.service.read(156)
        return self.service.update(156, action, state["status"], state["geaendert_am"], actor, request_id)

    def execute(self, sql, args=()):
        db = self.portal.get_db()
        try:
            rows = db.execute(sql, args).fetchall()
            db.commit()
            return rows
        finally:
            db.close()

    def test_schema_initialization_is_idempotent(self):
        init_schema(self.portal)
        self.assertFalse(self.service.read(156)["lackierbereit"])

    def test_ready_is_not_vehicle_finished_and_preserves_stage_and_status(self):
        previous = self.service.read(156)
        result = self.update()["fortschritt"]
        self.assertTrue(result["lackierbereit"])
        self.assertEqual(result["lackierbereit_label"], "Lackierbereit")
        self.assertFalse(result["fahrzeug_fertig"])
        self.assertEqual(result["status"], 3)
        self.assertEqual(result["produktion_schritt"], "vorarbeit")
        self.assertNotEqual(result["geaendert_am"], previous["geaendert_am"])
        self.assertTrue(result["lackierbereit_am"])
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM status_log")[0]["n"], 0)

    def test_phase_starts_clear_ready_and_keep_overall_status(self):
        self.update()
        paint = self.update("lackierung_starten", "request-2")["fortschritt"]
        self.assertEqual(paint["produktion_schritt"], "lackierung")
        self.assertFalse(paint["lackierbereit"])
        self.assertEqual(paint["lackierbereit_am"], "")
        finish = self.update("finish_starten", "request-3")["fortschritt"]
        self.assertEqual(finish["produktion_schritt"], "finish")
        self.assertEqual(finish["status"], 3)
        self.assertFalse(finish["fahrzeug_fertig"])

    def test_ready_preserves_unset_stage_and_audit_failure_rolls_back(self):
        self.execute("UPDATE auftraege SET produktion_schritt=NULL WHERE id=156")
        self.update()
        self.assertIsNone(self.execute("SELECT produktion_schritt FROM auftraege WHERE id=156")[0]["produktion_schritt"])
        previous = self.service.read(156)
        self.execute("""CREATE TRIGGER fail_audit BEFORE UPDATE OF result_json
                     ON assistent_fortschritt_audit BEGIN SELECT RAISE(ABORT,'test failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.update("lackierung_starten", "request-2")
        self.assertEqual(self.service.read(156), previous)
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM assistent_fortschritt_audit")[0]["n"], 1)

    def test_scheduled_allows_ready_but_not_phase_start(self):
        self.execute("UPDATE auftraege SET status=2 WHERE id=156")
        self.assertEqual(self.update()["fortschritt"]["status"], 2)
        for action in ("lackierung_starten", "finish_starten"):
            with self.subTest(action=action), self.assertRaises(ProgressError) as error:
                self.update(action, "phase")
            self.assertEqual(error.exception.code, "status_not_allowed")

    def test_stale_timestamp_and_status_rejected_without_audit(self):
        previous = self.service.read(156)
        self.execute("UPDATE auftraege SET geaendert_am='changed' WHERE id=156")
        with self.assertRaises(ProgressError) as error:
            self.update(expected=previous)
        self.assertEqual(error.exception.code, "stale_state")
        self.execute("UPDATE auftraege SET geaendert_am=?,status=2 WHERE id=156", (previous["geaendert_am"],))
        with self.assertRaises(ProgressError) as error:
            self.update(expected=previous)
        self.assertEqual(error.exception.code, "stale_state")
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM assistent_fortschritt_audit")[0]["n"], 0)

    def test_same_request_replays_without_second_write_even_after_later_action(self):
        previous = self.service.read(156)
        first = self.update(expected=previous)
        current = self.update("lackierung_starten", "request-2")["fortschritt"]
        replay = self.update(expected=previous)
        self.assertTrue(replay["wiederholt"])
        self.assertEqual(replay["fortschritt"], first["fortschritt"])
        self.assertEqual(self.service.read(156), current)
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM assistent_fortschritt_audit")[0]["n"], 2)

    def test_request_id_cannot_be_reused_with_changed_payload(self):
        previous = self.service.read(156)
        self.update(expected=previous)
        for action, expected in (("finish_starten", previous), ("lackierbereit", self.service.read(156))):
            with self.subTest(action=action), self.assertRaises(ProgressError) as error:
                self.update(action, expected=expected)
            self.assertEqual(error.exception.code, "request_conflict")

    def test_archive_and_ineligible_statuses_rejected(self):
        for status, archive in ((1, 0), (4, 0), (5, 0), (3, 1)):
            self.execute("UPDATE auftraege SET status=?,archiviert=? WHERE id=156", (status, archive))
            with self.subTest(status=status, archive=archive), self.assertRaises(ProgressError) as error:
                self.update()
            self.assertIn(error.exception.code, ("status_not_allowed", "archived"))
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM assistent_fortschritt_audit")[0]["n"], 0)

    def test_invalid_action_and_untrusted_request_fields_fail(self):
        for action in ("fertig", "zurueckgegeben", "", {}, None):
            with self.subTest(action=action), self.assertRaises(ProgressError) as error:
                self.update(action)
            self.assertEqual(error.exception.code, "invalid_action")
        with self.assertRaises(ProgressError):
            self.update(actor="")
        with self.assertRaises(ProgressError):
            self.update(request_id="request\n1")
        with self.assertRaises(ProgressError):
            self.service.read(True)

    def test_missing_order_is_404_and_rolls_back_reservation(self):
        with self.assertRaises(ProgressError) as error:
            self.service.update(999, "lackierbereit", 3, "old", "employee:1", "request-1")
        self.assertEqual(error.exception.status_code, 404)
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM assistent_fortschritt_audit")[0]["n"], 0)

    def test_concurrent_duplicate_only_writes_once(self):
        previous = self.service.read(156)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.update(expected=previous), range(2)))
        self.assertEqual(sorted(result["wiederholt"] for result in results), [False, True])
        self.assertEqual(results[0]["fortschritt"], results[1]["fortschritt"])
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM assistent_fortschritt_audit")[0]["n"], 1)

    def test_concurrent_different_requests_only_one_wins_cas(self):
        previous = self.service.read(156)
        def submit(key):
            try:
                return self.update(request_id=key, expected=previous)
            except ProgressError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(submit, ("one", "two")))
        self.assertEqual(sum(isinstance(result, dict) for result in results), 1)
        self.assertIn("stale_state", results)
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM assistent_fortschritt_audit")[0]["n"], 1)

    def test_actor_namespaces_are_separate(self):
        self.update()
        self.update("lackierung_starten", actor="employee:2")
        self.assertEqual(self.execute("SELECT COUNT(*) AS n FROM assistent_fortschritt_audit")[0]["n"], 2)


if __name__ == "__main__":
    unittest.main()
