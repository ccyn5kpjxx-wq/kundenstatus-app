"""Offline intake/paint/contact regressions; only temporary synthetic SQLite."""
from concurrent.futures import ThreadPoolExecutor
import ast
import json
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_assistent_auftrag import OrderActionError, WorkshopOrderActions


class OrderActionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.temp.name) / "orders.db")

        def get_db():
            db = sqlite3.connect(self.path, timeout=10)
            db.row_factory = sqlite3.Row
            return db

        def ensure_column(db, table, column, definition):
            if column not in [r["name"] for r in db.execute(f"PRAGMA table_info({table})")]:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

        self.portal = SimpleNamespace(get_db=get_db, ensure_column=ensure_column, USE_POSTGRES=False,
                                      create_auftrag=Mock(side_effect=AssertionError("non-atomic helper")),
                                      get_auftrag=Mock(side_effect=AssertionError("hydrating reader")))
        db = get_db()
        db.executescript("""CREATE TABLE auftraege (
            id INTEGER PRIMARY KEY AUTOINCREMENT, fahrzeug TEXT NOT NULL, kennzeichen TEXT DEFAULT '',
            fin_nummer TEXT DEFAULT '', hsn_nummer TEXT DEFAULT '', tsn_nummer TEXT DEFAULT '',
            kunde_name TEXT DEFAULT '', kunde_email TEXT DEFAULT '', kontakt_telefon TEXT DEFAULT '',
            beschreibung TEXT DEFAULT '', farbcode TEXT DEFAULT '', farbton TEXT DEFAULT '', farbton_2 TEXT DEFAULT '',
            status INTEGER DEFAULT 1, archiviert INTEGER DEFAULT 0, geaendert_am TEXT, erstellt_am TEXT,
            quelle TEXT DEFAULT '', werkstatt_neu INTEGER DEFAULT 0,
            token TEXT DEFAULT '', kunden_status_token TEXT DEFAULT '', kunden_status_aktiv INTEGER DEFAULT 1,
            analyse_pruefen INTEGER DEFAULT 0, analyse_hinweis TEXT DEFAULT '', notiz_intern TEXT DEFAULT '',
            transport_art TEXT DEFAULT 'standard', angebotsphase INTEGER DEFAULT 0,
            angebot_abgesendet INTEGER DEFAULT 0, angebot_status TEXT DEFAULT 'entwurf',
            annahme_datum TEXT DEFAULT '', fertig_datum TEXT DEFAULT '', abholtermin TEXT DEFAULT '',
            auftragsnummer TEXT DEFAULT '', preis_netto TEXT DEFAULT 'untouched');
            CREATE TABLE status_log (id INTEGER PRIMARY KEY, auftrag_id INTEGER, status INTEGER, zeitstempel TEXT);
            CREATE TABLE source_links (upload_id TEXT PRIMARY KEY, auftrag_id INTEGER);
            INSERT INTO auftraege(id,fahrzeug,kennzeichen,status,geaendert_am,auftragsnummer)
                VALUES(156,'Testfahrzeug','TEST-1',3,'29.09.2026 10:00','EXTERN-77');
        """)
        db.commit()
        db.close()
        self.service = WorkshopOrderActions(self.portal)
        self.who = {"actor": "mitarbeiter:1", "dokumentieren": 1, "lesen": 1}
        self.fields = {"kunde_name": "Testkundschaft", "fahrzeug": "Testmodell", "kennzeichen": "TEST N-17"}

    def tearDown(self):
        self.temp.cleanup()

    def execute(self, sql, args=()):
        db = self.portal.get_db()
        try:
            rows = db.execute(sql, args).fetchall()
            db.commit()
            return [dict(row) for row in rows]
        finally:
            db.close()

    def count(self, table):
        return self.execute(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]

    def create_preview(self, **kwargs):
        return self.service.preview_create(dict(self.fields, **kwargs), self.who)

    def confirm(self, preview, key="assistant:one", **kwargs):
        return self.service.confirm(preview, self.who, key, **kwargs)

    def test_color_preview_and_confirmation_preserve_exact_code_and_other_fields(self):
        preview = self.service.preview_color(156, {"farbcode": "007/Z9", "farbton": "Blau Metallic"}, self.who)
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)
        self.assertIn("nicht hinterlegt auf 007/Z9", preview["zusammenfassung"])
        self.assertEqual(json.loads(json.dumps(preview)), preview)
        result = self.confirm(preview)
        self.assertEqual(result["auftrag_id"], 156)
        row = self.execute("SELECT * FROM auftraege WHERE id=156")[0]
        self.assertEqual((row["farbcode"], row["farbton"]), ("007/Z9", "Blau Metallic"))
        self.assertEqual((row["status"], row["auftragsnummer"], row["preis_netto"]), (3, "EXTERN-77", "untouched"))
        self.assertEqual(self.count("status_log"), 0)
        self.portal.get_auftrag.assert_not_called()

    def test_color_overwrite_is_explicit_and_same_value_is_noop(self):
        self.execute("UPDATE auftraege SET farbcode='A001'")
        preview = self.service.preview_color(156, {"farbcode": "A002"}, self.who)
        self.assertEqual(preview["aenderungen"], [{"feld": "farbcode", "vorher": "A001", "nachher": "A002"}])
        self.confirm(preview)
        before = self.execute("SELECT * FROM auftraege")
        noop = self.service.preview_color(156, {"farbcode": "A002"}, self.who)
        self.assertTrue(noop["unveraendert"])
        self.assertTrue(self.confirm(noop, "noop")["unveraendert"])
        self.assertEqual(before, self.execute("SELECT * FROM auftraege"))

    def test_contact_patch_is_explicit_and_does_not_enable_customer_portal(self):
        self.execute("UPDATE auftraege SET kunden_status_aktiv=0")
        preview = self.service.preview_contact(156, {"kunde_email": "Test@Example.Invalid", "kontakt_telefon": "+49 123 456789"}, self.who)
        self.assertEqual(preview["art"], "kontakt")
        self.assertIn("test@example.invalid", preview["zusammenfassung"])
        result = self.confirm(preview)
        self.assertEqual(result["auftrag"]["kunde_email"], "test@example.invalid")
        row = self.execute("SELECT * FROM auftraege WHERE id=156")[0]
        self.assertEqual((row["kunden_status_aktiv"], row["status"]), (0, 3))
        self.assertEqual(self.count("status_log"), 0)

    def test_stale_color_and_contact_in_same_minute_rejected(self):
        for kind, field, before, after in (("color", "farbcode", "001", "002"), ("contact", "kunde_email", "one@example.invalid", "two@example.invalid")):
            preview = getattr(self.service, "preview_" + kind)(156, {field: before}, self.who)
            self.execute(f"UPDATE auftraege SET {field}=? WHERE id=156", (after,))
            with self.subTest(kind=kind), self.assertRaises(OrderActionError) as error:
                self.confirm(preview)
            self.assertEqual(error.exception.code, "stale_state")
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)

    def test_nonexistent_or_archived_or_returned_order_is_not_created_by_update(self):
        with self.assertRaises(OrderActionError) as error:
            self.service.preview_color(402, {"farbcode": "007"}, self.who)
        self.assertEqual(error.exception.status_code, 404)
        for sql in ("UPDATE auftraege SET archiviert=1", "UPDATE auftraege SET archiviert=0,status=5"):
            self.execute(sql)
            with self.assertRaises(OrderActionError):
                self.service.preview_contact(156, {"kunde_name": "Test"}, self.who)
        self.assertEqual(self.count("auftraege"), 1)

    def test_intake_never_predicts_number_and_optional_contacts_remain_empty(self):
        preview = self.create_preview()
        self.assertIsNone(preview["auftrag_id"])
        self.assertIn("erst beim Speichern", preview["zusammenfassung"])
        self.assertTrue(any("Telefonnummer fehlt" in note for note in preview["hinweise"]))
        self.assertTrue(any("Halter" in note for note in preview["hinweise"]))
        self.assertEqual(self.count("auftraege"), 1)
        result = self.confirm(preview)
        self.assertEqual(result["auftrag_id"], 157)
        row = self.execute("SELECT * FROM auftraege WHERE id=?", (result["auftrag_id"],))[0]
        self.assertEqual((row["status"], row["werkstatt_neu"], row["kunden_status_aktiv"]), (1, 1, 0))
        for field in ("kontakt_telefon", "kunde_email", "token", "kunden_status_token", "auftragsnummer", "annahme_datum", "fertig_datum", "abholtermin", "transport_art", "angebot_status"):
            self.assertEqual(row[field], "", field)
        self.assertEqual(self.execute("SELECT status FROM status_log"), [{"status": 1}])
        self.portal.create_auftrag.assert_not_called()

    def test_all_supplied_fields_are_reviewable_and_preserved(self):
        preview = self.create_preview(kunde_email="Test@Example.Invalid", kontakt_telefon="01234 567890",
                                      fin_nummer="wvwzzz1jzxw000001", hsn_nummer="0603", tsn_nummer="abc",
                                      beschreibung="Stoßfänger prüfen.", farbcode="007", farbton="Grau", farbton_2="Schwarz")
        for key in ("kunde_email", "kontakt_telefon", "fin_nummer", "hsn_nummer", "tsn_nummer", "beschreibung", "farbcode", "farbton", "farbton_2"):
            self.assertIn(preview["fields"][key], preview["zusammenfassung"])
        result = self.confirm(preview)
        row = self.execute("SELECT * FROM auftraege WHERE id=?", (result["auftrag_id"],))[0]
        for key, value in preview["fields"].items():
            self.assertEqual(row[key], value)

    def test_active_duplicates_use_existing_ids_but_old_completed_jobs_do_not_block(self):
        for status in (1, 2, 3, 4):
            self.execute("UPDATE auftraege SET status=?", (status,))
            with self.subTest(status=status), self.assertRaises(OrderActionError) as error:
                self.create_preview(kennzeichen="test 1")
            self.assertEqual(error.exception.code, "existing_order")
            self.assertEqual(error.exception.details["auftraege"][0]["id"], 156)
        self.execute("UPDATE auftraege SET status=5")
        self.assertIsNone(self.create_preview(kennzeichen="TEST-1")["auftrag_id"])

    def test_fin_duplicate_is_checked_even_with_different_plate(self):
        self.execute("UPDATE auftraege SET fin_nummer='WVWZZZ1JZXW000001'")
        with self.assertRaises(OrderActionError) as error:
            self.create_preview(fin_nummer="WVWZZZ1JZXW000001")
        self.assertEqual(error.exception.code, "existing_order")

    def test_duplicate_created_after_preview_is_rejected_at_confirmation(self):
        preview = self.create_preview()
        self.execute("UPDATE auftraege SET kennzeichen='TEST-N17'")
        with self.assertRaises(OrderActionError) as error:
            self.confirm(preview)
        self.assertEqual(error.exception.code, "existing_order")
        self.assertEqual(self.count("auftraege"), 1)
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)

    def test_replay_reuses_number_and_attachment_runs_once_in_same_transaction(self):
        preview = self.service.preview_create(self.fields, self.who, "upload-test-id")
        attached = []

        def attach(db, order_id):
            self.assertIsNotNone(db.execute("SELECT id FROM auftraege WHERE id=?", (order_id,)).fetchone())
            db.execute("INSERT INTO source_links VALUES(?,?)", (preview["source_id"], order_id))
            attached.append(order_id)

        first = self.confirm(preview, attach_source=attach)
        replay = self.confirm(preview)  # Callback is unnecessary for historical replay.
        self.assertEqual(first["auftrag_id"], replay["auftrag_id"])
        self.assertTrue(replay["wiederholt"])
        self.assertEqual(attached, [first["auftrag_id"]])
        self.assertEqual(self.count("source_links"), 1)
        self.assertEqual(self.count("auftraege"), 2)

    def test_source_missing_or_attachment_failure_rolls_back_everything(self):
        preview = self.service.preview_create(self.fields, self.who, "upload-test-id")
        with self.assertRaises(OrderActionError) as error:
            self.confirm(preview)
        self.assertEqual(error.exception.code, "source_attachment_missing")

        def broken(db, order_id):
            db.execute("INSERT INTO source_links VALUES('test',?)", (order_id,))
            raise RuntimeError("synthetic attachment failure")

        with self.assertRaises(RuntimeError):
            self.confirm(preview, attach_source=broken)
        for table, count in (("auftraege", 1), ("source_links", 0), ("status_log", 0), ("assistent_fortschritt_audit", 0)):
            self.assertEqual(self.count(table), count)

    def test_audit_failure_rolls_back_new_order_and_color_patch(self):
        self.execute("CREATE TRIGGER reject_audit BEFORE UPDATE OF result_json ON assistent_fortschritt_audit BEGIN SELECT RAISE(ABORT,'synthetic'); END")
        for preview in (self.create_preview(), self.service.preview_color(156, {"farbcode": "007"}, self.who)):
            with self.assertRaises(sqlite3.IntegrityError):
                self.confirm(preview)
        self.assertEqual(self.count("auftraege"), 1)
        self.assertEqual(self.execute("SELECT farbcode FROM auftraege")[0]["farbcode"], "")
        self.assertEqual(self.count("status_log"), 0)
        self.assertEqual(self.count("assistent_fortschritt_audit"), 0)

    def test_concurrent_same_key_creates_one_order_and_distinct_keys_detect_vehicle(self):
        preview = self.create_preview()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.confirm(preview), range(2)))
        self.assertEqual(sorted(r["wiederholt"] for r in results), [False, True])
        with self.assertRaises(OrderActionError) as error:
            self.confirm(preview, "different")
        self.assertEqual(error.exception.code, "existing_order")
        self.assertEqual(self.count("auftraege"), 2)

    def test_actor_rights_and_payload_binding_apply_before_replay(self):
        preview = self.create_preview()
        self.confirm(preview)
        for who in (None, {}, dict(self.who, dokumentieren=0), dict(self.who, dokumentieren="1"), dict(self.who, actor="mitarbeiter:2")):
            with self.subTest(who=who), self.assertRaises(OrderActionError) as error:
                self.service.confirm(preview, who, "assistant:one")
            self.assertEqual(error.exception.code, "permission_denied")
        changed = dict(preview, fields=dict(preview["fields"], kunde_name="Anderer Testname"))
        with self.assertRaises(OrderActionError) as error:
            self.confirm(changed)
        self.assertEqual(error.exception.code, "request_conflict")

    def test_field_validation_does_not_truncate_or_invent_identifiers(self):
        for fields in ({"kunde_name": ""}, {"kennzeichen": "", "fin_nummer": ""}, {"fin_nummer": "WVWZZZ1JZXW000001X"},
                       {"fin_nummer": "WVWZZZ1JZXW00000I"}, {"hsn_nummer": "12345"}, {"tsn_nummer": "AB"},
                       {"kontakt_telefon": "123"}, {"kunde_email": "not-an-address"}, {"auftrag_id": 402},
                       {"kunden_status_aktiv": 1}, {"status": 3}, {"iban": "forbidden"}, {"farbcode": []}):
            with self.subTest(fields=fields), self.assertRaises(OrderActionError):
                self.create_preview(**fields)
        for fields in ({}, {"kunde_email": "bad"}, {"kontakt_telefon": "12"}, {"kunde_name": ""}, {"kunden_status_aktiv": "1"}):
            with self.subTest(fields=fields), self.assertRaises(OrderActionError):
                self.service.preview_contact(156, fields, self.who)
        for fields in ({}, {"farbcode": ""}, {"farbcode": "unbekannt"}, {"preis": "10"}):
            with self.subTest(fields=fields), self.assertRaises(OrderActionError):
                self.service.preview_color(156, fields, self.who)

    def test_postgres_branch_uses_returning_and_transaction_vehicle_lock(self):
        original = self.portal.get_db
        statements = []
        # Execute the real portal adapter and SQL converter without importing
        # app.py, loading configuration or starting any background work.
        app_path = Path(__file__).resolve().parents[1] / "app.py"
        wanted = {"DbRow", "PostgresCursor", "PostgresConnection", "get_insert_table_name", "convert_sqlite_sql_to_postgres"}
        tree = ast.parse(app_path.read_text(encoding="utf-8-sig"))
        nodes = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in wanted]
        namespace = {"re": re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(app_path), "exec"), namespace)

        class Cursor:
            def __init__(self, db):
                self.cursor = db.cursor()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.cursor.close()

            def execute(self, sql, params):
                statements.append(sql)
                if "pg_advisory_xact_lock" in sql:
                    self.description = [SimpleNamespace(name="pg_advisory_xact_lock")]
                    self.rows, self.rowcount = [(None,)], 1
                else:
                    self.cursor.execute(sql.replace("%s", "?").replace(" FOR UPDATE", ""), params)
                    self.description = [SimpleNamespace(name=c[0]) for c in self.cursor.description] if self.cursor.description else None
                    self.rows = [tuple(row) for row in self.cursor.fetchall()] if self.description else []
                    self.rowcount = self.cursor.rowcount

            def fetchall(self):
                return self.rows

        def adapted():
            db = original()
            connection = SimpleNamespace(cursor=lambda: Cursor(db), commit=db.commit, rollback=db.rollback, close=db.close)
            return namespace["PostgresConnection"](connection)

        self.portal.USE_POSTGRES = True
        self.portal.get_db = adapted
        result = self.confirm(self.create_preview())
        self.assertEqual(result["auftrag_id"], 157)
        self.assertTrue(any("INSERT INTO auftraege" in sql and "RETURNING id" in sql for sql in statements))
        lock_index = next(i for i, sql in enumerate(statements) if "pg_advisory_xact_lock" in sql)
        write_index = next(i for i, sql in enumerate(statements) if "INSERT INTO auftraege" in sql)
        self.assertLess(lock_index, write_index)
        statements.clear()
        self.confirm(self.service.preview_color(156, {"farbcode": "007"}, self.who), "color")
        self.assertTrue(any(sql.endswith(" FOR UPDATE") for sql in statements))


if __name__ == "__main__":
    unittest.main()
