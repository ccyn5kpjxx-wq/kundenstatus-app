"""Isolated inspection persistence, access, stale-write and restore integration tests."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, nullcontext
import copy
import json
from pathlib import Path
import sqlite3
import socket
import sys
import threading
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parent))
import smoke_test as isolated
portal = isolated.portal
checklists = portal.inspektionscheckliste


class InspectionChecklistTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config["TESTING"] = True
        portal.init_db()

    def setUp(self):
        self.assertIs(socket.create_connection, isolated.deny_network)
        self.client = portal.app.test_client()
        self.csrf = "inspection-test-csrf"
        with self.client.session_transaction() as sess:
            sess[portal.CSRF_FIELD_NAME] = self.csrf
            sess["admin"] = True
        self.notifications = []
        for name in ("schedule_change_backup", "add_benachrichtigung", "sende_autohaus_benachrichtigung_mail",
                     "sende_endkunden_mail", "notify_customer_whatsapp_fertig", "fuehre_auftrag_status_wechsel_aus"):
            patcher = patch.object(portal, name)
            mocked = patcher.start()
            self.addCleanup(patcher.stop)
            if name != "schedule_change_backup":
                self.notifications.append(mocked)

    def tearDown(self):
        for mocked in self.notifications:
            mocked.assert_not_called()

    def order(self, art="inspektion", status=2):
        ident = portal.create_auftrag("intern", leistungsart=art, fahrzeug="Test Benziner", kunde_name="Testkunde")
        self.sql("UPDATE auftraege SET status=? WHERE id=?", (status, ident))
        return ident

    def sql(self, statement, args=(), rows=False):
        db = portal.get_db()
        try:
            cursor = db.execute(statement, args)
            result = [dict(r) for r in cursor.fetchall()] if rows else cursor.rowcount
            db.commit()
            return result
        finally:
            db.close()

    def initialize(self, ident, client=None):
        return (client or self.client).post(f"/admin/auftrag/{ident}/inspektionscheckliste/anlegen",
            data={portal.CSRF_FIELD_NAME: self.csrf}, headers={"X-Requested-With": "fetch"})

    def context(self, ident, admin=True):
        raw = self.sql("SELECT * FROM auftraege WHERE id=?", (ident,), rows=True)[0]
        return checklists.load_many(portal, [raw], admin=admin)[0]["inspektionscheckliste"]

    def point(self, ident):
        return self.context(ident)["punkte"][0]

    def save(self, ident, point, result="erledigt", note="Geprüft", client=None, version=None, fetch=True):
        return (client or self.client).post(f"/werkstatt/auftrag/{ident}/inspektionscheckliste/{point['id']}",
            data={portal.CSRF_FIELD_NAME: self.csrf, "version": str(point["version"]) if version is None else version,
                  "ergebnis": result, "notiz": note}, headers={"X-Requested-With": "fetch"} if fetch else {})

    def worker_client(self):
        client = portal.app.test_client()
        with client.session_transaction() as sess:
            sess[portal.CSRF_FIELD_NAME] = self.csrf
            sess["werkstatt_tafel"] = portal.werkstatt_tafel_session_token("INSPECT99")
        portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "INSPECT99")
        return client

    def test_get_never_initializes_checklist_and_context_is_internal(self):
        ident = self.order()
        before = self.sql("SELECT * FROM inspektions_checklisten", rows=True)
        for url in (f"/admin/auftrag/{ident}", f"/werkstatt/auftrag/{ident}", "/werkstatt/tafel"):
            response = self.client.get(url)
            self.assertEqual(response.status_code, 200, url)
        self.assertEqual(before, self.sql("SELECT * FROM inspektions_checklisten", rows=True))
        context = self.context(ident)
        self.assertFalse(context["vorhanden"])
        self.assertTrue(context["anlegbar"])
        self.assertEqual(len(context["vorlage_punkte"]), 24)
        self.assertFalse("inspektionscheckliste" in portal.get_auftrag(ident))

    def test_explicit_creation_is_complete_idempotent_and_keeps_evidence(self):
        ident = self.order()
        response = self.initialize(ident)
        self.assertEqual(response.status_code, 201)
        context = response.get_json()["inspektionscheckliste"]
        self.assertEqual((context["gesamt"], context["offen"]), (24, 24))
        self.assertEqual(len({p["key"] for p in context["punkte"]}), 24)
        point = context["punkte"][0]
        self.assertEqual(self.save(ident, point, "mangel", "Befestigung prüfen").status_code, 200)
        self.assertEqual(self.initialize(ident).status_code, 200)
        preserved = self.point(ident)
        self.assertEqual((preserved["ergebnis"], preserved["version"], preserved["notiz"]), ("mangel", 2, "Befestigung prüfen"))
        self.assertEqual(self.sql("SELECT COUNT(*) n FROM inspektions_checkpunkte WHERE auftrag_id=?", (ident,), rows=True)[0]["n"], 24)

    def test_access_csrf_and_person_identity_are_not_invented(self):
        ident = self.order()
        self.initialize(ident)
        point = self.point(ident)
        anonymous = portal.app.test_client()
        with anonymous.session_transaction() as sess:
            sess[portal.CSRF_FIELD_NAME] = self.csrf
        self.assertEqual(self.save(ident, point, client=anonymous).status_code, 302)
        worker = self.worker_client()
        self.assertEqual(self.initialize(ident, worker).status_code, 302)
        response = worker.post(f"/werkstatt/auftrag/{ident}/inspektionscheckliste/{point['id']}", data={"version": "1", "ergebnis": "erledigt"})
        self.assertEqual(response.status_code, 400)
        response = self.save(ident, point, client=worker)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["punkt"]["geaendert_von"], "Werkstatt")
        # Code rotation revokes an existing tablet session.
        portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "CHANGED99")
        self.assertEqual(self.save(ident, self.point(ident), client=worker).status_code, 302)

    def test_policy_is_rechecked_against_current_database_and_points_are_order_bound(self):
        ident = self.order()
        self.initialize(ident)
        point = self.point(ident)
        other = self.order()
        self.initialize(other)
        self.assertEqual(self.save(other, point).status_code, 404)
        self.sql("UPDATE auftraege SET leistungsart='lackierung' WHERE id=?", (ident,))
        self.assertEqual(self.save(ident, point).status_code, 409)
        self.sql("UPDATE auftraege SET leistungsart='inspektion', archiviert=1 WHERE id=?", (ident,))
        self.assertEqual(self.save(ident, point).status_code, 409)
        self.sql("UPDATE auftraege SET archiviert=0,status=5 WHERE id=?", (ident,))
        self.assertEqual(self.save(ident, point).status_code, 409)
        self.sql("UPDATE auftraege SET status=1 WHERE id=?", (ident,))
        self.assertEqual(self.save(ident, point, client=self.worker_client()).status_code, 409)
        self.assertEqual(self.save(ident, point).status_code, 200)
        self.assertEqual(self.point(ident)["version"], 2)
        self.assertEqual(self.initialize(self.order("bremsen")).status_code, 409)

    def test_structured_historical_type_uses_same_classification_in_get_and_write(self):
        ident = self.order(art="")
        intake = json.dumps({"anliegen": "mechanikanfrage", "mechanik_kategorien": ["inspektion"]})
        self.sql("UPDATE auftraege SET schaden_aufnahme_json=? WHERE id=?", (intake, ident))
        self.assertTrue(self.context(ident)["anlegbar"])
        self.assertEqual(self.initialize(ident).status_code, 201)
        self.assertEqual(self.save(ident, self.point(ident)).status_code, 200)
        point = self.point(ident)
        self.sql("UPDATE auftraege SET schaden_aufnahme_json=? WHERE id=?", ('{"anliegen":"reifenservice"}', ident))
        self.assertEqual(self.save(ident, point).status_code, 409)

    def test_validation_and_stale_form_keep_saved_note_and_offer_recovery(self):
        ident = self.order()
        self.initialize(ident)
        point = self.point(ident)
        for result, note, version in (("mangel", " ", "1"), ("nicht_zutreffend", "", "1"),
                ("wrong", "", "1"), ("erledigt", "x" * 601, "1"), ("offen", "x\x00y", "1"),
                ("offen", "", "0"), ("offen", "", "-1"), ("offen", "", "1.0")):
            with self.subTest(result=result, length=len(note), version=version):
                self.assertEqual(self.save(ident, point, result, note, version=version).status_code, 400)
        self.assertEqual(self.save(ident, point, note="Erster Stand").status_code, 200)
        response = self.save(ident, point, note="Meine ungespeicherte Notiz <script>", fetch=False)
        self.assertEqual(response.status_code, 409)
        self.assertIn("Zurück zur Inspektionscheckliste", response.text)
        self.assertIn("&lt;script&gt;", response.text)
        self.assertEqual(self.point(ident)["notiz"], "Erster Stand")
        # Explicit result values can be reversed, but each action needs the current version.
        current = self.point(ident)
        self.assertEqual(self.save(ident, current, "nicht_zutreffend", "Nicht verbaut").status_code, 200)
        self.assertEqual(self.save(ident, self.point(ident), "offen", "Erneut prüfen").status_code, 200)

    def test_database_cas_two_connections_one_winner_and_independent_points(self):
        ident = self.order()
        self.initialize(ident)
        point = self.point(ident)
        barrier = threading.Barrier(2)
        # Disable only the process-wide restore fence to prove the DB row lock and
        # version predicate also work across workers that do not share a Python lock.
        def write(point, text):
            barrier.wait(timeout=10)
            try:
                return checklists.save_point(portal, ident, point["id"], str(point["version"]), "erledigt", text, admin=True)
            except checklists.ChecklistError as exc:
                return exc.status
        with patch.object(portal, "portal_originals_operation_lock", side_effect=lambda: nullcontext()):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda text: write(point, text), ("Monitor A", "Monitor B")))
        self.assertEqual(sum(isinstance(r, dict) for r in results), 1)
        self.assertIn(409, results)
        self.assertEqual(self.point(ident)["version"], 2)
        points = self.context(ident)["punkte"][:2]
        barrier = threading.Barrier(2)
        with patch.object(portal, "portal_originals_operation_lock", side_effect=lambda: nullcontext()):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda p: write(p, p["key"]), points))
        self.assertTrue(all(isinstance(r, dict) for r in results))
        self.assertEqual([p["version"] for p in self.context(ident)["punkte"][:2]], [3, 2])

    def test_racing_initialization_is_once_and_last_check_does_not_finish_order(self):
        ident = self.order(status=3)
        barrier = threading.Barrier(2)
        def create(_):
            barrier.wait(timeout=10)
            return checklists.create_checklist(portal, ident)
        with patch.object(portal, "portal_originals_operation_lock", side_effect=lambda: nullcontext()):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(create, range(2)))
        self.assertEqual(sorted(results), [False, True])
        for point in self.context(ident)["punkte"]:
            self.assertEqual(self.save(ident, point).status_code, 200)
        self.assertEqual(self.context(ident)["erledigt"], 24)
        raw = self.sql("SELECT status,produktion_schritt FROM auftraege WHERE id=?", (ident,), rows=True)[0]
        self.assertEqual(raw["status"], 3)
        self.assertFalse(raw["produktion_schritt"])

    def test_database_type_race_is_checked_after_waiting_for_order_lock(self):
        ident = self.order()
        self.initialize(ident)
        point = self.point(ident)
        blocker = portal.get_db()
        blocker.execute("UPDATE auftraege SET leistungsart='bremsen' WHERE id=?", (ident,))
        reached_lock = threading.Event()
        get_db = portal.get_db
        class ObservedConnection:
            def __init__(self):
                self.real = get_db()
            def execute(self, sql, args=()):
                if sql.startswith("UPDATE auftraege SET status=status"):
                    reached_lock.set()
                return self.real.execute(sql, args)
            def __getattr__(self, key):
                return getattr(self.real, key)
        def write():
            try:
                return checklists.save_point(portal, ident, point["id"], "1", "erledigt", "", admin=True)
            except checklists.ChecklistError as exc:
                return exc.status
        try:
            with patch.object(portal, "get_db", side_effect=ObservedConnection), patch.object(portal, "portal_originals_operation_lock", side_effect=lambda: nullcontext()):
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(write)
                    self.assertTrue(reached_lock.wait(timeout=10))
                    blocker.commit()
                    self.assertEqual(pending.result(timeout=10), 409)
        finally:
            blocker.rollback()
            blocker.close()
        self.assertEqual(self.sql("SELECT version FROM inspektions_checkpunkte WHERE id=?", (point["id"],), rows=True)[0]["version"], 1)

    def test_safe_redirect_only_same_internal_order(self):
        ident = self.order()
        self.initialize(ident)
        for target, expected in (("https://evil.invalid/path", f"/admin/auftrag/{ident}#inspektionscheckliste"),
                                 ("http://[", f"/admin/auftrag/{ident}#inspektionscheckliste"),
                                 (f"/admin/auftrag/{ident}#inspektionspunkt-1", f"/admin/auftrag/{ident}#inspektionspunkt-1"),
                                 (f"/admin/auftrag/{ident+1}", f"/admin/auftrag/{ident}#inspektionscheckliste")):
            point = self.point(ident)
            response = self.client.post(f"/werkstatt/auftrag/{ident}/inspektionscheckliste/{point['id']}",
                data={portal.CSRF_FIELD_NAME: self.csrf, "version": point["version"], "ergebnis": "offen", "next": target})
            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], expected)

    def test_backup_export_json_and_sqlite_restore_and_old_archive_guard(self):
        ident = self.order()
        self.initialize(ident)
        self.save(ident, self.point(ident), "mangel", "Dokumentierter Prüfbedarf")
        path = portal.create_backup_package("inspection-isolated-test")
        with zipfile.ZipFile(path) as archive:
            export = json.loads(archive.read("backup.json"))
            self.assertIn(checklists.SCHEMA_FEATURE, export["schema_features"])
            self.assertTrue(any(p["auftrag_id"] == ident and p["notiz"] == "Dokumentierter Prüfbedarf" for p in export["tables"][checklists.BACKUP_TABLES[1]]))
            old = copy.deepcopy(export)
            old["schema_features"].remove(checklists.SCHEMA_FEATURE)
            for table in checklists.BACKUP_TABLES:
                old["tables"].pop(table)
            with self.assertRaisesRegex(ValueError, "Inspektionschecklisten"):
                portal.import_backup_json_rows_into_current_database(old, archive, set(archive.namelist()))
            broken = copy.deepcopy(export)
            broken["tables"].pop(checklists.BACKUP_TABLES[1])
            with self.assertRaisesRegex(ValueError, "Tabellen fehlen"):
                portal.validate_backup_binary_reference_completeness(broken, {})
            self.save(ident, self.point(ident), "erledigt", "Späterer Stand")
            portal.import_backup_json_rows_into_current_database(export, archive, set(archive.namelist()))
            self.assertEqual(self.point(ident)["notiz"], "Dokumentierter Prüfbedarf")
            source = Path(isolated.TEMP_DIR) / "inspection-source.db"
            source.write_bytes(archive.read("auftraege.db"))
            self.save(ident, self.point(ident), "erledigt", "Noch ein Stand")
            portal.import_sqlite_rows_into_current_database(source)
            self.assertEqual(self.point(ident)["ergebnis"], "mangel")
            old_source = Path(isolated.TEMP_DIR) / "inspection-old.db"
            with closing(sqlite3.connect(old_source)) as old_db:
                old_db.execute("CREATE TABLE auftraege (id INTEGER PRIMARY KEY)")
                old_db.commit()
            with self.assertRaisesRegex(ValueError, "Inspektionschecklisten"):
                checklists.ensure_state_for_import(portal, imported_db=old_source)

    def test_delete_removes_children_and_schema_is_additive_and_pg_translatable(self):
        ident = self.order()
        self.initialize(ident)
        portal.init_db()
        self.assertEqual(self.context(ident)["gesamt"], 24)
        portal.delete_auftrag(ident, safety_backup=False)
        for table in checklists.BACKUP_TABLES:
            self.assertFalse(self.sql(f"SELECT * FROM {table} WHERE auftrag_id=?", (ident,), rows=True))
        statements = []
        class Capture:
            def execute(self, sql):
                statements.append(portal.convert_sqlite_sql_to_postgres(sql))
        checklists.init_schema(Capture())
        self.assertEqual(len(statements), 2)
        self.assertTrue(all("AUTOINCREMENT" not in sql for sql in statements))
        self.assertTrue(all("SERIAL" in sql for sql in statements))
        self.assertIn("inspektions_checklisten", portal.BACKUP_TABLES)


if __name__ == "__main__":
    unittest.main(verbosity=2)
