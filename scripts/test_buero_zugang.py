"""Offline permission-boundary tests using temporary, synthetic employee data."""
from contextlib import closing
from copy import deepcopy
from functools import wraps
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from threading import RLock
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import Mock, patch

from flask import Flask, abort, session
from werkzeug.datastructures import MultiDict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import werkstatt_buero_zugang as access


class OfficeAccessTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "synthetic.db"
        self.app = Flask(__name__)
        self.app.secret_key = "synthetic-office-test-only"
        self.app.config.update(TESTING=True)

        def db():
            connection = sqlite3.connect(self.path)
            connection.row_factory = sqlite3.Row
            return connection

        def admin_required(fn):
            @wraps(fn)
            def wrapped(*args, **kwargs):
                # Simulate the application's future scoped admin decorator:
                # the module's grant route must still independently refuse staff.
                if not (session.get("admin") or session.get("assistent_mid")):
                    abort(403)
                return fn(*args, **kwargs)
            return wrapped

        def employee_identity(connection):
            mid = session.get("assistent_mid")
            if type(mid) is not int:
                return None
            row = connection.execute("""SELECT r.*,m.name AS mitarbeiter_name,m.aktiv FROM assistent_rechte r
                JOIN mitarbeiter m ON m.id=r.mitarbeiter_id WHERE r.mitarbeiter_id=?""", (mid,)).fetchone()
            if (not row or not row["aktiv"] or not row["lesen"]
                    or row["version"] != session.get("assistent_version")
                    or row["auth_version"] != session.get("assistent_auth_version")):
                return None
            return dict(row, actor=f"mitarbeiter:{mid}")

        lock = RLock()
        self.p = SimpleNamespace(app=self.app, get_db=db, admin_required=admin_required,
                                 employee_portal=SimpleNamespace(identity=employee_identity),
                                 portal_originals_operation_lock=lambda: lock,
                                 schedule_change_backup=Mock())
        with closing(db()) as connection:
            connection.executescript("""CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT,aktiv INTEGER);
                CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,version INTEGER,
                auth_version INTEGER,lesen INTEGER,dokumentieren INTEGER,einkaufen INTEGER,limit_cent INTEGER);
                INSERT INTO mitarbeiter VALUES(1,'Synthetic office employee',1),(2,'Other employee',1),(3,'Inactive',0);
                INSERT INTO assistent_rechte VALUES(1,1,1,1,0,0,0),(2,1,1,1,0,0,0),(3,1,1,1,0,0,0);""")
            connection.commit()
        self.service = access.register_office_access(self.p)
        self.service.init_schema()
        self.service.init_schema()
        self.admin = self.app.test_client()
        self.staff = self.app.test_client()
        self.anonymous = self.app.test_client()
        with self.admin.session_transaction() as value:
            value.update(admin=True, csrf_token="synthetic-csrf")
        with self.staff.session_transaction() as value:
            value.update(assistent_mid=1, assistent_version=1, assistent_auth_version=1,
                         csrf_token="synthetic-csrf")
        self.rules = patch.dict(access.OFFICE_ALLOWED_ENDPOINTS, {
            "termine": {"calendar.overview": frozenset({"GET"}), "calendar.create": frozenset({"POST"})},
            "belege": {"office_documents.upload": frozenset({"POST"})},
        })
        self.rules.start()
        self.addCleanup(self.rules.stop)

    def sql(self, query, values=()):
        with closing(self.p.get_db()) as connection:
            cursor = connection.execute(query, values)
            rows = cursor.fetchall()
            connection.commit()
            return rows

    def form(self, **changes):
        value = dict(csrf_token="synthetic-csrf", mitarbeiter_id="1", version="0",
                     action="save", categories=["termine"])
        value.update(changes)
        return value

    def grant(self, **changes):
        return self.admin.post("/admin/buero-zugaenge", data=self.form(**changes))

    def identity(self, **session_changes):
        with self.app.test_request_context():
            session.update(assistent_mid=1, assistent_version=1, assistent_auth_version=1)
            session.update(session_changes)
            return self.service.identity()

    def allowed(self, endpoint, method="GET"):
        with self.app.test_request_context():
            session.update(assistent_mid=1, assistent_version=1, assistent_auth_version=1)
            return self.service.allows(endpoint, method)

    def snapshot(self):
        return {"tables": {table: [dict(r) for r in self.sql("SELECT * FROM " + table)]
                           for table in access.BACKUP_TABLES}}

    def test_default_denies_every_employee_and_does_not_modify_assistant_rights(self):
        self.assertIsNone(self.identity())
        self.assertFalse(self.allowed("calendar.overview"))
        self.assertEqual(self.sql("SELECT COUNT(*) FROM buero_zugaenge")[0][0], 0)
        self.assertIs(self.p.office_access, self.service)
        self.assertEqual(self.sql("SELECT dokumentieren,einkaufen,limit_cent FROM assistent_rechte WHERE mitarbeiter_id=1")[0][:], (0, 0, 0))

    def test_category_and_method_boundaries_no_admin_promotion(self):
        self.assertEqual(self.grant().status_code, 303)
        who = self.identity()
        self.assertEqual((who["mitarbeiter_id"], who["mitarbeiter_name"], who["actor"]),
                         (1, "Synthetic office employee", "mitarbeiter:1"))
        self.assertEqual((who["dokumentieren"], who["einkaufen"]), (0, 0))
        self.assertEqual(who["office_categories"], ("termine",))
        self.assertTrue(self.allowed("calendar.overview", "GET"))
        self.assertTrue(self.allowed("calendar.create", "POST"))
        for endpoint, method in [("calendar.overview", "POST"), ("calendar.create", "GET"),
                                 ("office_documents.upload", "POST"), ("admin_mitarbeiter", "GET"),
                                 ("assistent.index", "GET"), ("bestellordner", "GET"),
                                 ("calendar.overview.extra", "GET"), ("calendar.*", "GET"),
                                 ("/admin/calendar", "GET"), (None, "GET"),
                                 ("calendar.overview", ["GET"])]:
            self.assertFalse(self.allowed(endpoint, method), (endpoint, method))
        with self.staff.session_transaction() as value:
            self.assertFalse(value.get("admin"))

    def test_another_employee_cannot_inherit_office_categories(self):
        self.grant()
        self.assertIsNone(self.identity(assistent_mid=2))

    def test_staff_and_anonymous_cannot_grant_even_if_outer_admin_guard_admits_staff(self):
        for client in (self.staff, self.anonymous):
            self.assertEqual(client.get("/admin/buero-zugaenge").status_code, 403)
            self.assertEqual(client.post("/admin/buero-zugaenge", data=self.form()).status_code, 403)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM buero_zugaenge")[0][0], 0)

    def test_csrf_missing_wrong_and_conflicting_copies_rejected_without_write(self):
        for data in (self.form(csrf_token="wrong"), {k:v for k,v in self.form().items() if k != "csrf_token"},
                     MultiDict([("csrf_token", "synthetic-csrf"), ("csrf_token", "wrong"),
                                ("mitarbeiter_id", "1"), ("version", "0"), ("action", "save"), ("categories", "termine")])):
            self.assertEqual(self.admin.post("/admin/buero-zugaenge", data=data).status_code, 400)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM buero_zugang_audit")[0][0], 0)

    def test_identical_csrf_copies_allow_save_and_revoke_but_not_ambiguous_employee(self):
        form = MultiDict(self.form())
        form.setlist("csrf_token", ["synthetic-csrf", "synthetic-csrf"])
        self.assertEqual(self.admin.post("/admin/buero-zugaenge", data=form).status_code, 303)
        form.add("mitarbeiter_id", "2")
        self.assertEqual(self.admin.post("/admin/buero-zugaenge", data=form).status_code, 400)
        form = MultiDict(self.form(action="revoke", version="1", categories=[]))
        form.setlist("csrf_token", ["synthetic-csrf", "synthetic-csrf"])
        self.assertEqual(self.admin.post("/admin/buero-zugaenge", data=form).status_code, 303)
        self.assertIsNone(self.identity())
        self.assertEqual(self.sql("SELECT COUNT(*) FROM buero_zugang_audit")[0][0], 2)

    def test_unknown_empty_duplicate_categories_and_inactive_staff_rejected(self):
        for changes in ({"categories": ["admin"]}, {"categories": []},
                        {"categories": ["termine", "termine"]}, {"mitarbeiter_id": "3"}):
            self.assertEqual(self.grant(**changes).status_code, 400)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM buero_zugaenge")[0][0], 0)

    def test_revoke_takes_effect_immediately_and_old_form_cannot_regrant(self):
        self.grant()
        self.assertIsNotNone(self.identity())
        self.assertEqual(self.grant(action="revoke", version="1", categories=[]).status_code, 303)
        self.assertIsNone(self.identity())
        self.assertEqual(self.grant(version="1").status_code, 400)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM buero_zugang_audit")[0][0], 2)
        self.assertEqual(self.sql("SELECT aktiv,version FROM buero_zugaenge")[0][:], (0, 2))

    def test_personal_revoke_auth_reset_or_rights_change_invalidates_grant(self):
        self.grant()
        for query in ("UPDATE mitarbeiter SET aktiv=0 WHERE id=1",
                      "UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1",
                      "UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1",
                      "UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1"):
            self.sql(query)
            self.assertIsNone(self.identity())
            self.sql("UPDATE mitarbeiter SET aktiv=1 WHERE id=1")
            self.sql("UPDATE assistent_rechte SET lesen=1,version=1,auth_version=1 WHERE mitarbeiter_id=1")
        # Even a fresh personal login must not silently revive a grant bound to
        # the previous rights/password revision.
        self.sql("UPDATE assistent_rechte SET version=2,auth_version=2 WHERE mitarbeiter_id=1")
        self.assertIsNone(self.identity(assistent_version=2, assistent_auth_version=2))

    def test_malformed_stored_categories_fail_closed(self):
        self.grant()
        for raw in ("{}", "[]", '["admin"]', '["termine","termine"]', "broken"):
            self.sql("UPDATE buero_zugaenge SET kategorien_json=?", (raw,))
            self.assertIsNone(self.identity())

    def test_grant_endpoint_cannot_be_added_to_office_allowlist(self):
        self.grant()
        with patch.dict(access.OFFICE_ALLOWED_ENDPOINTS["termine"], {"office_access.admin": {"GET", "POST"}}):
            self.assertFalse(self.allowed("office_access.admin", "POST"))

    def test_admin_form_is_private_and_escapes_employee_name(self):
        self.sql("UPDATE mitarbeiter SET name='<script>alert(1)</script>' WHERE id=1")
        response = self.admin.get("/admin/buero-zugaenge")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"<script>", response.data)
        self.assertIn(b"&lt;script&gt;", response.data)
        self.assertEqual(response.headers["Cache-Control"], "private, no-store")

    def test_restore_requires_exact_current_grants_and_revocation_audit(self):
        self.grant()
        current = self.snapshot()
        access.ensure_office_access_for_import(self.p, export=current)
        self.grant(action="revoke", version="1", categories=[])
        before = self.snapshot()
        for snapshot in (current, {"tables": {}}, {"tables": {"buero_zugaenge": []}}):
            with self.assertRaises(ValueError):
                access.ensure_office_access_for_import(self.p, export=snapshot)
        self.assertEqual(self.snapshot(), before)
        access.ensure_office_access_for_import(self.p, export=before)

    def test_old_empty_sqlite_backup_allowed_only_before_any_grant(self):
        backup = Path(self.tmp.name) / "old.db"
        with closing(sqlite3.connect(backup)) as db:
            db.execute("CREATE TABLE old_data(id INTEGER)")
            db.commit()
        access.ensure_office_access_for_import(self.p, imported_db=backup)
        self.grant()
        with self.assertRaises(ValueError):
            access.ensure_office_access_for_import(self.p, imported_db=backup)

    def test_service_write_methods_reject_staff_directly(self):
        with self.app.test_request_context():
            session.update(assistent_mid=1, assistent_version=1, assistent_auth_version=1)
            with self.assertRaises(PermissionError):
                self.service.set_access(1, ["termine"], expected_version=0)
            with self.assertRaises(PermissionError):
                self.service.admin_rows()


if __name__ == "__main__":
    main()
