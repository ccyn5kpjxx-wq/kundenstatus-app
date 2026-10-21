"""Synthetic access-control tests; no application secrets, real database, or network."""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import date
from functools import wraps
from pathlib import Path
import hashlib
import sqlite3
import sys
import tempfile
from threading import RLock
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

from flask import Flask, abort, session
from werkzeug.datastructures import MultiDict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import werkstatt_servicezugang as access
from werkstatt_kundenflotten import register_kundenflotten


class ServiceAccessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "synthetic.db"
        self.app = Flask(__name__, template_folder=str(ROOT / "templates"), static_folder=str(ROOT / "static"))
        self.app.secret_key = "synthetic-service-access-only"
        self.app.config.update(TESTING=True, SERVICE_ACCESS_ENABLED=True)

        def db():
            connection = sqlite3.connect(self.path)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            return connection

        def admin_required(fn):
            @wraps(fn)
            def wrapped(*args, **kwargs):
                if not session.get("admin"):
                    abort(403)
                return fn(*args, **kwargs)
            return wrapped

        lock = RLock()
        self.p = SimpleNamespace(app=self.app, get_db=db, admin_required=admin_required,
            portal_originals_operation_lock=lambda: lock, login_rate_limit_status=Mock(return_value=(False, 0)),
            record_failed_login=Mock(), clear_login_attempts=Mock(), clear_remember_login_cookie=Mock())
        self.fleet = register_kundenflotten(self.p)
        self.fleet.init_schema()
        with self.fleet.db() as conn:
            access.ensure_schema(conn)
            access.ensure_schema(conn)
        self.service = access.register_service_access(self.p, self.fleet)
        self.company = self.fleet.create_company({"name": "Synthetic firm"})
        self.other = self.fleet.create_company({"name": "Other synthetic firm"})
        self.vehicle = self.car(self.company, "TEST 1", "Alex", "alex@example.invalid")
        self.second = self.car(self.company, "TEST 2", "Sam", "sam@example.invalid")
        self.foreign = self.car(self.other, "TEST 3", "Alex", "alex@example.invalid")
        self.client = self.app.test_client()
        self.admin = self.app.test_client()
        with self.admin.session_transaction() as sess:
            sess.update(admin=True, csrf_token="synthetic-csrf")
        self.password = "Synthetic-Passwort-2026"

    def car(self, company, plate, name, email):
        key = self.fleet.create_vehicle(company, {"kennzeichen": plate, "modell": "Synthetic car", "km": "12000", "km_datum": date.today().isoformat()})
        self.fleet.change_vehicle(company, key, "fahrer", {"name": name, "email": email, "kanal": "keine"})
        return key

    def sql(self, query, args=()):
        with closing(self.p.get_db()) as db:
            rows = db.execute(query, args).fetchall()
            db.commit()
            return rows

    def prepare(self, **changes):
        data = dict(name="Alex", email="alex@example.invalid", firma_id=self.company,
                    fahrzeug_ids=[self.vehicle], bestaetigt="1", request_id=uuid4().hex)
        data.update(changes)
        return self.service.prepare_invitation(data)

    def activate(self, **changes):
        invitation = self.prepare(**changes)
        self.service.accept(invitation["token"], self.password, self.password)
        return invitation

    def csrf(self, client=None):
        with (client or self.client).session_transaction() as sess:
            return sess.get("csrf_token")

    def login(self, client=None, **changes):
        client = client or self.client
        client.get("/service/anmelden")
        data = dict(email="alex@example.invalid", passwort=self.password, csrf_token=self.csrf(client))
        data.update(changes)
        return client.post("/service/anmelden", data=data)

    def who(self, client=None):
        with (client or self.client).session_transaction() as sess:
            state = dict(sess)
        with self.app.test_request_context():
            session.update(state)
            return self.service.identity()

    def test_invitation_only_hash_and_expiry_persist_no_password_or_bearer(self):
        result = self.prepare()
        row = dict(self.sql("SELECT * FROM service_einladungen")[0])
        self.assertEqual(row["token_hash"], hashlib.sha256(result["token"].encode()).hexdigest())
        self.assertEqual(row["gueltig_bis"] - row["erstellt_am"], 24 * 3600)
        self.assertNotIn(result["token"], repr(self.sql("SELECT * FROM service_einladungen")))
        self.assertEqual(self.sql("SELECT aktiv,passwort_hash FROM service_zugaenge")[0][0], 0)

    def test_invitation_is_once_and_password_hash_is_strong(self):
        result = self.activate()
        self.assertTrue(self.sql("SELECT passwort_hash FROM service_zugaenge")[0][0].startswith("scrypt:"))
        self.assertIsNone(self.service.invitation(result["token"]))
        with self.assertRaises(ValueError):
            self.service.accept(result["token"], self.password, self.password)
        self.assertEqual(self.login().status_code, 303)

    def test_concurrent_acceptance_cannot_reuse_the_same_code(self):
        result = self.prepare()
        def accept_once(_):
            try:
                self.service.accept(result["token"], self.password, self.password)
                return "accepted"
            except ValueError:
                return "rejected"
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertCountEqual(list(pool.map(accept_once, range(2))), ["accepted", "rejected"])
        self.assertEqual(self.sql("SELECT COUNT(*) FROM service_einladungen WHERE benutzt_am IS NOT NULL")[0][0], 1)

    def test_expired_and_revoked_tokens_cannot_activate(self):
        result = self.prepare()
        self.sql("UPDATE service_einladungen SET gueltig_bis=?", (int(time.time()) - 1,))
        self.assertEqual(self.client.get("/service/einladung/" + result["token"]).status_code, 410)
        with self.assertRaises(ValueError):
            self.service.accept(result["token"], self.password, self.password)
        result = self.prepare()
        with self.fleet.db() as db:
            access.revoke_vehicle(db, self.vehicle)
        self.assertIsNone(self.service.invitation(result["token"]))
        self.assertEqual(self.sql("SELECT aktiv FROM service_zugaenge")[0][0], 0)

    def test_two_drivers_and_foreign_company_are_separate(self):
        self.activate()
        self.activate(name="Sam", email="sam@example.invalid", fahrzeug_ids=[self.second])
        self.login()
        who = self.who()
        self.assertEqual([v["id"] for v in self.service.vehicles(who)], [self.vehicle])
        self.assertTrue(self.service.authorize(who, self.company, self.vehicle))
        self.assertFalse(self.service.authorize(who, self.company, self.second))
        self.assertFalse(self.service.authorize(who, self.other, self.foreign))
        self.assertFalse(self.service.authorize(who, self.other, self.vehicle))
        self.assertFalse(self.service.authorize({"rolle": "firma", "id": who["id"], "version": who["version"]}, self.company, self.vehicle))
        self.assertEqual(set(self.service.vehicles(who)[0]), {"id", "firma_id", "firma_name", "kennzeichen", "modell", "km", "km_datum"})

    def test_admin_must_confirm_current_driver_and_same_company(self):
        for changes in ({"bestaetigt": ""}, {"fahrzeug_ids": [self.second]}, {"fahrzeug_ids": [self.foreign]}, {"email": "someone@example.invalid"}, {"fahrzeug_ids": []}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.prepare(**changes)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM service_zugaenge")[0][0], 0)

    def test_driver_change_revokes_active_session_and_stale_identity_immediately(self):
        self.activate()
        self.login()
        who = self.who()
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            access.revoke_vehicle(db, self.vehicle)
            db.execute("UPDATE kundenflotten_fahrer SET name='New driver',email='new@example.invalid' WHERE fahrzeug_id=?", (self.vehicle,))
        self.assertIsNone(self.who())
        self.assertFalse(self.service.authorize(who, self.company, self.vehicle))
        self.assertEqual(self.service.vehicles(who), [])

    def test_fingerprint_blocks_access_even_if_old_writer_missed_revocation_hook(self):
        self.activate()
        self.login()
        who = self.who()
        self.sql("UPDATE kundenflotten_fahrer SET name='Different driver' WHERE fahrzeug_id=?", (self.vehicle,))
        self.assertFalse(self.service.authorize(who, self.company, self.vehicle))
        self.assertEqual(self.service.vehicles(who), [])

    def test_reissue_replaces_assignments_invalidates_password_and_old_link(self):
        first = self.prepare()
        second = self.prepare()
        self.assertIsNone(self.service.invitation(first["token"]))
        self.service.accept(second["token"], self.password, self.password)
        self.login()
        who = self.who()
        third = self.prepare()
        self.assertIsNone(self.who())
        self.assertFalse(self.service.authorize(who, self.company, self.vehicle))
        self.assertEqual(self.login().status_code, 401)
        self.assertIsNotNone(self.service.invitation(third["token"]))

    def test_same_request_cannot_generate_another_invitation(self):
        request_id = uuid4().hex
        self.prepare(request_id=request_id)
        with self.assertRaises(ValueError):
            self.prepare(request_id=request_id)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM service_einladungen")[0][0], 1)

    def test_login_rotates_csrf_removes_other_roles_and_clears_remember_cookies(self):
        self.activate()
        with self.client.session_transaction() as sess:
            sess.update(admin=True, autohaus_id=3, flotte_zugang="other-role", csrf_token="old")
        self.assertEqual(self.login().status_code, 303)
        with self.client.session_transaction() as sess:
            self.assertNotIn("admin", sess)
            self.assertNotIn("flotte_zugang", sess)
            self.assertNotIn("autohaus_id", sess)
            self.assertNotEqual(sess["csrf_token"], "old")
        self.assertEqual(self.p.clear_remember_login_cookie.call_count, 2)
        self.assertEqual(self.who()["rolle"], "fahrer")

    def test_session_expiry_future_timestamp_and_wrong_version_fail(self):
        self.activate()
        for changes in ({"service_login_am": time.time() - 9 * 3600}, {"service_login_am": time.time() + 100}, {"service_version": 99}):
            self.login()
            with self.client.session_transaction() as sess:
                sess.update(changes)
            self.assertIsNone(self.who())

    def test_csrf_logout_and_duplicate_token_rejected(self):
        self.activate()
        self.assertEqual(self.client.post("/service/anmelden", data={"email": "alex@example.invalid", "passwort": self.password}).status_code, 400)
        self.login()
        self.assertEqual(self.client.post("/service/abmelden").status_code, 400)
        data = MultiDict([("csrf_token", self.csrf()), ("csrf_token", "bad")])
        self.assertEqual(self.client.post("/service/abmelden", data=data).status_code, 400)
        self.assertEqual(self.client.post("/service/abmelden", data={"csrf_token": self.csrf()}).status_code, 303)
        self.assertIsNone(self.who())

    def test_login_and_invitation_rate_limits(self):
        self.activate()
        self.assertEqual(self.login(passwort="wrong").status_code, 401)
        self.p.record_failed_login.assert_called_with("servicezugang", "login")
        self.p.login_rate_limit_status.return_value = (True, 30)
        self.assertEqual(self.login().status_code, 429)
        result = self.prepare()
        url = "/service/einladung/" + result["token"]
        self.client.get(url)
        self.assertEqual(self.client.post(url, data={"csrf_token": self.csrf(), "passwort": self.password, "passwort_wiederholen": self.password}).status_code, 429)
        self.assertIsNotNone(self.service.invitation(result["token"]))

    def test_password_validation_and_invitation_route(self):
        result = self.prepare()
        for password, repeated in (("short", "short"), (self.password, "wrong"), ("x" * 129, "x" * 129)):
            with self.assertRaises(ValueError):
                self.service.accept(result["token"], password, repeated)
        url = "/service/einladung/" + result["token"]
        self.assertEqual(self.client.get(url).status_code, 200)
        self.assertEqual(self.client.post(url, data={"csrf_token": self.csrf(), "passwort": self.password, "passwort_wiederholen": self.password}).status_code, 303)
        self.assertEqual(self.client.get(url).status_code, 410)

    def test_admin_only_preparation_suspension_and_private_headers(self):
        path = "/admin/service-zugaenge"
        self.assertEqual(self.client.get(path).status_code, 403)
        page = self.admin.get(path)
        self.assertEqual(page.status_code, 200)
        self.assertIn("no-store", page.headers["Cache-Control"])
        self.assertEqual(page.headers["Referrer-Policy"], "no-referrer")
        result = self.activate()
        self.login()
        self.assertEqual(self.admin.post(path, data={"aktion": "sperren", "zugang_id": result["zugang_id"], "csrf_token": "synthetic-csrf"}).status_code, 303)
        self.assertIsNone(self.who())
        self.assertEqual(self.login().status_code, 401)
        with patch.dict(self.app.config, SERVICE_ACCESS_ENABLED=False):
            self.assertEqual(self.client.get("/service/anmelden").status_code, 404)
            self.assertIsNone(self.who())

    def test_admin_form_shows_code_once_and_scope_is_explicit(self):
        data = dict(name="Alex", email="alex@example.invalid", firma_id=self.company,
                    fahrzeug_ids=[self.vehicle], bestaetigt="1", request_id=uuid4().hex, csrf_token="synthetic-csrf")
        page = self.admin.post("/admin/service-zugaenge", data=data)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Persönlicher Einrichtungslink", page.text)
        self.assertIn("Noch nicht versendet", page.text)
        self.assertEqual(self.admin.post("/admin/service-zugaenge", data=data).status_code, 400)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM service_einladungen")[0][0], 1)

    def test_backup_preserves_exact_revocations_invitation_use_and_assignments(self):
        result = self.prepare()
        before = {"tables": {table: [dict(r) for r in self.sql(f"SELECT * FROM {table}")] for table in access.BACKUP_TABLES}}
        access.ensure_state_for_import(get_db=self.p.get_db, export=before)
        self.service.accept(result["token"], self.password, self.password)
        with self.assertRaises(ValueError):
            access.ensure_state_for_import(get_db=self.p.get_db, export=before)
        current = {"tables": {table: [dict(r) for r in self.sql(f"SELECT * FROM {table}")] for table in access.BACKUP_TABLES}}
        access.ensure_state_for_import(get_db=self.p.get_db, export=current)
        broken = deepcopy(current)
        broken["tables"]["service_zuordnungen"][0]["fahrer_fingerprint"] = "wrong"
        with self.assertRaises(ValueError):
            access.ensure_state_for_import(get_db=self.p.get_db, export=broken)
        with self.assertRaises(ValueError):
            access.ensure_state_for_import(get_db=self.p.get_db, export={"tables": {}})
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            access.revoke_vehicle(db, self.vehicle)
        with self.assertRaises(ValueError):
            access.ensure_state_for_import(get_db=self.p.get_db, export=current)

    def test_sqlite_restore_cannot_restore_pre_revocation_access(self):
        self.activate()
        source = Path(self.tmp.name) / "before-revocation.db"
        with closing(self.p.get_db()) as db, closing(sqlite3.connect(source)) as snapshot:
            db.backup(snapshot)
        access.ensure_state_for_import(get_db=self.p.get_db, imported_db=source)
        with self.p.portal_originals_operation_lock(), self.fleet.db() as db:
            access.revoke_vehicle(db, self.vehicle)
        with self.assertRaises(ValueError):
            access.ensure_state_for_import(get_db=self.p.get_db, imported_db=source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
