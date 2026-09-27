"""Isolated Flask/SQLite tests; no production app, network or real data."""

import hashlib
import io
import json
import pathlib
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest

from flask import Flask, abort, jsonify, request, session

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from werkstatt_fortschritt_api import MAX_BODY_BYTES, progress_csrf_exempt, register_progress_api


class ProgressApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.temp.name) / "test.db")
        self.settings = {}
        def get_db():
            db = sqlite3.connect(self.path)
            db.row_factory = sqlite3.Row
            return db
        def ensure_column(db, table, column, definition):
            if column not in [row["name"] for row in db.execute(f"PRAGMA table_info({table})")]:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        app = Flask(__name__)
        app.config.update(TESTING=True, SECRET_KEY="isolated-progress-test")
        self.portal = SimpleNamespace(app=app, get_db=get_db, ensure_column=ensure_column,
                                      get_app_setting=lambda name, default="": self.settings.get(name, default), USE_POSTGRES=False)
        db = get_db()
        db.executescript("""CREATE TABLE auftraege (
            id INTEGER PRIMARY KEY, fahrzeug TEXT, kennzeichen TEXT,
            status INTEGER, archiviert INTEGER DEFAULT 0,
            produktion_schritt TEXT DEFAULT '', geaendert_am TEXT NOT NULL);
            INSERT INTO auftraege VALUES(156,'Audi A4','TEST-1',3,0,'vorarbeit','28.09.2026 09:30');""")
        db.commit()
        db.close()
        self.service = register_progress_api(self.portal)
        self.enforce_csrf = False

        @app.before_request
        def csrf_like_host():
            if not self.enforce_csrf or request.method != "POST":
                return None
            if progress_csrf_exempt(self.portal):
                return None
            if not session.get("csrf_token") or request.headers.get("X-CSRF-Token") != session["csrf_token"]:
                abort(400)

        @app.post("/other")
        def other():
            return jsonify(ok=True)

        self.client = app.test_client()
        self.url = "/api/werkstatt/v1/auftraege/156/fortschritt"
        self.headers = {"Authorization": "Bearer synthetic-progress-secret"}
        self.grant(["auftraege:lesen", "auftraege:fortschritt"])

    def tearDown(self):
        self.temp.cleanup()

    def grant(self, scopes, token="synthetic-progress-secret"):
        self.settings["ASSISTANT_API_GRANT"] = json.dumps({"hash": hashlib.sha256(token.encode()).hexdigest(), "scopes": scopes})

    def payload(self, **changes):
        result = {"action": "lackierbereit", "expected_status": 3,
                  "expected_changed_at": "28.09.2026 09:30", "request_id": "request-1"}
        result.update(changes)
        return result

    def post(self, **changes):
        return self.client.post(self.url, json=self.payload(**changes), headers=self.headers)

    def audit_rows(self):
        db = self.portal.get_db()
        try:
            return [dict(row) for row in db.execute("SELECT * FROM assistent_fortschritt_audit")]
        finally:
            db.close()

    def test_get_requires_read_scope_and_does_not_mutate(self):
        self.assertEqual(self.client.get(self.url).status_code, 401)
        result = self.client.get(self.url, headers=self.headers)
        self.assertEqual(result.status_code, 200)
        self.assertFalse(result.json["lackierbereit"])
        self.assertEqual(result.headers["Cache-Control"], "no-store")
        self.assertEqual(self.audit_rows(), [])
        self.assertEqual(self.client.head(self.url, headers=self.headers).status_code, 200)
        self.assertEqual(self.audit_rows(), [])
        self.grant(["auftraege:fortschritt"])
        self.assertEqual(self.client.get(self.url, headers=self.headers).status_code, 403)

    def test_existing_read_grant_stays_read_only(self):
        self.grant(["auftraege:lesen", "dokumente:lesen", "einkauf:lesen"])
        before = self.settings.copy()
        result = self.post()
        self.assertEqual(result.status_code, 403)
        self.assertEqual(result.json["code"], "scope_missing")
        self.assertEqual(self.settings, before)
        self.assertEqual(self.audit_rows(), [])

    def test_write_and_actor_are_server_derived(self):
        result = self.post()
        self.assertEqual(result.status_code, 200)
        self.assertTrue(result.json["fortschritt"]["lackierbereit"])
        self.assertFalse(result.json["fortschritt"]["fahrzeug_fertig"])
        audit = self.audit_rows()[0]
        self.assertRegex(audit["actor"], r"^avatar:[0-9a-f]{20}$")
        self.assertNotIn("synthetic-progress-secret", str(audit))
        self.assertEqual(self.post(actor="admin").status_code, 400)
        self.assertEqual(len(self.audit_rows()), 1)

    def test_replay_cas_and_reused_payload_conflict(self):
        self.assertEqual(self.post().status_code, 200)
        replay = self.post()
        self.assertTrue(replay.json["wiederholt"])
        stale = self.post(request_id="request-2")
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(stale.json["code"], "stale_state")
        self.assertEqual(self.post(action="finish_starten").json["code"], "request_conflict")
        self.assertEqual(len(self.audit_rows()), 1)

    def test_auth_does_not_accept_cookie_query_or_other_tokens(self):
        with self.client.session_transaction() as state:
            state.update(admin=True, csrf_token="csrf")
        for headers in ({}, {"Authorization": "Basic synthetic-progress-secret"},
                        {"Authorization": "Bearer wrong"}, {"X-API-Key": "synthetic-progress-secret"}):
            with self.subTest(headers=headers):
                result = self.client.post(self.url + "?token=synthetic-progress-secret", json=self.payload(), headers=headers)
                self.assertEqual(result.status_code, 401)
                self.assertEqual(result.headers["Cache-Control"], "no-store")

    def test_revocation_and_malformed_grants_fail_closed(self):
        for grant in ("", "{", "null", "[]", json.dumps({"hash": "ä" * 64, "scopes": []}),
                      json.dumps({"hash": hashlib.sha256(b"synthetic-progress-secret").hexdigest(), "scopes": "auftraege:fortschritt"})):
            self.settings["ASSISTANT_API_GRANT"] = grant
            with self.subTest(grant=grant):
                self.assertEqual(self.post().status_code, 401)

    def test_exact_fields_and_types_are_required(self):
        for payload in ([], None, {}, {"action": "lackierbereit"}, self.payload(expected_status="3"),
                        self.payload(expected_status=True), self.payload(action="fertig"), self.payload(request_id=4)):
            result = self.client.post(self.url, data=json.dumps(payload), content_type="application/json", headers=self.headers)
            with self.subTest(payload=payload):
                self.assertEqual(result.status_code, 400)
                self.assertEqual(result.headers["Cache-Control"], "no-store")
        self.assertEqual(self.audit_rows(), [])

    def test_invalid_duplicate_or_nonfinite_json_is_rejected(self):
        for data in ('{', '{"action":"lackierbereit","action":"finish_starten"}', '{"expected_status":NaN}'):
            with self.subTest(data=data):
                result = self.client.post(self.url, data=data, content_type="application/json", headers=self.headers)
                self.assertEqual(result.status_code, 400)
                self.assertEqual(result.json["code"], "invalid_json")

    def test_json_content_type_required(self):
        self.assertEqual(self.client.post(self.url, data=self.payload(), headers=self.headers).status_code, 415)
        self.assertEqual(self.client.post(self.url, data=json.dumps(self.payload()), content_type="text/plain", headers=self.headers).status_code, 415)

    def test_oversized_known_and_streamed_bodies_rejected(self):
        raw = (" " * MAX_BODY_BYTES + json.dumps(self.payload())).encode()
        result = self.client.post(self.url, data=raw, content_type="application/json", headers=self.headers)
        self.assertEqual(result.status_code, 413)
        self.assertEqual(result.headers["Cache-Control"], "no-store")
        result = self.client.post(self.url, content_type="application/json", headers=self.headers,
                                  environ_overrides={"wsgi.input": io.BytesIO(raw), "wsgi.input_terminated": True,
                                                     "HTTP_TRANSFER_ENCODING": "chunked", "CONTENT_LENGTH": ""})
        self.assertEqual(result.status_code, 413)
        self.assertEqual(self.audit_rows(), [])

    def test_global_csrf_exception_is_endpoint_specific_and_scope_still_required(self):
        self.enforce_csrf = True
        self.assertEqual(self.post().status_code, 200)
        self.assertEqual(self.client.post("/other", headers=self.headers).status_code, 400)
        self.grant(["auftraege:lesen"])
        self.assertEqual(self.post().status_code, 403)
        self.assertEqual(self.client.post(self.url, json=self.payload()).status_code, 400)
        with self.client.session_transaction() as state:
            state.update(admin=True, csrf_token="csrf")
        # A cookie + valid CSRF may pass the host guard, never bearer authorization.
        result = self.client.post(self.url, json=self.payload(), headers={"X-CSRF-Token": "csrf"})
        self.assertEqual(result.status_code, 401)

    def test_exact_body_limit_is_accepted_for_known_and_streamed_input(self):
        raw = json.dumps(self.payload()).encode()
        raw += b" " * (MAX_BODY_BYTES - len(raw))
        result = self.client.post(self.url, data=raw, content_type="application/json", headers=self.headers)
        self.assertEqual(result.status_code, 200)
        result = self.client.post(self.url, content_type="application/json", headers=self.headers,
                                  environ_overrides={"wsgi.input": io.BytesIO(raw), "wsgi.input_terminated": True,
                                                     "HTTP_TRANSFER_ENCODING": "chunked", "CONTENT_LENGTH": ""})
        self.assertEqual(result.status_code, 200)
        self.assertTrue(result.json["wiederholt"])

    def test_missing_order_has_structured_404(self):
        result = self.client.get(self.url.replace("156", "999"), headers=self.headers)
        self.assertEqual(result.status_code, 404)
        self.assertEqual(result.json["code"], "not_found")


if __name__ == "__main__":
    unittest.main()
