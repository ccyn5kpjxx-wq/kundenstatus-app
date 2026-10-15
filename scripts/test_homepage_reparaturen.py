"""Isolated regressions for portal links and public damage uploads."""

import base64
from hashlib import sha256
from html.parser import HTMLParser
from io import BytesIO
import unittest
from unittest.mock import patch

from werkzeug.datastructures import FileStorage, MultiDict

from test_privat_portal_integration import PNG_BYTES, base_payload, portal, session_tokens


MB = 1024 * 1024


def photo(size):
    return PNG_BYTES + b"\0" * (size - len(PNG_BYTES))


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.hrefs.append(dict(attrs).get("href", ""))


class HomepageRepairTests(unittest.TestCase):
    def setUp(self):
        portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
        self.client = portal.app.test_client()
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        for name in ("sende_oeffentliche_anfrage_benachrichtigung", "schedule_change_backup"):
            mock = patch.object(portal, name)
            mock.start()
            self.addCleanup(mock.stop)
        self.client.get("/privat/schaden")
        self.csrf, self.form_token = session_tokens(self.client)

    def scalar(self, sql, params=()):
        db = portal.get_db()
        try:
            return db.execute(sql, params).fetchone()[0]
        finally:
            db.close()

    def send(self, sizes=()):
        values = base_payload(self.csrf, self.form_token, kontaktweg="telefon")
        values.update(kunde_name="Synthetischer Uploadtest", kunde_email="")
        response = self.client.post("/privat/schaden", data=MultiDict([
            *values.items(),
            *[("dateien", (BytesIO(photo(size)), f"foto-{i}.png"))
              for i, size in enumerate(sizes)],
        ]), content_type="multipart/form-data")
        response.request.close()
        response.request.environ["wsgi.input"].close()
        return response

    def assert_stored(self, sizes):
        order_id = self.scalar("SELECT MAX(id) FROM auftraege")
        files = portal.list_dateien(order_id)
        self.assertEqual([item["size"] for item in files], list(sizes))
        db = portal.get_db()
        try:
            for item, size in zip(files, sizes):
                payload = photo(size)
                self.assertEqual((portal.UPLOAD_DIR / item["stored_name"]).read_bytes(), payload)
                backup = db.execute("SELECT * FROM datei_backups WHERE datei_id=?", (item["id"],)).fetchone()
                self.assertIsNotNone(backup)
                self.assertEqual(backup["size"], size)
                self.assertEqual(backup["file_sha256"], sha256(payload).hexdigest())
                self.assertEqual(base64.b64decode(backup["file_base64"]), payload)
        finally:
            db.close()

    def test_public_portal_buttons_use_working_portal_host(self):
        with patch.object(portal, "PUBLIC_SITE_ONLY", True), patch.object(portal, "PORTAL_BASE_URL", "https://portal.example.test"):
            response = self.client.get("/portale")
            self.assertEqual(response.status_code, 200)
            links = Links()
            links.feed(response.get_data(as_text=True))
            for path in ("/privat/", "/partner", "/versicherung"):
                self.assertIn("https://portal.example.test" + path, links.hrefs)
                self.assertNotIn(path, links.hrefs)
        for path in ("/privat/", "/partner", "/versicherung"):
            self.assertEqual(self.client.get(path).status_code, 200)

    def test_public_upload_shows_actual_limits_and_live_copy(self):
        html = self.client.get("/privat/schaden").get_data(as_text=True)
        self.assertIn("je max. 50 MB / 100 MB gesamt", html)
        self.assertIn("mit Ihrer Schadenmeldung sicher an uns übermittelt", html)
        self.assertNotIn("In dieser Designvorschau bleiben", html)
        self.assertIn('id="remove-files"', html)
        self.assertIn('id="upload-error"', html)

    def test_large_phone_photo_survives_storage_and_database_backup(self):
        sizes = (20 * MB,)
        with patch.object(portal, "build_document_analysis_bundle_safe", side_effect=AssertionError("Public OCR must not run")):
            self.assertEqual(self.send(sizes).status_code, 302)
        self.assert_stored(sizes)

    def test_exact_single_file_limit(self):
        sizes = (50 * MB,)
        self.assertEqual(self.send(sizes).status_code, 302)
        self.assert_stored(sizes)

    def test_ten_files_at_exact_total_limit(self):
        sizes = (10 * MB,) * 10
        self.assertEqual(self.send(sizes).status_code, 302)
        self.assert_stored(sizes)

    def test_over_single_file_limit_does_not_create_case(self):
        before = self.scalar("SELECT COUNT(*) FROM auftraege")
        response = self.send((50 * MB + 1,))
        self.assertEqual(response.status_code, 400)
        self.assertIn("50 MB", response.get_data(as_text=True))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM auftraege"), before)

    def test_over_total_limit_does_not_create_case(self):
        before = self.scalar("SELECT COUNT(*) FROM auftraege")
        response = self.send((40 * MB, 40 * MB, 20 * MB + 1))
        self.assertEqual(response.status_code, 400)
        self.assertIn("100 MB", response.get_data(as_text=True))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM auftraege"), before)

    def test_eleven_files_are_rejected_without_partial_case(self):
        before = self.scalar("SELECT COUNT(*) FROM auftraege")
        response = self.send((len(PNG_BYTES),) * 11)
        self.assertEqual(response.status_code, 400)
        self.assertIn("höchstens 10 Dateien", response.get_data(as_text=True))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM auftraege"), before)

    def test_can_submit_without_files_after_rejected_selection(self):
        before = self.scalar("SELECT COUNT(*) FROM auftraege")
        self.assertEqual(self.send((len(PNG_BYTES),) * 11).status_code, 400)
        self.assertEqual(self.send().status_code, 302)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM auftraege"), before + 1)

    def test_public_request_cap_message(self):
        response = self.client.post("/privat/schaden", data=b"x" * (101 * MB + 1),
                                    content_type="multipart/form-data; boundary=test")
        response.request.environ["wsgi.input"].close()
        self.assertEqual(response.status_code, 413)
        self.assertIn("100 MB", response.get_data(as_text=True))

    def test_internal_and_partner_limits_stay_unchanged(self):
        file = FileStorage(BytesIO(photo(13 * MB)), filename="intern.png")
        self.assertTrue(portal.validate_schadenaufnahme_uploads([file])[1])
        with portal.app.test_request_context("/admin/versicherungsschaden", method="POST"):
            portal.configure_website_upload_limit()
            self.assertEqual(portal.request.max_content_length, 25 * MB)
            html, status = portal.render_schadenaufnahme_assistent()
            self.assertEqual(status, 200)
            self.assertIn("je max. 12 MB / 25 MB gesamt", html)


if __name__ == "__main__":
    unittest.main(verbosity=2)
