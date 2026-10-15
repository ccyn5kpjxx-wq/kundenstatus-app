"""Isolated upload regressions; uses synthetic photos and disables all notifications."""

from io import BytesIO
from pathlib import Path
import unittest
from unittest.mock import patch

from werkzeug.datastructures import FileStorage, MultiDict

from test_homepage_kurzanfrage import PNG_1X1, portal


MB = 1024 * 1024


def photo(size):
    return PNG_1X1 + b"\0" * (size - len(PNG_1X1))


class HomepagePhotoTests(unittest.TestCase):
    def setUp(self):
        portal.app.config["TESTING"] = True
        self.client = portal.app.test_client()
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        for name in ("sende_website_lead_benachrichtigung", "notify_lead_workshop",
                     "lead_notice", "schedule_change_backup"):
            mock = patch.object(portal, name)
            mock.start()
            self.addCleanup(mock.stop)
        self.client.get("/anfrage?anliegen=lackanfrage")
        with self.client.session_transaction() as session:
            self.csrf = session[portal.CSRF_FIELD_NAME]

    def count(self):
        db = portal.get_db()
        try:
            return db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        finally:
            db.close()

    def send(self, sizes=(), anliegen="lackanfrage"):
        response = self.client.post("/anfrage", data=MultiDict([
            (portal.CSRF_FIELD_NAME, self.csrf),
            ("anliegen", anliegen), ("name", "Upload Test"),
            ("telefon", "01711234567"), ("nachricht", "Synthetischer Test"),
            *[("bilder", (BytesIO(photo(size)), f"foto-{i}.png"))
              for i, size in enumerate(sizes)],
        ]), content_type="multipart/form-data")
        response.request.close()
        response.request.environ["wsgi.input"].close()
        return response

    def latest_files(self):
        db = portal.get_db()
        try:
            lead_id = db.execute("SELECT MAX(id) FROM leads").fetchone()[0]
        finally:
            db.close()
        return portal.list_lead_dateien(lead_id)

    def test_large_phone_photos_survive_request_and_storage(self):
        # Exceeds both former single-file and whole-request limits.
        sizes = (15 * MB, 16 * MB)
        self.assertEqual(self.send(sizes).status_code, 302)
        files = self.latest_files()
        self.assertEqual([f["size"] for f in files], list(sizes))
        for item, size in zip(files, sizes):
            self.assertEqual((portal.UPLOAD_DIR / item["stored_name"]).read_bytes(), photo(size))

    def test_exact_single_photo_limit(self):
        self.assertEqual(self.send((50 * MB,)).status_code, 302)
        self.assertEqual(self.latest_files()[0]["size"], 50 * MB)

    def test_five_photos_at_exact_total_limit(self):
        self.assertEqual(self.send((20 * MB,) * 5).status_code, 302)
        self.assertEqual(sum(f["size"] for f in self.latest_files()), 100 * MB)

    def test_single_photo_over_limit_has_no_lead(self):
        before = self.count()
        response = self.send((50 * MB + 1,))
        self.assertEqual(response.status_code, 400)
        self.assertIn("50 MB", response.get_data(as_text=True))
        self.assertEqual(self.count(), before)

    def test_total_over_limit_has_no_lead(self):
        before = self.count()
        response = self.send((40 * MB, 40 * MB, 20 * MB + 1))
        self.assertEqual(response.status_code, 400)
        self.assertIn("100 MB", response.get_data(as_text=True))
        self.assertEqual(self.count(), before)

    def test_request_cap_uses_homepage_message(self):
        before = self.count()
        response = self.client.post("/anfrage", data=b"x" * (101 * MB + 1),
                                    content_type="multipart/form-data; boundary=test")
        self.assertEqual(response.status_code, 413)
        self.assertIn("100 MB", response.get_data(as_text=True))
        self.assertEqual(self.count(), before)

    def test_without_photos_in_both_forms(self):
        for anliegen in ("lackanfrage", "dellenreparatur"):
            with self.subTest(anliegen=anliegen):
                before = self.count()
                self.assertEqual(self.send(anliegen=anliegen).status_code, 302)
                self.assertEqual(self.count(), before + 1)
                self.assertEqual(self.latest_files(), [])

    def test_invalid_photo_then_without_photos(self):
        before = self.count()
        self.assertEqual(self.send((50 * MB + 1,)).status_code, 400)
        self.assertEqual(self.send().status_code, 302)
        self.assertEqual(self.count(), before + 1)

    def test_photo_count_and_signature_still_checked(self):
        before = self.count()
        self.assertEqual(self.send((len(PNG_1X1),) * 6).status_code, 400)
        with patch(__name__ + ".photo", return_value=b"not a PNG"):
            self.assertEqual(self.send((100,)).status_code, 400)
        self.assertEqual(self.count(), before)

    def test_storage_failure_rolls_back_lead_and_photos(self):
        before = self.count()
        paths = set(portal.UPLOAD_DIR.glob("lead-*"))
        real_save = portal.save_lead_upload_file
        calls = []

        def fail_second(*args, **kwargs):
            calls.append(True)
            if len(calls) == 2:
                raise OSError("simulated disk failure")
            return real_save(*args, **kwargs)

        with patch.object(portal, "save_lead_upload_file", side_effect=fail_second):
            self.assertEqual(self.send((len(PNG_1X1),) * 2).status_code, 503)
        self.assertEqual(self.count(), before)
        self.assertEqual(set(portal.UPLOAD_DIR.glob("lead-*")), paths)

    def test_other_uploads_keep_their_limits(self):
        file = FileStorage(BytesIO(photo(13 * MB)), filename="workshop.png")
        self.assertTrue(portal.validate_website_lead_uploads([file])[1])
        with self.assertRaises(ValueError):
            portal.save_lead_attachment_bytes(0, "workshop.png", photo(13 * MB))
        self.assertEqual(portal.app.config["MAX_CONTENT_LENGTH"], 25 * MB)
        with portal.app.test_request_context("/admin", method="POST"):
            portal.configure_website_upload_limit()
            self.assertEqual(portal.request.max_content_length, 25 * MB)

    def test_partial_disk_write_is_removed(self):
        before = self.count()
        paths = set(portal.UPLOAD_DIR.glob("lead-*"))
        write_bytes = Path.write_bytes

        def fail_write(path, payload):
            if path.name.startswith("lead-"):
                write_bytes(path, payload[:10])
                raise OSError("simulated partial write")
            return write_bytes(path, payload)

        with patch.object(Path, "write_bytes", fail_write):
            self.assertEqual(self.send((len(PNG_1X1),)).status_code, 503)
        self.assertEqual(self.count(), before)
        self.assertEqual(set(portal.UPLOAD_DIR.glob("lead-*")), paths)

    def test_both_forms_publish_matching_limits(self):
        for anliegen in ("lackanfrage", "dellenreparatur"):
            html = self.client.get("/anfrage?anliegen=" + anliegen).get_data(as_text=True)
            for marker in ('data-max-file-mb="50"', 'data-max-total-mb="100"',
                           "je max. 50 MB", "zusammen max. 100 MB",
                           "auch ohne Bilder senden", "homepage-anfrage-upload.js"):
                self.assertIn(marker, html)


if __name__ == "__main__":
    unittest.main(verbosity=2)
