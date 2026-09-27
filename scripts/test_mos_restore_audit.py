"""Offline checks for the read-only PostgreSQL/upload restore auditor."""

import base64
from contextlib import redirect_stderr
from hashlib import sha256
from io import StringIO
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_mos_restore import (
    RestoreAuditError, _decode_checked, compare_manifest, main, upload_inventory,
    verify_signed_documents,
)


class RestoreAuditTests(unittest.TestCase):
    def test_upload_inventory_detects_changed_and_missing_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source"
            restored = Path(temporary) / "restored"
            source.mkdir()
            restored.mkdir()
            for directory in (source, restored):
                (directory / "synthetic-a.bin").write_bytes(b"first synthetic upload")
                (directory / "synthetic-b.bin").write_bytes(b"second synthetic upload")
            baseline, names = upload_inventory(source)
            actual, restored_names = upload_inventory(restored)
            self.assertEqual(names, restored_names)
            self.assertEqual(baseline, actual)
            (restored / "synthetic-b.bin").write_bytes(b"corrupted synthetic upload")
            self.assertNotEqual(baseline, upload_inventory(restored)[0])
            (restored / "synthetic-b.bin").unlink()
            self.assertNotEqual(baseline, upload_inventory(restored)[0])

    def test_upload_inventory_rejects_nested_or_linked_entries(self):
        with tempfile.TemporaryDirectory() as temporary:
            uploads = Path(temporary) / "uploads"
            uploads.mkdir()
            (uploads / "nested").mkdir()
            with self.assertRaises(RestoreAuditError):
                upload_inventory(uploads)

    def test_signed_pdf_digest_and_type_are_checked(self):
        pdf = b"%PDF-1.4\nsynthetic only\n%%EOF"
        encoded = base64.b64encode(pdf).decode("ascii")
        digest = sha256(pdf).hexdigest()
        self.assertEqual(pdf, _decode_checked(encoded, digest, "synthetic", prefix=b"%PDF"))
        with self.assertRaises(RestoreAuditError):
            _decode_checked(encoded, "0" * 64, "synthetic", prefix=b"%PDF")
        with self.assertRaises(RestoreAuditError):
            _decode_checked(base64.b64encode(b"not a pdf").decode("ascii"), digest,
                            "synthetic", prefix=b"%PDF")

    def test_incomplete_historic_contract_fails_explicitly(self):
        class Rows:
            def execute(self, statement):
                return [('{}', sha256(b'{}').hexdigest(), '',
                         base64.b64encode(b'%PDF-1.4 synthetic').decode('ascii'),
                         sha256(b'%PDF-1.4 synthetic').hexdigest(), 'synthetic-hold')]

        with self.assertRaisesRegex(RestoreAuditError, 'ohne gespeicherte PNG-Unterschrift'):
            verify_signed_documents(Rows(), {'miet_checkout_contracts'})

    def test_connection_failure_never_prints_secret(self):
        import psycopg

        output = StringIO()
        with patch.dict(os.environ, {
                'MOS_RESTORE_AUDIT_DATABASE_URL': 'postgresql://user:SECRET_VALUE@127.0.0.1/db',
                'MOS_RESTORE_AUDIT_UPLOAD_DIR': tempfile.gettempdir()}), \
                patch.object(psycopg, 'connect', side_effect=RuntimeError('SECRET_VALUE')), \
                redirect_stderr(output):
            self.assertEqual(main(['--expected-database', 'synthetic',
                                   '--output', str(Path(tempfile.gettempdir()) / 'never-written.json')]), 1)
        self.assertNotIn('SECRET_VALUE', output.getvalue())
        self.assertIn('RESTORE_AUDIT_FAILED', output.getvalue())

    def test_manifest_comparison_fails_closed_on_any_component(self):
        baseline = {"format_version": 1, "tables": {"miet_checkout_holds": {"rows": 1}},
                    "sequences": {"example_id_seq": {"last": 1}},
                    "signed_documents": {"mos_contract_pdfs": 1},
                    "uploads": {"count": 1, "sha256": "x"}, "upload_references_checked": 1}
        compare_manifest(dict(baseline), baseline)
        for field in ("tables", "sequences", "signed_documents", "uploads",
                      "upload_references_checked"):
            changed = dict(baseline)
            changed[field] = None
            with self.subTest(field=field), self.assertRaises(RestoreAuditError):
                compare_manifest(changed, baseline)


if __name__ == "__main__":
    unittest.main()
