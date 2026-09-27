"""Offline regression checks for invoice sources; no Flask app or live access."""

import contextlib
import json
import pathlib
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import werkstatt_rechnungsquelle as reader

try:
    import fitz
except ImportError:
    fitz = None


VOUCHER = "7c082c15-eae1-4457-9dc7-7cf70fe08e3f"
SALES_VOUCHER = "8c082c15-eae1-4457-9dc7-7cf70fe08e3f"
FILE = "7d083c15-eae1-4457-9dc7-7cf70fe08e3f"
TOKEN = "synthetic-test-key-not-a-live-secret"
TEXT = "Klebeband gruen 50 mm 2 Rollen 3,45 EUR. Lieferposition der Werkstatt: Abdeckband zum Lackieren."


class Response:
    def __init__(self, payload=b"", status=200, headers=None, chunks=None):
        self.status_code = status
        self.headers = headers or {}
        self.payload = payload
        self.chunks = chunks
        self.closed = False

    def iter_content(self, chunk_size):
        if self.chunks is not None:
            yield from self.chunks
        else:
            for start in range(0, len(self.payload), chunk_size):
                yield self.payload[start:start + chunk_size]

    def close(self):
        self.closed = True


class InvoiceSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="test-rechnungsquelle-")
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.dbfile = self.root / "source.db"
        with contextlib.closing(sqlite3.connect(self.dbfile)) as db:
            db.executescript("""
                CREATE TABLE einkauf_belege (
                    id INTEGER, beleg_typ TEXT, lieferant TEXT, original_name TEXT,
                    stored_name TEXT, extrahierter_text TEXT, status TEXT
                );
                CREATE TABLE lexware_rechnungen (
                    id INTEGER, voucher_id TEXT, voucher_type TEXT,
                    voucher_status TEXT, status TEXT, contact_name TEXT,
                    voucher_number TEXT
                );
            """)
        self.calls = []
        self.responses = []
        self.parser = mock.Mock(side_effect=lambda text, filename="": [{
            "produkt_name": "Klebeband gruen 50 mm", "artikelnummer": "K50",
            "stueckzahl": 2, "ve": "Rolle", "preis": "3,45", "kategorie": "Klebeband",
            "produkt_beschreibung": "Produkt\nIBAN DE02120300000000202051\n2 Rollen",
        }])
        self.vision = mock.Mock(return_value=[])
        self.portal = types.SimpleNamespace(
            get_db=self.get_db, UPLOAD_DIR=self.root,
            get_fitz=lambda: fitz,
            extract_einkauf_beleg_positions=self.parser,
            extract_einkauf_beleg_positions_openai=self.vision,
            get_requests=lambda: types.SimpleNamespace(get=self.get),
            LEXWARE_API_KEY=TOKEN, LEXWARE_API_BASE_URL="https://api.lexware.io",
        )
        self.throttle = mock.patch.object(reader, "_throttle")
        self.throttle.start()
        self.addCleanup(self.throttle.stop)

    def get_db(self):
        db = sqlite3.connect(self.dbfile)
        db.row_factory = sqlite3.Row
        return db

    def seed_local(self, source_id=1, supplier="Supplier A", filename="invoice.pdf", text="", kind="rechnung"):
        with contextlib.closing(self.get_db()) as db:
            db.execute("INSERT INTO einkauf_belege VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (source_id, kind, supplier, "Invoice.pdf", filename, text, "importiert"))
            db.commit()

    def seed_lexware(self, voucher=VOUCHER, kind="purchaseinvoice", status="open"):
        with contextlib.closing(self.get_db()) as db:
            db.execute("INSERT INTO lexware_rechnungen VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (42, voucher, kind, status, "offen", "Actual Supplier", "Invoice-7"))
            db.commit()

    def get(self, url, **kwargs):
        self.assertTrue(url.startswith("https://api.lexware.io/v1/"))
        self.assertFalse(kwargs["allow_redirects"])
        self.assertTrue(kwargs["stream"])
        self.assertEqual(kwargs["timeout"], (5, 30))
        self.assertEqual(kwargs["headers"]["Authorization"], f"Bearer {TOKEN}")
        self.calls.append(url)
        if not self.responses:
            raise AssertionError("Unplanned request; tests never contact the network")
        return self.responses.pop(0)

    def metadata(self, status=200, **overrides):
        payload = {
            "id": VOUCHER, "type": "purchaseinvoice", "voucherStatus": "open",
            "files": [FILE], "totalGrossAmount": 99887766,
            "paymentStatus": "private-bookkeeping-marker",
        }
        payload.update(overrides)
        return Response(json.dumps(payload).encode(), status=status)

    def pdf(self, pages=1):
        with fitz.open() as document:
            for number in range(pages):
                page = document.new_page()
                page.insert_text((50, 50), f"{TEXT} Seite {number + 1}")
            return document.tobytes()

    def test_unknown_uuid_and_sales_denied_before_network(self):
        self.seed_lexware(SALES_VOUCHER, kind="salesinvoice")
        for source_id in (SALES_VOUCHER, VOUCHER, "42", "../../invoice"):
            with self.subTest(source_id=source_id):
                result = reader.read_source(self.portal, "lexware", source_id)
                self.assertEqual(result["status"], "unavailable")
                self.assertFalse(result["candidates"])
        self.assertEqual(self.calls, [])

    def test_void_source_denied_before_network(self):
        self.seed_lexware(status="voided")
        result = reader.read_source(self.portal, "lexware", VOUCHER)
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(self.calls, [])

    def test_stored_text_has_no_invented_supplier_or_page_coverage(self):
        self.seed_local(supplier="", filename="", text=TEXT)
        result = reader.read_source(self.portal, "einkauf", 1)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["source"]["supplier"], "")
        self.assertEqual(result["candidates"][0]["lieferant"], "")
        self.assertIsNone(result["candidates"][0]["source"]["page"])
        self.assertIsNone(result["coverage"]["pages_total"])
        self.assertFalse(result["coverage"]["complete"])
        self.assertFalse(result["coverage"]["extraction_verified"])
        self.vision.assert_not_called()

    @unittest.skipUnless(fitz, "PyMuPDF is required for synthetic PDF coverage")
    def test_uuid_lookup_products_only_and_download_cleanup(self):
        self.seed_lexware()
        meta = self.metadata()
        binary = Response(self.pdf())
        self.responses = [meta, binary]
        created = []
        original_tempdir = tempfile.TemporaryDirectory

        def record_tempdir(*args, **kwargs):
            directory = original_tempdir(*args, **kwargs)
            created.append(pathlib.Path(directory.name))
            return directory

        with mock.patch.object(reader.tempfile, "TemporaryDirectory", side_effect=record_tempdir):
            result = reader.read_source(self.portal, "lexware", VOUCHER)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["source"]["id"], VOUCHER)
        self.assertEqual(result["source"]["supplier"], "Actual Supplier")
        self.assertEqual(self.calls, [f"https://api.lexware.io/v1/vouchers/{VOUCHER}", f"https://api.lexware.io/v1/files/{FILE}"])
        self.assertTrue(meta.closed and binary.closed)
        self.assertTrue(created)
        self.assertTrue(all(not path.exists() for path in created))
        candidate = result["candidates"][0]
        self.assertEqual(candidate["source"]["page"], 1)
        self.assertEqual(candidate["source"]["file_id"], FILE)
        self.assertEqual(candidate["price_evidence"], {"value": "3,45", "basis": "unknown", "verified": False})
        self.assertFalse(candidate["price_verified"])
        serialized = json.dumps(result)
        for denied in ("IBAN", "DE021203", "99887766", "paymentStatus", "private-bookkeeping-marker", TOKEN):
            self.assertNotIn(denied, serialized)
        self.vision.assert_not_called()

    @unittest.skipUnless(fitz, "PyMuPDF is required for synthetic PDF coverage")
    def test_every_page_is_processed_to_explicit_cap_and_text_precedes_ai(self):
        self.seed_local()
        (self.root / "invoice.pdf").write_bytes(self.pdf(31))
        result = reader.read_source(self.portal, "einkauf", 1)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["coverage"]["pages_total"], 31)
        self.assertEqual(result["coverage"]["pages_read"], 30)
        self.assertEqual(result["coverage"]["pages_attempted"], 30)
        self.assertFalse(result["coverage"]["complete"])
        self.assertEqual([item["source"]["page"] for item in result["candidates"]], list(range(1, 31)))
        self.assertEqual(self.parser.call_count, 30)
        self.vision.assert_not_called()
        self.assertTrue(any("Seitengrenze" in warning for warning in result["warnings"]))

    def test_non_success_and_redirect_metadata_are_sanitized(self):
        self.seed_lexware()
        for status in (302, 401, 403, 404, 429, 500):
            with self.subTest(status=status):
                response = Response((TOKEN + " private-provider-error").encode(), status=status,
                                    headers={"Location": "https://not-allowed.invalid/private"})
                self.responses = [response]
                result = reader.read_source(self.portal, "lexware", VOUCHER)
                self.assertEqual(result["status"], "unavailable")
                self.assertFalse(result["candidates"])
                serialized = json.dumps(result)
                for denied in (TOKEN, "private-provider-error", "not-allowed.invalid"):
                    self.assertNotIn(denied, serialized)
                self.assertTrue(response.closed)
        self.assertEqual(len(self.calls), 6)

    def test_non_success_file_does_not_follow_redirect(self):
        self.seed_lexware()
        response = Response(TOKEN.encode(), status=302, headers={"Location": "https://not-allowed.invalid"})
        self.responses = [self.metadata(), response]
        result = reader.read_source(self.portal, "lexware", VOUCHER)
        self.assertEqual(result["status"], "unavailable")
        self.assertFalse(result["coverage"]["complete"])
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(response.closed)
        self.assertNotIn(TOKEN, json.dumps(result))

    def test_remote_type_or_identity_mismatch_denied_before_file(self):
        self.seed_lexware()
        for overrides in ({"type": "salesinvoice"}, {"id": SALES_VOUCHER}, {"voucherStatus": "voided"}, {"files": []}):
            with self.subTest(overrides=overrides):
                self.responses = [self.metadata(**overrides)]
                result = reader.read_source(self.portal, "lexware", VOUCHER)
                self.assertEqual(result["status"], "unavailable")
                self.assertFalse(result["candidates"])
        self.assertEqual(len(self.calls), 4)

    def test_oversized_file_and_metadata_are_rejected_and_closed(self):
        self.seed_lexware()
        binary = Response(headers={"Content-Length": str(reader.MAX_FILE_BYTES + 1)})
        self.responses = [self.metadata(), binary]
        result = reader.read_source(self.portal, "lexware", VOUCHER)
        self.assertEqual(result["status"], "unavailable")
        self.assertTrue(binary.closed)
        metadata = Response(chunks=[b"x" * (1024 * 1024), b"x"])
        self.responses = [metadata]
        result = reader.read_source(self.portal, "lexware", VOUCHER)
        self.assertEqual(result["status"], "unavailable")
        self.assertTrue(metadata.closed)

    def test_other_host_rejected_without_request(self):
        self.seed_lexware()
        self.portal.LEXWARE_API_BASE_URL = "https://not-allowed.invalid"
        result = reader.read_source(self.portal, "lexware", VOUCHER)
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
