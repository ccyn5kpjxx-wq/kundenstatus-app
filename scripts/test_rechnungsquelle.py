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
                    voucher_number TEXT, voucher_date TEXT
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
            db.execute("INSERT INTO lexware_rechnungen VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (42, voucher, kind, status, "offen", "Actual Supplier", "Invoice-7", "2026-09-12T00:00:00.000+02:00"))
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

    def test_quarantined_or_payment_metadata_never_opens_file_restore_or_stored_ocr(self):
        self.seed_local(source_id=1, supplier='TOP-Color GmbH', filename='forbidden.pdf', text='PRIVATE STORED TEXT', kind='gesperrt')
        self.seed_local(source_id=2, supplier='TOP-Color GmbH', filename='forbidden.pdf', text='PRIVATE STORED TEXT')
        with contextlib.closing(self.get_db()) as db:
            db.execute("UPDATE einkauf_belege SET original_name='SEPA-Mandat.pdf' WHERE id=2")
            db.commit()
        self.portal.assistant_mail_sources_restore_file = mock.Mock()
        with mock.patch.object(reader, '_read_file') as read, mock.patch.object(reader, '_stored_text') as fallback:
            for source_id in (1, 2):
                result = reader.read_source(self.portal, 'einkauf', source_id)
                self.assertEqual(result['status'], 'unavailable')
                self.assertEqual(result['candidates'], [])
                self.assertNotIn('PRIVATE STORED TEXT', json.dumps(result))
            read.assert_not_called()
            fallback.assert_not_called()
        self.portal.assistant_mail_sources_restore_file.assert_not_called()
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

    def test_legacy_default_and_package_counts_are_not_invoice_quantity_evidence(self):
        for raw in (None, 0, 1, 25, -1, True, 'NaN', '1.000', 'unknown'):
            for description in ('Klebeband', 'Schleifpapier 25 Stück/Pack', 'Menge: unbekannt'):
                position = {'produkt_name': 'Schleifpapier 25 Stück/Pack', 'stueckzahl': raw,
                            've': 'Pack', 'produkt_beschreibung': description}
                item = reader._candidate(position, {'supplier': 'Test Supplier'}, 'file', 1, 'a' * 64)
                self.assertIsNone(item['stueckzahl'])
                self.assertEqual(item['quantity_evidence']['basis'], 'unknown')
                self.assertEqual(item['package_evidence']['value'], '25')
                self.assertEqual(item['package_evidence']['per_unit'], 'Pack')
                self.assertEqual(item['source']['quantity_version'], 1)

    def test_explicit_decimal_invoice_quantity_is_distinct_from_package_content(self):
        position = {'produkt_name': 'Schleifpapier 25 Stück/Pack', 'stueckzahl': 25, 've': 'Pack',
                    'produkt_beschreibung': 'Menge: 2 Pack; IBAN DE02120300000000202051',
                    'quelle': 'Schleifpapier 25 Stück/Pack Menge: 2 Pack'}
        item = reader._candidate(position, {'supplier': 'Test Supplier'}, 'file', 1, 'a' * 64)
        self.assertEqual(item['stueckzahl'], '2')
        self.assertEqual(item['quantity_evidence']['unit'], 'Pack')
        self.assertEqual(item['quantity_evidence']['source_field'], 'Menge')
        self.assertEqual(item['package_evidence']['value'], '25')
        self.assertNotIn('DE021203', json.dumps(item))
        for text, expected in (('Menge: 2,5 Liter', '2.5'), ('Menge: 1.000 Pack', None),
                               ('Menge: -2 Pack', None), ('Menge: 0 Pack', None)):
            position.update(quelle=text, produkt_beschreibung=text)
            self.assertEqual(reader._candidate(position, {'supplier': 'Test Supplier'}, 'file', 1, 'digest')['stueckzahl'], expected)
        position.update(quelle='Menge: 2 Pack', produkt_beschreibung='Menge: 3 Pack')
        self.assertIsNone(reader._candidate(position, {'supplier': 'Test Supplier'}, 'file', 1, 'digest')['stueckzahl'])

    def test_invoice_date_requires_named_source_not_import_or_arbitrary_date(self):
        self.seed_local(filename='', text='Rechnungsdatum: 14.09.2026\n' + TEXT)
        result = reader.read_source(self.portal, 'einkauf', 1)
        self.assertEqual(result['source']['date'], '2026-09-14')
        self.assertEqual(result['candidates'][0]['source']['date'], '2026-09-14')
        for value in ('31.02.2026', 'yesterday', 'Bank 14.09.2026', None):
            self.assertIsNone(reader.invoice_date(value))
        clean = reader._result('einkauf', 1)
        reader._date_from_text('Importdatum: 14.09.2026', clean)
        self.assertIsNone(clean['source'].get('date'))

    @unittest.skipUnless(fitz, "PyMuPDF is required for synthetic PDF coverage")
    def test_missing_original_restores_exact_local_blob_before_text_fallback(self):
        self.seed_local(filename='invoice.pdf')
        def restore(name):
            self.assertEqual(name, 'invoice.pdf')
            (self.root / name).write_bytes(self.pdf())
            return True
        self.portal.assistant_mail_sources_restore_file = mock.Mock(side_effect=restore)
        result = reader.read_source(self.portal, 'einkauf', 1)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['coverage']['files_read'], 1)
        self.portal.assistant_mail_sources_restore_file.assert_called_once_with('invoice.pdf')
        self.assertEqual(self.calls, [])
        reader.read_source(self.portal, 'einkauf', 1)
        self.portal.assistant_mail_sources_restore_file.assert_called_once()

    def test_restore_unknown_corrupt_or_unsafe_original_never_uses_network_or_new_receipt(self):
        for index, name in enumerate(('../outside.pdf', 'nested/invoice.pdf', 'missing.pdf'), 1):
            self.seed_local(source_id=index, filename=name, text=TEXT)
        restore = self.portal.assistant_mail_sources_restore_file = mock.Mock(return_value=False)
        for index in (1, 2):
            self.assertEqual(reader.read_source(self.portal, 'einkauf', index)['status'], 'partial')
        restore.assert_not_called()
        self.assertEqual(reader.read_source(self.portal, 'einkauf', 3)['status'], 'partial')
        restore.assert_called_once_with('missing.pdf')
        restore.side_effect = ValueError('private-backup-marker')
        result = reader.read_source(self.portal, 'einkauf', 3)
        self.assertEqual(result['status'], 'partial')
        self.assertTrue(any('Sicherung' in warning for warning in result['warnings']))
        self.assertNotIn('private-backup-marker', json.dumps(result))
        self.assertEqual(self.calls, [])
        with contextlib.closing(self.get_db()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM einkauf_belege').fetchone()[0], 3)

    def test_staged_webp_original_uses_existing_image_reader_and_cleans_temp_file(self):
        from PIL import Image
        Image.new('RGB', (2, 2), color='white').save(self.root / 'invoice.webp', format='WEBP')
        self.seed_local(filename='invoice.webp')
        paths = []
        def local_text(path, filename):
            self.assertEqual(path.suffix, '.webp')
            self.assertTrue(path.is_file())
            paths.append(path)
            return TEXT
        self.portal.extract_document_text_local = local_text
        result = reader.read_source(self.portal, 'einkauf', 1)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['coverage']['pages_read'], 1)
        self.assertEqual(len(result['candidates']), 1)
        self.assertTrue(paths and all(not path.exists() for path in paths))
        self.assertTrue((self.root / 'invoice.webp').is_file())
        self.assertEqual(self.calls, [])

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
        self.assertEqual(candidate["source"]["date"], '2026-09-12')
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

    def test_conflicting_later_page_clears_date_on_all_earlier_candidates(self):
        self.seed_local()
        with fitz.open() as document:
            for value in ('01.09.2026', '02.09.2026'):
                page = document.new_page()
                page.insert_text((50, 50), 'Rechnungsdatum ' + value + '\nSynthetic product invoice')
            document.save(self.root / 'invoice.pdf')
        result = reader.read_source(self.portal, 'einkauf', 1)
        self.assertIsNone(result['source']['date'])
        self.assertEqual(len(result['candidates']), 2)
        self.assertTrue(all(row['source']['date'] is None for row in result['candidates']))
        self.assertNotIn('_invoice_dates', result)
        self.assertFalse(self.calls)


if __name__ == "__main__":
    unittest.main()
