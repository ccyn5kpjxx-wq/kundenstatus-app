"""Isolated catalog tests: temporary SQLite, synthetic receipts, no app/network."""
import importlib.util
import json
import pathlib
import sqlite3
import sys
import tempfile
import threading
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
if importlib.util.find_spec('werkstatt_rechnungsquelle') is None:
    reader = types.ModuleType('werkstatt_rechnungsquelle')
    reader.read_source = lambda *args: (_ for _ in ()).throw(AssertionError('Unmocked reader'))
    sys.modules['werkstatt_rechnungsquelle'] = reader

from werkstatt_artikel_import import InvoiceCatalog


class FakePortal:
    def __init__(self, path):
        self.path = path

    def get_db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        return db


def inventory():
    return {'einkaufsbelege': [{'id': 1, 'lieferant': 'Test Supplier', 'original_name': 'invoice.pdf'}],
            'lieferantenrechnungen': []}


def candidate(**changes):
    row = {'produkt_name': 'Klebeband grün', 'artikelnummer': 'AB-12', 've': 'Rolle',
           'gebinde': '30 mm x 50 m', 'farbe': 'grün', 'stueckzahl': 2,
           'preis': '12,50', 'source': {'page': 1, 'line': 1}}
    row.update(changes)
    return row


def extracted(*candidates):
    return {'status': 'ok', 'candidates': list(candidates), 'coverage': {'complete': True}, 'warnings': []}


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.portal = FakePortal(str(pathlib.Path(self.temp.name) / 'catalog.db'))
        self.catalog = InvoiceCatalog(self.portal)
        self.catalog.prepare(inventory())

    def tearDown(self):
        self.temp.cleanup()

    def update(self, sql, args=()):
        db = self.portal.get_db()
        try:
            db.execute(sql, args)
            db.commit()
        finally:
            db.close()

    def process(self, result):
        with patch('werkstatt_artikel_import.read_source', return_value=result):
            return self.catalog.process_next()

    def test_prepare_and_completed_processing_are_idempotent(self):
        self.assertEqual(len(self.catalog.prepare(inventory())['quellen']), 1)
        self.process(extracted(candidate()))
        with patch('werkstatt_artikel_import.read_source', side_effect=AssertionError('Already done')):
            self.assertEqual(self.catalog.process_next()['vorschlaege'], 1)
        # Even an explicit retry of the same extraction cannot duplicate it.
        self.update("UPDATE assistent_rechnungsimporte SET state='offen'")
        self.assertEqual(self.process(extracted(candidate()))['vorschlaege'], 1)

    def test_new_sources_prioritize_requested_suppliers_without_aliasing(self):
        suppliers = ['Other Supplier', 'Tech Masters', 'Car-Parts', 'Topcolor', 'Auto-Color']
        rows = [{'id': index, 'lieferant': supplier, 'original_name': 'invoice.pdf'}
                for index, supplier in enumerate(suppliers, 2)]
        self.catalog.prepare({'einkaufsbelege': rows, 'lieferantenrechnungen': []})
        stored = self.catalog.rows('SELECT supplier FROM assistent_rechnungsimporte WHERE source_id!=? ORDER BY id', ('1',))
        self.assertEqual([row['supplier'] for row in stored],
                         ['Topcolor', 'Auto-Color', 'Car-Parts', 'Tech Masters', 'Other Supplier'])

    def test_complete_page_coverage_without_products_still_requires_review(self):
        report = self.process(extracted())
        self.assertEqual(report['quellen'][0]['state'], 'pruefen')
        self.assertEqual(report['vorschlaege'], 0)
        self.assertTrue(report['quellen'][0]['result']['abdeckung']['complete'])
        self.assertIn('Keine Produktpositionen', ' '.join(report['quellen'][0]['result']['hinweise']))

    def test_exact_variants_supplier_and_source_rows_survive(self):
        rows = [candidate(), candidate(gebinde='50 mm x 50 m'), candidate(farbe='rot'),
                candidate(groesse='30 cm'), candidate(source={'page': 1, 'line': 8})]
        self.process(extracted(*rows))
        stored = self.catalog.rows('SELECT identity_key,payload_json FROM assistent_rechnungsartikel ORDER BY id')
        self.assertEqual(len(stored), 5)
        self.assertEqual(len({r['identity_key'] for r in stored}), 4)
        payloads = [json.loads(r['payload_json']) for r in stored]
        self.assertEqual(payloads[1]['gebinde'], '50 mm x 50 m')
        self.assertEqual(payloads[2]['farbe'], 'rot')
        self.assertEqual(payloads[3]['groesse'], '30 cm')
        self.assertEqual(payloads[0]['menge'], '2')
        self.assertEqual(payloads[4]['quelle']['zeile'], 8)
        other = inventory()
        other['einkaufsbelege'].append({'id': 2, 'lieferant': 'Other Supplier', 'original_name': 'second.pdf'})
        self.catalog.prepare(other)
        self.process(extracted(candidate()))
        keys = self.catalog.rows('SELECT identity_key FROM assistent_rechnungsartikel ORDER BY id')
        self.assertNotEqual(keys[0]['identity_key'], keys[-1]['identity_key'])

    def test_identical_positions_without_line_are_not_silently_collapsed(self):
        report = self.process(extracted(candidate(source={'page': 1}), candidate(source={'page': 1})))
        self.assertEqual(report['vorschlaege'], 2)
        rows = self.catalog.search('Klebeband')
        self.assertEqual({r['quelle']['extraktionsindex'] for r in rows}, {1, 2})
        self.assertTrue(all(r['quelle']['zeile'] is None for r in rows))

    def test_proposals_never_become_orders_and_extra_fields_are_not_persisted(self):
        self.process(extracted(candidate(bank_account='secret-bank', amount_total='secret-total', status='bestellt')))
        article = self.catalog.search('Klebeband')[0]
        self.assertEqual(article['status'], 'vorschlag')
        self.assertFalse(article['bestellbar'])
        self.assertFalse(article['preis_geprueft'])
        self.assertNotIn('secret-', json.dumps(article))
        tables = self.catalog.rows("SELECT name FROM sqlite_master WHERE type='table'")
        self.assertNotIn('einkaufsliste', {r['name'] for r in tables})
        self.assertNotIn('einkauf_artikel', {r['name'] for r in tables})

    def test_missing_provenance_remains_unknown(self):
        report = self.process(extracted(candidate(source=None)))
        self.assertEqual(report['quellen'][0]['state'], 'pruefen')
        self.assertFalse(report['quellen'][0]['result']['abdeckung']['complete'])
        self.assertEqual(self.catalog.search('Klebeband')[0]['quelle']['positionsnachweis'], 'ungeklaert')

    def test_file_provenance_and_coverage_are_retained_without_extra_metadata(self):
        result = extracted(candidate(source={'page': 2, 'file_id': 'attachment-A', 'sha256': 'a' * 64}))
        result['coverage'].update({'files_total': 2, 'pages_read': 3, 'bank_field': 'private-secret',
                                  'files': [{'file_id': 'attachment-A', 'pages_read': 2, 'complete': True,
                                             'bank_field': 'private-secret'}]})
        report = self.process(result)
        source = self.catalog.search('Klebeband')[0]['quelle']
        self.assertEqual(source['datei_id'], 'attachment-A')
        self.assertEqual(source['datei_sha256'], 'a' * 64)
        self.assertEqual(source['seite'], 2)
        coverage = report['quellen'][0]['result']['abdeckung']
        self.assertEqual(coverage['files'][0]['pages_read'], 2)
        self.assertEqual(coverage['files_total'], 2)
        self.assertNotIn('private-secret', json.dumps(report))

    def test_malformed_reader_output_finishes_with_review(self):
        for result in (None, [], {'candidates': None}, extracted(None, ['bad'], candidate())):
            with self.subTest(result=result):
                self.update("UPDATE assistent_rechnungsimporte SET state='offen'")
                report = self.process(result)
                self.assertEqual(report['laeuft'], 0)
                self.assertEqual(report['offen'], 0)
                self.assertEqual(report['quellen'][0]['state'], 'pruefen')

    def test_reader_failure_does_not_strand_lease_or_leak_exception(self):
        with patch('werkstatt_artikel_import.read_source', side_effect=RuntimeError('private-secret')):
            report = self.catalog.process_next()
        self.assertEqual(report['laeuft'], 0)
        self.assertEqual(report['quellen'][0]['state'], 'pruefen')
        self.assertNotIn('private-secret', json.dumps(report))

    def test_start_reclaims_expired_but_not_active_lease(self):
        expired = (datetime.now(timezone.utc) - timedelta(minutes=16)).isoformat()
        self.update("UPDATE assistent_rechnungsimporte SET state='laeuft',lease='old',started_at=?", (expired,))
        self.assertEqual(self.catalog.prepare(inventory())['offen'], 1)
        self.update("UPDATE assistent_rechnungsimporte SET state='laeuft',lease='active',started_at=?",
                    (datetime.now(timezone.utc).isoformat(),))
        self.assertEqual(self.catalog.prepare(inventory())['laeuft'], 1)

    def test_concurrent_request_does_not_duplicate_an_active_read(self):
        entered, release = threading.Event(), threading.Event()
        errors = []

        def reader(*args):
            entered.set()
            if not release.wait(5):
                raise AssertionError('Test synchronization timed out')
            return extracted(candidate())

        def worker():
            try:
                self.catalog.process_next()
            except Exception as exc:
                errors.append(exc)

        with patch('werkstatt_artikel_import.read_source', side_effect=reader) as mocked:
            thread = threading.Thread(target=worker)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(self.catalog.process_next()['laeuft'], 1)
                self.assertEqual(mocked.call_count, 1)
            finally:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(self.catalog.status()['vorschlaege'], 1)

    def test_expired_worker_cannot_publish_after_replacement(self):
        entered, release = threading.Event(), threading.Event()
        errors = []

        def old_reader(*args):
            entered.set()
            release.wait(5)
            return extracted(candidate(produkt_name='Old worker result'))

        def worker():
            try:
                self.catalog.process_next()
            except Exception as exc:
                errors.append(exc)

        with patch('werkstatt_artikel_import.read_source', side_effect=old_reader):
            thread = threading.Thread(target=worker)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                expired = (datetime.now(timezone.utc) - timedelta(minutes=16)).isoformat()
                self.update('UPDATE assistent_rechnungsimporte SET started_at=?', (expired,))
                self.catalog.prepare(inventory())
                self.process(extracted(candidate(produkt_name='Replacement result')))
            finally:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(self.catalog.status()['vorschlaege'], 1)
        self.assertEqual(self.catalog.search('Replacement')[0]['produkt_name'], 'Replacement result')
        self.assertEqual(self.catalog.search('Old'), [])


if __name__ == '__main__':
    unittest.main()
