"""Isolated catalog tests: temporary SQLite, synthetic receipts, no app/network."""
import importlib.util
from functools import wraps
import io
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
from unittest.mock import Mock

from flask import Flask, abort, redirect, request, session

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
if importlib.util.find_spec('werkstatt_rechnungsquelle') is None:
    reader = types.ModuleType('werkstatt_rechnungsquelle')
    reader.read_source = lambda *args: (_ for _ in ()).throw(AssertionError('Unmocked reader'))
    sys.modules['werkstatt_rechnungsquelle'] = reader

from werkstatt_artikel_import import InvoiceCatalog, register_invoice_catalog


class FakePortal:
    def __init__(self, path):
        self.path = path
        self.settings = {'ASSISTANT_MATERIAL_SUPPLIERS': json.dumps(['Test Supplier', 'Other Supplier', 'Auto-Color'])}

    def get_app_setting(self, key, default=''):
        return self.settings.get(key, default)

    def set_app_setting(self, key, value):
        self.settings[key] = value

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
                         ['Topcolor', 'Car-Parts', 'Tech Masters', 'Other Supplier', 'Auto-Color'])

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

    def test_prepare_holds_unknown_and_redacts_blocked_sources_before_read(self):
        self.update('DELETE FROM assistent_rechnungsimporte')
        report = self.catalog.prepare({'einkaufsbelege': [
            {'id': 10, 'lieferant': 'Volksbank Muster eG', 'original_name': 'private-bank-reference.pdf'},
            {'id': 11, 'lieferant': 'Neue Materialfirma GmbH', 'original_name': 'unknown-reference.pdf'},
        ], 'lieferantenrechnungen': []})
        self.assertEqual((report['offen'], report['ausgeschlossen'], report['zuordnen']), (0, 1, 1))
        self.assertNotIn('Volksbank', json.dumps(report))
        self.assertNotIn('private-bank-reference', json.dumps(report))
        self.assertNotIn('unknown-reference', json.dumps(report))
        self.assertIn('Neue Materialfirma', json.dumps(report))
        with patch('werkstatt_artikel_import.read_source') as read:
            self.catalog.process_next()
            read.assert_not_called()

    def test_existing_open_queue_is_rechecked_without_prepare(self):
        self.update('DELETE FROM assistent_rechnungsimporte')
        for index, supplier in enumerate(('Volksbank Muster', 'Unknown Materials'), 20):
            self.update("INSERT INTO assistent_rechnungsimporte(source_key,source_kind,source_id,supplier,reference,state) VALUES(?,?,?,?,?,'offen')",
                        ('einkauf:'+str(index), 'einkauf', str(index), supplier, 'hidden-original.pdf'))
        with patch('werkstatt_artikel_import.read_source') as read:
            result = self.catalog.process_next()
            read.assert_not_called()
        self.assertEqual((result['ausgeschlossen'], result['zuordnen'], result['offen']), (1, 1, 0))
        self.assertEqual({row['state'] for row in self.catalog.rows('SELECT state FROM assistent_rechnungsimporte')}, {'ausgeschlossen', 'zuordnen'})

    def test_historical_proposals_and_counts_follow_current_scope(self):
        self.process(extracted(candidate()))
        self.assertEqual(self.catalog.status()['vorschlaege'], 1)
        self.portal.settings['ASSISTANT_MATERIAL_SUPPLIERS'] = '[]'
        with patch('werkstatt_artikel_import.read_source') as read:
            self.assertEqual(self.catalog.search('Klebeband'), [])
            report = self.catalog.process_next()
            read.assert_not_called()
        self.assertEqual(report['vorschlaege'], 0)
        self.assertEqual(report['zuordnen'], 1)
        # Proposals remain locally recoverable, but are not supplied to the avatar.
        self.assertEqual(self.catalog.rows('SELECT COUNT(*) AS n FROM assistent_rechnungsartikel')[0]['n'], 1)
        self.update("UPDATE assistent_rechnungsimporte SET supplier='Volksbank Muster',reference='private-bank-reference'")
        report = self.catalog.status()
        self.assertEqual(report['ausgeschlossen'], 1)
        self.assertNotIn('Volksbank', json.dumps(report))
        self.assertNotIn('private-bank-reference', json.dumps(report))
        self.assertEqual(report['vorschlaege'], 0)

    def test_source_rule_is_checked_again_immediately_before_read(self):
        allowed = self.catalog.source_rule({'supplier': 'Test Supplier'})
        held = self.catalog.source_rule({'supplier': 'Unknown Supplier'})
        original = self.catalog.source_rule
        checks = []

        def changed_after_claim(source, allowed_suppliers=None):
            if allowed_suppliers is None:
                checks.append(source)
                return held
            return original(source, allowed_suppliers)

        self.assertTrue(allowed['allowed'])
        with patch.object(self.catalog, 'source_rule', side_effect=changed_after_claim), patch('werkstatt_artikel_import.read_source') as read:
            self.catalog.process_next()
            read.assert_not_called()
        self.assertTrue(checks)
        self.assertEqual(self.catalog.rows('SELECT state FROM assistent_rechnungsimporte')[0]['state'], 'zuordnen')

    def test_revocation_during_read_prevents_publication(self):
        def read(*args):
            self.portal.settings['ASSISTANT_MATERIAL_SUPPLIERS'] = '[]'
            return extracted(candidate())
        with patch('werkstatt_artikel_import.read_source', side_effect=read):
            report = self.catalog.process_next()
        self.assertEqual(report['zuordnen'], 1)
        self.assertEqual(report['vorschlaege'], 0)
        self.assertEqual(self.catalog.rows('SELECT COUNT(*) AS n FROM assistent_rechnungsartikel')[0]['n'], 0)

    def test_malformed_server_allowlist_fails_closed(self):
        for value in ('invalid-json', '{"Test Supplier":true}', '"Test Supplier"'):
            self.portal.settings['ASSISTANT_MATERIAL_SUPPLIERS'] = value
            with patch('werkstatt_artikel_import.read_source') as read:
                self.assertEqual(self.catalog.process_next()['zuordnen'], 1)
                read.assert_not_called()


class CatalogUploadAndApprovalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.portal = FakePortal(str(pathlib.Path(self.temp.name) / 'catalog.db'))
        self.portal.app = Flask('catalog-upload-test', template_folder=str(pathlib.Path(__file__).resolve().parents[1] / 'templates'))
        self.portal.app.config.update(TESTING=True, SECRET_KEY='synthetic-session')

        def admin_required(function):
            @wraps(function)
            def guarded(*args, **kwargs):
                if not session.get('admin'):
                    return redirect('/login')
                return function(*args, **kwargs)
            return guarded

        self.portal.admin_required = admin_required
        self.portal.save_einkauf_beleg_upload = Mock(return_value={'id': 99})
        self.service = types.SimpleNamespace(invoice_sources=lambda include_held=False: inventory())
        self.catalog = register_invoice_catalog(self.portal, self.service)
        self.catalog.prepare(inventory())

        @self.portal.app.before_request
        def csrf():
            if request.method == 'POST' and request.form.get('csrf_token') != session.get('csrf_token', 'required'):
                abort(400)

        self.client = self.portal.app.test_client()
        with self.client.session_transaction() as state:
            state.update(admin=True, csrf_token='synthetic-csrf')

    def upload(self, supplier, filenames=('invoice.pdf',)):
        return self.client.post('/admin/assistent-artikel/upload', data={
            'lieferant': supplier, 'csrf_token': 'synthetic-csrf',
            'rechnungen': [(io.BytesIO(b'%PDF-synthetic identical bytes'), filename) for filename in filenames],
        }, content_type='multipart/form-data')

    def test_unknown_blocked_and_empty_supplier_are_not_saved(self):
        for supplier in ('', 'Unknown Material Supplier', 'Volksbank', 'Allianz Versicherung'):
            self.assertEqual(self.upload(supplier).status_code, 302)
        self.portal.save_einkauf_beleg_upload.assert_not_called()

    def test_allowed_upload_deduplicates_identical_files_and_blocks_financial_filename(self):
        self.assertEqual(self.upload('Top-Color GmbH', ('invoice-a.pdf', 'invoice-b.pdf', 'Kontoauszug.pdf')).status_code, 302)
        self.portal.save_einkauf_beleg_upload.assert_called_once()
        self.assertEqual(self.portal.save_einkauf_beleg_upload.call_args.kwargs,
                         {'lieferant': 'Top-Color GmbH', 'beleg_typ': 'rechnung'})

    def test_admin_and_csrf_required_for_supplier_approval_without_block_override(self):
        self.catalog.prepare({'einkaufsbelege': [
            {'id': 30, 'lieferant': 'Unknown Materials', 'original_name': 'unknown.pdf'},
            {'id': 31, 'lieferant': 'Volksbank', 'original_name': 'secret.pdf'},
        ], 'lieferantenrechnungen': []})
        rows = self.catalog.rows('SELECT id,supplier FROM assistent_rechnungsimporte')
        ids = {row['supplier']: row['id'] for row in rows}
        unknown = '/admin/assistent-artikel/lieferant/'+str(ids['Unknown Materials'])+'/freigeben'
        bank = '/admin/assistent-artikel/lieferant/'+str(ids['Volksbank'])+'/freigeben'
        self.assertEqual(self.client.post(unknown).status_code, 400)
        with self.client.session_transaction() as state:
            state['admin'] = False
        self.assertEqual(self.client.post(unknown, data={'csrf_token': 'synthetic-csrf'}).status_code, 302)
        self.assertNotIn('Unknown Materials', self.catalog.allowed_suppliers())
        with self.client.session_transaction() as state:
            state['admin'] = True
        self.assertEqual(self.client.post(bank, data={'csrf_token': 'synthetic-csrf'}).status_code, 302)
        self.assertNotIn('Volksbank', self.catalog.allowed_suppliers())
        self.assertEqual(self.client.post(unknown, data={'csrf_token': 'synthetic-csrf', 'supplier': 'attacker-name'}).status_code, 302)
        self.assertIn('Unknown Materials', self.catalog.allowed_suppliers())
        self.assertNotIn('attacker-name', self.catalog.allowed_suppliers())


if __name__ == '__main__':
    unittest.main()
