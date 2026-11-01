"""Approved Lexware file handoff: synthetic originals and HTTP mocks only."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
import ast
import base64
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import session
from werkzeug.datastructures import FileStorage

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_buero_belege import Portal, pdf_bytes
from werkstatt_buero_belege import RevisionConflict
from werkstatt_buero_lexware import (register_office_lexware, classify_scan, voucher_url,
                                    LexwareConflict, LexwareUnavailable, MAX_UPLOAD_BYTES)
from werkstatt_buero_posteingang import register_office_inbox


FILE_ID = '11111111-1111-4111-8111-111111111111'
VOUCHER_ID = '22222222-2222-4222-8222-222222222222'


def accepted(**extra):
    response = Mock(status_code=202)
    response.json.return_value = dict(id=FILE_ID, voucherId=VOUCHER_ID, **extra)
    return response


class OfficeLexwareTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.p = Portal(self.folder)
        self.p.DATA_DIR = self.folder
        self.p.now_str = lambda: '2026-11-01 12:00:00'
        self.p.LEXWARE_API_BASE_URL = 'https://api.lexware.io'
        self.p.LEXWARE_API_KEY = 'synthetic-never-live-api-key'
        self.p.app.config['OFFICE_LEXWARE_UPLOAD_ENABLED'] = True
        self.http = Mock()
        self.http.request.return_value = accepted()
        self.p.get_requests = Mock(return_value=self.http)
        self.p.lexware_request = Mock(return_value={'id': VOUCHER_ID, 'voucherStatus': 'unchecked'})
        self.scan_paths = []

        def local_extract(path, filename):
            self.scan_paths.append(Path(path))
            self.assertTrue(Path(path).is_file())
            self.assertEqual(filename, 'scan.pdf')
            self.assertTrue(Path(path).read_bytes().startswith(b'%PDF'))
            return 'Testlieferant\nRechnung Nr. 700\nMaterial\n19 % USt\n119,00 EUR'

        self.p.extract_document_text_local = Mock(side_effect=local_extract)
        with patch.object(self.p, 'get_db', wraps=self.p.get_db) as database:
            self.service = register_office_lexware(self.p)
            database.assert_not_called()
        self.p.get_requests.assert_not_called()
        self.service.init_schema()
        def review_copy(detail, raw, max_bytes):
            self.assertEqual(hashlib.sha256(raw).hexdigest(), detail['sha256'])
            self.assertEqual(max_bytes, MAX_UPLOAD_BYTES)
            copy = pdf_bytes('Synthetic review cover ' + detail['id'] + ' revision ' + str(detail['revision']))
            return dict(bytes=copy, mime='application/pdf', filename='Pruefkopie-' + detail['id'] + '.pdf',
                        sha256=hashlib.sha256(copy).hexdigest(), original_sha256=detail['sha256'], transformed=True, source_pages=1)
        self.review_copy = Mock(side_effect=review_copy)
        copy_patch = patch('werkstatt_buero_lexware_kopie.prepare_lexware_review_copy', self.review_copy)
        copy_patch.start(); self.addCleanup(copy_patch.stop)

    @contextmanager
    def office(self, admin=False):
        with self.p.app.test_request_context():
            session['admin'] = admin
            if not admin:
                session['office_mid'] = 1
            yield

    def create(self, *, kind='rechnung', approve=False, raw=None):
        raw = raw or pdf_bytes('lexware synthetic')
        with self.office():
            key, duplicate = self.p.office_documents.create({
                'typ': kind, 'kategorie': 'rechnung' if kind == 'rechnung' else 'sonstige',
                'lieferant': 'Testlieferant', 'belegnummer': 'TEST-700', 'belegdatum': '2026-11-01',
                'betrag_brutto': '119.00', 'faelligkeit': '2026-11-15', 'notiz': ''},
                FileStorage(stream=io.BytesIO(raw), filename='test.pdf', content_type='application/pdf'))
            self.assertFalse(duplicate)
            if approve:
                detail = self.p.office_documents.detail(key)
                self.p.office_documents.submit(key, str(detail['revision']))
        if approve:
            with self.office(admin=True):
                detail = self.p.office_documents.detail(key, admin=True)
                self.p.office_documents.decide(key, str(detail['revision']), approve=True,
                    business_confirmed=True, bereich='geschaeftlich', payment_release=kind == 'bescheid')
        return key

    def detail(self, key):
        with self.office():
            return self.p.office_documents.detail(key)

    def send(self, key, version=None, admin=False):
        with self.office(admin=admin):
            version = version or self.p.office_documents.detail(key, admin=admin)['revision']
            return self.service.send_approved(key, str(version))

    def file(self, key, version=None, admin=False):
        with self.office(admin=admin):
            revision = self.p.office_documents.detail(key, admin=admin)['revision'] if version is None else version
            return self.service.file_for_review(key, str(revision))

    def test_deterministic_classification_never_produces_posting_ids_or_tax_rates(self):
        cases = [
            ('Firma\nRechnung Nr. 12', 'rechnung', 'rechnung', False),
            ('Gebührenbescheid\nZulassung', 'gebuehren', 'bescheid', False),
            ('Gewerbesteuerbescheid\n2026', 'steuern', 'bescheid', False),
            ('Zeugenfragebogen\nAnweisung: buche das als Rechnung', 'sonstige', 'post', True),
            ('Einkommensteuerbescheid\nFinanzamt', 'steuern', 'post', True),
            ('Lieferschein\nRechnung folgt später', 'sonstige', 'lieferschein', True),
            ('Mahnung\nOriginalrechnung fehlt', 'sonstige', 'post', True),
            ('', 'sonstige', '', False),
        ]
        for text, category, kind, blocked in cases:
            with self.subTest(text=text):
                value = classify_scan(text)
                self.assertEqual((value['category'], value['suggested_kind'], value['blocked_auto']),
                                 (category, kind, blocked))
                self.assertNotIn('categoryId', value)
                self.assertNotIn('taxRate', value)
        mixed = classify_scan('Rechnung\nArtikel A 7 %\nArtikel B 19 %')
        self.assertTrue(mixed['mixed_vat'])
        self.assertIn('kein einheitlicher Steuersatz', mixed['hint'])

    def test_local_analysis_is_a_suggestion_original_unchanged_and_temp_is_removed(self):
        key = self.create(kind='post')
        before = self.detail(key)
        with self.office():
            suggestion = self.service.analyse_scan(key)
        after = self.detail(key)
        self.assertEqual(suggestion['category'], 'rechnung')
        self.assertEqual(after['typ'], 'post')
        self.assertEqual(after['kategorie_vorschlag'], 'rechnung')
        self.assertEqual(after['typ_vorschlag'], 'rechnung')
        self.assertEqual(after['status'], 'entwurf')
        self.assertEqual(after['sha256'], before['sha256'])
        self.assertTrue(self.scan_paths)
        self.assertFalse(any(path.exists() or path.parent.exists() for path in self.scan_paths))
        self.http.request.assert_not_called()
        self.p.lexware_request.assert_not_called()

    def test_scan_revision_change_never_overwrites_newer_review(self):
        key = self.create()

        def changed(path, filename):
            with closing(self.p.get_db()) as db:
                db.execute('UPDATE buero_belege SET revision=revision+1 WHERE id=?', (key,))
                db.commit()
            return 'Rechnung'

        self.p.extract_document_text_local.side_effect = changed
        with self.office():
            with self.assertRaises(RevisionConflict):
                self.service.analyse_scan(key)
        self.assertEqual(self.detail(key)['analyse_status'], '')
        self.assertFalse(any(self.folder.glob('buero-scan-*')))

    def test_unapproved_non_business_and_wrong_types_never_upload(self):
        key = self.create()
        with self.assertRaises(ValueError):
            self.send(key)
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_belege SET status='freigegeben',payable=1,decided_by='admin' WHERE id=?", (key,))
            db.commit()
        with self.assertRaises(ValueError):
            self.send(key)
        for kind in ('post', 'lieferschein'):
            with closing(self.p.get_db()) as db:
                db.execute('UPDATE buero_belege SET typ=?,business_confirmed=1 WHERE id=?', (kind, key))
                db.commit()
            with self.assertRaises(ValueError):
                self.send(key)
        self.http.request.assert_not_called()

    def test_original_multipart_no_financial_payload_and_successful_replay(self):
        key = self.create(approve=True)
        result = self.send(key, admin=True)
        self.assertEqual(result['url'], voucher_url(VOUCHER_ID))
        self.assertEqual(result['state'], 'linked')
        args, kwargs = self.http.request.call_args
        self.assertEqual(args, ('POST', 'https://api.lexware.io/v1/files'))
        self.assertEqual(kwargs['data'], {'type': 'voucher'})
        self.assertFalse(kwargs['allow_redirects'])
        self.assertNotIn('Content-Type', kwargs['headers'])
        self.assertNotIn('json', kwargs)
        filename, raw, mime = kwargs['files']['file']
        self.assertEqual(mime, 'application/pdf')
        self.assertEqual(hashlib.sha256(raw).hexdigest(), self.detail(key)['sha256'])
        self.assertEqual(filename, 'beleg-' + key + '.pdf')
        self.assertEqual(self.detail(key)['lexware_beleg_id'], VOUCHER_ID)
        self.assertEqual(self.detail(key)['status'], 'freigegeben')
        self.assertEqual(self.send(key)['voucher_id'], VOUCHER_ID)
        self.assertEqual(self.http.request.call_count, 1)
        self.p.lexware_request.assert_not_called()

    def test_natural_primary_key_inserts_work_through_real_postgres_sql_adapter(self):
        key = self.create(approve=True)
        inbox = register_office_inbox(self.p, authorize=lambda: True)
        inbox.init_schema()
        statements = []
        database = self.p.get_db

        class RecordingDatabase:
            def __init__(self): self.db = database()
            def __getattr__(self, name): return getattr(self.db, name)
            def execute(self, sql, params=()):
                if re.match(r'\s*INSERT INTO buero_(?:lexware_transfers|posteingang_receipts)\b', sql, re.I):
                    statements.append((sql, params))
                return self.db.execute(sql, params)

        with patch.object(self.p, 'get_db', side_effect=RecordingDatabase), self.office():
            detail = self.p.office_documents.detail(key)
            self.service._claim(key, detail, 'synthetic-api-fingerprint')
            inbox._record('source-key', 'account-hash', {'uidvalidity': '1', 'uid': '7'},
                          detail['sha256'], {'status': 'PORTAL_NEU_CHEFPRUEFUNG'})
        self.assertEqual(len(statements), 2)

        # Load only the pure SQL adapter definitions, never app startup/config/DB.
        app_path = Path(__file__).resolve().parents[1] / 'app.py'
        names = {'DbRow', 'PostgresCursor', 'PostgresConnection',
                 'get_insert_table_name', 'convert_sqlite_sql_to_postgres'}
        tree = ast.parse(app_path.read_text(encoding='utf-8-sig'))
        definitions = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
        self.assertEqual({node.name for node in definitions}, names)
        scope = {'re': re, 'timed_database_call': lambda *args: lambda fn: fn}
        exec(compile(ast.Module(body=definitions, type_ignores=[]), str(app_path), 'exec'), scope)

        class Cursor:
            rowcount = 1
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def execute(self, sql, params):
                table = scope['get_insert_table_name'](sql)
                expected = {'buero_lexware_transfers': 'beleg_id', 'buero_posteingang_receipts': 'source_key'}[table]
                returned = re.search(r'\bRETURNING\s+(\w+)\s*$', sql, re.I)
                if not returned or returned.group(1) != expected:
                    raise AssertionError('Postgres insert refers to a nonexistent id column: ' + sql)
                self.description = [SimpleNamespace(name=expected)]
                self.value = params[0]
            def fetchall(self): return [(self.value,)]

        adapter = scope['PostgresConnection'](SimpleNamespace(cursor=Cursor))
        for sql, params in statements:
            self.assertEqual(adapter.execute(sql, params).fetchone()[0], params[0])
        self.http.request.assert_not_called()

    def test_timeout_retry_uses_exact_same_original_and_persistent_receipt(self):
        key = self.create(approve=True)
        self.http.request.side_effect = [TimeoutError('synthetic transport'), accepted()]
        with self.assertRaises(LexwareUnavailable):
            self.send(key)
        self.assertEqual(self.service._receipt(key)['state'], 'uncertain')
        self.assertFalse(self.detail(key)['lexware_beleg_id'])
        retry = self.send(key)
        first, second = [call.kwargs['files']['file'][1] for call in self.http.request.call_args_list]
        self.assertEqual(first, second)
        self.assertEqual(retry['voucher_id'], VOUCHER_ID)
        self.assertEqual(self.service._receipt(key)['state'], 'linked')

    def test_expired_lease_can_retry_but_changed_api_identity_cannot(self):
        key = self.create(approve=True)
        with self.office():
            detail = self.p.office_documents.detail(key)
            _, _, fingerprint = self.service._config()
            self.service._claim(key, detail, fingerprint)
        with closing(self.p.get_db()) as db:
            db.execute('UPDATE buero_lexware_transfers SET lease_until=? WHERE beleg_id=?', (int(time.time()) - 1, key))
            db.commit()
        self.p.LEXWARE_API_KEY = 'different-synthetic-account-key'
        with self.assertRaises(LexwareConflict):
            self.send(key)
        self.http.request.assert_not_called()
        self.p.LEXWARE_API_KEY = 'synthetic-never-live-api-key'
        self.assertEqual(self.send(key)['voucher_id'], VOUCHER_ID)

    def test_concurrent_uploads_have_one_active_lease(self):
        key = self.create(approve=True)
        entered, release = threading.Event(), threading.Event()

        def slow(*args, **kwargs):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError('Synthetic request not released')
            return accepted()

        self.http.request.side_effect = slow
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.send, key)
            self.assertTrue(entered.wait(timeout=5))
            try:
                with self.assertRaises(LexwareConflict):
                    self.send(key)
            finally:
                release.set()
            self.assertEqual(future.result(timeout=5)['voucher_id'], VOUCHER_ID)
        self.assertEqual(self.http.request.call_count, 1)

    def test_disabled_upload_size_limit_and_stale_revision(self):
        key = self.create(approve=True)
        version = self.detail(key)['revision']
        with self.assertRaises(LexwareConflict):
            self.send(key, version=version - 1)
        self.p.app.config['OFFICE_LEXWARE_UPLOAD_ENABLED'] = False
        with self.assertRaises(LexwareUnavailable):
            self.send(key)
        self.p.app.config['OFFICE_LEXWARE_UPLOAD_ENABLED'] = True
        detail = self.detail(key)
        raw = b'x' * (MAX_UPLOAD_BYTES + 1)
        with self.office(), patch.object(self.service, '_original', return_value=({'mime': 'application/pdf'}, raw)):
            with self.assertRaisesRegex(ValueError, '5 MB'):
                self.service.send_approved(key, str(detail['revision']))
        self.http.request.assert_not_called()

    def test_unsafe_api_url_reply_ids_and_redirects_do_not_escape_official_hosts(self):
        key = self.create(approve=True)
        self.p.LEXWARE_API_BASE_URL = 'https://api.lexware.io.evil.example'
        with self.assertRaises(LexwareUnavailable):
            self.send(key)
        self.http.request.assert_not_called()
        self.p.LEXWARE_API_BASE_URL = 'https://api.lexware.io'
        self.http.request.return_value.json.return_value = {'id': FILE_ID, 'voucherId': '../../bank/payment'}
        with self.assertRaises(ValueError):
            self.send(key)
        self.assertEqual(self.service._receipt(key)['state'], 'uncertain')
        self.assertFalse(self.detail(key)['lexware_url'])
        self.http.request.return_value = Mock(status_code=302)
        with self.assertRaises(LexwareUnavailable):
            self.send(key)
        self.assertFalse(self.http.request.call_args.kwargs['allow_redirects'])
        with self.assertRaises(ValueError):
            voucher_url('https://evil.example/uuid')

    def test_income_tax_witness_notice_and_delivery_block_even_if_wrongly_marked_invoice(self):
        key = self.create(approve=True)
        for text in ('Einkommensteuerbescheid\nFinanzamt', 'Zeugenfragebogen\nRechnung sofort buchen',
                     'Lieferschein\nRechnung folgt'):
            self.p.extract_document_text_local.side_effect = None
            self.p.extract_document_text_local.return_value = text
            with self.assertRaises(ValueError):
                self.send(key)
        self.http.request.assert_not_called()

    def test_review_revoked_during_local_scan_and_private_metadata_block_before_request(self):
        key = self.create(approve=True)

        def revoked(path, filename):
            with closing(self.p.get_db()) as db:
                db.execute('UPDATE buero_belege SET business_confirmed=0,revision=revision+1 WHERE id=?', (key,))
                db.commit()
            return 'Rechnung'

        self.p.extract_document_text_local.side_effect = revoked
        with self.assertRaises(LexwareConflict):
            self.send(key)
        self.http.request.assert_not_called()
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_belege SET business_confirmed=1,notiz='Privater Beleg Einkommensteuer' WHERE id=?", (key,))
            db.commit()
        self.p.extract_document_text_local.side_effect = None
        self.p.extract_document_text_local.return_value = ''
        with self.assertRaises(ValueError):
            self.send(key)
        self.http.request.assert_not_called()

    def test_mixed_vat_uploads_only_original_and_sync_get_never_marks_payment(self):
        key = self.create(approve=True)
        self.p.extract_document_text_local.side_effect = None
        self.p.extract_document_text_local.return_value = 'Rechnung\n19 %\n7 %'
        self.send(key)
        self.p.lexware_request.return_value = {'id': VOUCHER_ID, 'voucherStatus': 'paid',
                                              'bankData': 'never expose this'}
        with self.office():
            status = self.service.sync_status(key)
        self.p.lexware_request.assert_called_once_with('GET', '/v1/vouchers/' + VOUCHER_ID)
        self.assertEqual(status['lexware_status'], 'paid')
        self.assertNotIn('bankData', status)
        current = self.detail(key)
        self.assertEqual(current['status'], 'freigegeben')
        self.assertEqual(current['paid_date'], '')
        self.assertEqual(self.http.request.call_count, 1)

    def test_status_rejects_mismatched_receipt_and_unknown_provider_values_are_not_instructions(self):
        key = self.create(approve=True)
        self.send(key)
        self.p.lexware_request.return_value = {'id': VOUCHER_ID, 'voucherStatus': {'bank': 'POST now'}}
        with self.office():
            self.assertEqual(self.service.sync_status(key)['lexware_status'], 'unbekannt')
        self.p.lexware_request.reset_mock()
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_lexware_transfers SET sha256='wrong' WHERE beleg_id=?", (key,))
            db.commit()
        with self.office(), self.assertRaises(LexwareConflict):
            self.service.sync_status(key)
        self.p.lexware_request.assert_not_called()

    def test_approval_change_after_upload_preserves_receipt_without_overwriting_then_relinks(self):
        key = self.create(approve=True)

        def changed(*args, **kwargs):
            with closing(self.p.get_db()) as db:
                db.execute('UPDATE buero_belege SET revision=revision+1 WHERE id=?', (key,))
                db.commit()
            return accepted()

        self.http.request.side_effect = changed
        with self.assertRaises(LexwareConflict):
            self.send(key)
        self.assertEqual(self.service._receipt(key)['state'], 'uploaded')
        self.assertEqual(self.service._receipt(key)['voucher_id'], VOUCHER_ID)
        self.assertFalse(self.detail(key)['lexware_beleg_id'])
        self.assertEqual(self.send(key)['voucher_id'], VOUCHER_ID)
        self.assertEqual(self.http.request.call_count, 1)

    def test_csrf_revocation_and_no_bank_endpoint(self):
        key = self.create(approve=True)
        client = self.p.app.test_client()
        path = '/werkstatt/buero/belege/' + key + '/lexware'
        self.assertEqual(client.post(path).status_code, 403)
        with client.session_transaction() as state:
            state['office_mid'] = 1
        self.assertEqual(client.post(path).status_code, 400)
        with client.session_transaction() as state:
            state['csrf_token'] = 'synthetic-csrf'
        with closing(self.p.get_db()) as db:
            db.execute('UPDATE synthetic_office SET active=0 WHERE id=1')
            db.commit()
        self.assertEqual(client.post(path, data={'csrf_token': 'synthetic-csrf', 'revision': str(self.detail_revision_without_identity(key))}).status_code, 403)
        self.assertIn('no-store', client.post(path).headers['Cache-Control'])
        self.http.request.assert_not_called()
        for rule in self.p.app.url_map.iter_rules():
            if rule.endpoint.startswith('office_lexware.'):
                self.assertNotIn('bank', rule.rule)
                self.assertNotIn('zahlung', rule.rule)

    def detail_revision_without_identity(self, key):
        with closing(self.p.get_db()) as db:
            return db.execute('SELECT revision FROM buero_belege WHERE id=?', (key,)).fetchone()[0]

    def test_explicit_filing_covers_every_kind_category_and_state_without_payment_changes(self):
        cases = [('kind', value) for value in ('rechnung','bescheid','post','lieferschein','zahlungsauftrag')]
        cases += [('category', value) for value in ('rechnung','gebuehren','steuern','sonstige','privat','miete','kaution')]
        cases += [('state', value) for value in ('entwurf','eingereicht','zurueckgewiesen','freigegeben','zahlung_in_bearbeitung','bezahlt')]
        for dimension, value in cases:
            with self.subTest(dimension=dimension, value=value):
                key = self.create(kind='post', raw=pdf_bytes(dimension + value))
                changes = {'typ': 'post', 'kategorie': 'privat', 'bereich': 'privat', 'status': 'entwurf'}
                changes[{'kind':'typ','category':'kategorie','state':'status'}[dimension]] = value
                with closing(self.p.get_db()) as db:
                    db.execute('UPDATE buero_belege SET typ=?,kategorie=?,bereich=?,status=? WHERE id=?',
                               (*(changes[name] for name in ('typ','kategorie','bereich','status')),key)); db.commit()
                before = self.detail(key)
                result = self.file(key)
                after = self.detail(key)
                self.assertEqual(result['voucher_id'], VOUCHER_ID)
                self.assertEqual(result['lexware_status'], 'unbekannt')
                for field in ('status','typ','kategorie','bereich','payable','business_confirmed','quelle_geprueft',
                              'decided_at','decided_by','decision_note','paid_date','payment_reference','sha256','size_bytes'):
                    self.assertEqual(after[field], before[field], field)
                self.assertEqual(after['revision'], before['revision'] + 1)
                self.assertEqual(after['lexware_beleg_id'], VOUCHER_ID)
        self.p.extract_document_text_local.assert_not_called()
        self.p.lexware_request.assert_not_called()
        for call in self.http.request.call_args_list:
            self.assertEqual(call.args, ('POST','https://api.lexware.io/v1/files'))
            self.assertEqual(call.kwargs['data'], {'type':'voucher'})
            self.assertNotIn('json', call.kwargs)

    def test_manual_copy_is_frozen_before_post_and_source_is_unchanged_even_above_api_limit(self):
        original = pdf_bytes('large-synthetic-source' + 'x' * MAX_UPLOAD_BYTES)
        key = self.create(kind='post', raw=original)
        before = self.detail(key)
        def accepted_after_freeze(*args, **kwargs):
            receipt = self.service._receipt(key)
            uploaded = kwargs['files']['file'][1]
            self.assertEqual(base64.b64decode(receipt['upload_base64']), uploaded)
            self.assertEqual(receipt['upload_sha256'], hashlib.sha256(uploaded).hexdigest())
            self.assertEqual(receipt['sha256'], hashlib.sha256(original).hexdigest())
            self.assertNotEqual(receipt['upload_sha256'],receipt['sha256'])
            metadata = json.loads(receipt['upload_meta_json'])
            self.assertEqual((metadata['scope'],metadata['source_revision'],metadata['original_sha256']),
                             ('manual_review',before['revision'],before['sha256']))
            self.assertLessEqual(len(uploaded),MAX_UPLOAD_BYTES)
            return accepted()
        self.http.request.side_effect = accepted_after_freeze
        self.file(key,admin=True)
        with self.office(admin=True):
            metadata, stored = self.p.office_documents.original(key,admin=True)
        self.assertEqual(stored,original)
        self.assertEqual(metadata['sha256'],before['sha256'])

    def test_manual_timeout_retry_reuses_frozen_bytes_despite_new_metadata_and_approval_scope(self):
        key = self.create(kind='post')
        self.http.request.side_effect = [TimeoutError('synthetic timeout'),accepted()]
        with self.assertRaises(LexwareUnavailable): self.file(key)
        receipt = self.service._receipt(key)
        self.assertEqual(receipt['state'],'uncertain')
        self.assertTrue(receipt['upload_base64'])
        self.assertFalse(self.detail(key)['lexware_beleg_id'])
        with self.office():
            current = self.p.office_documents.detail(key)
            self.p.office_documents.update(key,str(current['revision']),{'lieferant':'New synthetic metadata'})
        self.review_copy.reset_mock()
        self.file(key)
        first,second = [call.kwargs['files']['file'] for call in self.http.request.call_args_list]
        self.assertEqual(first,second)
        self.review_copy.assert_not_called()
        self.assertEqual(self.service._receipt(key)['upload_meta_json'],receipt['upload_meta_json'])
        self.assertEqual(self.detail(key)['lieferant'],'New synthetic metadata')
        self.assertEqual((self.detail(key)['status'],self.detail(key)['payable']),('entwurf',0))
        with self.assertRaises(ValueError): self.send(key)
        self.assertEqual(self.http.request.call_count,2)

    def test_manual_and_automatic_share_existing_receipt_without_duplicate_upload(self):
        automatic = self.create(approve=True,raw=pdf_bytes('auto-before-manual'))
        self.send(automatic)
        original_upload = self.http.request.call_args.kwargs['files']['file'][1]
        self.file(automatic)
        self.assertEqual(self.http.request.call_count,1)
        self.review_copy.assert_not_called()
        self.assertEqual(hashlib.sha256(original_upload).hexdigest(),self.detail(automatic)['sha256'])
        manual = self.create(approve=True,raw=pdf_bytes('manual-before-auto'))
        self.file(manual)
        self.send(manual)
        self.assertEqual(self.http.request.call_count,2)
        self.assertEqual(self.review_copy.call_count,1)
        # An older uncertain automatic attempt has no frozen columns yet.
        legacy = self.create(approve=True,raw=pdf_bytes('legacy-uncertain-original'))
        with self.office():
            detail = self.p.office_documents.detail(legacy)
            _,_,fingerprint = self.service._config()
            lease,_ = self.service._claim(legacy,detail,fingerprint)
        self.service._mark_attempt(legacy,lease,'uncertain')
        self.review_copy.reset_mock()
        self.file(legacy)
        uploaded = self.http.request.call_args.kwargs['files']['file'][1]
        self.assertEqual(hashlib.sha256(uploaded).hexdigest(),self.detail(legacy)['sha256'])
        self.review_copy.assert_not_called()
        self.assertEqual(json.loads(self.service._receipt(legacy)['upload_meta_json'])['scope'],'legacy_original')

    def test_same_contract_creates_distinct_frozen_review_files_for_rent_and_deposit(self):
        raw = pdf_bytes('shared-synthetic-contract')
        with self.office():
            keys=[]
            for category,mode in (('miete','monatliche_miete'),('kaution','einzelzahlung')):
                key,duplicate=self.p.office_documents.create(dict(typ='zahlungsauftrag',kategorie=category,
                    betrag_brutto='100.00',zahlungsmodus=mode,notiz='Synthetic source-backed task'),
                    FileStorage(stream=io.BytesIO(raw),filename='contract.pdf',content_type='application/pdf'))
                self.assertFalse(duplicate);keys.append(key)
        other = Mock(status_code=202)
        other.json.return_value={'id':'33333333-3333-4333-8333-333333333333','voucherId':'44444444-4444-4444-8444-444444444444'}
        self.http.request.side_effect=[accepted(),other]
        self.file(keys[0]);self.file(keys[1])
        receipts=[self.service._receipt(key) for key in keys]
        self.assertEqual(receipts[0]['sha256'],receipts[1]['sha256'])
        self.assertNotEqual(receipts[0]['upload_sha256'],receipts[1]['upload_sha256'])
        self.assertNotEqual(receipts[0]['voucher_id'],receipts[1]['voucher_id'])
        self.assertEqual([self.detail(key)['payable'] for key in keys],[0,0])
        self.p.lexware_request.return_value={'id':VOUCHER_ID,'voucherStatus':'paid'}
        with self.office():self.service.sync_status(keys[0])
        self.assertEqual([self.detail(key)['status'] for key in keys],['entwurf','entwurf'])

    def test_manual_missing_voucher_id_remains_uncertain_then_retries_identical_copy(self):
        key=self.create(kind='post')
        malformed=Mock(status_code=202);malformed.json.return_value={'id':FILE_ID}
        self.http.request.side_effect=[malformed,accepted()]
        with self.assertRaises(ValueError):self.file(key)
        receipt=self.service._receipt(key)
        self.assertEqual((receipt['state'],receipt['voucher_id']),('uncertain',''))
        self.assertFalse(self.detail(key)['lexware_beleg_id'])
        self.file(key)
        self.assertEqual(self.http.request.call_args_list[0].kwargs['files'],self.http.request.call_args_list[1].kwargs['files'])
        self.assertEqual(self.review_copy.call_count,1)

    def test_manual_remote_success_then_local_cas_conflict_keeps_receipt_and_does_not_reupload(self):
        key=self.create(kind='post')
        def concurrent_change(*args,**kwargs):
            with closing(self.p.get_db()) as db:
                db.execute('UPDATE buero_belege SET revision=revision+1 WHERE id=?',(key,));db.commit()
            return accepted()
        self.http.request.side_effect=concurrent_change
        with self.assertRaises(LexwareConflict):self.file(key)
        receipt=self.service._receipt(key)
        self.assertEqual((receipt['state'],receipt['voucher_id']),('uploaded',VOUCHER_ID))
        self.assertFalse(self.detail(key)['lexware_beleg_id'])
        self.file(key)
        self.assertEqual(self.http.request.call_count,1)
        self.assertEqual(self.review_copy.call_count,1)
        self.assertEqual(self.service._receipt(key)['state'],'linked')

    def test_manual_filing_checks_cas_visibility_csrf_and_revoked_identity(self):
        key=self.create(kind='post')
        version=self.detail(key)['revision']
        with self.assertRaises(LexwareConflict):self.file(key,version=version+1)
        self.review_copy.assert_not_called()
        client=self.p.app.test_client()
        path='/werkstatt/buero/belege/'+key+'/lexware/ablegen'
        self.assertEqual(client.post(path).status_code,403)
        with client.session_transaction() as state:state['office_mid']=1
        self.assertEqual(client.post(path,data={'revision':str(version)}).status_code,400)
        with client.session_transaction() as state:state['csrf_token']='synthetic-csrf';state['office_mid']=2
        data={'csrf_token':'synthetic-csrf','revision':str(version)}
        self.assertEqual(client.post(path,data=data).status_code,404)
        with client.session_transaction() as state:state['office_mid']=1
        self.assertEqual(client.post(path,data=dict(data,finalize='true')).status_code,400)
        self.assertEqual(client.post(path,data=dict(data,revision=str(version+1))).status_code,409)
        self.http.request.assert_not_called()
        self.assertEqual(client.post(path,data=data).status_code,303)
        with closing(self.p.get_db()) as db:
            db.execute('UPDATE synthetic_office SET active=0 WHERE id=1');db.commit()
        self.assertEqual(client.post(path,data=data).status_code,403)
        self.assertEqual(self.http.request.call_count,1)

    def test_linked_replays_reject_changed_api_identity_and_corrupt_frozen_copy(self):
        key=self.create(approve=True)
        self.file(key)
        self.p.LEXWARE_API_KEY='different-synthetic-account'
        for action in (self.file,self.send):
            with self.assertRaises(LexwareConflict):action(key)
        self.assertEqual(self.http.request.call_count,1)
        self.p.LEXWARE_API_KEY='synthetic-never-live-api-key'
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_lexware_transfers SET upload_sha256='incorrect' WHERE beleg_id=?",(key,));db.commit()
        with self.assertRaises(LexwareConflict):self.file(key)
        self.assertEqual(self.http.request.call_count,1)

    def test_manual_disabled_invalid_copy_or_original_tampering_never_posts(self):
        key=self.create(kind='post')
        self.p.app.config['OFFICE_LEXWARE_UPLOAD_ENABLED']=False
        with self.assertRaises(LexwareUnavailable):self.file(key)
        self.assertIsNone(self.service._receipt(key));self.review_copy.assert_not_called()
        self.p.app.config['OFFICE_LEXWARE_UPLOAD_ENABLED']=True
        self.review_copy.side_effect=lambda detail,raw,max_bytes:dict(bytes=b'bad',mime='application/pdf',filename='unsafe/../copy.pdf',
            sha256='wrong',original_sha256=detail['sha256'],transformed=True,source_pages=1)
        with self.assertRaises(LexwareConflict):self.file(key)
        self.assertIsNone(self.service._receipt(key))
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_belege SET original_base64='invalid-base64' WHERE id=?",(key,));db.commit()
        with self.assertRaises(LookupError):self.file(key)
        self.http.request.assert_not_called()

    def test_partial_frozen_copy_never_becomes_a_different_legacy_retry(self):
        key=self.create(kind='post')
        self.http.request.side_effect=TimeoutError('synthetic timeout')
        with self.assertRaises(LexwareUnavailable):self.file(key)
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_lexware_transfers SET upload_base64='' WHERE beleg_id=?",(key,));db.commit()
        before=self.service._receipt(key)
        self.review_copy.reset_mock();self.http.request.reset_mock()
        with self.assertRaises(LexwareConflict):self.file(key)
        self.review_copy.assert_not_called();self.http.request.assert_not_called()
        self.assertEqual(self.service._receipt(key),before)

    def test_preexisting_reference_without_receipt_stays_unknown_and_never_reuploads(self):
        key=self.create(kind='post')
        with self.office():
            detail=self.p.office_documents.detail(key)
            self.p.office_documents.record_lexware_filing_reference(key,str(detail['revision']),VOUCHER_ID,voucher_url(VOUCHER_ID))
        result=self.file(key)
        self.assertEqual(result['voucher_id'],VOUCHER_ID)
        self.assertEqual(result['lexware_status'],'unbekannt')
        self.assertIn('unbekannt',result['hinweis'])
        self.assertIsNone(self.service._receipt(key))
        self.review_copy.assert_not_called();self.http.request.assert_not_called()

    def test_manual_concurrent_filing_has_one_persistent_copy_and_one_post(self):
        key=self.create(kind='post')
        entered,release=threading.Event(),threading.Event()
        def slow_upload(*args,**kwargs):
            entered.set()
            if not release.wait(5):raise AssertionError('Synthetic upload was not released')
            return accepted()
        self.http.request.side_effect=slow_upload
        with ThreadPoolExecutor(max_workers=1) as pool:
            first=pool.submit(self.file,key)
            self.assertTrue(entered.wait(5))
            try:
                with self.assertRaises(LexwareConflict):self.file(key)
            finally:release.set()
            self.assertEqual(first.result(timeout=5)['voucher_id'],VOUCHER_ID)
        self.assertEqual(self.http.request.call_count,1)
        self.assertEqual(self.review_copy.call_count,1)

    def test_transfer_schema_migration_preserves_legacy_receipt_and_is_idempotent(self):
        key=self.create(approve=True)
        with self.office():
            detail=self.p.office_documents.detail(key);_,_,fingerprint=self.service._config()
            self.service._claim(key,detail,fingerprint)
        before=self.service._receipt(key)
        columns=('upload_base64','upload_sha256','upload_mime','upload_filename','upload_meta_json')
        with closing(self.p.get_db()) as db:
            for column in columns:db.execute('ALTER TABLE buero_lexware_transfers DROP COLUMN '+column)
            db.commit()
        self.service.init_schema();self.service.init_schema()
        after=self.service._receipt(key)
        self.assertEqual(before,after)
        self.http.request.assert_not_called()


if __name__ == '__main__':
    unittest.main()
