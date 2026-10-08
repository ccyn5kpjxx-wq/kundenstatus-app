"""Isolated ledger/HTTP checks. No real records, invoices or supplier calls."""
import base64
import copy
import json
import socket
import zipfile
from contextlib import nullcontext
from unittest.mock import patch
import ast
from copy import deepcopy
from contextlib import closing
import hashlib
import hmac
import io
from pathlib import Path
import sqlite3
import re
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask
from PIL import Image
from werkzeug.datastructures import FileStorage
from werkstatt_einkaufseingang import MaterialIntake, IntakeConflict, TABLES, register_intake


def png(color='blue'):
    buffer = io.BytesIO()
    Image.new('RGB', (4, 4), color).save(buffer, format='PNG')
    return buffer.getvalue()


class IntakeTests(unittest.TestCase):
    def test_printed_zero_position_is_valid_and_idempotent(self):
        group = self.group(quantity='2')
        file = self.attach(group)
        value = self.delivery(group, file, position=0)
        self.service.record_delivery(group['id'], value)
        self.service.record_delivery(group['id'], value)
        detail = self.service.detail(group['id'])
        self.assertEqual(detail['lines'][0]['delivered_quantity'], '2')
        self.assertEqual(len(detail['lines'][0]['delivery_events']), 1)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / 'synthetic.db'
        self.items = []
        self.truncated = False
        def get_db():
            db = sqlite3.connect(self.database)
            db.row_factory = sqlite3.Row
            return db
        self.p = SimpleNamespace(get_db=get_db, app=Flask(__name__),
            cockpit_data=SimpleNamespace(catalog=SimpleNamespace(knowledge_rows=lambda **kwargs: {'items': deepcopy(self.items), 'truncated': self.truncated})),
            extract_document_text_local=lambda path, name: 'Art. TEST\nMenge unbekannt\nIBAN DE12345678901234567890\nPasswort geheim')
        self.p.app.secret_key = 'synthetic-test-only'
        self.service = register_intake(self.p)
        self.payload = {'supplier': 'Testlieferant', 'source_key': 'synthetic:2026-10-05:11:02',
            'external_ref': 'Synthetischer Lieferantenchat 11:02', 'source_at': '2026-10-05T11:02:00+02:00',
            'already_ordered': True, 'original_author': None,
            'lines': [{'product': 'Testband', 'sku': 'TEST-50', 'variant': '50 mm', 'unit': 'Karton', 'pack': '',
                       'quantity': None, 'urgent': None, 'category': 'material'}]}

    def tearDown(self):
        self.temporary.cleanup()

    def group(self, quantity=None):
        value = deepcopy(self.payload)
        value['lines'][0]['quantity'] = quantity
        return self.service.create(value)

    def attach(self, group, kind='lieferschein', color='blue'):
        return self.service.attach(group['id'], FileStorage(stream=io.BytesIO(png(color)), filename='test.png'), kind)

    def delivery(self, group, file, quantity='2', position=1):
        return {'revision': self.service.detail(group['id'])['revision'], 'line_id': group['lines'][0]['id'],
                'file_id': file['id'], 'position': position, 'quantity': quantity, 'unit': 'Karton'}

    def candidate(self, group, ident=1, amount='12.50', date='2026-09-28'):
        return {'vorschlag_id': ident, 'produkt_name': 'Testband', 'artikelnummer': 'TEST-50',
                'lieferant': 'Testlieferant', 'groesse': '50 mm', 'farbe': '', 'gebinde': '', 've': 'Karton',
                'historischer_preishinweis': amount, 'quelle': {'art': 'einkauf', 'beleg_id': ident, 'datum': date, 'position': 1},
                'price_evidence': {'basis': 'unknown'}, 'package_evidence': {'basis': 'unknown'}}

    def price(self, group, file, amount='10.00', role='plan', position=1):
        line = group['lines'][0]
        return {'revision': self.service.detail(group['id'])['revision'], 'line_id': line['id'], 'file_id': file['id'],
                'page': 1, 'position': position, 'role': role, 'amount': amount, 'unit': 'Karton', 'pack': '',
                'currency': 'EUR', 'tax_basis': 'gross', 'tax_rate': '19', 'discount_basis': 'nach Rabatt',
                'source_date': '2026-09-28', 'reviewed': True,
                'identity': {'supplier': 'Testlieferant', **{key: line[key] for key in ('product', 'sku', 'variant', 'unit', 'pack')}}}

    def test_historical_create_null_unknown_and_no_dispatch_schema(self):
        group = self.group()
        self.assertTrue(group['already_ordered'])
        self.assertFalse(group['dispatchable'])
        self.assertIsNone(group['lines'][0]['quantity'])
        self.assertIsNone(group['lines'][0]['urgent'])
        self.assertIsNone(group['original_author'])
        self.assertFalse(group['totals']['complete'])
        self.assertIsNone(group['lines'][0]['planned_total'])
        with closing(self.p.get_db()) as db:
            names = {row['name'] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(names - {'sqlite_sequence'}, set(TABLES))

    def test_source_replay_is_idempotent_and_conflict_is_not_second_order(self):
        first = self.group()
        self.assertEqual(self.group()['id'], first['id'])
        changed = deepcopy(self.payload)
        changed['lines'][0]['quantity'] = '5'
        with self.assertRaises(IntakeConflict):
            self.service.create(changed)
        self.assertEqual(len(self.service.list()), 1)

    def test_invalid_or_guessed_input_rejected(self):
        for field, value in [('quantity', True), ('quantity', 1.5), ('quantity', '0'), ('quantity', '-1'), ('urgent', 'false')]:
            bad = deepcopy(self.payload)
            bad['lines'][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.service.create(bad)
        bad = deepcopy(self.payload)
        bad['source_at'] = '2026-10-05T11:02:00'
        with self.assertRaises(ValueError):
            self.service.create(bad)
        with self.assertRaises(PermissionError):
            self.service.create(self.payload, 'mitarbeiter:1')

    def test_ppg_dp7000_product_is_not_an_iban_but_real_bank_data_remains_blocked(self):
        product = 'PPG DELTRON DP7000 STANDARD THINNER'
        data = deepcopy(self.payload)
        data['lines'][0]['product'] = product
        group = self.service.create(data)
        self.assertEqual(group['lines'][0]['product'], product)
        for extra in ('DE89370400440532013000', 'DE89 3704 0044 0532 0130 00',
                      'GB82 WEST 1234 5698 7654 32', 'NL91 ABNA 0417 1643 00',
                      'IBAN unleserlich', 'Passwort geheim', 'DE12345678901234567890'):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                bad = deepcopy(data)
                bad['source_key'] = 'invalid:test'
                bad['lines'][0]['product'] = product+' '+extra
                self.service.create(bad)
        self.p.extract_document_text_local = lambda *args: product+'\nDE89370400440532013000\nIBAN unleserlich\nPasswort geheim'
        file = self.attach(group)
        result = self.service.analyze_file(group['id'], file['id'])
        self.assertEqual(result['draft_text'], product)

    def test_exact_original_bytes_and_duplicate_different_role(self):
        group = self.group()
        file = self.attach(group)
        raw, mime, name = self.service.original(group['id'], file['id'])
        self.assertEqual(raw, png())
        self.assertEqual(mime, 'image/png')
        self.assertEqual(file['sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(self.attach(group)['id'], file['id'])
        with self.assertRaises(IntakeConflict):
            self.attach(group, 'rechnung')
        with self.assertRaises(ValueError):
            self.service.attach(group['id'], FileStorage(stream=io.BytesIO(b'not a png'), filename='x.png'), 'materialfoto')

    def test_file_not_accessible_from_another_group(self):
        group = self.group()
        file = self.attach(group)
        other = deepcopy(self.payload)
        other['source_key'] = 'other'
        second = self.service.create(other)
        with self.assertRaises(ValueError):
            self.service.original(second['id'], file['id'])

    def test_ocr_remains_draft_redacts_bank_and_never_infers_quantity(self):
        group = self.group()
        file = self.attach(group)
        result = self.service.analyze_file(group['id'], file['id'])
        self.assertEqual(result['extraction_status'], 'pruefen')
        self.assertIn('Menge unbekannt', result['draft_text'])
        self.assertNotIn('IBAN', result['draft_text'])
        self.assertNotIn('geheim', result['draft_text'])
        self.assertIsNone(self.service.detail(group['id'])['lines'][0]['quantity'])

    def test_unknown_order_quantity_never_becomes_complete(self):
        group = self.group()
        file = self.attach(group)
        result = self.service.record_delivery(group['id'], self.delivery(group, file))
        self.assertEqual(result['lines'][0]['delivered_quantity'], '2')
        self.assertEqual(result['lines'][0]['delivery_state'], 'liefermenge_belegt_bestellmenge_offen')

    def test_partial_delivery_cumulative_idempotency_and_conflict(self):
        group = self.group('5')
        file = self.attach(group)
        payload = self.delivery(group, file)
        first = self.service.record_delivery(group['id'], payload)
        self.assertEqual(first['lines'][0]['delivery_state'], 'teillieferung')
        repeat = self.service.record_delivery(group['id'], payload)
        self.assertEqual(repeat['revision'], first['revision'])
        self.assertEqual(repeat['lines'][0]['delivered_quantity'], '2')
        bad = dict(payload, quantity='3')
        with self.assertRaises(IntakeConflict):
            self.service.record_delivery(group['id'], bad)
        second = self.attach(group, color='red')
        complete = self.service.record_delivery(group['id'], self.delivery(group, second, '3'))
        self.assertEqual(complete['lines'][0]['delivery_state'], 'geliefert')
        self.assertEqual(complete['lines'][0]['delivered_quantity'], '5')

    def test_delivery_rejects_stale_missing_revision_and_wrong_unit_or_kind(self):
        group = self.group()
        file = self.attach(group)
        for edit in ({'revision': 1}, {'revision': None}, {'unit': 'Stück'}):
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                self.service.record_delivery(group['id'], dict(self.delivery(group, file), **edit))
        invoice = self.attach(group, 'rechnung', 'red')
        with self.assertRaises(ValueError):
            self.service.record_delivery(group['id'], self.delivery(group, invoice))

    def test_catalog_latest_by_invoice_date_not_id_and_exact_variant(self):
        group = self.group()
        self.items = [self.candidate(group, 99, date='2026-01-01'), self.candidate(group, 2, date='2026-09-28')]
        self.items.append(dict(self.candidate(group, 3), groesse='30 mm'))
        result = self.service.catalog_candidates(group['id'], group['lines'][0]['id'])
        self.assertEqual(result['suggested_id'], 2)
        self.assertEqual(len(result['matches']), 2)
        row = self.service.set_catalog_price(group['id'], group['lines'][0]['id'], {'revision': group['revision'], 'proposal_id': 2})
        price = row['lines'][0]['plan_price']
        self.assertFalse(price['verified'])
        self.assertEqual(price['tax_basis'], 'unknown')
        self.assertEqual(price['currency'], 'unknown')
        self.assertIsNone(row['lines'][0]['planned_total'])

    def test_catalog_unknown_dates_conflicting_same_day_and_truncation(self):
        group = self.group()
        self.items = [self.candidate(group), self.candidate(group, 2, date=None)]
        self.assertIsNone(self.service.catalog_candidates(group['id'], group['lines'][0]['id'])['suggested_id'])
        self.items = [self.candidate(group), self.candidate(group, 2, amount='13')]
        self.assertIsNone(self.service.catalog_candidates(group['id'], group['lines'][0]['id'])['suggested_id'])
        self.items = [self.candidate(group)]
        self.truncated = True
        self.assertIsNone(self.service.catalog_candidates(group['id'], group['lines'][0]['id'])['suggested_id'])

    def test_catalog_revoked_source_not_reused(self):
        group = self.group()
        self.items = [self.candidate(group)]
        self.service.catalog_candidates(group['id'], group['lines'][0]['id'])
        self.items = []
        with self.assertRaises(ValueError):
            self.service.set_catalog_price(group['id'], group['lines'][0]['id'], {'revision': group['revision'], 'proposal_id': 1})

    def test_reviewed_price_totals_and_price_difference(self):
        group = self.group('3')
        file = self.attach(group, 'rechnung')
        plan = self.price(group, file)
        result = self.service.record_price(group['id'], plan)
        self.assertEqual(result['totals']['known_subtotal'], '30.00')
        self.assertTrue(result['totals']['complete'])
        actual = self.price(group, file, amount='12', role='invoice', position=2)
        result = self.service.record_price(group['id'], actual)
        comparison = result['lines'][0]['price_comparison']
        self.assertTrue(comparison['comparable'])
        self.assertEqual(comparison['difference'], '2')
        self.assertEqual(comparison['percent'], '20.00')
        self.assertIsNone(comparison['cause'])
        # Replay the exact original price does not insert another observation.
        self.service.record_price(group['id'], plan)
        self.assertEqual(len(self.service.detail(group['id'])['lines'][0]['prices']), 2)

    def test_differing_tax_unit_pack_or_discount_not_comparable(self):
        group = self.group()
        file = self.attach(group, 'rechnung')
        result = self.service.record_price(group['id'], self.price(group, file))
        plan = result['lines'][0]['plan_price']
        for key, value in [('unit', 'Stück'), ('pack', '96'), ('tax_basis', 'net'), ('tax_rate', None), ('discount_basis', 'vor Rabatt'), ('currency', 'USD')]:
            with self.subTest(key=key):
                self.assertFalse(self.service.compare_prices(plan, dict(plan, **{key: value}))['comparable'])

    def test_invoice_price_requires_review_receipt_and_exact_identity(self):
        group = self.group()
        file = self.attach(group, 'rechnung')
        for edit in ({'reviewed': False}, {'revision': None}, {'unit': 'Stück'}, {'pack': '96'}, {'identity': {}}):
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                self.service.record_price(group['id'], dict(self.price(group, file), **edit))
        delivery = self.attach(group, 'lieferschein', 'red')
        with self.assertRaises(ValueError):
            self.service.record_price(group['id'], self.price(group, delivery))

    def test_line_clarification_audit_and_identity_locked_after_evidence(self):
        group = self.group()
        result = self.service.update_line(group['id'], group['lines'][0]['id'], {
            'revision': group['revision'], 'reviewed': True, 'reason': 'Test: ausdrückliche Mengenangabe', 'quantity': '4'})
        self.assertEqual(result['lines'][0]['quantity'], '4')
        with closing(self.p.get_db()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM einkauf_eingang_klaerungen').fetchone()[0], 1)
        file = self.attach(group)
        result = self.service.record_delivery(group['id'], self.delivery(group, file))
        with self.assertRaises(IntakeConflict):
            self.service.update_line(group['id'], group['lines'][0]['id'], {
                'revision': result['revision'], 'reviewed': True, 'reason': 'Test', 'sku': 'OTHER'})

    def test_http_auth_csrf_conflict_and_private_headers(self):
        client = self.p.app.test_client()
        url = '/admin/assistent-bestellungen/eingang'
        self.assertEqual(client.get(url).status_code, 403)
        with client.session_transaction() as session:
            session['admin'] = True
            session['csrf_token'] = 'synthetic-csrf'
        self.assertEqual(client.post(url, json=self.payload).status_code, 400)
        headers = {'X-CSRF-Token': 'synthetic-csrf'}
        response = client.post(url, json=self.payload, headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        altered = deepcopy(self.payload)
        altered['external_ref'] = 'different'
        self.assertEqual(client.post(url, json=altered, headers=headers).status_code, 409)
        self.assertEqual(client.get(url).status_code, 200)

    def test_no_default_financial_approval_for_unknown_quantities(self):
        group = self.group()
        file = self.attach(group, 'rechnung')
        result = self.service.record_price(group['id'], self.price(group, file))
        self.assertIsNone(result['lines'][0]['planned_total'])
        self.assertFalse(result['totals']['complete'])
        self.assertFalse(result['dispatchable'])

    def test_latest_price_uses_invoice_date_even_when_old_price_inserted_last(self):
        group = self.group('2')
        file = self.attach(group, 'rechnung')
        recent = dict(self.price(group, file, '15'), source_date='2026-09-28')
        self.service.record_price(group['id'], recent)
        old = dict(self.price(group, file, '10', position=2), source_date='2026-08-01')
        result = self.service.record_price(group['id'], old)
        self.assertEqual(result['lines'][0]['plan_price']['amount'], '15')
        self.assertEqual(result['totals']['known_subtotal'], '30.00')

    def test_conflicting_same_date_prices_are_not_resolved_by_insertion_order(self):
        group = self.group('2')
        file = self.attach(group, 'rechnung')
        self.service.record_price(group['id'], self.price(group, file, '15'))
        result = self.service.record_price(group['id'], self.price(group, file, '10', position=2))
        self.assertIsNone(result['lines'][0]['plan_price'])
        self.assertTrue(result['lines'][0]['price_warnings'])
        self.assertFalse(result['totals']['complete'])

    def test_later_catalog_import_of_older_invoice_does_not_replace_newer_plan(self):
        group = self.group()
        self.items = [self.candidate(group, 1, amount='15', date='2026-09-28'),
                      self.candidate(group, 99, amount='10', date='2026-08-01')]
        result = self.service.set_catalog_price(group['id'], group['lines'][0]['id'], {'proposal_id': 1, 'revision': group['revision']})
        result = self.service.set_catalog_price(group['id'], group['lines'][0]['id'], {'proposal_id': 99, 'revision': result['revision']})
        self.assertEqual(result['lines'][0]['plan_price']['amount'], '15')

    def test_supplier_contact_alias_is_not_automatically_invoice_supplier(self):
        payload = deepcopy(self.payload)
        payload['supplier'] = 'Mr.bean Ppg'
        group = self.service.create(payload)
        self.items = [dict(self.candidate(group), lieferant='TOP-Color')]
        self.assertEqual(self.service.catalog_candidates(group['id'], group['lines'][0]['id'])['matches'], [])
        with self.assertRaises(ValueError):
            self.service.set_catalog_price(group['id'], group['lines'][0]['id'], {'proposal_id': 1, 'revision': group['revision']})

    def test_quantity_clarification_recomputes_total_and_delivery_without_rewriting_evidence(self):
        group = self.group('5')
        invoice = self.attach(group, 'rechnung')
        self.service.record_price(group['id'], self.price(group, invoice))
        delivery = self.attach(group, 'lieferschein', 'red')
        result = self.service.record_delivery(group['id'], self.delivery(group, delivery, '2'))
        clarified = self.service.update_line(group['id'], group['lines'][0]['id'], {
            'revision': result['revision'], 'quantity': '4', 'reviewed': True, 'reason': 'Tatsächliche Bestellung am Original geprüft'})
        self.assertEqual(clarified['totals']['known_subtotal'], '40.00')
        self.assertEqual(clarified['lines'][0]['delivery_state'], 'teillieferung')
        self.assertEqual(len(clarified['lines'][0]['delivery_events']), 1)
        self.assertEqual(len(clarified['lines'][0]['prices']), 1)
        with self.assertRaises(IntakeConflict):
            self.service.update_line(group['id'], group['lines'][0]['id'], {
                'revision': clarified['revision'], 'unit': 'Stück', 'reviewed': True, 'reason': 'Test'})
        actual = self.service.detail(group['id'])
        self.assertEqual(actual['revision'], clarified['revision'])
        self.assertEqual(actual['lines'][0]['unit'], 'Karton')

    def test_identity_clarification_cannot_invalidate_only_price_evidence(self):
        group = self.group('2')
        file = self.attach(group, 'rechnung')
        result = self.service.record_price(group['id'], self.price(group, file))
        with self.assertRaises(IntakeConflict):
            self.service.update_line(group['id'], group['lines'][0]['id'], {
                'revision': result['revision'], 'variant': '30 mm', 'reviewed': True, 'reason': 'Test'})
        self.assertEqual(self.service.detail(group['id'])['totals']['known_subtotal'], '20.00')

    def test_actual_postgres_adapter_returning_and_schema_contract(self):
        # Load only the real adapter declarations; never import/start app.py.
        source = Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf-8')
        names = {'DbRow', 'PostgresCursor', 'PostgresConnection', 'split_sql_script',
                 'get_insert_table_name', 'convert_sqlite_sql_to_postgres'}
        nodes = [node for node in ast.parse(source).body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
        namespace = {'re': re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), '<actual-postgres-adapter>', 'exec'), namespace)
        statements = []

        class Cursor:
            def __init__(self, db):
                self.raw = db.cursor()
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.raw.close()
            def execute(self, sql, params):
                statements.append(sql)
                # Exercise the adapter with synthetic SQLite. This verifies
                # RETURNING/row shapes, not a running PostgreSQL server.
                translated = sql.replace('%s', '?').replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT')
                self.raw.execute(translated, params)
                self.rowcount = self.raw.rowcount
                self.description = [SimpleNamespace(name=item[0]) for item in self.raw.description] if self.raw.description else None
            def fetchall(self):
                return self.raw.fetchall()

        path = Path(self.temporary.name) / 'adapter.db'
        def get_db():
            db = sqlite3.connect(path)
            return namespace['PostgresConnection'](SimpleNamespace(cursor=lambda: Cursor(db),
                commit=db.commit, rollback=db.rollback, close=db.close))
        self.p.get_db = get_db
        self.service = MaterialIntake(self.p)
        group = self.group('2')
        file = self.attach(group)
        result = self.service.record_delivery(group['id'], self.delivery(group, file, '1'))
        self.assertEqual(result['lines'][0]['delivery_state'], 'teillieferung')
        invoice = self.attach(group, 'rechnung', 'red')
        result = self.service.record_price(group['id'], self.price(group, invoice))
        self.assertEqual(result['totals']['known_subtotal'], '20.00')
        self.assertEqual(sum('CREATE TABLE' in sql for sql in statements), len(TABLES))
        self.assertTrue(all('SERIAL PRIMARY KEY' in sql for sql in statements if 'CREATE TABLE' in sql))
        self.assertTrue(all('RETURNING id' in sql for sql in statements if sql.startswith('INSERT')))


class IntakeUiTests(unittest.TestCase):
    def setUp(self):
        from test_bestellungen import FakePortal
        from werkstatt_bestellungen import register_orders
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p = FakePortal(Path(self.temp.name))
        self.p.cockpit_data = SimpleNamespace(catalog=SimpleNamespace(knowledge_rows=lambda **kw: {'items': [], 'truncated': False}))
        self.orders = register_orders(self.p)
        self.service = register_intake(self.p)
        self.client = self.p.app.test_client()
        with self.client.session_transaction() as session:
            session['admin'] = True
            session['csrf_token'] = 'test-csrf'
        self.prefix = '/admin/assistent-bestellungen/eingang'

    def post(self, action, values):
        return self.client.post(self.prefix+'/form/'+action, data={'csrf_token': 'test-csrf', **values})

    def create(self):
        response = self.post('anlegen', {'mode': 'bereits_bestellt', 'supplier': 'Testlieferant',
            'source_key': 'test:chat:2026-10-05', 'external_ref': 'Testchat 11:02',
            'source_at': '2026-10-05T11:02', 'products': 'Testband\nTestfarbe', 'original_author': ''})
        self.assertEqual(response.status_code, 303)
        return self.service.detail(self.service.list()[0]['id'])

    def test_historical_form_renders_and_keeps_quantities_unknown_without_order_queue(self):
        group = self.create()
        self.assertTrue(group['already_ordered'])
        self.assertTrue(all(row['quantity'] is None for row in group['lines']))
        response = self.client.get(self.prefix+'/ansicht', query_string={'id': group['id']})
        self.assertEqual(response.status_code, 200)
        self.assertIn('Menge offen', response.get_data(as_text=True))
        with closing(self.p.get_db()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_bestellanforderungen').fetchone()[0], 0)

    def test_ui_admin_csrf_and_invalid_ids(self):
        self.assertEqual(self.client.post(self.prefix+'/form/anlegen', data={}).status_code, 400)
        self.assertIn(self.client.get(self.prefix+'/ansicht?id=999999').status_code, (400, 404))
        self.assertIn(self.post('auslesen', {'group_id': '999999', 'file_id': '999999'}).status_code, (400, 404))
        with self.client.session_transaction() as session:
            session.pop('admin')
        self.assertEqual(self.client.get(self.prefix+'/ansicht').status_code, 403)
        self.assertEqual(self.post('anlegen', {}).status_code, 403)

    def test_original_route_private_download_and_foreign_id(self):
        group = self.create()
        file = self.service.attach(group['id'], FileStorage(stream=io.BytesIO(png()), filename='test.png'), 'rechnung')
        path = self.prefix+f'/{group["id"]}/dateien/{file["id"]}/original'
        response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, png())
        self.assertTrue(response.headers['Content-Disposition'].startswith('attachment;'))
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(self.client.get(self.prefix+f'/999999/dateien/{file["id"]}/original').status_code, 400)
        with self.client.session_transaction() as session:
            session.pop('admin')
        self.assertEqual(self.client.get(path).status_code, 403)

    def test_price_form_requires_review_and_current_revision(self):
        group = self.create()
        line_id = group['lines'][0]['id']
        group = self.service.update_line(group['id'], line_id, {'revision': group['revision'], 'reviewed': True,
            'reason': 'Synthetisches Etikett', 'sku': 'TEST', 'unit': 'Karton', 'quantity': '2'})
        file = self.service.attach(group['id'], FileStorage(stream=io.BytesIO(png()), filename='test.png'), 'rechnung')
        group = self.service.detail(group['id'])
        form = {'group_id': group['id'], 'line_id': line_id, 'file_id': file['id'], 'position': '1', 'page': '1',
                'role': 'plan', 'amount': '10', 'unit': 'Karton', 'pack': '', 'tax_basis': 'gross', 'tax_rate': '19',
                'discount_basis': 'nach Rabatt', 'source_date': '2026-09-28', 'revision': group['revision'], 'reviewed': ''}
        self.assertEqual(self.post('preis', form).status_code, 400)
        self.assertEqual(self.post('preis', dict(form, reviewed='ja', revision=1)).status_code, 400)
        self.assertEqual(self.post('preis', dict(form, reviewed='ja')).status_code, 303)
        self.assertEqual(self.service.detail(group['id'])['totals']['known_subtotal'], '20.00')


class IntakeStorageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global fixture, p
        guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('Network forbidden in storage tests'))
        guard.start()
        cls.addClassCleanup(guard.stop)
        import test_assistent as fixture
        p = fixture.p

    def setUp(self):
        self.assertEqual(Path(p.DB).parent, Path(fixture.TEMP.name))
        with fixture.database() as db:
            for table in reversed(TABLES):
                db.execute('DELETE FROM ' + table)
        self.service = p.workshop_intake
        self.payload = dict(supplier='Synthetic Supplier', source_key='review:test:86',
            external_ref='Synthetic source', source_at='2026-10-05T11:02:00+02:00',
            already_ordered=True, original_author='Synthetic author',
            lines=[dict(product='Synthetic paint', sku='TEST-001', variant='white', unit='Can',
                        pack='1 L', quantity='3', urgent=None, category='farbe_lack')])

    def populated(self):
        group = self.service.create(copy.deepcopy(self.payload))
        group = self.service.update_line(group['id'], group['lines'][0]['id'], dict(
            revision=group['revision'], reviewed=True, reason='Synthetic explicit clarification', quantity='4'))
        delivery = self.service.attach(group['id'], FileStorage(stream=io.BytesIO(png('blue')),
            filename='synthetic-delivery.png'), 'lieferschein')
        invoice = self.service.attach(group['id'], FileStorage(stream=io.BytesIO(png('red')),
            filename='synthetic-invoice.png'), 'rechnung')
        group = self.service.record_delivery(group['id'], dict(revision=self.service.detail(group['id'])['revision'],
            line_id=group['lines'][0]['id'], file_id=delivery['id'], position=1, quantity='2', unit='Can'))
        line = group['lines'][0]
        for role, amount in [('plan','12.50'), ('invoice','15.00')]:
            group = self.service.record_price(group['id'], dict(revision=group['revision'], line_id=line['id'],
                file_id=invoice['id'], position=1 if role == 'plan' else 2, page=1, role=role, amount=amount,
                unit='Can', pack='1 L', currency='EUR', tax_basis='net', tax_rate='19',
                discount_basis='after discount; no cash discount', source_date='2026-09-28', reviewed=True,
                identity=dict(supplier=self.payload['supplier'], **{k: line[k] for k in ('product','sku','variant','unit','pack')})))
        return group

    def archive(self, *, legacy=False, mutate=None, sqlite_snapshot=False, legacy_automation=False, legacy_dialog=False, legacy_orders=False):
        from werkstatt_einkaufsmonitor import TABLES as monitor_tables
        from werkstatt_materialkanal import TABLES as channel_tables
        automation_tables = monitor_tables + channel_tables
        from werkstatt_materialdialog import TABLES as dialog_tables
        order_tables = ('assistent_bestellanforderungen', 'assistent_bestellpakete', 'assistent_bestellkontakte', 'assistent_bestellkonfiguration')
        stream = io.BytesIO()
        export = dict(format_version=p.BACKUP_FORMAT_VERSION, schema_features=list(p.BACKUP_SCHEMA_FEATURES),
                      tables={}, binary_blobs=[])
        with zipfile.ZipFile(stream,'w') as archive, closing(p.get_db()) as db:
            for table in p.BACKUP_TABLES:
                if table == 'datei_backups' or (legacy and table in TABLES) or (legacy_automation and table in automation_tables) or (legacy_dialog and table in dialog_tables) or (legacy_orders and table in order_tables):
                    continue
                rows, references, size = p.write_table_rows_and_binary_blobs(db, archive, table)
                export['tables'][table] = rows
                export['binary_blobs'].extend(references)
            if legacy:
                export['schema_features'].remove('werkstatt_einkaufseingang_v1')
            if legacy_automation:
                export['schema_features'].remove('werkstatt_materialautomatik_v1')
            if legacy_dialog:
                export['schema_features'].remove('werkstatt_materialdialog_v1')
            if legacy_orders:
                export['schema_features'].remove('werkstatt_assistent_v2')
            if mutate:
                mutate(export)
            if sqlite_snapshot:
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / 'snapshot.db'
                    with closing(sqlite3.connect(path)) as snapshot:
                        db.backup(snapshot)
                        p.strip_externalized_blobs_from_sqlite_snapshot(snapshot)
                        if legacy:
                            for table in reversed(TABLES):
                                snapshot.execute('DROP TABLE '+table)
                        if legacy_automation:
                            for table in reversed(automation_tables):
                                snapshot.execute('DROP TABLE '+table)
                        if legacy_dialog:
                            for table in reversed(dialog_tables):
                                snapshot.execute('DROP TABLE '+table)
                        if legacy_orders:
                            for table in reversed(order_tables):
                                snapshot.execute('DROP TABLE '+table)
                        snapshot.commit()
                    archive.writestr('auftraege.db', path.read_bytes())
            archive.writestr('backup.json', json.dumps(export))
            archive.writestr('manifest.json', json.dumps(dict(format_version=p.BACKUP_FORMAT_VERSION,
                binary_blob_count=len(export['binary_blobs']),
                binary_blob_bytes=sum(r['size'] for r in export['binary_blobs']))))
        stream.seek(0)
        return stream, export

    def table_rows(self):
        with closing(p.get_db()) as db:
            return {table:[dict(row) for row in db.execute('SELECT * FROM '+table+' ORDER BY id')] for table in TABLES}

    def test_full_json_roundtrip_preserves_six_tables_originals_and_price_basis(self):
        group = self.populated()
        before = self.table_rows()
        stream, export = self.archive()
        self.assertTrue(all(before[t] for t in TABLES))
        self.assertEqual(len(export['binary_blobs']),2)
        with zipfile.ZipFile(stream) as archive:
            for row in export['tables']['einkauf_eingang_dateien']:
                self.assertEqual(row['original_base64'],'')
            p.import_backup_json_rows_into_current_database(export,archive,set(archive.namelist()))
        self.assertEqual(self.table_rows(),before)
        self.assertEqual(self.service.detail(group['id']), group)
        self.assertEqual(self.service.original(group['id'], group['files'][0]['id'])[0], png('blue'))

    def test_sqlite_snapshot_externalizes_and_hydrates_exact_bytes(self):
        group=self.populated()
        stream, export=self.archive(sqlite_snapshot=True)
        with zipfile.ZipFile(stream) as archive, tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'restored.db'
            path.write_bytes(archive.read('auftraege.db'))
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM einkauf_eingang_dateien WHERE original_base64 != ''").fetchone()[0],0)
            p.hydrate_imported_sqlite_backup_blobs(path,archive,set(archive.namelist()),export)
            with closing(sqlite3.connect(path)) as db:
                for raw, sha in db.execute('SELECT original_base64,sha256 FROM einkauf_eingang_dateien'):
                    self.assertEqual(hashlib.sha256(base64.b64decode(raw)).hexdigest(),sha)

    def test_new_feature_requires_every_one_of_six_tables(self):
        self.populated()
        _, export=self.archive()
        for table in TABLES:
            with self.subTest(table=table):
                damaged=copy.deepcopy(export)
                del damaged['tables'][table]
                with self.assertRaises(ValueError):
                    p.validate_backup_binary_reference_completeness(damaged,p.backup_binary_reference_map(damaged))

    def test_legacy_json_without_intake_feature_or_tables_remains_importable(self):
        self.populated()
        stream, export=self.archive(legacy=True)
        with zipfile.ZipFile(stream) as archive:
            p.import_backup_json_rows_into_current_database(export,archive,set(archive.namelist()))
        p.workshop_intake_init_schema()
        self.assertTrue(all(not rows for rows in self.table_rows().values()))
        self.assertTrue(self.service.create(copy.deepcopy(self.payload))['id'])

    def test_legacy_sqlite_actual_admin_restore_calls_schema_hook(self):
        self.populated()
        stream, export=self.archive(legacy=True,sqlite_snapshot=True)
        client=p.app.test_client()
        with client.session_transaction() as session:
            session['admin']=True
            session['csrf_token']='synthetic-review-csrf'
        routes=len(list(p.app.url_map.iter_rules()))
        hook=p.workshop_intake_init_schema
        with patch.object(p,'workshop_intake_init_schema',wraps=hook) as spy:
            response=client.post('/admin/daten-import', data={'csrf_token':'synthetic-review-csrf',
                'datenpaket':(stream,'synthetic-old-backup.zip')},content_type='multipart/form-data')
            self.assertEqual(response.status_code,302)
            with client.session_transaction() as session:
                flashes=session.get('_flashes',[])
            self.assertTrue(any(kind=='success' for kind,text in flashes),flashes)
            spy.assert_called_once()
        self.assertEqual(len(list(p.app.url_map.iter_rules())),routes)
        self.assertTrue(all(not rows for rows in self.table_rows().values()))
        self.assertTrue(self.service.create(copy.deepcopy(self.payload))['id'])

    def test_bad_binary_hash_rejects_and_rolls_back_without_losing_existing_rows(self):
        self.populated()
        before=self.table_rows()
        stream, export=self.archive(mutate=lambda value:value['binary_blobs'][0].update(sha256='0'*64))
        with zipfile.ZipFile(stream) as archive, self.assertRaises(ValueError):
            p.import_backup_json_rows_into_current_database(export,archive,set(archive.namelist()))
        self.assertEqual(self.table_rows(),before)

    def test_pg_adapter_json_and_sqlite_import_keep_ids_reset_all_six_sequences(self):
        self.populated()
        before=self.table_rows()
        stream,export=self.archive(sqlite_snapshot=True)
        original_get_db=p.get_db
        statements=[]
        sequences={}
        class Cursor:
            def __init__(self,db):
                self.raw=db.cursor()
            def __enter__(self):
                return self
            def __exit__(self,*args):
                self.raw.close()
            def execute(self,sql,params):
                statements.append(sql)
                if 'information_schema.columns' in sql:
                    rows=self.raw.execute('PRAGMA table_info('+params[0]+')').fetchall()
                    names=['name','type'] if 'data_type' in sql else ['name']
                    self.result=[tuple(row[name] for name in names) for row in rows]
                elif 'pg_get_serial_sequence' in sql:
                    names=['sequence_name']
                    self.result=[(params[0]+'_id_seq',)]
                elif 'setval(' in sql:
                    table=params[0].removesuffix('_id_seq')
                    value=self.raw.execute('SELECT MAX(id),COUNT(*) FROM '+table).fetchone()
                    sequences[table]=(value[0] or 1,bool(value[1]))
                    names=['setval']
                    self.result=[(value[0] or 1,)]
                else:
                    self.raw.execute(sql.replace('%s','?').replace('SERIAL PRIMARY KEY','INTEGER PRIMARY KEY AUTOINCREMENT'),params)
                    self.result=self.raw.fetchall() if self.raw.description else []
                    names=[item[0] for item in self.raw.description] if self.raw.description else []
                self.rowcount=self.raw.rowcount
                self.description=[SimpleNamespace(name=name) for name in names] if names else None
            def fetchall(self):
                return self.result
        def pg_db():
            db=sqlite3.connect(p.DB)
            db.row_factory=sqlite3.Row
            return p.PostgresConnection(SimpleNamespace(cursor=lambda:Cursor(db),commit=db.commit,rollback=db.rollback,close=db.close))
        with zipfile.ZipFile(stream) as archive, tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'hydrated.db'
            path.write_bytes(archive.read('auftraege.db'))
            p.hydrate_imported_sqlite_backup_blobs(path,archive,set(archive.namelist()),export)
            for kind in ['json','sqlite']:
                with self.subTest(kind=kind):
                    sequences.clear()
                    with patch.object(p,'USE_POSTGRES',True), patch.object(p,'get_db',side_effect=pg_db), \
                         patch.object(p,'BACKUP_TABLES',list(TABLES)), \
                         patch.object(p,'portal_originals_operation_lock',side_effect=nullcontext):
                        if kind=='json':
                            p.import_backup_json_rows_into_current_database(export,archive,set(archive.namelist()))
                        else:
                            p.import_sqlite_rows_into_current_database(path)
                    self.assertEqual(self.table_rows(),before)
                    self.assertEqual(set(sequences),set(TABLES))
                    for table in TABLES:
                        self.assertEqual(sequences[table],(max(row['id'] for row in before[table]),True))
        inserts=[sql for sql in statements if sql.startswith('INSERT INTO einkauf_eingang')]
        self.assertTrue(inserts)
        self.assertTrue(all(sql.count('RETURNING id')==1 for sql in inserts))

    def test_missing_original_reference_rejected_for_json_import(self):
        self.populated()
        before=self.table_rows()
        stream, export=self.archive(mutate=lambda value:value.update(binary_blobs=[]))
        with zipfile.ZipFile(stream) as archive, self.assertRaises(ValueError):
            p.import_backup_json_rows_into_current_database(export,archive,set(archive.namelist()))
        self.assertEqual(self.table_rows(),before)

    def test_missing_original_reference_rejected_for_sqlite_restore(self):
        self.populated()
        stream, export=self.archive(sqlite_snapshot=True,mutate=lambda value:value.update(binary_blobs=[]))
        with zipfile.ZipFile(stream) as archive, tempfile.TemporaryDirectory() as directory, self.assertRaises(ValueError):
            p.extract_import_package_files(archive,set(archive.namelist()),Path(directory))


class AutomationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        IntakeStorageTests.setUpClass.__func__(cls)

    def setUp(self):
        self.client = p.app.test_client()
        with self.client.session_transaction() as state:
            state.update(admin=True, csrf_token='automation-test')
        self.secret = 'synthetic-webhook-secret'
        self.config = patch.dict(p.app.config, MATERIAL_WHATSAPP_ENABLED=True,
                                 MATERIAL_WHATSAPP_PHONE_IDS='1234', MATERIAL_WHATSAPP_WORKER_ENABLED=False,
                                 PURCHASE_MONITOR_WORKER_ENABLED=False)
        self.config.start()
        self.addCleanup(self.config.stop)

    def signed(self, payload, secret=None):
        raw = json.dumps(payload).encode()
        signature = 'sha256=' + hmac.new((secret or self.secret).encode(), raw, hashlib.sha256).hexdigest()
        return raw, {'Content-Type':'application/json','X-Hub-Signature-256':signature}

    def test_webhook_rejects_unsigned_oversized_and_malformed_before_intake(self):
        with patch.object(p,'WHATSAPP_APP_SECRET',self.secret), patch.object(p,'whatsapp_bridge_enabled',return_value=False), \
             patch.object(p.material_channel,'ingest_webhook') as intake:
            self.assertEqual(self.client.post('/webhooks/whatsapp',json={}).status_code,403)
            raw, headers = self.signed({}, secret='wrong')
            self.assertEqual(self.client.post('/webhooks/whatsapp',data=raw,headers=headers).status_code,403)
            self.assertEqual(self.client.post('/webhooks/whatsapp',data=b'x'*(256*1024+1)).status_code,413)
            raw, headers = self.signed([])
            self.assertEqual(self.client.post('/webhooks/whatsapp',data=raw,headers=headers).status_code,400)
            intake.assert_not_called()

    def test_signed_material_webhook_works_independently_of_vehicle_bridge_without_sending(self):
        payload = {'entry':[]}
        raw, headers = self.signed(payload)
        with patch.object(p,'WHATSAPP_APP_SECRET',self.secret), patch.object(p,'whatsapp_bridge_enabled',return_value=False), \
             patch.object(p.material_channel,'ingest_webhook',return_value={'accepted':1,'duplicates':0}) as intake, \
             patch.object(p,'process_whatsapp_webhook') as legacy:
            response = p.app.test_client().post('/webhooks/whatsapp',data=raw,headers=headers)
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.json['material']['accepted'],1)
            intake.assert_called_once_with(raw,headers['X-Hub-Signature-256'])
            legacy.assert_not_called()

    def test_material_channel_text_and_photos_do_not_enter_last_vehicle_chat(self):
        payload = {'entry':[{'changes':[{'value':{'metadata':{'phone_number_id':'1234'},
                    'messages':[{'id':'synthetic','type':'text','text':{'body':'dringend'}}]}}]}]}
        with patch.object(p,'handle_whatsapp_inbound_message') as legacy:
            self.assertEqual(p.process_whatsapp_webhook(payload,['1234']),0)
            legacy.assert_not_called()
        with patch.object(p,'resolve_whatsapp_reply_auftrag_id') as resolve:
            self.assertFalse(p.handle_whatsapp_inbound_message({'id':'photo','type':'image'}))
            resolve.assert_not_called()
        self.assertEqual(p.process_whatsapp_webhook({'entry':[None,{'changes':[None,{'value':[]}]}]}),0)

    def test_admin_controls_require_admin_and_csrf_before_monitor_or_mapping_changes(self):
        path='/admin/assistent-bestellungen/eingang/automatik/rechnungen-start'
        with patch.object(p.workshop_purchase_monitor,'configure') as change:
            anon=p.app.test_client()
            with anon.session_transaction() as state:
                state['csrf_token']='anonymous-test'
            self.assertIn(anon.post(path,data={'csrf_token':'anonymous-test'}).status_code,(302,403))
            self.assertEqual(self.client.post(path).status_code,400)
            change.assert_not_called()
            response=self.client.post(path,data={'csrf_token':'automation-test','interval_seconds':'300'})
            self.assertEqual(response.status_code,303)
            change.assert_called_once_with(True,interval_seconds=300)
        with patch.object(p.material_channel,'verify_sender') as assign:
            path='/admin/assistent-bestellungen/eingang/automatik/absender'
            self.assertEqual(self.client.post(path,data={'employee_id':'1'}).status_code,400)
            assign.assert_not_called()

    def test_paused_material_receiver_stays_excluded_and_legacy_requires_exact_receiver(self):
        message={'id':'synthetic','type':'text','text':{'body':'dringend'}}
        payload={'entry':[{'changes':[{'value':{'metadata':{'phone_number_id':'1234'},'messages':[message]}}]}]}
        raw,headers=self.signed(payload)
        with patch.dict(p.app.config,MATERIAL_WHATSAPP_ENABLED=False), \
             patch.object(p,'WHATSAPP_PHONE_NUMBER_ID','1234'), patch.object(p,'WHATSAPP_APP_SECRET',self.secret), \
             patch.object(p,'whatsapp_bridge_enabled',return_value=True), \
             patch.object(p,'handle_whatsapp_inbound_message',return_value=True) as legacy, \
             patch.object(p.material_channel,'ingest_webhook') as material:
            response=self.client.post('/webhooks/whatsapp',data=raw,headers=headers)
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.json['processed'],0)
            legacy.assert_not_called()
            material.assert_not_called()
            for receiver in (None,'foreign',[],1234):
                payload['entry'][0]['changes'][0]['value']['metadata']={'phone_number_id':receiver}
                self.assertEqual(p.process_whatsapp_webhook(payload),0)
            legacy.assert_not_called()
            payload['entry'][0]['changes'][0]['value']['metadata']={'phone_number_id':'1234'}
            self.assertEqual(p.process_whatsapp_webhook(payload),1)
            legacy.assert_called_once_with(message)

    def test_admin_status_renders_without_network_and_worker_start_stays_opt_in(self):
        with patch.object(p.workshop_purchase_monitor.sources,'identity',side_effect=ValueError('not configured')):
            response=self.client.get('/admin/assistent-bestellungen/eingang/ansicht')
        self.assertEqual(response.status_code,200)
        self.assertIn('Automatischer Eingang',response.text)
        self.assertIn('nicht automatisch gelesen',response.text)
        self.assertIn('Einrichtung offen',response.text)
        from werkstatt_einkaufsmonitor import start_purchase_monitor_worker
        from werkstatt_materialkanal import start_material_worker
        with patch('threading.Thread.start',side_effect=AssertionError('Unexpected worker')):
            start_purchase_monitor_worker(p)
            start_material_worker(p)

    def test_new_automation_tables_are_backed_up_and_required_only_with_feature(self):
        from werkstatt_einkaufsmonitor import TABLES as monitor_tables
        from werkstatt_materialkanal import TABLES as channel_tables
        tables = monitor_tables + channel_tables
        self.assertTrue(set(tables).issubset(set(p.BACKUP_TABLES)))
        storage = IntakeStorageTests()
        stream, export = storage.archive()
        for table in tables:
            damaged=copy.deepcopy(export)
            del damaged['tables'][table]
            with self.assertRaises(ValueError):
                p.validate_backup_binary_reference_completeness(damaged,p.backup_binary_reference_map(damaged))
        legacy=copy.deepcopy(export)
        legacy['schema_features'].remove('werkstatt_materialautomatik_v1')
        for table in tables:
            del legacy['tables'][table]
        p.validate_backup_binary_reference_completeness(legacy,p.backup_binary_reference_map(legacy))
        with fixture.database() as db:
            for table in reversed(tables):
                db.execute('DROP TABLE '+table)
        p.workshop_purchase_monitor_init_schema()
        p.material_channel_init_schema()
        with fixture.database() as db:
            for table in tables:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0)

    def test_dialog_tables_required_in_current_backup_but_optional_in_old_backup(self):
        from werkstatt_materialdialog import TABLES as tables
        self.assertTrue(set(tables).issubset(set(p.BACKUP_TABLES)))
        stream, export = IntakeStorageTests().archive()
        for table in tables:
            damaged = copy.deepcopy(export)
            del damaged['tables'][table]
            with self.assertRaises(ValueError):
                p.validate_backup_binary_reference_completeness(damaged, p.backup_binary_reference_map(damaged))
        legacy = copy.deepcopy(export)
        legacy['schema_features'].remove('werkstatt_materialdialog_v1')
        for table in tables:
            del legacy['tables'][table]
        p.validate_backup_binary_reference_completeness(legacy, p.backup_binary_reference_map(legacy))

    def test_old_sqlite_restore_recreates_dialog_schema_without_routes_or_messages(self):
        from werkstatt_materialdialog import TABLES as tables
        stream, _ = IntakeStorageTests().archive(legacy_dialog=True, sqlite_snapshot=True)
        routes = len(list(p.app.url_map.iter_rules()))
        with patch.object(p, 'material_dialog_init_schema', wraps=p.material_dialog_init_schema) as schema, \
             patch.object(p.material_dialog, 'process_next', side_effect=AssertionError('No worker during restore')):
            response = self.client.post('/admin/daten-import', data={'csrf_token':'automation-test',
                'datenpaket':(stream,'synthetic-before-material-dialog.zip')}, content_type='multipart/form-data')
            self.assertEqual(response.status_code, 302)
            with self.client.session_transaction() as state:
                self.assertTrue(any(kind == 'success' for kind, text in state.get('_flashes', [])), state.get('_flashes'))
            schema.assert_called_once()
        self.assertEqual(len(list(p.app.url_map.iter_rules())), routes)
        with fixture.database() as db:
            for table in tables:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0], 0)

    def test_pre_orders_backup_recreates_order_tables_and_schedule_without_sending(self):
        stream, _ = IntakeStorageTests().archive(legacy_orders=True, sqlite_snapshot=True)
        with patch.object(p, 'workshop_orders_init_schema', wraps=p.workshop_orders_init_schema) as schema, \
             patch.object(p.workshop_orders, 'tick', side_effect=AssertionError('No dispatcher during restore')):
            response = self.client.post('/admin/daten-import', data={'csrf_token':'automation-test',
                'datenpaket':(stream,'synthetic-before-orders.zip')}, content_type='multipart/form-data')
            self.assertEqual(response.status_code, 302)
            with self.client.session_transaction() as state:
                self.assertTrue(any(kind == 'success' for kind, text in state.get('_flashes', [])), state.get('_flashes'))
            schema.assert_called_once()
        with fixture.database() as db:
            for table in ('assistent_bestellanforderungen','assistent_bestellpakete','assistent_bestellkontakte','assistent_bestellkonfiguration'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0], 0)
            db.execute('SELECT schedule_version,legacy_due_at FROM assistent_bestellanforderungen')
            db.execute('SELECT not_before_at FROM assistent_bestellpakete')

    def test_dialog_roundtrip_preserves_proof_revision_and_uncertain_question_without_retry(self):
        from werkstatt_materialdialog import TABLES as tables
        def rows():
            with fixture.database() as db:
                return {table:[dict(row) for row in db.execute('SELECT * FROM '+table+' ORDER BY id')] for table in tables}
        try:
            with fixture.database() as db:
                for table in reversed(tables):
                    db.execute('DELETE FROM '+table)
                db.execute('''INSERT INTO einkauf_material_dialoge(message_id,revision,state,fields_json,
                    snapshot_json,snapshot_hash,dispatch_id,dispatch_state,created_at,updated_at)
                    VALUES(912,3,'accepted',?,?,'synthetic-hash','synthetic-order','queued',1,2)''',
                    (json.dumps({'quantity':{'value':'1','proof':{'kind':'text','id':12}}}), json.dumps({'request_key':'material:1'})))
                draft_id = db.execute('SELECT id FROM einkauf_material_dialoge').fetchone()[0]
                db.execute('''INSERT INTO einkauf_material_texte(phone_number_id,wamid,canonical_hash,sender_id,
                    sender_revision,employee_id,rights_version,reply_to,body,source_at,state,draft_id,bound_revision,created_at)
                    VALUES('synthetic-phone','synthetic-wamid','hash',1,2,5,3,'synthetic-photo','ein Karton',
                        '2026-10-06T08:00:00+00:00','applied',?,2,1)''', (draft_id,))
                db.execute('''INSERT INTO einkauf_material_rueckfragen(draft_id,revision,field,body,state,error_code,created_at,updated_at)
                    VALUES(?,2,'quantity','Synthetische Mengenfrage','uncertain','sendestatus_unklar_nicht_erneut_senden',1,2)''', (draft_id,))
            before = rows()
            stream, export = IntakeStorageTests().archive()
            with zipfile.ZipFile(stream) as archive:
                p.import_backup_json_rows_into_current_database(export, archive, set(archive.namelist()))
            self.assertEqual(rows(), before)
        finally:
            with fixture.database() as db:
                for table in reversed(tables):
                    db.execute('DELETE FROM '+table)

    def test_price_evidence_keeps_explicit_currency_and_tax_without_inventing_values(self):
        from werkstatt_artikel_import import _price_evidence
        value = _price_evidence({'value':'12.50','currency':'CHF','tax_basis':'net','tax_rate':'8,1','verified':True})
        self.assertEqual((value['currency'],value['tax_basis'],value['tax_rate']),('CHF','net','8.1'))
        self.assertFalse(value['verified'])
        for invalid in ({},{'currency':'anything','tax_basis':'assumed','tax_rate':'200'}):
            value=_price_evidence(invalid)
            self.assertNotIn('currency',value)
            self.assertNotIn('tax_basis',value)
            self.assertNotIn('tax_rate',value)

    def test_actual_old_backup_restore_recreates_automation_without_duplicate_routes(self):
        storage=IntakeStorageTests()
        stream, export=storage.archive(legacy_automation=True,sqlite_snapshot=True)
        routes=len(list(p.app.url_map.iter_rules()))
        with patch.object(p,'workshop_purchase_monitor_init_schema',wraps=p.workshop_purchase_monitor_init_schema) as monitor, \
             patch.object(p,'material_channel_init_schema',wraps=p.material_channel_init_schema) as channel:
            response=self.client.post('/admin/daten-import',data={'csrf_token':'automation-test',
                'datenpaket':(stream,'synthetic-before-automation.zip')},content_type='multipart/form-data')
            self.assertEqual(response.status_code,302)
            with self.client.session_transaction() as state:
                self.assertTrue(any(kind=='success' for kind,text in state.get('_flashes',[])),state.get('_flashes'))
            monitor.assert_called_once()
            channel.assert_called_once()
        self.assertEqual(len(list(p.app.url_map.iter_rules())),routes)

    def test_automation_history_roundtrip_keeps_identity_and_retries(self):
        from werkstatt_einkaufsmonitor import TABLES as monitor_tables
        from werkstatt_materialkanal import TABLES as channel_tables
        tables=monitor_tables+channel_tables
        def rows():
            with fixture.database() as db:
                return {table:[dict(row) for row in db.execute('SELECT * FROM '+table+' ORDER BY id')] for table in tables}
        try:
            with fixture.database() as db:
                for table in reversed(tables):
                    db.execute('DELETE FROM '+table)
                db.execute("INSERT INTO assistent_einkaufsmonitor(account,enabled,updated_at,configured_by) VALUES('synthetic',0,'2026-10-05','admin')")
                db.execute("INSERT INTO assistent_einkaufsmonitor_quellen(account,file_id,beleg_id,state,attempts,next_retry_at,created_at,updated_at) VALUES('synthetic',17,19,'queued',2,12345,'2026-10-05','2026-10-05')")
                db.execute("INSERT INTO einkauf_material_absender(phone_e164,employee_id,source_note,verified_at,verified_by,active,revision) VALUES('49123456789',55,'synthetic','2026-10-05','admin',0,2)")
                sender=db.execute('SELECT id FROM einkauf_material_absender').fetchone()[0]
                db.execute('''INSERT INTO einkauf_material_nachrichten(phone_number_id,wamid,canonical_hash,sender_id,sender_revision,
                    employee_id,employee_name,rights_version,media_id,mime,expected_sha256,source_at,received_at,updated_at,state,attempts)
                    VALUES('1234','synthetic-wamid','hash',?,2,55,'Testperson',3,'5678','image/png','hash','2026-10-05',1,1,'blocked',2)''',(sender,))
                db.execute("INSERT INTO einkauf_material_worker(worker_key,heartbeat_at,last_error) VALUES('material',100,'synthetic')")
            before=rows()
            storage=IntakeStorageTests()
            stream,export=storage.archive()
            with zipfile.ZipFile(stream) as archive:
                p.import_backup_json_rows_into_current_database(export,archive,set(archive.namelist()))
            self.assertEqual(rows(),before)
        finally:
            with fixture.database() as db:
                for table in reversed(tables):
                    db.execute('DELETE FROM '+table)


if __name__ == '__main__':
    unittest.main()
