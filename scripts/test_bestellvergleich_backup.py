"""Exercise actual restore paths with synthetic price records; no app or workers."""
import ast
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
import io
import json
import os
from pathlib import Path
import pathlib
import re
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock
import zipfile

from flask import Flask, flash, redirect, request, url_for

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_bestellvergleich import OrderPriceComparison, TABLES as PRICE_TABLES

TABLES = (*PRICE_TABLES, 'ordinary')


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


def connection(path):
    db = sqlite3.connect(path, factory=ClosingConnection)
    db.row_factory = sqlite3.Row
    return db


class ComparisonBackupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tree = ast.parse(ROOT.joinpath('app.py').read_text(encoding='utf-8'))
        names = {'portal_originals_operation_lock', 'portal_originals_locked',
                 'ensure_material_external_claims_for_import', 'admin_daten_import',
                 'import_backup_json_rows_into_current_database', 'import_sqlite_rows_into_current_database'}
        nodes = [node for node in cls.tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        assert {node.name for node in nodes} == names
        cls.code = compile(ast.Module(body=nodes, type_ignores=[]), '<actual-comparison-restore>', 'exec')

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='comparison-restore-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db_path = self.root / 'target.db'
        self.portal = type('Portal', (), {'get_db': lambda _: connection(self.db_path)})()
        self.comparison = OrderPriceComparison(self.portal)
        with connection(self.db_path) as db:
            db.execute('CREATE TABLE ordinary (id INTEGER PRIMARY KEY,value TEXT)')
            db.execute('INSERT INTO ordinary VALUES(1,?)', ('current',))
        self.app = Flask(__name__)
        self.app.secret_key = 'synthetic-only'
        self.app.testing = True
        self.app.add_url_rule('/dashboard', endpoint='dashboard', view_func=lambda: 'ok')
        self.ns = {
            'contextmanager': contextmanager, 'wraps': wraps, 'os': os, 'sqlite3': sqlite3,
            'pathlib': pathlib, 'tempfile': tempfile, 'zipfile': zipfile, 'datetime': datetime,
            'USE_POSTGRES': False, 'DATA_DIR': self.root, 'DB': self.db_path,
            'PORTAL_ORIGINALS_FILE_LOCK': self.root / 'restore.lock',
            '_portal_originals_thread_lock': threading.RLock(), '_portal_originals_lock_state': threading.local(),
            'get_db': self.portal.get_db,
            'get_table_columns': lambda db, table: {row['name'] for row in db.execute(f'PRAGMA table_info({table})')},
            'get_table_column_types': lambda db, table: {}, 'normalize_import_value': lambda value, _type: value,
            'BACKUP_TABLES': TABLES, 'BACKUP_BINARY_FIELDS': {}, 'backup_binary_reference_map': lambda export: {},
            'validate_backup_binary_reference_completeness': lambda *args: None,
            'ensure_no_database_only_originals_for_import': lambda *args: None,
            'ensure_no_unrestorable_mos_data_for_import': lambda: None,
            'app': self.app, 'admin_required': lambda fn: fn, 'request': request, 'flash': flash,
            'redirect': redirect, 'url_for': url_for, 'clean_text': str,
            'log_import_package_event': lambda *args, **kwargs: None,
            'validate_import_package_archive': lambda archive: (archive.namelist(), {}),
            'create_backup_package': Mock(), 'copy_sqlite_database_snapshot': Mock(),
            'replace_uploads_from_import': Mock(), 'init_db': Mock(),
            'order_price_comparison_init_schema': Mock(wraps=self.comparison.init_schema)}
        exec(self.code, self.ns)
        self.client = self.app.test_client()

    def seed(self, table=PRICE_TABLES[0], key='material:1'):
        values = dict(order_key=key, order_fingerprint='frozen-order', payload_hash='frozen-price',
                      payload_json=json.dumps({'amount': '12.34', 'source': {'page': 2}}),
                      dispatch_state_at_capture='external_sent', created_at='2026-10-06T17:00:00+00:00',
                      created_by='admin')
        if table == PRICE_TABLES[1]:
            values['source_key'] = 'original:synthetic-sha256:2:1'
        with connection(self.db_path) as db:
            db.execute(f'INSERT INTO {table} ({",".join(values)}) VALUES ({",".join("?" for _ in values)})', tuple(values.values()))

    def export(self):
        with connection(self.db_path) as db:
            return {'tables': {table: [dict(row) for row in db.execute(f'SELECT * FROM {table}')]
                               for table in TABLES}}

    def sqlite_source(self, export, omit_prices=False):
        path = self.root / 'incoming.db'
        portal = type('Portal', (), {'get_db': lambda _: connection(path)})()
        OrderPriceComparison(portal)
        with connection(path) as db:
            db.execute('CREATE TABLE ordinary(id INTEGER PRIMARY KEY,value TEXT)')
            for table, rows in export['tables'].items():
                for row in rows:
                    db.execute(f'INSERT INTO {table} ({",".join(row)}) VALUES ({",".join("?" for _ in row)})', tuple(row.values()))
            if omit_prices:
                for table in PRICE_TABLES:
                    db.execute(f'DROP TABLE {table}')
        return path

    def assert_rejected_unchanged(self, export):
        before = self.export()
        with self.assertRaisesRegex(ValueError, 'Bestellpreis-Nachweise'):
            self.ns['import_backup_json_rows_into_current_database'](export, None, [])
        self.assertEqual(self.export(), before)

    def test_old_json_cannot_erase_estimate_or_invoice(self):
        old = self.export()
        for table in PRICE_TABLES:
            with self.subTest(table=table):
                self.seed(table)
                self.assert_rejected_unchanged(old)

    def test_old_json_cannot_change_any_frozen_field(self):
        for table in PRICE_TABLES:
            self.seed(table)
        for table in PRICE_TABLES:
            for field in self.export()['tables'][table][0]:
                with self.subTest(table=table, field=field):
                    changed = self.export()
                    changed['tables'][table][0][field] = 20 if field == 'id' else 'replaced'
                    self.assert_rejected_unchanged(changed)

    def test_missing_or_duplicate_record_is_rejected(self):
        self.seed()
        for mutation in ('table', 'field', 'duplicate'):
            changed = self.export()
            if mutation == 'table':
                del changed['tables'][PRICE_TABLES[0]]
            elif mutation == 'field':
                del changed['tables'][PRICE_TABLES[0]][0]['payload_hash']
            else:
                changed['tables'][PRICE_TABLES[0]] *= 2
            self.assert_rejected_unchanged(changed)

    def test_current_json_restores_unrelated_data_and_both_records(self):
        for table in PRICE_TABLES:
            self.seed(table)
        current = self.export()
        current['tables']['ordinary'][0]['value'] = 'restored'
        self.ns['import_backup_json_rows_into_current_database'](current, None, [])
        self.assertEqual(self.export(), current)

    def test_legacy_json_without_prices_still_restores_before_first_capture(self):
        current = self.export()
        for table in PRICE_TABLES:
            del current['tables'][table]
        current['tables']['ordinary'][0]['value'] = 'legacy'
        self.ns['import_backup_json_rows_into_current_database'](current, None, [])
        self.assertEqual(self.export()['tables']['ordinary'], current['tables']['ordinary'])

    def test_old_sqlite_rows_cannot_erase_prices(self):
        path = self.sqlite_source(self.export(), omit_prices=True)
        self.seed()
        before = self.export()
        with self.assertRaisesRegex(ValueError, 'Bestellpreis-Nachweise'):
            self.ns['import_sqlite_rows_into_current_database'](path)
        self.assertEqual(self.export(), before)

    def test_current_sqlite_restores_unrelated_data_and_prices(self):
        for table in PRICE_TABLES:
            self.seed(table)
        current = self.export()
        current['tables']['ordinary'][0]['value'] = 'SQLite restore'
        self.ns['import_sqlite_rows_into_current_database'](self.sqlite_source(current))
        self.assertEqual(self.export(), current)

    def post_backup(self):
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, 'w') as archive:
            archive.writestr('placeholder', 'synthetic')
        payload.seek(0)
        return self.client.post('/admin/daten-import', data={'datenpaket': (payload, 'backup.zip')})

    def test_sqlite_file_wins_over_newer_json_before_any_mutation(self):
        path = self.sqlite_source(self.export(), omit_prices=True)
        self.seed()
        current = self.export()
        self.ns['extract_import_package_files'] = lambda *args: (path, self.root / 'uploads', current)
        self.assertEqual(self.post_backup().status_code, 302)
        with self.client.session_transaction() as session:
            self.assertTrue(any('Bestellpreis-Nachweise' in message for _, message in session['_flashes']))
        for name in ('create_backup_package', 'copy_sqlite_database_snapshot', 'replace_uploads_from_import', 'init_db'):
            self.ns[name].assert_not_called()
        self.assertEqual(self.export(), current)

    def test_legacy_sqlite_replacement_recreates_new_schema(self):
        path = self.sqlite_source(self.export(), omit_prices=True)
        self.ns['extract_import_package_files'] = lambda *args: (path, self.root / 'uploads', None)
        def replace(source, destination, **_):
            if Path(destination) == self.db_path:
                with connection(self.db_path) as db:
                    for table in PRICE_TABLES:
                        db.execute(f'DROP TABLE {table}')
        self.ns['copy_sqlite_database_snapshot'].side_effect = replace
        self.assertEqual(self.post_backup().status_code, 302)
        self.ns['order_price_comparison_init_schema'].assert_called_once_with()
        self.assertEqual(self.export()['tables'][PRICE_TABLES[0]], [])

    def test_restore_waits_for_price_capture_lock(self):
        old = self.export()
        begun, done = threading.Event(), threading.Event()
        errors = []
        def restore():
            begun.set()
            try:
                self.ns['import_backup_json_rows_into_current_database'](old, None, [])
            except ValueError as exc:
                errors.append(str(exc))
            finally:
                done.set()
        with self.ns['portal_originals_operation_lock']():
            worker = threading.Thread(target=restore)
            worker.start()
            self.assertTrue(begun.wait(2))
            self.assertFalse(done.wait(.05))
            self.seed()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertTrue(errors and 'Bestellpreis-Nachweise' in errors[0])
        self.assertEqual(len(self.export()['tables'][PRICE_TABLES[0]]), 1)

    def test_incoming_sqlite_is_not_queried_as_postgres(self):
        self.seed()
        path = self.sqlite_source(self.export())
        target = connection(self.db_path)
        self.addCleanup(target.close)
        self.ns['USE_POSTGRES'] = True
        schema = self.ns['get_table_columns']
        self.ns['get_table_columns'] = lambda db, table: schema(db, table) if db is target else self.fail('SQLite source queried through PG')
        self.ns['ensure_material_external_claims_for_import'](imported_db=path, target=target)

    def test_backup_feature_requires_both_tables_but_legacy_remains_valid(self):
        names = {'BACKUP_TABLES', 'BACKUP_SCHEMA_FEATURES', 'BACKUP_EXTERNALIZED_BINARY_FORMAT_VERSION'}
        values = {}
        for node in self.tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id in names:
                values[node.targets[0].id] = ast.literal_eval(node.value)
        self.assertTrue(set(PRICE_TABLES) <= set(values['BACKUP_TABLES']))
        self.assertIn('werkstatt_bestellvergleich_v1', values['BACKUP_SCHEMA_FEATURES'])
        node = next(node for node in self.tree.body if isinstance(node, ast.FunctionDef) and node.name == 'validate_backup_binary_reference_completeness')
        values.update(BACKUP_BINARY_FIELDS={}, clean_text=str)
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<actual-backup-features>', 'exec'), values)
        legacy = {'format_version': 4, 'tables': {name: [] for name in values['BACKUP_TABLES'] if name not in PRICE_TABLES}}
        validate = values['validate_backup_binary_reference_completeness']
        validate(legacy, {})
        legacy['schema_features'] = ['werkstatt_bestellvergleich_v1']
        with self.assertRaisesRegex(ValueError, 'Tabellen fehlen'):
            validate(legacy, {})
        legacy['tables'].update({name: [] for name in PRICE_TABLES})
        validate(legacy, {})

    def test_real_postgres_adapter_converts_new_schema(self):
        names = {'PostgresCursor', 'PostgresConnection', 'split_sql_script', 'get_insert_table_name', 'convert_sqlite_sql_to_postgres'}
        nodes = [node for node in self.tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
        namespace = {'re': re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), '<actual-pg-adapter>', 'exec'), namespace)
        statements = []
        cursor = Mock(rowcount=0, description=None)
        cursor.execute.side_effect = lambda sql, params: statements.append(sql)
        manager = Mock()
        manager.__enter__ = Mock(return_value=cursor)
        manager.__exit__ = Mock(return_value=False)
        pg = Mock()
        pg.cursor.return_value = manager
        adapter = namespace['PostgresConnection'](pg)
        portal = type('Portal', (), {'get_db': lambda _: adapter})()
        OrderPriceComparison(portal)
        self.assertEqual(len(statements), 2)
        self.assertTrue(all('SERIAL PRIMARY KEY' in sql and 'AUTOINCREMENT' not in sql for sql in statements))
        pg.commit.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
