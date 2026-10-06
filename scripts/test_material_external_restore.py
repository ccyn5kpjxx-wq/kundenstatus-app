"""Synthetic restore regressions, without app startup, workers or network.

Run the actual import/guard/route functions against temporary SQLite stores.
An external mail is irreversible: restoring an older backup must not release
its claim even if the employee or automatic channel has since been paused.
"""
import ast
from contextlib import contextmanager
from datetime import datetime
from functools import wraps
import io
import json
import os
from pathlib import Path
import pathlib
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import Mock
import zipfile

from flask import Flask, flash, redirect, request, url_for


TABLES = ('einkauf_material_dialoge', 'assistent_audit', 'ordinary')
SCHEMA = '''
CREATE TABLE einkauf_material_dialoge (
 id INTEGER PRIMARY KEY, message_id INTEGER, revision INTEGER, state TEXT,
 fields_json TEXT, snapshot_json TEXT, snapshot_hash TEXT, dispatch_id TEXT,
 dispatch_state TEXT, updated_at REAL);
CREATE TABLE assistent_audit (
 id INTEGER PRIMARY KEY, actor TEXT, auftrag_id INTEGER, aktion TEXT, details TEXT, zeit TEXT);
CREATE TABLE ordinary (id INTEGER PRIMARY KEY, value TEXT);
'''


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


class ExternalRestoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        names = {'portal_originals_operation_lock', 'portal_originals_locked',
                 'ensure_material_external_claims_for_import',
                 'import_backup_json_rows_into_current_database',
                 'import_sqlite_rows_into_current_database', 'admin_daten_import'}
        tree = ast.parse(Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf-8'))
        nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
        if {node.name for node in nodes} != names:
            raise AssertionError('Actual restore functions missing')
        cls.code = compile(ast.Module(body=nodes, type_ignores=[]), '<actual-material-restore>', 'exec')

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='material-external-restore-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db_path = self.root / 'target.db'
        with connection(self.db_path) as db:
            db.executescript(SCHEMA)
            db.execute('INSERT INTO ordinary VALUES(1,?)', ('current',))
        self.app = Flask(__name__)
        self.app.secret_key = 'synthetic-only-secret'
        self.app.testing = True
        self.app.add_url_rule('/dashboard', endpoint='dashboard', view_func=lambda: 'ok')
        self.ns = {'contextmanager': contextmanager, 'wraps': wraps, 'os': os,
            'sqlite3': sqlite3, 'pathlib': pathlib, 'tempfile': tempfile, 'zipfile': zipfile,
            'datetime': datetime, 'USE_POSTGRES': False, 'DATA_DIR': self.root, 'DB': self.db_path,
            'PORTAL_ORIGINALS_FILE_LOCK': self.root / 'restore.lock',
            '_portal_originals_thread_lock': threading.RLock(), '_portal_originals_lock_state': threading.local(),
            'get_db': lambda: connection(self.db_path),
            'get_table_columns': lambda db, table: {row['name'] for row in db.execute(f'PRAGMA table_info({table})')},
            'get_table_column_types': lambda db, table: {},
            'normalize_import_value': lambda value, _type: value, 'BACKUP_TABLES': TABLES,
            'BACKUP_BINARY_FIELDS': {}, 'backup_binary_reference_map': lambda export: {},
            'validate_backup_binary_reference_completeness': lambda *args: None,
            'ensure_no_database_only_originals_for_import': lambda *args: None,
            'ensure_no_unrestorable_mos_data_for_import': lambda: None,
            'app': self.app, 'admin_required': lambda fn: fn, 'request': request, 'flash': flash,
            'redirect': redirect, 'url_for': url_for, 'clean_text': str,
            'log_import_package_event': lambda *args, **kwargs: None,
            'validate_import_package_archive': lambda archive: (archive.namelist(), {}),
            'create_backup_package': Mock(), 'copy_sqlite_database_snapshot': Mock(),
            'replace_uploads_from_import': Mock(), 'init_db': Mock()}
        exec(self.code, self.ns)
        self.client = self.app.test_client()

    def seed_claim(self, state='external_pending'):
        snapshot = {'kind': 'manual_external_order', 'reservation_id': 'a' * 32,
                    'draft_id': 1, 'recipient': 'orders@example.invalid', 'max_total_cents': 25000}
        if state == 'external_sent':
            snapshot.update(sent_at='2026-10-06T16:20:00+00:00', send_evidence='synthetic sent item')
        with connection(self.db_path) as db:
            db.execute('INSERT INTO einkauf_material_dialoge VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (1, 7, 5, state, '{"quantity":{"value":"1"}}', json.dumps(snapshot),
                        'synthetic-snapshot-hash', '', '', 10.0))
            db.execute('INSERT INTO assistent_audit VALUES(?,?,?,?,?,?)',
                       (9, 'admin', None, 'material_external_reserved',
                        json.dumps({'draft_id': 1, 'reservation_id': 'a' * 32}), 'synthetic-time'))
            if state == 'external_sent':
                db.execute('INSERT INTO assistent_audit VALUES(?,?,?,?,?,?)',
                           (10, 'admin', None, 'material_external_sent',
                            json.dumps({'draft_id': 1, 'reservation_id': 'a' * 32}), 'synthetic-send-time'))

    def export(self):
        with connection(self.db_path) as db:
            return {'tables': {table: [dict(row) for row in db.execute(f'SELECT * FROM {table}')]
                               for table in TABLES}}

    def sqlite_source(self, export, *, omit_material_table=False):
        path = self.root / 'incoming.db'
        with connection(path) as db:
            db.executescript(SCHEMA)
            for table, rows in export['tables'].items():
                for row in rows:
                    keys = list(row)
                    db.execute(f'INSERT INTO {table} ({",".join(keys)}) VALUES ({",".join("?" for _ in keys)})',
                               tuple(row[key] for key in keys))
            if omit_material_table:
                db.execute('DROP TABLE einkauf_material_dialoge')
        return path

    def assert_json_rejected_unchanged(self, export):
        before = self.export()
        with self.assertRaisesRegex(ValueError, 'externe Bestellreservierungen'):
            self.ns['import_backup_json_rows_into_current_database'](export, None, [])
        self.assertEqual(self.export(), before)

    def test_old_json_cannot_remove_pending_or_uncertain_send(self):
        old = self.export()
        self.seed_claim()  # Pending also covers uncertain SMTP/provider outcome; never auto-retry.
        self.assert_json_rejected_unchanged(old)

    def test_sent_claim_cannot_be_downgraded_to_pending(self):
        self.seed_claim('external_sent')
        older = self.export()
        older['tables']['einkauf_material_dialoge'][0]['state'] = 'external_pending'
        self.assert_json_rejected_unchanged(older)

    def test_frozen_order_and_source_link_cannot_be_replaced(self):
        self.seed_claim()
        for key, value in [('message_id', 8), ('revision', 4), ('fields_json', '{}'),
                           ('snapshot_json', '{}'), ('snapshot_hash', 'different'),
                           ('dispatch_id', 'another'), ('dispatch_state', 'queued')]:
            with self.subTest(key=key):
                older = self.export()
                older['tables']['einkauf_material_dialoge'][0][key] = value
                self.assert_json_rejected_unchanged(older)

    def test_audit_cannot_be_removed_or_changed(self):
        self.seed_claim('external_sent')
        for change in ('remove', 'details', 'actor'):
            with self.subTest(change=change):
                older = self.export()
                if change == 'remove':
                    older['tables']['assistent_audit'].pop()
                else:
                    older['tables']['assistent_audit'][0][change] = 'changed'
                self.assert_json_rejected_unchanged(older)

    def test_orphan_audit_still_protects_history(self):
        self.seed_claim()
        with connection(self.db_path) as db:
            db.execute('DELETE FROM einkauf_material_dialoge')
        old = self.export()
        old['tables']['assistent_audit'] = []
        self.assert_json_rejected_unchanged(old)

    def test_compatible_json_allows_restore_of_other_data(self):
        self.seed_claim('external_sent')
        current = self.export()
        current['tables']['ordinary'][0]['value'] = 'restored'
        self.ns['import_backup_json_rows_into_current_database'](current, None, [])
        self.assertEqual(self.export(), current)

    def test_legacy_restore_without_external_claim_is_unchanged(self):
        old = self.export()
        old['tables']['ordinary'][0]['value'] = 'legacy'
        self.ns['import_backup_json_rows_into_current_database'](old, None, [])
        self.assertEqual(self.export(), old)

    def test_old_sqlite_rows_block_before_delete(self):
        path = self.sqlite_source(self.export(), omit_material_table=True)
        self.seed_claim()
        before = self.export()
        with self.assertRaisesRegex(ValueError, 'externe Bestellreservierungen'):
            self.ns['import_sqlite_rows_into_current_database'](path)
        self.assertEqual(self.export(), before)

    def test_compatible_sqlite_rows_restore_succeeds(self):
        self.seed_claim('external_sent')
        current = self.export()
        current['tables']['ordinary'][0]['value'] = 'sqlite restored'
        self.ns['import_sqlite_rows_into_current_database'](self.sqlite_source(current))
        self.assertEqual(self.export(), current)

    def test_sqlite_source_inspection_does_not_use_postgres_schema_queries(self):
        self.seed_claim()
        path = self.sqlite_source(self.export())
        self.ns['USE_POSTGRES'] = True
        # The live target might be PostgreSQL but the incoming file remains SQLite.
        target = connection(self.db_path)
        self.addCleanup(target.close)
        schema = self.ns['get_table_columns']
        self.ns['get_table_columns'] = lambda db, table: schema(db, table) if db is target else self.fail('source treated as PostgreSQL')
        self.ns['ensure_material_external_claims_for_import'](imported_db=path, target=target)

    def test_route_inspects_actual_sqlite_not_matching_json_and_stops_before_changes(self):
        path = self.sqlite_source(self.export())
        self.seed_claim('external_sent')
        before = self.export()
        self.ns['extract_import_package_files'] = lambda *args: (path, self.root / 'uploads', before)
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, 'w') as archive:
            archive.writestr('placeholder', 'synthetic')
        payload.seek(0)
        response = self.client.post('/admin/daten-import', data={'datenpaket': (payload, 'backup.zip')})
        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as session:
            self.assertTrue(any('externe Bestellreservierungen' in message for _, message in session['_flashes']))
        for name in ('create_backup_package', 'copy_sqlite_database_snapshot', 'replace_uploads_from_import', 'init_db'):
            self.ns[name].assert_not_called()
        self.assertEqual(self.export(), before)

    def test_restore_waits_for_reservation_lock_then_sees_new_claim(self):
        old = self.export()
        begun, done = threading.Event(), threading.Event()
        errors = []
        def import_old():
            begun.set()
            try:
                self.ns['import_backup_json_rows_into_current_database'](old, None, [])
            except ValueError as exc:
                errors.append(str(exc))
            finally:
                done.set()
        with self.ns['portal_originals_operation_lock']():
            worker = threading.Thread(target=import_old)
            worker.start()
            self.assertTrue(begun.wait(2))
            self.assertFalse(done.wait(.05))
            self.seed_claim()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertTrue(errors and 'externe Bestellreservierungen' in errors[0])
        self.assertEqual(self.export()['tables']['einkauf_material_dialoge'][0]['state'], 'external_pending')


if __name__ == '__main__':
    unittest.main()
