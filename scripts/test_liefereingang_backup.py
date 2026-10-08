"""Run real restore paths against synthetic confirmed deliveries, without app workers."""
import ast
import base64
import hashlib
import hmac
import io
import json
from pathlib import Path
import re
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
import zipfile

from PIL import Image
from werkzeug.datastructures import FileStorage

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import test_bestellvergleich_backup as comparison_backup
from werkstatt_einkaufseingang import MaterialIntake, TABLES as INTAKE_TABLES
from werkstatt_liefereingang import OrderDelivery, get_delivery

DELIVERIES = 'assistent_bestelllieferungen'
TABLES = (*comparison_backup.PRICE_TABLES, *INTAKE_TABLES, DELIVERIES, 'ordinary')
connection = comparison_backup.connection


class DeliveryBackupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        comparison_backup.ComparisonBackupTests.setUpClass.__func__(cls)

    def setUp(self):
        comparison_backup.ComparisonBackupTests.setUp(self)
        self.portal.order_price_comparison = self.comparison
        self.portal.portal_originals_operation_lock = self.ns['portal_originals_operation_lock']
        self.intake = self.portal.workshop_intake = MaterialIntake(self.portal)
        self.delivery = get_delivery(self.portal)
        self.order = dict(key='material:synthetic', fingerprint='frozen-order',
                          supplier='Synthetic supplier', product='Synthetic paint', sku='SYN-1',
                          variant='silver 0.5 l', unit='Dose', quantity='1',
                          created_at='2026-10-07T12:00:00+00:00')
        self.comparison._order = lambda db, key: dict(self.order)
        # Keep the parent portal namespace and its real employee restore guards.
        # The extracted imports resolve that portal through sys.modules[__name__].
        self.ns.update(
            BACKUP_TABLES=TABLES, json=json, base64=base64,
            workshop_intake_init_schema=Mock(wraps=self.intake.init_schema),
            order_delivery_init_schema=Mock(wraps=self.delivery.init_schema))

    def seed(self):
        # A valid in-memory PNG: no customer document or live database is used.
        buffer = io.BytesIO()
        Image.new('RGB', (2, 2), 'white').save(buffer, format='PNG')
        raw = buffer.getvalue()
        _, group, file = self.delivery.attach(self.order['key'], FileStorage(
            stream=io.BytesIO(raw), filename='synthetic.png', content_type='image/png'))
        self.delivery.record(self.order['key'], dict(
            group_id=group['id'], file_id=file['id'], page=1, position=0, quantity='1',
            supplier=self.order['supplier'], sku=self.order['sku'], variant=self.order['variant'],
            unit=self.order['unit'], reviewed=True))
        return group, file

    def export(self):
        with connection(self.db_path) as db:
            return {'tables': {table: [dict(row) for row in db.execute(f'SELECT * FROM {table}')]
                               for table in TABLES}}

    def sqlite_source(self, export, omit_delivery=False):
        path = self.root / 'incoming.db'
        portal = SimpleNamespace(get_db=lambda: connection(path))
        portal.order_price_comparison = comparison_backup.OrderPriceComparison(portal)
        MaterialIntake(portal)
        OrderDelivery(portal)
        with connection(path) as db:
            db.execute('CREATE TABLE ordinary(id INTEGER PRIMARY KEY,value TEXT)')
            for table, rows in export['tables'].items():
                for row in rows:
                    db.execute(f'INSERT INTO {table} ({",".join(row)}) VALUES ({",".join("?" for _ in row)})',
                               tuple(row.values()))
            if omit_delivery:
                db.execute(f'DROP TABLE {DELIVERIES}')
        return path

    def assert_rejected_unchanged(self, export):
        before = self.export()
        with self.assertRaisesRegex(ValueError, 'bestätigte Lieferungen'):
            self.ns['import_backup_json_rows_into_current_database'](export, None, [])
        self.assertEqual(self.export(), before)

    post_backup = comparison_backup.ComparisonBackupTests.post_backup

    def test_old_json_cannot_erase_confirmed_delivery(self):
        old = self.export()
        self.seed()
        self.assert_rejected_unchanged(old)

    def test_each_frozen_delivery_field_is_preserved(self):
        self.seed()
        for field in self.export()['tables'][DELIVERIES][0]:
            with self.subTest(field=field):
                changed = self.export()
                changed['tables'][DELIVERIES][0][field] = 20 if field == 'id' else 'replaced'
                self.assert_rejected_unchanged(changed)

    def test_missing_and_duplicate_delivery_records_are_rejected(self):
        self.seed()
        for mutation in ('table', 'field', 'duplicate'):
            with self.subTest(mutation=mutation):
                changed = self.export()
                if mutation == 'table':
                    del changed['tables'][DELIVERIES]
                elif mutation == 'field':
                    del changed['tables'][DELIVERIES][0]['source_key']
                else:
                    changed['tables'][DELIVERIES] *= 2
                self.assert_rejected_unchanged(changed)

    def test_confirmed_delivery_keeps_source_original_and_group(self):
        self.seed()
        for table in ('einkauf_eingang', 'einkauf_eingang_dateien'):
            for mutation in ('missing', 'changed'):
                with self.subTest(table=table, mutation=mutation):
                    changed = self.export()
                    if mutation == 'missing':
                        changed['tables'][table] = []
                    else:
                        field = 'supplier' if table == 'einkauf_eingang' else 'original_base64'
                        changed['tables'][table][0][field] = 'replaced'
                    self.assert_rejected_unchanged(changed)

    def test_current_json_restores_data_and_preserves_status_and_original(self):
        group, file = self.seed()
        current = self.export()
        current['tables']['ordinary'][0]['value'] = 'restored'
        self.ns['import_backup_json_rows_into_current_database'](current, None, [])
        self.assertEqual(self.export(), current)
        self.assertEqual(self.delivery.detail(self.order['key'])['state'], 'geliefert')
        self.assertTrue(self.intake.original(group['id'], file['id'])[0].startswith(b'\x89PNG'))

    def test_externalized_json_checks_actual_original_bytes(self):
        group, file = self.seed()
        original = self.intake.original(group['id'], file['id'])[0]
        reader = next(node for node in self.tree.body if isinstance(node, ast.FunctionDef)
                      and node.name == 'read_backup_binary_blob')
        self.ns.update(BACKUP_BINARY_FIELDS={'einkauf_eingang_dateien': {
            'original_base64': {'max_bytes': 8 * 1024 * 1024}}}, hmac=hmac,
            sha256_bytes=lambda raw: hashlib.sha256(raw).hexdigest())
        exec(compile(ast.Module(body=[reader], type_ignores=[]), '<actual-original-reader>', 'exec'), self.ns)
        for raw in (original, b'different-original'):
            with self.subTest(original=raw == original):
                export = self.export()
                export['tables']['einkauf_eingang_dateien'][0]['original_base64'] = ''
                reference = dict(table='einkauf_eingang_dateien', column='original_base64',
                                 row_id=file['id'], zip_path='database_blobs/original.bin',
                                 size=len(raw), sha256=hashlib.sha256(raw).hexdigest())
                self.ns['backup_binary_reference_map'] = lambda export: {
                    ('einkauf_eingang_dateien', file['id'], 'original_base64'): reference}
                buffer = io.BytesIO()
                with zipfile.ZipFile(buffer, 'w') as archive:
                    archive.writestr(reference['zip_path'], raw)
                buffer.seek(0)
                before = self.export()
                with zipfile.ZipFile(buffer) as archive:
                    if raw == original:
                        self.ns['import_backup_json_rows_into_current_database'](export, archive, archive.namelist())
                    else:
                        with self.assertRaisesRegex(ValueError, 'bestätigte Lieferungen'):
                            self.ns['import_backup_json_rows_into_current_database'](export, archive, archive.namelist())
                self.assertEqual(self.export(), before)

    def test_legacy_json_is_compatible_before_first_delivery(self):
        old = self.export()
        del old['tables'][DELIVERIES]
        old['tables']['ordinary'][0]['value'] = 'legacy'
        self.ns['import_backup_json_rows_into_current_database'](old, None, [])
        self.assertEqual(self.export()['tables']['ordinary'], old['tables']['ordinary'])
        self.assertEqual(self.export()['tables'][DELIVERIES], [])

    def test_old_sqlite_rows_cannot_erase_delivery(self):
        path = self.sqlite_source(self.export(), omit_delivery=True)
        self.seed()
        before = self.export()
        with self.assertRaisesRegex(ValueError, 'bestätigte Lieferungen'):
            self.ns['import_sqlite_rows_into_current_database'](path)
        self.assertEqual(self.export(), before)

    def test_current_sqlite_rows_restore_delivery_and_original(self):
        group, file = self.seed()
        current = self.export()
        current['tables']['ordinary'][0]['value'] = 'SQLite restored'
        self.ns['import_sqlite_rows_into_current_database'](self.sqlite_source(current))
        self.assertEqual(self.export(), current)
        self.assertEqual(self.delivery.detail(self.order['key'])['state'], 'geliefert')
        self.assertTrue(self.intake.original(group['id'], file['id'])[0].startswith(b'\x89PNG'))

    def test_sqlite_replacement_checks_actual_file_before_any_mutation(self):
        path = self.sqlite_source(self.export(), omit_delivery=True)
        self.seed()
        current = self.export()
        self.ns['extract_import_package_files'] = lambda *args: (path, self.root / 'uploads', current)
        self.assertEqual(self.post_backup().status_code, 302)
        with self.client.session_transaction() as session:
            self.assertTrue(any('bestätigte Lieferungen' in message for _, message in session['_flashes']))
        for name in ('create_backup_package', 'copy_sqlite_database_snapshot', 'replace_uploads_from_import', 'init_db'):
            self.ns[name].assert_not_called()
        self.assertEqual(self.export(), current)

    def test_legacy_sqlite_replacement_recreates_delivery_schema(self):
        path = self.sqlite_source(self.export(), omit_delivery=True)
        self.ns['extract_import_package_files'] = lambda *args: (path, self.root / 'uploads', None)
        def replace(source, destination, **kwargs):
            if Path(destination) == self.db_path:
                with connection(self.db_path) as db:
                    db.execute(f'DROP TABLE {DELIVERIES}')
        self.ns['copy_sqlite_database_snapshot'].side_effect = replace
        self.assertEqual(self.post_backup().status_code, 302)
        self.ns['order_delivery_init_schema'].assert_called_once_with()
        self.assertEqual(self.export()['tables'][DELIVERIES], [])

    def test_restore_waits_for_delivery_write_lock(self):
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
        self.assertTrue(errors and 'bestätigte Lieferungen' in errors[0])
        self.assertEqual(len(self.export()['tables'][DELIVERIES]), 1)

    def test_backup_feature_requires_delivery_table_and_accepts_legacy(self):
        names = {'BACKUP_TABLES', 'BACKUP_SCHEMA_FEATURES', 'BACKUP_EXTERNALIZED_BINARY_FORMAT_VERSION'}
        values = {}
        for node in self.tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id in names:
                values[node.targets[0].id] = ast.literal_eval(node.value)
        self.assertIn(DELIVERIES, values['BACKUP_TABLES'])
        self.assertIn('werkstatt_liefereingang_v1', values['BACKUP_SCHEMA_FEATURES'])
        node = next(node for node in self.tree.body if isinstance(node, ast.FunctionDef) and node.name == 'validate_backup_binary_reference_completeness')
        values.update(BACKUP_BINARY_FIELDS={}, clean_text=str)
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<actual-delivery-backup-features>', 'exec'), values)
        old = {'format_version': 4, 'tables': {name: [] for name in values['BACKUP_TABLES'] if name != DELIVERIES}}
        validate = values['validate_backup_binary_reference_completeness']
        validate(old, {})
        old['schema_features'] = ['werkstatt_liefereingang_v1']
        with self.assertRaisesRegex(ValueError, 'Tabellen fehlen'):
            validate(old, {})
        old['tables'][DELIVERIES] = []
        validate(old, {})

    def test_startup_registers_delivery_and_restore_hook(self):
        portal = SimpleNamespace(get_db=self.portal.get_db, order_price_comparison=self.comparison)
        node = next(node for node in self.tree.body if isinstance(node, ast.Expr)
                    and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
                    and node.value.func.id == 'get_delivery')
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<actual-delivery-startup>', 'exec'),
             {'get_delivery': get_delivery, 'sys': SimpleNamespace(modules={'startup': portal}), '__name__': 'startup'})
        self.assertIsInstance(portal.order_delivery, OrderDelivery)
        portal.order_delivery_init_schema()
        self.assertIs(get_delivery(portal), portal.order_delivery)

    def test_postgres_adapter_translates_delivery_schema(self):
        names = {'PostgresCursor', 'PostgresConnection', 'split_sql_script', 'get_insert_table_name', 'convert_sqlite_sql_to_postgres'}
        nodes = [node for node in self.tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
        namespace = {'re': re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), '<actual-delivery-pg-adapter>', 'exec'), namespace)
        statements = []
        cursor = Mock(rowcount=0, description=None)
        cursor.execute.side_effect = lambda sql, params: statements.append(sql)
        manager = Mock()
        manager.__enter__ = Mock(return_value=cursor)
        manager.__exit__ = Mock(return_value=False)
        pg = Mock()
        pg.cursor.return_value = manager
        portal = SimpleNamespace(get_db=lambda: namespace['PostgresConnection'](pg))
        portal.order_price_comparison = comparison_backup.OrderPriceComparison(portal)
        statements.clear()
        pg.commit.reset_mock()
        OrderDelivery(portal)
        self.assertEqual(len(statements), 1)
        self.assertIn('SERIAL PRIMARY KEY', statements[0])
        self.assertNotIn('AUTOINCREMENT', statements[0])
        pg.commit.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
