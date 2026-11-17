"""Postgres portal must retain its disabled shop's separate persistent ledger."""
import ast
from hashlib import sha256
from io import BytesIO
import pathlib
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
from flask import Flask
from shop_admin import register_shop_admin
from shop_production import existing_shop_backup_storage
from shop_storage import ShopStorage, StorageError
from test_shop_cart_quotes import Fixture, NOW, OIL, line

TREE = ast.parse((ROOT / 'app.py').read_text(encoding='utf-8-sig'))


def assignment(name):
    return next(node for node in ast.walk(TREE) if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == name for target in node.targets))


def config(base, environ):
    # Execute the actual app's Postgres/upload configuration, without importing
    # the portal or opening its database/background integrations.
    nodes = [assignment('DATABASE_URL'), assignment('USE_POSTGRES')]
    nodes.append(next(node for node in TREE.body if isinstance(node, ast.If)
                      and isinstance(node.test, ast.Name) and node.test.id == 'USE_POSTGRES'))
    nodes.append(assignment('SHOP_PRIVATE_DATA_DIR'))
    namespace = {'os': SimpleNamespace(environ=environ), 'pathlib': pathlib,
                 'BASE': base, 'DATA_DIR': base / 'data', 'DB': base / 'data/auftraege.db',
                 'UPLOAD_DIR': base / 'data/uploads'}
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])),
                 str(ROOT / 'app.py'), 'exec'), namespace)
    return namespace


def backup_fallback(namespace):
    node = next(node for node in ast.walk(TREE) if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name) and node.func.id == 'existing_shop_backup_storage')
    return eval(compile(ast.Expression(node), str(ROOT / 'app.py'), 'eval'),
                {**namespace, 'existing_shop_backup_storage': existing_shop_backup_storage})


class PersistentRootChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.base = self.root / 'project'
        self.data = self.root / 'persistent'
        self.data.mkdir()
        self.env = {'RENDER': 'true', 'DATABASE_URL': 'postgresql://synthetic.invalid/portal',
                    'DATA_DIR': str(self.data), 'SHOP_RELEASE_ENABLED': '0'}
        self.namespace = config(self.base, self.env)
        self.network = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def order(self):
        storage = ShopStorage(self.data, clock=lambda: NOW).provision(create=True)
        store = storage.order_store()
        fixture = Fixture()
        customer = 'synthetic-pg-root-buyer'
        context = {'postcode': '74821'}
        quote = fixture.service_instance().quote({'lines': [line(OIL)]}, customer_id=customer,
                                                 vehicle_ref='CAR-1', delivery_context=context)
        store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash,
                         customer_id=customer, mode='test', delivery_context=context)
        order = store.create_order(quote.quote_hash, customer_id=customer, mode='test',
                                   idempotency_key='synthetic-pg-root-order')
        return storage, order

    def test_postgres_retains_portal_local_default_but_shop_uses_explicit_persistent_root(self):
        self.assertTrue(self.namespace['USE_POSTGRES'])
        self.assertEqual(self.namespace['DATA_DIR'], self.base / 'data')
        self.assertEqual(self.namespace['SHOP_PRIVATE_DATA_DIR'], self.data)
        default = config(self.base, {'RENDER': 'true', 'DATABASE_URL': self.env['DATABASE_URL']})
        self.assertEqual(default['SHOP_PRIVATE_DATA_DIR'], Path('/var/data').resolve())
        local = config(self.base, {'DATABASE_URL': self.env['DATABASE_URL']})
        self.assertEqual(local['SHOP_PRIVATE_DATA_DIR'], (self.base / 'data').resolve())

    def test_disabled_postgres_release_backup_contains_existing_order_and_never_uses_portal_root(self):
        storage, order = self.order()
        resolved = backup_fallback(self.namespace)
        self.assertEqual(resolved.path, storage.path)
        raw = BytesIO()
        with zipfile.ZipFile(raw, 'w') as archive:
            info = resolved.write_backup(archive)
        with zipfile.ZipFile(BytesIO(raw.getvalue())) as archive:
            self.assertEqual(archive.read(info['marker']['path']), b'shop_storage_v2\n')
            self.assertIn(order['id'].encode(), archive.read(info['path']))
        self.assertFalse((self.base / 'data').exists())
        self.assertFalse((self.data / 'auftraege.db').exists())

    def test_disabled_postgres_admin_uses_same_existing_ledger_read_only(self):
        storage, order = self.order()
        before = sha256(storage.path.read_bytes()).hexdigest()
        app = Flask('synthetic-pg-root', template_folder=str(ROOT / 'templates'))
        app.config.update(TESTING=True, SECRET_KEY='synthetic-pg-root-key')
        app.extensions['shop_production'] = {'ready': False, 'reason': 'release_configuration_incomplete'}
        call = next(node for node in TREE.body if isinstance(node, ast.Expr)
                    and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
                    and node.value.func.id == 'register_shop_admin')
        exec(compile(ast.Module(body=[call], type_ignores=[]), str(ROOT / 'app.py'), 'exec'),
             {**self.namespace, 'app': app, 'register_shop_admin': register_shop_admin})
        client = app.test_client()
        with client.session_transaction() as session:
            session['admin'] = True
        with (patch.object(ShopStorage, 'provision', side_effect=AssertionError('No provisioning')),
              patch.object(ShopStorage, 'connect', side_effect=AssertionError('No RW connection'))):
            response = client.get('/admin/shop')
            self.assertEqual(response.status_code, 200)
            self.assertIn(order['id'], response.get_data(as_text=True))
        self.assertEqual(sha256(storage.path.read_bytes()).hexdigest(), before)
        self.assertFalse((self.base / 'data').exists())

    def test_missing_or_lost_persistent_ledger_never_becomes_empty_portal_shop(self):
        self.assertIsNone(backup_fallback(self.namespace))
        self.assertFalse((self.data / 'shop-v2').exists())
        storage, _ = self.order()
        storage.path.unlink()
        with self.assertRaises(StorageError):
            backup_fallback(self.namespace)
        self.assertFalse((self.base / 'data').exists())
        self.assertFalse(storage.path.exists())


if __name__ == '__main__':
    unittest.main()
