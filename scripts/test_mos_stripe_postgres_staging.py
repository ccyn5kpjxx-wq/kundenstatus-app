"""No PostgreSQL/Stripe network: local PG launcher preflight and HTTP gate tests."""

import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from flask import Flask

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT))

import run_mos_stripe_postgres_staging as launcher
from mos_public_booking import isolated_postgres_stripe_test, register


NAME = 'mos_stripe_acceptance_' + 'a' * 32
CFG = {'host': '127.0.0.1', 'port': 55439, 'user': 'mos_test_admin',
       'password': 'dummy-local-secret', 'dbname': 'postgres'}
URL = launcher.database_url(CFG, NAME)


class PostgresStripePreflight(unittest.TestCase):
    def test_private_file_and_exact_cluster_required(self):
        with tempfile.TemporaryDirectory() as private:
            with patch.object(launcher, 'PRIVATE_ROOT', Path(private)):
                path = Path(private) / 'connection.json'
                path.write_text(json.dumps(CFG), encoding='utf-8')
                self.assertEqual(launcher.load_cluster_config(path), CFG)
                for key, value in [('host', 'localhost'), ('port', 5432),
                                   ('port', True), ('user', 'postgres'),
                                   ('dbname', NAME), ('password', '')]:
                    with self.subTest(key=key, value=value):
                        path.write_text(json.dumps({**CFG, key: value}), encoding='utf-8')
                        with self.assertRaises(ValueError):
                            launcher.load_cluster_config(path)
                outside = Path(private).parent / 'not-private.json'
                with self.assertRaises(ValueError):
                    launcher.load_cluster_config(outside)

    def test_pg_test_gate_rejects_other_databases_modes_and_live_keys(self):
        booking = {'mode': 'stripe_test', 'test_configuration': True,
                   'origin': launcher.TEST_ORIGIN, 'postgres_test_database': NAME}
        portal = SimpleNamespace(USE_POSTGRES=True, DATABASE_URL=URL,
                                 app=SimpleNamespace(config={'MOS_PUBLIC_STRIPE_LIVE_KEY': ''}))
        self.assertTrue(isolated_postgres_stripe_test(portal, booking))
        for url in (URL.replace(':55439/', ':5432/'),
                    URL.replace('127.0.0.1', 'db.example.invalid'),
                    URL.replace(NAME, 'operational'), URL + '?sslmode=disable'):
            with self.subTest(url=url):
                portal.DATABASE_URL = url
                self.assertFalse(isolated_postgres_stripe_test(portal, booking))
        portal.DATABASE_URL = URL
        for update in ({'mode': 'offline'}, {'test_configuration': False},
                       {'origin': 'https://example.invalid'},
                       {'postgres_test_database': 'operational'}):
            with self.subTest(update=update):
                self.assertFalse(isolated_postgres_stripe_test(portal, {**booking, **update}))
        portal.app.config['MOS_PUBLIC_STRIPE_LIVE_KEY'] = 'sk_live_dummy'
        self.assertFalse(isolated_postgres_stripe_test(portal, booking))

    def test_environment_cannot_inherit_operational_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {'DATABASE_URL': 'postgresql://prod.invalid/customer',
                                         'MOS_STRIPE_LIVE_KEY': 'sk_live_dummy',
                                         'PGHOST': 'prod.invalid', 'LEXWARE_API_KEY': 'dummy'}, clear=True):
                clean = launcher.clean_environment(directory, URL)
                with launcher.isolated_process_environment(clean):
                    self.assertEqual(os.environ['DATABASE_URL'], URL)
                    self.assertEqual(os.environ['MOS_STRIPE_LIVE_KEY'], '')
                    self.assertNotIn('PGHOST', os.environ)
                    self.assertNotIn('LEXWARE_API_KEY', os.environ)
                    self.assertFalse((ROOT / '.env.local').exists())
                self.assertEqual(os.environ['PGHOST'], 'prod.invalid')

    def test_fresh_database_uses_create_only(self):
        class Sql(str):
            def format(self, value):
                return Sql(super().format(value))

        class Admin:
            def __init__(self): self.statements = []
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def execute(self, statement):
                self.statements.append(str(statement))
                return SimpleNamespace(fetchone=lambda: (
                    'mos_test_admin', 'postgres', '127.0.0.1/32', 55439))

        admin = Admin()
        fake = SimpleNamespace(connect=lambda **kw: admin,
                               sql=SimpleNamespace(SQL=Sql, Identifier=lambda value: value))
        with patch.dict(sys.modules, {'psycopg': fake}), patch.object(
                launcher.secrets, 'token_hex', return_value='b' * 32):
            name = launcher.fresh_test_database(CFG)
        self.assertEqual(name, 'mos_stripe_acceptance_' + 'b' * 32)
        self.assertEqual(admin.statements[1], 'CREATE DATABASE ' + name)
        self.assertEqual(len(admin.statements), 2)

    def test_http_index_only_for_exact_local_pg_test_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / 'fake-http.sqlite3'
            app = Flask('pg-test-http', template_folder=str(ROOT / 'templates'))
            app.secret_key = 'dummy-http-test-secret'
            app.config.update(
                MOS_PUBLIC_BOOKING={
                    'enabled': True, 'test_configuration': True, 'mode': 'stripe_test',
                    'origin': launcher.TEST_ORIGIN, 'postgres_test_database': NAME,
                    'fleet': {'i10': {'id': 1, 'daily_cents': 3900},
                              'kona': {'id': 2, 'daily_cents': 5900}},
                    'slots': [], 'terms_version': 'draft:TEST', 'terms_text': 'TEST',
                    'vat_included': True, 'day_rule': 'elapsed_24h_ceil',
                    'included_km_day': 150, 'extra_km_cents': 25, 'max_days': 30,
                    'deposit_cents': 50000, 'deductible_cents': 100000,
                    'deposit_method': 'card_authorization_at_booking',
                    'cancellation_policy': 'free_48h_then_10pct_rent',
                },
                MOS_PUBLIC_STRIPE_TEST_KEY='sk_test_dummy',
                MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY='pk_test_dummy',
                MOS_PUBLIC_WEBHOOK_SECRET='whsec_dummy',
                MOS_PUBLIC_STRIPE_LIVE_KEY='',
            )

            def get_db():
                db = sqlite3.connect(db_path)
                db.row_factory = sqlite3.Row
                return db

            portal = SimpleNamespace(app=app, USE_POSTGRES=True, DATABASE_URL=URL,
                                     DB=db_path, get_db=get_db, admin_required=lambda func: func)
            register(portal)
            response = app.test_client().get('/mietwagen-test/', base_url=launcher.TEST_ORIGIN)
            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Keine echte Anmietung oder Geldbewegung', response.data)
            self.assertEqual(app.extensions['mos_public_booking']['cfg']['mode'], 'stripe_test')


if __name__ == '__main__':
    unittest.main()
