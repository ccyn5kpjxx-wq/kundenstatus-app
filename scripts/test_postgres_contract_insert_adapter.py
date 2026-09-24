"""Offline regression for PostgreSQL inserts into the contract's text-key table."""

from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_mos_public_test import build_test_app


class FakeCursor:
    def __init__(self, statements):
        self.statements = statements
        self.description = None
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params):
        self.statements.append((query, params))
        if 'miet_checkout_contracts' in query.lower() and 'returning id' in query.lower():
            raise AssertionError('miet_checkout_contracts has hold_id, not id')
        self.description = ([SimpleNamespace(name='id')] if 'RETURNING id' in query else None)

    def fetchall(self):
        return [(42,)] if self.description else []


class FakeConnection:
    def __init__(self):
        self.statements = []

    def cursor(self):
        return FakeCursor(self.statements)


class PostgresContractInsertTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix='mos-pg-contract-adapter-')
        cls.portal = build_test_app(cls.directory.name)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_contract_insert_does_not_request_nonexistent_id(self):
        fake = FakeConnection()
        db = self.portal.PostgresConnection(fake)
        cursor = db.execute('''INSERT INTO miet_checkout_contracts
            (hold_id,contract_json,contract_sha256,signer_name,signed_at,
             signature_png_base64,pdf_base64,pdf_sha256)
            VALUES (?,?,?,?,?,?,?,?)''', tuple('abcdefgh'))
        sql, params = fake.statements[0]
        self.assertNotIn('RETURNING id', sql)
        self.assertIn('VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', sql)
        self.assertEqual(params, tuple('abcdefgh'))
        self.assertIsNone(cursor.lastrowid)

    def test_integer_id_insert_keeps_lastrowid_behavior(self):
        fake = FakeConnection()
        db = self.portal.PostgresConnection(fake)
        cursor = db.execute('INSERT INTO mietvorgaenge (kennzeichen) VALUES (?)', ('TEST',))
        self.assertIn('RETURNING id', fake.statements[0][0])
        self.assertEqual(cursor.lastrowid, 42)
        self.assertEqual(db.lastrowid, 42)


if __name__ == '__main__':
    unittest.main()
