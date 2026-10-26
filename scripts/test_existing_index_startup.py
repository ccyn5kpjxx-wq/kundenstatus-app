"""Existing PG indexes avoid DDL; missing indexes retain normal creation."""
import sqlite3
import unittest
from unittest.mock import Mock, patch
import test_navigation_readmodels as support

p = support.p


class ExistingIndexStartupTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.executescript('''
            CREATE TABLE pg_indexes (indexname TEXT, schemaname TEXT);
            CREATE TABLE pg_class (oid INTEGER, relnamespace INTEGER);
            CREATE TABLE pg_namespace (oid INTEGER, nspname TEXT);
            INSERT INTO pg_namespace VALUES (11,'public'), (12,'other');
            INSERT INTO pg_class VALUES (101,11), (201,12);
            CREATE TABLE leads (id INTEGER, website TEXT, status TEXT);
        ''')
        self.db.create_function('to_regclass', 1, lambda name:{'leads':101,'other.leads':201}.get(name))
        self.statements = []
        self.db.set_trace_callback(self.statements.append)

    def tearDown(self):
        self.db.close()

    def test_existing_index_in_resolved_table_schema_never_runs_create(self):
        self.db.execute("INSERT INTO pg_indexes VALUES ('idx_leads','public')")
        self.statements.clear()
        with patch.object(p,'USE_POSTGRES',True):
            p.ensure_index(self.db,'idx_leads','leads',('website','status','id'))
        self.assertEqual(len(self.statements),1)
        self.assertTrue(self.statements[0].lstrip().startswith('SELECT 1 FROM pg_indexes'))
        self.assertFalse(any('CREATE INDEX' in s for s in self.statements))

    def test_same_name_in_another_schema_does_not_skip_required_creation(self):
        self.db.execute("INSERT INTO pg_indexes VALUES ('idx_leads','other')")
        self.statements.clear()
        with patch.object(p,'USE_POSTGRES',True):
            p.ensure_index(self.db,'idx_leads','leads',('website','status','id'))
        self.assertEqual(len(self.statements),2)
        self.assertIn('CREATE INDEX IF NOT EXISTS idx_leads ON leads (website, status, id)',self.statements[1])
        self.assertTrue(self.db.execute("PRAGMA index_list(leads)").fetchall())

    def test_resolved_nondefault_schema_is_honored(self):
        self.db.execute("INSERT INTO pg_indexes VALUES ('idx_leads','other')")
        self.statements.clear()
        with patch.object(p,'USE_POSTGRES',True):
            p.ensure_index(self.db,'idx_leads','other.leads',('id',))
        self.assertEqual(len(self.statements),1)
        self.assertFalse(any('CREATE INDEX' in s for s in self.statements))

    def test_sqlite_keeps_original_path_and_metadata_failures_propagate(self):
        with patch.object(p,'USE_POSTGRES',False):
            p.ensure_index(self.db,'idx_leads','leads',('id',))
        self.assertEqual(len(self.statements),1)
        self.assertTrue(self.statements[0].startswith('CREATE INDEX IF NOT EXISTS'))
        failure=RuntimeError('synthetic catalog failure')
        db=Mock()
        db.execute.side_effect=failure
        with patch.object(p,'USE_POSTGRES',True), self.assertRaises(RuntimeError) as raised:
            p.ensure_index(db,'idx_leads','leads',('id',))
        self.assertIs(raised.exception,failure)
        db.execute.assert_called_once()


if __name__=='__main__':
    unittest.main(verbosity=2)
