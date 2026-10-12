"""Actual import guards preserve confirmation links and their original sources."""
from copy import deepcopy
import json
from pathlib import Path
import unittest

import test_bestellvergleich_backup as fixtures
from werkstatt_bestellbestaetigung import TABLE


class ConfirmationBackupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures.ComparisonBackupTests.setUpClass()

    def setUp(self):
        self.base=fixtures.ComparisonBackupTests('runTest')
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.ns=self.base.ns
        self.ns['json']=json
        self.ns['BACKUP_TABLES']=(*fixtures.TABLES,TABLE,'einkauf_belege','assistent_rechnungsartikel','assistent_rechnungsimporte')
        with fixtures.connection(self.base.db_path) as db:
            db.executescript('''CREATE TABLE einkauf_belege(id INTEGER PRIMARY KEY,stored_name TEXT,beleg_typ TEXT);
                CREATE TABLE assistent_rechnungsartikel(id INTEGER PRIMARY KEY,import_id INTEGER,payload_json TEXT);
                CREATE TABLE assistent_rechnungsimporte(id INTEGER PRIMARY KEY,source_key TEXT);''')
        self.old=self.export()
        with fixtures.connection(self.base.db_path) as db:
            db.execute('INSERT INTO einkauf_belege VALUES(99,?,?)',('synthetic-ab.pdf','auftragsbestaetigung'))
            db.execute('INSERT INTO assistent_rechnungsimporte VALUES(5,?)',('einkauf:99',))
            db.execute('INSERT INTO assistent_rechnungsartikel VALUES(1,5,?)',('{"document_type":"auftragsbestaetigung"}',))
            payload={'proposal_id':1,'source':{'beleg_id':99,'sha256':'a'*64},'amount':'12.50','document_type':'auftragsbestaetigung'}
            db.execute(f'''INSERT INTO {TABLE}(order_key,order_fingerprint,source_key,payload_hash,payload_json,created_at,created_by)
                VALUES(?,?,?,?,?,?,?)''',('material:1','frozen-order','original:a:1:2','frozen-quote',json.dumps(payload),'2026-10-12T15:00:00+00:00','admin'))

    def export(self):
        with fixtures.connection(self.base.db_path) as db:
            return {'tables':{table:[dict(row) for row in db.execute(f'SELECT * FROM {table}')]
                              for table in self.ns['BACKUP_TABLES']}}

    def test_old_json_cannot_remove_confirmation(self):
        before=self.export()
        with self.assertRaises(ValueError):
            self.ns['import_backup_json_rows_into_current_database'](self.old,None,[])
        self.assertEqual(self.export(),before)

    def test_source_proposal_import_and_quote_tamper_each_block_restore(self):
        for table in (TABLE,'einkauf_belege','assistent_rechnungsartikel','assistent_rechnungsimporte'):
            with self.subTest(table=table):
                current=self.export()
                altered=deepcopy(current)
                altered['tables'][table]=[]
                with self.assertRaises(ValueError):
                    self.ns['ensure_material_external_claims_for_import'](export=altered)
                self.assertEqual(self.export(),current)
        altered=self.export()
        altered['tables'][TABLE][0]['payload_json']='{"amount":"0.01"}'
        with self.assertRaises(ValueError):self.ns['ensure_material_external_claims_for_import'](export=altered)

    def test_same_snapshot_guard_accepts_and_optional_schema_bridge_is_declared(self):
        self.ns['ensure_material_external_claims_for_import'](export=self.export())
        source=Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf-8')
        self.assertIn('"werkstatt_bestellbestaetigung_v1"',source)
        self.assertIn('required_tables.add("assistent_bestellpreis_bestaetigungen")',source)


if __name__ == '__main__':
    unittest.main()
