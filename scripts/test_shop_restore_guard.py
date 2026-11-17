"""Legacy importer must reject mixed financial recovery before target writes."""
import ast
from contextlib import closing
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock,patch
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from shop_storage import SHOP_RESTORE_MESSAGE,reject_portal_only_restore


def app_function(name,namespace):
    tree=ast.parse((ROOT/'app.py').read_text(encoding='utf-8-sig'))
    node=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name==name)
    node.decorator_list=[]
    module=ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[]))
    exec(compile(module,str(ROOT/'app.py'),'exec'),namespace)
    return namespace[name]


class RestoreGuardChecks(unittest.TestCase):
    def test_shop_metadata_or_zip_subtree_is_rejected_without_any_database_access(self):
        with patch('sqlite3.connect',side_effect=AssertionError('No target DB')):
            for changes in ({'export':{'shop_v2':{}}},{'manifest':{'shop_v2':None}},
                {'export':{'tables':{'shop_orders_v2':[]}}},
                {'names':['backup.json','shop-v2/initialized']},
                {'names':['backup.json','SHOP-V2\\shop.sqlite3']},
                {'names':['nested/shop-v2/mail-outbox/private.eml']}):
                with self.subTest(changes=changes),self.assertRaisesRegex(ValueError,'Portal und Shop müssen gemeinsam'):
                    reject_portal_only_restore(**changes)

    def test_legacy_non_shop_package_is_not_blocked_or_modified(self):
        export={'format_version':1,'tables':{'auftraege':[]}}
        before=json.dumps(export,sort_keys=True)
        with patch('sqlite3.connect',side_effect=AssertionError('No DB')):
            self.assertIsNone(reject_portal_only_restore(export=export,manifest={},names={'backup.json','uploads/SYNTHETIC.pdf'}))
        self.assertEqual(json.dumps(export,sort_keys=True),before)

    def test_standalone_shop_sqlite_is_inspected_read_only_before_target_open(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'source.sqlite3'
            with closing(sqlite3.connect(path)) as db:db.execute('CREATE TABLE shop_orders_v2 (id TEXT)')
            before=sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError,'Shop-Bestellungen'):
                reject_portal_only_restore(imported_db=path)
            self.assertEqual(sha256(path.read_bytes()).hexdigest(),before)
            self.assertEqual(list(Path(temporary).iterdir()),[path])

    def test_actual_zip_extractor_rejects_before_extraction_or_hydration(self):
        raw=BytesIO()
        with zipfile.ZipFile(raw,'w') as archive:
            archive.writestr('backup.json',json.dumps({'tables':{},'shop_v2':{'path':'shop-v2/shop.sqlite3'}}))
            archive.writestr('shop-v2/shop.sqlite3',b'SYNTHETIC not a real database')
        denied=Mock(side_effect=AssertionError('No extraction, validation side effect or DB'))
        with tempfile.TemporaryDirectory() as temporary,zipfile.ZipFile(BytesIO(raw.getvalue())) as archive:
            namespace={'read_backup_json_from_archive':lambda a,n:json.loads(a.read('backup.json')),
                'read_backup_manifest_from_archive':lambda a,n:{},
                'validate_import_original_coverage':denied,'copy_import_package_member':denied,
                'hydrate_imported_sqlite_backup_blobs':denied,'get_db':denied}
            function=app_function('extract_import_package_files',namespace)
            with self.assertRaisesRegex(ValueError,'Portal und Shop müssen gemeinsam'):
                function(archive,set(archive.namelist()),Path(temporary))
            self.assertEqual(list(Path(temporary).iterdir()),[])
            denied.assert_not_called()

    def test_actual_json_rows_importer_rejects_before_validation_or_target_connection(self):
        denied=Mock(side_effect=AssertionError('No target mutation'))
        namespace={'get_db':denied,'backup_binary_reference_map':denied,
                   'validate_backup_binary_reference_completeness':denied}
        function=app_function('import_backup_json_rows_into_current_database',namespace)
        with self.assertRaisesRegex(ValueError,'Shop-Bestellungen'):
            function({'shop_v2':{},'tables':{}},None,set())
        denied.assert_not_called()

    def test_actual_sqlite_rows_importer_rejects_before_legacy_mutating_connection(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'source.sqlite3'
            with closing(sqlite3.connect(path)) as db:db.execute('CREATE TABLE shop_payment_attempts_v2 (id TEXT)')
            denied=Mock(side_effect=AssertionError('Legacy mutating target/source open forbidden'))
            namespace={'sqlite3':Mock(connect=denied),'get_db':denied}
            function=app_function('import_sqlite_rows_into_current_database',namespace)
            with self.assertRaisesRegex(ValueError,'Shop-Bestellungen'):
                function(path)
            denied.assert_not_called()


if __name__=='__main__':unittest.main()
