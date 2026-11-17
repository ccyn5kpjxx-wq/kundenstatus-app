"""Only new marked GET runs may be pruned; synthetic files and no providers."""
from pathlib import Path
import json
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from shop_alzura_storage import DisplayStorageError, LiveReadRetention, MARKER
from shop_partner_native.alzura_readonly_catalog import _atomic_json


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.runs=self.root/'new-live-runs'
        self.manager=LiveReadRetention([self.runs],max_bytes=2048,max_runs=2)

    def tearDown(self):
        self.temp.cleanup()

    def run_dir(self,n,size=0):
        path=self.manager.create(self.runs/f'{n:032x}')
        if size:(path/'01-tyres-search.body').write_bytes(b'x'*size)
        return path

    def test_constructor_no_io_and_prunes_only_owned_runs_with_combined_byte_reserve(self):
        self.assertFalse(self.runs.exists())
        audit=self.root/'existing-audit'
        audit.mkdir()
        (audit/'preserve.body').write_bytes(b'audit')
        first=self.run_dir(1,1100)
        second=self.run_dir(2,500)
        self.manager.prepare(second,600)
        self.assertFalse(first.exists())
        self.assertTrue(second.exists())
        self.assertEqual((audit/'preserve.body').read_bytes(),b'audit')

    def test_count_cap_and_current_run_size_fail_before_fetch(self):
        first=self.run_dir(1)
        second=self.run_dir(2)
        third=self.run_dir(3)
        self.manager.prepare(third,0)
        self.assertFalse(first.exists())
        self.assertTrue(second.exists())
        (third/'01-tyres-search.body').write_bytes(b'x'*2100)
        with self.assertRaisesRegex(DisplayStorageError,'live_read_storage_limit'):
            self.manager.prepare(third,1)
        self.assertTrue(third.exists())

    def test_unknown_content_and_unmarked_directory_are_never_deleted(self):
        current=self.run_dir(1)
        unknown=self.runs/('f'*32)
        unknown.mkdir()
        (unknown/'customer-document.txt').write_text('preserve')
        with self.assertRaises(DisplayStorageError):self.manager.prepare(current,2000)
        self.assertEqual((unknown/'customer-document.txt').read_text(),'preserve')
        with self.assertRaises(FileExistsError):self.manager.create(unknown)
        with self.assertRaises(DisplayStorageError):self.manager.create(self.root/('a'*32))
        self.assertTrue((current/MARKER).exists())

    def test_shared_roots_and_wrapper_bounded_before_each_read(self):
        other=self.root/'parts-live-runs'
        manager=LiveReadRetention([self.runs,other],max_bytes=2048,max_runs=2)
        old=manager.create(self.runs/('a'*32))
        (old/'01-tyres-search.body').write_bytes(b'x'*1700)
        calls=[]
        class Fake:
            def __init__(self,*,output_dir):self.path=output_dir
            def read(self,area,operation,params):
                calls.append((area,operation))
                (self.path/'01-products-items.body').write_bytes(b'{}')
                return 'synthetic'
        factory=manager.reader_factory(Fake,reserve_bytes=500)
        reader=factory(output_dir=other/('b'*32))
        self.assertEqual(reader.read('products','items',{}),'synthetic')
        self.assertFalse(old.exists())
        self.assertEqual(calls,[('products','items')])

    def test_atomic_metadata_target_fits_windows_when_old_target_plus_uuid_does_not(self):
        # Reproduces the real deep private release path: body/metadata names
        # fit MAX_PATH, but the former metadata+UUID temporary did not.
        directory=self.root/('r'*(200-len(str(self.root))-1))
        directory.mkdir()
        target=directory/'01-alloys-details.meta.json'
        self.assertGreater(len(str(target))+37,260)
        _atomic_json(target,{'synthetic':True,'ok':True})
        self.assertEqual(json.loads(target.read_text()),{'synthetic':True,'ok':True})
        self.assertEqual(list(directory.iterdir()),[target])


if __name__=='__main__':unittest.main()
