"""Pinned local originals, temporary files and zero network/provider activity."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(Path(__file__).resolve().parent))
from shop_document_overlay import DocumentOverlayError, local_product_document_resources
from shop_documents import ReleasedProductDocuments, document_asset
from test_shop_documents import LABEL, image, pdf


def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()


class LocalDocumentChecks(unittest.TestCase):
    def setUp(self):
        self.network=patch.object(socket,'socket',side_effect=AssertionError('Network forbidden'));self.network.start();self.addCleanup(self.network.stop)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        (self.root/'documents').mkdir();self.base='a'*64;self.overlay='b'*64
        self.records,self.payloads,self.resources={},{},{}
        data=pdf()
        for i in range(29):
            url='https://eprel.ec.europa.eu/fiches/tyres/Fiche_'+str(123456+i)+'_EN.pdf';asset=document_asset(url)
            self.resources[asset]={'url':url};self.payloads[asset]=data
            self.records[asset]={'name':asset,'sha256':sha256(data).hexdigest(),'size':len(data),'mime':'application/pdf'}
            (self.root/'documents'/asset).write_bytes(data)
        self.extra=document_asset('https://eprel.ec.europa.eu/fiches/tyres/Fiche_987654_EN.pdf')
        self.resources[self.extra]={'url':'https://eprel.ec.europa.eu/fiches/tyres/Fiche_987654_EN.pdf'}
        self.bundle={'documents':{'resources':self.resources,'approved_assets':list(self.resources)},'prices':'UNCHANGED','fitment':'UNCHANGED'}
        self.manifest={'base_bundle_sha256':self.base,'overlay_sha256':self.overlay,'counts':{'assets':29,'bytes':sum(r['size'] for r in self.records.values())},'records':self.records}
        self.pin()

    def pin(self):
        raw=encode(self.manifest);(self.root/'manifest.json').write_bytes(raw)
        self.record={'path':'manifest.json','sha256':sha256(raw).hexdigest(),'base_bundle_sha256':self.base}

    def load(self,record=None):
        return local_product_document_resources(self.root,self.record if record is None else record,self.bundle,
            base_bundle_sha256=self.base,overlay_sha256=self.overlay)

    def test_absent_record_is_legacy_and_never_looks_for_local_files(self):
        with patch('shop_document_overlay._read',side_effect=AssertionError('No IO')):
            self.assertIs(local_product_document_resources(self.root,None,self.bundle,base_bundle_sha256=self.base,overlay_sha256=None),self.resources)

    def test_exact_originals_are_local_with_no_network_and_bundle_is_immutable(self):
        original=deepcopy(self.bundle);result=self.load();self.assertEqual(self.bundle,original)
        self.assertEqual(result[self.extra],self.resources[self.extra]);self.assertNotIn('file',result[self.extra])
        http=Mock();http.get.side_effect=AssertionError('No remote fallback')
        service=ReleasedProductDocuments(result,approved_assets=set(result),enabled=True,allow_network=True,
            local_root=self.root/'documents',http=http,max_network_reads_per_period=1)
        for asset,payload in self.payloads.items():
            self.assertEqual(result[asset],{**self.resources[asset],'file':asset,'sha256':self.records[asset]['sha256']})
            self.assertEqual(service.read(asset),(payload,'application/pdf'))
        self.assertEqual(len(service.attempts),0);http.get.assert_not_called()
        (self.root/'documents'/next(iter(self.records))).write_bytes(b'BROKEN')
        fresh=ReleasedProductDocuments(result,approved_assets=set(result),enabled=True,allow_network=True,
            local_root=self.root/'documents',http=http,max_network_reads_per_period=1)
        with self.assertRaises(Exception):fresh.read(next(iter(self.records)))
        http.get.assert_not_called()

    def test_bad_descriptor_paths_types_and_hashes_rejected(self):
        for change in [{'path':'../x'},{'path':'/x'},{'path':'a'*101},{'extra':True},{'sha256':'f'*64},{'base_bundle_sha256':'e'*64}]:
            with self.subTest(change=change),self.assertRaises(DocumentOverlayError):self.load({**self.record,**change})
        with self.assertRaises(DocumentOverlayError):local_product_document_resources(self.root,True,self.bundle,base_bundle_sha256=self.base,overlay_sha256=self.overlay)

    def test_original_jpeg_label_is_verified_and_served_without_reencoding(self):
        old=next(iter(self.records));del self.resources[old];del self.records[old];del self.payloads[old]
        asset=document_asset(LABEL);data=image();self.resources[asset]={'url':LABEL};self.payloads[asset]=data
        self.records[asset]={'name':asset,'sha256':sha256(data).hexdigest(),'size':len(data),'mime':'image/jpeg'}
        (self.root/'documents'/asset).write_bytes(data);self.bundle['documents']['approved_assets']=list(self.resources)
        self.manifest['counts']['bytes']=sum(r['size'] for r in self.records.values());self.pin()
        resources=self.load();http=Mock();http.get.side_effect=AssertionError('No HTTP')
        service=ReleasedProductDocuments(resources,approved_assets=set(resources),enabled=True,allow_network=False,
                                         local_root=self.root/'documents',http=http)
        self.assertEqual(service.read(asset),(data,'image/jpeg'));http.get.assert_not_called()

    def test_wrong_base_overlay_count_unknown_asset_and_resource_conflict_fail_closed(self):
        original=deepcopy(self.manifest)
        for change in [{'base_bundle_sha256':'f'*64},{'overlay_sha256':'f'*64},{'counts':{'assets':28,'bytes':1}}]:
            self.manifest={**original,**change};self.pin()
            with self.assertRaises(DocumentOverlayError):self.load()
        self.manifest=original;self.pin();asset=next(iter(self.records))
        self.bundle['documents']['approved_assets'].remove(asset)
        with self.assertRaises(DocumentOverlayError):self.load()
        self.bundle['documents']['approved_assets'].append(asset)
        self.resources[asset]['file']='another.pdf'
        with self.assertRaises(DocumentOverlayError):self.load()

    def test_missing_tampered_oversized_and_wrong_mime_files_fail_before_binding(self):
        asset=next(iter(self.records));path=self.root/'documents'/asset
        for payload in (b'',b'NOT_A_PDF',b'x'*(4*1024*1024+1)):
            path.write_bytes(payload)
            with self.assertRaises(DocumentOverlayError):self.load()
        path.write_bytes(self.payloads[asset]);self.records[asset]['mime']='image/jpeg';self.pin()
        with self.assertRaisesRegex(DocumentOverlayError,'mime_mismatch'):self.load()
        path.unlink()
        with self.assertRaises(DocumentOverlayError):self.load()

    def test_duplicate_json_keys_and_structurally_invalid_original_rejected(self):
        raw=b'{"records":{},"records":{}}';(self.root/'manifest.json').write_bytes(raw);self.record['sha256']=sha256(raw).hexdigest()
        with self.assertRaisesRegex(DocumentOverlayError,'duplicate_key'):self.load()
        asset=next(iter(self.records));bad=b'%PDF-1.7\nNOT_A_REAL_PDF\n%%EOF';(self.root/'documents'/asset).write_bytes(bad)
        self.records[asset].update(sha256=sha256(bad).hexdigest(),size=len(bad));self.pin()
        with self.assertRaisesRegex(DocumentOverlayError,'content_invalid'):self.load()

    def test_manifest_or_document_symlink_marker_refused(self):
        actual=Path.is_symlink
        for name in ['manifest.json',next(iter(self.records))]:
            def linked(path):return path.name==name or actual(path)
            with patch.object(Path,'is_symlink',linked),self.assertRaises(DocumentOverlayError):self.load()

if __name__=='__main__':unittest.main()
