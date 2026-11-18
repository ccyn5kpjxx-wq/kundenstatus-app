"""Original bytes, exact identity, 15% prices and merchant freight in a temp release."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
from shop_catalog_public import variant_fingerprint
from shop_parts_release import PartsReleaseError, load_parts_release
from shop_parts_freight import _hash
from shop_rsu_parts_evidence import build_parts_binding, parts_source_record
from test_reconcile_rsu_parts import row, EAN


def encode(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def install_parts_fixture(root,bundle,base_sha,*,fixed=True,offers=('6.00',),normalized_changes=None):
    """A genuine synthetic DOM/GET pair; no provider, portal or business DB."""
    product=next(r for r in bundle['products'] if r['selection_id'].startswith('parts:'))
    pid=product['selection_id'];identity=variant_fingerprint(product)
    maker=product['manufacturer_name'];mpn=product['details']['Herstellerartikelnummer'];ean=product['details']['EAN']
    observed=(datetime.now(timezone.utc)-timedelta(seconds=2)).isoformat()
    sources=root/'parts-sources';sources.mkdir(exist_ok=True)
    def pin(name,data):
        (sources/name).write_bytes(data)
        return {'file':name,'sha256':sha256(data).hexdigest()}
    lines=[f'- heading "{maker} {mpn}" [level=1]:','- generic: "1"',
           '  - row "Lieferoptionen Bestand EK Stück Versand EK Gesamt VK1":']
    from decimal import Decimal
    for freight in offers:
        suffix=f'{Decimal(freight)+10:.2f}'.replace('.',',') if freight is not None else '10,00'
        price=f'{Decimal(freight):.2f}'.replace('.',',')+' €' if freight is not None else '-'
        lines.append(f'  - row "vorauss. 20.11.26-26.11.26 2 10,00 € {price} {suffix} € 15,00 €":')
    lines += ['- button "In den Warenkorb":',f'  - row "Marke {maker}":',
              f'  - row "Hersteller Artikelnummer {mpn}":','  - row "RSU Artikelnummer 123":',f'  - row "EAN {ean}":']
    ui=pin('supplier.dom.txt','\n'.join(lines).encode())
    rest=pin('rest.json',encode({'schema_version':1,'supplier':'RSU','mode':'live','requests':1,'orders':0,'payments':0,
        'records':[{'supplier_article_id':'123','expected_ean':ean,'ean':ean,'quantity':1,'mode':'live',
                    'operation':'REST_INQUIRY_GET','ok':True,'unit_net':'10.00','stock':2,'retrieved_at':observed}]}))
    obs={'article_id':'123','manufacturer':maker,'mpn':mpn,'ean':ean,'read_kind':'inquiry','observed_at':observed,
         'quantity':1,'unit_net':'10.00','stock':2,'unit_label':'Stück','packing_quantity':1}
    obs.update(normalized_changes or {})
    norm=pin('normalized.json',encode({'schema_version':1,'supplier':'RSU','currency':'EUR','price_basis':'net','records':[obs]}))
    capture=pin('captures.json',encode({'schema_version':1,'captures':[
        {'file':ui['file'],'sha256':ui['sha256'],'observed_at':observed}]}))
    binding=build_parts_binding(product_id=pid,identity_hash=identity,category=product['category'],
        catalogue=parts_source_record('catalogue',source_sha256=base_sha,row_number=bundle['products'].index(product)+1,
            article_id=pid,manufacturer=maker,mpn=mpn,ean=ean),
        rsu=parts_source_record('rsu',source_sha256=norm['sha256'],row_number=1,article_id='123',manufacturer=maker,mpn=mpn,ean=ean))
    common={'product_id':pid,'identity_hash':identity,'binding_evidence_id':binding['mapping']['evidence_id'],
            'supplier_article_id':'123','ean':ean,'source_ref':ui}
    if fixed:
        freight={**common,'rule':'merchant_fixed_parts_freight_v1','shipping_class':'standard','direct_gross_cent':821,
            'reshipment_gross_cent':1642,'quantity_min':1,'quantity_max':20,'merchant_cost_difference_accepted':True,
            'policy_evidence_id':'user-fixed-parts-freight-20261118'}
    else:
        freight={**common,'rule':'rsu_ui_unique_q1_freight_v1','inquiry_ref':rest,
            'manufacturer':binding['mapping']['evidence']['rsu']['identity']['manufacturer'],
            'mpn':binding['mapping']['evidence']['rsu']['identity']['mpn'],
            'quantity':1,'offer_count':1,'unit_net':'10.00','stock_reported':2,'freight_net':'6.00','currency':'EUR',
            'mainland_only':True,'observed_at':observed,'inquiry_observed_at':observed,
            'expires_at':(datetime.fromisoformat(observed)+timedelta(minutes=15)).isoformat(),'retail_gross_cent':821}
    freight['proof_sha256']=_hash(freight)
    seed={'source':'RSU','identity_hash':identity,'unit_gross_cent':1369,'stock_reported':2,'observed_at':observed,'evidence_id':'f'*64}
    release={'schema_version':1,'base_bundle_sha256':base_sha,'rsu_bindings':{pid:binding},
             'rsu_price_seeds':{'records':{pid:seed},'evidence_id':'e'*64},'shipping_classes':{pid:freight},'source_refs':[ui,rest,norm,capture]}
    record=save_release(root,release)
    return record,release


def save_release(root,release):
    raw=encode(release);(root/'parts.json').write_bytes(raw)
    return {'path':'parts.json','sha256':sha256(raw).hexdigest(),'base_bundle_sha256':release['base_bundle_sha256']}


class PartsReleaseChecks(unittest.TestCase):
    def setUp(self):
        guard=patch.object(socket,'socket',side_effect=AssertionError('Network forbidden'));guard.start();self.addCleanup(guard.stop)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        self.product=row();self.pid=self.product['selection_id'];self.base='a'*64
        self.bundle={'products':[self.product],'publication':{'released_ids':[self.pid]},'rsu_bindings':{},
                     'rsu_price_seeds':{'records':{},'evidence_id':'b'*64},'shipping_classes':{}}

    def load(self,record):return load_parts_release(self.root,record,self.bundle,base_bundle_sha256=self.base)

    def test_absence_preserves_same_object_without_any_file_access(self):
        with patch('shop_parts_release.read',side_effect=AssertionError('No read')):
            merged,meta=self.load(None)
        self.assertIs(merged,self.bundle);self.assertEqual(meta,{})

    def test_exact_originals_seed_15percent_and_preserve_base_tariffs_and_rows(self):
        before=deepcopy(self.bundle);record,release=install_parts_fixture(self.root,self.bundle,self.base)
        merged,meta=self.load(record)
        self.assertEqual(self.bundle,before);self.assertEqual(merged['products'],before['products'])
        self.assertEqual(merged['shipping_classes'],{})
        self.assertEqual(merged['rsu_price_seeds']['records'][self.pid]['unit_gross_cent'],1369)
        self.assertEqual(meta['freight'][self.pid]['direct_gross_cent'],821)
        self.assertEqual(meta['ids'],frozenset([self.pid]))

    def test_unique_observation_only_release_remains_supported(self):
        record,_=install_parts_fixture(self.root,self.bundle,self.base,fixed=False)
        self.assertEqual(self.load(record)[1]['ids'],frozenset([self.pid]))

    def test_fixed_tariff_accepts_real_multiwarehouse_standard_but_not_bulky_or_unknown(self):
        for offers,valid in [(('6.00','6.00'),True),(('6.00','60.00'),False),(('6.00',None),False)]:
            record,_=install_parts_fixture(self.root,self.bundle,self.base,offers=offers)
            if valid:self.assertEqual(len(self.load(record)[1]['ids']),1)
            else:
                with self.assertRaises(PartsReleaseError):self.load(record)

    def test_forged_unique_offer_is_rejected_even_with_valid_self_hash_and_source_hash(self):
        record,_=install_parts_fixture(self.root,self.bundle,self.base,fixed=False,offers=('6.00','6.00'))
        with self.assertRaisesRegex(PartsReleaseError,'original_ambiguous'):self.load(record)

    def test_unique_offer_original_visit_cannot_be_renewed_by_newer_get(self):
        _,release=install_parts_fixture(self.root,self.bundle,self.base,fixed=False)
        proof=release['shipping_classes'][self.pid]
        proof['observed_at']=(datetime.fromisoformat(proof['observed_at'])+timedelta(seconds=1)).isoformat()
        proof['proof_sha256']=_hash({k:v for k,v in proof.items() if k!='proof_sha256'})
        with self.assertRaisesRegex(PartsReleaseError,'original_visit_missing'):self.load(save_release(self.root,release))

    def test_normalized_price_stock_timestamp_or_identity_cannot_authenticate_itself(self):
        for change in [{'unit_net':'11.00'},{'stock':5},{'observed_at':'2026-11-17T12:00:00Z'},
                       {'manufacturer':'OTHER'},{'mpn':'OTHER'},{'ean':'5901234123457'}]:
            record,_=install_parts_fixture(self.root,self.bundle,self.base,normalized_changes=change)
            with self.subTest(change=change),self.assertRaises(PartsReleaseError):self.load(record)

    def test_seed_markup_or_stock_cannot_use_market_price(self):
        for key,value in [('unit_gross_cent',1190),('stock_reported',7),('source','ALZURA')]:
            _,release=install_parts_fixture(self.root,self.bundle,self.base)
            release['rsu_price_seeds']['records'][self.pid][key]=value
            with self.subTest(key=key),self.assertRaises(PartsReleaseError):self.load(save_release(self.root,release))

    def test_incomplete_or_existing_binding_cannot_be_activated(self):
        _,release=install_parts_fixture(self.root,self.bundle,self.base)
        for field in ['rsu_bindings','shipping_classes']:
            bad=deepcopy(release);bad[field]={}
            with self.subTest(field=field),self.assertRaises(PartsReleaseError):self.load(save_release(self.root,bad))
        self.bundle['rsu_bindings'][self.pid]=release['rsu_bindings'][self.pid]
        with self.assertRaisesRegex(PartsReleaseError,'existing_binding'):self.load(save_release(self.root,release))

    def test_missing_corrupt_sources_changed_catalogue_or_wrong_base_fail_closed(self):
        record,_=install_parts_fixture(self.root,self.bundle,self.base)
        (self.root/'parts-sources'/'rest.json').write_bytes(b'{}')
        with self.assertRaisesRegex(PartsReleaseError,'source_hash'):self.load(record)
        record,_=install_parts_fixture(self.root,self.bundle,self.base)
        self.product['details']['Herstellerartikelnummer']='OTHER'
        with self.assertRaises(PartsReleaseError):self.load(record)
        self.product['details']['Herstellerartikelnummer']='A-1'
        record['base_bundle_sha256']='d'*64
        with self.assertRaises(PartsReleaseError):self.load(record)

    def test_source_reference_traversal_and_duplicate_json_key_are_rejected(self):
        _,release=install_parts_fixture(self.root,self.bundle,self.base)
        release['source_refs'][0]['file']='../supplier.dom.txt'
        with self.assertRaises(PartsReleaseError):self.load(save_release(self.root,release))
        raw=b'{"schema_version":1,"schema_version":1}'
        (self.root/'parts.json').write_bytes(raw)
        record={'path':'parts.json','sha256':sha256(raw).hexdigest(),'base_bundle_sha256':self.base}
        with self.assertRaisesRegex(PartsReleaseError,'duplicate'):self.load(record)


if __name__=='__main__':unittest.main()
