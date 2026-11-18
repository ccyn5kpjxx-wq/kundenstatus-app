"""Nine-row supplement: synthetic originals, isolation and tampering checks."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT/'scripts'))
import shop_rim_width_supplement as extension
from shop_catalog_public import variant_fingerprint
from shop_rim_width_candidates import released_width_pairs
from shop_rsu_match import alzura_record, match_catalogues, rsu_record
from test_shop_rim_width_candidates import registry, rim


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def fixture():
    rows, bindings, proofs = [], {}, {}
    for n, (diameter, width) in enumerate(((16, 6.5), (16, 7), (21, 9), (21, 10.5)), 1):
        row = rim(); row.update(selection_id='alloy_wheel:'+f'{n:024x}', width=str(width), diameter=str(diameter)); rows.append(row)
    for n, (pid, values) in enumerate(extension.EXPECTED.items(), 1):
        width, ratio, diameter, load, xl, alz_row, rsu_row, receipt = values
        mpn='SYNTHETIC-'+str(n)
        p={'selection_id':pid,'category':'tyre','dimensions_valid':True,'manufacturer_name':'Continental',
           'name':'Synthetic Model '+str(n),'article_number':mpn,'width':str(width),'ratio':str(ratio),
           'diameter':str(diameter),'load_index':str(load),'speed_index':'V','season':'Sommerreifen',
           'size':f'{width}/{ratio} R{diameter} {load}V','details':{'Herstellerartikelnummer':mpn}}
        a=dict(id=str(123456+n),brandname='Continental',manufacturer_number=mpn,width=str(width),height=str(ratio),
               inner_diameter=str(diameter),tyre_type='R',load_index=str(load),speed_index='V',type='S',
               dot='0',demo='0',da_decke='0',runflat='0',ms='0',**{'3pmfs':'0'})
        r=dict(Artikelnummer=str(654321+n),Hersteller='Continental',Herstellerartikelnummer=mpn,
               Reifenbreite=str(width),Reifenquerschnitt=str(ratio),Reifendurchmesser=str(diameter),Bauart='R',
               Loadindex=str(load),Speedindex='V',Saison='s',Artikeltyp='Reifen',Artikeluntertyp='PKW',Dot='',Demo='',Da='')
        mapping=match_catalogues([alzura_record(a,archive_sha256=extension.SOURCES['alzura_archive_sha256'],row_number=alz_row)],
                                [rsu_record(r,csv_sha256=receipt,row_number=rsu_row)])['mappings'][0]
        identity=variant_fingerprint(p)
        bindings[pid]={'identity_hash':identity,'category':'tyre','mapping':mapping,'expected_ean':'4019238594195'}
        proof={'rule':'alzura_full_export_nominal_text_v1','product_id':pid,'identity_hash':identity,
               'alzura_source_sha256':extension.SOURCES['alzura_archive_sha256'],'alzura_row_number':alz_row,
               'alzura_article_id':a['id'],'articlename':f'SYNTHETIC {width}/{ratio} R{diameter} {load} V'+(' XL' if xl else ''),
               'description':'','info':'','manufacturer_description':'','runflat':'0',
               'rsu_source_sha256':receipt,'rsu_row_number':rsu_row,'rsu_profile':'SYNTHETIC'+(' XL' if xl else '')}
        proofs[pid]={'proof':proof,'proof_sha256':sha256(encode(proof)).hexdigest()};rows.append(p)
    table=registry();table['source']['sha256']=extension.WIDTH_TABLE_SHA
    bundle={'products':rows,'rsu_bindings':bindings,'nominal_tyre_sources':{},'rsu_price_seeds':{'records':{pid:{} for pid in proofs}},
            'publication':{'released_ids':[p['selection_id'] for p in rows]},'reviewed_rim_width_registry':table,
            'compatible_pairs':{},'verified_fitments':{},'documents':{'unchanged':True},'shipping_classes':{'unchanged':True}}
    value={'schema_version':1,'base_bundle_sha256':'a'*64,'registry_rows':deepcopy(list(extension.REVIEWED_ROWS)),
           'nominal_tyre_sources':proofs,'sources':dict(extension.SOURCES)}
    return bundle,value


class RimWidthSupplementChecks(unittest.TestCase):
    def setUp(self):
        guard=patch.object(socket,'socket',side_effect=AssertionError('No network'));guard.start();self.addCleanup(guard.stop)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        self.bundle,self.value=fixture();self.record=self.save()

    def save(self, raw=None):
        raw=encode(self.value) if raw is None else raw;(self.root/'supplement.json').write_bytes(raw)
        return {'path':'supplement.json','sha256':sha256(raw).hexdigest(),'base_bundle_sha256':'a'*64}

    def load(self, record=None, bundle=None):
        return extension.load_rim_width_supplement(self.root,self.record if record is None else record,
                self.bundle if bundle is None else bundle,base_bundle_sha256='a'*64)

    def test_absence_does_no_io(self):
        with patch.object(extension,'_read',side_effect=AssertionError('No read')):
            self.assertIs(extension.load_rim_width_supplement(self.root,None,self.bundle,base_bundle_sha256='a'*64),self.bundle)

    def test_append_only_preserves_every_other_capability_and_input(self):
        before=deepcopy(self.bundle);result=self.load();self.assertEqual(self.bundle,before)
        self.assertEqual(len(result['nominal_tyre_sources']),9)
        self.assertEqual(result['reviewed_rim_width_registry']['rows'][:-9],self.bundle['reviewed_rim_width_registry']['rows'])
        for key in self.bundle:
            if key not in ('nominal_tyre_sources','reviewed_rim_width_registry'):self.assertIs(result[key],self.bundle[key])
        pairs=released_width_pairs(result,approved_ids=set(result['rsu_bindings']))
        self.assertEqual({p['tyre_id'] for values in pairs.values() for p in values},set(extension.EXPECTED))
        self.assertTrue(all(p['scope']=='rim_width_candidate' and 'fitment_spec' not in p for values in pairs.values() for p in values))

    def test_descriptor_hash_path_and_extra_capability_rejected(self):
        for delta in ({'path':'../escape'},{'path':str(self.root/'supplement.json')},{'sha256':'f'*64},
                      {'base_bundle_sha256':'f'*64},{'extra':True}):
            with self.subTest(delta=delta),self.assertRaises(extension.RimWidthSupplementError):self.load({**self.record,**delta})
        for key in ('rsu_bindings','rsu_price_seeds','verified_fitments','documents'):
            v=deepcopy(self.value);v[key]={}
            with self.subTest(key=key),self.assertRaises(extension.RimWidthSupplementError):self.load(self.save(encode(v)))

    def test_exact_reviewed_row_types_dimensions_pages_and_source_pins_required(self):
        for change in ('width','page','bool_type','source'):
            v=deepcopy(self.value)
            if change=='width':v['registry_rows'][0]['permitted_rim_widths_in'].append(7)
            elif change=='page':v['registry_rows'][0]['pdf_page']=22
            elif change=='bool_type':v['registry_rows'][0]['visually_reviewed']=1
            else:v['sources']['width_table_sha256']='f'*64
            with self.subTest(change=change),self.assertRaises(extension.RimWidthSupplementError):self.load(self.save(encode(v)))

    def test_original_reference_text_identity_or_rehashed_tampering_rejected(self):
        pid=next(iter(extension.EXPECTED))
        for key,value in (('alzura_row_number',1),('rsu_row_number',1),('rsu_source_sha256','f'*64),
                          ('info','SSR'),('articlename','SYNTHETIC 295/30 R21 101 V XL'),('identity_hash','f'*64)):
            v=deepcopy(self.value);v['nominal_tyre_sources'][pid]['proof'][key]=value
            v['nominal_tyre_sources'][pid]['proof_sha256']=sha256(encode(v['nominal_tyre_sources'][pid]['proof'])).hexdigest()
            with self.subTest(key=key),self.assertRaises(extension.RimWidthSupplementError):self.load(self.save(encode(v)))

    def test_existing_row_source_override_missing_membership_or_duplicate_rejected(self):
        pid=next(iter(extension.EXPECTED))
        for mode in ('seed','binding','released','old_source','old_size','duplicate'):
            b=deepcopy(self.bundle)
            if mode=='seed':b['rsu_price_seeds']['records'].pop(pid)
            elif mode=='binding':b['rsu_bindings'].pop(pid)
            elif mode=='released':b['publication']['released_ids'].remove(pid)
            elif mode=='old_source':b['nominal_tyre_sources'][pid]=deepcopy(self.value['nominal_tyre_sources'][pid])
            elif mode=='old_size':b['reviewed_rim_width_registry']['rows'].append(deepcopy(extension.REVIEWED_ROWS[0]))
            else:b['products'].append(deepcopy(b['products'][-1]))
            with self.subTest(mode=mode),self.assertRaises(extension.RimWidthSupplementError):self.load(bundle=b)

    def test_old_pairs_cannot_rescue_new_unproved_tyres_or_foreign_brand_registry(self):
        b=deepcopy(self.bundle);b['products']=[p for p in b['products'] if p['category']=='tyre']
        b['compatible_pairs']={'alloy_wheel:'+'b'*24:[{'tyre_id':p} for p in extension.EXPECTED]}
        with self.assertRaisesRegex(extension.RimWidthSupplementError,'not_eligible'):self.load(bundle=b)
        b=deepcopy(self.bundle);b['reviewed_rim_width_registry']['brands']=['Michelin']
        with self.assertRaises(extension.RimWidthSupplementError):self.load(bundle=b)

    def test_oversize_duplicate_json_and_nonfinite_fail_closed(self):
        for raw in (b' '*(extension.MAX_BYTES+1),b'{"x":1,"x":2}',b'{"x":NaN}',b'{'):
            with self.subTest(size=len(raw)),self.assertRaises(extension.RimWidthSupplementError):self.load(self.save(raw))


if __name__=='__main__':unittest.main(verbosity=2)
