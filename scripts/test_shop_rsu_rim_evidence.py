"""Synthetic exact-SKU evidence, no private files, providers or live operations."""
from copy import deepcopy
from datetime import datetime,timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import json
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
from shop_catalog_public import variant_fingerprint
from shop_rsu_rim_evidence import (CURRENT_RULE,NATIVE_RULE,RimEvidenceError,_hash,_mapping,
    WIDTH_TABLE_SHA,derive_width_pairs,normalize_rim_geometry,prepare_expansion_bundle,validate_expansion_mapping)
from shop_rsu_quotes import RSUQuoteProvider,RSUQuotePolicy,RSUQuoteError
from tyresystem_api import ArticleInquiry
from test_shop_rsu_quotes import bindings as tyre_bindings

EAN='4026569455576'; PID='alloy_wheel:'+'a'*24
G={'width':'8','diameter':'18','offset':'39','bolt_count':'5','pcd':'112','centre_bore':'66.6'}
PHOTO='bilder/'+'c'*64+'.png'; BUNDLE_SHA='b'*64
NOW=datetime(2026,11,17,20,tzinfo=timezone.utc)


def record(article='42'):
    return {'Artikelnummer':article,'ean':EAN,'Hersteller':'Dezent','Profil':'TR silver 8x18 ET39 - LK5/112 ML66.6',
        'Herstellerartikelnummer':'TTRG8SA39AE','Artikeltyp':'Felge','Artikeluntertyp':'Alu','Dot':'','Demo':'','Da':''}


def current_fixture():
    product={'selection_id':PID,'category':'alloy_wheel','name':'DEZENT TR SILVER - Silber',
        'manufacturer_name':'DEZENT','width':'8','diameter':'18','size':'8 × 18 Zoll',
        'photo':PHOTO,'gallery':[PHOTO],'details':{'EAN':EAN},'quote_public':None,'price':None}
    identity=variant_fingerprint(product);r=record()
    proof={'rule':CURRENT_RULE,'product_id':PID,'identity_hash':identity,'alzura_source_sha256':'d'*64,
        'alzura':{'product_id':PID,'category':'alloy_wheel','manufacturer':'dezent','description':'tr silver - silber',
            'ean':EAN,'geometry':deepcopy(G),'manufacturer_article_number':None},
        'primary_sources':[{'provider':'ALZURA','area':'alloys','operation':'details','run':'fixture',
            'body_file':'one.body','body_sha256':'e'*64,'local_original_verified':True}],
        'source_reference':'TTRG8SA39AE','source_reference_equals_rsu_mpn':True,
        'rsu_source_sha256':'f'*64,'rsu_row_number':12,'rsu_record':r,'rsu_record_fingerprint':_hash(r),
        'rsu_geometry':deepcopy(G),'unique_rsu_ean':True,'reviewed_exact_ean_and_geometry':True,
        'manufacturer_text_equal':True,'description_text_equal':False,
        'vehicle_fitment_confirmed':False,'procurement_enabled':False}
    candidate={'product_id':PID,'category':'alloy_wheel','identity_hash':identity,'expected_ean':EAN,
        'supplier_article_id':'42','proof':proof,'proof_sha256':_hash(proof),'inquiries':[{'unit_net':'PRIVATE_OLD_PRICE'}]}
    return product,candidate


def native_fixture(article='99'):
    r=record(article);r.update(Hersteller='Borbet',Profil='V crystal silver 8x18 ET39 - LK5/112 ML66.6')
    pid='alloy_wheel:'+sha256(('RSU:'+article).encode()).hexdigest()[:24]
    product={'selection_id':pid,'category':'alloy_wheel','department':'wheels','name':'Borbet V crystal silver',
        'manufacturer_name':'Borbet','article_number':r['Herstellerartikelnummer'],**G,'ratio':None,
        'size':'8 × 18 Zoll · ET 39 · 5×112','dimensions_valid':True,'photo':PHOTO,'gallery':[PHOTO],
        'caption':'Modellfoto – genaue Größe und Ausführung der Auswahl beachten.',
        'details':{'EAN':EAN,'Herstellerartikelnummer':r['Herstellerartikelnummer'],'Einpresstiefe':'39 mm',
            'Lochkreis':'112','Lochzahl':'5','Mittenloch':'66.6','Fahrzeugpassung':'Für das konkrete Fahrzeug gesondert zu prüfen'},
        'identifiers':{'provider':'RSU','provider_id':article,'ean':EAN,'manufacturer_article_number':r['Herstellerartikelnummer']},
        'price':None,'quote_public':None,'price_status':'update_required','fitment_status':'not_confirmed',
        'approval':None,'technical_reports':[],'order_enabled':False}
    source={'selection_id':'alloy_wheel:'+'2'*24,'category':'alloy_wheel','manufacturer_name':'BORBET',
        'name':'BORBET V Crystal Silver','photo':PHOTO,'gallery':[PHOTO],'width':'8','diameter':'18'}
    photo={'rule':'exact_folded_manufacturer_full_design_finish_v1','manufacturer':'borbet','description':'v crystal silver',
        'photo':PHOTO,'image_url':'/shop/media/'+'c'*64+'.png','source_bundle_sha256':BUNDLE_SHA,
        'source_products':[{'product_id':source['selection_id'],'identity_hash':variant_fingerprint(source),
            'name':source['name'],'manufacturer':source['manufacturer_name']}],
        'model_photo_only':True,'sku_geometry_or_vehicle_fitment_not_inferred':True}
    key=_hash({'manufacturer':'borbet','description':'v crystal silver'})
    candidate={'product':product,'identity_hash':variant_fingerprint(product),'supplier_article_id':article,'expected_ean':EAN,
        'source_sha256':'f'*64,'source_row_number':13,'source_record':r,'source_record_fingerprint':_hash(r),'photo_proof_id':key}
    proof={'rule':NATIVE_RULE,'product':product,'identity_hash':candidate['identity_hash'],'rsu_source_sha256':'f'*64,
        'rsu_row_number':13,'rsu_record':r,'rsu_record_fingerprint':_hash(r),'photo_family':photo,'photo_proof_id':key}
    return product,candidate,photo,source,proof


def setup_bundle():
    p,c=current_fixture();n,nc,photo,source,_=native_fixture()
    b={'schema_version':1,'products':[p,source], 'publication':{'released_ids':[p['selection_id'],source['selection_id']],
        'image_urls':{PHOTO:photo['image_url']},'document_urls':{'original.pdf':'/shop/documents/original.pdf'}},
        'media':{'resources':{'c'*64+'.png':{'url':'https://media1.tyre-shopping.com/model.png'}},'approved_assets':['c'*64+'.png']},
        'browsing_evidence':{},'verified_fitments':{},'compatible_pairs':{},'rsu_bindings':tyre_bindings(),
        'shipping_classes':{},'product_documents':{'preserved':{'original':'document'}},'documents':{'preserved':True}}
    data={'immutable_sources':[{'file':'/private/bundle.json','sha256':BUNDLE_SHA}],
        'exact_current_id_candidates':[c],'native_rsu_candidates':[nc],'photo_family_proofs':{nc['photo_proof_id']:photo}}
    return b,data


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.network=patch.object(socket,'socket',side_effect=AssertionError('network_forbidden'))
        self.network.start();self.addCleanup(self.network.stop)

    def verify(self,proof,product_id=PID,identity=None):
        identity=identity or current_fixture()[1]['identity_hash']
        return validate_expansion_mapping(_mapping(proof),product_id=product_id,category='alloy_wheel',
            expected_ean=EAN,identity_hash=identity)

    def test_current_trade_name_difference_keeps_exact_geometry_and_no_prices(self):
        p,c=current_fixture();r=self.verify(c['proof'])
        self.assertEqual((r['supplier_article_id'],r['rim_geometry']),('42',G))
        self.assertNotIn('unit_net',json.dumps(r));self.assertNotIn('stock',r)

    def test_geometry_is_complete_bounded_and_not_boolean(self):
        self.assertEqual(normalize_rim_geometry({**G,'width':'8.00'}),G)
        for bad in ({k:v for k,v in G.items() if k!='centre_bore'},{**G,'pcd':True},{**G,'bolt_count':'5.5'},
                    {**G,'diameter':'31'},{**G,'offset':'151'},{**G,'centre_bore':'0'}):
            with self.subTest(bad=bad),self.assertRaises(RimEvidenceError):normalize_rim_geometry(bad)

    def test_valid_hashes_do_not_override_conflicting_source_facts(self):
        for mutation in (
            lambda p:p['alzura']['geometry'].update(centre_bore='57.1'),
            lambda p:p['alzura'].update(manufacturer='wrong company'),
            lambda p:p['alzura'].update(description='wrong model silver'),
            lambda p:p.update(unique_rsu_ean=False),lambda p:p.update(vehicle_fitment_confirmed=True),
            lambda p:p['primary_sources'][0].update(local_original_verified=False),
            lambda p:p['primary_sources'][0].update(operation='profiSearch'),
            lambda p:p['primary_sources'][0].update(body_file='../foreign.body'),
            lambda p:p.update(identity_hash='0'*64),lambda p:p['rsu_record'].update(Demo='1'),
            lambda p:p['rsu_record'].update(ean='4000000000000')):
            proof=deepcopy(current_fixture()[1]['proof']);mutation(proof)
            proof['rsu_record_fingerprint']=_hash(proof['rsu_record'])
            with self.subTest(mutation=mutation),self.assertRaises(RimEvidenceError):self.verify(proof)

    def test_unreviewed_trade_alias_and_false_reference_claim_rejected(self):
        for mutate in (lambda p:p['alzura'].update(description='tr black'),
                       lambda p:p.update(source_reference_equals_rsu_mpn=False)):
            p=deepcopy(current_fixture()[1]['proof']);mutate(p)
            with self.assertRaises(RimEvidenceError):self.verify(p)

    def test_native_source_id_and_exact_family_photo_valid(self):
        p,_,_,_,proof=native_fixture()
        r=self.verify(proof,p['selection_id'],variant_fingerprint(p));self.assertEqual(r['rim_geometry'],G)
        self.assertNotIn('alzura_source_sha256',r)

    def test_native_rejects_forged_geometry_model_photos_prices_and_fitments(self):
        changes=(lambda p:p['product'].update(centre_bore='57.1'),lambda p:p['product'].update(name='Borbet Wrong'),
            lambda p:p['product'].update(price={'unit_net':'5'}),lambda p:p['product'].update(order_enabled=True),
            lambda p:p['product']['details'].update(Fahrzeugpassung='Passt Audi A3'),
            lambda p:p['photo_family'].update(description='v black'),
            lambda p:p['photo_family']['source_products'][0].update(name='BORBET V Black'),
            lambda p:p['photo_family'].update(image_url='https://attacker.invalid/image.png'),
            lambda p:p['product'].update(size='20 × 20 Zoll'))
        for mutate in changes:
            product,_,_,_,proof=native_fixture();mutate(proof);identity=variant_fingerprint(proof['product']);proof['identity_hash']=identity
            with self.subTest(mutation=mutate),self.assertRaises(RimEvidenceError):self.verify(proof,product['selection_id'],identity)

    def test_provider_geometry_is_approved_identity_bound_copy_without_io(self):
        p,c=current_fixture();mapping=_mapping(c['proof']);data={PID:{'identity_hash':c['identity_hash'],
            'category':'alloy_wheel','mapping':mapping,'expected_ean':EAN}}
        class Inquiry:
            calls=[]
            def inquire(self,article,quantity):
                self.calls.append((article,quantity));return ArticleInquiry(article,EAN,quantity,Decimal('100'),20,'Fixture',NOW,'test')
        inquiry=Inquiry();policy=RSUQuotePolicy('test','EUR',True,'synthetic-rim-review')
        off=RSUQuoteProvider(bindings=data,approved_ids=set(),policy=policy,inquiry=inquiry,clock=lambda:NOW)
        product={'id':PID,'identity_hash':c['identity_hash'],'category':'alloy_wheel'}
        self.assertIsNone(off.rim_geometry(product))
        provider=RSUQuoteProvider(bindings=data,approved_ids={PID},policy=policy,inquiry=inquiry,clock=lambda:NOW)
        result=provider.rim_geometry(product);self.assertEqual(result['geometry'],G);self.assertEqual(inquiry.calls,[])
        self.assertIs(result['model_photo_only'],False)
        result['geometry']['pcd']='100';self.assertEqual(provider.rim_geometry(product)['geometry'],G)
        self.assertIsNone(provider.rim_geometry({**product,'identity_hash':'0'*64}))
        provider.retail_quote(product,4);self.assertEqual(inquiry.calls,[('42',4)])

    def test_builder_preserves_baseline_documents_identities_and_private_budgets(self):
        bundle,data=setup_bundle();before=deepcopy(bundle); original_data=deepcopy(data)
        result=prepare_expansion_bundle(bundle,data,source_bundle_sha256=BUNDLE_SHA,approved_current_ids={PID})
        out=result['bundle'];self.assertEqual(bundle,before);self.assertEqual(data,original_data)
        for key in bundle.keys()-{'rsu_bindings','shipping_classes'}:self.assertEqual(out[key],bundle[key])
        self.assertEqual(out['shipping_classes'][PID],'pkw_rim');self.assertEqual(len(out['rsu_bindings']),2)
        self.assertEqual(out['rsu_rim_geometry'][PID]['geometry'],G)
        self.assertNotIn('PRIVATE_OLD_PRICE',json.dumps(out));self.assertNotIn('budget',out)

    def test_builder_native_adds_released_photo_and_shipping_without_vehicle_approval(self):
        b,d=setup_bundle();out=prepare_expansion_bundle(b,d,source_bundle_sha256=BUNDLE_SHA,
            approved_current_ids={PID},include_natives=True)['bundle'];native=d['native_rsu_candidates'][0]['product']['selection_id']
        self.assertIn(native,out['publication']['released_ids']);self.assertEqual(out['shipping_classes'][native],'pkw_rim')
        self.assertEqual(out['verified_fitments'],{});self.assertEqual(out['documents'],b['documents'])
        self.assertEqual(out['media'],b['media']);self.assertEqual(len(out['products']),len(b['products'])+1)
        self.assertEqual(next(p for p in out['products'] if p['selection_id']==native)['quote_public'],None)

    def test_builder_refuses_changed_fingerprint_source_and_unreleased_photo(self):
        for change in (lambda b,d:b['products'][0].update(name='Changed product'),
            lambda b,d:d['immutable_sources'][0].update(sha256='0'*64),
            lambda b,d:b['products'][1].update(photo='bilder/'+'e'*64+'.png')):
            b,d=setup_bundle();change(b,d)
            with self.assertRaises(RimEvidenceError):prepare_expansion_bundle(b,d,source_bundle_sha256=BUNDLE_SHA,
                approved_current_ids={PID},include_natives=True)
        b,d=setup_bundle()
        with self.assertRaises(RimEvidenceError):prepare_expansion_bundle(b,d,source_bundle_sha256=BUNDLE_SHA,
            approved_current_ids={'alloy_wheel:'+'f'*24})

    def width_fixture(self):
        b,d=setup_bundle();tyre={'selection_id':'tyre:'+'3'*24,'category':'tyre',
            'manufacturer_name':'Continental','name':'Continental reviewed tyre','width':225,'ratio':40,'diameter':18,'group':'Sommerreifen'}
        b['products'].append(tyre);b['publication']['released_ids'].append(tyre['selection_id'])
        url='https://www.continental-reifen.de/reviewed-table.pdf'
        b['compatible_pairs'][PID]=[{'reviewed':True,'tyre_id':tyre['selection_id'],
            'rim_identity':variant_fingerprint(b['products'][0]),'tyre_identity':variant_fingerprint(tyre),
            'scope':'rim_width_candidate','evidence_id':'rim-width:reviewedfixture',
            'source':{'url':url,'sha256':WIDTH_TABLE_SHA,'page':30}}]
        table={'scope':'continental_group_dimensional_proposals_only','is_vehicle_approval':False,'construction':'R',
            'source':{'url':url,'sha256':WIDTH_TABLE_SHA},'rows':[{'width_mm':225,'ratio_percent':40,'diameter_in':18,
                'pdf_page':30,'visually_reviewed':True,'permitted_rim_widths_in':[7.5,8,8.5,9]}]}
        return b,d,table,tyre

    def test_native_finder_rebinds_only_reviewed_same_dimensions_not_vehicle_approval(self):
        b,d,table,tyre=self.width_fixture()
        result=prepare_expansion_bundle(b,d,source_bundle_sha256=BUNDLE_SHA,approved_current_ids={PID},
            include_natives=True,width_registry=table);out=result['bundle']
        native=d['native_rsu_candidates'][0]['product'];pairs=out['compatible_pairs'][native['selection_id']]
        self.assertEqual(len(pairs),1);self.assertEqual(pairs[0]['scope'],'rim_width_candidate')
        self.assertEqual(pairs[0]['rim_identity'],variant_fingerprint(native))
        self.assertEqual(pairs[0]['tyre_identity'],variant_fingerprint(tyre));self.assertEqual(out['verified_fitments'],{})
        self.assertEqual(result['report']['native_width_candidate_sets'],1)
        narrower=deepcopy(native);narrower['width']='7.5'
        self.assertEqual(derive_width_pairs(b,[narrower]),{})
        self.assertIn(narrower['selection_id'],derive_width_pairs(b,[narrower],width_registry=table))
        narrower['diameter']='19';self.assertEqual(derive_width_pairs(b,[narrower],width_registry=table),{})

    def test_width_proposals_reject_changed_tyre_unreviewed_page_and_wrong_scope(self):
        for change in (lambda b,t:b['products'][-1].update(name='Changed tyre'),
            lambda b,t:b['compatible_pairs'][PID][0].update(scope='vehicle_approved'),
            lambda b,t:b['compatible_pairs'][PID][0]['source'].update(sha256='0'*64),
            lambda b,t:t['rows'][0].update(visually_reviewed=False),
            lambda b,t:t['rows'][0].update(permitted_rim_widths_in=[6,6.5])):
            b,d,table,_=self.width_fixture();change(b,table)
            self.assertEqual(derive_width_pairs(b,[d['native_rsu_candidates'][0]['product']],width_registry=table),{})

    def test_native_copies_only_unanimous_same_family_finish_with_new_fingerprint(self):
        b,d=setup_bundle();source=b['products'][1];source['details']={'Farbe':'Crystal Silver'}
        photo=next(iter(d['photo_family_proofs'].values()));photo['source_products'][0]['identity_hash']=variant_fingerprint(source)
        result=prepare_expansion_bundle(b,d,source_bundle_sha256=BUNDLE_SHA,approved_current_ids={PID},include_natives=True)
        native=d['native_rsu_candidates'][0]['product'];new=next(p for p in result['bundle']['products'] if p['selection_id']==native['selection_id'])
        self.assertEqual(new['details']['Farbe'],'Crystal Silver')
        self.assertNotEqual(variant_fingerprint(new),variant_fingerprint(native))
        self.assertEqual(result['bundle']['rsu_bindings'][new['selection_id']]['identity_hash'],variant_fingerprint(new))

    def test_candidate_size_guard_remains_bounded(self):
        b,d=setup_bundle()
        with patch('shop_rsu_rim_evidence.MAX_CANDIDATE_BYTES',1),self.assertRaises(RimEvidenceError) as captured:
            prepare_expansion_bundle(b,d,source_bundle_sha256=BUNDLE_SHA,approved_current_ids={PID})
        self.assertEqual(captured.exception.code,'candidate_bundle_too_large')
        self.assertGreater(captured.exception.preparation_report['candidate_json_bytes'],1)


if __name__=='__main__':unittest.main()
