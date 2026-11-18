"""Synthetic offline reconciliation: no provider, release or financial writes."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parent))
from reconcile_rsu_parts import (ReconciliationError,catalogue_records,decode,encode,load_observations,
    missing_identity_requests,overlay,parts_source_record,read_pinned,reconcile,source_rows,write_new)

ID='parts:'+'a'*24
EAN='4006381333931'
AS_OF='2026-11-18T12:00:00+00:00'


def row(identifier=ID,ean=EAN,mpn='A-1'):
    return {'selection_id':identifier,'manufacturer_name':'MAKER','category':'accessory','department':'parts',
        'name':'SYNTHETIC part','article_number':mpn,'unit_label':'Stück','packing_quantity':1,'new_part':True,
        'details':{'Herstellerartikelnummer':mpn,'EAN':ean},'price':{'source':'ALZURA','unit_gross_cent':999999},
        'provider_id':'ALZURA_VENDOR_CANARY'}


def observation(article='123',mpn='A-1',ean=EAN,**changes):
    value={'article_id':article,'manufacturer':'MAKER','mpn':mpn,'ean':ean,'read_kind':'inquiry',
        'observed_at':'2026-11-18T11:59:00+00:00','quantity':1,'unit_net':'10.00','stock':2,
        'category':'accessory','unit_label':'Stück','packing_quantity':1,'condition':'new','freight':None,'supplier_offer':None}
    value.update(changes)
    return {'source':parts_source_record('rsu',source_sha256='b'*64,row_number=1,
        article_id=article,manufacturer='MAKER',mpn=mpn,ean=ean),'row':value,
        'source_refs':[{'file':'supplier.json','sha256':'b'*64}]}


def with_freight():
    item=observation();ui={'file':'supplier-ui.dom.txt','sha256':'c'*64};rest={'file':'rest.json','sha256':'d'*64}
    item['source_refs'] += [ui,rest]
    item['row']['freight']={'rule':'rsu_ui_unique_q1_freight_v1','source_ref':ui,'inquiry_ref':rest,
        'observed_at':'2026-11-18T11:57:00+00:00','quantity':1,'offer_count':1,'supplier_article_id':'123',
        'ean':EAN,'manufacturer':'MAKER','mpn':'A-1','unit_net':'10.00','stock_reported':2,
        'freight_net':'6.00','currency':'EUR','mainland_only':True}
    return item


class ReconciliationChecks(unittest.TestCase):
    def setUp(self):
        guard=patch.object(socket,'socket',side_effect=AssertionError('No network'));guard.start();self.addCleanup(guard.stop)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)

    def run_reconciliation(self,items=None,rows=None):
        bundle={'products':rows or [row()]};original=deepcopy(bundle)
        result=reconcile(bundle,'a'*64,[observation()] if items is None else items,as_of=AS_OF)
        self.assertEqual(bundle,original)
        return result

    def test_real_supplier_observation_creates_exact_proof_and_15percent_seed_only(self):
        result=self.run_reconciliation();binding=result['bindings'][ID];proof=binding['mapping']['evidence']
        self.assertEqual(proof['catalogue']['article_id'],ID);self.assertEqual(proof['rsu']['article_id'],'123')
        self.assertEqual(proof['match_basis'],'ean_manufacturer_mpn');self.assertIsNone(proof['supplier_offer'])
        self.assertEqual(result['rsu_price_seeds'][ID]['unit_gross_cent'],1369)
        self.assertNotIn('ALZURA_VENDOR_CANARY',json.dumps(result));self.assertNotIn('999999',json.dumps(result))
        self.assertFalse(result['coverage'][0]['checkout_ready']);self.assertIn('rsu_parts_freight_source_missing',result['coverage'][0]['blocked_reasons'])
        self.assertEqual(overlay(result,activation_ready=True)['rsu_bindings'],{})
        candidate=overlay(result,activation_ready=False);self.assertIn(ID,candidate['rsu_bindings']);self.assertEqual(candidate['shipping_classes'],{})

    def test_missing_catalogue_ean_uses_unique_exact_maker_mpn_not_vendor_id(self):
        result=self.run_reconciliation(rows=[row(ean='')]);self.assertEqual(result['bindings'][ID]['mapping']['evidence']['match_basis'],'manufacturer_mpn')
        conflict=self.run_reconciliation([observation(ean='5901234123457')]);self.assertFalse(conflict['bindings'])
        self.assertIn('rsu_ean_missing_or_conflicting',conflict['coverage'][0]['blocked_reasons'])
        missing=row(ean='');missing.pop('packing_quantity')
        result=self.run_reconciliation(rows=[missing]);self.assertFalse(result['bindings'])
        self.assertIn('mpn_only_sale_unit_or_pack_evidence_missing',result['coverage'][0]['blocked_reasons'])
        for changes in [{'unit_label':'Packung'},{'packing_quantity':2}]:
            self.assertFalse(self.run_reconciliation([observation(**changes)],[row(ean='')])['bindings'])

    def test_duplicate_supplier_ids_catalogue_skus_and_conflicting_identity_are_not_chosen(self):
        for sources,rows in [([observation(),observation(article='124')],None),
            ([observation()],[row(),row('parts:'+'c'*24)]),
            ([observation(),observation(mpn='OTHER')],None)]:
            result=self.run_reconciliation(sources,rows);self.assertFalse(result['bindings']);self.assertFalse(result['rsu_price_seeds'])
        marked=observation();marked['source_id_conflict']=True
        self.assertFalse(self.run_reconciliation([marked])['bindings'])

    def test_offer_selector_cannot_be_silently_dropped(self):
        result=self.run_reconciliation([observation(supplier_offer={'warehouse_id':'SYNTHETIC'})])
        self.assertFalse(result['bindings']);self.assertIn('unusable_or_undocumented_supplier_offer',result['coverage'][0]['blocked_reasons'])

    def test_ui_prices_missing_prices_q2_and_market_values_do_not_become_seed(self):
        for changes in [{'read_kind':'supplier_ui'},{'unit_net':None},{'quantity':2},{'unit_net':float('nan')},{'unit_net':'-1'},
                        {'observed_at':'2026-11-01T12:00:00Z'},{'observed_at':'2026-11-20T12:00:00Z'},{'stock':True}]:
            with self.subTest(changes=changes):
                result=self.run_reconciliation([observation(**changes)]);self.assertIn(ID,result['bindings']);self.assertFalse(result['rsu_price_seeds'])
        self.assertFalse(self.run_reconciliation([])['rsu_price_seeds'])

    def test_historical_stock_is_not_relabelled_fresh_or_extended_to_other_quantity(self):
        result=self.run_reconciliation([observation(observed_at='2026-11-17T12:00:00Z',stock=1)])
        self.assertEqual(result['coverage'][0]['supported_observed_quantities'],[1]);self.assertEqual(result['rsu_price_seeds'][ID]['stock_reported'],1)
        self.assertIn('fresh_exact_quantity_stock_required',result['coverage'][0]['blocked_reasons'])
        self.assertIn('exact_quantity_stock_insufficient_or_unknown',self.run_reconciliation([observation(stock=0)])['coverage'][0]['blocked_reasons'])

    def test_latest_timestamp_wins_but_same_timestamp_conflicting_price_is_blocked(self):
        result=self.run_reconciliation([observation(observed_at='2026-11-17T12:00:00Z',unit_net='9.00'),observation(unit_net='10.00')])
        self.assertEqual(result['rsu_price_seeds'][ID]['unit_gross_cent'],1369)
        result=self.run_reconciliation([observation(unit_net='9.00'),observation(unit_net='10.00')]);self.assertFalse(result['rsu_price_seeds'])

    def test_explicit_json_supplier_and_csv_headers_required(self):
        base={'schema_version':1,'supplier':'RSU','currency':'EUR','price_basis':'net','records':[observation()['row']]}
        self.assertEqual(len(list(source_rows(encode(base),{'format':'rsu_read_json'}))),1)
        for key,value in [('supplier','ALZURA'),('price_basis','gross'),('currency','USD')]:
            with self.assertRaises(ReconciliationError):list(source_rows(encode({**base,key:value}),{'format':'rsu_read_json'}))
        with self.assertRaises(ReconciliationError):list(source_rows(b'wholesaler_id;price\n123;10',{'format':'rsu_csv'}))
        csv=b'Artikelnummer;Hersteller;Herstellerartikelnummer;ean;Einkaufspreis;Lagerbestand\n123;MAKER;A-1;4006381333931;10,00;2\n'
        parsed=list(source_rows(csv,{'format':'rsu_csv','observed_at':AS_OF}));self.assertEqual(parsed[0][0],1);self.assertEqual(parsed[0][1]['unit_net'],'10,00')

    def test_source_hash_duplicate_key_bounded_input_and_immutable_output(self):
        p=self.root/'source.json';p.write_bytes(b'{}')
        with self.assertRaises(ReconciliationError):read_pinned(p,'f'*64)
        with self.assertRaises(ReconciliationError):read_pinned(p,sha256(p.read_bytes()).hexdigest(),1)
        with self.assertRaises(ReconciliationError):decode(b'{"x":1,"x":2}')
        out=self.root/'output.json';first=write_new(out,{'one':1});self.assertEqual(write_new(out,{'one':1}),first)
        with self.assertRaises(ReconciliationError):write_new(out,{'one':2})
        self.assertEqual(decode(out.read_bytes()),{'one':1})

    def test_cross_source_id_conflict_outside_wanted_maker_is_detected(self):
        supplier={'schema_version':1,'supplier':'RSU','currency':'EUR','price_basis':'net',
            'records':[observation()['row'],observation(mpn='UNRELATED')['row']]}
        p=self.root/'supplier.json';p.write_bytes(encode(supplier));manifest={'schema_version':1,'sources':[{'file':str(p),'sha256':sha256(p.read_bytes()).hexdigest(),'format':'rsu_read_json'}]}
        rows,stats=load_observations(manifest,self.root/'manifest.json',catalogue_records({'products':[row()]},'a'*64))
        self.assertEqual(stats['rows_scanned'],2);self.assertEqual(stats['conflicting_supplier_article_ids'],1)
        self.assertFalse(self.run_reconciliation(rows)['bindings'])

    def test_original_receipt_hash_is_verified_and_missing_lookup_plan_has_no_guessed_id(self):
        receipt=self.root/'original.json';receipt.write_bytes(b'ORIGINAL')
        supplier={'schema_version':1,'supplier':'RSU','currency':'EUR','price_basis':'net','records':[observation()['row']],
            'source_receipts':[{'file':str(receipt),'sha256':sha256(b'ORIGINAL').hexdigest()}]}
        p=self.root/'supplier.json';p.write_bytes(encode(supplier));manifest={'schema_version':1,'sources':[{'file':str(p),'sha256':sha256(p.read_bytes()).hexdigest(),'format':'rsu_read_json'}]}
        receipt.write_bytes(b'ALTERED')
        with self.assertRaises(ReconciliationError):load_observations(manifest,self.root/'manifest.json',catalogue_records({'products':[row()]},'a'*64))
        result=self.run_reconciliation([]);catalogue=catalogue_records({'products':[row()]},'a'*64)
        requests=missing_identity_requests(catalogue,result)
        self.assertEqual(requests['requests_executed'],0);self.assertFalse(requests['supplier_ids_guessed'])
        self.assertEqual(requests['requests'][0]['mpn'],'a-1');self.assertNotIn('supplier_article_id',requests['requests'][0])

    def test_genuine_unique_fresh_q1_offer_can_prepare_oil_activation_with_15percent_freight(self):
        oil=row();oil.update(category='oil',department='accessories',unit_label='1 Liter')
        item=with_freight();item['row']['condition']=None
        result=self.run_reconciliation([item],[oil]);entry=result['coverage'][0]
        self.assertTrue(entry['checkout_ready']);self.assertEqual(entry['blocked_reasons'],[])
        active=overlay(result,activation_ready=True);self.assertIn(ID,active['rsu_bindings'])
        freight=active['shipping_classes'][ID];self.assertEqual(freight['retail_gross_cent'],821)
        self.assertEqual(freight['expires_at'],'2026-11-18T12:12:00+00:00','newer REST must not extend older UI validity')
        copied=dict(freight);proof=copied.pop('proof_sha256');self.assertEqual(sha256(encode(copied)).hexdigest(),proof)
        self.assertEqual(freight['binding_evidence_id'],active['rsu_bindings'][ID]['mapping']['evidence_id'])
        # Vehicle-specific parts still need their independent vehicle/sale policy.
        self.assertFalse(self.run_reconciliation([item])['coverage'][0]['checkout_ready'])

    def test_ambiguous_expired_future_or_inconsistent_ui_freight_never_activates(self):
        oil=row();oil.update(category='care',department='accessories')
        for field,value in [('offer_count',2),('quantity',2),('stock_reported',3),('unit_net','9.00'),
                            ('ean','5901234123457'),('mainland_only',False),('currency','USD'),
                            ('observed_at','2026-11-18T12:01:00Z'),('observed_at','2026-11-18T11:00:00Z')]:
            item=with_freight();item['row']['freight'][field]=value
            result=self.run_reconciliation([item],[oil]);self.assertFalse(result['shipping_classes']);self.assertFalse(overlay(result,activation_ready=True)['rsu_bindings'])
        item=with_freight();item['row']['freight']['source_ref']={'file':'not-verified.txt','sha256':'f'*64}
        self.assertFalse(self.run_reconciliation([item],[oil])['shipping_classes'])
        item=with_freight();item['row']['freight']['freight_net']='0.00'
        result=self.run_reconciliation([item],[oil]);self.assertTrue(result['coverage'][0]['checkout_ready']);self.assertEqual(result['shipping_classes'][ID]['retail_gross_cent'],0)


if __name__=='__main__':unittest.main()
