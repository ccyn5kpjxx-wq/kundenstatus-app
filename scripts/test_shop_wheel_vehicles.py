"""Vehicle filters must retain the exact article, PDF, type and tyre-size scope."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask
from shop_catalog_public import variant_fingerprint
from shop_public_api import create_shop_public_blueprint
from test_shop_catalog_public import part, verified, wheel
from test_shop_catalog_enriched import catalogue, pair, tyre


class VehicleApplicationTests(unittest.TestCase):
    def setUp(self):
        self.rim = wheel(name='Same design', identifiers={'provider': 'ALZURA', 'provider_id': '617'},
            vehicle_candidates=[{'scope':'provider', 'car_id':12, 'brand':'MINI', 'model':'Cooper', 'variant':'Typ R56'}])
        self.unrelated = wheel('b', name='Same design', identifiers={'provider':'ALZURA','provider_id':'618'},
            vehicle_candidates=[{'scope':'provider','car_id':13,'brand':'MINI','model':'Countryman','variant':'Typ R60'}])
        self.fit = verified(self.rim)
        self.fit['report_keys'] = ['ONE', 'TWO']
        base = self.fit['vehicles'][0]
        self.fit['vehicles'] = [{**base, 'make':'MINI', 'model':'Cooper', 'conditions':['R56 only']},
            {**base,'make':'Audi','model':'Q5','conditions':['Q5 only']}]
        self.path = '/shop/documents/' + 'a'*64 + '.pdf'
        self.good = tyre('1','Winterreifen')
        self.inquiry = tyre('2','Winterreifen')
        self.weak = {**tyre('3','Winterreifen'), 'load_index':'90'}
        self.wrong = {**tyre('4','Winterreifen'), 'diameter':'17','size':'205/55 R17 91V'}
        self.conflict = {**tyre('5','Winterreifen'), 'size':'215/65 R18 91V'}
        source = {'scope':'rim_width_candidate','evidence_id':'reviewed-width-row',
                  'source':{'page':2,'sha256':'e'*64}}
        self.rule = {'make':'MINI','model':'Cooper','variant':'Typ R56','report':'a'*64,
                     'page':2,'size':'205/55 R18', 'min_load_index':91,'condition':'R56 only'}
        fake = part(name='MINI Cooper Ventilkappen')
        self.rows = [self.rim, self.unrelated, self.good, self.inquiry, self.weak, self.wrong, self.conflict, fake]
        self.rid, self.tid = self.rim['selection_id'], self.good['selection_id']
        with patch('shop_wheel_vehicles._report_rows', side_effect=[{variant_fingerprint(self.rim):[self.rule]},{}]):
            self.catalog = catalogue(self.rows, verified_fitments={self.rid:self.fit},
                compatible_pairs={self.rid:[{**pair(self.rim,t),**source} for t in (self.good,self.weak,self.wrong,self.conflict)]},
                procurement_ids={self.rid,self.tid,self.inquiry['selection_id']},
                document_urls={'ONE':self.path,'TWO':'/shop/documents/'+'b'*64+'.pdf'})
        app=Flask(__name__)
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED']=True
        app.register_blueprint(create_shop_public_blueprint(catalogue=self.catalog))
        self.client=app.test_client()
        self.base='/shop/api/v2/products'
        self.selected=self.get('',q='mini')['wheel_vehicles'][0]
        self.filters={'wheel_make':'MINI','wheel_vehicle':self.selected['key']}

    def get(self, path, **params):
        response=self.client.get(self.base+path,query_string=params)
        self.assertEqual(response.status_code,200,response.json)
        return response.json

    def test_text_brand_model_families_variants_and_documents_keep_scope(self):
        self.assertTrue(self.selected['has_report'])
        self.assertEqual(self.selected['model'],'Cooper')
        self.assertEqual(self.get('', q='MINI')['total'],2)
        result=self.get('', q='MiNi Cooper', group='family')
        self.assertEqual(result['wheel_query_make'],'MINI')
        self.assertEqual(result['items'][0]['family'],{'variant_count':1,'diameters':['18']})
        for item in (result['items'][0],self.get('/'+self.rid,wheel_query='MINI Cooper')['item'],
                     self.get('/'+self.rid+'/variants',wheel_query='mini cooper')['items'][0]):
            self.assertEqual(item['id'],self.rid)
            self.assertEqual(item['fitment']['reports'],[self.path])
            self.assertEqual(item['fitment']['vehicles'][0]['report_url'],self.path)
            self.assertEqual([v['make'] for v in item['fitment']['vehicles']],['MINI'])
            self.assertNotIn('product_documents',item)
        self.assertEqual(self.get('',q='Mini Cooper S123')['items'],[])
        self.assertEqual(self.client.get(self.base+'/'+self.unrelated['selection_id'],query_string=self.filters).status_code,404)
        self.assertEqual(self.get('',wheel_make='BMW',wheel_vehicle=self.selected['key'])['items'],[])

    def test_released_original_pdf_bytes_enable_only_this_rim_and_preserve_inquiry_path(self):
        from test_shop_rim_reports import fixture
        row, doc, urls = fixture()
        asset = next(iter(urls.values())).split('/')[-1]
        digest = 'c' * 64
        rule = {'make':'MINI', 'model':'Cooper', 'variant':'Typ UKL-L', 'conditions':['55–155 kW. Nur Fließheck.'],
                'report':asset[:-4], 'pdf_sha256':digest, 'model_aliases':['One','One D','Cooper D','Cooper S'], 'page':2, 'size':'205/45 R19','min_load_index':0}
        token = variant_fingerprint(row)
        unreviewed = wheel('b', manufacturer_name='Other', identifiers={'provider':'ALZURA','provider_id':'33'},
            vehicle_candidates=[{'scope':'provider','car_id':9,'brand':'Mini','model':'Cooper','variant':'Typ UKL-L'}])
        def build(hash_value):
            with patch('shop_wheel_vehicles._report_rows', side_effect=[{}, {token:[rule]}]):
                return catalogue([row,unreviewed], product_documents={row['selection_id']:doc}, document_urls=urls,
                    released_rim_report_resources={asset:{'kind':'wheel_report','sha256':hash_value,'file':asset}})
        catalog=build(digest)
        choices=catalog.page(department='wheels',wheel_make='MINI')['wheel_vehicles']
        self.assertEqual(len(choices),1)
        self.assertEqual(choices[0]['reviewed_model_names'],['Cooper','Cooper D','Cooper S','One','One D'])
        filtered = catalog.page(department='wheels',wheel_make='MINI',brand='Other')
        self.assertNotIn('reviewed_model_names',filtered['wheel_vehicles'][0])
        article = catalog.get(unreviewed['selection_id'],wheel_vehicle=choices[0]['key'])
        self.assertNotIn('reviewed_model_names',article['wheel_vehicle_match'])
        self.assertFalse(article['wheel_vehicle_match']['has_report'])
        self.assertEqual(catalog.get(row['selection_id'])['fitment']['vehicles'][0]['report_url'],next(iter(urls.values())))
        filters={'wheel_vehicle':choices[0]['key'],'wheel_query':'MINI'}
        items=catalog.page(department='wheels',complete='1',group='family',**filters)['items']
        variants=catalog.variants(row['selection_id'],complete='1',**filters)['items']
        for product in (items[0],variants[0],catalog.get(row['selection_id'],**filters)):
            self.assertEqual(product['fitment']['reports'],list(urls.values()))
            self.assertEqual(product['tyre_seasons'],[])
            self.assertEqual(product['vehicle_sizes'],['205/45 R19'])
            self.assertEqual(product['fitment']['vehicles'][0]['report_page'],2)
            self.assertFalse(product['order_enabled'])
        for name in ('One','One D','Cooper','Cooper D','Cooper S'):
            self.assertEqual(catalog.page(department='wheels',q='MINI '+name)['items'][0]['fitment']['reports'],list(urls.values()))
        for name in ('Countryman', 'JCW', 'Cooper ALL4', 'Cooper Cabrio', 'Cooper SD'):
            self.assertEqual(catalog.page(department='wheels',q='MINI '+name)['items'],[])
        changed = build('f'*64).page(department='wheels',wheel_make='MINI')
        self.assertEqual([p['id'] for p in changed['items']],[unreviewed['selection_id']])
        self.assertNotIn('reviewed_model_names',changed['wheel_vehicles'][0])

    def test_technical_pairs_price_and_seasons_require_selected_type_and_exact_size(self):
        listing=self.get('', department='tyres',wheel_make='MINI')
        self.assertEqual(listing['items'],[])
        chosen=self.get('',department='tyres',season='winter',**self.filters)
        self.assertEqual([p['id'] for p in chosen['items']],[self.tid,self.inquiry['selection_id']])
        other=chosen['items'][1]
        self.assertEqual(other['purchase_status'],'inquiry_only')
        self.assertEqual(other['wheel_fitment_scope'],'report_size_inquiry')
        self.assertEqual(other['wheel_fitment_sources'][0]['rim_id'],self.rid)
        self.assertEqual(other['wheel_fitment_sources'][0]['reports'],[self.path])
        self.assertEqual(self.get('',department='tyres',q='MINI Cooper R56')['total'],2)
        for args in ({'wheel_query':'MINI Countryman'}, {'make':'Audi','model':'Q5'}, {'wheel_make':'MINI'}):
            self.assertEqual(self.get('/'+self.rid+'/compatible-tyres',**args)['items'],[])
        exact=self.get('/'+self.rid+'/compatible-tyres',season='winter',**self.filters)
        self.assertEqual(exact['vehicle_sizes'],['205/55 R18'])
        self.assertEqual([p['id'] for p in exact['items']],[self.tid])
        self.assertEqual(self.get('/'+self.rid+'/complete-wheel-price',tyre_id=self.tid,quantity=1,**self.filters)['tyre_id'],self.tid)
        self.assertEqual(self.client.get(self.base+'/'+self.rid+'/complete-wheel-price',
            query_string={**self.filters,'tyre_id':other['id'],'quantity':1}).status_code,404)
        self.assertEqual(self.get('',department='wheels',complete='1',group='family',**self.filters)['items'][0]['tyre_seasons'],['winter'])
        for pid in (self.wrong['selection_id'],self.conflict['selection_id']):
            self.assertIsNone(self.catalog.complete_wheel_price(self.rid,pid,quantity=1))


if __name__ == '__main__':
    unittest.main()
