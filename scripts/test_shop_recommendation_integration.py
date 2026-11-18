"""Real enriched catalogue/API comparison with synthetic approved inputs."""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT/'scripts'))
from flask import Flask
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_catalog_public import CatalogError, variant_fingerprint
from shop_public_api import create_shop_public_blueprint
from test_shop_catalog_public import NOW, PHOTO, IMAGE, wheel
from test_shop_catalog_enriched import tyre, pair


def offer(number=1, unit=1000, *, size=('225', '40', '18'), stock=12, season='Sommerreifen', model=None):
    row = tyre()
    row.update(selection_id='tyre:'+f'{number:024x}', name=model or f'Synthetic Model {number}',
               manufacturer_name='Synthetic Brand', width=size[0], ratio=size[1], diameter=size[2],
               size=f'{size[0]}/{size[1]} R{size[2]} 92Y', load_index='92', speed_index='Y', season=season)
    observed = NOW-timedelta(days=1)
    row['quote_public'] = {'kind':'merchant_pricebook','source':'RSU','currency':'EUR','vat_percent':'19.00',
        'observed_at':observed.isoformat(),'expires_at':(observed+timedelta(days=7)).isoformat(),
        'review_at':(observed+timedelta(days=5)).isoformat(),'stock_reported':stock,
        'stock_observed_at':observed.isoformat(),'stock_expires_at':(observed+timedelta(minutes=15)).isoformat(),
        'prices_by_quantity':{str(q):{'quantity':q,'unit_gross_cent':unit,'items_gross_cent':q*unit,
                                    'currency':'EUR','vat_percent':'19.00'} for q in range(1,5)}}
    return row


class RecommendationIntegrationChecks(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket,'socket',side_effect=AssertionError('No network'))
        guard.start();self.addCleanup(guard.stop)
        self.rim=wheel()

    def catalogue(self, tyres, *, enabled=True, paired=None, excluded=(), unbound=(), verified=None, missing_docs=()):
        pairs=[pair(self.rim,row) for row in tyres if paired is None or row['selection_id'] in paired]
        documents, paths={},{}
        for n,row in enumerate(tyres,1):
            label='/shop/documents/'+f'{n:064x}'+'.jpg'
            sheet='/shop/documents/'+f'{n+10000:064x}'+'.pdf'
            documents[row['selection_id']]={'identity_hash':variant_fingerprint(row),
                'eprel_id':str(100000+n),'tyre_label_url':label,'tyre_sheet_url':sheet}
            if row['selection_id'] in missing_docs:
                documents[row['selection_id']].pop('tyre_sheet_url')
            paths['label'+str(n)]=label;paths['sheet'+str(n)]=sheet
        return EnrichedPublicCatalog([self.rim,*tyres],recommendation_ladder=enabled,
            recommendation_document_ids=verified,
            released_ids=[self.rim['selection_id'],*[row['selection_id'] for row in tyres]],
            image_urls={PHOTO:IMAGE},document_urls=paths,product_documents=documents,
            compatible_pairs={self.rim['selection_id']:pairs},allow_published_prices=True,
            procurement_ids={row['selection_id'] for row in tyres}-set(unbound),
            checkout_excluded_ids=set(excluded),rsu_only_prices=True,clock=lambda:NOW)

    def result(self,catalogue,**kwargs):
        return catalogue.compatible_tyres(self.rim['selection_id'],quantity=kwargs.pop('quantity',4),
                                          season=kwargs.pop('season','summer'),**kwargs)

    def api(self,catalogue,warmup=None):
        app=Flask(__name__)
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED']=True
        app.register_blueprint(create_shop_public_blueprint(catalogue=catalogue,catalogue_warmup=warmup))
        return app.test_client()

    def test_default_flag_preserves_original_order_response_and_shape(self):
        rows=[offer(1,9000,model='A expensive'),offer(2,1000,model='Z affordable')]
        result=self.result(self.catalogue(rows,enabled=False))
        self.assertEqual([p['id'] for p in result['items']],[row['selection_id'] for row in rows])
        self.assertNotIn('available_sizes',result);self.assertNotIn('selected_size',result)
        for invalid in (None,1,'true'):
            with self.subTest(invalid=invalid),self.assertRaises(CatalogError):self.catalogue(rows,enabled=invalid)

    def test_same_size_ladder_exposes_all_size_options_without_mixing(self):
        rows=[offer(n,n*1000) for n in range(1,10)]
        other=[offer(n,500,size=('235','45','18')) for n in (10,11)]
        catalogue=self.catalogue([*rows,*other]);result=self.result(catalogue)
        self.assertEqual(result['available_sizes'],['225/40 R18','235/45 R18'])
        self.assertEqual(result['selected_size'],'225/40 R18')
        self.assertEqual([p['id'] for p in result['items']],[offer(n)['selection_id'] for n in (1,3,5,7,9)])
        self.assertTrue(all(p['geometry']['width']=='225' and p['geometry']['ratio']=='40' for p in result['items']))
        self.assertEqual(result['scope'],'rim_width_candidate')
        self.assertTrue(all(p['fitment']['status']=='not_confirmed' and p['order_enabled'] is False for p in result['items']))
        chosen=self.result(catalogue,tyre_size='235/45 R18')
        self.assertEqual(chosen['selected_size'],'235/45 R18')
        self.assertEqual({p['id'] for p in chosen['items']},{row['selection_id'] for row in other})

    def test_model_execution_variants_do_not_fill_the_five_comparison_slots(self):
        rows=[offer(1,2000,model='Touring Model'),offer(2,1000,model='touring model'),
              offer(3,3000),offer(4,4000),offer(5,5000)]
        rows[1]['load_index']='94';rows[1]['speed_index']='W'
        result=self.result(self.catalogue(rows))
        self.assertEqual(len(result['items']),4)
        self.assertNotIn(rows[0]['selection_id'],[p['id'] for p in result['items']])
        self.assertEqual(result['items'][0]['geometry']['load_index'],'94')
        self.assertEqual(result['items'][0]['geometry']['speed_index'],'W')

    def test_quantity_stock_season_price_expiry_and_known_bad_documents_stay_closed(self):
        good,short,expired,no_quantity,winter,bad_document=[offer(n,n*1000) for n in range(1,7)]
        short['quote_public']['stock_reported']=2
        expired['quote_public'].update(observed_at=(NOW-timedelta(days=8)).isoformat(),
            expires_at=(NOW-timedelta(days=1)).isoformat(),review_at=(NOW-timedelta(days=3)).isoformat(),
            stock_observed_at=(NOW-timedelta(days=8)).isoformat(),stock_expires_at=(NOW-timedelta(days=8)+timedelta(minutes=15)).isoformat())
        no_quantity['quote_public']['prices_by_quantity'].pop('4')
        winter['season']='Winterreifen'
        catalogue=self.catalogue([good,short,expired,no_quantity,winter,bad_document],excluded={bad_document['selection_id']})
        self.assertEqual([p['id'] for p in self.result(catalogue)['items']],[good['selection_id']])
        self.assertEqual({p['id'] for p in self.result(catalogue,quantity=2)['items']},
            {row['selection_id'] for row in (good,short,no_quantity)})
        self.assertEqual([p['id'] for p in self.result(catalogue,season='winter')['items']],[winter['selection_id']])
        self.assertIsNotNone(catalogue.get(bad_document['selection_id'])['price'])

    def test_price_or_source_membership_does_not_create_pair_evidence_or_pad_results(self):
        documented,unreviewed,unbound=[offer(n,n*1000) for n in range(1,4)]
        catalogue=self.catalogue([documented,unreviewed,unbound],
            paired={documented['selection_id'],unbound['selection_id']},unbound={unbound['selection_id']})
        self.assertEqual([p['id'] for p in self.result(catalogue)['items']],[documented['selection_id']])
        self.assertEqual(catalogue.page(department='tyres')['total'],3)
        self.assertIsNone(catalogue.get(unbound['selection_id'])['price'])
        none=self.catalogue([unbound],unbound={unbound['selection_id']})
        self.assertEqual(self.result(none)['items'],[])

    def test_explicit_unavailable_size_does_not_silently_fallback(self):
        catalogue=self.catalogue([offer()])
        result=self.result(catalogue,tyre_size='205/45 R18')
        self.assertEqual(result['items'],[])
        self.assertEqual(result['selected_size'],'205/45 R18')
        self.assertEqual(result['available_sizes'],['225/40 R18'])
        for size in ('225/40R18','225/40 R31','225/40 R18\n',True):
            with self.subTest(size=size),self.assertRaises(CatalogError):self.result(catalogue,tyre_size=size)

    def test_actual_document_cache_membership_and_both_public_documents_are_required(self):
        good,not_cached,missing_sheet=[offer(n,n*1000) for n in range(1,4)]
        rows=[good,not_cached,missing_sheet]
        catalogue=self.catalogue(rows,verified={good['selection_id'],missing_sheet['selection_id']},
                                 missing_docs={missing_sheet['selection_id']})
        self.assertEqual([p['id'] for p in self.result(catalogue)['items']],[good['selection_id']])
        self.assertIsNotNone(catalogue.get(not_cached['selection_id'])['price'])
        self.assertEqual(self.result(self.catalogue(rows,verified=set()))['items'],[])
        for verified in ([],True,{'tyre:'+'f'*24}):
            with self.subTest(verified=verified),self.assertRaises(CatalogError):self.catalogue(rows,verified=verified)

    def test_public_api_accepts_size_and_rejects_bad_duplicate_unknown_parameters(self):
        rows=[offer(1,1000),offer(2,2000,size=('235','45','18'))]
        catalogue=self.catalogue(rows);client=self.api(catalogue)
        path='/shop/api/v2/products/'+self.rim['selection_id']+'/compatible-tyres'
        response=client.get(path,query_string={'quantity':'4','season':'summer','tyre_size':'235/45 R18'})
        self.assertEqual(response.status_code,200)
        value=response.get_json();self.assertEqual(value['selected_size'],'235/45 R18')
        self.assertEqual([p['id'] for p in value['items']],[rows[1]['selection_id']])
        self.assertFalse(value['prices_refreshing'])
        for query in ('tyre_size=225%2F40R18','tyre_size=225%2F40+R18&tyre_size=235%2F45+R18',
                      'tyre_size='+('x'*161),'arbitrary_size=225%2F40+R18'):
            with self.subTest(query=query):self.assertEqual(client.get(path+'?'+query).status_code,400)
        self.assertEqual(client.post('/shop/api/v2/orders',json={}).status_code,404)

    def test_read_only_api_keeps_quantities_immutable_and_never_calls_financial_services(self):
        rows=[offer(1,1000),offer(2,2000)];before=deepcopy(rows)
        catalogue=self.catalogue(rows);warmup=Mock(return_value=False);client=self.api(catalogue,warmup)
        response=client.get('/shop/api/v2/products/'+self.rim['selection_id']+'/compatible-tyres',
                            query_string={'quantity':'3','season':'summer'})
        self.assertEqual(response.status_code,200)
        value=response.get_json();self.assertEqual(rows,before)
        self.assertEqual(value['items'][0]['price']['prices_by_quantity']['3']['items_gross_cent'],3000)
        self.assertTrue(all(p['order_enabled'] is False for p in value['items']))
        self.assertTrue(all(p['product_documents'].get('tyre_label_url') and p['product_documents'].get('tyre_sheet_url') for p in value['items']))
        self.assertTrue(warmup.called)
        self.assertEqual(warmup.call_args.kwargs.get('quantities'),(3,))

    def test_exact_size_controls_radio_mounting_and_late_responses_in_real_frontend(self):
        result=subprocess.run(['node',str(ROOT/'scripts/test_shop_recommendation_ui.js'),
                               str(ROOT/'static/shop_v2/shop.js')],capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('Exact-size tyre selection UI checks passed',result.stdout)


if __name__=='__main__':unittest.main(verbosity=2)
