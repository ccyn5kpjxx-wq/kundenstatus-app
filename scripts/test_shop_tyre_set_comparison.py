"""Exercise the public four-tyre comparison with an isolated shop and no suppliers."""
from datetime import timedelta
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask
from shop_catalog_public import variant_fingerprint
from shop_public_api import create_shop_public_blueprint
from shop_tyre_tests import TyreTest
from shop_freight import MainlandDeliveryEligibility, RSURetailFreight
from shop_production import _service_quote
from test_shop_catalog_enriched import catalogue, tyre, NOW
from test_shop_cart_quotes import Fixture, TYRE, line
from shop_tyre_pricing import tyre_unit_gross_cent


class TyreSetComparisonChecks(unittest.TestCase):
    path = '/shop/api/v2/tyre-set-comparison'
    search = {'width': '205', 'ratio': '55', 'diameter': '18', 'season': 'summer'}

    def setUp(self):
        self.rows, self.book, self.docs, self.shipping, self.ratings = [], {}, {}, {}, {}
        self.excluded=set()
        self.images = {'label': '/shop/documents/'+'1'*64+'.png', 'sheet': '/shop/documents/'+'2'*64+'.pdf'}

    def add(self, index, cents, *, wet='C', grade=None, stock=12, q1=1000, pickup=0, size='205/55 R18 91V'):
        pid='tyre:'+format(index,'024x')
        row={**tyre(), 'selection_id':pid, 'name':f'Synthetic {index:04d}', 'size':size,
             'quality_facts':{'wet_grip':wet,'fuel_efficiency':'C','external_noise_db':70},
             'identifiers':{'ean':format(index,'013d')}}
        observed=NOW-timedelta(hours=1)
        self.rows.append(row)
        self.book[pid]={'kind':'merchant_pricebook','source':'RSU','stock_reported':stock,'observed_at':observed.isoformat(),
                        'expires_at':(observed+timedelta(days=7)).isoformat(),'review_at':(observed+timedelta(days=1)).isoformat(),
                        'stock_observed_at':observed.isoformat(),'stock_expires_at':(observed+timedelta(minutes=15)).isoformat(),
                        'prices_by_quantity':{str(q):{'quantity':q,'vat_percent':'19.00','unit_gross_cent':unit,
                            'items_gross_cent':q*unit} for q,unit in [(1,q1),(4,cents)]}}
        self.docs[pid]={'identity_hash':variant_fingerprint(row),'tyre_label_url':self.images['label'],
                        'tyre_sheet_url':self.images['sheet'],'eprel_id':'12345'}
        self.shipping[pid]=pickup
        if grade is not None:
            self.ratings[row['identifiers']['ean']]=TyreTest(variant_fingerprint(row),('C',wet,'70'),
                2025,grade,'sommerreifen/205-55-r18/synthetic-test/')
        return pid

    def client(self, enabled=True, **overrides):
        with patch.dict('shop_catalog_public.ADAC_TYRE_TESTS',self.ratings,clear=True):
            cat=catalogue(self.rows,**{
                'procurement_ids':set(self.book),'shipping_ready_ids':set(self.shipping),'checkout_excluded_ids':self.excluded,
                'document_urls':self.images,'product_documents':self.docs,'quote_lookup':self.book.get,
                'allow_published_prices':True,'quote_lookup_authoritative':True,'rsu_only_prices':True,
                'tyre_payment_costs':True,'tyre_pickup_freight':lambda pid,qty:self.shipping.get(pid) if qty==4 else None,
                **overrides})
        app=Flask('tyre-compare')
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED']=enabled
        def forbidden(*args,**kwargs):
            raise AssertionError('No supplier lookup, checkout, or reservation in the comparison')
        app.register_blueprint(create_shop_public_blueprint(catalogue=cat,catalogue_warmup=forbidden,authorize_quote=forbidden))
        return app.test_client()

    def compare(self, **query):
        response=self.client().get(self.path,query_string={**self.search,**query})
        self.assertEqual(response.status_code,200,response.data)
        return response.json

    def test_global_winners_and_four_tier_match_each_selected_service_total(self):
        for i in range(40):
            self.add(i,10000+i)
        cheapest=self.add(40,1800,wet='E',q1=50000)
        wet=self.add(41,3000,wet='A',q1=100)
        tested=self.add(42,3500,grade=1.7,wet='B')
        goods_only=self.compare()
        self.assertEqual(goods_only['eligible_count'],43)
        self.assertEqual([o['product']['id'] for o in goods_only['items']],[cheapest,wet,tested])
        self.assertEqual([o['criteria'] for o in goods_only['items']],[['lowest_price'],['best_wet_grip'],['best_adac_grade']])
        self.assertEqual(goods_only['items'][2]['product']['test_rating']['grade'],1.7)
        for free, mounted in zip(goods_only['items'],self.compare(montage='1')['items']):
            unit=tyre_unit_gross_cent(self.book[free['product']['id']]['prices_by_quantity']['4']['unit_gross_cent'])
            self.assertEqual(free['goods_gross_cent'],4*unit)
            self.assertEqual(free['total_gross_cent'],free['goods_gross_cent'])
            self.assertIsNone(free['freight_gross_cent'])
            self.assertEqual(free['mounted_total_gross_cent'],mounted['total_gross_cent'])
            self.assertEqual((mounted['mounting_gross_cent'],mounted['freight_gross_cent'],mounted['total_gross_cent']),
                             (10000,0,4*unit+10000))

    def test_excludes_unorderable_and_mismatched_rows_and_merges_joint_winner(self):
        good=self.add(1,9100,wet='A',grade=2.0)
        self.add(2,100,stock=3)
        self.book[self.add(3,100)]['prices_by_quantity'].pop('4')
        self.docs.pop(self.add(4,100))
        self.shipping.pop(self.add(5,100))
        self.excluded.add(self.add(6,100))
        self.add(7,100,size='225/55 R18 91V')
        self.add(8,100,pickup=None)
        stale=self.add(9,100)
        observed=NOW-timedelta(days=2)
        self.book[stale].update(observed_at=observed.isoformat(),review_at=(observed+timedelta(days=1)).isoformat(),
                                expires_at=(observed+timedelta(days=7)).isoformat())
        result=self.compare()
        self.assertEqual(result['eligible_count'],1)
        self.assertEqual([o['product']['id'] for o in result['items']],[good])
        self.assertEqual(result['items'][0]['criteria'],['lowest_price','best_wet_grip','best_adac_grade'])
        self.assertEqual(self.compare(season='winter')['items'],[])

    def test_ties_and_unproven_test_or_wet_class_never_grant_an_unearned_badge(self):
        a=self.add(1,5000,wet='A');b=self.add(2,5100,wet='A',grade=1.0)
        self.add(3,4000,wet='F')
        self.rows[1]['name']='Changed variant'
        self.docs[b]['identity_hash']=variant_fingerprint(self.rows[1])
        offers=self.compare()['items']
        self.assertEqual([o['criteria'] for o in offers],[['lowest_price'],['best_wet_grip']])
        self.assertEqual(offers[1]['product']['id'],a)
        self.assertNotIn('test_rating',offers[1]['product'])

    def test_mounting_price_can_change_the_winner_and_unsupported_tariff_stays_empty(self):
        dearer=self.add(1,5000,pickup=5000);cheaper=self.add(2,5100,pickup=0)
        self.assertEqual(self.compare()['items'][0]['product']['id'],dearer)
        self.assertEqual(self.compare(montage='1')['items'][0]['product']['id'],cheaper)
        row=self.rows[0];row.update(diameter='22',size='205/55 R22 91V')
        self.docs[dearer]['identity_hash']=variant_fingerprint(row)
        self.assertIsNone(self.compare(diameter='22')['items'][0]['mounted_total_gross_cent'])
        self.assertEqual(self.compare(diameter='22',montage='1')['items'],[])

    def test_last_daily_observation_is_distinguished_from_fresh_and_too_old_stock(self):
        for i, age in enumerate([timedelta(minutes=5),timedelta(hours=23),timedelta(hours=24)]):
            pid = self.add(i,1000+i,wet=['C','A','B'][i])
            seen = NOW-age
            # A historic 5-day price remains valid even when the stock is too old.
            self.book[pid].update(observed_at=seen.isoformat(),review_at=(seen+timedelta(days=5)).isoformat(),
                expires_at=(seen+timedelta(days=7)).isoformat(),stock_observed_at=seen.isoformat(),
                stock_expires_at=(seen+timedelta(minutes=15)).isoformat())
        result=self.compare()
        self.assertEqual(result['eligible_count'],2)
        self.assertEqual([o['stock_current'] for o in result['items']],[True,False])
        self.assertEqual([o['stock_observed_at'] for o in result['items']],
                         [(NOW-timedelta(minutes=5)).isoformat(),(NOW-timedelta(hours=23)).isoformat()])

    def test_mounted_offer_matches_gross_sum_in_original_cart_quote(self):
        pid = self.add(1,5821,q1=6923)
        cart = Fixture()
        cart.products[pid] = {**cart.products[TYRE],'id':pid,'fitment_required':False}
        freight = RSURetailFreight({pid:'pkw_tyre'},mode='test',basis='net',evidence_id='fixture',
            checked_at=NOW-timedelta(days=1),destination_eligibility=MainlandDeliveryEligibility(),clock=lambda:NOW)
        def tariff(code,product,quantity,options):
            return {**_service_quote(code,product,quantity,options),**cart.current()}
        def retail(product,quantity):
            cents=self.book[pid]['prices_by_quantity'][str(quantity)]['unit_gross_cent']
            return {**cart.retail(product,quantity),'unit_gross_cent':cents}
        actual=cart.quote([line(pid,4,services={'mounting':True})],service_quote=tariff,retail_quote=retail,
                          freight_quote=freight,tyre_payment_costs=True).public()
        self.assertTrue(actual['ready'],actual['blocked_reasons'])
        comparison=self.client(tyre_pickup_freight=freight.tyre_collection_gross_cent)
        selected=comparison.get(self.path,query_string={**self.search,'montage':'1'}).json['items'][0]
        self.assertEqual(selected['total_gross_cent'],actual['total_gross_cent'])
        self.assertEqual(selected['total_gross_cent'],4*tyre_unit_gross_cent(5821)+10000)

    def test_read_only_boundary_and_exact_inputs(self):
        self.add(1,1000)
        client=self.client()
        self.assertEqual(self.client(False).get(self.path,query_string=self.search).status_code,404)
        self.assertEqual(client.post(self.path,query_string=self.search).status_code,405)
        for change in [{'width':None},{'width':'0255'},{'diameter':'22.5'},{'season':''},{'season':'all'},
                       {'montage':'0'},{'quantity':'1'},{'vehicle':'xxx'},{'items_gross_cent':'0'}]:
            with self.subTest(change=change):
                self.assertEqual(client.get(self.path,query_string={**self.search,**change}).status_code,400)
        params=list(self.search.items())+[('width','225')]
        self.assertEqual(client.get(self.path,query_string=params).status_code,400)


if __name__ == '__main__':
    unittest.main()
