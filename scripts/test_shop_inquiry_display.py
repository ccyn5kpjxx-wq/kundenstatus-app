"""Visible retained retail amounts cannot grant procurement or stock authority."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_catalog_public import CatalogError, variant_fingerprint
from test_shop_catalog_public import NOW, PHOTO, IMAGE, part, wheel


def snapshot(row):
    observed=NOW-timedelta(hours=1)
    return {'identity_hash':variant_fingerprint(row),'price':{
        'source':'RSU','kind':'merchant_pricebook','currency':'EUR','vat_percent':'19.00',
        'observed_at':observed.isoformat(),'expires_at':(observed+timedelta(days=7)).isoformat(),
        'review_at':(observed+timedelta(days=5)).isoformat(),'stock_observed_at':observed.isoformat(),
        'stock_expires_at':(observed+timedelta(minutes=15)).isoformat(),'stock_reported':2,
        'prices_by_quantity':{str(q):{'quantity':q,'unit_gross_cent':1369,'items_gross_cent':1369*q,
                                     'vat_percent':'19.00'} for q in range(1,21)}}}


class InquiryDisplayChecks(unittest.TestCase):
    def setUp(self):
        guard=patch.object(socket,'socket',side_effect=AssertionError('No provider network'))
        guard.start();self.addCleanup(guard.stop)
        self.row=part(category='body_part',department='parts',name='SYNTHETIC panel',price=None)
        self.pid=self.row['id'];self.value=snapshot(self.row)

    def catalog(self,rows=None,**changes):
        rows=rows or [self.row]
        kwargs=dict(released_ids=[r.get('selection_id',r.get('id')) for r in rows],
            image_urls={PHOTO:IMAGE},rsu_only_prices=True,clock=lambda:NOW)
        kwargs.update(changes)
        return EnrichedPublicCatalog(rows,**kwargs)

    def test_retained_amount_is_visible_without_cache_supplier_or_purchasing_authority(self):
        cache=Mock(side_effect=AssertionError('Unbound parts must not call purchase cache'))
        before=deepcopy(self.value)
        catalog=self.catalog(inquiry_prices={self.pid:self.value},quote_lookup=cache,quote_lookup_authoritative=True)
        product=catalog.get(self.pid)
        self.assertEqual(product['price']['prices_by_quantity']['1']['unit_gross_cent'],1369)
        self.assertEqual(product['purchase_status'],'inquiry_only')
        self.assertFalse(product['order_enabled']);self.assertFalse(product['price']['stock_current'])
        self.assertEqual(product['price_label'],'')
        self.assertEqual(product['fitment']['status'],'not_confirmed')
        self.assertEqual(catalog._procurement_ids,frozenset());cache.assert_not_called()
        self.assertNotIn('identity_hash',json.dumps(product))
        product['price']['prices_by_quantity']['1']['unit_gross_cent']=1
        self.value['price']['stock_reported']=99
        self.assertEqual(catalog.get(self.pid)['price']['stock_reported'],2)
        self.assertEqual(catalog.get(self.pid)['price']['prices_by_quantity']['1']['unit_gross_cent'],1369)
        self.assertEqual(before['price']['stock_reported'],2)

    def test_default_unbound_imports_and_caches_still_cannot_expose_market_prices(self):
        cache=Mock(side_effect=AssertionError('No supplier access'))
        product=self.catalog(quote_lookup=cache,quote_lookup_authoritative=True).get(self.pid)
        self.assertIsNone(product['price']);self.assertEqual(product['price_status'],'not_offered')
        cache.assert_not_called()

    def test_wrong_identity_market_source_unknown_products_and_purchase_overlap_are_rejected(self):
        for change in [{'identity_hash':'f'*64},{'extra_permission':True},
                       {'price':{**self.value['price'],'source':'ALZURA'}},
                       {'price':{**self.value['price'],'currency':'USD'}},
                       {'price':{**self.value['price'],'review_at':NOW.isoformat()}}]:
            value={**deepcopy(self.value),**change}
            with self.subTest(change=change),self.assertRaises(CatalogError):self.catalog(inquiry_prices={self.pid:value})
        with self.assertRaises(CatalogError):self.catalog(inquiry_prices={'parts:'+'a'*24:self.value})
        with self.assertRaises(CatalogError):self.catalog(procurement_ids={self.pid},inquiry_prices={self.pid:self.value})
        accessory=part(department='accessories')
        with self.assertRaises(CatalogError):self.catalog([accessory],inquiry_prices={accessory['id']:snapshot(accessory)})

    def test_expiry_hides_price_without_relabelling_old_stock_or_arming_an_order(self):
        catalog=self.catalog(inquiry_prices={self.pid:self.value})
        expired=catalog.get(self.pid,now=NOW+timedelta(days=8))
        self.assertIsNone(expired['price']);self.assertEqual(expired['purchase_status'],'inquiry_only')
        self.assertFalse(expired['order_enabled'])

    def test_purchasable_goods_then_priced_inquiries_then_unpriced_inquiries(self):
        rim=wheel(quote_public=None);unknown=part('e',category='body_part',department='parts',name='AAA unknown',price=None)
        quote=deepcopy(wheel()['quote_public']);quote['source']='RSU'
        cache=Mock(side_effect=lambda pid:quote if pid==rim['selection_id'] else (_ for _ in ()).throw(AssertionError('No parts cache')))
        catalog=self.catalog([unknown,self.row,rim],procurement_ids={rim['selection_id']},quote_lookup=cache,
            quote_lookup_authoritative=True,inquiry_prices={self.pid:self.value})
        self.assertEqual([p['id'] for p in catalog.page()['items']],[rim['selection_id'],self.pid,unknown['id']])
        self.assertTrue(all(call.args==(rim['selection_id'],) for call in cache.call_args_list))


if __name__=='__main__':unittest.main()
