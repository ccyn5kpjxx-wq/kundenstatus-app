"""Gross-only display adapters with synthetic readers; never provider IO."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_alzura_display import AlzuraDisplayError, AlzuraDisplayQuoteProvider, PATHS, provider_factory
from shop_catalog_public import variant_fingerprint
from shop_price_cache import PublicPriceCache
from shop_partner_native import alzura_readonly_catalog as native_api
from shop_partner_native.shop_live_quotes import QuoteService
from shop_partner_native.shop_parts_provider import PartsQuoteService

NOW = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
WHEEL = 'alloy_wheel:' + 'a' * 24
PART = 'parts:' + 'b' * 24
ROWS = [{'selection_id': WHEEL, 'category':'alloy_wheel','name':'Synthetic rim','manufacturer_name':'Synthetic'},
        {'selection_id': PART, 'category':'oil','name':'Synthetic oil','manufacturer_name':'Synthetic'}]
PRODUCTS = {row['selection_id']: {'id':row['selection_id'],'category':row['category'],
            'identity_hash':variant_fingerprint(row)} for row in ROWS}


def wheel_quote():
    return {'source':'ALZURA','stock_reported':20,'observed_at':NOW.isoformat(),
        'expires_at':(NOW+timedelta(minutes=15)).isoformat(),
        'prices_by_quantity': {str(q):{'quantity':q,'unit_gross_cent':10000-q*10,
            'items_gross_cent':q*(10000-q*10),'vat_percent':'19.00'} for q in range(1,5)},
        'supplier_buy_price':'PRIVATE_CANARY','access_token':'PRIVATE_CANARY'}


def parts_quote():
    return {'stock':10,'unit_gross_cent':2000,'observed_at':NOW.isoformat(),
        'expires_at':(NOW+timedelta(minutes=15)).isoformat(),
        'base_price_unit':'L','base_price_gross_cent':400,'supplier_net':'PRIVATE_CANARY'}


class DisplayTests(unittest.TestCase):
    def setUp(self):
        self.calls=[]
        self.wheel=wheel_quote()
        self.part=parts_quote()
        self.now=NOW
        self.network=patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden'))
        self.network.start()

    def tearDown(self):
        self.network.stop()

    def reader(self, ids):
        self.calls.append(ids)
        return {'quotes':{pid: self.wheel if pid.startswith('alloy_wheel:') else self.part for pid in ids}}

    def provider(self, **changes):
        config=dict(approved_ids={WHEEL,PART},mode='live',enabled=True,reads_approved=True,
            wheel_quotes=self.reader,parts_quotes=self.reader,clock=lambda:self.now,
            unit1_parts_contract_confirmed=True)
        config.update(changes)
        return AlzuraDisplayQuoteProvider(ROWS,**config)

    def test_unarmed_or_unreleased_or_changed_identity_makes_no_reads(self):
        for changes in ({'enabled':False},{'reads_approved':False}):
            self.assertIsNone(self.provider(**changes).retail_quote(PRODUCTS[WHEEL],1))
        provider=self.provider()
        for product,qty in (({**PRODUCTS[WHEEL],'identity_hash':'f'*64},1),
                            ({**PRODUCTS[WHEEL],'category':'tyre'},1),
                            ({**PRODUCTS[WHEEL],'id':'alloy_wheel:'+'f'*24},1),
                            (PRODUCTS[WHEEL],True),(PRODUCTS[WHEEL],5)):
            self.assertIsNone(provider.retail_quote(product,qty))
        self.assertEqual(self.calls,[])

    def test_exact_quantity_same_offer_public_projection_and_no_procurement_claim(self):
        provider=self.provider()
        for qty in (1,2,3,4):
            quote=provider.retail_quote(PRODUCTS[WHEEL],qty)
            self.assertEqual(quote['unit_gross_cent'],10000-qty*10)
            self.assertEqual(quote['quantity'],qty)
            self.assertEqual(quote['identity_hash'],PRODUCTS[WHEEL]['identity_hash'])
            self.assertTrue(quote['display_only'])
            self.assertFalse(quote['procurement_confirmed'])
            self.assertNotIn('PRIVATE_CANARY',json.dumps(quote))
        self.assertEqual(self.calls,[[WHEEL]]*4)

    def test_parts_unit_one_only_and_base_price_preserved(self):
        provider=self.provider()
        quote=provider.retail_quote(PRODUCTS[PART],1)
        self.assertEqual(quote['base_price_gross_cent'],400)
        self.assertEqual(quote['base_price_unit'],'L')
        self.assertNotIn('PRIVATE_CANARY',json.dumps(quote))
        self.assertIsNone(provider.retail_quote(PRODUCTS[PART],2))
        self.assertIsNone(self.provider(unit1_parts_contract_confirmed=False).retail_quote(PRODUCTS[PART],1))
        self.assertEqual(self.calls,[[PART]])

    def test_fresh_actual_clock_and_invalid_supplier_public_fields_fail_closed(self):
        provider=self.provider()
        for mutation in (
            lambda q:q.update(source='OTHER'),
            lambda q:q.update(stock_reported=3),
            lambda q:q.update(observed_at=(NOW+timedelta(seconds=1)).isoformat()),
            lambda q:q.update(expires_at=(NOW+timedelta(minutes=16)).isoformat()),
            lambda q:q['prices_by_quantity']['4'].update(items_gross_cent=1),
            lambda q:q['prices_by_quantity']['4'].update(vat_percent='7.00'),
            lambda q:q['prices_by_quantity']['4'].update(unit_gross_cent=True),
        ):
            self.wheel=wheel_quote()
            mutation(self.wheel)
            self.assertIsNone(provider.retail_quote(PRODUCTS[WHEEL],4))
        self.wheel=wheel_quote()
        self.now=NOW+timedelta(minutes=15)
        self.assertIsNone(provider.retail_quote(PRODUCTS[WHEEL],1))
        self.now=NOW
        def slow(ids):
            self.now=NOW+timedelta(minutes=15)
            return {'quotes':{WHEEL:wheel_quote()}}
        self.assertIsNone(self.provider(wheel_quotes=slow).retail_quote(PRODUCTS[WHEEL],1))

    def test_review_quarantine_and_native_id_alias_are_explicit(self):
        rows=deepcopy(ROWS)
        rows[0]['price_status']='review_pending'
        provider=AlzuraDisplayQuoteProvider(rows,approved_ids={WHEEL,PART},mode='live',
            enabled=True,reads_approved=True,wheel_quotes=self.reader,clock=lambda:NOW)
        self.assertIsNone(provider.retail_quote(PRODUCTS[WHEEL],1))
        alias='alloy_wheel:'+'c'*24
        provider=self.provider(native_ids={WHEEL:alias})
        self.assertIsNotNone(provider.retail_quote(PRODUCTS[WHEEL],2))
        self.assertEqual(self.calls,[[alias]])
        with self.assertRaises(AlzuraDisplayError):
            self.provider(native_ids={WHEEL:'tyre:'+'c'*24})

    def test_factory_has_no_token_catalog_reads_threads_or_network_and_explicit_paths(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            config={key:key+'.json' for key in PATHS}
            config.update(mode='live',enabled=False,reads_approved=False)
            bundle={'products':ROWS,'publication':{'released_ids':[WHEEL,PART]}}
            with patch.object(native_api,'load_tokens',side_effect=AssertionError('token read forbidden')), \
                 patch.object(QuoteService,'_load_mapping',side_effect=AssertionError('catalogue read forbidden')), \
                 patch.object(PartsQuoteService,'_load',side_effect=AssertionError('catalogue read forbidden')), \
                 patch.object(Path,'read_text',side_effect=AssertionError('file read forbidden')), \
                 patch('threading.Thread',side_effect=AssertionError('thread forbidden')):
                provider=provider_factory(bundle,config,root)
                self.assertIsNone(provider.retail_quote(PRODUCTS[WHEEL],1))
            self.assertEqual(list(root.iterdir()),[])
            for key,value in (('token_file','../outside.json'),('wheel_snapshot','public/catalog.json')):
                with self.subTest(key=key),self.assertRaisesRegex(AlzuraDisplayError,'explicit_private_display_configuration_required'):
                    provider_factory(bundle,{**config,key:value},root)
            with self.assertRaises(AlzuraDisplayError):
                provider_factory(bundle,{'mode':'live'},root)

    def test_shared_native_ledger_call_ceiling_is_enforced_with_no_provider_calls(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'ledger.json'
            # Independent service instances must share the persisted monthly
            # count, including calls already spent before this process starts.
            attempts=[{'at':(NOW-timedelta(seconds=3*(9900-i))).isoformat(),
                'area':'tyres','operation':'search','run_id':str(i)} for i in range(9899)]
            path.write_text(json.dumps({'schema_version':1,'attempts':attempts}))
            wheel=native_api.BudgetLedger(path,monthly_limit=9900,run_limit=8)
            parts=native_api.BudgetLedger(path,monthly_limit=9900,run_limit=8)
            wheel.reserve('tyres','search','new-wheel',NOW)
            with self.assertRaisesRegex(native_api.ReadError,'local_monthly_budget_exhausted'):
                parts.reserve('products','items','new-part',NOW+timedelta(seconds=3))

    def test_display_cache_consumes_gross_quotes_without_approving_procurement(self):
        cache=PublicPriceCache(PRODUCTS,approved_ids={WHEEL,PART},provider=self.provider(),
            source='ALZURA',require_procurement=False,mode='live',enabled=True,reads_approved=True,
            max_provider_reads_per_period=3,spacing_seconds=0.1,
            clock=lambda:self.now,monotonic=lambda:(self.now-NOW).total_seconds())
        try:
            self.assertTrue(cache.start(background=False))
            self.assertTrue(cache.warmup([WHEEL],quantities=(2,)))
            self.assertEqual(self.calls,[])
            self.assertTrue(cache.run_once())
            wheel=cache.get(WHEEL)
            self.assertEqual(wheel['source'],'ALZURA')
            self.assertEqual(wheel['prices_by_quantity']['2']['unit_gross_cent'],9980)
            self.now+=timedelta(seconds=1)
            self.assertTrue(cache.warmup([PART],quantities=(1,)))
            self.assertTrue(cache.run_once())
            part=cache.get(PART)
            self.assertEqual(part['base_price_gross_cent'],400)
            for private in ('PRIVATE_CANARY','procurement_confirmed','identity_hash','supplier_net'):
                self.assertNotIn(private,json.dumps([wheel,part]))
        finally:cache.stop()


if __name__=='__main__':
    unittest.main()
