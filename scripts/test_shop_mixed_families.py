"""Mixed shop design cards keep exact articles and purchasing boundaries.

Only synthetic rows, Flask test requests and a fake browser are used. Network
connections are forbidden; grouping remains a read-only catalogue operation.
"""
from collections import Counter
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_public_api import create_shop_public_blueprint
from test_shop_catalog_public import NOW, IMAGE, PHOTO, wheel, part, verified
from test_shop_catalog_enriched import tyre
from test_shop_catalog_ranking import quote, rim_proof


def pid(row):
    return row.get('selection_id', row.get('id'))


class MixedFamilyChecks(unittest.TestCase):
    def setUp(self):
        self.no_network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.no_network.start()
        self.black18 = wheel('1', name='SYNTHETIC Design Black', diameter='18')
        self.black19 = wheel('2', name='SYNTHETIC Design Black', diameter='19', size='8 × 19 Zoll')
        self.matte = wheel('3', name='SYNTHETIC Design Black', details={'Farbe': 'Black matt'})
        self.silver = wheel('4', name='SYNTHETIC Design Silver', details={'Farbe': 'Silver'})
        self.other_brand = wheel('5', name='SYNTHETIC Design Black', manufacturer_name='OTHER')
        self.steel = wheel('6', name='SYNTHETIC Design Black', category='steel_wheel',
                           selection_id='steel_wheel:' + '6' * 24)
        self.tyres = [tyre('a'), tyre('b')]
        self.parts = [part('c', category='body_part', department='parts'), part('d')]
        self.rows = [self.black18, self.black19, self.matte, self.silver,
                     self.other_brand, self.steel, *self.tyres, *self.parts]
        self.calls = Counter()
        sourced = pid(self.black19)

        def cached(identifier):
            self.calls[identifier] += 1
            return quote() if identifier == sourced else None

        self.catalogue = EnrichedPublicCatalog(self.rows,
            released_ids={pid(r) for r in self.rows}, image_urls={PHOTO: IMAGE},
            document_urls={'REPORT': '/shop/documents/' + 'a' * 64 + '.pdf'},
            verified_fitments={pid(self.black18): verified(self.black18)},
            procurement_ids={sourced}, shipping_ready_ids={sourced},
            standalone_rims={sourced: rim_proof(self.black19)},
            quote_lookup=cached, quote_lookup_authoritative=True, clock=lambda: NOW)

    def tearDown(self):
        self.no_network.stop()

    def test_mixed_grouping_before_pagination_keeps_every_non_rim(self):
        self.assertEqual(self.catalogue.page()['total'], 10)
        items = []
        for number in range(1, 6):
            self.calls.clear()
            result = self.catalogue.page(group='family', page=number, page_size=2)
            self.assertEqual(result['total'], 9)
            self.assertEqual(result['has_more'], number < 5)
            self.assertEqual(self.calls, Counter({pid(row): 1 for row in self.rows}))
            items.extend(result['items'])
        self.assertEqual(len({row['id'] for row in items}), 9)
        self.assertEqual({row['id'] for row in items if row['department'] != 'wheels'},
                         {pid(row) for row in [*self.tyres, *self.parts]})
        self.assertEqual(items[0]['id'], pid(self.black19))
        self.assertEqual(items[0]['family'], {'variant_count': 2, 'diameters': ['18', '19']})
        self.assertTrue(all(row['order_enabled'] is False for row in items))

    def test_finishes_brands_materials_and_filtered_exact_variants_stay_separate(self):
        rims = self.catalogue.page(group='family', department='wheels')
        self.assertEqual(rims['total'], 5)
        self.assertEqual({row['id'] for row in rims['items']},
                         {pid(row) for row in [self.black19, self.matte, self.silver, self.other_brand, self.steel]})
        filtered = self.catalogue.page(group='family', diameter='18')
        self.assertIn(pid(self.black18), {row['id'] for row in filtered['items']})
        self.assertNotIn(pid(self.black19), {row['id'] for row in filtered['items']})
        self.assertEqual(self.catalogue.page(group='family', category='tyre')['total'], 2)
        self.assertEqual(self.catalogue.page(group='family', department='parts')['total'], 1)
        self.assertEqual(self.catalogue.page(group='family', q='Design Black', brand='OTHER')['total'], 1)
        self.assertEqual(self.catalogue.variants(pid(self.black18), diameter='19')['items'][0]['id'], pid(self.black19))

    def test_family_browsing_never_spreads_fitment_price_or_geometry_to_other_variants(self):
        matched = self.catalogue.page(group='family', make='Audi', model='A3', year=2016)
        self.assertEqual([row['id'] for row in matched['items']], [pid(self.black18)])
        self.assertIsNone(matched['items'][0]['price'])
        exact = self.catalogue.get(pid(self.black19))
        self.assertEqual(exact['fitment']['status'], 'not_confirmed')
        self.assertEqual(exact['geometry']['diameter'], '19')
        self.assertEqual(exact['price']['prices_by_quantity']['1']['unit_gross_cent'], 12345)
        self.assertFalse(exact['order_enabled'])
        variants = self.catalogue.variants(pid(self.black18))['items']
        self.assertEqual([row['id'] for row in variants], [pid(self.black19), pid(self.black18)])
        self.assertIsNone(variants[1]['price'])
        self.assertEqual(variants[1]['fitment']['status'], 'reviewed_with_conditions')

    def test_actual_http_contract_accepts_mixed_family_and_exact_variant_drilldown(self):
        app = Flask('mixed-families')
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        app.register_blueprint(create_shop_public_blueprint(catalogue=self.catalogue))
        client = app.test_client()
        response = client.get('/shop/api/v2/products?group=family&page=1&page_size=24')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['total'], 9)
        response = client.get('/shop/api/v2/products/' + pid(self.black19) + '/variants?diameter=18&page_size=24')
        self.assertEqual(response.status_code, 200)
        self.assertEqual([row['id'] for row in response.get_json()['items']], [pid(self.black18)])
        self.assertEqual(client.get('/shop/api/v2/products/' + pid(self.black19)).get_json()['item']['id'], pid(self.black19))

    def test_home_browser_groups_then_drills_to_exact_variant_without_writes(self):
        code = r'''
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(process.argv[1],'utf8'),logic=require(process.argv[1]);
for(const department of ['',undefined,'wheels'])assert.equal(logic.buildQuery({},department,1).get('group'),'family');
for(const department of ['tyres','parts','accessories'])assert.equal(logic.buildQuery({},department,1).has('group'),false);
const A='alloy_wheel:'+'a'.repeat(24),B='alloy_wheel:'+'b'.repeat(24),T='tyre:'+'c'.repeat(24),P='parts:'+'d'.repeat(24);
const future=new Date(Date.now()+600000).toISOString(),past=new Date(Date.now()-1000).toISOString();
function product(id,size,amount){return{id,name:id===T?'SYNTHETIC Tyre':id===P?'SYNTHETIC Oil':'SYNTHETIC Design Black',category:id===T?'tyre':id===P?'oil':'alloy_wheel',department:id===T?'tyres':id===P?'accessories':'wheels',brand:'SYNTHETIC',size,geometry:{diameter:size.includes('19')?'19':'18'},images:[],specifications:{Ausführung:size},purchase_status:'supplier_check_required',price_status:'current',price:{source:'RSU',currency:'EUR',vat_percent:'19.00',stock_reported:20,observed_at:past,expires_at:future,prices_by_quantity:Object.fromEntries([1,2,3,4].map(q=>[q,{quantity:q,unit_gross_cent:amount,items_gross_cent:q*amount}]))},fitment:{status:'not_confirmed',vehicles:[],reports:[]}};}
const a=product(A,'8 × 18 Zoll · ET45 · 5x112',12000),b=product(B,'8 × 19 Zoll · ET35 · 5x112',16000),t=product(T,'225/40 R19 93Y',10000),p=product(P,'1 Liter',2000);
const family={...a,family:{variant_count:2,diameters:['18','19']}};
class Node{constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.listeners={};this.dataset={};this.textContent='';this.hidden=false;this.disabled=false;this.isConnected=true;this.classList={toggle(){}};this.elements={namedItem:()=>null};}append(...items){this.children.push(...items);}replaceChildren(...items){this.children=[];this.append(...items);}addEventListener(key,fn){this.listeners[key]=fn;}setAttribute(key,value){this[key]=value;}showModal(){this.open=true;}close(){this.open=false;}scrollIntoView(){}}
const walk=n=>[n,...n.children.flatMap(walk)],flush=async()=>{await new Promise(r=>setImmediate(r));await new Promise(r=>setImmediate(r));};
async function boot(department='',page='home'){
 const nodes=Object.fromEntries(['close-product','product-dialog','product-detail','shop-announcement','catalog-products','catalog-filters','catalog-status','previous-page','next-page','page-label'].map(id=>[id,new Node()]));
 const body=new Node('body');body.dataset={api:'/shop/api/v2',department,page};const calls=[],saved=[];
 const win={document:{body,visibilityState:'visible',createElement:tag=>new Node(tag),getElementById:id=>nodes[id]||null,querySelector:()=>({content:''}),querySelectorAll:()=>[],addEventListener(){}},localStorage:{getItem:()=>null,setItem:(k,v)=>saved.push(JSON.parse(v))},location:{search:''},setTimeout(){},clearTimeout(){},setInterval(){},fetch:async(url,options)=>{
  calls.push({url,options});assert.equal(options.body,undefined,'no quote/order/payment writes');const u=new URL(url,'https://synthetic.invalid');let value;
  if(u.pathname.endsWith('/variants')){const diameter=u.searchParams.get('diameter');value={items:diameter==='19'?[b]:diameter==='18'?[a]:[a,b],page:1,total:diameter?1:2,has_more:false};}
  else if(u.pathname.endsWith('/products')){value={items:[family,t,p],page:Number(u.searchParams.get('page')),total:30,has_more:u.searchParams.get('page')==='1'};}
  else if(decodeURIComponent(u.pathname).endsWith('/'+B)){value={item:b};}
  else throw new Error('unexpected '+url);
  return{ok:true,text:async()=>JSON.stringify(value)};
 }};
 class FormData{constructor(form){this.values=Object.entries(form.values||{});}[Symbol.iterator](){return this.values[Symbol.iterator]();}}
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date,console,FormData});await flush();return{nodes,calls,saved};
}
(async()=>{
 const f=await boot();assert.equal(new URL(f.calls[0].url,'https://x.invalid').searchParams.get('group'),'family');
 assert.equal(f.nodes['catalog-products'].children.length,3,'non-rim article cards retained');
 const design=f.nodes['catalog-products'].children[0];assert.equal(walk(design).filter(n=>n.tagName==='SELECT').length,0,'family cannot add a guessed size');
 assert.ok(walk(design).some(n=>n.textContent==='Preis der gezeigten Ausführung'));
 await walk(design).find(n=>n.textContent==='Ausführung wählen').listeners.click();await flush();
 assert.ok(f.calls.at(-1).url.includes('/'+encodeURIComponent(A)+'/variants?'));
 let rows=walk(f.nodes['product-detail']).filter(n=>n.className==='shop-family-variant');assert.equal(rows.length,2);
 assert.ok(walk(rows[0]).some(n=>n.textContent.includes('ET45')));assert.ok(walk(rows[1]).some(n=>n.textContent.includes('ET35')));
 assert.ok(walk(rows[0]).some(n=>n.textContent.includes('120,00')));assert.ok(walk(rows[1]).some(n=>n.textContent.includes('160,00')));
 const sizes=walk(f.nodes['product-detail']).find(n=>n.tagName==='SELECT');sizes.value='19';await sizes.listeners.change();await flush();
 assert.equal(new URL(f.calls.at(-1).url,'https://x.invalid').searchParams.get('diameter'),'19');
 rows=walk(f.nodes['product-detail']).filter(n=>n.className==='shop-family-variant');assert.equal(rows.length,1);
 await walk(rows[0]).find(n=>n.textContent==='Diese Felge ansehen').listeners.click();await flush();
 assert.ok(f.calls.some(c=>c.url.includes('/'+encodeURIComponent(B)+'?quantity=1')),'exact SKU detail fetched');
 assert.ok(walk(f.nodes['product-detail']).some(n=>n.textContent===b.size));
 const qty=walk(f.nodes['product-detail']).find(n=>n.tagName==='SELECT');qty.value='2';qty.listeners.change();await flush();
 walk(f.nodes['product-detail']).find(n=>n.textContent==='Nur Felge in den Warenkorb').listeners.click();
 assert.equal(f.saved.at(-1)[0].id,B);assert.equal(f.saved.at(-1)[0].quantity,2);assert.equal(f.calls.some(c=>/cart\/quote|\/orders|checkout|buyer/.test(c.url)),false);
 await f.nodes['next-page'].listeners.click();await flush();assert.equal(new URL(f.calls.at(-1).url,'https://x.invalid').searchParams.get('group'),'family');assert.equal(new URL(f.calls.at(-1).url,'https://x.invalid').searchParams.get('page'),'2');
 f.nodes['catalog-filters'].values={q:'SYNTHETIC',category:'tyre'};await f.nodes['catalog-filters'].listeners.submit({preventDefault(){},currentTarget:f.nodes['catalog-filters']});await flush();
 let query=new URL(f.calls.at(-1).url,'https://x.invalid').searchParams;assert.equal(query.get('q'),'SYNTHETIC');assert.equal(query.get('category'),'tyre');assert.equal(query.get('page'),'1');assert.equal(query.get('group'),'family');
 const complete=await boot('wheels','komplettraeder');query=new URL(complete.calls[0].url,'https://x.invalid').searchParams;assert.equal(query.get('group'),'family');assert.equal(query.get('complete'),'1');
 console.log('Mixed family exact variant flow passed');
})().catch(e=>{console.error(e);process.exit(1)});
'''
        result = subprocess.run(['node', '-e', code, str(ROOT / 'static/shop_v2/shop.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Mixed family exact variant flow passed', result.stdout)


if __name__ == '__main__':
    unittest.main()
