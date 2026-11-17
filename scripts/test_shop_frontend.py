"""Synthetic frontend boundaries; no providers, real addresses or payments."""
from html.parser import HTMLParser
from pathlib import Path
import json
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from flask import Flask
from shop_frontend import create_shop_frontend_blueprint


class Markup(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.tags = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))


class FrontendChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.app = Flask(__name__, template_folder=str(ROOT / 'templates'), static_folder=str(ROOT / 'static'))
        self.app.register_blueprint(create_shop_frontend_blueprint())
        self.client = self.app.test_client()

    def tearDown(self):
        self.network.stop()

    def test_unregistered_factory_and_literal_enable_gate(self):
        self.assertNotIn('SHOP_FRONTEND_ENABLED', self.app.config)
        for enabled in (None, False, 'true', 1):
            self.app.config['SHOP_FRONTEND_ENABLED'] = enabled
            self.assertEqual(self.client.get('/shop/').status_code, 404)
            self.assertEqual(self.client.get('/shop/warenkorb/').status_code, 404)
        self.app.config['SHOP_FRONTEND_ENABLED'] = True
        self.assertEqual(self.client.get('/shop/').status_code, 200)

    def test_all_pages_render_without_catalogue_or_inline_private_feed(self):
        self.app.config['SHOP_FRONTEND_ENABLED'] = True
        for name in ('', 'felgen', 'reifen', 'komplettraeder', 'ersatzteile', 'zubehoer', 'service', 'warenkorb'):
            response = self.client.get('/shop/' + (name + '/' if name else ''))
            self.assertEqual(response.status_code, 200, name)
            source = response.get_data(as_text=True)
            parsed = Markup(source)
            self.assertIn('Gärtner', source)
            self.assertNotIn('prices_by_quantity', source)
            self.assertNotIn('provider_id', source)
            self.assertNotIn('PRIVATE_CANARY', source)
            self.assertFalse(any(tag == 'script' and not attrs.get('src') for tag, attrs in parsed.tags))
            self.assertFalse(any(key.startswith('on') for _, attrs in parsed.tags for key in attrs))
            self.assertIn("script-src 'self'", response.headers['Content-Security-Policy'])
            self.assertEqual(response.headers['Cache-Control'], 'no-store')
            self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
            self.assertIn('noindex, nofollow', source)
            self.assertNotIn('felgenstudio', source)  # unavailable feature must not be linked

    def test_return_paths_do_not_trust_browser_payment_hints(self):
        self.app.config.update(SHOP_FRONTEND_ENABLED=True, SHOP_FRONTEND_INDEXABLE=True)
        identifier = 'sho_' + 'a' * 32
        for kind in ('success', 'cancel'):
            response = self.client.get('/shop/orders/' + identifier + '/' + kind + '?paid=1&session_id=PRIVATE_CANARY')
            self.assertEqual(response.status_code, 200)
            source = response.get_data(as_text=True)
            self.assertIn('data-order-id="' + identifier + '"', source)
            self.assertNotIn('PRIVATE_CANARY', source)
            self.assertNotIn('Zahlung bestätigt.', source)
            self.assertIn('keine Zahlung', source)
            self.assertIn('noindex, nofollow', source)
        self.assertEqual(self.client.get('/shop/orders/anything/success').status_code, 404)
        self.assertEqual(self.client.post('/shop/orders/' + identifier + '/success').status_code, 405)

    def test_checkout_and_buyer_controls_are_explicitly_gated(self):
        self.app.config['SHOP_FRONTEND_ENABLED'] = True
        source = self.client.get('/shop/warenkorb/').get_data(as_text=True)
        self.assertIn('data-checkout-enabled="false"', source)
        self.assertIn('data-order-enabled="false"', source)
        self.assertNotIn('id="buyer-address"', source)
        self.app.config.update(SHOP_BUYER_ENABLED=True, SHOP_CART_QUOTE_ENABLED=True,
                               SHOP_ORDER_DRAFT_ENABLED=True, SHOP_CHECKOUT_ENABLED=True)
        source = self.client.get('/shop/warenkorb/').get_data(as_text=True)
        self.assertIn('id="buyer-address"', source)
        self.assertIn('data-checkout-enabled="true"', source)
        self.assertIn('id="checkout-button"', source)
        self.assertIn('type="button" hidden', source)

    def test_approved_links_only_and_csrf_failure_is_closed(self):
        for link in ('https://external.example', '//external.example', '/a/../admin', '/x\\y', '/x\nfoo'):
            with self.assertRaises(ValueError):
                create_shop_frontend_blueprint(company_url=link)
        with self.assertRaises(ValueError):
            create_shop_frontend_blueprint(feature_links={'unknown': '/x'})
        app = Flask('links', template_folder=str(ROOT / 'templates'), static_folder=str(ROOT / 'static'))
        app.register_blueprint(create_shop_frontend_blueprint(url_prefix='/preview/shop',
            csrf_token=lambda: '"><script>PRIVATE_CANARY</script>',
            feature_links={'studio': '/approved-studio'}))
        app.config['SHOP_FRONTEND_ENABLED'] = True
        source = app.test_client().get('/preview/shop/felgen/').get_data(as_text=True)
        self.assertIn('href="/approved-studio"', source)
        self.assertIn('href="/preview/shop/warenkorb/"', source)
        self.assertNotIn('PRIVATE_CANARY', source)
        self.assertIn('name="shop-csrf" content=""', source)

    def test_only_bounded_marketing_assets_and_same_origin_assets_exist(self):
        self.app.config['SHOP_FRONTEND_ENABLED'] = True
        total = sum(path.stat().st_size for path in (ROOT / 'static/shop_v2').iterdir() if path.is_file())
        self.assertLess(total, 6 * 1024 * 1024)
        for key in ('', 'reifen', 'ersatzteile'):
            parsed = Markup(self.client.get('/shop/' + (key + '/' if key else '')).get_data(as_text=True))
            for tag, attrs in parsed.tags:
                source = attrs.get('src') if tag in ('script', 'img') else attrs.get('href') if tag == 'link' else None
                if source:
                    self.assertTrue(source.startswith('/static/shop_v2/'), source)
                    with self.client.get(source) as asset:
                        self.assertEqual(asset.status_code, 200, source)

    def test_javascript_public_projection_cart_pagination_and_return_status(self):
        code = r'''
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = process.argv[1];
const logic = require(path);
const now = Date.parse('2026-11-17T12:00:00Z');
const id = 'tyre:' + 'a'.repeat(24);
const product = {id, category:'tyre', name:'SYNTHETIC tyre', size:'205/55 R16', images:[], price_status:'current', price:{
 currency:'EUR',vat_percent:'19.00',observed_at:'2026-11-17T11:59:00Z',expires_at:'2026-11-17T12:10:00Z',stock_reported:8,
 prices_by_quantity:{'1':{quantity:1,unit_gross_cent:12345,items_gross_cent:12345},'4':{quantity:4,unit_gross_cent:12000,items_gross_cent:48000}}}};
assert.equal(logic.currentPrice(product,4,now).total,48000);
assert.equal(logic.currentPrice(product,4,Date.parse('2026-11-17T12:10:00Z')),null);
assert.equal(logic.currentPrice({...product,price_status:'update_required'},1,now),null);
assert.equal(logic.currentPrice({...product,price:{...product.price,currency:'USD'}},1,now),null);
assert.equal(logic.currentPrice({...product,price:{...product.price,stock_reported:3}},4,now),null);
assert.equal(logic.currentPrice({...product,price:{...product.price,prices_by_quantity:{'1':{quantity:1,unit_gross_cent:12345,items_gross_cent:1}}}},1,now),null);
const selected=logic.normaliseCart([{id,quantity:4,services:{mounting:true},net_price:'PRIVATE_CANARY',provider_id:'PRIVATE_CANARY',price:1}]);
assert.deepEqual(selected,[{id,quantity:4,services:{mounting:true}}]);
assert.deepEqual(logic.normaliseCart([{id,quantity:5}]),[]);
assert.deepEqual(logic.normaliseCart([{id:[id],quantity:1}]),[]);
assert.deepEqual(logic.normaliseCart([{id,quantity:1,services:{painting:true,paint_color:'<script>'}}]),[]);
assert.deepEqual(logic.normaliseCart([{id,quantity:1,services:{mounting:'true'}}]),[]);
assert.equal(logic.buildQuery({q:'205/55 R16',page_size:'9999',token:'PRIVATE_CANARY',tsn:'bht'},'tyres',2).get('page_size'),'24');
assert.equal(logic.buildQuery({token:'PRIVATE_CANARY'},'tyres',2).has('token'),false);
assert.equal(logic.buildQuery({tsn:'bht'},'wheels',2).get('tsn'),'BHT');
for (const bad of ['https://tracker.example/x.png','//tracker.example/x.png','/a/../x.png','/a//x.png','/x.svg']) assert.equal(logic.safeMedia(bad),false);
assert.equal(logic.safeMedia('/shop/media/product.webp'),true);
assert.equal(logic.safeCheckout('https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC#fid=test'),true);
assert.equal(logic.safeCheckout('https://checkout.stripe.com.evil.example/c/pay/cs_test_SYNTHETIC'),false);
assert.equal(logic.safeCheckout('https://checkout.stripe.com@evil.example/c/pay/cs_test_SYNTHETIC'),false);
class Node {
 constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.dataset={};this.textContent='';this.isConnected=true;this.classList={toggle(){}};this.listeners={};this.hidden=false;this.fields={};this.elements={namedItem:key=>this.fields[key]||(this.fields[key]=new Node('input'))};}
 append(...nodes){this.children.push(...nodes);} replaceChildren(...nodes){this.children=[...nodes];}
 addEventListener(key,fn){this.listeners[key]=fn;} setAttribute(){} replaceWith(){}
}
async function fixture(page,result,search='') {
 const nodes={};
 for(const key of ['close-product','shop-announcement',...(page==='order_status'?['order-status','order-summary']:['catalog-products','catalog-status','previous-page','next-page','page-label','catalog-filters'])])nodes[key]=new Node();
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page,department:'tyres',orderId:'sho_'+'a'.repeat(32)};
 const requests=[];
 const win={document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key]||null,querySelector:()=>({content:''}),querySelectorAll:()=>[]},
  localStorage:{getItem:()=> '[]',setItem(){}},setTimeout(){},setInterval(){},location:{search},
  fetch:async(url,options)=>{requests.push({url,options});return {ok:true,text:async()=>JSON.stringify(result)}}};
 vm.runInNewContext(fs.readFileSync(path,'utf8'),{window:win,URLSearchParams,URL,Intl,Date,console});
 await new Promise(resolve=>setImmediate(resolve));
 return {nodes,requests};
}
async function cartFixture(deferredQuote=false){
 const nodes={};for(const key of ['close-product','shop-announcement','cart-items','cart-summary','cart-status','request-quote','quote-status','quote-review','checkout-button','checkout-note','buyer-address','address-status','delivery-fields'])nodes[key]=new Node();
 nodes['checkout-button'].hidden=true;
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:'warenkorb',buyerEnabled:'true',quoteEnabled:'true',orderEnabled:'true',checkoutEnabled:'true'};
 const requests=[],redirects=[];
 const hash='b'.repeat(64),orderId='sho_'+'a'.repeat(32);
 const quote={ready:true,currency:'eur',quote_hash:hash,total_gross_cent:48000,expires_at:new Date(Date.now()+600000).toISOString(),
  lines:[{id,name:'SYNTHETIC tyre',quantity:4,items_gross_cent:48000,services:[]}],shipments:[{destination:'workshop',gross_cent:0}]};
 let resolveQuote;
 const waitQuote=deferredQuote?new Promise(resolve=>{resolveQuote=resolve}):null;
 const win={document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key]||null,querySelector:()=>({content:''}),querySelectorAll:()=>[]},
  localStorage:{getItem:()=>JSON.stringify([{id,quantity:4,services:{mounting:true}}]),setItem(){}},sessionStorage:{getItem:()=>null,setItem(){}},
  crypto:{randomUUID:()=> 'SYNTHETIC-IDEMPOTENCY'},setTimeout(){},setInterval(){},location:{pathname:'/shop/warenkorb/',assign:url=>redirects.push(url)},
  fetch:async(url,options)=>{
   requests.push({url,options});let value;
   if(url.endsWith('/buyer/session'))value={guest:true,address_ready:true,csrf_token:'SYNTHETIC_TOKEN_12345678901234567890',expires_at:quote.expires_at};
   else if(url.includes('/products/'))value={item:product};
   else if(url.endsWith('/cart/quote')){if(waitQuote)await waitQuote;value={quote,quote_retained:true};}
   else if(url.endsWith('/orders'))value={order:{id:orderId,quote_hash:hash,amount_gross_cent:48000},payment_enabled:true};
   else if(url.endsWith('/checkout'))value={checkout:{order_id:orderId,currency:'eur',amount_gross_cent:48000,checkout_url:'https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC#fid=synthetic',capture_triggered:false,supplier_order_created:false}};
   else throw new Error('Unexpected request '+url);
   return {ok:true,text:async()=>JSON.stringify(value)};
  }};
 vm.runInNewContext(fs.readFileSync(path,'utf8'),{window:win,URLSearchParams,URL,Intl,Date,console});
 await new Promise(resolve=>setImmediate(resolve));
 return {nodes,requests,redirects,resolveQuote};
}
(async()=>{
 const catalog=await fixture('reifen',{items:Array.from({length:24},(_,i)=>({...product,id:'tyre:'+i.toString(16).padStart(24,'0')})),page:1,total:25000,has_more:true});
 assert.equal(catalog.requests.length,1);
 assert.equal(new URL('https://shop.example'+catalog.requests[0].url).searchParams.get('page_size'),'24');
 assert.equal(catalog.nodes['catalog-products'].children.length,24);
 assert.equal(catalog.nodes['next-page'].disabled,false);
 const tooLarge=await fixture('reifen',{items:Array.from({length:25},()=>product),page:1,total:25,has_more:false});
 assert.equal(tooLarge.nodes['catalog-products'].children.length,0);
 const status=await fixture('order_status',{order:{id:'sho_'+'a'.repeat(32),status:'authorized',amount_gross_cent:48000,mode:'test'}},'?paid=1&status=paid');
 assert.equal(status.requests.length,1);
 assert.equal(status.requests[0].options.method,undefined);
 assert.equal(status.requests[0].url.includes('?'),false);
 assert.equal(status.nodes['order-status'].textContent,'Kartenbetrag reserviert. Noch nicht eingezogen.');
 const cart=await cartFixture();
 assert.equal(cart.requests.some(r=>r.options.method==='POST'),false);
 await cart.nodes['request-quote'].listeners.click();
 assert.equal(cart.nodes['checkout-button'].hidden,false);
 assert.equal(cart.requests.filter(r=>r.url.endsWith('/orders')).length,0);
 await cart.nodes['checkout-button'].listeners.click();
 assert.equal(cart.redirects.length,1);
 assert.equal(cart.requests.filter(r=>r.url.endsWith('/orders')).length,1);
 const quoteRequest=cart.requests.find(r=>r.url.endsWith('/cart/quote'));
 assert.deepEqual(JSON.parse(quoteRequest.options.body),{lines:[{id,quantity:4,services:{mounting:true}}]});
 assert.equal(quoteRequest.options.headers['X-CSRF-Token'],'SYNTHETIC_TOKEN_12345678901234567890');
 assert.deepEqual(Object.keys(JSON.parse(cart.requests.find(r=>r.url.endsWith('/orders')).options.body)).sort(),['idempotency_key','quote_hash']);
 assert.equal(cart.requests.find(r=>r.url.endsWith('/checkout')).options.body,'{}');
 const stale=await cartFixture(true);
 const pending=stale.nodes['request-quote'].listeners.click();
 stale.nodes['buyer-address'].listeners.input();
 stale.resolveQuote();await pending;
 assert.equal(stale.nodes['checkout-button'].hidden,true);
 assert.equal(stale.requests.some(r=>r.url.endsWith('/orders')),false);
 console.log('Synthetic frontend boundaries passed');
})().catch(error=>{console.error(error);process.exit(1)});
'''
        result = subprocess.run(['node', '-e', code, str(ROOT / 'static/shop_v2/shop.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Synthetic frontend boundaries passed', result.stdout)

    def test_javascript_bounded_price_polling_quantity_races_and_image_retry(self):
        code = r'''
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync(process.argv[1],'utf8');
const A='tyre:'+'a'.repeat(24),B='tyre:'+'b'.repeat(24);
const base=Date.parse('2026-11-17T12:00:00Z');
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function pending(id=A){return {id,category:'tyre',name:id===A?'SYNTHETIC A':'SYNTHETIC B',images:[],size:'205/55 R16',price_status:'update_required',price:null};}
function priced(id=A,q=1,cents=12345){return {...pending(id),price_status:'current',price:{currency:'EUR',vat_percent:'19.00',observed_at:new Date(base-1000).toISOString(),expires_at:new Date(base+899000).toISOString(),stock_reported:20,prices_by_quantity:{[q]:{quantity:q,unit_gross_cent:cents,items_gross_cent:q*cents}}}};}
function page(item,flag,page=1){const value={items:[item],page,total:2,has_more:page===1};if(flag!==undefined)value.prices_refreshing=flag;return value;}
class Node{
 constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.dataset={};this.listeners={};this.textContent='';this.isConnected=true;this.hidden=false;this.open=false;this.urls=[];this.fields={};this.classList={toggle(){}};this.elements={namedItem:key=>walk(this).find(n=>n.name===key)||this.fields[key]||null};}
 append(...nodes){for(const node of nodes){node.parent=this;this.children.push(node);}}
 replaceChildren(...nodes){for(const n of this.children){n.parent=null;n.isConnected=false;}this.children=[];this.append(...nodes);}
 addEventListener(key,fn){this.listeners[key]=fn;}
 setAttribute(key,value){this[key]=value;}
 replaceWith(node){this.replacement=node;this.isConnected=false;if(this.parent){const i=this.parent.children.indexOf(this);this.parent.children[i]=node;node.parent=this.parent;}}
 closest(tag){for(let n=this.parent;n;n=n.parent)if(n.tagName===tag.toUpperCase())return n;return null;}
 showModal(){this.open=true;}close(){this.open=false;}
 set src(value){this._src=value;this.urls.push(value);}get src(){return this._src;}
}
function walk(node){return [node,...node.children.flatMap(walk)];}
function find(node,cls){return walk(node).find(n=>n.className===cls);}
class Clock{
 constructor(){this.now=base;this.nextId=1;this.jobs=new Map();}
 set(fn,delay){const id=this.nextId++;this.jobs.set(id,{fn,at:this.now+delay,delay});return id;}
 clear(id){this.jobs.delete(id);}
 async next(){const [id,job]=[...this.jobs].sort((a,b)=>a[1].at-b[1].at)[0]||[];if(!job)return false;this.jobs.delete(id);this.now=job.at;job.fn();await flush();return true;}
 async drain(max=30){let count=0;while(this.jobs.size&&count++<max)await this.next();assert.ok(count<=max,'timer bound exceeded');}
}
async function boot(handler,{mode='reifen',cart=[],visibility='visible'}={}){
 const clock=new Clock(),nodes={};
 const keys=['close-product','product-dialog','product-detail','shop-announcement',...(mode==='warenkorb'?['cart-items','cart-summary','cart-status','request-quote','quote-status','quote-review','checkout-button','checkout-note']:['catalog-products','catalog-status','previous-page','next-page','page-label','catalog-filters'])];
 for(const key of keys)nodes[key]=new Node();
 nodes['product-dialog'].tagName='DIALOG';nodes['product-dialog'].append(nodes['product-detail']);
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:mode,department:'tyres',quoteEnabled:'false',buyerEnabled:'false'};
 const calls=[],stored=[];
 const events={};
 const document={body,visibilityState:visibility,createElement:tag=>new Node(tag),getElementById:id=>nodes[id]||null,querySelector:()=>({content:''}),querySelectorAll:()=>[],addEventListener:(event,fn)=>{events[event]=fn;}};
 const win={document,localStorage:{getItem:()=>JSON.stringify(cart),setItem:(key,value)=>stored.push(value)},setTimeout:(fn,delay)=>clock.set(fn,delay),clearTimeout:id=>clock.clear(id),setInterval(){},location:{search:'',pathname:'/shop/warenkorb/'},
  fetch:async(url,options)=>{calls.push({url,options});const result=await handler(url,calls.length);return {ok:true,text:async()=>JSON.stringify(result)};}};
 class FakeDate extends Date{constructor(...args){super(...(args.length?args:[clock.now]));}static now(){return clock.now;}}
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date:FakeDate,Math:Object.assign(Object.create(Math),{random:()=>0}),console});
 await flush();return {clock,nodes,calls,stored,document,events};
}
(async()=>{
 // Missing/false warmup flags do not create background GET timers.
 for(const flag of [undefined,false]){const f=await boot(()=>page(pending(),flag));assert.equal(f.clock.jobs.size,0);assert.equal(f.calls.length,1);}
 // A warmup response updates the existing card/price node and preserves controls.
 let n=0;const f=await boot(()=>++n===1?page(pending(),true):page(priced(),false));
 const card=f.nodes['catalog-products'].children[0],price=find(card,'shop-price'),select=find(card,'shop-quantity');
 assert.equal(f.clock.jobs.size,1);await f.clock.next();
 assert.equal(f.nodes['catalog-products'].children[0],card);assert.equal(find(card,'shop-quantity'),select);assert.equal(select.value,'1');
 assert.equal(price.children[0].textContent,'123,45 €');assert.equal(f.clock.jobs.size,0);assert.equal(f.calls.length,2);
 // Exhausted cache/budget remains visibly pending after exactly eight retries.
 const bounded=await boot(()=>page(pending(),true));await bounded.clock.drain();assert.equal(bounded.calls.length,9);assert.equal(bounded.clock.jobs.size,0);
 bounded.document.visibilityState='hidden';bounded.events.visibilitychange();bounded.document.visibilityState='visible';bounded.events.visibilitychange();await bounded.clock.drain();assert.equal(bounded.calls.length,9);
 // Navigating while an old poll is in flight cannot alter the new page.
 let oldResolve;let firstPoll=true;
 const changed=await boot(url=>{const q=new URL('https://shop.test'+url).searchParams;if(q.get('page')==='2')return page(priced(B),false,2);if(firstPoll){firstPoll=false;return page(pending(),true);}return new Promise(resolve=>{oldResolve=resolve;});});
 await changed.clock.next();changed.nodes['next-page'].listeners.click();await flush();
 const pageTwo=changed.nodes['catalog-products'].children[0];oldResolve(page(priced(A,1,99999),true));await flush();
 assert.equal(changed.nodes['catalog-products'].children[0],pageTwo);assert.equal(find(pageTwo,'shop-price').children[0].textContent,'123,45 €');assert.equal(changed.nodes['page-label'].textContent,'Seite 2');
 // Old q1 responses cannot erase a later q2 price or reset its selector.
 let q1Resolve;
 const quantity=await boot(url=>{const u=new URL('https://shop.test'+url);if(u.pathname.endsWith('/products'))return page(priced(),false);if(u.searchParams.get('quantity')==='1')return new Promise(resolve=>{q1Resolve=resolve;});return {item:priced(A,2,2222),prices_refreshing:false};});
 const quantityCard=quantity.nodes['catalog-products'].children[0],quantitySelect=find(quantityCard,'shop-quantity'),quantityPrice=find(quantityCard,'shop-price');
 quantitySelect.value='1';quantitySelect.listeners.change();quantitySelect.value='2';quantitySelect.listeners.change();await flush();
 assert.equal(quantityPrice.children[0].textContent,'22,22 €');q1Resolve({item:priced(A,1,99999),prices_refreshing:true});await flush();
 assert.equal(quantitySelect.value,'2');assert.equal(quantityPrice.children[0].textContent,'22,22 €');assert.equal(quantity.clock.jobs.size,0);
 // Exact quantity polling is independently capped and always includes quantity=3.
 const tiers=await boot(url=>url.includes('/products?')?page(priced(),false):{item:pending(),prices_refreshing:true});
 const tierSelect=find(tiers.nodes['catalog-products'].children[0],'shop-quantity');tierSelect.value='3';tierSelect.listeners.change();await flush();await tiers.clock.drain();
 assert.equal(tiers.calls.length,10);assert.ok(tiers.calls.slice(1).every(r=>new URL('https://shop.test'+r.url).searchParams.get('quantity')==='3'));assert.equal(tierSelect.value,'3');
 // Price replies preserve an open detail form, selected quantity and service checkbox.
 let detailReads=0;
 const detail=await boot(url=>{if(url.includes('/products?'))return page(priced(),false);const q=Number(new URL('https://shop.test'+url).searchParams.get('quantity'));detailReads++;return {item:detailReads<3?pending():priced(A,q,3333),prices_refreshing:detailReads<3};});
 const detailCard=detail.nodes['catalog-products'].children[0];find(detailCard,'shop-detail-link').listeners.click();await flush();
 const form=find(detail.nodes['product-detail'],'shop-options'),mount=walk(form).find(n=>n.name==='mounting'),detailSelect=find(form,'shop-quantity');
 mount.checked=true;detailSelect.value='3';detailSelect.listeners.change();await flush();await detail.clock.next();
 assert.equal(find(detail.nodes['product-detail'],'shop-options'),form);assert.equal(mount.checked,true);assert.equal(detailSelect.value,'3');assert.equal(find(detail.nodes['product-detail'],'shop-price').children[0].textContent,'33,33 €');
 // Closing the dialog suppresses its pending timer rather than fetching hidden details.
 const closed=await boot(url=>url.includes('/products?')?page(priced(),false):{item:pending(),prices_refreshing:true});find(closed.nodes['catalog-products'].children[0],'shop-detail-link').listeners.click();await flush();closed.nodes['close-product'].listeners.click();await closed.clock.drain();assert.equal(closed.calls.length,2);
 // Cart price polling updates its existing estimate, not the line controls.
 let cartReads=0;const cart=await boot(()=>({item:++cartReads===1?pending():priced(A,2,4444),prices_refreshing:cartReads===1}),{mode:'warenkorb',cart:[{id:A,quantity:2,services:{}}]});
 const cartLine=cart.nodes['cart-items'].children[0],cartSelect=find(cartLine,'shop-quantity'),cartTotal=find(cart.nodes['cart-summary'],'shop-summary-total');await cart.clock.next();
 assert.equal(cart.nodes['cart-items'].children[0],cartLine);assert.equal(find(cartLine,'shop-quantity'),cartSelect);assert.equal(cartSelect.value,'2');assert.equal(cartTotal.textContent,'88,88 €');assert.equal(cart.calls.length,2);
 // Hidden documents do not warm/poll prices.
 let hiddenReads=0;const hidden=await boot(()=>++hiddenReads===1?page(pending(),true):page(priced(),false),{visibility:'hidden'});assert.equal(hidden.clock.jobs.size,0);
 hidden.document.visibilityState='visible';hidden.events.visibilitychange();await hidden.clock.next();assert.equal(hidden.calls.length,2);assert.equal(find(hidden.nodes['catalog-products'].children[0],'shop-price').children[0].textContent,'123,45 €');
 // A hidden-tab timer pauses rather than losing work; resume retains its budget.
 let pausedReads=0;const paused=await boot(()=>++pausedReads===1?page(pending(),true):page(priced(),false));
 paused.document.visibilityState='hidden';await paused.clock.next();assert.equal(paused.calls.length,1);assert.equal(paused.clock.jobs.size,0);
 paused.document.visibilityState='visible';paused.events.visibilitychange();await paused.clock.next();assert.equal(paused.calls.length,2);assert.equal(paused.clock.jobs.size,0);
 let pausedTierReads=0;const pausedTier=await boot(url=>{if(url.includes('/products?'))return page(priced(),false);return {item:++pausedTierReads===1?pending():priced(A,3,5555),prices_refreshing:pausedTierReads===1};});
 const pausedTierSelect=find(pausedTier.nodes['catalog-products'].children[0],'shop-quantity');pausedTierSelect.value='3';pausedTierSelect.listeners.change();await flush();
 pausedTier.document.visibilityState='hidden';await pausedTier.clock.next();assert.equal(pausedTier.calls.length,2);
 pausedTier.document.visibilityState='visible';pausedTier.events.visibilitychange();await pausedTier.clock.next();assert.equal(pausedTier.calls.length,3);assert.equal(pausedTierSelect.value,'3');assert.equal(find(pausedTier.nodes['catalog-products'].children[0],'shop-price').children[0].textContent,'55,55 €');
 // Five image retries with bounded jitter/backoff, then a clear fallback.
 const images=await boot(()=>page({...priced(),images:['/shop/media/synthetic.webp']},false));const image=walk(images.nodes['catalog-products']).find(n=>n.tagName==='IMG');
 for(let retry=0;retry<5;retry++){image.onerror();assert.equal(images.clock.jobs.size,1);const job=[...images.clock.jobs.values()][0];assert.ok(job.delay>=400&&job.delay<=5300);await images.clock.next();}
 image.onerror();assert.equal(image.urls.length,6);assert.equal(images.clock.jobs.size,0);assert.equal(image.replacement.textContent,'Produktbild gerade nicht verfügbar');
 for(const fixture of [f,bounded,changed,quantity,tiers,detail,closed,cart,hidden,paused,pausedTier,images])assert.ok(fixture.calls.every(call=>call.options.method===undefined&&call.url.startsWith('/shop/api/v2/products')),'read-only product calls only');
 console.log('Bounded price/image polling passed');
})().catch(error=>{console.error(error);process.exit(1)});
'''
        result = subprocess.run(['node', '-e', code, str(ROOT / 'static/shop_v2/shop.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Bounded price/image polling passed', result.stdout)


if __name__ == '__main__':
    unittest.main()
