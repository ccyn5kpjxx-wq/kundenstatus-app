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
sys.path.insert(0, str(Path(__file__).resolve().parent))
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
        for name in ('', 'felgen', 'reifen', 'komplettraeder', 'ersatzteile', 'zubehoer', 'service', 'warenkorb', 'felgenstudio'):
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
            self.assertIn('data-studio-url="/shop/felgenstudio/"', source)

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
        self.assertIn('data-consumer-enabled="false"', source)
        self.assertNotIn('id="buyer-address"', source)
        self.app.config.update(SHOP_BUYER_ENABLED=True, SHOP_CART_QUOTE_ENABLED=True,
                               SHOP_ORDER_DRAFT_ENABLED=True, SHOP_CHECKOUT_ENABLED=True, SHOP_CONSUMER_ENABLED=True)
        source = self.client.get('/shop/warenkorb/').get_data(as_text=True)
        self.assertIn('id="buyer-address"', source)
        selects = [attrs for tag, attrs in Markup(source).tags if tag == 'select']
        self.assertEqual(selects, [{'name': 'delivery_salutation', 'autocomplete': 'off'}])
        options = [attrs for tag, attrs in Markup(source).tags if tag == 'option']
        self.assertEqual([attrs.get('value') for attrs in options], ['', 'Herr', 'Frau', 'Firma'])
        self.assertFalse(any('selected' in attrs for attrs in options))
        self.assertIn('data-checkout-enabled="true"', source)
        self.assertIn('data-consumer-enabled="true"', source)
        self.assertIn('id="checkout-button"', source)
        self.assertIn('type="button" hidden', source)
        self.assertLess(source.index('</aside>'), source.index('id="quote-review"'))

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
        self.assertLess(total, 8 * 1024 * 1024)
        for key in ('', 'reifen', 'ersatzteile', 'felgenstudio'):
            parsed = Markup(self.client.get('/shop/' + (key + '/' if key else '')).get_data(as_text=True))
            for tag, attrs in parsed.tags:
                source = attrs.get('src') if tag in ('script', 'img') else attrs.get('href') if tag == 'link' else None
                if source:
                    self.assertTrue(source.startswith('/static/shop_v2/'), source)
                    with self.client.get(source) as asset:
                        self.assertEqual(asset.status_code, 200, source)

    def test_studio_and_parts_keep_approved_flow_without_embedded_products(self):
        self.app.config['SHOP_FRONTEND_ENABLED'] = True
        studio = self.client.get('/shop/felgenstudio/')
        source = studio.get_data(as_text=True)
        for identifier in ('ws-canvas', 'ws-enlarge', 'ws-photo-input', 'ws-size', 'ws-variant', 'ws-data', 'ws-previous', 'ws-next'):
            self.assertIn('id="' + identifier + '"', source)
        self.assertIn('"products": []', source)
        self.assertIn('wird nicht hochgeladen', source)
        self.assertIn("img-src 'self' blob:", studio.headers['Content-Security-Policy'])
        self.assertNotIn('action=', source)
        parts = self.client.get('/shop/ersatzteile/').get_data(as_text=True)
        self.assertIn('Automarke', parts)
        self.assertIn('name="keys"', parts)
        self.assertLess(parts.index('Automarke'), parts.index('Produktsuche'))

    def test_frontend_queries_match_real_public_enriched_api_contract(self):
        from shop_catalog_enriched import EnrichedPublicCatalog
        from shop_public_api import create_shop_public_blueprint
        from test_shop_catalog_public import wheel, part, PHOTO, NOW, variant_fingerprint
        rim = wheel(name='SYNTHETIC Rim Black')
        smaller = wheel('b', name='SYNTHETIC Rim Black', size='7.5 × 17 Zoll', diameter='17')
        tyre = wheel('c', selection_id='tyre:' + 'c' * 24, category='tyre',
                     name='SYNTHETIC Tyre', size='225/40 R18 92Y', season='Sommerreifen')
        body = part(category='body_part', department='parts', name='SYNTHETIC Fender')
        face = {'reviewed': True, 'width': 800, 'height': 800, 'cx': 400, 'cy': 400,
                'rx': 350, 'ry': 350, 'rotation': 0, 'hub': {'x': 400, 'y': 400}}
        evidence = {
            rim['selection_id']: {'variant_fingerprint': variant_fingerprint(rim), 'photo': PHOTO, 'photo_face': face},
            body['id']: {'variant_fingerprint': variant_fingerprint(body), 'provider_reviewed': True,
                         'provider_vehicles': [{'make': 'Audi', 'model': 'A3', 'year_from': 2016, 'year_to': 2017,
                                               'hsn': '0588', 'tsn': 'BHT', 'label': 'Audi A3 synthetic variant'}]},
        }
        catalogue = EnrichedPublicCatalog([rim, smaller, tyre, body],
            released_ids={rim['selection_id'], smaller['selection_id'], tyre['selection_id'], body['id']},
            image_urls={PHOTO: '/shop/media/synthetic.webp'}, browsing_evidence=evidence,
            compatible_pairs={rim['selection_id']: [{'reviewed': True, 'tyre_id': tyre['selection_id'],
                'rim_identity': variant_fingerprint(rim), 'tyre_identity': variant_fingerprint(tyre)}]}, clock=lambda: NOW)
        app = Flask('synthetic-enriched-boundary')
        app.register_blueprint(create_shop_public_blueprint(catalogue=catalogue))
        app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        client = app.test_client()
        groups = client.get('/shop/api/v2/products?department=wheels&group=family&page_size=24').get_json()
        self.assertEqual(groups['total'], 1)
        self.assertEqual(groups['items'][0]['family']['variant_count'], 2)
        variants = client.get('/shop/api/v2/products/' + rim['selection_id'] + '/variants?page=1&page_size=24&diameter=17')
        self.assertEqual(variants.status_code, 200)
        self.assertEqual([row['id'] for row in variants.get_json()['items']], [smaller['selection_id']])
        studio = client.get('/shop/api/v2/products?department=wheels&studio=1&page_size=24').get_json()
        self.assertEqual([row['id'] for row in studio['items']], [rim['selection_id']])
        recommendations = client.get('/shop/api/v2/products/' + rim['selection_id'] + '/compatible-tyres?quantity=4&season=summer').get_json()
        self.assertEqual(recommendations['scope'], 'rim_width_candidate')
        self.assertEqual([row['id'] for row in recommendations['items']], [tyre['selection_id']])
        matched = client.get('/shop/api/v2/products?department=parts&make=Audi&model=A3&year=2016').get_json()
        self.assertEqual([row['id'] for row in matched['items']], [body['id']])
        self.assertEqual(matched['items'][0]['fitment']['status'], 'provider_vehicle')
        self.assertNotIn('PRIVATE_CANARY', json.dumps([groups, variants.get_json(), studio, recommendations, matched]))

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
const rimId='alloy_wheel:'+'b'.repeat(24),group='set|'+rimId+'|'+id,pair=[{id:rimId,quantity:4,services:{},assembly_group:group},{id,quantity:4,services:{mounting:true,runflat:true},assembly_group:group}];
assert.deepEqual(logic.removeCartLine(pair,0),[{id,quantity:4,services:{mounting:true,runflat:true}}]);
assert.deepEqual(logic.removeCartLine(pair,1),[{id:rimId,quantity:4,services:{}}]);assert.equal(pair[0].assembly_group,group);assert.equal(pair[1].quantity,4);
assert.equal(logic.removeCartLine([...pair,{id,quantity:4,services:{mounting:true,runflat:true}}],0),null);
assert.deepEqual(logic.normaliseCart([{id,quantity:5}]),[]);
assert.deepEqual(logic.normaliseCart([{id:[id],quantity:1}]),[]);
assert.deepEqual(logic.normaliseCart([{id,quantity:1,services:{painting:true,paint_color:'<script>'}}]),[]);
assert.deepEqual(logic.normaliseCart([{id,quantity:1,services:{mounting:'true'}}]),[]);
assert.equal(logic.buildQuery({q:'205/55 R16',page_size:'9999',token:'PRIVATE_CANARY',tsn:'bht'},'tyres',2).get('page_size'),'24');
assert.equal(logic.buildQuery({token:'PRIVATE_CANARY'},'tyres',2).has('token'),false);
assert.equal(logic.buildQuery({tsn:'bht'},'wheels',2).get('tsn'),'BHT');
assert.deepEqual(logic.vehicleQuery({make:' Audi ',model:'',year:''}),{make:'Audi'});
assert.deepEqual(logic.vehicleQuery({keys:'0588 / bht'}),{hsn:'0588',tsn:'BHT'});
assert.deepEqual(logic.vehicleQuery({keys:'0588BHT'}),{hsn:'0588',tsn:'BHT'});
assert.equal(logic.vehicleQuery({make:'',model:'A3'}),null);
assert.equal(logic.vehicleQuery({make:'Audi',year:'2016'}),null);
assert.equal(logic.buildQuery({group:'family'},'wheels',1).get('group'),'family');
for (const bad of ['https://tracker.example/x.png','//tracker.example/x.png','/a/../x.png','/a//x.png','/x.svg']) assert.equal(logic.safeMedia(bad),false);
assert.equal(logic.safeMedia('/shop/media/product.webp'),true);
assert.equal(logic.inquiryOnly({purchase_status:'inquiry_only'}),true);assert.equal(logic.inquiryOnly({purchase_status:'supplier_check_required'}),false);assert.equal(logic.inquiryOnly({price_label:'Richtpreis'}),false);
assert.equal(logic.safeCheckout('https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC#fid=test'),true);
assert.equal(logic.safeCheckout('https://checkout.stripe.com/g/pay/cs_live_SYNTHETIC#fid=test'),true);
assert.equal(logic.safeCheckout('https://checkout.stripe.com/g/pay/cs_test_SYNTHETIC'),true);
assert.equal(logic.safeCheckout('https://checkout.stripe.com/g/pay/cs_live_SYNTHETIC/extra'),false);
assert.equal(logic.safeCheckout('https://checkout.stripe.com.evil.example/g/pay/cs_live_SYNTHETIC'),false);
assert.equal(logic.safeCheckout('http://checkout.stripe.com/g/pay/cs_live_SYNTHETIC'),false);
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
async function cartFixture(deferredQuote=false,consumerEnabled=true,invalidTerms=false,mixed=false){
 const nodes={};for(const key of ['close-product','shop-announcement','cart-items','cart-summary','cart-status','request-quote','quote-status','quote-review','checkout-button','checkout-note','buyer-address','address-status','delivery-fields'])nodes[key]=new Node();
 nodes['checkout-button'].hidden=true;
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:'warenkorb',buyerEnabled:'true',quoteEnabled:'true',orderEnabled:'true',checkoutEnabled:'true',consumerEnabled:String(consumerEnabled),cartRequestUrl:'/anfrage?anliegen=allgemein&shop_anfrage=1'};
 const requests=[],redirects=[],drafts=[];
 const hash='b'.repeat(64),orderId='sho_'+'a'.repeat(32);
 const quote={ready:true,currency:'eur',quote_hash:hash,total_gross_cent:48000,expires_at:new Date(Date.now()+600000).toISOString(),
  lines:[{id,name:'SYNTHETIC tyre',quantity:4,items_gross_cent:48000,services:[]}],shipments:[{destination:'workshop',gross_cent:0}]};
 let resolveQuote;
 const waitQuote=deferredQuote?new Promise(resolve=>{resolveQuote=resolve}):null;
 const win={document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key]||null,querySelector:()=>({content:''}),querySelectorAll:()=>[]},
  localStorage:{getItem:()=>JSON.stringify([{id,quantity:4,services:{mounting:true}}]),setItem(){}},sessionStorage:{getItem:()=>null,setItem:(key,value)=>drafts.push({key,value})},
  crypto:{randomUUID:()=> 'SYNTHETIC-IDEMPOTENCY'},setTimeout(){},setInterval(){},location:{pathname:'/shop/warenkorb/',assign:url=>redirects.push(url)},
  fetch:async(url,options)=>{
   requests.push({url,options});let value;
   if(url.endsWith('/buyer/session'))value={guest:true,address_ready:true,csrf_token:'SYNTHETIC_TOKEN_12345678901234567890',expires_at:quote.expires_at};
   else if(url.endsWith('/buyer/address'))value={guest:true,address_ready:true,csrf_token:'SYNTHETIC_TOKEN_12345678901234567890',expires_at:quote.expires_at};
   else if(url.includes('/products/'))value={item:product};
   else if(url.endsWith('/cart/quote')){if(waitQuote)await waitQuote;value={quote:mixed?{...quote,ready:false,total_gross_cent:null,known_subtotal_gross_cent:48000,blocked_reasons:[{code:'mixed_cart_manual_review_required'}]}:quote,quote_retained:!mixed};}
   else if(url.endsWith('/consumer/terms'))value={version:'SYNTHETIC 1',pack_hash:'c'.repeat(64),consumer_only:true,texts:{agb:'SYNTHETIC AGB',widerruf:'SYNTHETIC Widerruf',widerrufsformular:'SYNTHETIC Formular',lieferung:'SYNTHETIC Lieferung',datenschutz:invalidTerms?'':'SYNTHETIC Datenschutz'}};
   else if(url.endsWith('/consumer/acceptance'))value={accepted:true,quote_hash:hash,pack_hash:'c'.repeat(64),accepted_at:new Date().toISOString()};
   else if(url.endsWith('/orders'))value={order:{id:orderId,quote_hash:hash,amount_gross_cent:48000},payment_enabled:true};
   else if(url.endsWith('/checkout'))value={checkout:{order_id:orderId,currency:'eur',amount_gross_cent:48000,checkout_url:'https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC#fid=synthetic',capture_triggered:false,supplier_order_created:false}};
   else throw new Error('Unexpected request '+url);
   return {ok:true,text:async()=>JSON.stringify(value)};
  }};
 class SyntheticFormData{constructor(form){this.form=form;}get(key){const field=this.form.fields[key];return key==='different_delivery'?(field?.checked?'on':null):(field?.value??null);}}
 vm.runInNewContext(fs.readFileSync(path,'utf8'),{window:win,URLSearchParams,URL,Intl,Date,console,FormData:SyntheticFormData});
 await new Promise(resolve=>setImmediate(resolve));
 return {nodes,requests,redirects,drafts,resolveQuote};
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
 assert.equal(cart.nodes['checkout-button'].disabled,true);
 assert.equal(cart.requests.filter(r=>r.url.endsWith('/orders')).length,0);
 await cart.nodes['checkout-button'].listeners.click();assert.equal(cart.requests.some(r=>r.url.endsWith('/consumer/acceptance')||r.url.endsWith('/orders')),false);
 const walk=node=>[node,...node.children.flatMap(walk)];
 const accept=walk(cart.nodes['quote-review']).find(n=>n.name==='terms_accepted'),consumer=walk(cart.nodes['quote-review']).find(n=>n.name==='consumer');assert.equal(accept.checked,false);assert.equal(consumer.checked,false);
 const reviewedNodes=walk(cart.nodes['quote-review']),warranty=reviewedNodes.find(n=>n.tagName==='IMG'&&n.src==='/static/recht/reifen-gewaehrleistung-de.jpg');assert.ok(warranty);assert.ok(warranty.alt.includes('mindestens zwei Jahre'));assert.ok(reviewedNodes.indexOf(warranty)<reviewedNodes.indexOf(accept));
 accept.checked=true;accept.listeners.change();assert.equal(cart.nodes['checkout-button'].disabled,true);consumer.checked=true;consumer.listeners.change();assert.equal(cart.nodes['checkout-button'].disabled,false);
 await cart.nodes['checkout-button'].listeners.click();
 assert.equal(cart.redirects.length,1);
 assert.equal(cart.requests.filter(r=>r.url.endsWith('/orders')).length,1);
 const quoteRequest=cart.requests.find(r=>r.url.endsWith('/cart/quote'));
 assert.deepEqual(JSON.parse(quoteRequest.options.body),{lines:[{id,quantity:4,services:{mounting:true}}]});
 assert.equal(quoteRequest.options.headers['X-CSRF-Token'],'SYNTHETIC_TOKEN_12345678901234567890');
 assert.deepEqual(Object.keys(JSON.parse(cart.requests.find(r=>r.url.endsWith('/orders')).options.body)).sort(),['idempotency_key','quote_hash']);
 assert.equal(cart.requests.find(r=>r.url.endsWith('/checkout')).options.body,'{}');
 const acceptance=cart.requests.find(r=>r.url.endsWith('/consumer/acceptance'));assert.deepEqual(JSON.parse(acceptance.options.body),{quote_hash:'b'.repeat(64),pack_hash:'c'.repeat(64),terms_accepted:true,consumer:true});assert.ok(cart.requests.indexOf(acceptance)<cart.requests.findIndex(r=>r.url.endsWith('/orders')));
 const disabled=await cartFixture(false,false);await disabled.nodes['request-quote'].listeners.click();assert.equal(disabled.nodes['checkout-button'].hidden,true);await disabled.nodes['checkout-button'].listeners.click();assert.equal(disabled.requests.some(r=>r.url.includes('/consumer/')||r.url.endsWith('/orders')),false);
 const badTerms=await cartFixture(false,true,true);await badTerms.nodes['request-quote'].listeners.click();assert.equal(badTerms.nodes['checkout-button'].hidden,true);assert.equal(badTerms.requests.some(r=>r.url.endsWith('/orders')),false);
 const manual=await cartFixture(false,true,false,true);await manual.nodes['request-quote'].listeners.click();assert.equal(manual.nodes['checkout-button'].hidden,true);assert.equal(manual.requests.some(r=>r.url.includes('/consumer/')||r.url.endsWith('/orders')),false);
 assert.ok(walk(manual.nodes['quote-review']).some(n=>n.textContent==='Bekannte Zwischensumme · noch kein Gesamtangebot'));assert.ok(walk(manual.nodes['quote-review']).some(n=>n.textContent==='480,00 €'));
 const offer=walk(manual.nodes['quote-review']).find(n=>n.tagName==='BUTTON'&&n.textContent==='Gemischten Warenkorb als Gesamtangebot anfragen');assert.ok(offer);offer.listeners.click();assert.deepEqual(manual.redirects,['/anfrage?anliegen=allgemein&shop_anfrage=1']);const draft=JSON.parse(manual.drafts.find(d=>d.key==='gaertner-shop-offer-request-v1').value);assert.ok(draft.message.includes('4 × SYNTHETIC tyre'));assert.equal(JSON.stringify(draft).includes(id),false);assert.equal(manual.requests.some(r=>r.url.endsWith('/orders')),false);
 const stale=await cartFixture(true);
 const pending=stale.nodes['request-quote'].listeners.click();
 stale.nodes['buyer-address'].listeners.input();
 stale.resolveQuote();await pending;
 assert.equal(stale.nodes['checkout-button'].hidden,true);
 assert.equal(stale.requests.some(r=>r.url.endsWith('/orders')),false);
 // Delivery salutation is explicit, optional and never inferred from a name.
 async function address(choice,different=false){const f=await cartFixture(),form=f.nodes['buyer-address'];for(const [key,value] of Object.entries({name:'Synthetic Person',email:'synthetic@example.invalid',recipient:'Synthetic Recipient',street:'Teststraße 12',postcode:'74821',city:'Mosbach',delivery_salutation:choice,delivery_recipient:'Other Recipient',delivery_street:'Andere Straße 7',delivery_postcode:'10115',delivery_city:'Berlin'}))form.elements.namedItem(key).value=value;form.elements.namedItem('different_delivery').checked=different;await form.listeners.submit({preventDefault(){}});return f;}
 const blankAddress=await address('');const blankPayload=JSON.parse(blankAddress.requests.find(r=>r.url.endsWith('/buyer/address')).options.body);assert.equal(blankPayload.delivery,null);assert.equal(Object.hasOwn(blankPayload.billing,'salutation'),false);
 for(const choice of ['Herr','Frau','Firma']){const f=await address(choice);const payload=JSON.parse(f.requests.find(r=>r.url.endsWith('/buyer/address')).options.body);assert.equal(payload.delivery.salutation,choice);assert.equal(payload.delivery.recipient,'Synthetic Recipient');assert.equal(Object.hasOwn(payload.billing,'salutation'),false);assert.equal(f.requests.some(r=>r.url.endsWith('/orders')||r.url.endsWith('/checkout')),false);}
 const separate=await address('Frau',true),separatePayload=JSON.parse(separate.requests.find(r=>r.url.endsWith('/buyer/address')).options.body);assert.equal(separatePayload.delivery.salutation,'Frau');assert.equal(separatePayload.delivery.recipient,'Other Recipient');assert.equal(separatePayload.billing.recipient,'Synthetic Recipient');
 const invalidAn=await address('An');assert.equal(invalidAn.requests.some(r=>r.url.endsWith('/buyer/address')),false);assert.ok(invalidAn.nodes['address-status'].textContent.includes('Eingaben prüfen'));
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
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:mode,department:['felgen','komplettraeder'].includes(mode)?'wheels':'tyres',quoteEnabled:'false',buyerEnabled:'false',completeUrl:'/shop/komplettraeder/',studioUrl:'/shop/felgenstudio/'};
 const calls=[],stored=[];
 const events={};
 const document={body,visibilityState:visibility,createElement:tag=>new Node(tag),getElementById:id=>nodes[id]||null,querySelector:()=>({content:''}),querySelectorAll:()=>[],addEventListener:(event,fn)=>{events[event]=fn;}};
 const win={document,localStorage:{getItem:()=>JSON.stringify(cart),setItem:(key,value)=>stored.push(value)},setTimeout:(fn,delay)=>clock.set(fn,delay),clearTimeout:id=>clock.clear(id),setInterval(){},location:{search:'',pathname:'/shop/warenkorb/'},
  fetch:async(url,options)=>{calls.push({url,options});const result=await handler(url,calls.length);return {ok:true,text:async()=>JSON.stringify(result)};}};
 class FakeDate extends Date{constructor(...args){super(...(args.length?args:[clock.now]));}static now(){return clock.now;}}
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date:FakeDate,Math:Object.assign(Object.create(Math),{random:()=>0}),console});
 await flush();return {clock,nodes,calls,stored,document,events,win};
}
(async()=>{
 // Missing/false warmup flags do not create background GET timers.
 for(const flag of [undefined,false]){const f=await boot(()=>page(pending(),flag));assert.equal(f.clock.jobs.size,0);assert.equal(f.calls.length,1);}
 // ALZURA market prices stay numeric, but never imply confirmed RSU supply.
 const marketProduct={...priced(),purchase_status:'inquiry_only',price_label:'Richtpreis'};
 const market=await boot(url=>url.includes('/products?')?page(marketProduct,false):{item:marketProduct,prices_refreshing:false});
 const marketCard=market.nodes['catalog-products'].children[0],marketPrice=find(marketCard,'shop-price');assert.equal(find(marketPrice,'shop-price-kind').textContent,'Richtpreis');assert.ok(walk(marketPrice).some(n=>n.tagName==='STRONG'&&n.textContent==='123,45 €'));assert.ok(walk(marketPrice).some(n=>n.textContent==='Preis und Verfügbarkeit auf Anfrage'));assert.equal(walk(marketPrice).some(n=>n.textContent==='20 Stück gemeldet'),false);
 const marketAdd=walk(marketCard).find(n=>n.tagName==='BUTTON'&&n.textContent==='Zur Anfrageauswahl');assert.ok(marketAdd);marketAdd.listeners.click();assert.equal(JSON.parse(market.stored.at(-1))[0].id,A);assert.equal(JSON.stringify(JSON.parse(market.stored.at(-1))).includes('price'),false);
 find(marketCard,'shop-detail-link').listeners.click();await flush();assert.equal(find(market.nodes['product-detail'],'shop-price-kind').textContent,'Richtpreis');assert.ok(walk(market.nodes['product-detail']).some(n=>n.textContent==='Zur Anfrageauswahl'));
 const bound=await boot(()=>page({...priced(),purchase_status:'supplier_check_required'},false));assert.equal(find(bound.nodes['catalog-products'],'shop-price-kind'),undefined);assert.ok(walk(bound.nodes['catalog-products']).some(n=>n.textContent==='In den Warenkorb'));
 const labelUrl='/shop/documents/'+'d'.repeat(64)+'.jpg',sheetUrl='/shop/documents/'+'e'.repeat(64)+'.pdf';
 const documented=await boot(()=>page({...priced(),product_documents:{tyre_label_url:labelUrl,tyre_sheet_url:sheetUrl,eprel_id:'12345'}},false));const labelButton=find(documented.nodes['catalog-products'],'shop-tyre-label');assert.ok(labelButton);assert.equal(labelButton.children[0].src,labelUrl);assert.ok(labelButton['aria-label'].includes('vergrößert ansehen'));assert.ok(walk(documented.nodes['catalog-products']).some(n=>n.tagName==='A'&&n.href===sheetUrl&&n.textContent==='Produktdatenblatt (PDF) ↗'));labelButton.listeners.click();const labelDialog=find(documented.document.body,'shop-document-dialog');assert.equal(labelDialog.open,true);assert.ok(walk(labelDialog).some(n=>n.tagName==='IMG'&&n.src===labelUrl));
 const badDocs=await boot(()=>page({...priced(),product_documents:{tyre_label_url:'https://foreign.test/private.jpg',tyre_sheet_url:'javascript:unsafe'}},false));assert.equal(find(badDocs.nodes['catalog-products'],'shop-tyre-label'),undefined);assert.ok(walk(badDocs.nodes['catalog-products']).some(n=>n.textContent==='EU-Reifenlabel und Produktdatenblatt werden noch geprüft.'));
 let marketReads=0;const marketPoll=await boot(()=>++marketReads===1?page({...pending(),purchase_status:'inquiry_only'},true):page(priced(),false));await marketPoll.clock.next();assert.equal(find(marketPoll.nodes['catalog-products'],'shop-price-kind').textContent,'Richtpreis');assert.ok(walk(marketPoll.nodes['catalog-products']).some(n=>n.textContent==='123,45 €'));
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
 assert.ok(walk(detail.nodes['product-detail']).some(n=>n.textContent==='Reifengröße sowie Last- und Geschwindigkeitsindex vor dem Kauf prüfen. Fahrzeugpassung für Montage wird gesondert geprüft.'));
 // Closing the dialog suppresses its pending timer rather than fetching hidden details.
 const closed=await boot(url=>url.includes('/products?')?page(priced(),false):{item:pending(),prices_refreshing:true});find(closed.nodes['catalog-products'].children[0],'shop-detail-link').listeners.click();await flush();closed.nodes['close-product'].listeners.click();await closed.clock.drain();assert.equal(closed.calls.length,2);
 // Cart price polling updates its existing estimate, not the line controls.
 let cartReads=0;const cart=await boot(()=>({item:++cartReads===1?pending():priced(A,2,4444),prices_refreshing:cartReads===1}),{mode:'warenkorb',cart:[{id:A,quantity:2,services:{}}]});
 const cartLine=cart.nodes['cart-items'].children[0],cartSelect=find(cartLine,'shop-quantity'),cartTotal=find(cart.nodes['cart-summary'],'shop-summary-total');await cart.clock.next();
 assert.equal(cart.nodes['cart-items'].children[0],cartLine);assert.equal(find(cartLine,'shop-quantity'),cartSelect);assert.equal(cartSelect.value,'2');assert.equal(cartTotal.textContent,'88,88 €');assert.equal(cart.calls.length,2);
 assert.ok(walk(cartLine).some(n=>n.textContent==='Reifengröße sowie Last- und Geschwindigkeitsindex vor dem Kauf prüfen. Fahrzeugpassung für Montage wird gesondert geprüft.'));
 const marketCartProduct={...priced(A,2,4444),purchase_status:'inquiry_only'};marketCartProduct.price.stock_reported=0;
 const marketCart=await boot(()=>({item:marketCartProduct,prices_refreshing:false}),{mode:'warenkorb',cart:[{id:A,quantity:2,services:{}}]});assert.equal(find(marketCart.nodes['cart-items'],'shop-price-kind').textContent,'Richtpreis');assert.equal(find(marketCart.nodes['cart-summary'],'shop-summary-total').textContent,'88,88 €');assert.ok(walk(marketCart.nodes['cart-summary']).some(n=>n.textContent==='Unverbindliche Artikel-Zwischensumme'));assert.ok(walk(marketCart.nodes['cart-summary']).some(n=>n.textContent==='Enthält Richtpreise. Preis und Verfügbarkeit auf Anfrage.'));assert.equal(marketCart.nodes['request-quote'].disabled,true);
 // Opening and reviewing a mounting wish neither books nor alters the cart.
 const writesBefore=cart.stored.length,readsBefore=cart.calls.length;
 const appointment=walk(cartLine).find(n=>n.tagName==='BUTTON'&&n.textContent==='Montagetermin in Mosbach buchen');assert.ok(appointment);appointment.listeners.click();
 const requestDialog=find(cart.document.body,'shop-mounting-dialog');assert.equal(requestDialog.open,true);assert.equal(cart.stored.length,writesBefore);assert.equal(cart.calls.length,readsBefore);
 assert.ok(walk(requestDialog).some(n=>n.textContent==='Terminbestätigung nach Lieferklärung'));
 const requestForm=walk(requestDialog).find(n=>n.tagName==='FORM');requestForm.listeners.submit({preventDefault(){}});
 assert.equal(cart.stored.length,writesBefore);assert.equal(cart.calls.length,readsBefore);assert.ok(walk(requestDialog).some(n=>n.textContent.includes('löst keine Buchung aus')));
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
 // Rim -> tyre -> mounting -> explicit atomic cart action; opening/searching adds nothing.
 const rimId='alloy_wheel:'+'c'.repeat(24),rim=(q=1)=>({...priced(rimId,q,10000),category:'alloy_wheel',name:'SYNTHETIC Rim',geometry:{diameter:'18'}});
 const complete=await boot(url=>{const u=new URL('https://shop.test'+url),q=Number(u.searchParams.get('quantity')||1);if(u.pathname.endsWith('/products'))return page(rim(),false);if(u.pathname.endsWith('/compatible-tyres'))return {rim_id:rimId,items:[priced(A,q,3000)],scope:'approved',prices_refreshing:false};return {item:rim(q),prices_refreshing:false};},{mode:'komplettraeder'});
 find(complete.nodes['catalog-products'].children[0],'shop-detail-link').listeners.click();await flush();
 const completeForm=find(complete.nodes['product-detail'],'shop-options'),completeQty=find(completeForm,'shop-quantity');completeQty.value='4';completeQty.listeners.change();await flush();
 const bundle=find(complete.nodes['product-detail'],'shop-bundle-panel'),search=walk(bundle).find(n=>n.tagName==='BUTTON'&&n.textContent.startsWith('Passende Reifen'));
 await search.listeners.click();assert.deepEqual(JSON.parse(complete.stored.at(-1)),[]);
 const tyre=walk(bundle).find(n=>n.name==='bundle_tyre');tyre.checked=true;tyre.listeners.change();
 const mounting=walk(bundle).find(n=>n.name==='bundle_mounting');mounting.checked=true;mounting.listeners.change();
 const add=walk(bundle).find(n=>n.tagName==='BUTTON'&&n.textContent==='Felgen + Reifen in den Warenkorb');assert.equal(add.disabled,false);add.listeners.click();
 const pair=JSON.parse(complete.stored.at(-1));assert.equal(pair.length,2);assert.deepEqual(pair.map(row=>row.quantity),[4,4]);assert.equal(pair[0].id,rimId);assert.equal(pair[1].id,A);assert.equal(pair[0].assembly_group,pair[1].assembly_group);assert.deepEqual(pair[0].services,{});assert.deepEqual(pair[1].services,{mounting:true});
 assert.equal(complete.calls.some(call=>call.options.method!==undefined),false);
 // Candidate cards remain selectable during a price warmup. Updating that
 // exact selected tyre must unlock the explicit pair action without reselecting.
 const delayedPair=await boot(url=>{const u=new URL('https://shop.test'+url),q=Number(u.searchParams.get('quantity')||1);if(u.pathname.endsWith('/products'))return page({...rim(),purchase_status:'inquiry_only'},false);if(u.pathname.endsWith('/compatible-tyres'))return {rim_id:rimId,items:[{...pending(),purchase_status:'inquiry_only'}],scope:'rim_width_candidate',prices_refreshing:true};if(decodeURIComponent(u.pathname).endsWith('/products/'+A))return {item:{...priced(A,q,3000),purchase_status:'inquiry_only'},prices_refreshing:false};return {item:{...rim(q),purchase_status:'inquiry_only'},prices_refreshing:false};},{mode:'komplettraeder'});
 find(delayedPair.nodes['catalog-products'].children[0],'shop-detail-link').listeners.click();await flush();
 const delayedQty=find(find(delayedPair.nodes['product-detail'],'shop-options'),'shop-quantity');delayedQty.value='4';delayedQty.listeners.change();await flush();
 const delayedBundle=find(delayedPair.nodes['product-detail'],'shop-bundle-panel'),delayedSearch=walk(delayedBundle).find(n=>n.tagName==='BUTTON'&&n.textContent.startsWith('Passende Reifen'));
 await delayedSearch.listeners.click();const delayedRadio=walk(delayedBundle).find(n=>n.name==='bundle_tyre');assert.ok(delayedRadio);delayedRadio.checked=true;delayedRadio.listeners.change();
 const delayedAdd=walk(delayedBundle).find(n=>n.tagName==='BUTTON'&&n.textContent==='Felgen + Reifen zur Anfrageauswahl');assert.equal(delayedAdd.disabled,true);assert.deepEqual(JSON.parse(delayedPair.stored.at(-1)),[]);
 await delayedPair.clock.next();assert.equal(delayedRadio.checked,true);assert.equal(delayedAdd.disabled,false);assert.ok(walk(delayedBundle).some(n=>n.textContent==='Größen-/Breitenvorschlag: Fahrzeugfreigabe, Tragfähigkeit und Auflagen sind noch zu prüfen.'));delayedAdd.listeners.click();assert.deepEqual(JSON.parse(delayedPair.stored.at(-1)).map(row=>row.quantity),[4,4]);
 // Family cards require an exact article choice before any cart action.
 const family=await boot(url=>{const u=new URL('https://shop.test'+url);if(u.pathname.endsWith('/products'))return page({...rim(),family:{variant_count:2,diameters:['17','18']}},false);if(u.pathname.endsWith('/variants'))return {items:[rim()],page:1,total:1,has_more:false};return {item:rim()};},{mode:'felgen'});
 const familyCard=family.nodes['catalog-products'].children[0];assert.equal(find(familyCard,'shop-quantity'),undefined);find(familyCard,'shop-detail-link').listeners.click();await flush();
 assert.ok(family.calls.some(call=>call.url.includes('/variants?')&&call.url.includes('page_size=24')));assert.deepEqual(JSON.parse(family.stored.at(-1)),[]);
 const exact=walk(family.nodes['product-detail']).find(n=>n.tagName==='BUTTON'&&n.textContent==='Diese Felge ansehen');exact.listeners.click();await flush();assert.ok(find(family.nodes['product-detail'],'shop-bundle-panel'));
 for(const fixture of [f,bounded,changed,quantity,tiers,detail,closed,cart,hidden,paused,pausedTier,images,complete,delayedPair,family,market,bound,marketPoll,marketCart,documented,badDocs])assert.ok(fixture.calls.every(call=>call.options.method===undefined&&call.url.startsWith('/shop/api/v2/products')),'read-only product calls only');
 console.log('Bounded price/image polling passed');
})().catch(error=>{console.error(error);process.exit(1)});
'''
        result = subprocess.run(['node', '-e', code, str(ROOT / 'static/shop_v2/shop.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Bounded price/image polling passed', result.stdout)

    def test_approved_studio_geometry_zoom_and_public_api_bridge(self):
        code = r'''
const assert=require('node:assert/strict');
const G=require(process.argv[1]),Z=require(process.argv[2]),B=require(process.argv[3]);
const face={width:800,height:800,cx:419.533,cy:399.048,rx:377.055,ry:400.475,rotation:-5.099,hub:{x:439.96,y:399.21},reviewed:true};
assert.equal(G.validatePhotoMeta({name:'car.svg',type:'image/svg+xml',size:100}), 'Bitte ein JPG-, PNG- oder WebP-Foto wählen.');
assert.notEqual(G.validatePhotoMeta({name:'car.png',type:'image/png',size:13*1024*1024}),'');
assert.equal(G.validSignature(new Uint8Array([137,80,78,71,13,10,26,10]),'image/png'),true);
assert.equal(G.validSignature(new Uint8Array([60,115,118,103]),'image/png'),false);
assert.equal(G.fittedDimensions(8000,6000),null);
assert.deepEqual(G.fittedDimensions(6000,4000),{width:2048,height:1365});
assert.equal(G.safePhoto('/shop/media/synthetic.webp'),true);assert.equal(G.safePhoto('https://foreign.test/wheel.png'),false);assert.equal(G.safePhoto('bilder/private.png'),false);
const checked=G.photoFace(face);assert.ok(checked);assert.equal(G.photoFace({...face,reviewed:false}),null);assert.equal(G.photoFace(face,799,800),null);
const centre=G.sourceToFace(checked,checked.hub);assert.ok(Math.hypot(centre.x,centre.y)<1e-12);
for(let n=0;n<24;n++){const point={x:Math.cos(n*Math.PI/12),y:Math.sin(n*Math.PI/12)},source=G.faceToSource(checked,point),unit=G.ellipseUnit(checked,source),back=G.sourceToFace(checked,source);assert.ok(Math.abs(unit.x**2+unit.y**2-1)<1e-10);assert.ok(Math.hypot(back.x-point.x,back.y-point.y)<1e-10);}
assert.equal(G.rimBackdrop({name:'SYNTHETIC black wheel'}),'contrast');assert.equal(G.rimPlacement(800,700,100),null);
const placement=G.rimPlacement(800,800,100,{rxRatio:.82,ryRatio:.85,rotation:3});assert.deepEqual(placement,{rx:82,ry:85,rotation:3});
const view=G.viewportTransform(1600,900,1200,800,{zoom:3,focusX:.78,focusY:.69});assert.equal(view.zoom,3);assert.ok(view.scale>0);
assert.deepEqual(Z.targetSize(3000,1500,2),{width:2048,height:1024});assert.equal(Z.zoomValue(100),6);
const frame={zoom:2,scale:1,focusX:.5,focusY:.5,offsetX:-400,offsetY:-150,width:800,height:600,sceneWidth:1600,sceneHeight:900},point={x:600,y:400};
const old=Z.scenePoint(point,frame),focus=Z.anchoredFocus(frame,point,4),scale=2;
assert.ok(Math.abs((point.x-(400-focus.focusX*1600*scale))/(1600*scale)-old.x)<1e-10);
const id='alloy_wheel:'+'a'.repeat(24);
const product={id,category:'alloy_wheel',name:'SYNTHETIC Wheel',brand:'SYNTHETIC',size:'8 x 18',images:['/shop/media/synthetic.webp'],geometry:{diameter:'18'},specifications:{Farbe:'Black',private:'PRIVATE_CANARY'},photo_face:{...face,private:'PRIVATE_CANARY'},provider_id:'PRIVATE_CANARY',net_price:'PRIVATE_CANARY',price:{currency:'EUR',vat_percent:'19.00',observed_at:'2026-11-17T12:00:00Z',expires_at:'2026-11-17T12:10:00Z',stock_reported:20,private:'PRIVATE_CANARY',prices_by_quantity:{'4':{quantity:4,unit_gross_cent:10000,items_gross_cent:40000,net_price:'PRIVATE_CANARY'}}}};
const normal=B.normaliseProduct(product);assert.equal(normal.selection_id,id);assert.equal(normal.photo,'/shop/media/synthetic.webp');assert.ok(G.photoFace(normal.photo_face));assert.equal(JSON.stringify(normal).includes('PRIVATE_CANARY'),false);
assert.equal(B.normaliseProduct({...product,purchase_status:'inquiry_only',price_label:'PRIVATE_CANARY'}).public_product.purchase_status,'inquiry_only');assert.equal(JSON.stringify(B.normaliseProduct({...product,purchase_status:'PRIVATE_CANARY'})).includes('PRIVATE_CANARY'),false);
assert.equal(B.normaliseProduct({...product,images:['https://foreign.test/rim.webp']}),null);
assert.equal(B.normaliseProduct({...product,geometry:{diameter:'99'}}),null);
async function bridge(count){const elements={},requests=[],pages=[];for(const key of ['ws-api-status','ws-next','ws-previous','ws-page-label','ws-family-query'])elements[key]={value:'',listeners:{},addEventListener(name,fn){this.listeners[name]=fn;}};
const win={document:{body:{dataset:{page:'felgenstudio'}},getElementById:key=>elements[key]},URLSearchParams,location:{search:''},ShopWheelStudioController:{setProducts:rows=>pages.push(rows)},ShopStorefront:{read:async path=>{requests.push(path);return {items:Array.from({length:count},(_,n)=>({...product,id:'alloy_wheel:'+n.toString(16).padStart(24,'0')})),page:1,total:count,has_more:false};}},setTimeout(){},clearTimeout(){}};
B.mount(win);await new Promise(resolve=>setImmediate(resolve));return {elements,requests,pages};}
async function imageChecks(){
 let img,jobs=[];const win={Image:class{constructor(){img=this;this.urls=[];}set src(value){this.urls.push(value);}},setTimeout:(fn,delay)=>{assert.ok(delay>=400&&delay<=5300);jobs.push(fn);return jobs.length;},clearTimeout(){}};
 const exhausted=G.loadStudioImage(win,'/shop/media/synthetic.webp').then(()=>false,()=>true);
 for(let n=0;n<5;n++){img.onerror();assert.equal(jobs.length,1);jobs.shift()();}img.onerror();assert.equal(await exhausted,true);assert.equal(img.urls.length,6);assert.equal(jobs.length,0);
 const successful=G.loadStudioImage(win,'/shop/media/synthetic.webp');img.onerror();jobs.shift()();img.onload();assert.equal(await successful,img);
 let active=true;const abandoned=G.loadStudioImage(win,'/shop/media/synthetic.webp',()=>active).then(()=>false,()=>true);img.onerror();active=false;jobs.shift()();assert.equal(await abandoned,true);assert.equal(img.urls.length,1);
 const local=G.loadStudioImage(win,'blob:local-photo').then(()=>false,()=>true);img.onerror();assert.equal(await local,true);assert.equal(jobs.length,0);
}
(async()=>{await imageChecks();const valid=await bridge(24);assert.equal(valid.pages.length,1);assert.equal(valid.pages[0].length,24);const query=new URL('https://shop.test'+valid.requests[0]).searchParams;assert.equal(query.get('studio'),'1');assert.equal(query.get('page_size'),'24');assert.equal(valid.requests.length,1);const oversized=await bridge(25);assert.equal(oversized.pages.length,0);console.log('Approved studio geometry and public bridge passed');})().catch(error=>{console.error(error);process.exit(1)});
'''
        result = subprocess.run(['node', '-e', code,
                                 str(ROOT / 'static/shop_v2/studio.js'),
                                 str(ROOT / 'static/shop_v2/studio-zoom.js'),
                                 str(ROOT / 'static/shop_v2/studio-api.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Approved studio geometry and public bridge passed', result.stdout)


if __name__ == '__main__':
    unittest.main()
