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

    def test_shop_aliases_lead_to_the_checkout_origin_and_offer_existing_cart_transfer(self):
        origin = 'https://auto-lackierzentrum.de'
        self.app.config.update(SHOP_FRONTEND_ENABLED=True, SHOP_BUYER_ENABLED=True, SHOP_PUBLIC_ORIGIN=origin)
        for alias, key in (('https://kundenstatus-app.onrender.com', 'portal'), ('https://www.auto-lackierzentrum.de', 'www')):
            with self.subTest(alias=alias):
                for path in ('/shop/', '/shop/komplettraeder/', '/shop/felgenstudio/'):
                    page = self.client.get(path + '?artikel=test&utm_source=ads&email=PRIVATE', base_url=alias)
                    self.assertEqual(page.status_code, 302)
                    self.assertEqual(page.location, origin + path + '?artikel=test&utm_source=ads')
                previous = self.client.get('/shop/warenkorb/?auswahl=old', base_url=alias)
                tyre_entry = self.client.get('/shop/reifen/?montage=1&email=PRIVATE', base_url=alias)
                self.assertEqual(tyre_entry.location, origin + '/shop/reifen/?montage=1')
                self.assertIn('data-transfer="send"', previous.text)
                self.assertIn('href="' + origin + '/shop/warenkorb/"', previous.text)
                self.assertNotIn('id="buyer-address"', previous.text)
                incoming = self.client.get('/shop/warenkorb/?uebernahme=' + key, base_url=origin)
                self.assertIn('data-transfer="receive"', incoming.text)
                self.assertIn('data-peer-origin="' + alias + '"', incoming.text)
        for path in ('/shop/warenkorb/', '/shop/warenkorb/?uebernahme=https://untrusted.test'):
            self.assertIn('id="buyer-address"', self.client.get(path, base_url=origin).text)
        self.assertNotIn('data-transfer=', self.client.get('/shop/warenkorb/', base_url='http://localhost').text)

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
        self.assertEqual(selects, [{'name': 'appointment_time', 'disabled': None}, {'name': 'delivery_salutation', 'autocomplete': 'off'}, {'name': 'workshop_delivery'}])
        options = [attrs for tag, attrs in Markup(source).tags if tag == 'option']
        self.assertEqual([attrs.get('value') for attrs in options], ['', '', 'Herr', 'Frau', 'Firma', 'personal_offer', 'collection', 'customer_reshipment'])
        self.assertEqual([attrs.get('value') for attrs in options if 'selected' in attrs], ['collection'])
        self.assertIn('data-checkout-enabled="true"', source)
        self.assertIn('data-consumer-enabled="true"', source)
        self.assertIn('id="checkout-button"', source)
        self.assertIn('type="button" hidden', source)
        self.assertLess(source.index('</aside>'), source.index('id="quote-review"'))

    def test_mainland_declaration_is_a_literal_opt_in_and_never_prechecked(self):
        self.app.config.update(SHOP_FRONTEND_ENABLED=True, SHOP_BUYER_ENABLED=True)
        for enabled in (None, False, 'true', 1, True):
            with self.subTest(enabled=enabled):
                self.app.config['SHOP_MAINLAND_DELIVERY_ENABLED'] = enabled
                source = self.client.get('/shop/warenkorb/').get_data(as_text=True)
                fields = [attrs for tag, attrs in Markup(source).tags
                          if tag == 'input' and attrs.get('name') == 'delivery_mainland']
                self.assertIn('data-mainland-delivery-enabled="' + ('true' if enabled is True else 'false') + '"', source)
                if enabled is True:
                    self.assertEqual(len(fields), 1)
                    self.assertEqual(fields[0]['type'], 'checkbox')
                    self.assertNotIn('checked', fields[0])
                    self.assertIn('disabled', fields[0])  # JS requires it only for a customer shipping leg.
                    self.assertIn('Meine Lieferadresse liegt auf dem deutschen Festland (keine Inselzustellung).', source)
                    self.assertIn('Für Inselzustellung bestätigen wir Versandkosten', source)
                else:
                    self.assertEqual(fields, [])
                self.assertIn('data-checkout-enabled="false"', source)

    def test_complete_wheel_controls_require_literal_server_opt_in(self):
        self.app.config.update(SHOP_FRONTEND_ENABLED=True, SHOP_BUYER_ENABLED=True)
        for enabled in (None, False, 'true', 1, True):
            with self.subTest(enabled=enabled):
                self.app.config['SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED'] = enabled
                source = self.client.get('/shop/warenkorb/').get_data(as_text=True)
                fields = [attrs for tag, attrs in Markup(source).tags
                          if tag == 'input' and attrs.get('name') == 'vehicle_ref']
                self.assertIn('data-deferred-complete-wheels-enabled="' + ('true' if enabled is True else 'false') + '"', source)
                if enabled is True:
                    self.assertEqual(len(fields), 1)
                    self.assertEqual(fields[0]['maxlength'], '200')
                    self.assertEqual(fields[0]['minlength'], '3')
                    self.assertNotIn('required', fields[0])  # Dynamic only for complete-wheel review carts.
                    self.assertNotIn('value', fields[0])
                    self.assertIn('Fahrzeugpassung und Gutachten prüfen wir vor der Abbuchung.', source)
                else:
                    self.assertEqual(fields, [])
                self.assertIn('data-checkout-enabled="false"', source)

    def test_parts_exact_vehicle_options_require_explicit_flag_and_strict_registry_projection(self):
        self.app.config.update(SHOP_FRONTEND_ENABLED=True, SHOP_BUYER_ENABLED=True)
        fixture={'vehicle_ref':'alzura-vehicle:120604','car_id':120604,'manufacturer_id':5,'model_id':10253,
            'make':'Audi','model':'A3 (8V1, 8VK)','engine':'1.0 TFSI','engine_codes':['CHZD'],
            'date_from_yyyymm':201607,'date_to_yyyymm':201712,'hsn_tsn':['0588BHT'],
            'power_kw_from':85,'power_kw_to':85}
        vehicles=[]
        for car in (100043,117083,120604,802380):
            item={**fixture,'vehicle_ref':'alzura-vehicle:'+str(car),'car_id':car}
            vehicles.append(item)
        vehicles[-1].update(make='BMW',model='3er Touring',engine='M340i xDrive',date_from_yyyymm=202509,date_to_yyyymm=None)
        self.app.config['SHOP_PARTS_VEHICLE_OPTIONS']=vehicles
        for flag in (None,False,'true',1,True):
            self.app.config['SHOP_PARTS_DIRECT_ENABLED']=flag
            source=self.client.get('/shop/warenkorb/').get_data(as_text=True)
            selects=[attrs for tag,attrs in Markup(source).tags if tag=='select' and attrs.get('name')=='parts_vehicle_ref']
            self.assertEqual(bool(selects),flag is True)
            self.assertIn('data-parts-direct-enabled="'+('true' if flag is True else 'false')+'"',source)
            if flag is True:
                self.assertIn('07/2016–12/2017',source);self.assertIn('09/2025–offen',source)
                self.assertIn('0588/BHT',source);self.assertIn('CHZD',source)
                for car in (100043,117083,120604,802380):self.assertIn('value="alzura-vehicle:'+str(car)+'"',source)
                self.assertIn('für jeden gewählten Artikel',source)
        for change in [{'vehicle_ref':'alzura-vehicle:999'},{'car_id':True},{'date_from_yyyymm':201613},
                       {'date_to_yyyymm':201501},{'hsn_tsn':[['0588BHT']]},{'engine_codes':['BAD\nCODE']}]:
            self.app.config['SHOP_PARTS_VEHICLE_OPTIONS']=[{**fixture,**change}]
            source=self.client.get('/shop/warenkorb/').get_data(as_text=True)
            self.assertNotIn('value="alzura-vehicle:',source)
            self.assertIn('keine bestätigte Fahrzeugauswahl verfügbar',source)
        malicious={**fixture,'make':'<script>UNTRUSTED</script>'}
        self.app.config['SHOP_PARTS_VEHICLE_OPTIONS']=[malicious]
        source=self.client.get('/shop/warenkorb/').get_data(as_text=True)
        self.assertIn('&lt;script&gt;UNTRUSTED&lt;/script&gt;',source)
        self.assertNotIn('<script>UNTRUSTED</script>',source)

    def test_parts_vehicle_selection_saved_context_and_payload_are_checked_offline(self):
        result=subprocess.run(['node',str(ROOT/'scripts/test_shop_parts_vehicle_ui.js'),str(ROOT/'static/shop_v2/shop.js')],
                              capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('Exact parts vehicle UI checks passed',result.stdout)

    def test_complete_wheel_review_ui_uses_only_synthetic_saved_context(self):
        result = subprocess.run(['node', str(ROOT / 'scripts/test_shop_complete_wheel_ui.js'),
                                 str(ROOT / 'static/shop_v2/shop.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Complete-wheel authorization UI checks passed', result.stdout)

    def test_mainland_payload_and_delivery_changes_use_real_frontend_handlers(self):
        code = r'''
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const path=process.argv[1],source=fs.readFileSync(path,'utf8'),logic=require(path),ID='tyre:'+'a'.repeat(24);
const row=services=>({id:ID,quantity:1,services:services||{}});
assert.equal(logic.cartNeedsCustomerDelivery([row()]),true);
assert.equal(logic.cartNeedsCustomerDelivery([row({runflat:true})]),true);
assert.equal(logic.cartNeedsCustomerDelivery([row({mounting:true})]),false);
assert.equal(logic.cartNeedsCustomerDelivery([row({mounting:true})],'customer_reshipment'),true);
assert.equal(logic.cartNeedsCustomerDelivery([row({mounting:true}),row()]),true);
assert.equal(logic.cartNeedsCustomerDelivery([]),false);
class Node{
 constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.fields={};this.listeners={};this.dataset={};this.textContent='';this.value='';this.checked=false;this.hidden=false;this.disabled=false;this.isConnected=true;this.classList={toggle(){}};this.elements={namedItem:key=>this.fields[key]||null};}
 append(...nodes){this.children.push(...nodes);}replaceChildren(...nodes){this.children=[];this.append(...nodes);}
 addEventListener(name,fn){this.listeners[name]=fn;}setAttribute(name,value){this[name]=value;}
 removeAttribute(name){delete this[name];}scrollIntoView(){}replaceWith(){}showModal(){this.open=true;}close(){this.open=false;}
}
const flush=()=>new Promise(resolve=>setImmediate(resolve));
async function boot({enabled=true,services={},storedMainland,missing=false,readySession=false}={}){
 const nodes={};for(const key of ['close-product','product-dialog','product-detail','shop-announcement','cart-items','cart-summary','cart-status','request-quote','quote-status','quote-review','checkout-button','checkout-note','buyer-address','address-status','delivery-fields','cart-workflow-note','mainland-delivery-confirmation'])nodes[key]=new Node();
 const form=nodes['buyer-address'];
 for(const key of ['name','email','phone','recipient','street','postcode','city','delivery_recipient','delivery_street','delivery_postcode','delivery_city','different_delivery','delivery_salutation','workshop_delivery','delivery_mainland']){
  const field=new Node('input');field.name=key;field.type=['different_delivery','delivery_mainland'].includes(key)?'checkbox':'text';form.fields[key]=field;
 }
 Object.assign(form.fields.name,{value:'SYNTHETIC'});Object.assign(form.fields.email,{value:'synthetic@example.invalid'});
 Object.assign(form.fields.recipient,{value:'SYNTHETIC'});Object.assign(form.fields.street,{value:'Synthetic 1'});Object.assign(form.fields.postcode,{value:'74821'});Object.assign(form.fields.city,{value:'Mosbach'});
 Object.assign(form.fields.delivery_recipient,{value:'OTHER SYNTHETIC'});Object.assign(form.fields.delivery_street,{value:'Other 2'});Object.assign(form.fields.delivery_postcode,{value:'74821'});Object.assign(form.fields.delivery_city,{value:'Mosbach'});
 form.fields.workshop_delivery.value='collection';form.fields.delivery_salutation.value='Firma';
 if(missing)delete form.fields.delivery_mainland;
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:'warenkorb',buyerEnabled:'true',quoteEnabled:'true',orderEnabled:'false',checkoutEnabled:'false',consumerEnabled:'false',mainlandDeliveryEnabled:enabled?'true':'false',cartRequestUrl:'/anfrage?anliegen=allgemein&shop_anfrage=1'};
 const calls=[],token='SYNTHETIC_TOKEN_12345678901234567890',expiry=new Date(Date.now()+600000).toISOString();
 const session=()=>({guest:true,address_ready:readySession,csrf_token:token,expires_at:expiry,workshop_delivery:'collection',...(storedMainland===undefined?{}:{delivery_mainland:storedMainland})});
 const product={id:ID,category:'tyre',name:'SYNTHETIC TYRE',size:'205/55 R18',images:[],purchase_status:'supplier_check_required',price_status:'current',price:{currency:'EUR',vat_percent:'19.00',observed_at:new Date(Date.now()-1000).toISOString(),expires_at:expiry,stock_reported:20,prices_by_quantity:{'1':{quantity:1,unit_gross_cent:10000,items_gross_cent:10000}}}};
 const win={document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key]||null,querySelector:()=>({content:token}),querySelectorAll:()=>[],addEventListener(){}},localStorage:{getItem:()=>JSON.stringify([row(services)]),setItem(){}},sessionStorage:{getItem:()=>null,setItem(){}},location:{pathname:'/shop/warenkorb/',assign(){throw Error('No navigation');}},setTimeout(){},clearTimeout(){},setInterval(){},
 fetch:async(url,options)=>{calls.push({url,options});let value;
  if(url.endsWith('/buyer/session'))value=session();
  else if(url.includes('/products/'))value={item:product,prices_refreshing:false};
  else if(url.endsWith('/buyer/address')){const saved=JSON.parse(options.body);value={...session(),address_ready:true,...(Object.hasOwn(saved,'delivery_mainland')?{delivery_mainland:saved.delivery_mainland}:{})};}
  else throw Error('Forbidden request '+url);
  return{ok:true,text:async()=>JSON.stringify(value)};
 }};
 class FormData{constructor(form){this.form=form;}get(key){const field=this.form.elements.namedItem(key);return !field||field.disabled?null:field.type==='checkbox'?(field.checked?'on':null):field.value||null;}}
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date,console,FormData});await flush();await flush();
 return{nodes,form,calls,field:form.elements.namedItem('delivery_mainland'),submit:async()=>{await form.listeners.submit({preventDefault(){}});await flush();await flush();},change:()=>form.listeners.input()};
}
const puts=f=>f.calls.filter(c=>c.url.endsWith('/buyer/address'));
(async()=>{
 const direct=await boot();assert.equal(direct.field.required,true);assert.equal(direct.field.disabled,false);assert.equal(direct.nodes['mainland-delivery-confirmation'].hidden,false);
 assert.equal(direct.field.checked,false);await direct.submit();assert.equal(JSON.parse(puts(direct).at(-1).options.body).delivery_mainland,false);
 direct.field.checked=true;direct.change();assert.equal(direct.field.checked,true,'checking declaration must not clear itself');await direct.submit();assert.equal(JSON.parse(puts(direct).at(-1).options.body).delivery_mainland,true);
 assert.equal(typeof JSON.parse(puts(direct).at(-1).options.body).delivery_mainland,'boolean');
 const missing=await boot({missing:true});await missing.submit();assert.equal(puts(missing).length,0,'missing enabled control never asserts true');
 const malformed=await boot();malformed.field.checked='true';await malformed.submit();assert.equal(puts(malformed).length,0,'string cannot become a declaration');
 const badSession=await boot({storedMainland:'true',readySession:true});assert.equal(badSession.field.checked,false);assert.equal(badSession.nodes['request-quote'].disabled,true,'invalid session declaration cannot mark address ready');
 const legacy=await boot({enabled:false,missing:true});await legacy.submit();assert.equal(Object.hasOwn(JSON.parse(puts(legacy).at(-1).options.body),'delivery_mainland'),false,'disabled feature preserves old payload');
 const saved=await boot({storedMainland:true});assert.equal(saved.field.checked,true);saved.form.fields.phone.value='SYNTHETIC';saved.change();assert.equal(saved.field.checked,true);
 saved.form.fields.street.value='Synthetic 3';saved.change();assert.equal(saved.field.checked,false,'effective billing delivery changed');
 saved.field.checked=true;saved.change();assert.equal(saved.field.checked,true);
 saved.form.fields.different_delivery.checked=true;saved.form.fields.different_delivery.listeners.change({target:saved.form.fields.different_delivery});assert.equal(saved.field.checked,false,'separate delivery switched');
 saved.field.checked=true;saved.change();saved.form.fields.street.value='Billing-only 4';saved.change();assert.equal(saved.field.checked,true,'unrelated billing address does not change separate delivery');
 saved.form.fields.delivery_postcode.value='10115';saved.change();assert.equal(saved.field.checked,false,'actual separate postcode changed');
 const collection=await boot({services:{mounting:true}});assert.equal(collection.field.required,false);assert.equal(collection.field.disabled,true);assert.equal(collection.nodes['mainland-delivery-confirmation'].hidden,true);
 await collection.submit();assert.equal(JSON.parse(puts(collection).at(-1).options.body).delivery_mainland,false,'workshop collection keeps bool without forced mainland assertion');
 collection.form.fields.workshop_delivery.value='customer_reshipment';collection.form.fields.workshop_delivery.listeners.change();assert.equal(collection.field.required,true);assert.equal(collection.field.checked,false);
 assert.equal([direct,missing,malformed,legacy,saved,collection].flatMap(f=>f.calls).some(c=>/\/orders|\/checkout|\/consumer|\/cart\/quote/.test(c.url)),false);
 console.log('Mainland UI declaration and saved-address payload checks passed');
})().catch(error=>{console.error(error);process.exit(1)});
'''
        result = subprocess.run(['node', '-e', code, str(ROOT / 'static/shop_v2/shop.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Mainland UI declaration and saved-address payload checks passed', result.stdout)

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
                    self.assertTrue(source.startswith('/static/shop_v2/') or source in {
                        '/static/google_ads_tracking.js', '/static/google_ads_tracking.css'}, source)
                    with self.client.get(source) as asset:
                        self.assertEqual(asset.status_code, 200, source)

    def test_home_categories_use_released_same_origin_media_and_link_to_full_configurator(self):
        self.app.config.update(SHOP_FRONTEND_ENABLED=True, SHOP_MEDIA_ENABLED=True)
        home = self.client.get('/shop/').get_data(as_text=True)
        tags = Markup(home).tags
        tiles = [attrs['href'] for tag, attrs in tags if tag == 'a' and attrs.get('class') == 'shop-category-tile']
        self.assertEqual(tiles, ['/shop/felgen/', '/shop/reifen/', '/shop/ersatzteile/', '/shop/zubehoer/'])
        photos = [attrs for tag, attrs in tags if tag == 'img' and attrs['src'].startswith('/shop/media/')]
        self.assertEqual(len({p['src'] for p in photos}), 4)
        for photo in photos:
            self.assertRegex(photo['src'], r'^/shop/media/[a-f0-9]{64}\.(?:jpg|png)$')
            self.assertEqual((photo['alt'], photo['loading']), ('', 'lazy'))
        fallback = [photo['data-fallback-src'] for photo in photos if 'data-fallback-src' in photo]
        self.assertEqual(fallback, ['/static/homepage/leistungsfotos-v2/oelservice.jpg'])
        with self.client.get(fallback[0]) as image:
            self.assertEqual(image.status_code, 200)
            self.assertEqual(image.mimetype, 'image/jpeg')
        self.assertIn('href="/shop/komplettraeder/">Kompletträder zusammenstellen', home)
        self.assertIn('href="#produktgruppen"', home)
        self.assertIn('href="/">Firmenhomepage', home)
        self.app.config['SHOP_MEDIA_ENABLED'] = False
        labelled = Markup(self.client.get('/shop/').get_data(as_text=True))
        self.assertFalse(any(tag == 'img' and attrs['src'].startswith('/shop/media/') for tag, attrs in labelled.tags))
        self.assertEqual(tiles, [attrs['href'] for tag, attrs in labelled.tags if tag == 'a' and attrs.get('class') == 'shop-category-tile'])

    def test_optional_google_measurement_has_only_bounded_external_csp_allowance(self):
        self.app.config['SHOP_FRONTEND_ENABLED'] = True
        response = self.client.get('/shop/felgen/')
        directives = {parts[0]: set(parts[1:]) for text in response.headers['Content-Security-Policy'].split(';')
                      if (parts := text.split())}
        self.assertEqual(directives['default-src'], {"'none'"})
        self.assertEqual(directives['style-src'], {"'self'"})
        self.assertEqual(directives['script-src'], {"'self'", 'https://www.googletagmanager.com',
                         'https://www.googleadservices.com', 'https://www.google.com'})
        self.assertEqual(directives['connect-src'], {"'self'", 'https://www.googletagmanager.com',
                         'https://www.googleadservices.com', 'https://googleads.g.doubleclick.net',
                         'https://pagead2.googlesyndication.com', 'https://ad.doubleclick.net',
                         'https://www.google.com', 'https://www.google.de'})
        self.assertEqual(directives['frame-src'], {'https://www.googletagmanager.com'})
        self.assertEqual(directives['form-action'], {"'self'"})
        self.assertEqual(directives['base-uri'], {"'none'"})
        self.assertEqual(directives['frame-ancestors'], {"'none'"})
        self.assertNotIn('*', response.headers['Content-Security-Policy'])
        self.assertNotIn('unsafe-inline', response.headers['Content-Security-Policy'])
        self.assertNotIn('unsafe-eval', response.headers['Content-Security-Policy'])
        self.assertNotIn('google-analytics.com', response.headers['Content-Security-Policy'])
        for tag, attrs in Markup(response.get_data(as_text=True)).tags:
            if tag == 'script':
                self.assertTrue(attrs['src'].startswith('/static/'))
                self.assertNotIn('http', attrs['src'])

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
        for path in ('/shop/felgen/', '/shop/komplettraeder/'):
            wheels = self.client.get(path).get_data(as_text=True)
            self.assertIn('name="wheel_make"', wheels)
            self.assertIn('type="search" id="wheel-make"', wheels)
            self.assertIn('list="wheel-make-options"', wheels)
            self.assertIn('href="/anfrage?anliegen=felgenservice#kurzanfrage"', wheels)
            self.assertLess(wheels.index('Fahrzeugmarke'), wheels.index('Felgenhersteller'))
            for make in ('Audi', 'BMW', 'MINI'):
                self.assertIn(f'<option value="{make}">{make}</option>', wheels)

    def test_frontend_queries_match_real_public_enriched_api_contract(self):
        from shop_catalog_enriched import EnrichedPublicCatalog
        from shop_public_api import create_shop_public_blueprint
        from test_shop_catalog_public import wheel, part, PHOTO, NOW, variant_fingerprint
        rim = wheel(name='SYNTHETIC Rim Black')
        smaller = wheel('b', name='SYNTHETIC Rim Black', size='7.5 × 17 Zoll', diameter='17')
        tyre = wheel('c', selection_id='tyre:' + 'c' * 24, category='tyre',
                     name='SYNTHETIC Tyre', size='225/40 R18 92Y', width='225', ratio='40', season='Sommerreifen')
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
const product = {id, category:'tyre', purchase_status:'supplier_check_required', name:'SYNTHETIC tyre', size:'205/55 R16', images:[], price_status:'current', price:{
 currency:'EUR',vat_percent:'19.00',observed_at:'2026-11-17T11:59:00Z',expires_at:'2026-11-17T12:10:00Z',stock_reported:8,
 prices_by_quantity:{'1':{quantity:1,unit_gross_cent:12345,items_gross_cent:12345},'4':{quantity:4,unit_gross_cent:12000,items_gross_cent:48000}}}};
assert.equal(logic.currentPrice(product,4,now).total,48000);
const published={...product,price:{...product.price,source:'RSU',kind:'merchant_pricebook',
 observed_at:'2026-11-15T12:00:00Z',expires_at:'2026-11-22T12:00:00Z',
 stock_observed_at:'2026-11-15T12:00:00Z',stock_expires_at:'2026-11-15T12:15:00Z'}};
assert.equal(logic.currentPrice(published,4,now).total,48000);
assert.equal(logic.currentPrice(published,4,now).stockCurrent,false);
assert.equal(logic.currentPrice({...published,price:{...published.price,stock_reported:0}},4,now).total,48000);
assert.equal(logic.currentPrice({...published,price:{...published.price,source:'ALZURA'}},4,now),null);
assert.equal(logic.currentPrice({...published,price:{...published.price,kind:'supplier_quote'}},4,now),null);
assert.equal(logic.currentPrice({...published,price:{...published.price,expires_at:'2026-11-23T12:00:00Z'}},4,now),null);
assert.equal(logic.quotedPrice({...product,purchase_status:'inquiry_only'},4,now),null);
assert.equal(logic.currentPrice({...product,purchase_status:'inquiry_only'},4,now),null);
const pricedInquiry={...published,purchase_status:'inquiry_only',price:{...published.price,review_at:'2026-11-20T12:00:00Z'}};
const inquiryBefore=JSON.stringify(pricedInquiry);
assert.equal(logic.displayPrice(pricedInquiry,4,now).unit,12000);
assert.equal(logic.displayPrice(pricedInquiry,4,now).total,48000);
assert.equal(logic.displayPrice(pricedInquiry,4,now).stockCurrent,false);
assert.equal(logic.displayPrice(pricedInquiry,4,now).displayOnly,true);
assert.equal(logic.currentPrice(pricedInquiry,4,now),null);
assert.equal(logic.quotedPrice(pricedInquiry,4,now),null);
const inquiryRows=[{product:pricedInquiry,selection:{id,quantity:4,services:{}}}];
assert.equal(logic.cartOfferReady(inquiryRows),true,'the existing inquiry request remains available');
assert.equal(logic.cartSourceReady(inquiryRows),false);
assert.equal(logic.cartPurchaseFlow(inquiryRows,true),'inquiry');
assert.equal(JSON.stringify(pricedInquiry),inquiryBefore,'display path cannot change eligibility or the original price object');
for(const delta of [{source:'ALZURA'},{kind:'supplier_quote'},{currency:'USD'},{vat_percent:'7.00'},
 {review_at:'2026-11-21T12:00:00Z'},{stock_observed_at:'2026-11-18T12:00:00Z'},
 {stock_expires_at:'2026-11-15T12:16:00Z'},
 {prices_by_quantity:{'4':{quantity:4,unit_gross_cent:'12000',items_gross_cent:48000}}},
 {prices_by_quantity:{'4':{quantity:4,unit_gross_cent:-1,items_gross_cent:-4}}},
 {prices_by_quantity:{'4':{quantity:4,unit_gross_cent:NaN,items_gross_cent:NaN}}},
 {prices_by_quantity:{'4':{quantity:4,unit_gross_cent:12000,items_gross_cent:1}}}])
 assert.equal(logic.displayPrice({...pricedInquiry,price:{...pricedInquiry.price,...delta}},4,now),null);
assert.equal(logic.displayPrice({...product,purchase_status:'inquiry_only'},4,now),null,'raw short supplier quotes cannot be used as retained inquiry prices');
const partnerId='parts:'+'c'.repeat(24), partnerInquiry={...pricedInquiry,id:partnerId,category:'body_part',department:'parts',
 price:{...pricedInquiry.price,kind:'partner_inquiry_pricebook',source:'ALZURA',stock_current:false}};
const partnerBefore=JSON.stringify(partnerInquiry);
assert.equal(logic.displayPrice(partnerInquiry,4,now).total,48000);
assert.equal(logic.displayPrice(partnerInquiry,4,now).stockCurrent,false);
assert.equal(logic.currentPrice(partnerInquiry,4,now),null);
assert.equal(logic.quotedPrice(partnerInquiry,4,now),null);
const partnerRows=[{product:partnerInquiry,selection:{id:partnerId,quantity:4,services:{}}}];
assert.equal(logic.cartSourceReady(partnerRows),false);
assert.equal(logic.cartPurchaseFlow(partnerRows,true),'inquiry');
for(const delta of [{purchase_status:'supplier_check_required'},{department:'accessories'},{id:null},{category:'tyre'},{id,category:'tyre',department:'tyres'}]){
 assert.equal(logic.displayPrice({...partnerInquiry,...delta},4,now),null);
 assert.equal(logic.currentPrice({...partnerInquiry,...delta},4,now),null);
}
for(const delta of [{source:'RSU'},{currency:'USD'},{vat_percent:'7.00'},
 {review_at:'2026-11-21T12:00:00Z'},{expires_at:'2026-11-23T12:00:00Z'}])
 assert.equal(logic.displayPrice({...partnerInquiry,price:{...partnerInquiry.price,...delta}},4,now),null);
assert.equal(JSON.stringify(partnerInquiry),partnerBefore);
assert.equal(logic.currentPrice(product,4,Date.parse('2026-11-17T12:10:00Z')),null);
assert.equal(logic.currentPrice({...product,price_status:'update_required'},1,now),null);
assert.equal(logic.currentPrice({...product,price:{...product.price,currency:'USD'}},1,now),null);
assert.equal(logic.currentPrice({...product,price:{...product.price,stock_reported:3}},4,now),null);
assert.equal(logic.currentPrice({...product,price:{...product.price,prices_by_quantity:{'1':{quantity:1,unit_gross_cent:12345,items_gross_cent:1}}}},1,now),null);
const selected=logic.normaliseCart([{id,quantity:4,services:{mounting:true},net_price:'PRIVATE_CANARY',provider_id:'PRIVATE_CANARY',price:1}]);
assert.deepEqual(selected,[{id,quantity:4,services:{mounting:true}}]);
const rimId='alloy_wheel:'+'b'.repeat(24),group='set|'+rimId+'|'+id,pair=[{id:rimId,quantity:4,services:{},assembly_group:group},{id,quantity:4,services:{mounting:true,runflat:true},assembly_group:group}];
const exactDimensions={width:'8',diameter:'18',offset:'45',bolt_count:'5',pcd:'108',centre_bore:'63.4'};
const modelNote='Modellfoto. Größe und genaue Ausführung können von der Abbildung abweichen.';
assert.equal(logic.modelPhotoNote({category:'alloy_wheel',image_note:modelNote}),modelNote);
assert.equal(logic.modelPhotoNote({category:'steel_wheel',image_note:modelNote}),modelNote);
assert.equal(logic.modelPhotoNote({category:'alloy_wheel',caption:'RAW_PRIVATE_CAPTION',image_note:'Unreviewed caption'}),'');
assert.equal(logic.modelPhotoNote({category:'alloy_wheel',caption:modelNote}),'');
assert.equal(logic.modelPhotoNote({category:'tyre',image_note:modelNote}),'');
assert.deepEqual(logic.rimDimensions({category:'alloy_wheel',sale_dimensions:exactDimensions}),[
 ['Felgenbreite','8 Zoll'],['Durchmesser','18 Zoll'],['Einpresstiefe (ET)','45 mm'],['Lochzahl','5'],['Lochkreisdurchmesser','108 mm'],['Mittenloch','63,4 mm']]);
assert.equal(logic.rimDimensions({category:'steel_wheel',sale_dimensions:exactDimensions}).length,6);
assert.deepEqual(logic.rimDimensions({category:'alloy_wheel',geometry:exactDimensions}),[]);
assert.deepEqual(logic.rimDimensions({category:'tyre',sale_dimensions:exactDimensions}),[]);
for(const malformed of [{...exactDimensions,centre_bore:undefined},{...exactDimensions,pcd:'5x108'},
 {...exactDimensions,width:8},{...exactDimensions,offset:'<script>'},Object.create(exactDimensions)])
 assert.deepEqual(logic.rimDimensions({category:'alloy_wheel',sale_dimensions:malformed}),[]);
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
assert.equal(logic.buildQuery({width:'255',ratio:'40',diameter:'19'},'tyres',1).get('width'),'255');
assert.equal(logic.buildQuery({width:'255',ratio:'40',diameter:'19'},'tyres',1).get('ratio'),'40');
assert.equal(logic.buildQuery({diameter:'19'},'wheels',1).get('diameter'),'19');
assert.deepEqual(logic.vehicleQuery({make:' Audi ',model:'',year:''}),{make:'Audi'});
assert.deepEqual(logic.vehicleQuery({keys:'0588 / bht'}),{hsn:'0588',tsn:'BHT'});
assert.deepEqual(logic.vehicleQuery({keys:'0588BHT'}),{hsn:'0588',tsn:'BHT'});
assert.equal(logic.vehicleQuery({make:'',model:'A3'}),null);
assert.equal(logic.vehicleQuery({make:'Audi',year:'2016'}),null);
assert.equal(logic.buildQuery({group:'family'},'wheels',1).get('group'),'family');
assert.equal(logic.buildQuery({wheel_make:'MINI'},'wheels',1).get('group'),'family');
assert.equal(logic.buildQuery({wheel_make:'Audi',make:'Audi',model:'A3'},'wheels',1).has('group'),false);
assert.equal(logic.buildQuery({hsn:'0588',tsn:'BHT'},'wheels',1).has('group'),false);
assert.equal(logic.buildQuery({complete:'1'},'wheels',1).get('complete'),'1');
assert.equal(logic.buildQuery({complete:'true'},'wheels',1).has('complete'),false);
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
 const personal=await address('personal_offer');assert.equal(JSON.parse(personal.requests.find(r=>r.url.endsWith('/buyer/address')).options.body).delivery,null);
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
const source=fs.readFileSync(process.argv[2],'utf8');
const A='tyre:'+'a'.repeat(24),B='tyre:'+'b'.repeat(24);
const base=Date.parse('2026-11-17T12:00:00Z');
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function pending(id=A){return {id,category:'tyre',purchase_status:'supplier_check_required',name:id===A?'SYNTHETIC A':'SYNTHETIC B',images:[],size:'205/55 R16',price_status:'update_required',price:null};}
function priced(id=A,q=1,cents=12345){return {...pending(id),price_status:'current',price:{currency:'EUR',vat_percent:'19.00',observed_at:new Date(base-1000).toISOString(),expires_at:new Date(base+899000).toISOString(),stock_reported:20,prices_by_quantity:{[q]:{quantity:q,unit_gross_cent:cents,items_gross_cent:q*cents}}}};}
function page(item,flag,page=1){const value={items:[item],page,total:2,has_more:page===1};if(flag!==undefined)value.prices_refreshing=flag;return value;}
class Node{
 constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.dataset={};this.listeners={};this.textContent='';this.isConnected=true;this.hidden=false;this.open=false;this.urls=[];this.fields={};this.classList={toggle(){}};this.elements={namedItem:key=>walk(this).find(n=>n.name===key)||this.fields[key]||null};}
 append(...nodes){for(const node of nodes){node.parent=this;this.children.push(node);}}
 replaceChildren(...nodes){for(const n of this.children){n.parent=null;n.isConnected=false;}this.children=[];this.append(...nodes);}
 addEventListener(key,fn){this.listeners[key]=fn;}
 setAttribute(key,value){this[key]=value;}
 getAttribute(key){return this[key]??null;}
 scrollIntoView(){}
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
async function boot(handler,{mode='reifen',cart=[],visibility='visible',tiles=[]}={}){
 const clock=new Clock(),nodes={};
 const keys=['close-product','product-dialog','product-detail','shop-announcement',...(mode==='warenkorb'?['cart-items','cart-summary','cart-status','request-quote','quote-status','quote-review','checkout-button','checkout-note']:['catalog-products','catalog-status','previous-page','next-page','page-label','catalog-filters'])];
 for(const key of keys)nodes[key]=new Node();
 if(['felgen','komplettraeder'].includes(mode)){
   const form=nodes['catalog-filters'];
   const input=nodes['wheel-make']=new Node('input');
   input.name='wheel_make';input.value='';input.form=form;form.fields[input.name]=input;
   form.fields.q=new Node('input');form.fields.q.value='';
   const vehicle=nodes['wheel-vehicle']=new Node('select');vehicle.value='';form.fields.wheel_vehicle=vehicle;
   nodes['wheel-vehicle-field']=new Node('label');
   form.requestSubmit=()=>form.listeners.submit({preventDefault(){},currentTarget:form});
   nodes['wheel-make-options']=new Node('datalist');nodes['wheel-make-options'].options=nodes['wheel-make-options'].children;
   nodes['wheel-make-inquiry']=new Node('a');
 }
 nodes['product-dialog'].tagName='DIALOG';nodes['product-dialog'].append(nodes['product-detail']);
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:mode,department:['felgen','komplettraeder'].includes(mode)?'wheels':'tyres',quoteEnabled:'false',buyerEnabled:'false',completeUrl:'/shop/komplettraeder/',studioUrl:'/shop/felgenstudio/'};
 const calls=[],stored=[];
 const events={};
 const headerLink=new Node('a');headerLink.href='/shop/warenkorb/';
 const document={body,visibilityState:visibility,createElement:tag=>new Node(tag),getElementById:id=>nodes[id]||null,querySelector:selector=>selector==='a.shop-cart-link'?headerLink:({content:''}),querySelectorAll:selector=>selector==='.shop-category-tile img'?tiles:[],addEventListener:(event,fn)=>{events[event]=fn;}};
 const win={document,localStorage:{getItem:()=>JSON.stringify(cart),setItem:(key,value)=>stored.push(value)},setTimeout:(fn,delay)=>clock.set(fn,delay),clearTimeout:id=>clock.clear(id),setInterval(){},location:{search:'',pathname:'/shop/warenkorb/'},
  fetch:async(url,options)=>{calls.push({url,options});const result=await handler(url,calls.length);return {ok:true,text:async()=>JSON.stringify(result)};}};
 class FakeDate extends Date{constructor(...args){super(...(args.length?args:[clock.now]));}static now(){return clock.now;}}
 function FormData(form){return Object.entries(form.fields).map(([name,field])=>[name,field.value]);}
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date:FakeDate,FormData,Math:Object.assign(Object.create(Math),{random:()=>0}),console});
 await flush();return {clock,nodes,calls,stored,document,events,win};
}
(async()=>{
 // Missing/false warmup flags do not create background GET timers.
 for(const flag of [undefined,false]){const f=await boot(()=>page(pending(),flag));assert.equal(f.clock.jobs.size,0);assert.equal(f.calls.length,1);}
 // Unmapped raw prices are never displayed or usable, even in an old response.
 const marketProduct={...priced(),purchase_status:'inquiry_only',price_label:'Richtpreis'};
 const market=await boot(url=>url.includes('/products?')?page(marketProduct,false):{item:marketProduct,prices_refreshing:false});
 const marketCard=market.nodes['catalog-products'].children[0],marketPrice=find(marketCard,'shop-price');assert.equal(find(marketPrice,'shop-price-kind'),undefined);assert.ok(walk(marketPrice).some(n=>n.tagName==='STRONG'&&n.textContent==='Preis und Lieferbarkeit auf Anfrage'));assert.equal(walk(marketPrice).some(n=>n.textContent.includes('123,45')||n.textContent.includes('Richtpreis')||n.textContent==='20 Stück gemeldet'),false);
 const marketAdd=walk(marketCard).find(n=>n.tagName==='BUTTON'&&n.textContent==='Zur Anfrage hinzufügen');assert.ok(marketAdd);assert.notEqual(marketAdd.disabled,true);marketAdd.listeners.click();assert.deepEqual(JSON.parse(market.stored.at(-1)),[{id:A,quantity:1,services:{}}]);assert.equal(market.calls.some(r=>r.options.method==='POST'),false);
 find(marketCard,'shop-detail-link').listeners.click();await flush();assert.ok(walk(market.nodes['product-detail']).some(n=>n.tagName==='BUTTON'&&!n.disabled&&n.textContent==='Zur Anfrage hinzufügen'));assert.equal(walk(market.nodes['product-detail']).some(n=>n.textContent.includes('123,45')||n.textContent.includes('Richtpreis')),false);
 const bound=await boot(()=>page({...priced(),purchase_status:'supplier_check_required'},false));assert.equal(find(bound.nodes['catalog-products'],'shop-price-kind'),undefined);assert.ok(walk(bound.nodes['catalog-products']).some(n=>n.textContent==='In den Warenkorb'));
 const labelUrl='/shop/documents/'+'d'.repeat(64)+'.jpg',sheetUrl='/shop/documents/'+'e'.repeat(64)+'.pdf';
 const documented=await boot(()=>page({...priced(),product_documents:{tyre_label_url:labelUrl,tyre_sheet_url:sheetUrl,eprel_id:'12345'}},false));const labelButton=find(documented.nodes['catalog-products'],'shop-tyre-label');assert.ok(labelButton);assert.equal(labelButton.children[0].src,labelUrl);assert.ok(labelButton['aria-label'].includes('vergrößert ansehen'));assert.ok(walk(documented.nodes['catalog-products']).some(n=>n.tagName==='A'&&n.href===sheetUrl&&n.textContent==='Produktdatenblatt (PDF) ↗'));labelButton.listeners.click();const labelDialog=find(documented.document.body,'shop-document-dialog');assert.equal(labelDialog.open,true);assert.ok(walk(labelDialog).some(n=>n.tagName==='IMG'&&n.src===labelUrl));
 const badDocs=await boot(()=>page({...priced(),product_documents:{tyre_label_url:'https://foreign.test/private.jpg',tyre_sheet_url:'javascript:unsafe'}},false));assert.equal(find(badDocs.nodes['catalog-products'],'shop-tyre-label'),undefined);assert.ok(walk(badDocs.nodes['catalog-products']).some(n=>n.textContent==='EU-Reifenlabel und Produktdatenblatt werden noch geprüft.'));
 let marketReads=0;const marketPoll=await boot(()=>++marketReads===1?page({...pending(),purchase_status:'inquiry_only',price_status:'not_offered'},true):page(priced(),false));assert.equal(marketPoll.clock.jobs.size,0);assert.equal(marketPoll.calls.length,1);assert.ok(walk(marketPoll.nodes['catalog-products']).some(n=>n.textContent==='Preis und Lieferbarkeit auf Anfrage'));assert.equal(walk(marketPoll.nodes['catalog-products']).some(n=>n.textContent==='Preis wird aktualisiert'),false);
 const unofferedPart={...priced('parts:'+'d'.repeat(24)),category:'body_part',department:'parts',purchase_status:'inquiry_only',price_status:'current'};
 const parts=await boot(url=>url.includes('/products?')?page(unofferedPart,true):{item:unofferedPart,prices_refreshing:true},{mode:'ersatzteile'});assert.equal(parts.clock.jobs.size,0);const partsCard=parts.nodes['catalog-products'].children[0];assert.ok(walk(partsCard).some(n=>n.tagName==='BUTTON'&&!n.disabled&&n.textContent==='Zur Anfrage hinzufügen'));assert.equal(walk(partsCard).some(n=>n.textContent.includes('123,45')||n.textContent.includes('Richtpreis')),false);find(partsCard,'shop-detail-link').listeners.click();await flush();assert.equal(walk(parts.nodes['product-detail']).some(n=>n.textContent.includes('123,45')||n.textContent.includes('Richtpreis')),false);assert.equal(parts.clock.jobs.size,0);
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
 const marketCart=await boot(()=>({item:marketCartProduct,prices_refreshing:true}),{mode:'warenkorb',cart:[{id:A,quantity:2,services:{}}]});assert.equal(find(marketCart.nodes['cart-items'],'shop-price-kind'),undefined);assert.equal(find(marketCart.nodes['cart-summary'],'shop-summary-total').textContent,'Gesamtangebot auf Anfrage');assert.equal(walk(marketCart.nodes['cart-items']).some(n=>n.textContent.includes('88,88')||n.textContent.includes('Richtpreis')),false);assert.equal(marketCart.nodes['request-quote'].disabled,true);assert.equal(marketCart.clock.jobs.size,0);assert.deepEqual(JSON.parse(marketCart.stored.at(-1)),[{id:A,quantity:2,services:{}}]);assert.ok(walk(marketCart.nodes['cart-items']).some(n=>n.tagName==='BUTTON'&&n.textContent==='Entfernen'));
 // Opening and reviewing a mounting wish neither books nor alters the cart.
 const writesBefore=cart.stored.length,readsBefore=cart.calls.length;
 const appointment=walk(cartLine).find(n=>n.tagName==='BUTTON'&&n.textContent==='Montage auswählen oder ändern');assert.ok(appointment);appointment.listeners.click();
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
 const tile=new Node('img'),photo='/shop/media/synthetic.jpg',fallback='/static/homepage/leistungsfotos-v2/oelservice.jpg';
 tile.setAttribute('src',photo);tile.setAttribute('data-fallback-src',fallback);
 const home=await boot(()=>page(priced(),false),{mode:'home',tiles:[tile]});
 assert.equal(tile.src,photo);tile.onerror();assert.equal(tile.src,fallback);assert.equal(home.clock.jobs.size,0);
 tile.onload();assert.equal(tile.replacement,undefined);
 // Rim -> tyre -> mounting -> explicit atomic cart action; opening/searching adds nothing.
 const rimId='alloy_wheel:'+'c'.repeat(24),rim=(q=1)=>({...priced(rimId,q,10000),category:'alloy_wheel',name:'SYNTHETIC Rim',geometry:{diameter:'18'},tyre_seasons:['summer']});
 // Standalone rim selection shows only the server-reviewed six exact dimensions.
 const exactDimensions={width:'8',diameter:'18',offset:'45',bolt_count:'5',pcd:'108',centre_bore:'63.4'};
 const modelNote='Modellfoto. Größe und genaue Ausführung können von der Abbildung abweichen.';
 const standalone=await boot(url=>{const u=new URL('https://shop.test'+url),q=Number(u.searchParams.get('quantity')||1),item={...rim(q),purchase_status:'supplier_check_required',sale_dimensions:exactDimensions,image_note:modelNote,caption:'RAW_PRIVATE_CAPTION'};return u.pathname.endsWith('/products')?page(item,false):{item,prices_refreshing:false};},{mode:'felgen'});
 assert.ok(walk(standalone.nodes['catalog-products']).some(n=>n.tagName==='SMALL'&&n.textContent===modelNote));
 find(standalone.nodes['catalog-products'].children[0],'shop-detail-link').listeners.click();await flush();
 assert.ok(walk(find(standalone.nodes['product-detail'],'shop-gallery')).some(n=>n.tagName==='SMALL'&&n.textContent===modelNote));
 assert.equal(walk(standalone.nodes['catalog-products']).some(n=>n.textContent.includes('RAW_PRIVATE_CAPTION')),false);assert.equal(walk(standalone.nodes['product-detail']).some(n=>n.textContent.includes('RAW_PRIVATE_CAPTION')),false);
 assert.ok(walk(standalone.nodes['product-detail']).some(n=>n.textContent==='Genaue Felgenausführung'));
 for(const value of ['Felgenbreite','Durchmesser','Einpresstiefe (ET)','Lochzahl','Lochkreisdurchmesser','Mittenloch','63,4 mm'])assert.ok(walk(standalone.nodes['product-detail']).some(n=>n.textContent===value));
 assert.ok(walk(standalone.nodes['product-detail']).some(n=>n.textContent==='Felgen ohne Montage. Die Fahrzeugpassung wird damit nicht bestätigt.'));
 assert.ok(walk(standalone.nodes['product-detail']).some(n=>n.textContent.includes('kein Gutachten im Shop hinterlegt')));
 assert.deepEqual(JSON.parse(standalone.stored.at(-1)),[]);
 const standaloneQuantity=find(find(standalone.nodes['product-detail'],'shop-options'),'shop-quantity');standaloneQuantity.value='3';standaloneQuantity.listeners.change();await flush();
 walk(standalone.nodes['product-detail']).find(n=>n.tagName==='BUTTON'&&n.textContent==='Nur Felge in den Warenkorb').listeners.click();
 assert.deepEqual(JSON.parse(standalone.stored.at(-1)),[{id:rimId,quantity:3,services:{}}]);
 assert.ok(walk(standalone.nodes['product-detail']).some(n=>n.textContent==='63,4 mm'));
 const standaloneCart=await boot(()=>({item:{...rim(3),purchase_status:'supplier_check_required',sale_dimensions:exactDimensions},prices_refreshing:false}),{mode:'warenkorb',cart:[{id:rimId,quantity:3,services:{}}]});
 const cartDimensions=walk(standaloneCart.nodes['cart-items']).find(n=>n.textContent.startsWith('Ausführung: '));assert.ok(cartDimensions);
 for(const value of ['Felgenbreite: 8 Zoll','Durchmesser: 18 Zoll','Einpresstiefe (ET): 45 mm','Lochzahl: 5','Lochkreisdurchmesser: 108 mm','Mittenloch: 63,4 mm'])assert.ok(cartDimensions.textContent.includes(value));
 assert.ok(walk(standaloneCart.nodes['cart-items']).some(n=>n.textContent==='Felgen ohne Montage. Die Fahrzeugpassung wird damit nicht bestätigt.'));
 assert.deepEqual(JSON.parse(standaloneCart.stored.at(-1)),[{id:rimId,quantity:3,services:{}}]);
 const reportPath='/shop/documents/verified.pdf';
 const reviewedRim=q=>({...rim(q),fitment:{status:'reviewed_with_conditions',vehicles:[{make:'Audi',model:'A3',hsn:'0588',tsn:'BHT',conditions:['Auflage prüfen']}],reports:[reportPath]},product_documents:{rim_reports:[{scope:'provider_document_not_vehicle_approval',url:reportPath,report_number:'TEST-123'}]}});
 const complete=await boot(url=>{const u=new URL('https://shop.test'+url),q=Number(u.searchParams.get('quantity')||1);if(u.pathname.endsWith('/products'))return page(reviewedRim(q),false);if(u.pathname.endsWith('/compatible-tyres'))return {rim_id:rimId,items:[priced(A,q,3000)],scope:'approved',prices_refreshing:false};return {item:reviewedRim(q),prices_refreshing:false};},{mode:'komplettraeder'});
 function configureCard(fixture) {
   const buttons=walk(fixture.nodes['catalog-products'].children[0]).filter(n=>n.tagName==='BUTTON');
   assert.ok(!buttons.some(n=>n.textContent==='In den Warenkorb'));
   return buttons.find(n=>n.textContent==='Komplettrad zusammenstellen').listeners.click();
 }
 assert.equal(new URL('https://shop.test'+complete.calls[0].url).searchParams.get('complete'),'1');
 assert.equal(new URL('https://shop.test'+standalone.calls[0].url).searchParams.has('complete'),false);
 configureCard(complete);await flush();
 const reportArea=find(complete.nodes['product-detail'],'shop-fitment shop-rim-fitment');
 assert.ok(reportArea && !reportArea.closest('details'),'the report must be visible before tyre selection');
 assert.ok(walk(reportArea).some(n=>n.textContent==='Audi A3'));
 assert.ok(walk(reportArea).some(n=>n.textContent.includes('0588/BHT')));
 assert.ok(walk(reportArea).some(n=>n.textContent==='Auflage prüfen'));
 const reportLinks=walk(reportArea).filter(n=>n.tagName==='A');
 assert.equal(reportLinks.length,1);assert.equal(reportLinks[0].href,reportPath);
 const completeForm=find(complete.nodes['product-detail'],'shop-options'),completeQty=find(completeForm,'shop-quantity');completeQty.value='4';completeQty.listeners.change();await flush();
 const bundle=find(complete.nodes['product-detail'],'shop-bundle-panel'),search=walk(bundle).find(n=>n.tagName==='BUTTON'&&n.textContent.startsWith('Passende Reifen'));
 await search.listeners.click();assert.deepEqual(JSON.parse(complete.stored.at(-1)),[]);
 const tyre=walk(bundle).find(n=>n.name==='bundle_tyre');tyre.checked=true;tyre.listeners.change();
 const mounting=walk(bundle).find(n=>n.name==='bundle_mounting');mounting.checked=true;mounting.listeners.change();
 const add=walk(bundle).find(n=>n.tagName==='BUTTON'&&n.textContent==='Felgen + Reifen in den Warenkorb');assert.equal(add.disabled,false);add.listeners.click();
 const pair=JSON.parse(complete.stored.at(-1));assert.equal(pair.length,2);assert.deepEqual(pair.map(row=>row.quantity),[4,4]);assert.equal(pair[0].id,rimId);assert.equal(pair[1].id,A);assert.equal(pair[0].assembly_group,pair[1].assembly_group);assert.deepEqual(pair[0].services,{});assert.deepEqual(pair[1].services,{mounting:true});
 assert.equal(complete.calls.some(call=>call.options.method!==undefined),false);
 // A winter-only documented set must not offer a summer dead end.
 const winterOnly=await boot(url=>{const u=new URL('https://shop.test'+url),item={...rim(),tyre_seasons:['winter','allseason','unreviewed']};if(u.pathname.endsWith('/products'))return page({...item,family:{variant_count:2,diameters:['18']}},false);if(u.pathname.endsWith('/variants'))return {items:[item],page:1,total:1,has_more:false};if(u.pathname.endsWith('/compatible-tyres'))return {rim_id:rimId,items:[priced(A,1,3000)],scope:'rim_width_candidate',prices_refreshing:false};return {item,prices_refreshing:false};},{mode:'komplettraeder'});
 configureCard(winterOnly);await flush();
 assert.ok(winterOnly.calls.some(call=>call.url.includes('/variants?')&&new URL('https://shop.test'+call.url).searchParams.get('complete')==='1'));
 walk(winterOnly.nodes['product-detail']).find(n=>n.tagName==='BUTTON'&&n.textContent==='Reifen zu dieser Felge').listeners.click();await flush();
 const winterBundle=find(winterOnly.nodes['product-detail'],'shop-bundle-panel'),winterSeason=walk(winterBundle).find(n=>n.tagName==='SELECT');assert.deepEqual(winterSeason.children.map(n=>n.value),['winter','allseason']);assert.equal(winterSeason.value,'winter');
 await walk(winterBundle).find(n=>n.tagName==='BUTTON'&&n.textContent.startsWith('Passende Reifen')).listeners.click();assert.ok(winterOnly.calls.some(call=>call.url.includes('/compatible-tyres?')&&new URL('https://shop.test'+call.url).searchParams.get('season')==='winter'));assert.ok(walk(winterBundle).some(n=>n.name==='bundle_tyre'));
 const noCandidates=await boot(url=>new URL('https://shop.test'+url).pathname.endsWith('/products')?page({...rim(),tyre_seasons:[]},false):{item:{...rim(),tyre_seasons:[]},prices_refreshing:false},{mode:'felgen'});
 find(noCandidates.nodes['catalog-products'].children[0],'shop-detail-link').listeners.click();await flush();const emptyBundle=find(noCandidates.nodes['product-detail'],'shop-bundle-panel'),emptySearch=walk(emptyBundle).find(n=>n.tagName==='BUTTON'&&n.textContent.startsWith('Passende Reifen'));assert.equal(emptySearch.disabled,true);await emptySearch.listeners.click();assert.equal(noCandidates.calls.some(call=>call.url.includes('/compatible-tyres')),false);
 // Candidate cards remain selectable during a price warmup. Updating that
 // exact selected tyre must unlock the explicit pair action without reselecting.
 const delayedPair=await boot(url=>{const u=new URL('https://shop.test'+url),q=Number(u.searchParams.get('quantity')||1);if(u.pathname.endsWith('/products'))return page({...rim(),purchase_status:'supplier_check_required'},false);if(u.pathname.endsWith('/compatible-tyres'))return {rim_id:rimId,items:[{...pending(),purchase_status:'supplier_check_required'}],scope:'rim_width_candidate',prices_refreshing:true};if(decodeURIComponent(u.pathname).endsWith('/products/'+A))return {item:{...priced(A,q,3000),purchase_status:'supplier_check_required'},prices_refreshing:false};return {item:{...rim(q),purchase_status:'supplier_check_required'},prices_refreshing:false};},{mode:'komplettraeder'});
 configureCard(delayedPair);await flush();
 const delayedQty=find(find(delayedPair.nodes['product-detail'],'shop-options'),'shop-quantity');delayedQty.value='4';delayedQty.listeners.change();await flush();
 const delayedBundle=find(delayedPair.nodes['product-detail'],'shop-bundle-panel'),delayedSearch=walk(delayedBundle).find(n=>n.tagName==='BUTTON'&&n.textContent.startsWith('Passende Reifen'));
 await delayedSearch.listeners.click();const delayedRadio=walk(delayedBundle).find(n=>n.name==='bundle_tyre');assert.ok(delayedRadio);delayedRadio.checked=true;delayedRadio.listeners.change();
 const delayedAdd=walk(delayedBundle).find(n=>n.tagName==='BUTTON'&&n.textContent==='Felgen + Reifen zur Prüfung in den Warenkorb');assert.equal(delayedAdd.disabled,true);assert.deepEqual(JSON.parse(delayedPair.stored.at(-1)),[]);
 await delayedPair.clock.next();assert.equal(delayedRadio.checked,true);assert.equal(delayedAdd.disabled,false);assert.ok(walk(delayedBundle).some(n=>n.textContent==='Größen-/Breitenvorschlag: Fahrzeugfreigabe, Tragfähigkeit und Auflagen sind noch zu prüfen.'));delayedAdd.listeners.click();assert.deepEqual(JSON.parse(delayedPair.stored.at(-1)).map(row=>row.quantity),[4,4]);
 const unofferedBundle=await boot(url=>{const u=new URL('https://shop.test'+url);if(u.pathname.endsWith('/products'))return page(rim(),false);if(u.pathname.endsWith('/compatible-tyres'))return {rim_id:rimId,items:[{...priced(A,1,3000),purchase_status:'inquiry_only'}],scope:'rim_width_candidate',prices_refreshing:true};return {item:rim(),prices_refreshing:false};},{mode:'komplettraeder'});
 configureCard(unofferedBundle);await flush();const unavailablePanel=find(unofferedBundle.nodes['product-detail'],'shop-bundle-panel');await walk(unavailablePanel).find(n=>n.tagName==='BUTTON'&&n.textContent.startsWith('Passende Reifen')).listeners.click();const unavailableRadio=walk(unavailablePanel).find(n=>n.name==='bundle_tyre');unavailableRadio.checked=true;unavailableRadio.listeners.change();const unavailableAdd=walk(unavailablePanel).find(n=>n.tagName==='BUTTON'&&n.textContent==='Felgen + Reifen zur Anfrage hinzufügen');assert.equal(unavailableAdd.disabled,false);unavailableAdd.listeners.click();assert.equal(JSON.parse(unofferedBundle.stored.at(-1)).length,2);assert.equal(unofferedBundle.calls.some(r=>r.options.method==='POST'),false);assert.equal(walk(unavailablePanel).some(n=>n.textContent.includes('30,00')||n.textContent.includes('Richtpreis')),false);assert.equal(unofferedBundle.clock.jobs.size,1);assert.equal([...unofferedBundle.clock.jobs.values()][0].delay,4500);
 // Family cards require an exact article choice before any cart action.
 const family=await boot(url=>{const u=new URL('https://shop.test'+url);if(u.pathname.endsWith('/products'))return page({...rim(),family:{variant_count:2,diameters:['17','18']}},false);if(u.pathname.endsWith('/variants'))return {items:[rim()],page:1,total:1,has_more:false};return {item:rim()};},{mode:'felgen'});
 const input=family.nodes['wheel-make'];input.value='Au';input.listeners.input({currentTarget:input});
 input.value='Audi';input.listeners.input({currentTarget:input});await family.clock.next();
 assert.equal(new URL('https://shop.test'+family.calls.at(-1).url).searchParams.get('wheel_make'),'Audi');
 const familyCard=family.nodes['catalog-products'].children[0];assert.equal(find(familyCard,'shop-quantity'),undefined);find(familyCard,'shop-detail-link').listeners.click();await flush();
 assert.ok(family.calls.some(call=>call.url.includes('/variants?')&&call.url.includes('page_size=24')&&call.url.includes('wheel_make=Audi')));assert.deepEqual(JSON.parse(family.stored.at(-1)),[]);
 const exact=walk(family.nodes['product-detail']).find(n=>n.tagName==='BUTTON'&&n.textContent==='Diese Felge ansehen');exact.listeners.click();await flush();assert.ok(find(family.nodes['product-detail'],'shop-bundle-panel'));
 // Free-text vehicle scope survives selection, the exact detail and the season lookup.
 const vehicleKey='e'.repeat(24), proof={key:vehicleKey,make:'MINI',model:'Cooper',variant:'UKL-L',has_report:true};
 const scoped=await boot(url=>{
   const u=new URL('https://shop.test'+url);
   if(u.pathname.endsWith('/products'))return {...page(rim(),false),wheel_query_make:u.searchParams.has('q')?'MINI':undefined,wheel_vehicles:[proof]};
   if(u.pathname.endsWith('/compatible-tyres'))return {rim_id:rimId,scope:'blocked',items:[]};
   return {item:rim(),prices_refreshing:false};
 },{mode:'komplettraeder'});
 const scopedForm=scoped.nodes['catalog-filters'];scopedForm.fields.q.value='MINI Cooper';scopedForm.requestSubmit();await flush();
 const selector=scoped.nodes['wheel-vehicle'];
 assert.ok(walk(selector).some(n=>n.value===vehicleKey && n.textContent.includes('mit Gutachten')));
 selector.value=vehicleKey;scopedForm.listeners.change({currentTarget:scopedForm,target:{name:'wheel_vehicle'}});await flush();
 configureCard(scoped);await flush();
 for(const suffix of [encodeURIComponent(rimId),'/compatible-tyres']){
   const url=scoped.calls.map(call=>new URL('https://shop.test'+call.url)).find(u=>u.pathname.endsWith(suffix));
   assert.equal(url.searchParams.get('wheel_query'),'MINI Cooper');assert.equal(url.searchParams.get('wheel_vehicle'),vehicleKey);
   assert.equal(url.searchParams.get('wheel_make'),'MINI');
 }
 assert.ok(walk(scoped.nodes['product-detail']).some(n=>n.textContent==='Passung dieser Felge anfragen'));
 assert.ok(!walk(scoped.nodes['product-detail']).some(n=>n.textContent==='Nur Felge in den Warenkorb'));
 for(const fixture of [f,bounded,changed,quantity,tiers,detail,closed,cart,hidden,paused,pausedTier,images,standalone,standaloneCart,complete,winterOnly,noCandidates,delayedPair,unofferedBundle,family,market,parts,bound,marketPoll,marketCart,documented,badDocs])assert.ok(fixture.calls.every(call=>call.options.method===undefined&&call.url.startsWith('/shop/api/v2/products')),'read-only product calls only');
 console.log('Bounded price/image polling passed');
})().catch(error=>{console.error(error);process.exit(1)});
'''
        result = subprocess.run(['node', '-', str(ROOT / 'static/shop_v2/shop.js')],
                                input=code, capture_output=True, encoding='utf-8', timeout=30)
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
