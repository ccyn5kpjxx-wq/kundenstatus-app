"""Anonymous existing-counter integration with a fake browser and no HTTP."""
from html.parser import HTMLParser
from pathlib import Path
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
    def __init__(self, source):
        super().__init__()
        self.scripts = []
        self.feed(source)

    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            self.scripts.append(dict(attrs))


class AnalyticsChecks(unittest.TestCase):
    def test_every_shop_shell_uses_same_origin_external_counter_script(self):
        app = Flask(__name__, root_path=str(ROOT), template_folder=str(ROOT / 'templates'), static_folder=str(ROOT / 'static'))
        app.config['SHOP_FRONTEND_ENABLED'] = True
        app.register_blueprint(create_shop_frontend_blueprint())
        with patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden')):
            for key in ('', 'felgen', 'reifen', 'komplettraeder', 'ersatzteile', 'zubehoer', 'service', 'warenkorb', 'felgenstudio'):
                with self.subTest(page=key):
                    response = app.test_client().get('/shop/' + (key + '/' if key else ''))
                    self.assertEqual(response.status_code, 200)
                    scripts = Markup(response.get_data(as_text=True)).scripts
                    ours = [attrs for attrs in scripts if attrs.get('src') == '/static/shop_v2/analytics.js']
                    self.assertEqual(len(ours), 1)
                    self.assertIn('defer', ours[0])
                    self.assertTrue(all(attrs.get('src', '').startswith('/static/') for attrs in scripts))
                    self.assertIn("connect-src 'self'", response.headers['Content-Security-Policy'])

    def test_counter_script_reads_no_sensitive_shop_data_or_new_tracking_storage(self):
        source = (ROOT / 'static/shop_v2/analytics.js').read_text(encoding='utf-8')
        for expression in ('localStorage', 'sessionStorage', 'document.cookie', 'FormData', '.value', '.files',
                           'textContent', 'innerHTML', 'location.search', 'location.hash', 'dataset.order',
                           'sendBeacon', 'gtag(', 'googletagmanager'):
            self.assertNotIn(expression, source)
        self.assertIn("credentials:'omit'", source)
        self.assertIn("referrerPolicy:'no-referrer'", source)

    def test_synthetic_browser_counts_only_fixed_actions_and_redacts_all_user_values(self):
        code = r'''
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const source=fs.readFileSync(process.argv[1],'utf8');
class Element{
  constructor(matches={}){this.matches=matches;this.id='';}
  closest(selector){return this.matches[selector]||null;}
  get value(){throw Error('Sensitive field value read');}
  get files(){throw Error('Photo input read');}
  get textContent(){throw Error('Product/customer text read');}
}
function boot(changes={}){
 const calls=[],listeners={},loads={};
 const location={pathname:'/shop/felgen/',origin:'https://shop.example.test'};
 const document={body:{dataset:{page:'felgen'}},readyState:'complete',
   referrer:'https://search.example.test/private-customer?email=PRIVATE_CANARY#secret',
   addEventListener:(kind,fn)=>{listeners[kind]=fn;}};
 const navigator={webdriver:false};
 const win={location,fetch:(url,options)=>{calls.push({url,options,payload:JSON.parse(options.body)});return Promise.resolve({status:204});},
   addEventListener:(kind,fn)=>{loads[kind]=fn;}};
 changes({document,navigator,win,location});
 const ctx=vm.createContext({window:win,document,navigator,Element,URL,Promise});
 vm.runInContext(source,ctx);
 return{calls,listeners,loads,ctx,win,document,navigator};
}
const b=boot(()=>{});
assert.equal(b.calls.length,1);assert.equal(b.calls[0].url,'/api/besucher');
assert.deepEqual(b.calls[0].payload,{website:'auto-lackierzentrum',seite:'/shop/felgen',referrer:'search.example.test'});
assert.equal(b.calls[0].options.credentials,'omit');assert.equal(b.calls[0].options.referrerPolicy,'no-referrer');
assert.equal(b.calls[0].options.mode,'same-origin');assert.equal(b.calls[0].options.keepalive,true);
vm.runInContext(source,b.ctx);assert.equal(b.calls.length,1);
function emit(kind,matches={},trusted=true){const target=new Element(matches);b.listeners[kind]({target,isTrusted:trusted});return target;}
const cases=[['#request-quote','quote-request'],['#checkout-button','checkout-start'],
 ['.shop-card-media, .shop-card .shop-detail-link','product-view'],['.shop-family-controls button','variant-view'],
 ['.shop-card-controls button, .shop-detail-copy > button.shop-button','cart-add'],
 ['.shop-cart-controls button','cart-remove'],['.shop-bundle-panel > button:first-of-type','matching-tyres'],
 ['.shop-bundle-panel > button:last-of-type','cart-add'],['.shop-mounting-action button','appointment-open'],
 ['.shop-mounting-dialog a','appointment-form'],['.shop-fitment a','document-open'],
 ['.shop-detail-picture, #ws-canvas, #ws-enlarge','image-zoom']];
for(const [selector,code] of cases){emit('click',{[selector]:new Element()});assert.equal(b.calls.at(-1).payload.shop_click,code);}
const before=b.calls.length;emit('click',{'#request-quote':new Element()},false);
emit('click',{'button:disabled':new Element(),'#checkout-button':new Element()});assert.equal(b.calls.length,before);
const form=new Element();form.id='vehicle-filters';emit('submit',{'form':form});assert.equal(b.calls.at(-1).payload.shop_click,'vehicle-filter');
form.id='catalog-filters';emit('submit',{'form':form});assert.equal(b.calls.at(-1).payload.shop_click,'catalogue-filter');
form.id='buyer-address';const beforeAddress=b.calls.length;emit('submit',{'form':form});assert.equal(b.calls.length,beforeAddress);
emit('change',{'.shop-card-controls select, .shop-cart-controls select, .shop-options select:not([name])':new Element()});
assert.equal(b.calls.at(-1).payload.shop_click,'quantity-change');
emit('change',{'.shop-bundle-option input[name="bundle_tyre"]':new Element()});assert.equal(b.calls.at(-1).payload.shop_click,'tyre-choice');
const beforeUnknown=b.calls.length;emit('change');emit('click');assert.equal(b.calls.length,beforeUnknown);
const link=new Element();link.getAttribute=()=>'/shop/reifen/?email=PRIVATE_CANARY#PRIVATE_CANARY';
emit('click',{'a[href]':link});assert.equal(b.calls.at(-1).payload.shop_click,'nav-reifen');
link.getAttribute=()=>'#sortiment';const beforeAnchor=b.calls.length;emit('click',{'a[href]':link});assert.equal(b.calls.length,beforeAnchor);
link.getAttribute=()=>'https://evil.example/shop/reifen/';emit('click',{'a[href]':link});assert.equal(b.calls.length,beforeAnchor);
for(const call of b.calls.slice(1))assert.deepEqual(Object.keys(call.payload).sort(),['seite','shop_click','website']);
assert.ok(!JSON.stringify(b.calls).includes('PRIVATE_CANARY'));
const late=boot(({document})=>{document.readyState='loading';});assert.equal(late.calls.length,0);late.loads.load();late.loads.load();assert.equal(late.calls.length,1);
const inherited=boot(({win})=>{win.__gaertnerBesucherErfasst=true;});assert.equal(inherited.calls.length,0);assert.ok(inherited.listeners.click);
for(const changes of [({navigator})=>{navigator.webdriver=true;},({navigator})=>{navigator.doNotTrack='1';},
 ({navigator})=>{navigator.globalPrivacyControl=true;},({document,location})=>{document.body.dataset.page='order_status';location.pathname='/shop/orders/sho_PRIVATE_CANARY/success';},
 ({location})=>{location.pathname='/unreviewed-path/';}]){const disabled=boot(changes);assert.equal(disabled.calls.length,0);assert.deepEqual(Object.keys(disabled.listeners),[]);}
const bounded=boot(()=>{});for(let i=0;i<300;i++)bounded.listeners.click({target:new Element({'#request-quote':new Element()}),isTrusted:true});assert.equal(bounded.calls.length,200);
console.log('Synthetic anonymous counter checks passed');
'''
        result = subprocess.run(['node', '-e', code, str(ROOT / 'static/shop_v2/analytics.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
