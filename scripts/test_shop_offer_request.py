"""Explicit editable draft transfer only; no real lead, email or form POST."""
from pathlib import Path
from html.parser import HTMLParser
import json
import subprocess
import sys
import unittest

from jinja2 import Environment, select_autoescape

ROOT = Path(__file__).resolve().parents[1]


class OfferDraftChecks(unittest.TestCase):
    def test_public_selection_bounded_prefill_and_no_automatic_submission(self):
        code = r'''
const assert=require('node:assert/strict'),S=require(process.argv[1]),F=require(process.argv[2]);
const now=Date.now(),tyre='tyre:'+'a'.repeat(24),body='parts:'+'b'.repeat(24),oil='parts:'+'c'.repeat(24);
function item(id,name,quantity,services={}){return {selection:{id,quantity,services},product:{id,name,size:'SYNTHETIC size',specifications:{Ausführung:'SYNTHETIC exact variant'},provider_id:'PRIVATE_CANARY',identity_hash:'PRIVATE_CANARY',net_price:'PRIVATE_CANARY',price:{unit_gross_cent:12345}}};}
const value=S.buildOfferRequest([item(tyre,'SYNTHETIC tyre',4,{mounting:true}),item(body,'SYNTHETIC body',1,{painting:true,paint_color:'LY9C'}),item(oil,'SYNTHETIC oil',2)],now);
assert.equal(value.position_count,3);assert.equal(value.omitted_count,0);assert.ok(value.message.length<=2000);assert.ok(value.message.includes('4 × SYNTHETIC tyre'));assert.ok(value.message.includes('Lackierung: LY9C'));assert.ok(value.message.includes('2 × SYNTHETIC oil'));assert.ok(value.message.includes('Werkstatt Mosbach'));assert.ok(value.message.includes('Lieferziel angefragt: Kunde'));
for(const hidden of ['PRIVATE_CANARY',tyre,body,oil,'unit_gross_cent','12345'])assert.equal(JSON.stringify(value).includes(hidden),false);
const long=S.buildOfferRequest(Array.from({length:40},(_,i)=>item('parts:'+i.toString(16).padStart(24,'0'),'SYNTHETIC '+('long '.repeat(40)),1,{painting:true,paint_color:'SYNTHETIC COLOUR'})),now);
assert.ok(long.message.length<=2000);assert.ok(long.full_text.length<=32000);assert.ok(long.omitted_count>0);assert.ok(long.message.includes('Die vollständige Auswahl wird beim expliziten Senden der Anfrage mitgesendet'));assert.equal(long.message.includes('nachreichen'),false);assert.equal(long.position_count,40);
assert.equal(S.buildOfferRequest([]),null);assert.equal(S.buildOfferRequest([null]),null);
assert.equal(F.draft({...value,created_at:now-31*60*1000},now),null);assert.equal(F.draft({...value,created_at:now+1},now),null);assert.equal(F.draft({...value,message:'x'.repeat(2001)},now),null);
for(const invalid of [{...value,price:1},{...value,full_text:'\ud800'},{...value,full_text:'x\x85y'},{...value,full_text:'ü'.repeat(17000)},{...value,position_count:true},{...value,full_text:' '}])assert.equal(F.draft(invalid,now),null);
assert.ok(F.draft({...value,full_text:'SYNTHETIC 🚗'},now));
class Node{constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.value='';}append(...items){this.children.push(...items);}setAttribute(key,value){this[key]=value;}}
function boot(payload,{search='?anliegen=allgemein&shop_anfrage=1',existing='',retained,marker='1',method='post',enabled='true',concern='allgemein'}={}){
 const parent=new Node('div'),form=new Node('form'),field=new Node('textarea');form.method=method;form.dataset={shopOfferForm:enabled};form.submit=()=>{throw Error('No submission');};form.requestSubmit=form.submit;field.parentElement=parent;field.value=existing;field.closest=()=>form;
 const elements={anliegen:{value:concern}};if(retained!==undefined){elements.shop_anfrage={value:marker};elements.shop_auswahl={value:retained};}form.elements={namedItem:name=>elements[name]||form.children.find(n=>n.name===name)||null};
 let reads=0;const win={location:{pathname:'/anfrage',search},URLSearchParams,sessionStorage:{getItem:key=>{assert.equal(key,'gaertner-shop-offer-request-v1');reads++;return JSON.stringify(payload);}},document:{getElementById:key=>key==='nachricht'?field:null,createElement:tag=>new Node(tag)},fetch(){throw Error('No network');}};
 const mounted=F.mount(win);return {mounted,parent,field,reads,form,win};
}
const normal=boot(value);assert.equal(normal.mounted,true);assert.equal(normal.field.value,value.message);assert.ok(normal.parent.children[0].textContent.includes('Noch nicht übermittelt'));
assert.equal(normal.form.children.length,2);assert.deepEqual(normal.form.children.map(n=>[n.name,n.type]),[['shop_anfrage','hidden'],['shop_auswahl','hidden']]);assert.equal(normal.form.elements.namedItem('shop_anfrage').value,'1');assert.deepEqual(JSON.parse(normal.form.elements.namedItem('shop_auswahl').value),value);assert.equal(F.mount(normal.win),false);assert.equal(normal.form.children.length,2);
const untouched=boot(value,{existing:'Existing customer draft'});assert.equal(untouched.field.value,'Existing customer draft');assert.ok(untouched.parent.children.some(n=>n.tagName==='DETAILS'));
const oversized=boot(long);assert.equal(oversized.mounted,true);assert.ok(oversized.parent.children.some(n=>n.textContent?.includes('Die vollständige Auswahl wird mit Ihrer Anfrage mitgesendet')));const readonly=oversized.parent.children.find(n=>n.tagName==='DETAILS').children.find(n=>n.tagName==='TEXTAREA');assert.equal(readonly.readOnly,true);assert.equal(readonly.name,undefined);assert.equal(readonly.value,long.full_text);assert.deepEqual(JSON.parse(oversized.form.elements.namedItem('shop_auswahl').value),long);
const submitted={...long,created_at:now-24*60*60*1000},retainedRaw=JSON.stringify(submitted);const errorPage=boot(value,{search:'',existing:'Customer edited message',retained:retainedRaw});assert.equal(errorPage.mounted,true);assert.equal(errorPage.reads,0);assert.equal(errorPage.field.value,'Customer edited message');assert.equal(errorPage.form.elements.namedItem('shop_auswahl').value,retainedRaw);assert.equal(errorPage.form.children.length,0);assert.equal(errorPage.parent.children.find(n=>n.tagName==='DETAILS').children.find(n=>n.tagName==='TEXTAREA').value,long.full_text);
const deliberatelyEmpty=boot(value,{search:'',existing:'',retained:retainedRaw});assert.equal(deliberatelyEmpty.mounted,true);assert.equal(deliberatelyEmpty.field.value,'');assert.equal(deliberatelyEmpty.reads,0);
for(const retained of ['not JSON',JSON.stringify({...value,full_text:'\ud800'}),JSON.stringify({...value,price:1})]){const invalid=boot(value,{retained});assert.equal(invalid.mounted,false);assert.equal(invalid.reads,0);assert.equal(invalid.form.elements.namedItem('shop_auswahl').value,retained);}
for(const options of [{method:'get'},{enabled:'false'},{concern:'reifenservice'}]){const other=boot(value,options);assert.equal(other.mounted,false);assert.equal(other.reads,0);assert.equal(other.form.children.length,0);}
const inactive=boot(value,{search:'?anliegen=allgemein'});assert.equal(inactive.mounted,false);assert.equal(inactive.reads,0);assert.equal(inactive.field.value,'');
console.log('Explicit offer draft transfer passed');console.log(JSON.stringify(JSON.parse(oversized.form.elements.namedItem('shop_auswahl').value)));
'''
        result = subprocess.run(['node', '-e', code, str(ROOT/'static/shop_v2/shop.js'),
                                 str(ROOT/'static/shop_v2/offer-request.js')], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Explicit offer draft transfer passed', result.stdout)
        # The actual hidden payload round-trips through the strict server parser;
        # all 40 lines survive even when only a short message fits the textarea.
        sys.path.insert(0, str(ROOT))
        from shop_service_requests import parse_shop_offer_context
        payload = json.loads(result.stdout.splitlines()[-1])
        parsed = parse_shop_offer_context({'anliegen': 'allgemein', 'shop_anfrage': '1',
                                          'shop_auswahl': json.dumps(payload, ensure_ascii=False)})
        self.assertEqual(parsed['full_text'], payload['full_text'])
        self.assertEqual(parsed['position_count'], 40)
        self.assertTrue(parsed['requires_manual_review'])
        self.assertFalse(parsed['quote_confirmed'])

    def test_actual_template_preserves_escaped_submitted_context_without_query(self):
        source = (ROOT/'templates/homepage_anfrage.html').read_text(encoding='utf-8-sig')
        start = source.index('<form class="form-card"', source.index('{% elif not kurzanfrage %}'))
        fragment = source[start:source.index('</form>', start)+len('</form>')]
        environment = Environment(autoescape=select_autoescape(default_for_string=True))
        environment.globals.update(url_for=lambda endpoint, **kwargs: '/anfrage', csrf_field=lambda: '<input name="csrf_token" value="SYNTHETIC-CSRF">')
        payload = json.dumps({'version': 1, 'created_at': 1700000000000,
            'message': 'SYNTHETIC message', 'full_text': 'SYNTHETIC <script>not executable</script> & "quoted" body',
            'position_count': 40, 'omitted_count': 38}, ensure_ascii=False)

        class Inputs(HTMLParser):
            def __init__(self):
                super().__init__(convert_charrefs=True)
                self.inputs, self.forms = [], []
            def handle_starttag(self, tag, attrs):
                if tag == 'input':
                    self.inputs.append(dict(attrs))
                elif tag == 'form':
                    self.forms.append(dict(attrs))

        def render(formdata):
            return environment.from_string(fragment).render(formdata=formdata, errors=['SYNTHETIC contact error'],
                anliegen_optionen={'allgemein': 'Allgemein'}, kurzanfrage_optionen={}, besichtigungsarten={},
                bilder_max_anzahl=5, bilder_max_datei_mb=5, bilder_max_gesamt_mb=20)

        formdata = {'anliegen': 'allgemein', 'shop_anfrage': '1', 'shop_auswahl': payload,
                    'nachricht': 'Customer edited message <keep>', 'name': 'SYNTHETIC'}
        rendered = render(formdata)
        parser = Inputs(); parser.feed(rendered)
        hidden = {item['name']: item['value'] for item in parser.inputs if item.get('name') in {'shop_anfrage', 'shop_auswahl'}}
        self.assertEqual(hidden, {'shop_anfrage': '1', 'shop_auswahl': payload})
        self.assertTrue(all(item.get('type') == 'hidden' for item in parser.inputs if item.get('name') in hidden))
        self.assertEqual(parser.forms[0]['method'], 'post')
        self.assertEqual(parser.forms[0]['action'], '/anfrage')
        self.assertEqual(parser.forms[0]['data-shop-offer-form'], 'true')
        self.assertIn('Customer edited message &lt;keep&gt;', rendered)
        self.assertNotIn('<script>not executable</script>', rendered)
        plain = Inputs(); plain.feed(render({'anliegen': 'allgemein'}))
        self.assertFalse(any(item.get('name') in {'shop_anfrage', 'shop_auswahl'} for item in plain.inputs))


if __name__ == '__main__':
    unittest.main()
