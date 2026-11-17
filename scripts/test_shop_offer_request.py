"""Explicit editable draft transfer only; no real lead, email or form POST."""
from pathlib import Path
import subprocess
import unittest

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
assert.ok(long.message.length<=2000);assert.ok(long.full_text.length<=32000);assert.ok(long.omitted_count>0);assert.ok(long.message.includes('passen nicht in dieses Formular'));assert.equal(long.position_count,40);
assert.equal(S.buildOfferRequest([]),null);assert.equal(S.buildOfferRequest([null]),null);
assert.equal(F.draft({...value,created_at:now-31*60*1000},now),null);assert.equal(F.draft({...value,created_at:now+1},now),null);assert.equal(F.draft({...value,message:'x'.repeat(2001)},now),null);
class Node{constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.value='';}append(...items){this.children.push(...items);}setAttribute(key,value){this[key]=value;}}
function boot(payload,{search='?anliegen=allgemein&shop_anfrage=1',existing=''}={}){
 const parent=new Node('div'),form={method:'post',submit(){throw Error('No submission');},requestSubmit(){throw Error('No submission');}},field=new Node('textarea');field.parentElement=parent;field.value=existing;field.closest=()=>form;
 let reads=0;const win={location:{pathname:'/anfrage',search},URLSearchParams,sessionStorage:{getItem:key=>{assert.equal(key,'gaertner-shop-offer-request-v1');reads++;return JSON.stringify(payload);}},document:{getElementById:key=>key==='nachricht'?field:null,createElement:tag=>new Node(tag)},fetch(){throw Error('No network');}};
 const mounted=F.mount(win);return {mounted,parent,field,reads};
}
const normal=boot(value);assert.equal(normal.mounted,true);assert.equal(normal.field.value,value.message);assert.ok(normal.parent.children[0].textContent.includes('Noch nicht übermittelt'));
const untouched=boot(value,{existing:'Existing customer draft'});assert.equal(untouched.field.value,'Existing customer draft');assert.ok(untouched.parent.children.some(n=>n.tagName==='DETAILS'));
const oversized=boot(long);assert.ok(oversized.parent.children.some(n=>n.textContent?.includes('nicht vollständig')));const readonly=oversized.parent.children.find(n=>n.tagName==='DETAILS').children.find(n=>n.tagName==='TEXTAREA');assert.equal(readonly.readOnly,true);assert.equal(readonly.name,undefined);assert.equal(readonly.value,long.full_text);
const inactive=boot(value,{search:'?anliegen=allgemein'});assert.equal(inactive.mounted,false);assert.equal(inactive.reads,0);assert.equal(inactive.field.value,'');
console.log('Explicit offer draft transfer passed');
'''
        result = subprocess.run(['node', '-e', code, str(ROOT/'static/shop_v2/shop.js'),
                                 str(ROOT/'static/shop_v2/offer-request.js')], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Explicit offer draft transfer passed', result.stdout)


if __name__ == '__main__':
    unittest.main()
