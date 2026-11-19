"""Semantic fake-DOM checkout/inquiry transitions; every HTTP response is local."""
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


class CartFlowChecks(unittest.TestCase):
    def test_explicit_inquiry_direct_checkout_and_mutation_states(self):
        code = r'''
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const path=process.argv[1],source=fs.readFileSync(path,'utf8'),logic=require(path);
const A='tyre:'+'a'.repeat(24),B='alloy_wheel:'+'b'.repeat(24),C='parts:'+'c'.repeat(24);
const expiry=new Date(Date.now()+600000).toISOString(),hash='d'.repeat(64);
const fullKey='gaertner-shop-v2-selection', prefix='gaertner-shop-v2-separate:', token='11111111-1111-4111-8111-111111111111', orderId='sho_'+'1'.repeat(32);
function product(id=A,status='supplier_check_required'){
 const category=id.startsWith('tyre:')?'tyre':id.startsWith('alloy_wheel:')?'alloy_wheel':'body_part';
 return {id,category,name:'SYNTHETIC '+(category==='body_part'?'Tür Ölpflege':'Artikel'),size:'205/55 R18',images:[],purchase_status:status,price_status:'current',price:{currency:'EUR',vat_percent:'19.00',observed_at:new Date(Date.now()-1000).toISOString(),expires_at:expiry,stock_reported:20,prices_by_quantity:Object.fromEntries([1,2,3,4].map(q=>[q,{quantity:q,unit_gross_cent:10000,items_gross_cent:q*10000}]))}};
}
const selection=(id=A,services={},quantity=1)=>({id,quantity,services});
const item=(id=A,status='supplier_check_required',services={})=>({product:product(id,status),selection:selection(id,services)});
assert.equal(logic.cartPurchaseFlow([item()]),'checkout');
assert.equal(logic.cartPurchaseFlow([item(A,'supplier_check_required',{mounting:true})]),'checkout');
assert.equal(logic.cartPurchaseFlow([item(),item(A,'supplier_check_required',{runflat:true})]),'checkout');
assert.equal(logic.cartPurchaseFlow([item(),item(A,'supplier_check_required',{mounting:true})]),'checkout');
assert.equal(logic.cartPurchaseFlow([item(),item(B)]),'checkout');
assert.equal(logic.cartSourceReady([item(),item(B)]),true);
assert.equal(logic.cartPurchaseFlow([item(C,'inquiry_only')]),'inquiry');
assert.equal(logic.cartOfferReady([item(C,'inquiry_only')]),true);
assert.equal(logic.cartSourceReady([item(C,'inquiry_only')]),false);
for(const bad of [[],[null],[{product:null,selection:selection()}],[{product:product(B),selection:selection()}],[{product:{...product(),purchase_status:'unknown'},selection:selection()}],[{product:product(),selection:selection(A,{},0)}]])assert.equal(logic.cartPurchaseFlow(bad),'unavailable');
assert.equal(logic.cartPurchaseFlow([{product:{...product(),id:Symbol('bad')},selection:selection()}]),'unavailable');
class Node{
 constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.fields={};this.listeners={};this.dataset={};this.textContent='';this.hidden=false;this.disabled=false;this.isConnected=true;this.classList={toggle(){}};this.elements={namedItem:key=>walk(this).find(n=>n.name===key)||this.fields[key]||(this.fields[key]=new Node('input'))};}
 append(...nodes){for(const node of nodes){node.parent=this;this.children.push(node);}}
 replaceChildren(...nodes){this.children=[];this.append(...nodes);}
 addEventListener(name,fn){this.listeners[name]=fn;}setAttribute(name,value){this[name]=value;}
 scrollIntoView(){this.scrolled=true;}focus(){this.focused=true;}replaceWith(){}showModal(){this.open=true;}close(){this.open=false;}
}
function walk(node){return[node,...node.children.flatMap(walk)];}
const flush=()=>new Promise(resolve=>setImmediate(resolve));
async function boot(lines,{unbound=false,address=true,blocked=false,missing=false,deferBuyer=false,deferQuote=false,quoteFailures=0,omitLast=false,storage=new Map(),search='',canCheckout=false,returned=null,features={}}={}){
 const nodes={};for(const key of ['close-product','product-dialog','product-detail','shop-announcement','cart-items','cart-summary','cart-status','request-quote','quote-status','quote-review','checkout-button','checkout-note','buyer-address','address-status','delivery-fields','cart-workflow-note','cart-separate','order-status','order-summary'])nodes[key]=new Node();
 nodes['checkout-button'].hidden=true;nodes['buyer-address'].hidden=true;
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:'warenkorb',buyerEnabled:'true',quoteEnabled:'true',orderEnabled:'true',checkoutEnabled:'true',consumerEnabled:'true',cartRequestUrl:'/anfrage?anliegen=allgemein&shop_anfrage=1'};
 Object.assign(body.dataset,features);
 if(returned){body.dataset.page='order_status';body.dataset.orderId=orderId;delete nodes['cart-items'];}
 if(!storage.has(fullKey))storage.set(fullKey,JSON.stringify(lines));
 const calls=[],drafts=[],redirects=[];let releaseBuyer,releaseQuote;
 const pendingBuyer=deferBuyer?new Promise(resolve=>releaseBuyer=resolve):null,pendingQuote=deferQuote?new Promise(resolve=>releaseQuote=resolve):null;
 let amount=0;
 const win={document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key]||null,querySelector:()=>({content:''}),querySelectorAll:()=>[],addEventListener(){}},
 localStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value)},sessionStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>{storage.set(key,value);drafts.push({key,value});},removeItem:key=>storage.delete(key)},
 location:{pathname:'/shop/warenkorb/',search,assign:url=>redirects.push(url)},crypto:{randomUUID:()=>token},setTimeout(){},clearTimeout(){},setInterval(){},
 fetch:async(url,options)=>{calls.push({url,options});let value;
  if(url.endsWith('/buyer/session')){if(pendingBuyer)await pendingBuyer;value={guest:true,address_ready:address,csrf_token:'SYNTHETIC_TOKEN_12345678901234567890',expires_at:expiry};}
  else if(url.includes('/products/')){const id=decodeURIComponent(url.split('/products/')[1].split('?')[0]),p=product(id,unbound||id===C?'inquiry_only':'supplier_check_required');if(id===B)p.sale_dimensions={width:'8',diameter:'18',offset:'40',bolt_count:'5',pcd:'112',centre_bore:'70'};value={item:missing?null:p,prices_refreshing:false};}
  else if(url.endsWith('/buyer/address'))value={address_ready:true,csrf_token:'SYNTHETIC_TOKEN_12345678901234567890'};
  else if(url.endsWith('/cart/quote')){
    if(pendingQuote)await pendingQuote;
    if(quoteFailures>0){quoteFailures--;throw Error('service_unavailable');}
    const items=JSON.parse(options.body).lines.map(({id,quantity})=>({id,name:product(id).name,quantity,items_gross_cent:10000*quantity,services:[]}));
    if(omitLast)items.pop();
    amount=items.reduce((sum,item)=>sum+item.items_gross_cent+821,0);
    const readyQuote={ready:true,currency:'eur',quote_hash:hash,total_gross_cent:amount,expires_at:expiry,lines:items,shipments:items.map(()=>({destination:'customer',gross_cent:821}))};
    value={quote:blocked?{...readyQuote,ready:false,total_gross_cent:null,known_subtotal_gross_cent:amount,blocked_reasons:[{code:'freight_unconfirmed'}]}:readyQuote,quote_retained:!blocked};
  }
  else if(url.endsWith('/consumer/terms'))value={version:'SYNTHETIC',pack_hash:'b'.repeat(64),consumer_only:true,texts:{agb:'Terms',widerruf:'Withdrawal',widerrufsformular:'Form',lieferung:'Delivery',datenschutz:'Privacy'}};
  else if(canCheckout&&url.endsWith('/consumer/acceptance'))value={accepted:true,quote_hash:hash,pack_hash:'b'.repeat(64),accepted_at:new Date().toISOString()};
  else if(canCheckout&&url.endsWith('/orders'))value={order:{id:orderId,quote_hash:hash,amount_gross_cent:amount},payment_enabled:true};
  else if(canCheckout&&url.endsWith('/checkout'))value={checkout:{order_id:orderId,currency:'eur',amount_gross_cent:amount,checkout_url:'https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC',capture_triggered:false,supplier_order_created:false}};
  else if(returned&&url.endsWith('/orders/'+orderId))value={order:{id:orderId,mode:'test',amount_gross_cent:10000,quote_hash:hash,...returned}};
  else throw new Error('Unexpected request '+url);
  return {ok:true,text:async()=>JSON.stringify(value)};}};
 class FormData{constructor(form){this.form=form;}get(key){return this.form.elements.namedItem(key).value||null;}}
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date,console,FormData});await flush();await flush();
 return {nodes,calls,drafts,redirects,storage,releaseBuyer,releaseQuote};
}
const writes=f=>f.calls.filter(c=>c.options.body!=null),press=f=>f.nodes['request-quote'].listeners.click();
(async()=>{
 const rows=[selection(A),selection(B),selection(C)];
 const main=await boot(rows),options=walk(main.nodes['cart-separate']).filter(n=>n.tagName==='BUTTON');
 assert.equal(options.length,1,'one checkout for all orderable items');assert.equal(options[0].textContent,'Artikel zusammen kaufen');
 options[0].listeners.click();
 assert.deepEqual(main.redirects,['/shop/warenkorb/?auswahl='+token]);assert.equal(writes(main).length,0);
 assert.deepEqual(JSON.parse(main.storage.get(fullKey)),rows,'entering separate checkout preserves the original');
 const partialArgs={storage:main.storage,search:'?auswahl='+token,canCheckout:true};
 const solo=await boot(rows,partialArgs);
 assert.equal(solo.nodes['cart-items'].children.length,2);assert.equal(solo.nodes['buyer-address'].hidden,false);
 assert.ok(walk(solo.nodes['cart-separate']).some(n=>n.href==='/shop/warenkorb/'));
 assert.equal(walk(solo.nodes['cart-items']).filter(n=>n.tagName==='SELECT'||n.textContent==='Entfernen').length,0);
 await press(solo);assert.equal(writes(solo).length,0,'explicitly confirm the delivery address for this separate order');
 solo.nodes['buyer-address'].elements.namedItem('delivery_salutation').value='Frau';
 await solo.nodes['buyer-address'].listeners.submit({preventDefault(){}});
 const outgoing=solo.calls.find(c=>c.url.endsWith('/cart/quote'));assert.deepEqual(JSON.parse(outgoing.options.body),{lines:rows.slice(0,2)});
 assert.ok(walk(solo.nodes['quote-review']).some(n=>n.textContent===logic.money(21642)));
 assert.equal(solo.nodes['checkout-button'].disabled,true);
 const consent=walk(solo.nodes['quote-review']).filter(n=>n.type==='checkbox');assert.equal(consent.length,2);
 for(const n of consent){n.checked=true;n.listeners.change();}
 await solo.nodes['checkout-button'].listeners.click();
 assert.deepEqual(solo.redirects,['https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC']);
 assert.deepEqual(JSON.parse(main.storage.get(fullKey)),rows,'provider navigation alone is not success');
 for(const status of ['payment_pending','payment_failed','cancelled']){
   await boot(rows,{storage:main.storage,returned:{status}});assert.deepEqual(JSON.parse(main.storage.get(fullKey)),rows);
 }
 await boot(rows,{storage:main.storage,returned:{status:'paid',quote_hash:'f'.repeat(64)}});
 assert.deepEqual(JSON.parse(main.storage.get(fullKey)),rows,'different quote never removes the row');
 await boot(rows,{storage:main.storage,returned:{status:'paid'}});
 assert.deepEqual(JSON.parse(main.storage.get(fullKey)),[rows[2]],'only the inquiry-only part remains');
 await boot(rows,{storage:main.storage,returned:{status:'paid'}});assert.deepEqual(JSON.parse(main.storage.get(fullKey)),[rows[2]],'repeated return is idempotent');
 const gone=await boot(rows,partialArgs);assert.equal(gone.nodes['request-quote'].disabled,true);assert.equal(gone.nodes['cart-items'].children.length,0);
 for(const search of ['?auswahl=','?auswahl=wrong','?auswahl='+token]){
   const f=await boot(rows,{search});assert.equal(f.nodes['request-quote'].disabled,true);assert.equal(f.nodes['buyer-address'].hidden,true);assert.deepEqual(JSON.parse(f.storage.get(fullKey)),rows);
 }
 const changed=await boot(rows);walk(changed.nodes['cart-separate']).find(n=>n.tagName==='BUTTON').listeners.click();
 const staleSolo=await boot(rows,{storage:changed.storage,search:'?auswahl='+token});
 changed.storage.set(fullKey,JSON.stringify([selection(A,{},2),...rows.slice(1)]));await press(staleSolo);
 assert.equal(writes(staleSolo).length,0);assert.equal(staleSolo.nodes['request-quote'].disabled,true);
 const set=[{...selection(B),assembly_group:'pair'},{...selection(A,{mounting:true}),assembly_group:'pair'}];
 const withSet=await boot([...set,rows[2]],{features:{deferredCompleteWheelsEnabled:'true'}}),groupButtons=walk(withSet.nodes['cart-separate']).filter(n=>n.tagName==='BUTTON');
 assert.equal(groupButtons.length,1);assert.equal(groupButtons[0].textContent,'Mit Montage zur Kasse');groupButtons[0].listeners.click();assert.deepEqual(JSON.parse(withSet.storage.get(prefix+token)),set);
 const noWheelCheckout=await boot([...set,rows[2]]);assert.equal(noWheelCheckout.nodes['cart-separate'].hidden,true);
 const previews=await boot(rows,{features:{checkoutEnabled:'false'}});assert.equal(previews.nodes['cart-separate'].hidden,true);
 const pair='set|'+B+'|'+A;
 const pairRows=[{...selection(B,{},2),selection_pair:pair},{...selection(A,{},2),selection_pair:pair}];
 const threePlusInquiry=[...pairRows,selection('tyre:'+'e'.repeat(24)),rows[2]];
 const many=await boot(threePlusInquiry),manyButtons=walk(many.nodes['cart-separate']).filter(n=>n.tagName==='BUTTON');
 assert.equal(manyButtons.length,1);manyButtons[0].listeners.click();assert.deepEqual(JSON.parse(many.storage.get(prefix+token)),threePlusInquiry.slice(0,3));
 const duplicate=await boot([...pairRows,selection(A)]);await press(duplicate);
 assert.deepEqual(JSON.parse(duplicate.calls.find(c=>c.url.endsWith('/cart/quote')).options.body).lines,[selection(B,{},2),selection(A,{},3)]);
 const overLimit=await boot([...pairRows,selection(A,{},3)]);await press(overLimit);
 assert.equal(overLimit.calls.some(c=>c.url.endsWith('/cart/quote')),false);
 assert.ok(overLimit.nodes['quote-status'].textContent.includes('bestellbare Menge'));
 const mixed=await boot(pairRows,{address:false,canCheckout:true});
 assert.ok(walk(mixed.nodes['cart-items']).some(n=>n.textContent==='Anzahl je Artikel'));
 assert.ok(walk(mixed.nodes['cart-items']).some(n=>n.textContent==='2 Reifen zur gewählten Felge'));
 assert.equal(mixed.nodes['request-quote'].textContent,'Gemeinsam zur Kasse');assert.equal(mixed.nodes['request-quote'].disabled,false);assert.equal(mixed.nodes['buyer-address'].hidden,false);
 assert.equal(mixed.nodes['cart-separate'].hidden,true,'no separate checkout buttons');
 await press(mixed);assert.equal(writes(mixed).length,0);
 await mixed.nodes['buyer-address'].listeners.submit({preventDefault(){}});
 const mixedQuote=mixed.calls.find(c=>c.url.endsWith('/cart/quote'));
 assert.deepEqual(JSON.parse(mixedQuote.options.body).lines,[selection(B,{},2),selection(A,{},2)],'no assembly or montage added to unmounted pair');
 assert.ok(walk(mixed.nodes['quote-review']).some(n=>n.textContent===logic.money(41642)));
 assert.match(mixed.nodes['checkout-note'].textContent,/Gesamtbetrag einmal/);
 assert.doesNotMatch(mixed.nodes['checkout-note'].textContent,/montage|termin/i,'no service promised for loose wheels');
 const tyreMounting=await boot([selection(A,{mounting:true})]);await press(tyreMounting);
 assert.match(tyreMounting.nodes['checkout-note'].textContent,/Montagetermin/);
 for(const n of walk(mixed.nodes['quote-review']).filter(n=>n.type==='checkbox')){n.checked=true;n.listeners.change();}
 await mixed.nodes['checkout-button'].listeners.click();
 assert.equal(mixed.calls.filter(c=>c.url.endsWith('/orders')).length,1);assert.equal(mixed.calls.filter(c=>c.url.endsWith('/checkout')).length,1);
 assert.deepEqual(mixed.redirects,['https://checkout.stripe.com/c/pay/cs_test_SYNTHETIC']);
 const editedAfterPayment=new Map(mixed.storage),changedRow={...pairRows[0],quantity:3};
 editedAfterPayment.set(fullKey,JSON.stringify([changedRow,pairRows[1],rows[2]]));
 await boot(pairRows,{storage:editedAfterPayment,returned:{status:'paid'}});
 assert.deepEqual(JSON.parse(editedAfterPayment.get(fullKey)),[changedRow,rows[2]],'keep subsequent edits and remove independently completed rows');
 mixed.storage.set(fullKey,JSON.stringify([...pairRows,rows[2]]));
 await boot(pairRows,{storage:mixed.storage,returned:{status:'authorized'}});assert.deepEqual(JSON.parse(mixed.storage.get(fullKey)),[rows[2]]);
 const incomplete=await boot(rows.slice(0,2),{omitLast:true});await press(incomplete);assert.equal(incomplete.nodes['checkout-button'].hidden,true);
 assert.equal(incomplete.calls.some(c=>c.url.includes('/consumer/')),false,'missing quote line cannot become payable');
 const inquiry=await boot([selection(C)],{unbound:true,address:false});
 assert.equal(inquiry.nodes['request-quote'].disabled,false);assert.equal(inquiry.nodes['buyer-address'].hidden,true);await inquiry.nodes['buyer-address'].listeners.submit({preventDefault(){}});assert.equal(writes(inquiry).length,0);
 await press(inquiry);assert.equal(writes(inquiry).length,0);assert.equal(inquiry.redirects.length,1);assert.ok(JSON.parse(inquiry.drafts[0].value).full_text.includes('Tür Ölpflege'));
 const absent=await boot([selection()],{missing:true});assert.equal(absent.nodes['request-quote'].disabled,true);await press(absent);assert.equal(writes(absent).length,0);assert.equal(absent.redirects.length,0);
 const noAddress=await boot([selection()],{address:false}), form=noAddress.nodes['buyer-address'];
 assert.equal(form.hidden,false);assert.equal(noAddress.nodes['request-quote'].disabled,false);assert.equal(noAddress.nodes['request-quote'].textContent,'Zur Kasse');
 await press(noAddress);assert.equal(writes(noAddress).length,0);assert.equal(form.scrolled,true);assert.equal(form.elements.namedItem('name').focused,true);
 assert.equal(form.elements.namedItem('delivery_salutation').required,true);form.elements.namedItem('delivery_salutation').value='Frau';
 const submit=f=>f.nodes['buyer-address'].listeners.submit({preventDefault(){}});
 await Promise.all([submit(noAddress),submit(noAddress)]);
 assert.deepEqual(writes(noAddress).map(c=>c.url),['/shop/api/v2/buyer/address','/shop/api/v2/cart/quote']);
 assert.equal(noAddress.nodes['quote-review'].scrolled,true);assert.equal(noAddress.nodes['checkout-button'].hidden,false);assert.equal(noAddress.nodes['checkout-button'].disabled,true);
 const retry=await boot([selection()],{address:false,quoteFailures:1});await submit(retry);
 assert.equal(retry.nodes['request-quote'].textContent,'Erneut prüfen');assert.equal(retry.nodes['request-quote'].disabled,false);assert.ok(retry.nodes['quote-status'].textContent);
 await press(retry);assert.equal(retry.nodes['checkout-button'].hidden,false);assert.equal(retry.calls.filter(c=>c.url.endsWith('/buyer/address')).length,1);
 const direct=await boot([selection()]);assert.equal(direct.nodes['request-quote'].textContent,'Gesamtpreis prüfen');assert.equal(direct.nodes['buyer-address'].hidden,false);await press(direct);
 assert.equal(direct.calls.filter(c=>c.url.endsWith('/cart/quote')).length,1);assert.equal(direct.nodes['checkout-button'].hidden,false);assert.equal(direct.nodes['checkout-button'].disabled,true);
 await direct.nodes['checkout-button'].listeners.click();assert.equal(direct.calls.some(c=>c.url.endsWith('/orders')||c.url.endsWith('/checkout')||c.url.endsWith('/consumer/acceptance')),false);
 const manual=await boot([selection()],{blocked:true});await press(manual);assert.equal(manual.nodes['request-quote'].textContent,'Erneut prüfen');assert.ok(manual.nodes['quote-status'].textContent.includes('Noch keine Zahlung'));assert.equal(manual.nodes['checkout-button'].hidden,true);
 assert.equal(walk(manual.nodes['quote-review']).filter(n=>n.tagName==='BUTTON').length,1);assert.equal(walk(manual.nodes['cart-summary']).filter(n=>n.tagName==='BUTTON').length,0);
 const qty=walk(manual.nodes['cart-items']).find(n=>n.tagName==='SELECT');qty.value='2';qty.listeners.change();await flush();await flush();
 assert.equal(manual.nodes['request-quote'].textContent,'Gesamtpreis prüfen');assert.equal(manual.nodes['quote-review'].children.length,0);assert.equal(manual.nodes['checkout-button'].hidden,true);assert.equal(manual.nodes['checkout-note'].textContent.includes('Prüfung erneut starten'),false);
 const removable=await boot([selection(),selection(C)]);assert.equal(removable.nodes['request-quote'].textContent,'Gesamtangebot anfragen');
 walk(removable.nodes['cart-items'].children[1]).find(n=>n.textContent==='Entfernen').listeners.click();await flush();await flush();assert.equal(removable.nodes['request-quote'].textContent,'Gesamtpreis prüfen');assert.equal(removable.nodes['buyer-address'].hidden,false);assert.equal(removable.nodes['request-quote'].disabled,false);assert.equal(writes(removable).length,0);
 const stale=await boot([selection()],{deferQuote:true});const request=press(stale);walk(stale.nodes['cart-items']).find(n=>n.textContent==='Entfernen').listeners.click();await flush();stale.releaseQuote();await request;assert.equal(stale.nodes['checkout-button'].hidden,true);assert.equal(stale.calls.some(c=>c.url.endsWith('/consumer/terms')),false);
 const lateSession=await boot([selection()],{deferBuyer:true});lateSession.nodes['buyer-address'].listeners.input();lateSession.releaseBuyer();await flush();await flush();assert.equal(lateSession.nodes['request-quote'].textContent,'Zur Kasse');await press(lateSession);assert.equal(writes(lateSession).length,0);
 const forty=Array.from({length:40},(_,i)=>selection('parts:'+i.toString(16).padStart(24,'0'),{painting:true,paint_color:'RAL 9005'},1));
 const full=await boot(forty,{unbound:true,address:false});await press(full);assert.equal(writes(full).length,0);const payload=JSON.parse(full.drafts[0].value);
 assert.deepEqual(Object.keys(payload).sort(),['created_at','full_text','message','omitted_count','position_count','version']);assert.equal(payload.position_count,40);assert.ok(payload.omitted_count>0);assert.ok(payload.message.length<=2000);assert.ok(payload.full_text.length<=32000);assert.ok(payload.full_text.includes('40. 1 × SYNTHETIC Tür Ölpflege'));assert.ok(payload.full_text.includes('RAL 9005'));assert.ok(payload.message.includes('vollständige Auswahl'));assert.equal(Buffer.from(payload.full_text,'utf8').toString('utf8'),payload.full_text);
 console.log('Cart inquiry/direct transitions passed');
})().catch(error=>{console.error(error);process.exit(1)});
'''
        result = subprocess.run(['node', '-e', code, str(ROOT / 'static/shop_v2/shop.js')],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Cart inquiry/direct transitions passed', result.stdout)


if __name__ == '__main__':
    unittest.main()
