/* Offline synthetic DOM + endpoint adapters; never connects to a provider. */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const path = process.argv[2], source = fs.readFileSync(path, 'utf8'), logic = require(path);
const RIM = 'alloy_wheel:' + 'a'.repeat(24), TYRE = 'tyre:' + 'b'.repeat(24), OTHER = 'parts:' + 'c'.repeat(24);
const HASH = 'd'.repeat(64), PACK = 'e'.repeat(64), TOKEN = 'SYNTHETIC_TOKEN_12345678901234567890';
const dimensions = {width:'8',diameter:'18',offset:'45',bolt_count:'5',pcd:'108',centre_bore:'63.4'};
function product(id, category) {
  return {id,category,name:'SYNTHETIC ' + category,size:category === 'tyre' ? '225/40 R18 92 Y XL' : '8 × 18 Zoll',images:[],
    sale_dimensions:category === 'alloy_wheel' ? {...dimensions} : undefined,purchase_status:'supplier_check_required',price_status:'current',
    price:{currency:'EUR',vat_percent:'19.00',stock_reported:20,observed_at:new Date(Date.now()-1000).toISOString(),expires_at:new Date(Date.now()+500000).toISOString(),
      prices_by_quantity:Object.fromEntries([1,2,3,4].map(quantity => [quantity,{quantity,unit_gross_cent:10000,items_gross_cent:quantity*10000}]))}};
}
const rim = () => product(RIM,'alloy_wheel'), tyre = () => product(TYRE,'tyre');
const selected = () => [{id:RIM,quantity:4,services:{},assembly_group:'SYNTHETIC_1'}, {id:TYRE,quantity:4,services:{mounting:true},assembly_group:'SYNTHETIC_1'}];
const items = () => selected().map(selection=>({selection,product:selection.id===RIM?rim():tyre()}));
const requirements = () => [{code:'mixed_cart_manual_review_required'}, {code:'fitment_unconfirmed',product_id:RIM}, {code:'assembly_fitment_unconfirmed',assembly_group:'SYNTHETIC_1'}];
const quote = () => ({version:2,ready:true,authorization_ready:true,fulfillment_ready:false,blocked_reasons:[],review_requirements:requirements(),currency:'eur',quote_hash:HASH,total_gross_cent:89000,
  expires_at:new Date(Date.now()+300000).toISOString(),lines:[{id:RIM,name:'SYNTHETIC RIM',quantity:4,items_gross_cent:40000,services:[]}, {id:TYRE,name:'SYNTHETIC TYRE',quantity:4,items_gross_cent:40000,services:[{code:'mounting',gross_cent:9000}]}],shipments:[{destination:'workshop',gross_cent:0}]});
function changed(fn) { const value=items(); fn(value); return value; }
assert.equal(logic.cartPurchaseFlow(items()),'inquiry');
for (const flag of [undefined,false,'true',1]) assert.equal(logic.cartPurchaseFlow(items(),flag),'inquiry');
assert.equal(logic.cartPurchaseFlow(items(),true),'checkout');
for (const mutate of [
  rows=>delete rows[0].selection.assembly_group,
  rows=>rows[1].selection.quantity=3,
  rows=>rows[1].selection.services.mounting=false,
  rows=>rows[0].selection.services.mounting=true,
  rows=>rows[1].selection.services.installation=true,
  rows=>rows[0].product.sale_dimensions.diameter='19',
  rows=>delete rows[0].product.sale_dimensions,
  rows=>rows[1].product.price_status='update_required',
  rows=>rows[1].product.purchase_status='inquiry_only',
  rows=>rows[1].product.price.stock_reported=2,
  rows=>rows.push({product:product(OTHER,'body_part'),selection:{id:OTHER,quantity:1,services:{}}})
]) assert.equal(logic.completeWheelGroups(changed(mutate)),null);
assert.equal(logic.reviewQuoteReady(quote(),items(),true),true);
for (const flag of [false,undefined,'true',1]) assert.equal(logic.reviewQuoteReady(quote(),items(),flag),false);
for (const mutate of [
  q=>q.authorization_ready='true', q=>q.fulfillment_ready=true, q=>q.version='2', q=>q.ready=false,
  q=>q.blocked_reasons=[{code:'freight_unconfirmed'}], q=>q.review_requirements=[],
  q=>q.review_requirements.push({...q.review_requirements[0]}),
  q=>q.review_requirements.push({code:'supplier_order_authorized'}),
  q=>q.review_requirements[1].product_id=TYRE,
  q=>q.review_requirements[1].approved=true,
  q=>q.review_requirements[2].assembly_group='FOREIGN',
  q=>q.lines[1].quantity=3,
  q=>q.lines[1].id=OTHER,
  q=>q.lines.push({...q.lines[1]})
]) { const value=quote(); mutate(value); assert.equal(logic.reviewQuoteReady(value,items(),true),false); }
const four = items(), second = items(), RIM2='alloy_wheel:'+'f'.repeat(24); second[0].selection.assembly_group=second[1].selection.assembly_group='SYNTHETIC_2';second[0].selection.id=second[0].product.id=RIM2;
four.push(...second); const multi=quote(), secondLines=quote().lines;secondLines[0].id=RIM2;multi.lines.push(...secondLines);multi.review_requirements.push({code:'assembly_fitment_unconfirmed',assembly_group:'SYNTHETIC_2'},{code:'fitment_unconfirmed',product_id:RIM2});
assert.equal(logic.reviewQuoteReady(multi,four,true),true,'same requirement code for another group is distinct');
assert.equal(logic.vehicleReference('  Audi A3 2016  '),'Audi A3 2016');assert.equal(logic.vehicleReference(''),'');
for(const bad of [null,7,'x'.repeat(201),'Audi\nA3','Audi\u007fA3'])assert.equal(logic.vehicleReference(bad),null);

class Node {
 constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.fields={};this.listeners={};this.dataset={};this.textContent='';this.value='';this.type='text';this.checked=false;this.hidden=false;this.disabled=false;this.isConnected=true;this.classList={toggle(){}};this.elements={namedItem:key=>this.fields[key]||null};}
 append(...nodes){this.children.push(...nodes);}replaceChildren(...nodes){this.children=[...nodes];}addEventListener(key,fn){this.listeners[key]=fn;}
 setAttribute(key,value){this[key]=value;}removeAttribute(key){delete this[key];}scrollIntoView(){}replaceWith(){}showModal(){}close(){}
}
const walk=node=>[node,...node.children.flatMap(child=>typeof child==='object'?walk(child):[])];
const flush=async()=>{await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));};
async function boot({enabled=true,rows=selected(),missing=false,storedVehicle='SYNTHETIC Audi A3 2016',quoteValue=quote(),missingTerms=false,waitQuote=false,partsEnabled=false}={}){
 const nodes={};for(const key of ['close-product','product-dialog','product-detail','shop-announcement','cart-items','cart-summary','cart-status','request-quote','quote-status','quote-review','checkout-button','checkout-note','buyer-address','address-status','delivery-fields','cart-workflow-note','complete-wheel-vehicle'])nodes[key]=new Node();
 const form=nodes['buyer-address'];for(const key of ['name','email','recipient','street','postcode','city','workshop_delivery','different_delivery','vehicle_ref'])form.fields[key]=new Node('input');
 form.fields.workshop_delivery.value='collection';form.fields.different_delivery.type='checkbox';form.fields.vehicle_ref.value='';
 if(missing)delete form.fields.vehicle_ref;
 if(partsEnabled){const field=form.fields.parts_vehicle_ref=new Node('select');field.options=[{value:''},{value:'alzura-vehicle:120604'}];nodes['parts-vehicle-choice']=new Node();}
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:'warenkorb',buyerEnabled:'true',quoteEnabled:'true',consumerEnabled:'true',orderEnabled:'true',checkoutEnabled:'true',deferredCompleteWheelsEnabled:String(enabled),partsDirectEnabled:String(partsEnabled),cartRequestUrl:'/anfrage?anliegen=allgemein&shop_anfrage=1'};
 const calls=[],stored=[],redirects=[];let resolveQuote;const pending=waitQuote?new Promise(resolve=>resolveQuote=resolve):null;
 const session=()=>({address_ready:true,csrf_token:TOKEN,workshop_delivery:'collection',...(storedVehicle===undefined?{}:{vehicle_ref:storedVehicle})});
 const win={document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key]||null,querySelector:()=>({content:TOKEN}),querySelectorAll:()=>[],addEventListener(){}},localStorage:{getItem:()=>JSON.stringify(rows),setItem:(_key,value)=>stored.push(value)},sessionStorage:{getItem:()=>null,setItem(){}},location:{pathname:'/shop/warenkorb/',assign:url=>redirects.push(url)},setTimeout(){},clearTimeout(){},setInterval(){},crypto:{randomUUID:()=> 'SYNTHETIC'},
  fetch:async(url,options)=>{calls.push({url,options});let value;
   if(url.endsWith('/buyer/session'))value=session();
   else if(url.includes('/complete-wheel-price?')){
     const q=Number(new URL(url,'https://synthetic.test').searchParams.get('quantity'));
     value={rim_id:RIM,tyre_id:TYRE,quantity:q,currency:'EUR',goods_gross_cent:20000*q,mounting_gross_cent:2250*q,freight_gross_cent:0,total_gross_cent:22250*q};
   }
   else if(url.includes('/products/'))value={item:url.includes(encodeURIComponent(RIM))||url.includes(RIM)?rim():tyre(),prices_refreshing:false};
   else if(url.endsWith('/buyer/address')){const payload=JSON.parse(options.body);value={...session(),...(Object.hasOwn(payload,'vehicle_ref')?{vehicle_ref:payload.vehicle_ref}:{})};}
   else if(url.endsWith('/cart/quote')){if(pending)await pending;value={quote:quoteValue,quote_retained:true};}
   else if(url.endsWith('/consumer/terms'))value={version:'SYNTHETIC 2',consumer_only:true,pack_hash:PACK,texts:{agb:'SYNTHETIC AGB',widerruf:'SYNTHETIC W',widerrufsformular:'SYNTHETIC WF',lieferung:'SYNTHETIC L',datenschutz:'SYNTHETIC D',...(missingTerms?{}:{komplettraeder_zahlung:'SYNTHETIC reservation terms'})}};
   else throw Error('Forbidden request '+url);
   return{ok:true,text:async()=>JSON.stringify(value)};
  }};
 class FormData{constructor(form){this.form=form;}get(key){const f=this.form.elements.namedItem(key);return !f||f.disabled?null:f.type==='checkbox'?(f.checked?'on':null):f.value;}}
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date,console,FormData});await flush();
 return{nodes,form,calls,stored,redirects,resolveQuote,request:()=>nodes['request-quote'].listeners.click(),submit:async()=>{await form.listeners.submit({preventDefault(){}});await flush();},change:()=>form.listeners.input()};
}
async function statusPage(status, technical){
 const nodes=Object.fromEntries(['close-product','shop-announcement','order-status','order-summary'].map(key=>[key,new Node()]));
 const body=new Node('body'),id='sho_'+'1'.repeat(32);body.dataset={api:'/shop/api/v2',page:'order_status',orderId:id};const calls=[];
 const win={document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key]||null,querySelector:()=>({content:''}),querySelectorAll:()=>[]},localStorage:{getItem:()=>'[]',setItem(){}},location:{},setTimeout(){},setInterval(){},
 fetch:async(url,options)=>{calls.push({url,options});assert.equal(url,'/shop/api/v2/orders/'+id);return{ok:true,text:async()=>JSON.stringify({order:{id,status,amount_gross_cent:89000,mode:'test'},...(technical===undefined?{}:{technical_review:technical})})};}};
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date,console});await flush();return{nodes,calls};
}
(async()=>{
 const f=await boot();assert.equal(f.nodes['buyer-address'].hidden,false);assert.equal(f.form.fields.vehicle_ref.required,true);assert.equal(f.form.fields.vehicle_ref.value,'SYNTHETIC Audi A3 2016');
 const wheelWithPartsFeature=await boot({partsEnabled:true});assert.equal(wheelWithPartsFeature.form.fields.vehicle_ref.required,true);assert.equal(wheelWithPartsFeature.form.fields.parts_vehicle_ref.required,false);assert.equal(wheelWithPartsFeature.nodes['parts-vehicle-choice'].hidden,true);await wheelWithPartsFeature.submit();assert.equal(JSON.parse(wheelWithPartsFeature.calls.filter(c=>c.url.endsWith('/buyer/address')).at(-1).options.body).vehicle_ref,'SYNTHETIC Audi A3 2016','parts feature preserves complete-wheel freeform context');
 assert.equal(f.nodes['request-quote'].disabled,false);await f.request();
 const rendered=walk(f.nodes['quote-review']);assert.ok(rendered.some(n=>n.textContent==='890,00 €'));assert.ok(f.nodes['checkout-note'].textContent.includes('zunächst auf Ihrer Karte reserviert'));
 assert.ok(f.nodes['checkout-note'].textContent.includes('Erst nach technischer Prüfung und vollständiger Lieferantenbestätigung buchen wir ab.'));
 assert.ok(walk(f.nodes['cart-items']).some(n=>n.textContent==='Montage nach Wareneingang abstimmen'));
 assert.equal(rendered.filter(n=>n.textContent==='Montage').length,1,'montage shown once from server quote');
 assert.ok(rendered.some(n=>n.textContent==='Die genaue Rad-Reifen-Kombination wird technisch geprüft.'));
 assert.ok(rendered.some(n=>n.textContent.includes('Apple Pay')));assert.equal(f.nodes['checkout-button'].textContent,'Gesamtbetrag online reservieren');assert.equal(f.nodes['checkout-button'].hidden,false);assert.equal(f.nodes['checkout-button'].disabled,true);
 const checkboxes=rendered.filter(n=>n.type==='checkbox');assert.equal(checkboxes.length,2);for(const input of checkboxes){input.checked=true;input.listeners.change();}assert.equal(f.nodes['checkout-button'].disabled,false);
 f.form.fields.vehicle_ref.value='SYNTHETIC BMW 3er 2018';f.change();assert.equal(f.nodes['checkout-button'].hidden,true);assert.equal(f.nodes['request-quote'].textContent,'Gemeinsam zur Kasse');
 await f.submit();assert.equal(JSON.parse(f.calls.filter(c=>c.url.endsWith('/buyer/address')).at(-1).options.body).vehicle_ref,'SYNTHETIC BMW 3er 2018');
 assert.equal(f.calls.filter(c=>c.url.endsWith('/cart/quote')).length,2,'confirmed change automatically requests a new mounted-set total');
 assert.equal(f.nodes['checkout-button'].textContent,'Gesamtbetrag online reservieren');assert.equal(f.nodes['checkout-button'].disabled,true,'fresh quote requires new consent');
 const stale=await boot();await stale.request();
 const setSelectors=walk(stale.nodes['cart-items']).filter(n=>n.tagName==='SELECT');
 assert.equal(setSelectors.length,1);
 assert.equal(setSelectors[0]['aria-label'],'Anzahl Kompletträder');
 assert.ok(walk(stale.nodes['cart-summary']).some(n=>n.textContent===logic.money(89000)));
 setSelectors[0].value='3';setSelectors[0].listeners.change();await flush();
 assert.deepEqual(JSON.parse(stale.stored.at(-1)).map(r=>r.quantity),[3,3]);
 assert.equal(stale.nodes['checkout-button'].hidden,true,'editing invalidates the old binding');
 assert.equal(stale.nodes['request-quote'].textContent,'Gesamtpreis prüfen');
 assert.ok(walk(stale.nodes['cart-summary']).some(n=>n.textContent===logic.money(66750)));
 const reloaded=await boot({rows:JSON.parse(stale.stored.at(-1))});
 assert.equal(walk(reloaded.nodes['cart-items']).find(n=>n.tagName==='SELECT').value,'3');
 reloaded.form.fields.workshop_delivery.value='customer_reshipment';reloaded.form.fields.workshop_delivery.listeners.change();await flush();
 assert.ok(walk(reloaded.nodes['cart-summary']).some(n=>n.textContent.includes('zzgl. Versand')));
 assert.ok(!walk(reloaded.nodes['cart-summary']).some(n=>n.textContent.includes('Gesamtpreis inkl. Montage und Versand')));
 const mismatched=selected();mismatched[0].quantity=2;
 const repair=await boot({rows:mismatched});
 const repairSelect=walk(repair.nodes['cart-items']).find(n=>n.tagName==='SELECT');
 assert.equal(repairSelect.value,'');assert.equal(repair.nodes['request-quote'].textContent,'Gesamtangebot anfragen');
 repairSelect.value='3';repairSelect.listeners.change();await flush();
 assert.deepEqual(JSON.parse(repair.stored.at(-1)).map(r=>r.quantity),[3,3]);assert.equal(repair.nodes['request-quote'].textContent,'Gesamtpreis prüfen');
 walk(repair.nodes['cart-items']).find(n=>n.textContent==='Satz aufteilen').listeners.click();await flush();
 assert.equal(walk(repair.nodes['cart-items']).filter(n=>n.tagName==='SELECT').length,2);
 assert.ok(JSON.parse(repair.stored.at(-1)).every(row=>!row.assembly_group));
 const looseSet=selected().map(({assembly_group,...line})=>({...line,services:{},selection_pair:'set|'+RIM+'|'+TYRE}));
 const loose=await boot({rows:looseSet});
 assert.equal(loose.nodes['request-quote'].textContent,'Gesamtpreis prüfen','eligible unmounted rim and tyre share one quote');
 const looseSelectors=walk(loose.nodes['cart-items']).filter(n=>n.tagName==='SELECT');
 assert.equal(looseSelectors.length,1);looseSelectors[0].value='2';looseSelectors[0].listeners.change();await flush();
 assert.deepEqual(JSON.parse(loose.stored.at(-1)).map(r=>r.quantity),[2,2]);
 assert.ok(JSON.parse(loose.stored.at(-1)).every(r=>r.selection_pair&&!r.assembly_group));
 const pending=await boot({waitQuote:true});const request=pending.request();const remove=walk(pending.nodes['cart-items']).find(n=>n.tagName==='BUTTON'&&n.textContent==='Satz entfernen');remove.listeners.click();await flush();pending.resolveQuote();await request;
 assert.deepEqual(JSON.parse(pending.stored.at(-1)),[]);assert.equal(pending.nodes['checkout-button'].hidden,true,'late quote cannot authorize removed set');
 for(const storedVehicle of [7,'x'.repeat(201),'SYNTHETIC\nA3']){const bad=await boot({storedVehicle});assert.equal(bad.nodes['request-quote'].disabled,true);}
 const firstSet=await boot({storedVehicle:''});
 assert.equal(firstSet.nodes['request-quote'].textContent,'Gemeinsam zur Kasse','a returning tyre customer may still need to enter a vehicle');
 await firstSet.request();assert.equal(firstSet.calls.some(c=>c.url.endsWith('/cart/quote')),false);
 firstSet.form.fields.vehicle_ref.value='SYNTHETIC Audi A3 2016';firstSet.change();await firstSet.submit();
 assert.equal(firstSet.nodes['checkout-button'].textContent,'Gesamtbetrag online reservieren');
 const missing=await boot({missing:true});await missing.submit();assert.equal(missing.calls.filter(c=>c.url.endsWith('/buyer/address')).length,0);
 const blank=await boot();blank.form.fields.vehicle_ref.value='';blank.change();await blank.submit();assert.equal(blank.calls.filter(c=>c.url.endsWith('/buyer/address')).length,0);
 const single=[{id:TYRE,quantity:1,services:{}}],optional=await boot({rows:single});assert.equal(optional.form.fields.vehicle_ref.required,false);optional.form.fields.vehicle_ref.value='';optional.change();await optional.submit();assert.equal(JSON.parse(optional.calls.filter(c=>c.url.endsWith('/buyer/address')).at(-1).options.body).vehicle_ref,'');
 const legacy=await boot({enabled:false,rows:single,missing:true});await legacy.submit();assert.equal(Object.hasOwn(JSON.parse(legacy.calls.filter(c=>c.url.endsWith('/buyer/address')).at(-1).options.body),'vehicle_ref'),false);
 const invalid=quote();invalid.review_requirements.push({code:'freight_unconfirmed'});const badReview=await boot({quoteValue:invalid});await badReview.request();assert.equal(badReview.nodes['checkout-button'].hidden,true);assert.equal(badReview.calls.some(c=>c.url.endsWith('/consumer/terms')),false);
 const badTerms=await boot({missingTerms:true});await badTerms.request();assert.equal(badTerms.nodes['checkout-button'].hidden,true);assert.equal(badTerms.nodes['checkout-button'].disabled,true);
 const authorized=await statusPage('authorized',{required:true,state:'pending',expires_at:null});assert.equal(authorized.nodes['order-status'].textContent,'Kartenbetrag reserviert. Noch nicht eingezogen.');assert.ok(walk(authorized.nodes['order-summary']).some(n=>n.textContent.includes('Die technische Prüfung')&&n.textContent.includes('läuft')));
 const paid=await statusPage('paid',{required:true,state:'approved',expires_at:null});assert.equal(paid.nodes['order-status'].textContent,'Zahlung bestätigt.');assert.ok(walk(paid.nodes['order-summary']).some(n=>n.textContent==='Technische Prüfung abgeschlossen.'));assert.equal(walk(paid.nodes['order-summary']).some(n=>n.textContent.includes('Abbuchung erfolgt erst')),false);
 const old=await statusPage('authorized');assert.equal(walk(old.nodes['order-summary']).some(n=>n.className==='shop-review-note'),false,'legacy status remains unchanged');
 const malformed=await statusPage('authorized',{required:'true',state:'approved'});assert.equal(walk(malformed.nodes['order-summary']).some(n=>n.className==='shop-review-note'),false);
 assert.equal([f,stale,pending,missing,blank,optional,legacy,badReview,badTerms].flatMap(x=>x.calls).some(c=>/\/orders|\/checkout|\/consumer\/acceptance/.test(c.url)),false);
 console.log('Complete-wheel authorization UI checks passed');
})().catch(error=>{console.error(error);process.exit(1)});
