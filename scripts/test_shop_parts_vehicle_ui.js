/* Offline DOM/address adapters only. No provider, order or payment transport. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const path=process.argv[2],source=fs.readFileSync(path,'utf8'),logic=require(path);
const ID='parts:'+'a'.repeat(24),A='alzura-vehicle:120604',B='alzura-vehicle:117083',refs=[A,B,'alzura-vehicle:100043','alzura-vehicle:802380'];
const TOKEN='SYNTHETIC_CSRF_TOKEN_1234567890';
assert.equal(logic.cartNeedsPartsVehicle([{product:{department:'parts',category:'accessory'}}]),true);
assert.equal(logic.cartNeedsPartsVehicle([{product:{department:'accessories',category:'body_part'}}]),true);
for(const category of ['oil','care','accessory'])assert.equal(logic.cartNeedsPartsVehicle([{product:{department:'accessories',category}}]),false);
assert.equal(logic.partsVehicleReference(A,refs),A);
for(const ref of ['',undefined,'Audi','0588BHT','alzura-vehicle:999','alzura-vehicle:0120604',A+' '])assert.equal(logic.partsVehicleReference(ref,refs),null);
assert.equal(logic.partsVehicleReference(A,[A,A]),null);
class Node{
 constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.fields={};this.listeners={};this.dataset={};this.textContent='';this.value='';this.checked=false;this.hidden=false;this.disabled=false;this.isConnected=true;this.classList={toggle(){}};this.elements={namedItem:key=>this.fields[key]||null};}
 append(...nodes){this.children.push(...nodes);}replaceChildren(...nodes){this.children=[...nodes];}addEventListener(key,fn){this.listeners[key]=fn;}setAttribute(name,value){this[name]=value;}scrollIntoView(){}replaceWith(){}
}
const flush=async()=>{await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));};
async function boot({enabled=true,saved,department='parts',missing=false,options=refs,complete=false}={}){
 const nodes={};for(const key of ['close-product','shop-announcement','cart-items','cart-summary','cart-status','request-quote','quote-status','quote-review','checkout-button','checkout-note','buyer-address','address-status','delivery-fields','cart-workflow-note','parts-vehicle-choice','complete-wheel-vehicle'])nodes[key]=new Node();
 const form=nodes['buyer-address'];for(const key of ['name','email','recipient','street','postcode','city','different_delivery','workshop_delivery'])form.fields[key]=new Node('input');
 form.fields.workshop_delivery.value='collection';form.fields.different_delivery.type='checkbox';
 if(enabled&&!missing){const field=form.fields.parts_vehicle_ref=new Node('select');field.options=[{value:''},...options.map(value=>({value}))];}
 if(complete)form.fields.vehicle_ref=new Node('input');
 const body=new Node('body');body.dataset={api:'/shop/api/v2',page:'warenkorb',buyerEnabled:'true',quoteEnabled:'true',orderEnabled:'false',checkoutEnabled:'false',consumerEnabled:'false',partsDirectEnabled:String(enabled),deferredCompleteWheelsEnabled:String(complete),cartRequestUrl:'/anfrage?anliegen=allgemein&shop_anfrage=1'};
 const product={id:ID,category:department==='parts'?'body_part':'oil',department,name:'SYNTHETIC PART',images:[],purchase_status:'supplier_check_required',price_status:'current',price:{currency:'EUR',vat_percent:'19.00',stock_reported:2,observed_at:new Date(Date.now()-1000).toISOString(),expires_at:new Date(Date.now()+500000).toISOString(),prices_by_quantity:{'1':{quantity:1,unit_gross_cent:1000,items_gross_cent:1000}}}};
 const calls=[];let retained=saved;
 const session=()=>({address_ready:true,csrf_token:TOKEN,workshop_delivery:'collection',...(retained===undefined?{}:{vehicle_ref:retained})});
 const win={document:{body,createElement:tag=>new Node(tag),getElementById:key=>nodes[key]||null,querySelector:()=>({content:TOKEN}),querySelectorAll:()=>[]},localStorage:{getItem:()=>JSON.stringify([{id:ID,quantity:1,services:{}}]),setItem(){}},location:{pathname:'/shop/warenkorb/',assign(){throw Error('No navigation');}},setTimeout(){},clearTimeout(){},setInterval(){},
 fetch:async(url,options)=>{calls.push({url,options});let value;
  if(url.endsWith('/buyer/session'))value=session();
  else if(url.includes('/products/'))value={item:product,prices_refreshing:false};
  else if(url.endsWith('/buyer/address')){const payload=JSON.parse(options.body);retained=payload.vehicle_ref;value=session();}
  else if(url.endsWith('/cart/quote'))value={quote:{version:1,ready:false,currency:'eur',lines:[],shipments:[],blocked_reasons:[{code:'fitment_unconfirmed'}]},quote_retained:false};
  else throw Error('Unexpected financial request '+url);
  return{ok:true,text:async()=>JSON.stringify(value)};
 }};
 class FormData{constructor(form){this.form=form;}get(key){const f=this.form.elements.namedItem(key);return !f||f.disabled?null:f.type==='checkbox'?(f.checked?'on':null):f.value;}}
 vm.runInNewContext(source,{window:win,URL,URLSearchParams,Intl,Date,console,FormData});await flush();
 return{nodes,form,calls,choice:form.fields.parts_vehicle_ref,submit:async()=>{await form.listeners.submit({preventDefault(){}});await flush();},change:()=>form.listeners.change(),quote:()=>nodes['request-quote'].listeners.click()};
}
const puts=f=>f.calls.filter(call=>call.url.endsWith('/buyer/address'));
(async()=>{
 const empty=await boot();assert.equal(empty.choice.required,true);assert.equal(empty.choice.disabled,false);assert.equal(empty.nodes['request-quote'].disabled,true);await empty.submit();assert.equal(puts(empty).length,0);
 for(const saved of ['Audi','0588BHT','alzura-vehicle:999']){const f=await boot({saved});assert.equal(f.choice.value,'');assert.equal(f.nodes['request-quote'].disabled,true);}
 const ready=await boot({saved:A});assert.equal(ready.choice.value,A);assert.equal(ready.nodes['request-quote'].disabled,false);
 ready.choice.value=B;ready.change();assert.equal(ready.nodes['request-quote'].disabled,true,'vehicle change invalidates saved address and old quote');await ready.submit();
 const payload=JSON.parse(puts(ready).at(-1).options.body);assert.equal(payload.vehicle_ref,B);assert.equal(Object.hasOwn(payload,'parts_vehicle_ref'),false);assert.equal(puts(ready).at(-1).options.headers['X-CSRF-Token'],TOKEN);
 assert.equal(ready.nodes['request-quote'].disabled,false);await ready.quote();assert.equal(ready.nodes['checkout-button'].hidden,true,'backend fitment failure is never overridden by an allowlisted vehicle choice');
 ready.choice.value='Audi';ready.change();await ready.submit();assert.equal(puts(ready).length,1,'freeform brand cannot be serialized');
 const unknown=await boot({options:[],saved:A});assert.equal(unknown.nodes['request-quote'].disabled,true);await unknown.submit();assert.equal(puts(unknown).length,0);
 const missing=await boot({missing:true,saved:A});await missing.submit();assert.equal(puts(missing).length,0);
 const oil=await boot({department:'accessories'});assert.equal(oil.choice.required,false);assert.equal(oil.choice.disabled,true);assert.equal(oil.nodes['parts-vehicle-choice'].hidden,true);assert.equal(oil.nodes['request-quote'].disabled,false);await oil.submit();assert.equal(JSON.parse(puts(oil)[0].options.body).vehicle_ref,'');
 const legacy=await boot({enabled:false});await legacy.submit();assert.equal(Object.hasOwn(JSON.parse(puts(legacy)[0].options.body),'vehicle_ref'),false);
 const both=await boot({saved:A,complete:true});both.form.fields.vehicle_ref.value='Audi without exact model';await both.submit();assert.equal(JSON.parse(puts(both)[0].options.body).vehicle_ref,A,'parts selector takes precedence over hidden wheel freeform');
 const generic=await boot({department:'accessories',complete:true,saved:'Audi A3 2016'});await generic.submit();assert.equal(JSON.parse(puts(generic)[0].options.body).vehicle_ref,'Audi A3 2016','existing optional freeform is preserved when parts selection is unnecessary');
 assert.equal([empty,ready,unknown,missing,oil,legacy,both,generic].flatMap(f=>f.calls).some(call=>/\/orders|\/checkout|\/consumer/.test(call.url)),false);
 console.log('Exact parts vehicle UI checks passed');
})().catch(error=>{console.error(error);process.exit(1)});
