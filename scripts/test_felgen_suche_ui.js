// Execute the real rim search against a small DOM and synthetic deferred replies.
// No application import, browser, real timers, dependencies or network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const START = Date.parse('2026-11-03T10:00:00Z');
const SOURCE = path.join(__dirname,'../static/homepage-felgen-suche.js');

function harness(initial={}, {query='', facetsUrl='', searchUrl='/api/felgenangebote/suche'}={}) {
  let now=START, timerId=0;
  const timers=new Map(), requests=[], inputs={};
  const document={activeElement:null};
  class Element {
    constructor(tag='div', attributes={}) {
      this.tagName=tag.toUpperCase(); this.attributes={...attributes}; this.children=[]; this.parentElement=null;
      this.dataset={}; this.handlers={}; this.className=attributes.class||''; this.id=attributes.id||'';
      this.name=attributes.name||''; this.type=attributes.type||''; this._value=attributes.value||'';
      this.hidden=false; this.disabled=false; this._text='';
      this.classList={toggle:(name,on) => {
        const values=this.className.split(/\s+/).filter(Boolean).filter(value=>value!==name);
        if (on) values.push(name); this.className=values.join(' ');
      }};
    }
    get value() { return this._value; }
    set value(value) {
      const selected=String(value);
      this._value=this.tagName==='SELECT'&&!this.children.some(option=>option.value===selected)?'':selected;
    }
    get textContent() { return this._text+this.children.map(child=>child.textContent).join(''); }
    set textContent(value) { this.replaceChildren(); this._text=String(value); }
    append(...children) { children.forEach(child=>{ child.parentElement=this; this.children.push(child); }); }
    replaceChildren(...children) { this.children.forEach(child=>{child.parentElement=null;}); this.children=[]; this._text=''; this.append(...children); }
    setAttribute(name,value) { this.attributes[name]=String(value); }
    getAttribute(name) { return Object.hasOwn(this.attributes,name)?this.attributes[name]:null; }
    hasAttribute(name) { return Object.hasOwn(this.attributes,name); }
    removeAttribute(name) { delete this.attributes[name]; }
    matches(selector) {
      const match=selector.match(/^([a-z][\w-]*)?([.#][\w-]+)?(\[[\w-]+(?:="[^"]*")?\])?$/i);
      if (!match) throw new Error('Unsupported fake DOM selector: '+selector);
      if (match[1] && this.tagName!==match[1].toUpperCase()) return false;
      if (match[2]?.startsWith('.') && !this.className.split(/\s+/).includes(match[2].slice(1))) return false;
      if (match[2]?.startsWith('#') && this.id!==match[2].slice(1)) return false;
      if (match[3]) {
        const [,name,value]=match[3].match(/^\[([\w-]+)(?:="([^"]*)")?\]$/);
        const actual=['name','id','type'].includes(name)?this[name]:this.getAttribute(name);
        if (value===undefined ? actual===null : actual!==value) return false;
      }
      return true;
    }
    querySelectorAll(selector) { return this.children.flatMap(child=>[child,...child.querySelectorAll('*')]).filter(child=>selector==='*'||child.matches(selector)); }
    querySelector(selector) { return this.querySelectorAll(selector)[0]||null; }
    closest(selector) { return this.matches(selector)?this:this.parentElement?.closest(selector)||null; }
    focus() { document.activeElement=this; }
    addEventListener(type,handler) { (this.handlers[type]||=[]).push(handler); }
    async dispatch(type) {
      const event={target:this,defaultPrevented:false,preventDefault(){this.defaultPrevented=true;}};
      for (const handler of this.handlers[type]||[]) await handler(event);
      return event;
    }
    click() { return this.disabled?Promise.resolve():this.dispatch('click'); }
  }
  const root=new Element(), form=new Element('form',{'data-rim-search':''});
  form.dataset={searchUrl,facetsUrl}; root.append(form);
  const defaults={suche:'',hersteller:'',material:'alle',anzahl:'4',sortierung:'preis_auf',...initial};
  for (const [name,value] of Object.entries(defaults)) {
    const group=new Element('div',{class:'field'});
    const input=new Element(name==='suche'?'input':'select',{name,value});
    if(input.tagName==='SELECT'){
      const options={hersteller:facetsUrl?['']:['','Borbet','Dezent',value],material:['alle','alu','stahl'],
        anzahl:['1','2','3','4','5','6','7','8'],sortierung:['preis_auf','preis_ab','name']}[name];
      options.forEach(value=>input.append(new Element('option',{value})));input.value=value;
    }
    group.append(input); form.append(group); inputs[name]=input;
  }
  inputs.suche.setAttribute('aria-describedby','rim-query-help');
  inputs.hersteller.setAttribute('data-rim-brand',''); inputs.hersteller.disabled=!!facetsUrl;
  const button=new Element('button',{'data-rim-search-button':''}), status=new Element('p',{'data-rim-search-status':''});
  const results=new Element('div',{'data-rim-search-results':''}), catalog=new Element('p',{'data-rim-catalog-status':''});
  form.append(button,status,results); root.append(catalog);
  document.querySelector=selector=>root.querySelector(selector);
  document.createElement=tag=>new Element(tag);
  class ControlledDate extends Date {
    constructor(...args) { super(...(args.length?args:[now])); }
    static now() { return now; }
  }
  function fetch(url,options) {
    return new Promise((resolve,reject)=>requests.push({url:new URL(String(url)),options,reject,
      respond(payload,code=200,headers={}) { resolve({status:code,ok:code>=200&&code<300,json:async()=>payload,
        headers:{get:name=>Object.entries(headers).find(([key])=>key.toLowerCase()===name.toLowerCase())?.[1]||null}}); }
    }));
  }
  const window={fetch,AbortController,location:{origin:'https://shop.example.invalid',search:query},
    setTimeout(callback,delay) { const id=++timerId; timers.set(id,{callback,at:now+delay}); return id; },
    clearTimeout(id) { timers.delete(id); },
    setInterval(callback,delay) { const id=++timerId; timers.set(id,{callback,at:now+delay,interval:delay}); return id; },
    clearInterval(id) { timers.delete(id); }};
  vm.runInNewContext(fs.readFileSync(SOURCE,'utf8'),{document,window,fetch,AbortController,URL,URLSearchParams,Intl,Date:ControlledDate,console},{filename:SOURCE});
  return {form,button,status,results,catalog,inputs,requests,document,
    search:()=>button.click(),
    async change(name,value,type='change') { inputs[name].value=String(value); await inputs[name].dispatch(type); },
    async advance(ms) {
      const end=now+ms; let next;
      while ((next=[...timers].filter(([,timer])=>timer.at<=end).sort((a,b)=>a[1].at-b[1].at)[0])) {
        const [id,timer]=next; now=timer.at;
        if (timer.interval) timer.at+=timer.interval; else timers.delete(id);
        await timer.callback();
      }
      now=end;
    }};
}
const product=changes=>({artikel_id:'12345',ean:null,hersteller_artikelnummer:'SYNTHETIC-1',hersteller:'Borbet',profil:'Y',
  bezeichnung:'Synthetische Testfelge',material:'alu',bestand:10,anzahl:4,kennungspassung:'nicht_geprueft',
  preisstand:new Date(START-1000).toISOString(),gueltig_bis:new Date(START+600000).toISOString(),
  preis:{einzel_brutto:'100.00',gesamt_felgen_brutto:'400.00',versand_brutto:'7.14',gesamt_brutto:'407.14',waehrung:'EUR'},
  auswahl_url:'/felgen/auswahl/.eJ-synthetic.token_1.sig',...changes});
const offers=(item=product(),page=1)=>({status:'ok',verfuegbar:true,produkte:[item],treffer_gesamt:25,seite:page,seiten:3});
const noOffers={status:'keine_treffer',code:'keine_treffer',verfuegbar:false,produkte:[]};
const facets=changes=>({status:'ok',hersteller:['Borbet','Dezent'],materialien:['alu','stahl'],artikel_gesamt:25,
  preisstand:new Date(START-1000).toISOString(),gueltig_bis:new Date(START+600000).toISOString(),...changes});
const flush=async()=>{for(let n=0;n<12;n++)await Promise.resolve();};
const heading=h=>h.results.querySelector('h3')?.textContent||'';
const action=(h,text)=>{const result=h.results.querySelectorAll('button').find(button=>button.textContent===text);assert.ok(result,text);return result;};
async function reply(h,pending,payload,code=200,headers={}) {h.requests.at(-1).respond(payload,code,headers);await pending;await flush();}

const cases=[
  ['explicit links wait for valid facets, set visible filters and start exactly one GET',async()=>{
    const h=harness({}, {facetsUrl:'/api/felgenkatalog/auswahl',query:'?anzeigen=1&suche=Y&hersteller=Borbet&material=alu&anzahl=2&sortierung=name'});
    assert.equal(h.inputs.anzahl.value,'2');assert.equal(h.inputs.suche.value,'Y');
    assert.equal(h.inputs.hersteller.value,'','A real select has no matching brand option while loading');
    assert.equal(h.requests.length,1);assert.equal(h.requests[0].url.pathname,'/api/felgenkatalog/auswahl');
    h.requests[0].respond(facets());await flush();
    assert.equal(h.requests.length,2);assert.equal(h.requests[1].url.pathname,'/api/felgenangebote/suche');assert.equal(h.inputs.hersteller.value,'Borbet');
    for(const [key,value]of Object.entries({suche:'Y',hersteller:'Borbet',material:'alu',anzahl:'2',sortierung:'name'}))assert.equal(h.requests[1].url.searchParams.get(key),value);
    await reply(h,Promise.resolve(),noOffers);assert.equal(h.requests.length,2);
    assert.ok(h.requests.every(request=>request.options.method==='GET'));
    const ordinary=harness({}, {facetsUrl:'/api/felgenkatalog/auswahl'});
    ordinary.requests[0].respond(facets());await flush();assert.equal(ordinary.requests.length,1);
    const defaults=harness({}, {facetsUrl:'/api/felgenkatalog/auswahl',query:'?anzeigen=1'});
    defaults.requests[0].respond(facets());await flush();assert.equal(defaults.requests.length,2);
    assert.equal(defaults.requests[1].url.searchParams.get('material'),'alle');await reply(defaults,Promise.resolve(),noOffers);
  }],
  ['field validation and safe server errors never submit invalid or private input',async()=>{
    for(const [name,value]of [['suche','x'.repeat(121)],['hersteller','x'.repeat(101)],['material','all'],['anzahl','9'],['sortierung','testnote_auf']]){
      const h=harness({[name]:value}),pending=h.search();assert.equal(h.requests.length,0);await pending;
      assert.equal(h.inputs[name].getAttribute('aria-invalid'),'true');
    }
    const h=harness();await reply(h,h.search(),{feld:'suche',nachricht:'SECRET provider detail'},400);
    assert.equal(h.inputs.suche.getAttribute('aria-invalid'),'true');assert.match(h.inputs.suche.getAttribute('aria-describedby'),/rim-query-help/);
    assert.doesNotMatch(h.form.textContent,/SECRET/);await h.change('suche','Y','input');
    assert.equal(h.inputs.suche.getAttribute('aria-invalid'),null);assert.equal(h.inputs.suche.getAttribute('aria-describedby'),'rim-query-help');
  }],
  ['real product shape renders gross totals and GET selection without asserting vehicle fitment',async()=>{
    const h=harness(),name='<img src=x onerror=synthetic>';
    await reply(h,h.search(),offers(product({bezeichnung:name,private_value:'SECRET'})));
    assert.equal(heading(h),name);assert.equal(h.results.querySelectorAll('img').length,0);
    assert.match(h.results.textContent,/Felgenfoto noch nicht hinterlegt/);assert.match(h.results.textContent,/Passung.*noch nicht geprüft/);
    assert.match(h.results.querySelector('.tire-result-pricing').textContent,/407,14/);assert.match(h.results.textContent,/ohne Reifen/);
    const choose=h.results.querySelector('.tire-result-select');assert.equal(choose.textContent,'Felge auswählen');
    assert.equal(choose.href,product().auswahl_url);assert.equal((await choose.click()).defaultPrevented,false);
    assert.equal(h.requests.length,1);assert.doesNotMatch(h.results.textContent,/SECRET|Fahrzeug passend|Passgarantie/);
  }],
  ['invalid quotes are discarded while optional action and photo failures keep valid prices',async()=>{
    const base=product();
    for(const changes of [{bestand:3},{anzahl:2},{gueltig_bis:new Date(START).toISOString()},
      {kennungspassung:'geprueft'},{artikel_id:12345},{artikel_id:'12345\n'},
      {preis:{...base.preis,gesamt_brutto:'1.00'}},{preis:{...base.preis,einzel_brutto:'100.00\n'}}]){
      const h=harness();await reply(h,h.search(),offers(product(changes)));assert.equal(h.results.querySelectorAll('article').length,0);
    }
    for(const auswahl_url of ['https://evil.invalid/felgen/auswahl/token','/felgen/auswahl/token?price=1',
      'https://shop.example.invalid/felgen/auswahl/token#','/felgen/auswahl/../token']){
      const h=harness();await reply(h,h.search(),offers(product({auswahl_url})));
      assert.equal(h.results.querySelectorAll('article').length,1);assert.equal(h.results.querySelector('.tire-result-select'),null);
    }
    const url='/api/felgenkatalog/bild/12345/'+ 'a'.repeat(64)+'.webp';
    for(const bild of [{url,alt:'Synthetische Felgenabbildung',bezug:'artikel',motiv:'reifen'},
      {url:'https://evil.invalid'+url,alt:'Synthetische Felgenabbildung',bezug:'artikel',motiv:'felge'},
      {url:url.replace('/12345/','/12346/'),alt:'Synthetische Felgenabbildung',bezug:'artikel',motiv:'felge'}]){
      const h=harness();await reply(h,h.search(),offers(product({bild})));assert.equal(h.results.querySelectorAll('img').length,0);assert.ok(h.results.querySelector('.tire-result-select'));
    }
    const h=harness();await reply(h,h.search(),offers(product({bild:{url,alt:'Synthetische Felgenabbildung',bezug:'artikel',motiv:'felge'}})));
    const image=h.results.querySelector('img');assert.equal(image.getAttribute('loading'),'lazy');assert.equal(image.getAttribute('decoding'),'async');
    assert.equal(image.getAttribute('src'),url);assert.equal(image.getAttribute('alt'),'Synthetische Felgenabbildung');
    assert.equal(image.getAttribute('width'),'160');assert.equal(image.getAttribute('height'),'160');
    assert.equal(image,h.results.querySelector('.rim-result-image'));
    for(const element of [image,h.results.querySelector('.rim-result-media'),h.results.querySelector('.rim-result-image-box'),h.results.querySelector('figcaption')]){
      assert.ok(element);assert.equal(element.getAttribute('style'),null,'CSS owns responsive image sizing and caption styling');
    }
    assert.equal(h.results.querySelector('figcaption').textContent,'Artikelabbildung · Felge ohne Reifen.');
    await image.dispatch('error');assert.equal(h.results.querySelectorAll('img').length,0);assert.match(h.results.textContent,/Felgenfoto noch nicht hinterlegt/);assert.equal(h.requests.length,1);
    assert.equal(h.results.querySelector('figcaption').textContent,'');
    assert.ok(h.results.querySelector('.tire-result-select'));assert.match(h.results.querySelector('.tire-result-pricing').textContent,/407,14/);
  }],
  ['article rim photos require exact local SKU metadata and never substitute tire or assembled-wheel motifs',async()=>{
    const url='/api/felgenkatalog/bild/12345/'+ 'b'.repeat(64)+'.webp';
    const valid={url,alt:'Synthetische konkrete Artikelausführung',bezug:'artikel',motiv:'felge'};
    const invalid=[undefined,null,{}, {url,alt:valid.alt,motiv:'felge'}, {...valid,extra:'not-public'},
      {...valid,bezug:'profil'}, {...valid,motiv:'reifen'}, {...valid,motiv:'komplettrad'}, {...valid,motiv:'felge_mit_reifen'},
      {...valid,url:'https://shop.example.invalid'+url}, {...valid,url:'//shop.example.invalid'+url},
      {...valid,url:url+'?'}, {...valid,url:url+'#'}, {...valid,url:url+'?secret=1'}, {...valid,url:url+'\n'},
      {...valid,url:url.replace('b'.repeat(64),'B'.repeat(64))}, {...valid,url:url.replace('b'.repeat(64),'b'.repeat(63))},
      {...valid,url:url.replace('/bild/','/bild/../bild/')}, {...valid,url:url.replace('/12345/','/12346/')},
      {...valid,alt:''}, {...valid,alt:'x'.repeat(251)}, {...valid,alt:'\u0000secret'}, {...valid,alt:'<img onerror=synthetic>'}];
    for(const bild of invalid){
      const h=harness();await reply(h,h.search(),offers(product({bild})));
      assert.equal(h.results.querySelectorAll('article').length,1);assert.equal(h.results.querySelectorAll('img').length,0);
      assert.match(h.results.textContent,/Felgenfoto noch nicht hinterlegt/);assert.ok(h.results.querySelector('.tire-result-select'));
      assert.match(h.results.querySelector('.tire-result-pricing').textContent,/407,14/);assert.equal(h.requests.length,1);
    }
  }],
  ['pagination preserves all rim filters and a changed dropdown makes one page-one request',async()=>{
    const initial={suche:'Y',hersteller:'Borbet',material:'alu',anzahl:'4',sortierung:'preis_ab'},h=harness(initial);
    await reply(h,h.search(),offers());await action(h,'Weitere Felgen →').click();
    assert.equal(h.requests[1].url.searchParams.get('seite'),'2');
    Object.entries(initial).forEach(([key,value])=>assert.equal(h.requests[1].url.searchParams.get(key),value));
    await reply(h,Promise.resolve(),offers(product(),2));await h.change('anzahl','2','input');assert.equal(h.requests.length,2);
    await h.change('anzahl','2');assert.equal(h.requests.length,3);assert.equal(h.requests[2].url.searchParams.get('seite'),null);
    assert.equal(h.requests[2].url.searchParams.get('anzahl'),'2');await reply(h,Promise.resolve(),noOffers);
  }],
  ['late replies, expired quotes and retained old links cannot replace or clear newer results',async()=>{
    const h=harness(),old=h.search();await h.change('suche','new','input');const fresh=h.search();
    h.requests[0].respond(offers(product({bezeichnung:'Old'})));await old;assert.equal(h.results.querySelectorAll('article').length,0);
    h.requests[1].respond(offers(product({bezeichnung:'New',gueltig_bis:new Date(START+2000).toISOString()})));await fresh;
    assert.equal(heading(h),'New');const stale=h.results.querySelector('.tire-result-select');
    await h.change('suche','newer','input');await reply(h,h.search(),offers(product({bezeichnung:'Newer'})));
    await h.advance(2000);assert.equal((await stale.click()).defaultPrevented,true);assert.equal(heading(h),'Newer');
    const expiry=harness();await reply(expiry,expiry.search(),offers(product({gueltig_bis:new Date(START+2000).toISOString()})));
    const link=expiry.results.querySelector('.tire-result-select');await expiry.advance(2000);
    assert.equal(expiry.results.querySelectorAll('article').length,0);assert.equal((await link.click()).defaultPrevented,true);
  }],
  ['catalogue, no-result, network and timeout errors stay distinct and cooldown never auto-retries',async()=>{
    for(const [code,match]of [['cache_stale',/Aktuelle Felgenpreise/],['configuration',/Felgenkatalog/],['unknown',/vorübergehend/]]){
      const h=harness();await reply(h,h.search(),{code,nachricht:'SECRET',verfuegbar:false},503);assert.match(heading(h),match);assert.doesNotMatch(h.form.textContent,/SECRET/);
    }
    const none=harness();await reply(none,none.search(),noOffers);assert.match(heading(none),/Keine passenden Felgen/);
    const network=harness(),pending=network.search();network.requests[0].reject(new Error('SECRET'));await pending;assert.match(heading(network),/Verbindung/);
    const timeout=harness(),slow=timeout.search();await timeout.advance(12000);assert.equal(timeout.requests[0].options.signal.aborted,true);
    timeout.requests[0].reject(new Error('SECRET'));await slow;assert.match(heading(timeout),/dauert.*lange/);
    const limited=harness();await reply(limited,limited.search(),{},429,{'Retry-After':'3'});assert.equal(limited.button.disabled,true);
    await limited.change('material','stahl');assert.equal(limited.requests.length,1);await limited.advance(3000);
    assert.equal(limited.button.disabled,false);assert.equal(limited.requests.length,1);
  }],
  ['unsafe facets and invalid links cannot leak data or silently start a default search',async()=>{
    const foreign=harness({}, {facetsUrl:'https://evil.invalid/api/felgenkatalog/auswahl',query:'?anzeigen=1'});await flush();assert.equal(foreign.requests.length,0);
    const foreignSearch=harness({}, {searchUrl:'https://evil.invalid/api/felgenangebote/suche'});await foreignSearch.search();assert.equal(foreignSearch.requests.length,0);
    for(const data of [facets({gueltig_bis:'invalid'}),facets({gueltig_bis:new Date(START-1).toISOString()}),facets({hersteller:['SECRET\u0000']}),facets({materialien:['reifen']})]){
      const h=harness({}, {facetsUrl:'/api/felgenkatalog/auswahl',query:'?anzeigen=1'});h.requests[0].respond(data);await flush();assert.equal(h.requests.length,1);assert.doesNotMatch(h.form.textContent+h.catalog.textContent,/SECRET/);
    }
    for(const query of ['?anzeigen=1&anzeigen=1','?anzeigen=1&anzahl=4&anzahl=4','?anzeigen=1&material=all','?anzeigen=1&suche='+ 'x'.repeat(121)]){
      const h=harness({}, {facetsUrl:'/api/felgenkatalog/auswahl',query});h.requests[0].respond(facets());await flush();assert.equal(h.requests.length,1);
    }
    const changed=harness({}, {facetsUrl:'/api/felgenkatalog/auswahl',query:'?anzeigen=1'});
    await changed.change('suche','Latest input','input');changed.requests[0].respond(facets());await flush();
    assert.equal(changed.requests.length,1);assert.equal(changed.inputs.suche.value,'Latest input');
    const absentBrand=harness({}, {facetsUrl:'/api/felgenkatalog/auswahl',query:'?anzeigen=1&hersteller=NotInCatalogue'});
    absentBrand.requests[0].respond(facets());await flush();
    assert.equal(absentBrand.requests[1].url.searchParams.get('hersteller'),'NotInCatalogue','Never silently widen an explicitly requested brand');
    await reply(absentBrand,Promise.resolve(),noOffers);
  }],
];
async function run(){for(const [name,test]of cases){await test();console.log('ok - '+name);}console.log(`Felgensuche UI: ${cases.length}/${cases.length} offline checks passed.`);}
if(require.main===module)run().catch(error=>{console.error(error);process.exitCode=1;});
module.exports={harness,product,run};
