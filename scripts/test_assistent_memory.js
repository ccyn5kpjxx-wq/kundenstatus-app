// Offline persistence lifecycle and UI tests: no account, network or storage.
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const flush=()=>new Promise(setImmediate);
function deferred(){let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};}
class Element {
  constructor(){this.dataset={};this.children=[];this.value='';this.textContent='';}
  append(...items){this.children.push(...items);}
  replaceChildren(...items){this.children=items;}
  querySelectorAll(){return [];}
  showModal(){this.open=true;}
  close(){this.open=false;}
  focus(){this.focused=true;}
}
function fixture(){
  const requests=[],status=[],timers=new Map(),elements=new Map();let timer=0,stops=0,resets=0;
  const $=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
  const doc={getElementById:$,createElement:()=>new Element()};
  const data={generation:'a'.repeat(48),entries:[],notes:[],next_before_id:null};
  const behaviour={};
  const context={window:{crypto:require('node:crypto').webcrypto,confirm:()=>behaviour.confirm!==false},AbortController,URLSearchParams,document:doc,
    setTimeout:(fn,ms)=>{timers.set(++timer,{fn,ms});return timer;},clearTimeout:id=>timers.delete(id)};
  vm.createContext(context);vm.runInContext(fs.readFileSync(path.join(__dirname,'..','static','assistent-memory.js'),'utf8'),context);
  const memory=new context.window.AssistantMemory({request:async(path,data,method,signal)=>{requests.push({path,data,method,signal});if(behaviour.request)return behaviour.request(path,data,method,signal);return {...(method==='GET'?fixtureData():{generation:'b'.repeat(48)})};},
    onStatus:message=>status.push(message),onStop:()=>stops++,onReset:()=>resets++});
  function fixtureData(){return data;}
  return {memory,requests,status,timers,data,behaviour,$,doc,context,stops:()=>stops,resets:()=>resets};
}
(async()=>{
  let f=fixture(),handle=await f.memory.begin();
  assert.equal(handle.generation,f.data.generation);assert.equal(f.requests[0].method,'GET');
  const entry={role:'user',text:'Auftrag 402, morgen weiter.',eventId:'item_1:0'};
  const saving=deferred();f.behaviour.request=async(path)=>path==='/gedaechtnis/gespraech'?saving.promise:f.data;
  f.memory.record(handle,entry);f.memory.record(handle,entry);
  const next=f.memory.begin();await flush();
  assert.equal(f.requests.length,2,'dedupe and next context wait for one durable write');
  assert.equal(f.requests[1].data.event_id,handle.session+':user:item_1:0');
  assert.equal(f.requests[1].data.generation,handle.generation);assert.equal(f.requests[1].data.role,'user');
  saving.resolve({generation:handle.generation});const fresh=await next;
  assert.equal(f.requests.length,3);assert.notEqual(fresh.session,handle.session);
  assert.equal(f.memory.pending,0);assert.equal(f.timers.size,0);

  // A timed out write is not retried; queued contributions do not each hold up
  // restart for another full timeout. Existing speech stays alive.
  f=fixture();handle=await f.memory.begin();const write=deferred();f.behaviour.request=()=>write.promise;
  f.memory.record(handle,entry);f.memory.record(handle,{...entry,eventId:'item_2'});await flush();
  const oldRequest=f.requests.at(-1);[...f.timers.values()][0].fn();await f.memory.queue;
  assert.equal(oldRequest.signal.aborted,true);assert.equal(f.requests.length,2);assert.equal(f.stops(),0);
  assert.match(f.status.at(-1),/nicht gespeichert/);assert.equal(handle.disabled,true);
  f.memory.record(handle,{...entry,eventId:'item_3'});await f.memory.queue;assert.equal(f.requests.length,2);
  write.resolve({generation:handle.generation});await flush();assert.match(f.status.at(-1),/nicht gespeichert/);
  delete f.behaviour.request;await f.memory.begin();assert.match(f.status.at(-1),/nicht gespeichert/,'new start must not pretend a lost turn was saved');

  // Forgotten content cannot return via delayed callbacks, retry or old handle.
  f=fixture();handle=await f.memory.begin();const pending=deferred();f.behaviour.request=async(path)=>path==='/gedaechtnis/gespraech'?pending.promise:{generation:'c'.repeat(48)};
  f.memory.record(handle,entry);await flush();const clear=f.memory.mutate('/gedaechtnis',{},'DELETE');
  assert.equal(f.stops(),1);assert.equal(f.requests.length,2,'clear first drains already accepted transcript');
  f.memory.record(handle,{...entry,eventId:'after_stop'});pending.resolve({generation:handle.generation});await clear;
  assert.equal(f.requests.length,3);assert.equal(f.requests.at(-1).method,'DELETE');assert.equal(f.requests.at(-1).data.generation,handle.generation);
  assert.equal(f.resets(),1);f.memory.record(handle,{...entry,eventId:'late_callback'});await f.memory.queue;assert.equal(f.requests.length,3);

  // Cross-tab changes stop the speech session and invalidate all queued events.
  f=fixture();handle=await f.memory.begin();f.behaviour.request=async()=>{throw Object.assign(new Error('stale'),{status:409});};
  f.memory.record(handle,entry);f.memory.record(handle,{...entry,eventId:'queued'});await f.memory.queue;
  assert.equal(f.stops(),1);assert.equal(f.requests.length,2);assert.equal(f.memory.generation,null);assert.match(f.status.at(-1),/geändert/);

  // Preload failure is an actionable start error, not an unpinned conversation.
  f=fixture();f.behaviour.request=async()=>{throw new Error('offline');};await assert.rejects(f.memory.begin(),/erneut starten oder schreiben/);
  assert.equal(f.timers.size,0);
  f=fixture();const initial=deferred();f.behaviour.request=()=>initial.promise;const beginning=f.memory.begin();await flush();
  f.memory.epoch++;initial.resolve(f.data);await assert.rejects(beginning,/erneut starten/);

  // Complete text only: no silent truncation, no invalid role or duplicate IDs.
  f=fixture();handle=await f.memory.begin();
  f.memory.record(handle,{...entry,text:'x'.repeat(8001)});f.memory.record(handle,{...entry,role:'system'});
  f.memory.record(handle,{...entry,eventId:undefined});await f.memory.queue;assert.equal(f.requests.length,1);assert.match(f.status.at(-1),/nicht gespeichert/);

  // UI content is rendered as text. Search/pagination stays actor-neutral, and
  // deleting all requires a deliberate confirmation before the request.
  f=fixture();f.data.entries=[{id:2,role:'user',text:'<img src=x onerror=alert(1)>',source:'voice',zeit:'2026-10-03T10:00:00'}];
  f.data.notes=[{id:8,text:'Am Montag weiter.',updated_at:'03.10.2026 10:00:00'}];f.data.next_before_id=2;
  f.memory.mount();await f.$('memory-open').onclick();assert.equal(f.$('memory-dialog').open,true);
  assert.match(f.$('memory-notes').children[0].children[1].textContent,/3\.10\.2026/,'German server timestamp stays October 3');
  const card=f.$('memory-entries').children[0];assert.equal(card.children[1].textContent,f.data.entries[0].text);assert.equal(card.children[1].innerHTML,undefined);
  f.$('memory-search-text').value='Klebeband';await f.$('memory-search').onsubmit();assert.match(f.requests.at(-1).path,/suche=Klebeband/);
  await f.$('memory-more').onclick();assert.match(f.requests.at(-1).path,/before_id=2/);assert.doesNotMatch(f.requests.at(-1).path,/actor|mitarbeiter/);
  f.behaviour.confirm=false;const count=f.requests.length;await f.$('memory-clear').onclick();assert.equal(f.requests.length,count);
  f.behaviour.confirm=true;await f.$('memory-clear').onclick();assert.equal(f.requests.at(-2).method,'DELETE');assert.equal(f.stops(),1);assert.equal(f.resets(),1);
  const edit=f.$('memory-notes').children[0].children[2];await edit.onclick();assert.equal(f.$('memory-note-text').value,'Am Montag weiter.');
  f.$('memory-note-text').value='Am Dienstag weiter.';await f.$('memory-note-form').onsubmit();
  assert.equal(f.requests.at(-2).data.id,8);assert.equal(f.requests.at(-2).data.text,'Am Dienstag weiter.');assert.equal(f.requests.at(-2).method,'POST');
  assert.match(f.$('memory-status').textContent,/Notiz gespeichert/);
  // A later background/start read must not silently advance an open editor's
  // compare-and-swap generation and overwrite a change made elsewhere.
  const pinned=f.data.generation;await f.$('memory-notes').children[0].children[2].onclick();
  f.data.generation='c'.repeat(48);await f.memory.begin();f.$('memory-note-text').value='Alter offener Entwurf';
  await f.$('memory-note-form').onsubmit();assert.equal(f.requests.at(-2).data.generation,pinned);
  assert.equal(f.timers.size,0);
  console.log('PASS: personal memory preload, durable queue, duplicate filtering, safe failure/timeout, stale generation fencing, forgetting, text-only UI, search, pagination and explicit mutation.');
})().catch(error=>{console.error(error);process.exitCode=1;});
