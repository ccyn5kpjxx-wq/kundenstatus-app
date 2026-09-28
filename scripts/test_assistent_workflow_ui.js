// Offline lifecycle checks of the real workflow script. No browser, microphone or network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'static', 'assistent-workflow.js'), 'utf8');
const flush = () => new Promise(setImmediate);
const deferred = () => {let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};};
const event = (value) => ({preventDefault(){},submitter:value?{value}:undefined});

class Element {
  constructor(tag='div') {this.tagName=tag.toUpperCase();this.dataset={};this.children=[];this.listeners=new Map();this.elements={};this.value='';this.textContent='';this.hidden=false;this.disabled=false;this.open=false;}
  append(...children){for(const child of children){child.parent=this;this.children.push(child);}}
  replaceChildren(...children){this.children=[];this.append(...children);}
  addEventListener(name,fn){if(!this.listeners.has(name))this.listeners.set(name,[]);this.listeners.get(name).push(fn);}
  emit(name,args=event()){return Promise.all((this.listeners.get(name)||[]).map(fn=>fn(args)));}
  querySelectorAll(selector){const descendants=this.children.flatMap(child=>[child,...child.querySelectorAll('*')]);return descendants.filter(child=>selector==='*'||selector==='button'&&child.tagName==='BUTTON'||selector==='label'&&child.tagName==='LABEL'||selector==='input'&&child.tagName==='INPUT'||selector==='[data-edit-field]'&&'editField'in child.dataset||selector==='[data-workflow-panel]'&&'workflowPanel'in child.dataset||selector==='[data-workflow-tab]'&&'workflowTab'in child.dataset);}
  setAttribute(name,value){this[name]=value;}
  removeAttribute(name){delete this[name];}
  remove(){this.parent.children=this.parent.children.filter(value=>value!==this);}
  showModal(){this.open=true;}
  close(){this.open=false;this.emit('close');}
  scrollIntoView(){}
  focus(){}
  reset(){for(const input of Object.values(this.elements)){input.value='';input.checked=false;}}
}
class Form {
  constructor(element){this.items=[];if(element){const controls=new Set([...Object.values(element.elements),...element.querySelectorAll('*')]);for(const input of controls)if(input.name&&!input.disabled&&(input.type!=='checkbox'||input.checked))this.append(input.name,input.value);}}
  append(key,value){this.items.push([key,value]);}
  get(key){return this.items.find(pair=>pair[0]===key)?.[1]??null;}
  getAll(key){return this.items.filter(pair=>pair[0]===key).map(pair=>pair[1]);}
  [Symbol.iterator](){return this.items[Symbol.iterator]();}
}
async function fixture(){
  const elements=new Map(),document=new Element(),requests=[],proposals=[];
  const get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
  document.getElementById=get;document.createElement=tag=>new Element(tag);document.createTextNode=text=>Object.assign(new Element('#text'),{textContent:text});
  get('assistant').dataset={documentsEnabled:'true',offersEnabled:'true'};
  const form=(id,names)=>{const value=get(id);value.tagName='FORM';for(const name of names){const input=new Element('input');input.name=name;value.elements[name]=input;value.append(input);}value.append(new Element('button'));return value;};
  form('workflow-upload-form',['file','purpose']).elements.purpose.value='schaden';
  get('workflow-upload-form').elements.file.files=[{name:'synthetic.png'}];
  form('workflow-new-form',['kunde_name','fahrzeug','kennzeichen','fin_nummer','hsn_nummer','tsn_nummer','kunde_email','kontakt_telefon','beschreibung','farbcode','farbton','farbton_2']);
  form('workflow-color-form',['farbcode','farbton','farbton_2']);form('workflow-contact-form',['kunde_name','kunde_email','kontakt_telefon']);
  for(const id of ['workflow-color-form','workflow-contact-form'])for(const input of Object.values(get(id).elements))input.dataset.editField='';
  form('workflow-mail-form',['art','supplier_id','text','gesamt_brutto']).elements.art.value='lieferantenanfrage';
  get('workflow-mail-form').elements.supplier_id.tagName='SELECT';
  get('workflow-mail-form').append(get('workflow-attachments'));
  form('workflow-review',[]).append(get('workflow-review-fields'));
  const dialog=get('workflow-dialog');
  for(const name of ['upload','new','edit','mail','sources']){
    const panel=get('panel-'+name);panel.dataset.workflowPanel=name;dialog.append(panel);
    const button=get('tab-'+name);button.tagName='BUTTON';button.dataset.workflowTab=name;dialog.append(button);
  }
  let order={id:156,fahrzeug:'Audi Test',kennzeichen:'TEST-156',farbcode:'OLD'},stop=0;
  const handlers=new Map([['/unterlagen',[]],['/lieferanten',[]],['/angebote/156',{sources:[]}],['/angebote/157',{sources:[]}]]);
  const host={getOrder:()=>order,stopVoice(){stop++;},async proposed(value){proposals.push(value);},
    async api(route,data){requests.push({route,data});const value=handlers.get(route);if(value instanceof Error)throw value;return typeof value==='function'?value(data):value??{};}};
  const window={AssistantWorkflowHost:host,crypto:require('node:crypto').webcrypto};
  const context={window,document,FormData:Form,console,setTimeout,clearTimeout};vm.createContext(context);
  vm.runInContext(source.replace(/\}\)\(\);\s*$/,'window.workflowTest={open,tab,review,getSelected:()=>selected,getNewSource:()=>newSource};\n})();'),context);
  await flush();
  return {get,document,requests,proposals,handlers,test:window.workflowTest,window,
    async changeOrder(value){order=value;await document.emit('assistant-order-changed');await flush();},
    findButton(text){return get('workflow-uploads').querySelectorAll('button').find(button=>button.textContent===text);},
    async open(name){context.window.workflowTest.open(name);await flush();},
    stopCount:()=>stop};
}

(async()=>{
  // Same dynamic button cannot prepare twice while its request is pending.
  let f=await fixture();const staged={id:'upload-a',original_name:'photo.png',zweck:'schaden',status:'pruefen',analyse:{felder:{},hinweise:[]}};
  f.handlers.set('/unterlagen',[staged]);await f.open('upload');
  let pending=deferred();f.handlers.set('/vorschlag',()=>pending.promise);
  let button=f.findButton('Original diesem Auftrag zuordnen');
  const first=button.onclick(event()),second=button.onclick(event());await flush();
  assert.equal(f.requests.filter(value=>value.route==='/vorschlag').length,1);
  pending.resolve({id:'proposal',status:'vorschlag'});await Promise.all([first,second]);
  assert.equal(f.proposals.length,1);assert.ok(f.stopCount()>0);
  assert.ok(!f.requests.some(value=>/bestaetigen|senden/.test(value.route)));

  // Mail submit is also single-flight; order changes cannot retarget its captured proposal.
  f=await fixture();await f.open('mail');
  f.get('workflow-mail-form').elements.text.value='Explicit scope for 156';
  pending=deferred();f.handlers.set('/vorschlag',()=>pending.promise);
  const sendOne=f.get('workflow-mail-form').onsubmit(event());
  const sendTwo=f.get('workflow-mail-form').onsubmit(event());await flush();
  let sent=f.requests.filter(value=>value.route==='/vorschlag');
  assert.equal(sent.length,1);assert.equal(sent[0].data.auftrag_id,156);
  assert.equal(sent[0].data.text,'Explicit scope for 156');
  await f.changeOrder({id:157,fahrzeug:'Other'});
  pending.resolve({id:'old-order-proposal',status:'vorschlag'});await Promise.all([sendOne,sendTwo]);
  assert.equal(f.proposals.length,0,'late old-order proposal must not replace new-order review');
  assert.ok(!f.requests.some(value=>/bestaetigen|senden/.test(value.route)));

  // Close while a proposal is pending: retain server proposal, never confirm or reopen it.
  f=await fixture();await f.open('edit');
  let color=f.get('workflow-color-form').elements.farbcode;color.value='LY9B';await color.emit('input');
  pending=deferred();f.handlers.set('/vorschlag',()=>pending.promise);
  const save=f.get('workflow-color-form').onsubmit(event());f.get('workflow-close').onclick();
  pending.resolve({id:'proposal',status:'vorschlag'});await save;
  assert.equal(f.proposals.length,0);assert.equal(f.get('workflow-dialog').open,false);

  // A closed upload remains recoverable by listing; its delayed response cannot overwrite UI.
  f=await fixture();await f.open('upload');pending=deferred();f.handlers.set('/unterlagen',data=>data?pending.promise:[]);
  const upload=f.get('workflow-upload-form').onsubmit(event());await flush();f.get('workflow-close').onclick();
  pending.resolve(staged);await upload;
  assert.equal(f.test.getSelected(),null);assert.equal(f.get('workflow-dialog').open,false);
  f.handlers.set('/unterlagen',[staged]);await f.open('upload');
  assert.ok(f.findButton('Angaben prüfen'));

  // A lost upload response reuses its request id. A different file gets a fresh id.
  f=await fixture();await f.open('upload');let attempts=0;
  f.handlers.set('/unterlagen',data=>{if(!data)return [];if(++attempts===1)throw new Error('Antwort verloren');return staged;});
  await f.get('workflow-upload-form').onsubmit(event());await f.get('workflow-upload-form').onsubmit(event());
  let uploads=f.requests.filter(value=>value.route==='/unterlagen'&&value.data);
  assert.equal(uploads[0].data.get('request_id'),uploads[1].data.get('request_id'));
  await f.get('workflow-upload-form').elements.file.emit('change');await f.get('workflow-upload-form').onsubmit(event());
  uploads=f.requests.filter(value=>value.route==='/unterlagen'&&value.data);
  assert.notEqual(uploads[1].data.get('request_id'),uploads[2].data.get('request_id'));

  // An old order's delayed source response never appears for the new selected order.
  f=await fixture();pending=deferred();f.handlers.set('/angebote/156',()=>pending.promise);
  await f.open('sources');await f.changeOrder({id:157,fahrzeug:'Other'});
  pending.resolve({sources:[{id:1,title:'OLD SECRET',text:'Wrong order'}]});await flush();
  assert.equal(f.get('workflow-sources').children.length,0);

  // Editing one field, leaving and returning to the same order must preserve the draft.
  f=await fixture();await f.open('edit');color=f.get('workflow-color-form').elements.farbcode;
  color.value='LY9B';await color.emit('input');f.test.tab('upload');f.test.tab('edit');
  assert.equal(color.value,'LY9B','tab switching must not discard unsaved paint code');
  assert.equal(color.dataset.changed,'true');
  await f.changeOrder({id:157,farbcode:'NEW-ORDER'});
  assert.equal(color.value,'NEW-ORDER','a different order must never inherit another order edit');

  // Inspecting a card again after analysis must use the latest analysis, not its old closure.
  f=await fixture();f.handlers.set('/unterlagen',[{...staged,status:'bereit'}]);
  f.handlers.set('/unterlagen/upload-a/analyse',{...staged,analyse:{felder:{farbcode:'LY9B'},hinweise:['Geprüfte Quelle']}});
  await f.open('upload');await f.findButton('Angaben auslesen').onclick(event());
  await f.findButton('Angaben prüfen').onclick(event());
  assert.equal(f.test.getSelected().analyse.felder.farbcode,'LY9B','review must keep newly read data');

  // Mail preparation is scoped to one selected order; no old scope or price carries over.
  f=await fixture();await f.open('mail');const mail=f.get('workflow-mail-form');
  mail.elements.text.value='Parts for order 156';mail.elements.gesamt_brutto.value='1200';
  await f.changeOrder({id:157,fahrzeug:'Other'});
  assert.equal(mail.elements.text.value,'','new order requires a new mail scope');
  assert.equal(mail.elements.gesamt_brutto.value,'','new order requires a new sales price');

  // Returning to an order restores its own mail draft, never another order's fields.
  await f.changeOrder({id:156,fahrzeug:'Audi Test'});
  assert.equal(mail.elements.text.value,'Parts for order 156');
  assert.equal(mail.elements.gesamt_brutto.value,'1200');
  pending=deferred();f.handlers.set('/lieferanten',()=>pending.promise);
  await f.changeOrder({id:157,fahrzeug:'Other'});
  await mail.onsubmit(event());
  assert.equal(f.requests.filter(value=>value.route==='/vorschlag').length,0,'mail cannot prepare while target is loading');
  pending.resolve([]);await flush();

  // New intake explicitly ignores the old active order, and confirmed creation clears the source.
  f=await fixture();f.window.AssistantWorkflow.openUpload(null);await flush();
  assert.match(f.get('workflow-order').textContent,/Kein Auftrag/);
  f.test.review({...staged,analyse:{felder:{fahrzeug:'New Car',kennzeichen:'NEW-1'},hinweise:[]}});
  await f.get('workflow-review').onsubmit(event('new'));
  assert.equal(f.test.getNewSource(),staged.id);
  f.get('workflow-new-form').elements.kunde_name.value='Explicit new customer';
  f.get('workflow-close').onclick();await f.open('new');
  assert.equal(f.get('workflow-new-form').elements.kunde_name.value,'Explicit new customer','close must preserve deliberate draft');
  f.window.AssistantWorkflow.created();
  assert.equal(f.test.getNewSource(),null);
  assert.equal(f.get('workflow-new-form').elements.kunde_name.value,'');

  // Choosing another upload during OCR must not be overwritten by the first OCR reply.
  f=await fixture();const next={...staged,id:'upload-b',original_name:'second.png'};
  f.handlers.set('/unterlagen',[staged,next]);pending=deferred();f.handlers.set('/unterlagen/upload-a/analyse',()=>pending.promise);
  await f.open('upload');const analyzing=f.findButton('Angaben auslesen').onclick(event());await flush();
  const reviewButtons=f.get('workflow-uploads').querySelectorAll('button').filter(value=>value.textContent==='Angaben prüfen');
  await reviewButtons[1].onclick(event());assert.equal(f.test.getSelected().id,'upload-b');
  pending.resolve({...staged,analyse:{felder:{fahrbcode:'OLD'},hinweise:[]}});await analyzing;
  assert.equal(f.test.getSelected().id,'upload-b','late OCR may refresh the list but must not select an old document');

  // Even the same document's delayed analysis must preserve a hand-corrected field.
  f=await fixture();const old={...staged,analyse:{felder:{farbcode:'OCR-OLD'},hinweise:[]}};
  f.handlers.set('/unterlagen',[old]);pending=deferred();f.handlers.set('/unterlagen/upload-a/analyse',()=>pending.promise);
  await f.open('upload');await f.findButton('Angaben prüfen').onclick(event());
  const correcting=f.findButton('Angaben auslesen').onclick(event());await flush();
  const edited=f.get('workflow-review-fields').querySelectorAll('input').find(input=>input.name==='farbcode');
  edited.value='MANUELL-KORREKT';await f.get('workflow-review').emit('input');
  pending.resolve({...staged,analyse:{felder:{farbcode:'LATE-OCR'},hinweise:[]}});await correcting;
  assert.equal(f.get('workflow-review-fields').querySelectorAll('input').find(input=>input.name==='farbcode').value,'MANUELL-KORREKT');
  f.handlers.set('/vorschlag',{id:'color-proposal',status:'vorschlag'});
  await f.get('workflow-review').onsubmit(event('color'));
  assert.equal(f.requests.find(value=>value.route==='/vorschlag').data.felder.farbcode,'MANUELL-KORREKT');

  console.log('Workflow UI lifecycle checks passed.');
})().catch(error=>{console.error(error);process.exitCode=1;});
