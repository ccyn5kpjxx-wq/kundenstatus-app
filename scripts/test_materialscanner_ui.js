'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {createMaterialCodeScanner} = require('../static/materialscanner.js');
const settle = async () => {for (let n=0;n<8;n++) await new Promise(resolve=>setImmediate(resolve));};
const deferred = () => {let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
function fixture({supported=true, formats=['qr_code','ean_13'], camera=null, detect=null, blob=null, play=null}={}) {
  const events={}, timers=new Map(), received=[], selected=[], detectorCalls=[];let timer=0,allowed=true,stops=0;
  const document={activeElement:null, hidden:false, addEventListener:(name,fn)=>{events[name]=fn;}};
  const el=()=>({hidden:false,disabled:false,events:{},focus(){document.activeElement=this;},addEventListener(name,fn){this.events[name]=fn;},click(){this.events.click?.({});}});
  const ids=Object.fromEntries(['dialog','video','status','capture','close','fallback'].map(name=>[name,el()]));
  ids.dialog.hidden=true;
  Object.assign(ids.video,{readyState:2,videoWidth:640,videoHeight:480,srcObject:null,play:async()=>{if(play)await play.promise;},pause(){this.paused=true;}});
  document.getElementById=id=>ids[id.replace('material-scan-','')];
  document.createElement=()=>({getContext:()=>({drawImage(){}}),toBlob(fn){if(blob) blob.promise.then(fn);else fn({size:120,type:'image/jpeg'});}});
  const stream={getTracks:()=>[{stop(){stops++;}}]};
  class Detector {
    static async getSupportedFormats(){return formats;}
    constructor(options){selected.push(options.formats);}
    async detect(video){detectorCalls.push(video);return detect?detect(video):[];}
  }
  class File {constructor(parts,name,options){this.parts=parts;this.name=name;this.type=options.type;this.size=120;}}
  const window={isSecureContext:true,addEventListener:(name,fn)=>{events[name]=fn;},File};
  const getUserMediaCalls=[];
  const scanner=createMaterialCodeScanner({document,window,BarcodeDetector:supported?Detector:null,
    mediaDevices:{getUserMedia(settings){getUserMediaCalls.push(settings);return camera?camera.promise:Promise.resolve(stream);}},
    canAdd:()=>allowed,onPhoto:file=>received.push(file),onFallback:()=>received.push('fallback'),File,
    setTimeout:(fn,ms)=>{timers.set(++timer,{fn,ms});return timer;},clearTimeout:id=>timers.delete(id)});
  return {scanner,ids,received,selected,stream,events,timers,document,getUserMediaCalls,detectorCalls,
    get stops(){return stops;},setAllowed(value){allowed=value;},fireTimer(ms){for(const [id,entry]of [...timers])if(entry.ms===ms){timers.delete(id);entry.fn();}}};
}
test('unsupported live scanner keeps explicit photo fallback without requesting camera',async()=>{
  const f=fixture({supported:false});await f.scanner.open();
  assert.equal(f.ids.dialog.hidden,false);assert.match(f.ids.status.textContent,/Fotografiere/);
  assert.equal(f.getUserMediaCalls.length,0);f.ids.fallback.click();
  assert.deepEqual(f.received,['fallback']);assert.equal(f.ids.dialog.hidden,true);
});
test('formats are feature detected and camera uses environment without audio',async()=>{
  const f=fixture({formats:['qr_code','unknown']});await f.scanner.open();
  assert.deepEqual(f.selected,[['qr_code']]);assert.equal(f.getUserMediaCalls[0].audio,false);
  assert.deepEqual(f.getUserMediaCalls[0].video.facingMode,{ideal:'environment'});
  assert.equal(f.ids.video.muted,true);assert.equal(f.ids.video.playsInline,true);
  f.scanner.stop();assert.equal(f.stops,1);assert.equal(f.ids.video.srcObject,null);
});
test('missing formats or failed platform feature check keep photo fallback without camera',async()=>{
  const f=fixture({formats:['unsupported']});await f.scanner.open();
  assert.equal(f.getUserMediaCalls.length,0);assert.match(f.ids.status.textContent,/keinen direkten Scan/);
  f.ids.fallback.click();assert.deepEqual(f.received,['fallback']);assert.equal(f.timers.size,0);
  const gate=deferred(),g=fixture({formats:gate.promise});const opening=g.scanner.open();
  gate.reject(new Error('platform'));await opening;
  assert.equal(g.getUserMediaCalls.length,0);assert.equal(g.ids.dialog.hidden,false);
  g.ids.fallback.click();assert.deepEqual(g.received,['fallback']);assert.equal(g.timers.size,0);
});
test('cancel during feature check cannot open camera later and next scanner start works',async()=>{
  const gate=deferred(),f=fixture({formats:gate.promise});const opening=f.scanner.open();await settle();
  f.ids.close.click();gate.resolve(['qr_code']);await opening;
  assert.equal(f.getUserMediaCalls.length,0);assert.equal(f.ids.dialog.hidden,true);assert.equal(f.timers.size,0);
  await f.scanner.open();assert.equal(f.getUserMediaCalls.length,1);assert.equal(f.ids.dialog.hidden,false);
  f.scanner.stop();assert.equal(f.stops,1);assert.equal(f.timers.size,0);
});
test('late video playback after cancel cannot resume detection or leave timers and tracks',async()=>{
  const gate=deferred(),f=fixture({play:gate});const opening=f.scanner.open();await settle();
  assert.equal(f.ids.video.srcObject,f.stream);f.ids.close.click();gate.resolve();await opening;
  assert.equal(f.stops,1);assert.equal(f.ids.video.srcObject,null);assert.equal(f.detectorCalls.length,0);
  assert.equal(f.timers.size,0);assert.equal(f.received.length,0);assert.equal(f.ids.dialog.hidden,true);
});
test('cancel while permission pending stops late camera tracks and never adds a photo',async()=>{
  const camera=deferred(),f=fixture({camera});const opening=f.scanner.open();await settle();
  f.ids.close.click();camera.resolve(f.stream);await opening;
  assert.equal(f.stops,1);assert.equal(f.ids.dialog.hidden,true);assert.equal(f.received.length,0);
});
test('permission timeout stops a camera resolving later',async()=>{
  const camera=deferred(),f=fixture({camera});const opening=f.scanner.open();await settle();f.fireTimer(30000);
  camera.resolve(f.stream);await opening;assert.equal(f.stops,1);assert.match(f.ids.status.textContent,/noch nicht bereit/);
  assert.equal(f.received.length,0);
});
test('one QR or URL code captures only an image and never transfers decoded instructions',async()=>{
  const f=fixture({detect:async()=>[{rawValue:'https://evil.example/9-dringend',format:'qr_code'}]});await f.scanner.open();await settle();
  assert.equal(f.received.length,1);assert.equal(f.received[0].name,'artikelcode.jpg');assert.equal(f.received[0].type,'image/jpeg');
  assert.equal('rawValue' in f.received[0],false);assert.equal(f.stops,1);assert.equal(f.ids.dialog.hidden,true);
});
test('multiple codes do not select or duplicate an article',async()=>{
  const f=fixture({detect:async()=>[{rawValue:'a'},{rawValue:'b'}]});await f.scanner.open();await settle();
  assert.equal(f.received.length,0);assert.match(f.ids.status.textContent,/Mehrere Codes/);f.scanner.stop();
});
test('cancel during in-flight detection cannot later capture',async()=>{
  const gate=deferred(),f=fixture({detect:()=>gate.promise});await f.scanner.open();f.scanner.stop();
  gate.resolve([{rawValue:'10035120'}]);await settle();assert.equal(f.received.length,0);assert.equal(f.stops,1);
});
test('cancel during canvas conversion cannot later create a request',async()=>{
  const gate=deferred(),f=fixture({blob:gate});await f.scanner.open();f.ids.capture.click();await settle();f.ids.close.click();
  gate.resolve({size:120});await settle();assert.equal(f.received.length,0);assert.equal(f.stops,1);
});
test('camera denied and detector failure both retain usable photo fallback',async()=>{
  const camera=deferred(),f=fixture({camera});const opening=f.scanner.open();camera.reject(new Error('denied'));await opening;
  assert.match(f.ids.status.textContent,/nicht geöffnet/);assert.equal(f.ids.dialog.hidden,false);f.ids.fallback.click();assert.equal(f.received[0],'fallback');
  const g=fixture({detect:async()=>{throw new Error('platform');}});await g.scanner.open();await settle();assert.equal(g.stops,1);
  assert.match(g.ids.status.textContent,/direkte Scan/);g.ids.fallback.click();assert.equal(g.received[0],'fallback');
});
test('pagehide, hidden page and escape stop the camera',async()=>{
  for(const event of ['pagehide','visibilitychange','Escape']){
    const f=fixture();await f.scanner.open();
    if(event==='Escape')f.ids.dialog.events.keydown({key:event,preventDefault(){}});
    else {f.document.hidden=true;f.events[event]();}
    assert.equal(f.stops,1);assert.equal(f.ids.dialog.hidden,true);assert.equal(f.timers.size,0);
  }
});
test('locked form and double start never create additional camera requests',async()=>{
  const f=fixture();f.setAllowed(false);await f.scanner.open();assert.equal(f.getUserMediaCalls.length,0);
  f.setAllowed(true);await f.scanner.open();await f.scanner.open();assert.equal(f.getUserMediaCalls.length,1);f.scanner.stop();
});
test('capture after a changed identity or full batch does not append',async()=>{
  const f=fixture();await f.scanner.open();f.setAllowed(false);f.ids.capture.click();await settle();
  assert.equal(f.received.length,0);f.scanner.stop();
});
