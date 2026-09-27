// Device-independent lifecycle tests. No browser permissions or network involved.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('static/assistent-voice.js','utf8');
function fixture() {
  let now=0, level=0, stopped=0, closed=0, frame, nextTimer=0;
  const timers=new Map(), recorders=[], errors=[], states=[];
  const stream={getTracks:()=>[{stop:()=>stopped++}]};
  class Recorder {
    static isTypeSupported(){return true;}
    constructor(){this.mimeType='audio/webm';this.state='inactive';recorders.push(this);}
    start(){this.state='recording';}
    stop(){this.state='inactive';this.ondataavailable?.({data:new Blob(['test'])});this.onstop?.();}
  }
  class AudioContext {
    state='running';
    async resume(){}
    async close(){closed++;this.state='closed';}
    createAnalyser(){return {getFloatTimeDomainData:a=>a.fill(level)};}
    createMediaStreamSource(){return {connect(){}};}
  }
  const context={window:{isSecureContext:true,AudioContext,MediaRecorder:Recorder},document:{hidden:false},navigator:{mediaDevices:{getUserMedia:async()=>stream}},MediaRecorder:Recorder,Blob,Float32Array,performance:{now:()=>now},setTimeout:(fn,ms)=>{timers.set(++nextTimer,{fn,ms});return nextTimer;},clearTimeout:id=>timers.delete(id),requestAnimationFrame:fn=>{frame=fn;return 1;},cancelAnimationFrame:()=>{frame=null;}};
  vm.createContext(context);vm.runInContext(source,context);
  let release, segments=0;
  const voice=new context.window.AssistantVoiceMode({onSegment:async()=>{segments++;await new Promise(resolve=>release=resolve);},onState:(...s)=>states.push(s),onError:e=>errors.push(e)});
  return {voice,context,timers,recorders,errors,states,get segments(){return segments;},get stopped(){return stopped;},get closed(){return closed;},release:()=>release(),tick:(time,volume)=>{now=time;level=volume;const callback=frame;frame=null;callback?.();}};
}
(async()=>{
  const f=fixture();await f.voice.start();assert.equal(f.recorders.length,1);
  for(let i=1;i<=5;i++)f.tick(i*30,.1);
  f.tick(1700,0);assert.equal(f.segments,1);assert.equal(f.recorders[0].state,'inactive');
  assert.equal(f.recorders.length,1,'No recording while response is pending');
  f.release();await new Promise(setImmediate);
  const resume=[...f.timers.values()].find(t=>t.ms===500);assert.ok(resume);resume.fn();assert.equal(f.recorders.length,2);
  f.voice.stop();assert.equal(f.stopped,1);assert.equal(f.closed,1);assert.equal(f.timers.size,0);
  const silent=fixture();await silent.voice.start();silent.tick(21000,0);assert.equal(silent.voice.active,false);assert.equal(silent.errors.length,1);assert.equal(silent.segments,0);
  const denied=fixture();denied.context.navigator.mediaDevices.getUserMedia=async()=>{throw new Error('denied');};await assert.rejects(()=>denied.voice.start());assert.equal(denied.voice.active,false);assert.equal(denied.closed,1);
  const late=fixture();let grant;late.context.navigator.mediaDevices.getUserMedia=()=>new Promise(r=>grant=r);const starting=late.voice.start();await new Promise(setImmediate);late.voice.stop();let tracksStopped=0;grant({getTracks:()=>[{stop:()=>tracksStopped++}]});await starting;assert.equal(tracksStopped,1);assert.equal(late.recorders.length,0);
  console.log('PASS: pause detection, recording suspended during reply, restart, stop cleanup, silence, denied and late permissions');
})().catch(error=>{console.error(error);process.exitCode=1;});
