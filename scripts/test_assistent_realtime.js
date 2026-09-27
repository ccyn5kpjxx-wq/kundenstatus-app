const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
function fixture(){
  let stopped=0;const timers=new Map();let timer=0;const sent=[],states=[],errors=[],texts=[],calls=[];
  const stream={getTracks:()=>[{stop:()=>stopped++}]};
  const channel={readyState:'open',send:value=>sent.push(JSON.parse(value)),close(){this.readyState='closed';this.onclose?.();}};
  class PC{constructor(){this.connectionState='new';}addTrack(){}createDataChannel(){return channel;}async createOffer(){return {sdp:'v=0\r\ntest'};}async setLocalDescription(){}async setRemoteDescription(){channel.onopen();}close(){this.connectionState='closed';this.onconnectionstatechange?.();}}
  const audio={muted:false,play:async()=>{},pause(){},removeAttribute(){}};
  const context={window:{isSecureContext:true,RTCPeerConnection:PC},RTCPeerConnection:PC,navigator:{mediaDevices:{getUserMedia:async()=>stream}},AbortController,
    setTimeout:(fn)=>{timers.set(++timer,fn);return timer;},clearTimeout:id=>timers.delete(id),setInterval:(fn)=>{timers.set(++timer,fn);return timer;},clearInterval:id=>timers.delete(id)};
  vm.createContext(context);vm.runInContext(fs.readFileSync('static/assistent-realtime.js','utf8'),context);
  const voice=new context.window.AssistantRealtime({audio,api:async(path,data)=>{calls.push({path,data});return {sdp:'v=0\r\nanswer',instructions:'current',result:{id:156},event:{type:'auftrag',data:{id:156}}};},onState:(...args)=>states.push(args),onError:e=>errors.push(e),onText:(...args)=>texts.push(args),onEvent:async()=>{}});
  return {voice,audio,stream,context,channel,sent,states,errors,texts,calls,timers,stopped:()=>stopped};
}
(async()=>{
  let f=fixture();await f.voice.start(156);
  assert.equal(f.voice.active,true);assert.equal(f.calls[0].data.auftrag_id,156);
  await f.voice.event({type:'output_audio_buffer.started'});
  assert.equal(f.audio.muted,false);
  await f.voice.event({type:'input_audio_buffer.speech_started'});
  assert.equal(f.audio.muted,true);assert.equal(f.stopped(),0,'mic must remain live during interruption');
  await f.voice.event({type:'output_audio_buffer.started'});assert.equal(f.audio.muted,false);
  await f.voice.event({type:'response.function_call_arguments.done',name:'auftrag_lesen',arguments:'{"auftrag_id":156}',call_id:'test'});
  assert.equal(f.sent.at(-1).type,'response.create');
  await f.voice.refresh();assert.equal(f.sent.at(-1).type,'session.update');
  f.voice.stop();assert.equal(f.stopped(),1);assert.equal(f.audio.srcObject,null);assert.equal(f.timers.size,0);assert.equal(f.errors.length,0);
  f=fixture();let release;f.context.navigator.mediaDevices.getUserMedia=()=>new Promise(r=>release=r);
  const starting=f.voice.start();f.voice.stop();release(f.stream);await starting;assert.equal(f.stopped(),1);assert.equal(f.calls.length,0);
  f=fixture();await f.voice.start();f.voice.api=async()=>{throw new Error('401');};await f.voice.refresh();assert.equal(f.voice.active,false);assert.equal(f.stopped(),1);
  f=fixture();await f.voice.start();f.voice.onEvent=async()=>f.voice.stop();await f.voice.event({type:'response.function_call_arguments.done',name:'aktion_vorschlagen',arguments:'{}',call_id:'test'});assert.equal(f.sent.length,0,'no competing realtime response during confirmation');
  console.log('Realtime lifecycle, barge-in, late microphone, revoked access and confirmation handoff passed.');
})().catch(error=>{console.error(error);process.exitCode=1;});
