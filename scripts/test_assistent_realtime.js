// Offline WebRTC lifecycle tests: no device permissions, requests or real audio.
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const flush=()=>new Promise(setImmediate);
function deferred(){let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};}
function fixture(){
  const behaviour={},timers=new Map(),sent=[],states=[],errors=[],texts=[],calls=[],events=[],pcs=[],streams=[],remoteStreams=[],blocked=[];
  let timer=0,stopped=0;
  function newStream(){
    const handlers=new Map();
    const track={kind:'audio',stop(){stopped++;},addEventListener:(name,fn)=>handlers.set(name,fn),removeEventListener:(name,fn)=>{if(handlers.get(name)===fn)handlers.delete(name);}};
    const stream={getTracks:()=>[track],track,handlers};streams.push(stream);return stream;
  }
  class Stream{constructor(tracks){this.tracks=tracks;}getTracks(){return this.tracks;}}
  class PC{
    constructor(){
      this.connectionState='new';pcs.push(this);
      this.channel={readyState:'open',send:value=>sent.push({pc:this,event:JSON.parse(value)}),close(){this.readyState='closed';this.onclose?.();}};
    }
    addTrack(){}
    createDataChannel(){return this.channel;}
    async createOffer(){return behaviour.offer?behaviour.offer():{sdp:'v=0\r\nsynthetic-offer'};}
    async setLocalDescription(){if(behaviour.local)return behaviour.local();}
    async setRemoteDescription(){if(behaviour.remote)return behaviour.remote();if(behaviour.autoOpen!==false)this.channel.onopen?.();}
    close(){this.connectionState='closed';this.onconnectionstatechange?.();}
  }
  const audio={muted:false,srcObject:null,plays:0,play(){this.plays++;return behaviour.play?behaviour.play():Promise.resolve();},pause(){},removeAttribute(){}};
  const context={window:{isSecureContext:true,RTCPeerConnection:PC},RTCPeerConnection:PC,MediaStream:Stream,
    navigator:{mediaDevices:{getUserMedia:()=>behaviour.microphone?behaviour.microphone():Promise.resolve(newStream())}},AbortController,
    setTimeout:(fn,ms)=>{timers.set(++timer,{fn,ms});return timer;},clearTimeout:id=>timers.delete(id),
    setInterval:(fn,ms)=>{timers.set(++timer,{fn,ms,interval:true});return timer;},clearInterval:id=>timers.delete(id)};
  vm.createContext(context);vm.runInContext(fs.readFileSync(path.join(__dirname,'..','static','assistent-realtime.js'),'utf8'),context);
  const voice=new context.window.AssistantRealtime({audio,
    api:async(url,data,raw,signal)=>{
      calls.push({url,data,signal});if(behaviour.api)return behaviour.api(url,data,signal);
      return {sdp:'v=0\r\nsynthetic-answer',instructions:'current',result:{id:156},event:{type:'auftrag',data:{id:156}}};
    },onState:(...args)=>states.push(args),onError:error=>errors.push(error),onText:(...args)=>texts.push(args),
    onEvent:async event=>{events.push(event);if(behaviour.event)return behaviour.event(event);},
    onPlaybackBlocked:(...args)=>blocked.push(args),onRemoteStream:stream=>remoteStreams.push(stream)});
  const fire=ms=>{const entry=[...timers.entries()].find(([,value])=>value.ms===ms);assert.ok(entry,`expected timer ${ms}`);if(!entry[1].interval)timers.delete(entry[0]);entry[1].fn();};
  return {voice,audio,behaviour,context,timers,states,errors,texts,calls,events,pcs,streams,sent,remoteStreams,blocked,newStream,fire,stopped:()=>stopped};
}
const toolEvent={type:'response.function_call_arguments.done',name:'auftrag_lesen',arguments:'{"auftrag_id":156}',call_id:'synthetic'};
(async()=>{
  let f=fixture();const start=f.voice.start(156);
  assert.equal(f.states[0][2].phase,'microphone');assert.match(f.states[0][1],/Mikrofon/);
  await start;assert.equal(f.voice.active,true);assert.equal(f.calls[0].data.auftrag_id,156);
  assert.deepEqual(f.states.map(state=>state[2].phase),['microphone','connection','server','connection','connected']);
  assert.match(f.states[1][1],/vorbereitet/);assert.match(f.states[2][1],/KI-Sprachdienst/);
  await f.voice.event({type:'output_audio_buffer.started'});assert.equal(f.audio.muted,false);
  await f.voice.event({type:'input_audio_buffer.speech_started'});assert.equal(f.audio.muted,true);assert.equal(f.stopped(),0);
  await f.voice.event({type:'output_audio_buffer.started'});assert.equal(f.audio.muted,false);
  await f.voice.event(toolEvent);assert.equal(f.sent.at(-1).event.type,'response.create');
  await f.voice.refresh();assert.equal(f.sent.at(-1).event.type,'session.update');
  f.voice.stop();assert.equal(f.stopped(),1);assert.equal(f.audio.srcObject,null);assert.equal(f.timers.size,0);assert.equal(f.remoteStreams.at(-1),null);

  // A never-resolving permission prompt has its own deadline and no server call.
  f=fixture();let mic=deferred();f.behaviour.microphone=()=>mic.promise;
  let pending=f.voice.start();assert.equal(f.voice.phase,'microphone');f.fire(60000);await pending;
  assert.equal(f.calls.length,0);assert.equal(f.errors[0].phase,'microphone');assert.match(f.errors[0].message,/nicht bereitgestellt/);
  mic.resolve(f.newStream());await flush();assert.equal(f.stopped(),1);assert.equal(f.pcs.length,0);
  // Explicit stop also completes the start promise without waiting for permission.
  f=fixture();mic=deferred();f.behaviour.microphone=()=>mic.promise;pending=f.voice.start();f.voice.stop();await pending;
  assert.equal(f.errors.length,0);mic.resolve(f.newStream());await flush();assert.equal(f.stopped(),1);
  for(const [name,word] of [['NotAllowedError',/nicht erlaubt/],['NotFoundError',/Kein Mikrofon/],['NotReadableError',/nicht öffnen/],['SecurityError',/blockiert/]]){
    f=fixture();f.behaviour.microphone=()=>Promise.reject(Object.assign(new Error('private device details'),{name}));await f.voice.start();
    assert.equal(f.voice.active,false);assert.match(f.errors[0].message,word);assert.equal(f.errors[0].phase,'microphone');
  }

  // A stuck offer is distinguishable from a stuck server call.
  f=fixture();const offer=deferred();f.behaviour.offer=()=>offer.promise;pending=f.voice.start();await flush();
  assert.equal(f.voice.phase,'connection');assert.match(f.states.at(-1)[1],/vorbereitet/);assert.equal(f.calls.length,0);
  f.fire(25000);await pending;assert.match(f.errors[0].message,/vorbereiten/);offer.resolve({sdp:'v=0\r\nlate'});await flush();assert.equal(f.calls.length,0);
  f=fixture();const server=deferred();f.behaviour.api=()=>server.promise;pending=f.voice.start();await flush();
  assert.equal(f.voice.phase,'server');assert.ok([...f.timers.values()].some(timer=>timer.ms===80000));
  assert.equal([...f.timers.values()].some(timer=>timer.ms===25000),false);
  f.fire(80000);await pending;assert.equal(f.calls[0].signal.aborted,true);assert.equal(f.errors[0].phase,'server');
  assert.match(f.errors[0].message,/Mikrofonfreigabe ist vorhanden/);server.reject(new Error('late server rejection'));await flush();assert.equal(f.errors.length,1);
  f=fixture();f.behaviour.autoOpen=false;await f.voice.start();assert.equal(f.voice.phase,'connection');f.fire(25000);
  assert.match(f.errors[0].message,/Audioverbindung kam nicht zustande/);
  f=fixture();f.behaviour.api=async()=>({sdp:'invalid'});await f.voice.start();assert.match(f.errors[0].message,/gültige Verbindungsantwort/);

  // Every old PC/channel/microphone/timer callback stays harmless after restart.
  f=fixture();await f.voice.start();
  const oldPc=f.voice.pc,oldChannel=f.voice.channel;
  const callbacks={track:oldPc.ontrack,state:oldPc.onconnectionstatechange,open:oldChannel.onopen,message:oldChannel.onmessage,close:oldChannel.onclose,error:oldChannel.onerror,ended:f.streams[0].handlers.get('ended')};
  const oldTimers=[...f.timers.values()].map(timer=>timer.fn);
  f.voice.stop();await f.voice.start();const freshPc=f.voice.pc,count=f.timers.size;
  callbacks.track({streams:[f.newStream()]});callbacks.state();callbacks.open();callbacks.close();callbacks.error();callbacks.ended();
  callbacks.message({data:JSON.stringify({type:'input_audio_buffer.speech_started'})});oldTimers.forEach(callback=>callback());
  assert.equal(f.voice.active,true);assert.equal(f.voice.pc,freshPc);assert.equal(f.audio.muted,false);assert.equal(f.audio.srcObject,null);
  assert.equal(f.timers.size,count);assert.equal(f.errors.length,0);assert.equal(f.texts.length,0);f.voice.stop();

  // A rejected old play() promise cannot stop a fresh session.
  f=fixture();await f.voice.start();const play=deferred();f.behaviour.play=()=>play.promise;
  f.voice.pc.ontrack({streams:[f.newStream()]});f.voice.stop();delete f.behaviour.play;await f.voice.start();
  play.reject(Object.assign(new Error('old pause'),{name:'AbortError'}));await flush();
  assert.equal(f.voice.active,true);assert.equal(f.errors.length,0);f.voice.stop();

  // Old refresh results and cleanup cannot update/unlock a newer refresh.
  for(const rejectOld of [false,true]){
    f=fixture();await f.voice.start();const oldRefresh=deferred();f.behaviour.api=()=>oldRefresh.promise;
    const refreshing=f.voice.refresh();f.voice.stop();delete f.behaviour.api;await f.voice.start();
    const newRefresh=deferred();f.behaviour.api=()=>newRefresh.promise;const newer=f.voice.refresh();
    if(rejectOld)oldRefresh.reject(new Error('obsolete refresh'));else oldRefresh.resolve({instructions:'obsolete'});
    await refreshing;assert.equal(f.voice.active,true);assert.equal(f.voice.refreshing,true);assert.equal(f.errors.length,0);assert.equal(f.sent.length,0);
    const calls=f.calls.length;await f.voice.refresh();assert.equal(f.calls.length,calls);
    newRefresh.resolve({instructions:'new'});await newer;assert.equal(f.sent.length,1);assert.equal(f.sent[0].event.session.instructions,'new');f.voice.stop();
  }
  f=fixture();await f.voice.start();f.behaviour.api=async()=>{throw new Error('401');};await f.voice.refresh();assert.equal(f.voice.active,false);assert.equal(f.stopped(),1);

  // Tools and their UI handoff also retain the originating session.
  for(const rejectOld of [false,true]){
    f=fixture();await f.voice.start();const tool=deferred();f.behaviour.api=()=>tool.promise;
    const working=f.voice.event(toolEvent);f.voice.stop();delete f.behaviour.api;await f.voice.start(380);
    if(rejectOld)tool.reject(new Error('old tool error'));else tool.resolve({result:{id:156},event:{type:'auftrag',data:{id:156}}});
    await working;assert.equal(f.voice.order,380);assert.equal(f.events.length,0);assert.equal(f.sent.length,0);assert.equal(f.errors.length,0);f.voice.stop();
  }
  f=fixture();await f.voice.start();const handoff=deferred();f.behaviour.event=()=>handoff.promise;
  const working=f.voice.event(toolEvent);await flush();f.voice.stop();await f.voice.start();handoff.reject(new Error('old UI error'));await working;
  assert.equal(f.voice.active,true);assert.equal(f.sent.length,0);assert.equal(f.errors.length,0);f.voice.stop();
  f=fixture();await f.voice.start();f.behaviour.event=async()=>f.voice.stop();await f.voice.event(toolEvent);assert.equal(f.sent.length,0);

  // Blocked autoplay retains mic/data channel and offers a real user-gesture retry.
  f=fixture();await f.voice.start();f.behaviour.play=()=>Promise.reject(Object.assign(new Error('denied'),{name:'NotAllowedError'}));
  const remote=f.newStream();f.voice.pc.ontrack({streams:[remote]});await flush();
  assert.equal(f.voice.active,true);assert.equal(f.stopped(),0);assert.equal(f.voice.playbackBlocked,true);assert.equal(f.blocked.length,1);
  assert.equal(f.remoteStreams.at(-1),remote);assert.equal(f.errors.length,0);
  await f.voice.event({type:'output_audio_buffer.started'});assert.notEqual(f.states.at(-1)[0],'speaking');
  assert.equal(await f.voice.resumePlayback(),false);delete f.behaviour.play;
  assert.equal(await f.voice.resumePlayback(),true);assert.equal(f.voice.playbackBlocked,false);assert.equal(f.states.at(-1)[0],'speaking');
  const oldRetry=f.blocked[0][1].retry;f.voice.stop();await f.voice.start();const plays=f.audio.plays;
  assert.equal(await oldRetry(),false);assert.equal(f.audio.plays,plays);f.voice.stop();assert.equal(await f.voice.resumePlayback(),false);

  // Streamless track events work, and optional meter failures cannot affect speech.
  f=fixture();await f.voice.start();f.voice.onRemoteStream=()=>{throw new Error('optional meter failure');};
  const remoteTrack={kind:'audio'};f.voice.pc.ontrack({streams:[],track:remoteTrack});await flush();
  assert.equal(f.audio.srcObject.getTracks()[0],remoteTrack);assert.equal(f.voice.active,true);assert.equal(f.errors.length,0);f.voice.stop();
  f=fixture();await f.voice.start();assert.doesNotThrow(()=>f.voice.channel.onmessage({data:'invalid JSON'}));assert.equal(f.voice.active,false);assert.match(f.errors[0].message,/ungültige Nachricht/);
  console.log('PASS: startup phases/timeouts, permission errors, old callbacks, playback retry, refresh/tool isolation, barge-in and cleanup.');
})().catch(error=>{console.error(error);process.exitCode=1;});
