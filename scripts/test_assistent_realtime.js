// Offline WebRTC lifecycle tests: no device permissions, requests or real audio.
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const flush=()=>new Promise(setImmediate);
function deferred(){let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};}
function fixture(){
  const behaviour={},timers=new Map(),sent=[],states=[],errors=[],texts=[],calls=[],events=[],pcs=[],streams=[],remoteStreams=[],blocked=[],fetchCalls=[];
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
    async setRemoteDescription(answer){this.remoteDescription=answer;if(behaviour.remote)return behaviour.remote();if(behaviour.autoOpen!==false)this.channel.onopen?.();}
    close(){this.connectionState='closed';this.onconnectionstatechange?.();}
  }
  const audio={muted:false,srcObject:null,plays:0,play(){this.plays++;return behaviour.play?behaviour.play():Promise.resolve();},pause(){},removeAttribute(){}};
  const context={window:{isSecureContext:true,RTCPeerConnection:PC},RTCPeerConnection:PC,MediaStream:Stream,
    navigator:{mediaDevices:{getUserMedia:()=>behaviour.microphone?behaviour.microphone():Promise.resolve(newStream())}},AbortController,
    fetch:async(url,options)=>{fetchCalls.push({url,options});if(behaviour.fetch)return behaviour.fetch(url,options);return {ok:true,status:201,text:async()=> 'v=0\r\nsynthetic-answer'};},
    setTimeout:(fn,ms)=>{timers.set(++timer,{fn,ms});return timer;},clearTimeout:id=>timers.delete(id),
    setInterval:(fn,ms)=>{timers.set(++timer,{fn,ms,interval:true});return timer;},clearInterval:id=>timers.delete(id)};
  vm.createContext(context);vm.runInContext(fs.readFileSync(path.join(__dirname,'..','static','assistent-realtime.js'),'utf8'),context);
  const voice=new context.window.AssistantRealtime({audio,
    api:async(url,data,raw,signal)=>{
      calls.push({url,data,signal});if(behaviour.api)return behaviour.api(url,data,signal);
      if(url==='/realtime/start')return {client_secret:'synthetic-ephemeral-only',expires_at:Math.floor(Date.now()/1000)+60};
      return {sdp:'v=0\r\nsynthetic-answer',instructions:'current',result:{id:156},event:{type:'auftrag',data:{id:156}}};
    },onState:(...args)=>states.push(args),onError:error=>errors.push(error),onText:(...args)=>texts.push(args),
    onEvent:async event=>{events.push(event);if(behaviour.event)return behaviour.event(event);},
    onPlaybackBlocked:(...args)=>blocked.push(args),onRemoteStream:stream=>remoteStreams.push(stream)});
  const fire=ms=>{const entry=[...timers.entries()].find(([,value])=>value.ms===ms);assert.ok(entry,`expected timer ${ms}`);if(!entry[1].interval)timers.delete(entry[0]);entry[1].fn();};
  return {voice,audio,behaviour,context,timers,states,errors,texts,calls,events,pcs,streams,sent,remoteStreams,blocked,fetchCalls,newStream,fire,stopped:()=>stopped};
}
const toolEvent={type:'response.function_call_arguments.done',name:'auftrag_lesen',arguments:'{"auftrag_id":156}',call_id:'synthetic'};
(async()=>{
  let f=fixture();const start=f.voice.start(156);
  assert.equal(f.states[0][2].phase,'microphone');assert.match(f.states[0][1],/Mikrofon/);
  await start;assert.equal(f.voice.active,true);assert.equal(f.calls[0].data.auftrag_id,156);
  assert.equal(f.calls[0].data.transport,'browser');assert.equal(f.calls[0].data.sdp,'v=0\r\nsynthetic-offer');
  assert.equal(f.fetchCalls.length,1);
  assert.equal(f.fetchCalls[0].url,'https://api.openai.com/v1/realtime/calls');
  const directRequest=f.fetchCalls[0].options;
  assert.equal(directRequest.method,'POST');assert.equal(directRequest.body,'v=0\r\nsynthetic-offer');
  assert.equal(directRequest.headers.Authorization,'Bearer synthetic-ephemeral-only');
  assert.equal(directRequest.headers['Content-Type'],'application/sdp');
  assert.equal(directRequest.signal,f.calls[0].signal);
  assert.equal(directRequest.credentials,'omit');assert.equal(directRequest.redirect,'error');
  assert.equal(directRequest.cache,'no-store');assert.equal(directRequest.referrerPolicy,'no-referrer');
  assert.equal(f.voice.pc.remoteDescription.sdp,'v=0\r\nsynthetic-answer');
  assert.doesNotMatch(JSON.stringify({voice:f.voice,states:f.states,texts:f.texts}),/synthetic-ephemeral-only/,'credential is not retained on the controller or exposed to UI callbacks');
  assert.deepEqual(f.states.map(state=>state[2].phase),['microphone','connection','server','server','connection','connected']);
  assert.match(f.states[1][1],/vorbereitet/);assert.match(f.states[2][1],/KI-Sprachdienst/);
  await f.voice.event({type:'output_audio_buffer.started'});assert.equal(f.audio.muted,false);
  await f.voice.event({type:'input_audio_buffer.speech_started'});assert.equal(f.audio.muted,true);assert.equal(f.stopped(),0);
  await f.voice.event({type:'output_audio_buffer.started'});assert.equal(f.audio.muted,false);
  await f.voice.event(toolEvent);assert.equal(f.sent.at(-1).event.type,'response.create');
  await f.voice.refresh();assert.equal(f.sent.at(-1).event.type,'session.update');
  f.voice.stop();assert.equal(f.stopped(),1);assert.equal(f.audio.srcObject,null);assert.equal(f.timers.size,0);assert.equal(f.remoteStreams.at(-1),null);

  // Invalid credentials cannot create a provider request, and API response data
  // cannot substitute a destination or weaken the fixed no-redirect policy.
  for(const access of [{},{client_secret:'',expires_at:Date.now()/1000+60},
    {client_secret:'synthetic-expired',expires_at:1},
    {client_secret:'synthetic-invalid-expiry',expires_at:'9999999999'}]){
    f=fixture();f.behaviour.api=async()=>access;await f.voice.start();
    assert.equal(f.fetchCalls.length,0);assert.equal(f.voice.active,false);
    assert.match(f.errors[0].message,/kurzlebige Sprachzugang/);assert.equal(f.stopped(),1);
    assert.doesNotMatch(f.errors[0].message,/synthetic/);
  }
  f=fixture();f.behaviour.api=async()=>({client_secret:'synthetic-ephemeral-only',expires_at:Date.now()/1000+60,url:'https://untrusted.example.invalid/receive'});
  await f.voice.start();assert.equal(f.fetchCalls[0].url,'https://api.openai.com/v1/realtime/calls');f.voice.stop();
  f=fixture();const obsoleteAccess=deferred();f.behaviour.api=()=>obsoleteAccess.promise;
  const obsoleteStart=f.voice.start();await flush();f.voice.stop();await obsoleteStart;
  delete f.behaviour.api;await f.voice.start();const freshConnection=f.voice.pc;
  obsoleteAccess.resolve({client_secret:'obsolete-ephemeral-only',expires_at:Date.now()/1000+60});await flush();
  assert.equal(f.fetchCalls.length,1,'a credential delivered after Stop must never reach the provider');
  assert.equal(f.voice.pc,freshConnection);assert.equal(f.voice.active,true);assert.equal(f.errors.length,0);f.voice.stop();

  // Never read or display provider error bodies; all direct failures remain
  // finite and release the microphone without retrying or changing API mode.
  for(const status of [400,401,403,429,500,504]){
    f=fixture();let reads=0;
    f.behaviour.fetch=async()=>({ok:false,status,text:async()=>{reads++;return 'PRIVATE_TOKEN_AND_ACCOUNT';}});
    await f.voice.start();assert.equal(reads,0);assert.equal(f.errors.length,1);
    assert.doesNotMatch(f.errors[0].message,/PRIVATE|synthetic/);assert.equal(f.voice.active,false);
    assert.equal(f.stopped(),1);assert.equal(f.timers.size,0);assert.equal(f.fetchCalls.length,1);assert.equal(f.calls.length,1);
  }
  for(const where of ['fetch','body']){
    f=fixture();const fail=async()=>{throw new Error('PRIVATE_TOKEN_AND_ACCOUNT');};
    f.behaviour.fetch=where==='fetch'?fail:async()=>({ok:true,text:fail});
    await f.voice.start();assert.equal(f.voice.active,false);assert.equal(f.errors.length,1);
    assert.doesNotMatch(f.errors[0].message,/PRIVATE_TOKEN_AND_ACCOUNT/);assert.equal(f.stopped(),1);
  }

  // Stop wins during both direct fetch and body reading. A late old response
  // or rejection cannot set an SDP, play audio, or stop a freshly started call.
  for(const where of ['fetch','body'])for(const rejectOld of [false,true]){
    f=fixture();const exchange=deferred();let lateBodyReads=0;
    f.behaviour.fetch=where==='fetch'?()=>exchange.promise:async()=>({ok:true,text:()=>exchange.promise});
    const opening=f.voice.start();await flush();const oldPc=f.voice.pc,oldSignal=f.fetchCalls[0].options.signal;
    f.voice.stop();await opening;assert.equal(oldSignal.aborted,true);assert.equal(f.stopped(),1);
    delete f.behaviour.fetch;await f.voice.start();const latestPc=f.voice.pc;
    if(rejectOld)exchange.reject(new Error('PRIVATE_OLD_FAILURE'));
    else exchange.resolve(where==='fetch'?{ok:true,text:async()=>{lateBodyReads++;return 'v=0\r\nobsolete-answer';}}:'v=0\r\nobsolete-answer');
    await flush();assert.equal(lateBodyReads,0);assert.equal(oldPc.remoteDescription,undefined);
    assert.equal(f.voice.pc,latestPc);assert.equal(latestPc.remoteDescription.sdp,'v=0\r\nsynthetic-answer');
    assert.equal(f.voice.active,true);assert.equal(f.errors.length,0);f.voice.stop();
  }

  // Token retrieval and direct SDP/body exchange share the existing 80-second
  // deadline; the second network leg must not silently reset that budget.
  for(const where of ['fetch','body']){
    f=fixture();const access=deferred(),exchange=deferred();
    f.behaviour.api=()=>access.promise;
    f.behaviour.fetch=where==='fetch'?()=>exchange.promise:async()=>({ok:true,text:()=>exchange.promise});
    const opening=f.voice.start();await flush();
    const deadline=[...f.timers.entries()].find(([,value])=>value.ms===80000)[0];
    access.resolve({client_secret:'synthetic-ephemeral-only',expires_at:Date.now()/1000+60});await flush();
    assert.equal([...f.timers.entries()].find(([,value])=>value.ms===80000)[0],deadline);
    const signal=f.fetchCalls[0].options.signal;f.fire(80000);await opening;
    assert.equal(signal.aborted,true);assert.equal(f.voice.active,false);assert.equal(f.stopped(),1);
    assert.equal(f.errors.length,1);assert.equal(f.errors[0].phase,'server');assert.equal(f.timers.size,0);
    exchange.reject(new Error('PRIVATE_LATE_TIMEOUT'));await flush();assert.equal(f.errors.length,1);
  }

  // Provider response failures arrive as response.done, not necessarily as an
  // error event. They must stop the waiting state and release all resources,
  // without exposing raw provider/account details or attempting another call.
  for(const [code,expected] of [
    ['server_error',/keine Antwort erzeugen/],['rate_limit_exceeded',/ausgelastet/],
    ['insufficient_quota',/Werkstattleitung/],['billing_hard_limit_reached',/Werkstattleitung/],
    ['unknown_private_code',/keine Antwort erzeugen/]
  ]){
    f=fixture();await f.voice.start();
    await f.voice.event({type:'input_audio_buffer.speech_started'});
    await f.voice.event({type:'input_audio_buffer.speech_stopped'});
    await f.voice.event({type:'response.created',response:{id:'synthetic-failure'}});
    f.voice.channel.onmessage({data:JSON.stringify({type:'response.done',response:{id:'synthetic-failure',status:'failed',
      status_details:{type:'failed',error:{code,message:'PRIVATE_SYNTHETIC_ACCOUNT_DATA'}}}})});
    await flush();
    assert.equal(f.errors.length,1);assert.match(f.errors[0].message,expected);
    assert.doesNotMatch(f.errors[0].message,/PRIVATE_SYNTHETIC|unknown_private_code|server_error/);
    assert.equal(f.errors[0].phase,'connected');assert.equal(f.voice.active,false);
    assert.equal(f.stopped(),1);assert.equal(f.audio.srcObject,null);assert.equal(f.audio.muted,false);
    assert.equal(f.timers.size,0);assert.equal(f.calls[0].signal.aborted,true);
    assert.equal(f.calls.length,1);assert.equal(f.sent.length,0,'a failed answer is never retried or confirmed automatically');
  }
  for(const reason of ['max_output_tokens','content_filter']){
    f=fixture();await f.voice.start();
    await f.voice.event({type:'response.created',response:{id:'synthetic-incomplete'}});
    await f.voice.event({type:'output_audio_buffer.started',response_id:'synthetic-incomplete'});
    await f.voice.event({type:'response.done',response:{id:'synthetic-incomplete',status:'incomplete',status_details:{reason}}});
    assert.equal(f.voice.active,false);assert.equal(f.errors.length,1);
    assert.match(f.errors[0].message,reason==='max_output_tokens'?/abgeschnitten/:/nicht vollständig/);
    assert.equal(f.sent.length,0);assert.equal(f.stopped(),1);
  }

  // A normal user interruption is not a fatal provider failure. Neither its
  // cancellation nor any obsolete terminal response may stop the new turn.
  f=fixture();await f.voice.start();
  await f.voice.event({type:'response.created',response:{id:'synthetic-old'}});
  await f.voice.event({type:'output_audio_buffer.started',response_id:'synthetic-old'});
  await f.voice.event({type:'input_audio_buffer.speech_started'});
  await f.voice.event({type:'response.done',response:{id:'synthetic-old',status:'cancelled',status_details:{reason:'turn_detected'}}});
  assert.equal(f.voice.active,true);assert.equal(f.audio.muted,true);assert.equal(f.states.at(-1)[0],'listening');
  await f.voice.event({type:'input_audio_buffer.speech_stopped'});
  await f.voice.event({type:'response.created',response:{id:'synthetic-new'}});
  await f.voice.event({type:'output_audio_buffer.started',response_id:'synthetic-new'});
  for(const status of ['cancelled','failed','incomplete','completed']){
    await f.voice.event({type:'response.done',response:{id:'synthetic-old',status}});
    assert.equal(f.voice.active,true);assert.equal(f.audio.muted,false);assert.equal(f.states.at(-1)[0],'speaking');
  }
  await f.voice.event({type:'response.done',response:{id:'synthetic-new',status:'completed',output:[{type:'message'}]}});
  assert.equal(f.states.at(-1)[0],'speaking','generation done is not the end of queued audio playback');
  await f.voice.event({type:'output_audio_buffer.stopped',response_id:'synthetic-new'});
  assert.equal(f.states.at(-1)[0],'listening');assert.equal(f.errors.length,0);f.voice.stop();

  // Successful tool responses still produce exactly one normal continuation;
  // response.done must not close the channel while the local tool is pending.
  f=fixture();await f.voice.start();const toolResponse=deferred();f.behaviour.api=()=>toolResponse.promise;
  await f.voice.event({type:'response.created',response:{id:'synthetic-tool'}});
  const pendingTool=f.voice.event({...toolEvent,response_id:'synthetic-tool'});
  await f.voice.event({type:'response.done',response:{id:'synthetic-tool',status:'completed',output:[{type:'function_call'}]}});
  assert.equal(f.voice.active,true);assert.equal(f.sent.length,0);
  toolResponse.resolve({result:{id:156}});await pendingTool;
  assert.deepEqual(f.sent.map(row=>row.event.type),['conversation.item.create','response.create']);
  assert.equal(f.errors.length,0);f.voice.stop();

  // A never-resolving permission prompt has its own deadline and no server call.
  f=fixture();let mic=deferred();f.behaviour.microphone=()=>mic.promise;
  let pending=f.voice.start();assert.equal(f.voice.phase,'microphone');f.fire(60000);await pending;
  assert.equal(f.calls.length,0);assert.equal(f.errors[0].phase,'microphone');assert.match(f.errors[0].message,/nicht bereitgestellt/);
  mic.resolve(f.newStream());await flush();assert.equal(f.stopped(),1);assert.equal(f.pcs.length,0);
  // Explicit stop also completes the start promise without waiting for permission.
  f=fixture();mic=deferred();f.behaviour.microphone=()=>mic.promise;pending=f.voice.start();f.voice.stop();await pending;
  assert.equal(f.errors.length,0);mic.resolve(f.newStream());await flush();assert.equal(f.stopped(),1);
  // Permission may resolve just before Stop, while start still awaits its race.
  f=fixture();const justGranted=f.newStream();f.behaviour.microphone=()=>Promise.resolve(justGranted);
  pending=f.voice.start();await Promise.resolve();f.voice.stop();await pending;
  assert.equal(f.stopped(),1,'the newly granted microphone must already belong to the session');
  assert.equal(f.pcs.length,0);assert.equal(f.calls.length,0);assert.equal(f.errors.length,0);
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
  f=fixture();f.behaviour.fetch=async()=>({ok:true,text:async()=> 'invalid'});await f.voice.start();assert.match(f.errors[0].message,/gültige Verbindungsantwort/);

  // Every old PC/channel/microphone/timer callback stays harmless after restart.
  f=fixture();await f.voice.start();
  const oldPc=f.voice.pc,oldChannel=f.voice.channel;
  const callbacks={track:oldPc.ontrack,state:oldPc.onconnectionstatechange,open:oldChannel.onopen,message:oldChannel.onmessage,close:oldChannel.onclose,error:oldChannel.onerror,ended:f.streams[0].handlers.get('ended')};
  const oldTimers=[...f.timers.values()].map(timer=>timer.fn);
  f.voice.stop();await f.voice.start();const freshPc=f.voice.pc,count=f.timers.size;
  callbacks.track({streams:[f.newStream()]});callbacks.state();callbacks.open();callbacks.close();callbacks.error();callbacks.ended();
  callbacks.message({data:JSON.stringify({type:'input_audio_buffer.speech_started'})});oldTimers.forEach(callback=>callback());
  callbacks.message({data:JSON.stringify({type:'response.done',response:{id:'obsolete-session',status:'failed'}})});
  assert.equal(f.voice.active,true);assert.equal(f.voice.pc,freshPc);assert.equal(f.audio.muted,false);assert.equal(f.audio.srcObject,null);
  assert.equal(f.timers.size,count);assert.equal(f.errors.length,0);assert.equal(f.texts.length,0);f.voice.stop();

  // Brief network loss recovers, without unmuting a user interruption.
  f=fixture();await f.voice.start();const recoveringPc=f.voice.pc;
  await f.voice.event({type:'output_audio_buffer.started'});
  await f.voice.event({type:'input_audio_buffer.speech_started'});
  recoveringPc.connectionState='disconnected';recoveringPc.onconnectionstatechange();
  const recoveryTimer=[...f.timers.entries()].find(([,timer])=>timer.ms===5000);
  assert.ok(recoveryTimer);assert.equal(f.voice.active,true);assert.equal(f.stopped(),0);
  recoveringPc.onconnectionstatechange();
  assert.equal([...f.timers.values()].filter(timer=>timer.ms===5000).length,1);
  assert.ok(f.timers.has(recoveryTimer[0]),'duplicate disconnected events cannot postpone the deadline');
  recoveringPc.connectionState='connected';recoveringPc.onconnectionstatechange();
  assert.equal(f.voice.active,true);assert.equal(f.audio.muted,true);assert.equal(f.states.at(-1)[0],'listening');
  assert.equal(f.timers.has(recoveryTimer[0]),false);assert.equal(f.errors.length,0);f.voice.stop();
  // A persistent loss still ends the session and releases all resources.
  f=fixture();await f.voice.start();f.voice.pc.connectionState='disconnected';f.voice.pc.onconnectionstatechange();
  f.fire(5000);assert.equal(f.voice.active,false);assert.equal(f.stopped(),1);assert.equal(f.timers.size,0);
  assert.match(f.errors[0].message,/nicht wiederhergestellt/);
  // Stopping during recovery must not let its old timeout affect the next call.
  f=fixture();await f.voice.start();f.voice.pc.connectionState='disconnected';f.voice.pc.onconnectionstatechange();
  const staleRecovery=[...f.timers.values()].find(timer=>timer.ms===5000).fn;
  f.voice.stop();await f.voice.start();staleRecovery();assert.equal(f.voice.active,true);assert.equal(f.errors.length,0);
  f.voice.pc.connectionState='failed';f.voice.pc.onconnectionstatechange();assert.equal(f.voice.active,false);assert.equal(f.timers.size,0);

  // A rejected old play() promise cannot stop a fresh session.
  f=fixture();await f.voice.start();const play=deferred();f.behaviour.play=()=>play.promise;
  f.voice.pc.ontrack({streams:[f.newStream()]});f.voice.stop();delete f.behaviour.play;await f.voice.start();
  play.reject(Object.assign(new Error('old pause'),{name:'AbortError'}));await flush();
  assert.equal(f.voice.active,true);assert.equal(f.errors.length,0);f.voice.stop();

  // Playback attempts within one session have their own ownership, too.
  for(const name of ['NotAllowedError','NotSupportedError']){
    f=fixture();await f.voice.start();const olderPlay=deferred();f.behaviour.play=()=>olderPlay.promise;
    f.voice.pc.ontrack({streams:[f.newStream()]});delete f.behaviour.play;
    assert.equal(await f.voice.resumePlayback(),true);
    olderPlay.reject(Object.assign(new Error('obsolete play attempt'),{name}));await flush();
    assert.equal(f.voice.active,true);assert.equal(f.voice.playbackBlocked,false);assert.equal(f.blocked.length,0);assert.equal(f.errors.length,0);f.voice.stop();
  }
  f=fixture();await f.voice.start();const obsoleteSuccess=deferred();f.behaviour.play=()=>obsoleteSuccess.promise;
  f.voice.pc.ontrack({streams:[f.newStream()]});
  f.behaviour.play=()=>Promise.reject(Object.assign(new Error('current attempt denied'),{name:'NotAllowedError'}));
  assert.equal(await f.voice.resumePlayback(),false);obsoleteSuccess.resolve();await flush();
  assert.equal(f.voice.playbackBlocked,true,'an obsolete success must not hide the current playback block');f.voice.stop();

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
  console.log('PASS: direct ephemeral WebRTC transport, bounded token/SDP exchange, safe errors, stale/cancel cleanup; startup phases, permissions, playback, refresh/tools, barge-in and response failures.');
})().catch(error=>{console.error(error);process.exitCode=1;});
