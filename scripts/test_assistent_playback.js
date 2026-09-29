// Offline lifecycle regression using the real page script, without microphone or network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '..', 'static', 'assistent.js'), 'utf8');
const flush = () => new Promise(setImmediate);
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}
class Element {
  constructor() {
    this.dataset = {}; this.elements = {text: {value: '',focus(){this.focused=true;}}, id: {value: ''}};
    this.listeners = new Map(); this.children = []; this.textContent = '';
  }
  addEventListener(name, handler) {
    if (!this.listeners.has(name)) this.listeners.set(name, new Set());
    this.listeners.get(name).add(handler);
  }
  removeEventListener(name, handler) { this.listeners.get(name)?.delete(handler); }
  emit(name, event) { return Promise.all([...this.listeners.get(name) || []].map(handler=>handler(event))); }
  append(child) { this.children.push(child); child.parentNode=this; }
  remove() { if(this.parentNode)this.parentNode.children=this.parentNode.children.filter(child=>child!==this);this.parentNode=null; }
  prepend(child) { this.children.unshift(child); }
  replaceChildren(...children) { this.children = children; }
  querySelectorAll() { return []; }
  querySelector(tag) { return this.children.find(child=>child.tagName===tag) || new Element(); }
  closest() { return this; }
  reset() {}
  focus() { this.focused = true; }
  scrollIntoView() {}
  showModal() { this.open = true; }
}
async function fixture(options={}) {
  const elements = new Map(), document = new Element(), requests = [], dialogs = [], transcriptions = [], timers = new Map(), revoked = [], meterCalls = [], apiRequests = [], mediaCalls = [];
  const responses = options.responses || new Map();
  const get = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
  get('assistant').dataset = {readOnly: 'true', ready: 'true', statusEnabled:String(!!options.statusEnabled), purchaseEnabled:String(!!options.purchaseEnabled)};
  get('read-aloud').checked = true;
  const audio = get('speech');
  Object.assign(audio, {srcObject: null, src: '', ended: false, paused: true, plays: 0, pauses: 0});
  audio.play = () => { audio.plays++; return audio.nextPlay || Promise.resolve(); };
  audio.pause = () => { audio.pauses++; audio.paused = true; audio.emit('pause'); };
  audio.removeAttribute = name => { if (name === 'src') audio.src = ''; };
  audio.load = () => {};
  document.hidden = false;
  document.head = new Element();
  document.getElementById = get;
  document.createElement = tag => Object.assign(new Element(), {tagName:tag});
  document.querySelector = () => ({content: 'synthetic-csrf'});
  let timerId = 0, urlId = 0;
  class Voice {
    constructor(options) { Object.assign(this, options); this.active = false; this.generation = 0; }
    stop() { this.active = false; this.generation++; this.onState('idle', 'Beendet.'); }
    async start() { this.active = true; this.generation++; this.onState('listening', 'Ich höre zu.'); }
  }
  class Realtime extends Voice {
    async start() { await super.start(); this.audio.srcObject = {syntheticRealtimeStream: true}; }
    stop() { super.stop(); this.audio.srcObject = null; }
    async resumePlayback() { meterCalls.push('resumePlayback'); return true; }
  }
  class Meter {
    async unlock() { meterCalls.push('unlock'); return true; }
    useBlob(blob, url) { meterCalls.push({blob, url}); }
    useStream(stream) { meterCalls.push({stream}); }
    clear() { meterCalls.push('clear'); }
    suspend() { meterCalls.push('suspend'); }
  }
  class Recorder {
    static isTypeSupported() { return true; }
    constructor() { this.state = 'inactive'; this.mimeType = 'audio/webm'; }
    start() { this.state = 'recording'; }
    stop() { this.state = 'inactive'; this.ondataavailable?.({data: new Blob(['synthetic recording'])}); this.onstop?.(); }
  }
  const context = {
    document, AbortController, FormData, Blob, MediaRecorder: Recorder,
    navigator: {mediaDevices: {getUserMedia: (...args) => {mediaCalls.push(args);return (options.getUserMedia || (async () => ({getTracks: () => [{stop() {}}]})))(...args);}}},
    window: {isSecureContext: true, crypto:require('node:crypto').webcrypto, MediaRecorder: Recorder, AssistantVoiceMode: Voice, AssistantRealtime: Realtime, OutputAudioMeter: Meter, addEventListener() {}},
    URL: {createObjectURL: () => `blob:synthetic-${++urlId}`, revokeObjectURL: url => revoked.push(url)},
    setTimeout: (fn,ms) => { timers.set(++timerId, {fn,ms}); return timerId; },
    clearTimeout: id => timers.delete(id),
    fetch: async (url, requestOptions) => {
      apiRequests.push({url,options:requestOptions});
      const route=url.replace('/werkstatt/assistent','');
      if(responses.has(route))return {ok:true,json:async()=>{
        const value=responses.get(route);return typeof value==='function'?value(requestOptions):value;
      }};
      if (url.endsWith('/sprechen')) {
        const body = deferred();
        // Intentionally ignore abort to cover already-delivered responses too.
        requests.push({...body, signal: requestOptions.signal});
        return {ok: true, blob: () => body.promise};
      }
      if (url.endsWith('/dialog') || url.endsWith('/audio')) {
        const body = deferred();
        (url.endsWith('/dialog') ? dialogs : transcriptions).push(body);
        return {ok: true, json: () => body.promise};
      }
      return {ok: true, json: async () => url.endsWith('/aktionen') ? [] : {modus: 'unavailable'}};
    }
  };
  Object.assign(context.window,options.audioConstructors||{});
  vm.createContext(context);
  // Expose closures only in this in-memory test instance; production has no test API.
  vm.runInContext(source.replace(/\}\)\(\);\s*$/, 'window.playbackTest={say,stopVoice,safe,realtime,prepareReadback,refresh,showOrder,chat,getPending:()=>pending,getCurrent:()=>current,getRealtime:()=>realtime};\n})();'), context);
  await flush();
  return {test: context.window.playbackTest, get, audio, requests, dialogs, transcriptions, timers, revoked, document, meterCalls, apiRequests, responses, mediaCalls, window:context.window, constructors:{Voice,Realtime,Meter},
    state: () => get('avatar').dataset.state,
    deliver: async index => { requests[index].resolve(new Blob(['synthetic audio'])); await flush(); }};
}

(async () => {
  // A failed optional asset must not abort initialization of text/menu/workflows.
  let f=await fixture({audioConstructors:{AssistantRealtime:undefined,AssistantVoiceMode:undefined,OutputAudioMeter:undefined}});
  assert.equal(f.get('audio-components-help').hidden,false);
  assert.match(f.get('audio-components-message').textContent,/Texteingabe und Menü bleiben verfügbar/);
  assert.equal(f.get('voice-mode').disabled,true);
  assert.equal(f.get('record').disabled,false,'standalone recording is not dependent on voice scripts');
  assert.equal(typeof f.window.AssistantWorkflowHost.api,'function');
  assert.equal(f.document.head.children.length,0,'no automatic reload loop');
  f.get('write-message').onclick();
  assert.equal(f.get('assistant-menu').open,true);assert.equal(f.get('chat').elements.text.focused,true);
  f.get('read-aloud').checked=false;f.get('chat').elements.text.value='Was steht im Auftrag?';
  let written=f.get('chat').onsubmit({preventDefault(){}});await flush();
  assert.equal(f.dialogs.length,1);f.dialogs[0].resolve({text:'Die Beschreibung ist verfügbar.',events:[]});await written;
  assert.ok(f.get('conversation').children.some(row=>row.textContent==='KI: Die Beschreibung ist verfügbar.'));
  assert.equal(f.mediaCalls.length,0);assert.equal(f.get('chat').elements.text.value,'');

  // Reload only the missing fixed local asset, once, preserving unsaved forms.
  f=await fixture({audioConstructors:{AssistantRealtime:undefined}});
  f.get('chat').elements.text.value='Ungesendeter Text';f.get('order-form').elements.id.value='123';
  const bootstrapRetry=f.get('audio-components-retry').onclick({preventDefault(){}});
  assert.equal(f.document.head.children.length,1);
  const script=f.document.head.children[0];assert.match(script.src,/^\/static\/assistent-realtime\.js\?audio-retry=\d+$/);
  await f.get('audio-components-retry').onclick({preventDefault(){}});
  assert.equal(f.document.head.children.length,1,'double click cannot load a second script');
  f.window.AssistantRealtime=f.constructors.Realtime;script.onload();await bootstrapRetry;
  assert.equal(f.get('audio-components-help').hidden,true);assert.equal(f.get('voice-mode').disabled,false);
  assert.equal(f.get('chat').elements.text.value,'Ungesendeter Text');assert.equal(f.get('order-form').elements.id.value,'123');
  assert.equal(f.test.getRealtime().active,false);assert.equal(f.mediaCalls.length,0);
  assert.ok(!f.apiRequests.some(row=>row.options.method==='POST'),'asset recovery cannot confirm an action or start a session');
  await f.get('voice-mode').onclick({preventDefault(){}});assert.equal(f.test.getRealtime().active,true);f.test.stopVoice();

  // A failed or late reload stays bounded and leaves the working controller alone.
  f=await fixture({audioConstructors:{AssistantVoiceMode:undefined}});
  const originalRealtime=f.test.getRealtime();
  const failedRetry=f.get('audio-components-retry').onclick({preventDefault(){}});
  const delayed=f.document.head.children[0],lateLoad=delayed.onload;
  [...f.timers.values()].find(timer=>timer.ms===15000).fn();await failedRetry;
  assert.equal(f.document.head.children.length,0);assert.equal(f.get('audio-components-retry').disabled,true);
  assert.match(f.get('audio-components-message').textContent,/Nachladeversuch ist beendet/);
  f.window.AssistantVoiceMode=f.constructors.Voice;lateLoad();await flush();
  assert.equal(f.test.getRealtime(),originalRealtime);assert.equal(f.mediaCalls.length,0);
  assert.equal(f.get('voice-mode').disabled,false,'working realtime stays available without spoken confirmation');
  await f.get('audio-components-retry').onclick({preventDefault(){}});assert.equal(f.document.head.children.length,0);

  // Constructor errors and malformed exports are isolated too, without re-executing
  // already evaluated scripts (VoiceMode has a top-level lexical class declaration).
  const Broken=class{constructor(){throw new Error('synthetic constructor failure');}};
  f=await fixture({audioConstructors:{AssistantRealtime:Broken,AssistantVoiceMode:{invalid:true},OutputAudioMeter:Broken}});
  f.get('write-message').onclick();assert.equal(f.get('assistant-menu').open,true);
  const brokenRetry=f.get('audio-components-retry').onclick({preventDefault(){}});
  assert.equal(f.document.head.children.length,1);assert.match(f.document.head.children[0].src,/assistent-voice\.js/);
  f.document.head.children[0].onerror();await brokenRetry;
  assert.equal(f.get('audio-components-retry').disabled,true);assert.equal(f.mediaCalls.length,0);

  // The optional meter/animation may fail during use without blocking real audio.
  const FailingMeter=class{unlock(){throw new Error('meter failed');}clear(){}useBlob(){}useStream(){}suspend(){}};
  f=await fixture({audioConstructors:{OutputAudioMeter:FailingMeter}});
  f.window.AssistantAvatar={instance:{animation:{setAudioLevel(){throw new Error('animation failed');}}}};
  f.get('write-message').onclick();f.get('read-aloud').checked=false;f.get('chat').elements.text.value='Weiter';
  written=f.get('chat').onsubmit({preventDefault(){}});await flush();
  f.dialogs[0].resolve({text:'Weiter geht es.',events:[]});await written;
  assert.equal(f.state(),'idle');

  // Recovery cannot remove the stop control while another audio feature is busy.
  f=await fixture({audioConstructors:{AssistantRealtime:undefined}});
  const recoveryReadback=f.test.say('Bitte vollständig prüfen.');
  const duringAudioRetry=f.get('audio-components-retry').onclick({preventDefault(){}});
  f.window.AssistantRealtime=f.constructors.Realtime;f.document.head.children[0].onload();await duringAudioRetry;
  assert.equal(f.get('voice-stop').hidden,false);assert.equal(f.get('record').disabled,true);
  f.test.stopVoice();assert.equal(await recoveryReadback,false);await f.deliver(0);
  assert.equal(f.audio.plays,0,'stopped TTS cannot return after dependency recovery');

  // Stop must settle immediately, abort fetch, and ignore its eventual body.
  f = await fixture();
  let result = f.test.say('Erste Antwort');
  assert.equal(f.state(), 'thinking');
  assert.equal(f.get('voice-stop').hidden, false, 'stop is available while TTS loads');
  f.test.stopVoice();
  assert.equal(await result, false);
  assert.equal(f.requests[0].signal.aborted, true);
  await f.deliver(0);
  assert.equal(f.audio.plays, 0);
  assert.equal(f.state(), 'idle');

  // Speaking follows playback, including pause/buffering, completion and replay.
  f = await fixture(); result = f.test.say('Antwort'); await f.deliver(0);
  assert.equal(f.state(), 'thinking', 'resolved play promise alone is not audible output');
  f.audio.emit('playing'); assert.equal(f.state(), 'speaking');
  f.audio.pause(); assert.equal(f.state(), 'idle');
  f.audio.emit('waiting'); assert.equal(f.state(), 'thinking');
  f.audio.emit('playing'); assert.equal(f.state(), 'speaking');
  f.audio.ended = true; f.audio.emit('ended');
  assert.equal(await result, true); assert.equal(f.state(), 'idle'); assert.equal(f.timers.size, 0);
  f.audio.ended = false; f.audio.emit('playing'); assert.equal(f.state(), 'speaking');
  f.audio.emit('ended'); assert.equal(f.state(), 'idle');
  f.test.stopVoice(); assert.equal(f.revoked.length, 1);
  assert.equal([...f.audio.listeners.values()].reduce((sum, set) => sum + set.size, 0), 0);

  // Autoplay recovery also works for typed answers from the avatar's main screen.
  f = await fixture();
  const denied = deferred(); f.audio.nextPlay = denied.promise;
  result = f.test.say('Antwort');
  await f.deliver(0); denied.reject(Object.assign(new Error('blocked'),{name:'NotAllowedError'})); await flush();
  assert.equal(f.state(), 'idle'); assert.equal(f.timers.size, 0);
  assert.equal(f.get('audio-resume').hidden,false);
  assert.match(f.get('avatar-status').textContent,/Ton einschalten/);
  f.audio.nextPlay=null;await f.get('audio-resume').onclick({preventDefault(){}});
  assert.equal(f.audio.plays,2);assert.equal(f.get('audio-resume').hidden,true);
  assert.ok(!f.meterCalls.includes('resumePlayback'),'TTS resumes its own audio, not WebRTC');
  f.audio.emit('playing');assert.equal(f.state(),'speaking');
  f.audio.emit('ended');assert.equal(await result,true);f.test.stopVoice();

  // Decoder errors still stop the speaking state truthfully.
  f = await fixture(); result = f.test.say('Antwort');
  const broken = assert.rejects(result, /abgespielt/);
  await f.deliver(0); f.audio.emit('playing'); f.audio.emit('error'); await broken;
  assert.equal(f.state(), 'error'); f.test.stopVoice();

  // Losing focus must not replace the actual failure with an innocuous stop label.
  f = await fixture(); await f.test.safe(async () => { throw new Error('Mikrofon ist blockiert.'); })();
  f.document.hidden = true; f.document.emit('visibilitychange');
  assert.equal(f.state(), 'error'); assert.equal(f.get('avatar-status').textContent, 'Mikrofon ist blockiert.');
  assert.ok(f.meterCalls.includes('suspend'));

  // Output observation receives exactly the owned blob; stopping clears it.
  f = await fixture(); result = f.test.say('Antwort'); await f.deliver(0);
  assert.equal(f.meterCalls.filter(value => value?.blob).length, 1);
  assert.equal(f.meterCalls.find(value => value?.blob).url, f.audio.src);
  f.test.stopVoice(); await result; assert.equal(f.meterCalls.at(-1), 'clear');
  const remote = {testRemote: true}; f.test.realtime.onRemoteStream(remote);
  assert.equal(f.meterCalls.at(-1).stream, remote);
  f.test.realtime.onRemoteStream(null); assert.equal(f.meterCalls.at(-1), 'clear');

  // Autoplay recovery is one deliberate tap, not a new microphone/session request.
  f.test.realtime.active = true; f.test.realtime.onPlaybackBlocked();
  assert.equal(f.get('audio-resume').hidden, false);
  await f.get('audio-resume').onclick({preventDefault() {}});
  assert.equal(f.get('audio-resume').hidden, true);
  assert.equal(f.get('audio-resume').disabled, false);
  assert.ok(f.meterCalls.includes('unlock')); assert.ok(f.meterCalls.includes('resumePlayback'));

  // A newer answer owns the audio; old fetch failures must not report an error.
  f = await fixture();
  const old = f.test.say('Alt'); const newer = f.test.say('Neu');
  assert.equal(await old, false); assert.equal(f.requests[0].signal.aborted, true);
  await f.deliver(1); f.audio.emit('playing');
  f.requests[0].reject(new Error('late old network error')); await flush();
  assert.equal(f.state(), 'speaking'); assert.equal(f.audio.plays, 1);
  f.audio.emit('ended'); assert.equal(await newer, true); f.test.stopVoice();

  // Queued media callbacks and a late play rejection cannot change a new run.
  f = await fixture(); const latePlay = deferred(); f.audio.nextPlay = latePlay.promise;
  const playingOld = f.test.say('Alt'); await f.deliver(0);
  const oldHandlers = [...f.audio.listeners.values()].flatMap(set => [...set]);
  f.test.stopVoice(); assert.equal(await playingOld, false); f.audio.nextPlay = null;
  const playingNew = f.test.say('Neu'); await f.deliver(1); f.audio.emit('playing');
  for (const callback of oldHandlers) callback();
  latePlay.reject(new Error('late old play rejection')); await flush();
  assert.equal(f.state(), 'speaking');
  f.audio.emit('ended'); assert.equal(await playingNew, true); f.test.stopVoice();

  // Even an error already rejected before restart cannot overwrite its new UI.
  f = await fixture(); result = f.test.say('Alt');
  const captured = result.catch(error => error);
  f.requests[0].reject(new Error('old failure')); const oldError = await captured;
  const latest = f.test.say('Neu');
  await f.test.safe(async () => { throw oldError; })();
  assert.equal(f.state(), 'thinking'); assert.equal(f.get('status').textContent, '');
  f.test.stopVoice(); assert.equal(await latest, false);

  // Starting WebRTC detaches classic listeners; its events keep owning state.
  f = await fixture(); result = f.test.say('Antwort'); await f.deliver(0);
  const queuedEnded = [...f.audio.listeners.get('ended')][0];
  await f.get('voice-mode').onclick({preventDefault() {}});
  assert.equal(await result, false); assert.equal(f.state(), 'listening');
  queuedEnded(); f.audio.emit('playing'); f.audio.emit('pause'); f.audio.emit('waiting');
  assert.equal(f.state(), 'listening'); assert.ok(f.audio.srcObject);
  f.test.stopVoice();

  // Leaving the page follows the same cancellation path without late playback.
  f = await fixture(); result = f.test.say('Antwort');
  f.document.hidden = true; f.document.emit('visibilitychange');
  assert.equal(await result, false); await f.deliver(0);
  assert.equal(f.audio.plays, 0); assert.equal(f.state(), 'idle');

  // Stop before /dialog returns: no old answer, TTS or error after immediate restart.
  for (const fail of [false, true]) {
    f = await fixture(); f.get('chat').elements.text.value = 'Alte Frage';
    const dialog = f.get('chat').onsubmit({preventDefault() {}});
    assert.equal(f.dialogs.length, 1);
    f.test.stopVoice();
    await f.get('voice-mode').onclick({preventDefault() {}});
    assert.equal(f.state(), 'listening', 'stop releases the old request for restart immediately');
    if (fail) f.dialogs[0].reject(new Error('obsolete dialog failure'));
    else f.dialogs[0].resolve({text: 'Veraltete Antwort', events: []});
    await dialog; await flush();
    assert.equal(f.requests.length, 0); assert.equal(f.audio.plays, 0);
    assert.equal(f.state(), 'listening'); assert.equal(f.get('status').textContent, '');
    assert.equal(f.get('conversation').children.length, 1, 'only the original user question remains');
    f.test.stopVoice();
  }

  // A submitted single recording cannot start chat after stop or page exit.
  for (const leavePage of [false, true]) {
    f = await fixture();
    await f.get('record').onclick({preventDefault() {}});
    await f.get('record').onclick({preventDefault() {}});
    assert.equal(f.transcriptions.length, 1);
    if (leavePage) { f.document.hidden = true; f.document.emit('visibilitychange'); }
    else f.test.stopVoice();
    f.transcriptions[0].resolve({text: 'Verspätete Transkription'}); await flush();
    assert.equal(f.dialogs.length, 0); assert.equal(f.requests.length, 0);
    assert.equal(f.get('chat').elements.text.value, ''); assert.equal(f.state(), 'idle');
  }
  // Cancelling an unanswered permission prompt settles immediately; late tracks close.
  for(const leavePage of [false,true]){
    const request=deferred();let stops=0;
    f=await fixture({getUserMedia:()=>request.promise});
    const opening=f.get('record').onclick({preventDefault(){}});
    assert.match(f.get('record').textContent,/abbrechen/);
    if(leavePage){f.document.hidden=true;f.document.emit('visibilitychange');}
    else await f.get('record').onclick({preventDefault(){}});
    await opening;
    request.resolve({getTracks:()=>[{stop(){stops++;}}]});await flush();
    assert.equal(stops,1);assert.equal(f.transcriptions.length,0);assert.equal(f.timers.size,0);
    assert.equal(f.state(),'idle');
  }
  // Stop in the microtask between stream acquisition and recorder construction releases ownership.
  const acquisition=deferred();let acquiredStops=0;
  f=await fixture({getUserMedia:()=>acquisition.promise});
  const pendingRecording=f.get('record').onclick({preventDefault(){}});
  acquisition.resolve({getTracks:()=>[{stop(){acquiredStops++;}}]});
  await Promise.resolve();f.test.stopVoice();await pendingRecording;
  assert.ok(acquiredStops>=1);assert.equal(f.transcriptions.length,0);

  // An unanswered permission prompt times out with visible help and still releases late tracks.
  const unanswered=deferred();let timeoutStops=0;
  f=await fixture({getUserMedia:()=>unanswered.promise});
  const timeoutRecording=f.get('record').onclick({preventDefault(){}});
  const [helpId,help]=[...f.timers].find(([,timer])=>timer.ms===8000);
  f.timers.delete(helpId);help.fn();assert.equal(f.get('voice-help').open,true);
  const [deadlineId,deadline]=[...f.timers].find(([,timer])=>timer.ms===60000);
  f.timers.delete(deadlineId);deadline.fn();await timeoutRecording;
  assert.equal(f.state(),'error');assert.equal(f.timers.size,0);
  unanswered.resolve({getTracks:()=>[{stop(){timeoutStops++;}}]});await flush();
  assert.equal(timeoutStops,1);assert.equal(f.transcriptions.length,0);

  // A cancelled old opening cannot cancel an immediate new permission request.
  const firstMic=deferred(),secondMic=deferred();let micCalls=0,oldMicStops=0,newMicStops=0;
  f=await fixture({getUserMedia:()=>++micCalls===1?firstMic.promise:secondMic.promise});
  const openingFirst=f.get('record').onclick({preventDefault(){}});
  f.test.stopVoice();
  const openingSecond=f.get('record').onclick({preventDefault(){}});
  await openingFirst;
  assert.match(f.get('record').textContent,/abbrechen/);
  secondMic.resolve({getTracks:()=>[{stop(){newMicStops++;}}]});await openingSecond;
  assert.equal(f.state(),'listening');assert.equal(newMicStops,0);
  firstMic.resolve({getTracks:()=>[{stop(){oldMicStops++;}}]});await flush();
  assert.equal(oldMicStops,1);assert.equal(newMicStops,0);f.test.stopVoice();

  // Speech transcription must not overwrite a draft typed while it was processing.
  f=await fixture();f.get('read-aloud').checked=false;
  await f.get('record').onclick({preventDefault(){}});
  await f.get('record').onclick({preventDefault(){}});
  f.get('chat').elements.text.value='Neuer geschriebener Entwurf';
  f.transcriptions[0].resolve({text:'Erkannte Sprachnachricht'});await flush();
  assert.equal(f.get('chat').elements.text.value,'Neuer geschriebener Entwurf');
  f.dialogs[0].resolve({text:'Antwort',events:[]});await flush();

  // Sending an earlier question never erases the next draft typed during its answer.
  f=await fixture();f.get('read-aloud').checked=false;
  f.get('chat').elements.text.value='Erste Frage';
  const submission=f.get('chat').onsubmit({preventDefault(){}});
  assert.equal(f.get('chat').elements.text.value,'');
  f.get('chat').elements.text.value='Meine nächste Frage';
  f.dialogs[0].resolve({text:'Antwort',events:[]});await submission;
  assert.equal(f.get('chat').elements.text.value,'Meine nächste Frage');
  const statusProposal={id:'status-1',auftrag_id:156,art:'status',status:'vorschlag',daten:{text:'Auftrag 156: Lackierbereit melden.',fortschritt:{}}};
  const orderProposal={id:'purchase-1',auftrag_id:156,art:'bestellung',status:'vorschlag',daten:{lieferant:'Testlieferant',teilenummer:'BAND-50',bezeichnung:'Grünes Klebeband',menge:2,stueckpreis_brutto_cent:1000,versand_brutto_cent:500,nebenkosten_brutto_cent:0,gesamt_cent:2500,versand:{recipient:'test@example.invalid',variant:'grün, 50 mm',unit:'Rollen',urgent:false,max_total_cents:25000,price_basis:'gross',price_source:'Bestätigtes Testangebot'}}};
  const queued={...orderProposal,id:'purchase-queued',status:'bestellung_eingeplant',versandstatus:{message:'Für Montag eingeplant. Noch nicht versendet.'}};
  const legacy={id:'legacy',auftrag_id:156,art:'einkauf',status:'vorschlag',daten:orderProposal.daten};
  const routes=new Map([['/aktionen',[statusProposal,orderProposal,queued,legacy]]]);
  f=await fixture({statusEnabled:true,purchaseEnabled:true,responses:routes});
  let cards=f.get('actions').children;
  assert.equal(cards.length,3,'targeted capabilities do not re-enable legacy write forms/actions');
  assert.match(cards[0].children[0].textContent,/Statusänderung.*Zur Prüfung/);
  assert.match(cards[0].children[0].textContent,/Lackierbereit/);
  assert.doesNotMatch(cards[0].children[0].textContent,/NaN|undefined|Versand/);
  assert.equal(cards[0].children.find(x=>x.tagName==='button').textContent,'Status ändern');
  assert.match(cards[1].children[0].textContent,/50 mm/);
  assert.match(cards[1].children[0].textContent,/2 Rollen/);
  assert.match(cards[1].children[0].textContent,/test@example.invalid/);
  assert.match(cards[1].children[0].textContent,/Kostenrahmen: 250,00/);
  assert.match(cards[1].children[0].textContent,/Preise: brutto in EUR/);
  assert.match(cards[1].children[0].textContent,/Preisquelle: Bestätigtes Testangebot/);
  assert.match(cards[1].children[0].textContent,/Montag um 12:00/);
  assert.equal(cards[1].children.find(x=>x.tagName==='button').textContent,'Verbindlich bestellen');
  assert.ok(cards.every(card=>card.children.every(x=>x.tagName!=='a')),'new actions have no misleading email-draft link');
  assert.ok(cards[2].children.every(x=>x.tagName!=='button'),'confirmed orders cannot be resubmitted');
  assert.ok(cards[2].children.some(x=>x.textContent===queued.versandstatus.message));

  // A deliberate click confirms exactly the displayed action and refreshes its own order.
  f.test.showOrder({id:156,kennzeichen:'TEST-156',status:2});
  routes.set('/bestaetigen/status-1',()=>{routes.set('/aktionen',[{...statusProposal,status:'status_geaendert'},queued]);return {ok:true,status:'status_geaendert',hinweis:'Status ist jetzt lackierbereit.',auftrag:{id:156,kennzeichen:'TEST-156',status:3}};});
  await cards[0].children.find(x=>x.textContent==='Status ändern').onclick({preventDefault(){}});
  assert.equal(f.test.getCurrent().status,3);
  assert.equal(f.get('status').textContent,'Status ist jetzt lackierbereit.');
  const confirmation=f.apiRequests.find(x=>x.url.endsWith('/bestaetigen/status-1'));
  assert.equal(confirmation.options.method,'POST');assert.equal(confirmation.options.headers['X-CSRF-Token'],'synthetic-csrf');
  assert.ok(f.get('actions').children.every(card=>card.children.every(x=>x.tagName!=='button')));

  // Manual status changes prepare a proposal; they never commit at form submit.
  f.get('status-change').elements.aktion={value:'finish_starten'};
  routes.set('/vorschlag',statusProposal);
  const beforeConfirm=f.apiRequests.filter(x=>x.url.includes('/bestaetigen/')).length;
  await f.get('status-change').onsubmit({preventDefault(){}});
  assert.deepEqual(JSON.parse(f.apiRequests.find(x=>x.url.endsWith('/vorschlag')).options.body),{art:'status',auftrag_id:156,aktion:'finish_starten'});
  assert.equal(f.apiRequests.filter(x=>x.url.includes('/bestaetigen/')).length,beforeConfirm);

  const challenge={action_id:orderProposal.id,nonce:'one-use-test-nonce',phrase:'Bestellung für Auftrag 156 verbindlich bestätigen',text:'Bitte zwei Rollen grünes Klebeband, 50 mm, für 25 Euro brutto prüfen.'};
  routes.set('/vorlesen/purchase-1',challenge);
  let readback=f.test.prepareReadback(orderProposal);await flush();
  assert.equal(f.test.getPending(),null,'a pending TTS request is not an armed confirmation');
  f.test.stopVoice();assert.equal(await readback,false);
  assert.equal(f.test.getPending(),null,'interrupted readback never arms confirmation');
  readback=f.test.prepareReadback(orderProposal);await flush();
  await f.deliver(f.requests.length-1);f.audio.emit('playing');
  assert.equal(f.test.getPending(),null,'partial readback is not sufficient');
  f.audio.emit('ended');assert.equal(await readback,true);
  assert.equal(f.test.getPending().nonce,challenge.nonce);
  let spoken=f.test.chat('Ja');await flush();
  assert.ok(!f.apiRequests.some(x=>x.url.endsWith('/sprache-bestaetigen')),'a generic yes cannot order');
  f.test.stopVoice();await spoken;
  routes.set('/sprache-bestaetigen',()=>({ok:true,status:'bestellung_eingeplant',hinweis:'Bestellung bestätigt.',versandstatus:queued.versandstatus}));
  spoken=f.test.chat(challenge.phrase);await flush();
  assert.deepEqual(JSON.parse(f.apiRequests.find(x=>x.url.endsWith('/sprache-bestaetigen')).options.body),{nonce:challenge.nonce,text:challenge.phrase});
  assert.match(f.get('status').textContent,/Noch nicht versendet/);
  assert.equal(f.test.getPending(),null);
  f.test.stopVoice();await spoken;

  readback=f.test.prepareReadback(orderProposal);await flush();await f.deliver(f.requests.length-1);f.audio.emit('ended');await readback;
  routes.set('/auftrag/200',{id:200,kennzeichen:'TEST-200'});
  await f.document.emit('assistant-open-order',{detail:{id:200},preventDefault(){}});
  assert.equal(f.test.getPending(),null,'the menu order shortcut cancels the old spoken confirmation');
  assert.equal(f.test.getCurrent().id,200);

  f=await fixture({responses:new Map([['/aktionen',[statusProposal,orderProposal]]])});
  assert.equal(f.get('actions').children.length,0,'read-only without capabilities does not expose mutation proposals');
  await assert.rejects(f.test.prepareReadback(orderProposal),/Freigabe/);
  await f.get('status-change').onsubmit({preventDefault(){}});
  assert.ok(!f.apiRequests.some(x=>x.url.includes('/vorschlag')||x.url.includes('/bestaetigen')||x.url.includes('/vorlesen')),'hidden controls do not grant write capabilities');

  // Missing spoken confirmation never arms a nonce, but a deliberate reviewed
  // button still works using the existing protected confirmation endpoint.
  const fallbackRoutes=new Map([['/aktionen',[orderProposal]],['/bestaetigen/purchase-1',()=>{
    fallbackRoutes.set('/aktionen',[queued]);return {ok:true,hinweis:'Bestellung bestätigt.',versandstatus:queued.versandstatus};
  }]]);
  f=await fixture({purchaseEnabled:true,audioConstructors:{AssistantVoiceMode:undefined},responses:fallbackRoutes});
  const fallbackCard=f.get('actions').children[0];
  assert.equal(fallbackCard.children.find(button=>button.textContent==='Vorlesen & per Sprache bestätigen').disabled,true);
  assert.equal(await f.test.prepareReadback(orderProposal),false);assert.equal(f.test.getPending(),null);
  assert.ok(!f.apiRequests.some(row=>row.options.method==='POST'),'missing voice cannot approve or arm an action');
  await fallbackCard.children.find(button=>button.textContent==='Verbindlich bestellen').onclick({preventDefault(){}});
  assert.equal(f.apiRequests.filter(row=>row.url.endsWith('/bestaetigen/purchase-1')).length,1);
  assert.match(f.get('status').textContent,/Noch nicht versendet/);

  // Only SMTP-accepted orders can create a fresh unapproved proposal, never resend.
  const previousStates=['sent','copy_pending','uncertain','queued','blocked'];
  const previousOrders=previousStates.map(state=>({...queued,id:'previous-'+state,versandstatus:{state,message:state}}));
  const repeatRoutes=new Map([['/aktionen',previousOrders]]);
  f=await fixture({purchaseEnabled:true,responses:repeatRoutes});
  cards=f.get('actions').children;
  assert.equal(cards.filter(card=>card.children.some(x=>x.textContent==='Erneut vorbereiten')).length,2);
  assert.ok(cards.every(card=>card.children.every(x=>x.textContent!=='Verbindlich bestellen')));
  const retry=cards[0].children.find(x=>x.textContent==='Erneut vorbereiten');
  let repeatAttempts=0;
  repeatRoutes.set('/erneut-vorbereiten/previous-sent',()=>{
    if(++repeatAttempts===1)throw new Error('Synthetic lost response');
    repeatRoutes.set('/aktionen',[...previousOrders,{...orderProposal,id:'new-repeat'}]);
    return {...orderProposal,id:'new-repeat'};
  });
  await retry.onclick({preventDefault(){}});
  await retry.onclick({preventDefault(){}});
  const repeatedRequests=f.apiRequests.filter(x=>x.url.includes('/erneut-vorbereiten/'));
  assert.equal(repeatedRequests.length,2);
  assert.equal(JSON.parse(repeatedRequests[0].options.body).request_id,JSON.parse(repeatedRequests[1].options.body).request_id,'retry reuses its client key after a lost response');
  assert.match(JSON.parse(repeatedRequests[0].options.body).request_id,/^[a-zA-Z0-9-]{16,80}$/);
  assert.ok(!f.apiRequests.some(x=>x.url.includes('/bestaetigen/')||x.url.includes('/bestellen/')),'repeat preparation cannot send');
  assert.match(f.get('status').textContent,/Neue Bestellung vorbereitet, noch nicht ausgelöst/);
  assert.equal(f.get('actions').children.at(-1).children.find(x=>x.tagName==='button').textContent,'Verbindlich bestellen','new proposal still requires deliberate confirmation');

  console.log('PASS: optional audio bootstrap isolation, bounded deliberate recovery without microphone/action side effects; TTS lifecycle, generation overlap, WebRTC handoff, stopped requests; targeted capabilities and exact confirmation after full readback.');
})().catch(error => { console.error(error); process.exitCode = 1; });
