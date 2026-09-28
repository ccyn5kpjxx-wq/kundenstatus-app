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
    this.dataset = {}; this.elements = {text: {value: ''}, id: {value: ''}};
    this.listeners = new Map(); this.children = []; this.textContent = '';
  }
  addEventListener(name, handler) {
    if (!this.listeners.has(name)) this.listeners.set(name, new Set());
    this.listeners.get(name).add(handler);
  }
  removeEventListener(name, handler) { this.listeners.get(name)?.delete(handler); }
  emit(name) { for (const handler of this.listeners.get(name) || []) handler(); }
  append(child) { this.children.push(child); }
  replaceChildren(...children) { this.children = children; }
  querySelectorAll() { return []; }
  querySelector() { return new Element(); }
  closest() { return this; }
  reset() {}
}
async function fixture() {
  const elements = new Map(), document = new Element(), requests = [], dialogs = [], transcriptions = [], timers = new Map(), revoked = [], meterCalls = [];
  const get = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
  get('assistant').dataset = {readOnly: 'true', ready: 'true'};
  get('read-aloud').checked = true;
  const audio = get('speech');
  Object.assign(audio, {srcObject: null, src: '', ended: false, paused: true, plays: 0, pauses: 0});
  audio.play = () => { audio.plays++; return audio.nextPlay || Promise.resolve(); };
  audio.pause = () => { audio.pauses++; audio.paused = true; audio.emit('pause'); };
  audio.removeAttribute = name => { if (name === 'src') audio.src = ''; };
  audio.load = () => {};
  document.hidden = false;
  document.getElementById = get;
  document.createElement = () => new Element();
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
    navigator: {mediaDevices: {getUserMedia: async () => ({getTracks: () => [{stop() {}}]})}},
    window: {isSecureContext: true, MediaRecorder: Recorder, AssistantVoiceMode: Voice, AssistantRealtime: Realtime, OutputAudioMeter: Meter, addEventListener() {}},
    URL: {createObjectURL: () => `blob:synthetic-${++urlId}`, revokeObjectURL: url => revoked.push(url)},
    setTimeout: fn => { timers.set(++timerId, fn); return timerId; },
    clearTimeout: id => timers.delete(id),
    fetch: async (url, options) => {
      if (url.endsWith('/sprechen')) {
        const body = deferred();
        // Intentionally ignore abort to cover already-delivered responses too.
        requests.push({...body, signal: options.signal});
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
  vm.createContext(context);
  // Expose closures only in this in-memory test instance; production has no test API.
  vm.runInContext(source.replace(/\}\)\(\);\s*$/, 'window.playbackTest={say,stopVoice,safe,realtime};\n})();'), context);
  await flush();
  return {test: context.window.playbackTest, get, audio, requests, dialogs, transcriptions, timers, revoked, document, meterCalls,
    state: () => get('avatar').dataset.state,
    deliver: async index => { requests[index].resolve(new Blob(['synthetic audio'])); await flush(); }};
}

(async () => {
  // Stop must settle immediately, abort fetch, and ignore its eventual body.
  let f = await fixture();
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

  // Autoplay rejection and decoder errors stop the speaking state truthfully.
  f = await fixture();
  const denied = deferred(); f.audio.nextPlay = denied.promise;
  result = f.test.say('Antwort'); const rejected = assert.rejects(result, /blockiert/);
  await f.deliver(0); denied.reject(new Error('NotAllowedError')); await rejected;
  assert.equal(f.state(), 'error'); assert.equal(f.timers.size, 0);
  f.test.stopVoice();
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
  console.log('PASS: TTS lifecycle, generation overlap, WebRTC handoff, stopped dialog/error and stopped single-recording transcription.');
})().catch(error => { console.error(error); process.exitCode = 1; });
