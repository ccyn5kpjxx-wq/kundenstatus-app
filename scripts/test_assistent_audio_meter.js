// Output-only measurement regressions: synthetic audio, no network or hardware.
const assert = require('node:assert/strict');
const {OutputAudioMeter} = require('../static/assistent-audio-meter.js');

class Events {
  constructor() { this.listeners = new Map(); }
  addEventListener(name, listener) {
    if (!this.listeners.has(name)) this.listeners.set(name, new Set());
    this.listeners.get(name).add(listener);
  }
  removeEventListener(name, listener) { this.listeners.get(name)?.delete(listener); }
  emit(name) { for (const listener of [...this.listeners.get(name) || []]) listener(); }
  listenerCount() { return [...this.listeners.values()].reduce((sum, group) => sum + group.size, 0); }
}

const flush = () => new Promise(resolve => setImmediate(resolve));
function fixture() {
  const window = new Events();
  window.document = new Events(); window.document.hidden = false;
  const audio = new Events();
  Object.assign(audio, {paused: false, ended: false, muted: false, seeking: false, volume: 1, readyState: 4,
    currentTime: 0, srcObject: null, src: ''});
  audio.pause = audio.play = () => { throw new Error('Meter must never control playback'); };
  Object.defineProperty(window, 'navigator', {get() { throw new Error('No microphone access'); }});
  window.fetch = () => { throw new Error('No network access'); };
  let frameId = 0, now = 0, raw = 0, disconnected = 0;
  const frames = new Map(), levels = [], contexts = [], boundStreams = [];
  window.requestAnimationFrame = callback => { frames.set(++frameId, callback); return frameId; };
  window.cancelAnimationFrame = id => frames.delete(id);
  class Context extends Events {
    constructor() { super(); this.state = 'suspended'; this.jobs = []; contexts.push(this); }
    async resume() { this.state = 'running'; this.emit('statechange'); }
    async suspend() { this.state = 'suspended'; this.emit('statechange'); }
    async close() { this.state = 'closed'; this.emit('statechange'); }
    get destination() { throw new Error('Meter must not create another audible output'); }
    createMediaElementSource() { throw new Error('Meter must not reroute playback'); }
    createAnalyser() {
      return {fftSize: 0, getFloatTimeDomainData(samples) {
        for (let i = 0; i < samples.length; i++) samples[i] = i % 2 ? raw : -raw;
      }, disconnect() { disconnected++; }};
    }
    createMediaStreamSource(stream) {
      boundStreams.push(stream);
      return {connect(target) { assert.equal(typeof target.getFloatTimeDomainData, 'function'); }, disconnect() { disconnected++; }};
    }
    decodeAudioData(bytes, success, failure) {
      const job = {bytes}; this.jobs.push(job);
      return new Promise((resolve, reject) => {
        job.resolve = value => { success(value); resolve(value); };
        job.reject = error => { failure(error); reject(error); };
      });
    }
  }
  window.AudioContext = Context;
  const track = {readyState: 'live', stop() { throw new Error('Remote track belongs to transport'); }};
  const stream = {getAudioTracks: () => [track]};
  const meter = new OutputAudioMeter({window, audio, onLevel: value => levels.push(value)});
  return {meter, window, audio, frames, levels, contexts, boundStreams, stream, track,
    raw(value) { raw = value; },
    latest() { return levels.at(-1); },
    disconnected() { return disconnected; },
    step(delta = 16) {
      now += delta;
      const pending = [...frames.entries()];
      for (const [id, callback] of pending) { frames.delete(id); callback(now); }
      assert.ok(frames.size <= 1, 'at most one RAF loop owns this source');
    },
    async streamStart() {
      audio.srcObject = stream;
      meter.useStream(stream);
      assert.equal(contexts.length, 0, 'constructor/useStream do not unlock audio');
      assert.equal(await meter.unlock(), true);
    }
  };
}

function decoded(samples, channels = 1, rate = 1000) {
  return {sampleRate: rate, duration: samples.length / rate, length: samples.length,
    numberOfChannels: channels, getChannelData: () => Float32Array.from(samples)};
}
const blob = () => ({size: 32, arrayBuffer: async () => new ArrayBuffer(32)});

(async () => {
  const live = fixture();
  await live.streamStart();
  assert.deepEqual(live.boundStreams, [live.stream]);
  live.raw(0.5); live.step(); live.step();
  assert.ok(live.latest() > 0.3 && live.latest() < 0.5, 'smoothed RMS attack follows output');
  live.raw(0); for (let i = 0; i < 4; i++) live.step();
  assert.equal(live.latest(), 0, 'mouth closes within 64 ms of silence');
  live.raw(0.004); live.step(); live.step();
  assert.equal(live.latest(), 0, 'quiet floor does not create mouth chatter');
  live.raw(2); live.step(); live.step();
  assert.ok(live.latest() > 0 && live.latest() <= 1);
  for (const [field, off, on, event] of [
    ['muted', true, false, 'volumechange'], ['paused', true, false, 'pause'],
    ['ended', true, false, 'ended'], ['volume', 0, 1, 'volumechange']
  ]) {
    live.audio[field] = off; live.audio.emit(event);
    assert.equal(live.latest(), 0); assert.equal(live.frames.size, 0);
    live.audio[field] = on; live.audio.emit('playing'); live.step();
    assert.ok(live.latest() > 0);
  }
  live.audio.emit('waiting');
  assert.equal(live.latest(), 0); assert.equal(live.frames.size, 0);
  live.audio.emit('playing'); live.step();
  const stale = [...live.frames.values()][0];
  live.window.document.hidden = true; live.window.document.emit('visibilitychange');
  assert.equal(live.latest(), 0); assert.equal(live.frames.size, 0);
  live.window.document.hidden = false; live.window.document.emit('visibilitychange');
  stale(9000);
  assert.equal(live.frames.size, 1, 'a cancelled RAF cannot revive a second loop');
  live.contexts[0].state = 'suspended'; live.contexts[0].emit('statechange');
  assert.equal(live.latest(), 0); assert.equal(live.frames.size, 0);
  assert.equal(await live.meter.unlock(), true);
  live.step(); assert.ok(live.latest() > 0);
  live.meter.suspend();
  assert.equal(live.latest(), 0); assert.equal(live.frames.size, 0);
  assert.equal(live.contexts[0].state, 'suspended');
  await live.meter.unlock(); live.step();
  assert.ok(live.latest() > 0);
  assert.equal(live.contexts.length, 1, 'one owned analysis context is reused');
  assert.equal(live.boundStreams.length, 1, 'same stream graph is reused after unlock');
  live.meter.clear();
  assert.equal(live.latest(), 0); assert.equal(live.frames.size, 0);
  assert.equal(live.disconnected(), 2);
  assert.equal(live.audio.srcObject, live.stream); assert.equal(live.track.readyState, 'live');
  live.meter.destroy();
  assert.equal(live.audio.listenerCount(), 0); assert.equal(live.window.document.listenerCount(), 0);
  assert.equal(live.contexts[0].state, 'closed');

  const tts = fixture();
  await tts.meter.unlock();
  tts.audio.src = 'blob:current-tts';
  assert.equal(tts.meter.useBlob(blob(), tts.audio.src), undefined, 'decode does not block the caller/player');
  await flush();
  assert.equal(tts.frames.size, 0, 'no synthetic mouth motion before decoded samples');
  const samples = [...Array(20).fill(0.2), ...Array(20).fill(0.8), ...Array(160).fill(0)];
  tts.contexts[0].jobs[0].resolve(decoded(samples)); await flush();
  assert.equal(tts.meter.source.envelope.length, 10, 'only compact 20 ms levels are retained');
  assert.equal(tts.meter.source.blob, null);
  tts.audio.currentTime = 0.025; tts.step(); tts.step();
  assert.ok(tts.latest() > 0.5 && tts.latest() < 0.8, 'playback currentTime selects the actual audio segment');
  tts.audio.seeking = true; tts.audio.emit('seeking');
  assert.equal(tts.latest(), 0, 'seeking closes before the newly selected audio is audible');
  assert.equal(tts.frames.size, 0, 'seeking must not sample the destination ahead of playback');
  tts.audio.currentTime = 0; tts.audio.emit('timeupdate');
  assert.equal(tts.frames.size, 0, 'timeupdate during seeking cannot reopen the mouth');
  tts.audio.seeking = false; tts.audio.emit('seeked'); tts.step();
  assert.ok(tts.latest() > 0 && tts.latest() < 0.2, 'seeked resumes from the actual new position');
  tts.audio.currentTime = 0.15;
  for (let i = 0; i < 4; i++) tts.step();
  assert.equal(tts.latest(), 0);
  tts.audio.currentTime = 0.025; tts.step();
  tts.audio.src = 'blob:other-output'; tts.step();
  assert.equal(tts.latest(), 0); assert.equal(tts.frames.size, 0, 'a stale URL cannot animate a new output');
  assert.equal(tts.boundStreams.length, 0);
  tts.meter.destroy();

  const late = fixture();
  await late.meter.unlock();
  late.audio.src = 'blob:old'; late.meter.useBlob(blob(), late.audio.src); await flush();
  const old = late.contexts[0].jobs[0];
  late.audio.srcObject = late.stream; late.meter.useStream(late.stream);
  old.resolve(decoded(Array(100).fill(1))); await flush();
  assert.equal(late.meter.source.kind, 'stream', 'late TTS decode cannot replace current realtime source');
  late.raw(0); late.step(); assert.equal(late.latest(), 0);
  const current = [...late.frames.values()][0];
  late.meter.clear(); current(100);
  assert.equal(late.frames.size, 0); assert.equal(late.latest(), 0);
  late.meter.destroy();

  const earlyClear = fixture(); await earlyClear.meter.unlock();
  let finishBytes;
  earlyClear.audio.src = 'blob:cancelled';
  earlyClear.meter.useBlob({size: 10, arrayBuffer: () => new Promise(resolve => { finishBytes = resolve; })}, earlyClear.audio.src);
  earlyClear.meter.clear(); finishBytes(new ArrayBuffer(10)); await flush();
  assert.equal(earlyClear.contexts[0].jobs.length, 0, 'clear before arrayBuffer completion prevents decoding');
  earlyClear.meter.destroy();

  const fail = fixture(); await fail.meter.unlock();
  fail.audio.src = 'blob:unsupported'; fail.meter.useBlob(blob(), fail.audio.src); await flush();
  fail.contexts[0].jobs[0].reject(new Error('unsupported codec')); await flush();
  assert.equal(fail.latest(), 0); assert.equal(fail.frames.size, 0);
  assert.equal(fail.audio.paused, false); assert.equal(fail.audio.src, 'blob:unsupported');
  fail.meter.destroy();

  const noApi = fixture(); noApi.window.AudioContext = undefined;
  assert.equal(await noApi.meter.unlock(), false);
  assert.equal(noApi.latest(), 0); assert.equal(noApi.frames.size, 0); noApi.meter.destroy();
  const deferred = fixture();
  deferred.audio.src = 'blob:before-gesture'; deferred.meter.useBlob(blob(), deferred.audio.src);
  assert.equal(deferred.contexts.length, 0);
  await deferred.meter.unlock(); await flush();
  assert.equal(deferred.contexts[0].jobs.length, 1);
  deferred.meter.suspend();
  deferred.contexts[0].jobs[0].resolve(decoded(Array(100).fill(0.5))); await flush();
  assert.equal(deferred.frames.size, 0, 'decode finishing while suspended cannot restart analysis');
  await deferred.meter.unlock(); deferred.step(); assert.ok(deferred.latest() > 0);
  deferred.window.emit('pagehide');
  assert.equal(deferred.latest(), 0); assert.equal(deferred.frames.size, 0); deferred.meter.destroy();

  const ended = fixture(); await ended.streamStart(); ended.raw(0.5); ended.step();
  ended.track.readyState = 'ended'; ended.step();
  assert.equal(ended.latest(), 0); assert.equal(ended.frames.size, 0, 'ended remote tracks cannot animate');
  ended.meter.source.node.disconnect = ended.meter.source.analyser.disconnect = () => { throw new Error('already detached'); };
  assert.doesNotThrow(() => ended.meter.clear(), 'cleanup failures must not escape into speech controls');
  assert.equal(ended.audio.paused, false); ended.meter.destroy();

  const unlocking = fixture(); await unlocking.streamStart();
  unlocking.raw(0.5);
  const context = unlocking.contexts[0], attempts = [];
  context.state = 'suspended'; context.emit('statechange');
  context.resume = () => new Promise((resolve, reject) => attempts.push({resolve, reject}));
  const oldUnlock = unlocking.meter.unlock();
  const newUnlock = unlocking.meter.unlock();
  context.state = 'running'; context.emit('statechange'); attempts[1].resolve();
  assert.equal(await newUnlock, true);
  unlocking.step(); assert.ok(unlocking.latest() > 0);
  attempts[0].reject(new Error('old audio gesture rejected'));
  assert.equal(await oldUnlock, false);
  assert.ok(unlocking.latest() > 0, 'an old unlock failure cannot blank newer audible output');
  assert.equal(unlocking.frames.size, 1, 'an old unlock failure cannot cancel the current measurement loop');
  unlocking.meter.destroy();

  console.log('PASS: output RMS smoothing/silence, stream and compact TTS envelopes, source ownership, pause/mute/seek/context/visibility gates, stale unlock/RAF/decode guards and cleanup without playback or microphone access.');
})().catch(error => { console.error(error); process.exitCode = 1; });
