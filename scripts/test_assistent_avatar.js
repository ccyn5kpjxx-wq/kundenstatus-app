// Synthetic DOM and timers only: no browser permissions, sound, or network.
const assert = require('node:assert/strict');
const {AvatarAnimationController, initAssistantAvatar} = require('../static/assistent-avatar.js');

class Events {
  constructor() { this.listeners = new Map(); }
  addEventListener(event, fn) {
    if (!this.listeners.has(event)) this.listeners.set(event, new Set());
    this.listeners.get(event).add(fn);
  }
  removeEventListener(event, fn) { this.listeners.get(event)?.delete(fn); }
  async emit(event, value = {}) {
    await Promise.all([...this.listeners.get(event) || []].map(fn => fn(value)));
  }
}

class Element extends Events {
  constructor(dataset = {}) {
    super(); this.dataset = dataset; this.attributes = {}; this.value = ''; this.textContent = '';
    this.disabled = false; this.open = false; this.focusCount = 0;
    this.style = {setProperty(name, value) { this[name] = value; }};
  }
  setAttribute(name, value) { this.attributes[name] = value; }
  focus() { this.focusCount++; }
  showModal() { this.open = true; }
  close() { this.open = false; this.emit('close'); }
  querySelectorAll() { return this.children || []; }
}

function clock() {
  let id = 0;
  const timers = new Map();
  return {
    timers,
    setTimeout(fn, delay) { timers.set(++id, {fn, delay}); return id; },
    clearTimeout(timer) { timers.delete(timer); },
    tick() {
      const [timer, value] = timers.entries().next().value || [];
      assert.ok(value, 'an expected animation timer exists');
      timers.delete(timer); value.fn(); return value.delay;
    }
  };
}

class Observer {
  constructor(callback) { this.callback = callback; this.disconnected = false; }
  observe(target, config) { this.target = target; this.config = config; }
  trigger() { this.callback(); }
  disconnect() { this.disconnected = true; }
}

function animationFixture(state = 'idle') {
  const time = clock();
  const document = new Events(); document.hidden = false;
  const media = new Events(); media.matches = false;
  const avatar = new Element({state});
  const portraits = [new Element(), new Element()];
  const controller = new AvatarAnimationController({
    avatar, portraits, document, mediaQuery: media, MutationObserver: Observer,
    setTimeout: time.setTimeout, clearTimeout: time.clearTimeout, random: () => 0
  });
  return {controller, avatar, portraits, document, media, time,
    state(value) { avatar.dataset.state = value; controller.observer.trigger(); }};
}

function pickerFixture(character = 'chris') {
  const time = clock();
  const document = new Events(); document.hidden = false;
  const window = new Events();
  const media = new Events(); media.matches = false;
  Object.assign(window, {setTimeout: time.setTimeout, clearTimeout: time.clearTimeout,
    MutationObserver: Observer, matchMedia: () => media, AbortController});
  const elements = Object.fromEntries([
    'assistant', 'avatar', 'avatar-picker', 'avatar-choose', 'avatar-picker-close',
    'avatar-picker-status', 'profile-character', 'avatar-choice-label', 'avatar-name'
  ].map(id => [id, new Element()]));
  elements.assistant.dataset.character = character;
  elements.avatar.dataset.state = 'thinking';
  elements.avatar.children = [new Element()];
  const options = ['chris', 'mila', 'robot', 'drache', 'zauberfuchs', 'einhorn', 'phoenix', 'greif', 'waldgeist', 'unexpected'].map(character => new Element({characterOption: character}));
  elements['avatar-picker'].children = options;
  elements['avatar-name'].textContent = 'Existing personal name';
  const token = {content: 'synthetic-csrf'};
  document.getElementById = id => elements[id];
  document.querySelector = () => token;
  let resolve, reject;
  const requests = [];
  window.fetch = (url, options) => {
    requests.push({url, options});
    return new Promise((yes, no) => {
      resolve = yes; reject = no;
      options.signal.addEventListener('abort', () => no(new Error('aborted')));
    });
  };
  // Accessing microphone/browser media in this module would fail this fixture.
  Object.defineProperty(window, 'navigator', {get() { throw new Error('No media access allowed'); }});
  const app = initAssistantAvatar(document, window);
  return {app, document, window, media, elements, options, requests, token, time,
    resolve: value => resolve(value), reject: error => reject(error)};
}

(async () => {
  const motion = animationFixture('speaking');
  const mouthLevel = () => Number(motion.avatar.style['--avatar-output-level']);
  assert.deepEqual(motion.portraits.map(p => p.dataset.frame), ['rest', 'rest']);
  assert.equal(motion.time.timers.size, 0, 'speaking without audible output has no fake animation loop');
  assert.equal(motion.avatar.dataset.audioActive, 'false');
  motion.controller.setAudioLevel(0.1);
  const quietScale = Number(motion.avatar.style['--avatar-mouth-scale']);
  const stale = [...motion.time.timers.values()][0].fn;
  motion.controller.setAudioLevel(0.9);
  assert.equal(mouthLevel(), 0.9);
  assert.equal(motion.avatar.dataset.audioActive, 'true');
  assert.ok(Number(motion.avatar.style['--avatar-mouth-scale']) > quietScale, 'mouth opening follows measured loudness');
  assert.deepEqual(motion.portraits.map(p => p.dataset.frame), ['rest', 'rest'], 'loudness never switches whole portrait frames');
  stale();
  assert.equal(mouthLevel(), 0.9, 'a cancelled watchdog cannot clear newer samples');
  assert.equal(motion.time.timers.size, 1, 'only one finite meter watchdog is pending');
  assert.equal(motion.time.tick(), 220);
  assert.equal(mouthLevel(), 0, 'a stalled output meter returns to rest');
  assert.equal(motion.time.timers.size, 0, 'watchdog is not an animation loop');
  for (const value of [0, -1, 0.02, NaN, Infinity, '0.9', null, undefined]) {
    motion.controller.setAudioLevel(0.8);
    motion.controller.setAudioLevel(value);
    assert.equal(mouthLevel(), 0, `silence/invalid sample ${value} closes immediately`);
    assert.equal(motion.avatar.dataset.audioActive, 'false');
    assert.equal(motion.time.timers.size, 0);
  }
  motion.controller.setAudioLevel(5);
  assert.equal(mouthLevel(), 1, 'levels clamp to the normalized output range');
  assert.equal(Number(motion.avatar.style['--avatar-mouth-scale']), 1);
  const interrupted = [...motion.time.timers.values()][0].fn;
  motion.state('listening');
  assert.equal(motion.portraits[0].dataset.frame, 'rest', 'barge-in immediately closes mouth');
  assert.equal(mouthLevel(), 0);
  motion.controller.setAudioLevel(0.9);
  assert.equal(mouthLevel(), 0, 'late output/microphone samples cannot animate while listening');
  interrupted();
  assert.equal(motion.portraits[0].dataset.frame, 'rest', 'a cancelled speaking frame cannot revive');
  assert.equal(motion.time.tick(), 3200);
  assert.equal(motion.portraits[0].dataset.frame, 'blink');
  assert.equal(motion.time.tick(), 130);
  assert.equal(motion.portraits[0].dataset.frame, 'rest');
  for (const state of ['thinking', 'error', 'unexpected']) {
    motion.state(state);
    motion.controller.setAudioLevel(1);
    assert.equal(mouthLevel(), 0);
    assert.equal(motion.portraits[0].dataset.frame, 'rest');
    assert.equal(motion.time.timers.size, 0);
  }
  motion.state('speaking');
  motion.controller.setAudioLevel(0.8);
  motion.document.hidden = true; await motion.document.emit('visibilitychange');
  assert.equal(motion.time.timers.size, 0); assert.equal(motion.portraits[0].dataset.frame, 'rest');
  assert.equal(mouthLevel(), 0);
  motion.controller.setAudioLevel(0.8); assert.equal(mouthLevel(), 0);
  motion.document.hidden = false; await motion.document.emit('visibilitychange');
  assert.equal(motion.time.timers.size, 0, 'becoming visible waits for a fresh audible sample');
  motion.controller.setAudioLevel(0.8);
  motion.media.matches = true; await motion.media.emit('change');
  assert.equal(motion.time.timers.size, 0); assert.equal(motion.portraits[0].dataset.frame, 'rest');
  assert.equal(mouthLevel(), 0);
  motion.controller.setAudioLevel(0.8); assert.equal(mouthLevel(), 0);
  motion.state('idle'); assert.equal(motion.time.timers.size, 0, 'reduced motion also prevents blinking');
  motion.media.matches = false; await motion.media.emit('change');
  assert.equal(motion.time.timers.size, 1);
  motion.state('speaking'); motion.controller.setAudioLevel(0.8);
  motion.controller.suspend();
  assert.equal(mouthLevel(), 0); assert.equal(motion.time.timers.size, 0);
  motion.controller.setAudioLevel(0.8); assert.equal(mouthLevel(), 0);
  motion.controller.resume();
  assert.equal(mouthLevel(), 0, 'page restore does not reuse a stale level');
  motion.controller.setAudioLevel(0.8);
  motion.controller.destroy();
  assert.equal(motion.time.timers.size, 0); assert.equal(motion.controller.observer.disconnected, true);
  assert.equal(mouthLevel(), 0);
  motion.controller.setAudioLevel(0.8);
  assert.equal(mouthLevel(), 0); assert.equal(motion.time.timers.size, 0);
  assert.equal(motion.document.listeners.get('visibilitychange').size, 0);
  assert.equal(motion.media.listeners.get('change').size, 0);

  // Exercise the actual browser export without browser/media/network access.
  const browserExport = pickerFixture(); browserExport.app.destroy();
  browserExport.document.readyState = 'loading';
  browserExport.window.document = browserExport.document;
  require('node:vm').runInNewContext(require('node:fs').readFileSync(require.resolve('../static/assistent-avatar.js'), 'utf8'), {window: browserExport.window});
  assert.equal(browserExport.window.AssistantAvatar.instance, null);
  await browserExport.document.emit('DOMContentLoaded');
  const exposed = browserExport.window.AssistantAvatar.instance;
  assert.equal(typeof exposed.animation.setAudioLevel, 'function', 'output meter has a stable public integration point');
  browserExport.elements.avatar.dataset.state = 'speaking'; exposed.animation.observer.trigger();
  exposed.animation.setAudioLevel(0.6);
  assert.equal(browserExport.elements.avatar.style['--avatar-output-level'], '0.600');
  await browserExport.window.emit('pagehide');
  assert.equal(browserExport.elements.avatar.style['--avatar-output-level'], '0');
  exposed.destroy();

  const picker = pickerFixture();
  await picker.elements['avatar-choose'].emit('click');
  assert.equal(picker.elements['avatar-picker'].open, true);
  assert.equal(picker.options[0].attributes['aria-pressed'], 'true');
  const saving = picker.options[1].emit('click');
  assert.equal(picker.requests.length, 1);
  assert.equal(picker.elements.assistant.dataset.character, 'chris');
  assert.equal(picker.options[0].attributes['aria-pressed'], 'true', 'saved selection stays until confirmation');
  assert.equal(picker.options[1].dataset.pending, 'true');
  assert.ok(picker.options.every(button => button.disabled));
  await picker.options[2].emit('click');
  assert.equal(picker.requests.length, 1, 'programmatic duplicate clicks cannot launch a second save');
  let cancelled = false;
  await picker.elements['avatar-picker'].emit('cancel', {preventDefault() { cancelled = true; }});
  assert.equal(cancelled, true);
  const request = picker.requests[0];
  assert.equal(request.url, '/werkstatt/assistent/avatar');
  assert.equal(request.options.method, 'POST');
  assert.equal(request.options.credentials, 'same-origin');
  assert.equal(request.options.headers['X-CSRF-Token'], 'synthetic-csrf');
  assert.deepEqual(JSON.parse(request.options.body), {character: 'mila'});
  picker.resolve({ok: true, json: async () => ({ok: true, character: 'mila'})});
  await saving;
  assert.equal(picker.elements.assistant.dataset.character, 'mila');
  assert.equal(picker.elements['profile-character'].value, 'mila');
  assert.equal(picker.elements['avatar-choice-label'].textContent, 'Mila');
  assert.equal(picker.options[1].attributes['aria-pressed'], 'true');
  assert.equal(picker.elements['avatar-picker'].open, false);
  assert.ok(picker.elements['avatar-choose'].focusCount > 0);
  assert.equal(picker.elements['avatar-name'].textContent, 'Existing personal name');
  assert.equal(picker.time.timers.size, 0, 'save timeout cleaned up');
  assert.equal(picker.options.find(button => button.dataset.characterOption === 'unexpected').disabled, true, 'unknown figures cannot be selected');
  await picker.window.emit('pagehide');
  assert.equal(picker.app.animation.suspended, true);
  await picker.window.emit('pageshow');
  assert.equal(picker.app.animation.suspended, false);
  picker.app.destroy();

  for (const response of [
    {ok: false},
    {ok: true, json: async () => ({ok: true, character: 'robot'})},
    {ok: true, json: async () => { throw new Error('invalid JSON'); }}
  ]) {
    const failed = pickerFixture();
    await failed.elements['avatar-choose'].emit('click');
    const pending = failed.options[1].emit('click');
    failed.resolve(response); await pending;
    assert.equal(failed.elements.assistant.dataset.character, 'chris');
    assert.equal(failed.options[0].attributes['aria-pressed'], 'true');
    assert.match(failed.elements['avatar-picker-status'].textContent, /nicht gespeichert/);
    assert.equal(failed.elements['avatar-picker'].open, true);
    assert.equal(failed.options[1].disabled, false);
    assert.equal(failed.time.timers.size, 0);
    failed.app.destroy();
  }
  const timeout = pickerFixture();
  const pendingTimeout = timeout.options[1].emit('click');
  assert.equal(timeout.time.tick(), 15000); await pendingTimeout;
  assert.equal(timeout.requests[0].options.signal.aborted, true);
  assert.equal(timeout.options[1].disabled, false);
  assert.equal(timeout.elements.assistant.dataset.character, 'chris');
  timeout.app.destroy();

  const disposed = pickerFixture();
  const pendingDispose = disposed.options[1].emit('click');
  disposed.app.destroy(); await pendingDispose;
  assert.equal(disposed.time.timers.size, 0);
  assert.equal(disposed.elements.assistant.dataset.character, 'chris');
  const missingToken = pickerFixture(); missingToken.token.content = '';
  await missingToken.options[1].emit('click');
  assert.equal(missingToken.requests.length, 0);
  assert.match(missingToken.elements['avatar-picker-status'].textContent, /Anmeldung/);
  missingToken.app.destroy();
  for (const [character, label] of [['drache', 'Drache'], ['zauberfuchs', 'Zauberfuchs'], ['einhorn', 'Einhorn'], ['phoenix', 'Phönix'], ['greif', 'Greif'], ['waldgeist', 'Waldgeist']]) {
    const fantasy = pickerFixture();
    const option = fantasy.options.find(button => button.dataset.characterOption === character);
    assert.equal(option.disabled, false);
    await fantasy.elements['avatar-choose'].emit('click');
    const pending = option.emit('click');
    assert.deepEqual(JSON.parse(fantasy.requests[0].options.body), {character});
    fantasy.resolve({ok: true, json: async () => ({ok: true, character})});
    await pending;
    assert.equal(fantasy.elements.assistant.dataset.character, character);
    assert.equal(fantasy.elements['profile-character'].value, character);
    assert.equal(fantasy.elements['avatar-choice-label'].textContent, label);
    assert.equal(option.attributes['aria-pressed'], 'true');
    fantasy.app.destroy();
  }
  for (const character of ['chris', 'mila', 'robot']) {
    const legacy = pickerFixture(character);
    const option = legacy.options.find(button => button.dataset.characterOption === character);
    assert.equal(option.attributes['aria-pressed'], 'true', 'existing saved figure stays selected');
    assert.equal(legacy.elements['profile-character'].value, character);
    await option.emit('click');
    assert.equal(legacy.requests.length, 0, 'choosing the saved legacy figure does not rewrite it');
    legacy.app.destroy();
  }
  const fallback = pickerFixture('missing-character');
  assert.equal(fallback.elements['profile-character'].value, 'drache');
  assert.equal(fallback.elements['avatar-choice-label'].textContent, 'Drache');
  assert.equal(fallback.options.find(button => button.dataset.characterOption === 'drache').attributes['aria-pressed'], 'true');
  fallback.app.destroy();
  console.log('PASS: measured output levels, silent/stalled meter rest, no fake phonemes, state gating, blink, reduced motion, visibility, cleanup and public instance; confirmed picker persistence, CSRF, duplicate/error/timeout guards, focus and unchanged personal name.');
})().catch(error => { console.error(error); process.exitCode = 1; });
