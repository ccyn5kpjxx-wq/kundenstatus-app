'use strict';
// Isolated VM, synthetic DOM, tokens and fetch only. No account or real network.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/mitarbeiter_einrichtung.js'), 'utf8');
const TOKEN = 'A'.repeat(64);
const settle = async () => { for (let n = 0; n < 8; n++) await new Promise(resolve => setImmediate(resolve)); };
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return {promise, resolve, reject}; };
const reply = (data, status = 200) => ({ok: status >= 200 && status < 300, json: async () => data});
class Element {
  constructor() { this.value = ''; this.textContent = ''; this.hidden = false; this.disabled = false; this.events = {}; this.classes = new Set(); this.dataset = {}; this.validity = ''; }
  classList = {add: name => this.classes.add(name), contains: name => this.classes.has(name)};
  addEventListener(name, fn) { (this.events[name] ||= []).push(fn); }
  fire(name, event = {}) { return Promise.all((this.events[name] || []).map(fn => fn(event))); }
  setCustomValidity(message) { this.validity = message; }
  focus() { this.focused = true; }
  select() { this.selected = true; }
  set innerHTML(_) { throw new Error('HTML injection sink is forbidden'); }
}
function fixture({hash = '#token=' + TOKEN, hiddenToken = '', admin = false, response = null, write = null} = {}) {
  const names = ['employee-setup', 'setup-form', 'setup-token', 'setup-status', 'setup-password', 'setup-password-confirm', 'setup-submit', 'setup-title', 'setup-identity', 'copy-status'];
  const elements = Object.fromEntries(names.map(name => [name, new Element()]));
  const el = name => elements[name];
  el('employee-setup').dataset.checkUrl = '/werkstatt/zugang/einrichten/pruefen';
  el('setup-form').hidden = true;
  el('setup-token').value = hiddenToken;
  el('setup-form').querySelector = () => ({value: 'synthetic-csrf'});
  el('setup-form').reportValidity = () => !el('setup-password-confirm').validity && el('setup-password').value.length >= 12 && el('setup-password-confirm').value.length >= 12;
  const copies = [], buttons = [], calls = [], historyCalls = [], timers = new Map(), events = {}; let nextTimer = 0;
  if (admin) {
    for (const id of ['invite-link-1', 'invite-link-2']) {
      const input = elements[id] = new Element(); input.value = 'https://portal.test/werkstatt/zugang/einrichten#token=' + (id.endsWith('1') ? 'B' : 'C').repeat(64);
      const button = new Element(); button.dataset.copyLink = id; buttons.push(button);
    }
  }
  const context = {document: {getElementById: id => admin && id === 'employee-setup' ? null : elements[id], querySelectorAll: () => buttons},
    location: {hash, pathname: '/werkstatt/zugang/einrichten'},
    history: {replaceState(...args) { historyCalls.push(args); context.location.hash = ''; }},
    window: {addEventListener(name, fn) { events[name] = fn; }},
    navigator: {clipboard: {async writeText(value) { copies.push(value); if (write) return write(value); }}},
    URLSearchParams, AbortController,
    setTimeout(fn, ms) { const id = ++nextTimer; timers.set(id, {fn, ms}); return id; }, clearTimeout(id) { timers.delete(id); },
    fetch(url, options) { calls.push({url, options}); return response ? response(options) : Promise.resolve(reply({valid: true, employee: {id: 7, name: 'Testperson'}})); }
  };
  vm.runInNewContext(source, context, {filename: 'mitarbeiter_einrichtung.js'});
  return {el, calls, historyCalls, timers, events, copies, buttons, context,
    fireTimeout() { for (const entry of [...timers.values()]) entry.fn(); },
    submit() { const event = {prevented: false, preventDefault() {this.prevented = true;}}; el('setup-form').fire('submit', event); return event; }};
}
test('fragment is erased synchronously and check sends only POST token with CSRF and no cache', async () => {
  const gate = deferred(), f = fixture({hash: '#token=' + TOKEN + '&ignored=secret', response: () => gate.promise});
  assert.deepEqual(f.historyCalls[0], [null, '', '/werkstatt/zugang/einrichten']);
  assert.equal(f.context.location.hash, ''); assert.equal(f.el('setup-form').hidden, true);
  assert.equal(f.calls.length, 1); const call = f.calls[0];
  assert.equal(call.url, '/werkstatt/zugang/einrichten/pruefen'); assert.equal(call.url.includes(TOKEN), false);
  assert.equal(call.options.method, 'POST'); assert.equal(call.options.credentials, 'same-origin'); assert.equal(call.options.cache, 'no-store');
  assert.equal(call.options.headers['X-CSRF-Token'], 'synthetic-csrf');
  assert.deepEqual(JSON.parse(call.options.body), {token: TOKEN});
  gate.resolve(reply({valid: true, employee: {id: 7, name: 'Testperson'}})); await settle();
  assert.equal(f.el('setup-form').hidden, false); assert.equal(f.timers.size, 0);
});
test('missing, short and injected fragments never cause a request or expose password form', async () => {
  for (const hash of ['', '#token=short', '#token=%3Cimg%20onerror%3Dx%3E', '#token=' + 'A'.repeat(151)]) {
    const f = fixture({hash}); await settle(); assert.equal(f.calls.length, 0); assert.equal(f.el('setup-form').hidden, true);
    assert.equal(f.el('setup-status').classList.contains('is-error'), true);
    assert.equal(f.el('setup-status').textContent.includes('<img'), false);
    if (hash) assert.equal(f.context.location.hash, '');
  }
});
test('server-rendered retry token is checked in POST without adding it to address or storing password', async () => {
  const f = fixture({hash: '', hiddenToken: TOKEN});
  f.el('setup-password').value = 'never-send-this-to-the-check'; await settle();
  assert.equal(f.historyCalls.length, 0); assert.deepEqual(JSON.parse(f.calls[0].options.body), {token: TOKEN});
  assert.equal(f.el('setup-token').value, TOKEN); assert.equal(f.el('setup-form').hidden, false);
});
test('employee identity and reflected error markup are text rather than executable HTML', async () => {
  const name = '<img src=x onerror=alert(1)>', f = fixture({response: async () => reply({valid: true, employee: {id: 7, name}})});
  await settle(); assert.equal(f.el('setup-title').textContent, 'Hallo ' + name); assert.match(f.el('setup-identity').textContent, /7/);
  const message = '<svg onload=alert(1)>', g = fixture({response: async () => reply({valid: false, error: message}, 400)});
  await settle(); assert.equal(g.el('setup-status').textContent, message); assert.equal(g.el('setup-form').hidden, true);
});
test('only explicitly valid checks with integer own identity can expose form', async () => {
  for (const data of [{valid: false}, {valid: 'true', employee: {id: 7, name: 'Test'}}, {valid: true}, {valid: true, employee: {id: '7', name: 'Test'}}, {valid: true, employee: {id: 7, name: null}}]) {
    const f = fixture({response: async () => reply(data)}); await settle();
    assert.equal(f.el('setup-form').hidden, true); assert.equal(f.el('setup-status').classList.contains('is-error'), true); assert.equal(f.timers.size, 0);
  }
});
test('HTTP failure stays closed even if response claims valid and no token is reflected by default error', async () => {
  const f = fixture({response: async () => reply({valid: true, employee: {id: 7, name: 'Test'}}, 500)}); await settle();
  assert.equal(f.el('setup-form').hidden, true); assert.equal(f.el('setup-status').textContent.includes(TOKEN), false);
  assert.equal(f.timers.size, 0);
});
test('network and unreadable JSON failures keep form hidden and clear pending timer', async () => {
  const f = fixture({response: async () => {throw new Error('Network unavailable');}}); await settle();
  assert.equal(f.el('setup-form').hidden, true); assert.equal(f.timers.size, 0); assert.match(f.el('setup-status').textContent, /Network unavailable/);
  const g = fixture({response: async () => ({ok: true, json: async () => {throw new SyntaxError('Invalid JSON');}})}); await settle();
  assert.equal(g.el('setup-form').hidden, true); assert.equal(g.timers.size, 0);
});
test('15 second check timeout aborts request and explains retry without leaking token', async () => {
  const f = fixture({response: options => new Promise((resolve, reject) => options.signal.addEventListener('abort', () => {const error = new Error('abort'); error.name = 'AbortError'; reject(error);} ))});
  assert.equal([...f.timers.values()][0].ms, 15000); f.fireTimeout(); await settle();
  assert.equal(f.calls[0].options.signal.aborted, true); assert.equal(f.el('setup-form').hidden, true);
  assert.match(f.el('setup-status').textContent, /ursprünglichen Einrichtungslink/); assert.equal(f.el('setup-status').textContent.includes(TOKEN), false); assert.equal(f.timers.size, 0);
});
test('both password input events recalculate mismatch and invalid submission remains editable', async () => {
  const f = fixture(); await settle(); f.el('setup-password').value = 'self chosen password'; f.el('setup-password-confirm').value = 'different password';
  await f.el('setup-password-confirm').fire('input'); assert.match(f.el('setup-password-confirm').validity, /überein/);
  const invalid = f.submit(); assert.equal(invalid.prevented, true); assert.equal(f.el('setup-submit').disabled, false);
  f.el('setup-password').value = 'different password'; await f.el('setup-password').fire('input'); assert.equal(f.el('setup-password-confirm').validity, '');
  f.el('setup-password-confirm').value = ''; await f.el('setup-password-confirm').fire('input'); assert.equal(f.el('setup-password-confirm').validity, ''); assert.equal(f.submit().prevented, true);
});
test('valid native submission disables double click; pageshow restores it without another POST', async () => {
  const f = fixture(); await settle(); f.el('setup-password').value = f.el('setup-password-confirm').value = 'self chosen password';
  assert.equal(f.submit().prevented, false); assert.equal(f.el('setup-submit').disabled, true); assert.match(f.el('setup-submit').textContent, /eingerichtet/);
  f.events.pageshow(); assert.equal(f.el('setup-submit').disabled, false); assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].options.body.includes('self chosen password'), false);
});
test('admin copy chooses only requested employee link; failure selects same link for manual copy', async () => {
  const f = fixture({admin: true}); await f.buttons[1].fire('click'); assert.deepEqual(f.copies, [f.el('invite-link-2').value]); assert.match(f.el('copy-status').textContent, /nur an diese Person/); assert.equal(f.calls.length, 0);
  const g = fixture({admin: true, write: async () => {throw new Error('Clipboard denied');}}); await g.buttons[0].fire('click');
  assert.equal(g.el('invite-link-1').focused, true); assert.equal(g.el('invite-link-1').selected, true); assert.equal(g.el('invite-link-2').focused, undefined); assert.match(g.el('copy-status').textContent, /markierten Link/);
});
test('password template requires matching new passwords and has no third party request or referrer exposure', () => {
  const template = fs.readFileSync(path.join(__dirname, '../templates/mitarbeiter_einrichtung.html'), 'utf8');
  for (const id of ['setup-password', 'setup-password-confirm']) {
    const field = template.match(new RegExp('<input[^>]*id="' + id + '"[^>]*>'))?.[0];
    assert.ok(field); assert.match(field, /type="password"/); assert.match(field, /required/); assert.match(field, /minlength="12"/); assert.match(field, /maxlength="256"/); assert.match(field, /autocomplete="new-password"/);
  }
  assert.match(template, /name="referrer" content="no-referrer"/); assert.doesNotMatch(template, /(?:src|href)="https?:\/\//);
  assert.match(template, /role="status" aria-live="polite"/); assert.match(template, /method="post" action="\/werkstatt\/zugang\/einrichten"/);
});
