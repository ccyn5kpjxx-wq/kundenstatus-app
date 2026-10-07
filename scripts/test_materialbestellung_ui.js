'use strict';
// Synthetic files, DOM, clock and fetch only. Never uploads a real photo or orders material.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {createMaterialOrderController} = require('../static/materialbestellung.js');
class Element {
  constructor(tag = 'div') {
    this.tagName = tag.toUpperCase(); this.children = []; this.dataset = {}; this.attributes = {};
    this.events = {}; this.value = ''; this.textContent = ''; this.disabled = false; this.hidden = false; this.files = [];
  }
  append(...items) { this.children.push(...items); }
  replaceChildren(...items) { this.children = items; this.textContent = ''; }
  setAttribute(key, value) { this.attributes[key] = value; }
  addEventListener(key, fn) { (this.events[key] ||= []).push(fn); }
  fire(key) { for (const fn of this.events[key] || []) fn({preventDefault() {}}); }
  click() { this.clicked = true; this.fire('click'); }
  focus() { this.focused = true; }
  set innerHTML(_) { throw new Error('HTML injection sink is forbidden'); }
}
const descendants = node => node.children.flatMap(child => [child, ...descendants(child)]);
const settle = async () => {for (let i = 0; i < 8; i++) await new Promise(resolve => setImmediate(resolve));};
const deferred = () => {let resolve, reject; const promise = new Promise((a, b) => {resolve = a; reject = b;}); return {promise, resolve, reject};};
const photo = (name = 'test.jpg', size = 120, type = 'image/jpeg') => ({name, size, type});
const reply = (data, status = 200) => ({ok: status >= 200 && status < 300, status, json: async () => data});
class Data {
  constructor() {this.fields = new Map();}
  append(name, value, filename) {this.fields.set(name, value); if (filename) (this.filenames ||= new Map()).set(name, filename);}
}
function fixture({saved = null, scannerFactory = null, limitCents = '25000'} = {}) {
  const names = ['camera', 'album', 'camera-button', 'album-button', 'scan-button', 'submit', 'form', 'count', 'empty', 'summary', 'status', 'recovery', 'recovery-text', 'check', 'reload', 'refresh', 'items', 'history', 'history-status'];
  const elements = Object.fromEntries(names.map(name => ['material-' + name, new Element()]));
  const host = new Element(); host.dataset = {endpoint: '/werkstatt/materialbestellung/anforderungen', actor: 'mitarbeiter:7', limitCents};
  const document = {getElementById: id => id === 'materialbestellung' ? host : elements[id], createElement: tag => new Element(tag), querySelector: () => ({content: 'synthetic-csrf'})};
  const calls = [], revoked = [], timers = new Map(), store = new Map(), events = {}; let seq = 0, timerId = 0;
  const key = 'materialbestellung:pending:mitarbeiter:7'; if (saved) store.set(key, JSON.stringify(saved));
  const api = {async fetch(url, settings) {
    calls.push({url, settings});
    if (settings.method === 'POST') {
      const rows = JSON.parse(settings.body.fields.get('positionen'));
      const requestId = settings.body.fields.get('request_id');
      return reply({request_id: requestId, anforderungen: rows.map((row, index) => ({client_id: row.id, id: index + 1,
        request_id: requestId, code: 'M-' + (index + 1), quantity: String(row.menge), unit: 'Stück', urgent: row.dringend,
        state: 'review', label: 'Interne Prüfung', product: 'Synthetischer Testartikel', vorgang: row.vorgang, beschreibung: row.beschreibung}))});
    }
    return reply({anforderungen: []});
  }};
  const controller = createMaterialOrderController({document, fetch: (...args) => api.fetch(...args),
    URL: {createObjectURL: () => 'blob:synthetic-' + ++seq, revokeObjectURL: url => revoked.push(url)},
    crypto: {randomUUID: () => '00000000-0000-4000-8000-' + String(++seq).padStart(12, '0')},
    FormData: Data, AbortController, scannerFactory, setTimeout: fn => {timers.set(++timerId, fn); return timerId;},
    clearTimeout: id => timers.delete(id), storage: {getItem: k => store.get(k), setItem: (k, v) => store.set(k, v), removeItem: k => store.delete(k)},
    window: {addEventListener: (name, fn) => {events[name] = fn;}}});
  const $ = name => elements['material-' + name];
  const cards = () => $('items').children;
  const field = (index, tag, predicate = () => true) => descendants(cards()[index]).find(el => el.tagName === tag && predicate(el));
  const quantity = index => field(index, 'INPUT', el => el.type === 'number');
  const step = (index, text) => field(index, 'BUTTON', el => el.textContent === text);
  const posts = () => calls.filter(call => call.settings.method === 'POST');
  return {controller, $, calls, revoked, timers, store, key, api, cards, field, quantity, step, posts, events,
    fireTimers() {for (const fn of [...timers.values()]) fn();},
    historyText: () => descendants($('history')).map(el => el.textContent).join(' ')};
}

test('two photos retain separate quantities, urgent choice and exact multipart binding', async () => {
  const f = fixture(), first = photo('one.jpg'), second = photo('two.png', 124, 'image/png');
  f.controller.addFiles([first, second]);
  f.step(0, '+').click(); f.step(0, '+').click(); f.step(1, '+').click();
  const urgent = f.field(1, 'INPUT', el => el.type === 'radio' && el.value === 'dringend'); urgent.checked = true; urgent.fire('change');
  await f.controller.submit();
  const call = f.posts()[0], fields = call.settings.body.fields, rows = JSON.parse(fields.get('positionen'));
  assert.deepEqual(rows.map(({menge, dringend}) => ({menge, dringend})), [{menge: 3, dringend: false}, {menge: 2, dringend: true}]);
  assert.equal(fields.get('foto_' + rows[0].id), first); assert.equal(fields.get('foto_' + rows[1].id), second);
  assert.equal(fields.get('csrf_token'), 'synthetic-csrf'); assert.equal(call.settings.headers['X-CSRF-Token'], 'synthetic-csrf');
  assert.equal(call.settings.credentials, 'same-origin'); assert.equal(f.cards().length, 0); assert.equal(f.revoked.length, 2);
  assert.match(f.historyText(), /3 Stück/); assert.match(f.historyText(), /2 Stück/); assert.match(f.historyText(), /Noch nicht bestellt/);
});

test('stepper boundaries and invalid manual quantity never create zero, fraction or overflow', async () => {
  const f = fixture(); f.controller.addFiles([photo()]);
  assert.equal(f.step(0, '−').disabled, true); f.step(0, '−').click(); assert.equal(f.quantity(0).value, '1');
  const input = f.quantity(0); input.value = '999'; input.fire('change'); assert.equal(f.step(0, '+').disabled, true);
  f.step(0, '+').click(); assert.equal(input.value, '999');
  for (const value of ['0', '-1', '1.5', '1000', '', '2e2', '1x']) {
    input.value = value; input.fire('change'); assert.equal(input.value, '999'); assert.match(f.$('status').textContent, /1 bis 999/);
  }
  input.value = '4'; // A focused field need not have emitted change before submitting.
  await f.controller.submit(); assert.equal(JSON.parse(f.posts()[0].settings.body.fields.get('positionen'))[0].menge, 4);
});

test('explicit Monday choice cancels urgency only for that image', async () => {
  const f = fixture(); f.controller.addFiles([photo(), photo('second.png', 123, 'image/png')]);
  for (let i = 0; i < 2; i++) {
    const urgent = f.field(i, 'INPUT', el => el.value === 'dringend'); urgent.checked = true; urgent.fire('change');
  }
  const monday = f.field(0, 'INPUT', el => el.value === 'montag'); monday.checked = true; monday.fire('change');
  assert.equal(f.field(0, 'INPUT', el => el.value === 'dringend').checked, false);
  await f.controller.submit();
  assert.deepEqual(JSON.parse(f.posts()[0].settings.body.fields.get('positionen')).map(row => row.dringend), [false, true]);
});

test('screenshot inquiry and description stay bound to their own image without HTML interpretation', async () => {
  const f = fixture(); f.controller.addFiles([photo('teil-screenshot.png', 123, 'image/png'), photo('material.jpg')]);
  const description = f.field(0, 'TEXTAREA'); description.value = 'Halter am Kotflügel <img onerror=alert(1)> dringend 9 Stück'; description.fire('input');
  const inquiry = f.field(0, 'INPUT', el => el.type === 'checkbox'); inquiry.checked = true; inquiry.fire('change');
  f.step(1, '+').click(); await f.controller.submit();
  const rows = JSON.parse(f.posts()[0].settings.body.fields.get('positionen'));
  assert.equal(rows[0].vorgang, 'anfrage'); assert.equal(rows[0].menge, 1); assert.equal(rows[0].dringend, false);
  assert.equal(rows[0].beschreibung, description.value);
  assert.equal(rows[1].vorgang, 'bestellung'); assert.equal(rows[1].beschreibung, ''); assert.equal(rows[1].menge, 2);
  assert.match(f.historyText(), /Teileanfrage erfasst/); assert.match(f.historyText(), /Noch keine Bestellung ausgelöst/);
  assert.match(f.historyText(), /<img onerror=alert\(1\)>/);
});

test('uncertain retry freezes inquiry, description and timing with original screenshot', async () => {
  const f = fixture(), previous = f.api.fetch; let first = true;
  f.api.fetch = async (url, settings) => {
    if (settings.method === 'POST' && first) {first = false; f.calls.push({url, settings}); throw new Error('Unterbrochen');}
    return previous(url, settings);
  };
  f.controller.addFiles([photo('screenshot.png', 123, 'image/png')]);
  const text = f.field(0, 'TEXTAREA'), inquiry = f.field(0, 'INPUT', el => el.type === 'checkbox');
  text.value = 'Halter links'; text.fire('input'); inquiry.checked = true; inquiry.fire('change');
  await f.controller.submit();
  assert.equal(text.disabled, true); assert.equal(inquiry.disabled, true);
  text.value = 'Anderer Artikel'; text.fire('input'); inquiry.checked = false; inquiry.fire('change');
  const urgent = f.field(0, 'INPUT', el => el.value === 'dringend'); urgent.checked = true; urgent.fire('change');
  await f.controller.submit();
  assert.equal(f.posts()[0].settings.body.fields.get('positionen'), f.posts()[1].settings.body.fields.get('positionen'));
});

test('oversized focused description is not silently truncated or sent', async () => {
  const f = fixture(); f.controller.addFiles([photo()]);
  const description = f.field(0, 'TEXTAREA'); description.value = 'x'.repeat(501);
  await f.controller.submit(); assert.equal(f.posts().length, 0); assert.equal(description.focused, true);
  assert.match(f.$('status').textContent, /500 Zeichen/);
});

test('an invalid focused quantity stops submission instead of sending an old value', async () => {
  const f = fixture(); f.controller.addFiles([photo()]); f.quantity(0).value = '0';
  await f.controller.submit(); assert.equal(f.posts().length, 0); assert.equal(f.quantity(0).focused, true);
});

test('HEIC, nonimage, empty, oversized and aggregate limits are rejected before upload', async () => {
  const f = fixture();
  for (const file of [photo('iphone.heic', 100, 'image/heic'), photo('note.pdf', 100, 'application/pdf'), photo('empty.jpg', 0), photo('large.jpg', 8 * 1024 * 1024 + 1)]) {
    f.controller.addFiles([file]); assert.equal(f.cards().length, 0); assert.equal(f.$('submit').disabled, true);
  }
  f.controller.addFiles([photo('iphone.HEIC', 100, '')]); assert.match(f.$('status').textContent, /HEIC.*JPEG/);
  f.controller.addFiles(Array.from({length: 11}, () => photo())); assert.match(f.$('status').textContent, /10 Fotos/);
  f.controller.addFiles(Array.from({length: 7}, () => photo('big.jpg', 8 * 1024 * 1024))); assert.match(f.$('status').textContent, /50 MB/);
  assert.equal(f.cards().length, 0); await f.controller.submit(); assert.equal(f.posts().length, 0);
  f.controller.addFiles(Array.from({length: 10}, () => photo())); assert.equal(f.cards().length, 10);
  assert.equal(f.$('camera-button').disabled, true); assert.equal(f.$('album-button').disabled, true);
});

test('remove before submit revokes only that preview and binds remaining article', async () => {
  const f = fixture(); f.controller.addFiles([photo('first.jpg'), photo('second.jpg')]);
  f.field(0, 'BUTTON', el => el.textContent === 'Entfernen').click();
  assert.equal(f.cards().length, 1); assert.equal(f.revoked.length, 1);
  await f.controller.submit(); const rows = JSON.parse(f.posts()[0].settings.body.fields.get('positionen'));
  assert.equal(rows.length, 1); assert.equal(f.posts()[0].settings.body.fields.get('foto_' + rows[0].id).name, 'second.jpg');
});

test('double click makes one request; frozen controls cannot mutate a pending batch', async () => {
  const f = fixture(), gate = deferred(), previous = f.api.fetch;
  f.api.fetch = async (url, settings) => {if (settings.method === 'POST') {f.calls.push({url, settings}); return gate.promise;} return previous(url, settings);};
  f.controller.addFiles([photo()]); const first = f.controller.submit(); void f.controller.submit();
  f.step(0, '+').click(); f.quantity(0).value = '8'; f.quantity(0).fire('change'); f.controller.addFiles([photo('extra.jpg')]);
  f.field(0, 'BUTTON', el => el.textContent === 'Entfernen').click();
  assert.equal(f.posts().length, 1); assert.equal(f.quantity(0).value, '1'); assert.equal(f.cards().length, 1);
  assert.equal(f.$('camera-button').disabled, true); assert.equal(f.$('submit').disabled, true);
  gate.resolve(reply({error: 'Abgelehnt', accepted: false}, 400)); await first;
  assert.equal(f.$('camera-button').disabled, false); assert.equal(f.$('recovery').hidden, true);
});

test('timeout freezes exact data and retry repeats same batch, client ids and file objects', async () => {
  const f = fixture(), previous = f.api.fetch; let first = true;
  f.api.fetch = async (url, settings) => {
    if (settings.method === 'POST' && first) {first = false; f.calls.push({url, settings}); return new Promise(() => {});}
    return previous(url, settings);
  };
  f.controller.addFiles([photo()]); f.step(0, '+').click();
  const attempt = f.controller.submit(); f.fireTimers(); await attempt;
  assert.match(f.$('status').textContent, /unklar/); assert.equal(f.$('camera-button').disabled, true);
  assert.equal(f.$('submit').disabled, false); assert.equal(f.step(0, '+').disabled, true); assert.ok(f.store.has(f.key));
  f.step(0, '+').click(); assert.equal(f.quantity(0).value, '2');
  await f.controller.submit();
  const [a, b] = f.posts().map(call => call.settings.body.fields);
  assert.equal(a.get('request_id'), b.get('request_id')); assert.equal(a.get('positionen'), b.get('positionen'));
  for (const [key, value] of a) if (key.startsWith('foto_')) assert.equal(value, b.get(key));
  assert.equal(f.store.has(f.key), false); assert.equal(f.$('camera-button').disabled, false);
  f.controller.addFiles([photo('next.jpg')]); await f.controller.submit();
  assert.notEqual(f.posts()[2].settings.body.fields.get('request_id'), a.get('request_id'));
});

test('server 500 or malformed acknowledgement keeps same replay id and no edit escape', async () => {
  for (const response of [reply({error: 'Serverfehler'}, 500), reply({anforderungen: [], request_id: 'wrong'})]) {
    const f = fixture(); f.api.fetch = async (url, settings) => {f.calls.push({url, settings}); return response;};
    f.controller.addFiles([photo()]); await f.controller.submit(); await f.controller.submit();
    assert.equal(f.$('camera-button').disabled, true); assert.match(f.$('status').textContent, /unklar/);
    assert.equal(f.posts()[0].settings.body.fields.get('request_id'), f.posts()[1].settings.body.fields.get('request_id'));
  }
});

test('safe rejected validation unlocks editable photos and discards unsuccessful batch id', async () => {
  const f = fixture(), previous = f.api.fetch; let first = true;
  f.api.fetch = async (url, settings) => {
    if (first && settings.method === 'POST') {first = false; f.calls.push({url, settings}); return reply({error: 'Foto ungültig.', accepted: false}, 400);}
    return previous(url, settings);
  };
  f.controller.addFiles([photo()]); await f.controller.submit(); f.step(0, '+').click(); await f.controller.submit();
  assert.notEqual(f.posts()[0].settings.body.fields.get('request_id'), f.posts()[1].settings.body.fields.get('request_id'));
  assert.equal(JSON.parse(f.posts()[1].settings.body.fields.get('positionen'))[0].menge, 2);
});

test('changed login context blocks old page, preserving original uncertain request', async () => {
  const f = fixture(); let calls = 0;
  f.api.fetch = async (url, settings) => {
    f.calls.push({url, settings}); calls++;
    if (calls === 1) throw new Error('Keine Bestätigung');
    return reply({error: 'Sicherheitsprüfung fehlgeschlagen.', accepted: false, reload_required: true}, 403);
  };
  f.controller.addFiles([photo()]); await f.controller.submit(); const marker = f.store.get(f.key);
  await f.controller.submit(); assert.equal(f.store.get(f.key), marker);
  assert.equal(f.$('submit').disabled, true); assert.equal(f.$('reload').hidden, false);
  await f.controller.refresh(); await f.controller.submit(); assert.equal(calls, 2);
  assert.match(f.$('recovery-text').textContent, /ursprünglichen persönlichen Zugang/);
});

test('old login context cannot fetch and render another employees history', async () => {
  const f = fixture(); f.api.fetch = async (url, settings) => {f.calls.push({url, settings}); return reply({error: 'Seite neu laden.', accepted: false}, 403);};
  await f.controller.refresh(); assert.equal(f.$('history').children.length, 0);
  assert.equal(f.$('camera-button').disabled, true); assert.equal(f.$('reload').hidden, false);
  assert.equal(f.calls[0].settings.headers['X-CSRF-Token'], 'synthetic-csrf');
  f.controller.addFiles([photo()]); await f.controller.submit(); assert.equal(f.posts().length, 0);
});

test('reload uses own history and unlocks only after complete original batch is evidenced', async () => {
  const saved = {id: 'batch-original', clientIds: ['photo-one', 'photo-two']}, f = fixture({saved});
  let rows = [{request_id: 'another-batch', client_id: 'photo-one', id: 1, state: 'review'}];
  f.api.fetch = async (url, settings) => {f.calls.push({url, settings}); return reply({anforderungen: rows});};
  await f.controller.refresh(); assert.equal(f.$('camera-button').disabled, true); assert.equal(f.$('submit').disabled, true);
  rows = [{request_id: saved.id, client_id: 'photo-one', id: 1, state: 'review'}]; await f.controller.refresh();
  assert.equal(f.$('camera-button').disabled, true);
  rows.push({request_id: saved.id, client_id: 'photo-two', id: 2, state: 'review'}); await f.controller.refresh();
  assert.equal(f.$('camera-button').disabled, false); assert.equal(f.store.has(f.key), false);
  assert.ok(f.calls.every(call => call.url.startsWith('/werkstatt/materialbestellung/anforderungen') && !call.settings.method));
  assert.ok(f.calls.some(call => call.url.endsWith('?request_id=batch-original')));
});

test('reload verifies exact batch even when ordinary history is capped', async () => {
  const saved = {id: 'older-batch', clientIds: ['older-photo']}, f = fixture({saved});
  f.api.fetch = async (url, settings) => {f.calls.push({url, settings}); return reply({anforderungen: url.includes('?request_id=')
    ? [{request_id: saved.id, client_id: 'older-photo', id: 80, state: 'review'}]
    : [{request_id: 'newer', client_id: 'newer-photo', id: 150, state: 'review'}]});};
  await f.controller.refresh(); assert.equal(f.$('camera-button').disabled, false);
  assert.equal(f.$('history').children.length, 2); assert.equal(f.store.has(f.key), false);
});

test('history and filenames are literal text; actual sent state is distinguished from capture', async () => {
  const f = fixture(), malicious = '<img src=x onerror=alert(1)>';
  f.controller.addFiles([photo(malicious)]); assert.equal(f.field(0, 'P').textContent, malicious);
  f.api.fetch = async () => reply({anforderungen: [
    {id: 1, code: 'M-1', product: malicious, quantity: '1', state: 'review', label: malicious},
    {id: 2, code: 'M-2', product: 'Test', quantity: '2', state: 'external_sent', label: 'Versandt'},
  ]});
  await f.controller.refresh(); assert.match(f.historyText(), /Noch nicht bestellt/); assert.match(f.historyText(), /Bestellung versandt/);
  assert.ok(descendants(f.$('history')).some(el => el.textContent === malicious));
  assert.doesNotMatch(fs.readFileSync(path.join(__dirname, '../static/materialbestellung.js'), 'utf8'), /\.innerHTML\s*=/);
});

test('pending recognition reports capture and no premature employee clarification', async () => {
  const f = fixture(); f.api.fetch = async () => reply({anforderungen: [{id: 1, state: 'open',
    analysis_state: 'pending', label: 'Foto wird ausgelesen', employee_reply_required: true, questions: []}]});
  await f.controller.refresh(); assert.match(f.historyText(), /Foto wird ausgelesen.*Noch nicht bestellt/);
  assert.doesNotMatch(f.historyText(), /Klärung ist nötig/);
  assert.equal(descendants(f.$('history')).filter(el => el.tagName === 'FORM').length, 0);
});

test('fresh dispatch state takes precedence over material handover state', async () => {
  const f = fixture(); f.api.fetch = async () => reply({anforderungen: [
    {id: 1, state: 'accepted', dispatch_state: 'sent', label: 'Bestellt'},
    {id: 2, state: 'accepted', dispatch_state: 'copy_pending', label: 'Bestellt'},
    {id: 3, state: 'accepted', dispatch_state: 'uncertain', label: 'Versand unklar'},
    {id: 4, state: 'accepted', dispatch_state: 'partial', label: 'Versand unklar'},
    {id: 5, state: 'accepted', dispatch_state: 'queued', label: 'Sammelbestellung Montag 14 Uhr'},
    {id: 6, state: 'accepted', dispatch_state: 'blocked', label: 'Bestellversand gesperrt'},
  ]});
  await f.controller.refresh();
  const [sent, copy, uncertain, partial, queued, blocked] = f.$('history').children.map(card => descendants(card).map(el => el.textContent).join(' '));
  assert.match(sent, /Bestellung versandt/); assert.doesNotMatch(sent, /noch keinen Versand/);
  assert.match(copy, /Bestellung versandt/); assert.match(uncertain, /Bitte nicht erneut bestellen/);
  assert.match(partial, /Bitte nicht erneut bestellen/); assert.match(queued, /Noch nicht versandt/);
  assert.match(blocked, /Werkstattleitung prüft/);
});

test('duplicate answer stays revision-bound and timeout retry cannot switch to opposite answer', async () => {
  const f = fixture(), row = {id: 8, revision: 4, state: 'open', employee_reply_required: true,
    questions: [{field: 'possible_duplicate', body: 'Zusätzlich benötigt?'}]};
  let first = true;
  f.api.fetch = async (url, settings) => {
    f.calls.push({url, settings});
    if (!settings.method) return reply({anforderungen: [row]});
    if (first) {first = false; throw new Error('Verbindung unterbrochen');}
    return reply({anforderung: {...row, revision: 5, state: 'review', employee_reply_required: false}});
  };
  await f.controller.refresh();
  const buttons = descendants(f.$('history')).filter(el => el.tagName === 'BUTTON');
  buttons[0].click(); await settle();
  assert.equal(buttons[0].disabled, false); assert.equal(buttons[1].disabled, true);
  buttons[1].click(); await settle(); // Synthetic click still fires on disabled controls.
  const sent = f.posts().map(call => JSON.parse(call.settings.body));
  assert.equal(sent[0].antwort, 'ja'); assert.equal(sent[0].revision, 4);
  assert.equal(sent[1].antwort, 'ja'); assert.equal(sent[1].request_id, sent[0].request_id);
  assert.equal(f.posts()[0].url, '/werkstatt/materialbestellung/anforderungen/8/antwort');
});

test('unclear free answer remains visible and frozen when history is refreshed', async () => {
  const f = fixture(), row = {id: 9, revision: 3, state: 'open', employee_reply_required: true,
    analysis_state: 'done', questions: [{field: 'article', body: 'Welche Breite meinst du?'}]};
  let first = true;
  f.api.fetch = async (url, settings) => {
    f.calls.push({url, settings});
    if (!settings.method) return reply({anforderungen: [row]});
    if (first) {first = false; throw new Error('Keine Bestätigung');}
    return reply({anforderung: {...row, state: 'review', revision: 4, employee_reply_required: false}});
  };
  await f.controller.refresh();
  let form = descendants(f.$('history')).find(el => el.tagName === 'FORM');
  let input = descendants(form).find(el => el.tagName === 'INPUT'); input.value = '30 mm'; form.fire('submit'); await settle();
  await f.controller.refresh(); form = descendants(f.$('history')).find(el => el.tagName === 'FORM');
  input = descendants(form).find(el => el.tagName === 'INPUT');
  assert.equal(input.disabled, true); assert.equal(input.value, '30 mm');
  form.fire('submit'); await settle();
  const answers = f.posts().map(call => JSON.parse(call.settings.body));
  assert.deepEqual(answers[1], answers[0]); assert.equal(answers[1].antwort, '30 mm');
});

test('oversized free answer is editable and never reaches backend', async () => {
  const f = fixture(); f.api.fetch = async (url, settings) => {f.calls.push({url, settings}); return reply({anforderungen: [{id: 9,
    revision: 1, state: 'open', analysis_state: 'done', employee_reply_required: true, questions: [{field: 'article', body: 'Artikel?'}]}]});};
  await f.controller.refresh(); const form = descendants(f.$('history')).find(el => el.tagName === 'FORM');
  const input = descendants(form).find(el => el.tagName === 'INPUT');
  assert.equal(input.maxLength, 200); input.value = 'x'.repeat(201); form.fire('submit'); await settle();
  assert.equal(f.posts().length, 0); assert.equal(input.disabled, false); assert.equal(input.focused, true);
  assert.match(f.historyText(), /höchstens 200 Zeichen/);
});

test('template keeps personal login redirect, native camera, album picker and protected branches', () => {
  const html = fs.readFileSync(path.join(__dirname, '../templates/materialbestellung.html'), 'utf8');
  assert.match(html, /csrf_field\(\)/); assert.match(html, /name="next" value="\/werkstatt\/materialbestellung"/);
  assert.match(html, /action="\/werkstatt\/assistent\/login"/); assert.match(html, /capture="environment"/);
  assert.match(html, /id="material-album"[^>]+multiple/); assert.match(html, /elif not can_order/);
  assert.match(html, /Erfasst bedeutet noch nicht bestellt/); assert.doesNotMatch(html, /impersonat/i);
});

test('scanner captures through the same personal photo batch and never supplies code as order data', async () => {
  let bindings, opened = 0, stopped = 0;
  const f = fixture({scannerFactory: settings => {bindings = settings; return {open(){opened++;},stop(){stopped++;}};}});
  f.$('scan-button').click(); assert.equal(opened, 1); assert.equal(bindings.canAdd(), true);
  bindings.onFallback(); assert.equal(f.$('camera').clicked, true);
  const scanned = photo('artikelcode.jpg'); bindings.onPhoto(scanned); f.step(0, '+').click();
  await f.controller.submit();
  const fields = f.posts()[0].settings.body.fields, rows = JSON.parse(fields.get('positionen'));
  assert.equal(rows[0].menge, 2); assert.equal(fields.get('foto_' + rows[0].id), scanned);
  assert.equal('barcode' in rows[0], false); assert.equal('decodedCode' in rows[0], false); assert.ok(stopped > 0);
});

test('scanner is blocked while a prior batch is uncertain or ten pictures are selected', async () => {
  let bindings;
  const f = fixture({scannerFactory: settings => {bindings = settings; return {open(){},stop(){}};}});
  f.controller.addFiles(Array.from({length:10}, () => photo()));
  assert.equal(f.$('scan-button').disabled, true); assert.equal(bindings.canAdd(), false);
  f.api.fetch = async () => {throw new Error('Unklar');}; await f.controller.submit();
  assert.equal(bindings.canAdd(), false); assert.equal(f.$('scan-button').disabled, true);
});

test('server code recognition is literal informational text without a QR link', async () => {
  const f = fixture(); f.api.fetch = async () => reply({anforderungen:[{id:1, state:'review', product:'Test',
    decodedCode:'TEST-QR', code_erkennung:{status:'erkannt'}},{id:2,state:'review',product:'Test2',code_erkennung:{status:'mehrdeutig'}}]});
  await f.controller.refresh(); assert.match(f.historyText(), /Artikelcode erkannt: TEST-QR/);
  assert.match(f.historyText(), /Mehrere Codes/); assert.equal(descendants(f.$('history')).some(el => el.tagName === 'A'), false);
});

test('a rejected camera frame keeps the actual file-limit error instead of claiming success', () => {
  let bindings;
  const f = fixture({scannerFactory: settings => {bindings = settings; return {open(){},stop(){}};}});
  f.controller.addFiles(Array.from({length:6}, () => photo('large.jpg', 8 * 1024 * 1024)));
  bindings.onPhoto(photo('artikelcode.jpg', 3 * 1024 * 1024));
  assert.equal(f.cards().length, 6); assert.match(f.$('status').textContent, /50 MB/);
  assert.doesNotMatch(f.$('status').textContent, /hinzugefügt/);
});

test('urgency tiles explain immediate personal cap and Monday without promising purchases for inquiries', async () => {
  const f = fixture(); f.controller.addFiles([photo()]);
  const cardText = () => descendants(f.cards()[0]).map(el => el.textContent).join(' ');
  assert.match(cardText(), /Nicht dringend/); assert.match(cardText(), /Montag · 14 Uhr/);
  assert.match(cardText(), /Sofort · bis 250 € brutto/);
  const urgent = f.field(0, 'INPUT', el => el.value === 'dringend'); urgent.checked = true; urgent.fire('change');
  assert.match(cardText(), /inklusive Versand und Nebenkosten/);
  const inquiry = f.field(0, 'INPUT', el => el.type === 'checkbox'); inquiry.checked = true; inquiry.fire('change');
  assert.match(cardText(), /Zeitnah intern klären/); assert.doesNotMatch(cardText(), /Sofort · bis/);
  assert.match(cardText(), /wird noch nicht bestellt/); assert.equal(urgent.checked, true);
  inquiry.checked = false; inquiry.fire('change'); assert.match(cardText(), /Sofort · bis 250 € brutto/);
  await f.controller.submit(); assert.equal(JSON.parse(f.posts()[0].settings.body.fields.get('positionen'))[0].dringend, true);
});

test('the urgency label uses the server personal cap and never invents a 250 euro allowance', () => {
  for (const [limitCents, expected] of [['12000', /bis 120 € brutto/], ['24999', /bis 249,99 € brutto/], ['', /Sofort nach Klärung/], ['-1', /Sofort nach Klärung/], ['25001', /Sofort nach Klärung/]]) {
    const f = fixture({limitCents}); f.controller.addFiles([photo()]);
    const text = descendants(f.cards()[0]).map(el => el.textContent).join(' ');
    assert.match(text, expected); assert.doesNotMatch(text, /bis 250 € brutto/);
  }
});
