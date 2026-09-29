'use strict';
// Synthetic DOM and API; no browser, camera, real image upload or purchase call.
const test = require('node:test');
const assert = require('node:assert/strict');
const {createMaterialPhotoController} = require('../static/assistent-materialfoto.js');
class Element {
  constructor(tag = 'div') { this.tagName = tag.toUpperCase(); this.children = []; this.dataset = {}; this.events = {}; this.attributes = {}; this.textContent = ''; this.disabled = false; this.hidden = false; this.open = false; this.files = []; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; this.textContent = ''; }
  setAttribute(name, value) { this.attributes[name] = value; }
  removeAttribute(name) { delete this.attributes[name]; delete this[name]; }
  set innerHTML(value) { throw new Error('HTML injection sink must not be used'); }
  addEventListener(name, fn) { (this.events[name] ||= []).push(fn); }
  fire(name) { for (const fn of this.events[name] || []) fn({preventDefault() {}}); }
  showModal() { this.open = true; }
  close() { this.open = false; this.fire('close'); }
  focus() { this.focused = true; }
  querySelectorAll(selector) { return descendants(this).filter(el => selector === '[data-materialfoto-action]' && el.dataset.materialfotoAction); }
}
const descendants = node => node.children.flatMap(child => [child, ...descendants(child)]);
const settle = async () => { for (let i = 0; i < 8; i++) await new Promise(resolve => setImmediate(resolve)); };
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => {resolve = a; reject = b;}); return {promise, resolve, reject}; };
function fixture() {
  const names = ['dialog', 'open', 'close', 'file', 'form', 'submit', 'preview', 'status', 'results', 'recent', 'history'];
  const elements = Object.fromEntries(names.map(name => ['materialfoto-' + name, new Element()]));
  const $ = name => elements['materialfoto-' + name];
  $('dialog').append(...names.filter(name => !['dialog', 'open'].includes(name)).map($));
  const hit = {id: 'a'.repeat(32), produkt_name: 'Test Abdeckband 30 mm', lieferant: 'Test Supplier',
    artikelnummer: 'SYNTHETIC-30', groesse: '30 mm', ve: 'Stück',
    quelle: {art: 'einkauf', beleg_id: 1, seite: 2},
    packinhalt: {menge: '32', einheit: 'Stück', pro: 'VE'}, bestellbar: false};
  const photo = {id: 'b'.repeat(32), status: 'pruefen', frage: 'Welche Variante passt?', merkmale: {produkt: 'Band', breite: '30 mm'}, treffer: [hit]};
  const calls = [], selected = [], messages = [], revoked = []; let resets = 0, sequence = 0;
  const host = {
    async api(path, data) {
      calls.push({path, data});
      if (path === '/materialfotos' && data === undefined) return {fotos: []};
      if (path === '/materialfotos') return {foto: {...photo, status: 'bereit', treffer: []}};
      if (path.endsWith('/analyse')) return {foto: photo};
      if (path.endsWith('/auswahl')) return {auswahl: hit, frage: 'Artikel gewählt. Keine Bestellung.'};
      return {foto: photo};
    },
    selectMaterial(value) { selected.push(value); }, status(value) {messages.push(value);},
    clearMaterialSelection() {resets++;},
  };
  const url = {createObjectURL: () => 'blob:synthetic-' + ++sequence, revokeObjectURL: value => revoked.push(value)};
  class Data { constructor() {this.fields = new Map();} append(key, value) {this.fields.set(key, value);} }
  const controller = createMaterialPhotoController({host, document: {getElementById: id => elements[id], createElement: tag => new Element(tag)},
    URL: url, crypto: {randomUUID: () => 'synthetic-request-id-' + ++sequence}, FormData: Data});
  const choose = () => { $('file').files = [{name: 'label.png', size: 128, type: 'image/png'}]; $('file').fire('change'); };
  const upload = async () => {controller.open(); await settle(); choose(); $('form').fire('submit'); await settle();};
  const button = label => descendants($('dialog')).find(el => el.tagName === 'BUTTON' && el.textContent === label);
  return {$, controller, host, hit, photo, calls, selected, messages, revoked, choose, upload, button, resets: () => resets};
}
test('explicit variant selection updates context only, preserving units and source', async () => {
  const f = fixture(); await f.upload();
  assert.equal(f.selected.length, 0); assert.equal(f.resets(), 1);
  const text = descendants(f.$('results')).map(el => el.textContent).join(' ');
  assert.match(text, /32 Stück je VE/); assert.match(text, /Einheit laut Beleg: Stück/);
  assert.match(text, /Beleg 1 · Seite 2/); assert.doesNotMatch(text, /Karton/);
  await f.button('Diesen Artikel meine ich').onclick();
  assert.equal(f.selected[0].bestellbar, false); assert.equal(f.$('dialog').open, false);
  assert.ok(f.calls.every(call => call.path.startsWith('/materialfotos')));
  assert.equal(f.calls.filter(call => call.path.endsWith('/auswahl')).length, 1);
  assert.ok(f.revoked.length); assert.equal(f.$('open').focused, true);
});
test('labels are rendered as literal text and missing pack stays unknown', async () => {
  const f = fixture(); f.hit.produkt_name = '<img src=x onerror=alert(1)>'; f.hit.packinhalt = null;
  await f.upload();
  assert.ok(descendants(f.$('results')).some(el => el.textContent === f.hit.produkt_name));
  assert.ok(descendants(f.$('results')).some(el => el.textContent === 'Packinhalt nicht belegt.'));
});
test('double submit is single and closing discards delayed recognition', async () => {
  const f = fixture(), gate = deferred(), previous = f.host.api;
  f.host.api = async (path, data) => path.endsWith('/analyse') ? gate.promise : previous(path, data);
  f.controller.open(); await settle(); f.choose();
  f.$('form').fire('submit'); f.$('form').fire('submit'); await settle();
  assert.equal(f.calls.filter(call => call.path === '/materialfotos' && call.data).length, 1);
  assert.equal(f.$('file').disabled, true);
  f.controller.close(); gate.resolve({foto: f.photo}); await settle();
  assert.equal(f.$('results').children.length, 0); assert.equal(f.selected.length, 0);
  assert.equal(f.$('file').disabled, false);
});
test('ambiguous upload failure retries same request id and never chooses implicitly', async () => {
  const f = fixture(), previous = f.host.api; let first = true; const ids = [];
  f.host.api = async (path, data) => {
    if (path === '/materialfotos' && data) {
      ids.push(data.fields.get('request_id'));
      if (first) {first = false; throw new Error('Verbindung unterbrochen');}
    }
    return previous(path, data);
  };
  await f.upload(); assert.match(f.$('status').textContent, /Verbindung unterbrochen/);
  f.$('form').fire('submit'); await settle();
  assert.deepEqual(ids, [ids[0], ids[0]]); assert.equal(f.selected.length, 0);
});
test('revoked or failed selection stays open, reports error and cannot update context', async () => {
  const f = fixture(); await f.upload(); const previous = f.host.api;
  f.host.api = (path, data) => path.endsWith('/auswahl') ? Promise.reject(new Error('Artikeltreffer nicht mehr verfügbar.')) : previous(path, data);
  await f.button('Diesen Artikel meine ich').onclick();
  assert.equal(f.$('dialog').open, true); assert.equal(f.selected.length, 0);
  assert.match(f.$('status').textContent, /nicht mehr verfügbar/);
});
test('delayed selection cannot change context after dialog closes', async () => {
  const f = fixture(); await f.upload(); const gate = deferred(), previous = f.host.api;
  f.host.api = (path, data) => path.endsWith('/auswahl') ? gate.promise : previous(path, data);
  const action = f.button('Diesen Artikel meine ich').onclick(); f.controller.close();
  gate.resolve({auswahl: f.hit}); await action;
  assert.equal(f.selected.length, 0);
});
test('oversized image is rejected locally before any upload', async () => {
  const f = fixture(); f.controller.open(); await settle();
  f.$('file').files = [{size: 8 * 1024 * 1024 + 1}]; f.$('file').fire('change');
  f.$('form').fire('submit'); await settle();
  assert.equal(f.$('submit').disabled, true); assert.match(f.$('status').textContent, /8 MB/);
  assert.equal(f.calls.filter(call => call.data).length, 0);
});
test('catalog outage is retryable and is never presented as an absent article', async () => {
  const f = fixture(); f.photo.artikelsuche_verfuegbar = false; f.photo.treffer = [];
  f.photo.frage = 'Artikelsuche derzeit nicht verfügbar.'; await f.upload();
  assert.ok(f.button('Artikelsuche erneut prüfen'));
  assert.doesNotMatch(descendants(f.$('results')).map(el => el.textContent).join(' '), /Es wurde nichts zugeordnet/);
  assert.equal(f.selected.length, 0);
});
