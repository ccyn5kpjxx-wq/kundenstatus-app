// Run the actual search script against a tiny DOM and controlled fetch replies.
// No browser, app import, network, dependencies or real timers are used.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const SOURCE = path.join(__dirname, '../static/homepage-reifen-suche.js');
const START = Date.parse('2026-11-03T10:00:00Z');

function createHarness(initial = {}, {standalone = false} = {}) {
  let now = START, timerId = 0;
  const timers = new Map(), requests = [], fields = new Map(), groups = new Map();
  const document = {activeElement: null};

  class Element {
    constructor(tag = 'div', attributes = {}) {
      this.tagName = tag.toUpperCase();
      this.attributes = {...attributes};
      this.children = [];
      this.parentElement = null;
      this.dataset = {};
      this.handlers = {};
      this.className = attributes.class || '';
      this.name = attributes.name || '';
      this.id = attributes.id || '';
      this.type = attributes.type || '';
      this.value = attributes.value || '';
      this.checked = false;
      this.hidden = false;
      this.disabled = false;
      this.noValidate = false;
      this._text = '';
      this.classList = {
        contains: name => this.className.split(/\s+/).includes(name),
        toggle: (name, force) => {
          const present = this.classList.contains(name);
          const wanted = force === undefined ? !present : !!force;
          const classes = this.className.split(/\s+/).filter(Boolean).filter(item => item !== name);
          if (wanted) classes.push(name);
          this.className = classes.join(' ');
          return wanted;
        },
        add: name => this.classList.toggle(name, true),
        remove: name => this.classList.toggle(name, false),
      };
    }
    get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
    set textContent(value) { this.replaceChildren(); this._text = String(value); }
    append(...children) {
      children.forEach(child => {
        if (typeof child !== 'object') { const text = new Element('span'); text.textContent = child; child = text; }
        child.parentElement = this;
        this.children.push(child);
      });
    }
    appendChild(child) { this.append(child); return child; }
    replaceChildren(...children) {
      this.children.forEach(child => { child.parentElement = null; });
      this.children = [];
      this._text = '';
      this.append(...children);
    }
    setAttribute(name, value) { this.attributes[name] = String(value); }
    getAttribute(name) { return name in this.attributes ? this.attributes[name] : null; }
    hasAttribute(name) { return name in this.attributes; }
    removeAttribute(name) { delete this.attributes[name]; }
    focus() { document.activeElement = this; }
    scrollIntoView() {}
    addEventListener(type, handler) { (this.handlers[type] ||= []).push(handler); }
    async dispatch(type) {
      const event = {target: this, currentTarget: this, defaultPrevented: false,
        preventDefault() { this.defaultPrevented = true; }};
      for (const handler of this.handlers[type] || []) await handler(event);
      return event;
    }
    click() { return this.disabled ? Promise.resolve() : this.dispatch('click'); }
    matches(selector) {
      if (selector.includes(',')) return selector.split(',').some(part => this.matches(part.trim()));
      let rest = selector.trim();
      const checked = rest.endsWith(':checked');
      if (checked) rest = rest.slice(0, -8);
      const tag = rest.match(/^[a-z][\w-]*/i);
      if (tag) { if (this.tagName !== tag[0].toUpperCase()) return false; rest = rest.slice(tag[0].length); }
      const tokens = rest.match(/#[\w-]+|\.[\w-]+|\[[\w-]+(?:="[^"]*")?\]/g) || [];
      if (tokens.join('') !== rest) throw new Error(`Unsupported fake DOM selector: ${selector}`);
      return (!checked || this.checked) && tokens.every(token => {
        if (token[0] === '#') return this.id === token.slice(1);
        if (token[0] === '.') return this.classList.contains(token.slice(1));
        const [, name, value] = token.match(/^\[([\w-]+)(?:="([^"]*)")?\]$/);
        const actual = ['name', 'id', 'type'].includes(name) ? this[name] : this.getAttribute(name);
        return value === undefined ? actual !== null : actual === value;
      });
    }
    querySelectorAll(selector) {
      return this.children.flatMap(child => [child, ...child.querySelectorAll('*')])
        .filter(child => selector === '*' || child.matches(selector));
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
    closest(selector) { return this.matches(selector) ? this : this.parentElement?.closest(selector) || null; }
  }

  const form = new Element('form');
  function addField(name, value = '', type = 'text', checked = false) {
    const group = groups.get(name) || new Element(type === 'radio' ? 'fieldset' : 'div', {class: 'field'});
    const tag = ['reifen_qualitaet', 'reifen_anzahl'].includes(name) ? 'select' : 'input';
    const field = new Element(tag, {name, id: type === 'radio' ? `${name}_${value}` : name, type, value});
    field.checked = checked;
    group.append(field);
    if (!groups.has(name)) { groups.set(name, group); form.append(group); }
    if (!fields.has(name)) fields.set(name, []);
    fields.get(name).push(field);
    return field;
  }
  addField('reifen_groesse_vorne', '205/55 R16 91V');
  addField('reifen_groesse_hinten');
  ['sommer', 'winter', 'ganzjahr'].forEach(value => addField('reifen_saison', value, 'radio', value === 'winter'));
  addField('reifen_qualitaet', 'premium');
  addField('reifen_anzahl', '4');
  ['ja', 'nein'].forEach(value => addField('reifen_montage', value, 'radio', value === 'ja'));
  addField('reifen_produktwunsch');
  fields.get('reifen_groesse_vorne')[0].setAttribute('aria-describedby', 'reifen-groesse-hinweis');
  const panel = new Element('section', {'data-tire-search': ''});
  if (standalone) panel.setAttribute('data-standalone-shop', '');
  panel.dataset.searchUrl = '/api/reifenangebote/suche';
  panel.hidden = true;
  const button = new Element('button', {'data-tire-search-button': ''});
  const status = new Element('p', {'data-tire-search-status': ''});
  const results = new Element('div', {'data-tire-search-results': ''});
  panel.append(button, status, results); form.append(panel);
  document.querySelector = selector => form.matches(selector) ? form : form.querySelector(selector);
  document.querySelectorAll = selector => form.querySelectorAll(selector);
  document.getElementById = id => form.querySelector(`#${id}`);
  document.createElement = tag => new Element(tag);

  function set(name, value) {
    const entries = fields.get(name);
    if (!entries) throw new Error(`Unknown fixture field: ${name}`);
    entries.forEach(field => {
      if (field.type === 'radio') field.checked = field.value === value;
      else field.value = String(value);
    });
  }
  Object.entries(initial).forEach(([name, value]) => set(name, value));
  class FormData {
    constructor(target) { this.form = target; }
    get(name) {
      const entry = this.form.querySelectorAll(`[name="${name}"]`)
        .find(field => field.type !== 'radio' || field.checked);
      return entry ? entry.value : null;
    }
  }
  class ControlledDate extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }
    static now() { return now; }
  }
  function fetch(url, options) {
    return new Promise((resolve, reject) => {
      requests.push({url: new URL(String(url)), options,
        respond(payload, code = 200, headers = {}) {
          resolve({status: code, ok: code >= 200 && code < 300, json: async () => payload,
            headers: {get: name => Object.entries(headers).find(([key]) => key.toLowerCase() === name.toLowerCase())?.[1] || null}});
        },
        reject,
      });
    });
  }
  const window = {fetch, AbortController, location: {origin: 'https://reifen.example.invalid'},
    setTimeout(callback, delay) { const id = ++timerId; timers.set(id, {callback, at: now + delay}); return id; },
    clearTimeout(id) { timers.delete(id); },
    setInterval(callback, delay) { const id = ++timerId; timers.set(id, {callback, at: now + delay, interval: delay}); return id; },
    clearInterval(id) { timers.delete(id); },
  };
  vm.runInNewContext(fs.readFileSync(SOURCE, 'utf8'), {
    document, window, fetch, FormData, AbortController, URL, URLSearchParams,
    Intl, Date: ControlledDate, console,
  }, {filename: SOURCE});
  return {document, form, panel, button, status, results, requests, fields, set,
    field: name => fields.get(name)[0],
    search: () => button.dispatch('click'),
    async change(name, value) { set(name, value); await fields.get(name)[0].dispatch('change'); },
    async advance(milliseconds) {
      const target = now + milliseconds;
      let next;
      while ((next = [...timers].filter(([, timer]) => timer.at <= target).sort((a, b) => a[1].at - b[1].at)[0])) {
        const [id, timer] = next;
        now = timer.at;
        if (timer.interval) timer.at += timer.interval;
        else timers.delete(id);
        await timer.callback();
      }
      now = target;
    },
  };
}

function syntheticProduct(changes = {}) {
  return {artikel_id: 'offline-222', bezeichnung: 'Synthetischer Testreifen', groesse: '205/55 R16 91V',
    saison: 'winter', qualitaet: 'premium', lieferzeit: 'Synthetische Angabe',
    preisstand: new Date(START - 1000).toISOString(), gueltig_bis: new Date(START + 600000).toISOString(),
    bestell_url: '/reifen/bestellen/offline-token',
    preis: {einzel_brutto: '100.00', gesamt_reifen_brutto: '400.00', versand_brutto: '0.00', montage_brutto: '80.00', gesamt_brutto: '480.00', waehrung: 'EUR'},
    ...changes};
}

const noOffers = {status: 'keine_treffer', code: 'keine_treffer', verfuegbar: false, produkte: []};
const offers = product => ({status: 'ok', verfuegbar: true, produkte: [product || syntheticProduct()]});
const heading = h => h.results.querySelector('h3')?.textContent || '';
const action = (h, text) => {
  const control = h.results.querySelectorAll('button').find(item => item.textContent === text);
  assert.ok(control, `Visible action exists: ${text}`);
  return control;
};
const flush = async () => { for (let i = 0; i < 6; i += 1) await Promise.resolve(); };
async function reply(h, pending, payload, code = 200, headers = {}) {
  h.requests.at(-1).respond(payload, code, headers);
  await pending;
  await flush();
}

const cases = [
  ['missing size and season identify only their fields without fetching', async () => {
    for (const [initial, name] of [[{reifen_groesse_vorne: ''}, 'reifen_groesse_vorne'], [{reifen_saison: ''}, 'reifen_saison']]) {
      const h = createHarness(initial);
      await h.search();
      assert.equal(h.requests.length, 0);
      assert.equal(h.field(name).getAttribute('aria-invalid'), 'true');
      assert.equal(h.document.activeElement.name, name);
      assert.ok(h.field(name).getAttribute('aria-describedby').includes(`tire-error-${name}`));
      const other = name === 'reifen_saison' ? 'reifen_groesse_vorne' : 'reifen_saison';
      assert.equal(h.field(other).getAttribute('aria-invalid'), null);
    }
  }],
  ['incomplete dimensions and invalid rim or speed index are rejected locally', async () => {
    for (const [size, message] of [['225/40',/Zollgröße fehlt/], ['225/40 R1',/Zollgröße ist unvollständig/],
                                 ['225/40 R18 92Z',/Last- und Geschwindigkeitsindex/], ['225/40 R18 Z8',/Last- und Geschwindigkeitsindex/]]) {
      const h = createHarness({reifen_groesse_vorne: size});
      await h.search();
      assert.equal(h.requests.length, 0, `Invalid size must not use the search budget: ${size}`);
      assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-invalid'), 'true');
      assert.equal(h.document.activeElement.name, 'reifen_groesse_vorne');
      assert.equal(h.field('reifen_saison').getAttribute('aria-invalid'), null);
      assert.match(h.form.querySelector('#tire-error-reifen_groesse_vorne').textContent,message);
    }
  }],
  ['both unambiguous size shortcuts each start one search', async () => {
    for (const size of ['2254018', '225 40 18']) {
      const h = createHarness({reifen_groesse_vorne: size});
      const pending = h.search();
      assert.equal(h.requests.length, 1, `Valid size shortcut must reach the backend: ${size}`);
      assert.equal(h.requests[0].url.searchParams.get('groesse'), size);
      assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-invalid'), null);
      await reply(h, pending, noOffers);
      assert.match(heading(h), /Keine passenden Angebote/);
    }
  }],
  ['equivalent front and rear size spellings start exactly one search', async () => {
    const h = createHarness({reifen_groesse_vorne: '2254018', reifen_groesse_hinten: '225/40R18'});
    const pending = h.search();
    assert.equal(h.requests.length, 1);
    assert.equal(h.requests[0].url.searchParams.get('groesse'), '2254018');
    assert.equal(h.field('reifen_groesse_hinten').value, '225/40R18');
    assert.equal(h.field('reifen_groesse_hinten').getAttribute('aria-invalid'), null);
    await reply(h, pending, noOffers);
  }],
  ['different axle load or speed indices require two executions without fetching', async () => {
    for (const rear of ['225/40R18 95V', '225/40R18 92W']) {
      const h = createHarness({reifen_groesse_vorne: '225/40R18 92V', reifen_groesse_hinten: rear});
      await h.search();
      assert.equal(h.requests.length, 0, 'An axle index must never be silently dropped');
      assert.match(heading(h), /Zwei Reifenausführungen/);
      assert.match(h.results.textContent, /Last- und Geschwindigkeitsindizes/);
      assert.equal(h.field('reifen_groesse_vorne').value, '225/40R18 92V');
      assert.equal(h.field('reifen_groesse_hinten').value, rear);
    }
  }],
  ['invalid optional rear size is marked locally and clearing it permits search', async () => {
    const h = createHarness({reifen_groesse_hinten: '225/40'});
    await h.search();
    assert.equal(h.requests.length, 0);
    assert.equal(h.field('reifen_groesse_hinten').getAttribute('aria-invalid'), 'true');
    assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-invalid'), null);
    assert.equal(h.document.activeElement.name, 'reifen_groesse_hinten');
    await h.change('reifen_groesse_hinten', '');
    assert.equal(h.field('reifen_groesse_hinten').getAttribute('aria-invalid'), null);
    await reply(h, h.search(), noOffers);
    assert.equal(h.requests.length, 1);
  }],
  ['editing and retry preserve existing help and remove stale error attributes', async () => {
    const h = createHarness({reifen_groesse_vorne: '', reifen_saison: ''});
    await h.search();
    await h.search();
    assert.equal(h.form.querySelectorAll('#tire-error-reifen_groesse_vorne').length, 1);
    assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-describedby').split('tire-error-').length, 2);
    await h.change('reifen_groesse_vorne', '225/40 R18 92V');
    await h.change('reifen_saison', 'sommer');
    assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-invalid'), null);
    assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-describedby'), 'reifen-groesse-hinweis');
    h.fields.get('reifen_saison').forEach(field => {
      assert.equal(field.getAttribute('aria-invalid'), null);
      assert.equal(field.getAttribute('aria-describedby'), null);
    });
    const pending = h.search();
    await reply(h, pending, noOffers);
    assert.match(heading(h), /Keine passenden Angebote/);
  }],
  ['400 marks the server-selected field and never displays provider text', async () => {
    const h = createHarness();
    const pending = h.search();
    await reply(h, pending, {feld: 'saison', nachricht: 'SECRET provider internals <script>'}, 400);
    h.fields.get('reifen_saison').forEach(field => assert.equal(field.getAttribute('aria-invalid'), 'true'));
    assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-invalid'), null);
    assert.equal(h.document.activeElement.name, 'reifen_saison');
    assert.doesNotMatch(h.form.textContent, /SECRET|<script>/);
    const again = h.search();
    await reply(h, again, {feld: '__proto__', nachricht: 'SECRET'}, 400);
    assert.equal(h.field('reifen_saison').getAttribute('aria-invalid'), null);
    assert.match(heading(h), /Sucheingaben/);
  }],
  ['no matches, expired catalogue and network failure give distinct guidance', async () => {
    const empty = createHarness();
    await reply(empty, empty.search(), noOffers);
    assert.match(heading(empty), /Keine passenden Angebote/);
    assert.equal(empty.results.querySelectorAll('button').length, 3);
    const stale = createHarness();
    await reply(stale, stale.search(), {code: 'katalog_veraltet', verfuegbar: false, produkte: []}, 503);
    assert.match(heading(stale), /Aktuelle Reifenpreise fehlen/);
    assert.match(stale.results.textContent, /Eingabe ist in Ordnung/);
    assert.doesNotMatch(stale.results.textContent, /Keine passenden Angebote/);
    const offline = createHarness();
    const pending = offline.search();
    offline.requests[0].reject(new Error('SECRET network details'));
    await pending;
    assert.match(heading(offline), /Verbindung.*unterbrochen/);
    assert.doesNotMatch(offline.results.textContent, /SECRET|Keine passenden Angebote/);
    for (const h of [empty, stale, offline]) assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-invalid'), null);
  }],
  ['explicit all-brands retry keeps dimensions and load/speed index', async () => {
    const h = createHarness({reifen_groesse_vorne: '225/40 R18 92V'});
    await reply(h, h.search(), noOffers);
    assert.equal(h.requests.length, 1, 'No automatic broadening after no matches');
    await action(h, 'Alle Marken anzeigen').click();
    assert.equal(h.requests.length, 2);
    const query = h.requests[1].url.searchParams;
    assert.equal(query.get('groesse'), '225/40 R18 92V');
    assert.equal(query.get('qualitaet'), 'alle');
    assert.equal(query.get('montage'), 'ja');
    assert.equal(query.get('saison'), 'winter');
    assert.equal(query.get('anzahl'), '4');
    h.requests[1].respond(offers(syntheticProduct({qualitaet: null, groesse: '225/40 R18 92V'})));
    await flush();
    assert.equal(heading(h), 'Synthetischer Testreifen');
    assert.doesNotMatch(h.results.textContent, /undefined|null|Premium/);
  }],
  ['explicit tires-only retry changes only mounting', async () => {
    const h = createHarness({reifen_groesse_vorne: '225/40 R18 92V'});
    await reply(h, h.search(), {code: 'montage_nicht_online', verfuegbar: false, produkte: []});
    assert.match(heading(h), /Montageumfang/);
    assert.equal(h.requests.length, 1);
    await action(h, 'Reifen ohne Montage anzeigen').click();
    const query = h.requests[1].url.searchParams;
    assert.equal(query.get('montage'), 'nein');
    assert.equal(query.get('groesse'), '225/40 R18 92V');
    assert.equal(query.get('qualitaet'), 'premium');
    assert.equal(query.get('saison'), 'winter');
    assert.equal(query.get('anzahl'), '4');
    h.requests[1].respond(noOffers);
    await flush();
  }],
  ['late response after a filter change cannot overwrite the new search', async () => {
    const h = createHarness();
    const old = h.search();
    await h.change('reifen_qualitaet', 'alle');
    assert.equal(h.requests[0].options.signal.aborted, true);
    const current = h.search();
    h.requests[0].respond({feld: 'groesse'}, 400);
    await old;
    assert.equal(h.button.disabled, true, 'Old finally must not unlock a newer request');
    assert.equal(h.field('reifen_groesse_vorne').getAttribute('aria-invalid'), null);
    await reply(h, current, offers());
    assert.equal(heading(h), 'Synthetischer Testreifen');
    assert.equal(h.button.disabled, false);
    assert.equal(h.panel.hasAttribute('aria-busy'), false);
  }],
  ['429 cooldown survives filter changes and does not trigger hidden retries', async () => {
    const h = createHarness();
    await reply(h, h.search(), {code: 'zu_viele_anfragen'}, 429, {'Retry-After': '3'});
    assert.match(heading(h), /kurz warten/);
    assert.equal(h.button.disabled, true);
    await h.change('reifen_qualitaet', 'alle');
    await h.search();
    assert.equal(h.requests.length, 1);
    await h.advance(3000);
    assert.equal(h.button.disabled, false);
    assert.equal(h.requests.length, 1);
    await reply(h, h.search(), noOffers);
    assert.equal(h.requests.length, 2);
  }],
  ['expired displayed prices disappear and cannot be selected from a retained link', async () => {
    const h = createHarness();
    await reply(h, h.search(), offers(syntheticProduct({gueltig_bis: new Date(START + 2000).toISOString()})));
    const choose = h.results.querySelector('.tire-result-select');
    assert.ok(choose);
    await h.advance(2000);
    assert.equal(h.results.querySelectorAll('article').length, 0);
    assert.match(heading(h), /Preise.*abgelaufen/);
    assert.equal((await choose.click()).defaultPrevented, true);
    assert.equal(h.requests.length, 1);
  }],
  ['foreign or script checkout URLs are rejected and product names stay literal text', async () => {
    for (const bestell_url of ['https://evil.invalid/reifen/bestellen/token', 'javascript:alert(1)']) {
      const h = createHarness();
      await reply(h, h.search(), offers(syntheticProduct({bestell_url})));
      assert.equal(h.results.querySelectorAll('article').length, 0);
      assert.equal(h.results.querySelectorAll('a').length, 0);
      assert.match(heading(h), /bestätigter Preis/);
    }
    const safe = createHarness();
    const text = '<img src=x onerror="synthetic">';
    await reply(safe, safe.search(), offers(syntheticProduct({bezeichnung: text})));
    assert.equal(heading(safe), text);
    assert.equal(safe.results.querySelectorAll('img').length, 0);
  }],
  ['timeout differs from an ordinary network error', async () => {
    const h = createHarness();
    const pending = h.search();
    await h.advance(12000);
    assert.equal(h.requests[0].options.signal.aborted, true);
    h.requests[0].reject(new Error('Synthetic abort'));
    await pending;
    assert.match(heading(h), /dauert.*lange/);
    assert.doesNotMatch(heading(h), /Verbindung/);
  }],
  ['only the standalone shop intercepts submission and disables native validation', async () => {
    const requestForm = createHarness();
    assert.equal(requestForm.form.noValidate, false);
    assert.equal(requestForm.form.handlers.submit, undefined);
    const shop = createHarness({}, {standalone: true});
    assert.equal(shop.form.noValidate, true);
    const submit = await shop.form.dispatch('submit');
    assert.equal(submit.defaultPrevented, true);
    assert.equal(shop.requests.length, 1);
    shop.requests[0].respond(noOffers);
    await flush();
  }],
];

async function run() {
  for (const [name, check] of cases) {
    await check();
    console.log(`ok - ${name}`);
  }
  console.log(`Reifensuche UI: ${cases.length}/${cases.length} offline checks passed.`);
}
if (require.main === module) run().catch(error => { console.error(error); process.exitCode = 1; });
module.exports = {createHarness, syntheticProduct, run, assert};
