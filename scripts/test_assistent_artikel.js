// Offline DOM regression: repeated progress updates keep actionable CSRF forms.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.listeners = {}; this.textContent = ''; }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren(...children) { this.children = children; }
  addEventListener(name, handler) { this.listeners[name] = handler; }
}

(async () => {
  const elements = Object.fromEntries(['import-form', 'start', 'stop', 'progress', 'sources'].map(id => [id, new Element(id)]));
  elements['import-form'].elements = {csrf_token: {value: 'synthetic-csrf'}};
  const calls = [];
  const sources = ['ausgelesen', 'pruefen', 'laeuft', 'offen', 'ausgeschlossen', 'zuordnen'].map((state, index) => ({
    id: index + 1, state, supplier: '<script>literal supplier</script>', reference: 'synthetic.pdf', result: {hinweise: []}
  }));
  // Invalid identifiers never become URL paths, even if report input is malformed.
  sources.push({...sources[0], id: '../other'});
  const context = {
    document: {getElementById: id => elements[id], createElement: tag => new Element(tag)},
    fetch: async (url, options) => {
      calls.push({url, options});
      return {ok: true, json: async () => ({quellen: sources, offen: 0, laeuft: 0, vorschlaege: 2})};
    },
    setTimeout
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'static', 'assistent-artikel.js'), 'utf8'), context);
  for (let update = 0; update < 2; update++) {
    await elements['import-form'].listeners.submit({preventDefault() {}});
    const rows = elements.sources.children;
    assert.equal(rows.length, sources.length);
    for (const index of [0, 1]) {
      const form = rows[index].children[0];
      assert.equal(form.tagName, 'form');
      assert.equal(form.method, 'post');
      assert.equal(form.action, `/admin/assistent-artikel/quelle/${index + 1}/wiederholen`);
      assert.equal(form.children[0].type, 'hidden');
      assert.equal(form.children[0].name, 'csrf_token');
      assert.equal(form.children[0].value, 'synthetic-csrf');
      assert.equal(form.children[1].type, 'submit');
      assert.match(rows[index].textContent, /<script>literal supplier<\/script>/);
      assert.equal(rows[index].children.length, 1, 'supplier remains text, no injected element');
    }
    for (const row of rows.slice(2)) assert.equal(row.children.length, 0);
    assert.match(rows[4].textContent, /^Ausgeschlossene Quelle/);
  }
  assert.equal(calls.length, 2);
  assert.equal(calls[0].options.headers['X-CSRF-Token'], 'synthetic-csrf');
  console.log('PASS: retry forms survive repeated renders, include CSRF, and remain restricted to terminal sources.');
})().catch(error => { console.error(error); process.exitCode = 1; });
