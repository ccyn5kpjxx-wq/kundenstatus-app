const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../static/mietwagen_vorschau/index.html'), 'utf8');
const fragments = [...html.matchAll(/<a\b[^>]*href="(#[^"]*)"/g)].map(m => m[1]);
const ids = new Set([...html.matchAll(/\bid="([^"]+)"/g)].map(m => m[1]));
assert(fragments.length > 0);
for (const fragment of fragments) assert(ids.has(fragment.slice(1)), `Missing target: ${fragment}`);
const start = html.indexOf('function fragmentLinksAufAktuelleSeite(){');
const end = html.indexOf("    const menuToggle=", start);
assert(start > 0 && end > start, 'Shipped initializer exists');
for (const address of [
  'https://autovermietung-mos.de/#flotte',
  'https://autovermietung-mos.de/?utm_source=test#flotte',
  'https://autovermietung-mos.de/mietwagen-vorschau/#flotte',
  'https://kundenstatus-app.onrender.com/mietwagen-vorschau/?campaign=test#top',
]) {
  const links = fragments.map(fragment => ({ getAttribute: () => fragment, href: '' }));
  const context = vm.createContext({ URL, window: { location: { href: address } }, document: {
    querySelectorAll: selector => { assert.equal(selector, 'a[href^="#"]'); return links; },
  } });
  vm.runInContext(html.slice(start, end), context);
  links.forEach((link, i) => {
    const target = new URL(link.href);
    const current = new URL(address);
    assert.equal(target.origin, current.origin);
    assert.equal(target.pathname, current.pathname);
    assert.equal(target.search, current.search);
    assert.equal(target.hash, fragments[i]);
  });
}
console.log(`PASS: ${fragments.length} fragment links, all targets, root/legacy URLs and query preservation`);
