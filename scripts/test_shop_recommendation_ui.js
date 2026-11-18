/* Offline real storefront interactions. Synthetic data only; no provider or payment calls. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[2], 'utf8');
const RIM = 'alloy_wheel:' + 'a'.repeat(24);
const SIZES = ['225/40 R18', '235/45 R18'];

class Node {
  constructor(tag = 'div') {
    Object.assign(this, {tagName: tag.toUpperCase(), children: [], listeners: {}, dataset: {},
      textContent: '', value: '', checked: false, hidden: false, disabled: false,
      isConnected: true, classList: {toggle() {}}});
    this.elements = {namedItem: () => null};
  }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  addEventListener(name, listener) { this.listeners[name] = listener; }
  setAttribute(name, value) { this[name] = value; }
  getAttribute(name) { return this[name]; }
  removeAttribute(name) { delete this[name]; }
  showModal() { this.open = true; }
  close() { this.open = false; }
  scrollIntoView() {}
}
const walk = node => [node, ...node.children.flatMap(child => typeof child === 'object' ? walk(child) : [])];
const flush = async () => { await new Promise(resolve => setImmediate(resolve)); await new Promise(resolve => setImmediate(resolve)); };

function product(number, size = SIZES[0], unit = 5000) {
  const [_, width, ratio, diameter] = size.match(/^(\d+)\/(\d+) R(\d+)$/);
  return {id: 'tyre:' + number.toString(16).padStart(24, '0'), category: 'tyre', department: 'tyres',
    brand: 'Synthetic Brand', name: 'Synthetic Model ' + number, size: size + ' 92Y',
    images: [], geometry: {width, ratio, diameter, load_index: '92', speed_index: 'Y'},
    specifications: {}, purchase_status: 'supplier_check_required', price_status: 'current',
    price: {currency: 'EUR', vat_percent: '19.00', kind: 'merchant_pricebook', source: 'RSU', stock_reported: 20,
      observed_at: new Date(Date.now() - 86400000).toISOString(), expires_at: new Date(Date.now() + 86400000).toISOString(),
      stock_expires_at: new Date(Date.now() - 1000).toISOString(),
      prices_by_quantity: Object.fromEntries([1, 2, 3, 4].map(quantity => [quantity,
        {quantity, unit_gross_cent: unit, items_gross_cent: quantity * unit}]))}};
}
function rim() {
  return {...product(99), id: RIM, category: 'alloy_wheel', department: 'wheels', name: 'Synthetic Rim',
    tyre_seasons: ['summer', 'winter'], sale_dimensions: {width: '8', diameter: '18', offset: '45',
      bolt_count: '5', pcd: '108', centre_bore: '63.4'}};
}
function recommendations(query) {
  const selected = query.get('tyre_size') || SIZES[0];
  const items = selected === SIZES[0]
    ? [1, 2, 3, 4, 5].map(n => product(n, selected, n * 5000))
    : [6, 7].map(n => product(n, selected, n * 5000));
  return {rim_id: RIM, items, scope: 'rim_width_candidate', available_sizes: SIZES,
    selected_size: selected, prices_refreshing: false};
}
async function boot(responder = recommendations) {
  const nodes = Object.fromEntries(['close-product', 'product-dialog', 'product-detail', 'shop-announcement'].map(id => [id, new Node()]));
  const body = new Node('body'); body.dataset = {api: '/shop/api/v2', page: 'komplettraeder'};
  const link = new Node('a'); link.href = '/shop/warenkorb/';
  const calls = [], stored = [];
  const window = {document: {body, createElement: tag => new Node(tag), getElementById: id => nodes[id] || null,
    querySelector: selector => selector === 'a.shop-cart-link' ? link : {content: ''}, querySelectorAll: () => [], addEventListener() {}},
    localStorage: {getItem: () => '[]', setItem: (_, value) => stored.push(value)},
    location: {search: '', pathname: '/shop/komplettraeder/'}, setTimeout() {}, clearTimeout() {}, setInterval() {},
    fetch: async (url, options) => {
      calls.push({url, options});
      assert.equal(options.method || 'GET', 'GET', 'product selection must remain read only');
      const parsed = new URL(url, 'https://synthetic.invalid');
      assert.ok(parsed.pathname.startsWith('/shop/api/v2/products/'), 'no financial or order endpoint');
      const value = parsed.pathname.endsWith('/compatible-tyres')
        ? await responder(parsed.searchParams) : {item: rim(), prices_refreshing: false};
      return {ok: true, text: async () => JSON.stringify(value)};
    }};
  vm.runInNewContext(source, {window, URL, URLSearchParams, Intl, Date, console});
  await window.ShopStorefront.openProduct(RIM); await flush();
  const panel = walk(nodes['product-detail']).find(node => node.className === 'shop-bundle-panel');
  assert.ok(panel);
  const labels = panel.children.filter(node => node.tagName === 'LABEL');
  return {nodes, panel, calls, stored,
    season: labels.find(node => node.textContent === 'Welche Reifen möchten Sie?').children[0],
    sizeLabel: labels.find(node => node.textContent === 'Reifengröße für den Vergleich'),
    size: labels.find(node => node.textContent === 'Reifengröße für den Vergleich').children[0],
    quantity: walk(nodes['product-detail']).find(node => node.tagName === 'SELECT' && node['aria-label'] === 'Anzahl'),
    search: panel.children.find(node => node.tagName === 'BUTTON'),
    add: panel.children.find(node => node.tagName === 'BUTTON' && node.textContent.startsWith('Felgen + Reifen')),
    mounting: walk(panel).find(node => node.name === 'bundle_mounting'),
    choices: panel.children.find(node => node.className === 'shop-bundle-choices')};
}
const radios = state => walk(state.choices).filter(node => node.name === 'bundle_tyre');
const requestQueries = state => state.calls.filter(call => call.url.includes('/compatible-tyres?'))
  .map(call => new URL(call.url, 'https://synthetic.invalid').searchParams);

(async () => {
  const f = await boot();
  assert.equal(f.sizeLabel.hidden, true);
  await f.search.listeners.click();
  assert.deepEqual(f.size.children.map(node => node.value), SIZES);
  assert.equal(f.size.value, SIZES[0]); assert.equal(f.sizeLabel.hidden, false);
  assert.equal(radios(f).length, 5);
  assert.equal(requestQueries(f)[0].has('tyre_size'), false);
  assert.deepEqual(walk(f.choices).filter(node => node.tagName === 'STRONG').map(node => node.textContent),
    ['50,00\u00a0€', '100,00\u00a0€', '150,00\u00a0€', '200,00\u00a0€', '250,00\u00a0€']);
  assert.equal(walk(f.panel).some(node => /Budget|Premium|Mittelklasse/.test(node.textContent)), false, 'price does not invent a quality segment');
  f.mounting.checked = true; f.mounting.listeners.change();
  f.size.value = SIZES[1]; f.size.listeners.change(); await flush();
  assert.equal(requestQueries(f).at(-1).get('tyre_size'), SIZES[1]);
  assert.equal(radios(f).length, 2, 'short real selection is not padded');
  assert.equal(f.mounting.checked, true);
  assert.equal(f.add.disabled, true, 'old tyre radio does not survive size change');
  assert.ok(walk(f.choices).filter(node => node.tagName === 'P' && node.textContent.includes(' R18')).every(node => node.textContent.startsWith(SIZES[1])));
  f.season.value = 'winter'; f.season.listeners.change();
  assert.equal(f.sizeLabel.hidden, true); assert.equal(f.size.value, ''); assert.equal(radios(f).length, 0);
  await f.search.listeners.click();
  assert.equal(requestQueries(f).at(-1).get('season'), 'winter');
  assert.equal(requestQueries(f).at(-1).has('tyre_size'), false, 'new season selects its own documented size');
  f.quantity.value = '4'; f.quantity.listeners.change(); await flush();
  assert.equal(f.sizeLabel.hidden, true); assert.equal(f.size.value, ''); assert.equal(radios(f).length, 0);
  await f.search.listeners.click();
  assert.equal(requestQueries(f).at(-1).get('quantity'), '4');
  assert.equal(requestQueries(f).at(-1).has('tyre_size'), false);
  const radio = radios(f)[0]; radio.checked = true; radio.listeners.change();
  assert.equal(f.add.disabled, false); f.add.listeners.click();
  const cart = JSON.parse(f.stored.at(-1));
  assert.equal(cart.length, 2); assert.equal(cart[0].quantity, 4); assert.equal(cart[1].quantity, 4);
  assert.deepEqual(cart[1].services, {mounting: true}); assert.equal(cart[0].assembly_group, cart[1].assembly_group);
  assert.ok(walk(f.panel).some(node => node.textContent === 'Im Warenkorb'));

  let release;
  const slow = await boot(async query => { await new Promise(resolve => { release = resolve; }); return recommendations(query); });
  const pending = slow.search.listeners.click(); await flush();
  slow.quantity.value = '4'; slow.quantity.listeners.change(); await flush();
  release(); await pending;
  assert.equal(slow.sizeLabel.hidden, true); assert.equal(radios(slow).length, 0, 'late old-quantity response is discarded');

  const legacy = await boot(query => { const value = recommendations(query); delete value.available_sizes; delete value.selected_size; return value; });
  await legacy.search.listeners.click(); assert.equal(legacy.sizeLabel.hidden, true); assert.equal(radios(legacy).length, 5);

  const empty = await boot(query => query.has('tyre_size')
    ? {...recommendations(query), items: [], available_sizes: [SIZES[0]]} : recommendations(query));
  await empty.search.listeners.click(); empty.size.value = SIZES[1]; empty.size.listeners.change(); await flush();
  assert.equal(radios(empty).length, 0); assert.equal(empty.size.value, SIZES[1]);
  assert.equal(empty.size.children.find(node => node.value === SIZES[1]).disabled, true, 'no fallback for unavailable selected size');

  for (const mutate of [
    value => value.available_sizes.push(SIZES[0]),
    value => value.selected_size = '225/40 R18\n',
    value => value.items[0].geometry.width = '235',
    value => value.items[0].geometry.diameter = '19'
  ]) {
    const invalid = await boot(query => { const value = recommendations(query); value.available_sizes = [...SIZES]; mutate(value); return value; });
    await invalid.search.listeners.click(); assert.equal(radios(invalid).length, 0); assert.equal(invalid.add.disabled, true);
    assert.ok(walk(invalid.panel).some(node => node.textContent.startsWith('Die passenden Reifen sind gerade nicht sicher abrufbar.')));
  }
  const wrongSize = await boot(query => query.has('tyre_size') ? recommendations(new URLSearchParams()) : recommendations(query));
  await wrongSize.search.listeners.click(); wrongSize.size.value = SIZES[1]; wrongSize.size.listeners.change(); await flush();
  assert.equal(radios(wrongSize).length, 0, 'server must not substitute a different requested size');
  console.log('Exact-size tyre selection UI checks passed');
})().catch(error => { console.error(error); process.exit(1); });
