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
    mounting_unit_gross_cent: {mounting: 2500, runflat: 500, disposal: 400},
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
async function boot(responder = recommendations, {configure = false, setPrice, vehicle = {}, wheel = rim()} = {}) {
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
      let value;
      if (parsed.pathname.endsWith('/compatible-tyres')) value = await responder(parsed.searchParams);
      else if (parsed.pathname.endsWith('/complete-wheel-price') && setPrice) value = await setPrice(parsed.searchParams);
      else value = {item: wheel, prices_refreshing: false};
      return {ok: true, text: async () => JSON.stringify(value)};
    }};
  vm.runInNewContext(source, {window, URL, URLSearchParams, Intl, Date, console});
  await window.ShopStorefront.openProduct(RIM, configure, vehicle); await flush();
  const panel = walk(nodes['product-detail']).find(node => node.className === 'shop-bundle-panel');
  assert.ok(panel);
  const labels = panel.children.filter(node => node.tagName === 'LABEL');
  return {nodes, panel, calls, stored,
    season: labels.find(node => node.textContent === 'Welche Reifen möchten Sie?').children[0],
    sizeLabel: labels.find(node => node.textContent === 'Reifengröße für den Vergleich'),
    size: labels.find(node => node.textContent === 'Reifengröße für den Vergleich').children[0],
    quantity: walk(nodes['product-detail']).find(node => node.tagName === 'SELECT' && node['aria-label'] === (configure ? 'Anzahl Kompletträder' : 'Anzahl')),
    search: panel.children.find(node => node.tagName === 'BUTTON'),
    add: panel.children.find(node => node.tagName === 'BUTTON' && node.textContent.startsWith('Felgen + Reifen')),
    mounting: walk(panel).find(node => node.name === 'bundle_mounting'),
    choices: panel.children.find(node => node.className === 'shop-bundle-choices')};
}
const radios = state => walk(state.choices).filter(node => node.name === 'bundle_tyre');
const requestQueries = state => state.calls.filter(call => call.url.includes('/compatible-tyres?'))
  .map(call => new URL(call.url, 'https://synthetic.invalid').searchParams);

(async () => {
  const makeSelection = {wheel_make: 'BMW'};
  const makePrices = query => ({...recommendations(query), vehicle_filter_scope: 'make_only'});
  for (const status of ['supplier_check_required', 'inquiry_only']) {
    const wheel = {...rim(), purchase_status: status};
    if (status === 'inquiry_only') wheel.price = null;
    const m = await boot(makePrices, {configure:true, vehicle:makeSelection, wheel});
    assert.equal(requestQueries(m)[0].get('wheel_make'), 'BMW');
    assert.equal(radios(m).length, 5);
    assert.ok(walk(m.panel).some(n => n.textContent.includes('bestätigt keine Fahrzeugfreigabe')));
    assert.equal(m.add.disabled, true);
    if (status === 'inquiry_only') assert.equal(m.add.textContent, 'Felgen + Reifen zur Anfrage hinzufügen');
    const radio = radios(m)[0]; radio.checked = true; radio.listeners.change();
    assert.equal(m.add.disabled, false);
    assert.equal(m.add.textContent, status === 'inquiry_only' ? 'Felgen + Reifen zur Anfrage hinzufügen' : 'Felgen + Reifen zur Prüfung in den Warenkorb');
  }
  const precise = {wheel_make:'BMW', wheel_vehicle:'example-exact'};
  const exactEmpty = await boot(() => ({rim_id:RIM,scope:'rim_width_candidate',vehicle_filter_scope:'vehicle',
    items:[],available_sizes:[],selected_size:null,vehicle_sizes:[]}), {configure:true,vehicle:precise});
  assert.ok(requestQueries(exactEmpty).every(q => q.get('wheel_vehicle') === precise.wheel_vehicle));
  const separate = walk(exactEmpty.panel).find(n => n.tagName === 'A' && n.target === '_blank');
  assert.equal(separate.rel, 'noopener');
  const href = new URL(separate.href, 'https://synthetic.invalid');
  assert.equal(href.pathname, '/shop/komplettraeder/');
  assert.deepEqual([...href.searchParams.entries()], [['artikel', RIM]]);
  assert.equal(radios(exactEmpty).length, 0); assert.equal(exactEmpty.add.disabled, true);
  const noOffer = await boot(() => ({rim_id:RIM,scope:'rim_width_candidate',vehicle_filter_scope:'vehicle',
    items:[],available_sizes:[],selected_size:null,vehicle_sizes:SIZES,empty_reason:'no_priced_offer_for_quantity'}),
    {configure:true,vehicle:precise});
  noOffer.quantity.value='4'; noOffer.quantity.listeners.change(); await flush();
  assert.ok(walk(noOffer.panel).some(n=>n.textContent.includes('Für 4 Stück liegt aktuell kein Angebot mit bestätigtem Preis')));
  assert.ok(walk(noOffer.panel).some(n=>n.textContent.includes('Einzelreifen in den Größen aus dem Gutachten')));
  assert.equal(noOffer.add.disabled,true);
  const unsafe = await boot(q => ({...makePrices(q), scope:'approved'}), {configure:true,vehicle:makeSelection});
  assert.equal(radios(unsafe).length, 0); assert.equal(unsafe.add.disabled, true);
  let releaseFirstPreview;
  const direct=await boot(recommendations,{configure:true,setPrice:async query=>{
    const q=Number(query.get('quantity')), value={rim_id:RIM,tyre_id:query.get('tyre_id'),quantity:q,currency:'EUR',
      goods_gross_cent:10000*q,mounting_gross_cent:2500*q,freight_gross_cent:0,total_gross_cent:12500*q};
    if(q===1) await new Promise(resolve=>{releaseFirstPreview=resolve;});
    return value;
  }});
  assert.equal(radios(direct).length,5,'the primary configure path loads candidates immediately');
  const testURL = 'https://www.adac.de/rund-ums-fahrzeug/ausstattung-technik-zubehoer/reifen/reifentest/synthetic/';
  const reviews = await boot(query => {
    const result = recommendations(query);
    result.rated_sizes = query.get('season') === 'winter' ? [SIZES[0]] : [];
    if (result.rated_sizes.includes(result.selected_size)) {
      result.items[0].test_rating = {source: 'ADAC', grade: 2.5, stars: 4, verdict: 'gut', year: 2025, url: testURL};
      result.items[1].test_rating = {source: 'ADAC', grade: 5.5, stars: 0, verdict: 'mangelhaft', year: 2024, url: testURL};
      result.items[2].quality_facts = {wet_grip: 'A'};
      result.items[3].test_rating = {...result.items[0].test_rating, url: 'https://example.invalid/test'};
    }
    return result;
  }, {configure: true});
  assert.equal(walk(reviews.choices).some(n => n.className === 'shop-rating-stars'), false);
  reviews.season.value = 'winter'; reviews.season.listeners.change(); await flush();
  assert.equal(reviews.size.value, SIZES[0]);
  assert.deepEqual(reviews.size.children.map(n => n.textContent), [SIZES[0] + ' · mit ADAC-Test', SIZES[1]]);
  const [rated, zero, unknown, unverified] = reviews.choices.children.map(walk);
  assert.ok(rated.some(n => n.textContent === '★★★★☆' && n['aria-label'].includes('4 von 5')));
  assert.ok(rated.some(n => n.textContent === 'ADAC 2,5 · gut'));
  assert.ok(rated.some(n => n.href === testURL && n.textContent.includes('2025 · 225/40 R18 92Y')));
  assert.ok(rated.some(n => n.tagName === 'LABEL' && n.htmlFor === radios(reviews)[0].id));
  assert.ok(zero.some(n => n.textContent === '☆☆☆☆☆'));
  assert.equal(unknown.some(n => n.className === 'shop-rating-stars'), false);
  assert.equal(unverified.some(n => n.className === 'shop-rating-stars'), false);
  assert.ok(unknown.some(n => n.textContent.includes('keine ADAC-Testnote hinterlegt')));
  const reviewedChoice = radios(reviews)[0];
  reviewedChoice.checked = true; reviewedChoice.listeners.change();
  const reviewedSummary = reviews.panel.children.find(n => n.className === 'shop-bundle-summary');
  assert.ok(walk(reviewedSummary).some(n => n.textContent === '★★★★☆'));
  assert.ok(walk(reviewedSummary).some(n => n.href === testURL));
  reviews.size.value = SIZES[1]; reviews.size.listeners.change(); await flush();
  assert.equal(walk(reviews.choices).some(n => n.href === testURL || n.className === 'shop-rating-stars'), false);
  assert.equal(walk(reviewedSummary).some(n => n.href === testURL), false);
  assert.ok(walk(reviews.panel).some(n => n.textContent.includes('in ' + SIZES[1] + ' ist keine ADAC-Testnote hinterlegt')));
  const first=radios(direct)[0];first.checked=true;first.listeners.change();
  direct.mounting.checked=true;direct.mounting.listeners.change();
  assert.ok(walk(direct.panel).some(n=>n.textContent.includes('+25,00\u00a0€ für 1 Reifen')));
  assert.ok(walk(direct.panel).some(n=>n.className==='shop-summary-total' && n.textContent==='125,00\u00a0€'),
    'show goods plus service synchronously while the single-unit freight request is pending');
  assert.ok(walk(direct.panel).some(n=>n.textContent.includes('zzgl. Versand')));
  direct.quantity.value='3';direct.quantity.listeners.change();
  assert.ok(walk(direct.panel).some(n=>n.className==='shop-summary-total' && n.textContent==='375,00\u00a0€'),
    'changing quantity displays the new published tier before the updated recommendations arrive');
  assert.equal(direct.add.disabled,true,'rechecking the recommendation cannot add an outdated selection');
  await flush();
  assert.equal(radios(direct)[0].checked,true,'matching selection is kept at the new quantity');
  const amounts=()=>walk(direct.panel).filter(n=>n.className==='shop-summary-total').map(n=>n.textContent);
  assert.deepEqual(amounts(),['375,00\u00a0€']);
  releaseFirstPreview();await flush();assert.deepEqual(amounts(),['375,00\u00a0€'],'delayed one-wheel total cannot replace three-wheel selection');
  assert.deepEqual(JSON.parse(direct.stored.at(-1)),[],'configuring alone does not add a set');
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
