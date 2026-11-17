/* Public projections only. No embedded feed, native provider IDs or prices. */
(function (root) {
  'use strict';
  const PRODUCT = /^(tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}$/;
  const CATEGORIES = new Set(['tyre', 'alloy_wheel', 'steel_wheel', 'body_part', 'oil', 'care', 'accessory']);
  const FLAGS = ['mounting', 'runflat', 'disposal', 'installation', 'painting', 'oil_service'];
  const CART_KEY = 'gaertner-shop-v2-selection';
  const MAX_PRICE_RETRIES = 8, MAX_IMAGE_RETRIES = 5;
  const money = n => Number.isSafeInteger(n) && n >= 0 ? new Intl.NumberFormat('de-DE', {style: 'currency', currency: 'EUR'}).format(n / 100) : 'Wird geprüft';
  const maxQuantity = id => id.startsWith('parts:') ? 20 : 4;
  function currentPrice(product, quantity, now = Date.now()) {
    const p = product && product.price;
    if (!p || typeof product.id !== 'string' || !PRODUCT.test(product.id) || product.price_status !== 'current' || p.currency !== 'EUR' || p.vat_percent !== '19.00' ||
        !Number.isInteger(quantity) || quantity < 1 || quantity > maxQuantity(product.id || '') ||
        !Number.isSafeInteger(p.stock_reported) || p.stock_reported < quantity) return null;
    const observed = Date.parse(p.observed_at), expires = Date.parse(p.expires_at);
    if (!Number.isFinite(observed) || !Number.isFinite(expires) || observed > now || expires <= now || expires - observed > 900000 || expires <= observed) return null;
    const line = p.prices_by_quantity && p.prices_by_quantity[String(quantity)];
    if (!line || line.quantity !== quantity || !Number.isSafeInteger(line.unit_gross_cent) || line.unit_gross_cent < 1 || line.unit_gross_cent > 10000000 ||
        line.items_gross_cent !== line.unit_gross_cent * quantity) return null;
    return {unit: line.unit_gross_cent, total: line.items_gross_cent, stock: p.stock_reported, expires};
  }
  function normaliseCart(value) {
    if (!Array.isArray(value) || value.length > 40) return [];
    const rows = [], seen = new Set();
    for (const item of value) {
      if (!item || typeof item !== 'object' || typeof item.id !== 'string' || !PRODUCT.test(item.id) || !Number.isInteger(item.quantity) || item.quantity < 1 || item.quantity > maxQuantity(item.id)) continue;
      const options = item.services || {};
      if (typeof options !== 'object' || Array.isArray(options) || Object.keys(options).some(k => !FLAGS.includes(k) && !['paint_color', 'care'].includes(k))) continue;
      const services = {};
      let invalid = false;
      for (const key of FLAGS) {
        if (Object.hasOwn(options, key)) {
          if (typeof options[key] !== 'boolean') invalid = true;
          else if (options[key]) services[key] = true;
        }
      }
      const color = options.paint_color || '';
      if (typeof color !== 'string' || color.length > 60 || /[\x00-\x1f\x7f<>&{}]/.test(color) || Boolean(color.trim()) !== Boolean(services.painting)) invalid = true;
      if (services.painting) services.paint_color = color.trim();
      if (options.care) {
        if (!['inside', 'outside', 'complete'].includes(options.care)) invalid = true;
        else services.care = options.care;
      }
      const group = item.assembly_group || '';
      if (typeof group !== 'string' || (group && !/^[A-Za-z0-9_:|.\-]{1,180}$/.test(group))) invalid = true;
      if (invalid) continue;
      const line = {id: item.id, quantity: item.quantity, services};
      if (group) line.assembly_group = group;
      const signature = JSON.stringify(line);
      if (!seen.has(signature)) { rows.push(line); seen.add(signature); }
    }
    return rows;
  }
  function buildQuery(values, department, page) {
    const query = new URLSearchParams({page: String(Number.isInteger(page) && page > 0 ? Math.min(page, 10000) : 1), page_size: '24'});
    if (['tyres', 'wheels', 'parts', 'accessories'].includes(department)) query.set('department', department);
    for (const field of ['q', 'brand', 'category', 'make', 'model', 'year', 'hsn', 'tsn']) {
      const value = typeof values[field] === 'string' ? values[field].trim() : '';
      if (value) query.set(field, field === 'tsn' ? value.toUpperCase() : value);
    }
    return query;
  }
  function safeMedia(value, pdf = false) {
    return typeof value === 'string' && value.length <= 250 && /^\/(?!\/)[A-Za-z0-9_./-]+$/.test(value) &&
      !value.slice(1).split('/').some(p => !p || p === '.' || p === '..') && (pdf ? /\.pdf$/i : /\.(png|jpe?g|webp)$/i).test(value);
  }
  function safeCheckout(value) {
    if (typeof value !== 'string' || value.length > 5000 || /[\s\\]/.test(value)) return false;
    try {
      const u = new URL(value);
      return u.protocol === 'https:' && u.hostname === 'checkout.stripe.com' && !u.username && !u.password && (!u.port || u.port === '443') && /^\/(c\/pay|pay)\/cs_(test|live)_[A-Za-z0-9]+$/.test(u.pathname);
    } catch (_) { return false; }
  }
  const logic = {currentPrice, normaliseCart, buildQuery, safeMedia, safeCheckout, money};
  if (typeof module !== 'undefined' && module.exports) module.exports = logic;
  if (!root.document) return;
  const document = root.document, body = document.body, api = body.dataset.api;
  if (api !== '/shop/api/v2') return;
  const byId = id => document.getElementById(id);
  const el = (tag, text, cls) => { const node = document.createElement(tag); if (text != null) node.textContent = String(text); if (cls) node.className = cls; return node; };
  const button = (text, handler, cls = 'shop-button') => { const node = el('button', text, cls); node.type = 'button'; node.addEventListener('click', handler); return node; };
  const priceNodes = new Map();
  const imageLoads = new WeakMap();
  let cart = [], quote = null, retained = false, revision = 0, csrf = document.querySelector('meta[name=shop-csrf]').content || '', addressReady = false;
  try { cart = normaliseCart(JSON.parse(root.localStorage.getItem(CART_KEY) || '[]')); } catch (_) { /* selection starts empty */ }
  function notice(text) { const n = byId('shop-announcement'); n.textContent = text; n.hidden = false; root.setTimeout(() => { n.hidden = true; }, 4500); }
  function invalidateQuote() {
    quote = null; retained = false;
    if (byId('quote-review')) byId('quote-review').replaceChildren();
    if (byId('checkout-button')) byId('checkout-button').hidden = true;
  }
  function saveCart() {
    revision++;
    invalidateQuote();
    try { root.localStorage.setItem(CART_KEY, JSON.stringify(cart)); } catch (_) { notice('Auswahl bleibt nur in diesem geöffneten Fenster erhalten.'); }
    document.querySelectorAll('[data-cart-count]').forEach(node => { node.textContent = String(cart.reduce((n, row) => n + row.quantity, 0)); });
  }
  saveCart();
  async function requestJSON(path, options = {}) {
    const headers = {...options.headers};
    if (options.body != null) { headers['Content-Type'] = 'application/json'; if (csrf) headers['X-CSRF-Token'] = csrf; }
    const response = await root.fetch(api + path, {...options, headers, credentials: 'same-origin', cache: 'no-store', redirect: 'error'});
    const text = await response.text();
    if (text.length > 4 * 1024 * 1024) throw new Error('response_unavailable');
    let result; try { result = JSON.parse(text); } catch (_) { throw new Error('service_unavailable'); }
    if (!response.ok) throw new Error(typeof result.error === 'string' ? result.error : 'service_unavailable');
    return result;
  }
  function errorText(error) {
    const code = error && error.message;
    if (['authorization_required', 'invalid_session', 'session_expired', 'csrf_invalid'].includes(code)) return 'Die Sitzung ist abgelaufen. Bitte diese Seite neu laden und Ihre Anschrift erneut bestätigen.';
    if (['invalid_query', 'invalid_vehicle_filter'].includes(code)) return 'Bitte die Filter prüfen. HSN und TSN gemeinsam eingeben oder Marke, Modell und Baujahr auswählen.';
    if (code === 'invalid_request' || code === 'invalid_address') return 'Bitte Ihre Eingaben prüfen. Es werden vollständige Anschriften in Deutschland benötigt.';
    if (code === 'not_found' || code === 'catalogue_unavailable') return 'Das Sortiment ist derzeit noch nicht verfügbar. Bitte versuchen Sie es später erneut.';
    if (code === 'product_not_found') return 'Dieser Artikel ist nicht mehr im freigegebenen Sortiment.';
    return 'Die Angaben können gerade nicht sicher geprüft werden. Bitte später erneut versuchen.';
  }
  function updatePrice(node, product, quantity) {
    const price = currentPrice(product, quantity);
    node.replaceChildren();
    if (!price) { node.append(el('strong', 'Preis wird aktualisiert'), el('small', 'Preis oder Verfügbarkeit für diese Menge noch nicht bestätigt.')); return; }
    node.append(el('strong', money(price.unit)), el('small', 'pro ' + (product.unit_label || 'Stück') + ' · inkl. 19 % MwSt.'));
    if (quantity > 1) node.append(el('small', quantity + ' Stück: ' + money(price.total)));
    if (product.price.base_price_unit === 'L' && Number.isSafeInteger(product.price.base_price_gross_cent)) node.append(el('small', money(product.price.base_price_gross_cent) + ' / Liter'));
    node.append(el('small', price.stock + ' Stück gemeldet', 'shop-stock'), el('small', 'Versand und optionale Leistungen im Gesamtpreis.'));
  }
  function priceBox(product, quantity, options = {}) {
    const box = el('div', null, 'shop-price'); updatePrice(box, product, quantity);
    priceNodes.set(box, {product, quantity, generation: 0, timer: null, pendingRefresh: null, ...options}); return box;
  }
  function visibleDocument() { return document.visibilityState !== 'hidden'; }
  const priceRetryDelay = retries => Math.min(20000, 1200 * (2 ** retries)) + Math.floor(Math.random() * 400);
  function priceActive(node, state) {
    return priceNodes.get(node) === state && node.isConnected && (!state.active || state.active());
  }
  function copyPrice(target, incoming) {
    if (!validProduct(incoming) || incoming.id !== target.id) return false;
    // Only the already-public price projection changes. No title, identity,
    // service/quantity selection, image, fitment or controls are replaced.
    target.price_status = incoming.price_status;
    target.price = incoming.price;
    return true;
  }
  function priceChanged(node, state) {
    updatePrice(node, state.product, state.quantity);
    if (state.onUpdate) state.onUpdate();
  }
  function scheduleQuantityPrice(node, result, generation, retries = 0) {
    const state = priceNodes.get(node);
    if (!state || generation !== state.generation || !priceActive(node, state)) return;
    if (result.prices_refreshing !== true || currentPrice(state.product, state.quantity) || retries >= MAX_PRICE_RETRIES) {
      state.pendingRefresh = null; return;
    }
    state.pendingRefresh = {generation, retries};
    if (!visibleDocument()) return;
    if (state.timer != null) root.clearTimeout(state.timer);
    state.timer = root.setTimeout(() => {
      state.timer = null;
      if (visibleDocument()) { state.pendingRefresh = null; fetchQuantityPrice(node, generation, retries + 1); }
    }, priceRetryDelay(retries));
  }
  async function fetchQuantityPrice(node, generation, retries = 0) {
    const state = priceNodes.get(node);
    if (!state || state.generation !== generation || !priceActive(node, state) || !visibleDocument()) return;
    const quantity = state.quantity;
    try {
      const result = await requestJSON('/products/' + encodeURIComponent(state.product.id) + '?quantity=' + quantity);
      if (!priceActive(node, state) || state.generation !== generation || state.quantity !== quantity) return;
      if (!copyPrice(state.product, result.item)) return;
      priceChanged(node, state);
      scheduleQuantityPrice(node, result, generation, retries);
    } catch (_) { /* keep the current/explicitly pending price; no blind retry */ }
  }
  function changePriceQuantity(node, quantity) {
    const state = priceNodes.get(node);
    if (!state) return;
    if (state.timer != null) root.clearTimeout(state.timer);
    state.timer = null; state.pendingRefresh = null; state.quantity = quantity; state.generation++;
    priceChanged(node, state);
    fetchQuantityPrice(node, state.generation);
  }
  function setImageSource(img, source) {
    const old = imageLoads.get(img);
    if (old && old.timer != null) root.clearTimeout(old.timer);
    const state = {source, retries: 0, timer: null}; imageLoads.set(img, state);
    const active = () => {
      const dialog = img.closest && img.closest('dialog');
      return img.isConnected && (!dialog || dialog.open);
    };
    img.onload = () => { if (imageLoads.get(img) === state && state.timer != null) { root.clearTimeout(state.timer); state.timer = null; } };
    img.onerror = () => {
      if (imageLoads.get(img) !== state || state.timer != null || !active()) return;
      if (state.retries >= MAX_IMAGE_RETRIES) { img.replaceWith(el('span', 'Produktbild gerade nicht verfügbar', 'shop-no-image')); return; }
      const delay = Math.min(5000, 400 * (2 ** state.retries)) + Math.floor(Math.random() * 300);
      state.retries++;
      state.timer = root.setTimeout(() => {
        state.timer = null;
        if (imageLoads.get(img) === state && active()) img.src = source + '?shop_retry=' + state.retries;
      }, delay);
    };
    img.src = source;
  }
  function quantitySelect(id, value, onChange) {
    const select = el('select', null, 'shop-quantity');
    select.setAttribute('aria-label', 'Anzahl');
    for (let n = 1; n <= maxQuantity(id); n++) { const option = el('option', n); option.value = String(n); select.append(option); }
    select.value = String(value); select.addEventListener('change', () => onChange(Number(select.value))); return select;
  }
  function productImage(product, cls, lazy = true) {
    const source = Array.isArray(product.images) && product.images.find(url => safeMedia(url));
    if (!source) return el('span', 'Produktbild noch nicht verfügbar', 'shop-no-image');
    const img = el('img', null, cls); img.alt = product.name; if (lazy) img.loading = 'lazy'; img.decoding = 'async';
    setImageSource(img, source); return img;
  }
  function validProduct(product) { return product && PRODUCT.test(product.id) && typeof product.name === 'string' && CATEGORIES.has(product.category); }
  function addToCart(product, quantity, services = {}) {
    const candidate = normaliseCart([{id: product.id, quantity, services}]);
    if (!candidate.length) { notice('Bitte Ihre Auswahl und den Farbcode prüfen.'); return; }
    const row = candidate[0];
    const existing = cart.find(item => item.id === row.id && JSON.stringify(item.services) === JSON.stringify(row.services) && !item.assembly_group);
    if (existing) existing.quantity = Math.min(maxQuantity(row.id), existing.quantity + quantity);
    else if (cart.length < 40) cart.push(row);
    else { notice('Maximal 40 Positionen möglich. Bitte die Auswahl im Warenkorb prüfen.'); return; }
    saveCart(); notice(product.name + ' ist im Warenkorb.');
  }
  function card(product, active) {
    const node = el('article', null, 'shop-card'), media = button('', () => openProduct(product.id), 'shop-card-media');
    media.setAttribute('aria-label', product.name + ' ansehen'); media.append(productImage(product, ''));
    node.append(media, el('h3', product.name), el('p', [product.brand, product.size].filter(Boolean).join(' · '), 'shop-card-meta'));
    let count = 1;
    const price = priceBox(product, count, {active}), controls = el('div', null, 'shop-card-controls'), label = el('label', 'Anzahl');
    label.append(quantitySelect(product.id, count, value => { count = value; changePriceQuantity(price, value); }));
    controls.append(label, button('In den Warenkorb', () => addToCart(product, count)));
    node.append(price, controls, button('Artikel ansehen', () => openProduct(product.id), 'shop-detail-link'));
    return {node, price, product};
  }
  let page = 1, sequence = 0, filters = {}, vehicleFilters = {}, pageTimer = null, pendingPageRefresh = null;
  const visibleCards = new Map();
  function pendingPagePrices() {
    return [...visibleCards.values()].some(({price}) => {
      const state = priceNodes.get(price);
      return state && state.generation === 0 && state.quantity === 1 && priceActive(price, state) && !currentPrice(state.product, 1);
    });
  }
  function schedulePagePrices(result, run, query, expectedPage, retries = 0) {
    if (run !== sequence) return;
    if (result.prices_refreshing !== true || !pendingPagePrices() || retries >= MAX_PRICE_RETRIES) {
      pendingPageRefresh = null; return;
    }
    pendingPageRefresh = {run, query, expectedPage, retries};
    if (!visibleDocument()) return;
    if (pageTimer != null) root.clearTimeout(pageTimer);
    pageTimer = root.setTimeout(async () => {
      pageTimer = null;
      if (run !== sequence || !visibleDocument() || !pendingPagePrices()) return;
      pendingPageRefresh = null;
      try {
        const next = await requestJSON('/products?' + query);
        if (run !== sequence || !Array.isArray(next.items) || next.items.length > 24 || next.page !== expectedPage) return;
        for (const incoming of next.items) {
          const target = incoming && visibleCards.get(incoming.id);
          if (!target) continue;
          const state = priceNodes.get(target.price);
          // Once quantity controls were used, their exact-quantity request owns
          // the price. An old/default q1 page response cannot overwrite q2/q3.
          if (!state || state.generation !== 0 || state.quantity !== 1 || !priceActive(target.price, state)) continue;
          if (copyPrice(target.product, incoming)) priceChanged(target.price, state);
        }
        schedulePagePrices(next, run, query, expectedPage, retries + 1);
      } catch (_) { /* background read failures never cause unbounded retries */ }
    }, priceRetryDelay(retries));
  }
  async function loadPage() {
    const run = ++sequence, status = byId('catalog-status'), grid = byId('catalog-products');
    if (pageTimer != null) root.clearTimeout(pageTimer); pageTimer = null;
    pendingPageRefresh = null;
    for (const target of visibleCards.values()) {
      const state = priceNodes.get(target.price);
      if (state && state.timer != null) root.clearTimeout(state.timer);
    }
    visibleCards.clear();
    status.textContent = 'Sortiment wird geladen …';
    byId('previous-page').disabled = true; byId('next-page').disabled = true;
    try {
      const expectedPage = page, query = buildQuery({...filters, ...vehicleFilters}, body.dataset.department, page).toString();
      const result = await requestJSON('/products?' + query);
      if (run !== sequence) return;
      if (!Array.isArray(result.items) || result.items.length > 24 || !Number.isInteger(result.total) || result.page !== page) throw new Error('invalid_catalogue');
      const cards = result.items.filter(validProduct).map(product => card(product, () => run === sequence));
      for (const item of cards) visibleCards.set(item.product.id, item);
      grid.replaceChildren(...cards.map(item => item.node));
      status.textContent = result.total ? result.total + ' Artikel · ' + result.items.length + ' auf dieser Seite' : 'Keine bestätigten Treffer für diese Auswahl. Bitte die Filter ändern.';
      byId('page-label').textContent = 'Seite ' + page;
      byId('previous-page').disabled = page <= 1; byId('next-page').disabled = result.has_more !== true;
      schedulePagePrices(result, run, query, expectedPage);
    } catch (error) { if (run === sequence) { grid.replaceChildren(); status.textContent = errorText(error); } }
  }
  if (typeof document.addEventListener === 'function') document.addEventListener('visibilitychange', () => {
    if (!visibleDocument()) return;
    const pageWork = pendingPageRefresh;
    if (pageWork && pageTimer == null) schedulePagePrices({prices_refreshing: true}, pageWork.run, pageWork.query, pageWork.expectedPage, pageWork.retries);
    for (const [node, state] of priceNodes) {
      const work = state.pendingRefresh;
      if (work && state.timer == null && priceActive(node, state)) scheduleQuantityPrice(node, {prices_refreshing: true}, work.generation, work.retries);
    }
  });
  if (byId('catalog-products')) {
    byId('catalog-filters').addEventListener('submit', event => { event.preventDefault(); filters = Object.fromEntries(new FormData(event.currentTarget)); page = 1; loadPage(); });
    if (byId('vehicle-filters')) {
      byId('vehicle-filters').addEventListener('submit', event => { event.preventDefault(); vehicleFilters = Object.fromEntries(new FormData(event.currentTarget)); page = 1; loadPage(); });
      byId('vehicle-filters').addEventListener('reset', () => { vehicleFilters = {}; page = 1; loadPage(); });
    }
    byId('previous-page').addEventListener('click', () => { if (page > 1) { page--; loadPage(); } });
    byId('next-page').addEventListener('click', () => { page++; loadPage(); });
    loadPage();
  }
  function checkbox(label, name, form) {
    const wrap = el('label', null, 'shop-checkbox'), input = el('input'); input.type = 'checkbox'; input.name = name;
    wrap.append(input, el('span', label)); form.append(wrap); return input;
  }
  let detailSequence = 0;
  async function openProduct(id) {
    const run = ++detailSequence;
    const dialog = byId('product-dialog'), content = byId('product-detail');
    content.replaceChildren(el('p', 'Artikel wird geladen …')); if (!dialog.open) dialog.showModal();
    try {
      const result = await requestJSON('/products/' + encodeURIComponent(id) + '?quantity=1'), product = result.item;
      if (run !== detailSequence || !dialog.open) return;
      if (!validProduct(product) || product.id !== id) throw new Error('invalid_product');
      const grid = el('div', null, 'shop-detail-grid'), gallery = el('div', null, 'shop-gallery'), copy = el('div', null, 'shop-detail-copy');
      const mainImage = productImage(product, 'shop-detail-picture', false); gallery.append(mainImage);
      if (mainImage.tagName === 'IMG') mainImage.addEventListener('click', () => mainImage.classList.toggle('shop-zoomed'));
      if (Array.isArray(product.images) && product.images.length > 1) {
        const thumbnails = el('div', null, 'shop-thumbnails');
        for (const url of product.images.filter(url => safeMedia(url)).slice(0, 12)) {
          const thumb = button('', () => { if (mainImage.tagName === 'IMG') setImageSource(mainImage, url); }, '');
          thumb.setAttribute('aria-label', 'Weitere Produktansicht'); const img = el('img'); img.alt = ''; setImageSource(img, url); thumb.append(img); thumbnails.append(thumb);
        }
        gallery.append(thumbnails);
      }
      const title = el('h2', product.name); title.id = 'product-title'; copy.append(el('p', product.brand, 'shop-eyebrow'), title, el('p', product.size, 'shop-muted'));
      let count = 1;
      const price = priceBox(product, count, {active: () => run === detailSequence && dialog.open}), form = el('form', null, 'shop-options'); form.addEventListener('submit', e => e.preventDefault());
      const quantity = el('label', 'Anzahl'); quantity.append(quantitySelect(id, count, n => { count = n; changePriceQuantity(price, n); })); form.append(quantity);
      if (['tyre', 'alloy_wheel', 'steel_wheel'].includes(product.category)) {
        checkbox('Montage bei uns in Mosbach dazu wählen', 'mounting', form);
        form.append(el('small', 'Terminvereinbarung erst nach Wareneingang. Servicepreis wird im Gesamtpreis geprüft.', 'shop-muted'));
      }
      if (product.category === 'body_part') {
        const painting = checkbox('Wunschlackierung dazu wählen', 'painting', form), color = el('input');
        color.type = 'text'; color.name = 'paint_color'; color.maxLength = 60; color.placeholder = 'Farbcode / Wunschfarbe'; color.setAttribute('aria-label', 'Farbcode / Wunschfarbe'); color.hidden = true;
        painting.addEventListener('change', () => { color.hidden = !painting.checked; if (!painting.checked) color.value = ''; }); form.append(color);
        form.append(el('small', 'Eignung des Neuteils und Lackierpreis werden vor dem Abschluss geprüft.', 'shop-muted'));
      }
      if (product.category === 'oil') checkbox('Ölservice in unserer Werkstatt dazu wählen', 'oil_service', form);
      if (product.category === 'care') {
        const label = el('label', 'Aufbereitung auf Wunsch'), select = el('select', null, 'shop-quantity'); select.name = 'care';
        for (const [value, text] of [['', 'Ohne Aufbereitung'], ['inside', 'Innen'], ['outside', 'Außen'], ['complete', 'Innen und außen']]) { const opt = el('option', text); opt.value = value; select.append(opt); }
        label.append(select); form.append(label);
      }
      copy.append(price, form, button('In den Warenkorb', () => {
        const services = {}; for (const flag of FLAGS) { const field = form.elements.namedItem(flag); if (field && field.checked) services[flag] = true; }
        if (services.painting) services.paint_color = form.elements.namedItem('paint_color').value;
        const care = form.elements.namedItem('care'); if (care && care.value) services.care = care.value;
        addToCart(product, count, services);
      }));
      if (product.description) copy.append(el('p', product.description, 'shop-muted'));
      const specs = el('dl', null, 'shop-specs');
      for (const [key, value] of Object.entries(product.specifications || {}).slice(0, 40)) specs.append(el('dt', key), el('dd', value)); copy.append(specs);
      const fitment = el('section', null, 'shop-fitment'); fitment.append(el('h3', 'Gutachten und Fahrzeugzuordnung'));
      if (product.fitment && product.fitment.status === 'reviewed_with_conditions') {
        for (const url of (product.fitment.reports || []).filter(url => safeMedia(url, true))) { const link = el('a', 'Gutachten zur Ausführung öffnen ↗'); link.href = url; link.target = '_blank'; link.rel = 'noopener'; fitment.append(link, el('br')); }
        for (const vehicle of (product.fitment.vehicles || []).slice(0, 128)) {
          fitment.append(el('p', [vehicle.make, vehicle.model, vehicle.year_from + '–' + vehicle.year_to, vehicle.hsn && vehicle.hsn + '/' + vehicle.tsn].filter(Boolean).join(' · ')));
          const list = el('ul'); for (const condition of vehicle.conditions || []) list.append(el('li', condition)); fitment.append(list);
        }
        fitment.append(el('p', 'Auflagen und genaue Ausführung vor der Montage prüfen. Die Zuordnung ersetzt keine Prüfung am Fahrzeug.'));
      } else fitment.append(el('p', 'Für diese Ausführung ist hier noch keine bestätigte Fahrzeugzuordnung mit Gutachten hinterlegt. Passung vor einer Bestellung prüfen lassen.'));
      copy.append(fitment); grid.append(gallery, copy); content.replaceChildren(grid);
      scheduleQuantityPrice(price, result, 0);
    } catch (error) { if (run === detailSequence && dialog.open) content.replaceChildren(el('p', errorText(error))); }
  }
  byId('close-product').addEventListener('click', () => { detailSequence++; byId('product-dialog').close(); });
  function serviceDescription(row) {
    const s = row.services, values = [];
    if (s.mounting) values.push('Montage'); if (s.painting) values.push('Lackierung: ' + s.paint_color);
    if (s.installation) values.push('Einbau'); if (s.oil_service) values.push('Ölservice');
    if (s.care) values.push('Aufbereitung ' + ({inside: 'innen', outside: 'außen', complete: 'komplett'})[s.care]);
    return values.join(' · ');
  }
  let cartRun = 0;
  async function renderCart() {
    const run = ++cartRun, container = byId('cart-items'), summary = byId('cart-summary');
    container.replaceChildren(); summary.replaceChildren(); byId('cart-status').textContent = cart.length ? 'Artikel werden geprüft …' : 'Ihr Warenkorb ist noch leer.';
    if (!cart.length) { byId('request-quote').disabled = true; return; }
    let subtotal = 0, complete = true, subtotalExpires = Infinity, subtotalOutput = null;
    const subtotalItems = [];
    let unavailable = false;
    function refreshSubtotal() {
      if (run !== cartRun || !subtotalOutput) return;
      let value = 0, until = Infinity, ready = !unavailable;
      for (const item of subtotalItems) {
        const current = currentPrice(item.product, item.quantity);
        if (!current) ready = false;
        else { value += current.total; until = Math.min(until, current.expires); }
      }
      subtotalOutput.textContent = ready ? money(value) : 'Preise werden geprüft';
      if (ready) subtotalOutput.dataset.subtotalExpires = String(until);
      else delete subtotalOutput.dataset.subtotalExpires;
    }
    // At most 40 exact public details, in batches of four; never fetch the feed.
    for (let offset = 0; offset < cart.length; offset += 4) {
      const rows = cart.slice(offset, offset + 4);
      const results = await Promise.all(rows.map(async row => { try { return await requestJSON('/products/' + encodeURIComponent(row.id) + '?quantity=' + row.quantity); } catch (_) { return null; } }));
      if (run !== cartRun) return;
      rows.forEach((row, i) => {
        const result = results[i], product = result && result.item, index = offset + i, article = el('article', null, 'shop-cart-item'), copy = el('div');
        let pendingPrice = null;
        if (!validProduct(product) || product.id !== row.id) { complete = false; unavailable = true; copy.append(el('h3', 'Artikel nicht mehr verfügbar'), el('p', 'Bitte aus der Auswahl entfernen.', 'shop-muted')); article.append(el('div'), copy); }
        else {
          const price = priceBox(product, row.quantity, {active: () => run === cartRun, onUpdate: refreshSubtotal});
          pendingPrice = price;
          article.append(productImage(product, '')); copy.append(el('h3', product.name), el('p', product.size, 'shop-muted'), price);
          subtotalItems.push({product, quantity: row.quantity});
          const current = currentPrice(product, row.quantity); if (current) { subtotal += current.total; subtotalExpires = Math.min(subtotalExpires, current.expires); } else complete = false;
          const service = serviceDescription(row); if (service) copy.append(el('p', service, 'shop-muted'));
          copy.append(el('p', service || row.assembly_group ? 'Lieferziel zur Prüfung: Werkstatt Mosbach' : 'Lieferziel zur Prüfung: Kundenanschrift', 'shop-muted'));
          article.append(copy);
        }
        const controls = el('div', null, 'shop-cart-controls'), quantity = el('label', 'Anzahl');
        quantity.append(quantitySelect(row.id, row.quantity, n => { cart[index].quantity = n; saveCart(); renderCart(); }));
        controls.append(quantity, button('Entfernen', () => { cart.splice(index, 1); saveCart(); renderCart(); }, 'shop-detail-link')); copy.append(controls); container.append(article);
        if (pendingPrice) scheduleQuantityPrice(pendingPrice, result, 0);
      });
    }
    const total = el('p', complete ? money(subtotal) : 'Preise werden geprüft', 'shop-summary-total');
    subtotalOutput = total;
    if (complete) total.dataset.subtotalExpires = String(subtotalExpires);
    summary.append(el('p', 'Artikel-Zwischensumme', 'shop-muted'), total, el('p', 'Zuzüglich bestätigter Montage, Lackierung und Versand. Noch kein Gesamtangebot.', 'shop-muted'));
    refreshSubtotal();
    byId('cart-status').textContent = '';
    byId('request-quote').disabled = body.dataset.quoteEnabled !== 'true' || !addressReady;
    if (body.dataset.quoteEnabled !== 'true') byId('quote-status').textContent = 'Der verbindliche Gesamtpreis wird freigeschaltet, sobald Artikel, Service und Versand sicher abgeglichen sind.';
    else if (!addressReady) byId('quote-status').textContent = 'Bitte zuerst Ihre Rechnungs- und Lieferanschrift bestätigen.';
  }
  async function buyerSession() {
    const result = await requestJSON('/buyer/session');
    if (typeof result.csrf_token !== 'string' || !/^[A-Za-z0-9_\-.:]{16,256}$/.test(result.csrf_token) || typeof result.address_ready !== 'boolean') throw new Error('invalid_session');
    csrf = result.csrf_token; addressReady = result.address_ready; return result;
  }
  function addressPayload(form) {
    const data = new FormData(form), get = key => String(data.get(key) || '').trim();
    const billing = {recipient: get('recipient'), street: get('street'), postcode: get('postcode'), city: get('city'), country: 'DE'};
    const contact = {name: get('name'), email: get('email')}; if (get('phone')) contact.phone = get('phone');
    const delivery = data.get('different_delivery') ? {recipient: get('delivery_recipient'), street: get('delivery_street'), postcode: get('delivery_postcode'), city: get('delivery_city'), country: 'DE'} : null;
    return {contact, billing, delivery};
  }
  const BLOCKED = {fitment_unconfirmed: 'Fahrzeugpassung noch nicht bestätigt', rsu_mapping_unconfirmed: 'Lieferantenartikel noch nicht sicher abgeglichen', freight_unconfirmed: 'Versandkosten noch nicht bestätigt', assembly_fitment_unconfirmed: 'Rad-Reifen-Kombination noch nicht freigegeben', service_unconfirmed: 'Servicepreis noch nicht bestätigt', stock_insufficient_or_unconfirmed: 'Bestand für diese Menge noch nicht bestätigt', procurement_price_unconfirmed: 'Lieferantenpreis noch nicht bestätigt'};
  function showQuote(result) {
    const value = result.quote, review = byId('quote-review');
    if (!value || typeof value.ready !== 'boolean' || value.currency !== 'eur' || !Array.isArray(value.lines) || !Array.isArray(value.shipments)) throw new Error('invalid_quote');
    review.replaceChildren();
    const lines = el('dl');
    for (const line of value.lines) {
      lines.append(el('dt', line.quantity + ' × ' + line.name), el('dd', money(line.items_gross_cent)));
      for (const service of line.services || []) lines.append(el('dt', ({mounting: 'Montage', painting: 'Lackierung', oil_service: 'Ölservice', installation: 'Einbau', care: 'Aufbereitung'})[service.code] || 'Erforderliches Zubehör'), el('dd', money(service.gross_cent)));
    }
    for (const shipment of value.shipments) lines.append(el('dt', shipment.destination === 'workshop' ? 'Versand zur Werkstatt' : 'Direktversand zum Kunden'), el('dd', money(shipment.gross_cent)));
    review.append(lines);
    if (value.ready === true && result.quote_retained === true && Number.isSafeInteger(value.total_gross_cent) && value.total_gross_cent > 0 && /^[a-f0-9]{64}$/.test(value.quote_hash) && Date.parse(value.expires_at) > Date.now()) {
      quote = value; retained = true; review.append(el('p', 'Gesamtpreis inkl. MwSt.', 'shop-muted'), el('p', money(value.total_gross_cent), 'shop-summary-total'));
      byId('quote-status').textContent = 'Artikel, Leistungen und Versand wurden für diese Auswahl geprüft.';
      byId('checkout-button').hidden = !(body.dataset.orderEnabled === 'true' && body.dataset.checkoutEnabled === 'true');
      byId('checkout-note').textContent = 'Bitte Ihre Auswahl prüfen. Montage wird mitgekauft; den Termin vereinbaren wir nach Wareneingang.';
    } else {
      invalidateQuote(); review.append(el('p', 'Ein verbindlicher Gesamtpreis ist noch nicht möglich.', 'shop-review-note'));
      const list = el('ul', null, 'shop-errors'), reasons = new Set();
      for (const item of value.blocked_reasons || []) reasons.add(BLOCKED[item.code] || 'Ein Preis-, Service- oder Lieferdetail muss noch bestätigt werden.');
      for (const reason of reasons) list.append(el('li', reason)); review.append(list);
      byId('quote-status').textContent = 'Es wurde keine Bestellung angelegt.';
    }
  }
  if (byId('cart-items')) {
    renderCart();
    if (body.dataset.buyerEnabled === 'true') buyerSession().then(() => renderCart()).catch(error => { byId('address-status').textContent = errorText(error); });
    if (byId('buyer-address')) {
      const form = byId('buyer-address');
      form.elements.namedItem('different_delivery').addEventListener('change', event => { byId('delivery-fields').hidden = !event.target.checked; byId('delivery-fields').disabled = !event.target.checked; });
      form.addEventListener('input', () => { revision++; invalidateQuote(); addressReady = false; byId('request-quote').disabled = true; });
      form.addEventListener('submit', async event => {
        event.preventDefault(); revision++; invalidateQuote(); addressReady = false; byId('address-status').textContent = 'Anschrift wird geprüft …';
        const submittedRevision = revision;
        try {
          const result = await requestJSON('/buyer/address', {method: 'PUT', body: JSON.stringify(addressPayload(form))});
          if (result.address_ready !== true || typeof result.csrf_token !== 'string') throw new Error('invalid_session');
          if (submittedRevision !== revision) { byId('address-status').textContent = 'Die Anschrift wurde während der Prüfung geändert. Bitte erneut bestätigen.'; return; }
          csrf = result.csrf_token; addressReady = true; byId('address-status').textContent = 'Anschrift für diese Bestellung gespeichert.'; renderCart();
        } catch (error) { byId('address-status').textContent = errorText(error); }
      });
    }
    byId('request-quote').addEventListener('click', async () => {
      if (body.dataset.quoteEnabled !== 'true' || !addressReady || !cart.length) return;
      invalidateQuote(); byId('request-quote').disabled = true; byId('quote-status').textContent = 'Gesamtpreis und Lieferwege werden geprüft …';
      const submittedRevision = revision, submitted = JSON.stringify({lines: cart});
      try {
        const result = await requestJSON('/cart/quote', {method: 'POST', body: submitted});
        if (submittedRevision !== revision || !addressReady) { byId('quote-status').textContent = 'Ihre Auswahl wurde geändert. Bitte den Gesamtpreis erneut prüfen.'; return; }
        showQuote(result);
      }
      catch (error) { byId('quote-status').textContent = errorText(error); }
      finally { byId('request-quote').disabled = !addressReady; }
    });
    byId('checkout-button').addEventListener('click', async () => {
      if (body.dataset.orderEnabled !== 'true' || body.dataset.checkoutEnabled !== 'true' || !quote || !retained || !addressReady) return;
      if (Date.parse(quote.expires_at) <= Date.now()) { invalidateQuote(); byId('quote-status').textContent = 'Das Angebot ist abgelaufen. Bitte den Gesamtpreis erneut prüfen.'; return; }
      const frozenQuote = quote, hash = frozenQuote.quote_hash; byId('checkout-button').disabled = true; byId('quote-status').textContent = 'Ihre geprüfte Auswahl wird übernommen …';
      let orderId = null;
      try {
        const keyName = 'shop-order-key:' + hash; let key;
        try { key = root.sessionStorage.getItem(keyName); } catch (_) { /* memory fallback */ }
        if (!key) { key = 'shop-' + root.crypto.randomUUID(); try { root.sessionStorage.setItem(keyName, key); } catch (_) { /* server idempotency still binds this attempt */ } }
        const draft = await requestJSON('/orders', {method: 'POST', body: JSON.stringify({quote_hash: hash, idempotency_key: key})});
        if (!draft.order || !/^sho_[a-f0-9]{32}$/.test(draft.order.id)) throw new Error('order_unavailable');
        orderId = draft.order.id;
        if (draft.payment_enabled !== true || draft.order.quote_hash !== hash || draft.order.amount_gross_cent !== frozenQuote.total_gross_cent) throw new Error('checkout_unavailable');
        const result = await requestJSON('/orders/' + orderId + '/checkout', {method: 'POST', body: '{}'});
        if (!result.checkout || result.checkout.order_id !== orderId || result.checkout.currency !== 'eur' || result.checkout.amount_gross_cent !== frozenQuote.total_gross_cent || !safeCheckout(result.checkout.checkout_url) || result.checkout.capture_triggered !== false || result.checkout.supplier_order_created !== false) throw new Error('checkout_unavailable');
        root.location.assign(result.checkout.checkout_url);
      } catch (_) {
        byId('quote-status').textContent = orderId ? 'Der Zahlungsstatus muss sicher geprüft werden. Bitte keine neue Bestellung starten.' : 'Der Abschluss konnte nicht sicher bestätigt werden. Bitte prüfen Sie den Status vor einem neuen Versuch.';
        if (orderId) { const link = el('a', 'Bestellstatus prüfen'); link.href = root.location.pathname.replace(/\/warenkorb\/?$/, '') + '/orders/' + orderId + '/success'; byId('quote-review').append(link); }
      }
    });
  }
  if (body.dataset.page === 'order_status') {
    const id = body.dataset.orderId, status = byId('order-status'), summary = byId('order-summary');
    const labels = {draft: 'Auswahl gespeichert. Noch keine Zahlung.', payment_pending: 'Zahlungsstatus wird noch geprüft.', authorized: 'Kartenbetrag reserviert. Noch nicht eingezogen.', paid: 'Zahlung bestätigt.', payment_failed: 'Zahlung nicht bestätigt.', cancelled: 'Dieser Zahlungsvorgang ist beendet.'};
    let checks = 0;
    async function poll() {
      if (!/^sho_[a-f0-9]{32}$/.test(id)) return;
      try {
        const result = await requestJSON('/orders/' + id), order = result.order;
        if (!order || order.id !== id || !Object.hasOwn(labels, order.status)) throw new Error('order_unavailable');
        status.textContent = labels[order.status]; summary.replaceChildren(el('p', 'Bestellung ' + id), el('p', money(order.amount_gross_cent), 'shop-summary-total'));
        if (order.mode === 'test') summary.append(el('p', 'Testvorgang · keine echte Zahlung', 'shop-notice'));
        if (order.status === 'payment_pending' && ++checks < 12) root.setTimeout(poll, 5000);
      } catch (error) { status.textContent = errorText(error); }
    }
    poll();
  }
  root.setInterval(() => {
    for (const [node, item] of priceNodes) { if (!node.isConnected) priceNodes.delete(node); else updatePrice(node, item.product, item.quantity); }
    if (quote && Date.parse(quote.expires_at) <= Date.now()) { invalidateQuote(); if (byId('quote-status')) byId('quote-status').textContent = 'Das Angebot ist abgelaufen. Bitte den Gesamtpreis erneut prüfen.'; }
    document.querySelectorAll('[data-subtotal-expires]').forEach(node => { if (Number(node.dataset.subtotalExpires) <= Date.now()) { node.textContent = 'Preise werden aktualisiert'; delete node.dataset.subtotalExpires; } });
  }, 15000);
})(typeof window !== 'undefined' ? window : globalThis);
