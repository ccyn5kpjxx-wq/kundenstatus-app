/* Public projections only. No embedded feed, native provider IDs or prices. */
(function (root) {
  'use strict';
  const PRODUCT = /^(tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}$/;
  const CATEGORIES = new Set(['tyre', 'alloy_wheel', 'steel_wheel', 'body_part', 'oil', 'care', 'accessory']);
  const FLAGS = ['mounting', 'runflat', 'disposal', 'installation', 'painting', 'oil_service'];
  const CART_KEY = 'gaertner-shop-v2-selection';
  const MAX_PRICE_RETRIES = 8, MAX_IMAGE_RETRIES = 5;
  const TYRE_PURCHASE_NOTE = 'Reifengröße sowie Last- und Geschwindigkeitsindex vor dem Kauf prüfen. Fahrzeugpassung für Montage wird gesondert geprüft.';
  const inquiryOnly = product => product && product.purchase_status === 'inquiry_only';
  const money = n => Number.isSafeInteger(n) && n >= 0 ? new Intl.NumberFormat('de-DE', {style: 'currency', currency: 'EUR'}).format(n / 100) : 'Wird geprüft';
  const maxQuantity = id => id.startsWith('parts:') ? 20 : 4;
  function quotedPrice(product, quantity, now = Date.now()) {
    const p = product && product.price;
    if (!p || typeof product.id !== 'string' || !PRODUCT.test(product.id) || product.price_status !== 'current' || p.currency !== 'EUR' || p.vat_percent !== '19.00' ||
        !Number.isInteger(quantity) || quantity < 1 || quantity > maxQuantity(product.id || '') ||
        !Number.isSafeInteger(p.stock_reported) || p.stock_reported < 0) return null;
    const observed = Date.parse(p.observed_at), expires = Date.parse(p.expires_at);
    if (!Number.isFinite(observed) || !Number.isFinite(expires) || observed > now || expires <= now || expires - observed > 900000 || expires <= observed) return null;
    const line = p.prices_by_quantity && p.prices_by_quantity[String(quantity)];
    if (!line || line.quantity !== quantity || !Number.isSafeInteger(line.unit_gross_cent) || line.unit_gross_cent < 1 || line.unit_gross_cent > 10000000 ||
        line.items_gross_cent !== line.unit_gross_cent * quantity) return null;
    return {unit: line.unit_gross_cent, total: line.items_gross_cent, stock: p.stock_reported, expires};
  }
  function currentPrice(product, quantity, now = Date.now()) {
    const price = quotedPrice(product, quantity, now);
    return price && price.stock >= quantity ? price : null;
  }
  const selectionPrice = (product, quantity) => inquiryOnly(product) ? quotedPrice(product, quantity) : currentPrice(product, quantity);
  function qualityFacts(product) {
    const facts = product?.quality_facts || {}, result = [];
    if (/^[A-G]$/.test(facts.wet_grip || '')) result.push(['Nasshaftung', facts.wet_grip]);
    if (/^[A-G]$/.test(facts.fuel_efficiency || '')) result.push(['Kraftstoffeffizienz', facts.fuel_efficiency]);
    if (/^[A-C]$/.test(facts.noise_class || '')) result.push(['Geräuschklasse', facts.noise_class]);
    if (/^\d{1,3}(?:[.,]\d)?$/.test(String(facts.external_noise_db || '')) && Number(String(facts.external_noise_db).replace(',', '.')) <= 150) result.push(['Außengeräusch', facts.external_noise_db + ' dB']);
    if (/^C[123]$/.test(facts.vehicle_class || '')) result.push(['Reifenklasse', facts.vehicle_class]);
    return result;
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
  function removeCartLine(value, index) {
    if (!Array.isArray(value) || !Number.isInteger(index) || index < 0 || index >= value.length) return null;
    const group = value[index].assembly_group;
    const rows = value.filter((_, i) => i !== index).map(item => {
      const row = {...item, services: {...item.services}};
      if (group && row.assembly_group === group) {
        delete row.assembly_group;
        row.services = row.id.startsWith('tyre:') ? Object.fromEntries(['mounting', 'runflat', 'disposal'].filter(key => row.services[key] === true).map(key => [key, true])) : {};
      }
      return row;
    });
    // Clearing a group must not silently deduplicate another selected line.
    // Ask for an explicit quantity adjustment if two now-identical rows clash.
    const seen = new Set();
    for (const row of rows) { const key = JSON.stringify(row); if (seen.has(key)) return null; seen.add(key); }
    return rows;
  }
  function buildOfferRequest(items, now = Date.now()) {
    if (!Array.isArray(items) || items.length < 1 || items.length > 40 || items.some(item => !item || typeof item !== 'object') || !Number.isSafeInteger(now)) return null;
    const text = (value, limit) => typeof value === 'string' ? value.replace(/[\x00-\x1f\x7f]/g, ' ').trim().slice(0, limit) : '';
    const lines = items.map(({product, selection}, index) => {
      const selected = normaliseCart([selection])[0]; if (!selected) return null;
      const name = text(product?.name, 160) || 'Artikel derzeit nicht zuordenbar – bitte Auswahl nachreichen';
      const facts = [text(product?.size, 100), text(product?.specifications?.Ausführung, 100)].filter(Boolean);
      const services = selected.services, labels = [];
      if (services.mounting) labels.push('Montage'); if (services.runflat) labels.push('Runflat'); if (services.disposal) labels.push('Altreifenentsorgung');
      if (services.painting) labels.push('Lackierung: ' + services.paint_color); if (services.installation) labels.push('Einbau'); if (services.oil_service) labels.push('Ölservice');
      if (services.care) labels.push('Aufbereitung ' + ({inside:'innen',outside:'außen',complete:'innen und außen'})[services.care]);
      const workshop = Boolean(selected.assembly_group || services.mounting || services.painting || services.installation || services.oil_service || services.care);
      return (index+1) + '. ' + selected.quantity + ' × ' + name + (facts.length ? ' · '+facts.join(' · ') : '') + (labels.length ? ' · '+labels.join(', ') : '') + '\nLieferziel angefragt: ' + (workshop ? 'Werkstatt Mosbach' : 'Kunde');
    });
    if (lines.some(line => !line)) return null;
    const heading = 'Bitte ein unverbindliches Gesamtangebot für diese Shopauswahl einschließlich Leistungen und Versand erstellen. Lieferbarkeit und Lieferzeit bitte bestätigen. Noch keine Bestellung oder Terminbuchung.\n\n';
    const full = heading + lines.join('\n\n');
    let kept = 0, message = heading;
    for (const line of lines) { if (message.length + line.length + 260 > 2000) break; message += line + '\n\n'; kept++; }
    const omitted = lines.length-kept;
    if (omitted) message += 'Weitere ' + omitted + ' Positionen passen nicht in dieses Formular. Bitte die vollständige Auswahl nachreichen. Die unten sichtbare vollständige Liste wurde noch nicht übermittelt.';
    return {version:1, created_at:now, message:message.trim(), full_text:full, position_count:lines.length, omitted_count:omitted};
  }
  function buildQuery(values, department, page) {
    const query = new URLSearchParams({page: String(Number.isInteger(page) && page > 0 ? Math.min(page, 10000) : 1), page_size: '24'});
    if (['tyres', 'wheels', 'parts', 'accessories'].includes(department)) query.set('department', department);
    for (const field of ['q', 'brand', 'category', 'make', 'model', 'year', 'hsn', 'tsn']) {
      const value = typeof values[field] === 'string' ? values[field].trim() : '';
      if (value) query.set(field, field === 'tsn' ? value.toUpperCase() : value);
    }
    if (values.group === 'family') query.set('group', 'family');
    if (values.studio === '1') query.set('studio', '1');
    return query;
  }
  function vehicleQuery(values) {
    const result = {};
    for (const key of ['make', 'model', 'year', 'hsn', 'tsn']) if (typeof values[key] === 'string' && values[key].trim()) result[key] = values[key].trim();
    if (typeof values.keys === 'string' && values.keys.trim()) {
      const match = /^(\d{4})\s*(?:[\s/\-]\s*)?([A-Z0-9]{3})$/i.exec(values.keys.trim());
      if (!match) return null;
      return {hsn: match[1], tsn: match[2].toUpperCase()};
    }
    if (result.hsn || result.tsn) {
      if (!/^\d{4}$/.test(result.hsn || '') || !/^[A-Z0-9]{3}$/i.test(result.tsn || '')) return null;
      return {hsn: result.hsn, tsn: result.tsn.toUpperCase()};
    }
    if (result.model && !result.make || result.year && (!result.make || !result.model || !/^\d{4}$/.test(result.year) || Number(result.year) < 1900 || Number(result.year) > 2100)) return null;
    return result;
  }
  function safeMedia(value, pdf = false) {
    return typeof value === 'string' && value.length <= 250 && /^\/(?!\/)[A-Za-z0-9_./-]+$/.test(value) &&
      !value.slice(1).split('/').some(p => !p || p === '.' || p === '..') && (pdf ? /\.pdf$/i : /\.(png|jpe?g|webp)$/i).test(value);
  }
  function safeCheckout(value) {
    if (typeof value !== 'string' || value.length > 5000 || /[\s\\]/.test(value)) return false;
    try {
      const u = new URL(value);
      return u.protocol === 'https:' && u.hostname === 'checkout.stripe.com' && !u.username && !u.password && (!u.port || u.port === '443') && /^\/(c\/pay|g\/pay|pay)\/cs_(test|live)_[A-Za-z0-9]+$/.test(u.pathname);
    } catch (_) { return false; }
  }
  const logic = {currentPrice, quotedPrice, inquiryOnly, qualityFacts, normaliseCart, removeCartLine, buildOfferRequest, buildQuery, vehicleQuery, safeMedia, safeCheckout, money};
  if (typeof module !== 'undefined' && module.exports) module.exports = logic;
  if (!root.document) return;
  const document = root.document, body = document.body, api = body.dataset.api;
  if (api !== '/shop/api/v2') return;
  const byId = id => document.getElementById(id);
  const el = (tag, text, cls) => { const node = document.createElement(tag); if (text != null) node.textContent = String(text); if (cls) node.className = cls; return node; };
  const button = (text, handler, cls = 'shop-button') => { const node = el('button', text, cls); node.type = 'button'; node.addEventListener('click', handler); return node; };
  const priceNodes = new Map();
  const imageLoads = new WeakMap();
  let cart = [], quote = null, retained = false, revision = 0, csrf = document.querySelector('meta[name=shop-csrf]').content || '', addressReady = false, consumerState = null, consumerRun = 0, checkoutInProgress = false;
  try { cart = normaliseCart(JSON.parse(root.localStorage.getItem(CART_KEY) || '[]')); } catch (_) { /* selection starts empty */ }
  function notice(text) { const n = byId('shop-announcement'); n.textContent = text; n.hidden = false; root.setTimeout(() => { n.hidden = true; }, 4500); }
  function invalidateQuote() {
    quote = null; retained = false; consumerState = null; consumerRun++;
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
    if (['terms_acceptance_required', 'terms_changed_new_quote_required'].includes(code)) return 'Die Vertragsunterlagen müssen für das aktuelle Angebot erneut geprüft und bestätigt werden. Bitte den Gesamtpreis neu prüfen.';
    if (['legal_terms_unavailable', 'consumer_unavailable'].includes(code)) return 'Die Vertragsunterlagen sind gerade nicht verfügbar. Ein Abschluss ist deshalb noch nicht möglich.';
    if (code === 'product_not_found') return 'Dieser Artikel ist nicht mehr im freigegebenen Sortiment.';
    return 'Die Angaben können gerade nicht sicher geprüft werden. Bitte später erneut versuchen.';
  }
  function updatePrice(node, product, quantity) {
    const price = quotedPrice(product, quantity), inquiry = inquiryOnly(product);
    node.replaceChildren();
    if (inquiry) node.append(el('span', 'Richtpreis', 'shop-price-kind'));
    if (!price) { node.append(el('strong', 'Preis wird aktualisiert'), el('small', 'Preis oder Verfügbarkeit für diese Menge noch nicht bestätigt.')); if (inquiry) node.append(el('small', 'Preis und Verfügbarkeit auf Anfrage', 'shop-inquiry-note')); appendTyreDocuments(node, product); return; }
    node.append(el('strong', money(price.unit)), el('small', 'pro ' + (product.unit_label || 'Stück') + ' · inkl. 19 % MwSt.'));
    if (quantity > 1) node.append(el('small', quantity + ' Stück: ' + money(price.total)));
    if (product.price.base_price_unit === 'L' && Number.isSafeInteger(product.price.base_price_gross_cent)) node.append(el('small', money(product.price.base_price_gross_cent) + ' / Liter'));
    if (inquiry) node.append(el('small', 'Preis und Verfügbarkeit auf Anfrage', 'shop-inquiry-note'));
    else node.append(el('small', price.stock === 0 ? 'Derzeit nicht verfügbar' : price.stock < quantity ? 'Nur ' + price.stock + ' Stück gemeldet · gewünschte Menge nicht verfügbar' : price.stock + ' Stück gemeldet', 'shop-stock'));
    node.append(el('small', 'Versand und optionale Leistungen im Gesamtpreis.'));
    appendTyreDocuments(node, product);
  }
  function appendTyreDocuments(node, product) {
    if (product.category !== 'tyre') return;
    const docs = product.product_documents || {}, label = docs.tyre_label_url, sheet = docs.tyre_sheet_url;
    const labelValid = safeMedia(label) && /\.(jpg|png)$/i.test(label), sheetValid = safeMedia(sheet, true);
    if (!labelValid && !sheetValid) { node.append(el('small', 'EU-Reifenlabel und Produktdatenblatt werden noch geprüft.')); return; }
    const box = el('div', null, 'shop-tyre-documents');
    if (labelValid) {
      const open = button('', () => {
        const dialog = el('dialog', null, 'shop-document-dialog'), image = el('img'); image.alt = 'EU-Reifenlabel für ' + product.name; setImageSource(image, label);
        const close = button('×', () => dialog.close(), 'shop-close'); close.setAttribute('aria-label', 'EU-Reifenlabel schließen');
        const original = el('a', 'Originales EU-Reifenlabel in voller Größe öffnen ↗'); original.href = label; original.target = '_blank'; original.rel = 'noopener';
        dialog.setAttribute('aria-label', 'EU-Reifenlabel'); dialog.append(close, el('h2', 'EU-Reifenlabel'), image, original); dialog.addEventListener('close', () => dialog.remove()); document.body.append(dialog); dialog.showModal();
      }, 'shop-tyre-label');
      open.setAttribute('aria-label', 'EU-Reifenlabel für ' + product.name + ' vergrößert ansehen'); const image = el('img'); image.alt = 'EU-Reifenlabel'; image.loading = 'lazy'; setImageSource(image, label); open.append(image); box.append(open);
    }
    const links = el('div');
    if (sheetValid) { const link = el('a', 'Produktdatenblatt (PDF) ↗'); link.href = sheet; link.target = '_blank'; link.rel = 'noopener'; links.append(link); }
    if (/^[1-9][0-9]{0,11}$/.test(docs.eprel_id || '')) links.append(el('small', 'EPREL-Nr. ' + docs.eprel_id));
    if (!labelValid || !sheetValid) links.append(el('small', 'Produktunterlagen werden noch vervollständigt.'));
    box.append(links); node.append(box);
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
    if (['inquiry_only', 'supplier_check_required'].includes(incoming.purchase_status)) target.purchase_status = incoming.purchase_status;
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
    saveCart(); notice(inquiryOnly(product) ? 'Artikel ist in Ihrer Anfrageauswahl. Preis und Verfügbarkeit bleiben zu bestätigen.' : product.name + ' ist im Warenkorb.');
  }
  function card(product, active) {
    const family = ['alloy_wheel', 'steel_wheel'].includes(product.category) && Number.isSafeInteger(product.family?.variant_count) && product.family.variant_count > 1;
    const view = () => family ? openFamily(product) : openProduct(product.id);
    const node = el('article', null, 'shop-card'), media = button('', view, 'shop-card-media');
    media.setAttribute('aria-label', product.name + ' ansehen'); media.append(productImage(product, ''));
    node.append(media, el('h3', product.name), el('p', [product.brand, product.size].filter(Boolean).join(' · '), 'shop-card-meta'));
    let count = 1;
    const price = priceBox(product, count, {active}), controls = el('div', null, 'shop-card-controls'), label = el('label', 'Anzahl');
    if (family) {
      node.append(el('p', product.family.variant_count + ' Ausführungen' + (Array.isArray(product.family.diameters) ? ' · ' + product.family.diameters.slice(0, 15).join(' / ') + ' Zoll' : ''), 'shop-card-meta'));
      node.append(el('p', 'Preis der gezeigten Ausführung', 'shop-card-meta'));
      controls.className += ' shop-family-controls';
      controls.append(button('Ausführung wählen', view));
    } else {
      label.append(quantitySelect(product.id, count, value => { count = value; changePriceQuantity(price, value); }));
      controls.append(label, button(inquiryOnly(product) ? 'Zur Anfrageauswahl' : 'In den Warenkorb', () => addToCart(product, count)));
    }
    if (product.department === 'parts') node.append(el('p', 'Lieferzeit wird bestätigt', 'shop-card-meta'));
    node.append(price, controls, button(family ? 'Felge ansehen' : 'Artikel ansehen', view, 'shop-detail-link'));
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
      const expectedPage = page, query = buildQuery({...filters, ...vehicleFilters, ...(body.dataset.department === 'wheels' ? {group: 'family'} : {})}, body.dataset.department, page).toString();
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
      let vehicleTimer = null;
      const applyVehicle = form => {
        const selected = vehicleQuery(Object.fromEntries(new FormData(form)));
        if (!selected) { byId('catalog-status').textContent = 'Bitte HSN und TSN gemeinsam oder Marke, Modell und Baujahr passend eingeben.'; return; }
        vehicleFilters = selected; page = 1; loadPage();
      };
      byId('vehicle-filters').addEventListener('submit', event => { event.preventDefault(); if (vehicleTimer != null) root.clearTimeout(vehicleTimer); applyVehicle(event.currentTarget); });
      byId('vehicle-filters').addEventListener('input', event => {
        if (vehicleTimer != null) root.clearTimeout(vehicleTimer);
        const form = event.currentTarget;
        vehicleTimer = root.setTimeout(() => applyVehicle(form), 450);
      });
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
  async function openFamily(representative, variantPage = 1, diameter = '') {
    const run = ++detailSequence, dialog = byId('product-dialog'), content = byId('product-detail');
    content.replaceChildren(el('p', 'Ausführungen werden geladen …')); if (!dialog.open) dialog.showModal();
    try {
      const query = new URLSearchParams({page: String(variantPage), page_size: '24'}); if (diameter) query.set('diameter', diameter);
      const result = await requestJSON('/products/' + encodeURIComponent(representative.id) + '/variants?' + query);
      if (run !== detailSequence || !dialog.open) return;
      if (!Array.isArray(result.items) || result.items.length > 24 || !Number.isInteger(result.total)) throw new Error('invalid_catalogue');
      const title = el('h2', representative.name); title.id = 'product-title';
      const grid = el('div', null, 'shop-family-list'), heading = el('div');
      heading.append(el('p', 'Design und Oberfläche', 'shop-eyebrow'), title, el('p', 'Größe und genaue Ausführung wählen. Preise und Gutachten gehören zum einzelnen Artikel.', 'shop-muted'));
      if (Array.isArray(representative.family?.diameters)) {
        const label = el('label', 'Zollgröße'), select = el('select', null, 'shop-quantity');
        const all = el('option', 'Alle Größen'); all.value = ''; select.append(all);
        for (const size of representative.family.diameters.slice(0, 30)) { const value = String(size); if (!/^\d{1,2}(?:[.,]\d{1,2})?$/.test(value)) continue; const option = el('option', value + ' Zoll'); option.value = value; select.append(option); }
        select.value = diameter; select.addEventListener('change', () => openFamily(representative, 1, select.value)); label.append(select); heading.append(label);
      }
      for (const product of result.items.filter(validProduct)) {
        if (!['alloy_wheel', 'steel_wheel'].includes(product.category)) continue;
        const row = el('article', null, 'shop-family-variant'), description = el('div');
        description.append(el('h3', product.size || product.name), el('p', [product.specifications?.Ausführung, product.article_number && 'Art.-Nr. ' + product.article_number].filter(Boolean).join(' · '), 'shop-muted'));
        const price = priceBox(product, 1, {active: () => run === detailSequence && dialog.open});
        row.append(description, price, button('Diese Felge ansehen', () => openProduct(product.id))); grid.append(row);
        Promise.resolve().then(() => scheduleQuantityPrice(price, result, 0));
      }
      const navigation = el('nav', null, 'shop-pagination'), previous = button('← Zurück', () => openFamily(representative, variantPage - 1, diameter), 'shop-button shop-button-light'), next = button('Weiter →', () => openFamily(representative, variantPage + 1, diameter), 'shop-button shop-button-light');
      previous.disabled = variantPage <= 1; next.disabled = result.has_more !== true;
      navigation.append(previous, el('span', 'Seite ' + variantPage + ' · ' + result.total + ' Ausführungen'), next);
      content.replaceChildren(heading, grid, navigation);
    } catch (error) { if (run === detailSequence && dialog.open) content.replaceChildren(el('p', errorText(error))); }
  }
  function addLines(rows) {
    const clean = normaliseCart(rows);
    if (!Array.isArray(rows) || clean.length !== rows.length || !clean.length) return false;
    const next = normaliseCart(JSON.parse(JSON.stringify(cart)));
    for (const row of clean) {
      const existing = next.find(item => item.id === row.id && JSON.stringify(item.services) === JSON.stringify(row.services) && (item.assembly_group || '') === (row.assembly_group || ''));
      if (existing) { if (existing.quantity + row.quantity > maxQuantity(row.id)) return false; existing.quantity += row.quantity; }
      else next.push(row);
    }
    if (next.length > 40) return false;
    cart = next; saveCart(); notice('Felgen und Reifen sind gemeinsam im Warenkorb.'); return true;
  }
  function bundlePanel(rim, getQuantity, active) {
    const panel = el('section', null, 'shop-bundle-panel'), heading = el('h2', 'Felge + Reifen. Ihre Wahl.');
    panel.append(el('p', 'Komplettrad zusammenstellen', 'shop-eyebrow'), heading);
    const status = el('p', 'Reifen erst auf Ihren Wunsch suchen. Es wird noch nichts in den Warenkorb gelegt.', 'shop-muted'), choices = el('div', null, 'shop-bundle-choices');
    const seasonLabel = el('label', 'Welche Reifen möchten Sie?'), season = el('select', null, 'shop-quantity');
    for (const [value, name] of [['summer', 'Sommerreifen'], ['winter', 'Winterreifen'], ['allseason', 'Ganzjahresreifen']]) { const option = el('option', name); option.value = value; season.append(option); }
    season.value = 'summer';
    seasonLabel.append(season);
    let serial = 0, selected = null, scope = 'blocked';
    const mountingLabel = el('label', null, 'shop-checkbox'), mounting = el('input'); mounting.type = 'checkbox'; mounting.name = 'bundle_mounting';
    mountingLabel.append(mounting, el('span', 'Mit Montage bei uns in Mosbach'));
    const summary = el('div', null, 'shop-bundle-summary'), add = button('Felgen + Reifen in den Warenkorb', () => {
      if (!selected || !active()) return;
      const quantity = getQuantity();
      if (!selectionPrice(rim, quantity) || !selectionPrice(selected, quantity)) { updateSummary(); return; }
      const group = mounting.checked ? 'set|' + rim.id + '|' + selected.id : '';
      const lines = [{id: rim.id, quantity, services: {}}, {id: selected.id, quantity, services: mounting.checked ? {mounting: true} : {}}];
      if (group) for (const line of lines) line.assembly_group = group;
      if (!addLines(lines)) notice('Bitte die vorhandenen Mengen im Warenkorb prüfen. Es wurde nichts hinzugefügt.');
    });
    add.disabled = true;
    function updateSummary() {
      summary.replaceChildren();
      const quantity = getQuantity(), rimPrice = selectionPrice(rim, quantity), tyrePrice = selected && selectionPrice(selected, quantity), inquiry = inquiryOnly(rim) || inquiryOnly(selected);
      add.disabled = !selected || !rimPrice || !tyrePrice;
      if (!selected) return;
      summary.append(el('p', quantity + ' × Felge + ' + quantity + ' × Reifen', 'shop-muted'));
      if (inquiry) summary.append(el('span', 'Richtpreis', 'shop-price-kind'), el('p', 'Preis und Verfügbarkeit auf Anfrage', 'shop-inquiry-note'));
      if (rimPrice && tyrePrice) summary.append(el('strong', money(rimPrice.total + tyrePrice.total), 'shop-summary-total'));
      else summary.append(el('p', 'Aktuelle Preise für diese Menge werden noch geprüft.', 'shop-muted'));
      summary.append(el('p', mounting.checked ? 'Montagepreis und Versand werden im Warenkorb verbindlich ergänzt. Den Termin vereinbaren wir nach Wareneingang.' : 'Ohne Montage als separate Felgen und Reifen. Versand wird im Warenkorb geprüft.', 'shop-muted'));
      if (scope !== 'approved') summary.append(el('p', 'Größen-/Breitenvorschlag: Fahrzeugfreigabe, Tragfähigkeit und Auflagen sind noch zu prüfen.', 'shop-notice'));
      add.textContent = inquiry ? 'Felgen + Reifen zur Anfrageauswahl' : scope === 'approved' ? 'Felgen + Reifen in den Warenkorb' : 'Felgen + Reifen zur Prüfung in den Warenkorb';
    }
    function clear() { serial++; selected = null; choices.replaceChildren(); summary.replaceChildren(); add.disabled = true; search.disabled = false; status.textContent = 'Auswahl geändert. Bitte passende Reifen erneut suchen.'; }
    const search = button('Passende Reifen zu dieser Felge suchen', async () => {
      const run = ++serial, quantity = getQuantity(); selected = null; choices.replaceChildren(); updateSummary(); status.textContent = 'Reifenvorschläge werden geprüft …'; search.disabled = true;
      try {
        const query = new URLSearchParams({quantity: String(quantity), season: season.value, ...vehicleFilters});
        const result = await requestJSON('/products/' + encodeURIComponent(rim.id) + '/compatible-tyres?' + query);
        if (run !== serial || !active()) return;
        if (result.rim_id !== rim.id || !Array.isArray(result.items) || result.items.length > 5 || !['approved', 'rim_width_candidate', 'blocked'].includes(result.scope)) throw new Error('invalid_recommendations');
        scope = result.scope;
        if (scope === 'blocked' || !result.items.length) { status.textContent = 'Für diese Auswahl liegt noch kein dokumentierter Reifenvorschlag vor. Fahrzeug und genaue Felgenausführung prüfen lassen.'; return; }
        status.textContent = scope === 'approved' ? 'Dokumentierte Reifenvorschläge. Die konkrete Fahrzeugausführung und Auflagen beachten.' : 'Vorschläge nach dokumentiertem Größen-/Breitenabgleich. Die Fahrzeugfreigabe bleibt offen.';
        for (const tyre of result.items.filter(validProduct)) {
          if (tyre.category !== 'tyre') continue;
          const row = el('label', null, 'shop-bundle-option'), radio = el('input'); radio.type = 'radio'; radio.name = 'bundle_tyre'; radio.value = tyre.id;
          const details = el('div'); details.append(el('h3', tyre.name), el('p', tyre.size, 'shop-muted'));
          const price = priceBox(tyre, quantity, {active: () => run === serial && active(), onUpdate: updateSummary});
          details.append(price);
          const quality = qualityFacts(tyre);
          for (const [label, value] of quality) details.append(el('p', label + ': ' + value, 'shop-card-meta'));
          if (!quality.length) for (const label of ['Nasshaftung', 'Kraftstoffeffizienz', 'Außengeräusch']) if (tyre.specifications?.[label]) details.append(el('p', label + ': ' + tyre.specifications[label], 'shop-card-meta'));
          row.append(radio, productImage(tyre, ''), details); choices.append(row);
          radio.addEventListener('change', () => { if (radio.checked) { selected = tyre; updateSummary(); } });
          scheduleQuantityPrice(price, result, 0);
        }
      } catch (_) { if (run === serial && active()) status.textContent = 'Die passenden Reifen sind gerade nicht sicher abrufbar. Bitte später erneut versuchen; es wurde nichts hinzugefügt.'; }
      finally { if (run === serial && active()) search.disabled = false; }
    });
    mounting.addEventListener('change', updateSummary); season.addEventListener('change', clear);
    panel.append(search, seasonLabel, status, choices, mountingLabel, summary, add, el('p', 'Kein Vorabtermin. Montage wird auf Wunsch mitgekauft; Terminabstimmung nach Eingang der Ware.', 'shop-muted'));
    return {node: panel, quantityChanged: clear, priceChanged: updateSummary};
  }
  async function openProduct(id) {
    const run = ++detailSequence;
    const dialog = byId('product-dialog'), content = byId('product-detail');
    content.replaceChildren(el('p', 'Artikel wird geladen …')); if (!dialog.open) dialog.showModal();
    try {
      const result = await requestJSON('/products/' + encodeURIComponent(id) + '?quantity=1'), product = result.item;
      if (run !== detailSequence || !dialog.open) return;
      if (!validProduct(product) || product.id !== id) throw new Error('invalid_product');
      const isRim = ['alloy_wheel', 'steel_wheel'].includes(product.category);
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
      let count = 1, bundle = null;
      const price = priceBox(product, count, {active: () => run === detailSequence && dialog.open, onUpdate: () => bundle?.priceChanged()}), form = el('form', null, 'shop-options'); form.addEventListener('submit', e => e.preventDefault());
      const quantity = el('label', 'Anzahl'); quantity.append(quantitySelect(id, count, n => { count = n; changePriceQuantity(price, n); bundle?.quantityChanged(); })); form.append(quantity);
      if (product.category === 'tyre') {
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
      copy.append(price, form, button(inquiryOnly(product) ? (isRim ? 'Nur Felge zur Anfrageauswahl' : 'Zur Anfrageauswahl') : isRim ? 'Nur Felge in den Warenkorb' : 'In den Warenkorb', () => {
        const services = {}; for (const flag of FLAGS) { const field = form.elements.namedItem(flag); if (field && field.checked) services[flag] = true; }
        if (services.painting) services.paint_color = form.elements.namedItem('paint_color').value;
        const care = form.elements.namedItem('care'); if (care && care.value) services.care = care.value;
        addToCart(product, count, services);
      }));
      if (product.description) copy.append(el('p', product.description, 'shop-muted'));
      const specs = el('dl', null, 'shop-specs');
      for (const [key, value] of Object.entries(product.specifications || {}).slice(0, 40)) specs.append(el('dt', key), el('dd', value)); copy.append(specs);
      const quality = qualityFacts(product);
      if (quality.length) { const labels = el('dl', null, 'shop-specs'); for (const [label, value] of quality) labels.append(el('dt', label), el('dd', value)); copy.append(el('h3', 'EU-Reifenlabel'), labels); }
      const fitment = el('section', null, 'shop-fitment'); fitment.append(el('h3', product.category === 'tyre' ? 'Ihre Reifenauswahl' : product.department === 'parts' ? 'Bestätigte Fahrzeugzuordnung' : 'Gutachten und Fahrzeugzuordnung'));
      if (product.category === 'tyre') fitment.append(el('p', TYRE_PURCHASE_NOTE));
      if (product.fitment && (product.fitment.status === 'reviewed_with_conditions' || product.department === 'parts' && product.fitment.status === 'provider_vehicle')) {
        for (const url of (product.fitment.reports || []).filter(url => safeMedia(url, true))) { const link = el('a', 'Gutachten zur Ausführung öffnen ↗'); link.href = url; link.target = '_blank'; link.rel = 'noopener'; fitment.append(link, el('br')); }
        for (const vehicle of (product.fitment.vehicles || []).slice(0, 128)) {
          fitment.append(el('p', [vehicle.label || [vehicle.make, vehicle.model].filter(Boolean).join(' '), vehicle.year_from && vehicle.year_to ? vehicle.year_from + '–' + vehicle.year_to : 'Baujahr nicht belegt · genaue Ausführung im Gutachten prüfen', vehicle.hsn && vehicle.hsn + '/' + vehicle.tsn].filter(Boolean).join(' · ')));
          const list = el('ul'); for (const condition of vehicle.conditions || []) list.append(el('li', condition)); fitment.append(list);
        }
        fitment.append(el('p', product.department === 'parts' ? 'Bestätigte Anbieterzuordnung. Motorisierung, Karosserieausführung und Artikelhinweise vor dem Kauf prüfen.' : 'Auflagen und genaue Ausführung vor der Montage prüfen. Die Zuordnung ersetzt keine Prüfung am Fahrzeug.'));
      } else if (product.category !== 'tyre') fitment.append(el('p', 'Für diese Ausführung ist hier noch keine bestätigte Fahrzeugzuordnung mit Gutachten hinterlegt. Passung vor einer Bestellung prüfen lassen.'));
      if (product.department === 'parts' || product.department === 'accessories') copy.append(el('p', 'Lieferzeit wird bestätigt. Gemeldeter Bestand ist keine Lieferzusage.', 'shop-muted'));
      copy.append(fitment);
      if (isRim) {
        const rimPane = el('section', null, 'shop-rim-pane'); rimPane.append(gallery, copy);
        bundle = bundlePanel(product, () => count, () => run === detailSequence && dialog.open);
        grid.append(rimPane, bundle.node);
      } else grid.append(gallery, copy);
      content.replaceChildren(grid);
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
  function mountingRequest(row, product) {
    const target = row.assembly_group ? cart.find(item => item.assembly_group === row.assembly_group && item.id.startsWith('tyre:')) : row;
    if (!target || !target.id.startsWith('tyre:')) return;
    const dialog = el('dialog', null, 'shop-mounting-dialog'), form = el('form'), heading = el('h2', 'Montage und Terminwunsch.');
    const mountingLabel = el('label', null, 'shop-checkbox'), mounting = el('input'); mounting.type = 'checkbox'; mounting.checked = Boolean(target.services.mounting || target.assembly_group); mounting.disabled = Boolean(target.assembly_group);
    mountingLabel.append(mounting, el('span', 'Montage bei uns in Mosbach auswählen. Der Servicepreis wird im Gesamtangebot geprüft.'));
    const dateLabel = el('label', 'Terminwunsch (optional)'), date = el('input'); date.type = 'date'; date.name = 'mounting_date';
    const today = new Date(); date.min = [today.getFullYear(), String(today.getMonth() + 1).padStart(2, '0'), String(today.getDate()).padStart(2, '0')].join('-'); dateLabel.append(date);
    const periodLabel = el('label', 'Tageszeit'), period = el('select');
    for (const [value, label] of [['flexible', 'Flexibel'], ['morning', 'Vormittags'], ['afternoon', 'Nachmittags']]) { const option = el('option', label); option.value = value; period.append(option); }
    period.value = 'flexible'; periodLabel.append(period);
    const fields = el('div', null, 'shop-form-grid'); fields.append(dateLabel, periodLabel);
    const status = el('p', 'Noch keine Anfrage übermittelt.', 'shop-muted'); status.setAttribute('role', 'status');
    const submit = el('button', 'Auswahl und Terminwunsch prüfen', 'shop-button'); submit.type = 'submit';
    form.append(mountingLabel, fields, submit, status);
    form.addEventListener('submit', event => {
      event.preventDefault();
      if (!cart.includes(target)) { status.textContent = 'Die Auswahl hat sich geändert. Bitte den Dialog erneut öffnen.'; return; }
      if (date.value && (!/^\d{4}-\d{2}-\d{2}$/.test(date.value) || date.value < date.min)) { status.textContent = 'Bitte einen zukünftigen Terminwunsch auswählen.'; return; }
      if (!target.assembly_group && Boolean(target.services.mounting) !== mounting.checked) {
        if (mounting.checked) target.services.mounting = true; else delete target.services.mounting;
        saveCart(); renderCart();
      }
      status.textContent = mounting.checked ? 'Montage ausgewählt. ' + (date.value ? 'Terminwunsch ' + date.value.split('-').reverse().join('.') + ' · ' + ({flexible: 'Flexibel', morning: 'Vormittags', afternoon: 'Nachmittags'})[period.value] + '. ' : '') + 'Noch nicht übermittelt. Wir bestätigen den Termin nach Lieferklärung.' : 'Montage nicht ausgewählt. Ein Terminwunsch löst keine Buchung aus.';
    });
    dialog.setAttribute('aria-label', 'Reifenmontage und Terminwunsch');
    dialog.append(button('×', () => dialog.close(), 'shop-close'), el('p', 'Montage in Mosbach', 'shop-eyebrow'), heading,
      el('p', target.quantity + ' × ' + (product.category === 'tyre' ? product.name : 'Reifen Ihrer Komplettrad-Auswahl'), 'shop-muted'),
      el('p', 'Terminbestätigung nach Lieferklärung', 'shop-appointment-helper'),
      el('p', 'Wir stimmen den Termin ab, sobald Lieferung und Montageumfang geklärt sind. Hier werden keine freien Kalendertermine angezeigt.', 'shop-muted'), form);
    const serviceUrl = body.dataset.serviceRequestUrl;
    if (typeof serviceUrl === 'string' && /^\/(?!\/)/.test(serviceUrl) && !/[\x00-\x20\\]/.test(serviceUrl)) {
      const link = el('a', 'Terminwunsch im Werkstattformular anfragen ↗', 'shop-detail-link'); link.href = serviceUrl;
      dialog.append(link, el('p', 'Das Werkstattformular wird separat geöffnet. Ihre Auswahl hier hat noch keinen Termin reserviert.', 'shop-muted'));
    }
    dialog.addEventListener('close', () => dialog.remove()); document.body.append(dialog); dialog.showModal();
  }
  let cartRun = 0, offerSelection = [];
  function offerButton(target, label = 'Warenkorb als Gesamtangebot anfragen') {
    if (body.dataset.cartRequestUrl !== '/anfrage?anliegen=allgemein&shop_anfrage=1' || !cart.length) return;
    target.append(button(label, () => {
      const value = buildOfferRequest(offerSelection);
      if (!value) { notice('Die Auswahl wird noch geladen. Bitte gleich erneut versuchen.'); return; }
      try { root.sessionStorage.setItem('gaertner-shop-offer-request-v1', JSON.stringify(value)); }
      catch (_) { notice('Die Auswahl konnte nicht an das Anfrageformular übergeben werden. Bitte die Artikel dort manuell ergänzen.'); return; }
      root.location.assign(body.dataset.cartRequestUrl);
    }, 'shop-button shop-button-light'), el('p', 'Öffnet das editierbare Anfrageformular. Ihre Auswahl ist noch nicht an uns übermittelt.', 'shop-muted'));
  }
  async function renderCart() {
    const run = ++cartRun, container = byId('cart-items'), summary = byId('cart-summary');
    container.replaceChildren(); summary.replaceChildren(); offerSelection = []; byId('cart-status').textContent = cart.length ? 'Artikel werden geprüft …' : 'Ihr Warenkorb ist noch leer.';
    if (!cart.length) { byId('request-quote').disabled = true; return; }
    let subtotal = 0, complete = true, subtotalExpires = Infinity, subtotalOutput = null, subtotalLabel = null, inquiryNote = null;
    const subtotalItems = [];
    let unavailable = false;
    function refreshSubtotal() {
      if (run !== cartRun || !subtotalOutput) return;
      let value = 0, until = Infinity, ready = !unavailable;
      for (const item of subtotalItems) {
        const current = selectionPrice(item.product, item.quantity);
        if (!current) ready = false;
        else { value += current.total; until = Math.min(until, current.expires); }
      }
      subtotalOutput.textContent = ready ? money(value) : 'Preise werden geprüft';
      const inquiry = subtotalItems.some(item => inquiryOnly(item.product));
      if (subtotalLabel) subtotalLabel.textContent = inquiry ? 'Unverbindliche Artikel-Zwischensumme' : 'Artikel-Zwischensumme';
      if (inquiryNote) inquiryNote.hidden = !inquiry;
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
        offerSelection.push({product: validProduct(product) && product.id === row.id ? product : null, selection: row});
        let pendingPrice = null;
        if (!validProduct(product) || product.id !== row.id) { complete = false; unavailable = true; copy.append(el('h3', 'Artikel nicht mehr verfügbar'), el('p', 'Bitte aus der Auswahl entfernen.', 'shop-muted')); article.append(el('div'), copy); }
        else {
          const price = priceBox(product, row.quantity, {active: () => run === cartRun, onUpdate: refreshSubtotal});
          pendingPrice = price;
          article.append(productImage(product, '')); copy.append(el('h3', product.name), el('p', product.size, 'shop-muted'), price);
          subtotalItems.push({product, quantity: row.quantity});
          const current = selectionPrice(product, row.quantity); if (current) { subtotal += current.total; subtotalExpires = Math.min(subtotalExpires, current.expires); } else complete = false;
          const service = serviceDescription(row); if (service) copy.append(el('p', service, 'shop-muted'));
          copy.append(el('p', service || row.assembly_group ? 'Lieferziel zur Prüfung: Werkstatt Mosbach' : 'Lieferziel zur Prüfung: Kundenanschrift', 'shop-muted'));
          if (product.category === 'tyre') copy.append(el('p', TYRE_PURCHASE_NOTE, 'shop-muted'));
          if (product.category === 'tyre' || row.assembly_group) {
            const action = el('div', null, 'shop-mounting-action'); action.append(button('Montagetermin in Mosbach buchen', () => mountingRequest(row, product), 'shop-button shop-button-secondary'), el('small', 'Terminbestätigung nach Lieferklärung', 'shop-muted')); copy.append(action);
          }
          article.append(copy);
        }
        const controls = el('div', null, 'shop-cart-controls'), quantity = el('label', 'Anzahl');
        quantity.append(quantitySelect(row.id, row.quantity, n => { cart[index].quantity = n; saveCart(); renderCart(); }));
        controls.append(quantity, button('Entfernen', () => { const next = removeCartLine(cart, index); if (!next) { notice('Die verbleibende Auswahl überschneidet sich mit einem anderen Artikel. Bitte zuerst die Mengen oder eine der anderen Positionen anpassen.'); return; } cart = next; saveCart(); renderCart(); }, 'shop-detail-link')); copy.append(controls); container.append(article);
        if (pendingPrice) scheduleQuantityPrice(pendingPrice, result, 0);
      });
    }
    const total = el('p', complete ? money(subtotal) : 'Preise werden geprüft', 'shop-summary-total');
    subtotalOutput = total;
    if (complete) total.dataset.subtotalExpires = String(subtotalExpires);
    subtotalLabel = el('p', 'Artikel-Zwischensumme', 'shop-muted'); inquiryNote = el('p', 'Enthält Richtpreise. Preis und Verfügbarkeit auf Anfrage.', 'shop-inquiry-note'); inquiryNote.hidden = true;
    summary.append(subtotalLabel, total, inquiryNote, el('p', 'Zuzüglich bestätigter Montage, Lackierung und Versand. Noch kein Gesamtangebot.', 'shop-muted'));
    refreshSubtotal();
    offerButton(summary);
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
    let delivery = data.get('different_delivery') ? {recipient: get('delivery_recipient'), street: get('delivery_street'), postcode: get('delivery_postcode'), city: get('delivery_city'), country: 'DE'} : null;
    const salutation = get('delivery_salutation');
    if (salutation && !['Herr', 'Frau', 'Firma'].includes(salutation)) throw new Error('invalid_address');
    if (salutation) delivery = {...(delivery || billing), salutation};
    return {contact, billing, delivery};
  }
  const BLOCKED = {mixed_cart_manual_review_required: 'Für gemischte Warenkörbe bestätigen wir vor der Zahlung das vollständige Gesamtangebot.', fitment_unconfirmed: 'Fahrzeugpassung noch nicht bestätigt', rsu_mapping_unconfirmed: 'Lieferantenartikel noch nicht sicher abgeglichen', freight_unconfirmed: 'Versandkosten noch nicht bestätigt', assembly_fitment_unconfirmed: 'Rad-Reifen-Kombination noch nicht freigegeben', service_unconfirmed: 'Servicepreis noch nicht bestätigt', stock_insufficient_or_unconfirmed: 'Bestand für diese Menge noch nicht bestätigt', procurement_price_unconfirmed: 'Lieferantenpreis noch nicht bestätigt'};
  Object.assign(BLOCKED, {
    delivery_salutation_required: 'Für den Direktversand bitte eine Lieferanrede wählen oder ein persönliches Versandangebot anfragen.',
    product_documents_unconfirmed: 'Die erforderlichen Originalunterlagen für diesen Reifen sind noch nicht vollständig bestätigt.'
  });
  async function consumerTerms(frozenQuote) {
    const checkout = byId('checkout-button'); checkout.hidden = true; checkout.disabled = true; consumerState = null;
    if (body.dataset.consumerEnabled !== 'true') { byId('checkout-note').textContent = 'Der Abschluss ist noch nicht freigeschaltet. Vertragsunterlagen und Kundenbestätigung müssen zuerst verfügbar sein.'; return; }
    const run = ++consumerRun, terms = await requestJSON('/consumer/terms');
    if (run !== consumerRun || quote !== frozenQuote || !retained) return;
    const documents = {agb: 'Allgemeine Geschäftsbedingungen', widerruf: 'Widerrufsbelehrung', widerrufsformular: 'Muster-Widerrufsformular', lieferung: 'Lieferung und Leistungen', datenschutz: 'Datenschutzhinweise'};
    if (!terms || !/^[a-f0-9]{64}$/.test(terms.pack_hash) || typeof terms.version !== 'string' || !terms.version || terms.version.length > 100 || terms.consumer_only !== true || !terms.texts || Object.keys(documents).some(key => typeof terms.texts[key] !== 'string' || !terms.texts[key] || terms.texts[key].length > 65536)) throw new Error('legal_terms_unavailable');
    const section = el('section', null, 'shop-consumer-review'); section.append(el('h3', 'Vor dem Abschluss'), el('p', 'Vertragsunterlagen · Version ' + terms.version, 'shop-muted'));
    if (typeof terms.before_order_notice === 'string' && terms.before_order_notice.length > 0 && terms.before_order_notice.length <= 4000) section.append(el('p', terms.before_order_notice, 'shop-review-note'));
    for (const [key, title] of Object.entries(documents)) { const details = el('details'), summary = el('summary', title), text = el('div', terms.texts[key], 'shop-legal-text'); details.append(summary, text); section.append(details); }
    const warranty = el('section', null, 'shop-warranty-notice'), warrantyLink = el('a'), warrantyImage = el('img');
    warrantyLink.href = '/static/recht/reifen-gewaehrleistung-de.jpg'; warrantyLink.target = '_blank'; warrantyLink.rel = 'noopener';
    warrantyLink.setAttribute('aria-label', 'EU-Hinweis zur gesetzlichen Gewährleistung in voller Größe öffnen');
    warrantyImage.src = warrantyLink.href; warrantyImage.alt = 'EU-Hinweis zur gesetzlichen Gewährleistung: mindestens zwei Jahre und Verbraucherrechte bei mangelhaften Waren.';
    warranty.append(el('h3', 'Gesetzliche Gewährleistung'), el('p', 'Offizieller EU-Hinweis. Zum Vergrößern anklicken.', 'shop-muted')); warrantyLink.append(warrantyImage); warranty.append(warrantyLink); section.append(warranty);
    const consumerLabel = el('label', null, 'shop-checkbox'), consumer = el('input'); consumer.type = 'checkbox'; consumer.name = 'consumer'; consumer.checked = false;
    consumerLabel.append(consumer, el('span', 'Ich bestelle als Privatkunde. Gewerbliche Bestellungen sind hier noch nicht freigeschaltet.'));
    const acceptLabel = el('label', null, 'shop-checkbox'), accept = el('input'); accept.type = 'checkbox'; accept.name = 'terms_accepted'; accept.checked = false;
    acceptLabel.append(accept, el('span', 'Ich akzeptiere die AGB und habe Widerrufsbelehrung sowie Liefer- und Leistungshinweise gelesen.'));
    consumerState = {quoteHash: frozenQuote.quote_hash, packHash: terms.pack_hash, consumer, accept};
    const changed = () => { if (consumerState && consumerState.quoteHash === frozenQuote.quote_hash && quote === frozenQuote && !checkoutInProgress) checkout.disabled = !(consumer.checked && accept.checked); };
    consumer.addEventListener('change', changed); accept.addEventListener('change', changed); section.append(consumerLabel, acceptLabel); byId('quote-review').append(section);
    checkout.hidden = !(body.dataset.orderEnabled === 'true' && body.dataset.checkoutEnabled === 'true'); changed();
  }
  async function showQuote(result) {
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
      byId('checkout-note').textContent = 'Bitte Ihre Auswahl prüfen. Montage wird mitgekauft; den Termin vereinbaren wir nach Wareneingang.';
      await consumerTerms(value);
    } else {
      invalidateQuote(); review.append(lines);
      if (Number.isSafeInteger(value.known_subtotal_gross_cent) && value.known_subtotal_gross_cent >= 0) review.append(el('p', 'Bekannte Zwischensumme · noch kein Gesamtangebot', 'shop-muted'), el('p', money(value.known_subtotal_gross_cent), 'shop-summary-total'));
      review.append(el('p', 'Ein verbindlicher Gesamtpreis ist noch nicht möglich.', 'shop-review-note'));
      const list = el('ul', null, 'shop-errors'), reasons = new Set();
      for (const item of value.blocked_reasons || []) reasons.add(BLOCKED[item.code] || 'Ein Preis-, Service- oder Lieferdetail muss noch bestätigt werden.');
      for (const reason of reasons) list.append(el('li', reason)); review.append(list);
      const mixed = (value.blocked_reasons || []).some(item => item.code === 'mixed_cart_manual_review_required');
      offerButton(review, mixed ? 'Gemischten Warenkorb als Gesamtangebot anfragen' : 'Auswahl als persönliches Angebot anfragen');
      byId('quote-status').textContent = 'Es wurde keine Bestellung angelegt.';
    }
    review.scrollIntoView?.({block:'start',behavior:'smooth'});
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
        await showQuote(result);
      }
      catch (error) { byId('quote-status').textContent = errorText(error); }
      finally { byId('request-quote').disabled = !addressReady; }
    });
    byId('checkout-button').addEventListener('click', async () => {
      if (body.dataset.orderEnabled !== 'true' || body.dataset.checkoutEnabled !== 'true' || body.dataset.consumerEnabled !== 'true' || !quote || !retained || !addressReady || checkoutInProgress || !consumerState || consumerState.quoteHash !== quote.quote_hash || !consumerState.consumer.checked || !consumerState.accept.checked) return;
      if (Date.parse(quote.expires_at) <= Date.now()) { invalidateQuote(); byId('quote-status').textContent = 'Das Angebot ist abgelaufen. Bitte den Gesamtpreis erneut prüfen.'; return; }
      const frozenQuote = quote, hash = frozenQuote.quote_hash, submittedRevision = revision, pack = consumerState.packHash; checkoutInProgress = true; byId('checkout-button').disabled = true; byId('quote-status').textContent = 'Ihre geprüfte Auswahl wird übernommen …';
      let orderId = null;
      try {
        const acceptance = await requestJSON('/consumer/acceptance', {method: 'POST', body: JSON.stringify({quote_hash: hash, pack_hash: pack, terms_accepted: true, consumer: true})});
        if (!acceptance || acceptance.accepted !== true || acceptance.quote_hash !== hash || acceptance.pack_hash !== pack || !Number.isFinite(Date.parse(acceptance.accepted_at))) throw new Error('terms_acceptance_required');
        if (submittedRevision !== revision || quote !== frozenQuote || !addressReady) throw new Error('terms_changed_new_quote_required');
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
      } catch (error) {
        if (!orderId && ['terms_acceptance_required', 'terms_changed_new_quote_required', 'legal_terms_unavailable', 'consumer_unavailable'].includes(error && error.message)) { invalidateQuote(); checkoutInProgress = false; byId('quote-status').textContent = errorText(error); return; }
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
  root.ShopStorefront = Object.freeze({
    read: path => {
      if (typeof path !== 'string' || !/^\/products(?:[/?]|$)/.test(path) || path.length > 1000) return Promise.reject(new Error('invalid_product_path'));
      return requestJSON(path);
    },
    currentPrice, money, vehicleQuery, buildQuery,
    imageElement: product => validProduct(product) ? productImage(product, '') : el('span', 'Produktbild noch nicht verfügbar', 'shop-no-image'),
    priceElement: (product, quantity = 1) => {
      if (!validProduct(product) || !Number.isInteger(quantity) || quantity < 1 || quantity > maxQuantity(product.id)) return el('p', 'Preis wird geprüft', 'shop-muted');
      const price = priceBox(product, quantity);
      Promise.resolve().then(() => fetchQuantityPrice(price, 0));
      return price;
    },
    openProduct,
  });
  if (body.dataset.page === 'komplettraeder') {
    const linked = new URLSearchParams(root.location.search).get('artikel');
    if (typeof linked === 'string' && /^(alloy_wheel|steel_wheel):[a-f0-9]{24}$/.test(linked)) openProduct(linked);
  }
  root.setInterval(() => {
    for (const [node, item] of priceNodes) { if (!node.isConnected) priceNodes.delete(node); else priceChanged(node, item); }
    if (quote && Date.parse(quote.expires_at) <= Date.now()) { invalidateQuote(); if (byId('quote-status')) byId('quote-status').textContent = 'Das Angebot ist abgelaufen. Bitte den Gesamtpreis erneut prüfen.'; }
    document.querySelectorAll('[data-subtotal-expires]').forEach(node => { if (Number(node.dataset.subtotalExpires) <= Date.now()) { node.textContent = 'Preise werden aktualisiert'; delete node.dataset.subtotalExpires; } });
  }, 15000);
})(typeof window !== 'undefined' ? window : globalThis);
