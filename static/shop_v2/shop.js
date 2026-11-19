/* Public projections only. No embedded feed, native provider IDs or prices. */
(function (root) {
  'use strict';
  const PRODUCT = /^(tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}$/;
  const CATEGORIES = new Set(['tyre', 'alloy_wheel', 'steel_wheel', 'body_part', 'oil', 'care', 'accessory']);
  const FLAGS = ['mounting', 'runflat', 'disposal', 'installation', 'painting', 'oil_service'];
  const CART_KEY = 'gaertner-shop-v2-selection';
  const PARTIAL_KEY = 'gaertner-shop-v2-separate:';
  const TOKEN = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/;
  const MAX_PRICE_RETRIES = 8, MAX_IMAGE_RETRIES = 5;
  const TYRE_PURCHASE_NOTE = 'Reifengröße sowie Last- und Geschwindigkeitsindex vor dem Kauf prüfen. Fahrzeugpassung für Montage wird gesondert geprüft.';
  const MODEL_PHOTO_NOTE = 'Modellfoto. Größe und genaue Ausführung können von der Abbildung abweichen.';
  const inquiryOnly = product => product && product.purchase_status === 'inquiry_only';
  const modelPhotoNote = product => product && ['alloy_wheel', 'steel_wheel'].includes(product.category) && product.image_note === MODEL_PHOTO_NOTE ? MODEL_PHOTO_NOTE : '';
  const money = n => Number.isSafeInteger(n) && n >= 0 ? new Intl.NumberFormat('de-DE', {style: 'currency', currency: 'EUR'}).format(n / 100) : 'Wird geprüft';
  const maxQuantity = id => id.startsWith('parts:') ? 20 : 4;
  function projectedPrice(product, quantity, now) {
    const p = product && product.price;
    if (!p || typeof product.id !== 'string' || !PRODUCT.test(product.id) || product.price_status !== 'current' || p.currency !== 'EUR' || p.vat_percent !== '19.00' ||
        !Number.isInteger(quantity) || quantity < 1 || quantity > maxQuantity(product.id || '') ||
        !Number.isSafeInteger(p.stock_reported) || p.stock_reported < 0) return null;
    const observed = Date.parse(p.observed_at), expires = Date.parse(p.expires_at);
    const partnerInquiry = p.kind === 'partner_inquiry_pricebook' && p.source === 'ALZURA';
    if (partnerInquiry && (!inquiryOnly(product) || !product.id.startsWith('parts:') || product.department !== 'parts' ||
        !['body_part', 'accessory'].includes(product.category))) return null;
    const published = p.kind === 'merchant_pricebook' && p.source === 'RSU' || partnerInquiry;
    if (!Number.isFinite(observed) || !Number.isFinite(expires) || observed > now || expires <= now || expires - observed > (published ? 604800000 : 900000) || expires <= observed) return null;
    const line = p.prices_by_quantity && p.prices_by_quantity[String(quantity)];
    if (!line || line.quantity !== quantity || !Number.isSafeInteger(line.unit_gross_cent) || line.unit_gross_cent < 1 || line.unit_gross_cent > 10000000 ||
        line.items_gross_cent !== line.unit_gross_cent * quantity) return null;
    const stockExpires = Date.parse(p.stock_expires_at);
    return {unit: line.unit_gross_cent, total: line.items_gross_cent, stock: p.stock_reported, expires,
      stockCurrent: !published || Number.isFinite(stockExpires) && stockExpires > now};
  }
  function quotedPrice(product, quantity, now = Date.now()) {
    return inquiryOnly(product) ? null : projectedPrice(product, quantity, now);
  }
  function displayPrice(product, quantity, now = Date.now()) {
    if (!inquiryOnly(product)) return quotedPrice(product, quantity, now);
    const p = product.price;
    if (!p || !(p.kind === 'merchant_pricebook' && p.source === 'RSU' ||
        p.kind === 'partner_inquiry_pricebook' && p.source === 'ALZURA' && product.department === 'parts' &&
        typeof product.id === 'string' && product.id.startsWith('parts:') && ['body_part', 'accessory'].includes(product.category))) return null;
    const observed = Date.parse(p.observed_at), reviewed = Date.parse(p.review_at), stockObserved = Date.parse(p.stock_observed_at), stockExpires = Date.parse(p.stock_expires_at);
    if (![observed, reviewed, stockObserved, stockExpires].every(Number.isFinite) || reviewed !== observed + 432000000 ||
        stockObserved < observed || stockObserved > now || stockObserved >= stockExpires || stockExpires - stockObserved > 900000) return null;
    const price = projectedPrice(product, quantity, now);
    // Retained retail information only. No current stock, quote, procurement
    // permission or checkout readiness follows from displaying this number.
    return price ? {...price, stockCurrent: false, displayOnly: true} : null;
  }
  function currentPrice(product, quantity, now = Date.now()) {
    const price = quotedPrice(product, quantity, now);
    return price && (!price.stockCurrent || price.stock >= quantity) ? price : null;
  }
  const selectionPrice = (product, quantity) => currentPrice(product, quantity);
  function rimDimensions(product) {
    if (!product || !['alloy_wheel', 'steel_wheel'].includes(product.category)) return [];
    // This projection is supplied only after the server verifies the exact
    // supplier variant. Do not manufacture it from names or partial geometry.
    const dimensions = product.sale_dimensions;
    if (!dimensions || typeof dimensions !== 'object' || Array.isArray(dimensions)) return [];
    const fields = [['width', 'Felgenbreite', ' Zoll'], ['diameter', 'Durchmesser', ' Zoll'],
      ['offset', 'Einpresstiefe (ET)', ' mm'], ['bolt_count', 'Lochzahl', ''],
      ['pcd', 'Lochkreisdurchmesser', ' mm'], ['centre_bore', 'Mittenloch', ' mm']];
    if (fields.some(([key]) => !Object.hasOwn(dimensions, key) || typeof dimensions[key] !== 'string' ||
      !(key === 'offset' ? /^[+-]?\d{1,3}(?:\.\d{1,3})?$/ : key === 'bolt_count' ? /^\d{1,2}$/ : /^\d{1,3}(?:\.\d{1,3})?$/).test(dimensions[key]))) return [];
    return fields.map(([key, label, unit]) => [label, dimensions[key].replace('.', ',') + unit]);
  }
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
      // A customer may select a wheel pair delivered as individual goods. This
      // key only links its quantity controls and never represents assembly.
      const pair = item.selection_pair || '';
      if (pair) {
        const parts = typeof pair === 'string' && /^set\|((?:alloy_wheel|steel_wheel):[a-f0-9]{24})\|(tyre:[a-f0-9]{24})$/.exec(pair);
        if (group || !parts || ![parts[1], parts[2]].includes(item.id)) invalid = true;
      }
      if (invalid) continue;
      const line = {id: item.id, quantity: item.quantity, services};
      if (group) line.assembly_group = group;
      if (pair) line.selection_pair = pair;
      const signature = JSON.stringify(line);
      if (!seen.has(signature)) { rows.push(line); seen.add(signature); }
    }
    return rows;
  }
  function wheelSetFor(cart, row) {
    const key = row.assembly_group ? 'assembly_group' : 'selection_pair';
    if (!row[key]) return null;
    const members = cart.filter(item => item[key] === row[key]);
    const rim = members.find(item => item.id.startsWith('alloy_wheel:') || item.id.startsWith('steel_wheel:'));
    const tyre = members.find(item => item.id.startsWith('tyre:'));
    return members.length === 2 && rim && tyre ? {rim, tyre} : null;
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
  function cartOfferReady(items) {
    return Array.isArray(items) && items.length > 0 && items.length <= 40 && items.every(item => {
      const product = item?.product, selected = normaliseCart([item?.selection])[0];
      return selected && validProduct(product) && product.id === selected.id && product.name.trim() &&
        ['inquiry_only', 'supplier_check_required'].includes(product.purchase_status) &&
        (product.id.startsWith('parts:') ? !['tyre', 'alloy_wheel', 'steel_wheel'].includes(product.category) : product.id.startsWith(product.category + ':'));
    });
  }
  function cartSourceReady(items) {
    return cartOfferReady(items) && items.every(item => item.product.purchase_status === 'supplier_check_required');
  }
  // This is only a UI route to the server's quote check, never a fitment proof.
  function completeWheelGroups(items) {
    if (!cartSourceReady(items)) return null;
    const groups = new Map();
    for (const {product, selection} of items) {
      const row = normaliseCart([selection])[0], group = row?.assembly_group;
      if (!group || group.length > 80 || !currentPrice(product, row.quantity)) return null;
      if (!groups.has(group)) groups.set(group, []);
      groups.get(group).push({product, row});
    }
    const result = [];
    for (const [group, members] of groups) {
      const rim = members.find(item => ['alloy_wheel', 'steel_wheel'].includes(item.product.category));
      const tyre = members.find(item => item.product.category === 'tyre');
      if (members.length !== 2 || !rim || !tyre || rim.row.quantity !== tyre.row.quantity ||
          Object.keys(rim.row.services).length || tyre.row.services.mounting !== true ||
          Object.keys(tyre.row.services).some(key => !['mounting', 'runflat', 'disposal'].includes(key)) || rimDimensions(rim.product).length !== 6) return null;
      const size = typeof tyre.product.size === 'string' && /^[0-9]{3}\/[0-9]{2,3}\s*R([0-9]{2})(?:\s+[A-Za-z0-9 +./()\-]+)?$/.exec(tyre.product.size);
      if (!size || Number(size[1]) !== Number(rim.product.sale_dimensions.diameter)) return null;
      result.push({group, rimId: rim.row.id, tyreId: tyre.row.id, quantity: rim.row.quantity});
    }
    return result.length ? result : null;
  }
  function checkoutLines(rows) {
    // An unmounted configurator pair and an individually added product can
    // refer to the same supplier item. Keep the displayed cart intact.
    const result = new Map();
    for (const {id, quantity, services, assembly_group} of rows) {
      const key = JSON.stringify([id, services, assembly_group]);
      const line = result.get(key) || {id, quantity: 0, services};
      if (assembly_group) line.assembly_group = assembly_group;
      line.quantity += quantity;
      if (line.quantity > maxQuantity(id)) throw new Error('combined_quantity_limit');
      result.set(key, line);
    }
    return [...result.values()];
  }
  function quoteMatchesSelection(lines, items) {
    const remaining = checkoutLines(items.map(item => item.selection));
    if (!Array.isArray(lines) || lines.length !== remaining.length) return false;
    for (const line of lines) {
      const index = remaining.findIndex(row => line && row.id === line.id && row.quantity === line.quantity);
      if (index < 0) return false;
      remaining.splice(index, 1);
    }
    return true;
  }
  function reviewQuoteReady(value, items, enabled) {
    const groups = enabled === true && completeWheelGroups(items);
    if (!groups || !value || value.version !== 2 || value.ready !== true || value.authorization_ready !== true ||
        value.fulfillment_ready !== false || !Array.isArray(value.blocked_reasons) || value.blocked_reasons.length ||
        !Array.isArray(value.review_requirements) || !value.review_requirements.length || value.review_requirements.length > 81 ||
        !quoteMatchesSelection(value.lines, items)) return false;
    const seen = new Set();
    for (const item of value.review_requirements) {
      if (!item || typeof item !== 'object' || Array.isArray(item)) return false;
      const keys = Object.keys(item).sort().join(','), code = item.code;
      let key;
      if (code === 'mixed_cart_manual_review_required' && keys === 'code') key = code;
      else if (code === 'fitment_unconfirmed' && keys === 'code,product_id' && groups.some(group => group.rimId === item.product_id)) key = code + ':' + item.product_id;
      else if (code === 'assembly_fitment_unconfirmed' && keys === 'assembly_group,code' && groups.some(group => group.group === item.assembly_group)) key = code + ':' + item.assembly_group;
      else return false;
      if (seen.has(key)) return false;
      seen.add(key);
    }
    return true;
  }
  function vehicleReference(value) {
    return typeof value === 'string' && value.trim().length <= 200 && !/[\x00-\x1f\x7f\ud800-\udfff]/.test(value) ? value.trim() : null;
  }
  function cartNeedsPartsVehicle(items) {
    return Array.isArray(items) && items.some(item => item?.product?.department === 'parts' || item?.product?.category === 'body_part');
  }
  function partsVehicleReference(value, allowed) {
    const canonical = /^alzura-vehicle:[1-9][0-9]{0,8}$/;
    if (typeof value !== 'string' || !canonical.test(value) || !Array.isArray(allowed) || !allowed.length || allowed.length > 512 ||
        allowed.some(ref => typeof ref !== 'string' || !canonical.test(ref)) || new Set(allowed).size !== allowed.length) return null;
    return allowed.includes(value) ? value : null;
  }
  function cartPurchaseFlow(items, deferredCompleteWheels = false, partsEnabled = true) {
    if (!cartOfferReady(items)) return 'unavailable';
    if (items.some(item => inquiryOnly(item.product) || (item.product.id.startsWith('parts:') && !partsEnabled))) return 'inquiry';
    if (items.some(item => item.selection.assembly_group)) {
      return deferredCompleteWheels === true && completeWheelGroups(items) ? 'checkout' : 'inquiry';
    }
    return 'checkout';
  }
  function separatePurchases(items, mountedEnabled, partsEnabled) {
    if (items.length < 2 || cartPurchaseFlow(items, mountedEnabled, partsEnabled) === 'checkout') return [];
    const loose = [], mounted = [], seen = new Set();
    for (const item of items) {
      const {selection, product} = item;
      if (!cartSourceReady([item]) || !currentPrice(product, selection.quantity) ||
          (product.id.startsWith('parts:') && !partsEnabled)) continue;
      const group = selection.assembly_group;
      if (group) {
        if (seen.has(group)) continue;
        seen.add(group);
        const set = items.filter(other => other.selection.assembly_group === group);
        if (mountedEnabled && completeWheelGroups(set)) mounted.push(...set);
      } else loose.push(item);
    }
    return [loose, mounted].filter(group => group.length);
  }
  // The URL identifies a tab-local selection. Always resolve its exact rows
  // against the current basket before quoting, ordering or removing anything.
  function selectedCartRows(whole, snapshot, requireAll = true) {
    if (!Array.isArray(snapshot) || !snapshot.length || snapshot.length > 40) return [];
    const wanted = normaliseCart(snapshot);
    if (wanted.length !== snapshot.length) return [];
    const keys = new Set(wanted.map(row => JSON.stringify(row)));
    const found = whole.filter(row => keys.has(JSON.stringify(row)));
    const incompleteSets = new Set();
    for (const group of new Set(wanted.map(row => row.assembly_group).filter(Boolean))) {
      if (found.filter(row => row.assembly_group === group).length !== 2 ||
          whole.filter(row => row.assembly_group === group).length !== 2) incompleteSets.add(group);
    }
    const matched = found.filter(row => !incompleteSets.has(row.assembly_group));
    return !requireAll || matched.length === wanted.length ? matched : [];
  }
  function cartNeedsCustomerDelivery(rows, workshopDelivery = 'collection') {
    if (!Array.isArray(rows) || !rows.length) return false;
    if (workshopDelivery === 'customer_reshipment') return true;
    return rows.some(row => {
      const selected = normaliseCart([row])[0];
      if (!selected) return true;
      const services = selected.services;
      return !Boolean(selected.assembly_group || services.mounting || services.painting || services.installation || services.oil_service || services.care);
    });
  }
  function deliveryAddressSignature(values) {
    const prefix = values.different_delivery === true ? 'delivery_' : '';
    return JSON.stringify(['recipient', 'street', 'postcode', 'city'].map(key => String(values[prefix + key] || '').trim()));
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
    if (omitted) message += 'Weitere ' + omitted + ' Positionen sind in der vollständigen Auswahl enthalten. Die vollständige Auswahl wird beim expliziten Senden der Anfrage mitgesendet.';
    return {version:1, created_at:now, message:message.trim(), full_text:full, position_count:lines.length, omitted_count:omitted};
  }
  function buildQuery(values, department, page) {
    const query = new URLSearchParams({page: String(Number.isInteger(page) && page > 0 ? Math.min(page, 10000) : 1), page_size: '24'});
    if (['tyres', 'wheels', 'parts', 'accessories'].includes(department)) query.set('department', department);
    for (const field of ['q', 'brand', 'category', 'make', 'model', 'year', 'hsn', 'tsn', 'width', 'ratio', 'diameter']) {
      const value = typeof values[field] === 'string' ? values[field].trim() : '';
      if (value) query.set(field, field === 'tsn' ? value.toUpperCase() : value);
    }
    // The mixed shop start page also contains rims. Group their designs on
    // the server before pagination; tyres and parts retain their exact IDs.
    if (!department || department === 'wheels' || values.group === 'family') query.set('group', 'family');
    if (values.studio === '1') query.set('studio', '1');
    if (values.complete === '1') query.set('complete', '1');
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
  const logic = {currentPrice, quotedPrice, displayPrice, inquiryOnly, rimDimensions, modelPhotoNote, qualityFacts, normaliseCart, removeCartLine, cartOfferReady, cartSourceReady, cartPurchaseFlow, completeWheelGroups, reviewQuoteReady, vehicleReference, cartNeedsPartsVehicle, partsVehicleReference, cartNeedsCustomerDelivery, deliveryAddressSignature, buildOfferRequest, buildQuery, vehicleQuery, safeMedia, safeCheckout, money};
  if (typeof module !== 'undefined' && module.exports) module.exports = logic;
  if (!root.document) return;
  const document = root.document, body = document.body, api = body.dataset.api;
  if (api !== '/shop/api/v2') return;
  const byId = id => document.getElementById(id);
  const el = (tag, text, cls) => { const node = document.createElement(tag); if (text != null) node.textContent = String(text); if (cls) node.className = cls; return node; };
  const button = (text, handler, cls = 'shop-button') => { const node = el('button', text, cls); node.type = 'button'; node.addEventListener('click', handler); return node; };
  const priceNodes = new Map();
  const imageLoads = new WeakMap();
  const paymentCheckout = ['buyerEnabled', 'quoteEnabled', 'orderEnabled', 'checkoutEnabled', 'consumerEnabled'].every(flag => body.dataset[flag] === 'true');
  let buyerSessionReady = false, addressSaving = false;
  let cart = [], cartStored = false, quote = null, retained = false, revision = 0, csrf = document.querySelector('meta[name=shop-csrf]').content || '', addressReady = false, consumerState = null, consumerRun = 0, checkoutInProgress = false, sourceReady = false, offerReady = false, cartFlow = 'unavailable', quoteBlocked = false, quoteBusy = false, quoteRequestRun = 0, confirmedWorkshopDelivery = 'collection';
  const readCart = () => normaliseCart(JSON.parse(root.localStorage.getItem(CART_KEY) || '[]'));
  const partialToken = body.dataset.page === 'warenkorb' ? new URLSearchParams(root.location.search || '').get('auswahl') : null;
  const partial = partialToken !== null;
  let basket = [], selectedSnapshot = null;
  try {
    basket = readCart();
    if (partial && TOKEN.test(partialToken)) selectedSnapshot = JSON.parse(root.sessionStorage.getItem(PARTIAL_KEY + partialToken));
  } catch (_) { /* Missing or invalid selection stays empty. */ }
  cart = partial ? selectedCartRows(basket, selectedSnapshot) : basket;
  function updateBasketCount(rows) {
    document.querySelectorAll('[data-cart-count]').forEach(node => { node.textContent = String(rows.reduce((n, row) => n + row.quantity, 0)); });
  }
  function notice(text) { const n = byId('shop-announcement'); n.textContent = text; n.hidden = false; root.setTimeout(() => { n.hidden = true; }, 4500); }
  function invalidateQuote() {
    quote = null; retained = false; consumerState = null; consumerRun++; quoteBlocked = false;
    if (byId('quote-review')) byId('quote-review').replaceChildren();
    if (byId('checkout-button')) byId('checkout-button').hidden = true;
    if (byId('checkout-note')) byId('checkout-note').textContent = 'Ein Gesamtpreis entsteht erst mit bestätigten Service- und Versandkosten. Ihre Auswahl allein löst keine Bestellung aus.';
  }
  function saveCart() {
    revision++;
    quoteRequestRun++; quoteBusy = false;
    invalidateQuote();
    try { root.localStorage.setItem(CART_KEY, JSON.stringify(cart)); cartStored = true; } catch (_) { cartStored = false; notice('Auswahl bleibt nur in diesem geöffneten Fenster erhalten.'); }
    updateBasketCount(cart);
  }
  if (partial || body.dataset.page === 'order_status') updateBasketCount(basket); else saveCart();
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
    if (code === 'combined_quantity_limit') return 'Für denselben Artikel wurde die bestellbare Menge überschritten. Bitte die Menge im Warenkorb anpassen.';
    if (['authorization_required', 'invalid_session', 'session_expired', 'csrf_invalid'].includes(code)) return 'Die Sitzung ist abgelaufen. Bitte diese Seite neu laden und Ihre Anschrift erneut bestätigen.';
    if (code === 'parts_vehicle_required') return 'Bitte das genaue Fahrzeug mit Motorisierung und Bauzeit auswählen und die Anschrift erneut bestätigen.';
    if (['invalid_query', 'invalid_vehicle_filter'].includes(code)) return 'Bitte die Filter prüfen. HSN und TSN gemeinsam eingeben oder Marke, Modell und Baujahr auswählen.';
    if (code === 'invalid_request' || code === 'invalid_address') return 'Bitte Ihre Eingaben prüfen. Es werden vollständige Anschriften in Deutschland benötigt.';
    if (code === 'not_found' || code === 'catalogue_unavailable') return 'Das Sortiment ist derzeit noch nicht verfügbar. Bitte versuchen Sie es später erneut.';
    if (['terms_acceptance_required', 'terms_changed_new_quote_required'].includes(code)) return 'Die Vertragsunterlagen müssen für das aktuelle Angebot erneut geprüft und bestätigt werden. Bitte den Gesamtpreis neu prüfen.';
    if (['legal_terms_unavailable', 'consumer_unavailable'].includes(code)) return 'Die Vertragsunterlagen sind gerade nicht verfügbar. Ein Abschluss ist deshalb noch nicht möglich.';
    if (code === 'product_not_found') return 'Dieser Artikel ist nicht mehr im freigegebenen Sortiment.';
    return 'Die Angaben können gerade nicht sicher geprüft werden. Bitte später erneut versuchen.';
  }
  function updatePrice(node, product, quantity) {
    const price = displayPrice(product, quantity), inquiry = inquiryOnly(product);
    node.replaceChildren();
    if (inquiry) {
      if (price) {
        node.append(el('strong', money(price.unit)), el('small', 'pro ' + (product.unit_label || 'Stück') + ' · inkl. 19 % MwSt.'));
        if (quantity > 1) node.append(el('small', quantity + ' ×: ' + money(price.total)));
        node.append(el('small', 'Preisstand: ' + new Intl.DateTimeFormat('de-DE', {day:'2-digit',month:'2-digit',year:'numeric'}).format(Date.parse(product.price.observed_at))));
      } else node.append(el('strong', 'Preis und Lieferbarkeit auf Anfrage'));
      node.append(el('small', 'Nur auf Anfrage. Preis und Lieferbarkeit bestätigen wir im Angebot. Keine direkte Bestellung oder Zahlung.'));
      appendTyreDocuments(node, product); return;
    }
    if (!price) { node.append(el('strong', 'Preis wird aktualisiert'), el('small', 'Preis oder Verfügbarkeit für diese Menge noch nicht bestätigt.')); appendTyreDocuments(node, product); return; }
    node.append(el('strong', money(price.unit)), el('small', 'pro ' + (product.unit_label || 'Stück') + ' · inkl. 19 % MwSt.'));
    if (quantity > 1) node.append(el('small', quantity + ' Stück: ' + money(price.total)));
    if (product.price.base_price_unit === 'L' && Number.isSafeInteger(product.price.base_price_gross_cent)) node.append(el('small', money(product.price.base_price_gross_cent) + ' / Liter'));
    node.append(el('small', !price.stockCurrent ? 'Bestand wird vor der Zahlung geprüft' : price.stock === 0 ? 'Derzeit nicht verfügbar' : price.stock < quantity ? 'Nur ' + price.stock + ' Stück gemeldet · gewünschte Menge nicht verfügbar' : price.stock + ' Stück gemeldet', 'shop-stock'));
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
    if (inquiryOnly(state.product) || result.prices_refreshing !== true || currentPrice(state.product, state.quantity) || retries >= MAX_PRICE_RETRIES) {
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
    if (!state || inquiryOnly(state.product) || state.generation !== generation || !priceActive(node, state) || !visibleDocument()) return;
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
  function quantitySelect(id, value, onChange, label = 'Anzahl') {
    const select = el('select', null, 'shop-quantity');
    select.setAttribute('aria-label', label);
    for (let n = 1; n <= maxQuantity(id); n++) { const option = el('option', n); option.value = String(n); select.append(option); }
    select.value = String(value); select.addEventListener('change', () => onChange(Number(select.value))); return select;
  }
  function productImage(product, cls, lazy = true) {
    const source = Array.isArray(product.images) && product.images.find(url => safeMedia(url));
    if (!source) return el('span', 'Produktbild noch nicht verfügbar', 'shop-no-image');
    const img = el('img', null, cls); img.alt = product.name; if (lazy) img.loading = 'lazy'; img.decoding = 'async';
    setImageSource(img, source); return img;
  }
  function validProduct(product) { return product && typeof product.id === 'string' && PRODUCT.test(product.id) && typeof product.name === 'string' && CATEGORIES.has(product.category); }
  function addToCart(product, quantity, services = {}) {
    if (!validProduct(product)) return;
    const candidate = normaliseCart([{id: product.id, quantity, services}]);
    if (!candidate.length) { notice('Bitte Ihre Auswahl und den Farbcode prüfen.'); return; }
    const row = candidate[0];
    const existing = cart.find(item => item.id === row.id && JSON.stringify(item.services) === JSON.stringify(row.services) && !item.assembly_group && !item.selection_pair);
    if (existing) existing.quantity = Math.min(maxQuantity(row.id), existing.quantity + quantity);
    else if (cart.length < 40) cart.push(row);
    else { notice('Maximal 40 Positionen möglich. Bitte die Auswahl im Warenkorb prüfen.'); return; }
    saveCart(); notice(product.name + (inquiryOnly(product) ? ' ist zur lokalen Anfrage hinzugefügt.' : ' ist im Warenkorb.'));
  }
  function card(product, active) {
    const family = ['alloy_wheel', 'steel_wheel'].includes(product.category) && Number.isSafeInteger(product.family?.variant_count) && product.family.variant_count > 1;
    const configure = body.dataset.page === 'komplettraeder' && ['alloy_wheel', 'steel_wheel'].includes(product.category);
    const view = () => family ? openFamily(product, 1, typeof filters.diameter === 'string' ? filters.diameter : '', configure) : openProduct(product.id, configure);
    const node = el('article', null, 'shop-card'), media = button('', view, 'shop-card-media');
    media.setAttribute('aria-label', product.name + ' ansehen'); media.append(productImage(product, ''));
    node.append(media);
    const imageNote = modelPhotoNote(product); if (imageNote) node.append(el('small', imageNote, 'shop-muted'));
    node.append(el('h3', product.name), el('p', [product.brand, product.size].filter(Boolean).join(' · '), 'shop-card-meta'));
    let count = 1, add = null;
    const price = priceBox(product, count, {active, onUpdate: () => { if (add) { add.disabled = false; add.textContent = inquiryOnly(product) ? 'Zur Anfrage hinzufügen' : 'In den Warenkorb'; } }}), controls = el('div', null, 'shop-card-controls'), label = el('label', 'Anzahl');
    if (family) {
      node.append(el('p', product.family.variant_count + ' Ausführungen' + (Array.isArray(product.family.diameters) ? ' · ' + product.family.diameters.slice(0, 15).join(' / ') + ' Zoll' : ''), 'shop-card-meta'));
      node.append(el('p', 'Preis der gezeigten Ausführung', 'shop-card-meta'));
    }
    if (configure || family) {
      controls.className += ' shop-family-controls';
      controls.append(button(configure ? 'Komplettrad zusammenstellen' : 'Ausführung wählen', view));
    } else {
      label.append(quantitySelect(product.id, count, value => { count = value; changePriceQuantity(price, value); }));
      add = button(inquiryOnly(product) ? 'Zur Anfrage hinzufügen' : 'In den Warenkorb', () => addToCart(product, count));
      controls.append(label, add);
    }
    if (product.department === 'parts') node.append(el('p', 'Lieferzeit wird bestätigt', 'shop-card-meta'));
    node.append(price, controls);
    if (!configure) node.append(button(family ? 'Felge ansehen' : 'Artikel ansehen', view, 'shop-detail-link'));
    return {node, price, product};
  }
  let page = 1, sequence = 0, filters = {}, vehicleFilters = {}, pageTimer = null, pendingPageRefresh = null;
  const visibleCards = new Map();
  function pendingPagePrices() {
    return [...visibleCards.values()].some(({price}) => {
      const state = priceNodes.get(price);
      return state && !inquiryOnly(state.product) && state.generation === 0 && state.quantity === 1 && priceActive(price, state) && !currentPrice(state.product, 1);
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
    const modelPicker = byId('parts-model-refinement');
    if (modelPicker) { modelPicker.parentElement.hidden = true; modelPicker.disabled = true; }
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
      const expectedPage = page, query = buildQuery({...filters, ...vehicleFilters, ...(body.dataset.page === 'komplettraeder' ? {complete: '1'} : {})}, body.dataset.department, page).toString();
      const result = await requestJSON('/products?' + query);
      if (run !== sequence) return;
      if (!Array.isArray(result.items) || result.items.length > 24 || !Number.isInteger(result.total) || result.page !== page) throw new Error('invalid_catalogue');
      const cards = result.items.filter(validProduct).map(product => card(product, () => run === sequence));
      for (const item of cards) visibleCards.set(item.product.id, item);
      grid.replaceChildren(...cards.map(item => item.node));
      if (modelPicker && Array.isArray(result.vehicle_models) && result.vehicle_models.length) {
        const prompt = el('option', 'Baureihe wählen');
        prompt.value = ''; prompt.disabled = true;
        modelPicker.replaceChildren(prompt);
        for (const series of result.vehicle_models.slice(0, 128)) {
          if (typeof series?.make !== 'string' || !series.make || typeof series.model !== 'string' || !series.model) continue;
          const option = el('option', series.model);
          option.value = series.model; option.dataset.make = series.make;
          modelPicker.append(option);
        }
        modelPicker.value = vehicleFilters.model || '';
        if (modelPicker.selectedIndex === -1) modelPicker.selectedIndex = 0;
        modelPicker.parentElement.hidden = modelPicker.options.length < 2 ||
          (modelPicker.options.length === 2 && modelPicker.value !== '');
        modelPicker.disabled = modelPicker.parentElement.hidden;
      }
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
    byId('catalog-filters').addEventListener('reset', () => { filters = {}; page = 1; loadPage(); });
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
        if (event.target.id === 'parts-model-refinement') return;
        const form = event.currentTarget;
        vehicleTimer = root.setTimeout(() => applyVehicle(form), 450);
      });
      byId('vehicle-filters').addEventListener('reset', () => { vehicleFilters = {}; page = 1; loadPage(); });
      byId('parts-model-refinement')?.addEventListener('change', event => {
        const form = byId('vehicle-filters'), option = event.currentTarget.selectedOptions[0];
        if (!option?.value || !option.dataset.make) return;
        form.elements.make.value = option.dataset.make;
        form.elements.model.value = option.value;
        form.requestSubmit();
      });
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
  async function openFamily(representative, variantPage = 1, diameter = '', configure = false) {
    const run = ++detailSequence, dialog = byId('product-dialog'), content = byId('product-detail');
    content.replaceChildren(el('p', 'Ausführungen werden geladen …')); if (!dialog.open) dialog.showModal();
    try {
      const query = new URLSearchParams({page: String(variantPage), page_size: '24'}); if (diameter) query.set('diameter', diameter);
      if (body.dataset.page === 'komplettraeder') query.set('complete', '1');
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
        select.value = diameter; select.addEventListener('change', () => openFamily(representative, 1, select.value, configure)); label.append(select); heading.append(label);
      }
      for (const product of result.items.filter(validProduct)) {
        if (!['alloy_wheel', 'steel_wheel'].includes(product.category)) continue;
        const row = el('article', null, 'shop-family-variant'), description = el('div');
        description.append(el('h3', product.size || product.name), el('p', [product.specifications?.Ausführung, product.article_number && 'Art.-Nr. ' + product.article_number].filter(Boolean).join(' · '), 'shop-muted'));
        const price = priceBox(product, 1, {active: () => run === detailSequence && dialog.open});
        row.append(description, price, button(configure ? 'Passende Reifen wählen' : 'Diese Felge ansehen', () => openProduct(product.id, configure))); grid.append(row);
        Promise.resolve().then(() => scheduleQuantityPrice(price, result, 0));
      }
      const navigation = el('nav', null, 'shop-pagination'), previous = button('← Zurück', () => openFamily(representative, variantPage - 1, diameter, configure), 'shop-button shop-button-light'), next = button('Weiter →', () => openFamily(representative, variantPage + 1, diameter, configure), 'shop-button shop-button-light');
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
      const existing = next.find(item => item.id === row.id && JSON.stringify(item.services) === JSON.stringify(row.services) && item.assembly_group === row.assembly_group && item.selection_pair === row.selection_pair);
      if (existing) { if (existing.quantity + row.quantity > maxQuantity(row.id)) return false; existing.quantity += row.quantity; }
      else next.push(row);
    }
    if (next.length > 40) return false;
    cart = next; saveCart(); notice('Felgen und Reifen sind gemeinsam im Warenkorb.'); return true;
  }
  function bundlePanel(rim, getQuantity, active, configure) {
    const panel = el('section', null, 'shop-bundle-panel'), heading = el('h2', 'Felge + Reifen. Ihre Wahl.');
    panel.append(el('p', 'Komplettrad zusammenstellen', 'shop-eyebrow'), heading);
    const status = el('p', 'Reifen erst auf Ihren Wunsch suchen. Es wird noch nichts in den Warenkorb gelegt.', 'shop-muted'), choices = el('div', null, 'shop-bundle-choices');
    const choicePrices = new Set();
    function clearChoices() {
      for (const box of choicePrices) {
        const state = priceNodes.get(box);
        if (state?.timer != null) root.clearTimeout(state.timer);
        priceNodes.delete(box);
      }
      choicePrices.clear();
      choices.replaceChildren();
    }
    const seasonLabel = el('label', 'Welche Reifen möchten Sie?'), season = el('select', null, 'shop-quantity');
    const seasons = [['summer', 'Sommerreifen'], ['winter', 'Winterreifen'], ['allseason', 'Ganzjahresreifen']].filter(([value]) => Array.isArray(rim.tyre_seasons) && rim.tyre_seasons.includes(value));
    for (const [value, name] of seasons) { const option = el('option', name); option.value = value; season.append(option); }
    season.value = seasons.some(([value]) => value === 'summer') ? 'summer' : seasons[0]?.[0] || '';
    if (!seasons.length) status.textContent = 'Für diese Felgenausführung sind noch keine dokumentierten Reifenvorschläge hinterlegt.';
    seasonLabel.append(season);
    const sizeLabel = el('label', 'Reifengröße für den Vergleich'), size = el('select', null, 'shop-quantity');
    sizeLabel.hidden = true; sizeLabel.append(size);
    function exactSize(value) {
      if (typeof value !== 'string') return false;
      const match = value.match(/^([0-9]{3})\/([0-9]{2,3}) R([0-9]{1,2})$/);
      return !!match && match[0] === value && Number(match[1]) >= 100 && Number(match[1]) <= 455 &&
        Number(match[2]) >= 10 && Number(match[2]) <= 100 && Number(match[3]) >= 8 && Number(match[3]) <= 30;
    }
    function comparisonSize(product) {
      const geometry = product?.geometry;
      if (!geometry || !['width', 'ratio', 'diameter'].every(key => typeof geometry[key] === 'string' && /^[0-9]+(?:\.0+)?$/.test(geometry[key]))) return '';
      const value = Number(geometry.width) + '/' + Number(geometry.ratio) + ' R' + Number(geometry.diameter);
      return exactSize(value) ? value : '';
    }
    function setSizes(result) {
      const provided = Object.hasOwn(result, 'available_sizes') || Object.hasOwn(result, 'selected_size');
      if (!provided) { size.replaceChildren(); size.value = ''; sizeLabel.hidden = true; return false; }
      if (!Array.isArray(result.available_sizes) || result.available_sizes.length > 1000 ||
          !result.available_sizes.every(exactSize) || new Set(result.available_sizes).size !== result.available_sizes.length ||
          !(result.selected_size === null || exactSize(result.selected_size)) ||
          result.items.length && (!result.available_sizes.includes(result.selected_size) ||
            !result.items.every(tyre => validProduct(tyre) && tyre.category === 'tyre' && comparisonSize(tyre) === result.selected_size))) throw new Error('invalid_recommendation_sizes');
      size.replaceChildren();
      for (const value of result.available_sizes) { const option = el('option', value); option.value = value; size.append(option); }
      if (result.selected_size && !result.available_sizes.includes(result.selected_size)) {
        const missing = el('option', result.selected_size + ' – aktuell keine Angebote'); missing.value = result.selected_size; missing.disabled = true; size.append(missing);
      }
      size.value = result.selected_size || ''; sizeLabel.hidden = !result.available_sizes.length && !result.selected_size;
      return true;
    }
    let serial = 0, selected = null, scope = 'blocked';
    const mountingLabel = el('label', null, 'shop-checkbox'), mounting = el('input'); mounting.type = 'checkbox'; mounting.name = 'bundle_mounting';
    mountingLabel.append(mounting, el('span', 'Mit Montage bei uns in Mosbach'));
    const feedback = el('div', null, 'shop-cart-feedback');
    feedback.setAttribute('role', 'status'); feedback.setAttribute('aria-live', 'polite'); feedback.setAttribute('aria-atomic', 'true');
    function clearFeedback() { feedback.replaceChildren(); delete feedback.dataset.kind; }
    function showFeedback(kind, title, text, linkToCart = false) {
      feedback.dataset.kind = kind;
      feedback.replaceChildren(el('strong', title), el('p', text));
      const cartHref = document.querySelector('a.shop-cart-link')?.getAttribute('href');
      if (linkToCart && cartHref === '/shop/warenkorb/') {
        const link = el('a', 'Zum Warenkorb', 'shop-button shop-button-light'); link.href = cartHref; feedback.append(link);
      }
      feedback.scrollIntoView({block: 'nearest'});
    }
    const summary = el('div', null, 'shop-bundle-summary'), add = button('Felgen + Reifen in den Warenkorb', () => {
      clearFeedback();
      if (!selected || !active()) return;
      const quantity = getQuantity();
      if ((!inquiryOnly(rim) && !selectionPrice(rim, quantity)) || (!inquiryOnly(selected) && !selectionPrice(selected, quantity))) {
        updateSummary(); showFeedback('error', 'Noch nicht hinzugefügt', 'Bitte die aktuellen Preise und die gewünschte Anzahl prüfen.'); return;
      }
      const group = 'set|' + rim.id + '|' + selected.id;
      const lines = [{id: rim.id, quantity, services: {}}, {id: selected.id, quantity, services: mounting.checked ? {mounting: true} : {}}];
      for (const line of lines) line[mounting.checked ? 'assembly_group' : 'selection_pair'] = group;
      if (!addLines(lines)) {
        showFeedback('error', 'Nicht hinzugefügt', 'Bitte die vorhandenen Mengen im Warenkorb prüfen. Es wurde nichts hinzugefügt.'); return;
      }
      const added = quantity + (quantity === 1 ? ' Felge + ' : ' Felgen + ') + quantity + ' Reifen hinzugefügt.';
      if (cartStored) showFeedback('success', 'Im Warenkorb', added, true);
      else showFeedback('warning', 'In dieser Ansicht hinzugefügt', added + ' Ihr Browser konnte die Auswahl nicht speichern. Lassen Sie dieses Fenster geöffnet.');
    });
    add.disabled = true;
    function updateSummary() {
      summary.replaceChildren();
      const quantity = getQuantity(), rimPrice = selectionPrice(rim, quantity), tyrePrice = selected && selectionPrice(selected, quantity), inquiry = inquiryOnly(rim) || inquiryOnly(selected);
      add.disabled = !selected || (!inquiryOnly(rim) && !rimPrice) || (!inquiryOnly(selected) && !tyrePrice);
      if (!selected) return;
      summary.append(el('p', quantity + ' × Felge + ' + quantity + ' × Reifen', 'shop-muted'));
      if (inquiry) summary.append(el('p', 'Diese Kombination wird als Gesamtangebot angefragt. Noch keine Bestellung oder Zahlung.', 'shop-inquiry-note'));
      if (rimPrice && tyrePrice && mounting.checked && !inquiry) {
        const output = el('div');
        summary.append(output);
        showMountedTotal(output, rimPrice.total + tyrePrice.total, [{rim, tyre: selected, quantity, services: {mounting: true}}], true);
      } else if (rimPrice && tyrePrice) summary.append(el('strong', money(rimPrice.total + tyrePrice.total), 'shop-summary-total'));
      else if (!inquiry) summary.append(el('p', 'Aktuelle Preise für diese Menge werden noch geprüft.', 'shop-muted'));
      summary.append(el('p', mounting.checked ? 'Abholung in Mosbach. Den Montagetermin vereinbaren wir nach Wareneingang.' : 'Ohne Montage als separate Felgen und Reifen. Versand wird im Warenkorb geprüft.', 'shop-muted'));
      if (scope !== 'approved') summary.append(el('p', 'Größen-/Breitenvorschlag: Fahrzeugfreigabe, Tragfähigkeit und Auflagen sind noch zu prüfen.', 'shop-notice'));
      add.textContent = inquiry ? 'Felgen + Reifen zur Anfrage hinzufügen' : scope === 'approved' ? 'Felgen + Reifen in den Warenkorb' : 'Felgen + Reifen zur Prüfung in den Warenkorb';
    }
    function clear() { serial++; selected = null; clearFeedback(); clearChoices(); summary.replaceChildren(); size.replaceChildren(); size.value = ''; sizeLabel.hidden = true; size.disabled = false; add.disabled = true; search.disabled = !seasons.length; status.textContent = seasons.length ? 'Auswahl geändert. Bitte passende Reifen erneut suchen.' : 'Für diese Felgenausführung sind noch keine dokumentierten Reifenvorschläge hinterlegt.'; }
    async function searchTyres(previousTyre = '') {
      if (!seasons.some(([value]) => value === season.value)) return;
      clearFeedback();
      if (configure) search.hidden = true;
      const run = ++serial, quantity = getQuantity(), requestedSize = size.value; selected = null; clearChoices(); updateSummary(); status.textContent = 'Reifenvorschläge werden geprüft …'; search.disabled = true; size.disabled = true;
      try {
        const query = new URLSearchParams({quantity: String(quantity), season: season.value, ...vehicleFilters});
        if (exactSize(requestedSize)) query.set('tyre_size', requestedSize);
        const result = await requestJSON('/products/' + encodeURIComponent(rim.id) + '/compatible-tyres?' + query);
        if (run !== serial || !active()) return;
        if (result.rim_id !== rim.id || !Array.isArray(result.items) || result.items.length > 5 || !['approved', 'rim_width_candidate', 'blocked'].includes(result.scope)) throw new Error('invalid_recommendations');
        if (exactSize(requestedSize) && result.selected_size !== requestedSize) throw new Error('changed_recommendation_size');
        const comparison = setSizes(result);
        scope = result.scope;
        if (scope === 'blocked' || !result.items.length) { status.textContent = 'Für diese Auswahl liegt noch kein dokumentierter Reifenvorschlag vor. Fahrzeug und genaue Felgenausführung prüfen lassen.'; return; }
        status.textContent = scope === 'approved' ? 'Dokumentierte Reifenvorschläge. Die konkrete Fahrzeugausführung und Auflagen beachten.' : 'Vorschläge nach dokumentiertem Größen-/Breitenabgleich. Die Fahrzeugfreigabe bleibt offen.';
        if (comparison) status.textContent += ' Angebote gleicher Reifengröße nach Preis aufsteigend. Last- und Geschwindigkeitsindex gesondert beachten.';
        for (const tyre of result.items.filter(validProduct)) {
          if (tyre.category !== 'tyre') continue;
          const row = el('label', null, 'shop-bundle-option'), radio = el('input'); radio.type = 'radio'; radio.name = 'bundle_tyre'; radio.value = tyre.id;
          const details = el('div'); details.append(el('h3', tyre.name), el('p', tyre.size, 'shop-muted'));
          const price = priceBox(tyre, quantity, {active: () => run === serial && active(), onUpdate: updateSummary});
          choicePrices.add(price);
          details.append(price);
          const quality = qualityFacts(tyre);
          for (const [label, value] of quality) details.append(el('p', label + ': ' + value, 'shop-card-meta'));
          if (!quality.length) for (const label of ['Nasshaftung', 'Kraftstoffeffizienz', 'Außengeräusch']) if (tyre.specifications?.[label]) details.append(el('p', label + ': ' + tyre.specifications[label], 'shop-card-meta'));
          row.append(radio, productImage(tyre, ''), details); choices.append(row);
          radio.addEventListener('change', () => { if (radio.checked) { clearFeedback(); selected = tyre; updateSummary(); } });
          if (previousTyre === tyre.id) { radio.checked = true; selected = tyre; updateSummary(); }
          scheduleQuantityPrice(price, result, 0);
        }
      } catch (_) {
        if (run === serial && active()) {
          status.textContent = 'Die passenden Reifen sind gerade nicht sicher abrufbar. Bitte später erneut versuchen; es wurde nichts hinzugefügt.';
          if (configure) { search.hidden = false; search.textContent = 'Reifenauswahl erneut laden'; }
        }
      }
      finally { if (run === serial && active()) { search.disabled = false; size.disabled = false; } }
    }
    const search = button('Passende Reifen zu dieser Felge suchen', searchTyres);
    search.hidden = Boolean(configure);
    search.disabled = !seasons.length;
    mounting.addEventListener('change', () => { clearFeedback(); updateSummary(); });
    season.addEventListener('change', () => { clear(); if (configure) searchTyres(); });
    size.addEventListener('change', () => { if (exactSize(size.value)) searchTyres(); });
    panel.append(seasonLabel, sizeLabel, search, status, choices, mountingLabel, summary, add, feedback);
    return {node: panel, search: searchTyres, quantityChanged: () => configure ? searchTyres(selected?.id) : clear(), priceChanged: updateSummary};
  }
  const setPriceRequests = new Map();
  async function readSetPrice({rim, tyre, services, quantity}) {
    const query = new URLSearchParams({tyre_id: tyre.id, quantity: String(quantity)});
    for (const extra of ['runflat', 'disposal']) if (services[extra] === true) query.set(extra, '1');
    const url = '/products/' + encodeURIComponent(rim.id) + '/complete-wheel-price?' + query;
    let entry = setPriceRequests.get(url);
    if (!entry || entry.until <= Date.now()) {
      if (setPriceRequests.size >= 40) setPriceRequests.delete(setPriceRequests.keys().next().value);
      entry = {until: Date.now() + 30000, result: requestJSON(url).catch(() => null)};
      setPriceRequests.set(url, entry);
    }
    const value = await entry.result;
    const rimPrice = selectionPrice(rim, quantity), tyrePrice = selectionPrice(tyre, quantity);
    if (!value || !rimPrice || !tyrePrice || value.rim_id !== rim.id || value.tyre_id !== tyre.id ||
        value.quantity !== quantity || value.currency !== 'EUR' || value.goods_gross_cent !== rimPrice.total + tyrePrice.total ||
        !Number.isSafeInteger(value.mounting_gross_cent) || value.mounting_gross_cent < 0) return null;
    if (!(value.freight_gross_cent === null && value.total_gross_cent === null ||
        Number.isSafeInteger(value.freight_gross_cent) && value.freight_gross_cent >= 0 &&
        value.total_gross_cent === value.goods_gross_cent + value.mounting_gross_cent + value.freight_gross_cent)) return null;
    return value;
  }
  async function showMountedTotal(target, goods, sets, pickupPrice) {
    // Each render owns its nodes. A late reply cannot change a newer quantity or delivery choice.
    const details = el('dl', null, 'shop-cost-lines'), mount = el('dd', 'Wird geprüft'), freight = el('dd', 'Wird geprüft');
    const label = el('p', 'Ware inkl. Montage', 'shop-muted'), total = el('strong', 'Wird geprüft', 'shop-summary-total');
    details.append(el('dt', 'Warenwert'), el('dd', money(goods)), el('dt', 'Montage'), mount, el('dt', 'Versand'), freight);
    target.replaceChildren(details, label, total);
    const prices = await Promise.all(sets.map(readSetPrice));
    if (prices.some(value => !value)) { mount.textContent = 'Im Gesamtangebot'; freight.textContent = 'Im Gesamtangebot'; total.textContent = 'Gesamtpreis wird geprüft'; return; }
    const mounting = prices.reduce((sum, value) => sum + value.mounting_gross_cent, 0);
    mount.textContent = money(mounting);
    const confirmed = pickupPrice && prices.length === 1 && Number.isSafeInteger(prices[0].total_gross_cent);
    freight.textContent = confirmed ? money(prices[0].freight_gross_cent) : 'Wird für die Lieferung bestätigt';
    label.textContent = confirmed ? 'Gesamtpreis inkl. Montage und Versand zur Werkstatt' : 'Zwischensumme inkl. Montage · zzgl. Versand';
    total.textContent = money(confirmed ? prices[0].total_gross_cent : goods + mounting);
  }
  async function openProduct(id, configure = false) {
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
      const imageNote = modelPhotoNote(product); if (imageNote) gallery.append(el('small', imageNote, 'shop-muted'));
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
      const dimensions = rimDimensions(product);
      const details = configure && isRim ? el('details', null, 'shop-rim-details') : copy;
      if (details !== copy) details.append(el('summary', 'Felgendetails, Gutachten und Einzelkauf'));
      if (dimensions.length) {
        const specification = el('dl', null, 'shop-specs');
        for (const [label, value] of dimensions) specification.append(el('dt', label), el('dd', value));
        details.append(el('h3', 'Genaue Felgenausführung'), specification,
          el('p', 'Felgen ohne Montage. Die Fahrzeugpassung wird damit nicht bestätigt.', 'shop-muted'));
      }
      let count = 1, bundle = null, add = null;
      const price = priceBox(product, count, {active: () => run === detailSequence && dialog.open, onUpdate: () => { if (add) { add.disabled = false; add.textContent = inquiryOnly(product) ? 'Zur Anfrage hinzufügen' : isRim ? 'Nur Felge in den Warenkorb' : 'In den Warenkorb'; } bundle?.priceChanged(); }}), form = el('form', null, 'shop-options'); form.addEventListener('submit', e => e.preventDefault());
      const quantityLabel = configure ? 'Anzahl Kompletträder' : 'Anzahl';
      const quantity = el('label', quantityLabel); quantity.append(quantitySelect(id, count, n => { count = n; changePriceQuantity(price, n); bundle?.quantityChanged(); }, quantityLabel)); form.append(quantity);
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
      add = button(inquiryOnly(product) ? 'Zur Anfrage hinzufügen' : isRim ? 'Nur Felge in den Warenkorb' : 'In den Warenkorb', () => {
        const services = {}; for (const flag of FLAGS) { const field = form.elements.namedItem(flag); if (field && field.checked) services[flag] = true; }
        if (services.painting) services.paint_color = form.elements.namedItem('paint_color').value;
        const care = form.elements.namedItem('care'); if (care && care.value) services.care = care.value;
        addToCart(product, count, services);
      });
      copy.append(price, form);
      if (details === copy) copy.append(add);
      else { add.className = 'shop-detail-link'; details.append(add); }
      if (product.description) details.append(el('p', product.description, 'shop-muted'));
      const specs = el('dl', null, 'shop-specs');
      for (const [key, value] of Object.entries(product.specifications || {}).slice(0, 40)) specs.append(el('dt', key), el('dd', value)); details.append(specs);
      if (product.department === 'parts' && Array.isArray(product.application_notes) && product.application_notes.length >= 1 && product.application_notes.length <= 12 &&
          product.application_notes.every(note => typeof note === 'string' && note.length >= 1 && note.length <= 2000)) {
        const notes = el('section', null, 'shop-application-notes'), list = el('ul');
        notes.append(el('h3', 'Hinweise zu diesem Ersatzteil'));
        for (const note of product.application_notes) list.append(el('li', note));
        notes.append(list); copy.append(notes);
      }
      const quality = qualityFacts(product);
      if (quality.length) { const labels = el('dl', null, 'shop-specs'); for (const [label, value] of quality) labels.append(el('dt', label), el('dd', value)); copy.append(el('h3', 'EU-Reifenlabel'), labels); }
      const fitment = el('section', null, 'shop-fitment'); fitment.append(el('h3', product.category === 'tyre' ? 'Ihre Reifenauswahl' : product.department === 'parts' ? 'Bestätigte Fahrzeugzuordnung' : 'Gutachten und Fahrzeugzuordnung'));
      if (isRim) {
        const originals = product.product_documents?.rim_reports;
        if (Array.isArray(originals) && originals.length) {
          fitment.append(el('h4', 'Originalgutachten zu dieser Ausführung'));
          for (const report of originals.slice(0, 48)) {
            if (report.scope !== 'provider_document_not_vehicle_approval' || !safeMedia(report.url, true)) continue;
            const link = el('a', [report.report_number, report.type_description].filter(Boolean).join(' · ') || 'Originalgutachten öffnen ↗');
            link.href = report.url; link.target = '_blank'; link.rel = 'noopener'; fitment.append(link, el('br'));
          }
          fitment.append(el('p', 'Originalunterlagen des Anbieters. Die genaue Felgenausführung, Fahrzeugausführung und Auflagen im Dokument prüfen. Eine bestätigte Fahrzeugzuordnung ist unten gesondert aufgeführt.', 'shop-muted'));
        }
      }
      if (product.category === 'tyre') fitment.append(el('p', TYRE_PURCHASE_NOTE));
      if (product.fitment && (product.fitment.status === 'reviewed_with_conditions' || product.department === 'parts' && product.fitment.status === 'provider_vehicle')) {
        for (const url of (product.fitment.reports || []).filter(url => safeMedia(url, true))) { const link = el('a', 'Gutachten zur Ausführung öffnen ↗'); link.href = url; link.target = '_blank'; link.rel = 'noopener'; fitment.append(link, el('br')); }
        for (const vehicle of (product.fitment.vehicles || []).slice(0, 128)) {
          fitment.append(el('p', [vehicle.label || [vehicle.make, vehicle.model].filter(Boolean).join(' '), vehicle.year_from && vehicle.year_to ? vehicle.year_from + '–' + vehicle.year_to : 'Baujahr nicht belegt · genaue Ausführung im Gutachten prüfen', vehicle.hsn && vehicle.hsn + '/' + vehicle.tsn].filter(Boolean).join(' · ')));
          const list = el('ul'); for (const condition of vehicle.conditions || []) list.append(el('li', condition)); fitment.append(list);
        }
        fitment.append(el('p', product.department === 'parts' ? 'Bestätigte Anbieterzuordnung. Motorisierung, Karosserieausführung und Artikelhinweise vor dem Kauf prüfen.' : 'Auflagen und genaue Ausführung vor der Montage prüfen. Die Zuordnung ersetzt keine Prüfung am Fahrzeug.'));
      } else if (product.category !== 'tyre') fitment.append(el('p', dimensions.length ?
        'Verkauf nach den angegebenen Felgenmaßen. Fahrzeugpassung und Auflagen sind vor der Montage zu prüfen.' :
        'Für diese Ausführung ist hier noch keine bestätigte Fahrzeugzuordnung mit Gutachten hinterlegt. Passung vor einer Bestellung prüfen lassen.'));
      if (product.department === 'parts' || product.department === 'accessories') copy.append(el('p', 'Lieferzeit wird bestätigt. Gemeldeter Bestand ist keine Lieferzusage.', 'shop-muted'));
      details.append(fitment);
      if (isRim) {
        const rimPane = el('section', null, 'shop-rim-pane'); rimPane.append(gallery, copy);
        bundle = bundlePanel(product, () => count, () => run === detailSequence && dialog.open, configure);
        grid.append(rimPane, bundle.node);
        if (details !== copy) grid.append(details);
      } else grid.append(gallery, copy);
      content.replaceChildren(grid);
      dialog.scrollTop = 0;
      if (configure && bundle) bundle.search();
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
        const pair = wheelSetFor(cart, target);
        if (pair && target.selection_pair && mounting.checked) {
          if (cart.some(item => item.assembly_group === target.selection_pair)) {
            status.textContent = 'Dieser Satz mit Montage liegt bereits im Warenkorb. Bitte dort die Menge anpassen.';
            return;
          }
          for (const member of [pair.rim, pair.tyre]) {
            member.assembly_group = member.selection_pair;
            delete member.selection_pair;
          }
        }
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
  function currentSeparateCart() {
    if (!partial) return true;
    try {
      const whole = readCart();
      updateBasketCount(whole);
      if (cart.length && root.sessionStorage.getItem(PARTIAL_KEY + partialToken) === JSON.stringify(selectedSnapshot) &&
          selectedCartRows(whole, selectedSnapshot).length === cart.length) return true;
    } catch (_) { /* Cannot verify the selection. */ }
    if (cart.length) {
      cart = []; revision++; quoteRequestRun++; quoteBusy = false;
      invalidateQuote(); renderCart();
    }
    return false;
  }
  function startSeparateCheckout(items) {
    try {
      const rows = items.map(item => item.selection);
      if (selectedCartRows(readCart(), rows).length !== rows.length) {
        notice('Der Warenkorb wurde geändert. Bitte laden Sie ihn neu.'); return;
      }
      const token = root.crypto.randomUUID();
      root.sessionStorage.setItem(PARTIAL_KEY + token, JSON.stringify(rows));
      root.location.assign(root.location.pathname + '?auswahl=' + token);
    } catch (_) { notice('Der separate Kauf konnte nicht geöffnet werden. Bitte den Warenkorb neu laden.'); }
  }
  function separateChoices() {
    const panel = byId('cart-separate');
    if (!panel) return;
    if (partial) {
      let message = 'Sie bestellen nur die hier angezeigten Artikel. Versand und Gesamtpreis gelten für diese Auswahl.';
      if (!cart.length) message = 'Diese Auswahl ist abgelaufen oder wurde im Warenkorb geändert. Bitte wählen Sie die gewünschten Artikel erneut.';
      else if (cartFlow === 'unavailable' && offerSelection.length === cart.length) message = 'Diese Auswahl ist derzeit nicht bestellbar. Im vollständigen Warenkorb können Sie Ihre Auswahl prüfen.';
      if (panel.dataset.message === message) return;
      panel.dataset.message = message;
      panel.hidden = false;
      const back = el('a', 'Zum vollständigen Warenkorb', 'shop-detail-link'); back.href = root.location.pathname;
      panel.replaceChildren(el('h2', 'Ihre Bestellauswahl'), el('p', message), back);
      return;
    }
    const choices = paymentCheckout && offerSelection.length === cart.length
      ? separatePurchases(offerSelection, body.dataset.deferredCompleteWheelsEnabled === 'true', body.dataset.partsDirectEnabled === 'true') : [];
    panel.hidden = !choices.length;
    const signature = JSON.stringify(choices.map(group => group.map(({selection, product}) => [selection, product.name])));
    if (signature === panel.dataset.choices) return;
    panel.dataset.choices = signature;
    panel.replaceChildren();
    if (!choices.length) return;
    panel.append(el('h2', 'Direkt zur Kasse'), el('p', choices.length > 1
      ? 'Artikel mit technischer Satzprüfung haben einen eigenen Zahlungsablauf. Wählen Sie die gewünschte Bestellung.'
      : 'Diese Positionen können Sie jetzt bestellen. Die übrige Auswahl bleibt im Warenkorb.'));
    for (const group of choices) {
      const list = el('ul');
      for (const {product, selection} of group) list.append(el('li', selection.quantity + ' × ' + product.name));
      const isMounted = Boolean(group[0].selection.assembly_group);
      const label = isMounted ? 'Mit Montage zur Kasse' : group.length > 1 ? 'Artikel zusammen kaufen' : 'Artikel kaufen';
      panel.append(list, button(label, () => startSeparateCheckout(group)));
    }
  }
  function openOfferRequest() {
    if (body.dataset.cartRequestUrl !== '/anfrage?anliegen=allgemein&shop_anfrage=1' || !offerReady || !cartOfferReady(offerSelection)) {
      notice('Die Artikelinformationen werden noch geladen. Bitte gleich erneut versuchen.'); return;
    }
    const value = buildOfferRequest(offerSelection);
    if (!value) { notice('Die Auswahl konnte noch nicht vollständig vorbereitet werden.'); return; }
    try { root.sessionStorage.setItem('gaertner-shop-offer-request-v1', JSON.stringify(value)); }
    catch (_) { notice('Die Auswahl konnte nicht an das Anfrageformular übergeben werden. Bitte die Artikel dort manuell ergänzen.'); return; }
    root.location.assign(body.dataset.cartRequestUrl);
  }
  function updateCartAction() {
    const allLoaded = cart.length > 0 && offerSelection.length === cart.length;
    offerReady = allLoaded && cartOfferReady(offerSelection);
    sourceReady = offerReady && cartSourceReady(offerSelection);
    const partsEnabled = !paymentCheckout || body.dataset.partsDirectEnabled === 'true';
    let nextFlow = allLoaded ? cartPurchaseFlow(offerSelection, body.dataset.deferredCompleteWheelsEnabled === 'true', partsEnabled) : 'unavailable';
    if (partial && (!sourceReady || nextFlow !== 'checkout')) nextFlow = 'unavailable';
    if (allLoaded && nextFlow !== 'checkout' && quote) invalidateQuote();
    cartFlow = nextFlow;
    const form = byId('buyer-address'), primary = byId('request-quote'), note = byId('cart-workflow-note');
    updateMainlandRequirement(form);
    updateVehicleRequirement(form);
    const deliverySalutation = form?.elements.namedItem('delivery_salutation');
    if (deliverySalutation) deliverySalutation.required = paymentCheckout && cartNeedsCustomerDelivery(cart);
    if (form) form.hidden = cartFlow !== 'checkout';
    if (cartFlow !== 'checkout') {
      byId('checkout-button').hidden = true; byId('checkout-button').disabled = true;
      if (cartFlow === 'inquiry') byId('checkout-note').textContent = 'Gesamtangebot wird vor Bezahlung bestätigt. Noch keine Bestellung oder Zahlung.';
    }
    if (note) note.textContent = cartFlow === 'inquiry' ? 'Sie können Ihre gesamte Auswahl auch als Angebot anfragen.' : cartFlow === 'checkout' ? 'Nach Ihrer Anschrift sehen Sie den Gesamtpreis mit Versand und gewählten Leistungen.' : cart.length ? 'Die vollständigen Artikelinformationen werden geprüft.' : '';
    const enterAddress = paymentCheckout && form && !addressReady && buyerSessionReady;
    primary.textContent = cartFlow === 'inquiry' ? 'Gesamtangebot anfragen' : quoteBlocked ? 'Erneut prüfen' : enterAddress ? (cart.length > 1 ? 'Gemeinsam zur Kasse' : 'Zur Kasse') : 'Gesamtpreis prüfen';
    primary.disabled = checkoutInProgress || addressSaving || quoteBusy || cartFlow === 'unavailable' ||
      (cartFlow === 'inquiry' ? !offerReady || body.dataset.cartRequestUrl !== '/anfrage?anliegen=allgemein&shop_anfrage=1' : body.dataset.quoteEnabled !== 'true' || (!addressReady && !enterAddress) || !sourceReady);
    if (!quote && !quoteBlocked && !quoteBusy) {
      byId('quote-status').textContent = cartFlow === 'inquiry' ? 'Noch keine Bestellung oder Zahlung. Wir bestätigen das vollständige Angebot persönlich.' :
        cartFlow === 'unavailable' ? (cart.length ? 'Bitte die Artikelinformationen laden lassen oder nicht zuordenbare Artikel entfernen.' : '') :
        body.dataset.quoteEnabled !== 'true' ? 'Der verbindliche Gesamtpreis ist noch nicht freigeschaltet.' :
        !addressReady ? 'Bitte zuerst Ihre Rechnungs- und Lieferanschrift bestätigen.' : '';
    }
  }
  function offerButton(target, label = 'Warenkorb als Gesamtangebot anfragen') {
    if (body.dataset.cartRequestUrl !== '/anfrage?anliegen=allgemein&shop_anfrage=1' || !offerReady) return;
    target.append(button(label, openOfferRequest, 'shop-button shop-button-light'), el('p', 'Öffnet das editierbare Anfrageformular. Ihre Auswahl ist noch nicht an uns übermittelt.', 'shop-muted'));
  }
  async function renderCart() {
    const run = ++cartRun, container = byId('cart-items'), summary = byId('cart-summary');
    container.replaceChildren(); summary.replaceChildren(); offerSelection = []; sourceReady = false; offerReady = false; byId('cart-status').textContent = cart.length ? 'Artikel werden geprüft …' : partial ? '' : 'Ihr Warenkorb ist noch leer.';
    updateCartAction(); separateChoices();
    if (!cart.length) return;
    let subtotal = 0, complete = true, subtotalExpires = Infinity, subtotalOutput = null, subtotalLabel = null, inquiryNote = null;
    const mountedSummary = el('div'), summaryNote = el('p', 'Versand und gewählte Leistungen werden im Gesamtpreis ausgewiesen.', 'shop-muted');
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
      const inquiry = subtotalItems.some(item => inquiryOnly(item.product));
      updateCartAction(); separateChoices();
      subtotalOutput.textContent = inquiry ? 'Gesamtangebot auf Anfrage' : ready ? money(value) : 'Preise werden geprüft';
      if (subtotalLabel) subtotalLabel.textContent = 'Artikel-Zwischensumme';
      if (inquiryNote) inquiryNote.hidden = !inquiry;
      if (ready) subtotalOutput.dataset.subtotalExpires = String(until);
      else delete subtotalOutput.dataset.subtotalExpires;
      const sets = ready && !inquiry ? completeWheelGroups(offerSelection) : null;
      mountedSummary.hidden = !sets;
      subtotalOutput.hidden = Boolean(sets); subtotalLabel.hidden = Boolean(sets); summaryNote.hidden = Boolean(sets);
      if (sets) {
        const prices = sets.map(set => {
          const members = offerSelection.filter(item => item.selection.assembly_group === set.group);
          const tyre = members.find(item => item.product.id === set.tyreId);
          return {rim: members.find(item => item.product.id === set.rimId).product, tyre: tyre.product, quantity: set.quantity, services: tyre.selection.services};
        });
        const delivery = byId('buyer-address')?.elements.namedItem('workshop_delivery')?.value || 'collection';
        showMountedTotal(mountedSummary, value, prices, delivery === 'collection');
      }
    }
    // At most 40 exact public details, in batches of four; never fetch the feed.
    for (let offset = 0; offset < cart.length; offset += 4) {
      const rows = cart.slice(offset, offset + 4);
      const results = await Promise.all(rows.map(async row => { try { return await requestJSON('/products/' + encodeURIComponent(row.id) + '?quantity=' + row.quantity); } catch (_) { return null; } }));
      if (run !== cartRun) return;
      rows.forEach((row, i) => {
        const result = results[i], product = result && result.item, index = offset + i, article = el('article', null, 'shop-cart-item'), copy = el('div');
        const wheelSet = wheelSetFor(cart, row);
        offerSelection.push({product: validProduct(product) && product.id === row.id ? product : null, selection: row});
        let pendingPrice = null;
        if (!validProduct(product) || product.id !== row.id) { complete = false; unavailable = true; copy.append(el('h3', 'Artikel nicht mehr verfügbar'), el('p', 'Bitte aus der Auswahl entfernen.', 'shop-muted')); article.append(el('div'), copy); }
        else {
          const price = priceBox(product, row.quantity, {active: () => run === cartRun, onUpdate: refreshSubtotal});
          pendingPrice = price;
          article.append(productImage(product, '')); copy.append(el('h3', product.name), el('p', product.size, 'shop-muted'), price);
          const dimensions = rimDimensions(product);
          if (dimensions.length) {
            copy.append(el('p', 'Ausführung: ' + dimensions.map(([label, value]) => label + ': ' + value).join(' · '), 'shop-card-meta'));
            if (!row.assembly_group && !Object.keys(row.services).length) copy.append(
              el('p', 'Felgen ohne Montage. Die Fahrzeugpassung wird damit nicht bestätigt.', 'shop-muted'));
          }
          subtotalItems.push({product, quantity: row.quantity});
          const current = selectionPrice(product, row.quantity); if (current) { subtotal += current.total; subtotalExpires = Math.min(subtotalExpires, current.expires); } else complete = false;
          const service = serviceDescription(row); if (service) copy.append(el('p', service, 'shop-muted'));
          copy.append(el('p', service || row.assembly_group ? 'Lieferziel zur Prüfung: Werkstatt Mosbach' : 'Lieferziel zur Prüfung: Kundenanschrift', 'shop-muted'));
          if (product.category === 'tyre') copy.append(el('p', TYRE_PURCHASE_NOTE, 'shop-muted'));
          if (!partial && (product.category === 'tyre' || (row.assembly_group && !wheelSet))) {
            const deferredMontage = row.assembly_group && body.dataset.deferredCompleteWheelsEnabled === 'true';
            const action = el('div', null, 'shop-mounting-action'); action.append(button(deferredMontage ? 'Montage nach Wareneingang abstimmen' : 'Montagetermin in Mosbach buchen', () => mountingRequest(row, product), 'shop-button shop-button-secondary'), el('small', deferredMontage ? 'Montage ist ausgewählt. Terminvereinbarung nach Wareneingang.' : 'Terminbestätigung nach Lieferklärung', 'shop-muted')); copy.append(action);
          }
          article.append(copy);
        }
        if (partial) copy.append(el('p', 'Anzahl: ' + row.quantity, 'shop-muted'));
        else if (!wheelSet || wheelSet.rim === row) {
          const controls = el('div', null, 'shop-cart-controls'), name = wheelSet ? (row.assembly_group ? 'Anzahl Kompletträder' : 'Anzahl je Artikel') : 'Anzahl', quantity = el('label', name);
          const select = quantitySelect(row.id, row.quantity, n => {
            if (run !== cartRun || !Number.isInteger(n) || n < 1 || n > maxQuantity(row.id)) return;
            row.quantity = n;
            if (wheelSet) wheelSet.tyre.quantity = n;
            saveCart(); renderCart();
          }, name);
          if (wheelSet && wheelSet.rim.quantity !== wheelSet.tyre.quantity) {
            const choose = el('option', 'Gemeinsame Menge wählen'); choose.value = ''; choose.disabled = true;
            select.append(choose); select.value = '';
          }
          quantity.append(select);
          controls.append(quantity, button(wheelSet ? 'Satz entfernen' : 'Entfernen', () => {
            const next = wheelSet ? cart.filter(item => item !== wheelSet.rim && item !== wheelSet.tyre) : removeCartLine(cart, index);
            if (!next) { notice('Die verbleibende Auswahl überschneidet sich mit einem anderen Artikel. Bitte zuerst die Mengen oder eine der anderen Positionen anpassen.'); return; }
            cart = next; saveCart(); renderCart();
          }, 'shop-detail-link'));
          if (wheelSet) controls.append(button('Satz aufteilen', () => {
            const next = cart.map(item => {
              if (item !== wheelSet.rim && item !== wheelSet.tyre) return item;
              const {assembly_group, selection_pair, ...single} = item;
              return single;
            });
            if (normaliseCart(next).length !== next.length) { notice('Die Einzelartikel sind bereits im Warenkorb. Bitte zuerst die vorhandenen Positionen prüfen.'); return; }
            cart = next; saveCart(); renderCart();
          }, 'shop-detail-link'));
          copy.append(controls);
        } else copy.append(el('p', row.quantity + (row.assembly_group ? ' Reifen im Komplettradsatz' : ' Reifen zur gewählten Felge'), 'shop-muted'));
        container.append(article);
        if (pendingPrice) scheduleQuantityPrice(pendingPrice, result, 0);
      });
    }
    const total = el('p', complete ? money(subtotal) : 'Preise werden geprüft', 'shop-summary-total');
    subtotalOutput = total;
    if (complete) total.dataset.subtotalExpires = String(subtotalExpires);
    subtotalLabel = el('p', 'Artikel-Zwischensumme', 'shop-muted'); inquiryNote = el('p', 'Anfrageartikel bleiben in Ihrer Auswahl. Preis und Lieferbarkeit bestätigen wir im Gesamtangebot.', 'shop-inquiry-note'); inquiryNote.hidden = true;
    summary.append(subtotalLabel, total, inquiryNote, summaryNote, mountedSummary);
    refreshSubtotal();
    byId('cart-status').textContent = '';
    updateCartAction();
  }
  async function buyerSession() {
    const requestedRevision = revision;
    const result = await requestJSON('/buyer/session');
    if (typeof result.csrf_token !== 'string' || !/^[A-Za-z0-9_\-.:]{16,256}$/.test(result.csrf_token) || typeof result.address_ready !== 'boolean') throw new Error('invalid_session');
    csrf = result.csrf_token;
    if (requestedRevision === revision) { restoreWorkshopDelivery(result); addressReady = result.address_ready && !partial; updateVehicleRequirement(byId('buyer-address')); }
    buyerSessionReady = true;
    return result;
  }
  function restoreWorkshopDelivery(result) {
    const value = result.workshop_delivery || 'collection';
    if (!['collection', 'customer_reshipment'].includes(value)) throw new Error('invalid_session');
    confirmedWorkshopDelivery = value;
    const field = byId('buyer-address')?.elements.namedItem('workshop_delivery');
    if (field) field.value = value;
    const form = byId('buyer-address');
    if (body.dataset.mainlandDeliveryEnabled === 'true') {
      if (result.delivery_mainland !== undefined && typeof result.delivery_mainland !== 'boolean') throw new Error('invalid_session');
      const mainland = form?.elements.namedItem('delivery_mainland');
      if (mainland) mainland.checked = result.delivery_mainland === true;
    }
    if (body.dataset.deferredCompleteWheelsEnabled === 'true') {
      const vehicle = form?.elements.namedItem('vehicle_ref');
      if (!vehicle || result.vehicle_ref !== undefined && vehicleReference(result.vehicle_ref) === null) throw new Error('invalid_session');
      vehicle.value = result.vehicle_ref === undefined ? '' : vehicleReference(result.vehicle_ref);
    }
    if (body.dataset.partsDirectEnabled === 'true') {
      const choice = form?.elements.namedItem('parts_vehicle_ref');
      if (!choice || result.vehicle_ref !== undefined && vehicleReference(result.vehicle_ref) === null) throw new Error('invalid_session');
      choice.value = partsVehicleReference(result.vehicle_ref, partsVehicleChoices(form)) || '';
    }
    updateMainlandRequirement(form);
    updateVehicleRequirement(form);
  }
  function updateMainlandRequirement(form) {
    if (!form || body.dataset.mainlandDeliveryEnabled !== 'true') return;
    const field = form.elements.namedItem('delivery_mainland'), region = byId('mainland-delivery-confirmation');
    if (!field || !region) return;
    const required = cartNeedsCustomerDelivery(cart, form.elements.namedItem('workshop_delivery')?.value || 'collection');
    field.required = required; field.disabled = !required; region.hidden = !required;
  }
  function updateVehicleRequirement(form) {
    if (!form) return;
    const parts = body.dataset.partsDirectEnabled === 'true' && cartNeedsPartsVehicle(offerSelection);
    if (body.dataset.partsDirectEnabled === 'true') {
      const field = form.elements.namedItem('parts_vehicle_ref'), region = byId('parts-vehicle-choice');
      if (region) region.hidden = !parts;
      if (field) { field.required = parts; field.disabled = !parts || !partsVehicleChoices(form).length; }
      if (parts && !partsVehicleReference(field?.value, partsVehicleChoices(form))) addressReady = false;
    }
    if (body.dataset.deferredCompleteWheelsEnabled === 'true') {
      const field = form.elements.namedItem('vehicle_ref'), region = byId('complete-wheel-vehicle');
      if (!field || !region) return;
      const required = !parts && Boolean(completeWheelGroups(offerSelection));
      field.required = required; region.hidden = !required;
      if (required && (vehicleReference(field.value) === null || vehicleReference(field.value).length < 3)) addressReady = false;
    }
  }
  function partsVehicleChoices(form) {
    const field = form?.elements.namedItem('parts_vehicle_ref');
    if (!field || field.tagName !== 'SELECT' || !field.options) return [];
    const values = Array.from(field.options).map(option => option.value).filter(Boolean);
    return values.length <= 512 && values.every(value => /^alzura-vehicle:[1-9][0-9]{0,8}$/.test(value)) && new Set(values).size === values.length ? values : [];
  }
  function currentDeliverySignature(form) {
    const values = {};
    for (const key of ['recipient', 'street', 'postcode', 'city', 'delivery_recipient', 'delivery_street', 'delivery_postcode', 'delivery_city']) values[key] = form.elements.namedItem(key)?.value || '';
    values.different_delivery = form.elements.namedItem('different_delivery')?.checked === true;
    return deliveryAddressSignature(values);
  }
  function addressPayload(form) {
    const data = new FormData(form), get = key => String(data.get(key) || '').trim();
    const billing = {recipient: get('recipient'), street: get('street'), postcode: get('postcode'), city: get('city'), country: 'DE'};
    const contact = {name: get('name'), email: get('email')}; if (get('phone')) contact.phone = get('phone');
    let delivery = data.get('different_delivery') ? {recipient: get('delivery_recipient'), street: get('delivery_street'), postcode: get('delivery_postcode'), city: get('delivery_city'), country: 'DE'} : null;
    const salutationChoice = get('delivery_salutation');
    const salutation = salutationChoice === 'personal_offer' ? '' : salutationChoice;
    if (salutation && !['Herr', 'Frau', 'Firma'].includes(salutation)) throw new Error('invalid_address');
    if (salutation) delivery = {...(delivery || billing), salutation};
    const payload = {contact, billing, delivery}, workshopDelivery = get('workshop_delivery');
    if (workshopDelivery) {
      if (!['collection', 'customer_reshipment'].includes(workshopDelivery)) throw new Error('invalid_address');
      payload.workshop_delivery = workshopDelivery;
    }
    if (body.dataset.mainlandDeliveryEnabled === 'true') {
      const field = form.elements.namedItem('delivery_mainland');
      if (!field || field.type !== 'checkbox' || typeof field.checked !== 'boolean') throw new Error('invalid_address');
      payload.delivery_mainland = field.checked;
    }
    if (body.dataset.partsDirectEnabled === 'true' && cartNeedsPartsVehicle(offerSelection)) {
      const field = form.elements.namedItem('parts_vehicle_ref');
      const value = partsVehicleReference(field?.value, partsVehicleChoices(form));
      if (!value) throw new Error('parts_vehicle_required');
      payload.vehicle_ref = value;
    } else if (body.dataset.deferredCompleteWheelsEnabled === 'true') {
      const field = form.elements.namedItem('vehicle_ref'), value = field && vehicleReference(field.value);
      if (!field || value === null || completeWheelGroups(offerSelection) && value.length < 3) throw new Error('invalid_address');
      payload.vehicle_ref = value;
    } else if (body.dataset.partsDirectEnabled === 'true') {
      payload.vehicle_ref = '';
    }
    return payload;
  }
  const BLOCKED = {mixed_cart_manual_review_required: 'Für gemischte Warenkörbe bestätigen wir vor der Zahlung das vollständige Gesamtangebot.', fitment_unconfirmed: 'Fahrzeugpassung noch nicht bestätigt', rsu_mapping_unconfirmed: 'Lieferantenartikel noch nicht sicher abgeglichen', freight_unconfirmed: 'Versandkosten noch nicht bestätigt', assembly_fitment_unconfirmed: 'Rad-Reifen-Kombination noch nicht freigegeben', service_unconfirmed: 'Servicepreis noch nicht bestätigt', stock_insufficient_or_unconfirmed: 'Bestand für diese Menge noch nicht bestätigt', procurement_price_unconfirmed: 'Lieferantenpreis noch nicht bestätigt'};
  Object.assign(BLOCKED, {
    product_inquiry_only: 'Dieser Artikel ist nur auf Anfrage erhältlich.',
    delivery_salutation_required: 'Für den Direktversand bitte eine Lieferanrede wählen oder ein persönliches Versandangebot anfragen.',
    product_documents_unconfirmed: 'Die erforderlichen Originalunterlagen für diesen Reifen sind noch nicht vollständig bestätigt.'
  });
  async function consumerTerms(frozenQuote) {
    const checkout = byId('checkout-button'); checkout.hidden = true; checkout.disabled = true; consumerState = null;
    if (body.dataset.consumerEnabled !== 'true') { byId('checkout-note').textContent = 'Der Abschluss ist noch nicht freigeschaltet. Vertragsunterlagen und Kundenbestätigung müssen zuerst verfügbar sein.'; return; }
    const run = ++consumerRun, terms = await requestJSON('/consumer/terms');
    if (run !== consumerRun || quote !== frozenQuote || !retained) return;
    const documents = {agb: 'Allgemeine Geschäftsbedingungen', widerruf: 'Widerrufsbelehrung', widerrufsformular: 'Muster-Widerrufsformular', lieferung: 'Lieferung und Leistungen', datenschutz: 'Datenschutzhinweise'};
    if (frozenQuote.version === 2) documents.komplettraeder_zahlung = 'Kompletträder: Kartenreservierung und technische Prüfung';
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
    checkout.textContent = frozenQuote.version === 2 ? 'Gesamtbetrag online reservieren' : 'Gesamtpreis bezahlen';
  }
  async function showQuote(result) {
    const value = result.quote, review = byId('quote-review');
    if (!value || typeof value.ready !== 'boolean' || value.currency !== 'eur' || !Array.isArray(value.lines) || !Array.isArray(value.shipments)) throw new Error('invalid_quote');
    if (value.ready && !quoteMatchesSelection(value.lines, offerSelection)) throw new Error('invalid_quote');
    const reviewPayment = value.version === 2;
    if (reviewPayment && !reviewQuoteReady(value, offerSelection, body.dataset.deferredCompleteWheelsEnabled === 'true')) throw new Error('invalid_quote');
    if (value.version !== undefined && value.version !== 1 && value.version !== 2) throw new Error('invalid_quote');
    review.replaceChildren();
    const lines = el('dl');
    for (const line of value.lines) {
      lines.append(el('dt', line.quantity + ' × ' + line.name), el('dd', money(line.items_gross_cent)));
      for (const service of line.services || []) lines.append(el('dt', ({mounting: 'Montage', painting: 'Lackierung', oil_service: 'Ölservice', installation: 'Einbau', care: 'Aufbereitung'})[service.code] || 'Erforderliches Zubehör'), el('dd', money(service.gross_cent)));
    }
    const reshipment = confirmedWorkshopDelivery === 'customer_reshipment';
    for (const shipment of value.shipments) lines.append(el('dt', shipment.destination === 'workshop' ? reshipment ? 'Versand zur Werkstatt und anschließend zum Kunden' : 'Versand zur Werkstatt · Abholung' : 'Direktversand zum Kunden'), el('dd', money(shipment.gross_cent)));
    review.append(lines);
    if (value.ready === true && result.quote_retained === true && Number.isSafeInteger(value.total_gross_cent) && value.total_gross_cent > 0 && /^[a-f0-9]{64}$/.test(value.quote_hash) && Date.parse(value.expires_at) > Date.now()) {
      quote = value; retained = true; review.append(el('p', 'Gesamtpreis inkl. MwSt.', 'shop-muted'), el('p', money(value.total_gross_cent), 'shop-summary-total'));
      byId('quote-status').textContent = reviewPayment ? 'Gesamtpreis einschließlich Montage und Versand bestätigt. Die technische Prüfung steht noch aus.' : 'Artikel, Leistungen und Versand wurden für diese Auswahl geprüft.';
      const reservation = reviewPayment
        ? 'Der vollständige Gesamtbetrag wird zunächst auf Ihrer Karte reserviert. Erst nach technischer Prüfung und vollständiger Lieferantenbestätigung buchen wir ab.'
        : 'Wir reservieren den Gesamtbetrag einmal. Sobald alle Artikel bestätigt sind, buchen wir ihn ab.';
      const includesMounting = offerSelection.some(item => item.selection.services.mounting === true);
      byId('checkout-note').textContent = reservation + (includesMounting ? ' Den Montagetermin vereinbaren wir nach Wareneingang.' : '');
      if (reviewPayment) {
        review.append(el('p', byId('checkout-note').textContent, 'shop-review-note'), el('p', 'Sicher mit Karte. Apple Pay erscheint im Zahlungsfenster, wenn Ihr Gerät und Konto es unterstützen.', 'shop-muted'));
        const pending = el('ul', null, 'shop-review-requirements'), descriptions = {
          mixed_cart_manual_review_required: 'Vollständige Kombination und Lieferbarkeit werden geprüft.',
          fitment_unconfirmed: 'Fahrzeugpassung und Felgengutachten werden geprüft.',
          assembly_fitment_unconfirmed: 'Die genaue Rad-Reifen-Kombination wird technisch geprüft.'
        };
        for (const text of new Set(value.review_requirements.map(item => descriptions[item.code]))) pending.append(el('li', text));
        review.append(pending);
      }
      await consumerTerms(value);
    } else {
      invalidateQuote(); quoteBlocked = true; review.append(lines);
      if (Number.isSafeInteger(value.known_subtotal_gross_cent) && value.known_subtotal_gross_cent >= 0) review.append(el('p', 'Bekannte Zwischensumme · noch kein Gesamtangebot', 'shop-muted'), el('p', money(value.known_subtotal_gross_cent), 'shop-summary-total'));
      review.append(el('p', 'Ein verbindlicher Gesamtpreis ist noch nicht möglich.', 'shop-review-note'));
      const list = el('ul', null, 'shop-errors'), reasons = new Set();
      for (const item of value.blocked_reasons || []) reasons.add(BLOCKED[item.code] || 'Ein Preis-, Service- oder Lieferdetail muss noch bestätigt werden.');
      for (const reason of reasons) list.append(el('li', reason)); review.append(list);
      const mixed = (value.blocked_reasons || []).some(item => item.code === 'mixed_cart_manual_review_required');
      offerButton(review, mixed ? 'Gemischten Warenkorb als Gesamtangebot anfragen' : 'Auswahl als persönliches Angebot anfragen');
      byId('quote-status').textContent = 'Noch keine Zahlung. Das vollständige Angebot muss zuerst bestätigt werden.';
      byId('checkout-note').textContent = 'Es wurde keine Bestellung angelegt. Sie können die Auswahl als Gesamtangebot anfragen oder die Prüfung erneut starten.';
    }
    updateCartAction();
    review.scrollIntoView?.({block:'start',behavior:'smooth'});
  }
  if (byId('cart-items')) {
    function syncStoredCart() {
      if (partial) { currentSeparateCart(); return; }
      try {
        const latest = readCart();
        if (JSON.stringify(cart) === JSON.stringify(latest)) return;
        cart = latest; revision++; quoteRequestRun++; quoteBusy = false;
        invalidateQuote(); updateBasketCount(cart); renderCart();
      } catch (_) { /* Keep the in-memory cart when storage is unavailable. */ }
    }
    root.addEventListener?.('storage', event => { if (event.key === CART_KEY || event.key === null) syncStoredCart(); });
    root.addEventListener?.('pageshow', event => { if (event.persisted) syncStoredCart(); });
    renderCart();
    if (body.dataset.buyerEnabled === 'true') buyerSession().then(() => renderCart()).catch(error => { byId('address-status').textContent = errorText(error); });
    if (byId('buyer-address')) {
      const form = byId('buyer-address');
      let previousDelivery = currentDeliverySignature(form);
      const resetChangedDelivery = () => {
        if (body.dataset.mainlandDeliveryEnabled !== 'true') return;
        const current = currentDeliverySignature(form);
        if (current !== previousDelivery) {
          const field = form.elements.namedItem('delivery_mainland'); if (field) field.checked = false;
          previousDelivery = current;
        }
      };
      const addressChanged = () => { resetChangedDelivery(); revision++; quoteRequestRun++; quoteBusy = false; invalidateQuote(); addressReady = false; updateCartAction(); };
      form.elements.namedItem('different_delivery').addEventListener('change', event => { byId('delivery-fields').hidden = !event.target.checked; byId('delivery-fields').disabled = !event.target.checked; addressChanged(); });
      form.elements.namedItem('workshop_delivery')?.addEventListener('change', () => { addressChanged(); renderCart(); });
      form.addEventListener('input', addressChanged);
      form.addEventListener('change', addressChanged);
      form.addEventListener('submit', async event => {
        event.preventDefault();
        if (!currentSeparateCart()) return;
        if (cartFlow !== 'checkout' || !sourceReady || !offerReady || addressSaving || quoteBusy || checkoutInProgress) return;
        addressSaving = true; revision++; quoteRequestRun++; quoteBusy = false;
        invalidateQuote(); addressReady = false; updateCartAction(); byId('address-status').textContent = 'Anschrift wird geprüft …';
        const submittedRevision = revision;
        try {
          const result = await requestJSON('/buyer/address', {method: 'PUT', body: JSON.stringify(addressPayload(form))});
          if (result.address_ready !== true || typeof result.csrf_token !== 'string') throw new Error('invalid_session');
          if (submittedRevision !== revision) { byId('address-status').textContent = 'Die Anschrift wurde während der Prüfung geändert. Bitte erneut bestätigen.'; return; }
          csrf = result.csrf_token; restoreWorkshopDelivery(result); buyerSessionReady = true; addressReady = true; updateVehicleRequirement(form);
          byId('address-status').textContent = addressReady ? 'Anschrift für diese Bestellung gespeichert.' : 'Bitte das genaue Fahrzeug auswählen und die Anschrift erneut bestätigen.';
          await renderCart();
        } catch (error) { byId('address-status').textContent = errorText(error); return; }
        finally { addressSaving = false; updateCartAction(); }
        if (paymentCheckout && submittedRevision === revision) await requestCartQuote();
      });
    }
    async function requestCartQuote() {
      if (!currentSeparateCart()) return;
      if (cartFlow !== 'checkout' || body.dataset.quoteEnabled !== 'true' || !addressReady || !cart.length || !sourceReady || !offerReady || quoteBusy || checkoutInProgress) return;
      invalidateQuote(); quoteBusy = true; const quoteRun = ++quoteRequestRun;
      updateCartAction(); byId('quote-status').textContent = 'Gesamtpreis und Lieferwege werden geprüft …';
      const submittedRevision = revision;
      try {
        const submitted = JSON.stringify({lines: checkoutLines(cart)});
        const result = await requestJSON('/cart/quote', {method: 'POST', body: submitted});
        if (quoteRun !== quoteRequestRun || submittedRevision !== revision || !addressReady) return;
        await showQuote(result);
      }
      catch (error) {
        if (quoteRun !== quoteRequestRun || submittedRevision !== revision) return;
        invalidateQuote(); quoteBlocked = true; byId('quote-status').textContent = errorText(error);
      }
      finally { if (quoteRun === quoteRequestRun) { quoteBusy = false; updateCartAction(); } }
    }
    byId('request-quote').addEventListener('click', () => {
      if (!currentSeparateCart() || byId('request-quote').disabled) return;
      if (cartFlow === 'inquiry') { openOfferRequest(); return; }
      if (!addressReady) {
        const form = byId('buyer-address');
        form.scrollIntoView?.({block: 'start', behavior: 'smooth'});
        const field = form.querySelector?.('input:invalid, select:invalid') || form.elements.namedItem('name');
        field?.focus?.({preventScroll: true}); return;
      }
      return requestCartQuote();
    });
    byId('checkout-button').addEventListener('click', async () => {
      if (!currentSeparateCart()) return;
      if (cartFlow !== 'checkout' || body.dataset.orderEnabled !== 'true' || body.dataset.checkoutEnabled !== 'true' || body.dataset.consumerEnabled !== 'true' || !quote || !retained || !addressReady || !sourceReady || !offerReady || checkoutInProgress || !consumerState || consumerState.quoteHash !== quote.quote_hash || !consumerState.consumer.checked || !consumerState.accept.checked) return;
      if (Date.parse(quote.expires_at) <= Date.now() || quote.version === 2 && !reviewQuoteReady(quote, offerSelection, body.dataset.deferredCompleteWheelsEnabled === 'true')) { invalidateQuote(); byId('quote-status').textContent = 'Das Angebot ist abgelaufen oder wurde geändert. Bitte den Gesamtpreis erneut prüfen.'; return; }
      const frozenQuote = quote, hash = frozenQuote.quote_hash, submittedRevision = revision, pack = consumerState.packHash; checkoutInProgress = true; byId('checkout-button').disabled = true; byId('quote-status').textContent = 'Ihre geprüfte Auswahl wird übernommen …';
      let orderId = null;
      try {
        const acceptance = await requestJSON('/consumer/acceptance', {method: 'POST', body: JSON.stringify({quote_hash: hash, pack_hash: pack, terms_accepted: true, consumer: true})});
        if (!acceptance || acceptance.accepted !== true || acceptance.quote_hash !== hash || acceptance.pack_hash !== pack || !Number.isFinite(Date.parse(acceptance.accepted_at))) throw new Error('terms_acceptance_required');
        if (!currentSeparateCart() || submittedRevision !== revision || quote !== frozenQuote || !addressReady || cartFlow !== 'checkout' || !sourceReady) throw new Error('terms_changed_new_quote_required');
        const keyName = 'shop-order-key:' + hash; let key;
        try { key = root.sessionStorage.getItem(keyName); } catch (_) { /* memory fallback */ }
        if (!key) { key = 'shop-' + root.crypto.randomUUID(); try { root.sessionStorage.setItem(keyName, key); } catch (_) { /* server idempotency still binds this attempt */ } }
        const draft = await requestJSON('/orders', {method: 'POST', body: JSON.stringify({quote_hash: hash, idempotency_key: key})});
        if (!draft.order || !/^sho_[a-f0-9]{32}$/.test(draft.order.id)) throw new Error('order_unavailable');
        orderId = draft.order.id;
        if (!currentSeparateCart() || submittedRevision !== revision || quote !== frozenQuote || cartFlow !== 'checkout' || !sourceReady) throw new Error('terms_changed_new_quote_required');
        if (draft.payment_enabled !== true || draft.order.quote_hash !== hash || draft.order.amount_gross_cent !== frozenQuote.total_gross_cent) throw new Error('checkout_unavailable');
        try {
          const record = partial ? {hash, token: partialToken} : {hash, rows: cart};
          root.sessionStorage.setItem(PARTIAL_KEY + orderId, JSON.stringify(record));
        } catch (_) { /* A disabled browser store must not interrupt payment. */ }
        const result = await requestJSON('/orders/' + orderId + '/checkout', {method: 'POST', body: '{}'});
        if (!result.checkout || result.checkout.order_id !== orderId || result.checkout.currency !== 'eur' || result.checkout.amount_gross_cent !== frozenQuote.total_gross_cent || !safeCheckout(result.checkout.checkout_url) || result.checkout.capture_triggered !== false || result.checkout.supplier_order_created !== false) throw new Error('checkout_unavailable');
        // Remember consent only; this is not a conversion or a payment confirmation.
        try { root.gaertnerAdsRememberCheckout?.(orderId); } catch (_) { /* Measurement must not block payment navigation. */ }
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
        if (order.status === 'paid' || order.status === 'authorized') {
          try {
            const record = JSON.parse(root.sessionStorage.getItem(PARTIAL_KEY + id));
            if (record && /^[a-f0-9]{64}$/.test(record.hash) && record.hash === order.quote_hash) {
              const hasToken = TOKEN.test(record.token);
              const rows = hasToken ? JSON.parse(root.sessionStorage.getItem(PARTIAL_KEY + record.token)) : record.rows;
              const latest = readCart(), selected = selectedCartRows(latest, rows, false);
              if (selected.length) {
                const remaining = latest.filter(row => !selected.includes(row));
                root.localStorage.setItem(CART_KEY, JSON.stringify(remaining)); updateBasketCount(remaining);
              }
              if (hasToken) root.sessionStorage.removeItem(PARTIAL_KEY + record.token);
              root.sessionStorage.removeItem(PARTIAL_KEY + id);
            }
          } catch (_) { /* Checkout status remains authoritative even if browser storage is unavailable. */ }
        }
        const technical = result.technical_review;
        if (technical && technical.required === true && ['pending', 'approved', 'rejected', 'expired'].includes(technical.state)) {
          const reviewStates = {pending: 'Die technische Prüfung von Fahrzeugpassung, Gutachten und Lieferbarkeit läuft. Eine Kartenreservierung ist noch keine Abbuchung.', approved: 'Die technische Prüfung ist abgeschlossen. Die Abbuchung erfolgt erst nach vollständiger Lieferantenbestätigung.', rejected: 'Die Zusammenstellung konnte technisch nicht bestätigt werden. Die Kartenreservierung wird freigegeben.', expired: 'Die Prüfungsfrist ist abgelaufen. Die Kartenreservierung wird freigegeben.'};
          summary.append(el('p', technical.state === 'approved' && order.status === 'paid' ? 'Technische Prüfung abgeschlossen.' : reviewStates[technical.state], 'shop-review-note'));
        }
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
  if (body.dataset.page === 'home') {
    for (const photo of document.querySelectorAll('.shop-category-tile img')) {
      const source = photo.getAttribute('src');
      if (!photo.naturalWidth && safeMedia(source)) setImageSource(photo, source);
    }
  }
  if (body.dataset.page === 'komplettraeder') {
    const linked = new URLSearchParams(root.location.search).get('artikel');
    if (typeof linked === 'string' && /^(alloy_wheel|steel_wheel):[a-f0-9]{24}$/.test(linked)) openProduct(linked, true);
  }
  root.setInterval(() => {
    for (const [node, item] of priceNodes) { if (!node.isConnected) priceNodes.delete(node); else priceChanged(node, item); }
    if (quote && Date.parse(quote.expires_at) <= Date.now()) { invalidateQuote(); if (byId('quote-status')) byId('quote-status').textContent = 'Das Angebot ist abgelaufen. Bitte den Gesamtpreis erneut prüfen.'; }
    document.querySelectorAll('[data-subtotal-expires]').forEach(node => { if (Number(node.dataset.subtotalExpires) <= Date.now()) { node.textContent = 'Preise werden aktualisiert'; delete node.dataset.subtotalExpires; } });
  }, 15000);
})(typeof window !== 'undefined' ? window : globalThis);
