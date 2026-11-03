(() => {
  'use strict';
  const form = document.querySelector('form[data-rim-search]');
  if (!form || !window.fetch || !window.AbortController) return;
  const button = form.querySelector('[data-rim-search-button]');
  const status = form.querySelector('[data-rim-search-status]');
  const results = form.querySelector('[data-rim-search-results]');
  if (!button || !status || !results) return;
  const catalogStatus = document.querySelector('[data-rim-catalog-status]');
  const keys = ['suche', 'hersteller', 'material', 'anzahl', 'sortierung'];
  const materials = {alle:'Alle Materialien', alu:'Alufelge', stahl:'Stahlfelge'};
  const sorts = ['preis_auf', 'preis_ab', 'name'];
  const messages = {suche:'Bitte einen Suchbegriff mit höchstens 120 Zeichen eingeben.',
    hersteller:'Bitte eine Marke mit höchstens 100 Zeichen auswählen.', material:'Bitte alle Materialien, Alu oder Stahl auswählen.',
    anzahl:'Bitte eine Anzahl zwischen 1 und 8 Felgen auswählen.', sortierung:'Bitte eine der angebotenen Sortierungen auswählen.'};
  const money = new Intl.NumberFormat('de-DE', {style:'currency', currency:'EUR'});
  const clock = new Intl.DateTimeFormat('de-DE', {hour:'2-digit', minute:'2-digit', timeZone:'Europe/Berlin'});
  let generation = 0, controller = null, expiryTimer = null, cooldownTimer = null, retryUntil = 0, nextPage = 1, linkedBrand = null;
  const node = (tag, className, text) => {
    const result = document.createElement(tag);
    if (className) result.className = className;
    if (text !== undefined) result.textContent = text;
    return result;
  };
  const field = name => form.querySelector(`[name="${name}"]`);
  const value = name => String(field(name)?.value || '').trim();
  const text = (value, maximum, required=false) => typeof value === 'string'
    && value.length <= maximum && (!required || value.length > 0) && value === value.trim() && !/[\p{C}]/u.test(value);
  const read = () => Object.fromEntries(keys.map(key => [key, value(key)]));
  const invalid = query => keys.filter(key => key === 'suche' ? !text(query[key],120)
    : key === 'hersteller' ? !text(query[key],100)
    : key === 'material' ? !Object.prototype.hasOwnProperty.call(materials,query[key])
    : key === 'anzahl' ? !['1','2','3','4','5','6','7','8'].includes(query[key]) : !sorts.includes(query[key]));
  const say = (message, error=false) => { status.textContent = message; status.classList.toggle('is-error',error); };
  const fieldError = (name, message='') => {
    const input = field(name);
    if (!input) return;
    const id = `rim-error-${name}`;
    let error = form.querySelector(`#${id}`);
    const parent = input.closest('.field') || input.parentElement;
    if (message && !error && parent) { error = node('p','tire-field-error'); error.id = id; parent.append(error); }
    if (error) { error.textContent = message; error.hidden = !message; }
    const described = (input.getAttribute('aria-describedby') || '').split(/\s+/).filter(value => value && value !== id);
    if (message) { input.setAttribute('aria-invalid','true'); described.push(id); }
    else input.removeAttribute('aria-invalid');
    if (described.length) input.setAttribute('aria-describedby',described.join(' '));
    else input.removeAttribute('aria-describedby');
  };
  const buttonState = () => {
    const seconds = Math.max(0,Math.ceil((retryUntil-Date.now())/1000));
    button.disabled = seconds > 0;
    button.textContent = seconds ? `In ${seconds} Sekunden erneut suchen` : 'Felgenpreise anzeigen';
    if (!seconds) { window.clearInterval(cooldownTimer); cooldownTimer = null; }
  };
  const clear = () => {
    generation += 1;
    if (controller) controller.abort();
    controller = null;
    window.clearTimeout(expiryTimer);
    results.replaceChildren();
    form.removeAttribute('aria-busy');
    buttonState();
  };
  const feedback = (title, message) => {
    const box = node('section','tire-search-feedback');
    box.append(node('h3','',title),node('p','',message));
    results.replaceChildren(box);
  };
  const failed = code => {
    say('Felgenpreise können gerade nicht angezeigt werden.',true);
    if (code === 'keine_treffer') {
      say('Keine Felgen für diese Auswahl gefunden.');
      feedback('Keine passenden Felgen gefunden','Bitte ändern Sie Suchbegriff, Marke oder Material und starten Sie die Suche erneut. Ihre Auswahl bleibt erhalten.');
    } else if (code === 'cache_stale' || code === 'preis_abgelaufen') {
      feedback('Aktuelle Felgenpreise fehlen','Die gespeicherten Preise sind abgelaufen. Bitte laden Sie später aktuelle Preise. Ihre Suchangaben bleiben erhalten.');
    } else if (code === 'configuration') {
      feedback('Der Felgenkatalog ist noch nicht verfügbar','Für die Felgensuche steht gerade kein freigegebener aktueller Katalog bereit. Bitte versuchen Sie es später erneut.');
    } else if (code === 'timeout') {
      feedback('Die Felgensuche dauert gerade zu lange','Bitte starten Sie die Suche erneut. Ihre Suchangaben bleiben erhalten.');
    } else if (code === 'network') {
      feedback('Verbindung zur Felgensuche unterbrochen','Bitte prüfen Sie die Verbindung und versuchen Sie es erneut. Ihre Suchangaben bleiben erhalten.');
    } else feedback('Felgenpreise vorübergehend nicht verfügbar','Bitte versuchen Sie es später erneut. Ihre Suchangaben bleiben erhalten.');
  };
  const localUrl = (raw, pattern) => {
    if (typeof raw !== 'string' || /[?#\p{C}]/u.test(raw)) return null;
    try {
      const target = new URL(raw,window.location.origin);
      if (target.origin !== window.location.origin || target.username || target.password || target.search || target.hash
          || (raw !== target.pathname && raw !== target.href) || !pattern.test(target.pathname)) return null;
      return target;
    } catch (_) { return null; }
  };
  const amount = value => {
    if (!text(value,9,true) || !/^[0-9]{1,6}\.[0-9]{2}$/.test(value)) return null;
    const result = Number(value.replace('.',''));
    return Number.isSafeInteger(result) && result <= 10000000 ? result : null;
  };
  const validProduct = (product, query) => {
    if (!product || typeof product !== 'object' || !text(product.artikel_id,18,true) || !/^[1-9][0-9]{0,17}$/.test(product.artikel_id)
        || !['bezeichnung','hersteller','profil'].every(key => text(product[key],250,true))
        || !['alu','stahl'].includes(product.material) || product.kennungspassung !== 'nicht_geprueft'
        || product.anzahl !== Number(query.anzahl) || !Number.isInteger(product.bestand) || product.bestand < product.anzahl
        || (query.material !== 'alle' && product.material !== query.material)
        || (query.hersteller && product.hersteller !== query.hersteller)) return false;
    const at = Date.parse(product.preisstand), until = Date.parse(product.gueltig_bis);
    if (!Number.isFinite(at) || !Number.isFinite(until) || until <= Date.now() || until <= at) return false;
    const price = product.preis;
    if (!price || price.waehrung !== 'EUR') return false;
    const values = ['einzel_brutto','gesamt_felgen_brutto','versand_brutto','gesamt_brutto'].map(key => amount(price[key]));
    return values.every(value => value !== null) && values[0] > 0 && values[1] === values[0] * product.anzahl && values[3] === values[1] + values[2];
  };
  const photo = product => {
    const figure = node('figure','tire-result-media rim-result-media');
    const visual = node('div','tire-result-image-box rim-result-image-box');
    const caption = node('figcaption','tire-result-image-caption');
    const unavailable = () => { visual.replaceChildren(node('span','tire-result-image-placeholder','Felgenfoto noch nicht hinterlegt')); caption.textContent = ''; };
    const image = product.bild;
    const target = image && typeof image === 'object' && Object.keys(image).length === 4
      && Object.keys(image).every(key => ['url','alt','bezug','motiv'].includes(key))
      && image.motiv === 'felge' && image.bezug === 'artikel' && text(image.alt,250,true) && !/[<>]/.test(image.alt)
      && typeof image.url === 'string' && image.url.startsWith('/api/felgenkatalog/bild/')
      ? localUrl(image.url,new RegExp(`^/api/felgenkatalog/bild/${product.artikel_id}/[0-9a-f]{64}\\.webp$`)) : null;
    if (target) {
      const picture = node('img','tire-result-image rim-result-image');
      picture.setAttribute('width','160'); picture.setAttribute('height','160');
      picture.setAttribute('loading','lazy'); picture.setAttribute('decoding','async');
      picture.setAttribute('alt',image.alt);
      picture.addEventListener('error',unavailable); picture.setAttribute('src',target.pathname);
      visual.append(picture); caption.textContent = 'Artikelabbildung · Felge ohne Reifen.';
    } else unavailable();
    figure.append(visual,caption); return figure;
  };
  const render = (products, query) => {
    const renderedGeneration = generation;
    products.forEach(product => {
      const card = node('article','tire-result');
      const identity = node('div','tire-result-identity'), pricing = node('div','tire-result-pricing'), detail = node('div','tire-result-detail');
      card.append(photo(product),identity,pricing,detail);
      identity.append(node('h3','',product.bezeichnung),node('p','tire-result-meta',`${product.hersteller} · ${materials[product.material]}`));
      identity.append(node('p','tire-result-stock','Im Lieferantenkatalog verfügbar'));
      identity.append(node('p','tire-result-meta','Die Passung für Ihr Fahrzeug ist noch nicht geprüft.'));
      const unit = node('p','tire-result-unit');
      unit.append(node('strong','',money.format(amount(product.preis.einzel_brutto)/100)),node('span','','pro Felge · inkl. MwSt. · ohne Reifen'));
      const total = node('div','tire-result-total');
      total.append(node('span','',`${query.anzahl} Felgen · Gesamt inkl. MwSt.`),node('strong','',money.format(amount(product.preis.gesamt_brutto)/100)));
      pricing.append(unit,total);
      const breakdown = node('dl');
      breakdown.append(node('dt','',`${query.anzahl} Felgen`),node('dd','',money.format(amount(product.preis.gesamt_felgen_brutto)/100)),
        node('dt','','Versand / Bezugskosten'),node('dd','',money.format(amount(product.preis.versand_brutto)/100)));
      detail.append(breakdown,node('p','tire-result-validity',`Preisstand ${clock.format(new Date(product.preisstand))} Uhr · gültig bis ${clock.format(new Date(product.gueltig_bis))} Uhr (Mosbach)`));
      const target = localUrl(product.auswahl_url,/^\/felgen\/auswahl\/[A-Za-z0-9_.-]{1,2000}$/);
      if (target) {
        const choose = node('a','tire-result-select','Felge auswählen'); choose.href = product.auswahl_url;
        choose.addEventListener('click',event => {
          if (generation !== renderedGeneration) { event.preventDefault(); return; }
          if (Date.parse(product.gueltig_bis) <= Date.now()) { event.preventDefault(); clear(); failed('preis_abgelaufen'); }
        });
        detail.append(choose);
      } else detail.append(node('p','tire-result-closed','Die Auswahl ist gerade nicht verfügbar. Bitte laden Sie die Preise erneut.'));
      results.append(card);
    });
    expiryTimer = window.setTimeout(() => { clear(); failed('preis_abgelaufen'); },Math.max(0,Math.min(...products.map(product => Date.parse(product.gueltig_bis)))-Date.now()));
  };
  button.addEventListener('click',async () => {
    if (retryUntil > Date.now()) return;
    const page = nextPage; nextPage = 1;
    clear(); keys.forEach(name => fieldError(name));
    const query = read(), errors = invalid(query);
    if (errors.length) {
      errors.forEach(name => fieldError(name,messages[name])); field(errors[0])?.focus(); say('Bitte das markierte Suchfeld prüfen.',true); return;
    }
    const target = localUrl(form.dataset.searchUrl,/^\/api\/felgenangebote\/suche$/);
    if (!target) { failed('configuration'); return; }
    Object.entries(query).forEach(([key,value]) => target.searchParams.set(key,value));
    if (page > 1) target.searchParams.set('seite',String(page));
    const current = generation;
    controller = new AbortController(); const requestController = controller;
    let timedOut = false;
    const timeout = window.setTimeout(() => { timedOut = true; requestController.abort(); },12000);
    form.setAttribute('aria-busy','true'); say('Aktuelle Felgenpreise werden geladen …');
    try {
      const response = await fetch(target,{method:'GET',signal:requestController.signal,cache:'no-store',credentials:'same-origin',headers:{Accept:'application/json'}});
      const payload = await response.json();
      if (current !== generation) return;
      if (response.status === 400) {
        const name = payload && keys.includes(payload.feld) ? payload.feld : null;
        if (name) { fieldError(name,messages[name]); field(name)?.focus(); }
        say('Bitte Ihre Sucheingaben prüfen.',true); return;
      }
      if (response.status === 429) {
        const seconds = Number(response.headers.get('Retry-After'));
        retryUntil = Date.now() + (Number.isFinite(seconds) && seconds >= 1 ? Math.min(seconds,3600) : 30) * 1000;
        feedback('Bitte kurz warten','Sie haben mehrere Suchen gestartet. Ihre Auswahl bleibt erhalten; nach Ablauf der Wartezeit können Sie erneut suchen.');
        cooldownTimer = window.setInterval(buttonState,1000); return;
      }
      if (!response.ok || !payload || payload.verfuegbar !== true || payload.status !== 'ok') {
        failed(payload && (payload.code === 'keine_treffer' || payload.status === 'keine_treffer' ? 'keine_treffer' : payload.code)); return;
      }
      if (!Array.isArray(payload.produkte)) { failed(); return; }
      const products = payload.produkte.slice(0,12).filter(product => validProduct(product,query));
      if (!products.length) { failed(); return; }
      render(products,query);
      const total = Number.isInteger(payload.treffer_gesamt) && payload.treffer_gesamt >= products.length ? payload.treffer_gesamt : products.length;
      say(`${products.length}${total > products.length ? ` von ${total}` : ''} passende Felgenangebote. Preise inklusive Mehrwertsteuer. Sie können eine Felge auswählen; die Fahrzeugpassung ist noch nicht geprüft.`);
      if (Number.isInteger(payload.seiten) && payload.seiten > 1 && payload.seiten <= 10000 && Number.isInteger(payload.seite) && payload.seite >= 1 && payload.seite <= payload.seiten) {
        const pages = node('nav','tire-pagination'); pages.setAttribute('aria-label','Weitere Felgenangebote');
        const previous = node('button','','← Zurück'), next = node('button','','Weitere Felgen →');
        previous.type = next.type = 'button'; previous.disabled = payload.seite <= 1; next.disabled = payload.seite >= payload.seiten;
        previous.addEventListener('click',() => { nextPage = payload.seite-1; button.click(); });
        next.addEventListener('click',() => { nextPage = payload.seite+1; button.click(); });
        pages.append(previous,node('span','',`Seite ${payload.seite} von ${payload.seiten}`),next); results.append(pages);
      }
    } catch (_) { if (current === generation) failed(timedOut ? 'timeout' : 'network'); }
    finally { window.clearTimeout(timeout); if (current === generation) { controller = null; form.removeAttribute('aria-busy'); buttonState(); } }
  });
  form.noValidate = true;
  form.addEventListener('submit',event => { event.preventDefault(); button.click(); });
  keys.forEach(name => {
    const input = field(name);
    if (!input) return;
    const changed = automatic => {
      nextPage = 1; clear(); fieldError(name); say('Auswahl geändert. Bitte Felgenpreise erneut abrufen.');
      if (automatic && name !== 'suche' && !invalid(read()).length) button.click();
    };
    input.addEventListener('input',() => changed(false)); input.addEventListener('change',() => changed(true));
  });
  const loadFacets = async (desiredBrand, desiredGeneration) => {
    const brand = form.querySelector('[data-rim-brand]');
    const target = localUrl(form.dataset.facetsUrl,/^\/api\/felgenkatalog\/auswahl$/);
    if (!brand || !target) return false;
    const abort = new AbortController(), timeout = window.setTimeout(() => abort.abort(),15000);
    try {
      const response = await fetch(target,{method:'GET',signal:abort.signal,cache:'no-store',credentials:'same-origin',headers:{Accept:'application/json'}});
      const data = await response.json(), at = Date.parse(data && data.preisstand), until = Date.parse(data && data.gueltig_bis);
      if (!response.ok || !data || data.status !== 'ok' || !Array.isArray(data.hersteller) || data.hersteller.length > 1000
          || !data.hersteller.every(name => text(name,100,true)) || new Set(data.hersteller).size !== data.hersteller.length
          || !Array.isArray(data.materialien) || !data.materialien.every(material => ['alu','stahl'].includes(material))
          || !Number.isInteger(data.artikel_gesamt) || data.artikel_gesamt < 0 || data.artikel_gesamt > 300000
          || !Number.isFinite(at) || !Number.isFinite(until) || until <= Date.now() || until <= at) throw new Error();
      const selected = desiredBrand !== null && generation === desiredGeneration ? desiredBrand : brand.value;
      const names = data.hersteller.slice();
      if (selected && !names.includes(selected)) names.push(selected);
      const all = node('option','','Alle Marken'); all.value = '';
      const options = names.map(name => { const option = node('option','',name); option.value = name; return option; });
      brand.replaceChildren(all,...options); brand.value = selected; brand.disabled = false;
      if (catalogStatus) catalogStatus.textContent = `${data.artikel_gesamt} Felgen im verfügbaren Katalog · ${data.hersteller.length} Marken · Preisstand ${clock.format(new Date(data.preisstand))} Uhr`;
      return true;
    } catch (_) {
      if (catalogStatus) catalogStatus.textContent = 'Die Markenliste ist gerade nicht verfügbar. Ihre Sucheingaben bleiben erhalten.';
      return false;
    } finally { window.clearTimeout(timeout); }
  };
  const applyLink = () => {
    const params = new URLSearchParams(window.location.search || '');
    if (params.getAll('anzeigen').length !== 1 || params.get('anzeigen') !== '1' || keys.some(key => params.getAll(key).length > 1)) return false;
    const query = read(); keys.forEach(key => { if (params.has(key)) query[key] = params.get(key); });
    if (invalid(query).length) return false;
    if (params.has('hersteller')) linkedBrand = query.hersteller;
    keys.forEach(key => { if (key !== 'hersteller' && params.has(key) && field(key)) field(key).value = query[key]; });
    return true;
  };
  const linked = applyLink(), linkedGeneration = generation;
  buttonState();
  loadFacets(linkedBrand,linkedGeneration).then(ready => { if (ready && linked && generation === linkedGeneration) button.click(); });
})();
