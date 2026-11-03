(() => {
  'use strict';
  const panel = document.querySelector('[data-tire-search]');
  if (!panel || !window.fetch || !window.AbortController) return;
  const form = panel.closest('form');
  const standalone = panel.hasAttribute('data-standalone-shop');
  const button = panel.querySelector('[data-tire-search-button]');
  const status = panel.querySelector('[data-tire-search-status]');
  const results = panel.querySelector('[data-tire-search-results]');
  const criteria = ['reifen_groesse_vorne', 'reifen_groesse_hinten', 'reifen_saison', 'reifen_qualitaet', 'reifen_anzahl', 'reifen_montage', 'reifen_hersteller', 'reifen_sortierung'];
  const money = new Intl.NumberFormat('de-DE', {style: 'currency', currency: 'EUR'});
  const clock = new Intl.DateTimeFormat('de-DE', {hour:'2-digit', minute:'2-digit', timeZone:'Europe/Berlin'});
  const seasons = {sommer:'Sommerreifen', winter:'Winterreifen', ganzjahr:'Ganzjahresreifen'};
  const qualities = {alle:'Alle Marken', preisbewusst:'Preisbewusst', mittelklasse:'Mittelklasse', markenqualitaet:'Markenqualität', premium:'Premium'};
  const productQualities = ['preisbewusst', 'mittelklasse', 'markenqualitaet', 'premium'];
  let controller = null;
  let generation = 0;
  let expiryTimer = null;
  let cooldownTimer = null;
  let retryUntil = 0;
  let nextPage = 1;
  let refreshDimensions = null;
  let lastDimensionRefresh = 0;
  const names = {groesse:'reifen_groesse_vorne', groesse_hinten:'reifen_groesse_hinten', saison:'reifen_saison', qualitaet:'reifen_qualitaet', anzahl:'reifen_anzahl', montage:'reifen_montage', hersteller:'reifen_hersteller', sortierung:'reifen_sortierung'};
  const messages = {
    groesse:'Bitte die Reifengröße eingeben, zum Beispiel 225/40 R18 92V. Auch 2254018 ist möglich.',
    groesse_hinten:'Bitte die Größe der Hinterreifen prüfen, zum Beispiel 225/40 R18 92V, oder das optionale Feld leeren.',
    saison:'Bitte Sommerreifen, Winterreifen oder Ganzjahresreifen auswählen.',
    qualitaet:'Bitte eine Marken- und Preiswahl auswählen, zum Beispiel „Alle Marken“.',
    anzahl:'Bitte eine Anzahl zwischen 1 und 8 Reifen auswählen.',
    montage:'Bitte Abholung oder Montage auswählen.',
    hersteller:'Bitte eine Marke aus dem Katalog oder „Alle Marken“ auswählen.',
    sortierung:'Bitte eine der angebotenen Sortierungen auswählen.'
  };

  const value = name => String(new FormData(form).get(name) || '').trim();
  const say = (text, error=false) => {
    status.textContent = text;
    status.classList.toggle('is-error', error);
  };
  const node = (tag, className, text) => {
    const el = document.createElement(tag);
    if (className) el.className = className;
    if (text !== undefined) el.textContent = text;
    return el;
  };
  const buttonState = () => {
    const seconds = Math.max(0, Math.ceil((retryUntil - Date.now()) / 1000));
    button.disabled = seconds > 0;
    button.textContent = seconds ? `In ${seconds} Sekunden erneut suchen` : 'Reifenpreise anzeigen';
    if (!seconds) { window.clearInterval(cooldownTimer); cooldownTimer = null; }
  };
  const fieldError = (key, text) => {
    const fields = form.querySelectorAll(`[name="${names[key]}"]`);
    if (!fields.length) return;
    const id = `tire-error-${names[key]}`;
    const parent = fields[0].closest('.field') || fields[0].closest('fieldset') || fields[0].closest('.tire-sort');
    let error = form.querySelector(`#${id}`);
    if (text && !error && parent) { error = node('p', 'tire-field-error'); error.id = id; parent.append(error); }
    if (error) { error.textContent = text || ''; error.hidden = !text; }
    fields.forEach(field => {
      const described = (field.getAttribute('aria-describedby') || '').split(/\s+/).filter(v => v && v !== id);
      if (text) { field.setAttribute('aria-invalid', 'true'); described.push(id); }
      else field.removeAttribute('aria-invalid');
      if (described.length) field.setAttribute('aria-describedby', described.join(' '));
      else field.removeAttribute('aria-describedby');
    });
  };
  const resetErrors = () => Object.keys(names).forEach(key => fieldError(key));
  const parseSize = text => {
    if (text.length > 40 || /[\x00-\x1f\x7f]/.test(text)) return null;
    const match = text.toUpperCase().match(/^(\d{3})(?:\s*\/\s*|\s+)(\d{2})(?:\s*Z?R\s*|\s+)(\d{2})(?:\s*(\d{2,3})?\s*([A-Z])?)?$/)
      || text.match(/^(\d{3})(\d{2})(\d{2})$/);
    if (!match) return null;
    const [, width, ratio, rim, load, speed] = match;
    const valid = Number(width) >= 100 && Number(width) <= 500 && Number(ratio) >= 10 && Number(ratio) <= 95
      && Number(rim) >= 10 && Number(rim) <= 30 && (!load || (Number(load) >= 60 && Number(load) <= 150))
      && (!speed || ['L','M','N','P','Q','R','S','T','U','H','V','W','Y'].includes(speed));
    return valid ? [Number(width), Number(ratio), Number(rim), load ? Number(load) : null, speed || null] : null;
  };
  const sizeMessage = (text, key='groesse') => {
    if (parseSize(text)) return messages[key];
    if (/^\d{3}\s*(?:\/|\s)\s*\d{2}\s*$/i.test(text))
      return 'Die Zollgröße fehlt. Bitte ergänzen Sie zum Beispiel R18: 225/40 R18.';
    if (/^\d{3}\s*(?:\/|\s)\s*\d{2}\s*Z?R\s*\d?$/i.test(text))
      return 'Die Zollgröße ist unvollständig. Bitte zwei Ziffern angeben, zum Beispiel R18.';
    const base = text.match(/^(\d{3}(?:\s*\/\s*|\s+)\d{2}(?:\s*Z?R\s*|\s+)\d{2})(.+)$/i);
    if (base && parseSize(base[1]))
      return 'Die Angabe nach der Reifengröße ist ungültig. Last- und Geschwindigkeitsindex sehen zum Beispiel so aus: 92V. Bitte Ihre tatsächlichen Fahrzeugangaben übernehmen.';
    return messages[key];
  };
  const focusField = key => {
    const field = form.querySelector(`[name="${names[key] || names.groesse}"]`);
    if (field) field.focus();
  };
  const feedback = (title, text, actions=[]) => {
    const box = node('section', 'tire-search-feedback');
    box.append(node('h3', '', title), node('p', '', text));
    if (actions.length) {
      const controls = node('div', 'tire-feedback-actions');
      actions.forEach(action => {
        const control = node('button', '', action.label);
        control.type = 'button';
        control.addEventListener('click', action.run);
        controls.append(control);
      });
      box.append(controls);
    }
    try {
      const target = new URL(panel.dataset.inquiryUrl, window.location.origin);
      if (target.origin === window.location.origin && target.pathname === '/anfrage') {
        const contact = node('a', 'tire-feedback-contact', 'Persönliche Beratung anfragen');
        contact.href = target.href;
        box.append(contact);
      }
    } catch (_) { /* An inquiry link is optional. */ }
    results.replaceChildren(box);
  };
  const clear = () => {
    generation += 1;
    if (controller) controller.abort();
    controller = null;
    window.clearTimeout(expiryTimer);
    results.replaceChildren();
    buttonState();
    panel.removeAttribute('aria-busy');
  };
  const searchWith = changes => {
    Object.entries(changes).forEach(([key, val]) => {
      const fields = form.querySelectorAll(`[name="${names[key]}"]`);
      fields.forEach(field => {
        if (field.type === 'radio') field.checked = field.value === val;
        else field.value = val;
      });
    });
    button.click();
  };
  const alternatives = query => {
    const actions = [];
    if (query.qualitaet !== 'alle' || query.hersteller) actions.push({label:'Alle Marken anzeigen', run:() => searchWith({qualitaet:'alle',hersteller:''})});
    if (query.montage === 'ja') actions.push({label:'Reifen ohne Montage anzeigen', run:() => searchWith({montage:'nein'})});
    actions.push({label:'Suche anpassen', run:() => focusField('groesse')});
    return actions;
  };
  const failed = (code, query) => {
    const retry = [{label:'Erneut versuchen', run:() => button.click()}];
    say('Die Suche konnte nicht abgeschlossen werden.', true);
    if (code === 'keine_treffer') {
      const selection = [query.groesse, seasons[query.saison], `${query.anzahl} Reifen`].join(' · ');
      say('Keine Treffer für diese Auswahl.');
      feedback('Keine passenden Angebote gefunden', `${selection}. Für diese Auswahl sind aktuell keine bestätigten Onlineangebote vorhanden. Sie können die Filter anpassen.`, alternatives(query));
    } else if (code === 'montage_nicht_online') {
      feedback('Diesen Montageumfang stimmen wir persönlich ab', 'Online können Sie Montage für 1 bis 4 Reifen von 13 bis 20 Zoll auswählen. Sie können die Reifen zunächst ohne Montage suchen.', alternatives(query));
    } else if (code === 'katalog_veraltet') {
      feedback('Aktuelle Reifenpreise fehlen', 'Der letzte Preisstand ist abgelaufen. Ihre Eingabe ist in Ordnung. Bitte versuchen Sie es später erneut; abgelaufene Preise zeigen wir nicht als aktuelle Angebote an.', retry);
    } else if (code === 'preis_abgelaufen') {
      feedback('Die angezeigten Preise sind abgelaufen', 'Bitte laden Sie die aktuellen Preise erneut. Ihre Reifengröße und Filter bleiben erhalten.', retry);
    } else if (code === 'katalog_nicht_bereit') {
      feedback('Die Onlinepreissuche wird noch eingerichtet', 'Aktuell steht noch kein freigegebener Reifenkatalog für die Onlinepreissuche bereit. Ihre Eingabe ist in Ordnung. Für Ihren Reifenbedarf erreichen Sie uns über die persönliche Anfrage.');
    } else if (code === 'angebote_nicht_pruefbar') {
      feedback('Für diese Auswahl fehlt ein bestätigter Preis', 'Die gefundenen Daten reichen für ein sicheres Onlineangebot nicht aus. Sie können Ihre Filter anpassen oder sich persönlich beraten lassen.', alternatives(query));
    } else if (code === 'timeout') {
      feedback('Die Preissuche dauert gerade zu lange', 'Ihre Auswahl bleibt erhalten. Bitte versuchen Sie die Suche noch einmal.', retry);
    } else if (code === 'network') {
      feedback('Verbindung zur Preissuche unterbrochen', 'Die Suche konnte gerade keine Antwort laden. Ihre Auswahl bleibt erhalten. Bitte prüfen Sie die Verbindung und versuchen Sie es erneut.', retry);
    } else {
      feedback('Reifenpreise vorübergehend nicht verfügbar', 'Die Preissuche ist momentan nicht erreichbar. Das liegt nicht an Ihrer Reifengröße. Bitte versuchen Sie es später erneut.', retry);
    }
  };
  const amount = value => {
    if (typeof value !== 'string' || !/^\d{1,6}\.\d{2}$/.test(value)) throw new Error('Ungültiger Preis');
    const cents = Number(value.replace('.', ''));
    if (!Number.isSafeInteger(cents) || cents > 10000000) throw new Error('Ungültiger Preis');
    return cents;
  };
  const selectionUrl = raw => {
    if (typeof raw !== 'string' || /[?#]/.test(raw)) return null;
    try {
      const target = new URL(raw, window.location.origin);
      if (target.origin !== window.location.origin || target.username || target.password || target.search || target.hash
          || (raw !== target.pathname && raw !== target.href)
          || !/^\/reifen\/bestellen\/[A-Za-z0-9_.-]{1,2000}(?:\/(?:auswahl|kauf))?$/.test(target.pathname)) return null;
      return raw;
    } catch (_) { return null; }
  };
  const validProduct = (product, query) => {
    if (!product || typeof product !== 'object' || !product.preis || typeof product.preis !== 'object') return false;
    if (!['artikel_id','bezeichnung','groesse','lieferzeit'].every(k => typeof product[k] === 'string' && product[k].length > 0 && product[k].length <= 250)) return false;
    if (product.saison !== query.saison || product.preis.waehrung !== 'EUR') return false;
    if (query.hersteller && product.hersteller !== query.hersteller) return false;
    if (product.qualitaet !== null && !productQualities.includes(product.qualitaet)) return false;
    if (query.qualitaet !== 'alle' && product.qualitaet !== query.qualitaet) return false;
    const until = Date.parse(product.gueltig_bis);
    const at = Date.parse(product.preisstand);
    if (!Number.isFinite(until) || until <= Date.now() || !Number.isFinite(at)) return false;
    try {
      const p = product.preis;
      const unit = amount(p.einzel_brutto);
      const tires = amount(p.gesamt_reifen_brutto);
      const shipping = amount(p.versand_brutto);
      const mounting = amount(p.montage_brutto);
      const total = amount(p.gesamt_brutto);
      return unit > 0 && tires === unit * Number(query.anzahl) && total === tires + shipping + mounting
        && (query.montage === 'ja' || mounting === 0);
    } catch (_) { return false; }
  };
  const profileImage = product => {
    const image = product.bild;
    if (!image || typeof image !== 'object' || Object.keys(image).length !== 4
        || !Object.keys(image).every(key => ['url', 'alt', 'bezug', 'motiv'].includes(key))
        || image.bezug !== 'profil' || image.motiv !== 'reifen'
        || typeof image.url !== 'string' || typeof image.alt !== 'string'
        || !image.alt.length || image.alt.length > 250 || image.alt !== image.alt.trim()
        || /[\p{C}<>]/u.test(image.alt)) return null;
    const match = /^\/api\/reifenkatalog\/bild\/([1-9][0-9]{0,17})\/[0-9a-f]{64}\.webp$/.exec(image.url);
    if (!match || match[0] !== image.url || match[1] !== product.artikel_id) return null;
    return {url:image.url, alt:image.alt};
  };
  const productFigure = product => {
    const figure = node('figure', 'tire-result-media');
    // Keep the shared inquiry usable even when only its existing search CSS is loaded.
    figure.setAttribute('style', 'margin:0;max-width:120px;min-height:190px;');
    const visual = node('div', 'tire-result-image-box');
    visual.setAttribute('style', 'width:100%;height:144px;display:grid;place-items:center;');
    const caption = node('figcaption', 'tire-result-image-caption');
    caption.setAttribute('style', 'font-size:11px;line-height:1.45;margin-top:8px;overflow-wrap:anywhere;');
    const unavailable = () => {
      visual.replaceChildren(node('span', 'tire-result-image-placeholder', 'Kein Profilbild verfügbar'));
      caption.textContent = '';
    };
    const image = profileImage(product);
    if (image) {
      const picture = node('img', 'tire-result-image');
      picture.setAttribute('width', '120');
      picture.setAttribute('height', '144');
      picture.setAttribute('loading', 'lazy');
      picture.setAttribute('decoding', 'async');
      picture.setAttribute('alt', image.alt);
      picture.setAttribute('style', 'display:block;width:100%;max-width:120px;height:144px;object-fit:contain;');
      picture.addEventListener('error', unavailable);
      picture.setAttribute('src', image.url);
      visual.append(picture);
      caption.textContent = 'Profilabbildung · Größe/Ausführung kann abweichen.';
    } else unavailable();
    figure.append(visual, caption);
    return figure;
  };
  const render = (products, query) => {
    const renderedGeneration = generation;
    products.forEach(product => {
      const card = node('article', 'tire-result');
      const identity = node('div', 'tire-result-identity');
      const pricing = node('div', 'tire-result-pricing');
      const detail = node('div', 'tire-result-detail');
      card.append(productFigure(product), identity, pricing, detail);
      identity.append(node('h3', '', product.bezeichnung));
      const details = [product.groesse, seasons[product.saison]];
      if (productQualities.includes(product.qualitaet)) details.push(qualities[product.qualitaet]);
      identity.append(node('p', 'tire-result-meta', details.join(' · ')));
      if (Number.isInteger(product.bestand) && product.bestand >= Number(query.anzahl)) identity.append(node('p', 'tire-result-stock', 'Im Lieferantenkatalog verfügbar'));
      const label = product.eu_label;
      if (label && ['A','B','C','D','E'].includes(label.kraftstoffeffizienz)
          && ['A','B','C','D','E'].includes(label.nasshaftung)
          && ['A','B','C'].includes(label.rollgeraeuschklasse)
          && Number.isInteger(label.rollgeraeusch_db) && label.rollgeraeusch_db >= 40 && label.rollgeraeusch_db <= 130) {
        let eprel;
        try { eprel = new URL(label.eprel_url); } catch (_) { eprel = null; }
        if (eprel && eprel.protocol === 'https:' && eprel.hostname === 'eprel.ec.europa.eu'
            && !eprel.username && !eprel.password && !eprel.search && !eprel.hash
            && (!eprel.port || eprel.port === '443')
            && /^\/(?:qr\/\d{1,12}|screen\/product\/tyres\/\d{1,12})\/?$/.test(eprel.pathname)) {
          identity.append(node('p', 'tire-result-label', `Kraftstoff ${label.kraftstoffeffizienz} · Nasshaftung ${label.nasshaftung} · Rollgeräusch ${label.rollgeraeusch_db} dB (${label.rollgeraeuschklasse})`));
          const labelLink = node('a', 'tire-result-label-link', 'EU-Reifenlabel und Produktdatenblatt');
          labelLink.href = eprel.href;
          labelLink.target = '_blank';
          labelLink.rel = 'noopener noreferrer';
          identity.append(labelLink);
        }
      }
      const unit = node('p', 'tire-result-unit');
      unit.append(node('strong', '', money.format(amount(product.preis.einzel_brutto) / 100)), node('span', '', 'pro Reifen · inkl. MwSt. · ohne Felge'));
      pricing.append(unit);
      const breakdown = node('dl');
      const line = (label, val) => breakdown.append(node('dt', '', label), node('dd', '', money.format(amount(val) / 100)));
      line(`${query.anzahl} Reifen`, product.preis.gesamt_reifen_brutto);
      line('Versand / Bezugskosten', product.preis.versand_brutto);
      if (query.montage === 'ja') line('Montage', product.preis.montage_brutto);
      detail.append(breakdown);
      const total = node('div', 'tire-result-total');
      total.append(node('span', '', `${query.anzahl} Reifen · Gesamt inkl. MwSt.`), node('strong', '', money.format(amount(product.preis.gesamt_brutto) / 100)));
      pricing.append(total);
      detail.append(node('p', 'tire-result-delivery', `Lieferzeit: ${product.lieferzeit}`));
      detail.append(node('p', 'tire-result-validity', `Preisstand ${clock.format(new Date(product.preisstand))} Uhr · Preis gültig bis ${clock.format(new Date(product.gueltig_bis))} Uhr (Mosbach)`));
      const target = selectionUrl(product.bestell_url);
      if (!target) {
        detail.append(node('p', 'tire-result-closed', 'Die Reifenauswahl ist gerade nicht verfügbar. Bitte laden Sie die Preise erneut.'));
        results.append(card);
        return;
      }
      const choose = node('a', 'tire-result-select', 'Reifen auswählen');
      choose.href = target;
      choose.addEventListener('click', event => {
        if (generation !== renderedGeneration) { event.preventDefault(); return; }
        if (Date.parse(product.gueltig_bis) <= Date.now()) {
          event.preventDefault(); clear(); failed('preis_abgelaufen', query); return;
        }
      });
      detail.append(choose); results.append(card);
    });
    const expires = Math.min(...products.map(p => Date.parse(p.gueltig_bis)));
    expiryTimer = window.setTimeout(() => { clear(); failed('preis_abgelaufen', query); }, Math.max(0, expires - Date.now()));
  };

  button.addEventListener('click', async () => {
    if (retryUntil > Date.now()) return;
    const requestedPage = nextPage;
    nextPage = 1;
    clear();
    resetErrors();
    const query = {groesse:value('reifen_groesse_vorne'), saison:value('reifen_saison'), qualitaet:value('reifen_qualitaet'), anzahl:value('reifen_anzahl'), montage:value('reifen_montage')};
    if (value('reifen_hersteller')) query.hersteller = value('reifen_hersteller');
    if (value('reifen_sortierung') && value('reifen_sortierung') !== 'preis_auf') query.sortierung = value('reifen_sortierung');
    if (requestedPage > 1) query.seite = String(requestedPage);
    const missing = [];
    if (!parseSize(query.groesse)) missing.push('groesse');
    const rear = value('reifen_groesse_hinten');
    if (rear && !parseSize(rear)) missing.push('groesse_hinten');
    if (!Object.prototype.hasOwnProperty.call(seasons, query.saison)) missing.push('saison');
    if (!Object.prototype.hasOwnProperty.call(qualities, query.qualitaet)) missing.push('qualitaet');
    if (!/^[1-8]$/.test(query.anzahl)) missing.push('anzahl');
    if (!['ja','nein'].includes(query.montage)) missing.push('montage');
    if (missing.length) {
      missing.forEach(key => fieldError(key, key === 'groesse' ? sizeMessage(query.groesse) : key === 'groesse_hinten' ? sizeMessage(rear,key) : messages[key]));
      say(missing.length === 1 ? 'Bitte das markierte Feld prüfen.' : `Bitte die ${missing.length} markierten Felder prüfen.`, true);
      focusField(missing[0]); return;
    }
    if (rear && JSON.stringify(parseSize(rear)) !== JSON.stringify(parseSize(query.groesse))) {
      say('Vorder- und Hinterreifen haben unterschiedliche Angaben.');
      feedback('Zwei Reifenausführungen persönlich abstimmen', 'Für unterschiedliche Größen oder Last- und Geschwindigkeitsindizes vorne und hinten erstellen wir Ihnen ein persönliches Gesamtangebot. Ihre Angaben bleiben erhalten.'); return;
    }
    const current = generation;
    controller = new AbortController();
    const requestController = controller;
    let timedOut = false;
    const timeout = window.setTimeout(() => { timedOut = true; requestController.abort(); }, 12000);
    button.disabled = true; button.textContent = 'Preise werden geladen …'; panel.setAttribute('aria-busy', 'true');
    say('Passende Reifen und aktuelle Preise werden gesucht.');
    try {
      const url = new URL(panel.dataset.searchUrl, window.location.origin);
      url.search = new URLSearchParams(query).toString();
      const response = await fetch(url, {signal:requestController.signal, cache:'no-store', credentials:'same-origin', headers:{Accept:'application/json'}});
      if (current !== generation) return;
      let payload = null;
      try { payload = await response.json(); } catch (_) { /* Show only our own safe messages. */ }
      if (current !== generation) return;
      if (response.status === 400) {
        const key = payload && Object.prototype.hasOwnProperty.call(names, payload.feld) ? payload.feld : null;
        if (key) { fieldError(key, key === 'groesse' ? sizeMessage(query.groesse) : messages[key]); focusField(key); say('Bitte das markierte Feld prüfen.', true); }
        else { say('Bitte Ihre Sucheingaben prüfen.', true); feedback('Die Sucheingaben konnten nicht verarbeitet werden', 'Bitte prüfen Sie Größe, Saison und Anzahl und starten Sie die Suche erneut.', [{label:'Eingaben prüfen',run:() => focusField('groesse')}]); }
        return;
      }
      if (response.status === 429) {
        const seconds = Number(response.headers.get('Retry-After'));
        retryUntil = Date.now() + (Number.isFinite(seconds) && seconds >= 1 ? Math.min(seconds,3600) : 30) * 1000;
        say('Sie haben gerade mehrere Suchen gestartet. Bitte warten Sie kurz.');
        feedback('Bitte kurz warten', 'Ihre Auswahl bleibt erhalten. Sobald die Wartezeit auf der Suchschaltfläche abgelaufen ist, können Sie erneut suchen.');
        cooldownTimer = window.setInterval(buttonState, 1000);
        return;
      }
      if (!response.ok) { failed(payload && payload.code, query); return; }
      if (payload && payload.verfuegbar === false) {
        failed(payload.code || (payload.status === 'keine_treffer' ? 'keine_treffer' : 'katalog_nicht_verfuegbar'), query); return;
      }
      if (!payload || payload.status !== 'ok' || payload.verfuegbar !== true || !Array.isArray(payload.produkte)) {
        failed('angebote_nicht_pruefbar', query); return;
      }
      const products = payload.produkte.slice(0,12).filter(p => validProduct(p, query));
      if (!products.length) { failed('angebote_nicht_pruefbar', query); return; }
      render(products, query);
      if (refreshDimensions && Date.now() - lastDimensionRefresh >= 300000) refreshDimensions();
      const total = Number.isInteger(payload.treffer_gesamt) && payload.treffer_gesamt >= products.length ? payload.treffer_gesamt : products.length;
      const count = total > products.length ? `${products.length} von ${total} Angeboten` : `${products.length} passende${products.length === 1 ? 's Angebot' : ' Angebote'}`;
      say(`${count}. Preise inklusive Mehrwertsteuer.${panel.dataset.orderEnabled === 'false' ? ' Sie können Reifen auswählen und die Auswahl prüfen. Der verbindliche Bestellabschluss ist noch geschlossen.' : ` Reifen auswählen und Bestellung mit Zahlungsart${query.montage === 'ja' ? ' und Montagetermin' : ''} prüfen.`}`);
      if (Number.isInteger(payload.seiten) && payload.seiten > 1 && payload.seiten <= 10000
          && Number.isInteger(payload.seite) && payload.seite >= 1 && payload.seite <= payload.seiten) {
        const pages = node('nav', 'tire-pagination');
        pages.setAttribute('aria-label', 'Weitere Reifenangebote');
        const previous = node('button', '', '← Zurück'); previous.type = 'button'; previous.disabled = payload.seite <= 1;
        const next = node('button', '', 'Weitere Reifen →'); next.type = 'button'; next.disabled = payload.seite >= payload.seiten;
        previous.addEventListener('click', () => { nextPage = payload.seite - 1; button.click(); });
        next.addEventListener('click', () => { nextPage = payload.seite + 1; button.click(); });
        pages.append(previous, node('span', '', `Seite ${payload.seite} von ${payload.seiten}`), next); results.append(pages);
      }
    } catch (_) {
      if (current === generation) failed(timedOut ? 'timeout' : 'network', query);
    } finally {
      window.clearTimeout(timeout);
      if (current === generation) {
        controller = null; buttonState(); panel.removeAttribute('aria-busy');
      }
    }
  });
  if (standalone) {
    form.noValidate = true;
    form.addEventListener('submit', event => { event.preventDefault(); button.click(); });
  }
  criteria.forEach(name => {
    form.querySelectorAll(`[name="${name}"]`).forEach(field => {
      const changed = automatic => {
        const key = Object.keys(names).find(key => names[key] === name);
        if (key) fieldError(key);
        nextPage = 1; clear(); say('Auswahl geändert. Bitte Preise erneut abrufen.');
        if (automatic && ['reifen_hersteller','reifen_sortierung','reifen_saison'].includes(name)
            && parseSize(value('reifen_groesse_vorne'))) button.click();
      };
      field.addEventListener('input', () => changed(false));
      field.addEventListener('change', () => changed(true));
    });
  });
  document.querySelectorAll('[data-tire-example]').forEach(example => example.addEventListener('click', () => {
    searchWith({groesse:example.dataset.tireExample, saison:'winter', qualitaet:'alle', anzahl:'4', montage:'nein'});
  }));
  const loadDimensions = async () => {
    const width = form.querySelector('[data-tire-width]');
    const ratio = form.querySelector('[data-tire-ratio]');
    const rim = form.querySelector('[data-tire-rim]');
    const brand = form.querySelector('[data-tire-brand]');
    const direct = form.querySelector('[name="reifen_groesse_vorne"]');
    const catalogStatus = document.querySelector('[data-tire-catalog-status]');
    if (!panel.dataset.facetsUrl || !width || !ratio || !rim || !brand || !direct) return;
    let dimensions = [], suffix = '';
    const options = (field, values, desired, placeholder) => {
      const all = [node('option', '', placeholder)]; all[0].value = '';
      values.forEach(value => { const option = node('option', '', String(value)); option.value = String(value); all.push(option); });
      field.replaceChildren(...all);
      field.value = values.some(value => String(value) === String(desired)) ? String(desired) : '';
      field.disabled = !values.length;
    };
    const available = () => dimensions.filter(item => item.saisons.includes(value('reifen_saison')));
    const unique = values => [...new Set(values)].sort((a,b) => a-b);
    const preserveSuffix = () => {
      const parsed = parseSize(direct.value.trim());
      if (parsed) return `${parsed[3] || ''}${parsed[4] || ''}`;
      const base = direct.value.match(/^\d{3}(?:\s*\/\s*|\s+)\d{2}(?:\s*Z?R\s*|\s+)\d{2}(.*)$/i);
      return base ? base[1].trim() : suffix;
    };
    const synchronize = () => {
      const parsed = parseSize(direct.value.trim());
      const rows = available();
      suffix = preserveSuffix();
      options(width, unique(rows.map(row => row.breite)), parsed ? parsed[0] : width.value, 'Breite');
      const heights = rows.filter(row => String(row.breite) === width.value);
      options(ratio, unique(heights.map(row => row.querschnitt)), parsed ? parsed[1] : ratio.value, 'Höhe');
      const rims = heights.filter(row => String(row.querschnitt) === ratio.value);
      options(rim, unique(rims.map(row => row.zoll)), parsed ? parsed[2] : rim.value, 'Zoll');
    };
    [width, ratio, rim].forEach((field,index) => field.addEventListener('change', () => {
      suffix = preserveSuffix();
      const rows = available();
      if (index === 0) {
        options(ratio, unique(rows.filter(row => String(row.breite) === width.value).map(row => row.querschnitt)), '', 'Höhe');
      }
      if (index <= 1) {
        options(rim, unique(rows.filter(row => String(row.breite) === width.value && String(row.querschnitt) === ratio.value).map(row => row.zoll)), '', 'Zoll');
      }
      direct.value = width.value && ratio.value && rim.value ? `${width.value}/${ratio.value} R${rim.value}${suffix ? ' ' + suffix : ''}` : '';
      nextPage = 1; clear(); fieldError('groesse');
      say(direct.value ? 'Größe gewählt. Jetzt Reifenpreise anzeigen.' : 'Bitte Breite, Höhe und Zoll vollständig auswählen.');
    }));
    direct.addEventListener('input', synchronize);
    form.querySelectorAll('[name="reifen_saison"]').forEach(field => field.addEventListener('change', synchronize));
    let loading = false;
    const refresh = async () => {
    if (loading) return;
    loading = true;
    const abort = new AbortController();
    const timeout = window.setTimeout(() => abort.abort(), 15000);
    try {
      const url = new URL(panel.dataset.facetsUrl, window.location.origin);
      if (url.origin !== window.location.origin || url.pathname !== '/api/reifenkatalog/auswahl' || url.search) throw new Error();
      const response = await fetch(url, {signal:abort.signal,cache:'no-store',credentials:'same-origin',headers:{Accept:'application/json'}});
      const data = await response.json();
      if (!response.ok || data.status !== 'ok' || !Array.isArray(data.dimensionen) || data.dimensionen.length > 10000
          || !Array.isArray(data.hersteller) || data.hersteller.length > 1000
          || !Number.isFinite(Date.parse(data.preisstand)) || !Number.isFinite(Date.parse(data.gueltig_bis)) || Date.parse(data.gueltig_bis) <= Date.now()) throw new Error();
      if (!data.dimensionen.every(item => item && Number.isInteger(item.breite) && item.breite >= 100 && item.breite <= 500
          && Number.isInteger(item.querschnitt) && item.querschnitt >= 10 && item.querschnitt <= 95
          && Number.isInteger(item.zoll) && item.zoll >= 10 && item.zoll <= 30
          && Array.isArray(item.saisons) && item.saisons.length >= 1 && item.saisons.length <= 3
          && item.saisons.every(season => Object.prototype.hasOwnProperty.call(seasons,season)))
          || !data.hersteller.every(item => typeof item === 'string' && item.length > 0 && item.length <= 100 && !/[\x00-\x1f\x7f]/.test(item))) throw new Error();
      dimensions = data.dimensionen;
      lastDimensionRefresh = Date.now();
      const selectedBrand = brand.value;
      const brands = data.hersteller.slice();
      if (selectedBrand && !brands.includes(selectedBrand)) brands.push(selectedBrand);
      options(brand, brands, selectedBrand, 'Alle Marken');
      synchronize();
      if (catalogStatus) catalogStatus.textContent = `${dimensions.length} Größen im verfügbaren PKW-Katalog · ${data.hersteller.length} Marken · Preisstand ${clock.format(new Date(data.preisstand))} Uhr`;
    } catch (_) {
      [width,ratio,rim].forEach(field => options(field, [], '', '–'));
      if (catalogStatus) catalogStatus.textContent = 'Die Größenliste ist gerade nicht verfügbar. Sie können Ihre Reifengröße direkt eingeben.';
    } finally { window.clearTimeout(timeout); loading = false; }
    };
    refreshDimensions = refresh;
    await refresh();
  };
  const applySearchLink = () => {
    if (!standalone) return false;
    const params = new URLSearchParams(window.location.search || '');
    if (['anzeigen', 'groesse', 'saison', 'anzahl'].some(key => params.getAll(key).length !== 1)
        || ['qualitaet', 'montage'].some(key => params.getAll(key).length > 1)
        || params.get('anzeigen') !== '1') return false;
    const selection = {groesse:params.get('groesse').trim(), saison:params.get('saison'), anzahl:params.get('anzahl')};
    if (!parseSize(selection.groesse) || !Object.prototype.hasOwnProperty.call(seasons, selection.saison)
        || !/^[1-8]$/.test(selection.anzahl)) return false;
    const hiddenOption = field => field.type === 'hidden' || field.hidden || field.closest('[hidden]')
      || (typeof field.getClientRects === 'function' && !field.getClientRects().length);
    for (const key of ['qualitaet', 'montage']) {
      if (!params.has(key)) continue;
      const selected = params.get(key);
      if (key === 'qualitaet' ? !Object.prototype.hasOwnProperty.call(qualities, selected) : !['ja', 'nein'].includes(selected)) return false;
      const fields = Array.from(form.querySelectorAll(`[name="${names[key]}"]`));
      if (!fields.length) return false;
      const hidden = fields.filter(hiddenOption);
      if (hidden.length) {
        if (hidden.length !== fields.length || value(names[key]) !== selected) return false;
        continue;
      }
      const offered = fields.some(field => !field.disabled && !field.closest('fieldset[disabled]')
        && (field.type === 'radio' ? field.value === selected
          : field.tagName === 'SELECT' && Array.from(field.querySelectorAll('option')).some(option => option.value === selected && !option.disabled && !option.hidden)));
      if (!offered) return false;
      selection[key] = selected;
    }
    Object.entries(selection).forEach(([key, selected]) => {
      form.querySelectorAll(`[name="${names[key]}"]`).forEach(field => {
        if (field.type === 'radio') field.checked = field.value === selected;
        else field.value = selected;
      });
    });
    return true;
  };
  const linkedSearch = applySearchLink();
  const linkedGeneration = generation;
  panel.hidden = false;
  loadDimensions().then(() => { if (linkedSearch && generation === linkedGeneration) button.click(); });
})();
