(() => {
  'use strict';
  const panel = document.querySelector('[data-tire-search]');
  if (!panel || !window.fetch || !window.AbortController) return;
  const form = panel.closest('form');
  const standalone = panel.hasAttribute('data-standalone-shop');
  const button = panel.querySelector('[data-tire-search-button]');
  const status = panel.querySelector('[data-tire-search-status]');
  const results = panel.querySelector('[data-tire-search-results]');
  const criteria = ['reifen_groesse_vorne', 'reifen_groesse_hinten', 'reifen_saison', 'reifen_qualitaet', 'reifen_anzahl', 'reifen_montage'];
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
  const names = {groesse:'reifen_groesse_vorne', groesse_hinten:'reifen_groesse_hinten', saison:'reifen_saison', qualitaet:'reifen_qualitaet', anzahl:'reifen_anzahl', montage:'reifen_montage'};
  const messages = {
    groesse:'Bitte die Reifengröße eingeben, zum Beispiel 225/40 R18 92V. Auch 2254018 ist möglich.',
    groesse_hinten:'Bitte die Größe der Hinterreifen prüfen, zum Beispiel 225/40 R18 92V, oder das optionale Feld leeren.',
    saison:'Bitte Sommerreifen, Winterreifen oder Ganzjahresreifen auswählen.',
    qualitaet:'Bitte eine Marken- und Preiswahl auswählen, zum Beispiel „Alle Marken“.',
    anzahl:'Bitte eine Anzahl zwischen 1 und 8 Reifen auswählen.',
    montage:'Bitte Abholung oder Montage auswählen.'
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
    const parent = fields[0].closest('.field') || fields[0].closest('fieldset');
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
    if (query.qualitaet !== 'alle') actions.push({label:'Alle Marken anzeigen', run:() => searchWith({qualitaet:'alle'})});
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
  const validProduct = (product, query) => {
    if (!product || typeof product !== 'object' || !product.preis || typeof product.preis !== 'object') return false;
    if (!['artikel_id','bezeichnung','groesse','lieferzeit'].every(k => typeof product[k] === 'string' && product[k].length > 0 && product[k].length <= 250)) return false;
    if (product.saison !== query.saison || product.preis.waehrung !== 'EUR') return false;
    if (product.qualitaet !== null && !productQualities.includes(product.qualitaet)) return false;
    if (query.qualitaet !== 'alle' && product.qualitaet !== query.qualitaet) return false;
    try {
      const destination = new URL(product.bestell_url, window.location.origin);
      if (typeof product.bestell_url !== 'string' || destination.origin !== window.location.origin || !destination.pathname.startsWith('/reifen/bestellen/')) return false;
    } catch (_) { return false; }
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
  const render = (products, query) => {
    products.forEach(product => {
      const card = node('article', 'tire-result');
      card.append(node('h3', '', product.bezeichnung));
      const details = [product.groesse, seasons[product.saison]];
      if (productQualities.includes(product.qualitaet)) details.push(qualities[product.qualitaet]);
      card.append(node('p', 'tire-result-meta', details.join(' · ')));
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
          card.append(node('p', 'tire-result-label', `Kraftstoff ${label.kraftstoffeffizienz} · Nasshaftung ${label.nasshaftung} · Rollgeräusch ${label.rollgeraeusch_db} dB (${label.rollgeraeuschklasse})`));
          const labelLink = node('a', 'tire-result-label-link', 'EU-Reifenlabel und Produktdatenblatt');
          labelLink.href = eprel.href;
          labelLink.target = '_blank';
          labelLink.rel = 'noopener noreferrer';
          card.append(labelLink);
        }
      }
      const unit = node('p', 'tire-result-unit');
      unit.append(node('strong', '', money.format(amount(product.preis.einzel_brutto) / 100)), node('span', '', 'pro Reifen · inkl. MwSt.'));
      card.append(unit);
      const breakdown = node('dl');
      const line = (label, val) => breakdown.append(node('dt', '', label), node('dd', '', money.format(amount(val) / 100)));
      line(`${query.anzahl} Reifen`, product.preis.gesamt_reifen_brutto);
      line('Versand / Bezugskosten', product.preis.versand_brutto);
      if (query.montage === 'ja') line('Montage', product.preis.montage_brutto);
      card.append(breakdown);
      const total = node('div', 'tire-result-total');
      total.append(node('span', '', 'Gesamt inkl. MwSt.'), node('strong', '', money.format(amount(product.preis.gesamt_brutto) / 100)));
      card.append(total, node('p', 'tire-result-delivery', `Lieferzeit: ${product.lieferzeit}`));
      card.append(node('p', 'tire-result-validity', `Preisstand ${clock.format(new Date(product.preisstand))} Uhr · Preis gültig bis ${clock.format(new Date(product.gueltig_bis))} Uhr (Mosbach)`));
      if (panel.dataset.orderEnabled === 'false') {
        card.append(node('p', 'tire-result-closed', 'Der Bestellabschluss wird in Kürze freigeschaltet.'));
        results.append(card);
        return;
      }
      const choose = node('a', 'tire-result-select', 'Reifen bestellen');
      choose.href = product.bestell_url;
      choose.addEventListener('click', event => {
        if (Date.parse(product.gueltig_bis) <= Date.now()) {
          event.preventDefault(); clear(); failed('preis_abgelaufen', query); return;
        }
      });
      card.append(choose); results.append(card);
    });
    const expires = Math.min(...products.map(p => Date.parse(p.gueltig_bis)));
    expiryTimer = window.setTimeout(() => { clear(); failed('preis_abgelaufen', query); }, Math.max(0, expires - Date.now()));
  };

  button.addEventListener('click', async () => {
    if (retryUntil > Date.now()) return;
    clear();
    resetErrors();
    const query = {groesse:value('reifen_groesse_vorne'), saison:value('reifen_saison'), qualitaet:value('reifen_qualitaet'), anzahl:value('reifen_anzahl'), montage:value('reifen_montage')};
    const missing = [];
    if (!parseSize(query.groesse)) missing.push('groesse');
    const rear = value('reifen_groesse_hinten');
    if (rear && !parseSize(rear)) missing.push('groesse_hinten');
    if (!Object.prototype.hasOwnProperty.call(seasons, query.saison)) missing.push('saison');
    if (!Object.prototype.hasOwnProperty.call(qualities, query.qualitaet)) missing.push('qualitaet');
    if (!/^[1-8]$/.test(query.anzahl)) missing.push('anzahl');
    if (!['ja','nein'].includes(query.montage)) missing.push('montage');
    if (missing.length) {
      missing.forEach(key => fieldError(key, messages[key]));
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
        if (key) { fieldError(key, messages[key]); focusField(key); say('Bitte das markierte Feld prüfen.', true); }
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
      say(`${products.length} passende${products.length === 1 ? 's Angebot' : ' Angebote'}. Preise inklusive Mehrwertsteuer.${panel.dataset.orderEnabled === 'false' ? ' Der Bestellabschluss ist noch geschlossen.' : ` Reifen auswählen und Bestellung mit Zahlungsart${query.montage === 'ja' ? ' und Montagetermin' : ''} prüfen.`}`);
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
      const changed = () => {
        const key = Object.keys(names).find(key => names[key] === name);
        if (key) fieldError(key);
        clear(); say('Auswahl geändert. Bitte Preise erneut abrufen.');
      };
      field.addEventListener('input', changed);
      field.addEventListener('change', changed);
    });
  });
  document.querySelectorAll('[data-tire-example]').forEach(example => example.addEventListener('click', () => {
    searchWith({groesse:example.dataset.tireExample, saison:'winter', qualitaet:'alle', anzahl:'4', montage:'nein'});
  }));
  panel.hidden = false;
})();
