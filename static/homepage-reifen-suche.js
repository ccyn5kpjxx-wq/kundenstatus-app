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
  const clear = () => {
    generation += 1;
    if (controller) controller.abort();
    controller = null;
    window.clearTimeout(expiryTimer);
    results.replaceChildren();
    button.disabled = false;
    button.textContent = 'Reifenpreise anzeigen';
    panel.removeAttribute('aria-busy');
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
          event.preventDefault(); clear(); say('Der angezeigte Preis ist abgelaufen. Bitte die Preise erneut abrufen.'); return;
        }
      });
      card.append(choose); results.append(card);
    });
    const expires = Math.min(...products.map(p => Date.parse(p.gueltig_bis)));
    expiryTimer = window.setTimeout(() => { clear(); say('Die Preisgültigkeit ist abgelaufen. Bitte die Preise erneut abrufen.'); }, Math.max(0, expires - Date.now()));
  };

  button.addEventListener('click', async () => {
    clear();
    const query = {groesse:value('reifen_groesse_vorne'), saison:value('reifen_saison'), qualitaet:value('reifen_qualitaet'), anzahl:value('reifen_anzahl'), montage:value('reifen_montage')};
    if (!query.groesse || !seasons[query.saison] || !qualities[query.qualitaet] || !/^[1-8]$/.test(query.anzahl) || !['ja','nein'].includes(query.montage)) {
      say('Für die Preissuche bitte Größe, Saison, Anzahl, Marken- und Preiswahl sowie Montagewahl ergänzen.', true); return;
    }
    if (value('reifen_groesse_hinten') && value('reifen_groesse_hinten').toUpperCase().replace(/\s/g,'') !== query.groesse.toUpperCase().replace(/\s/g,'')) {
      say('Bei unterschiedlichen Größen vorne und hinten erstellen wir Ihnen über die persönliche Anfrage ein Gesamtangebot.'); return;
    }
    const current = generation;
    controller = new AbortController();
    const requestController = controller;
    const timeout = window.setTimeout(() => requestController.abort(), 12000);
    button.disabled = true; button.textContent = 'Preise werden geladen …'; panel.setAttribute('aria-busy', 'true');
    say('Passende Reifen und aktuelle Preise werden gesucht.');
    try {
      const url = new URL(panel.dataset.searchUrl, window.location.origin);
      url.search = new URLSearchParams(query).toString();
      const response = await fetch(url, {signal:requestController.signal, cache:'no-store', credentials:'same-origin', headers:{Accept:'application/json'}});
      if (response.status === 400) {
        if (current === generation) say('Bitte die Reifengröße und Ihre Auswahl prüfen, zum Beispiel 205/55 R16 91V.', true);
        return;
      }
      if (response.status === 429) {
        if (current === generation) say('Bitte warten Sie vor der nächsten Preissuche etwas.');
        return;
      }
      if (!response.ok) throw new Error('Suche nicht verfügbar');
      const payload = await response.json();
      if (current !== generation) return;
      if (payload.status === 'keine_treffer') {
        say('Für diese Auswahl sind gerade keine bestätigten Preise verfügbar. Wir erstellen Ihnen gern ein persönliches Angebot.'); return;
      }
      if (payload.status !== 'ok' || payload.verfuegbar !== true || !Array.isArray(payload.produkte)) {
        say('Onlinepreise sind gerade nicht verfügbar. Über die persönliche Anfrage erhalten Sie Ihr Angebot innerhalb von 24 Stunden per E-Mail.'); return;
      }
      const products = payload.produkte.slice(0,12).filter(p => validProduct(p, query));
      if (!products.length) { say('Für diese Auswahl sind gerade keine bestätigten Preise verfügbar. Wir erstellen Ihnen gern ein persönliches Angebot.'); return; }
      render(products, query);
      say(`${products.length} passende${products.length === 1 ? 's Angebot' : ' Angebote'}. Preise inklusive Mehrwertsteuer.${panel.dataset.orderEnabled === 'false' ? ' Der Bestellabschluss ist noch geschlossen.' : ` Reifen auswählen und Bestellung mit Zahlungsart${query.montage === 'ja' ? ' und Montagetermin' : ''} prüfen.`}`);
    } catch (_) {
      if (current === generation) say('Die Preise konnten gerade nicht abgerufen werden. Über die persönliche Anfrage helfen wir Ihnen weiter.');
    } finally {
      window.clearTimeout(timeout);
      if (current === generation) {
        controller = null; button.disabled = false; button.textContent = 'Reifenpreise anzeigen'; panel.removeAttribute('aria-busy');
      }
    }
  });
  if (standalone) form.addEventListener('submit', event => { event.preventDefault(); button.click(); });
  criteria.forEach(name => {
    form.querySelectorAll(`[name="${name}"]`).forEach(field => {
      field.addEventListener('input', () => { clear(); say('Auswahl geändert. Bitte Preise erneut abrufen.'); });
      field.addEventListener('change', () => { clear(); say('Auswahl geändert. Bitte Preise erneut abrufen.'); });
    });
  });
  panel.hidden = false;
})();
