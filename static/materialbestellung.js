/* Personal photo requests. Product, package and price approval stay in the workshop. */
(function (root, factory) {
  'use strict';
  if (typeof module === 'object' && module.exports) module.exports = {createMaterialOrderController: factory};
  else if (root.document.getElementById('materialbestellung')) {
    let storage = null;
    try { storage = root.sessionStorage; } catch (_) { /* Private browsing may deny storage. */ }
    root.MaterialOrder = factory({document: root.document, fetch: root.fetch.bind(root), URL: root.URL,
      crypto: root.crypto, FormData: root.FormData, AbortController: root.AbortController,
      storage, window: root, scannerFactory: root.createMaterialCodeScanner,
      setTimeout: root.setTimeout.bind(root), clearTimeout: root.clearTimeout.bind(root)});
    void root.MaterialOrder.refresh();
  }
})(typeof window === 'undefined' ? globalThis : window, function (options) {
  'use strict';
  const {document, fetch, URL, crypto, FormData, AbortController, storage, window} = options;
  const schedule = options.setTimeout || setTimeout, cancelTimer = options.clearTimeout || clearTimeout;
  const timeoutMs = options.timeoutMs || 45000;
  const $ = id => document.getElementById('material-' + id), host = document.getElementById('materialbestellung');
  const endpoint = host.dataset.endpoint, token = document.querySelector('meta[name="csrf-token"]')?.content || '';
  const previewEndpoint = host.dataset.previewEndpoint;
  const limitCents = Number(host.dataset.limitCents);
  const orderLimit = Number.isInteger(limitCents) && limitCents > 0 && limitCents <= 25000
    ? (limitCents / 100).toLocaleString('de-DE', {maximumFractionDigits: 2}) + ' € brutto' : '';
  const storageKey = 'materialbestellung:pending:' + host.dataset.actor;
  const photos = [], replies = new Map();
  let pending = null, busy = false, refreshing = false, history = [], identityExpired = false, intentionalReload = false;
  let scanner = null;
  const previewQueue = [];
  let previewBusy = false;
  let labelTarget = null;
  const node = (tag, text, className) => {
    const el = document.createElement(tag);
    if (text !== undefined) el.textContent = String(text);
    if (className) el.className = className;
    return el;
  };
  const note = (text, kind = '') => { $('status').textContent = text; $('status').dataset.kind = kind; };
  const locked = () => busy || Boolean(pending) || identityExpired;
  function persist(value) {
    try { if (value) storage?.setItem(storageKey, JSON.stringify(value)); else storage?.removeItem(storageKey); }
    catch (_) { /* In-memory idempotent retry remains available. */ }
  }
  try {
    const saved = JSON.parse(storage?.getItem(storageKey) || 'null');
    if (saved && typeof saved.id === 'string' && Array.isArray(saved.clientIds) && saved.clientIds.length) {
      pending = {id: saved.id, clientIds: saved.clientIds, restored: true};
    }
  } catch (_) { /* Ignore invalid browser state, never use it as an order. */ }

  function controls() {
    $('camera').disabled = locked(); $('album').disabled = locked();
    $('camera-button').disabled = locked() || photos.length >= 10;
    $('album-button').disabled = locked() || photos.length >= 10;
    if ($('scan-button')) $('scan-button').disabled = locked() || photos.length >= 10;
    if (locked()) scanner?.stop(false);
    updateSubmitControl();
    $('submit').textContent = identityExpired ? 'Bitte Seite neu laden' : busy ? 'Wünsche werden erfasst …' : pending ? 'Unverändert erneut versuchen' : photos.some(photo => photo.vorgang === 'anfrage') ? 'Wünsche senden →' : 'Bestellwünsche senden →';
    $('form').setAttribute('aria-busy', String(busy));
    $('count').textContent = photos.length + ' / 10';
    $('empty').hidden = photos.length > 0;
    const total = photos.reduce((sum, photo) => sum + photo.quantity, 0);
    $('summary').textContent = photos.length ? photos.length + ' Artikel · ' + total + ' Stück' : 'Noch kein Artikel erfasst';
    for (const photo of photos) {
      photo.controls.minus.disabled = locked() || photo.quantity <= 1;
      photo.controls.plus.disabled = locked() || photo.quantity >= 999;
      photo.controls.quantity.disabled = locked();
      // Choosing urgency must not overwrite a number that is still being
      // edited; some mobile browsers emit its change event only later.
      if (locked()) photo.controls.quantity.value = String(photo.quantity);
      photo.controls.urgent.disabled = locked(); photo.controls.urgent.checked = photo.urgent;
      photo.controls.monday.disabled = locked(); photo.controls.monday.checked = !photo.urgent;
      photo.controls.inquiry.disabled = locked(); photo.controls.inquiry.checked = photo.vorgang === 'anfrage';
      photo.controls.description.disabled = locked(); photo.controls.description.value = photo.beschreibung;
      photo.controls.mondayTitle.textContent = 'Nicht dringend';
      photo.controls.mondayDetail.textContent = photo.vorgang === 'anfrage' ? 'Intern klären' : 'Montag · 14 Uhr';
      photo.controls.urgentDetail.textContent = photo.vorgang === 'anfrage' ? 'Zeitnah intern klären' : orderLimit ? 'Sofort · bis ' + orderLimit : 'Sofort nach Klärung';
      photo.controls.monday.setAttribute('aria-label', photo.controls.mondayTitle.textContent + ' für Artikel ' + (photos.indexOf(photo) + 1));
      photo.controls.timingHint.textContent = photo.vorgang === 'anfrage'
        ? 'Nur eine Teileanfrage. Die Werkstattleitung klärt das Teil; es wird noch nicht bestellt.'
        : photo.urgent ? (orderLimit ? 'Automatisch sofort bis ' + orderLimit + ' inklusive Versand und Nebenkosten, sobald Artikel, Lieferant und Gesamtkosten eindeutig sind. Offene Angaben oder höhere Beträge klärt die Werkstattleitung.' : 'Artikel, Lieferant und Kosten werden intern geklärt. Die persönliche Bestellgrenze muss feststehen.') : 'Sammelbestellung am Montag um 14 Uhr.';
      photo.controls.remove.disabled = locked();
      for (const button of photo.previewNodes?.actions || []) button.disabled = locked();
    }
    $('recovery').hidden = !pending && !identityExpired;
    $('recovery-text').textContent = identityExpired ? 'Deine Anmeldung oder Berechtigung hat sich geändert. Lade die Seite neu, bevor du weiterarbeitest. Ein vorheriger unklarer Eingang wird unter dem ursprünglichen persönlichen Zugang geprüft.' : pending?.restored
      ? 'Ein vorheriger Sendeausgang ist noch unklar. Prüfe den gespeicherten Eingang. Bitte sende dieselben Artikel nicht als neuen Wunsch. Die Fotos der vorherigen Seite sind nach dem Neuladen nicht mehr verfügbar.'
      : 'Der Eingang ist noch nicht sicher bestätigt. Fotos und Mengen bleiben unverändert. Lass diese Seite geöffnet und wiederhole denselben Versuch oder prüfe die gespeicherten Wünsche.';
    $('check').disabled = identityExpired || busy || refreshing;
    $('check').hidden = identityExpired; $('reload').hidden = !identityExpired;
    $('refresh').disabled = identityExpired || refreshing;
  }
  function clearPhotos() {
    previewQueue.splice(0);
    labelTarget = null;
    for (const photo of photos) {URL.revokeObjectURL(photo.url); if (photo.labelUrl) URL.revokeObjectURL(photo.labelUrl);}
    photos.splice(0); $('items').replaceChildren();
  }
  function updateQuantity(photo, value) {
    if (locked()) { controls(); return; }
    const text = String(value);
    if (!/^\d{1,3}$/.test(text) || Number(text) < 1 || Number(text) > 999) {
      photo.controls.quantity.value = String(photo.quantity);
      photo.quantityDraft = String(photo.quantity);
      note('Bitte eine ganze Stückzahl von 1 bis 999 wählen.', 'error'); controls(); return;
    }
    photo.quantity = Number(text); photo.quantityDraft = text; photo.controls.quantity.value = text; note(''); controls();
  }
  function renderPhotos() {
    $('items').replaceChildren();
    for (const [index, photo] of photos.entries()) {
      const card = node('article', undefined, 'photo-card'); card.dataset.clientId = photo.id;
      const img = node('img', undefined, 'photo-preview'); img.src = photo.url; img.alt = 'Foto oder Screenshot für Artikel ' + (index + 1);
      const original = node('a', undefined, 'photo-open'); original.href = photo.url;
      original.target = '_blank'; original.rel = 'noopener noreferrer';
      original.setAttribute('aria-label', 'Originalfoto für Artikel ' + (index + 1) + ' öffnen');
      original.append(img, node('span', 'Foto öffnen'));
      const details = node('div', undefined, 'photo-details'), heading = node('div', undefined, 'photo-title-row');
      const title = node('h3', 'Artikel ' + (index + 1));
      const remove = node('button', 'Entfernen', 'remove-button'); remove.type = 'button';
      remove.setAttribute('aria-label', 'Artikel ' + (index + 1) + ' entfernen');
      remove.addEventListener('click', () => {
        if (locked()) return;
        const at = photos.indexOf(photo); if (at < 0) return;
        URL.revokeObjectURL(photo.url); if (photo.labelUrl) URL.revokeObjectURL(photo.labelUrl);
        if (labelTarget === photo) labelTarget = null;
        photos.splice(at, 1); note(''); renderPhotos();
      });
      heading.append(title, remove);
      const row = node('div', undefined, 'quantity-row'), label = node('label', 'Stückzahl');
      label.setAttribute('for', 'quantity-' + photo.id);
      const stepper = node('div', undefined, 'quantity-stepper');
      const minus = node('button', '−'), plus = node('button', '+'), quantity = node('input');
      minus.type = plus.type = 'button'; minus.setAttribute('aria-label', 'Stückzahl für Artikel ' + (index + 1) + ' verringern');
      plus.setAttribute('aria-label', 'Stückzahl für Artikel ' + (index + 1) + ' erhöhen');
      quantity.type = 'number'; quantity.id = 'quantity-' + photo.id; quantity.min = '1'; quantity.max = '999'; quantity.step = '1';
      quantity.inputMode = 'numeric'; quantity.required = true; quantity.value = photo.quantityDraft ?? String(photo.quantity);
      minus.addEventListener('click', () => updateQuantity(photo, Math.max(1, photo.quantity - 1)));
      plus.addEventListener('click', () => updateQuantity(photo, Math.min(999, photo.quantity + 1)));
      quantity.addEventListener('input', () => {
        if (locked()) return;
        photo.quantityDraft = quantity.value;
        if (/^\d{1,3}$/.test(quantity.value) && Number(quantity.value) >= 1 && Number(quantity.value) <= 999) {
          photo.quantity = Number(quantity.value);
        }
      });
      quantity.addEventListener('change', () => updateQuantity(photo, quantity.value));
      stepper.append(minus, quantity, plus); row.append(label, stepper);
      details.append(heading, row, node('p', photo.file.name, 'photo-filename')); card.append(original, details);
      const previewPanel = node('div', undefined, 'article-preview');
      previewPanel.setAttribute('role', 'status'); previewPanel.setAttribute('aria-live', 'polite');
      photo.previewNodes = {title, panel: previewPanel, index, actions: []};
      card.append(previewPanel); paintPreview(photo);
      const timing = node('fieldset', undefined, 'timing-choice');
      timing.append(node('legend', 'Wann wird es gebraucht?'));
      const timingOptions = node('div', undefined, 'timing-options');
      const monday = node('input'), urgent = node('input');
      let mondayTitle, mondayDetail, urgentDetail;
      for (const [input, value, titleText, detailText] of [[monday, 'montag', 'Nicht dringend', 'Montag · 14 Uhr'], [urgent, 'dringend', 'Dringend', 'Sofort']]) {
        input.type = 'radio'; input.name = 'timing-' + photo.id; input.value = value;
        input.setAttribute('aria-label', titleText + ' für Artikel ' + (index + 1));
        input.addEventListener('change', () => { if (!locked() && input.checked) photo.urgent = value === 'dringend'; controls(); });
        const choice = node('label', undefined, 'timing-option');
        const copy = node('span'), titleNode = node('strong', titleText), detailNode = node('small', detailText);
        if (input === monday) { mondayTitle = titleNode; mondayDetail = detailNode; }
        else urgentDetail = detailNode;
        copy.append(titleNode, detailNode);
        choice.append(input, copy); timingOptions.append(choice);
      }
      const timingHint = node('p', '', 'timing-hint'); timing.append(timingOptions, timingHint); card.append(timing);
      const extra = node('details', undefined, 'photo-extra');
      photo.extra = extra;
      if (photo.preview && !['loading', 'matched', 'label'].includes(photo.preview.lookup_status)) extra.open = true;
      extra.append(node('summary', 'Beschreibung oder Teil anfragen (optional)'));
      const descriptionLabel = node('label', 'Kurze Beschreibung', 'description-label'), description = node('textarea');
      description.id = 'description-' + photo.id; description.rows = 2; description.maxLength = 500;
      description.placeholder = 'Zum Beispiel: Abdeckfolie oder Halter am Kotflügel';
      descriptionLabel.setAttribute('for', description.id);
      description.addEventListener('input', () => { if (!locked()) photo.beschreibung = description.value; else description.value = photo.beschreibung; });
      const inquiryLabel = node('label', undefined, 'inquiry-label'), inquiry = node('input'); inquiry.type = 'checkbox';
      inquiry.setAttribute('aria-label', 'Teil nur anfragen für Artikel ' + (index + 1));
      inquiry.addEventListener('change', () => { if (!locked()) photo.vorgang = inquiry.checked ? 'anfrage' : 'bestellung'; controls(); });
      inquiryLabel.append(inquiry, node('span', 'Teil nur anfragen · noch nicht bestellen'));
      extra.append(descriptionLabel, description, inquiryLabel); card.append(extra);
      photo.controls = {minus, plus, quantity, urgent, monday, mondayTitle, mondayDetail, urgentDetail, inquiry, description, timingHint, remove}; $('items').append(card);
    }
    controls();
  }
  function validateFiles(files, replacingLabel = null) {
    if (!files.length) return '';
    if (!replacingLabel && photos.length + files.length > 10) return 'Du kannst höchstens 10 Fotos auf einmal senden. Bitte weniger Fotos auswählen.';
    let size = photos.reduce((sum, photo) => sum + photo.file.size + (photo === replacingLabel ? 0 : photo.labelFile?.size || 0), 0);
    for (const file of files) {
      if (/\.(heic|heif)$/i.test(file.name || '') || /image\/(heic|heif)/i.test(file.type || '')) {
        return 'Dieses iPhone-Foto ist im HEIC-Format. Bitte ein JPEG auswählen oder in den Kameraeinstellungen „Maximale Kompatibilität“ verwenden.';
      }
      const type = String(file.type || '').toLowerCase();
      if (!(type ? ['image/jpeg', 'image/png', 'image/webp'].includes(type) : /\.(jpe?g|png|webp)$/i.test(file.name || ''))) {
        return 'Bitte ein Foto oder einen Screenshot als JPEG, PNG oder WebP auswählen.';
      }
      if (!Number.isFinite(file.size) || file.size < 1 || file.size > 8 * 1024 * 1024) return 'Jedes Foto muss kleiner als oder gleich 8 MB sein und darf nicht leer sein.';
      size += file.size;
    }
    return size > 50 * 1024 * 1024 ? 'Alle ausgewählten Fotos zusammen dürfen höchstens 50 MB groß sein.' : '';
  }
  function addFiles(files) {
    if (locked()) return;
    const selected = Array.from(files || []), error = validateFiles(selected);
    if (error) { note(error, 'error'); return; }
    for (const file of selected) {
      const photo = {id: crypto.randomUUID(), file, quantity: 1, urgent: false, vorgang: 'bestellung', beschreibung: '', url: URL.createObjectURL(file),
        preview: null, artikelkorrektur: false, rejectedCodes: [], previewVersion: 0, confirmedPreview: false};
      photos.push(photo);
      if (previewEndpoint) enqueuePreview(photo, file, false);
    }
    if (selected.length) { note(''); renderPhotos(); void runPreviews(); }
  }
  function needsConfirmation(photo) {return photo.preview?.lookup_status === 'loading' || Boolean(photo.preview?.product && ['matched', 'label'].includes(photo.preview.lookup_status) && !photo.confirmedPreview);}
  function updateSubmitControl() {
    $('submit').disabled = identityExpired || busy || (!pending && !photos.length) || Boolean(pending?.restored)
      || (!pending && photos.some(photo => needsConfirmation(photo)));
  }
  function enqueuePreview(photo, file, readLabel) {
    photo.confirmedPreview = false;
    photo.preview = {lookup_status: 'loading', message: readLabel ? 'Produktname auf dem Etikett wird gelesen …' : 'Artikelcode wird gelesen und im Katalog gesucht …'};
    previewQueue.push({photo, file, readLabel, version: ++photo.previewVersion});
  }
  function chooseLabel(photo, reject = false) {
    if (locked() || !photos.includes(photo)) return;
    photo.artikelkorrektur = true;
    if (reject) {
      const rejected = [photo.preview?.decodedCode, ...(photo.preview?.matches || []).map(hit => hit.artikelnummer)].filter(Boolean);
      photo.rejectedCodes = [...new Set([...photo.rejectedCodes, ...rejected])].slice(0, 8);
      if (photo.previewDescription && photo.beschreibung === photo.previewDescription) {
        photo.beschreibung = ''; photo.controls.description.value = '';
      }
      photo.previewDescription = '';
      photo.previewVersion++;
      photo.preview = {lookup_status: 'awaiting_label', message: 'Zuordnung abgelehnt. Fotografiere jetzt das Etikett mit dem ausgeschriebenen Produktnamen oder trage ihn unten ein.'};
      photo.confirmedPreview = false; paintPreview(photo); updateSubmitControl();
    }
    labelTarget = photo; $('camera').click();
  }
  function addLabel(photo, files) {
    if (locked() || !photos.includes(photo)) return;
    const file = Array.from(files || [])[0]; if (!file) return;
    // Validate with the same supported formats and size ceiling, retaining the
    // first scan photo and counting both originals against the upload limit.
    const error = validateFiles([file], photo);
    if (error) {note(error, 'error'); return;}
    if (photo.labelUrl) URL.revokeObjectURL(photo.labelUrl);
    photo.labelFile = file; photo.labelUrl = URL.createObjectURL(file); photo.artikelkorrektur = true;
    enqueuePreview(photo, file, true); renderPhotos(); void runPreviews();
  }
  function paintPreview(photo) {
    if (!photo.previewNodes) return;
    const {title, panel, index} = photo.previewNodes, result = photo.preview;
    panel.replaceChildren(); panel.hidden = !result;
    title.textContent = ['matched', 'label'].includes(result?.lookup_status) && result.product ? result.product : 'Artikel ' + (index + 1);
    photo.previewNodes.actions = [];
    if (!result) return;
    panel.dataset.state = result.lookup_status;
    if (result.decodedCode) panel.append(node('p', 'Artikelcode: ' + result.decodedCode, 'article-code'));
    panel.append(node('p', result.message || 'Bitte Foto und Artikel intern zuordnen.', 'article-preview-message'));
    for (const match of Array.isArray(result.matches) ? result.matches.slice(0, 8) : []) {
      const candidate = node('div', undefined, 'article-candidate');
      candidate.append(node('strong', match.produkt_name || 'Artikel prüfen'));
      candidate.append(node('p', [match.lieferant, match.artikelnummer, match.groesse, match.farbe, match.gebinde, match.ve].filter(Boolean).join(' · ')));
      if (match.quelle) {
        const source = match.quelle;
        candidate.append(node('small', 'Quelle: ' + (source.art === 'lexware' ? 'Lieferantenrechnung' : 'Einkaufsbeleg') + ' ' + source.beleg_id
          + (source.seite ? ' · Seite ' + source.seite : '') + (source.datum ? ' · ' + source.datum : '')));
      }
      panel.append(candidate);
    }
    if (photo.labelUrl) {
      const labelOriginal = node('a', 'Etikettfoto öffnen'); labelOriginal.href = photo.labelUrl;
      labelOriginal.target = '_blank'; labelOriginal.rel = 'noopener noreferrer'; panel.append(labelOriginal);
    }
    const actions = node('div', undefined, 'article-preview-actions');
    const action = (text, handler) => {
      const button = node('button', text); button.type = 'button'; button.disabled = locked();
      button.addEventListener('click', handler); actions.append(button); photo.previewNodes.actions.push(button);
    };
    if (['matched', 'label'].includes(result.lookup_status) && result.product) {
      if (photo.confirmedPreview) actions.append(node('strong', 'Artikel bestätigt · Stückzahl wählen'));
      else action('Artikel stimmt', () => {
        if (locked()) return;
        photo.confirmedPreview = true;
        if (photo.artikelkorrektur && (!photo.beschreibung || photo.beschreibung === photo.previewDescription)) {
          photo.beschreibung = result.product; photo.previewDescription = result.product;
          photo.controls.description.value = photo.beschreibung;
        }
        paintPreview(photo); updateSubmitControl(); photo.controls.quantity.focus();
      });
      action('Falsches Produkt · Etikett fotografieren', () => chooseLabel(photo, true));
    } else if (result.lookup_status !== 'loading') action('Etikett mit Produktnamen fotografieren', () => chooseLabel(photo));
    panel.append(actions);
    if (!['loading', 'matched', 'label'].includes(result.lookup_status) && photo.extra) photo.extra.open = true;
  }
  async function runPreviews() {
    if (previewBusy || !previewEndpoint) return;
    previewBusy = true;
    try {
      while (previewQueue.length && !locked()) {
        const {photo, file, readLabel, version} = previewQueue.shift();
        if (!photos.includes(photo)) continue;
        const body = new FormData(); body.append('foto', file, file.name);
        if (readLabel) {body.append('modus', 'etikett'); body.append('abgelehnte_codes', JSON.stringify(photo.rejectedCodes));}
        try {
          const result = await request(previewEndpoint, {method: 'POST', body});
          if (photos.includes(photo) && photo.previewVersion === version) {photo.preview = result; paintPreview(photo); updateSubmitControl();}
        } catch (error) {
          if (!photos.includes(photo) || photo.previewVersion !== version) continue;
          photo.preview = {lookup_status: 'unavailable', message: error.reloadRequired
            ? 'Dein Zugang hat sich geändert. Bitte die Seite neu laden.'
            : 'Die Artikelvorschau ist gerade nicht verfügbar. Du kannst den Artikel beschreiben und das Foto senden.'};
          paintPreview(photo);
          updateSubmitControl();
          if (error.reloadRequired) {identityExpired = true; note(photo.preview.message, 'error'); controls();}
        }
      }
    } finally { previewBusy = false; }
  }
  async function request(path, settings = {}) {
    const abort = AbortController ? new AbortController() : null;
    let timer;
    const deadline = new Promise((_, reject) => { timer = schedule(() => {
      abort?.abort(); reject(new Error('Die Verbindung hat zu lange gebraucht.'));
    }, timeoutMs); });
    try {
      return await Promise.race([deadline, (async () => {
        const response = await fetch(path, {...settings, credentials: 'same-origin', redirect: 'error', signal: abort?.signal,
          headers: {'Accept': 'application/json', 'X-CSRF-Token': token, ...(settings.headers || {})}});
        let data;
        try { data = await response.json(); } catch (_) {
          const error = new Error('Der Server hat den Eingang nicht eindeutig bestätigt.');
          error.reloadRequired = response.status === 401 || response.status === 403; throw error;
        }
        if (!response.ok) {
          const error = new Error(data?.error || 'Die Anfrage konnte nicht verarbeitet werden.');
          error.safeRejected = data?.accepted === false; error.status = response.status;
          error.reloadRequired = Boolean(data?.reload_required) || response.status === 401 || response.status === 403; throw error;
        }
        return data;
      })()]);
    } finally { cancelTimer(timer); }
  }
  function batchComplete(rows, batch) {
    return batch.clientIds.every(id => rows.some(row => row.client_id === id && row.request_id === batch.id));
  }
  function received(rows) {
    const ids = new Set(rows.map(row => row.id));
    history = [...rows, ...history.filter(row => !ids.has(row.id))];
    pending = null; persist(null); clearPhotos();
    note('Bestellwünsche erfasst. Den aktuellen Stand siehst du unten.'); renderHistory();
  }
  async function submit() {
    if (identityExpired || busy || pending?.restored || (!pending && !photos.length)) return;
    if (!pending && photos.some(photo => needsConfirmation(photo))) {
      note(photos.some(photo => photo.preview?.lookup_status === 'loading') ? 'Der Artikel wird noch erkannt. Bitte kurz auf das Ergebnis warten.'
        : 'Bitte zuerst „Artikel stimmt“ wählen oder das falsch erkannte Produkt korrigieren.', 'error'); return;
    }
    scanner?.stop(false);
    previewQueue.splice(0);
    const retrying = Boolean(pending);
    // Commit the focused number field before freezing the exact batch.
    if (!pending) for (const photo of photos) {
      const value = String(photo.controls.quantity.value);
      if (!/^\d{1,3}$/.test(value) || Number(value) < 1 || Number(value) > 999) {
        note('Bitte für jeden Artikel eine ganze Stückzahl von 1 bis 999 wählen.', 'error'); photo.controls.quantity.focus(); return;
      }
      photo.quantity = Number(value);
      const description = String(photo.controls.description.value).trim();
      if (description.length > 500) {
        note('Bitte die Beschreibung auf höchstens 500 Zeichen kürzen.', 'error'); photo.controls.description.focus(); return;
      }
      photo.beschreibung = description;
    }
    if (!pending) {
      pending = {id: crypto.randomUUID(), clientIds: photos.map(photo => photo.id), rows: photos.map(photo => ({
        id: photo.id, menge: photo.quantity, dringend: photo.urgent, vorgang: photo.vorgang, beschreibung: photo.beschreibung, file: photo.file,
        ...(photo.artikelkorrektur ? {artikelkorrektur: true, abgelehnte_codes: [...photo.rejectedCodes],
          ...(photo.labelFile ? {etikett_datei: true, labelFile: photo.labelFile} : {})} : {})}))};
      persist({id: pending.id, clientIds: pending.clientIds});
    }
    const batch = pending;
    const body = new FormData(); body.append('csrf_token', token); body.append('request_id', batch.id);
    body.append('positionen', JSON.stringify(batch.rows.map(({file, labelFile, ...position}) => position)));
    for (const row of batch.rows) {
      body.append('foto_' + row.id, row.file, row.file.name);
      if (row.labelFile) body.append('etikett_' + row.id, row.labelFile, row.labelFile.name);
    }
    busy = true; note('Fotos und Stückzahlen werden erfasst …'); controls();
    try {
      const data = await request(endpoint, {method: 'POST', body});
      const rows = data?.anforderungen;
      if (data.request_id !== batch.id || !Array.isArray(rows) || rows.length !== batch.clientIds.length ||
          !batch.clientIds.every(id => rows.some(row => row.client_id === id && row.id && row.state))) {
        throw new Error('Der Server hat den vollständigen Eingang noch nicht bestätigt.');
      }
      received(rows);
    } catch (error) {
      if (error.reloadRequired) {
        identityExpired = true;
        if (error.safeRejected && !retrying) { pending = null; persist(null); }
        note(error.message + ' Bitte die Seite neu laden.', 'error');
      }
      else if (error.safeRejected) { pending = null; persist(null); note(error.message, 'error'); }
      else note(error.message + ' Der Sendeausgang ist unklar. Bitte unverändert erneut versuchen.', 'uncertain');
    } finally { busy = false; controls(); }
  }
  const sentStates = new Set(['sent', 'ordered', 'external_sent']);
  const staffQuestion = row => row.employee_reply_required && row.analysis_state !== 'pending' && row.analysis_state !== 'processing' && Array.isArray(row.questions)
    ? row.questions.find(entry => entry.field !== 'internal_review') : null;
  function explanation(row) {
    if (row.state === 'cancelled') return 'Dieser Wunsch wurde geschlossen.';
    if (row.vorgang === 'anfrage') return 'Teileanfrage erfasst. Die Werkstattleitung prüft das Bild und die Beschreibung. Noch keine Bestellung ausgelöst.';
    if (row.dispatch_state === 'sent' || row.dispatch_state === 'copy_pending') return 'Bestellung versandt.';
    if (row.dispatch_state === 'uncertain' || row.dispatch_state === 'partial') return 'Der Versandstatus ist unklar. Bitte nicht erneut bestellen.';
    if (row.dispatch_state === 'sending') return 'Der Versand läuft. Bitte nicht erneut bestellen.';
    if (row.dispatch_state === 'failed') return 'Der Versand ist fehlgeschlagen. Die Werkstattleitung prüft den Vorgang.';
    if (row.dispatch_state === 'blocked') return 'Der Bestellversand ist gesperrt. Die Werkstattleitung prüft den Vorgang.';
    if (row.dispatch_state === 'not_sent') return 'Der Versand ist noch offen. Die Werkstattleitung prüft den Vorgang.';
    if (row.dispatch_state === 'queued' || row.dispatch_state === 'ready') return row.urgent
      ? 'Für die dringende Bestellverarbeitung vorgemerkt. Noch nicht versandt.'
      : 'Für die Sammelbestellung am Montag um 14 Uhr vorgemerkt. Noch nicht versandt.';
    if (row.state === 'external_pending') return 'Der Versand wird geprüft. Bitte nicht erneut bestellen.';
    if (sentStates.has(row.state)) return 'Bestellung versandt.';
    if (row.state === 'accepted') return 'An die Bestellverarbeitung übergeben. Das bestätigt noch keinen Versand.';
    if (row.analysis_state === 'pending' || row.analysis_state === 'processing') return 'Foto wird ausgelesen. Noch nicht bestellt.';
    if (staffQuestion(row)) return 'Eine kurze Klärung ist nötig. Noch nicht bestellt.';
    if (row.state === 'approved') return row.urgent ? 'Intern geprüft. Dringende Verarbeitung steht an.' : 'Intern geprüft. Für die Sammelbestellung am Montag um 14 Uhr vorgemerkt.';
    return 'Erfasst. Artikel, Liefergebinde und aktuelle Kosten werden intern geprüft. Noch nicht bestellt.';
  }
  async function answer(row, text, status, buttons, input) {
    if (identityExpired) return;
    let reply = replies.get(row.id);
    if (reply?.busy) return;
    if (!reply && (!text || text.length > 200)) {
      status.textContent = 'Bitte eine kurze Antwort mit höchstens 200 Zeichen eingeben.';
      input?.focus(); return;
    }
    if (!reply) { reply = {id: crypto.randomUUID(), revision: row.revision, text, busy: false}; replies.set(row.id, reply); }
    reply.busy = true; buttons.forEach(button => {button.disabled = true;}); if (input) input.disabled = true;
    status.textContent = 'Antwort wird erfasst …';
    try {
      const data = await request(endpoint + '/' + encodeURIComponent(row.id) + '/antwort', {method: 'POST',
        headers: {'Content-Type': 'application/json'}, body: JSON.stringify({revision: reply.revision, antwort: reply.text, request_id: reply.id})});
      const updated = data.anforderung || data.anforderungen?.[0] || data;
      if (!updated.id || updated.id !== row.id || !updated.state) throw new Error('Die Antwort wurde noch nicht eindeutig bestätigt.');
      replies.delete(row.id); history = history.map(entry => entry.id === row.id ? updated : entry); renderHistory();
    } catch (error) {
      reply.busy = false;
      if (error.reloadRequired) {
        identityExpired = true; status.textContent = error.message + ' Bitte die Seite neu laden.'; controls();
      }
      else if (error.safeRejected) { replies.delete(row.id); buttons.forEach(button => {button.disabled = false;}); if (input) input.disabled = false; status.textContent = error.message; }
      else {
        status.textContent = error.message + ' Bitte dieselbe Antwort erneut versuchen.';
        buttons.forEach(button => {button.disabled = button.dataset.answer !== reply.text;});
      }
    }
  }
  function renderHistory() {
    $('history').replaceChildren();
    $('history-status').textContent = history.length ? '' : 'Hier erscheinen deine erfassten Bestellwünsche.';
    for (const row of history) {
      const card = node('article', undefined, 'history-card'); card.dataset.orderId = row.id;
      const head = node('div', undefined, 'history-card-head'), title = node('div');
      title.append(node('p', row.code || 'Bestellwunsch', 'history-code'), node('h3', row.product || 'Produktfoto wird geprüft'));
      head.append(title, node('span', row.label || 'Erfasst', 'state-badge'));
      const amount = [row.quantity || '—', row.unit || 'Stück'].join(' ');
      const timing = row.vorgang === 'anfrage' ? (row.urgent ? 'Dringende Teileanfrage' : 'Teileanfrage · nicht dringend') : (row.urgent ? 'Dringend' : 'Regulär · Montag 14 Uhr');
      card.append(head, node('p', amount + ' · ' + timing, 'history-meta'), node('p', explanation(row), 'history-note'));
      if (row.beschreibung) card.append(node('p', row.beschreibung, 'history-description'));
      if (row.decodedCode) card.append(node('p', 'Artikelcode erkannt: ' + row.decodedCode, 'history-code-result'));
      else if (row.code_erkennung?.status === 'mehrdeutig') card.append(node('p', 'Mehrere Codes im Foto. Die Zuordnung wird intern geprüft.', 'history-code-result'));
      const question = staffQuestion(row);
      if (question) {
        const area = node('div', undefined, 'history-question'), status = node('p', '', 'answer-status'), buttons = [];
        let answerInput = null;
        area.append(node('p', question.body));
        if (question.field === 'possible_duplicate') {
          const actions = node('div', undefined, 'answer-actions');
          for (const [text, label] of [['ja', 'Ja, zusätzlich benötigt'], ['nein', 'Nein, keine weitere']]) {
            const button = node('button', label, 'secondary-button'); button.type = 'button'; button.dataset.answer = text;
            button.addEventListener('click', () => { void answer(row, text, status, buttons); }); buttons.push(button); actions.append(button);
          }
          area.append(actions);
        } else {
          const form = node('form', undefined, 'answer-form'), label = node('label', 'Deine Antwort'), input = node('input');
          answerInput = input;
          input.id = 'answer-' + row.id; input.required = true; input.maxLength = 200; label.setAttribute('for', input.id);
          const button = node('button', 'Antwort senden', 'secondary-button'); button.type = 'submit'; buttons.push(button);
          form.append(label, input, button);
          form.addEventListener('submit', event => {
            event.preventDefault(); const text = input.value.trim(); if (!text && !replies.has(row.id)) return;
            const reply = replies.get(row.id); button.dataset.answer = reply?.text || text;
            void answer(row, text, status, buttons, input);
          }); area.append(form);
        }
        const reply = replies.get(row.id);
        if (reply) {
          status.textContent = 'Antwort noch nicht sicher bestätigt. Bitte unverändert erneut versuchen.';
          if (answerInput) { answerInput.value = reply.text; answerInput.disabled = true; buttons[0].dataset.answer = reply.text; }
          buttons.forEach(button => {button.disabled = reply.busy || (button.dataset.answer && button.dataset.answer !== reply.text);});
        }
        area.append(status); card.append(area);
      }
      $('history').append(card);
    }
  }
  async function refresh() {
    if (identityExpired || refreshing) return;
    refreshing = true; controls();
    try {
      const data = await request(endpoint), rows = data?.anforderungen;
      if (!Array.isArray(rows)) throw new Error('Bestellwünsche konnten nicht geladen werden.');
      history = rows;
      if (pending) {
        // The ordinary history may be capped. Verify the exact uncertain batch separately.
        const exact = batchComplete(rows, pending) ? rows : (await request(endpoint + '?request_id=' + encodeURIComponent(pending.id)))?.anforderungen;
        if (Array.isArray(exact) && batchComplete(exact, pending)) received(exact.filter(row => pending.clientIds.includes(row.client_id)));
      }
      renderHistory();
    } catch (error) {
      if (error.reloadRequired) {identityExpired = true; note(error.message + ' Bitte die Seite neu laden.', 'error');}
      $('history-status').textContent = error.message + (error.reloadRequired ? ' Bitte die Seite neu laden.' : ' Bitte erneut aktualisieren.');
    }
    finally { refreshing = false; controls(); }
  }
  for (const source of ['camera', 'album']) {
    $(source + '-button').addEventListener('click', () => { if (!locked()) {labelTarget = null; $(source).click();} });
    $(source).addEventListener('change', () => {
      const target = source === 'camera' ? labelTarget : null; labelTarget = null;
      if (target) addLabel(target, $(source).files); else addFiles($(source).files);
      $(source).value = '';
    });
    $(source).addEventListener('cancel', () => {labelTarget = null;});
  }
  if (options.scannerFactory && $('scan-button')) {
    scanner = options.scannerFactory({document, window, canAdd: () => !locked() && photos.length < 10,
      onPhoto: file => {const before = photos.length; addFiles([file]); if (photos.length > before) note('Codefoto hinzugefügt. Der Artikel wird gesucht; Stückzahl wählen und senden.');},
      onFallback: () => {labelTarget = null; $('camera').click();}});
    $('scan-button').addEventListener('click', () => {labelTarget = null; void scanner.open();});
  }
  $('form').addEventListener('submit', event => {event.preventDefault(); void submit();});
  $('refresh').addEventListener('click', () => {void refresh();}); $('check').addEventListener('click', () => {void refresh();});
  $('reload').addEventListener('click', () => {intentionalReload = true; window?.location?.reload();});
  window?.addEventListener('beforeunload', event => {if (!intentionalReload && (pending || [...replies.values()].some(reply => reply.busy || reply.id))) {event.preventDefault(); event.returnValue = '';}});
  if (pending) note('Der letzte Sendeausgang wird anhand deiner gespeicherten Wünsche geprüft.', 'uncertain');
  controls();
  return {addFiles, submit, refresh};
});
