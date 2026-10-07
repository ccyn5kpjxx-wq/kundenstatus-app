/* Personal photo requests. Product, package and price approval stay in the workshop. */
(function (root, factory) {
  'use strict';
  if (typeof module === 'object' && module.exports) module.exports = {createMaterialOrderController: factory};
  else if (root.document.getElementById('materialbestellung')) {
    let storage = null;
    try { storage = root.sessionStorage; } catch (_) { /* Private browsing may deny storage. */ }
    root.MaterialOrder = factory({document: root.document, fetch: root.fetch.bind(root), URL: root.URL,
      crypto: root.crypto, FormData: root.FormData, AbortController: root.AbortController,
      storage, window: root, setTimeout: root.setTimeout.bind(root), clearTimeout: root.clearTimeout.bind(root)});
    void root.MaterialOrder.refresh();
  }
})(typeof window === 'undefined' ? globalThis : window, function (options) {
  'use strict';
  const {document, fetch, URL, crypto, FormData, AbortController, storage, window} = options;
  const schedule = options.setTimeout || setTimeout, cancelTimer = options.clearTimeout || clearTimeout;
  const timeoutMs = options.timeoutMs || 45000;
  const $ = id => document.getElementById('material-' + id), host = document.getElementById('materialbestellung');
  const endpoint = host.dataset.endpoint, token = document.querySelector('meta[name="csrf-token"]')?.content || '';
  const storageKey = 'materialbestellung:pending:' + host.dataset.actor;
  const photos = [], replies = new Map();
  let pending = null, busy = false, refreshing = false, history = [], identityExpired = false, intentionalReload = false;
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
    $('submit').disabled = identityExpired || busy || (!pending && !photos.length) || Boolean(pending?.restored);
    $('submit').textContent = identityExpired ? 'Bitte Seite neu laden' : busy ? 'Bestellwünsche werden erfasst …' : pending ? 'Unverändert erneut versuchen' : 'Bestellwünsche senden →';
    $('form').setAttribute('aria-busy', String(busy));
    $('count').textContent = photos.length + ' / 10';
    $('empty').hidden = photos.length > 0;
    const total = photos.reduce((sum, photo) => sum + photo.quantity, 0);
    $('summary').textContent = photos.length ? photos.length + (photos.length === 1 ? ' Artikel' : ' Artikel') + ' · ' + total + ' Stück' : 'Noch kein Foto ausgewählt';
    for (const photo of photos) {
      photo.controls.minus.disabled = locked() || photo.quantity <= 1;
      photo.controls.plus.disabled = locked() || photo.quantity >= 999;
      photo.controls.quantity.disabled = locked(); photo.controls.quantity.value = String(photo.quantity);
      photo.controls.urgent.disabled = locked(); photo.controls.urgent.checked = photo.urgent;
      photo.controls.remove.disabled = locked();
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
    for (const photo of photos) URL.revokeObjectURL(photo.url);
    photos.splice(0); $('items').replaceChildren();
  }
  function updateQuantity(photo, value) {
    if (locked()) { controls(); return; }
    const text = String(value);
    if (!/^\d{1,3}$/.test(text) || Number(text) < 1 || Number(text) > 999) {
      note('Bitte eine ganze Stückzahl von 1 bis 999 wählen.', 'error'); controls(); return;
    }
    photo.quantity = Number(text); note(''); controls();
  }
  function renderPhotos() {
    $('items').replaceChildren();
    for (const [index, photo] of photos.entries()) {
      const card = node('article', undefined, 'photo-card'); card.dataset.clientId = photo.id;
      const img = node('img', undefined, 'photo-preview'); img.src = photo.url; img.alt = 'Produktfoto für Artikel ' + (index + 1);
      const details = node('div', undefined, 'photo-details'), heading = node('div', undefined, 'photo-title-row');
      const title = node('h3', 'Artikel ' + (index + 1));
      const remove = node('button', 'Entfernen', 'remove-button'); remove.type = 'button';
      remove.setAttribute('aria-label', 'Artikel ' + (index + 1) + ' entfernen');
      remove.addEventListener('click', () => {
        if (locked()) return;
        const at = photos.indexOf(photo); if (at < 0) return;
        URL.revokeObjectURL(photo.url); photos.splice(at, 1); note(''); renderPhotos();
      });
      heading.append(title, remove);
      const row = node('div', undefined, 'quantity-row'), label = node('label', 'Stückzahl');
      label.setAttribute('for', 'quantity-' + photo.id);
      const stepper = node('div', undefined, 'quantity-stepper');
      const minus = node('button', '−'), plus = node('button', '+'), quantity = node('input');
      minus.type = plus.type = 'button'; minus.setAttribute('aria-label', 'Stückzahl für Artikel ' + (index + 1) + ' verringern');
      plus.setAttribute('aria-label', 'Stückzahl für Artikel ' + (index + 1) + ' erhöhen');
      quantity.type = 'number'; quantity.id = 'quantity-' + photo.id; quantity.min = '1'; quantity.max = '999'; quantity.step = '1';
      quantity.inputMode = 'numeric'; quantity.required = true; quantity.value = String(photo.quantity);
      minus.addEventListener('click', () => updateQuantity(photo, Math.max(1, photo.quantity - 1)));
      plus.addEventListener('click', () => updateQuantity(photo, Math.min(999, photo.quantity + 1)));
      quantity.addEventListener('change', () => updateQuantity(photo, quantity.value));
      stepper.append(minus, quantity, plus); row.append(label, stepper);
      const urgentLabel = node('label', undefined, 'urgent-label'), urgent = node('input'); urgent.type = 'checkbox'; urgent.checked = photo.urgent;
      urgent.addEventListener('change', () => { if (!locked()) photo.urgent = urgent.checked; controls(); });
      urgentLabel.append(urgent, node('span', 'Dringend · vor dem Sammeltermin'));
      details.append(heading, row, urgentLabel, node('p', photo.file.name, 'photo-filename')); card.append(img, details);
      photo.controls = {minus, plus, quantity, urgent, remove}; $('items').append(card);
    }
    controls();
  }
  function validateFiles(files) {
    if (!files.length) return '';
    if (photos.length + files.length > 10) return 'Du kannst höchstens 10 Fotos auf einmal senden. Bitte weniger Fotos auswählen.';
    let size = photos.reduce((sum, photo) => sum + photo.file.size, 0);
    for (const file of files) {
      if (/\.(heic|heif)$/i.test(file.name || '') || /image\/(heic|heif)/i.test(file.type || '')) {
        return 'Dieses iPhone-Foto ist im HEIC-Format. Bitte ein JPEG auswählen oder in den Kameraeinstellungen „Maximale Kompatibilität“ verwenden.';
      }
      const type = String(file.type || '').toLowerCase();
      if (!(type ? ['image/jpeg', 'image/png', 'image/webp'].includes(type) : /\.(jpe?g|png|webp)$/i.test(file.name || ''))) {
        return 'Bitte ein Produktfoto als JPEG, PNG oder WebP auswählen. Andere Dateiformate werden nicht übernommen.';
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
    for (const file of selected) photos.push({id: crypto.randomUUID(), file, quantity: 1, urgent: false, url: URL.createObjectURL(file)});
    if (selected.length) { note(''); renderPhotos(); }
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
    const retrying = Boolean(pending);
    // Commit the focused number field before freezing the exact batch.
    if (!pending) for (const photo of photos) {
      const value = String(photo.controls.quantity.value);
      if (!/^\d{1,3}$/.test(value) || Number(value) < 1 || Number(value) > 999) {
        note('Bitte für jeden Artikel eine ganze Stückzahl von 1 bis 999 wählen.', 'error'); photo.controls.quantity.focus(); return;
      }
      photo.quantity = Number(value);
    }
    if (!pending) {
      pending = {id: crypto.randomUUID(), clientIds: photos.map(photo => photo.id), rows: photos.map(photo => ({
        id: photo.id, menge: photo.quantity, dringend: photo.urgent, file: photo.file}))};
      persist({id: pending.id, clientIds: pending.clientIds});
    }
    const batch = pending;
    const body = new FormData(); body.append('csrf_token', token); body.append('request_id', batch.id);
    body.append('positionen', JSON.stringify(batch.rows.map(({id, menge, dringend}) => ({id, menge, dringend}))));
    for (const row of batch.rows) body.append('foto_' + row.id, row.file, row.file.name);
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
    if (row.state === 'cancelled') return 'Dieser Bestellwunsch wurde geschlossen.';
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
      card.append(head, node('p', amount + ' · ' + (row.urgent ? 'Dringend' : 'Regulär · Montag 14 Uhr'), 'history-meta'), node('p', explanation(row), 'history-note'));
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
    $(source + '-button').addEventListener('click', () => { if (!locked()) $(source).click(); });
    $(source).addEventListener('change', () => { addFiles($(source).files); $(source).value = ''; });
  }
  $('form').addEventListener('submit', event => {event.preventDefault(); void submit();});
  $('refresh').addEventListener('click', () => {void refresh();}); $('check').addEventListener('click', () => {void refresh();});
  $('reload').addEventListener('click', () => {intentionalReload = true; window?.location?.reload();});
  window?.addEventListener('beforeunload', event => {if (!intentionalReload && (pending || [...replies.values()].some(reply => reply.busy || reply.id))) {event.preventDefault(); event.returnValue = '';}});
  if (pending) note('Der letzte Sendeausgang wird anhand deiner gespeicherten Wünsche geprüft.', 'uncertain');
  controls();
  return {addFiles, submit, refresh};
});
