/* Material selection only. The host owns the separate purchase review. */
(function (root, factory) {
  'use strict';
  if (typeof module === 'object' && module.exports) module.exports = {createMaterialPhotoController: factory};
  else if (root.AssistantMaterialPhotoHost && root.document.getElementById('materialfoto-dialog')) {
    root.AssistantMaterialPhoto = factory({host: root.AssistantMaterialPhotoHost, document: root.document,
      URL: root.URL, crypto: root.crypto, FormData: root.FormData});
  }
})(typeof window === 'undefined' ? globalThis : window, function (options) {
  'use strict';
  const {host, document, URL, crypto, FormData} = options;
  const $ = id => document.getElementById(id), dialog = $('materialfoto-dialog');
  const input = $('materialfoto-file'), submit = $('materialfoto-submit'), preview = $('materialfoto-preview');
  let generation = 0, busy = false, file = null, requestId = null, stagedId = null, previewUrl = null, photo = null;
  const current = run => run === generation && dialog.open;
  const item = value => value?.foto || value?.photo || value;
  const note = text => { $('materialfoto-status').textContent = text || ''; };
  const node = (tag, text) => { const el = document.createElement(tag); if (text !== undefined) el.textContent = text; return el; };
  function controls() {
    input.disabled = busy; submit.disabled = busy || !file;
    dialog.querySelectorAll('[data-materialfoto-action]').forEach(button => {button.disabled = busy;});
    dialog.setAttribute('aria-busy', String(busy));
  }
  async function work(fn) {
    if (busy) return;
    const run = generation; busy = true; controls();
    try { await fn(run); }
    catch (error) { if (current(run)) note(error?.message || 'Anfrage fehlgeschlagen. Bitte erneut versuchen.'); }
    finally { busy = false; controls(); }
  }
  function button(text, fn) {
    const el = node('button', text); el.type = 'button'; el.className = 'secondary';
    el.dataset.materialfotoAction = 'true'; el.onclick = () => work(fn); return el;
  }
  function discardPreview() {
    if (previewUrl) URL.revokeObjectURL(previewUrl);
    previewUrl = null; preview.removeAttribute('src'); preview.hidden = true;
  }
  function changed() {
    if (busy) return;
    discardPreview(); file = input.files?.[0] || null; requestId = null; stagedId = null; photo = null;
    $('materialfoto-results').replaceChildren(); note('');
    if (file && (file.size > 8 * 1024 * 1024 || file.size < 1)) {
      file = null; note('Bitte ein Foto zwischen 1 Byte und 8 MB auswählen.');
    }
    if (file) {
      requestId = crypto.randomUUID(); previewUrl = URL.createObjectURL(file);
      preview.src = previewUrl; preview.hidden = false;
    }
    controls();
  }
  function render(value) {
    photo = item(value);
    const results = $('materialfoto-results'); results.replaceChildren();
    if (!photo?.id) { note('Foto nicht verfügbar. Bitte neu auswählen.'); return; }
    note(photo.frage);
    const fields = photo.merkmale || {};
    const found = [fields.produkt, fields.marke, fields.breite, fields.farbe].filter(Boolean).join(' · ');
    if (found) results.append(node('p', 'Auf dem Etikett erkannt (ungeprüft): ' + found));
    if (photo.status !== 'pruefen') {
      results.append(button(photo.status === 'analyse' ? 'Auswertung prüfen' : 'Etikett auslesen', async run => {
        note('Etikett wird ausgelesen …');
        const response = await host.api('/materialfotos/' + encodeURIComponent(photo.id) + '/analyse', {});
        if (current(run)) render(response);
      }));
      return;
    }
    const hits = Array.isArray(photo.treffer) ? photo.treffer : [];
    if (photo.artikelsuche_verfuegbar === false) {
      results.append(button('Artikelsuche erneut prüfen', async run => {
        const result = await host.api('/materialfotos/' + encodeURIComponent(photo.id));
        if (current(run)) render(result);
      }));
      return;
    }
    if (!hits.length) results.append(node('p', 'Nenne dem Assistenten den Artikelnamen und die Breite auf dem Etikett. Es wurde nichts zugeordnet.'));
    for (const hit of hits) {
      const card = node('article'); card.className = 'materialfoto-card';
      card.append(node('h3', hit.produkt_name || 'Artikel prüfen'));
      card.append(node('p', [hit.groesse, hit.farbe, hit.lieferant].filter(Boolean).join(' · ')));
      const pack = hit.packinhalt;
      card.append(node('p', pack ? `Packinhalt laut Beleg: ${pack.menge} ${pack.einheit} je ${pack.pro} (ungeprüft).` : 'Packinhalt nicht belegt.'));
      if (hit.ve) card.append(node('p', 'Einheit laut Beleg: ' + hit.ve));
      const details = node('details'); details.append(node('summary', 'Artikelnummer und Quelle'));
      details.append(node('p', 'Artikelnummer: ' + (hit.artikelnummer || 'nicht hinterlegt')));
      const source = hit.quelle || {};
      details.append(node('p', [source.art === 'lexware' ? 'Lieferantenrechnung' : 'Einkaufsbeleg',
        source.beleg_id ? 'Beleg ' + source.beleg_id : '', source.seite ? 'Seite ' + source.seite : '',
        source.position ? 'Position ' + source.position : '', source.datum || ''].filter(Boolean).join(' · ')));
      card.append(details);
      const photoId = photo.id;
      card.append(button('Diesen Artikel meine ich', async run => {
        note('Artikelauswahl wird geprüft …');
        const result = await host.api('/materialfotos/' + encodeURIComponent(photoId) + '/auswahl', {treffer_id: hit.id});
        if (!current(run)) return;
        if (!result?.auswahl) throw new Error('Artikelauswahl wurde nicht gespeichert. Bitte erneut prüfen.');
        host.selectMaterial(result.auswahl);
        host.status?.(result.frage || 'Artikel gewählt. Noch keine Bestellung bestätigt.');
        close();
      }));
      results.append(card);
    }
    if (photo.treffer_gekuerzt) results.append(node('p', 'Dies ist ein Ausschnitt der Treffer. Bitte Artikel und Breite bei Bedarf genauer nennen.'));
  }
  async function upload(run) {
    if (!file || !requestId) return;
    note('Foto wird privat gespeichert …');
    if (!stagedId) {
      const data = new FormData(); data.append('file', file); data.append('request_id', requestId);
      const response = await host.api('/materialfotos', data);
      if (!current(run)) return;
      const staged = item(response);
      if (!staged?.id) throw new Error('Foto konnte nicht gespeichert werden.');
      stagedId = staged.id; host.clearMaterialSelection?.(); render(staged);
    }
    note('Etikett wird ausgelesen …');
    const response = await host.api('/materialfotos/' + encodeURIComponent(stagedId) + '/analyse', {});
    if (current(run)) render(response);
  }
  async function recent(run) {
    try {
      const result = await host.api('/materialfotos');
      if (!current(run)) return;
      const items = Array.isArray(result) ? result : result?.fotos || [];
      const list = $('materialfoto-recent'); list.replaceChildren();
      for (const entry of items.slice(0, 10)) {
        list.append(button('Foto vom ' + (entry.erstellt_am || 'letzten Upload') + ' prüfen', async active => {
          const result = await host.api('/materialfotos/' + encodeURIComponent(entry.id));
          if (current(active)) render(result);
        }));
      }
      $('materialfoto-history').hidden = !items.length;
    } catch (error) {
      if (current(run)) $('materialfoto-recent').textContent = 'Vorherige Fotos konnten nicht geladen werden.';
    }
  }
  function open() {
    generation++; note('Fotografiere nur das Produktetikett. Keine Personen, Rechnungen oder Fahrzeugpapiere.');
    if (!dialog.open) dialog.showModal();
    controls(); void recent(generation);
  }
  function cleanup() {
    generation++; discardPreview(); file = null; requestId = null; stagedId = null; photo = null;
    input.value = ''; $('materialfoto-results').replaceChildren(); controls();
  }
  function close() { cleanup(); if (dialog.open) dialog.close(); $('materialfoto-open')?.focus(); }
  $('materialfoto-open')?.addEventListener('click', open);
  $('materialfoto-close').addEventListener('click', close);
  dialog.addEventListener('cancel', cleanup);
  dialog.addEventListener('close', cleanup);
  input.addEventListener('change', changed);
  $('materialfoto-form').addEventListener('submit', event => { event.preventDefault(); void work(upload); });
  controls();
  return {open, close};
});
