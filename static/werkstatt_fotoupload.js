/* Original photos travel one at a time; the server's 25-MiB request cap stays. */
(function (root, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root && root.document) api.bind(root.document, root);
})(typeof window !== 'undefined' ? window : null, function () {
  'use strict';

  async function uploadSeries(files, send, maxBytes, progress) {
    const results = [];
    for (let index = 0; index < files.length; index += 1) {
      const file = files[index];
      if (progress) progress(index + 1, files.length);
      let result;
      if (file.size > maxBytes) {
        result = { state: 'rejected', message: 'Dieses einzelne Foto ist zu groß.' };
      } else {
        try { result = await send(file); }
        catch (_) { result = { state: 'unknown', message: 'Die Speicherung konnte nicht bestätigt werden.' }; }
      }
      results.push({ name: file.name, ...result });
      // A request may already have saved the file when its response is lost.
      // Never retry automatically or resubmit the already successful photos.
      if (result.state !== 'saved') break;
    }
    return { results, saved: results.filter(r => r.state === 'saved').length,
      pending: files.length - results.length, complete: results.length === files.length && results.every(r => r.state === 'saved') };
  }

  function uploadProof(response, page, detailURL, orderId) {
    if (!response.ok) return {
      state: response.status >= 400 && response.status < 500 ? 'rejected' : 'unknown',
      message: response.status === 413 ? 'Dieses einzelne Foto ist zu groß.' : 'Der Upload wurde nicht bestätigt. Bitte den Auftrag prüfen.'
    };
    const expected = new URL(detailURL);
    let actual;
    try { actual = new URL(response.url); } catch (_) { actual = null; }
    if (!actual || actual.origin !== expected.origin || actual.pathname !== expected.pathname ||
        !page.body || page.body.dataset.auftragId !== String(orderId)) {
      return { state: 'unknown', message: 'Keine Bestätigung am richtigen Auftrag. Bitte Anmeldung und Auftrag prüfen.' };
    }
    const warning = page.querySelector('.toast.t-warning, .toast.t-danger');
    // An unconsumed success flash from an earlier request must never mask a
    // warning from this request (e.g. after an interrupted redirect).
    if (warning) return { state: 'rejected', message: warning.textContent.trim() };
    const marker = page.querySelector('[data-foto-upload-gespeichert="1"]');
    if (marker) return { state: 'saved' };
    return { state: 'unknown', message: 'Die Speicherung konnte nicht bestätigt werden.' };
  }

  function bind(doc, win) {
    const form = doc.querySelector('[data-foto-upload]');
    if (!form) return;
    const status = doc.querySelector('[data-foto-upload-status]');
    const refresh = doc.querySelector('[data-foto-upload-aktualisieren]');
    const overlay = doc.querySelector('[data-upload-overlay]');
    const progress = doc.querySelector('[data-foto-upload-fortschritt]');
    const inputs = Array.from(form.querySelectorAll('input[type="file"]'));
    const labels = Array.from(form.querySelectorAll('.foto-upload-knopf'));
    const detailURL = new URL(form.dataset.auftragUrl, win.location.href).href;
    const busy = active => {
      win.werkstattFotoUploadAktiv = active;
      overlay.classList.toggle('show', active);
      inputs.forEach(input => { input.disabled = active; });
      labels.forEach(label => label.classList.toggle('gesperrt', active));
      form.setAttribute('aria-busy', String(active));
      if (!active) doc.dispatchEvent(new win.Event('werkstatt-fotoupload-ende'));
    };

    async function send(file, fields) {
      const data = new win.FormData();
      // Never construct FormData(form): both gallery/camera inputs may contain
      // the entire original selection. Only explicit non-file fields plus one
      // original File belong in this request.
      fields.forEach(([key, value]) => data.append(key, value));
      data.append('fotos', file, file.name);
      const controller = new win.AbortController();
      const timeout = win.setTimeout(() => controller.abort(), 90000);
      try {
        const response = await win.fetch(form.action, { method: 'POST', body: data,
          credentials: 'same-origin', signal: controller.signal });
        if (!response.ok) return uploadProof(response, {}, detailURL, form.dataset.auftragId);
        const page = new win.DOMParser().parseFromString(await response.text(), 'text/html');
        return uploadProof(response, page, detailURL, form.dataset.auftragId);
      } finally { win.clearTimeout(timeout); }
    }

    win.fotoUploadStarten = async input => {
      if (win.werkstattFotoUploadAktiv || !input.files || !input.files.length) return;
      const files = Array.from(input.files);
      const fields = Array.from(form.querySelectorAll('input[type="hidden"][name]')).map(i => [i.name, i.value]);
      refresh.hidden = true;
      status.textContent = '';
      busy(true);
      try {
        const result = await uploadSeries(files, file => send(file, fields), Number(form.dataset.maxDateiBytes), (n, total) => {
          progress.textContent = `Foto ${n} von ${total} wird hochgeladen — bitte warten…`;
          status.textContent = progress.textContent;
        });
        if (result.complete) {
          status.textContent = `${result.saved} Foto(s) gespeichert. Auftrag wird aktualisiert.`;
          busy(false);
          win.location.reload();
          return;
        }
        const failed = result.results[result.results.length - 1];
        status.textContent = `${result.saved} von ${files.length} Foto(s) bestätigt gespeichert. ${failed.name}: ${failed.message} ` +
          (failed.state === 'unknown' ? 'Bitte vor einem erneuten Upload prüfen, ob dieses Foto bereits vorhanden ist. ' : '') +
          `${result.pending} weitere Foto(s) wurden noch nicht gesendet. Bereits bestätigte Fotos bitte nicht erneut auswählen.`;
        refresh.hidden = false;
      } finally {
        inputs.forEach(i => { i.value = ''; });
        busy(false);
      }
    };
    form.addEventListener('submit', event => {
      event.preventDefault();
      const selected = inputs.find(input => input.files && input.files.length);
      if (selected) win.fotoUploadStarten(selected);
    });
    win.addEventListener('beforeunload', event => {
      if (win.werkstattFotoUploadAktiv) { event.preventDefault(); event.returnValue = ''; }
    });
    win.addEventListener('pageshow', event => { if (event.persisted) busy(false); });
  }
  return { uploadSeries, uploadProof, bind };
});
