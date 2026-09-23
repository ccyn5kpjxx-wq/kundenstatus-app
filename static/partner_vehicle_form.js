/* Keep an unfinished vehicle form in this tab, scoped to the signed-in session. */
window.initPartnerVehicleForm = ({ form, showStep, updateTransport, updateUpload, stopLoading, getStep, isAnalyzing }) => {
  const status = form.querySelector('[data-draft-status]');
  const discard = form.querySelector('[data-discard-draft]');
  const file = form.querySelector('[name="dateien"]');
  const id = form.elements.namedItem('_draft_id');
  const prefix = 'partner-vehicle-draft:';
  const key = prefix + form.dataset.draftScope + ':' + form.elements.namedItem('csrf_token').value;
  const fields = Array.from(form.elements).filter(el => el.name && ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName)
    && !['hidden', 'file', 'submit', 'button'].includes(el.type));
  const completed = JSON.parse(form.dataset.completedDrafts || '[]');
  const maxAge = 2 * 60 * 60 * 1000;
  // getRandomValues also works on a local HTTP/LAN portal (randomUUID does not).
  const newDraftId = () => Array.from(crypto.getRandomValues(new Uint32Array(4))).join('-');
  let submitting = false;
  let stored = null;
  let storageAvailable = true;
  const values = () => Object.fromEntries(fields.map(el => [el.name, el.value]));
  const emptyValues = Object.fromEntries(fields.map(el => [el.name, el.tagName === 'SELECT' ? el.options[0]?.value || '' : '']));
  const hasInput = () => fields.some(el => el.value !== emptyValues[el.name]) || file.files.length > 0;
  try {
    // Drop expired drafts and drafts from other login sessions in this tab.
    for (const oldKey of Object.keys(sessionStorage).filter(k => k.startsWith(prefix))) {
      let record;
      try { record = JSON.parse(sessionStorage.getItem(oldKey)); } catch (_) { /* corrupt record */ }
      if (oldKey !== key || !record || Date.now() - record.savedAt > maxAge || completed.includes(record.id)) {
        sessionStorage.removeItem(oldKey);
      }
    }
    stored = JSON.parse(sessionStorage.getItem(key) || 'null');
  } catch (_) { storageAvailable = false; }
  if (form.dataset.postback !== '1' && stored) {
    fields.forEach(el => { if (typeof stored.values?.[el.name] === 'string') el.value = stored.values[el.name]; });
    id.value = stored.id;
    fields.filter(el => el.value).forEach(el => el.closest('details')?.setAttribute('open', ''));
    form.elements.namedItem('fertig_datum').value = form.elements.namedItem('abholtermin').value;
    updateTransport();
    showStep(stored.step || 1, false);
    status.textContent = 'Ihre bisherigen Angaben wurden wiederhergestellt. Noch kein Auftrag gesendet.'
      + (stored.hadFiles ? ' Bitte Dateien erneut auswählen und analysieren.' : '');
    if (stored.hadFiles) {
      form.querySelector('[data-document-options]').open = true;
      form.querySelector('[data-analysis-file-required]').value = '1';
      updateUpload();
    }
  }
  if (!id.value) id.value = newDraftId();
  const saveDraft = (announce = true) => {
    discard.hidden = !hasInput();
    if (!storageAvailable) {
      status.textContent = 'Zwischenspeichern ist in diesem Browser nicht möglich. Bitte die Seite bis zum Senden geöffnet lassen.';
      return;
    }
    try {
      if (hasInput()) {
        sessionStorage.setItem(key, JSON.stringify({ id: id.value, values: values(), step: getStep(),
          hadFiles: file.files.length > 0 || (stored?.hadFiles && !file.value), savedAt: Date.now() }));
        if (announce) status.textContent = 'Angaben in diesem Tab zwischengespeichert (2 Stunden). Noch kein Auftrag gesendet. Dateien bitte bei erneutem Öffnen wieder auswählen.';
      } else sessionStorage.removeItem(key);
    } catch (_) {
      storageAvailable = false;
      status.textContent = 'Zwischenspeichern ist nicht möglich. Bitte die Seite bis zum Senden geöffnet lassen.';
    }
  };
  form.addEventListener('input', () => saveDraft());
  form.addEventListener('change', () => saveDraft());
  form.addEventListener('click', () => saveDraft(false));
  form.addEventListener('keydown', event => {
    if (event.key !== 'Enter' || event.isComposing || event.target.tagName !== 'INPUT'
      || ['file', 'submit', 'button'].includes(event.target.type)) return;
    event.preventDefault();
    if (isAnalyzing()) return;
    if (getStep() === 1) form.querySelector('[data-step-next]').click();
    else form.requestSubmit(form.querySelector('[data-save-vehicle]'));
  });
  form.querySelector('[data-remove-files]').addEventListener('click', () => {
    file.value = '';
    stored = null;
    updateUpload(true);
    saveDraft();
  });
  discard.addEventListener('click', () => {
    if (!window.confirm('Alle bisherigen Angaben und die Dateiauswahl für dieses neue Fahrzeug verwerfen?')) return;
    fields.forEach(el => { el.value = emptyValues[el.name]; });
    file.value = '';
    stored = null;
    form.querySelectorAll('[name^="analyse_"][type="hidden"]').forEach(el => { el.value = ''; });
    form.elements.namedItem('fertig_datum').value = '';
    form.querySelector('[data-analysis-result]').hidden = true;
    try { sessionStorage.removeItem(key); } catch (_) { /* unavailable storage */ }
    id.value = newDraftId();
    updateUpload(); updateTransport(); showStep(1);
    status.textContent = 'Eingaben verworfen. Sie können ein neues Fahrzeug eintragen.';
    discard.hidden = true;
  });
  form.addEventListener('submit', event => {
    if (event.defaultPrevented) return;
    if (submitting || isAnalyzing()) { event.preventDefault(); return; }
    saveDraft(false);
    submitting = true;
  });
  window.addEventListener('beforeunload', event => {
    if (!submitting && hasInput()) { event.preventDefault(); event.returnValue = ''; }
  });
  window.addEventListener('pagehide', () => saveDraft(false));
  window.addEventListener('pageshow', event => {
    stopLoading();
    // A cached submitted form must recheck the server's completion marker.
    if (event.persisted && submitting) { window.location.reload(); return; }
    submitting = false;
  });
  if (form.dataset.postback === '1') {
    status.textContent = 'Ihre Angaben sind erhalten geblieben. Bitte prüfen Sie den Hinweis oben; Dateien gegebenenfalls erneut auswählen.';
    saveDraft(false);
  } else if (stored) saveDraft(false);
  else status.textContent = 'Sie können Angaben und Dateien auch nach dem Anlegen ergänzen oder ändern.';
};
