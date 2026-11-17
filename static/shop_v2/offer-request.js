/* Explicit shop-to-contact draft only. No fetch, submission or customer fields. */
(function(root) {
  'use strict';
  const KEY = 'gaertner-shop-offer-request-v1';
  const FIELDS = ['version', 'created_at', 'message', 'full_text', 'position_count', 'omitted_count'];
  function selection(value) {
    if (!value || typeof value !== 'object' || Array.isArray(value)
        || Object.keys(value).length !== FIELDS.length || !FIELDS.every(key => Object.hasOwn(value, key))
        || value.version !== 1 || !Number.isSafeInteger(value.created_at) || value.created_at < 0
        || typeof value.message !== 'string' || !value.message.trim() || value.message.length > 2000
        || typeof value.full_text !== 'string' || !value.full_text.trim() || value.full_text.length > 32000
        || !Number.isInteger(value.position_count) || value.position_count < 1 || value.position_count > 40
        || !Number.isInteger(value.omitted_count) || value.omitted_count < 0 || value.omitted_count > value.position_count
        || /[\x00-\x08\x0b-\x1f\x7f-\x9f]/.test(value.message+value.full_text)
        || [...value.message+value.full_text].some(char => char.codePointAt(0) >= 0xd800 && char.codePointAt(0) <= 0xdfff)) return null;
    const result = Object.fromEntries(FIELDS.map(key => [key, value[key]]));
    if (new root.TextEncoder().encode(JSON.stringify(result)).length > 32768) return null;
    return result;
  }
  function draft(value, now = Date.now()) {
    const result = selection(value);
    return result && result.created_at <= now && now-result.created_at <= 30*60*1000 ? result : null;
  }
  function mount(win) {
    if (win.location.pathname !== '/anfrage') return false;
    const field = win.document.getElementById('nachricht'), form = field?.closest('form');
    if (!field || !form || String(form.method).toLowerCase() !== 'post'
        || form.dataset.shopOfferForm !== 'true' || form.dataset.shopOfferMounted === 'true'
        || form.elements.namedItem('anliegen')?.value !== 'allgemein') return false;
    const marker = form.elements.namedItem('shop_anfrage'), payload = form.elements.namedItem('shop_auswahl');
    const retained = Boolean(marker || payload);
    let value, raw;
    try {
      if (retained) {
        // A validation-error page owns its submitted context. Never replace it
        // with a newer/stale session draft, even if the retained input is bad.
        if (!marker || marker.value !== '1' || !payload || typeof payload.value !== 'string') return false;
        raw = payload.value;
        if (raw.length > 32768 || new root.TextEncoder().encode(raw).length > 32768) return false;
        value = selection(JSON.parse(raw));
      } else {
        const query = new win.URLSearchParams(win.location.search);
        if (query.get('anliegen') !== 'allgemein' || query.get('shop_anfrage') !== '1') return false;
        raw = win.sessionStorage.getItem(KEY);
        if (!raw || raw.length > 32768) return false;
        value = draft(JSON.parse(raw));
      }
    } catch (_) { return false; }
    if (!value) return false;
    if (!retained) {
      for (const [name, content] of [['shop_anfrage', '1'], ['shop_auswahl', JSON.stringify(value)]]) {
        const input = win.document.createElement('input'); input.type = 'hidden'; input.name = name; input.value = content; form.append(input);
      }
    }
    form.dataset.shopOfferMounted = 'true';
    const note=win.document.createElement('p'); note.className='hint'; note.setAttribute('role','status');
    if (!retained && !field.value.trim()) { field.value=value.message; note.textContent='Shopauswahl in die Nachricht übernommen. Bitte prüfen und bei Bedarf ändern. Noch nicht übermittelt; erst Ihr Klick auf „Anfrage unverbindlich senden“ sendet die Anfrage.'; }
    else note.textContent='Ihre Nachricht und die vollständige Shopauswahl bleiben erhalten. Noch nicht übermittelt; erst Ihr Klick auf „Anfrage unverbindlich senden“ sendet die Anfrage.';
    field.parentElement.append(note);
    const explanation=win.document.createElement('p'); explanation.className='hint'; explanation.textContent='Die vollständige Auswahl wird mit Ihrer Anfrage mitgesendet. Sie ist unverbindlich und wird von uns geprüft; es entsteht keine Bestellung oder Zahlung.';
    if (value.omitted_count) explanation.textContent += ' '+value.omitted_count+' von '+value.position_count+' Positionen stehen nur in der vollständigen Auswahl, nicht in der kurzen Nachricht.';
    const details=win.document.createElement('details'), summary=win.document.createElement('summary'), list=win.document.createElement('textarea'); summary.textContent='Vollständige Shopauswahl zur Prüfung';list.value=value.full_text;list.readOnly=true;list.rows=12;list.setAttribute('aria-label','Vollständige Shopauswahl, wird mit Ihrer Anfrage mitgesendet');details.append(summary,list);field.parentElement.append(explanation,details);
    return true;
  }
  if (typeof module !== 'undefined' && module.exports) module.exports={draft,mount};
  if (root.document) mount(root);
})(typeof window !== 'undefined' ? window : globalThis);
