/* Explicit shop-to-contact draft only. No fetch, submission or customer fields. */
(function(root) {
  'use strict';
  const KEY = 'gaertner-shop-offer-request-v1';
  function draft(value, now = Date.now()) {
    if (!value || typeof value !== 'object' || value.version !== 1 || !Number.isSafeInteger(value.created_at)
        || value.created_at > now || now-value.created_at > 30*60*1000
        || typeof value.message !== 'string' || !value.message || value.message.length > 2000
        || typeof value.full_text !== 'string' || value.full_text.length > 32000
        || !Number.isInteger(value.position_count) || value.position_count < 1 || value.position_count > 40
        || !Number.isInteger(value.omitted_count) || value.omitted_count < 0 || value.omitted_count > value.position_count
        || /[\x00-\x08\x0b-\x1f\x7f]/.test(value.message+value.full_text)) return null;
    return {message:value.message,full_text:value.full_text,position_count:value.position_count,omitted_count:value.omitted_count};
  }
  function mount(win) {
    const query = new win.URLSearchParams(win.location.search);
    if (win.location.pathname !== '/anfrage' || query.get('anliegen') !== 'allgemein' || query.get('shop_anfrage') !== '1') return false;
    const field = win.document.getElementById('nachricht'), form = field?.closest('form');
    if (!field || !form || String(form.method).toLowerCase() !== 'post') return false;
    let value;
    try { const raw=win.sessionStorage.getItem(KEY); if (!raw || raw.length > 40000) return false; value=draft(JSON.parse(raw)); } catch (_) { return false; }
    if (!value) return false;
    const note=win.document.createElement('p'); note.className='hint'; note.setAttribute('role','status');
    if (!field.value.trim()) { field.value=value.message; note.textContent='Shopauswahl in die Nachricht übernommen. Bitte prüfen und bei Bedarf ändern. Noch nicht übermittelt; erst Ihr Klick auf „Anfrage unverbindlich senden“ sendet die Anfrage.'; }
    else note.textContent='Ihre vorhandene Nachricht bleibt erhalten. Bitte die Shopauswahl aus der vollständigen Liste bei Bedarf ergänzen. Noch nicht übermittelt.';
    field.parentElement.append(note);
    if (value.omitted_count || field.value !== value.message) {
      const warning=win.document.createElement('p'); warning.className='hint'; warning.textContent=value.omitted_count+' von '+value.position_count+' Positionen konnten nicht vollständig in das Nachrichtenfeld übernommen werden. Bitte die vollständige Auswahl nachreichen. Die folgende Liste wird nicht automatisch mitgesendet.';
      const details=win.document.createElement('details'), summary=win.document.createElement('summary'), list=win.document.createElement('textarea'); summary.textContent='Vollständige Shopauswahl zur Prüfung';list.value=value.full_text;list.readOnly=true;list.rows=12;list.setAttribute('aria-label','Vollständige Shopauswahl, wird nicht automatisch mitgesendet');details.append(summary,list);field.parentElement.append(warning,details);
    }
    return true;
  }
  if (typeof module !== 'undefined' && module.exports) module.exports={draft,mount};
  if (root.document) mount(root);
})(typeof window !== 'undefined' ? window : globalThis);
