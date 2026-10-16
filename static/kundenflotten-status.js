(() => {
  'use strict';
  const root = document.querySelector('[data-case-root]');
  if (!root) return;
  const button = document.querySelector('[data-case-refresh]');
  const note = document.querySelector('[data-refresh-note]');
  const target = new URL(root.dataset.statusUrl, window.location.href);
  if (target.origin !== window.location.origin) return;
  let busy = false, stopped = false;
  function unavailable() {
    stopped = true;
    document.querySelector('[data-case-identity]').remove();
    const p = document.createElement('p'); p.className = 'case-empty';
    p.textContent = 'Dieser Statuszugang ist aktuell nicht verfügbar. Bitte stimmen Sie sich mit Ihrer Werkstatt ab.';
    root.replaceChildren(p); button.disabled = true; note.textContent = '';
  }
  async function refresh() {
    if (busy || stopped || document.hidden) return;
    busy = true; button.disabled = true;
    try {
      const response = await fetch(target.href, {credentials:'same-origin', cache:'no-store',headers:{'Accept':'application/json'}});
      if (response.redirected || [401,403,404].includes(response.status)) { unavailable(); return; }
      if (!response.ok) throw new Error('unavailable');
      const data = await response.json();
      if (!Array.isArray(data.cases)) throw new Error('invalid');
      const cards = [...root.querySelectorAll('[data-case-card]')];
      if (cards.length !== data.cases.length || cards.some((card,i) => card.dataset.caseId !== String(data.cases[i].id))) { window.location.reload(); return; }
      cards.forEach((card,i) => {
        const value = data.cases[i];
        for (const [selector,key] of [['title','title'],['state','label'],['description','description'],['time','updated'],['stage','stage']]) {
          card.querySelector('[data-case-'+selector+']').textContent = value[key] || '';
        }
        card.querySelector('[data-case-stage]').hidden = !value.stage;
        card.querySelectorAll('.kf-case-steps li').forEach((step,index) => {
          const state = value.steps[index]; step.classList.toggle('done',!!state.done); step.classList.toggle('current',!!state.current);
          if (state.current) step.setAttribute('aria-current','step'); else step.removeAttribute('aria-current');
        });
      });
      note.textContent = 'Status aus dem Werkstattauftrag abgeglichen. Letztes Auftragsupdate steht direkt am Fall.';
    } catch (_) {
      note.textContent = 'Aktualisierung gerade nicht möglich. Der angezeigte Stand bleibt der zuletzt geladene. Bitte erneut versuchen.';
    } finally { busy = false; button.disabled = stopped; }
  }
  button.addEventListener('click',refresh);
  document.addEventListener('visibilitychange',() => { if (!document.hidden) refresh(); });
  window.setInterval(refresh,30000);
})();
