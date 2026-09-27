(() => {
  'use strict';
  const form = document.getElementById('import-form');
  const start = document.getElementById('start');
  const stop = document.getElementById('stop');
  const progress = document.getElementById('progress');
  let paused = false;
  const render = report => {
    progress.textContent = `${report.quellen.length} Belege erfasst · ${report.offen} offen · ${report.laeuft} in Arbeit · ${report.vorschlaege} Artikelvorschläge gespeichert`;
    const list = document.getElementById('sources');
    list.replaceChildren(...report.quellen.map(source => {
      const row = document.createElement('li');
      row.textContent = `${source.state === 'ausgeschlossen' ? 'Ausgeschlossene Quelle' : source.supplier || 'Lieferant ungeklärt'} · ${source.reference} · ${source.state} ${(source.result.hinweise || []).join(' · ')}`;
      if (['ausgelesen', 'pruefen'].includes(source.state) && Number.isSafeInteger(source.id) && source.id > 0) {
        const retry = document.createElement('form');
        retry.method = 'post';
        retry.action = `/admin/assistent-artikel/quelle/${source.id}/wiederholen`;
        const csrf = document.createElement('input');
        csrf.type = 'hidden'; csrf.name = 'csrf_token'; csrf.value = form.elements.csrf_token.value;
        const button = document.createElement('button');
        button.type = 'submit'; button.textContent = 'Nur diesen Beleg erneut einlesen';
        retry.appendChild(csrf); retry.appendChild(button); row.appendChild(retry);
      }
      return row;
    }));
  };
  const post = async path => {
    const response = await fetch(path, {method:'POST', credentials:'same-origin',
      headers:{'X-CSRF-Token':form.elements.csrf_token.value}});
    if (!response.ok) throw new Error('Verarbeitung unterbrochen. Bitte Seite neu öffnen; gespeicherte Vorschläge bleiben erhalten.');
    return response.json();
  };
  stop.addEventListener('click', () => { paused = true; stop.disabled = true; });
  form.addEventListener('submit', async event => {
    event.preventDefault();
    paused = false; start.disabled = true; stop.disabled = false;
    try {
      let report = await post('/admin/assistent-artikel/start');
      render(report);
      while (!paused && (report.offen > 0 || report.laeuft > 0)) {
        // Another worker may still own a receipt; polling also recovers stale
        // leases after a worker crash instead of leaving Start permanently stuck.
        if (report.laeuft > 0) await new Promise(resolve => setTimeout(resolve, 2000));
        if (paused) break;
        report = await post('/admin/assistent-artikel/weiter'); render(report);
      }
      if (paused) progress.textContent += ' · Pausiert. Erneutes Starten setzt fort.';
      if (!report.offen && !report.laeuft) {
        progress.textContent += report.quellen.length ? ' · Durchlauf beendet. Belege mit „prüfen“ brauchen Nacharbeit.' : ' · Keine Lieferantenrechnungen in diesen Quellen vorhanden.';
        const link = document.createElement('a'); link.href = '/admin/assistent-artikel';
        link.textContent = ' Gespeicherte Vorschläge anzeigen'; progress.appendChild(link);
      }
    } catch (error) { progress.textContent = error.message; }
    finally { start.disabled = false; stop.disabled = true; }
  });
})();
