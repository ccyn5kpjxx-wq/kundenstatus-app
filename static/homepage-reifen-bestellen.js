(() => {
  'use strict';
  const date = document.querySelector('[data-tire-date]');
  const time = document.querySelector('[data-tire-time]');
  const status = document.querySelector('[data-tire-time-status]');
  if (!date || !time || !status) return;
  let generation = 0;
  date.addEventListener('change', async () => {
    const current = ++generation;
    time.replaceChildren(new Option('Freie Zeiten werden geprüft …', ''));
    time.disabled = true;
    try {
      const url = new URL('/api/reifenservice/termine', window.location.origin);
      url.searchParams.set('datum', date.value);
      const response = await fetch(url, {cache:'no-store', credentials:'same-origin'});
      if (!response.ok) throw new Error('Keine Terminantwort');
      const payload = await response.json();
      if (current !== generation) return;
      const slots = Array.isArray(payload.zeiten) ? payload.zeiten.filter(t => typeof t === 'string' && /^\d{2}:00$/.test(t)) : [];
      time.replaceChildren(new Option(slots.length ? 'Uhrzeit auswählen' : 'Keine freien Zeiten', ''));
      slots.forEach(slot => time.add(new Option(`${slot} Uhr`, slot)));
      status.textContent = 'Terminvorschau. Lieferdatum und freier Platz werden beim Bestellabschluss erneut geprüft.';
    } catch (_) {
      if (current !== generation) return;
      time.replaceChildren(new Option('Zeiten konnten nicht geladen werden', ''));
      status.textContent = 'Bitte wählen Sie das Datum erneut.';
    } finally { if (current === generation) time.disabled = false; }
  });
})();
