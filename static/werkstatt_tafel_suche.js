(() => {
  const input = document.querySelector('[data-tafel-suche]');
  if (!input) return;

  const reset = document.querySelector('[data-tafel-reset]');
  const result = document.querySelector('[data-tafel-treffer]');
  const counters = [...document.querySelectorAll('[data-tafel-anzahl]')];

  function normalize(value) {
    return value.toLocaleLowerCase('de').normalize('NFD')
      .replace(/\p{M}/gu, '').replace(/ß/g, 'ss').replace(/[^a-z0-9]/g, '');
  }

  const columns = [...document.querySelectorAll('[data-tafel-spalte]')].map(column => ({
    counters: counters.filter(counter => counter.dataset.tafelAnzahl === column.dataset.tafelSpalte),
    empty: column.querySelector('[data-tafel-leer]'),
    cards: [...column.querySelectorAll('[data-suchkarte]')].map(card => ({
      card,
      fields: [card.dataset.suchnummer, card.dataset.suchkennzeichen, card.dataset.suchfahrzeug]
        .map(value => normalize(value || '')),
    })),
  }));

  function applySearch() {
    const needle = normalize(input.value.trim());
    let total = 0;
    for (const column of columns) {
      let visible = 0;
      for (const {card, fields} of column.cards) {
        card.hidden = Boolean(needle) && !fields.some(field => field.includes(needle));
        if (!card.hidden) visible++;
      }
      for (const counter of column.counters) counter.textContent = String(visible);
      if (column.empty) column.empty.hidden = !needle || visible > 0;
      total += visible;
    }
    reset.hidden = input.value.length === 0;
    result.textContent = needle ? total + ' Treffer' : '';
  }

  function update() {
    applySearch();
    // Der Filter überlebt die automatische Aktualisierung nur in diesem Verlaufseintrag.
    try {
      history.replaceState({...history.state, werkstattSuche: input.value}, '');
    } catch {
      // Die Suche funktioniert auch, wenn der Browser den Verlauf schreibschützt.
    }
  }

  function clear() {
    input.value = '';
    update();
    input.focus();
  }

  input.value = history.state?.werkstattSuche || '';
  input.addEventListener('input', update);
  input.addEventListener('keydown', event => {
    if (event.key === 'Escape' && input.value) {
      event.preventDefault();
      clear();
    }
  });
  reset.addEventListener('click', clear);
  applySearch();
})();
