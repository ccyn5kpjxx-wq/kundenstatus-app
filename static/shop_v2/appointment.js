/* Optional mounting preference for one shop purchase. The server rechecks the time. */
(function (root) {
  'use strict';
  const datePattern = /^\d{4}-\d{2}-\d{2}$/;
  const timePattern = /^(?:[01]\d|2[0-3]):[0-5]\d$/;
  function label(value) {
    return value.date.split('-').reverse().join('.') + ' um ' + value.time + ' Uhr';
  }
  function create(panel, storage, loadSlots, context) {
    const enable = panel.querySelector('[name=appointment_requested]');
    const day = panel.querySelector('[name=appointment_date]');
    const hour = panel.querySelector('[name=appointment_time]');
    const fields = panel.querySelector('[data-appointment-fields]');
    const status = panel.querySelector('[role=status]');
    const retry = panel.querySelector('[data-appointment-retry]');
    const review = panel.ownerDocument.getElementById('checkout-appointment-note');
    const key = 'gaertner-shop-montagetermin:' + context;
    let scope = null, current = {requested: false, date: '', time: ''}, generation = 0, loadedFor = '';
    const today = () => {
      const parts = new Intl.DateTimeFormat('sv-SE', {timeZone: 'Europe/Berlin', year: 'numeric', month: '2-digit', day: '2-digit'}).formatToParts(new Date());
      const value = key => parts.find(part => part.type === key).value;
      return [value('year'), value('month'), value('day')].join('-');
    };
    function updateReview() {
      if (!review) return;
      review.hidden = panel.hidden || !current.requested || !current.date || !current.time || loadedFor !== current.date;
      review.textContent = review.hidden ? '' : 'Ihr Montageterminwunsch: ' + label(current) + '. Die Bestätigung der Werkstatt folgt nach Lieferklärung.';
    }
    function persist() {
      updateReview();
      try { storage.setItem(key, JSON.stringify({scope, ...current})); } catch (_) { /* Retain edits in this window. */ }
    }
    function option(value, text) {
      const node = panel.ownerDocument.createElement('option');
      node.value = value; node.textContent = text;
      return node;
    }
    function updateControls() {
      fields.hidden = !current.requested;
      enable.checked = current.requested;
      day.value = current.date; day.min = today();
    }
    async function refresh() {
      const run = ++generation, date = current.date;
      loadedFor = ''; updateReview(); retry.hidden = true; hour.disabled = true;
      hour.replaceChildren(option('', 'Uhrzeit wählen'));
      status.textContent = '';
      if (!scope || !current.requested || !datePattern.test(date) || date < today()) return;
      status.textContent = 'Freie Zeiten werden geladen …';
      try {
        const result = await loadSlots(date);
        if (run !== generation) return;
        if (!Array.isArray(result?.zeiten) || result.zeiten.some(time => !timePattern.test(time))) throw new Error('appointment_service_unavailable');
        const times = [...new Set(result.zeiten)];
        loadedFor = date;
        times.forEach(time => hour.append(option(time, time + ' Uhr')));
        hour.disabled = times.length === 0;
        if (current.time && !times.includes(current.time)) {
          current.time = ''; persist();
          status.textContent = 'Die gewählte Uhrzeit ist inzwischen belegt. Bitte wählen Sie erneut.';
        } else status.textContent = times.length ? 'Wählen Sie Ihre gewünschte Uhrzeit.' : 'Für diesen Tag ist keine Zeit frei. Bitte wählen Sie ein anderes Datum.';
        hour.value = current.time;
        if (current.time) status.textContent = 'Ausgewählt: ' + label(current) + '.';
        updateReview();
      } catch (_) {
        if (run !== generation) return;
        status.textContent = 'Die freien Zeiten sind gerade nicht verfügbar. Sie können erneut prüfen oder den Terminwunsch abwählen.';
        retry.hidden = false;
      }
    }
    function scopeFor(rows) {
      const mounted = rows.filter(row => row.id.startsWith('tyre:') && row.services?.mounting)
        .map(row => [row.id, row.quantity, row.assembly_group || '']).sort();
      return mounted.length ? JSON.stringify(mounted) : '';
    }
    function sync(rows) {
      const next = scopeFor(rows);
      if (scope === next) return;
      const previous = scope; scope = next; panel.hidden = !scope;
      current = {requested: false, date: '', time: ''};
      if (scope) {
        try {
          const record = JSON.parse(storage.getItem(key));
          if (record?.scope === scope && record.requested === true) {
            current = {requested: true, date: datePattern.test(record.date) ? record.date : '', time: timePattern.test(record.time) ? record.time : ''};
          }
        } catch (_) { /* The form also works without browser storage. */ }
      }
      if (previous !== null && !scope) persist();
      updateControls(); refresh();
    }
    enable.addEventListener('change', () => {
      current = {requested: enable.checked, date: '', time: ''}; persist(); updateControls(); refresh();
      if (current.requested) day.focus();
    });
    day.addEventListener('change', () => { current.date = day.value; current.time = ''; persist(); refresh(); });
    hour.addEventListener('change', () => { current.time = hour.value; persist(); status.textContent = current.time ? 'Terminwunsch ' + label(current) + '. Wir bestätigen ihn nach Klärung der Lieferung.' : 'Bitte wählen Sie eine Uhrzeit.'; });
    retry.addEventListener('click', refresh);
    return {
      sync,
      show(available) { panel.hidden = !scope || !available; updateReview(); },
      copyFor(rows, token) {
        if (current.requested && scope && scopeFor(rows) === scope) {
          try { storage.setItem('gaertner-shop-montagetermin:' + token, JSON.stringify({scope, ...current})); } catch (_) {}
        }
      },
      selected() {
        if (!scope || !current.requested) return null;
        if (!current.date || current.date < today() || loadedFor !== current.date || !timePattern.test(current.time) || ![...hour.options].some(o => o.value === current.time)) {
          status.textContent = 'Bitte wählen Sie einen freien Tag und eine Uhrzeit oder bestellen Sie ohne Terminwunsch.';
          panel.scrollIntoView({block: 'center'}); (current.date && !hour.disabled ? hour : day).focus();
          throw new Error('invalid_appointment_preference');
        }
        return {date: current.date, time: current.time};
      },
      refresh,
      focus() { if (scope && !panel.hidden) { panel.scrollIntoView({block: 'center'}); enable.focus(); } },
      lock(locked) { panel.disabled = locked; },
    };
  }
  root.ShopAppointmentPreference = Object.freeze({create, label});
})(typeof window !== 'undefined' ? window : globalThis);
