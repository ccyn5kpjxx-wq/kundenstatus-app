/* Only offered, offset-qualified slots determine elapsed time and date limits. */
(() => {
  'use strict';
  const form = document.getElementById('booking-dates');
  const slotData = document.getElementById('booking-slots');
  if (!form || !slotData) return;
  const maxDays = Number(form.dataset.maxDays);
  const dayMilliseconds = 24 * 60 * 60 * 1000;
  const startDate = document.getElementById('start-date');
  const startTime = document.getElementById('start-time');
  const endDate = document.getElementById('end-date');
  const endTime = document.getElementById('end-time');
  const message = document.getElementById('date-message');
  const fields = [startDate, startTime, endDate, endTime];
  let adjustment = '';
  if (!Number.isInteger(maxDays) || maxDays < 1) {
    message.textContent = 'Die Buchungsdaten konnten nicht geladen werden. Bitte die Seite neu laden.';
    startDate.setCustomValidity(message.textContent);
    return;
  }
  const slots = JSON.parse(slotData.textContent).map(iso => ({
    iso, day: iso.slice(0, 10), time: iso.slice(11, 16), stamp: Date.parse(iso)
  })).filter(slot => Number.isFinite(slot.stamp)).sort((a, b) => a.stamp - b.stamp);
  const byDate = new Map();
  for (const slot of slots) {
    if (!byDate.has(slot.day)) byDate.set(slot.day, []);
    byDate.get(slot.day).push(slot);
  }

  function selectedSlot(date, time) {
    return (byDate.get(date.value) || []).find(slot => slot.time === time.value);
  }

  function fillTimes(date, select, requested = select.value) {
    const available = byDate.get(date.value) || [];
    select.replaceChildren();
    function option(value, text) {
      const item = document.createElement('option');
      item.value = value;
      item.textContent = text;
      select.append(item);
    }
    if (!requested) option('', available.length ? 'Bitte Uhrzeit wählen' : 'Kein Übergabetermin verfügbar');
    for (const slot of available) option(slot.time, slot.time + ' Uhr');
    // Keep the submitted choice visible when a closure or invalid input made it
    // unavailable. A later correction must be the customer's own selection.
    if (requested && !available.some(slot => slot.time === requested)) {
      option(requested, requested + ' Uhr – nicht verfügbar');
    }
    select.value = requested;
  }

  function allowedReturns(start) {
    if (!start) return [];
    const deadline = start.stamp + maxDays * dayMilliseconds;
    return slots.filter(slot => slot.stamp > start.stamp && slot.stamp <= deadline);
  }

  function validate() {
    for (const field of fields) field.setCustomValidity('');
    const start = selectedSlot(startDate, startTime);
    const end = selectedSlot(endDate, endTime);
    const returns = allowedReturns(start);
    if (slots.length) {
      startDate.min = slots[0].day;
      startDate.max = slots[slots.length - 1].day;
      endDate.min = returns.length ? returns[0].day : start ? start.day : slots[0].day;
      endDate.max = returns.length ? returns[returns.length - 1].day : start ? start.day : slots[slots.length - 1].day;
    }
    let error = '';
    let field = null;
    let days = null;
    if (!startDate.value) {
      error = 'Bitte ein Abholdatum wählen.'; field = startDate;
    } else if (!(byDate.get(startDate.value) || []).length) {
      error = 'An diesem Abholdatum ist kein Übergabetermin geöffnet. Bitte ein anderes Datum wählen.'; field = startDate;
    } else if (!start) {
      error = 'Bitte eine verfügbare Abholzeit wählen.'; field = startTime;
    } else if (!returns.length) {
      error = 'Für diesen Abholtermin ist kein späterer Rückgabetermin verfügbar. Bitte einen früheren Abholtermin wählen.'; field = startTime;
    } else if (!endDate.value) {
      error = 'Bitte ein Rückgabedatum wählen.'; field = endDate;
    } else if (!(byDate.get(endDate.value) || []).length) {
      error = 'An diesem Rückgabedatum ist kein Übergabetermin geöffnet. Bitte ein anderes Datum wählen.'; field = endDate;
    } else if (!end) {
      error = 'Bitte eine verfügbare Rückgabezeit wählen.'; field = endTime;
    } else if (end.stamp <= start.stamp) {
      error = 'Die Rückgabe muss nach der Abholung liegen. Bitte Datum und Uhrzeit prüfen.'; field = endTime;
    } else {
      days = Math.ceil((end.stamp - start.stamp) / dayMilliseconds);
      if (days > maxDays) {
        error = `Du hast ${days} Miettage gewählt. Online sind höchstens ${maxDays} Miettage (je 24 Stunden) möglich. Bitte die Rückgabe anpassen.`;
        field = endDate;
      }
    }
    if (field) field.setCustomValidity(error);
    message.className = error ? 'small error' : 'small';
    message.textContent = error || [adjustment, days ? `Gewählte Mietdauer: ${days} Miettag${days === 1 ? '' : 'e'}; maximal ${maxDays} online möglich.` : ''].filter(Boolean).join(' ');
    return {valid: !error, start, end, returns, days};
  }

  function pickupChanged() {
    fillTimes(startDate, startTime);
    const start = selectedSlot(startDate, startTime);
    const end = selectedSlot(endDate, endTime);
    const returns = allowedReturns(start);
    adjustment = '';
    if (start && returns.length && (!end || end.stamp <= start.stamp || end.stamp > start.stamp + maxDays * dayMilliseconds)) {
      const target = start.stamp + dayMilliseconds;
      const next = returns.find(slot => slot.stamp >= target) || returns[returns.length - 1];
      endDate.value = next.day;
      fillTimes(endDate, endTime, next.time);
      adjustment = `Rückgabe wurde auf ${next.day.split('-').reverse().join('.')} um ${next.time} Uhr angepasst. Bitte prüfen.`;
    }
    validate();
  }

  function returnChanged() {
    adjustment = '';
    fillTimes(endDate, endTime);
    validate();
  }

  function restore() {
    // Initial rendering, server errors and Back navigation preserve all values.
    fillTimes(startDate, startTime);
    fillTimes(endDate, endTime);
    adjustment = '';
    validate();
  }

  startDate.addEventListener('change', pickupChanged);
  startTime.addEventListener('change', pickupChanged);
  endDate.addEventListener('change', returnChanged);
  endTime.addEventListener('change', returnChanged);
  // Handle validation ourselves so an earlier custom error cannot swallow the
  // submit event before the current values have been checked again.
  form.noValidate = true;
  form.addEventListener('submit', event => {
    const result = validate();
    if (!result.valid || !form.checkValidity()) {
      event.preventDefault();
      form.reportValidity();
    }
  });
  window.addEventListener('pageshow', restore);
  restore();
})();
