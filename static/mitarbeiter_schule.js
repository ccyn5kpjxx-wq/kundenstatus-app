(() => {
  'use strict';
  for (const form of document.querySelectorAll('[data-school-form]')) {
    const allDay = form.querySelector('[data-school-all-day]');
    const fields = form.querySelector('[data-school-time-fields]');
    const start = form.querySelector('[data-school-start]');
    const end = form.querySelector('[data-school-end]');
    if (!allDay || !fields || !start || !end) continue;
    const sync = () => {
      fields.hidden = allDay.checked;
      allDay.setAttribute('aria-expanded', String(!allDay.checked));
      for (const input of [start, end]) {
        input.disabled = allDay.checked;
        input.required = !allDay.checked;
      }
    };
    allDay.addEventListener('change', sync);
    window.addEventListener('pageshow', sync);
    sync();
  }
})();
