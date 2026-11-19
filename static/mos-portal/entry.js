(() => {
  const select = document.getElementById('vehicle');
  const group = document.getElementById('entry-vehicles');
  const fallback = document.getElementById('entry-vehicle-fallback');
  if (!select || !group || !fallback) return;

  const choices = Array.from(group.querySelectorAll('button[data-model]'));
  if (!choices.length) return;

  function reflectModel() {
    for (const choice of choices) {
      choice.setAttribute('aria-pressed', String(choice.dataset.model === select.value));
    }
  }

  for (const choice of choices) {
    choice.addEventListener('click', () => {
      select.value = choice.dataset.model;
      select.dispatchEvent(new Event('change', { bubbles: true }));
    });
  }
  select.addEventListener('change', reflectModel);
  window.addEventListener('pageshow', reflectModel);
  reflectModel();
  fallback.hidden = true;
  group.hidden = false;
  document.getElementById('entry-photo-credit').hidden = false;
})();
