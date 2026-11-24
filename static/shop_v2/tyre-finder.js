(() => {
  'use strict';
  const form = document.querySelector('.tyre-finder-form');
  if (!form) return;

  const names = ['width', 'ratio', 'diameter'];
  const season = () => form.elements.namedItem('season').value;
  const field = name => form.elements.namedItem(name);

  // Keep typed and linked sizes searchable even if today's catalogue has no
  // record for them. The ordinary number inputs work while the choices load.
  function installChoices(rows) {
    if (!rows.length) return;
    function update(changedIndex = -1) {
      let matching = rows.filter(row => !season() || row[3] === season());
      names.forEach((name, index) => {
        let select = field(name);
        const previous = select.value;
        if (select.tagName !== 'SELECT') {
          const input = select;
          select = document.createElement('select');
          select.name = input.name;
          input.replaceWith(select);
        }
        const values = [...new Set(matching.map(row => String(row[index])))];
        const preserve = previous && (changedIndex < 0 || index <= changedIndex);
        if (preserve && !values.includes(previous)) values.push(previous);
        values.sort((a, b) => Number(a) - Number(b));
        select.replaceChildren(new Option('Alle', ''), ...values.map(value => new Option(value, value)));
        select.value = values.includes(previous) ? previous : '';
        if (select.value) matching = matching.filter(row => String(row[index]) === select.value);
      });
    }
    function activate() {
      if (names.includes(document.activeElement?.name) && form.contains(document.activeElement)) {
        form.addEventListener('focusout', () => setTimeout(activate, 0), {once: true});
        return;
      }
      update();
      form.addEventListener('change', event => {
        const index = names.indexOf(event.target.name);
        if (index >= 0 || event.target.name === 'season') update(index);
      }, true);
    }
    activate();
  }
  fetch(document.body.dataset.api + '/tyre-sizes', {credentials: 'same-origin'})
    .then(response => response.ok ? response.json() : null)
    .then(data => {
      if (Array.isArray(data?.items)) installChoices(data.items);
    }).catch(() => { /* The same form accepts manually entered dimensions. */ });

})();
