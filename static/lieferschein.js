document.querySelectorAll('form[data-lieferschein-automatik]').forEach(form => {
  const button = form.querySelector('button');
  const label = button.textContent;
  form.addEventListener('submit', () => {
    button.disabled = true;
    button.textContent = 'Wird analysiert und zugeordnet …';
    form.setAttribute('aria-busy', 'true');
  });
  window.addEventListener('pageshow', () => {
    button.disabled = false;
    button.textContent = label;
    form.removeAttribute('aria-busy');
  });
});
