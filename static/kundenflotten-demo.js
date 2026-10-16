(() => {
  'use strict';
  const dialog = document.getElementById('app-dialog');
  const content = document.getElementById('dialog-content');
  const result = document.getElementById('demo-result');
  const titles = {hu:'HU / AU planen',reifen:'Reifen & Räder',service:'Service planen',schaden:'Schaden melden',panne:'Panne melden',kilometer:'Kilometer aktualisieren',termine:'Ihre Termine',nachrichten:'Ihre Nachrichten',kontakt:'Persönlich für Sie da.',profil:'Ihr Profil'};
  let urls = [];
  let opener;
  function cleanup() { urls.forEach(url => URL.revokeObjectURL(url)); urls = []; }
  function open(name) {
    const template = document.getElementById('tpl-' + name);
    if (!template || !titles[name]) return;
    if (!dialog.open) opener = document.activeElement;
    cleanup();
    document.getElementById('dialog-title').textContent = titles[name];
    content.replaceChildren(template.content.cloneNode(true));
    const now = new Date();
    const today = [now.getFullYear(), String(now.getMonth() + 1).padStart(2, '0'), String(now.getDate()).padStart(2, '0')].join('-');
    content.querySelectorAll('input[type="date"]').forEach(input => { input.min = today; });
    if (!dialog.open) dialog.showModal();
    dialog.scrollTop = 0;
    const first = name === 'panne' ? content.querySelector('.panne-call') : content.querySelector('input,select,textarea,button');
    if (first) first.focus({preventScroll: name === 'panne'});
    if (name === 'panne') dialog.scrollTop = 0;
  }
  document.addEventListener('click', event => {
    const trigger = event.target.closest('[data-open]');
    if (trigger) open(trigger.dataset.open);
  });
  document.getElementById('dialog-close').addEventListener('click', () => dialog.close());
  dialog.addEventListener('close', () => { cleanup(); if (opener) opener.focus(); });
  content.addEventListener('change', event => {
    if (event.target.type !== 'file') return;
    cleanup();
    const files = [...event.target.files];
    const previews = content.querySelector('.photo-previews');
    const error = content.querySelector('.upload-error');
    const invalid = files.length > 5 || files.some(file => !['image/jpeg','image/png','image/webp'].includes(file.type) || file.size > 10 * 1024 * 1024);
    event.target.setCustomValidity(invalid ? 'Bitte bis zu fünf JPEG-, PNG- oder WebP-Bilder mit jeweils höchstens 10 MB auswählen.' : '');
    error.textContent = event.target.validationMessage;
    previews.replaceChildren();
    if (invalid) return;
    files.forEach(file => { const img = document.createElement('img'); const url = URL.createObjectURL(file); urls.push(url); img.src = url; img.alt = 'Lokale Vorschau: ' + file.name; previews.append(img); });
  });
  content.addEventListener('submit', event => {
    const form = event.target.closest('[data-demo-form]');
    if (!form) return;
    event.preventDefault();
    if (!form.reportValidity()) return;
    const heading = document.createElement('strong');
    heading.textContent = form.dataset.demoForm + ' – Vorschau abgeschlossen';
    const text = document.createElement('p');
    text.textContent = 'So würde Ihre Anfrage anschließend bestätigt werden. Es wurde nichts gesendet oder gespeichert; Fotos bleiben nur in dieser Browseransicht.';
    const clear = document.createElement('button');
    clear.type = 'button'; clear.className = 'text-button'; clear.textContent = 'Hinweis schließen';
    clear.addEventListener('click', () => { result.hidden = true; });
    result.replaceChildren(heading, text, clear); result.hidden = false;
    dialog.close(); result.scrollIntoView({behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth', block:'center'});
  });
})();
