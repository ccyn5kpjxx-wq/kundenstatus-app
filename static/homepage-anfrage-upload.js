(() => {
  const input = document.getElementById('bilder');
  const preview = document.getElementById('photo-preview');
  const form = input && input.closest('form');
  if (!input || !preview || !form) return;
  const maxFiles = Number(input.dataset.maxFiles);
  const maxFileMb = Number(input.dataset.maxFileMb);
  const maxTotalMb = Number(input.dataset.maxTotalMb);
  let objectUrls = [];

  const clearPreview = () => {
    objectUrls.forEach((url) => URL.revokeObjectURL(url));
    objectUrls = [];
    preview.replaceChildren();
    preview.dataset.invalid = '';
  };

  const validateSelection = () => {
    const files = Array.from(input.files || []);
    const problems = [];
    if (files.length > maxFiles) problems.push(`Bitte höchstens ${maxFiles} Bilder auswählen.`);
    if (files.some((file) => file.size > maxFileMb * 1024 * 1024)) problems.push(`Ein einzelnes Bild darf höchstens ${maxFileMb} MB groß sein.`);
    if (files.reduce((sum, file) => sum + file.size, 0) > maxTotalMb * 1024 * 1024) problems.push(`Alle Bilder zusammen dürfen höchstens ${maxTotalMb} MB groß sein.`);
    return { files, problems };
  };

  const renderSelection = () => {
    clearPreview();
    const { files, problems } = validateSelection();
    if (!files.length) { preview.classList.remove('is-visible'); return; }

    files.slice(0, maxFiles).forEach((file) => {
      const item = document.createElement('div');
      item.className = 'photo-preview-item';
      if (file.type && file.type !== 'image/heic') {
        const image = document.createElement('img');
        const objectUrl = URL.createObjectURL(file);
        objectUrls.push(objectUrl);
        image.src = objectUrl;
        image.alt = '';
        item.appendChild(image);
      } else {
        const placeholder = document.createElement('div');
        placeholder.className = 'photo-preview-placeholder';
        placeholder.textContent = 'HEIC';
        item.appendChild(placeholder);
      }
      const name = document.createElement('span');
      name.className = 'photo-preview-name';
      name.textContent = file.name;
      item.appendChild(name);
      preview.appendChild(item);
    });

    const status = document.createElement('p');
    status.className = `photo-preview-status${problems.length ? ' is-error' : ''}`;
    status.textContent = problems.length
      ? `${problems.join(' ')} Entfernen Sie die Bilder, um ohne Bilder zu senden.`
      : `${files.length} Bild${files.length === 1 ? '' : 'er'} ausgewählt.`;
    preview.appendChild(status);
    const remove = document.createElement('button');
    remove.type = 'button';
    remove.className = 'photo-remove';
    remove.textContent = 'Bilder entfernen';
    remove.addEventListener('click', () => {
      input.value = '';
      renderSelection();
      input.focus();
    });
    preview.appendChild(remove);
    preview.dataset.invalid = problems.length ? '1' : '';
    preview.classList.add('is-visible');
  };

  input.addEventListener('change', renderSelection);
  form.addEventListener('reset', () => {
    clearPreview();
    preview.classList.remove('is-visible');
  });
  form.addEventListener('submit', (event) => {
    if (validateSelection().problems.length) {
      event.preventDefault();
      renderSelection();
      input.focus();
    }
  });
})();
