(() => {
  'use strict';
  const setup = document.getElementById('employee-setup');
  if (setup) {
    const form = document.getElementById('setup-form');
    const tokenField = document.getElementById('setup-token');
    const status = document.getElementById('setup-status');
    const password = document.getElementById('setup-password');
    const confirmation = document.getElementById('setup-password-confirm');
    const submit = document.getElementById('setup-submit');
    const hash = new URLSearchParams(location.hash.slice(1));
    const token = hash.get('token') || tokenField.value;
    if (location.hash) history.replaceState(null, '', location.pathname);
    const showError = message => { status.textContent = message; status.classList.add('is-error'); };
    if (!token || !/^[A-Za-z0-9_-]{40,150}$/.test(token)) {
      form.hidden = true;
      showError('Bitte öffne deinen vollständigen persönlichen Einrichtungslink. Falls er abgelaufen ist, hilft dir die Werkstattleitung.');
    } else {
      tokenField.value = token;
      const csrf = form.querySelector('input[name="csrf_token"]');
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 15000);
      fetch(setup.dataset.checkUrl, {method:'POST', credentials:'same-origin', cache:'no-store', signal:controller.signal,
        headers:{'Content-Type':'application/json','X-CSRF-Token':csrf.value}, body:JSON.stringify({token})})
        .then(async response => {
          const result = await response.json();
          if (!response.ok || result.valid !== true) throw new Error(result.error || 'Dein Einrichtungslink ist ungültig oder abgelaufen. Bitte fordere einen neuen an.');
          if (!result.employee || typeof result.employee.name !== 'string' || !Number.isInteger(result.employee.id)) throw new Error('Der Zugang konnte nicht geprüft werden. Bitte öffne deinen Einrichtungslink erneut.');
          document.getElementById('setup-title').textContent = 'Hallo ' + result.employee.name;
          document.getElementById('setup-identity').textContent = 'Mitarbeiter-ID ' + result.employee.id + ' · nur dein eigenes Konto';
          form.hidden = false;
          if (!status.classList.contains('is-error')) status.textContent = 'Wähle ein Passwort mit mindestens 12 Zeichen, zum Beispiel mehrere Wörter.';
        })
        .catch(error => {
          form.hidden = true;
          showError(error.name === 'AbortError' ? 'Die Verbindung dauert zu lange. Öffne deinen ursprünglichen Einrichtungslink bitte erneut.' : error.message || 'Verbindung fehlgeschlagen. Öffne deinen ursprünglichen Einrichtungslink bitte erneut.');
        })
        .finally(() => clearTimeout(timer));
    }
    const checkMatch = () => confirmation.setCustomValidity(confirmation.value && password.value !== confirmation.value ? 'Die beiden Passwörter stimmen noch nicht überein.' : '');
    password.addEventListener('input', checkMatch);
    confirmation.addEventListener('input', checkMatch);
    form.addEventListener('submit', event => {
      checkMatch();
      if (!form.reportValidity()) { event.preventDefault(); return; }
      submit.disabled = true;
      submit.textContent = 'Dein Zugang wird eingerichtet …';
    });
    window.addEventListener('pageshow', () => { submit.disabled = false; submit.textContent = 'Zugang einrichten und Profil öffnen →'; });
  }
  const loginLink = document.getElementById('employee-login-link');
  const loginAnchor = document.getElementById('employee-login-anchor');
  if (loginLink && loginAnchor) loginLink.value = loginAnchor.href;
  const copyStatus = document.getElementById('copy-status');
  document.querySelectorAll('[data-copy-link]').forEach(button => button.addEventListener('click', async () => {
    const input = document.getElementById(button.dataset.copyLink);
    if (!input) return;
    try {
      await navigator.clipboard.writeText(input.value);
      if (copyStatus) copyStatus.textContent = button.dataset.copyMessage || 'Link kopiert. Bitte nur an diese Person weitergeben.';
    } catch (_) {
      input.focus(); input.select();
      if (copyStatus) copyStatus.textContent = input === loginLink ? 'Bitte den markierten Anmeldelink kopieren. Die jeweilige Mitarbeiter-ID mitgeben.' : 'Bitte den markierten Link kopieren und nur an diese Person weitergeben.';
    }
  }));
})();
