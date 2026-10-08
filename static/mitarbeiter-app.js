(() => {
  'use strict';

  let pendingInstall = null;
  let promptOpen = false;
  let installed = false;
  let buttons = [];
  let status = [];
  let help = [];
  const displayMode = window.matchMedia('(display-mode: standalone)');
  const agent = navigator.userAgent || '';
  const isIOS = /iPhone|iPad|iPod/i.test(agent) ||
    (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
  const platform = isIOS ? 'ios' : /Android/i.test(agent) ? 'android' : 'browser';
  const inApp = /WhatsApp|FBAN|FBAV|Instagram/i.test(agent);

  function setStatus(message) {
    status.forEach((element) => { element.textContent = message; });
  }

  function isStandalone() {
    return installed || displayMode.matches || navigator.standalone === true;
  }

  function renderButtons() {
    const alreadyInstalled = isStandalone();
    buttons.forEach((button) => {
      const disabled = alreadyInstalled || promptOpen;
      if ('disabled' in button) button.disabled = disabled;
      button.setAttribute('aria-disabled', String(disabled));
      button.textContent = alreadyInstalled ? 'App ist installiert' :
        promptOpen ? 'Installation geöffnet …' :
          pendingInstall ? 'App installieren' : 'App aufs Handy holen';
    });
  }

  function showHelp() {
    help.forEach((element) => { element.open = true; });
    const message = inApp ?
      'Öffne diesen Link zuerst in Safari oder Chrome. Dort kannst du die App zum Home-Bildschirm hinzufügen.' :
      platform === 'ios' ?
        'In Safari auf „Teilen“ tippen, dann „Zum Home-Bildschirm“ wählen und mit „Hinzufügen“ bestätigen.' :
        platform === 'android' ?
          'Öffne das Browsermenü in Chrome und wähle „App installieren“ oder „Zum Startbildschirm hinzufügen“.' :
          'Nutze das Installationssymbol in der Adressleiste oder den Punkt „App installieren“ im Browsermenü. Du kannst das Portal auch direkt im Browser nutzen.';
    setStatus(message);
    if (help[0]) help[0].scrollIntoView({
      behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth',
      block: 'nearest'
    });
  }

  async function install(event) {
    event.preventDefault();
    if (isStandalone() || promptOpen) return;
    if (!pendingInstall) {
      showHelp();
      return;
    }
    const installEvent = pendingInstall;
    pendingInstall = null;
    promptOpen = true;
    renderButtons();
    try {
      await installEvent.prompt();
      const choice = await installEvent.userChoice;
      setStatus(isStandalone() ? 'Die App ist installiert. Öffne sie künftig über das Gärtner-Symbol.' :
        choice.outcome === 'accepted' ?
          'Installation angefordert. Sobald die App fertig ist, findest du sie auf deinem Home-Bildschirm.' :
          'Du kannst die App später installieren. Dein Portal bleibt hier erreichbar.');
    } catch (_) {
      showHelp();
    } finally {
      promptOpen = false;
      renderButtons();
    }
  }

  window.addEventListener('beforeinstallprompt', (event) => {
    event.preventDefault();
    pendingInstall = event;
    renderButtons();
    if (!isStandalone()) setStatus('Bereit zum Installieren – tippe auf „App installieren“.');
  });

  window.addEventListener('appinstalled', () => {
    installed = true;
    pendingInstall = null;
    renderButtons();
    setStatus('Die App ist installiert. Öffne sie künftig über das Gärtner-Symbol.');
  });

  function displayChanged() {
    renderButtons();
    if (isStandalone()) setStatus('Du nutzt das Mitarbeiterportal bereits als App.');
  }

  if (displayMode.addEventListener) displayMode.addEventListener('change', displayChanged);
  else if (displayMode.addListener) displayMode.addListener(displayChanged);

  function init() {
    buttons = Array.from(document.querySelectorAll('[data-app-install]'));
    status = Array.from(document.querySelectorAll('[data-app-install-status]'));
    help = Array.from(document.querySelectorAll('[data-app-help]'));
    document.querySelectorAll('[data-app-platform]').forEach((element) => {
      element.hidden = element.getAttribute('data-app-platform') !== platform;
    });
    document.querySelectorAll('[data-app-in-app]').forEach((element) => { element.hidden = !inApp; });
    buttons.forEach((button) => { button.addEventListener('click', install); });
    renderButtons();
    if (isStandalone()) setStatus('Du nutzt das Mitarbeiterportal bereits als App.');
    else if (pendingInstall) setStatus('Bereit zum Installieren – tippe auf „App installieren“.');
    else if (inApp) setStatus('Zum Installieren den Link in Safari oder Chrome öffnen.');

    if ('serviceWorker' in navigator && window.isSecureContext) {
      navigator.serviceWorker.register('/werkstatt/app-sw.js', {
        scope: '/werkstatt/', updateViaCache: 'none'
      }).catch(() => {
        if (!isStandalone()) setStatus('Die App-Installation ist gerade nicht verfügbar. Du kannst das Portal weiter im Browser nutzen oder es später erneut versuchen.');
      });
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, { once: true });
  else init();
})();
