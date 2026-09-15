/* Preserve the existing form nodes and their drafts while navigating the order. */
(() => {
  'use strict';
  const nav = document.querySelector('[data-partner-tabs]');
  const tabs = Array.from(nav.querySelectorAll('[data-section]'));
  const panels = Array.from(document.querySelectorAll('[data-partner-section]'));
  nav.setAttribute('role', 'tablist');
  tabs.forEach(tab => {
    tab.id = 'partner-tab-' + tab.dataset.section;
    tab.setAttribute('role', 'tab');
    tab.setAttribute('aria-controls', 'bereich-' + tab.dataset.section);
  });
  panels.forEach(panel => {
    panel.setAttribute('role', 'tabpanel');
    panel.setAttribute('aria-labelledby', 'partner-tab-' + panel.dataset.partnerSection);
  });
  function activate(section) {
    if (!panels.some(panel => panel.dataset.partnerSection === section)) return;
    panels.forEach(panel => { panel.hidden = panel.dataset.partnerSection !== section; });
    tabs.forEach(tab => {
      const selected = tab.dataset.section === section;
      tab.setAttribute('aria-selected', String(selected));
      tab.tabIndex = selected ? 0 : -1;
    });
  }
  function reveal(target, scroll = false) {
    if (!target) return;
    const panel = target.closest('[data-partner-section]');
    if (panel) activate(panel.dataset.partnerSection);
    let ancestor = target;
    while (ancestor) {
      if (ancestor.tagName === 'DETAILS') ancestor.open = true;
      ancestor = ancestor.parentElement;
    }
    if (scroll) window.requestAnimationFrame(() => target.scrollIntoView({ block: 'start', behavior: 'auto' }));
  }
  function hashTarget() {
    try { return document.getElementById(decodeURIComponent(window.location.hash.slice(1))); }
    catch (_) { return null; }
  }
  function followHash(scroll = true) {
    const target = hashTarget();
    if (target) reveal(target, scroll);
  }
  function updateHash(id) {
    if (window.location.hash !== '#' + id) history.pushState(null, '', '#' + id);
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', event => {
      event.preventDefault();
      activate(tab.dataset.section);
      updateHash('bereich-' + tab.dataset.section);
    });
    tab.addEventListener('keydown', event => {
      let next;
      if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
      else if (event.key === 'ArrowLeft') next = (index - 1 + tabs.length) % tabs.length;
      else if (event.key === 'Home') next = 0;
      else if (event.key === 'End') next = tabs.length - 1;
      else return;
      event.preventDefault();
      tabs[next].focus();
      tabs[next].click();
    });
  });
  document.querySelectorAll('[data-open-details]').forEach(button => {
    button.addEventListener('click', () => {
      const target = document.getElementById(button.dataset.openDetails);
      if (!target) return;
      updateHash(target.id);
      reveal(target, true);
      target.querySelector('summary')?.focus({ preventScroll: true });
    });
  });
  document.addEventListener('click', event => {
    const link = event.target.closest('a[href^="#"]');
    if (!link || link.closest('[data-partner-tabs]')) return;
    const id = link.getAttribute('href').slice(1);
    const target = document.getElementById(id);
    if (!target) return;
    event.preventDefault();
    updateHash(id);
    reveal(target, true);
    if (target.tagName === 'DETAILS') target.querySelector('summary')?.focus({ preventScroll: true });
    else if (target.hasAttribute('tabindex')) target.focus({ preventScroll: true });
    else target.querySelector('input, textarea, select, button, a')?.focus({ preventScroll: true });
  });
  document.addEventListener('invalid', event => reveal(event.target, true), true);
  window.addEventListener('hashchange', () => followHash());
  window.addEventListener('popstate', () => {
    activate('uebersicht');
    followHash();
  });
  activate('uebersicht');
  followHash();

  let dirty = false;
  const markDirty = event => { if (event.target.closest('form')) dirty = true; };
  document.addEventListener('input', markDirty);
  document.addEventListener('change', markDirty);
  const refresh = document.querySelector('[data-portal-refresh]');
  const warning = document.querySelector('[data-refresh-warning]');
  refresh.addEventListener('click', event => {
    if (!dirty) return;
    event.preventDefault();
    warning.hidden = false;
    warning.scrollIntoView({ block: 'nearest' });
  });
  document.querySelector('[data-refresh-discard]').addEventListener('click', () => window.location.assign(refresh.href));

  const config = JSON.parse(document.getElementById('partner-order-config').textContent);
  const transportLabels = config.transportLabels || {};
  const transportSelect = document.querySelector('[data-transport-select]');
  const annahmeLabel = document.querySelector('[data-annahme-label]');
  const abholungLabel = document.querySelector('[data-abholung-label]');
  const neueAbholungLabel = document.querySelector('[data-neue-abholung-label]');
  const updateTransportLabels = () => {
    if (!transportSelect) return;
    const selected = transportLabels[transportSelect.value] || transportLabels.standard || {};
    if (annahmeLabel) annahmeLabel.textContent = selected.partner_annahme_label || 'Fahrzeug wird von Ihnen angeliefert';
    if (abholungLabel) abholungLabel.textContent = selected.partner_abholung_label || 'Fahrzeug wird von Ihnen geholt';
    if (neueAbholungLabel) neueAbholungLabel.textContent = 'Neue ' + (selected.partner_abholung_label || 'Fahrzeug wird von Ihnen geholt');
  };
  transportSelect?.addEventListener('change', updateTransportLabels);
  updateTransportLabels();

  const overlay = document.querySelector('[data-loading-overlay]');
  const loadingText = document.querySelector('[data-loading-text]');
  const newsTitle = document.querySelector('[data-loading-news-title]');
  const newsText = document.querySelector('[data-loading-news-text]');
  const newsItems = config.loadingNews || [];
  let newsIndex = 0;
  let newsTimer;
  function updateNews() {
    if (!newsItems.length || !newsTitle || !newsText) return;
    const item = newsItems[newsIndex++ % newsItems.length];
    newsTitle.textContent = item.title;
    newsText.textContent = item.text;
  }
  document.querySelectorAll('[data-upload-form]').forEach(form => {
    form.addEventListener('submit', event => {
      if (!overlay || !loadingText) return;
      loadingText.textContent = event.submitter?.dataset.loadingMessage || 'Der Auftrag wird gespeichert. Bitte kurz warten.';
      updateNews();
      if (newsTimer) clearInterval(newsTimer);
      if (newsItems.length) newsTimer = setInterval(updateNews, 3200);
      overlay.classList.add('show');
    });
  });
})();
