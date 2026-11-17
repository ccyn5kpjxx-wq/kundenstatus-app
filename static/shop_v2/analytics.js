/* Existing Werkstattcockpit counters only. Never inspect customer/shop data. */
(() => {
  'use strict';
  if (window.__gaertnerShopCounters || navigator.webdriver || navigator.doNotTrack === '1'
      || window.doNotTrack === '1' || navigator.globalPrivacyControl === true) return;
  const pages = Object.freeze({home:'/shop',felgen:'/shop/felgen',reifen:'/shop/reifen',
    komplettraeder:'/shop/komplettraeder',ersatzteile:'/shop/ersatzteile',zubehoer:'/shop/zubehoer',
    service:'/shop/service',warenkorb:'/shop/warenkorb',felgenstudio:'/shop/felgenstudio'});
  const page = pages[document.body?.dataset.page];
  // In particular, never send return/status URLs containing an order reference.
  if (!page || window.location.pathname.replace(/\/$/, '') !== page) return;
  window.__gaertnerShopCounters = true;
  const website = 'auto-lackierzentrum', endpoint = '/api/besucher';
  let remaining = 200;
  function send(payload) {
    if (remaining <= 0 || typeof window.fetch !== 'function') return;
    remaining--;
    try {
      Promise.resolve(window.fetch(endpoint, {method:'POST',credentials:'omit',mode:'same-origin',
        headers:{'Content-Type':'text/plain;charset=UTF-8'},body:JSON.stringify(payload),
        referrerPolicy:'no-referrer',keepalive:true})).catch(() => {});
    } catch (_) { /* Analytics must never interfere with shopping. */ }
  }
  function pageView() {
    if (window.__gaertnerBesucherErfasst) return;
    window.__gaertnerBesucherErfasst = true;
    let referrer = '';
    try {
      const parsed = new URL(document.referrer);
      if (['https:','http:'].includes(parsed.protocol) && /^[a-z0-9.-]{1,200}$/i.test(parsed.hostname)) referrer = parsed.hostname;
    } catch (_) {}
    send({website,seite:page,referrer});
  }
  if (document.readyState === 'complete') pageView();
  else window.addEventListener('load', pageView, {once:true});

  const paths = Object.freeze({'/shop':'nav-shop','/shop/felgen':'nav-felgen','/shop/reifen':'nav-reifen',
    '/shop/komplettraeder':'nav-komplettraeder','/shop/ersatzteile':'nav-ersatzteile','/shop/zubehoer':'nav-zubehoer',
    '/shop/service':'nav-service','/shop/warenkorb':'nav-cart','/shop/felgenstudio':'nav-studio',
    '/':'nav-homepage','/homepage':'nav-homepage'});
  const clickRules = Object.freeze([
    ['#request-quote','quote-request'],['#checkout-button','checkout-start'],
    ['#previous-page, #ws-previous','pagination-previous'],['#next-page, #ws-next','pagination-next'],
    ['.shop-mounting-action button','appointment-open'],['.shop-mounting-dialog a','appointment-form'],
    ['.shop-cart-controls button','cart-remove'],['.shop-family-controls button','variant-view'],
    ['.shop-card-media, .shop-card .shop-detail-link','product-view'],
    ['.shop-card-controls button, .shop-detail-copy > button.shop-button','cart-add'],
    ['.shop-family-variant button','variant-view'],['.shop-bundle-panel > button:first-of-type','matching-tyres'],
    ['.shop-bundle-panel > button:last-of-type','cart-add'],['.shop-fitment a','document-open'],
    ['.shop-thumbnails button','image-view'],['.shop-detail-picture, #ws-canvas, #ws-enlarge','image-zoom'],
    ['#vehicle-filters button[type="reset"]','filter-reset'],['.modern-brand','nav-logo']
  ]);
  function target(event) {
    return event.isTrusted && event.target instanceof Element ? event.target : null;
  }
  function count(code) { send({website,seite:page,shop_click:code}); }
  document.addEventListener('click', event => {
    const node = target(event); if (!node || node.closest('button:disabled')) return;
    for (const [selector, code] of clickRules) if (node.closest(selector)) { count(code); return; }
    const link = node.closest('a[href]'); if (!link) return;
    try {
      const href = link.getAttribute('href');
      if (!href || href.startsWith('#')) return;
      const destination = new URL(href, window.location.origin);
      if (destination.origin !== window.location.origin) return;
      if (destination.pathname === '/anfrage') { count('appointment-form'); return; }
      const path = destination.pathname.replace(/\/$/, '') || '/';
      if (paths[path]) count(paths[path]);
    } catch (_) {}
  }, true);
  document.addEventListener('submit', event => {
    const node = target(event); if (!node) return;
    const form = node.closest('form'); if (!form) return;
    if (form.id === 'catalog-filters') count('catalogue-filter');
    else if (form.id === 'vehicle-filters') count('vehicle-filter');
    else if (form.closest('.shop-mounting-dialog')) count('appointment-check');
    // In particular, never inspect or count contact/address form fields.
  }, true);
  document.addEventListener('change', event => {
    const node = target(event); if (!node) return;
    if (node.closest('.shop-card-controls select, .shop-cart-controls select, .shop-options select:not([name])')) count('quantity-change');
    else if (node.closest('.shop-bundle-option input[name="bundle_tyre"]')) count('tyre-choice');
    else if (node.closest('.shop-bundle-panel select')) count('tyre-season');
    else if (node.closest('.shop-options input[type="checkbox"], .shop-options select[name="care"], .shop-bundle-panel input[name="bundle_mounting"]')) count('service-choice');
    else if (node.closest('#ws-size, #ws-variant')) count('variant-view');
  }, true);
})();
