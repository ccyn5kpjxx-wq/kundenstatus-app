/* Optional basic-consent measurement. No Google request before consent. */
(() => {
  'use strict';
  if (window.__gaertnerAdsTrackingLoaded) return;
  window.__gaertnerAdsTrackingLoaded = true;
  const currentScript = document.currentScript;
  const googleTagId = 'AW-18349808556';
  const storageKey = 'gaertner_google_ads_consent_v1';
  const scopeKey = 'gaertner_google_ads_scope_v2';
  const sentKey = 'gaertner_google_ads_sent_v1';
  const checkoutKey = 'gaertner_google_ads_checkouts_v1';
  const maxAge = 30 * 86400000;
  const allowedHosts = new Set(['auto-lackierzentrum.de', 'www.auto-lackierzentrum.de', 'kundenstatus-app.onrender.com',
    'tomorrowworks-agentur.de', 'www.tomorrowworks-agentur.de', 'autovermietung-mos.de', 'www.autovermietung-mos.de']);
  const privacySignal = navigator.doNotTrack === '1' || window.doNotTrack === '1' || navigator.globalPrivacyControl === true;
  const enabled = location.protocol === 'https:' && allowedHosts.has(location.hostname) && !privacySignal;
  const label = value => typeof value === 'string' && /^AW-18349808556\/[A-Za-z0-9_-]{10,100}$/.test(value) ? value : '';
  const conversionLabels = Object.freeze({
    form: 'AW-18349808556/25Q1COiHtdYcEKy3761E',
    whatsapp: 'AW-18349808556/4cKKCN-mndccEKy3761E',
    phone: 'AW-18349808556/r58gCOKmndccEKy3761E',
    purchase: label(currentScript?.dataset.purchaseLabel),
    shop_lead: label(currentScript?.dataset.shopLeadLabel)
  });
  const lastSent = new Map(), inFlight = new Set(), pendingEvents = [];
  let consent = null, shopScope = false, banner, returnFocus, tagReady = false, tagConfigured = false;
  try {
    const stored = localStorage.getItem(storageKey);
    if (stored === 'granted' || stored === 'denied') consent = stored;
    shopScope = localStorage.getItem(scopeKey) === 'shop_v2';
    // The former banner covered enquiries; purchase values need the updated choice.
    if (consent === 'granted' && conversionLabels.purchase && !shopScope) consent = null;
  } catch (_) { /* Preference remains in memory when storage is blocked. */ }
  const readMap = key => {
    if (consent !== 'granted') return {};
    try {
      const raw = JSON.parse(localStorage.getItem(key) || '{}'), now = Date.now();
      if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
      return Object.fromEntries(Object.entries(raw).filter(([id, time]) =>
        /^[a-z0-9_:.-]{1,180}$/.test(id) && Number.isFinite(time) && time <= now && now - time < maxAge).slice(-128));
    } catch (_) { return {}; }
  };
  const writeMap = (key, map) => {
    if (consent !== 'granted') return;
    try { localStorage.setItem(key, JSON.stringify(map)); } catch (_) { /* Google transaction IDs still deduplicate. */ }
  };
  function gtag() {
    window.dataLayer = window.dataLayer || [];
    // gtag.js processes commands pushed as Arguments objects.
    window.dataLayer.push(arguments);
  }
  const safePage = () => {
    // Allow only the ad click identifiers needed for attribution after consent.
    const path = /^\/shop\/orders\//.test(location.pathname) ? '/shop/bestellstatus/' : location.pathname;
    const safe = new URL(location.origin + path), source = new URLSearchParams(location.search);
    for (const key of ['gclid', 'dclid', 'gclsrc', 'wbraid', 'gbraid']) {
      const value = source.get(key);
      if (value && value.length <= 512 && /^[A-Za-z0-9_.~-]+$/.test(value)) safe.searchParams.set(key, value);
    }
    return safe.href;
  };
  const safeReferrer = () => {
    try { const ref = new URL(document.referrer); return /^https?:$/.test(ref.protocol) ? ref.origin + '/' : ''; }
    catch (_) { return ''; }
  };
  const setGoogleConsent = granted => {
    gtag('consent', 'update', {
      ad_storage: granted ? 'granted' : 'denied', analytics_storage: 'denied',
      ad_user_data: 'denied', ad_personalization: 'denied'
    });
  };
  const activateTag = () => {
    if (!enabled || consent !== 'granted' || !tagReady) return;
    setGoogleConsent(true);
    if (!tagConfigured) {
      tagConfigured = true;
      gtag('set', 'ads_data_redaction', true);
      gtag('set', 'allow_ad_personalization_signals', false);
      gtag('set', 'linker', {
        domains: ['auto-lackierzentrum.de', 'kundenstatus-app.onrender.com'], accept_incoming: true
      });
      gtag('js', new Date());
      gtag('config', googleTagId, {page_location: safePage(), page_referrer: safeReferrer(),
        send_page_view: false, allow_ad_personalization_signals: false});
    }
    for (const event of pendingEvents.splice(0)) gtag('event', 'conversion', event);
  };
  const loadGoogleTag = () => {
    if (!enabled || consent !== 'granted') return false;
    if (window.__gaertnerGoogleTagLoaded) return true;
    window.__gaertnerGoogleTagLoaded = true;
    gtag('consent', 'default', {
      ad_storage: 'denied', analytics_storage: 'denied', ad_user_data: 'denied', ad_personalization: 'denied'
    });
    const tag = document.createElement('script');
    tag.async = true; tag.referrerPolicy = 'no-referrer';
    tag.src = `https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(googleTagId)}`;
    tag.addEventListener('load', () => { tagReady = true; activateTag(); }, {once: true});
    tag.addEventListener('error', () => {
      window.__gaertnerGoogleTagLoaded = false; tagReady = false;
      inFlight.clear(); pendingEvents.length = 0; tag.remove();
    }, {once: true});
    document.head.appendChild(tag);
    return true;
  };
  function saveConsent(value, {persist = true, grantedScope = true} = {}) {
    if (!['granted', 'denied'].includes(value)) return;
    consent = value;
    shopScope = value === 'granted' && grantedScope;
    if (persist) try {
      localStorage.setItem(storageKey, value);
      if (shopScope) localStorage.setItem(scopeKey, 'shop_v2'); else localStorage.removeItem(scopeKey);
    } catch (_) {}
    if (value === 'granted') { if (window.__gaertnerGoogleTagLoaded) activateTag(); else loadGoogleTag(); }
    else {
      inFlight.clear(); pendingEvents.length = 0;
      // A later grant must not measure orders started before this withdrawal.
      try { localStorage.removeItem(checkoutKey); } catch (_) {}
      if (tagReady) setGoogleConsent(false);
    }
    if (banner) banner.hidden = true;
    if (returnFocus?.isConnected) returnFocus.focus();
    window.dispatchEvent(new CustomEvent('gaertner-ads-consent', {detail: {granted: value === 'granted' && enabled}}));
  }
  function ensureConsentUi() {
    if (banner) return;
    if (!document.querySelector('link[data-gaertner-ads-style], link[href="/static/google_ads_tracking.css"]')) {
      const css = document.createElement('link');
      css.rel = 'stylesheet'; css.href = '/static/google_ads_tracking.css'; css.dataset.gaertnerAdsStyle = 'true';
      document.head.appendChild(css);
    }
    banner = document.createElement('section');
    banner.id = 'gaertner-ads-consent'; banner.setAttribute('role', 'dialog');
    banner.setAttribute('aria-labelledby', 'gaertner-ads-consent-title');
    const title = document.createElement('strong'); title.id = 'gaertner-ads-consent-title'; title.textContent = 'Optionale Google-Ads-Messung';
    const copy = document.createElement('p');
    copy.textContent = 'Dürfen wir messen, ob unsere Google-Anzeigen zu Anfragen und Shopkäufen führen? Die Website funktioniert auch ohne Zustimmung. ';
    const privacy = document.createElement('a');
    const configuredPrivacy = currentScript?.dataset.privacyUrl || '/datenschutz';
    privacy.href = /^\/(?!\/)[^\\\s]*$/.test(configuredPrivacy) ? configuredPrivacy : '/datenschutz';
    privacy.textContent = 'Mehr zum Datenschutz'; copy.append(privacy);
    const actions = document.createElement('div'); actions.className = 'gaertner-ads-actions';
    for (const [value, text] of [['denied', 'Nein danke'], ['granted', 'Zustimmen']]) {
      const button = document.createElement('button'); button.type = 'button'; button.dataset.consent = value;
      button.textContent = text; button.addEventListener('click', () => saveConsent(value)); actions.append(button);
    }
    banner.append(title, copy, actions); document.body.append(banner); banner.hidden = consent !== null;
    const settings = document.createElement('button'); settings.id = 'gaertner-ads-settings';
    settings.type = 'button'; settings.textContent = 'Datenschutz-Einstellungen';
    settings.addEventListener('click', () => { returnFocus = settings; banner.hidden = false; banner.querySelector('button').focus(); });
    document.body.append(settings);
    banner.addEventListener('keydown', event => { if (event.key === 'Escape') { event.preventDefault(); saveConsent('denied'); } });
  }
  window.gaertnerAdsConsentGranted = () => enabled && consent === 'granted';
  window.gaertnerAdsRememberCheckout = orderId => {
    if (!enabled || consent !== 'granted' || !shopScope || !/^sho_[a-f0-9]{32}$/.test(orderId)) return false;
    const map = readMap(checkoutKey); map[orderId] = Date.now(); writeMap(checkoutKey, map); return true;
  };
  window.gaertnerAdsCheckoutAllowed = orderId => enabled && consent === 'granted' && shopScope
    && /^sho_[a-f0-9]{32}$/.test(orderId) && Object.hasOwn(readMap(checkoutKey), orderId);
  window.gaertnerAdsTrack = (type, payload = {}) => {
    const sendTo = Object.hasOwn(conversionLabels, type) ? conversionLabels[type] : '';
    if (!enabled || consent !== 'granted' || !sendTo) return false;
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return false;
    const transactionId = payload.transaction_id;
    if (transactionId !== undefined && type !== 'purchase'
        && (!['form', 'shop_lead'].includes(type) || !/^lead_ads_[a-f0-9]{64}$/.test(transactionId || ''))) return false;
    if (type === 'purchase' && (!shopScope || !/^shop_ads_[a-f0-9]{64}$/.test(transactionId || '')
        || typeof payload.value !== 'number' || !Number.isFinite(payload.value) || payload.value <= 0
        || payload.value > 99999999.99 || Math.abs(payload.value * 100 - Math.round(payload.value * 100)) > 0.000001
        || payload.currency !== 'EUR')) return false;
    if (type === 'shop_lead' && !/^lead_ads_[a-f0-9]{64}$/.test(transactionId || '')) return false;
    const dedup = transactionId ? type + ':' + transactionId : '';
    if (dedup && (inFlight.has(dedup) || Object.hasOwn(readMap(sentKey), dedup))) return false;
    const now = Date.now();
    if (!dedup && now - (lastSent.get(type) || 0) < 1500) return false;
    if (!loadGoogleTag()) return false;
    lastSent.set(type, now);
    const event = {send_to: sendTo, page_location: safePage(), page_referrer: safeReferrer()};
    if (dedup) {
      inFlight.add(dedup); event.transaction_id = transactionId;
      event.event_callback = () => {
        if (consent !== 'granted') return;
        const map = readMap(sentKey); map[dedup] = Date.now(); writeMap(sentKey, map); inFlight.delete(dedup);
      };
    }
    if (type === 'purchase') { event.value = payload.value; event.currency = 'EUR'; }
    if (tagReady) gtag('event', 'conversion', event); else pendingEvents.push(event);
    // True means queued locally, never proof of delivery to Google.
    return true;
  };
  window.gaertnerAdsOpenSettings = () => { ensureConsentUi(); returnFocus = document.activeElement; banner.hidden = false; banner.querySelector('button').focus(); };
  window.addEventListener('storage', event => {
    if (event.key !== null && ![storageKey, scopeKey].includes(event.key)) return;
    let stored = null, scope = false;
    try { stored = localStorage.getItem(storageKey); scope = localStorage.getItem(scopeKey) === 'shop_v2'; } catch (_) {}
    if (stored === 'granted' && conversionLabels.purchase && !scope) {
      // An old tab's enquiry-only choice cannot grant the new purchase scope.
      consent = null; shopScope = false; inFlight.clear(); pendingEvents.length = 0;
      try { localStorage.removeItem(checkoutKey); } catch (_) {}
      if (tagReady) setGoogleConsent(false);
      if (banner) banner.hidden = false;
      return;
    }
    const next = stored === 'granted' ? 'granted' : 'denied';
    if (next !== consent || scope !== shopScope) saveConsent(next, {persist: false, grantedScope: scope});
  });
  document.addEventListener('click', event => {
    const link = event.target.closest?.('a[href]'); if (!link) return;
    const href = String(link.getAttribute('href') || '').toLowerCase();
    if (href.startsWith('tel:')) window.gaertnerAdsTrack('phone');
    else if (href.includes('wa.me/') || href.includes('api.whatsapp.com/')) window.gaertnerAdsTrack('whatsapp');
  }, true);
  const init = () => { ensureConsentUi(); if (consent === 'granted') loadGoogleTag(); };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, {once: true}); else init();
})();
