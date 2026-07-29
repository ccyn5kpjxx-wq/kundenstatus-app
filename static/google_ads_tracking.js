(() => {
  if (window.__gaertnerAdsTrackingLoaded) return;
  window.__gaertnerAdsTrackingLoaded = true;

  const currentScript = document.currentScript;
  const privacyUrl = currentScript?.dataset.privacyUrl || '/datenschutz';
  const googleTagId = 'AW-18349808556';
  const conversionLabels = {
    form: 'AW-18349808556/25Q1COiHtdYcEKy3761E',
    whatsapp: 'AW-18349808556/4cKKCN-mndccEKy3761E',
    phone: 'AW-18349808556/r58gCOKmndccEKy3761E'
  };
  const storageKey = 'gaertner_google_ads_consent_v1';
  const pendingEvents = [];
  const lastSent = new Map();
  let consent = null;
  let banner;

  try {
    const storedConsent = localStorage.getItem(storageKey);
    if (storedConsent === 'granted' || storedConsent === 'denied') consent = storedConsent;
  } catch (error) {}

  const gtag = (...args) => {
    window.dataLayer = window.dataLayer || [];
    window.dataLayer.push(args);
  };

  const setGoogleConsent = (granted) => {
    gtag('consent', 'update', {
      ad_storage: granted ? 'granted' : 'denied',
      analytics_storage: 'denied',
      ad_user_data: 'denied',
      ad_personalization: 'denied'
    });
  };

  const sendConversion = (type) => {
    const sendTo = conversionLabels[type];
    if (!sendTo || consent !== 'granted') return false;
    const now = Date.now();
    if (now - (lastSent.get(type) || 0) < 1500) return false;
    lastSent.set(type, now);
    gtag('event', 'conversion', { send_to: sendTo, value: 1.0, currency: 'EUR' });
    return true;
  };

  const loadGoogleTag = () => {
    if (window.__gaertnerGoogleTagLoaded) return;
    window.__gaertnerGoogleTagLoaded = true;
    gtag('consent', 'default', {
      ad_storage: 'denied',
      analytics_storage: 'denied',
      ad_user_data: 'denied',
      ad_personalization: 'denied'
    });
    setGoogleConsent(true);
    gtag('set', 'linker', {
      domains: ['auto-lackierzentrum.de', 'kundenstatus-app.onrender.com'],
      accept_incoming: true
    });
    gtag('js', new Date());
    gtag('config', googleTagId);
    const tag = document.createElement('script');
    tag.async = true;
    tag.src = `https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(googleTagId)}`;
    document.head.appendChild(tag);
    pendingEvents.splice(0).forEach(sendConversion);
  };

  const saveConsent = (value) => {
    consent = value;
    try { localStorage.setItem(storageKey, value); } catch (error) {}
    if (value === 'granted') loadGoogleTag();
    else if (window.dataLayer) setGoogleConsent(false);
    if (banner) banner.hidden = true;
  };

  const ensureConsentUi = () => {
    if (banner) return;
    const style = document.createElement('style');
    style.textContent = `
      #gaertner-ads-consent{position:fixed;z-index:2147483000;left:16px;right:16px;bottom:16px;max-width:720px;margin:auto;padding:20px;border:1px solid rgba(20,28,36,.18);border-radius:18px;background:#fff;color:#17202a;box-shadow:0 18px 60px rgba(18,24,31,.24);font:15px/1.5 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
      #gaertner-ads-consent[hidden]{display:none}
      #gaertner-ads-consent strong{display:block;margin-bottom:6px;font-size:18px}
      #gaertner-ads-consent p{margin:0;color:#4e5964}
      #gaertner-ads-consent a{color:inherit;text-decoration:underline}
      #gaertner-ads-consent .gaertner-ads-actions{display:flex;flex-wrap:wrap;gap:9px;margin-top:16px}
      #gaertner-ads-consent button{min-height:42px;padding:0 16px;border:1px solid #cfd5db;border-radius:999px;background:#fff;color:#17202a;font:inherit;font-weight:750;cursor:pointer}
      #gaertner-ads-consent button[data-consent="granted"]{border-color:#17202a;background:#17202a;color:#fff}
      #gaertner-ads-settings{position:fixed;z-index:2147482999;left:12px;bottom:12px;min-height:34px;padding:0 12px;border:1px solid rgba(20,28,36,.18);border-radius:999px;background:#fff;color:#27313a;box-shadow:0 6px 22px rgba(18,24,31,.13);font:700 12px/1 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;cursor:pointer}
      @media(max-width:560px){#gaertner-ads-consent{left:9px;right:9px;bottom:9px;padding:17px}#gaertner-ads-consent .gaertner-ads-actions button{flex:1}}
    `;
    document.head.appendChild(style);

    banner = document.createElement('section');
    banner.id = 'gaertner-ads-consent';
    banner.setAttribute('role', 'dialog');
    banner.setAttribute('aria-labelledby', 'gaertner-ads-consent-title');
    banner.innerHTML = `
      <strong id="gaertner-ads-consent-title">Optionale Google-Ads-Messung</strong>
      <p>Dürfen wir messen, ob unsere Google-Anzeigen zu einer Anfrage führen? Die Website funktioniert auch ohne Zustimmung. <a href="${privacyUrl}">Mehr zum Datenschutz</a></p>
      <div class="gaertner-ads-actions">
        <button type="button" data-consent="denied">Nein danke</button>
        <button type="button" data-consent="granted">Zustimmen</button>
      </div>
    `;
    banner.addEventListener('click', (event) => {
      const button = event.target.closest('button[data-consent]');
      if (button) saveConsent(button.dataset.consent);
    });
    document.body.appendChild(banner);

    const settings = document.createElement('button');
    settings.id = 'gaertner-ads-settings';
    settings.type = 'button';
    settings.textContent = 'Datenschutz-Einstellungen';
    settings.addEventListener('click', () => {
      banner.hidden = false;
      banner.querySelector('button[data-consent="granted"]')?.focus();
    });
    document.body.appendChild(settings);
    banner.hidden = consent !== null;
  };

  window.gaertnerAdsTrack = (type) => {
    if (consent === 'granted') return sendConversion(type);
    if (consent === null && type === 'form' && !pendingEvents.includes(type)) pendingEvents.push(type);
    return false;
  };
  window.gaertnerAdsOpenSettings = () => {
    ensureConsentUi();
    banner.hidden = false;
  };

  document.addEventListener('click', (event) => {
    const link = event.target.closest?.('a[href]');
    if (!link) return;
    const href = String(link.getAttribute('href') || '').toLowerCase();
    if (href.startsWith('tel:')) window.gaertnerAdsTrack('phone');
    else if (href.includes('wa.me/') || href.includes('api.whatsapp.com/')) window.gaertnerAdsTrack('whatsapp');
  }, true);

  const init = () => {
    ensureConsentUi();
    if (consent === 'granted') loadGoogleTag();
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, { once: true });
  else init();
})();
