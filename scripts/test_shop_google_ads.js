/* Offline DOM/Google-queue doubles only: no resources, vendor calls or payments. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
let JSDOM;
try { ({JSDOM} = require('jsdom')); }
catch (_) { ({JSDOM} = require(path.resolve(__dirname, '../../preview-validation/node_modules/jsdom/lib/api.js'))); }
const shared = fs.readFileSync(path.resolve(__dirname, '../static/google_ads_tracking.js'), 'utf8');
const adapter = fs.readFileSync(path.resolve(__dirname, '../static/shop_v2/conversions.js'), 'utf8');
const orderId = 'sho_' + 'a'.repeat(32), purchaseId = 'shop_ads_' + 'b'.repeat(64), leadId = 'lead_ads_' + 'c'.repeat(64);
const purchaseLabel = 'AW-18349808556/SYNTHETIC_purchase', leadLabel = 'AW-18349808556/SYNTHETIC_lead';
const consentKey = 'gaertner_google_ads_consent_v1', checkoutKey = 'gaertner_google_ads_checkouts_v1', sentKey = 'gaertner_google_ads_sent_v1';
const scopeKey = 'gaertner_google_ads_scope_v2';
let checks = 0;
function check(name, fn) { return Promise.resolve().then(fn).then(() => { checks++; console.log('PASS ' + name); }); }
function browser(options = {}) {
  const dom = new JSDOM('<!doctype html><html><head><meta name="shop-csrf" content="synthetic-csrf-token-1234"></head><body></body></html>', {
    url: options.url || 'https://auto-lackierzentrum.de/shop/reifen/?gclid=Click_123-abc&email=private%40example.invalid&q=private#private',
    referrer: 'https://example.invalid/customer/private?email=private%40example.invalid', runScripts: 'outside-only'
    // No resources option: jsdom will not load external scripts, CSS or pixels.
  });
  const w = dom.window, timers = [], fetches = [];
  w.setTimeout = (fn, delay) => { timers.push({fn, delay}); return timers.length; };
  w.clearTimeout = () => {};
  w.fetch = (...args) => { fetches.push(args); return Promise.reject(new Error('Network forbidden')); };
  if (options.consent) w.localStorage.setItem(consentKey, options.consent);
  if (options.consent === 'granted' && !options.legacyGrant) w.localStorage.setItem(scopeKey, 'shop_v2');
  for (const [key, value] of Object.entries(options.storage || {})) w.localStorage.setItem(key, value);
  if (options.dnt) Object.defineProperty(w.navigator, 'doNotTrack', {value: '1'});
  if (options.gpc) Object.defineProperty(w.navigator, 'globalPrivacyControl', {value: true});
  const script = w.document.createElement('script');
  if (options.labels !== false) { script.dataset.purchaseLabel = purchaseLabel; script.dataset.shopLeadLabel = leadLabel; }
  Object.defineProperty(w.document, 'currentScript', {get: () => script, configurable: true});
  w.eval(shared); w.document.dispatchEvent(new w.Event('DOMContentLoaded'));
  const loadTag = () => w.document.querySelector('script[src*="googletagmanager"]')?.dispatchEvent(new w.Event('load'));
  if (!options.slowTag) loadTag();
  return {dom, w, timers, fetches, events: () => (w.dataLayer || []).filter(row => row[0] === 'event'),
    googleScripts: () => [...w.document.querySelectorAll('script[src]')].filter(node => /googletagmanager/.test(node.src)),
    loadTag, grant: () => { w.document.querySelector('button[data-consent="granted"]').click(); if (!options.slowTag) loadTag(); },
    deny: () => w.document.querySelector('button[data-consent="denied"]').click(),
    close: () => w.close()};
}
function paid() { return {transaction_id: purchaseId, value: 123.45, currency: 'EUR'}; }
function configureOrder(b) {
  Object.assign(b.w.document.body.dataset, {page: 'order_status', orderId,
    measurementUrl: '/shop/api/v2/orders/' + orderId + '/measurement'});
}
function response(value, options = {}) { return {ok: options.ok !== false,
  headers: {get: () => options.mime || 'application/json'}, json: async () => value}; }
async function settle() { await new Promise(resolve => setImmediate(resolve)); await new Promise(resolve => setImmediate(resolve)); }

(async () => {
  await check('unknown consent sends nothing; discarded lead is never replayed on grant', () => {
    const b = browser(); assert.equal(b.googleScripts().length, 0); assert.equal(b.w.dataLayer, undefined);
    assert.equal(b.w.gaertnerAdsTrack('shop_lead', {transaction_id: leadId}), false);
    assert.equal(b.w.gaertnerAdsRememberCheckout(orderId), false); assert.equal(b.w.localStorage.getItem(checkoutKey), null);
    b.grant(); assert.equal(b.googleScripts().length, 1); assert.equal(b.events().length, 0); b.close();
  });
  await check('denial sends no tag or denial ping and later grant does not replay', () => {
    const b = browser({consent: 'denied'}); b.w.gaertnerAdsTrack('form'); b.deny();
    assert.equal(b.googleScripts().length, 0); assert.equal(b.w.dataLayer, undefined);
    b.grant(); assert.equal(b.events().length, 0); b.close();
  });
  await check('legacy enquiry consent is retained but shop purchase scope requires the updated choice', () => {
    const b = browser({consent: 'granted', legacyGrant: true});
    assert.equal(b.w.localStorage.getItem(consentKey), 'granted'); assert.equal(b.googleScripts().length, 0);
    assert.equal(b.w.document.querySelector('#gaertner-ads-consent').hidden, false);
    assert.equal(b.w.gaertnerAdsRememberCheckout(orderId), false); assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), false);
    b.grant(); assert.equal(b.w.localStorage.getItem(scopeKey), 'shop_v2'); assert.equal(b.w.gaertnerAdsRememberCheckout(orderId), true);
    b.deny(); assert.equal(b.w.localStorage.getItem(scopeKey), null); b.close();
    const oldForm = browser({consent: 'granted', legacyGrant: true, labels: false});
    assert.equal(oldForm.w.gaertnerAdsTrack('form'), true); assert.equal(oldForm.w.gaertnerAdsRememberCheckout(orderId), false); oldForm.close();
  });
  await check('granted attribution retains only validated Google click IDs; no PII or fragments', () => {
    const b = browser({consent: 'granted'}), config = b.w.dataLayer.find(row => row[0] === 'config')[2];
    assert.equal(config.page_location, 'https://auto-lackierzentrum.de/shop/reifen/?gclid=Click_123-abc');
    assert.equal(config.page_referrer, 'https://example.invalid/'); assert.equal(config.send_page_view, false);
    assert.equal(config.allow_ad_personalization_signals, false); assert.equal(b.fetches.length, 0);
    assert.equal(b.googleScripts()[0].referrerPolicy, 'no-referrer');
    assert.equal(b.w.document.querySelectorAll('style').length, 0);
    assert.equal(b.w.document.querySelector('link').getAttribute('href'), '/static/google_ads_tracking.css'); b.close();
  });
  await check('all allowed click parameters bounded; attacker-supplied query omitted', () => {
    const b = browser({consent: 'granted', url: 'https://auto-lackierzentrum.de/shop/?gclid=bad%20value&dclid=abc&gclsrc=aw.ds&wbraid=A_b-1&gbraid=long_2&token=secret'});
    const url = b.w.dataLayer.find(row => row[0] === 'config')[2].page_location;
    assert.equal(url, 'https://auto-lackierzentrum.de/shop/?dclid=abc&gclsrc=aw.ds&wbraid=A_b-1&gbraid=long_2'); b.close();
  });
  await check('status page order reference is redacted', () => {
    const b = browser({consent: 'granted', url: 'https://auto-lackierzentrum.de/shop/orders/' + orderId + '/success?email=secret#token'});
    assert.equal(b.w.dataLayer.find(row => row[0] === 'config')[2].page_location, 'https://auto-lackierzentrum.de/shop/bestellstatus/'); b.close();
  });
  await check('missing labels fail closed while existing form tracking stays available', () => {
    const b = browser({consent: 'granted', labels: false});
    assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), false);
    assert.equal(b.w.gaertnerAdsTrack('shop_lead', {transaction_id: leadId}), false);
    assert.equal(b.w.gaertnerAdsTrack('form'), true); assert.equal(b.events().length, 1);
    assert.equal(b.events()[0][2].value, undefined); b.close();
  });
  await check('purchase amount and IDs strictly validated; unknown types/PII rejected', () => {
    const b = browser({consent: 'granted'});
    for (const value of [0, -1, Infinity, NaN, '123.45', 123.456, 100000000]) assert.equal(b.w.gaertnerAdsTrack('purchase', {...paid(), value}), false);
    for (const p of [{...paid(), currency: 'USD'}, {...paid(), transaction_id: 'customer@example.invalid'}, {}]) assert.equal(b.w.gaertnerAdsTrack('purchase', p), false);
    assert.equal(b.w.gaertnerAdsTrack('toString'), false);
    assert.equal(b.w.gaertnerAdsTrack('phone', {transaction_id: 'private@example.invalid'}), false);
    assert.equal(b.events().length, 0); b.close();
  });
  await check('durable dedup only after tag callback; same transaction queued once and survives reload', () => {
    const b = browser({consent: 'granted'});
    assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), true); assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), false);
    assert.equal(b.w.localStorage.getItem(sentKey), null); assert.equal(b.events().length, 1);
    assert.equal(b.events()[0][2].value, 123.45); b.events()[0][2].event_callback();
    const stored = b.w.localStorage.getItem(sentKey); assert.ok(stored.includes(purchaseId)); b.close();
    const again = browser({consent: 'granted', storage: {[sentKey]: stored}});
    assert.equal(again.w.gaertnerAdsTrack('purchase', paid()), false); assert.equal(again.events().length, 0); again.close();
  });
  await check('tag load failure retains no durable success marker and permits retry', () => {
    const b = browser({consent: 'granted', slowTag: true}); b.w.gaertnerAdsTrack('purchase', paid());
    b.googleScripts()[0].dispatchEvent(new b.w.Event('error'));
    assert.equal(b.w.localStorage.getItem(sentKey), null); assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), true); b.close();
  });
  await check('slow tag grant/event/revoke/load cannot replay withdrawn conversion or config', () => {
    const b = browser({consent: 'granted', slowTag: true});
    assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), true); assert.equal(b.events().length, 0);
    assert.equal(b.w.dataLayer.some(row => row[0] === 'config'), false);
    b.deny(); b.loadTag(); assert.equal(b.events().length, 0);
    assert.equal(b.w.dataLayer.some(row => row[0] === 'config'), false);
    b.grant(); assert.equal(b.events().length, 0); b.close();
  });
  await check('slow tag flushes each still-consented event once after actual load', () => {
    const b = browser({consent: 'granted', slowTag: true});
    assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), true); assert.equal(b.events().length, 0);
    b.loadTag(); assert.equal(b.events().length, 1); b.loadTag(); assert.equal(b.events().length, 1); b.close();
  });
  await check('checkout consent required beforehand; revocation clears markers and blocks late callback', () => {
    const b = browser({consent: 'granted'}); assert.equal(b.w.gaertnerAdsRememberCheckout(orderId), true);
    assert.equal(b.w.gaertnerAdsCheckoutAllowed(orderId), true); b.w.gaertnerAdsTrack('purchase', paid());
    const callback = b.events()[0][2].event_callback; b.deny(); callback();
    assert.equal(b.w.localStorage.getItem(checkoutKey), null); assert.equal(b.w.localStorage.getItem(sentKey), null);
    assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), false); b.grant();
    assert.equal(b.w.gaertnerAdsCheckoutAllowed(orderId), false); b.close();
  });
  await check('cross-tab revocation blocks current tab and drops checkout permission', () => {
    const b = browser({consent: 'granted'}); b.w.gaertnerAdsRememberCheckout(orderId);
    b.w.localStorage.setItem(consentKey, 'denied'); b.w.localStorage.removeItem(scopeKey);
    b.w.dispatchEvent(new b.w.StorageEvent('storage', {key: consentKey, newValue: 'denied'}));
    assert.equal(b.w.gaertnerAdsCheckoutAllowed(orderId), false); assert.equal(b.w.gaertnerAdsTrack('form'), false); b.close();
  });
  await check('cleared cross-tab storage withdraws stale in-memory consent without recreating preferences', () => {
    const b = browser({consent: 'granted'}); b.w.gaertnerAdsRememberCheckout(orderId); b.w.localStorage.clear();
    b.w.dispatchEvent(new b.w.StorageEvent('storage', {key: null}));
    assert.equal(b.w.gaertnerAdsTrack('form'), false); assert.equal(b.w.gaertnerAdsCheckoutAllowed(orderId), false);
    assert.equal(b.w.localStorage.length, 0); b.close();
  });
  await check('legacy other-tab grant cannot mint the purchase scope; explicit new scope can sync', () => {
    const b = browser({consent: 'denied'});
    b.w.localStorage.setItem(consentKey, 'granted');
    b.w.dispatchEvent(new b.w.StorageEvent('storage', {key: consentKey, newValue: 'granted'}));
    assert.equal(b.w.localStorage.getItem(scopeKey), null); assert.equal(b.googleScripts().length, 0);
    assert.equal(b.w.gaertnerAdsRememberCheckout(orderId), false); assert.equal(b.w.gaertnerAdsTrack('purchase', paid()), false);
    assert.equal(b.w.document.querySelector('#gaertner-ads-consent').hidden, false);
    b.w.localStorage.setItem(scopeKey, 'shop_v2');
    b.w.dispatchEvent(new b.w.StorageEvent('storage', {key: scopeKey, newValue: 'shop_v2'}));
    b.loadTag(); assert.equal(b.w.gaertnerAdsRememberCheckout(orderId), true); b.close();
  });
  await check('DNT/GPC and non-production hosts never load Google', () => {
    for (const options of [{dnt: true}, {gpc: true}, {url: 'http://127.0.0.1:8898/shop/'}, {url: 'https://untrusted.invalid/shop/'}]) {
      const b = browser({...options, consent: 'granted'}); assert.equal(b.googleScripts().length, 0);
      assert.equal(b.w.gaertnerAdsTrack('form'), false); assert.equal(b.w.gaertnerAdsRememberCheckout(orderId), false); b.close();
    }
  });
  await check('existing rental and agency host tracking remains available', () => {
    for (const host of ['autovermietung-mos.de', 'www.autovermietung-mos.de', 'tomorrowworks-agentur.de', 'www.tomorrowworks-agentur.de']) {
      const b = browser({consent: 'granted', url: 'https://' + host + '/'});
      assert.equal(b.googleScripts().length, 1); assert.equal(b.w.gaertnerAdsTrack('form'), true); b.close();
    }
  });
  await check('server-rendered success receipt only counts an opaque shop lead once', () => {
    const b = browser({consent: 'granted'}), receipt = b.w.document.createElement('span');
    receipt.dataset.shopLeadId = leadId; b.w.document.body.append(receipt); b.w.eval(adapter); b.w.eval(adapter);
    assert.equal(b.events().length, 1); assert.equal(b.events()[0][2].send_to, leadLabel);
    assert.equal(b.events()[0][2].transaction_id, leadId); assert.equal(b.events()[0][2].value, undefined); b.close();
  });
  await check('no success receipt or denied receipt never counts a lead later', () => {
    const b = browser({consent: 'denied'}), receipt = b.w.document.createElement('span');
    receipt.dataset.shopLeadId = leadId; b.w.document.body.append(receipt); b.w.eval(adapter); b.grant();
    b.w.eval(adapter); assert.equal(b.events().length, 0); b.close();
    const noReceipt = browser({consent: 'granted'}); noReceipt.w.eval(adapter); assert.equal(noReceipt.events().length, 0); noReceipt.close();
  });
  await check('ordinary server success routes only to existing form label; unknown kind rejected', () => {
    for (const kind of ['form', 'unknown']) {
      const b = browser({consent: 'granted'}), receipt = b.w.document.createElement('span');
      receipt.dataset.shopLeadId = leadId; receipt.dataset.adsEventKind = kind; b.w.document.body.append(receipt); b.w.eval(adapter);
      assert.equal(b.events().length, kind === 'form' ? 1 : 0);
      if (kind === 'form') assert.equal(b.events()[0][2].send_to, 'AW-18349808556/25Q1COiHtdYcEKy3761E'); b.close();
    }
  });
  await check('verified paid read-only endpoint emits once with CSRF; no provider or checkout action', async () => {
    const b = browser({consent: 'granted'}); configureOrder(b); b.w.gaertnerAdsRememberCheckout(orderId);
    b.w.fetch = async (...args) => { b.fetches.push(args); return response({purchase: paid()}); };
    b.w.eval(adapter); await settle(); assert.equal(b.events().length, 1); assert.equal(b.fetches.length, 1);
    const [url, options] = b.fetches[0]; assert.equal(url, '/shop/api/v2/orders/' + orderId + '/measurement');
    assert.equal(options.method, 'GET'); assert.equal(options.headers['X-CSRF-Token'], 'synthetic-csrf-token-1234');
    assert.equal(options.credentials, 'same-origin'); assert.equal(options.cache, 'no-store'); assert.equal(options.body, undefined);
    b.w.eval(adapter); assert.equal(b.fetches.length, 1); b.close();
  });
  await check('authorization/no payment proof polls without conversion; bounded five-minute window', async () => {
    const b = browser({consent: 'granted'}); configureOrder(b); b.w.gaertnerAdsRememberCheckout(orderId);
    b.w.fetch = async () => response({purchase: null}); b.w.eval(adapter); await settle();
    assert.equal(b.events().length, 0); assert.equal(b.timers.length, 1); assert.equal(b.timers[0].delay, 5000);
    for (let i = 0; i < 65; i++) { const next = b.timers.shift(); if (!next) break; await next.fn(); await settle(); }
    assert.equal(b.events().length, 0); assert.equal(b.timers.length, 0); b.close();
  });
  await check('return URL alone, missing CSRF, tampered endpoint and extra proof data never convert', async () => {
    for (const variant of ['unmarked', 'csrf', 'endpoint', 'extra']) {
      const b = browser({consent: 'granted'}); configureOrder(b);
      if (variant !== 'unmarked') b.w.gaertnerAdsRememberCheckout(orderId);
      if (variant === 'csrf') b.w.document.querySelector('meta').content = '';
      if (variant === 'endpoint') b.w.document.body.dataset.measurementUrl = 'https://untrusted.invalid/probe';
      b.w.fetch = async (...args) => { b.fetches.push(args); return response({purchase: {...paid(), email: 'private@example.invalid'}}); };
      b.w.eval(adapter); await settle(); assert.equal(b.events().length, 0);
      assert.equal(b.fetches.length, variant === 'extra' ? 1 : 0); b.close();
    }
  });
  await check('revocation during status fetch prevents conversion', async () => {
    const b = browser({consent: 'granted'}); configureOrder(b); b.w.gaertnerAdsRememberCheckout(orderId);
    let resolve; b.w.fetch = () => new Promise(done => { resolve = done; }); b.w.eval(adapter);
    b.deny(); resolve(response({purchase: paid()})); await settle(); assert.equal(b.events().length, 0); b.close();
  });
  await check('transport/non-JSON/unauthorized responses do not turn into purchases', async () => {
    for (const result of [response({purchase: paid()}, {ok: false}), response({purchase: paid()}, {mime: 'text/html'})]) {
      const b = browser({consent: 'granted'}); configureOrder(b); b.w.gaertnerAdsRememberCheckout(orderId);
      b.w.fetch = async () => result; b.w.eval(adapter); await settle(); assert.equal(b.events().length, 0); assert.equal(b.timers.length, 0); b.close();
    }
  });
  console.log('OK ' + checks + ' offline Google Ads measurement checks; zero external requests.');
})().catch(error => { console.error(error); process.exitCode = 1; });
