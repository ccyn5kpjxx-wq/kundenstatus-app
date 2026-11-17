/* Read-only, customer-scoped proof. Return URLs and authorizations are not purchases. */
(() => {
  'use strict';
  const body = document.body;
  const receipt = document.querySelector('[data-shop-lead-id]');
  const leadId = receipt?.dataset.shopLeadId, kind = receipt?.dataset.adsEventKind || 'shop_lead';
  if (/^lead_ads_[a-f0-9]{64}$/.test(leadId || '') && ['form', 'shop_lead'].includes(kind)) {
    const observed = window.__gaertnerAdsObservedLeads || (window.__gaertnerAdsObservedLeads = new Set());
    if (!observed.has(leadId)) { observed.add(leadId); window.gaertnerAdsTrack?.(kind, {transaction_id: leadId}); }
  }
  if (!body || body.dataset.page !== 'order_status' || window.__gaertnerShopConversions) return;
  const orderId = body.dataset.orderId, endpoint = body.dataset.measurementUrl;
  if (!/^sho_[a-f0-9]{32}$/.test(orderId || '') || endpoint !== '/shop/api/v2/orders/' + orderId + '/measurement') return;
  window.__gaertnerShopConversions = true;
  const csrf = document.querySelector('meta[name="shop-csrf"]')?.content;
  if (!/^[A-Za-z0-9_\-.:]{16,256}$/.test(csrf || '')) return;
  let checks = 0, stopped = false;
  async function poll() {
    if (stopped || checks >= 60 || !window.gaertnerAdsCheckoutAllowed?.(orderId)) return;
    checks++;
    try {
      const response = await window.fetch(endpoint, {method: 'GET', credentials: 'same-origin', mode: 'same-origin',
        cache: 'no-store', referrerPolicy: 'no-referrer', headers: {'Accept': 'application/json', 'X-CSRF-Token': csrf}});
      if (!response.ok || !/application\/json/i.test(response.headers.get('Content-Type') || '')) { stopped = true; return; }
      const result = await response.json();
      if (!window.gaertnerAdsCheckoutAllowed?.(orderId)) { stopped = true; return; }
      const proof = result?.purchase;
      if (proof && /^shop_ads_[a-f0-9]{64}$/.test(proof.transaction_id || '')
          && Object.keys(proof).length === 3 && proof.currency === 'EUR'
          && typeof proof.value === 'number' && Number.isFinite(proof.value) && proof.value > 0) {
        window.gaertnerAdsTrack?.('purchase', proof); stopped = true; return;
      }
      if (proof !== null) { stopped = true; return; }
    } catch (_) { /* Optional measurement never blocks status or checkout. */ }
    if (!stopped && checks < 60) window.setTimeout(poll, 5000);
  }
  // The checkout marker only exists if consent was granted before leaving for payment.
  poll();
})();
