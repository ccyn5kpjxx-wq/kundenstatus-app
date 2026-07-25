(() => {
  if (window.__gaertnerBesucherErfasst || navigator.webdriver) return;
  window.__gaertnerBesucherErfasst = true;
  const endpoint = 'https://kundenstatus-app.onrender.com/api/besucher';
  let referrer = '';
  try { referrer = document.referrer ? new URL(document.referrer).hostname : ''; } catch (error) {}
  const payload = JSON.stringify({
    website: 'autovermietung-mos',
    seite: window.location.pathname,
    referrer
  });
  const senden = () => {
    if (navigator.sendBeacon) {
      const body = new Blob([payload], { type: 'text/plain;charset=UTF-8' });
      if (navigator.sendBeacon(endpoint, body)) return;
    }
    window.fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      body: payload,
      credentials: 'omit',
      mode: 'cors',
      keepalive: true
    }).catch(() => {});
  };
  if (document.readyState === 'complete') senden();
  else window.addEventListener('load', senden, { once: true });
})();
