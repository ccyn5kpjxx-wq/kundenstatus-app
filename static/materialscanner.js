/* Camera recognition is optional. The captured photo remains the server's source. */
(function (root, factory) {
  'use strict';
  if (typeof module === 'object' && module.exports) module.exports = {createMaterialCodeScanner: factory};
  else root.createMaterialCodeScanner = factory;
})(typeof window === 'undefined' ? globalThis : window, function (options) {
  'use strict';
  const {document, window, canAdd, onPhoto, onFallback} = options;
  const $ = name => document.getElementById('material-scan-' + name);
  const modal = $('dialog'), video = $('video'), status = $('status');
  const schedule = options.setTimeout || window.setTimeout.bind(window);
  const clear = options.clearTimeout || window.clearTimeout.bind(window);
  const File = options.File || window.File;
  let generation = 0, stream = null, frameTimer = null, deadline = null, capturing = false, previousFocus = null;
  function stopTracks(value) { for (const track of value?.getTracks?.() || []) track.stop(); }
  function stop(restoreFocus = true) {
    generation++; clear(frameTimer); clear(deadline); frameTimer = deadline = null;
    stopTracks(stream); stream = null; video.pause?.(); video.srcObject = null;
    video.hidden = true; $('capture').hidden = true; modal.hidden = true; capturing = false;
    if (restoreFocus) previousFocus?.focus?.();
  }
  function fallback(message) {
    stopTracks(stream); stream = null; clear(frameTimer); clear(deadline); frameTimer = deadline = null;
    video.pause?.(); video.srcObject = null; video.hidden = true; $('capture').hidden = true;
    status.textContent = message;
  }
  function localDetector() {
    const ZXing = options.ZXingBrowser || window.ZXingBrowser;
    if (!ZXing?.BrowserMultiFormatReader || !ZXing.BarcodeFormat) return null;
    const reader = new ZXing.BrowserMultiFormatReader(new Map());
    reader.possibleFormats = ['QR_CODE', 'EAN_13', 'EAN_8', 'UPC_A', 'UPC_E'].map(name => ZXing.BarcodeFormat[name]);
    const canvas = document.createElement('canvas'), context = canvas.getContext('2d', {willReadFrequently: true});
    return {async detect(source) {
      if (!source.videoWidth || !source.videoHeight) return [];
      const ratio = Math.min(1, 1280 / Math.max(source.videoWidth, source.videoHeight));
      canvas.width = Math.round(source.videoWidth * ratio); canvas.height = Math.round(source.videoHeight * ratio);
      context.drawImage(source, 0, 0, canvas.width, canvas.height);
      try {
        const result = reader.decodeFromCanvas(canvas);
        return result ? [{rawValue: result.getText(), format: result.getBarcodeFormat()}] : [];
      } catch (error) {
        const kind = error?.getKind?.() || error?.constructor?.kind || error?.name;
        if (['NotFoundException', 'ChecksumException', 'FormatException'].includes(kind)) return [];
        throw error;
      }
    }};
  }
  async function capture(ticket) {
    if (ticket !== generation || modal.hidden || capturing || !canAdd()) return;
    if (!video.videoWidth || !video.videoHeight) { status.textContent = 'Die Kamera wird noch vorbereitet. Oder fotografiere den Code.'; return; }
    capturing = true; clear(frameTimer); $('capture').disabled = true;
    try {
      const canvas = document.createElement('canvas');
      const ratio = Math.min(1, 2048 / Math.max(video.videoWidth, video.videoHeight));
      canvas.width = Math.round(video.videoWidth * ratio); canvas.height = Math.round(video.videoHeight * ratio);
      canvas.getContext('2d').drawImage(video, 0, 0, canvas.width, canvas.height);
      const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', .94));
      if (ticket !== generation || modal.hidden || !canAdd()) return;
      if (!blob) throw new Error('capture');
      const file = new File([blob], 'artikelcode.jpg', {type: 'image/jpeg'});
      stop(); onPhoto(file);
    } catch (_) {
      if (ticket === generation) fallback('Das Kamerabild konnte nicht übernommen werden. Bitte den Code fotografieren.');
    } finally { if (ticket === generation) { capturing = false; $('capture').disabled = false; } }
  }
  async function inspect(detector, ticket) {
    if (ticket !== generation || modal.hidden || capturing) return;
    try {
      if (video.readyState >= 2) {
        const codes = await detector.detect(video);
        if (ticket !== generation || modal.hidden || capturing) return;
        if (codes.length === 1) { status.textContent = 'Code gefunden. Foto wird übernommen …'; await capture(ticket); return; }
        if (codes.length > 1) status.textContent = 'Mehrere Codes im Bild. Bitte nur einen Artikel vor die Kamera halten.';
      }
      if (ticket === generation && !modal.hidden && !capturing) frameTimer = schedule(() => {void inspect(detector, ticket);}, 250);
    } catch (_) { if (ticket === generation) fallback('Der direkte Scan ist hier nicht verfügbar. Fotografiere den Code; danach suchen wir direkt den Artikel.'); }
  }
  async function open() {
    if (!canAdd() || !modal.hidden) return;
    previousFocus = document.activeElement; stop(false); modal.hidden = false;
    const ticket = generation;
    $('close').focus(); $('capture').disabled = false;
    status.textContent = 'Kamera wird vorbereitet. Du kannst den Code auch direkt fotografieren.';
    const Detector = options.BarcodeDetector || window.BarcodeDetector;
    const devices = options.mediaDevices || window.navigator?.mediaDevices;
    if (!window.isSecureContext || !devices?.getUserMedia) {
      fallback('Fotografiere den QR-Code oder Barcode. Danach suchen wir direkt den Artikel. Ohne Code genügt ein normales Artikelfoto.'); return;
    }
    // Permission prompts may remain unanswered. Cancel and late resolution are both safe.
    deadline = schedule(() => {
      if (ticket !== generation) return;
      generation++; fallback('Die Kamera ist noch nicht bereit. Nutze „Code fotografieren“ oder versuche es erneut.');
    }, 30000);
    try {
      let detector = null;
      if (Detector) {
        try {
          const supported = await Detector.getSupportedFormats();
          if (ticket !== generation || modal.hidden) return;
          const formats = ['qr_code', 'ean_13', 'ean_8', 'upc_a', 'upc_e'].filter(format => supported.includes(format));
          if (formats.length) detector = new Detector({formats});
        } catch (_) { /* The bundled decoder also works without native support. */ }
      }
      if (ticket !== generation || modal.hidden) return;
      detector ||= localDetector();
      if (!detector) { fallback('Dieser Browser unterstützt keinen direkten Scan. Fotografiere den Code; danach suchen wir den Artikel.'); return; }
      const acquired = await devices.getUserMedia({audio: false, video: {facingMode: {ideal: 'environment'}, width: {ideal: 1920}, height: {ideal: 1080}}});
      if (ticket !== generation || modal.hidden || !canAdd()) { stopTracks(acquired); return; }
      stream = acquired; video.muted = true; video.playsInline = true; video.srcObject = stream; video.hidden = false;
      await video.play();
      if (ticket !== generation || modal.hidden) return;
      clear(deadline); deadline = schedule(() => {
        if (ticket === generation) { generation++; fallback('Noch kein Code gefunden. Du kannst ihn fotografieren oder ein Artikelfoto verwenden.'); }
      }, 60000);
      $('capture').hidden = false; status.textContent = 'Halte einen QR-Code oder Barcode ruhig in die Mitte. Die Stückzahl wählst du danach.';
      void inspect(detector, ticket);
    } catch (_) {
      if (ticket === generation) fallback('Die Kamera konnte nicht geöffnet werden. Bitte „Code fotografieren“ oder ein vorhandenes Bild verwenden.');
    }
  }
  $('close').addEventListener('click', () => stop());
  $('fallback').addEventListener('click', () => { if (!canAdd()) return; stop(); onFallback(); });
  $('capture').addEventListener('click', () => {void capture(generation);});
  modal.addEventListener('keydown', event => {
    if (event.key === 'Escape') {event.preventDefault(); stop();}
    if (event.key === 'Tab') {
      const first = $('close'), last = $('fallback');
      if (event.shiftKey && document.activeElement === first) {event.preventDefault(); last.focus();}
      else if (!event.shiftKey && document.activeElement === last) {event.preventDefault(); first.focus();}
    }
  });
  document.addEventListener?.('visibilitychange', () => {if (document.hidden) stop(false);});
  window.addEventListener('pagehide', () => stop(false));
  return {open, stop};
});
