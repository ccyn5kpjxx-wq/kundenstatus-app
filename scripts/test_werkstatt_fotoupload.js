'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { uploadSeries, uploadProof, bind } = require('../static/werkstatt_fotoupload.js');

const url = 'http://localhost:5091/werkstatt/auftrag/156';
const files = Array.from({ length: 7 }, (_, n) => ({ name: `original-${n}.jpg`, size: 6 * 1024 * 1024 }));
const proofPage = (id = '156', marker = true, warning = '') => ({
  body: { dataset: { auftragId: id } },
  querySelector: selector => selector.startsWith('[data-') ? (marker ? {} : null) :
    (warning ? { textContent: warning } : null)
});
const response = (status = 200, responseUrl = url) => ({ ok: status >= 200 && status < 300, status, url: responseUrl });

test('42 MiB selection sends original objects serially, one request at a time', async () => {
  let active = 0, peak = 0;
  const received = [], progress = [];
  const result = await uploadSeries(files, async file => {
    active++; peak = Math.max(peak, active); received.push(file);
    await Promise.resolve(); active--;
    return { state: 'saved' };
  }, 25 * 1024 * 1024 - 65536, (...args) => progress.push(args));
  assert.equal(peak, 1); assert.equal(result.saved, 7); assert.equal(result.complete, true);
  files.forEach((file, n) => assert.equal(received[n], file));
  assert.deepEqual(progress, files.map((_, n) => [n + 1, 7]));
});

test('lost response stops without retry; only confirmed saves count', async () => {
  let calls = 0;
  const result = await uploadSeries(files, async () => {
    if (++calls === 3) throw new Error('connection lost after server saved');
    return { state: 'saved' };
  }, 25 * 1024 * 1024);
  assert.equal(calls, 3); assert.equal(result.saved, 2); assert.equal(result.pending, 4);
  assert.equal(result.results[2].state, 'unknown'); assert.equal(result.complete, false);
});

test('single oversized file is refused before network, later files remain unsent', async () => {
  let calls = 0;
  const result = await uploadSeries([{ name: 'too-large.jpg', size: 101 }, ...files], async () => {
    calls++; return { state: 'saved' };
  }, 100);
  assert.equal(calls, 0); assert.equal(result.results[0].state, 'rejected');
  assert.equal(result.pending, 7);
});

test('confirmation requires success marker, correct order and same-origin detail URL', () => {
  assert.deepEqual(uploadProof(response(), proofPage(), url, 156), { state: 'saved' });
  for (const [reply, page] of [
    [response(), proofPage('156', false)], [response(), proofPage('157')],
    [response(200, 'http://localhost:5091/login'), proofPage()],
    [response(200, 'https://other.example/werkstatt/auftrag/156'), proofPage()],
    [response(200, ''), proofPage()], [response(), {}]
  ]) assert.equal(uploadProof(reply, page, url, 156).state, 'unknown');
  const warning = uploadProof(response(), proofPage('156', false, ' Kein Foto gespeichert. '), url, 156);
  assert.equal(warning.state, 'rejected'); assert.equal(warning.message, 'Kein Foto gespeichert.');
  assert.equal(uploadProof(response(), proofPage('156', true, 'Upload abgewiesen'), url, 156).state, 'rejected');
  assert.equal(uploadProof(response(413), {}, url, 156).state, 'rejected');
  assert.equal(uploadProof(response(500), {}, url, 156).state, 'unknown');
});

function harness(outcomes) {
  const listeners = {}, events = [], requests = [], timeouts = new Map();
  const element = extra => ({ textContent: '', hidden: false,
    classList: { toggle: (name, active) => events.push([name, active]) }, ...extra });
  const gallery = element({ files: files.slice(0, 3), value: 'gallery-selection', disabled: false });
  const camera = element({ files: [files[6]], value: 'camera-selection', disabled: false });
  const status = element(), refresh = element(), overlay = element(), progress = element();
  const form = element({ action: url + '/fotos', dataset: {
    auftragId: '156', auftragUrl: url, maxDateiBytes: String(25 * 1024 * 1024 - 65536)
  }, querySelectorAll: selector => selector.includes('hidden') ? [{ name: 'csrf_token', value: 'test-csrf' }] :
    selector.includes('file') ? [gallery, camera] : [element()],
  setAttribute: (key, value) => events.push([key, value]),
  addEventListener: (name, handler) => { listeners[name] = handler; } });
  const selectors = { '[data-foto-upload]': form, '[data-foto-upload-status]': status,
    '[data-foto-upload-aktualisieren]': refresh, '[data-upload-overlay]': overlay,
    '[data-foto-upload-fortschritt]': progress };
  const doc = { querySelector: key => selectors[key], dispatchEvent: event => events.push(event.type) };
  const win = {
    location: { href: url, reload: () => { win.reloads++; } }, reloads: 0,
    FormData: class { constructor(...args) { assert.equal(args.length, 0); this.entries = []; }
      append(...args) { this.entries.push(args); } },
    DOMParser: class { parseFromString() { return proofPage(); } }, AbortController,
    Event: class { constructor(type) { this.type = type; } },
    setTimeout: handler => { const id = timeouts.size + 1; timeouts.set(id, handler); return id; },
    clearTimeout: id => timeouts.delete(id),
    addEventListener: (name, handler) => { listeners[name] = handler; },
    fetch: async (address, options) => {
      assert.equal(win.werkstattFotoUploadAktiv, true);
      assert.equal(gallery.disabled, true); assert.equal(camera.disabled, true);
      assert.equal(address, form.action); assert.equal(options.credentials, 'same-origin');
      requests.push(options.body.entries);
      const outcome = outcomes.shift();
      if (outcome instanceof Error) throw outcome;
      return { ...response(outcome), text: async () => 'specific server confirmation' };
    }
  };
  bind(doc, win);
  return { win, gallery, camera, status, refresh, events, requests, timeouts, listeners };
}

test('UI builds only CSRF + selected original, and reloads after all confirmations', async () => {
  const h = harness([200, 200, 200]);
  await h.win.fotoUploadStarten(h.gallery);
  assert.equal(h.requests.length, 3); assert.equal(h.win.reloads, 1);
  h.requests.forEach((fields, n) => {
    assert.deepEqual(fields[0], ['csrf_token', 'test-csrf']);
    assert.deepEqual(fields[1], ['fotos', files[n], files[n].name]);
    assert.equal(fields.length, 2);
  });
  assert.equal(h.win.werkstattFotoUploadAktiv, false); assert.equal(h.gallery.disabled, false);
  assert.equal(h.timeouts.size, 0); assert.equal(h.camera.value, '');
  assert.ok(h.events.includes('werkstatt-fotoupload-ende'));
});

test('UI reports partial/unknown upload, offers refresh and never resends', async () => {
  const h = harness([200, new Error('lost response')]);
  await h.win.fotoUploadStarten(h.gallery);
  assert.equal(h.requests.length, 2); assert.equal(h.win.reloads, 0); assert.equal(h.refresh.hidden, false);
  assert.match(h.status.textContent, /1 von 3/);
  assert.match(h.status.textContent, /vor einem erneuten Upload prüfen/);
  assert.match(h.status.textContent, /1 weitere Foto/);
  assert.equal(h.win.werkstattFotoUploadAktiv, false); assert.equal(h.timeouts.size, 0);
});
