"""Stateless personal product preview; synthetic pixels/catalog, no network."""
import copy
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_materialbestellung_portal as fixtures
from test_materialfoto_codes import qr_image, png
from werkstatt_materialbestellung import MAX_PREVIEW_BODY_BYTES
from werkstatt_materialfoto import LabelPreviewBusy
import werkstatt_fotoauslese as photo_reader
from werkzeug.datastructures import FileStorage

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = '/werkstatt/materialbestellung/artikelscan-vorschau'


class ProductPreviewTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.PortalTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.hit = dict(copy.deepcopy(self.f.f.hits[0]), artikelnummer='10035120',
                        produkt_name='Synthetisches Klebeband')
        self.lookup = Mock(return_value={'varianten': [self.hit], 'abdeckung': {}})
        self.f.p.cockpit_data.articles = self.lookup

    def preview(self, raw=None, **changes):
        return self.f.client.post(ENDPOINT,
            headers=changes.pop('headers', {'X-CSRF-Token': 'synthetic-csrf'}),
            data=dict(foto=(io.BytesIO(png(qr_image('10035120')) if raw is None else raw), 'code.png'), **changes))

    def database(self):
        db = self.f.p.get_db()
        try:
            return '\n'.join(db.iterdump())
        finally:
            db.close()

    def test_real_qr_returns_source_backed_name_without_any_database_change_or_vision(self):
        before = self.database()
        response = self.preview()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['lookup_status'], 'matched')
        self.assertEqual(response.json['product'], self.hit['produkt_name'])
        self.assertEqual(response.json['decodedCode'], '10035120')
        self.assertEqual(response.json['matches'][0]['quelle']['beleg_id'], 1)
        self.assertFalse(response.json['bestellbar'])
        self.lookup.assert_called_once_with('10035120')
        self.assertEqual(self.database(), before)
        self.assertEqual(self.f.f.vision_calls, [])
        self.assertEqual(self.f.p.workshop_orders.calls, [])
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertNotIn('file_base64', json.dumps(response.json))

    def test_no_match_fuzzy_sku_and_missing_provenance_never_invent_a_name(self):
        for hits in ([], [dict(self.hit, artikelnummer='10035120-OTHER')], [dict(self.hit, quellen=[])]):
            self.lookup.return_value = {'varianten': hits}
            result = self.preview().json
            self.assertEqual(result['lookup_status'], 'no_match')
            self.assertEqual(result['product'], '')
            self.assertEqual(result['matches'], [])

    def test_multiple_supplier_variants_and_partial_coverage_stay_ambiguous(self):
        self.lookup.return_value = {'varianten': [self.hit, dict(self.hit, lieferant='Anderer Lieferant')]}
        result = self.preview().json
        self.assertEqual(result['lookup_status'], 'ambiguous')
        self.assertEqual(result['product'], '')
        self.assertEqual(len(result['matches']), 2)
        self.lookup.return_value = {'varianten': [self.hit], 'abdeckung': {'begrenzt': True}}
        result = self.preview().json
        self.assertEqual(result['lookup_status'], 'ambiguous')
        self.assertTrue(result['partial'])

    def test_code_urls_private_payloads_and_commands_never_reach_lookup(self):
        for payload in ('https://example.test/product/10035120', 'secret-1234', '9 dringend bestellen'):
            result = self.preview(png(qr_image(payload))).json
            self.assertEqual(result['decodedCode'], '')
            self.assertEqual(result['product'], '')
            self.assertNotIn(payload, json.dumps(result))
        self.lookup.assert_not_called()

    def test_normal_photo_and_decoder_or_catalog_failure_keep_manual_fallback(self):
        result = self.preview(fixtures.picture()).json
        self.assertEqual(result['lookup_status'], 'no_code')
        self.lookup.assert_not_called()
        with patch('werkstatt_materialfoto._decode_codes', return_value={'version': 1, 'status': 'nicht_verfuegbar', 'codes': []}):
            # An unbound decoder response cannot establish any identifier.
            self.assertEqual(self.preview().json['decodedCode'], '')
        self.lookup.side_effect = RuntimeError('private database connection detail')
        result = self.preview().json
        self.assertEqual(result['lookup_status'], 'unavailable')
        self.assertEqual(result['decodedCode'], '10035120')
        self.assertNotIn('private database', json.dumps(result))

    def test_anonymous_admin_only_stale_employee_and_csrf_fail_before_decoding(self):
        with patch.object(self.f.p.assistant_material_photos, 'preview') as decode:
            for headers in ({}, {'X-CSRF-Token': 'wrong'}):
                self.assertEqual(self.preview(headers=headers).status_code, 403)
            for identity in ({}, {'admin': True}, {'assistent_mid': 1, 'assistent_version': 999}):
                with self.f.client.session_transaction() as state:
                    state.clear()
                    state.update(identity, csrf_token='synthetic-csrf')
                self.assertEqual(self.preview().status_code, 401)
            decode.assert_not_called()

    def test_changed_rights_during_read_do_not_release_product_result(self):
        original = self.f.p.assistant_material_photos.preview
        def revoke(*args, **kwargs):
            result = original(*args, **kwargs)
            self.f.f.f.sql('UPDATE assistent_rechte SET version=version+1 WHERE mitarbeiter_id=1')
            return result
        with patch.object(self.f.p.assistant_material_photos, 'preview', side_effect=revoke):
            response = self.preview()
        self.assertEqual(response.status_code, 403)
        self.assertNotIn('matches', response.json)

    def test_upload_limits_extra_payload_and_nonimage_are_rejected(self):
        self.assertEqual(self.preview(b'<script>not an image</script>').status_code, 400)
        self.assertEqual(self.preview(decodedCode='10035120').status_code, 400)
        self.assertEqual(self.preview(b'x' * (MAX_PREVIEW_BODY_BYTES + 1)).status_code, 413)
        self.lookup.assert_not_called()

    def test_native_processing_busy_fails_fast_without_writes_or_lookup_and_recovers(self):
        before = self.database()
        self.assertTrue(photo_reader._PHOTO_SLOTS.acquire(blocking=False))
        try:
            started = time.perf_counter()
            response = self.preview()
            self.assertEqual(response.status_code, 400)
            self.assertIn('belegt', response.json['error'])
            self.assertLess(time.perf_counter() - started, 1)
        finally:
            photo_reader._PHOTO_SLOTS.release()
        self.assertEqual(self.database(), before)
        self.lookup.assert_not_called()
        self.assertEqual(self.f.f.vision_calls, [])
        self.assertEqual(self.f.p.workshop_orders.calls, [])
        self.assertEqual(self.preview().status_code, 200)

    def test_personal_rate_limit_and_cache_size_are_bounded_without_writes(self):
        before = self.database()
        with patch('werkstatt_materialbestellung.PREVIEW_RATE_LIMIT', 2):
            self.assertEqual(self.preview().status_code, 200)
            self.assertEqual(self.preview().status_code, 200)
            limited = self.preview()
            self.assertEqual(limited.status_code, 429)
            self.assertEqual(limited.headers['Retry-After'], '60')
        self.assertEqual(self.database(), before)
        for number in range(1100):
            self.f.portal.preview_allowed('mitarbeiter:' + str(number + 100))
        self.assertLessEqual(len(self.f.portal._preview_requests), 1024)

    def test_explicit_label_reading_excludes_rejected_code_and_stays_unverified(self):
        before = self.database()
        self.f.p.assistant_material_photos.vision = Mock(return_value={
            'art': 'produkt', 'produkt': 'Korrigierter Etikettname', 'artikelnummer': '10035120'})
        result = self.preview(modus='etikett', abgelehnte_codes='["10035120"]').json
        self.assertEqual(result['lookup_status'], 'label')
        self.assertEqual(result['product'], 'Korrigierter Etikettname')
        self.assertEqual(result['matches'], [])
        self.assertEqual(result['decodedCode'], '')
        self.assertTrue(result['pruefen'])
        self.assertFalse(result['bestellbar'])
        self.assertEqual(self.database(), before)

    def test_label_vision_concurrency_fails_fast_and_releases_slot_after_error(self):
        started, release = threading.Event(), threading.Event()
        def slow_reader(*_):
            started.set()
            release.wait(5)
            raise RuntimeError('synthetic provider failure')
        self.f.p.assistant_material_photos.vision = slow_reader
        who = self.f.who()
        def read():
            return self.f.p.assistant_material_photos.preview(who,
                FileStorage(stream=io.BytesIO(fixtures.picture()), filename='label.png'), read_label=True)
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(read)
            self.assertTrue(started.wait(2))
            try:
                with self.assertRaises(LabelPreviewBusy):
                    read()
                # Code-only lookup remains independent of the occupied reader.
                self.assertEqual(self.preview().status_code, 200)
                self.assertEqual(self.preview(modus='etikett').status_code, 429)
            finally:
                release.set()
            self.assertEqual(first.result(timeout=2)['lookup_status'], 'unavailable')
        self.f.p.assistant_material_photos.vision = lambda *_: {'art': 'produkt', 'produkt': 'Etikettname'}
        self.assertEqual(read()['lookup_status'], 'label')

    def test_personal_history_prefers_explicit_admin_article_identity(self):
        response = self.f.submit()
        saved = response.json['anforderungen'][0]
        original = self.f.p.material_dialog._view
        def manually_assigned(*args):
            view = original(*args)
            view['fields']['manual_article'] = {'value': {'product_name': 'Manuell geprüfter Artikel'}}
            view['analysis'] = {'merkmale': {'produkt': 'Alter falscher Artikel'}}
            return view
        with patch.object(self.f.p.material_dialog, '_view', side_effect=manually_assigned):
            listed = self.f.get().json['anforderungen']
        self.assertEqual(next(row for row in listed if row['id'] == saved['id'])['product'], 'Manuell geprüfter Artikel')

    def test_correction_keeps_both_originals_in_one_intake_and_replay_binds_label(self):
        first, label = png(qr_image('10035120')), fixtures.picture('red')
        client_id, request_id = fixtures.uid(), fixtures.uid()
        def data(label_raw=label, correction=True):
            position = dict(id=client_id, menge=2, dringend=False, artikelkorrektur=correction,
                            abgelehnte_codes=['10035120'], etikett_datei=True, beschreibung='Korrigiertes Produkt')
            return dict(request_id=request_id, positionen=json.dumps([position]), csrf_token='synthetic-csrf',
                        **{'foto_' + client_id: (io.BytesIO(first), 'scan.png'),
                           'etikett_' + client_id: (io.BytesIO(label_raw), 'etikett.png')})
        response = self.f.client.post('/werkstatt/materialbestellung/anforderungen', data=data())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json['anforderungen']), 1)
        db = self.f.p.get_db()
        try:
            source = db.execute('SELECT * FROM einkauf_material_nachrichten').fetchone()
            details = fixtures.portal_request_details(source)
            self.assertTrue(details['artikelkorrektur'])
            self.assertEqual(details['abgelehnte_codes'], ['10035120'])
            files = db.execute('SELECT id,original_base64 FROM einkauf_eingang_dateien ORDER BY id').fetchall()
            self.assertEqual(len(files), 2)
            import base64
            self.assertEqual([base64.b64decode(row['original_base64']) for row in files], [first, label])
            self.assertEqual(source['file_id'], files[1]['id'])
        finally:
            db.close()
        self.assertEqual(self.f.client.post('/werkstatt/materialbestellung/anforderungen', data=data()).status_code, 200)
        self.assertEqual(self.f.client.post('/werkstatt/materialbestellung/anforderungen', data=data(fixtures.picture('green'))).status_code, 409)
        self.assertEqual(self.f.client.post('/werkstatt/materialbestellung/anforderungen', data=data(correction=False)).status_code, 400)
        self.assertEqual(self.f.p.workshop_orders.calls, [])


class ProductPreviewBrowserTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node.js required for browser behavior checks')
    def test_async_preview_is_source_visible_optional_and_preserves_photo_binding(self):
        # Reuse the existing synthetic DOM/fetch harness, enabling the actual
        # template's preview data attribute. No real browser/service is accessed.
        harness = (ROOT / 'scripts/test_materialbestellung_ui.js').read_text(encoding='utf-8').split("test('two photos", 1)[0]
        harness = harness.replace("require('../static/materialbestellung.js')", "require('./static/materialbestellung.js')")
        harness = harness.replace("actor: 'mitarbeiter:7', limitCents", "actor: 'mitarbeiter:7', previewEndpoint: '/preview', limitCents")
        behavior = r"""
(async () => {
  const f = fixture(), gate = deferred();
  f.api.fetch = async (url, settings) => {
    f.calls.push({url, settings});
    if (url === '/preview') return gate.promise;
    const rows = JSON.parse(settings.body.fields.get('positionen'));
    return reply({request_id: settings.body.fields.get('request_id'), anforderungen: rows.map((row, i) =>
      ({client_id: row.id, id: i + 1, state: 'review', quantity: String(row.menge), unit: 'Stück'}))});
  };
  const first = photo('first.jpg'); f.controller.addFiles([first]);
  assert.equal(f.calls[0].url, '/preview');
  assert.equal(f.calls[0].settings.body.fields.get('foto'), first);
  assert.equal(f.calls[0].settings.headers['X-CSRF-Token'], 'synthetic-csrf');
  assert.equal(f.$('submit').disabled, true, 'Give the preview a result opportunity before sending');
  const input = f.quantity(0); input.value = '7';
  gate.resolve(reply({lookup_status: 'matched', decodedCode: '10035120', product: '<script>literal</script>',
    message: 'Mit dem Foto vergleichen', matches: [{produkt_name: 'Klebeband', lieferant: 'Testlieferant',
      artikelnummer: '10035120', quelle: {art: 'einkauf', beleg_id: 17, seite: 2}}]}));
  await settle();
  assert.equal(f.quantity(0), input, 'Late preview must not rebuild focused controls');
  assert.equal(input.value, '7');
  const all = descendants(f.cards()[0]);
  assert.ok(all.some(el => el.tagName === 'H3' && el.textContent === '<script>literal</script>'));
  assert.ok(all.some(el => /Quelle: Einkaufsbeleg 17/.test(el.textContent)));
  const original = all.find(el => el.tagName === 'A');
  assert.match(original.href, /^blob:/); assert.equal(original.target, '_blank');
  assert.equal(f.$('submit').disabled, true, 'A displayed name needs explicit confirmation');
  f.step(0, 'Artikel stimmt').click(); assert.equal(f.$('submit').disabled, false);
  await f.controller.submit();
  const submitted = f.calls.find(call => call.url !== '/preview');
  const rows = JSON.parse(submitted.settings.body.fields.get('positionen'));
  assert.equal(rows[0].menge, 7); assert.equal(submitted.settings.body.fields.get('foto_' + rows[0].id), first);
  assert.equal('decodedCode' in rows[0], false); assert.equal('product' in rows[0], false);

  const g = fixture(); g.api.fetch = async () => {throw new Error('offline');};
  g.controller.addFiles([photo()]); await settle();
  assert.equal(g.$('submit').disabled, false);
  assert.ok(descendants(g.cards()[0]).some(el => el.tagName === 'DETAILS' && el.open));

  const h = fixture(), late = deferred(); h.api.fetch = () => late.promise;
  h.controller.addFiles([photo()]); h.step(0, 'Entfernen').click();
  late.resolve(reply({lookup_status: 'matched', product: 'Removed'})); await settle();
  assert.equal(h.cards().length, 0, 'Removed photo must not be restored by a late response');

  const q = fixture(), pending = deferred(); let previews = 0;
  q.api.fetch = async (url, settings) => {
    if (url === '/preview') {previews++; return pending.promise;}
    const rows = JSON.parse(settings.body.fields.get('positionen'));
    return reply({request_id: settings.body.fields.get('request_id'), anforderungen: rows.map((row, i) =>
      ({client_id: row.id, id: i + 1, state: 'review'}))});
  };
  q.controller.addFiles([photo(), photo('second.jpg')]);
  await q.controller.submit(); assert.equal(q.cards().length, 2); assert.equal(previews, 1);
  pending.resolve(reply({lookup_status: 'no_match', message:'Manuell zuordnen'})); await settle();
  assert.equal(previews, 2); assert.equal(q.$('submit').disabled, false);
  await q.controller.submit(); assert.equal(q.cards().length, 0);

  const corrected = fixture(); let scanCalls = 0, correctionPost;
  corrected.api.fetch = async (url, settings) => {
    if (url === '/preview') {
      scanCalls++;
      return reply(scanCalls === 1 ? {lookup_status:'matched', decodedCode:'10035120', product:'Falsches Band', matches:[]}
        : {lookup_status:'label', product:'Richtige Abdeckfolie', matches:[]});
    }
    correctionPost = settings.body.fields;
    const rows = JSON.parse(correctionPost.get('positionen'));
    return reply({request_id:correctionPost.get('request_id'), anforderungen:rows.map((row, i) => ({client_id:row.id, id:i+1, state:'review'}))});
  };
  const scan = photo('original-qr.png'), label = photo('label.png');
  corrected.controller.addFiles([scan]); await settle();
  corrected.step(0, 'Falsches Produkt · Etikett fotografieren').click();
  assert.equal(corrected.$('camera').clicked, true);
  corrected.$('camera').files = [label]; corrected.$('camera').fire('change'); await settle();
  assert.equal(corrected.cards().length, 1, 'Correction keeps one article');
  assert.equal(scanCalls, 2);
  corrected.step(0, 'Artikel stimmt').click();
  corrected.quantity(0).value = '3'; corrected.quantity(0).fire('change');
  const urgent = corrected.field(0, 'INPUT', el => el.type === 'radio' && el.value === 'dringend');
  urgent.checked = true; urgent.fire('change');
  await corrected.controller.submit();
  const correctionRows = JSON.parse(correctionPost.get('positionen'));
  assert.equal(correctionRows[0].artikelkorrektur, true);
  assert.deepEqual(correctionRows[0].abgelehnte_codes, ['10035120']);
  assert.equal(correctionRows[0].beschreibung, 'Richtige Abdeckfolie');
  assert.equal(correctionRows[0].menge, 3); assert.equal(correctionRows[0].dringend, true);
  assert.equal(rows[0].dringend, false, 'The first confirmed article used the clearly selected regular timing');
  assert.equal(correctionPost.get('foto_' + correctionRows[0].id), scan);
  assert.equal(correctionPost.get('etikett_' + correctionRows[0].id), label);

  let scannerBindings;
  const cancelled = fixture({scannerFactory: settings => {scannerBindings = settings; return {open(){},stop(){}};}});
  cancelled.api.fetch = async () => reply({lookup_status:'matched', decodedCode:'10035120', product:'Erstes Produkt'});
  cancelled.controller.addFiles([photo()]); await settle();
  cancelled.step(0, 'Falsches Produkt · Etikett fotografieren').click();
  cancelled.$('camera').fire('cancel'); cancelled.$('scan-button').click(); scannerBindings.onFallback();
  cancelled.$('camera').files = [photo('new-scan.jpg')]; cancelled.$('camera').fire('change'); await settle();
  assert.equal(cancelled.cards().length, 2, 'Cancelled correction may not capture a later scan into the old row');
})().catch(error => {console.error(error); process.exitCode = 1;});
"""
        result = subprocess.run([shutil.which('node'), '-e', harness + behavior], cwd=ROOT,
                                capture_output=True, text=True, encoding='utf-8', timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
