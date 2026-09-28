"""Synthetic private uploads, OCR proposals and exact original attachment; no network."""
import concurrent.futures
import io
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import test_assistent as fixture
from PIL import Image
from werkzeug.datastructures import FileStorage
from werkstatt_assistent_uploads import AssistantUploads, MAX_BYTES, ensure_upload_schema

p = fixture.p
database = fixture.database
ACTOR = 'mitarbeiter:1'


class UploadTests(unittest.TestCase):
    def setUp(self):
        self.legacy = fixture.AssistantTests(methodName='runTest')
        self.legacy.setUp()
        self.addCleanup(self.legacy.tearDown)
        self.enterContext(patch('requests.sessions.Session.request', side_effect=AssertionError('Network forbidden')))
        self.enterContext(patch.object(p, 'apply_document_data_to_auftrag', side_effect=AssertionError('No auto update')))
        self.enterContext(patch.object(p, 'get_auftrag', side_effect=AssertionError('No mutating order hydration')))
        self.analysis = self.enterContext(patch.object(p, 'build_document_analysis_bundle_safe', return_value={
            'text': '', 'structured': {'fahrzeug': 'Audi Test', 'kennzeichen': 'TEST-1',
                                     'beschreibung': 'Stoßfänger vorne rechts prüfen'}, 'status': 'ai_ready'}))
        self.service = AssistantUploads(p)
        with database() as db:
            db.execute('DELETE FROM assistent_uploads')
            db.execute('DELETE FROM datei_backups')
        image = Image.new('RGB', (32, 24), 'green')
        stream = io.BytesIO()
        image.save(stream, format='PNG')
        self.raw = stream.getvalue()

    def stage(self, purpose='schaden', raw=None, filename='synthetic.png', key='synthetic-upload-123456789'):
        return self.service.stage(ACTOR, FileStorage(stream=io.BytesIO(self.raw if raw is None else raw),
                                  filename=filename, content_type='untrusted/client-type'), key, purpose)

    def prepared(self, **args):
        staged = self.stage(**args)
        return self.service.analyze(ACTOR, staged['id'])

    def rows(self, sql, args=()):
        with database() as db:
            return [dict(row) for row in db.execute(sql, args).fetchall()]

    def test_stage_validates_content_and_retries_without_order_or_file_writes(self):
        before = self.rows('SELECT * FROM auftraege')
        one = self.stage(filename='../../synthetic.png')
        two = self.stage(filename='synthetic.png')
        self.assertEqual(one['id'], two['id'])
        self.assertEqual(one['mime_type'], 'image/png')
        self.assertNotIn('/', one['original_name'])
        self.assertNotIn('file_base64', json.dumps(one))
        self.assertEqual(self.service.read_content(ACTOR, one['id'])[0], self.raw)
        self.assertEqual(len(self.rows('SELECT * FROM assistent_uploads')), 1)
        self.assertEqual(self.rows('SELECT * FROM dateien'), [])
        self.assertEqual(self.rows('SELECT * FROM auftraege'), before)
        self.analysis.assert_not_called()
        for raw, name in ((b'<html>bad</html>', 'bad.png'), (b'fake pdf', 'bad.pdf'),
                          (self.raw, 'photo.docx'), (b'x' * (MAX_BYTES + 1), 'large.jpg')):
            with self.subTest(name=name), self.assertRaises(ValueError):
                self.stage(raw=raw, filename=name)
        with self.assertRaises(ValueError):
            self.stage(purpose='fahrzeugschein')  # Same request key must not mean new input.

    def test_ownership_is_required_for_every_operation_and_mail_resolver(self):
        item = self.prepared()
        for actor in ('admin', 'mitarbeiter:2', '../../unsafe'):
            for operation in (lambda: self.service.get(actor, item['id']),
                              lambda: self.service.analyze(actor, item['id']),
                              lambda: self.service.read_content(actor, item['id']),
                              lambda: self.service.attach(actor, item['id'], 156, confirmed=True)):
                with self.subTest(actor=actor), self.assertRaises(ValueError):
                    operation()
        self.assertEqual(self.service.list('admin'), [])
        self.assertEqual(self.service.list(ACTOR)[0]['id'], item['id'])

    def test_analysis_removes_bank_and_finance_data_keeps_vehicle_work_and_no_customer_guess(self):
        self.analysis.return_value = {'text': 'IBAN DE89370400440532013000\nGesamt netto 400,00 EUR',
                                     'structured': {'fahrzeug': 'Audi A4', 'kennzeichen': 'TEST-1',
                                        'kunde_name': 'Ella Musterfrau', 'kontakt_telefon': '0123456789',
                                        'fin_nummer': 'WAUZZZ8K9AA000001', 'hsn_nummer': '0588',
                                        'tsn_nummer': 'ABC123', 'rep_max_kosten': '400.00',
                                        'beschreibung': 'Stoßfänger lackieren\nIBAN DE89370400440532013000\nGesamt brutto 400 EUR',
                                        'analyse_text': 'Farbe grün\nBIC TESTDEFFXXX'},
                                     'hint': 'Kontostand geheim', 'analysis_json': '{"bank": "secret"}'}
        before = self.rows('SELECT * FROM auftraege')
        item = self.prepared(purpose='fahrzeugschein')
        fields = item['analyse']['felder']
        self.assertEqual(fields['fahrzeug'], 'Audi A4')
        self.assertEqual(fields['beschreibung'], 'Stoßfänger lackieren')
        self.assertEqual(fields['fin_nummer'], 'WAUZZZ8K9AA000001')
        self.assertEqual(fields['analyse_text'], 'Farbe grün')
        self.assertEqual(fields['kunde_name'], 'Ella Musterfrau')
        self.assertNotIn('kontakt_telefon', fields)
        self.assertNotIn('rep_max_kosten', fields)
        for secret in ('DE893704', 'TESTDEFF', '400', 'secret', 'geheim'):
            self.assertNotIn(secret, json.dumps(item))
        self.assertTrue(item['analyse']['bankdaten_entfernt'])
        self.assertIn('Halter ist nicht automatisch', ' '.join(item['analyse']['hinweise']))
        self.assertEqual(self.rows('SELECT * FROM auftraege'), before)

    def test_registration_holder_is_only_a_reviewable_name_proposal_without_contact_inference(self):
        before = self.rows('SELECT * FROM auftraege')
        self.analysis.return_value = {'text': 'Fahrzeughalter: Ella Musterfrau\nKennzeichen: NEU-100', 'structured': {}}
        holder = self.prepared(purpose='fahrzeugschein')
        self.assertEqual(holder['analyse']['felder']['kunde_name'], 'Ella Musterfrau')
        self.assertIn('tatsächlichen Auftraggeber bestätigen', ' '.join(holder['analyse']['hinweise']))
        self.assertNotIn('kunde_email', holder['analyse']['felder'])
        self.assertNotIn('kontakt_telefon', holder['analyse']['felder'])
        self.assertEqual(self.rows('SELECT * FROM auftraege'), before)
        for purpose in ('angebot', 'schaden', 'sonstiges'):
            other = self.prepared(purpose=purpose, key='synthetic-holder-' + purpose + '-123456789')
            self.assertNotIn('kunde_name', other['analyse']['felder'])
        self.analysis.return_value = {'text': '', 'structured': {'anspruchsteller_name': 'Markus Muster'}}
        named = self.prepared(purpose='fahrzeugschein', key='synthetic-holder-structured-123456789')
        self.assertEqual(named['analyse']['felder']['kunde_name'], 'Markus Muster')

    def test_analysis_handles_failure_and_stale_lease_without_stranding_original(self):
        item = self.stage()
        with database() as db:
            db.execute("UPDATE assistent_uploads SET status='analyse',lease_token='old',lease_until=?", (time.time()-1,))
        self.analysis.side_effect = RuntimeError('IBAN secret must never surface')
        result = self.service.analyze(ACTOR, item['id'])
        self.assertEqual(result['status'], 'pruefen')
        self.assertEqual(result['analyse']['felder'], {})
        self.assertNotIn('secret', json.dumps(result))
        self.assertEqual(self.service.read_content(ACTOR, item['id'])[0], self.raw)
        row = self.rows('SELECT lease_token,lease_until FROM assistent_uploads')[0]
        self.assertEqual(row, {'lease_token': '', 'lease_until': 0.0})
        self.service.analyze(ACTOR, item['id'])
        self.assertEqual(self.analysis.call_count, 1)

    def test_only_explicit_offer_keeps_quote_prices_and_omits_invoice_or_bank_text(self):
        self.analysis.return_value = {'text': 'Angebot TEST-50\nStoßfänger 123,00 EUR netto\nIBAN DE89370400440532013000', 'structured': {}}
        item = self.prepared(purpose='angebot')
        self.assertIn('123,00 EUR netto', item['analyse']['angebotsinhalt'])
        self.assertNotIn('DE893704', json.dumps(item))
        attached = self.service.attach(ACTOR, item['id'], 156, confirmed=True)
        document = self.rows('SELECT * FROM dateien WHERE id=?', (attached['datei_id'],))[0]
        self.assertEqual(document['dokument_typ'], 'Angebot (ungeprüft)')
        self.assertEqual(document['analyse_quelle'], 'assistent_upload_v1')
        self.assertIn('123,00 EUR', document['extrahierter_text'])
        for index, excluded in enumerate(('Rechnung TEST-1\nSaldo 1000 EUR', 'Kontoauszug\n1000 EUR', 'Bilanz 2026\n1000 EUR')):
            self.analysis.return_value = {'text': excluded, 'structured': {}}
            blocked = self.prepared(purpose='angebot', key='synthetic-excluded-document-' + str(index))
            self.assertEqual(blocked['analyse']['angebotsinhalt'], '')
            self.assertNotIn('1000', json.dumps(blocked))

    def test_confirmed_attachment_is_original_private_backup_and_idempotent(self):
        item = self.prepared()
        before = self.rows('SELECT * FROM auftraege')
        with self.assertRaises(ValueError):
            self.service.attach(ACTOR, item['id'], 156)
        result = self.service.attach(ACTOR, item['id'], 156, confirmed=True)
        again = self.service.attach(ACTOR, item['id'], 156, confirmed=True)
        self.assertTrue(again['duplicate'])
        self.assertEqual(result['datei_id'], again['datei_id'])
        with self.assertRaises(ValueError):
            self.service.attach(ACTOR, item['id'], 157, confirmed=True)
        file = self.rows('SELECT * FROM dateien')[0]
        self.assertEqual(file['kategorie'], 'assistent')
        self.assertEqual(file['sichtbarkeit_geprueft'], 1)
        for audience in ('kunde', 'partner', 'versicherung'):
            self.assertEqual(file[audience + '_sichtbar'], 0)
            self.assertFalse(p.document_visible(dict(file, **{audience + '_sichtbar': 1}), audience))
        self.assertEqual(file['extrahierter_text'], '')
        path = p.UPLOAD_DIR / file['stored_name']
        self.assertEqual(path.read_bytes(), self.raw)
        path.unlink()
        self.assertEqual(p.ensure_upload_file_available(file).read_bytes(), self.raw)
        self.assertEqual(self.rows('SELECT * FROM auftraege'), before)
        self.assertEqual(len(self.rows('SELECT * FROM datei_backups')), 1)

    def test_atomic_attachment_rolls_back_with_new_order_transaction(self):
        item = self.prepared()
        with self.assertRaisesRegex(RuntimeError, 'abort synthetic transaction'):
            with self.service.db() as db:
                db.execute("INSERT INTO auftraege(id,fahrzeug,erstellt_am,geaendert_am) VALUES(999,'Synthetic',?,?)", (p.now_str(), p.now_str()))
                self.service.attach(ACTOR, item['id'], 999, confirmed=True, db=db)
                raise RuntimeError('abort synthetic transaction')
        self.assertEqual(self.rows('SELECT id FROM auftraege WHERE id=999'), [])
        self.assertEqual(self.rows('SELECT id FROM dateien'), [])
        self.assertEqual(self.rows('SELECT id FROM datei_backups'), [])
        self.assertIsNone(self.service.get(ACTOR, item['id'])['datei_id'])
        self.assertEqual(self.service.attach(ACTOR, item['id'], 156, confirmed=True)['auftrag_id'], 156)

    def test_concurrent_attachment_creates_one_committed_file(self):
        item = self.prepared()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.service.attach(ACTOR, item['id'], 156, confirmed=True), range(2)))
        self.assertEqual({row['datei_id'] for row in results}, {results[0]['datei_id']})
        self.assertEqual(len(self.rows('SELECT * FROM dateien')), 1)

    def test_active_order_and_finished_analysis_required(self):
        item = self.stage()
        with self.assertRaises(ValueError):
            self.service.attach(ACTOR, item['id'], 156, confirmed=True)
        self.service.analyze(ACTOR, item['id'])
        with database() as db:
            db.execute('UPDATE auftraege SET archiviert=1 WHERE id=156')
        for order in (156, 999, True, '157'):
            with self.subTest(order=order), self.assertRaises(ValueError):
                self.service.attach(ACTOR, item['id'], order, confirmed=True)
        self.assertEqual(self.rows('SELECT * FROM dateien'), [])

    def test_external_original_routes_deny_private_files_even_with_legacy_flags(self):
        item = self.prepared()
        saved = self.service.attach(ACTOR, item['id'], 156, confirmed=True)
        file = self.rows('SELECT * FROM dateien')[0]
        file.update(kunde_sichtbar=1, partner_sichtbar=1, versicherung_sichtbar=1)
        with patch.object(p, 'get_datei', return_value=file), \
             patch.object(p, 'partner_session_required', return_value=({'id': 1, 'slug': 'synthetic'}, None)), \
             patch.object(p, 'versicherung_session_required', return_value=({'id': 1}, None)), \
             patch.object(p, 'get_auftrag_by_kunden_status_token', return_value={'id': 156}), \
             patch.object(p, 'customer_order_documents_allowed', return_value=True):
            for route in (f'/partner/synthetic/datei/{saved["datei_id"]}',
                          f'/partner/synthetic/datei/{saved["datei_id"]}/download',
                          f'/versicherung/synthetic/datei/{saved["datei_id"]}',
                          f'/status/synthetic/dokument/{saved["datei_id"]}',
                          f'/status/synthetic/bild/{saved["datei_id"]}'):
                self.assertEqual(self.legacy.client.get(route).status_code, 404, route)
        self.assertTrue(p.document_visible({'quelle': 'autohaus', 'kategorie': 'standard'}, 'partner'))
        self.assertTrue(p.document_visible({'quelle': 'intern', 'kategorie': 'standard', 'sichtbarkeit_geprueft': 1, 'kunde_sichtbar': 1}, 'kunde'))

    def test_mail_resolver_only_exact_owned_damage_image(self):
        item = self.prepared()
        attached = self.service.attach(ACTOR, item['id'], 156, confirmed=True)
        result = self.service.attachment_resolver(ACTOR, 156, attached['datei_id'], 'lieferant')
        self.assertEqual(result['content'], self.raw)
        for actor, order, kind in (('admin', 156, 'kunde'), (ACTOR, 157, 'kunde'), (ACTOR, 156, 'broadcast')):
            with self.assertRaises(ValueError):
                self.service.attachment_resolver(actor, order, attached['datei_id'], kind)
        vehicle = self.prepared(purpose='fahrzeugschein', key='synthetic-vehicle-123456789')
        private = self.service.attach(ACTOR, vehicle['id'], 156, confirmed=True)
        with self.assertRaises(ValueError):
            self.service.attachment_resolver(ACTOR, 156, private['datei_id'], 'kunde')
        with database() as db:
            db.execute("UPDATE assistent_uploads SET file_sha256='tampered' WHERE upload_id=?", (item['id'],))
        with self.assertRaises(ValueError):
            self.service.attachment_resolver(ACTOR, 156, attached['datei_id'], 'kunde')

    def test_normal_pdf_allowed_active_and_encrypted_pdf_rejected(self):
        import fitz
        with fitz.open() as document:
            document.new_page().insert_text((40, 50), 'Synthetic test document')
            clean = document.tobytes()
            accepted = self.stage(raw=clean, filename='synthetic.pdf')
            self.assertEqual(accepted['mime_type'], 'application/pdf')
            js = document.get_new_xref()
            document.update_object(js, r'<< /S /JavaScript /JS (app.alert\(1\)) >>')
            document.xref_set_key(document.pdf_catalog(), 'OpenAction', f'{js} 0 R')
            with self.assertRaises(ValueError):
                self.stage(raw=document.tobytes(), filename='active.pdf', key='synthetic-active-123456789')
        with fitz.open() as document:
            document.new_page()
            with self.assertRaises(ValueError):
                self.stage(raw=document.tobytes(encryption=fitz.PDF_ENCRYPT_AES_256, owner_pw='test', user_pw='test'),
                           filename='encrypted.pdf', key='synthetic-encrypted-123456789')

    def test_schema_backup_includes_original_and_lease_can_recover_after_restore(self):
        item = self.stage()
        self.assertIn('assistent_uploads', p.BACKUP_TABLES)
        self.assertIn('werkstatt_avatar_uploads_v1', p.BACKUP_SCHEMA_FEATURES)
        with database() as db:
            rows, refs, size = p.write_table_rows_and_binary_blobs(db, None, 'assistent_uploads')
        self.assertEqual(rows[0]['upload_id'], item['id'])
        self.assertTrue(rows[0]['file_base64'])
        with tempfile.TemporaryDirectory() as temporary:
            db = sqlite3.connect(Path(temporary) / 'restore.db')
            ensure_upload_schema(db)
            ensure_upload_schema(db)
            columns = list(rows[0])
            db.execute('INSERT INTO assistent_uploads(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')', list(rows[0].values()))
            self.assertEqual(db.execute('SELECT file_sha256 FROM assistent_uploads').fetchone()[0], rows[0]['file_sha256'])
            db.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
