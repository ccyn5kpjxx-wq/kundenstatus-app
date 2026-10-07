"""Personal order search, scoped writes, private originals and restore: offline."""
import base64
import copy
import concurrent.futures
from contextlib import closing
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import zipfile
from unittest import TestCase, main
from unittest.mock import patch

import fitz
from PIL import Image
from werkzeug.datastructures import FileStorage
import test_assistent as fixture
from werkstatt_mitarbeiter_auftraege import register_employee_orders, ensure_employee_orders_for_import, _pdf_work_copy, _photo

p, database = fixture.p, fixture.database
if 'employee_orders' not in p.app.blueprints:
    register_employee_orders(p)


def png():
    out = io.BytesIO(); Image.new('RGB', (80, 60), '#4087bb').save(out, 'PNG'); return out.getvalue()


def pdf(*lines, scan=False):
    with fitz.open() as document:
        page = document.new_page()
        if scan:
            page.insert_image(fitz.Rect(20, 20, 200, 120), stream=png())
        for n, text in enumerate(lines):
            page.insert_text((40, 50 + n * 25), text)
        return document.tobytes()


class EmployeeOrderTests(TestCase):
    def setUp(self):
        self.fixture = fixture.AssistantTests('runTest'); self.fixture.setUp(); self.addCleanup(self.fixture.tearDown)
        self.client = self.fixture.client
        self.admin = self.fixture.make_client(admin=True)
        self.service = p.employee_orders
        self.mode = patch.dict(p.app.config, EMPLOYEE_ORDER_OPERATIONS_ENABLED=True)
        self.mode.start(); self.addCleanup(self.mode.stop)
        self.renderer = patch('werkstatt_mitarbeiter_auftraege.render_template', return_value='personal-order-page')
        self.render = self.renderer.start(); self.addCleanup(self.renderer.stop)
        self.external = patch.object(p, 'fuehre_auftrag_status_wechsel_aus', side_effect=AssertionError('No mail helper'))
        self.external.start(); self.addCleanup(self.external.stop)
        with database() as db:
            db.execute('DELETE FROM assistent_fortschritt_audit')
            db.execute('DELETE FROM datei_backups')
            db.execute('DELETE FROM status_log')
            db.execute('UPDATE assistent_rechte SET dokumentieren=0,auth_version=1 WHERE mitarbeiter_id=1')
            db.execute('''INSERT INTO auftraege(id,fahrzeug,kennzeichen,beschreibung,analyse_text,status,produktion_schritt,
                annahme_datum,annahme_uhrzeit,fertig_datum,fertig_uhrzeit,abholtermin,abhol_uhrzeit,transport_art,
                farbcode,farbton,auftragsnummer,erstellt_am,geaendert_am) VALUES(102,'Audi Test','TEST-102',
                'Stoßfänger vorne lackieren','Arbeitsangabe am Original prüfen',2,'',
                '10.10.2026','09:30','12.10.2026','12:00','12.10.2026','16:00','hol_und_bring',
                'LY7W','Silber','CUSTOMER-555',?,?)''', (p.now_str(), p.now_str()))

    def page(self, number='102', client=None):
        response = (client or self.client).get('/werkstatt/mein-konto/auftraege?nummer=' + number)
        self.assertEqual(response.status_code, 200)
        return self.render.call_args.kwargs

    def form(self, action='in_arbeit_starten'):
        data = self.page()
        item = next(item for item in data['action_forms'] if item['aktion'] == action)
        return dict(aktion=action, request_id=item['request_id'], confirmed='ja', csrf_token='test-csrf')

    def status(self, form, oid=102, client=None):
        return (client or self.client).post(f'/werkstatt/mein-konto/auftraege/{oid}/status', data=form)

    def photo(self, token=None, raw=None, filename='schaden.png', oid=102, **extra):
        token = token or self.page()['photo_request_id']
        return self.client.post(f'/werkstatt/mein-konto/auftraege/{oid}/fotos', data=dict(
            csrf_token='test-csrf', request_id=token, confirmed='ja', fotos=(io.BytesIO(raw or png()), filename), **extra))

    def state(self):
        with database() as db:
            return dict(db.execute('SELECT * FROM auftraege WHERE id=102').fetchone())

    def audit_count(self):
        with database() as db:
            return db.execute('SELECT COUNT(*) FROM assistent_fortschritt_audit').fetchone()[0]

    def store(self, raw, *, oid=102, name='Arbeitsauftrag.pdf', mime='application/pdf', category='standard', dtype='Reparaturauftrag', **fields):
        with database() as db:
            cursor = db.execute('''INSERT INTO dateien(auftrag_id,original_name,stored_name,mime_type,size,quelle,kategorie,
                dokument_typ,hochgeladen_am,sichtbarkeit_geprueft) VALUES(?,?,?, ?,?,'intern',?,?,?,1)''',
                (oid, name, 'synthetic-' + hashlib.sha256(raw).hexdigest()[:20] + '.bin', mime, len(raw), category, dtype, p.now_str()))
            did = cursor.lastrowid
            for key, value in fields.items():
                db.execute(f'UPDATE dateien SET {key}=? WHERE id=?', (value, did))
            db.execute('INSERT INTO datei_backups(datei_id,file_base64,file_sha256,size,erstellt_am) VALUES(?,?,?,?,?)',
                       (did, base64.b64encode(raw).decode('ascii'), hashlib.sha256(raw).hexdigest(), len(raw), p.now_str()))
            return did

    @staticmethod
    def file_url(did, oid=102):
        return f'/werkstatt/mein-konto/auftraege/{oid}/datei/{did}'

    def snapshot(self):
        with database() as db:
            return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)]
                               for table in ('assistent_fortschritt_audit', 'auftraege', 'dateien', 'datei_backups', 'mitarbeiter', 'assistent_rechte')}}

    def test_exact_number_returns_only_requested_order_without_finance_or_hydration(self):
        before = self.state()
        with patch.object(p, 'get_auftrag', side_effect=AssertionError('No hydration')):
            data = self.page()
        order = data['order']
        self.assertEqual(order['nummer'], 102)
        self.assertEqual(order['externe_referenz'], 'CUSTOMER-555')
        self.assertEqual(order['arbeit'], 'Stoßfänger vorne lackieren')
        self.assertEqual(order['farbcode'], 'LY7W')
        self.assertEqual(order['termine'][0], {'label':'Abholung durch uns','datum':'10.10.2026','uhrzeit':'09:30'})
        self.assertEqual(order['fertig']['uhrzeit'], '12:00')
        self.assertEqual(order['termine'][2]['uhrzeit'], '16:00')
        self.assertNotIn('preis_netto', order); self.assertNotIn('fin_nummer', order)
        self.assertEqual(before, self.state())
        self.assertEqual(data['employee'], {'id':1,'name':'Testperson'})

    def test_empty_search_never_lists_all_orders_and_external_reference_not_number(self):
        data = self.page('')
        self.assertIsNone(data['order']); self.assertEqual(data['documents'], [])
        for value in ('CUSTOMER-555', '102 OR 1=1', '-1', '0102', '102,156'):
            self.assertEqual(self.client.get('/werkstatt/mein-konto/auftraege?nummer=' + value).status_code, 400)
        self.assertEqual(self.client.get('/werkstatt/mein-konto/auftraege?nummer=999').status_code, 404)

    def test_personal_identity_required_and_shared_or_admin_cookie_does_not_grant_it(self):
        client = p.app.test_client()
        with client.session_transaction() as s:
            s.update(admin=True, werkstatt_tafel=True)
        self.assertEqual(client.get('/werkstatt/mein-konto/auftraege?nummer=102').status_code, 302)
        self.assertEqual(self.page()['employee']['id'], 1)
        with self.client.session_transaction() as s:
            self.assertFalse(s.get('werkstatt_tafel'))

    def test_revoked_auth_rights_activity_and_gate_deny_writes(self):
        form = self.form()
        for statement in ('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1',
                          'UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1',
                          'UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1',
                          'UPDATE mitarbeiter SET aktiv=0 WHERE id=1'):
            with self.subTest(statement=statement), database() as db:
                db.execute(statement)
            self.assertEqual(self.status(form).status_code, 403)
            with database() as db:
                db.execute('UPDATE assistent_rechte SET auth_version=1,version=1,lesen=1 WHERE mitarbeiter_id=1')
                db.execute('UPDATE mitarbeiter SET aktiv=1 WHERE id=1')
        with patch.dict(p.app.config, EMPLOYEE_ORDER_OPERATIONS_ENABLED=False):
            self.assertEqual(self.page()['action_forms'], [])
            self.assertFalse(self.page()['photo_request_id'])
            self.assertEqual(self.status(form).status_code, 403)
        self.assertEqual(self.state()['status'], 2); self.assertEqual(self.audit_count(), 0)

    def test_status_workflow_has_durable_replay_no_toggle_no_mail_and_no_broad_rights(self):
        first = self.form(); self.assertEqual(self.status(first).status_code, 303)
        self.assertEqual(self.status(first).status_code, 303)
        self.assertEqual(self.audit_count(), 1)
        for action, stage in (('vorarbeit_starten','vorarbeit'), ('karosserie_starten','karosserie'),
                              ('lackierung_starten','lackierung'), ('finish_starten','finish')):
            form = self.form(action)
            self.assertEqual(self.status(form).status_code, 303)
            self.assertEqual(self.status(form).status_code, 303)
            self.assertEqual(self.state()['produktion_schritt'], stage)
            self.assertEqual(self.state()['status'], 3)
        self.assertEqual(self.status(self.form('fertig_melden')).status_code, 303)
        self.assertEqual(self.state()['status'], 4)
        self.assertEqual(self.page()['action_forms'], [])
        with database() as db:
            self.assertEqual(db.execute('SELECT dokumentieren FROM assistent_rechte WHERE mitarbeiter_id=1').fetchone()[0], 0)
            self.assertEqual([row[0] for row in db.execute('SELECT status FROM status_log ORDER BY id')], [3,4])

    def test_no_archive_unreleased_insurance_or_office_status_writes(self):
        form = self.form()
        for change in ('archiviert=1', 'versicherung_id=1,versicherung_freigabe_status=\'offen\',schaden_eigenauftrag=0', 'status=5'):
            with database() as db: db.execute('UPDATE auftraege SET ' + change + ' WHERE id=102')
            self.assertEqual(self.client.get('/werkstatt/mein-konto/auftraege?nummer=102').status_code, 404)
            self.assertEqual(self.status(form).status_code, 404)
            with database() as db: db.execute("UPDATE auftraege SET archiviert=0,status=2,versicherung_id=NULL,versicherung_freigabe_status='' WHERE id=102")
        with database() as db: db.execute('UPDATE auftraege SET status=1 WHERE id=102')
        self.assertEqual(self.page()['action_forms'], [])
        self.assertFalse(self.page()['photo_request_id'])
        self.assertEqual(self.audit_count(), 0)

    def test_stale_same_minute_stage_and_page_race_never_confirm_unseen_state(self):
        form = self.form()
        with database() as db: db.execute("UPDATE auftraege SET produktion_schritt='karosserie' WHERE id=102")
        self.status(form)
        self.assertEqual(self.state()['status'], 2); self.assertEqual(self.audit_count(), 0)
        original = self.service.progress.preview
        def mutate(*args, **kwargs):
            with database() as db: db.execute("UPDATE auftraege SET geaendert_am='new-state' WHERE id=102")
            return original(*args, **kwargs)
        with patch.object(self.service.progress, 'preview', side_effect=mutate):
            data = self.page()
        self.assertFalse(data['action_forms']); self.assertFalse(data['photo_request_id'])
        self.assertIn('geändert', data['error'])

    def test_nonce_cannot_change_actor_order_payload_or_auth_version(self):
        form = self.form()
        self.status(dict(form, expected_status='2'))
        self.status(dict(form, actor='admin'))
        self.status(dict(form, confirmed='nein'))
        self.status(dict(form, request_id='not-a-server-form'))
        self.status(form, oid=156)
        with self.client.session_transaction() as s:
            s['assistent_auth_version'] = 2
        self.assertEqual(self.status(form).status_code, 403)
        self.assertEqual(self.audit_count(), 0)

    def test_csrf_prevents_status_and_photos_without_effect(self):
        form = self.form(); del form['csrf_token']
        self.assertEqual(self.status(form).status_code, 400)
        self.assertEqual(self.client.post('/werkstatt/mein-konto/auftraege/102/fotos', data={
            'fotos': (io.BytesIO(png()), 'schaden.png')}).status_code, 400)
        self.assertEqual(self.audit_count(), 0)

    def test_concurrent_same_status_form_exactly_one_audit(self):
        form = self.form()
        with self.client.session_transaction() as session:
            saved = dict(session)
        def submit(_):
            client = p.app.test_client()
            with client.session_transaction() as session: session.update(saved)
            return self.status(form, client=client).status_code
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(submit, range(2))), [303,303])
        self.assertEqual(self.audit_count(), 1); self.assertEqual(self.state()['status'], 3)

    def test_internal_photo_replay_db_backup_and_admin_original_without_public_release(self):
        token = self.page()['photo_request_id']
        self.assertEqual(self.photo(token).status_code, 303)
        self.assertEqual(self.photo(token).status_code, 303)
        data = self.page()
        self.assertEqual(len(data['photos']), 1)
        did = data['photos'][0]['id']
        response = self.client.get(self.file_url(did))
        self.assertEqual(response.status_code, 200); self.assertEqual(response.mimetype, 'image/jpeg')
        self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
        self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertNotIn('ETag', response.headers)
        with database() as db:
            row = dict(db.execute('SELECT * FROM dateien WHERE id=?', (did,)).fetchone())
            self.assertEqual((row['kunde_sichtbar'],row['partner_sichtbar'],row['versicherung_sichtbar']), (0,0,0))
            self.assertEqual(row['kategorie'], 'assistent')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM dateien').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM datei_backups').fetchone()[0], 1)
        with self.admin.get('/admin/datei/' + str(did)) as admin_response:
            self.assertEqual(admin_response.status_code, 200)
        self.assertTrue(p.upload_file_path(row).exists())
        # A missing persistent disk original still opens via the DB backup.
        p.upload_file_path(row).unlink()
        with self.admin.get('/admin/datei/' + str(did)) as admin_response:
            self.assertEqual(admin_response.status_code, 200)
        self.assertEqual(self.client.get(self.file_url(did, 156)).status_code, 404)
        self.assertEqual(p.app.test_client().get(self.file_url(did)).status_code, 404)

    def test_invalid_photo_payload_never_partial_batch_and_different_retry_rejected(self):
        token = self.page()['photo_request_id']
        self.photo(token, raw=b'<script>x</script>', filename='schaden.png')
        self.photo(token, filename='Rechnung.png')
        self.photo(token, filename='schaden.pdf')
        self.photo(token, filename='schaden.jpg')
        self.assertEqual(self.audit_count(), 0)
        self.photo(token)
        other = io.BytesIO(); Image.new('RGB', (80,60), 'red').save(other,'PNG')
        self.photo(token, raw=other.getvalue())
        with database() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM dateien').fetchone()[0], 1)

    def test_pdf_bank_and_amount_redaction_preserves_work_and_original_bytes(self):
        raw = pdf('Reparaturauftrag Audi: Stossfaenger lackieren', 'Farbcode LY7W',
                  'IBAN DE02120300000000202051', 'Kosten: 123.45 EUR')
        did = self.store(raw)
        response = self.client.get(self.file_url(did))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['X-Workshop-Work-Copy'], '1')
        with fitz.open(stream=response.data, filetype='pdf') as document:
            text = ''.join(page.get_text() for page in document)
            self.assertIn('Stossfaenger lackieren', text); self.assertIn('LY7W', text)
            self.assertNotIn('IBAN', text); self.assertNotIn('123.45', text)
            self.assertNotIn('DE021203', text)
        row = p.get_datei(did)
        self.assertEqual(p.load_datei_backup_bytes(row), raw)
        self.assertTrue(self.page()['documents'][0]['arbeitskopie'])

    def test_pdf_scans_hidden_images_active_content_and_private_documents_are_blocked(self):
        for raw in (pdf('Arbeitsauftrag: Lackieren', scan=True), pdf(scan=True)):
            did = self.store(raw)
            self.assertEqual(self.client.get(self.file_url(did)).status_code, 404)
        with fitz.open(stream=pdf('Arbeitsauftrag'), filetype='pdf') as document:
            document[0].add_text_annot((30, 50), 'IBAN DE02120300000000202051')
            did = self.store(document.tobytes())
            self.assertEqual(self.client.get(self.file_url(did)).status_code, 404)
        for raw, name in ((pdf('Rechnung privat'), 'neutral.pdf'), (pdf('Lohnzettel geheim'), 'neutral.pdf'),
                          (pdf('Arbeitsauftrag'), 'kontoauszug.pdf')):
            did = self.store(raw, name=name)
            self.assertEqual(self.client.get(self.file_url(did)).status_code, 404)
        serialized = json.dumps(self.page(), default=str)
        self.assertNotIn('Lohnzettel', serialized); self.assertNotIn('kontoauszug', serialized)
        self.assertTrue(any(item['gesperrt'] for item in self.page()['documents']))

    def test_existing_scanned_document_and_financial_image_metadata_are_not_work_photos(self):
        for category, dtype, fields in (('standard','Reparaturauftrag',{}), ('assistent','Assistent-Unterlage',{}),
                                        ('fertigbild','Arbeitsfoto',{'extrahierter_text':'Rechnung Nr 123'}),
                                        ('fertigbild','Arbeitsfoto',{'extrakt_kurz':'IBAN DE02120300000000202051'})):
            did = self.store(png(), mime='image/png', name='bild.png', category=category, dtype=dtype, **fields)
            self.assertEqual(self.client.get(self.file_url(did)).status_code, 404)

    def test_work_description_bank_and_contact_lines_are_removed(self):
        with database() as db:
            db.execute("UPDATE auftraege SET beschreibung='Stossfaenger lackieren\nIBAN DE02120300000000202051\nPreis: 500 EUR\nTelefon: 0123456789' WHERE id=102")
        text = self.page()['order']['arbeit']
        self.assertIn('lackieren', text)
        self.assertNotIn('DE021203', text); self.assertNotIn('0123456789', text); self.assertNotIn('500', text)

    def test_restore_current_snapshot_ok_but_old_audit_photo_or_visibility_missing_denied(self):
        self.status(self.form()); self.photo()
        snapshot = self.snapshot()
        ensure_employee_orders_for_import(p, export=snapshot)
        for table in ('assistent_fortschritt_audit', 'auftraege', 'dateien', 'datei_backups'):
            changed = copy.deepcopy(snapshot); changed['tables'][table] = []
            with self.subTest(table=table), self.assertRaises(ValueError):
                ensure_employee_orders_for_import(p, export=changed)
        changed = copy.deepcopy(snapshot); changed['tables']['dateien'][0]['kunde_sichtbar'] = 1
        with self.assertRaises(ValueError): ensure_employee_orders_for_import(p, export=changed)
        changed = copy.deepcopy(snapshot)
        next(row for row in changed['tables']['auftraege'] if row['id'] == 102)['status'] = 2
        with self.assertRaises(ValueError): ensure_employee_orders_for_import(p, export=changed)

    def test_restore_sqlite_checks_actual_db_not_contradictory_json(self):
        self.photo()
        snapshot = self.snapshot()
        path = Path(fixture.TEMP.name) / 'order-restore-copy.db'
        with database() as db, closing(sqlite3.connect(path)) as copydb:
            db.backup(copydb)
        ensure_employee_orders_for_import(p, imported_db=path, export={'tables':{}})
        with closing(sqlite3.connect(path)) as copydb:
            copydb.execute('DELETE FROM assistent_fortschritt_audit'); copydb.commit()
        with self.assertRaises(ValueError): ensure_employee_orders_for_import(p, imported_db=path, export=snapshot)

    def test_pdf_metadata_links_removed_and_modified_backup_hash_not_served(self):
        with fitz.open(stream=pdf('Arbeitsauftrag Farbcode LY7W'), filetype='pdf') as document:
            document.set_metadata({'author':'private', 'subject':'Bank data'})
            document[0].insert_link({'kind':fitz.LINK_URI,'from':fitz.Rect(20,20,40,40),'uri':'https://example.invalid'})
            raw = document.tobytes()
        did = self.store(raw)
        response = self.client.get(self.file_url(did))
        self.assertEqual(response.status_code,200)
        with fitz.open(stream=response.data, filetype='pdf') as document:
            self.assertFalse(document.metadata['author']); self.assertFalse(document[0].get_links())
        with database() as db: db.execute("UPDATE datei_backups SET file_sha256='bad' WHERE datei_id=?", (did,))
        self.assertEqual(self.client.get(self.file_url(did)).status_code,404)

    def test_pdf_outlined_bank_footer_not_copied_as_vectors(self):
        with fitz.open(stream=pdf('Reparaturauftrag: Stossfaenger lackieren', 'Farbcode LY7W'), filetype='pdf') as source:
            page = source[0]
            # Searchable text is legitimate; unsearchable vector graphics may
            # spell an account number. Copying the original page is unsafe.
            shape = page.new_shape()
            for offset in range(25):
                x = 40 + offset * 8
                shape.draw_rect(fitz.Rect(x, 740, x + 5, 752))
            shape.finish(color=(0,0,0)); shape.commit()
            raw = source.tobytes()
        self.assertTrue(fitz.open(stream=raw,filetype='pdf')[0].get_drawings())
        clean, changed = _pdf_work_copy(raw)
        self.assertTrue(changed)
        with fitz.open(stream=clean,filetype='pdf') as document:
            self.assertFalse(document[0].get_drawings())
            self.assertIn('Farbcode LY7W', document[0].get_text())
            self.assertIn('Bereinigte Arbeitskopie', document[0].get_text())

    def test_enterprise_financial_free_text_is_removed_without_currency_symbol(self):
        with database() as db:
            db.execute("UPDATE auftraege SET beschreibung='Stossfaenger lackieren\nGesamtbetrag: 1234,50\nGewinn: 456,70\nSaldo: 9876,54' WHERE id=102")
        text = self.page()['order']['arbeit']
        self.assertEqual(text, 'Stossfaenger lackieren')

    def test_restore_actor_login_cannot_roll_back_without_hr_or_invitation_rows(self):
        self.status(self.form())
        snapshot = self.snapshot()
        # No profile/invitation footprint: this guard owns the new audit's binding.
        for table, column, value in (('mitarbeiter','name','Different person'), ('mitarbeiter','aktiv',0),
                                     ('assistent_rechte','passwort_hash','old-hash'), ('assistent_rechte','auth_version',0),
                                     ('assistent_rechte','version',0), ('assistent_rechte','dokumentieren',1)):
            changed = copy.deepcopy(snapshot)
            changed['tables'][table][0][column] = value
            with self.subTest(table=table,column=column), self.assertRaises(ValueError):
                ensure_employee_orders_for_import(p, export=changed)
        with database() as db: db.execute("UPDATE assistent_fortschritt_audit SET actor='admin' WHERE order_id=102")
        with self.assertRaises(ValueError): ensure_employee_orders_for_import(p, export=self.snapshot())

    def test_restore_missing_rights_cannot_be_recreated_by_integer_string_ids(self):
        self.photo()
        snapshot = self.snapshot()
        with database() as db: db.execute('DELETE FROM assistent_rechte WHERE mitarbeiter_id=1')
        for value in (1,'1','001','+1',True,1.0):
            changed = copy.deepcopy(snapshot); changed['tables']['assistent_rechte'][0]['mitarbeiter_id'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                ensure_employee_orders_for_import(p, export=changed)
        ensure_employee_orders_for_import(p, export=self.snapshot())

    def test_normal_zip_without_datei_backups_requires_exact_original_member(self):
        self.photo()
        snapshot = self.snapshot()
        backup = snapshot['tables'].pop('datei_backups')[0]
        photo = snapshot['tables']['dateien'][0]
        member = 'uploads/' + photo['stored_name']
        for raw, name, accepted in ((base64.b64decode(backup['file_base64']), member, True),
                                    (b'incorrect pixels', member, False),
                                    (base64.b64decode(backup['file_base64']), 'uploads/unrelated.jpg', False)):
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w') as archive: archive.writestr(name, raw)
            with zipfile.ZipFile(io.BytesIO(buffer.getvalue())) as archive:
                if accepted:
                    ensure_employee_orders_for_import(p, export=snapshot, archive=archive, names=archive.namelist())
                else:
                    with self.assertRaises(ValueError):
                        ensure_employee_orders_for_import(p, export=snapshot, archive=archive, names=archive.namelist())

    def test_long_work_text_keeps_final_repair_instruction(self):
        work = ('Weitere Arbeit kontrollieren.\n' * 180) + 'Als Letztes Zierleiste links ersetzen.'
        with database() as db: db.execute('UPDATE auftraege SET beschreibung=?,analyse_text=? WHERE id=102', (work,work))
        order = self.page()['order']
        self.assertTrue(order['arbeit'].endswith('Zierleiste links ersetzen.'))
        self.assertTrue(order['analyse_text'].endswith('Zierleiste links ersetzen.'))
        self.assertFalse(order['text_gekuerzt'])

    def test_auth_gate_and_form_checked_before_photo_pixel_decode(self):
        token = self.page()['photo_request_id']
        with patch('werkstatt_mitarbeiter_auftraege._photo', side_effect=AssertionError('No unauthorized image decode')):
            with patch.dict(p.app.config, EMPLOYEE_ORDER_OPERATIONS_ENABLED=False):
                self.assertEqual(self.photo(token).status_code, 403)
            self.assertEqual(self.photo('client-forged-token').status_code, 303)
            with self.client.session_transaction() as session:
                session.pop('assistent_mid')
            self.assertEqual(self.photo(token).status_code, 403)

    def test_old_restore_missing_audit_table_reinitialized_without_route_registration(self):
        self.addCleanup(p.workshop_progress_init_schema)
        with database() as db:
            db.execute('DROP TABLE assistent_fortschritt_audit')
        p.init_db()
        # admin_daten_import executes this registered hook after base init_db.
        p.workshop_progress_init_schema()
        with database() as db:
            self.assertIn('id', p.get_table_columns(db, 'assistent_fortschritt_audit'))
        self.assertEqual(self.status(self.form()).status_code, 303)
        self.assertEqual(self.photo().status_code, 303)
        self.assertEqual(self.audit_count(), 2)

    def test_failed_photo_db_backup_cleans_only_new_paths_not_existing_originals(self):
        token = self.page()['photo_request_id']
        with patch.object(p, 'store_datei_backup', return_value=False):
            self.photo(token)
        target = p.UPLOAD_DIR / ('employee-order-' + token + '-0.jpg')
        self.assertFalse(target.exists()); self.assertEqual(self.audit_count(), 0)
        self.photo(token)
        self.assertTrue(target.exists())
        before = target.read_bytes()
        with patch.object(p, 'store_datei_backup', side_effect=AssertionError('Replay does not touch originals')):
            self.photo(token)
        self.assertEqual(target.read_bytes(), before)


if __name__ == '__main__':
    main()
