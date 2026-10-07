"""Private profile/payroll/time/restore tests on isolated synthetic employees."""
import base64
import copy
import concurrent.futures
from contextlib import closing
import io
import json
import re
from pathlib import Path
import sqlite3
from unittest import TestCase, main
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import fitz
from PIL import Image
from werkzeug.datastructures import FileStorage
import test_assistent as fixture
from werkstatt_mitarbeiter_portal import (
    register_employee_portal, PROFILE_FIELDS, TABLES, MAX_DOCUMENT_BYTES,
    ensure_employee_private_state_for_import, _document)

p, database = fixture.p, fixture.database
if 'employee_portal' not in p.app.blueprints:
    register_employee_portal(p)


def png():
    data = io.BytesIO()
    Image.new('RGB', (60, 80), 'white').save(data, 'PNG')
    return data.getvalue()


def pdf():
    with fitz.open() as document:
        document.new_page().insert_text((30, 30), 'Synthetic private payslip')
        return document.tobytes()


class EmployeePortalTests(TestCase):
    def setUp(self):
        self.f = fixture.AssistantTests('runTest'); self.f.setUp(); self.addCleanup(self.f.tearDown)
        self.service = p.employee_portal
        self.admin = self.f.make_client(admin=True)
        self.client = self.f.client
        with database() as db:
            for table in (*TABLES, 'mitarbeiter_zeitstempel', 'mitarbeiter_zeitstatus',
                          'mitarbeiter_urlaub', 'mitarbeiter_urlaubskonten', 'mitarbeiter_urlaubsantraege'):
                db.execute('DELETE FROM ' + table)
            db.execute("INSERT INTO mitarbeiter(id,name,aktiv,erstellt_am,geaendert_am) VALUES(2,'Other Person',1,?,?)",
                       (p.now_str(), p.now_str()))
            db.execute('INSERT INTO assistent_rechte(mitarbeiter_id,passwort_hash,lesen,dokumentieren,einkaufen,limit_cent,version,auth_version) '
                       "VALUES(2,'',1,0,1,25000,1,1)")
        self.renderer = patch('werkstatt_mitarbeiter_portal.render_template', return_value='private-page')
        self.render = self.renderer.start(); self.addCleanup(self.renderer.stop)

    @staticmethod
    def fields(**values):
        result = {key: '' for key in PROFILE_FIELDS}
        result.update(values)
        return result

    def profile(self, mid=1, **values):
        return self.admin.post(f'/admin/mitarbeiter/{mid}/portal',
                               data=dict(self.fields(**values), csrf_token='test-csrf'))

    def upload(self, mid=1, raw=None, filename='2026-10.pdf', period='2026-10', **kwargs):
        return self.admin.post(f'/admin/mitarbeiter/{mid}/portal/lohnzettel', data={
            'csrf_token': kwargs.get('csrf_token', 'test-csrf'), 'period': period,
            'file': (io.BytesIO(pdf() if raw is None else raw), filename)})

    def payroll_id(self, mid=1):
        with database() as db:
            return db.execute('SELECT id FROM mitarbeiter_lohnzettel WHERE mitarbeiter_id=?', (mid,)).fetchone()[0]

    def snapshot(self):
        with database() as db:
            return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)]
                               for table in (*TABLES, 'mitarbeiter', 'assistent_rechte')}}

    def test_profile_only_admin_writes_and_current_employee_reads(self):
        self.assertEqual(self.profile(personalnummer='P-001', steuer_id='12345678901',
                                      adresse='Teststraße 1\n74821 Testort').status_code, 303)
        self.assertEqual(self.client.get('/werkstatt/mein-konto?mitarbeiter_id=2&actor=admin').status_code, 200)
        data = self.render.call_args.kwargs
        self.assertEqual(data['employee'], {'id': 1, 'name': 'Testperson'})
        self.assertEqual(data['profile']['steuer_id'], '12345678901')
        self.assertNotIn('passwort_hash', data['who'])
        self.assertFalse(data['urlaub']['bekannt']); self.assertIsNone(data['urlaub']['resttage'])
        self.assertIsNone(data['arbeitszeit']['heute_stunden'])
        self.assertEqual(data['betriebsurlaub'], [])
        response = self.client.post('/admin/mitarbeiter/1/portal', data=dict(self.fields(), csrf_token='test-csrf'))
        self.assertEqual(response.status_code, 302)

    def test_old_contact_fallback_is_explicit_and_empty_saved_fields_clear_it(self):
        with database() as db:
            db.execute("UPDATE mitarbeiter SET adresse='Existing address',email='person@test.invalid',notiz='Internal note' WHERE id=1")
        self.client.get('/werkstatt/mein-konto')
        profile = self.render.call_args.kwargs['profile']
        self.assertEqual(profile['adresse'], 'Existing address')
        self.assertNotIn('notiz', profile)
        self.profile()
        self.client.get('/werkstatt/mein-konto')
        self.assertEqual(self.render.call_args.kwargs['profile']['adresse'], '')

    def test_invalid_private_fields_and_banking_keys_are_not_saved(self):
        for fields in ({'steuer_id':'123'}, {'email':'x\n@y.test'}, {'geburtsdatum':'2100-01-01'},
                       {'geburtsdatum':'2026-02-30'}, {'steuernummer':'text'}, {'telefon':'abc'},
                       {'adresse':'<script>x</script>'}, {'personalnummer':'a' * 41}):
            with self.subTest(fields=fields):
                self.assertEqual(self.profile(**fields).status_code, 400)
        self.assertEqual(self.admin.post('/admin/mitarbeiter/1/portal', data=dict(
            self.fields(), csrf_token='test-csrf', iban='should-not-be-accepted')).status_code, 400)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_portal_profile').fetchone()[0], 0)

    def test_csrf_profile_upload_and_holiday_cannot_mutate(self):
        self.assertEqual(self.admin.post('/admin/mitarbeiter/1/portal', data=self.fields()).status_code, 400)
        self.assertEqual(self.upload(csrf_token='wrong').status_code, 400)
        self.assertEqual(self.admin.post('/admin/mitarbeiter/betriebsurlaub', data={
            'start_datum':'2026-12-24','end_datum':'2027-01-02'}).status_code, 400)
        with database() as db:
            self.assertTrue(all(db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0] == 0 for table in TABLES))

    def test_valid_original_pdf_png_jpeg_download_as_private_attachment(self):
        jpg = io.BytesIO(); Image.new('RGB', (40, 30)).save(jpg, 'JPEG')
        for raw, name, mime in ((pdf(),'pay.pdf','application/pdf'), (png(),'pay.png','image/png'),
                                (jpg.getvalue(),'pay.jpeg','image/jpeg')):
            with self.subTest(name=name):
                self.assertEqual(self.upload(raw=raw, filename=name).status_code, 303)
                with database() as db:
                    row = db.execute('SELECT * FROM mitarbeiter_lohnzettel WHERE filename=?', (name,)).fetchone()
                response = self.client.get('/werkstatt/mein-konto/lohnzettel/' + str(row['id']))
                self.assertEqual(response.status_code, 200); self.assertEqual(response.data, raw)
                self.assertEqual(response.mimetype, mime)
                self.assertTrue(response.headers['Content-Disposition'].startswith('attachment;'))
                self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
                self.assertIn('no-store', response.headers['Cache-Control'])
                self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
                self.assertNotIn('ETag', response.headers)

    def test_no_foreign_document_or_admin_flag_on_personal_route(self):
        self.upload(mid=2)
        payroll_id = self.payroll_id(2)
        path = '/werkstatt/mein-konto/lohnzettel/' + str(payroll_id)
        self.assertEqual(self.client.get(path + '?mitarbeiter_id=2&admin=1').status_code, 404)
        self.assertEqual(self.admin.get(path).status_code, 404)
        self.assertEqual(p.app.test_client().get(path).status_code, 404)
        self.assertEqual(self.admin.get(f'/admin/mitarbeiter/2/portal/lohnzettel/{payroll_id}').status_code, 200)
        self.assertEqual(self.admin.get(f'/admin/mitarbeiter/1/portal/lohnzettel/{payroll_id}').status_code, 404)

    def test_changed_auth_rights_or_activity_revokes_private_access(self):
        self.upload(); path = '/werkstatt/mein-konto/lohnzettel/' + str(self.payroll_id())
        for query in ('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1',
                      'UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1',
                      'UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1',
                      'UPDATE mitarbeiter SET aktiv=0 WHERE id=1'):
            with self.subTest(query=query):
                with database() as db: db.execute(query)
                self.assertEqual(self.client.get(path).status_code, 404)
                self.assertEqual(self.client.get('/werkstatt/mein-konto').status_code, 302)
                with database() as db:
                    db.execute('UPDATE assistent_rechte SET auth_version=1,version=1,lesen=1 WHERE mitarbeiter_id=1')
                    db.execute('UPDATE mitarbeiter SET aktiv=1 WHERE id=1')

    def test_nonexistent_employee_id_is_404_for_admin_get_profile_and_upload(self):
        self.assertEqual(self.admin.get('/admin/mitarbeiter/999/portal').status_code,404)
        self.assertEqual(self.profile(mid=999).status_code,404)
        self.assertEqual(self.upload(mid=999).status_code,404)
        self.assertEqual(self.admin.get('/admin/mitarbeiter/2147483648/portal').status_code,404)

    def test_upload_duplicates_keep_one_row_and_no_ai_or_public_file(self):
        original = pdf()
        with patch.object(p, 'get_openai_api_key', side_effect=AssertionError('No AI')):
            self.assertEqual(self.upload(raw=original).status_code, 303)
            self.assertEqual(self.upload(raw=original, filename='othername.pdf').status_code, 303)
            self.client.get('/werkstatt/mein-konto')
        data = self.render.call_args.kwargs
        self.assertEqual(len(data['payrolls']), 1)
        self.assertNotIn('original_base64', data['payrolls'][0])
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_lohnzettel').fetchone()[0], 1)
            audit = [dict(row) for row in db.execute('SELECT * FROM assistent_audit')]
        self.assertNotIn(base64.b64encode(original).decode(), json.dumps(audit))

    def test_bad_type_signature_corruption_encrypted_pdf_and_period_are_rejected(self):
        encrypted = fitz.open(); encrypted.new_page()
        encoded = encrypted.tobytes(encryption=fitz.PDF_ENCRYPT_AES_256, user_pw='secret', owner_pw='owner')
        encrypted.close()
        for raw, name in ((b'<script>x</script>','pay.html'), (b'%PDF-not-real','pay.pdf'),
                          (png(),'pay.jpg'), (png(),'pay.pdf'), (pdf(),'pay.png'),
                          (encoded,'pay.pdf'), (b'\x89PNG\r\n\x1a\ninvalid','pay.png')):
            with self.subTest(name=name, signature=raw[:10]):
                self.assertEqual(self.upload(raw=raw,filename=name).status_code, 400)
        self.assertEqual(self.upload(period='2026-13').status_code, 400)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_lohnzettel').fetchone()[0], 0)

    def test_upload_ten_mb_cap_and_filename_cannot_be_header_or_path(self):
        with self.assertRaisesRegex(ValueError, '10 MB'):
            _document(FileStorage(io.BytesIO(b'x' * (MAX_DOCUMENT_BYTES + 1)), filename='x.pdf'))
        self.assertEqual(self.upload(filename='../../privateName.pdf').status_code, 303)
        self.assertEqual(self.upload(filename='private\x0d\x0aName.pdf').status_code, 400)
        with database() as db:
            name = db.execute('SELECT filename FROM mitarbeiter_lohnzettel').fetchone()[0]
        self.assertNotIn('/',name); self.assertNotIn('\r',name); self.assertNotIn('\n',name)

    def test_corrupt_stored_bytes_are_not_served(self):
        self.upload(); payroll_id = self.payroll_id()
        with database() as db:
            db.execute("UPDATE mitarbeiter_lohnzettel SET original_base64='changed' WHERE id=?", (payroll_id,))
        self.assertEqual(self.client.get(f'/werkstatt/mein-konto/lohnzettel/{payroll_id}').status_code, 404)

    def test_holiday_year_overlap_and_no_personal_leave_deduction(self):
        response = self.admin.post('/admin/mitarbeiter/betriebsurlaub', data={
            'csrf_token':'test-csrf','start_datum':'2026-12-24','end_datum':'2027-01-02','notiz':'Winterpause'})
        self.assertEqual(response.status_code, 303)
        self.assertEqual(len(self.service.company_holidays(2026)), 1)
        self.assertEqual(len(self.service.company_holidays(2027)), 1)
        self.assertEqual(self.service.company_holidays(2028), [])
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_urlaub').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_urlaubskonten').fetchone()[0], 0)
        self.assertEqual(self.client.get('/admin/mitarbeiter/betriebsurlaub').status_code, 302)

    def test_invalid_or_repeated_holiday(self):
        payload = {'csrf_token':'test-csrf','start_datum':'2026-12-24','end_datum':'2027-01-02','notiz':''}
        self.admin.post('/admin/mitarbeiter/betriebsurlaub', data=payload)
        self.admin.post('/admin/mitarbeiter/betriebsurlaub', data=payload)
        self.assertEqual(len(self.service.company_holidays()), 1)
        payload['end_datum'] = '2026-12-23'
        self.assertEqual(self.admin.post('/admin/mitarbeiter/betriebsurlaub', data=payload).status_code, 400)

    def time_form(self, client=None):
        with (client or self.client).get('/werkstatt/assistent/arbeitszeit') as response:
            self.assertEqual(response.status_code, 200)
        with p.app.test_request_context('/'):
            session = __import__('flask').session
            session.update(assistent_mid=1, assistent_version=1, assistent_auth_version=1)
            who = self.service.identity()
            nonce = self.service.new_time_form(who, 0)
            forms = dict(session['employee_time_forms'])
        with (client or self.client).session_transaction() as state:
            state['employee_time_forms'] = forms
        return nonce

    def stamp(self, nonce, action='kommen', revision='0', client=None, **extras):
        return (client or self.client).post('/werkstatt/mein-konto/zeit', data={
            'csrf_token':'test-csrf','aktion':action,'revision':revision,
            'request_id':nonce,'confirmed':'ja',**extras})

    def test_direct_time_stamp_server_clock_repeat_and_invalid_transition(self):
        nonce = self.time_form()
        self.assertEqual(self.stamp(nonce).status_code, 303)
        self.assertEqual(self.stamp(nonce).status_code, 303)
        self.assertEqual(self.stamp(nonce, action='pause').status_code, 303)
        with database() as db:
            rows = db.execute('SELECT * FROM mitarbeiter_zeitstempel').fetchall()
        self.assertEqual(len(rows), 1); self.assertEqual(rows[0]['mitarbeiter_id'], 1)
        self.assertEqual(rows[0]['aktion'], 'kommen'); self.assertEqual(rows[0]['revision'], 1)

    def test_old_form_account_switch_or_rights_revocation_cannot_stamp(self):
        nonce = self.time_form()
        with self.client.session_transaction() as state: state['assistent_mid'] = 2
        self.stamp(nonce)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0], 0)
        with self.client.session_transaction() as state: state['assistent_mid'] = 1
        with database() as db: db.execute('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1')
        self.stamp(nonce)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0], 0)

    def test_time_requires_nonce_csrf_current_revision_and_no_foreign_timestamp(self):
        nonce = self.time_form()
        for extra in ({'revision':'1'}, {'request_id':'unknown'}, {'mid':'2'}, {'timestamp':'2000-01-01'},
                      {'confirmed':'no'}, {'csrf_token':'wrong'}):
            with self.subTest(extra=extra): self.stamp(nonce, **extra)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0], 0)

    def test_parallel_time_clicks_have_one_real_stamp_and_no_ai_dependency(self):
        nonce = self.time_form()
        with self.client.session_transaction() as state:
            forms = dict(state['employee_time_forms'])
        def post():
            client = self.f.make_client()
            with client.session_transaction() as state: state['employee_time_forms'] = forms
            return self.stamp(nonce, client=client).status_code
        with patch.object(p, 'get_openai_api_key', side_effect=AssertionError('No AI')), \
             concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(lambda _: post(), range(2))), [303,303])
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0], 1)

    def test_second_stale_form_cannot_add_invalid_transition(self):
        old = self.time_form()
        with self.client.session_transaction() as state: forms = dict(state['employee_time_forms'])
        with p.app.test_request_context('/'):
            session = __import__('flask').session
            session.update(assistent_mid=1,assistent_version=1,employee_time_forms=forms)
            fresh = self.service.new_time_form(self.service.identity(),0)
            forms = dict(session['employee_time_forms'])
        with self.client.session_transaction() as state: state['employee_time_forms'] = forms
        self.stamp(fresh)
        self.stamp(old)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0],1)

    def test_natural_profile_insert_is_explicit_postgres_returning_key(self):
        class Cursor:
            description = [type('Column', (), {'name':'mitarbeiter_id'})()]
            rowcount = 1
            def __enter__(self): return self
            def __exit__(self,*_): return False
            def execute(self, query, params):
                self.query = query
                if 'RETURNING id' in query:
                    raise AssertionError('Private profile has mitarbeiter_id, not id')
            def fetchall(self): return [(1,)]
        cursor = Cursor()
        class Connection:
            def cursor(self): return cursor
        db = p.PostgresConnection(Connection())
        db.execute('INSERT INTO mitarbeiter_portal_profile(mitarbeiter_id,updated_at,updated_by) '
                   'VALUES(?,?,?) ON CONFLICT(mitarbeiter_id) DO UPDATE SET updated_at=excluded.updated_at '
                   'RETURNING mitarbeiter_id',(1,'now','admin')).fetchall()
        self.assertNotIn('?',cursor.query)
        self.assertIn('RETURNING mitarbeiter_id',cursor.query)

    def test_real_setup_profile_navigation_and_time_form_are_connected(self):
        self.renderer.stop()  # Full template/registration integration, no UI stub.
        self.assertEqual(self.profile(personalnummer='P-001').status_code,303)
        self.assertEqual(self.upload().status_code,303)
        with patch.dict(p.app.config,EMPLOYEE_INVITE_PUBLIC_BASE_URL='https://portal.test'):
            invite = p.employee_invitations.issue(1)
        token = parse_qs(urlsplit(invite['url']).fragment)['token'][0]
        client = p.app.test_client()
        with client.session_transaction() as state: state['csrf_token']='synthetic-setup'
        response = client.post('/werkstatt/zugang/einrichten',data={
            'token':token,'password':'personal-test-password-123',
            'password_confirm':'personal-test-password-123','csrf_token':'synthetic-setup'})
        self.assertEqual((response.status_code,response.location),(303,'/werkstatt/mein-konto'))
        response = client.get(response.location)
        self.assertEqual(response.status_code,200)
        self.assertIn('P-001',response.text)
        self.assertEqual(self.client.get('/werkstatt/mein-konto').status_code,302)
        response = client.get('/werkstatt/assistent/arbeitszeit')
        self.assertEqual(response.status_code,200)
        nonce = re.search(r'name="request_id" value="([^"]+)"',response.text).group(1)
        with client.session_transaction() as state: csrf=state['csrf_token']
        payload = {'csrf_token':csrf,'aktion':'kommen','revision':'0','request_id':nonce,'confirmed':'ja'}
        self.assertEqual(client.post('/werkstatt/mein-konto/zeit',data=payload).status_code,303)
        self.assertEqual(client.post('/werkstatt/mein-konto/zeit',data=payload).status_code,303)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0],1)
        for path in ('/werkstatt/assistent/arbeitszeit','/werkstatt/assistent/urlaub',
                     '/werkstatt/materialbestellung','/werkstatt/mein-konto'):
            with self.subTest(path=path): self.assertEqual(client.get(path).status_code,200)
        for path in ('/admin/mitarbeiter/1/portal','/admin/mitarbeiter/betriebsurlaub',
                     '/admin/mitarbeiter/einrichtung'):
            with self.subTest(path=path): self.assertEqual(self.admin.get(path).status_code,200)

    def test_empty_legacy_restore_allowed_but_profile_payroll_holidays_fully_preserved(self):
        ensure_employee_private_state_for_import(p, export={'tables':{}})
        self.profile(steuer_id='12345678901'); self.upload()
        self.admin.post('/admin/mitarbeiter/betriebsurlaub', data={'csrf_token':'test-csrf',
            'start_datum':'2026-12-24','end_datum':'2027-01-02','notiz':'Winterpause'})
        current = self.snapshot()
        ensure_employee_private_state_for_import(p, export=current)
        for table in (*TABLES,'mitarbeiter','assistent_rechte'):
            old = copy.deepcopy(current); old['tables'][table] = []
            with self.subTest(table=table), self.assertRaisesRegex(ValueError,'Datenimport gesperrt'):
                ensure_employee_private_state_for_import(p, export=old)
        for table in TABLES:
            for column in current['tables'][table][0]:
                old = copy.deepcopy(current); old['tables'][table][0][column] = 'changed'
                with self.subTest(table=table,column=column), self.assertRaises(ValueError):
                    ensure_employee_private_state_for_import(p, export=old)
        self.assertEqual(current,self.snapshot())

    def test_sqlite_source_authoritative_and_owner_reassignment_blocked(self):
        self.upload(); current = self.snapshot()
        source = Path(fixture.TEMP.name) / 'private-synthetic-source.db'
        if source.exists(): source.unlink()
        with database() as db:
            ddl = [row[0] for row in db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?,?)", (*TABLES,'mitarbeiter','assistent_rechte'))]
        with closing(sqlite3.connect(source)) as db, db:
            for sql in ddl: db.execute(sql)
            for table,rows in current['tables'].items():
                for row in rows:
                    db.execute('INSERT INTO ' + table + '(' + ','.join(row) + ') VALUES(' + ','.join('?' for _ in row) + ')', tuple(row.values()))
        ensure_employee_private_state_for_import(p, imported_db=source, export={'tables':{}})
        with closing(sqlite3.connect(source)) as db, db: db.execute('UPDATE mitarbeiter_lohnzettel SET mitarbeiter_id=2')
        with self.assertRaises(ValueError):
            ensure_employee_private_state_for_import(p, imported_db=source, export=current)
        source.unlink()

    def test_private_document_access_revocation_cannot_roll_back_with_restore(self):
        self.upload()
        previous = self.snapshot()
        with database() as db:
            db.execute('UPDATE assistent_rechte SET auth_version=2,lesen=0 WHERE mitarbeiter_id=1')
        with self.assertRaisesRegex(ValueError,'Datenimport gesperrt'):
            ensure_employee_private_state_for_import(p,export=previous)
        ensure_employee_private_state_for_import(p,export=self.snapshot())

    def test_private_profile_without_login_cannot_regain_old_login_via_restore(self):
        self.upload()
        previous = self.snapshot()
        with database() as db:
            db.execute('DELETE FROM assistent_rechte WHERE mitarbeiter_id=1')
        with self.assertRaisesRegex(ValueError,'Datenimport gesperrt'):
            ensure_employee_private_state_for_import(p,export=previous)
        for encoded_id in ('1','001','+1',1.0,True):
            old = copy.deepcopy(previous)
            old['tables']['assistent_rechte'][0]['mitarbeiter_id'] = encoded_id
            with self.subTest(encoded_id=encoded_id), self.assertRaises(ValueError):
                ensure_employee_private_state_for_import(p,export=old)
        ensure_employee_private_state_for_import(p,export=self.snapshot())

    def test_externalized_payroll_actual_bytes_must_match_and_borrowed_connection_stays_open(self):
        original = pdf(); self.upload(raw=original)
        current = self.snapshot(); row = current['tables']['mitarbeiter_lohnzettel'][0]
        row['original_base64'] = ''
        reference = {'table':'mitarbeiter_lohnzettel','row_id':row['id'],'column':'original_base64'}
        refs = {('mitarbeiter_lohnzettel',row['id'],'original_base64'):reference}
        with patch.object(p,'backup_binary_reference_map',return_value=refs), \
             patch.object(p,'read_backup_binary_blob',return_value=original):
            with database() as db:
                ensure_employee_private_state_for_import(p,export=current,target=db,archive=object(),names=[])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_lohnzettel').fetchone()[0],1)
            with self.assertRaises(ValueError): ensure_employee_private_state_for_import(p,export=current)
        with patch.object(p,'backup_binary_reference_map',return_value=refs), \
             patch.object(p,'read_backup_binary_blob',return_value=b'not original'):
            with self.assertRaises(ValueError):
                ensure_employee_private_state_for_import(p,export=current,archive=object(),names=[])


if __name__ == '__main__':
    main()
