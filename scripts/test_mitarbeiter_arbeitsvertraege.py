"""Private contract originals: synthetic owners/files, isolated storage, no AI/network."""
import atexit
import base64
import copy
import concurrent.futures
from contextlib import closing
import io
import json
import os
from pathlib import Path
import sqlite3
import struct
import tempfile
import warnings
import zipfile
from unittest import TestCase, main
from unittest.mock import patch

import fitz
from PIL import Image
from werkzeug.datastructures import MultiDict

ISOLATED = tempfile.TemporaryDirectory(prefix='employee-contract-test-')
atexit.register(ISOLATED.cleanup)
os.environ.update(BACKUP_DIR=str(Path(ISOLATED.name) / 'backups'),
                  DELETED_UPLOAD_DIR=str(Path(ISOLATED.name) / 'deleted_uploads'),
                  AUTO_BACKUP_ENABLED='0', AUTO_CHANGE_BACKUP_ENABLED='0', AUTO_BACKUP_ON_STARTUP='0')

import test_mitarbeiter_portal as fixture
atexit.register(fixture.fixture.TEMP.cleanup)
from werkstatt_mitarbeiter_portal import DOCX_MIME, MAX_DOCUMENT_BYTES, TABLES, ensure_employee_private_state_for_import

p, database = fixture.p, fixture.database
CONTRACTS = 'mitarbeiter_arbeitsvertraege'


def contract_pdf(text='Synthetic employee contract'):
    with fitz.open() as document:
        document.new_page().insert_text((40, 40), text)
        return document.tobytes()


def contract_docx(*, content_type=None, document=None, extra=()):
    ns = 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'
    entries = [('[Content_Types].xml', '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Override PartName="/word/document.xml" ContentType="' + (content_type or DOCX_MIME + '.main+xml')
                + '"/></Types>'),
               ('_rels/.rels', '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
                'Target="word/document.xml"/></Relationships>'),
               ('word/document.xml', document if document is not None else '<w:document xmlns:w="' + ns
                + '"><w:body><w:p><w:r><w:t>Synthetic archived draft</w:t></w:r></w:p></w:body></w:document>')]
    stream = io.BytesIO()
    with warnings.catch_warnings(), zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as archive:
        warnings.simplefilter('ignore', UserWarning)  # Deliberate duplicate ZIP-member regression.
        for name, data in (*entries, *extra): archive.writestr(name, data)
    return stream.getvalue()


class ContractTests(TestCase):
    def setUp(self):
        self.base = fixture.EmployeePortalTests('runTest')
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.admin, self.client, self.service = self.base.admin, self.base.client, self.base.service

    def upload(self, *, mid=1, raw=None, filename='contract.pdf', titel='', client=None, **extra):
        payload = {'csrf_token': 'test-csrf', 'titel': titel,
                   'file': (io.BytesIO(raw if raw is not None else contract_pdf()), filename)}
        payload.update(extra)
        response = (client or self.admin).post(f'/admin/mitarbeiter/{mid}/portal/arbeitsvertrag',
                                               data=payload, content_type='multipart/form-data')
        # Werkzeug may spool large synthetic multipart bodies to a separate temp file.
        response.request.environ['wsgi.input'].close()
        return response

    def rows(self):
        with database() as db:
            return [dict(row) for row in db.execute('SELECT * FROM ' + CONTRACTS + ' ORDER BY id')]

    def path(self, row):
        return '/werkstatt/mein-konto/arbeitsvertrag/' + str(row['id'])

    def test_own_original_pdf_and_safe_metadata(self):
        original = contract_pdf()
        self.assertEqual(self.upload(raw=original, titel='Arbeitsvertrag').status_code, 303)
        row = self.rows()[0]
        response = self.client.get(self.path(row))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, original)
        self.assertTrue(response.headers['Content-Disposition'].startswith('attachment;'))
        self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
        self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
        self.client.get('/werkstatt/mein-konto')
        metadata = self.base.render.call_args.kwargs['contracts']
        self.assertEqual(metadata[0]['titel'], 'Arbeitsvertrag')
        self.assertNotIn('original_base64', metadata[0])
        self.assertNotIn('sha256', metadata[0])
        self.assertNotIn('period', row)

    def test_foreign_guessed_url_query_and_admin_owner_mismatch(self):
        self.upload(mid=2)
        row = self.rows()[0]
        path = self.path(row)
        for client in (self.client, self.admin, p.app.test_client()):
            self.assertEqual(client.get(path + '?mitarbeiter_id=2&admin=1').status_code, 404)
        admin_path = f'/admin/mitarbeiter/2/portal/arbeitsvertrag/{row["id"]}'
        self.assertEqual(self.admin.get(admin_path).status_code, 200)
        self.assertNotEqual(self.client.get(admin_path).status_code, 200)
        self.assertEqual(self.admin.get(admin_path.replace('/2/portal/', '/1/portal/')).status_code, 404)

    def test_auth_version_rights_activity_and_missing_login_revoke_access(self):
        self.upload(); path = self.path(self.rows()[0])
        for query in ('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1',
                      'UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1',
                      'UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1',
                      'UPDATE mitarbeiter SET aktiv=0 WHERE id=1'):
            with self.subTest(query=query):
                with database() as db: db.execute(query)
                self.assertEqual(self.client.get(path).status_code, 404)
                with database() as db:
                    db.execute('UPDATE assistent_rechte SET auth_version=1,version=1,lesen=1 WHERE mitarbeiter_id=1')
                    db.execute('UPDATE mitarbeiter SET aktiv=1 WHERE id=1')
        with database() as db: db.execute('DELETE FROM assistent_rechte WHERE mitarbeiter_id=1')
        self.assertEqual(self.client.get(path).status_code, 404)

    def test_only_admin_with_csrf_can_upload(self):
        for response in (self.upload(client=self.client), self.upload(client=p.app.test_client()),
                         self.upload(csrf_token='wrong'), self.upload(csrf_token='')):
            self.assertNotEqual(response.status_code, 303)
        self.assertEqual(self.rows(), [])

    def test_duplicate_original_keeps_first_name_and_addendum_stays_separate(self):
        original = contract_pdf()
        self.upload(raw=original, titel='Vertrag')
        self.upload(raw=original, filename='other.pdf', titel='Anderer Name')
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.rows()[0]['titel'], 'Vertrag')
        self.upload(raw=contract_pdf('Synthetic later addendum'), titel='Nachtrag')
        self.assertEqual(len(self.rows()), 2)
        self.assertEqual(self.rows()[1]['titel'], 'Nachtrag')

    def test_simultaneous_duplicate_upload_has_one_original_and_one_audit(self):
        original = contract_pdf()
        clients = [self.base.f.make_client(admin=True) for _ in range(2)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            statuses = list(pool.map(lambda client: self.upload(raw=original, client=client).status_code, clients))
        self.assertEqual(statuses, [303, 303])
        self.assertEqual(len(self.rows()), 1)
        with database() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM assistent_audit WHERE aktion=?",
                ('mitarbeiter_portal_arbeitsvertrag_hinterlegt',)).fetchone()[0], 1)

    def test_upload_does_not_change_other_private_state_or_use_ai_or_public_files(self):
        self.base.profile(steuer_id='12345678901', personalnummer='Synthetic-ID')
        self.base.upload()
        protected = ('mitarbeiter', 'assistent_rechte', 'mitarbeiter_portal_profile',
                     'mitarbeiter_lohnzettel', 'mitarbeiter_betriebsurlaub',
                     'mitarbeiter_zeitstempel', 'mitarbeiter_zeitstatus', 'mitarbeiter_urlaubskonten')
        with database() as db:
            before = {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)] for table in protected}
        upload_root = Path(p.UPLOAD_DIR)
        before_files = set(upload_root.rglob('*'))
        with patch.object(p, 'get_openai_api_key', side_effect=AssertionError('No AI for contracts')):
            self.assertEqual(self.upload().status_code, 303)
            self.assertEqual(self.client.get(self.path(self.rows()[0])).status_code, 200)
        with database() as db:
            after = {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)] for table in protected}
        self.assertEqual(after, before)
        self.assertEqual(set(upload_root.rglob('*')), before_files)

    def test_png_jpeg_originals_are_supported_without_conversion(self):
        for kind, name in (('PNG', 'contract.png'), ('JPEG', 'contract.jpg')):
            data = io.BytesIO(); Image.new('RGB', (60, 80), 'white').save(data, kind)
            original = data.getvalue()
            self.assertEqual(self.upload(raw=original, filename=name).status_code, 303)
            row = self.rows()[-1]
            self.assertEqual(self.client.get(self.path(row)).data, original)

    def test_bad_or_encrypted_word_and_oversize_documents_rejected(self):
        with fitz.open() as document:
            document.new_page()
            encrypted = document.tobytes(encryption=fitz.PDF_ENCRYPT_AES_256, user_pw='secret', owner_pw='owner')
        for raw, name in ((b'PKfake-word', 'contract.docx'), (b'%PDF-invalid', 'contract.pdf'),
                          (encrypted, 'contract.pdf'), (contract_pdf(), 'contract.png'),
                          (b'x' * (MAX_DOCUMENT_BYTES + 1), 'contract.pdf')):
            with self.subTest(name=name, size=len(raw)):
                self.assertEqual(self.upload(raw=raw, filename=name).status_code, 400)
        self.assertEqual(self.rows(), [])

    def test_docx_original_draft_download_is_exact_private_and_owner_bound(self):
        original = contract_docx()
        self.assertEqual(self.upload(raw=original, filename='HINFAELLIG_Entwurf.docx',
                                     titel='Hinfälliger Vertragsentwurf').status_code, 303)
        row = self.rows()[0]
        response = self.client.get(self.path(row))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, original)
        self.assertEqual(response.mimetype, DOCX_MIME)
        self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
        self.assertIn('attachment;', response.headers['Content-Disposition'])
        self.assertIn('HINFAELLIG_Entwurf.docx', response.headers['Content-Disposition'])
        self.upload(raw=original, mid=2, filename='other.docx')
        self.assertEqual(self.client.get(self.path(self.rows()[1])).status_code,404)

    def test_docx_is_not_accepted_for_payroll_or_other_extensions(self):
        original = contract_docx()
        self.assertEqual(self.base.upload(raw=original, filename='payroll.docx').status_code, 400)
        for filename in ('contract.docm','contract.zip','contract.pdf','contract.doc'):
            self.assertEqual(self.upload(raw=original,filename=filename).status_code,400)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_lohnzettel').fetchone()[0],0)
        self.assertEqual(self.rows(),[])

    def test_foreign_zip_forged_word_structure_and_bad_xml_are_rejected(self):
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as archive: archive.writestr('unrelated.txt','Not a Word document')
        for original in (stream.getvalue(),b'PK\x03\x04not-a-zip',contract_docx(document='<notWord/>'),
                         contract_docx(document='<broken'),contract_docx(content_type='application/pdf'),
                         contract_docx(document='<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"/>')):
            self.assertEqual(self.upload(raw=original,filename='contract.docx').status_code,400)
        self.assertEqual(self.rows(),[])

    def test_docx_macro_parts_content_types_relationships_and_dtd_are_rejected(self):
        macro_relationship='<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' \
            '<Relationship Id="rId2" Type="http://schemas.microsoft.com/office/2006/relationships/vbaProject" Target="renamed.dat"/></Relationships>'
        dtd='<!DOCTYPE w:document [<!ENTITY test "not evaluated">]><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">' \
            '<w:body><w:p>&test;</w:p></w:body></w:document>'
        for original in (contract_docx(content_type='application/vnd.ms-word.document.macroEnabled.main+xml'),
                         contract_docx(extra=[('word/vbaProject.bin',b'macro')]),
                         contract_docx(extra=[('word/_rels/document.xml.rels',macro_relationship),('word/renamed.dat',b'macro')]),
                         contract_docx(document=dtd),contract_docx(document=dtd.encode('utf-16')),
                         contract_docx(extra=[('word/embeddings/object.bin',b'embedded program')])):
            self.assertEqual(self.upload(raw=original,filename='contract.docx').status_code,400)
        self.assertEqual(self.rows(),[])

    def test_docx_zip_inspection_is_bounded_and_ambiguous_paths_rejected(self):
        excessive_count=bytearray(contract_docx())
        end=excessive_count.rfind(b'PK\x05\x06')
        struct.pack_into('<HH',excessive_count,end+8,65535,65535)
        for original in (contract_docx(extra=[('../outside.xml','x')]),
                         contract_docx(extra=[('word/document.xml','duplicate')]),
                         contract_docx(extra=[('WORD/DOCUMENT.XML','case duplicate')]),
                         contract_docx(extra=[('word/media/large.txt',b'x'*(16*1024*1024+1))]),
                         contract_docx(document=b'x'*(4*1024*1024+1)),
                         contract_docx(extra=[('word/media/'+str(n)+'.txt','x') for n in range(510)]),
                         bytes(excessive_count)):
            self.assertLess(len(original),MAX_DOCUMENT_BYTES)
            self.assertEqual(self.upload(raw=original,filename='contract.docx').status_code,400)
        self.assertEqual(self.rows(),[])

    def test_strict_docx_namespace_is_accepted_without_conversion(self):
        original=contract_docx(document='<w:document xmlns:w="http://purl.oclc.org/ooxml/wordprocessingml/main"><w:body/></w:document>')
        self.assertEqual(self.upload(raw=original,filename='strict.docx').status_code,303)
        self.assertEqual(self.client.get(self.path(self.rows()[0])).data,original)

    def test_optional_title_strict_form_and_single_file(self):
        self.assertEqual(self.upload(titel='').status_code, 303)
        for values in ({'titel': '<script>x</script>'}, {'titel': 'x' * 121}, {'titel': 'x\ny'},
                       {'mitarbeiter_id': '2'}, {'period': '2026-10'}):
            self.assertEqual(self.upload(**values).status_code, 400)
        for payload in (
            MultiDict([('csrf_token','test-csrf'), ('titel','one'), ('titel','two'),
                       ('file',(io.BytesIO(contract_pdf()),'one.pdf'))]),
            MultiDict([('csrf_token','test-csrf'), ('file',(io.BytesIO(contract_pdf()),'one.pdf')),
                       ('file',(io.BytesIO(contract_pdf()),'two.pdf'))]),
        ):
            self.assertEqual(self.admin.post('/admin/mitarbeiter/1/portal/arbeitsvertrag',
                             data=payload, content_type='multipart/form-data').status_code, 400)
        self.assertEqual(len(self.rows()), 1)

    def test_unknown_employee_and_large_document_ids_are_404(self):
        for mid in (999, 2147483648):
            self.assertEqual(self.upload(mid=mid).status_code, 404)
        for value in ('0','999','2147483648','99999999999999999999999999999999'):
            self.assertEqual(self.client.get('/werkstatt/mein-konto/arbeitsvertrag/' + value).status_code, 404)

    def test_original_integrity_checked_and_audit_omits_private_content(self):
        original = contract_pdf('Private synthetic agreement')
        self.upload(raw=original, filename='private-contract-name.pdf', titel='Private document title')
        row = self.rows()[0]
        with database() as db:
            audit = json.dumps([dict(item) for item in db.execute('SELECT * FROM assistent_audit')])
        for secret in ('private-contract-name', 'Private document title', base64.b64encode(original).decode()):
            self.assertNotIn(secret, audit)
        for column, value in (('original_base64','invalid!'), ('sha256','0' * 64),
                              ('size_bytes',1), ('mime','text/html')):
            with self.subTest(column=column):
                with database() as db: db.execute(f'UPDATE {CONTRACTS} SET {column}=? WHERE id=?', (value,row['id']))
                self.assertEqual(self.client.get(self.path(row)).status_code,404)
                with database() as db: db.execute(f'UPDATE {CONTRACTS} SET {column}=? WHERE id=?', (row[column],row['id']))

    def test_contract_only_restore_preserves_document_owner_and_missing_login(self):
        self.upload(); current = self.base.snapshot()
        ensure_employee_private_state_for_import(p, export=current)
        for table in (CONTRACTS, 'mitarbeiter', 'assistent_rechte'):
            invalid = copy.deepcopy(current); invalid['tables'][table] = []
            with self.subTest(table=table), self.assertRaises(ValueError):
                ensure_employee_private_state_for_import(p, export=invalid)
        for column in current['tables'][CONTRACTS][0]:
            invalid = copy.deepcopy(current); invalid['tables'][CONTRACTS][0][column] = 'changed'
            with self.subTest(column=column), self.assertRaises(ValueError):
                ensure_employee_private_state_for_import(p, export=invalid)
        with database() as db: db.execute('DELETE FROM assistent_rechte WHERE mitarbeiter_id=1')
        with self.assertRaises(ValueError): ensure_employee_private_state_for_import(p, export=current)
        ensure_employee_private_state_for_import(p, export=self.base.snapshot())

    def test_externalized_contract_bytes_must_match(self):
        original = contract_pdf(); self.upload(raw=original)
        current = self.base.snapshot(); row = current['tables'][CONTRACTS][0]
        row['original_base64'] = ''
        reference = {'table':CONTRACTS,'row_id':row['id'],'column':'original_base64'}
        refs = {(CONTRACTS,row['id'],'original_base64'):reference}
        with patch.object(p,'backup_binary_reference_map',return_value=refs), \
             patch.object(p,'read_backup_binary_blob',return_value=original):
            with database() as db:
                ensure_employee_private_state_for_import(p,export=current,target=db,archive=object(),names=[])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM ' + CONTRACTS).fetchone()[0],1)
            with self.assertRaises(ValueError): ensure_employee_private_state_for_import(p,export=current)
        with patch.object(p,'backup_binary_reference_map',return_value=refs), \
             patch.object(p,'read_backup_binary_blob',return_value=b'wrong'):
            with self.assertRaises(ValueError):
                ensure_employee_private_state_for_import(p,export=current,archive=object(),names=[])

    def test_sqlite_restore_uses_actual_contract_originals_and_owner(self):
        self.upload(); current = self.base.snapshot()
        with tempfile.TemporaryDirectory(dir=ISOLATED.name) as folder:
            source = Path(folder) / 'source.db'
            tables = tuple(current['tables'])
            with database() as db:
                ddl = [row[0] for row in db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name IN ("
                        + ','.join('?' for _ in tables) + ')', tables)]
            with closing(sqlite3.connect(source)) as db, db:
                for sql in ddl: db.execute(sql)
                for table, rows in current['tables'].items():
                    for row in rows:
                        db.execute('INSERT INTO ' + table + '(' + ','.join(row) + ') VALUES('
                                   + ','.join('?' for _ in row) + ')', tuple(row.values()))
            ensure_employee_private_state_for_import(p, imported_db=source)
            with closing(sqlite3.connect(source)) as db, db:
                db.execute('UPDATE ' + CONTRACTS + ' SET mitarbeiter_id=2')
            with self.assertRaises(ValueError): ensure_employee_private_state_for_import(p, imported_db=source)

    def test_real_profile_templates_keep_contracts_separate_and_person_bound(self):
        self.base.renderer.stop()
        self.upload(titel='My synthetic contract')
        self.upload(mid=2,titel='Other private contract')
        personal = self.client.get('/werkstatt/mein-konto').get_data(as_text=True)
        self.assertIn('My synthetic contract', personal)
        self.assertNotIn('Other private contract', personal)
        self.assertIn('/werkstatt/mein-konto#arbeitsvertraege',personal)
        self.assertIn('id="arbeitsvertraege"',personal)
        self.assertIn('Steuer-ID',personal)
        admin = self.admin.get('/admin/mitarbeiter/2/portal').get_data(as_text=True)
        self.assertIn('Other private contract',admin)
        self.assertNotIn('My synthetic contract',admin)
        self.assertIn('action="/admin/mitarbeiter/2/portal/arbeitsvertrag"',admin)
        self.assertIn('name="titel"',admin)
        self.assertNotIn('type="date"',admin.split('id="arbeitsvertraege"',1)[1].split('id="lohnzettel"',1)[0])


if __name__ == '__main__':
    main()
