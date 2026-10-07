"""Actual JSON/SQLite import guards with synthetic personal photo batches."""
import base64
import copy
import io
import json
from pathlib import Path
import sqlite3
import sys
import threading
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_material_external_restore as fixtures


TABLES = (*fixtures.TABLES, 'einkauf_material_nachrichten', 'einkauf_material_texte',
          'einkauf_material_rueckfragen', 'einkauf_eingang', 'einkauf_eingang_positionen',
          'einkauf_eingang_dateien', 'assistent_materialfotos', 'assistent_bestellanforderungen',
          'assistent_bestellpakete')
SCHEMA = '''
CREATE TABLE einkauf_material_nachrichten (
 id INTEGER PRIMARY KEY,phone_number_id TEXT,wamid TEXT,canonical_hash TEXT,
 sender_id INTEGER,sender_revision INTEGER,employee_id INTEGER,rights_version INTEGER,
 expected_sha256 TEXT,caption TEXT,source_at TEXT,intake_id INTEGER,file_id INTEGER,assistant_photo_id INTEGER);
CREATE TABLE einkauf_material_texte (
 id INTEGER PRIMARY KEY,phone_number_id TEXT,wamid TEXT,body TEXT,draft_id INTEGER,state TEXT);
CREATE TABLE einkauf_material_rueckfragen (id INTEGER PRIMARY KEY,draft_id INTEGER,revision INTEGER,body TEXT,state TEXT);
CREATE TABLE einkauf_eingang (id INTEGER PRIMARY KEY,source_key TEXT,created_by TEXT);
CREATE TABLE einkauf_eingang_positionen (id INTEGER PRIMARY KEY,eingang_id INTEGER,quantity TEXT);
CREATE TABLE einkauf_eingang_dateien (id INTEGER PRIMARY KEY,eingang_id INTEGER,sha256 TEXT,original_base64 TEXT);
CREATE TABLE assistent_materialfotos (id INTEGER PRIMARY KEY,foto_id TEXT,actor TEXT,request_id TEXT,file_sha256 TEXT,file_base64 TEXT);
CREATE TABLE assistent_bestellanforderungen (id TEXT PRIMARY KEY,actor_id TEXT,request_id TEXT,snapshot_json TEXT,batch_id TEXT);
CREATE TABLE assistent_bestellpakete (id TEXT PRIMARY KEY,payload_json TEXT,fingerprint TEXT,state TEXT,result_json TEXT);
CREATE TABLE mailbox_outbox (id INTEGER PRIMARY KEY,token TEXT,fingerprint TEXT,state TEXT,attempt TEXT);
'''


class PersonalRestoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures.ExternalRestoreTests.setUpClass()

    def setUp(self):
        self.f = fixtures.ExternalRestoreTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.ns, self.client = self.f.ns, self.f.client
        self.ns.update(BACKUP_TABLES=TABLES, base64=base64)
        self.ns['BACKUP_BINARY_FIELDS'] = {
            'einkauf_eingang_dateien': {'original_base64': {}},
            'assistent_materialfotos': {'file_base64': {}},
        }
        self.ns['backup_binary_reference_map'] = lambda export: {
            (r['table'], r['row_id'], r['column']): r for r in export.get('binary_blobs', [])}
        # The production blob reader also checks ZIP size and digest; this
        # fixture supplies known member bytes to exercise guard comparisons.
        self.ns['read_backup_binary_blob'] = lambda archive, names, ref: archive.read(ref['zip_path'])
        with self.db() as db:
            db.executescript(SCHEMA)

    def db(self):
        return fixtures.connection(self.f.db_path)

    def seed(self, state='accepted'):
        with self.db() as db:
            db.execute('INSERT INTO einkauf_material_nachrichten VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (5, 'portal:personal', 'portal.1.request.item', 'whole-batch-and-photo-hash', 0, 1, 1, 1,
                 'original-sha', '2 Stück, dringend', '2026-10-07T12:00:00+00:00', 7, 8, 'f' * 32))
            db.execute('INSERT INTO einkauf_material_dialoge VALUES(?,?,?,?,?,?,?,?,?,?)',
                (6, 5, 4, state, '{"quantity":{"value":"2"}}', '{"frozen":"order"}',
                 'snapshot-sha', 'order-uuid', 'sent', 10.0))
            db.execute('INSERT INTO einkauf_material_texte VALUES(?,?,?,?,?,?)',
                (10, 'portal:personal', 'portalreply.1.reply.6', 'M-6 R3: ja', 6, 'applied'))
            db.execute('INSERT INTO einkauf_material_rueckfragen VALUES(?,?,?,?,?)', (11, 6, 3, 'Noch zwei?', 'superseded'))
            db.execute('INSERT INTO einkauf_eingang VALUES(?,?,?)', (7, 'portal-photo:1:original', 'mitarbeiter:1'))
            db.execute('INSERT INTO einkauf_eingang_positionen VALUES(?,?,?)', (12, 7, '2'))
            db.execute('INSERT INTO einkauf_eingang_dateien VALUES(?,?,?,?)',
                (8, 7, 'original-sha', base64.b64encode(b'original-photo').decode()))
            db.execute('INSERT INTO assistent_materialfotos VALUES(?,?,?,?,?,?)',
                (9, 'f' * 32, 'mitarbeiter:1', 'portal-analysis', 'analysis-sha', base64.b64encode(b'analysis-photo').decode()))
            db.execute('INSERT INTO assistent_audit VALUES(?,?,?,?,?,?)',
                (13, 'mitarbeiter:1', None, 'material_portal_submitted', '{"batch_hash":"original"}', 'synthetic-time'))
            db.execute('INSERT INTO assistent_audit VALUES(?,?,?,?,?,?)',
                (14, 'mitarbeiter:1', None, 'material_portal_answer', '{"draft_id":6,"text_id":10}', 'synthetic-time'))
            if state == 'accepted':
                db.execute('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?,?,?)',
                    ('order-uuid', 'mitarbeiter:1', 'material:6', '{"frozen":"snapshot"}', 'batch-uuid'))
                db.execute('INSERT INTO assistent_bestellpakete VALUES(?,?,?,?,?)',
                    ('batch-uuid', '{"frozen":"mail"}', 'frozen-mail-sha', 'sent', '{"state":"sent"}'))
                db.execute('INSERT INTO mailbox_outbox VALUES(?,?,?,?,?)',
                    (15, 'batch-uuid', 'mime-fingerprint', 'sent', 'smtp-attempt'))

    def export(self, outbox=False):
        with self.db() as db:
            return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)]
                               for table in (*TABLES, *(['mailbox_outbox'] if outbox else []))}}

    def source(self, export):
        path = self.f.root / 'personal-source.db'
        if path.exists():
            path.unlink()
        with self.db() as db:
            ddl = [row[0] for row in db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name<>'sqlite_sequence'")]
        with fixtures.connection(path) as db:
            for statement in ddl:
                db.execute(statement)
            for table, rows in export['tables'].items():
                for row in rows:
                    db.execute('INSERT INTO ' + table + ' (' + ','.join(row) + ') VALUES (' +
                               ','.join('?' for _ in row) + ')', tuple(row.values()))
        return path

    def rejected(self, export):
        before = self.export(outbox=True)
        with self.assertRaisesRegex(ValueError, 'persönliche Foto-Bestellungen'):
            self.ns['import_backup_json_rows_into_current_database'](export, None, [])
        self.assertEqual(before, self.export(outbox=True))

    def test_old_json_cannot_remove_unprocessed_photo_batch(self):
        old = self.export()
        self.seed(state='open')
        self.rejected(old)

    def test_sent_chain_members_cannot_disappear_or_be_replaced(self):
        self.seed()
        for table in TABLES:
            if table == 'ordinary':
                continue
            with self.subTest(table=table):
                old = self.export()
                old['tables'][table] = []
                self.rejected(old)

    def test_source_identity_quantity_and_dispatch_cannot_roll_back(self):
        self.seed()
        fields = {'einkauf_material_nachrichten': ('wamid','canonical_hash','employee_id','rights_version',
                  'sender_revision','expected_sha256','intake_id','file_id','assistant_photo_id','caption'),
                  'einkauf_material_dialoge': ('message_id','revision','state','fields_json','dispatch_id','dispatch_state'),
                  'assistent_bestellanforderungen': ('request_id','actor_id','snapshot_json','batch_id'),
                  'assistent_bestellpakete': ('payload_json','state','result_json'),
                  'assistent_audit': ('actor','details')}
        for table, keys in fields.items():
            for key in keys:
                with self.subTest(table=table, key=key):
                    changed = self.export()
                    changed['tables'][table][0][key] = 'replaced'
                    self.rejected(changed)

    def test_inquiry_mode_and_description_are_immutable_originals_in_restore(self):
        self.seed(state='review')
        details = {'menge':1,'dringend':True,'vorgang':'anfrage','beschreibung':'Stoßstange rechts'}
        caption = 'portal-request:v1:' + json.dumps(details,ensure_ascii=False,sort_keys=True,separators=(',',':'))
        with self.db() as db:
            db.execute('UPDATE einkauf_material_nachrichten SET caption=? WHERE id=5',(caption,))
            db.execute('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=6',
                       (json.dumps({'vorgang':{'value':'anfrage'},'beschreibung':{'value':details['beschreibung']},
                                    'order_requested':{'value':False}}),))
        for changes in ({'vorgang':'bestellung'},{'beschreibung':'Stoßstange links'}):
            replaced = self.export()
            altered = dict(details,**changes)
            replaced['tables']['einkauf_material_nachrichten'][0]['caption'] = 'portal-request:v1:'+json.dumps(altered)
            self.rejected(replaced)
        replaced = self.export()
        replaced['tables']['einkauf_material_dialoge'][0]['fields_json'] = '{"order_requested":{"value":true}}'
        self.rejected(replaced)
        self.ns['import_backup_json_rows_into_current_database'](self.export(),None,[])
        with self.db() as db:
            self.assertEqual(db.execute('SELECT caption FROM einkauf_material_nachrichten WHERE id=5').fetchone()['caption'],caption)

    def test_matching_current_json_restores_unrelated_data(self):
        self.seed()
        current = self.export()
        current['tables']['ordinary'][0]['value'] = 'restored'
        self.ns['import_backup_json_rows_into_current_database'](current, None, [])
        self.assertEqual(current, self.export())
        self.assertEqual(self.export(outbox=True)['tables']['mailbox_outbox'][0]['state'], 'sent')

    def test_current_sqlite_passes_and_old_sqlite_stops_before_delete(self):
        old = self.export(outbox=True)
        self.seed()
        before = self.export(outbox=True)
        with self.assertRaisesRegex(ValueError, 'persönliche Foto-Bestellungen'):
            self.ns['import_sqlite_rows_into_current_database'](self.source(old))
        self.assertEqual(before, self.export(outbox=True))
        self.ns['import_sqlite_rows_into_current_database'](self.source(before))
        self.assertEqual(before, self.export(outbox=True))

    def test_sqlite_must_keep_actual_outbox_and_not_trust_json_beside_it(self):
        self.seed()
        good = self.export(outbox=True)
        bad = copy.deepcopy(good)
        bad['tables']['mailbox_outbox'] = []
        path = self.source(bad)
        self.ns['extract_import_package_files'] = lambda *args: (path, self.f.root / 'uploads', good)
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, 'w') as archive:
            archive.writestr('placeholder', 'synthetic')
        payload.seek(0)
        response = self.client.post('/admin/daten-import', data={'datenpaket': (payload, 'backup.zip')})
        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as session:
            self.assertTrue(any('persönliche Foto-Bestellungen' in text for _, text in session['_flashes']))
        for name in ('create_backup_package','copy_sqlite_database_snapshot','replace_uploads_from_import','init_db'):
            self.ns[name].assert_not_called()
        self.assertEqual(good, self.export(outbox=True))

    def test_supplier_batch_keeps_other_members_together(self):
        self.seed()
        with self.db() as db:
            db.execute('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?,?,?)',
                       ('another-order', 'mitarbeiter:2', 'material:22', '{}', 'batch-uuid'))
        bad = self.export()
        bad['tables']['assistent_bestellanforderungen'].pop()
        self.rejected(bad)

    def test_orphan_personal_audit_or_answer_keeps_idempotent_history(self):
        self.seed()
        with self.db() as db:
            db.execute('DELETE FROM einkauf_material_nachrichten')
        bad = self.export()
        bad['tables']['assistent_audit'] = []
        self.rejected(bad)
        bad = self.export()
        bad['tables']['einkauf_material_texte'] = []
        self.rejected(bad)

    def binary_export(self, changed=False, inline=False):
        export = self.export()
        export['binary_blobs'] = []
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, 'w') as archive:
            for table, column in [('einkauf_eingang_dateien','original_base64'),('assistent_materialfotos','file_base64')]:
                row = export['tables'][table][0]
                raw = base64.b64decode(row[column])
                if changed and table == 'einkauf_eingang_dateien':
                    raw = b'wrong-photo'
                path = table + '/' + str(row['id'])
                archive.writestr(path, raw)
                export['binary_blobs'].append(dict(table=table,column=column,row_id=row['id'],zip_path=path))
                if not inline:
                    row[column] = ''
        payload.seek(0)
        return export, zipfile.ZipFile(payload)

    def test_matching_externalized_original_bytes_allow_current_json(self):
        self.seed()
        export, archive = self.binary_export()
        with archive:
            self.ns['import_backup_json_rows_into_current_database'](export, archive, archive.namelist())
        self.assertEqual(base64.b64decode(self.export()['tables']['einkauf_eingang_dateien'][0]['original_base64']), b'original-photo')

    def test_changed_actual_zip_bytes_rejected_even_with_matching_inline_copy(self):
        self.seed()
        before = self.export()
        for inline in (False, True):
            with self.subTest(inline=inline):
                export, archive = self.binary_export(changed=True, inline=inline)
                with archive, self.assertRaisesRegex(ValueError, 'persönliche Foto-Bestellungen'):
                    self.ns['import_backup_json_rows_into_current_database'](export, archive, archive.namelist())
                self.assertEqual(before, self.export())

    def test_restore_waits_for_personal_submit_then_rejects_older_backup(self):
        old = self.export()
        begun, done = threading.Event(), threading.Event()
        errors = []
        def restoring():
            begun.set()
            try:
                self.ns['import_backup_json_rows_into_current_database'](old, None, [])
            except ValueError as exc:
                errors.append(str(exc))
            finally:
                done.set()
        with self.ns['portal_originals_operation_lock']():
            worker = threading.Thread(target=restoring)
            worker.start()
            self.assertTrue(begun.wait(2))
            self.assertFalse(done.wait(.05))
            self.seed()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertTrue(errors and 'persönliche Foto-Bestellungen' in errors[0])
        self.assertEqual(self.export()['tables']['assistent_bestellpakete'][0]['state'], 'sent')


if __name__ == '__main__':
    unittest.main()
