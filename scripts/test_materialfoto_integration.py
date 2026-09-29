"""Real Flask routes on the shared synthetic test fixture; all model calls mocked."""
import io
import json
import base64
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch
import zipfile

import test_assistent as fixture
from PIL import Image

p, database = fixture.p, fixture.database

class MaterialPhotoHttpTests(unittest.TestCase):
    make_client = fixture.AssistantTests.make_client
    post = fixture.AssistantTests.post
    tearDown = fixture.AssistantTests.tearDown

    def setUp(self):
        fixture.AssistantTests.setUp(self)
        p.app.config['ASSISTANT_READ_ONLY'] = True
        with database() as db:
            db.execute('DELETE FROM assistent_materialfotos')
            # Shared fixture replaces demo jobs with IDs 156/157; discard only
            # its orphaned synthetic status logs so the backup is referentially valid.
            db.execute('DELETE FROM status_log WHERE auftrag_id NOT IN (SELECT id FROM auftraege)')
            db.execute('UPDATE assistent_rechte SET dokumentieren=0,limit_cent=0 WHERE mitarbeiter_id=1')
        self.enterContext(patch.object(p.assistant_material_photos, 'vision', return_value={
            'art': 'produkt', 'produkt': 'Test Abdeckband', 'breite': '30 mm'}))
        self.enterContext(patch.object(p.cockpit_data, 'articles', return_value={'varianten': [{
            'produkt_name': 'Test Abdeckband 30 mm', 'lieferant': 'Test Supplier', 'artikelnummer': 'TEST-30',
            'groesse': '30 mm', 've': 'Stück', 'quellen': [{'art': 'einkauf', 'beleg_id': 1, 'seite': 1}]}]}))
        buffer = io.BytesIO(); Image.new('RGB', (10, 10), 'green').save(buffer, format='PNG')
        self.raw = buffer.getvalue()

    def upload(self, csrf=True):
        return self.client.post('/werkstatt/assistent/materialfotos',
            data={'file': (io.BytesIO(self.raw), 'synthetic-label.png'), 'request_id': 'synthetic-photo-request-1234'},
            headers={'X-CSRF-Token': 'test-csrf'} if csrf else {})

    def test_read_only_purchase_reader_can_choose_without_order_or_document_rights(self):
        staged = self.upload(); self.assertEqual(staged.status_code, 200, staged.get_json())
        photo_id = staged.get_json()['id']
        analyzed = self.post('/materialfotos/' + photo_id + '/analyse')
        self.assertEqual(analyzed.status_code, 200, analyzed.get_json())
        chosen = self.post('/materialfotos/' + photo_id + '/auswahl', {'treffer_id': analyzed.get_json()['treffer'][0]['id']})
        self.assertEqual(chosen.status_code, 200, chosen.get_json())
        self.assertFalse(chosen.get_json()['auswahl']['bestellbar'])
        self.assertEqual(chosen.headers['Cache-Control'], 'no-store')
        for table in ('assistent_aktionen', 'dateien'):
            with database() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 0)
        view = self.client.get('/werkstatt/assistent').get_data(as_text=True)
        self.assertIn('id="materialfoto-dialog"', view)
        self.assertIn('id="materialfoto-open"', view)

    def test_current_rights_csrf_and_actor_are_enforced_by_routes(self):
        self.assertEqual(self.upload(csrf=False).status_code, 400)
        self.assertEqual(p.app.test_client().get('/werkstatt/assistent/materialfotos').status_code, 401)
        staged = self.upload().get_json(); path = '/werkstatt/assistent/materialfotos/' + staged['id']
        other = self.make_client(admin=True)
        self.assertEqual(other.get(path).status_code, 400, 'Admin may not read another actor private photos')
        with database() as db: db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.client.get(path).status_code, 403)
        self.assertEqual(self.post('/materialfotos/' + staged['id'] + '/analyse').status_code, 403)
        view = self.client.get('/werkstatt/assistent').get_data(as_text=True)
        self.assertNotIn('id="materialfoto-dialog"', view)

    def test_client_cannot_turn_material_selection_into_an_order_or_inject_prices(self):
        staged = self.upload().get_json(); result = self.post('/materialfotos/' + staged['id'] + '/analyse').get_json()
        response = self.post('/materialfotos/' + staged['id'] + '/auswahl', {
            'treffer_id': result['treffer'][0]['id'], 'bestellbestaetigung': True, 'preis': 1})
        self.assertEqual(response.status_code, 400)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT selected_hit FROM assistent_materialfotos').fetchone()[0], '')
        self.assertNotIn('file_base64', json.dumps(result))

    def selected(self):
        staged = self.upload().get_json()
        photo = self.post('/materialfotos/' + staged['id'] + '/analyse').get_json()
        return self.post('/materialfotos/' + staged['id'] + '/auswahl', {'treffer_id': photo['treffer'][0]['id']}).get_json()['auswahl']

    def test_text_and_realtime_tools_only_expose_current_actor_selection(self):
        selection = self.selected()
        realtime = self.post('/realtime/werkzeug', {'name': 'materialfoto_lesen', 'arguments': {}})
        self.assertEqual(realtime.status_code, 200, realtime.get_json())
        self.assertEqual(realtime.get_json()['result']['auswahl']['id'], selection['id'])
        self.assertFalse(realtime.get_json()['result']['auswahl']['bestellbar'])
        self.assertEqual(self.post('/realtime/werkzeug', {'name': 'materialfoto_lesen', 'arguments': {'actor': 'admin'}}).status_code, 400)
        event = self.post('/realtime/werkzeug', {'name': 'materialfoto_anfordern', 'arguments': {}})
        self.assertEqual(event.get_json()['event']['type'], 'materialfoto')
        admin_read = self.post('/realtime/werkzeug', {'name': 'materialfoto_lesen', 'arguments': {}}, client=self.make_client(admin=True))
        self.assertIsNone(admin_read.get_json()['result']['auswahl'])
        context = self.client.get('/werkstatt/assistent/realtime/kontext').get_json()
        self.assertIn(selection['artikelnummer'], context['instructions'])
        self.assertIn('keine Preis-, Mengen- oder Dringlichkeitsbestätigung', context['instructions'])
        self.assertEqual(self.post('/realtime/werkzeug', {'name': 'bestellung_vorschlagen', 'arguments': {}}).status_code, 400)
        first = Mock(); first.json.return_value = {'output': [{'type': 'function_call', 'name': 'materialfoto_lesen', 'arguments': '{}', 'call_id': 'photo-read'}]}
        second = Mock(); second.json.return_value = {'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'Wie viel brauchst du, und ist es dringend?'}]}]}
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-key'), patch('werkstatt_assistent.requests.post', side_effect=[first, second]) as model:
            answer = self.post('/dialog', {'text': 'Welchen Artikel habe ich auf meinem Produktfoto gewählt?'})
        self.assertEqual(answer.status_code, 200, answer.get_json())
        messages = model.call_args.kwargs['json']['input']
        result = next(item for item in messages if item.get('type') == 'function_call_output')
        payload = json.loads(result['output'])
        self.assertEqual(payload['auswahl']['artikelnummer'], selection['artikelnummer'])
        self.assertFalse(payload['auswahl']['bestellbar'])
        with database() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0], 0)

    def test_non_native_cockpit_cannot_access_photo_or_tools(self):
        selection = self.selected()
        admin = self.make_client(admin=True)
        with patch.dict(p.app.config, ASSISTANT_NATIVE_COCKPIT=False):
            self.assertEqual(admin.get('/werkstatt/assistent/materialfotos').status_code, 403)
            self.assertEqual(self.post('/materialfotos/' + selection['foto_id'] + '/analyse', client=admin).status_code, 403)
            self.assertEqual(self.post('/realtime/werkzeug', {'name': 'materialfoto_lesen', 'arguments': {}}, client=admin).status_code, 400)

    def test_private_photo_and_new_tables_survive_json_and_sqlite_backup(self):
        selection = self.selected()
        new_tables = {'mitarbeiter_urlaubskonten', 'mitarbeiter_urlaubsantraege', 'mitarbeiter_urlaub_audit',
                      'mitarbeiter_zeitstatus', 'mitarbeiter_zeitstempel', 'assistent_materialfotos'}
        with database() as db:
            original = dict(db.execute('SELECT * FROM assistent_materialfotos').fetchone())
        backup = p.create_backup_package('synthetic-material-photo-test')
        with zipfile.ZipFile(backup) as archive, tempfile.TemporaryDirectory() as folder:
            names = set(archive.namelist())
            export = json.loads(archive.read('backup.json'))
            self.assertTrue(new_tables <= set(export['tables']))
            self.assertTrue({'werkstatt_personal_v1', 'werkstatt_materialfotos_v1'} <= set(export['schema_features']))
            row = export['tables']['assistent_materialfotos'][0]
            self.assertEqual(row['file_base64'], '')
            ref = next(item for item in export['binary_blobs'] if item['table'] == 'assistent_materialfotos')
            self.assertEqual(archive.read(ref['zip_path']), base64.b64decode(original['file_base64']))
            p.validate_backup_binary_reference_completeness(export, p.backup_binary_reference_map(export))
            p.import_backup_json_rows_into_current_database(export, archive, names)
            with database() as db:
                restored = dict(db.execute('SELECT * FROM assistent_materialfotos').fetchone())
            self.assertEqual(restored, original)
            copied = Path(folder) / 'restored.db'; copied.write_bytes(archive.read('auftraege.db'))
            with closing(sqlite3.connect(copied)) as db: self.assertEqual(db.execute('SELECT file_base64 FROM assistent_materialfotos').fetchone()[0], '')
            p.hydrate_imported_sqlite_backup_blobs(copied, archive, names, export)
            with closing(sqlite3.connect(copied)) as db:
                self.assertEqual(db.execute('SELECT file_base64 FROM assistent_materialfotos').fetchone()[0], original['file_base64'])
                for table in new_tables: db.execute('DROP TABLE ' + table)
            def old_db():
                db = sqlite3.connect(copied); db.row_factory = sqlite3.Row; return db
            with patch.object(p, 'get_db', side_effect=old_db):
                p.assistant_init_schema(); p.assistant_init_schema()
                with closing(old_db()) as db:
                    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    self.assertTrue(new_tables <= tables)
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_materialfotos').fetchone()[0], 0)
            legacy = json.loads(json.dumps(export))
            legacy['schema_features'] = [f for f in legacy['schema_features'] if f not in {'werkstatt_personal_v1', 'werkstatt_materialfotos_v1'}]
            legacy['binary_blobs'] = [ref for ref in legacy['binary_blobs'] if ref['table'] not in new_tables]
            for table in new_tables: legacy['tables'].pop(table)
            p.validate_backup_binary_reference_completeness(legacy, p.backup_binary_reference_map(legacy))
            for feature in ('werkstatt_personal_v1', 'werkstatt_materialfotos_v1'):
                inconsistent = dict(legacy, schema_features=legacy['schema_features'] + [feature])
                with self.assertRaises(ValueError): p.validate_backup_binary_reference_completeness(inconsistent, p.backup_binary_reference_map(inconsistent))
        self.assertEqual(p.assistant_material_photos.context({'actor': 'mitarbeiter:1', 'lesen': 1, 'einkaufen': 1})['id'], selection['id'])


if __name__ == '__main__': unittest.main()
