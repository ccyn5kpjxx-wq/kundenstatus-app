"""Offline regressions for natural-key PG inserts and the restore schema hook."""
import sqlite3
import base64
import hashlib
import io
import zipfile
import tempfile
import types
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import test_assistent as fixture

p = fixture.p


class PgCursorOnSqlite:
    """Run actual PostgresConnection SQL on synthetic SQLite for RETURNING checks."""
    def __init__(self, db, statements):
        self.cursor = db.cursor()
        self.statements = statements

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cursor.close()

    def execute(self, sql, params):
        self.statements.append(sql)
        self.cursor.execute(sql.replace('%s', '?'), params)
        self.rowcount = self.cursor.rowcount
        self.description = [types.SimpleNamespace(name=c[0]) for c in self.cursor.description] if self.cursor.description else None

    def fetchall(self):
        return self.cursor.fetchall()


class AssistantStorageTests(unittest.TestCase):
    setUp = fixture.AssistantTests.setUp
    tearDown = fixture.AssistantTests.tearDown
    make_client = fixture.AssistantTests.make_client
    post = fixture.AssistantTests.post

    def test_mail_attachment_backup_externalizes_and_preserves_original_bytes(self):
        raw = b'%PDF-1.4 synthetic invoice attachment'
        p.assistant_mail_sources_init_schema()
        with fixture.database() as db:
            db.execute('''INSERT INTO assistent_mailquellen_dateien
                (sha256,supplier,beleg_id,stored_name,original_name,file_base64,size,created_at)
                VALUES(?,?,?,?,?,?,?,?)''', (hashlib.sha256(raw).hexdigest(), 'Synthetic materials', 1,
                'synthetic.pdf', 'synthetic.pdf', base64.b64encode(raw).decode(), len(raw), '2026-09-28'))
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, 'w') as archive:
                rows, references, size = p.write_table_rows_and_binary_blobs(db, archive, 'assistent_mailquellen_dateien')
        self.assertEqual(rows[0]['file_base64'], '')
        self.assertEqual(size, len(raw))
        self.assertEqual(references[0]['sha256'], hashlib.sha256(raw).hexdigest())
        with zipfile.ZipFile(stream) as archive:
            self.assertEqual(archive.read(references[0]['zip_path']), raw)

    def test_mail_source_restore_schema_and_backup_feature_preserve_cursor(self):
        from werkstatt_mailquellen import TABLES
        self.assertTrue(set(TABLES) <= set(p.BACKUP_TABLES))
        self.assertIn('werkstatt_mailquellen_v1', p.BACKUP_SCHEMA_FEATURES)
        routes_before=len(list(p.app.url_map.iter_rules()))
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'old-backup.db'
            def restored_db():
                db=sqlite3.connect(path); db.row_factory=sqlite3.Row; return db
            with patch.object(p,'get_db',side_effect=restored_db):
                p.assistant_mail_sources_init_schema()
                with closing(restored_db()) as db:
                    db.execute("INSERT INTO assistent_mailquellen_laeufe(account,run_token,state,started_at) VALUES('synthetic','run','paused','2026-09-28')")
                    db.commit()
                p.assistant_mail_sources_init_schema()
                with closing(restored_db()) as db:
                    actual={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    self.assertTrue(set(TABLES) <= actual)
                    self.assertEqual(db.execute('SELECT state FROM assistent_mailquellen_laeufe').fetchone()[0],'paused')
        self.assertEqual(len(list(p.app.url_map.iter_rules())),routes_before)

    def adapted_db(self, statements):
        sqlite = self.original_get_db()
        pg = p.PostgresConnection(types.SimpleNamespace(cursor=lambda: PgCursorOnSqlite(sqlite, statements)))

        def execute(sql, params=()):
            if sql.lstrip().startswith(('INSERT INTO assistent_profile(', 'INSERT INTO assistent_rechte(')):
                return pg.execute(sql, params)
            return sqlite.execute(sql, params)

        return types.SimpleNamespace(execute=execute, commit=sqlite.commit, rollback=sqlite.rollback, close=sqlite.close)

    def test_profile_insert_and_update_use_actual_natural_key_through_pg_adapter(self):
        self.original_get_db = p.get_db
        statements = []
        with patch.object(p, 'get_db', side_effect=lambda: self.adapted_db(statements)):
            for data in ({'name':'First synthetic avatar','character':'mila'}, {'name':'Updated synthetic avatar'}):
                result = self.post('/profil', dict(data, stil='ruhig', stimme='coral', avatar='blau'))
                self.assertEqual(result.status_code, 200)
        self.assertEqual(len(statements), 2)
        self.assertTrue(all(sql.endswith('RETURNING actor') for sql in statements))
        with fixture.database() as db:
            row = db.execute('SELECT actor,name,character FROM assistent_profile').fetchone()
        self.assertEqual((row['actor'], row['name'], row['character']), ('mitarbeiter:1', 'Updated synthetic avatar', 'mila'))

    def test_character_picker_insert_update_and_backup_preserve_natural_key(self):
        self.original_get_db=p.get_db
        statements=[]
        with patch.object(p,'get_db',side_effect=lambda:self.adapted_db(statements)):
            for character in ('robot','mila'):
                self.assertEqual(self.post('/avatar',{'character':character}).json,{'ok':True,'character':character})
        self.assertEqual(len(statements),2)
        self.assertTrue(all(sql.endswith('RETURNING actor') for sql in statements))
        self.assertIn('assistent_profile',p.BACKUP_TABLES)
        with fixture.database() as db:
            rows,refs,size=p.write_table_rows_and_binary_blobs(db,None,'assistent_profile')
        self.assertEqual((rows[0]['actor'],rows[0]['name'],rows[0]['character']),('mitarbeiter:1','Chris','mila'))
        self.assertEqual((refs,size),([],0))

    def test_rights_insert_and_update_use_employee_key_through_pg_adapter(self):
        with fixture.database() as db:
            db.execute('DELETE FROM assistent_rechte')
        admin = self.make_client(admin=True)
        self.original_get_db = p.get_db
        statements = []
        with patch.object(p, 'get_db', side_effect=lambda: self.adapted_db(statements)):
            for password in ('synthetic-password-123', ''):
                result = admin.post('/werkstatt/assistent/rechte', data={
                    'csrf_token': 'test-csrf', 'mitarbeiter_id': '1', 'password': password,
                    'lesen': 'on', 'limit': '0',
                })
                self.assertEqual(result.status_code, 200)
        self.assertEqual(len(statements), 2)
        self.assertTrue(all(sql.endswith('RETURNING mitarbeiter_id') for sql in statements))
        with fixture.database() as db:
            row = db.execute('SELECT mitarbeiter_id,version,lesen FROM assistent_rechte').fetchone()
        self.assertEqual((row['mitarbeiter_id'], row['version'], row['lesen']), (1, 2, 1))

    def test_restore_schema_hook_is_idempotent_and_does_not_reregister_routes(self):
        routes_before = len(list(p.app.url_map.iter_rules()))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'synthetic-restored.db'
            def restored_db():
                db=sqlite3.connect(path)
                db.row_factory=sqlite3.Row
                return db
            with patch.object(p, 'get_db', side_effect=restored_db):
                p.assistant_init_schema()
                db = sqlite3.connect(path)
                try:
                    db.execute("INSERT INTO assistent_profile(actor,name,stil,stimme) VALUES('admin','Saved avatar','ruhig','coral')")
                    db.commit()
                    p.assistant_init_schema()
                    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    self.assertTrue({'assistent_rechte', 'assistent_profile', 'assistent_aktionen', 'assistent_audit', 'assistent_dialog'} <= tables)
                    self.assertEqual(db.execute('SELECT name FROM assistent_profile').fetchone()[0], 'Saved avatar')
                    self.assertEqual(db.execute('SELECT character FROM assistent_profile').fetchone()[0], 'drache')
                finally:
                    db.close()
        self.assertEqual(len(list(p.app.url_map.iter_rules())), routes_before)

    def test_restore_migrates_old_profile_and_preserves_existing_character(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'legacy-profile.db'
            def restored_db():
                db=sqlite3.connect(path)
                db.row_factory=sqlite3.Row
                return db
            with closing(restored_db()) as db:
                db.execute('CREATE TABLE assistent_profile(actor TEXT PRIMARY KEY,name TEXT NOT NULL,stil TEXT NOT NULL,stimme TEXT NOT NULL)')
                db.execute("INSERT INTO assistent_profile VALUES('mitarbeiter:1','Existing','knapp','ash')")
                db.commit()
            with patch.object(p,'get_db',side_effect=restored_db):
                p.assistant_init_schema()
                with closing(restored_db()) as db:
                    self.assertEqual(tuple(db.execute('SELECT name,stil,stimme,character FROM assistent_profile').fetchone()),('Existing','knapp','ash','drache'))
                    db.execute("UPDATE assistent_profile SET character='mila'")
                    db.commit()
                p.assistant_init_schema()
                with closing(restored_db()) as db:
                    self.assertEqual(db.execute('SELECT character FROM assistent_profile').fetchone()[0],'mila')


if __name__ == '__main__':
    unittest.main(verbosity=2)
