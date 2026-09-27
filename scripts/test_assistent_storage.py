"""Offline regressions for natural-key PG inserts and the restore schema hook."""
import sqlite3
import tempfile
import types
import unittest
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
            for name in ('First synthetic avatar', 'Updated synthetic avatar'):
                result = self.post('/profil', {'name': name, 'stil': 'ruhig', 'stimme': 'coral', 'avatar': 'blau'})
                self.assertEqual(result.status_code, 200)
        self.assertEqual(len(statements), 2)
        self.assertTrue(all(sql.endswith('RETURNING actor') for sql in statements))
        with fixture.database() as db:
            row = db.execute('SELECT actor,name FROM assistent_profile').fetchone()
        self.assertEqual((row['actor'], row['name']), ('mitarbeiter:1', 'Updated synthetic avatar'))

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
            with patch.object(p, 'get_db', side_effect=lambda: sqlite3.connect(path)):
                p.assistant_init_schema()
                db = sqlite3.connect(path)
                try:
                    db.execute("INSERT INTO assistent_profile(actor,name,stil,stimme) VALUES('admin','Saved avatar','ruhig','coral')")
                    db.commit()
                    p.assistant_init_schema()
                    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                    self.assertTrue({'assistent_rechte', 'assistent_profile', 'assistent_aktionen', 'assistent_audit', 'assistent_dialog'} <= tables)
                    self.assertEqual(db.execute('SELECT name FROM assistent_profile').fetchone()[0], 'Saved avatar')
                finally:
                    db.close()
        self.assertEqual(len(list(p.app.url_map.iter_rules())), routes_before)


if __name__ == '__main__':
    unittest.main(verbosity=2)
