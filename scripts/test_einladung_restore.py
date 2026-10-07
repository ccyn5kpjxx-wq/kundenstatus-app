"""One-time invitation restore regressions on temporary synthetic databases."""
import ast
import copy
import io
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_einladung_restore import ensure_employee_invitation_state_for_import


SCHEMA = '''
CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT,aktiv INTEGER,rolle TEXT);
CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,passwort_hash TEXT,
 lesen INTEGER,dokumentieren INTEGER,einkaufen INTEGER,limit_cent INTEGER,version INTEGER,auth_version INTEGER);
CREATE TABLE assistent_einladungen(mitarbeiter_id INTEGER PRIMARY KEY,token_hash TEXT UNIQUE,
 issued_at INTEGER,expires_at INTEGER,used_at INTEGER,rights_fingerprint TEXT);
'''


class InvitationRestoreTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'current.db'
        self.source_path = Path(temporary.name) / 'snapshot.db'
        self.p = SimpleNamespace(get_db=self.db, get_table_columns=self.columns)
        with self.db() as db:
            db.executescript(SCHEMA)
            db.execute("INSERT INTO mitarbeiter VALUES(7,'Synthetic employee',1,'Worker')")
            db.execute("INSERT INTO assistent_rechte VALUES(7,'',1,1,1,25000,4,1)")
            db.execute("INSERT INTO assistent_einladungen VALUES(7,'synthetic-token-hash',100,500,NULL,'synthetic-rights-fingerprint')")

    def db(self):
        db = sqlite3.connect(self.path); db.row_factory = sqlite3.Row
        self.addCleanup(db.close)
        return db

    @staticmethod
    def columns(db, table):
        return [row['name'] for row in db.execute('PRAGMA table_info(' + table + ')')]

    def snapshot(self):
        with self.db() as db:
            return {'tables': {name: [dict(row) for row in db.execute('SELECT * FROM ' + name)]
                              for name in ('mitarbeiter', 'assistent_rechte', 'assistent_einladungen')}}

    def source(self, export):
        with closing(sqlite3.connect(self.source_path)) as db, db:
            db.executescript(SCHEMA)
            for table, rows in export['tables'].items():
                for row in rows:
                    fields = list(row)
                    db.execute('INSERT INTO ' + table + '(' + ','.join(fields) + ') VALUES(' + ','.join('?' for _ in fields) + ')',
                               tuple(row[key] for key in fields))
        return self.source_path

    def redeem(self):
        with self.db() as db:
            db.execute('UPDATE assistent_einladungen SET used_at=200')
            db.execute("UPDATE assistent_rechte SET passwort_hash='synthetic-new-password-hash',auth_version=auth_version+1")

    def test_current_json_is_allowed_without_mutation_or_closing_borrowed_connection(self):
        with self.db() as db:
            ensure_employee_invitation_state_for_import(self.p, export=self.snapshot(), target=db)
            self.assertEqual(db.execute('SELECT auth_version FROM assistent_rechte').fetchone()[0], 1)

    def test_legacy_database_without_invitation_table_remains_restorable(self):
        with self.db() as db:
            db.execute('DROP TABLE assistent_einladungen')
        ensure_employee_invitation_state_for_import(self.p, export={})

    def test_empty_invitation_table_allows_legacy_snapshot(self):
        with self.db() as db:
            db.execute('DELETE FROM assistent_einladungen')
        ensure_employee_invitation_state_for_import(self.p, export={'tables': {}})

    def test_issued_invitation_cannot_disappear_even_without_backup_table_registration(self):
        for replacement in (None, []):
            snapshot = self.snapshot()
            if replacement is None: del snapshot['tables']['assistent_einladungen']
            else: snapshot['tables']['assistent_einladungen'] = replacement
            with self.assertRaisesRegex(ValueError, 'Datenimport gesperrt'):
                ensure_employee_invitation_state_for_import(self.p, export=snapshot)

    def test_pre_redemption_snapshot_cannot_revive_token_or_previous_password(self):
        previous = self.snapshot(); self.redeem()
        with self.assertRaises(ValueError):
            ensure_employee_invitation_state_for_import(self.p, export=previous)
        current = self.snapshot()
        self.assertEqual(current['tables']['assistent_einladungen'][0]['used_at'], 200)
        self.assertEqual(current['tables']['assistent_rechte'][0]['auth_version'], 2)
        ensure_employee_invitation_state_for_import(self.p, export=current)

    def test_superseded_invitation_cannot_become_valid_again(self):
        previous = self.snapshot()
        with self.db() as db:
            db.execute("UPDATE assistent_einladungen SET token_hash='synthetic-reissued-hash',issued_at=200,expires_at=600")
        with self.assertRaises(ValueError):
            ensure_employee_invitation_state_for_import(self.p, export=previous)

    def test_every_invitation_field_including_nullable_consumption_is_protected(self):
        for column, replacement in (('token_hash','different'), ('issued_at',99), ('expires_at',999),
                                    ('used_at',200), ('rights_fingerprint','different')):
            snapshot = self.snapshot(); snapshot['tables']['assistent_einladungen'][0][column] = replacement
            with self.subTest(column=column), self.assertRaises(ValueError):
                ensure_employee_invitation_state_for_import(self.p, export=snapshot)
        snapshot = self.snapshot(); del snapshot['tables']['assistent_einladungen'][0]['used_at']
        with self.assertRaises(ValueError):
            ensure_employee_invitation_state_for_import(self.p, export=snapshot)

    def test_password_auth_version_rights_version_and_grants_cannot_roll_back(self):
        self.redeem()
        for column, replacement in (('passwort_hash',''), ('auth_version',1), ('version',3),
                                    ('lesen',0), ('einkaufen',0), ('limit_cent',50000)):
            snapshot = self.snapshot(); snapshot['tables']['assistent_rechte'][0][column] = replacement
            with self.subTest(column=column), self.assertRaises(ValueError):
                ensure_employee_invitation_state_for_import(self.p, export=snapshot)

    def test_missing_auth_version_is_not_silently_recreated_after_restore(self):
        snapshot = self.snapshot(); del snapshot['tables']['assistent_rechte'][0]['auth_version']
        with self.assertRaises(ValueError):
            ensure_employee_invitation_state_for_import(self.p, export=snapshot)

    def test_employee_identity_and_deactivation_are_protected_but_other_details_can_restore(self):
        for column, replacement in (('id',8), ('name','Different employee'), ('aktiv',0)):
            snapshot = self.snapshot(); snapshot['tables']['mitarbeiter'][0][column] = replacement
            with self.subTest(column=column), self.assertRaises(ValueError):
                ensure_employee_invitation_state_for_import(self.p, export=snapshot)
        snapshot = self.snapshot(); snapshot['tables']['mitarbeiter'][0]['rolle'] = 'Older descriptive role'
        ensure_employee_invitation_state_for_import(self.p, export=snapshot)

    def test_missing_duplicate_and_malformed_identity_rows_stop_before_import(self):
        for table in ('mitarbeiter', 'assistent_rechte', 'assistent_einladungen'):
            for malformed in ([], {}, ['not a row']):
                snapshot = self.snapshot(); snapshot['tables'][table] = malformed
                with self.subTest(table=table, malformed=malformed), self.assertRaises(ValueError):
                    ensure_employee_invitation_state_for_import(self.p, export=snapshot)
            snapshot = self.snapshot(); snapshot['tables'][table] *= 2
            with self.subTest(table=table, duplicate=True), self.assertRaises(ValueError):
                ensure_employee_invitation_state_for_import(self.p, export=snapshot)

    def test_actual_sqlite_file_wins_over_absent_or_contradictory_json(self):
        snapshot = self.snapshot(); path = self.source(snapshot)
        ensure_employee_invitation_state_for_import(self.p, imported_db=path, export={})
        wrong_json = copy.deepcopy(snapshot); wrong_json['tables']['assistent_einladungen'][0]['token_hash'] = 'not-the-imported-file'
        ensure_employee_invitation_state_for_import(self.p, imported_db=path, export=wrong_json)

    def test_old_sqlite_cannot_be_masked_by_current_json(self):
        path = self.source(self.snapshot()); self.redeem()
        with self.assertRaises(ValueError):
            ensure_employee_invitation_state_for_import(self.p, imported_db=path, export=self.snapshot())

    def test_sqlite_missing_table_or_old_auth_schema_is_blocked(self):
        path = self.source(self.snapshot())
        with closing(sqlite3.connect(path)) as db, db: db.execute('DROP TABLE assistent_einladungen')
        with self.assertRaises(ValueError):
            ensure_employee_invitation_state_for_import(self.p, imported_db=path)
        path.unlink(); path = self.source(self.snapshot())
        with closing(sqlite3.connect(path)) as db, db: db.execute('ALTER TABLE assistent_rechte DROP COLUMN auth_version')
        with self.assertRaises(ValueError):
            ensure_employee_invitation_state_for_import(self.p, imported_db=path)

    def test_invalid_sqlite_is_read_only_and_errors_do_not_expose_credentials(self):
        missing = self.source_path
        with self.assertRaises(ValueError) as error:
            ensure_employee_invitation_state_for_import(self.p, imported_db=missing)
        self.assertFalse(missing.exists())
        self.assertNotIn('synthetic-token-hash', str(error.exception))
        self.source_path.write_text('not a database', encoding='utf8')
        with self.assertRaises(ValueError):
            ensure_employee_invitation_state_for_import(self.p, imported_db=self.source_path)

    def test_guard_fences_destructive_import_before_any_delete(self):
        snapshot = self.snapshot(); self.redeem()
        with self.db() as db:
            with self.assertRaises(ValueError):
                ensure_employee_invitation_state_for_import(self.p, export=snapshot, target=db)
                db.execute('DELETE FROM assistent_einladungen')
            self.assertEqual(db.execute('SELECT used_at FROM assistent_einladungen').fetchone()[0], 200)


class ActualInvitationImportTests(unittest.TestCase):
    """Run the real guarded import entry points on the isolated AST fixture."""
    @classmethod
    def setUpClass(cls):
        import test_material_external_restore as fixture
        cls.fixture = fixture
        fixture.ExternalRestoreTests.setUpClass()

    def setUp(self):
        self.f = self.fixture.ExternalRestoreTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.ns = self.f.ns
        self.tables = (*self.fixture.TABLES, 'mitarbeiter', 'assistent_rechte', 'assistent_einladungen')
        self.ns['BACKUP_TABLES'] = self.tables
        with self.fixture.connection(self.f.db_path) as db:
            db.executescript(SCHEMA)
            db.execute("INSERT INTO mitarbeiter VALUES(7,'Synthetic employee',1,'Worker')")
            db.execute("INSERT INTO assistent_rechte VALUES(7,'',1,1,1,25000,4,1)")
            db.execute("INSERT INTO assistent_einladungen VALUES(7,'synthetic-token-hash',100,500,NULL,'synthetic-rights-fingerprint')")

    def snapshot(self):
        with self.fixture.connection(self.f.db_path) as db:
            return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)]
                               for table in self.tables}}

    def redeem(self):
        with self.ns['portal_originals_operation_lock'](), self.fixture.connection(self.f.db_path) as db:
            db.execute('UPDATE assistent_einladungen SET used_at=200')
            db.execute("UPDATE assistent_rechte SET passwort_hash='synthetic-new-password-hash',auth_version=2")

    def source(self, snapshot, filename):
        path = self.f.root / filename
        with self.fixture.connection(path) as db:
            db.executescript(self.fixture.SCHEMA + SCHEMA)
            for table, rows in snapshot['tables'].items():
                for row in rows:
                    db.execute('INSERT INTO ' + table + '(' + ','.join(row) + ') VALUES(' +
                               ','.join('?' for _ in row) + ')', tuple(row.values()))
        return path

    def test_actual_json_import_cannot_revive_consumed_link_but_current_backup_can_restore_other_data(self):
        old = self.snapshot(); self.redeem(); current = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'persönliche Einladungen'):
            self.ns['import_backup_json_rows_into_current_database'](old, None, [])
        self.assertEqual(self.snapshot(), current)
        current['tables']['ordinary'][0]['value'] = 'restored unrelated value'
        self.ns['import_backup_json_rows_into_current_database'](current, None, [])
        self.assertEqual(self.snapshot(), current)

    def test_actual_sqlite_row_import_rejects_old_auth_and_allows_matching_current_source(self):
        old_source = self.source(self.snapshot(), 'old.db'); self.redeem(); current = self.snapshot()
        with self.assertRaisesRegex(ValueError, 'persönliche Einladungen'):
            self.ns['import_sqlite_rows_into_current_database'](old_source)
        self.assertEqual(self.snapshot(), current)
        current['tables']['ordinary'][0]['value'] = 'sqlite unrelated value'
        self.ns['import_sqlite_rows_into_current_database'](self.source(current, 'current.db'))
        self.assertEqual(self.snapshot(), current)

    def test_outer_import_route_uses_real_sqlite_over_current_json_before_file_backup_or_replacement(self):
        old_source = self.source(self.snapshot(), 'route-old.db'); self.redeem(); before = self.snapshot()
        self.ns['extract_import_package_files'] = lambda *_: (old_source, None, before)
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, 'w') as archive:
            archive.writestr('backup.json', json.dumps(before))
            archive.writestr('database/auftraege.db', b'synthetic member; fixture returns actual source')
        payload.seek(0)
        response = self.f.client.post('/admin/daten-import', data={'datenpaket': (payload, 'backup.zip')})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.snapshot(), before)
        for name in ('create_backup_package', 'copy_sqlite_database_snapshot', 'replace_uploads_from_import', 'init_db'):
            self.ns[name].assert_not_called()
        with self.f.client.session_transaction() as session:
            self.assertTrue(any('persönliche Einladungen' in message for _, message in session['_flashes']))


class BackupInvitationCompatibilityTests(unittest.TestCase):
    def test_real_backup_validator_accepts_legacy_table_catalogue_before_invitations_exist(self):
        tree = ast.parse(Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf8'))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == 'validate_backup_binary_reference_completeness')
        assignment = next(node for node in tree.body if isinstance(node, ast.Assign)
                          and any(isinstance(target, ast.Name) and target.id == 'BACKUP_TABLES' for target in node.targets))
        tables = ast.literal_eval(assignment.value)
        namespace = {'BACKUP_TABLES': tables, 'BACKUP_BINARY_FIELDS': {},
                     'BACKUP_EXTERNALIZED_BINARY_FORMAT_VERSION': 2,
                     'clean_text': lambda value: str(value or '').strip()}
        exec(compile(ast.Module(body=[function], type_ignores=[]), '<actual-backup-validator>', 'exec'), namespace)
        legacy = {'format_version': 4, 'schema_features': [],
                  'tables': {name: [] for name in tables if name != 'assistent_einladungen'}}
        namespace['validate_backup_binary_reference_completeness'](legacy, {})


if __name__ == '__main__':
    unittest.main()
