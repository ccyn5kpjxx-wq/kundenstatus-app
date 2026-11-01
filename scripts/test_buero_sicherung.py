"""Synthetic restore tests: office actors keep their identity and login state."""
from contextlib import closing
from copy import deepcopy
import hashlib
from pathlib import Path
import sqlite3
import sys
import tempfile
from threading import RLock
from types import SimpleNamespace
from unittest import TestCase, main

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_buero_zugang import OfficeAccess
from werkstatt_buero_bereiche import OfficeSections
from werkstatt_buero_belege import OfficeDocuments
from werkstatt_buero_lexware import OfficeLexware
from werkstatt_buero_posteingang import OfficeInbox
from werkstatt_buero_lohn import OfficePayroll
from werkstatt_buero_sicherung import TABLES, ensure_office_state_for_import


class OfficeRestoreTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        self.path = self.folder / 'current.sqlite'
        self.lock = RLock()
        self.serial = 0

        def database():
            db = sqlite3.connect(self.path)
            db.row_factory = sqlite3.Row
            return db

        self.p = SimpleNamespace(get_db=database, portal_originals_operation_lock=lambda: self.lock)
        with closing(database()) as db:
            db.executescript('''
                CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT NOT NULL,aktiv INTEGER NOT NULL,telefon TEXT);
                CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,passwort_hash TEXT NOT NULL,
                    version INTEGER NOT NULL,auth_version INTEGER NOT NULL,lesen INTEGER NOT NULL,
                    dokumentieren INTEGER NOT NULL,einkaufen INTEGER NOT NULL,limit_cent INTEGER NOT NULL);
            ''')
            for mid in range(1, 7):
                db.execute('INSERT INTO mitarbeiter VALUES(?,?,1,?)', (mid, 'Synthetic employee ' + str(mid), '000'))
                db.execute('INSERT INTO assistent_rechte VALUES(?,?,1,1,1,0,0,0)', (mid, 'synthetic-auth-secret-' + str(mid)))
            db.commit()
        for cls in (OfficeAccess, OfficeSections, OfficeDocuments, OfficeLexware, OfficeInbox, OfficePayroll):
            cls(self.p).init_schema()
        with closing(database()) as db:
            for mid, active in ((1, 1), (5, 0)):
                db.execute('''INSERT INTO buero_zugaenge
                    (mitarbeiter_id,aktiv,kategorien_json,version,rechte_version,auth_version,geaendert_am,geaendert_von)
                    VALUES(?,?,'["belege"]',1,1,1,'2026-11-01','admin')''', (mid, active))
            # Author, audit actor and reader deliberately have no office grant:
            # historical actor ownership must survive a later grant deletion too.
            db.execute('''INSERT INTO buero_belege
                (id,typ,filename,mime,size_bytes,sha256,dedup_key,original_base64,created_by,created_at,updated_at)
                VALUES(?,'post','synthetic.pdf','application/pdf',3,?,'datei:synthetic','YWJj','mitarbeiter:2','2026-11-01','2026-11-01')''',
                ('a' * 32, hashlib.sha256(b'abc').hexdigest()))
            db.execute('''INSERT INTO buero_belege_audit VALUES(?,?,'mitarbeiter:3','erfasst','{}','2026-11-01')''', ('b' * 32, 'a' * 32))
            db.execute('''INSERT INTO buero_belege_seen VALUES(?,?,'mitarbeiter:4',1,'2026-11-01')''', ('c' * 32, 'a' * 32))
            db.commit()

    def snapshot(self):
        with closing(self.p.get_db()) as db:
            return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table).fetchall()]
                               for table in (*TABLES, 'mitarbeiter', 'assistent_rechte')}}

    def sqlite_copy(self, sql='', params=()):
        self.serial += 1
        path = self.folder / ('incoming-' + str(self.serial) + '.sqlite')
        with closing(self.p.get_db()) as source, closing(sqlite3.connect(path)) as target:
            source.backup(target)
            if sql:
                target.execute(sql, params)
                target.commit()
        return path

    def reject_json(self, export):
        with self.assertRaises(ValueError) as error:
            ensure_office_state_for_import(self.p, export=export)
        self.assertNotIn('synthetic-auth-secret', str(error.exception))
        self.assertNotIn('private-import-secret', str(error.exception))

    def test_matching_json_keeps_external_connection_open_and_performs_no_write(self):
        before = self.snapshot()
        with closing(self.p.get_db()) as target:
            ensure_office_state_for_import(self.p, export=before, target=target)
            self.assertEqual(target.execute('SELECT COUNT(*) FROM buero_belege').fetchone()[0], 1)
        self.assertEqual(self.snapshot(), before)

    def test_sqlite_is_authoritative_read_only_source_even_with_conflicting_json(self):
        path = self.sqlite_copy()
        before = path.read_bytes()
        conflicting = self.snapshot()
        conflicting['tables']['mitarbeiter'][0]['name'] = 'private-import-secret'
        ensure_office_state_for_import(self.p, export=conflicting, imported_db=path)
        self.assertEqual(path.read_bytes(), before)

    def test_json_rejects_changed_deleted_or_reused_grantee_identity(self):
        original = self.snapshot()
        for mid in (1, 5):
            for field, value in (('id', 101), ('name', 'private-import-secret'), ('aktiv', 0)):
                with self.subTest(mid=mid, field=field):
                    export = deepcopy(original)
                    next(row for row in export['tables']['mitarbeiter'] if row['id'] == mid)[field] = value
                    self.reject_json(export)
            export = deepcopy(original)
            export['tables']['mitarbeiter'] = [row for row in export['tables']['mitarbeiter'] if row['id'] != mid]
            self.reject_json(export)
        self.assertEqual(self.snapshot(), original)

    def test_json_rejects_all_login_changes_missing_rights_and_ambiguous_employee_rows(self):
        original = self.snapshot()
        for field, value in (('passwort_hash', 'private-import-secret'), ('version', 2), ('auth_version', 2),
                             ('lesen', 0), ('dokumentieren', 1), ('einkaufen', 1), ('limit_cent', 100)):
            with self.subTest(field=field):
                export = deepcopy(original)
                export['tables']['assistent_rechte'][0][field] = value
                self.reject_json(export)
        for table, key in (('mitarbeiter', 'id'), ('assistent_rechte', 'mitarbeiter_id')):
            for mode in ('missing', 'duplicate', 'boolean'):
                with self.subTest(table=table, mode=mode):
                    export = deepcopy(original)
                    row = next(row for row in export['tables'][table] if row[key] == 1)
                    if mode == 'missing': export['tables'][table].remove(row)
                    elif mode == 'duplicate': export['tables'][table].append(deepcopy(row))
                    else: row[key] = True
                    self.reject_json(export)

    def test_sqlite_rejects_identity_and_full_auth_changes_without_mutating_either_database(self):
        original = self.snapshot()
        mutations = [
            ("UPDATE mitarbeiter SET name='private-import-secret' WHERE id=1", ()),
            ('UPDATE mitarbeiter SET aktiv=0 WHERE id=1', ()),
            ('UPDATE mitarbeiter SET id=101 WHERE id=1', ()),
            ('DELETE FROM mitarbeiter WHERE id=1', ()),
            ("UPDATE assistent_rechte SET passwort_hash='private-import-secret' WHERE mitarbeiter_id=1", ()),
            ('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1', ()),
            ('UPDATE assistent_rechte SET mitarbeiter_id=101 WHERE mitarbeiter_id=1', ()),
            ('DELETE FROM assistent_rechte WHERE mitarbeiter_id=1', ()),
        ]
        for sql, params in mutations:
            with self.subTest(sql=sql):
                path = self.sqlite_copy(sql, params)
                source_bytes = path.read_bytes()
                with self.assertRaises(ValueError) as error:
                    ensure_office_state_for_import(self.p, imported_db=path)
                self.assertNotIn('private-import-secret', str(error.exception))
                self.assertEqual(path.read_bytes(), source_bytes)
                self.assertEqual(self.snapshot(), original)

    def test_receipt_author_audit_actor_and_reader_stay_bound_without_a_grant(self):
        original = self.snapshot()
        for mid in (2, 3, 4):
            with self.subTest(mid=mid):
                export = deepcopy(original)
                next(row for row in export['tables']['mitarbeiter'] if row['id'] == mid)['name'] = 'Other synthetic person'
                self.reject_json(export)
                path = self.sqlite_copy('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=?', (mid,))
                with self.assertRaises(ValueError):
                    ensure_office_state_for_import(self.p, imported_db=path)

    def test_deleted_historical_actor_and_login_must_remain_absent(self):
        with closing(self.p.get_db()) as db:
            db.execute('DELETE FROM assistent_rechte WHERE mitarbeiter_id=2')
            db.execute('DELETE FROM mitarbeiter WHERE id=2')
            db.commit()
        export = self.snapshot()
        ensure_office_state_for_import(self.p, export=export)
        ensure_office_state_for_import(self.p, imported_db=self.sqlite_copy())
        for table, row in (
                ('mitarbeiter', {'id': 2, 'name': 'Other synthetic person', 'aktiv': 1, 'telefon': '000'}),
                ('assistent_rechte', {'mitarbeiter_id': 2, 'passwort_hash': 'private-import-secret', 'version': 1,
                                     'auth_version': 1, 'lesen': 1, 'dokumentieren': 0, 'einkaufen': 0, 'limit_cent': 0})):
            with self.subTest(table=table):
                changed = deepcopy(export)
                changed['tables'][table].append(row)
                self.reject_json(changed)
        path = self.sqlite_copy("INSERT INTO mitarbeiter VALUES(2,'Other synthetic person',1,'000')")
        with self.assertRaises(ValueError):
            ensure_office_state_for_import(self.p, imported_db=path)

    def test_unrelated_employee_and_nonidentity_fields_do_not_block_restore(self):
        export = self.snapshot()
        export['tables']['mitarbeiter'][0]['telefon'] = '111'
        export['tables']['mitarbeiter'][5]['name'] = 'Unrelated synthetic person'
        export['tables']['assistent_rechte'][5]['passwort_hash'] = 'different-unrelated-secret'
        ensure_office_state_for_import(self.p, export=export)

    def test_deleted_actor_id_cannot_be_revived_through_json_type_coercion(self):
        original = self.snapshot()
        with closing(self.p.get_db()) as db:
            db.execute('DELETE FROM assistent_rechte WHERE mitarbeiter_id=2')
            db.execute('DELETE FROM mitarbeiter WHERE id=2')
            db.commit()
        current = self.snapshot()
        for table, key in (('mitarbeiter', 'id'), ('assistent_rechte', 'mitarbeiter_id')):
            row = next(row for row in original['tables'][table] if row[key] == 2)
            for disguised in ('2', '02', ' 2 ', 2.0, True, None):
                with self.subTest(table=table, disguised=disguised):
                    export = deepcopy(current)
                    export['tables'][table].append(dict(row, **{key: disguised}))
                    self.reject_json(export)
        self.assertEqual(self.snapshot(), current)

    def test_legacy_restore_without_any_office_state_needs_no_employee_snapshot(self):
        with closing(self.p.get_db()) as db:
            for table in reversed(TABLES): db.execute('DELETE FROM ' + table)
            db.commit()
        ensure_office_state_for_import(self.p, export={'tables': {}})


if __name__ == '__main__':
    main()
