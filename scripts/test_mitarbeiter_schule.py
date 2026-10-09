"""School reporting on synthetic SQLite only; never import the live app."""
import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from copy import deepcopy
from functools import wraps
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest

from flask import Flask, abort, session
from werkzeug.datastructures import MultiDict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_mitarbeiter_schule import register_school, TABLES
from werkstatt_mitarbeiter_portal import EmployeePortal, ensure_employee_private_state_for_import
from werkstatt_mitarbeiter_selfservice import EmployeeSelfService


class Portal:
    def __init__(self, folder):
        self.path = folder / 'synthetic.sqlite'
        self.app = Flask(__name__)
        self.app.config.update(TESTING=True, SECRET_KEY='isolated-school-test', ASSISTANT_NATIVE_COCKPIT=True)
        self.lock = threading.RLock()
        self.backups = []
        with closing(self.get_db()) as db:
            db.executescript('''CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT,aktiv INTEGER);
                INSERT INTO mitarbeiter VALUES(1,'Synthetic Own',1),(2,'Synthetic Other',1);
                CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,lesen INTEGER,version INTEGER,auth_version INTEGER,
                    dokumentieren INTEGER DEFAULT 0,einkaufen INTEGER DEFAULT 0,limit_cent INTEGER DEFAULT 0);
                INSERT INTO assistent_rechte(mitarbeiter_id,lesen,version,auth_version) VALUES(1,1,1,1),(2,1,1,1);
                CREATE TABLE mitarbeiter_urlaub(id INTEGER PRIMARY KEY,mitarbeiter_id INTEGER,start_datum TEXT,end_datum TEXT,notiz TEXT);
                CREATE TABLE mitarbeiter_zeitstempel(id INTEGER PRIMARY KEY,mitarbeiter_id INTEGER,aktion TEXT);
                INSERT INTO mitarbeiter_zeitstempel VALUES(1,1,'synthetic-unchanged');''')
        self.employee_portal = EmployeePortal.__new__(EmployeePortal)
        self.employee_portal.p = self
        self.assistant_selfservice = EmployeeSelfService(self)
        self.app.add_url_rule('/urlaub', endpoint='assistent.vacation_page', view_func=lambda: 'synthetic-vacation')
        self.app.add_url_rule('/urlaub/admin', endpoint='assistent.vacation_admin', view_func=lambda: 'synthetic-admin')
        self.school = register_school(self)

    def get_db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def get_table_columns(db, table):
        return [row['name'] for row in db.execute('PRAGMA table_info(' + table + ')').fetchall()]

    @contextmanager
    def portal_originals_operation_lock(self):
        with self.lock:
            yield

    def schedule_change_backup(self, reason):
        self.backups.append(reason)

    @staticmethod
    def admin_required(view):
        @wraps(view)
        def guarded(*args, **kwargs):
            if not session.get('admin'):
                abort(403)
            return view(*args, **kwargs)
        return guarded

    @staticmethod
    def bw_feiertage(year):
        return {}


class SchoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p = Portal(Path(self.temp.name))
        self.client = self.p.app.test_client()
        self.login(self.client)

    @staticmethod
    def login(client, mid=1):
        with client.session_transaction() as state:
            state.clear()
            state.update(assistent_mid=mid, assistent_version=1, assistent_auth_version=1, csrf_token='synthetic-csrf')

    def rows(self, table):
        with closing(self.p.get_db()) as db:
            return [dict(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY id').fetchall()]

    def update(self, sql, args=()):
        with closing(self.p.get_db()) as db:
            db.execute(sql, args)
            db.commit()

    @staticmethod
    def form(**changes):
        data = dict(von='2026-10-12', bis='', ganztag='', start_zeit='08:00', end_zeit='15:30',
                    notiz='Synthetic school', request_id='a' * 32, confirmed='ja', csrf_token='synthetic-csrf')
        data.update(changes)
        return data

    def create(self, client=None, **changes):
        return (client or self.client).post('/werkstatt/mein-konto/abwesenheiten/schule', data=self.form(**changes))

    def personal(self, client=None):
        with self.p.app.test_request_context():
            session.update(assistent_mid=1, assistent_version=1, assistent_auth_version=1)
            return self.p.school.personal({'actor': 'mitarbeiter:1'}, 2026)

    def snapshot(self):
        with closing(self.p.get_db()) as db:
            return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table).fetchall()]
                               for table in (*TABLES, 'mitarbeiter', 'assistent_rechte')}}

    def test_own_report_admin_visibility_and_no_vacation_or_time_change(self):
        before = {t: self.rows(t) for t in ('mitarbeiter_urlaub', 'mitarbeiter_zeitstempel')}
        annual = next(p for p in self.p.assistant_selfservice.admin_view({'actor': 'admin'}, 2026)['mitarbeiter'] if p['id'] == 1)
        self.p.assistant_selfservice.save_account({'actor': 'admin'}, 1, {
            'jahr': 2026, 'resttage': '10', 'stichtag': '2026-01-01', 'arbeitstage': [0, 1, 2, 3, 4],
            'feiertage': 'BW', 'confirmed': True, 'version': annual['basis']['version'], 'basis_token': annual['basis_token']})
        self.assertEqual(self.create().status_code, 303)
        own = self.personal()
        self.assertTrue(own['enabled'])
        self.assertEqual(own['eintraege'][0]['von'], '2026-10-12')
        self.assertEqual(own['eintraege'][0]['bis'], '2026-10-12')
        self.assertEqual(own['eintraege'][0]['status'], 'gemeldet')
        self.assertEqual({t: self.rows(t) for t in before}, before)
        admin = self.p.assistant_selfservice.admin_view({'actor': 'admin'}, 2026)
        own_admin = next(p for p in admin['mitarbeiter'] if p['id'] == 1)
        other_admin = next(p for p in admin['mitarbeiter'] if p['id'] == 2)
        self.assertEqual(own_admin['konto']['resttage'], '10')
        self.assertEqual(len(own_admin['schule']), 1)
        self.assertEqual(other_admin['schule'], [])
        self.assertEqual(self.rows(TABLES[1])[0]['aktion'], 'schule_gemeldet')

    def test_concurrent_replay_creates_one_row_and_one_audit(self):
        def submit(_):
            client = self.p.app.test_client()
            self.login(client)
            return self.create(client).status_code
        with ThreadPoolExecutor(max_workers=3) as pool:
            self.assertEqual(list(pool.map(submit, range(3))), [303] * 3)
        self.assertEqual(len(self.rows(TABLES[0])), 1)
        self.assertEqual(len(self.rows(TABLES[1])), 1)
        self.create(request_id='b' * 32)
        self.assertEqual(len(self.rows(TABLES[0])), 1)
        self.assertEqual(len(self.rows(TABLES[1])), 1)
        self.create(end_zeit='16:00')
        self.assertEqual(self.rows(TABLES[0])[0]['end_zeit'], '15:30')

    def test_owner_isolation_and_withdrawal_version(self):
        self.create()
        other = self.p.app.test_client()
        self.login(other, 2)
        self.create(other, request_id='b' * 32, notiz='Other private school')
        self.assertNotIn('Other private school', str(self.personal()))
        url = '/werkstatt/mein-konto/abwesenheiten/schule/' + 'a' * 32 + '/zurueckziehen'
        data = dict(csrf_token='synthetic-csrf', confirmed='ja', version=1)
        self.assertEqual(other.post(url, data=data).status_code, 403)
        self.client.post(url, data=dict(data, version=9))
        self.assertEqual(self.rows(TABLES[0])[0]['status'], 'gemeldet')
        self.assertEqual(self.client.post(url, data=data).status_code, 303)
        self.assertEqual(self.rows(TABLES[0])[0]['status'], 'zurueckgezogen')
        self.assertEqual(self.rows(TABLES[0])[0]['version'], 2)
        self.client.post(url, data=data)
        self.assertEqual(len(self.rows(TABLES[1])), 3)

    def test_csrf_confirmation_and_forged_target_never_write(self):
        for changes in ({'csrf_token': ''}, {'csrf_token': 'wrong'}, {'confirmed': ''}, {'mitarbeiter_id': '2'}, {'actor': 'admin'}):
            with self.subTest(changes=changes):
                self.assertEqual(self.create(**changes).status_code, 400)
        self.assertEqual(self.rows(TABLES[0]), [])
        duplicates = MultiDict(self.form())
        duplicates.add('von', '2026-10-13')
        self.assertEqual(self.client.post('/werkstatt/mein-konto/abwesenheiten/schule', data=duplicates).status_code, 400)
        csrf_copies = MultiDict(self.form())
        csrf_copies.add('csrf_token', 'wrong')
        self.assertEqual(self.client.post('/werkstatt/mein-konto/abwesenheiten/schule', data=csrf_copies).status_code, 400)
        csrf_copies.setlist('csrf_token', ['synthetic-csrf', 'synthetic-csrf'])
        self.assertEqual(self.client.post('/werkstatt/mein-konto/abwesenheiten/schule', data=csrf_copies).status_code, 303)
        self.assertEqual(len(self.rows(TABLES[0])), 1)

    def test_invalid_or_unknown_dates_and_times_never_write(self):
        for changes in ({'von': ''}, {'von': '2026-02-30'}, {'von': '20261012'}, {'von': '1999-10-12'},
                        {'bis': '2026-10-11'}, {'bis': '2028-10-12'}, {'bis': '2026-02-30'},
                        {'start_zeit': ''}, {'end_zeit': '24:00'}, {'end_zeit': '07:59'},
                        {'ganztag': 'on'}, {'ganztag': 'ja'}, {'notiz': 'x' * 301}, {'quelle': 'x' * 301}, {'quelle': 'Bad\nsource'}):
            with self.subTest(changes=changes):
                self.assertEqual(self.create(**changes).status_code, 303)
        self.assertEqual(self.rows(TABLES[0]), [])
        self.assertEqual(self.create(ganztag='ja', start_zeit='', end_zeit='').status_code, 303)
        self.assertEqual(self.rows(TABLES[0])[0]['ganztag'], 1)

    def test_multiday_plan_and_year_boundary_visibility(self):
        self.assertEqual(self.create(von='2026-12-30', bis='2027-01-02').status_code, 303)
        for year in (2026, 2027):
            self.assertEqual(len(self.p.school.admin_rows({'actor': 'admin'}, year)), 1)
        row = self.rows(TABLES[0])[0]
        self.assertEqual((row['von'], row['bis']), ('2026-12-30', '2027-01-02'))

    def test_school_overlap_blocks_but_adjacent_time_windows_are_allowed(self):
        self.create(bis='2026-10-14', start_zeit='08:00', end_zeit='12:00')
        self.create(request_id='b' * 32, von='2026-10-13', start_zeit='11:59', end_zeit='15:00', notiz='Different lesson')
        self.assertEqual(len(self.rows(TABLES[0])), 1)
        self.create(request_id='c' * 32, von='2026-10-13', ganztag='ja', start_zeit='', end_zeit='')
        self.assertEqual(len(self.rows(TABLES[0])), 1)
        self.create(request_id='d' * 32, von='2026-10-13', start_zeit='12:00', end_zeit='15:00')
        self.assertEqual(len(self.rows(TABLES[0])), 2)
        self.assertEqual(len(self.rows(TABLES[1])), 2)

    def test_active_rights_and_both_session_versions_rechecked(self):
        for sql in ('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1',
                    'UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1',
                    'UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1',
                    'UPDATE mitarbeiter SET aktiv=0 WHERE id=1'):
            self.update(sql)
            self.assertEqual(self.create().status_code, 403)
            self.update('UPDATE assistent_rechte SET lesen=1,version=1,auth_version=1 WHERE mitarbeiter_id=1')
            self.update('UPDATE mitarbeiter SET aktiv=1 WHERE id=1')
        self.assertEqual(self.rows(TABLES[0]), [])
        with self.client.session_transaction() as state:
            state.clear(); state.update(admin=True, csrf_token='synthetic-csrf')
        self.assertEqual(self.create().status_code, 403)
        self.assertEqual(self.rows(TABLES[1]), [])

    def test_admin_plan_has_real_admin_actor_and_own_target_binding(self):
        admin = self.p.app.test_client()
        with admin.session_transaction() as state:
            state.update(admin=True, csrf_token='synthetic-csrf')
        url = '/admin/mitarbeiter/1/schule'
        self.assertEqual(self.client.post(url, data=self.form()).status_code, 403)
        for changes in ({'csrf_token': ''}, {'confirmed': ''}, {'mitarbeiter_id': '2'}, {'actor': 'mitarbeiter:1'}):
            self.assertEqual(admin.post(url, data=self.form(**changes)).status_code, 400)
        self.assertEqual(admin.post(url, data=self.form(quelle='Synthetic invitation')).status_code, 303)
        record = self.rows(TABLES[0])[0]
        self.assertEqual((record['mitarbeiter_id'], record['quelle']), (1, 'Synthetic invitation'))
        audit = self.rows(TABLES[1])[0]
        self.assertEqual(audit['actor'], 'admin')
        self.assertIn('Synthetic invitation', audit['details'])
        data = dict(csrf_token='synthetic-csrf', confirmed='ja', version=1)
        path = '/admin/mitarbeiter/2/schule/' + 'a' * 32 + '/zurueckziehen'
        self.assertEqual(admin.post(path, data=data).status_code, 403)
        path = path.replace('/2/', '/1/')
        self.assertEqual(self.client.post(path, data=data).status_code, 403)
        self.assertEqual(admin.post(path, data=dict(data, version=9)).status_code, 303)
        self.assertEqual(self.rows(TABLES[0])[0]['status'], 'gemeldet')
        self.assertEqual(admin.post(path, data=data).status_code, 303)
        self.assertEqual(self.rows(TABLES[0])[0]['status'], 'zurueckgezogen')
        self.assertEqual(len(self.rows(TABLES[1])), 2)
        self.assertTrue(all(row['actor'] == 'admin' for row in self.rows(TABLES[1])))
        self.assertIn('#mitarbeiter-1', admin.post(url, data=self.form(request_id='b' * 32)).headers['Location'])

    def test_admin_cannot_create_for_inactive_or_unknown_employee(self):
        admin = self.p.app.test_client()
        with admin.session_transaction() as state:
            state.update(admin=True, csrf_token='synthetic-csrf')
        self.update('UPDATE mitarbeiter SET aktiv=0 WHERE id=1')
        for mid in (1, 99):
            self.assertEqual(admin.post(f'/admin/mitarbeiter/{mid}/schule', data=self.form()).status_code, 303)
        self.assertEqual(self.rows(TABLES[0]), [])
        self.assertEqual(self.rows(TABLES[1]), [])

    def test_restore_preserves_records_audit_owner_and_access(self):
        # An empty installation may restore a legacy package without school.
        ensure_employee_private_state_for_import(self.p, export={'tables': {}})
        self.create()
        export = self.snapshot()
        ensure_employee_private_state_for_import(self.p, export=export)
        for table, column, value in ((TABLES[0], 'status', 'zurueckgezogen'), (TABLES[0], 'mitarbeiter_id', 2),
                                     (TABLES[1], 'aktion', 'wrong'), ('assistent_rechte', 'auth_version', 0),
                                     ('mitarbeiter', 'name', 'Wrong owner')):
            changed = deepcopy(export)
            changed['tables'][table][0][column] = value
            with self.subTest(table=table, column=column), self.assertRaises(ValueError):
                ensure_employee_private_state_for_import(self.p, export=changed)
        with self.assertRaises(ValueError):
            ensure_employee_private_state_for_import(self.p, export={'tables': {}})
        path = Path(self.temp.name) / 'restore.sqlite'
        with closing(self.p.get_db()) as source, closing(sqlite3.connect(path)) as target:
            source.backup(target)
        ensure_employee_private_state_for_import(self.p, imported_db=path)
        with closing(sqlite3.connect(path)) as source:
            source.execute('DELETE FROM mitarbeiter_schule_audit'); source.commit()
        with self.assertRaises(ValueError):
            ensure_employee_private_state_for_import(self.p, imported_db=path)

    def test_backup_and_post_restore_schema_integration_without_app_import(self):
        source = (Path(__file__).resolve().parents[1] / 'app.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        assignment = next(n for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'BACKUP_TABLES' for t in n.targets))
        self.assertTrue(set(TABLES).issubset(ast.literal_eval(assignment.value)))
        self.assertIn('"employee_school_init_schema"', source)
        self.p.school.init_schema()
        self.assertEqual(self.rows(TABLES[0]), [])


if __name__ == '__main__':
    unittest.main()
