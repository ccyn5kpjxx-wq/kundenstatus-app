"""Offline leave workflow tests: synthetic people, isolated SQLite, no live app."""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import date
from functools import wraps
from pathlib import Path
import json
import ast
import re
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from flask import Flask, Blueprint, abort, session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_mitarbeiter_selfservice import EmployeeSelfService, register_selfservice, TABLES

ADMIN = {'actor': 'admin'}
PERSON = {'actor': 'mitarbeiter:1', 'mitarbeiter_id': 1, 'lesen': 1}
OTHER = {'actor': 'mitarbeiter:2', 'mitarbeiter_id': 2, 'lesen': 1}


class Portal:
    def __init__(self, root):
        self.path = root / 'test.sqlite'
        self.app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'templates'))
        self.app.config.update(TESTING=True, SECRET_KEY='synthetic-only')
        self.backups = []
        with closing(self.get_db()) as db:
            db.executescript('''CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT,aktiv INTEGER,urlaubsanspruch TEXT,arbeitszeit TEXT);
            INSERT INTO mitarbeiter VALUES(1,'Synthetic Own',1,'30 Tage','40 Stunden');
            INSERT INTO mitarbeiter VALUES(2,'Synthetic Other',1,'private-other-field','private-other-hours');
            CREATE TABLE mitarbeiter_urlaub(id INTEGER PRIMARY KEY AUTOINCREMENT,mitarbeiter_id INTEGER,start_datum TEXT,end_datum TEXT,notiz TEXT,erstellt_am TEXT,geaendert_am TEXT);''')

    def get_db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def schedule_change_backup(self, reason):
        self.backups.append(reason)

    @staticmethod
    def bw_feiertage(year):
        return {date(year, 10, 3): 'Synthetic holiday', date(year, 12, 25): 'Synthetic holiday'}

    @staticmethod
    def admin_required(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if not session.get('admin'):
                abort(403)
            return fn(*args, **kwargs)
        return wrapped


class LeaveFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p = Portal(Path(self.temp.name))
        self.now = patch('werkstatt_mitarbeiter_selfservice._today', return_value=date(2026, 9, 29))
        self.now.start(); self.addCleanup(self.now.stop)
        self.s = EmployeeSelfService(self.p)

    def rows(self, sql, args=()):
        db = self.p.get_db()
        try:
            return [dict(r) for r in db.execute(sql, args).fetchall()]
        finally:
            db.close()

    def update(self, sql, args=()):
        with closing(self.p.get_db()) as db:
            db.execute(sql, args)
            db.commit()

    def account(self, mid=1, **changes):
        person = next(x for x in self.s.admin_view(ADMIN, 2026)['mitarbeiter'] if x['id'] == mid)
        data = {'jahr': 2026, 'resttage': '10', 'stichtag': '2026-09-29', 'arbeitstage': [0,1,2,3,4],
                'feiertage': 'BW', 'confirmed': True, 'version': person['basis']['version'], 'basis_token': person['basis_token']}
        data.update(changes)
        self.s.save_account(ADMIN, mid, data)
        return data

    def apply(self, who=PERSON, start='2026-10-05', end='2026-10-09', key='synthetic-request-0001'):
        return self.s.apply(who, start, end, key)

    def approve(self, item, **changes):
        args = {'version': item['version'], 'account_version': self.s.summary(PERSON, 2026).get('konto_version')}
        args.update(changes)
        self.s.review(ADMIN, item['id'], 'genehmigt', **args)


class LeaveTests(LeaveFixture):
    def test_unknown_legacy_text_never_becomes_balance_and_request_not_approval(self):
        summary = self.s.summary(PERSON, 2026)
        self.assertFalse(summary['bekannt']); self.assertIsNone(summary['resttage'])
        preview = self.s.preview(PERSON, '2026-10-05', '2026-10-09')
        self.assertIsNone(preview['tage'])
        item = self.apply()
        self.assertEqual(item['status'], 'beantragt')
        self.assertIsNone(item['tage'])
        self.assertEqual(self.rows('SELECT * FROM mitarbeiter_urlaub'), [])
        with self.assertRaisesRegex(ValueError, 'Resturlaubskonto'):
            self.approve(item)
        self.assertEqual(self.rows('SELECT status FROM mitarbeiter_urlaubsantraege')[0]['status'], 'beantragt')

    def test_scope_admin_not_self_other_employee_and_revoked_active(self):
        self.apply(OTHER, key='synthetic-other-request')
        own = json.dumps(self.s.summary(PERSON, 2026))
        self.assertNotIn('private-other', own); self.assertNotIn('synthetic-other', own)
        self.assertEqual(self.s.summary(PERSON, 2026)['antraege'], [])
        for who in (ADMIN, {}, dict(PERSON, actor='mitarbeiter:2'), dict(PERSON, lesen=0)):
            with self.assertRaises(PermissionError): self.s.summary(who, 2026)
            with self.assertRaises(PermissionError): self.apply(who)
        self.update('UPDATE mitarbeiter SET aktiv=0 WHERE id=1')
        with self.assertRaises(PermissionError): self.s.summary(PERSON, 2026)

    def test_workdays_holidays_and_atomic_approval_one_debit(self):
        self.account(arbeitstage=[0,1,2,3,4,5])
        preview = self.s.preview(PERSON, '2026-10-02', '2026-10-05')
        self.assertEqual(preview['tage'], '2')  # Friday + Monday, explicit Saturday holiday excluded.
        item = self.apply(start='2026-10-02', end='2026-10-05')
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '10')
        self.approve(item)
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '8')
        self.assertEqual(len(self.rows('SELECT * FROM mitarbeiter_urlaub')), 1)
        self.assertEqual(self.s.summary(PERSON, 2026)['antraege'][0]['status'], 'genehmigt')
        self.assertEqual(self.s.summary(PERSON, 2026)['kalendereintraege'], [])
        with self.assertRaises(ValueError): self.approve(item)
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '8')

    def test_deduplicated_request_and_different_payload_fail_closed(self):
        one = self.apply()
        self.assertEqual(self.apply()['id'], one['id'])
        with self.assertRaises(ValueError): self.apply(end='2026-10-08')
        with self.assertRaises(ValueError): self.apply(key='another-request-00001')
        self.assertEqual(len(self.rows('SELECT * FROM mitarbeiter_urlaubsantraege')), 1)

    def test_withdraw_only_own_pending_and_no_calendar_or_debit(self):
        item = self.apply()
        with self.assertRaises(ValueError): self.s.withdraw(OTHER, item['id'], 1)
        self.s.withdraw(PERSON, item['id'], 1)
        self.assertEqual(self.s.summary(PERSON, 2026)['antraege'][0]['status'], 'zurueckgezogen')
        with self.assertRaises(ValueError): self.s.review(ADMIN, item['id'], 'genehmigt', 1)
        self.assertEqual(self.rows('SELECT * FROM mitarbeiter_urlaub'), [])

    def test_baseline_old_future_calendar_included_then_new_approval_only(self):
        self.update("INSERT INTO mitarbeiter_urlaub(mitarbeiter_id,start_datum,end_datum,notiz) VALUES(1,'02.11.2026','06.11.2026','Previously considered')")
        self.account()
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '10')
        item = self.apply(); self.approve(item)
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '5')
        self.account(resttage='5')
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '5', 'New reviewed balance already includes approved request')
        self.update("UPDATE mitarbeiter_urlaub SET end_datum='07.11.2026' WHERE notiz='Previously considered'")
        self.assertFalse(self.s.summary(PERSON, 2026)['bekannt'])

    def test_stale_ui_snapshot_and_new_legacy_period_make_account_unknown(self):
        person = self.s.admin_view(ADMIN, 2026)['mitarbeiter'][0]
        self.update("INSERT INTO mitarbeiter_urlaub(mitarbeiter_id,start_datum,end_datum,notiz) VALUES(1,'02.11.2026','06.11.2026','Synthetic')")
        with self.assertRaisesRegex(ValueError, 'seit dem Öffnen'):
            self.account(basis_token=person['basis_token'])
        self.account()
        self.update("INSERT INTO mitarbeiter_urlaub(mitarbeiter_id,start_datum,end_datum,notiz) VALUES(1,'09.11.2026','10.11.2026','Synthetic')")
        self.assertFalse(self.s.summary(PERSON, 2026)['bekannt'])

    def test_modified_or_deleted_managed_calendar_requires_review(self):
        self.account(); item = self.apply(); self.approve(item)
        self.update('DELETE FROM mitarbeiter_urlaub')
        self.assertFalse(self.s.summary(PERSON, 2026)['bekannt'])

    def test_admin_only_status_cas_and_changed_account(self):
        self.account(); item = self.apply()
        with self.assertRaises(PermissionError): self.s.review(PERSON, item['id'], 'genehmigt', 1, 1)
        self.account(resttage='8')
        with self.assertRaisesRegex(ValueError, 'Kontobasis'): self.approve(item, account_version=1)
        self.s.review(ADMIN, item['id'], 'abgelehnt', 1)
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '8')
        with self.assertRaises(ValueError): self.approve(item)

    def test_insufficient_balance_cannot_approve(self):
        self.account(resttage='0.5')
        item = self.apply()
        with self.assertRaisesRegex(ValueError, 'ausreichender'): self.approve(item)
        self.assertEqual(self.rows('SELECT * FROM mitarbeiter_urlaub'), [])
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '0,5')

    def test_invalid_dates_and_account_basis_do_not_mutate(self):
        for start,end in [('2026-10-09','2026-10-05'), ('2026-12-30','2027-01-03'), ('2026-02-30','2026-03-01'), ('2026-09-28','2026-10-01')]:
            with self.assertRaises(ValueError): self.s.preview(PERSON, start, end)
        for changes in ({'resttage':'30 Tage'},{'arbeitstage':[]},{'arbeitstage':[True]},{'feiertage':''},{'confirmed':False},{'stichtag':'2026-10-01'}):
            with self.assertRaises(ValueError): self.account(**changes)
        self.assertEqual(self.rows('SELECT * FROM mitarbeiter_urlaubskonten'), [])

    def test_failure_after_calendar_insert_rolls_back_everything(self):
        self.account(); item = self.apply()
        with patch.object(self.s, '_audit', side_effect=RuntimeError('synthetic rollback')):
            with self.assertRaises(RuntimeError): self.approve(item)
        self.assertEqual(self.rows('SELECT * FROM mitarbeiter_urlaub'), [])
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '10')
        self.assertEqual(self.s.summary(PERSON, 2026)['antraege'][0]['status'], 'beantragt')

    def test_reentrant_schema_restore_and_tables(self):
        self.account(); item = self.apply()
        self.s.init_schema()
        self.assertEqual(self.s.summary(PERSON, 2026)['antraege'][0]['id'], item['id'])
        self.assertEqual(set(TABLES), {'mitarbeiter_urlaubskonten','mitarbeiter_urlaubsantraege','mitarbeiter_urlaub_audit'})

    def test_concurrent_approvals_cannot_overdraw_same_account(self):
        self.account(resttage='5')
        first = self.apply()
        second = self.apply(start='2026-10-12', end='2026-10-16', key='second-synthetic-request')
        def attempt(item):
            try:
                self.s.review(ADMIN, item['id'], 'genehmigt', 1, 1)
                return 'approved'
            except ValueError:
                return 'blocked'
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(attempt, [first, second]))
        self.assertEqual(sorted(results), ['approved', 'blocked'])
        self.assertEqual(self.s.summary(PERSON, 2026)['resttage'], '0')
        self.assertEqual(len(self.rows('SELECT * FROM mitarbeiter_urlaub')), 1)

    def test_real_postgres_adapter_contract_without_live_database(self):
        names = {'DbRow','PostgresCursor','PostgresConnection','convert_sqlite_sql_to_postgres','get_insert_table_name','split_sql_script'}
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'app.py').read_text(encoding='utf-8'))
        nodes = [node for node in tree.body if isinstance(node,(ast.ClassDef,ast.FunctionDef)) and node.name in names]
        namespace = {'re': re}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'app.py (adapter only)','exec'),namespace)
        statements, database_path = [], self.p.path
        class Cursor:
            def __init__(self, connection): self.cursor = connection.cursor()
            def __enter__(self): return self
            def __exit__(self, *args): self.cursor.close()
            def execute(self, sql, params):
                statements.append(sql)
                self.cursor.execute(sql.replace('%s','?').replace('SERIAL PRIMARY KEY','INTEGER PRIMARY KEY AUTOINCREMENT'), params)
                self.rows = self.cursor.fetchall() if self.cursor.description else []
                self.rowcount = self.cursor.rowcount
                self.description = [type('Column',(),{'name':col[0]}) for col in self.cursor.description] if self.cursor.description else None
            def fetchall(self): return self.rows
        class Connection:
            def __init__(self): self.connection = sqlite3.connect(database_path)
            def cursor(self): return Cursor(self.connection)
            def commit(self): self.connection.commit()
            def rollback(self): self.connection.rollback()
            def close(self): self.connection.close()
        with patch.object(self.p, 'get_db', side_effect=lambda:namespace['PostgresConnection'](Connection())):
            self.s.init_schema(); self.account(); item = self.apply(); self.approve(item)
            self.assertEqual(self.s.summary(PERSON,2026)['resttage'],'5')
        self.assertTrue(any('ON CONFLICT(mitarbeiter_id,jahr)' in sql and sql.endswith('RETURNING mitarbeiter_id') for sql in statements))


class RouteTests(LeaveFixture):
    def setUp(self):
        super().setUp()
        bp = Blueprint('assistent', __name__, url_prefix='/werkstatt/assistent')
        bp.add_url_rule('', 'page', lambda:'assistant')
        self.p.app.add_url_rule('/admin/mitarbeiter', 'admin_mitarbeiter', lambda:'synthetic')
        def protected(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                who = session.get('who')
                if not who: abort(401)
                return fn(who, *args, **kwargs)
            return wrapper
        self.s = register_selfservice(self.p, bp, protected)
        self.p.app.register_blueprint(bp)
        self.client = self.p.app.test_client()
        with self.client.session_transaction() as s:
            s['who'] = PERSON; s['csrf_token'] = 'test-token'

    def test_personal_html_only_own_context_and_csrf(self):
        self.apply(OTHER, key='another-personal-key')
        response = self.client.get('/werkstatt/assistent/urlaub')
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('Synthetic Other', response.text)
        self.assertNotIn('private-other', response.text)
        self.assertIn('Resturlaub noch nicht verlässlich bekannt', response.text)
        payload = {'von':'2026-10-05','bis':'2026-10-09','request_id':'synthetic-route-request','confirmed':'ja','mitarbeiter_id':'2'}
        self.assertEqual(self.client.post('/werkstatt/assistent/urlaub/antrag', data=payload).status_code, 400)
        payload['csrf_token'] = 'test-token'
        self.assertEqual(self.client.post('/werkstatt/assistent/urlaub/antrag', data=payload).status_code, 303)
        self.assertEqual(len(self.s.summary(PERSON,2026)['antraege']), 1)
        self.assertEqual(len(self.s.summary(OTHER,2026)['antraege']), 1, 'Form cannot choose other identity')

    def test_admin_scope_csrf_and_render(self):
        self.assertEqual(self.client.get('/werkstatt/assistent/urlaub/verwaltung').status_code, 403)
        with self.client.session_transaction() as s: s['admin']=True; s['who']=ADMIN
        self.assertEqual(self.client.get('/werkstatt/assistent/urlaub/stand').status_code, 403)
        self.assertEqual(self.client.get('/werkstatt/assistent/urlaub').status_code, 302)
        response = self.client.get('/werkstatt/assistent/urlaub/verwaltung')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Synthetic Own', response.text)
        self.assertEqual(self.client.post('/werkstatt/assistent/urlaub/verwaltung/konto/1', data={}).status_code, 400)


if __name__ == '__main__':
    unittest.main()
