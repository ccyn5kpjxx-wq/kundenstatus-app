"""Illness/AU safety on temporary synthetic storage; no application import."""
import ast
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from copy import deepcopy
from datetime import date, datetime, timezone
from functools import wraps
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from flask import Flask, abort, session
from werkzeug.datastructures import MultiDict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_mitarbeiter_krankheit import register_illness, TABLES
from werkstatt_mitarbeiter_portal import EmployeePortal, ensure_employee_private_state_for_import, TABLES as PRIVATE_TABLES
from werkstatt_mitarbeiter_chef import chef_overview


class Portal:
    def __init__(self, folder):
        self.path = folder / 'synthetic-illness.sqlite'
        self.app = Flask(__name__)
        self.app.config.update(TESTING=True, SECRET_KEY='synthetic-illness-only', ASSISTANT_NATIVE_COCKPIT=True)
        self.lock = threading.RLock()
        self.backups = []
        self.deny_writes = False
        with closing(self.get_db()) as db:
            db.executescript('''CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT,aktiv INTEGER,
                adresse TEXT DEFAULT '',geburtsdatum TEXT DEFAULT '',email TEXT DEFAULT '',telefon TEXT DEFAULT '');
                INSERT INTO mitarbeiter(id,name,aktiv) VALUES(1,'Synthetic Own',1),(2,'Synthetic Other',1),(3,'Synthetic Inactive',0);
                CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,lesen INTEGER,version INTEGER,auth_version INTEGER,
                    dokumentieren INTEGER DEFAULT 0,einkaufen INTEGER DEFAULT 0,limit_cent INTEGER DEFAULT 0);
                INSERT INTO assistent_rechte(mitarbeiter_id,lesen,version,auth_version) VALUES(1,1,1,1),(2,1,1,1),(3,1,1,1);
                CREATE TABLE mitarbeiter_urlaub(id INTEGER PRIMARY KEY,mitarbeiter_id INTEGER,start_datum TEXT,end_datum TEXT);
                CREATE TABLE mitarbeiter_zeitstempel(id INTEGER PRIMARY KEY,mitarbeiter_id INTEGER,aktion TEXT);
                INSERT INTO mitarbeiter_urlaub VALUES(1,1,'2026-08-03','2026-08-04');
                INSERT INTO mitarbeiter_zeitstempel VALUES(1,1,'synthetic-unchanged');''')
        self.employee_portal = EmployeePortal.__new__(EmployeePortal)
        self.employee_portal.p = self
        self.employee_portal.init_schema()
        self.assistant_selfservice = SimpleNamespace(summary=lambda who: {'synthetic': 'unchanged'})
        self.assistant_time = SimpleNamespace(summary=lambda who: {'status': {'zustand': 'abwesend'}, 'abgeschlossene_arbeitszeit': '0'})
        self.illness = register_illness(self)

    def get_db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        if self.deny_writes:
            db.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action in
                              (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE) else sqlite3.SQLITE_OK)
        return db

    @staticmethod
    def get_table_columns(db, table):
        return [row['name'] for row in db.execute('PRAGMA table_info(' + table + ')')]

    def ensure_column(self, db, table, column, definition):
        if column not in self.get_table_columns(db, table):
            db.execute('ALTER TABLE ' + table + ' ADD COLUMN ' + column + ' ' + definition)

    @contextmanager
    def portal_originals_operation_lock(self):
        with self.lock:
            yield

    def schedule_change_backup(self, reason):
        self.backups.append(reason)

    @staticmethod
    def backup_binary_reference_map(export):
        return {}

    @staticmethod
    def admin_required(view):
        @wraps(view)
        def guarded(*args, **kwargs):
            if not session.get('admin'):
                abort(403)
            return view(*args, **kwargs)
        return guarded


class IllnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p = Portal(Path(self.temp.name))
        self.client = self.p.app.test_client()
        self.login(self.client)

    @staticmethod
    def login(client, mid=1, admin=False):
        with client.session_transaction() as state:
            state.clear()
            state.update(csrf_token='synthetic-csrf')
            if admin:
                state['admin'] = True
            else:
                state.update(assistent_mid=mid, assistent_version=1, assistent_auth_version=1)

    @contextmanager
    def identity(self, mid=1, admin=False):
        with self.p.app.test_request_context():
            session.update(csrf_token='synthetic-csrf')
            if admin:
                session['admin'] = True
            else:
                session.update(assistent_mid=mid, assistent_version=1, assistent_auth_version=1)
            yield

    @staticmethod
    def form(**changes):
        values = dict(von='2026-04-06', bis='2026-04-09', typ='krank', quelle='eigene_meldung',
                      original_id='', request_id='a' * 32, confirmed='ja', csrf_token='synthetic-csrf')
        values.update(changes)
        return values

    def create(self, **changes):
        return self.client.post('/werkstatt/mein-konto/abwesenheiten/krankheit', data=self.form(**changes))

    def rows(self, table):
        with closing(self.p.get_db()) as db:
            return [dict(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY id')]

    def update(self, sql, args=()):
        with closing(self.p.get_db()) as db:
            db.execute(sql, args)
            db.commit()

    def original(self, mid=1, mime='application/pdf'):
        raw = b'%PDF-1.4\nSynthetic employer-copy document, not a real certificate.\n%%EOF'
        with closing(self.p.get_db()) as db:
            key = db.execute('''INSERT INTO mitarbeiter_arbeitsvertraege
                (mitarbeiter_id,titel,filename,mime,size_bytes,sha256,original_base64,created_at,created_by)
                VALUES(?,?,?,?,?,?,?,?,?) RETURNING id''',
                (mid, 'Synthetic private AU', 'synthetic-private.pdf', mime, len(raw), hashlib.sha256(raw).hexdigest(),
                 base64.b64encode(raw).decode(), '2026-04-01T10:00:00+02:00', 'admin')).fetchone()['id']
            db.commit()
        return key, raw

    def snapshot(self):
        with closing(self.p.get_db()) as db:
            return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)]
                               for table in (*PRIVATE_TABLES, 'mitarbeiter', 'assistent_rechte')}}

    def test_own_period_profile_integration_and_no_holiday_or_stamp_write(self):
        before = {table: self.rows(table) for table in ('mitarbeiter_urlaub', 'mitarbeiter_zeitstempel')}
        response = self.create()
        self.assertEqual(response.status_code, 303)
        self.assertTrue(response.location.endswith('#krankmeldungen'))
        with self.identity():
            view = self.p.employee_portal.personal_view()['krankmeldungen']
        self.assertEqual(len(view['eintraege']), 1)
        self.assertEqual(view['eintraege'][0]['bis'], '2026-04-09')
        self.assertEqual(view['originale'], [])
        with self.identity(2):
            self.assertEqual(self.p.illness.personal({'actor': 'mitarbeiter:2'})['eintraege'], [])
        with self.identity(admin=True):
            self.assertEqual(len(self.p.employee_portal.admin_view(1)['krankmeldungen']['eintraege']), 1)
        self.assertEqual({table: self.rows(table) for table in before}, before)

    def test_dates_enums_private_fields_are_strict_and_single_day_defaults(self):
        for changes in ({'von': '2026-02-30'}, {'bis': '2026-04-05'}, {'bis': '2028-04-09'},
                        {'von': '20260406'}, {'von': '1999-01-01'}, {'typ': 'diagnosis'}, {'quelle': 'free medical text'}):
            self.assertEqual(self.create(**changes).status_code, 303)
            self.assertEqual(self.rows(TABLES[0]), [])
        for field in ('diagnose', 'versichertennummer', 'notiz', 'mitarbeiter_id'):
            self.assertEqual(self.create(**{field: 'unwanted'}).status_code, 400)
        with self.identity():
            for field in ('typ', 'quelle'):
                with self.assertRaises(ValueError):
                    self.p.illness.create(self.form(**{field: []}))
        self.assertEqual(self.create(bis='').status_code, 303)
        self.assertEqual(self.rows(TABLES[0])[0]['bis'], '2026-04-06')

    def test_csrf_confirmation_business_duplicates_and_uploads_rejected(self):
        self.assertEqual(self.create(csrf_token='wrong').status_code, 400)
        self.assertEqual(self.create(confirmed='').status_code, 400)
        data = MultiDict(self.form())
        data.add('von', '2026-04-07')
        self.assertEqual(self.client.post('/werkstatt/mein-konto/abwesenheiten/krankheit', data=data).status_code, 400)
        data = MultiDict(self.form())
        data.add('csrf_token', 'wrong')
        self.assertEqual(self.client.post('/werkstatt/mein-konto/abwesenheiten/krankheit', data=data).status_code, 400)
        self.assertEqual(self.create(file=(io.BytesIO(b'synthetic'), 'synthetic.pdf')).status_code, 400)
        self.assertEqual(self.rows(TABLES[0]), [])
        data = MultiDict(self.form())
        data.add('csrf_token', 'synthetic-csrf')
        self.assertEqual(self.client.post('/werkstatt/mein-konto/abwesenheiten/krankheit', data=data).status_code, 303)
        self.assertEqual(len(self.rows(TABLES[0])), 1)

    def test_current_active_read_rights_and_both_session_versions_required(self):
        for sql, reset in (("UPDATE mitarbeiter SET aktiv=0 WHERE id=1", "UPDATE mitarbeiter SET aktiv=1 WHERE id=1"),
                           ("UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1", "UPDATE assistent_rechte SET lesen=1 WHERE mitarbeiter_id=1"),
                           ("UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1", "UPDATE assistent_rechte SET version=1 WHERE mitarbeiter_id=1"),
                           ("UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1", "UPDATE assistent_rechte SET auth_version=1 WHERE mitarbeiter_id=1")):
            self.update(sql)
            self.assertEqual(self.create().status_code, 403)
            with self.identity(), self.assertRaises(PermissionError):
                self.p.illness.personal({'actor': 'mitarbeiter:1'})
            self.update(reset)
        self.assertEqual(self.rows(TABLES[0]), [])
        self.login(self.client, admin=True)
        self.assertEqual(self.create().status_code, 403)

    def test_admin_target_binding_actor_and_inactive_creation(self):
        self.assertEqual(self.client.post('/admin/mitarbeiter/2/krankheit', data=self.form()).status_code, 403)
        self.login(self.client, admin=True)
        self.assertEqual(self.client.post('/admin/mitarbeiter/2/krankheit', data=self.form()).status_code, 303)
        self.assertEqual(self.rows(TABLES[0])[0]['mitarbeiter_id'], 2)
        self.assertEqual(self.rows(TABLES[1])[0]['actor'], 'admin')
        self.assertEqual(self.client.post('/admin/mitarbeiter/3/krankheit', data=self.form(request_id='b' * 32)).status_code, 303)
        self.assertEqual(len(self.rows(TABLES[0])), 1)
        self.assertEqual(self.client.post('/admin/mitarbeiter/2/krankheit', data=self.form(csrf_token='wrong')).status_code, 400)

    def test_identity_and_original_owner_are_rechecked_after_entering_write_lock(self):
        @contextmanager
        def revoke_at_lock():
            self.update('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1')
            yield
        with patch.object(self.p, 'portal_originals_operation_lock', revoke_at_lock):
            self.assertEqual(self.create().status_code, 403)
        self.assertEqual(self.rows(TABLES[0]), [])
        own, _ = self.original()
        self.login(self.client, admin=True)
        @contextmanager
        def move_at_lock():
            self.update('UPDATE mitarbeiter_arbeitsvertraege SET mitarbeiter_id=2 WHERE id=?', (own,))
            yield
        with patch.object(self.p, 'portal_originals_operation_lock', move_at_lock):
            response = self.client.post('/admin/mitarbeiter/1/krankheit',
                                        data=self.form(original_id=str(own), quelle='arbeitgeber_au'))
        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.rows(TABLES[0]), [])

    def test_replays_semantic_duplicates_and_overlaps_do_not_duplicate_audit(self):
        self.create()
        self.create()
        self.create(request_id='b' * 32)
        self.create(request_id='c' * 32, von='2026-04-08', bis='2026-04-12', typ='folge')
        self.create(typ='erst')
        self.assertEqual(len(self.rows(TABLES[0])), 1)
        self.assertEqual(len(self.rows(TABLES[1])), 1)
        self.create(request_id='d' * 32, von='2026-04-10', bis='2026-04-12', typ='folge')
        self.assertEqual(len(self.rows(TABLES[0])), 2)

    def test_parallel_same_request_is_one_record_and_one_audit(self):
        def submit(_):
            client = self.p.app.test_client()
            self.login(client)
            return client.post('/werkstatt/mein-konto/abwesenheiten/krankheit', data=self.form()).status_code
        with ThreadPoolExecutor(max_workers=3) as pool:
            self.assertEqual(list(pool.map(submit, range(3))), [303] * 3)
        self.assertEqual(len(self.rows(TABLES[0])), 1)
        self.assertEqual(len(self.rows(TABLES[1])), 1)

    def test_withdraw_owner_version_csrf_and_admin_separation(self):
        self.create()
        url = '/werkstatt/mein-konto/abwesenheiten/krankheit/' + 'a' * 32 + '/zurueckziehen'
        data = dict(version='1', confirmed='ja', csrf_token='synthetic-csrf')
        self.login(self.client, 2)
        self.assertEqual(self.client.post(url, data=data).status_code, 403)
        self.login(self.client)
        self.assertEqual(self.client.post(url, data={**data, 'version': '2'}).status_code, 303)
        self.assertEqual(self.rows(TABLES[0])[0]['status'], 'gemeldet')
        self.assertEqual(self.client.post(url, data={**data, 'csrf_token': 'wrong'}).status_code, 400)
        self.login(self.client, admin=True)
        self.assertEqual(self.client.post(url, data=data).status_code, 403)
        self.assertEqual(self.client.post('/admin/mitarbeiter/2/krankheit/' + 'a' * 32 + '/zurueckziehen', data=data).status_code, 403)
        self.assertEqual(self.client.post('/admin/mitarbeiter/1/krankheit/' + 'a' * 32 + '/zurueckziehen', data=data).status_code, 303)
        self.assertEqual(self.rows(TABLES[0])[0]['status'], 'zurueckgezogen')
        self.assertEqual(self.rows(TABLES[0])[0]['version'], 2)
        withdrawal = next(row for row in self.rows(TABLES[1]) if row['aktion'] == 'krankheit_zurueckgezogen')
        self.assertEqual(withdrawal['actor'], 'admin')
        self.client.post('/admin/mitarbeiter/1/krankheit/' + 'a' * 32 + '/zurueckziehen', data=data)
        self.assertEqual(len(self.rows(TABLES[1])), 2)

    def test_owned_au_original_is_private_and_foreign_or_docx_original_rejected(self):
        own, raw = self.original()
        other, _ = self.original(2)
        self.assertEqual(self.create(original_id=str(other), quelle='arbeitgeber_au').status_code, 303)
        self.assertEqual(self.rows(TABLES[0]), [])
        self.assertEqual(self.create(original_id=str(own)).status_code, 303)
        self.assertEqual(self.rows(TABLES[0]), [])
        self.create(original_id=str(own), quelle='arbeitgeber_au', typ='erst')
        with self.identity():
            row = self.p.illness.personal({'actor': 'mitarbeiter:1'})['eintraege'][0]
            self.assertEqual(row['original_url'], '/werkstatt/mein-konto/arbeitsvertrag/' + str(own))
            self.assertEqual(self.p.employee_portal.contract(own)[1], raw)
        with self.identity(2), self.assertRaises(LookupError):
            self.p.employee_portal.contract(own)
        with self.identity(admin=True):
            self.assertEqual(self.p.employee_portal.contract(own, admin_mid=1)[1], raw)
            with self.assertRaises(LookupError):
                self.p.employee_portal.contract(own, admin_mid=2)
            view = self.p.illness.admin_profile(2)
            self.assertEqual([row['id'] for row in view['originale']], [other])
        self.update('UPDATE mitarbeiter_arbeitsvertraege SET mime=? WHERE id=?',
                    ('application/vnd.openxmlformats-officedocument.wordprocessingml.document', other))
        self.login(self.client, 2)
        self.create(original_id=str(other), quelle='arbeitgeber_au', request_id='b' * 32)
        self.assertEqual(len(self.rows(TABLES[0])), 1)

    def test_corrupt_original_bytes_size_or_hash_cannot_link_or_expose_url(self):
        own, raw = self.original()
        original = self.rows('mitarbeiter_arbeitsvertraege')[0]
        for column, bad in (('original_base64', '!invalid!'), ('original_base64', base64.b64encode(b'other').decode()),
                            ('size_bytes', 0), ('sha256', 'f' * 64)):
            self.update('UPDATE mitarbeiter_arbeitsvertraege SET ' + column + '=? WHERE id=?', (bad, own))
            self.create(original_id=str(own), quelle='arbeitgeber_au')
            self.assertEqual(self.rows(TABLES[0]), [])
            self.update('UPDATE mitarbeiter_arbeitsvertraege SET ' + column + '=? WHERE id=?', (original[column], own))
        self.create(original_id=str(own), quelle='arbeitgeber_au')
        self.update('UPDATE mitarbeiter_arbeitsvertraege SET size_bytes=1 WHERE id=?', (own,))
        with self.identity():
            row = self.p.illness.personal({'actor': 'mitarbeiter:1'})['eintraege'][0]
            self.assertEqual(row['original_url'], '')
            self.assertEqual(row['original_status'], 'Originalzuordnung prüfen')
        self.update('UPDATE mitarbeiter_arbeitsvertraege SET size_bytes=?,mitarbeiter_id=2 WHERE id=?', (len(raw), own))
        with self.identity():
            self.assertEqual(self.p.illness.personal({'actor': 'mitarbeiter:1'})['eintraege'][0]['original_url'], '')

    def test_source_without_original_stays_optional_and_audit_contains_only_hash_version(self):
        self.create(quelle='arbeitgeber_au', typ='erst')
        with self.identity():
            row = self.p.illness.personal({'actor': 'mitarbeiter:1'})['eintraege'][0]
        self.assertEqual(row['original_url'], '')
        self.assertEqual(row['quelle_label'], 'AU-Arbeitgeberexemplar vorliegend')
        audit = self.rows(TABLES[1])[0]
        self.assertEqual(set(json.loads(audit['details'])), {'version', 'record_sha256'})
        self.assertNotIn('arbeitgeber_au', audit['details'])
        self.assertEqual(self.p.backups, ['mitarbeiter-krankheit'])

    def test_chef_current_and_recent_history_are_private_minimal_and_read_only(self):
        own, _ = self.original()
        self.create(quelle='arbeitgeber_au', typ='erst', original_id=str(own))
        for index in range(9):
            self.create(von=f'2026-05-{index + 1:02d}', bis='', request_id=f'{index + 20:032x}')
        # Keep a current period even when it was created before the latest eight.
        self.update("UPDATE mitarbeiter_krankmeldungen SET erstellt_am='2001-01-01' WHERE id=?", ('a' * 32,))
        self.p.deny_writes = True
        brief = self.p.illness.admin_briefing({'actor': 'admin'}, date(2026, 4, 8))
        self.assertEqual(len(brief['rows']), 9)
        current = next(row for row in brief['rows'] if row['id'] == 'a' * 32)
        self.assertEqual(current['status_key'], 'aktuell')
        self.assertEqual(current['datum_label'], '06.04.2026 – 09.04.2026')
        history = self.p.illness.admin_history({'actor': 'admin'}, date(2026, 6, 1))
        self.assertTrue(all(row['status_key'] == 'vergangen' for row in history))
        self.assertEqual(set(current), {'id', 'mitarbeiter_id', 'name', 'datum_label', 'status_key', 'status_label', 'url'})
        text = json.dumps(brief)
        self.assertNotIn('arbeitgeber_au', text)
        self.assertNotIn('synthetic-private', text)
        self.assertNotIn('original', text)
        for key in ('_team', '_absences', '_orders'):
            self.enterContext(patch('werkstatt_mitarbeiter_chef.' + key, return_value={'rows': [], 'error': ''}))
        result = chef_overview(self.p, {'actor': 'admin'}, [], now=datetime(2026, 4, 8, tzinfo=timezone.utc))
        self.assertEqual(result['krankmeldungen'], brief)

    def test_chef_unknown_invalid_withdrawn_and_inactive_are_not_current_team(self):
        self.create()
        for value, expected in (('zurueckgezogen', 'zurueckgezogen'), ('corrupt', 'pruefen')):
            self.update('UPDATE mitarbeiter_krankmeldungen SET status=?', (value,))
            row = self.p.illness.admin_briefing({'actor': 'admin'}, date(2026, 4, 8))['rows'][0]
            self.assertEqual(row['status_key'], expected)
        self.update("UPDATE mitarbeiter_krankmeldungen SET status='gemeldet',bis='invalid'")
        self.assertEqual(self.p.illness.admin_history({'actor': 'admin'}, date(2026, 4, 8))[0]['status_key'], 'pruefen')
        self.update("UPDATE mitarbeiter_krankmeldungen SET bis='2026-04-09'")
        self.update('UPDATE mitarbeiter SET aktiv=0 WHERE id=1')
        self.assertEqual(self.p.illness.admin_briefing({'actor': 'admin'}, date(2026, 4, 8))['rows'][0]['status_key'], 'inaktiv')
        self.assertEqual(self.p.illness.admin_history({'actor': 'admin'}, date(2026, 4, 10))[0]['status_key'], 'vergangen')
        with self.assertRaises(PermissionError):
            self.p.illness.admin_history({'actor': 'mitarbeiter:1'})

    def test_history_route_is_admin_only_private_and_no_original_fields(self):
        self.create()
        self.assertEqual(self.client.get('/admin/mitarbeiter/krankmeldungen').status_code, 403)
        self.login(self.client, admin=True)
        with patch('werkstatt_mitarbeiter_krankheit.render_template', return_value='synthetic history') as renderer:
            response = self.client.get('/admin/mitarbeiter/krankmeldungen')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Cache-Control'], 'private, no-store')
        self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
        self.assertNotIn('original_id', renderer.call_args.kwargs['data']['rows'][0])

    def test_restore_guards_legacy_missing_period_audit_owner_auth_and_original(self):
        ensure_employee_private_state_for_import(self.p, export={'tables': {}})
        own, _ = self.original()
        self.create(original_id=str(own), quelle='arbeitgeber_au')
        snapshot = self.snapshot()
        ensure_employee_private_state_for_import(self.p, export=snapshot)
        variants = []
        for table in TABLES:
            altered = deepcopy(snapshot)
            altered['tables'].pop(table)
            variants.append(altered)
        for table, column, value in ((TABLES[0], 'mitarbeiter_id', 2), (TABLES[0], 'status', 'zurueckgezogen'),
                                    ('assistent_rechte', 'auth_version', 2), ('mitarbeiter', 'name', 'Changed synthetic owner'),
                                    ('mitarbeiter_arbeitsvertraege', 'original_base64', ''),
                                    ('mitarbeiter_arbeitsvertraege', 'mitarbeiter_id', 2)):
            altered = deepcopy(snapshot)
            altered['tables'][table][0][column] = value
            variants.append(altered)
        for altered in variants + [{'tables': {}}]:
            with self.assertRaises(ValueError):
                ensure_employee_private_state_for_import(self.p, export=altered)
        path = Path(self.temp.name) / 'synthetic-restore.sqlite'
        with closing(self.p.get_db()) as source, closing(sqlite3.connect(path)) as target:
            source.backup(target)
        ensure_employee_private_state_for_import(self.p, imported_db=path)
        with closing(sqlite3.connect(path)) as target:
            target.execute('DELETE FROM mitarbeiter_krankheit_audit')
            target.commit()
        with self.assertRaises(ValueError):
            ensure_employee_private_state_for_import(self.p, imported_db=path)

    def test_backup_feature_required_tables_and_schema_hooks_without_app_import(self):
        source = (Path(__file__).resolve().parents[1] / 'app.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        values = {name: ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                  for target in node.targets if isinstance(target, ast.Name) and (name := target.id) in
                  ('BACKUP_TABLES', 'BACKUP_SCHEMA_FEATURES')}
        self.assertTrue(set(TABLES).issubset(values['BACKUP_TABLES']))
        self.assertIn('werkstatt_krankheit_v1', values['BACKUP_SCHEMA_FEATURES'])
        validator = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'validate_backup_binary_reference_completeness')
        feature = next(node for node in validator.body if isinstance(node, ast.If) and
                       isinstance(node.test, ast.Compare) and isinstance(node.test.left, ast.Constant) and node.test.left.value == 'werkstatt_krankheit_v1')
        self.assertEqual(ast.literal_eval(feature.body[0].value.args[0]), set(TABLES))
        # Execute only this pure validator AST, never top-level application code.
        namespace = dict(BACKUP_EXTERNALIZED_BINARY_FORMAT_VERSION=2, BACKUP_TABLES=values['BACKUP_TABLES'],
                         BACKUP_BINARY_FIELDS={}, clean_text=lambda value: str(value or '').strip())
        exec(compile(ast.Module(body=[validator], type_ignores=[]), 'isolated-backup-validator', 'exec'), namespace)
        validate = namespace['validate_backup_binary_reference_completeness']
        legacy = dict(format_version=2, schema_features=[], tables={table: [] for table in values['BACKUP_TABLES'] if table not in TABLES})
        validate(legacy, {})
        marked = deepcopy(legacy)
        marked['schema_features'] = ['werkstatt_krankheit_v1']
        with self.assertRaises(ValueError):
            validate(marked, {})
        marked['tables'].update({table: [] for table in TABLES})
        validate(marked, {})
        self.assertIn('"employee_illness_init_schema"', source)
        before = self.rows(TABLES[0])
        self.p.illness.init_schema()
        self.assertEqual(self.rows(TABLES[0]), before)


if __name__ == '__main__':
    unittest.main()
