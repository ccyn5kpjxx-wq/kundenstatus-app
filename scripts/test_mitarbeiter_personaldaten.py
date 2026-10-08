"""Additional private payroll-header fields: synthetic employees, isolated paths."""
import atexit
import copy
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
from unittest import TestCase, main

ISOLATED = tempfile.TemporaryDirectory(prefix='employee-profile-test-')
atexit.register(ISOLATED.cleanup)
os.environ.update(BACKUP_DIR=str(Path(ISOLATED.name)/'backups'),
                  DELETED_UPLOAD_DIR=str(Path(ISOLATED.name)/'deleted'),
                  AUTO_BACKUP_ENABLED='0', AUTO_CHANGE_BACKUP_ENABLED='0', AUTO_BACKUP_ON_STARTUP='0')
import test_mitarbeiter_portal as fixture
atexit.register(fixture.fixture.TEMP.cleanup)
from werkstatt_mitarbeiter_portal import (EmployeePortal, LEGACY_PROFILE_FIELDS,
    PRIVATE_PROFILE_COLUMNS, PROFILE_FIELDS, ensure_employee_private_state_for_import)

p, database = fixture.p, fixture.database
VALUES = dict(sozialversicherungsnummer='12123456A123', krankenkasse='Synthetische Krankenkasse',
              steuerklasse='1', eintrittsdatum='2026-09-01')


class PrivateProfileTests(TestCase):
    def setUp(self):
        self.base=fixture.EmployeePortalTests('runTest'); self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.admin, self.client, self.service=self.base.admin,self.base.client,self.base.service

    def profile(self):
        with database() as db:
            row=db.execute('SELECT * FROM mitarbeiter_portal_profile WHERE mitarbeiter_id=1').fetchone()
            return dict(row) if row else None

    def test_only_personal_owner_and_admin_read_fields_without_audit_content(self):
        self.assertEqual(self.base.profile(**VALUES).status_code,303)
        self.base.renderer.stop()
        personal=self.client.get('/werkstatt/mein-konto?mitarbeiter_id=2').text
        admin=self.admin.get('/admin/mitarbeiter/1/portal').text
        other=self.admin.get('/admin/mitarbeiter/2/portal').text
        other_fields=other.split('<div class="profile-edit-grid">',1)[1].split('</div>',1)[0]
        for value in VALUES.values():
            self.assertIn(value,personal); self.assertIn(value,admin)
            self.assertNotIn('value="'+value+'"',other_fields)
        for label in ('Sozialversicherungsnummer','Krankenkasse','Steuerklasse','Eintrittsdatum'):
            self.assertIn(label,personal); self.assertIn(label,admin)
        foreign=p.app.test_client()
        with foreign.session_transaction() as state:
            state.update(assistent_mid=2,assistent_version=1,assistent_auth_version=1)
        foreign_fields=foreign.get('/werkstatt/mein-konto?mitarbeiter_id=1').text.split('<dl class="profile-fields">',1)[1].split('</dl>',1)[0]
        for value in VALUES.values(): self.assertNotIn('<dd>'+value+'</dd>',foreign_fields)
        with database() as db:
            audit=json.dumps([dict(row) for row in db.execute('SELECT * FROM assistent_audit')])
            public=dict(db.execute('SELECT * FROM mitarbeiter WHERE id=1').fetchone())
        for key,value in VALUES.items():
            self.assertNotIn(key,public)
            if key!='steuerklasse': self.assertNotIn(value,audit)

    def test_validation_rejects_bad_fields_and_unknown_banking_keys_without_writes(self):
        for values in ({'sozialversicherungsnummer':'a'*21},{'sozialversicherungsnummer':'ABC\n123'},
                       {'sozialversicherungsnummer':'<x>'},{'krankenkasse':'a'*121},
                       {'krankenkasse':'<script>x</script>'},{'steuerklasse':'0'},{'steuerklasse':'7'},
                       {'steuerklasse':'01'},{'steuerklasse':'1/1'},{'eintrittsdatum':'01.09.2026'},
                       {'eintrittsdatum':'2026-02-30'},{'eintrittsdatum':'2026-09-01\n'},
                       {'iban':'banking not permitted'}):
            with self.subTest(fields=values): self.assertEqual(self.base.profile(**values).status_code,400)
        self.assertIsNone(self.profile())

    def test_old_forms_and_service_payloads_keep_new_values_explicit_empty_can_clear(self):
        self.base.profile(**VALUES)
        legacy={key:'' for key in LEGACY_PROFILE_FIELDS}; legacy['personalnummer']='Updated old form'
        self.assertEqual(self.admin.post('/admin/mitarbeiter/1/portal',data=dict(legacy,csrf_token='test-csrf')).status_code,303)
        self.assertEqual({key:self.profile()[key] for key in VALUES},VALUES)
        with p.app.test_request_context('/'):
            from flask import session
            session['admin']=True
            self.service.save_profile(1,legacy)
        self.assertEqual({key:self.profile()[key] for key in VALUES},VALUES)
        self.assertEqual(self.base.profile(**{key:'' for key in VALUES}).status_code,303)
        self.assertTrue(all(self.profile()[key]=='' for key in VALUES))

    def test_new_fields_remain_private_after_rights_or_activity_revocation(self):
        self.base.profile(**VALUES)
        for sql in ('UPDATE assistent_rechte SET auth_version=2 WHERE mitarbeiter_id=1',
                    'UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1',
                    'UPDATE mitarbeiter SET aktiv=0 WHERE id=1'):
            with database() as db: db.execute(sql)
            self.assertEqual(self.client.get('/werkstatt/mein-konto').status_code,302)
            with database() as db:
                db.execute('UPDATE assistent_rechte SET auth_version=1,lesen=1 WHERE mitarbeiter_id=1')
                db.execute('UPDATE mitarbeiter SET aktiv=1 WHERE id=1')
        response=self.client.post('/admin/mitarbeiter/1/portal',data=dict(self.base.fields(**VALUES),csrf_token='test-csrf'))
        self.assertNotEqual(response.status_code,303)

    def test_legacy_schema_migrates_to_empty_fields_without_inventing_values(self):
        path=Path(ISOLATED.name)/'legacy-profile.db'
        with closing(sqlite3.connect(path)) as db, db:
            db.executescript("CREATE TABLE mitarbeiter_portal_profile(mitarbeiter_id INTEGER PRIMARY KEY,personalnummer TEXT DEFAULT '',"
                "steuer_id TEXT DEFAULT '',steuernummer TEXT DEFAULT '',adresse TEXT DEFAULT '',geburtsdatum TEXT DEFAULT '',"
                "email TEXT DEFAULT '',telefon TEXT DEFAULT '',updated_at TEXT,updated_by TEXT);"
                "INSERT INTO mitarbeiter_portal_profile(mitarbeiter_id,steuer_id,updated_at,updated_by) VALUES(1,'12345678901','old','admin');")
        def connect():
            db=sqlite3.connect(path); db.row_factory=sqlite3.Row; return db
        EmployeePortal(SimpleNamespace(get_db=connect,ensure_column=p.ensure_column)).init_schema()
        with closing(connect()) as db:
            row=dict(db.execute('SELECT * FROM mitarbeiter_portal_profile').fetchone())
        self.assertEqual(row['steuer_id'],'12345678901')
        self.assertTrue(all(row[key]=='' for key in PRIVATE_PROFILE_COLUMNS))

    def test_legacy_backup_missing_new_columns_allowed_only_while_empty(self):
        self.base.profile()
        before=self.base.snapshot(); legacy=copy.deepcopy(before)
        for key in PRIVATE_PROFILE_COLUMNS: legacy['tables']['mitarbeiter_portal_profile'][0].pop(key)
        ensure_employee_private_state_for_import(p,export=legacy)
        self.base.profile(**VALUES)
        with self.assertRaises(ValueError): ensure_employee_private_state_for_import(p,export=legacy)
        current=self.base.snapshot()
        ensure_employee_private_state_for_import(p,export=current)
        for key in PRIVATE_PROFILE_COLUMNS:
            changed=copy.deepcopy(current); changed['tables']['mitarbeiter_portal_profile'][0][key]=''
            with self.subTest(column=key), self.assertRaises(ValueError):
                ensure_employee_private_state_for_import(p,export=changed)

    def test_work_plan_preserves_private_fields_and_does_not_change_access(self):
        self.base.profile(**VALUES)
        with database() as db: before=dict(db.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=1').fetchone())
        with p.app.test_request_context('/'):
            from flask import session
            session['admin']=True
            self.service.save_work_plan(1,dict(wochenstunden='40',tagesstunden='8',pausenminuten='60',beginn='08:00',arbeitstage=[0,1,2,3,4]))
        self.assertEqual({key:self.profile()[key] for key in VALUES},VALUES)
        with database() as db:
            self.assertEqual(dict(db.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=1').fetchone()),before)


if __name__=='__main__': main()
