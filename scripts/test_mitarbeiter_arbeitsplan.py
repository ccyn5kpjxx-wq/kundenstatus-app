"""Explicit personal Sollplans, separate from actual stamps; synthetic data only."""
import copy
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import patch

from werkzeug.datastructures import MultiDict

import test_mitarbeiter_portal as fixture
from werkstatt_mitarbeiter_portal import EmployeePortal, WORK_PLAN_COLUMNS, _plan_data, _plan_view

p, database = fixture.p, fixture.database


class EmployeeWorkPlanTests(TestCase):
    def setUp(self):
        self.base = fixture.EmployeePortalTests('runTest')
        self.base.setUp(); self.addCleanup(self.base.doCleanups)
        self.service = self.base.service
        self.admin, self.client = self.base.admin, self.base.client

    @staticmethod
    def fields(**changes):
        fields = dict(wochenstunden='40', tagesstunden='8', pausenminuten='60',
                      beginn='08:00', arbeitstage=[0, 1, 2, 3, 4])
        fields.update(changes)
        return fields

    def save(self, mid=1, client=None, **changes):
        fields = self.fields(**changes)
        fields['arbeitstage'] = [str(day) for day in fields['arbeitstage']]
        return (client or self.admin).post(f'/admin/mitarbeiter/{mid}/portal/arbeitsplan',
                                          data=dict(fields, csrf_token='test-csrf'))

    def plan(self, mid=1):
        return self.service.work_plan(mid)

    def test_unknown_is_read_only_without_global_defaults_or_fictional_stamps(self):
        for mid in (1, 2):
            plan = self.plan(mid)
            self.assertFalse(plan['bekannt'])
            self.assertEqual((plan['beginn'], plan['ende'], plan['tage']), ('', '', []))
            self.assertIsNone(plan['pausenminuten'])
        with database() as db:
            for table in ('mitarbeiter_portal_profile', 'mitarbeiter_zeitstempel', 'mitarbeiter_zeitstatus'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 0)

    def test_40_hour_plan_ends_at_17_and_belongs_only_to_selected_existing_employee(self):
        self.assertEqual(self.save().status_code, 303)
        plan = self.plan()
        self.assertTrue(plan['bekannt'])
        self.assertEqual((plan['wochenstunden'], plan['tagesstunden'], plan['pausenminuten']), ('40', '8', 60))
        self.assertEqual((plan['beginn'], plan['ende'], plan['tage_label']), ('08:00', '17:00', 'Montag–Freitag'))
        self.assertIn('keine erfassten Stempel', plan['hinweis'])
        self.assertFalse(self.plan(2)['bekannt'])
        with database() as db:
            row = dict(db.execute('SELECT * FROM mitarbeiter_portal_profile').fetchone())
            self.assertEqual(row['mitarbeiter_id'], 1)
            self.assertEqual(row['arbeitsplan_wochenminuten'], 2400)
            self.assertEqual(row['arbeitsplan_tagesminuten'], 480)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0], 0)

    def test_plan_save_preserves_existing_private_fields_and_first_row_contact_fallback(self):
        with database() as db:
            db.execute("UPDATE mitarbeiter SET adresse='Proven contact address',email='employee@test.invalid' WHERE id=1")
        self.save()
        self.client.get('/werkstatt/mein-konto')
        self.assertEqual(self.base.render.call_args.kwargs['profile']['adresse'], 'Proven contact address')
        self.assertEqual(self.base.profile(personalnummer='P-1', steuer_id='12345678901').status_code, 303)
        self.assertEqual(self.plan()['ende'], '17:00')
        self.save(beginn='07:30')
        self.client.get('/werkstatt/mein-konto')
        self.assertEqual(self.base.render.call_args.kwargs['profile']['steuer_id'], '12345678901')
        self.assertEqual(self.plan()['ende'], '16:30')

    def test_personal_profile_and_time_report_show_same_own_plan_without_foreign_query_switch(self):
        self.save()
        self.save(mid=2, wochenstunden='20', tagesstunden='4')
        self.client.get('/werkstatt/mein-konto?mitarbeiter_id=2&actor=admin')
        own = self.base.render.call_args.kwargs
        self.assertEqual(own['employee']['id'], 1)
        self.assertEqual(own['arbeitsplan']['ende'], '17:00')
        report = p.assistant_time.report(1, '2026-10')
        self.assertEqual(report['arbeitsplan'], own['arbeitsplan'])
        self.assertEqual(p.assistant_time.report(2, '2026-10')['arbeitsplan']['ende'], '13:00')
        self.assertEqual(report['abgeschlossene_arbeitszeit'], '0:00 Stunden')

    def test_plan_does_not_subtract_unstamped_pause_or_create_target_time_entries(self):
        self.save()
        who = {'actor': 'mitarbeiter:1', 'mitarbeiter_id': 1, 'lesen': 1}
        for revision, action, hour in ((0, 'kommen', 6), (1, 'gehen', 15)):
            with patch.object(p.assistant_time, 'now', return_value=datetime(2026, 10, 8, hour, tzinfo=timezone.utc)):
                p.assistant_time.stamp(who, action, 'workplan-actual-' + action, revision)
        report = p.assistant_time.report(1, '2026-10')
        self.assertEqual(report['abgeschlossene_arbeitszeit'], '9:00 Stunden')
        self.assertEqual(report['schichten'][0]['pause'], '0:00 Stunden')
        self.assertEqual(report['arbeitsplan']['tagesstunden'], '8')
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0], 2)

    def test_explicit_pause_stamps_still_produce_actual_8_hours_and_1_hour_pause(self):
        self.save()
        who = {'actor': 'mitarbeiter:1', 'mitarbeiter_id': 1, 'lesen': 1}
        for revision, (action, hour) in enumerate((('kommen', 6), ('pause', 10), ('weiter', 11), ('gehen', 15))):
            with patch.object(p.assistant_time, 'now', return_value=datetime(2026, 10, 8, hour, tzinfo=timezone.utc)):
                p.assistant_time.stamp(who, action, 'workplan-pause-' + action, revision)
        report = p.assistant_time.report(1, '2026-10')
        self.assertEqual(report['abgeschlossene_arbeitszeit'], '8:00 Stunden')
        self.assertEqual(report['schichten'][0]['pause'], '1:00 Stunden')

    def test_invalid_or_inconsistent_plans_do_not_save_partial_profiles(self):
        invalid = ({'wochenstunden': '39'}, {'tagesstunden': '0'}, {'tagesstunden': '8.01'},
                   {'wochenstunden': 'NaN'}, {'pausenminuten': '-1'}, {'pausenminuten': '1.5'},
                   {'beginn': '8:00'}, {'beginn': '24:00'}, {'beginn': '20:00'},
                   {'arbeitstage': []}, {'arbeitstage': [0, 0, 1, 2, 3]}, {'arbeitstage': [0, 1, 2, 3, 7]})
        for changes in invalid:
            with self.subTest(changes=changes):
                self.assertEqual(self.save(**changes).status_code, 400)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_portal_profile').fetchone()[0], 0)
        for changes in ({'arbeitstage': [True]}, {'pausenminuten': 60}, {'beginn': None}, {'iban': 'forbidden'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                _plan_data(self.fields(**changes))

    def test_route_requires_admin_csrf_exact_fields_and_single_scalar_values(self):
        self.assertEqual(self.save(client=self.client).status_code, 302)
        payload = self.fields(); payload['arbeitstage'] = ['0', '1', '2', '3', '4']
        route = '/admin/mitarbeiter/1/portal/arbeitsplan'
        self.assertEqual(self.admin.post(route, data=payload).status_code, 400)
        self.assertEqual(self.admin.post(route, data=dict(payload, csrf_token='test-csrf', iban='forbidden')).status_code, 400)
        duplicates = MultiDict(dict(payload, csrf_token='test-csrf'))
        duplicates.add('wochenstunden', '20')
        self.assertEqual(self.admin.post(route, data=duplicates).status_code, 400)
        self.assertEqual(self.save(mid=999).status_code, 404)
        self.assertFalse(self.plan()['bekannt'])

    def test_corrupt_stored_plan_fails_closed_as_unknown(self):
        self.save()
        with database() as db:
            db.execute("UPDATE mitarbeiter_portal_profile SET arbeitsplan_tage_json='[true]' WHERE mitarbeiter_id=1")
        plan = self.plan()
        self.assertFalse(plan['bekannt'])
        self.assertIn('intern geprüft', plan['hinweis'])
        self.assertEqual(plan['ende'], '')

    def test_fractional_hours_are_minute_exact_and_weekly_total_consistent(self):
        self.assertEqual(self.save(wochenstunden='37,5', tagesstunden='7.5').status_code, 303)
        plan = self.plan()
        self.assertEqual((plan['wochenstunden'], plan['tagesstunden'], plan['ende']), ('37.5', '7.5', '16:30'))

    def test_actual_plan_upsert_uses_explicit_natural_postgres_returning_key(self):
        queries = []
        get_db = p.get_db
        class TrackedConnection:
            def __init__(self): self.db = get_db()
            def __getattr__(self, key): return getattr(self.db, key)
            def execute(self, query, args=()):
                if query.startswith('INSERT INTO mitarbeiter_portal_profile'):
                    queries.append((query, args))
                return self.db.execute(query, args)
        with patch.object(p, 'get_db', side_effect=TrackedConnection):
            self.assertEqual(self.save().status_code, 303)
        self.assertEqual(len(queries), 1)
        class Cursor:
            description = [SimpleNamespace(name='mitarbeiter_id')]
            rowcount = 1
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def execute(self, query, args):
                self.query = query
                if 'RETURNING id' in query:
                    raise AssertionError('Personal profile has only mitarbeiter_id')
            def fetchall(self): return [(1,)]
        cursor = Cursor()
        adapter = p.PostgresConnection(SimpleNamespace(cursor=lambda: cursor))
        adapter.execute(*queries[0]).fetchall()
        self.assertIn('RETURNING mitarbeiter_id', cursor.query)
        self.assertNotIn('?', cursor.query)

    def test_legacy_private_schema_migration_preserves_values_without_assigning_a_plan(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'legacy.db'
            def get_db():
                db = sqlite3.connect(path); db.row_factory = sqlite3.Row; return db
            with closing(get_db()) as db:
                db.executescript('''CREATE TABLE mitarbeiter_portal_profile(mitarbeiter_id INTEGER PRIMARY KEY,
                    personalnummer TEXT DEFAULT '',steuer_id TEXT DEFAULT '',steuernummer TEXT DEFAULT '',
                    adresse TEXT DEFAULT '',geburtsdatum TEXT DEFAULT '',email TEXT DEFAULT '',telefon TEXT DEFAULT '',
                    updated_at TEXT,updated_by TEXT); INSERT INTO mitarbeiter_portal_profile(mitarbeiter_id,steuer_id,updated_at,updated_by)
                    VALUES(1,'12345678901','old','admin');''')
                db.commit()
            service = EmployeePortal(SimpleNamespace(get_db=get_db, ensure_column=p.ensure_column))
            service.init_schema(); service.init_schema()
            with closing(get_db()) as db:
                row = dict(db.execute('SELECT * FROM mitarbeiter_portal_profile').fetchone())
            self.assertEqual(row['steuer_id'], '12345678901')
            self.assertTrue(set(WORK_PLAN_COLUMNS) <= set(row))
            self.assertFalse(_plan_view(row)['bekannt'])

    def test_optional_time_hook_is_compatible_with_time_service_without_personal_portal(self):
        with patch.object(p, 'employee_portal', None):
            report = p.assistant_time.report(1, '2026-10')
        self.assertNotIn('arbeitsplan', report)
        self.assertEqual(report['abgeschlossene_arbeitszeit'], '0:00 Stunden')

    def test_pure_admin_personal_time_entry_redirects_to_existing_admin_time_page(self):
        with self.admin.session_transaction() as state:
            for key in ('assistent_mid', 'assistent_version', 'assistent_auth_version'):
                state.pop(key, None)
        response = self.admin.get('/werkstatt/assistent/arbeitszeit?mitarbeiter_id=2')
        self.assertEqual((response.status_code, response.location), (302, '/admin/arbeitszeit'))
        self.assertEqual(self.admin.get(response.location).status_code, 200)

    def test_valid_personal_identity_wins_over_admin_flag_on_personal_time_page(self):
        self.base.renderer.stop()
        self.save()
        response = self.admin.get('/werkstatt/assistent/arbeitszeit?mitarbeiter_id=2&monat=2026-10')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Testperson', response.text)
        self.assertIn('40 Stunden', response.text)
        self.assertNotIn('Other Person', response.text)
        self.assertIn('action="/werkstatt/mein-konto/zeit"', response.text)
        self.assertNotIn('name="mitarbeiter_id"', response.text)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_zeitstempel').fetchone()[0], 0)

    def test_anonymous_personal_time_entry_stays_protected(self):
        response = p.app.test_client().get('/werkstatt/assistent/arbeitszeit')
        self.assertIn(response.status_code, (302, 401, 403))
        self.assertNotEqual(response.location, '/admin/arbeitszeit')
        self.assertNotIn('action="/werkstatt/mein-konto/zeit"', response.text)

    def test_actual_json_sqlite_restore_preserves_plan_and_allows_current_snapshot(self):
        from test_material_external_restore import ExternalRestoreTests, connection
        self.save()
        ExternalRestoreTests.setUpClass()
        restore = ExternalRestoreTests('runTest'); restore.setUp(); self.addCleanup(restore.doCleanups)
        source = p.get_db()
        try:
            with closing(sqlite3.connect(restore.db_path)) as target:
                source.backup(target)
        finally:
            source.close()
        tables = (*fixture.TABLES, 'mitarbeiter', 'assistent_rechte')
        restore.ns['BACKUP_TABLES'] = tables
        def snapshot():
            with connection(restore.db_path) as db:
                return {'tables': {table: [dict(row) for row in db.execute('SELECT * FROM ' + table)] for table in tables}}
        before = snapshot()
        old = copy.deepcopy(before)
        old['tables']['mitarbeiter_portal_profile'][0]['arbeitsplan_beginn'] = '07:00'
        with self.assertRaisesRegex(ValueError, 'persönliche'):
            restore.ns['import_backup_json_rows_into_current_database'](old, None, [])
        self.assertEqual(snapshot(), before)
        incoming = restore.root / 'old-plan.db'
        with closing(sqlite3.connect(restore.db_path)) as current, closing(sqlite3.connect(incoming)) as old_db:
            current.backup(old_db)
        with connection(incoming) as db:
            db.execute("UPDATE mitarbeiter_portal_profile SET arbeitsplan_beginn='07:00'")
        with self.assertRaisesRegex(ValueError, 'persönliche'):
            restore.ns['import_sqlite_rows_into_current_database'](incoming)
        self.assertEqual(snapshot(), before)
        restore.ns['import_backup_json_rows_into_current_database'](before, None, [])
        with closing(sqlite3.connect(restore.db_path)) as current, closing(sqlite3.connect(incoming)) as current_copy:
            current.backup(current_copy)
        restore.ns['import_sqlite_rows_into_current_database'](incoming)
        self.assertEqual(snapshot(), before)


if __name__ == '__main__':
    main()
