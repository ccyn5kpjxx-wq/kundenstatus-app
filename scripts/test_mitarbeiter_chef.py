"""Chef projection on isolated synthetic storage, with writes denied on reads."""
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch
from flask import session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_bestelluebersicht as order_fixture
from werkstatt_arbeitszeit import TimeTracking
from werkstatt_mitarbeiter_schule import EmployeeSchool
from werkstatt_mitarbeiter_portal import EmployeePortal
from werkstatt_mitarbeiter_chef import chef_overview

ADMIN = {'actor': 'admin'}


class ChefTests(unittest.TestCase):
    def setUp(self):
        self.f = order_fixture.OverviewTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.p = self.f.p
        self.now = datetime(2026, 9, 29, 10, tzinfo=timezone.utc)
        with closing(self.p.get_db()) as db:
            db.execute('ALTER TABLE mitarbeiter ADD COLUMN aktiv INTEGER NOT NULL DEFAULT 1')
            db.execute("INSERT INTO mitarbeiter VALUES(3,'Synthetic inactive','private-inactive@example.invalid',0)")
            db.commit()
        self.p.assistant_time = TimeTracking(self.p, now=lambda: self.now)
        self.p.employee_school = EmployeeSchool(self.p)
        self.employees = [dict(id=1, name='Testperson A', aktiv=1, urlaube=[]),
                          dict(id=2, name='Testperson B', aktiv=1, urlaube=[]),
                          dict(id=3, name='Synthetic inactive', aktiv=0, urlaube=[])]

    def read(self):
        return chef_overview(self.p, ADMIN, self.employees, now=self.now)

    def school(self, key, mid, start, end=None, status='gemeldet', *,
               start_time='08:00', end_time='15:30', note='Synthetic lesson', source='Synthetic source', whole=0):
        with closing(self.p.get_db()) as db:
            db.execute('''INSERT INTO mitarbeiter_schulabwesenheiten
                (id,mitarbeiter_id,von,bis,ganztag,start_zeit,end_zeit,notiz,quelle,status,erstellt_am)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                (key, mid, start, end or start, whole, start_time, end_time, note, source, status, self.now.isoformat()))
            db.commit()

    def school_rows(self):
        return [row for row in self.read()['abwesenheiten']['rows'] if row['art'] == 'schule']

    def test_course_is_one_period_despite_varying_daily_hours_and_personal_times_are_preserved(self):
        hours = [('08:00', '16:00'), ('07:15', '15:30'), ('07:15', '15:30'),
                 ('07:15', '15:30'), ('07:15', '14:30')]
        for index, (start, end) in enumerate(hours):
            self.school(f'{index:032x}', 1, f'2026-11-{9 + index:02d}', start_time=start, end_time=end)
        # Resolve the real personal read service against synthetic rights.
        with closing(self.p.get_db()) as db:
            db.executescript('''CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,lesen INTEGER,
                version INTEGER,auth_version INTEGER,dokumentieren INTEGER,einkaufen INTEGER,limit_cent INTEGER);
                INSERT INTO assistent_rechte VALUES(1,1,1,1,0,0,0);''')
        self.p.employee_portal = EmployeePortal.__new__(EmployeePortal)
        self.p.employee_portal.p = self.p
        with self.p.app.test_request_context():
            session.update(assistent_mid=1, assistent_version=1, assistent_auth_version=1)
            before = self.p.employee_school.personal({'actor': 'mitarbeiter:1'}, 2026)['eintraege']
            rows = self.school_rows()
            after = self.p.employee_school.personal({'actor': 'mitarbeiter:1'}, 2026)['eintraege']
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['datum_label'], '09.11.2026 – 13.11.2026')
        self.assertEqual(rows[0]['zeit_label'], '')
        self.assertEqual(after, before)
        self.assertEqual([(row['start_zeit'], row['end_zeit']) for row in after], hours)

    def test_gaps_and_withdrawn_middle_day_split_course_and_today_keeps_original_beginning(self):
        for day in (9, 10, 11, 12, 13, 16):
            self.school(f'{day:032x}', 1, f'2026-11-{day:02d}', status='zurueckgezogen' if day == 11 else 'gemeldet')
        self.now = datetime(2026, 11, 10, 10, tzinfo=timezone.utc)
        rows = self.school_rows()
        self.assertEqual([row['datum_label'] for row in rows],
                         ['09.11.2026 – 10.11.2026', '12.11.2026 – 13.11.2026', '16.11.2026'])
        self.assertEqual([row['heute'] for row in rows], [True, False, False])
        self.assertTrue(all(row['zeit_label'] == '' for row in rows))

    def test_different_course_source_note_and_owner_do_not_merge(self):
        self.school('a' * 32, 1, '2026-11-09', note='Course A', source='Source A')
        self.school('b' * 32, 1, '2026-11-10', note='Course A', source='Source B')
        self.school('c' * 32, 1, '2026-11-11', note='Course B', source='Source B')
        self.school('d' * 32, 2, '2026-11-12', note='Course B', source='Source B')
        rows = self.school_rows()
        self.assertEqual([row['datum_label'] for row in rows],
                         ['09.11.2026', '10.11.2026', '11.11.2026', '12.11.2026'])
        self.assertEqual([row['mitarbeiter_id'] for row in rows], [1, 1, 1, 2])

    def test_missing_course_evidence_never_groups_unknown_school_days(self):
        for day in (9, 10):
            self.school(f'{day:032x}', 1, f'2026-11-{day:02d}', note='')
        for day in (11, 12):
            self.school(f'{day:032x}', 1, f'2026-11-{day:02d}', source='')
        for day in (13, 14):
            self.school(f'{day:032x}', 1, f'2026-11-{day:02d}', source=' ')
        self.assertEqual(len(self.school_rows()), 6)

    def test_continuous_course_over_year_boundary_keeps_beginning_and_deduplicates_source_rows(self):
        self.now = datetime(2027, 1, 1, 10, tzinfo=timezone.utc)
        for index, day in enumerate(('2026-12-30', '2026-12-31', '2027-01-01', '2027-01-02')):
            self.school(f'{index:032x}', 1, day)
        # A multi-day original appears in both yearly projections, only once
        # in the input set; nested/overlapping dates do not extend the course.
        self.school('f' * 32, 1, '2026-12-31', '2027-01-01')
        rows = self.school_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['datum_label'], '30.12.2026 – 02.01.2027')
        self.assertTrue(rows[0]['heute'])

    def test_current_clock_counts_and_today_next_absences_are_separate(self):
        self.p.assistant_time.stamp(dict(mitarbeiter_id=1, actor='mitarbeiter:1', lesen=1), 'kommen', 'synthetic-clock-start', 0)
        self.employees[1]['urlaube'] = [dict(start_iso='2026-09-29', end_iso='2026-10-01', notiz='Existing calendar'),
                                       dict(start_iso='2026-08-01', end_iso='2026-08-05', notiz='Past calendar')]
        self.school('a' * 32, 1, '2026-09-29')
        self.school('b' * 32, 1, '2026-11-09', '2026-11-13')
        self.school('c' * 32, 2, '2026-10-01', status='zurueckgezogen')
        self.school('d' * 32, 3, '2026-10-01')
        before = self.p.assistant_time.state(dict(mitarbeiter_id=1, actor='mitarbeiter:1', lesen=1))
        result = self.read()
        self.assertEqual(result['team']['counts'], dict(arbeitet=1, pause=0, abwesend=1, pruefen=0))
        self.assertEqual(len(result['team']['rows']), 2)
        self.assertEqual(result['team']['rows'][0]['label'], 'Angestempelt')
        rows = result['abwesenheiten']['rows']
        self.assertEqual([row['art'] for row in rows], ['schule', 'urlaub', 'schule'])
        self.assertEqual([row['heute'] for row in rows], [True, True, False])
        self.assertEqual(rows[-1]['datum_label'], '09.11.2026 – 13.11.2026')
        self.assertEqual(rows[1]['zeit_label'], 'Im Urlaubskalender')
        self.assertEqual(self.p.assistant_time.state(dict(mitarbeiter_id=1, actor='mitarbeiter:1', lesen=1)), before)
        self.assertNotIn('online', str(result).lower())
        self.assertNotIn('private-', str(result))

    def test_team_uses_rounded_clock_detail_without_changing_original_event(self):
        self.now = datetime(2026, 9, 29, 12, 32, 59, tzinfo=timezone.utc)
        who = dict(mitarbeiter_id=1, actor='mitarbeiter:1', lesen=1)
        self.p.assistant_time.stamp(who, 'kommen', 'synthetic-rounded-team', 0)
        with closing(self.p.get_db()) as db:
            before = [tuple(row) for row in db.execute('SELECT * FROM mitarbeiter_zeitstempel')]
        row = next(person for person in self.read()['team']['rows'] if person['id'] == 1)
        self.assertIn('14:30', row['detail'])
        self.assertNotIn('14:32', row['detail'])
        self.assertIn('berechnet', row['detail'])
        with closing(self.p.get_db()) as db:
            after = [tuple(row) for row in db.execute('SELECT * FROM mitarbeiter_zeitstempel')]
        self.assertEqual(before, after)
        self.assertIn('12:32:59', before[0][3])

    def test_order_counts_cover_paging_drafts_and_uncertain_states(self):
        for index in range(30):
            self.f.order('queued-' + str(index))
        self.f.batch('sent-batch', state='sent', outbox='sent')
        self.f.order('sent-one', batch='sent-batch')
        self.f.batch('uncertain-batch', state='uncertain')
        self.f.order('uncertain-one', batch='uncertain-batch')
        self.f.action('draft')
        self.f.action('approved', state='intern_freigegeben')
        self.f.action('unknown', state='unexpected')
        result = self.read()['bestellungen']
        self.assertFalse(result['error'])
        self.assertEqual(result['counts'], dict(offen=2, eingeplant=30, versandt=1, pruefen=2))
        self.assertLessEqual(len(result['rows']), 8)
        self.assertTrue(all('Gespeicherter Betrag' in row['preis_label'] or 'nicht belegt' in row['preis_label'] for row in result['rows']))
        for private in ('private-mail-account', 'NEVER_SHOW_BANK_DATA', 'orders@example.invalid'):
            self.assertNotIn(private, str(result))

    def test_all_projection_reads_work_under_sql_write_denial(self):
        self.f.order('one')
        self.school('a' * 32, 1, '2026-10-01')
        self.school('b' * 32, 1, '2026-10-02', start_time='07:15', end_time='14:30')
        original = self.p.get_db
        forbidden = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
                     sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_DROP_TABLE, sqlite3.SQLITE_ALTER_TABLE}
        def readonly():
            db = original()
            db.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action in forbidden else sqlite3.SQLITE_OK)
            return db
        with patch.object(self.p, 'get_db', side_effect=readonly), \
                patch.object(self.f.manager, 'tick', side_effect=AssertionError('no dispatch')), \
                patch.object(self.f.manager.dispatch, 'status', side_effect=AssertionError('no recovery')), \
                patch.object(self.p.assistant_time, 'stamp', side_effect=AssertionError('no stamp')):
            result = self.read()
        self.assertTrue(all(not result[key]['error'] for key in ('team', 'abwesenheiten', 'bestellungen')))
        self.assertEqual(result['bestellungen']['counts']['eingeplant'], 1)
        self.assertEqual(result['abwesenheiten']['rows'][0]['datum_label'], '01.10.2026 – 02.10.2026')

    def test_permission_and_source_failures_never_become_zero_counts(self):
        for who in ({}, {'actor': 'mitarbeiter:1'}):
            with self.assertRaises(PermissionError):
                chef_overview(self.p, who, self.employees, now=self.now)
        with patch.object(self.p.assistant_time, 'admin_employees', side_effect=RuntimeError('private diagnostic')), \
                patch.object(self.p.employee_school, 'admin_rows', side_effect=RuntimeError('private diagnostic')), \
                patch('werkstatt_mitarbeiter_chef.OrderOverview.page', side_effect=RuntimeError('private diagnostic')):
            result = self.read()
        for key in ('team', 'abwesenheiten', 'bestellungen'):
            self.assertIsNone(result[key]['counts'])
            self.assertTrue(result[key]['error'])
        self.assertNotIn('private diagnostic', str(result))


if __name__ == '__main__':
    unittest.main()
