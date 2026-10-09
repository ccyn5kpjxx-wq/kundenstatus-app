"""Chef projection on isolated synthetic storage, with writes denied on reads."""
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_bestelluebersicht as order_fixture
from werkstatt_arbeitszeit import TimeTracking
from werkstatt_mitarbeiter_schule import EmployeeSchool
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

    def school(self, key, mid, start, end=None, status='gemeldet'):
        with closing(self.p.get_db()) as db:
            db.execute('''INSERT INTO mitarbeiter_schulabwesenheiten
                (id,mitarbeiter_id,von,bis,ganztag,start_zeit,end_zeit,notiz,quelle,status,erstellt_am)
                VALUES(?,?,?,?,0,'08:00','15:30','Synthetic lesson','Synthetic source',?,?)''',
                (key, mid, start, end or start, status, self.now.isoformat()))
            db.commit()

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
