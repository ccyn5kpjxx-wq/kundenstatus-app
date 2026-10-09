"""Daily45 derived arithmetic on synthetic events, never a live app/database."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import sys
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_arbeitszeit as fixture
from werkstatt_personal_assistent import PersonalActions, TOOLS, RULES


class CalculatedTimeTests(unittest.TestCase):
    def setUp(self):
        self.f = fixture.TimeTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.p, self.s = self.f.p, self.f.s

    def at(self, instant, action):
        return self.f.at(instant, action)

    def legacy(self, instant, action):
        self.f.legacy_at(instant, action)

    def shift(self, start, end):
        self.at(start, 'kommen')
        self.at(end, 'gehen')

    def read(self, month='2026-09'):
        return self.s.summary(fixture.PERSON, month)

    def events(self):
        with self.s.db() as db:
            return [dict(row) for row in db.execute('SELECT * FROM mitarbeiter_zeitstempel ORDER BY revision')]

    def test_eight_hours_one_daily_break_raw_rows_and_schema_unchanged(self):
        self.shift('2026-09-29T06:00:00+00:00', '2026-09-29T14:00:00+00:00')
        before = self.events()
        with self.s.db() as db:
            schema = [tuple(row) for row in db.execute("SELECT name,sql FROM sqlite_master ORDER BY name")]
        report = self.read()
        self.assertEqual(report['abgeschlossene_arbeitszeit'], '8:00 Stunden')
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '7:15 Stunden')
        self.assertEqual(report['pausenabzug_sekunden'], 2700)
        self.assertEqual(report['schichten'][0]['arbeit_sekunden'], 28800)
        self.assertEqual(report['schichten'][0]['pause_sekunden'], 0)
        self.assertEqual(report['schichten'][0]['berechnete_arbeitszeit'], '7:15 Stunden')
        self.assertEqual(self.events(), before)
        with self.s.db() as db:
            self.assertEqual([tuple(row) for row in db.execute("SELECT name,sql FROM sqlite_master ORDER BY name")], schema)

    def test_two_shifts_same_start_day_share_exactly_one_break(self):
        self.shift('2026-09-29T06:00:00+00:00', '2026-09-29T10:00:00+00:00')
        self.shift('2026-09-29T11:00:00+00:00', '2026-09-29T15:00:00+00:00')
        report = self.read()
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '7:15 Stunden')
        self.assertEqual([row['pausenabzug_sekunden'] for row in report['schichten']], [2700, 0])
        self.assertEqual([row['berechnete_arbeitszeit'] for row in report['schichten']], ['3:15 Stunden', '4:00 Stunden'])
        self.assertEqual(len(report['arbeitstage']), 1)

    def test_short_shifts_spread_remaining_break_without_negative_work(self):
        for start, end in (('06:00', '06:20'), ('07:00', '07:20'), ('08:00', '08:10')):
            self.shift('2026-09-29T' + start + ':00+00:00', '2026-09-29T' + end + ':00+00:00')
        report = self.read()
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '0:05 Stunden')
        self.assertEqual([row['pausenabzug_sekunden'] for row in report['schichten']], [1200, 1200, 300])
        self.shift('2026-09-30T06:00:00+00:00', '2026-09-30T06:20:00+00:00')
        row = self.read()['schichten'][-1]
        self.assertEqual(row['berechnete_arbeitszeit'], '0:00 Stunden')
        self.assertEqual(row['pausenabzug_sekunden'], 1200)

    def test_each_new_start_day_gets_its_own_deduction(self):
        for day in (29, 30):
            self.shift(f'2026-09-{day}T06:00:00+00:00', f'2026-09-{day}T14:00:00+00:00')
        report = self.read()
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '14:30 Stunden')
        self.assertEqual(report['pausenabzug_sekunden'], 5400)

    def test_real_short_pause_counts_towards45_and_longer_pause_is_preserved(self):
        self.at('2026-09-29T06:00:00+00:00', 'kommen')
        self.legacy('2026-09-29T10:00:00+00:00', 'pause')
        self.legacy('2026-09-29T10:30:00+00:00', 'weiter')
        self.at('2026-09-29T14:00:00+00:00', 'gehen')
        report = self.read()
        self.assertEqual(report['abgeschlossene_arbeitszeit'], '7:30 Stunden')
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '7:15 Stunden')
        self.assertEqual(report['pausenabzug_sekunden'], 900)
        self.at('2026-09-30T06:00:00+00:00', 'kommen')
        self.legacy('2026-09-30T10:00:00+00:00', 'pause')
        self.legacy('2026-09-30T11:00:00+00:00', 'weiter')
        self.at('2026-09-30T14:00:00+00:00', 'gehen')
        row = self.read()['schichten'][-1]
        self.assertEqual(row['pause'], '1:00 Stunden')
        self.assertEqual(row['arbeitszeit'], '7:00 Stunden')
        self.assertEqual(row['berechnete_arbeitszeit'], '7:00 Stunden')
        self.assertEqual(row['pausenabzug_sekunden'], 0)

    def test_real_pauses_in_separate_shifts_combine_for_day_floor(self):
        for hour in (6, 12):
            self.at(f'2026-09-29T{hour:02d}:00:00+00:00', 'kommen')
            self.legacy(f'2026-09-29T{hour+1:02d}:00:00+00:00', 'pause')
            self.legacy(f'2026-09-29T{hour+1:02d}:30:00+00:00', 'weiter')
            self.at(f'2026-09-29T{hour+2:02d}:00:00+00:00', 'gehen')
        report = self.read()
        self.assertEqual(report['pausenabzug_sekunden'], 0)
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '3:00 Stunden')

    def test_overnight_month_split_has_one_start_day_not_second45(self):
        self.shift('2026-09-30T20:00:00+00:00', '2026-10-01T04:00:00+00:00')
        before = self.events()
        october, september = self.read('2026-10'), self.read('2026-09')
        self.assertEqual(september['abgeschlossene_arbeitszeit'], '2:00 Stunden')
        self.assertEqual(october['abgeschlossene_arbeitszeit'], '6:00 Stunden')
        self.assertEqual(september['berechnete_abgeschlossene_arbeitszeit'], '1:15 Stunden')
        self.assertEqual(october['berechnete_abgeschlossene_arbeitszeit'], '6:00 Stunden')
        self.assertEqual(october['pausenabzug_sekunden'], 0)
        self.assertEqual(october['schichten'][0]['arbeitstag'], '2026-09-30')
        self.assertEqual(sum(report['berechnete_abgeschlossene_arbeit_sekunden'] for report in (september, october)), 7 * 3600 + 900)
        self.assertEqual(self.events(), before)

    def test_earlier_previous_month_day_shift_consumed_break_before_crossing_shift(self):
        self.shift('2026-09-30T06:00:00+00:00', '2026-09-30T07:00:00+00:00')
        self.shift('2026-09-30T21:50:00+00:00', '2026-10-01T00:00:00+00:00')
        october, september = self.read('2026-10'), self.read('2026-09')
        self.assertEqual(october['berechnete_abgeschlossene_arbeitszeit'], '2:00 Stunden')
        self.assertEqual(october['pausenabzug_sekunden'], 0)
        self.assertEqual(september['berechnete_abgeschlossene_arbeitszeit'], '0:25 Stunden')
        self.assertEqual(september['pausenabzug_sekunden'], 2700)
        self.assertEqual(len(october['schichten']), 1)

    def test_real_pause_on_previous_month_start_day_prevents_double_deduction(self):
        self.at('2026-09-30T06:00:00+00:00', 'kommen')
        self.legacy('2026-09-30T06:30:00+00:00', 'pause')
        self.legacy('2026-09-30T07:30:00+00:00', 'weiter')
        self.at('2026-09-30T08:00:00+00:00', 'gehen')
        self.shift('2026-09-30T21:50:00+00:00', '2026-10-01T00:00:00+00:00')
        october, september = self.read('2026-10'), self.read('2026-09')
        self.assertEqual(october['pausenabzug_sekunden'], 0)
        self.assertEqual(september['pausenabzug_sekunden'], 0)
        self.assertEqual(october['berechnete_abgeschlossene_arbeitszeit'], '2:00 Stunden')
        self.assertEqual(september['berechnete_abgeschlossene_arbeitszeit'], '1:10 Stunden')

    def test_deduction_can_split_at_month_boundary_without_exceeding45(self):
        self.shift('2026-09-30T21:50:00+00:00', '2026-09-30T23:00:00+00:00')
        september, october = self.read('2026-09'), self.read('2026-10')
        self.assertEqual(september['pausenabzug_sekunden'], 600)
        self.assertEqual(october['pausenabzug_sekunden'], 2100)
        self.assertEqual(september['berechnete_abgeschlossene_arbeitszeit'], '0:00 Stunden')
        self.assertEqual(october['berechnete_abgeschlossene_arbeitszeit'], '0:25 Stunden')

    def test_real_pause_crossing_midnight_is_not_replaced_by_another_daily_floor(self):
        self.at('2026-09-30T20:00:00+00:00', 'kommen')
        self.legacy('2026-09-30T21:30:00+00:00', 'pause')
        self.legacy('2026-09-30T22:30:00+00:00', 'weiter')
        self.at('2026-10-01T00:00:00+00:00', 'gehen')
        for month in ('2026-09', '2026-10'):
            report = self.read(month)
            self.assertEqual(report['schichten'][0]['pause'], '0:30 Stunden')
            self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '1:30 Stunden')
            self.assertEqual(report['pausenabzug_sekunden'], 0)

    def test_exact_midnight_end_does_not_create_zero_length_new_month_day(self):
        self.shift('2026-10-31T20:00:00+00:00', '2026-10-31T23:00:00+00:00')
        november = self.read('2026-11')
        self.assertEqual(november['schichten'], [])
        self.assertEqual(november['arbeitstage'], [])
        self.assertEqual(november['pausenabzug_sekunden'], 0)

    def test_dst_elapsed_seconds_and_exact_second_display_without_rounding(self):
        for start, end, month in (('2026-03-29T00:30:00+00:00', '2026-03-29T02:30:00+00:00', '2026-03'),
                                  ('2026-10-25T00:30:00+00:00', '2026-10-25T02:30:00+00:00', '2026-10')):
            self.shift(start, end)
            report = self.read(month)
            self.assertEqual(report['abgeschlossene_arbeitszeit'], '2:00 Stunden')
            self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '1:15 Stunden')
        self.shift('2026-11-02T07:00:00+00:00', '2026-11-02T15:00:07+00:00')
        report = self.read('2026-11')
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '7:15:07 Stunden')
        self.assertEqual(report['berechnete_abgeschlossene_arbeit_sekunden'], 26107)
        self.assertEqual(report['schichten'][0]['arbeit_sekunden'], 28807)

    def test_open_second_shift_keeps_raw_closed_work_but_day_calculation_pending(self):
        self.shift('2026-09-29T06:00:00+00:00', '2026-09-29T07:00:00+00:00')
        self.at('2026-09-29T08:00:00+00:00', 'kommen')
        self.f.clock = datetime.fromisoformat('2026-09-29T09:00:00+00:00')
        report = self.read()
        self.assertEqual(report['abgeschlossene_arbeitszeit'], '1:00 Stunden')
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '0:00 Stunden')
        self.assertTrue(report['berechnung_pruefen'])
        self.assertTrue(all(row['berechnete_arbeitszeit'] is None for row in report['schichten']))
        self.assertIsNone(report['arbeitstage'][0]['pausenabzug'])
        self.at('2026-09-29T10:00:00+00:00', 'gehen')
        self.assertEqual(self.read()['berechnete_abgeschlossene_arbeitszeit'], '2:15 Stunden')

    def test_long_shift_is_review_only_and_never_finalized(self):
        self.shift('2026-09-29T06:00:00+00:00', '2026-09-30T07:00:00+00:00')
        report = self.read()
        self.assertTrue(report['pruefen'])
        self.assertTrue(report['berechnung_pruefen'])
        self.assertIsNone(report['schichten'][0]['berechnete_arbeitszeit'])
        self.assertEqual(report['berechnete_abgeschlossene_arbeit_sekunden'], 0)

    def test_backward_overlapping_closed_shifts_do_not_finalize_derived_total(self):
        self.shift('2026-09-29T07:00:00+00:00', '2026-09-29T08:00:00+00:00')
        self.shift('2026-09-29T07:30:00+00:00', '2026-09-29T08:30:00+00:00')
        before = self.events()
        report = self.read()
        self.assertEqual(report['abgeschlossene_arbeitszeit'], '2:00 Stunden')
        self.assertTrue(report['berechnung_pruefen'])
        self.assertEqual(report['berechnete_abgeschlossene_arbeit_sekunden'], 0)
        self.assertEqual(self.events(), before)

    def test_backward_legacy_segment_and_unknown_action_are_review_only(self):
        self.at('2026-09-29T06:00:00+00:00', 'kommen')
        self.legacy('2026-09-29T08:00:00+00:00', 'pause')
        self.legacy('2026-09-29T07:30:00+00:00', 'weiter')
        self.at('2026-09-29T10:00:00+00:00', 'gehen')
        self.assertTrue(self.read()['berechnung_pruefen'])
        self.shift('2026-09-30T06:00:00+00:00', '2026-09-30T14:00:00+00:00')
        with self.s.db() as db:
            db.execute("UPDATE mitarbeiter_zeitstempel SET aktion='unexpected' WHERE revision=3")
        report = self.read()
        self.assertIsNone(report['schichten'][0]['berechnete_arbeitszeit'])
        self.assertEqual(report['schichten'][-1]['berechnete_arbeitszeit'], '7:15 Stunden')

    def test_new_pause_actions_rejected_legacy_replay_unchanged_and_paused_shift_can_end(self):
        self.at('2026-09-29T06:00:00+00:00', 'kommen')
        before = self.events()
        for action in ('pause', 'weiter'):
            with self.assertRaises(ValueError):
                self.s.preview(fixture.PERSON, action)
            with self.assertRaises(ValueError):
                self.s.stamp(fixture.PERSON, action, 'synthetic-new-' + action, 1)
        self.assertEqual(self.events(), before)
        self.legacy('2026-09-29T10:00:00+00:00', 'pause')
        before = self.events()
        repeat = self.s.stamp(fixture.PERSON, 'pause', 'legacy-request-2', 1)
        self.assertTrue(repeat['wiederholt'])
        self.assertEqual(repeat['zeit'], '2026-09-29T10:00:00+00:00')
        self.assertEqual(self.events(), before)
        self.assertEqual(self.s.preview(fixture.PERSON, 'gehen')['revision'], 2)
        self.at('2026-09-29T11:00:00+00:00', 'gehen')
        report = self.read()
        self.assertEqual(report['schichten'][0]['pause'], '1:00 Stunden')
        self.assertEqual(report['berechnete_abgeschlossene_arbeitszeit'], '4:00 Stunden')
        self.assertEqual(report['pausenabzug_sekunden'], 0)

    def test_parallel_same_end_delivery_remains_one_event_and_one_daily_deduction(self):
        self.at('2026-09-29T06:00:00+00:00', 'kommen')
        self.f.clock = datetime.fromisoformat('2026-09-29T14:00:00+00:00')
        barrier = threading.Barrier(2)
        def end():
            barrier.wait(timeout=4)
            return self.s.stamp(fixture.PERSON, 'gehen', 'synthetic-concurrent-end', 1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [future.result() for future in [pool.submit(end), pool.submit(end)]]
        self.assertEqual(sum(result['wiederholt'] for result in results), 1)
        self.assertEqual(len(self.events()), 2)
        self.assertEqual(self.read()['pausenabzug_sekunden'], 2700)

    def test_old_continue_replay_does_not_reopen_or_change_closed_shift(self):
        self.at('2026-09-29T06:00:00+00:00', 'kommen')
        self.legacy('2026-09-29T10:00:00+00:00', 'pause')
        self.legacy('2026-09-29T10:30:00+00:00', 'weiter')
        self.at('2026-09-29T14:00:00+00:00', 'gehen')
        before = self.events()
        repeated = self.s.stamp(fixture.PERSON, 'weiter', 'legacy-request-3', 2)
        self.assertTrue(repeated['wiederholt'])
        self.assertEqual(repeated['zeit'], '2026-09-29T10:30:00+00:00')
        self.assertEqual(self.s.state(fixture.PERSON)['zustand'], 'abwesend')
        self.assertEqual(self.events(), before)

    def test_personal_profile_context_keeps_raw_and_calculated_month_separate(self):
        from flask import session
        from types import SimpleNamespace
        from werkstatt_mitarbeiter_portal import EmployeePortal
        self.shift('2026-09-29T06:00:00+00:00', '2026-09-29T14:00:00+00:00')
        with self.s.db() as db:
            db.executescript('''CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,lesen INTEGER,version INTEGER,
                auth_version INTEGER,dokumentieren INTEGER,einkaufen INTEGER,limit_cent INTEGER);
                INSERT INTO assistent_rechte VALUES(1,1,1,1,0,0,0);''')
        portal = EmployeePortal.__new__(EmployeePortal)
        portal.p = self.p
        self.p.assistant_time = self.s
        self.p.assistant_selfservice = SimpleNamespace(summary=lambda who: {'synthetic': True})
        with self.p.app.test_request_context():
            session.update(assistent_mid=1, assistent_version=1, assistent_auth_version=1)
            with patch.object(portal, '_profile', return_value={}), patch.object(portal, '_payrolls', return_value=[]), \
                    patch.object(portal, '_contracts', return_value=[]), patch.object(portal, 'company_holidays', return_value=[]):
                view = portal.personal_view()['arbeitszeit']
        self.assertEqual(view['monat_stunden'], '8:00 Stunden')
        self.assertEqual(view['berechnete_monat_stunden'], '7:15 Stunden')
        self.assertEqual(view['pausenabzug'], '0:45 Stunden')
        self.assertFalse(view['berechnung_pruefen'])

    def test_personal_language_tools_only_begin_end_and_old_confirmation_cannot_add_pause(self):
        definition = next(tool for tool in TOOLS if tool['name'] == 'arbeitszeit_vorschlagen')
        self.assertEqual(definition['parameters']['properties']['aktion']['enum'], ['kommen', 'gehen'])
        self.assertIn('Keine Pause oder Weiter-Stempel vorbereiten', RULES)
        with self.s.db() as db:
            db.executescript('''CREATE TABLE assistent_aktionen(id TEXT PRIMARY KEY,actor TEXT,auftrag_id INTEGER,
                art TEXT,payload TEXT,fingerprint TEXT UNIQUE,status TEXT DEFAULT 'vorschlag',erstellt_am TEXT);''')
        self.p.now_str = lambda: self.f.clock.isoformat()
        actions = PersonalActions(self.p, None, self.s, self.s.db, lambda *args: None)
        proposal = actions.propose(fixture.PERSON, {'art': 'arbeitszeit', 'aktion': 'kommen'})
        self.assertEqual(self.events(), [])
        actions.confirm(fixture.PERSON, proposal)
        for action in ('pause', 'weiter'):
            with self.assertRaises(ValueError):
                actions.propose(fixture.PERSON, {'art': 'arbeitszeit', 'aktion': action})
            old = dict(id='synthetic-old-' + action, actor=fixture.PERSON['actor'], art='arbeitszeit',
                       payload=json.dumps(dict(aktion=action, revision=1)))
            with self.assertRaises(ValueError):
                actions.confirm(fixture.PERSON, old)
        self.assertEqual(len(self.events()), 1)
        self.legacy('2026-09-29T10:00:00+00:00', 'pause')
        with self.s.db() as db:
            db.execute("UPDATE mitarbeiter_zeitstempel SET request_id='synthetic-old-ack' WHERE revision=2")
            db.execute('INSERT INTO assistent_aktionen(id,actor,art,payload,status) VALUES(?,?,?,?,?)',
                       ('synthetic-old-ack', fixture.PERSON['actor'], 'arbeitszeit', json.dumps(dict(aktion='pause', revision=1)), 'vorschlag'))
            old = dict(db.execute("SELECT * FROM assistent_aktionen WHERE id='synthetic-old-ack'").fetchone())
        before = self.events()
        self.assertTrue(actions.confirm(fixture.PERSON, old)['wiederholt'])
        self.assertEqual(self.events(), before)
        proposal = actions.propose(fixture.PERSON, {'art': 'arbeitszeit', 'aktion': 'gehen'})
        actions.confirm(fixture.PERSON, proposal)
        self.assertEqual(self.s.state(fixture.PERSON)['zustand'], 'abwesend')

    def test_reporting_calculation_succeeds_with_all_database_writes_denied(self):
        self.shift('2026-09-29T06:00:00+00:00', '2026-09-29T14:00:00+00:00')
        original_get_db = self.p.get_db
        def read_only():
            db = original_get_db()
            db.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action in
                              (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
                               sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_ALTER_TABLE) else sqlite3.SQLITE_OK)
            return db
        with patch.object(self.p, 'get_db', read_only):
            self.assertEqual(self.read()['berechnete_abgeschlossene_arbeitszeit'], '7:15 Stunden')


if __name__ == '__main__':
    unittest.main()
