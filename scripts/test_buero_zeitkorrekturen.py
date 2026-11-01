"""Audited office overrides on the real clock calculation; synthetic DB only."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sys
from threading import RLock
from types import SimpleNamespace
import unittest

from flask import session
from werkzeug.datastructures import MultiDict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_arbeitszeit as time_fixture
from werkstatt_buero_bereiche import register_office_sections


class OfficeCorrectionTests(unittest.TestCase):
    def setUp(self):
        fixture = time_fixture.TimeTests('runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.f, self.p, self.time = fixture, fixture.p, fixture.s
        self.p.assistant_time = self.time
        self.lock = RLock()
        self.p.portal_originals_operation_lock = lambda: self.lock
        self.allowed = True
        def identity(db=None):
            return {'actor': 'mitarbeiter:1', 'mitarbeiter_id': 1, 'office_categories': ('arbeitszeiten',)} if session.get('office') and self.allowed else None
        self.p.office_access = SimpleNamespace(identity=identity)
        self.service = register_office_sections(self.p)
        self.service.init_schema()
        self.client = self.p.app.test_client()
        with self.client.session_transaction() as state:
            state.update(office=True, csrf_token='correction-csrf')

    def shift(self, start='2026-09-29T07:00:00+00:00', end='2026-09-29T15:00:00+00:00'):
        self.f.at(start, 'kommen')
        self.f.at(end, 'gehen')

    def rows(self, table):
        with closing(self.p.get_db()) as db:
            return [dict(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY 1').fetchall()]

    def day(self, day='2026-09-29', mid=1):
        return next(row for row in self.service.daily_reviews(day) if row['mitarbeiter_id'] == mid)

    def form(self, day='2026-09-29', mid=1, **changes):
        row = self.day(day, mid)
        value = MultiDict(dict(csrf_token='correction-csrf', mitarbeiter_id=str(mid), datum=day, version=str(row['version']),
            fingerprint=row['fingerprint'], von='09:15', bis='16:45', grund='Tatsächliche Zeit mit Mitarbeiter und Anwesenheitsnotiz abgeglichen.', action='save', confirmed='1'))
        for key, item in changes.items(): value.setlist(key, [item])
        return value

    def correct(self, **changes):
        form = self.form(**changes)
        response = self.client.post('/werkstatt/buero/arbeitszeiten/korrigieren', data=form)
        self.assertEqual(response.status_code, 303, response.get_data(as_text=True))
        return form

    def check(self, day='2026-09-29', mid=1):
        current = self.day(day, mid)
        response = self.client.post('/werkstatt/buero/arbeitszeiten/pruefen', data=dict(csrf_token='correction-csrf',
            mitarbeiter_id=str(mid), datum=day, fingerprint=current['fingerprint'], version=str(current['version']), status='geprueft', notiz=''))
        self.assertEqual(response.status_code, 303, response.get_data(as_text=True))

    def read(self, month='2026-09', *, original_only=False):
        return self.time.report(1, month, original_only=original_only)

    def test_originals_visible_missing_values_empty_and_correction_requires_recheck(self):
        self.shift()
        before = self.rows('mitarbeiter_zeitstempel')
        state = self.rows('mitarbeiter_zeitstatus')
        self.assertEqual(self.day()['von_input'], '09:00:00')
        self.assertEqual((self.day(mid=2)['von_input'], self.day(mid=2)['bis_input']), ('', ''))
        original = self.read(original_only=True)
        self.check()
        self.correct()
        self.assertEqual(self.day()['status'], 'offen')
        pending = self.read()
        self.assertEqual(pending['berechnete_abgeschlossene_arbeitszeit'], original['berechnete_abgeschlossene_arbeitszeit'])
        self.assertFalse(pending['tageskorrekturen'][0]['applied'])
        self.check()
        result = self.read()
        self.assertEqual(result['berechnete_abgeschlossene_arbeitszeit'], '6:45 Stunden')
        self.assertEqual(result['urspruenglich_berechnete_abgeschlossene_arbeitszeit'], '7:15 Stunden')
        self.assertEqual(result['abgeschlossene_arbeitszeit'], original['abgeschlossene_arbeitszeit'])
        self.assertEqual(result['schichten'], original['schichten'])
        self.assertTrue(result['tageskorrekturen'][0]['applied'])
        self.assertEqual(self.rows('mitarbeiter_zeitstempel'), before)
        self.assertEqual(self.rows('mitarbeiter_zeitstatus'), state)
        html = self.client.get('/werkstatt/buero/arbeitszeiten?tag=2026-09-29&monat=2026-09&mitarbeiter=1').get_data(as_text=True)
        self.assertIn('6:45 Stunden', html)
        self.assertIn('In Summe enthalten', html)

    def test_source_or_version_change_rejects_stale_edit_and_reopens_applied_override(self):
        self.shift()
        stale = self.form()
        self.correct()
        self.assertEqual(self.client.post('/werkstatt/buero/arbeitszeiten/korrigieren', data=stale).status_code, 409)
        self.check()
        stale = self.form()
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE mitarbeiter_zeitstempel SET zeit='2026-09-29T15:30:00+00:00' WHERE aktion='gehen'")
            db.commit()
        self.assertEqual(self.client.post('/werkstatt/buero/arbeitszeiten/korrigieren', data=stale).status_code, 409)
        self.assertEqual(self.day()['status'], 'offen')
        result = self.read()
        self.assertFalse(result['tageskorrekturen'][0]['applied'])
        self.assertEqual(result['berechnete_abgeschlossene_arbeitszeit'], '7:45 Stunden')
        self.check()
        self.assertEqual(self.read()['berechnete_abgeschlossene_arbeitszeit'], '6:45 Stunden')

    def test_pauses_and_separate_shift_gap_survive_corrected_daily_envelope(self):
        self.f.at('2026-09-29T07:00:00+00:00', 'kommen')
        self.f.legacy_at('2026-09-29T08:00:00+00:00', 'pause')
        self.f.legacy_at('2026-09-29T09:00:00+00:00', 'weiter')
        self.f.at('2026-09-29T10:00:00+00:00', 'gehen')
        self.shift('2026-09-29T11:00:00+00:00', '2026-09-29T15:00:00+00:00')
        raw = self.read(original_only=True)
        self.assertEqual(raw['berechnete_abgeschlossene_arbeitszeit'], '6:00 Stunden')
        self.correct(von='08:30', bis='17:30')
        self.check()
        result = self.read()
        self.assertEqual(result['berechnete_abgeschlossene_arbeitszeit'], '7:00 Stunden')
        self.assertEqual(result['pausenabzug_sekunden'], 0)
        self.assertEqual(result['schichten'], raw['schichten'])

    def test_overnight_month_boundary_allocates_daily_break_only_once(self):
        self.shift('2026-09-30T20:00:00+00:00', '2026-10-01T04:00:00+00:00')
        self.correct(day='2026-09-30', von='23:00', bis='06:00', bis_folgetag='1')
        self.check('2026-09-30')
        september, october = self.read('2026-09'), self.read('2026-10')
        self.assertEqual(september['berechnete_abgeschlossene_arbeitszeit'], '0:15 Stunden')
        self.assertEqual(october['berechnete_abgeschlossene_arbeitszeit'], '6:00 Stunden')
        self.assertEqual(september['pausenabzug_sekunden'] + october['pausenabzug_sekunden'], 2700)

    def test_dst_overnight_uses_real_elapsed_time_and_rejects_ambiguous_local_inputs(self):
        self.shift('2026-03-28T21:00:00+00:00', '2026-03-29T04:00:00+00:00')
        self.correct(day='2026-03-28', von='22:15', bis='06:15', bis_folgetag='1')
        self.check('2026-03-28')
        self.assertEqual(self.read('2026-03')['berechnete_abgeschlossene_arbeitszeit'], '6:15 Stunden')
        for day in ('2026-03-29', '2026-10-25'):
            form = self.form(day=day, von='02:30', bis='04:00')
            self.assertEqual(self.client.post('/werkstatt/buero/arbeitszeiten/korrigieren', data=form).status_code, 409)

    def test_explicit_missing_stamps_correction_never_manufactures_clock_events(self):
        self.assertEqual(self.day()['von_input'], '')
        self.correct(von='09:00', bis='10:00')
        self.check()
        self.assertEqual(self.read()['berechnete_abgeschlossene_arbeitszeit'], '0:15 Stunden')
        self.assertEqual(self.rows('mitarbeiter_zeitstempel'), [])
        self.assertEqual(self.read()['schichten'], [])

    def test_csrf_reason_confirmation_and_fresh_permission_are_mandatory(self):
        self.shift()
        for changes in ({'csrf_token': 'wrong'}, {'grund': ''}, {'confirmed': '0'}, {'bis': '08:00'}, {'von': '25:00'}):
            response = self.client.post('/werkstatt/buero/arbeitszeiten/korrigieren', data=self.form(**changes))
            self.assertIn(response.status_code, (400, 409))
        form = self.form()
        form.add('von', '08:00')
        self.assertEqual(self.client.post('/werkstatt/buero/arbeitszeiten/korrigieren', data=form).status_code, 409)
        form = self.form()
        form.setlist('csrf_token', ['correction-csrf', 'wrong'])
        self.assertEqual(self.client.post('/werkstatt/buero/arbeitszeiten/korrigieren', data=form).status_code, 400)
        self.assertEqual(self.rows('buero_arbeitszeit_pruefung_audit'), [])
        original_lock = self.p.portal_originals_operation_lock
        def revoke_before_mutation():
            self.allowed = False
            return original_lock()
        self.p.portal_originals_operation_lock = revoke_before_mutation
        self.assertEqual(self.client.post('/werkstatt/buero/arbeitszeiten/korrigieren', data=self.form()).status_code, 403)
        self.assertEqual(self.rows('buero_arbeitszeit_pruefung_audit'), [])

    def test_reset_retains_audit_and_restores_original_sum_only_after_review(self):
        self.shift()
        self.correct()
        self.check()
        self.correct(action='reset')
        self.assertEqual(self.day()['status'], 'offen')
        self.assertFalse(self.day()['corrected'])
        self.assertEqual(self.read()['berechnete_abgeschlossene_arbeitszeit'], '7:15 Stunden')
        actions = [json.loads(row['details_json']).get('aktion') for row in self.rows('buero_arbeitszeit_pruefung_audit')]
        self.assertIn('zeitkorrektur', actions)
        self.assertIn('korrektur_zurueckgenommen', actions)

    def test_missing_start_keeps_team_editable_and_checked_interval_can_supply_calculation(self):
        with closing(self.p.get_db()) as db:
            db.execute("INSERT INTO mitarbeiter_zeitstempel VALUES('orphan-end',1,'gehen','2026-09-29T15:00:00+00:00','orphan-request',1)")
            db.commit()
        html = self.client.get('/werkstatt/buero/arbeitszeiten?tag=2026-09-29&monat=2026-09&mitarbeiter=1').get_data(as_text=True)
        self.assertIn('Synthetic Own', html)
        self.assertIn('name="von"', html)
        before = self.rows('mitarbeiter_zeitstempel')
        self.correct(von='09:00', bis='17:00')
        self.check()
        result = self.read()
        self.assertEqual(result['berechnete_abgeschlossene_arbeitszeit'], '7:15 Stunden')
        self.assertEqual(result['schichten'], [])
        self.assertTrue(result['original_auswertungsfehler'])
        self.assertEqual(self.rows('mitarbeiter_zeitstempel'), before)
        with self.assertRaises(ValueError): self.read(original_only=True)

    def test_incomplete_correction_cannot_be_finally_checked_or_applied(self):
        self.shift()
        self.correct(bis='')
        row = self.day()
        response = self.client.post('/werkstatt/buero/arbeitszeiten/pruefen', data=dict(csrf_token='correction-csrf',
            mitarbeiter_id='1', datum='2026-09-29', fingerprint=row['fingerprint'], version=str(row['version']), status='geprueft', notiz=''))
        self.assertEqual(response.status_code, 409)
        result = self.read()
        self.assertFalse(result['tageskorrekturen'][0]['applied'])
        self.assertEqual(self.day()['status'], 'offen')

    def test_override_overlapping_other_workday_never_double_counts_hours(self):
        self.shift()
        self.shift('2026-09-30T07:00:00+00:00', '2026-09-30T15:00:00+00:00')
        self.correct(von='23:00', bis='12:00', bis_folgetag='1')
        self.check()
        result = self.read()
        self.assertEqual(result['berechnete_abgeschlossene_arbeitszeit'], '14:30 Stunden')
        self.assertFalse(result['tageskorrekturen'][0]['applied'])
        self.assertIn('Überschneidung', result['tageskorrekturen'][0]['status_label'])

    def test_new_neighbor_after_month_boundary_invalidates_previous_month_override(self):
        self.shift('2026-09-30T20:00:00+00:00', '2026-10-01T04:00:00+00:00')
        self.correct(day='2026-09-30', von='22:00', bis='08:00', bis_folgetag='1')
        self.check('2026-09-30')
        self.assertTrue(self.read('2026-09')['tageskorrekturen'][0]['applied'])
        source_before = self.day('2026-09-30')['fingerprint']
        self.shift('2026-10-01T05:00:00+00:00', '2026-10-01T13:00:00+00:00')
        self.assertEqual(self.day('2026-09-30')['fingerprint'], source_before)
        september, october = self.read('2026-09'), self.read('2026-10')
        self.assertFalse(september['tageskorrekturen'][0]['applied'])
        self.assertFalse(october['tageskorrekturen'][0]['applied'])
        self.assertEqual(september['berechnete_abgeschlossene_arbeitszeit'], '1:15 Stunden')

    def test_conflicting_checked_overrides_are_both_refused(self):
        self.shift()
        self.shift('2026-09-30T07:00:00+00:00', '2026-09-30T15:00:00+00:00')
        self.correct(von='23:00', bis='10:00', bis_folgetag='1')
        self.check()
        self.correct(day='2026-09-30', von='09:00', bis='17:00')
        self.check('2026-09-30')
        result = self.read()
        self.assertTrue(all(not item['applied'] for item in result['tageskorrekturen']))
        self.assertEqual(result['berechnete_abgeschlossene_arbeitszeit'], '14:30 Stunden')

    def test_next_month_checked_override_is_included_only_for_overlap_protection(self):
        self.shift('2026-09-30T07:00:00+00:00', '2026-09-30T15:00:00+00:00')
        self.shift('2026-10-01T16:00:00+00:00', '2026-10-01T21:00:00+00:00')
        self.correct(day='2026-09-30', von='23:00', bis='12:00', bis_folgetag='1')
        self.check('2026-09-30')
        self.assertTrue(self.read('2026-09')['tageskorrekturen'][0]['applied'])
        self.correct(day='2026-10-01', von='09:00', bis='17:00')
        self.check('2026-10-01')
        september, october = self.read('2026-09'), self.read('2026-10')
        self.assertFalse(september['tageskorrekturen'][0]['applied'])
        self.assertEqual([row['datum'] for row in september['tageskorrekturen']], ['2026-09-30'])
        self.assertEqual(september['berechnete_abgeschlossene_arbeitszeit'], '7:15 Stunden')
        self.assertTrue(all(not row['applied'] for row in october['tageskorrekturen']))
        self.assertEqual(october['berechnete_abgeschlossene_arbeitszeit'], '4:15 Stunden')


if __name__ == '__main__':
    unittest.main()
