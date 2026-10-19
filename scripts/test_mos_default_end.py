"""Return-time defaults and real template rendering; no portal app or database."""
from pathlib import Path
import re
import sys
import unittest

from jinja2 import Environment, FileSystemLoader

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from mos_public_booking import default_end_slot, weekly_handover_slots
from datetime import datetime


class DefaultEndTests(unittest.TestCase):
    def test_normal_day_prefers_24_hours_not_last_six_day_slot(self):
        slots=['2026-10-19T16:00:00+02:00','2026-10-19T20:00:00+02:00',
               '2026-10-20T15:00:00+02:00','2026-10-20T16:00:00+02:00',
               '2026-10-24T20:00:00+02:00']
        self.assertEqual(default_end_slot(slots,5),slots[3])

    def test_next_offered_time_after_24_hours(self):
        slots=['2026-10-19T16:00:00+02:00','2026-10-20T15:00:00+02:00',
               '2026-10-20T18:00:00+02:00','2026-10-21T08:00:00+02:00']
        self.assertEqual(default_end_slot(slots,5),slots[2])

    def test_spring_forward_uses_elapsed_24_hours(self):
        slots=['2026-03-28T09:00:00+01:00','2026-03-29T09:00:00+02:00',
               '2026-03-29T10:00:00+02:00','2026-03-30T09:00:00+02:00']
        self.assertEqual(default_end_slot(slots,5),slots[2])

    def test_autumn_fallback_uses_elapsed_24_hours(self):
        slots=['2026-10-24T09:00:00+02:00','2026-10-25T07:00:00+01:00',
               '2026-10-25T08:00:00+01:00','2026-10-25T09:00:00+01:00']
        self.assertEqual(default_end_slot(slots,5),slots[2])

    def test_weekend_chooses_next_offered_monday(self):
        slots=list(weekly_handover_slots(datetime.fromisoformat('2026-10-23T19:30:00+02:00')))
        self.assertEqual(default_end_slot(slots,5),'2026-10-24T20:00:00+02:00')
        saturday=[slot for slot in slots if slot>='2026-10-24T20:00:00+02:00']
        self.assertEqual(default_end_slot(saturday,5),'2026-10-26T08:00:00+01:00')

    def test_short_window_falls_back_to_last_later_slot(self):
        slots=['2026-10-19T08:00:00+02:00','2026-10-19T09:00:00+02:00',
               '2026-10-19T20:00:00+02:00']
        self.assertEqual(default_end_slot(slots,5),slots[-1])

    def test_limit_excludes_sixth_day_before_fallback(self):
        slots=['2026-10-19T16:00:00+02:00','2026-10-19T20:00:00+02:00',
               '2026-10-24T20:00:00+02:00']
        self.assertEqual(default_end_slot(slots,5),slots[1])

    def test_no_eligible_later_slot_has_no_default(self):
        for slots in ([],['2026-10-19T16:00:00+02:00'],
                      ['2026-10-19T16:00:00+02:00','2026-10-24T20:00:00+02:00']):
            with self.subTest(slots=slots):self.assertIsNone(default_end_slot(slots,5))

    def test_test_mode_longer_limit_is_preserved(self):
        slots=['2026-10-19T16:00:00+02:00','2026-10-24T20:00:00+02:00']
        self.assertEqual(default_end_slot(slots,30),slots[1])

    def test_real_template_selects_one_default_in_live_and_test_mode(self):
        env=Environment(loader=FileSystemLoader(ROOT/'templates'),autoescape=True)
        env.filters['mos_slot']=lambda value:value
        env.globals.update(url_for=lambda endpoint:'/'+endpoint,csrf_field=lambda:'')
        slots=['2026-10-19T16:00:00+02:00','2026-10-20T16:00:00+02:00',
               '2026-10-24T20:00:00+02:00']
        for live in (False,True):
            with self.subTest(live=live):
                rendered=env.get_template('mos_public/index.html').render(
                    slots=slots,default_end=default_end_slot(slots,5),live=live,
                    config={},booking_config={},listings={'i10':'Hyundai i10'})
                end_select=re.search(r'<select name="end" id="end">(.*?)</select>',rendered,re.S)[1]
                self.assertEqual(re.findall(r'<option value="([^"]*)"[^>]*\bselected\b',end_select),[slots[1]])
        rendered=env.get_template('mos_public/index.html').render(
            slots=[slots[0]],default_end=None,live=True,config={},booking_config={},listings={})
        self.assertIn('<option value="" disabled selected>Bitte Rückgabetermin wählen</option>',rendered)


if __name__=='__main__':unittest.main()
