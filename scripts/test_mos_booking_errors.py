"""Public selection errors remain actionable and retain the customer's dates."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import html
from pathlib import Path
import re
import sys
import tempfile
import unittest
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
from run_mos_public_test import build_test_app
from mos_public_booking import WEEKLY_HANDOVER, checked_rental_window

BERLIN=ZoneInfo('Europe/Berlin')


class BookingErrorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory(prefix='mos-errors-')
        cls.portal=build_test_app(cls.tmp.name,origin='http://localhost')
        cls.base=deepcopy(cls.portal.app.config['MOS_PUBLIC_BOOKING'])

    @classmethod
    def tearDownClass(cls):cls.tmp.cleanup()

    def setUp(self):
        cfg=deepcopy(self.base)
        cfg.update(slots=[],weekly_handover=deepcopy(WEEKLY_HANDOVER),max_days=5,
                   deposit_method='card_authorization_at_booking',deposit_timing='before_pickup')
        self.portal.app.config.update(TESTING=True,MOS_PUBLIC_BOOKING=cfg)
        self.portal.app.extensions.pop('mos_public_booking',None)
        self.client=self.portal.app.test_client()
        self.client.get('/mietwagen-test/')
        day=datetime.now(BERLIN).date()+timedelta(days=60)
        self.day=day+timedelta(days=(-day.weekday())%7)  # Monday
        self.fields={'vehicle':'i10','start_date':self.day.isoformat(),'start_time':'14:00',
                     'end_date':(self.day+timedelta(days=1)).isoformat(),'end_time':'14:00'}

    def post(self,**changes):
        with self.client.session_transaction() as session:csrf=session['csrf_token']
        return self.client.post('/mietwagen-test/quote',data={**self.fields,**changes,'csrf_token':csrf})

    def test_ten_day_error_explains_limit_and_keeps_every_field(self):
        end=(self.day+timedelta(days=10)).isoformat()
        response=self.post(end_date=end)
        self.assertEqual(response.status_code,409)
        page=response.get_data(as_text=True)
        self.assertIn('10 berechnete Miettage',page)
        self.assertIn('auf 5 Miettage begrenzt',page)
        self.assertNotIn('Ungültiger Mietzeitraum.',page)
        self.assertIn('id="booking-dates"',page)
        for name,value in {**self.fields,'end_date':end}.items():
            if name=='vehicle':
                self.assertRegex(page,r'<option value="i10"[^>]*selected')
            elif name.endswith('_date'):
                self.assertRegex(page,r'name="'+name+r'"[^>]*value="'+value+r'"')
            else:
                self.assertIn('value="'+value+'" selected',page)

    def test_five_days_plus_one_hour_gets_the_correct_six_day_message(self):
        end=(self.day+timedelta(days=5)).isoformat()  # Saturday is open
        self.assertEqual(self.post(end_date=end,end_time='14:00').status_code,200)
        response=self.post(end_date=end,end_time='15:00')
        self.assertEqual(response.status_code,409)
        self.assertIn('6 berechnete Miettage',response.get_data(as_text=True))

    def test_change_selection_link_keeps_vehicle_dates_and_times(self):
        response=self.post()
        self.assertEqual(response.status_code,200)
        page=response.get_data(as_text=True)
        url=html.unescape(re.search(r'<a href="([^"]+)">Auswahl ändern</a>',page)[1])
        changed=self.client.get(url)
        self.assertEqual(changed.status_code,200)
        page=changed.get_data(as_text=True)
        self.assertRegex(page,r'<option value="i10"[^>]*selected')
        for name in ('start_date','end_date'):
            self.assertRegex(page,r'name="'+name+r'"[^>]*value="'+self.fields[name]+r'"')

    def test_reversed_return_and_past_pickup_have_specific_messages(self):
        reversed_response=self.post(end_date=self.day.isoformat(),end_time='13:00')
        self.assertIn('Rückgabe muss nach der Abholung',reversed_response.get_data(as_text=True))
        past=(datetime.now(BERLIN).date()-timedelta(days=2)).isoformat()
        response=self.post(start_date=past)
        self.assertIn('Abholtermin liegt bereits in der Vergangenheit',response.get_data(as_text=True))

    def test_explicitly_empty_fields_remain_empty_after_error(self):
        response=self.post(end_date='',end_time='')
        self.assertEqual(response.status_code,409)
        page=response.get_data(as_text=True)
        self.assertRegex(page,r'name="end_date"[^>]*value=""')
        self.assertIn('<option value="" selected>Bitte Uhrzeit wählen</option>',page)

    def test_query_prefill_ignores_contact_and_untrusted_markup_is_escaped(self):
        response=self.client.get('/mietwagen-test/',query_string={
            **self.fields,'email':'private@example.invalid','start_time':'"><script>alert(1)</script>'})
        page=response.get_data(as_text=True)
        self.assertNotIn('private@example.invalid',page)
        self.assertNotIn('<script>alert(1)</script>',page)


class ElapsedRentalWindowTests(unittest.TestCase):
    def test_dst_boundaries_use_elapsed_24_hours(self):
        now=datetime(2026,1,1,tzinfo=timezone.utc)
        spring_start='2027-03-24T14:00:00+01:00'
        spring_end='2027-03-29T15:00:00+02:00'
        self.assertEqual(checked_rental_window(spring_start,spring_end,5,now)[2],5)
        autumn_start='2027-10-27T14:00:00+02:00'
        with self.assertRaisesRegex(ValueError,'6 berechnete Miettage'):
            checked_rental_window(autumn_start,'2027-11-01T14:00:00+01:00',5,now)
        self.assertEqual(checked_rental_window(autumn_start,'2027-11-01T13:00:00+01:00',5,now)[2],5)

    def test_limit_is_configuration_driven(self):
        now=datetime(2026,1,1,tzinfo=timezone.utc)
        self.assertEqual(checked_rental_window('2027-01-04T14:00:00+01:00',
                                             '2027-01-11T14:00:00+01:00',7,now)[2],7)


if __name__=='__main__':unittest.main()
