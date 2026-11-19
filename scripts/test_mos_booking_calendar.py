"""Calendar form and staff day filtering against a fresh synthetic portal database."""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
import sys
import tempfile
import unittest
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
from run_mos_public_test import build_test_app
from mos_public_booking import WEEKLY_HANDOVER, init_slot_schema, local_slot_to_iso

BERLIN=ZoneInfo('Europe/Berlin')


class BookingCalendarTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='mos-calendar-')
        cls.portal=build_test_app(cls.temp.name,origin='http://localhost')
        cls.base_config=deepcopy(cls.portal.app.config['MOS_PUBLIC_BOOKING'])

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.portal.app.config.update(TESTING=True)
        self.portal.app.extensions.pop('mos_public_booking',None)
        cfg=deepcopy(self.base_config)
        cfg.update(enabled=True,slots=[],weekly_handover=deepcopy(WEEKLY_HANDOVER),max_days=5)
        self.portal.app.config['MOS_PUBLIC_BOOKING']=cfg
        db=self.portal.get_db()
        try:
            init_slot_schema(db,[])
            db.execute('DELETE FROM miet_checkout_slots')
            db.commit()
        finally:db.close()
        self.client=self.portal.app.test_client()
        self.assertEqual(self.client.get('/mietwagen-test/').status_code,200)
        self.admin=self.portal.app.test_client()
        with self.admin.session_transaction() as session:session['admin']=True
        self.assertEqual(self.admin.get('/mietwagen-test/admin/termine').status_code,200)
        future=datetime.now(BERLIN).date()+timedelta(days=300)
        self.day=future+timedelta(days=(1-future.weekday())%7)  # Tuesday, about ten months ahead

    def post(self,client,path,data):
        with client.session_transaction() as session:token=session['csrf_token']
        return client.post(path,data={**data,'csrf_token':token})

    def quote(self,**changes):
        data={'vehicle':'i10','start_date':self.day.isoformat(),'start_time':'10:00',
              'end_date':(self.day+timedelta(days=1)).isoformat(),'end_time':'10:00'}
        data.update(changes)
        return self.post(self.client,'/mietwagen-test/quote',data)

    def test_calendar_fields_quote_ten_months_ahead_without_opening_each_time(self):
        response=self.quote()
        self.assertEqual(response.status_code,200,response.get_data(as_text=True))
        text=response.get_data(as_text=True)
        self.assertIn(self.day.strftime('%d.%m.%Y'),text)
        db=self.portal.get_db()
        try:
            rows=db.execute('SELECT slot,active FROM miet_checkout_slots').fetchall()
            self.assertEqual({row['slot'] for row in rows},
                             {local_slot_to_iso(self.day.isoformat()+'T10:00'),
                              local_slot_to_iso((self.day+timedelta(days=1)).isoformat()+'T10:00')})
        finally:db.close()

    def test_unavailable_sunday_and_closed_time_are_rejected_server_side(self):
        sunday=self.day+timedelta(days=(6-self.day.weekday())%7)
        self.assertEqual(self.quote(start_date=sunday.isoformat(),
                                    end_date=(sunday+timedelta(days=1)).isoformat()).status_code,409)
        slot=local_slot_to_iso(self.day.isoformat()+'T10:00')
        closed=self.post(self.admin,'/mietwagen-test/admin/termine',{'action':'close','slot':slot})
        self.assertEqual(closed.status_code,303)
        self.assertEqual(self.quote().status_code,409)
        page=self.client.get('/mietwagen-test/').get_data(as_text=True)
        available=json.loads(re.search(r'<script id="booking-slots" type="application/json">(.*?)</script>',page,re.S)[1])
        self.assertNotIn(slot,available)

    def test_public_availability_and_direct_booking_share_the_calendar(self):
        fields = {'vehicle': 'i10', 'start_date': self.day.isoformat(), 'start_time': '10:00',
                  'end_date': (self.day + timedelta(days=1)).isoformat(), 'end_time': '10:00'}
        path = '/mietwagen-test/availability'
        origin = 'https://autovermietung-mos.de'
        response = self.portal.app.test_client().get(path, query_string=fields, headers={'Origin': origin})
        self.assertEqual(response.json, {'available': True})
        self.assertEqual(response.headers['Access-Control-Allow-Origin'], origin)
        self.assertNotIn('Access-Control-Allow-Credentials', response.headers)
        self.assertNotIn('Access-Control-Allow-Origin', self.client.get(
            path, query_string=fields, headers={'Origin': 'https://unrelated.example'}).headers)
        for method in ('online', 'onsite'):
            page = self.client.get('/mietwagen-test/buchen', query_string={**fields, 'payment_method': method})
            self.assertEqual(page.status_code, 200)
            self.assertIn('name="quote_token"', page.get_data(as_text=True))
            self.assertIn('Auswahl ändern', page.get_data(as_text=True))
        db = self.portal.get_db()
        try:
            # Even a full booking preview creates no slots, holds or reservations on GET.
            for table in ('miet_checkout_slots', 'miet_checkout_holds', 'mietvorgaenge'):
                self.assertEqual(db.execute('SELECT count(*) AS n FROM ' + table).fetchone()['n'], 0)
            slot = local_slot_to_iso(fields['end_date'] + 'T' + fields['end_time'])
            db.execute('INSERT INTO miet_checkout_slots (slot,active,created_at,updated_at) VALUES (?,0,?,?)',
                       (slot, slot, slot))
            db.commit()
        finally:
            db.close()
        self.assertEqual(self.client.get(path, query_string=fields).json['reason'], 'closed')
        self.assertEqual(self.client.get(path, query_string={**fields, 'start_date':'bad'}).json['reason'], 'invalid')
        self.assertEqual(self.client.get(path, query_string={**fields, 'vehicle':'unknown'}).json['reason'], 'unavailable')
        sunday = self.day + timedelta(days=6-self.day.weekday())
        self.assertEqual(self.client.get(path, query_string={
            **fields, 'start_date': sunday.isoformat(), 'end_date': (sunday+timedelta(days=1)).isoformat()
        }).json['reason'], 'closed')

    def test_manual_calendar_booking_blocks_each_touched_day_and_only_one_vehicle(self):
        fields = {'vehicle':'i10', 'start_date':self.day.isoformat(), 'start_time':'10:00',
                  'end_date':(self.day+timedelta(days=1)).isoformat(), 'end_time':'10:00'}
        vehicle_id = self.base_config['fleet']['i10']['id']
        identifier = self.portal.create_mietvorgang(vehicle_id, kunde_name='Kalendertest',
                                                     start_datum=fields['end_date'], end_datum=fields['end_date'])
        try:
            for start, end in ((self.day, self.day+timedelta(days=1)),
                               (self.day+timedelta(days=1), self.day+timedelta(days=2))):
                fields.update(start_date=start.isoformat(), end_date=end.isoformat())
                answer = self.client.get('/mietwagen-test/availability', query_string=fields)
                self.assertEqual((answer.json['available'], answer.json['reason']), (False, 'occupied'))
                self.assertNotIn('Kalendertest', answer.get_data(as_text=True))
                self.assertEqual(self.client.get('/mietwagen-test/buchen', query_string=fields).status_code, 409)
                self.assertEqual(self.client.get('/mietwagen-test/availability',
                                 query_string={**fields, 'vehicle':'kona'}).json, {'available': True})
        finally:
            db = self.portal.get_db()
            try:
                db.execute('DELETE FROM mietvorgaenge WHERE id=?', (identifier,))
                db.commit()
            finally:
                db.close()

    def test_invalid_calendar_fields_and_reverse_or_equal_return_are_rejected(self):
        for changes in ({'start_date':'2027-02-30'}, {'start_date':'not-a-date'},
                        {'start_time':'25:00'}, {'start_time':'10:30'},
                        {'end_date':self.day.isoformat(),'end_time':'10:00'},
                        {'end_date':self.day.isoformat(),'end_time':'09:00'}):
            with self.subTest(changes=changes):
                self.assertIn(self.quote(**changes).status_code,(400,409))

    def test_calendar_post_and_admin_slot_changes_require_csrf_and_admin(self):
        self.assertEqual(self.client.post('/mietwagen-test/quote',data={
            'vehicle':'i10','start_date':self.day.isoformat(),'start_time':'10:00',
            'end_date':(self.day+timedelta(days=1)).isoformat(),'end_time':'10:00'}).status_code,400)
        self.assertEqual(self.admin.post('/mietwagen-test/admin/termine',data={
            'action':'close','slot':local_slot_to_iso(self.day.isoformat()+'T10:00')}).status_code,400)
        self.assertEqual(self.client.get('/mietwagen-test/admin/termine').status_code,302)

    def test_staff_day_filter_shows_only_selected_days_thirteen_regular_times(self):
        response=self.admin.get('/mietwagen-test/admin/termine?day='+self.day.isoformat())
        self.assertEqual(response.status_code,200)
        text=response.get_data(as_text=True)
        close_slots=re.findall(r'name="slot" value="([^"]+)"',text)
        self.assertEqual(len(close_slots),13)
        self.assertTrue(all(slot.startswith(self.day.isoformat()+'T') for slot in close_slots))
        self.assertIn('Regelmäßige Termine müssen nicht einzeln angelegt werden.',text)
        self.assertEqual(self.admin.get('/mietwagen-test/admin/termine?day=2027-02-30').status_code,400)

    def test_staff_day_filter_keeps_closed_times_on_their_own_day(self):
        other_day=self.day+timedelta(days=1)
        here=local_slot_to_iso(self.day.isoformat()+'T10:00')
        elsewhere=local_slot_to_iso(other_day.isoformat()+'T10:00')
        for slot in (here,elsewhere):
            self.assertEqual(self.post(self.admin,'/mietwagen-test/admin/termine',
                                       {'action':'close','slot':slot}).status_code,303)
        text=self.admin.get('/mietwagen-test/admin/termine?day='+self.day.isoformat()).get_data(as_text=True)
        self.assertIn('value="'+here+'"',text)
        self.assertNotIn('value="'+elsewhere+'"',text)
        self.assertEqual(len(re.findall(r'name="slot" value="([^"]+)"',text)),13)


if __name__=='__main__':unittest.main()
