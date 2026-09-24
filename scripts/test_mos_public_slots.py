"""Admin-managed handover times are authoritative even before launch."""
from datetime import datetime, timedelta, timezone
from base64 import b64encode
import html
from io import BytesIO
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo
from PIL import Image, ImageDraw

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
from run_mos_public_test import build_test_app
from mos_public_booking import (WEEKLY_HANDOVER, WEEKLY_SLOT_HORIZON_DAYS,
                                init_slot_schema, local_slot_to_iso, selected_slots_open,
                                weekly_handover_enabled, weekly_handover_slots)


class SlotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='mos-slots-')
        cls.portal=build_test_app(cls.temp.name,origin='http://localhost')

    @classmethod
    def tearDownClass(cls):cls.temp.cleanup()

    def setUp(self):
        self.portal.app.config.update(TESTING=True)
        self.portal.app.extensions.pop('mos_public_booking',None)
        cfg=self.portal.app.config['MOS_PUBLIC_BOOKING']
        cfg['enabled']=False
        cfg['slots']=[]
        cfg.pop('weekly_handover',None)
        cfg.pop('deposit_method',None)
        db=self.portal.get_db()
        try:
            init_slot_schema(db,[])
            tables={r['name'] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            for table in ('miet_checkout_refunds','miet_checkout_cancellations','miet_checkout_contracts',
                          'miet_checkout_events','miet_checkout_deposit_auths','miet_checkout_creation_attempts',
                          'miet_checkout_holds','mietvorgaenge'):
                if table in tables:db.execute('DELETE FROM '+table)
            db.execute('DELETE FROM miet_checkout_slots')
            db.commit()
        finally:db.close()
        self.client=self.portal.app.test_client()
        with self.client.session_transaction() as session:session['admin']=True
        self.assertEqual(self.client.get('/mietwagen-test/admin/termine').status_code,200)

    def post(self,path,data):
        with self.client.session_transaction() as session:token=session['csrf_token']
        return self.client.post(path,data={**data,'csrf_token':token})

    def open(self,dt):
        local=dt.astimezone(ZoneInfo('Europe/Berlin')).strftime('%Y-%m-%dT%H:%M')
        response=self.post('/mietwagen-test/admin/termine',{'action':'add','local_slot':local})
        self.assertEqual(response.status_code,303)
        return local_slot_to_iso(local)

    def signature(self):
        image=Image.new('RGBA',(700,180),(255,255,255,0))
        draw=ImageDraw.Draw(image)
        draw.line([(40,110),(90,40),(125,125),(180,60),(240,110),(320,50),(380,105)],
                  fill=(20,30,40,255),width=5)
        output=BytesIO();image.save(output,format='PNG')
        return 'data:image/png;base64,'+b64encode(output.getvalue()).decode('ascii')

    def test_admin_auth_csrf_and_prelaunch_access(self):
        self.assertNotIn('mos_public_booking',self.portal.app.extensions)
        self.assertEqual(self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled'],False)
        anonymous=self.portal.app.test_client()
        self.assertEqual(anonymous.get('/mietwagen-test/admin/termine').status_code,302)
        self.assertEqual(self.client.post('/mietwagen-test/admin/termine',data={'action':'add'}).status_code,400)
        self.assertNotIn('mos_public_booking',self.portal.app.extensions)

    def test_weekly_hours_include_same_day_and_20h_but_not_sunday(self):
        berlin=ZoneInfo('Europe/Berlin')
        monday=datetime(2026,9,28,19,30,tzinfo=berlin)
        slots=list(weekly_handover_slots(monday))
        self.assertEqual(slots[0],'2026-09-28T20:00:00+02:00')
        self.assertNotIn('2026-09-28T19:00:00+02:00',slots)
        self.assertNotIn('2026-09-28T21:00:00+02:00',slots)
        self.assertIn('2026-10-03T20:00:00+02:00',slots)
        self.assertFalse(any(s.startswith('2026-10-04') for s in slots))
        self.assertFalse(any(datetime.fromisoformat(s).date() >=
                             (monday.date()+timedelta(days=WEEKLY_SLOT_HORIZON_DAYS)) for s in slots))

    def test_weekly_rule_rejects_unconfirmed_hours(self):
        wrong=dict(WEEKLY_HANDOVER,first_hour=7)
        with self.assertRaisesRegex(ValueError,'08 bis 20'):
            weekly_handover_enabled({'weekly_handover':wrong})

    def test_weekly_hours_use_berlin_offset_across_summer_and_winter_change(self):
        berlin=ZoneInfo('Europe/Berlin')
        spring=list(weekly_handover_slots(datetime(2026,3,28,19,30,tzinfo=berlin)))
        autumn=list(weekly_handover_slots(datetime(2026,10,24,19,30,tzinfo=berlin)))
        self.assertIn('2026-03-28T20:00:00+01:00',spring)
        self.assertIn('2026-03-30T08:00:00+02:00',spring)
        self.assertFalse(any(s.startswith('2026-03-29') for s in spring))
        self.assertIn('2026-10-24T20:00:00+02:00',autumn)
        self.assertIn('2026-10-26T08:00:00+01:00',autumn)
        self.assertFalse(any(s.startswith('2026-10-25') for s in autumn))

    def test_weekly_virtual_slots_respect_launch_gate_and_admin_override(self):
        cfg=self.portal.app.config['MOS_PUBLIC_BOOKING']
        cfg['weekly_handover']=dict(WEEKLY_HANDOVER)
        slots=list(weekly_handover_slots())
        self.assertGreater(len(slots),1)
        a,b=slots[:2]
        # Staff can close dates in advance, while customers still see no
        # booking flow before the explicit launch/insurance gates.
        self.assertIn(a,self.client.get('/mietwagen-test/admin/termine').get_data(as_text=True))
        self.assertEqual(self.client.get('/mietwagen-test/').status_code,404)
        cfg['enabled']=True
        self.assertIn(a,self.client.get('/mietwagen-test/').get_data(as_text=True))
        db=self.portal.get_db()
        try:self.assertIsNone(db.execute('SELECT slot FROM miet_checkout_slots WHERE slot=?',(a,)).fetchone())
        finally:db.close()
        # Closing a still-virtual time creates a durable inactive override.
        self.assertEqual(self.post('/mietwagen-test/admin/termine',{'action':'close','slot':a}).status_code,303)
        self.assertNotIn(a,self.client.get('/mietwagen-test/').get_data(as_text=True))
        self.client.get('/mietwagen-test/admin/termine')
        self.assertNotIn(a,self.client.get('/mietwagen-test/').get_data(as_text=True))
        self.assertEqual(self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b}).status_code,409)
        self.assertEqual(self.post('/mietwagen-test/admin/termine',{'action':'reopen','slot':a}).status_code,303)
        self.assertEqual(self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b}).status_code,200)

    def test_weekly_quote_materializes_selected_slots_for_deposit_and_checkout(self):
        cfg=self.portal.app.config['MOS_PUBLIC_BOOKING']
        cfg['weekly_handover']=dict(WEEKLY_HANDOVER)
        cfg['deposit_method']='card_authorization_at_booking'
        cfg['enabled']=True
        now=datetime.now(timezone.utc)
        future=[s for s in weekly_handover_slots(now) if
                datetime.fromisoformat(s).astimezone(timezone.utc)>now+timedelta(hours=2)]
        a,b=future[:2]
        self.assertEqual(self.client.get('/mietwagen-test/').status_code,200)
        quote=self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b})
        self.assertEqual(quote.status_code,200)
        db=self.portal.get_db()
        try:
            rows=db.execute('SELECT slot,active FROM miet_checkout_slots WHERE slot IN (?,?)',(a,b)).fetchall()
            self.assertEqual({r['slot'] for r in rows if r['active']==1},{a,b})
        finally:db.close()
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',quote.get_data(as_text=True))[1])
        checkout=self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes',
            'sign_confirm':'yes','name':'Test','email':'test@example.invalid','signature_data':self.signature()})
        self.assertEqual(checkout.status_code,303)
        hold=checkout.location.rsplit('/',2)[-2]
        self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test',{}).status_code,303)
        rent=self.post('/mietwagen-test/status/'+hold+'/retry',{})
        self.assertEqual(rent.status_code,303)
        self.assertEqual(self.post(rent.location,{}).status_code,303)
        self.assertEqual(self.portal.app.extensions['mos_public_booking']['service'].read(hold)['status'],'confirmed')

    def test_weekly_virtual_slots_reject_outside_horizon_without_manual_open(self):
        now=datetime(2026,9,28,9,30,tzinfo=ZoneInfo('Europe/Berlin'))
        start='2026-09-28T10:00:00+02:00'
        end='2026-09-28T11:00:00+02:00'
        too_far='2026-10-05T10:00:00+02:00'
        db=self.portal.get_db()
        try:
            self.assertTrue(selected_slots_open(db,start,end,weekly=True,now=now))
            self.assertFalse(selected_slots_open(db,start,too_far,weekly=True,now=now))
            self.assertFalse(selected_slots_open(db,start,'2026-10-04T10:00:00+02:00',weekly=True,now=now))
        finally:db.close()

    def test_open_close_and_reopen_are_shared_by_page_and_quote(self):
        first=datetime.now(timezone.utc)+timedelta(hours=2)
        last=first+timedelta(days=1,hours=1)
        a,b=self.open(first),self.open(last)
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        page=self.client.get('/mietwagen-test/')
        self.assertEqual(page.status_code,200)
        self.assertIn(a,page.get_data(as_text=True))
        self.assertIn(b,page.get_data(as_text=True))
        quote=self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b})
        self.assertEqual(quote.status_code,200)
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',quote.get_data(as_text=True))[1])

        self.assertEqual(self.post('/mietwagen-test/admin/termine',{'action':'close','slot':a}).status_code,303)
        db=self.portal.get_db()
        try:
            init_slot_schema(db,[a])  # A config reload must not silently reopen a closed date.
            db.commit()
            self.assertEqual(db.execute('SELECT active FROM miet_checkout_slots WHERE slot=?',(a,)).fetchone()['active'],0)
        finally:db.close()
        self.assertNotIn(a,self.client.get('/mietwagen-test/').get_data(as_text=True))
        self.assertEqual(self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b}).status_code,409)
        # A signed price preview cannot bypass an appointment closed before checkout.
        rejected=self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes',
            'sign_confirm':'yes','name':'Test','email':'test@example.invalid','signature_data':'invalid'})
        self.assertEqual(rejected.status_code,409)
        self.assertEqual(self.post('/mietwagen-test/admin/termine',{'action':'reopen','slot':a}).status_code,303)
        self.assertEqual(self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b}).status_code,200)

    def test_invalid_and_arbitrary_times_are_rejected(self):
        for local in ('2026-03-29T02:30','2026-10-25T02:30','2030-13-01T09:00'):
            self.assertEqual(self.post('/mietwagen-test/admin/termine',
                             {'action':'add','local_slot':local}).status_code,409)
        past=(datetime.now(ZoneInfo('Europe/Berlin'))-timedelta(days=1)).strftime('%Y-%m-%dT%H:%M')
        self.assertEqual(self.post('/mietwagen-test/admin/termine',
                         {'action':'add','local_slot':past}).status_code,409)
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        self.assertIn('keine persönlichen Übergabetermine',self.client.get('/mietwagen-test/').get_data(as_text=True))
        self.assertEqual(self.post('/mietwagen-test/quote',{'vehicle':'i10',
            'start':'2030-01-01T09:00:00+01:00','end':'2030-01-02T09:00:00+01:00'}).status_code,409)

    def test_closing_after_card_authorization_prevents_rent_checkout(self):
        self.portal.app.config['MOS_PUBLIC_BOOKING']['deposit_method']='card_authorization_at_booking'
        first=datetime.now(timezone.utc)+timedelta(hours=2)
        a,b=self.open(first),self.open(first+timedelta(days=1,hours=1))
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        self.client.get('/mietwagen-test/')
        quote=self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b})
        self.assertEqual(quote.status_code,200)
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',quote.get_data(as_text=True))[1])
        checkout=self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes',
            'sign_confirm':'yes','name':'Test','email':'test@example.invalid','signature_data':self.signature()})
        self.assertEqual(checkout.status_code,303)
        hold=checkout.location.rsplit('/',2)[-2]
        self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test',{}).status_code,303)

        # Even if a close bypasses the admin cleanup, retry must release the card hold.
        db=self.portal.get_db()
        try:
            db.execute('UPDATE miet_checkout_slots SET active=0 WHERE slot=?',(a,));db.commit()
        finally:db.close()
        blocked=self.post('/mietwagen-test/status/'+hold+'/retry',{})
        self.assertEqual(blocked.status_code,409)
        state=self.portal.app.extensions['mos_public_booking']
        self.assertEqual(state['service'].read(hold)['status'],'released')
        deposit=state['service']._deposit_record(hold)
        self.assertEqual(deposit['status'],'released')
        self.assertIsNone(state['service'].read(hold)['session_id'])

    def test_closing_an_open_checkout_expires_it(self):
        self.portal.app.config['MOS_PUBLIC_BOOKING']['deposit_method']='card_authorization_at_booking'
        first=datetime.now(timezone.utc)+timedelta(hours=2)
        a,b=self.open(first),self.open(first+timedelta(days=1,hours=1))
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        self.client.get('/mietwagen-test/')
        quote=self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b})
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',quote.get_data(as_text=True))[1])
        checkout=self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes',
            'sign_confirm':'yes','name':'Test','email':'test@example.invalid','signature_data':self.signature()})
        hold=checkout.location.rsplit('/',2)[-2]
        self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test',{}).status_code,303)
        self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/retry',{}).status_code,303)
        state=self.portal.app.extensions['mos_public_booking']
        self.assertIsNotNone(state['service'].read(hold)['session_id'])
        self.assertEqual(self.post('/mietwagen-test/admin/termine',{'action':'close','slot':a}).status_code,303)
        self.assertEqual(state['service'].read(hold)['status'],'released')
        self.assertEqual(state['service']._deposit_record(hold)['status'],'released')

    def test_closing_slot_keeps_existing_confirmed_rental(self):
        self.portal.app.config['MOS_PUBLIC_BOOKING']['deposit_method']='card_authorization_at_booking'
        first=datetime.now(timezone.utc)+timedelta(hours=2)
        a,b=self.open(first),self.open(first+timedelta(days=1,hours=1))
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        self.client.get('/mietwagen-test/')
        quote=self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b})
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',quote.get_data(as_text=True))[1])
        checkout=self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes',
            'sign_confirm':'yes','name':'Test','email':'test@example.invalid','signature_data':self.signature()})
        hold=checkout.location.rsplit('/',2)[-2]
        self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test',{}).status_code,303)
        rent=self.post('/mietwagen-test/status/'+hold+'/retry',{})
        self.assertEqual(rent.status_code,303)
        self.assertEqual(self.post(rent.location,{}).status_code,303)
        state=self.portal.app.extensions['mos_public_booking']
        before=state['service'].read(hold)
        self.assertEqual(before['status'],'confirmed')
        self.assertEqual(self.post('/mietwagen-test/admin/termine',{'action':'close','slot':a}).status_code,303)
        after=state['service'].read(hold)
        self.assertEqual(after['status'],'confirmed')
        self.assertEqual(after['mietvorgang_id'],before['mietvorgang_id'])

    def test_admin_close_during_gateway_create_keeps_session_for_review(self):
        self.portal.app.config['MOS_PUBLIC_BOOKING']['deposit_method']='card_authorization_at_booking'
        first=datetime.now(timezone.utc)+timedelta(hours=2)
        a,b=self.open(first),self.open(first+timedelta(days=1,hours=1))
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        self.client.get('/mietwagen-test/')
        quote=self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b})
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',quote.get_data(as_text=True))[1])
        checkout=self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes',
            'sign_confirm':'yes','name':'Test','email':'test@example.invalid','signature_data':self.signature()})
        hold=checkout.location.rsplit('/',2)[-2]
        self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test',{}).status_code,303)
        state=self.portal.app.extensions['mos_public_booking']
        admin=self.portal.app.test_client()
        with admin.session_transaction() as session:session['admin']=True
        self.assertEqual(admin.get('/mietwagen-test/admin/termine').status_code,200)
        with admin.session_transaction() as session:csrf=session['csrf_token']
        created=[]
        original_create=state['gateway'].create

        def create_while_admin_closes(params,key):
            session=original_create(params,key)
            created.append(session['id'])
            closed=admin.post('/mietwagen-test/admin/termine',data={
                'csrf_token':csrf,'action':'close','slot':a})
            self.assertEqual(closed.status_code,303)
            return session

        with (patch.object(state['gateway'],'create',side_effect=create_while_admin_closes),
              patch.object(self.portal.app.logger,'exception')):
            response=self.post('/mietwagen-test/status/'+hold+'/retry',{})
        self.assertEqual(response.status_code,503)
        self.assertNotIn('Location',response.headers)
        self.assertEqual(len(created),1)
        after=state['service'].read(hold)
        self.assertEqual(after['session_id'],created[0])
        self.assertEqual(after['status'],'review')
        self.assertEqual(state['gateway'].retrieve(created[0])['status'],'expired')

    def test_admin_close_during_deposit_creation_keeps_intent_for_review(self):
        self.portal.app.config['MOS_PUBLIC_BOOKING']['deposit_method']='card_authorization_at_booking'
        first=datetime.now(timezone.utc)+timedelta(hours=2)
        a,b=self.open(first),self.open(first+timedelta(days=1,hours=1))
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        self.client.get('/mietwagen-test/')
        quote=self.post('/mietwagen-test/quote',{'vehicle':'i10','start':a,'end':b})
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',quote.get_data(as_text=True))[1])
        state=self.portal.app.extensions['mos_public_booking']
        admin=self.portal.app.test_client()
        with admin.session_transaction() as session:session['admin']=True
        self.assertEqual(admin.get('/mietwagen-test/admin/termine').status_code,200)
        with admin.session_transaction() as session:csrf=session['csrf_token']
        created=[]
        original_create=state['gateway'].create_deposit_intent

        def create_intent_while_admin_closes(params,key):
            intent=original_create(params,key)
            created.append(intent['id'])
            closed=admin.post('/mietwagen-test/admin/termine',data={
                'csrf_token':csrf,'action':'close','slot':a})
            self.assertEqual(closed.status_code,303)
            return intent

        with (patch.object(state['gateway'],'create_deposit_intent',side_effect=create_intent_while_admin_closes),
              patch.object(self.portal.app.logger,'exception')):
            response=self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes',
                'sign_confirm':'yes','name':'Test','email':'test@example.invalid','signature_data':self.signature()})
        self.assertEqual(len(created),1)
        self.assertNotIn('Location',response.headers)
        self.assertIn(response.status_code,(409,503))
        db=self.portal.get_db()
        try:hold=db.execute('SELECT id FROM miet_checkout_holds').fetchone()['id']
        finally:db.close()
        after=state['service'].read(hold)
        deposit=state['service']._deposit_record(hold)
        self.assertEqual(after['status'],'review')
        self.assertIsNone(after['session_id'])
        self.assertEqual(deposit['status'],'review')
        self.assertEqual(deposit['intent_id'],created[0])


if __name__=='__main__':unittest.main()
