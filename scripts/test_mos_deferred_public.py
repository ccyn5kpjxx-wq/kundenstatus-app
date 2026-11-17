"""Deferred public booking routes with synthetic inventory and an offline provider."""
from base64 import b64encode
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import html
from io import BytesIO
import json
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid
from zoneinfo import ZoneInfo

from itsdangerous import URLSafeTimedSerializer
from PIL import Image, ImageDraw

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
from run_mos_public_test import build_test_app
from mos_public_booking import WEEKLY_HANDOVER, default_end_slot, weekly_handover_slots
from mos_public_contract import presign_quote
from mos_deposit_reminder import create_access_token

BERLIN=ZoneInfo('Europe/Berlin')


class DeferredPublicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='mos-deferred-public-')
        cls.portal=build_test_app(cls.temp.name,origin='http://localhost')
        cls.base_config=deepcopy(cls.portal.app.config['MOS_PUBLIC_BOOKING'])
        image=Image.new('RGBA',(700,180),(255,255,255,0))
        ImageDraw.Draw(image).line([(40,110),(90,40),(125,125),(180,60),(240,110),(320,50),(380,105)],
                                  fill=(20,30,40,255),width=5)
        output=BytesIO();image.save(output,'PNG')
        cls.signature='data:image/png;base64,'+b64encode(output.getvalue()).decode('ascii')

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        cfg=deepcopy(self.base_config)
        cfg.update(enabled=True,slots=[],weekly_handover=deepcopy(WEEKLY_HANDOVER),max_days=5,
                   deposit_method='card_authorization_at_booking',deposit_timing='before_pickup')
        self.portal.app.config.update(TESTING=True,MOS_PUBLIC_BOOKING=cfg)
        self.portal.app.extensions.pop('mos_public_booking',None)
        # Fresh synthetic vehicles per test keep all immutable paid snapshots
        # available without deleting or reinterpreting previous test bookings.
        db=self.portal.get_db()
        try:
            for slug,name in [('kona','KONA'),('i10','i10')]:
                cfg['fleet'][slug]['id']=db.execute('''INSERT INTO mietfahrzeuge
                    (kennzeichen,bezeichnung,erstellt_am,geaendert_am) VALUES (?,?,?,?)''',
                    ('TEST-'+uuid.uuid4().hex[:10],'Synthetischer '+name,
                     self.portal.now_str(),self.portal.now_str())).lastrowid
            db.commit()
        finally:db.close()
        self.client=self.portal.app.test_client()
        self.assertEqual(self.client.get('/mietwagen-test/').status_code,200)
        self.state=self.portal.app.extensions['mos_public_booking']
        self.service=self.state['service'];self.gateway=self.state['gateway']
        date=datetime.now(BERLIN).date()+timedelta(days=300)
        self.day=date+timedelta(days=(1-date.weekday())%7)

    def post(self,client,path,data):
        with client.session_transaction() as session:token=session['csrf_token']
        return client.post(path,data={**data,'csrf_token':token})

    def preview(self, *, near=False):
        if near:
            now=datetime.now(timezone.utc)
            slots=[s for s in weekly_handover_slots(now) if datetime.fromisoformat(s)>now+timedelta(hours=2)]
            start=slots[0];end=default_end_slot(slots,5)
            fields={'start':start,'end':end}
        else:
            fields={'start_date':self.day.isoformat(),'start_time':'10:00',
                    'end_date':(self.day+timedelta(days=1)).isoformat(),'end_time':'10:00'}
        response=self.post(self.client,'/mietwagen-test/quote',{'vehicle':'i10',**fields})
        self.assertEqual(response.status_code,200,response.get_data(as_text=True))
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',response.get_data(as_text=True))[1])
        return token

    def checkout(self, *, near=False):
        token=self.preview(near=near)
        response=self.post(self.client,'/mietwagen-test/checkout',
            {'quote_token':token,'accept':'yes','sign_confirm':'yes','name':'Synthetischer Kunde',
             'email':'customer@example.invalid','signature_data':self.signature})
        self.assertEqual(response.status_code,303,response.get_data(as_text=True))
        if '/simulate/' in response.location:
            sid=response.location.rsplit('/',1)[-1]
            hold_id=self.gateway.retrieve(sid)['metadata']['hold_id']
        else:
            hold_id=response.location.split('/status/',1)[1].split('/',1)[0]
        return self.service.read(hold_id),response

    def paid_booking(self):
        hold,response=self.checkout()
        self.assertIn('/simulate/',response.location)
        paid=self.post(self.client,response.location,{})
        self.assertEqual(paid.status_code,303,paid.get_data(as_text=True))
        hold=self.service.read(hold['id'])
        self.assertEqual(hold['status'],'confirmed')
        return hold

    @contextmanager
    def due_window(self,hold):
        start=datetime.fromisoformat(json.loads(hold['payload'])['quote']['start_slot'])
        moment=start-timedelta(hours=6)
        class FrozenDateTime(datetime):
            @classmethod
            def now(cls,tz=None):
                return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)
        with ExitStack() as stack:
            stack.enter_context(patch('mietwagen_checkout.datetime',FrozenDateTime))
            stack.enter_context(patch('mos_public_booking.datetime',FrozenDateTime))
            stack.enter_context(patch.object(self.gateway,'clock',return_value=moment.timestamp()))
            yield

    def rental_status(self,hold):
        db=self.portal.get_db()
        try:return db.execute('SELECT status FROM mietvorgaenge WHERE id=?',(hold['mietvorgang_id'],)).fetchone()['status']
        finally:db.close()

    def synthetic_live_access_fixture(self):
        """Adapt only this disposable offline fixture to the token's live shape.

        No provider call or live configuration is used; the replacement quote is
        explicitly signed anew and its local PDF is regenerated by the route.
        """
        hold=self.paid_booking()
        payload=json.loads(hold['payload'])
        q=payload['quote'];q['test_only']=False
        payload['quote']=presign_quote(q,payload['customer'],self.signature)
        db=self.portal.get_db()
        try:
            db.execute('DELETE FROM miet_checkout_contract_delivery WHERE hold_id=?',(hold['id'],))
            db.execute('DELETE FROM miet_checkout_contracts WHERE hold_id=?',(hold['id'],))
            db.execute('UPDATE miet_checkout_holds SET payload=?,session_id=?,payment_intent=? WHERE id=?',
                       (json.dumps(payload,sort_keys=True),'cs_live_synthetic_'+hold['id'],
                        'pi_synthetic_'+hold['id'],hold['id']))
            db.commit()
        finally:db.close()
        return self.service.read(hold['id'])

    def scoped_client(self,hold):
        client=self.portal.app.test_client()
        self.assertEqual(client.get('/mietwagen-test/buchung-oeffnen').status_code,200)
        token=create_access_token(self.portal,hold)
        granted=self.post(client,'/mietwagen-test/buchung-oeffnen',{'token':token})
        self.assertEqual(granted.status_code,303,granted.get_data(as_text=True))
        return client

    def test_quote_and_rent_payment_ten_months_ahead_do_not_authorize_a_card(self):
        with patch.object(self.service,'prepare_deposit',wraps=self.service.prepare_deposit) as prepare:
            hold=self.paid_booking()
            q=json.loads(hold['payload'])['quote']
            self.assertEqual(q['deposit_timing'],'before_pickup')
            self.assertEqual(q['deposit_authorized_cents'],50000)
            self.assertEqual(q['amount_cents'],q['rental_cents'])
            self.assertEqual(q['start_slot'][:10],self.day.isoformat())
            prepare.assert_not_called()
        self.assertIsNone(self.service._deposit_record(hold['id']))
        page=self.client.get('/mietwagen-test/status/'+hold['id'])
        self.assertEqual(page.status_code,200)
        self.assertIn('eine gültige Reservierung ist noch nicht bestätigt',page.get_data(as_text=True))
        self.assertEqual(self.rental_status(hold),'aktiv')

    def test_early_card_page_redirects_without_provider_calls(self):
        hold,response=self.checkout()
        with patch.object(self.gateway,'create_deposit_intent',wraps=self.gateway.create_deposit_intent) as create:
            self.assertEqual(self.client.get('/mietwagen-test/status/'+hold['id']+'/kaution').status_code,303)
            self.assertEqual(self.post(self.client,response.location,{}).status_code,303)
            self.assertEqual(self.client.get('/mietwagen-test/status/'+hold['id']+'/kaution').status_code,303)
            self.assertEqual(self.post(self.client,'/mietwagen-test/status/'+hold['id']+'/kaution-test',{}).status_code,409)
            create.assert_not_called()
        self.assertIsNone(self.service._deposit_record(hold['id']))

    def test_later_card_route_prepares_and_authorizes_after_time_jump(self):
        hold=self.paid_booking()
        with self.due_window(hold):
            page=self.client.get('/mietwagen-test/status/'+hold['id']+'/kaution')
            self.assertEqual(page.status_code,200,page.get_data(as_text=True))
            self.assertIn('Test-Kartenreservierung simulieren',page.get_data(as_text=True))
            self.assertIn('hast du bereits bezahlt',page.get_data(as_text=True))
            self.assertNotIn('Anschließend bezahlst du den Mietpreis',page.get_data(as_text=True))
            authorized=self.post(self.client,'/mietwagen-test/status/'+hold['id']+'/kaution-test',{})
            self.assertEqual(authorized.status_code,303,authorized.get_data(as_text=True))
            self.assertTrue(self.service.deposit_ready(hold['id']))
        self.assertEqual(self.service.read(hold['id'])['status'],'confirmed')
        self.assertEqual(self.rental_status(hold),'aktiv')

    def test_rejected_later_card_releases_only_authorization_and_preserves_paid_rental(self):
        hold=self.paid_booking()
        with self.due_window(hold):
            self.assertEqual(self.client.get('/mietwagen-test/status/'+hold['id']+'/kaution').status_code,200)
            record=self.service._deposit_record(hold['id'])
            self.gateway.authorize_deposit_intent(record['intent_id'],funding='debit')
            page=self.client.get('/mietwagen-test/status/'+hold['id']+'/kaution')
            self.assertEqual(page.status_code,409,page.get_data(as_text=True))
            self.assertIn('bezahlte Buchung bleibt bestehen',page.get_data(as_text=True))
        self.assertEqual(self.service._deposit_record(hold['id'])['status'],'released')
        self.assertEqual(self.service.read(hold['id'])['status'],'confirmed')
        self.assertEqual(self.rental_status(hold),'aktiv')

    def test_existing_immediate_authorization_path_is_preserved(self):
        self.portal.app.config['MOS_PUBLIC_BOOKING'].pop('deposit_timing')
        self.portal.app.extensions.pop('mos_public_booking')
        self.assertEqual(self.client.get('/mietwagen-test/').status_code,200)
        self.state=self.portal.app.extensions['mos_public_booking']
        self.service=self.state['service'];self.gateway=self.state['gateway']
        hold,response=self.checkout(near=True)
        self.assertTrue(response.location.endswith('/kaution'))
        self.assertIsNotNone(self.service._deposit_record(hold['id']))
        self.assertIsNone(hold['session_id'])
        self.assertEqual(self.post(self.client,'/mietwagen-test/status/'+hold['id']+'/kaution-test',{}).status_code,303)
        rent=self.post(self.client,'/mietwagen-test/status/'+hold['id']+'/retry',{})
        self.assertIn('/simulate/',rent.location)
        self.assertEqual(self.post(self.client,rent.location,{}).status_code,303)
        self.assertEqual(self.service.read(hold['id'])['status'],'confirmed')

    def test_resume_requires_explicit_csrf_post_and_valid_signed_token(self):
        hold=self.synthetic_live_access_fixture()
        token=create_access_token(self.portal,hold)
        client=self.portal.app.test_client()
        response=client.get('/mietwagen-test/buchung-oeffnen#'+token)
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.headers['Cache-Control'],'no-store')
        self.assertEqual(response.headers['Referrer-Policy'],'no-referrer')
        with client.session_transaction() as session:self.assertNotIn('mos_deposit_access',session)
        self.assertEqual(client.post('/mietwagen-test/buchung-oeffnen',data={'token':token}).status_code,400)
        self.assertEqual(self.post(client,'/mietwagen-test/buchung-oeffnen',{'token':'invalid'}).status_code,404)
        granted=self.post(client,'/mietwagen-test/buchung-oeffnen',{'token':token})
        self.assertEqual(granted.status_code,303)
        with client.session_transaction() as session:
            self.assertEqual(set(session['mos_deposit_access']),{hold['id']})
            self.assertEqual(session['mos_deposit_access'][hold['id']]['scope'],'deposit')

    def test_scoped_resume_allows_only_own_status_and_deposit_not_cancel_or_pdf(self):
        hold=self.synthetic_live_access_fixture()
        client=self.scoped_client(hold)
        prefix='/mietwagen-test/status/'+hold['id']
        status=client.get(prefix)
        self.assertEqual(status.status_code,200)
        for forbidden in ('Buchung stornieren','/vertrag.pdf','/bestaetigung.txt'):
            self.assertNotIn(forbidden,status.get_data(as_text=True))
        self.assertEqual(client.get(prefix+'/kaution').status_code,303)
        for suffix in ('/vertrag.pdf','/bestaetigung.txt'):
            self.assertEqual(client.get(prefix+suffix).status_code,404)
        for suffix,data in (('/cancel',{}),('/stornieren',{'confirm':'yes'}),('/retry',{})):
            self.assertEqual(self.post(client,prefix+suffix,data).status_code,404)
        self.assertEqual(client.get('/mietwagen-test/status/unrelated-synthetic-hold').status_code,404)
        with self.due_window(hold):
            self.assertEqual(client.get(prefix+'/kaution').status_code,200)

    def test_pausing_new_bookings_keeps_later_deposit_route_available(self):
        hold=self.paid_booking()
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=False
        with self.due_window(hold):
            self.assertEqual(self.client.get('/mietwagen-test/status/'+hold['id']+'/kaution').status_code,200)
            self.assertEqual(self.post(self.client,'/mietwagen-test/status/'+hold['id']+'/kaution-test',{}).status_code,303)
        self.assertEqual(self.service.read(hold['id'])['status'],'confirmed')

    def test_pausing_new_bookings_keeps_scoped_email_access_available(self):
        hold=self.synthetic_live_access_fixture()
        self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=False
        client=self.scoped_client(hold)
        self.assertEqual(client.get('/mietwagen-test/status/'+hold['id']).status_code,200)
        with self.due_window(hold):
            self.assertEqual(client.get('/mietwagen-test/status/'+hold['id']+'/kaution').status_code,200)


if __name__=='__main__':unittest.main()
