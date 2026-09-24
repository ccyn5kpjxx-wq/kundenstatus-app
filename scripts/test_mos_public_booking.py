from pathlib import Path
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import html
from io import BytesIO
import json
import os
import re
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo
from PIL import Image, ImageDraw
from pypdf import PdfReader
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
def deny(*a,**k):raise AssertionError('External network forbidden')
patch('socket.socket.connect',deny).start();patch('socket.socket.connect_ex',deny).start();patch('socket.create_connection',deny).start()
from run_mos_public_test import build_test_app
from mos_public_contract import signed_payload
from mos_booking.production import cancellation_fee
TEMP=tempfile.TemporaryDirectory(prefix='mos-public-tests-')
# Simulate an operator launching the test from a live-configured shell.
with patch.dict(os.environ, {
    'MOS_BOOKING_CONFIG_FILE':str(Path(TEMP.name)/'must-not-read-live-config.json'),
    'MOS_STRIPE_TEST_KEY':'synthetic-test-sentinel',
    'MOS_STRIPE_LIVE_KEY':'synthetic-live-sentinel',
    'MOS_STRIPE_WEBHOOK_SECRET':'synthetic-webhook-sentinel',
}):
    portal=build_test_app(TEMP.name,origin='http://localhost')

# Keep three-day rentals inside the offline card hold (seven days), while the
# second pickup slot stays more than 48 hours away at every test run time.
first_slot=datetime.now(timezone.utc)+timedelta(hours=36)
portal.app.config['MOS_PUBLIC_BOOKING']['slots']=[
    (first_slot+timedelta(days=day)).astimezone(ZoneInfo('Europe/Berlin')).isoformat()
    for day in range(5)
]


class PublicTests(unittest.TestCase):
    def setUp(self):
        portal.app.config.update(TESTING=True)
        portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        self.client=portal.app.test_client()
        self.client.get('/mietwagen-test/')
        db=portal.get_db()
        for table in ('miet_checkout_refunds','miet_checkout_cancellations','miet_checkout_contracts',
                      'miet_checkout_events','miet_checkout_deposit_auths','miet_checkout_creation_attempts',
                      'miet_checkout_holds','mietvorgaenge'):db.execute('DELETE FROM '+table)
        db.commit();db.close()
        self.cfg=portal.app.extensions['mos_public_booking']['cfg']

    def post(self,path,data=None,client=None):
        client=client or self.client
        with client.session_transaction() as s:csrf=s.get('csrf_token')
        return client.post(path,data={**(data or {}),'csrf_token':csrf})

    def quote(self,slug='kona',start_index=0,end_index=3):
        r=self.post('/mietwagen-test/quote',{'vehicle':slug,'start':self.cfg['slots'][start_index],
                                             'end':self.cfg['slots'][end_index]})
        self.assertEqual(r.status_code,200)
        return html.unescape(re.search(r'name="quote_token" value="([^"]+)"',r.get_data(as_text=True))[1]),r

    def checkout(self,token):
        return self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes','sign_confirm':'yes',
            'name':'Test','email':'test@example.invalid','signature_data':self.signature()})

    def rows(self):
        db=portal.get_db()
        try:return [dict(r) for r in db.execute('SELECT * FROM mietvorgaenge').fetchall()]
        finally:db.close()

    def paid_hold(self):
        token,_=self.quote();checkout=self.checkout(token)
        sid=checkout.location.rsplit('/',1)[1]
        paid=self.post(checkout.location)
        self.assertEqual(paid.status_code,303)
        return paid.location.rsplit('/',1)[1]

    def signature(self):
        image=Image.new('RGBA',(700,180),(255,255,255,0))
        draw=ImageDraw.Draw(image)
        draw.line([(40,110),(90,40),(125,125),(180,60),(240,110),(320,50),(380,105)],fill=(20,30,40,255),width=5)
        buf=BytesIO();image.save(buf,format='PNG')
        return 'data:image/png;base64,'+b64encode(buf.getvalue()).decode('ascii')

    def card_deposit(self,slug='kona',start_index=0,end_index=3):
        token,quote_page=self.quote(slug,start_index,end_index)
        checkout=self.checkout(token)
        self.assertEqual(checkout.status_code,303)
        self.assertTrue(checkout.location.endswith('/kaution'))
        hold=checkout.location.rsplit('/',2)[-2]
        state=portal.app.extensions['mos_public_booking']
        intent_id=state['service']._deposit_record(hold)['intent_id']
        self.assertIsNone(state['service'].read(hold)['session_id'])
        return hold,intent_id,quote_page

    def paid_card_booking(self,slug='kona',start_index=0,end_index=3):
        hold,intent_id,_=self.card_deposit(slug,start_index,end_index)
        self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test').status_code,303)
        rent=self.post('/mietwagen-test/status/'+hold+'/retry')
        self.assertEqual(rent.status_code,303)
        self.assertEqual(self.post(rent.location).status_code,303)
        state=portal.app.extensions['mos_public_booking']
        h=state['service'].read(hold)
        self.assertEqual(h['status'],'confirmed')
        return hold,intent_id,json.loads(h['payload'])['quote']

    def cancellation_rows(self,hold):
        db=portal.get_db()
        try:
            cancellation=db.execute('SELECT * FROM miet_checkout_cancellations WHERE id=?',(hold,)).fetchone()
            refunds=[dict(row) for row in db.execute(
                'SELECT amount_cents,kind,status FROM miet_checkout_refunds WHERE hold_id=?',(hold,)).fetchall()]
            return dict(cancellation) if cancellation else None,refunds
        finally:db.close()

    def test_card_authorization_before_rent_only_checkout(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,quote_page=self.card_deposit()
            self.assertIn('147,00',quote_page.get_data(as_text=True))
            self.assertIn('nicht abgebucht',quote_page.get_data(as_text=True))
            state=portal.app.extensions['mos_public_booking']
            gateway=state['gateway']
            self.assertEqual(gateway.retrieve_deposit_intent(intent_id)['status'],'requires_payment_method')
            deposit_page=self.client.get('/mietwagen-test/status/'+hold+'/kaution')
            self.assertEqual(deposit_page.status_code,200)
            self.assertIn('Kaution auf der Karte reservieren',deposit_page.get_data(as_text=True))
            self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/retry').location,
                             '/mietwagen-test/status/'+hold+'/kaution')
            self.assertEqual(self.rows(),[])
            reserved=self.post('/mietwagen-test/status/'+hold+'/kaution-test')
            self.assertEqual(reserved.status_code,303)
            intent=gateway.retrieve_deposit_intent(intent_id)
            self.assertEqual(intent['status'],'requires_capture')
            self.assertEqual(intent['amount_capturable'],50000)
            self.assertEqual(intent['amount_received'],0)
            self.assertIn('Kaution auf der Kreditkarte reserviert',self.client.get(reserved.location).get_data(as_text=True))
            rent=self.post('/mietwagen-test/status/'+hold+'/retry')
            self.assertEqual(rent.status_code,303)
            sid=rent.location.rsplit('/',1)[1]
            self.assertEqual(gateway.retrieve(sid)['amount_total'],14700)
            self.assertEqual(self.rows(),[])
            paid=self.post(rent.location)
            self.assertEqual(paid.status_code,303)
            self.assertEqual(len(self.rows()),1)
            receipt=self.client.get('/mietwagen-test/status/'+hold+'/bestaetigung.txt').get_data(as_text=True)
            self.assertIn('nicht abgebucht: 500.00 EUR',receipt)
            self.assertIn('Gezahlter Mietpreis: 147.00 EUR',receipt)
            self.assertEqual(gateway.retrieve_deposit_intent(intent_id)['amount_received'],0)

    def test_card_deposit_cancel_before_rent_releases_hold(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,_=self.card_deposit()
            self.post('/mietwagen-test/status/'+hold+'/kaution-test')
            self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/stornieren',
                                       {'confirm':'yes'}).status_code,409)
            released=self.post('/mietwagen-test/status/'+hold+'/cancel')
            self.assertEqual(released.status_code,303)
            state=portal.app.extensions['mos_public_booking']
            self.assertEqual(state['service'].read(hold)['status'],'released')
            self.assertEqual(state['service']._deposit_record(hold)['status'],'released')
            self.assertEqual(state['gateway'].retrieve_deposit_intent(intent_id)['status'],'canceled')
            self.assertEqual(self.rows(),[])
            self.assertEqual(self.cancellation_rows(hold),(None,[]))
            status_page=self.client.get(released.location).get_data(as_text=True)
            self.assertIn('freigegeben',status_page)
            self.assertNotIn('Kartenreservierung wird geprüft',status_page)
            self.assertIn('war auf der Kreditkarte reserviert und wurde freigegeben',status_page)
            self.quote()

    def test_debit_or_prepaid_card_cannot_start_rent_checkout(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            state=portal.app.extensions['mos_public_booking']
            for funding in ('debit','prepaid'):
                with self.subTest(funding=funding):
                    hold,intent_id,_=self.card_deposit()
                    state['gateway'].authorize_deposit_intent(intent_id,funding=funding)
                    status=self.client.get('/mietwagen-test/status/'+hold)
                    self.assertEqual(status.status_code,200)
                    page=status.get_data(as_text=True)
                    self.assertIn('freigegeben',page)
                    self.assertIn('Bitte neu buchen',page)
                    self.assertEqual(state['service'].read(hold)['status'],'released')
                    self.assertIsNone(state['service'].read(hold)['session_id'])
                    self.assertEqual(state['service']._deposit_record(hold)['status'],'released')
                    intent=state['gateway'].retrieve_deposit_intent(intent_id)
                    self.assertEqual(intent['status'],'canceled')
                    self.assertEqual(intent['amount_received'],0)
                    self.assertEqual(self.rows(),[])
                    retry=self.post('/mietwagen-test/status/'+hold+'/retry')
                    self.assertEqual(retry.status_code,303)
                    self.assertEqual(retry.location,'/mietwagen-test/status/'+hold)

    def test_card_deposit_paid_cancel_refunds_only_rent_and_releases_card(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,_=self.card_deposit(start_index=1,end_index=4)
            self.post('/mietwagen-test/status/'+hold+'/kaution-test')
            rent=self.post('/mietwagen-test/status/'+hold+'/retry')
            self.assertEqual(self.post(rent.location).status_code,303)
            cancelled=self.post('/mietwagen-test/status/'+hold+'/stornieren',{'confirm':'yes'})
            self.assertEqual(cancelled.status_code,303)
            state=portal.app.extensions['mos_public_booking']
            self.assertEqual(state['service']._deposit_record(hold)['status'],'released')
            intent=state['gateway'].retrieve_deposit_intent(intent_id)
            self.assertEqual(intent['status'],'canceled')
            self.assertEqual(intent['amount_received'],0)
            db=portal.get_db()
            try:
                refunds=[dict(row) for row in db.execute(
                    'SELECT amount_cents,status FROM miet_checkout_refunds WHERE hold_id=?',(hold,)).fetchall()]
            finally:db.close()
            self.assertEqual(refunds,[{'amount_cents':14700,'status':'succeeded'}])
            status_page=self.client.get(cancelled.location).get_data(as_text=True)
            self.assertIn('Buchung storniert',status_page)
            self.assertNotIn('Kartenreservierung wird geprüft',status_page)
            self.assertIn('war auf der Kreditkarte reserviert und wurde freigegeben',status_page)

    def test_new_card_policy_exactly_48_hours_refunds_all_rent_and_releases_deposit(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,q=self.paid_card_booking(start_index=1,end_index=4)
            self.assertEqual(q['cancellation_policy'],'free_48h_then_10pct_rent')
            start=datetime.fromisoformat(q['start_slot'])
            state=portal.app.extensions['mos_public_booking']
            rid=state['ledger'].cancel(hold,requested_at=start-timedelta(hours=48))
            self.assertEqual(rid,'cancel-'+hold)
            cancelled=self.post('/mietwagen-test/status/'+hold+'/stornieren',{'confirm':'yes'})
            self.assertEqual(cancelled.status_code,303)
            self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/stornieren',{'confirm':'yes'}).status_code,303)
            row,refunds=self.cancellation_rows(hold)
            self.assertEqual(row['fee_cents'],0)
            self.assertEqual(refunds,[{'amount_cents':14700,'kind':'cancellation','status':'succeeded'}])
            self.assertEqual(state['service']._deposit_record(hold)['status'],'released')
            intent=state['gateway'].retrieve_deposit_intent(intent_id)
            self.assertEqual(intent['status'],'canceled')
            self.assertEqual(intent['amount_received'],0)

    def test_new_card_policy_inside_48_hours_keeps_only_ten_percent_of_rent(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,q=self.paid_card_booking(start_index=1,end_index=4)
            self.assertEqual(q['cancellation_policy'],'free_48h_then_10pct_rent')
            start=datetime.fromisoformat(q['start_slot'])
            self.assertEqual(cancellation_fee(q,start-timedelta(hours=1)),1470)
            state=portal.app.extensions['mos_public_booking']
            state['ledger'].cancel(hold,requested_at=start-timedelta(hours=48)+timedelta(seconds=1))
            self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/stornieren',
                                       {'confirm':'yes'}).status_code,303)
            row,refunds=self.cancellation_rows(hold)
            self.assertEqual(row['fee_cents'],1470)
            self.assertEqual(refunds,[{'amount_cents':13230,'kind':'cancellation','status':'succeeded'}])
            self.assertEqual(state['service']._deposit_record(hold)['status'],'released')
            intent=state['gateway'].retrieve_deposit_intent(intent_id)
            self.assertEqual(intent['status'],'canceled')
            self.assertEqual(intent['amount_received'],0)
            self.assertIn('Stornogebühr nach bisheriger Minderung: 14,70 €',
                          self.client.get('/mietwagen-test/status/'+hold).get_data(as_text=True))

    def test_legacy_quote_without_policy_keeps_original_cancellation_rule(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            _,_,new_quote=self.paid_card_booking(start_index=1,end_index=4)
            start=datetime.fromisoformat(new_quote['start_slot'])
            old_quote=dict(new_quote)
            old_quote.pop('cancellation_policy')
            self.assertEqual(cancellation_fee(new_quote,start-timedelta(hours=36)),1470)
            self.assertEqual(cancellation_fee(old_quote,start-timedelta(hours=36)),0)
            self.assertEqual(cancellation_fee(old_quote,start-timedelta(hours=23)),4900)

    def test_card_deposit_admin_release_requires_recorded_return(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,_=self.card_deposit()
            self.post('/mietwagen-test/status/'+hold+'/kaution-test')
            rent=self.post('/mietwagen-test/status/'+hold+'/retry')
            self.assertEqual(self.post(rent.location).status_code,303)
            state=portal.app.extensions['mos_public_booking']
            active_check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            self.assertEqual(active_check.exit_code,0,active_check.output)
            self.assertIn('1 aktive Kartenreservierungen',active_check.output)
            self.assertIn('offene Fehler: 0',active_check.output)
            self.assertEqual(state['gateway'].retrieve_deposit_intent(intent_id)['status'],'requires_capture')
            admin=portal.app.test_client()
            with admin.session_transaction() as session:session['admin']=True
            admin.get('/mietwagen-test/admin')
            action='/mietwagen-test/admin/'+hold
            data={'action':'deposit','reason':'Rückgabeprotokoll geprüft'}
            premature=self.post(action,data,client=admin)
            self.assertEqual(premature.status_code,409)
            self.assertIn('erst nach protokollierter',premature.get_data(as_text=True))
            self.assertEqual(state['gateway'].retrieve_deposit_intent(intent_id)['status'],'requires_capture')
            rental_id=state['service'].read(hold)['mietvorgang_id']
            db=portal.get_db()
            try:
                db.execute("UPDATE mietvorgaenge SET status='zurueck' WHERE id=?",(rental_id,))
                db.commit()
            finally:db.close()
            released=self.post(action,data,client=admin)
            self.assertEqual(released.status_code,303)
            self.assertEqual(state['gateway'].retrieve_deposit_intent(intent_id)['status'],'canceled')
            self.assertEqual(state['service']._deposit_record(hold)['status'],'released')
            settled_check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            self.assertEqual(settled_check.exit_code,0,settled_check.output)
            self.assertIn('0 aktive Kartenreservierungen',settled_check.output)
            self.assertIn('offene Fehler: 0',settled_check.output)
            status_page=self.client.get('/mietwagen-test/status/'+hold).get_data(as_text=True)
            self.assertIn('war auf der Kreditkarte reserviert und wurde freigegeben',status_page)
            self.assertNotIn('Kartenreservierung wird geprüft',status_page)

    def test_incoming_vehicle_has_no_public_quote(self):
        vid=self.cfg['fleet']['kona']['id']
        db=portal.get_db()
        db.execute("UPDATE mietfahrzeuge SET status='bald' WHERE id=?",(vid,));db.commit();db.close()
        try:
            r=self.post('/mietwagen-test/quote',{'vehicle':'kona','start':self.cfg['slots'][0],'end':self.cfg['slots'][3]})
            self.assertEqual(r.status_code,409)
        finally:
            db=portal.get_db()
            db.execute("UPDATE mietfahrzeuge SET status='verfuegbar' WHERE id=?",(vid,));db.commit();db.close()

    def test_inherited_live_configuration_is_ignored(self):
        rules={r.rule for r in portal.app.url_map.iter_rules()}
        self.assertIn('/mietwagen-test/',rules)
        self.assertNotIn('/mieten/',rules)
        for key in ('MOS_PUBLIC_STRIPE_TEST_KEY','MOS_PUBLIC_STRIPE_LIVE_KEY','MOS_PUBLIC_WEBHOOK_SECRET'):
            self.assertFalse(portal.app.config.get(key))
        self.assertEqual(self.cfg['mode'],'offline')

    def test_end_to_end_signed_confirmation(self):
        token,r=self.quote();self.assertIn('147,00',r.get_data(as_text=True))
        r=self.checkout(token);self.assertEqual(r.status_code,303)
        page=self.client.get(r.location);self.assertIn('Offline-Testzahlung',page.get_data(as_text=True))
        result=self.post(r.location);self.assertEqual(result.status_code,303)
        status=self.client.get(result.location);self.assertIn('Testzahlung verifiziert',status.get_data(as_text=True))
        self.assertEqual(len(self.rows()),1)
        self.post(r.location);self.assertEqual(len(self.rows()),1)

    def test_stripe_sandbox_quote_includes_separate_deposit(self):
        self.cfg['mode']='stripe_test'
        try:
            token,page=self.quote()
            self.assertIn('647,00',page.get_data(as_text=True))
            self.assertIn('500',page.get_data(as_text=True))
            gateway=portal.app.extensions['mos_public_booking']['gateway']
            with patch.object(gateway,'create',wraps=gateway.create) as create:
                response=self.checkout(token)
                self.assertEqual(response.status_code,303)
                params=create.call_args.args[0]
                self.assertEqual([i['price_data']['unit_amount'] for i in params['line_items']],[14700,50000])
                self.assertIn('TEST',params['custom_text']['submit']['message'])
                self.assertFalse(gateway.livemode)
        finally:self.cfg['mode']='offline'

    def test_two_listings_server_prices(self):
        db=portal.get_db()
        try:
            names={r['bezeichnung'] for r in db.execute('SELECT bezeichnung FROM mietfahrzeuge').fetchall()}
        finally:db.close()
        self.assertEqual(names,{'TESTDATENSATZ Hyundai KONA N Line X','TESTDATENSATZ Hyundai i10'})
        self.assertEqual(set(portal.app.config['MOS_PUBLIC_BOOKING']['fleet']),{'kona','i10'})
        self.assertEqual(portal.app.config['MOS_PUBLIC_BOOKING']['mode'],'offline')
        for slug,total in [('kona','147,00'),('i10','117,00')]:
            _,r=self.quote(slug);self.assertIn(total,r.get_data(as_text=True))
        self.assertEqual(self.post('/mietwagen-test/quote',{'vehicle':'c3','start':self.cfg['slots'][0],'end':self.cfg['slots'][3]}).status_code,409)

    def test_off_leaves_inquiry_and_no_public_entry(self):
        portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=False
        self.assertEqual(self.client.get('/mietwagen-test/').status_code,404)
        r=self.client.get('/mietwagen-vorschau/')
        self.assertIn('Unverbindliche Anfrage',r.get_data(as_text=True))
        self.assertNotIn('Testbuchung für KONA',r.get_data(as_text=True))
        r.close()

    def test_csrf_owner_tampering_and_missing_acceptance(self):
        token,_=self.quote()
        self.assertEqual(self.client.post('/mietwagen-test/checkout').status_code,400)
        self.assertEqual(self.checkout(token+'tampered').status_code,409)
        self.assertEqual(self.post('/mietwagen-test/checkout',{'quote_token':token}).status_code,409)
        other=portal.app.test_client();other.get('/mietwagen-test/')
        r=self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes'},other)
        self.assertEqual(r.status_code,404)

    def test_fake_slot_and_fiat_rejected(self):
        for data in [{'vehicle':'fiat','start':self.cfg['slots'][0],'end':self.cfg['slots'][3]},
                     {'vehicle':'kona','start':'2030-01-01T09:00:00+01:00','end':self.cfg['slots'][3]}]:
            self.assertEqual(self.post('/mietwagen-test/quote',data).status_code,409)

    def test_cancel_and_retry_single_hold(self):
        token,_=self.quote();r=self.checkout(token);again=self.checkout(token)
        self.assertEqual(r.location,again.location)
        h=portal.app.extensions['mos_public_booking']['gateway'].retrieve(r.location.rsplit('/',1)[1])['metadata']['hold_id']
        self.assertIn('noch nicht bestätigt',self.client.get('/mietwagen-test/status/'+h+'?paid=true').get_data(as_text=True))
        self.assertEqual(self.rows(),[])
        other=portal.app.test_client();self.assertEqual(other.get('/mietwagen-test/status/'+h).status_code,404)
        self.post('/mietwagen-test/status/'+h+'/cancel')
        self.assertIn('freigegeben',self.client.get('/mietwagen-test/status/'+h).get_data(as_text=True))
        self.quote()

    def test_external_webhook_signature_and_replay(self):
        token,_=self.quote();r=self.checkout(token);state=portal.app.extensions['mos_public_booking'];sid=r.location.rsplit('/',1)[1]
        state['gateway'].pay(sid);body,sig=state['gateway'].signed_event(sid)
        self.assertEqual(self.client.post('/mietwagen-test/webhook',data=body,headers={'Stripe-Signature':'bad'}).status_code,400)
        for _ in range(2):self.assertEqual(self.client.post('/mietwagen-test/webhook',data=body,headers={'Stripe-Signature':sig}).status_code,204)
        self.assertEqual(len(self.rows()),1)

    def test_paid_return_without_webhook_does_not_confirm(self):
        token,_=self.quote();r=self.checkout(token);state=portal.app.extensions['mos_public_booking'];sid=r.location.rsplit('/',1)[1]
        s=state['gateway'].pay(sid);h=s['metadata']['hold_id']
        self.client.get('/mietwagen-test/status/'+h)
        self.assertEqual(self.rows(),[])

    def test_reconcile_alarms_on_paid_checkout_without_signed_webhook(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,_=self.card_deposit()
            self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test').status_code,303)
            rent=self.post('/mietwagen-test/status/'+hold+'/retry')
            self.assertEqual(rent.status_code,303)
            state=portal.app.extensions['mos_public_booking']
            sid=state['service'].read(hold)['session_id']
            self.assertEqual(state['gateway'].pay(sid)['payment_status'],'paid')
            self.assertEqual(state['gateway'].retrieve_deposit_intent(intent_id)['amount_received'],0)
            check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            self.assertNotEqual(check.exit_code,0,check.output)
            self.assertIn('bezahlte Checkouts ohne Webhook: 1',check.output)
            self.assertEqual(state['service'].read(hold)['status'],'pending')
            self.assertEqual(self.rows(),[])

    def test_reconcile_alarms_if_card_hold_disappears_during_open_rent_checkout(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,_=self.card_deposit()
            self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test').status_code,303)
            rent=self.post('/mietwagen-test/status/'+hold+'/retry')
            self.assertEqual(rent.status_code,303)
            state=portal.app.extensions['mos_public_booking']
            session_id=state['service'].read(hold)['session_id']
            self.assertEqual(state['gateway'].retrieve(session_id)['status'],'open')
            state['gateway'].cancel_deposit_intent(intent_id,'synthetic-external-release-'+hold)

            check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            self.assertNotEqual(check.exit_code,0,check.output)
            self.assertIn('offene Fehler: 1',check.output)
            self.assertEqual(state['service'].read(hold)['status'],'pending')
            self.assertEqual(state['gateway'].retrieve(session_id)['status'],'open')
            self.assertEqual(self.rows(),[])

    def test_reconcile_alarms_if_card_hold_is_recorded_released_but_rent_checkout_open(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,_,_=self.card_deposit()
            self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test').status_code,303)
            rent=self.post('/mietwagen-test/status/'+hold+'/retry')
            self.assertEqual(rent.status_code,303)
            state=portal.app.extensions['mos_public_booking']
            session_id=state['service'].read(hold)['session_id']
            # Simulate a release committed before an intervening cleanup step.
            state['service'].release_deposit(hold,'synthetic interrupted cleanup')
            self.assertEqual(state['service']._deposit_record(hold)['status'],'released')
            self.assertEqual(state['gateway'].retrieve(session_id)['status'],'open')

            check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            self.assertNotEqual(check.exit_code,0,check.output)
            self.assertIn('offene Fehler: 1',check.output)
            self.assertEqual(state['service'].read(hold)['status'],'pending')
            self.assertEqual(self.rows(),[])

    def test_reconcile_alarms_on_review_hold(self):
        token,_=self.quote()
        checkout=self.checkout(token)
        state=portal.app.extensions['mos_public_booking']
        sid=checkout.location.rsplit('/',1)[1]
        hold=state['gateway'].retrieve(sid)['metadata']['hold_id']
        db=portal.get_db()
        try:
            db.execute("UPDATE miet_checkout_holds SET status='review',grund='test_provider_review' WHERE id=?",(hold,))
            db.commit()
        finally:db.close()
        check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
        self.assertNotEqual(check.exit_code,0,check.output)
        self.assertIn('Prüffälle: 1',check.output)
        self.assertEqual(state['service'].read(hold)['status'],'review')
        self.assertEqual(self.rows(),[])

    def test_reconcile_alarms_on_failed_refund_without_resubmitting(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,_,quote=self.paid_card_booking()
            state=portal.app.extensions['mos_public_booking']
            rid=state['ledger'].cancel(hold,requested_at=datetime.fromisoformat(quote['start_slot'])-timedelta(hours=1))
            payment_intent=state['service'].read(hold)['payment_intent']
            def failed_refund(pi,amount,key):
                self.assertEqual(pi,payment_intent)
                self.assertEqual(key,'mos-refund-'+rid)
                return {'id':'re_offline_failed_synthetic','object':'refund','payment_intent':pi,
                        'amount':amount,'currency':'eur','status':'failed'}
            with patch.object(state['gateway'],'refund',side_effect=failed_refund):
                self.assertEqual(state['ledger'].process(rid),'failed')
            state['service'].release_deposit(hold,'Synthetische Stornierung')
            db=portal.get_db()
            try:
                before=dict(db.execute('SELECT * FROM miet_checkout_refunds WHERE id=?',(rid,)).fetchone())
            finally:db.close()
            self.assertEqual(before['status'],'failed')
            self.assertEqual(before['provider_id'],'re_offline_failed_synthetic')
            with patch.object(state['gateway'],'refund') as send, patch.object(state['gateway'],'retrieve_refund') as retrieve:
                check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
                send.assert_not_called()
                retrieve.assert_not_called()
            self.assertNotEqual(check.exit_code,0,check.output)
            self.assertIn('fehlgeschlagene Erstattungen: 1',check.output)
            self.assertIn('offene Fehler: 0',check.output)
            db=portal.get_db()
            try:
                after=dict(db.execute('SELECT * FROM miet_checkout_refunds WHERE id=?',(rid,)).fetchone())
                count=db.execute('SELECT COUNT(*) AS n FROM miet_checkout_refunds WHERE hold_id=?',(hold,)).fetchone()['n']
            finally:db.close()
            self.assertEqual(after,before)
            self.assertEqual(count,1)

    def test_reconcile_alarms_on_stale_checkout_creation_without_session(self):
        token,_=self.quote()
        state=portal.app.extensions['mos_public_booking']
        with patch.object(state['gateway'],'create',side_effect=ConnectionError('synthetic timeout')):
            self.assertEqual(self.checkout(token).status_code,503)
        db=portal.get_db()
        try:
            hold=db.execute('SELECT id FROM miet_checkout_holds').fetchone()['id']
            db.execute('UPDATE miet_checkout_creation_attempts SET created_at=? WHERE hold_id=?',
                       (int(time.time())-601,hold))
            db.commit()
        finally:db.close()
        self.assertIsNone(state['service'].read(hold)['session_id'])
        with patch.object(state['gateway'],'create') as create:
            check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            create.assert_not_called()
        self.assertNotEqual(check.exit_code,0,check.output)
        self.assertIn('unklare Checkout-Aufträge: 1',check.output)
        self.assertEqual(state['service'].read(hold)['status'],'pending')
        self.assertEqual(self.rows(),[])

    def test_reconcile_alarms_on_releasing_deposit_without_rent_session(self):
        with patch.dict(self.cfg,{'deposit_method':'card_authorization_at_booking'}):
            hold,intent_id,_=self.card_deposit()
            self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/kaution-test').status_code,303)
            state=portal.app.extensions['mos_public_booking']
            db=portal.get_db()
            try:
                db.execute("UPDATE miet_checkout_deposit_auths SET status='releasing' WHERE hold_id=?",(hold,))
                db.commit()
            finally:db.close()
            self.assertIsNone(state['service'].read(hold)['session_id'])
            with patch.object(state['gateway'],'cancel_deposit_intent') as release, patch.object(state['gateway'],'create') as create:
                check=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
                release.assert_not_called()
                create.assert_not_called()
            self.assertNotEqual(check.exit_code,0,check.output)
            self.assertIn('ungeklärte Kartenreservierungen: 1',check.output)
            self.assertEqual(state['service']._deposit_record(hold)['status'],'releasing')
            self.assertEqual(state['gateway'].retrieve_deposit_intent(intent_id)['status'],'requires_capture')
            self.assertEqual(state['service'].read(hold)['status'],'pending')
            self.assertEqual(self.rows(),[])

    def test_paid_contract_signature_and_immutable_pdf(self):
        token,quote_page=self.quote()
        self.assertIn('Testvertrag vor der Testzahlung',quote_page.get_data(as_text=True))
        self.assertIn('Gärtner GmbH Karosserie + Lack',quote_page.get_data(as_text=True))
        checkout=self.checkout(token)
        self.assertEqual(checkout.status_code,303)
        sid=checkout.location.rsplit('/',1)[1]
        hold=portal.app.extensions['mos_public_booking']['gateway'].retrieve(sid)['metadata']['hold_id']
        status_url='/mietwagen-test/status/'+hold
        page=self.client.get(status_url).get_data(as_text=True)
        self.assertIn('Dein unterschriebener Mietvertrag',page)
        self.assertIn('Zahlung noch offen',page)
        self.assertIn('Gärtner GmbH Karosserie + Lack',page)
        self.assertIn('keine eigene Vertragspartei',page)
        self.assertIn(self.cfg['terms_text'],page)
        self.assertEqual(self.rows(),[])
        self.assertEqual(self.client.get(status_url+'/vertrag.pdf').status_code,404)
        h=portal.app.extensions['mos_public_booking']['service'].read(hold)
        unsigned_snapshot=json.loads(h['payload'])['quote']
        self.assertTrue(unsigned_snapshot['signed_at'])
        self.assertEqual(len(unsigned_snapshot['signed_contract_hash']),64)
        paid=self.post(checkout.location)
        self.assertEqual(paid.status_code,303)
        db=portal.get_db()
        try:stored=dict(db.execute('SELECT * FROM miet_checkout_contracts WHERE hold_id=?',(hold,)).fetchone())
        finally:db.close()
        document=json.loads(stored['contract_json'])
        self.assertEqual(document['lessor_name'],'Gärtner GmbH Karosserie + Lack')
        self.assertEqual(document['terms_text'],self.cfg['terms_text'])
        self.assertEqual(document['deposit_cents'],50000)
        self.assertEqual(document['deductible_cents'],100000)
        self.assertEqual(stored['signed_at'],unsigned_snapshot['signed_at'])
        self.assertEqual(stored['contract_sha256'],unsigned_snapshot['signed_contract_hash'])
        self.assertNotIn('payment_intent',document)
        pdf=self.client.get(status_url+'/vertrag.pdf')
        self.assertEqual(pdf.status_code,200)
        self.assertTrue(pdf.data.startswith(b'%PDF-'))
        self.assertIn('attachment;',pdf.headers['Content-Disposition'])
        self.assertEqual(sha256(pdf.data).hexdigest(),stored['pdf_sha256'])
        text='\n'.join(p.extract_text() or '' for p in PdfReader(BytesIO(pdf.data)).pages)
        self.assertIn('Gärtner GmbH Karosserie + Lack',text)
        self.assertIn('TESTENTWURF',text)
        self.assertNotEqual(portal.app.test_client().get('/mietwagen-test/admin/'+hold+'/vertrag.pdf').status_code,200)
        admin=portal.app.test_client()
        with admin.session_transaction() as s:s['admin']=True
        self.assertEqual(admin.get('/mietwagen-test/admin/'+hold+'/vertrag.pdf').data,pdf.data)
        self.assertIn('digital unterschrieben',admin.get('/mietwagen-test/admin').get_data(as_text=True))
        self.cfg['terms_text']='Neue Regeln dürfen alte Buchungen nicht ändern.'
        try:
            self.assertEqual(self.client.get(status_url+'/vertrag.pdf').data,pdf.data)
            self.assertIn('TESTENTWURF',self.client.get(status_url).get_data(as_text=True))
            self.assertEqual(self.checkout(token).location,status_url)
            self.assertEqual(self.client.get(status_url+'/vertrag.pdf').data,pdf.data)
        finally:self.cfg['terms_text']=document['terms_text']

    def test_signature_required_before_checkout_and_owner(self):
        token,_=self.quote()
        base={'quote_token':token,'accept':'yes','sign_confirm':'yes','name':'Test','email':'test@example.invalid'}
        no_confirmation=self.post('/mietwagen-test/checkout',{**base,'sign_confirm':'','signature_data':self.signature()})
        self.assertIn('ausdrücklich unterschreiben',no_confirmation.get_data(as_text=True))
        self.assertIn('Unterschrift',self.post('/mietwagen-test/checkout',base).get_data(as_text=True))
        blank=Image.new('RGBA',(700,180),(255,255,255,0));buf=BytesIO();blank.save(buf,format='PNG')
        empty='data:image/png;base64,'+b64encode(buf.getvalue()).decode('ascii')
        invalid=self.post('/mietwagen-test/checkout',{**base,'signature_data':empty})
        self.assertIn('lesbare Unterschrift',invalid.get_data(as_text=True))
        db=portal.get_db()
        try:self.assertEqual(db.execute('SELECT COUNT(*) AS n FROM miet_checkout_holds').fetchone()['n'],0)
        finally:db.close()
        checkout=self.checkout(token)
        sid=checkout.location.rsplit('/',1)[1]
        hold=portal.app.extensions['mos_public_booking']['gateway'].retrieve(sid)['metadata']['hold_id']
        status_url='/mietwagen-test/status/'+hold
        self.assertIn('Dein unterschriebener Mietvertrag',self.client.get(status_url).get_data(as_text=True))
        other=portal.app.test_client();other.get('/mietwagen-test/')
        self.assertEqual(other.get(status_url).status_code,404)
        self.assertEqual(other.get(status_url+'/vertrag.pdf').status_code,404)
        self.assertEqual(self.post(status_url+'/retry',client=other).status_code,404)
        self.assertEqual(self.post(status_url+'/unterschrift',{'signature_data':self.signature()}).status_code,404)

    def test_signature_record_binds_time_to_signed_terms(self):
        token,_=self.quote();checkout=self.checkout(token)
        sid=checkout.location.rsplit('/',1)[1]
        hold=portal.app.extensions['mos_public_booking']['gateway'].retrieve(sid)['metadata']['hold_id']
        payload=json.loads(portal.app.extensions['mos_public_booking']['service'].read(hold)['payload'])
        signed_payload(payload)
        payload['quote']['signed_at']='2031-01-01T00:00:00+00:00'
        with self.assertRaisesRegex(ValueError,'Unterschriftsnachweis'):
            signed_payload(payload)

    def test_checkout_failure_keeps_signed_retry_available(self):
        token,_=self.quote()
        gateway=portal.app.extensions['mos_public_booking']['gateway']
        with patch.object(gateway,'create',side_effect=ConnectionError('provider unavailable')):
            failed=self.checkout(token)
        self.assertEqual(failed.status_code,503)
        self.assertIn('Zahlung sicher erneut öffnen',failed.get_data(as_text=True))
        db=portal.get_db()
        try:hold=db.execute('SELECT id FROM miet_checkout_holds').fetchone()['id']
        finally:db.close()
        self.assertEqual(self.post('/mietwagen-test/status/'+hold+'/retry').status_code,303)

    def test_cancelled_paid_booking_keeps_signed_copy(self):
        hold=self.paid_hold();url='/mietwagen-test/status/'+hold
        pdf=self.client.get(url+'/vertrag.pdf').data
        cancelled=self.post(url+'/stornieren',{'confirm':'yes'})
        self.assertEqual(cancelled.status_code,303)
        self.assertIn('storniert',self.client.get(url).get_data(as_text=True))
        self.assertEqual(self.client.get(url+'/vertrag.pdf').data,pdf)


if __name__=='__main__':unittest.main()
