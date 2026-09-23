from pathlib import Path
import html
import os
import re
import sys
import tempfile
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
def deny(*a,**k):raise AssertionError('External network forbidden')
patch('socket.socket.connect',deny).start();patch('socket.socket.connect_ex',deny).start();patch('socket.create_connection',deny).start()
from run_mos_public_test import build_test_app
TEMP=tempfile.TemporaryDirectory(prefix='mos-public-tests-')
# Simulate an operator launching the test from a live-configured shell.
with patch.dict(os.environ, {
    'MOS_BOOKING_CONFIG_FILE':str(Path(TEMP.name)/'must-not-read-live-config.json'),
    'MOS_STRIPE_TEST_KEY':'synthetic-test-sentinel',
    'MOS_STRIPE_LIVE_KEY':'synthetic-live-sentinel',
    'MOS_STRIPE_WEBHOOK_SECRET':'synthetic-webhook-sentinel',
}):
    portal=build_test_app(TEMP.name,origin='http://localhost')


class PublicTests(unittest.TestCase):
    def setUp(self):
        portal.app.config.update(TESTING=True)
        portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        self.client=portal.app.test_client()
        db=portal.get_db()
        for table in ('miet_checkout_events','miet_checkout_holds','mietvorgaenge'):db.execute('DELETE FROM '+table)
        db.commit();db.close()
        self.client.get('/mietwagen-test/')
        self.cfg=portal.app.extensions['mos_public_booking']['cfg']

    def post(self,path,data=None,client=None):
        client=client or self.client
        with client.session_transaction() as s:csrf=s.get('csrf_token')
        return client.post(path,data={**(data or {}),'csrf_token':csrf})

    def quote(self,slug='kona'):
        r=self.post('/mietwagen-test/quote',{'vehicle':slug,'start':self.cfg['slots'][0],'end':self.cfg['slots'][3]})
        self.assertEqual(r.status_code,200)
        return html.unescape(re.search(r'name="quote_token" value="([^"]+)"',r.get_data(as_text=True))[1]),r

    def checkout(self,token):
        return self.post('/mietwagen-test/checkout',{'quote_token':token,'accept':'yes','name':'Test','email':'test@example.invalid'})

    def rows(self):
        db=portal.get_db()
        try:return [dict(r) for r in db.execute('SELECT * FROM mietvorgaenge').fetchall()]
        finally:db.close()

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


if __name__=='__main__':unittest.main()
