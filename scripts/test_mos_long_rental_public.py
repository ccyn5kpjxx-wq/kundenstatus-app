"""Public hybrid quotes and payment routes; synthetic inventory and no network."""
from contextlib import ExitStack
from datetime import timedelta
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
import html,json,re,sys,unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
import test_mos_deferred_public as deferred_fixture
from itsdangerous import URLSafeTimedSerializer
from mos_public_contract import snapshot,signed_payload
from mos_booking.production import launch_errors


class LongRentalPublicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        def deny(*a,**k):raise AssertionError('No external network in synthetic public tests')
        cls.net=ExitStack()
        for name in ('socket.socket.connect','socket.socket.connect_ex','socket.create_connection'):
            cls.net.enter_context(patch(name,deny))
        deferred_fixture.DeferredPublicTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        deferred_fixture.DeferredPublicTests.tearDownClass();cls.net.close()

    def setUp(self):
        self.f=deferred_fixture.DeferredPublicTests();self.f.setUp()
        self.portal=self.f.portal;self.client=self.f.client
        cfg=self.portal.app.config['MOS_PUBLIC_BOOKING']
        cfg.update(max_days=30,long_rental_deposit_method='charge_with_rent_refund_after_return')
        self.portal.app.extensions.pop('mos_public_booking',None)
        self.assertEqual(self.client.get('/mietwagen-test/').status_code,200)
        self.state=self.portal.app.extensions['mos_public_booking']
        self.service=self.state['service'];self.gateway=self.state['gateway']

    def preview(self,days,*,start=None):
        start=start or self.f.day  # Tuesday, leaving six-day returns on Monday.
        r=self.f.post(self.client,'/mietwagen-test/quote',{
            'vehicle':'kona','start_date':start.isoformat(),'start_time':'14:00',
            'end_date':(start+timedelta(days=days)).isoformat(),'end_time':'14:00'})
        if r.status_code!=200:return r,None,None
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',r.get_data(as_text=True))[1])
        quote=URLSafeTimedSerializer(self.portal.app.secret_key,salt='mos-public-quote-v1').loads(token)['quote']
        return r,quote,token

    def paid(self):
        r,q,token=self.preview(10)
        self.assertEqual(r.status_code,200)
        with patch.object(self.service,'prepare_deposit',side_effect=AssertionError('No extra card hold')):
            r=self.f.post(self.client,'/mietwagen-test/checkout',{
                'quote_token':token,'accept':'yes','sign_confirm':'yes',
                'name':'Synthetischer Langmietkunde','email':'long@example.invalid',
                'signature_data':self.f.signature})
        self.assertEqual(r.status_code,303,r.get_data(as_text=True))
        self.assertIn('/simulate/',r.location)
        sid=r.location.rsplit('/',1)[-1]
        hold_id=self.gateway.retrieve(sid)['metadata']['hold_id']
        response=self.f.post(self.client,r.location,{})
        self.assertEqual(response.status_code,303,response.get_data(as_text=True))
        return self.service.read(hold_id),q

    def test_five_days_keep_deferred_credit_card_hold(self):
        r,q,_=self.preview(5,start=self.f.day-timedelta(days=1))
        self.assertEqual(r.status_code,200)
        self.assertEqual((q['deposit_authorized_cents'],q['deposit_charged_cents']),(50000,0))
        self.assertEqual(q['amount_cents'],q['rental_cents'])
        self.assertEqual(q['deposit_timing'],'before_pickup')
        self.assertNotIn('long_rental_deposit_policy',q)

    def test_six_ten_and_thirty_days_split_rent_and_paid_deposit(self):
        for days in (6,10,30):
            with self.subTest(days=days):
                r,q,_=self.preview(days)
                self.assertEqual(r.status_code,200,r.get_data(as_text=True))
                self.assertEqual(q['days'],days)
                self.assertEqual(q['long_rental_deposit_policy'],'paid_at_booking_v1')
                self.assertEqual((q['deposit_authorized_cents'],q['deposit_charged_cents']),(0,50000))
                self.assertEqual(q['amount_cents'],q['rental_cents']+50000)
                self.assertNotIn('deposit_timing',q)
                self.assertNotIn('deposit_authorization_mode',q)
                page=r.get_data(as_text=True)
                self.assertIn('Gesamtzahlung',page)
                self.assertIn('rückzahlbare Kaution',page)
        self.assertEqual(self.preview(31)[0].status_code,409)

    def test_paid_long_booking_needs_no_authorization_and_preserves_split_in_receipt(self):
        h,q=self.paid()
        self.assertEqual(h['status'],'confirmed')
        self.assertEqual((q['rental_cents'],q['amount_cents']),(49000,99000))
        db=self.portal.get_db()
        try:self.assertIsNone(db.execute('SELECT hold_id FROM miet_checkout_deposit_auths WHERE hold_id=?',(h['id'],)).fetchone())
        finally:db.close()
        payload=json.loads(h['payload'])
        contract,_,_=signed_payload(payload)
        self.assertEqual(contract['long_rental_deposit_policy'],'paid_at_booking_v1')
        receipt=self.client.get('/mietwagen-test/status/'+h['id']+'/bestaetigung.txt')
        self.assertEqual(receipt.status_code,200)
        self.assertIn('Miete: 490.00 EUR',receipt.get_data(as_text=True))
        self.assertIn('Kaution eingezogen: 500.00 EUR',receipt.get_data(as_text=True))
        self.assertIn('Gesamtzahlung: 990.00 EUR',receipt.get_data(as_text=True))

    def test_marker_and_amounts_are_bound_to_signed_contract(self):
        _,q,_=self.preview(10)
        payload={'quote':{**q,'accepted_terms':True},'customer':{'name':'Synthetic','email':'none@example.invalid'}}
        original=snapshot(payload)
        self.assertEqual(original['long_rental_deposit_policy'],'paid_at_booking_v1')
        for key,value in [('days',5),('amount_cents',49000),('deposit_authorized_cents',50000),
                          ('long_rental_deposit_policy','unknown'),('deposit_timing','before_pickup')]:
            with self.subTest(key=key),self.assertRaises(ValueError):
                snapshot({**payload,'quote':{**payload['quote'],key:value}})


if __name__=='__main__':unittest.main()
