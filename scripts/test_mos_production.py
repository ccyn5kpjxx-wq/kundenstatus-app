"""Production gates and financial logic using isolated SQLite + offline provider only."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta,timezone
from pathlib import Path
from unittest.mock import patch
import json,sys,tempfile,unittest,uuid
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
def deny(*a,**k):raise AssertionError('External network forbidden')
patch('socket.socket.connect',deny).start();patch('socket.socket.connect_ex',deny).start();patch('socket.create_connection',deny).start()
from run_mos_public_test import build_test_app
TEMP=tempfile.TemporaryDirectory(prefix='mos-production-tests-')
portal=build_test_app(TEMP.name,origin='http://localhost')
portal.app.test_client().get('/mietwagen-test/')
from mos_booking.production import launch_errors,StripeLiveGateway,cancellation_fee
from mos_booking.gateway import StripeTestGateway
from mietwagen_checkout import SharedCheckout


class ProductionTests(unittest.TestCase):
    def setUp(self):
        portal.app.config.update(TESTING=True)
        portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        portal.app.config['MOS_SHARED_CHECKOUT_ENABLED']=True
        self.state=portal.app.extensions['mos_public_booking'];self.s=self.state['service'];self.g=self.state['gateway'];self.l=self.state['ledger']
        self.start=datetime.now(timezone.utc)+timedelta(days=10)
        db=portal.get_db()
        for table in ['miet_checkout_refunds','miet_checkout_cancellations','miet_checkout_events','miet_checkout_holds','mietvorgaenge']:db.execute('DELETE FROM '+table)
        db.commit();db.close()

    def paid(self):
        q={'amount_cents':64700,'rental_cents':14700,'deposit_charged_cents':50000,'currency':'eur',
           'rules_version':'draft:test','daily_cents':4900,'start_slot':self.start.isoformat(),
           'end_slot':(self.start+timedelta(days=3)).isoformat()}
        h=self.s.reserve(uuid.uuid4().hex,self.state['cfg']['fleet']['kona']['id'],self.start.date().isoformat(),
                         (self.start+timedelta(days=3)).date().isoformat(),{'name':'Test','email':'test@example.invalid'},q)
        session=self.s.create_checkout(h['id']);self.g.pay(session['id']);body,sig=self.g.signed_event(session['id'])
        self.s.handle_signed_event(body,sig,self.state['secret']);return self.s.read(h['id']),q

    def refund(self,rid):
        db=portal.get_db()
        try:return dict(db.execute('SELECT * FROM miet_checkout_refunds WHERE id=?',(rid,)).fetchone())
        finally:db.close()

    def test_launch_denies_missing_evidence_and_c3(self):
        self.assertTrue(launch_errors({}))
        cfg=dict(self.state['cfg']);cfg['fleet']['c3']={'id':999}
        self.assertTrue(any('KONA' in e for e in launch_errors(cfg)))
        del cfg['fleet']['c3']
        with self.assertRaises(ValueError):StripeLiveGateway('rk_live_fake',cfg)

    def test_restricted_test_key_supported_live_rejected(self):
        with patch('stripe.StripeClient') as client:
            StripeTestGateway('rk_test_fake');client.assert_called_once()
        with self.assertRaises(ValueError):StripeTestGateway('rk_live_fake')

    def test_cancellation_boundary_and_deposit_not_fee(self):
        _,q=self.paid()
        self.assertEqual(cancellation_fee(q,self.start-timedelta(hours=24)),0)
        self.assertEqual(cancellation_fee(q,self.start-timedelta(hours=24)+timedelta(seconds=1)),4900)
        self.assertEqual(cancellation_fee(q,self.start),4900)

    def test_free_cancel_full_refund_once_and_inventory_free(self):
        h,_=self.paid();rid=self.l.cancel(h['id'],self.start-timedelta(days=2))
        self.assertEqual(self.refund(rid)['amount_cents'],64700)
        self.assertEqual(self.l.process(rid),'succeeded');self.assertEqual(self.l.process(rid),'succeeded')
        self.assertEqual(self.l.cancel(h['id'],self.start-timedelta(days=2)),rid)
        db=portal.get_db()
        self.assertTrue(portal.mietfahrzeug_zeitraum_frei_db(db,h['mietfahrzeug_id'],self.start.date(),(self.start+timedelta(days=3)).date()));db.close()

    def test_late_cancel_and_lesser_damage_credit(self):
        h,_=self.paid();rid=self.l.cancel(h['id'],self.start-timedelta(hours=1))
        self.assertEqual(self.refund(rid)['amount_cents'],59800)
        self.l.process(rid)
        key=uuid.uuid4().hex;credit=self.l.credit(h['id'],4900,'Wiedervermietung: kein Schaden',key)
        self.assertEqual(self.l.process(credit),'succeeded')
        self.assertEqual(self.l.credit(h['id'],4900,'Wiedervermietung: kein Schaden',key),key)
        with self.assertRaises(ValueError):self.l.credit(h['id'],1,'zu viel',uuid.uuid4().hex)

    def test_customer_cannot_cancel_after_start_admin_can_record_no_show(self):
        h,_=self.paid()
        with self.assertRaises(ValueError):self.l.cancel(h['id'],self.start+timedelta(minutes=1))
        rid=self.l.cancel(h['id'],self.start+timedelta(minutes=1),admin=True,no_show=True)
        self.assertEqual(self.refund(rid)['amount_cents'],59800)

    def test_timeout_after_refund_retries_identical_operation(self):
        h,_=self.paid();rid=self.l.cancel(h['id']);original=self.g.refund
        def timeout(*args):original(*args);raise TimeoutError()
        with patch.object(self.g,'refund',side_effect=timeout):
            with self.assertRaises(TimeoutError):self.l.process(rid)
        self.assertEqual(self.refund(rid)['status'],'queued')
        self.assertEqual(self.l.process(rid),'succeeded')

    def test_old_unknown_refund_does_not_resubmit(self):
        h,_=self.paid();rid=self.l.cancel(h['id']);db=portal.get_db()
        db.execute('UPDATE miet_checkout_refunds SET created_at=? WHERE id=?',((datetime.now(timezone.utc)-timedelta(days=2)).isoformat(),rid));db.commit();db.close()
        with patch.object(self.g,'refund') as send:
            with self.assertRaises(ValueError):self.l.process(rid)
            send.assert_not_called()

    def test_parallel_cancel_one_ledger_entry(self):
        h,_=self.paid()
        with ThreadPoolExecutor(5) as pool:ids=list(pool.map(lambda _:self.l.cancel(h['id']),range(5)))
        self.assertEqual(len(set(ids)),1)

    def test_deposit_only_after_return(self):
        h,_=self.paid()
        with self.assertRaises(ValueError):self.l.deposit(h['id'],'geprüft')
        db=portal.get_db();db.execute("UPDATE mietvorgaenge SET status='zurueck' WHERE id=?",(h['mietvorgang_id'],));db.commit();db.close()
        rid=self.l.deposit(h['id'],'Rückgabeprotokoll geprüft, keine Abzüge')
        self.assertEqual(self.refund(rid)['amount_cents'],50000)
        self.assertEqual(self.l.process(rid),'succeeded')

    def test_admin_route_requires_authentication(self):
        r=portal.app.test_client().get('/mietwagen-test/admin')
        self.assertNotEqual(r.status_code,200)

    def ready_config(self):
        cfg=json.loads(json.dumps(self.state['cfg']))
        cfg.update(mode='live',live_enabled=True,origin='https://booking.example.invalid',
            terms_version='test-fixture-final-v1',deposit_method='charge_with_rent_refund_after_return',
            privacy_url='https://booking.example.invalid/privacy',merchant_name='Gärtner GmbH Karosserie + Lack',
            merchant_address='Binauer Höhe 4, 74821 Mosbach, Deutschland',
            merchant_email='test@example.invalid',merchant_phone='TEST')
        cfg['launch']={name:{'approved_by':'TEST FIXTURE','approved_at':'2026-09-22','evidence':'TEST ONLY'}
            for name in ['business_review','legal_review','finance_review','privacy_review','sandbox_acceptance','postgres_acceptance']}
        cfg['launch']['insurance']={}
        for slug,v in cfg['fleet'].items():
            v['expected_name']='TEST'
            cfg['launch']['insurance'][slug]={'verified':True,'use':'paid_self_drive','evidence':'TEST ONLY','vehicle_id':v['id']}
        return cfg

    def test_each_launch_gate_independently_required(self):
        cfg=self.ready_config();self.assertEqual(launch_errors(cfg),[])
        wrong=json.loads(json.dumps(cfg));wrong['merchant_name']='Autovermietung MOS'
        self.assertTrue(any('Vermieter' in error for error in launch_errors(wrong)))
        for field in ['business_review','legal_review','finance_review','privacy_review','sandbox_acceptance','postgres_acceptance','insurance']:
            changed=json.loads(json.dumps(cfg));del changed['launch'][field]
            self.assertTrue(launch_errors(changed),field)
        with patch('stripe.StripeClient') as client:
            StripeLiveGateway('rk_live_TEST_NOT_REAL',cfg);client.assert_called_once()

    def test_live_checkout_split_total_and_mode_bound_webhook(self):
        cfg=self.ready_config()
        with patch('stripe.StripeClient'):
            gateway=StripeLiveGateway('rk_live_TEST_NOT_REAL',cfg)
        service=SharedCheckout(portal,gateway,'https://booking.example.invalid/mieten/status')
        q={'amount_cents':64700,'rental_cents':14700,'deposit_charged_cents':50000,'currency':'eur',
           'rules_version':'fixture-final','daily_cents':4900,'start_slot':self.start.isoformat(),
           'end_slot':(self.start+timedelta(days=3)).isoformat(),'vehicle_name':'KONA TEST','days':3,
           'included_km':450,'extra_km_cents':25}
        h=service.reserve(uuid.uuid4().hex,self.state['cfg']['fleet']['kona']['id'],self.start.date().isoformat(),
             (self.start+timedelta(days=3)).date().isoformat(),{'name':'TEST','email':'test@example.invalid'},q)
        def create(params,key):
            self.assertEqual(sum(i['price_data']['unit_amount'] for i in params['line_items']),64700)
            self.assertEqual(len(params['line_items']),2);self.assertEqual(params['submit_type'],'pay')
            self.assertEqual(params['success_url'],'https://booking.example.invalid/mieten/status/'+h['id'])
            return {'id':'cs_live_TEST_FIXTURE','url':'https://checkout.stripe.com/TEST','livemode':True,'mode':'payment',
                'currency':'eur','amount_total':64700,'client_reference_id':h['id'],'metadata':params['metadata']}
        with patch.object(gateway,'create',side_effect=create):session=service.create_checkout(h['id'])
        self.assertEqual(session['id'],'cs_live_TEST_FIXTURE')
        with self.assertRaises(ValueError):self.s.validate(h,session)


if __name__=='__main__':unittest.main()
