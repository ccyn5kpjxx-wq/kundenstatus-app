"""Acceptance harness uses fake Stripe HTTP and actual SDK signatures only."""
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
import json
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'scripts'))
import stripe
import shop_sandbox_acceptance as acceptance


class FakeStripe:
    def __init__(self,now):
        self.now,self.creates,self.captures,self.cancels=now,0,0,0
        self.session,self.intent=None,None
        self.v1=SimpleNamespace(checkout=SimpleNamespace(sessions=SimpleNamespace(create=self.create,retrieve=self.session_get)),
            payment_intents=SimpleNamespace(retrieve=self.intent_get,capture=self.capture,cancel=self.cancel),
            charges=SimpleNamespace(retrieve=self.charge_get))

    def create(self,params,*,options):
        self.creates+=1
        amount=sum(item['quantity']*item['price_data']['unit_amount'] for item in params['line_items'])
        assert params['payment_intent_data']['capture_method']=='manual'
        self.session={'object':'checkout.session','id':'cs_test_SANDBOX','livemode':False,'mode':'payment','currency':'eur',
            'amount_total':amount,'metadata':deepcopy(params['metadata']),'client_reference_id':params['client_reference_id'],
            'status':'open','payment_status':'unpaid','expires_at':params['expires_at'],
            'success_url':params['success_url'],'cancel_url':params['cancel_url'],'payment_link':None,
            'payment_intent':None,'url':'https://checkout.stripe.com/c/pay/cs_test_SANDBOX#PRIVATE_URL_CANARY'}
        return deepcopy(self.session)

    def authorize(self):
        self.session.update(status='complete',payment_intent='pi_SANDBOX',url=None)
        amount=self.session['amount_total']
        self.intent={'object':'payment_intent','id':'pi_SANDBOX','livemode':False,'currency':'eur','amount':amount,
            'amount_capturable':amount,'amount_received':0,'capture_method':'manual','metadata':deepcopy(self.session['metadata']),
            'status':'requires_capture','payment_method_types':['card'],'latest_charge':'ch_SANDBOX'}

    def session_get(self,sid):
        assert sid=='cs_test_SANDBOX'
        return deepcopy(self.session)
    def intent_get(self,iid):
        assert iid=='pi_SANDBOX'
        return deepcopy(self.intent)
    def charge_get(self,cid):
        assert cid=='ch_SANDBOX'
        return {'object':'charge','id':cid,'payment_intent':'pi_SANDBOX','livemode':False,'currency':'eur',
            'amount':self.intent['amount'],'captured':False,'paid':True,'amount_captured':0,'refunded':False,
            'amount_refunded':0,'payment_method_details':{'type':'card','card':{'capture_before':int(self.now.timestamp())+7200}}}
    def capture(self,iid,params,*,options):
        assert iid=='pi_SANDBOX' and params=={'amount_to_capture':self.intent['amount']}
        self.captures+=1
        self.intent.update(status='succeeded',amount_capturable=0,amount_received=self.intent['amount'])
        return deepcopy(self.intent)
    def cancel(self,iid,params,*,options):
        assert iid=='pi_SANDBOX' and params=={'cancellation_reason':'abandoned'}
        self.cancels+=1
        self.intent.update(status='canceled',amount_capturable=0,amount_received=0)
        return deepcopy(self.intent)


class AcceptanceTests(unittest.TestCase):
    key='rk_test_SYNTHETIC_ONLY_KEY'

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'private-sandbox.json'
        self.now=datetime.now(timezone.utc)
        self.client=FakeStripe(self.now)
        self.network=patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden'))
        self.network.start()

    def tearDown(self):
        self.network.stop()
        self.temp.cleanup()

    def start(self,outcome='accepted'):
        return acceptance.start_sandbox(self.path,key=self.key,public_origin='https://shop.example.test',
            supplier_outcome=outcome,sdk=stripe,client=self.client,clock=lambda:self.now)

    def verify(self):
        return acceptance.verify_sandbox(self.path,key=self.key,sdk=stripe,client=self.client,clock=lambda:self.now)

    def test_default_and_help_plan_no_environment_files_threads_or_network(self):
        stream=StringIO()
        class NoEnvironment(dict):
            def get(self,*args):raise AssertionError('env read forbidden')
        with redirect_stdout(stream),patch.object(Path,'open',side_effect=AssertionError('file read forbidden')):
            self.assertEqual(acceptance.main([],environ=NoEnvironment()),0)
        self.assertEqual(json.loads(stream.getvalue())['mode'],'offline_plan')
        self.assertEqual(list(Path(self.temp.name).iterdir()),[])

    def test_live_missing_or_legacy_only_key_is_rejected_before_any_io(self):
        for env in ({},{'SHOP_SANDBOX_STRIPE_KEY':'sk_live_DO_NOT_TOUCH'},
                    {'REIFEN_STRIPE_SECRET_KEY':self.key}):
            stream=StringIO()
            with redirect_stdout(stream),patch.object(Path,'open',side_effect=AssertionError('file read forbidden')):
                self.assertEqual(acceptance.main(['--run-sandbox','--private-state',str(self.path),
                    '--public-origin','https://shop.example.test'],environ=env),2)
            self.assertEqual(json.loads(stream.getvalue())['code'],'explicit_test_key_required')
        self.assertEqual(self.client.creates,0)

    def test_manual_test_checkout_is_private_and_pending_never_orders_or_captures(self):
        result=self.start()
        self.assertTrue(result['hosted_url_saved_privately'])
        self.assertEqual(self.client.creates,1)
        for private in ('PRIVATE_URL_CANARY','cs_test_','sho_','whsec_',self.key):
            self.assertNotIn(private,json.dumps(result))
        state=json.loads(self.path.read_text())
        self.assertIn('PRIVATE_URL_CANARY',state['checkout']['checkout_url'])
        self.assertNotIn(self.key,self.path.read_text())
        pending=self.verify()
        self.assertEqual(pending['status'],'awaiting_hosted_test_card')
        self.assertEqual((self.client.captures,self.client.cancels),(0,0))
        with self.assertRaisesRegex(acceptance.SandboxAcceptanceError,'already_exists'):
            self.start()
        self.assertEqual(self.client.creates,1)

    def test_full_fake_supplier_acceptance_captures_test_only_and_replay_never_posts_again(self):
        self.start()
        self.client.authorize()
        report=self.verify()
        self.assertEqual(report['status'],'paid')
        self.assertEqual(report['fulfillment_state'],'captured')
        self.assertEqual(report['fake_supplier_calls_this_verify'],3)
        self.assertEqual(report['fake_smtp_submissions_this_verify'],2)
        self.assertTrue(report['synthetic_signed_event_verified'])
        self.assertFalse(report['actual_stripe_webhook_delivery_exercised'])
        self.assertEqual((report['real_supplier_calls'],report['real_emails'],report['live_payments']),(0,0,0))
        self.assertEqual((self.client.captures,self.client.cancels),(1,0))
        again=self.verify()
        self.assertEqual(again['fake_supplier_calls_this_verify'],0)
        self.assertEqual((self.client.captures,self.client.cancels),(1,0))

    def test_all_rejection_cancels_test_hold_and_timeout_keeps_manual_review(self):
        for outcome,expected,captures,cancels in (('rejected','cancelled',0,1),('timeout','manual_review',0,0)):
            with self.subTest(outcome=outcome):
                self.path=Path(self.temp.name)/(outcome+'.json')
                self.client=FakeStripe(self.now)
                self.start(outcome)
                self.client.authorize()
                result=self.verify()
                self.assertEqual(result['fulfillment_state'],expected)
                self.assertEqual((self.client.captures,self.client.cancels),(captures,cancels))
                self.assertEqual(self.verify()['fake_supplier_calls_this_verify'],0)
                self.assertEqual((self.client.captures,self.client.cancels),(captures,cancels))


if __name__=='__main__':unittest.main()
