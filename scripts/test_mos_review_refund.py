"""Offline regression: paid-but-unconfirmed MOS review refunds never make a rental."""
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
import html
import json
import re
import sys
import tempfile
import unittest

from PIL import Image, ImageDraw

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'scripts'))


def deny(*args,**kwargs):
    raise AssertionError('External network forbidden')


patch('socket.socket.connect',deny).start()
patch('socket.socket.connect_ex',deny).start()
patch('socket.create_connection',deny).start()
from run_mos_public_test import build_test_app

TEMP=tempfile.TemporaryDirectory(prefix='mos-review-refund-')
portal=build_test_app(TEMP.name,origin='http://localhost')
portal.app.config['MOS_PUBLIC_BOOKING']['deposit_method']='card_authorization_at_booking'


class ReviewRefundTests(unittest.TestCase):
    def setUp(self):
        portal.app.config.update(TESTING=True)
        portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=True
        portal.app.config['MOS_SHARED_CHECKOUT_ENABLED']=True
        self.client=portal.app.test_client()
        self.client.get('/mietwagen-test/')
        self.state=portal.app.extensions['mos_public_booking']
        self.s=self.state['service']
        self.g=self.state['gateway']
        self.l=self.state['ledger']
        db=portal.get_db()
        try:
            for table in ('miet_checkout_review_refunds','miet_checkout_refunds',
                          'miet_checkout_cancellations','miet_checkout_events',
                          'miet_checkout_deposit_auths','miet_checkout_creation_attempts',
                          'miet_checkout_contracts','miet_checkout_holds','mietvorgaenge'):
                db.execute('DELETE FROM '+table)
            db.commit()
        finally:
            db.close()

    def post(self,path,data=None,client=None):
        client=client or self.client
        with client.session_transaction() as session:
            csrf=session.get('csrf_token')
        return client.post(path,data={**(data or {}),'csrf_token':csrf})

    def signature(self):
        picture=Image.new('RGBA',(700,180),(255,255,255,0))
        ImageDraw.Draw(picture).line([(40,110),(90,40),(125,125),(180,60),
                                      (240,110),(320,50),(380,105)],
                                     fill=(20,30,40,255),width=5)
        stream=BytesIO();picture.save(stream,format='PNG')
        return 'data:image/png;base64,'+b64encode(stream.getvalue()).decode('ascii')

    def prepared_checkout(self):
        cfg=self.state['cfg']
        page=self.post('/mietwagen-test/quote',{'vehicle':'kona',
                      'start':cfg['slots'][0],'end':cfg['slots'][3]})
        self.assertEqual(page.status_code,200)
        token=html.unescape(re.search(r'name="quote_token" value="([^"]+)"',
                                    page.get_data(as_text=True))[1])
        reserved=self.post('/mietwagen-test/checkout',{'quote_token':token,
                           'accept':'yes','sign_confirm':'yes','name':'Test',
                           'email':'test@example.invalid','signature_data':self.signature()})
        self.assertEqual(reserved.status_code,303)
        hold_id=reserved.location.rsplit('/',2)[-2]
        intent_id=self.s._deposit_record(hold_id)['intent_id']
        self.g.authorize_deposit_intent(intent_id)
        rent=self.post('/mietwagen-test/status/'+hold_id+'/retry')
        self.assertEqual(rent.status_code,303)
        session_id=self.s.read(hold_id)['session_id']
        self.assertTrue(session_id)
        return hold_id,intent_id,session_id

    def paid_review(self):
        hold,intent,sid=self.prepared_checkout()
        portal.app.config['MOS_SHARED_CHECKOUT_ENABLED']=False
        portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=False
        paid=self.g.pay(sid)
        body,sig=self.g.signed_event(sid)
        self.assertIsNone(self.s.handle_signed_event(body,sig,self.state['secret']))
        self.assertEqual(self.s.read(hold)['status'],'review')
        self.assertEqual(self.s.read(hold)['payment_intent'],paid['payment_intent'])
        return hold,intent,sid

    def admin_client(self):
        admin=portal.app.test_client()
        with admin.session_transaction() as session:
            session['admin']=True
        self.assertEqual(admin.get('/mietwagen-test/admin').status_code,200)
        return admin

    def refund_action(self,hold,admin=None,**overrides):
        data={'action':'review_full_refund','operator_name':'Werkstattprüfung',
              'reason':'Kunde kann das nicht bestätigte Auto nicht übernehmen.',
              'confirm_review_refund':'yes'}
        data.update(overrides)
        return self.post('/mietwagen-test/admin/'+hold,data,
                         client=admin or self.admin_client())

    def rows(self,hold):
        db=portal.get_db()
        try:
            refunds=[dict(row) for row in db.execute(
                'SELECT * FROM miet_checkout_refunds WHERE hold_id=?',(hold,)).fetchall()]
            audit=db.execute('SELECT * FROM miet_checkout_review_refunds WHERE hold_id=?',
                             (hold,)).fetchone()
            rental_count=db.execute('SELECT COUNT(*) AS n FROM mietvorgaenge').fetchone()['n']
            return refunds,dict(audit) if audit else None,rental_count
        finally:
            db.close()

    def test_admin_full_refund_and_card_release_are_separate_and_terminal(self):
        hold,intent,sid=self.paid_review()
        unauthorized=portal.app.test_client().post('/mietwagen-test/admin/'+hold,
            data={'action':'review_full_refund','reason':'Synthetic reason'})
        self.assertNotEqual(unauthorized.status_code,303)
        admin=self.admin_client()
        self.assertEqual(self.refund_action(hold,admin,confirm_review_refund='').status_code,409)
        self.assertEqual(self.rows(hold)[0],[])
        self.assertEqual(self.refund_action(hold,admin).status_code,303)
        refunds,audit,rental_count=self.rows(hold)
        self.assertEqual((len(refunds),refunds[0]['status'],refunds[0]['kind']),
                         (1,'succeeded','review_full_refund'))
        self.assertEqual(refunds[0]['amount_cents'],14700)
        self.assertEqual(audit['session_id'],sid)
        self.assertEqual(audit['payment_intent'],self.s.read(hold)['payment_intent'])
        self.assertEqual(rental_count,0)
        self.assertEqual((self.s.read(hold)['status'],self.s.read(hold)['grund']),
                         ('released','review_full_refund_completed'))
        self.assertEqual(self.s._deposit_record(hold)['status'],'released')
        self.assertEqual(self.g.retrieve_deposit_intent(intent)['status'],'canceled')
        status_page=self.client.get('/mietwagen-test/status/'+hold).get_data(as_text=True)
        self.assertIn('Die vollständige Mietpreiserstattung wurde vom Zahlungsanbieter bestätigt',
                      status_page)
        self.assertEqual(self.refund_action(hold,admin).status_code,409)
        self.assertEqual(len(self.rows(hold)[0]),1)
        body,sig=self.g.signed_event(sid,event_id='evt_offline_late_review_refund')
        self.assertIsNone(self.s.handle_signed_event(body,sig,self.state['secret']))
        self.assertEqual(self.s.read(hold)['status'],'released')
        self.assertEqual(self.rows(hold)[2],0)

    def test_unpaid_or_mismatched_provider_session_cannot_queue_refund(self):
        hold,_,sid=self.prepared_checkout()
        db=portal.get_db()
        try:
            db.execute("UPDATE miet_checkout_holds SET status='review' WHERE id=?",(hold,))
            db.commit()
        finally:db.close()
        self.assertEqual(self.refund_action(hold).status_code,409)
        self.assertEqual(self.rows(hold)[0],[])
        paid=self.g.pay(sid)
        db=portal.get_db()
        try:
            db.execute('UPDATE miet_checkout_holds SET payment_intent=? WHERE id=?',
                       (paid['payment_intent'],hold))
            db.commit()
        finally:db.close()
        original=self.g.retrieve
        with patch.object(self.g,'retrieve',side_effect=lambda key:{**original(key),'payment_intent':'pi_other'}):
            self.assertEqual(self.refund_action(hold).status_code,409)
        self.assertEqual(self.rows(hold)[0],[])
        self.assertEqual(self.s.read(hold)['status'],'review')

    def test_provider_quote_mismatch_and_existing_refund_block_new_full_refund(self):
        hold,_,sid=self.paid_review()
        original=self.g.retrieve
        with patch.object(self.g,'retrieve',side_effect=lambda key:{**original(key),'amount_total':1}):
            self.assertEqual(self.refund_action(hold).status_code,409)
        self.assertEqual(self.rows(hold)[0],[])
        db=portal.get_db()
        try:
            db.execute('''INSERT INTO miet_checkout_refunds
                (id,hold_id,amount_cents,kind,status,reason,created_at)
                VALUES (?,?,?,?,?,?,?)''',
                ('prior-'+hold,hold,100,'other','failed','synthetic prior attempt',
                 datetime.now(timezone.utc).isoformat()))
            db.commit()
        finally:
            db.close()
        self.assertEqual(self.refund_action(hold).status_code,409)
        refunds,audit,rental_count=self.rows(hold)
        self.assertEqual((len(refunds),refunds[0]['id'],audit,rental_count),
                         (1,'prior-'+hold,None,0))

    def test_stripe_refund_without_livemode_completes_review(self):
        hold,_,_=self.paid_review()
        original=self.g.refund
        def stripe_schema(pi,amount,key):
            return {k:v for k,v in original(pi,amount,key).items() if k!='livemode'}
        with patch.object(self.g,'refund',side_effect=stripe_schema):
            self.assertEqual(self.refund_action(hold).status_code,303)
        refunds,_,rental_count=self.rows(hold)
        self.assertEqual((len(refunds),refunds[0]['status'],rental_count),(1,'succeeded',0))
        self.assertEqual(self.s.read(hold)['status'],'released')

    def test_mismatched_refund_response_never_completes_review(self):
        hold,_,_=self.paid_review()
        original=self.g.refund
        def wrong_payment(pi,amount,key):
            return {**original(pi,amount,key),'payment_intent':'pi_other'}
        with patch.object(self.g,'refund',side_effect=wrong_payment):
            self.assertEqual(self.refund_action(hold).status_code,303)
        refunds,_,rental_count=self.rows(hold)
        self.assertEqual((len(refunds),refunds[0]['status'],rental_count),(1,'queued',0))
        self.assertEqual(self.s.read(hold)['status'],'review')

    def test_uncertain_refund_stays_blocked_and_retries_same_reference(self):
        hold,_,_=self.paid_review()
        with patch.object(self.g,'refund',side_effect=ConnectionError('synthetic timeout')):
            self.assertEqual(self.refund_action(hold).status_code,303)
        refunds,audit,rental_count=self.rows(hold)
        self.assertEqual((len(refunds),refunds[0]['status'],rental_count),(1,'queued',0))
        self.assertEqual(refunds[0]['id'],'review-full-'+hold)
        self.assertEqual(self.s.read(hold)['status'],'review')
        self.assertEqual(self.s._deposit_record(hold)['status'],'released')
        self.assertEqual(self.refund_action(hold).status_code,303)
        refunds,_,rental_count=self.rows(hold)
        self.assertEqual((len(refunds),refunds[0]['status'],rental_count),(1,'succeeded',0))
        self.assertEqual(self.s.read(hold)['status'],'released')

    def test_card_release_uncertainty_keeps_review_even_after_refund(self):
        hold,_,_=self.paid_review()
        with patch.object(self.s,'release_deposit',side_effect=ConnectionError('synthetic timeout')):
            self.assertEqual(self.refund_action(hold).status_code,303)
        self.assertEqual(self.rows(hold)[0][0]['status'],'succeeded')
        self.assertEqual(self.s.read(hold)['status'],'review')
        self.assertEqual(self.refund_action(hold).status_code,303)
        self.assertEqual(self.s.read(hold)['status'],'released')
        self.assertEqual(len(self.rows(hold)[0]),1)


if __name__=='__main__':
    unittest.main()
