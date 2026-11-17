"""Public card-correction routes against disposable inventory and an offline provider."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
import test_mos_deferred_public as flow_fixture


class DepositRetryRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Flask's test client stays in process; all provider objects are synthetic.
        cls.network=patch('socket.socket.connect',side_effect=AssertionError('No network in public retry tests'))
        cls.network.start()
        cls.addClassCleanup(cls.network.stop)
        flow_fixture.DeferredPublicTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        flow_fixture.DeferredPublicTests.tearDownClass()

    def setUp(self):
        self.flow=flow_fixture.DeferredPublicTests()
        self.flow.setUp()
        self.portal=self.flow.portal;self.client=self.flow.client
        self.service=self.flow.service;self.gateway=self.flow.gateway
        self.hold=self.flow.paid_booking()
        self.status_url='/mietwagen-test/status/'+self.hold['id']
        self.retry_url=self.status_url+'/kaution-neu'

    def post(self,path,data=None,client=None):
        return self.flow.post(client or self.client,path,data or {})

    def execute(self,sql,params=()):
        db=self.portal.get_db()
        try:db.execute(sql,params);db.commit()
        finally:db.close()

    def attempts(self):
        db=self.portal.get_db()
        try:return [dict(row) for row in db.execute(
            'SELECT * FROM miet_checkout_deposit_attempts WHERE hold_id=? ORDER BY generation',
            (self.hold['id'],)).fetchall()]
        finally:db.close()

    def rent_session_count(self):
        with closing(sqlite3.connect(self.gateway.path)) as db:
            return db.execute('SELECT COUNT(*) FROM demo_sessions').fetchone()[0]

    def release_bad_card(self):
        # Caller supplies the due-window clock so the subsequent HTTP requests
        # exercise the same timing as the real customer-facing correction flow.
        form=self.client.get(self.status_url+'/kaution')
        self.assertEqual(form.status_code,200,form.get_data(as_text=True))
        original=self.service._deposit_record(self.hold['id'])['intent_id']
        self.gateway.authorize_deposit_intent(original,funding='debit')
        failed=self.client.get(self.status_url+'/kaution')
        self.assertEqual(failed.status_code,409,failed.get_data(as_text=True))
        self.assertEqual(self.service._deposit_record(self.hold['id'])['status'],'released')
        self.assertEqual(self.gateway.retrieve_deposit_intent(original)['status'],'canceled')
        return original

    def assert_no_retry_ui(self,text):
        self.assertNotIn('action="'+self.retry_url+'"',text)
        self.assertNotIn('Mit anderer Kreditkarte erneut versuchen',text)

    def assert_same_paid_rental(self):
        current=self.service.read(self.hold['id'])
        self.assertEqual(current['status'],'confirmed')
        for key in ('session_id','payment_intent','mietvorgang_id'):
            self.assertEqual(current[key],self.hold[key])
        self.assertEqual(self.flow.rental_status(current),'aktiv')

    def test_released_bad_card_exposes_csrf_post_but_get_never_starts_an_attempt(self):
        with self.flow.due_window(self.hold):
            original=self.release_bad_card()
            before=self.attempts()
            with patch.object(self.gateway,'create_deposit_intent',wraps=self.gateway.create_deposit_intent) as create:
                page=self.client.get(self.status_url)
                self.assertEqual(page.status_code,200)
                text=page.get_data(as_text=True)
                form=re.search(r'<form\b[^>]*action="'+re.escape(self.retry_url)+r'"[^>]*>(.*?)</form>',text,re.S)
                self.assertIsNotNone(form)
                self.assertIn('method="post"',form[0])
                self.assertIn('name="csrf_token"',form[1])
                self.assertIn('Mietpreis wird dabei nicht noch einmal bezahlt',text)
                self.assertEqual(self.client.get(self.retry_url).status_code,405)
                create.assert_not_called()
            self.assertEqual(self.attempts(),before)
            self.assertEqual(self.service._deposit_record(self.hold['id'])['intent_id'],original)
        self.assert_same_paid_rental()

    def test_explicit_retry_gets_new_generation_without_new_rent_and_replays_once(self):
        with self.flow.due_window(self.hold):
            original=self.release_bad_card()
            sessions=self.rent_session_count()
            with patch.object(self.gateway,'create',side_effect=AssertionError('No second rent checkout')), \
                    patch.object(self.gateway,'capture_deposit_intent',side_effect=AssertionError('Never charge the deposit')):
                result=self.post(self.retry_url)
                self.assertEqual(result.status_code,303,result.get_data(as_text=True))
                self.assertEqual(result.location,self.status_url+'/kaution')
                current=self.service._deposit_record(self.hold['id'])
                self.assertNotEqual(current['intent_id'],original)
                self.assertEqual(current['status'],'awaiting_card')
                self.assertEqual(self.post(self.retry_url).status_code,303)
                self.assertEqual(self.service._deposit_record(self.hold['id'])['intent_id'],current['intent_id'])
                self.assertEqual(self.client.get(result.location).status_code,200)
                authorized=self.post(self.status_url+'/kaution-test')
                self.assertEqual(authorized.status_code,303,authorized.get_data(as_text=True))
                self.assertTrue(self.service.deposit_ready(self.hold['id']))
            attempts=self.attempts()
            self.assertEqual([row['generation'] for row in attempts],[1,2])
            self.assertEqual((attempts[0]['intent_id'],attempts[0]['status'],attempts[0]['amount_received']),
                             (original,'released',0))
            self.assertEqual(attempts[1]['status'],'authorized')
            self.assertEqual(self.rent_session_count(),sessions)
        self.assert_same_paid_rental()

    def test_retry_requires_csrf_and_customer_ownership(self):
        with self.flow.due_window(self.hold):
            self.release_bad_card()
            foreign=self.portal.app.test_client()
            self.assertEqual(foreign.get('/mietwagen-test/').status_code,200)
            with patch.object(self.gateway,'create_deposit_intent') as create:
                self.assertEqual(self.client.post(self.retry_url,data={}).status_code,400)
                self.assertEqual(self.post(self.retry_url,client=foreign).status_code,404)
                create.assert_not_called()
            self.assertEqual(len(self.attempts()),1)
        self.assert_same_paid_rental()

    def test_individual_deposit_scope_can_retry_without_gaining_cancel_rights(self):
        with self.flow.due_window(self.hold):
            self.release_bad_card()
            scoped=self.portal.app.test_client()
            scoped.get('/mietwagen-test/buchung-oeffnen')
            quote=json.loads(self.hold['payload'])['quote']
            with scoped.session_transaction() as state:
                state['mos_deposit_access']={self.hold['id']:{'scope':'deposit',
                    'owner_hash':quote['owner_hash'],'payload_sha256':sha256(self.hold['payload'].encode()).hexdigest(),
                    'expires_at':int((datetime.now(timezone.utc)+timedelta(hours=1)).timestamp())}}
            self.assertEqual(scoped.get(self.status_url).status_code,200)
            self.assertEqual(self.post(self.retry_url,client=scoped).status_code,303)
            self.assertEqual(self.post(self.status_url+'/stornieren',{'confirm':'yes'},client=scoped).status_code,404)
            self.assertEqual(scoped.get(self.status_url+'/vertrag.pdf').status_code,404)
        self.assert_same_paid_rental()

    def test_uncertain_releasing_or_review_state_has_no_retry_button_or_new_intent(self):
        with self.flow.due_window(self.hold):
            self.release_bad_card()
            for status in ('releasing','review'):
                self.execute('UPDATE miet_checkout_deposit_auths SET status=? WHERE hold_id=?',
                             (status,self.hold['id']))
                with self.subTest(status=status), \
                        patch.object(self.gateway,'create_deposit_intent') as create, \
                        patch.object(self.gateway,'retrieve_deposit_intent') as retrieve:
                    page=self.client.get(self.status_url)
                    self.assertEqual(page.status_code,200)
                    self.assert_no_retry_ui(page.get_data(as_text=True))
                    blocked=self.post(self.retry_url)
                    self.assertEqual(blocked.status_code,409)
                    create.assert_not_called();retrieve.assert_not_called()
            self.assertEqual(len(self.attempts()),1)
        self.assert_same_paid_rental()

    def test_completed_handover_hides_all_card_actions_and_rejects_retry(self):
        with self.flow.due_window(self.hold):
            self.release_bad_card()
            self.execute('INSERT INTO miet_checkout_handovers VALUES (?,?,?,?,?,?,1,1,1,1)',
                         (self.hold['id'],self.hold['mietvorgang_id'],datetime.now(timezone.utc).isoformat(),
                          'Synthetischer Mitarbeiter',1234,'synthetic-handover'))
            with patch.object(self.gateway,'create_deposit_intent') as create:
                page=self.client.get(self.status_url)
                self.assertEqual(page.status_code,200)
                text=page.get_data(as_text=True)
                self.assert_no_retry_ui(text)
                self.assertNotIn('500 € Kaution online reservieren</a>',text)
                self.assertIn('Fahrzeug wurde bereits übergeben',text)
                self.assertEqual(self.post(self.retry_url).status_code,409)
                create.assert_not_called()
        self.assert_same_paid_rental()

    def test_returned_rental_hides_all_card_actions_and_rejects_retry(self):
        with self.flow.due_window(self.hold):
            self.release_bad_card()
            self.execute("UPDATE mietvorgaenge SET status='zurueck',rueckgabe_datum=? WHERE id=?",
                         (datetime.now(timezone.utc).date().isoformat(),self.hold['mietvorgang_id']))
            with patch.object(self.gateway,'create_deposit_intent') as create:
                page=self.client.get(self.status_url)
                self.assertEqual(page.status_code,200)
                text=page.get_data(as_text=True)
                self.assert_no_retry_ui(text)
                self.assertNotIn('500 € Kaution online reservieren</a>',text)
                self.assertIn('Miete ist beendet',text)
                self.assertEqual(self.post(self.retry_url).status_code,409)
                create.assert_not_called()
            self.assertEqual(len(self.attempts()),1)

    def test_early_retry_is_hidden_and_blocked_without_losing_paid_rental(self):
        with self.flow.due_window(self.hold):self.release_bad_card()
        with patch.object(self.gateway,'create_deposit_intent') as create:
            page=self.client.get(self.status_url)
            self.assertEqual(page.status_code,200)
            self.assert_no_retry_ui(page.get_data(as_text=True))
            self.assertEqual(self.post(self.retry_url).status_code,409)
            create.assert_not_called()
        self.assert_same_paid_rental()

    def test_existing_paid_customer_can_retry_while_new_bookings_are_paused(self):
        with self.flow.due_window(self.hold):
            self.release_bad_card()
            self.portal.app.config['MOS_PUBLIC_BOOKING']['enabled']=False
            page=self.client.get(self.status_url)
            self.assertEqual(page.status_code,200)
            self.assertIn('action="'+self.retry_url+'"',page.get_data(as_text=True))
            self.assertEqual(self.post(self.retry_url).status_code,303)
            self.assertEqual(len(self.attempts()),2)
        self.assert_same_paid_rental()

    def test_first_status_get_after_provider_cancel_immediately_offers_explicit_retry(self):
        with self.flow.due_window(self.hold):
            self.assertEqual(self.client.get(self.status_url+'/kaution').status_code,200)
            self.assertEqual(self.post(self.status_url+'/kaution-test').status_code,303)
            record=self.service._deposit_record(self.hold['id'])
            self.assertEqual(record['status'],'authorized')
            original=record['intent_id']
            # Simulate an external, verified zero-charge provider cancellation;
            # the local row is intentionally still authorized before this GET.
            self.gateway.cancel_deposit_intent(original,'synthetic-external-cancel-'+self.hold['id'])
            self.assertEqual(self.service._deposit_record(self.hold['id'])['status'],'authorized')
            before_sessions=self.rent_session_count()
            with patch.object(self.gateway,'create_deposit_intent',wraps=self.gateway.create_deposit_intent) as create:
                first=self.client.get(self.status_url)
                self.assertEqual(first.status_code,200,first.get_data(as_text=True))
                text=first.get_data(as_text=True)
                self.assertIn('action="'+self.retry_url+'"',text)
                self.assertNotIn('500 € Kaution online reservieren</a>',text)
                self.assertEqual(self.service._deposit_record(self.hold['id'])['status'],'released')
                self.assertEqual(len(self.attempts()),1)
                create.assert_not_called()
                self.assertEqual(self.post(self.retry_url).status_code,303)
                self.assertEqual(create.call_count,1)
            self.assertEqual(len(self.attempts()),2)
            self.assertNotEqual(self.service._deposit_record(self.hold['id'])['intent_id'],original)
            self.assertEqual(self.rent_session_count(),before_sessions)
        self.assert_same_paid_rental()


if __name__=='__main__':unittest.main()
