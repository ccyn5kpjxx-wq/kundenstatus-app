"""New marked long-rental deposits: paid once, returned only after clearance."""
from base64 import b64encode
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import unittest
import uuid
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'scripts'))
import test_mos_deferred_deposit as deferred_fixture
from test_postgres_contract_insert_adapter import FakeConnection
from mietwagen_checkout import (LONG_RENTAL_DEPOSIT_METHOD,LONG_RENTAL_DEPOSIT_POLICY,
    is_long_rental_paid_deposit,_checkout_disclosure)
from mos_public_contract import presign_quote,signed_payload
from mos_booking.production import RefundLedger
from mos_booking.gateway import StripeTestGateway
from mos_return import init_schema as init_return_schema,record as record_return,record_clearance


class LongRentalDepositTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deferred_fixture.DeferredDepositTests.setUpClass()
        cls.portal=deferred_fixture.DeferredDepositTests.portal
        db=cls.portal.get_db()
        try:init_return_schema(db);db.commit()
        finally:db.close()

    @classmethod
    def tearDownClass(cls):deferred_fixture.DeferredDepositTests.tearDownClass()

    def setUp(self):
        self.fixture=deferred_fixture.DeferredDepositTests();self.fixture.setUp()
        self.service=self.fixture.service;self.gateway=self.fixture.gateway
        self.ledger=RefundLedger(self.service)

    def raw_quote(self,days=10):
        start=(datetime.now(timezone.utc)+timedelta(days=100)).replace(second=0,microsecond=0)
        end=start+timedelta(days=days)
        return {'amount_cents':3900*days+50000,'rental_cents':3900*days,'deposit_charged_cents':50000,
            'deposit_authorized_cents':0,'deposit_cents':50000,'checkout_deposit':True,
            'deposit_method':LONG_RENTAL_DEPOSIT_METHOD,'long_rental_deposit_policy':LONG_RENTAL_DEPOSIT_POLICY,
            'currency':'eur','rules_version':'synthetic-long-rental-v1','vehicle_id':self.fixture.vehicle_id,
            'vehicle_name':'Synthetischer Langmietwagen','vehicle_plate':'TEST-LONG','vehicle_vin':'',
            'start_slot':start.isoformat(),'end_slot':end.isoformat(),'days':days,'daily_cents':3900,
            'deductible_cents':100000,'included_km':150*days,'extra_km_cents':25,
            'terms_text':'Synthetischer Test: Miete plus 500 EUR Kaution, Erstattung nach ordnungsgemäßer Rückgabe.',
            'test_only':False,'cancellation_policy':'free_48h_then_10pct_rent'}

    def hold(self,quote=None):
        quote=quote or self.raw_quote()
        signed=presign_quote(quote,self.fixture.customer,self.fixture.signature)
        return self.service.reserve(uuid.uuid4().hex,self.fixture.vehicle_id,
            datetime.fromisoformat(quote['start_slot']).date().isoformat(),
            datetime.fromisoformat(quote['end_slot']).date().isoformat(),self.fixture.customer,signed)

    def pay(self,hold):return self.fixture.pay(hold)[0]

    def proper_return(self,paid,*,clear=True):
        q=json.loads(paid['payload'])['quote'];start=datetime.fromisoformat(q['start_slot']);end=datetime.fromisoformat(q['end_slot'])
        db=self.portal.get_db()
        try:
            db.execute('INSERT INTO miet_checkout_handovers VALUES (?,?,?,?,?,?,1,1,1,1)',
                (paid['id'],paid['mietvorgang_id'],start.isoformat(),'Synthetischer Mitarbeiter',1000,'TEST-HANDOVER'))
            record_return(db,paid['id'],returned_at=end,operator_name='Synthetischer Mitarbeiter',
                odometer_km=1100,fuel_full=True,condition_recorded=True,damage_free=clear,
                charges_resolved=clear,no_objection=clear,protocol_ref='TEST-RETURN',
                note='' if clear else 'Schaden wird geprüft',now=end)
            db.commit()
        finally:db.close()

    def row(self,table,hold_id):
        db=self.portal.get_db()
        try:return db.execute('SELECT * FROM '+table+' WHERE hold_id=?',(hold_id,)).fetchone()
        finally:db.close()

    def test_ten_day_payment_has_two_line_items_and_no_card_hold(self):
        hold=self.hold();seen=[];create=self.gateway.create
        def capture(params,key):seen.append(params);return create(params,key)
        with patch.object(self.gateway,'create',side_effect=capture),patch.object(self.gateway,'create_deposit_intent') as auth:
            paid=self.pay(hold);auth.assert_not_called()
        self.assertEqual([row['price_data']['unit_amount'] for row in seen[0]['line_items']],[39000,50000])
        self.assertIsNone(self.service._deposit_record(paid['id']))
        evidence=self.service.verify_charged_deposit(paid['id'])
        self.assertEqual(evidence['object'],'checkout.session')
        self.assertEqual((evidence['amount_total'],evidence['deposit_net_cents'],evidence['refunded_cents']),(89000,50000,0))
        self.assertNotIn('client_secret',evidence)
        body,sig=self.gateway.signed_event(paid['session_id'])
        self.assertEqual(self.service.handle_signed_event(body,sig,'whsec_synthetic'),paid['mietvorgang_id'])
        _,disclosure=_checkout_disclosure(json.loads(paid['payload'])['quote'],test_mode=False)
        self.assertIn('500 EUR rückzahlbare Kaution im Zahlbetrag',disclosure)
        self.assertLessEqual(len(disclosure),500)

    def test_marked_quote_invariants_and_signed_policy_are_required(self):
        for changes in ({'days':5},{'days':31},{'amount_cents':39000},{'deposit_charged_cents':49999},
                        {'deposit_authorized_cents':50000},{'deposit_timing':'before_pickup'},
                        {'checkout_deposit':False},{'long_rental_deposit_policy':'unknown'}):
            with self.subTest(changes=changes),self.assertRaises(ValueError):self.hold({**self.raw_quote(),**changes})
        hold=self.hold();payload=json.loads(hold['payload'])
        contract,_,_=signed_payload(payload)
        self.assertEqual(contract['long_rental_deposit_policy'],LONG_RENTAL_DEPOSIT_POLICY)
        payload['quote'].pop('long_rental_deposit_policy')
        with self.assertRaises(ValueError):signed_payload(payload)

    def test_fresh_provider_refund_and_invalid_payment_prevent_handover_evidence(self):
        paid=self.pay(self.hold())
        self.gateway.refund(paid['payment_intent'],5000,'synthetic-external-refund')
        with self.assertRaisesRegex(ValueError,'beim Anbieter geprüft'):self.service.verify_charged_deposit(paid['id'])

    def test_refund_or_cancellation_racing_provider_read_is_checked_again(self):
        paid=self.pay(self.hold());retrieve=self.gateway.retrieve_payment
        def cancel_during_read(pi):
            evidence=retrieve(pi)
            self.ledger.cancel(paid['id'])
            return evidence
        with patch.object(self.gateway,'retrieve_payment',side_effect=cancel_during_read):
            with self.assertRaisesRegex(ValueError,'storniert'):self.service.verify_charged_deposit(paid['id'])

    def test_lock_wait_does_not_refresh_old_provider_evidence_for_handover(self):
        paid=self.pay(self.hold());clock=[datetime.now(timezone.utc)];initial=clock[0]
        original=self.service.locked;calls=[0]
        class FrozenDateTime(datetime):
            @classmethod
            def now(cls,tz=None):return clock[0].astimezone(tz) if tz else clock[0].replace(tzinfo=None)
        @contextmanager
        def delayed(vehicle_id):
            calls[0]+=1
            if calls[0]==2:clock[0]+=timedelta(seconds=61)
            with original(vehicle_id) as locked:yield locked
        with patch('mietwagen_checkout.datetime',FrozenDateTime),patch.object(self.service,'locked',side_effect=delayed):
            evidence=self.service.verify_charged_deposit(paid['id'])
        self.assertEqual(datetime.fromisoformat(evidence['verified_at']),initial)
        from mos_handover import _paid_deposit_ready
        contract,_,_=signed_payload(json.loads(paid['payload']))
        db=self.portal.get_db()
        try:
            with patch.object(self.gateway,'livemode',True),self.assertRaisesRegex(ValueError,'Frische Zahlungsprüfung'):
                _paid_deposit_ready(db,self.service,paid,json.loads(paid['payload'])['quote'],
                    contract,{**evidence,'livemode':True},clock[0])
        finally:db.close()

    def test_late_cancellation_fee_excludes_the_full_deposit(self):
        paid=self.pay(self.hold());q=json.loads(paid['payload'])['quote']
        rid=self.ledger.cancel(paid['id'],requested_at=datetime.fromisoformat(q['start_slot'])-timedelta(hours=24))
        row=self.row('miet_checkout_refunds',paid['id'])
        self.assertEqual(row['amount_cents'],85100)  # 89000 - 10% of 39000 rent.
        self.assertEqual(self.ledger.process(rid),'succeeded')
        self.assertEqual(self.ledger.process(rid),'succeeded')
        with self.assertRaises(ValueError):self.ledger.deposit(paid['id'],'Cannot refund twice')

    def test_refund_requires_a_qualified_factual_return_and_is_idempotent(self):
        paid=self.pay(self.hold())
        with self.assertRaisesRegex(ValueError,'ordnungsgemäßer'):self.ledger.deposit(paid['id'],'Too early')
        self.proper_return(paid)
        rid=self.ledger.deposit(paid['id'],'Ordnungsgemäße Rückgabe')
        self.assertEqual(self.ledger.deposit(paid['id'],'Repeated request'),rid)
        self.assertEqual(self.ledger.process(rid),'succeeded')
        self.assertEqual(self.ledger.deposit(paid['id'],'Already processed'),rid)
        self.assertEqual(self.ledger.process(rid),'succeeded')
        self.assertEqual(self.row('miet_checkout_refunds',paid['id'])['amount_cents'],50000)
        self.assertEqual(self.gateway.retrieve_payment(paid['payment_intent'])['latest_charge']['amount_refunded'],50000)

    def test_disputed_return_waits_for_documented_clearance(self):
        paid=self.pay(self.hold());self.proper_return(paid,clear=False)
        with self.assertRaises(ValueError):self.ledger.deposit(paid['id'],'Unresolved damage')
        q=json.loads(paid['payload'])['quote'];db=self.portal.get_db()
        try:
            record_clearance(db,paid['id'],operator_name='Synthetischer Mitarbeiter',evidence_ref='TEST-CLEARANCE',
                note='Alle Forderungen sind geklärt.',fuel_resolved=True,damage_resolved=True,time_km_resolved=True,
                now=datetime.fromisoformat(q['end_slot'])+timedelta(hours=1))
            db.commit()
        finally:db.close()
        self.assertEqual(self.ledger.deposit(paid['id'],'Vollständig geklärt'),'deposit-'+paid['id'])

    def test_external_deposit_refund_is_not_paid_a_second_time_from_the_rent_balance(self):
        paid=self.pay(self.hold(self.raw_quote(20)));self.proper_return(paid)
        self.gateway.refund(paid['payment_intent'],50000,'synthetic-external-deposit-refund')
        with self.assertRaisesRegex(ValueError,'beim Anbieter geprüft'):self.ledger.deposit(paid['id'],'Would duplicate external refund')
        self.assertIsNone(self.row('miet_checkout_refunds',paid['id']))

    def test_lost_refund_reply_reuses_the_original_key_and_never_pays_two_deposits(self):
        paid=self.pay(self.hold());self.proper_return(paid)
        rid=self.ledger.deposit(paid['id'],'Ordnungsgemäße Rückgabe')
        refund=self.gateway.refund
        def lose_reply(pi,amount,key,**kwargs):
            refund(pi,amount,key,**kwargs)
            raise OSError('Synthetic response lost after refund')
        with patch.object(self.gateway,'refund',side_effect=lose_reply):
            with self.assertRaises(OSError):self.ledger.process(rid)
        db=self.portal.get_db()
        try:
            db.execute('UPDATE miet_checkout_refunds SET created_at=? WHERE id=?',
                ((datetime.now(timezone.utc)-timedelta(days=2)).isoformat(),rid));db.commit()
        finally:db.close()
        self.assertEqual(self.ledger.deposit(paid['id'],'Retry same saved refund'),rid)
        with patch.object(self.gateway,'refund',wraps=refund) as repeated_create:
            self.assertEqual(self.ledger.process(rid),'succeeded')
            repeated_create.assert_not_called()
        self.assertEqual(self.gateway.retrieve_payment(paid['payment_intent'])['latest_charge']['amount_refunded'],50000)

    def test_unknown_old_request_with_no_matching_provider_refund_is_not_recreated(self):
        paid=self.pay(self.hold());self.proper_return(paid)
        rid=self.ledger.deposit(paid['id'],'Qualified return')
        db=self.portal.get_db()
        try:
            db.execute('UPDATE miet_checkout_refunds SET created_at=? WHERE id=?',
                ((datetime.now(timezone.utc)-timedelta(days=2)).isoformat(),rid));db.commit()
        finally:db.close()
        with patch.object(self.gateway,'refund') as create,self.assertRaisesRegex(ValueError,'Wiederholungsfensters'):
            self.ledger.process(rid)
        create.assert_not_called()

    def test_incomplete_foreign_or_multiple_provider_refunds_block_first_create(self):
        paid=self.pay(self.hold());self.proper_return(paid)
        rid=self.ledger.deposit(paid['id'],'Qualified return')
        own={'id':'re_synthetic','object':'refund','payment_intent':paid['payment_intent'],
            'currency':'eur','amount':50000,'status':'pending','metadata':{
                'mos_refund_id':rid,'hold_id':paid['id'],'refund_kind':'deposit_return'}}
        pages=[{'object':'list','has_more':True,'data':[]},
               {'object':'list','has_more':False,'data':[own,own]}]
        for changes in ({'metadata':{}},{'amount':49999},{'amount':True},{'currency':'usd'},
                        {'payment_intent':'pi_other'},{'object':'charge'},{'status':'unknown'}):
            pages.append({'object':'list','has_more':False,'data':[{**own,**changes}]})
        for page in pages:
            with self.subTest(page=page),patch.object(self.gateway,'list_refunds',return_value=page),\
                    patch.object(self.gateway,'refund') as create:
                with self.assertRaisesRegex(ValueError,'Erstattung'):self.ledger.process(rid)
                create.assert_not_called()

    def test_own_pending_refund_is_adopted_without_issuing_a_second_request(self):
        paid=self.pay(self.hold());self.proper_return(paid)
        rid=self.ledger.deposit(paid['id'],'Qualified return')
        own={'id':'re_synthetic_pending','object':'refund','payment_intent':paid['payment_intent'],
            'currency':'eur','amount':50000,'status':'pending','metadata':{
                'mos_refund_id':rid,'hold_id':paid['id'],'refund_kind':'deposit_return'}}
        with patch.object(self.gateway,'list_refunds',return_value={'object':'list','has_more':False,'data':[own]}),\
                patch.object(self.gateway,'refund') as create:
            self.assertEqual(self.ledger.process(rid),'submitted');create.assert_not_called()
        self.assertEqual(self.row('miet_checkout_refunds',paid['id'])['provider_id'],own['id'])

    def test_changed_return_clearance_after_enqueue_blocks_provider_refund(self):
        paid=self.pay(self.hold());self.proper_return(paid)
        rid=self.ledger.deposit(paid['id'],'Qualified return')
        db=self.portal.get_db()
        try:
            db.execute('UPDATE miet_checkout_returns SET no_objection=0,charges_resolved=0 WHERE hold_id=?',(paid['id'],));db.commit()
        finally:db.close()
        with patch.object(self.gateway,'refund') as create,self.assertRaises(ValueError):self.ledger.process(rid)
        create.assert_not_called()

    def test_external_refund_after_enqueue_blocks_a_second_refund_from_the_rent_balance(self):
        paid=self.pay(self.hold(self.raw_quote(20)));self.proper_return(paid)
        rid=self.ledger.deposit(paid['id'],'Qualified return, not processed yet')
        self.gateway.refund(paid['payment_intent'],50000,'synthetic-external-after-enqueue')
        with patch.object(self.gateway,'refund',wraps=self.gateway.refund) as create:
            with self.assertRaisesRegex(ValueError,'Erstattung'):
                self.ledger.process(rid)
            create.assert_not_called()
        self.assertEqual(self.gateway.retrieve_payment(paid['payment_intent'])['latest_charge']['amount_refunded'],50000)

    def test_external_refund_after_list_read_is_detected_by_the_fresh_charge(self):
        paid=self.pay(self.hold(self.raw_quote(20)));self.proper_return(paid)
        rid=self.ledger.deposit(paid['id'],'Qualified return')
        list_refunds=self.gateway.list_refunds;refund=self.gateway.refund
        def race(pi):
            page=list_refunds(pi)
            refund(pi,50000,'synthetic-after-list-read')
            return page
        with patch.object(self.gateway,'list_refunds',side_effect=race),patch.object(self.gateway,'refund') as create:
            with self.assertRaisesRegex(ValueError,'beim Anbieter geprüft'):self.ledger.process(rid)
            create.assert_not_called()
        self.assertEqual(self.gateway.retrieve_payment(paid['payment_intent'])['latest_charge']['amount_refunded'],50000)

    def test_historical_charged_quote_without_new_marker_keeps_its_original_contract_shape(self):
        quote=self.raw_quote(1);quote.pop('long_rental_deposit_policy')
        hold=self.hold(quote)
        self.assertFalse(is_long_rental_paid_deposit(json.loads(hold['payload'])['quote']))
        contract,_,_=signed_payload(json.loads(hold['payload']))
        self.assertNotIn('long_rental_deposit_policy',contract)
        self.assertEqual(contract['deposit_charged_cents'],50000)

    def test_paid_review_refunds_rent_and_deposit_and_closes_without_a_card_row(self):
        hold=self.hold();session=self.service.create_checkout(hold['id']);self.gateway.pay(session['id'])
        body,sig=self.gateway.signed_event(session['id'])
        self.portal.app.config['MOS_SHARED_CHECKOUT_ENABLED']=False
        self.assertIsNone(self.service.handle_signed_event(body,sig,'whsec_synthetic'))
        rid=self.ledger.review_full_refund(hold['id'],'Werkstattprüfung','Synthetischer Prüffall')
        self.assertEqual(self.row('miet_checkout_refunds',hold['id'])['amount_cents'],89000)
        with self.assertRaises(ValueError):self.ledger.close_review_refund(hold['id'])
        self.assertEqual(self.ledger.process(rid),'succeeded')
        self.assertEqual(self.ledger.review_full_refund(hold['id'],'Werkstattprüfung','Synthetische Wiederholung'),rid)
        self.assertEqual(self.ledger.close_review_refund(hold['id']),'released')
        self.assertIsNone(self.service._deposit_record(hold['id']))
        self.portal.app.config['MOS_SHARED_CHECKOUT_ENABLED']=True
        later,later_sig=self.gateway.signed_event(session['id'])
        self.assertIsNone(self.service.handle_signed_event(later,later_sig,'whsec_synthetic'))

    def test_return_refund_rechecks_cancellation_during_provider_read(self):
        paid=self.pay(self.hold());self.proper_return(paid);retrieve=self.gateway.retrieve_payment
        def changed(pi):
            evidence=retrieve(pi);db=self.portal.get_db()
            try:
                db.execute('INSERT INTO miet_checkout_cancellations (id,requested_at,fee_cents,reason) VALUES (?,?,0,?)',
                    (paid['id'],datetime.now(timezone.utc).isoformat(),'Synthetic race'))
                db.commit()
            finally:db.close()
            return evidence
        with patch.object(self.gateway,'retrieve_payment',side_effect=changed):
            with self.assertRaisesRegex(ValueError,'Stornierte'):self.ledger.deposit(paid['id'],'Race')
        self.assertIsNone(self.row('miet_checkout_refunds',paid['id']))

    def test_review_refund_insert_keeps_real_postgres_adapter_natural_key_behavior(self):
        hold=self.hold();session=self.service.create_checkout(hold['id']);self.gateway.pay(session['id'])
        body,sig=self.gateway.signed_event(session['id']);self.portal.app.config['MOS_SHARED_CHECKOUT_ENABLED']=False
        self.service.handle_signed_event(body,sig,'whsec_synthetic')
        statements=[];original=self.portal.get_db
        def traced():
            db=original();db.set_trace_callback(statements.append);return db
        with patch.object(self.portal,'get_db',side_effect=traced):
            self.ledger.review_full_refund(hold['id'],'Werkstattprüfung','Synthetic adapter test')
        sql=next(text for text in statements if text.lstrip().upper().startswith('INSERT INTO MIET_CHECKOUT_REVIEW_REFUNDS'))
        fake=FakeConnection();self.portal.PostgresConnection(fake).execute(sql)
        self.assertNotIn('RETURNING id',fake.statements[0][0])

    def test_gateway_payment_lookup_is_read_only_and_expands_charge(self):
        gateway=object.__new__(StripeTestGateway)
        with patch.object(StripeTestGateway,'retrieve_payment',wraps=StripeTestGateway.retrieve_payment) as lookup:
            from unittest.mock import Mock
            gateway.client=Mock();gateway.client.v1.payment_intents.retrieve.return_value.to_dict.return_value={'id':'pi_synthetic'}
            self.assertEqual(lookup(gateway,'pi_synthetic'),{'id':'pi_synthetic'})
            gateway.client.v1.payment_intents.retrieve.assert_called_once_with('pi_synthetic',{'expand':['latest_charge']})
            gateway.client.v1.payment_intents.create.assert_not_called()

    def test_gateway_refund_metadata_and_bounded_list_use_actual_sdk_adapter(self):
        from unittest.mock import Mock
        gateway=object.__new__(StripeTestGateway);gateway.client=Mock()
        metadata={'mos_refund_id':'deposit-synthetic','hold_id':'synthetic','refund_kind':'deposit_return'}
        gateway.refund('pi_synthetic',50000,'mos-refund-deposit-synthetic',metadata=metadata)
        gateway.client.v1.refunds.create.assert_called_once_with(
            {'payment_intent':'pi_synthetic','amount':50000,'metadata':metadata},
            options={'idempotency_key':'mos-refund-deposit-synthetic'})
        gateway.list_refunds('pi_synthetic')
        gateway.client.v1.refunds.list.assert_called_once_with({'payment_intent':'pi_synthetic','limit':100})


if __name__=='__main__':unittest.main()
