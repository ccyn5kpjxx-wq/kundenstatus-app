"""Synthetic handover, qualified return and refund-monitor checks for long rents."""

from base64 import b64encode
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
import json
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))

from mietwagen_checkout import SharedCheckout, init_schema as checkout_schema
from mos_booking.production import RefundLedger, init_refund_schema
from mos_handover import record as handover_record, init_schema as handover_schema
from mos_return import (record as return_record, init_schema as return_schema,
    record_clearance, record_vehicle_readiness, deposit_release_eligible,
    queue_qualified_charged_deposit_refunds, charged_deposit_refund_status)
import test_mos_handover as handover_fixture
from mos_public_contract import presign_quote
from PIL import Image, ImageDraw


def paid_quote(start, end):
    return {'test_only': False, 'start_slot': start.isoformat(), 'end_slot': end.isoformat(),
        'days': 10, 'daily_cents': 4900, 'rental_cents': 49000, 'amount_cents': 99000,
        'currency': 'eur', 'deposit_method': 'charge_with_rent_refund_after_return',
        'long_rental_deposit_policy': 'paid_at_booking_v1', 'deposit_cents': 50000,
        'deposit_charged_cents': 50000, 'deposit_authorized_cents': 0,
        'checkout_deposit': True, 'included_km': 1500, 'extra_km_cents': 25,
        'vehicle_id':3,'vehicle_name':'Synthetic Long Rental','vehicle_plate':'TEST-LONG','vehicle_vin':'',
        'rules_version':'synthetic-long-rental-v1','terms_text':'Synthetic agreed long-rental terms.',
        'deductible_cents':100000}


class ChargedHandoverTests(unittest.TestCase):
    def setUp(self):
        self.fixture = handover_fixture.HandoverTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        self.service = self.fixture.service
        self.now = self.fixture.now
        self.quote = paid_quote(self.fixture.start,self.fixture.start+timedelta(days=10))
        self.db.execute('ALTER TABLE miet_checkout_holds ADD COLUMN session_id TEXT')
        self.db.execute("UPDATE miet_checkout_holds SET payload=?,session_id='cs_live_synthetic' WHERE id='h1'",
                        (json.dumps({'quote':self.quote}),))
        init_refund_schema(self.db)
        self.db.execute('DELETE FROM miet_checkout_deposit_auths')
        self.contract = {**self.fixture.contract,**self.quote}
        self.save_contract()
        self.evidence = {'id':'cs_live_synthetic','object':'checkout.session','mode':'payment',
            'livemode':True,'client_reference_id':'h1','metadata':{'hold_id':'h1','quote_hash':'quote-digest'},
            'payment_intent':'pi_payment','status':'complete','payment_status':'paid',
            'amount_total':99000,'currency':'eur','deposit_net_cents':50000,'refunded_cents':0,
            'verified_at':self.now.isoformat()}

    def save_contract(self):
        canonical = json.dumps(self.contract,ensure_ascii=False,sort_keys=True,separators=(',',':'))
        self.db.execute('UPDATE miet_checkout_contracts SET contract_json=?,contract_sha256=?',
                        (canonical,sha256(canonical.encode()).hexdigest()))

    def handover(self,evidence=None):
        return handover_record(self.db,self.service,'h1',payment_evidence=evidence or self.evidence,
            operator_name='Synthetic Operator',odometer_km=12345,protocol_ref='SYNTHETIC-HANDOVER',
            receipt_confirmed=True,license_checked=True,fuel_full=True,condition_recorded=True,now=self.now)

    def test_paid_long_rent_needs_no_authorization_and_preserves_first_record(self):
        self.assertEqual(self.handover()['mietvorgang_id'],7)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM miet_checkout_deposit_auths').fetchone()[0],0)
        self.assertEqual(self.handover()['operator_name'],'Synthetic Operator')

    def test_missing_underpaid_refunded_or_stale_provider_evidence_blocks_keys(self):
        for changes in ({'amount_total':49000},{'payment_status':'unpaid'},{'refunded_cents':1},
                        {'deposit_net_cents':49999},{'livemode':False},
                        {'verified_at':(self.now-timedelta(seconds=61)).isoformat()},
                        {'payment_intent':'pi_other'},{'metadata':{'hold_id':'other','quote_hash':'quote-digest'}}):
            with self.subTest(changes=changes),self.assertRaises(ValueError):
                self.handover({**self.evidence,**changes})
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM miet_checkout_handovers').fetchone()[0],0)

    def test_refund_between_provider_check_and_vehicle_lock_blocks_handover(self):
        self.db.execute('''INSERT INTO miet_checkout_refunds
            (id,hold_id,amount_cents,kind,status,reason,created_at)
            VALUES ('synthetic-refund','h1',1,'review','queued','synthetic',?)''',(self.now.isoformat(),))
        with self.assertRaisesRegex(ValueError,'Erstattung'):
            self.handover()

    def test_missing_policy_marker_or_unsigned_contract_policy_is_not_reinterpreted(self):
        self.contract.pop('long_rental_deposit_policy')
        self.save_contract()
        with self.assertRaises(ValueError):
            self.handover()
        self.quote.pop('long_rental_deposit_policy')
        self.db.execute('UPDATE miet_checkout_holds SET payload=?',(json.dumps({'quote':self.quote}),))
        with self.assertRaises(ValueError):
            self.handover()

    def test_unsent_contract_or_cancellation_still_blocks_the_paid_deposit_flow(self):
        self.db.execute("UPDATE miet_checkout_contract_delivery SET status='review'")
        with self.assertRaises(ValueError):
            self.handover()
        self.db.execute("UPDATE miet_checkout_contract_delivery SET status='sent'")
        self.db.execute("INSERT INTO miet_checkout_cancellations (id) VALUES ('h1')")
        with self.assertRaises(ValueError):
            self.handover()


class Portal:
    USE_POSTGRES = False

    def __init__(self,path):
        self.path = path
        self.app = SimpleNamespace(config={})

    def get_db(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def bw_feiertage(year):
        return set()


class ChargedReturnRefundTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='mos-long-return-')
        self.addCleanup(self.tmp.cleanup)
        self.portal = Portal(str(Path(self.tmp.name)/'synthetic.sqlite3'))
        self.start = datetime(2027,5,3,8,tzinfo=timezone.utc)
        self.end = self.start+timedelta(days=10)
        self.now = self.end+timedelta(minutes=5)
        self.quote = paid_quote(self.start,self.end)
        customer={'name':'Synthetic Customer','email':'customer@example.test'}
        image=Image.new('RGBA',(700,180),(255,255,255,0))
        ImageDraw.Draw(image).line([(30,110),(80,40),(130,120),(220,55)],fill=(20,30,40,255),width=5)
        signature=BytesIO()
        image.save(signature,'PNG')
        self.quote=presign_quote(self.quote,customer,'data:image/png;base64,'+b64encode(signature.getvalue()).decode())
        db = self.portal.get_db()
        db.execute('CREATE TABLE mietfahrzeuge (id INTEGER PRIMARY KEY)')
        db.execute('INSERT INTO mietfahrzeuge VALUES (3)')
        db.execute('''CREATE TABLE mietvorgaenge (id INTEGER PRIMARY KEY,mietfahrzeug_id INTEGER,
            status TEXT,rueckgabe_datum TEXT,geaendert_am TEXT)''')
        db.execute("INSERT INTO mietvorgaenge VALUES (7,3,'aktiv','',NULL)")
        checkout_schema(db)
        init_refund_schema(db)
        return_schema(db)
        handover_schema(db)
        db.execute('''INSERT INTO miet_checkout_holds
            (id,request_key,mietfahrzeug_id,start_datum,end_datum,payload,fingerprint,status,
             expires_at,session_id,payment_intent,mietvorgang_id)
            VALUES ('h1','synthetic-request',3,?,?,?,'synthetic-hash','confirmed',0,'cs_live_synthetic','pi_synthetic',7)''',
            (self.start.date().isoformat(),self.end.date().isoformat(),json.dumps({'quote':self.quote,'customer':customer})))
        db.execute('''INSERT INTO miet_checkout_handovers VALUES
            ('h1',7,?,'Synthetic Operator',12000,'SYNTHETIC-HANDOVER',1,1,1,1)''',(self.start.isoformat(),))
        db.commit()
        db.close()
        self.refund_calls = []
        self.provider_refunds = {}
        def refund(payment_intent,amount,key,*,metadata=None):
            self.refund_calls.append((payment_intent,amount,key))
            if key in self.provider_refunds:
                saved=self.provider_refunds[key]
                if (saved['payment_intent']!=payment_intent or saved['amount']!=amount
                        or saved['metadata']!=dict(metadata or {})):
                    raise ValueError('Synthetic refund idempotency parameters changed')
                return dict(saved)
            result={'object':'refund','id':'re_synthetic','payment_intent':payment_intent,
                    'amount':amount,'currency':'eur','status':'succeeded','metadata':dict(metadata or {})}
            self.provider_refunds[key]=result
            payment['latest_charge']['amount_refunded']+=amount
            return dict(result)
        def list_refunds(payment_intent):
            return {'object':'list','has_more':False,'data':[
                dict(row) for row in self.provider_refunds.values() if row['payment_intent']==payment_intent]}
        def retrieve_refund(refund_id):
            return dict(next(row for row in self.provider_refunds.values() if row['id']==refund_id))
        self.service = SharedCheckout.__new__(SharedCheckout)
        self.service.p = self.portal
        checkout={'id':'cs_live_synthetic','object':'checkout.session','mode':'payment','livemode':True,
            'status':'complete','payment_status':'paid','client_reference_id':'h1',
            'metadata':{'hold_id':'h1','quote_hash':'synthetic-hash'},'payment_intent':'pi_synthetic',
            'amount_total':99000,'currency':'eur'}
        payment={'id':'pi_synthetic','object':'payment_intent','livemode':True,'status':'succeeded',
            'amount':99000,'amount_received':99000,'amount_capturable':0,'currency':'eur',
            'latest_charge':{'id':'ch_synthetic','object':'charge','payment_intent':'pi_synthetic',
                'currency':'eur','amount':99000,'paid':True,'captured':True,'disputed':False,'amount_refunded':0}}
        self.service.gateway = SimpleNamespace(refund=refund,livemode=True,
            retrieve=lambda _session:checkout,retrieve_payment=lambda _intent:payment,
            list_refunds=list_refunds,retrieve_refund=retrieve_refund)
        self.ledger = RefundLedger(self.service)

    def record_return(self,proper=True):
        db = self.portal.get_db()
        try:
            row = return_record(db,'h1',returned_at=self.end,operator_name='Synthetic Operator',
                odometer_km=12010,fuel_full=proper,condition_recorded=True,damage_free=proper,
                charges_resolved=proper,no_objection=proper,protocol_ref='SYNTHETIC-RETURN',
                note='' if proper else 'Tank und Schaden pruefen',now=self.now)
            db.commit()
            return row
        finally:
            db.close()

    def test_proper_return_queues_exactly_one_500eur_refund_and_reconciles_it(self):
        self.record_return()
        self.assertEqual(queue_qualified_charged_deposit_refunds(self.portal,self.ledger)['queued'],1)
        self.assertEqual(queue_qualified_charged_deposit_refunds(self.portal,self.ledger)['queued'],1)
        db = self.portal.get_db()
        self.assertEqual(db.execute('SELECT COUNT(*) FROM miet_checkout_refunds').fetchone()[0],1)
        self.assertEqual(db.execute('SELECT amount_cents FROM miet_checkout_refunds').fetchone()[0],50000)
        db.close()
        self.assertEqual(self.ledger.process('deposit-h1'),'succeeded')
        self.assertEqual(self.ledger.process('deposit-h1'),'succeeded')
        self.assertEqual(self.refund_calls,[('pi_synthetic',50000,'mos-refund-deposit-h1')])
        self.assertEqual(charged_deposit_refund_status(self.portal,now=self.now)['pending'],0)

    def test_disputed_return_requires_real_clearance_not_only_vehicle_readiness(self):
        self.record_return(proper=False)
        db = self.portal.get_db()
        record_vehicle_readiness(db,'h1',operator_name='Synthetic Operator',evidence_ref='SYNTHETIC-READY',
            note='Fahrzeug technisch freigegeben',fuel_ready=True,damage_ready=True,cleaned=True,
            safe_to_rent=True,now=self.now+timedelta(minutes=1))
        db.commit()
        self.assertFalse(deposit_release_eligible(db,'h1',7))
        db.close()
        self.assertEqual(queue_qualified_charged_deposit_refunds(self.portal,self.ledger)['queued'],0)
        with self.assertRaises(ValueError):
            self.ledger.deposit('h1','synthetic')
        db = self.portal.get_db()
        record_clearance(db,'h1',operator_name='Synthetic Operator',evidence_ref='SYNTHETIC-CLEAR',
            note='Tank und Schaden mit Kunde vollstaendig geklaert',fuel_resolved=True,damage_resolved=True,
            time_km_resolved=True,now=self.now+timedelta(minutes=2))
        db.commit()
        db.close()
        self.assertEqual(queue_qualified_charged_deposit_refunds(self.portal,self.ledger)['queued'],1)

    def test_legacy_charged_quotes_are_not_auto_refunded_by_new_policy(self):
        self.record_return()
        quote = dict(self.quote)
        quote.pop('long_rental_deposit_policy')
        db = self.portal.get_db()
        db.execute('UPDATE miet_checkout_holds SET payload=?',(json.dumps({'quote':quote}),))
        db.commit()
        db.close()
        self.assertEqual(queue_qualified_charged_deposit_refunds(self.portal,self.ledger)['queued'],0)
        self.assertEqual(charged_deposit_refund_status(self.portal,now=self.now)['pending'],0)

    def test_missing_failed_or_uncertain_refund_is_visible_and_not_replaced(self):
        self.record_return()
        self.assertEqual(charged_deposit_refund_status(self.portal,now=self.now)['pending'],1)
        queue_qualified_charged_deposit_refunds(self.portal,self.ledger)
        db = self.portal.get_db()
        db.execute("UPDATE miet_checkout_refunds SET status='submitted',provider_id='re_synthetic'")
        db.commit()
        db.close()
        state = charged_deposit_refund_status(self.portal,now=self.now+timedelta(days=5))
        self.assertEqual((state['pending'],state['overdue']),(1,1))
        db = self.portal.get_db()
        db.execute("UPDATE miet_checkout_refunds SET status='failed'")
        db.commit()
        db.close()
        state = charged_deposit_refund_status(self.portal,now=self.now)
        self.assertEqual(state['failed'],1)
        queue_qualified_charged_deposit_refunds(self.portal,self.ledger)
        self.assertEqual(self.ledger.process('deposit-h1'),'failed')
        self.assertEqual(self.refund_calls,[])

    def test_corrupt_operator_clearance_is_not_qualified(self):
        self.record_return(proper=False)
        db = self.portal.get_db()
        db.execute('''INSERT INTO miet_checkout_return_clearances VALUES
            ('h1',?,'','SYNTHETIC','Keine echte Entscheidung',1,1,1)''',(self.now.isoformat(),))
        db.commit()
        self.assertFalse(deposit_release_eligible(db,'h1',7))
        db.close()
        state = queue_qualified_charged_deposit_refunds(self.portal,self.ledger)
        self.assertEqual((state['queued'],state['errors']),(0,1))


class ChargedRefundCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import test_mos_long_rental_deposit as long_fixture
        cls.long_fixture = long_fixture
        cls.network_patches = []
        def deny(*args,**kwargs):
            raise AssertionError('External network forbidden in long-rental CLI test')
        for name in ('socket.socket.connect','socket.socket.connect_ex','socket.create_connection'):
            item=patch(name,deny)
            item.start()
            cls.network_patches.append(item)
        long_fixture.LongRentalDepositTests.setUpClass()

    @classmethod
    def tearDownClass(cls):
        cls.long_fixture.LongRentalDepositTests.tearDownClass()
        for item in reversed(cls.network_patches):
            item.stop()

    def test_existing_reconciliation_cli_queues_then_processes_returned_long_deposit_once(self):
        fixture=self.long_fixture.LongRentalDepositTests()
        fixture.setUp()
        paid=fixture.pay(fixture.hold())
        fixture.proper_return(paid)
        portal=fixture.portal
        from mos_order_receipt import init_schema as receipt_schema
        db=portal.get_db()
        try:
            receipt_schema(db)
            stamp=datetime.now(timezone.utc).isoformat()
            db.execute('''INSERT INTO miet_checkout_order_receipts
                (hold_id,session_id,payment_intent,recipient,amount_cents,currency,payload_sha256,
                 provider_observed_at,enqueued_at,status,next_attempt_at)
                VALUES (?,?,?,?,?,'eur',?,?,?,'sent',?)''',
                (paid['id'],paid['session_id'],paid['payment_intent'],'customer@example.test',
                 json.loads(paid['payload'])['quote']['amount_cents'],sha256(paid['payload'].encode()).hexdigest(),
                 stamp,stamp,stamp))
            db.commit()
        finally:db.close()
        state={'service':fixture.service,'gateway':fixture.gateway,'ledger':fixture.ledger,
               'cfg':portal.app.config['MOS_PUBLIC_BOOKING']}
        with patch.dict(portal.app.extensions,{'mos_public_booking':state}):
            first=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            self.assertEqual(first.exit_code,0,first.output)
            self.assertIn('bezahlte Kautionsrückzahlungen vorgemerkt: 1',first.output)
            self.assertIn('bezahlte Kautionsrückzahlungen offen: 0',first.output)
            again=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            self.assertEqual(again.exit_code,0,again.output)
            self.assertIn('bezahlte Kautionsrückzahlungen vorgemerkt: 0',again.output)
        db=portal.get_db()
        try:
            rows=db.execute('SELECT amount_cents,status FROM miet_checkout_refunds WHERE hold_id=?',(paid['id'],)).fetchall()
            self.assertEqual(len(rows),1)
            self.assertEqual((rows[0]['amount_cents'],rows[0]['status']),(50000,'succeeded'))
        finally:db.close()
        # A failed/unclear persisted outcome must turn the same cron red and
        # must never create a replacement refund reference.
        db=portal.get_db()
        db.execute("UPDATE miet_checkout_refunds SET status='failed' WHERE hold_id=?",(paid['id'],))
        db.commit()
        db.close()
        with patch.dict(portal.app.extensions,{'mos_public_booking':state}), \
                patch.object(fixture.gateway,'refund',side_effect=AssertionError('No repeated refund')):
            failed=portal.app.test_cli_runner().invoke(args=['mos-booking-reconcile'])
            self.assertNotEqual(failed.exit_code,0)
            self.assertIn('bezahlte Kautionsrückzahlungen offen: 1',failed.output)


if __name__=='__main__':
    unittest.main()
