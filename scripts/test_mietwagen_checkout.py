"""Real portal functions/routes with synthetic records in an isolated database."""
from concurrent.futures import ThreadPoolExecutor
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import uuid
from PIL import Image, ImageDraw

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP = tempfile.TemporaryDirectory(prefix='mos-shared-inventory-')
os.environ.update({'RENDER':'isolated-test','DATABASE_URL':'','REQUIRE_POSTGRES_ON_RENDER':'0',
    'DATA_DIR':TEMP.name,'SQLITE_DB_PATH':str(Path(TEMP.name)/'portal.db'),
    'UPLOAD_DIR':str(Path(TEMP.name)/'uploads'),'FLASK_SECRET_KEY':'test-only-secret',
    'BACKUP_DIR':str(Path(TEMP.name)/'backups'),'DELETED_UPLOAD_DIR':str(Path(TEMP.name)/'deleted'),
    'AUTO_BACKUP_ENABLED':'0','AUTO_CHANGE_BACKUP_ENABLED':'0','LEXWARE_API_KEY':'',
    'GOOGLE_DOC_AI_SERVICE_ACCOUNT_FILE':'','GOOGLE_DOC_AI_PROJECT_ID':'',
    'WHATSAPP_ACCESS_TOKEN':'','WHATSAPP_WORKSHOP_NUMBERS':'','MAIL_IMAP_PASS':'','MAIL_SMTP_PASS':'',
    'ADMIN_PASS':'test-only','OPENAI_API_KEY':'','GOOGLE_APPLICATION_CREDENTIALS':'',
    'SCHADEN_IMAP_PASS':'','SCHADEN_SMTP_PASS':'','SMTP_PASSWORD':''})

def no_network(*args, **kwargs):
    raise AssertionError('No external network in inventory tests')

patch('socket.socket.connect', no_network).start()
patch('socket.socket.connect_ex', no_network).start()
patch('socket.create_connection', no_network).start()
exists = Path.exists
with patch.object(Path, 'exists', lambda p: False if p in (ROOT/'.env',ROOT/'.env.local') else exists(p)):
    import app as portal
from mietwagen_checkout import SharedCheckout
from mos_booking.gateway import OfflineGateway
from mos_public_contract import presign_quote
assert portal.app.config['MOS_SHARED_CHECKOUT_ENABLED'] is False


class InventoryTests(unittest.TestCase):
    def setUp(self):
        portal.app.config.update(TESTING=True, MOS_SHARED_CHECKOUT_ENABLED=True)
        self.secret = 'test-webhook-secret'
        self.gateway = OfflineGateway(Path(TEMP.name)/'provider.db','http://127.0.0.1:5084',self.secret)
        self.s = SharedCheckout(portal,self.gateway)
        db=portal.get_db()
        self.vid=db.execute('''INSERT INTO mietfahrzeuge (kennzeichen,bezeichnung,erstellt_am,geaendert_am)
            VALUES ('TEST ONLY','Synthetisch',?,?)''',(portal.now_str(),portal.now_str())).lastrowid
        db.commit();db.close()
        self.customer={'name':'Test Person','email':'test@example.invalid','telefon':''}
        self.quote={'amount_cents':10000,'currency':'eur','rules_version':'synthetic-test-only'}

    def hold(self, key=None, start='2030-01-10', end='2030-01-12'):
        return self.s.reserve(key or uuid.uuid4().hex,self.vid,start,end,self.customer,self.quote)

    def authorization_hold(self, start_delta=None):
        start = datetime.now(timezone.utc) + (start_delta or timedelta(days=1))
        start = start.replace(second=0, microsecond=0)
        end = start + timedelta(days=1)
        image = Image.new('RGBA', (700, 180), (255, 255, 255, 0))
        draw = ImageDraw.Draw(image)
        draw.line([(40, 110), (90, 40), (125, 125), (180, 60), (240, 110), (320, 50), (380, 105)],
                  fill=(20, 30, 40, 255), width=5)
        output = BytesIO()
        image.save(output, format='PNG')
        signature = 'data:image/png;base64,' + b64encode(output.getvalue()).decode('ascii')
        quote = {'amount_cents': 7800, 'rental_cents': 7800, 'deposit_charged_cents': 0,
                 'deposit_authorized_cents': 50000, 'deposit_cents': 50000,
                 'deposit_method': 'card_authorization_at_booking', 'currency': 'eur',
                 'rules_version': 'synthetic-card-auth-v1', 'vehicle_id': self.vid,
                 'vehicle_name': 'Synthetischer Testwagen', 'vehicle_plate': 'TEST ONLY',
                 'vehicle_vin': '', 'start_slot': start.isoformat(), 'end_slot': end.isoformat(),
                 'days': 1, 'daily_cents': 7800, 'deductible_cents': 100000,
                 'included_km': 100, 'extra_km_cents': 30, 'terms_text': 'Nur synthetischer Test.',
                 'test_only': True}
        signed = presign_quote(quote, self.customer, signature)
        return self.s.reserve(uuid.uuid4().hex, self.vid, start.date().isoformat(),
                              end.date().isoformat(), self.customer, signed)

    def admin(self,start='2030-01-10',end='2030-01-12'):
        return portal.create_mietvorgang(self.vid,kunde_name='Test Admin',kunde_email='admin@example.invalid',start_datum=start,end_datum=end)

    def count(self):
        db=portal.get_db()
        try:return db.execute('SELECT COUNT(*) AS n FROM mietvorgaenge WHERE mietfahrzeug_id=?',(self.vid,)).fetchone()['n']
        finally:db.close()

    def event(self,h,paid=True,event_id=None):
        s=self.s.create_checkout(h['id'])
        if paid and s['status']=='open':self.gateway.pay(s['id'])
        raw,sig=self.gateway.signed_event(s['id'],event_id=event_id)
        return self.s.handle_signed_event(raw,sig,self.secret)

    def client(self):
        c=portal.app.test_client()
        with c.session_transaction() as s:s['admin']=True;s['csrf_token']='test-csrf'
        return c

    def test_disabled_creation_but_existing_holds_still_block_admin(self):
        self.hold()
        portal.app.config['MOS_SHARED_CHECKOUT_ENABLED']=False
        with self.assertRaises(ValueError):self.hold(start='2030-02-01',end='2030-02-02')
        with self.assertRaises(ValueError):self.admin()

    def test_admin_then_hold_and_hold_then_admin(self):
        self.admin()
        with self.assertRaises(ValueError):self.hold()
        h=self.hold(start='2030-02-01',end='2030-02-02')
        with self.assertRaises(ValueError):self.admin('2030-02-02','2030-02-03')
        self.assertEqual(self.s.read(h['id'])['status'],'pending')

    def test_competing_admin_checkout_exactly_one(self):
        barrier=threading.Barrier(8)
        def run(i):
            barrier.wait()
            try:
                return ('ok',self.admin() if i%2 else self.hold()['id'])
            except ValueError:return ('conflict',None)
        with ThreadPoolExecutor(8) as pool:results=list(pool.map(run,range(8)))
        self.assertEqual(sum(r[0]=='ok' for r in results),1)

    def test_same_request_parallel_single_hold(self):
        key=uuid.uuid4().hex
        with ThreadPoolExecutor(5) as pool:holds=list(pool.map(lambda _:self.hold(key),range(5)))
        self.assertEqual(len({h['id'] for h in holds}),1)
        with self.assertRaises(ValueError):self.hold(key,end='2030-01-13')

    def test_signed_success_parallel_exactly_one_rental(self):
        h=self.hold();s=self.s.create_checkout(h['id']);self.gateway.pay(s['id'])
        raw,sig=self.gateway.signed_event(s['id'])
        with ThreadPoolExecutor(5) as pool:ids=list(pool.map(lambda _:self.s.handle_signed_event(raw,sig,self.secret),range(5)))
        self.assertEqual(len(set(ids)),1);self.assertEqual(self.count(),1)
        self.assertEqual(self.s.read(h['id'])['status'],'confirmed')
        with self.assertRaises(ValueError):self.admin()
        portal.storniere_mietvorgang(ids[0], 'Isolierter Test')
        self.s.handle_signed_event(raw,sig,self.secret)
        self.assertEqual(self.count(),1)

    def test_cancel_releases_only_provider_expired(self):
        h=self.hold();self.s.create_checkout(h['id'])
        self.s.cancel_or_reconcile(h['id'],cancel=True)
        self.assertEqual(self.s.read(h['id'])['status'],'released')
        self.admin()

    def test_timeout_and_local_expiry_keep_hold(self):
        h=self.hold()
        with patch.object(self.gateway,'create',side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):self.s.create_checkout(h['id'])
        db=portal.get_db();db.execute('UPDATE miet_checkout_holds SET expires_at=1 WHERE id=?',(h['id'],));db.commit();db.close()
        self.s.cancel_or_reconcile(h['id'],cancel=True)
        with self.assertRaises(ValueError):self.admin()

    def test_paid_during_cancel_waits_for_signed_webhook(self):
        h=self.hold();s=self.s.create_checkout(h['id']);self.gateway.pay(s['id'])
        self.s.cancel_or_reconcile(h['id'],cancel=True)
        self.assertEqual(self.count(),0)
        with self.assertRaises(ValueError):self.admin()
        self.event(h);self.assertEqual(self.count(),1)

    def test_invalid_signature_and_amount_never_fulfil(self):
        h=self.hold();s=self.s.create_checkout(h['id']);self.gateway.pay(s['id'])
        raw,sig=self.gateway.signed_event(s['id'])
        with self.assertRaises(Exception):self.s.handle_signed_event(raw,'bad',self.secret)
        self.gateway.change(s['id'],lambda d:d.update(amount_total=1))
        with self.assertRaises(ValueError):self.s.handle_signed_event(raw,sig,self.secret)
        self.assertEqual(self.count(),0)
        with self.assertRaises(ValueError):self.admin()

    def test_late_payment_after_release_requires_review(self):
        h=self.hold();s=self.s.create_checkout(h['id']);self.s.cancel_or_reconcile(h['id'],True)
        self.admin()
        self.gateway.change(s['id'],lambda d:d.update(status='complete',payment_status='paid',payment_intent='pi_late_test'))
        raw,sig=self.gateway.signed_event(s['id'])
        self.s.handle_signed_event(raw,sig,self.secret)
        self.assertEqual(self.count(),1);self.assertEqual(self.s.read(h['id'])['status'],'review')

    def test_acceptance_route_cannot_bypass_hold(self):
        self.hold();db=portal.get_db()
        cols=db.execute('PRAGMA table_info(mietwagen_anfragen)').fetchall()
        values={'auto_name':'Test','name':'Test','email':'test@example.invalid','telefon':'',
                'start_datum':'10.01.2030','end_datum':'12.01.2030','erstellt_am':portal.now_str(),'geaendert_am':portal.now_str()}
        values={k:v for k,v in values.items() if k in {r['name'] for r in cols}}
        names=','.join(values);marks=','.join('?' for _ in values)
        aid=db.execute(f'INSERT INTO mietwagen_anfragen ({names}) VALUES ({marks})',tuple(values.values())).lastrowid
        db.commit();db.close()
        client=self.client()
        response=client.post(f'/admin/mietanfrage/{aid}/uebernehmen',data={'csrf_token':'test-csrf','mietfahrzeug_id':self.vid})
        self.assertEqual(response.status_code,302);self.assertEqual(self.count(),0)
        with client.session_transaction() as session:
            self.assertTrue(any('belegt' in msg for _,msg in session.get('_flashes',[])))

    def test_date_change_route_cannot_bypass_hold(self):
        rid=self.admin('2030-02-01','2030-02-02');self.hold()
        v=portal.get_mietvorgang(rid);db=portal.get_db()
        vehicle=dict(db.execute('SELECT * FROM mietfahrzeuge WHERE id=?',(self.vid,)).fetchone());db.close()
        client=self.client()
        response=client.post(f'/admin/mietvorgang/{rid}/vertrag/speichern',data={
            'csrf_token':'test-csrf','kunde_name':'Test','kunde_email':'test@example.invalid',
            'start_datum':'10.01.2030','end_datum':'12.01.2030','expected_version':max(1,int(v.get('vertrag_version') or 1)),
            'expected_draft_hash':portal.mietvertrag_entwurf_hash(v,vehicle,portal.mietvertrag_auftrag(v)),
            'expected_text_version':portal.MIETVERTRAG_TEXT_VERSION})
        self.assertEqual(response.status_code,302)
        self.assertEqual(portal.get_mietvorgang(rid)['start_datum'],'01.02.2030')
        with client.session_transaction() as session:
            self.assertTrue(any('belegt' in msg for _,msg in session.get('_flashes',[])))

    def test_provider_timeout_retry_preserves_session(self):
        h=self.hold();original=self.gateway.create
        def fail_after_create(params,key):
            original(params,key)
            raise TimeoutError('response lost')
        with patch.object(self.gateway,'create',side_effect=fail_after_create):
            with self.assertRaises(TimeoutError):self.s.create_checkout(h['id'])
        s=self.s.create_checkout(h['id'])
        self.assertEqual(s['id'],self.s.create_checkout(h['id'])['id'])
        self.event(h);self.assertEqual(self.count(),1)

    def test_webhook_before_creation_response(self):
        h=self.hold();original=self.gateway.create
        def paid_before_response(params,key):
            s=original(params,key);self.gateway.pay(s['id'])
            raw,sig=self.gateway.signed_event(s['id'])
            self.s.handle_signed_event(raw,sig,self.secret)
            return s
        with patch.object(self.gateway,'create',side_effect=paid_before_response):self.s.create_checkout(h['id'])
        self.assertEqual(self.s.read(h['id'])['status'],'confirmed');self.assertEqual(self.count(),1)

    def test_unpaid_event_and_provider_failure_keep_hold(self):
        h=self.hold();s=self.s.create_checkout(h['id'])
        raw,sig=self.gateway.signed_event(s['id'])
        self.s.handle_signed_event(raw,sig,self.secret)
        self.assertEqual(self.count(),0)
        with patch.object(self.gateway,'retrieve',side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):self.s.cancel_or_reconcile(h['id'],True)
        with self.assertRaises(ValueError):self.admin()

    def test_expiry_after_paid_event_cannot_release_rental(self):
        h=self.hold();rid=self.event(h);s=self.s.read(h['id'])
        raw,sig=self.gateway.signed_event(s['session_id'],kind='checkout.session.expired')
        self.assertEqual(self.s.handle_signed_event(raw,sig,self.secret),rid)
        self.assertEqual(self.count(),1)
        with self.assertRaises(ValueError):self.admin()

    def test_competing_date_change_and_checkout(self):
        rid=self.admin('2030-02-01','2030-02-02');v=portal.get_mietvorgang(rid)
        db=portal.get_db();vehicle=dict(db.execute('SELECT * FROM mietfahrzeuge WHERE id=?',(self.vid,)).fetchone());db.close()
        data={'csrf_token':'test-csrf','kunde_name':'Test','kunde_email':'test@example.invalid',
              'start_datum':'10.01.2030','end_datum':'12.01.2030','expected_version':max(1,int(v.get('vertrag_version') or 1)),
              'expected_draft_hash':portal.mietvertrag_entwurf_hash(v,vehicle,portal.mietvertrag_auftrag(v)),
              'expected_text_version':portal.MIETVERTRAG_TEXT_VERSION}
        barrier=threading.Barrier(2)
        def change():
            client=self.client();barrier.wait()
            return client.post(f'/admin/mietvorgang/{rid}/vertrag/speichern',data=data).status_code
        def reserve():
            barrier.wait()
            try:return self.hold()['id']
            except ValueError:return None
        with ThreadPoolExecutor(2) as pool:
            a=pool.submit(change);b=pool.submit(reserve);self.assertEqual(a.result(),302);hold=b.result()
        moved=portal.get_mietvorgang(rid)['start_datum']=='10.01.2030'
        self.assertNotEqual(moved,bool(hold))

    def test_competing_request_acceptance_and_checkout(self):
        db=portal.get_db()
        aid=db.execute('''INSERT INTO mietwagen_anfragen
            (name,email,start_datum,end_datum,erstellt_am) VALUES ('Test','test@example.invalid','10.01.2030','12.01.2030',?)''',
            (portal.now_str(),)).lastrowid
        db.commit();db.close();barrier=threading.Barrier(2)
        def accept():
            client=self.client();barrier.wait()
            return client.post(f'/admin/mietanfrage/{aid}/uebernehmen',data={'csrf_token':'test-csrf','mietfahrzeug_id':self.vid}).status_code
        def reserve():
            barrier.wait()
            try:return self.hold()['id']
            except ValueError:return None
        with ThreadPoolExecutor(2) as pool:
            a=pool.submit(accept);b=pool.submit(reserve);self.assertEqual(a.result(),302);hold=b.result()
        self.assertEqual(self.count()+bool(hold),1)

    def test_incoming_vehicle_cannot_be_reserved(self):
        db=portal.get_db()
        db.execute("UPDATE mietfahrzeuge SET status='bald' WHERE id=?",(self.vid,));db.commit();db.close()
        with self.assertRaises(ValueError):self.hold()

    def test_incoming_after_checkout_requires_review(self):
        h=self.hold();self.s.create_checkout(h['id'])
        db=portal.get_db()
        db.execute("UPDATE mietfahrzeuge SET status='bald' WHERE id=?",(self.vid,));db.commit();db.close()
        self.event(h)
        self.assertEqual(self.count(),0)
        self.assertEqual(self.s.read(h['id'])['status'],'review')

    def test_maintenance_before_payment_requires_review(self):
        h=self.hold();self.s.create_checkout(h['id']);db=portal.get_db()
        db.execute("UPDATE mietfahrzeuge SET status='wartung' WHERE id=?",(self.vid,));db.commit();db.close()
        self.event(h);self.assertEqual(self.count(),0);self.assertEqual(self.s.read(h['id'])['status'],'review')

    def test_same_payment_intent_cannot_create_second_rental(self):
        first=self.hold();self.event(first);pi=self.s.read(first['id'])['payment_intent']
        second=self.hold(start='2030-03-01',end='2030-03-02');session=self.s.create_checkout(second['id'])
        self.gateway.change(session['id'],lambda s:s.update(status='complete',payment_status='paid',payment_intent=pi))
        raw,sig=self.gateway.signed_event(session['id']);self.s.handle_signed_event(raw,sig,self.secret)
        self.assertEqual(self.count(),1);self.assertEqual(self.s.read(second['id'])['status'],'review')

    def test_fulfilment_failure_rolls_back_rental_and_event(self):
        h=self.hold();s=self.s.create_checkout(h['id']);self.gateway.pay(s['id'])
        raw,sig=self.gateway.signed_event(s['id']);db=portal.get_db()
        db.execute("""CREATE TRIGGER fail_confirmation BEFORE UPDATE OF status ON miet_checkout_holds
            WHEN NEW.status='confirmed' BEGIN SELECT RAISE(ABORT, 'test rollback'); END""")
        db.commit();db.close()
        try:
            with self.assertRaises(Exception):self.s.handle_signed_event(raw,sig,self.secret)
            self.assertEqual(self.count(),0)
            db=portal.get_db()
            self.assertEqual(db.execute('SELECT COUNT(*) AS n FROM miet_checkout_events WHERE hold_id=?',(h['id'],)).fetchone()['n'],0)
            db.close()
        finally:
            db=portal.get_db();db.execute('DROP TRIGGER fail_confirmation');db.commit();db.close()
        self.s.handle_signed_event(raw,sig,self.secret);self.assertEqual(self.count(),1)

    def test_authorization_then_rent_only_checkout_then_signed_confirmation(self):
        h = self.authorization_hold()
        with self.assertRaises(ValueError):
            self.s.create_checkout(h['id'])
        intent = self.s.prepare_deposit(h['id'])
        self.assertEqual(intent['amount'], 50000)
        self.assertFalse(intent['ready'])
        with self.assertRaises(ValueError):
            self.s.create_checkout(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        self.assertTrue(self.s.reconcile_deposit(h['id'])['ready'])
        session = self.s.create_checkout(h['id'])
        self.assertEqual(session['amount_total'], 7800)
        self.gateway.pay(session['id'])
        raw, sig = self.gateway.signed_event(session['id'])
        rid = self.s.handle_signed_event(raw, sig, self.secret)
        self.assertTrue(rid)
        self.assertEqual(self.s.read(h['id'])['status'], 'confirmed')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'authorized')

    def test_short_card_hold_cannot_open_rent_checkout(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=24*3600)
        with self.assertRaisesRegex(ValueError, 'reicht nicht'):
            self.s.create_checkout(h['id'])
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertIsNone(self.s.read(h['id'])['session_id'])
        self.s.cancel_or_reconcile(h['id'], cancel=True)
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'released')

    def test_card_hold_expiring_exactly_at_return_plus_24h_is_rejected(self):
        h = self.authorization_hold()
        q = json.loads(h['payload'])['quote']
        minimum_deadline = int((datetime.fromisoformat(q['end_slot']) + timedelta(hours=24)).timestamp())
        now = int(time.time())
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.clock = lambda: now
        self.gateway.authorize_deposit_intent(intent['id'],
                                              valid_for_seconds=minimum_deadline-now)
        with patch.object(self.gateway, 'create') as rent_checkout:
            with self.assertRaisesRegex(ValueError, 'reicht nicht'):
                self.s.create_checkout(h['id'])
            rent_checkout.assert_not_called()
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertIsNone(self.s.read(h['id'])['session_id'])
        self.assertEqual(self.gateway.retrieve_deposit_intent(intent['id'])['status'], 'canceled')

    def test_failed_card_challenge_never_opens_rent_checkout(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        # Offline model of a card challenge followed by an authentication
        # failure. Stripe's browser 3DS challenge itself is not simulated.
        for provider_status in ('requires_action', 'requires_payment_method'):
            with self.subTest(provider_status=provider_status):
                response = {**intent, 'status': provider_status}
                with patch.object(self.gateway, 'retrieve_deposit_intent', return_value=response), \
                        patch.object(self.gateway, 'create') as rent_checkout:
                    self.assertFalse(self.s.reconcile_deposit(h['id'])['ready'])
                    with self.assertRaisesRegex(ValueError, 'noch nicht vollständig'):
                        self.s.create_checkout(h['id'])
                    rent_checkout.assert_not_called()
        self.assertEqual(self.count(), 0)
        self.assertIsNone(self.s.read(h['id'])['session_id'])
        self.s.cancel_or_reconcile(h['id'], cancel=True)
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertEqual(self.gateway.retrieve_deposit_intent(intent['id'])['status'], 'canceled')

    def test_late_duplicate_unpaid_checkout_events_do_not_restore_booking(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        session = self.s.create_checkout(h['id'])
        self.gateway.expire(session['id'])
        first, signature = self.gateway.signed_event(session['id'], kind='checkout.session.expired')
        self.assertIsNone(self.s.handle_signed_event(first, signature, self.secret))
        self.assertIsNone(self.s.handle_signed_event(first, signature, self.secret))
        late, late_signature = self.gateway.signed_event(session['id'], kind='checkout.session.completed')
        self.assertIsNone(self.s.handle_signed_event(late, late_signature, self.secret))
        self.assertEqual(self.count(), 0)
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'released')
        self.assertEqual(self.gateway.retrieve_deposit_intent(intent['id'])['amount_received'], 0)
        with self.assertRaisesRegex(ValueError, 'nicht mehr zahlbar'):
            self.gateway.pay(session['id'])

    def test_card_checkout_creation_timeout_retries_same_session_before_cancel(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        original_create = self.gateway.create
        created = {}

        def lose_checkout_response(params, key):
            created['session'] = original_create(params, key)
            raise TimeoutError('synthetic lost Checkout response')

        with patch.object(self.gateway, 'create', side_effect=lose_checkout_response):
            with self.assertRaises(TimeoutError):
                self.s.create_checkout(h['id'])
        self.assertIsNone(self.s.read(h['id'])['session_id'])
        with self.assertRaisesRegex(ValueError, 'manuell abgeglichen'):
            self.s.cancel_or_reconcile(h['id'], cancel=True)
        self.assertEqual(self.s.read(h['id'])['status'], 'pending')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'authorized')

        retried = self.s.create_checkout(h['id'])
        self.assertEqual(retried['id'], created['session']['id'])
        self.assertEqual(retried['amount_total'], 7800)
        self.s.cancel_or_reconcile(h['id'], cancel=True)
        self.assertEqual(self.gateway.retrieve(retried['id'])['status'], 'expired')
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'released')
        self.assertEqual(self.gateway.retrieve_deposit_intent(intent['id'])['amount_received'], 0)

    def test_paid_checkout_during_cancel_and_out_of_order_events_confirms_once(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        session = self.s.create_checkout(h['id'])
        self.gateway.pay(session['id'])
        self.s.cancel_or_reconcile(h['id'], cancel=True)
        self.assertEqual(self.s.read(h['id'])['status'], 'pending')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'authorized')
        # A stale expiry event cannot override the current paid provider state.
        expired, expired_sig = self.gateway.signed_event(session['id'], kind='checkout.session.expired')
        rental_id = self.s.handle_signed_event(expired, expired_sig, self.secret)
        completed, completed_sig = self.gateway.signed_event(session['id'])
        self.assertEqual(self.s.handle_signed_event(completed, completed_sig, self.secret), rental_id)
        self.assertEqual(self.s.handle_signed_event(expired, expired_sig, self.secret), rental_id)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.s.read(h['id'])['status'], 'confirmed')
        self.assertEqual(self.gateway.retrieve_deposit_intent(intent['id'])['amount_received'], 0)

    def test_debit_card_hold_is_released_without_rent_checkout(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], funding='debit')
        with self.assertRaisesRegex(ValueError, 'Kreditkarte'):
            self.s.create_checkout(h['id'])
        self.assertEqual(self.gateway.retrieve_deposit_intent(intent['id'])['status'], 'canceled')
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertIsNone(self.s.read(h['id'])['session_id'])

    def test_card_confirmation_extends_checkout_window_once(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        db = portal.get_db()
        db.execute('UPDATE miet_checkout_holds SET expires_at=? WHERE id=?', (int(time.time()) + 100, h['id']))
        db.commit();db.close()
        self.assertTrue(self.s.reconcile_deposit(h['id'])['ready'])
        extended = self.s.read(h['id'])['expires_at']
        self.assertGreater(extended, int(time.time()) + 3500)
        self.assertEqual(self.s.create_checkout(h['id'])['amount_total'], 7800)
        self.assertEqual(self.s.read(h['id'])['expires_at'], extended)

    def test_unknown_authorization_outcome_keeps_stock_and_late_retry_is_blocked(self):
        h = self.authorization_hold()
        original = self.gateway.create_deposit_intent
        def lose_response(params, key):
            original(params, key)
            raise TimeoutError('provider response lost')
        with patch.object(self.gateway, 'create_deposit_intent', side_effect=lose_response):
            with self.assertRaises(TimeoutError):
                self.s.prepare_deposit(h['id'])
        with self.assertRaises(ValueError):
            self.s.cancel_or_reconcile(h['id'], cancel=True)
        with self.assertRaises(ValueError):
            self.admin(h['start_datum'], h['end_datum'])
        db = portal.get_db()
        db.execute('UPDATE miet_checkout_deposit_auths SET created_at=1 WHERE hold_id=?', (h['id'],))
        db.commit();db.close()
        with self.assertRaisesRegex(ValueError, 'manuell abgeglichen'):
            self.s.prepare_deposit(h['id'])
        self.assertEqual(self.s.read(h['id'])['status'], 'pending')

    def test_paid_rent_with_released_card_hold_requires_review(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        session = self.s.create_checkout(h['id'])
        self.gateway.cancel_deposit_intent(intent['id'], 'test-external-release-' + h['id'])
        self.gateway.pay(session['id'])
        raw, sig = self.gateway.signed_event(session['id'])
        self.assertIsNone(self.s.handle_signed_event(raw, sig, self.secret))
        self.assertEqual(self.count(), 0)
        self.assertEqual(self.s.read(h['id'])['status'], 'review')

    def test_cancel_authorization_and_unpaid_rent_releases_inventory(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        session = self.s.create_checkout(h['id'])
        self.s.cancel_or_reconcile(h['id'], cancel=True)
        self.assertEqual(self.gateway.retrieve(session['id'])['status'], 'expired')
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'released')
        self.assertEqual(self.s.release_deposit(h['id'], 'repeat'), 'released')

    def test_expired_unpaid_card_hold_is_released_by_reconciliation(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        db = portal.get_db()
        db.execute('UPDATE miet_checkout_holds SET expires_at=1 WHERE id=?', (h['id'],))
        db.commit();db.close()
        self.s.cancel_or_reconcile(h['id'])
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'released')

    def test_unknown_rent_checkout_creation_never_auto_releases_card_or_stock(self):
        h = self.authorization_hold()
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        original = self.gateway.create
        def lose_response(params, key):
            original(params, key)
            raise TimeoutError('checkout response lost')
        with patch.object(self.gateway, 'create', side_effect=lose_response):
            with self.assertRaises(TimeoutError):
                self.s.create_checkout(h['id'])
        db = portal.get_db()
        db.execute('UPDATE miet_checkout_holds SET expires_at=1 WHERE id=?', (h['id'],))
        db.commit();db.close()
        with self.assertRaisesRegex(ValueError, 'manuell abgeglichen'):
            self.s.cancel_or_reconcile(h['id'])
        self.assertEqual(self.s.read(h['id'])['status'], 'pending')
        self.assertEqual(self.s._deposit_record(h['id'])['status'], 'authorized')

    def test_future_near_term_pickup_needs_no_generic_lead_time(self):
        h = self.authorization_hold(start_delta=timedelta(minutes=3))
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        self.assertEqual(self.s.create_checkout(h['id'])['amount_total'], 7800)

    def test_elapsed_pickup_cancels_unused_card_hold_before_checkout(self):
        h = self.authorization_hold(start_delta=timedelta(minutes=3))
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        class AfterPickup(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(hours=1)
        with patch('mietwagen_checkout.datetime', AfterPickup):
            with self.assertRaisesRegex(ValueError, 'Abholtermin'):
                self.s.create_checkout(h['id'])
        self.assertEqual(self.s.read(h['id'])['status'], 'released')
        self.assertEqual(self.gateway.retrieve_deposit_intent(intent['id'])['status'], 'canceled')
        self.assertIsNone(self.s.read(h['id'])['session_id'])

    def test_paid_webhook_after_pickup_needs_review(self):
        h = self.authorization_hold(start_delta=timedelta(minutes=3))
        intent = self.s.prepare_deposit(h['id'])
        self.gateway.authorize_deposit_intent(intent['id'], valid_for_seconds=7*24*3600)
        session = self.s.create_checkout(h['id'])
        self.gateway.pay(session['id'])
        raw, sig = self.gateway.signed_event(session['id'])
        class AfterPickup(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(hours=1)
        with patch('mietwagen_checkout.datetime', AfterPickup):
            self.assertIsNone(self.s.handle_signed_event(raw, sig, self.secret))
        self.assertEqual(self.s.read(h['id'])['status'], 'review')
        self.assertEqual(self.s.read(h['id'])['grund'], 'paid_pickup_passed_review')
        self.assertEqual(self.count(), 0)


if __name__=='__main__':
    unittest.main()
