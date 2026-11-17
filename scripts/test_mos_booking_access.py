"""Synthetic management-link recovery; no SMTP or provider network requests."""

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import json
import sqlite3
import sys
import tempfile
import unittest
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))

from itsdangerous import URLSafeSerializer
from mos_booking_access import (TOKEN_SALT, AccessExpired, access_url,
    create_access_token, session_access_valid, validate_access_token)


class Portal:
    def __init__(self, path):
        self.path = path
        self.app = SimpleNamespace(secret_key='synthetic-booking-secret-' * 3,
            config={'MOS_PUBLIC_BOOKING': {'origin': 'https://mos.example.test'}})

    def get_db(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db


class BookingAccessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='mos-booking-access-')
        self.addCleanup(self.tmp.cleanup)
        self.portal = Portal(str(Path(self.tmp.name) / 'synthetic.sqlite3'))
        self.end = datetime(2027, 5, 20, 18, tzinfo=timezone.utc)
        self.now = self.end - timedelta(days=100)
        self.hold = {'id': 'synthetic-paid-hold', 'status': 'confirmed',
            'payment_intent': 'pi_synthetic', 'mietvorgang_id': 7,
            'payload': json.dumps({'customer': {'email': 'customer@example.test'},
                'quote': {'test_only': False, 'owner_hash': 'a' * 64,
                          'end_slot': self.end.isoformat()}}, sort_keys=True)}
        db = self.portal.get_db()
        db.execute('''CREATE TABLE miet_checkout_holds (id TEXT PRIMARY KEY,
            status TEXT,payment_intent TEXT,mietvorgang_id INTEGER,payload TEXT)''')
        db.execute('INSERT INTO miet_checkout_holds VALUES (?,?,?,?,?)',
            tuple(self.hold[key] for key in ('id','status','payment_intent','mietvorgang_id','payload')))
        db.commit()
        db.close()

    def update(self, **values):
        self.hold.update(values)
        db = self.portal.get_db()
        for key, value in values.items():
            self.assertIn(key, {'status','payment_intent','mietvorgang_id','payload'})
            db.execute('UPDATE miet_checkout_holds SET '+key+'=? WHERE id=?', (value,self.hold['id']))
        db.commit()
        db.close()

    def token(self):
        return create_access_token(self.portal,self.hold,self.now)

    def test_signed_link_is_hold_snapshot_scope_bound_and_uses_fragment(self):
        url = urlsplit(access_url(self.portal,self.hold,now=self.now))
        self.assertEqual(url.path,'/mieten/buchung-verwalten')
        self.assertEqual(url.query,'')
        access = validate_access_token(self.portal,url.fragment,self.now)
        self.assertEqual(access['scope'],'booking')
        self.assertEqual(access['hold_id'],self.hold['id'])
        self.assertEqual(access['payload_sha256'],sha256(self.hold['payload'].encode()).hexdigest())
        self.assertEqual(access['expires_at'],int((self.end+timedelta(days=30)).timestamp()))
        self.assertNotIn('customer@example.test',json.dumps(access))
        self.assertTrue(session_access_valid(self.hold,access,self.now))
        self.assertFalse(session_access_valid(dict(self.hold,id='different-hold'),access,self.now))

    def test_tamper_wrong_scope_wrong_owner_and_expired_are_rejected(self):
        token = self.token()
        with self.assertRaises(ValueError):
            validate_access_token(self.portal,token[:-5]+'wrong',self.now)
        signer = URLSafeSerializer(self.portal.app.secret_key,salt=TOKEN_SALT,signer_kwargs={'digest_method':sha256})
        original = signer.loads(token)
        for key,value in [('scope','deposit'),('owner_hash','b'*64),('expires_at',original['expires_at']+1)]:
            with self.subTest(key=key),self.assertRaises(ValueError):
                validate_access_token(self.portal,signer.dumps(dict(original,**{key:value})),self.now)
        with self.assertRaises(ValueError):
            validate_access_token(self.portal,token,self.end+timedelta(days=30))
        with self.assertRaises(AccessExpired):
            create_access_token(self.portal,self.hold,self.end+timedelta(days=31))

    def test_return_and_cancellation_do_not_remove_paid_booking_access(self):
        token = self.token()
        # Cancellation/refund state is stored separately, leaving the paid
        # booking snapshot readable. A later released status also stays readable.
        self.update(status='released')
        self.assertEqual(validate_access_token(self.portal,token,self.end+timedelta(days=2))['scope'],'booking')

    def test_unpaid_or_modified_payload_cannot_recover(self):
        token = self.token()
        self.update(payment_intent=None)
        with self.assertRaises(ValueError):
            validate_access_token(self.portal,token,self.now)
        self.update(payment_intent='pi_synthetic',payload=self.hold['payload'].replace('a'*64,'b'*64))
        with self.assertRaises(ValueError):
            validate_access_token(self.portal,token,self.now)

    def test_deposit_token_does_not_authenticate_management_route(self):
        token = URLSafeSerializer(self.portal.app.secret_key,salt='mos-deposit-access-v1').dumps(
            {'hold_id':self.hold['id'],'owner_hash':'a'*64,'scope':'deposit'})
        with self.assertRaises(ValueError):
            validate_access_token(self.portal,token,self.now)


class BookingAccessRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Reuse the existing isolated portal fixture. Its socket guard rejects
        # all external traffic; no operational environment or data is read.
        import test_mos_public_booking as public_tests
        cls.public_tests = public_tests

    def setUp(self):
        self.fixture = self.public_tests.PublicTests()
        self.fixture.setUp()
        self.portal = self.public_tests.portal
        self.client = self.fixture.client
        from mos_public_contract import presign_quote as original_presign
        def live_snapshot(quote,*args,**kwargs):
            return original_presign(dict(quote,test_only=False),*args,**kwargs)
        with patch('mos_public_booking.presign_quote',side_effect=live_snapshot):
            self.hold_id = self.fixture.paid_hold()
        db = self.portal.get_db()
        try:
            self.hold = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?',(self.hold_id,)).fetchone())
        finally:
            db.close()
        self.token = create_access_token(self.portal,self.hold)
        self.prefix = '/mietwagen-test'
        self.status_url = self.prefix+'/status/'+self.hold_id

    def recover(self,client=None):
        client = client or self.client
        landing = client.get(self.prefix+'/buchung-verwalten')
        self.assertEqual(landing.status_code,200)
        with client.session_transaction() as state:
            csrf = state['csrf_token']
        return client.post(self.prefix+'/buchung-verwalten',data={'token':self.token,'csrf_token':csrf})

    def test_expired_browser_session_recovers_full_booking_without_extending_admin_session(self):
        lifetime = self.portal.app.permanent_session_lifetime
        with self.client.session_transaction() as state:
            original_owner = state['mos_public_owner']
            state.clear()  # An expired eight-hour session arrives without its old owner.
        self.assertEqual(self.client.get(self.status_url).status_code,404)
        self.assertEqual(self.recover().status_code,303)
        page = self.client.get(self.status_url)
        self.assertEqual(page.status_code,200)
        text = page.get_data(as_text=True)
        self.assertIn('Buchung stornieren',text)
        self.assertEqual(self.client.get(self.status_url+'/vertrag.pdf').status_code,200)
        self.assertEqual(self.client.get(self.status_url+'/bestaetigung.txt').status_code,200)
        with self.client.session_transaction() as state:
            self.assertNotEqual(state['mos_public_owner'],original_owner)
            self.assertEqual(state['mos_booking_access'][self.hold_id]['scope'],'booking')
            self.assertFalse(state.get('admin',False))
            state.clear()
        self.assertEqual(self.client.get(self.status_url).status_code,404)
        self.assertEqual(self.recover().status_code,303)  # The same email works after another expiry.
        self.assertEqual(self.portal.app.permanent_session_lifetime,lifetime)

    def test_get_has_no_grant_and_post_requires_fresh_csrf(self):
        other = self.portal.app.test_client()
        landing = other.get(self.prefix+'/buchung-verwalten')
        self.assertEqual(landing.status_code,200)
        self.assertNotIn(self.hold_id,landing.get_data(as_text=True))
        self.assertEqual(landing.headers['Cache-Control'],'no-store')
        self.assertEqual(landing.headers['Referrer-Policy'],'no-referrer')
        with other.session_transaction() as state:
            self.assertNotIn('mos_booking_access',state)
        bad = other.post(self.prefix+'/buchung-verwalten',data={'token':self.token})
        self.assertEqual(bad.status_code,400)
        self.assertEqual(other.get(self.status_url).status_code,404)

    def test_management_cannot_read_other_hold_or_admin_and_deposit_scope_stays_limited(self):
        quote,_ = self.fixture.quote(slug='i10')
        checkout = self.fixture.checkout(quote)
        paid = self.fixture.post(checkout.location)
        self.assertEqual(paid.status_code,303)
        other_hold = paid.location.rsplit('/',1)[1]
        fresh = self.portal.app.test_client()
        self.assertEqual(self.recover(fresh).status_code,303)
        self.assertEqual(fresh.get(self.prefix+'/status/'+other_hold).status_code,404)
        self.assertEqual(fresh.get(self.prefix+'/status/'+other_hold+'/vertrag.pdf').status_code,404)
        self.assertNotEqual(fresh.get(self.prefix+'/admin').status_code,200)
        self.assertNotEqual(fresh.get('/admin/cockpit').status_code,200)
        deposit_only = self.portal.app.test_client()
        deposit_only.get(self.prefix+'/buchung-oeffnen')
        with deposit_only.session_transaction() as state:
            state['mos_deposit_access']={self.hold_id:{
                'scope':'deposit','owner_hash':json.loads(self.hold['payload'])['quote']['owner_hash'],
                'payload_sha256':sha256(self.hold['payload'].encode()).hexdigest(),
                'expires_at':int((datetime.now(timezone.utc)+timedelta(hours=1)).timestamp())}}
        self.assertEqual(deposit_only.get(self.status_url).status_code,200)
        self.assertEqual(deposit_only.get(self.status_url+'/vertrag.pdf').status_code,404)
        self.assertEqual(deposit_only.get(self.status_url+'/bestaetigung.txt').status_code,404)
        denied = self.fixture.post(self.status_url+'/stornieren',{'confirm':'yes'},client=deposit_only)
        self.assertEqual(denied.status_code,404)

    def test_canceled_booking_can_recover_and_see_refund_status(self):
        canceled = self.fixture.post(self.status_url+'/stornieren',{'confirm':'yes'})
        self.assertEqual(canceled.status_code,303)
        fresh = self.portal.app.test_client()
        self.assertEqual(self.recover(fresh).status_code,303)
        page = fresh.get(self.status_url)
        self.assertEqual(page.status_code,200)
        self.assertIn('Buchung storniert',page.get_data(as_text=True))
        self.assertEqual(fresh.get(self.status_url+'/vertrag.pdf').status_code,200)

    def test_unavailable_status_has_generic_recovery_guidance_without_identifying_hold(self):
        fresh = self.portal.app.test_client()
        own = fresh.get(self.status_url)
        missing = fresh.get(self.prefix+'/status/nonexistent-hold')
        self.assertEqual(own.status_code,404)
        self.assertEqual(missing.status_code,404)
        self.assertEqual(own.get_data(),missing.get_data())
        self.assertIn('persönlichen Link aus deiner Vertrags-E-Mail',own.get_data(as_text=True))
        self.assertNotIn(self.hold_id,own.get_data(as_text=True))


if __name__=='__main__':
    unittest.main()
