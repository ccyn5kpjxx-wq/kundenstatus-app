"""Stripe test adapter and explicitly synthetic, persistent offline provider."""
import hashlib
from contextlib import closing
import hmac
import json
import sqlite3
import time
import uuid
from urllib.parse import urlsplit
import stripe


class StripeTestGateway:
    livemode = False
    def __init__(self, key):
        if not key.startswith(('sk_test_', 'rk_test_')):
            raise ValueError('Nur ein Stripe-Testschlüssel ist erlaubt; Livebetrieb ist gesperrt.')
        self.client = stripe.StripeClient(key, max_network_retries=2)

    def create(self, params, key):
        result = self.client.v1.checkout.sessions.create(params, options={'idempotency_key': key}).to_dict()
        target = urlsplit(result.get('url') or '')
        if target.scheme != 'https' or target.hostname != 'checkout.stripe.com' or target.username or target.password:
            raise ValueError('Unerwartete Checkout-Domain.')
        return result

    def retrieve(self, sid):
        return self.client.v1.checkout.sessions.retrieve(sid).to_dict()

    def expire(self, sid):
        return self.client.v1.checkout.sessions.expire(sid).to_dict()

    def refund(self, payment_intent, amount, key):
        return self.client.v1.refunds.create({'payment_intent':payment_intent,'amount':amount},
                    options={'idempotency_key':key}).to_dict()

    def retrieve_refund(self, refund_id):
        return self.client.v1.refunds.retrieve(refund_id).to_dict()


class OfflineGateway:
    livemode = False
    """No network. The session database contains only synthetic test objects."""
    def __init__(self, path, base_url, secret, clock=time.time):
        self.path, self.base_url, self.secret, self.clock = str(path), base_url, secret, clock
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS demo_sessions (key TEXT PRIMARY KEY, id TEXT UNIQUE, params TEXT, data TEXT)')

    def create(self, params, key):
        encoded = json.dumps(params, sort_keys=True)
        with closing(sqlite3.connect(self.path, timeout=15)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT params,data FROM demo_sessions WHERE key=?', (key,)).fetchone()
            if existing:
                if existing[0] != encoded:
                    raise ValueError('Idempotenzparameter verändert.')
                return json.loads(existing[1])
            sid = 'cs_test_offline_' + uuid.uuid4().hex
            data = {'id': sid, 'object': 'checkout.session', 'livemode': False, 'mode': 'payment',
                    'status': 'open', 'payment_status': 'unpaid', 'currency': 'eur',
                    'amount_total': sum(item['quantity']*item['price_data']['unit_amount'] for item in params['line_items']),
                    'metadata': params['metadata'], 'client_reference_id': params['client_reference_id'],
                    'expires_at': params['expires_at'], 'url': self.base_url + '/simulate/' + sid,
                    'payment_intent': None}
            db.execute('INSERT INTO demo_sessions VALUES (?,?,?,?)', (key, sid, encoded, json.dumps(data)))
            return data

    def retrieve(self, sid):
        with closing(sqlite3.connect(self.path)) as db, db:
            row = db.execute('SELECT data FROM demo_sessions WHERE id=?', (sid,)).fetchone()
        if not row:
            raise ValueError('Testsession nicht gefunden.')
        data = json.loads(row[0])
        if data['status'] == 'open' and data['expires_at'] <= self.clock():
            return self.expire(sid)
        return data

    def change(self, sid, transform):
        with closing(sqlite3.connect(self.path, timeout=15)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            data = json.loads(db.execute('SELECT data FROM demo_sessions WHERE id=?', (sid,)).fetchone()[0])
            transform(data)
            db.execute('UPDATE demo_sessions SET data=? WHERE id=?', (json.dumps(data), sid))
            return data

    def expire(self, sid):
        def apply(data):
            if data['status'] == 'open':
                data['status'] = 'expired'
        return self.change(sid, apply)

    def pay(self, sid):
        def apply(data):
            if data['status'] != 'open' or data['expires_at'] <= self.clock():
                raise ValueError('Diese Testsession ist nicht mehr zahlbar.')
            data.update(status='complete', payment_status='paid', payment_intent='pi_offline_' + uuid.uuid4().hex)
        return self.change(sid, apply)

    def signed_event(self, sid, kind='checkout.session.completed', event_id=None):
        body = json.dumps({'object': 'event', 'id': event_id or 'evt_offline_' + uuid.uuid4().hex,
                           'livemode': False, 'type': kind, 'data': {'object': {'id': sid}}}).encode()
        stamp = int(time.time())
        sig = hmac.new(self.secret.encode(), str(stamp).encode() + b'.' + body, hashlib.sha256).hexdigest()
        return body, f't={stamp},v1={sig}'

    def refund(self, payment_intent, amount, key):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS demo_refunds (id TEXT PRIMARY KEY, key TEXT UNIQUE, data TEXT)')
            old=db.execute('SELECT data FROM demo_refunds WHERE key=?',(key,)).fetchone()
            if old:
                result=json.loads(old[0])
                if result['payment_intent']!=payment_intent or result['amount']!=amount:
                    raise ValueError('Test-Erstattungsreferenz verändert.')
                return result
            result={'id':'re_offline_'+uuid.uuid4().hex,'object':'refund','payment_intent':payment_intent,
                    'amount':amount,'currency':'eur','livemode':False,'status':'succeeded'}
            db.execute('INSERT INTO demo_refunds VALUES (?,?,?)',(result['id'],key,json.dumps(result)))
            return result

    def retrieve_refund(self, refund_id):
        with closing(sqlite3.connect(self.path)) as db:
            row=db.execute('SELECT data FROM demo_refunds WHERE id=?',(refund_id,)).fetchone()
        if not row:raise ValueError('Test-Erstattung fehlt.')
        return json.loads(row[0])


def verified_event(body, signature, secret):
    # Use the official SDK against raw bytes; altered/stale bodies fail verification.
    return stripe.Webhook.construct_event(body, signature, secret, tolerance=300).to_dict()
