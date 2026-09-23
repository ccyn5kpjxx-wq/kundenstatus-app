"""Stripe test adapter and explicitly synthetic, persistent offline provider."""
import hashlib
from contextlib import closing
from datetime import datetime, timezone
import hmac
import json
import sqlite3
import time
import uuid
from urllib.parse import urlsplit
import stripe


DEPOSIT_CENTS = 50_000


def _deposit_key(key):
    if not isinstance(key, str) or not key or len(key) > 255 or any(ord(c) < 32 for c in key):
        raise ValueError('Ungültige Idempotenzreferenz für die Kaution.')
    return key


def _deposit_params(params):
    """Only a separate, card-only, uncaptured EUR 500 intent is acceptable."""
    allowed = {'amount', 'currency', 'capture_method', 'payment_method_types',
               'metadata', 'description', 'payment_method_options'}
    if (not isinstance(params, dict) or set(params) - allowed
            or type(params.get('amount')) is not int or params['amount'] != DEPOSIT_CENTS
            or params.get('currency') != 'eur' or params.get('capture_method') != 'manual'
            or params.get('payment_method_types') != ['card']):
        raise ValueError('Kaution muss separat als 500 EUR Kartenautorisierung ohne Einzug angelegt werden.')
    if 'metadata' in params and (not isinstance(params['metadata'], dict)
                                  or not all(isinstance(k, str) and isinstance(v, str)
                                             for k, v in params['metadata'].items())):
        raise ValueError('Ungültige Kautionsreferenz.')
    if 'description' in params and not isinstance(params['description'], str):
        raise ValueError('Ungültige Kautionsbeschreibung.')
    if 'payment_method_options' in params:
        options = params['payment_method_options']
        if (not isinstance(options, dict) or set(options) != {'card'}
                or not isinstance(options['card'], dict)
                or set(options['card']) != {'request_extended_authorization'}
                or options['card']['request_extended_authorization'] != 'if_available'):
            raise ValueError('Nicht freigegebene Kautions-Kartenoption.')
    # Extended authorizations require a separately eligible Stripe account.
    # The default manual-capture hold uses the ordinary card window. Even when
    # the caller explicitly opts into an extension, coverage is determined
    # solely from the authorized charge's actual capture_before timestamp.
    return dict(params)


def validate_deposit_intent(data, livemode, expected_id=None):
    """Fail closed on a Stripe response that is not the requested card hold.

    `capture_before` is copied from the expanded charge, never inferred from
    an assumed seven-day window. It is an aware UTC ISO timestamp or None.
    """
    if (not isinstance(data, dict) or data.get('object') != 'payment_intent'
            or not isinstance(data.get('id'), str) or not data['id'].startswith('pi_')
            or (expected_id is not None and data['id'] != expected_id)
            or data.get('livemode') is not livemode
            or data.get('currency') != 'eur'
            or type(data.get('amount')) is not int or data['amount'] != DEPOSIT_CENTS
            or data.get('capture_method') != 'manual'
            or data.get('payment_method_types') != ['card']
            or type(data.get('amount_capturable')) is not int
            or not 0 <= data['amount_capturable'] <= DEPOSIT_CENTS
            or type(data.get('amount_received')) is not int
            or not 0 <= data['amount_received'] <= DEPOSIT_CENTS):
        raise ValueError('Stripe-Kautionsantwort passt nicht zur Kreditkartenautorisierung.')
    status = data.get('status')
    if status not in {'requires_payment_method', 'requires_confirmation', 'requires_action',
                      'processing', 'requires_capture', 'canceled', 'succeeded'}:
        raise ValueError('Unbekannter Kautionsstatus.')
    if (status == 'succeeded' and data['amount_received'] == 0
            or status != 'succeeded' and data['amount_received'] != 0):
        raise ValueError('Widersprüchlicher Kautionseinzug.')
    result = dict(data)
    result['capture_before'] = None
    result['card_funding'] = None
    result['credit_eligible'] = False
    if status == 'requires_capture':
        charge = data.get('latest_charge')
        card = (charge.get('payment_method_details') or {}).get('card') if isinstance(charge, dict) else None
        deadline = card.get('capture_before') if isinstance(card, dict) else None
        if (data['amount_capturable'] != DEPOSIT_CENTS or not isinstance(charge, dict)
                or charge.get('object') != 'charge'
                or not isinstance(charge.get('id'), str) or not charge['id'].startswith('ch_')
                or charge.get('payment_intent') != data['id']
                or charge.get('currency') != 'eur' or charge.get('amount') != DEPOSIT_CENTS
                or charge.get('captured') is not False
                or type(deadline) is not int or deadline <= 0):
            raise ValueError('Kautionssperre ohne verifizierbare Kartenfrist.')
        try:
            result['capture_before'] = datetime.fromtimestamp(deadline, timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError('Ungültige Frist der Kautionssperre.') from exc
        # Card-only payment methods include debit and prepaid. Preserve those
        # authorized states so the caller can safely cancel the hold before any
        # rent is charged, but never mark them as acceptable credit cards.
        funding = card.get('funding')
        result['card_funding'] = funding if funding in {'credit', 'debit', 'prepaid'} else 'unknown'
        result['credit_eligible'] = result['card_funding'] == 'credit'
    elif data['amount_capturable'] != 0:
        raise ValueError('Widersprüchlicher Betrag der Kautionssperre.')
    return result


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

    def create_deposit_intent(self, params, key):
        request = _deposit_params(params)
        request['expand'] = ['latest_charge']
        result = self.client.v1.payment_intents.create(
            request, options={'idempotency_key': _deposit_key(key)}).to_dict()
        return validate_deposit_intent(result, self.livemode)

    def retrieve_deposit_intent(self, intent_id):
        result = self.client.v1.payment_intents.retrieve(
            intent_id, {'expand': ['latest_charge']}).to_dict()
        return validate_deposit_intent(result, self.livemode, intent_id)

    def cancel_deposit_intent(self, intent_id, key):
        result = self.client.v1.payment_intents.cancel(
            intent_id, options={'idempotency_key': _deposit_key(key)}).to_dict()
        checked = validate_deposit_intent(result, self.livemode, intent_id)
        if checked['status'] != 'canceled':
            raise ValueError('Kautionssperre wurde nicht freigegeben.')
        return checked

    def capture_deposit_intent(self, intent_id, amount_cents, key):
        if type(amount_cents) is not int or not 0 < amount_cents <= DEPOSIT_CENTS:
            raise ValueError('Ungültiger Kautionseinzug.')
        result = self.client.v1.payment_intents.capture(
            intent_id, {'amount_to_capture': amount_cents},
            options={'idempotency_key': _deposit_key(key)}).to_dict()
        checked = validate_deposit_intent(result, self.livemode, intent_id)
        if (checked['status'] != 'succeeded' or checked.get('amount_received') != amount_cents):
            raise ValueError('Kautionseinzug nicht bestätigt.')
        return checked


class OfflineGateway:
    livemode = False
    """No network. The session database contains only synthetic test objects."""
    def __init__(self, path, base_url, secret, clock=time.time):
        self.path, self.base_url, self.secret, self.clock = str(path), base_url, secret, clock
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE IF NOT EXISTS demo_sessions (key TEXT PRIMARY KEY, id TEXT UNIQUE, params TEXT, data TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS demo_deposit_intents (key TEXT PRIMARY KEY, id TEXT UNIQUE, params TEXT, data TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS demo_deposit_operations (key TEXT PRIMARY KEY, intent_id TEXT, action TEXT, amount INTEGER, data TEXT)')

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

    def create_deposit_intent(self, params, key):
        request = _deposit_params(params)
        key = _deposit_key(key)
        encoded = json.dumps(request, sort_keys=True)
        with closing(sqlite3.connect(self.path, timeout=15)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT params,data FROM demo_deposit_intents WHERE key=?', (key,)).fetchone()
            if old:
                if old[0] != encoded:
                    raise ValueError('Idempotenzparameter der Kaution verändert.')
                return validate_deposit_intent(json.loads(old[1]), False)
            intent_id = 'pi_offline_' + uuid.uuid4().hex
            data = {'id': intent_id, 'object': 'payment_intent', 'livemode': False,
                    'currency': 'eur', 'amount': DEPOSIT_CENTS, 'amount_capturable': 0,
                    'amount_received': 0, 'capture_method': 'manual',
                    'payment_method_types': ['card'], 'status': 'requires_payment_method',
                    'client_secret': intent_id + '_secret_synthetic_not_a_card',
                    'latest_charge': None, 'metadata': request.get('metadata', {})}
            db.execute('INSERT INTO demo_deposit_intents VALUES (?,?,?,?)',
                       (key, intent_id, encoded, json.dumps(data)))
            return validate_deposit_intent(data, False)

    def _deposit_operation(self, intent_id, action, amount, key):
        key = _deposit_key(key)
        with closing(sqlite3.connect(self.path, timeout=15)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT intent_id,action,amount,data FROM demo_deposit_operations WHERE key=?', (key,)).fetchone()
            if old:
                if old[:3] != (intent_id, action, amount):
                    raise ValueError('Idempotenzreferenz der Kaution verändert.')
                return validate_deposit_intent(json.loads(old[3]), False, intent_id)
            row = db.execute('SELECT data FROM demo_deposit_intents WHERE id=?', (intent_id,)).fetchone()
            if not row:
                raise ValueError('Test-Kautionsautorisierung fehlt.')
            data = json.loads(row[0])
            if action == 'cancel':
                if data['status'] not in {'requires_payment_method', 'requires_confirmation',
                                          'requires_action', 'requires_capture', 'canceled'}:
                    raise ValueError('Kautionssperre kann nicht mehr freigegeben werden.')
                data.update(status='canceled', amount_capturable=0)
            elif action == 'capture':
                if data['status'] != 'requires_capture':
                    raise ValueError('Keine einziehbare Kautionssperre.')
                deadline = data['latest_charge']['payment_method_details']['card']['capture_before']
                if self.clock() >= deadline:
                    raise ValueError('Kautionssperre ist abgelaufen.')
                data.update(status='succeeded', amount_capturable=0, amount_received=amount)
                data['latest_charge'].update(captured=True, amount_captured=amount)
            else:
                raise ValueError('Unbekannte Kautionsaktion.')
            checked = validate_deposit_intent(data, False, intent_id)
            db.execute('UPDATE demo_deposit_intents SET data=? WHERE id=?', (json.dumps(data), intent_id))
            db.execute('INSERT INTO demo_deposit_operations VALUES (?,?,?,?,?)',
                       (key, intent_id, action, amount, json.dumps(data)))
            return checked

    def retrieve_deposit_intent(self, intent_id):
        with closing(sqlite3.connect(self.path, timeout=15)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT data FROM demo_deposit_intents WHERE id=?', (intent_id,)).fetchone()
            if not row:
                raise ValueError('Test-Kautionsautorisierung fehlt.')
            data = json.loads(row[0])
            if data['status'] == 'requires_capture':
                deadline = data['latest_charge']['payment_method_details']['card']['capture_before']
                if self.clock() >= deadline:
                    data.update(status='canceled', amount_capturable=0)
                    db.execute('UPDATE demo_deposit_intents SET data=? WHERE id=?', (json.dumps(data), intent_id))
            return validate_deposit_intent(data, False, intent_id)

    def cancel_deposit_intent(self, intent_id, key):
        return self._deposit_operation(intent_id, 'cancel', 0, key)

    def capture_deposit_intent(self, intent_id, amount_cents, key):
        if type(amount_cents) is not int or not 0 < amount_cents <= DEPOSIT_CENTS:
            raise ValueError('Ungültiger Kautionseinzug.')
        return self._deposit_operation(intent_id, 'capture', amount_cents, key)

    def authorize_deposit_intent(self, intent_id, valid_for_seconds=7*24*3600,
                                 funding='credit'):
        """Synthetic card confirmation for offline tests; never calls Stripe."""
        if (type(valid_for_seconds) is not int or valid_for_seconds <= 0
                or funding not in {'credit', 'debit', 'prepaid', 'unknown'}):
            raise ValueError('Ungültige Test-Autorisierungsdauer.')
        with closing(sqlite3.connect(self.path, timeout=15)) as db, db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT data FROM demo_deposit_intents WHERE id=?', (intent_id,)).fetchone()
            if not row:
                raise ValueError('Test-Kautionsautorisierung fehlt.')
            data = json.loads(row[0])
            if data['status'] != 'requires_payment_method':
                raise ValueError('Test-Kaution ist nicht mehr bestätigbar.')
            data.update(status='requires_capture', amount_capturable=DEPOSIT_CENTS,
                        latest_charge={'id': 'ch_offline_' + uuid.uuid4().hex,
                                       'object': 'charge', 'payment_intent': intent_id,
                                       'currency': 'eur', 'amount': DEPOSIT_CENTS,
                                       'captured': False,
                                       'payment_method_details': {'card': {
                                           'funding': funding,
                                           'capture_before': int(self.clock() + valid_for_seconds)}}})
            checked = validate_deposit_intent(data, False, intent_id)
            db.execute('UPDATE demo_deposit_intents SET data=? WHERE id=?', (json.dumps(data), intent_id))
            return checked


def verified_event(body, signature, secret):
    # Use the official SDK against raw bytes; altered/stale bodies fail verification.
    return stripe.Webhook.construct_event(body, signature, secret, tolerance=300).to_dict()
