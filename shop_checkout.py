"""Mixed-cart hosted Checkout orchestration, disabled until explicitly enabled.

No SDK, credentials, network defaults, capture, refund or supplier operation.
The injected gateway must expose mode, create_session(params, idempotency_key=),
retrieve_session(id), retrieve_intent(id) and verify_webhook(bytes, signature).
verify_webhook MUST cryptographically verify the original bytes, signature,
tolerance and account before returning a Stripe-shaped event. An optional
find_session(expected_binding) performs a bounded READ-ONLY unique lookup; None
or an ambiguous result never permits another creation POST.

order_lookup(order_id) resolves a trusted server-owned customer/mode privately.
It is not an HTTP lookup and must not trust identity from event metadata.
Only a frozen OrderStore record supplies products, services, EUR retail cents
and return paths. Session completion is a hint, not proof of captured money.
All authorizations require later current procurement/price/deadline validation
before any other component may capture or order goods.
"""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import hmac
import json
import re
from urllib.parse import urlsplit

from shop_order_store import OrderStoreError

INTEGRATION = 'gaertner_shop_v2'
MAX_AMOUNT = 100_000_000
MAX_WEBHOOK = 256 * 1024
SESSION_LIFETIME = 3600  # Stripe requires >=30 min; this is NOT quote validity.
SESSION_EVENTS = {'checkout.session.completed', 'checkout.session.expired',
                  'checkout.session.async_payment_succeeded', 'checkout.session.async_payment_failed'}
INTENT_EVENTS = {'payment_intent.amount_capturable_updated', 'payment_intent.succeeded',
                 'payment_intent.payment_failed', 'payment_intent.canceled'}
SERVICES = {'mounting': 'Montage', 'painting': 'Lackierung', 'installation': 'Einbau',
            'oil_service': 'Ölservice', 'care': 'Aufbereitung'}


class CheckoutError(ValueError):
    """Only fixed public-safe codes, never provider bodies or exception text."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        raise CheckoutError('invalid_checkout_clock') from None


def _text(value, limit):
    return (isinstance(value, str) and 0 < len(value) <= limit
            and not any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in value))


def _amount(value, zero=False):
    return type(value) is int and (0 if zero else 1) <= value <= MAX_AMOUNT


def _origin(value):
    if not _text(value, 300) or '\\' in value or any(c.isspace() for c in value):
        raise CheckoutError('invalid_configuration')
    try:
        url = urlsplit(value)
        if (url.scheme != 'https' or not url.hostname or url.username or url.password
                or url.port not in (None, 443) or url.path not in ('', '/') or url.query or url.fragment
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.\-]*[A-Za-z0-9]', url.hostname)):
            raise ValueError()
        return 'https://' + url.hostname.lower()
    except (TypeError, ValueError):
        raise CheckoutError('invalid_configuration') from None


class ShopCheckout:
    def __init__(self, store, gateway=None, *, mode, public_origin, enabled=False,
                 order_lookup=None, clock=None, review_service=None):
        if not isinstance(mode, str) or mode not in {'test', 'live'} or type(enabled) is not bool:
            raise CheckoutError('invalid_configuration')
        self.store, self.gateway, self.mode = store, gateway, mode
        self.public_origin, self.enabled = _origin(public_origin), enabled
        self.order_lookup = order_lookup
        self.review_service = review_service
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return '<ShopCheckout mode=' + self.mode + ' enabled=' + str(self.enabled) + '>'

    def readiness(self):
        ready = (self.enabled is True and getattr(self.gateway, 'mode', None) == self.mode
                 and callable(self.order_lookup)
                 and all(callable(getattr(self.gateway, name, None)) for name in
                         ('create_session', 'retrieve_session', 'retrieve_intent', 'verify_webhook')))
        return {'ready': ready, 'mode': self.mode, 'capture_method': 'manual', 'payment_methods': ['card']}

    def _active(self):
        if not self.enabled:
            raise CheckoutError('checkout_disabled')
        if not self.readiness()['ready']:
            raise CheckoutError('checkout_unavailable')

    def _now(self):
        return _stamp(self.clock())

    def _store(self, method, *args, **kwargs):
        try:
            return getattr(self.store, method)(*args, **kwargs)
        except OrderStoreError as error:
            raise CheckoutError(error.code) from None

    def _load(self, order_id, customer_id):
        order = self._store('get_order', order_id, customer_id=customer_id, mode=self.mode)
        snapshot = self._store('snapshot_for_order', order_id, customer_id=customer_id, mode=self.mode)
        try:
            value = dict(snapshot)
            supplied = value.pop('quote_hash')
            digest = sha256(_json(value).encode('utf-8')).hexdigest()
            if (not isinstance(supplied, str) or not re.fullmatch(r'[a-f0-9]{64}', supplied)
                    or not hmac.compare_digest(digest, order['quote_hash'])
                    or not hmac.compare_digest(supplied, order['quote_hash'])
                    or value.get('customer_id') != customer_id or value.get('mode') != self.mode
                    or value.get('currency') != 'eur' or value.get('vat_percent') != '19.00'
                    or value.get('ready') is not True or value.get('blocked_reasons') != []
                    or value.get('total_gross_cent') != order['amount_gross_cent']):
                raise ValueError()
        except (KeyError, TypeError, ValueError, UnicodeError):
            raise CheckoutError('order_binding_mismatch') from None
        if value.get('version') == 2:
            from shop_complete_wheels import is_review_snapshot
            try:
                state = self.review_service.readiness()
                if (not is_review_snapshot(value) or state.get('ready') is not True
                        or state.get('mode') != self.mode):
                    raise ValueError()
            except Exception:
                raise CheckoutError('order_review_unavailable') from None
        return order, snapshot

    @staticmethod
    def _line_items(snapshot, amount):
        result, total = [], 0

        def append(name, cents, quantity=1):
            nonlocal total
            if not _text(name, 200) or not _amount(cents, zero=True) or type(quantity) is not int or not 1 <= quantity <= 20:
                raise CheckoutError('order_binding_mismatch')
            total += cents * quantity
            if cents:
                result.append({'quantity': quantity, 'price_data': {'currency': 'eur', 'unit_amount': cents,
                    'tax_behavior': 'inclusive', 'product_data': {'name': name}}})

        try:
            for line in snapshot['lines']:
                append(line['name'], line['unit_gross_cent'], line['quantity'])
                if line['items_gross_cent'] != line['unit_gross_cent'] * line['quantity']:
                    raise ValueError()
                for service in line['services']:
                    code = service['code']
                    label = SERVICES.get(code, 'Erforderliches Zubehör' if isinstance(code, str) and code.startswith('required:') else None)
                    if label is None:
                        raise ValueError()
                    append(label + ' · ' + line['name'][:150], service['gross_cent'])
            for group in snapshot['shipments']:
                append('Versand zur Werkstatt' if group['destination'] == 'workshop' else 'Direktversand zum Kunden', group['gross_cent'])
        except (KeyError, TypeError, ValueError):
            raise CheckoutError('order_binding_mismatch') from None
        if total != amount or not _amount(total) or not 1 <= len(result) <= 100:
            raise CheckoutError('checkout_line_limit_or_total')
        return result

    def _metadata(self, order, attempt):
        return {'integration': INTEGRATION, 'order_id': order['id'], 'quote_hash': order['quote_hash'],
                'payment_attempt': attempt['id'], 'capture_method': 'manual'}

    def _urls(self, order):
        base = self.public_origin + '/shop/orders/' + order['id']
        return base + '/success?session_id={CHECKOUT_SESSION_ID}', base + '/cancel'

    @staticmethod
    def _expiry(attempt):
        return int(_stamp(attempt['created_at']).timestamp()) + SESSION_LIFETIME

    def _params(self, order, snapshot, attempt, items):
        metadata = self._metadata(order, attempt)
        success, cancel = self._urls(order)
        return {'mode': 'payment', 'ui_mode': 'hosted_page', 'locale': 'de', 'payment_method_types': ['card'],
                'client_reference_id': order['id'], 'metadata': metadata,
                'payment_intent_data': {'capture_method': 'manual', 'metadata': dict(metadata)},
                'line_items': items, 'success_url': success, 'cancel_url': cancel,
                'expires_at': self._expiry(attempt), 'automatic_tax': {'enabled': False},
                'allow_promotion_codes': False}

    def _session(self, raw, order, attempt, *, expected_id=None):
        if not isinstance(raw, Mapping):
            raise CheckoutError('payment_binding_mismatch')
        sid = raw.get('id')
        metadata = raw.get('metadata')
        expected_metadata = self._metadata(order, attempt)
        success, cancel = self._urls(order)
        if (not isinstance(sid, str) or not re.fullmatch('cs_' + self.mode + r'_[A-Za-z0-9]{1,200}', sid)
                or expected_id is not None and sid != expected_id or raw.get('object') != 'checkout.session'
                or raw.get('livemode') is not (self.mode == 'live') or raw.get('mode') != 'payment'
                or raw.get('currency') != 'eur' or type(raw.get('amount_total')) is not int
                or raw['amount_total'] != order['amount_gross_cent']
                or raw.get('client_reference_id') != order['id'] or not isinstance(metadata, Mapping)
                or any(metadata.get(key) != value for key, value in expected_metadata.items())
                or not isinstance(raw.get('status'), str) or raw['status'] not in {'open', 'complete', 'expired'}
                or not isinstance(raw.get('payment_status'), str) or raw['payment_status'] not in {'unpaid', 'paid'}
                or type(raw.get('expires_at')) is not int or raw['expires_at'] != self._expiry(attempt)
                or raw.get('success_url') != success or raw.get('cancel_url') != cancel
                or raw.get('payment_link')):
            raise CheckoutError('payment_binding_mismatch')
        intent = raw.get('payment_intent')
        intent_id = intent.get('id') if isinstance(intent, Mapping) else intent
        if (intent_id is not None and (not isinstance(intent_id, str) or not re.fullmatch(r'pi_[A-Za-z0-9]{1,200}', intent_id))
                or raw['status'] == 'complete' and intent_id is None):
            raise CheckoutError('payment_binding_mismatch')
        url = raw.get('url') or None
        if url is not None:
            if not _text(url, 5000) or '\\' in url or any(c.isspace() for c in url):
                raise CheckoutError('checkout_url_rejected')
            try:
                parsed = urlsplit(url)
                if (parsed.scheme != 'https' or parsed.hostname != 'checkout.stripe.com'
                        or parsed.username or parsed.password or parsed.port not in (None, 443)
                        or parsed.path not in ('/c/pay/' + sid, '/g/pay/' + sid, '/pay/' + sid)):
                    raise ValueError()
            except ValueError:
                raise CheckoutError('checkout_url_rejected') from None
        if raw['status'] == 'open' and not url:
            raise CheckoutError('checkout_url_rejected')
        return {'id': sid, 'intent_id': intent_id, 'intent_object': intent if isinstance(intent, Mapping) else None,
                'status': raw['status'], 'payment_status': raw['payment_status'], 'url': url,
                'expires_at': raw['expires_at']}

    def _intent(self, raw, intent_id, order, attempt):
        metadata = raw.get('metadata') if isinstance(raw, Mapping) else None
        if (not isinstance(raw, Mapping) or raw.get('id') != intent_id or raw.get('object') != 'payment_intent'
                or raw.get('livemode') is not (self.mode == 'live') or raw.get('currency') != 'eur'
                or type(raw.get('amount')) is not int or raw['amount'] != order['amount_gross_cent']
                or raw.get('capture_method') != 'manual' or not isinstance(metadata, Mapping)
                or any(metadata.get(key) != value for key, value in self._metadata(order, attempt).items())
                or raw.get('on_behalf_of') or raw.get('transfer_data') or raw.get('application_fee_amount')):
            raise CheckoutError('payment_binding_mismatch')
        state, capturable, received = raw.get('status'), raw.get('amount_capturable'), raw.get('amount_received')
        if (not isinstance(state, str) or state not in {'requires_payment_method', 'requires_confirmation',
                'requires_action', 'processing', 'requires_capture', 'succeeded', 'canceled'}
                or not _amount(capturable, zero=True) or not _amount(received, zero=True)
                or capturable > order['amount_gross_cent'] or received > order['amount_gross_cent']):
            raise CheckoutError('payment_state_unconfirmed')
        kind = None
        if state == 'requires_capture':
            if capturable != order['amount_gross_cent'] or received != 0:
                raise CheckoutError('payment_state_unconfirmed')
            kind = 'authorized'
        elif state == 'succeeded':
            if capturable != 0 or received != order['amount_gross_cent']:
                raise CheckoutError('payment_state_unconfirmed')
            kind = 'paid'
        elif capturable != 0 or received != 0:
            raise CheckoutError('payment_state_unconfirmed')
        elif state == 'canceled':
            kind = 'cancelled'
        elif state == 'requires_payment_method' and isinstance(raw.get('last_payment_error'), Mapping) and raw['last_payment_error']:
            kind = 'failed'
        return {'id': intent_id, 'state': state, 'kind': kind}

    def _read_session(self, order, attempt, *, session_hint=None, intent_hint=None):
        # Record read initiation, not delayed response/commit time. A slower old
        # GET must not supersede a newer observation of this same payment.
        observed_at = self._now().isoformat()
        sid = attempt.get('session_id')
        if session_hint is not None and sid is not None and session_hint != sid:
            raise CheckoutError('payment_binding_mismatch')
        sid = sid or session_hint
        try:
            if sid is not None:
                raw = self.gateway.retrieve_session(sid)
            else:
                finder = getattr(self.gateway, 'find_session', None)
                if not callable(finder):
                    raise CheckoutError('checkout_reconciliation_required')
                raw = finder({'mode': self.mode, 'currency': 'eur', 'amount_total': order['amount_gross_cent'],
                              'metadata': self._metadata(order, attempt), 'idempotency_key': attempt['idempotency_key'],
                              'created_at': attempt['created_at'], 'expires_at': self._expiry(attempt)})
                if not isinstance(raw, Mapping):
                    raise CheckoutError('checkout_reconciliation_required')
            session = self._session(raw, order, attempt, expected_id=sid)
            if intent_hint is not None and intent_hint != session['intent_id']:
                raise CheckoutError('payment_binding_mismatch')
            intent = None
            if session['intent_id'] is not None:
                source = session['intent_object']
                if source is None:
                    source = self.gateway.retrieve_intent(session['intent_id'])
                intent = self._intent(source, session['intent_id'], order, attempt)
        except CheckoutError:
            raise
        except Exception:
            raise CheckoutError('checkout_reconciliation_required') from None
        self._store('bind_payment', order['id'], attempt['id'], customer_id=order['customer_id'], mode=self.mode,
                    session_id=session['id'], intent_id=session['intent_id'],
                    amount_gross_cent=order['amount_gross_cent'], currency='eur')
        kind = intent['kind'] if intent else None
        if session['status'] == 'expired' and intent is None:
            kind = 'cancelled'
        elif intent and intent['state'] == 'requires_payment_method' and order['status'] == 'authorized':
            kind = 'failed'
        return session, intent, kind, observed_at

    def _event(self, order, attempt, session, kind, event_id, occurred_at):
        return {'event_id': event_id, 'order_id': order['id'], 'attempt_id': attempt['id'],
                'mode': self.mode, 'currency': 'eur', 'amount_gross_cent': order['amount_gross_cent'],
                'session_id': session['id'], 'intent_id': session['intent_id'], 'kind': kind,
                'occurred_at': occurred_at}

    def _observe(self, order, attempt, session, kind, observed_at):
        if kind is None:
            return
        latest = self._store('get_order', order['id'], customer_id=order['customer_id'], mode=self.mode)
        target = {'authorized': 'authorized', 'paid': 'paid', 'failed': 'payment_failed', 'cancelled': 'cancelled'}[kind]
        if latest['status'] == target or latest['status'] == 'paid':
            return
        observed = _stamp(observed_at).isoformat()
        material = [order['id'], attempt['id'], session['id'], session['intent_id'], kind, observed]
        event_id = 'obs_' + sha256(_json(material).encode()).hexdigest()
        self._store('apply_verified_event', self._event(order, attempt, session, kind, event_id, observed), verified=True)

    def _result(self, order, session=None, kind=None):
        current = self._store('get_order', order['id'], customer_id=order['customer_id'], mode=self.mode)
        status = current['status']
        can_redirect = (session is not None and session['status'] == 'open'
                        and status in {'draft', 'payment_pending', 'payment_failed'}
                        and kind not in {'authorized', 'paid', 'cancelled'}
                        and session['expires_at'] > int(self._now().timestamp()))
        return {'order_id': current['id'], 'status': status, 'mode': self.mode, 'currency': 'eur',
                'amount_gross_cent': current['amount_gross_cent'], 'session_id': session['id'] if session else None,
                'checkout_url': session['url'] if can_redirect else None,
                'payment_state': {'authorized': 'authorized', 'paid': 'paid', 'payment_failed': 'failed',
                                  'cancelled': 'cancelled'}.get(status, 'pending'),
                'authorization_confirmed': kind == 'authorized' and status == 'authorized',
                'payment_confirmed': status == 'paid', 'capture_triggered': False, 'supplier_order_created': False}

    def status(self, order_id, *, customer_id):
        """Read the retained status only; a browser return is never proof."""
        order = self._store('get_order', order_id, customer_id=customer_id, mode=self.mode)
        result = self._result(order)
        snapshot = self._store('snapshot_for_order', order_id, customer_id=customer_id, mode=self.mode)
        if snapshot.get('version') == 2:
            state = {'required': True, 'state': 'pending', 'expires_at': None}
            try:
                review = self.review_service.status(order_id, customer_id=customer_id)
                if review.get('required') is True and review.get('state') in {'pending', 'approved', 'rejected', 'expired'}:
                    state.update(state=review['state'], expires_at=review.get('expires_at'))
            except Exception:
                pass  # A read failure never promotes an authorization to approval.
            result['technical_review'] = state
        return result

    def start(self, order_id, *, customer_id):
        self._active()
        order, snapshot = self._load(order_id, customer_id)
        if order['status'] in {'paid', 'cancelled'}:
            return self._result(order)
        items = self._line_items(snapshot, order['amount_gross_cent'])
        attempt = self._store('claim_payment', order_id, customer_id=customer_id, mode=self.mode)
        if not attempt['can_create']:
            session, _, kind, observed_at = self._read_session(order, attempt)
            self._observe(order, attempt, session, kind, observed_at)
            return self._result(order, session, kind)
        params = self._params(order, snapshot, attempt, items)
        # A queued database connection or slow preparation must not extend quote validity.
        if not _stamp(snapshot['created_at']) <= self._now() < _stamp(snapshot['expires_at']):
            self._store('mark_payment_unknown', order_id, attempt['id'], customer_id=customer_id, mode=self.mode)
            raise CheckoutError('quote_expired')
        try:
            raw = self.gateway.create_session(params, idempotency_key=attempt['idempotency_key'])
            session = self._session(raw, order, attempt)
            if session['status'] != 'open' or session['payment_status'] != 'unpaid':
                raise CheckoutError('payment_state_unconfirmed')
            if session['intent_id'] is not None:
                source = session['intent_object'] or self.gateway.retrieve_intent(session['intent_id'])
                self._intent(source, session['intent_id'], order, attempt)
            self._store('bind_payment', order_id, attempt['id'], customer_id=customer_id, mode=self.mode,
                        session_id=session['id'], intent_id=session['intent_id'],
                        amount_gross_cent=order['amount_gross_cent'], currency='eur')
        except Exception:
            try:
                self._store('mark_payment_unknown', order_id, attempt['id'], customer_id=customer_id, mode=self.mode)
            except CheckoutError:
                pass
            raise CheckoutError('checkout_reconciliation_required') from None
        return self._result(order, session)

    def reconcile(self, order_id, *, customer_id):
        self._active()
        order, _ = self._load(order_id, customer_id)
        if order['status'] in {'draft', 'paid', 'cancelled'}:
            return self._result(order)
        attempt = self._store('claim_payment', order_id, customer_id=customer_id, mode=self.mode)
        if attempt['can_create']:
            raise CheckoutError('checkout_reconciliation_required')
        session, _, kind, observed_at = self._read_session(order, attempt)
        self._observe(order, attempt, session, kind, observed_at)
        return self._result(order, session, kind)

    def handle_webhook(self, raw_body, signature):
        self._active()
        if not isinstance(raw_body, bytes) or not 0 < len(raw_body) <= MAX_WEBHOOK or not _text(signature, 4096):
            raise CheckoutError('invalid_webhook')
        try:
            event = self.gateway.verify_webhook(raw_body, signature)
        except Exception:
            raise CheckoutError('invalid_webhook') from None
        if (not isinstance(event, Mapping) or event.get('object') != 'event'
                or event.get('livemode') is not (self.mode == 'live') or event.get('account') or event.get('context')
                or not isinstance(event.get('id'), str) or not re.fullmatch(r'evt_[A-Za-z0-9]{1,200}', event['id'])
                or type(event.get('created')) is not int or event['created'] <= 0
                or event['created'] > self._now().timestamp() + 120 or not isinstance(event.get('type'), str)):
            raise CheckoutError('invalid_webhook')
        event_type = event['type']
        if event_type not in SESSION_EVENTS | INTENT_EVENTS:
            return {'received': True, 'ignored': True}
        data = event.get('data')
        obj = data.get('object') if isinstance(data, Mapping) else None
        metadata = obj.get('metadata') if isinstance(obj, Mapping) else None
        if not isinstance(metadata, Mapping) or metadata.get('integration') != INTEGRATION:
            return {'received': True, 'ignored': True}
        order_id = metadata.get('order_id')
        if not isinstance(order_id, str) or not re.fullmatch(r'sho_[a-f0-9]{32}', order_id):
            raise CheckoutError('payment_binding_mismatch')
        try:
            identity = self.order_lookup(order_id)
        except Exception:
            raise CheckoutError('order_unavailable') from None
        if not isinstance(identity, Mapping) or identity.get('mode') != self.mode:
            raise CheckoutError('order_unavailable')
        order, _ = self._load(order_id, identity.get('customer_id'))
        if event['created'] < _stamp(order['created_at']).timestamp() - 120:
            raise CheckoutError('invalid_webhook')
        if order['status'] == 'draft':
            raise CheckoutError('payment_binding_mismatch')
        attempt = self._store('claim_payment', order_id, customer_id=order['customer_id'], mode=self.mode)
        if attempt['can_create'] or any(metadata.get(key) != value for key, value in self._metadata(order, attempt).items()):
            raise CheckoutError('payment_binding_mismatch')
        occurred = datetime.fromtimestamp(event['created'], timezone.utc).isoformat()
        if event_type in SESSION_EVENTS:
            if obj.get('object') != 'checkout.session':
                raise CheckoutError('payment_binding_mismatch')
            signed_session = self._session(obj, order, attempt, expected_id=attempt.get('session_id'))
            expected_state = 'expired' if event_type == 'checkout.session.expired' else 'complete'
            if signed_session['status'] != expected_state:
                raise CheckoutError('payment_state_unconfirmed')
            session, _, kind, observed_at = self._read_session(order, attempt, session_hint=signed_session['id'])
            if (event_type == 'checkout.session.expired' and signed_session['status'] == 'expired'
                    and signed_session['payment_status'] == 'unpaid' and signed_session['intent_id'] is None
                    and session['status'] == 'expired' and session['payment_status'] == 'unpaid'
                    and session['intent_id'] is None and kind not in {'authorized', 'paid'}):
                # Only matching unpaid/no-PI expiry may close the order. An old
                # Session hint must never erase current authorization evidence.
                self._store('apply_verified_event', self._event(order, attempt, signed_session, 'cancelled', event['id'], occurred), verified=True)
        else:
            if obj.get('object') != 'payment_intent' or not isinstance(obj.get('id'), str):
                raise CheckoutError('payment_binding_mismatch')
            signed_intent = self._intent(obj, obj['id'], order, attempt)
            expected_state = {'payment_intent.amount_capturable_updated': 'requires_capture',
                              'payment_intent.succeeded': 'succeeded', 'payment_intent.canceled': 'canceled',
                              'payment_intent.payment_failed': 'requires_payment_method'}[event_type]
            if signed_intent['state'] != expected_state:
                raise CheckoutError('payment_state_unconfirmed')
            session, _, kind, observed_at = self._read_session(order, attempt, intent_hint=signed_intent['id'])
            event_kind = signed_intent['kind']
            if event_kind in {'paid', 'cancelled'} and kind != event_kind:
                raise CheckoutError('payment_state_unconfirmed')
            if event_type == 'payment_intent.payment_failed' and signed_intent['state'] == 'requires_payment_method':
                event_kind = 'failed'
            if event_kind is not None:
                # The raw event ID always retains its immutable event-time semantics.
                self._store('apply_verified_event', self._event(order, attempt, session, event_kind, event['id'], occurred), verified=True)
        # Session completion alone is not paid. Current GET evidence is retained
        # separately, so replay after a state change cannot change an event ID.
        self._observe(order, attempt, session, kind, observed_at)
        latest = self._store('get_order', order_id, customer_id=order['customer_id'], mode=self.mode)
        return {'received': True, 'ignored': False, 'order_id': order_id, 'status': latest['status']}
