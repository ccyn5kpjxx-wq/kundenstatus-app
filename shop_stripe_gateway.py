"""Dedicated Stripe transport for the mixed shop, never automatically enabled.

Only session creation (manual card authorization), reads and signature checking
are exposed. There is deliberately no capture, cancellation, refund, supplier
operation or Connect destination. Constructing a client performs no network IO.
The caller must persist and claim its exact server order before create_session.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import math
import re
import time
from urllib.parse import urlsplit


class StripeShopError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _plain(value):
    if not isinstance(value, Mapping) and callable(getattr(value, 'to_dict', None)):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise StripeShopError('invalid_provider_response')
    return dict(value)


def _pattern(value, pattern):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _origin(value):
    try:
        if not isinstance(value, str) or len(value) > 250 or '\\' in value or any(ord(c) < 33 for c in value):
            raise ValueError()
        parsed = urlsplit(value)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.port not in (None, 443) or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
            raise ValueError()
        return value.rstrip('/')
    except (TypeError, ValueError):
        raise StripeShopError('invalid_configuration') from None


class StripeShopGateway:
    """Explicit SHOP_STRIPE_* config; no rental/legacy key fallback.

    SDK timeout is bounded, automatic retries are off. A failed create has an
    UNKNOWN outcome; the durable checkout service must reconcile, never issue
    another POST. Returned provider data is private and still needs exact order
    binding checks in ShopCheckout before any status can change.
    """
    def __init__(self, config, *, client=None, sdk=None, clock=None):
        if not isinstance(config, Mapping) or config.get('SHOP_STRIPE_MODE') not in ('test', 'live'):
            raise StripeShopError('invalid_configuration')
        self.mode = config['SHOP_STRIPE_MODE']
        self.livemode = self.mode == 'live'
        key = config.get('SHOP_STRIPE_SECRET_KEY')
        secret = config.get('SHOP_STRIPE_WEBHOOK_SECRET')
        if (not _pattern(key, rf'(?:rk|sk)_{self.mode}_[A-Za-z0-9_]{{8,256}}')
                or not _pattern(secret, r'whsec_[A-Za-z0-9_]{8,256}')):
            raise StripeShopError('invalid_configuration')
        self.public_origin = _origin(config.get('SHOP_STRIPE_PUBLIC_ORIGIN'))
        self._secret = secret
        self.clock = clock or time.time
        if sdk is None:
            import stripe
            sdk = stripe
        self._sdk = sdk
        self.client = client if client is not None else sdk.StripeClient(
            key, http_client=sdk.RequestsClient(timeout=(5.0, 15.0)), max_network_retries=0)

    def _now(self):
        value = self.clock()
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise StripeShopError('invalid_configuration')
            value = value.astimezone(timezone.utc).timestamp()
        if type(value) not in (int, float) or not math.isfinite(value):
            raise StripeShopError('invalid_configuration')
        return value

    def _return_url(self, value):
        try:
            if not isinstance(value, str) or len(value) > 1000 or '\\' in value or any(ord(c) < 33 for c in value):
                return False
            parsed = urlsplit(value)
            return (parsed.scheme == 'https' and parsed.netloc == urlsplit(self.public_origin).netloc
                    and not parsed.username and not parsed.password
                    and parsed.path.startswith('/shop/') and not parsed.fragment)
        except ValueError:
            return False

    def create_session(self, params, *, idempotency_key):
        # Keep this SDK boundary narrow even if a future caller gains a bug.
        allowed = {'mode', 'ui_mode', 'locale', 'payment_method_types', 'client_reference_id',
                   'metadata', 'payment_intent_data', 'line_items', 'success_url', 'cancel_url',
                   'expires_at', 'billing_address_collection', 'automatic_tax', 'allow_promotion_codes'}
        if (not isinstance(params, Mapping) or set(params) - allowed
                or params.get('mode') != 'payment' or params.get('payment_method_types') != ['card']
                or params.get('ui_mode', 'hosted_page') != 'hosted_page'
                or params.get('automatic_tax', {'enabled': False}) != {'enabled': False}
                or params.get('allow_promotion_codes', False) is not False
                or not _pattern(idempotency_key, r'[A-Za-z0-9_:-]{8,200}')
                or not self._return_url(params.get('success_url'))
                or not self._return_url(params.get('cancel_url'))):
            raise StripeShopError('invalid_checkout_request')
        metadata = params.get('metadata')
        intent = params.get('payment_intent_data')
        if (not isinstance(metadata, Mapping) or metadata.get('integration') != 'gaertner_shop_v2'
                or set(metadata) != {'integration', 'order_id', 'quote_hash', 'payment_attempt', 'capture_method'}
                or not _pattern(metadata.get('order_id'), r'sho_[a-f0-9]{32}')
                or params.get('client_reference_id') != metadata['order_id']
                or not _pattern(metadata.get('quote_hash'), r'[a-f0-9]{64}')
                or not _pattern(metadata.get('payment_attempt'), r'spa_[a-f0-9]{32}')
                or metadata.get('capture_method') != 'manual'
                or not isinstance(intent, Mapping) or set(intent) != {'metadata', 'capture_method'}
                or intent.get('capture_method') != 'manual' or intent.get('metadata') != metadata):
            raise StripeShopError('invalid_checkout_request')
        if (type(params.get('expires_at')) is not int
                or not 1800 <= params['expires_at'] - self._now() <= 86400):
            raise StripeShopError('invalid_checkout_request')
        items = params.get('line_items')
        if not isinstance(items, list) or not 1 <= len(items) <= 100:
            raise StripeShopError('invalid_checkout_request')
        total = 0
        for item in items:
            if not isinstance(item, Mapping) or set(item) != {'quantity', 'price_data'}:
                raise StripeShopError('invalid_checkout_request')
            price = item['price_data']
            if (type(item['quantity']) is not int or not 1 <= item['quantity'] <= 100
                    or not isinstance(price, Mapping)
                    or set(price) - {'currency', 'unit_amount', 'product_data', 'tax_behavior'}
                    or price.get('tax_behavior', 'inclusive') != 'inclusive'
                    or price.get('currency') != 'eur' or type(price.get('unit_amount')) is not int
                    or not 1 <= price['unit_amount'] <= 100_000_000
                    or not isinstance(price.get('product_data'), Mapping)
                    or set(price['product_data']) != {'name'}
                    or not isinstance(price['product_data']['name'], str)
                    or not 1 <= len(price['product_data']['name']) <= 200):
                raise StripeShopError('invalid_checkout_request')
            total += item['quantity'] * price['unit_amount']
        if total > 100_000_000:
            raise StripeShopError('invalid_checkout_request')
        try:
            return _plain(self.client.v1.checkout.sessions.create(dict(params), options={'idempotency_key': idempotency_key}))
        except Exception:
            raise StripeShopError('creation_outcome_unknown') from None

    def retrieve_session(self, session_id):
        if not _pattern(session_id, rf'cs_{self.mode}_[A-Za-z0-9]{{1,200}}'):
            raise StripeShopError('invalid_session_id')
        try:
            return _plain(self.client.v1.checkout.sessions.retrieve(session_id))
        except Exception:
            raise StripeShopError('provider_unavailable') from None

    def retrieve_intent(self, intent_id):
        if not _pattern(intent_id, r'pi_[A-Za-z0-9]{1,200}'):
            raise StripeShopError('invalid_intent_id')
        try:
            return _plain(self.client.v1.payment_intents.retrieve(intent_id))
        except Exception:
            raise StripeShopError('provider_unavailable') from None

    def verify_webhook(self, raw_body, signature):
        if (not isinstance(raw_body, bytes) or not 0 < len(raw_body) <= 262144
                or not isinstance(signature, str) or not 1 <= len(signature) <= 4096):
            raise StripeShopError('invalid_webhook')
        timestamps = [part[2:] for part in signature.split(',') if part.startswith('t=')]
        now = self._now()
        if (len(timestamps) != 1
                or not _pattern(timestamps[0], r'[0-9]{1,12}') or abs(now - int(timestamps[0])) > 300):
            raise StripeShopError('invalid_webhook')
        try:
            event = _plain(self._sdk.Webhook.construct_event(raw_body, signature, self._secret, tolerance=300))
        except Exception:
            raise StripeShopError('invalid_webhook') from None
        if event.get('livemode') is not self.livemode or event.get('account') or event.get('context'):
            raise StripeShopError('invalid_webhook')
        return event
