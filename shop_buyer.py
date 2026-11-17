"""Dedicated, server-held guest buyer sessions and validated German addresses.

No portal/admin identity, account registration, email, provider, order or payment
operation is inherited. Enable explicitly and inject a durable backend. Only a
random opaque bearer token enters the Secure/HttpOnly __Host cookie; contacts,
addresses and CSRF state remain private on the server. Guest ownership does not
claim that an email address or delivery address has been independently verified.

Backend contract: create(key, record)->bool (atomic insert-if-absent), load(key)
->private record|None, compare_and_swap(key, revision, record)->bool and delete(key).
Keys are SHA256(cookie), never raw cookies. SQLiteBuyerBackend is an optional
implementation; its connection factory must point to a deliberately provisioned
SQLite store, never the portal's PostgreSQL connection. No DB opens by default.

HTTP: GET /shop/api/v2/buyer/session, PUT /buyer/address, DELETE /buyer/session.
Mutations require exact configured HTTPS Origin, own cookie and X-CSRF-Token.
The host must enable SHOP_BUYER_ENABLED and explicitly exempt these exact buyer
mutations from any legacy portal CSRF middleware; this module supplies their
separate buyer protections. Do not exempt an entire URL prefix.
"""
from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import hmac
import json
import re
import secrets
import sqlite3
import unicodedata
from urllib.parse import urlsplit
from uuid import uuid4

from flask import Blueprint, Response, current_app, request

COOKIE_NAME = '__Host-shop_buyer_v2'
MAX_BODY = 16384
MAX_RECORD = 65536
TOKEN = re.compile(r'[A-Za-z0-9_-]{43}')
KEY = re.compile(r'[a-f0-9]{64}')
PRINCIPAL = re.compile(r'sg_[a-f0-9]{32}')
RECORD_FIELDS = {'version', 'revision', 'customer_id', 'mode', 'csrf_token',
                 'created_at', 'expires_at', 'context'}


class BuyerError(ValueError):
    """Fixed public-safe code only; never include contacts, cookies or SQL."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _stamp(value):
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(stamp, datetime) or stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError()
        return stamp.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        raise BuyerError('buyer_unavailable') from None


def _origin(value):
    try:
        if not isinstance(value, str) or len(value) > 300 or '\\' in value or any(c.isspace() for c in value):
            raise ValueError()
        parsed = urlsplit(value)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.port not in (None, 443) or parsed.path not in ('', '/') or parsed.query or parsed.fragment
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.\-]*[A-Za-z0-9]', parsed.hostname)):
            raise ValueError()
        return 'https://' + parsed.hostname.lower()
    except (TypeError, ValueError):
        raise BuyerError('invalid_buyer_configuration') from None


def _text(value, maximum, *, minimum=1):
    if (not isinstance(value, str) or len(value) > maximum
            or any(unicodedata.category(c).startswith('C') or c in '\u2028\u2029' for c in value)):
        raise BuyerError('invalid_address')
    result = unicodedata.normalize('NFC', value).strip()
    if not minimum <= len(result) <= maximum or '<' in result or '>' in result:
        raise BuyerError('invalid_address')
    return result


def _address(value):
    fields = {'recipient', 'street', 'postcode', 'city', 'country'}
    if not isinstance(value, dict) or set(value) != fields:
        raise BuyerError('invalid_address')
    result = {'recipient': _text(value['recipient'], 160, minimum=2),
              'street': _text(value['street'], 160, minimum=3),
              'postcode': _text(value['postcode'], 5), 'city': _text(value['city'], 100, minimum=2),
              'country': _text(value['country'], 2).upper()}
    if (result['country'] != 'DE' or not re.fullmatch(r'[0-9]{5}', result['postcode'])
            or result['postcode'] == '00000' or not re.search(r'[0-9]', result['street'])
            or re.search(r'(?i)\b(?:postfach|packstation|paketstation|postfiliale)\b', result['street'])):
        raise BuyerError('unsupported_delivery_address')
    return result


def validate_context(payload):
    """Normalize user-entered data, not supplier deliverability or freight proof."""
    if not isinstance(payload, dict) or set(payload) != {'contact', 'billing', 'delivery'}:
        raise BuyerError('invalid_address')
    contact = payload['contact']
    if not isinstance(contact, dict) or not {'name', 'email'} <= set(contact) <= {'name', 'email', 'phone'}:
        raise BuyerError('invalid_address')
    result_contact = {'name': _text(contact['name'], 160, minimum=2), 'email': _text(contact['email'], 254)}
    if (not re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\.[A-Za-z]{2,63}", result_contact['email'])
            or '..' in result_contact['email'] or result_contact['email'].startswith('.')):
        raise BuyerError('invalid_address')
    local, domain = result_contact['email'].rsplit('@', 1)
    result_contact['email'] = local + '@' + domain.lower()
    if 'phone' in contact:
        phone = _text(contact['phone'], 40, minimum=0)
        if phone and (not re.fullmatch(r'[+0-9 ()/.-]{5,40}', phone) or len(re.sub(r'\D', '', phone)) < 5):
            raise BuyerError('invalid_address')
        if phone:
            result_contact['phone'] = phone
    billing = _address(payload['billing'])
    delivery = deepcopy(billing) if payload['delivery'] is None else _address(payload['delivery'])
    return {'contact': result_contact, 'billing': billing, 'delivery': delivery}


def _record(value, now, *, require_fresh=True):
    if (not isinstance(value, Mapping) or set(value) != RECORD_FIELDS or value.get('version') != 1
            or type(value.get('version')) is not int or type(value.get('revision')) is not int
            or not 1 <= value['revision'] <= 1_000_000 or not isinstance(value.get('customer_id'), str)
            or not PRINCIPAL.fullmatch(value['customer_id']) or not isinstance(value.get('mode'), str)
            or value['mode'] not in {'test', 'live'}
            or not isinstance(value.get('csrf_token'), str) or not TOKEN.fullmatch(value['csrf_token'])):
        raise BuyerError('buyer_unavailable')
    created, expiry = _stamp(value['created_at']), _stamp(value['expires_at'])
    if created > now + timedelta(seconds=60) or not timedelta(minutes=5) <= expiry - created <= timedelta(days=7):
        raise BuyerError('buyer_unavailable')
    if require_fresh and not created - timedelta(seconds=60) <= now < expiry:
        return None
    if value['context'] is not None and validate_context(value['context']) != value['context']:
        raise BuyerError('buyer_unavailable')
    return deepcopy(dict(value))


def init_buyer_schema(connection):
    """Explicit initialization only, using an owned SQLite connection."""
    connection.execute('''CREATE TABLE IF NOT EXISTS shop_buyer_sessions_v2 (
        session_key TEXT PRIMARY KEY, revision INTEGER NOT NULL,
        expires_at TEXT NOT NULL, record_json TEXT NOT NULL)''')
    connection.execute('CREATE INDEX IF NOT EXISTS shop_buyer_expiry_v2 ON shop_buyer_sessions_v2(expires_at)')
    connection.commit()


class SQLiteBuyerBackend:
    def __init__(self, connection_factory, *, clock=None):
        if not callable(connection_factory):
            raise BuyerError('invalid_buyer_configuration')
        self.factory, self.clock = connection_factory, clock or (lambda: datetime.now(timezone.utc))

    @contextmanager
    def _connection(self, *, write=False):
        connection = None
        try:
            connection = self.factory()
            if not isinstance(connection, sqlite3.Connection):
                raise BuyerError('buyer_unavailable')
            if write:
                connection.execute('BEGIN IMMEDIATE')
            yield connection
            if write:
                connection.commit()
        except Exception:
            if isinstance(connection, sqlite3.Connection):
                try:
                    connection.rollback()
                except Exception:
                    pass
            raise BuyerError('buyer_unavailable') from None
        finally:
            if isinstance(connection, sqlite3.Connection):
                try:
                    connection.close()
                except Exception:
                    pass

    @staticmethod
    def _key(key):
        if not isinstance(key, str) or not KEY.fullmatch(key):
            raise BuyerError('buyer_unavailable')

    def _encode(self, record):
        checked = _record(record, _stamp(self.clock()))
        if checked is None:
            raise BuyerError('buyer_unavailable')
        encoded = _json(checked)
        if len(encoded.encode('utf-8')) > MAX_RECORD:
            raise BuyerError('buyer_unavailable')
        return checked, encoded

    def create(self, key, record):
        self._key(key)
        checked, encoded = self._encode(record)
        if checked['revision'] != 1:
            raise BuyerError('buyer_unavailable')
        with self._connection(write=True) as connection:
            return connection.execute('INSERT OR IGNORE INTO shop_buyer_sessions_v2 VALUES (?,?,?,?)',
                (key, 1, checked['expires_at'], encoded)).rowcount == 1

    def load(self, key):
        self._key(key)
        with self._connection() as connection:
            row = connection.execute('SELECT record_json,revision,expires_at FROM shop_buyer_sessions_v2 WHERE session_key=?', (key,)).fetchone()
        if row is None:
            return None
        try:
            if not isinstance(row[0], str) or len(row[0].encode('utf-8')) > MAX_RECORD:
                raise ValueError()
            value = _record(json.loads(row[0]), _stamp(self.clock()))
            if value is not None and (value['revision'] != row[1] or value['expires_at'] != row[2]):
                raise ValueError()
            return value
        except Exception:
            raise BuyerError('buyer_unavailable') from None

    def compare_and_swap(self, key, expected_revision, record):
        self._key(key)
        checked, encoded = self._encode(record)
        if type(expected_revision) is not int or checked['revision'] != expected_revision + 1:
            raise BuyerError('buyer_unavailable')
        with self._connection(write=True) as connection:
            row = connection.execute('SELECT record_json,revision FROM shop_buyer_sessions_v2 WHERE session_key=?', (key,)).fetchone()
            if row is None or row[1] != expected_revision:
                return False
            prior = _record(json.loads(row[0]), _stamp(self.clock()))
            if prior is None:
                return False
            if any(checked[field] != prior[field] for field in ('customer_id', 'mode', 'created_at', 'expires_at', 'csrf_token')):
                raise BuyerError('buyer_unavailable')
            return connection.execute('UPDATE shop_buyer_sessions_v2 SET revision=?,record_json=? WHERE session_key=? AND revision=?',
                (checked['revision'], encoded, key, expected_revision)).rowcount == 1

    def delete(self, key):
        self._key(key)
        with self._connection(write=True) as connection:
            connection.execute('DELETE FROM shop_buyer_sessions_v2 WHERE session_key=?', (key,))


class ShopBuyer:
    def __init__(self, backend=None, *, public_origin, mode, enabled=False, clock=None,
                 ttl_seconds=86400, delivery_rule=None):
        if (not isinstance(mode, str) or mode not in {'test', 'live'} or type(enabled) is not bool or type(ttl_seconds) is not int
                or not 300 <= ttl_seconds <= 604800 or delivery_rule is not None and not callable(delivery_rule)):
            raise BuyerError('invalid_buyer_configuration')
        self.backend, self.public_origin, self.mode, self.enabled = backend, _origin(public_origin), mode, enabled
        self.clock, self.ttl_seconds, self.delivery_rule = clock or (lambda: datetime.now(timezone.utc)), ttl_seconds, delivery_rule

    def __repr__(self):
        return '<ShopBuyer mode=' + self.mode + ' enabled=' + str(self.enabled) + '>'

    def readiness(self):
        return {'ready': self.enabled and all(callable(getattr(self.backend, name, None)) for name in
                ('create', 'load', 'compare_and_swap', 'delete')), 'mode': self.mode, 'identity': 'guest'}

    def _active(self):
        if not self.enabled or not self.readiness()['ready']:
            raise BuyerError('buyer_unavailable')

    def _request(self, req, *, mutation):
        self._active()
        if (req.scheme != 'https' or req.host_url.rstrip('/') != self.public_origin
                or req.headers.get('Origin') not in (None, self.public_origin)
                or mutation and req.headers.get('Origin') != self.public_origin
                or req.headers.get('Sec-Fetch-Site') not in (None, 'same-origin')):
            raise BuyerError('request_not_allowed')

    def _call(self, method, *args):
        try:
            return getattr(self.backend, method)(*args)
        except Exception:
            raise BuyerError('buyer_unavailable') from None

    def _session(self, req, *, csrf=False):
        self._request(req, mutation=csrf)
        tokens = req.cookies.getlist(COOKIE_NAME)
        if len(tokens) != 1 or not TOKEN.fullmatch(tokens[0]):
            raise BuyerError('buyer_session_required')
        key = sha256(tokens[0].encode('ascii')).hexdigest()
        record = self._call('load', key)
        if record is None:
            raise BuyerError('buyer_session_required')
        record = _record(record, _stamp(self.clock()))
        if record is None or record['mode'] != self.mode:
            raise BuyerError('buyer_session_required')
        supplied = req.headers.get('X-CSRF-Token')
        if csrf and (not isinstance(supplied, str) or not TOKEN.fullmatch(supplied)
                     or not hmac.compare_digest(supplied, record['csrf_token'])):
            raise BuyerError('buyer_session_required')
        return key, record

    @staticmethod
    def public(record):
        return {'guest': True, 'address_ready': record['context'] is not None,
                'csrf_token': record['csrf_token'], 'expires_at': record['expires_at']}

    def establish(self, req):
        """Return (public state, new opaque cookie or None). No portal identity."""
        self._request(req, mutation=False)
        try:
            _, record = self._session(req)
            return self.public(record), None
        except BuyerError as error:
            if error.code != 'buyer_session_required':
                raise
        now = _stamp(self.clock())
        for _ in range(3):
            token = secrets.token_urlsafe(32)
            record = {'version': 1, 'revision': 1, 'customer_id': 'sg_' + uuid4().hex, 'mode': self.mode,
                      'csrf_token': secrets.token_urlsafe(32), 'created_at': now.isoformat(),
                      'expires_at': (now + timedelta(seconds=self.ttl_seconds)).isoformat(), 'context': None}
            if self._call('create', sha256(token.encode('ascii')).hexdigest(), record) is True:
                return self.public(record), token
        raise BuyerError('buyer_unavailable')

    def save_address(self, req, payload):
        key, record = self._session(req, csrf=True)
        context = validate_context(payload)
        if self.delivery_rule is not None:
            try:
                allowed = self.delivery_rule(deepcopy(context))
            except Exception:
                raise BuyerError('buyer_unavailable') from None
            if allowed is not True:
                raise BuyerError('unsupported_delivery_address')
        record['context'], expected = context, record['revision']
        record['revision'] += 1
        if self._call('compare_and_swap', key, expected, record) is not True:
            raise BuyerError('buyer_session_changed')
        return self.public(record)

    def sign_out(self, req):
        key, _ = self._session(req, csrf=True)
        self._call('delete', key)

    def authorize_quote(self, req):
        try:
            _, record = self._session(req, csrf=req.method not in {'GET', 'HEAD', 'OPTIONS'})
            return record['customer_id']
        except BuyerError:
            return None

    def quote_context(self, req, principal):
        _, record = self._session(req, csrf=req.method not in {'GET', 'HEAD', 'OPTIONS'})
        if record['customer_id'] != principal or record['context'] is None:
            raise BuyerError('buyer_address_required')
        return {'vehicle_ref': '', 'delivery_context': deepcopy(record['context'])}


def create_shop_buyer_blueprint(buyer):
    bp = Blueprint('shop_buyer_v2', __name__, url_prefix='/shop/api/v2/buyer')

    def reply(body, status=200):
        response = Response(_json(body).encode('utf-8'), status=status, content_type='application/json; charset=utf-8')
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Vary'] = 'Cookie, Origin'
        return response

    @bp.before_request
    def enabled():
        if current_app.config.get('SHOP_BUYER_ENABLED') is not True:
            return reply({'error': 'not_found'}, 404)
        if buyer is None or not buyer.readiness()['ready']:
            return reply({'error': 'buyer_unavailable'}, 503)
        if request.args or request.headers.get('Transfer-Encoding'):
            return reply({'error': 'invalid_request'}, 400)

    @bp.errorhandler(BuyerError)
    def error(error):
        code = error.code
        status = 503 if code == 'buyer_unavailable' else 409 if code == 'buyer_session_changed' else 400
        if code in {'request_not_allowed', 'buyer_session_required'}:
            status = 403
        return reply({'error': code}, status)

    @bp.get('/session')
    def session_state():
        if request.content_length not in (None, 0):
            raise BuyerError('invalid_request')
        result, token = buyer.establish(request)
        response = reply(result)
        if token is not None:
            response.set_cookie(COOKIE_NAME, token, max_age=buyer.ttl_seconds, expires=_stamp(result['expires_at']),
                                secure=True, httponly=True, samesite='Lax', path='/')
        return response

    @bp.put('/address')
    def address():
        if (request.mimetype != 'application/json' or request.content_length is None
                or not 0 < request.content_length <= MAX_BODY):
            raise BuyerError('invalid_request')
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError()
                result[key] = value
            return result
        try:
            payload = json.loads(request.get_data(cache=False), object_pairs_hook=unique,
                                 parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        except (ValueError, UnicodeError, RecursionError):
            raise BuyerError('invalid_request') from None
        return reply(buyer.save_address(request, payload))

    @bp.delete('/session')
    def sign_out():
        if request.content_length not in (None, 0):
            raise BuyerError('invalid_request')
        buyer.sign_out(request)
        response = reply({'signed_out': True})
        response.delete_cookie(COOKIE_NAME, path='/', secure=True, httponly=True, samesite='Lax')
        return response

    return bp
