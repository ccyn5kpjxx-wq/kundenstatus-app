"""Booking-specific recovery after an ordinary browser session has expired.

Tokens travel only in an email URL fragment and a CSRF-protected POST. They
never extend the application's session lifetime or grant access to other holds.
"""

from datetime import datetime, timedelta, timezone
from hashlib import sha256
import hmac
import json
from urllib.parse import urlsplit

from itsdangerous import BadData, URLSafeSerializer


TOKEN_SALT = 'mos-booking-management-v1'
TOKEN_KEYS = {'hold_id', 'owner_hash', 'scope', 'payload_sha256', 'expires_at'}


class AccessExpired(ValueError):
    """A historical contract can still be mailed without an active web link."""


def _now(value=None):
    value = value or datetime.now(timezone.utc)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError('Zeitpunkt braucht eine Zeitzone.')
    return value.astimezone(timezone.utc)


def _snapshot(hold):
    try:
        payload = json.loads(hold['payload'])
        quote = payload['quote']
        owner_hash = quote['owner_hash']
        end = datetime.fromisoformat(quote['end_slot'])
        intent = hold.get('payment_intent')
        if quote.get('payment_method') == 'onsite':
            from mietwagen_checkout import validate_onsite_quote
            validate_onsite_quote(quote)
            valid_order = (not intent and (hold.get('status') == 'confirmed'
                           or (hold.get('status') == 'released' and hold.get('grund') == 'onsite_cancelled')))
        else:
            valid_order = isinstance(intent, str) and intent.startswith('pi_')
        if (hold.get('status') not in {'confirmed', 'review', 'released'}
                or not hold.get('mietvorgang_id')
                or not valid_order
                or quote.get('test_only') is not False
                or not isinstance(owner_hash, str) or len(owner_hash) != 64
                or any(char not in '0123456789abcdef' for char in owner_hash)
                or end.tzinfo is None):
            raise ValueError('Buchungszugang ist nicht verfügbar.')
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError('Buchungszugang ist nicht verfügbar.') from exc
    return quote, end.astimezone(timezone.utc) + timedelta(days=30)


def _serializer(portal):
    secret = portal.app.secret_key
    if not isinstance(secret, (str, bytes)) or len(secret) < 32:
        raise ValueError('Sicherer Buchungszugang ist nicht eingerichtet.')
    return URLSafeSerializer(secret, salt=TOKEN_SALT, signer_kwargs={'digest_method': sha256})


def _read_hold(portal, hold_id):
    db = portal.get_db()
    try:
        row = db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone()
        if row is None:
            raise ValueError('Buchungszugang ist nicht verfügbar.')
        return dict(row)
    finally:
        db.close()


def session_access_valid(hold, access, now=None):
    """Validate a scoped entry already authenticated into Flask's signed session."""
    try:
        quote, deadline = _snapshot(hold)
        return (isinstance(access, dict)
                and access.get('scope') == 'booking'
                and access.get('hold_id') == hold.get('id')
                and type(access.get('expires_at')) is int
                and access['expires_at'] == int(deadline.timestamp())
                and _now(now).timestamp() < access['expires_at']
                and isinstance(access.get('owner_hash'), str)
                and hmac.compare_digest(access['owner_hash'], quote['owner_hash'])
                and isinstance(access.get('payload_sha256'), str)
                and hmac.compare_digest(access['payload_sha256'], sha256(hold['payload'].encode()).hexdigest()))
    except (KeyError, TypeError, ValueError):
        return False


def create_access_token(portal, hold, now=None):
    stamp = _now(now)
    current = _read_hold(portal, hold['id'])
    if current['payload'] != hold['payload']:
        raise ValueError('Buchungszugang ist nicht verfügbar.')
    quote, deadline = _snapshot(current)
    if stamp >= deadline:
        raise AccessExpired('Buchungszugang ist abgelaufen.')
    return _serializer(portal).dumps({
        'hold_id': current['id'], 'owner_hash': quote['owner_hash'], 'scope': 'booking',
        'payload_sha256': sha256(current['payload'].encode()).hexdigest(),
        'expires_at': int(deadline.timestamp()),
    })


def validate_access_token(portal, token, now=None):
    try:
        if not isinstance(token, str) or not token or len(token) > 4096:
            raise ValueError('Buchungszugang ist ungültig oder abgelaufen.')
        access = _serializer(portal).loads(token)
        if (not isinstance(access, dict) or set(access) != TOKEN_KEYS
                or not isinstance(access['hold_id'], str) or not 1 <= len(access['hold_id']) <= 200):
            raise ValueError('Buchungszugang ist ungültig oder abgelaufen.')
        hold = _read_hold(portal, access['hold_id'])
        if not session_access_valid(hold, access, now):
            raise ValueError('Buchungszugang ist ungültig oder abgelaufen.')
        return access
    except (BadData, KeyError, TypeError) as exc:
        raise ValueError('Buchungszugang ist ungültig oder abgelaufen.') from exc


def access_url(portal, hold, *, origin=None, now=None):
    origin = origin or portal.app.config.get('MOS_PUBLIC_BOOKING', {}).get('origin', '')
    parsed = urlsplit(origin)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.path
            or parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise ValueError('Sicherer Buchungszugang benötigt die konfigurierte HTTPS-Adresse.')
    return origin + '/mieten/buchung-verwalten#' + create_access_token(portal, hold, now)
