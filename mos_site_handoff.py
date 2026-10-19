"""Bounded, session-bound prefill for the public MOS Site; never books or pays."""

from datetime import datetime, timezone
import hmac
import math
import re
import secrets
import time
from zoneinfo import ZoneInfo

from mos_contract_delivery import _single_address


SITE_ORIGINS = (
    'https://autovermietung-mos.de',
    'https://www.autovermietung-mos.de',
    'https://autovermietung-mos.info979896.chatgpt.site',
)
VEHICLES = {'1': 'i10', '3': 'kona'}
SESSION_KEY = 'mos_site_handoffs'
MAX_AGE_SECONDS = 300
BERLIN = ZoneInfo('Europe/Berlin')


def issue_nonce(session, now=None):
    now = int(time.time() if now is None else now)
    states = {key: stamp for key, stamp in session.get(SESSION_KEY, {}).items()
              if type(stamp) is int and 0 <= now - stamp <= MAX_AGE_SECONDS}
    while len(states) >= 4:
        del states[min(states, key=states.get)]
    nonce = secrets.token_urlsafe(32)
    states[nonce] = now
    session[SESSION_KEY] = states
    return nonce


def consume_nonce(session, nonce, now=None):
    now = int(time.time() if now is None else now)
    states = dict(session.get(SESSION_KEY, {}))
    if not isinstance(nonce, str) or not 20 <= len(nonce) <= 100:
        raise ValueError('Datenübernahme abgelaufen. Bitte die Fahrzeugauswahl erneut öffnen.')
    key = next((key for key in states if hmac.compare_digest(key, nonce)), None)
    stamp = states.pop(key, None) if key is not None else None
    session[SESSION_KEY] = states
    if type(stamp) is not int or not 0 <= now - stamp <= MAX_AGE_SECONDS:
        raise ValueError('Datenübernahme abgelaufen. Bitte die Fahrzeugauswahl erneut öffnen.')


def validated_prefill(fields, cfg, now=None):
    """Validate Site fields; prices/availability still come only from quote()."""
    now = now or datetime.now(timezone.utc)
    vehicle = fields.get('vehicle_id', '')
    if not isinstance(vehicle, str) or vehicle not in VEHICLES:
        raise ValueError('Dieses Fahrzeug ist nur auf Anfrage verfügbar.')
    slug = VEHICLES[vehicle]
    if cfg.get('fleet', {}).get(slug, {}).get('id') != int(vehicle):
        raise ValueError('Die Fahrzeugzuordnung muss geprüft werden.')

    slots = []
    for key in ('start', 'end'):
        raw = fields.get(key, '')
        if not isinstance(raw, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:00', raw):
            raise ValueError('Bitte einen stündlichen Abhol- und Rückgabetermin wählen.')
        try:
            slot = datetime.strptime(raw, '%Y-%m-%dT%H:%M').replace(tzinfo=BERLIN)
        except ValueError:
            raise ValueError('Ungültiger Übergabetermin.') from None
        if slot.weekday() == 6 or not 8 <= slot.hour <= 20:
            raise ValueError('Übergaben sind Montag bis Samstag stündlich von 08 bis 20 Uhr möglich.')
        if slot.astimezone(timezone.utc) <= now.astimezone(timezone.utc):
            raise ValueError('Bitte künftige Übergabetermine wählen.')
        slots.append(slot)
    seconds = (slots[1].astimezone(timezone.utc) - slots[0].astimezone(timezone.utc)).total_seconds()
    max_days = min(5, cfg['max_days'])
    if seconds <= 0 or math.ceil(seconds / 86400) > max_days:
        raise ValueError('Bitte einen Mietzeitraum von höchstens fünf Miettagen wählen.')

    name, email = fields.get('name', ''), fields.get('email', '')
    if not isinstance(name, str) or not isinstance(email, str):
        raise ValueError('Bitte gültigen Namen und E-Mail angeben.')
    name, email = name.strip(), email.strip()
    if (not 1 <= len(name) <= 150 or any(ord(char) < 32 for char in name)
            or len(email) > 254 or not _single_address(email)):
        raise ValueError('Bitte gültigen Namen und E-Mail angeben.')
    planned = fields.get('planned_km', '')
    if (not isinstance(planned, str) or planned and
            (not re.fullmatch(r'\d{1,7}', planned) or int(planned) > 1000000)):
        raise ValueError('Bitte eine gültige geplante Fahrstrecke angeben.')
    return {'vehicle': slug, 'start': slots[0].isoformat(), 'end': slots[1].isoformat(),
            'name': name, 'email': email, 'planned_km': int(planned) if planned else None}
