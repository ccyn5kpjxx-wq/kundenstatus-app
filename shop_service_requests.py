"""Connect shop service buttons to the existing persistent workshop intake.

This adapter opens a form only. Its GET never reserves capacity or contacts a
customer. The existing /anfrage handler remains responsible for CSRF, rate
limits, explicit customer submission, slot reservation, leads, protected admin
views, confirmation notices and backups. Purchased-goods arrival is a separate
concern; a shop cart must never trigger an appointment implicitly.
"""

import json
import unicodedata
from typing import NamedTuple

SERVICE_REQUEST_PATH = '/anfrage?anliegen=reifenservice'
CART_REQUEST_PATH = '/anfrage?anliegen=allgemein&shop_anfrage=1'
MAX_CART_REQUEST_BYTES = 32768


class WorkshopService(NamedTuple):
    label: str
    description: str
    intent: str
    option: str = ''


WORKSHOP_SERVICES = {
    'reifenmontage': WorkshopService('Reifenmontage', 'Sie bringen lose Reifen mit. Wir ziehen sie auf Ihre Felgen auf, wuchten sie aus und montieren die Räder am Fahrzeug.', 'reifenservice', 'montage'),
    'raederwechsel': WorkshopService('Räderwechsel', 'Ihre Reifen sind bereits auf Felgen. Wir wechseln die kompletten Räder und prüfen Zustand und Luftdruck.', 'reifenservice', 'radwechsel'),
    'reifencheck': WorkshopService('Reifencheck', 'Profil, Zustand und Luftdruck kontrollieren lassen.', 'reifenservice', 'pruefung'),
    'oelservice': WorkshopService('Ölwechsel / Ölservice', 'Herstellerfreigabe, Füllmenge und Ölfilter passend zum Fahrzeug prüfen.', 'mechanikanfrage', 'oelwechsel'),
    'lackierarbeiten': WorkshopService('Lackierarbeiten', 'Neuteile in Wunschfarbe, Teil- und Komplettlackierung oder Spot-Repair.', 'lackanfrage'),
    'aufbereitung': WorkshopService('Fahrzeugaufbereitung', 'Innen, außen oder komplett. Den Umfang wählen Sie im Formular.', 'fahrzeugpflege'),
    'karosserie': WorkshopService('Karosserie / Smart Repair', 'Unfallschäden, Kunststoffreparatur und Kleinschäden.', 'reparaturanfrage'),
    'dellen': WorkshopService('Dellen entfernen', 'Park- und Hageldellen prüfen und instand setzen.', 'dellenreparatur'),
    'autoglas': WorkshopService('Autoglas', 'Steinschlagreparatur oder Scheibentausch nach Prüfung.', 'glasschaden'),
    'mechanik': WorkshopService('Mechanik / Service', 'Fahrzeugservice und Bremsenarbeiten nach Abstimmung.', 'mechanikanfrage'),
    'softlack': WorkshopService('Softlack / Industrielackierung', 'Oberflächen im Innenraum und geeignete Industriebauteile.', 'lackanfrage'),
}


def selected_workshop_service(key, intent):
    service = WORKSHOP_SERVICES.get(key)
    return service if service and service.intent == intent else None


class ShopOfferError(ValueError):
    """A fixed validation error, without submitted text or personal data."""

    def __init__(self):
        super().__init__('invalid_shop_offer_context')


def parse_shop_offer_context(form):
    """Read bounded customer-reported selection text for an ordinary lead.

    No catalogue, quote, order, payment, availability or compatibility is
    confirmed here. The caller uses the existing explicit contact-form POST,
    CSRF, rate limiting, validation and durable lead workflow unchanged.
    A missing marker must never silently discard a submitted long selection.
    """
    marker, raw = form.get('shop_anfrage'), form.get('shop_auswahl')
    if marker in (None, '') and raw in (None, ''):
        return None
    try:
        if marker != '1' or form.get('anliegen') != 'allgemein':
            raise ShopOfferError()
        if hasattr(form, 'getlist') and any(len(form.getlist(key)) != 1
                for key in ('shop_anfrage', 'shop_auswahl', 'anliegen')):
            raise ShopOfferError()
        if (not isinstance(raw, str) or not raw or len(raw) > MAX_CART_REQUEST_BYTES
                or len(raw.encode('utf-8')) > MAX_CART_REQUEST_BYTES):
            raise ShopOfferError()

        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ShopOfferError()
                result[key] = value
            return result

        def invalid_constant(_):
            raise ShopOfferError()

        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
        if (not isinstance(value, dict) or set(value) != {
                'version', 'created_at', 'message', 'full_text', 'position_count', 'omitted_count'}
                or type(value['version']) is not int or value['version'] != 1
                or type(value['created_at']) is not int or not 0 <= value['created_at'] <= 2**53 - 1
                or type(value['position_count']) is not int or not 1 <= value['position_count'] <= 40
                or type(value['omitted_count']) is not int
                or not 0 <= value['omitted_count'] <= value['position_count']):
            raise ShopOfferError()
        for key, maximum in (('message', 2000), ('full_text', 32000)):
            text = value[key]
            if (not isinstance(text, str) or not text.strip() or len(text) > maximum
                    or any((unicodedata.category(char) == 'Cc' and char not in '\n\t')
                           or 0xD800 <= ord(char) <= 0xDFFF for char in text)):
                raise ShopOfferError()
        return {'version': 1, 'source': 'customer_reported_shop_selection',
                'full_text': value['full_text'], 'position_count': value['position_count'],
                'requires_manual_review': True, 'quote_confirmed': False}
    except ShopOfferError:
        raise
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise ShopOfferError() from None


def prepare_service_request_links(portal, *, enabled=False):
    """Return frontend feature links only for the real persistent intake.

    Explicit activation is required. A stateless PUBLIC_SITE_ONLY service must
    not advertise its locally blocked POST route. This helper deliberately
    avoids credentials, databases, request/session values, external URLs and
    any provider or notification callback. No user-controlled URL is accepted.
    """
    if enabled is not True or getattr(portal, 'PUBLIC_SITE_ONLY', None) is not False:
        return {}
    app = getattr(portal, 'app', None)
    if app is None or not callable(getattr(app, 'view_functions', {}).get('website_anfrage')):
        return {}
    try:
        rules = app.url_map.iter_rules('website_anfrage')
        available = any(rule.rule == '/anfrage' and not rule.arguments
                        and {'GET', 'POST'} <= rule.methods for rule in rules)
    except (AttributeError, KeyError, TypeError):
        return {}
    return {'service_request': SERVICE_REQUEST_PATH, 'cart_request': CART_REQUEST_PATH} if available else {}
