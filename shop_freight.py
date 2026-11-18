"""Reviewed retail shipping tariff derived from RSU, plus 15% on shipping.

Goods markups are unaffected. This is our published customer shipping charge,
not a claim of a supplier reservation or final supplier invoice. Tax basis,
product shipping classes and destination eligibility require explicit server
configuration; unclassified goods/destinations never silently become free.
Each private shipment item is one already aggregated supplier SKU/destination
and therefore one RSU API order. Thresholds never combine different SKUs.
No network, credentials, supplier orders or payment operations live here.
"""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
import json
import re

SHIPPING_CLASSES = {'pkw_tyre', 'pkw_rim', 'complete_wheel', 'rdks_sensor',
                    'centring_ring', 'mounting_supply', 'parts_standard', 'parts_bulky', 'free'}
MAX_CENTS = 1_000_000
# GLS lists these as examples of island-surcharge postcodes, not an exhaustive
# inventory of German islands. The explicit customer declaration must exclude
# ALL island addresses, including ones absent from this defensive denylist.
# https://www.gls-pakete.de/glossar/inselzuschlag
KNOWN_ISLAND_POSTCODES = frozenset({
    '18565', '25849', '25859', '25863', '25869', '25938', '25946', '26465',
    '26474', '26486', '26548', '26571', '26579', '26757', '27498',
}) | frozenset(str(value) for value in range(25980, 26000))


class MainlandDeliveryEligibility:
    """Merchant delivery-area policy, never a supplier/address verification.

    TYREsystem publishes delivery within Germany and separate per-parcel island
    costs. Parcel count is unknown here, so no island price is inferred. The
    existing postcode allowlist remains available without opting into mainland
    declarations; extending it requires BOTH explicit server and saved buyer
    opt-ins. https://www.tyresystem.de/lieferbedingungen
    """
    def __init__(self, approved_postcodes=(), *, mainland_delivery_approved=False):
        if (not isinstance(approved_postcodes, (list, tuple, set, frozenset))
                or len(approved_postcodes) > 100000
                or any(not isinstance(value, str) or not re.fullmatch(r'[0-9]{5}', value)
                       or value == '00000' for value in approved_postcodes)):
            raise ValueError('invalid_shipping_policy')
        self.approved_postcodes = frozenset(approved_postcodes)
        self.mainland_enabled = mainland_delivery_approved is True

    def __call__(self, destination, context):
        if (not isinstance(destination, str) or destination not in {'workshop', 'customer'} or not isinstance(context, Mapping)
                or 'delivery_mainland' in context and type(context['delivery_mainland']) is not bool):
            return False
        choice = context.get('workshop_delivery', 'collection')
        if not isinstance(choice, str) or choice not in {'collection', 'customer_reshipment'}:
            return False
        if destination == 'workshop' and choice == 'collection':
            return True
        # Reuse the exact saved-buyer address rules rather than creating a
        # weaker country/postcode-only path at checkout or worker revalidation.
        from shop_buyer import BuyerError, _address
        try:
            delivery = context.get('delivery')
            if not isinstance(delivery, Mapping):
                return False
            delivery = _address(dict(delivery), delivery=True)
        except BuyerError:
            return False
        text = ' '.join(delivery[key] for key in ('recipient', 'street', 'city'))
        if (delivery['postcode'] in KNOWN_ISLAND_POSTCODES
                or re.search(r'(?i)\b(?:postfach|packstation|paketstation|postfiliale)\b', text)):
            return False
        return (delivery['postcode'] in self.approved_postcodes
                or self.mainland_enabled and context.get('delivery_mainland') is True)


def shipping_gross_cent(base_cent, *, basis):
    """Apply the user's 15% shipping markup and correct gross rounding once."""
    if type(base_cent) is not int or not 0 <= base_cent <= MAX_CENTS or basis not in {'net', 'gross'}:
        raise ValueError('invalid_shipping_price')
    factor = Decimal('1.15') * (Decimal('1.19') if basis == 'net' else Decimal('1'))
    result = int((Decimal(base_cent) * factor).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    if result > MAX_CENTS:
        raise ValueError('invalid_shipping_price')
    return result


def _hash(value):
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                             separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        raise ValueError('invalid_shipping_policy') from None


class RSURetailFreight:
    def __init__(self, shipping_classes, *, mode, basis=None, evidence_id, checked_at,
                 destination_eligibility=None, clock=None):
        if (not isinstance(shipping_classes, Mapping) or mode not in {'test', 'live'}
                or basis not in {'net', 'gross', None} or not isinstance(evidence_id, str)
                or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', evidence_id)
                or any(not isinstance(key, str) or not re.fullmatch(r'(?:tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}', key)
                       or value not in SHIPPING_CLASSES for key, value in shipping_classes.items())):
            raise ValueError('invalid_shipping_policy')
        self.classes = dict(shipping_classes)
        self.mode, self.basis, self.evidence_id = mode, basis, evidence_id
        self.checked_at = _stamp(checked_at)
        self.eligibility = destination_eligibility
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def workshop_collection_gross_cent(self, rim_id, tyre_id, quantity):
        """Read the same one-shipment tariff used at checkout for workshop pickup."""
        context = {'workshop_delivery': 'collection'}
        shipment = {'id': 'workshop', 'destination': 'workshop', 'context_hash': _hash(context),
                    'private_items': [{'id': identifier, 'quantity': quantity} for identifier in (rim_id, tyre_id)]}
        shipment['shipment_hash'] = _hash(shipment)
        quote = self(shipment, context)
        return quote['gross_cent'] if quote is not None else None

    def __call__(self, shipment, context):
        try:
            now = _stamp(self.clock())
            if (self.basis is None or not self.checked_at <= now < self.checked_at + timedelta(days=30)
                    or not isinstance(shipment, Mapping) or not isinstance(context, Mapping)
                    or shipment.get('destination') not in {'workshop', 'customer'}
                    or shipment.get('context_hash') != _hash(context)):
                return None
            expected = {key: shipment[key] for key in ('id', 'destination', 'private_items', 'context_hash')}
            if shipment.get('shipment_hash') != _hash(expected):
                return None
            workshop_delivery = context.get('workshop_delivery', 'collection')
            if (not isinstance(workshop_delivery, str)
                    or workshop_delivery not in {'collection', 'customer_reshipment'}):
                return None
            # Eligibility is separate from address input validation, e.g. the
            # operator's confirmed mainland/island delivery areas.
            if not callable(self.eligibility) or self.eligibility(shipment['destination'], context) is not True:
                return None
            legs = 2 if shipment['destination'] == 'workshop' and workshop_delivery == 'customer_reshipment' else 1
            if legs == 2 and self.eligibility('customer', context) is not True:
                return None
            rows = shipment['private_items']
            if not isinstance(rows, list) or not 1 <= len(rows) <= 40:
                return None
            base, seen = 0, set()
            for row in rows:
                if (not isinstance(row, Mapping) or row.get('id') not in self.classes
                        or row['id'] in seen or type(row.get('quantity')) is not int
                        or not 1 <= row['quantity'] <= 20):
                    return None
                seen.add(row['id'])
                category = self.classes[row['id']]
                quantity = row['quantity']
                # One position per API order: a different SKU creates another
                # fee/threshold even when both orders go to the same address.
                if category in {'pkw_tyre', 'pkw_rim', 'complete_wheel'} and quantity == 1:
                    base += 600
                elif category in {'rdks_sensor', 'centring_ring'} and quantity < 4:
                    base += 600
                elif category in {'mounting_supply', 'parts_standard'}:
                    base += 600
                elif category == 'parts_bulky':
                    base += 6000
            # Our retail shipment line rounds once after summing the supplier
            # order charges and any explicitly requested second shipping leg.
            # Direct-to-customer and workshop collection always have one leg.
            # Supplier-free inbound freight does not establish our outbound
            # tariff. Hold reshipment until that separate cost is known.
            if legs == 2 and base == 0:
                return None
            gross = shipping_gross_cent(base * legs, basis=self.basis)
            return {'confirmed': True, 'shipment_hash': shipment['shipment_hash'],
                    'context_hash': shipment['context_hash'], 'mode': self.mode,
                    'currency': 'eur', 'vat_percent': '19.00', 'gross_cent': gross,
                    'observed_at': now.isoformat(), 'expires_at': (now + timedelta(minutes=15)).isoformat(),
                    'retail_tariff_evidence': self.evidence_id, 'shipping_markup_percent': '15.00'}
        except Exception:
            return None
