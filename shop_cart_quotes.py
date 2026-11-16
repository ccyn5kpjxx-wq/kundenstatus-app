"""Server-owned mixed-cart quotes. No HTTP, payment, database or supplier writes.

All providers are injected. Their confirmations must bind the exact product
identity, quantity, mode and current timestamps. Missing confirmations block a
quote; a missing freight amount is never interpreted as free delivery.

Provider contracts (only these whitelisted fields are retained):
* catalogue_lookup(id): id, category, name, identity_hash; fitment_required,
  fitment={confirmed, identity_hash, vehicle_ref, evidence_id}; new_part,
  painting_eligible; required_fees_confirmed, required_fees (codes); diameter,
  tyre_size (for wheel assembly).
* retail_quote(product, aggregate_quantity): confirmed, product_id,
  identity_hash, quantity, mode, currency='eur', unit_gross_cent, stock,
  observed_at, expires_at, vat_percent='19.00', procurement_confirmed=True.
* rsu_mapping(product): confirmed, product_id, identity_hash, supplier='RSU',
  supplier_article_id, evidence_id.
* service_quote(code, product, quantity, options): confirmed, product_id,
  identity_hash, quantity, options_hash, mode, currency, gross_cent,
  observed_at, expires_at, vat_percent='19.00'. Amount covers this entire service/quantity.
* freight_quote(shipment, delivery_context): confirmed, shipment_hash,
  context_hash, mode, currency, gross_cent, observed_at, expires_at, vat_percent='19.00'.
* assembly_fitment(rim, tyre, vehicle_ref): confirmed, rim_identity_hash,
  tyre_identity_hash, vehicle_ref, tyre_size, rim_diameter, evidence_id.

Timestamps are aware ISO-8601 strings. Confirmation lifetimes cannot exceed
15 minutes. identity_hash is a server-owned exact-variant SHA256. Hashes bind
the immutable server snapshot; clients must never establish trust by supplying
their own snapshot/hash. Persist the returned snapshot before later checkout.
"""
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import hmac
import json
import re


ID = re.compile(r'(tyre|alloy_wheel|steel_wheel|parts):[0-9a-f]{24}')
HASH = re.compile(r'[0-9a-f]{64}')
CODE = re.compile(r'[a-z][a-z_]{0,39}')
GROUP = re.compile(r'[A-Za-z0-9_:|.\-]{1,180}')
CATEGORIES = {'tyre', 'alloy_wheel', 'steel_wheel', 'body_part', 'oil', 'care', 'accessory'}
WHEELS = {'tyre', 'alloy_wheel', 'steel_wheel'}
RIMS = {'alloy_wheel', 'steel_wheel'}
MAX_CENTS = 100_000_000
TTL = timedelta(minutes=15)
SERVICE_FLAGS = {'mounting', 'runflat', 'disposal', 'installation', 'painting', 'oil_service'}
SERVICE_FIELDS = SERVICE_FLAGS | {'paint_color', 'care'}


class QuoteInputError(ValueError):
    """Fixed, public input error code; never carries provider error text."""
    def __init__(self, code='invalid_cart'):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def _text(value, maximum=200):
    return (isinstance(value, str) and 0 < len(value.strip()) <= maximum
            and not any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in value))


def _amount(value, *, zero=False):
    return type(value) is int and (0 if zero else 1) <= value <= MAX_CENTS


def _stamp(value):
    if not isinstance(value, str) or len(value) > 50:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return stamp.astimezone(timezone.utc) if stamp.tzinfo is not None else None
    except (ValueError, OverflowError):
        return None


def retail_unit_gross_cent(partner_net, category):
    """Trusted server net amount -> rounded retail unit; never a client input."""
    if not isinstance(category, str) or category not in CATEGORIES or isinstance(partner_net, (float, bool)):
        raise ValueError('invalid_partner_price')
    try:
        net = Decimal(partner_net)
        if not net.is_finite() or not Decimal('0') < net <= Decimal('100000') or net.as_tuple().exponent < -4:
            raise ValueError('invalid_partner_price')
        factor = Decimal('1.10') if category in WHEELS else Decimal('1.15')
        result = int((net * factor * Decimal('1.19') * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError('invalid_partner_price') from None
    if not _amount(result):
        raise ValueError('invalid_partner_price')
    return result


def normalize_cart(payload):
    if not isinstance(payload, Mapping) or set(payload) != {'lines'}:
        raise QuoteInputError()
    values = payload['lines']
    if not isinstance(values, list) or not 1 <= len(values) <= 40:
        raise QuoteInputError('invalid_line_count')
    result, seen = [], set()
    for value in values:
        if (not isinstance(value, Mapping) or not {'id', 'quantity'} <= set(value)
                or set(value) - {'id', 'quantity', 'services', 'assembly_group'}):
            raise QuoteInputError('invalid_line')
        identifier = value['id']
        if not isinstance(identifier, str) or not ID.fullmatch(identifier):
            raise QuoteInputError('invalid_product_id')
        quantity = value['quantity']
        if type(quantity) is not int or not 1 <= quantity <= (20 if identifier.startswith('parts:') else 4):
            raise QuoteInputError('invalid_quantity')
        services = value.get('services', {})
        if not isinstance(services, Mapping) or set(services) - SERVICE_FIELDS:
            raise QuoteInputError('invalid_services')
        clean = {key: False for key in sorted(SERVICE_FLAGS)}
        for key in SERVICE_FLAGS & set(services):
            if type(services[key]) is not bool:
                raise QuoteInputError('invalid_services')
            clean[key] = services[key]
        colour = services.get('paint_color', '')
        if (not isinstance(colour, str) or len(colour) > 60
                or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF or c in '<>&{}' for c in colour)
                or bool(colour.strip()) != clean['painting']):
            raise QuoteInputError('invalid_paint_colour')
        clean['paint_color'] = colour.strip()
        care = services.get('care', '')
        if not isinstance(care, str) or care not in {'', 'inside', 'outside', 'complete'}:
            raise QuoteInputError('invalid_care_service')
        clean['care'] = care
        group = value.get('assembly_group', '')
        if not isinstance(group, str) or (group and not GROUP.fullmatch(group)):
            raise QuoteInputError('invalid_assembly_group')
        row = {'id': identifier, 'quantity': quantity, 'services': clean, 'assembly_group': group}
        signature = _json(row)
        if signature in seen:
            raise QuoteInputError('duplicate_line')
        seen.add(signature)
        result.append(row)
    return result


@dataclass(frozen=True)
class CartQuote:
    _json_snapshot: str
    quote_hash: str

    def snapshot(self):
        return {**json.loads(self._json_snapshot), 'quote_hash': self.quote_hash}

    def public(self):
        snapshot = self.snapshot()
        fields = ('version', 'mode', 'currency', 'vat_percent', 'created_at', 'expires_at', 'ready',
                  'known_subtotal_gross_cent', 'total_gross_cent', 'blocked_reasons', 'quote_hash')
        result = {key: snapshot[key] for key in fields}
        result['lines'] = [{key: row[key] for key in ('id', 'name', 'category', 'quantity',
                           'destination', 'unit_gross_cent', 'items_gross_cent', 'services')}
                          for row in snapshot['lines']]
        result['shipments'] = [{key: group[key] for key in ('id', 'destination', 'items', 'gross_cent')}
                              for group in snapshot['shipments']]
        # Quote readiness does not activate an order, supplier or payment path.
        result.update(order_enabled=False, payment_enabled=False)
        return result


def restore_quote(snapshot, expected_hash, *, mode, now):
    """Verify against a hash retained by the server, not one trusted from a client."""
    if not isinstance(snapshot, Mapping) or not isinstance(expected_hash, str) or not HASH.fullmatch(expected_hash):
        raise QuoteInputError('invalid_quote_snapshot')
    value = dict(snapshot)
    supplied = value.pop('quote_hash', None)
    try:
        encoded = _json(value)
        actual = hashlib.sha256(encoded.encode('utf-8')).hexdigest()
        good = isinstance(supplied, str) and hmac.compare_digest(actual, expected_hash) and hmac.compare_digest(supplied, expected_hash)
    except (ValueError, TypeError):
        good = False
    if not good or value.get('mode') != mode or value.get('currency') != 'eur':
        raise QuoteInputError('quote_binding_mismatch')
    start, expiry = _stamp(value.get('created_at')), _stamp(value.get('expires_at'))
    if (not isinstance(now, datetime) or now.tzinfo is None or start is None or expiry is None
            or not start <= now.astimezone(timezone.utc) < expiry or expiry - start > TTL):
        raise QuoteInputError('quote_expired')
    return CartQuote(encoded, actual)


class CartQuoteService:
    def __init__(self, catalogue_lookup=None, retail_quote=None, rsu_mapping=None,
                 service_quote=None, freight_quote=None, assembly_fitment=None,
                 *, mode='test', clock=None):
        if not isinstance(mode, str) or mode not in {'test', 'live'}:
            raise ValueError('invalid_quote_mode')
        self.catalogue_lookup, self.retail_quote, self.rsu_mapping = catalogue_lookup, retail_quote, rsu_mapping
        self.service_quote, self.freight_quote, self.assembly_fitment = service_quote, freight_quote, assembly_fitment
        self.mode, self.clock = mode, clock or (lambda: datetime.now(timezone.utc))

    @staticmethod
    def _call(provider, *args):
        if not callable(provider):
            return None
        try:
            value = provider(*deepcopy(args))
            # Copy/strip only in the typed consumers; never return error text.
            return deepcopy(dict(value)) if isinstance(value, Mapping) else None
        except Exception:
            return None

    def _current(self, value, now):
        if not value or value.get('confirmed') is not True:
            return None, 'confirmation_missing'
        if value.get('mode') != self.mode:
            return None, 'mode_mismatch'
        if value.get('currency') != 'eur':
            return None, 'currency_mismatch'
        if value.get('vat_percent') != '19.00':
            return None, 'vat_unconfirmed'
        observed, expiry = _stamp(value.get('observed_at')), _stamp(value.get('expires_at'))
        if observed is None or expiry is None or not timedelta(0) < expiry - observed <= TTL:
            return None, 'invalid_quote_time'
        if observed > now:
            return None, 'quote_from_future'
        if expiry <= now:
            return None, 'quote_expired'
        return expiry, None

    def quote(self, payload, *, customer_id, vehicle_ref='', delivery_context=None):
        rows = normalize_cart(payload)
        if (isinstance(customer_id, bool) or not isinstance(customer_id, (str, int))
                or type(customer_id) is int and customer_id <= 0 or not _text(str(customer_id), 100)):
            raise QuoteInputError('customer_required')
        if (not isinstance(vehicle_ref, str) or len(vehicle_ref) > 200
                or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in vehicle_ref)):
            raise QuoteInputError('invalid_vehicle_context')
        try:
            context_json = _json(delivery_context)
            if len(context_json.encode('utf-8')) > 16384:
                raise ValueError()
            context = json.loads(context_json)
        except (TypeError, ValueError):
            raise QuoteInputError('invalid_delivery_context') from None
        now = self.clock()
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError('aware_clock_required')
        now = now.astimezone(timezone.utc)
        expiry = now + TTL
        reasons, products, mappings, prices, demand = [], {}, {}, {}, {}

        def block(code, **where):
            item = {'code': code, **where}
            if item not in reasons:
                reasons.append(item)

        def shorten(stamp):
            nonlocal expiry
            if stamp is not None:
                expiry = min(expiry, stamp)

        for row in rows:
            demand[row['id']] = demand.get(row['id'], 0) + row['quantity']
        for identifier, count in sorted(demand.items()):
            p = self._call(self.catalogue_lookup, identifier)
            prefix = identifier.split(':', 1)[0]
            if (not p or p.get('id') != identifier or not isinstance(p.get('category'), str) or p.get('category') not in CATEGORIES
                    or (p['category'] != prefix if prefix != 'parts' else p['category'] in WHEELS)
                    or not _text(p.get('name'), 180) or not isinstance(p.get('identity_hash'), str)
                    or not HASH.fullmatch(p['identity_hash'])):
                block('catalogue_identity_unconfirmed', product_id=identifier)
                continue
            # Only store fields used by this boundary; arbitrary provider payloads stay private.
            p = {key: p.get(key) for key in ('id', 'name', 'category', 'identity_hash', 'fitment_required',
                 'fitment', 'new_part', 'painting_eligible', 'required_fees_confirmed', 'required_fees', 'diameter', 'tyre_size')}
            products[identifier] = p
            if p['category'] in WHEELS and (type(p.get('diameter')) is not int or not 8 <= p['diameter'] <= 30):
                block('product_dimensions_unconfirmed', product_id=identifier)
            fitment_required = p['fitment_required']
            if type(fitment_required) is not bool or p['category'] in WHEELS | {'body_part'} and not fitment_required:
                block('fitment_unconfirmed', product_id=identifier)
            elif fitment_required:
                fitment = p.get('fitment')
                if (not isinstance(fitment, Mapping) or fitment.get('confirmed') is not True
                        or fitment.get('identity_hash') != p['identity_hash'] or not vehicle_ref
                        or fitment.get('vehicle_ref') != vehicle_ref or not _text(fitment.get('evidence_id'))):
                    block('fitment_unconfirmed', product_id=identifier)
            fees = p.get('required_fees')
            if (p.get('required_fees_confirmed') is not True or not isinstance(fees, list)
                    or len(fees) > 10 or any(not isinstance(code, str) or not CODE.fullmatch(code) for code in fees)
                    or len(set(fees)) != len(fees)):
                block('required_fees_unconfirmed', product_id=identifier)
                p['required_fees'] = []
            mapping = self._call(self.rsu_mapping, p)
            if (not mapping or mapping.get('confirmed') is not True or mapping.get('product_id') != identifier
                    or mapping.get('identity_hash') != p['identity_hash'] or mapping.get('supplier') != 'RSU'
                    or not _text(mapping.get('supplier_article_id'), 100) or not _text(mapping.get('evidence_id'))):
                block('rsu_mapping_unconfirmed', product_id=identifier)
            else:
                mappings[identifier] = {key: mapping[key] for key in ('supplier_article_id', 'evidence_id')}
            price = self._call(self.retail_quote, p, count)
            until, error = self._current(price, now)
            if error:
                block(error, product_id=identifier, dependency='retail_quote')
            elif (price.get('product_id') != identifier or price.get('identity_hash') != p['identity_hash']
                    or type(price.get('quantity')) is not int or price['quantity'] != count):
                block('price_identity_unconfirmed', product_id=identifier)
            elif not _amount(price.get('unit_gross_cent')) or not _amount(price['unit_gross_cent'] * count):
                block('price_amount_unconfirmed', product_id=identifier)
            elif type(price.get('stock')) is not int or not count <= price['stock'] <= 1_000_000_000:
                block('stock_insufficient_or_unconfirmed', product_id=identifier)
            elif price.get('procurement_confirmed') is not True:
                block('procurement_price_unconfirmed', product_id=identifier)
            else:
                prices[identifier] = {'unit_gross_cent': price['unit_gross_cent'], 'stock': price['stock']}
                shorten(until)

        procurement = {}
        for identifier, mapping in mappings.items():
            article = mapping['supplier_article_id']
            if article in procurement and procurement[article] != identifier:
                block('duplicate_procurement_identity', product_id=identifier)
            procurement[article] = identifier

        grouped = {}
        for i, row in enumerate(rows):
            if row['assembly_group']:
                grouped.setdefault(row['assembly_group'], []).append(i)
        assembly_owner, valid_groups = {}, {}
        for group, indices in sorted(grouped.items()):
            members = [rows[i] for i in indices]
            selected = [products.get(row['id']) for row in members]
            tyre = next((p for p in selected if p and p['category'] == 'tyre'), None)
            rim = next((p for p in selected if p and p['category'] in RIMS), None)
            valid = (len(indices) == 2 and tyre is not None and rim is not None
                     and members[0]['quantity'] == members[1]['quantity']
                     and type(rim.get('diameter')) is int and rim['diameter'] == tyre.get('diameter'))
            if valid:
                proof = self._call(self.assembly_fitment, rim, tyre, vehicle_ref)
                tyre_size = tyre.get('tyre_size')
                size = re.fullmatch(r'[0-9]{3}/[0-9]{2,3}\s*R([0-9]{2})(?:\s+[A-Za-z0-9 +./()\-]+)?', tyre_size or '') if isinstance(tyre_size, str) else None
                valid = (proof is not None and proof.get('confirmed') is True and bool(vehicle_ref)
                         and proof.get('vehicle_ref') == vehicle_ref
                         and proof.get('rim_identity_hash') == rim['identity_hash']
                         and proof.get('tyre_identity_hash') == tyre['identity_hash']
                         and _text(tyre_size, 100) and size is not None and int(size.group(1)) == rim['diameter']
                         and proof.get('tyre_size') == tyre_size
                         and type(proof.get('rim_diameter')) is int and proof['rim_diameter'] == rim['diameter']
                         and _text(proof.get('evidence_id')))
            if not valid:
                block('assembly_fitment_unconfirmed', assembly_group=group)
                continue
            owner = next(i for i in indices if rows[i]['id'] == tyre['id'])
            if not any(rows[i]['services']['mounting'] for i in indices):
                block('assembly_mounting_unconfirmed', assembly_group=group)
            valid_groups[group] = {'rim_id': rim['id'], 'tyre_id': tyre['id'], 'evidence_id': proof['evidence_id']}
            assembly_owner[group] = owner

        priced_lines, shipments = [], {}
        known, known_any = 0, False
        for index, row in enumerate(rows):
            p, price = products.get(row['id']), prices.get(row['id'])
            options = row['services']
            if not p:
                continue
            destination = 'workshop' if (row['assembly_group'] or any(options[k] for k in ('mounting', 'painting', 'installation', 'oil_service')) or options['care']) else 'customer'
            item = {'id': row['id'], 'identity_hash': p['identity_hash'], 'name': p['name'].strip(),
                    'category': p['category'], 'quantity': row['quantity'], 'destination': destination,
                    'unit_gross_cent': price['unit_gross_cent'] if price else None,
                    'items_gross_cent': price['unit_gross_cent'] * row['quantity'] if price else None, 'services': []}
            if item['items_gross_cent'] is not None:
                known += item['items_gross_cent']
                known_any = True
            requests = []
            group = row['assembly_group']
            if options['mounting'] and not group:
                if p['category'] not in WHEELS:
                    block('service_not_applicable', line=index, service='mounting')
                else:
                    requests.append(('mounting', {'runflat': options['runflat'], 'disposal': options['disposal']}))
            elif not group and (options['runflat'] or options['disposal']):
                block('mounting_required_for_options', line=index)
            if group in assembly_owner and assembly_owner[group] == index and any(rows[i]['services']['mounting'] for i in grouped[group]):
                requests.append(('mounting', {**valid_groups[group],
                    'runflat': any(rows[i]['services']['runflat'] for i in grouped[group]),
                    'disposal': any(rows[i]['services']['disposal'] for i in grouped[group])}))
            for code in ('painting', 'installation', 'oil_service', 'care'):
                if not options[code]:
                    continue
                applicable = (code == 'painting' and p['category'] == 'body_part' and p['new_part'] is True and p['painting_eligible'] is True
                              or code == 'installation' and p['category'] in {'body_part', 'accessory'}
                              or code == 'oil_service' and p['category'] == 'oil'
                              or code == 'care' and p['category'] == 'care')
                if not applicable:
                    block('service_not_applicable', line=index, service=code)
                else:
                    request_options = {'paint_color': options['paint_color']} if code == 'painting' else {'scope': options['care']} if code == 'care' else {}
                    requests.append((code, request_options))
            requests.extend(('required:' + code, {}) for code in p['required_fees'])
            for code, service_options in requests:
                response = self._call(self.service_quote, code, p, row['quantity'], service_options)
                until, error = self._current(response, now)
                if error or (response.get('product_id') != p['id'] or response.get('identity_hash') != p['identity_hash']
                             or type(response.get('quantity')) is not int or response['quantity'] != row['quantity']
                             or response.get('options_hash') != _hash(service_options) or not _amount(response.get('gross_cent'), zero=True)):
                    block(error or 'service_quote_unconfirmed', line=index, service=code)
                    item['services'].append({'code': code, 'gross_cent': None})
                else:
                    shorten(until)
                    known += response['gross_cent']
                    known_any = True
                    item['services'].append({'code': code, 'gross_cent': response['gross_cent']})
            priced_lines.append(item)
            shipment = shipments.setdefault(destination, {'id': destination, 'destination': destination, 'items': [], 'private_items': [], 'gross_cent': None})
            shipment['items'].append({'id': p['id'], 'quantity': row['quantity']})
            shipment['private_items'].append({'id': p['id'], 'identity_hash': p['identity_hash'], 'quantity': row['quantity'], 'mapping': mappings.get(p['id'])})

        for shipment in shipments.values():
            aggregate = {}
            for item in shipment['private_items']:
                if item['id'] not in aggregate:
                    aggregate[item['id']] = deepcopy(item)
                else:
                    aggregate[item['id']]['quantity'] += item['quantity']
            shipment['private_items'] = [aggregate[key] for key in sorted(aggregate)]
            shipment['items'] = [{'id': item['id'], 'quantity': item['quantity']} for item in shipment['private_items']]
            shipment['context_hash'] = _hash(context)
            shipment['shipment_hash'] = _hash({key: shipment[key] for key in ('id', 'destination', 'private_items', 'context_hash')})
            response = self._call(self.freight_quote, json.loads(_json(shipment)), json.loads(context_json)) if all(item['mapping'] for item in shipment['private_items']) else None
            until, error = self._current(response, now)
            if error or (response.get('shipment_hash') != shipment['shipment_hash'] or response.get('context_hash') != shipment['context_hash']
                         or not _amount(response.get('gross_cent'), zero=True)):
                block(error or 'freight_unconfirmed', shipment_id=shipment['id'], dependency='freight')
            else:
                shorten(until)
                shipment['gross_cent'] = response['gross_cent']
                known += response['gross_cent']
                known_any = True
        if not _amount(known, zero=True):
            block('total_exceeds_limit')
            known = None
        value = {'version': 1, 'mode': self.mode, 'currency': 'eur', 'vat_percent': '19.00', 'customer_id': str(customer_id).strip(),
                 'vehicle_ref': vehicle_ref, 'request': rows, 'created_at': now.isoformat(), 'expires_at': expiry.isoformat(),
                 'ready': not reasons, 'known_subtotal_gross_cent': known if known_any else None, 'total_gross_cent': known if not reasons else None,
                 'blocked_reasons': reasons, 'lines': priced_lines, 'shipments': list(shipments.values()),
                 'assembly_evidence': valid_groups,
                 'product_checks': {key: {'fitment_evidence_id': value['fitment'].get('evidence_id') if isinstance(value['fitment'], Mapping) and _text(value['fitment'].get('evidence_id')) else None,
                                         'required_fees': value['required_fees']} for key, value in products.items()}}
        encoded = _json(value)
        return CartQuote(encoded, hashlib.sha256(encoded.encode('utf-8')).hexdigest())
