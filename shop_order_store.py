"""Durable mixed-cart order/payment journal; no network or provider operations.

Only a server-created CartQuote.snapshot() and a separately server-retained
expected_quote_hash may enter save_quote(). create_order() accepts a hash only
and loads the saved server record bound to the authenticated customer/mode.
Never accept a client-supplied snapshot. A ready quote freezes retail totals,
services, explicit freight and exact procurement mappings in one transaction.

Payment workflow: claim_payment() once, then call the external adapter using
the returned stable idempotency key. A lost response leaves the claim durable;
later callers receive can_create=False and must reconcile that same operation.
bind_payment() accepts a checked adapter response, including its exact binding.
Its initial intent may be None (open Checkout Session); trusted reconciliation
can attach that same Session's intent once, never substitute another one.
apply_verified_event() requires an explicit signature-verified event. For paid
events amount_gross_cent is the amount actually captured, not merely intended.
occurred_at must be the verified provider event timestamp, not receipt time.
No browser-return entrypoint, payment API, supplier order, capture or refund is
implemented here. Connections returned by the factory are owned and closed.
"""
from collections import Counter
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import hmac
import json
import re
import sqlite3
from uuid import uuid4
from shop_tyre_pricing import fixed_tyre_offer, tyre_unit_gross_cent
from shop_workshop_queue import HANDLED_SCHEMA

MAX_CENTS = 100_000_000
ID = re.compile(r'(?:tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}')
HASH = re.compile(r'[a-f0-9]{64}')
PRINCIPAL = re.compile(r'[A-Za-z0-9_:@.\-]{1,128}')
KEY = re.compile(r'[A-Za-z0-9_:\-]{8,128}')
ORDER_ID = re.compile(r'sho_[a-f0-9]{32}')
ATTEMPT_ID = re.compile(r'spa_[a-f0-9]{32}')
PROVIDER_ID = re.compile(r'[A-Za-z0-9_]{3,200}')
EVENT_KINDS = {'authorized', 'paid', 'failed', 'cancelled'}


class OrderStoreError(ValueError):
    """Fixed codes only; never include records, provider text or SQL messages."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False)


def _stamp(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise OrderStoreError('invalid_quote_time') from None


def _amount(value, zero=False):
    return type(value) is int and (0 if zero else 1) <= value <= MAX_CENTS


def _text(value, maximum=200):
    return (isinstance(value, str) and 0 < len(value.strip()) <= maximum
            and not any(ord(c) < 32 or ord(c) == 127 for c in value))


def _identity(customer_id, mode):
    if (not isinstance(customer_id, str) or not PRINCIPAL.fullmatch(customer_id)
            or not isinstance(mode, str) or mode not in {'test', 'live'}):
        raise OrderStoreError('invalid_order_identity')


def available_appointment_times(day, calendar):
    try:
        if not isinstance(day, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', day):
            raise ValueError()
        date.fromisoformat(day)
    except ValueError:
        raise OrderStoreError('invalid_appointment_preference') from None
    try:
        times = calendar(day)
        if not isinstance(times, (tuple, list)) or len(times) > 24 or not all(
                isinstance(slot, str) and re.fullmatch(r'(?:[01][0-9]|2[0-3]):00', slot) for slot in times):
            raise ValueError()
        return list(times)
    except Exception:
        raise OrderStoreError('appointment_service_unavailable') from None


def _priced_workshop_services(line):
    return {service['code'] for service in line.get('services', [])
            if isinstance(service, Mapping) and isinstance(service.get('code'), str)
            and _amount(service.get('gross_cent'), zero=True)}


def workshop_service_codes(snapshot):
    """Return only work selected by the customer and costed on the same quote."""
    if not isinstance(snapshot, Mapping):
        return {}
    lines, requests = snapshot.get('lines'), snapshot.get('request')
    if not isinstance(lines, list) or not isinstance(requests, list) or len(lines) != len(requests):
        return {}
    rows = list(zip(lines, requests))
    for line, item in rows:
        if (not isinstance(line, Mapping) or not isinstance(item, Mapping)
                or not isinstance(line.get('id'), str) or line['id'] != item.get('id')
                or not isinstance(line.get('services'), list) or not isinstance(item.get('services'), Mapping)
                or not isinstance(item.get('assembly_group', ''), str)):
            return {}

    # For complete wheels either component can carry the one installation charge.
    selected_groups = {item['assembly_group'] for line, item in rows
                       if item.get('assembly_group') and line.get('destination') == 'workshop'
                       and item['services'].get('mounting') is True}
    result = {}
    for index, (line, item) in enumerate(rows):
        if line.get('destination') != 'workshop':
            continue
        options = item['services']
        selected = {key for key in ('painting', 'installation', 'oil_service', 'mounting') if options.get(key) is True}
        if options.get('care') in ('inside', 'outside', 'complete'):
            selected.add('care')
        if item.get('assembly_group') in selected_groups:
            selected.add('mounting')
        booked = selected & _priced_workshop_services(line)
        if booked:
            result[index] = booked
    return result


def requires_workshop_order(snapshot):
    return bool(workshop_service_codes(snapshot))


def workshop_mounting_services(snapshot):
    """Map purchased tyre installation lines to their options by snapshot index."""
    rows = list(zip(snapshot['lines'], snapshot['request']))
    groups = {}
    for line, item in rows:
        if item['assembly_group']:
            groups.setdefault(item['assembly_group'], []).append((line, item))

    mounted_groups = {
        group for group, members in groups.items()
        if all(line['destination'] == 'workshop' for line, _ in members)
        and any(line['id'].startswith('tyre:') for line, _ in members)
        and any(item['services'].get('mounting') is True for _, item in members)
        and any('mounting' in _priced_workshop_services(line) for line, _ in members)
    }
    result = {}
    for index, (line, item) in enumerate(rows):
        group = item['assembly_group']
        tyre = line['id'].startswith('tyre:')
        if group in mounted_groups:
            services = ['Komplettradmontage'] if tyre else []
            options = [member['services'] for _, member in groups[group]]
        elif (not group and tyre and line['destination'] == 'workshop'
                and item['services'].get('mounting') is True and 'mounting' in _priced_workshop_services(line)):
            services = ['Reifenmontage']
            options = [item['services']]
        else:
            continue
        if services:
            for key, label in (('runflat', 'Runflat'), ('disposal', 'Altreifenentsorgung')):
                if any(selection.get(key) for selection in options):
                    services.append(label)
        result[index] = services
    return result


def workshop_mounting_items(snapshot):
    return [{'name': snapshot['lines'][index]['name'], 'quantity': snapshot['lines'][index]['quantity'],
             'services': services} for index, services in workshop_mounting_services(snapshot).items()]


def _workshop_tyre_mounting(snapshot):
    return bool(workshop_mounting_services(snapshot))


def _validate_snapshot(snapshot, expected_hash, customer_id, mode):
    _identity(customer_id, mode)
    if (not isinstance(snapshot, Mapping) or not isinstance(expected_hash, str)
            or not HASH.fullmatch(expected_hash)):
        raise OrderStoreError('invalid_quote_snapshot')
    value = dict(snapshot)
    supplied = value.pop('quote_hash', None)
    try:
        encoded = _json(value)
        encoded_bytes = encoded.encode('utf-8')
    except (TypeError, ValueError, OverflowError, UnicodeError):
        raise OrderStoreError('invalid_quote_snapshot') from None
    actual = sha256(encoded_bytes).hexdigest()
    if (len(encoded_bytes) > 256 * 1024 or not isinstance(supplied, str) or not HASH.fullmatch(supplied)
            or not hmac.compare_digest(actual, expected_hash)
            or not hmac.compare_digest(supplied, expected_hash)):
        raise OrderStoreError('quote_binding_mismatch')
    # Detach nested data before inspecting it; later caller mutation cannot
    # change either the values validated below or the canonical frozen record.
    value = json.loads(encoded)
    from shop_complete_wheels import is_review_snapshot
    review = value.get('version') == 2 and is_review_snapshot(value)
    if (type(value.get('version')) is not int or value['version'] not in (1, 2)
            or value['version'] == 2 and not review
            or value['version'] == 1 and any(key in value for key in
                ('authorization_ready', 'fulfillment_ready', 'review_requirements', 'assembly_proposals'))
            or value.get('ready') is not True or value.get('blocked_reasons') != []
            or value.get('customer_id') != customer_id or value.get('mode') != mode
            or value.get('currency') != 'eur' or value.get('vat_percent') != '19.00'
            or not _amount(value.get('total_gross_cent'))
            or type(value.get('known_subtotal_gross_cent')) is not int
            or value['known_subtotal_gross_cent'] != value['total_gross_cent']):
        raise OrderStoreError('quote_not_ready')
    start, expiry = _stamp(value.get('created_at')), _stamp(value.get('expires_at'))
    if not timedelta(0) < expiry - start <= timedelta(minutes=15):
        raise OrderStoreError('invalid_quote_time')
    lines, requests, shipments = value.get('lines'), value.get('request'), value.get('shipments')
    if (not isinstance(lines, list) or not 1 <= len(lines) <= 40
            or not isinstance(requests, list) or len(requests) != len(lines)
            or not isinstance(shipments, list) or not 1 <= len(shipments) <= 2):
        raise OrderStoreError('invalid_quote_lines')
    demand, identities, total, groups = Counter(), {}, 0, {}
    for line, requested in zip(lines, requests):
        if not isinstance(line, Mapping) or not isinstance(requested, Mapping):
            raise OrderStoreError('invalid_quote_lines')
        product, quantity = line.get('id'), line.get('quantity')
        if (not isinstance(product, str) or not ID.fullmatch(product) or type(quantity) is not int
                or not 1 <= quantity <= (20 if product.startswith('parts:') else 4)
                or requested.get('id') != product or type(requested.get('quantity')) is not int
                or requested['quantity'] != quantity or not isinstance(line.get('identity_hash'), str)
                or not HASH.fullmatch(line['identity_hash']) or line.get('destination') not in {'customer', 'workshop'}
                or not _amount(line.get('unit_gross_cent')) or not _amount(line.get('items_gross_cent'))
                or line['items_gross_cent'] != line['unit_gross_cent'] * quantity
                or not isinstance(requested.get('services'), Mapping)
                or not isinstance(requested.get('assembly_group'), str)):
            raise OrderStoreError('invalid_quote_lines')
        if 'tyre_price_policy' in line:
            if (line.get('category') != 'tyre' or not _amount(line.get('base_unit_gross_cent'))
                    or (line['tyre_price_policy'], line['unit_gross_cent']) != fixed_tyre_offer(product, line['identity_hash'])):
                raise OrderStoreError('invalid_quote_lines')
        elif 'base_unit_gross_cent' in line:
            base = line['base_unit_gross_cent']
            if (line.get('category') != 'tyre' or not product.startswith('tyre:')
                    or not _amount(base) or tyre_unit_gross_cent(base) != line['unit_gross_cent']):
                raise OrderStoreError('invalid_quote_lines')
        if product in identities and identities[product] != line['identity_hash']:
            raise OrderStoreError('quote_identity_conflict')
        identities[product] = line['identity_hash']
        demand[(line['destination'], product)] += quantity
        total += line['items_gross_cent']
        services = line.get('services')
        if not isinstance(services, list) or len(services) > 20:
            raise OrderStoreError('service_cost_unconfirmed')
        codes = set()
        for service in services:
            if (not isinstance(service, Mapping) or not _text(service.get('code'), 80)
                    or service['code'] in codes or not _amount(service.get('gross_cent'), zero=True)):
                raise OrderStoreError('service_cost_unconfirmed')
            codes.add(service['code'])
            total += service['gross_cent']
        options = requested['services']
        required = {code for code in ('painting', 'installation', 'oil_service', 'care') if options.get(code)}
        group = requested['assembly_group']
        if group:
            groups.setdefault(group, []).append((line, requested, codes))
        elif options.get('mounting'):
            required.add('mounting')
        checks = value.get('product_checks')
        check = checks.get(product) if isinstance(checks, Mapping) else None
        if not isinstance(check, Mapping) or not isinstance(check.get('required_fees'), list):
            raise OrderStoreError('service_cost_unconfirmed')
        if any(not _text(code, 40) for code in check['required_fees']):
            raise OrderStoreError('service_cost_unconfirmed')
        required.update('required:' + code for code in check['required_fees'])
        if not required <= codes:
            raise OrderStoreError('service_cost_unconfirmed')
    for group, members in groups.items():
        evidence = value.get('assembly_evidence')
        mounting = any(requested['services'].get('mounting') for _, requested, _ in members)
        pending = review and group in value['assembly_proposals']
        if (len(members) != 2 or not isinstance(evidence, Mapping) or group not in evidence and not pending
                or not mounting
                or sum('mounting' in codes for _, _, codes in members) != int(mounting)):
            raise OrderStoreError('service_cost_unconfirmed')
    seen_groups, shipped = set(), Counter()
    for group in shipments:
        if (not isinstance(group, Mapping) or group.get('id') not in {'customer', 'workshop'}
                or group['id'] in seen_groups or group.get('destination') != group['id']
                or not _amount(group.get('gross_cent'), zero=True)
                or any(not isinstance(group.get(key), str) or not HASH.fullmatch(group[key])
                       for key in ('context_hash', 'shipment_hash'))):
            raise OrderStoreError('freight_unconfirmed')
        seen_groups.add(group['id'])
        items, private_items = group.get('items'), group.get('private_items')
        if (not isinstance(items, list) or not isinstance(private_items, list)
                or not 1 <= len(items) <= 40 or len(items) != len(private_items)):
            raise OrderStoreError('procurement_unconfirmed')
        seen_items = set()
        for item, private in zip(items, private_items):
            if not isinstance(item, Mapping) or not isinstance(private, Mapping):
                raise OrderStoreError('procurement_unconfirmed')
            product, quantity = item.get('id'), item.get('quantity')
            mapping = private.get('mapping')
            if (product not in identities or product in seen_items or type(quantity) is not int or quantity <= 0
                    or private.get('id') != product or type(private.get('quantity')) is not int
                    or private['quantity'] != quantity or private.get('identity_hash') != identities[product]
                    or not isinstance(mapping, Mapping)
                    or not _text(mapping.get('supplier_article_id'), 100) or not _text(mapping.get('evidence_id'), 200)):
                raise OrderStoreError('procurement_unconfirmed')
            seen_items.add(product)
            shipped[(group['id'], product)] += quantity
        total += group['gross_cent']
    if demand != shipped or not _amount(total) or total != value['total_gross_cent']:
        raise OrderStoreError('quote_total_mismatch')
    return encoded, start, expiry


def _delivery_context(snapshot, context):
    """Freeze caller-validated addresses exactly; never read a mutable account."""
    try:
        encoded = _json(context)
        if len(encoded.encode('utf-8')) > 16384:
            raise ValueError()
    except (TypeError, ValueError, UnicodeError):
        raise OrderStoreError('delivery_context_mismatch') from None
    digest = sha256(encoded.encode('utf-8')).hexdigest()
    if (any(group['context_hash'] != digest for group in snapshot['shipments'])
            or any(group['destination'] == 'customer' for group in snapshot['shipments'])
            and (not isinstance(context, Mapping) or not context)):
        raise OrderStoreError('delivery_context_mismatch')
    return encoded


def init_schema(connection):
    """Create isolated tables on a dedicated setup connection, never on import."""
    connection.executescript('''
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS shop_quotes_v2 (
            quote_hash TEXT PRIMARY KEY, customer_id TEXT NOT NULL, mode TEXT NOT NULL,
            snapshot_json TEXT NOT NULL, delivery_context_json TEXT NOT NULL,
            created_at TEXT NOT NULL, expires_at TEXT NOT NULL,
            CHECK(mode IN ('test','live'))
        );
        CREATE TRIGGER IF NOT EXISTS shop_quotes_v2_immutable
        BEFORE UPDATE ON shop_quotes_v2 BEGIN SELECT RAISE(ABORT,'immutable_quote'); END;
        CREATE TABLE IF NOT EXISTS shop_quote_goods_cost_v1 (
            quote_hash TEXT PRIMARY KEY REFERENCES shop_quotes_v2(quote_hash),
            cost_net_cent INTEGER NOT NULL CHECK(cost_net_cent > 0)
        );
        CREATE TABLE IF NOT EXISTS shop_orders_v2 (
            id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, mode TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE, quote_hash TEXT NOT NULL UNIQUE,
            snapshot_json TEXT NOT NULL, delivery_context_json TEXT NOT NULL,
            amount_gross_cent INTEGER NOT NULL,
            currency TEXT NOT NULL, quote_created_at TEXT NOT NULL,
            quote_expires_at TEXT NOT NULL, created_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft',
            CHECK(mode IN ('test','live')), CHECK(currency='eur'),
            CHECK(amount_gross_cent>0 AND amount_gross_cent<=100000000)
        );
        CREATE TRIGGER IF NOT EXISTS shop_orders_v2_immutable
        BEFORE UPDATE OF customer_id,mode,idempotency_key,quote_hash,snapshot_json,delivery_context_json,
            amount_gross_cent,currency,quote_created_at,quote_expires_at,created_at
        ON shop_orders_v2 BEGIN SELECT RAISE(ABORT,'immutable_order'); END;
        CREATE TABLE IF NOT EXISTS shop_order_appointment_requests_v2 (
            order_id TEXT PRIMARY KEY REFERENCES shop_orders_v2(id),
            requested_date TEXT NOT NULL, requested_time TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shop_payment_attempts_v2 (
            id TEXT PRIMARY KEY, order_id TEXT NOT NULL UNIQUE REFERENCES shop_orders_v2(id),
            idempotency_key TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
            session_id TEXT UNIQUE, intent_id TEXT UNIQUE,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, last_event_at TEXT
        );
        CREATE TABLE IF NOT EXISTS shop_payment_events_v2 (
            id TEXT PRIMARY KEY, order_id TEXT NOT NULL REFERENCES shop_orders_v2(id),
            attempt_id TEXT NOT NULL REFERENCES shop_payment_attempts_v2(id),
            fingerprint TEXT NOT NULL, kind TEXT NOT NULL, received_at TEXT NOT NULL
        );
    ''')
    connection.execute(HANDLED_SCHEMA)
    connection.execute('CREATE INDEX IF NOT EXISTS shop_orders_workshop_status_idx ON shop_orders_v2(mode,status,id)')


class OrderStore:
    def __init__(self, connection_factory, *, clock=None):
        if not callable(connection_factory):
            raise OrderStoreError('storage_unavailable')
        self._connect = connection_factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _now(self):
        return _stamp(self._clock())

    @contextmanager
    def _connection(self, *, write=False):
        connection = None
        try:
            connection = self._connect()
            if not isinstance(connection, sqlite3.Connection) or connection.in_transaction:
                raise OrderStoreError('storage_unavailable')
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA foreign_keys=ON')
            if write:
                connection.execute('BEGIN IMMEDIATE')
            yield connection
            if write:
                connection.commit()
        except OrderStoreError:
            if isinstance(connection, sqlite3.Connection) and connection.in_transaction:
                connection.rollback()
            raise
        except Exception:
            if isinstance(connection, sqlite3.Connection) and connection.in_transaction:
                connection.rollback()
            raise OrderStoreError('storage_unavailable') from None
        finally:
            if isinstance(connection, sqlite3.Connection):
                connection.close()

    @staticmethod
    def _row(db, order_id, customer_id, mode):
        _identity(customer_id, mode)
        if not isinstance(order_id, str) or not ORDER_ID.fullmatch(order_id):
            raise OrderStoreError('order_not_found')
        row = db.execute('SELECT * FROM shop_orders_v2 WHERE id=? AND customer_id=? AND mode=?',
                         (order_id, customer_id, mode)).fetchone()
        if row is None:
            raise OrderStoreError('order_not_found')
        return row

    @staticmethod
    def _public_order(row):
        return {key: row[key] for key in ('id', 'customer_id', 'mode', 'quote_hash',
                'amount_gross_cent', 'currency', 'created_at', 'quote_expires_at', 'status')}

    def save_quote(self, snapshot, *, expected_quote_hash, customer_id, mode, delivery_context=None,
                   goods_cost_net_cent=None):
        encoded, start, expiry = _validate_snapshot(snapshot, expected_quote_hash, customer_id, mode)
        context_json = _delivery_context(json.loads(encoded), delivery_context)
        with self._connection(write=True) as db:
            if not start <= self._now() < expiry:
                raise OrderStoreError('quote_expired')
            prior = db.execute('SELECT * FROM shop_quotes_v2 WHERE quote_hash=?', (expected_quote_hash,)).fetchone()
            if prior is not None:
                if (prior['customer_id'] != customer_id or prior['mode'] != mode
                        or prior['snapshot_json'] != encoded or prior['delivery_context_json'] != context_json):
                    raise OrderStoreError('quote_binding_mismatch')
            else:
                db.execute('INSERT INTO shop_quotes_v2 VALUES (?,?,?,?,?,?,?)',
                           (expected_quote_hash, customer_id, mode, encoded, context_json, start.isoformat(), expiry.isoformat()))
                if type(goods_cost_net_cent) is int and goods_cost_net_cent > 0:
                    db.execute('INSERT INTO shop_quote_goods_cost_v1 (quote_hash,cost_net_cent) VALUES (?,?)',
                               (expected_quote_hash, goods_cost_net_cent))
        return {'quote_hash': expected_quote_hash, 'expires_at': expiry.isoformat(),
                'total_gross_cent': json.loads(encoded)['total_gross_cent'], 'currency': 'eur'}

    @staticmethod
    def _saved_quote(db, quote_hash, customer_id, mode):
        _identity(customer_id, mode)
        if not isinstance(quote_hash, str) or not HASH.fullmatch(quote_hash):
            raise OrderStoreError('quote_not_found')
        row = db.execute('SELECT * FROM shop_quotes_v2 WHERE quote_hash=? AND customer_id=? AND mode=?',
                         (quote_hash, customer_id, mode)).fetchone()
        if row is None:
            raise OrderStoreError('quote_not_found')
        snapshot = {**json.loads(row['snapshot_json']), 'quote_hash': quote_hash}
        encoded, start, expiry = _validate_snapshot(snapshot, quote_hash, customer_id, mode)
        context_json = _delivery_context(snapshot, json.loads(row['delivery_context_json']))
        return snapshot, encoded, context_json, start, expiry

    def lookup_server_quote(self, quote_hash, *, customer_id, mode):
        with self._connection() as db:
            snapshot, _, _, start, expiry = self._saved_quote(db, quote_hash, customer_id, mode)
            if not start <= self._now() < expiry:
                raise OrderStoreError('quote_expired')
            return snapshot

    def _order_with_preference(self, db, row):
        order = self._public_order(row)
        wish = db.execute('''SELECT requested_date, requested_time FROM shop_order_appointment_requests_v2
                             WHERE order_id=?''', (row['id'],)).fetchone()
        if wish:
            order['appointment_preference'] = {'date': wish['requested_date'], 'time': wish['requested_time'],
                                               'status': 'requested'}
        return order

    def _replayed_order(self, db, idempotency_key, customer_id, mode, quote_hash, encoded, wish):
        prior = db.execute('SELECT * FROM shop_orders_v2 WHERE idempotency_key=?', (idempotency_key,)).fetchone()
        if prior is None:
            return None
        if (prior['customer_id'] != customer_id or prior['mode'] != mode
                or prior['quote_hash'] != quote_hash or prior['snapshot_json'] != encoded):
            raise OrderStoreError('idempotency_conflict')
        order = self._order_with_preference(db, prior)
        if wish is not None and order.get('appointment_preference') != {**wish, 'status': 'requested'}:
            raise OrderStoreError('appointment_preference_conflict')
        return order

    def create_order(self, quote_hash, *, customer_id, mode, idempotency_key,
                     appointment_preference=None, appointment_calendar=None):
        if not isinstance(idempotency_key, str) or not KEY.fullmatch(idempotency_key):
            raise OrderStoreError('invalid_idempotency_key')
        calendar_error = None
        if appointment_preference is not None:
            if (not isinstance(appointment_preference, dict) or set(appointment_preference) != {'date', 'time'}
                    or not isinstance(appointment_preference['date'], str)
                    or not isinstance(appointment_preference['time'], str)
                    or not re.fullmatch(r'(?:[01][0-9]|2[0-3]):00', appointment_preference['time'])):
                raise OrderStoreError('invalid_appointment_preference')
            with self._connection() as db:
                snapshot, encoded, context_json, start, expiry = self._saved_quote(db, quote_hash, customer_id, mode)
                prior = self._replayed_order(db, idempotency_key, customer_id, mode, quote_hash, encoded, appointment_preference)
                if prior is not None:
                    return prior
                if not start <= self._now() < expiry:
                    raise OrderStoreError('quote_expired')
                delivery = json.loads(context_json)
                if (not isinstance(delivery, dict) or delivery.get('workshop_delivery', 'collection') != 'collection'
                        or not _workshop_tyre_mounting(snapshot)):
                    raise OrderStoreError('appointment_not_eligible')
            day, time = appointment_preference['date'], appointment_preference['time']
            # The portal calendar may use a separate database. Wait for its
            # answer without holding the shop lock; the preference is no hold.
            try:
                if time not in available_appointment_times(day, appointment_calendar):
                    calendar_error = OrderStoreError('appointment_slot_unavailable')
            except OrderStoreError as error:
                calendar_error = error
        with self._connection(write=True) as db:
            snapshot, encoded, context_json, start, expiry = self._saved_quote(db, quote_hash, customer_id, mode)
            prior = self._replayed_order(db, idempotency_key, customer_id, mode, quote_hash, encoded, appointment_preference)
            if prior is not None:
                return prior
            if db.execute('SELECT 1 FROM shop_orders_v2 WHERE quote_hash=?', (quote_hash,)).fetchone():
                raise OrderStoreError('quote_already_ordered')
            now = self._now()
            if not start <= now < expiry:
                raise OrderStoreError('quote_expired')
            if calendar_error is not None:
                raise calendar_error
            order_id = 'sho_' + uuid4().hex
            db.execute('''INSERT INTO shop_orders_v2
                (id,customer_id,mode,idempotency_key,quote_hash,snapshot_json,delivery_context_json,amount_gross_cent,
                 currency,quote_created_at,quote_expires_at,created_at,status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'draft')''',
                (order_id, customer_id, mode, idempotency_key, quote_hash, encoded, context_json,
                 snapshot['total_gross_cent'], 'eur', start.isoformat(), expiry.isoformat(), now.isoformat()))
            if appointment_preference is not None:
                db.execute('''INSERT INTO shop_order_appointment_requests_v2
                              (order_id, requested_date, requested_time) VALUES (?,?,?)''', (order_id, day, time))
            return self._order_with_preference(db, self._row(db, order_id, customer_id, mode))

    def get_order(self, order_id, *, customer_id, mode):
        with self._connection() as db:
            return self._order_with_preference(db, self._row(db, order_id, customer_id, mode))

    def purchase_conversion(self, order_id, *, customer_id, mode):
        """Read verified sale evidence for this owner; never reconcile or pay."""
        from shop_measurement import purchase_projection
        with self._connection() as db:
            # One read snapshot keeps order status, paid event and capture
            # journal mutually consistent while the payment worker advances.
            db.execute('BEGIN')
            row = self._row(db, order_id, customer_id, mode)
            return purchase_projection(db, row)

    def lookup_payment_owner(self, order_id, *, mode):
        """Internal signed-webhook resolver, NEVER expose as an HTTP lookup.

        Resolve the owner from our journal, not event metadata. This is not
        payment proof; checkout still verifies the exact order/attempt binding.
        """
        if (not isinstance(order_id, str) or not ORDER_ID.fullmatch(order_id)
                or not isinstance(mode, str) or mode not in {'test', 'live'}):
            raise OrderStoreError('order_not_found')
        with self._connection() as db:
            row = db.execute('SELECT customer_id,mode FROM shop_orders_v2 WHERE id=? AND mode=?',
                             (order_id, mode)).fetchone()
            if row is None:
                raise OrderStoreError('order_not_found')
            return {'customer_id': row['customer_id'], 'mode': row['mode']}

    def snapshot_for_order(self, order_id, *, customer_id, mode):
        """Private server record, never a public HTTP order representation."""
        with self._connection() as db:
            row = self._row(db, order_id, customer_id, mode)
            return {**json.loads(row['snapshot_json']), 'quote_hash': row['quote_hash']}

    def delivery_context_for_order(self, order_id, *, customer_id, mode):
        """Private frozen context; caller separately validated its addresses."""
        with self._connection() as db:
            return json.loads(self._row(db, order_id, customer_id, mode)['delivery_context_json'])

    def claim_payment(self, order_id, *, customer_id, mode):
        with self._connection(write=True) as db:
            order = self._row(db, order_id, customer_id, mode)
            prior = db.execute('SELECT * FROM shop_payment_attempts_v2 WHERE order_id=?', (order_id,)).fetchone()
            created = prior is None
            if created:
                now = self._now()
                if order['status'] != 'draft' or not _stamp(order['quote_created_at']) <= now < _stamp(order['quote_expires_at']):
                    raise OrderStoreError('quote_expired_or_order_closed')
                db.execute('''INSERT INTO shop_payment_attempts_v2
                    (id,order_id,idempotency_key,state,created_at,updated_at) VALUES (?,?,?,'claimed',?,?)''',
                    ('spa_' + uuid4().hex, order_id, 'shop-checkout-v1:' + order_id, now.isoformat(), now.isoformat()))
                db.execute("UPDATE shop_orders_v2 SET status='payment_pending' WHERE id=?", (order_id,))
                prior = db.execute('SELECT * FROM shop_payment_attempts_v2 WHERE order_id=?', (order_id,)).fetchone()
            return {**dict(prior), 'can_create': created,
                    'reconcile_required': not created and (prior['state'] in {'claimed', 'unknown'}
                        or prior['state'] == 'bound' and prior['intent_id'] is None),
                    'amount_gross_cent': order['amount_gross_cent'], 'currency': order['currency'], 'mode': order['mode']}

    def mark_payment_unknown(self, order_id, attempt_id, *, customer_id, mode):
        with self._connection(write=True) as db:
            self._row(db, order_id, customer_id, mode)
            cursor = db.execute("UPDATE shop_payment_attempts_v2 SET state='unknown',updated_at=? WHERE id=? AND order_id=? AND state='claimed'",
                                (self._now().isoformat(), attempt_id, order_id))
            if cursor.rowcount == 0 and db.execute('SELECT 1 FROM shop_payment_attempts_v2 WHERE id=? AND order_id=?', (attempt_id, order_id)).fetchone() is None:
                raise OrderStoreError('payment_attempt_not_found')

    def bind_payment(self, order_id, attempt_id, *, customer_id, mode, session_id, intent_id,
                     amount_gross_cent, currency):
        _identity(customer_id, mode)
        if (not isinstance(attempt_id, str) or not ATTEMPT_ID.fullmatch(attempt_id)
                or not isinstance(session_id, str) or not PROVIDER_ID.fullmatch(session_id)
                or not session_id.startswith('cs_' + mode + '_')
                or (intent_id is not None and (not isinstance(intent_id, str) or not PROVIDER_ID.fullmatch(intent_id) or not intent_id.startswith('pi_')))
                or not _amount(amount_gross_cent) or currency != 'eur'):
            raise OrderStoreError('payment_binding_mismatch')
        with self._connection(write=True) as db:
            order = self._row(db, order_id, customer_id, mode)
            attempt = db.execute('SELECT * FROM shop_payment_attempts_v2 WHERE id=? AND order_id=?', (attempt_id, order_id)).fetchone()
            if attempt is None or order['amount_gross_cent'] != amount_gross_cent:
                raise OrderStoreError('payment_binding_mismatch')
            if attempt['session_id'] is not None:
                if (attempt['session_id'] != session_id
                        or attempt['intent_id'] is not None and intent_id not in (None, attempt['intent_id'])):
                    raise OrderStoreError('payment_binding_mismatch')
                if attempt['intent_id'] is not None or intent_id is None:
                    return dict(attempt)
                if attempt['state'] == 'cancelled':
                    raise OrderStoreError('payment_binding_mismatch')
                # Exact checked Session binding already exists; a trusted
                # adapter retrieval can learn its intent once after creation.
                try:
                    db.execute('UPDATE shop_payment_attempts_v2 SET intent_id=?,updated_at=? WHERE id=?',
                               (intent_id, self._now().isoformat(), attempt_id))
                except sqlite3.IntegrityError:
                    raise OrderStoreError('payment_binding_conflict') from None
                return dict(db.execute('SELECT * FROM shop_payment_attempts_v2 WHERE id=?', (attempt_id,)).fetchone())
            if attempt['state'] not in {'claimed', 'unknown'}:
                raise OrderStoreError('payment_binding_mismatch')
            try:
                db.execute("UPDATE shop_payment_attempts_v2 SET state='bound',session_id=?,intent_id=?,updated_at=? WHERE id=?",
                           (session_id, intent_id, self._now().isoformat(), attempt_id))
            except sqlite3.IntegrityError:
                raise OrderStoreError('payment_binding_conflict') from None
            return dict(db.execute('SELECT * FROM shop_payment_attempts_v2 WHERE id=?', (attempt_id,)).fetchone())

    def apply_verified_event(self, event, *, verified=False):
        fields = {'event_id', 'order_id', 'attempt_id', 'mode', 'currency', 'amount_gross_cent', 'session_id', 'intent_id', 'kind', 'occurred_at'}
        if verified is not True or not isinstance(event, Mapping) or set(event) != fields:
            raise OrderStoreError('unverified_payment_event')
        try:
            event = json.loads(_json(dict(event)))
        except (TypeError, ValueError, OverflowError):
            raise OrderStoreError('payment_event_mismatch') from None
        if (not isinstance(event['event_id'], str) or not PROVIDER_ID.fullmatch(event['event_id'])
                or not isinstance(event['kind'], str) or event['kind'] not in EVENT_KINDS
                or not isinstance(event['mode'], str) or event['mode'] not in {'test', 'live'}
                or event['currency'] != 'eur' or not _amount(event['amount_gross_cent'])
                or not isinstance(event['order_id'], str) or not ORDER_ID.fullmatch(event['order_id'])
                or not isinstance(event['attempt_id'], str) or not ATTEMPT_ID.fullmatch(event['attempt_id'])
                or not isinstance(event['session_id'], str) or not PROVIDER_ID.fullmatch(event['session_id'])
                or not (event['intent_id'] is None and event['kind'] == 'cancelled'
                    or isinstance(event['intent_id'], str) and PROVIDER_ID.fullmatch(event['intent_id']))):
            raise OrderStoreError('payment_event_mismatch')
        fingerprint = sha256(_json(dict(event)).encode()).hexdigest()
        with self._connection(write=True) as db:
            now = self._now()
            try:
                occurred = _stamp(event['occurred_at'])
            except OrderStoreError:
                raise OrderStoreError('payment_event_mismatch') from None
            if occurred > now + timedelta(minutes=2):
                raise OrderStoreError('payment_event_mismatch')
            prior = db.execute('SELECT * FROM shop_payment_events_v2 WHERE id=?', (event['event_id'],)).fetchone()
            if prior is not None:
                if prior['fingerprint'] != fingerprint:
                    raise OrderStoreError('payment_event_conflict')
                order = db.execute('SELECT * FROM shop_orders_v2 WHERE id=?', (prior['order_id'],)).fetchone()
                return self._public_order(order)
            order = db.execute('SELECT * FROM shop_orders_v2 WHERE id=?', (event['order_id'],)).fetchone()
            attempt = db.execute('SELECT * FROM shop_payment_attempts_v2 WHERE id=? AND order_id=?',
                                 (event['attempt_id'], event['order_id'])).fetchone()
            if (order is None or attempt is None or attempt['session_id'] is None
                    or order['mode'] != event['mode'] or order['currency'] != event['currency']
                    or order['amount_gross_cent'] != event['amount_gross_cent']
                    or attempt['session_id'] != event['session_id'] or attempt['intent_id'] != event['intent_id']):
                raise OrderStoreError('payment_event_mismatch')
            if occurred < _stamp(order['created_at']) - timedelta(minutes=2):
                raise OrderStoreError('payment_event_mismatch')
            state = {'authorized': 'authorized', 'paid': 'paid', 'failed': 'payment_failed', 'cancelled': 'cancelled'}[event['kind']]
            previous = _stamp(attempt['last_event_at']) if attempt['last_event_at'] else None
            rank = {'draft': 0, 'payment_pending': 0, 'payment_failed': 1,
                    'authorized': 2, 'cancelled': 3, 'paid': 4}
            newer = previous is None or occurred > previous or occurred == previous and rank[state] > rank.get(order['status'], 0)
            # Paid is never downgraded; confirmed captured funds are retained even
            # if a delayed success arrives after an earlier failure notification.
            if order['status'] != 'paid' and (state == 'paid' or order['status'] != 'cancelled' and newer):
                db.execute('UPDATE shop_orders_v2 SET status=? WHERE id=?', (state, order['id']))
                db.execute('UPDATE shop_payment_attempts_v2 SET state=?,updated_at=?,last_event_at=? WHERE id=?',
                           (state, now.isoformat(), max(occurred, previous or occurred).isoformat(), attempt['id']))
            db.execute('INSERT INTO shop_payment_events_v2 VALUES (?,?,?,?,?,?)',
                       (event['event_id'], order['id'], attempt['id'], fingerprint, event['kind'], now.isoformat()))
            return self._public_order(db.execute('SELECT * FROM shop_orders_v2 WHERE id=?', (order['id'],)).fetchone())
