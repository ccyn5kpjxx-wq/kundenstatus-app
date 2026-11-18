"""Disabled procurement saga with one durable single-position POST per item.

No credentials, provider, filesystem or background-worker calls on import or
construction. Callers inject reviewed adapters and a deliberately provisioned
SQLite factory. This module never treats an RSU ACK as echoed/delivered facts.
It accepts only request-bound ACKs under the separate written-policy adapter.

fresh_inquiry(item, exact_shipment_quantity) must perform a NEW read, returning
confirmed/product_id/identity_hash/supplier_article_id/mapping_evidence_id/
expected_ean/ean/quantity/unit_net/
currency='EUR'/mode/stock/observed_at/expires_at. Evidence predating this step
is rejected. item is an exact frozen private shipment item (with mapping).
revalidate(snapshot, delivery_context, destinations=tuple) returns a NEW server
CartQuote.snapshot for only the original requests in the remaining destinations,
including current fitment, mandatory services/fees and freight. It may not
change the customer's frozen identities, options, destinations or mappings.
delivery_address(destination, context) supplies a reviewed RSU address; customer
delivery must be neutral. No mutable customer/account address is read here.

payment_actions supplies authorization/capture/cancel(order_id). It owns the
separate durable single payment-action claim. Its proof_resolver must be this
saga's proof_resolver, never a caller-supplied dictionary. Transport authorization
must call authorize_submission, which consumes a second one-use DB grant. Each
frozen private item/destination has its own child and stable reference. Legacy
per-shipment claims never become new position grants.
"""
from collections import Counter
from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import re
import sqlite3

from shop_cart_quotes import retail_unit_gross_cent
from shop_order_store import _validate_snapshot, _delivery_context
from shop_rsu_orders import freeze_order_request, request_sha256
from shop_order_review import requires_review

TTL = timedelta(minutes=15)
HASH = re.compile(r'[a-f0-9]{64}')
ORDER = re.compile(r'sho_[a-f0-9]{32}')
OWNER = re.compile(r'[A-Za-z0-9_:@.\-]{1,128}')
PAYMENT_FIELDS = ('order_id', 'mode', 'currency', 'quote_hash', 'amount_gross_cent',
                  'payment_attempt', 'session_id', 'intent_id')
POLICY_FIELDS = {'account_fingerprint', 'mode_evidence_id', 'acceptance_evidence_id'}


class FulfillmentError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _hash(value):
    return sha256(_json(value).encode('utf-8')).hexdigest()


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise FulfillmentError('fulfillment_unavailable') from None


def _identity(order_id, customer_id, mode):
    if (not isinstance(order_id, str) or not ORDER.fullmatch(order_id)
            or not isinstance(customer_id, str) or not OWNER.fullmatch(customer_id)
            or not isinstance(mode, str) or mode not in {'test', 'live'}):
        raise FulfillmentError('invalid_fulfillment_identity')


def init_fulfillment_schema(connection):
    """Explicit provisioning only; may share the dedicated shop SQLite DB."""
    connection.executescript('''
        PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS shop_fulfillment_v2 (
          order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, mode TEXT NOT NULL,
          binding_json TEXT NOT NULL, plan_hash TEXT NOT NULL, lines_json TEXT NOT NULL,
          policy_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'open',
          reason TEXT, action_state TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS shop_fulfillment_identity_immutable_v2
          BEFORE UPDATE OF order_id,customer_id,mode,binding_json,plan_hash,lines_json,policy_json,created_at
          ON shop_fulfillment_v2 BEGIN SELECT RAISE(ABORT,'immutable_fulfillment'); END;
        CREATE TABLE IF NOT EXISTS shop_fulfillment_positions_v3 (
          order_id TEXT NOT NULL REFERENCES shop_fulfillment_v2(order_id),
          child_key TEXT NOT NULL, destination TEXT NOT NULL, item_json TEXT NOT NULL,
          reference TEXT NOT NULL UNIQUE,
          state TEXT NOT NULL DEFAULT 'pending', request_hash TEXT, payload_json TEXT,
          grant_used INTEGER NOT NULL DEFAULT 0, receipt_json TEXT, authorization_json TEXT,
          updated_at TEXT NOT NULL, PRIMARY KEY(order_id,child_key));
        CREATE TRIGGER IF NOT EXISTS shop_fulfillment_claim_immutable_v3
          BEFORE UPDATE OF request_hash,payload_json,authorization_json
          ON shop_fulfillment_positions_v3 WHEN OLD.state != 'pending'
          BEGIN SELECT RAISE(ABORT,'immutable_supplier_claim'); END;
        CREATE TRIGGER IF NOT EXISTS shop_fulfillment_position_identity_immutable_v3
          BEFORE UPDATE OF child_key,destination,item_json,reference
          ON shop_fulfillment_positions_v3
          BEGIN SELECT RAISE(ABORT,'immutable_supplier_position'); END;
    ''')
    connection.commit()


class FulfillmentJournal:
    def __init__(self, connection_factory, *, clock=None):
        if not callable(connection_factory):
            raise FulfillmentError('invalid_fulfillment_configuration')
        self.factory, self.clock = connection_factory, clock or (lambda: datetime.now(timezone.utc))

    @contextmanager
    def _connection(self, *, write=False):
        db = None
        try:
            db = self.factory()
            if not isinstance(db, sqlite3.Connection):
                raise ValueError()
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA foreign_keys=ON')
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            if write:
                db.commit()
        except FulfillmentError:
            if db is not None:
                db.rollback()
            raise
        except Exception:
            if isinstance(db, sqlite3.Connection):
                db.rollback()
            raise FulfillmentError('fulfillment_storage_unavailable') from None
        finally:
            if isinstance(db, sqlite3.Connection):
                db.close()

    def _now(self):
        return _stamp(self.clock()).isoformat()

    @staticmethod
    def _row(db, order_id, customer_id, mode):
        _identity(order_id, customer_id, mode)
        row = db.execute('SELECT * FROM shop_fulfillment_v2 WHERE order_id=? AND customer_id=? AND mode=?',
                         (order_id, customer_id, mode)).fetchone()
        if row is None:
            raise FulfillmentError('fulfillment_not_found')
        return row

    def ensure(self, binding, *, plan_hash, lines, policy, children):
        owner, order_id, mode = binding['customer_id'], binding['order_id'], binding['mode']
        _identity(order_id, owner, mode)
        encoded, line_json, policy_json = _json(binding), _json(lines), _json(policy)
        with self._connection(write=True) as db:
            legacy = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='shop_fulfillment_shipments_v2'").fetchone()
            if legacy and db.execute('SELECT 1 FROM shop_fulfillment_shipments_v2 WHERE order_id=?', (order_id,)).fetchone():
                raise FulfillmentError('legacy_supplier_claim_requires_review')
            row = db.execute('SELECT * FROM shop_fulfillment_v2 WHERE order_id=?', (order_id,)).fetchone()
            if row is not None:
                if (row['customer_id'] != owner or row['mode'] != mode or row['binding_json'] != encoded
                        or row['plan_hash'] != plan_hash or row['lines_json'] != line_json or row['policy_json'] != policy_json):
                    raise FulfillmentError('fulfillment_binding_mismatch')
                found = {r['child_key']: (r['destination'], r['item_json']) for r in db.execute(
                    'SELECT child_key,destination,item_json FROM shop_fulfillment_positions_v3 WHERE order_id=?', (order_id,))}
                expected = {c['child_key']: (c['destination'], _json(c['item'])) for c in children}
                if found != expected:
                    raise FulfillmentError('fulfillment_binding_mismatch')
            else:
                now = self._now()
                db.execute('INSERT INTO shop_fulfillment_v2 (order_id,customer_id,mode,binding_json,plan_hash,lines_json,policy_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)',
                           (order_id, owner, mode, encoded, plan_hash, line_json, policy_json, now, now))
                for child in sorted(children, key=lambda c: (c['destination'], c['child_key'])):
                    reference = 'SHOP-' + order_id[4:] + '-' + ('C' if child['destination'] == 'customer' else 'W') + '-' + child['child_key'][:24]
                    db.execute('INSERT INTO shop_fulfillment_positions_v3 (order_id,child_key,destination,item_json,reference,updated_at) VALUES (?,?,?,?,?,?)',
                               (order_id, child['child_key'], child['destination'], _json(child['item']), reference, now))

    def load(self, order_id, *, customer_id, mode):
        with self._connection() as db:
            row = dict(self._row(db, order_id, customer_id, mode))
            row['children'] = [dict(r) for r in db.execute('SELECT * FROM shop_fulfillment_positions_v3 WHERE order_id=? ORDER BY destination,child_key', (order_id,))]
            return row

    def claim(self, order_id, *, customer_id, mode, child_key, payload, authorization):
        frozen = freeze_order_request(payload)
        encoded, digest = _json(frozen.as_dict()), request_sha256(frozen)
        with self._connection(write=True) as db:
            parent = self._row(db, order_id, customer_id, mode)
            if parent['state'] != 'open':
                return False
            row = db.execute('SELECT * FROM shop_fulfillment_positions_v3 WHERE order_id=? AND child_key=?', (order_id, child_key)).fetchone()
            if row is None or row['reference'] != payload['reference']:
                raise FulfillmentError('fulfillment_binding_mismatch')
            item, position = json.loads(row['item_json']), frozen.as_dict()['positions'][0]
            if (position['supplier_article_id'] != item['mapping']['supplier_article_id']
                    or position['quantity'] != item['quantity']):
                raise FulfillmentError('fulfillment_binding_mismatch')
            if row['state'] != 'pending':
                return False
            if db.execute("SELECT 1 FROM shop_fulfillment_positions_v3 WHERE order_id=? AND state IN ('claimed','unknown','rejected')", (order_id,)).fetchone():
                return False
            return db.execute("UPDATE shop_fulfillment_positions_v3 SET state='claimed',request_hash=?,payload_json=?,authorization_json=?,updated_at=? WHERE order_id=? AND child_key=? AND state='pending'",
                (digest, encoded, _json(authorization), self._now(), order_id, child_key)).rowcount == 1

    def grant(self, envelope):
        """Consume the one-use transport permission inside its pre-POST check."""
        if not isinstance(envelope, Mapping):
            return False
        with self._connection(write=True) as db:
            row = db.execute('''SELECT child.*,parent.mode,parent.policy_json,parent.state AS parent_state
              FROM shop_fulfillment_positions_v3 child JOIN shop_fulfillment_v2 parent USING(order_id)
              WHERE child.reference=?''', (envelope.get('reference'),)).fetchone()
            if row is None or row['state'] != 'claimed' or row['parent_state'] != 'open' or row['grant_used'] != 0:
                return False
            policy = json.loads(row['policy_json'])
            if (envelope.get('request_hash') != row['request_hash'] or envelope.get('configured_mode') != row['mode']
                    or envelope.get('idempotency_key') != row['reference']
                    or any(envelope.get(key) != policy[key] for key in POLICY_FIELDS)):
                return False
            authorization = json.loads(row['authorization_json'])
            now = _stamp(self.clock())
            if (not _stamp(authorization['observed_at']) <= now < _stamp(authorization['procurement_expires_at'])
                    or _stamp(authorization['authorized_until']) <= now + timedelta(seconds=authorization['minimum_capture_seconds'])):
                return False
            return db.execute('UPDATE shop_fulfillment_positions_v3 SET grant_used=1 WHERE reference=? AND grant_used=0', (row['reference'],)).rowcount == 1

    def submission_owner(self, reference, *, mode):
        """Resolve a frozen position internally; never a public ownership API."""
        if not isinstance(reference, str) or len(reference) > 200:
            return None
        with self._connection() as db:
            row = db.execute('''SELECT p.order_id,f.customer_id FROM shop_fulfillment_positions_v3 p
                JOIN shop_fulfillment_v2 f ON f.order_id=p.order_id WHERE p.reference=? AND f.mode=?''',
                (reference, mode)).fetchone()
            return dict(row) if row else None

    def settle(self, order_id, *, customer_id, mode, child_key, state, receipt):
        if state not in {'accepted', 'rejected', 'unknown'}:
            raise FulfillmentError('fulfillment_binding_mismatch')
        encoded = _json(receipt)
        if len(encoded.encode()) > 65536:
            raise FulfillmentError('fulfillment_binding_mismatch')
        with self._connection(write=True) as db:
            self._row(db, order_id, customer_id, mode)
            row = db.execute('SELECT * FROM shop_fulfillment_positions_v3 WHERE order_id=? AND child_key=?', (order_id, child_key)).fetchone()
            if (row is None or row['state'] != 'claimed' or row['grant_used'] != 1
                    or receipt.get('reference') != row['reference'] or receipt.get('request_hash') != row['request_hash']):
                raise FulfillmentError('fulfillment_binding_mismatch')
            db.execute('UPDATE shop_fulfillment_positions_v3 SET state=?,receipt_json=?,updated_at=? WHERE order_id=? AND child_key=?',
                       (state, encoded, self._now(), order_id, child_key))
            if state == 'unknown':
                db.execute("UPDATE shop_fulfillment_v2 SET state='manual_review',reason='supplier_outcome_unknown',updated_at=? WHERE order_id=?", (self._now(), order_id))

    def reject_unordered(self, order_id, *, customer_id, mode, reason):
        with self._connection(write=True) as db:
            row = self._row(db, order_id, customer_id, mode)
            states = {r[0] for r in db.execute('SELECT state FROM shop_fulfillment_positions_v3 WHERE order_id=?', (order_id,))}
            if row['state'] != 'open' or not states <= {'pending', 'rejected'}:
                return False
            db.execute("UPDATE shop_fulfillment_v2 SET state='rejected',reason=?,updated_at=? WHERE order_id=?", (reason, self._now(), order_id))
            return True

    def mark_accepted(self, order_id, *, customer_id, mode):
        with self._connection(write=True) as db:
            row = self._row(db, order_id, customer_id, mode)
            children = list(db.execute('SELECT state FROM shop_fulfillment_positions_v3 WHERE order_id=?', (order_id,)))
            if row['state'] != 'open' or not children or any(r[0] != 'accepted' for r in children):
                return False
            db.execute("UPDATE shop_fulfillment_v2 SET state='accepted',updated_at=? WHERE order_id=?", (self._now(), order_id))
            return True

    def manual(self, order_id, *, customer_id, mode, reason):
        with self._connection(write=True) as db:
            self._row(db, order_id, customer_id, mode)
            db.execute("UPDATE shop_fulfillment_v2 SET state='manual_review',reason=?,updated_at=? WHERE order_id=? AND state='open'", (reason, self._now(), order_id))

    def action_result(self, order_id, *, customer_id, mode, state):
        if state not in {'captured', 'cancelled', 'unknown', 'conflict'}:
            raise FulfillmentError('fulfillment_binding_mismatch')
        with self._connection(write=True) as db:
            row = self._row(db, order_id, customer_id, mode)
            if row['state'] not in {'accepted', 'rejected'}:
                raise FulfillmentError('fulfillment_binding_mismatch')
            # A delayed uncertain read must not erase a completed money action.
            if row['action_state'] in {'captured', 'cancelled'}:
                if state in {row['action_state'], 'unknown'}:
                    return
                state = 'conflict'
            db.execute('UPDATE shop_fulfillment_v2 SET action_state=?,updated_at=? WHERE order_id=?', (state, self._now(), order_id))

    def proof(self, order_id, *, operation, mode):
        """Internal payment callback, never expose this through a public API."""
        if operation not in {'capture', 'cancel'} or mode not in {'test', 'live'} or not isinstance(order_id, str) or not ORDER.fullmatch(order_id):
            raise FulfillmentError('fulfillment_proof_unavailable')
        with self._connection() as db:
            row = db.execute('SELECT * FROM shop_fulfillment_v2 WHERE order_id=? AND mode=?', (order_id, mode)).fetchone()
            if row is None or row['state'] != ('accepted' if operation == 'capture' else 'rejected'):
                raise FulfillmentError('fulfillment_proof_unavailable')
            states = [r[0] for r in db.execute('SELECT state FROM shop_fulfillment_positions_v3 WHERE order_id=?', (order_id,))]
            if not states or (operation == 'capture' and any(s != 'accepted' for s in states)) or (operation == 'cancel' and any(s not in {'pending', 'rejected'} for s in states)):
                raise FulfillmentError('fulfillment_proof_unavailable')
            binding = json.loads(row['binding_json'])
            proof = {key: binding[key] for key in PAYMENT_FIELDS}
            proof.update(proof_id='fulfillment-v2:' + order_id + ':' + operation, verified=True, persisted=True,
                         all_goods_accepted=operation == 'capture')
            if operation == 'capture':
                proof['accepted_lines'] = json.loads(row['lines_json'])
            else:
                proof['cancel_allowed'] = True
            return proof


class ShopFulfillment:
    def __init__(self, order_store, journal, payment_actions=None, rsu_transport=None, *, mode,
                 supplier_policy=None, fresh_inquiry=None, revalidate=None, delivery_address=None,
                 enabled=False, clock=None, minimum_capture_seconds=120, review_service=None):
        if (mode not in {'test', 'live'} or type(enabled) is not bool
                or type(minimum_capture_seconds) is not int or not 60 <= minimum_capture_seconds <= 3600):
            raise FulfillmentError('invalid_fulfillment_configuration')
        policy = dict(supplier_policy or {})
        if (set(policy) != POLICY_FIELDS or not isinstance(policy.get('account_fingerprint'), str)
                or not HASH.fullmatch(policy['account_fingerprint'])
                or any(not isinstance(policy.get(key), str) or not 1 <= len(policy[key]) <= 160 for key in POLICY_FIELDS - {'account_fingerprint'})):
            if enabled:
                raise FulfillmentError('invalid_fulfillment_configuration')
        self.store, self.journal, self.payment, self.transport = order_store, journal, payment_actions, rsu_transport
        self.mode, self.enabled, self.policy = mode, enabled, deepcopy(policy)
        self.inquiry, self.revalidate, self.delivery_address = fresh_inquiry, revalidate, delivery_address
        self.clock, self.margin = clock or (lambda: datetime.now(timezone.utc)), minimum_capture_seconds
        self.review_service = review_service

    def __repr__(self):
        return '<ShopFulfillment mode=' + self.mode + ' enabled=' + str(self.enabled) + '>'

    def readiness(self):
        return {'ready': self.enabled is True and all(callable(fn) for fn in (
            self.inquiry, self.revalidate, self.delivery_address,
            getattr(self.store, 'get_order', None), getattr(self.store, 'snapshot_for_order', None),
            getattr(self.store, 'delivery_context_for_order', None), getattr(self.store, 'claim_payment', None),
            getattr(self.journal, 'ensure', None), getattr(self.journal, 'load', None),
            getattr(self.journal, 'claim', None), getattr(self.journal, 'grant', None),
            getattr(self.journal, 'settle', None), getattr(self.journal, 'proof', None),
            getattr(self.payment, 'authorization', None), getattr(self.payment, 'capture', None),
            getattr(self.payment, 'cancel', None), getattr(self.transport, 'submit_order', None))), 'mode': self.mode}

    def _now(self):
        return _stamp(self.clock())

    def _load(self, order_id, customer_id):
        _identity(order_id, customer_id, self.mode)
        try:
            order = self.store.get_order(order_id, customer_id=customer_id, mode=self.mode)
            snapshot = self.store.snapshot_for_order(order_id, customer_id=customer_id, mode=self.mode)
            context = self.store.delivery_context_for_order(order_id, customer_id=customer_id, mode=self.mode)
            _validate_snapshot(snapshot, order['quote_hash'], customer_id, self.mode)
            _delivery_context(snapshot, context)
            if order['amount_gross_cent'] != snapshot['total_gross_cent'] or order['currency'] != 'eur':
                raise ValueError()
            if order['status'] not in {'authorized', 'paid', 'cancelled'}:
                raise ValueError()
            attempt = self.store.claim_payment(order_id, customer_id=customer_id, mode=self.mode)
            if attempt['can_create'] or not attempt['session_id'] or not attempt['intent_id']:
                raise ValueError()
        except Exception:
            raise FulfillmentError('frozen_order_unavailable') from None
        binding = {'order_id': order_id, 'customer_id': customer_id, 'mode': self.mode, 'currency': 'eur',
                   'amount_gross_cent': order['amount_gross_cent'], 'quote_hash': order['quote_hash'],
                   'payment_attempt': attempt['id'], 'session_id': attempt['session_id'], 'intent_id': attempt['intent_id']}
        return order, snapshot, context, binding

    def _authorization(self, binding):
        try:
            started = self._now()
            value = self.payment.authorization(binding['order_id'])
            now = self._now()
            if (not isinstance(value, Mapping) or value.get('verified') is not True or value.get('state') != 'authorized'
                    or any(value.get(key) != binding[key] for key in PAYMENT_FIELDS)
                    or not started <= _stamp(value.get('observed_at')) <= now
                    or _stamp(value.get('authorized_until')) <= now + timedelta(seconds=self.margin)):
                raise ValueError()
            return {key: deepcopy(value[key]) for key in (*PAYMENT_FIELDS, 'observed_at', 'authorized_until')}
        except Exception:
            raise FulfillmentError('authorization_unconfirmed') from None

    @staticmethod
    def _children(snapshot):
        children, seen, references = [], set(), set()
        for group in snapshot['shipments']:
            for item in group['private_items']:
                material = {'destination': group['destination'], 'item': item}
                child_key = _hash(material)
                reference_part = (group['destination'], child_key[:24])
                if child_key in seen or reference_part in references:
                    raise FulfillmentError('frozen_order_unavailable')
                seen.add(child_key)
                references.add(reference_part)
                children.append({'child_key': child_key, 'destination': group['destination'], 'item': deepcopy(item)})
        if not children:
            raise FulfillmentError('frozen_order_unavailable')
        return children

    def _fresh_plan(self, snapshot, context, children):
        started = self._now()
        try:
            destinations = {child['destination'] for child in children}
            old_lines = [v for v in snapshot['lines'] if v['destination'] in destinations]
            old_requests = [r for r, v in zip(snapshot['request'], snapshot['lines']) if v['destination'] in destinations]
            fresh = self.revalidate(deepcopy(snapshot), deepcopy(context), destinations=tuple(sorted(destinations)))
            if callable(getattr(fresh, 'snapshot', None)):
                fresh = fresh.snapshot()
            _validate_snapshot(fresh, fresh['quote_hash'], snapshot['customer_id'], self.mode)
            _delivery_context(fresh, context)
            if (fresh['request'] != old_requests or fresh.get('vehicle_ref') != snapshot.get('vehicle_ref')
                    or not started <= _stamp(fresh['created_at']) <= self._now()
                    or not self._now() < _stamp(fresh['expires_at']) or len(fresh['lines']) != len(old_lines)):
                raise ValueError()
            if requires_review(snapshot) and (not requires_review(fresh)
                    or any(fresh.get(key) != snapshot.get(key) for key in
                           ('review_requirements', 'assembly_proposals', 'assembly_evidence', 'product_checks'))):
                # Approval covers these exact technical dependencies. A newly
                # generated financial quote cannot silently substitute another
                # nominal source or erase an unresolved/required condition.
                raise ValueError()
            demand, unit_limits = Counter(), {}
            for old, new in zip(old_lines, fresh['lines']):
                if (any(new.get(key) != old.get(key) for key in ('id', 'identity_hash', 'category', 'quantity', 'destination'))
                        or new['unit_gross_cent'] > old['unit_gross_cent']):
                    raise ValueError()
                old_services = {v['code']: v['gross_cent'] for v in old['services']}
                new_services = {v['code']: v['gross_cent'] for v in new['services']}
                if set(old_services) != set(new_services) or any(new_services[k] > old_services[k] for k in old_services):
                    raise ValueError()
                key = (old['id'], old['destination'])
                unit_limits[key] = min(unit_limits.get(key, old['unit_gross_cent']), old['unit_gross_cent'])
            for child in children:
                item = json.loads(child['item_json'])
                demand[item['id']] += item['quantity']
            old_groups = {v['id']: v for v in snapshot['shipments'] if v['id'] in destinations}
            if set(old_groups) != {v['id'] for v in fresh['shipments']}:
                raise ValueError()
            for group in fresh['shipments']:
                old = old_groups[group['id']]
                if (any(group.get(key) != old.get(key) for key in ('id', 'destination', 'items', 'private_items', 'context_hash', 'shipment_hash'))
                        or group['gross_cent'] > old['gross_cent']):
                    raise ValueError()
            net = {}
            for child in sorted(children, key=lambda c: (c['destination'], c['child_key'])):
                item = json.loads(child['item_json'])
                identifier, quantity = item['id'], item['quantity']
                read_started = self._now()
                result = self.inquiry(deepcopy(item), quantity)
                now = self._now()
                if (not isinstance(result, Mapping) or result.get('confirmed') is not True or result.get('mode') != self.mode
                        or result.get('currency') != 'EUR' or result.get('product_id') != identifier
                        or result.get('identity_hash') != item['identity_hash']
                        or result.get('supplier_article_id') != item['mapping']['supplier_article_id']
                        or result.get('mapping_evidence_id') != item['mapping']['evidence_id']
                        or not isinstance(result.get('expected_ean'), str) or result.get('ean') != result['expected_ean']
                        or type(result.get('quantity')) is not int or result['quantity'] != quantity
                        or type(result.get('stock')) is not int or not demand[identifier] <= result['stock'] <= 1_000_000_000
                        or not read_started <= _stamp(result.get('observed_at')) <= now
                        or not timedelta(0) < _stamp(result.get('expires_at')) - _stamp(result['observed_at']) <= TTL
                        or not now < _stamp(result['expires_at'])
                        or not isinstance(result.get('unit_net'), str) or not re.fullmatch(r'(?:0|[1-9][0-9]{0,5})(?:\.[0-9]{1,4})?', result['unit_net'])):
                    raise ValueError()
                category = next(v['category'] for v in old_lines if v['id'] == identifier)
                retail = retail_unit_gross_cent(result['unit_net'], category)
                if retail > unit_limits[(identifier, child['destination'])]:
                    raise ValueError()
                net[child['child_key']] = {key: deepcopy(result[key]) for key in ('ean', 'unit_net', 'expires_at')}
            if self._now() >= _stamp(fresh['expires_at']) or any(self._now() >= _stamp(v['expires_at']) for v in net.values()):
                raise ValueError()
            return fresh, net
        except Exception:
            raise FulfillmentError('procurement_revalidation_failed') from None

    def _payload(self, context, child, net):
        address = self.delivery_address(child['destination'], deepcopy(context))
        if (not isinstance(address, Mapping) or type(address.get('neutraldelivery')) is not int
                or address['neutraldelivery'] != 1):
            raise FulfillmentError('delivery_unconfirmed')
        item = json.loads(child['item_json'])
        position = {'pos': 1, 'supplier_article_id': item['mapping']['supplier_article_id'],
                    'ean': net['ean'], 'quantity': item['quantity'], 'unit_net': net['unit_net'], 'currency': 'EUR'}
        return freeze_order_request({'reference': child['reference'], 'positions': [position],
                                     'delivery_address': dict(address)}).as_dict()

    def _receipt(self, payload, result):
        if (not isinstance(result, Mapping) or result.get('reference') != payload['reference']
                or result.get('request_hash') != request_sha256(payload)
                or result.get('echo_confirmed') is not False or result.get('fulfillment_confirmed') is not False
                or result.get('capture_authorized') is not False or result.get('retry_authorized') is not False
                or not isinstance(result.get('positions'), list) or len(result['positions']) != len(payload['positions'])):
            return 'unknown'
        outcomes = set()
        for expected, actual in zip(payload['positions'], result['positions']):
            if (not isinstance(actual, Mapping) or any(actual.get(key) != value for key, value in expected.items())
                    or actual.get('request_bound') is not True or actual.get('echo_confirmed') is not False
                    or actual.get('facts_source') != 'submitted_request_under_external_policy'):
                return 'unknown'
            outcome, number = actual.get('outcome'), actual.get('order_number')
            if (outcome == 'accepted' and isinstance(number, str)
                    and (re.fullmatch(r'[1-9][0-9]{0,29}', number) or self.mode == 'test' and re.fullmatch(r'Test_[0-9]{1,25}', number))):
                outcomes.add('accepted')
            elif outcome == 'rejected' and number is None:
                outcomes.add('rejected')
            else:
                return 'unknown'
        expected = 'accepted' if outcomes == {'accepted'} else 'rejected' if outcomes == {'rejected'} else 'partial'
        return expected if result.get('outcome') == expected else 'unknown'

    def authorize_submission(self, envelope):
        if not isinstance(envelope, Mapping) or not self.readiness()['ready'] or envelope.get('configured_mode') != self.mode:
            return False
        try:
            owner = self.journal.submission_owner(envelope.get('reference'), mode=self.mode)
            if owner is None:
                return False
            _, snapshot, _, binding = self._load(owner['order_id'], owner['customer_id'])
            self._require_review(snapshot, binding)
        except Exception:
            return False
        return self.journal.grant(envelope)

    def proof_resolver(self, order_id, *, operation):
        if not self.enabled:
            raise FulfillmentError('fulfillment_disabled')
        if operation == 'capture':
            owner = self.store.lookup_payment_owner(order_id, mode=self.mode)
            _, snapshot, _, binding = self._load(order_id, owner['customer_id'])
            self._require_review(snapshot, binding)
        return self.journal.proof(order_id, operation=operation, mode=self.mode)

    def _require_review(self, snapshot, binding):
        try:
            if not requires_review(snapshot):
                return None
            if self.review_service is None or self.review_service.readiness().get('ready') is not True or self.review_service.mode != self.mode:
                raise ValueError()
            return self.review_service.require_approved(binding['order_id'], customer_id=binding['customer_id'], binding=binding)
        except Exception:
            raise FulfillmentError('review_approval_required') from None

    def status(self, order_id, *, customer_id):
        row = self.journal.load(order_id, customer_id=customer_id, mode=self.mode)
        state = row['state']
        if row['action_state'] in {'unknown', 'conflict'} or any(v['state'] in {'claimed', 'unknown'} for v in row['children']):
            state = 'manual_review'
        elif state == 'accepted':
            state = 'captured' if row['action_state'] == 'captured' else 'awaiting_capture'
        elif state == 'rejected':
            state = 'cancelled' if row['action_state'] == 'cancelled' else 'awaiting_cancellation'
        return {'order_id': order_id, 'mode': self.mode, 'state': state,
                'shipments': [{'destination': c['destination'], 'state': c['state']} for c in row['children']],
                'payment_confirmed': state == 'captured', 'authorization_released': state == 'cancelled',
                'delivery_confirmed': False, 'supplier_retry_allowed': False}

    def _action(self, order_id, customer_id, operation):
        if operation == 'capture':
            try:
                _, snapshot, _, binding = self._load(order_id, customer_id)
                self._require_review(snapshot, binding)
            except Exception:
                return {**self.status(order_id, customer_id=customer_id), 'state': 'manual_review'}
        try:
            result = getattr(self.payment, operation)(order_id)
            row = self.journal.load(order_id, customer_id=customer_id, mode=self.mode)
            binding = json.loads(row['binding_json'])
            if (not isinstance(result, Mapping) or result.get('order_id') != order_id or result.get('operation') != operation
                    or result.get('mode') != self.mode or result.get('currency') != 'eur'
                    or result.get('amount_gross_cent') != binding['amount_gross_cent']):
                raise ValueError()
            expected = 'captured' if operation == 'capture' else 'cancelled'
            if (result.get('state') == expected and result.get('payment_confirmed') is (operation == 'capture')
                    and result.get('authorization_released') is (operation == 'cancel')):
                state = expected
            elif result.get('state') == 'unknown':
                state = 'unknown'
            else:
                state = 'conflict'
        except Exception:
            state = 'unknown'
        self.journal.action_result(order_id, customer_id=customer_id, mode=self.mode, state=state)
        return self.status(order_id, customer_id=customer_id)

    def run(self, order_id, *, customer_id):
        if not self.readiness()['ready']:
            raise FulfillmentError('fulfillment_disabled')
        order, snapshot, context, binding = self._load(order_id, customer_id)
        demand = Counter()
        for line in snapshot['lines']:
            demand[line['id']] += line['quantity']
        self.journal.ensure(binding, plan_hash=_hash({'supplier_command_schema': 'single_position_v3', 'snapshot': snapshot, 'context': context}),
            lines=[{'id': key, 'quantity': demand[key]} for key in sorted(demand)], policy=self.policy,
            children=self._children(snapshot))
        row = self.journal.load(order_id, customer_id=customer_id, mode=self.mode)
        if row['action_state'] in {'captured', 'cancelled', 'conflict'} or row['state'] == 'manual_review':
            return self.status(order_id, customer_id=customer_id)
        if requires_review(snapshot):
            try:
                if self.review_service is None or self.review_service.readiness().get('ready') is not True or self.review_service.mode != self.mode:
                    raise ValueError()
                review = self.review_service.ensure(order_id, customer_id=customer_id)
                if review['state'] in {'rejected', 'expired'}:
                    if row['state'] == 'rejected' or self.journal.reject_unordered(order_id, customer_id=customer_id,
                            mode=self.mode, reason='technical_review_' + review['state']):
                        return self._action(order_id, customer_id, 'cancel')
                    self.journal.manual(order_id, customer_id=customer_id, mode=self.mode, reason='review_expired_after_procurement')
                    return {**self.status(order_id, customer_id=customer_id), 'state': 'manual_review'}
                self._require_review(snapshot, binding)
            except Exception:
                return {**self.status(order_id, customer_id=customer_id), 'state': 'awaiting_review'}
        if row['state'] in {'accepted', 'rejected'}:
            return self._action(order_id, customer_id, 'capture' if row['state'] == 'accepted' else 'cancel')
        if order['status'] != 'authorized':
            self.journal.manual(order_id, customer_id=customer_id, mode=self.mode, reason='authorization_unconfirmed')
            return self.status(order_id, customer_id=customer_id)
        for child in row['children']:
            if child['state'] in {'claimed', 'unknown'}:
                return self.status(order_id, customer_id=customer_id)
            if child['state'] == 'accepted':
                continue
            if child['state'] == 'rejected':
                break
            try:
                authorization = self._authorization(binding)
            except FulfillmentError:
                self.journal.manual(order_id, customer_id=customer_id, mode=self.mode, reason='authorization_unconfirmed')
                return self.status(order_id, customer_id=customer_id)
            try:
                self._require_review(snapshot, binding)
                latest = self.journal.load(order_id, customer_id=customer_id, mode=self.mode)
                current = next(v for v in latest['children'] if v['child_key'] == child['child_key'])
                if current['state'] != 'pending' or any(v['state'] in {'claimed', 'unknown', 'rejected'} for v in latest['children']):
                    return self.status(order_id, customer_id=customer_id)
                remaining = [v for v in latest['children'] if v['state'] == 'pending']
                fresh, net = self._fresh_plan(snapshot, context, remaining)
                self._require_review(snapshot, binding)
                payload = self._payload(context, child, net[child['child_key']])
                # A slow service/stock/address read must not consume the hold's
                # remaining capture window or refresh any observation time.
                authorization = self._authorization(binding)
                if self._now() >= _stamp(fresh['expires_at']) or any(self._now() >= _stamp(v['expires_at']) for v in net.values()):
                    raise FulfillmentError('procurement_revalidation_failed')
                authorization.update(procurement_expires_at=min(
                    [_stamp(fresh['expires_at'])] + [_stamp(v['expires_at']) for v in net.values()]).isoformat(),
                    minimum_capture_seconds=self.margin)
            except Exception:
                if self.journal.reject_unordered(order_id, customer_id=customer_id, mode=self.mode, reason='procurement_revalidation_failed'):
                    return self._action(order_id, customer_id, 'cancel')
                self.journal.manual(order_id, customer_id=customer_id, mode=self.mode, reason='partial_procurement_revalidation_failed')
                return self.status(order_id, customer_id=customer_id)
            if not self.journal.claim(order_id, customer_id=customer_id, mode=self.mode, child_key=child['child_key'], payload=payload, authorization=authorization):
                return self.status(order_id, customer_id=customer_id)
            try:
                self._require_review(snapshot, binding)
                result = self.transport.submit_order(deepcopy(payload), idempotency_key=child['reference'])
                outcome = self._receipt(payload, result)
            except Exception:
                outcome, result = 'unknown', None
            if outcome == 'unknown':
                receipt = {'reference': payload['reference'], 'request_hash': request_sha256(payload), 'outcome': 'unknown'}
            else:
                receipt = {key: deepcopy(result[key]) for key in ('reference', 'request_hash', 'outcome', 'positions', 'echo_confirmed', 'fulfillment_confirmed')}
            supplier_state = 'unknown' if outcome == 'partial' else outcome
            # If the transport refused before consuming its grant, this remains
            # a claimed/uncertain attempt. Never turn it into a retry permission.
            try:
                self.journal.settle(order_id, customer_id=customer_id, mode=self.mode, child_key=child['child_key'], state=supplier_state, receipt=receipt)
            except FulfillmentError:
                return self.status(order_id, customer_id=customer_id)
            if supplier_state == 'unknown':
                return self.status(order_id, customer_id=customer_id)
            if outcome == 'rejected':
                break
        if self.journal.mark_accepted(order_id, customer_id=customer_id, mode=self.mode):
            return self._action(order_id, customer_id, 'capture')
        if self.journal.reject_unordered(order_id, customer_id=customer_id, mode=self.mode, reason='supplier_rejected'):
            return self._action(order_id, customer_id, 'cancel')
        self.journal.manual(order_id, customer_id=customer_id, mode=self.mode, reason='partial_supplier_acceptance')
        return self.status(order_id, customer_id=customer_id)
