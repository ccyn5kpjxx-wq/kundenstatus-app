"""Explicit future manual-card capture/release boundary, disabled by default.

This module performs no operation on import/construction. Runtime activation is
separate from Checkout. Only a server order ID enters capture/cancel: identity,
amount, card binding and immutable fulfillment proof are loaded privately.
proof_resolver(order_id, operation='capture'|'cancel') MUST load a verified,
persisted proof from the fulfillment journal, never return an HTTP payload.

The injected journal's atomic claim returns can_create exactly once. Capture and
cancel share one claim per order/PI, so opposite operations cannot race. Any
lost/invalid response leaves an unknown claim; subsequent calls only retrieve
the same PI and never repeat a POST. There is no refund operation. A captured
payment cannot be 'cancelled'. Actual SDK methods are reached only after all
binding/proof/deadline checks and an explicit enabled=True configuration.
"""
from collections import Counter
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from hashlib import sha256
import json
import re
import sqlite3
from uuid import uuid4

from shop_checkout import ShopCheckout

BINDING_FIELDS = {'order_id', 'customer_id', 'mode', 'currency', 'amount_gross_cent',
                  'quote_hash', 'payment_attempt', 'session_id', 'intent_id'}
PROOF_BINDING = BINDING_FIELDS - {'customer_id'}
MAX_JSON = 65536


class PaymentActionError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise PaymentActionError('payment_action_unavailable') from None


def _plain(value):
    if not isinstance(value, Mapping) and callable(getattr(value, 'to_dict', None)):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise PaymentActionError('payment_binding_mismatch')
    return dict(value)


def _freeze(value):
    try:
        encoded = _json(value)
        if len(encoded.encode('utf-8')) > MAX_JSON:
            raise ValueError()
        return encoded
    except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError):
        raise PaymentActionError('payment_binding_mismatch') from None


def _binding(value):
    if not isinstance(value, dict) or set(value) != BINDING_FIELDS:
        raise PaymentActionError('payment_binding_mismatch')
    patterns = {'order_id': r'sho_[a-f0-9]{32}', 'customer_id': r'[A-Za-z0-9_:@.\-]{1,128}',
                'quote_hash': r'[a-f0-9]{64}', 'payment_attempt': r'spa_[a-f0-9]{32}',
                'session_id': r'cs_(?:test|live)_[A-Za-z0-9]{1,200}', 'intent_id': r'pi_[A-Za-z0-9]{1,200}'}
    if (any(not isinstance(value.get(key), str) or not re.fullmatch(pattern, value[key]) for key, pattern in patterns.items())
            or not isinstance(value['mode'], str) or value['mode'] not in {'test', 'live'}
            or not value['session_id'].startswith('cs_' + value['mode'] + '_') or value['currency'] != 'eur'
            or type(value['amount_gross_cent']) is not int or not 0 < value['amount_gross_cent'] <= 100_000_000):
        raise PaymentActionError('payment_binding_mismatch')
    return _freeze(value)


def init_payment_action_schema(connection):
    connection.executescript('''
        CREATE TABLE IF NOT EXISTS shop_payment_actions_v2 (
            order_id TEXT PRIMARY KEY, intent_id TEXT UNIQUE NOT NULL,
            id TEXT UNIQUE NOT NULL, operation TEXT NOT NULL,
            binding_json TEXT NOT NULL, proof_json TEXT NOT NULL,
            idempotency_key TEXT UNIQUE NOT NULL, state TEXT NOT NULL,
            created_at TEXT NOT NULL, result_json TEXT);
        CREATE TRIGGER IF NOT EXISTS shop_payment_action_immutable_v2
        BEFORE UPDATE OF order_id,intent_id,id,operation,binding_json,proof_json,idempotency_key,created_at
        ON shop_payment_actions_v2 BEGIN SELECT RAISE(ABORT,'immutable_payment_action'); END;
    ''')
    connection.commit()


class PaymentActionJournal:
    """Owned SQLite connections; claim/bind under write transactions, no IO default."""
    def __init__(self, connection_factory, *, clock=None):
        if not callable(connection_factory):
            raise PaymentActionError('invalid_payment_action_configuration')
        self.factory, self.clock = connection_factory, clock or (lambda: datetime.now(timezone.utc))

    @contextmanager
    def _db(self, *, write=False):
        connection = None
        try:
            connection = self.factory()
            if not isinstance(connection, sqlite3.Connection):
                raise ValueError()
            connection.row_factory = sqlite3.Row
            if write:
                connection.execute('BEGIN IMMEDIATE')
            yield connection
            if write:
                connection.commit()
        except PaymentActionError:
            if isinstance(connection, sqlite3.Connection):
                try:
                    connection.rollback()
                except Exception:
                    pass
            raise
        except Exception:
            if isinstance(connection, sqlite3.Connection):
                try:
                    connection.rollback()
                except Exception:
                    pass
            raise PaymentActionError('payment_action_unavailable') from None
        finally:
            if isinstance(connection, sqlite3.Connection):
                try:
                    connection.close()
                except Exception:
                    pass

    @staticmethod
    def _row(row, *, can_create=False):
        if row is None:
            return None
        return {'id': row['id'], 'operation': row['operation'], 'binding': json.loads(row['binding_json']),
                'proof': json.loads(row['proof_json']), 'state': row['state'],
                'idempotency_key': row['idempotency_key'], 'created_at': row['created_at'],
                'result': json.loads(row['result_json']) if row['result_json'] else None, 'can_create': can_create}

    def lookup(self, order_id, *, mode):
        if not isinstance(order_id, str) or not re.fullmatch(r'sho_[a-f0-9]{32}', order_id):
            raise PaymentActionError('payment_binding_mismatch')
        with self._db() as db:
            value = self._row(db.execute('SELECT * FROM shop_payment_actions_v2 WHERE order_id=?', (order_id,)).fetchone())
        if value is not None and value['binding']['mode'] != mode:
            raise PaymentActionError('payment_binding_mismatch')
        return value

    def claim(self, binding, operation, proof):
        encoded = _binding(binding)
        if operation not in ('capture', 'cancel'):
            raise PaymentActionError('payment_binding_mismatch')
        frozen_proof = _freeze(proof)
        with self._db(write=True) as db:
            prior = db.execute('SELECT * FROM shop_payment_actions_v2 WHERE order_id=?', (binding['order_id'],)).fetchone()
            if prior is not None:
                if prior['operation'] != operation or prior['binding_json'] != encoded or prior['proof_json'] != frozen_proof:
                    raise PaymentActionError('payment_action_conflict')
                return self._row(prior)
            identity = 'spx_' + uuid4().hex
            key = 'shop-' + operation + '-v1:' + sha256(encoded.encode('utf-8')).hexdigest()
            db.execute('INSERT INTO shop_payment_actions_v2 VALUES (?,?,?,?,?,?,?,?,?,NULL)',
                (binding['order_id'], binding['intent_id'], identity, operation, encoded, frozen_proof, key,
                 'claimed', _stamp(self.clock()).isoformat()))
            return self._row(db.execute('SELECT * FROM shop_payment_actions_v2 WHERE id=?', (identity,)).fetchone(), can_create=True)

    def mark_unknown(self, action_id):
        with self._db(write=True) as db:
            cursor = db.execute("UPDATE shop_payment_actions_v2 SET state='unknown' WHERE id=? AND state='claimed'", (action_id,))
            if cursor.rowcount != 1 and db.execute('SELECT 1 FROM shop_payment_actions_v2 WHERE id=?', (action_id,)).fetchone() is None:
                raise PaymentActionError('payment_action_not_found')

    def settle(self, action_id, binding, result):
        encoded = _binding(binding)
        if (not isinstance(result, dict) or set(result) != {'state', 'observed_at'}
                or result['state'] not in ('captured', 'cancelled')
                or _stamp(result['observed_at']) > _stamp(self.clock())):
            raise PaymentActionError('payment_binding_mismatch')
        with self._db(write=True) as db:
            row = db.execute('SELECT * FROM shop_payment_actions_v2 WHERE id=?', (action_id,)).fetchone()
            if row is None or row['binding_json'] != encoded:
                raise PaymentActionError('payment_binding_mismatch')
            if row['state'] in ('captured', 'cancelled'):
                if row['state'] != result['state']:
                    raise PaymentActionError('payment_action_conflict')
                return self._row(row)
            db.execute('UPDATE shop_payment_actions_v2 SET state=?,result_json=? WHERE id=?',
                       (result['state'], _freeze(result), action_id))
            return self._row(db.execute('SELECT * FROM shop_payment_actions_v2 WHERE id=?', (action_id,)).fetchone())


class PaymentActions:
    def __init__(self, store, gateway, journal, proof_resolver, *, mode, enabled=False, clock=None):
        if not isinstance(mode, str) or mode not in {'test', 'live'} or type(enabled) is not bool:
            raise PaymentActionError('invalid_payment_action_configuration')
        self.store, self.gateway, self.journal, self.proof_resolver = store, gateway, journal, proof_resolver
        self.mode, self.enabled, self.clock = mode, enabled, clock or (lambda: datetime.now(timezone.utc))

    def readiness(self):
        intents = getattr(getattr(getattr(self.gateway, 'client', None), 'v1', None), 'payment_intents', None)
        charges = getattr(getattr(getattr(self.gateway, 'client', None), 'v1', None), 'charges', None)
        return {'ready': (self.enabled and getattr(self.gateway, 'mode', None) == self.mode
                and callable(self.proof_resolver) and callable(getattr(charges, 'retrieve', None))
                and all(callable(getattr(intents, name, None)) for name in ('capture', 'cancel'))
                and all(callable(getattr(self.gateway, name, None)) for name in ('retrieve_session', 'retrieve_intent'))
                and all(callable(getattr(self.journal, name, None)) for name in ('lookup', 'claim', 'mark_unknown', 'settle'))),
                'mode': self.mode, 'capture_method': 'manual'}

    def _active(self):
        if not self.enabled:
            raise PaymentActionError('payment_actions_disabled')
        if not self.readiness()['ready']:
            raise PaymentActionError('payment_action_unavailable')

    def _load(self, order_id):
        try:
            owner = self.store.lookup_payment_owner(order_id, mode=self.mode)
            if not isinstance(owner, Mapping) or owner.get('mode') != self.mode:
                raise ValueError()
            checker = ShopCheckout(self.store, self.gateway, mode=self.mode,
                                   public_origin=self.gateway.public_origin, clock=self.clock)
            order, snapshot = checker._load(order_id, owner['customer_id'])
            if order['status'] == 'draft':
                raise ValueError()
            attempt = self.store.claim_payment(order_id, customer_id=owner['customer_id'], mode=self.mode)
            if attempt['can_create'] or not attempt.get('session_id') or not attempt.get('intent_id'):
                raise ValueError()
            binding = {'order_id': order_id, 'customer_id': order['customer_id'], 'mode': self.mode,
                       'currency': 'eur', 'amount_gross_cent': order['amount_gross_cent'], 'quote_hash': order['quote_hash'],
                       'payment_attempt': attempt['id'], 'session_id': attempt['session_id'], 'intent_id': attempt['intent_id']}
            _binding(binding)
            return order, snapshot, attempt, binding, checker
        except Exception:
            raise PaymentActionError('payment_binding_mismatch') from None

    def _current(self, loaded):
        order, _, attempt, binding, checker = loaded
        observed = _stamp(self.clock()).isoformat()
        try:
            session = checker._session(self.gateway.retrieve_session(binding['session_id']), order, attempt,
                                       expected_id=binding['session_id'])
            if session['intent_id'] != binding['intent_id']:
                raise ValueError()
            raw = _plain(self.gateway.retrieve_intent(binding['intent_id']))
            intent = checker._intent(raw, binding['intent_id'], order, attempt)
            if raw.get('payment_method_types') != ['card']:
                raise ValueError()
            return session, raw, intent, observed
        except Exception:
            raise PaymentActionError('payment_state_unconfirmed') from None

    def _deadline(self, raw, binding):
        charge_id = raw.get('latest_charge')
        charge_id = charge_id.get('id') if isinstance(charge_id, Mapping) else charge_id
        if not isinstance(charge_id, str) or not re.fullmatch(r'ch_[A-Za-z0-9]{1,200}', charge_id):
            raise PaymentActionError('authorization_unconfirmed')
        try:
            charge = _plain(self.gateway.client.v1.charges.retrieve(charge_id))
            details = charge.get('payment_method_details')
            card = details.get('card') if isinstance(details, Mapping) else None
            deadline = card.get('capture_before') if isinstance(card, Mapping) else None
            if (charge.get('object') != 'charge' or charge.get('id') != charge_id
                    or charge.get('payment_intent') != binding['intent_id']
                    or charge.get('livemode') is not (self.mode == 'live') or charge.get('currency') != 'eur'
                    or type(charge.get('amount')) is not int or charge['amount'] != binding['amount_gross_cent']
                    or charge.get('captured') is not False or charge.get('paid') is not True
                    or type(charge.get('amount_captured')) is not int or charge['amount_captured'] != 0
                    or charge.get('refunded') or type(charge.get('amount_refunded', 0)) is not int
                    or charge.get('amount_refunded', 0) != 0 or charge.get('on_behalf_of') or charge.get('transfer')
                    or charge.get('application_fee') or charge.get('application_fee_amount')
                    or not isinstance(details, Mapping) or details.get('type') != 'card'
                    or type(deadline) is not int or deadline <= _stamp(self.clock()).timestamp()):
                raise ValueError()
            return datetime.fromtimestamp(deadline, timezone.utc).isoformat()
        except Exception:
            raise PaymentActionError('authorization_unconfirmed') from None

    def authorization(self, order_id):
        """READ ONLY fresh full manual-card hold; no fulfillment claim or POST."""
        self._active()
        loaded = self._load(order_id)
        session, raw, intent, observed = self._current(loaded)
        if session['status'] != 'complete' or intent['kind'] != 'authorized':
            raise PaymentActionError('authorization_unconfirmed')
        return {**loaded[3], 'verified': True, 'state': 'authorized',
                'authorized_until': self._deadline(raw, loaded[3]), 'observed_at': observed}

    def _proof(self, loaded, operation):
        binding, snapshot = loaded[3], loaded[1]
        try:
            proof = json.loads(_freeze(self.proof_resolver(binding['order_id'], operation=operation)))
            if (not isinstance(proof, dict) or proof.get('verified') is not True or proof.get('persisted') is not True
                    or not isinstance(proof.get('proof_id'), str) or not re.fullmatch(r'[A-Za-z0-9_:-]{3,160}', proof['proof_id'])
                    or proof.get('operation', operation) != operation
                    or any(type(proof.get(field)) is not type(binding[field])
                           or proof[field] != binding[field] for field in PROOF_BINDING)):
                raise ValueError()
            if operation == 'capture':
                if proof.get('all_goods_accepted') is not True:
                    raise ValueError()
                expected = Counter()
                for line in snapshot['lines']:
                    expected[line['id']] += line['quantity']
                accepted = Counter()
                if not isinstance(proof.get('accepted_lines'), list) or not proof['accepted_lines']:
                    raise ValueError()
                for line in proof['accepted_lines']:
                    if (not isinstance(line, dict) or set(line) != {'id', 'quantity'}
                            or not isinstance(line['id'], str) or type(line['quantity']) is not int or not 1 <= line['quantity'] <= 20
                            or line['id'] in accepted):
                        raise ValueError()
                    accepted[line['id']] = line['quantity']
                if accepted != expected:
                    raise ValueError()
            elif proof.get('cancel_allowed') is not True or proof.get('all_goods_accepted') is not False:
                raise ValueError()
            return proof
        except Exception:
            raise PaymentActionError('fulfillment_unconfirmed') from None

    def _result(self, action):
        binding, state = action['binding'], action['state']
        return {'order_id': binding['order_id'], 'operation': action['operation'],
                'state': state if state in {'captured', 'cancelled'} else 'unknown', 'mode': self.mode,
                'currency': 'eur', 'amount_gross_cent': binding['amount_gross_cent'],
                'payment_confirmed': state == 'captured', 'authorization_released': state == 'cancelled'}

    def _settle(self, loaded, action, kind, observed):
        if kind not in {'paid', 'cancelled'}:
            return self._result(action)
        state = 'captured' if kind == 'paid' else 'cancelled'
        retained = self.journal.settle(action['id'], loaded[3], {'state': state, 'observed_at': observed})
        stamp = retained['result']['observed_at']
        binding = loaded[3]
        self.store.apply_verified_event({'event_id': action['id'] + '_' + state, 'order_id': binding['order_id'],
                'attempt_id': binding['payment_attempt'], 'mode': self.mode, 'currency': 'eur',
                'amount_gross_cent': binding['amount_gross_cent'], 'session_id': binding['session_id'],
                'intent_id': binding['intent_id'], 'kind': kind, 'occurred_at': stamp}, verified=True)
        return self._result(retained)

    def _reconcile(self, loaded, action):
        if action['binding'] != loaded[3]:
            raise PaymentActionError('payment_binding_mismatch')
        _, _, intent, observed = self._current(loaded)
        return self._settle(loaded, action, intent['kind'], observed)

    def reconcile(self, order_id):
        self._active()
        loaded = self._load(order_id)
        action = self.journal.lookup(order_id, mode=self.mode)
        if action is None:
            raise PaymentActionError('payment_action_not_found')
        return self._reconcile(loaded, action)

    def _operate(self, order_id, operation):
        self._active()
        loaded = self._load(order_id)
        binding = loaded[3]
        prior = self.journal.lookup(order_id, mode=self.mode)
        if prior is not None:
            if prior['operation'] != operation:
                raise PaymentActionError('payment_action_conflict')
            return self._reconcile(loaded, prior)
        proof = self._proof(loaded, operation)
        session, raw, intent, observed = self._current(loaded)
        if operation == 'cancel' and intent['kind'] == 'paid':
            raise PaymentActionError('captured_payment_requires_refund')
        deadline = None
        if intent['kind'] == 'authorized':
            if operation == 'capture':
                if session['status'] != 'complete':
                    raise PaymentActionError('authorization_unconfirmed')
                deadline = self._deadline(raw, binding)
        elif intent['kind'] not in {'paid', 'cancelled'}:
            raise PaymentActionError('authorization_unconfirmed')
        action = self.journal.claim(binding, operation, proof)
        if not action['can_create']:
            return self._reconcile(loaded, action)
        if intent['kind'] in {'paid', 'cancelled'}:
            return self._settle(loaded, action, intent['kind'], observed)
        try:
            if deadline is not None and _stamp(deadline) <= _stamp(self.clock()):
                raise ValueError()
            params = {'amount_to_capture': binding['amount_gross_cent']} if operation == 'capture' else {'cancellation_reason': 'abandoned'}
            response = _plain(getattr(self.gateway.client.v1.payment_intents, operation)(binding['intent_id'], params,
                                     options={'idempotency_key': action['idempotency_key']}))
            normalized = loaded[4]._intent(response, binding['intent_id'], loaded[0], loaded[2])
            expected = 'paid' if operation == 'capture' else 'cancelled'
            if response.get('payment_method_types') != ['card'] or normalized['kind'] != expected:
                raise ValueError()
            return self._settle(loaded, action, normalized['kind'], _stamp(self.clock()).isoformat())
        except Exception:
            try:
                self.journal.mark_unknown(action['id'])
            except Exception:
                pass
            raise PaymentActionError('payment_action_reconciliation_required') from None

    def capture(self, order_id):
        return self._operate(order_id, 'capture')

    def cancel(self, order_id):
        return self._operate(order_id, 'cancel')
