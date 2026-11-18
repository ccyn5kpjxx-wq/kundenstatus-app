"""Durable technical approval of an exact, already authorized shop order.

No default connections, credentials, supplier writes, capture, cancellation,
background work or app imports. Approval never invents catalogue fitment: it is
an attributable operator decision bound to the frozen financial order. Only the
initial worker hold verification uses injected read-only payment authorization;
operator decisions and ordinary status/fences are database-only.
"""
from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import re
import sqlite3
import unicodedata

ORDER = re.compile(r'sho_[a-f0-9]{32}')
HASH = re.compile(r'[a-f0-9]{64}')
FUNDS = ('order_id', 'customer_id', 'mode', 'quote_hash', 'amount_gross_cent',
         'currency', 'payment_attempt', 'session_id', 'intent_id')


class OrderReviewError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _hash(value):
    return sha256(_json(value).encode('utf-8')).hexdigest()


def _stamp(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        raise OrderReviewError('review_unavailable') from None


def _text(value, maximum=200):
    return (isinstance(value, str) and 0 < len(value.strip()) <= maximum
            and value == value.strip() and not any(unicodedata.category(c).startswith('C') or c in '\u2028\u2029' for c in value)
            and '<' not in value and '>' not in value)


def requires_review(snapshot):
    if (isinstance(snapshot, Mapping) and type(snapshot.get('version')) is int
            and snapshot['version'] == 1 and not snapshot.get('review_requirements')
            and 'authorization_ready' not in snapshot and 'fulfillment_ready' not in snapshot):
        return False
    from shop_complete_wheels import is_review_snapshot
    if not is_review_snapshot(snapshot):
        raise OrderReviewError('invalid_review_snapshot')
    return True


def init_order_review_schema(connection):
    connection.executescript('''
      CREATE TABLE IF NOT EXISTS shop_order_reviews_v1 (
        order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, mode TEXT NOT NULL,
        binding_json TEXT NOT NULL, requirements_json TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('pending','approved','rejected','expired')),
        expires_at TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        actor_id TEXT, evidence_json TEXT, decided_at TEXT);
      CREATE TRIGGER IF NOT EXISTS shop_order_review_binding_immutable_v1
        BEFORE UPDATE OF order_id,customer_id,mode,binding_json,requirements_json,expires_at,created_at
        ON shop_order_reviews_v1 BEGIN SELECT RAISE(ABORT,'immutable_order_review'); END;
      CREATE TRIGGER IF NOT EXISTS shop_order_review_decision_immutable_v1
        BEFORE UPDATE OF actor_id,evidence_json,decided_at ON shop_order_reviews_v1
        WHEN OLD.state!='pending' BEGIN SELECT RAISE(ABORT,'immutable_order_review_decision'); END;
    ''')
    connection.commit()


class OrderReviewJournal:
    def __init__(self, connection_factory, *, clock=None):
        if not callable(connection_factory):
            raise OrderReviewError('review_unavailable')
        self.factory, self.clock = connection_factory, clock or (lambda: datetime.now(timezone.utc))

    @contextmanager
    def _db(self, *, write=False):
        db = None
        try:
            db = self.factory()
            if not isinstance(db, sqlite3.Connection) or db.in_transaction:
                raise ValueError()
            db.row_factory = sqlite3.Row
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            if write:
                db.commit()
        except Exception as error:
            if isinstance(db, sqlite3.Connection):
                db.rollback()
            if isinstance(error, OrderReviewError):
                raise
            raise OrderReviewError('review_unavailable') from None
        finally:
            if isinstance(db, sqlite3.Connection):
                db.close()

    def payment_binding(self, order_id, *, customer_id, mode):
        with self._db() as db:
            row = db.execute('''SELECT o.id AS order_id,o.customer_id,o.mode,o.quote_hash,
              o.amount_gross_cent,o.currency,a.id AS payment_attempt,a.session_id,a.intent_id
              FROM shop_orders_v2 o JOIN shop_payment_attempts_v2 a ON a.order_id=o.id
              WHERE o.id=? AND o.customer_id=? AND o.mode=? AND o.status IN ('authorized','paid','cancelled')
                AND a.session_id IS NOT NULL AND a.intent_id IS NOT NULL
                AND EXISTS (SELECT 1 FROM shop_payment_events_v2 e WHERE e.order_id=o.id
                    AND e.attempt_id=a.id AND e.kind='authorized')''', (order_id, customer_id, mode)).fetchone()
            return dict(row) if row else None

    def get(self, binding, *, margin_seconds, project_expiry=True):
        with self._db() as db:
            row = db.execute('SELECT * FROM shop_order_reviews_v1 WHERE order_id=?', (binding['order_id'],)).fetchone()
            if row is None:
                return None
            if row['binding_json'] != _json(binding):
                raise OrderReviewError('review_binding_mismatch')
            result = dict(row)
            if (project_expiry and row['state'] in {'pending', 'approved'}
                    and _stamp(self.clock()) + timedelta(seconds=margin_seconds) >= _stamp(row['expires_at'])):
                result['state'] = 'expired'
            return result

    def expire(self, binding, *, margin_seconds):
        """Explicit worker transition; ordinary GET/status never writes."""
        with self._db(write=True) as db:
            row = db.execute('SELECT * FROM shop_order_reviews_v1 WHERE order_id=?', (binding['order_id'],)).fetchone()
            if row is None or row['binding_json'] != _json(binding):
                raise OrderReviewError('review_binding_mismatch')
            if (row['state'] in {'pending', 'approved'}
                    and _stamp(self.clock()) + timedelta(seconds=margin_seconds) >= _stamp(row['expires_at'])):
                db.execute("UPDATE shop_order_reviews_v1 SET state='expired',updated_at=? WHERE order_id=?",
                           (_stamp(self.clock()).isoformat(), binding['order_id']))

    def ensure(self, binding, requirements, expires_at):
        now = _stamp(self.clock()).isoformat()
        with self._db(write=True) as db:
            old = db.execute('SELECT * FROM shop_order_reviews_v1 WHERE order_id=?', (binding['order_id'],)).fetchone()
            if old:
                if old['binding_json'] != _json(binding) or old['requirements_json'] != _json(requirements):
                    raise OrderReviewError('review_binding_mismatch')
                return dict(old)
            db.execute('''INSERT INTO shop_order_reviews_v1
              (order_id,customer_id,mode,binding_json,requirements_json,state,expires_at,created_at,updated_at)
              VALUES (?,?,?,?,?,'pending',?,?,?)''', (binding['order_id'], binding['customer_id'], binding['mode'],
                _json(binding), _json(requirements), expires_at, now, now))
            return dict(db.execute('SELECT * FROM shop_order_reviews_v1 WHERE order_id=?', (binding['order_id'],)).fetchone())

    def decide(self, binding, *, state, actor_id, evidence, margin_seconds):
        if state not in {'approved', 'rejected'} or not _text(actor_id, 128):
            raise OrderReviewError('invalid_review_decision')
        now = _stamp(self.clock())
        with self._db(write=True) as db:
            row = db.execute('SELECT * FROM shop_order_reviews_v1 WHERE order_id=?', (binding['order_id'],)).fetchone()
            if row is None or row['binding_json'] != _json(binding):
                raise OrderReviewError('review_binding_mismatch')
            if now + timedelta(seconds=margin_seconds) >= _stamp(row['expires_at']):
                raise OrderReviewError('review_expired')
            if row['state'] == state and row['actor_id'] == actor_id and row['evidence_json'] == _json(evidence):
                return dict(row)
            if row['state'] != 'pending':
                raise OrderReviewError('review_not_pending')
            db.execute('''UPDATE shop_order_reviews_v1 SET state=?,actor_id=?,evidence_json=?,decided_at=?,updated_at=?
                          WHERE order_id=? AND state='pending' ''',
                       (state, actor_id, _json(evidence), now.isoformat(), now.isoformat(), binding['order_id']))
            return dict(db.execute('SELECT * FROM shop_order_reviews_v1 WHERE order_id=?', (binding['order_id'],)).fetchone())


class ShopOrderReview:
    def __init__(self, store, journal, payment_actions=None, *, mode, enabled=False, clock=None,
                 minimum_capture_seconds=120):
        if (mode not in {'test', 'live'} or type(enabled) is not bool
                or type(minimum_capture_seconds) is not int or not 60 <= minimum_capture_seconds <= 3600):
            raise OrderReviewError('invalid_review_configuration')
        self.store, self.journal, self.payment = store, journal, payment_actions
        self.mode, self.enabled, self.margin = mode, enabled, minimum_capture_seconds
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def readiness(self):
        return {'ready': self.enabled and all(callable(getattr(self.store, m, None)) for m in
                    ('get_order', 'snapshot_for_order', 'delivery_context_for_order'))
                and all(callable(getattr(self.journal, m, None)) for m in ('get', 'ensure', 'decide', 'payment_binding', 'expire'))
                and callable(getattr(self.payment, 'authorization', None)), 'mode': self.mode}

    def _load(self, order_id, customer_id):
        if not self.readiness()['ready']:
            raise OrderReviewError('review_unavailable')
        if not isinstance(order_id, str) or not ORDER.fullmatch(order_id) or not _text(customer_id, 128):
            raise OrderReviewError('review_binding_mismatch')
        try:
            order = self.store.get_order(order_id, customer_id=customer_id, mode=self.mode)
            snapshot = self.store.snapshot_for_order(order_id, customer_id=customer_id, mode=self.mode)
            context = self.store.delivery_context_for_order(order_id, customer_id=customer_id, mode=self.mode)
            if not requires_review(snapshot):
                return order, snapshot, None
            canonical = dict(snapshot)
            supplied = canonical.pop('quote_hash')
            if (supplied != order['quote_hash'] or _hash(canonical) != supplied
                    or snapshot['customer_id'] != customer_id or snapshot['mode'] != self.mode
                    or snapshot['total_gross_cent'] != order['amount_gross_cent'] or order['currency'] != 'eur'
                    or any(group['context_hash'] != _hash(context) for group in snapshot['shipments'])):
                raise ValueError()
            funds = self.journal.payment_binding(order_id, customer_id=customer_id, mode=self.mode)
            if funds is None:
                return order, snapshot, None
            binding = {**funds, 'context_hash': _hash(context), 'cart_hash': _hash(snapshot['request']),
                'requirements_hash': _hash(snapshot['review_requirements']),
                'proposals_hash': _hash(snapshot.get('assembly_proposals', {}))}
            return order, snapshot, binding
        except OrderReviewError:
            raise
        except Exception:
            raise OrderReviewError('review_binding_mismatch') from None

    def _authorization(self, binding):
        started = _stamp(self.clock())
        try:
            value = self.payment.authorization(binding['order_id'])
            now = _stamp(self.clock())
            if (not isinstance(value, Mapping) or value.get('verified') is not True or value.get('state') != 'authorized'
                    or any(type(value.get(k)) is not type(binding[k]) or value[k] != binding[k] for k in FUNDS)
                    or not started <= _stamp(value.get('observed_at')) <= now
                    or now + timedelta(seconds=self.margin) >= _stamp(value.get('authorized_until'))):
                raise ValueError()
            return _stamp(value['authorized_until']).isoformat()
        except Exception:
            raise OrderReviewError('authorization_unconfirmed') from None

    @staticmethod
    def _public(order, snapshot, row):
        required = requires_review(snapshot)
        evidence = json.loads(row['evidence_json']) if row and row['evidence_json'] else None
        result = {'required': required, 'order_id': order['id'], 'mode': order['mode'],
            'state': row['state'] if row else 'pending' if required else 'not_required',
            'quote_hash': order['quote_hash'], 'amount_gross_cent': order['amount_gross_cent'], 'currency': 'eur',
            'expires_at': row['expires_at'] if row else None, 'authorization_verified': row is not None,
            'review_requirements': deepcopy(snapshot.get('review_requirements', [])),
            'vehicle_ref': evidence.get('vehicle_ref', snapshot.get('vehicle_ref', '')) if evidence else snapshot.get('vehicle_ref', '')}
        if evidence:
            result['review_evidence'] = evidence
        return result

    def status(self, order_id, *, customer_id):
        order, snapshot, binding = self._load(order_id, customer_id)
        row = self.journal.get(binding, margin_seconds=self.margin,
                               project_expiry=order['status'] not in {'paid', 'cancelled'}) if binding else None
        return self._public(order, snapshot, row)

    def ensure(self, order_id, *, customer_id):
        order, snapshot, binding = self._load(order_id, customer_id)
        if not requires_review(snapshot):
            return self._public(order, snapshot, None)
        if binding is None:
            raise OrderReviewError('authorization_unconfirmed')
        historical = order['status'] in {'paid', 'cancelled'}
        row = self.journal.get(binding, margin_seconds=self.margin, project_expiry=not historical)
        if historical:
            if row is None:
                raise OrderReviewError('authorization_unconfirmed')
            return self._public(order, snapshot, row)
        if order['status'] != 'authorized':
            raise OrderReviewError('authorization_unconfirmed')
        if row is None:
            self.journal.ensure(binding, snapshot['review_requirements'], self._authorization(binding))
        elif row['state'] == 'expired':
            self.journal.expire(binding, margin_seconds=self.margin)
        return self.status(order_id, customer_id=customer_id)

    def require_approved(self, order_id, *, customer_id, binding=None):
        order, snapshot, actual = self._load(order_id, customer_id)
        if not requires_review(snapshot):
            return {'required': False, 'state': 'not_required'}
        if actual is None or binding is not None and any(binding.get(k) != actual[k] for k in FUNDS):
            raise OrderReviewError('review_binding_mismatch')
        row = self.journal.get(actual, margin_seconds=self.margin,
                               project_expiry=order['status'] not in {'paid', 'cancelled'})
        if row is None or row['state'] != 'approved':
            raise OrderReviewError('review_approval_required')
        return {**self._public(order, snapshot, row), 'verified': True, 'persisted': True,
                'binding_hash': _hash(actual)}

    @staticmethod
    def _evidence(snapshot, evidence):
        technical = any(r['code'] in {'fitment_unconfirmed', 'assembly_fitment_unconfirmed'}
                        for r in snapshot['review_requirements'])
        if evidence is None and not technical:
            return {'full_order_confirmed': True}
        fields = {'vehicle_ref', 'certificate_references', 'conditions_confirmed', 'full_order_confirmed'}
        if (not isinstance(evidence, Mapping) or set(evidence) != fields or not _text(evidence['vehicle_ref'])
                or evidence['conditions_confirmed'] is not True or evidence['full_order_confirmed'] is not True
                or not isinstance(evidence['certificate_references'], list)
                or not 1 <= len(evidence['certificate_references']) <= 20
                or any(not _text(ref) for ref in evidence['certificate_references'])
                or len(set(evidence['certificate_references'])) != len(evidence['certificate_references'])
                or snapshot.get('vehicle_ref') and snapshot['vehicle_ref'] != evidence['vehicle_ref']):
            raise OrderReviewError('review_evidence_required')
        return deepcopy(dict(evidence))

    def _decide(self, order_id, *, customer_id, expected_quote_hash, actor_id, evidence, state):
        order, snapshot, binding = self._load(order_id, customer_id)
        if (not requires_review(snapshot) or expected_quote_hash != order['quote_hash']
                or not isinstance(expected_quote_hash, str) or not HASH.fullmatch(expected_quote_hash)
                or not _text(actor_id, 128) or binding is None):
            raise OrderReviewError('review_binding_mismatch')
        parsed = self._evidence(snapshot, evidence) if state == 'approved' else None
        # Operator actions are provider-free. They consume only a hold already
        # verified and persisted by the worker; the saga independently checks
        # the actual hold again before any supplier or capture operation.
        row = self.journal.get(binding, margin_seconds=self.margin)
        if row is None or order['status'] != 'authorized':
            raise OrderReviewError('authorization_unconfirmed')
        if row['state'] == 'expired':
            raise OrderReviewError('review_expired')
        self.journal.decide(binding, state=state, actor_id=actor_id, evidence=parsed, margin_seconds=self.margin)
        return self.status(order_id, customer_id=customer_id)

    def approve(self, order_id, *, customer_id, expected_quote_hash, actor_id, evidence=None):
        return self._decide(order_id, customer_id=customer_id, expected_quote_hash=expected_quote_hash,
                            actor_id=actor_id, evidence=evidence, state='approved')

    def reject(self, order_id, *, customer_id, expected_quote_hash, actor_id):
        return self._decide(order_id, customer_id=customer_id, expected_quote_hash=expected_quote_hash,
                            actor_id=actor_id, evidence=None, state='rejected')
