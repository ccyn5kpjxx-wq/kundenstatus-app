"""Fail-closed retention for abandoned MOS pre-payment signatures.

Dry-run is the default. A real purge needs an explicit documented policy,
separate runtime enablement and migrated legal-hold/observation tables. The
code never touches paid, review, pending or provider-linked bookings.
"""

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json


PROPOSED_RETENTION_DAYS = 30  # Operator/legal proposal, not a statutory period.
POLICY_SCOPE = 'provider_untouched_released_holds_v1'


@dataclass(frozen=True)
class RetentionPolicy:
    days: int = PROPOSED_RETENTION_DAYS
    approval_ref: str = ''

    def __post_init__(self):
        if type(self.days) is not int or self.days < 1:
            raise ValueError('A positive, documented retention period is required.')
        if not isinstance(self.approval_ref, str):
            raise ValueError('Invalid policy approval reference.')


_REPORT_SQL = '''SELECT h.id,h.status,h.payload,h.expires_at,h.session_id,h.payment_intent,
    h.mietvorgang_id,
    EXISTS (SELECT 1 FROM miet_checkout_contracts c WHERE c.hold_id=h.id) AS has_contract,
    EXISTS (SELECT 1 FROM miet_checkout_contract_delivery m WHERE m.hold_id=h.id) AS has_delivery,
    EXISTS (SELECT 1 FROM miet_checkout_order_receipts o WHERE o.hold_id=h.id) AS has_order_receipt,
    EXISTS (SELECT 1 FROM miet_checkout_deposit_auths d WHERE d.hold_id=h.id) AS has_deposit,
    EXISTS (SELECT 1 FROM miet_checkout_creation_attempts a WHERE a.hold_id=h.id) AS has_attempt,
    EXISTS (SELECT 1 FROM miet_checkout_events e WHERE e.hold_id=h.id) AS has_event,
    EXISTS (SELECT 1 FROM miet_checkout_refunds r WHERE r.hold_id=h.id) AS has_refund,
    EXISTS (SELECT 1 FROM miet_checkout_cancellations x WHERE x.id=h.id) AS has_cancellation,
    EXISTS (SELECT 1 FROM miet_checkout_handovers u WHERE u.hold_id=h.id) AS has_handover,
    EXISTS (SELECT 1 FROM miet_checkout_retention_blocks b WHERE b.hold_id=h.id) AS has_legal_hold,
    (SELECT s.first_seen_released_at FROM miet_checkout_retention_seen s WHERE s.hold_id=h.id)
        AS first_seen_released_at
    FROM miet_checkout_holds h'''

_DELETE_SQL = '''DELETE FROM miet_checkout_holds WHERE id=? AND status='released'
    AND session_id IS NULL AND payment_intent IS NULL AND mietvorgang_id IS NULL
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_contracts c WHERE c.hold_id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_contract_delivery m WHERE m.hold_id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_order_receipts o WHERE o.hold_id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_deposit_auths d WHERE d.hold_id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_creation_attempts a WHERE a.hold_id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_events e WHERE e.hold_id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_refunds r WHERE r.hold_id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_cancellations x WHERE x.id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_handovers u WHERE u.hold_id=miet_checkout_holds.id)
    AND NOT EXISTS (SELECT 1 FROM miet_checkout_retention_blocks b WHERE b.hold_id=miet_checkout_holds.id)'''


def init_schema(db):
    """Idempotent migration, called only by an explicit operator command."""
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_retention_blocks (
        hold_id TEXT PRIMARY KEY, reason TEXT NOT NULL, set_at TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_retention_seen (
        hold_id TEXT PRIMARY KEY, first_seen_released_at TEXT NOT NULL)''')


def parse_cutoff(value):
    """Parse an explicit UTC cutoff; never infer a retention duration."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError('A UTC cutoff must be an ISO-8601 string.')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError('Invalid ISO-8601 cutoff.') from exc
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError('The cutoff must explicitly use UTC.')
    if parsed >= datetime.now(timezone.utc):
        raise ValueError('The cutoff must be in the past.')
    return parsed


def _utc(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (AttributeError, TypeError, ValueError):
        return None


def _expiry(value):
    try:
        return datetime.fromtimestamp(int(value), timezone.utc)
    except (OverflowError, TypeError, ValueError):
        return None


def _signed_quote(payload):
    try:
        quote = json.loads(payload)['quote']
        if not isinstance(quote, dict):
            return True, None
        if not quote.get('signature_png_base64'):
            return False, None
        if (quote.get('deposit_method') != 'card_authorization_at_booking'
                or quote.get('slot_policy') != 'db_open_slots_v1'
                or quote.get('test_only') is not False
                or not isinstance(quote.get('signed_contract_hash'), str)
                or len(quote['signed_contract_hash']) != 64
                or not isinstance(quote.get('signature_record_hash'), str)
                or len(quote['signature_record_hash']) != 64):
            return True, None  # Historic/unknown quote type requires review.
        return True, _utc(quote.get('signed_at'))
    except (KeyError, TypeError, ValueError, AttributeError):
        return True, None


def classify(row, cutoff=None):
    """Return a conservative non-identifying reason for retaining a hold."""
    row = dict(row)
    has_signature, signed_at = _signed_quote(row['payload'])
    if not has_signature:
        return 'no_signature'
    expires = _expiry(row['expires_at'])
    if signed_at is None or expires is None or expires < signed_at:
        return 'invalid_or_legacy_signed_record'
    if row['status'] == 'confirmed' or row['mietvorgang_id'] or row['payment_intent'] or row['has_contract']:
        return 'contract_or_payment_record'
    if row['status'] == 'review':
        return 'manual_review'
    if row['status'] == 'pending':
        return 'pending'
    if row['status'] != 'released':
        return 'unknown_status'
    if row['has_legal_hold']:
        return 'legal_hold'
    # A released hold can still have a paid Checkout or unresolved card hold.
    if (row['session_id'] or row['has_delivery'] or row['has_order_receipt']
            or row['has_deposit'] or row['has_attempt']
            or row['has_event'] or row['has_refund'] or row['has_cancellation']
            or row['has_handover']):
        return 'provider_or_financial_history'
    if cutoff is None:
        return 'isolated_released_no_policy_cutoff'
    observed = _utc(row['first_seen_released_at'])
    if row['first_seen_released_at'] and observed is None:
        return 'invalid_observation_timestamp'
    if observed is None:
        return 'isolated_released_not_observed'
    if signed_at > cutoff or expires > cutoff or observed > cutoff:
        return 'isolated_released_newer_than_cutoff'
    return 'isolated_released_before_cutoff'


def retention_report(db, cutoff=None):
    """Aggregate a read-only report; never return names, images or hold IDs."""
    if cutoff is not None and (not isinstance(cutoff, datetime) or cutoff.tzinfo is None):
        raise ValueError('A timezone-aware cutoff is required.')
    counts = Counter()
    for row in db.execute(_REPORT_SQL).fetchall():
        counts[classify(row, cutoff)] += 1
    return {
        'cutoff_utc': cutoff.astimezone(timezone.utc).isoformat() if cutoff else None,
        'automatic_deletion': False,
        'total_holds': sum(counts.values()),
        'holds_with_signature': sum(n for reason, n in counts.items() if reason != 'no_signature'),
        'classes': dict(sorted(counts.items())),
    }


def block_hold(db, hold_id, reason, *, postgres=False, now=None):
    """Record a case-specific legal hold while locking the same booking row."""
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError('A documented legal-hold reason is required.')
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Timezone-aware time required.')
    try:
        if not postgres:
            db.execute('BEGIN IMMEDIATE')
        suffix = ' FOR UPDATE' if postgres else ''
        row = db.execute('SELECT id FROM miet_checkout_holds WHERE id=?' + suffix, (hold_id,)).fetchone()
        if row is None:
            raise ValueError('Booking hold not found.')
        db.execute('''INSERT INTO miet_checkout_retention_blocks (hold_id,reason,set_at)
            VALUES (?,?,?) ON CONFLICT (hold_id) DO UPDATE SET reason=excluded.reason,
                set_at=excluded.set_at RETURNING hold_id''',
            (hold_id, reason.strip(), now.astimezone(timezone.utc).isoformat())).fetchone()
        db.commit()
    except Exception:
        db.rollback()
        raise


def cleanup(db, policy=None, *, apply=False, postgres=False, now=None):
    """Observe or purge only isolated released holds after explicit approval.

    Apply first marks qualifying holds as seen, then waits the full configured
    period from that mark. Every deletion rechecks provider/financial/legal
    references while the hold row is locked. No scheduled runner is installed.
    """
    policy = policy or RetentionPolicy()
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('Timezone-aware time required.')
    now = now.astimezone(timezone.utc)
    cutoff = now - timedelta(days=policy.days)
    if not apply:
        return {'observed': 0, 'deleted': 0, 'report': retention_report(db, cutoff)}
    if not policy.approval_ref.strip():
        raise ValueError('Actual cleanup requires a documented policy approval reference.')
    observed, deleted = 0, 0
    try:
        if not postgres:
            db.execute('BEGIN IMMEDIATE')
        suffix = ' FOR UPDATE OF h' if postgres else ''
        rows = db.execute(_REPORT_SQL + " WHERE h.status='released'" + suffix).fetchall()
        for row in rows:
            category = classify(row, cutoff)
            if category == 'isolated_released_not_observed':
                db.execute('''INSERT INTO miet_checkout_retention_seen
                    (hold_id,first_seen_released_at) VALUES (?,?)
                    ON CONFLICT (hold_id) DO NOTHING RETURNING hold_id''',
                    (row['id'], now.isoformat())).fetchone()
                observed += 1
            elif category == 'isolated_released_before_cutoff':
                cursor = db.execute(_DELETE_SQL, (row['id'],))
                if cursor.rowcount != 1:
                    raise RuntimeError('Booking changed during retention cleanup; rolled back.')
                db.execute('DELETE FROM miet_checkout_retention_seen WHERE hold_id=?', (row['id'],))
                deleted += 1
        result = {'observed': observed, 'deleted': deleted,
                  'report': retention_report(db, cutoff)}
        db.commit()
        return result
    except Exception:
        db.rollback()
        raise
