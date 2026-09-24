"""Read-only inventory of pre-payment MOS signatures awaiting a retention policy.

The hold payload contains the drawn signature before a rental is paid.  This
module deliberately has no deletion path: a released hold can still have an
unresolved Stripe operation, and the operator has not approved a retention
period for abandoned signatures.
"""

from collections import Counter
from datetime import datetime, timezone
import json


_REPORT_SQL = '''SELECT h.status,h.payload,h.session_id,h.payment_intent,h.mietvorgang_id,
    EXISTS (SELECT 1 FROM miet_checkout_contracts c WHERE c.hold_id=h.id) AS has_contract,
    EXISTS (SELECT 1 FROM miet_checkout_deposit_auths d WHERE d.hold_id=h.id) AS has_deposit,
    EXISTS (SELECT 1 FROM miet_checkout_creation_attempts a WHERE a.hold_id=h.id) AS has_attempt,
    EXISTS (SELECT 1 FROM miet_checkout_events e WHERE e.hold_id=h.id) AS has_event,
    EXISTS (SELECT 1 FROM miet_checkout_refunds r WHERE r.hold_id=h.id) AS has_refund,
    EXISTS (SELECT 1 FROM miet_checkout_cancellations x WHERE x.id=h.id) AS has_cancellation
    FROM miet_checkout_holds h'''


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


def _signed_at(payload):
    try:
        quote = json.loads(payload)['quote']
        if not isinstance(quote, dict):
            return False, None
        if not quote.get('signature_png_base64'):
            return False, None
        signed = datetime.fromisoformat(quote['signed_at'].replace('Z', '+00:00'))
        if signed.tzinfo is None:
            return True, None
        return True, signed.astimezone(timezone.utc)
    except (KeyError, TypeError, ValueError, AttributeError):
        # A malformed signed record needs manual review, never automatic cleanup.
        return True, None


def classify(row, cutoff=None):
    """Return a conservative, non-identifying reason for retaining a hold."""
    row = dict(row)
    has_signature, signed_at = _signed_at(row['payload'])
    if not has_signature:
        return 'no_signature'
    if signed_at is None:
        return 'invalid_signed_record'
    if row['status'] == 'confirmed' or row['mietvorgang_id'] or row['payment_intent'] or row['has_contract']:
        return 'contract_or_payment_record'
    if row['status'] == 'review':
        return 'manual_review'
    if row['status'] == 'pending':
        return 'pending'
    if row['status'] != 'released':
        return 'unknown_status'
    # Even released holds may later get a paid Checkout webhook or an unresolved
    # deposit response. Never call them isolated if *any* provider trace exists.
    if (row['session_id'] or row['has_deposit'] or row['has_attempt'] or row['has_event']
            or row['has_refund'] or row['has_cancellation']):
        return 'provider_or_financial_history'
    if cutoff is None:
        return 'isolated_released_no_policy_cutoff'
    if signed_at > cutoff:
        return 'isolated_released_newer_than_cutoff'
    return 'isolated_released_before_cutoff'


def retention_report(db, cutoff=None):
    """Aggregate a read-only report; never return names, images or hold IDs.

    The caller must choose and document any cutoff.  A technical candidate is
    not approval to delete it; legal holds, backups and other personal fields
    in the payload still require a separate policy and implementation.
    """
    if cutoff is not None and (not isinstance(cutoff, datetime) or cutoff.tzinfo is None):
        raise ValueError('A timezone-aware cutoff is required.')
    counts = Counter()
    for row in db.execute(_REPORT_SQL):
        counts[classify(row, cutoff)] += 1
    return {
        'cutoff_utc': cutoff.astimezone(timezone.utc).isoformat() if cutoff else None,
        'automatic_deletion': False,
        'total_holds': sum(counts.values()),
        'holds_with_signature': sum(n for reason, n in counts.items() if reason != 'no_signature'),
        'classes': dict(sorted(counts.items())),
    }
