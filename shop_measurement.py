"""Read-only, owner-scoped conversion evidence from the shop's payment ledger.

This module never contacts Stripe, a supplier or Google and does not create an
order, payment, queue item or tracking identifier in storage. The hosting HTTP
boundary must authenticate the buyer before invoking the store method. Optional
browser consent remains a separate requirement before transmitting to Google.
"""
from hashlib import sha256
import json
import re


def purchase_projection(connection, order):
    """Return minimal sale evidence, or None while the retained proof is absent.

    ``order`` is the server-owned, customer-bound OrderStore row. A successful
    return URL, Checkout Session completion and a card authorization are not
    captured funds. Require the independently settled capture action and its
    exact immutable order/attempt binding in addition to a verified paid event.
    """
    if (order['mode'] != 'live' or order['status'] != 'paid' or order['currency'] != 'eur'
            or type(order['amount_gross_cent']) is not int
            or not 0 < order['amount_gross_cent'] <= 100_000_000
            or not isinstance(order['id'], str)
            or not re.fullmatch(r'sho_[a-f0-9]{32}', order['id'])):
        return None
    # Production initializes this journal explicitly. An older or test-only
    # deployment without the capture journal must not infer purchase from paid.
    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='shop_payment_actions_v2'").fetchone() is None:
        return None
    attempt = connection.execute('''SELECT * FROM shop_payment_attempts_v2
        WHERE order_id=? AND state='paid' AND session_id IS NOT NULL
          AND intent_id IS NOT NULL''', (order['id'],)).fetchone()
    action = connection.execute('''SELECT * FROM shop_payment_actions_v2
        WHERE order_id=? AND operation='capture' AND state='captured' ''',
        (order['id'],)).fetchone()
    if attempt is None or action is None or action['intent_id'] != attempt['intent_id']:
        return None
    try:
        binding = json.loads(action['binding_json'])
        result = json.loads(action['result_json'])
    except (ValueError, TypeError):
        return None
    expected = {'order_id': order['id'], 'customer_id': order['customer_id'],
                'mode': 'live', 'currency': 'eur',
                'amount_gross_cent': order['amount_gross_cent'],
                'payment_attempt': attempt['id'], 'session_id': attempt['session_id'],
                'intent_id': attempt['intent_id'], 'quote_hash': order['quote_hash']}
    if (not isinstance(binding, dict) or set(binding) != set(expected)
            or any(type(binding[key]) is not type(value) or binding[key] != value
                   for key, value in expected.items())
            or not isinstance(result, dict) or set(result) != {'state', 'observed_at'}
            or result.get('state') != 'captured'
            or not isinstance(result.get('observed_at'), str)):
        return None
    event = connection.execute('''SELECT 1 FROM shop_payment_events_v2
        WHERE order_id=? AND attempt_id=? AND kind='paid' ''',
        (order['id'], attempt['id'])).fetchone()
    if event is None:
        return None
    # A pseudonymous, deterministic ID lets Google deduplicate a refreshed
    # status page without exposing our order ID, customer ID, address or email.
    identity = sha256(('gaertner-shop-purchase-v1:' + order['id']).encode('ascii')).hexdigest()
    return {'transaction_id': 'shop_ads_' + identity,
            'value': order['amount_gross_cent'] / 100, 'currency': 'EUR'}
