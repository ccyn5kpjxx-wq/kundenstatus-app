"""Read the saved RSU acknowledgements for an internal order detail view."""
from collections import Counter
import json
import re


_LABELS = {
    'accepted': 'Bei TYREsystem bestellt',
    'rejected': 'Von TYREsystem abgelehnt',
    'pending': 'Noch nicht an TYREsystem gesendet',
    'uncertain': 'Lieferantenannahme ungeklärt',
    'undocumented': 'Kein Lieferantenbeleg hinterlegt',
}
_HASH = re.compile(r'[a-f0-9]{64}')


def _status(state, key=None, number=None):
    return {'state': state, 'label': _LABELS[state], 'position_key': key, 'order_number': number}


def _ack_status(row, item, mode):
    key, state, used, reference, digest, raw_receipt = row
    if state == 'pending' and not used and not raw_receipt:
        return _status('pending', key)
    if state not in {'accepted', 'rejected'} or used != 1 or not isinstance(digest, str) or not _HASH.fullmatch(digest):
        return _status('uncertain', key)

    try:
        receipt = json.loads(raw_receipt)
        if (receipt['reference'] != reference or receipt['request_hash'] != digest or receipt['outcome'] != state
                or len(receipt['positions']) != 1):
            return _status('uncertain', key)
        position = receipt['positions'][0]
        if (position['pos'] != 1 or position['outcome'] != state or position['request_bound'] is not True
                or position['supplier_article_id'] != item['mapping']['supplier_article_id']
                or type(position['quantity']) is not int or position['quantity'] != item['quantity']):
            return _status('uncertain', key)
        number = position['order_number']
        if state == 'rejected' and number is None:
            return _status('rejected', key)
        if state == 'accepted' and isinstance(number, str):
            if re.fullmatch(r'[1-9][0-9]{0,29}', number) or mode == 'test' and re.fullmatch(r'Test_[0-9]{1,25}', number):
                return _status('accepted', key, number)
    except (ValueError, TypeError, KeyError, IndexError, RecursionError):
        pass
    return _status('uncertain', key)


def supplier_status_by_line(db, tables, order_id, mode, lines):
    """One safe view per frozen shop line; a supplier position can cover several lines."""
    required = {'shop_fulfillment_v2', 'shop_fulfillment_positions_v3'}
    if mode not in {'test', 'live'} or not required <= tables:
        return [_status('undocumented') for _ in lines]

    demand = Counter()
    keys = []
    for line in lines:
        if (not isinstance(line, dict) or line.get('destination') not in ('customer', 'workshop')
                or not isinstance(line.get('id'), str) or not isinstance(line.get('identity_hash'), str)
                or type(line.get('quantity')) is not int or line['quantity'] <= 0):
            keys.append(None)
            continue
        identity = (line['destination'], line['id'], line['identity_hash'])
        keys.append(identity)
        demand[identity] += line['quantity']

    matched = {}
    rows = db.execute('''SELECT p.destination, substr(p.item_json,1,16385),
               p.child_key, p.state, p.grant_used, p.reference, p.request_hash, substr(p.receipt_json,1,65537)
        FROM shop_fulfillment_positions_v3 p
        JOIN shop_fulfillment_v2 f ON f.order_id = p.order_id
        WHERE p.order_id = ? AND f.mode = ?''', (order_id, mode))
    for destination, raw_item, *row in rows:
        try:
            item = json.loads(raw_item)
            identity = (destination, item['id'], item['identity_hash'])
            if identity not in demand:
                continue
            if (identity in matched or type(item['quantity']) is not int or item['quantity'] != demand[identity]
                    or not isinstance(row[0], str) or not _HASH.fullmatch(row[0])):
                matched[identity] = _status('uncertain')
            else:
                matched[identity] = _ack_status(row, item, mode)
        except (ValueError, TypeError, KeyError, RecursionError):
            continue
    return [matched.get(key) or _status('undocumented') for key in keys]
