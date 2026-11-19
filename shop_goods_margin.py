"""Private merchandise costing. Supplier observations stay outside the customer quote."""
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
import json
import re


def _cent(value):
    return int(value.quantize(Decimal('1'), rounding=ROUND_HALF_UP))


def _purchase_cent(unit_net, quantity):
    if (not isinstance(unit_net, str) or not re.fullmatch(r'[0-9]{1,7}(?:\.[0-9]{1,4})?', unit_net)
            or type(quantity) is not int or not 1 <= quantity <= 20 or Decimal(unit_net) <= 0):
        return None
    return _cent(Decimal(unit_net) * quantity * 100)


def retained_goods_cost(snapshot, retained_inquiry):
    """Read already observed net prices at the quoted quantity, without supplier IO."""
    if not callable(retained_inquiry):
        return None
    total = 0
    for group in snapshot['shipments']:
        for item in group['private_items']:
            qty = item['quantity']
            proof = retained_inquiry(item, qty)
            if (not isinstance(proof, dict) or proof.get('confirmed') is not True
                    or proof.get('product_id') != item['id'] or proof.get('identity_hash') != item['identity_hash']
                    or proof.get('supplier_article_id') != item['mapping']['supplier_article_id']
                    or proof.get('mapping_evidence_id') != item['mapping']['evidence_id']
                    or proof.get('quantity') != qty or proof.get('currency') != 'EUR' or proof.get('mode') != snapshot['mode']):
                return None
            cost = _purchase_cent(proof.get('unit_net'), qty)
            if cost is None:
                return None
            total += cost
    return total


def _accepted_purchase(positions, snapshot):
    """Use submitted net prices only when every frozen goods position was accepted."""
    expected = {(group['destination'], item['id']): item for group in snapshot['shipments']
                for item in group['private_items']}
    if not expected or len(positions) != len(expected):
        return None
    total = 0
    for row in positions:
        if row['state'] != 'accepted' or not row['payload_json']:
            return None
        item = json.loads(row['item_json'])
        if expected.pop((row['destination'], item['id']), None) != item:
            return None
        payload = json.loads(row['payload_json'])
        parts = payload['positions']
        if len(parts) != 1:
            return None
        part = parts[0]
        if (part.get('currency') != 'EUR' or part.get('quantity') != item['quantity']
                or part.get('supplier_article_id') != item['mapping']['supplier_article_id']):
            return None
        cost = _purchase_cent(part.get('unit_net'), item['quantity'])
        if cost is None:
            return None
        total += cost
    return total


def _money(cent):
    amount = abs(cent)
    euros = f'{amount // 100:,}'.replace(',', '.')
    return f'{"−" if cent < 0 else ""}{euros},{amount % 100:02d} €'


def admin_goods_margins(db, tables, order_ids):
    """Bounded page of owner-only projections, including journals predating frozen costs."""
    if not order_ids:
        return {}
    marks = ','.join('?' for _ in order_ids)
    costs = {}
    if 'shop_quote_goods_cost_v1' in tables:
        costs = dict(db.execute(f'''SELECT o.id, c.cost_net_cent FROM shop_orders_v2 o
            JOIN shop_quote_goods_cost_v1 c ON c.quote_hash=o.quote_hash WHERE o.id IN ({marks})''', order_ids))
    positions = defaultdict(list)
    if 'shop_fulfillment_positions_v3' in tables:
        for row in db.execute(f'''SELECT order_id,destination,state,item_json,payload_json
                FROM shop_fulfillment_positions_v3 WHERE order_id IN ({marks})''', order_ids):
            positions[row['order_id']].append(row)
    margins = {}
    for row in db.execute(f'''SELECT id,status,currency,snapshot_json FROM shop_orders_v2
                              WHERE id IN ({marks})''', order_ids):
        oid = row['id']
        details = margins[oid] = {'status': 'EK fehlt'}
        if row['status'] in ('cancelled', 'payment_failed'):
            details['status'] = 'Kein Umsatz'
            continue
        try:
            snapshot = json.loads(row['snapshot_json'])
            if row['currency'] != 'eur' or snapshot['vat_percent'] != '19.00':
                continue
            gross = [line['items_gross_cent'] for line in snapshot['lines']]
            if not gross or any(type(amount) is not int or amount <= 0 for amount in gross):
                continue
            sale = _cent(Decimal(sum(gross)) * 100 / 119)
            if sale <= 0:
                continue
            details['sale'] = _money(sale)
            cost = _accepted_purchase(positions[oid], snapshot)
            source = 'Lieferantenbestellung'
            if cost is None:
                cost = costs.get(oid)
                source = 'EK bei Preisprüfung'
            if type(cost) is not int or cost <= 0:
                continue
            profit = sale - cost
            percent = (Decimal(profit) * 100 / sale).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
            details.update(status='Kalkuliert', source=source, purchase=_money(cost), amount=_money(profit),
                           percent=format(percent, 'f').replace('.', ',') + ' %')
        except (KeyError, ValueError, TypeError, AttributeError):
            details['status'] = 'Daten prüfen'
    return margins
