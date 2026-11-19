"""Manually recorded supplier delivery estimates for the internal order screen."""
from contextlib import closing
from datetime import date
import json

from shop_supplier_status import supplier_status_by_line


DATE_TABLE = 'shop_supplier_delivery_dates_v1'


class DeliveryDateConflict(Exception):
    pass


class InvalidDeliveryDate(ValueError):
    pass


class DeliveryOrderMissing(LookupError):
    pass


def delivery_dates(db, tables, order_id):
    if DATE_TABLE not in tables:
        return {}
    rows = db.execute('''SELECT child_key, delivery_date, version
        FROM shop_supplier_delivery_dates_v1 WHERE order_id=?''', (order_id,))
    return {key: {'date': day or '', 'version': version} for key, day, version in rows}


def save_delivery_date(storage, order_id, child_key, day, version):
    try:
        if day and date.fromisoformat(day).isoformat() != day:
            raise ValueError()
    except (TypeError, ValueError):
        raise InvalidDeliveryDate('Bitte ein gültiges Datum eingeben.') from None
    with closing(storage.connect()) as db, db:
        # Validate the supplier receipt and the displayed version in the same
        # write transaction. A cleared date retains its version for open tabs.
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT mode, substr(snapshot_json,1,131073) FROM shop_orders_v2 WHERE id=?',
                         (order_id,)).fetchone()
        if row is None:
            raise DeliveryOrderMissing('Bestellung nicht gefunden.')
        mode, raw = row
        if mode != 'live':
            raise DeliveryDateConflict('Ein Testauftrag hat keinen Liefertermin vom Lieferanten.')
        if not isinstance(raw, str) or len(raw.encode('utf-8')) > 131072:
            raise ValueError('invalid_order_snapshot')
        lines = json.loads(raw)['lines']
        if not isinstance(lines, list) or not 1 <= len(lines) <= 100:
            raise ValueError('invalid_order_snapshot')
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not any(s['state'] == 'accepted' and s['position_key'] == child_key
                   for s in supplier_status_by_line(db, tables, order_id, mode, lines)):
            raise DeliveryDateConflict('Für diese Position liegt keine bestätigte Lieferantenbestellung vor.')

        current = db.execute('''SELECT delivery_date, version FROM shop_supplier_delivery_dates_v1
            WHERE order_id=? AND child_key=?''', (order_id, child_key)).fetchone()
        if (current[1] if current else 0) != version:
            raise DeliveryDateConflict('Der Liefertermin wurde zwischenzeitlich geändert. Bitte den aktuellen Stand prüfen.')
        if (current[0] if current else None) == (day or None):
            return
        if current:
            db.execute('''UPDATE shop_supplier_delivery_dates_v1 SET delivery_date=?, version=version+1
                WHERE order_id=? AND child_key=?''', (day or None, order_id, child_key))
        else:
            db.execute('''INSERT INTO shop_supplier_delivery_dates_v1 (order_id,child_key,delivery_date,version)
                VALUES (?,?,?,1)''', (order_id, child_key, day))
