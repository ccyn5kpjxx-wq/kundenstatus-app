"""Private workshop handling of financially accepted live shop orders."""
from datetime import datetime, timezone
import logging
from zoneinfo import ZoneInfo


HANDLED_TABLE = 'shop_workshop_handled_v1'
HANDLED_SCHEMA = '''CREATE TABLE IF NOT EXISTS shop_workshop_handled_v1 (
    order_id TEXT PRIMARY KEY REFERENCES shop_orders_v2(id), handled_at TEXT NOT NULL
)'''


def _queue_sql(tables):
    """One row per admitted order, using only indexed status/identity data."""
    predicates = ["o.mode='live'", "o.status IN ('authorized','paid')",
                  """NOT EXISTS (SELECT 1 FROM shop_payment_attempts_v2 p WHERE p.order_id=o.id
                                  AND p.state IN ('cancelled','failed'))"""]
    review_checks = []
    if 'shop_order_reviews_v1' in tables:
        predicates.append("""NOT EXISTS (SELECT 1 FROM shop_order_reviews_v1 r WHERE r.order_id=o.id
                             AND r.state IN ('rejected','expired'))""")
        review_checks.append("""EXISTS (SELECT 1 FROM shop_order_reviews_v1 r WHERE r.order_id=o.id
                                AND r.state='pending' AND julianday(r.expires_at)>julianday('now'))""")
    if 'shop_order_workflow_v2' in tables:
        predicates.append("""NOT EXISTS (SELECT 1 FROM shop_order_workflow_v2 w
                             WHERE w.order_id=o.id AND w.state='cancelled')""")
        # A workflow may already be awaiting review while its journal is still being created.
        no_review = (""" AND NOT EXISTS (SELECT 1 FROM shop_order_reviews_v1 r WHERE r.order_id=o.id)"""
                     if 'shop_order_reviews_v1' in tables else '')
        review_checks.append("""EXISTS (SELECT 1 FROM shop_order_workflow_v2 w
                                WHERE w.order_id=o.id AND w.state='awaiting_review')""" + no_review)
    if 'shop_fulfillment_v2' in tables:
        predicates.append("""NOT EXISTS (SELECT 1 FROM shop_fulfillment_v2 f WHERE f.order_id=o.id
                             AND (f.state='rejected' OR f.action_state='cancelled'))""")
    review_pending = ' OR '.join(f'({condition})' for condition in review_checks) or '0'
    handled = ('EXISTS (SELECT 1 FROM shop_workshop_handled_v1 h WHERE h.order_id=o.id)'
               if HANDLED_TABLE in tables else '0')
    return (f'SELECT o.id, {handled} AS acknowledged, ({review_pending}) AS review_pending '
            f'FROM shop_orders_v2 o WHERE {" AND ".join(predicates)}')


def pending_count(db, tables):
    query = _queue_sql(tables)
    return db.execute(f'SELECT COUNT(*) FROM ({query}) WHERE NOT acknowledged OR review_pending').fetchone()[0]


def add_workshop_status(db, tables, orders):
    if not orders:
        return
    ids = [order['id'] for order in orders]
    marks = ','.join('?' for _ in ids)
    rows = {r['id']: r for r in db.execute(_queue_sql(tables) + f' AND o.id IN ({marks})', ids)}
    for order in orders:
        row = rows.get(order['id'])
        handled = bool(row and row['acknowledged'] and not row['review_pending'])
        order['workshop_handled'] = handled
        order['workshop_pending'] = bool(row) and not handled
        order['processing_blocked_reason'] = ('Bitte zuerst die technische Auftragsprüfung abschließen.'
                                              if row and row['review_pending'] else None)


def appointment_handling_block(day, time, confirmed):
    """A future wish stays in the inbox until the separate calendar reservation exists."""
    if not day and not time:
        return None
    try:
        requested = datetime.fromisoformat(f'{day}T{time}').replace(tzinfo=ZoneInfo('Europe/Berlin'))
    except (TypeError, ValueError):
        return 'Bitte den gespeicherten Montagetermin prüfen.'
    if requested <= datetime.now(timezone.utc):
        return None
    if confirmed is None:
        return 'Der Werkstattkalender ist gerade nicht erreichbar. Bitte erneut versuchen.'
    if not confirmed:
        return 'Bitte zuerst den gewünschten Montagetermin bestätigen.'
    return None


def mark_handled(db, tables, order_id, confirmed_appointments):
    """Caller holds the shop write lock; calendar confirmations are separately durable."""
    row = db.execute(_queue_sql(tables) + ' AND o.id=?', (order_id,)).fetchone()
    if row is None:
        return 'Diese Bestellung ist nicht zur Werkstattbearbeitung freigegeben.'
    if row['review_pending']:
        return 'Bitte zuerst die technische Auftragsprüfung abschließen.'
    if row['acknowledged']:
        return None
    if 'shop_order_appointment_requests_v2' in tables:
        wish = db.execute('''SELECT requested_date, requested_time FROM shop_order_appointment_requests_v2
                             WHERE order_id=?''', (order_id,)).fetchone()
        if wish and appointment_handling_block(*wish, False):
            confirmed = None
            if callable(confirmed_appointments):
                try:
                    confirmed = order_id in confirmed_appointments([order_id])
                except Exception:
                    logging.getLogger(__name__).exception('Werkstattkalender für die Bearbeitungsfreigabe nicht erreichbar')
            reason = appointment_handling_block(*wish, confirmed)
            if reason:
                return reason
    if HANDLED_TABLE not in tables:
        db.execute(HANDLED_SCHEMA)
    db.execute('INSERT INTO shop_workshop_handled_v1 (order_id,handled_at) VALUES (?,?) '
               'ON CONFLICT(order_id) DO NOTHING', (order_id, datetime.now(timezone.utc).isoformat()))
    return None
