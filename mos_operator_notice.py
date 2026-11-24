"""Transactional notifications for the rental station. SMTP is worker-only."""

from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from hashlib import sha256
import json
import os
from urllib.parse import quote as urlquote
from zoneinfo import ZoneInfo

from mos_contract_delivery import _single_address, _smtp_send, confirmed_order


BERLIN = ZoneInfo('Europe/Berlin')
STATION_ADDRESS = 'info@autovermietung-mos.de'


def init_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_operator_notices (
        hold_id TEXT NOT NULL, event_type TEXT NOT NULL, status TEXT NOT NULL,
        enqueued_at TEXT NOT NULL, next_attempt_at TEXT NOT NULL, attempted_at TEXT,
        accepted_at TEXT, recipient TEXT, attempts INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (hold_id,event_type))''')
    db.execute('''CREATE INDEX IF NOT EXISTS idx_mos_operator_notices_due
        ON miet_checkout_operator_notices (status,next_attempt_at)''')


def enqueue(db, hold_id, event_type, quote):
    """Commit in the caller's transaction; a replay adds no second notice."""
    if event_type not in ('payment_pending', 'confirmed'):
        raise ValueError('Unbekannte Art der Stationsmeldung.')
    if quote.get('test_only') is not False or not quote.get('signed_at'):
        return
    now = datetime.now(timezone.utc).isoformat()
    db.execute('''INSERT INTO miet_checkout_operator_notices
        (hold_id,event_type,status,enqueued_at,next_attempt_at)
        VALUES (?,?,'queued',?,?) ON CONFLICT (hold_id,event_type) DO NOTHING RETURNING hold_id''',
        (hold_id, event_type, now, now)).fetchone()


def backfill_recent(db, now):
    """Cover a recent confirmation made by the old web release during rollout."""
    rows = db.execute('''SELECT h.id,h.payload,h.payment_intent FROM miet_checkout_holds h
        JOIN mietvorgaenge r ON r.id=h.mietvorgang_id
        LEFT JOIN miet_checkout_operator_notices n ON n.hold_id=h.id AND n.event_type='confirmed'
        LEFT JOIN miet_checkout_cancellations c ON c.id=h.id
        WHERE h.status='confirmed' AND r.status='aktiv' AND h.end_datum>=?
          AND n.hold_id IS NULL AND c.id IS NULL''', (now.astimezone(BERLIN).date().isoformat(),))
    for row in rows.fetchall():
        try:
            q = json.loads(row['payload'])['quote']
            if q.get('test_only') is not False or not q.get('signed_at'):
                continue
            signed = datetime.fromisoformat(q['signed_at'])
        except (ValueError, TypeError, KeyError, AttributeError):
            # Legacy rows without a usable signing time cannot qualify for this backfill.
            continue
        if (signed.tzinfo is not None and now - timedelta(hours=48) <= signed <= now
                and confirmed_order('confirmed', row['payment_intent'], q)):
            enqueue(db, row['id'], 'confirmed', q)


def _message(row, recipient, sender, display_name, origin):
    payload = json.loads(row['payload'])
    q, customer = payload['quote'], payload['customer']
    if q.get('test_only') is not False or not q.get('signed_at'):
        raise ValueError('Echte unterschriebene Bestellung fehlt.')
    confirmed = row['event_type'] == 'confirmed'
    if confirmed and not (row['mietvorgang_id'] and
            confirmed_order(row['hold_status'], row['payment_intent'], q)):
        raise ValueError('Bestätigter Mietvorgang fehlt.')
    title = 'Website-Buchung bestätigt' if confirmed else 'Website-Anfrage eingegangen'
    if confirmed:
        status = ('Buchung bestätigt. Zahlung bei Abholung.' if q.get('payment_method') == 'onsite'
                  else 'Buchung bestätigt. Onlinezahlung eingegangen.')
    else:
        status = {
            'pending': 'Unterschriebenes Angebot. Onlinezahlung steht noch aus.',
            'released': 'Der Zahlungsversuch ist beendet. Es liegt keine bestätigte Buchung vor.',
            'review': 'Der Zahlungsvorgang muss im Buchungsportal geprüft werden.',
        }[row['hold_status']]

    def local_slot(key):
        return datetime.fromisoformat(q[key]).astimezone(BERLIN).strftime('%d.%m.%Y, %H:%M Uhr')

    ref = urlquote(row['hold_id'], safe='')
    message = EmailMessage()
    message['To'] = recipient
    message['From'] = formataddr((display_name, sender))
    message['Subject'] = f"MOS · {title} · {q['vehicle_name']}"
    digest = sha256(f"{row['hold_id']}:{row['event_type']}".encode()).hexdigest()[:32]
    message['Message-ID'] = f"<mos-station-{digest}@{sender.rsplit('@', 1)[1]}>"
    message.set_content('\n'.join([
        status, '', f"Fahrzeug: {q['vehicle_name']}", f"Abholung: {local_slot('start_slot')}",
        f"Rückgabe: {local_slot('end_slot')}", f"Kunde: {customer['name']}",
        f"E-Mail: {customer['email']}", '', f"Im Buchungsportal öffnen: {origin}/mieten/admin?hold_id={ref}#hold-{ref}",
    ]))
    return message


def deliver_one(portal, key, mail_cfg, origin):
    now = datetime.now(timezone.utc).isoformat()
    db = portal.get_db()
    try:
        row = db.execute('''SELECT n.*, h.payload,h.status AS hold_status,h.payment_intent,
                h.mietvorgang_id,c.id AS cancellation FROM miet_checkout_operator_notices n
            JOIN miet_checkout_holds h ON h.id=n.hold_id
            LEFT JOIN miet_checkout_cancellations c ON c.id=h.id
            WHERE n.hold_id=? AND n.event_type=? AND n.status='queued' AND n.next_attempt_at<=?''',
            (*key, now)).fetchone()
        if not row:
            return
        row = dict(row)
        if row['cancellation'] or (row['event_type'] == 'payment_pending' and row['hold_status'] == 'confirmed'):
            db.execute("""UPDATE miet_checkout_operator_notices SET status='superseded'
                WHERE hold_id=? AND event_type=? AND status='queued'""", key)
            db.commit()
            return
        recipient = row['recipient'] or os.environ.get('MOS_OPERATOR_NOTIFICATION_EMAIL') or STATION_ADDRESS
        if not _single_address(recipient) or not _single_address(mail_cfg.get('from_address')):
            raise ValueError('Stations- oder Absenderadresse ist ungültig.')
        message = _message(row, recipient, mail_cfg['from_address'], mail_cfg.get('display_name', ''), origin)
        claimed = db.execute("""UPDATE miet_checkout_operator_notices
            SET status='sending',recipient=?,attempted_at=?,attempts=attempts+1
            WHERE hold_id=? AND event_type=? AND status='queued' AND next_attempt_at<=?""",
            (recipient, now, *key, now)).rowcount
        db.commit()
    finally:
        db.close()
    if claimed != 1:
        return
    result = _smtp_send(message, mail_cfg)
    stamp = datetime.now(timezone.utc)
    status = {'sent': 'sent', 'not_sent': 'queued', 'uncertain': 'review'}[result]
    retry = stamp + timedelta(seconds=min(3600, 60 * 2 ** min(row['attempts'], 6)))
    db = portal.get_db()
    try:
        db.execute("""UPDATE miet_checkout_operator_notices SET status=?,accepted_at=?,next_attempt_at=?
            WHERE hold_id=? AND event_type=? AND status='sending'""",
            (status, stamp.isoformat() if status == 'sent' else None, retry.isoformat(), *key))
        db.commit()
    finally:
        db.close()


def run_cycle(portal, mail_cfg, origin):
    """Run only inside the enabled live contract worker with its validated SMTP account."""
    now = datetime.now(timezone.utc)
    db = portal.get_db()
    try:
        backfill_recent(db, now)
        db.commit()
        due = db.execute("""SELECT hold_id,event_type FROM miet_checkout_operator_notices
            WHERE status='queued' AND next_attempt_at<=? ORDER BY enqueued_at LIMIT 100""",
            (datetime.now(timezone.utc).isoformat(),)).fetchall()
    finally:
        db.close()
    errors = 0
    for row in due:
        try:
            deliver_one(portal, (row['hold_id'], row['event_type']), mail_cfg, origin)
        except Exception:
            portal.app.logger.exception('MOS Stationsmeldung konnte nicht verarbeitet werden')
            errors += 1
    db = portal.get_db()
    try:
        stale = (now - timedelta(minutes=10)).isoformat()
        return errors + db.execute("""SELECT COUNT(*) AS n FROM miet_checkout_operator_notices
            WHERE status='review' OR (status='sending' AND attempted_at<?)""", (stale,)).fetchone()['n']
    finally:
        db.close()
