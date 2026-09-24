"""Electronic receipt of a paid MOS order, separate from booking acceptance.

The caller must first authenticate the Stripe event and retrieve/validate the
current Checkout Session from Stripe. This module deliberately makes no Stripe
request. Enqueue in the same database transaction as processing the event, also
when the paid hold ends in manual review. SMTP happens only in an enabled worker.
"""

from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from hashlib import sha256
import json

from mos_contract_delivery import _single_address, _smtp_send


def init_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_order_receipts (
        hold_id TEXT PRIMARY KEY, session_id TEXT NOT NULL UNIQUE,
        payment_intent TEXT NOT NULL, recipient TEXT NOT NULL,
        amount_cents INTEGER NOT NULL, currency TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL, source_event_id TEXT,
        provider_observed_at TEXT NOT NULL, enqueued_at TEXT NOT NULL,
        status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        attempted_at TEXT, next_attempt_at TEXT NOT NULL,
        accepted_at TEXT, detail TEXT NOT NULL DEFAULT '')''')


def _utc(value):
    value = value or datetime.now(timezone.utc)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError('Zeitpunkt braucht eine Zeitzone.')
    return value.astimezone(timezone.utc).isoformat()


def _snapshot(hold, session):
    """Check local consistency; this does not authenticate a Stripe response."""
    if not isinstance(hold, dict) or not isinstance(session, dict):
        raise ValueError('Bestellung oder Anbieterstatus fehlt.')
    try:
        payload_text = hold['payload']
        payload = json.loads(payload_text)
        quote, customer = payload['quote'], payload['customer']
        amount = quote['amount_cents']
        recipient = customer['email']
        session_id = session['id']
        payment_intent = session['payment_intent']
        metadata = session['metadata']
        if (quote.get('test_only') is not False
                or type(amount) is not int or amount <= 0
                or quote.get('currency') != 'eur'
                or not _single_address(recipient)
                or session.get('livemode') is not True
                or session.get('status') != 'complete'
                or session.get('payment_status') != 'paid'
                or session.get('mode') != 'payment'
                or not isinstance(session_id, str) or not session_id.startswith('cs_live_')
                or not isinstance(payment_intent, str) or not payment_intent.startswith('pi_')
                or hold.get('session_id') not in (None, session_id)
                or session.get('client_reference_id') != hold['id']
                or metadata.get('hold_id') != hold['id']
                or metadata.get('quote_hash') != hold['fingerprint']
                or session.get('amount_total') != amount
                or session.get('currency') != 'eur'):
            raise ValueError('Bezahlte Bestellung muss geprüft werden.')
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError('Bestelldaten müssen geprüft werden.') from exc
    return (session_id, payment_intent, recipient, amount, 'eur',
            sha256(payload_text.encode('utf-8')).hexdigest())


def enqueue(db, hold, session, *, source_event_id=None, observed_at=None):
    """Persist once for any verified paid live Checkout, including review holds.

    ``session`` must come from an authenticated Stripe retrieve after the
    signed webhook (or another trusted server-side check), never from a browser
    or the webhook payload alone. The caller owns and commits the transaction.
    Returns True when inserted, False for a consistent duplicate.
    """
    snapshot = _snapshot(hold, session)
    if source_event_id is not None and (not isinstance(source_event_id, str)
                                    or not source_event_id.startswith('evt_')):
        raise ValueError('Ereignisreferenz muss geprüft werden.')
    observed = _utc(observed_at)
    now = _utc(None)
    result = db.execute('''INSERT INTO miet_checkout_order_receipts
        (hold_id,session_id,payment_intent,recipient,amount_cents,currency,
         payload_sha256,source_event_id,provider_observed_at,enqueued_at,status,next_attempt_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,'queued',?) ON CONFLICT (hold_id) DO NOTHING''',
        (hold['id'], *snapshot, source_event_id, observed, now, now))
    if result.rowcount == 1:
        return True
    row = db.execute('SELECT * FROM miet_checkout_order_receipts WHERE hold_id=?',
                     (hold['id'],)).fetchone()
    if not row or tuple(row[key] for key in ('session_id', 'payment_intent', 'recipient',
                                             'amount_cents', 'currency', 'payload_sha256')) != snapshot:
        raise ValueError('Eingangsbestätigung kollidiert mit anderen Bestelldaten.')
    return False


def _message(row):
    message = EmailMessage()
    message['To'] = row['recipient']
    message['Subject'] = 'Eingang Ihrer MOS-Online-Bestellung'
    euros, cents = divmod(row['amount_cents'], 100)
    message.set_content(
        'Wir bestätigen den Eingang Ihrer Online-Bestellung für die Fahrzeugmiete.\n\n'
        f"Bestellreferenz: {row['hold_id']}\n"
        f'Mietpreis laut Bestellung: {euros},{cents:02d} EUR\n\n'
        'Der Zahlungsanbieter meldet die Mietpreiszahlung als abgeschlossen. '
        'Diese Eingangsbestätigung ist keine zusätzliche Annahme oder Zusage '
        'einer Fahrzeugübergabe. Falls die Bestellung geprüft werden muss, '
        'informiert Sie die Werkstatt über das Ergebnis.\n\n'
        'Vermieter: Gärtner GmbH Karosserie + Lack\n')
    return message


def _verified_row(db, hold_id):
    row = db.execute('''SELECT r.*,h.payload,h.fingerprint,h.session_id AS hold_session_id,
        h.id AS existing_hold_id FROM miet_checkout_order_receipts r
        LEFT JOIN miet_checkout_holds h ON h.id=r.hold_id
        WHERE r.hold_id=?''', (hold_id,)).fetchone()
    if not row:
        raise ValueError('Keine Eingangsbestätigung vorhanden.')
    row = dict(row)
    if (not row['existing_hold_id'] or not _single_address(row['recipient'])
            or row['hold_session_id'] not in (None, row['session_id'])
            or not row['session_id'].startswith('cs_live_')
            or not row['payment_intent'].startswith('pi_')
            or row['currency'] != 'eur' or row['amount_cents'] <= 0
            or sha256(row['payload'].encode('utf-8')).hexdigest() != row['payload_sha256']):
        raise ValueError('Eingangsbestätigung muss manuell geprüft werden.')
    payload = json.loads(row['payload'])
    if (payload['quote'].get('test_only') is not False
            or payload['quote']['amount_cents'] != row['amount_cents']
            or payload['customer']['email'] != row['recipient']):
        raise ValueError('Eingangsbestätigung muss manuell geprüft werden.')
    return row


def deliver_one(portal, hold_id, cfg, *, live, enabled):
    """At-most-once SMTP claim; uncertain DATA outcomes require manual review."""
    if not live or not enabled or not cfg.get('smtp_configured') or not (cfg.get('smtp_ssl') or cfg.get('smtp_tls')):
        return 'disabled'
    if not _single_address(cfg.get('from_address')) or not cfg.get('_smtp_password'):
        return 'disabled'
    db = portal.get_db()
    try:
        try:
            row = _verified_row(db, hold_id)
            if row['status'] != 'queued':
                return row['status']
            message = _message(row)
            message['From'] = formataddr((cfg.get('display_name', ''), cfg['from_address']))
            message['Message-ID'] = ('<mos-order-'
                                     + sha256(hold_id.encode()).hexdigest()[:32]
                                     + '@' + cfg['from_address'].rsplit('@', 1)[1] + '>')
        except (ValueError, KeyError, TypeError, json.JSONDecodeError):
            db.execute("UPDATE miet_checkout_order_receipts SET status='review',detail=? WHERE hold_id=? AND status='queued'",
                       ('Bestelldaten oder Empfänger müssen geprüft werden.', hold_id))
            db.commit()
            return 'review'
        now = _utc(None)
        claim = db.execute('''UPDATE miet_checkout_order_receipts
            SET status='sending',attempts=attempts+1,attempted_at=?,detail=''
            WHERE hold_id=? AND status='queued' AND next_attempt_at<=?''',
            (now, hold_id, now))
        db.commit()  # Durable fence before network I/O.
        if claim.rowcount != 1:
            return 'not_due'
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    outcome = _smtp_send(message, cfg)
    db = portal.get_db()
    try:
        stamp = datetime.now(timezone.utc)
        if outcome == 'sent':
            db.execute('''UPDATE miet_checkout_order_receipts
                SET status='sent',accepted_at=?,detail='' WHERE hold_id=? AND status='sending' ''',
                (stamp.isoformat(), hold_id))
        elif outcome == 'not_sent':
            delay = min(3600, 60 * (2 ** min(row['attempts'], 6)))
            db.execute('''UPDATE miet_checkout_order_receipts
                SET status='queued',next_attempt_at=?,detail=? WHERE hold_id=? AND status='sending' ''',
                ((stamp + timedelta(seconds=delay)).isoformat(),
                 'Mailserver hat die Nachricht nicht angenommen; erneuter Versuch vorgesehen.', hold_id))
        else:
            db.execute('''UPDATE miet_checkout_order_receipts
                SET status='review',detail=? WHERE hold_id=? AND status='sending' ''',
                ('Mailserver-Antwort unklar; vor erneutem Versand manuell prüfen.', hold_id))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return outcome


def pending_ids(portal, limit=20):
    db = portal.get_db()
    try:
        now = _utc(None)
        return [r['hold_id'] for r in db.execute('''SELECT hold_id FROM miet_checkout_order_receipts
            WHERE status='queued' AND next_attempt_at<=?
            ORDER BY enqueued_at LIMIT ?''', (now, max(1, min(int(limit), 100)))).fetchall()]
    finally:
        db.close()


def unresolved_count(portal):
    db = portal.get_db()
    try:
        return db.execute("SELECT COUNT(*) AS n FROM miet_checkout_order_receipts WHERE status!='sent'").fetchone()['n']
    finally:
        db.close()
