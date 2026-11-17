"""One deposit reminder per confirmed advance booking; no payment-provider calls.

SMTP claims are committed before network I/O. An uncertain DATA outcome is never
retried automatically. Access links grant only the individual deposit scope.
"""

from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from hashlib import sha256
import hmac
import json
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from itsdangerous import BadSignature, URLSafeSerializer

from mos_contract_delivery import _single_address, _smtp_send
from mos_company_identity import LEGAL_NAME, business_letter_details
from werkstatt_mail_identity import add_workshop_mail_footer


TABLE = 'miet_checkout_deposit_reminders'
TOKEN_SALT = 'mos-deposit-access-v1'
BERLIN = ZoneInfo('Europe/Berlin')


def _now(value=None):
    value = value or datetime.now(timezone.utc)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError('Zeitpunkt braucht eine Zeitzone.')
    return value.astimezone(timezone.utc)


def init_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_deposit_reminders (
        hold_id TEXT PRIMARY KEY, recipient TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL, start_slot TEXT NOT NULL,
        enqueued_at TEXT NOT NULL, status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, attempted_at TEXT,
        next_attempt_at TEXT NOT NULL, accepted_at TEXT,
        detail TEXT NOT NULL DEFAULT '')''')


def _booking(db, hold_id):
    row = db.execute('''SELECT h.*,r.status AS rental_status,
        r.rueckgabe_datum AS returned_at,d.status AS deposit_status,
        d.capture_before AS deposit_capture_before,
        EXISTS (SELECT 1 FROM miet_checkout_cancellations c WHERE c.id=h.id) AS canceled
        FROM miet_checkout_holds h
        LEFT JOIN mietvorgaenge r ON r.id=h.mietvorgang_id
        LEFT JOIN miet_checkout_deposit_auths d ON d.hold_id=h.id
        WHERE h.id=?''', (hold_id,)).fetchone()
    if row is None:
        raise ValueError('Buchungszugang ist nicht verfügbar.')
    return dict(row)


def _snapshot(hold):
    try:
        payload = json.loads(hold['payload'])
        quote = payload['quote']
        owner = quote['owner_hash']
        start = datetime.fromisoformat(quote['start_slot'])
        end = datetime.fromisoformat(quote['end_slot'])
        if (hold.get('status') != 'confirmed' or not hold.get('mietvorgang_id')
                or hold.get('rental_status') != 'aktiv' or hold.get('returned_at')
                or hold.get('canceled') or not str(hold.get('session_id', '')).startswith('cs_live_')
                or not str(hold.get('payment_intent', '')).startswith('pi_')
                or quote.get('test_only') is not False
                or quote.get('deposit_timing') != 'before_pickup'
                or quote.get('deposit_method') != 'card_authorization_at_booking'
                or not isinstance(owner, str) or len(owner) != 64
                or any(c not in '0123456789abcdef' for c in owner)
                or start.tzinfo is None or end.tzinfo is None or end <= start
                or not _single_address(payload['customer']['email'])):
            raise ValueError('Buchungszugang ist nicht verfügbar.')
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError('Buchungszugang ist nicht verfügbar.') from exc
    return payload, quote, start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def _serializer(portal):
    secret = portal.app.secret_key
    if not isinstance(secret, (str, bytes)) or len(secret) < 32:
        raise ValueError('Sicherer Buchungszugang ist nicht eingerichtet.')
    return URLSafeSerializer(secret, salt=TOKEN_SALT)


def create_access_token(portal, hold, now=None):
    """Sign an immutable, active booking snapshot; never include recipient data."""
    stamp = _now(now)
    db = portal.get_db()
    try:
        current = _booking(db, hold['id'])
        if current['payload'] != hold['payload']:
            raise ValueError('Buchungszugang ist nicht verfügbar.')
        _, quote, start, _ = _snapshot(current)
    finally:
        db.close()
    expires = start + timedelta(days=1)
    if stamp >= expires:
        raise ValueError('Buchungszugang ist abgelaufen.')
    return _serializer(portal).dumps({
        'hold_id': current['id'], 'owner_hash': quote['owner_hash'], 'scope': 'deposit',
        'payload_sha256': sha256(current['payload'].encode()).hexdigest(),
        'expires_at': int(expires.timestamp()),
    })


def validate_access_token(portal, token, now=None):
    """Recheck signature, fixed expiry and current booking before granting scope."""
    stamp = _now(now)
    try:
        if not isinstance(token, str) or not token or len(token) > 4096:
            raise ValueError('Buchungszugang ist ungültig oder abgelaufen.')
        data = _serializer(portal).loads(token)
        if (not isinstance(data, dict)
                or set(data) != {'hold_id', 'owner_hash', 'scope', 'payload_sha256', 'expires_at'}
                or data['scope'] != 'deposit' or type(data['expires_at']) is not int
                or not isinstance(data['hold_id'], str) or not 1 <= len(data['hold_id']) <= 200
                or not isinstance(data['owner_hash'], str) or not isinstance(data['payload_sha256'], str)
                or stamp.timestamp() >= data['expires_at']):
            raise ValueError('Buchungszugang ist ungültig oder abgelaufen.')
        db = portal.get_db()
        try:
            hold = _booking(db, data['hold_id'])
            _, quote, start, _ = _snapshot(hold)
        finally:
            db.close()
        if (data['expires_at'] != int((start + timedelta(days=1)).timestamp())
                or not hmac.compare_digest(data['owner_hash'], quote['owner_hash'])
                or not hmac.compare_digest(data['payload_sha256'], sha256(hold['payload'].encode()).hexdigest())):
            raise ValueError('Buchungszugang ist ungültig oder abgelaufen.')
        return data
    except (BadSignature, KeyError, TypeError) as exc:
        raise ValueError('Buchungszugang ist ungültig oder abgelaufen.') from exc


def access_url(portal, hold, *, origin=None, now=None):
    cfg = portal.app.config.get('MOS_PUBLIC_BOOKING', {})
    origin = origin or cfg.get('origin', '')
    parsed = urlsplit(origin)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.path
            or parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise ValueError('Sicherer Buchungszugang benötigt die konfigurierte HTTPS-Adresse.')
    # Fragments never reach HTTP access logs or the Referer header. The generic
    # landing page moves the token into a CSRF-protected confirmation POST.
    return origin + '/mieten/buchung-oeffnen#' + create_access_token(portal, hold, now)


def _authorization_valid(hold, end, stamp):
    """Local evidence only; provider reconciliation independently verifies it."""
    if hold.get('deposit_status') != 'authorized':
        return False
    try:
        until = datetime.fromisoformat(hold['deposit_capture_before'])
        return (until.tzinfo is not None
                and until.astimezone(timezone.utc) > end + timedelta(hours=24)
                and until.astimezone(timezone.utc) > stamp)
    except (ValueError, TypeError):
        return False


def _needs_deposit(hold, end, now=None):
    if hold.get('deposit_status') == 'authorized':
        return not _authorization_valid(hold, end, _now(now))
    return hold.get('deposit_status') in (None, 'creating', 'awaiting_card')


def enqueue_due(portal, now=None):
    """Queue within the twelve-hour window; record missed reminders as overdue."""
    stamp = _now(now)
    result = {'queued': 0, 'overdue': 0, 'errors': 0}
    db = portal.get_db()
    try:
        init_schema(db)
        ids = [r['id'] for r in db.execute(
            "SELECT id FROM miet_checkout_holds WHERE status='confirmed' AND mietvorgang_id IS NOT NULL").fetchall()]
        for hold_id in ids:
            hold = _booking(db, hold_id)
            try:
                payload, _, start, end = _snapshot(hold)
            except ValueError:
                continue  # Historic, canceled and completed rentals need no reminder.
            if not _needs_deposit(hold, end, stamp) or stamp < start - timedelta(hours=12):
                continue
            overdue = stamp >= start
            state = 'overdue' if overdue else 'queued'
            inserted = db.execute('''INSERT INTO miet_checkout_deposit_reminders
                (hold_id,recipient,payload_sha256,start_slot,enqueued_at,status,next_attempt_at,detail)
                VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (hold_id) DO NOTHING RETURNING hold_id''',
                (hold_id, payload['customer']['email'], sha256(hold['payload'].encode()).hexdigest(),
                 start.isoformat(), stamp.isoformat(), state, stamp.isoformat(),
                 'Kautionserinnerung vor Abholung nicht zugestellt; manuell prüfen.' if overdue else '')).fetchone()
            if inserted is not None:
                result[state] += 1
            if overdue:
                db.execute('''UPDATE miet_checkout_deposit_reminders SET status='overdue',detail=?
                    WHERE hold_id=? AND status='queued' ''',
                    ('Kautionserinnerung vor Abholung nicht zugestellt; manuell prüfen.', hold_id))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return result


def _message(portal, row, hold, start, stamp):
    message = EmailMessage()
    message['To'] = row['recipient']
    message['Subject'] = 'Ihre MOS-Miete: Kaution vor der Abholung reservieren'
    message.set_content(
        'Ihre Abholung steht bevor. Bitte reservieren Sie jetzt die Kaution von 500,00 EUR '
        'online auf einer Kreditkarte. Die Kaution wird nicht abgebucht; '
        'Debit- und Prepaidkarten werden nicht akzeptiert.\n\n'
        'Abholung: ' + start.astimezone(BERLIN).strftime('%d.%m.%Y um %H:%M Uhr') + '\n'
        'Buchungsreferenz: ' + hold['id'] + '\n\n'
        'Buchung sicher öffnen und Kaution reservieren:\n' + access_url(portal, hold, now=stamp) + '\n\n'
        'Für die Schlüsselübergabe muss die Kreditkartenreservierung gültig sein. '
        'Die tatsächliche Autorisierungsfrist Ihrer Bank wird geprüft. '
        'Bitte halten Sie diesen persönlichen Zugangslink vertraulich.\n\n'
        'Vermieter: ' + LEGAL_NAME + '\n' + business_letter_details() + '\n')
    add_workshop_mail_footer(message)
    return message


def deliver_one(portal, hold_id, cfg, *, live, enabled, now=None):
    if (not live or not enabled or not cfg.get('smtp_configured')
            or not (cfg.get('smtp_ssl') or cfg.get('smtp_tls'))
            or not _single_address(cfg.get('from_address')) or not cfg.get('_smtp_password')):
        return 'disabled'
    stamp = _now(now)
    db = portal.get_db()
    try:
        row = db.execute('SELECT * FROM miet_checkout_deposit_reminders WHERE hold_id=?', (hold_id,)).fetchone()
        if row is None:
            return 'not_due'
        row = dict(row)
        if row['status'] != 'queued':
            return row['status']
        try:
            hold = _booking(db, hold_id)
            payload, _, start, end = _snapshot(hold)
        except ValueError:
            db.execute("UPDATE miet_checkout_deposit_reminders SET status='skipped',detail=? WHERE hold_id=? AND status='queued'",
                       ('Buchung ist nicht mehr für eine Kautionserinnerung geöffnet.', hold_id))
            db.commit()
            return 'skipped'
        if (sha256(hold['payload'].encode()).hexdigest() != row['payload_sha256']
                or row['recipient'] != payload['customer']['email']):
            db.execute("UPDATE miet_checkout_deposit_reminders SET status='review',detail=? WHERE hold_id=? AND status='queued'",
                       ('Buchung oder Empfänger muss manuell geprüft werden.', hold_id))
            db.commit()
            return 'review'
        if not _needs_deposit(hold, end, stamp):
            db.execute("UPDATE miet_checkout_deposit_reminders SET status='skipped',detail='' WHERE hold_id=? AND status='queued'", (hold_id,))
            db.commit()
            return 'skipped'
        if stamp >= start:
            db.execute("UPDATE miet_checkout_deposit_reminders SET status='overdue',detail=? WHERE hold_id=? AND status='queued'",
                       ('Kautionserinnerung vor Abholung nicht zugestellt; manuell prüfen.', hold_id))
            db.commit()
            return 'overdue'
        if stamp < start - timedelta(hours=12):
            return 'not_due'
        message = _message(portal, row, hold, start, stamp)
        message['From'] = formataddr((cfg.get('display_name', ''), cfg['from_address']))
        message['Message-ID'] = ('<mos-deposit-' + sha256(hold_id.encode()).hexdigest()[:32]
                                 + '@' + cfg['from_address'].rsplit('@', 1)[1] + '>')
        claim = db.execute('''UPDATE miet_checkout_deposit_reminders
            SET status='sending',attempts=attempts+1,attempted_at=?,detail=''
            WHERE hold_id=? AND status='queued' AND next_attempt_at<=?''',
            (stamp.isoformat(), hold_id, stamp.isoformat()))
        db.commit()
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
        if outcome == 'sent':
            db.execute("UPDATE miet_checkout_deposit_reminders SET status='sent',accepted_at=?,detail='' WHERE hold_id=? AND status='sending'",
                       (stamp.isoformat(), hold_id))
        elif outcome == 'not_sent':
            delay = min(3600, 60 * (2 ** min(row['attempts'], 6)))
            db.execute("UPDATE miet_checkout_deposit_reminders SET status='queued',next_attempt_at=?,detail=? WHERE hold_id=? AND status='sending'",
                       ((stamp + timedelta(seconds=delay)).isoformat(), 'Mailserver hat die Nachricht nicht angenommen; erneuter Versuch vorgesehen.', hold_id))
        else:
            db.execute("UPDATE miet_checkout_deposit_reminders SET status='review',detail=? WHERE hold_id=? AND status='sending'",
                       ('Mailserver-Antwort unklar; vor erneutem Versand manuell prüfen.', hold_id))
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return outcome


def pending_ids(portal, limit=20, now=None):
    db = portal.get_db()
    try:
        return [r['hold_id'] for r in db.execute('''SELECT hold_id FROM miet_checkout_deposit_reminders
            WHERE status='queued' AND next_attempt_at<=? ORDER BY enqueued_at LIMIT ?''',
            (_now(now).isoformat(), max(1, min(int(limit), 100)))).fetchall()]
    finally:
        db.close()


def unresolved_count(portal):
    db = portal.get_db()
    try:
        ids = [r['hold_id'] for r in db.execute(
            "SELECT hold_id FROM miet_checkout_deposit_reminders WHERE status NOT IN ('sent','skipped')").fetchall()]
        unresolved = 0
        for hold_id in ids:
            try:
                hold = _booking(db, hold_id)
            except ValueError:
                unresolved += 1  # Missing booking evidence needs review.
                continue
            if (hold.get('canceled') or hold.get('returned_at')
                    or hold.get('rental_status') in {'zurueck', 'storniert'}):
                continue
            unresolved += 1
        return unresolved
    finally:
        db.close()


def missing_deposit_overdue(portal, now=None):
    """Monitor the card requirement independently from the reminder's delivery.

    A sent message is not a card authorization. Keep its SMTP audit untouched
    while detecting overdue bookings with no intent, a released/uncertain
    intent, or an expired/insufficient local authorization snapshot.
    """
    stamp = _now(now)
    db = portal.get_db()
    try:
        ids = [r['id'] for r in db.execute(
            "SELECT id FROM miet_checkout_holds WHERE status='confirmed' AND mietvorgang_id IS NOT NULL").fetchall()]
        missing = 0
        for hold_id in ids:
            hold = _booking(db, hold_id)
            if (hold.get('canceled') or hold.get('returned_at')
                    or hold.get('rental_status') in {'zurueck', 'storniert'}):
                continue
            try:
                quote = json.loads(hold['payload'])['quote']
                if quote.get('deposit_timing') != 'before_pickup' or quote.get('test_only') is not False:
                    continue
                _, _, start, end = _snapshot(hold)
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                raise ValueError('Vorausbuchung für die Kautionsüberwachung muss geprüft werden.') from exc
            if stamp >= start and not _authorization_valid(hold, end, stamp):
                missing += 1
        return missing
    finally:
        db.close()


def run_cycle(portal, cfg, *, live, enabled, now=None):
    result = {'queued': 0, 'overdue': 0, 'errors': 0, 'sent': 0, 'unresolved': 0,
              'missing_deposit_overdue': 0}
    if not live or not enabled:
        return result
    result.update(enqueue_due(portal, now))
    for hold_id in pending_ids(portal, now=now):
        try:
            outcome = deliver_one(portal, hold_id, cfg, live=live, enabled=enabled, now=now)
            if outcome == 'sent':
                result['sent'] += 1
            elif outcome not in {'skipped', 'not_due'}:
                result['errors'] += 1
        except Exception:
            result['errors'] += 1  # No token, address or provider detail in logs.
    result['unresolved'] = unresolved_count(portal)
    try:
        result['missing_deposit_overdue'] = missing_deposit_overdue(portal, now)
    except ValueError:
        result['errors'] += 1
    return result
