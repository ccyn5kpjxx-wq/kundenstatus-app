"""Durable MOS contract-confirmation outbox.

Only a separately enabled CLI worker may contact SMTP. A lost reply after DATA
is never retried automatically: delivery may have succeeded at the mail server.
"""

from base64 import b64decode
from datetime import datetime, timedelta, timezone
from email import policy
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from hashlib import sha256
import json
import smtplib
import ssl


def init_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_contract_delivery (
        hold_id TEXT PRIMARY KEY, pdf_sha256 TEXT NOT NULL, recipient TEXT NOT NULL,
        status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        enqueued_at TEXT NOT NULL, attempted_at TEXT, next_attempt_at TEXT,
        accepted_at TEXT, detail TEXT NOT NULL DEFAULT '')''')


def enqueue(db, hold, contract, pdf_sha256):
    """Queue only a verified paid booking; keep the recipient and PDF immutable."""
    if contract.get('test_only') is not False:
        return False
    if (hold['status'] != 'confirmed' or not hold['mietvorgang_id']
            or not hold['payment_intent'] or not pdf_sha256):
        return False
    recipient = contract['customer_email']
    if not _single_address(recipient):
        raise ValueError('Vertrags-E-Mail-Adresse muss geprüft werden.')
    now = datetime.now(timezone.utc).isoformat()
    db.execute('''INSERT INTO miet_checkout_contract_delivery
        (hold_id,pdf_sha256,recipient,status,enqueued_at,next_attempt_at)
        VALUES (?,?,?,'queued',?,?) ON CONFLICT (hold_id) DO NOTHING RETURNING hold_id''',
        (hold['id'], pdf_sha256, recipient, now, now))
    return True


def _single_address(value):
    if not isinstance(value, str) or any(c in value for c in '\r\n\x00,;'):
        return False
    name, address = parseaddr(value)
    try:
        value.encode('ascii')
    except UnicodeEncodeError:
        return False  # Internationalized envelopes need a separately tested SMTPUTF8 path.
    return not name and address == value and address.count('@') == 1 and ' ' not in address


def _message(hold_id, recipient, pdf, cancelled):
    message = EmailMessage()
    message['To'] = recipient
    message['Subject'] = 'Ihre MOS-Vertragskopie'
    body = ('Beigefügt erhalten Sie die Kopie des bei Ihrer Buchung geschlossenen '
            'Mietvertrags mit den damals vereinbarten Mietbedingungen.\n\n'
            'Buchungsreferenz: ' + hold_id + '\n\n'
            'Vermieter: Gärtner GmbH Karosserie + Lack\n')
    if cancelled:
        body += ('\nDie Buchung wurde inzwischen storniert. Diese Vertragskopie ist keine '
                 'erneute Buchungszusage; bitte beachten Sie den Storno- und Erstattungsstand.\n')
    message.set_content(body)
    message.add_attachment(pdf, maintype='application', subtype='pdf',
                           filename='MOS-Mietvertrag-' + hold_id + '.pdf')
    return message


def _verified_row(db, hold_id):
    row = db.execute('''SELECT d.*,c.contract_json,c.contract_sha256,c.pdf_base64,c.pdf_sha256 AS stored_pdf_sha256,
        c.signed_at,c.signature_png_base64,h.status AS hold_status,h.payment_intent,h.mietvorgang_id,
        cancellation.id AS cancellation_id
        FROM miet_checkout_contract_delivery d
        JOIN miet_checkout_contracts c ON c.hold_id=d.hold_id
        JOIN miet_checkout_holds h ON h.id=d.hold_id
        LEFT JOIN miet_checkout_cancellations cancellation ON cancellation.id=h.id
        WHERE d.hold_id=?''', (hold_id,)).fetchone()
    if not row:
        raise ValueError('Vertragsversand ohne abgeschlossene Buchung.')
    row = dict(row)
    contract = json.loads(row['contract_json'])
    pdf = b64decode(row['pdf_base64'], validate=True)
    canonical = json.dumps(contract, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    if (row['hold_status'] != 'confirmed' or not row['payment_intent'] or not row['mietvorgang_id']
            or contract.get('test_only') is not False or not row['signed_at']
            or not row['signature_png_base64'] or not _single_address(row['recipient'])
            or row['recipient'] != contract.get('customer_email')
            or sha256(canonical.encode('utf-8')).hexdigest() != row['contract_sha256']
            or row['pdf_sha256'] != row['stored_pdf_sha256']
            or sha256(pdf).hexdigest() != row['pdf_sha256']):
        raise ValueError('Vertragsversand muss manuell geprüft werden.')
    return row, contract, pdf


def _smtp_send(message, cfg):
    """Return sent / not_sent / uncertain, with DATA ambiguity kept separate."""
    smtp = None
    phase = 'before_data'
    try:
        kwargs = {'timeout': 30, 'context': ssl.create_default_context()}
        if cfg.get('local_hostname'):
            kwargs['local_hostname'] = cfg['local_hostname']
        if cfg['smtp_ssl']:
            smtp = smtplib.SMTP_SSL(cfg['smtp_host'], cfg['smtp_port'], **kwargs)
        else:
            kwargs.pop('context')
            smtp = smtplib.SMTP(cfg['smtp_host'], cfg['smtp_port'], **kwargs)
            smtp.starttls(context=ssl.create_default_context())
        smtp.login(cfg['smtp_user'], cfg['_smtp_password'])
        sender = cfg['from_address']
        recipient = message['To']
        code, _ = smtp.mail(sender)
        if code != 250:
            return 'not_sent'
        code, _ = smtp.rcpt(recipient)
        if code not in (250, 251):
            return 'not_sent'
        phase = 'data'
        code, _ = smtp.data(message.as_bytes(policy=policy.SMTP))
        if code != 250:
            return 'not_sent'  # An explicit negative SMTP reply means no acceptance.
        phase = 'accepted'
        return 'sent'
    except smtplib.SMTPDataError as exc:
        return 'not_sent' if phase == 'data' and 400 <= exc.smtp_code <= 599 else 'uncertain'
    except Exception:
        return 'uncertain' if phase in {'data', 'accepted'} else 'not_sent'
    finally:
        if smtp is not None:
            try:
                smtp.quit()
            except Exception:
                try:
                    smtp.close()
                except Exception:
                    pass


def deliver_one(portal, hold_id, cfg, *, live, enabled):
    """Claim before SMTP; a crashed or ambiguous attempt needs human review."""
    if not live or not enabled or not cfg.get('smtp_configured') or not (cfg.get('smtp_ssl') or cfg.get('smtp_tls')):
        return 'disabled'
    if not _single_address(cfg.get('from_address')) or not cfg.get('_smtp_password'):
        return 'disabled'
    db = portal.get_db()
    try:
        try:
            row, contract, pdf = _verified_row(db, hold_id)
            if row['status'] != 'queued':
                return row['status']
            message = _message(hold_id, row['recipient'], pdf, bool(row['cancellation_id']))
            message['From'] = formataddr((cfg.get('display_name', ''), cfg['from_address']))
            message['Message-ID'] = '<mos-contract-' + sha256(hold_id.encode()).hexdigest()[:32] + '@' + cfg['from_address'].rsplit('@', 1)[1] + '>'
        except (ValueError, KeyError, TypeError):
            db.execute("UPDATE miet_checkout_contract_delivery SET status='review',detail=? WHERE hold_id=? AND status='queued'",
                       ('Buchung, Empfänger oder PDF muss geprüft werden.', hold_id))
            db.commit()
            return 'review'
        now = datetime.now(timezone.utc).isoformat()
        claim = db.execute('''UPDATE miet_checkout_contract_delivery
            SET status='sending',attempts=attempts+1,attempted_at=?,detail=''
            WHERE hold_id=? AND status='queued' AND next_attempt_at<=?''', (now, hold_id, now))
        db.commit()  # Persist the at-most-once fence before network I/O.
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
            db.execute('''UPDATE miet_checkout_contract_delivery
                SET status='sent',accepted_at=?,detail='' WHERE hold_id=? AND status='sending' ''',
                (stamp.isoformat(), hold_id))
        elif outcome == 'not_sent':
            delay = min(3600, 60 * (2 ** min(row['attempts'], 6)))
            db.execute('''UPDATE miet_checkout_contract_delivery
                SET status='queued',next_attempt_at=?,detail=? WHERE hold_id=? AND status='sending' ''',
                ((stamp + timedelta(seconds=delay)).isoformat(),
                 'Mailserver hat die Nachricht nicht angenommen; erneuter Versuch vorgesehen.', hold_id))
        else:
            db.execute('''UPDATE miet_checkout_contract_delivery
                SET status='review',detail=? WHERE hold_id=? AND status='sending' ''',
                ('Mailserver-Antwort unklar; vor erneutem Versand manuell prüfen.', hold_id))
        db.commit()
    except Exception:
        db.rollback()
        raise  # A DB failure leaves sending fenced; never resend automatically.
    finally:
        db.close()
    return outcome


def pending_ids(portal, limit=20):
    db = portal.get_db()
    try:
        now = datetime.now(timezone.utc).isoformat()
        return [r['hold_id'] for r in db.execute('''SELECT hold_id FROM miet_checkout_contract_delivery
            WHERE status='queued' AND next_attempt_at<=?
            ORDER BY enqueued_at LIMIT ?''', (now, max(1, min(int(limit), 100)))).fetchall()]
    finally:
        db.close()


def unfinished_live_ids(portal, limit=None):
    """Find confirmed bookings missing a PDF/outbox, excluding explicit test holds."""
    db = portal.get_db()
    try:
        cursor = db.execute('''SELECT h.id,h.payload FROM miet_checkout_holds h
            LEFT JOIN miet_checkout_contracts c ON c.hold_id=h.id
            LEFT JOIN miet_checkout_contract_delivery d ON d.hold_id=h.id
            WHERE h.status='confirmed' AND h.payment_intent IS NOT NULL
              AND h.mietvorgang_id IS NOT NULL AND (c.hold_id IS NULL OR d.hold_id IS NULL)
            ORDER BY h.id''')
        ids = []
        for row in cursor.fetchall():
            try:
                if json.loads(row['payload'])['quote'].get('test_only') is True:
                    continue
            except (TypeError, ValueError, KeyError):
                pass  # Corrupt payload needs review; do not silently exclude it.
            ids.append(row['id'])
            if limit is not None and len(ids) >= limit:
                break
        return ids
    finally:
        db.close()


def unresolved_count(portal):
    db = portal.get_db()
    try:
        row = db.execute("SELECT COUNT(*) AS n FROM miet_checkout_contract_delivery WHERE status!='sent'").fetchone()
        return row['n']
    finally:
        db.close()
