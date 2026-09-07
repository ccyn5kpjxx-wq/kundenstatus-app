"""Durable, at-most-once SMTP submission and independently retryable Sent copies.

Only ``send`` submits SMTP. Callers must authenticate and require a deliberate
admin action. The database and storage directory must both survive restarts;
workers sharing a database must share this private directory, too. A crash in
DATA cannot be resolved automatically: the token remains blocked for sending.
"""

import base64
from contextlib import contextmanager
from copy import deepcopy
from email import policy
from email.message import EmailMessage
from email.utils import getaddresses, make_msgid
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import smtplib
import ssl
import threading
import time
import uuid


_DDL = """CREATE TABLE IF NOT EXISTS mailbox_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT NOT NULL UNIQUE,
    fingerprint TEXT NOT NULL,
    attempt TEXT NOT NULL,
    state TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    payload_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    recipients TEXT NOT NULL,
    account TEXT NOT NULL,
    subject TEXT NOT NULL DEFAULT '',
    to_header TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    sent_folder TEXT NOT NULL DEFAULT '',
    accepted TEXT NOT NULL DEFAULT '[]',
    refused TEXT NOT NULL DEFAULT '{}',
    copy_done INTEGER NOT NULL DEFAULT 0,
    copy_owner TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL
)"""

_SENT_ALIASES = {
    'sent', 'sent items', 'sent messages', 'gesendet',
    'gesendete objekte', 'gesendete elemente',
}
_RESULT_FIELDS = ('state', 'revision', 'accepted', 'refused', 'copy_done',
                  'sent_folder', 'detail', 'updated_at')


def _quote(value):
    if not isinstance(value, str) or any(c in value for c in '\r\n\x00'):
        raise ValueError('Ungültiger Postfachwert.')
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def _fingerprint(msg):
    """Hash meaning, including attachment bytes, but not generated MIME framing."""
    def part_value(part, root=True):
        headers = []
        for key, value in part.items():
            key = key.lower()
            if key in {'mime-version', 'content-transfer-encoding', 'content-type'} or (
                    root and key in {'date', 'message-id'}):
                continue
            headers.append((key, str(value)))
        content_params = sorted((str(k).lower(), str(v)) for k, v in
                                part.get_params()[1:] if str(k).lower() != 'boundary') if part.get_params() else []
        if part.is_multipart():
            body = [part_value(child, root=False) for child in part.get_payload()]
        else:
            body = base64.b64encode(part.get_payload(decode=True) or b'').decode('ascii')
        return [sorted(headers), part.get_content_type(), content_params, body]
    canonical = json.dumps(part_value(msg), ensure_ascii=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('ascii')).hexdigest()


def _lock_fd(fd, unlock=False):
    if os.name == 'nt':
        import msvcrt
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK if unlock else msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(fd, fcntl.LOCK_UN if unlock else fcntl.LOCK_EX | fcntl.LOCK_NB)


class MailOutbox:
    def __init__(self, get_db, storage_dir, mailbox_service):
        self.get_db = get_db
        self.storage_dir = Path(storage_dir).resolve()
        self.service = mailbox_service
        self._schema_ready = False
        self._schema_lock = threading.Lock()

    @staticmethod
    def _token(token):
        try:
            return str(uuid.UUID(str(token)))
        except (ValueError, TypeError, AttributeError):
            raise ValueError('Ungültiger Versandvorgang.') from None

    def _ensure_schema(self):
        if self._schema_ready:
            return
        with self._schema_lock:
            if self._schema_ready:
                return
            with self._db() as db:
                db.execute(_DDL)
                db.commit()
            self._schema_ready = True

    @contextmanager
    def _db(self):
        db = self.get_db()
        try:
            yield db
        except BaseException:
            try:
                db.rollback()
            except Exception:
                pass
            raise
        finally:
            db.close()

    def _path(self, name):
        if not re.fullmatch(r'[a-f0-9]{64}(?:\.[a-f0-9]{32})?\.(?:eml|json|lock)', name):
            raise ValueError('Ungültige interne Nachrichtendatei.')
        return self.storage_dir / name

    @staticmethod
    def _key(token):
        return hashlib.sha256(token.encode('ascii')).hexdigest()

    def _journal_path(self, row):
        return self._path(self._key(row['token']) + '.' + row['attempt'] + '.json')

    def _atomic_write(self, path, data):
        self.storage_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.storage_dir, 0o700)
        temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            if os.name != 'nt':
                directory = os.open(self.storage_dir, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    @contextmanager
    def _lock(self, token):
        self.storage_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.storage_dir, 0o700)
        fd = os.open(self._path(self._key(token) + '.lock'), os.O_RDWR | os.O_CREAT, 0o600)
        locked = False
        try:
            if os.fstat(fd).st_size == 0:
                os.write(fd, b'0')
            try:
                _lock_fd(fd)
                locked = True
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                    raise
            yield locked
        finally:
            if locked:
                _lock_fd(fd, unlock=True)
            os.close(fd)

    def _active(self, token):
        """Inspect an existing lock without creating files or touching the network."""
        try:
            fd = os.open(self._path(self._key(token) + '.lock'), os.O_RDWR)
        except FileNotFoundError:
            return False
        try:
            try:
                _lock_fd(fd)
            except OSError as exc:
                if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                    return True
                raise
            _lock_fd(fd, unlock=True)
            return False
        finally:
            os.close(fd)

    def _read(self, token):
        self._ensure_schema()
        with self._db() as db:
            found = db.execute('SELECT * FROM mailbox_outbox WHERE token=?', (token,)).fetchone()
            db.commit()
        if found is None:
            return None
        row = dict(found)
        row['accepted'] = json.loads(row['accepted'])
        row['refused'] = json.loads(row['refused'])
        row['recipients'] = json.loads(row['recipients'])
        try:
            journal = json.loads(self._journal_path(row).read_text(encoding='utf-8'))
            if (journal.get('attempt') == row['attempt'] and journal.get('token') == token
                    and journal.get('revision', -1) >= row['revision']):
                row.update({field: journal[field] for field in _RESULT_FIELDS})
        except (OSError, ValueError, KeyError):
            pass
        return row

    def _legacy(self, token):
        # Old deployments may have guarded tokens without a recoverable MIME file.
        with self._db() as db:
            try:
                row = db.execute('SELECT status FROM mailbox_send_guard WHERE token=?', (token,)).fetchone()
                db.commit()
            except Exception as exc:
                db.rollback()
                if getattr(exc, 'sqlstate', None) == '42P01' or 'no such table: mailbox_send_guard' in str(exc):
                    return None
                raise
        if row is None:
            return None
        return {'token': token, 'state': 'uncertain', 'message':
                'Dieser Versand wurde bereits mit der früheren Version gestartet. Bitte IONOS prüfen; nicht erneut senden.',
                'can_retry': False, 'copy_pending': False, 'can_retry_copy': False,
                'accepted': [], 'refused': {}, 'payload_available': False, 'legacy': True}

    def _public(self, row):
        state = row['state']
        pending = state in ('copy_pending', 'partial') and not row['copy_done']
        messages = {
            'sending': 'Versand läuft. Bitte auf die Rückmeldung warten; nicht erneut senden.',
            'uncertain': 'Versandstatus unklar. Die Nachricht könnte angenommen worden sein. Nicht erneut senden; bitte IONOS prüfen.',
            'not_sent': 'Nicht versandt. ' + (row.get('detail') or 'Der Mailserver hat die Nachricht nicht angenommen.'),
            'copy_pending': 'Vom Mailserver angenommen. Die Kopie in Gesendet fehlt noch; nur die Ablage wiederholen.',
            'partial': 'Nur ein Teil der Empfänger wurde angenommen. Nicht die gesamte Nachricht erneut senden.',
            'sent': 'Vom Mailserver angenommen und in Gesendet gespeichert.',
        }
        return {'token': row['token'], 'state': state, 'message': messages[state],
                'subject': row['subject'], 'to': row['to_header'], 'created_at': row['created_at'],
                'message_id': row['message_id'], 'can_retry': state == 'not_sent',
                'copy_pending': pending, 'can_retry_copy': pending,
                'accepted': row['accepted'], 'refused': row['refused'],
                'payload_available': self._path(row['payload_name']).is_file()}

    def status(self, token):
        token = self._token(token)
        row = self._read(token)
        if row is None:
            return self._legacy(token)
        if row['state'] == 'sending' and not self._active(token):
            # Re-read after observing the lock: a sender may just have completed.
            row = self._read(token)
            if row['state'] == 'sending':
                row['state'] = 'uncertain'
        return self._public(row)

    def payload(self, token):
        row = self._read(self._token(token))
        if row is None:
            raise FileNotFoundError('Nachrichtendatei nicht vorhanden.')
        return self._path(row['payload_name']).read_bytes()

    def recent(self, limit=30):
        self._ensure_schema()
        limit = max(1, min(100, int(limit)))
        with self._db() as db:
            rows = db.execute('SELECT token FROM mailbox_outbox ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
            db.commit()
        return [self.status(row['token']) for row in rows]

    def _sync_db(self, row):
        with self._db() as db:
            db.execute('''UPDATE mailbox_outbox SET state=?, revision=?, accepted=?, refused=?,
                copy_done=?, sent_folder=?, detail=?, updated_at=?
                WHERE token=? AND attempt=? AND revision<?''',
                (row['state'], row['revision'], json.dumps(row['accepted']), json.dumps(row['refused']),
                 int(row['copy_done']), row['sent_folder'], row['detail'], row['updated_at'],
                 row['token'], row['attempt'], row['revision']))
            db.commit()

    def _record(self, row, **values):
        row.update(values)
        row['revision'] += 1
        row['updated_at'] = time.time()
        journal_ok = db_ok = False
        try:
            journal = {key: row[key] for key in (*_RESULT_FIELDS, 'attempt', 'token')}
            self._atomic_write(self._journal_path(row), json.dumps(journal).encode('utf-8'))
            journal_ok = True
        except OSError:
            pass
        try:
            self._sync_db(row)
            db_ok = True
        except Exception:
            pass
        return journal_ok or db_ok

    def _new(self, token, msg, fingerprint, cfg):
        message = deepcopy(msg)
        if not message.get('Message-ID'):
            message['Message-ID'] = make_msgid()
        recipients = list(dict.fromkeys(addr for _, addr in getaddresses(
            [str(value) for name in ('To', 'Cc', 'Bcc') for value in message.get_all(name, [])])))
        if not recipients:
            raise ValueError('Bitte gültige Empfänger angeben.')
        # Bcc belongs in the SMTP envelope, never in the Sent copy or wire headers.
        if 'Bcc' in message:
            del message['Bcc']
        attempt = uuid.uuid4().hex
        name = self._key(token) + '.' + uuid.uuid4().hex + '.eml'
        raw = message.as_bytes(policy=policy.SMTP)
        self._atomic_write(self._path(name), raw)
        row = {'token': token, 'fingerprint': fingerprint, 'attempt': attempt, 'state': 'sending',
               'revision': 0, 'payload_name': name, 'message_id': str(message['Message-ID']),
               'recipients': recipients, 'account': str(cfg.get('smtp_user', '')).strip().casefold(),
               'subject': str(message.get('Subject', '')), 'to_header': str(message.get('To', '')),
               'created_at': time.time(),
               'sent_folder': '', 'accepted': [], 'refused': {}, 'copy_done': 0,
               'detail': '', 'updated_at': time.time()}
        with self._db() as db:
            cursor = db.execute('''INSERT INTO mailbox_outbox
                (token,fingerprint,attempt,state,revision,payload_name,message_id,recipients,account,
                 subject,to_header,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(token) DO NOTHING''',
                (token, fingerprint, attempt, 'sending', 0, name, row['message_id'],
                 json.dumps(recipients), row['account'], row['subject'], row['to_header'],
                 row['created_at'], row['updated_at']))
            won = cursor.rowcount == 1
            db.commit()
        if not won:
            self._path(name).unlink()
            return None
        return row

    def _preflight(self, row, cfg):
        if not cfg.get('smtp_configured'):
            raise ValueError('SMTP ist noch nicht eingerichtet.')
        if not (cfg.get('smtp_ssl') or cfg.get('smtp_tls')):
            raise ValueError('Verschlüsselter SMTP-Zugang erforderlich.')
        account = str(self.service.config().get('user', '')).strip().casefold()
        if not account or account != str(cfg.get('smtp_user', '')).strip().casefold() or account != row['account']:
            raise ValueError('SMTP und IMAP müssen dasselbe Postfach verwenden.')
        with self.service.connect() as client:
            folders = self.service.folders(client)
            target = next((folder['id'] for folder in folders
                           if '\\sent' in str(folder.get('flags', '')).casefold().split()), None)
            target = target or next((folder['id'] for folder in folders
                                      if str(folder.get('label', '')).strip().casefold() in _SENT_ALIASES), None)
            if not target:
                raise ValueError('Der Gesendet-Ordner fehlt. Bitte zuerst in IONOS prüfen.')
            self.service.select(client, target, readonly=True)
        row['sent_folder'] = target

    def _smtp(self, row, cfg, raw):
        smtp = None
        phase = 'connect'
        refused = {}
        accepted = []
        try:
            options = {'timeout': 30}
            if cfg.get('local_hostname'):
                options['local_hostname'] = cfg['local_hostname']
            if cfg.get('smtp_ssl'):
                smtp = smtplib.SMTP_SSL(cfg['smtp_host'], cfg['smtp_port'],
                                       context=ssl.create_default_context(), **options)
            else:
                smtp = smtplib.SMTP(cfg['smtp_host'], cfg['smtp_port'], **options)
                smtp.starttls(context=ssl.create_default_context())
            smtp.login(cfg['smtp_user'], cfg['_smtp_password'])
            smtp.ehlo_or_helo_if_needed()
            sender = getaddresses([str(cfg['from_address'])])[0][1]
            # Unicode display names are MIME-encoded; international envelopes
            # require explicit SMTPUTF8 support and are otherwise safely rejected.
            mail_options = []
            try:
                (sender + ''.join(row['recipients'])).encode('ascii')
            except UnicodeEncodeError:
                if not smtp.has_extn('smtputf8'):
                    raise ValueError('Der Mailserver unterstützt diese Empfängeradresse nicht.')
                mail_options = ['SMTPUTF8', 'BODY=8BITMIME']
            code, _ = smtp.mail(sender, options=mail_options)
            if code != 250:
                raise smtplib.SMTPSenderRefused(code, b'Absender abgelehnt', sender)
            for recipient in row['recipients']:
                code, _ = smtp.rcpt(recipient)
                if code in (250, 251):
                    accepted.append(recipient)
                else:
                    refused[recipient] = {'code': int(code), 'message': 'Empfänger vom Mailserver abgelehnt.'}
                    if code == 421:
                        raise smtplib.SMTPRecipientsRefused(refused)
            if not accepted:
                raise smtplib.SMTPRecipientsRefused(refused)
            phase = 'data'
            code, _ = smtp.data(raw)
            if code != 250:
                raise smtplib.SMTPDataError(code, b'Nachricht abgelehnt')
            phase = 'accepted'
            self._record(row, state='partial' if refused else 'copy_pending', accepted=accepted,
                         refused=refused, detail='', copy_done=0)
        except Exception as exc:
            if phase == 'accepted':
                # A local recording problem cannot revoke the server's positive
                # DATA response, and must never make this token retryable.
                row.update(state='partial' if refused else 'copy_pending',
                           accepted=accepted, refused=refused)
                return
            # An explicit negative DATA reply confirms non-acceptance. A lost
            # connection after entering DATA does not, even if QUIT also fails.
            known = phase != 'data' or (isinstance(exc, smtplib.SMTPDataError)
                                       and 400 <= exc.smtp_code <= 599)
            detail = ('Der Mailserver ist nicht erreichbar oder hat den Versand abgelehnt. '
                      'Text und Anhänge bleiben erhalten.')
            if isinstance(exc, ValueError):
                detail = str(exc)
            self._record(row, state='not_sent' if known else 'uncertain',
                         accepted=[], refused=refused, detail=detail)
        finally:
            if smtp is not None:
                # QUIT is cleanup. Its response cannot reverse successful DATA.
                try:
                    smtp.quit()
                except Exception:
                    try:
                        smtp.close()
                    except Exception:
                        pass

    def send(self, token, msg: EmailMessage, cfg: dict):
        token = self._token(token)
        fingerprint = _fingerprint(msg)
        self._ensure_schema()
        with self._lock(token) as locked:
            if not locked:
                row = self._read(token)
                if row and row['fingerprint'] != fingerprint:
                    raise ValueError('Dieser Versandvorgang gehört zu einem anderen Nachrichteninhalt.')
                return self.status(token) or {'token': token, 'state': 'sending', 'can_retry': False,
                    'copy_pending': False, 'can_retry_copy': False, 'accepted': [], 'refused': {},
                    'payload_available': False, 'message': 'Der Versandvorgang wird vorbereitet.'}
            row = self._read(token)
            if row is None:
                legacy = self._legacy(token)
                if legacy:
                    return legacy
                row = self._new(token, msg, fingerprint, cfg)
                if row is None:
                    row = self._read(token)
                    if row['fingerprint'] != fingerprint:
                        raise ValueError('Dieser Versandvorgang gehört zu einem anderen Nachrichteninhalt.')
                    return self._public(row)
            else:
                if row['fingerprint'] != fingerprint:
                    raise ValueError('Dieser Versandvorgang gehört zu einem anderen Nachrichteninhalt.')
                if row['state'] != 'not_sent':
                    if row['state'] == 'sending':
                        row['state'] = 'uncertain'  # Lock was free: previous sender was interrupted.
                    return self._public(row)
                # Reconcile a durable known rejection before the compare-and-swap.
                self._sync_db(row)
                attempt = uuid.uuid4().hex
                with self._db() as db:
                    cursor = db.execute('''UPDATE mailbox_outbox SET state='sending', attempt=?, revision=0,
                        accepted='[]', refused='{}', copy_done=0, copy_owner='', detail='', updated_at=?
                        WHERE token=? AND attempt=? AND state='not_sent' ''',
                        (attempt, time.time(), token, row['attempt']))
                    won = cursor.rowcount == 1
                    db.commit()
                if not won:
                    return self.status(token)
                row.update(attempt=attempt, revision=0, state='sending', accepted=[], refused={}, copy_done=0)
            try:
                raw = self._path(row['payload_name']).read_bytes()
                self._preflight(row, cfg)
            except Exception as exc:
                detail = str(exc) if isinstance(exc, ValueError) else 'Postfach oder Nachrichtendatei nicht verfügbar. Text und Anhänge bleiben erhalten.'
                self._record(row, state='not_sent', detail=detail)
                return self._public(row)
            if not self._record(row, state='sending'):
                # Never submit if both persistent stores fail before SMTP.
                row.update(state='not_sent', detail='Versand konnte nicht sicher gespeichert werden.')
                return self._public(row)
            self._smtp(row, cfg, raw)
            if row['state'] in ('copy_pending', 'partial'):
                self._copy(row)
            return self._public(row)

    def _copy(self, row):
        # The OS lock held by send/retry_copy proves any previous copy worker
        # sharing this durable directory has stopped; reclaim its stale DB claim.
        owner = uuid.uuid4().hex
        claimed = False
        try:
            self._sync_db(row)
            with self._db() as db:
                db.execute("UPDATE mailbox_outbox SET copy_owner='' WHERE token=? AND attempt=?",
                           (row['token'], row['attempt']))
                cursor = db.execute('''UPDATE mailbox_outbox SET copy_owner=?
                    WHERE token=? AND attempt=? AND copy_owner='' AND copy_done=0
                    AND state IN ('copy_pending','partial')''', (owner, row['token'], row['attempt']))
                claimed = cursor.rowcount == 1
                db.commit()
            if not claimed:
                return
            account = str(self.service.config().get('user', '')).strip().casefold()
            if account != row['account']:
                raise ValueError('Die Gesendet-Kopie gehört zu einem anderen Postfach.')
            raw = self._path(row['payload_name']).read_bytes()
            with self.service.connect() as client:
                self.service.select(client, row['sent_folder'], readonly=True)
                # Search before *every* append, including recovery after the
                # server accepted APPEND but its response was lost.
                status, data = client.uid('SEARCH', None, 'HEADER', 'Message-ID', _quote(row['message_id']))
                if status != 'OK' or not isinstance(data, (list, tuple)) or not data or data[0] is None:
                    raise ValueError('Gesendet-Kopie konnte nicht sicher geprüft werden.')
                found = any(value.strip() for value in data if isinstance(value, bytes))
                if not found:
                    status, _ = client.append(_quote(row['sent_folder']), r'(\Seen)', None, raw)
                    if status != 'OK':
                        raise ValueError('Gesendet-Kopie wurde nicht bestätigt.')
                durable = self._record(row, state='partial' if row['refused'] else 'sent', copy_done=1, detail='')
                if durable:
                    try:
                        self._path(row['payload_name']).unlink()
                    except OSError:
                        pass
        except Exception:
            # The accepted SMTP result is already durable. An IMAP or database
            # failure must never turn it into a general retryable send failure.
            pass
        finally:
            if claimed:
                try:
                    with self._db() as db:
                        db.execute("UPDATE mailbox_outbox SET copy_owner='' WHERE token=? AND copy_owner=?",
                                   (row['token'], owner))
                        db.commit()
                except Exception:
                    pass

    def retry_copy(self, token):
        token = self._token(token)
        self._ensure_schema()
        with self._lock(token) as locked:
            if not locked:
                return self.status(token)
            row = self._read(token)
            if row is None:
                raise ValueError('Versandvorgang nicht gefunden.')
            if row['state'] not in ('copy_pending', 'partial') or row['copy_done']:
                if row['state'] == 'sending':
                    row['state'] = 'uncertain'
                return self._public(row)
            self._copy(row)
            return self._public(row)
