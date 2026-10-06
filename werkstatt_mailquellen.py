"""Resumable, admin-started supplier mail inventory; never writes to IMAP/SMTP.

Header coverage is distinct from document extraction. Unknown addresses require
an explicit read-scope classification, which never verifies an ordering contact.
No legacy mail importer, finance marker, article apply or order path is called.
"""
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import email
from email import policy
from email.utils import formataddr, getaddresses, parseaddr
import hashlib
import hmac
import io
import json
from pathlib import Path
import re
import time
import uuid

from flask import Blueprint, abort, jsonify, render_template, request, session
from werkzeug.utils import secure_filename
from mailbox_client import Mailbox, folder_label
from werkstatt_bestellungen import _email
from werkstatt_rechnungsfreigabe import classify_invoice_source, normalize_supplier

TABLES = tuple('assistent_mailquellen_' + suffix for suffix in ('laeufe', 'ordner', 'nachrichten', 'absender', 'dateien'))
KNOWN_DOMAINS = {'top-color.net': 'TOP-Color GmbH', 'tech-masters.de': 'TECH-MASTERS Deutschland GmbH'}
HEADER_BATCH = 40
MAX_FILE = 20 * 1024 * 1024
MAX_ATTACHMENTS = 20
_FOLDER_BLOCK = re.compile(r'bank|konto|finanz|finance|buchhalt|buchfuehr|steuer|lohn|gehalt|payroll|personal|persoenlich|family|familie|bewerbung|medizin|medical|arzt|krank|privat|private|urlaub|reise|travel|versicherung|\b(?:bwa|fibu|tax|hr)\b', re.I)
_INVOICE = re.compile(r'rechnung|invoice|faktura|(?:^|[\s_\-])re\d{4,}', re.I)
_NON_PURCHASE = re.compile(r'gutschrift|credit\s*note|storno|retour|reklamation|refund|zahlungserinnerung|mahnung|zahlungsavis|kontoauszug', re.I)
_PAYMENT_META = re.compile(r'\b(?:sepa[a-z]*|mandat[a-z]*|ueberweisung[a-z]*|doppelzahlung[a-z]*|doppelbuchung[a-z]*|zahlungs(?:avis|erinnerung|eingang|ausgang|bestaetigung|abgleich|aufforderung)[a-z]*|lastschrift[a-z]*|ruecklastschrift[a-z]*|konto[a-z]*|bank[a-z]*|iban|bic|saldo|mahnung[a-z]*)\b', re.I)


def financial_metadata(*values):
    return bool(_PAYMENT_META.search(' '.join(normalize_supplier(value) for value in values)))


def financial_header(sender, name, subject):
    # Observed TECH-MASTERS reminder series; do not generalize product codes.
    return financial_metadata(sender, name, subject) or (
        str(sender).lower().endswith('@tech-masters.de') and bool(re.search(r'\bM\d{2}-\d+\b', str(subject), re.I)))


def now():
    return datetime.now(timezone.utc).isoformat()


def text(value, limit=300):
    return ' '.join(str(value or '').replace('\x00', '').split())[:limit]


def address(value):
    try:
        return _email(value).lower()
    except ValueError:
        return ''


class MailSources:
    def __init__(self, portal, mailbox=None):
        self.p = portal
        self.mailbox = mailbox or Mailbox(portal.get_werkstatt_imap_config)
        self.init_schema()

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def init_schema(self):
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS assistent_mailquellen_laeufe (
                  id INTEGER PRIMARY KEY AUTOINCREMENT, account TEXT UNIQUE NOT NULL,
                  run_token TEXT NOT NULL, state TEXT NOT NULL, started_at TEXT NOT NULL,
                  finished_at TEXT DEFAULT '', lease TEXT DEFAULT '', lease_until DOUBLE PRECISION DEFAULT 0,
                  last_error TEXT DEFAULT '');
                CREATE TABLE IF NOT EXISTS assistent_mailquellen_ordner (
                  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL,
                  folder TEXT NOT NULL, label TEXT NOT NULL, state TEXT NOT NULL,
                  outgoing INTEGER DEFAULT 0,
                  validity TEXT DEFAULT '', uids_json TEXT DEFAULT '[]', cursor INTEGER DEFAULT 0,
                  total INTEGER DEFAULT 0, missing INTEGER DEFAULT 0, snapshot_at TEXT DEFAULT '',
                  UNIQUE(run_id,folder));
                CREATE TABLE IF NOT EXISTS assistent_mailquellen_nachrichten (
                  id INTEGER PRIMARY KEY AUTOINCREMENT, account TEXT NOT NULL,
                  run_token TEXT NOT NULL, folder TEXT NOT NULL, validity TEXT NOT NULL,
                  uid TEXT NOT NULL, sender TEXT DEFAULT '', sender_name TEXT DEFAULT '',
                  subject TEXT DEFAULT '', message_date TEXT DEFAULT '', message_id TEXT DEFAULT '',
                  supplier TEXT DEFAULT '', state TEXT NOT NULL, note TEXT DEFAULT '',
                  attachments_json TEXT DEFAULT '[]', updated_at TEXT NOT NULL,
                  UNIQUE(account,folder,validity,uid));
                CREATE TABLE IF NOT EXISTS assistent_mailquellen_absender (
                  id INTEGER PRIMARY KEY AUTOINCREMENT, account TEXT NOT NULL,
                  email TEXT NOT NULL, supplier TEXT NOT NULL, approved_at TEXT NOT NULL,
                  UNIQUE(account,email));
                CREATE TABLE IF NOT EXISTS assistent_mailquellen_dateien (
                  id INTEGER PRIMARY KEY AUTOINCREMENT, sha256 TEXT UNIQUE NOT NULL,
                  supplier TEXT NOT NULL, beleg_id INTEGER NOT NULL, stored_name TEXT NOT NULL,
                  original_name TEXT NOT NULL, file_base64 TEXT NOT NULL, size INTEGER NOT NULL,
                  created_at TEXT NOT NULL);
            ''')
            ensure = getattr(self.p, 'ensure_column', None)
            additions = {
                'assistent_mailquellen_ordner': {
                    'outgoing': 'INTEGER DEFAULT 0', 'incremental': 'INTEGER DEFAULT 0',
                    'high_water_uid': 'BIGINT DEFAULT 0', 'poll_ceiling_uid': 'BIGINT DEFAULT 0',
                    'scan_end_uid': 'BIGINT DEFAULT 0'},
                'assistent_mailquellen_nachrichten': {
                    'raw_sha256': "TEXT DEFAULT ''", 'message_fingerprint': "TEXT DEFAULT ''",
                    'duplicate_of': 'INTEGER', 'monitor_catalog_queued': 'INTEGER DEFAULT 0'},
            }
            for table, columns in additions.items():
                for column, definition in columns.items():
                    if callable(ensure):
                        ensure(db, table, column, definition)
                    elif column not in {row[1] for row in db.execute('PRAGMA table_info('+table+')').fetchall()}:
                        db.execute('ALTER TABLE '+table+' ADD COLUMN '+column+' '+definition)
            db.execute('CREATE INDEX IF NOT EXISTS idx_mailquellen_message_fingerprint ON assistent_mailquellen_nachrichten(account,message_fingerprint)')

    def identity(self):
        config = self.p.get_werkstatt_imap_config()
        if not config.get('configured'):
            raise ValueError('Das betriebliche Postfach ist noch nicht verbunden.')
        account = hashlib.sha256((str(config.get('host', '')).lower() + ':' + str(config.get('port', '')) + ':' + str(config.get('user', '')).lower()).encode()).hexdigest()
        return account, address(config.get('user', ''))

    def start(self):
        account, _ = self.identity()
        with self.db() as db:
            self._quarantine(db, account)
            previous = db.execute('SELECT * FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone()
            if previous and previous['state'] in ('active', 'paused'):
                db.execute("UPDATE assistent_mailquellen_laeufe SET state='active',last_error='' WHERE id=?", (previous['id'],))
                return self._status(db, account)
        # LIST alone: blocked folders are never SELECTed or header-fetched.
        with self.mailbox.connect() as client:
            folders = self.mailbox.folders(client)
        with self.db() as db:
            db.execute('''INSERT INTO assistent_mailquellen_laeufe(account,run_token,state,started_at)
                VALUES(?,?,'new',?) ON CONFLICT(account) DO NOTHING''', (account, uuid.uuid4().hex, now()))
            db.execute('UPDATE assistent_mailquellen_laeufe SET account=account WHERE account=?', (account,))
            row = dict(db.execute('SELECT * FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone())
            if row['state'] == 'active' or row['lease_until'] > time.time():
                return self._status(db, account)
            token = uuid.uuid4().hex
            db.execute("UPDATE assistent_mailquellen_laeufe SET run_token=?,state='active',started_at=?,finished_at='',last_error='',lease='',lease_until=0 WHERE id=?", (token, now(), row['id']))
            db.execute('DELETE FROM assistent_mailquellen_ordner WHERE run_id=?', (row['id'],))
            for folder in folders:
                name = str(folder['id'])
                label = text(folder.get('label') or folder_label(name))
                state = 'excluded' if _FOLDER_BLOCK.search(normalize_supplier(label)) else 'pending'
                outgoing = '\\sent' in str(folder.get('flags', '')).lower() or bool(re.search(r'\b(?:sent|gesendet|gesendete)\b', normalize_supplier(label)))
                db.execute('INSERT INTO assistent_mailquellen_ordner(run_id,folder,label,state,outgoing) VALUES(?,?,?,?,?)', (row['id'], name, label, state, int(outgoing)))
            return self._status(db, account)

    def pause(self):
        account, _ = self.identity()
        with self.db() as db:
            db.execute("UPDATE assistent_mailquellen_laeufe SET state='paused' WHERE account=? AND state='active'", (account,))
            return self._status(db, account)

    def start_incremental(self, guard=None):
        """Resume the current snapshot or poll new UIDs; never reset saved originals."""
        account, _ = self.identity()
        with self.db() as db:
            if guard is not None:
                guard(db)
            self._quarantine(db, account)
            previous = db.execute('SELECT * FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone()
            if previous and previous['state'] in ('active', 'paused'):
                db.execute("UPDATE assistent_mailquellen_laeufe SET state='active',last_error='' WHERE id=?", (previous['id'],))
                return self._status(db, account)
        with self.mailbox.connect() as client:
            folders = self.mailbox.folders(client)
        with self.db() as db:
            if guard is not None:
                guard(db)
            db.execute('''INSERT INTO assistent_mailquellen_laeufe(account,run_token,state,started_at)
                VALUES(?,?,'new',?) ON CONFLICT(account) DO NOTHING''', (account, uuid.uuid4().hex, now()))
            db.execute('UPDATE assistent_mailquellen_laeufe SET account=account WHERE account=?', (account,))
            run = dict(db.execute('SELECT * FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone())
            if run['lease_until'] > time.time() or run['state'] == 'active':
                return self._status(db, account)
            db.execute("UPDATE assistent_mailquellen_laeufe SET run_token=?,state='active',started_at=?,finished_at='',last_error='' WHERE id=?",
                       (uuid.uuid4().hex, now(), run['id']))
            # Folder records are checkpoints, not a disposable inventory. An
            # absent/deleted folder never authorizes a fallback mailbox read.
            db.execute("UPDATE assistent_mailquellen_ordner SET state='excluded' WHERE run_id=?", (run['id'],))
            for folder in folders:
                name = str(folder['id'])
                label = text(folder.get('label') or folder_label(name))
                outgoing = '\\sent' in str(folder.get('flags', '')).lower() or bool(re.search(r'\b(?:sent|gesendet|gesendete)\b', normalize_supplier(label)))
                state = 'excluded' if outgoing or _FOLDER_BLOCK.search(normalize_supplier(label)) else 'pending'
                old = db.execute('SELECT * FROM assistent_mailquellen_ordner WHERE run_id=? AND folder=?', (run['id'], name)).fetchone()
                high_water = int(old['high_water_uid'] or 0) if old else 0
                # A previous manual full inventory is usable only after its
                # complete saved UID list, never an incomplete header cursor.
                if old and not old['incremental'] and old['validity']:
                    ids = json.loads(old['uids_json'] or '[]')
                    if old['cursor'] >= len(ids) and not old['missing']:
                        high_water = max([int(uid) for uid in ids] + [high_water])
                db.execute('''INSERT INTO assistent_mailquellen_ordner
                    (run_id,folder,label,state,outgoing,incremental,high_water_uid)
                    VALUES(?,?,?,?,?,1,?) ON CONFLICT(run_id,folder) DO UPDATE SET
                    label=excluded.label,state=excluded.state,outgoing=excluded.outgoing,incremental=1,
                    high_water_uid=excluded.high_water_uid,poll_ceiling_uid=0,scan_end_uid=0,
                    uids_json='[]',cursor=0,total=0,missing=0,snapshot_at='' ''',
                    (run['id'], name, label, state, int(outgoing), high_water))
            return self._status(db, account)

    @contextmanager
    def owned(self, run):
        with self.db() as db:
            if run.get('_guard') is not None:
                run['_guard'](db)
            db.execute('UPDATE assistent_mailquellen_laeufe SET account=account WHERE id=?', (run['id'],))
            row = db.execute('SELECT lease,run_token,state,lease_until FROM assistent_mailquellen_laeufe WHERE id=?', (run['id'],)).fetchone()
            if not row or row['lease'] != run['lease'] or row['run_token'] != run['run_token'] or row['state'] != 'active' or row['lease_until'] <= time.time():
                raise ValueError('Dieser Einleseschritt ist abgelaufen. Gespeicherte Quellen bleiben erhalten.')
            yield db

    def step(self, guard=None):
        account, own_address = self.identity()
        with self.db() as db:
            if guard is not None:
                guard(db)
            lease = uuid.uuid4().hex
            changed = db.execute("UPDATE assistent_mailquellen_laeufe SET lease=?,lease_until=? WHERE account=? AND state='active' AND lease_until<?", (lease, time.time()+180, account, time.time()))
            if not changed.rowcount:
                return self._status(db, account)
            run = dict(db.execute('SELECT * FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone())
            run['_guard'] = guard
            folder = db.execute("SELECT * FROM assistent_mailquellen_ordner WHERE run_id=? AND state IN ('pending','headers') ORDER BY id LIMIT 1", (run['id'],)).fetchone()
            message = None if folder else db.execute("SELECT * FROM assistent_mailquellen_nachrichten WHERE account=? AND run_token=? AND state='queued' ORDER BY id LIMIT 1", (account, run['run_token'])).fetchone()
        try:
            if folder:
                self._headers(run, dict(folder), own_address)
            elif message:
                self._attachments(run, dict(message), own_address)
            else:
                with self.owned(run) as db:
                    db.execute("UPDATE assistent_mailquellen_laeufe SET state=CASE WHEN state='paused' THEN state ELSE 'done' END,finished_at=? WHERE id=?", (now(), run['id']))
        except Exception:
            with self.db() as db:
                db.execute("UPDATE assistent_mailquellen_laeufe SET state='paused',last_error=? WHERE id=? AND lease=?", ('Verbindung oder Verarbeitung unterbrochen. Erneut starten setzt am letzten gespeicherten Schritt fort.', run['id'], lease))
        finally:
            with self.db() as db:
                db.execute("UPDATE assistent_mailquellen_laeufe SET lease='',lease_until=0 WHERE id=? AND lease=?", (run['id'], lease))
        return self.status()

    def _classify(self, db, account, msg, own_address):
        name, sender = parseaddr(str(msg.get('From', '')))
        sender = address(sender)
        subject = text(msg.get('Subject', ''), 400)
        if financial_header(sender, name, subject):
            return '', '', '', 'excluded'
        rule = classify_invoice_source({'supplier': name or sender, 'reference': subject})
        sender_rule = classify_invoice_source({'supplier': sender})
        if rule['decision'] == 'block' or sender_rule['decision'] == 'block':
            return '', '', '', 'excluded'
        outbound = sender == own_address
        if outbound:
            recipients = [(n, address(a)) for n, a in getaddresses([str(msg.get('To', ''))]) if address(a) != own_address]
            if len(recipients) != 1:
                return sender, text(name), '', 'other'
            name, sender = recipients[0]
            if classify_invoice_source({'supplier': name or sender, 'reference': subject})['decision'] == 'block':
                return '', '', '', 'excluded'
        if not sender:
            return '', text(name), '', 'other' if outbound else 'review'
        approved = db.execute('SELECT supplier FROM assistent_mailquellen_absender WHERE account=? AND email=?', (account, sender)).fetchone()
        supplier = approved['supplier'] if approved else KNOWN_DOMAINS.get(sender.rsplit('@', 1)[-1], '')
        if outbound:
            return sender, text(name), supplier, 'other'
        # A familiar display name on an unrelated domain grants no body access.
        if supplier and classify_invoice_source({'supplier': supplier, 'reference': subject}, [supplier])['allowed']:
            if outbound or _NON_PURCHASE.search(subject):
                return sender, text(name), supplier, 'other'
            return sender, text(name), supplier, 'queued'
        return sender, text(name), '', 'review'

    def _headers(self, run, folder, own_address):
        if folder.get('incremental'):
            return self._incremental_headers(run, folder, own_address)
        with self.mailbox.connect() as client:
            version = self.mailbox.select(client, folder['folder'])
            if not re.fullmatch(r'[1-9][0-9]*', str(version or '')):
                raise ValueError('Ordnerkennung fehlt; keine sichere Fortsetzung möglich.')
            if folder['validity'] and folder['validity'] != version:
                with self.owned(run) as db:
                    db.execute("UPDATE assistent_mailquellen_ordner SET state='changed' WHERE id=?", (folder['id'],))
                    db.execute("UPDATE assistent_mailquellen_nachrichten SET state='review_files',note=? WHERE account=? AND run_token=? AND folder=? AND state='queued'", ('Ordner wurde zwischenzeitlich erneuert; neue Momentaufnahme erforderlich.', run['account'], run['run_token'], folder['folder']))
                return
            if not folder['validity']:
                status, data = client.uid('SEARCH', None, 'ALL')
                if status != 'OK':
                    raise ValueError('Headerinventar nicht erreichbar.')
                ids = [x.decode('ascii') for x in (data[0] or b'').split() if re.fullmatch(rb'[1-9][0-9]*', x)]
                if len(ids) > 1000000:
                    raise ValueError('Ordnergrenze erreicht; separate Prüfung nötig.')
                folder.update(validity=version, uids_json=json.dumps(ids), total=len(ids), snapshot_at=now())
            ids = json.loads(folder['uids_json'])
            chosen = ids[folder['cursor']:folder['cursor']+HEADER_BATCH]
            headers = {}
            if chosen:
                status, rows = client.uid('FETCH', ','.join(chosen), '(UID BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE MESSAGE-ID)])')
                if status != 'OK':
                    raise ValueError('Header nicht erreichbar.')
                for row in rows or []:
                    if not isinstance(row, tuple) or not isinstance(row[1], bytes):
                        continue
                    match = re.search(rb'\bUID (\d+)\b', row[0])
                    if match and match[1].decode() in chosen and len(row[1]) <= 65536:
                        headers[match[1].decode()] = email.message_from_bytes(row[1], policy=policy.default)
        with self.owned(run) as db:
            for uid, msg in headers.items():
                sender, name, supplier, state = self._classify(db, run['account'], msg, own_address)
                if folder.get('outgoing') and state != 'excluded':
                    state = 'other'
                hidden = state == 'excluded'
                db.execute('''INSERT INTO assistent_mailquellen_nachrichten
                  (account,run_token,folder,validity,uid,sender,sender_name,subject,message_date,message_id,supplier,state,updated_at)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account,folder,validity,uid)
                  DO UPDATE SET run_token=excluded.run_token,sender=excluded.sender,sender_name=excluded.sender_name,
                    subject=excluded.subject,message_date=excluded.message_date,message_id=excluded.message_id,supplier=excluded.supplier,
                    state=CASE WHEN excluded.state IN ('excluded','other','review') THEN excluded.state
                      WHEN assistent_mailquellen_nachrichten.state IN ('review','review_files') THEN excluded.state
                      ELSE assistent_mailquellen_nachrichten.state END''',
                  (run['account'], run['run_token'], folder['folder'], version, uid, sender, name,
                   '' if hidden else text(msg.get('Subject', ''), 400), '' if hidden else text(msg.get('Date', '')),
                   '' if hidden else text(msg.get('Message-ID', '')), supplier, state, now()))
            cursor = folder['cursor'] + len(chosen)
            db.execute('''UPDATE assistent_mailquellen_ordner SET validity=?,uids_json=?,total=?,cursor=?,missing=missing+?,snapshot_at=?,state=? WHERE id=?''',
              (version, folder['uids_json'], folder['total'], cursor, len(chosen)-len(headers), folder['snapshot_at'], 'done' if cursor >= len(ids) else 'headers', folder['id']))

    def _incremental_headers(self, run, folder, own_address):
        """Persist header outcomes before advancing a bounded UID checkpoint."""
        with self.mailbox.connect() as client:
            version = self.mailbox.select(client, folder['folder'])
            if not re.fullmatch(r'[1-9][0-9]*', str(version or '')):
                raise ValueError('Ordnerkennung fehlt; keine sichere Fortsetzung möglich.')
            if folder['validity'] and folder['validity'] != version:
                with self.owned(run) as db:
                    db.execute("UPDATE assistent_mailquellen_nachrichten SET state='review_files',note=? WHERE account=? AND folder=? AND validity=? AND state='queued'",
                               ('UIDVALIDITY geändert; alte Nachrichtenreferenz wird nicht erneut geöffnet.', run['account'], folder['folder'], folder['validity']))
                    db.execute("UPDATE assistent_mailquellen_ordner SET validity=?,high_water_uid=0,poll_ceiling_uid=0,scan_end_uid=0,uids_json='[]',cursor=0,total=0,missing=0,snapshot_at='',state='pending' WHERE id=?", (version, folder['id']))
                return
            ceiling = int(folder['poll_ceiling_uid'] or 0)
            if not folder['snapshot_at']:
                values = client.response('UIDNEXT')[1]
                value = values[0].decode('ascii') if values and isinstance(values[0], bytes) else ''
                if not re.fullmatch(r'[1-9][0-9]*', value) or int(value) > 4294967296:
                    raise ValueError('UIDNEXT fehlt; kein sicherer inkrementeller Stand.')
                ceiling = int(value)-1
                folder.update(poll_ceiling_uid=ceiling, snapshot_at=now())
            high_water = int(folder['high_water_uid'] or 0)
            if high_water > ceiling:
                raise ValueError('UID-Fortsetzung ist widersprüchlich; Ordnerprüfung erforderlich.')
            ids = json.loads(folder['uids_json'] or '[]')
            scan_end = int(folder['scan_end_uid'] or 0)
            if scan_end <= high_water:
                scan_end = min(high_water+1000, ceiling)
                ids = []
                if scan_end > high_water:
                    status, data = client.uid('SEARCH', None, 'UID', f'{high_water+1}:{scan_end}')
                    if status != 'OK':
                        raise ValueError('UID-Fenster nicht erreichbar.')
                    ids = sorted({int(uid) for uid in (data[0] or b'').split() if re.fullmatch(rb'[1-9][0-9]*',uid)})
                    # IMAP range semantics can include the last UID for an empty
                    # range; never advance/read a UID outside our exact window.
                    ids = [str(uid) for uid in ids if high_water < uid <= scan_end]
                folder['cursor'] = 0
            chosen = ids[folder['cursor']:folder['cursor']+HEADER_BATCH]
            headers = {}
            if chosen:
                status, rows = client.uid('FETCH', ','.join(chosen), '(UID BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE MESSAGE-ID)])')
                if status != 'OK':
                    raise ValueError('Header nicht erreichbar.')
                for row in rows or []:
                    if isinstance(row, tuple) and isinstance(row[1],bytes):
                        match = re.search(rb'\bUID (\d+)\b',row[0])
                        if match and match[1].decode() in chosen and len(row[1]) <= 65536:
                            headers[match[1].decode()] = email.message_from_bytes(row[1],policy=policy.default)
                missing = sorted(set(chosen)-set(headers))
                if missing:
                    status, data = client.uid('SEARCH', None, 'UID', ','.join(missing))
                    if status != 'OK' or any(uid.decode() in missing for uid in (data[0] or b'').split()):
                        raise ValueError('Header fehlen noch; Checkpoint bleibt unverändert.')
            else:
                missing = []
        with self.owned(run) as db:
            for uid, msg in headers.items():
                sender,name,supplier,state = self._classify(db,run['account'],msg,own_address)
                hidden = state == 'excluded'
                db.execute('''INSERT INTO assistent_mailquellen_nachrichten
                    (account,run_token,folder,validity,uid,sender,sender_name,subject,message_date,message_id,supplier,state,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account,folder,validity,uid) DO UPDATE SET
                    run_token=excluded.run_token,sender=excluded.sender,sender_name=excluded.sender_name,
                    subject=excluded.subject,message_date=excluded.message_date,message_id=excluded.message_id,
                    supplier=excluded.supplier,state=CASE WHEN excluded.state IN ('excluded','other','review') THEN excluded.state
                    WHEN assistent_mailquellen_nachrichten.state IN ('review','review_files') THEN excluded.state
                    ELSE assistent_mailquellen_nachrichten.state END''',
                    (run['account'],run['run_token'],folder['folder'],version,uid,sender,name,
                     '' if hidden else text(msg.get('Subject',''),400), '' if hidden else text(msg.get('Date','')),
                     '' if hidden else text(msg.get('Message-ID','')),supplier,state,now()))
            for uid in missing:
                db.execute('''INSERT INTO assistent_mailquellen_nachrichten
                    (account,run_token,folder,validity,uid,state,note,updated_at) VALUES(?,?,?,?,?,'gone',?,?)
                    ON CONFLICT(account,folder,validity,uid) DO NOTHING''',
                    (run['account'],run['run_token'],folder['folder'],version,uid,'Nachricht nach UID-Erfassung entfernt; kein Inhalt gelesen.',now()))
            cursor = int(folder['cursor'])+len(chosen)
            done_window = cursor >= len(ids)
            if done_window:
                high_water = scan_end
            state = 'done' if done_window and high_water >= ceiling else 'headers'
            db.execute('''UPDATE assistent_mailquellen_ordner SET validity=?,uids_json=?,cursor=?,total=total+?,
                high_water_uid=?,poll_ceiling_uid=?,scan_end_uid=?,snapshot_at=?,state=? WHERE id=?''',
                (version,'[]' if done_window else json.dumps(ids),0 if done_window else cursor,len(chosen),
                 high_water,ceiling,scan_end,folder['snapshot_at'],state,folder['id']))

    def _attachments(self, run, source, own_address):
        # Recheck current metadata permission before downloading any body/part.
        with self.owned(run) as db:
            header = email.message.EmailMessage()
            header['From'] = formataddr((source['sender_name'], source['sender']))
            header['Subject'] = source['subject']
            sender, _, supplier, state = self._classify(db, run['account'], header, own_address)
            if state != 'queued' or sender != source['sender'] or supplier != source['supplier']:
                db.execute("UPDATE assistent_mailquellen_nachrichten SET state=?,note=? WHERE id=?", ('excluded' if state == 'excluded' else 'review', 'Aktuelle Absenderzuordnung passt nicht mehr. Vor Inhaltszugriff prüfen.', source['id']))
                return
        try:
            with self.mailbox.connect() as client:
                self.mailbox.select(client, source['folder'], validity=source['validity'])
                raw, _ = self.mailbox.raw(client, source['uid'])
        except ValueError:
            with self.owned(run) as db:
                db.execute("UPDATE assistent_mailquellen_nachrichten SET state='review_files',note=? WHERE id=?", ('Nachricht nicht mehr unter dieser Referenz lesbar oder größer als 25 MB. Separat prüfen.', source['id']))
            return
        msg = email.message_from_bytes(raw, policy=policy.default)
        with self.owned(run) as db:
            sender, _, supplier, state = self._classify(db, run['account'], msg, own_address)
            if state != 'queued' or sender != source['sender'] or supplier != source['supplier']:
                db.execute("UPDATE assistent_mailquellen_nachrichten SET state='review',note=? WHERE id=?", ('Absender oder Lesefreigabe hat sich geändert. Erneut zuordnen.', source['id']))
                return
            message_id = text(msg.get('Message-ID', ''))
            if message_id != source['message_id']:
                db.execute("UPDATE assistent_mailquellen_nachrichten SET state='review_files',note=? WHERE id=?",
                           ('Message-ID stimmt nicht mehr mit dem Header überein; Quelle prüfen.',source['id']))
                return
            raw_hash = hashlib.sha256(raw).hexdigest()
            fingerprint = hashlib.sha256(json.dumps([run['account'],sender,message_id,raw_hash],separators=(',',':')).encode()).hexdigest()
            duplicate = db.execute("SELECT id,attachments_json,state,note FROM assistent_mailquellen_nachrichten WHERE account=? AND message_fingerprint=? AND state='files' AND id<>? ORDER BY id LIMIT 1", (run['account'],fingerprint,source['id'])).fetchone()
            db.execute('UPDATE assistent_mailquellen_nachrichten SET raw_sha256=?,message_fingerprint=? WHERE id=?', (raw_hash,fingerprint,source['id']))
            if duplicate:
                db.execute('UPDATE assistent_mailquellen_nachrichten SET duplicate_of=?,state=?,attachments_json=?,note=?,updated_at=? WHERE id=?',
                           (duplicate['id'],duplicate['state'],duplicate['attachments_json'],'Identische Originalnachricht bereits erfasst; vorhandene Belege verknüpft.',now(),source['id']))
                return
            results = []
            attachments = [part for part in msg.walk() if part.get_filename() or part.get_content_disposition() == 'attachment']
            for index, part in enumerate(attachments[:MAX_ATTACHMENTS]):
                filename = text(part.get_filename() or 'Anhang', 180)
                rule = classify_invoice_source({'supplier': supplier, 'reference': filename}, [supplier])
                if not rule['allowed'] or financial_metadata(filename):
                    results.append({'state': 'excluded', 'note': 'Kein zulässiger Materialbeleg.'})
                    continue
                is_invoice = bool(_INVOICE.search(filename + ' ' + source['subject']))
                if not is_invoice or _NON_PURCHASE.search(filename) or part.get_content_disposition() == 'inline':
                    results.append({'state': 'other', 'name': filename, 'note': 'Keine Rechnungsdatei; nur als weitere Quelle erfasst.'})
                    continue
                raw_file = part.get_payload(decode=True) or b''
                try:
                    result = self._stage(db, raw_file, filename, supplier)
                    results.append(result)
                except ValueError as error:
                    results.append({'state': 'review', 'name': filename, 'note': str(error)})
            if len(attachments) > MAX_ATTACHMENTS:
                results.append({'state': 'review', 'note': 'Mehr als 20 Anhänge; weitere Dateien bleiben ungeprüft.'})
            state = 'files' if any(r['state'] in ('stored', 'duplicate') for r in results) else 'other'
            if any(r['state'] == 'review' for r in results):
                state = 'review_files'
            note = 'Rechnungsanhänge gespeichert; Artikelauslese erfolgt separat.' if state == 'files' else 'Keine vollständige Inhaltsauswertung. Mailtext und weitere Unterlagen bleiben eigene Quellen.'
            db.execute('UPDATE assistent_mailquellen_nachrichten SET state=?,note=?,attachments_json=?,updated_at=? WHERE id=?', (state, note, json.dumps(results, ensure_ascii=False), now(), source['id']))

    def _stage(self, db, raw, filename, supplier):
        if financial_metadata(filename) or not classify_invoice_source({'supplier': supplier, 'reference': filename}, [supplier])['allowed']:
            raise ValueError('Quelle vom Materialimport ausgeschlossen.')
        if not raw or len(raw) > MAX_FILE:
            raise ValueError('Datei leer oder größer als 20 MB; separat prüfen.')
        if raw.startswith(b'%PDF-'):
            suffix, mime = '.pdf', 'application/pdf'
        else:
            try:
                from PIL import Image
                with Image.open(io.BytesIO(raw)) as image:
                    suffix, mime = {'JPEG': ('.jpg', 'image/jpeg'), 'PNG': ('.png', 'image/png'), 'WEBP': ('.webp', 'image/webp')}[image.format]
                    image.verify()
            except Exception:
                raise ValueError('Kein unterstütztes Rechnungs-PDF oder Bild; separat prüfen.') from None
        digest = hashlib.sha256(raw).hexdigest()
        existing = db.execute('SELECT * FROM assistent_mailquellen_dateien WHERE sha256=?', (digest,)).fetchone()
        if existing:
            if normalize_supplier(existing['supplier']) != normalize_supplier(supplier):
                raise ValueError('Identische Datei ist einem anderen Lieferanten zugeordnet; prüfen.')
            if not db.execute("SELECT id FROM einkauf_belege WHERE id=? AND beleg_typ='rechnung'", (existing['beleg_id'],)).fetchone():
                raise ValueError('Bereits importierter Beleg ist gesperrt oder entfernt; keine automatische Neuanlage.')
            self._restore_row(dict(existing))
            return {'state': 'duplicate', 'beleg_id': existing['beleg_id'], 'sha256': digest, 'name': existing['original_name']}
        name = (Path(secure_filename(filename)).stem or 'Lieferantenrechnung')[:120] + suffix
        stored = 'mailquelle-' + digest + suffix
        original = None
        # Recognise a byte-identical prior manual upload, without OCR or body logs.
        for row in db.execute('SELECT id,lieferant,stored_name,beleg_typ FROM einkauf_belege WHERE size=?', (len(raw),)).fetchall():
            candidate = Path(self.p.UPLOAD_DIR) / str(row['stored_name'])
            if candidate.resolve().parent != Path(self.p.UPLOAD_DIR).resolve() or not candidate.is_file():
                continue
            if hashlib.sha256(candidate.read_bytes()).hexdigest() == digest:
                if row['beleg_typ'] != 'rechnung':
                    raise ValueError('Identische Datei ist kein freigegebener Rechnungsbeleg; prüfen.')
                if normalize_supplier(row['lieferant']) != normalize_supplier(supplier):
                    raise ValueError('Datei bereits mit anderem Lieferanten gespeichert; prüfen.')
                original = dict(row)
                break
        if original:
            beleg_id, stored = original['id'], original['stored_name']
        else:
            path = Path(self.p.UPLOAD_DIR) / stored
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.resolve().parent != Path(self.p.UPLOAD_DIR).resolve():
                raise ValueError('Dateipfad verlässt die interne Ablage.')
            if path.exists():
                if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                    raise ValueError('Vorhandene Datei stimmt nicht überein; kein Überschreiben.')
            else:
                path.write_bytes(raw)
            cursor = db.execute('''INSERT INTO einkauf_belege(beleg_typ,lieferant,original_name,stored_name,mime_type,size,extrahierter_text,positionen_count,status,erstellt_am)
                VALUES('rechnung',?,?,?,?,?,'',0,'importiert',?)''', (supplier, name, stored, mime, len(raw), now()))
            beleg_id = cursor.lastrowid
        db.execute('''INSERT INTO assistent_mailquellen_dateien(sha256,supplier,beleg_id,stored_name,original_name,file_base64,size,created_at)
            VALUES(?,?,?,?,?,?,?,?)''', (digest, supplier, beleg_id, stored, name, base64.b64encode(raw).decode('ascii'), len(raw), now()))
        return {'state': 'duplicate' if original else 'stored', 'beleg_id': beleg_id, 'sha256': digest, 'name': name}

    def _restore_row(self, row):
        name = row['stored_name']
        if not name or Path(name).name != name or '/' in name or '\\' in name:
            raise ValueError('Ungültiger gesicherter Dateiname.')
        path = Path(self.p.UPLOAD_DIR) / name
        if path.resolve().parent != Path(self.p.UPLOAD_DIR).resolve():
            raise ValueError('Dateipfad verlässt die interne Ablage.')
        if path.is_file():
            if path.stat().st_size != row['size'] or hashlib.sha256(path.read_bytes()).hexdigest() != row['sha256']:
                raise ValueError('Vorhandene Datei stimmt nicht mit der Sicherung überein; kein Überschreiben.')
            return
        raw = base64.b64decode(row['file_base64'], validate=True)
        if len(raw) != row['size'] or hashlib.sha256(raw).hexdigest() != row['sha256']:
            raise ValueError('Dateisicherung muss geprüft werden.')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    def restore_files(self):
        restored = 0
        with self.db() as db:
            rows = db.execute('''SELECT f.id,f.stored_name FROM assistent_mailquellen_dateien f
                JOIN einkauf_belege b ON b.id=f.beleg_id AND b.stored_name=f.stored_name''').fetchall()
            for row in rows:
                if not (Path(self.p.UPLOAD_DIR) / row['stored_name']).is_file():
                    self._restore_row(dict(db.execute('SELECT * FROM assistent_mailquellen_dateien WHERE id=?', (row['id'],)).fetchone()))
                    restored += 1
        return restored

    def restore_file(self, stored_name):
        if not isinstance(stored_name, str) or not stored_name or Path(stored_name).name != stored_name or '/' in stored_name or '\\' in stored_name:
            return False
        with self.db() as db:
            row = db.execute('''SELECT f.* FROM assistent_mailquellen_dateien f JOIN einkauf_belege b
                ON b.id=f.beleg_id AND b.stored_name=f.stored_name WHERE f.stored_name=? AND b.beleg_typ='rechnung' ''', (stored_name,)).fetchone()
            if not row:
                return False
            try:
                self._restore_row(dict(row))
            except (ValueError, OSError):
                return False
            return True

    def approve(self, message_id, supplier):
        account, _ = self.identity()
        supplier = text(supplier, 180)
        if not supplier or not classify_invoice_source({'supplier': supplier}, [supplier])['allowed']:
            raise ValueError('Eindeutigen Materiallieferanten angeben. Finanz- und Privatquellen bleiben ausgeschlossen.')
        with self.db() as db:
            row = db.execute("SELECT * FROM assistent_mailquellen_nachrichten WHERE id=? AND account=? AND state='review'", (message_id, account)).fetchone()
            if not row or not address(row['sender']):
                raise ValueError('Eindeutiger Absender fehlt oder diese Quelle wurde bereits eingeordnet.')
            if financial_header(row['sender'], row['sender_name'], row['subject']) or classify_invoice_source({'supplier': row['sender_name'] or row['sender'], 'reference': row['subject']})['decision'] == 'block':
                raise ValueError('Diese Quelle bleibt vom Materialimport ausgeschlossen.')
            db.execute('''INSERT INTO assistent_mailquellen_absender(account,email,supplier,approved_at) VALUES(?,?,?,?)
                ON CONFLICT(account,email) DO UPDATE SET supplier=excluded.supplier,approved_at=excluded.approved_at''', (account, row['sender'], supplier, now()))
            configured = db.execute("SELECT value FROM app_settings WHERE key='ASSISTANT_MATERIAL_SUPPLIERS'").fetchone()
            try:
                allowed = json.loads(configured['value']) if configured else []
                if not isinstance(allowed, list):
                    allowed = []
            except (ValueError, TypeError):
                allowed = []
            if supplier not in allowed:
                allowed.append(supplier)
            db.execute('''INSERT INTO app_settings(key,value,updated_at) VALUES('ASSISTANT_MATERIAL_SUPPLIERS',?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at RETURNING key''', (json.dumps(allowed, ensure_ascii=False), now()))
            for item in db.execute("SELECT id,subject FROM assistent_mailquellen_nachrichten WHERE account=? AND sender=? AND state='review'", (account, row['sender'])).fetchall():
                if classify_invoice_source({'supplier': supplier, 'reference': item['subject']}, [supplier])['allowed']:
                    state = 'other' if _NON_PURCHASE.search(item['subject']) else 'queued'
                    db.execute("UPDATE assistent_mailquellen_nachrichten SET supplier=?,state=?,note='' WHERE id=?", (supplier, state, item['id']))
            # Keep the existing snapshot when continuing after classifications.
            db.execute("UPDATE assistent_mailquellen_laeufe SET state='paused',finished_at='' WHERE account=? AND state='done'", (account,))
            return self._status(db, account)

    def _quarantine(self, db, account):
        """Revoke historical misclassification using metadata only, no blob reads."""
        denied = set()
        for row in db.execute("SELECT id,sender,sender_name,subject,attachments_json FROM assistent_mailquellen_nachrichten WHERE account=? AND state<>'excluded'", (account,)).fetchall():
            try:
                attachments = json.loads(row['attachments_json'] or '[]')
            except (ValueError, TypeError):
                attachments = []
            attachments = attachments if isinstance(attachments, list) else []
            blocked = financial_header(row['sender'], row['sender_name'], row['subject'])
            changed = False
            for item in attachments:
                if not isinstance(item, dict):
                    continue
                if blocked or financial_metadata(item.get('name')):
                    bid = item.get('beleg_id')
                    if isinstance(bid, int) and not isinstance(bid, bool) and bid > 0:
                        denied.add(bid)
                    item['state'] = 'excluded'
                    item['note'] = 'Quelle vom Materialimport ausgeschlossen.'
                    changed = True
            if blocked or changed:
                db.execute("UPDATE assistent_mailquellen_nachrichten SET state=?,note=?,attachments_json=? WHERE id=?", ('excluded' if blocked else 'review_files', 'Quelle vom Materialimport ausgeschlossen.', json.dumps(attachments, ensure_ascii=False), row['id']))
        for row in db.execute('SELECT beleg_id,original_name FROM assistent_mailquellen_dateien').fetchall():
            if financial_metadata(row['original_name']):
                denied.add(row['beleg_id'])
        for bid in denied:
            db.execute("UPDATE einkauf_belege SET beleg_typ='gesperrt' WHERE id=?", (bid,))
            db.execute("UPDATE assistent_rechnungsartikel SET active=0 WHERE import_id IN (SELECT id FROM assistent_rechnungsimporte WHERE source_kind='einkauf' AND source_id=?)", (str(bid),))
            db.execute("UPDATE assistent_rechnungsimporte SET state='ausgeschlossen',lease='',result_json=? WHERE source_kind='einkauf' AND source_id=?", (json.dumps({'hinweise':['Quelle vom Materialimport ausgeschlossen.'],'positionen':0}), str(bid)))

    def _status(self, db, account, query=''):
        query = text(query, 150)
        row = db.execute('SELECT * FROM assistent_mailquellen_laeufe WHERE account=?', (account,)).fetchone()
        if not row:
            return {'state': 'new', 'folders': [], 'counts': {}, 'unknown_senders': [], 'complete': False}
        run = dict(row)
        folders = [dict(r) for r in db.execute('SELECT id,label,state,total,cursor,missing,snapshot_at FROM assistent_mailquellen_ordner WHERE run_id=? ORDER BY id', (run['id'],)).fetchall()]
        counts = {r['state']: r['n'] for r in db.execute('SELECT state,COUNT(*) AS n FROM assistent_mailquellen_nachrichten WHERE account=? AND run_token=? GROUP BY state', (account, run['run_token'])).fetchall()}
        where, params = '', [account, run['run_token']]
        if query:
            pattern = '%' + query.lower().replace('!', '!!').replace('%', '!%').replace('_', '!_') + '%'
            where = " AND (LOWER(sender) LIKE ? ESCAPE '!' OR LOWER(sender_name) LIKE ? ESCAPE '!' OR LOWER(subject) LIKE ? ESCAPE '!')"
            params += [pattern, pattern, pattern]
        unknown = [dict(r) for r in db.execute("SELECT MIN(id) AS id,sender,sender_name,COUNT(*) AS n FROM assistent_mailquellen_nachrichten WHERE account=? AND run_token=? AND state='review'" + where + " GROUP BY sender,sender_name ORDER BY n DESC,sender LIMIT 100", params).fetchall()]
        quarantined = db.execute("SELECT COUNT(DISTINCT f.beleg_id) AS n FROM assistent_mailquellen_dateien f JOIN einkauf_belege b ON b.id=f.beleg_id WHERE b.beleg_typ='gesperrt'").fetchone()['n']
        sources = [dict(r) for r in db.execute("SELECT supplier,sender,subject,state,note,attachments_json FROM assistent_mailquellen_nachrichten WHERE account=? AND run_token=? AND supplier<>'' AND state IN ('files','other','review_files') ORDER BY id DESC LIMIT 50", (account, run['run_token'])).fetchall()]
        for source in sources:
            source['attachments'] = json.loads(source.pop('attachments_json'))
        headers_complete = all(f['state'] in ('done', 'excluded') and not f['missing'] for f in folders)
        return {'state': run['state'], 'started_at': run['started_at'], 'finished_at': run['finished_at'], 'busy': run['lease_until'] > time.time(),
                'folders': folders, 'counts': counts, 'unknown_senders': unknown, 'query': query, 'quarantined_files': quarantined, 'sources': sources, 'error': run['last_error'],
                'headers_complete': headers_complete,
                'complete': run['state'] == 'done' and headers_complete and not any(counts.get(key) for key in ('queued', 'review', 'review_files', 'other')),
                'hint': 'Ordner- und Headerstand zur angegebenen Erfassung. Nur Rechnungsanhänge werden übernommen; Mailtexte, Datenblätter und sonstige Anhänge sind noch nicht vollständig ausgewertet. Ausgeschlossene Ordner wurden nicht geöffnet.'}

    def status(self, query=''):
        account, _ = self.identity()
        with self.db() as db:
            self._quarantine(db, account)
            return self._status(db, account, query)


def register_mail_sources(portal):
    service = MailSources(portal)
    portal.assistant_mail_sources_init_schema = service.init_schema
    portal.assistant_mail_sources_restore_files = service.restore_files
    portal.assistant_mail_sources_restore_file = service.restore_file
    bp = Blueprint('assistant_mail_sources', __name__, url_prefix='/admin/assistent-mailquellen')

    def csrf():
        supplied = request.headers.get('X-CSRF-Token') or request.form.get('csrf_token') or ''
        expected = session.get('csrf_token') or ''
        if not expected or not hmac.compare_digest(str(supplied), str(expected)):
            abort(403)

    @bp.errorhandler(ValueError)
    def invalid(error):
        return jsonify(error=str(error)), 400

    @bp.get('')
    @portal.admin_required
    def index():
        return render_template('assistent_mailquellen.html', report=service.status(request.args.get('q', '')))

    @bp.get('/status')
    @portal.admin_required
    def status_route():
        return jsonify(service.status(request.args.get('q', '')))

    @bp.post('/start')
    @portal.admin_required
    def start_route():
        csrf()
        return jsonify(service.start())

    @bp.post('/weiter')
    @portal.admin_required
    def step_route():
        csrf()
        return jsonify(service.step())

    @bp.post('/pause')
    @portal.admin_required
    def pause_route():
        csrf()
        return jsonify(service.pause())

    @bp.post('/absender/<int:message_id>/freigeben')
    @portal.admin_required
    def approve_route(message_id):
        csrf()
        return jsonify(service.approve(message_id, request.form.get('lieferant')))

    @bp.after_request
    def no_store(response):
        response.headers['Cache-Control'] = 'private, no-store'
        return response

    portal.app.register_blueprint(bp)
    return service
