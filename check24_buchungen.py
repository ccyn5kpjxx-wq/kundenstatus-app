"""CHECK24 mail inbox and idempotent workshop import (no provider-side writes)."""
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
import hashlib
import json
import os
import re
import threading
import time
import uuid

from flask import Blueprint, abort, flash, make_response, redirect, render_template, request, url_for
from check24_mail import BERLIN, MAX_MAIL_BYTES, parse_check24_mail
from mailbox_client import Mailbox, quote

SYNC_LEASE_SECONDS = 300
SYNC_BUDGET_SECONDS = 60
SYNC_BATCH_LIMIT = 50


class Check24SyncStopped(RuntimeError):
    """Safe interruption: stale lease, revoked worker or changed mailbox."""


class Check24SyncBusy(RuntimeError):
    """Another process is already scanning this mailbox."""


def ionos_verified(raw, config):
    """Only call for bytes fetched from this configured TLS mailbox, never uploads.

    Trust the receiving IONOS server's top authentication result, not arbitrary
    lower headers. Unknown formats deliberately stay in the review queue.
    """
    if not config.get('ssl') or config.get('host', '').lower() not in {'imap.ionos.de', 'imap.1und1.de'}:
        return False
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    auth = msg.get_all('Authentication-Results', [])
    received = msg.get_all('Received', [])
    if not auth or not received:
        return False
    headers = [k.lower() for k, _ in msg.raw_items()]
    if headers.index('authentication-results') > headers.index('received'):
        return False
    top = str(auth[0])
    # Parenthesized comments and quoted strings could hide fake result tokens.
    if any(c in top for c in '()"'):
        return False
    clauses = top.split(';')
    if clauses[0].strip().lower() != 'kundenserver.de':
        return False
    parsed = {}
    for clause in clauses[1:]:
        tokens = clause.strip().lower().split()
        if not tokens:
            continue
        key, sep, value = tokens[0].partition('=')
        if key not in {'dkim', 'spf', 'dmarc'}:
            continue
        if not sep or value != 'pass' or key in parsed:
            return False
        props = {}
        for token in tokens[1:]:
            p, sep, v = token.partition('=')
            if not sep or p in props:
                return False
            props[p] = v
        parsed[key] = props
    if set(parsed) != {'dkim', 'spf', 'dmarc'}:
        return False
    if parsed['dkim'].get('header.i') != '@check24.de' or parsed['spf'].get('smtp.mailfrom') != 'montagepartner@check24.de' or parsed['dmarc'].get('header.from') != 'check24.de':
        return False
    if not re.search(r'\bby\s+mx\.kundenserver\.de\s+\(', str(received[0]), re.I):
        return False
    recipients = {address.lower() for _, address in getaddresses(msg.get_all('To', []))}
    if config.get('user', '').lower() not in recipients:
        return False
    try:
        arrival = parsedate_to_datetime(str(received[0]).rsplit(';', 1)[1])
        sent = parsedate_to_datetime(str(msg.get('Date', '')))
        if arrival.tzinfo is None or sent.tzinfo is None or abs(arrival - sent) > timedelta(days=1):
            return False
    except (ValueError, TypeError, IndexError):
        return False
    return True


class Check24Bookings:
    def __init__(self, portal):
        self.p = portal
        self._worker_lock = threading.Lock()
        self._worker = None
        self._worker_pid = None
        self._worker_stop = threading.Event()
        self._clock = time.time
        self._monotonic = time.monotonic
        self.init_schema()

    def init_schema(self):
        db = self.p.get_db()
        try:
            db.execute('''CREATE TABLE IF NOT EXISTS check24_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, raw_hash TEXT NOT NULL UNIQUE,
                source_key TEXT NOT NULL, provider_id TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'review',
                note TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                verified INTEGER NOT NULL DEFAULT 0)''')
            db.execute('''CREATE TABLE IF NOT EXISTS check24_bookings (
                provider_id TEXT PRIMARY KEY, order_id INTEGER NOT NULL UNIQUE,
                start_at TEXT NOT NULL, end_at TEXT NOT NULL,
                event_id INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1)''')
            db.execute('''CREATE TABLE IF NOT EXISTS check24_sync_state (
                key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '')''')
            db.execute('''CREATE TABLE IF NOT EXISTS check24_receipts (
                source_key TEXT PRIMARY KEY, raw_hash TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS check24_worker_state (
                id INTEGER PRIMARY KEY, account TEXT NOT NULL DEFAULT '',
                lease_token TEXT NOT NULL DEFAULT '', lease_until DOUBLE PRECISION NOT NULL DEFAULT 0,
                next_poll_at DOUBLE PRECISION NOT NULL DEFAULT 0, heartbeat_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_started_at DOUBLE PRECISION NOT NULL DEFAULT 0, last_success_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_error_code TEXT NOT NULL DEFAULT '', last_count INTEGER NOT NULL DEFAULT 0,
                failures INTEGER NOT NULL DEFAULT 0)''')
            db.execute('INSERT INTO check24_worker_state(id) VALUES(1) ON CONFLICT(id) DO NOTHING RETURNING id').fetchall()
            db.execute("INSERT INTO check24_sync_state(key,value) VALUES('booking_lock','') ON CONFLICT(key) DO NOTHING RETURNING key").fetchall()
            db.commit()
        finally:
            db.close()

    def lock(self, db):
        # Shared with the native reservation transaction; also a row lock on PG.
        db.execute("UPDATE check24_sync_state SET value=value WHERE key='booking_lock'")

    def overlaps(self, db, start, end, exclude=''):
        rows = db.execute('SELECT start_at,end_at FROM check24_bookings WHERE active=1 AND provider_id!=?', (exclude,)).fetchall()
        return any(datetime.fromisoformat(row['start_at']) < end and
                   datetime.fromisoformat(row['end_at']) > start for row in rows)

    def native_slot_blocked(self, db, day, time):
        start = datetime.fromisoformat(day + 'T' + time).replace(tzinfo=BERLIN)
        return self.overlaps(db, start, start + timedelta(hours=1))

    def _conflict(self, db, start, end, exclude=''):
        if self.overlaps(db, start, end, exclude):
            return True
        rows = db.execute('''SELECT s.datum,s.uhrzeit FROM reifen_termin_slots s
            JOIN leads l ON l.id=s.lead_id WHERE l.status!='verloren' AND s.datum=?''',
            (start.date().isoformat(),)).fetchall()
        for row in rows:
            existing = datetime.fromisoformat(row['datum'] + 'T' + row['uhrzeit']).replace(tzinfo=BERLIN)
            if existing < end and existing + timedelta(hours=1) > start:
                return True
        return False

    def _apply(self, db, event):
        data = json.loads(event['payload'])
        if not data.get('valid') or data.get('kind') != 'new':
            raise ValueError('Diese Nachricht muss anhand des CHECK24-Portals manuell geklärt werden.')
        old = db.execute('SELECT order_id FROM check24_bookings WHERE provider_id=?', (data['provider_id'],)).fetchone()
        if old:
            raise ValueError('Bestellnummer bereits übernommen. Abweichende Nachricht bitte mit dem vorhandenen Auftrag vergleichen.')
        start, end = datetime.fromisoformat(data['start_at']), datetime.fromisoformat(data['end_at'])
        if start <= datetime.now(BERLIN):
            raise ValueError('Der Termin liegt in der Vergangenheit. Bitte im Auftrag manuell nacherfassen.')
        if start.weekday() == 6 or start.hour < 7 or end.hour > 18 or (end.hour == 18 and end.minute):
            raise ValueError('Termin außerhalb der veröffentlichten Öffnungszeiten. Bitte im CHECK24-Portal prüfen.')
        if self._conflict(db, start, end):
            raise ValueError('Terminüberschneidung: Zeit ist bereits belegt. Bitte mit CHECK24 und Werkstatt abstimmen.')
        description = '\n'.join(filter(None, [
            'CHECK24 · Bestellung ' + data['provider_id'], data['service'],
            'Termin: ' + start.strftime('%d.%m.%Y %H:%M') + '–' + end.strftime('%H:%M') + ' Uhr',
            *data['tires'], 'Voraussichtliche Reifenlieferung: ' + data['delivery_date'] if data['delivery_date'] else '',
            'Gebuchter Gesamtpreis: ' + data['total_gross'].replace('.', ',') + ' € brutto' if data['total_gross'] else '',
        ]))
        intake = {'anliegen': 'reifenservice', 'reifen_leistungen': ['montage'],
                  'check24_bestellnummer': data['provider_id'], 'check24_start': data['start_at'],
                  'check24_ende': data['end_at'], 'check24_preis_brutto': data['total_gross']}
        order_id = self.p.create_auftrag('check24', kunde_name=data['customer_name'],
            kunde_email=data['customer_email'], kontakt_telefon=data['customer_phone'],
            fahrzeug='Reifenservice · CHECK24', auftragsnummer=data['provider_id'],
            schadenart='Reifenservice', leistungsart='reifenservice', beschreibung=description,
            analyse=description, annahme_datum=start.strftime('%d.%m.%Y'),
            schaden_aufnahme_json=json.dumps(intake, ensure_ascii=False), _db=db)
        db.execute('UPDATE auftraege SET status=2,annahme_uhrzeit=? WHERE id=?', (start.strftime('%H:%M'), order_id))
        db.execute('INSERT INTO status_log(auftrag_id,status,zeitstempel) VALUES(?,2,?)', (order_id, self.p.now_str()))
        db.execute('''INSERT INTO check24_bookings(provider_id,order_id,start_at,end_at,event_id)
            VALUES(?,?,?,?,?) RETURNING provider_id''', (data['provider_id'], order_id, data['start_at'], data['end_at'], event['id'])).fetchall()
        db.execute("UPDATE check24_events SET state='applied',note=? WHERE id=?", ('Als Auftrag ' + str(order_id) + ' übernommen.', event['id']))
        return order_id

    def ingest(self, raw, source_key='upload', verified=False, auto_apply=False, _sync_claim=None):
        data = parse_check24_mail(raw)
        # Caller provenance is never accepted from a public form or EML header.
        if not verified:
            auto_apply = False
        db = self.p.get_db()
        try:
            if _sync_claim:
                self._guard_sync(db, _sync_claim)
            self.lock(db)
            db.execute('''INSERT INTO check24_events(raw_hash,source_key,provider_id,payload,created_at,verified)
                VALUES(?,?,?,?,?,?) ON CONFLICT(raw_hash) DO NOTHING''',
                (data['raw_sha256'], source_key, data['provider_id'], json.dumps(data, ensure_ascii=False), self.p.now_str(), int(verified)))
            event = db.execute('SELECT * FROM check24_events WHERE raw_hash=?', (data['raw_sha256'],)).fetchone()
            if source_key != 'upload':
                db.execute('INSERT INTO check24_receipts(source_key,raw_hash) VALUES(?,?) ON CONFLICT(source_key) DO NOTHING RETURNING source_key',
                           (source_key, data['raw_sha256'])).fetchall()
            if event['state'] == 'review' and data.get('valid'):
                previous = db.execute('''SELECT e.payload FROM check24_bookings b
                    JOIN check24_events e ON e.id=b.event_id WHERE b.provider_id=?''', (data['provider_id'],)).fetchone()
                fields = ('provider_id', 'kind', 'start_at', 'end_at', 'customer_name', 'customer_phone',
                          'customer_email', 'service', 'tires', 'total_gross', 'delivery_date')
                if previous and all(data.get(k) == json.loads(previous['payload']).get(k) for k in fields):
                    db.execute("UPDATE check24_events SET state='duplicate',note='Diese Buchung ist bereits im Werkstattauftrag enthalten.' WHERE id=?", (event['id'],))
                    event = db.execute('SELECT * FROM check24_events WHERE id=?', (event['id'],)).fetchone()
            if auto_apply and event['state'] == 'review' and event['verified']:
                try:
                    self._apply(db, event)
                except ValueError as exc:
                    db.execute('UPDATE check24_events SET note=? WHERE id=?', (str(exc), event['id']))
            db.commit()
            return event['id']
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def apply(self, event_id):
        db = self.p.get_db()
        try:
            self.lock(db)
            event = db.execute('SELECT * FROM check24_events WHERE id=?', (event_id,)).fetchone()
            if not event:
                raise ValueError('Nachricht nicht gefunden.')
            if event['state'] == 'applied':
                row = db.execute('SELECT order_id FROM check24_bookings WHERE event_id=?', (event_id,)).fetchone()
                return row['order_id']
            if event['state'] != 'review':
                raise ValueError('Nachricht wurde bereits geprüft.')
            order_id = self._apply(db, event)
            db.commit()
            return order_id
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def rows(self, page=1):
        db = self.p.get_db()
        try:
            rows = db.execute('''SELECT e.*,b.order_id,b.start_at AS current_start,b.end_at AS current_end FROM check24_events e LEFT JOIN check24_bookings b
                ON b.provider_id=e.provider_id ORDER BY CASE WHEN e.state='review' THEN 0 ELSE 1 END,e.id DESC
                LIMIT 50 OFFSET ?''', ((max(1, page)-1)*50,)).fetchall()
            return [dict(row) | {'data': json.loads(row['payload'])} for row in rows]
        finally:
            db.close()

    def review_count(self):
        db = self.p.get_db()
        try:
            return db.execute("SELECT COUNT(*) FROM check24_events WHERE state='review'").fetchone()[0]
        finally:
            db.close()

    def cancel(self, event_id, note):
        """Explicit office confirmation after checking the provider; never from email text."""
        db = self.p.get_db()
        try:
            self.lock(db)
            event = db.execute('SELECT * FROM check24_events WHERE id=?', (event_id,)).fetchone()
            if not event:
                raise ValueError('Nachricht nicht gefunden.')
            booking = db.execute('SELECT * FROM check24_bookings WHERE provider_id=?', (event['provider_id'],)).fetchone()
            if not booking:
                raise ValueError('Kein zugehöriger CHECK24-Auftrag gefunden.')
            db.execute('UPDATE auftraege SET status=status WHERE id=?', (booking['order_id'],))
            order = db.execute('SELECT * FROM auftraege WHERE id=?', (booking['order_id'],)).fetchone()
            if not order or int(order['status'] or 0) >= 3 or order['lexware_invoice_id'] or order['rechnung_nummer']:
                raise ValueError('Auftrag bereits in Arbeit oder abgerechnet. Storno zuerst im Büro klären.')
            if booking['active']:
                db.execute('UPDATE check24_bookings SET active=0 WHERE provider_id=?', (booking['provider_id'],))
                db.execute('UPDATE auftraege SET archiviert=1,geaendert_am=? WHERE id=?', (self.p.now_str(), booking['order_id']))
            db.execute("UPDATE check24_events SET state='resolved',note=? WHERE id=?", ('Storno im CHECK24-Portal geprüft, Werkstattzeit freigegeben: ' + note, event_id))
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def reschedule(self, event_id, day, begin, finish, note):
        from check24_mail import _local_datetime
        from datetime import date
        try:
            start = _local_datetime(date.fromisoformat(day), begin)
            end = _local_datetime(date.fromisoformat(day), finish)
        except (ValueError, TypeError):
            raise ValueError('Bitte gültiges Datum und eindeutige Uhrzeiten eintragen.')
        if start <= datetime.now(BERLIN) or start >= end or start.weekday() == 6 or start.hour < 7 or end.hour > 18 or (end.hour == 18 and end.minute):
            raise ValueError('Bitte ein zukünftiges Zeitintervall innerhalb der Öffnungszeiten auswählen.')
        db = self.p.get_db()
        try:
            self.lock(db)
            event = db.execute('SELECT * FROM check24_events WHERE id=?', (event_id,)).fetchone()
            booking = db.execute('SELECT * FROM check24_bookings WHERE provider_id=?', (event['provider_id'],)).fetchone() if event else None
            if not booking or not booking['active']:
                raise ValueError('Keine aktive Buchung gefunden.')
            db.execute('UPDATE auftraege SET status=status WHERE id=?', (booking['order_id'],))
            order = db.execute('SELECT * FROM auftraege WHERE id=?', (booking['order_id'],)).fetchone()
            if not order or int(order['status'] or 0) >= 3 or order['lexware_invoice_id'] or order['rechnung_nummer']:
                raise ValueError('Auftrag bereits in Arbeit oder abgerechnet. Änderung zuerst im Büro klären.')
            if self._conflict(db, start, end, booking['provider_id']):
                raise ValueError('Die neue Zeit überschneidet sich mit einer vorhandenen Buchung.')
            intake = json.loads(order['schaden_aufnahme_json'] or '{}')
            intake.update(check24_start=start.isoformat(), check24_ende=end.isoformat())
            db.execute('UPDATE check24_bookings SET start_at=?,end_at=? WHERE provider_id=?', (start.isoformat(), end.isoformat(), booking['provider_id']))
            description = re.sub(r'(?m)^Termin: .+$', 'Termin: ' + start.strftime('%d.%m.%Y %H:%M') + '–' + end.strftime('%H:%M') + ' Uhr', order['beschreibung'] or '')
            analysis = re.sub(r'(?m)^Termin: .+$', 'Termin: ' + start.strftime('%d.%m.%Y %H:%M') + '–' + end.strftime('%H:%M') + ' Uhr', order['analyse_text'] or '')
            db.execute('''UPDATE auftraege SET annahme_datum=?,annahme_uhrzeit=?,schaden_aufnahme_json=?,
                beschreibung=?,analyse_text=?,geaendert_am=? WHERE id=?''',
                (start.strftime('%d.%m.%Y'), start.strftime('%H:%M'), json.dumps(intake, ensure_ascii=False), description, analysis, self.p.now_str(), booking['order_id']))
            db.execute("UPDATE check24_events SET state='resolved',note=? WHERE id=?", ('Termin nach CHECK24-Abgleich geändert: ' + start.strftime('%d.%m.%Y %H:%M') + '–' + end.strftime('%H:%M') + '. ' + note, event_id))
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def worker_enabled(self):
        value = self.p.app.config.get('CHECK24_WORKER_ENABLED')
        if value is None:
            value = os.environ.get('CHECK24_WORKER_ENABLED', '').strip().lower() in {'1', 'true', 'yes', 'on'}
        return value is True

    def worker_interval(self):
        try:
            return max(60, min(86400, int(self.p.app.config.get('CHECK24_WORKER_INTERVAL_SECONDS',
                os.environ.get('CHECK24_WORKER_INTERVAL_SECONDS', '300')))))
        except (TypeError, ValueError):
            return 300

    @staticmethod
    def _account(config):
        # This fingerprint identifies a configuration, never exposes its secret.
        fields = {key: config.get(key) for key in ('host', 'port', 'user', 'folder', 'ssl', 'password')}
        return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()

    def _claim_sync(self, config, worker):
        if worker and (not self.worker_enabled() or self._worker_stop.is_set()):
            return None
        now = self._clock()
        claim = {'token': uuid.uuid4().hex, 'account': self._account(config), 'worker': worker}
        db = self.p.get_db()
        try:
            if worker:
                db.execute('UPDATE check24_worker_state SET heartbeat_at=? WHERE id=1', (now,))
            changed = db.execute('''UPDATE check24_worker_state SET account=?,lease_token=?,lease_until=?,last_started_at=?
                WHERE id=1 AND (lease_token='' OR lease_until<=?) AND (next_poll_at<=? OR ?=0)''',
                (claim['account'], claim['token'], now + SYNC_LEASE_SECONDS, now, now, now, int(worker))).rowcount
            db.commit()
            return claim if changed == 1 else None
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _guard_sync(self, db, claim, *, renew=False):
        if self._account(self.p.get_werkstatt_imap_config()) != claim['account'] or (
            claim['worker'] and (not self.worker_enabled() or self._worker_stop.is_set())):
            raise Check24SyncStopped('CHECK24-Abgleich angehalten.')
        # The row stays locked until the caller commits the message publication.
        # A pre-fetch check alone cannot fence a worker whose lease expires in I/O.
        now = self._clock()
        changed = db.execute('''UPDATE check24_worker_state SET lease_until=CASE WHEN ?=1 THEN ? ELSE lease_until END
            WHERE id=1 AND account=? AND lease_token=? AND lease_until>?''',
            (int(renew), now + SYNC_LEASE_SECONDS, claim['account'], claim['token'], now)).rowcount
        if changed != 1:
            raise Check24SyncStopped('CHECK24-Abgleich angehalten.')

    def _renew_sync(self, claim):
        db = self.p.get_db()
        try:
            self._guard_sync(db, claim, renew=True)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _finish_sync(self, claim, processed, error=''):
        db = self.p.get_db()
        try:
            db.execute('UPDATE check24_worker_state SET id=id WHERE id=1')
            row = db.execute('SELECT * FROM check24_worker_state WHERE id=1').fetchone()
            # An expired owner must not publish success or clear its successor.
            if not row or row['lease_token'] != claim['token'] or row['lease_until'] <= self._clock():
                db.rollback()
                return
            if not error:
                self._guard_sync(db, claim)
            now = self._clock()
            failures = int(row['failures']) + 1 if error else 0
            delay = min(900, 60 * 2 ** min(failures - 1, 4)) if error else self.worker_interval()
            db.execute('''UPDATE check24_worker_state SET lease_token='',lease_until=0,next_poll_at=?,
                last_success_at=?,last_error_code=?,last_count=?,failures=? WHERE id=1 AND lease_token=?''',
                (now + delay, row['last_success_at'] if error else now, error, processed, failures, claim['token']))
            if not error:
                db.execute("INSERT INTO check24_sync_state(key,value) VALUES('last_sync',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value RETURNING key", (self.p.now_str(),)).fetchall()
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def status(self):
        db = self.p.get_db()
        try:
            row = dict(db.execute('SELECT * FROM check24_worker_state WHERE id=1').fetchone())
        finally:
            db.close()
        now = self._clock()
        enabled = self.worker_enabled()
        result = {key: row[key] for key in ('last_error_code', 'last_count', 'failures')}
        result.update(enabled=enabled, configured=bool(self.p.get_werkstatt_imap_config().get('configured')),
            busy=bool(row['lease_token'] and row['lease_until'] > now),
            worker_alive=bool(enabled and self._worker_pid == os.getpid() and self._worker
                and self._worker.is_alive() and not self._worker_stop.is_set()),
            heartbeat_stale=bool(not row['heartbeat_at'] or now - row['heartbeat_at'] >= 90),
            interval_seconds=self.worker_interval())
        for key in ('heartbeat_at', 'last_started_at', 'last_success_at', 'next_poll_at'):
            result[key] = datetime.fromtimestamp(row[key], timezone.utc).isoformat(timespec='seconds') if row[key] else ''
        for key, target in (('last_success_at', 'last_success_text'), ('next_poll_at', 'next_poll_text'), ('heartbeat_at', 'heartbeat_text')):
            result[target] = datetime.fromtimestamp(row[key], BERLIN).strftime('%d.%m.%Y %H:%M') if row[key] else ''
        return result

    def sync(self, auto_apply=False, *, worker=False):
        """One shared lease coordinates manual, CLI and optional worker scans."""
        config = self.p.get_werkstatt_imap_config()
        claim = self._claim_sync(config, worker)
        if not claim:
            if worker:
                return 0
            raise Check24SyncBusy('Ein CHECK24-Abgleich läuft bereits.')
        try:
            processed = self._scan(config, claim, auto_apply)
            self._finish_sync(claim, processed)
            return processed
        except Exception as exc:
            self._finish_sync(claim, 0, 'stopped' if isinstance(exc, Check24SyncStopped) else 'sync_failed')
            raise

    def _scan(self, cfg, claim, auto_apply):
        """Bounded read-only scan, including read mails. Never STORE/MOVE/EXPUNGE."""
        deadline = self._monotonic() + SYNC_BUDGET_SECONDS
        cfg = dict(cfg)
        cfg['timeout'] = min(20, max(1, int(cfg.get('timeout', 20))))
        box = Mailbox(lambda: cfg)
        folder = cfg.get('folder') or 'INBOX'
        processed = 0
        with box.connect() as client:
            validity = box.select(client, folder, readonly=True)
            if not re.fullmatch(r'[1-9][0-9]*', validity):
                raise ValueError('UIDVALIDITY des Postfachs fehlt; sicherer Abgleich nicht möglich.')
            since_day = (datetime.now(BERLIN) - timedelta(days=45)).date()
            month = ('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')[since_day.month-1]
            since = f'{since_day.day:02d}-{month}-{since_day.year}'
            status, rows = client.uid('SEARCH', None, 'SINCE', since, 'FROM', quote('montagepartner@check24.de'))
            if status != 'OK':
                raise ValueError('CHECK24-Nachrichten konnten nicht gesucht werden.')
            ids = (rows[0].split() if rows and rows[0] else [])
            account = hashlib.sha256((cfg['host'].lower() + '/' + cfg['user'].lower() + '/' + folder).encode()).hexdigest()
            # Oldest-first batches do not starve unread or older unprocessed mail.
            db = self.p.get_db()
            try:
                known = {r['source_key'] for r in db.execute('SELECT source_key FROM check24_receipts').fetchall()}
            finally:
                db.close()
            for uid in ids:
                if self._monotonic() >= deadline:
                    break
                key = account + '/' + validity + '/' + uid.decode('ascii')
                if key in known:
                    continue
                self._renew_sync(claim)
                try:
                    raw, _ = box.raw(client, uid.decode('ascii'))
                    self.ingest(raw, key, verified=ionos_verified(raw, cfg), auto_apply=auto_apply, _sync_claim=claim)
                except ValueError:
                    # A malformed/oversize message must be visible, but must not
                    # starve all later bookings on every scheduled run.
                    data = {'subject': 'CHECK24-Nachricht nicht lesbar', 'provider_id': '',
                            'customer_name': '', 'start_at': '', 'end_at': '', 'service': '',
                            'tires': [], 'valid': False, 'errors': ['Originalnachricht im Postfach prüfen. Automatische Übernahme nicht möglich.']}
                    digest = hashlib.sha256(('failed:' + key).encode()).hexdigest()
                    db = self.p.get_db()
                    try:
                        self._guard_sync(db, claim)
                        db.execute('''INSERT INTO check24_events(raw_hash,source_key,payload,created_at)
                            VALUES(?,?,?,?) ON CONFLICT(raw_hash) DO NOTHING''', (digest, key, json.dumps(data), self.p.now_str()))
                        db.execute('INSERT INTO check24_receipts(source_key,raw_hash) VALUES(?,?) ON CONFLICT(source_key) DO NOTHING RETURNING source_key', (key, digest)).fetchall()
                        db.commit()
                    except Exception:
                        db.rollback()
                        raise
                    finally:
                        db.close()
                processed += 1
                if processed >= SYNC_BATCH_LIMIT:
                    break
        return processed


def start_check24_worker(service):
    """Optional web-process thread. Registration alone never starts a scanner."""
    portal = service.p
    if portal.app.config.get('TESTING') or getattr(portal, 'PUBLIC_SITE_ONLY', False) or not service.worker_enabled():
        return False
    with service._worker_lock:
        if service._worker_pid == os.getpid() and service._worker and service._worker.is_alive():
            return False
        stop = threading.Event()
        service._worker_stop = stop
        def run():
            # Do not perform network I/O synchronously during app initialization.
            while not stop.wait(15):
                try:
                    with portal.app.app_context():
                        if service.worker_enabled():
                            service.sync(auto_apply=True, worker=True)
                except Exception:
                    portal.app.logger.warning('CHECK24-Abruf ausstehend; Prüfbereich und Verbindung kontrollieren.')
        service._worker_pid = os.getpid()
        service._worker = threading.Thread(target=run, name='werkstatt-check24', daemon=True)
        service._worker.start()
        return True


def register_check24(portal):
    service = Check24Bookings(portal)
    bp = Blueprint('check24', __name__)

    @bp.get('/admin/check24')
    @portal.admin_required
    def index():
        page = max(1, request.args.get('seite', 1, type=int))
        events = service.rows(page)
        response = make_response(render_template('werkstatt_check24.html', events=events, page=page,
            sync_status=service.status(),
            imap_configured=bool(portal.get_werkstatt_imap_config().get('configured')),
            check24_demo=bool(portal.app.config.get('CHECK24_DEMO'))))
        response.headers['Cache-Control'] = 'no-store'
        return response

    @bp.post('/admin/check24/import')
    @portal.admin_required
    def import_mail():
        file = request.files.get('mail')
        if not file or not file.filename.lower().endswith('.eml'):
            flash('Bitte die CHECK24-Originalmail als EML auswählen.', 'warning')
        else:
            try:
                service.ingest(file.read(MAX_MAIL_BYTES + 1))
                flash('Nachricht eingelesen. Daten prüfen und danach übernehmen.', 'success')
            except ValueError as exc:
                flash(str(exc), 'warning')
        return redirect(url_for('check24.index'))

    @bp.post('/admin/check24/sync')
    @portal.admin_required
    def sync():
        try:
            count = service.sync(auto_apply=True)
            flash(f'{count} neue Nachricht(en) geprüft. Eindeutige Buchungen wurden eingeplant; offene Prüfungen stehen unten.', 'success')
        except Check24SyncBusy:
            flash('Ein CHECK24-Abgleich läuft bereits. Bitte den laufenden Durchlauf abwarten.', 'info')
        except Exception:
            portal.app.logger.warning('CHECK24-Abruf fehlgeschlagen; Prüfbereich und Verbindung kontrollieren.')
            flash('Postfachabgleich fehlgeschlagen. Verbindung prüfen; bestehende Termine bleiben erhalten.', 'warning')
        return redirect(url_for('check24.index'))

    @bp.post('/admin/check24/<int:event_id>/uebernehmen')
    @portal.admin_required
    def apply(event_id):
        if request.form.get('reviewed') != '1':
            abort(400, description='Bitte den Abgleich mit CHECK24 bestätigen.')
        try:
            order_id = service.apply(event_id)
        except ValueError as exc:
            flash(str(exc), 'warning')
            return redirect(url_for('check24.index'))
        portal.schedule_change_backup('check24-buchung')
        return redirect(url_for('auftrag_detail', auftrag_id=order_id))

    @bp.post('/admin/check24/<int:event_id>/erledigt')
    @portal.admin_required
    def resolve(event_id):
        note = request.form.get('note', '').strip()
        if not note or len(note) > 1000:
            abort(400)
        db = portal.get_db()
        try:
            service.lock(db)
            db.execute("UPDATE check24_events SET state='resolved',note=? WHERE id=? AND state='review'", (note, event_id))
            db.commit()
        finally:
            db.close()
        flash('Prüfung dokumentiert. Bestehende Aufträge und Termine bleiben unverändert.', 'success')
        return redirect(url_for('check24.index'))

    @bp.post('/admin/check24/<int:event_id>/storno')
    @portal.admin_required
    def cancel(event_id):
        note = request.form.get('note', '').strip()
        if request.form.get('reviewed') != '1' or not note or len(note) > 1000:
            abort(400)
        try:
            service.cancel(event_id, note)
            portal.schedule_change_backup('check24-storno')
            flash('Storno dokumentiert. Auftrag archiviert und Werkstattzeit freigegeben.', 'success')
        except ValueError as exc:
            flash(str(exc), 'warning')
        return redirect(url_for('check24.index'))

    @bp.post('/admin/check24/<int:event_id>/verschieben')
    @portal.admin_required
    def reschedule(event_id):
        note = request.form.get('note', '').strip()
        if request.form.get('reviewed') != '1' or not note or len(note) > 1000:
            abort(400)
        try:
            service.reschedule(event_id, request.form.get('day', ''), request.form.get('begin', ''), request.form.get('finish', ''), note)
            portal.schedule_change_backup('check24-termin')
            flash('Werkstatttermin und Zeitreservierung gemeinsam geändert.', 'success')
        except ValueError as exc:
            flash(str(exc), 'warning')
        return redirect(url_for('check24.index'))

    portal.app.register_blueprint(bp)
    return service
