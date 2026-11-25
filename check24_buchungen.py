"""CHECK24 mail inbox and idempotent workshop import (no provider-side writes)."""
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from email.utils import getaddresses, parsedate_to_datetime
import hashlib
import imaplib
import json
import os
import re
import threading
import time
import uuid

from flask import Blueprint, abort, flash, make_response, redirect, render_template, request, url_for
from check24_mail import BERLIN, MAX_MAIL_BYTES, PARSER_VERSION, parse_check24_mail
from mailbox_client import Mailbox, quote

SYNC_LEASE_SECONDS = 300
SYNC_BUDGET_SECONDS = 60
SYNC_BATCH_LIMIT = 50


class Check24SyncStopped(RuntimeError):
    """Safe interruption: stale lease, revoked worker or changed mailbox."""


class Check24SyncBusy(RuntimeError):
    """Another process is already scanning this mailbox."""


def _needs_parser_refresh(data, attempted_version=0):
    """Only unresolved invalid new/unknown templates may use a newer parser."""
    return (data.get('kind') in {'new', 'unknown'} and not data.get('valid')
            and max(data.get('parser_version', 0), attempted_version) < PARSER_VERSION)


def _receiver_arrival(received):
    """Timestamp at the top receiving server. Never trust a forwarded lower header."""
    try:
        stamp = parsedate_to_datetime(str(received[0]).rsplit(';', 1)[1])
        return stamp if stamp.tzinfo is not None else None
    except (ValueError, TypeError, IndexError):
        return None


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
        arrival = _receiver_arrival(received)
        sent = parsedate_to_datetime(str(msg.get('Date', '')))
        if arrival is None or sent.tzinfo is None or abs(arrival - sent) > timedelta(days=1):
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
            db.execute("""INSERT INTO check24_sync_state(key,value) VALUES('whatsapp_enabled_from',?)
                          ON CONFLICT(key) DO NOTHING RETURNING key""", (str(self._clock()),)).fetchall()
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
        day = start.date().isoformat()
        for hour in self.p.reservierte_reifenstunden(db, day):
            existing = datetime.fromisoformat(day + 'T' + hour).replace(tzinfo=BERLIN)
            if existing < end and existing + timedelta(hours=1) > start:
                return True
        return False

    def _manual_orders(self, db, provider_id):
        """Only exact, explicitly labelled CHECK24 references identify a manual order."""
        if not re.fullmatch(r'[A-Z0-9]+', provider_id or ''):
            return []
        contains = '%' + provider_id + '%'
        rows = db.execute("""SELECT id,auftragsnummer,beschreibung,analyse_text FROM auftraege
            WHERE quelle='intern' AND leistungsart='reifenservice' AND
            (UPPER(TRIM(auftragsnummer))=? OR UPPER(beschreibung) LIKE ? OR UPPER(analyse_text) LIKE ?)
            ORDER BY id""", (provider_id, contains, contains)).fetchall()
        reference = re.compile(r'\bCHECK24[\s:·-]*(?:(?:Bestellung|Bestellnummer|Bestellnr\.?)'
                               r'[\s:#-]+)?' + re.escape(provider_id) + r'(?![A-Z0-9])', re.I)
        return [r['id'] for r in rows if (r['auftragsnummer'] or '').strip().upper() == provider_id
                or any(reference.search(r[key] or '') for key in ('beschreibung', 'analyse_text'))]

    def _attach_manual(self, db, order_id, data, start, intake):
        db.execute('UPDATE auftraege SET status=status WHERE id=?', (order_id,))
        order = dict(db.execute('SELECT * FROM auftraege WHERE id=?', (order_id,)).fetchone() or {})
        if not order or self._manual_orders(db, data['provider_id']) != [order_id]:
            raise ValueError('Die Bestellnummer ist keinem eindeutigen manuellen Reifenauftrag zugeordnet.')
        if (order['archiviert'] or int(order['status'] or 0) not in (1, 2)
                or self.p.reifen_rechnung_gesperrt(order)):
            raise ValueError('Nur offene Reifenaufträge vor Arbeitsbeginn und Abrechnung können verknüpft werden.')
        if (' '.join((order['kunde_name'] or '').split()).casefold() != ' '.join(data['customer_name'].split()).casefold()
                or self.p.parse_date(order['annahme_datum']) != start.date()
                or order['annahme_uhrzeit'] != start.strftime('%H:%M')):
            raise ValueError('Kundenname oder Annahmetermin im Auftrag weicht von der Buchung ab.')
        if (order['kontakt_telefon'] and data['customer_phone']
                and self.p.whatsapp_number_key(order['kontakt_telefon']) != self.p.whatsapp_number_key(data['customer_phone'])):
            raise ValueError('Die Telefonnummer im Auftrag weicht von der Buchung ab.')
        if (order['kunde_email'] and data['customer_email']
                and order['kunde_email'].strip().casefold() != data['customer_email'].strip().casefold()):
            raise ValueError('Die E-Mail-Adresse im Auftrag weicht von der Buchung ab.')
        if db.execute('SELECT provider_id FROM check24_bookings WHERE order_id=?', (order_id,)).fetchone():
            raise ValueError('Dieser Auftrag ist bereits einer anderen Bestellung zugeordnet.')
        if not data.get('total_gross'):
            raise ValueError('Für die Verknüpfung fehlt ein eindeutiger CHECK24-Bruttobetrag.')
        basis = self.p.berechne_check24_reifenservice(data['provider_id'], data['service'], data['total_gross'])
        draft = self.p.reifen_rechnungsentwurf(order)['kalkulation']
        if draft and draft['gesamt']['brutto_cent'] != basis['gesamt']['brutto_cent']:
            raise ValueError('Der gespeicherte Reifenrechnungsentwurf weicht vom CHECK24-Bruttobetrag ab. '
                             'Bitte den Entwurf vor der Verknüpfung prüfen.')
        existing = json.loads(order['schaden_aufnahme_json'] or '{}')
        if not isinstance(existing, dict) or any(
                existing.get(k) and existing[k] != v for k, v in intake.items() if k.startswith('check24_')):
            raise ValueError('Die gespeicherten Buchungsdaten im Auftrag weichen ab.')
        for key, value in intake.items():
            if key.startswith('check24_') or key not in existing:
                existing[key] = value
        db.execute("""UPDATE auftraege SET quelle='check24',auftragsnummer=?,schaden_aufnahme_json=?,
            status=2,geaendert_am=? WHERE id=?""", (order['auftragsnummer'] or data['provider_id'], json.dumps(existing, ensure_ascii=False),
                                                     self.p.now_str(), order_id))
        if int(order['status']) == 1:
            db.execute('INSERT INTO status_log(auftrag_id,status,zeitstempel) VALUES(?,2,?)', (order_id, self.p.now_str()))

    def _apply(self, db, event, *, existing_order_id=None, verified_data=None, confirmed_interval=None):
        data = verified_data if verified_data is not None else json.loads(event['payload'])
        if not data.get('valid') or data.get('kind') != 'new':
            raise ValueError('Diese Nachricht muss anhand des CHECK24-Portals manuell geklärt werden.')
        old = db.execute('SELECT order_id FROM check24_bookings WHERE provider_id=?', (data['provider_id'],)).fetchone()
        if old:
            raise ValueError('Bestellnummer bereits übernommen. Abweichende Nachricht bitte mit dem vorhandenen Auftrag vergleichen.')
        if existing_order_id is None and self._manual_orders(db, data['provider_id']):
            raise ValueError('Die CHECK24-Bestellnummer steht bereits in einem manuellen Reifenauftrag. Zuordnung prüfen.')
        original_interval = datetime.fromisoformat(data['start_at']), datetime.fromisoformat(data['end_at'])
        start, end = confirmed_interval or original_interval
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
                  'check24_bestellnummer': data['provider_id'], 'check24_start': start.isoformat(),
                  'check24_ende': end.isoformat(), 'check24_preis_brutto': data['total_gross']}
        if existing_order_id is None:
            order_id = self.p.create_auftrag('check24', kunde_name=data['customer_name'],
                kunde_email=data['customer_email'], kontakt_telefon=data['customer_phone'],
                fahrzeug='Reifenservice · CHECK24', kennzeichen=data.get('vehicle_plate', ''), auftragsnummer=data['provider_id'],
                schadenart='Reifenservice', leistungsart='reifenservice', beschreibung=description,
                analyse=description, annahme_datum=start.strftime('%d.%m.%Y'),
                schaden_aufnahme_json=json.dumps(intake, ensure_ascii=False), _db=db)
            db.execute('UPDATE auftraege SET status=2,annahme_uhrzeit=? WHERE id=?', (start.strftime('%H:%M'), order_id))
            db.execute('INSERT INTO status_log(auftrag_id,status,zeitstempel) VALUES(?,2,?)', (order_id, self.p.now_str()))
            note = 'Als Auftrag ' + str(order_id) + ' übernommen.'
        else:
            order_id = existing_order_id
            intake['check24_leistung'] = data['service']
            self._attach_manual(db, order_id, data, start, intake)
            note = ((event['note'] or '') + f'\nNach CHECK24-Abgleich mit Auftrag {order_id} verknüpft und Montagezeit reserviert.').strip()
            if (start, end) != original_interval:
                self.reopen_print(db, order_id)
                note += (f' Ursprünglicher Montagetermin laut CHECK24-Mail {original_interval[0]:%d.%m.%Y %H:%M}–{original_interval[1]:%H:%M} Uhr. '
                         f'Im CHECK24-Portal bestätigter und reservierter Termin {start:%d.%m.%Y %H:%M}–{end:%H:%M} Uhr.')
        db.execute('''INSERT INTO check24_bookings(provider_id,order_id,start_at,end_at,event_id)
            VALUES(?,?,?,?,?) RETURNING provider_id''', (data['provider_id'], order_id, start.isoformat(), end.isoformat(), event['id'])).fetchall()
        db.execute("UPDATE check24_events SET state='applied',note=?,payload=? WHERE id=?",
                   (note, json.dumps(data, ensure_ascii=False), event['id']))
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
            inserted = db.execute('''INSERT INTO check24_events(raw_hash,source_key,provider_id,payload,created_at,verified)
                VALUES(?,?,?,?,?,?) ON CONFLICT(raw_hash) DO NOTHING RETURNING id''',
                (data['raw_sha256'], source_key, data['provider_id'], json.dumps(data, ensure_ascii=False),
                 self.p.now_str(), int(verified))).fetchone()
            event = db.execute('SELECT * FROM check24_events WHERE raw_hash=?', (data['raw_sha256'],)).fetchone()
            previous_data = json.loads(event['payload'])
            attempt_key = 'parser_attempt:' + event['raw_hash']
            attempt = db.execute('SELECT value FROM check24_sync_state WHERE key=?', (attempt_key,)).fetchone()
            if (_sync_claim and event['state'] == 'review'
                    and _needs_parser_refresh(previous_data, int(attempt['value']) if attempt else 0)):
                # Re-read the identical original through the configured mailbox;
                # never upgrade uploads, failed receiver authentication or a
                # previously applied/resolved/valid event. raw_hash is immutable.
                if verified:
                    db.execute('''UPDATE check24_events SET payload=?,provider_id=?,verified=1,note=''
                        WHERE id=?''', (json.dumps(data, ensure_ascii=False), data['provider_id'], event['id']))
                else:
                    db.execute('UPDATE check24_events SET verified=0 WHERE id=?', (event['id'],))
                    # A failed authentication is not a successful reparse, but
                    # retrying it every batch could starve newer bookings.
                    db.execute('''INSERT INTO check24_sync_state(key,value) VALUES(?,?)
                        ON CONFLICT(key) DO UPDATE SET value=excluded.value RETURNING key''',
                        (attempt_key, str(PARSER_VERSION))).fetchall()
                event = db.execute('SELECT * FROM check24_events WHERE id=?', (event['id'],)).fetchone()
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
                    order_id = self._apply(db, event)
                except ValueError as exc:
                    db.execute('UPDATE check24_events SET note=? WHERE id=?', (str(exc), event['id']))
                else:
                    if _sync_claim and inserted and verified and not self.p.app.config.get('CHECK24_DEMO'):
                        self._queue_new_booking_notice(db, data, order_id)
            db.commit()
            return event['id']
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _queue_new_booking_notice(self, db, data, order_id):
        # The trusted received date prevents a first sync of the 45-day
        # backlog from notifying the station about pre-rollout bookings.
        arrived = _receiver_arrival((data.get('authentication') or {}).get('received') or [])
        if arrived is None:
            return
        enabled = db.execute("SELECT value FROM check24_sync_state WHERE key='whatsapp_enabled_from'").fetchone()
        if not enabled or not float(enabled['value']) < arrived.timestamp() <= self._clock():
            return
        from workshop_booking_whatsapp import enqueue
        start, end = datetime.fromisoformat(data['start_at']), datetime.fromisoformat(data['end_at'])
        enqueue(db, event_key='check24-booking:' + data['provider_id'], reference=f'C24-{order_id}',
                title='CHECK24-Reifentermin bestätigt',
                message=(f"{data['customer_name']} · {data['service']}\n"
                         f"Termin {start:%d.%m.%Y, %H:%M} bis {end:%H:%M} Uhr"),
                path=f'/admin/auftrag/{order_id}')

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

    def _read_booking_original(self, event, receipts):
        """Recover the same bytes from their recorded UID without trusting an EML upload."""
        cfg = self.p.get_werkstatt_imap_config()
        box = Mailbox(lambda: cfg)
        with box.connect() as client:
            for item in box.folders(client):
                folder = item['id']
                account = hashlib.sha256((cfg['host'].lower() + '/' + cfg['user'].lower() + '/' + folder).encode()).hexdigest()
                keys = [key.split('/') for key in receipts if re.fullmatch(account + r'/[1-9][0-9]*/[1-9][0-9]*', key)]
                if not keys:
                    continue
                validity = box.select(client, folder, readonly=True)
                for _, version, uid in keys:
                    if version != validity:
                        continue
                    try:
                        raw, _ = box.raw(client, uid)
                    except ValueError:
                        continue
                    if hashlib.sha256(raw).hexdigest() == event['raw_hash'] and ionos_verified(raw, cfg):
                        return parse_check24_mail(raw)
        raise ValueError('Verifizierte Originalmail nicht wiedergefunden. CHECK24-Postfach erneut abgleichen und Zuordnung wiederholen.')

    def link_existing(self, event_id, order_id, *, day='', begin='', finish=''):
        if any((day, begin, finish)) and not all((day, begin, finish)):
            raise ValueError('Für den abweichenden Montagetermin bitte Datum, Beginn und Ende zusammen eintragen.')
        confirmed_interval = self.reschedule_interval(day, begin, finish) if day else None
        db = self.p.get_db()
        try:
            event = db.execute('SELECT * FROM check24_events WHERE id=?', (event_id,)).fetchone()
            if not event or not event['verified']:
                raise ValueError('Für die Verknüpfung ist eine verifizierte CHECK24-Postfachnachricht erforderlich.')
            data = json.loads(event['payload'])
            if data.get('kind') != 'new' or event['state'] not in ('review', 'resolved', 'applied'):
                raise ValueError('Nur eine ursprüngliche Terminbuchung kann verknüpft werden.')
            receipts = [r['source_key'] for r in db.execute(
                'SELECT source_key FROM check24_receipts WHERE raw_hash=?', (event['raw_hash'],)).fetchall()]
        finally:
            db.close()
        if not data.get('valid'):
            data = self._read_booking_original(event, receipts)
        db = self.p.get_db()
        try:
            self.lock(db)
            event = db.execute('SELECT * FROM check24_events WHERE id=?', (event_id,)).fetchone()
            if (not event or not event['verified'] or data.get('provider_id') != event['provider_id']
                    or data.get('raw_sha256') != event['raw_hash']):
                raise ValueError('Die Originalnachricht passt nicht zur gespeicherten Buchung.')
            if event['state'] == 'applied':
                linked = db.execute('SELECT order_id,start_at,end_at FROM check24_bookings WHERE event_id=? AND active=1',
                                    (event_id,)).fetchone()
                if linked and linked['order_id'] == order_id:
                    if confirmed_interval and confirmed_interval != (
                            datetime.fromisoformat(linked['start_at']), datetime.fromisoformat(linked['end_at'])):
                        raise ValueError('Der Auftrag ist bereits für eine andere Montagezeit verknüpft. '
                                         'Bitte die bestätigte Terminänderung am verknüpften Auftrag übernehmen.')
                    return order_id
            if event['state'] not in ('review', 'resolved'):
                raise ValueError('Diese Nachricht ist bereits bearbeitet.')
            history = db.execute('SELECT id,verified,payload,state,note FROM check24_events WHERE provider_id=?',
                                 (event['provider_id'],)).fetchall()
            # Old manual resolutions have no structured outcome. Accept only a positive
            # cancellation statement about this reference, never a name or "nicht storniert".
            pid = re.escape(event['provider_id'])
            label = r'(?:(?:CHECK24|Bestellung|Bestellnummer)[\s:#-]+)*'
            cancelled_note = re.compile(
                r'(?:^|[.!;\n]\s*)\s*(?:' + label + pid + r'\s+(?:(?:wurde|ist)\s+)?(?:storniert|abgesagt)\b'
                r'|(?:Storno|Stornierung|Absage)\s+(?:für\s+)?' + label + pid + r'\b)', re.I)
            for entry in history:
                previous = json.loads(entry['payload'])
                documented_cancel = entry['state'] == 'resolved' and cancelled_note.search(entry['note'] or '')
                later_change = entry['id'] > event_id and entry['verified'] and previous.get('kind') != 'new'
                if documented_cancel or later_change:
                    raise ValueError('Zu dieser Bestellnummer liegt ein Stornovermerk oder eine weitere '
                                     'Änderungsnachricht vor. Den aktuellen Termin bitte im Büro klären.')
            result = self._apply(db, event, existing_order_id=order_id, verified_data=data,
                                 confirmed_interval=confirmed_interval)
            db.commit()
            return result
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def rows(self, page=1, *, manual_candidates=False):
        db = self.p.get_db()
        try:
            rows = db.execute('''SELECT e.*,b.order_id,b.start_at AS current_start,b.end_at AS current_end FROM check24_events e LEFT JOIN check24_bookings b
                ON b.provider_id=e.provider_id ORDER BY CASE WHEN e.state='review' THEN 0 ELSE 1 END,e.id DESC
                LIMIT 50 OFFSET ?''', ((max(1, page)-1)*50,)).fetchall()
            result = [dict(row) | {'data': json.loads(row['payload'])} for row in rows]
            if manual_candidates:
                for item in result:
                    if (item['state'] in ('review', 'resolved') and not item['order_id'] and item['verified']
                            and item['data'].get('kind') == 'new'):
                        matches = self._manual_orders(db, item['provider_id'])
                        item['manual_order_ids'] = matches
            return result
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
            if not order or int(order['status'] or 0) >= 3 or order['archiviert'] or self.p.reifen_rechnung_gesperrt(dict(order)):
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

    @staticmethod
    def reopen_print(db, order_id):
        # Bump even if already pending: an older PDF may still be rendering.
        db.execute("""UPDATE auftraege SET reifen_auftrag_gedruckt_am='',
            reifen_termin_revision=reifen_termin_revision+1 WHERE id=?
            AND COALESCE(archiviert,0)=0 AND COALESCE(angebotsphase,0)=0""", (order_id,))

    def reopen_for_office_dates(self, db, order, dates):
        """Compare office-supplied dates while the caller holds the order row lock."""
        if not self.p.auftrag_ist_reifen(order):
            return
        for field, value in dates.items():
            if order['quelle'] == 'check24' and field in ('start_datum', 'fertig_datum'):
                # The linked CHECK24 interval supplies both printed work times.
                continue
            normalize = self.p.format_time_value if field.endswith('_uhrzeit') else self.p.format_date
            if normalize(order[field]) != normalize(value):
                break
        else:
            return
        if self.is_check24_order(db, order):
            self.reopen_print(db, order['id'])

    def is_check24_order(self, db, order):
        if order['quelle'] == 'check24' or self.p.parse_schadenaufnahme_json(
                order.get('schaden_aufnahme_json')).get('check24_bestellnummer'):
            return True
        if order['quelle'] != 'intern':
            return False
        # Manual jobs often retain their own internal order number. An expressly
        # labelled reference identifies the job even before the original mail arrives.
        reference = (r'\bCHECK24[\s:·-]*(?:(?:Bestellung|Bestellnummer|Bestellnr\.?)[\s:#-]+'
                     r'|[\s:·-]+(?=[A-Z0-9]{0,19}[0-9]))[A-Z0-9]{4,20}\b')
        texts = (order.get('beschreibung') or '', order.get('analyse_text') or '')
        if any(re.search(reference, text, re.I) for text in texts):
            return True
        # A bare order number or an ambiguous word such as "CHECK24 Montage"
        # is conclusive only when the same reference was actually received.
        candidates = {(order.get('auftragsnummer') or '').strip().upper()}
        for text in texts:
            candidates.update(re.findall(r'\bCHECK24[\s:·-]+([A-Z0-9]{4,20})\b', text.upper()))
        codes = [code for code in candidates if re.fullmatch(r'[A-Z0-9]{4,20}', code)]
        return bool(codes and db.execute(
            'SELECT 1 FROM check24_events WHERE provider_id IN (' + ','.join('?' for _ in codes) + ') LIMIT 1',
            tuple(codes)).fetchone())

    @staticmethod
    def reschedule_interval(day, begin, finish):
        from check24_mail import _local_datetime
        from datetime import date
        try:
            if (not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', day)
                    or not all(re.fullmatch(r'[0-9]{2}:[0-9]{2}', clock) for clock in (begin, finish))):
                raise ValueError
            start = _local_datetime(date.fromisoformat(day), begin)
            end = _local_datetime(date.fromisoformat(day), finish)
        except (ValueError, TypeError):
            raise ValueError('Bitte gültiges Datum und eindeutige Uhrzeiten eintragen.')
        if start <= datetime.now(BERLIN) or start >= end or start.weekday() == 6 or start.hour < 7 or end.hour > 18 or (end.hour == 18 and end.minute):
            raise ValueError('Bitte ein zukünftiges Zeitintervall innerhalb der Öffnungszeiten auswählen.')
        return start, end

    def reschedule(self, event_id, day, begin, finish, note):
        start, end = self.reschedule_interval(day, begin, finish)
        db = self.p.get_db()
        try:
            self.lock(db)
            event = db.execute('SELECT * FROM check24_events WHERE id=?', (event_id,)).fetchone()
            booking = db.execute('SELECT * FROM check24_bookings WHERE provider_id=?', (event['provider_id'],)).fetchone() if event else None
            if not booking or not booking['active']:
                raise ValueError('Keine aktive Buchung gefunden.')
            db.execute('UPDATE auftraege SET status=status WHERE id=?', (booking['order_id'],))
            order = db.execute('SELECT * FROM auftraege WHERE id=?', (booking['order_id'],)).fetchone()
            if (not order or order['archiviert'] or int(order['status'] or 0) >= 3
                    or self.p.reifen_rechnung_gesperrt(dict(order))):
                raise ValueError('Auftrag archiviert, bereits in Arbeit oder abgerechnet. Änderung zuerst im Büro klären.')
            same_slot = (start == datetime.fromisoformat(booking['start_at'])
                         and end == datetime.fromisoformat(booking['end_at']))
            confirmed = start.strftime('%d.%m.%Y %H:%M') + '–' + end.strftime('%H:%M')
            db.execute("UPDATE check24_events SET state='resolved',note=? WHERE id=?",
                       ('Termin nach CHECK24-Abgleich bestätigt: ' + confirmed + '. ' + note, event_id))
            if same_slot:
                db.commit()
                return
            if self._conflict(db, start, end, booking['provider_id']):
                raise ValueError('Die neue Zeit überschneidet sich mit einer vorhandenen Buchung.')
            intake = json.loads(order['schaden_aufnahme_json'] or '{}')
            intake.update(check24_start=start.isoformat(), check24_ende=end.isoformat())
            db.execute('UPDATE check24_bookings SET start_at=?,end_at=? WHERE provider_id=?', (start.isoformat(), end.isoformat(), booking['provider_id']))
            self.reopen_print(db, booking['order_id'])
            description = re.sub(r'(?m)^Termin: .+$', 'Termin: ' + confirmed + ' Uhr', order['beschreibung'] or '')
            analysis = re.sub(r'(?m)^Termin: .+$', 'Termin: ' + confirmed + ' Uhr', order['analyse_text'] or '')
            db.execute('''UPDATE auftraege SET annahme_datum=?,annahme_uhrzeit=?,schaden_aufnahme_json=?,
                beschreibung=?,analyse_text=?,geaendert_am=? WHERE id=?''',
                (start.strftime('%d.%m.%Y'), start.strftime('%H:%M'), json.dumps(intake, ensure_ascii=False), description, analysis, self.p.now_str(), booking['order_id']))
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def arrival(self, order_id, day, at, note, disposal=None):
        """Record an earlier drop-off without moving the booked workshop slot."""
        from check24_mail import _local_datetime
        from datetime import date
        try:
            arrival = _local_datetime(date.fromisoformat(day), at)
        except (ValueError, TypeError):
            raise ValueError('Bitte ein gültiges Anlieferungsdatum und eine Uhrzeit angeben.')
        if disposal is not None and (not isinstance(disposal, str) or disposal not in ('0','1','2','3','4')):
            raise ValueError('Bitte null bis vier Altreifen zur Entsorgung angeben.')
        db = self.p.get_db()
        try:
            self.lock(db)
            booking = db.execute('SELECT * FROM check24_bookings WHERE order_id=? AND active=1', (order_id,)).fetchone()
            if not booking:
                raise ValueError('Keine aktive CHECK24-Buchung gefunden.')
            start = datetime.fromisoformat(booking['start_at'])
            if arrival.date() != start.date() or arrival.hour < 7 or arrival > start or arrival <= datetime.now(BERLIN):
                raise ValueError('Die Anlieferung muss am Montagetermin zwischen 07:00 Uhr und Montagebeginn liegen. Den Montagetermin selbst bitte im CHECK24-Bereich ändern.')
            db.execute('UPDATE auftraege SET status=status WHERE id=?', (order_id,))
            order = db.execute('SELECT * FROM auftraege WHERE id=?', (order_id,)).fetchone()
            if not order or int(order['status'] or 0) >= 3 or order['lexware_invoice_id'] or order['rechnung_nummer']:
                raise ValueError('Auftrag bereits in Arbeit oder abgerechnet. Anlieferung zuerst im Büro klären.')
            intake = json.loads(order['schaden_aufnahme_json'] or '{}')
            if disposal is not None:
                intake['check24_entsorgung_menge'] = int(disposal)
            if (self.p.parse_date(order['annahme_datum']) != arrival.date()
                    or self.p.format_time_value(order['annahme_uhrzeit']) != arrival.strftime('%H:%M')):
                self.reopen_print(db, order_id)
            db.execute('UPDATE auftraege SET annahme_datum=?,annahme_uhrzeit=?,notiz_intern=?,schaden_aufnahme_json=?,geaendert_am=? WHERE id=?',
                       (arrival.strftime('%d.%m.%Y'), arrival.strftime('%H:%M'), note, json.dumps(intake,ensure_ascii=False), self.p.now_str(), order_id))
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

    @staticmethod
    def _scan_folders(box, client, configured):
        # IONOS advertises either a top-level folder or a child of INBOX.
        # Restrict discovery to CHECK24 itself; archived/trash copies are not sources.
        available = [item['id'] for item in box.folders(client)]
        roots = list(dict.fromkeys([configured, 'INBOX']))
        result = [name for name in roots if name in available]
        candidates = {'check24'} | {
            root.casefold() + delimiter + 'check24' for root in roots for delimiter in ('.', '/')
        }
        result.extend(name for name in available if name.casefold() in candidates and name not in result)
        if not result:
            raise ValueError('Kein CHECK24-Quellordner im Postfach gefunden.')
        return result

    def _known_sync_receipts(self):
        db = self.p.get_db()
        try:
            known = {r['source_key'] for r in db.execute('SELECT source_key FROM check24_receipts').fetchall()}
            attempts = {r['key']: int(r['value']) for r in db.execute(
                'SELECT key,value FROM check24_sync_state WHERE key LIKE ?', ('parser_attempt:%',)).fetchall()}
            for receipt in db.execute('''SELECT r.source_key,r.raw_hash,e.payload FROM check24_receipts r
                JOIN check24_events e ON e.raw_hash=r.raw_hash WHERE e.state='review' ''').fetchall():
                old_data = json.loads(receipt['payload'])
                if _needs_parser_refresh(old_data, attempts.get('parser_attempt:' + receipt['raw_hash'], 0)):
                    known.discard(receipt['source_key'])
            return known
        finally:
            db.close()

    def _scan(self, cfg, claim, auto_apply):
        """Bounded read-only scan, including read mails. Never STORE/MOVE/EXPUNGE."""
        started = self._monotonic()
        cfg = dict(cfg)
        cfg['timeout'] = min(20, max(1, int(cfg.get('timeout', 20))))
        box = Mailbox(lambda: cfg)
        since_day = (datetime.now(BERLIN) - timedelta(days=45)).date()
        month = ('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')[since_day.month-1]
        since = f'{since_day.day:02d}-{month}-{since_day.year}'
        known = self._known_sync_receipts()
        processed = 0
        with box.connect() as client:
            folders = self._scan_folders(box, client, cfg.get('folder') or 'INBOX')
            for index, folder in enumerate(folders):
                if index and (processed >= SYNC_BATCH_LIMIT or self._monotonic() >= started + SYNC_BUDGET_SECONDS):
                    break
                # Leave capacity for mail filters that deliver straight to a subfolder.
                remaining_folders = len(folders) - index
                limit = (SYNC_BATCH_LIMIT - processed + remaining_folders - 1) // remaining_folders
                deadline = started + SYNC_BUDGET_SECONDS * (index + 1) / len(folders)
                processed += self._scan_folder(box, client, cfg, folder, since, known, limit, deadline, claim, auto_apply)
        return processed

    def _scan_folder(self, box, client, cfg, folder, since, known, limit, deadline, claim, auto_apply):
        processed = 0
        validity = box.select(client, folder, readonly=True)
        if not re.fullmatch(r'[1-9][0-9]*', validity):
            raise ValueError('UIDVALIDITY des Postfachs fehlt; sicherer Abgleich nicht möglich.')
        status, rows = client.uid('SEARCH', None, 'SINCE', since, 'FROM', quote('montagepartner@check24.de'))
        if status != 'OK':
            raise ValueError('CHECK24-Nachrichten konnten nicht gesucht werden.')
        ids = (rows[0].split() if rows and rows[0] else [])
        account = hashlib.sha256((cfg['host'].lower() + '/' + cfg['user'].lower() + '/' + folder).encode()).hexdigest()
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
                        'parser_version': PARSER_VERSION,
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
            if processed >= limit:
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
        events = service.rows(page, manual_candidates=True)
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

    @bp.post('/admin/check24/<int:event_id>/verknuepfen')
    @portal.admin_required
    def link_existing(event_id):
        order_id = request.form.get('order_id', type=int)
        if request.form.get('reviewed') != '1' or not order_id or order_id < 1:
            abort(400)
        try:
            service.link_existing(event_id, order_id, day=request.form.get('day', ''),
                                  begin=request.form.get('begin', ''), finish=request.form.get('finish', ''))
        except ValueError as exc:
            flash(str(exc), 'warning')
        except (OSError, EOFError, imaplib.IMAP4.error):
            flash('Das Postfach konnte nicht geprüft werden. Bitte den Abruf erneut versuchen.', 'warning')
        else:
            portal.schedule_change_backup('check24-verknuepfung')
            flash(f'Auftrag {order_id} mit CHECK24 verknüpft und Montagezeit reserviert.', 'success')
        return redirect(url_for('check24.index'))

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

    @bp.post('/admin/check24/auftrag/<int:order_id>/anlieferung')
    @portal.admin_required
    def arrival(order_id):
        note = request.form.get('note', '').strip()
        if len(note) > 4000:
            abort(400)
        try:
            service.arrival(order_id, request.form.get('day', ''), request.form.get('at', ''), note, request.form.get('disposal'))
            portal.schedule_change_backup('check24-anlieferung')
            flash('Anlieferung und Hinweise gespeichert. Die reservierte Montagezeit bleibt bestehen.', 'success')
        except ValueError as exc:
            flash(str(exc), 'warning')
        return redirect(url_for('admin_reifentermine'))

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
