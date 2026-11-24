"""Transactional operator notices for rentals and tyre/wheel orders."""

import logging
import os
from pathlib import Path
import sqlite3
import threading
import time
from contextlib import closing
from urllib.parse import urlsplit


log = logging.getLogger(__name__)
_thread_lock = threading.Lock()
_thread = None
_pid = None


def init_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS workshop_booking_wa_events (
        event_key TEXT PRIMARY KEY, reference TEXT NOT NULL, title TEXT NOT NULL,
        message TEXT NOT NULL, path TEXT NOT NULL, created_at BIGINT NOT NULL,
        expanded INTEGER NOT NULL DEFAULT 0)""")
    db.execute("""CREATE TABLE IF NOT EXISTS workshop_booking_wa_deliveries (
        event_key TEXT NOT NULL REFERENCES workshop_booking_wa_events(event_key) ON DELETE CASCADE,
        phone TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
        provider_id TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0,
        attempted_at BIGINT NOT NULL DEFAULT 0, retry_at BIGINT NOT NULL DEFAULT 0,
        updated_at BIGINT NOT NULL, detail TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (event_key, phone))""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_booking_wa_events_pending
                  ON workshop_booking_wa_events(expanded,created_at)""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_booking_wa_delivery_due
                  ON workshop_booking_wa_deliveries(state,retry_at)""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_booking_wa_delivery_phone
                  ON workshop_booking_wa_deliveries(phone,attempted_at)""")
    db.execute("""CREATE INDEX IF NOT EXISTS idx_booking_wa_delivery_provider
                  ON workshop_booking_wa_deliveries(provider_id)""")


def enqueue(db, *, event_key, reference, title, message, path):
    """Use the order's own transaction. Replayed transitions keep the original notice."""
    db.execute("""INSERT INTO workshop_booking_wa_events
        (event_key,reference,title,message,path,created_at) VALUES (?,?,?,?,?,?)
        ON CONFLICT(event_key) DO NOTHING RETURNING event_key""",
               (event_key, reference, title, message, path, int(time.time()))).fetchone()


def check_sqlite_replacement(live_path, incoming_path):
    """Reject erasure/rewind or newly actionable notices only for full-file restores."""
    tables = {
        'workshop_booking_wa_events': (
            'event_key,reference,title,message,path,created_at,expanded', 'expanded=0'),
        'workshop_booking_wa_deliveries': (
            'event_key,phone,state,provider_id,attempts,attempted_at,retry_at,updated_at,detail',
            "state IN ('pending','sending')"),
    }
    with closing(sqlite3.connect(':memory:', uri=True)) as db:
        for schema, path in [('currentdb', live_path), ('incoming', incoming_path)]:
            filename = Path(path)
            uri = filename.resolve().as_uri() + '?mode=ro' if filename.is_file() else ':memory:'
            db.execute(f'ATTACH DATABASE ? AS {schema}', (uri,))
        for table, (columns, actionable) in tables.items():
            def select(schema, where=''):
                exists = db.execute(f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?",
                                    (table,)).fetchone()
                if not exists:
                    # Empty relation with the same arity for historical backups.
                    return 'SELECT ' + ','.join('NULL' for _ in columns.split(',')) + ' WHERE 0'
                return f'SELECT {columns} FROM {schema}.{table}' + (f' WHERE {where}' if where else '')

            # Current attempts, including provider receipts, must survive verbatim.
            if db.execute(select('currentdb') + ' EXCEPT ' + select('incoming') + ' LIMIT 1').fetchone():
                raise ValueError('Datenimport gesperrt: Das SQLite-Paket enthält die aktuelle '
                                 'WhatsApp-Versandhistorie der Buchungen nicht vollständig.')
            # Importing a completed archive is safe; introducing a pending old
            # event into an empty (or otherwise different) portal can resend it.
            if db.execute(select('incoming', actionable) + ' EXCEPT ' + select('currentdb', actionable)
                          + ' LIMIT 1').fetchone():
                raise ValueError('Datenimport gesperrt: Das SQLite-Paket würde ausstehende '
                                 'WhatsApp-Buchungshinweise erneut aktivieren.')


def _prepare(db, phones, now, limit):
    # A crash after the initial submission may have left a delivered WhatsApp.
    # Never repeat it automatically; the operator can reconcile the provider log.
    db.execute("""UPDATE workshop_booking_wa_deliveries SET state='review', updated_at=?,
            detail='Versand unterbrochen. Annahme durch WhatsApp prüfen.'
            WHERE state='sending' AND attempted_at < ?""", (now, now - 300))
    events = db.execute("""SELECT event_key FROM workshop_booking_wa_events
        WHERE expanded=0 ORDER BY created_at,event_key LIMIT ?""", (limit,)).fetchall()
    for row in events:
        key = row['event_key']
        claimed = db.execute("""UPDATE workshop_booking_wa_events SET expanded=1
            WHERE event_key=? AND expanded=0 RETURNING event_key""", (key,)).fetchone()
        if claimed:
            for phone in phones:
                db.execute("""INSERT INTO workshop_booking_wa_deliveries(event_key,phone,updated_at)
                    VALUES (?,?,?) ON CONFLICT(event_key,phone) DO NOTHING RETURNING event_key""",
                           (key, phone, now)).fetchone()
    db.commit()


def _claim(db, now):
    claimed = db.execute("""UPDATE workshop_booking_wa_deliveries
        SET state='sending',attempts=attempts+1,attempted_at=?,updated_at=?
        WHERE (event_key,phone) = (
            SELECT event_key,phone FROM workshop_booking_wa_deliveries
            WHERE state='pending' AND retry_at<=? ORDER BY retry_at,updated_at,event_key,phone LIMIT 1)
        AND state='pending' AND retry_at<=? RETURNING event_key,phone,attempts""",
                         (now, now, now, now)).fetchone()
    if not claimed:
        db.commit()
        return None
    event = db.execute("""SELECT reference,title,message,path FROM workshop_booking_wa_events
                          WHERE event_key=?""", (claimed['event_key'],)).fetchone()
    db.commit()
    return {**dict(event), **dict(claimed)}


def _cockpit_link(portal, path):
    base = (portal.PORTAL_BASE_URL or os.environ.get('RENDER_EXTERNAL_URL')
            or 'https://kundenstatus-app.onrender.com').rstrip('/')
    origin = urlsplit(base)
    if origin.scheme not in ('http', 'https') or not origin.hostname:
        return ''
    if not path.startswith('/') or path.startswith('//') or '\\' in path:
        return ''
    return base + path


def _send(portal, notice):
    reference, title, message = notice['reference'], notice['title'], notice['message']
    link = _cockpit_link(portal, notice['path'])
    detail = message[:700]
    if link:
        detail += f'\nIm Cockpit öffnen: {link}'
    phone = notice['phone']
    payloads = []
    if portal.whatsapp_message_template_enabled():
        payloads.append(portal.build_whatsapp_template_payload(
            phone, {'fahrzeug': title}, detail, 'Buchungsportal', reference=reference))
    payloads.append(portal.build_whatsapp_text_payload(phone, f'{title} · {reference}\n{detail}'))
    for payload in payloads:
        ok, provider_id, error, status = portal.post_whatsapp_payload(payload, include_http_status=True)
        if ok and provider_id:
            return 'sent', provider_id, ''
        # Only an explicit client-error response is safe to follow with another
        # submission. Timeouts, 5xx and malformed success responses are uncertain.
        if status is None or status == 408 or (status != 0 and not 400 <= status < 500):
            return 'review', provider_id or '', 'Annahme durch WhatsApp unklar (HTTP %s).' % (status or 'unbekannt')
        if status != 400 or payload['type'] != 'template':
            break
    return 'pending', '', (error or 'Nachricht abgelehnt.')[:400]


def run_cycle(portal, *, limit=12):
    """Deliver only committed events; close DB connections before provider I/O."""
    if portal.whatsapp_bridge_config_errors():
        return 0
    phones = list(dict.fromkeys(portal.whatsapp_payload_phone(p) for p in portal.whatsapp_workshop_numbers() if p))
    if not phones:
        return 0
    with closing(portal.open_fresh_db()) as db:
        _prepare(db, phones, int(time.time()), limit)
    processed = 0
    for _ in range(limit):
        with closing(portal.open_fresh_db()) as db:
            notice = _claim(db, int(time.time()))
        if not notice:
            break
        try:
            # Drop deliveries for numbers explicitly removed from workshop recipients.
            if notice['phone'] not in phones:
                state, provider_id, detail = 'cancelled', '', 'Empfänger aus Werkstattkonfiguration entfernt.'
            else:
                state, provider_id, detail = _send(portal, notice)
        except Exception:
            state, provider_id, detail = 'review', '', 'Versand nicht eindeutig abgeschlossen.'
            log.exception('WhatsApp-Versand für Buchungsereignis unterbrochen.')
        now = int(time.time())
        delay = min(3600, 30 * 2 ** min(notice['attempts'] - 1, 7)) if state == 'pending' else 0
        with closing(portal.open_fresh_db()) as db:
            db.execute("""UPDATE workshop_booking_wa_deliveries
                SET state=?,provider_id=?,detail=?,updated_at=?,retry_at=?
                WHERE event_key=? AND phone=? AND attempts=? AND state IN ('sending','review')""",
                       (state, provider_id, detail, now, now + delay, notice['event_key'],
                        notice['phone'], notice['attempts']))
            db.commit()
        if state in ('review', 'pending'):
            log.warning('WhatsApp-Buchungshinweis %s: %s (Versuch %d).',
                        notice['event_key'], state, notice['attempts'])
        processed += 1
    return processed


def start_worker(portal):
    """Start once per serving process. The caller limits startup to the live portal."""
    global _thread, _pid
    with _thread_lock:
        if _pid == os.getpid() and _thread is not None and _thread.is_alive():
            return

        def poll():
            while True:
                try:
                    run_cycle(portal)
                except Exception:
                    log.exception('WhatsApp-Buchungsversand kann Warteschlange nicht verarbeiten.')
                time.sleep(10)

        _pid = os.getpid()
        _thread = threading.Thread(target=poll, name='booking-whatsapp', daemon=True)
        _thread.start()
