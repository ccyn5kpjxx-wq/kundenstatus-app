"""Transactional test inventory. All amounts are integer EUR cents.

This is deliberately NOT a second production inventory. The production portal's
mietvorgaenge writers must share its locks before live bookings can be enabled.
"""
from contextlib import contextmanager
from datetime import date, datetime
from zoneinfo import ZoneInfo
import hashlib
import json
import secrets
import sqlite3
import time


class BookingError(ValueError):
    pass


class Conflict(BookingError):
    pass


class ProviderUnavailable(RuntimeError):
    pass


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class BookingService:
    def __init__(self, path, gateway, base_url, clock=time.time):
        self.path, self.gateway, self.base_url, self.clock = str(path), gateway, base_url, clock
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS vehicles (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, day_cents INTEGER NOT NULL,
                    long_cents INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1);
                CREATE TABLE IF NOT EXISTS bookings (
                    id TEXT PRIMARY KEY, owner TEXT NOT NULL, request_key TEXT NOT NULL UNIQUE,
                    vehicle_id TEXT NOT NULL REFERENCES vehicles(id), start TEXT NOT NULL,
                    end TEXT NOT NULL, km INTEGER NOT NULL, quote TEXT NOT NULL,
                    quote_hash TEXT NOT NULL, state TEXT NOT NULL, created INTEGER NOT NULL,
                    expires INTEGER NOT NULL, session_id TEXT UNIQUE, checkout_url TEXT,
                    payment_intent TEXT UNIQUE, paid INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT '');
                CREATE INDEX IF NOT EXISTS booking_inventory ON bookings(vehicle_id,state,start,end);
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, kind TEXT NOT NULL);
            ''')

    @contextmanager
    def db(self, write=False):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        try:
            if write:
                db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def seed_demo(self):
        # Synthetic IDs: never imply these are actual portal vehicle IDs.
        with self.db(True) as db:
            db.executemany('INSERT OR IGNORE INTO vehicles VALUES (?,?,?,?,1)', [
                ('test-kona', 'Hyundai KONA N Line X', 5900, 4900),
                ('test-i10', 'Hyundai i10', 3900, 3900),
                ('test-c3', 'Citroën C3', 3900, 3900),
            ])

    def vehicles(self):
        with self.db() as db:
            return [dict(r) for r in db.execute('SELECT * FROM vehicles WHERE active=1 ORDER BY id')]

    def quote(self, vehicle, start, end, km):
        try:
            first, last = date.fromisoformat(start), date.fromisoformat(end)
        except (ValueError, TypeError):
            raise BookingError('Abhol- und Rückgabetag prüfen.') from None
        today = datetime.fromtimestamp(self.clock(), ZoneInfo('Europe/Berlin')).date()
        if first.isoformat() != start or last.isoformat() != end or first < today or last < first:
            raise BookingError('Der Zeitraum ist ungültig oder liegt in der Vergangenheit.')
        days = max(1, (last - first).days)
        if days > 90 or (first - today).days > 366:
            raise BookingError('Testgrenze: höchstens 90 Tage, maximal ein Jahr im Voraus.')
        if type(km) is not int or not 0 <= km <= 100000:
            raise BookingError('Kilometer als ganze Zahl zwischen 0 und 100000 angeben.')
        rate = vehicle['long_cents'] if days >= 3 else vehicle['day_cents']
        return {'vehicle_id': vehicle['id'], 'name': vehicle['name'], 'start': start, 'end': end,
                'days': days, 'day_cents': rate, 'km': km, 'included_km': days * 150,
                'extra_cents': max(0, km - days * 150) * 25,
                'total_cents': days * rate + max(0, km - days * 150) * 25,
                'currency': 'eur', 'tariff_version': 'public-tariff-2026-09-test-only',
                'conditions': 'TEST: Kaution, Versicherung, Übergabezeiten und Storno noch offen.'}

    @staticmethod
    def occupied(db, vehicle, start, end, exclude=''):
        # Keep the portal's conservative inclusive return-day occupancy in this test.
        return db.execute('''SELECT 1 FROM bookings WHERE vehicle_id=? AND id!=?
            AND state!='expired' AND start<=? AND end>=? LIMIT 1''',
            (vehicle, exclude, end, start)).fetchone() is not None

    def preview_quote(self, vehicle_id, start, end, km):
        with self.db() as db:
            vehicle = db.execute('SELECT * FROM vehicles WHERE id=? AND active=1', (vehicle_id,)).fetchone()
            if not vehicle:
                raise BookingError('Kein freigegebenes Testfahrzeug.')
            result = self.quote(vehicle, start, end, km)
            result['quote_hash'] = digest(json.dumps(result, sort_keys=True, ensure_ascii=False))
            result['available_in_test'] = not self.occupied(db, vehicle_id, start, end)
            return result

    def create(self, owner, key, vehicle_id, start, end, km, expected_quote_hash=None):
        if not isinstance(key, str) or not 16 <= len(key) <= 100:
            raise BookingError('Gültiger Idempotenzschlüssel fehlt.')
        identity = digest(owner + ':' + key)
        with self.db(True) as db:
            row = db.execute('SELECT * FROM bookings WHERE request_key=?', (identity,)).fetchone()
            if row:
                if (row['vehicle_id'], row['start'], row['end'], row['km']) != (vehicle_id, start, end, km):
                    raise Conflict('Schlüssel bereits für andere Buchungsdaten benutzt.')
                if expected_quote_hash is not None and expected_quote_hash != row['quote_hash']:
                    raise Conflict('Schlüssel gehört zu einem anderen Preisangebot.')
            else:
                vehicle = db.execute('SELECT * FROM vehicles WHERE id=? AND active=1', (vehicle_id,)).fetchone()
                if not vehicle:
                    raise BookingError('Kein freigegebenes Testfahrzeug.')
                quote = self.quote(vehicle, start, end, km)
                if self.occupied(db, vehicle_id, start, end):
                    raise Conflict('Das Testfahrzeug ist im gewählten Zeitraum belegt.')
                quote_json = json.dumps(quote, sort_keys=True, ensure_ascii=False)
                if expected_quote_hash is not None and expected_quote_hash != digest(quote_json):
                    raise Conflict('Der Preis hat sich geändert. Bitte den Testpreis erneut prüfen.')
                bid, now = secrets.token_urlsafe(24), int(self.clock())
                db.execute('''INSERT INTO bookings
                    (id,owner,request_key,vehicle_id,start,end,km,quote,quote_hash,state,created,expires)
                    VALUES (?,?,?,?,?,?,?,?,?,'creating',?,?)''',
                    (bid, digest(owner), identity, vehicle_id, start, end, km,
                     quote_json, digest(quote_json), now, now + 2100))
                row = db.execute('SELECT * FROM bookings WHERE id=?', (bid,)).fetchone()
            booking = dict(row)
        if booking['session_id']:
            return self.public(booking)
        # Retry only during the original creation window with EXACT same parameters/key.
        if int(self.clock()) > booking['created'] + 240:
            raise Conflict('Checkout-Ergebnis ungeklärt; Bestand bleibt bis zur Prüfung gesperrt.')
        try:
            session = self.gateway.create(self.checkout_params(booking), 'mos-test-' + booking['id'])
            self.validate_session(booking, session)
            if not session.get('url'):
                raise BookingError('Checkout-URL fehlt.')
        except Exception as exc:
            # A timeout may have happened after Stripe created the session. Never free stock here.
            with self.db(True) as db:
                db.execute("UPDATE bookings SET reason='checkout_uncertain' WHERE id=? AND state='creating'",
                           (booking['id'],))
            raise ProviderUnavailable('Checkout derzeit ungeklärt. Bitte denselben Versuch wiederholen.') from exc
        with self.db(True) as db:
            current = dict(db.execute('SELECT * FROM bookings WHERE id=?', (booking['id'],)).fetchone())
            if current['session_id'] and current['session_id'] != session['id']:
                raise Conflict('Abweichende Checkout-Session; interne Prüfung erforderlich.')
            db.execute('''UPDATE bookings SET session_id=?,checkout_url=?,
                state=CASE WHEN state='creating' THEN 'checkout_open' ELSE state END,
                reason=CASE WHEN state='creating' THEN '' ELSE reason END WHERE id=?''',
                (session['id'], session['url'], booking['id']))
        return self.get(booking['id'], owner)

    def checkout_params(self, booking):
        q = json.loads(booking['quote'])
        return {'mode': 'payment', 'payment_method_types': ['card'], 'locale': 'de',
                'client_reference_id': booking['id'],
                'metadata': {'booking_id': booking['id'], 'quote_hash': booking['quote_hash']},
                'line_items': [{'price_data': {'currency': 'eur', 'unit_amount': q['total_cents'],
                    'product_data': {'name': 'TEST · ' + q['name'],
                    'description': q['start'] + ' bis ' + q['end'] + ' · keine echte Anmietung'}}, 'quantity': 1}],
                'expires_at': booking['expires'],
                'success_url': self.base_url + '/booking/' + booking['id'],
                'cancel_url': self.base_url + '/booking/' + booking['id'] + '?checkout=cancelled'}

    def validate_session(self, booking, session):
        q = json.loads(booking['quote'])
        if (session.get('livemode') is not False or session.get('mode') != 'payment'
            or session.get('client_reference_id') != booking['id']
            or session.get('metadata', {}).get('booking_id') != booking['id']
            or session.get('metadata', {}).get('quote_hash') != booking['quote_hash']
            or session.get('amount_total') != q['total_cents'] or session.get('currency') != 'eur'
            or not str(session.get('id', '')).startswith('cs_test_')
            or (booking['session_id'] and session['id'] != booking['session_id'])):
            raise BookingError('Zahlung und gespeichertes Angebot passen nicht zusammen.')

    def handle_event(self, event):
        # Caller MUST verify the raw webhook signature before invoking this method.
        kinds = {'checkout.session.completed', 'checkout.session.async_payment_succeeded',
                 'checkout.session.expired', 'checkout.session.async_payment_failed'}
        if event.get('livemode') is not False:
            raise BookingError('Live-Ereignisse sind gesperrt.')
        if event.get('type') not in kinds:
            return
        sid = event['data']['object']['id']
        eid, kind = event['id'], event['type']
        with self.db() as db:
            previous = db.execute('SELECT * FROM events WHERE id=?', (eid,)).fetchone()
            if previous:
                if (previous['session_id'], previous['kind']) != (sid, kind):
                    raise BookingError('Ereignis-ID kollidiert.')
                return
        # Re-read canonical provider state rather than trusting stale event order.
        try:
            session = self.gateway.retrieve(sid)
        except Exception as exc:
            raise ProviderUnavailable('Zahlungsstatus nicht erreichbar; Webhook erneut zustellen.') from exc
        bid = session.get('metadata', {}).get('booking_id')
        with self.db(True) as db:
            row = db.execute('SELECT * FROM bookings WHERE session_id=?', (sid,)).fetchone()
            if row is None:
                row = db.execute('SELECT * FROM bookings WHERE id=?', (bid,)).fetchone()
            if not row:
                raise BookingError('Keine zugehörige Testbuchung.')
            b = dict(row)
            bid = b['id']
            try:
                self.validate_session(b, session)
            except BookingError:
                db.execute("UPDATE bookings SET state='review',reason='provider_data_mismatch' WHERE id=? AND paid=0 AND state!='confirmed'", (bid,))
                # Keep this operational alert even though the webhook gets a retryable failure.
                db.commit()
                raise
            previous = db.execute('SELECT * FROM events WHERE id=?', (eid,)).fetchone()
            if previous:
                if (previous['session_id'], previous['kind']) != (sid, kind):
                    raise BookingError('Ereignis-ID kollidiert.')
                return
            db.execute('INSERT INTO events VALUES (?,?,?)', (eid, sid, kind))
            db.execute('UPDATE bookings SET session_id=? WHERE id=?', (sid, bid))
            if b['state'] == 'confirmed' or b['paid']:
                return  # Never downgrade or double-fulfil an already processed payment.
            if session.get('payment_status') == 'paid' and session.get('status') == 'complete':
                pi = session.get('payment_intent')
                duplicate_pi = not isinstance(pi, str) or not pi.startswith('pi_') or db.execute(
                    'SELECT 1 FROM bookings WHERE payment_intent=? AND id!=?', (pi, bid)).fetchone()
                vehicle = db.execute('SELECT active FROM vehicles WHERE id=?', (b['vehicle_id'],)).fetchone()
                conflict = b['state'] == 'expired' or not vehicle['active'] or self.occupied(
                    db, b['vehicle_id'], b['start'], b['end'], bid)
                state = 'review' if duplicate_pi or conflict else 'confirmed'
                reason = 'paid_needs_review' if state == 'review' else ''
                db.execute('UPDATE bookings SET state=?,paid=1,payment_intent=?,reason=? WHERE id=?',
                           (state, None if duplicate_pi else pi, reason, bid))
            elif session.get('status') == 'expired' and session.get('payment_status') == 'unpaid':
                db.execute("UPDATE bookings SET state='expired',reason='' WHERE id=?", (bid,))
            elif session.get('status') == 'complete':
                db.execute("UPDATE bookings SET state='payment_pending',reason='awaiting_paid_webhook' WHERE id=?", (bid,))

    def reconcile(self, bid, cancel=False):
        with self.db() as db:
            b = dict(db.execute('SELECT * FROM bookings WHERE id=?', (bid,)).fetchone())
        if b['state'] in ('confirmed', 'expired') or b['paid']:
            return
        if not b['session_id']:
            return  # Unknown provider outcome stays blocked, including after local expiry.
        try:
            s = self.gateway.retrieve(b['session_id'])
            if cancel and s['status'] == 'open':
                s = self.gateway.expire(s['id'])
            self.validate_session(b, s)
        except Exception as exc:
            raise ProviderUnavailable('Bestand bleibt gesperrt, bis Stripe den Ablauf bestätigt.') from exc
        # A success page / reconciliation NEVER confirms payment. Wait for signed webhook.
        with self.db(True) as db:
            if s.get('status') == 'expired' and s.get('payment_status') == 'unpaid':
                db.execute("UPDATE bookings SET state='expired' WHERE id=? AND paid=0 AND state!='confirmed'", (bid,))

    def sweep(self):
        with self.db() as db:
            ids = [r['id'] for r in db.execute("SELECT id FROM bookings WHERE expires<=? AND state IN ('creating','checkout_open','payment_pending')", (int(self.clock()),))]
        for bid in ids:
            try:
                self.reconcile(bid)
            except ProviderUnavailable:
                pass  # Fail closed; admin can see the blocked entry and retry.

    @staticmethod
    def public(b):
        return {k: b[k] for k in ('id', 'state', 'expires', 'paid', 'reason', 'checkout_url')} | {
            'quote': json.loads(b['quote']), 'test_only': True,
            'pickup': 'Gärtner Karosserie & Lack · Binauer Höhe 4 · 74821 Mosbach-Lohrbach',
            'pickup_note': 'Persönliche Schlüsselübergabe. Termin muss noch vereinbart werden.'}

    def get(self, bid, owner):
        with self.db() as db:
            row = db.execute('SELECT * FROM bookings WHERE id=? AND owner=?', (bid, digest(owner))).fetchone()
            if not row:
                raise BookingError('Buchung nicht gefunden.')
            return self.public(dict(row))

    def admin_list(self):
        with self.db() as db:
            return [self.public(dict(r)) for r in db.execute('SELECT * FROM bookings ORDER BY created DESC')]
