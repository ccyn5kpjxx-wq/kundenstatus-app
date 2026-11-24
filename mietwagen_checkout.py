"""Shared portal inventory for TEST Checkout. No HTTP route or live payment support.

All writers lock mietfahrzeuge first. Holds remain visible when creation is disabled.
Only trusted server code may supply a quote/customer; never forward browser prices.
"""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import secrets
import time
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo
import mos_operator_notice
from werkstatt_kontakt import BUSINESS_ADDRESS as WORKSHOP_ADDRESS

STANDARD_DEPOSIT_MAX_DAYS = 5
EXTENDED_DEPOSIT_MAX_DAYS = 28
EXTENDED_DEPOSIT_MODE = 'extended_if_available'
LONG_RENTAL_DEPOSIT_METHOD = 'charge_with_rent_refund_after_return'
LONG_RENTAL_DEPOSIT_POLICY = 'paid_at_booking_v1'
LONG_RENTAL_MAX_DAYS = 30


def init_schema(db):
    mos_operator_notice.init_schema(db)
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_holds (
        id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE,
        mietfahrzeug_id INTEGER NOT NULL, start_datum TEXT NOT NULL, end_datum TEXT NOT NULL,
        payload TEXT NOT NULL, fingerprint TEXT NOT NULL, status TEXT NOT NULL,
        expires_at BIGINT NOT NULL, session_id TEXT UNIQUE, payment_intent TEXT UNIQUE,
        mietvorgang_id INTEGER UNIQUE, grund TEXT NOT NULL DEFAULT '')''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_events (
        id TEXT PRIMARY KEY, hold_id TEXT NOT NULL, session_id TEXT NOT NULL, kind TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_miet_checkout_fahrzeug ON miet_checkout_holds (mietfahrzeug_id, status)')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_deposit_auths (
        hold_id TEXT PRIMARY KEY, intent_id TEXT UNIQUE, status TEXT NOT NULL,
        created_at BIGINT NOT NULL, authorized_at BIGINT, capture_before TEXT,
        reason TEXT NOT NULL DEFAULT '')''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_deposit_attempts (
        hold_id TEXT NOT NULL, generation INTEGER NOT NULL, intent_id TEXT UNIQUE,
        status TEXT NOT NULL, created_at BIGINT NOT NULL, authorized_at BIGINT,
        capture_before TEXT, reason TEXT NOT NULL DEFAULT '',
        provider_status TEXT, amount_received BIGINT, released_verified_at TEXT,
        PRIMARY KEY (hold_id,generation))''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_creation_attempts (
        hold_id TEXT PRIMARY KEY, created_at BIGINT NOT NULL)''')


def _card_authorization_quote(quote):
    return quote.get('deposit_method') == 'card_authorization_at_booking'


def is_onsite_quote(quote):
    return quote.get('payment_method') == 'onsite' and quote.get('deposit_method') == 'pay_at_pickup'


def validate_onsite_quote(quote):
    """Verify the amounts which must be collected at handover, never by Stripe."""
    cents = ('amount_cents', 'rental_cents', 'daily_cents', 'deposit_cents',
             'deposit_authorized_cents', 'deposit_charged_cents')
    if (not is_onsite_quote(quote) or any(type(quote.get(key)) is not int for key in cents)
            or type(quote.get('days')) is not int or not 1 <= quote['days'] <= 30
            or quote['daily_cents'] <= 0 or quote['rental_cents'] != quote['daily_cents'] * quote['days']
            or quote['deposit_cents'] != 50000 or quote['amount_cents'] != 0
            or quote['deposit_authorized_cents'] != 0 or quote['deposit_charged_cents'] != 0
            or quote.get('checkout_deposit') is not False
            or any(key in quote for key in ('deposit_timing', 'deposit_authorization_mode', 'long_rental_deposit_policy'))):
        raise ValueError('Bei Vor-Ort-Zahlung sind Mietpreis und 500 EUR Kaution bei Abholung fällig.')


def is_long_rental_paid_deposit(quote):
    return (quote.get('long_rental_deposit_policy') == LONG_RENTAL_DEPOSIT_POLICY
            and quote.get('deposit_method') == LONG_RENTAL_DEPOSIT_METHOD)


def validate_long_rental_paid_deposit(quote):
    if (not is_long_rental_paid_deposit(quote)
            or type(quote.get('days')) is not int or not 5 < quote['days'] <= LONG_RENTAL_MAX_DAYS
            or type(quote.get('rental_cents')) is not int or quote['rental_cents'] <= 0
            or any(type(quote.get(key)) is not int for key in
                   ('amount_cents','deposit_charged_cents','deposit_cents','deposit_authorized_cents'))
            or quote.get('deposit_charged_cents') != 50000 or quote.get('deposit_cents') != 50000
            or quote.get('deposit_authorized_cents') != 0 or quote.get('checkout_deposit') is not True
            or quote.get('amount_cents') != quote['rental_cents'] + 50000
            or 'deposit_timing' in quote or 'deposit_authorization_mode' in quote):
        raise ValueError('Längere Mieten benötigen den vereinbarten Mietpreis plus 500 EUR rückzahlbare Kaution; sechs bis 30 Miettage.')


def deferred_deposit_quote(quote):
    """Only a new, signed quote can defer its separate card authorization."""
    return (_card_authorization_quote(quote)
            and quote.get('deposit_timing') == 'before_pickup')


def extended_deposit_quote(quote):
    return (deferred_deposit_quote(quote)
            and quote.get('deposit_authorization_mode') == EXTENDED_DEPOSIT_MODE
            and type(quote.get('days')) is int
            and STANDARD_DEPOSIT_MAX_DAYS < quote['days'] <= EXTENDED_DEPOSIT_MAX_DAYS)


def deposit_authorization_opens(quote):
    start = datetime.fromisoformat(quote['start_slot'])
    if start.tzinfo is None:
        raise ValueError('Abholtermin ohne Zeitzone ist nicht verbindlich.')
    return start.astimezone(timezone.utc) - timedelta(hours=12)


def deposit_authorization_due(quote, now=None):
    now = now or datetime.now(timezone.utc)
    end = datetime.fromisoformat(quote['end_slot'])
    if now.tzinfo is None or end.tzinfo is None:
        raise ValueError('Kautionszeitpunkt braucht eine Zeitzone.')
    return deposit_authorization_opens(quote) <= now.astimezone(timezone.utc) < end.astimezone(timezone.utc)


def _checkout_disclosure(quote, *, test_mode):
    """Repeat the signed booking's key costs beside Stripe's final pay button."""
    def local_slot(value):
        slot = datetime.fromisoformat(value)
        if slot.tzinfo is None:
            raise ValueError('Miettermin ohne Zeitzone ist nicht verbindlich.')
        return slot.astimezone(ZoneInfo('Europe/Berlin')).strftime('%d.%m.%Y %H:%M')

    description = (
        f"Abholung {local_slot(quote['start_slot'])}; Rückgabe {local_slot(quote['end_slot'])}; "
        f"{quote['included_km']} km inklusive; Mehrkilometer "
        f"{quote['extra_km_cents']/100:.2f} EUR/km. "
        f'Aktuelle Abholadresse für die persönliche Übergabe: Gärtner, {WORKSHOP_ADDRESS}.'
    )
    deposit = ('500 EUR rückzahlbare Kaution im Zahlbetrag; Erstattung nach ordnungsgemäßer Rückgabe. '
               if is_long_rental_paid_deposit(quote) else
               '500 EUR Kaution vor Abholung online auf Kreditkarte reservieren '
               '(frühestens 12 Stunden vorher), ohne Abbuchung. '
               if deferred_deposit_quote(quote) else
               '500 EUR Kaution bereits separat auf der Kreditkarte reserviert, ohne Abbuchung. '
               if _card_authorization_quote(quote) else
               'Eine rückzahlbare Kaution ist als eigener Posten ausgewiesen. ')
    cancellation = ''
    if quote.get('cancellation_policy') == 'free_48h_then_10pct_rent':
        fee = (quote['rental_cents'] + 5) // 10
        cancellation = (
            f'Bis einschließlich 48 Stunden vor Abholung kostenlos stornieren; danach '
            f'höchstens {fee/100:.2f} EUR (10 % der Miete), vorbehaltlich geringeren Schadens. '
        )
    message = (
        ('TEST – keine echte Zahlung. ' if test_mode else '') +
        'Sie bestellen die angezeigte Miete zahlungspflichtig. ' + deposit +
        'Vertragliche Selbstbeteiligung 1.000 EUR gemäß den vorab unterschriebenen Bedingungen. ' +
        cancellation + 'Vermieter: Gärtner GmbH Karosserie + Lack.'
    )
    if len(message) > 500:
        raise ValueError('Stripe-Hinweis zur Bestellung ist zu lang.')
    return description, message


def _pickup_passed(quote):
    slot = datetime.fromisoformat(quote['start_slot'])
    if slot.tzinfo is None:
        raise ValueError('Abholtermin ohne Zeitzone ist nicht verbindlich.')
    return slot.astimezone(timezone.utc) <= datetime.now(timezone.utc)


def _require_open_public_slots(db, quote, postgres=False):
    """Serialize public slot checks with admin close/open updates."""
    policy = quote.get('slot_policy')
    if policy is None:  # Existing internal and prototype quotes have no public slot policy.
        return
    if policy != 'db_open_slots_v1':
        raise ValueError('Unbekannte Übergabetermin-Regel.')
    start, end = quote.get('start_slot'), quote.get('end_slot')
    if not isinstance(start, str) or not isinstance(end, str) or start == end:
        raise ValueError('Übergabetermine fehlen oder sind ungültig.')
    if _pickup_passed(quote):
        raise ValueError('Der Abholtermin ist verstrichen. Bitte neu buchen.')
    suffix = ' FOR UPDATE' if postgres else ''
    rows = db.execute('SELECT slot,active FROM miet_checkout_slots WHERE slot IN (?,?)' + suffix,
                      (start, end)).fetchall()
    if {row['slot'] for row in rows if int(row['active']) == 1} != {start, end}:
        raise ValueError('Ein Übergabetermin ist nicht mehr freigegeben. Bitte neu buchen.')


def zeitraum_frei(db, vehicle_id, start, end, exclude_hold_id=None):
    # Never expire from the local clock: a payment may have won the expiry race.
    rows = db.execute('''SELECT id,start_datum,end_datum FROM miet_checkout_holds
        WHERE mietfahrzeug_id=? AND status IN ('pending','review')''', (int(vehicle_id),)).fetchall()
    return not any(r['id'] != exclude_hold_id and start <= date.fromisoformat(r['end_datum'])
                   and date.fromisoformat(r['start_datum']) <= (end or date.max) for r in rows)


@contextmanager
def locked_vehicle(portal, vehicle_id):
    """Serialize inventory changes for this vehicle in both checkout and the admin calendar."""
    db = portal.get_db()
    try:
        if not portal.USE_POSTGRES:
            db.execute('BEGIN IMMEDIATE')
        suffix = ' FOR UPDATE' if portal.USE_POSTGRES else ''
        vehicle = db.execute('SELECT * FROM mietfahrzeuge WHERE id=?' + suffix, (int(vehicle_id),)).fetchone()
        if vehicle is None:
            raise ValueError('Mietfahrzeug nicht gefunden.')
        yield db, vehicle
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


class SharedCheckout:
    def __init__(self, portal, gateway, return_url_base=None):
        from mos_booking.gateway import StripeTestGateway, OfflineGateway
        if not isinstance(gateway, (StripeTestGateway, OfflineGateway)):
            raise ValueError('Nur der abgesicherte Test-Gateway ist zulässig.')
        self.p, self.gateway = portal, gateway
        self.return_url_base = return_url_base
        if return_url_base:
            parsed = urlsplit(return_url_base)
            if (parsed.username or parsed.password or parsed.query or parsed.fragment
                or not parsed.hostname or (parsed.scheme != 'https' and not
                    (parsed.scheme == 'http' and parsed.hostname in {'127.0.0.1','localhost'}))):
                raise ValueError('Ungültige Test-Rücksprungadresse.')

    def _new_bookings_enabled(self):
        if not self.p.app.config.get('MOS_SHARED_CHECKOUT_ENABLED', False):
            return False
        cfg = self.p.app.config.get('MOS_PUBLIC_BOOKING', {})
        return cfg.get('mode') != 'live' or cfg.get('enabled') is True

    @staticmethod
    def _record_paused_paid_review(db, h, session):
        pi = session.get('payment_intent')
        valid_pi = (isinstance(pi, str) and pi.startswith('pi_')
                    and (not h['payment_intent'] or h['payment_intent'] == pi)
                    and not db.execute('SELECT id FROM miet_checkout_holds WHERE payment_intent=? AND id!=?',
                                       (pi, h['id'])).fetchone())
        db.execute('''UPDATE miet_checkout_holds
            SET status='review',grund=?,payment_intent=? WHERE id=?''',
            ('paid_bookings_disabled_review' if valid_pi else 'paid_bookings_disabled_payment_unclear_review',
             pi if valid_pi else h['payment_intent'], h['id']))

    def locked(self, vehicle_id):
        return locked_vehicle(self.p, vehicle_id)

    def read(self, hold_id):
        db = self.p.get_db()
        try:
            r = db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone()
            if r is None:
                raise ValueError('Reservierung nicht gefunden.')
            return dict(r)
        finally:
            db.close()

    def reserve(self, request_key, vehicle_id, start, end, customer, quote):
        if not self._new_bookings_enabled():
            raise ValueError('Gemeinsamer Test-Checkout ist deaktiviert.')
        start, end = self.p.validiere_mietzeitraum(start, end)
        self.p.validiere_mietkontakt(customer.get('telefon'), customer.get('email'))
        name = self.p.clean_text(customer.get('name'))
        if not name or not isinstance(request_key, str) or not 16 <= len(request_key) <= 200:
            raise ValueError('Kunde oder Idempotenzreferenz fehlt.')
        onsite = quote.get('payment_method') == 'onsite' or quote.get('deposit_method') == 'pay_at_pickup'
        if onsite:
            validate_onsite_quote(quote)
            if quote.get('slot_policy') != 'db_open_slots_v1':
                raise ValueError('Vor-Ort-Buchungen benötigen freigegebene Abhol- und Rückgabetermine.')
        if (type(quote.get('amount_cents')) is not int or (not onsite and quote['amount_cents'] <= 0)
                or quote.get('currency') != 'eur' or not quote.get('rules_version')):
            raise ValueError('Verbindlicher serverseitiger Preis und Regelversion erforderlich.')
        if 'deposit_timing' in quote and not deferred_deposit_quote(quote):
            raise ValueError('Unbekannter Zeitpunkt der Kartenreservierung.')
        if 'long_rental_deposit_policy' in quote:
            validate_long_rental_paid_deposit(quote)
        authorization_mode = quote.get('deposit_authorization_mode')
        if authorization_mode is not None and (
                authorization_mode != EXTENDED_DEPOSIT_MODE or not deferred_deposit_quote(quote)
                or type(quote.get('days')) is not int or not 1 <= quote['days'] <= EXTENDED_DEPOSIT_MAX_DAYS):
            raise ValueError('Ungültige Regel für die verlängerte Kreditkartenautorisierung.')
        if (deferred_deposit_quote(quote) and type(quote.get('days')) is int
                and quote['days'] > STANDARD_DEPOSIT_MAX_DAYS and not extended_deposit_quote(quote)):
            raise ValueError('Mieten über fünf Tage benötigen eine bestätigte verlängerte Kreditkartenautorisierung; höchstens 28 Miettage.')
        payload = json.dumps({'customer': {'name': name, 'telefon': customer.get('telefon', ''),
                            'email': customer.get('email', '')}, 'quote': quote}, sort_keys=True)
        verify_amounts = onsite or _card_authorization_quote(quote) or is_long_rental_paid_deposit(quote)
        notify_pending = not onsite and quote.get('test_only') is False and bool(quote.get('signed_contract_hash'))
        if verify_amounts:
            if (type(quote.get('rental_cents')) is not int or quote['rental_cents'] <= 0
                    or (_card_authorization_quote(quote) and (
                        quote['amount_cents'] != quote['rental_cents']
                        or quote.get('deposit_authorized_cents') != 50000
                        or quote.get('deposit_charged_cents') != 0))
                    or quote.get('vehicle_id') != int(vehicle_id)
                    or datetime.fromisoformat(quote['start_slot']).date() != start
                    or datetime.fromisoformat(quote['end_slot']).date() != end):
                raise ValueError('Mietpreis und gesonderte Kartenautorisierung stimmen nicht.')
        if verify_amounts or notify_pending:
            from mos_public_contract import signed_payload
            signed_payload(json.loads(payload))
        fingerprint = hashlib.sha256(json.dumps([int(vehicle_id),start.isoformat(),end.isoformat(),payload]).encode()).hexdigest()
        with self.locked(vehicle_id) as (db, vehicle):
            existing = db.execute('SELECT * FROM miet_checkout_holds WHERE request_key=?', (request_key,)).fetchone()
            if existing:
                if existing['fingerprint'] != fingerprint:
                    raise ValueError('Idempotenzreferenz mit anderen Angaben verwendet.')
                return dict(existing)
            if not int(vehicle['aktiv'] or 0) or self.p.normalize_mietfahrzeug_status(vehicle['status']) in {'bald','wartung','inaktiv'}:
                raise ValueError('Fahrzeug nicht freigegeben.')
            _require_open_public_slots(db, quote, self.p.USE_POSTGRES)
            if not self.p.mietfahrzeug_zeitraum_frei_db(db, vehicle_id, start, end):
                raise ValueError('Zeitraum bereits belegt.')
            hold_id = secrets.token_urlsafe(24)
            db.execute('''INSERT INTO miet_checkout_holds
                (id,request_key,mietfahrzeug_id,start_datum,end_datum,payload,fingerprint,status,expires_at)
                VALUES (?,?,?,?,?,?,?,'pending',?)''',
                (hold_id,request_key,int(vehicle_id),start.isoformat(),end.isoformat(),payload,fingerprint,int(time.time())+2100))
            if notify_pending:
                from mos_operator_notice import enqueue
                enqueue(db, hold_id, 'payment_pending', quote)
            if onsite:
                # Commit both the confirmed booking and its inventory allocation together.
                # The transient pending state cannot be observed by another transaction.
                rental_id = self._insert_rental(db, vehicle_id, start, end, name, customer,
                                               'MOS Vor-Ort-Zahlung ' + hold_id)
                db.execute("""UPDATE miet_checkout_holds SET status='confirmed',mietvorgang_id=?,
                              expires_at=0 WHERE id=?""", (rental_id, hold_id))
                mos_operator_notice.enqueue_confirmation(db, hold_id, quote, rental_id, name)
            return dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())

    def _insert_rental(self, db, vehicle_id, start, end, name, customer, note):
        now = self.p.now_str()
        return db.execute('''INSERT INTO mietvorgaenge
            (mietfahrzeug_id,auftrag_id,kunde_name,kunde_telefon,kunde_email,whatsapp_erlaubt,
             start_datum,end_datum,rueckgabe_datum,status,notiz,erstellt_am,geaendert_am)
            VALUES (?,0,?,?,?,0,?,?,'','aktiv',?,?,?)''',
            (vehicle_id, name, customer.get('telefon', ''), customer.get('email', ''),
             start.strftime(self.p.DATE_FMT), end.strftime(self.p.DATE_FMT), note, now, now)).lastrowid

    def _deposit_record(self, hold_id):
        db = self.p.get_db()
        try:
            row = db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone()
            return dict(row) if row else None
        finally:
            db.close()

    def _validated_charged_payment(self, h, checkout, payment, *, expected_refunded_cents=0):
        q = json.loads(h['payload'])['quote']
        validate_long_rental_paid_deposit(q)
        from mos_public_contract import signed_payload
        signed_payload(json.loads(h['payload']))
        self.validate(h, checkout)
        pi = checkout.get('payment_intent')
        charge = payment.get('latest_charge')
        if (type(expected_refunded_cents) is not int or expected_refunded_cents not in (0,50000)
                or checkout.get('status') != 'complete' or checkout.get('payment_status') != 'paid'
                or not isinstance(pi, str) or not pi.startswith('pi_')
                or h['payment_intent'] not in (None, pi)
                or payment.get('id') != pi or payment.get('object') != 'payment_intent'
                or payment.get('livemode') is not self.gateway.livemode
                or payment.get('status') != 'succeeded' or payment.get('currency') != 'eur'
                or any(type(payment.get(key)) is not int for key in ('amount','amount_received','amount_capturable'))
                or payment.get('amount') != q['amount_cents']
                or payment.get('amount_received') != q['amount_cents']
                or payment.get('amount_capturable') != 0
                or not isinstance(charge, dict) or charge.get('object') != 'charge'
                or charge.get('payment_intent') != pi or charge.get('currency') != 'eur'
                or charge.get('amount') != q['amount_cents'] or charge.get('paid') is not True
                or charge.get('captured') is not True or charge.get('disputed') is not False
                or type(charge.get('amount_refunded')) is not int
                or charge['amount_refunded'] != expected_refunded_cents):
            raise ValueError('Mietzahlung und vollständig bezahlte 500-EUR-Kaution müssen beim Anbieter geprüft werden.')
        return True

    def _require_charged_booking(self, db, h):
        if h['status'] != 'confirmed' or not h['mietvorgang_id'] or not h['payment_intent'] or not h['session_id']:
            raise ValueError('Keine bestätigte bezahlte Langzeitmiete.')
        validate_long_rental_paid_deposit(json.loads(h['payload'])['quote'])
        rental = db.execute('SELECT mietfahrzeug_id,status,rueckgabe_datum FROM mietvorgaenge WHERE id=?',
                            (h['mietvorgang_id'],)).fetchone()
        if (not rental or rental['mietfahrzeug_id'] != h['mietfahrzeug_id']
                or rental['status'] != 'aktiv' or rental['rueckgabe_datum']
                or db.execute('SELECT id FROM miet_checkout_cancellations WHERE id=?', (h['id'],)).fetchone()
                or db.execute('SELECT id FROM miet_checkout_refunds WHERE hold_id=?', (h['id'],)).fetchone()):
            raise ValueError('Miete storniert, beendet oder Erstattung vorhanden; Zahlungsprüfung erforderlich.')

    def verify_charged_deposit(self, hold_id):
        """Fresh read-only payment evidence; handover must recheck DB under lock."""
        h = self.read(hold_id)
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            self._require_charged_booking(db, current)
        checkout = self.gateway.retrieve(h['session_id'])
        payment = self.gateway.retrieve_payment(h['payment_intent'])
        self._validated_charged_payment(h, checkout, payment)
        verified_at = datetime.now(timezone.utc).isoformat()
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            self._require_charged_booking(db, current)
            self._validated_charged_payment(current, checkout, payment)
        evidence={key:checkout.get(key) for key in ('id','object','client_reference_id','metadata',
            'payment_intent','livemode','status','payment_status','mode','amount_total','currency')}
        return {**evidence, 'deposit_net_cents': 50000, 'refunded_cents': 0,
                'verified_at': verified_at}

    @staticmethod
    def _deposit_attempt(db, hold_id):
        row = db.execute('''SELECT * FROM miet_checkout_deposit_attempts
            WHERE hold_id=? ORDER BY generation DESC LIMIT 1''', (hold_id,)).fetchone()
        return dict(row) if row else None

    def _journal_deposit(self, db, hold_id, intent=None):
        """Mirror the current pointer under the vehicle lock; never delete history."""
        record = dict(db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                 (hold_id,)).fetchone())
        attempt = self._deposit_attempt(db, hold_id)
        if attempt and attempt['intent_id'] and attempt['intent_id'] != record['intent_id']:
            raise ValueError('Kautionsversuch stimmt nicht mit der aktuellen Kartenreservierung überein.')
        generation = attempt['generation'] if attempt else 1
        if not attempt:
            db.execute('''INSERT INTO miet_checkout_deposit_attempts
                (hold_id,generation,intent_id,status,created_at,authorized_at,capture_before,reason)
                VALUES (?,?,?,?,?,?,?,?) RETURNING generation''',
                (hold_id,generation,record['intent_id'],record['status'],record['created_at'],
                 record['authorized_at'],record['capture_before'],record['reason'])).fetchone()
        db.execute('''UPDATE miet_checkout_deposit_attempts
            SET intent_id=?,status=?,created_at=?,authorized_at=?,capture_before=?,reason=?
            WHERE hold_id=? AND generation=?''',
            (record['intent_id'],record['status'],record['created_at'],record['authorized_at'],
             record['capture_before'],record['reason'],hold_id,generation))
        if intent is not None:
            if intent.get('id') != record['intent_id']:
                raise ValueError('Anbieterstatus gehört nicht zum aktuellen Kautionsversuch.')
            released = (intent.get('status') == 'canceled'
                        and type(intent.get('amount_received')) is int and intent['amount_received'] == 0
                        and type(intent.get('amount_capturable')) is int and intent['amount_capturable'] == 0)
            db.execute('''UPDATE miet_checkout_deposit_attempts
                SET provider_status=?,amount_received=?,released_verified_at=COALESCE(?,released_verified_at)
                WHERE hold_id=? AND generation=?''',
                (intent.get('status'),intent.get('amount_received'),
                 datetime.now(timezone.utc).isoformat() if released else None,hold_id,generation))
        return self._deposit_attempt(db, hold_id)

    @staticmethod
    def _deposit_operation_key(hold_id, generation, *, release=False):
        key = ('mos-deposit-release-' if release else 'mos-deposit-') + hold_id
        return key if generation == 1 else key + '-g' + str(generation)

    def _canceled_deposit_verified(self, h, record):
        intent = self.gateway.retrieve_deposit_intent(record['intent_id'])
        self._validated_deposit(h, record, intent, enforce_coverage=False)
        if (intent.get('status') != 'canceled'
                or type(intent.get('amount_received')) is not int or intent['amount_received'] != 0
                or type(intent.get('amount_capturable')) is not int or intent['amount_capturable'] != 0):
            raise ValueError('Vor einem neuen Kartenversuch muss die bisherige Kaution sicher freigegeben sein.')
        return intent

    def can_retry_deposit(self, hold_id):
        """Read-only eligibility; only a locally released paid hold calls Stripe."""
        try:
            h = self.read(hold_id)
            record = self._deposit_record(hold_id)
            if (not deferred_deposit_quote(json.loads(h['payload'])['quote'])
                    or not record or record['status'] != 'released' or not record['intent_id']):
                return False
            with self.locked(h['mietfahrzeug_id']) as (db, _):
                current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
                self._require_deferred_deposit_booking(db, current)
            self._canceled_deposit_verified(h, record)
            with self.locked(h['mietfahrzeug_id']) as (db, _):
                current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
                self._require_deferred_deposit_booking(db, current)
                latest = db.execute('SELECT intent_id,status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                    (hold_id,)).fetchone()
                return latest['intent_id'] == record['intent_id'] and latest['status'] == 'released'
        except Exception:
            return False  # Unknown provider/booking state never permits a retry.

    def retry_deposit(self, hold_id):
        """Explicit correction after a verified canceled, never charged intent."""
        h = self.read(hold_id)
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            self._require_deferred_deposit_booking(db, current)
            row = db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone()
            if not row:
                raise ValueError('Es gibt keinen sicher freigegebenen Kartenversuch zu korrigieren.')
            record = dict(row)
            attempt = self._deposit_attempt(db, hold_id)
            resume = (attempt and attempt['generation'] >= 2
                      and attempt['intent_id'] == record['intent_id']
                      and record['status'] in {'creating','awaiting_card','authorized'})
            if not resume and (record['status'] != 'released' or not record['intent_id']):
                raise ValueError('Unklarer Kartenstatus: kein neuer Kartenversuch erlaubt.')
        if resume:
            return self.prepare_deposit(hold_id)  # Replay the already durable generation.
        canceled = self._canceled_deposit_verified(h, record)
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            self._require_deferred_deposit_booking(db, current)
            latest = dict(db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone())
            if latest['intent_id'] != record['intent_id'] or latest['status'] != 'released':
                attempt = self._deposit_attempt(db, hold_id)
                if not (attempt and attempt['generation'] >= 2
                        and attempt['intent_id'] == latest['intent_id']
                        and latest['status'] in {'creating','awaiting_card','authorized'}):
                    raise ValueError('Der Kartenstatus wurde geändert; bitte erneut prüfen.')
            else:
                previous = self._journal_deposit(db, hold_id, canceled)
                generation = previous['generation'] + 1
                now = int(time.time())
                db.execute('''INSERT INTO miet_checkout_deposit_attempts
                    (hold_id,generation,status,created_at) VALUES (?,?,'creating',?) RETURNING generation''',
                    (hold_id,generation,now)).fetchone()
                db.execute('''UPDATE miet_checkout_deposit_auths SET intent_id=NULL,status='creating',
                    created_at=?,authorized_at=NULL,capture_before=NULL,reason='' WHERE hold_id=?''',
                    (now,hold_id))
        return self.prepare_deposit(hold_id)

    def _validated_deposit(self, h, record, intent, enforce_coverage=True):
        q = json.loads(h['payload'])['quote']
        if not _card_authorization_quote(q):
            raise ValueError('Diese Buchung hat keine gesonderte Kartenautorisierung.')
        expected_id = record['intent_id'] if record else None
        ident = intent.get('id')
        metadata = intent.get('metadata')
        if (not isinstance(ident, str) or not ident.startswith('pi_')
                or (expected_id and ident != expected_id)
                or intent.get('livemode') is not self.gateway.livemode
                or intent.get('amount') != 50000
                or intent.get('currency') != 'eur'
                or not isinstance(metadata, dict)
                or metadata.get('hold_id') != h['id']
                or metadata.get('quote_hash') != h['fingerprint']):
            raise ValueError('Kartenautorisierung stimmt nicht mit der Reservierung überein.')
        status = intent.get('status')
        if status not in {'requires_payment_method', 'requires_confirmation', 'requires_action',
                          'processing', 'requires_capture', 'canceled'}:
            raise ValueError('Unbekannter Status der Kartenautorisierung.')
        ready = False
        capture_before = intent.get('capture_before')
        if status == 'requires_capture':
            if intent.get('amount_capturable') != 50000 or not capture_before:
                raise ValueError('Kaution nicht vollständig oder ohne sichere Autorisierungsfrist.')
            try:
                until = datetime.fromisoformat(capture_before.replace('Z', '+00:00'))
                end = datetime.fromisoformat(q['end_slot'])
                if until.tzinfo is None or end.tzinfo is None:
                    raise ValueError('Zeitzone fehlt.')
            except (TypeError, ValueError) as exc:
                raise ValueError('Autorisierungsfrist oder Rückgabetermin ungültig.') from exc
            if intent.get('card_funding') != 'credit':
                if enforce_coverage:
                    raise ValueError('Für die Kaution ist eine Kreditkarte erforderlich.')
            elif until.astimezone(timezone.utc) <= end.astimezone(timezone.utc) + timedelta(hours=24):
                if enforce_coverage:
                    raise ValueError('Kartenautorisierung reicht nicht bis nach der Rückgabe.')
            else:
                ready = True
        return ready

    def _require_deferred_deposit_booking(self, db, h, *, check_window=True):
        """A later authorization belongs only to an active, paid booking."""
        q = json.loads(h['payload'])['quote']
        if (not deferred_deposit_quote(q) or h['status'] != 'confirmed'
                or not h['payment_intent'] or not h['session_id'] or not h['mietvorgang_id']):
            raise ValueError('Kaution erst nach bestätigter Mietpreiszahlung reservieren.')
        from mos_public_contract import signed_payload
        signed_payload(json.loads(h['payload']))
        canceled = db.execute('SELECT id FROM miet_checkout_cancellations WHERE id=?', (h['id'],)).fetchone()
        rental = db.execute('SELECT mietfahrzeug_id,status,rueckgabe_datum FROM mietvorgaenge WHERE id=?',
                            (h['mietvorgang_id'],)).fetchone()
        handed_over = db.execute('SELECT hold_id FROM miet_checkout_handovers WHERE hold_id=?',
                                 (h['id'],)).fetchone()
        if (canceled or not rental or rental['mietfahrzeug_id'] != h['mietfahrzeug_id']
                or rental['status'] != 'aktiv' or rental['rueckgabe_datum'] or handed_over):
            raise ValueError('Für diese beendete oder bereits übergebene Buchung darf keine neue Kaution entstehen.')
        if check_window and not deposit_authorization_due(q):
            raise ValueError('Kaution frühestens zwölf Stunden vor Abholung und vor dem Rückgabetermin reservieren.')

    def prepare_deposit(self, hold_id):
        """Create one manual-capture card intent; uncertain outcomes retain stock."""
        h = self.read(hold_id)
        q = json.loads(h['payload'])['quote']
        deferred = deferred_deposit_quote(q)
        if not self._new_bookings_enabled() and not deferred:
            raise ValueError('Gemeinsamer Checkout ist deaktiviert.')
        if not _card_authorization_quote(q) or h['status'] != ('confirmed' if deferred else 'pending'):
            raise ValueError('Kartenautorisierung für diese Reservierung nicht möglich.')
        if not deferred and _pickup_passed(q):
            self._release_unusable_authorization(hold_id, 'Der Abholtermin ist verstrichen')
        if not deferred and h['expires_at'] <= int(time.time()):
            self.cancel_or_reconcile(hold_id)
            raise ValueError('Die Reservierungsfrist ist abgelaufen. Bitte neu buchen.')
        from mos_public_contract import signed_payload
        signed_payload(json.loads(h['payload']))
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            if deferred:
                self._require_deferred_deposit_booking(db, current)
            else:
                if current['status'] != 'pending':
                    raise ValueError('Reservierung nicht offen.')
                _require_open_public_slots(db, q, self.p.USE_POSTGRES)
            record = db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone()
            if record is None:
                db.execute('''INSERT INTO miet_checkout_deposit_auths (hold_id,status,created_at)
                    VALUES (?,'creating',?) RETURNING hold_id''', (hold_id, int(time.time()))).fetchone()
                record = db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone()
            record = dict(record)
            if record['status'] in {'releasing', 'released', 'review'}:
                raise ValueError('Kartenautorisierung gesperrt; manuelle Prüfung erforderlich.')
            attempt = self._journal_deposit(db, hold_id)
        if record['intent_id']:
            intent = self.gateway.retrieve_deposit_intent(record['intent_id'])
        else:
            # Stripe only guarantees an idempotency key for 24 hours. Never make a
            # second uncertain request after that window, even if no ID was stored.
            if int(time.time()) - record['created_at'] >= 23 * 3600:
                raise ValueError('Unklarer Autorisierungsauftrag muss manuell abgeglichen werden.')
            params = {'amount': 50000, 'currency': 'eur', 'capture_method': 'manual',
                      'payment_method_types': ['card'],
                      'metadata': {'hold_id': h['id'], 'quote_hash': h['fingerprint']},
                      'description': 'MOS Mietwagen – rückzahlbare Kaution als Kartenautorisierung'}
            if extended_deposit_quote(q):
                params['payment_method_options'] = {'card': {'request_extended_authorization': 'if_available'}}
            key = self._deposit_operation_key(h['id'], attempt['generation'])
            if deferred and getattr(self.gateway, 'settlement_only', False):
                if attempt['generation'] == 1:
                    intent = self.gateway.create_existing_deposit_intent(params, key, h)
                else:
                    intent = self.gateway.create_existing_deposit_intent(params, key, h, attempt=attempt)
            else:
                intent = self.gateway.create_deposit_intent(params, key)
        ready = self._validated_deposit(h, record, intent, enforce_coverage=False)
        expired_after_confirmation = False
        authorization_needs_review = False
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            latest = dict(db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone())
            if latest['intent_id'] and latest['intent_id'] != intent['id']:
                raise ValueError('Mehrdeutige Kartenautorisierung; manuelle Prüfung erforderlich.')
            authorization_needs_review = (current['status'] != ('confirmed' if deferred else 'pending')
                                          or latest['status'] in {'releasing', 'released', 'review'})
            if deferred and not authorization_needs_review:
                try:
                    self._require_deferred_deposit_booking(db, current)
                except ValueError:
                    authorization_needs_review = True
            if authorization_needs_review and not latest['intent_id']:
                # The admin can close a slot while Stripe is creating an intent.
                # Persist its ID before reporting a manual-review case.
                db.execute('''UPDATE miet_checkout_deposit_auths
                    SET intent_id=?,status='review',capture_before=?,reason=? WHERE hold_id=?''',
                    (intent['id'], intent.get('capture_before'), 'authorization_created_after_close', hold_id))
            if not deferred and not authorization_needs_review and ready and latest['authorized_at'] is None:
                now = int(time.time())
                if current['expires_at'] <= now:
                    expired_after_confirmation = True
                elif not current['session_id'] and not db.execute(
                        'SELECT hold_id FROM miet_checkout_creation_attempts WHERE hold_id=?', (hold_id,)).fetchone():
                    db.execute('UPDATE miet_checkout_holds SET expires_at=? WHERE id=?',
                               (max(current['expires_at'], now + 3600), hold_id))
            if not authorization_needs_review:
                db.execute('''UPDATE miet_checkout_deposit_auths
                    SET intent_id=?,status=?,capture_before=?,authorized_at=COALESCE(authorized_at,?) WHERE hold_id=?''',
                    (intent['id'], 'authorized' if ready else 'awaiting_card', intent.get('capture_before'),
                     int(time.time()) if ready else None, hold_id))
            self._journal_deposit(db, hold_id, intent)
        if authorization_needs_review:
            raise ValueError('Kartenautorisierung wurde während der Erstellung gesperrt; manuelle Prüfung erforderlich.')
        if expired_after_confirmation:
            self._release_unusable_authorization(hold_id, 'Reservierungsfrist ist abgelaufen')
        if intent['status'] == 'requires_capture' and not ready:
            reason = ('Für die Kaution ist eine Kreditkarte erforderlich' if intent.get('card_funding') != 'credit'
                      else 'Kartenautorisierung reicht nicht bis nach der Rückgabe')
            self._release_unusable_authorization(hold_id, reason)
        if intent['status'] == 'canceled':
            self._release_unusable_authorization(hold_id, 'Kartenautorisierung wurde beendet')
        return {**intent, 'ready': ready}

    def _release_unusable_authorization(self, hold_id, reason):
        """Cancel an unusable card hold before rent is charged."""
        h = self.read(hold_id)
        if h['status'] == 'confirmed' and deferred_deposit_quote(json.loads(h['payload'])['quote']):
            # A paid future rental must remain booked when the customer supplies
            # an unsuitable card or its authorization expires. Only release the
            # separate intent; the workshop can resolve the card problem.
            self.release_deposit(hold_id, reason)
            raise ValueError(reason + '; Kartenreservierung freigegeben. Die bezahlte Buchung bleibt bestehen; bitte Werkstatt kontaktieren.')
        self._begin_release_without_session(h, reason)
        self.release_deposit(hold_id, reason)
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = db.execute('SELECT status,session_id FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone()
            deposit = db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone()
            uncertain = db.execute('SELECT hold_id FROM miet_checkout_creation_attempts WHERE hold_id=?',
                                   (hold_id,)).fetchone()
            if (current['status'] == 'pending' and not current['session_id'] and not uncertain
                    and (not deposit or deposit['status'] == 'released')):
                db.execute("UPDATE miet_checkout_holds SET status='released' WHERE id=?", (hold_id,))
        raise ValueError(reason + '; sie wurde freigegeben. Bitte neu buchen.')

    def _begin_release_without_session(self, h, reason):
        """Prevent a racing rent checkout before canceling the card intent."""
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = db.execute('SELECT status,session_id FROM miet_checkout_holds WHERE id=?',
                                 (h['id'],)).fetchone()
            uncertain = db.execute('SELECT hold_id FROM miet_checkout_creation_attempts WHERE hold_id=?',
                                   (h['id'],)).fetchone()
            if current['status'] != 'pending' or current['session_id'] or uncertain:
                raise ValueError(reason + '; Mietpreis-Checkout muss manuell abgeglichen werden.')
            deposit = db.execute('SELECT status,intent_id FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                 (h['id'],)).fetchone()
            if deposit and not deposit['intent_id']:
                raise ValueError('Ausgang der Kartenautorisierung unklar; Bestand bleibt gesperrt.')
            if deposit and deposit['status'] not in {'released', 'review'}:
                db.execute("UPDATE miet_checkout_deposit_auths SET status='releasing',reason=? WHERE hold_id=?",
                           (str(reason)[:200], h['id']))
                self._journal_deposit(db, h['id'])

    def reconcile_deposit(self, hold_id):
        """Reload the provider; only a full, long-enough hold is ready for rent checkout."""
        h = self.read(hold_id)
        q = json.loads(h['payload'])['quote']
        if not _card_authorization_quote(q):
            raise ValueError('Keine gesonderte Kartenautorisierung.')
        record = self._deposit_record(hold_id)
        if not record or not record['intent_id'] or record['status'] in {'releasing', 'released', 'review'}:
            raise ValueError('Kartenautorisierung fehlt oder muss manuell geprüft werden.')
        intent = self.gateway.retrieve_deposit_intent(record['intent_id'])
        ready = self._validated_deposit(h, record, intent, enforce_coverage=False)
        if h['status'] == 'confirmed':
            if (deferred_deposit_quote(q) and record['status'] == 'authorized'
                    and intent.get('status') == 'canceled'
                    and type(intent.get('amount_received')) is int and intent['amount_received'] == 0
                    and type(intent.get('amount_capturable')) is int and intent['amount_capturable'] == 0):
                # Before handover, a freshly verified expired/canceled intent
                # may be mirrored locally. This never cancels the paid rental
                # or creates/replaces an intent; correction still needs POST.
                with self.locked(h['mietfahrzeug_id']) as (db, _):
                    current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
                    self._require_deferred_deposit_booking(db, current, check_window=False)
                    latest = db.execute('SELECT intent_id,status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                        (hold_id,)).fetchone()
                    if latest['intent_id'] != intent['id'] or latest['status'] != 'authorized':
                        raise ValueError('Kartenautorisierung wurde zwischenzeitlich geändert.')
                    db.execute("UPDATE miet_checkout_deposit_auths SET status='released',capture_before=NULL,reason=? WHERE hold_id=?",
                               ('Anbieter bestätigt abgelaufene oder beendete Kartenreservierung ohne Einzug.', hold_id))
                    self._journal_deposit(db, hold_id, intent)
                return {**intent, 'ready': False}
            if deferred_deposit_quote(q) and record['status'] != 'authorized':
                # The customer confirms the later intent directly at Stripe.
                # Persist that result without changing the paid rental or its
                # expired pre-payment hold deadline.
                with self.locked(h['mietfahrzeug_id']) as (db, _):
                    current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
                    latest = dict(db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone())
                    self._require_deferred_deposit_booking(db, current, check_window=False)
                    if (latest['intent_id'] != intent['id']
                            or latest['status'] in {'releasing', 'released', 'review'}):
                        raise ValueError('Kartenautorisierung wurde zwischenzeitlich geändert.')
                    db.execute('''UPDATE miet_checkout_deposit_auths
                        SET status=?,capture_before=?,authorized_at=COALESCE(authorized_at,?) WHERE hold_id=?''',
                        ('authorized' if ready else 'awaiting_card', intent.get('capture_before'),
                         int(time.time()) if ready else None, hold_id))
                    self._journal_deposit(db, hold_id, intent)
                if intent['status'] == 'requires_capture' and not ready:
                    reason = ('Für die Kaution ist eine Kreditkarte erforderlich' if intent.get('card_funding') != 'credit'
                              else 'Kartenautorisierung reicht nicht bis nach der Rückgabe')
                    self._release_unusable_authorization(hold_id, reason)
                if intent['status'] == 'canceled':
                    self._release_unusable_authorization(hold_id, 'Kartenautorisierung wurde beendet')
                return {**intent, 'ready': ready}
            # Monitoring a paid rental is read-only. It must never extend the
            # booking window, create another intent, or release a card hold.
            if not ready:
                raise ValueError('Bestätigte Buchung ohne gültige Kreditkartenreservierung; Admin-Prüfung erforderlich.')
            return {**intent, 'ready': True}
        expired_after_confirmation = False
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            latest = dict(db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone())
            if (current['status'] != 'pending' or latest['intent_id'] != intent['id']
                    or latest['status'] in {'releasing', 'released', 'review'}):
                raise ValueError('Kartenautorisierung wurde zwischenzeitlich geändert.')
            if ready and latest['authorized_at'] is None:
                now = int(time.time())
                if current['expires_at'] <= now:
                    expired_after_confirmation = True
                elif not current['session_id'] and not db.execute(
                        'SELECT hold_id FROM miet_checkout_creation_attempts WHERE hold_id=?', (hold_id,)).fetchone():
                    db.execute('UPDATE miet_checkout_holds SET expires_at=? WHERE id=?',
                               (max(current['expires_at'], now + 3600), hold_id))
            db.execute('''UPDATE miet_checkout_deposit_auths
                SET status=?,capture_before=?,authorized_at=COALESCE(authorized_at,?) WHERE hold_id=?''',
                ('authorized' if ready else 'awaiting_card', intent.get('capture_before'),
                 int(time.time()) if ready else None, hold_id))
            self._journal_deposit(db, hold_id, intent)
        if expired_after_confirmation:
            self._release_unusable_authorization(hold_id, 'Reservierungsfrist ist abgelaufen')
        if intent['status'] == 'requires_capture' and not ready:
            reason = ('Für die Kaution ist eine Kreditkarte erforderlich' if intent.get('card_funding') != 'credit'
                      else 'Kartenautorisierung reicht nicht bis nach der Rückgabe')
            self._release_unusable_authorization(hold_id, reason)
        if intent['status'] == 'canceled':
            self._release_unusable_authorization(hold_id, 'Kartenautorisierung wurde beendet')
        return {**intent, 'ready': ready}

    def deposit_ready(self, hold_id):
        return bool(self.reconcile_deposit(hold_id)['ready'])

    def release_deposit(self, hold_id, reason):
        """Cancel the unused authorization; never capture it or silently drop unknowns."""
        h = self.read(hold_id)
        q = json.loads(h['payload'])['quote']
        if not _card_authorization_quote(q):
            return 'not_applicable'
        record = self._deposit_record(hold_id)
        if not record:
            return 'not_created'
        if record['status'] == 'released':
            return 'released'
        if not record['intent_id']:
            raise ValueError('Ausgang der Kartenautorisierung unklar; Bestand bleibt gesperrt.')
        if record['status'] == 'review':
            raise ValueError('Kartenautorisierung muss manuell geprüft werden.')
        intent = self.gateway.retrieve_deposit_intent(record['intent_id'])
        self._validated_deposit(h, record, intent, enforce_coverage=False)
        if intent['status'] != 'canceled':
            with self.locked(h['mietfahrzeug_id']) as (db, _):
                current = db.execute('SELECT status,mietvorgang_id,payload FROM miet_checkout_holds WHERE id=?',
                                     (hold_id,)).fetchone()
                if not current:
                    raise ValueError('Buchung für Kartenfreigabe fehlt.')
                current_quote = json.loads(current['payload'])['quote']
                if current_quote.get('test_only') is False:
                    handover = db.execute('SELECT hold_id FROM miet_checkout_handovers WHERE hold_id=?',
                                          (hold_id,)).fetchone()
                    if handover:
                        from mos_return import deposit_release_eligible
                        if not deposit_release_eligible(db, hold_id, current['mietvorgang_id']):
                            raise ValueError('Kartenreservierung nach Schlüsselübergabe nur nach dokumentierter unbeanstandeter oder geklärter Rückgabe freigeben.')
                db.execute("UPDATE miet_checkout_deposit_auths SET status='releasing',reason=? WHERE hold_id=? AND status!='released'",
                           (str(reason)[:200], hold_id))
                attempt = self._journal_deposit(db, hold_id, intent)
            intent = self.gateway.cancel_deposit_intent(record['intent_id'],
                self._deposit_operation_key(h['id'], attempt['generation'], release=True))
            self._validated_deposit(h, record, intent, enforce_coverage=False)
        if intent['status'] != 'canceled':
            raise ValueError('Kaution nicht sicher freigegeben; manuelle Prüfung erforderlich.')
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            db.execute('''UPDATE miet_checkout_deposit_auths SET status='released',capture_before=NULL,reason=?
                WHERE hold_id=? AND intent_id=?''', (str(reason)[:200], hold_id, record['intent_id']))
            self._journal_deposit(db, hold_id, intent)
        return 'released'

    def create_checkout(self, hold_id):
        if not self._new_bookings_enabled():
            raise ValueError('Gemeinsamer Test-Checkout ist deaktiviert.')
        h = self.read(hold_id)
        if h['status'] != 'pending':
            raise ValueError('Reservierung nicht offen.')
        q = json.loads(h['payload'])['quote']
        if is_long_rental_paid_deposit(q):
            validate_long_rental_paid_deposit(q)
            from mos_public_contract import signed_payload
            signed_payload(json.loads(h['payload']))
            if _pickup_passed(q):
                self.cancel_or_reconcile(hold_id, cancel=True)
                raise ValueError('Der Abholtermin ist verstrichen. Bitte neu buchen.')
        if _card_authorization_quote(q):
            from mos_public_contract import signed_payload
            signed_payload(json.loads(h['payload']))
            if _pickup_passed(q):
                if h['session_id']:
                    self.cancel_or_reconcile(hold_id, cancel=True)
                    raise ValueError('Der Abholtermin ist verstrichen. Bitte neu buchen.')
                self._release_unusable_authorization(hold_id, 'Der Abholtermin ist verstrichen')
            if not deferred_deposit_quote(q) and not self.deposit_ready(hold_id):
                raise ValueError('Die 500 EUR Kaution sind noch nicht vollständig auf der Karte reserviert.')
            # A newly confirmed card authorization can extend the unpaid hold.
            # Use the persisted deadline for Stripe's idempotent Checkout request;
            # a retry after a lost response must send identical parameters.
            h = self.read(hold_id)
            if h['status'] != 'pending':
                raise ValueError('Reservierung nicht mehr offen.')
        if h['session_id']:
            with self.locked(h['mietfahrzeug_id']) as (db, _):
                _require_open_public_slots(db, q, self.p.USE_POSTGRES)
                if not self.p.mietfahrzeug_zeitraum_frei_db(
                        db,h['mietfahrzeug_id'],date.fromisoformat(h['start_datum']),
                        date.fromisoformat(h['end_datum']),exclude_hold_id=h['id']):
                    raise ValueError('Fahrzeug ist für neue Zahlungen derzeit nicht freigegeben.')
            return self.gateway.retrieve(h['session_id'])
        if h['expires_at'] - int(time.time()) < 1860:
            raise ValueError('Unklarer Checkout muss geprüft werden; Reservierung bleibt gesperrt.')
        # Fixed loopback URLs: no customer-facing or live endpoint is installed.
        return_url = (self.return_url_base.rstrip('/') + '/' + h['id']) if self.return_url_base else 'http://127.0.0.1:5084/'
        params = {'mode':'payment','payment_method_types':['card'], 'locale':'de',
                  'client_reference_id':h['id'], 'metadata':{'hold_id':h['id'],'quote_hash':h['fingerprint']},
                  'expires_at':h['expires_at'],
                  'success_url':return_url, 'cancel_url':return_url,
                  'line_items':[{'quantity':1,'price_data':{'currency':'eur','unit_amount':q['amount_cents'],
                     'product_data':{'name':'TEST Mietwagenreservierung – keine echte Zahlung'}}}]}
        if self.gateway.livemode or q.get('checkout_deposit') is True or _card_authorization_quote(q):
            description, submit_message = _checkout_disclosure(q, test_mode=not self.gateway.livemode)
            params['submit_type']='pay'
            params['line_items']=[{'quantity':1,'price_data':{'currency':'eur','unit_amount':q['rental_cents'],
                'product_data':{'name':q['vehicle_name']+' – '+str(q['days'])+' Miettag(e), inkl. MwSt.','description':description}}}]
            if not _card_authorization_quote(q):
                params['line_items'].append({'quantity':1,'price_data':{'currency':'eur','unit_amount':q['deposit_charged_cents'],
                    'product_data':{'name':'Rückzahlbare Kaution','description':'Gesonderte Sicherheitsleistung, Abrechnung nach Rückgabe gemäß Mietbedingungen.'}}})
            params['custom_text']={'submit':{'message':submit_message}}
        session_to_retrieve = None
        pickup_passed_before_provider = False
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            if current['status'] != 'pending':
                raise ValueError('Reservierung nicht mehr offen.')
            pickup_passed_before_provider = (_card_authorization_quote(q) or is_long_rental_paid_deposit(q)) and _pickup_passed(q)
            if not pickup_passed_before_provider:
                _require_open_public_slots(db, q, self.p.USE_POSTGRES)
                if not self.p.mietfahrzeug_zeitraum_frei_db(
                        db,h['mietfahrzeug_id'],date.fromisoformat(current['start_datum']),
                        date.fromisoformat(current['end_datum']),exclude_hold_id=hold_id):
                    raise ValueError('Fahrzeug ist für neue Zahlungen derzeit nicht freigegeben.')
                if _card_authorization_quote(q) and not deferred_deposit_quote(q):
                    deposit = db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                         (hold_id,)).fetchone()
                    if not deposit or deposit['status'] != 'authorized':
                        raise ValueError('Kartenautorisierung wurde zwischenzeitlich geändert.')
                if current['session_id']:
                    session_to_retrieve = current['session_id']
                else:
                    attempt = db.execute('SELECT created_at FROM miet_checkout_creation_attempts WHERE hold_id=?',
                                         (hold_id,)).fetchone()
                    if attempt and int(time.time()) - attempt['created_at'] >= 23 * 3600:
                        raise ValueError('Unklarer Mietpreis-Checkout muss manuell abgeglichen werden.')
                    if not attempt:
                        db.execute('''INSERT INTO miet_checkout_creation_attempts (hold_id,created_at)
                            VALUES (?,?) RETURNING hold_id''', (hold_id, int(time.time()))).fetchone()
        if pickup_passed_before_provider:
            if current['session_id']:
                self.cancel_or_reconcile(hold_id, cancel=True)
                raise ValueError('Der Abholtermin ist verstrichen. Bitte neu buchen.')
            self._release_unusable_authorization(hold_id, 'Der Abholtermin ist verstrichen')
        if session_to_retrieve:
            return self.gateway.retrieve(session_to_retrieve)
        # A timeout leaves the hold intact. Retrying uses exactly the same parameters/key.
        session = self.gateway.create(params, 'shared-hold-'+h['id'])
        self.validate(h, session)
        checkout_needs_review = False
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?',(hold_id,)).fetchone())
            self.validate(current, session)
            if current['status'] == 'pending':
                review_reason = None
                try:
                    _require_open_public_slots(db, q, self.p.USE_POSTGRES)
                except ValueError:
                    review_reason = 'slot_closed_review'
                if (not review_reason and not self.p.mietfahrzeug_zeitraum_frei_db(
                        db,h['mietfahrzeug_id'],date.fromisoformat(current['start_datum']),
                        date.fromisoformat(current['end_datum']),exclude_hold_id=hold_id)):
                    review_reason = 'vehicle_unavailable_review'
                checkout_needs_review = bool(review_reason)
                db.execute('''UPDATE miet_checkout_holds SET session_id=?,status=?,grund=? WHERE id=?''',
                           (session['id'], 'review' if checkout_needs_review else 'pending',
                            review_reason if checkout_needs_review else current['grund'], hold_id))
            elif current['session_id'] != session['id']:
                # Keep the provider ID for reconciliation if an admin closed the
                # slot while the create request was in flight.
                checkout_needs_review = True
                db.execute('''UPDATE miet_checkout_holds SET session_id=?,status='review',grund=? WHERE id=?''',
                           (session['id'], 'checkout_created_after_release_review', hold_id))
            elif current['status'] == 'review':
                checkout_needs_review = True
        if checkout_needs_review:
            try:
                if session.get('status') == 'open':
                    session = self.gateway.expire(session['id'])
                if session.get('status') == 'expired' and session.get('payment_status') == 'unpaid':
                    self.release_deposit(hold_id, 'Übergabetermin geschlossen')
            except Exception:
                self.p.app.logger.exception('MOS Checkout nach Terminschließung muss manuell abgeglichen werden')
            raise ValueError('Der Übergabetermin ist nicht mehr geöffnet. Zahlungsstatus wird geprüft.')
        if (_card_authorization_quote(q) or is_long_rental_paid_deposit(q)) and _pickup_passed(q):
            self.cancel_or_reconcile(hold_id, cancel=True)
            raise ValueError('Der Abholtermin ist verstrichen. Bitte neu buchen.')
        return session

    def validate(self, h, session):
        q = json.loads(h['payload'])['quote']
        if is_onsite_quote(q):
            raise ValueError('Für diese Vor-Ort-Buchung gibt es keine Onlinezahlung.')
        expected_live = self.gateway.livemode
        if (session.get('livemode') is not expected_live or session.get('mode') != 'payment'
            or not str(session.get('id','')).startswith('cs_live_' if expected_live else 'cs_test_')
            or (h['session_id'] and h['session_id'] != session['id'])
            or session.get('client_reference_id') != h['id']
            or session.get('metadata',{}).get('hold_id') != h['id']
            or session.get('metadata',{}).get('quote_hash') != h['fingerprint']
            or session.get('currency') != q['currency'] or session.get('amount_total') != q['amount_cents']):
            raise ValueError('Checkout stimmt nicht mit der Reservierung überein.')

    def handle_signed_event(self, body, signature, secret):
        from mos_booking.gateway import verified_event
        event = verified_event(body, signature, secret)
        kinds = {'checkout.session.completed','checkout.session.expired',
                 'checkout.session.async_payment_succeeded','checkout.session.async_payment_failed'}
        if event.get('livemode') is not self.gateway.livemode or event.get('type') not in kinds:
            raise ValueError('Kein unterstütztes Testereignis.')
        obj = event['data']['object']
        # Always reload authoritative provider state, regardless of event order.
        session = self.gateway.retrieve(obj['id'])
        h = self.read(session['metadata']['hold_id'])
        self.validate(h, session)
        q = json.loads(h['payload'])['quote']
        deposit_ok = True
        if _card_authorization_quote(q) and not deferred_deposit_quote(q):
            if session.get('status') == 'complete' and session.get('payment_status') == 'paid':
                try:
                    deposit_ok = self.deposit_ready(h['id'])
                except ValueError:
                    deposit_ok = False
            elif session.get('status') == 'expired' and session.get('payment_status') == 'unpaid':
                # A canceled/expired rent checkout cannot release inventory while
                # the separate card authorization is still active or uncertain.
                self.release_deposit(h['id'], 'Mietpreis-Checkout abgelaufen')
        with self.locked(h['mietfahrzeug_id']) as (db, vehicle):
            h = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?',(h['id'],)).fetchone())
            self.validate(h, session)
            new_bookings_enabled = self._new_bookings_enabled()
            old = db.execute('SELECT * FROM miet_checkout_events WHERE id=?',(event['id'],)).fetchone()
            if old:
                if (old['hold_id'],old['session_id'],old['kind']) != (h['id'],session['id'],event['type']):
                    raise ValueError('Ereignisreferenz kollidiert.')
                if (not new_bookings_enabled and not h['mietvorgang_id'] and h['status'] == 'pending'
                        and session.get('status') == 'complete' and session.get('payment_status') == 'paid'):
                    self._record_paused_paid_review(db, h, session)
                return h['mietvorgang_id']
            slots_open = True
            if (h['status'] == 'pending' and not h['mietvorgang_id']
                    and session.get('status') == 'complete' and session.get('payment_status') == 'paid'):
                # Lock the selected slots before changing this hold. An admin
                # may close a slot after the customer reaches Stripe but before
                # the follow-up cleanup marks pending holds for review.
                try:
                    _require_open_public_slots(db, q, self.p.USE_POSTGRES)
                except ValueError:
                    slots_open = False
            db.execute('INSERT INTO miet_checkout_events (id,hold_id,session_id,kind) VALUES (?,?,?,?)',
                       (event['id'],h['id'],session['id'],event['type']))
            db.execute('UPDATE miet_checkout_holds SET session_id=? WHERE id=?',(session['id'],h['id']))
            if (self.gateway.livemode and q.get('test_only') is False
                    and session.get('status') == 'complete' and session.get('payment_status') == 'paid'):
                from mos_order_receipt import enqueue as enqueue_order_receipt
                enqueue_order_receipt(db, h, session, source_event_id=event['id'])
            if h['mietvorgang_id']:
                return h['mietvorgang_id']  # Never recreate a returned/cancelled rental.
            if h['status'] == 'released' and h['grund'] == 'review_full_refund_completed':
                # An already verified full refund and separate card release are
                # terminal. A later signed paid event must not reopen the case.
                return None
            if h['status'] == 'review':
                return None  # A manual-review case cannot silently confirm on a later event.
            if session.get('status') == 'complete' and session.get('payment_status') == 'paid':
                if not new_bookings_enabled:
                    # Stripe may finish a session created before new bookings
                    # were paused. Keep payment for manual review, not a rental.
                    self._record_paused_paid_review(db, h, session)
                    return None
                pi = session.get('payment_intent')
                duplicate = not isinstance(pi,str) or not pi.startswith('pi_') or db.execute(
                    'SELECT id FROM miet_checkout_holds WHERE payment_intent=? AND id!=?',(pi,h['id'])).fetchone()
                start, end = date.fromisoformat(h['start_datum']), date.fromisoformat(h['end_datum'])
                pickup_passed = (_card_authorization_quote(q) or is_long_rental_paid_deposit(q)) and _pickup_passed(q)
                conflict = (pickup_passed or not slots_open or not deposit_ok or h['status'] == 'released' or duplicate or not int(vehicle['aktiv'] or 0)
                    or self.p.normalize_mietfahrzeug_status(vehicle['status']) in {'bald','wartung','inaktiv'}
                    or not self.p.mietfahrzeug_zeitraum_frei_db(db,h['mietfahrzeug_id'],start,end,exclude_hold_id=h['id']))
                if conflict:
                    reason = ('paid_pickup_passed_review' if pickup_passed else
                              'paid_slot_closed_review' if not slots_open else
                              'paid_deposit_needs_review' if not deposit_ok else 'paid_needs_review')
                    db.execute("UPDATE miet_checkout_holds SET status='review',grund=? WHERE id=?",(reason,h['id']))
                    return None
                c = json.loads(h['payload'])['customer']
                note = ('Stripe TEST Checkout ' if q.get('test_only') else 'Stripe Checkout ') + h['id']
                rental_id = self._insert_rental(db, h['mietfahrzeug_id'], start, end, c['name'], c, note)
                db.execute("UPDATE miet_checkout_holds SET status='confirmed',payment_intent=?,mietvorgang_id=? WHERE id=?",(pi,rental_id,h['id']))
                mos_operator_notice.enqueue_confirmation(db, h['id'], q, rental_id, c['name'])
                return rental_id
            if session.get('status') == 'expired' and session.get('payment_status') == 'unpaid' and h['status'] != 'review':
                deposit = db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                     (h['id'],)).fetchone() if _card_authorization_quote(q) else None
                if (not _card_authorization_quote(q)
                        or deferred_deposit_quote(q) and not deposit
                        or deposit and deposit['status'] == 'released'):
                    db.execute("UPDATE miet_checkout_holds SET status='released' WHERE id=?",(h['id'],))
            return None

    def cancel_or_reconcile(self, hold_id, cancel=False):
        h = self.read(hold_id)
        if h['status'] != 'pending':
            return
        new_bookings_enabled = self._new_bookings_enabled()
        q = json.loads(h['payload'])['quote']
        if not h['session_id']:
            if ((_card_authorization_quote(q) or is_long_rental_paid_deposit(q))
                    and (cancel or not new_bookings_enabled or h['expires_at'] <= int(time.time()) or _pickup_passed(q))):
                self._begin_release_without_session(h, 'Buchung vor Mietpreiszahlung abgebrochen')
                self.release_deposit(h['id'], 'Buchung vor Mietpreiszahlung abgebrochen')
                with self.locked(h['mietfahrzeug_id']) as (db, _):
                    current = db.execute('SELECT status,session_id FROM miet_checkout_holds WHERE id=?',
                                         (h['id'],)).fetchone()
                    deposit = db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                         (h['id'],)).fetchone()
                    uncertain = db.execute('SELECT hold_id FROM miet_checkout_creation_attempts WHERE hold_id=?',
                                           (h['id'],)).fetchone()
                    if (current['status'] == 'pending' and not current['session_id']
                            and not uncertain and (not deposit or deposit['status'] == 'released')):
                        db.execute("UPDATE miet_checkout_holds SET status='released' WHERE id=?",(h['id'],))
            return  # Unknown creation outcome never frees stock.
        session = self.gateway.retrieve(h['session_id'])
        if (cancel or not new_bookings_enabled or (_card_authorization_quote(q) or is_long_rental_paid_deposit(q)) and _pickup_passed(q)) and session.get('status') == 'open':
            session = self.gateway.expire(h['session_id'])
        self.validate(h, session)
        if (session.get('status') == 'expired' and session.get('payment_status') == 'unpaid'
                and _card_authorization_quote(q)):
            self.release_deposit(h['id'], 'Mietpreis-Checkout beendet')
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            if session.get('status') == 'expired' and session.get('payment_status') == 'unpaid':
                deposit = db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                     (h['id'],)).fetchone() if _card_authorization_quote(q) else None
                if (not _card_authorization_quote(q)
                        or deferred_deposit_quote(q) and not deposit
                        or deposit and deposit['status'] == 'released'):
                    db.execute("UPDATE miet_checkout_holds SET status='released' WHERE id=? AND status='pending'",(h['id'],))
        # Paid sessions wait for a signed webhook, including a cancellation/payment race.
