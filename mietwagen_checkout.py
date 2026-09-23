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


def init_schema(db):
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
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_creation_attempts (
        hold_id TEXT PRIMARY KEY, created_at BIGINT NOT NULL)''')


def _card_authorization_quote(quote):
    return quote.get('deposit_method') == 'card_authorization_at_booking'


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

    @contextmanager
    def locked(self, vehicle_id):
        db = self.p.get_db()
        try:
            if not self.p.USE_POSTGRES:
                db.execute('BEGIN IMMEDIATE')
            suffix = ' FOR UPDATE' if self.p.USE_POSTGRES else ''
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
        if not self.p.app.config.get('MOS_SHARED_CHECKOUT_ENABLED', False):
            raise ValueError('Gemeinsamer Test-Checkout ist deaktiviert.')
        start, end = self.p.validiere_mietzeitraum(start, end)
        self.p.validiere_mietkontakt(customer.get('telefon'), customer.get('email'))
        name = self.p.clean_text(customer.get('name'))
        if not name or not isinstance(request_key, str) or not 16 <= len(request_key) <= 200:
            raise ValueError('Kunde oder Idempotenzreferenz fehlt.')
        if (type(quote.get('amount_cents')) is not int or quote['amount_cents'] <= 0
                or quote.get('currency') != 'eur' or not quote.get('rules_version')):
            raise ValueError('Verbindlicher serverseitiger Preis und Regelversion erforderlich.')
        payload = json.dumps({'customer': {'name': name, 'telefon': customer.get('telefon', ''),
                            'email': customer.get('email', '')}, 'quote': quote}, sort_keys=True)
        if _card_authorization_quote(quote):
            if (type(quote.get('rental_cents')) is not int or quote['rental_cents'] <= 0
                    or quote['amount_cents'] != quote['rental_cents']
                    or quote.get('deposit_authorized_cents') != 50000
                    or quote.get('deposit_charged_cents') != 0
                    or quote.get('vehicle_id') != int(vehicle_id)
                    or datetime.fromisoformat(quote['start_slot']).date() != start
                    or datetime.fromisoformat(quote['end_slot']).date() != end):
                raise ValueError('Mietpreis und gesonderte Kartenautorisierung stimmen nicht.')
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
            return dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())

    def _deposit_record(self, hold_id):
        db = self.p.get_db()
        try:
            row = db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone()
            return dict(row) if row else None
        finally:
            db.close()

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

    def prepare_deposit(self, hold_id):
        """Create one manual-capture card intent; uncertain outcomes retain stock."""
        if not self.p.app.config.get('MOS_SHARED_CHECKOUT_ENABLED', False):
            raise ValueError('Gemeinsamer Checkout ist deaktiviert.')
        h = self.read(hold_id)
        q = json.loads(h['payload'])['quote']
        if not _card_authorization_quote(q) or h['status'] != 'pending':
            raise ValueError('Kartenautorisierung für diese Reservierung nicht möglich.')
        if _pickup_passed(q):
            self._release_unusable_authorization(hold_id, 'Der Abholtermin ist verstrichen')
        if h['expires_at'] <= int(time.time()):
            self.cancel_or_reconcile(hold_id)
            raise ValueError('Die Reservierungsfrist ist abgelaufen. Bitte neu buchen.')
        from mos_public_contract import signed_payload
        signed_payload(json.loads(h['payload']))
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
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
            intent = self.gateway.create_deposit_intent(params, 'mos-deposit-' + h['id'])
        ready = self._validated_deposit(h, record, intent, enforce_coverage=False)
        expired_after_confirmation = False
        authorization_needs_review = False
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            latest = dict(db.execute('SELECT * FROM miet_checkout_deposit_auths WHERE hold_id=?', (hold_id,)).fetchone())
            if latest['intent_id'] and latest['intent_id'] != intent['id']:
                raise ValueError('Mehrdeutige Kartenautorisierung; manuelle Prüfung erforderlich.')
            authorization_needs_review = (current['status'] != 'pending'
                                          or latest['status'] in {'releasing', 'released', 'review'})
            if authorization_needs_review and not latest['intent_id']:
                # The admin can close a slot while Stripe is creating an intent.
                # Persist its ID before reporting a manual-review case.
                db.execute('''UPDATE miet_checkout_deposit_auths
                    SET intent_id=?,status='review',capture_before=?,reason=? WHERE hold_id=?''',
                    (intent['id'], intent.get('capture_before'), 'authorization_created_after_close', hold_id))
            if not authorization_needs_review and ready and latest['authorized_at'] is None:
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
                db.execute("UPDATE miet_checkout_deposit_auths SET status='releasing',reason=? WHERE hold_id=? AND status!='released'",
                           (str(reason)[:200], hold_id))
            intent = self.gateway.cancel_deposit_intent(record['intent_id'], 'mos-deposit-release-' + h['id'])
            self._validated_deposit(h, record, intent, enforce_coverage=False)
        if intent['status'] != 'canceled':
            raise ValueError('Kaution nicht sicher freigegeben; manuelle Prüfung erforderlich.')
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            db.execute('''UPDATE miet_checkout_deposit_auths SET status='released',capture_before=NULL,reason=?
                WHERE hold_id=? AND intent_id=?''', (str(reason)[:200], hold_id, record['intent_id']))
        return 'released'

    def create_checkout(self, hold_id):
        if not self.p.app.config.get('MOS_SHARED_CHECKOUT_ENABLED', False):
            raise ValueError('Gemeinsamer Test-Checkout ist deaktiviert.')
        h = self.read(hold_id)
        if h['status'] != 'pending':
            raise ValueError('Reservierung nicht offen.')
        q = json.loads(h['payload'])['quote']
        if _card_authorization_quote(q):
            from mos_public_contract import signed_payload
            signed_payload(json.loads(h['payload']))
            if _pickup_passed(q):
                if h['session_id']:
                    self.cancel_or_reconcile(hold_id, cancel=True)
                    raise ValueError('Der Abholtermin ist verstrichen. Bitte neu buchen.')
                self._release_unusable_authorization(hold_id, 'Der Abholtermin ist verstrichen')
            if not self.deposit_ready(hold_id):
                raise ValueError('Die 500 EUR Kaution sind noch nicht vollständig auf der Karte reserviert.')
        if h['session_id']:
            with self.locked(h['mietfahrzeug_id']) as (db, _):
                _require_open_public_slots(db, q, self.p.USE_POSTGRES)
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
            description=(f"{q['start_slot']} bis {q['end_slot']}; persönliche Übergabe Gärtner, Binauer Höhe 4, Mosbach-Lohrbach. "
                         f"{q['included_km']} km inklusive; weitere Kilometer {q['extra_km_cents']/100:.2f} EUR/km. "
                         "Vertragliche Selbstbeteiligung 1.000 EUR gemäß vereinbarten Bedingungen.")
            params['submit_type']='pay'
            params['line_items']=[{'quantity':1,'price_data':{'currency':'eur','unit_amount':q['rental_cents'],
                'product_data':{'name':q['vehicle_name']+' – '+str(q['days'])+' Miettag(e), inkl. MwSt.','description':description}}}]
            if not _card_authorization_quote(q):
                params['line_items'].append({'quantity':1,'price_data':{'currency':'eur','unit_amount':q['deposit_charged_cents'],
                    'product_data':{'name':'Rückzahlbare Kaution','description':'Gesonderte Sicherheitsleistung, Abrechnung nach Rückgabe gemäß Mietbedingungen.'}}})
            deposit_message = ('Die 500 EUR Kaution wurden separat auf Ihrer Kreditkarte reserviert und werden nicht abgebucht. '
                               if _card_authorization_quote(q) else
                               'Mietpreis und rückzahlbare Kaution sind getrennt ausgewiesen. ')
            params['custom_text']={'submit':{'message':('TEST – keine echte Zahlung. ' if not self.gateway.livemode else '')+
                'Sie buchen zahlungspflichtig. '+deposit_message+'Es gelten die vorab bestätigten Mietbedingungen.'}}
        session_to_retrieve = None
        pickup_passed_before_provider = False
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())
            if current['status'] != 'pending':
                raise ValueError('Reservierung nicht mehr offen.')
            pickup_passed_before_provider = _card_authorization_quote(q) and _pickup_passed(q)
            if not pickup_passed_before_provider:
                _require_open_public_slots(db, q, self.p.USE_POSTGRES)
                if _card_authorization_quote(q):
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
                try:
                    _require_open_public_slots(db, q, self.p.USE_POSTGRES)
                except ValueError:
                    checkout_needs_review = True
                db.execute('''UPDATE miet_checkout_holds SET session_id=?,status=?,grund=? WHERE id=?''',
                           (session['id'], 'review' if checkout_needs_review else 'pending',
                            'slot_closed_review' if checkout_needs_review else current['grund'], hold_id))
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
        if _card_authorization_quote(q) and _pickup_passed(q):
            self.cancel_or_reconcile(hold_id, cancel=True)
            raise ValueError('Der Abholtermin ist verstrichen. Bitte neu buchen.')
        return session

    def validate(self, h, session):
        q = json.loads(h['payload'])['quote']
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
        if _card_authorization_quote(q):
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
            old = db.execute('SELECT * FROM miet_checkout_events WHERE id=?',(event['id'],)).fetchone()
            if old:
                if (old['hold_id'],old['session_id'],old['kind']) != (h['id'],session['id'],event['type']):
                    raise ValueError('Ereignisreferenz kollidiert.')
                return h['mietvorgang_id']
            db.execute('INSERT INTO miet_checkout_events (id,hold_id,session_id,kind) VALUES (?,?,?,?)',
                       (event['id'],h['id'],session['id'],event['type']))
            db.execute('UPDATE miet_checkout_holds SET session_id=? WHERE id=?',(session['id'],h['id']))
            if h['mietvorgang_id']:
                return h['mietvorgang_id']  # Never recreate a returned/cancelled rental.
            if h['status'] == 'review':
                return None  # A manual-review case cannot silently confirm on a later event.
            if session.get('status') == 'complete' and session.get('payment_status') == 'paid':
                pi = session.get('payment_intent')
                duplicate = not isinstance(pi,str) or not pi.startswith('pi_') or db.execute(
                    'SELECT id FROM miet_checkout_holds WHERE payment_intent=? AND id!=?',(pi,h['id'])).fetchone()
                start, end = date.fromisoformat(h['start_datum']), date.fromisoformat(h['end_datum'])
                pickup_passed = _card_authorization_quote(q) and _pickup_passed(q)
                conflict = (pickup_passed or not deposit_ok or h['status'] == 'released' or duplicate or not int(vehicle['aktiv'] or 0)
                    or self.p.normalize_mietfahrzeug_status(vehicle['status']) in {'bald','wartung','inaktiv'}
                    or not self.p.mietfahrzeug_zeitraum_frei_db(db,h['mietfahrzeug_id'],start,end,exclude_hold_id=h['id']))
                if conflict:
                    reason = ('paid_pickup_passed_review' if pickup_passed else
                              'paid_deposit_needs_review' if not deposit_ok else 'paid_needs_review')
                    db.execute("UPDATE miet_checkout_holds SET status='review',grund=? WHERE id=?",(reason,h['id']))
                    return None
                c = json.loads(h['payload'])['customer']
                now = self.p.now_str()
                cur = db.execute('''INSERT INTO mietvorgaenge
                    (mietfahrzeug_id,auftrag_id,kunde_name,kunde_telefon,kunde_email,whatsapp_erlaubt,
                     start_datum,end_datum,rueckgabe_datum,status,notiz,erstellt_am,geaendert_am)
                    VALUES (?,0,?,?,?,0,?,?,'','aktiv',?,?,?)''',
                    (h['mietfahrzeug_id'],c['name'],c['telefon'],c['email'],start.strftime(self.p.DATE_FMT),
                     end.strftime(self.p.DATE_FMT),'Stripe TEST Checkout '+h['id'],now,now))
                rental_id = cur.lastrowid
                db.execute("UPDATE miet_checkout_holds SET status='confirmed',payment_intent=?,mietvorgang_id=? WHERE id=?",(pi,rental_id,h['id']))
                return rental_id
            if session.get('status') == 'expired' and session.get('payment_status') == 'unpaid' and h['status'] != 'review':
                deposit = db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                     (h['id'],)).fetchone() if _card_authorization_quote(q) else None
                if not _card_authorization_quote(q) or (deposit and deposit['status'] == 'released'):
                    db.execute("UPDATE miet_checkout_holds SET status='released' WHERE id=?",(h['id'],))
            return None

    def cancel_or_reconcile(self, hold_id, cancel=False):
        h = self.read(hold_id)
        if h['status'] != 'pending':
            return
        q = json.loads(h['payload'])['quote']
        if not h['session_id']:
            if (_card_authorization_quote(q)
                    and (cancel or h['expires_at'] <= int(time.time()) or _pickup_passed(q))):
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
        if (cancel or _card_authorization_quote(q) and _pickup_passed(q)) and session.get('status') == 'open':
            session = self.gateway.expire(h['session_id'])
        self.validate(h, session)
        if (session.get('status') == 'expired' and session.get('payment_status') == 'unpaid'
                and _card_authorization_quote(q)):
            self.release_deposit(h['id'], 'Mietpreis-Checkout beendet')
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            if session.get('status') == 'expired' and session.get('payment_status') == 'unpaid':
                deposit = db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                                     (h['id'],)).fetchone() if _card_authorization_quote(q) else None
                if not _card_authorization_quote(q) or (deposit and deposit['status'] == 'released'):
                    db.execute("UPDATE miet_checkout_holds SET status='released' WHERE id=? AND status='pending'",(h['id'],))
        # Paid sessions wait for a signed webhook, including a cancellation/payment race.
