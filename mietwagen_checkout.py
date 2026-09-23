"""Shared portal inventory for TEST Checkout. No HTTP route or live payment support.

All writers lock mietfahrzeuge first. Holds remain visible when creation is disabled.
Only trusted server code may supply a quote/customer; never forward browser prices.
"""
from contextlib import contextmanager
from datetime import date
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
        fingerprint = hashlib.sha256(json.dumps([int(vehicle_id),start.isoformat(),end.isoformat(),payload]).encode()).hexdigest()
        with self.locked(vehicle_id) as (db, vehicle):
            existing = db.execute('SELECT * FROM miet_checkout_holds WHERE request_key=?', (request_key,)).fetchone()
            if existing:
                if existing['fingerprint'] != fingerprint:
                    raise ValueError('Idempotenzreferenz mit anderen Angaben verwendet.')
                return dict(existing)
            if not int(vehicle['aktiv'] or 0) or self.p.normalize_mietfahrzeug_status(vehicle['status']) in {'bald','wartung','inaktiv'}:
                raise ValueError('Fahrzeug nicht freigegeben.')
            if not self.p.mietfahrzeug_zeitraum_frei_db(db, vehicle_id, start, end):
                raise ValueError('Zeitraum bereits belegt.')
            hold_id = secrets.token_urlsafe(24)
            db.execute('''INSERT INTO miet_checkout_holds
                (id,request_key,mietfahrzeug_id,start_datum,end_datum,payload,fingerprint,status,expires_at)
                VALUES (?,?,?,?,?,?,?,'pending',?)''',
                (hold_id,request_key,int(vehicle_id),start.isoformat(),end.isoformat(),payload,fingerprint,int(time.time())+2100))
            return dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (hold_id,)).fetchone())

    def create_checkout(self, hold_id):
        if not self.p.app.config.get('MOS_SHARED_CHECKOUT_ENABLED', False):
            raise ValueError('Gemeinsamer Test-Checkout ist deaktiviert.')
        h = self.read(hold_id)
        if h['status'] != 'pending':
            raise ValueError('Reservierung nicht offen.')
        if h['session_id']:
            return self.gateway.retrieve(h['session_id'])
        if h['expires_at'] - int(time.time()) < 1860:
            raise ValueError('Unklarer Checkout muss geprüft werden; Reservierung bleibt gesperrt.')
        q = json.loads(h['payload'])['quote']
        # Fixed loopback URLs: no customer-facing or live endpoint is installed.
        return_url = (self.return_url_base.rstrip('/') + '/' + h['id']) if self.return_url_base else 'http://127.0.0.1:5084/'
        params = {'mode':'payment','payment_method_types':['card'], 'locale':'de',
                  'client_reference_id':h['id'], 'metadata':{'hold_id':h['id'],'quote_hash':h['fingerprint']},
                  'expires_at':h['expires_at'],
                  'success_url':return_url, 'cancel_url':return_url,
                  'line_items':[{'quantity':1,'price_data':{'currency':'eur','unit_amount':q['amount_cents'],
                     'product_data':{'name':'TEST Mietwagenreservierung – keine echte Zahlung'}}}]}
        if self.gateway.livemode or q.get('checkout_deposit') is True:
            description=(f"{q['start_slot']} bis {q['end_slot']}; persönliche Übergabe Gärtner, Binauer Höhe 4, Mosbach-Lohrbach. "
                         f"{q['included_km']} km inklusive; weitere Kilometer {q['extra_km_cents']/100:.2f} EUR/km. "
                         "Vertragliche Selbstbeteiligung 1.000 EUR gemäß vereinbarten Bedingungen.")
            params['submit_type']='pay'
            params['line_items']=[{'quantity':1,'price_data':{'currency':'eur','unit_amount':q['rental_cents'],
                'product_data':{'name':q['vehicle_name']+' – '+str(q['days'])+' Miettag(e), inkl. MwSt.','description':description}}},
                {'quantity':1,'price_data':{'currency':'eur','unit_amount':q['deposit_charged_cents'],
                'product_data':{'name':'Rückzahlbare Kaution','description':'Gesonderte Sicherheitsleistung, Abrechnung nach Rückgabe gemäß Mietbedingungen.'}}}]
            params['custom_text']={'submit':{'message':('TEST – keine echte Zahlung. ' if not self.gateway.livemode else '')+'Sie buchen zahlungspflichtig. Mietpreis und rückzahlbare Kaution sind getrennt ausgewiesen. Es gelten die vorab bestätigten Mietbedingungen.'}}
        # A timeout leaves the hold intact. Retrying uses exactly the same parameters/key.
        session = self.gateway.create(params, 'shared-hold-'+h['id'])
        self.validate(h, session)
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            current = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?',(hold_id,)).fetchone())
            self.validate(current, session)
            db.execute('UPDATE miet_checkout_holds SET session_id=? WHERE id=?',(session['id'],hold_id))
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
                conflict = (h['status'] == 'released' or duplicate or not int(vehicle['aktiv'] or 0)
                    or self.p.normalize_mietfahrzeug_status(vehicle['status']) in {'bald','wartung','inaktiv'}
                    or not self.p.mietfahrzeug_zeitraum_frei_db(db,h['mietfahrzeug_id'],start,end,exclude_hold_id=h['id']))
                if conflict:
                    db.execute("UPDATE miet_checkout_holds SET status='review',grund='paid_needs_review' WHERE id=?",(h['id'],))
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
                db.execute("UPDATE miet_checkout_holds SET status='released' WHERE id=?",(h['id'],))
            return None

    def cancel_or_reconcile(self, hold_id, cancel=False):
        h = self.read(hold_id)
        if h['status'] != 'pending' or not h['session_id']:
            return  # Unknown creation outcome never frees stock.
        session = self.gateway.retrieve(h['session_id'])
        if cancel and session.get('status') == 'open':
            session = self.gateway.expire(h['session_id'])
        self.validate(h, session)
        with self.locked(h['mietfahrzeug_id']) as (db, _):
            if session.get('status') == 'expired' and session.get('payment_status') == 'unpaid':
                db.execute("UPDATE miet_checkout_holds SET status='released' WHERE id=? AND status='pending'",(h['id'],))
        # Paid sessions wait for a signed webhook, including a cancellation/payment race.
