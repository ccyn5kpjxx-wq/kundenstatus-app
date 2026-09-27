"""Fail-closed launch validation and an idempotent, durable refund ledger.

No network call occurs on import. Runtime enabling requires explicit operator evidence.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import secrets
import stripe
from zoneinfo import ZoneInfo
from .gateway import StripeTestGateway
from mos_public_contract import LESSOR_ADDRESS, LESSOR_NAME, valid_lessor_contact

ACTIVE_LISTINGS={'kona','i10'}
CANCELLATION_POLICY_48H_10_PERCENT='free_48h_then_10pct_rent'
CANCELLATION_POLICY_24H_ONE_DAY='free_24h_then_one_day_rent'
BERLIN=ZoneInfo('Europe/Berlin')


def _is_draft_terms(value):
    """Reject unmistakable editorial markers even if a matching hash was signed off."""
    if not isinstance(value,str):
        return False
    text=value.casefold()
    if re.search(r'(?<![a-z0-9äöüß])(?:entwurf|draft|todo|tbd|platzhalter|prüffassung|prueffassung)(?![a-z0-9äöüß])',text):
        return True
    if 'nicht veröffentlichen' in text or 'nicht veroeffentlichen' in text:
        return True
    return bool(re.search(
        r'\[[^\]]*\b(?:vor veröffentlichung|vor veroeffentlichung|bestimmen|ergänzen|ergaenzen|'
        r'prüfen|pruefen|abstimmen|abnehmen|ersetzen|klären|klaeren|dokumentieren|nachtragen|nachweisen)\b[^\]]*\]',
        text,re.DOTALL))


def launch_errors(cfg):
    errors=[]
    if set(cfg.get('fleet',{})) != ACTIVE_LISTINGS:
        errors.append('Nur die bestätigten Fahrzeuge KONA und i10 dürfen freigegeben werden.')
    launch=cfg.get('launch',{})
    for name in ('business_review','legal_review','finance_review','privacy_review','sandbox_acceptance',
                 'postgres_acceptance','contract_delivery_acceptance','checkout_button_acceptance',
                 'order_receipt_acceptance','webhook_reconcile_acceptance','termination_review'):
        record=launch.get(name,{})
        if not (record.get('approved_by') and record.get('approved_at') and record.get('evidence')):
            errors.append('Freigabenachweis fehlt: '+name)
    legal=launch.get('legal_review',{})
    terms=cfg.get('terms_text')
    if (not isinstance(terms,str) or not terms.strip() or
            legal.get('terms_sha256') != hashlib.sha256(terms.encode('utf-8')).hexdigest() or
            legal.get('terms_version') != cfg.get('terms_version')):
        errors.append('Rechtsfreigabe gehört nicht zur aktuellen Bedingungsfassung.')
    if _is_draft_terms(terms) or _is_draft_terms(cfg.get('terms_version')):
        errors.append('Bedingungstext oder Version enthält Entwurfsmarker.')
    if type(launch.get('termination_review', {}).get('applies')) is not bool:
        errors.append('§-312k-Entscheidung zur Kündigungsstrecke fehlt.')
    for slug in ACTIVE_LISTINGS:
        record=launch.get('insurance',{}).get(slug,{})
        if not (record.get('verified') is True and record.get('use')=='paid_self_drive'
                and record.get('evidence') and record.get('vehicle_id')==cfg.get('fleet',{}).get(slug,{}).get('id')):
            errors.append('Belegter Selbstfahrervermietungsschutz fehlt: '+slug)
        if not cfg.get('fleet',{}).get(slug,{}).get('expected_name'):
            errors.append('Geprüfte Fahrzeugidentität fehlt: '+slug)
    if cfg.get('deposit_method')!='card_authorization_at_booking':
        errors.append('Online-Kartenautorisierung ohne Kautionseinzug ist nicht konfiguriert.')
    if cfg.get('cancellation_policy')!=CANCELLATION_POLICY_24H_ONE_DAY:
        errors.append('Die bestätigte Stornoregel (24 Stunden kostenlos, danach höchstens ein Miettag) fehlt.')
    if not cfg.get('terms_version') or cfg['terms_version'].startswith('draft:'):
        errors.append('Freigegebene Bedingungsversion fehlt.')
    for name in ('terms_text','privacy_url','merchant_name','merchant_address','merchant_email','merchant_phone'):
        if not cfg.get(name):errors.append('Pflichtangabe fehlt: '+name)
    if not valid_lessor_contact(cfg.get('merchant_email'),cfg.get('merchant_phone')):
        errors.append('Gültiger Vermieterkontakt mit E-Mail und Telefon fehlt.')
    if cfg.get('merchant_name') and cfg['merchant_name'] != LESSOR_NAME:
        errors.append('Vermieter muss Gärtner GmbH Karosserie + Lack sein.')
    if cfg.get('merchant_address') and cfg['merchant_address'] != LESSOR_ADDRESS:
        errors.append('Vermieteranschrift stimmt nicht mit dem Impressum überein.')
    if not cfg.get('origin','').startswith('https://'):
        errors.append('Livebetrieb erfordert HTTPS.')
    if cfg.get('live_enabled') is not True:
        errors.append('Live-Aktivierung fehlt.')
    return errors


def settlement_errors(cfg):
    """Allow existing live bookings to settle after new bookings are stopped.

    Insurance and current terms approval govern *new* rentals. Removing either
    approval must not strand an already paid rental or its card authorization.
    The runtime still needs an explicit live settlement mode and provider key.
    """
    errors=[]
    if cfg.get('mode')!='live' or cfg.get('enabled') is not False or cfg.get('live_enabled') is not True:
        errors.append('Nur ausdrücklich deaktivierte Live-Neubuchungen dürfen abgewickelt werden.')
    if cfg.get('deposit_method')!='card_authorization_at_booking':
        errors.append('Bestehende Kreditkarten-Autorisierungen erfordern den aktuellen Kautionsmodus.')
    if not cfg.get('origin','').startswith('https://'):
        errors.append('Bestehende Livebuchungen erfordern HTTPS.')
    return errors


class StripeLiveGateway(StripeTestGateway):
    livemode=True
    def __init__(self,key,cfg,*,settlement_only=False):
        errors=settlement_errors(cfg) if settlement_only else launch_errors(cfg)
        if errors:raise ValueError('; '.join(errors))
        if not key.startswith(('sk_live_','rk_live_')):raise ValueError('Live-Schlüssel fehlt oder falscher Modus.')
        self.settlement_only=settlement_only
        self.client=stripe.StripeClient(key,max_network_retries=2)

    def create(self,params,key):
        if self.settlement_only:
            raise ValueError('Bei deaktivierter Neubuchung darf kein Miet-Checkout erzeugt werden.')
        return super().create(params,key)

    def create_deposit_intent(self,params,key):
        if self.settlement_only:
            raise ValueError('Bei deaktivierter Neubuchung darf keine neue Kautionsautorisierung entstehen.')
        return super().create_deposit_intent(params,key)

    def capture_deposit_intent(self,intent_id,amount_cents,key):
        raise ValueError('Eine MOS-Kautionsautorisierung darf nicht als Zahlung eingezogen werden.')

    def refund(self,payment_intent,amount,key):
        return self.client.v1.refunds.create({'payment_intent':payment_intent,'amount':amount},
                    options={'idempotency_key':key}).to_dict()

    def retrieve_refund(self,refund_id):
        return self.client.v1.refunds.retrieve(refund_id).to_dict()


def init_refund_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_cancellations (
        id TEXT PRIMARY KEY, requested_at TEXT NOT NULL, fee_cents BIGINT NOT NULL,
        credit_cents BIGINT NOT NULL DEFAULT 0, reason TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_refunds (
        id TEXT PRIMARY KEY, hold_id TEXT NOT NULL, amount_cents BIGINT NOT NULL,
        kind TEXT NOT NULL, status TEXT NOT NULL, provider_id TEXT UNIQUE,
        reason TEXT NOT NULL, created_at TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_review_refunds (
        hold_id TEXT PRIMARY KEY, refund_id TEXT NOT NULL UNIQUE,
        session_id TEXT NOT NULL, payment_intent TEXT NOT NULL,
        amount_cents BIGINT NOT NULL, verified_at TEXT NOT NULL,
        operator_name TEXT NOT NULL, reason TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_limits (
        id TEXT PRIMARY KEY, attempts INTEGER NOT NULL)''')


def cancellation_fee(quote,requested_at):
    start=datetime.fromisoformat(quote['start_slot']).astimezone(timezone.utc)
    seconds_before_pickup=(start-requested_at.astimezone(timezone.utc)).total_seconds()
    policy=quote.get('cancellation_policy')
    if policy==CANCELLATION_POLICY_48H_10_PERCENT:
        if seconds_before_pickup>=48*3600:return 0
        rental_cents=quote.get('rental_cents')
        if type(rental_cents) is not int or rental_cents<0:
            raise ValueError('Mietpreis für Stornoberechnung fehlt.')
        # Integer half-up rounding: never include a charged or authorized deposit.
        return (rental_cents+5)//10
    if policy==CANCELLATION_POLICY_24H_ONE_DAY:
        if seconds_before_pickup>=24*3600:return 0
        rental_cents=quote.get('rental_cents')
        daily_cents=quote.get('daily_cents')
        if (type(rental_cents) is not int or rental_cents<0
                or type(daily_cents) is not int or daily_cents<=0):
            raise ValueError('Vereinbarter Tages- oder Gesamtmietpreis für Storno fehlt.')
        # The agreed daily rate already includes a KONA multi-day discount.
        # Never include a charged or authorized deposit or exceed the rent.
        return min(daily_cents,rental_cents)
    if policy is not None:
        raise ValueError('Unbekannte Stornoregel im Buchungssnapshot.')
    # Existing signed bookings without an explicit policy keep their original rule.
    if seconds_before_pickup>=24*3600:return 0
    return min(quote['daily_cents'],quote.get('rental_cents',quote['amount_cents']))


class RefundLedger:
    def __init__(self,service):self.s=service

    def review_full_refund(self,hold_id,operator_name,reason):
        """Queue one full rental refund after an operator verifies a paid Session.

        Browser redirects and review flags are insufficient payment evidence.
        The provider lookup is deliberately outside the vehicle transaction;
        all hold and refund conditions are checked again under its lock.
        """
        operator_name=operator_name.strip() if isinstance(operator_name,str) else ''
        reason=reason.strip() if isinstance(reason,str) else ''
        if not 2<=len(operator_name)<=100 or not 5<=len(reason)<=1000:
            raise ValueError('Prüfende Person und konkrete Begründung für die Vollerstattung angeben.')
        h=self.s.read(hold_id)
        if not h['session_id']:
            raise ValueError('Keine prüfbare Stripe-Zahlungssitzung vorhanden.')
        session=self.s.gateway.retrieve(h['session_id'])
        self.s.validate(h,session)
        q=json.loads(h['payload'])['quote']
        pi=session.get('payment_intent')
        if (h['status']!='review' or h['mietvorgang_id']
                or session.get('object')!='checkout.session'
                or session.get('status')!='complete' or session.get('payment_status')!='paid'
                or not isinstance(pi,str) or not pi.startswith('pi_')
                or h['payment_intent'] not in (None,pi)
                or q.get('deposit_method')!='card_authorization_at_booking'
                or q.get('deposit_charged_cents')!=0
                or q.get('deposit_authorized_cents')!=50000
                or type(q.get('rental_cents')) is not int or q['rental_cents']<=0
                or q.get('amount_cents')!=q['rental_cents']):
            raise ValueError('Prüffall und verifizierte Mietpreiszahlung stimmen nicht überein.')
        rid='review-full-'+hold_id
        with self.s.locked(h['mietfahrzeug_id']) as (db,_):
            current=dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?',
                                    (hold_id,)).fetchone())
            self.s.validate(current,session)
            if current['mietvorgang_id'] or current['status']!='review':
                raise ValueError('Prüffall wurde zwischenzeitlich geändert.')
            if current['payment_intent'] not in (None,pi) or db.execute(
                    'SELECT id FROM miet_checkout_holds WHERE payment_intent=? AND id!=?',
                    (pi,hold_id)).fetchone():
                raise ValueError('Stripe-Zahlung ist einem anderen Fall zugeordnet.')
            old=db.execute('SELECT * FROM miet_checkout_review_refunds WHERE hold_id=?',
                           (hold_id,)).fetchone()
            if old:
                if (old['refund_id']!=rid or old['session_id']!=session['id']
                        or old['payment_intent']!=pi or old['amount_cents']!=q['rental_cents']):
                    raise ValueError('Prüffall hat abweichenden Erstattungsauftrag.')
                return rid
            if db.execute('SELECT id FROM miet_checkout_refunds WHERE hold_id=?',
                          (hold_id,)).fetchone():
                raise ValueError('Bereits vorhandene Erstattung zuerst manuell abgleichen.')
            if db.execute('SELECT id FROM miet_checkout_cancellations WHERE id=?',
                          (hold_id,)).fetchone():
                raise ValueError('Bereits stornierte Buchung getrennt abrechnen.')
            note=('Vollerstattung einer bezahlten, nicht bestätigten Buchung; '
                  'Prüfung durch '+operator_name+'; Stripe-Session '+session['id']+
                  '; PaymentIntent '+pi+'; Grund: '+reason)
            self._enqueue(db,current,q['rental_cents'],'review_full_refund',note,rid)
            db.execute('''INSERT INTO miet_checkout_review_refunds
                (hold_id,refund_id,session_id,payment_intent,amount_cents,verified_at,operator_name,reason)
                VALUES (?,?,?,?,?,?,?,?)''',
                (hold_id,rid,session['id'],pi,q['rental_cents'],
                 datetime.now(timezone.utc).isoformat(),operator_name,reason))
            db.execute("UPDATE miet_checkout_holds SET payment_intent=? WHERE id=?",
                       (pi,hold_id))
            return rid

    def close_review_refund(self,hold_id):
        """Free inventory only after both the rent refund and card release."""
        h=self.s.read(hold_id)
        with self.s.locked(h['mietfahrzeug_id']) as (db,_):
            current=db.execute('SELECT status,grund,mietvorgang_id FROM miet_checkout_holds WHERE id=?',
                               (hold_id,)).fetchone()
            if current['status']=='released' and current['grund']=='review_full_refund_completed':
                return 'released'
            if current['status']!='review' or current['mietvorgang_id']:
                raise ValueError('Prüffall ist nicht mehr abschließbar.')
            audit=db.execute('SELECT * FROM miet_checkout_review_refunds WHERE hold_id=?',
                             (hold_id,)).fetchone()
            refund=(db.execute('SELECT * FROM miet_checkout_refunds WHERE id=?',
                               (audit['refund_id'],)).fetchone() if audit else None)
            deposit=db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                               (hold_id,)).fetchone()
            if (not audit or not refund or refund['status']!='succeeded'
                    or refund['kind']!='review_full_refund'
                    or refund['amount_cents']!=audit['amount_cents']
                    or refund['provider_id'] is None or not deposit
                    or deposit['status']!='released'):
                raise ValueError('Erstattung oder Kautionsfreigabe noch nicht belegt; Prüffall bleibt gesperrt.')
            db.execute("UPDATE miet_checkout_holds SET status='released',grund='review_full_refund_completed' WHERE id=?",
                       (hold_id,))
            return 'released'

    def _enqueue(self,db,h,amount,kind,reason,request_id):
        old=db.execute('SELECT * FROM miet_checkout_refunds WHERE id=?',(request_id,)).fetchone()
        if old:
            if old['hold_id']!=h['id'] or old['amount_cents']!=amount or old['kind']!=kind:
                raise ValueError('Erstattungsreferenz mit abweichendem Inhalt.')
            return request_id
        total=json.loads(h['payload'])['quote']['amount_cents']
        reserved=db.execute("SELECT COALESCE(SUM(amount_cents),0) AS n FROM miet_checkout_refunds WHERE hold_id=? AND status!='failed'",(h['id'],)).fetchone()['n']
        if type(amount) is not int or amount<0 or reserved+amount>total:
            raise ValueError('Erstattung übersteigt verbleibende Zahlung.')
        db.execute('''INSERT INTO miet_checkout_refunds (id,hold_id,amount_cents,kind,status,reason,created_at)
            VALUES (?,?,?,?,?,?,?)''',(request_id,h['id'],amount,kind,'succeeded' if amount==0 else 'queued',reason,datetime.now(timezone.utc).isoformat()))
        return request_id

    def cancel(self,hold_id,requested_at=None,admin=False,no_show=False,
               staffed_check=False,contact_attempt='',contact_attempt_at=None,
               operator_name='',review_note=''):
        now=requested_at or datetime.now(timezone.utc)
        h=self.s.read(hold_id);q=json.loads(h['payload'])['quote']
        if not h['mietvorgang_id'] or not h['payment_intent']:raise ValueError('Keine bestätigte bezahlte Buchung.')
        with self.s.locked(h['mietfahrzeug_id']) as (db,_):
            if db.execute('SELECT hold_id FROM miet_checkout_handovers WHERE hold_id=?',
                          (hold_id,)).fetchone():
                raise ValueError('Schlüsselübergabe ist dokumentiert; Storno nur nach manueller Abrechnung.')
            old=db.execute('SELECT * FROM miet_checkout_cancellations WHERE id=?',(hold_id,)).fetchone()
            if old:return 'cancel-'+hold_id
            start=datetime.fromisoformat(q['start_slot'])
            if no_show:
                if not admin or now.tzinfo is None or start.tzinfo is None:
                    raise ValueError('Nichterscheinen darf nur die Werkstatt mit gültigem Termin prüfen.')
                local=now.astimezone(BERLIN)
                if now.astimezone(timezone.utc)<start.astimezone(timezone.utc)+timedelta(hours=1):
                    raise ValueError('Nichterscheinen frühestens 60 Minuten nach dem Abholtermin prüfen.')
                if local.weekday()==6 or not (8<=local.hour<20):
                    raise ValueError('Nichterscheinen erst beim nächsten betreuten Werkstatttermin prüfen.')
                contact_attempt=contact_attempt.strip() if isinstance(contact_attempt,str) else ''
                operator_name=operator_name.strip() if isinstance(operator_name,str) else ''
                review_note=review_note.strip() if isinstance(review_note,str) else ''
                if (not staffed_check or not 2<=len(operator_name)<=100
                        or not 5<=len(contact_attempt)<=500 or not 3<=len(review_note)<=1000):
                    raise ValueError('Betreute Prüfung, verantwortliche Person und Kontaktversuch dokumentieren.')
                if (not isinstance(contact_attempt_at,datetime) or contact_attempt_at.tzinfo is None
                        or not start<=contact_attempt_at<=now):
                    raise ValueError('Zeit des Kontaktversuchs nach Abholbeginn und vor Prüfung angeben.')
            elif now>=start:
                raise ValueError('Nach Mietbeginn bitte die Werkstatt kontaktieren; keine automatische Stornierung.')
            rental=db.execute('SELECT status,rueckgabe_datum FROM mietvorgaenge WHERE id=?',(h['mietvorgang_id'],)).fetchone()
            if not rental or rental['status'] in {'zurueck','storniert'} or rental['rueckgabe_datum']:
                raise ValueError('Mietvorgang bereits beendet; manuelle Abrechnung erforderlich.')
            fee=cancellation_fee(q,now)
            reason=('Nichterscheinen nach betreuter Prüfung durch '+operator_name+
                    '; Kontaktversuch '+contact_attempt_at.isoformat()+': '+contact_attempt+
                    '; Prüfvermerk: '+review_note
                    if no_show else 'Stornierung')
            db.execute('INSERT INTO miet_checkout_cancellations (id,requested_at,fee_cents,reason) VALUES (?,?,?,?)',(hold_id,now.isoformat(),fee,reason))
            db.execute("UPDATE mietvorgaenge SET status='storniert',geaendert_am=? WHERE id=?",(self.s.p.now_str(),h['mietvorgang_id']))
            return self._enqueue(db,h,q['amount_cents']-fee,'cancellation',reason,'cancel-'+hold_id)

    def credit(self,hold_id,credit_cents,reason,request_id):
        if not reason.strip():raise ValueError('Begründung für Minderung fehlt.')
        h=self.s.read(hold_id)
        with self.s.locked(h['mietfahrzeug_id']) as (db,_):
            old=db.execute('SELECT id FROM miet_checkout_refunds WHERE id=?',(request_id,)).fetchone()
            if old:return self._enqueue(db,h,credit_cents,'cancellation_credit',reason,request_id)
            c=db.execute('SELECT * FROM miet_checkout_cancellations WHERE id=?',(hold_id,)).fetchone()
            if not c or type(credit_cents) is not int or credit_cents<=0 or c['credit_cents']+credit_cents>c['fee_cents']:
                raise ValueError('Minderung übersteigt die verbleibende Stornogebühr.')
            rid=self._enqueue(db,h,credit_cents,'cancellation_credit',reason,request_id)
            db.execute('UPDATE miet_checkout_cancellations SET credit_cents=credit_cents+? WHERE id=?',(credit_cents,hold_id))
            return rid

    def deposit(self,hold_id,reason):
        h=self.s.read(hold_id);q=json.loads(h['payload'])['quote']
        with self.s.locked(h['mietfahrzeug_id']) as (db,_):
            rental=db.execute('SELECT status FROM mietvorgaenge WHERE id=?',(h['mietvorgang_id'],)).fetchone()
            if not rental or rental['status']!='zurueck' or q.get('deposit_charged_cents',0)<=0:
                raise ValueError('Kaution erst nach protokollierter Rückgabe freigeben.')
            return self._enqueue(db,h,q['deposit_charged_cents'],'deposit_return',reason,'deposit-'+hold_id)

    def process(self,refund_id):
        db=self.s.p.get_db()
        try:
            r=db.execute('SELECT * FROM miet_checkout_refunds WHERE id=?',(refund_id,)).fetchone()
            if not r:raise ValueError('Erstattung nicht gefunden.')
            r=dict(r)
        finally:db.close()
        if r['status'] in {'succeeded','failed'}:return r['status']
        h=self.s.read(r['hold_id'])
        # On network uncertainty the same immutable request/idempotency key is retried.
        if not r['provider_id'] and (datetime.now(timezone.utc)-datetime.fromisoformat(r['created_at'])).total_seconds()>23*3600:
            raise ValueError('Unklarer Erstattungsauftrag außerhalb des sicheren Wiederholungsfensters: manuell mit Stripe abgleichen, nicht neu senden.')
        result=(self.s.gateway.retrieve_refund(r['provider_id']) if r['provider_id'] else
                self.s.gateway.refund(h['payment_intent'],r['amount_cents'],'mos-refund-'+r['id']))
        if (result.get('payment_intent')!=h['payment_intent'] or result.get('amount')!=r['amount_cents']
            or (r['provider_id'] and result.get('id')!=r['provider_id'])
            or result.get('currency')!='eur' or result.get('object')!='refund'
            or not str(result.get('id','')).startswith('re_')):
            raise ValueError('Erstattungsantwort passt nicht zum Auftrag.')
        status={'succeeded':'succeeded','failed':'failed','canceled':'failed'}.get(result.get('status'),'submitted')
        with self.s.locked(h['mietfahrzeug_id']) as (db,_):
            db.execute("UPDATE miet_checkout_refunds SET provider_id=?,status=? WHERE id=? AND status NOT IN ('succeeded','failed')",(result['id'],status,r['id']))
        return status
