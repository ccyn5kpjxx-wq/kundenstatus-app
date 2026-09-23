"""Fail-closed launch validation and an idempotent, durable refund ledger.

No network call occurs on import. Runtime enabling requires explicit operator evidence.
"""
from datetime import datetime, timezone
import json
import secrets
import stripe
from .gateway import StripeTestGateway

ACTIVE_LISTINGS={'kona','i10'}


def launch_errors(cfg):
    errors=[]
    if set(cfg.get('fleet',{})) != ACTIVE_LISTINGS:
        errors.append('Nur die bestätigten Fahrzeuge KONA und i10 dürfen freigegeben werden.')
    launch=cfg.get('launch',{})
    for name in ('business_review','legal_review','finance_review','privacy_review','sandbox_acceptance','postgres_acceptance'):
        record=launch.get(name,{})
        if not (record.get('approved_by') and record.get('approved_at') and record.get('evidence')):
            errors.append('Freigabenachweis fehlt: '+name)
    for slug in ACTIVE_LISTINGS:
        record=launch.get('insurance',{}).get(slug,{})
        if not (record.get('verified') is True and record.get('use')=='paid_self_drive'
                and record.get('evidence') and record.get('vehicle_id')==cfg.get('fleet',{}).get(slug,{}).get('id')):
            errors.append('Belegter Selbstfahrervermietungsschutz fehlt: '+slug)
        if not cfg.get('fleet',{}).get(slug,{}).get('expected_name'):
            errors.append('Geprüfte Fahrzeugidentität fehlt: '+slug)
    if cfg.get('deposit_method')!='charge_with_rent_refund_after_return':
        errors.append('Separater Kautionseinzug mit Rückerstattung ist nicht freigegeben.')
    if not cfg.get('terms_version') or cfg['terms_version'].startswith('draft:'):
        errors.append('Freigegebene Bedingungsversion fehlt.')
    for name in ('terms_text','privacy_url','merchant_name','merchant_address','merchant_email','merchant_phone'):
        if not cfg.get(name):errors.append('Pflichtangabe fehlt: '+name)
    if not cfg.get('origin','').startswith('https://'):
        errors.append('Livebetrieb erfordert HTTPS.')
    if cfg.get('live_enabled') is not True:
        errors.append('Live-Aktivierung fehlt.')
    return errors


class StripeLiveGateway(StripeTestGateway):
    livemode=True
    def __init__(self,key,cfg):
        errors=launch_errors(cfg)
        if errors:raise ValueError('; '.join(errors))
        if not key.startswith(('sk_live_','rk_live_')):raise ValueError('Live-Schlüssel fehlt oder falscher Modus.')
        self.client=stripe.StripeClient(key,max_network_retries=2)

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
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_limits (
        id TEXT PRIMARY KEY, attempts INTEGER NOT NULL)''')


def cancellation_fee(quote,requested_at):
    start=datetime.fromisoformat(quote['start_slot']).astimezone(timezone.utc)
    if (start-requested_at.astimezone(timezone.utc)).total_seconds()>=86400:return 0
    return min(quote['daily_cents'],quote.get('rental_cents',quote['amount_cents']))


class RefundLedger:
    def __init__(self,service):self.s=service

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

    def cancel(self,hold_id,requested_at=None,admin=False,no_show=False):
        now=requested_at or datetime.now(timezone.utc)
        h=self.s.read(hold_id);q=json.loads(h['payload'])['quote']
        if not h['mietvorgang_id'] or not h['payment_intent']:raise ValueError('Keine bestätigte bezahlte Buchung.')
        if now>=datetime.fromisoformat(q['start_slot']) and not (admin and no_show):
            raise ValueError('Nach Mietbeginn bitte die Werkstatt kontaktieren; keine automatische Stornierung.')
        with self.s.locked(h['mietfahrzeug_id']) as (db,_):
            old=db.execute('SELECT * FROM miet_checkout_cancellations WHERE id=?',(hold_id,)).fetchone()
            if old:return 'cancel-'+hold_id
            rental=db.execute('SELECT status,rueckgabe_datum FROM mietvorgaenge WHERE id=?',(h['mietvorgang_id'],)).fetchone()
            if not rental or rental['status'] in {'zurueck','storniert'} or rental['rueckgabe_datum']:
                raise ValueError('Mietvorgang bereits beendet; manuelle Abrechnung erforderlich.')
            fee=cancellation_fee(q,now)
            reason='Nichterscheinen nach Prüfung' if no_show else 'Stornierung'
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
