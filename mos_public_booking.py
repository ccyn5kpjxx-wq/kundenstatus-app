"""Public-facing TEST flow; explicit configuration, isolated portal DB, no live mode."""
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import time
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, current_app, redirect, render_template, request, session, url_for
from itsdangerous import URLSafeTimedSerializer, BadSignature
from mos_public_contract import LESSOR_ADDRESS, LESSOR_NAME, init_schema as init_contract_schema
from mos_public_contract import finalize as finalize_contract, presign_quote, read as read_contract
from mos_public_contract import signed_payload

LISTINGS = {'kona':'Hyundai KONA N Line X', 'i10':'Hyundai i10'}
BERLIN = ZoneInfo('Europe/Berlin')


def local_slot_to_iso(value):
    """Accept a future Berlin wall-clock minute only when its UTC offset is unambiguous."""
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}',value or ''):
        raise ValueError('Bitte einen gültigen Abhol- oder Rückgabetermin eingeben.')
    try: local=datetime.strptime(value,'%Y-%m-%dT%H:%M')
    except ValueError:raise ValueError('Bitte einen gültigen Abhol- oder Rückgabetermin eingeben.') from None
    possible=[]
    for fold in (0,1):
        aware=local.replace(tzinfo=BERLIN,fold=fold)
        if aware.astimezone(timezone.utc).astimezone(BERLIN).replace(tzinfo=None)==local:
            possible.append(aware)
    if len({d.utcoffset() for d in possible})!=1:
        raise ValueError('Dieser Termin liegt in einer mehrdeutigen oder nicht vorhandenen Zeitumstellung.')
    slot=possible[0]
    if slot.astimezone(timezone.utc)<=datetime.now(timezone.utc):
        raise ValueError('Nur künftige Übergabetermine können geöffnet werden.')
    return slot.isoformat()


def init_slot_schema(db, configured_slots):
    """The portal database is authoritative; config dates seed only previously unseen rows."""
    db.execute('''CREATE TABLE IF NOT EXISTS miet_checkout_slots (
        slot TEXT PRIMARY KEY, active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)''')
    now=datetime.now(timezone.utc).isoformat()
    for slot in configured_slots:
        db.execute('''INSERT INTO miet_checkout_slots (slot,active,created_at,updated_at)
            VALUES (?,1,?,?) ON CONFLICT (slot) DO NOTHING RETURNING slot''',(slot,now,now))


def listed_slots(db, active=True):
    rows=db.execute('SELECT slot,active FROM miet_checkout_slots').fetchall()
    now=datetime.now(timezone.utc)
    slots=[dict(r) for r in rows if datetime.fromisoformat(r['slot']).astimezone(timezone.utc)>now
           and (active is None or bool(r['active']) is active)]
    return sorted(slots,key=lambda row:datetime.fromisoformat(row['slot']).astimezone(timezone.utc))


def selected_slots_open(db,start,end):
    try:
        a,b=datetime.fromisoformat(start),datetime.fromisoformat(end)
        if (a.tzinfo is None or b.tzinfo is None or a.astimezone(timezone.utc)<=datetime.now(timezone.utc)
                or b.astimezone(timezone.utc)<=a.astimezone(timezone.utc)):
            return False
    except (TypeError,ValueError):return False
    active={row['slot'] for row in db.execute(
        'SELECT slot FROM miet_checkout_slots WHERE active=1 AND slot IN (?,?)',(start,end)).fetchall()}
    return active=={start,end}


def website_response(response, test_url=None, live=False):
    """Only an explicitly configured test deployment adds an entry; otherwise unchanged."""
    if not test_url:
        return response
    from markupsafe import escape
    parsed = urlsplit(test_url)
    if (not parsed.hostname or parsed.username or parsed.password or
        not (parsed.scheme == 'https' or parsed.scheme == 'http' and parsed.hostname in {'localhost','127.0.0.1'})):
        raise ValueError('Testlink benötigt HTTPS oder Loopback.')
    response.direct_passthrough = False
    label='KONA oder i10 online buchen' if live else 'Testbuchung für KONA oder i10 öffnen'
    banner = ('<aside style="position:fixed;bottom:0;left:0;right:0;padding:12px;background:#fff0cf;z-index:9999;text-align:center">'
              +('' if live else '<strong>Nur Testumgebung:</strong> ')+'<a href="'+str(escape(test_url))+'">'+label+'</a></aside>')
    response.set_data(response.get_data(as_text=True).replace('</body>',banner+'</body>'))
    response.headers['Cache-Control']='no-store'
    response.headers.pop('ETag',None)
    return response


def register(portal):
    app=portal.app
    app.jinja_env.filters['mos_slot'] = lambda value: datetime.fromisoformat(value).strftime('%d.%m.%Y · %H:%M Uhr')
    app.config.setdefault('MOS_PUBLIC_BOOKING', {'enabled':False})
    config_path=os.environ.get('MOS_BOOKING_CONFIG_FILE')
    if config_path:
        app.config['MOS_PUBLIC_BOOKING']=json.loads(Path(config_path).read_text(encoding='utf-8'))
    # Secrets are read only in an explicitly configured runtime, never committed or logged.
    if config_path:
        app.config['MOS_PUBLIC_STRIPE_TEST_KEY']=os.environ.get('MOS_STRIPE_TEST_KEY','')
        app.config['MOS_PUBLIC_STRIPE_LIVE_KEY']=os.environ.get('MOS_STRIPE_LIVE_KEY','')
        app.config['MOS_PUBLIC_WEBHOOK_SECRET']=os.environ.get('MOS_STRIPE_WEBHOOK_SECRET','')
        app.config['MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY']=os.environ.get('MOS_STRIPE_PUBLISHABLE_KEY','')
    prefix='/mieten' if app.config['MOS_PUBLIC_BOOKING'].get('mode')=='live' else '/mietwagen-test'
    bp=Blueprint('mos_public',__name__,url_prefix=prefix)

    def setup():
        existing=app.extensions.get('mos_public_booking')
        if existing:
            return existing
        cfg=app.config['MOS_PUBLIC_BOOKING']
        live=cfg.get('mode')=='live'
        # No accidental production inventory edits with a test Stripe key.
        if not live and (portal.USE_POSTGRES or not str(portal.DB).endswith('.mos-public-test.sqlite3')):
            raise ValueError('Getrennte *.mos-public-test.sqlite3 Portal-Testdatenbank erforderlich.')
        if cfg.get('mode') not in {'offline','stripe_test','live'} or (not live and cfg.get('test_configuration') is not True):
            raise ValueError('Explizite Testkonfiguration erforderlich.')
        origin=urlsplit(cfg['origin'])
        if (origin.path or origin.query or origin.fragment or origin.username or origin.password or not origin.hostname
            or not (origin.scheme=='https' or origin.scheme=='http' and origin.hostname in {'127.0.0.1','localhost'})):
            raise ValueError('Ungültige Test-Origin.')
        cfg=json.loads(json.dumps(cfg))
        if not live:cfg['fleet']={k:v for k,v in cfg['fleet'].items() if k in LISTINGS}
        if set(cfg['fleet']) != set(LISTINGS) or len({v['id'] for v in cfg['fleet'].values()}) != 2:
            raise ValueError('Nur zwei explizite, unterschiedliche Flottenzuordnungen für KONA und i10 sind zulässig.')
        for slug,v in cfg['fleet'].items():
            if type(v['id']) is not int or type(v['daily_cents']) is not int or v['daily_cents']<=0:
                raise ValueError('Ungültige Test-Flotte/Preis.')
            if 'discount_after_days' in v and (type(v['discount_after_days']) is not int or v['discount_after_days']<1
                or type(v.get('discount_cents')) is not int or v['discount_cents']<=0):
                raise ValueError('Ungültiger Test-Rabatt.')
        if (not live and not cfg['terms_version'].startswith('draft:')) or not cfg['terms_text'] or cfg.get('vat_included') is not True:
            raise ValueError('Entwurfsversion und Bruttopreise erforderlich.')
        if cfg.get('day_rule') != 'elapsed_24h_ceil' or cfg.get('deposit_cents')!=50000 or cfg.get('deductible_cents')!=100000:
            raise ValueError('Explizite Test-Zeitregel und beschlossene Beträge erforderlich.')
        if cfg.get('cancellation_policy') not in (None,'free_48h_then_10pct_rent'):
            raise ValueError('Unbekannte Stornoregel in der Buchungskonfiguration.')
        for key in ('included_km_day','extra_km_cents','max_days'):
            if type(cfg[key]) is not int or cfg[key]<1:raise ValueError('Test-Tarif unvollständig.')
        if not isinstance(cfg.get('slots'),list) or len(set(cfg['slots'])) != len(cfg['slots']):
            raise ValueError('Übergabetermine müssen eine Liste eindeutiger Zeitpunkte sein.')
        for slot in cfg['slots']:
            dt=datetime.fromisoformat(slot)
            if dt.tzinfo is None or dt.astimezone(BERLIN).isoformat()!=slot:
                raise ValueError('Slots müssen eindeutige Europe/Berlin-Zeitpunkte mit Offset sein.')
        from mos_booking.gateway import OfflineGateway, StripeTestGateway
        from mietwagen_checkout import SharedCheckout
        from mos_booking.production import StripeLiveGateway, RefundLedger, init_refund_schema, launch_errors
        if live:
            problems=launch_errors(cfg)
            if problems:raise ValueError('Buchung noch nicht freigegeben: '+'; '.join(problems))
            if not portal.USE_POSTGRES:
                raise ValueError('Livebetrieb benötigt den autoritativen PostgreSQL-Portalbestand, keine lokale Kopie.')
            if getattr(portal,'USING_EPHEMERAL_SECRET_KEY',True) or getattr(portal,'USING_GENERATED_FLASK_SECRET_KEY',True):
                raise ValueError('Persistenter sicherer Flask-Schlüssel erforderlich.')
            app.config.update(SESSION_COOKIE_SECURE=True,SESSION_COOKIE_SAMESITE='Lax')
        if cfg['mode']=='offline':
            secret=secrets.token_urlsafe(32)
            gateway=OfflineGateway(Path(portal.DB).with_suffix('.provider.sqlite3'),cfg['origin']+prefix,secret)
        else:
            secret=app.config.get('MOS_PUBLIC_WEBHOOK_SECRET','')
            if not secret.startswith('whsec_'):raise ValueError('Test-Webhook-Secret fehlt.')
            gateway=(StripeLiveGateway(app.config.get('MOS_PUBLIC_STRIPE_LIVE_KEY',''),cfg) if live else
                     StripeTestGateway(app.config.get('MOS_PUBLIC_STRIPE_TEST_KEY','')))
            if cfg.get('deposit_method')=='card_authorization_at_booking':
                publishable=app.config.get('MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY','')
                if not publishable.startswith('pk_live_' if live else 'pk_test_'):
                    raise ValueError('Passender Stripe-Schlüssel für das sichere Kartenformular fehlt.')
        service=SharedCheckout(portal,gateway,cfg['origin']+prefix+'/status')
        app.config['MOS_SHARED_CHECKOUT_ENABLED']=bool(app.config['MOS_PUBLIC_BOOKING'].get('enabled'))
        db=portal.get_db()
        try:
            init_refund_schema(db);init_contract_schema(db)
            init_slot_schema(db,cfg['slots']);db.commit()
        finally:db.close()
        existing={'cfg':cfg,'service':service,'gateway':gateway,'secret':secret,'ledger':RefundLedger(service)}
        app.extensions['mos_public_booking']=existing
        return existing

    @bp.before_request
    def guard():
        cfg=app.config.get('MOS_PUBLIC_BOOKING',{})
        if request.endpoint in {'mos_public.admin_slot_page','mos_public.admin_slots'}:
            # Operators can configure appointments before payment/insurance launch gates pass.
            # Both views still require the portal's admin session and global POST-CSRF check.
            if request.content_length and request.content_length>65536:abort(413)
            return
        settling={'mos_public.status','mos_public.retry','mos_public.cancel','mos_public.webhook','mos_public.receipt',
                  'mos_public.cancel_paid','mos_public.admin_bookings','mos_public.admin_action',
                  'mos_public.signed_contract_pdf','mos_public.admin_contract_pdf'}
        if not cfg.get('enabled') and not (request.endpoint in settling and cfg.get('mode') in {'offline','stripe_test','live'}):
            abort(404)
        state=setup()
        if request.host != urlsplit(state['cfg']['origin']).netloc:abort(403)
        if request.content_length and request.content_length>65536:abort(413)
        if request.endpoint!='mos_public.webhook':
            session.setdefault('mos_public_owner',secrets.token_urlsafe(32))
        if state['cfg']['mode']=='live' and request.method=='POST' and request.endpoint in {'mos_public.preview','mos_public.checkout'}:
            # Persisted across workers; never trust an unconfigured X-Forwarded-For header.
            bucket=int(time.time()//600)
            digest=hashlib.sha256((app.secret_key+str(request.remote_addr)).encode()).hexdigest()
            key=digest+':'+str(bucket);db=portal.get_db()
            try:
                if not portal.USE_POSTGRES:db.execute('BEGIN IMMEDIATE')
                db.execute('INSERT INTO miet_checkout_limits (id,attempts) VALUES (?,0) ON CONFLICT (id) DO NOTHING',(key,))
                row=db.execute('SELECT attempts FROM miet_checkout_limits WHERE id=?'+(' FOR UPDATE' if portal.USE_POSTGRES else ''),(key,)).fetchone()
                if row['attempts']>=30:db.rollback();abort(429)
                db.execute('UPDATE miet_checkout_limits SET attempts=attempts+1 WHERE id=?',(key,));db.commit()
            finally:db.close()

    @bp.context_processor
    def mode_context():
        state=app.extensions.get('mos_public_booking',{})
        cfg=state.get('cfg') or app.config.get('MOS_PUBLIC_BOOKING',{})
        return {'live':cfg.get('mode')=='live','booking_config':cfg}

    @bp.after_request
    def headers(response):
        response.headers.update({'Cache-Control':'no-store','X-Robots-Tag':'noindex, nofollow',
                                 'Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff',
                                 'X-Frame-Options':'DENY'})
        if request.endpoint=='mos_public.deposit_page':
            response.headers['Content-Security-Policy']=(
                "default-src 'self'; base-uri 'self'; object-src 'none'; frame-ancestors 'none'; "
                "form-action 'self' https://hooks.stripe.com; "
                "script-src 'self' 'unsafe-inline' https://js.stripe.com; "
                "style-src 'self' 'unsafe-inline' https://js.stripe.com; "
                "font-src 'self' data:; img-src 'self' data: https://*.stripe.com; "
                "frame-src https://js.stripe.com https://hooks.stripe.com; "
                "connect-src 'self' https://api.stripe.com https://r.stripe.com https://m.stripe.network")
        return response

    @bp.errorhandler(ValueError)
    def invalid(error):
        return render_template('mos_public/error.html',message=str(error)),409

    def owner():
        return hashlib.sha256(session['mos_public_owner'].encode()).hexdigest()

    def serializer():return URLSafeTimedSerializer(app.secret_key,salt='mos-public-quote-v1')

    def quote_slots_open(q):
        db=portal.get_db()
        try:return selected_slots_open(db,q['start_slot'],q['end_slot'])
        finally:db.close()

    def closed_slot_response(hold_id,q):
        if quote_slots_open(q):return None
        try:setup()['service'].cancel_or_reconcile(hold_id,cancel=True)
        except Exception:
            app.logger.exception('MOS Kartenreservierung/Checkout nach Terminschließung muss geprüft werden')
        return render_status(hold_id,'Der Übergabetermin ist nicht mehr geöffnet. Bitte neu buchen.'),409

    def quote(slug,start,end):
        state=setup();cfg=state['cfg']
        if slug not in LISTINGS:
            raise ValueError('Bitte ein verfügbares Fahrzeug und freigegebene Termine wählen.')
        db=portal.get_db()
        try:
            if not selected_slots_open(db,start,end):
                raise ValueError('Bitte ein verfügbares Fahrzeug und freigegebene Termine wählen.')
            a,b=datetime.fromisoformat(start),datetime.fromisoformat(end)
            seconds=(b.astimezone(timezone.utc)-a.astimezone(timezone.utc)).total_seconds()
            days=math.ceil(seconds/86400)
            if a<=datetime.now(timezone.utc) or seconds<=0 or days>cfg['max_days']:
                raise ValueError('Ungültiger Mietzeitraum.')
            f=cfg['fleet'][slug];rate=f['daily_cents']
            if days>=f.get('discount_after_days',cfg['max_days']+1):rate=f['discount_cents']
            vehicle=db.execute('SELECT id,bezeichnung,kennzeichen,fin_nummer,aktiv,status FROM mietfahrzeuge WHERE id=?',(f['id'],)).fetchone()
            if not vehicle or not int(vehicle['aktiv'] or 0) or portal.normalize_mietfahrzeug_status(vehicle['status']) in {'bald','wartung','inaktiv'}:
                raise ValueError('Das zugeordnete Fahrzeug ist nicht verfügbar.')
            if cfg['mode']=='live' and vehicle['bezeichnung']!=f['expected_name']:
                raise ValueError('Fahrzeugzuordnung muss von der Werkstatt geprüft werden.')
            if not portal.mietfahrzeug_zeitraum_frei_db(db,f['id'],a.date(),b.date()):
                raise ValueError('Der Zeitraum ist bereits belegt. Bitte andere Termine wählen.')
        finally:db.close()
        authorization=cfg.get('deposit_method')=='card_authorization_at_booking'
        charged=cfg['mode'] in {'live','stripe_test'} and not authorization
        quoted={'slug':slug,'vehicle_id':f['id'],'vehicle_name':LISTINGS[slug],
            'portal_vehicle_name':vehicle['bezeichnung'],
            'vehicle_plate':vehicle['kennzeichen'] or '', 'vehicle_vin':vehicle['fin_nummer'] or '',
            'start_slot':start,'end_slot':end,
            'slot_policy':'db_open_slots_v1',
            'days':days,'daily_cents':rate,'rental_cents':days*rate,
            'amount_cents':days*rate+(50000 if charged else 0),'currency':'eur',
            'deposit_charged_cents':50000 if charged else 0,
            'deposit_authorized_cents':50000 if authorization else 0,
            'deposit_method':cfg.get('deposit_method',''),
            'checkout_deposit':charged,
            'included_km':days*cfg['included_km_day'],'extra_km_cents':cfg['extra_km_cents'],
            'deposit_cents':cfg['deposit_cents'],'deductible_cents':cfg['deductible_cents'],
            'rules_version':cfg['terms_version'],'terms_text':cfg['terms_text'],'owner_hash':owner(),
            'test_only':cfg['mode']!='live','vat_included':True,
            'lessor_name':LESSOR_NAME,'lessor_address':LESSOR_ADDRESS}
        if cfg.get('cancellation_policy'):
            quoted['cancellation_policy']=cfg['cancellation_policy']
        return quoted

    def owned(hold_id):
        try:h=setup()['service'].read(hold_id)
        except ValueError:abort(404)
        payload=json.loads(h['payload'])
        if payload['quote'].get('owner_hash')!=owner():abort(404)
        return h,payload

    def account(hold_id):
        db=portal.get_db()
        try:
            c=db.execute('SELECT * FROM miet_checkout_cancellations WHERE id=?',(hold_id,)).fetchone()
            refunds=db.execute('SELECT * FROM miet_checkout_refunds WHERE hold_id=? ORDER BY created_at',(hold_id,)).fetchall()
            return {'cancellation':dict(c) if c else None,'refunds':[dict(r) for r in refunds]}
        finally:db.close()

    @bp.get('/status/<hold_id>/bestaetigung.txt')
    def receipt(hold_id):
        h,p=owned(hold_id)
        if not h['mietvorgang_id']:abort(409)
        finalize_contract(portal,hold_id)
        q=p['quote'];cfg=setup()['cfg']
        text=('BUCHUNGSBESTÄTIGUNG' if cfg['mode']=='live' else 'TESTBESTÄTIGUNG – KEIN MIETVERTRAG')
        text+='\nReferenz: '+h['id']+'\n'+q['vehicle_name']+'\n'+q['start_slot']+' bis '+q['end_slot']
        text+='\nMieter: '+p['customer']['name']+'\nE-Mail: '+p['customer']['email']
        text+=f"\nMiete: {q.get('rental_cents',q['amount_cents'])/100:.2f} EUR inkl. MwSt."
        if q.get('deposit_authorized_cents'):
            text+=f"\nKaution bei Buchung auf Kreditkarte autorisiert, nicht abgebucht: {q['deposit_authorized_cents']/100:.2f} EUR"
            text+=f"\nGezahlter Mietpreis: {q['amount_cents']/100:.2f} EUR"
        else:
            text+=f"\nKaution eingezogen: {q.get('deposit_charged_cents',0)/100:.2f} EUR\nGesamtzahlung: {q['amount_cents']/100:.2f} EUR"
        text+='\nAbholung persönlich: Gärtner, Binauer Höhe 4, 74821 Mosbach-Lohrbach.'
        text+='\nVermieter und Vertragspartner: '+q.get('lessor_name',LESSOR_NAME)+', '+q.get('lessor_address',LESSOR_ADDRESS)
        text+='\nAutovermietung MOS ist nur die Bezeichnung des Angebots, keine eigene Vertragspartei.'
        text+='\nBedingungsversion: '+q['rules_version']+'\n\n'+q['terms_text']
        db=portal.get_db()
        try:contract=read_contract(db,hold_id)
        finally:db.close()
        text+='\n\nDigitale Unterschrift: '+('am '+contract['signed_at']+' UTC gespeichert.' if contract else 'noch ausstehend.')
        if account(hold_id)['cancellation']:text+='\n\nACHTUNG: Diese Buchung wurde inzwischen storniert. Abrechnung siehe Statusseite.'
        return text,200,{'Content-Type':'text/plain; charset=utf-8','Content-Disposition':'attachment; filename="MOS-Buchungsbestaetigung.txt"'}

    def deposit_state(hold_id):
        db=portal.get_db()
        try:
            row=db.execute('SELECT status FROM miet_checkout_deposit_auths WHERE hold_id=?',
                           (hold_id,)).fetchone()
            return row['status'] if row else None
        finally:db.close()

    @bp.post('/status/<hold_id>/stornieren')
    def cancel_paid(hold_id):
        h,p=owned(hold_id)
        if request.form.get('confirm')!='yes':raise ValueError('Stornierung bitte ausdrücklich bestätigen.')
        rid=setup()['ledger'].cancel(hold_id)
        try:setup()['ledger'].process(rid)
        except Exception:pass  # Durable queued operation remains visible to admin/customer.
        if p['quote'].get('deposit_authorized_cents'):
            try:setup()['service'].release_deposit(hold_id,'Kundenstorno')
            except Exception:app.logger.exception('MOS Kartenreservierung nach Storno noch offen')
        return redirect(url_for('mos_public.status',hold_id=hold_id),code=303)

    @bp.get('/admin')
    @portal.admin_required
    def admin_bookings():
        db=portal.get_db()
        try:
            holds=[dict(r) for r in db.execute('''SELECT h.*,c.signed_at,d.status AS deposit_status FROM miet_checkout_holds h
                LEFT JOIN miet_checkout_contracts c ON c.hold_id=h.id
                LEFT JOIN miet_checkout_deposit_auths d ON d.hold_id=h.id
                ORDER BY h.expires_at DESC LIMIT 100''').fetchall()]
            open_slots=listed_slots(db,True)
            closed_slots=listed_slots(db,False)
        finally:db.close()
        for h in holds:
            h['q']=json.loads(h['payload'])['quote'];h['account']=account(h['id'])
            if h['signed_at']:
                h['signed_at']=datetime.fromisoformat(h['signed_at']).astimezone(ZoneInfo('Europe/Berlin')).strftime('%d.%m.%Y um %H:%M Uhr')
        return render_template('mos_public/admin.html',holds=holds,open_slots=open_slots,
                               closed_slots=closed_slots,request_id=secrets.token_urlsafe(24))

    @bp.get('/admin/termine')
    @portal.admin_required
    def admin_slot_page():
        db=portal.get_db()
        try:
            configured=app.config.get('MOS_PUBLIC_BOOKING',{}).get('slots',[])
            init_slot_schema(db,configured if isinstance(configured,list) else [])
            db.commit()
            open_slots=listed_slots(db,True)
            closed_slots=listed_slots(db,False)
        finally:db.close()
        return render_template('mos_public/admin.html',holds=[],open_slots=open_slots,
                               closed_slots=closed_slots,slot_only=True)

    @bp.post('/admin/termine')
    @portal.admin_required
    def admin_slots():
        action=request.form.get('action')
        affected=[]
        db=portal.get_db()
        try:
            configured=app.config.get('MOS_PUBLIC_BOOKING',{}).get('slots',[])
            init_slot_schema(db,configured if isinstance(configured,list) else [])
            now=datetime.now(timezone.utc).isoformat()
            if action=='add':
                slot=local_slot_to_iso(request.form.get('local_slot',''))
                db.execute('''INSERT INTO miet_checkout_slots (slot,active,created_at,updated_at)
                    VALUES (?,1,?,?) ON CONFLICT (slot) DO UPDATE SET active=1,updated_at=excluded.updated_at
                    RETURNING slot''',
                    (slot,now,now))
            elif action in {'close','reopen'}:
                slot=request.form.get('slot','')
                row=db.execute('SELECT slot FROM miet_checkout_slots WHERE slot=?',(slot,)).fetchone()
                if not row:raise ValueError('Übergabetermin nicht gefunden.')
                if action=='reopen' and datetime.fromisoformat(slot).astimezone(timezone.utc)<=datetime.now(timezone.utc):
                    raise ValueError('Nur künftige Übergabetermine können geöffnet werden.')
                db.execute('UPDATE miet_checkout_slots SET active=?,updated_at=? WHERE slot=?',
                           (1 if action=='reopen' else 0,now,slot))
                if action=='close':
                    rows=db.execute("SELECT id,payload FROM miet_checkout_holds WHERE status='pending'").fetchall()
                    for pending in rows:
                        q=json.loads(pending['payload'])['quote']
                        if slot in (q.get('start_slot'),q.get('end_slot')):affected.append(pending['id'])
            else:abort(400)
            db.commit()
        except Exception:
            db.rollback();raise
        finally:db.close()
        if affected:
            try:state=app.extensions.get('mos_public_booking') or setup()
            except Exception:
                state=None
                app.logger.exception('MOS Zahlungsdienst beim Schließen eines Übergabetermins nicht erreichbar')
            for hold_id in affected:
                try:
                    if state:state['service'].cancel_or_reconcile(hold_id,cancel=True)
                except Exception:
                    app.logger.exception('MOS offener Checkout nach Terminschließung muss geprüft werden')
                # A paid/uncertain provider race remains blocked for manual review;
                # already confirmed rentals are never changed by slot management.
                review_db=portal.get_db()
                try:
                    if not portal.USE_POSTGRES:review_db.execute('BEGIN IMMEDIATE')
                    suffix=' FOR UPDATE' if portal.USE_POSTGRES else ''
                    current=review_db.execute('SELECT status FROM miet_checkout_holds WHERE id=?'+suffix,
                                              (hold_id,)).fetchone()
                    if current and current['status']=='pending':
                        review_db.execute("UPDATE miet_checkout_holds SET status='review',grund='slot_closed_review' WHERE id=?",
                                          (hold_id,))
                        portal.flash('Ein offener Checkout ist wegen der Terminschließung in manueller Prüfung. Zahlungs- und Kartenstatus prüfen.','warning')
                    review_db.commit()
                except Exception:
                    review_db.rollback();raise
                finally:review_db.close()
        return redirect(url_for('mos_public.admin_slot_page'),code=303)

    @bp.get('/admin/<hold_id>/vertrag.pdf')
    @portal.admin_required
    def admin_contract_pdf(hold_id):
        return contract_pdf_response(hold_id)

    @bp.post('/admin/<hold_id>')
    @portal.admin_required
    def admin_action(hold_id):
        state=setup();action=request.form.get('action');reason=request.form.get('reason','').strip()
        h=state['service'].read(hold_id)
        q=json.loads(h['payload'])['quote']
        if not reason:raise ValueError('Begründung/Prüfvermerk erforderlich.')
        if action=='cancel':rid=state['ledger'].cancel(hold_id,admin=True,no_show=request.form.get('no_show')=='yes')
        elif action=='credit':
            key=request.form.get('request_id','')
            if not 20<=len(key)<=100:raise ValueError('Erstattungsreferenz fehlt.')
            rid=state['ledger'].credit(hold_id,int(request.form.get('cents','')),reason,key)
        elif action=='deposit':
            if q.get('deposit_authorized_cents'):
                db=portal.get_db()
                try:
                    rental=(db.execute('SELECT status FROM mietvorgaenge WHERE id=?',(h['mietvorgang_id'],)).fetchone()
                            if h['mietvorgang_id'] else None)
                finally:db.close()
                if not rental or rental['status']!='zurueck':
                    raise ValueError('Kartenreservierung erst nach protokollierter Rückgabe freigeben.')
                state['service'].release_deposit(hold_id,reason)
                return redirect(url_for('mos_public.admin_bookings'),code=303)
            rid=state['ledger'].deposit(hold_id,reason)
        elif action=='retry_refund':
            rid=request.form.get('refund_id','')
            if rid not in {r['id'] for r in account(hold_id)['refunds']}:abort(404)
        elif action=='reconcile':
            state['service'].cancel_or_reconcile(hold_id)
            return redirect(url_for('mos_public.admin_bookings'),code=303)
        else:abort(400)
        try:state['ledger'].process(rid)
        except Exception:
            portal.flash('Erstattungsauftrag gespeichert; Providerstatus unklar. Dieselbe Referenz erneut prüfen, keinen neuen Auftrag erzeugen.','warning')
        if action=='cancel' and q.get('deposit_authorized_cents'):
            try:state['service'].release_deposit(hold_id,reason)
            except Exception:
                portal.flash('Kartenreservierung noch offen; Providerstatus prüfen und Freigabe erneut ausführen.','warning')
        return redirect(url_for('mos_public.admin_bookings'),code=303)

    @bp.get('/')
    def index():
        db=portal.get_db()
        try:slots=[row['slot'] for row in listed_slots(db,True)]
        finally:db.close()
        return render_template('mos_public/index.html',listings=LISTINGS,slots=slots)

    @app.cli.command('mos-booking-reconcile')
    def reconcile_jobs():
        """Provider-only reconciliation; no confirmation without signed Checkout event."""
        state=setup();db=portal.get_db()
        try:
            holds=[r['id'] for r in db.execute("SELECT id FROM miet_checkout_holds WHERE status='pending'").fetchall()]
            refunds=[r['id'] for r in db.execute("SELECT id FROM miet_checkout_refunds WHERE status IN ('queued','submitted')").fetchall()]
        finally:db.close()
        errors=0
        for hid in holds:
            try:state['service'].cancel_or_reconcile(hid)
            except Exception:errors+=1
        db=portal.get_db()
        try:
            release_ids=[r['id'] for r in db.execute('''SELECT h.id FROM miet_checkout_holds h
                JOIN miet_checkout_cancellations c ON c.id=h.id
                JOIN miet_checkout_deposit_auths d ON d.hold_id=h.id
                WHERE d.status!='released' ''').fetchall()]
            active_ids=[r['id'] for r in db.execute('''SELECT h.id FROM miet_checkout_holds h
                JOIN miet_checkout_deposit_auths d ON d.hold_id=h.id
                WHERE h.status='confirmed' AND d.status!='released'
                AND NOT EXISTS (SELECT 1 FROM miet_checkout_cancellations c WHERE c.id=h.id)''').fetchall()]
        finally:db.close()
        for hid in release_ids:
            try:state['service'].release_deposit(hid,'Stornierung erneut abgleichen')
            except Exception:errors+=1
        for hid in active_ids:
            try:state['service'].reconcile_deposit(hid)
            except Exception:errors+=1
        for rid in refunds:
            try:state['ledger'].process(rid)
            except Exception:errors+=1
        # A provider-paid Checkout must remain unconfirmed until its signed
        # webhook arrives. Surface missing delivery to the operator instead of
        # letting a successful reconciliation run conceal the paid hold.
        db=portal.get_db()
        try:
            stale_before=int(time.time())-300
            pending_sessions=[dict(r) for r in db.execute('''SELECT id,session_id FROM miet_checkout_holds
                WHERE status='pending' AND session_id IS NOT NULL AND mietvorgang_id IS NULL''').fetchall()]
            uncertain_checkouts=db.execute('''SELECT COUNT(*) AS n FROM miet_checkout_holds h
                JOIN miet_checkout_creation_attempts a ON a.hold_id=h.id
                WHERE h.status='pending' AND h.session_id IS NULL AND a.created_at<=?''',
                (stale_before,)).fetchone()['n']
            unresolved_deposits=db.execute('''SELECT COUNT(*) AS n FROM miet_checkout_deposit_auths
                WHERE status IN ('releasing','review') OR (status='creating' AND created_at<=?)''',
                (stale_before,)).fetchone()['n']
            review_count=db.execute("SELECT COUNT(*) AS n FROM miet_checkout_holds WHERE status='review'").fetchone()['n']
            failed_refunds=db.execute("SELECT COUNT(*) AS n FROM miet_checkout_refunds WHERE status='failed'").fetchone()['n']
        finally:db.close()
        paid_without_webhook=0
        for pending in pending_sessions:
            try:
                h=state['service'].read(pending['id'])
                if h['status']!='pending' or h['session_id']!=pending['session_id'] or h['mietvorgang_id']:
                    continue
                session=state['gateway'].retrieve(pending['session_id'])
                state['service'].validate(h,session)
                if session.get('status')=='complete' and session.get('payment_status')=='paid':
                    current=state['service'].read(h['id'])
                    if current['status']=='pending' and current['session_id']==pending['session_id'] and not current['mietvorgang_id']:
                        paid_without_webhook+=1
            except Exception:errors+=1
        import click
        click.echo(f'Geprüft: {len(holds)} Reservierungen, {len(refunds)} Erstattungen, '
                   f'{len(release_ids)} Kartenfreigaben, {len(active_ids)} aktive Kartenreservierungen; '
                   f'offene Fehler: {errors}; bezahlte Checkouts ohne Webhook: {paid_without_webhook}; '
                   f'unklare Checkout-Aufträge: {uncertain_checkouts}; ungeklärte Kartenreservierungen: {unresolved_deposits}; '
                   f'Prüffälle: {review_count}; fehlgeschlagene Erstattungen: {failed_refunds}')
        if (errors or paid_without_webhook or uncertain_checkouts or unresolved_deposits
                or review_count or failed_refunds):
            raise click.ClickException('Offene Zahlungs- oder Prüffälle; Admin-Prüfung erforderlich.')

    @bp.post('/quote')
    def preview():
        q=quote(request.form.get('vehicle'),request.form.get('start'),request.form.get('end'))
        token=serializer().dumps({'quote':q,'nonce':secrets.token_urlsafe(20)})
        return render_template('mos_public/quote.html',q=q,token=token)

    @bp.post('/checkout')
    def checkout():
        if not app.config['MOS_PUBLIC_BOOKING'].get('enabled'):abort(404)
        if request.form.get('accept')!='yes':raise ValueError('Bitte die angezeigten Mietbedingungen ausdrücklich bestätigen.')
        token=request.form.get('quote_token','')
        try:data=serializer().loads(token,max_age=900)
        except BadSignature:raise ValueError('Preisübersicht abgelaufen oder ungültig. Bitte neu prüfen.')
        q=data['quote']
        if q['owner_hash']!=owner():abort(404)
        state=setup();key=hashlib.sha256((owner()+token).encode()).hexdigest()
        customer={'name':request.form.get('name','').strip(),'email':request.form.get('email','').strip(),'telefon':''}
        if not customer['name'] or not customer['email'] or len(customer['name'])>150 or len(customer['email'])>254:
            raise ValueError('Bitte gültigen Namen und E-Mail angeben.')
        if request.form.get('sign_confirm')!='yes':
            raise ValueError('Bitte den Vertrag vor der Zahlung ausdrücklich unterschreiben.')
        # A repeated POST retries the same immutable hold, even though it now occupies the period.
        db=portal.get_db()
        try:old=db.execute('SELECT id FROM miet_checkout_holds WHERE request_key=?',(key,)).fetchone()
        finally:db.close()
        if old:
            h,p=owned(old['id'])
            if p['customer']!=customer:
                raise ValueError('Kundendaten nach der Unterschrift geändert. Bitte neue Preisübersicht öffnen.')
            return retry(h['id'])
        if not old and quote(q['slug'],q['start_slot'],q['end_slot'])!=q:
            raise ValueError('Preis oder Regeln geändert. Bitte neue Übersicht bestätigen.')
        q=presign_quote(q,customer,request.form.get('signature_data',''))
        h=state['service'].reserve(key,q['vehicle_id'],datetime.fromisoformat(q['start_slot']).date().isoformat(),
                                  datetime.fromisoformat(q['end_slot']).date().isoformat(),customer,q)
        return retry(h['id'])

    @bp.post('/status/<hold_id>/retry')
    def retry(hold_id):
        h,p=owned(hold_id)
        if h['status']!='pending':return redirect(url_for('mos_public.status',hold_id=hold_id),code=303)
        if not app.config['MOS_PUBLIC_BOOKING'].get('enabled'):
            return redirect(url_for('mos_public.status',hold_id=hold_id),code=303)
        closed=closed_slot_response(hold_id,p['quote'])
        if closed:return closed
        signed_payload(p)
        state=setup()
        if p['quote'].get('deposit_authorized_cents'):
            try:
                state['service'].prepare_deposit(hold_id)
                deposit=state['service'].reconcile_deposit(hold_id)
            except Exception:
                if deposit_state(hold_id)=='released':
                    return render_status(hold_id,'Diese Kartenreservierung wurde freigegeben. Bitte mit einer Kreditkarte und einem passenden Termin neu buchen.'),409
                app.logger.exception('MOS Kartenreservierung konnte nicht geprüft werden')
                return render_status(hold_id,'Kartenreservierung derzeit nicht erreichbar. Bitte Status erneut prüfen.'),503
            if not deposit.get('ready'):
                if deposit.get('status') in {'requires_payment_method','requires_confirmation','requires_action'}:
                    return redirect(url_for('mos_public.deposit_page',hold_id=hold_id),code=303)
                return render_status(hold_id,'Kartenreservierung nicht ausreichend gültig. Es wurde kein Mietpreis abgebucht.'),409
        closed=closed_slot_response(hold_id,p['quote'])
        if closed:return closed
        try:s=setup()['service'].create_checkout(hold_id)
        except Exception:
            return render_status(hold_id,
                'Checkout derzeit nicht erreichbar. Bitte prüfe den Status und versuche es bei offener Reservierung erneut.'),503
        return redirect(s['url'],code=303)

    @bp.get('/status/<hold_id>/kaution')
    def deposit_page(hold_id):
        h,p=owned(hold_id)
        if (h['status']!='pending' or not app.config['MOS_PUBLIC_BOOKING'].get('enabled')
                or not p['quote'].get('deposit_authorized_cents')):
            return redirect(url_for('mos_public.status',hold_id=hold_id),code=303)
        closed=closed_slot_response(hold_id,p['quote'])
        if closed:return closed
        signed_payload(p)
        state=setup()
        try:
            state['service'].prepare_deposit(hold_id)
            deposit=state['service'].reconcile_deposit(hold_id)
        except Exception:
            if deposit_state(hold_id)=='released':
                return render_status(hold_id,'Diese Kartenreservierung wurde freigegeben. Bitte mit einer Kreditkarte und einem passenden Termin neu buchen.'),409
            app.logger.exception('MOS Kartenformular konnte nicht vorbereitet werden')
            return render_status(hold_id,'Kartenreservierung derzeit nicht erreichbar. Bitte Status erneut prüfen.'),503
        if deposit.get('ready'):
            return redirect(url_for('mos_public.status',hold_id=hold_id),code=303)
        if deposit.get('status') not in {'requires_payment_method','requires_confirmation','requires_action'}:
            return render_status(hold_id,'Kartenreservierung nicht verfügbar. Der Mietpreis wurde nicht abgebucht.'),409
        return render_template('mos_public/deposit.html',h=h,q=p['quote'],
            client_secret=deposit['client_secret'],
            publishable_key=app.config.get('MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY',''),
            offline=state['cfg']['mode']=='offline')

    @bp.post('/status/<hold_id>/kaution-test')
    def simulate_deposit(hold_id):
        h,p=owned(hold_id)
        state=setup()
        if (state['cfg']['mode']!='offline' or h['status']!='pending'
                or not p['quote'].get('deposit_authorized_cents')):
            abort(404)
        closed=closed_slot_response(hold_id,p['quote'])
        if closed:return closed
        signed_payload(p)
        deposit=state['service'].prepare_deposit(hold_id)
        state['gateway'].authorize_deposit_intent(deposit['id'])
        state['service'].reconcile_deposit(hold_id)
        return redirect(url_for('mos_public.status',hold_id=hold_id),code=303)

    @bp.get('/status/<hold_id>')
    def status(hold_id):
        return render_status(hold_id)

    def render_status(hold_id,error=None):
        h,p=owned(hold_id)
        try:setup()['service'].cancel_or_reconcile(hold_id)
        except Exception:pass
        h,_=owned(hold_id)
        deposit=None
        if p['quote'].get('deposit_authorized_cents'):
            card_state=deposit_state(hold_id)
            if card_state in {'released','releasing','review'}:
                deposit={'status':card_state,'ready':False}
            elif card_state:
                try:deposit=setup()['service'].reconcile_deposit(hold_id)
                except Exception:
                    if deposit_state(hold_id)=='released':
                        h,_=owned(hold_id)
                        deposit={'status':'released','ready':False}
                        error=error or 'Diese Kartenreservierung konnte nicht verwendet werden und wurde freigegeben. Bitte neu buchen.'
                    else:
                        app.logger.exception('MOS Kartenreservierung konnte nicht abgeglichen werden')
                        error=error or 'Kartenreservierung muss geprüft werden. Bitte Werkstatt kontaktieren.'
        acc=account(hold_id)
        if h['status']=='confirmed':
            try:finalize_contract(portal,hold_id)
            except Exception:
                app.logger.exception('MOS Vertrags-PDF konnte nicht abgeschlossen werden')
                error=error or 'Zahlung bestätigt; die Vertragskopie wird noch erstellt. Bitte aktualisiere den Status.'
        db=portal.get_db()
        try:signed=read_contract(db,hold_id)
        finally:db.close()
        presigned=p['quote'].get('signature_png_base64')
        contract=None
        if presigned:
            try:contract,_,_=signed_payload(p)
            except ValueError:
                app.logger.exception('MOS Vorab-Unterschrift stimmt nicht mit der Buchung überein')
                error=error or 'Die gespeicherte Unterschrift muss von der Werkstatt geprüft werden.'
                presigned=None
        if signed and not contract:
            contract=json.loads(signed['contract_json'])
        signed_at=signed['signed_at'] if signed else p['quote'].get('signed_at')
        signed_local=(datetime.fromisoformat(signed_at).astimezone(ZoneInfo('Europe/Berlin'))
                      .strftime('%d.%m.%Y um %H:%M Uhr')) if signed_at else None
        return render_template('mos_public/status.html',h=h,q=p['quote'],account=acc,error=error,deposit=deposit,
                               contract=contract,signed=signed,signed_local=signed_local,
                               presigned=presigned)

    @bp.get('/status/<hold_id>/vertrag.pdf')
    def signed_contract_pdf(hold_id):
        owned(hold_id)
        return contract_pdf_response(hold_id)

    def contract_pdf_response(hold_id):
        db=portal.get_db()
        try:contract=read_contract(db,hold_id)
        finally:db.close()
        if not contract:abort(404)
        from base64 import b64decode
        from flask import make_response
        pdf=b64decode(contract['pdf_base64'],validate=True)
        if hashlib.sha256(pdf).hexdigest()!=contract['pdf_sha256']:abort(500)
        response=make_response(pdf)
        response.headers['Content-Type']='application/pdf'
        response.headers['Content-Disposition']=f'attachment; filename="MOS-Mietvertrag-{hold_id}.pdf"'
        return response

    @bp.post('/status/<hold_id>/cancel')
    def cancel(hold_id):
        owned(hold_id)
        try:setup()['service'].cancel_or_reconcile(hold_id,cancel=True)
        except Exception:
            raise ValueError('Providerstatus unklar; Reservierung bleibt bis zur Klärung gesperrt.')
        return redirect(url_for('mos_public.status',hold_id=hold_id),code=303)

    @bp.post('/webhook')
    def webhook():
        state=setup()
        if request.content_length and request.content_length>65536:abort(413)
        import stripe
        try:
            body=request.get_data()
            state['service'].handle_signed_event(body,request.headers.get('Stripe-Signature',''),state['secret'])
        except (ValueError,KeyError,TypeError,stripe.SignatureVerificationError):return 'Invalid test event',400
        except Exception:return 'Retry later',503
        try:
            session_id=json.loads(body)['data']['object']['id']
            db=portal.get_db()
            try:row=db.execute('SELECT id FROM miet_checkout_holds WHERE session_id=?',(session_id,)).fetchone()
            finally:db.close()
            if row:finalize_contract(portal,row['id'])
        except Exception:return 'Retry later',503
        return '',204

    @bp.route('/simulate/<sid>',methods=['GET','POST'])
    def simulate(sid):
        state=setup()
        if state['cfg']['mode']!='offline':abort(404)
        try:s=state['gateway'].retrieve(sid)
        except Exception:abort(404)
        h,p=owned(s['metadata']['hold_id'])
        if request.method=='POST':
            if s['status']=='open':state['gateway'].pay(sid)
            raw,sig=state['gateway'].signed_event(sid)
            state['service'].handle_signed_event(raw,sig,state['secret'])
            finalize_contract(portal,h['id'])
            return redirect(url_for('mos_public.status',hold_id=h['id']),code=303)
        return render_template('mos_public/simulate.html',h=h,q=p['quote'])

    app.register_blueprint(bp)
