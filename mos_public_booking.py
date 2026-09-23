"""Public-facing TEST flow; explicit configuration, isolated portal DB, no live mode."""
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
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
        for key in ('included_km_day','extra_km_cents','max_days'):
            if type(cfg[key]) is not int or cfg[key]<1:raise ValueError('Test-Tarif unvollständig.')
        if not cfg['slots'] or len(set(cfg['slots'])) != len(cfg['slots']):raise ValueError('Test-Slots fehlen.')
        for slot in cfg['slots']:
            dt=datetime.fromisoformat(slot)
            if dt.tzinfo is None or dt.astimezone(ZoneInfo('Europe/Berlin')).isoformat()!=slot:
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
        service=SharedCheckout(portal,gateway,cfg['origin']+prefix+'/status')
        app.config['MOS_SHARED_CHECKOUT_ENABLED']=bool(app.config['MOS_PUBLIC_BOOKING'].get('enabled'))
        db=portal.get_db()
        try:init_refund_schema(db);init_contract_schema(db);db.commit()
        finally:db.close()
        existing={'cfg':cfg,'service':service,'gateway':gateway,'secret':secret,'ledger':RefundLedger(service)}
        app.extensions['mos_public_booking']=existing
        return existing

    @bp.before_request
    def guard():
        cfg=app.config.get('MOS_PUBLIC_BOOKING',{})
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
        cfg=state.get('cfg',{})
        return {'live':cfg.get('mode')=='live','booking_config':cfg}

    @bp.after_request
    def headers(response):
        response.headers.update({'Cache-Control':'no-store','X-Robots-Tag':'noindex, nofollow',
                                 'Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff',
                                 'X-Frame-Options':'DENY'})
        return response

    @bp.errorhandler(ValueError)
    def invalid(error):
        return render_template('mos_public/error.html',message=str(error)),409

    def owner():
        return hashlib.sha256(session['mos_public_owner'].encode()).hexdigest()

    def serializer():return URLSafeTimedSerializer(app.secret_key,salt='mos-public-quote-v1')

    def quote(slug,start,end):
        state=setup();cfg=state['cfg']
        if slug not in LISTINGS or start not in cfg['slots'] or end not in cfg['slots']:
            raise ValueError('Bitte ein Testfahrzeug und freigegebene Testtermine wählen.')
        a,b=datetime.fromisoformat(start),datetime.fromisoformat(end)
        seconds=(b.astimezone(timezone.utc)-a.astimezone(timezone.utc)).total_seconds()
        days=math.ceil(seconds/86400)
        if a<=datetime.now(timezone.utc) or seconds<=0 or days>cfg['max_days']:
            raise ValueError('Ungültiger Testzeitraum.')
        f=cfg['fleet'][slug];rate=f['daily_cents']
        if days>=f.get('discount_after_days',cfg['max_days']+1):rate=f['discount_cents']
        db=portal.get_db()
        try:
            vehicle=db.execute('SELECT id,bezeichnung,kennzeichen,fin_nummer,aktiv,status FROM mietfahrzeuge WHERE id=?',(f['id'],)).fetchone()
            if not vehicle or not int(vehicle['aktiv'] or 0) or portal.normalize_mietfahrzeug_status(vehicle['status']) in {'bald','wartung','inaktiv'}:
                raise ValueError('Zugeordnetes Testfahrzeug ist nicht verfügbar.')
            if cfg['mode']=='live' and vehicle['bezeichnung']!=f['expected_name']:
                raise ValueError('Fahrzeugzuordnung muss von der Werkstatt geprüft werden.')
            if not portal.mietfahrzeug_zeitraum_frei_db(db,f['id'],a.date(),b.date()):
                raise ValueError('Der Zeitraum ist bereits belegt. Bitte andere Termine wählen.')
        finally:db.close()
        return {'slug':slug,'vehicle_id':f['id'],'vehicle_name':LISTINGS[slug],
            'portal_vehicle_name':vehicle['bezeichnung'],
            'vehicle_plate':vehicle['kennzeichen'] or '', 'vehicle_vin':vehicle['fin_nummer'] or '',
            'start_slot':start,'end_slot':end,
            'days':days,'daily_cents':rate,'rental_cents':days*rate,
            'amount_cents':days*rate+(50000 if cfg['mode'] in {'live','stripe_test'} else 0),'currency':'eur',
            'deposit_charged_cents':50000 if cfg['mode'] in {'live','stripe_test'} else 0,
            'checkout_deposit':cfg['mode'] in {'live','stripe_test'},
            'included_km':days*cfg['included_km_day'],'extra_km_cents':cfg['extra_km_cents'],
            'deposit_cents':cfg['deposit_cents'],'deductible_cents':cfg['deductible_cents'],
            'rules_version':cfg['terms_version'],'terms_text':cfg['terms_text'],'owner_hash':owner(),
            'test_only':cfg['mode']!='live','vat_included':True,
            'lessor_name':LESSOR_NAME,'lessor_address':LESSOR_ADDRESS}

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
        text+=f"\nMiete: {q.get('rental_cents',q['amount_cents'])/100:.2f} EUR inkl. MwSt.\nKaution eingezogen: {q.get('deposit_charged_cents',0)/100:.2f} EUR\nGesamtzahlung: {q['amount_cents']/100:.2f} EUR"
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

    @bp.post('/status/<hold_id>/stornieren')
    def cancel_paid(hold_id):
        h,p=owned(hold_id)
        if request.form.get('confirm')!='yes':raise ValueError('Stornierung bitte ausdrücklich bestätigen.')
        rid=setup()['ledger'].cancel(hold_id)
        try:setup()['ledger'].process(rid)
        except Exception:pass  # Durable queued operation remains visible to admin/customer.
        return redirect(url_for('mos_public.status',hold_id=hold_id),code=303)

    @bp.get('/admin')
    @portal.admin_required
    def admin_bookings():
        db=portal.get_db()
        try:
            holds=[dict(r) for r in db.execute('''SELECT h.*,c.signed_at FROM miet_checkout_holds h
                LEFT JOIN miet_checkout_contracts c ON c.hold_id=h.id
                ORDER BY h.expires_at DESC LIMIT 100''').fetchall()]
        finally:db.close()
        for h in holds:
            h['q']=json.loads(h['payload'])['quote'];h['account']=account(h['id'])
            if h['signed_at']:
                h['signed_at']=datetime.fromisoformat(h['signed_at']).astimezone(ZoneInfo('Europe/Berlin')).strftime('%d.%m.%Y um %H:%M Uhr')
        return render_template('mos_public/admin.html',holds=holds,request_id=secrets.token_urlsafe(24))

    @bp.get('/admin/<hold_id>/vertrag.pdf')
    @portal.admin_required
    def admin_contract_pdf(hold_id):
        return contract_pdf_response(hold_id)

    @bp.post('/admin/<hold_id>')
    @portal.admin_required
    def admin_action(hold_id):
        state=setup();action=request.form.get('action');reason=request.form.get('reason','').strip()
        h=state['service'].read(hold_id)
        if not reason:raise ValueError('Begründung/Prüfvermerk erforderlich.')
        if action=='cancel':rid=state['ledger'].cancel(hold_id,admin=True,no_show=request.form.get('no_show')=='yes')
        elif action=='credit':
            key=request.form.get('request_id','')
            if not 20<=len(key)<=100:raise ValueError('Erstattungsreferenz fehlt.')
            rid=state['ledger'].credit(hold_id,int(request.form.get('cents','')),reason,key)
        elif action=='deposit':rid=state['ledger'].deposit(hold_id,reason)
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
        return redirect(url_for('mos_public.admin_bookings'),code=303)

    @bp.get('/')
    def index():
        cfg=setup()['cfg']
        return render_template('mos_public/index.html',listings=LISTINGS,slots=cfg['slots'])

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
        for rid in refunds:
            try:state['ledger'].process(rid)
            except Exception:errors+=1
        import click
        click.echo(f'Geprüft: {len(holds)} Reservierungen, {len(refunds)} Erstattungen; offene Fehler: {errors}')
        if errors:raise click.ClickException('Offene Providerfehler; Admin-Prüfung erforderlich.')

    @bp.post('/quote')
    def preview():
        q=quote(request.form.get('vehicle'),request.form.get('start'),request.form.get('end'))
        token=serializer().dumps({'quote':q,'nonce':secrets.token_urlsafe(20)})
        return render_template('mos_public/quote.html',q=q,token=token)

    @bp.post('/checkout')
    def checkout():
        if not app.config['MOS_PUBLIC_BOOKING'].get('enabled'):abort(404)
        if request.form.get('accept')!='yes':raise ValueError('Bitte den Testentwurf ausdrücklich bestätigen.')
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
        signed_payload(p)
        try:s=setup()['service'].create_checkout(hold_id)
        except Exception:
            return render_status(hold_id,
                'Checkout derzeit nicht erreichbar. Bitte prüfe den Status und versuche es bei offener Reservierung erneut.'),503
        return redirect(s['url'],code=303)

    @bp.get('/status/<hold_id>')
    def status(hold_id):
        return render_status(hold_id)

    def render_status(hold_id,error=None):
        h,p=owned(hold_id)
        try:setup()['service'].cancel_or_reconcile(hold_id)
        except Exception:pass
        h,_=owned(hold_id)
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
        return render_template('mos_public/status.html',h=h,q=p['quote'],account=acc,error=error,
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
