"""Shop operations on the explicitly provisioned shop SQLite.

The ledger reader remains read-only. Explicit technical review decisions use
the separately configured review service; never a provider or financial action.
No schema creation, portal DB fallback or provider lookup.
Registration/construction performs no database IO. Every request requires the
existing portal's real administrator session, not office or partner access.
"""
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime
from hashlib import sha256
import hmac
import json
from pathlib import Path
import re
import secrets
import sqlite3
from urllib.parse import urlsplit

from flask import Blueprint, abort, current_app, has_request_context, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException
from shop_order_store import OrderStore
from shop_storage import ShopStorage

ORDER=re.compile(r'sho_[a-f0-9]{32}')
WITHDRAWAL=re.compile(r'swd_[a-f0-9]{32}')
TABLES={'shop_orders_v2','shop_payment_attempts_v2','shop_fulfillment_v2',
        'shop_fulfillment_positions_v3','shop_order_workflow_v2','shop_order_receipts_v2','shop_withdrawals_v2',
        'shop_order_appointment_requests_v2'}
BASE_TABLES={'shop_quotes_v2','shop_orders_v2','shop_payment_attempts_v2',
             'shop_payment_events_v2','shop_buyer_sessions_v2'}
BASE_TRIGGERS={'shop_quotes_v2_immutable','shop_orders_v2_immutable'}
STATUS={'draft':'Entwurf','payment_pending':'Zahlung ausstehend','authorized':'Betrag reserviert',
        'paid':'Zahlung eingezogen','payment_failed':'Zahlung fehlgeschlagen','cancelled':'Reservierung beendet'}
PAYMENT={'claimed':'Zahlung wird vorbereitet','unknown':'Zahlungsausgang unklar',
         'bound':'Zahlungsseite erstellt','authorized':'Betrag reserviert','paid':'Zahlung eingezogen',
         'failed':'Zahlung fehlgeschlagen','cancelled':'Reservierung beendet'}
FLOW={'queued':'Vorgemerkt','running':'In Bearbeitung','awaiting_review':'Technische Prüfung ausstehend','fulfilled':'Abgeschlossen',
      'cancelled':'Abgebrochen','manual_review':'Manuelle Prüfung erforderlich'}
FULFILLMENT={'open':'Noch nicht vollständig bearbeitet','accepted':'Alle Positionen angenommen',
             'rejected':'Lieferant hat abgelehnt','manual_review':'Manuelle Prüfung erforderlich'}
ACTION={'claimed':'Zahlungsaktion läuft','unknown':'Zahlungsaktion unklar',
        'captured':'Betrag eingezogen','cancelled':'Reservierung beendet','conflict':'Manuelle Prüfung erforderlich'}
MAIL={'queued':'Vorgemerkt','sending':'Übergabe läuft','submitted':'An Maildienst übergeben',
      'unknown':'Versand unklar – prüfen','not_sent':'Nicht gesendet'}
DESTINATION={'customer':'Direkt zum Kunden','workshop':'Zur Werkstatt'}
SERVICE={'mounting':'Montage','painting':'Lackierung','installation':'Einbau','oil_service':'Ölservice','care':'Aufbereitung'}
REVIEW_REQUIREMENTS={'mixed_cart_manual_review_required':'Vollständigen Warenkorb und alle Lieferwege bestätigen',
    'fitment_unconfirmed':'Genaue Fahrzeugausführung und Felgenfreigabe prüfen',
    'assembly_fitment_unconfirmed':'Genaue Rad-Reifen-Kombination einschließlich Auflagen prüfen'}
REVIEW_STATES={'pending':'Technische Prüfung offen','approved':'Prüfung verbindlich freigegeben',
    'rejected':'Prüfung abgelehnt','expired':'Kartenreservierung abgelaufen'}
CSRF=re.compile(r'[A-Za-z0-9_-]{32,128}')
HASH=re.compile(r'[a-f0-9]{64}')


def _text(value,limit=200):
    return ''.join(c for c in value if ord(c)>=32 and ord(c)!=127)[:limit] if isinstance(value,str) else ''


def _money(value):
    if type(value) is not int or not 0<=value<=100_000_000:return 'Nicht verfügbar'
    return f'{value//100:,}'.replace(',','.')+','+f'{value%100:02d} €'


def _date(value):
    try:
        value=datetime.fromisoformat(value.replace('Z','+00:00'))
        if value.tzinfo is None or value.utcoffset() is None:raise ValueError()
        return value.strftime('%d.%m.%Y, %H:%M %z')
    except (ValueError,TypeError,AttributeError):return 'Nicht verfügbar'


class ShopAdminReader:
    def __init__(self,storage,store,*,existing_only=False):
        # No connections or filesystem checks until an authenticated request.
        if type(existing_only) is not bool:raise ValueError('invalid_shop_admin_configuration')
        self.storage,self.store=storage,store
        self.existing_only=existing_only

    @contextmanager
    def read(self):
        db=None
        try:
            storage=self.storage
            if (not isinstance(storage,ShopStorage) or not isinstance(self.store,OrderStore)
                    or storage.ready is not True and not self.existing_only):
                abort(404)
            storage._paths()
            if (not storage.path.is_file() or not storage.marker.is_file()
                    or storage.marker.stat().st_size!=len(b'shop_storage_v2\n')
                    or storage.marker.read_bytes()!=b'shop_storage_v2\n'):
                abort(404)
            db=sqlite3.connect(storage.path.as_uri()+'?mode=ro',uri=True,timeout=2)
            db.row_factory=sqlite3.Row
            db.execute('PRAGMA query_only=ON')
            # This connection can never mutate a journal even if a future
            # handler accidentally gains an UPDATE or provider-side helper.
            allowed={sqlite3.SQLITE_SELECT,sqlite3.SQLITE_READ,sqlite3.SQLITE_FUNCTION,
                     sqlite3.SQLITE_TRANSACTION,sqlite3.SQLITE_RECURSIVE}
            db.set_authorizer(lambda action,*args:sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY)
            db.execute('BEGIN')
            names=tuple(sorted(TABLES|BASE_TABLES|BASE_TRIGGERS))
            objects={(row[0],row[1]) for row in db.execute("SELECT type,name FROM sqlite_master WHERE name IN ("+','.join('?' for _ in names)+')',names)}
            tables={name for kind,name in objects if kind=='table'}
            if (not BASE_TABLES<=tables
                    or not {('trigger',name) for name in BASE_TRIGGERS}<=objects):
                abort(503,description='Shopbestellungen sind vorübergehend nicht verfügbar.')
            yield db,tables
        except (OSError,sqlite3.Error,ValueError):
            abort(503,description='Shopbestellungen sind vorübergehend nicht verfügbar.')
        finally:
            if db is not None:db.close()

    @staticmethod
    def _related(db,tables,rows):
        ids=[row['id'] for row in rows]
        if not ids:return {}
        placeholders=','.join('?' for _ in ids)
        result={oid:{'payment':None,'workflow':None,'fulfillment':None,'action':None,'uncertain_positions':0,'receipts':{}} for oid in ids}
        queries=(('shop_payment_attempts_v2','SELECT order_id,state FROM shop_payment_attempts_v2','payment'),
                 ('shop_order_workflow_v2','SELECT order_id,state FROM shop_order_workflow_v2','workflow'),
                 ('shop_fulfillment_v2','SELECT order_id,state,action_state FROM shop_fulfillment_v2','fulfillment'))
        for table,sql,key in queries:
            if table not in tables:continue
            for row in db.execute(sql+' WHERE order_id IN ('+placeholders+') LIMIT 100',ids):
                result[row['order_id']][key]=row['state']
                if key=='fulfillment':result[row['order_id']]['action']=row['action_state']
        if 'shop_order_appointment_requests_v2' in tables:
            for row in db.execute('''SELECT order_id, strftime('%d.%m.%Y', requested_date) AS day, requested_time
                FROM shop_order_appointment_requests_v2 WHERE order_id IN ('''+placeholders+')',ids):
                result[row['order_id']]['appointment']=f"{row['day']}, {row['requested_time']} Uhr"
        if 'shop_fulfillment_positions_v3' in tables:
            for row in db.execute('''SELECT order_id,SUM(CASE WHEN state IN ('claimed','unknown') THEN 1 ELSE 0 END) AS uncertain
                FROM shop_fulfillment_positions_v3 WHERE order_id IN ('''+placeholders+') GROUP BY order_id LIMIT 100',ids):
                result[row['order_id']]['uncertain_positions']=row['uncertain']
        if 'shop_order_receipts_v2' in tables:
            for row in db.execute('''SELECT order_id,
                MAX(CASE WHEN kind='order_received' THEN state END) AS received,
                MAX(CASE WHEN kind='order_confirmed' THEN state END) AS confirmed,
                MAX(CASE WHEN kind='order_cancelled' THEN state END) AS cancelled
                FROM shop_order_receipts_v2 WHERE order_id IN ('''+placeholders+') GROUP BY order_id LIMIT 100',ids):
                result[row['order_id']]['receipts']={key:row[key] for key in ('received','confirmed','cancelled') if row[key] is not None}
        return result

    @staticmethod
    def _summary(row,related):
        state=row['status']
        attention=(related['payment']=='unknown' or related['workflow'] in {'manual_review','awaiting_review'}
            or related['fulfillment']=='manual_review' or related['action'] in {'unknown','conflict'}
            or related['uncertain_positions']>0
            or any(value in {'unknown','not_sent'} for value in related['receipts'].values()))
        return {'id':row['id'],'created':_date(row['created_at']),'amount':_money(row['amount_gross_cent']) if row['currency']=='eur' else 'Währung prüfen',
            'status':STATUS.get(state,'Status unbekannt'),'mode':{'live':'Live','test':'Test'}.get(row['mode'],'Unbekannt'),
            'payment':PAYMENT.get(related['payment'],'Noch kein Zahlungsversuch'),
            'workflow':FLOW.get(related['workflow'],'Noch nicht gestartet'),
            'appointment':related.get('appointment'),
            'fulfillment':FULFILLMENT.get(related['fulfillment'],'Noch nicht gestartet'),
            'action':ACTION.get(related['action'],''),'attention':attention,
            'uncertain_positions':related['uncertain_positions'],
            'receipts':{key:MAIL.get(value,'Versandstatus unbekannt') for key,value in related['receipts'].items()},
            'invoice':'Ausstehend – keine Rechnung erstellt'}

    def orders(self,*,page=1,per_page=25):
        if type(page) is not int or not 1<=page<=9999 or type(per_page) is not int or not 1<=per_page<=100:abort(400)
        with self.read() as (db,tables):
            rows=list(db.execute('''SELECT id,mode,status,amount_gross_cent,currency,created_at FROM shop_orders_v2
                ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?''',(per_page,(page-1)*per_page)))
            if any(not isinstance(row['id'],str) or not ORDER.fullmatch(row['id']) for row in rows):abort(503)
            related=self._related(db,tables,rows)
            has_next=db.execute('SELECT 1 FROM shop_orders_v2 LIMIT 1 OFFSET ?',(page*per_page,)).fetchone() is not None
            return [self._summary(row,related[row['id']]) for row in rows],has_next

    def detail(self,order_id):
        if not isinstance(order_id,str) or not ORDER.fullmatch(order_id):abort(404)
        with self.read() as (db,tables):
            row=db.execute('''SELECT id,mode,status,amount_gross_cent,currency,created_at,quote_hash,
                substr(snapshot_json,1,131073) AS snapshot_json FROM shop_orders_v2 WHERE id=? LIMIT 1''',(order_id,)).fetchone()
            if row is None:abort(404)
            summary=self._summary(row,self._related(db,tables,[row])[order_id])
            if summary['appointment']:
                stored = db.execute('SELECT delivery_context_json FROM shop_orders_v2 WHERE id=?', (order_id,)).fetchone()
                context = json.loads(stored['delivery_context_json'])
                contact = context.get('contact') if isinstance(context, dict) else None
                if isinstance(contact, dict) and any(contact.get(key) for key in ('name','phone','email')):
                    summary['appointment_contact']={key:_text(contact.get(key),254) for key in ('name','phone','email')}
            summary['_review_binding']={key:row[key] for key in ('mode','status','amount_gross_cent','currency','quote_hash')}
            try:
                if len(row['snapshot_json'].encode('utf-8'))>131072:raise ValueError()
                snapshot=json.loads(row['snapshot_json'])
                if not isinstance(snapshot,dict) or not isinstance(snapshot.get('lines'),list) or len(snapshot['lines'])>100:raise ValueError()
                lines=[]
                for line in snapshot['lines']:
                    if not isinstance(line,dict) or type(line.get('quantity')) is not int or not 1<=line['quantity']<=20:raise ValueError()
                    services=line.get('services',[])
                    if not isinstance(services,list) or len(services)>20:raise ValueError()
                    lines.append({'id':_text(line.get('id'),64),'name':_text(line.get('name')),'quantity':line['quantity'],
                        'unit_price':_money(line.get('unit_gross_cent')),'items_price':_money(line.get('items_gross_cent')),
                        'destination':DESTINATION.get(line.get('destination'),'Versandziel ungeklärt'),
                        'services':[{'name':SERVICE.get(service.get('code'),'Erforderliches Zubehör'),
                                     'price':_money(service.get('gross_cent'))} for service in services if isinstance(service,dict)]})
                shipments=snapshot.get('shipments',[])
                if not isinstance(shipments,list) or len(shipments)>2:raise ValueError()
                summary.update(lines=lines,shipments=[{'destination':DESTINATION.get(item.get('destination'),'Versandziel ungeklärt'),
                    'price':_money(item.get('gross_cent'))} for item in shipments if isinstance(item,dict)])
            except (ValueError,TypeError,AttributeError,KeyError,UnicodeError):
                summary.update(lines=[],shipments=[],snapshot_unavailable=True,attention=True)
            withdrawals, more = self._withdrawals(db,tables,order_id=order_id,mode=row['mode'],per_page=25)
            summary.update(withdrawals=withdrawals,more_withdrawals=more)
            return summary

    @staticmethod
    def _withdrawal_summary(row):
        result={'id':row['id'],'created':_date(row['received_at']),
            'mode':{'test':'Test','live':'Live'}.get(row['mode'],'Unbekannt'),
            'state':'Persönlich prüfen' if row['state']=='manual_review' else 'Bearbeitungsstand prüfen',
            'mail':MAIL.get(row['mail_state'],'Versandstatus unbekannt'),
            'attention':True,'order_id':None,'name':'','email':'','reference':'','scope':'','invalid':False}
        try:
            raw=row['declaration_json']
            if not isinstance(raw,str) or len(raw.encode('utf-8'))>16384:raise ValueError()
            declaration=json.loads(raw)
            if not isinstance(declaration,dict) or set(declaration)!={'name','email','contract_reference','scope'}:raise ValueError()
            for source,target,maximum in (('name','name',160),('email','email',254),
                ('contract_reference','reference',240),('scope','scope',2000)):
                value=declaration[source]
                if (not isinstance(value,str) or not 1<=len(value)<=maximum
                    or any(ord(c)<32 and c not in '\n\t' or ord(c)==127 for c in value)):raise ValueError()
                result[target]=value
        except (ValueError,TypeError,UnicodeError):
            result.update(name='',email='',reference='',scope='',invalid=True)
        return result

    @classmethod
    def _withdrawals(cls,db,tables,*,page=1,per_page=25,order_id=None,mode=None,identifier=None):
        if 'shop_withdrawals_v2' not in tables:return [],False
        where,params=[],[]
        if order_id is not None:
            # Consumer stores canonical JSON. Exact post-parse comparison below
            # is authoritative; an unverified customer reference never grants
            # ownership or authorizes a refund/cancellation.
            where.append('instr(declaration_json,?)>0')
            params.append(json.dumps({'contract_reference':order_id},separators=(',',':'))[1:-1])
        if mode is not None:where.append('mode=?');params.append(mode)
        if identifier is not None:where.append('id=?');params.append(identifier)
        condition=' WHERE '+' AND '.join(where) if where else ''
        rows=list(db.execute('''SELECT id,mode,substr(declaration_json,1,16385) AS declaration_json,
            received_at,state,mail_state FROM shop_withdrawals_v2'''+condition+
            ' ORDER BY received_at DESC,id DESC LIMIT ? OFFSET ?',params+[per_page+1,(page-1)*per_page]))
        if any(not isinstance(row['id'],str) or not WITHDRAWAL.fullmatch(row['id']) for row in rows):abort(503)
        summaries=[cls._withdrawal_summary(row) for row in rows[:per_page]]
        for summary,row in zip(summaries,rows):
            summary['_mode']=row['mode']
        refs={item['reference'] for item in summaries if ORDER.fullmatch(item['reference'])}
        existing=set()
        if refs:
            existing={(row['id'],row['mode']) for row in db.execute('SELECT id,mode FROM shop_orders_v2 WHERE id IN ('+
                ','.join('?' for _ in refs)+')',sorted(refs))}
        for item in summaries:
            if (item['reference'],item.pop('_mode')) in existing:item['order_id']=item['reference']
        if order_id is not None:summaries=[item for item in summaries if item['reference']==order_id]
        return summaries,len(rows)>per_page

    def withdrawals(self,*,page=1,per_page=25,order_id=None):
        if (type(page) is not int or not 1<=page<=9999 or type(per_page) is not int or not 1<=per_page<=100
            or order_id is not None and (not isinstance(order_id,str) or not ORDER.fullmatch(order_id))):abort(400)
        with self.read() as (db,tables):
            return self._withdrawals(db,tables,page=page,per_page=per_page,order_id=order_id)

    def withdrawal(self,identifier):
        if not isinstance(identifier,str) or not WITHDRAWAL.fullmatch(identifier):abort(404)
        with self.read() as (db,tables):
            rows,_=self._withdrawals(db,tables,identifier=identifier,per_page=1)
            if not rows:abort(404)
            return rows[0]


def _private_root(value):
    if value is None:return None
    if not isinstance(value,(str,Path)):raise ValueError('explicit_shop_admin_root_required')
    root=Path(value)
    if not root.is_absolute():raise ValueError('explicit_shop_admin_root_required')
    return root


def _review_csrf_token():
    token=session.get('csrf_token')
    if not isinstance(token,str) or not CSRF.fullmatch(token):
        token=secrets.token_urlsafe(32)
        session['csrf_token']=token
    return token


def _review_projection(value,detail):
    """Whitelist status only. Approval/procurement authority stays in service."""
    if not isinstance(value,Mapping):raise ValueError('invalid_review')
    if value.get('required') is False and value.get('state')=='not_required':return None
    binding=detail['_review_binding']
    requirements=value.get('review_requirements')
    if (value.get('required') is not True or value.get('order_id')!=detail['id']
            or value.get('mode')!=binding['mode'] or binding['currency']!='eur' or value.get('currency')!='eur'
            or type(value.get('amount_gross_cent')) is not int
            or value['amount_gross_cent']!=binding['amount_gross_cent']
            or value.get('quote_hash')!=binding['quote_hash']
            or not isinstance(value.get('quote_hash'),str) or not HASH.fullmatch(value['quote_hash'])
            or value.get('state') not in REVIEW_STATES
            or not isinstance(requirements,list) or not 1<=len(requirements)<=100
            or any(not isinstance(item,Mapping) or item.get('code') not in REVIEW_REQUIREMENTS for item in requirements)
            or not isinstance(value.get('vehicle_ref',''),str) or len(value.get('vehicle_ref',''))>200):
        raise ValueError('invalid_review')
    technical=any(item['code'] in {'fitment_unconfirmed','assembly_fitment_unconfirmed'} for item in requirements)
    verified=value.get('authorization_verified') is True
    expires=_date(value.get('expires_at'))
    actionable=(value['state']=='pending' and verified and expires!='Nicht verfügbar'
                and binding['status']=='authorized')
    result={'state':value['state'],'label':REVIEW_STATES[value['state']],
        'quote_hash':value['quote_hash'],'vehicle_ref':_text(value.get('vehicle_ref'),200),
        'payment_state':binding['status'],
        'authorization_verified':verified,'expires':expires,'actionable':actionable,'technical':technical,
        'requirements':[{'label':REVIEW_REQUIREMENTS[item['code']],
            'product_id':_text(item.get('product_id'),64),'assembly_group':_text(item.get('assembly_group'),180)} for item in requirements]}
    evidence=value.get('review_evidence')
    if isinstance(evidence,Mapping):
        refs=evidence.get('certificate_references')
        result['evidence']={'vehicle_ref':_text(evidence.get('vehicle_ref'),200),
            'certificate_references':[_text(ref,200) for ref in refs[:20]] if isinstance(refs,list) else []}
    return result


def create_shop_admin_blueprint(*,private_data_dir=None):
    blueprint=Blueprint('shop_admin',__name__,template_folder='templates')
    blueprint.shop_admin_private_data_dir=_private_root(private_data_dir)

    @blueprint.before_request
    def administrator_only():
        if session.get('admin') is not True:abort(403)

    @blueprint.after_request
    def private_response(response):
        response.headers['Cache-Control']='private, no-store'
        response.headers['X-Robots-Tag']='noindex, nofollow, nosnippet'
        response.headers['X-Content-Type-Options']='nosniff'
        return response

    def reader():
        adapters=current_app.extensions.get('shop_production')
        storage=adapters.get('storage') if isinstance(adapters,Mapping) else None
        store=adapters.get('store') if isinstance(adapters,Mapping) else None
        if isinstance(storage,ShopStorage) and isinstance(store,OrderStore) and storage.ready is True:
            return ShopAdminReader(storage,store)
        root=blueprint.shop_admin_private_data_dir
        if root is None:abort(404)
        # An explicitly supplied persistent root may still have historical
        # orders when the selling release is disabled or its bootstrap failed.
        # Construction is inert: marker/schema checks happen in the RO reader.
        existing=ShopStorage(root)
        return ShopAdminReader(existing,existing.order_store(),existing_only=True)

    def review_dependencies():
        if current_app.config.get('SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED') is not True:abort(404)
        adapters=current_app.extensions.get('shop_production')
        service=adapters.get('order_review') if isinstance(adapters,Mapping) else None
        store=adapters.get('store') if isinstance(adapters,Mapping) else None
        if (service is None or not all(callable(getattr(service,name,None)) for name in ('readiness','status','approve','reject'))
                or not callable(getattr(store,'lookup_payment_owner',None))):abort(503)
        try:
            ready=service.readiness()
        except Exception:
            abort(503)
        mode=getattr(service,'mode',None)
        if (not isinstance(ready,Mapping) or ready.get('ready') is not True
                or mode not in {'test','live'} or ready.get('mode')!=mode):abort(503)
        return service,store,mode

    def review_status(service,store,mode,order_id,detail):
        owner=store.lookup_payment_owner(order_id,mode=mode)
        if (not isinstance(owner,Mapping) or owner.get('mode')!=mode
                or not isinstance(owner.get('customer_id'),str) or not owner['customer_id']):abort(503)
        value=service.status(order_id,customer_id=owner['customer_id'])
        return _review_projection(value,detail),owner['customer_id']

    def decision_form():
        origin=current_app.config.get('SHOP_REVIEW_ADMIN_ORIGIN',current_app.config.get('SHOP_PUBLIC_ORIGIN'))
        try:
            parsed=urlsplit(origin) if isinstance(origin,str) else None
        except ValueError:
            abort(503)
        if (not parsed or parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.path or parsed.query or parsed.fragment):abort(503)
        if (request.scheme!='https' or request.host_url.rstrip('/')!=origin
                or request.headers.get('Origin')!=origin
                or request.headers.get('Sec-Fetch-Site') not in (None,'same-origin')):abort(403)
        if request.args or request.mimetype!='application/x-www-form-urlencoded':abort(400)
        if request.content_length is None or not 0<request.content_length<=16384:abort(413)
        fields=request.form
        if any(len(fields.getlist(key))!=1 for key in fields):abort(400)
        supplied=fields.get('csrf_token')
        expected=session.get('csrf_token')
        if (not isinstance(expected,str) or not CSRF.fullmatch(expected)
                or not isinstance(supplied,str) or not CSRF.fullmatch(supplied)
                or not hmac.compare_digest(supplied,expected)):abort(403)
        return fields,'portal-admin-session:'+sha256(expected.encode('ascii')).hexdigest()[:32]

    @blueprint.app_context_processor
    def staff_queue_badge():
        result={'shop_admin_available':False,'shop_withdrawal_pending':0,'shop_withdrawal_unavailable':False}
        if (not has_request_context() or session.get('admin') is not True
            or request.path not in {'/admin','/admin/cockpit'} and not request.path.startswith('/admin/shop')):return result
        try:
            with reader().read() as (db,tables):
                result['shop_admin_available']=True
                if 'shop_withdrawals_v2' in tables:
                    count=db.execute("SELECT COUNT(*) FROM shop_withdrawals_v2 WHERE state='manual_review'").fetchone()[0]
                    result['shop_withdrawal_pending']=min(count,999)
        except HTTPException as error:
            if error.code!=404:result.update(shop_admin_available=True,shop_withdrawal_unavailable=True)
        return result

    @blueprint.get('/admin/shop/widerrufe')
    def withdrawals():
        if set(request.args)-{'page','per_page','order_id'} or any(len(request.args.getlist(key))!=1 for key in request.args):abort(400)
        page,per_page=request.args.get('page','1'),request.args.get('per_page','25')
        if not re.fullmatch(r'[1-9][0-9]{0,3}',page) or not re.fullmatch(r'[1-9][0-9]{0,2}',per_page):abort(400)
        page,per_page=int(page),int(per_page)
        if per_page>100:abort(400)
        order_id=request.args.get('order_id')
        rows,has_next=reader().withdrawals(page=page,per_page=per_page,order_id=order_id)
        return render_template('shop_v2/admin_orders.html',withdrawal_page=True,withdrawals=rows,
            withdrawal_order_filter=order_id,page=page,per_page=per_page,has_next=has_next)

    @blueprint.get('/admin/shop/widerrufe/<identifier>')
    def withdrawal_detail(identifier):
        if request.args:abort(400)
        return render_template('shop_v2/admin_orders.html',withdrawal_detail=reader().withdrawal(identifier))

    @blueprint.get('/admin/shop')
    def orders():
        if set(request.args)-{'page','per_page'} or any(len(request.args.getlist(key))!=1 for key in request.args):abort(400)
        page,per_page=request.args.get('page','1'),request.args.get('per_page','25')
        if not re.fullmatch(r'[1-9][0-9]{0,3}',page) or not re.fullmatch(r'[1-9][0-9]{0,2}',per_page):abort(400)
        page,per_page=int(page),int(per_page)
        if per_page>100:abort(400)
        rows,has_next=reader().orders(page=page,per_page=per_page)
        return render_template('shop_v2/admin_orders.html',orders=rows,detail=None,page=page,per_page=per_page,has_next=has_next)

    @blueprint.get('/admin/shop/<order_id>')
    def order_detail(order_id):
        if not ORDER.fullmatch(order_id):abort(404)
        if request.args:abort(400)
        detail=reader().detail(order_id)
        review=None
        if current_app.config.get('SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED') is True:
            try:
                service,store,mode=review_dependencies()
                review,_=review_status(service,store,mode,order_id,detail)
            except Exception:
                review={'unavailable':True}
        token=_review_csrf_token() if review and review.get('actionable') else None
        return render_template('shop_v2/admin_orders.html',orders=None,detail=detail,order_review=review,review_csrf=token)

    @blueprint.post('/admin/shop/<order_id>/pruefung/<decision>')
    def decide_review(order_id,decision):
        if not ORDER.fullmatch(order_id) or decision not in {'freigeben','ablehnen'}:abort(404)
        fields,actor=decision_form()
        service,store,mode=review_dependencies()
        detail=reader().detail(order_id)
        try:
            review,customer_id=review_status(service,store,mode,order_id,detail)
            if not review or not review['actionable']:abort(409)
            expected={'csrf_token','quote_hash'}
            if decision=='freigeben':
                expected.add('full_order_confirmed')
                if review['technical']:expected.update({'vehicle_ref','certificate_references','conditions_confirmed'})
            if set(fields)!=expected:abort(400)
            if (not isinstance(fields.get('quote_hash'),str) or not HASH.fullmatch(fields['quote_hash'])
                    or not hmac.compare_digest(fields['quote_hash'],review['quote_hash'])):abort(409)
            evidence=None
            if decision=='freigeben':
                if fields['full_order_confirmed']!='yes':abort(400)
                if review['technical']:
                    vehicle=fields['vehicle_ref'].strip()
                    refs=[line.strip() for line in fields['certificate_references'].splitlines() if line.strip()]
                    if (fields['conditions_confirmed']!='yes' or not 1<=len(vehicle)<=200
                            or review['vehicle_ref'] and vehicle!=review['vehicle_ref']
                            or not 1<=len(refs)<=20 or len(set(refs))!=len(refs) or any(len(ref)>200 for ref in refs)
                            or any(ord(c)<32 or ord(c)==127 or c in '<>' for text in [vehicle,*refs] for c in text)):
                        abort(400)
                    evidence={'vehicle_ref':vehicle,'certificate_references':refs,
                        'conditions_confirmed':True,'full_order_confirmed':True}
                updated=service.approve(order_id,customer_id=customer_id,expected_quote_hash=review['quote_hash'],actor_id=actor,evidence=evidence)
            else:
                updated=service.reject(order_id,customer_id=customer_id,expected_quote_hash=review['quote_hash'],actor_id=actor)
            result=_review_projection(updated,detail)
            if not result or result['state']!=('approved' if decision=='freigeben' else 'rejected'):abort(503)
        except HTTPException:
            raise
        except Exception as error:
            if getattr(error,'code',None) in {'review_expired','review_not_pending','review_binding_mismatch',
                    'review_already_decided','review_not_required','review_authorization_unconfirmed',
                    'authorization_unconfirmed','review_approval_required'}:abort(409)
            if getattr(error,'code',None) in {'review_evidence_required','invalid_review_decision'}:abort(400)
            abort(503)
        return redirect(url_for('shop_admin.order_detail',order_id=order_id),code=303)

    return blueprint


def register_shop_admin(app_or_portal,*,private_data_dir=None):
    app=getattr(app_or_portal,'app',app_or_portal)
    root=_private_root(private_data_dir)
    if 'shop_admin' in app.blueprints:
        blueprint=app.blueprints['shop_admin']
        if root is not None:blueprint.shop_admin_private_data_dir=root
        return blueprint
    blueprint=create_shop_admin_blueprint(private_data_dir=root)
    app.register_blueprint(blueprint)
    return blueprint
