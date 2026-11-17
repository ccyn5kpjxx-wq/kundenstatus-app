"""Opt-in Stripe TEST Checkout acceptance; suppliers and SMTP are always fake.

Default invocation is a no-IO plan. Never imports the production app or reads
legacy credentials. A human completes the hosted test Checkout using its URL
from the explicitly requested private JSON, then runs --run-sandbox --verify.
The signed event is local synthetic evidence, not Stripe webhook delivery.
"""
import argparse
from collections.abc import Mapping
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import sys
import tempfile
from uuid import uuid4

SCHEMA='shop-stripe-sandbox-acceptance-v1'
MARKER=b'shop-sandbox-acceptance-isolated-test-database-v1\n'
CUSTOMER='sandbox-customer'
TYRE='tyre:'+'a'*24
RIM='alloy_wheel:'+'b'*24
OIL='parts:'+'c'*24
ARTICLES={TYRE:'101',RIM:'102',OIL:'103'}
POLICY={'account_fingerprint':'e'*64,'mode_evidence_id':'SYNTHETIC-FAKE-SUPPLIER-MODE',
        'acceptance_evidence_id':'SYNTHETIC-FAKE-SUPPLIER-ACCEPTANCE'}


class SandboxAcceptanceError(ValueError):
    pass


def _json(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _now(clock=None):
    value=clock() if clock else datetime.now(timezone.utc)
    if not isinstance(value,datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise SandboxAcceptanceError('invalid_sandbox_clock')
    return value.astimezone(timezone.utc)


def _key(value):
    if not isinstance(value,str) or not re.fullmatch(r'(?:sk|rk)_test_[A-Za-z0-9_]{8,256}',value):
        raise SandboxAcceptanceError('explicit_test_key_required')
    return value


def _path(value):
    path=Path(value)
    if (not path.is_absolute() or path.suffix!='.json' or path.is_symlink()
            or path.resolve()!=path
            or any(part.casefold() in {'public','static','templates','wwwroot','outputs'} for part in path.parts)):
        raise SandboxAcceptanceError('explicit_private_state_path_required')
    return path


def _write(path,value,*,first=False):
    encoded=_json(value)
    if len(encoded.encode())>262144:raise SandboxAcceptanceError('sandbox_state_limit')
    if first:
        with path.open('x',encoding='utf-8') as stream:stream.write(encoded)
    else:
        temporary=path.with_name(path.name+'.'+uuid4().hex+'.tmp')
        with temporary.open('x',encoding='utf-8') as stream:stream.write(encoded)
        try:os.chmod(temporary,0o600)
        except OSError:pass
        os.replace(temporary,path)
    try:os.chmod(path,0o600)
    except OSError:pass


def _read(path):
    if not path.is_file() or path.stat().st_size>262144:
        raise SandboxAcceptanceError('invalid_sandbox_state')
    try:
        state=json.loads(path.read_text(encoding='utf-8'))
        directory=Path(state['run_directory'])
        database=Path(state['database'])
        if (state.get('schema')!=SCHEMA or state.get('mode')!='test' or state.get('customer_id')!=CUSTOMER
                or directory.parent!=path.parent or not directory.name.startswith('shop-sandbox-')
                or directory.is_symlink() or directory.resolve()!=directory
                or database!=directory/'acceptance.sqlite' or database.is_symlink()
                or not re.fullmatch(r'sho_[a-f0-9]{32}',state.get('order_id',''))
                or state.get('supplier_outcome') not in {'accepted','rejected','timeout'}
                or not re.fullmatch(r'whsec_[A-Za-z0-9_]{8,256}',state.get('local_webhook_secret',''))
                or (directory/'sandbox.marker').is_symlink()
                or (directory/'sandbox.marker').read_bytes()!=MARKER):
            raise ValueError()
        return state
    except Exception:raise SandboxAcceptanceError('invalid_sandbox_state') from None


def _modules():
    root=str(Path(__file__).resolve().parents[1])
    if root not in sys.path:sys.path.insert(0,root)
    from shop_cart_quotes import CartQuoteService, retail_unit_gross_cent
    from shop_checkout import ShopCheckout
    from shop_fulfillment import ShopFulfillment, FulfillmentJournal, init_fulfillment_schema
    from shop_order_store import OrderStore, init_schema
    from shop_payment_actions import PaymentActions, PaymentActionJournal, init_payment_action_schema
    from shop_order_receipts import ShopOrderReceipts, init_order_receipt_schema
    from shop_rsu_orders import request_sha256
    from shop_stripe_gateway import StripeShopGateway
    return locals()


def _ean(number):
    digits=str(number).zfill(12)
    total=sum(int(c)*(3 if i%2==0 else 1) for i,c in enumerate(reversed(digits)))
    return digits+str((10-total%10)%10)


class SyntheticCatalogue:
    """No external source: typed synthetic identities, prices and fitment only."""
    def __init__(self,modules,clock):
        self.m,self.clock=modules,clock
        self.products={}
        for pid,category in ((TYRE,'tyre'),(RIM,'alloy_wheel'),(OIL,'oil')):
            identity=sha256(_json([pid,'sandbox-variant']).encode()).hexdigest()
            self.products[pid]={'id':pid,'category':category,'name':'SANDBOX synthetic '+category,
                'identity_hash':identity,'fitment_required':category!='oil',
                'fitment':{'confirmed':True,'identity_hash':identity,'vehicle_ref':'SANDBOX-CAR','evidence_id':'SYNTHETIC-fitment'},
                'required_fees_confirmed':True,'required_fees':[],'new_part':True,
                'painting_eligible':False,'diameter':18,'tyre_size':'225/40 R18 92Y XL'}

    def current(self):
        now=_now(self.clock)
        return {'confirmed':True,'mode':'test','currency':'eur','vat_percent':'19.00',
                'observed_at':now.isoformat(),'expires_at':(now+timedelta(minutes=15)).isoformat()}

    def lookup(self,pid):return deepcopy(self.products.get(pid))

    def retail(self,product,quantity):
        return {**self.current(),'product_id':product['id'],'identity_hash':product['identity_hash'],
            'quantity':quantity,'unit_gross_cent':self.m['retail_unit_gross_cent']('1.00',product['category']),
            'stock':100,'procurement_confirmed':True}

    def mapping(self,product):
        return {'confirmed':True,'supplier':'RSU','product_id':product['id'],'identity_hash':product['identity_hash'],
                'supplier_article_id':ARTICLES[product['id']],'evidence_id':'SYNTHETIC-mapping'}

    def service(self,code,product,quantity,options):
        if code!='mounting':return None
        return {**self.current(),'product_id':product['id'],'identity_hash':product['identity_hash'],'quantity':quantity,
            'options_hash':sha256(_json(options).encode()).hexdigest(),'gross_cent':100*quantity}

    def freight(self,shipment,context):
        return {**self.current(),'shipment_hash':shipment['shipment_hash'],'context_hash':shipment['context_hash'],'gross_cent':100}

    def assembly(self,rim,tyre,vehicle):
        return {'confirmed':True,'rim_identity_hash':rim['identity_hash'],'tyre_identity_hash':tyre['identity_hash'],
                'vehicle_ref':vehicle,'tyre_size':tyre['tyre_size'],'rim_diameter':rim['diameter'],'evidence_id':'SYNTHETIC-assembly'}

    def quote(self,lines,context):
        service=self.m['CartQuoteService'](catalogue_lookup=self.lookup,retail_quote=self.retail,rsu_mapping=self.mapping,
            service_quote=self.service,freight_quote=self.freight,assembly_fitment=self.assembly,mode='test',clock=self.clock)
        return service.quote({'lines':lines},customer_id=CUSTOMER,vehicle_ref='SANDBOX-CAR',delivery_context=context)

    def inquiry(self,item,quantity):
        now=_now(self.clock)
        return {'confirmed':True,'product_id':item['id'],'identity_hash':item['identity_hash'],
            'supplier_article_id':item['mapping']['supplier_article_id'],'mapping_evidence_id':item['mapping']['evidence_id'],
            'expected_ean':_ean(ARTICLES[item['id']]),'ean':_ean(ARTICLES[item['id']]),'quantity':quantity,
            'currency':'EUR','mode':'test','stock':100,'unit_net':'1.0000',
            'observed_at':now.isoformat(),'expires_at':(now+timedelta(minutes=15)).isoformat()}

    def revalidate(self,snapshot,context,*,destinations):
        requests=[request for request,line in zip(snapshot['request'],snapshot['lines']) if line['destination'] in destinations]
        return self.quote(requests,context).snapshot()


class FakeSupplier:
    """There is intentionally no network/HTTP implementation in this class."""
    def __init__(self,modules,outcome):self.m,self.outcome,self.saga,self.calls=modules,outcome,None,[]

    def submit_order(self,payload,*,idempotency_key):
        digest=self.m['request_sha256'](payload)
        envelope={**POLICY,'configured_mode':'test','reference':payload['reference'],
                  'request_hash':digest,'idempotency_key':idempotency_key}
        if self.saga.authorize_submission(envelope) is not True:
            raise SandboxAcceptanceError('fake_supplier_grant_missing')
        self.calls.append(payload['reference'])
        if self.outcome=='timeout':raise TimeoutError('synthetic supplier timeout')
        positions=[{**row,'outcome':self.outcome,'order_number':'Test_1234' if self.outcome=='accepted' else None,
            'request_bound':True,'echo_confirmed':False,'facts_source':'submitted_request_under_external_policy'} for row in payload['positions']]
        return {'outcome':self.outcome,'reference':payload['reference'],'request_hash':digest,'positions':positions,
                'echo_confirmed':False,'fulfillment_confirmed':False,'capture_authorized':False,'retry_authorized':False}


class FakeOutbox:
    """Receipt MIME/claim path is exercised, but no SMTP connection exists."""
    def __init__(self):self.results={}
    def send(self,token,message,config,*,retry_not_sent):
        if retry_not_sent is not False:raise SandboxAcceptanceError('fake_smtp_retry_forbidden')
        self.results.setdefault(token,{'token':token,'state':'sent','accepted':[message['To']]})
        return self.results[token]
    def status(self,token):return self.results.get(token,{'token':token,'state':'not_sent'})


def _services(state,key,*,sdk=None,client=None,clock=None):
    m=_modules()
    clock=clock or (lambda:datetime.now(timezone.utc))
    factory=lambda:sqlite3.connect(state['database'],timeout=5)
    store=m['OrderStore'](factory,clock=clock)
    gateway=m['StripeShopGateway']({'SHOP_STRIPE_MODE':'test','SHOP_STRIPE_SECRET_KEY':_key(key),
        'SHOP_STRIPE_WEBHOOK_SECRET':state['local_webhook_secret'],'SHOP_STRIPE_PUBLIC_ORIGIN':state['public_origin']},
        sdk=sdk,client=client,clock=clock)
    checkout=m['ShopCheckout'](store,gateway,mode='test',public_origin=state['public_origin'],enabled=True,
        order_lookup=lambda oid:store.lookup_payment_owner(oid,mode='test'),clock=clock)
    return m,factory,store,gateway,checkout,clock


def start_sandbox(state_path,*,key,public_origin,supplier_outcome='accepted',sdk=None,client=None,clock=None):
    _key(key)
    if supplier_outcome not in {'accepted','rejected','timeout'}:raise SandboxAcceptanceError('invalid_fake_supplier_outcome')
    path=_path(state_path)
    if path.exists():raise SandboxAcceptanceError('sandbox_state_already_exists_use_verify')
    # Validate the origin/key before creating even the isolated local database.
    m=_modules()
    from shop_stripe_gateway import _origin
    public_origin=_origin(public_origin)
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    directory=Path(tempfile.mkdtemp(prefix='shop-sandbox-',dir=path.parent))
    (directory/'sandbox.marker').write_bytes(MARKER)
    state={'schema':SCHEMA,'mode':'test','customer_id':CUSTOMER,'run_directory':str(directory),
        'database':str(directory/'acceptance.sqlite'),'local_webhook_secret':'whsec_'+secrets.token_hex(24),
        'public_origin':public_origin,'supplier_outcome':supplier_outcome,'created_at':_now(clock).isoformat(),
        'status':'preparing','checkout':None}
    _write(path,state,first=True)
    m,factory,store,gateway,checkout,clock=_services(state,key,sdk=sdk,client=client,clock=clock)
    with closing(factory()) as db:
        for name in ('init_schema','init_fulfillment_schema','init_payment_action_schema','init_order_receipt_schema'):m[name](db)
    try:os.chmod(state['database'],0o600)
    except OSError:pass
    catalogue=SyntheticCatalogue(m,clock)
    address={'recipient':'SANDBOX recipient','street':'Teststrasse 1','postcode':'74821','city':'Mosbach','country':'DE'}
    context={'postcode':'74821','contact':{'name':'SANDBOX recipient','email':'sandbox@example.invalid'},
             'billing':address,'delivery':deepcopy(address)}
    lines=[{'id':pid,'quantity':4 if pid!=OIL else 1,'services':{'mounting':True} if pid!=OIL else {},
            'assembly_group':'sandbox-set' if pid!=OIL else ''} for pid in (TYRE,RIM,OIL)]
    quote=catalogue.quote(lines,context)
    if not quote.public()['ready']:raise SandboxAcceptanceError('synthetic_quote_not_ready')
    store.save_quote(quote.snapshot(),expected_quote_hash=quote.quote_hash,customer_id=CUSTOMER,mode='test',delivery_context=context)
    order=store.create_order(quote.quote_hash,customer_id=CUSTOMER,mode='test',idempotency_key='sandbox-'+uuid4().hex)
    state.update(order_id=order['id'],status='creating_checkout')
    _write(path,state)
    try:
        result=checkout.start(order['id'],customer_id=CUSTOMER)
        state.update(status='awaiting_hosted_test_card',checkout=result)
        _write(path,state)
    except Exception:
        state['status']='checkout_creation_unknown_do_not_repeat'
        _write(path,state)
        raise SandboxAcceptanceError('sandbox_checkout_reconciliation_required') from None
    return {'mode':'test','status':state['status'],'hosted_url_saved_privately':bool(result.get('checkout_url')),
            'manual_capture':True,'real_supplier_calls':0,'real_emails':0,'live_payments':0}


def _plain(value):
    if callable(getattr(value,'to_dict_recursive',None)):value=value.to_dict_recursive()
    if isinstance(value,Mapping):return {str(k):_plain(v) for k,v in value.items()}
    if isinstance(value,list):return [_plain(v) for v in value]
    if value is None or type(value) in (str,bool,int,float):return value
    raise SandboxAcceptanceError('invalid_test_provider_object')


def verify_sandbox(state_path,*,key,sdk=None,client=None,clock=None):
    _key(key)
    path=_path(state_path)
    state=_read(path)
    m,factory,store,gateway,checkout,clock=_services(state,key,sdk=sdk,client=client,clock=clock)
    oid=state['order_id']
    result=checkout.reconcile(oid,customer_id=CUSTOMER)
    if result['payment_state'] not in {'authorized','paid','cancelled'}:
        state['last_verification']={'mode':'test','status':'awaiting_hosted_test_card','real_supplier_calls':0,'real_emails':0,'live_payments':0}
        _write(path,state)
        return state['last_verification']
    attempt=store.claim_payment(oid,customer_id=CUSTOMER,mode='test')
    if not attempt.get('intent_id'):raise SandboxAcceptanceError('test_authorization_unconfirmed')
    intent=_plain(gateway.retrieve_intent(attempt['intent_id']))
    if intent.get('livemode') is not False:raise SandboxAcceptanceError('test_provider_mode_mismatch')
    event_type={'requires_capture':'payment_intent.amount_capturable_updated','succeeded':'payment_intent.succeeded',
                'canceled':'payment_intent.canceled'}.get(intent.get('status'))
    if event_type is None:raise SandboxAcceptanceError('test_authorization_unconfirmed')
    timestamp=int(_now(clock).timestamp())
    event={'object':'event','id':'evt_SANDBOX'+uuid4().hex,'livemode':False,'created':timestamp,
           'type':event_type,'data':{'object':intent}}
    raw=_json(event).encode()
    signature=hmac.new(state['local_webhook_secret'].encode(),str(timestamp).encode()+b'.'+raw,sha256).hexdigest()
    received=checkout.handle_webhook(raw,f't={timestamp},v1={signature}')
    replay=checkout.handle_webhook(raw,f't={timestamp},v1={signature}')
    if received.get('received') is not True or replay.get('received') is not True:
        raise SandboxAcceptanceError('signed_local_event_not_accepted')
    catalogue=SyntheticCatalogue(m,clock)
    journal=m['FulfillmentJournal'](factory,clock=clock)
    payment_journal=m['PaymentActionJournal'](factory,clock=clock)
    supplier=FakeSupplier(m,state['supplier_outcome'])
    saga=None
    payment=m['PaymentActions'](store,gateway,payment_journal,
        lambda order_id,operation:saga.proof_resolver(order_id,operation=operation),mode='test',enabled=True,clock=clock)
    saga=m['ShopFulfillment'](store,journal,payment,supplier,mode='test',supplier_policy=POLICY,
        fresh_inquiry=catalogue.inquiry,revalidate=catalogue.revalidate,enabled=True,clock=clock,
        delivery_address=lambda destination,context:{'neutraldelivery':1,'salutation':'Firma','name':'SANDBOX '+destination,
            'street':'Teststrasse 1','zipcode':'74821','city':'Mosbach','countrycode':'DE'})
    supplier.saga=saga
    outbox=FakeOutbox()
    receipts=m['ShopOrderReceipts'](store,factory,mode='test',sender_address='sandbox@example.invalid',enabled=True,
        delivery_enabled=True,outbox=outbox,smtp_config=lambda:{'from_address':'sandbox@example.invalid'},
        legal_snapshot=lambda *args,**kwargs:{'version':'SYNTHETIC-sandbox-only','texts':{key:'SANDBOX synthetic legal text'
            for key in ('agb','widerruf','lieferung','datenschutz')},'attachments':[]},clock=clock)
    before=store.get_order(oid,customer_id=CUSTOMER,mode='test')['status']
    if before=='authorized':
        receipts.enqueue(oid,customer_id=CUSTOMER,kind='order_received')
        receipts.deliver_once()
    fulfillment=saga.run(oid,customer_id=CUSTOMER)
    first_count=len(supplier.calls)
    repeated=saga.run(oid,customer_id=CUSTOMER)
    if len(supplier.calls)!=first_count:raise SandboxAcceptanceError('fake_supplier_replayed')
    final=store.get_order(oid,customer_id=CUSTOMER,mode='test')['status']
    if final in {'paid','cancelled'}:
        receipts.enqueue(oid,customer_id=CUSTOMER,kind='order_confirmed' if final=='paid' else 'order_cancelled')
        receipts.deliver_once()
    report={'mode':'test','status':final,'fulfillment_state':fulfillment.get('state'),
        'synthetic_signed_event_verified':True,'event_replay_accepted':True,'supplier_replay_blocked':True,
        'fake_supplier_calls_this_verify':first_count,'fake_smtp_submissions_this_verify':len(outbox.results),
        'test_payment_captured':final=='paid','test_authorization_released':final=='cancelled',
        'actual_stripe_webhook_delivery_exercised':False,'real_supplier_calls':0,'real_emails':0,'live_payments':0,
        'production_legal_pack_exercised':False}
    state.update(status='verified_'+final,last_verification=report)
    _write(path,state)
    return report


def main(argv=None,*,environ=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-sandbox',action='store_true')
    parser.add_argument('--verify',action='store_true')
    parser.add_argument('--private-state')
    parser.add_argument('--public-origin')
    parser.add_argument('--supplier-outcome',choices=['accepted','rejected','timeout'],default='accepted')
    args=parser.parse_args(argv)
    if not args.run_sandbox:
        print(_json({'mode':'offline_plan','network':False,'filesystem_changes':False,
            'required_opt_in':'--run-sandbox','required_explicit_key_env':'SHOP_SANDBOX_STRIPE_KEY',
            'supplier':'fake_only','smtp':'fake_only','live_payments':0}))
        return 0
    try:
        if not args.private_state:raise SandboxAcceptanceError('explicit_private_state_path_required')
        environment=os.environ if environ is None else environ
        key=_key(environment.get('SHOP_SANDBOX_STRIPE_KEY'))
        if args.verify:result=verify_sandbox(args.private_state,key=key)
        else:
            if not args.public_origin:raise SandboxAcceptanceError('explicit_public_origin_required')
            result=start_sandbox(args.private_state,key=key,public_origin=args.public_origin,supplier_outcome=args.supplier_outcome)
        print(_json(result))
        return 0
    except SandboxAcceptanceError as error:
        print(_json({'mode':'test','status':'blocked','code':str(error),'live_payments':0}))
        return 2
    except Exception:
        # SDK/API errors can include URLs, identifiers or secrets. Never print.
        print(_json({'mode':'test','status':'blocked','code':'sandbox_verification_unavailable','live_payments':0}))
        return 3


if __name__=='__main__':raise SystemExit(main())
