"""Authenticated read-only admin against isolated synthetic shop SQLite only."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
import json
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
from uuid import uuid4
from werkzeug.datastructures import MultiDict

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'scripts'))
from flask import Flask,render_template_string
from shop_admin import ShopAdminReader, register_shop_admin
from shop_fulfillment import init_fulfillment_schema
from shop_order_receipts import init_order_receipt_schema
from shop_order_review import init_order_review_schema
from shop_order_workflow import init_order_workflow_schema
from shop_storage import ShopStorage
from shop_consumer import init_consumer_schema
from test_shop_cart_quotes import Fixture,NOW,OIL,line


class ReviewStub:
    """No provider, payment or supplier method is available to these routes."""
    mode='test'

    def __init__(self,value):
        self.value=value
        self.calls=[]
        self.ready=True

    def readiness(self):return {'ready':self.ready,'mode':self.mode}

    def status(self,order_id,*,customer_id):
        self.calls.append(('status',order_id,customer_id))
        return deepcopy(self.value)

    def approve(self,order_id,**kwargs):
        self.calls.append(('approve',order_id,kwargs))
        self.value['state']='approved'
        return deepcopy(self.value)

    def reject(self,order_id,**kwargs):
        self.calls.append(('reject',order_id,kwargs))
        self.value['state']='rejected'
        return deepcopy(self.value)

    def ensure(self,*args,**kwargs):raise AssertionError('Admin must not inspect the payment provider')


class InvoiceFixture:
    def __init__(self, identifier):
        self.identifier = identifier
        self.state = 'available'
        self.writes = []
        self.edit_url = 'https://app.lexware.de/permalink/invoices/edit/00000000-0000-4000-8000-000000000001'

    def statuses(self, ids):
        return {key: {'state': self.state, 'edit_url': self.edit_url,
                      'voucher_number': 'TEST-101' if self.state == 'ready' else ''}
                for key in ids if key == self.identifier}

    def prepare(self, order_id):
        self.writes.append(('prepare', order_id))
        self.state = 'draft'

    def refresh(self, order_id):
        self.writes.append(('refresh', order_id))
        self.state = 'ready'

    def pdf(self, order_id):
        assert order_id == self.identifier
        return b'%PDF-1.4\nSynthetic', 'Rechnung-TEST-101.pdf'


class ShopAdminTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.storage=ShopStorage(self.root,clock=lambda:NOW).provision(create=True)
        self.store=self.storage.order_store()
        self.app=Flask(__name__,template_folder=str(ROOT/'templates'))
        self.app.config.update(TESTING=True,SECRET_KEY='synthetic-admin-session')
        self.app.extensions['shop_production']={'storage':self.storage,'store':self.store}
        register_shop_admin(self.app)
        self.client=self.app.test_client()
        self.network=patch.object(socket.socket,'connect',side_effect=AssertionError('network forbidden'))
        self.network.start()
        self.counter=0

    def tearDown(self):
        self.network.stop()
        self.temp.cleanup()

    def login(self,value=True,**other):
        with self.client.session_transaction() as session:
            session.clear()
            session.update(other)
            if value is not None:session['admin']=value

    def order(self,name='Synthetic oil'):
        self.counter+=1
        fixture=Fixture()
        fixture.products[OIL]['name']=name
        context={'postcode':'74821','address_reference':'PRIVATE_CONTEXT_CANARY'}
        customer='synthetic-'+str(self.counter)
        quote=fixture.service_instance().quote({'lines':[line(OIL)]},customer_id=customer,vehicle_ref='CAR-1',delivery_context=context)
        self.store.save_quote(quote.snapshot(),expected_quote_hash=quote.quote_hash,customer_id=customer,mode='test',delivery_context=context)
        return self.store.create_order(quote.quote_hash,customer_id=customer,mode='test',idempotency_key='synthetic-order-'+str(self.counter))

    def declaration(self,reference='SYNTHETIC external contract',mode='test',**changes):
        identifier='swd_'+uuid4().hex
        declaration={'name':'<script>SYNTHETIC Buyer</script>','email':'synthetic@example.invalid',
            'contract_reference':reference,'scope':'One article\nSYNTHETIC <b>scope</b>'}
        declaration.update(changes)
        with closing(self.storage.connect()) as db:
            init_consumer_schema(db)
            db.execute('''INSERT INTO shop_withdrawals_v2
                (id,mode,declaration_json,received_at,mail_token,mail_state) VALUES (?,?,?,?,?,?)''',
                (identifier,mode,json.dumps(declaration,sort_keys=True,ensure_ascii=False,separators=(',',':')),
                 NOW.isoformat(),str(uuid4()),'unknown'))
            db.commit()
        return identifier

    def review_order(self,*,technical=True):
        self.origin='https://shop.example'
        self.app.config.update(SERVER_NAME='shop.example',SHOP_PUBLIC_ORIGIN=self.origin,
            SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED=True)
        order=self.order()
        # A synthetic stored authorization only; no Stripe operation or real
        # review service is constructed in route-boundary tests.
        with closing(self.storage.connect()) as db:
            db.execute("UPDATE shop_orders_v2 SET status='authorized' WHERE id=?",(order['id'],))
            db.commit()
        self.review=ReviewStub({'required':True,'order_id':order['id'],'mode':'test','state':'pending',
            'quote_hash':order['quote_hash'],'amount_gross_cent':order['amount_gross_cent'],'currency':'eur',
            'expires_at':(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat(),
            'authorization_verified':True,'vehicle_ref':'SYNTHETIC verified vehicle',
            'review_requirements':[{'code':'assembly_fitment_unconfirmed','assembly_group':'synthetic-set'}]
                if technical else [{'code':'mixed_cart_manual_review_required'}]})
        self.app.extensions['shop_production']['order_review']=self.review
        self.login()
        self.client.get('/admin/shop/'+order['id'],base_url=self.origin)
        with self.client.session_transaction() as value:token=value['csrf_token']
        return order,token

    def decision(self,order,token,*,decision='freigeben',data=None,headers=None,base_url=None):
        if data is None:
            data={'csrf_token':token,'quote_hash':order['quote_hash']}
            if decision=='freigeben':
                data.update(full_order_confirmed='yes',vehicle_ref='SYNTHETIC verified vehicle',
                    certificate_references='SYNTHETIC report page 4\nSYNTHETIC tyre approval',conditions_confirmed='yes')
        return self.client.post('/admin/shop/'+order['id']+'/pruefung/'+decision,data=data,
            base_url=base_url or self.origin,headers={'Origin':self.origin,'Sec-Fetch-Site':'same-origin',**(headers or {})})

    def test_review_view_shows_exact_selection_and_never_prechecks_or_provides_false_capture(self):
        order,token=self.review_order()
        self.review.value['vehicle_ref']='SYNTHETIC <script>vehicle</script>'
        text=self.client.get('/admin/shop/'+order['id'],base_url=self.origin).get_data(as_text=True)
        self.assertIn('noch nicht eingezogen',text)
        self.assertIn('synthetic-set',text)
        self.assertIn(OIL,text)
        self.assertIn('&lt;script&gt;vehicle&lt;/script&gt;',text)
        self.assertNotIn('<script>vehicle',text)
        self.assertNotIn(' checked',text)
        self.assertIn('Danach darf der Hintergrundprozess den Einkauf auslösen',text)
        self.assertIn('name="csrf_token" value="'+token+'"',text)
        self.assertTrue(all(call[0]=='status' for call in self.review.calls))
        with closing(self.storage.connect()) as db:
            db.execute("UPDATE shop_orders_v2 SET status='paid' WHERE id=?",(order['id'],))
            db.commit()
        self.review.value['state']='approved'
        text=self.client.get('/admin/shop/'+order['id'],base_url=self.origin).get_data(as_text=True)
        self.assertIn('Der Zahlungseinzug ist in der Bestellung bestätigt.',text)
        self.assertNotIn('noch nicht eingezogen',text)
        self.assertNotIn('Prüfung verbindlich freigeben</button>',text)

    def test_review_approval_uses_server_owner_hash_and_session_actor_only(self):
        order,token=self.review_order()
        result=self.decision(order,token)
        self.assertEqual(result.status_code,303)
        self.assertEqual(result.headers['Location'],'/admin/shop/'+order['id'])
        actions=[call for call in self.review.calls if call[0]=='approve']
        self.assertEqual(len(actions),1)
        _,identifier,args=actions[0]
        self.assertEqual(identifier,order['id'])
        self.assertEqual(args['customer_id'],order['customer_id'])
        self.assertEqual(args['expected_quote_hash'],order['quote_hash'])
        self.assertRegex(args['actor_id'],r'^portal-admin-session:[a-f0-9]{32}$')
        self.assertNotIn(token,args['actor_id'])
        self.assertEqual(args['evidence'],{'vehicle_ref':'SYNTHETIC verified vehicle',
            'certificate_references':['SYNTHETIC report page 4','SYNTHETIC tyre approval'],
            'conditions_confirmed':True,'full_order_confirmed':True})
        self.assertEqual(self.decision(order,token).status_code,409)
        self.assertEqual(self.decision(order,token,decision='ablehnen').status_code,409)
        self.assertEqual(len([c for c in self.review.calls if c[0] in {'approve','reject'}]),1)

    def test_review_rejection_and_mixed_only_confirmation_use_narrow_contract(self):
        order,token=self.review_order()
        self.assertEqual(self.decision(order,token,decision='ablehnen').status_code,303)
        self.assertEqual([c[0] for c in self.review.calls if c[0]!='status'],['reject'])
        self.review.value['state']='pending'
        self.review.value['review_requirements']=[{'code':'mixed_cart_manual_review_required'}]
        data={'csrf_token':token,'quote_hash':order['quote_hash'],'full_order_confirmed':'yes'}
        self.assertEqual(self.decision(order,token,data=data).status_code,303)
        self.assertIsNone([c for c in self.review.calls if c[0]=='approve'][-1][2]['evidence'])

    def test_review_mutations_require_real_admin_exact_origin_csrf_and_post(self):
        order,token=self.review_order()
        url='/admin/shop/'+order['id']+'/pruefung/freigeben'
        self.assertEqual(self.client.get(url,base_url=self.origin).status_code,405)
        for value in (None,False,1,'yes'):
            self.login(value)
            self.assertEqual(self.decision(order,token).status_code,403)
        self.login()
        with self.client.session_transaction() as value:value['csrf_token']=token
        for headers in ({'Origin':'https://elsewhere.example'},{'Origin':''},{'Sec-Fetch-Site':'cross-site'}):
            self.assertEqual(self.decision(order,token,headers=headers).status_code,403)
        self.assertEqual(self.decision(order,'x'*43).status_code,403)
        self.assertEqual(self.decision(order,token,base_url='http://shop.example',headers={'Origin':'http://shop.example'}).status_code,403)
        self.assertFalse(any(c[0] in {'approve','reject'} for c in self.review.calls))

    def test_review_form_rejects_extra_duplicate_incomplete_or_tampered_fields(self):
        order,token=self.review_order()
        valid={'csrf_token':token,'quote_hash':order['quote_hash'],'full_order_confirmed':'yes',
            'vehicle_ref':'SYNTHETIC verified vehicle','certificate_references':'SYNTHETIC report','conditions_confirmed':'yes'}
        for changed in ({**valid,'amount_gross_cent':'1'},{**valid,'customer_id':'another'},
                {**valid,'conditions_confirmed':'false'},{**valid,'certificate_references':''},
                {**valid,'vehicle_ref':''},{k:v for k,v in valid.items() if k!='full_order_confirmed'},
                MultiDict([*valid.items(),('quote_hash',order['quote_hash'])])):
            self.assertEqual(self.decision(order,token,data=changed).status_code,400)
        self.assertEqual(self.decision(order,token,data={**valid,'quote_hash':'f'*64}).status_code,409)
        result=self.client.post('/admin/shop/'+order['id']+'/pruefung/freigeben',json=valid,
            base_url=self.origin,headers={'Origin':self.origin})
        self.assertEqual(result.status_code,400)
        self.assertFalse(any(c[0] in {'approve','reject'} for c in self.review.calls))

    def test_review_disabled_missing_unready_expired_unverified_or_misbound_fail_closed(self):
        order,token=self.review_order()
        for flag in (None,False,'true',1):
            self.app.config['SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED']=flag
            self.assertEqual(self.decision(order,token).status_code,404)
        self.app.config['SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED']=True
        self.review.ready=False
        self.assertEqual(self.decision(order,token).status_code,503)
        self.review.ready=True
        for changes in ({'state':'expired'},{'authorization_verified':False,'expires_at':None},
                {'state':'approved'},{'state':'rejected'}):
            original=deepcopy(self.review.value)
            self.review.value.update(changes)
            self.assertEqual(self.decision(order,token).status_code,409)
            self.review.value=original
        self.review.value['quote_hash']='f'*64
        self.assertEqual(self.decision(order,token).status_code,503)
        text=self.client.get('/admin/shop/'+order['id'],base_url=self.origin).get_data(as_text=True)
        self.assertIn('Freigabe und Ablehnung sind gesperrt',text)
        self.app.extensions['shop_production'].pop('order_review')
        self.assertEqual(self.decision(order,token).status_code,503)
        self.assertFalse(any(c[0] in {'approve','reject'} for c in self.review.calls))

    def test_review_uses_explicit_admin_origin_instead_of_arbitrary_host(self):
        order,token=self.review_order()
        self.app.config['SHOP_REVIEW_ADMIN_ORIGIN']='https://cockpit.example'
        self.assertEqual(self.decision(order,token).status_code,403)
        self.app.config['SHOP_REVIEW_ADMIN_ORIGIN']=self.origin
        self.app.config['SHOP_PUBLIC_ORIGIN']='https://public.example'
        self.assertEqual(self.decision(order,token).status_code,303)

    def test_actual_v2_review_service_admin_decision_is_durable_provider_free_and_get_read_only(self):
        from shop_order_review import ShopOrderReview, OrderReviewJournal, init_order_review_schema
        from test_shop_complete_wheels import make_quote, CONTEXT
        from test_shop_cart_quotes import TYRE,RIM
        self.origin='https://shop.example'
        self.app.config.update(SERVER_NAME='shop.example',SHOP_PUBLIC_ORIGIN=self.origin,
            SHOP_COMPLETE_WHEEL_CHECKOUT_ENABLED=True)
        quote=make_quote(rows=[line(RIM,4,group='set-1'),
            line(TYRE,4,services={'mounting':True},group='set-1')])
        self.assertEqual(quote.snapshot()['version'],2)
        self.store.save_quote(quote.snapshot(),expected_quote_hash=quote.quote_hash,
            customer_id='23',mode='test',delivery_context=CONTEXT)
        order=self.store.create_order(quote.quote_hash,customer_id='23',mode='test',
            idempotency_key='synthetic-admin-review-v2')
        attempt=self.store.claim_payment(order['id'],customer_id='23',mode='test')
        self.store.bind_payment(order['id'],attempt['id'],customer_id='23',mode='test',
            session_id='cs_test_SYNTHETIC_ADMIN',intent_id='pi_SYNTHETIC_ADMIN',
            amount_gross_cent=order['amount_gross_cent'],currency='eur')
        funds={'order_id':order['id'],'customer_id':'23','mode':'test','quote_hash':quote.quote_hash,
            'amount_gross_cent':order['amount_gross_cent'],'currency':'eur','payment_attempt':attempt['id'],
            'session_id':'cs_test_SYNTHETIC_ADMIN','intent_id':'pi_SYNTHETIC_ADMIN'}
        self.store.apply_verified_event({'event_id':'evt_SYNTHETIC_ADMIN_AUTH','order_id':order['id'],
            'attempt_id':attempt['id'],'mode':'test','amount_gross_cent':order['amount_gross_cent'],'currency':'eur',
            'session_id':funds['session_id'],'intent_id':funds['intent_id'],'kind':'authorized','occurred_at':NOW.isoformat()},verified=True)
        with closing(self.storage.connect()) as db:init_order_review_schema(db)
        now=[NOW]
        payment=SimpleNamespace(authorization=lambda identifier:{**funds,'verified':True,'state':'authorized',
            'observed_at':NOW.isoformat(),'authorized_until':(NOW+timedelta(hours=1)).isoformat()})
        journal=OrderReviewJournal(self.storage.connect,clock=lambda:now[0])
        review=ShopOrderReview(self.store,journal,payment,mode='test',enabled=True,clock=lambda:now[0])
        review.ensure(order['id'],customer_id='23')  # Explicit simulated worker verification only.
        self.app.extensions['shop_production']['order_review']=review
        self.login()
        def stored():
            with closing(self.storage.connect()) as db:
                return list(db.execute('SELECT binding_json,state,evidence_json,updated_at FROM shop_order_reviews_v1'))
        before=stored()
        with patch.object(payment,'authorization',side_effect=AssertionError('Provider access forbidden from admin')):
            response=self.client.get('/admin/shop/'+order['id'],base_url=self.origin)
            self.assertEqual(response.status_code,200)
            self.assertEqual(stored(),before)
            with self.client.session_transaction() as value:token=value['csrf_token']
            result=self.decision(order,token,data={'csrf_token':token,'quote_hash':quote.quote_hash,
                'full_order_confirmed':'yes','vehicle_ref':'CAR-1','conditions_confirmed':'yes',
                'certificate_references':'SYNTHETIC original report page 4'})
            self.assertEqual(result.status_code,303)
            status=review.status(order['id'],customer_id='23')
            self.assertEqual(status['state'],'approved')
            self.assertEqual(status['review_evidence']['certificate_references'],['SYNTHETIC original report page 4'])
            self.assertEqual(self.store.get_order(order['id'],customer_id='23',mode='test')['status'],'authorized')
            approved=stored()
            now[0]+=timedelta(hours=2)
            response=self.client.get('/admin/shop/'+order['id'],base_url=self.origin)
            self.assertIn('Kartenreservierung abgelaufen',response.get_data(as_text=True))
            self.assertEqual(stored(),approved,'Expiry on GET must remain a projection without a journal mutation')
            self.assertEqual(self.decision(order,token,decision='ablehnen').status_code,409)

    def test_construction_and_registration_have_no_database_or_filesystem_io(self):
        app=Flask('isolated-registration')
        with patch('sqlite3.connect',side_effect=AssertionError('DB forbidden')), \
             patch.object(Path,'is_file',side_effect=AssertionError('filesystem forbidden')):
            ShopAdminReader(self.storage,self.store)
            register_shop_admin(app)
            register_shop_admin(app)
        self.assertIn('shop_admin',app.blueprints)

    def test_requires_real_portal_admin_no_partner_office_or_truthy_inheritance(self):
        for value,other in ((None,{}),(False,{}),('yes',{}),(1,{}),
                            (None,{'partner_autohaus_id':1}),(None,{'office':True,'office_user_id':'synthetic'})):
            self.login(value,**other)
            with patch('sqlite3.connect',side_effect=AssertionError('DB forbidden')):
                result=self.client.get('/admin/shop')
            self.assertEqual(result.status_code,403)
            self.assertEqual(result.headers['Cache-Control'],'private, no-store')
        self.login()
        self.assertEqual(self.client.get('/admin/shop').status_code,200)

    def test_off_or_lost_storage_returns404_never_creates_empty_database(self):
        self.login()
        self.app.extensions['shop_production']={}
        self.assertEqual(self.client.get('/admin/shop').status_code,404)
        unprovisioned=ShopStorage(self.root/'not-created')
        self.app.extensions['shop_production']={'storage':unprovisioned,'store':unprovisioned.order_store()}
        self.assertEqual(self.client.get('/admin/shop').status_code,404)
        self.assertFalse(unprovisioned.directory.exists())
        self.app.extensions['shop_production']={'storage':self.storage,'store':self.store}
        self.storage.path.unlink()
        self.assertEqual(self.client.get('/admin/shop').status_code,404)
        self.assertFalse(self.storage.path.exists())

    def test_appointment_requires_remaining_authorization_or_paid_order(self):
        now=datetime.now(timezone.utc)
        order={'id':'sho_'+'a'*32,'mode':'live','status':'authorized'}
        related={'payment':'authorized','workflow':None,'action':None,'fulfillment':None,
                 'appointment_day':(now+timedelta(days=5)).date().isoformat(),'appointment_time':'10:00'}
        with closing(self.storage.connect()) as db:
            init_order_review_schema(db)
            tables={'shop_order_reviews_v1'}
            def reason():
                return ShopAdminReader._appointment_blocked_reason(db,tables,order,related)
            self.assertIsNotNone(reason())
            def record(expires,state='approved'):
                db.execute('DELETE FROM shop_order_reviews_v1')
                db.execute('''INSERT INTO shop_order_reviews_v1
                    (order_id,customer_id,mode,binding_json,requirements_json,state,expires_at,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?)''',(order['id'],'customer','live','{}','[]',state,
                    expires.isoformat(),now.isoformat(),now.isoformat()))
            record(now+timedelta(hours=2))
            self.assertIsNone(reason())
            record(now-timedelta(minutes=1))
            self.assertIsNotNone(reason())
            record(now+timedelta(hours=2),'rejected')
            self.assertIsNotNone(reason())
            order['status']='paid'
            self.assertIsNone(reason())
            related['workflow']='cancelled'
            self.assertIsNotNone(reason())

    def test_list_detail_frozen_gross_destinations_and_html_escape(self):
        order=self.order('<script>alert("x")</script> Synthetic oil')
        self.login()
        result=self.client.get('/admin/shop')
        self.assertEqual(result.status_code,200)
        self.assertIn(order['id'],result.get_data(as_text=True))
        detail=self.client.get('/admin/shop/'+order['id'])
        text=detail.get_data(as_text=True)
        self.assertEqual(detail.status_code,200)
        self.assertIn('&lt;script&gt;',text)
        self.assertNotIn('<script>alert',text)
        self.assertIn('68,43 €',text)
        self.assertIn('Direkt zum Kunden',text)
        self.assertIn('keine Rechnung erstellt',text)
        self.assertIn('nach Wareneingang',text)
        self.assertNotIn('PRIVATE_CONTEXT_CANARY',text)
        self.assertNotIn('PRIVATE_CANARY',text)
        self.assertEqual(detail.headers['X-Robots-Tag'],'noindex, nofollow, nosnippet')

    def test_pagination_is_bounded_and_no_mutation_routes_exist(self):
        for _ in range(3):self.order()
        self.login()
        first=self.client.get('/admin/shop?per_page=2').get_data(as_text=True)
        second=self.client.get('/admin/shop?per_page=2&page=2').get_data(as_text=True)
        self.assertEqual(first.count('href="/admin/shop/sho_'),2)
        self.assertEqual(second.count('href="/admin/shop/sho_'),1)
        self.assertIn('Ältere Bestellungen',first)
        self.assertIn('Neuere Bestellungen',second)
        for query in ('per_page=101','per_page=0','page=0','page=10000','page=1&page=2','where=1','page=1%20OR%201=1'):
            self.assertEqual(self.client.get('/admin/shop?'+query).status_code,400)
        with patch('sqlite3.connect',side_effect=AssertionError('DB forbidden')):
            self.assertEqual(self.client.post('/admin/shop',json={'capture':True}).status_code,405)
        self.assertEqual(self.client.get('/admin/shop/not-an-order').status_code,404)
        self.assertEqual(self.client.get('/admin/shop/sho_'+'f'*32).status_code,404)

    def test_invoice_actions_are_explicit_and_pdf_reads_are_private_and_side_effect_free(self):
        identifier = self.order()['id']
        source = InvoiceFixture(identifier)
        self.app.extensions['shop_invoice_service'] = source
        path = '/admin/shop/' + identifier
        prepare, refresh, pdf = (path + suffix for suffix in
                                 ('/rechnung/vorbereiten', '/rechnung/abgleichen', '/rechnung.pdf'))
        self.assertEqual(self.client.get(pdf).status_code, 403)
        self.login()
        self.assertIn('Bereit zur Rechnungserstellung', self.client.get('/admin/shop').get_data(as_text=True))
        before = self.client.get(path).get_data(as_text=True)
        self.assertIn('Endgültige Rechnung erstellen', before)
        self.assertIn('Lexware vergibt eine Rechnungsnummer.', before)
        self.assertNotIn('Online bezahlt.', before)
        with self.client.session_transaction() as saved:
            csrf = {'csrf_token': saved['csrf_token']}
        self.assertEqual(self.client.get(prepare).status_code, 405)
        self.assertEqual(self.client.get(pdf).status_code, 404)
        self.assertEqual(self.client.post(prepare, data={'csrf_token': 'x'*64}).status_code, 403)
        self.assertEqual(self.client.post(prepare, data=csrf, headers={'Origin':'https://fremd.example'}).status_code, 403)
        self.assertEqual(self.client.post(prepare, data={**csrf, 'send': '1'}).status_code, 400)
        self.assertEqual(source.writes, [])
        self.assertEqual(self.client.post(prepare, data=csrf).status_code, 303)
        self.assertIn('Entwurf in Lexware öffnen', self.client.get(path).get_data(as_text=True))
        self.assertEqual(self.client.post(prepare, data=csrf).status_code, 409)
        self.assertEqual(self.client.post(refresh, data=csrf).status_code, 303)
        view = self.client.get(path).get_data(as_text=True)
        self.assertIn('TEST-101', view)
        self.assertIn('PDF öffnen und drucken', view)
        self.assertNotIn('Online bezahlt.', view)
        summary = ShopAdminReader(self.storage, self.store).detail(identifier)
        summary.update(mode='Live', status='Zahlung eingezogen', payment='Zahlung eingezogen')
        with patch.object(ShopAdminReader, 'detail', return_value=summary):
            self.assertIn('Online bezahlt. Die Zahlung lässt sich in Lexware der Rechnung zuordnen.',
                          self.client.get(path).get_data(as_text=True))
        for query, disposition in (('', 'inline'), ('?download=1', 'attachment')):
            response = self.client.get(pdf + query)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.mimetype, 'application/pdf')
            self.assertEqual(response.data, b'%PDF-1.4\nSynthetic')
            self.assertTrue(response.headers['Content-Disposition'].startswith(disposition + ';'))
            self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertEqual(self.client.get(pdf + '?download=1&download=1').status_code, 400)
        self.assertEqual(source.writes, [('prepare', identifier), ('refresh', identifier)])

    def test_uncertain_invoice_can_only_be_reconciled_and_draft_links_are_bounded(self):
        identifier = self.order()['id']
        source = InvoiceFixture(identifier)
        self.app.extensions['shop_invoice_service'] = source
        self.login()
        path = '/admin/shop/' + identifier
        source.state = 'unknown'
        html = self.client.get(path).get_data(as_text=True)
        self.assertIn('Belegstatus prüfen', html)
        self.assertNotIn(path + '/rechnung/vorbereiten', html)
        source.state = 'draft'
        source.edit_url = 'https://app.lexware.de.untrusted.invalid/permalink/invoices/edit/x'
        html = self.client.get(path).get_data(as_text=True)
        self.assertNotIn('untrusted.invalid', html)
        self.assertIn('Belegstatus prüfen', html)
        source.state = 'ineligible'
        self.assertNotIn('Belegstatus prüfen', self.client.get(path).get_data(as_text=True))
        with self.client.session_transaction() as saved:
            csrf = {'csrf_token': saved['csrf_token']}
        for action in ('vorbereiten','abgleichen'):
            self.assertEqual(self.client.post(path + '/rechnung/' + action, data=csrf).status_code, 409)
        self.assertEqual(source.writes, [])

    def test_unknown_workflow_payment_supplier_and_receipt_show_manual_review_without_private_data(self):
        order=self.order()
        attempt=self.store.claim_payment(order['id'],customer_id=order['customer_id'],mode='test')
        self.store.mark_payment_unknown(order['id'],attempt['id'],customer_id=order['customer_id'],mode='test')
        with closing(self.storage.connect()) as db:
            init_fulfillment_schema(db)
            init_order_workflow_schema(db)
            init_order_receipt_schema(db)
            db.execute('''INSERT INTO shop_fulfillment_v2 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (order['id'],order['customer_id'],'test','PRIVATE_BINDING_CANARY','PRIVATE_PLAN','PRIVATE_NET_CANARY',
                 'PRIVATE_TOKEN_CANARY','manual_review','PRIVATE_REASON_CANARY','unknown',NOW.isoformat(),NOW.isoformat()))
            db.execute('''INSERT INTO shop_order_workflow_v2
                (order_id,customer_id,mode,binding_json,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?)''',
                (order['id'],order['customer_id'],'test','PRIVATE_BINDING_CANARY','manual_review',NOW.isoformat(),NOW.isoformat()))
            db.execute('''INSERT INTO shop_order_receipts_v2
                (order_id,kind,customer_id,mode,token,binding_json,recipient,sender,subject,body,legal_hash,fingerprint,created_at,state)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (order['id'],'order_received',order['customer_id'],'test','PRIVATE_MAIL_TOKEN_CANARY','PRIVATE_BINDING_CANARY',
                 'private@example.invalid','private@example.invalid','PRIVATE_SUBJECT_CANARY','PRIVATE_BODY_CANARY',
                 'PRIVATE_LEGAL_CANARY','PRIVATE_FINGERPRINT_CANARY',NOW.isoformat(),'unknown'))
            db.commit()
        self.login()
        for url in ('/admin/shop','/admin/shop/'+order['id']):
            text=self.client.get(url).get_data(as_text=True)
            self.assertIn('Manuelle Prüfung',text)
            self.assertIn('Zahlungsausgang unklar',text)
            self.assertIn('Versand unklar',text)
            self.assertNotIn('PRIVATE_',text)

    def test_database_connection_is_authorized_read_only_and_never_portal_database(self):
        order=self.order()
        reader=ShopAdminReader(self.storage,self.store)
        with self.app.app_context(),reader.read() as (db,tables):
            self.assertIn('shop_orders_v2',tables)
            with self.assertRaises(sqlite3.DatabaseError):db.execute("UPDATE shop_orders_v2 SET status='paid'")
            with self.assertRaises(sqlite3.DatabaseError):db.execute('PRAGMA query_only=OFF')
        self.assertEqual(self.store.get_order(order['id'],customer_id=order['customer_id'],mode='test')['status'],'draft')

    def test_disabled_release_existing_ledger_remains_visible_without_provision_or_hash_change(self):
        order=self.order()
        before=sha256(self.storage.path.read_bytes()).hexdigest()
        marker=self.storage.marker.read_bytes()
        register_shop_admin(self.app,private_data_dir=self.root)
        self.login()
        for adapters in (None,{}, {'ready':False,'reason':'release_configuration_incomplete'}):
            if adapters is None:self.app.extensions.pop('shop_production',None)
            else:self.app.extensions['shop_production']=adapters
            with patch.object(ShopStorage,'provision',side_effect=AssertionError('provision forbidden')), \
                 patch.object(ShopStorage,'connect',side_effect=AssertionError('RW storage connection forbidden')):
                result=self.client.get('/admin/shop')
                self.assertEqual(result.status_code,200)
                self.assertIn(order['id'],result.get_data(as_text=True))
                self.assertEqual(self.client.get('/admin/shop/'+order['id']).status_code,200)
        self.assertEqual(sha256(self.storage.path.read_bytes()).hexdigest(),before)
        self.assertEqual(self.storage.marker.read_bytes(),marker)

    def test_explicit_fallback_root_no_constructor_io_and_no_request_path_override(self):
        self.order()
        with patch('sqlite3.connect',side_effect=AssertionError('DB forbidden')), \
             patch.object(Path,'is_file',side_effect=AssertionError('filesystem forbidden')), \
             patch.object(Path,'resolve',side_effect=AssertionError('filesystem resolution forbidden')):
            register_shop_admin(self.app,private_data_dir=self.root)
        with self.assertRaisesRegex(ValueError,'explicit_shop_admin_root_required'):
            register_shop_admin(Flask('relative-root'),private_data_dir=Path('relative'))
        self.login()
        self.app.extensions['shop_production']={}
        self.assertEqual(self.client.get('/admin/shop?private_data_dir=elsewhere').status_code,400)
        self.assertEqual(self.client.get('/admin/shop').status_code,200)

    def test_fallback_missing_marker_corrupt_foreign_database_and_lost_database_fail_closed(self):
        self.login()
        self.app.extensions['shop_production']={}
        missing=self.root/'not-created'
        register_shop_admin(self.app,private_data_dir=missing)
        self.assertEqual(self.client.get('/admin/shop').status_code,404)
        self.assertFalse(missing.exists())
        foreign=self.root/'foreign'
        folder=foreign/'shop-v2'
        folder.mkdir(parents=True)
        database=folder/'shop.sqlite3'
        with closing(sqlite3.connect(database)) as db:db.execute('CREATE TABLE unrelated (value TEXT)')
        before=sha256(database.read_bytes()).hexdigest()
        register_shop_admin(self.app,private_data_dir=foreign)
        self.assertEqual(self.client.get('/admin/shop').status_code,404)
        (folder/'initialized').write_bytes(b'not-a-shop-marker')
        self.assertEqual(self.client.get('/admin/shop').status_code,404)
        (folder/'initialized').write_bytes(b'shop_storage_v2\n')
        self.assertEqual(self.client.get('/admin/shop').status_code,503)
        self.assertEqual(sha256(database.read_bytes()).hexdigest(),before)
        database.unlink()
        self.assertEqual(self.client.get('/admin/shop').status_code,404)
        self.assertFalse(database.exists())

    def test_withdrawal_queue_is_admin_only_and_optional_for_old_storage(self):
        for url in ('/admin/shop/widerrufe','/admin/shop/widerrufe/swd_'+'f'*32):
            with patch('sqlite3.connect',side_effect=AssertionError('Unauthenticated DB forbidden')):
                self.assertEqual(self.client.get(url).status_code,403)
        self.login(None,partner_autohaus_id=123)
        self.assertEqual(self.client.get('/admin/shop/widerrufe').status_code,403)
        self.login()
        response=self.client.get('/admin/shop/widerrufe')
        self.assertEqual(response.status_code,200)
        self.assertIn('Keine Widerrufserklärungen',response.text)
        with closing(self.storage.connect()) as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='shop_withdrawals_v2'").fetchone())

    def test_unknown_references_are_visible_escaped_and_never_claim_cancellation_or_refund(self):
        identifier=self.declaration()
        self.login()
        for url in ('/admin/shop/widerrufe','/admin/shop/widerrufe/'+identifier):
            response=self.client.get(url)
            self.assertEqual(response.status_code,200)
            self.assertIn('SYNTHETIC external contract',response.text)
            self.assertIn('&lt;script&gt;',response.text)
            self.assertNotIn('<script>SYNTHETIC',response.text)
            self.assertIn('Versand unklar',response.text)
            self.assertIn('Persönlich prüfen',response.text)
            self.assertNotIn('mail_token',response.text)
            self.assertEqual(response.headers['Cache-Control'],'private, no-store')
        response=self.client.get('/admin/shop/widerrufe/'+identifier)
        self.assertIn('keine Erstattung, Stornierung oder Lieferantenaktion',response.text)
        self.assertIn('&lt;b&gt;scope&lt;/b&gt;',response.text)
        with closing(self.storage.connect()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_orders_v2').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_payment_attempts_v2').fetchone()[0],0)

    def test_order_detail_shows_matching_reference_as_unverified_and_respects_mode(self):
        order=self.order()
        matching=self.declaration(order['id'])
        foreign_mode=self.declaration(order['id'],mode='live')
        unrelated=self.declaration('NOT THIS ORDER')
        self.login()
        text=self.client.get('/admin/shop/'+order['id']).text
        self.assertIn(matching,text)
        self.assertNotIn(foreign_mode,text)
        self.assertNotIn(unrelated,text)
        self.assertIn('unverifizierten Kundenangabe',text)
        self.assertIn('noch nicht storniert oder erstattet',text)
        self.assertIn('href="/admin/shop/'+order['id']+'"',self.client.get('/admin/shop/widerrufe/'+matching).text)
        self.assertNotIn('href="/admin/shop/'+order['id']+'"',self.client.get('/admin/shop/widerrufe/'+foreign_mode).text)

    def test_queue_survives_disabled_release_and_is_strictly_read_only(self):
        identifier=self.declaration()
        register_shop_admin(self.app,private_data_dir=self.root)
        self.app.extensions['shop_production']={}
        before=sha256(self.storage.path.read_bytes()).hexdigest()
        self.login()
        with patch.object(ShopStorage,'provision',side_effect=AssertionError('provision forbidden')), \
             patch.object(ShopStorage,'connect',side_effect=AssertionError('RW connection forbidden')):
            self.assertEqual(self.client.get('/admin/shop/widerrufe').status_code,200)
            self.assertEqual(self.client.get('/admin/shop/widerrufe/'+identifier).status_code,200)
        self.assertEqual(sha256(self.storage.path.read_bytes()).hexdigest(),before)
        for url in ('/admin/shop/widerrufe','/admin/shop/widerrufe/'+identifier):
            with patch('sqlite3.connect',side_effect=AssertionError('No mutation route DB')):
                self.assertEqual(self.client.post(url,json={'refund':True,'cancel':True}).status_code,405)

    def test_withdrawal_pagination_filter_and_invalid_query_are_bounded(self):
        order=self.order()
        for _ in range(3):self.declaration(order['id'])
        self.declaration('UNRELATED REFERENCE')
        self.login()
        response=self.client.get('/admin/shop/widerrufe?per_page=2&order_id='+order['id'])
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.text.count('href="/admin/shop/widerrufe/swd_'),2)
        self.assertNotIn('UNRELATED REFERENCE',response.text)
        self.assertIn('Ältere Erklärungen',response.text)
        for query in ('per_page=101','page=0','page=1&page=2','order_id=invalid','capture=true'):
            self.assertEqual(self.client.get('/admin/shop/widerrufe?'+query).status_code,400)
        self.assertEqual(self.client.get('/admin/shop/widerrufe/swd_'+'f'*32).status_code,404)

    def test_cockpit_badge_is_private_and_still_visible_after_selling_release_is_disabled(self):
        self.declaration()
        register_shop_admin(self.app,private_data_dir=self.root)
        self.app.extensions['shop_production']={}
        self.app.add_url_rule('/admin','synthetic_cockpit',
            lambda:render_template_string('{{ shop_admin_available }}|{{ shop_withdrawal_pending }}'))
        self.app.add_url_rule('/admin/cockpit','real_cockpit_path',
            lambda:render_template_string('{{ shop_admin_available }}|{{ shop_withdrawal_pending }}'))
        with patch('sqlite3.connect',side_effect=AssertionError('Anonymous badge DB forbidden')):
            self.assertEqual(self.client.get('/admin').text,'False|0')
        self.login()
        self.assertEqual(self.client.get('/admin').text,'True|1')
        self.assertEqual(self.client.get('/admin/cockpit').text,'True|1')


if __name__=='__main__':unittest.main()
