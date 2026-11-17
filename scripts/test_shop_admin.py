"""Authenticated read-only admin against isolated synthetic shop SQLite only."""
from contextlib import closing
from hashlib import sha256
from pathlib import Path
import json
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'scripts'))
from flask import Flask,render_template_string
from shop_admin import ShopAdminReader, register_shop_admin
from shop_fulfillment import init_fulfillment_schema
from shop_order_receipts import init_order_receipt_schema
from shop_order_workflow import init_order_workflow_schema
from shop_storage import ShopStorage
from shop_consumer import init_consumer_schema
from test_shop_cart_quotes import Fixture,NOW,OIL,line


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
