"""Offline persistent monitor regressions: fake IMAP, synthetic DB, no network."""
from copy import deepcopy
import ast
import hashlib
import json
import re
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from test_mailquellen import FakeMailbox, FakePortal, message
from werkstatt_artikel_import import InvoiceCatalog
from werkstatt_mailquellen import MailSources
from werkstatt_einkaufsmonitor import PurchaseMonitor, TABLES, register_monitor, start_purchase_monitor_worker


class IncrementalMailbox(FakeMailbox):
    def __init__(self,folders):
        super().__init__(folders)
        self.searches=[]
        self.omit=set()
    def response(self,name):
        assert name=='UIDNEXT'
        return name,[str(max(self.data[self.current],default=0)+1).encode()]
    def uid(self,command,*args):
        if command=='SEARCH' and len(args)>2 and args[1]=='UID':
            self.searches.append((self.current,args[2]))
            value=args[2]
            if ':' in value:
                lo,hi=map(int,value.split(':'))
                ids=[uid for uid in sorted(self.data[self.current]) if lo<=uid<=hi]
            else:
                requested=set(map(int,value.split(',')))
                ids=[uid for uid in sorted(self.data[self.current]) if uid in requested]
            return 'OK',[b' '.join(str(uid).encode() for uid in ids)]
        status,rows=super().uid(command,*args)
        if command=='FETCH':
            rows=[row for row in rows if not any(('UID '+str(uid)+' ').encode() in row[0] for uid in self.omit)]
        return status,rows


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.net=patch.object(socket.socket,'connect',side_effect=AssertionError('NETWORK FORBIDDEN'))
        self.net.start()
        self.addCleanup(self.net.stop)
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p=FakePortal(Path(self.temp.name))
        with self.p.get_db() as db:
            db.execute('DROP TABLE assistent_rechnungsartikel')
            db.execute('DROP TABLE assistent_rechnungsimporte')
        self.p.get_app_setting=lambda key,default='':default
        self.mail=IncrementalMailbox({'INBOX':{1:message()}})
        self.p.assistant_mail_sources=MailSources(self.p,self.mail)
        self.p.cockpit_data=SimpleNamespace(catalog=InvoiceCatalog(self.p))
        self.service=register_monitor(self.p)
        self.reads=[]
        self.read=patch('werkstatt_artikel_import.read_source',side_effect=self.reader)
        self.reader_mock=self.read.start()
        self.addCleanup(self.read.stop)

    def rows(self,sql,args=()):
        with self.p.get_db() as db:
            return [dict(row) for row in db.execute(sql,args).fetchall()]

    def reader(self,portal,kind,source_id):
        self.reads.append((kind,source_id))
        return dict(status='ok',candidates=[dict(produkt_name='Synthetic paint',artikelnummer='TEST-001',
            ve='Dose',gebinde='1 L',groesse='',farbe='white',preis='12.50',menge='2',
            price_evidence={'value':'12.50','basis':'gebindepreis_netto_abgeleitet'},
            quantity_evidence={'value':'2','unit':'Dose','basis':'invoice_line','source_field':'Menge'},
            package_evidence={'value':'1','unit':'L','per_unit':'Dose','basis':'explicit_description','text':'1 L'},
            source={'date':'2026-09-30','page':1,'position':1,'file_id':'synthetic','sha256':'1'*64})],
            coverage={'files_total':1,'files_read':1,'pages_total':1,'pages_read':1,'pages_attempted':1,'complete':True},warnings=[])

    def finish(self):
        for _ in range(50):
            state=self.service.tick(max_steps=2,force=True)
            if state['mail_state']=='done' and not state['counts']['queued']:
                return state
        self.fail('Monitor did not settle')

    def enable(self):
        self.service.configure(True)

    def test_registration_disabled_tick_and_worker_are_inert(self):
        self.assertFalse(self.service.status()['enabled'])
        self.assertFalse(start_purchase_monitor_worker(self.p))
        self.service.tick(force=True)
        self.assertEqual((self.mail.selected,self.reads),([],[]))
        self.assertTrue(set(TABLES)<=set(row['name'] for row in self.rows("SELECT name FROM sqlite_master WHERE type='table'")))
        self.assertFalse(self.service.status()['worker_recent'])

    def test_incremental_poll_after_done_reads_only_new_uid_and_preserves_evidence(self):
        self.enable()
        state=self.finish()
        self.assertEqual(state['counts']['complete'],1)
        self.assertFalse(state['worker_recent'])
        payload=json.loads(self.rows('SELECT payload_json FROM assistent_rechnungsartikel')[0]['payload_json'])
        self.assertEqual(payload['quelle']['datum'],'2026-09-30')
        self.assertEqual(payload['quantity_evidence']['value'],'2')
        self.assertFalse(payload['preis_geprueft'])
        self.assertFalse(payload['bestellbar'])
        self.mail.data['INBOX'][2]=message(subject='Rechnung RE12346',payload=b'%PDF-1.4 second synthetic')
        before=len(self.mail.header_calls)
        state=self.finish()
        self.assertEqual(self.mail.header_calls[before:],[('INBOX','2')])
        self.assertEqual(self.mail.raw_calls,[('INBOX','1'),('INBOX','2')])
        self.assertEqual(len(self.reads),2)
        self.assertEqual(state['counts']['complete'],2)
        self.finish()
        self.assertEqual(len(self.reads),2)

    def test_resume_after_restart_keeps_partial_header_window(self):
        self.mail.data['INBOX']={i:message(payload=None,subject='Sortiment') for i in range(1,62)}
        self.enable()
        self.service.tick(max_steps=1,force=True)
        folder=self.rows('SELECT * FROM assistent_mailquellen_ordner')[0]
        self.assertEqual((folder['cursor'],folder['high_water_uid']),(40,0))
        self.service=PurchaseMonitor(self.p)
        self.service.tick(max_steps=1,force=True)
        self.assertEqual(self.mail.header_calls[-1],('INBOX',','.join(map(str,range(41,62)))))
        folder=self.rows('SELECT * FROM assistent_mailquellen_ordner')[0]
        self.assertEqual(folder['high_water_uid'],61)

    def test_uidvalidity_change_rescans_headers_and_never_reuses_old_uid_identity(self):
        self.enable()
        self.finish()
        self.mail.versions['INBOX']='2'
        self.mail.data['INBOX']={1:message(subject='Rechnung RE77777',payload=b'%PDF-1.4 replaced mailbox')}
        self.finish()
        self.assertEqual({row['validity'] for row in self.rows('SELECT validity FROM assistent_mailquellen_nachrichten')},{'1','2'})
        self.assertEqual(len(self.reads),2)
        self.assertEqual(self.rows('SELECT high_water_uid FROM assistent_mailquellen_ordner')[0]['high_water_uid'],1)

    def test_finance_personal_unknown_spoof_and_sent_never_read_bodies(self):
        self.mail.data={'INBOX':{1:message(subject='SEPA Mandat'),2:message(sender='Topcolor <unknown@example.test>'),
            3:message(subject='Gutschrift RE12345'),4:message(sender='Other <unknown@unknown.test>')},
            'Personal':{1:message()},'Bank':{1:message()},'Sent':{1:message()}}
        self.mail.versions={name:'1' for name in self.mail.data}
        self.enable()
        self.finish()
        self.assertEqual(set(self.mail.selected),{'INBOX'})
        self.assertEqual(self.mail.raw_calls,[])
        self.assertEqual(self.reads,[])

    def test_exact_message_and_attachment_dedup_across_folders(self):
        same=self.mail.data['INBOX'][1]
        self.mail.data['Archive']={99:same}
        self.mail.versions['Archive']='1'
        self.enable()
        self.finish()
        self.assertEqual(len(self.rows('SELECT * FROM einkauf_belege')),1)
        self.assertEqual(len(self.rows('SELECT * FROM assistent_einkaufsmonitor_quellen')),1)
        self.assertEqual(len(self.reads),1)
        messages=self.rows('SELECT * FROM assistent_mailquellen_nachrichten ORDER BY id')
        self.assertEqual(messages[1]['duplicate_of'],messages[0]['id'])
        self.assertEqual(messages[0]['raw_sha256'],messages[1]['raw_sha256'])

    def test_same_message_id_different_original_is_not_suppressed(self):
        self.mail.data['INBOX'][2]=message(payload=b'%PDF-1.4 a different original')
        self.enable()
        self.finish()
        messages=self.rows('SELECT * FROM assistent_mailquellen_nachrichten')
        self.assertEqual(messages[0]['message_id'],messages[1]['message_id'])
        self.assertNotEqual(messages[0]['message_fingerprint'],messages[1]['message_fingerprint'])
        self.assertEqual(len(self.reads),2)

    def test_missing_header_is_retried_without_advancing_checkpoint(self):
        self.mail.omit={1}
        self.enable()
        state=self.service.tick(max_steps=1,force=True)
        self.assertEqual(state['state'],'error')
        self.assertEqual(self.rows('SELECT high_water_uid FROM assistent_mailquellen_ordner')[0]['high_water_uid'],0)
        self.assertEqual(self.mail.raw_calls,[])
        self.mail.omit=set()
        self.finish()
        self.assertEqual(len(self.reads),1)

    def test_pending_lexware_source_is_never_processed(self):
        with self.p.get_db() as db:
            db.execute("INSERT INTO assistent_rechnungsimporte(source_key,source_kind,source_id,supplier,reference) VALUES('lexware:synthetic','lexware','synthetic','TOP-Color GmbH','Synthetic prior invoice')")
        self.enable()
        self.finish()
        self.assertEqual(len(self.reads),1)
        self.assertEqual(self.reads[0][0],'einkauf')
        self.assertEqual(self.rows("SELECT state FROM assistent_rechnungsimporte WHERE source_kind='lexware'")[0]['state'],'offen')

    def test_second_worker_is_excluded_even_during_catalog_read(self):
        self.enable()
        other=PurchaseMonitor(self.p)
        reports=[]
        def reading(*args):
            reports.append(other.tick(force=True))
            return self.reader(*args)
        self.reader_mock.side_effect=reading
        self.finish()
        self.assertEqual(len(reports),1)
        self.assertTrue(reports[0]['busy'])
        self.assertEqual(len(self.reads),1)

    def test_catalog_crash_resumes_from_saved_original_without_second_body_download(self):
        self.enable()
        with patch.object(self.p.cockpit_data.catalog,'process_next',side_effect=OSError('synthetic failure')):
            for _ in range(6):
                state=self.service.tick(force=True)
                if state['counts']['error']:
                    break
        self.assertEqual(state['counts']['error'],1)
        self.assertEqual(len(self.mail.raw_calls),1)
        with self.p.get_db() as db:
            db.execute('UPDATE assistent_einkaufsmonitor_quellen SET next_retry_at=0')
        self.finish()
        self.assertEqual(len(self.mail.raw_calls),1)
        self.assertEqual(len(self.reads),1)

    def test_account_change_is_disabled_and_does_not_inherit_queue_or_credentials(self):
        self.enable()
        self.finish()
        self.p.get_werkstatt_imap_config=lambda:{'configured':True,'host':'other.example.test','port':993,'user':'other@example.test'}
        self.assertFalse(self.service.status()['enabled'])
        self.service.tick(force=True)
        self.assertEqual(len(self.reads),1)
        self.assertEqual(self.service.status()['counts']['complete'],0)

    def test_disable_fences_running_mail_publication(self):
        self.enable()
        original=self.mail.raw
        def raw(*args):
            result=original(*args)
            self.service.configure(False)
            return result
        self.mail.raw=raw
        for _ in range(3):
            state=self.service.tick(force=True)
        self.assertFalse(state['enabled'])
        self.assertEqual(self.rows('SELECT * FROM einkauf_belege'),[])

    def test_new_supplier_permission_resumes_held_unread_source(self):
        self.enable()
        with patch.object(self.p.cockpit_data.catalog,'source_rule',return_value={'allowed':False,'decision':'review'}):
            state=self.finish()
        self.assertEqual(state['counts']['review'],1)
        self.assertEqual(self.reads,[])
        state=self.finish()
        self.assertEqual(state['counts']['complete'],1)

    def test_disable_during_catalog_read_fences_article_publication(self):
        self.enable()
        def reading(*args):
            result=self.reader(*args)
            self.service.configure(False)
            return result
        self.reader_mock.side_effect=reading
        for _ in range(8):
            state=self.service.tick(force=True)
            if not state['enabled']:
                break
        self.assertFalse(state['enabled'])
        self.assertEqual(self.rows('SELECT * FROM assistent_rechnungsartikel'),[])
        self.assertEqual(self.rows('SELECT state FROM assistent_rechnungsimporte')[0]['state'],'offen')
        self.reader_mock.side_effect=self.reader
        self.enable()
        state=self.finish()
        self.assertEqual(state['counts']['complete'],1)
        self.assertEqual(len(self.mail.raw_calls),1)

    def test_disable_before_poll_start_prevents_new_header_read(self):
        self.enable()
        original=self.p.assistant_mail_sources.start_incremental
        def starting(*args,**kwargs):
            self.service.configure(False)
            return original(*args,**kwargs)
        with patch.object(self.p.assistant_mail_sources,'start_incremental',side_effect=starting):
            state=self.service.tick(force=True)
        self.assertFalse(state['enabled'])
        self.assertEqual(self.mail.header_calls,[])

    def test_sparse_uid_search_is_bounded_and_preserves_all_new_messages(self):
        self.mail.data['INBOX']={2:message(),1100:message(subject='Rechnung RE99999',payload=b'%PDF-1.4 synthetic sparse')}
        self.enable()
        self.finish()
        self.assertEqual(self.mail.searches,[('INBOX','1:1000'),('INBOX','1001:1100')])
        self.assertEqual(len(self.reads),2)

    def test_replaced_worker_lease_cannot_publish_or_clear_successor(self):
        self.enable()
        def reading(*args):
            result=self.reader(*args)
            with self.p.get_db() as db:
                db.execute("UPDATE assistent_einkaufsmonitor SET lease='successor',lease_until=99999999999")
            return result
        self.reader_mock.side_effect=reading
        for _ in range(8):
            state=self.service.tick(force=True)
            if state['busy']:
                break
        self.assertTrue(state['busy'])
        self.assertEqual(self.rows('SELECT lease FROM assistent_einkaufsmonitor')[0]['lease'],'successor')
        self.assertEqual(self.rows('SELECT * FROM assistent_rechnungsartikel'),[])

    def test_actual_postgres_adapter_schema_guards_and_targeted_import(self):
        source=Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf-8')
        names={'DbRow','PostgresCursor','PostgresConnection','split_sql_script','get_insert_table_name','convert_sqlite_sql_to_postgres'}
        nodes=[node for node in ast.parse(source).body if isinstance(node,(ast.ClassDef,ast.FunctionDef)) and node.name in names]
        namespace={'re':re}
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'<real-postgres-adapter>','exec'),namespace)
        original=self.p.get_db
        statements=[]
        class Cursor:
            def __init__(self,db):
                self.raw=db.cursor()
            def __enter__(self):
                return self
            def __exit__(self,*args):
                self.raw.close()
            def execute(self,sql,params):
                statements.append(sql)
                self.raw.execute(sql.replace('%s','?').replace('SERIAL PRIMARY KEY','INTEGER PRIMARY KEY AUTOINCREMENT'),params)
                self.rowcount=self.raw.rowcount
                self.description=[SimpleNamespace(name=item[0]) for item in self.raw.description] if self.raw.description else None
            def fetchall(self):
                return self.raw.fetchall()
        def db():
            raw=original()
            return namespace['PostgresConnection'](SimpleNamespace(cursor=lambda:Cursor(raw),commit=raw.commit,rollback=raw.rollback,close=raw.close))
        with patch.object(self.p,'get_db',side_effect=db):
            self.service.init_schema()
            self.p.assistant_mail_sources.init_schema()
            self.enable()
            state=self.finish()
        self.assertEqual(state['counts']['complete'],1)
        self.assertEqual(len(self.reads),1)
        self.assertTrue(all('SERIAL PRIMARY KEY' in sql for sql in statements if sql.startswith('CREATE TABLE')))
        self.assertTrue(all('RETURNING' in sql for sql in statements if sql.startswith('INSERT')))

    def test_heartbeat_reports_executed_worker_not_only_enabled_setting(self):
        self.p.app.config['PURCHASE_MONITOR_WORKER_ENABLED']=True
        self.enable()
        self.assertTrue(self.service.status()['worker_enabled'])
        self.assertFalse(self.service.status()['worker_recent'])
        self.service.tick(worker=True)
        self.assertTrue(self.service.status()['worker_recent'])

    def test_cli_status_never_connects_and_schema_hook_preserves_progress(self):
        self.enable()
        self.finish()
        before=self.rows('SELECT * FROM assistent_einkaufsmonitor_quellen')
        self.p.workshop_purchase_monitor_init_schema()
        self.assertEqual(self.rows('SELECT * FROM assistent_einkaufsmonitor_quellen'),before)
        reads=len(self.mail.raw_calls)
        result=self.p.app.test_cli_runner().invoke(args=['werkstatt-einkaufsmonitor','--status'])
        self.assertEqual(result.exit_code,0,result.output)
        self.assertEqual(len(self.mail.raw_calls),reads)

    def test_configuration_validation(self):
        for args in [(1,300),(True,True),(True,1),(False,100000)]:
            with self.subTest(args=args),self.assertRaises(ValueError):
                self.service.configure(*args)
        with self.assertRaises(PermissionError):
            self.service.configure(True,actor='mitarbeiter:1')


if __name__=='__main__':
    unittest.main(verbosity=2)
