"""Offline mailbox inventory tests: fake IMAP, synthetic receipts, temp DB only."""
from contextlib import contextmanager
from email.message import EmailMessage
from email import policy
from functools import wraps
import ast
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from flask import Flask, abort, session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_mailquellen import MailSources, register_mail_sources, TABLES


def message(sender='TOP-Color <rechnung@top-color.net>', subject='Rechnung RE12345', target='workshop@example.test', payload=b'%PDF-1.4 synthetic receipt', filename='RE12345.pdf'):
    msg=EmailMessage()
    msg['From']=sender;msg['To']=target;msg['Subject']=subject;msg['Message-ID']='<synthetic-'+hashlib.sha256((sender+subject).encode()).hexdigest()+'@example.test>'
    msg.set_content('Synthetic product receipt; never sent.')
    if payload is not None:
        msg.add_attachment(payload,maintype='application',subtype='pdf',filename=filename)
    return msg


class ClosingConnection(sqlite3.Connection):
    def __exit__(self,*args):
        try:return super().__exit__(*args)
        finally:self.close()


class FakePortal:
    def __init__(self, root):
        self.path=root/'test.sqlite';self.UPLOAD_DIR=root/'uploads'
        self.app=Flask(__name__,template_folder=str(Path(__file__).resolve().parents[1]/'templates'))
        self.app.config.update(SECRET_KEY='synthetic',TESTING=True)
        self.app.jinja_env.globals.update(csrf_field=lambda:'<input name="csrf_token" value="synthetic">')
        with self.get_db() as db:
            db.executescript('''CREATE TABLE app_settings(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT);
                CREATE TABLE assistent_rechnungsimporte(id INTEGER PRIMARY KEY AUTOINCREMENT,source_kind TEXT,source_id TEXT,state TEXT,lease TEXT,result_json TEXT);
                CREATE TABLE assistent_rechnungsartikel(id INTEGER PRIMARY KEY AUTOINCREMENT,import_id INTEGER,active INTEGER);
                CREATE TABLE einkauf_belege(id INTEGER PRIMARY KEY AUTOINCREMENT,beleg_typ TEXT,lieferant TEXT,original_name TEXT,stored_name TEXT,mime_type TEXT,size INTEGER,extrahierter_text TEXT,positionen_count INTEGER,status TEXT,erstellt_am TEXT);''')
    def get_db(self):
        db=sqlite3.connect(self.path,timeout=5,factory=ClosingConnection);db.row_factory=sqlite3.Row;return db
    def get_werkstatt_imap_config(self):
        return {'configured':True,'ssl':True,'host':'imap.example.test','port':993,'user':'workshop@example.test'}
    @staticmethod
    def admin_required(fn):
        @wraps(fn)
        def wrapped(*args,**kwargs):
            if not session.get('admin'):abort(403)
            return fn(*args,**kwargs)
        return wrapped


class FakeMailbox:
    def __init__(self, folders):
        self.data=folders;self.current='';self.selected=[];self.raw_calls=[];self.header_calls=[];self.versions={name:'1' for name in folders};self.fail=False;self.raw_error=False
    @contextmanager
    def connect(self):
        if self.fail:raise OSError('synthetic connection failure')
        yield self
    def folders(self, client):
        return [{'id':name,'label':name} for name in self.data]
    def select(self,client,folder,readonly=True,validity=None):
        assert readonly is True,'NO IMAP writes permitted'
        if validity is not None and validity!=self.versions[folder]:raise ValueError('Changed folder')
        self.current=folder;self.selected.append(folder)
        return self.versions[folder]
    def uid(self,command,*args):
        if command.upper()=='SEARCH':
            return 'OK',[b' '.join(str(x).encode() for x in sorted(self.data[self.current]))]
        assert command.upper()=='FETCH' and 'BODY.PEEK[HEADER.FIELDS' in args[1]
        self.header_calls.append((self.current,args[0]))
        rows=[]
        for uid in str(args[0]).split(','):
            msg=self.data[self.current].get(int(uid))
            if msg is not None:
                header=EmailMessage()
                for key in ('From','To','Subject','Date','Message-ID'):
                    if msg.get(key):header[key]=msg[key]
                rows.append((f'1 (UID {uid} BODY[HEADER]'.encode(),header.as_bytes()))
        return 'OK',rows
    def raw(self,client,uid):
        self.raw_calls.append((self.current,uid))
        if self.raw_error:raise ValueError('Oversize or gone')
        return self.data[self.current][int(uid)].as_bytes(policy=policy.SMTP),''


class MailSourcesTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.p=FakePortal(Path(self.temp.name));self.mail=FakeMailbox({'INBOX':{}})
        self.service=MailSources(self.p,self.mail)
    def rows(self,sql,args=()):
        with self.p.get_db() as db:return [dict(x) for x in db.execute(sql,args).fetchall()]
    def finish(self):
        report=self.service.start()
        for _ in range(200):
            if report['state']!='active':return report
            report=self.service.step()
        self.fail('Importer did not settle')
    def test_all_folder_cursor_resume_no_finance_or_spoof_body_access(self):
        self.mail.data={'INBOX':{i:message(payload=None,subject='Sortiment') for i in range(1,44)},
          'Lieferant':{1:message()},'Bank/Kontoauszuege':{1:message()},'Privat Reise':{1:message()},
          'Rechnung/Offen':{1:message(sender='TOP-Color <fraud@example.test>'),2:message(sender='Service <bank@commerzbank.de>'),3:message(sender='info@tech-masters.de',subject='Gutschrift Rechnung826-1')},
          'Gesendete Objekte':{1:message(sender='workshop@example.test',target='rechnung@top-color.net')}}
        self.mail.versions={name:'1' for name in self.mail.data}
        report=self.service.start();self.service.step()
        report=self.service.pause();self.assertEqual(report['state'],'paused')
        self.assertEqual(sum(x['cursor'] for x in report['folders']),40)
        report=self.finish()
        self.assertEqual(report['state'],'done');self.assertTrue(report['headers_complete']);self.assertFalse(report['complete'])
        self.assertNotIn('Bank/Kontoauszuege',self.mail.selected);self.assertNotIn('Privat Reise',self.mail.selected)
        self.assertNotIn(('Rechnung/Offen','1'),self.mail.raw_calls);self.assertNotIn(('Rechnung/Offen','2'),self.mail.raw_calls);self.assertNotIn(('Rechnung/Offen','3'),self.mail.raw_calls)
        self.assertNotIn(('Gesendete Objekte','1'),self.mail.raw_calls)
        self.assertEqual(len(self.rows('SELECT * FROM einkauf_belege')),1)
        self.assertEqual(report['counts']['review'],1)
    def test_same_attachment_dedup_across_messages_runs_and_restore(self):
        self.mail.data['INBOX']={1:message(),2:message(subject='Weiterleitung Rechnung RE12345')}
        report=self.finish();self.assertEqual(report['counts']['files'],2)
        self.assertEqual(len(self.rows('SELECT * FROM einkauf_belege')),1)
        self.finish();self.assertEqual(len(self.mail.raw_calls),2)
        self.assertEqual(len(self.rows('SELECT * FROM assistent_mailquellen_dateien')),1)
        row=self.rows('SELECT * FROM assistent_mailquellen_dateien')[0]
        path=self.p.UPLOAD_DIR/row['stored_name'];path.unlink()
        self.assertEqual(self.service.restore_files(),1);self.assertEqual(path.read_bytes(),b'%PDF-1.4 synthetic receipt')
        path.write_bytes(b'changed')
        with self.assertRaises(ValueError):self.service._restore_row(row)
        self.assertEqual(path.read_bytes(),b'changed')
    def test_unknown_approval_is_exact_read_scope_and_resumes_snapshot(self):
        self.mail.data['INBOX']={1:message(sender='Parts Test <parts@example.test>'),2:message(sender='Parts Test <other@example.test>')}
        report=self.finish();self.assertEqual(len(self.mail.raw_calls),0)
        item=next(r for r in report['unknown_senders'] if r['sender']=='parts@example.test')
        self.service.approve(item['id'],'Synthetic Materials GmbH')
        report=self.finish();self.assertEqual(self.mail.raw_calls,[('INBOX','1')]);self.assertEqual(report['counts']['review'],1)
        setting=json.loads(self.rows("SELECT value FROM app_settings WHERE key='ASSISTANT_MATERIAL_SUPPLIERS'")[0]['value'])
        self.assertEqual(setting,['Synthetic Materials GmbH'])
        self.assertEqual(self.rows('SELECT supplier FROM assistent_mailquellen_absender')[0]['supplier'],'Synthetic Materials GmbH')
    def test_changed_permission_checked_before_body(self):
        self.mail.data['INBOX']={1:message(sender='Parts <parts@example.test>')}
        report=self.finish();item=report['unknown_senders'][0]
        self.service.approve(item['id'],'Synthetic Materials GmbH');self.service.start()
        with self.p.get_db() as db:db.execute('DELETE FROM assistent_mailquellen_absender')
        report=self.service.step();self.assertEqual(self.mail.raw_calls,[]);self.assertEqual(report['counts']['review'],1)
    def test_unknown_outgoing_never_approvable_as_incoming_invoice(self):
        self.mail.data['INBOX']={1:message(sender='workshop@example.test',target='unknown@example.test')}
        report=self.finish();self.assertEqual(report['counts']['other'],1);self.assertEqual(report['unknown_senders'],[])
        row=self.rows('SELECT id FROM assistent_mailquellen_nachrichten')[0]
        with self.assertRaises(ValueError):self.service.approve(row['id'],'Synthetic Materials GmbH')
        self.assertEqual(self.mail.raw_calls,[])
    def test_sent_folder_alias_never_creates_supplier_body_permission(self):
        self.mail.data={'Sent Items':{1:message(sender='alias@example.test',target='unknown@example.test')}};self.mail.versions={'Sent Items':'1'}
        report=self.finish();self.assertEqual(report['counts']['other'],1);self.assertEqual(report['unknown_senders'],[])
        self.assertEqual(self.mail.raw_calls,[])
    def test_explicit_new_snapshot_retries_transient_body_failure(self):
        self.mail.data['INBOX']={1:message()};self.mail.raw_error=True
        report=self.finish();self.assertEqual(report['counts']['review_files'],1)
        self.mail.raw_error=False;report=self.finish();self.assertEqual(report['counts']['files'],1)
        row=self.rows('SELECT * FROM assistent_mailquellen_dateien')[0];path=self.p.UPLOAD_DIR/row['stored_name'];path.unlink()
        self.assertTrue(self.service.restore_file(row['stored_name']));self.assertTrue(path.is_file())
        self.assertFalse(self.service.restore_file('../foreign.pdf'));self.assertFalse(self.service.restore_file('unknown.pdf'))
    def test_credit_attachment_never_creates_positive_invoice(self):
        self.mail.data['INBOX']={1:message(filename='Gutschrift_RE12345.pdf'),2:message(subject='Reklamation Rechnung'),3:message(sender='info@tech-masters.de',subject='Rechnung826-00123')}
        report=self.finish();self.assertEqual(len(self.rows('SELECT * FROM einkauf_belege')),1)
        self.assertEqual(report['counts']['other'],2)
    def test_payment_headers_never_fetch_body_and_payment_files_not_staged(self):
        self.mail.data['INBOX']={1:message(subject='Überweisung 210000 Doppelzahlung TEST26-RE001122'),
          2:message(subject='SEPA-Mandat'),3:message(subject='Rechnung RE12345',filename='SEPA-Mandat.pdf'),
          4:message(subject='Rechnung RE67890'),5:message(sender='info@tech-masters.de',subject='TECH-MASTERS Deutschland GmbH - M26-00123'),
          6:message(subject='Rechnung M26-00123')}
        report=self.finish()
        self.assertEqual(self.mail.raw_calls,[('INBOX','3'),('INBOX','4'),('INBOX','6')])
        self.assertEqual(len(self.rows('SELECT * FROM einkauf_belege')),1)
        self.assertEqual(report['counts']['excluded'],3)
    def test_status_quarantines_historical_payment_metadata_without_reading_file(self):
        self.mail.data['INBOX']={1:message()};self.finish()
        bid=self.rows('SELECT id FROM einkauf_belege')[0]['id']
        with self.p.get_db() as db:
            db.execute("UPDATE assistent_mailquellen_nachrichten SET subject='Überweisung Doppelzahlung TEST26-RE001122'")
            imp=db.execute("INSERT INTO assistent_rechnungsimporte(source_kind,source_id,state,lease) VALUES('einkauf',?,'laeuft','old-lease')",(str(bid),)).lastrowid
            db.execute('INSERT INTO assistent_rechnungsartikel(import_id,active) VALUES(?,1)',(imp,))
        calls=list(self.mail.raw_calls)
        with patch.object(Path,'read_bytes',side_effect=AssertionError('NO FILE READ')):
            report=self.service.status()
        self.assertEqual(self.mail.raw_calls,calls);self.assertEqual(report['counts']['excluded'],1)
        self.assertEqual(report['quarantined_files'],1)
        self.assertEqual(self.rows('SELECT beleg_typ FROM einkauf_belege')[0]['beleg_typ'],'gesperrt')
        self.assertEqual(self.rows('SELECT state,lease FROM assistent_rechnungsimporte')[0],{'state':'ausgeschlossen','lease':''})
        self.assertEqual(self.rows('SELECT active FROM assistent_rechnungsartikel')[0]['active'],0)
        self.assertEqual(len(self.rows('SELECT id FROM assistent_mailquellen_dateien')),1)
        stored=self.rows('SELECT stored_name FROM assistent_mailquellen_dateien')[0]['stored_name']
        self.assertFalse(self.service.restore_file(stored))
        with self.service.db() as db:
            with self.assertRaises(ValueError):self.service._stage(db,b'%PDF-1.4 synthetic receipt','new-neutral-name.pdf','TOP-Color GmbH')
    def test_unknown_sender_search_finds_rare_supplier_beyond_first_hundred(self):
        self.service.start();account,_=self.service.identity()
        with self.p.get_db() as db:
            token=db.execute('SELECT run_token FROM assistent_mailquellen_laeufe').fetchone()['run_token']
            for i in range(102):
                db.execute('''INSERT INTO assistent_mailquellen_nachrichten(account,run_token,folder,validity,uid,sender,sender_name,subject,state,updated_at)
                    VALUES(?,?,'INBOX','1',?,?,?,?, 'review','synthetic')''',
                    (account,token,str(i+1),f'newsletter{i:03}@example.test','Newsletter','Neuigkeiten'))
            db.execute('''INSERT INTO assistent_mailquellen_nachrichten(account,run_token,folder,validity,uid,sender,sender_name,subject,state,updated_at)
                VALUES(?,?,'INBOX','1','200','zzparts@example.test','Seltener Lieferant','Car-Parts Angebot', 'review','synthetic')''',(account,token))
            db.execute("UPDATE assistent_mailquellen_laeufe SET state='paused'")
        report=self.service.status();self.assertEqual(len(report['unknown_senders']),100)
        self.assertNotIn('zzparts@example.test',[row['sender'] for row in report['unknown_senders']])
        for query in ['Car-Parts','zzparts','Seltener']:
            report=self.service.status(query);self.assertEqual([row['sender'] for row in report['unknown_senders']],['zzparts@example.test'])
            self.assertEqual(report['counts']['review'],103)
        self.assertEqual(self.service.status('%')['unknown_senders'],[])
        self.assertEqual(self.service.status("' OR 1=1 --")['unknown_senders'],[])
        self.assertEqual(self.mail.raw_calls,[])
    def test_changed_uidvalidity_and_body_limit_are_visible_not_retry_loops(self):
        self.mail.data['INBOX']={i:message() for i in range(1,45)}
        self.service.start();self.service.step();self.mail.versions['INBOX']='2'
        report=self.service.step();self.assertEqual(report['folders'][0]['state'],'changed')
        report=self.service.step();self.assertEqual(report['state'],'done');self.assertFalse(report['complete']);self.assertEqual(self.mail.raw_calls,[])
        self.mail.raw_error=True
        report=self.finish();self.assertEqual(report['state'],'done');self.assertGreater(report['counts']['review_files'],0)
    def test_connection_failure_pauses_cursor_and_resume(self):
        self.mail.data['INBOX']={1:message()};self.service.start();self.mail.fail=True
        report=self.service.step();self.assertEqual(report['state'],'paused');self.assertEqual(report['folders'][0]['cursor'],0)
        self.assertNotIn('synthetic connection',report['error']);self.mail.fail=False
        report=self.finish();self.assertEqual(report['counts']['files'],1)

    def test_missing_folder_identity_pauses_before_headers(self):
        self.mail.data['INBOX']={1:message()}
        self.mail.versions['INBOX']=''
        self.service.start()
        report=self.service.step()
        self.assertEqual(report['state'],'paused')
        self.assertEqual(report['folders'][0]['cursor'],0)
        self.assertEqual(self.mail.header_calls,[])
        self.assertEqual(self.mail.raw_calls,[])
    def test_invalid_file_has_visible_review_not_silent_complete(self):
        self.mail.data['INBOX']={1:message(payload=b'not an invoice')}
        report=self.finish();self.assertEqual(report['counts']['review_files'],1);self.assertFalse(report['complete'])
        self.assertEqual(self.rows('SELECT * FROM einkauf_belege'),[])
    def test_legacy_file_dedup_and_foreign_supplier_rejected(self):
        raw=b'%PDF-1.4 synthetic receipt';self.p.UPLOAD_DIR.mkdir();(self.p.UPLOAD_DIR/'manual.pdf').write_bytes(raw)
        with self.p.get_db() as db:db.execute("INSERT INTO einkauf_belege(beleg_typ,lieferant,stored_name,size) VALUES('rechnung','TOP-Color GmbH','manual.pdf',?)",(len(raw),))
        self.mail.data['INBOX']={1:message()};self.finish()
        self.assertEqual(len(self.rows('SELECT * FROM einkauf_belege')),1)
        self.assertEqual(self.rows('SELECT stored_name FROM assistent_mailquellen_dateien')[0]['stored_name'],'manual.pdf')
        with self.service.db() as db:
            with self.assertRaises(ValueError):self.service._stage(db,raw,'Invoice.pdf','Different Supplier')
    def test_admin_csrf_and_http_status(self):
        service=register_mail_sources(self.p);service.mailbox=self.mail
        client=self.p.app.test_client()
        self.assertEqual(client.post('/admin/assistent-mailquellen/start').status_code,403)
        with client.session_transaction() as sess:sess.update(admin=True,csrf_token='synthetic')
        self.assertEqual(client.post('/admin/assistent-mailquellen/start').status_code,403)
        response=client.post('/admin/assistent-mailquellen/start',headers={'X-CSRF-Token':'synthetic'})
        self.assertEqual(response.status_code,200);self.assertIn('no-store',response.headers['Cache-Control'])
        self.assertEqual(client.get('/admin/assistent-mailquellen').status_code,200)
        self.assertEqual(client.get('/admin/assistent-mailquellen/weiter').status_code,405)
        self.assertEqual(client.post('/admin/assistent-mailquellen/pause',headers={'X-CSRF-Token':'synthetic'}).json['state'],'paused')
    def test_lease_excludes_second_worker_and_expiry_recovers(self):
        self.mail.data['INBOX']={1:message()};self.service.start()
        with self.p.get_db() as db:db.execute("UPDATE assistent_mailquellen_laeufe SET lease='other',lease_until=99999999999")
        report=self.service.step();self.assertTrue(report['busy']);self.assertEqual(self.mail.header_calls,[])
        with self.p.get_db() as db:db.execute('UPDATE assistent_mailquellen_laeufe SET lease_until=0')
        self.assertEqual(self.service.step()['folders'][0]['cursor'],1)
    def test_real_postgres_adapter_schema_import_and_approval(self):
        names={'DbRow','PostgresCursor','PostgresConnection','convert_sqlite_sql_to_postgres','get_insert_table_name','split_sql_script'}
        tree=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_text(encoding='utf-8'))
        nodes=[node for node in tree.body if isinstance(node,(ast.ClassDef,ast.FunctionDef)) and node.name in names]
        namespace={'re':re};exec(compile(ast.Module(body=nodes,type_ignores=[]),'app.py (adapter only)','exec'),namespace)
        statements=[];database_path=self.p.path
        class Cursor:
            def __init__(self,connection):self.cursor=connection.cursor()
            def __enter__(self):return self
            def __exit__(self,*args):self.cursor.close()
            def execute(self,sql,params):
                statements.append(sql);self.cursor.execute(sql.replace('%s','?').replace('SERIAL PRIMARY KEY','INTEGER PRIMARY KEY AUTOINCREMENT'),params)
                self.rows=self.cursor.fetchall() if self.cursor.description else [];self.rowcount=self.cursor.rowcount
                self.description=[type('Column',(),{'name':col[0]}) for col in self.cursor.description] if self.cursor.description else None
            def fetchall(self):return self.rows
        class Connection:
            def __init__(self):self.connection=sqlite3.connect(database_path)
            def cursor(self):return Cursor(self.connection)
            def commit(self):self.connection.commit()
            def rollback(self):self.connection.rollback()
            def close(self):self.connection.close()
        self.mail.data['INBOX']={1:message(sender='Parts <parts@example.test>')}
        with patch.object(self.p,'get_db',side_effect=lambda:namespace['PostgresConnection'](Connection())):
            self.service.init_schema();report=self.finish()
            self.service.approve(report['unknown_senders'][0]['id'],'Synthetic Materials GmbH')
            report=self.finish();self.assertEqual(report['counts']['files'],1)
        self.assertTrue(any(sql.startswith('INSERT INTO app_settings') and sql.endswith('RETURNING key') for sql in statements))
        self.assertTrue(any('lease_until DOUBLE PRECISION' in sql for sql in statements))


if __name__=='__main__':unittest.main(verbosity=2)
