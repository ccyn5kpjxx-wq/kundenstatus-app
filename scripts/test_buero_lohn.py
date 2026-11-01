"""Synthetic payroll approval/publication tests; real private employee API, no bank."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta
import io
from pathlib import Path
import sqlite3
import sys
import tempfile
from threading import RLock
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import fitz
from flask import Flask, abort, send_file, session
from jinja2 import ChoiceLoader, DictLoader
from werkzeug.datastructures import FileStorage, MultiDict

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from werkstatt_mitarbeiter_portal import EmployeePortal
from werkstatt_buero_lohn import register_office_payroll, PayrollConflict, BACKUP_TABLES, ensure_office_payroll_for_import


def pdf(text):
    with fitz.open() as doc:
        doc.new_page().insert_text((40,50),text)
        return doc.tobytes()


class PayrollTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'synthetic.db'
        self.app=Flask(__name__,template_folder=str(ROOT/'templates'),static_folder=str(ROOT/'static'))
        self.app.secret_key='synthetic-payroll-only';self.app.config.update(TESTING=True,ASSISTANT_NATIVE_COCKPIT=True)
        self.app.jinja_loader=ChoiceLoader([DictLoader({'buero_nav.html':'<nav>Büro</nav>'}),self.app.jinja_loader])
        def db():
            c=sqlite3.connect(self.path);c.row_factory=sqlite3.Row;return c
        def office(db=None):
            if not session.get('office'):return None
            return {'actor':'mitarbeiter:1','mitarbeiter_id':1,'office_categories':session.get('categories',[])}
        self.lock=RLock()
        self.p=SimpleNamespace(app=self.app,get_db=db,office_access=SimpleNamespace(identity=office),
            portal_originals_operation_lock=lambda:self.lock,schedule_change_backup=Mock(),now_str=lambda:'2026-11-01T10:00:00')
        with closing(db()) as c:
            c.executescript('''CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,name TEXT,aktiv INTEGER);
                INSERT INTO mitarbeiter VALUES(1,'Office',1),(2,'Same Name',1),(3,'Same Name',1);
                CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,version INTEGER,auth_version INTEGER,
                lesen INTEGER,dokumentieren INTEGER,einkaufen INTEGER,limit_cent INTEGER);
                INSERT INTO assistent_rechte VALUES(1,1,1,1,0,0,0),(2,1,1,1,0,0,0),(3,1,1,1,0,0,0);
                CREATE TABLE assistent_audit(id INTEGER PRIMARY KEY,actor TEXT,auftrag_id INTEGER,aktion TEXT,details TEXT,zeit TEXT);
                CREATE TABLE mitarbeiter_lohnzettel(id INTEGER PRIMARY KEY AUTOINCREMENT,mitarbeiter_id INTEGER,
                period TEXT,filename TEXT,mime TEXT,size_bytes INTEGER,sha256 TEXT,original_base64 TEXT,created_at TEXT,created_by TEXT,
                UNIQUE(mitarbeiter_id,period,sha256));''');c.commit()
        self.p.employee_portal=EmployeePortal(self.p)
        self.service=register_office_payroll(self.p);self.service.init_schema()
        @self.app.get('/werkstatt/mein-konto/lohnzettel/<int:key>')
        def personal(key):
            try:meta,raw=self.p.employee_portal.payroll(key)
            except LookupError:abort(404)
            return send_file(io.BytesIO(raw),mimetype=meta['mime'],as_attachment=True,download_name=meta['filename'])
        self.admin=self.app.test_client();self.office=self.app.test_client();self.worker=self.app.test_client();self.other=self.app.test_client()
        with self.admin.session_transaction() as s:s.update(admin=True,csrf_token='test-csrf')
        with self.office.session_transaction() as s:s.update(office=True,categories=['loehne'],csrf_token='test-csrf',assistent_mid=1,assistent_version=1,assistent_auth_version=1)
        for client,mid in ((self.worker,2),(self.other,3)):
            with client.session_transaction() as s:s.update(assistent_mid=mid,assistent_version=1,assistent_auth_version=1,csrf_token='test-csrf')
        self.summary=pdf('Synthetic overall payroll evaluation')
        self.single=pdf('Synthetic individual payslip: owner ID 2')

    def upload(self,raw,name='original.pdf'):return FileStorage(stream=io.BytesIO(raw),filename=name,content_type='application/pdf')
    def context(self):
        ctx=self.app.test_request_context();ctx.push();session['admin']=True;self.addCleanup(ctx.pop);return ctx
    def version(self,key,item=None):
        with closing(self.p.get_db()) as db:return db.execute('SELECT version FROM '+('buero_lohnzettel' if item else 'buero_lohnlaeufe')+' WHERE id=?',(item or key,)).fetchone()['version']
    def make_run(self):
        with self.app.test_request_context():session['admin']=True;return self.service.create('2026-10',self.upload(self.summary))[0]
    def prepare(self):
        key=self.make_run()
        with self.app.test_request_context():
            session['admin']=True
            item=self.service.add_payslip(key,self.version(key),self.upload(self.single),confirmed_individual=True)[0]
            self.service.assign(key,item,self.version(key,item),2,confirmed=True)
        return key,item
    def approve(self,key):
        with self.app.test_request_context():session['admin']=True;self.service.approve(key,self.version(key),confirmed=True)
    def snapshot(self):
        with closing(self.p.get_db()) as db:
            return {'tables':{t:[dict(r) for r in db.execute('SELECT * FROM '+t).fetchall()] for t in (*BACKUP_TABLES,'mitarbeiter','assistent_rechte')}}

    def test_three_roles_summary_visibility_and_no_personal_leak(self):
        key,item=self.prepare()
        for route in ('/admin/buero/loehne','/admin/buero/loehne/'+key+'/auswertung','/admin/buero/loehne/'+key+'/lohnzettel/'+item):
            self.assertEqual(self.admin.get(route).status_code,200)
            self.assertEqual(self.office.get(route).status_code,403)
            self.assertEqual(self.worker.get(route).status_code,403)
        self.assertEqual(self.office.get('/werkstatt/buero/loehne/'+key+'/auswertung').status_code,403)
        self.assertNotIn(b'2026-10',self.office.get('/werkstatt/buero/loehne').data)
        self.approve(key)
        self.assertEqual(self.office.get('/werkstatt/buero/loehne/'+key+'/auswertung').status_code,200)
        self.assertEqual(self.worker.get('/werkstatt/buero/loehne/'+key+'/auswertung').status_code,403)
        self.assertNotIn(b'original.pdf',self.office.get('/werkstatt/buero/loehne').data)
        self.assertEqual(self.worker.get('/werkstatt/buero/loehne').status_code,403)

    def test_manual_stable_id_assignment_and_private_existing_download(self):
        key,item=self.prepare();self.approve(key)
        with self.app.test_request_context():
            session['admin']=True
            ident=self.service.publish(key,item,self.version(key,item),confirmed=True)
        self.assertEqual(self.worker.get('/werkstatt/mein-konto/lohnzettel/'+str(ident)).data,self.single)
        self.assertEqual(self.other.get('/werkstatt/mein-konto/lohnzettel/'+str(ident)).status_code,404)
        self.assertEqual(self.office.get('/werkstatt/mein-konto/lohnzettel/'+str(ident)).status_code,404)

    def test_summary_cannot_be_used_as_individual_and_confirmation_required(self):
        key=self.make_run()
        with self.app.test_request_context():
            session['admin']=True
            with self.assertRaises(ValueError):self.service.add_payslip(key,self.version(key),self.upload(self.summary),confirmed_individual=True)
            with self.assertRaises(ValueError):self.service.add_payslip(key,self.version(key),self.upload(self.single))
        with closing(self.p.get_db()) as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM mitarbeiter_lohnzettel').fetchone()[0],0)

    def test_publish_requires_batch_approval_and_explicit_checked_assignment(self):
        key,item=self.prepare()
        with self.app.test_request_context():
            session['admin']=True
            with self.assertRaises(ValueError):self.service.publish(key,item,self.version(key,item),confirmed=True)
            with self.assertRaises(ValueError):self.service.assign(key,item,self.version(key,item),3)
        self.approve(key)
        with self.app.test_request_context():
            session['admin']=True
            with self.assertRaises(ValueError):self.service.publish(key,item,self.version(key,item))

    def test_interrupted_publication_is_idempotent_and_owner_cannot_change(self):
        key,item=self.prepare();self.approve(key)
        original=self.p.employee_portal.upload_payroll
        def fail_after_insert(*args):original(*args);raise RuntimeError('synthetic interruption')
        with self.app.test_request_context():
            session['admin']=True
            with patch.object(self.p.employee_portal,'upload_payroll',side_effect=fail_after_insert):
                with self.assertRaises(RuntimeError):self.service.publish(key,item,self.version(key,item),confirmed=True)
            with self.assertRaises(ValueError):self.service.assign(key,item,self.version(key,item),3,confirmed=True)
            ident=self.service.publish(key,item,self.version(key,item),confirmed=True)
            self.assertEqual(self.service.publish(key,item,self.version(key,item),confirmed=True),ident)
        with closing(self.p.get_db()) as db:
            rows=db.execute('SELECT mitarbeiter_id FROM mitarbeiter_lohnzettel').fetchall();self.assertEqual([r[0] for r in rows],[2])

    def test_csrf_and_stale_approval_and_no_office_approval(self):
        key=self.make_run();v=self.version(key)
        form=dict(action='approve',key=key,version=str(v),confirmed='1')
        self.assertEqual(self.admin.post('/admin/buero/loehne',data=form).status_code,400)
        form['csrf_token']='test-csrf'
        self.assertEqual(self.office.post('/admin/buero/loehne',data=form).status_code,403)
        self.assertEqual(self.admin.post('/admin/buero/loehne',data=form).status_code,303)
        self.assertEqual(self.admin.post('/admin/buero/loehne',data=form).status_code,409)

    def test_identical_csrf_copies_allow_approval_and_payment_without_ambiguous_business_fields(self):
        key=self.make_run()
        form=MultiDict(dict(action='approve',key=key,version=str(self.version(key)),confirmed='1'))
        form.setlist('csrf_token',['test-csrf','wrong'])
        self.assertEqual(self.admin.post('/admin/buero/loehne',data=form).status_code,400)
        form.setlist('csrf_token',['test-csrf','test-csrf'])
        self.assertEqual(self.admin.post('/admin/buero/loehne',data=form).status_code,303)
        form=MultiDict(dict(action='payment',key=key,version=str(self.version(key)),confirmed='1',target='zahlung_in_bearbeitung'))
        form.setlist('csrf_token',['test-csrf','test-csrf'])
        form.add('target','dokumentiert')
        self.assertEqual(self.office.post('/werkstatt/buero/loehne',data=form).status_code,400)
        form.setlist('target',['zahlung_in_bearbeitung'])
        self.assertEqual(self.office.post('/werkstatt/buero/loehne',data=form).status_code,303)
        with self.office.session_transaction() as state:self.assertFalse(state.get('admin'))

    def test_pending_payment_count_supports_chef_preview_and_still_requires_explicit_office_grant(self):
        key=self.make_run()
        with self.app.test_request_context():
            session['admin']=True
            self.assertEqual(self.service.pending_payment_count(),0)
        self.approve(key)
        with self.app.test_request_context():
            session['admin']=True
            self.assertEqual(self.service.pending_payment_count(),1)
            self.assertEqual(self.service.pending_payment_count(admin=True),1)
        with self.app.test_request_context():
            session.update(office=True,categories=['loehne'])
            self.assertEqual(self.service.pending_payment_count(),1)
            self.assertEqual(self.service.pending_payment_count(admin=False),1)
            with self.assertRaises(PermissionError):self.service.pending_payment_count(admin=True)
            self.assertFalse(session.get('admin'))
            session['categories']=[]
            with self.assertRaises(PermissionError):self.service.pending_payment_count()
            session.clear()
            with self.assertRaises(PermissionError):self.service.pending_payment_count()

    def test_future_payment_date_preserves_open_task_revision_and_audit(self):
        key=self.make_run();self.approve(key)
        today=datetime.now(ZoneInfo('Europe/Berlin')).date()
        with self.app.test_request_context():
            session.update(office=True,categories=['loehne'])
            self.service.payment(key,self.version(key),'zahlung_in_bearbeitung',confirmed=True)
            before=self.snapshot();version=self.version(key)
            with self.assertRaisesRegex(ValueError,'Zukunft'):
                self.service.payment(key,version,'dokumentiert',confirmed=True,
                                     paid_date=(today+timedelta(days=1)).isoformat(),reference='Extern geprüft')
            self.assertEqual(self.snapshot(),before)
            self.assertEqual(self.service.pending_payment_count(),1)
            self.service.payment(key,version,'dokumentiert',confirmed=True,paid_date=today.isoformat(),reference='Extern geprüft')
            self.assertEqual(self.service.pending_payment_count(),0)

    def test_payment_task_exists_only_after_approval_and_documents_external_action(self):
        key=self.make_run()
        with self.app.test_request_context():
            session.update(office=True,categories=['loehne'])
            self.assertEqual(self.service.pending_payment_count(),0)
            with self.assertRaises(ValueError):self.service.payment(key,self.version(key),'zahlung_in_bearbeitung',confirmed=True)
        self.approve(key)
        with self.app.test_request_context():
            session.update(office=True,categories=['loehne'])
            self.assertEqual(self.service.pending_payment_count(),1)
            with self.assertRaises(ValueError):self.service.payment(key,self.version(key),'dokumentiert',confirmed=True,paid_date='2026-11-01',reference='Bank export checked')
            self.service.payment(key,self.version(key),'zahlung_in_bearbeitung',confirmed=True)
            with self.assertRaises(ValueError):self.service.payment(key,self.version(key),'dokumentiert',confirmed=True)
            self.service.payment(key,self.version(key),'dokumentiert',confirmed=True,paid_date='2026-11-01',reference='Extern geprüft')
            self.assertEqual(self.service.pending_payment_count(),0)

    def test_original_corruption_fails_closed_and_restores_preserve_owner(self):
        key,item=self.prepare();snapshot=self.snapshot()
        ensure_office_payroll_for_import(self.p,export=snapshot)
        altered=deepcopy(snapshot);altered['tables']['mitarbeiter'][1]['name']='Different owner'
        with self.assertRaises(ValueError):ensure_office_payroll_for_import(self.p,export=altered)
        with self.assertRaises(ValueError):ensure_office_payroll_for_import(self.p,export={'tables':{}})
        with closing(self.p.get_db()) as db:db.execute("UPDATE buero_lohnlaeufe SET original_base64='broken' WHERE id=?",(key,));db.commit()
        self.assertEqual(self.admin.get('/admin/buero/loehne/'+key+'/auswertung').status_code,404)

    def test_duplicate_month_original_and_unassigned_staff_have_no_access(self):
        key=self.make_run()
        with self.app.test_request_context():
            session['admin']=True
            self.assertEqual(self.service.create('2026-10',self.upload(self.summary)),(key,True))
            with self.assertRaises(ValueError):self.service.create('2026-11',self.upload(self.summary))
        with self.office.session_transaction() as value:value['categories']=[]
        self.assertEqual(self.office.get('/werkstatt/buero/loehne').status_code,403)


if __name__=='__main__':main()
