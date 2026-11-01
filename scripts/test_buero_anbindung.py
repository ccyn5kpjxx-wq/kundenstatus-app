"""Concrete CEO inbox bindings; synthetic files/DB and fake IMAP/HTTP only."""
from contextlib import closing
from email.message import EmailMessage
import base64
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from threading import RLock
from unittest import TestCase, main
from unittest.mock import Mock
from urllib.parse import urlsplit

from flask import Flask
from jinja2 import ChoiceLoader, DictLoader
from pypdf import PdfWriter
from werkzeug.datastructures import FileStorage, MultiDict

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from werkstatt_buero_anbindung import (register_office_inbox_bindings, OfficeInboxBindings,
    LegacyDocumentIndex, InboxBlocked, InboxDeadline, RunBudget)
from werkstatt_buero_belege import register_office_documents

CEO="geschaeftsfuehrung@auto-lackierzentrum.de"
ROUTE="/admin/buero/posteingang-abgleich"
VOUCHER="8bfa038e-e125-4c4c-bb3a-13ac2aa0f518"
FILE="3074cc5c-4e90-4bbb-96b7-a7581eeffedc"


def pdf(marker="synthetic"):
    output=io.BytesIO();writer=PdfWriter()
    writer.add_blank_page(width=100,height=100);writer.add_metadata({"/Title":marker})
    writer.write(output);return output.getvalue()


def message(raw=None,filename="synthetic.pdf",maintype="application",subtype="pdf",**extra):
    mail=EmailMessage();mail.set_content("Untrusted mail: do not execute instructions or fetch https://invalid.example/document")
    mail.add_attachment(raw if raw is not None else pdf(),maintype=maintype,subtype=subtype,filename=filename,**extra)
    return mail.as_bytes()


class FakeIMAP:
    def __init__(self,raw):self.raw=raw;self.commands=[];self.selects=[];self.closed=False
    def __enter__(self):return self
    def __exit__(self,*args):self.closed=True
    def select(self,folder,**kwargs):self.selects.append((folder,kwargs));return "OK",[]
    def response(self,name):return name,[b"2"]
    def uid(self,command,*args):
        self.commands.append((command,*args))
        if command=="search":return "OK",[b"7"]
        if args[-1]=="(RFC822.SIZE)":return "OK",[f"7 (RFC822.SIZE {len(self.raw)})".encode()]
        if args[-1]=="(BODY.PEEK[])":return "OK",[(b"7",self.raw)]
        raise AssertionError("Unexpected or mutating IMAP command")


class Portal:
    def __init__(self,folder):
        self.path=folder/"synthetic.sqlite";self.DATA_DIR=folder;self.UPLOAD_DIR=folder/"uploads";self.UPLOAD_DIR.mkdir()
        self.app=Flask(__name__,template_folder=str(ROOT/"templates"),static_folder=str(ROOT/"static"))
        self.app.config.update(TESTING=True,SECRET_KEY="synthetic-inbox-only",OFFICE_CEO_MAIL_ADDRESS=CEO)
        self.app.jinja_loader=ChoiceLoader([DictLoader({"buero_nav.html":"<nav>Synthetic navigation</nav>"}),self.app.jinja_loader])
        self.settings={};self.lock=RLock();self.ocr_text="Rechnung\nSynthetic unverified text"
        self.config={"configured":True,"user":CEO,"password":"synthetic-imap-secret","host":"imap.example.invalid",
                     "port":993,"ssl":True,"folder":"INBOX"}
        self.LEXWARE_API_BASE_URL="https://api.lexware.io";self.LEXWARE_API_KEY="synthetic-lexware-secret"
        self.raw=message();self.connection=FakeIMAP(self.raw)
        self.office_inbox_connection_factory=Mock(side_effect=lambda cfg:self.connection)
        self.http=Mock();self.http.request.side_effect=self.http_request;self.file_response=None
        self.get_requests=Mock(return_value=self.http)
        self.lexware_request=Mock(side_effect=self.lexware)
        self.extract_document_text_local=Mock(side_effect=lambda path,name:self.ocr_text)
        with closing(self.get_db()) as db:
            db.executescript("""CREATE TABLE einkauf_belege(id INTEGER PRIMARY KEY,stored_name TEXT);
                CREATE TABLE dateien(id INTEGER PRIMARY KEY,stored_name TEXT);
                CREATE TABLE datei_backups(datei_id INTEGER PRIMARY KEY,file_base64 TEXT,file_sha256 TEXT,size INTEGER);""")
            db.commit()
        register_office_documents(self).init_schema()
    def get_db(self):
        db=sqlite3.connect(self.path);db.row_factory=sqlite3.Row;return db
    def portal_originals_operation_lock(self):return self.lock
    def get_app_setting(self,key,default):return self.settings.get(key,default)
    def get_werkstatt_imap_config(self):return dict(self.config)
    def get_table_columns(self,db,table):return [r["name"] for r in db.execute("PRAGMA table_info("+table+")")]
    def load_datei_backup_bytes(self,row):
        with closing(self.get_db()) as db:
            data=db.execute("SELECT file_base64 FROM datei_backups WHERE datei_id=?",(row["id"],)).fetchone()
        return base64.b64decode(data["file_base64"],validate=True) if data else None
    def lexware(self,method,path,query=""):
        if method!="GET":raise AssertionError("Lexware writes forbidden")
        if path=="/v1/voucherlist":return {"last":True,"content":[]}
        raise AssertionError("Unexpected metadata request")
    def http_request(self,method,url,**kwargs):
        parsed=urlsplit(url)
        if parsed.path.startswith('/v1/files/'):
            if self.file_response is None:raise AssertionError('Unexpected file request')
            return self.file_response
        value=self.lexware_request(method,parsed.path,query=('?'+parsed.query if parsed.query else ''))
        raw=json.dumps(value).encode()
        response=Mock(status_code=200,headers={'Content-Length':str(len(raw))})
        response.iter_content.return_value=[raw]
        return response


class InboxBindingsTests(TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.p=Portal(Path(self.tmp.name));self.service=register_office_inbox_bindings(self.p)
        self.service.pipeline.init_schema();self.admin=self.p.app.test_client();self.office=self.p.app.test_client()
        with self.admin.session_transaction() as state:state.update(admin=True,csrf_token="synthetic-csrf")
        with self.office.session_transaction() as state:state.update(assistent_mid=7,csrf_token="synthetic-csrf")
    def fields(self,**changes):
        data=dict(csrf_token="synthetic-csrf",action="run",confirmed="1",lookback_days="14");data.update(changes);return data
    def run_chef(self):
        with self.p.app.test_request_context():
            from flask import session
            session["admin"]=True
            return self.service.run()
    def rows(self,table="buero_belege"):
        with closing(self.p.get_db()) as db:return [dict(row) for row in db.execute("SELECT * FROM "+table)]
    def set_message(self,raw):self.connection=FakeIMAP(raw);self.p.connection=self.connection
    @property
    def connection(self):return self.p.connection
    @connection.setter
    def connection(self,value):self.p.connection=value
    def test_office_and_anonymous_cannot_read_or_start_chef_inbox(self):
        for client in (self.office,self.p.app.test_client()):
            self.assertEqual(client.get(ROUTE).status_code,403)
            self.assertEqual(client.post(ROUTE,data=self.fields()).status_code,403)
        with self.assertRaises(PermissionError):self.service.run()
        self.p.office_inbox_connection_factory.assert_not_called();self.p.lexware_request.assert_not_called()
    def test_get_is_inert_private_and_contains_no_credentials(self):
        response=self.admin.get(ROUTE)
        self.assertEqual(response.status_code,200)
        self.assertIn(CEO,response.get_data(as_text=True))
        self.assertNotIn(self.p.config["password"],response.get_data(as_text=True))
        self.assertNotIn(self.p.LEXWARE_API_KEY,response.get_data(as_text=True))
        self.assertIn("no-store",response.headers["Cache-Control"])
        self.p.office_inbox_connection_factory.assert_not_called()
    def test_start_requires_unique_csrf_action_and_human_confirmation(self):
        cases=[self.fields(csrf_token="wrong"),self.fields(confirmed=""),self.fields(action="other"),
               MultiDict([*self.fields().items(),("csrf_token","wrong")]),
               MultiDict([*self.fields().items(),("lookback_days","14")]),
               MultiDict([*self.fields().items(),("action","run")])]
        for fields in cases:self.assertEqual(self.admin.post(ROUTE,data=fields).status_code,400)
        self.p.office_inbox_connection_factory.assert_not_called()
    def test_matching_central_and_page_csrf_tokens_are_accepted(self):
        data=MultiDict([*self.fields().items(),("csrf_token","synthetic-csrf")])
        response=self.admin.post(ROUTE,data=data)
        self.assertEqual(response.status_code,200,response.get_data(as_text=True))
        self.assertEqual(len(self.rows()),1)
    def test_account_mismatch_or_missing_setting_blocks_without_secret_disclosure(self):
        self.p.config["user"]="different@example.invalid"
        response=self.admin.post(ROUTE,data=self.fields())
        self.assertEqual(response.status_code,400)
        self.assertIn("nicht eindeutig gleich",response.get_data(as_text=True))
        self.assertNotIn("different@example.invalid",response.get_data(as_text=True))
        self.p.office_inbox_connection_factory.assert_not_called()
        self.p.app.config["OFFICE_CEO_MAIL_ADDRESS"]=""
        with self.assertRaises(InboxBlocked):self.run_chef()
    def test_inactive_sources_or_unencrypted_or_other_folder_block(self):
        for key,value in (("configured",False),("ssl",False),("folder","Archive"),("password","")):
            original=self.p.config[key];self.p.config[key]=value
            with self.assertRaises(InboxBlocked):self.run_chef()
            self.p.config[key]=original
        self.p.LEXWARE_API_KEY=""
        with self.assertRaises(InboxBlocked):self.run_chef()
        self.p.office_inbox_connection_factory.assert_not_called()
    def test_end_to_end_preserves_original_only_intake_and_local_unverified_suggestion(self):
        response=self.admin.post(ROUTE,data=self.fields())
        self.assertEqual(response.status_code,200,response.get_data(as_text=True))
        rows=self.rows();self.assertEqual(len(rows),1);row=rows[0]
        self.assertEqual((row["status"],row["bereich"],row["payable"],row["business_confirmed"]),("eingereicht","unklar",0,0))
        self.assertEqual((row["lieferant"],row["belegnummer"],row["betrag_cent"]),("","",None))
        self.assertEqual(row["created_by"],"system:chef-posteingang")
        self.assertEqual(base64.b64decode(row["original_base64"]),pdf())
        self.assertEqual(row["kategorie_vorschlag"],"rechnung")
        self.assertEqual(self.admin.post(ROUTE,data=self.fields()).status_code,200)
        self.assertEqual(len(self.rows()),1)
        self.assertEqual(self.connection.selects,[("INBOX",{"readonly":True})]*2)
        self.assertTrue(all(c[0] in {"search","fetch"} for c in self.connection.commands))
        commands=str(self.connection.commands);self.assertIn("BODY.PEEK[]",commands);self.assertNotIn("UNSEEN",commands)
        self.assertTrue(all(c.args[0]=="GET" for c in self.p.lexware_request.call_args_list))
        self.assertTrue(all(c.args[0]=="GET" for c in self.p.http.request.call_args_list))
        self.assertFalse(any("/v1/files/" in c.args[1] for c in self.p.http.request.call_args_list))
    def test_explicit_host_chef_job_works_without_request_session(self):
        service=OfficeInboxBindings(self.p,authorize=lambda:True)
        report=service.run()
        self.assertEqual(report["results"][0]["status"],"PORTAL_NEU_CHEFPRUEFUNG")
        self.assertEqual(self.rows()[0]["payable"],0)
    def test_missing_legacy_table_blocks_new_copy_and_reports_exact_gap(self):
        with closing(self.p.get_db()) as db:db.execute("DROP TABLE einkauf_belege");db.commit()
        report=self.run_chef()
        self.assertEqual(report["results"][0]["status"],"BESTANDSABGLEICH_UNVOLLSTAENDIG_KEINE_ANLAGE")
        self.assertFalse(report["legacy_complete"]);self.assertFalse(self.rows())
        self.assertTrue(any("einkauf_belege" in issue for issue in report["source_issues"]))
    def test_existing_old_purchase_scan_is_reused_without_new_intake(self):
        (self.p.UPLOAD_DIR/"old.pdf").write_bytes(pdf())
        with closing(self.p.get_db()) as db:db.execute("INSERT INTO einkauf_belege VALUES(1,'old.pdf')");db.commit()
        report=self.run_chef()
        self.assertTrue(report["legacy_complete"])
        self.assertEqual(report["results"][0]["status"],"PORTAL_BESTEHT_WORKFLOW_FORTSETZEN")
        self.assertFalse(self.rows())
    def test_database_original_counts_even_when_upload_file_is_absent(self):
        raw=pdf()
        with closing(self.p.get_db()) as db:
            db.execute("INSERT INTO dateien VALUES(1,'missing.pdf')")
            db.execute("INSERT INTO datei_backups VALUES(?,?,?,?)",(1,base64.b64encode(raw).decode(),hashlib.sha256(raw).hexdigest(),len(raw)));db.commit()
        report=self.run_chef()
        self.assertTrue(report["legacy_complete"]);self.assertFalse(self.rows())
        self.assertEqual(report["results"][0]["portal_key"],"datei:1")
    def test_missing_or_unsafe_legacy_original_prevents_false_complete(self):
        for filename in ("missing.pdf","../outside.pdf","C:\\outside.pdf"):
            with closing(self.p.get_db()) as db:
                db.execute("DELETE FROM einkauf_belege");db.execute("INSERT INTO einkauf_belege VALUES(1,?)",(filename,));db.commit()
            index=LegacyDocumentIndex(self.p);result=index.find("a"*64,{})
            self.assertFalse(result["complete"]);self.assertTrue(index.issues)
        self.assertFalse(self.rows())
    def test_different_old_scan_without_verified_identity_blocks_new_copy(self):
        (self.p.UPLOAD_DIR/"scan.pdf").write_bytes(pdf("Margitta scan"))
        with closing(self.p.get_db()) as db:db.execute("INSERT INTO einkauf_belege VALUES(1,'scan.pdf')");db.commit()
        report=self.run_chef()
        self.assertFalse(report["legacy_complete"]);self.assertFalse(self.rows())
        self.assertEqual(report["results"][0]["status"],"BESTANDSABGLEICH_UNVOLLSTAENDIG_KEINE_ANLAGE")
        self.assertTrue(any("anderer Scan" in note for note in report["source_issues"]))
    def test_different_existing_portal_original_without_verified_identity_blocks_new_copy(self):
        self.p.office_documents.create_from_intake({"typ":"post","kategorie":"sonstige"},
            FileStorage(stream=io.BytesIO(pdf("Existing portal scan")),filename="scan.pdf"),source_ref="synthetic")
        report=self.run_chef()
        self.assertEqual(report["results"][0]["status"],"BESTANDSABGLEICH_UNVOLLSTAENDIG_KEINE_ANLAGE")
        self.assertEqual(len(self.rows()),1)
    def test_corrupt_backup_or_unknown_upload_format_is_reported_as_gap(self):
        (self.p.UPLOAD_DIR/"valid.pdf").write_bytes(pdf())
        (self.p.UPLOAD_DIR/"unsupported.zip").write_bytes(b"PK-synthetic")
        with closing(self.p.get_db()) as db:
            db.execute("INSERT INTO dateien VALUES(1,'valid.pdf')")
            db.execute("INSERT INTO datei_backups VALUES(1,'invalid','invalid',-1)");db.commit()
        index=LegacyDocumentIndex(self.p);result=index.find("a"*64,{})
        self.assertFalse(result["complete"])
        self.assertTrue(any("Datenbankoriginal" in issue for issue in index.issues))
        self.assertTrue(any("Dokumentformat" in issue for issue in index.issues))
    def test_hash_index_limit_and_nested_folder_are_explicit_gaps(self):
        (self.p.UPLOAD_DIR/"nested").mkdir()
        self.assertFalse(LegacyDocumentIndex(self.p).find("a"*64,{})["complete"])
        (self.p.UPLOAD_DIR/"one.pdf").write_bytes(pdf())
        self.assertFalse(LegacyDocumentIndex(self.p,max_files=1).find("a"*64,{})["complete"])
    def test_unsupported_and_wrong_mime_attachments_are_visible_not_silently_ignored(self):
        for data in (message(b"zip",filename="document.zip",subtype="zip"),message(filename="document.pdf",maintype="text",subtype="plain")):
            self.set_message(data);report=self.run_chef()
            self.assertEqual(report["attachment_count"],0);self.assertTrue(report["source_issues"])
        self.assertFalse(self.rows())
    def test_local_document_validation_precedes_ocr(self):
        self.set_message(message(b"%PDF-this-is-not-a-valid-document"))
        response=self.admin.post(ROUTE,data=self.fields())
        self.assertEqual(response.status_code,400)
        self.p.extract_document_text_local.assert_not_called();self.p.lexware_request.assert_not_called()
        self.assertFalse(self.rows())
    def test_private_hint_stays_unclear_and_never_queries_company_lexware(self):
        self.p.ocr_text="Privatrechnung\nNur privat. Einkommensteuer"
        report=self.run_chef()
        self.p.lexware_request.assert_not_called()
        self.assertEqual(self.rows()[0]["bereich"],"unklar")
        self.assertEqual(self.rows()[0]["business_confirmed"],0)
        self.assertEqual(report["bank_actions"],0)
    def test_existing_private_portal_attachment_is_never_looked_up_in_lexware(self):
        self.p.office_documents.create_from_intake({"typ":"post","kategorie":"privat"},
            FileStorage(stream=io.BytesIO(pdf()),filename="private.pdf"),source_ref="synthetic",bereich="privat")
        report=self.run_chef()
        self.assertEqual(report["results"][0]["status"],"PRIVAT_BEREITS_IM_PORTAL")
        self.p.lexware_request.assert_not_called()
    def test_existing_lexware_file_uses_get_only_and_preserves_link_proposal(self):
        def api(method,path,query=""):
            self.assertEqual(method,"GET")
            if path=="/v1/voucherlist":return {"last":True,"content":[] if "archived=true" in query else [{"id":VOUCHER,"voucherType":"purchaseinvoice","totalAmount":119}]}
            self.assertEqual(path,"/v1/vouchers/"+VOUCHER);return {"type":"purchaseinvoice","files":[FILE]}
        self.p.lexware_request.side_effect=api
        response=Mock(status_code=200,headers={"Content-Length":str(len(pdf()))})
        response.iter_content.return_value=[pdf()];self.p.file_response=response
        report=self.run_chef()
        self.assertEqual(report["results"][0]["status"],"LEXWARE_BESTEHT_NUR_ZUORDNUNG_PRUEFEN")
        call=self.p.http.request.call_args
        self.assertEqual(call.args,("GET","https://api.lexware.io/v1/files/"+FILE))
        self.assertFalse(call.kwargs["allow_redirects"]);response.close.assert_called_once()
        self.assertEqual(self.rows()[0]["lexware_beleg_id"],"")
    def test_download_limit_and_arbitrary_file_id_block_before_unsafe_access(self):
        response=Mock(status_code=200,headers={"Content-Length":"100"});self.p.file_response=response
        with self.assertRaises(InboxBlocked):self.service._download(FILE,max_bytes=10)
        response.close.assert_called_once()
        self.p.http.request.reset_mock()
        with self.assertRaises(ValueError):self.service._download("https://evil.invalid/file",max_bytes=10)
        self.p.http.request.assert_not_called()
    def test_source_failure_cannot_expose_exception_credentials(self):
        self.p.office_inbox_connection_factory.side_effect=RuntimeError("synthetic-imap-secret synthetic-lexware-secret")
        response=self.admin.post(ROUTE,data=self.fields())
        self.assertEqual(response.status_code,400)
        text=response.get_data(as_text=True)
        self.assertNotIn(self.p.config["password"],text);self.assertNotIn(self.p.LEXWARE_API_KEY,text)
        self.assertIn("Quelle ist nicht verfügbar",text)
    def test_deadline_after_imap_size_stops_before_body_fetch_or_other_work(self):
        clock=[0.0];self.service.clock=lambda:clock[0]
        original=self.connection.uid
        def command(*args):
            result=original(*args)
            if args[-1]=="(RFC822.SIZE)":clock[0]=60.0
            return result
        self.connection.uid=command
        with self.assertRaises(InboxDeadline):self.run_chef()
        self.assertFalse(any("BODY.PEEK" in str(c) for c in self.connection.commands))
        self.p.http.request.assert_not_called();self.p.extract_document_text_local.assert_not_called()
        self.assertFalse(self.rows());self.assertTrue(self.connection.closed)
    def test_deadline_after_ocr_stops_before_inventory_or_intake(self):
        clock=[0.0];self.service.clock=lambda:clock[0]
        def extract(*args):clock[0]=61.0;return "Rechnung"
        self.p.extract_document_text_local.side_effect=extract
        with self.assertRaises(InboxDeadline):self.run_chef()
        self.p.http.request.assert_not_called();self.assertFalse(self.rows())
        self.assertTrue(any("Zeitbudget" in text for text in self.service.last_source_issues))
    def test_deadline_preserves_receipt_and_first_intake_but_blocks_second_attachment(self):
        mail=EmailMessage();mail.set_content("Synthetic only")
        for marker in ("first","second"):
            mail.add_attachment(pdf(marker),maintype="application",subtype="pdf",filename=marker+".pdf")
        self.set_message(mail.as_bytes())
        clock=[0.0];self.service.clock=lambda:clock[0]
        original=self.service.pipeline._record
        def record(*args):original(*args);clock[0]=61.0
        self.service.pipeline._record=record
        with self.assertRaises(InboxDeadline):self.run_chef()
        self.assertEqual(len(self.rows()),1)
        self.assertEqual(len(self.rows("buero_posteingang_receipts")),1)
        self.assertEqual(len(self.rows("buero_posteingang_audit")),1)
        self.assertEqual(self.p.extract_document_text_local.call_count,1)
        self.assertEqual(self.p.http.request.call_count,2)
        self.assertTrue(self.connection.closed)
    def test_deadline_closes_stream_without_another_chunk_or_retry(self):
        clock=[0.0];budget=RunBudget(lambda:clock[0]);read=[]
        def chunks():
            read.append(1);clock[0]=61.0;yield b"first"
            read.append(2);yield b"second"
        response=Mock(status_code=200,headers={})
        response.iter_content.side_effect=lambda *args:chunks();self.p.file_response=response
        with self.assertRaises(InboxDeadline):self.service._download(FILE,max_bytes=100,budget=budget)
        self.assertEqual(read,[1]);self.assertEqual(self.p.http.request.call_count,1)
        response.close.assert_called_once()
        before=self.p.http.request.call_count
        with self.assertRaises(InboxDeadline):self.service._download(FILE,max_bytes=100,budget=budget)
        self.assertEqual(self.p.http.request.call_count,before)
    def test_lexware_throttling_is_not_retried_and_deadline_hint_is_concrete(self):
        clock=[0.0];self.service.clock=lambda:clock[0]
        response=Mock(status_code=429,headers={"Retry-After":"8"})
        def throttled(*args,**kwargs):clock[0]=61.0;return response
        self.p.http.request.side_effect=throttled
        result=self.admin.post(ROUTE,data=self.fields())
        self.assertEqual(result.status_code,400)
        self.assertIn("Zeitbudget von 60 Sekunden",result.get_data(as_text=True))
        self.assertIn("Abgleichsnachweise bleiben erhalten",result.get_data(as_text=True))
        self.assertEqual(self.p.http.request.call_count,1)
        self.assertFalse(self.rows());response.close.assert_called_once()


if __name__=="__main__":main()
