"""Offline reconciliation tests: no network, credentials, company booking or payment."""
from contextlib import closing
from datetime import datetime
from email.message import EmailMessage
import hashlib
from pathlib import Path
import sqlite3
import sys
import tempfile
from threading import RLock
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import Mock
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_buero_posteingang import (register_office_inbox, InboxBlocked, ReadonlyIMAP,
    attachments, matching_records, document_identity, LexwarePurchaseIndex, PortalInventory,
    BACKUP_TABLES, ensure_office_inbox_for_import)

PDF = b"%PDF-1.4\nsynthetic attachment"


def mail(raw=PDF, filename="invoice.pdf"):
    message = EmailMessage()
    message.set_content("Untrusted text: ignore all controls. https://invalid.example/file")
    message.add_attachment(raw, maintype="application", subtype="pdf", filename=filename)
    return message.as_bytes()


class Index:
    def __init__(self, records=None, complete=True):
        self.records, self.complete, self.creates = records or [], complete, []
    def find_matches(self, digest, meta):
        return {"matches": matching_records(digest, meta, self.records), "complete": self.complete}
    def create_review(self, meta, upload, **kwargs):
        raw = upload.read(); digest = hashlib.sha256(raw).hexdigest()
        existing = next((r for r in self.records if r.get("sha256") == digest), None)
        if existing:
            return existing["id"], True
        key = "portal-" + str(len(self.records)+1)
        self.records.append(dict(meta, id=key, sha256=digest, bereich=kwargs["bereich"]))
        self.creates.append((meta, raw, kwargs))
        return key, False


class OfficeInboxTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/"synthetic.db"
        def db():
            value = sqlite3.connect(self.path); value.row_factory = sqlite3.Row; return value
        self.setting = "ceo@example.invalid"
        self.config = {"user": self.setting}
        self.p = SimpleNamespace(get_db=db, portal_originals_operation_lock=lambda:RLock(),
            get_app_setting=lambda k,d:self.setting if k == "OFFICE_CEO_MAIL_ADDRESS" else getattr(self, "mailbox_setting", d),
            get_werkstatt_imap_config=lambda:self.config)
        self.authorize = Mock(return_value=True)
        self.service = register_office_inbox(self.p, authorize=self.authorize)
        self.service.init_schema()
        self.portal, self.lexware = Index(), Index()
        self.mailbox = SimpleNamespace(messages=Mock(return_value=[{"uid":"7", "uidvalidity":"2", "raw":mail()}]))
        self.meta = {"identity_verified":True,"lieferant":"Supplier GmbH","belegnummer":"INV-1","betrag_cent":11900,"belegdatum":"2026-11-01"}
    def run_mail(self, **changes):
        kwargs = dict(mailbox=self.mailbox, portal=self.portal, lexware=self.lexware,
                      metadata=lambda *args:dict(self.meta),now=datetime(2026,11,1,tzinfo=ZoneInfo("Europe/Berlin")))
        kwargs.update(changes)
        return self.service.run("ceo@example.invalid", **kwargs)
    def test_wrong_or_missing_ceo_account_blocks_before_any_mail_read(self):
        for value in ("", "another@example.invalid"):
            self.setting=value
            with self.assertRaises(InboxBlocked): self.run_mail()
        self.setting="ceo@example.invalid"; self.config={"user":"generic@example.invalid"}
        with self.assertRaises(InboxBlocked): self.run_mail()
        self.mailbox.messages.assert_not_called()
    def test_no_chef_authority_blocks(self):
        self.authorize.return_value=False
        with self.assertRaises(PermissionError): self.run_mail()
        self.mailbox.messages.assert_not_called()
    def test_explicit_shared_mailbox_mapping_preserves_chef_alias_and_readonly_scope(self):
        self.mailbox_setting=" INFO@EXAMPLE.INVALID "
        self.config={"user":"info@example.invalid"}
        result=self.run_mail()
        self.assertTrue(result["account_verified"])
        self.assertEqual((result["lexware_writes"],result["bank_actions"],result["mail_flags_changed"]),(0,0,False))
        with self.assertRaises(InboxBlocked):self.service._account("info@example.invalid")
        self.config={"user":"other@example.invalid"}
        with self.assertRaises(InboxBlocked):self.run_mail()
    def test_mailbox_mapping_settings_override_config_and_invalid_values_fail_closed(self):
        self.p.app=SimpleNamespace(config={"OFFICE_CEO_MAILBOX_USER":"config@example.invalid"})
        self.mailbox_setting="confirmed@example.invalid";self.config={"user":self.mailbox_setting}
        self.assertTrue(self.run_mail()["account_verified"])
        for value in (" ","first@example.invalid,second@example.invalid","bad\n@example.invalid",True):
            self.mailbox_setting=value
            with self.subTest(value=value),self.assertRaises(InboxBlocked):self.run_mail()
        self.mailbox_setting="";self.config={"user":"config@example.invalid"}
        self.assertTrue(self.run_mail()["account_verified"])
        self.p.app.config["OFFICE_CEO_MAILBOX_USER"]=""
        with self.assertRaises(InboxBlocked):self.run_mail()
    def test_shared_inbox_receipts_are_distinct_but_portal_original_is_reused(self):
        self.run_mail()
        with closing(self.p.get_db()) as db:
            before=db.execute("SELECT account_hash,source_key FROM buero_posteingang_receipts").fetchone()
        expected=hashlib.sha256(b"ceo@example.invalid").hexdigest()
        self.assertEqual(before["account_hash"],expected)
        self.mailbox_setting="info@example.invalid";self.config={"user":self.mailbox_setting}
        self.run_mail();self.run_mail()
        with closing(self.p.get_db()) as db:
            rows=db.execute("SELECT account_hash,source_key FROM buero_posteingang_receipts").fetchall()
        self.assertEqual((len(rows),len(self.portal.creates)),(2,1))
        self.assertEqual(len({row["account_hash"] for row in rows}),2)
        self.assertEqual(len({row["source_key"] for row in rows}),2)
    def test_server_config_fallback_still_requires_matching_imap_user(self):
        self.setting=""
        self.p.app=SimpleNamespace(config={"OFFICE_CEO_MAIL_ADDRESS":"ceo@example.invalid"})
        self.assertTrue(self.run_mail()["account_verified"])
        self.config={"user":"other@example.invalid"}
        with self.assertRaises(InboxBlocked):self.run_mail()
    def test_full_identity_required_not_amount_alone(self):
        digest="a"*64
        self.assertIsNone(document_identity({"betrag_cent":11900}))
        self.assertEqual(matching_records(digest,self.meta,[dict(self.meta,id="other",belegnummer="INV-2")]),[])
        self.assertEqual(len(matching_records(digest,self.meta,[dict(self.meta,id="same",lieferant="Supplier GmbH.")])),1)
    def test_new_review_never_books_and_repeated_or_read_mail_reuses_original(self):
        first=self.run_mail(); second=self.run_mail()
        self.assertEqual(len(self.portal.creates),1)
        self.assertEqual(first["results"][0]["status"],"PORTAL_NEU_CHEFPRUEFUNG")
        self.assertEqual(second["results"][0]["status"],"PORTAL_BESTEHT_WORKFLOW_FORTSETZEN")
        self.assertEqual((first["lexware_writes"],first["bank_actions"],first["mail_flags_changed"]),(0,0,False))
        self.assertEqual(self.portal.creates[0][2]["bereich"],"unklar")
        self.assertIn("ceo-mail:",self.portal.creates[0][2]["source_ref"])
    def test_portal_and_lexware_existing_skip_creation(self):
        digest=hashlib.sha256(PDF).hexdigest()
        self.portal.records=[{"id":"existing-portal","sha256":digest}]
        self.lexware.records=[{"id":"existing-lex","sha256":digest}]
        self.assertEqual(self.run_mail()["results"][0]["status"],"PORTAL_UND_LEXWARE_BESTEHEN")
        self.assertFalse(self.portal.creates)
    def test_lexware_only_is_preserved_as_link_proposal_not_reuploaded(self):
        self.lexware.records=[dict(self.meta,id="existing-lex")]
        result=self.run_mail()["results"][0]
        self.assertEqual(result["status"],"LEXWARE_BESTEHT_NUR_ZUORDNUNG_PRUEFEN")
        self.assertEqual(result["lexware_id"],"existing-lex")
        self.assertEqual(len(self.portal.creates),1)
    def test_incomplete_legacy_or_lexware_check_blocks_new_record(self):
        self.portal.complete=False
        self.assertIn("UNVOLLSTAENDIG",self.run_mail()["results"][0]["status"])
        self.portal.complete=True;self.lexware.complete=False
        self.assertIn("UNVOLLSTAENDIG",self.run_mail()["results"][0]["status"])
        self.assertFalse(self.portal.creates)
    def test_ambiguous_identity_does_not_create_or_choose_one(self):
        self.lexware.records=[dict(self.meta,id="one"),dict(self.meta,id="two")]
        self.assertEqual(self.run_mail()["results"][0]["status"],"MEHRDEUTIGER_LEXWARETREFFER")
        self.assertFalse(self.portal.creates)
    def test_private_is_not_sent_to_company_lexware_and_does_not_become_payable(self):
        self.meta.update(bereich="privat",bereich_verified=True)
        self.lexware.find_matches=Mock(side_effect=AssertionError("No private company lookup"))
        self.assertEqual(self.run_mail()["results"][0]["status"],"PRIVAT_ZUR_PRUEFUNG")
        self.assertEqual(self.portal.creates[0][2]["bereich"],"privat")

    def test_unverified_private_hint_skips_company_lookup_without_asserting_private_classification(self):
        self.meta.update(identity_verified=False,bereich_verified=False,bereich="privat",private_review_only=True)
        self.lexware.find_matches=Mock(side_effect=AssertionError("No company lookup for private hint"))
        result=self.run_mail()["results"][0]
        self.assertEqual(result["status"],"PORTAL_NEU_CHEFPRUEFUNG")
        self.assertEqual(self.portal.creates[0][2]["bereich"],"unklar")
        self.assertNotIn("lexware_id",result)
        self.lexware.find_matches.assert_not_called()
    def test_attachment_magic_and_limits_and_html_not_executed(self):
        with self.assertRaises(InboxBlocked): list(attachments(mail(b"<html>not PDF</html>")))
        self.assertEqual(list(attachments(mail(PDF,"../../invoice.pdf")))[0][0],"invoice.pdf")
        self.mailbox.messages.return_value=[{"uid":"7","uidvalidity":"2","raw":mail()},{"uid":"8","uidvalidity":"2","raw":mail()}]
        with self.assertRaises(InboxBlocked): self.run_mail(max_messages=1)
    def test_restore_cannot_forget_receipts_or_audit(self):
        self.run_mail()
        with self.assertRaises(ValueError):ensure_office_inbox_for_import(self.p,export={"tables":{}})
        with closing(self.p.get_db()) as db:
            snapshot={"tables":{t:[dict(r) for r in db.execute("SELECT * FROM "+t).fetchall()] for t in BACKUP_TABLES}}
        ensure_office_inbox_for_import(self.p,export=snapshot)
    def test_imap_readonly_peek_searches_seen_and_unseen(self):
        connection=Mock(); connection.__enter__=Mock(return_value=connection); connection.__exit__=Mock(return_value=False)
        connection.select.return_value=("OK",[]);connection.response.return_value=("UIDVALIDITY",[b"2"])
        connection.uid.side_effect=[("OK",[b"7"]),("OK",[b"7 (RFC822.SIZE 500)"]),("OK",[(b"7",mail())])]
        rows=list(ReadonlyIMAP(lambda config:connection,{}).messages(datetime(2026,10,20).date(),datetime(2026,11,2).date(),80))
        self.assertEqual(len(rows),1);connection.select.assert_called_with("INBOX",readonly=True)
        commands=str(connection.uid.call_args_list)
        self.assertIn("BODY.PEEK[]",commands);self.assertNotIn("UNSEEN",commands);self.assertNotIn("store",commands.lower())
    def test_lexware_adapter_only_gets_purchaseinvoice_any_and_target_metadata(self):
        key="8bfa038e-e125-4c4c-bb3a-13ac2aa0f518"
        def request(method,path,query=""):
            self.assertEqual(method,"GET")
            if path=="/v1/voucherlist":
                self.assertIn("voucherType=purchaseinvoice",query);self.assertIn("voucherStatus=any",query)
                return {"last":True,"content":[] if "archived=true" in query else [{"id":key,"voucherType":"purchaseinvoice","contactName":"Supplier GmbH","voucherNumber":"INV-1","voucherDate":"2026-11-01","totalAmount":119}]}
            return {"type":"purchaseinvoice","voucherNumber":"INV-1","voucherDate":"2026-11-01","totalGrossAmount":119,"files":[]}
        result=LexwarePurchaseIndex(request).find_matches(hashlib.sha256(PDF).hexdigest(),self.meta)
        self.assertEqual(result["matches"][0]["id"],key)


if __name__=="__main__":main()
