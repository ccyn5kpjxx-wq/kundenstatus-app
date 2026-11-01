"""Office receipts on a synthetic temporary SQLite database; never import app.py."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from datetime import date, timedelta
import base64
import hashlib
import io
import json
import re
from html.parser import HTMLParser
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from flask import Flask, session
from jinja2 import ChoiceLoader, DictLoader
from PIL import Image
from pypdf import PdfWriter
from werkzeug.datastructures import FileStorage, MultiDict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_buero_belege import MAX_ORIGINAL_BYTES, MAX_REQUEST_BYTES, TABLES, RevisionConflict, register_office_documents, _beleg_ddl, init_office_documents_schema, normalize_iban, normalize_bic


def pdf_bytes(marker="synthetic"):
    out = io.BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    writer.add_metadata({"/Title": marker})
    writer.write(out)
    return out.getvalue()


def image_bytes(kind="PNG"):
    out = io.BytesIO()
    Image.new("RGB", (8, 8), "white").save(out, format=kind)
    return out.getvalue()


class Access:
    def __init__(self, portal):
        self.p = portal

    def identity(self, db=None):
        mid = session.get("office_mid")
        if type(mid) is not int:
            return None
        if db is None:
            with closing(self.p.get_db()) as connection:
                return self.identity(connection)
        row = db.execute("SELECT active FROM synthetic_office WHERE id=?", (mid,)).fetchone()
        return {"actor": f"mitarbeiter:{mid}", "mitarbeiter_id": mid, "mitarbeiter_name": "Synthetic Office", "office_categories": ("belege",)} if row and row["active"] else None


class Portal:
    def __init__(self, folder):
        self.path = folder / "synthetic.sqlite"
        self.app = Flask(__name__, template_folder=str(ROOT / "templates"), static_folder=str(ROOT / "static"))
        self.app.config.update(TESTING=True, SECRET_KEY="isolated-office-documents-test")
        self.app.jinja_loader = ChoiceLoader([DictLoader({"buero_nav.html": "<nav>Synthetic office navigation</nav>"}), self.app.jinja_loader])
        self.lock = threading.RLock()
        self.backups = []
        with closing(self.get_db()) as db:
            db.executescript("CREATE TABLE synthetic_office(id INTEGER PRIMARY KEY,active INTEGER); INSERT INTO synthetic_office VALUES(1,1),(2,1);")
            db.commit()
        self.office_access = Access(self)
        self.documents = register_office_documents(self)

    def get_db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    @contextmanager
    def portal_originals_operation_lock(self):
        with self.lock:
            yield

    def schedule_change_backup(self, reason):
        self.backups.append(reason)


class OfficeDocumentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p = Portal(Path(self.temp.name))
        self.office = self.p.app.test_client()
        self.admin = self.p.app.test_client()
        self.anon = self.p.app.test_client()
        self.login(self.office)
        self.login(self.admin, admin=True)

    @staticmethod
    def login(client, *, admin=False, mid=1):
        with client.session_transaction() as state:
            state.clear()
            state["csrf_token"] = "synthetic-csrf"
            if admin:
                state["admin"] = True
            else:
                state["office_mid"] = mid

    @staticmethod
    def fields(**changes):
        data = dict(typ="rechnung", lieferant="Synthetic Supplier", belegnummer="SYN-001", belegdatum="2026-01-03",
                    betrag_brutto="119,50", faelligkeit="2026-01-20", notiz="Synthetic business note", csrf_token="synthetic-csrf")
        data.update(changes)
        return data

    def upload(self, *, client=None, raw=None, filename="synthetic.pdf", **changes):
        data = self.fields(**changes)
        data["original"] = (io.BytesIO(raw if raw is not None else pdf_bytes()), filename)
        return (client or self.office).post("/werkstatt/buero/belege/erfassen", data=data)

    def create(self, **changes):
        response = self.upload(**changes)
        self.assertEqual(response.status_code, 303, response.get_data(as_text=True))
        return response.headers["Location"].rsplit("/", 1)[-1]

    def rows(self, table=TABLES[0]):
        self.assertIn(table, (*TABLES, "synthetic_office"))
        with closing(self.p.get_db()) as db:
            return [dict(row) for row in db.execute("SELECT * FROM " + table + " ORDER BY id").fetchall()]

    def row(self, key):
        return next(row for row in self.rows() if row["id"] == key)

    def action(self, key, action, *, admin=False, client=None, version=None, **changes):
        data = dict(csrf_token="synthetic-csrf", confirmed="ja", revision=str(self.row(key)["revision"] if version is None else version))
        if admin and action == "freigeben":
            data["bereich"] = "geschaeftlich"
        data.update(changes)
        base = "/admin/buero/belege/" if admin else "/werkstatt/buero/belege/"
        return (client or (self.admin if admin else self.office)).post(base + key + "/" + action, data=data)

    def submit(self, key):
        response = self.action(key, "einreichen")
        self.assertEqual(response.status_code, 303, response.get_data(as_text=True))

    def approve(self, key, **changes):
        response = self.action(key, "freigeben", admin=True, **changes)
        self.assertEqual(response.status_code, 303, response.get_data(as_text=True))

    def extract(self, key, fields, *, version=None, admin=False, mid=1, **kwargs):
        with self.p.app.test_request_context():
            if admin: session["admin"] = True
            else: session["office_mid"] = mid
            return self.p.documents.record_extracted_fields(key, str(version or self.row(key)["revision"]), fields, **kwargs)

    def test_chef_directly_approves_classified_draft_in_one_revision_without_self_submission(self):
        key = self.create(raw=pdf_bytes("direct-chef-draft"), typ="post", kategorie="")
        before = self.row(key)
        approved = []
        self.p.office_document_approved = approved.append
        response = self.action(key, "freigeben", admin=True, typ="rechnung", kategorie="rechnung")
        self.assertEqual(response.status_code, 303)
        after = self.row(key)
        self.assertEqual((after["typ"], after["kategorie"], after["status"], after["payable"], after["business_confirmed"]),
                         ("rechnung", "rechnung", "freigegeben", 1, 1))
        self.assertEqual(after["revision"], before["revision"] + 1)
        self.assertEqual(after["submitted_at"], "")
        for field in ("original_base64", "sha256", "created_by", "betrag_cent"):
            self.assertEqual(after[field], before[field])
        audit = self.rows(TABLES[1])
        self.assertEqual(len(audit), 2)
        entry = next(row for row in audit if row["aktion"] == "freigegeben")
        details = json.loads(entry["details_json"])
        self.assertEqual(details["vorher_status"], "entwurf")
        self.assertTrue(details["direkte_chefpruefung"])
        self.assertTrue(details["klassifizierung_geaendert"])
        self.assertFalse(any(row["aktion"] == "eingereicht" for row in audit))
        self.assertEqual(approved, [key])
        self.assertEqual(self.counts()["zahlung_offen"], 1)
        self.assertEqual(self.counts(admin=True)["offen_anzahl"], 0)

    def test_direct_approval_requires_explicit_classification_and_failed_attempt_changes_nothing(self):
        key = self.create(raw=pdf_bytes("explicit-classification"), typ="post")
        before, audits = self.row(key), self.rows(TABLES[1])
        for fields in ({}, {"typ": "rechnung"}, {"kategorie": "rechnung"},
                       {"typ": "rechnung", "kategorie": ""},
                       {"typ": "unknown", "kategorie": "rechnung"},
                       {"typ": "rechnung", "kategorie": "unknown"}):
            with self.subTest(fields=fields):
                self.assertEqual(self.action(key, "freigeben", admin=True, **fields).status_code, 400)
                self.assertEqual(self.row(key), before)
                self.assertEqual(self.rows(TABLES[1]), audits)

    def test_direct_invoice_checks_all_required_source_fields_before_changing_type(self):
        for missing in ("lieferant", "belegnummer", "belegdatum", "betrag_brutto"):
            with self.subTest(missing=missing):
                key = self.create(raw=pdf_bytes("direct-missing-" + missing), typ="post", **{missing: ""})
                before, audits = self.row(key), self.rows(TABLES[1])
                self.assertEqual(self.action(key, "freigeben", admin=True, typ="rechnung", kategorie="rechnung").status_code, 400)
                self.assertEqual(self.row(key), before)
                self.assertEqual(self.rows(TABLES[1]), audits)

    def test_returned_document_can_be_directly_reviewed_without_second_submission(self):
        key = self.create(raw=pdf_bytes("returned-direct"))
        self.submit(key)
        self.assertEqual(self.action(key, "zurueckweisen", admin=True, chef_notiz="Synthetic correction requested").status_code, 303)
        before = self.row(key)
        self.assertEqual(before["status"], "zurueckgewiesen")
        self.assertEqual(self.action(key, "freigeben", admin=True, typ="rechnung", kategorie="privat", bereich="privat").status_code, 303)
        row = self.row(key)
        self.assertEqual((row["status"], row["bereich"], row["business_confirmed"], row["payable"]), ("freigegeben", "privat", 0, 1))
        self.assertEqual(row["revision"], before["revision"] + 1)
        self.assertEqual(len([entry for entry in self.rows(TABLES[1]) if entry["aktion"] == "eingereicht"]), 1)

    def test_direct_bescheid_and_payment_order_keep_their_distinct_payment_confirmations(self):
        reviewed = self.create(raw=pdf_bytes("direct-bescheid-review"), typ="post")
        self.assertEqual(self.action(reviewed, "freigeben", admin=True, typ="bescheid", kategorie="gebuehren").status_code, 303)
        self.assertEqual(self.row(reviewed)["payable"], 0)
        payable = self.create(raw=pdf_bytes("direct-bescheid-pay"), typ="post")
        self.assertEqual(self.action(payable, "freigeben", admin=True, typ="bescheid", kategorie="gebuehren", zahlungspflichtig="ja").status_code, 303)
        self.assertEqual(self.row(payable)["payable"], 1)
        order = self.create(raw=pdf_bytes("direct-order"), **self.order_fields())
        before, audits = self.row(order), self.rows(TABLES[1])
        self.assertEqual(self.action(order, "freigeben", admin=True, typ="zahlungsauftrag", kategorie="miete").status_code, 400)
        self.assertEqual(self.row(order), before); self.assertEqual(self.rows(TABLES[1]), audits)
        self.assertEqual(self.action(order, "freigeben", admin=True, typ="zahlungsauftrag", kategorie="miete", zahlungspflichtig="ja").status_code, 303)
        self.assertEqual((self.row(order)["payable"], self.row(order)["quelle_geprueft"]), (1, 1))

    def test_direct_classification_changes_validate_mode_and_dedup_before_commit(self):
        raw = pdf_bytes("classification-dedup")
        original = self.create(raw=raw, typ="post")
        order = self.create(raw=raw, **self.order_fields(zahlungsmodus="einzelzahlung", lieferant="Synthetic Supplier",
                            belegdatum="2026-01-03", belegnummer="SYN-ORDER"))
        before, audits = self.row(order), self.rows(TABLES[1])
        self.assertNotEqual(original, order)
        self.assertEqual(self.action(order, "freigeben", admin=True, typ="rechnung", kategorie="rechnung").status_code, 400)
        self.assertEqual(self.row(order), before); self.assertEqual(self.rows(TABLES[1]), audits)
        monthly = self.create(raw=pdf_bytes("classification-monthly-mode"), **self.order_fields())
        before, audits = self.row(monthly), self.rows(TABLES[1])
        self.assertEqual(self.action(monthly, "freigeben", admin=True, typ="zahlungsauftrag", kategorie="kaution", zahlungspflichtig="ja").status_code, 400)
        self.assertEqual(self.row(monthly), before); self.assertEqual(self.rows(TABLES[1]), audits)

    def test_submitted_classification_change_cannot_bypass_invoice_requirements(self):
        key = self.create(raw=pdf_bytes("submitted-reclassification"), typ="post", lieferant="", belegnummer="", belegdatum="", betrag_brutto="")
        self.submit(key)
        before, audits = self.row(key), self.rows(TABLES[1])
        self.assertEqual(self.action(key, "freigeben", admin=True, typ="rechnung", kategorie="rechnung").status_code, 400)
        self.assertEqual(self.row(key), before); self.assertEqual(self.rows(TABLES[1]), audits)
        self.assertEqual(self.action(key, "freigeben", admin=True, typ="post", kategorie="sonstige").status_code, 303)
        self.assertEqual((self.row(key)["status"], self.row(key)["payable"]), ("freigegeben", 0))

    def test_direct_approval_has_fresh_revision_and_admin_boundary(self):
        key = self.create(raw=pdf_bytes("direct-stale-and-rights"))
        before, audits = self.row(key), self.rows(TABLES[1])
        self.assertEqual(self.action(key, "freigeben", admin=True, client=self.office, typ="rechnung", kategorie="rechnung").status_code, 403)
        self.assertEqual(self.action(key, "freigeben", admin=True, client=self.anon, typ="rechnung", kategorie="rechnung").status_code, 403)
        self.assertEqual(self.row(key), before); self.assertEqual(self.rows(TABLES[1]), audits)
        self.assertEqual(self.action(key, "freigeben", admin=True, typ="rechnung", kategorie="rechnung").status_code, 303)
        after, audits = self.row(key), self.rows(TABLES[1])
        self.assertEqual(self.action(key, "freigeben", admin=True, version=str(before["revision"]), typ="rechnung", kategorie="rechnung").status_code, 409)
        self.assertEqual(self.row(key), after); self.assertEqual(self.rows(TABLES[1]), audits)

    def test_direct_approval_rejects_corrupt_original_and_rolls_back_failed_audit(self):
        key = self.create(raw=pdf_bytes("direct-original-check"))
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_belege SET original_base64='broken' WHERE id=?", (key,)); db.commit()
        before, audits = self.row(key), self.rows(TABLES[1])
        self.assertEqual(self.action(key, "freigeben", admin=True, typ="rechnung", kategorie="rechnung").status_code, 400)
        self.assertEqual(self.row(key), before); self.assertEqual(self.rows(TABLES[1]), audits)
        valid = self.create(raw=pdf_bytes("direct-audit-rollback"), typ="post")
        before, audits, backups = self.row(valid), self.rows(TABLES[1]), list(self.p.backups)
        with self.p.app.test_request_context():
            session["admin"] = True
            with patch.object(self.p.documents, "_audit", side_effect=RuntimeError("synthetic audit failure")):
                with self.assertRaises(RuntimeError):
                    self.p.documents.decide(valid, str(before["revision"]), approve=True, kind="rechnung", category="rechnung",
                                            bereich="geschaeftlich", business_confirmed=True)
        self.assertEqual(self.row(valid), before); self.assertEqual(self.rows(TABLES[1]), audits)
        self.assertEqual(self.p.backups, backups)

    def test_chef_counts_unsubmitted_draft_as_open_but_returned_document_remains_office_work(self):
        key = self.create(raw=pdf_bytes("direct-open-count"), kategorie="rechnung")
        self.assertEqual((self.counts(admin=True)["neue_anzahl"], self.counts(admin=True)["offen_anzahl"]), (1, 1))
        self.assertEqual(self.counts(admin=True)["kategorien"]["rechnung"]["offen_anzahl"], 1)
        with self.p.app.test_request_context():
            session["admin"] = True
            result = self.p.documents.list(admin=True)
            self.assertTrue(result["rows"][0]["is_new"])
        self.admin.get("/admin/buero/belege/" + key)
        self.assertEqual((self.counts(admin=True)["neue_anzahl"], self.counts(admin=True)["offen_anzahl"]), (0, 1))
        self.submit(key)
        self.assertEqual(self.action(key, "zurueckweisen", admin=True, chef_notiz="Synthetic office correction").status_code, 303)
        self.assertEqual(self.counts(admin=True)["offen_anzahl"], 0)
        self.assertEqual(self.counts()["offen_anzahl"], 1)

    def test_file_first_upload_analysis_and_short_confirmed_review(self):
        initial = self.office.get("/werkstatt/buero/belege").get_data(as_text=True)
        self.assertIn('<details class="bb-manual">', initial)
        self.assertLess(initial.index('>Hochladen &amp; analysieren</button>'), initial.index('<summary>Selbst Angaben eintragen</summary>'))
        self.assertEqual(initial.count(" required"), 1)
        raw = pdf_bytes("file-first-analysis")

        def analyse(key):
            return self.p.documents.record_extracted_fields(key, str(self.row(key)["revision"]),
                {"typ": "rechnung", "kategorie": "rechnung", "lieferant": "Extracted Synthetic Supplier",
                 "belegnummer": "AUTO-1", "belegdatum": "2026-01-03", "betrag_brutto": "119.50"},
                "Synthetic source evidence", "local")

        self.p.office_document_uploaded = analyse
        response = self.office.post("/werkstatt/buero/belege/erfassen", data={
            "csrf_token": "synthetic-csrf", "original": (io.BytesIO(raw), "synthetic.pdf")})
        self.assertEqual(response.status_code, 303)
        key = response.headers["Location"].rsplit("/", 1)[-1]
        row = self.row(key)
        self.assertEqual((row["typ"], row["typ_vorschlag"], row["kategorie"], row["kategorie_vorschlag"]), ("post", "rechnung", "", "rechnung"))
        self.assertEqual((row["lieferant"], row["belegnummer"], row["betrag_cent"], row["faelligkeit"]), ("Extracted Synthetic Supplier", "AUTO-1", 11950, ""))
        self.assertEqual((row["status"], row["payable"], row["business_confirmed"]), ("entwurf", 0, 0))
        html = self.office.get(response.headers["Location"]).get_data(as_text=True)
        self.assertIn('<summary>Angaben bearbeiten</summary>', html)
        self.assertIn('<details class="bb-analysis-details">', html)
        self.assertIn('Belegart prüfen', html)
        self.assertEqual(self.action(key, "einreichen", typ="rechnung", kategorie="rechnung").status_code, 303)
        self.assertEqual((self.row(key)["typ"], self.row(key)["status"], self.row(key)["payable"]), ("rechnung", "eingereicht", 0))
        self.assertEqual(self.office.get("/werkstatt/buero/belege/" + key + "/original").data, raw)

    def test_extracted_values_preserve_manual_metadata_and_confirmed_folder(self):
        key = self.create(kategorie="rechnung", faelligkeit="")
        original = self.row(key)
        revision = self.extract(key, {"lieferant": "Other", "belegnummer": "OTHER", "belegdatum": "2026-02-01",
            "betrag_brutto": "999.99", "faelligkeit": "2026-02-20", "typ": "bescheid", "kategorie": "steuern"},
            hint="Synthetic fields only", source="ai")
        row = self.row(key)
        for name in ("lieferant", "belegnummer", "belegdatum", "betrag_cent", "notiz", "typ", "kategorie", "original_base64", "sha256"):
            self.assertEqual(row[name], original[name], name)
        self.assertEqual((row["faelligkeit"], row["typ_vorschlag"], row["kategorie_vorschlag"]), ("2026-02-20", "bescheid", ""))
        self.assertEqual(row["revision"], revision)
        with self.p.app.test_request_context():
            session["office_mid"] = 1
            detail = self.p.documents.detail(key)
        evidence = next(entry["details"] for entry in detail["audit"] if entry["aktion"] == "belegangaben_erkannt")
        self.assertEqual(evidence["analyse_quelle"], "ai")
        self.assertEqual(set(evidence["ergaenzte_felder"]), {"faelligkeit", "typ"})

    def test_zero_invalid_and_ambiguous_extraction_never_fabricates_a_value(self):
        key = self.create(typ="post", lieferant="", belegnummer="", belegdatum="", faelligkeit="", betrag_brutto="", notiz="")
        self.extract(key, {"lieferant": ["Supplier A", "Supplier B"], "belegnummer": {"first": "1", "second": "2"},
            "belegdatum": "2026-02-30", "faelligkeit": ["2026-02-01", "2026-02-02"], "betrag_brutto": "0.00", "typ": ["rechnung", "bescheid"]},
            hint="Mehrere Möglichkeiten; offen lassen", source="mixed")
        row = self.row(key)
        self.assertEqual((row["lieferant"], row["belegnummer"], row["belegdatum"], row["faelligkeit"], row["betrag_cent"], row["typ_vorschlag"]), ("", "", "", "", None, ""))
        self.assertIn("Nicht eindeutig übernommen", row["klassifizierung_hinweis"])
        # Unpayable post does not require invented sender, amount or due date.
        self.assertEqual(self.action(key, "einreichen", typ="post").status_code, 303)
        self.assertEqual(self.row(key)["payable"], 0)

    def test_extraction_safety_fields_stale_revision_and_regular_submission_are_closed(self):
        key = self.create(faelligkeit="")
        before = self.row(key)
        for field, value in (("payable", 1), ("business_confirmed", True), ("bereich", "geschaeftlich"), ("paid_date", "2026-01-03")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.extract(key, {field: value})
        self.assertEqual(self.row(key), before)
        self.extract(key, {"faelligkeit": "2026-02-01"})
        current = self.row(key)
        with self.assertRaises(RevisionConflict):
            self.extract(key, {"belegnummer": "stale"}, version=before["revision"])
        with self.assertRaises(LookupError):
            self.extract(key, {"lieferant": "Other actor"}, mid=2)
        self.assertEqual(self.row(key), current)
        self.submit(key)
        submitted = self.row(key)
        with self.assertRaises(RevisionConflict):
            self.extract(key, {"faelligkeit": "2026-03-01"}, admin=True)
        self.assertEqual(self.row(key), submitted)
        self.approve(key)
        approved = self.row(key)
        with self.assertRaises(RevisionConflict):
            self.extract(key, {"faelligkeit": "2026-04-01"}, admin=True)
        self.assertEqual(self.row(key), approved)

    def test_intake_extraction_remains_private_unapproved_and_limited_to_own_intake(self):
        raw = pdf_bytes("intake-extraction")
        payload = {"typ": "post"}
        key, _ = self.p.documents.create_from_intake(payload, FileStorage(stream=io.BytesIO(raw), filename="synthetic.pdf"), source_ref="synthetic-intake")
        revision = self.p.documents.record_extracted_fields(key, "1", {"lieferant": "Synthetic Private Sender",
            "betrag_brutto": "1604.00", "typ": "bescheid", "kategorie": "privat"}, "Synthetic private suggestion", "local", intake=True)
        row = self.row(key)
        self.assertEqual((row["revision"], row["status"], row["bereich"], row["business_confirmed"], row["payable"]), (revision, "eingereicht", "unklar", 0, 0))
        self.assertEqual(self.office.get("/werkstatt/buero/belege/" + key).status_code, 404)
        self.assertEqual(base64.b64decode(row["original_base64"]), raw)
        ordinary = self.create(raw=pdf_bytes("ordinary-extraction"))
        with self.assertRaises(PermissionError):
            self.p.documents.record_extracted_fields(ordinary, "1", {"lieferant": "Not own intake"}, intake=True)

    def test_extraction_failure_after_upload_preserves_original_and_skips_payment_orders(self):
        def failing(key):
            return self.p.documents.record_extracted_fields(key, str(self.row(key)["revision"]), {"payable": 1})
        self.p.office_document_uploaded = failing
        raw = pdf_bytes("extract-failure")
        key = self.create(raw=raw)
        self.assertEqual(self.office.get("/werkstatt/buero/belege/" + key + "/original").data, raw)
        self.assertIn("Scan-Analyse", self.row(key)["verarbeitung_hinweis"])
        order = self.create(raw=pdf_bytes("order-no-extraction"), **self.order_fields())
        before = self.row(order)
        with self.assertRaises(ValueError):
            self.extract(order, {"betrag_brutto": "999.00"})
        self.assertEqual(self.row(order), before)

    def test_concurrent_extraction_applies_only_one_current_revision(self):
        key = self.create(typ="post", lieferant="", betrag_brutto="")
        version = self.row(key)["revision"]
        def attempt(supplier):
            try:
                self.extract(key, {"lieferant": supplier}, version=version)
                return "saved"
            except RevisionConflict:
                return "stale"
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sorted(pool.map(attempt, ["First synthetic", "Second synthetic"])), ["saved", "stale"])
        self.assertEqual(self.row(key)["revision"], version + 1)
        self.assertEqual(len([row for row in self.rows(TABLES[1]) if row["aktion"] == "belegangaben_erkannt"]), 1)

    def test_invoice_full_workflow_original_integrity_and_audit(self):
        raw = pdf_bytes()
        key = self.create(raw=raw)
        row = self.row(key)
        self.assertEqual(row["betrag_cent"], 11950)
        self.assertEqual(row["status"], "entwurf")
        self.assertEqual(base64.b64decode(row["original_base64"]), raw)
        self.assertEqual(row["sha256"], hashlib.sha256(raw).hexdigest())
        self.assertNotIn("original_base64", self.office.get("/werkstatt/buero/belege/" + key).get_data(as_text=True))
        self.assertEqual(self.action(key, "bezahlt", zahlungsdatum="2026-01-05", zahlungsreferenz="synthetic-ref").status_code, 409)
        self.submit(key)
        with self.p.app.test_request_context():
            session["admin"] = True
            self.assertEqual(self.p.documents.pending_invoice_count(), 1)
            self.assertEqual(self.p.documents.pending_review_count(), 1)
        self.approve(key)
        self.assertEqual(self.row(key)["status"], "freigegeben")
        self.assertEqual(self.row(key)["paid_date"], "")
        self.assertEqual(self.row(key)["payable"], 1)
        self.assertEqual(self.action(key, "ueberweisung").status_code, 303)
        self.assertEqual(self.row(key)["status"], "zahlung_in_bearbeitung")
        self.assertEqual(self.row(key)["paid_date"], "")
        self.assertEqual(self.action(key, "bezahlt", zahlungsdatum="2026-01-05", zahlungsreferenz="synthetic-bank-ref").status_code, 303)
        row = self.row(key)
        self.assertEqual((row["status"], row["paid_date"], row["payment_reference"]), ("bezahlt", "2026-01-05", "synthetic-bank-ref"))
        original = self.office.get("/werkstatt/buero/belege/" + key + "/original")
        self.assertEqual(original.data, raw)
        self.assertIn("attachment;", original.headers["Content-Disposition"])
        self.assertIn("no-store", original.headers["Cache-Control"])
        self.assertEqual(original.headers["X-Content-Type-Options"], "nosniff")
        actions = [row["aktion"] for row in sorted(self.rows(TABLES[1]), key=lambda r: r["created_at"])]
        self.assertEqual(actions, ["erfasst", "eingereicht", "freigegeben", "ueberweisung_extern_in_bearbeitung", "zahlung_dokumentiert"])
        self.assertEqual(self.rows()[0]["original_base64"], base64.b64encode(raw).decode("ascii"))
        self.assertTrue(self.p.backups)

    def test_office_cannot_approve_reject_or_read_admin_routes(self):
        key = self.create()
        self.submit(key)
        for action in ("freigeben", "zurueckweisen"):
            self.assertEqual(self.action(key, action, admin=True, client=self.office, chef_notiz="synthetic").status_code, 403)
        for path in ("/admin/buero/belege", "/admin/buero/belege/" + key, "/admin/buero/belege/" + key + "/original"):
            self.assertEqual(self.office.get(path).status_code, 403)
        self.assertEqual(self.row(key)["status"], "eingereicht")
        self.assertEqual(self.admin.get("/admin/buero/belege/" + key).status_code, 200)

    def test_unauthorized_and_revoked_identity_cannot_read_or_write_original(self):
        key = self.create()
        for client in (self.anon, self.admin):
            for path in ("/werkstatt/buero/belege", "/werkstatt/buero/belege/" + key, "/werkstatt/buero/belege/" + key + "/original"):
                self.assertEqual(client.get(path).status_code, 403)
            self.assertEqual(self.upload(client=client).status_code, 403)
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE synthetic_office SET active=0 WHERE id=1")
            db.commit()
        self.assertEqual(self.office.get("/werkstatt/buero/belege/" + key + "/original").status_code, 403)
        self.assertEqual(self.action(key, "einreichen").status_code, 403)

    def test_hash_duplicate_preserves_metadata_and_concurrent_upload_is_one_document(self):
        key = self.create()
        original = self.row(key)
        duplicate = self.upload(lieferant="Must not overwrite", typ="post", betrag_brutto="1.00")
        self.assertEqual(duplicate.status_code, 303)
        self.assertTrue(duplicate.headers["Location"].endswith(key))
        self.assertEqual(self.row(key), original)
        self.assertEqual(len(self.rows(TABLES[1])), 1)
        raw = pdf_bytes("concurrent")
        def create(_):
            client = self.p.app.test_client()
            self.login(client, mid=2)
            return self.upload(client=client, raw=raw).status_code
        with ThreadPoolExecutor(max_workers=3) as pool:
            self.assertEqual(list(pool.map(create, range(3))), [303] * 3)
        self.assertEqual(len(self.rows()), 2)
        self.assertEqual(len(self.rows(TABLES[1])), 2)

    def test_stale_and_concurrent_review_cannot_overwrite_decision(self):
        key = self.create()
        self.submit(key)
        version = self.row(key)["revision"]
        self.approve(key, chef_notiz="First decision")
        self.assertEqual(self.action(key, "zurueckweisen", admin=True, version=version, chef_notiz="Stale").status_code, 409)
        data = self.fields(betrag_brutto="1.00")
        data["revision"] = str(version)
        self.assertEqual(self.office.post("/werkstatt/buero/belege/" + key + "/bearbeiten", data=data).status_code, 409)
        self.assertEqual((self.row(key)["status"], self.row(key)["betrag_cent"]), ("freigegeben", 11950))
        second = self.create(raw=pdf_bytes("competing-review"))
        self.submit(second)
        version = self.row(second)["revision"]
        def decide(action):
            client = self.p.app.test_client()
            self.login(client, admin=True)
            return self.action(second, action, admin=True, client=client, version=version, chef_notiz="Synthetic competing decision").status_code
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sorted(pool.map(decide, ("freigeben", "zurueckweisen"))), [303, 409])

    def test_rejection_correction_and_resubmission_keep_original_and_audit(self):
        key = self.create()
        self.submit(key)
        self.assertEqual(self.action(key, "zurueckweisen", admin=True, chef_notiz="").status_code, 400)
        self.assertEqual(self.action(key, "zurueckweisen", admin=True, chef_notiz="Betrag prüfen").status_code, 303)
        original = self.row(key)["original_base64"]
        data = self.fields(betrag_brutto="125.55")
        data["revision"] = str(self.row(key)["revision"])
        self.assertEqual(self.office.post("/werkstatt/buero/belege/" + key + "/bearbeiten", data=data).status_code, 303)
        self.assertEqual((self.row(key)["status"], self.row(key)["betrag_cent"]), ("entwurf", 12555))
        self.assertEqual(self.row(key)["business_confirmed"], 0)
        self.assertEqual(self.row(key)["original_base64"], original)
        self.submit(key)
        self.approve(key)
        audits = self.rows(TABLES[1])
        self.assertTrue(any("Betrag prüfen" in row["details_json"] for row in audits))

    def test_delivery_note_and_other_post_never_become_payable(self):
        for kind in ("lieferschein", "post"):
            key = self.create(raw=pdf_bytes(kind), typ=kind)
            self.submit(key)
            self.assertEqual(self.action(key, "freigeben", admin=True, zahlungspflichtig="ja").status_code, 400)
            self.approve(key)
            self.assertEqual(self.row(key)["payable"], 0)
            self.assertEqual(self.action(key, "ueberweisung").status_code, 400)
            self.assertEqual(self.action(key, "bezahlt", zahlungsdatum="2026-01-05", zahlungsreferenz="synthetic").status_code, 400)
            page = self.office.get("/werkstatt/buero/belege/" + key).get_data(as_text=True)
            self.assertNotIn("Tatsächlich ausgeführte Zahlung dokumentieren", page)

    def test_bescheid_requires_explicit_payment_release(self):
        reviewed = self.create(raw=pdf_bytes("bescheid-review"), typ="bescheid")
        self.submit(reviewed)
        self.approve(reviewed)
        self.assertEqual(self.row(reviewed)["payable"], 0)
        payable = self.create(raw=pdf_bytes("bescheid-payable"), typ="bescheid")
        self.submit(payable)
        self.approve(payable, zahlungspflichtig="ja")
        self.assertEqual(self.row(payable)["payable"], 1)
        self.assertEqual(self.action(payable, "bezahlt", zahlungsdatum="2026-01-05", zahlungsreferenz="synthetic-assessment-payment").status_code, 303)
        missing = self.create(raw=pdf_bytes("missing-amount"), typ="bescheid", betrag_brutto="")
        self.submit(missing)
        self.assertEqual(self.action(missing, "freigeben", admin=True, zahlungspflichtig="ja").status_code, 400)
        self.assertEqual(self.row(missing)["status"], "eingereicht")

    def test_csrf_duplicate_fields_mass_assignment_and_explicit_confirmations(self):
        self.assertEqual(self.upload(csrf_token="wrong").status_code, 400)
        self.assertEqual(self.upload(status="bezahlt").status_code, 400)
        key = self.create()
        base = "/werkstatt/buero/belege/" + key + "/einreichen"
        data = MultiDict([("csrf_token", "synthetic-csrf"), ("csrf_token", "wrong"), ("revision", "1"), ("confirmed", "ja")])
        self.assertEqual(self.office.post(base, data=data).status_code, 400)
        data.setlist("csrf_token", ["synthetic-csrf", "synthetic-csrf"])
        data.setlist("revision", ["1", "2"])
        self.assertEqual(self.office.post(base, data=data).status_code, 400)
        data.setlist("revision", ["1"])
        data.setlist("confirmed", [""])
        self.assertEqual(self.office.post(base, data=data).status_code, 400)
        data.setlist("confirmed", ["ja"])
        self.assertEqual(self.office.post(base, data=data).status_code, 303)
        self.approve(key)
        self.assertEqual(self.action(key, "bezahlt", confirmed="", zahlungsdatum="2026-01-05", zahlungsreferenz="synthetic").status_code, 400)
        self.assertEqual(self.row(key)["paid_date"], "")

    def test_valid_images_and_invalid_files_and_upload_limits(self):
        for fmt, extension in (("PNG", "png"), ("JPEG", "jpg")):
            self.create(raw=image_bytes(fmt), filename="synthetic." + extension)
        before = len(self.rows())
        invalids = [(b"<script>synthetic</script>", "fake.pdf"), (b"%PDF-damaged", "fake.pdf"),
                    (image_bytes(), "fake.jpg"), (pdf_bytes(), "fake.html")]
        for raw, filename in invalids:
            self.assertIn(self.upload(raw=raw, filename=filename).status_code, (400, 413))
        self.assertEqual(len(self.rows()), before)
        with self.p.app.test_request_context():
            session["office_mid"] = 1
            with self.assertRaises(ValueError):
                self.p.documents.create({k: v for k, v in self.fields().items() if k != "csrf_token"},
                                        FileStorage(stream=io.BytesIO(b"x" * (MAX_ORIGINAL_BYTES + 1)), filename="big.pdf"))
        response = self.office.post("/werkstatt/buero/belege/erfassen", environ_overrides={"CONTENT_LENGTH": str(MAX_REQUEST_BYTES + 1)})
        self.assertEqual(response.status_code, 413)

    def test_validation_and_no_automatic_payment_from_upload_or_approval(self):
        for value in ("1.234,56", "-1", "0", "NaN", "1.123", "1e2"):
            self.assertEqual(self.upload(betrag_brutto=value).status_code, 400)
        self.assertEqual(self.upload(belegdatum="2026-02-30").status_code, 400)
        key = self.create(belegnummer="", betrag_brutto="")
        self.assertEqual(self.action(key, "einreichen").status_code, 400)
        self.assertEqual(self.row(key)["status"], "entwurf")
        key = self.create(raw=pdf_bytes("payment-validation"))
        self.submit(key)
        self.approve(key)
        for changes in (dict(zahlungsdatum="2026-01-05", zahlungsreferenz=""),
                        dict(zahlungsdatum=(date.today() + timedelta(days=2)).isoformat(), zahlungsreferenz="synthetic")):
            self.assertEqual(self.action(key, "bezahlt", **changes).status_code, 400)
        self.assertEqual((self.row(key)["status"], self.row(key)["paid_date"], self.row(key)["payment_reference"]), ("freigegeben", "", ""))

    def test_missing_and_corrupt_original_fail_closed_and_html_escaped(self):
        key = self.create(lieferant="<script>alert('synthetic')</script>", notiz="<img src=x onerror=synthetic>")
        page = self.office.get("/werkstatt/buero/belege/" + key)
        self.assertEqual(page.status_code, 200)
        self.assertNotIn("<script>", page.get_data(as_text=True))
        self.assertIn("&lt;script&gt;", page.get_data(as_text=True))
        self.assertIn("no-store", page.headers["Cache-Control"])
        self.assertEqual(self.office.get("/werkstatt/buero/belege/" + "f" * 32).status_code, 404)
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_belege SET original_base64=? WHERE id=?", ("broken", key))
            db.commit()
        self.assertEqual(self.office.get("/werkstatt/buero/belege/" + key + "/original").status_code, 404)

    def counts(self, *, admin=False, mid=1):
        with self.p.app.test_request_context():
            session["admin" if admin else "office_mid"] = True if admin else mid
            return self.p.documents.counters(admin=admin)

    def test_seen_is_per_actor_and_never_approves_or_closes_work(self):
        key = self.create()
        self.submit(key)
        revision, audits = self.row(key)["revision"], len(self.rows(TABLES[1]))
        self.assertEqual(self.counts(admin=True)["neue_anzahl"], 1)
        self.assertEqual(self.counts(admin=True)["offen_anzahl"], 1)
        self.assertEqual(self.admin.get("/admin/buero/belege/" + key).status_code, 200)
        self.assertEqual(self.counts(admin=True)["neue_anzahl"], 0)
        self.assertEqual(self.counts(admin=True)["offen_anzahl"], 1)
        self.assertEqual((self.row(key)["status"], self.row(key)["revision"]), ("eingereicht", revision))
        self.assertEqual(len(self.rows(TABLES[1])), audits)
        self.approve(key)
        self.assertEqual(self.row(key)["business_confirmed"], 1)
        self.assertEqual((self.counts()["neue_anzahl"], self.counts()["zahlung_offen"]), (1, 1))
        self.office.get("/werkstatt/buero/belege/" + key)
        self.assertEqual((self.counts()["neue_anzahl"], self.counts()["zahlung_offen"]), (0, 1))
        self.assertEqual(self.counts(mid=2)["neue_anzahl"], 1)
        other = self.p.app.test_client()
        self.login(other, mid=2)
        other.get("/werkstatt/buero/belege/" + key + "/original")
        self.assertEqual(self.counts(mid=2)["neue_anzahl"], 0)
        self.assertEqual(self.row(key)["paid_date"], "")
        self.assertEqual(self.row(key)["status"], "freigegeben")

    def test_newer_revision_stays_unread_and_old_seen_cannot_overclear(self):
        key = self.create()
        self.submit(key)
        self.admin.get("/admin/buero/belege/" + key)
        old = self.row(key)["revision"]
        with self.p.app.test_request_context():
            session["office_mid"] = 1
            self.p.documents.record_scan_suggestion(key, str(old), "rechnung", "Synthetic classification", "rechnung")
        self.assertEqual(self.counts(admin=True)["neue_anzahl"], 1)
        with closing(self.p.get_db()) as db:
            self.p.documents._seen(db, {"actor": "admin"}, key, old)
            db.commit()
        self.assertEqual(self.counts(admin=True)["neue_anzahl"], 1)
        self.assertEqual(self.action(key, "freigeben", admin=True, version=old).status_code, 409)
        self.assertEqual(self.row(key)["status"], "eingereicht")

    def test_scan_suggestion_and_folders_never_set_kind_approval_or_payment(self):
        key = self.create(typ="post", betrag_brutto="")
        with self.p.app.test_request_context():
            session["office_mid"] = 1
            self.p.documents.record_scan_suggestion(key, "1", "steuern", "Synthetic only; review original", "bescheid")
            self.assertEqual(self.p.documents.list(category="steuern")["rows"][0]["id"], key)
        row = self.row(key)
        self.assertEqual((row["typ"], row["status"], row["payable"], row["business_confirmed"]), ("post", "entwurf", 0, 0))
        self.assertEqual(self.counts()["kategorien"]["steuern"]["gesamt"], 1)
        data = self.fields(typ="post", betrag_brutto="", kategorie="gebuehren")
        data["revision"] = str(row["revision"])
        self.assertEqual(self.office.post("/werkstatt/buero/belege/" + key + "/bearbeiten", data=data).status_code, 303)
        with self.p.app.test_request_context():
            session["admin"] = True
            self.p.documents.record_scan_suggestion(key, str(self.row(key)["revision"]), "sonstige", "Later synthetic proposal", "post")
        self.assertEqual(self.counts()["kategorien"]["gebuehren"]["gesamt"], 1)
        self.assertEqual(self.counts()["kategorien"]["sonstige"]["gesamt"], 0)
        self.submit(key)
        page = self.admin.get("/admin/buero/belege?kategorie=gebuehren&bereich=ungeprueft")
        self.assertIn("NEU", page.get_data(as_text=True))
        self.assertEqual(self.row(key)["status"], "eingereicht")

    def test_lexware_reference_requires_business_approval_safe_url_and_current_revision(self):
        key = self.create()
        with self.p.app.test_request_context():
            session["admin"] = True
            with self.assertRaises(ValueError):
                self.p.documents.record_lexware_reference(key, "1", "synthetic-id", "https://app.lexware.de/vouchers/synthetic-id")
        self.submit(key)
        with self.p.app.test_request_context():
            session["admin"] = True
            with self.assertRaises(ValueError):
                self.p.documents.decide(key, str(self.row(key)["revision"]), approve=True)
        self.approve(key)
        version = str(self.row(key)["revision"])
        with self.p.app.test_request_context():
            session["admin"] = True
            for url in ("javascript:synthetic", "https://app.lexware.de.evil.invalid/voucher", "https://app.lexware.de@evil.invalid/x", "https://app.lexware.de:bad/x"):
                with self.assertRaises(ValueError):
                    self.p.documents.record_lexware_reference(key, version, "synthetic-id", url)
            self.p.documents.record_lexware_reference(key, version, "synthetic-id", "https://app.lexware.de/vouchers/synthetic-id")
            with self.assertRaises(ValueError):
                self.p.documents.record_lexware_reference(key, version, "synthetic-id", "https://app.lexware.de/vouchers/synthetic-id")
        row = self.row(key)
        self.assertEqual((row["status"], row["paid_date"]), ("freigegeben", ""))
        self.assertEqual(row["lexware_beleg_id"], "synthetic-id")
        self.assertIn("Beleg in Lexware öffnen", self.office.get("/werkstatt/buero/belege/" + key).get_data(as_text=True))

    def test_read_counters_require_current_role_and_filter_does_not_mark_seen(self):
        key = self.create(kategorie="rechnung")
        self.submit(key)
        self.admin.get("/admin/buero/belege?bereich=ungeprueft&kategorie=rechnung")
        self.assertEqual(self.counts(admin=True)["neue_anzahl"], 1)
        self.assertEqual(self.counts(admin=True)["kategorien"]["rechnung"]["offen_anzahl"], 1)
        self.assertEqual(self.admin.get("/admin/buero/belege?kategorie=invalid").status_code, 400)
        with self.p.app.test_request_context():
            with self.assertRaises(PermissionError):
                self.p.documents.counters(admin=True)
            with self.assertRaises(PermissionError):
                self.p.documents.counters()

    def test_other_office_grants_do_not_grant_receipts(self):
        key = self.create()
        with patch.object(self.p.office_access, "identity", return_value={"actor": "mitarbeiter:1", "office_categories": ("termine",)}):
            self.assertEqual(self.office.get("/werkstatt/buero/belege").status_code, 403)
            self.assertEqual(self.office.get("/werkstatt/buero/belege/" + key + "/original").status_code, 403)
            self.assertEqual(self.action(key, "einreichen").status_code, 403)
            with self.p.app.test_request_context():
                session["office_mid"] = 1
                with self.assertRaises(PermissionError):
                    self.p.documents.counters()

    def test_private_intake_has_no_fake_session_and_is_chef_only_until_review(self):
        raw = pdf_bytes("synthetic-private-intake")
        payload = {k: v for k, v in self.fields(typ="post", betrag_brutto="").items() if k != "csrf_token"}
        with self.p.app.app_context():
            key, duplicate = self.p.documents.create_from_intake(payload, FileStorage(stream=io.BytesIO(raw), filename="synthetic.pdf"),
                                                               source_ref="synthetic-mailbox:001:attachment", bereich="privat")
            self.assertFalse(duplicate)
            snapshot, original = self.p.documents.intake_snapshot(key)
            self.assertEqual(original, raw)
            self.assertEqual(snapshot["bereich"], "privat")
            self.assertEqual(self.p.documents.find_by_sha256(hashlib.sha256(raw).hexdigest())["id"], key)
            self.p.documents.record_scan_suggestion(key, "1", "privat", "Synthetic private classification", "post", intake=True)
        self.assertEqual((self.row(key)["status"], self.row(key)["payable"], self.row(key)["business_confirmed"]), ("eingereicht", 0, 0))
        self.assertEqual(self.counts(admin=True)["kategorien"]["privat"]["neue_anzahl"], 1)
        for client in (self.office,):
            self.assertEqual(client.get("/werkstatt/buero/belege/" + key).status_code, 404)
            self.assertEqual(client.get("/werkstatt/buero/belege/" + key + "/original").status_code, 404)
        self.assertEqual(self.counts()["kategorien"]["privat"]["gesamt"], 0)
        self.assertEqual(self.admin.get("/admin/buero/belege/" + key).status_code, 200)
        self.assertEqual(self.counts(admin=True)["neue_anzahl"], 0)
        self.assertEqual(self.counts(admin=True)["offen_anzahl"], 1)
        regular = self.create(raw=pdf_bytes("not-intake"))
        with self.p.app.app_context():
            with self.assertRaises(PermissionError):
                self.p.documents.intake_snapshot(regular)
            with self.assertRaises(PermissionError):
                self.p.documents.record_scan_suggestion(regular, "1", "privat", "Synthetic", intake=True)

    def test_private_release_is_visible_to_office_but_never_business_lexware(self):
        key = self.create(raw=pdf_bytes("private-invoice"))
        other = self.p.app.test_client()
        self.login(other, mid=2)
        self.assertEqual(other.get("/werkstatt/buero/belege/" + key).status_code, 404)
        self.assertEqual(self.office.get("/werkstatt/buero/belege/" + key).status_code, 200)
        self.submit(key)
        calls = []
        self.p.office_document_approved = calls.append
        self.approve(key, bereich="privat")
        row = self.row(key)
        self.assertEqual((row["bereich"], row["payable"], row["business_confirmed"]), ("privat", 1, 0))
        self.assertEqual(calls, [])
        self.assertEqual(self.counts(mid=2)["kategorien"]["privat"]["zahlung_offen"], 1)
        response = other.get("/werkstatt/buero/belege/" + key)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Privater Beleg", response.get_data(as_text=True))
        with self.p.app.test_request_context():
            session["admin"] = True
            with self.assertRaises(ValueError):
                self.p.documents.record_lexware_reference(key, str(row["revision"]), "synthetic-private", "https://app.lexware.de/vouchers/synthetic-private")
        self.assertEqual(self.action(key, "bezahlt", client=other, zahlungsdatum="2026-01-05", zahlungsreferenz="synthetic-private-reference").status_code, 303)
        self.assertEqual((self.row(key)["status"], self.row(key)["business_confirmed"]), ("bezahlt", 0))

    def test_hooks_run_after_commit_and_failure_never_undoes_approval_or_duplicates(self):
        uploaded, approved = [], []
        def upload_hook(key):
            uploaded.append(key)
            self.assertEqual(self.row(key)["status"], "entwurf")
            raise RuntimeError("synthetic provider secret must not appear")
        def approve_hook(key):
            approved.append(key)
            self.assertEqual(self.row(key)["status"], "freigegeben")
            raise RuntimeError("synthetic provider secret must not appear")
        self.p.office_document_uploaded = upload_hook
        self.p.office_document_approved = approve_hook
        raw = pdf_bytes("hook-failure")
        key = self.create(raw=raw)
        self.assertEqual(uploaded, [key])
        self.assertIn("Scan-Analyse", self.row(key)["verarbeitung_hinweis"])
        self.assertNotIn("secret", self.row(key)["verarbeitung_hinweis"])
        self.upload(raw=raw)
        self.assertEqual(uploaded, [key])
        self.submit(key)
        self.approve(key)
        row = self.row(key)
        self.assertEqual((row["status"], row["payable"], row["business_confirmed"], row["paid_date"]), ("freigegeben", 1, 1, ""))
        self.assertEqual(approved, [key])
        self.assertIn("Lexware-Übertragung", row["verarbeitung_hinweis"])
        self.assertEqual(self.action(key, "freigeben", admin=True, version="1").status_code, 409)
        self.assertEqual(approved, [key])
        self.assertEqual(len(self.rows()), 1)

    def test_chef_can_correct_automatic_intake_without_bypassing_assignment(self):
        payload = {k: v for k, v in self.fields(typ="post", belegnummer="", betrag_brutto="").items() if k != "csrf_token"}
        with self.p.app.app_context():
            key, _ = self.p.documents.create_from_intake(payload, FileStorage(stream=io.BytesIO(pdf_bytes("chef-correction")), filename="synthetic.pdf"),
                                                       source_ref="synthetic-intake-correction")
        data = self.fields(typ="rechnung", betrag_brutto="72.00")
        data["revision"] = str(self.row(key)["revision"])
        self.assertEqual(self.admin.post("/admin/buero/belege/" + key + "/bearbeiten", data=data).status_code, 303)
        self.assertEqual((self.row(key)["status"], self.row(key)["bereich"], self.row(key)["payable"]), ("eingereicht", "unklar", 0))
        self.assertEqual(self.action(key, "freigeben", admin=True, bereich="unklar").status_code, 400)
        self.approve(key, bereich="geschaeftlich")
        self.assertEqual((self.row(key)["bereich"], self.row(key)["business_confirmed"]), ("geschaeftlich", 1))

    @staticmethod
    def order_fields(**changes):
        data = dict(typ="zahlungsauftrag", kategorie="miete", lieferant="", belegnummer="", belegdatum="", betrag_brutto="1500.00",
                    faelligkeit="2026-11-03", zahlungsempfaenger="Synthetic Landlord", verwendungszweck="Synthetic rent November 2026",
                    iban="DE89 3704 0044 0532 0130 00", zahlungsmodus="monatliche_miete", notiz="Synthetic reviewed contract basis")
        data.update(changes)
        return data

    def test_rent_payment_order_is_not_an_invoice_and_never_auto_uploads(self):
        uploaded, approved = [], []
        self.p.office_document_uploaded = uploaded.append
        self.p.office_document_approved = approved.append
        key = self.create(raw=pdf_bytes("synthetic-rental-contract"), **self.order_fields())
        self.assertEqual(uploaded, [])
        self.assertEqual(self.row(key)["typ"], "zahlungsauftrag")
        self.assertEqual(self.row(key)["betrag_cent"], 150000)
        self.submit(key)
        self.assertEqual(self.action(key, "freigeben", admin=True).status_code, 400)
        self.approve(key, zahlungspflichtig="ja")
        row = self.row(key)
        self.assertEqual((row["status"], row["payable"], row["quelle_geprueft"]), ("freigegeben", 1, 1))
        self.assertEqual(approved, [])
        self.assertEqual(row["lexware_beleg_id"], "")
        page = self.office.get("/werkstatt/buero/belege/" + key).get_data(as_text=True)
        self.assertIn("Zahlungsauftrag – keine Rechnung", page)
        self.assertIn("Synthetic Landlord", page)
        self.assertIn("keine Umsatzsteuer", page)
        with self.p.app.test_request_context():
            session["admin"] = True
            with self.assertRaises(ValueError):
                self.p.documents.record_lexware_reference(key, str(row["revision"]), "synthetic-rent", "https://app.lexware.de/vouchers/synthetic-rent")
        self.assertEqual(self.action(key, "bezahlt", zahlungsdatum="2026-01-05", zahlungsreferenz="synthetic-external-rent").status_code, 303)
        self.assertEqual(self.row(key)["betrag_cent"], 150000)
        self.assertEqual(len(self.rows()), 1)  # No automatic next-month task.

    def test_order_amount_and_source_mode_still_required_while_transfer_gaps_are_optional(self):
        for field in ("betrag_brutto", "zahlungsmodus"):
            key = self.create(raw=pdf_bytes("required-review-" + field), **self.order_fields(**{field:""}))
            self.assertEqual(self.action(key, "einreichen").status_code, 400)
            self.assertEqual((self.row(key)["status"], self.row(key)["payable"]), ("entwurf",0))
        for field in ("zahlungsempfaenger", "iban", "bic", "faelligkeit", "verwendungszweck"):
            key = self.create(raw=pdf_bytes("optional-transfer-" + field), **self.order_fields(**{field:""}))
            self.submit(key)
            self.approve(key, zahlungspflichtig="ja")
            row = self.row(key)
            self.assertEqual((row["status"],row["payable"],row[field]), ("freigegeben",1,""))
            self.assertEqual((row["paid_date"],row["payment_reference"]), ("",""))
        with self.p.app.app_context():
            key, _ = self.p.documents.create_from_intake(self.order_fields(betrag_brutto=""),
                FileStorage(stream=io.BytesIO(pdf_bytes("intake-no-amount")),filename="synthetic.pdf"),source_ref="synthetic-no-amount")
        self.assertEqual(self.action(key,"freigeben",admin=True,zahlungspflichtig="ja").status_code,400)
        self.assertEqual(self.row(key)["payable"],0)

    def test_same_contract_can_support_rent_and_deposit_without_duplicate_invoice(self):
        raw = pdf_bytes("shared-synthetic-contract")
        rent = self.create(raw=raw, **self.order_fields())
        duplicate = self.upload(raw=raw, **self.order_fields(betrag_brutto="999.00"))
        self.assertTrue(duplicate.headers["Location"].endswith(rent))
        self.assertEqual(self.row(rent)["betrag_cent"], 150000)
        deposit_fields = self.order_fields(kategorie="kaution", betrag_brutto="3000.00", zahlungsmodus="einzelzahlung",
                                          verwendungszweck="Synthetic contract deposit", notiz="One-off deposit required by synthetic contract")
        deposit = self.create(raw=raw, **deposit_fields)
        self.assertNotEqual(rent, deposit)
        self.assertEqual(self.row(rent)["sha256"], self.row(deposit)["sha256"])
        self.assertNotEqual(self.row(rent)["dedup_key"], self.row(deposit)["dedup_key"])
        self.assertEqual(base64.b64decode(self.row(deposit)["original_base64"]), raw)
        self.assertEqual({row["typ"] for row in self.rows()}, {"zahlungsauftrag"})
        self.submit(deposit)
        self.approve(deposit, zahlungspflichtig="ja", bereich="privat")
        self.assertEqual((self.row(deposit)["bereich"], self.row(deposit)["kategorie"], self.row(deposit)["business_confirmed"]), ("privat", "kaution", 0))
        self.assertEqual(self.row(rent)["betrag_cent"], 150000)  # No deposit offset.
        self.assertEqual(self.counts()["kategorien"]["privat"]["zahlung_offen"], 1)
        self.assertEqual(self.action(deposit, "bezahlt", zahlungsdatum="2026-01-05", zahlungsreferenz="synthetic-private-deposit").status_code, 303)
        self.assertEqual(len(self.rows()), 2)

    def test_single_payment_requires_reason_and_monthly_mode_is_rent_only(self):
        values = self.order_fields(kategorie="kaution", zahlungsmodus="einzelzahlung", notiz="")
        key = self.create(raw=pdf_bytes("unjustified-deposit"), **values)
        self.assertEqual(self.action(key, "einreichen").status_code, 400)
        self.assertEqual(self.upload(raw=pdf_bytes("wrong-template"), **self.order_fields(kategorie="kaution")).status_code, 400)
        valid = self.create(raw=pdf_bytes("checked-source"), **self.order_fields())
        self.submit(valid)
        with self.p.app.test_request_context():
            session["admin"] = True
            with self.assertRaises(ValueError):
                self.p.documents.decide(valid, str(self.row(valid)["revision"]), approve=True, payment_release=True,
                                        business_confirmed=True, bereich="geschaeftlich", source_confirmed=False)
        self.assertEqual(self.row(valid)["quelle_geprueft"], 0)

    def test_old_sqlite_hash_schema_migrates_without_changing_originals(self):
        raw = pdf_bytes("legacy-original")
        legacy = _beleg_ddl().replace("sha256 TEXT NOT NULL, original_base64", "sha256 TEXT NOT NULL UNIQUE, original_base64")
        legacy = legacy.replace("dedup_key TEXT NOT NULL DEFAULT '',", "").replace(",'zahlungsauftrag'", "")
        with closing(self.p.get_db()) as db:
            db.execute("DROP TABLE buero_belege")
            db.execute(legacy)
            db.execute("""INSERT INTO buero_belege(id,typ,filename,mime,size_bytes,sha256,original_base64,created_by,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)""", ("a" * 32, "rechnung", "synthetic.pdf", "application/pdf", len(raw), hashlib.sha256(raw).hexdigest(),
                                               base64.b64encode(raw).decode("ascii"), "mitarbeiter:1", "synthetic-created", "synthetic-updated"))
            db.commit()
            before = dict(db.execute("SELECT * FROM buero_belege").fetchone())
        self.p.documents.init_schema()
        after = self.row("a" * 32)
        for key, value in before.items():
            self.assertEqual(after[key], value, key)
        self.assertEqual(after["dedup_key"], "datei:" + hashlib.sha256(raw).hexdigest())
        self.assertEqual(self.office.get("/werkstatt/buero/belege/" + "a" * 32 + "/original").data, raw)
        payment = self.create(raw=raw, **self.order_fields())
        self.assertNotEqual(payment, "a" * 32)
        self.assertEqual(len(self.rows()), 2)
        self.p.documents.init_schema()  # Idempotent schema re-entry.
        self.assertEqual(len(self.rows()), 2)

    def test_postgres_schema_changes_only_old_hash_and_kind_constraints(self):
        class Cursor:
            def __init__(self, rows=()):
                self.rows = rows
            def fetchall(self):
                return self.rows
        class Database:
            def __init__(self):
                self.statements = []
            def execute(self, sql, params=()):
                self.statements.append(sql)
                if "FROM pg_constraint" in sql:
                    return Cursor([{"conname": "old_sha", "contype": "u", "definition": "UNIQUE (sha256)"},
                                   {"conname": "old_kind", "contype": "c", "definition": "CHECK (typ IN ('rechnung','post'))"},
                                   {"conname": "status_check", "contype": "c", "definition": "CHECK (status IN ('entwurf','eingereicht'))"}])
                return Cursor()
        db = Database()
        init_office_documents_schema(db, lambda *args: None, postgres=True)
        sql = "\n".join(db.statements)
        self.assertIn('DROP CONSTRAINT "old_sha"', sql)
        self.assertIn('DROP CONSTRAINT "old_kind"', sql)
        self.assertNotIn('DROP CONSTRAINT "status_check"', sql)
        self.assertIn("idx_buero_belege_dedup", sql)
        self.assertNotIn("PRAGMA", sql)

    def test_legacy_migration_refuses_unknown_columns_without_losing_originals(self):
        raw = pdf_bytes("legacy-with-extra-field")
        legacy = _beleg_ddl().replace("sha256 TEXT NOT NULL, original_base64", "sha256 TEXT NOT NULL UNIQUE, original_base64")
        legacy = legacy.replace(",'zahlungsauftrag'", "").replace("id TEXT PRIMARY KEY,", "id TEXT PRIMARY KEY, legacy_note TEXT DEFAULT 'retain this',", 1)
        with closing(self.p.get_db()) as db:
            db.execute("DROP TABLE buero_belege")
            db.execute(legacy)
            db.execute("""INSERT INTO buero_belege(id,typ,filename,mime,size_bytes,sha256,original_base64,created_by,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)""", ("b" * 32, "post", "synthetic.pdf", "application/pdf", len(raw), hashlib.sha256(raw).hexdigest(),
                                               base64.b64encode(raw).decode("ascii"), "mitarbeiter:1", "synthetic", "synthetic"))
            db.commit()
        with self.assertRaises(RuntimeError):
            self.p.documents.init_schema()
        with closing(self.p.get_db()) as db:
            row = dict(db.execute("SELECT * FROM buero_belege").fetchone())
            self.assertEqual(row["legacy_note"], "retain this")
            self.assertEqual(base64.b64decode(row["original_base64"]), raw)
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='buero_belege_schema_next'").fetchone())



    def test_transfer_normalization_rejects_invalid_bank_values_without_any_lookup(self):
        self.assertEqual(normalize_iban("de89 3704 0044 0532 0130 00"), "DE89370400440532013000")
        self.assertEqual(normalize_iban("GB82 WEST 1234 5698 7654 32"), "GB82WEST12345698765432")
        self.assertEqual(normalize_bic("test de ff xxx"), "TESTDEFFXXX")
        self.assertEqual(normalize_bic("1ESTDEFF"), "1ESTDEFF")
        self.assertEqual((normalize_iban(""), normalize_bic("")), ("", ""))
        for value in ("DE89370400440532013001", "DE8937040044053201300", "ZZ89370400440532013000", "DE89A70400440532013000", "DE00 3704 0044 0532 0130 00", None):
            with self.subTest(iban=value), self.assertRaises(ValueError): normalize_iban(value)
        for value in ("!ESTDEFF", "TEST12FF", "TESTDEFFXX", "TEST-DE-FF", "ßßDEFF", None):
            with self.subTest(bic=value), self.assertRaises(ValueError): normalize_bic(value)
        with self.assertRaises(ValueError): normalize_iban("", required=True)
        self.assertEqual(self.upload(iban="DE89370400440532013001").status_code, 400)
        self.assertEqual(self.upload(bic="BAD!DEFF").status_code, 400)
        self.assertEqual(self.rows(), [])

    def test_transfer_fields_are_protected_editable_and_audited_for_each_payable_kind(self):
        for kind in ("rechnung", "bescheid", "zahlungsauftrag"):
            payload = self.order_fields() if kind == "zahlungsauftrag" else dict(typ=kind, kategorie="rechnung" if kind == "rechnung" else "gebuehren")
            payload.update(zahlungsempfaenger="Synthetic checked payee", iban="de89 3704 0044 0532 0130 00", bic="testdeffxxx", verwendungszweck="Synthetic original reference")
            key = self.create(raw=pdf_bytes("bank-fields-" + kind), **payload)
            row = self.row(key)
            self.assertEqual((row["iban"], row["bic"]), ("DE89370400440532013000", "TESTDEFFXXX"))
            html = self.admin.get("/admin/buero/belege/" + key).get_data(as_text=True)
            for label in ("Überweisungsdaten", "Zahlungsempfänger", 'name="iban"', 'name="bic"', "Verwendungszweck"):
                self.assertIn(label, html)
            audit = json.loads(next(a for a in self.rows(TABLES[1]) if a["beleg_id"] == key)["details_json"])
            self.assertEqual((audit["metadata"]["iban"], audit["metadata"]["bic"]), (row["iban"], row["bic"]))
            self.assertEqual(self.anon.get("/admin/buero/belege/" + key).status_code, 403)
            self.assertEqual((row["status"], row["payable"], row["business_confirmed"]), ("entwurf", 0, 0))

    def test_all_transfer_gaps_remain_visible_but_allow_source_checked_order_approval(self):
        approved = Mock(); self.p.office_document_approved = approved
        empty = dict(zahlungsempfaenger="",iban="",bic="",faelligkeit="",verwendungszweck="")
        key = self.create(raw=pdf_bytes("optional-all-transfer"), **self.order_fields(**empty))
        before = self.row(key)
        response = self.action(key,"freigeben",admin=True,typ="zahlungsauftrag",kategorie="miete",zahlungspflichtig="ja")
        self.assertEqual(response.status_code,303,response.get_data(as_text=True))
        row = self.row(key)
        self.assertEqual((row["status"],row["revision"],row["payable"],row["quelle_geprueft"]), ("freigegeben",before["revision"]+1,1,1))
        self.assertTrue(all(row[name]=="" for name in empty))
        self.assertEqual((row["paid_date"],row["payment_reference"]), ("",""))
        with self.p.app.test_request_context():
            session["office_mid"]=1
            view=self.p.documents.detail(key)
        self.assertTrue(view["payment_allowed"])
        self.assertTrue(view["payment_data_pending"])
        self.assertEqual(set(view["transfer_missing"]),{"Zahlungsempfänger","IBAN","BIC","Fälligkeit","Verwendungszweck"})
        self.assertTrue(view["transfer_warning"].startswith("Nicht angegeben:"))
        approved.assert_not_called()

    def test_invalid_atomic_transfer_approval_rolls_back_and_keeps_user_input(self):
        key = self.create(raw=pdf_bytes("invalid-transfer"), **self.order_fields(zahlungsempfaenger="", iban=""))
        before, audits = self.row(key), len(self.rows(TABLES[1]))
        response = self.action(key, "freigeben", admin=True, typ="zahlungsauftrag", kategorie="miete", zahlungspflichtig="ja",
            zahlungsempfaenger="Synthetic unsaved payee", iban="DE89370400440532013001", bic="TESTDEFF",
            faelligkeit="2026-11-03", verwendungszweck="Synthetic unsaved purpose")
        self.assertEqual(response.status_code, 400)
        self.assertIn("IBAN-Prüfziffer ist ungültig", response.get_data(as_text=True))
        self.assertIn('value="Synthetic unsaved payee"', response.get_data(as_text=True))
        self.assertEqual((self.row(key), len(self.rows(TABLES[1]))), (before, audits))
        response = self.action(key, "freigeben", admin=True, version=str(before["revision"] + 1), typ="zahlungsauftrag", kategorie="miete",
                               zahlungspflichtig="ja", iban="DE89370400440532013000")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.row(key), before)

    def test_legacy_partial_edits_and_conflicting_scans_preserve_manual_bank_block(self):
        key = self.create(raw=pdf_bytes("manual-bank-preserve"), iban="DE89370400440532013000", bic="TESTDEFF", zahlungsempfaenger="Manual holder", verwendungszweck="Manual reference")
        response = self.office.post("/werkstatt/buero/belege/" + key + "/bearbeiten",
            data={"csrf_token":"synthetic-csrf", "revision":str(self.row(key)["revision"]), "notiz":"New synthetic note"})
        self.assertEqual(response.status_code, 303)
        revision = self.extract(key, {"iban":"GB82WEST12345698765432", "bic":"TESTGB22", "zahlungsempfaenger":"Other source holder", "verwendungszweck":"Other reference"})
        with self.p.app.test_request_context():
            session["office_mid"] = 1
            self.p.documents.record_scan_suggestion(key, str(revision), "rechnung", "Synthetic classification", "rechnung")
        row = self.row(key)
        self.assertEqual((row["iban"], row["bic"], row["zahlungsempfaenger"], row["verwendungszweck"]),
                         ("DE89370400440532013000", "TESTDEFF", "Manual holder", "Manual reference"))
        self.assertIn("Bankangaben passen nicht eindeutig", row["klassifizierung_hinweis"])
        for field, value in (("iban", "DE89370400440532013001"), ("bic", "BAD!DEFF")):
            response = self.office.post("/werkstatt/buero/belege/" + key + "/bearbeiten", data={
                "csrf_token":"synthetic-csrf", "revision":str(self.row(key)["revision"]), field:value})
            self.assertEqual(response.status_code, 400)
            self.assertEqual(self.row(key)["iban"], "DE89370400440532013000")
            self.assertEqual(self.row(key)["bic"], "TESTDEFF")
        for manual in ({"bic":"TESTDEFF"}, {"zahlungsempfaenger":"Manually chosen holder"}):
            another = self.create(raw=pdf_bytes("partial-manual-" + next(iter(manual))), **manual)
            self.extract(another, {"iban":"GB82WEST12345698765432", "zahlungsempfaenger":"Other source holder"})
            self.assertEqual(self.row(another)["iban"], "")

    def test_existing_eight_drafts_gain_empty_bank_columns_without_other_changes(self):
        for i in range(6):
            self.create(raw=pdf_bytes("old-photo-" + str(i)), typ="post", kategorie="sonstige")
        raw = pdf_bytes("old-shared-contract")
        self.create(raw=raw, **self.order_fields(iban=""))
        self.create(raw=raw, **self.order_fields(kategorie="kaution", iban="", zahlungsmodus="einzelzahlung", verwendungszweck="Synthetic old deposit"))
        before = {r["id"]:{k:v for k,v in r.items() if k not in ("iban","bic")} for r in self.rows()}
        old_audits, old_seen = self.rows(TABLES[1]), self.rows(TABLES[2])
        with closing(self.p.get_db()) as db:
            db.execute("ALTER TABLE buero_belege DROP COLUMN iban")
            db.execute("ALTER TABLE buero_belege DROP COLUMN bic")
            db.commit()
        self.p.documents.init_schema(); self.p.documents.init_schema()
        after = self.rows()
        self.assertEqual(len(after), 8)
        for row in after:
            self.assertEqual((row["iban"], row["bic"]), ("", ""))
            self.assertEqual({k:v for k,v in row.items() if k not in ("iban","bic")}, before[row["id"]])
            self.assertEqual((row["status"], row["payable"], row["business_confirmed"], row["quelle_geprueft"]), ("entwurf",0,0,0))
        self.assertEqual((self.rows(TABLES[1]), self.rows(TABLES[2])), (old_audits, old_seen))

    def test_payment_order_can_be_reclassified_as_nonpayable_without_hidden_rental_mode(self):
        key = self.create(raw=pdf_bytes("retire-mode"), **self.order_fields())
        response = self.action(key, "freigeben", admin=True, typ="post", kategorie="sonstige")
        self.assertEqual(response.status_code, 303, response.get_data(as_text=True))
        row = self.row(key)
        self.assertEqual((row["typ"],row["zahlungsmodus"],row["payable"],row["quelle_geprueft"]), ("post","",0,0))
        self.assertEqual(row["iban"], "DE89370400440532013000")


    def test_transfer_warnings_cover_every_kind_and_bic_only_is_informational(self):
        empty=dict(zahlungsempfaenger="",iban="",bic="",faelligkeit="",verwendungszweck="")
        for kind in ("rechnung","bescheid","lieferschein","post"):
            key=self.create(raw=pdf_bytes("all-kinds-transfer-"+kind),typ=kind,**empty)
            with self.p.app.test_request_context():
                session["office_mid"]=1
                view=self.p.documents.detail(key)
            self.assertEqual(len(view["transfer_missing"]),5)
            self.assertTrue(view["payment_data_pending"])
            self.assertFalse(view["payment_allowed"])
        key=self.create(raw=pdf_bytes("only-bic-absent"),zahlungsempfaenger="Synthetic payee",iban="DE89370400440532013000",
                        bic="",verwendungszweck="Synthetic payment reference")
        with self.p.app.test_request_context():
            session["office_mid"]=1
            view=self.p.documents.detail(key)
        self.assertEqual(view["transfer_missing"],["BIC"])
        self.assertFalse(view["payment_data_pending"])

    def test_private_single_order_without_bank_data_is_local_work_then_explicit_external_record(self):
        original=pdf_bytes("synthetic-private-travel-source")
        empty=dict(zahlungsempfaenger="",iban="",bic="",faelligkeit="",verwendungszweck="")
        payload=self.order_fields(kategorie="privat",zahlungsmodus="einzelzahlung",
            notiz="Synthetic user-confirmed private payment intent and checked source total",**empty)
        approved=Mock();self.p.office_document_approved=approved
        key=self.create(raw=original,**payload);before=self.row(key)
        # Wrong area, no source confirmation, missing amount and bad bank input
        # remain failures even though the five transfer gaps are permitted.
        bad_area=self.action(key,"freigeben",admin=True,typ="zahlungsauftrag",kategorie="privat",bereich="geschaeftlich",zahlungspflichtig="ja")
        self.assertEqual(bad_area.status_code,400);self.assertEqual(self.row(key),before)
        for changes in ({"zahlungspflichtig":""},{"iban":"DE89370400440532013001"},{"bic":"BAD!DEFF"}):
            args=dict(typ="zahlungsauftrag",kategorie="privat",bereich="privat",zahlungspflichtig="ja");args.update(changes)
            self.assertEqual(self.action(key,"freigeben",admin=True,**args).status_code,400)
            self.assertEqual(self.row(key),before)
        response=self.action(key,"freigeben",admin=True,typ="zahlungsauftrag",kategorie="privat",bereich="privat",zahlungspflichtig="ja")
        self.assertEqual(response.status_code,303,response.get_data(as_text=True))
        row=self.row(key)
        self.assertEqual((row["typ"],row["kategorie"],row["bereich"],row["status"],row["payable"],row["business_confirmed"],row["quelle_geprueft"]),
                         ("zahlungsauftrag","privat","privat","freigegeben",1,0,1))
        self.assertEqual(row["sha256"],hashlib.sha256(original).hexdigest())
        self.assertTrue(all(row[name]=="" for name in empty))
        self.assertEqual((row["paid_date"],row["payment_reference"],row["lexware_beleg_id"]),("","",""))
        self.assertEqual(self.action(key,"ueberweisung",version=str(before["revision"])).status_code,409)
        self.assertEqual(self.action(key,"ueberweisung",confirmed="").status_code,400)
        self.assertEqual(self.action(key,"ueberweisung").status_code,303)
        working=self.row(key)
        self.assertEqual((working["status"],working["paid_date"],working["payment_reference"]),("zahlung_in_bearbeitung","",""))
        self.assertEqual(self.action(key,"bezahlt",zahlungsdatum="2026-01-05",zahlungsreferenz="").status_code,400)
        self.assertEqual(self.action(key,"bezahlt",confirmed="",zahlungsdatum="2026-01-05",zahlungsreferenz="Synthetic external proof").status_code,400)
        self.assertEqual(self.row(key),working)
        self.assertEqual(self.action(key,"bezahlt",zahlungsdatum="2026-01-05",zahlungsreferenz="Synthetic external proof").status_code,303)
        paid=self.row(key)
        self.assertEqual((paid["status"],paid["paid_date"],paid["payment_reference"]),("bezahlt","2026-01-05","Synthetic external proof"))
        self.assertTrue(all(paid[name]=="" for name in empty))
        self.assertEqual(paid["lexware_beleg_id"],"")
        actions=[a["aktion"] for a in self.rows(TABLES[1]) if a["beleg_id"]==key]
        self.assertIn("ueberweisung_extern_in_bearbeitung",actions)
        self.assertIn("zahlung_dokumentiert",actions)
        approved.assert_not_called()  # The host's Lexware callback is not invoked.

    def test_private_order_requires_positive_source_amount_reason_and_original_integrity(self):
        for invalid in ({"betrag_brutto":""},{"notiz":""}):
            payload=self.order_fields(kategorie="privat",zahlungsmodus="einzelzahlung",iban="",**invalid)
            key=self.create(raw=pdf_bytes("bad-private-"+next(iter(invalid))),**payload)
            before=self.row(key)
            response=self.action(key,"freigeben",admin=True,typ="zahlungsauftrag",kategorie="privat",bereich="privat",zahlungspflichtig="ja")
            self.assertEqual(response.status_code,400);self.assertEqual(self.row(key),before)
        self.assertEqual(self.upload(raw=pdf_bytes("private-monthly"),**self.order_fields(kategorie="privat")).status_code,400)
        key=self.create(raw=pdf_bytes("private-original-integrity"),**self.order_fields(kategorie="privat",zahlungsmodus="einzelzahlung",iban=""))
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_belege SET original_base64=? WHERE id=?",(base64.b64encode(pdf_bytes("different-original")).decode(),key));db.commit()
        before=self.row(key)
        self.assertEqual(self.action(key,"freigeben",admin=True,typ="zahlungsauftrag",kategorie="privat",bereich="privat",zahlungspflichtig="ja").status_code,400)
        self.assertEqual(self.row(key),before)
        self.assertEqual(self.action(key,"freigeben",admin=True,client=self.office,typ="zahlungsauftrag",kategorie="privat",bereich="privat",zahlungspflichtig="ja").status_code,403)

    def test_private_single_order_same_source_cannot_duplicate_when_transfer_values_change(self):
        raw=pdf_bytes("one-private-source")
        payload=self.order_fields(kategorie="privat",zahlungsmodus="einzelzahlung",iban="",zahlungsempfaenger="",faelligkeit="",verwendungszweck="")
        key=self.create(raw=raw,**payload)
        response=self.upload(raw=raw,**dict(payload,zahlungsempfaenger="New synthetic payee",iban="DE89370400440532013000",
                                          faelligkeit="2026-11-04",verwendungszweck="New reference"))
        self.assertEqual(response.status_code,303)
        self.assertTrue(response.headers["Location"].endswith(key))
        self.assertEqual(len(self.rows()),1)
        self.assertEqual((self.row(key)["zahlungsempfaenger"],self.row(key)["iban"],self.row(key)["faelligkeit"]),("","",""))

    def test_reviewed_private_post_can_become_one_source_checked_private_payment_on_same_original(self):
        raw = pdf_bytes("reviewed-private-post-payment")
        key = self.create(raw=raw, typ="post", kategorie="privat", zahlungsempfaenger="", iban="", bic="", faelligkeit="", verwendungszweck="")
        hook = Mock(); self.p.office_document_approved = hook
        self.approve(key, typ="post", kategorie="privat", bereich="privat", chef_notiz="Synthetic original reviewed")
        before, history = self.row(key), self.rows(TABLES[1])
        response = self.action(key, "freigeben", admin=True, typ="zahlungsauftrag", kategorie="privat", bereich="privat",
                               zahlungsmodus="einzelzahlung", zahlungspflichtig="ja", chef_notiz="Synthetic source total and private payment intent checked")
        self.assertEqual(response.status_code, 303, response.get_data(as_text=True))
        row = self.row(key)
        self.assertEqual((row["id"], row["sha256"], row["original_base64"], row["size_bytes"]),
                         (before["id"], hashlib.sha256(raw).hexdigest(), before["original_base64"], before["size_bytes"]))
        self.assertEqual((row["typ"], row["kategorie"], row["bereich"], row["status"], row["payable"], row["quelle_geprueft"], row["business_confirmed"]),
                         ("zahlungsauftrag", "privat", "privat", "freigegeben", 1, 1, 0))
        self.assertEqual(row["revision"], before["revision"] + 1)
        self.assertEqual((row["paid_date"], row["payment_reference"], row["lexware_beleg_id"]), ("", "", ""))
        self.assertTrue(all(row[name] == "" for name in ("zahlungsempfaenger", "iban", "bic", "faelligkeit", "verwendungszweck")))
        after_history = self.rows(TABLES[1])
        self.assertTrue(all(entry in after_history for entry in history))
        followups = [json.loads(entry["details_json"]) for entry in after_history if json.loads(entry["details_json"]).get("private_post_zahlungsfreigabe")]
        self.assertEqual(len(followups), 1)
        self.assertEqual((followups[0]["vorher_status"], followups[0]["original_sha256"]), ("freigegeben", row["sha256"]))
        self.assertEqual(len(self.rows()), 1)
        with self.p.app.test_request_context():
            session["office_mid"] = 1
            view = self.p.documents.detail(key)
        self.assertTrue(view["payment_allowed"])
        self.assertTrue(view["payment_data_pending"])
        self.assertEqual(self.action(key, "freigeben", admin=True, typ="zahlungsauftrag", kategorie="privat", bereich="privat",
                                     zahlungsmodus="einzelzahlung", zahlungspflichtig="ja").status_code, 409)
        self.assertEqual(self.row(key), row)
        hook.assert_not_called()

    def test_reviewed_private_post_payment_requires_exact_transition_and_fresh_explicit_confirmation(self):
        key = self.create(raw=pdf_bytes("private-post-guard-matrix"), typ="post", kategorie="privat")
        old_revision = self.row(key)["revision"]
        self.approve(key, typ="post", kategorie="privat", bereich="privat")
        before, history = self.row(key), self.rows(TABLES[1])
        exact = dict(typ="zahlungsauftrag", kategorie="privat", bereich="privat", zahlungsmodus="einzelzahlung", zahlungspflichtig="ja")
        for change in ({"typ":"post"}, {"typ":"rechnung"}, {"typ":"bescheid"}, {"kategorie":"miete"},
                       {"bereich":"geschaeftlich"}, {"zahlungsmodus":"monatliche_miete"}, {"zahlungsmodus":""}):
            with self.subTest(change=change):
                self.assertEqual(self.action(key, "freigeben", admin=True, **dict(exact, **change)).status_code, 409)
                self.assertEqual(self.row(key), before)
                self.assertEqual(self.rows(TABLES[1]), history)
        for change in ({"zahlungspflichtig":""}, {"confirmed":""}, {"iban":"DE89370400440532013001"}, {"bic":"BAD!DEFF"}):
            self.assertEqual(self.action(key, "freigeben", admin=True, **dict(exact, **change)).status_code, 400)
            self.assertEqual(self.row(key), before)
        self.assertEqual(self.action(key, "freigeben", admin=True, version=str(old_revision), **exact).status_code, 409)
        self.assertEqual(self.action(key, "freigeben", admin=True, client=self.office, **exact).status_code, 403)
        self.assertEqual(self.action(key, "zurueckweisen", admin=True, chef_notiz="No general reopening").status_code, 409)
        self.assertEqual(self.row(key), before)
        self.assertEqual(self.rows(TABLES[1]), history)
        with closing(self.p.get_db()) as db:
            db.execute("UPDATE buero_belege SET original_base64=? WHERE id=?", (base64.b64encode(pdf_bytes("tampered-private-original")).decode(), key)); db.commit()
        corrupt = self.row(key)
        self.assertEqual(self.action(key, "freigeben", admin=True, **exact).status_code, 400)
        self.assertEqual(self.row(key), corrupt)
        self.assertEqual(self.rows(TABLES[1]), history)

    def test_private_post_followup_cannot_reopen_other_approved_kinds_areas_or_payment_states(self):
        exact = dict(typ="zahlungsauftrag", kategorie="privat", bereich="privat", zahlungsmodus="einzelzahlung", zahlungspflichtig="ja")
        for kind, area in (("post", "geschaeftlich"), ("lieferschein", "privat"), ("bescheid", "privat"), ("rechnung", "privat")):
            key = self.create(raw=pdf_bytes("not-private-post-" + kind + area), typ=kind, kategorie="privat" if area == "privat" else "sonstige")
            self.approve(key, typ=kind, kategorie="privat" if area == "privat" else "sonstige", bereich=area)
            before, history = self.row(key), self.rows(TABLES[1])
            self.assertEqual(self.action(key, "freigeben", admin=True, **exact).status_code, 409)
            self.assertEqual(self.row(key), before)
            self.assertEqual(self.rows(TABLES[1]), history)
        key = self.create(raw=pdf_bytes("approved-private-order-is-closed"), **self.order_fields(kategorie="privat", zahlungsmodus="einzelzahlung"))
        self.approve(key, **exact)
        for state in ("freigegeben", "zahlung_in_bearbeitung", "bezahlt"):
            if state == "zahlung_in_bearbeitung":
                self.assertEqual(self.action(key, "ueberweisung").status_code, 303)
            elif state == "bezahlt":
                self.assertEqual(self.action(key, "bezahlt", zahlungsdatum="2026-01-05", zahlungsreferenz="Synthetic external payment").status_code, 303)
            before, history = self.row(key), self.rows(TABLES[1])
            self.assertEqual(before["status"], state)
            self.assertEqual(self.action(key, "freigeben", admin=True, **exact).status_code, 409)
            self.assertEqual(self.row(key), before)
            self.assertEqual(self.rows(TABLES[1]), history)

    def test_private_post_followup_competing_requests_keep_one_revision_and_one_payment_approval(self):
        key = self.create(raw=pdf_bytes("private-post-competing-followup"), typ="post", kategorie="privat")
        self.approve(key, typ="post", kategorie="privat", bereich="privat")
        before = self.row(key)
        def review(kind):
            client = self.p.app.test_client(); self.login(client, admin=True)
            return self.action(key, "freigeben", admin=True, client=client, version=str(before["revision"]), typ=kind,
                               kategorie="privat", bereich="privat", zahlungsmodus="einzelzahlung", zahlungspflichtig="ja").status_code
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sorted(pool.map(review, ("zahlungsauftrag", "zahlungsauftrag"))), [303, 409])
        self.assertEqual(self.row(key)["revision"], before["revision"] + 1)
        self.assertEqual((self.row(key)["typ"], self.row(key)["payable"]), ("zahlungsauftrag", 1))
        key = self.create(raw=pdf_bytes("private-post-conflicting-normal-approval"), typ="post", kategorie="privat")
        self.approve(key, typ="post", kategorie="privat", bereich="privat")
        before = self.row(key)
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(review, ("post", "zahlungsauftrag"))), [409, 303])
        self.assertEqual(self.row(key)["revision"], before["revision"] + 1)

    def test_private_post_followup_html_keeps_failed_transfer_entries_and_can_correct_same_record(self):
        class ReviewForm(HTMLParser):
            def __init__(self, html):
                super().__init__(); self.active = False; self.action = ""; self.fields = {}; self.select = ""; self.textarea = ""
                self.feed(html)
            def handle_starttag(self, tag, attrs):
                attrs = dict(attrs)
                if tag == "form":
                    self.active = "bb-chef-review" in attrs.get("class", "").split()
                    if self.active: self.action = attrs["action"]
                if not self.active: return
                if tag == "input" and attrs.get("name") and attrs.get("type") != "checkbox":
                    self.fields[attrs["name"]] = attrs.get("value", "")
                elif tag == "select":
                    self.select = attrs["name"]; self.fields[self.select] = ""
                elif tag == "option" and self.select and "selected" in attrs:
                    self.fields[self.select] = attrs.get("value", "")
                elif tag == "textarea":
                    self.textarea = attrs["name"]; self.fields[self.textarea] = ""
            def handle_endtag(self, tag):
                if tag == "form": self.active = False
                elif tag == "select": self.select = ""
                elif tag == "textarea": self.textarea = ""
            def handle_data(self, data):
                if self.active and self.textarea: self.fields[self.textarea] += data
        key = self.create(raw=pdf_bytes("private-followup-html-correction"), typ="post", kategorie="privat")
        self.approve(key, typ="post", kategorie="privat", bereich="privat")
        before, history = self.row(key), self.rows(TABLES[1])
        url = "/admin/buero/belege/" + key
        initial = ReviewForm(self.admin.get(url).get_data(as_text=True))
        self.assertEqual(initial.action, url + "/freigeben")
        self.assertEqual((initial.fields["typ"], initial.fields["kategorie"], initial.fields["zahlungsmodus"]),
                         ("zahlungsauftrag", "privat", "einzelzahlung"))
        self.assertIn(initial.fields["bereich"], ("", "privat"))
        invalid = dict(initial.fields, bereich="privat", confirmed="ja", zahlungspflichtig="ja", iban="DE89370400440532013001",
                       zahlungsempfaenger="Synthetic payee & source", verwendungszweck="Synthetic private reference")
        response = self.admin.post(initial.action, data=invalid)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.row(key), before)
        self.assertEqual(self.rows(TABLES[1]), history)
        correction = ReviewForm(response.get_data(as_text=True))
        self.assertEqual(correction.action, initial.action)
        self.assertEqual(correction.fields["revision"], str(before["revision"]))
        for field in ("iban", "zahlungsempfaenger", "verwendungszweck"):
            self.assertEqual(correction.fields[field], invalid[field])
        correction.fields.update(confirmed="ja", zahlungspflichtig="ja", iban="DE89370400440532013000")
        self.assertEqual(self.admin.post(correction.action, data=correction.fields).status_code, 303)
        row = self.row(key)
        self.assertEqual((row["id"], row["sha256"], row["revision"]), (key, before["sha256"], before["revision"] + 1))
        self.assertEqual((row["status"], row["typ"], row["payable"], row["iban"]), ("freigegeben", "zahlungsauftrag", 1, "DE89370400440532013000"))
        self.assertEqual((row["paid_date"], row["payment_reference"], row["lexware_beleg_id"]), ("", "", ""))

if __name__ == "__main__":
    unittest.main()
