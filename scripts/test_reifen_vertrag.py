"""Offline contract-copy, withdrawal and durable-delivery regression tests."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from flask import Flask
from werkzeug.datastructures import MultiDict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import reifen_vertrag as contract


def order():
    return {"id": "synthetic-order-001", "kunde": {"name": "Anna Beispiel", "email": "anna@example.invalid"},
        "produkt": {"artikel_id": "DEMO-A", "bezeichnung": "Beispielreifen", "groesse": "205/55 R16 91V",
                    "preis": {"gesamt_reifen_brutto": "418.88", "versand_brutto": "11.90", "montage_brutto": "90.00", "gesamt_brutto": "520.78"}},
        "anzahl": 4, "amount_total": 52078, "currency": "eur", "zahlungsart": "vor_ort",
        "status": "bestellt_zahlung_offen", "bestaetigt": True, "zahlung_status": "vor_ort_offen",
        "accepted_at": "2026-11-01T15:30:00+00:00",
        "montage_datum": "2026-11-07", "montage_uhrzeit": "09:00",
        "rechtstexte": {"version": "test:1", "agb": "Vollständige AGB-Testfassung\nZweiter Absatz.",
                        "widerruf": "Vollständige Widerrufs-Testfassung.", "datenschutz": "Vollständige Datenschutz-Testfassung.",
                        "lieferung": "Vollständige Liefer-Testfassung."}}


class TireContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="reifen-contract-offline-")
        self.db_path = Path(self.temp.name) / "contract.sqlite3"
        self.clock = datetime(2026, 11, 1, 15, 30, tzinfo=timezone.utc)
        self.app = Flask(__name__, template_folder=str(ROOT / "templates"))
        self.app.config.update(TESTING=True, SECRET_KEY="synthetic-contract-test",
            REIFEN_CHECKOUT_MODE="off", REIFEN_VERTRAG_CLOCK=lambda: self.clock)
        self.portal = SimpleNamespace(app=self.app, get_db=self.db)
        self.network = patch("socket.socket.connect", side_effect=AssertionError("No real mail/network"))
        self.network.start()
        with closing(self.db()) as db:
            contract.init_schema(db)
            db.commit()
        contract.register_reifen_vertrag(self.portal)
        self.client = self.app.test_client()

    def tearDown(self):
        self.network.stop()
        self.temp.cleanup()

    def db(self):
        db = sqlite3.connect(self.db_path, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def rows(self, table):
        assert table in {"reifen_vertrag_outbox", "reifen_vertrag_anhaenge", "reifen_widerruf"}
        with closing(self.db()) as db:
            return [dict(row) for row in db.execute("SELECT * FROM " + table).fetchall()]

    def csrf(self, client=None):
        client = client or self.client
        client.get("/reifen/vertrag-widerrufen")
        with client.session_transaction() as session:
            return session["csrf_token"]

    def draft(self, **changes):
        data = {"csrf_token": self.csrf(), "name": "Anna Beispiel", "bestellnummer": "UNKNOWN-ORDER-001",
                "email": "anna@example.invalid", "umfang": "alles", "teil": ""}
        data.update(changes)
        response = self.client.post("/reifen/widerruf/pruefen", data=data)
        self.assertEqual(response.status_code, 200)
        identifier = re.search(r'/reifen/widerruf/bestaetigen/([a-f0-9]+)', response.get_data(as_text=True))[1]
        return identifier

    def confirm(self, identifier, client=None):
        client = client or self.client
        return client.post("/reifen/widerruf/bestaetigen/" + identifier,
                           data={"csrf_token": self.csrf(client)})

    def queue_order(self):
        with closing(self.db()) as db:
            identifier = contract.enqueue(db, order(), "bestellbestaetigung")
            db.commit()
        return identifier

    def enable_mail(self):
        self.app.config.update(REIFEN_VERTRAG_MAIL_FREIGABE=lambda: True,
                               REIFEN_SMTP_FROM="shop@example.invalid")

    def test_public_withdrawal_has_no_login_otp_supplier_or_checkout_gate(self):
        response = self.client.get("/reifen/vertrag-widerrufen")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Vertrag widerrufen", response.get_data(as_text=True))
        identifier = self.draft()
        self.assertEqual(self.confirm(identifier).status_code, 302)
        self.assertEqual(len(self.rows("reifen_vertrag_outbox")), 1)

    def test_review_step_does_not_submit_or_queue_mail(self):
        self.draft()
        self.assertEqual(self.rows("reifen_widerruf")[0]["confirmed_at"], "")
        self.assertEqual(self.rows("reifen_vertrag_outbox"), [])

    def test_confirmation_records_exact_declaration_time_and_is_idempotent(self):
        identifier = self.draft(umfang="teilweise", teil="Zwei hintere Reifen")
        self.confirm(identifier)
        original = self.rows("reifen_widerruf")[0]["confirmed_at"]
        self.clock += timedelta(minutes=4)
        with patch.object(contract, "FOOTER_TEXT", "Changed future sender details"):
            self.confirm(identifier)
        self.assertEqual(self.rows("reifen_widerruf")[0]["confirmed_at"], original)
        outbox = self.rows("reifen_vertrag_outbox")
        self.assertEqual(len(outbox), 1)
        self.assertIn("Zwei hintere Reifen", outbox[0]["body"])
        self.assertIn("01.11.2026 16:30:00", outbox[0]["body"])

    def test_unknown_order_reference_is_accepted_without_enumeration(self):
        identifier = self.draft(bestellnummer="not-in-any-order-database")
        response = self.confirm(identifier)
        page = self.client.get(response.location)
        self.assertEqual(page.status_code, 200)
        self.assertIn("Widerruf eingegangen", page.get_data(as_text=True))

    def test_no_order_cancel_or_payment_side_effect(self):
        identifier = self.draft()
        self.confirm(identifier)
        with closing(self.db()) as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("auftraege", tables)
        self.assertNotIn("reifen_bestellungen", tables)

    def test_draft_and_receipt_are_session_bound(self):
        identifier = self.draft()
        foreign = self.app.test_client()
        self.assertEqual(self.confirm(identifier, foreign).status_code, 404)
        self.confirm(identifier)
        self.assertEqual(foreign.get("/reifen/widerruf/eingang/" + identifier).status_code, 404)

    def test_missing_bad_and_conflicting_csrf_are_rejected(self):
        token = self.csrf()
        for data in ({}, {"csrf_token": "bad"}, MultiDict([("csrf_token", token), ("csrf_token", "bad")])):
            with self.subTest(data=data):
                self.assertEqual(self.client.post("/reifen/widerruf/pruefen", data=data).status_code, 400)

    def test_duplicate_identical_csrf_is_allowed_but_duplicate_fields_are_not(self):
        token = self.csrf()
        data = MultiDict([("csrf_token", token), ("csrf_token", token), ("name", "A"),
                         ("bestellnummer", "ORDER-001"), ("email", "a@example.invalid"), ("umfang", "alles")])
        self.assertEqual(self.client.post("/reifen/widerruf/pruefen", data=data).status_code, 200)
        data.add("bestellnummer", "ORDER-002")
        self.assertEqual(self.client.post("/reifen/widerruf/pruefen", data=data).status_code, 400)

    def test_partial_declaration_requires_which_part(self):
        data = {"csrf_token": self.csrf(), "name": "Anna", "bestellnummer": "ORDER-001",
                "email": "a@example.invalid", "umfang": "teilweise", "teil": ""}
        self.assertEqual(self.client.post("/reifen/widerruf/pruefen", data=data).status_code, 400)

    def test_size_and_rate_limits_apply(self):
        self.assertEqual(self.client.post("/reifen/widerruf/pruefen", data={"teil": "x" * 20000}).status_code, 413)
        token = self.csrf()
        for _ in range(30):
            self.client.post("/reifen/widerruf/pruefen", data={"csrf_token": token})
        self.assertEqual(self.client.post("/reifen/widerruf/pruefen", data={"csrf_token": token}).status_code, 429)

    def test_expired_unsubmitted_draft_needs_new_input(self):
        identifier = self.draft()
        self.clock += timedelta(minutes=31)
        self.assertEqual(self.confirm(identifier).status_code, 410)
        self.assertEqual(self.rows("reifen_vertrag_outbox"), [])

    def test_customer_text_is_html_escaped(self):
        identifier = self.draft(name='<img src=x onerror=alert(1)>')
        response = self.confirm(identifier)
        page = self.client.get(response.location).get_data(as_text=True)
        self.assertNotIn('<img src=x', page)
        self.assertIn('&lt;img', page)

    def test_contract_body_contains_full_immutable_legal_text_not_only_links(self):
        self.queue_order()
        row = self.rows("reifen_vertrag_outbox")[0]
        for text in order()["rechtstexte"].values():
            self.assertIn(text, row["body"])
        self.assertIn("520,78 EUR", row["body"])
        self.assertIn("4", row["body"])
        self.assertNotIn("einkauf", row["snapshot_json"])

    def test_verified_original_annex_is_stored_once_and_attached_unchanged(self):
        self.queue_order()
        other = order()
        other.update(id="synthetic-order-002", attachment_path="untrusted-and-ignored.jpg")
        with closing(self.db()) as db:
            contract.enqueue(db, other, "bestellbestaetigung")
            db.commit()
        blobs = self.rows("reifen_vertrag_anhaenge")
        self.assertEqual(len(blobs), 1)
        expected = "00e2b5ecc4f86bed59cc1ead77db7e98a318f264df97d77a01b5a160dee93138"
        self.assertEqual(blobs[0]["sha256"], expected)
        self.assertEqual(json.loads(self.rows("reifen_vertrag_outbox")[0]["snapshot_json"])["anhaenge"][0]["sha256"], expected)
        self.enable_mail()
        messages = []
        def sender(message):
            messages.append(message)
            return True
        self.assertEqual(contract.deliver_pending(self.portal, sender)["sent"], 2)
        for message in messages:
            attachments = list(message.iter_attachments())
            self.assertEqual(len(attachments), 1)
            self.assertEqual(attachments[0].get_content_type(), "image/jpeg")
            self.assertEqual(hashlib.sha256(attachments[0].get_payload(decode=True)).hexdigest(), expected)

    def test_delivery_uses_durable_blob_even_when_source_file_is_gone(self):
        self.queue_order()
        self.enable_mail()
        with patch.object(contract, "_ANNEX_PATH", Path(self.temp.name) / "missing-original.jpg"):
            self.assertEqual(contract.deliver_pending(self.portal, lambda message: True)["sent"], 1)

    def test_missing_or_corrupt_original_prevents_enqueuing_incomplete_contract(self):
        for corrupted in (False, True):
            with self.subTest(corrupted=corrupted):
                path = Path(self.temp.name) / ("bad-original.jpg" if corrupted else "missing.jpg")
                if corrupted:
                    path.write_bytes(b"\xff\xd8\xffnot-the-official-image")
                with patch.object(contract, "_ANNEX_PATH", path), closing(self.db()) as db, self.assertRaises(ValueError):
                    contract.enqueue(db, order(), "bestellbestaetigung")
                self.assertEqual(self.rows("reifen_vertrag_outbox"), [])

    def test_missing_or_corrupt_archived_blob_blocks_transport_without_retry(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt):
                with closing(self.db()) as db:
                    db.execute("DELETE FROM reifen_vertrag_outbox")
                    db.execute("DELETE FROM reifen_vertrag_anhaenge")
                    db.commit()
                self.queue_order()
                with closing(self.db()) as db:
                    if corrupt:
                        db.execute("UPDATE reifen_vertrag_anhaenge SET content_base64='Y29ycnVwdA=='")
                    else:
                        db.execute("DELETE FROM reifen_vertrag_anhaenge")
                    db.commit()
                self.enable_mail()
                messages = []
                first = contract.deliver_pending(self.portal, lambda message: messages.append(message))
                second = contract.deliver_pending(self.portal, lambda message: messages.append(message))
                self.assertEqual(first["unknown"], 1)
                self.assertEqual(second["unknown"], 0)
                self.assertEqual(messages, [])

    def test_enqueue_deduplicates_but_refuses_mutation(self):
        first = self.queue_order()
        self.assertEqual(self.queue_order(), first)
        changed = order()
        changed["rechtstexte"]["agb"] = "Changed legal terms"
        with closing(self.db()) as db, self.assertRaises(ValueError):
            contract.enqueue(db, changed, "bestellbestaetigung")
        self.assertEqual(len(self.rows("reifen_vertrag_outbox")), 1)

    def test_additional_contract_form_supplier_date_and_individual_declarations_are_retained(self):
        item = order()
        item["rechtstexte"].update(widerrufsformular="Vollständiges Muster-Widerrufsformular",
                                   anbieter={"firma": "Gärtner GmbH", "ort": "Mosbach"})
        item.update(lieferdatum="2026-11-06", accepted_at="2026-11-01T15:30:00+00:00",
                    erklaerungen={"frueher_montagebeginn": True, "kenntnis_montagewiderruf": True})
        with closing(self.db()) as db:
            contract.enqueue(db, item, "bestellbestaetigung")
            db.commit()
        row = self.rows("reifen_vertrag_outbox")[0]
        self.assertIn("Vollständiges Muster-Widerrufsformular", row["body"])
        self.assertIn("2026-11-06", row["body"])
        self.assertIn("frueher_montagebeginn", row["body"])
        self.assertEqual(json.loads(row["snapshot_json"])["rechtstexte"], item["rechtstexte"])

    def test_only_final_orders_receive_explicit_acceptance_and_correct_payment_text(self):
        self.queue_order()
        body = self.rows("reifen_vertrag_outbox")[0]["body"]
        self.assertIn("Wir nehmen Ihre folgende Bestellung verbindlich an.", body)
        self.assertIn("noch offen; Bezahlung vor Ort", body)
        item = order()
        item.update(id="paid-order-002", status="bestaetigt", zahlungsart="online", zahlung_status="bezahlt")
        with closing(self.db()) as db:
            contract.enqueue(db, item, "bestellbestaetigung")
            db.commit()
        self.assertTrue(any("bestätigt und bezahlt" in row["body"] for row in self.rows("reifen_vertrag_outbox")))
        item.update(id="unaccepted-order-003", bestaetigt=False, status="reserviert")
        with closing(self.db()) as db, self.assertRaises(ValueError):
            contract.enqueue(db, item, "bestellbestaetigung")

    def test_demo_copy_cannot_look_like_a_real_acceptance(self):
        item = order()
        item["demo"] = True
        with closing(self.db()) as db:
            contract.enqueue(db, item, "bestellbestaetigung")
            db.commit()
        row = self.rows("reifen_vertrag_outbox")[0]
        self.assertTrue(row["subject"].startswith("DEMO"))
        self.assertIn("keine echte Bestellung", row["body"])

    def test_missing_legal_version_or_recipient_injection_rejected(self):
        for change in (lambda item: item.pop("rechtstexte"),
                       lambda item: item["kunde"].update(email="a@example.invalid\nBcc: other@example.invalid")):
            item = order()
            change(item)
            with closing(self.db()) as db, self.assertRaises(ValueError):
                contract.enqueue(db, item, "bestellbestaetigung")

    def test_mail_gate_off_leaves_queue_untouched(self):
        self.queue_order()
        def forbidden(message):
            raise AssertionError("No send without explicit release")
        self.assertFalse(contract.deliver_pending(self.portal, forbidden)["enabled"])
        self.assertEqual(self.rows("reifen_vertrag_outbox")[0]["state"], "queued")

    def test_send_occurs_outside_database_lock_and_is_not_resent(self):
        self.queue_order()
        self.enable_mail()
        messages = []
        def sender(message):
            with closing(self.db()) as db:
                db.execute("UPDATE reifen_widerruf_limits SET attempts=attempts")
                db.commit()
            messages.append(message)
            return "sent"
        self.assertEqual(contract.deliver_pending(self.portal, sender)["sent"], 1)
        self.assertEqual(contract.deliver_pending(self.portal, sender)["sent"], 0)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["Message-ID"], self.rows("reifen_vertrag_outbox")[0]["message_id"])
        self.assertIn(order()["rechtstexte"]["agb"], messages[0].get_body(preferencelist=("plain",)).get_content())

    def test_ambiguous_send_and_definite_failure_are_distinct_and_not_retried(self):
        for outcome, state in (("uncertain", "unknown"), ("not_sent", "failed"), (False, "unknown")):
            with self.subTest(outcome=outcome):
                with closing(self.db()) as db:
                    db.execute("DELETE FROM reifen_vertrag_outbox")
                    db.commit()
                self.queue_order()
                self.enable_mail()
                calls = []
                def sender(message):
                    calls.append(message)
                    return outcome
                contract.deliver_pending(self.portal, sender)
                contract.deliver_pending(self.portal, sender)
                self.assertEqual(len(calls), 1)
                self.assertEqual(self.rows("reifen_vertrag_outbox")[0]["state"], state)

    def test_message_payload_tampering_is_detected_before_io(self):
        self.queue_order()
        self.enable_mail()
        with closing(self.db()) as db:
            db.execute("UPDATE reifen_vertrag_outbox SET body='tampered'")
            db.commit()
        messages = []
        result = contract.deliver_pending(self.portal, lambda message: messages.append(message))
        self.assertEqual(result["unknown"], 1)
        self.assertEqual(messages, [])

    def test_parallel_delivery_runners_do_not_duplicate_submission(self):
        self.queue_order()
        self.enable_mail()
        entered, release = threading.Event(), threading.Event()
        messages = []
        def sender(message):
            messages.append(message)
            entered.set()
            self.assertTrue(release.wait(5))
            return True
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(contract.deliver_pending, self.portal, sender)
            self.assertTrue(entered.wait(5))
            second = contract.deliver_pending(self.portal, sender)
            self.assertEqual(second["sent"], 0)
            release.set()
            self.assertEqual(first.result(timeout=5)["sent"], 1)
        self.assertEqual(len(messages), 1)


if __name__ == "__main__":
    unittest.main()
