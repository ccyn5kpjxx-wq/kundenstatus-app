"""Isolated demo checkout: verified phone, server prices and trusted payment.

No live supplier, SMS or financial provider is used. All application data lives
in a disposable database with the checkout's explicit demo suffix; threads and
network are blocked before app import. Fake providers declare demo_only=True.
"""
from __future__ import annotations

from copy import deepcopy
from contextlib import closing
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
with patch("threading.Thread.start"):
    import smoke_test as isolated

portal = isolated.portal
from reifen_bestellung import erstelle_auswahl_token, lade_auswahl_token


QUERY = {"groesse": "205/55 R16 91V", "saison": "sommer", "qualitaet": "premium",
         "anzahl": "4", "montage": "nein"}
SKU = "SYNTHETIC-TIRE-42"


class Clock:
    def __init__(self):
        self.value = datetime.now(timezone.utc)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


class Inputs(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.values = {}
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "input" and attrs.get("name"):
            self.values[attrs["name"]] = attrs.get("value", "")


class FakeSMS:
    demo_only = True

    def __init__(self):
        self.sent = []
        self.error = None
        self.result = True

    def send(self, phone, code, reference):
        self.sent.append({"phone": phone, "code": code, "reference": reference})
        if self.error:
            raise self.error
        return self.result


class FakeReservation:
    demo_only = True

    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.changes = {}
        self.error = None

    def reserve(self, order):
        self.calls.append(deepcopy(order))
        if self.error:
            raise self.error
        return {
            "reservierung_id": "demo_res_" + order["id"],
            "artikel_id": order["artikel_id"], "anzahl": order["anzahl"],
            "lieferdatum": (self.clock().date() + timedelta(days=3)).isoformat(),
            "gueltig_bis": (self.clock() + timedelta(minutes=15)).isoformat(),
            **self.changes,
        }


class FakeGateway:
    demo_only = True

    def __init__(self):
        self.created = []
        self.retrieved = []
        self.sessions = {}
        self.create_error = None
        self.retrieve_error = None

    def create(self, order):
        self.created.append(deepcopy(order))
        if self.create_error:
            raise self.create_error
        payment_id = "demo_pay_" + order["id"]
        self.sessions[payment_id] = {
            "id": payment_id, "status": "open", "livemode": False,
            "amount_total": order["amount_total"], "currency": order["currency"],
            "reference": order["id"], "quote_hash": order["quote_hash"],
        }
        return {"id": payment_id}

    def retrieve(self, payment_id):
        self.retrieved.append(payment_id)
        if self.retrieve_error:
            raise self.retrieve_error
        return deepcopy(self.sessions[payment_id])

    def paid(self, payment_id, **changes):
        self.sessions[payment_id].update(status="succeeded", **changes)


class ReifenBestellungTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
        source = Path(portal.DB).resolve()
        assert source.parent == isolated.TEMP_DIR.resolve(), "Only disposable smoke data may be copied"
        target = isolated.TEMP_DIR / "checkout.reifen-demo.sqlite3"
        # Preserve all module-created schemas for real admin/template rendering.
        with closing(sqlite3.connect(source)) as origin, closing(sqlite3.connect(target)) as destination:
            origin.backup(destination)
        cls.db_patch = patch.object(portal, "DB", target)
        cls.db_patch.start()
        portal.init_db()
        # This module normally initializes its schema during app import, before
        # the checkout fixture switches to the dedicated demo database.
        portal.check24_bookings.init_schema()
        cls.addClassCleanup(cls.db_patch.stop)
        assert "reifen_bestellung" in portal.app.blueprints, "Checkout blueprint must be wired by app.py"

    def setUp(self):
        db = portal.get_db()
        db.execute("DELETE FROM reifen_termin_slots")
        db.execute("DELETE FROM reifen_bestellungen")
        db.commit()
        db.close()
        self.clock = Clock()
        self.sms = FakeSMS()
        self.reservation = FakeReservation(self.clock)
        self.gateway = FakeGateway()
        self.article_changes = {}
        self.provider_calls = []
        self.client = portal.app.test_client()
        self.cfg = {
            "REIFENANGEBOT_PROVIDER": self.provider,
            "REIFEN_CHECKOUT_MODE": "demo",
            "REIFEN_CHECKOUT_SMS": self.sms,
            "REIFEN_CHECKOUT_RESERVIERUNG": self.reservation,
            "REIFEN_CHECKOUT_GATEWAY": self.gateway,
            "REIFEN_CHECKOUT_CLOCK": self.clock,
            "REIFEN_CHECKOUT_CODE_FACTORY": lambda: "610245",
            "REIFEN_CHECKOUT_TEXTE": {
                "version": "demo:1", "agb": "Synthetische Testbedingungen.",
                "widerruf": "Synthetischer Test-Widerruf.", "datenschutz": "Synthetische Testinformation.",
                "lieferung": "Synthetische Testlieferung zur Abholung.",
            },
        }
        cfg_patch = patch.dict(portal.app.config, self.cfg)
        cfg_patch.start()
        self.addCleanup(cfg_patch.stop)
        for name in ("lead_notice", "notify_lead_workshop", "schedule_change_backup",
                     "sende_website_lead_benachrichtigung"):
            provider = patch.object(portal, name)
            provider.start()
            self.addCleanup(provider.stop)
        self.clear_rate()

    def clear_rate(self):
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()

    def provider(self, query):
        self.provider_calls.append(deepcopy(query))
        return {"angebote": [{
            "artikel_id": SKU, "ean": "4000000000042", "bezeichnung": "Synthetischer Testreifen",
            "breite": 205, "querschnitt": 55, "zoll": 16, "lastindex": 91,
            "geschwindigkeitsindex": "V", "saison": "sommer", "qualitaet": "premium",
            "netto_einkauf": "100.00", "fracht_netto": "10.00", "waehrung": "EUR", "bestand": 8,
            "preis_bestaetigt": True, "abgerufen_am": self.clock().isoformat(),
            "gueltig_bis": (self.clock() + timedelta(minutes=15)).isoformat(), "lieferzeit": "3 Werktage",
            "montage_standard_bestaetigt": True, **self.article_changes,
        }]}

    def rows(self, table="reifen_bestellungen"):
        db = portal.get_db()
        try:
            return [dict(row) for row in db.execute("SELECT * FROM " + table).fetchall()]
        finally:
            db.close()

    def read(self, order):
        return next(row for row in self.rows() if row["id"] == order["id"])

    def set_fields(self, order, **fields):
        db = portal.get_db()
        try:
            db.execute("UPDATE reifen_bestellungen SET " + ",".join(key + "=?" for key in fields) + " WHERE id=?",
                       (*fields.values(), order["id"]))
            db.commit()
        finally:
            db.close()

    def token(self, **changes):
        return erstelle_auswahl_token(portal.app.secret_key, {**QUERY, **changes}, SKU)

    def draft(self, changes=None, *, query=None, client=None):
        if client is None:
            self.client = portal.app.test_client()
            client = self.client
        selection = self.token(**(query or {}))
        start = "/reifen/bestellen/" + selection
        page = client.get(start)
        self.assertEqual(page.status_code, 200)
        data = Inputs(page.get_data(as_text=True)).values
        data.update({
            "name": "Demo Reifen Testkunde", "email": "checkout@example.invalid", "telefon": "+491711234567",
            "strasse": "Teststraße 1", "plz": "74821", "ort": "Mosbach", "fahrzeug": "Demo Golf",
            "kennzeichen": "MOS DEMO 42", "zahlungsart": "online", **(changes or {}),
        })
        if query and query.get("montage") == "ja":
            data["standardmontage"] = "ja"
        response = client.post(start + "/entwurf", data=data)
        self.assertIn(response.status_code, (302, 303))
        order_url = urlsplit(response.location).path
        order = {"id": order_url.rstrip("/").rsplit("/", 1)[-1], "url": order_url, "token": selection}
        page = client.get(order_url)
        self.assertEqual(page.status_code, 200)
        order["quote_hash"] = Inputs(page.get_data(as_text=True)).values["quote_hash"]
        return order

    def post(self, order, suffix, data=None, *, client=None):
        client = client or self.client
        self.clear_rate()
        with client.session_transaction() as session:
            csrf = session.get(portal.CSRF_FIELD_NAME)
        return client.post(order["url"] + "/" + suffix,
                           data={portal.CSRF_FIELD_NAME: csrf, **(data or {})})

    def verify(self, order):
        self.post(order, "telefon-code")
        self.assertTrue(self.sms.sent)
        response = self.post(order, "telefon-pruefen", {"code": self.sms.sent[-1]["code"]})
        self.assertIn(response.status_code, (200, 302, 303))
        self.assertTrue(self.read(order)["telefon_bestaetigt_am"])

    def final(self, order, **changes):
        return self.post(order, "zahlung-vorbereiten", {
            "bestellpflicht": "ja", "bedingungen": "ja", "quote_hash": order["quote_hash"], **changes,
        })

    def ready(self, **draft_changes):
        order = self.draft(draft_changes)
        self.verify(order)
        return order

    def assert_not_confirmed(self, order):
        self.assertNotIn(self.read(order)["status"], ("demo_bestaetigt", "demo_bestellt_zahlung_offen"))

    def montage_date(self, days=21):
        day = self.clock().date() + timedelta(days=days)
        while day.weekday() >= 5:
            day += timedelta(days=1)
        return day.isoformat()

    def test_non_demo_default_and_non_isolated_requests_cannot_complete_checkout(self):
        before = len(self.rows())
        token = self.token()
        self.client.get("/anfrage?anliegen=reifenangebot")
        with self.client.session_transaction() as session:
            csrf = session[portal.CSRF_FIELD_NAME]
        for mode in (None, "", "live", "production"):
            with self.subTest(mode=mode), patch.dict(portal.app.config, {"REIFEN_CHECKOUT_MODE": mode}):
                response = self.client.post("/reifen/bestellen/" + token + "/entwurf", data={portal.CSRF_FIELD_NAME: csrf})
                self.assertIn(response.status_code, (403, 503))
        with patch.object(portal, "DB", isolated.TEMP_DIR / "not-demo.sqlite3"):
            self.assertIn(self.client.post("/reifen/bestellen/" + token + "/entwurf",
                                           data={portal.CSRF_FIELD_NAME: csrf}).status_code, (403, 503))
        response = self.client.post("/reifen/bestellen/" + token + "/entwurf",
                                    data={portal.CSRF_FIELD_NAME: csrf}, environ_overrides={"REMOTE_ADDR": "203.0.113.10"})
        self.assertIn(response.status_code, (403, 503))
        with patch.dict(portal.app.config, {"TESTING": False}):
            self.assertIn(self.client.post("/reifen/bestellen/" + token + "/entwurf",
                                           data={portal.CSRF_FIELD_NAME: csrf}).status_code, (403, 503))
        self.assertEqual(len(self.rows()), before)
        self.assertFalse(self.sms.sent or self.reservation.calls or self.gateway.created)

    def test_missing_sms_supplier_or_required_texts_block_checkout(self):
        token = self.token()
        cases = ({"REIFEN_CHECKOUT_SMS": None}, {"REIFEN_CHECKOUT_RESERVIERUNG": None}, {"REIFEN_CHECKOUT_GATEWAY": None},
                 {"REIFEN_CHECKOUT_TEXTE": {}}, {"REIFEN_CHECKOUT_TEXTE": {**self.cfg["REIFEN_CHECKOUT_TEXTE"], "widerruf": ""}})
        before = len(self.rows())
        self.client.get("/anfrage?anliegen=reifenangebot")
        with self.client.session_transaction() as session:
            csrf = session[portal.CSRF_FIELD_NAME]
        for changes in cases:
            with self.subTest(changes=changes), patch.dict(portal.app.config, changes):
                self.assertIn(self.client.post("/reifen/bestellen/" + token + "/entwurf",
                                               data={portal.CSRF_FIELD_NAME: csrf}).status_code, (403, 503))
        self.assertEqual(len(self.rows()), before)

    def test_selection_token_cannot_be_tampered_or_expired(self):
        token = self.token()
        self.assertEqual(lade_auswahl_token(portal.app.secret_key, token)["artikel_id"], SKU)
        self.assertIn(self.client.get("/reifen/bestellen/" + token + "tampered").status_code, (400, 404, 409))
        with patch("itsdangerous.timed.time.time", return_value=datetime.now(timezone.utc).timestamp() - 901):
            old = self.token()
        self.assertIn(self.client.get("/reifen/bestellen/" + old).status_code, (400, 404, 409))

    def test_customer_price_fields_are_ignored_and_draft_has_no_payment_or_slot(self):
        before_orders, before_slots = len(self.rows("auftraege")), len(self.rows("reifen_termin_slots"))
        order = self.draft({"amount_total": "1", "gesamt_brutto": "0.01", "angebot_preis": "0.01",
                            "payment_status": "paid", "telefon_bestaetigt_am": self.clock().isoformat()})
        row = self.read(order)
        product = json.loads(row["produkt_json"])
        self.assertEqual(product["preis"]["gesamt_brutto"], "535.50")
        self.assertFalse(row["payment_id"] or row["telefon_bestaetigt_am"])
        self.assertEqual(len(self.rows("auftraege")), before_orders)
        self.assertEqual(len(self.rows("reifen_termin_slots")), before_slots)
        self.assertFalse(self.gateway.created or self.reservation.calls)

    def test_phone_verification_is_required_before_both_payment_choices(self):
        for payment in ("online", "vor_ort"):
            with self.subTest(payment=payment):
                order = self.draft({"zahlungsart": payment})
                self.assertIn(self.final(order).status_code, (400, 409))
                self.assert_not_confirmed(order)
        self.assertFalse(self.gateway.created or self.reservation.calls)

    def test_otp_is_not_exposed_and_wrong_attempts_lock_out_the_correct_code(self):
        order = self.draft()
        self.post(order, "telefon-code")
        code = self.sms.sent[-1]["code"]
        row = self.read(order)
        self.assertTrue(row["otp_hash"])
        self.assertNotEqual(row["otp_hash"], code)
        self.assertNotIn(code, self.client.get(order["url"]).get_data(as_text=True))
        for _ in range(5):
            self.post(order, "telefon-pruefen", {"code": "000000"})
        self.post(order, "telefon-pruefen", {"code": code})
        self.assertFalse(self.read(order)["telefon_bestaetigt_am"])
        self.assertGreaterEqual(self.read(order)["otp_versuche"], 5)
        self.assertFalse(self.gateway.created or self.reservation.calls)

    def test_expired_otp_and_changed_number_do_not_remain_verified(self):
        order = self.draft()
        self.post(order, "telefon-code")
        self.clock.advance(301)
        self.post(order, "telefon-pruefen", {"code": self.sms.sent[-1]["code"]})
        self.assertFalse(self.read(order)["telefon_bestaetigt_am"])
        self.post(order, "telefon-code")
        self.post(order, "telefon-pruefen", {"code": self.sms.sent[-1]["code"]})
        self.assertTrue(self.read(order)["telefon_bestaetigt_am"])
        self.clock.advance(61)
        self.post(order, "telefon-code", {"telefon": "+491721234567"})
        self.assertEqual(self.sms.sent[-1]["phone"], "+491721234567")
        self.assertFalse(self.read(order)["telefon_bestaetigt_am"])
        self.assertIn(self.final(order).status_code, (400, 409))

    def test_sms_resend_cooldown_and_send_limit_are_persistent(self):
        order = self.draft()
        self.post(order, "telefon-code")
        current_hash = self.read(order)["otp_hash"]
        self.assertEqual(self.post(order, "telefon-code").status_code, 429)
        self.assertEqual(len(self.sms.sent), 1)
        self.assertEqual(self.read(order)["otp_hash"], current_hash)
        for _ in range(2):
            self.clock.advance(61)
            self.post(order, "telefon-code")
        self.clock.advance(61)
        self.assertEqual(self.post(order, "telefon-code").status_code, 429)
        self.assertEqual(len(self.sms.sent), 3)
        self.assertEqual(self.read(order)["otp_sendungen"], 3)

    def test_sms_failure_cannot_verify_even_a_known_demo_code(self):
        order = self.draft()
        self.sms.error = RuntimeError("SECRET-SMS-PROVIDER-CREDENTIAL")
        response = self.post(order, "telefon-code")
        self.assertNotIn("SECRET-SMS-PROVIDER-CREDENTIAL", response.get_data(as_text=True))
        self.post(order, "telefon-pruefen", {"code": "610245"})
        self.assertFalse(self.read(order)["telefon_bestaetigt_am"])

    def test_foreign_session_cannot_read_send_codes_or_prepare_another_order(self):
        order = self.draft()
        stranger = portal.app.test_client()
        stranger.get("/anfrage?anliegen=reifenangebot")
        self.assertEqual(stranger.get(order["url"]).status_code, 404)
        for action in ("telefon-code", "telefon-pruefen", "zahlung-vorbereiten", "zahlung-pruefen"):
            self.assertEqual(self.post(order, action, {"code": "610245", "quote_hash": order["quote_hash"]}, client=stranger).status_code, 404)
        self.assertFalse(self.sms.sent or self.gateway.created or self.reservation.calls)

    def test_explicit_order_and_conditions_and_current_quote_hash_are_mandatory(self):
        order = self.ready()
        for changes in ({"bestellpflicht": ""}, {"bedingungen": ""}, {"quote_hash": "tampered"}):
            with self.subTest(changes=changes):
                self.assertIn(self.final(order, **changes).status_code, (400, 409))
        self.assertFalse(self.gateway.created or self.reservation.calls)

    def test_changed_supplier_price_or_stock_requires_new_customer_review_before_payment(self):
        for changes in ({"netto_einkauf": "110.00"}, {"bestand": 3}):
            with self.subTest(changes=changes):
                self.article_changes = {}
                order = self.ready()
                self.article_changes = changes
                self.assertIn(self.final(order).status_code, (400, 409))
                self.assert_not_confirmed(order)
        self.assertFalse(self.gateway.created or self.reservation.calls)

    def test_unconfirmed_supplier_reservation_never_starts_payment(self):
        order = self.ready()
        self.reservation.changes = {"artikel_id": "WRONG-ARTICLE"}
        self.assertIn(self.final(order).status_code, (400, 409, 503))
        self.assert_not_confirmed(order)
        self.assertFalse(self.gateway.created)

    def test_online_checkout_is_unpaid_until_server_gateway_verification(self):
        order = self.ready()
        response = self.final(order)
        self.assertIn(response.status_code, (200, 302, 303))
        row = self.read(order)
        self.assertEqual(row["status"], "zahlung_offen")
        self.assertEqual(len(self.gateway.created), 1)
        sent = self.gateway.created[0]
        self.assertEqual((sent["amount_total"], sent["currency"], sent["quote_hash"]), (53550, "eur", order["quote_hash"]))
        self.client.get(order["url"] + "/erfolg?payment_status=paid&status=succeeded&amount_total=53550")
        self.assertEqual(self.read(order)["status"], "zahlung_offen")
        self.assertFalse(self.gateway.retrieved)
        self.gateway.paid(row["payment_id"])
        self.assertIn(self.post(order, "zahlung-pruefen").status_code, (200, 302, 303))
        self.assertEqual(self.read(order)["status"], "demo_bestaetigt")

    def test_pay_at_collection_is_binding_but_never_reported_as_paid(self):
        order = self.ready(zahlungsart="vor_ort")
        self.assertIn(self.final(order).status_code, (200, 302, 303))
        row = self.read(order)
        self.assertEqual(row["status"], "demo_bestellt_zahlung_offen")
        self.assertNotIn(row["payment_status"], ("paid", "succeeded"))
        self.assertFalse(row["payment_id"])
        self.assertFalse(self.gateway.created or self.gateway.retrieved)
        self.client.get(order["url"] + "/erfolg?payment_status=paid")
        self.post(order, "zahlung-pruefen", {"payment_status": "paid"})
        self.assertEqual(self.read(order)["status"], "demo_bestellt_zahlung_offen")
        self.assertFalse(self.gateway.retrieved)

    def test_paid_proof_must_match_amount_currency_reference_hash_and_demo_payment_id(self):
        cases = ({"amount_total": 1}, {"currency": "usd"}, {"reference": "another-order"},
                 {"quote_hash": "different-quote"}, {"livemode": True}, {"id": "demo_pay_other"})
        for changes in cases:
            with self.subTest(changes=changes):
                order = self.ready()
                self.final(order)
                payment_id = self.read(order)["payment_id"]
                self.gateway.paid(payment_id, **changes)
                self.post(order, "zahlung-pruefen")
                self.assertNotEqual(self.read(order)["status"], "demo_bestaetigt")

    def test_repeated_final_post_and_verification_do_not_repeat_supplier_or_payment_creation(self):
        order = self.ready()
        self.final(order)
        self.final(order)
        self.assertEqual(len(self.reservation.calls), 1)
        self.assertEqual(len(self.gateway.created), 1)
        self.gateway.paid(self.read(order)["payment_id"])
        self.post(order, "zahlung-pruefen")
        self.post(order, "zahlung-pruefen")
        self.assertEqual(self.read(order)["status"], "demo_bestaetigt")
        self.assertEqual(len(self.reservation.calls), 1)
        self.assertEqual(len(self.gateway.created), 1)

    def test_unknown_gateway_creation_is_not_blindly_retried(self):
        order = self.ready()
        self.gateway.create_error = RuntimeError("SECRET-PAYMENT-OUTCOME-UNKNOWN")
        response = self.final(order)
        self.assertNotIn("SECRET-PAYMENT-OUTCOME-UNKNOWN", response.get_data(as_text=True))
        self.final(order)
        self.assertEqual(len(self.gateway.created), 1)
        self.assertEqual(self.read(order)["status"], "pruefung_noetig")

    def test_mounting_requires_date_slot_and_early_service_consents(self):
        order = self.draft({"montage_datum": self.montage_date(7), "montage_uhrzeit": "09:00"}, query={"montage": "ja"})
        self.verify(order)
        self.assertIn(self.final(order).status_code, (400, 409))
        self.assertFalse(self.reservation.calls or self.gateway.created)

    def test_online_mounting_is_only_a_hold_until_payment_is_verified(self):
        before_slots = len(self.rows("reifen_termin_slots"))
        order = self.draft({"montage_datum": self.montage_date(), "montage_uhrzeit": "09:00"}, query={"montage": "ja"})
        self.verify(order)
        self.final(order)
        row = self.read(order)
        self.assertEqual(row["status"], "zahlung_offen")
        self.assertEqual(len(self.rows("reifen_termin_slots")), before_slots + 1)
        self.assertTrue(row["slot_lead_id"])
        self.assertEqual(self.gateway.created[-1]["amount_total"], 62550)
        self.assertNotIn("09:00", portal.freie_reifen_slots(self.montage_date()))
        self.client.get(order["url"] + "/erfolg?paid=true")
        self.assertEqual(self.read(order)["status"], "zahlung_offen")
        self.gateway.paid(row["payment_id"])
        self.post(order, "zahlung-pruefen")
        self.assertEqual(self.read(order)["status"], "demo_bestaetigt")

    def test_supplier_delivery_after_mounting_day_does_not_charge_or_hold_a_slot(self):
        before_slots = len(self.rows("reifen_termin_slots"))
        order = self.draft({"montage_datum": self.montage_date(), "montage_uhrzeit": "11:00"}, query={"montage": "ja"})
        self.verify(order)
        self.reservation.changes = {"lieferdatum": self.montage_date(25)}
        self.assertIn(self.final(order).status_code, (400, 409, 503))
        self.assertEqual(len(self.rows("reifen_termin_slots")), before_slots)
        self.assertFalse(self.gateway.created)

    def test_final_demo_record_stays_labeled_demo_after_mode_or_remote_address_changes(self):
        order = self.ready(zahlungsart="vor_ort")
        self.final(order)
        for config, environ in (({"REIFEN_CHECKOUT_MODE": "gesperrt"}, {}),
                                ({}, {"REMOTE_ADDR": "203.0.113.20"})):
            with self.subTest(config=config, environ=environ), patch.dict(portal.app.config, config):
                response = self.client.get(order["url"] + "/erfolg", environ_overrides=environ)
                self.assertEqual(response.status_code, 200)
                self.assertIn('class="demo"', response.get_data(as_text=True))
                self.assertIn("Keine echte Reifenbestellung oder Kundenzahlung.", response.get_data(as_text=True))
        self.assertEqual(self.read(order)["status"], "demo_bestellt_zahlung_offen")

    def test_stale_phone_snapshot_cannot_reset_a_started_or_final_order(self):
        service = portal.app.extensions["reifen_bestellung"]
        for concurrent_status in ("reservierung_laeuft", "zahlung_offen", "demo_bestaetigt"):
            with self.subTest(status=concurrent_status):
                order = self.ready()
                self.clock.advance(61)
                real_row = service.row
                intercepted = False

                def read_before_concurrent_commit(order_id):
                    nonlocal intercepted
                    snapshot = real_row(order_id)
                    if order_id == order["id"] and not intercepted:
                        intercepted = True
                        self.set_fields(order, status=concurrent_status)
                    return snapshot

                sent_before = len(self.sms.sent)
                with patch.object(service, "row", side_effect=read_before_concurrent_commit):
                    response = self.post(order, "telefon-code")
                self.assertIn(response.status_code, (409, 429))
                self.assertEqual(self.read(order)["status"], concurrent_status)
                self.assertTrue(self.read(order)["telefon_bestaetigt_am"])
                self.assertEqual(len(self.sms.sent), sent_before)

    def test_late_sms_failure_does_not_revert_a_concurrently_finalized_order(self):
        order = self.ready()
        verified_at = self.read(order)["telefon_bestaetigt_am"]
        self.clock.advance(61)

        def finalize_then_fail(phone, code, reference):
            self.set_fields(order, status="demo_bestaetigt", telefon_bestaetigt_am=verified_at)
            raise RuntimeError("synthetic late SMS outcome")

        with patch.object(self.sms, "send", side_effect=finalize_then_fail):
            self.assertEqual(self.post(order, "telefon-code").status_code, 503)
        self.assertEqual(self.read(order)["status"], "demo_bestaetigt")
        self.assertEqual(self.read(order)["telefon_bestaetigt_am"], verified_at)

    def test_saved_selection_can_create_a_changed_unverified_draft_and_exact_repost_is_deduplicated(self):
        order = self.ready()
        start = "/reifen/bestellen/" + order["token"]
        page = self.client.get(start)
        data = Inputs(page.get_data(as_text=True)).values
        data.update({
            "name": "Demo Reifen Testkunde", "email": "checkout@example.invalid", "telefon": "+491711234567",
            "strasse": "Teststraße 1", "plz": "74821", "ort": "Mosbach", "fahrzeug": "Demo Golf",
            "kennzeichen": "MOS DEMO 42", "zahlungsart": "online",
        })
        same = self.client.post(start + "/entwurf", data=data)
        self.assertIn(same.status_code, (302, 303))
        self.assertEqual(urlsplit(same.location).path, order["url"])
        self.assertEqual(len(self.rows()), 1)
        data["telefon"] = "+491721234567"
        changed = self.client.post(start + "/entwurf", data=data)
        self.assertIn(changed.status_code, (302, 303))
        new_id = urlsplit(changed.location).path.rsplit("/", 1)[-1]
        self.assertNotEqual(new_id, order["id"])
        new = next(row for row in self.rows() if row["id"] == new_id)
        self.assertFalse(new["telefon_bestaetigt_am"])
        self.assertEqual(new["telefon"], "+491721234567")
        self.assertTrue(self.read(order)["telefon_bestaetigt_am"])

    def test_admin_order_list_is_protected_and_pay_at_collection_is_not_labeled_paid(self):
        order = self.ready(zahlungsart="vor_ort")
        self.final(order)
        self.assertEqual(self.client.get("/admin/reifenbestellungen").status_code, 302)
        with self.client.session_transaction() as session:
            session["admin"] = True
        response = self.client.get("/admin/reifenbestellungen")
        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("ausschließlich Demobestellungen", body)
        self.assertIn("DEMO · bestellt, Zahlung vor Ort offen", body)
        self.assertNotIn("DEMO · bezahlt", body)
        self.assertIn("535,50", body)
        self.assertIn(order["id"], body)
        self.assertNotIn("610245", body)
        self.assertNotIn(self.read(order)["quote_hash"], body)

    def test_stale_price_change_cannot_revert_a_concurrently_final_order(self):
        order = self.ready()
        self.article_changes = {"netto_einkauf": "110.00"}

        def concurrent_provider(query):
            result = self.provider(query)
            self.set_fields(order, status="demo_bestaetigt", quote_hash="concurrent-final-quote")
            return result

        with patch.dict(portal.app.config, {"REIFENANGEBOT_PROVIDER": concurrent_provider}):
            self.final(order)
        self.assertEqual(self.read(order)["status"], "demo_bestaetigt")
        self.assertEqual(self.read(order)["quote_hash"], "concurrent-final-quote")
        self.assertFalse(self.reservation.calls or self.gateway.created)

    def test_stale_final_claim_cannot_reserve_against_a_concurrently_changed_quote(self):
        order = self.ready()

        def concurrent_provider(query):
            result = self.provider(query)
            self.set_fields(order, quote_hash="concurrent-new-quote")
            return result

        with patch.dict(portal.app.config, {"REIFENANGEBOT_PROVIDER": concurrent_provider}):
            self.final(order)
        self.assertEqual(self.read(order)["quote_hash"], "concurrent-new-quote")
        self.assertFalse(self.reservation.calls or self.gateway.created)

    def test_paid_after_reservation_expiry_needs_review_and_keeps_the_mounting_hold(self):
        order = self.draft({"montage_datum": self.montage_date(), "montage_uhrzeit": "12:00"}, query={"montage": "ja"})
        self.verify(order)
        self.final(order)
        row = self.read(order)
        hold_count = len(self.rows("reifen_termin_slots"))
        self.clock.advance(901)
        self.gateway.paid(row["payment_id"])
        self.post(order, "zahlung-pruefen")
        self.assertEqual(self.read(order)["status"], "pruefung_noetig")
        self.assertEqual(len(self.rows("reifen_termin_slots")), hold_count)
        self.assertTrue(self.read(order)["slot_lead_id"])

    def test_late_gateway_failure_cannot_downgrade_a_concurrently_verified_payment(self):
        order = self.ready()
        self.final(order)

        def verified_elsewhere_then_timeout(payment_id):
            self.set_fields(order, status="demo_bestaetigt", payment_status="demo_bezahlt")
            raise RuntimeError("synthetic late provider timeout")

        with patch.object(self.gateway, "retrieve", side_effect=verified_elsewhere_then_timeout):
            self.post(order, "zahlung-pruefen")
        row = self.read(order)
        self.assertEqual(row["status"], "demo_bestaetigt")
        self.assertEqual(row["payment_status"], "demo_bezahlt")


if __name__ == "__main__":
    unittest.main(verbosity=2)
