"""Real-provider checkout state machine with isolated data and fake IO only."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
with patch("threading.Thread.start"):
    import smoke_test as isolated

portal = isolated.portal
from reifen_bestellung import erstelle_auswahl_token, lade_auswahl_token
import reifen_kauf as core
import reifen_vertrag as contracts
from felgen_katalog import FelgenCatalogProvider, parse_felgen_feed
from raeder_auswahl import erstelle_felgen_auswahl_token
from test_felgen_katalog import POLICY, raw_feed


SKU = "SYNTHETICTIRE42"
QUERY = {"groesse": "205/55 R16 91V", "saison": "sommer", "qualitaet": "premium", "anzahl": "4", "montage": "nein"}


class Clock:
    def __init__(self):
        self.value = datetime.now(timezone.utc)
    def __call__(self):
        return self.value


class Supplier:
    livemode = False
    def __init__(self, clock):
        self.clock = clock
        self.inspections, self.reservations, self.commits = [], [], []
        self.inspect_change, self.reserve_change, self.commit_change = {}, {}, {}
        self.reserve_error = self.commit_error = None
    def inspect(self, selected):
        self.inspections.append(deepcopy(selected))
        return {"artikel_id": selected["artikel_id"], "paketversandfaehig": True,
                "lieferdatum": (self.clock().date()+timedelta(days=3)).isoformat(), **self.inspect_change}
    def reserve(self, order):
        self.reservations.append(deepcopy(order))
        if self.reserve_error:
            raise self.reserve_error
        return {"artikel_id": order["artikel_id"], "anzahl": order["anzahl"], "quote_hash": order["quote_hash"],
                "reservierung_id": "syntheticHold"+order["id"], "paketversandfaehig": True,
                "lieferdatum": (self.clock().date()+timedelta(days=3)).isoformat(),
                "gueltig_bis": datetime.fromtimestamp(order["payment_expires_at"]+600, timezone.utc).isoformat(),
                **self.reserve_change}
    def commit(self, order):
        self.commits.append(deepcopy(order))
        if self.commit_error:
            raise self.commit_error
        return {"bestaetigt": True, "bestellnummer": "syntheticSupplier"+order["id"], "reference": order["id"],
                "artikel_id": order["artikel_id"], "anzahl": order["anzahl"],
                "lieferdatum":order["reservierung"]["lieferdatum"],
                "reservierung_id":order["reservierung"]["reservierung_id"], **self.commit_change}


class SMS:
    def __init__(self):
        self.starts, self.checks = [], []
        self.start_error = self.check_error = None
    def start(self, phone, reference):
        self.starts.append((phone, reference))
        if self.start_error:
            raise self.start_error
        return {"status": "pending"}
    def check(self, phone, code, reference):
        self.checks.append((phone, code, reference))
        if self.check_error:
            raise self.check_error
        return code == "610245" and (phone, reference) in self.starts


class Gateway:
    livemode = False
    def __init__(self):
        self.creates, self.retrieves, self.events = [], [], []
        self.sessions = {}
        self.error = None
    def create(self, order):
        self.creates.append(deepcopy(order))
        assert type(order["payment_created_at"]) is int
        assert type(order["payment_expires_at"]) is int
        assert 1800 <= order["payment_expires_at"]-order["payment_created_at"] <= 86400
        if self.error:
            raise self.error
        sid = "cs_"+("live" if self.livemode else "test")+"_"+hashlib.sha256(order["id"].encode()).hexdigest()[:24]
        self.sessions[sid] = {"id": sid, "status": "open", "payment_status": "unpaid", "livemode": self.livemode,
            "reference": order["id"], "quote_hash": order["quote_hash"], "amount_total": order["amount_total"],
            "currency": "eur", "payment_intent": None, "url": "https://checkout.stripe.com/c/pay/"+sid,
            "expires_at": order["payment_expires_at"]}
        return deepcopy(self.sessions[sid])
    def retrieve(self, session_id, *, expected_order=None):
        self.retrieves.append((session_id, deepcopy(expected_order)))
        if self.error:
            raise self.error
        return deepcopy(self.sessions[session_id])
    def verify_webhook(self, raw, signature):
        self.events.append((raw, signature))
        if signature != "synthetic-verified-signature":
            raise ValueError("Invalid synthetic signature")
        return json.loads(raw)
    def paid(self, session_id):
        self.sessions[session_id].update(status="succeeded", payment_status="paid", payment_intent="pi_Synthetic"+session_id.rsplit("_",1)[-1])


class ReifenKaufTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
        source = Path(portal.DB).resolve()
        assert source.parent == isolated.TEMP_DIR.resolve(), "Only disposable smoke data may be copied"
        target = isolated.TEMP_DIR / "real-provider-core-test.sqlite3"
        with closing(sqlite3.connect(source)) as origin, closing(sqlite3.connect(target)) as destination:
            origin.backup(destination)
        cls.db_patch = patch.object(portal, "DB", target)
        cls.db_patch.start()
        cls.addClassCleanup(cls.db_patch.stop)
        portal.init_db()
        portal.check24_bookings.init_schema()
        db = portal.get_db()
        core.init_schema(db)
        contracts.init_schema(db)
        db.commit()
        db.close()
        if "reifen_kauf" not in portal.app.blueprints:
            core.register_reifen_kauf(portal)
        if "reifen_vertrag" not in portal.app.blueprints:
            contracts.register_reifen_vertrag(portal)
        cls.service = portal.app.extensions["reifen_kauf"]

    def setUp(self):
        db = portal.get_db()
        for table in ("reifen_termin_slots", "reifen_kauf_events", "reifen_kaeufe", "reifen_kauf_sms_budget", "reifen_vertrag_outbox"):
            db.execute("DELETE FROM "+table)
        db.commit()
        db.close()
        self.clock = Clock()
        self.supplier, self.sms, self.gateway = Supplier(self.clock), SMS(), Gateway()
        self.terms = {"version": "synthetic:approved-v1", "sha256": "a"*64, "freigegeben": True,
                      "status": "freigegeben", "agb": "Synthetic test terms", "widerruf": "Synthetic withdrawal",
                      "widerrufsformular": "Synthetic withdrawal form; customer and contract details remain blank",
                      "datenschutz": "Synthetic data text", "lieferung": "Synthetic pickup", "offene_entscheidungen": []}
        self.article_change = {}
        self.client = portal.app.test_client()
        self.cfg = {"REIFEN_KAUF_MODE": "test", "REIFEN_KAUF_ENABLED": True,
            "REIFEN_TERMS_APPROVED_SHA256": self.terms["sha256"], "REIFEN_KAUF_RECHTSTEXTE": lambda: deepcopy(self.terms),
            "REIFEN_KAUF_SUPPLIER": self.supplier, "REIFEN_KAUF_SMS": self.sms, "REIFEN_KAUF_GATEWAY": self.gateway,
            "REIFEN_KAUF_MAILER": lambda message: True, "REIFEN_KAUF_MAIL_ENABLED": True,
            "REIFEN_SMS_KOSTEN_FREIGEGEBEN": True, "REIFEN_SMS_MAX_STARTS_PRO_TAG": 50,
            "REIFENANGEBOT_PROVIDER": self.provider, "REIFEN_KAUF_CLOCK": self.clock}
        cfg_patch = patch.dict(portal.app.config, self.cfg)
        cfg_patch.start()
        self.addCleanup(cfg_patch.stop)
        for name in ("lead_notice", "notify_lead_workshop", "schedule_change_backup", "sende_website_lead_benachrichtigung"):
            blocker = patch.object(portal, name)
            blocker.start()
            self.addCleanup(blocker.stop)
        self.clear_rate()

    def clear_rate(self):
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()

    def provider(self, query):
        return {"angebote": [{"artikel_id": SKU, "ean": "4000000000042", "bezeichnung": "Synthetic Tire",
            "breite":205,"querschnitt":55,"zoll":16,"lastindex":91,"geschwindigkeitsindex":"V",
            "saison":"sommer","qualitaet":"premium","netto_einkauf":"100.00","fracht_netto":"10.00",
            "waehrung":"EUR","bestand":8,"preis_bestaetigt":True,"abgerufen_am":self.clock().isoformat(),
            "gueltig_bis":(self.clock()+timedelta(minutes=15)).isoformat(),"lieferzeit":"3 Werktage",
            "montage_standard_bestaetigt":True, **self.article_change}]}

    def rows(self, table="reifen_kaeufe"):
        db = portal.get_db()
        try:
            return [dict(row) for row in db.execute("SELECT * FROM "+table).fetchall()]
        finally:
            db.close()

    def read(self, order):
        return next(row for row in self.rows() if row["id"] == order["id"])

    def change(self, order, **fields):
        db = portal.get_db()
        db.execute("UPDATE reifen_kaeufe SET "+",".join(key+"=?" for key in fields)+" WHERE id=?", (*fields.values(), order["id"]))
        db.commit()
        db.close()

    def token(self, **query):
        with patch("itsdangerous.timed.time.time", return_value=self.clock().timestamp()):
            return erstelle_auswahl_token(portal.app.secret_key, {**QUERY, **query}, SKU)

    def csrf(self, client=None):
        with (client or self.client).session_transaction() as session:
            return session.get(portal.CSRF_FIELD_NAME)

    def draft(self, change=None, *, query=None, client=None, selection_token=None):
        self.client = client or portal.app.test_client()
        token = selection_token or self.token(**(query or {}))
        prefix = "/raeder/bestellen/" if token.startswith("felgen.") else "/reifen/bestellen/"
        response = self.client.get(prefix+token+"/kauf")
        self.assertEqual(response.status_code, 200)
        data = {"name":"Synthetic Customer", "email":"kauf@example.invalid", "telefon":"+491711234567",
                "strasse":"Teststraße 1", "plz":"74821", "ort":"Mosbach", "fahrzeug":"Synthetic Golf",
                "kennzeichen":"MOS SYNTH 42", "zahlungsart":"online", "standardmontage":"ja", **(change or {}),
                portal.CSRF_FIELD_NAME:self.csrf()}
        self.clear_rate()
        response = self.client.post("/reifen/kauf/"+token+"/entwurf", data=data)
        self.assertIn(response.status_code, (302,303), response.get_data(as_text=True)[:400])
        url = urlsplit(response.location).path
        return {"id":url.rsplit("/",1)[-1], "url":url, "token":token}

    def post(self, order, action, data=None, *, client=None, **kwargs):
        client = client or self.client
        self.clear_rate()
        return client.post(order["url"]+"/"+action,
            data={portal.CSRF_FIELD_NAME:self.csrf(client), **(data or {})}, **kwargs)

    def verify(self, order):
        self.assertIn(self.post(order,"telefon-code").status_code,(302,303))
        self.assertIn(self.post(order,"telefon-pruefen",{"code":"610245"}).status_code,(302,303))
        self.assertTrue(self.read(order)["telefon_bestaetigt_am"])

    def final(self, order, **change):
        return self.post(order,"zahlung-vorbereiten",{"bestellpflicht":"ja","bedingungen":"ja",
            "quote_hash":self.read(order)["quote_hash"], "frueher_montagebeginn":"ja",
            "kenntnis_montagewiderruf":"ja", **change})

    def ready(self, change=None, *, query=None):
        order = self.draft(change, query=query)
        self.verify(order)
        return order

    def worker_extensions(self, *, alive=True, stopped=False):
        return {"reifen_kauf_worker_thread":Mock(is_alive=Mock(return_value=alive)),
                "reifen_kauf_worker":Mock(is_set=Mock(return_value=stopped))}

    def montage_day(self, days=21):
        value = self.clock().date()+timedelta(days=days)
        while value.weekday()==6:
            value += timedelta(days=1)
        return value.isoformat()

    def test_incomplete_gate_blocks_sms_supplier_payment_and_drafts(self):
        cases = {"REIFEN_KAUF_ENABLED":False,"REIFEN_TERMS_APPROVED_SHA256":"wrong",
                 "REIFEN_KAUF_SUPPLIER":None,"REIFEN_KAUF_SMS":None,"REIFEN_KAUF_GATEWAY":None,
                 "REIFEN_KAUF_MAIL_ENABLED":False,"REIFEN_SMS_KOSTEN_FREIGEGEBEN":False}
        token = self.token()
        self.client.get("/reifen/bestellen/"+token+"/kauf")
        for key,value in cases.items():
            with self.subTest(key=key), patch.dict(portal.app.config,{key:value}):
                self.clear_rate()
                response = self.client.post("/reifen/kauf/"+token+"/entwurf",data={portal.CSRF_FIELD_NAME:self.csrf()})
                self.assertEqual(response.status_code,503)
        self.assertFalse(self.supplier.reservations or self.supplier.commits or self.sms.starts or self.gateway.creates or self.rows())

    def test_draft_ignores_browser_prices_and_is_owned(self):
        order = self.draft({"amount_total":"1","payment_status":"paid","quote_hash":"evil"})
        row = self.read(order)
        self.assertEqual(json.loads(row["produkt_json"])["preis"]["gesamt_brutto"],"535.50")
        self.assertFalse(row["payment_id"] or row["telefon_bestaetigt_am"])
        foreign = portal.app.test_client()
        self.assertEqual(foreign.get(order["url"]).status_code,404)
        foreign.get("/anfrage")
        self.assertEqual(self.post(order,"telefon-code",client=foreign).status_code,404)

    def test_sms_pending_overview_requests_only_the_received_code(self):
        order=self.draft()
        self.change(order,sms_state="pending")
        response=self.client.get(order["url"])
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        self.assertIn("SMS angefordert. Bitte geben Sie den erhaltenen Code ein.",html)
        self.assertNotIn('action="'+order["url"]+'/telefon-code"',html)
        self.assertIn('action="'+order["url"]+'/telefon-pruefen"',html)
        self.assertNotIn("Bestätigungscode senden",html)
        self.assertEqual(self.service.order(self.read(order))["sms_status"],"pending")
        self.assertFalse(self.sms.starts or self.sms.checks or self.gateway.creates)

    def test_sms_processing_and_uncertain_states_do_not_offer_another_send(self):
        order=self.draft()
        for status,message in (("sending","Die SMS-Anfrage wird geprüft."),("checking","Ihr Code wird geprüft."),
                ("unknown","Der Stand der Telefonprüfung ist noch unklar."),("limit","Die Telefonprüfung ist vorerst gesperrt."),
                ("PRIVATE-UNEXPECTED-STATUS","Der Stand der Telefonprüfung ist noch unklar.")):
            with self.subTest(status=status):
                self.change(order,sms_state=status)
                response=self.client.get(order["url"])
                self.assertEqual(response.status_code,200)
                html=response.get_data(as_text=True)
                self.assertIn(message,html)
                self.assertNotIn('action="'+order["url"]+'/telefon-code"',html)
                self.assertNotIn('action="'+order["url"]+'/telefon-pruefen"',html)
                self.assertNotIn("PRIVATE-UNEXPECTED-STATUS",html)
                public=self.service.order(self.read(order))
                self.assertEqual(public["sms_status"],status if status!="PRIVATE-UNEXPECTED-STATUS" else "unknown")
        self.assertFalse(self.sms.starts or self.sms.checks or self.gateway.creates)

    def test_sms_approved_overview_keeps_the_existing_confirmation(self):
        order=self.draft()
        self.change(order,sms_state="approved",telefon_bestaetigt_am=self.clock().isoformat(),state="telefon_bestaetigt")
        response=self.client.get(order["url"])
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        self.assertIn("✓ Telefonnummer bestätigt",html)
        self.assertNotIn('action="'+order["url"]+'/telefon-code"',html)
        self.assertNotIn('action="'+order["url"]+'/telefon-pruefen"',html)
        self.assertFalse(self.sms.starts or self.sms.checks or self.gateway.creates)

    def test_sms_empty_status_and_legacy_demo_model_keep_initial_controls(self):
        order=self.draft()
        public=self.service.order(self.read(order))
        self.assertEqual(public["sms_status"],"")
        for missing in (False,True):
            with self.subTest(legacy_demo_model=missing):
                model=dict(public)
                if missing:
                    model.pop("sms_status")
                with patch.object(self.service,"order",return_value=model):
                    response=self.client.get(order["url"])
                self.assertEqual(response.status_code,200)
                html=response.get_data(as_text=True)
                self.assertIn("Fordern Sie den Code an und geben Sie ihn hier ein.",html)
                self.assertIn('action="'+order["url"]+'/telefon-code"',html)
                self.assertIn('action="'+order["url"]+'/telefon-pruefen"',html)
        self.assertFalse(self.sms.starts or self.sms.checks or self.gateway.creates)

    def test_phone_and_final_consents_are_required_for_both_payment_choices(self):
        for method in ("online","vor_ort"):
            with self.subTest(method=method):
                order = self.draft({"zahlungsart":method})
                self.assertEqual(self.final(order).status_code,409)
                self.verify(order)
                self.assertEqual(self.final(order,bestellpflicht="nein").status_code,409)
        self.assertFalse(self.supplier.reservations or self.gateway.creates)

    def test_sms_budget_and_wrong_checks_are_bounded(self):
        order = self.draft()
        with patch.dict(portal.app.config,{"REIFEN_SMS_MAX_STARTS_PRO_TAG":1}):
            self.post(order,"telefon-code")
            self.assertEqual(self.post(order,"telefon-code").status_code,429)
        self.assertEqual(len(self.sms.starts),1)
        self.change(order,sms_state="pending")
        for _ in range(5):
            self.post(order,"telefon-pruefen",{"code":"000000"})
        self.assertFalse(self.read(order)["telefon_bestaetigt_am"])
        before = len(self.sms.checks)
        self.post(order,"telefon-pruefen",{"code":"610245"})
        self.assertEqual(len(self.sms.checks),before)

    def test_stale_or_changed_server_price_requires_new_confirmation(self):
        order = self.ready()
        self.article_change["netto_einkauf"]="110.00"
        self.assertEqual(self.final(order).status_code,409)
        self.assertEqual(self.read(order)["state"],"preis_geaendert")
        self.assertFalse(self.supplier.reservations or self.gateway.creates)
        self.final(order)
        self.assertEqual(self.gateway.creates[0]["amount_total"],58786)

    def test_shipping_readiness_and_reservation_coverage_fail_closed(self):
        for change in ({"paketversandfaehig":False},{"gueltig_bis":self.clock().isoformat()},{"anzahl":0},{"quote_hash":"wrong"}):
            with self.subTest(change=change):
                order = self.ready()
                self.supplier.reserve_change=change
                self.assertEqual(self.final(order).status_code,409)
                self.assertEqual(self.read(order)["state"],"pruefung_noetig")
        self.assertFalse(self.gateway.creates or self.supplier.commits)

    def test_online_created_with_frozen_attempt_is_not_order_acceptance(self):
        order=self.ready()
        self.assertIn(self.final(order).status_code,(302,303))
        row=self.read(order)
        self.assertEqual(row["state"],"zahlung_offen")
        self.assertEqual(row["payment_status"],"offen")
        sent=self.gateway.creates[0]
        self.assertIs(type(sent["payment_created_at"]),int)
        self.assertEqual(sent["payment_expires_at"]-sent["payment_created_at"],2700)
        self.assertFalse(self.supplier.commits or self.rows("reifen_vertrag_outbox"))
        self.final(order)
        self.assertEqual(len(self.gateway.creates),1)

    def test_only_verified_payment_then_supplier_commit_accepts_and_queues_immutable_contract(self):
        order=self.ready()
        self.final(order)
        sid=self.read(order)["payment_id"]
        self.gateway.paid(sid)
        self.post(order,"zahlung-pruefen")
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual(self.read(order)["payment_status"],"bezahlt")
        self.assertEqual(len(self.supplier.commits),1)
        copies=self.rows("reifen_vertrag_outbox")
        self.assertEqual(len(copies),1)
        snapshot=json.loads(copies[0]["snapshot_json"])
        self.assertEqual(snapshot["amount_total"],53550)
        self.assertEqual(snapshot["rechtstexte"]["version"],"synthetic:approved-v1")
        self.terms["agb"]="new text after acceptance"
        self.assertEqual(json.loads(self.rows("reifen_vertrag_outbox")[0]["snapshot_json"])["rechtstexte"]["agb"],"Synthetic test terms")
        self.post(order,"zahlung-pruefen")
        self.assertEqual(len(self.supplier.commits),1)

    def test_pay_on_pickup_is_binding_but_never_paid(self):
        order=self.ready({"zahlungsart":"vor_ort"})
        self.assertIn(self.final(order).status_code,(302,303))
        row=self.read(order)
        self.assertEqual(row["state"],"bestellt_zahlung_offen")
        self.assertEqual(row["payment_status"],"vor_ort_offen")
        self.assertFalse(row["payment_id"] or self.gateway.creates)
        self.assertEqual(len(self.supplier.commits),1)
        self.assertEqual(len(self.rows("reifen_vertrag_outbox")),1)

    def test_success_redirect_is_read_only_and_does_not_confirm_browser_paid_flags(self):
        order=self.ready()
        self.final(order)
        before=self.read(order)
        response=self.client.get("/reifen/zahlung/erfolg/"+order["id"]+"?paid=true&session_id=cs_live_forged")
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.read(order),before)
        self.assertFalse(self.gateway.retrieves or self.supplier.commits)

    def test_unpaid_finalize_is_rejected_even_if_called_by_internal_worker(self):
        order=self.ready()
        self.final(order)
        with portal.app.test_request_context("/"):
            with self.assertRaises(ValueError):
                self.service.finalize(self.read(order))
        self.assertFalse(self.supplier.commits)

    def test_changed_amount_currency_binding_or_mode_cannot_be_paid(self):
        cases=({"amount_total":1},{"currency":"usd"},{"reference":"foreign"},{"quote_hash":"c"*64},{"livemode":True})
        for change in cases:
            with self.subTest(change=change):
                order=self.ready()
                self.final(order)
                sid=self.read(order)["payment_id"]
                self.gateway.paid(sid)
                self.gateway.sessions[sid].update(change)
                self.post(order,"zahlung-pruefen")
                self.assertNotEqual(self.read(order)["state"],"bestaetigt")
        self.assertFalse(self.supplier.commits)

    def test_unknown_gateway_or_supplier_commit_keeps_holds_and_never_auto_retries(self):
        day=self.montage_day()
        order=self.ready({"montage_datum":day,"montage_uhrzeit":"09:00"},query={"montage":"ja"})
        self.gateway.error=TimeoutError("Synthetic uncertain create")
        self.assertEqual(self.final(order).status_code,409)
        self.assertEqual(self.read(order)["state"],"pruefung_noetig")
        self.assertNotIn("09:00",portal.freie_reifen_slots(day))
        self.final(order)
        self.assertEqual(len(self.gateway.creates),1)
        self.gateway.error=None

    def test_expired_reservation_after_payment_does_not_commit_or_release_calendar(self):
        day=self.montage_day()
        order=self.ready({"montage_datum":day,"montage_uhrzeit":"10:00"},query={"montage":"ja"})
        self.final(order)
        row=self.read(order)
        self.gateway.paid(row["payment_id"])
        self.clock.value=core.aware(json.loads(row["reservierung_json"])["gueltig_bis"])+timedelta(seconds=1)
        self.post(order,"zahlung-pruefen")
        self.assertEqual(self.read(order)["state"],"pruefung_noetig")
        self.assertNotIn("10:00",portal.freie_reifen_slots(day))
        self.assertFalse(self.supplier.commits)

    def test_shared_native_slot_conflict_does_not_charge_or_commit(self):
        day=self.montage_day()
        one=self.ready({"zahlungsart":"vor_ort","montage_datum":day,"montage_uhrzeit":"11:00"},query={"montage":"ja"})
        self.final(one)
        second=self.ready({"montage_datum":day,"montage_uhrzeit":"11:00"},query={"montage":"ja"})
        self.assertEqual(self.final(second).status_code,409)
        self.assertEqual(len(self.rows("reifen_termin_slots")),1)
        self.assertFalse(self.gateway.creates)
        self.assertEqual(len(self.supplier.commits),1)

    def test_core_does_not_mutate_separate_demo_orders(self):
        before=self.rows("reifen_bestellungen")
        order=self.ready({"zahlungsart":"vor_ort"})
        self.final(order)
        self.assertEqual(self.rows("reifen_bestellungen"),before)

    def test_pending_payment_can_settle_while_new_purchases_are_disabled(self):
        order=self.ready()
        self.final(order)
        sid=self.read(order)["payment_id"]
        self.gateway.paid(sid)
        with patch.dict(portal.app.config,{"REIFEN_KAUF_ENABLED":False}):
            self.post(order,"zahlung-pruefen")
        self.assertEqual(self.read(order)["state"],"bestaetigt")

    def test_mode_switch_never_sends_old_test_draft_to_live_gateway(self):
        order=self.ready()
        self.gateway.livemode=True
        self.supplier.livemode=True
        with patch.dict(portal.app.config,{"REIFEN_KAUF_MODE":"live","TESTING":False,"REIFEN_GEWAERLEISTUNG_NOTICE_READY":True}),patch.dict(portal.app.extensions,self.worker_extensions()):
            with portal.app.test_request_context("/",base_url="https://localhost"):
                self.assertTrue(self.service.readiness()["ready"])
            response=self.post(order,"zahlung-vorbereiten",{"bestellpflicht":"ja","bedingungen":"ja","quote_hash":self.read(order)["quote_hash"]},base_url="https://localhost")
            self.assertGreaterEqual(response.status_code,400)
        self.assertFalse(self.gateway.creates or self.supplier.reservations)

    def test_supplier_mode_must_exactly_match_gateway_and_purchase_mode_before_any_io(self):
        order=self.ready()
        self.supplier.inspections.clear()
        self.sms.starts.clear()
        self.sms.checks.clear()
        for mode,supplier_mode,gateway_mode in (("test",True,False),("live",False,True),("test",None,False),("live",1,True)):
            with self.subTest(mode=mode,supplier_mode=supplier_mode):
                self.supplier.livemode=supplier_mode
                self.gateway.livemode=gateway_mode
                provider=Mock(side_effect=AssertionError("Provider IO must remain blocked"))
                with patch.dict(portal.app.config,{"REIFEN_KAUF_MODE":mode,"TESTING":mode!="live",
                        "REIFEN_GEWAERLEISTUNG_NOTICE_READY":True,"REIFENANGEBOT_PROVIDER":provider}),patch.dict(portal.app.extensions,self.worker_extensions()):
                    with portal.app.test_request_context("/",base_url="https://localhost"):
                        self.assertFalse(self.service.readiness()["ready"])
                    self.clear_rate()
                    response=self.client.post("/reifen/kauf/"+order["token"]+"/entwurf",
                        data={portal.CSRF_FIELD_NAME:self.csrf()},base_url="https://localhost")
                    self.assertEqual(response.status_code,503)
                    self.assertEqual(self.post(order,"telefon-code",base_url="https://localhost").status_code,503)
                    self.assertEqual(self.post(order,"zahlung-vorbereiten",{"bestellpflicht":"ja","bedingungen":"ja",
                        "quote_hash":self.read(order)["quote_hash"]},base_url="https://localhost").status_code,503)
                    provider.assert_not_called()
                self.assertFalse(self.supplier.inspections or self.supplier.reservations or self.supplier.commits)
                self.assertFalse(self.sms.starts or self.sms.checks or self.gateway.creates or self.gateway.retrieves)

    def test_live_checkout_requires_a_running_worker_with_an_unset_stop_event(self):
        self.gateway.livemode=self.supplier.livemode=True
        cases=(("missing",{"reifen_kauf_worker_thread":None,"reifen_kauf_worker":None},False),
               ("dead",self.worker_extensions(alive=False),False),
               ("missing-stop",{**self.worker_extensions(),"reifen_kauf_worker":None},False),
               ("stopped",self.worker_extensions(stopped=True),False),
               ("running",self.worker_extensions(),True))
        with patch.dict(portal.app.config,{"REIFEN_KAUF_MODE":"live","TESTING":False,"REIFEN_GEWAERLEISTUNG_NOTICE_READY":True}):
            for label,extensions,expected in cases:
                with self.subTest(worker=label),patch.dict(portal.app.extensions,extensions),portal.app.test_request_context("/",base_url="https://localhost"):
                    readiness=self.service.readiness()
                    self.assertIs(readiness["ready"],expected,readiness["problems"])
                    if expected:
                        self.service.require_ready()
                    else:
                        self.assertIn("Zahlungsabgleich und Vertragsversand laufen noch nicht",readiness["problems"])
                        with self.assertRaises(Exception) as blocked:
                            self.service.require_ready()
                        self.assertEqual(blocked.exception.code,503)
        self.assertFalse(self.supplier.inspections or self.supplier.reservations or self.supplier.commits)
        self.assertFalse(self.sms.starts or self.sms.checks or self.gateway.creates or self.gateway.retrieves)

    def test_pending_test_payment_cannot_commit_after_supplier_was_changed_to_live(self):
        order=self.ready()
        self.final(order)
        self.gateway.paid(self.read(order)["payment_id"])
        self.supplier.livemode=True
        try:
            self.service.reconcile(order["id"])
        except ValueError:
            pass
        self.assertFalse(self.supplier.commits)
        self.assertNotIn(self.read(order)["state"],("bestaetigt","bestellt_zahlung_offen"))
        self.assertFalse(self.rows("reifen_vertrag_outbox"))

    def test_late_quote_reply_cannot_reset_a_concurrently_finalized_order(self):
        order=self.ready()
        original=self.provider
        def slow_quote(query):
            self.change(order,state="bestaetigt",payment_status="bezahlt")
            result=original(query)
            result["angebote"][0]["netto_einkauf"]="110.00"
            return result
        with patch.dict(portal.app.config,{"REIFENANGEBOT_PROVIDER":slow_quote}):
            self.final(order)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual(self.read(order)["payment_status"],"bezahlt")
        self.assertFalse(self.supplier.reservations or self.gateway.creates)

    def test_late_retrieve_failure_cannot_downgrade_confirmed_payment(self):
        order=self.ready()
        self.final(order)
        def late_failure(session_id, *, expected_order=None):
            self.change(order,state="bestaetigt",payment_status="bezahlt")
            raise TimeoutError("Synthetic delayed failure after another worker succeeded")
        self.gateway.retrieve=late_failure
        self.post(order,"zahlung-pruefen")
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual(self.read(order)["payment_status"],"bezahlt")

    def test_unknown_supplier_commit_is_not_accepted_or_blindly_repeated(self):
        order=self.ready()
        self.final(order)
        self.gateway.paid(self.read(order)["payment_id"])
        self.supplier.commit_error=TimeoutError("Synthetic uncertain supplier acceptance")
        self.post(order,"zahlung-pruefen")
        self.assertEqual(self.read(order)["state"],"pruefung_noetig")
        self.assertEqual(self.read(order)["payment_status"],"bezahlt")
        self.assertFalse(self.rows("reifen_vertrag_outbox"))
        self.post(order,"zahlung-pruefen")
        self.assertEqual(len(self.supplier.commits),1)

    def test_new_terms_version_blocks_an_old_consent_before_provider_calls(self):
        order=self.ready()
        self.terms["sha256"]="c"*64
        self.assertEqual(self.final(order).status_code,503)
        self.assertFalse(self.supplier.reservations or self.gateway.creates)

    def test_sms_budget_configuration_must_be_an_integer_not_bool_or_text(self):
        for value in (True,"50",None):
            with self.subTest(value=value), patch.dict(portal.app.config,{"REIFEN_SMS_MAX_STARTS_PRO_TAG":value}):
                with portal.app.test_request_context("/"):
                    self.assertFalse(self.service.readiness()["ready"])

    def test_past_delivery_reservation_cannot_open_a_payment_session(self):
        order=self.ready()
        self.supplier.reserve_change["lieferdatum"]=(self.clock().date()-timedelta(days=1)).isoformat()
        self.assertGreaterEqual(self.final(order).status_code,400)
        self.assertFalse(self.gateway.creates or self.supplier.commits)

    def test_early_mounting_needs_two_separate_express_declarations(self):
        order=self.ready({"montage_datum":self.montage_day(7),"montage_uhrzeit":"09:00"},query={"montage":"ja"})
        for missing in ("frueher_montagebeginn","kenntnis_montagewiderruf"):
            with self.subTest(missing=missing):
                self.assertEqual(self.final(order,**{missing:""}).status_code,409)
                self.assertFalse(self.supplier.reservations or self.gateway.creates)
        self.assertIn(self.final(order).status_code,(302,303))
        self.assertEqual(self.read(order)["state"],"zahlung_offen")
        declarations=json.loads(self.read(order)["erklaerungen_json"])
        self.assertTrue(all(declarations[key] for key in ("frueher_montagebeginn","kenntnis_montagewiderruf")))

    def test_product_get_limiter_runs_before_provider_io(self):
        token=self.token()
        calls=[]
        def counted(query):
            calls.append(query)
            return self.provider(query)
        with patch.dict(portal.app.config,{"REIFENANGEBOT_PROVIDER":counted}):
            responses=[self.client.get("/reifen/bestellen/"+token+"/kauf") for _ in range(12)]
        self.assertTrue(any(response.status_code==429 for response in responses))
        self.assertEqual(len(calls),sum(response.status_code==200 for response in responses))
        self.assertFalse(self.rows() or self.supplier.reservations or self.sms.starts or self.gateway.creates)

    def test_past_inspect_delivery_and_supplier_details_are_not_exposed(self):
        order=self.ready()
        self.supplier.inspect_change["lieferdatum"]=(self.clock().date()-timedelta(days=1)).isoformat()
        response=self.final(order)
        self.assertEqual(response.status_code,409)
        self.assertFalse(self.supplier.reservations or self.gateway.creates)
        self.supplier.inspect_change.clear()
        with patch.object(self.supplier,"inspect",side_effect=RuntimeError("SECRET-VENDOR-TOKEN-AND-PRIVATE-URL")):
            response=self.final(order)
        self.assertEqual(response.status_code,409)
        self.assertNotIn("SECRET-VENDOR",response.get_data(as_text=True))
        self.assertFalse(self.supplier.reservations or self.gateway.creates)

    def test_changed_inspection_delivery_requires_an_updated_confirmation(self):
        order=self.ready()
        old=self.read(order)["lieferdatum"]
        new=(self.clock().date()+timedelta(days=5)).isoformat()
        self.supplier.inspect_change["lieferdatum"]=new
        self.supplier.reserve_change["lieferdatum"]=new
        self.assertEqual(self.final(order).status_code,409)
        self.assertEqual(self.read(order)["state"],"preis_geaendert")
        self.assertNotEqual(old,new)
        self.assertEqual(self.read(order)["lieferdatum"],new)
        self.assertFalse(self.supplier.reservations or self.gateway.creates or self.supplier.commits)
        self.assertIn(self.final(order).status_code,(302,303))
        self.assertEqual(len(self.supplier.reservations),1)
        self.assertEqual(self.read(order)["state"],"zahlung_offen")

    def test_changed_reservation_delivery_is_saved_then_same_hold_is_reused(self):
        order=self.ready()
        new=(self.clock().date()+timedelta(days=6)).isoformat()
        self.supplier.reserve_change["lieferdatum"]=new
        self.assertEqual(self.final(order).status_code,409)
        row=self.read(order)
        self.assertEqual(row["state"],"preis_geaendert")
        self.assertEqual(row["lieferdatum"],new)
        saved=json.loads(row["reservierung_json"])
        self.assertEqual(saved["lieferdatum"],new)
        self.assertTrue(saved["reservierung_id"])
        self.assertFalse(self.gateway.creates or self.supplier.commits)
        expiry=row["payment_expires_at"]
        self.clock.value+=timedelta(seconds=1)
        self.assertIn(self.final(order).status_code,(302,303))
        self.assertEqual(len(self.supplier.reservations),1)
        self.assertEqual(self.read(order)["payment_expires_at"],expiry)
        self.assertEqual(len(self.gateway.creates),1)

    def test_mounting_workshop_order_exists_only_after_verified_payment_and_supplier_commit(self):
        day=self.montage_day()
        order=self.ready({"montage_datum":day,"montage_uhrzeit":"12:00"},query={"montage":"ja"})
        before={row["id"] for row in self.rows("auftraege")}
        self.final(order)
        self.assertIsNone(self.read(order)["auftrag_id"])
        self.assertEqual({row["id"] for row in self.rows("auftraege")},before)
        self.gateway.paid(self.read(order)["payment_id"])
        self.post(order,"zahlung-pruefen")
        row=self.read(order)
        self.assertEqual(row["state"],"bestaetigt")
        self.assertTrue(row["auftrag_id"])
        created=[item for item in self.rows("auftraege") if item["id"] not in before]
        self.assertEqual(len(created),1)
        self.assertEqual(created[0]["schadenart"],"Reifenservice")
        self.assertEqual(created[0]["status"],2)
        self.assertEqual(created[0]["annahme_uhrzeit"],"12:00")
        intake=json.loads(created[0]["schaden_aufnahme_json"])
        self.assertEqual(intake["anliegen"],"reifenservice")
        self.assertEqual(intake["reifen_kauf_id"],order["id"])
        self.assertIs(intake["reifen_kauf_demo"],True)
        self.assertIn("TEST",created[0]["beschreibung"])
        self.post(order,"zahlung-pruefen")
        self.assertEqual(len([item for item in self.rows("auftraege") if item["id"] not in before]),1)

    def test_public_only_get_redirects_to_fixed_portal_and_posts_cannot_store(self):
        token=self.token()
        paths=("/reifen/bestellen/"+token+"/kauf","/reifen/vertrag-widerrufen")
        before={table:len(self.rows(table)) for table in ("reifen_kaeufe","reifen_widerruf")}
        with patch.object(portal,"PUBLIC_SITE_ONLY",True),patch.object(portal,"PORTAL_BASE_URL","https://portal.example.invalid"):
            for path in paths:
                with self.subTest(path=path):
                    response=self.client.get(path+"?next=https://attacker.invalid",base_url="https://shop.example.invalid")
                    self.assertEqual(response.status_code,302)
                    self.assertEqual(urlsplit(response.location).netloc,"portal.example.invalid")
                    self.assertEqual(urlsplit(response.location).path,path)
            for path in ("/reifen/kauf/"+token+"/entwurf","/reifen/widerruf/pruefen"):
                response=self.client.post(path,data={"name":"Unstored Synthetic Customer"},base_url="https://shop.example.invalid")
                self.assertEqual(response.status_code,503)
        self.assertEqual({table:len(self.rows(table)) for table in before},before)
        self.assertFalse(self.supplier.reservations or self.sms.starts or self.gateway.creates)

    def test_checkout_and_withdrawal_size_limits_precede_csrf(self):
        with patch.object(portal,"PUBLIC_SITE_ONLY",False):
            for path,size in (("/reifen/kauf/invalid/entwurf",16385),("/reifen/widerruf/pruefen",16385),("/webhooks/reifen/stripe",262145)):
                with self.subTest(path=path):
                    response=self.client.post(path,data=b"unused="+b"x"*size,content_type="application/x-www-form-urlencoded")
                    self.assertEqual(response.status_code,413)
        self.assertFalse(self.supplier.reservations or self.sms.starts or self.gateway.creates)

    def test_webhook_signature_and_retrieval_not_browser_session_control_acceptance(self):
        order=self.ready()
        self.final(order)
        sid=self.read(order)["payment_id"]
        self.gateway.paid(sid)
        event={"event_id":"evt_syntheticCore","session_id":sid,"ignored":False,"type":"checkout.session.completed","livemode":False}
        raw=json.dumps(event).encode()
        response=portal.app.test_client().post("/webhooks/reifen/stripe",data=raw,headers={"Stripe-Signature":"bad"},content_type="application/json")
        self.assertEqual(response.status_code,400)
        response=portal.app.test_client().post("/webhooks/reifen/stripe",data=raw,headers={"Stripe-Signature":"synthetic-verified-signature"},content_type="application/json")
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        response=portal.app.test_client().post("/webhooks/reifen/stripe",data=raw,headers={"Stripe-Signature":"synthetic-verified-signature"},content_type="application/json")
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(self.supplier.commits),1)
        self.assertEqual(len(self.rows("reifen_kauf_events")),1)

    def test_owned_edit_preserves_form_and_refreshes_expired_link_without_get_mutation(self):
        day = self.montage_day()
        hour = portal.reifen_slot_zeiten(day)[0]
        order = self.draft({"zahlungsart":"vor_ort", "montage_datum":day, "montage_uhrzeit":hour}, query={"montage":"ja"})
        with patch("itsdangerous.timed.TimestampSigner.get_timestamp", return_value=int(time.time())-901):
            expired = self.token(montage="ja")
        self.change(order, auswahl_token=expired)
        before = self.read(order)
        self.article_change["netto_einkauf"] = "110.00"
        with self.client.session_transaction() as values:
            session_before = dict(values)
        with patch.object(portal, "freie_reifen_slots", return_value=[hour]) as slots:
            response = self.client.get(order["url"] + "/bearbeiten")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("Set-Cookie", response.headers)
        slots.assert_called_once_with(day)
        html = response.get_data(as_text=True)
        for name, value in json.loads(before["kunde_json"]).items():
            self.assertIn(f'name="{name}"', html)
            self.assertIn(f'value="{value}"', html)
        self.assertRegex(html, r'name="zahlungsart" value="vor_ort"\s+checked')
        self.assertIn(f'value="{day}"', html)
        self.assertIn(f'value="{hour}" selected', html)
        self.assertRegex(html, r'name="standardmontage" value="ja"\s+checked')
        # 4 x 143.99 tire gross + 11.90 freight + existing 90.00 mounting.
        self.assertIn("677,86", html)
        renewed = re.search(r'action="/reifen/kauf/([^"/]+)/entwurf"', html).group(1)
        self.assertEqual(lade_auswahl_token(portal.app.secret_key, renewed),
                         {"query":{**QUERY,"montage":"ja"},"artikel_id":SKU})
        with self.assertRaises(ValueError):
            lade_auswahl_token(portal.app.secret_key, expired)
        self.assertEqual(self.read(order), before)
        self.assertEqual(len(self.rows()), 1)
        with self.client.session_transaction() as values:
            self.assertEqual(dict(values), session_before)
        self.assertFalse(self.sms.starts or self.sms.checks or self.gateway.creates or self.supplier.reservations or self.supplier.commits)

    def test_edit_requires_owner_current_mode_and_unlocked_unpaid_draft(self):
        order = self.draft()
        path = order["url"] + "/bearbeiten"
        fresh = Mock(wraps=self.service.fresh_order)
        with patch.object(self.service, "fresh_order", fresh):
            self.assertEqual(portal.app.test_client().get(path).status_code, 404)
            for change in ({"state":"zahlung_offen"}, {"state":"zahlung_bestaetigt"}, {"state":"bestaetigt"},
                           {"state":"lieferauftrag_laeuft"}, {"state":"pruefung_noetig"},
                           {"state":"entwurf","sms_state":"sending"}, {"sms_state":"checking"},
                           {"sms_state":"unknown"}, {"sms_state":"limit"}, {"sms_state":"pending","sms_checks":0}, {"sms_state":"pending","sms_checks":5}):
                self.change(order, **change)
                self.clear_rate()
                before = self.read(order)
                self.assertEqual(self.client.get(path).status_code, 409)
                self.assertEqual(self.read(order), before)
            self.change(order, state="entwurf", sms_state="", sms_checks=0)
            with patch.dict(portal.app.config, {"REIFEN_KAUF_MODE":"live"}):
                self.clear_rate()
                self.assertEqual(self.client.get(path).status_code, 409)
            for change in ({"payment_id":"cs_test_already_started"}, {"payment_id":None,"payment_status":"bezahlt"},
                           {"payment_status":"unbezahlt","reservierung_json":json.dumps({"reservierung_id":"existing-hold"})}):
                self.change(order, **change)
                self.clear_rate()
                self.assertEqual(self.client.get(path).status_code, 409)
        fresh.assert_not_called()
        self.assertFalse(self.sms.starts or self.gateway.creates or self.supplier.reservations or self.supplier.commits)

    def test_owned_draft_can_confirm_after_entry_token_expiry_using_current_exact_sku(self):
        order = self.ready()
        with patch("itsdangerous.timed.TimestampSigner.get_timestamp", return_value=int(time.time())-901):
            expired = self.token()
        self.change(order, auswahl_token=expired)
        self.assertEqual(self.client.get("/reifen/bestellen/"+expired+"/kauf").status_code, 409)
        response = self.final(order)
        self.assertIn(response.status_code, (302,303))
        self.assertEqual(self.read(order)["state"], "zahlung_offen")
        self.assertEqual(self.gateway.creates[0]["artikel_id"], SKU)
        self.assertEqual(self.gateway.creates[0]["amount_total"], 53550)
        self.assertEqual(len(self.gateway.creates), 1)

    def test_owned_expired_link_still_requires_current_price_and_stock_confirmation(self):
        order = self.ready()
        with patch("itsdangerous.timed.TimestampSigner.get_timestamp", return_value=int(time.time())-901):
            expired = self.token()
        self.change(order, auswahl_token=expired)
        self.article_change["netto_einkauf"] = "110.00"
        response = self.final(order)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.read(order)["state"], "preis_geaendert")
        self.assertFalse(self.gateway.creates or self.supplier.reservations)
        self.article_change["bestand"] = 0
        self.assertEqual(self.final(order).status_code, 409)
        self.assertFalse(self.gateway.creates or self.supplier.reservations)

    def test_mounting_error_form_uses_submitted_day_and_marks_invalid_day_without_other_slots(self):
        day = self.montage_day()
        order = self.draft({"montage_datum":day,"montage_uhrzeit":portal.reifen_slot_zeiten(day)[0]}, query={"montage":"ja"})
        data = {**json.loads(self.read(order)["kunde_json"]), "email":"invalid-address", "zahlungsart":"vor_ort",
                "montage_datum":day,"montage_uhrzeit":"14:00","standardmontage":"ja",portal.CSRF_FIELD_NAME:self.csrf()}
        endpoint = "/reifen/kauf/"+order["token"]+"/entwurf"
        with patch.object(portal,"freie_reifen_slots",return_value=["14:00"]) as slots:
            response = self.client.post(endpoint,data=data)
        self.assertEqual(response.status_code,400)
        slots.assert_called_once_with(day)
        self.assertIn(f'value="{day}"',response.text)
        self.assertIn('value="14:00" selected',response.text)
        self.assertNotIn('value="08:00"',response.text)
        with patch.object(portal,"freie_reifen_slots") as slots:
            response = self.client.post(endpoint,data={**data,"montage_datum":"invalid-date"})
        self.assertEqual(response.status_code,400)
        slots.assert_not_called()
        self.assertRegex(response.text,r'name="montage_datum"[^>]*aria-invalid="true"')
        self.assertIn('id="tire-montage-date-error"',response.text)
        self.assertNotIn('value="14:00"',response.text)
        self.assertFalse(self.gateway.creates or self.supplier.reservations)

    def test_exhausted_sms_attempts_render_contact_and_cannot_be_reset_by_edit_or_resend(self):
        order = self.draft()
        self.post(order,"telefon-code")
        for _ in range(5):
            self.assertEqual(self.post(order,"telefon-pruefen",{"code":"000000"}).status_code,409)
        self.assertEqual(self.read(order)["sms_state"],"limit")
        before = self.read(order)
        response = self.client.get(order["url"])
        self.assertIn("Die Telefonprüfung ist vorerst gesperrt",response.text)
        self.assertIn("Werkstatt anrufen",response.text)
        self.assertNotIn('action="'+order["url"]+'/telefon-pruefen"',response.text)
        self.assertNotIn('action="'+order["url"]+'/telefon-code"',response.text)
        self.assertNotIn("Angaben ändern",response.text)
        self.assertEqual(self.client.get(order["url"]+"/bearbeiten").status_code,409)
        self.assertEqual(self.post(order,"telefon-code").status_code,409)
        self.assertEqual(self.read(order),before)
        self.assertEqual(len(self.sms.starts),1)
        self.assertEqual(len(self.sms.checks),5)
        self.assertFalse(self.gateway.creates or self.supplier.reservations)
        # Previously stored pending rows with an exhausted counter must render
        # the same recovery state without any GET migration/write.
        self.change(order,sms_state="pending")
        before = self.read(order)
        self.assertEqual(self.service.order(before)["sms_status"],"limit")
        self.assertIn("Werkstatt anrufen",self.client.get(order["url"]).text)
        self.assertEqual(self.read(order),before)

    def test_malformed_inspection_delivery_is_redacted_before_customer_error_render(self):
        order = self.ready()
        for value in ("PRIVATE-SUPPLIER-SECRET", None, "2026-99-99", {"private":"PRIVATE-SUPPLIER-SECRET"}):
            self.supplier.inspect_change["lieferdatum"] = value
            response = self.final(order)
            self.assertEqual(response.status_code,409)
            self.assertNotIn("PRIVATE-SUPPLIER-SECRET",response.text)
            self.assertIn("gültiges Bereitstellungsdatum",response.text)
        self.assertFalse(self.gateway.creates or self.supplier.reservations)

    def setup_rims(self):
        self.rim_changes = {}
        cache = SimpleNamespace(load=Mock(side_effect=lambda: parse_felgen_feed(
            raw_feed(self.rim_changes), abgerufen_am=self.clock())))
        self.rim_provider = FelgenCatalogProvider(cache, policy=deepcopy(POLICY),
            private_view_authorized=lambda: True, clock=self.clock)
        portal.app.config["FELGENANGEBOT_PROVIDER"] = self.rim_provider
        self.terms["produktarten"] = ["reifen", "felgen"]
        for changes in (self.supplier.inspect_change, self.supplier.reserve_change, self.supplier.commit_change):
            changes["produktart"] = "felgen"

    def rim_token(self, quantity=4):
        return erstelle_felgen_auswahl_token(portal.app.secret_key, "1", quantity, clock=self.clock)

    def rim_draft(self, change=None):
        return self.draft({"standardmontage":"", **(change or {})}, selection_token=self.rim_token())

    def test_rim_readiness_requires_explicit_released_category_scope_and_all_existing_gates(self):
        self.setup_rims()
        with portal.app.test_request_context("/", base_url="http://localhost"):
            self.assertEqual(self.service.readiness()["produktart"], "reifen")
            self.assertEqual(self.service.readiness_for("felgen")["produktart"], "felgen")
            self.assertTrue(self.service.readiness_for("felgen")["ready"])
            for scope in (None, [], ["reifen"], "felgen", ["felgen", "felgen"], ["felgen", "zubehoer"], [True]):
                with self.subTest(scope=scope), patch.dict(self.terms, {"produktarten":scope}):
                    self.assertFalse(self.service.readiness_for("felgen")["ready"])
            with patch.dict(self.terms, {"produktarten":["reifen", "felgen", "komplettraeder"]}):
                self.assertTrue(self.service.readiness_for("reifen")["ready"])
                self.assertTrue(self.service.readiness_for("felgen")["ready"])
                self.assertFalse(self.service.readiness_for("komplettraeder")["ready"])
            for item in ({"freigegeben":False}, {"status":"entwurf"}, {"offene_entscheidungen":["unknown"]}):
                with patch.dict(self.terms,item):
                    self.assertFalse(self.service.readiness_for("felgen")["ready"])
            for key, value in (("REIFEN_KAUF_ENABLED",False), ("REIFEN_TERMS_APPROVED_SHA256","bad"),
                               ("REIFEN_KAUF_GATEWAY",None), ("FELGENANGEBOT_PROVIDER",None)):
                with patch.dict(portal.app.config,{key:value}):
                    self.assertFalse(self.service.readiness_for("felgen")["ready"])
            with patch.dict(portal.app.config,{"REIFENANGEBOT_PROVIDER":None}):
                self.assertTrue(self.service.readiness_for("felgen")["ready"])
            self.assertFalse(self.service.readiness_for("komplettraeder")["ready"])
        # Missing scope must also block the real POST before any supplier/SMS/payment IO.
        self.terms.pop("produktarten")
        token = self.rim_token()
        self.client.get("/raeder/bestellen/"+token+"/kauf")
        response = self.client.post("/reifen/kauf/"+token+"/entwurf",data={portal.CSRF_FIELD_NAME:self.csrf()})
        self.assertEqual(response.status_code,503)
        self.assertFalse(self.rows() or self.sms.starts or self.supplier.inspections or self.gateway.creates)

    def test_rim_pickup_full_customer_phone_commit_is_typed_owned_deduplicated_and_unpaid(self):
        self.setup_rims()
        order = self.rim_draft({"zahlungsart":"vor_ort", "amount_total":"1", "produktart":"reifen", "anzahl":"8"})
        row = self.read(order)
        model = self.service.order(row)
        self.assertEqual(model["produktart"], "felgen")
        self.assertEqual(model["query"], {"produktart":"felgen", "anzahl":4})
        self.assertEqual(model["produkt"]["produktart"], "felgen")
        self.assertEqual(model["amount_total"], 52360)
        self.assertEqual(model["produkt"]["preis"]["gesamt_felgen_brutto"], "523.60")
        self.assertFalse(row["montage_datum"] or row["montage_uhrzeit"] or row["slot_lead_id"])
        self.assertNotIn("groesse", model["produkt"])
        self.assertEqual(portal.app.test_client().get(order["url"]).status_code,404)
        self.assertEqual(self.final(order).status_code,409)
        self.verify(order)
        html = self.client.get(order["url"]).text
        self.assertIn("4 Felgen à 130,90", html)
        self.assertIn("Fahrzeugpassung ist nicht geprüft", html)
        self.assertNotIn("Bestätigte Reifenlieferung", html)
        self.assertNotIn("{%", html)
        self.assertIn(self.final(order).status_code,(302,303))
        self.assertIn(self.final(order).status_code,(302,303))
        row = self.read(order)
        self.assertEqual(row["state"], "bestellt_zahlung_offen")
        self.assertEqual(row["payment_status"], "vor_ort_offen")
        self.assertFalse(row["payment_id"] or row["auftrag_id"] or row["slot_lead_id"])
        self.assertFalse(self.gateway.creates)
        self.assertEqual(len(self.supplier.reservations),1)
        self.assertEqual(len(self.supplier.commits),1)
        self.assertEqual(self.supplier.commits[0]["produktart"], "felgen")
        self.assertEqual(len(self.rows("reifen_vertrag_outbox")),1)

    def test_rim_online_callback_uses_saved_category_without_owner_and_never_redirect_paid(self):
        self.setup_rims()
        order = self.rim_draft()
        self.verify(order)
        self.assertIn(self.final(order).status_code,(302,303))
        row = self.read(order)
        self.assertEqual(row["state"], "zahlung_offen")
        self.assertEqual(self.gateway.creates[0]["produktart"],"felgen")
        self.assertEqual(self.gateway.creates[0]["amount_total"],52360)
        self.assertFalse(self.supplier.commits)
        self.client.get("/reifen/zahlung/erfolg/"+order["id"]+"?paid=1&produktart=reifen")
        self.assertEqual(self.read(order)["state"],"zahlung_offen")
        self.gateway.paid(row["payment_id"])
        event={"event_id":"evt_rim_test", "session_id":row["payment_id"], "ignored":False}
        self.terms.pop("produktarten")  # New purchases close; existing paid contract can still settle.
        with patch.dict(portal.app.config,{"REIFEN_KAUF_ENABLED":False}):
            for _ in range(2):
                response=portal.app.test_client().post("/webhooks/reifen/stripe",data=json.dumps(event),
                    headers={"Stripe-Signature":"synthetic-verified-signature"},content_type="application/json")
                self.assertEqual(response.status_code,200)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual(self.read(order)["payment_status"],"bezahlt")
        self.assertEqual(len(self.supplier.commits),1)
        self.assertEqual(len(self.rows("reifen_vertrag_outbox")),1)

    def test_rim_invalid_readonly_cross_salt_tampered_and_expired_tokens_cannot_start(self):
        from itsdangerous import URLSafeTimedSerializer
        self.setup_rims()
        token = self.rim_token()
        readonly = URLSafeTimedSerializer(portal.app.secret_key, salt="felgen-auswahl-v1").dumps({"artikel_id":"1","anzahl":4})
        expired = erstelle_felgen_auswahl_token(portal.app.secret_key, "1", 4,
            clock=lambda: self.clock()-timedelta(seconds=901))
        candidates = ("felgen."+readonly, "felgen."+self.token(), token.removeprefix("felgen."),
                      token+"X", expired)
        for candidate in candidates:
            with self.subTest(token=candidate[:30]):
                self.clear_rate()
                self.assertEqual(self.client.get("/raeder/bestellen/"+candidate+"/kauf").status_code,409)
                self.assertEqual(self.client.post("/reifen/kauf/"+candidate+"/entwurf",
                    data={portal.CSRF_FIELD_NAME:self.csrf()}).status_code,409)
        self.assertFalse(self.rows() or self.supplier.inspections or self.sms.starts or self.gateway.creates)

    def test_rim_owned_expiry_edit_and_current_exact_price_stock_revalidation(self):
        self.setup_rims()
        order = self.rim_draft()
        self.clock.value += timedelta(minutes=16)
        self.rim_changes["Einkaufspreis"] = "110,00"
        before = self.read(order)
        self.assertEqual(self.client.get("/raeder/bestellen/"+order["token"]+"/kauf").status_code,409)
        response = self.client.get(order["url"]+"/bearbeiten")
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.read(order),before)
        self.assertIn("575,96",response.text)
        renewed=re.search(r'action="/reifen/kauf/([^"/]+)/entwurf"',response.text).group(1)
        selection=self.service.resolver().from_token(renewed,produktart="felgen")
        self.assertEqual((selection.artikel_id,selection.anzahl),("1",4))
        self.assertNotIn('name="montage_datum"',response.text)
        self.verify(order)
        self.assertEqual(self.final(order).status_code,409)
        self.assertEqual(self.read(order)["state"],"preis_geaendert")
        self.assertFalse(self.gateway.creates or self.supplier.reservations)
        self.rim_changes["Lagerbestand"]="0"
        self.assertEqual(self.final(order).status_code,409)
        self.assertFalse(self.gateway.creates or self.supplier.reservations)
        self.rim_changes["Lagerbestand"]="8"
        self.assertIn(self.final(order).status_code,(302,303))
        self.assertEqual(self.gateway.creates[0]["amount_total"],57596)

    def test_rim_supplier_must_explicitly_acknowledge_category_at_each_phase(self):
        self.setup_rims()
        for category in (None,"reifen","komplettraeder"):
            self.supplier.inspect_change["produktart"]=category
            with self.assertRaises(ValueError):
                self.service.inspect(*self.service.fresh(self.rim_token()))
        self.supplier.inspect_change["produktart"]="felgen"
        for phase in ("reserve_change","commit_change"):
            with self.subTest(phase=phase):
                self.client=portal.app.test_client()
                order=self.rim_draft({"zahlungsart":"vor_ort"})
                self.verify(order)
                getattr(self.supplier,phase).pop("produktart")
                response=self.final(order)
                self.assertEqual(response.status_code,409)
                self.assertEqual(self.read(order)["state"],"pruefung_noetig")
                self.assertFalse(self.read(order)["accepted_at"])
                self.assertFalse(self.rows("reifen_vertrag_outbox") or self.gateway.creates)
                getattr(self.supplier,phase)["produktart"]="felgen"

    def test_rim_rejects_tire_mounting_and_stored_cross_category_before_external_actions(self):
        self.setup_rims()
        order=self.rim_draft()
        before=self.read(order)
        data={**json.loads(before["kunde_json"]),"zahlungsart":"online","montage_datum":self.montage_day(),
              "montage_uhrzeit":"09:00","standardmontage":"ja",portal.CSRF_FIELD_NAME:self.csrf()}
        response=self.client.post("/reifen/kauf/"+order["token"]+"/entwurf",data=data)
        self.assertEqual(response.status_code,400)
        self.assertEqual(len(self.rows()),1)
        self.assertEqual(self.read(order),before)
        wrong=json.loads(before["produkt_json"])
        wrong["produktart"]="reifen"
        self.change(order,produkt_json=json.dumps(wrong))
        with self.assertRaises(ValueError):
            self.service.fresh_order(self.read(order))
        with self.assertRaises(ValueError):
            self.service.order(self.read(order))
        self.assertFalse(self.gateway.creates or self.supplier.reservations or self.supplier.commits or self.sms.starts)

    def test_later_mounting_has_no_mandatory_early_waiver_and_holiday_boundary_is_conservative(self):
        order=self.ready({"montage_datum":self.montage_day(30),"montage_uhrzeit":"09:00"},query={"montage":"ja"})
        html=self.client.get(order["url"]).text
        self.assertNotIn('name="frueher_montagebeginn"',html)
        self.assertNotIn('name="kenntnis_montagewiderruf"',html)
        self.assertIn(self.final(order,frueher_montagebeginn="",kenntnis_montagewiderruf="").status_code,(302,303))
        self.assertEqual(self.read(order)["state"],"zahlung_offen")
        # Christmas + weekend extends a Dec 25 service deadline to Dec 28.
        self.clock.value=datetime(2026,12,11,10,tzinfo=timezone.utc)
        row={**self.read(order),"montage_datum":"2026-12-28","reservierung_json":"{}"}
        self.assertTrue(self.service.early_mounting(row))
        row["montage_datum"]="2026-12-29"
        self.assertFalse(self.service.early_mounting(row))

    def test_long_supplier_hold_rechecks_early_service_before_payment_and_delayed_acceptance(self):
        day=self.montage_day(17)
        order=self.ready({"montage_datum":day,"montage_uhrzeit":"09:00"},query={"montage":"ja"})
        self.assertNotIn('name="frueher_montagebeginn"',self.client.get(order["url"]).text)
        self.supplier.reserve_change["gueltig_bis"]=(self.clock()+timedelta(days=10)).isoformat()
        response=self.final(order,frueher_montagebeginn="",kenntnis_montagewiderruf="")
        self.assertEqual(response.status_code,409)
        self.assertEqual(self.read(order)["state"],"preis_geaendert")
        self.assertIn('name="frueher_montagebeginn"',response.text)
        self.assertFalse(self.gateway.creates or self.supplier.commits)
        self.assertIn(self.final(order).status_code,(302,303))
        self.assertEqual(len(self.supplier.reservations),1)
        # Simulate an older already-paid draft lacking declarations and a late
        # callback: never accept a now-early service without the required choice.
        self.change(order,erklaerungen_json=json.dumps({"bestellpflicht":True,"bedingungen":True}))
        self.clock.value += timedelta(days=7)
        self.gateway.paid(self.read(order)["payment_id"])
        response=self.post(order,"zahlung-pruefen")
        self.assertEqual(response.status_code,409)
        self.assertEqual(self.read(order)["state"],"pruefung_noetig")
        self.assertFalse(self.supplier.commits or self.rows("reifen_vertrag_outbox"))


if __name__=="__main__":
    unittest.main(verbosity=2)
