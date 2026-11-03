"""Automatic checkout against real parsers/journals, fake HTTP and temporary DB.

No environment, private provider file or real socket is available to the suite.
The existing isolated portal fixture supplies customer/CSRF/SMS behavior.
"""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_reifen_kauf as legacy
from reifen_bestellung import erstelle_auswahl_token
import reifen_tyresystem_auto as auto
from tyresystem_api import TyreSystemInquiryClient, account_fingerprint
import tyresystem_order as supplier
from test_tyresystem_order import Response, HTTP, ack


CONFIG = {"TYRESYSTEM_API_USERNAME":"synthetic-auto-account", "TYRESYSTEM_API_PASSWORD":"synthetic-password",
          "TYRESYSTEM_API_MODE":"test"}
SKU, EAN = "123", "0012345678905"


class InquiryHTTP:
    def __init__(self):
        self.calls, self.price, self.stock = [], "100.00", 8
    def get(self, url, **kwargs):
        self.calls.append(url)
        sku, quantity = url.rsplit("/", 2)[1:]
        return Response({"inquiryResponse":{"offerData":{"idArticle":sku,"ean":EAN,"price":self.price,
            "amount":int(quantity),"description":"Synthetic tire","errorCode":0,"stock":self.stock}}}, url=url)


class Gateway(legacy.Gateway):
    supports_manual_capture = True
    def __init__(self, case):
        super().__init__()
        self.case, self.captures, self.cancels, self.paid_orders = case, [], [], set()
        self.lose_capture_response = False
    def create(self, order):
        assert order["payment_capture_method"] == "manual"
        return super().create(order)
    def authorize(self, session_id):
        self.sessions[session_id].update(status="authorized", payment_status="authorized",
            payment_capture_method="manual", payment_intent="pi_SyntheticAuto"+session_id.rsplit("_",1)[-1], amount_received=0,
            amount_capturable=self.sessions[session_id]["amount_total"],
            capture_before=int(self.case.clock().timestamp())+3600)
    def result(self, order, state):
        return {"id":order["payment_intent"],"payment_intent":order["payment_intent"],"livemode":False,
            "amount_total":order["amount_total"],"amount_received":order["amount_total"] if state=="succeeded" else 0,
            "amount_capturable":0,"currency":"eur","reference":order["id"],"quote_hash":order["quote_hash"],
            "payment_capture_method":"manual","status":state,"payment_status":"paid" if state=="succeeded" else "canceled"}
    def capture(self, order):
        record = self.case.service.auto().record(order["id"])
        assert record["state"] == "capturing" and json.loads(record["receipt_json"])["bestaetigt"] is True
        assert self.case.rows("tyresystem_order_attempts")[0]["state"] == "acknowledged"
        if order["id"] not in self.paid_orders:
            self.captures.append(deepcopy(order))
            self.paid_orders.add(order["id"])
            if self.lose_capture_response:
                self.lose_capture_response = False
                raise TimeoutError("Synthetic lost capture response")
        return self.result(order, "succeeded")
    def cancel_authorization(self, order):
        self.cancels.append(deepcopy(order))
        return self.result(order, "canceled")


class AutoCheckoutTests(unittest.TestCase):
    # Reuse the disposable portal fixture, not its legacy test methods.
    setUpClass = classmethod(legacy.ReifenKaufTest.setUpClass.__func__)
    for _name in ("rows", "read", "change", "clear_rate", "csrf", "post", "verify", "final", "ready"):
        locals()[_name] = getattr(legacy.ReifenKaufTest, _name)

    def setUp(self):
        legacy.ReifenKaufTest.setUp(self)
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            block = patch(target, side_effect=AssertionError("No network"))
            block.start()
            self.addCleanup(block.stop)
        db = legacy.portal.get_db()
        for table in auto.BACKUP_TABLES:
            db.execute("DELETE FROM "+table)
        db.commit()
        db.close()
        self.terms.update(produktarten=["reifen", "felgen"], beschaffungsverfahren=[auto.VERFAHREN])
        self.inquiry_http, self.order_http = InquiryHTTP(), HTTP()
        self.reader = TyreSystemInquiryClient(CONFIG, http=self.inquiry_http, clock=self.clock,
            authorize=lambda sku, qty:self.service.auto_authorize_inquiry(sku, qty))
        client = supplier.TyreSystemOrderClient(CONFIG, http=self.order_http, clock=self.clock,
            authorize=lambda envelope:self.service.auto_authorize_order(envelope))
        self.orders = supplier.DurableTyreSystemOrders(legacy.portal.get_db, client)
        self.gateway = Gateway(self)
        self.policy = {"schema":1,"freigegeben":True,"nachweis":"synthetic-reviewed-policy",
            "configured_mode":"test","account_fingerprint":account_fingerprint(CONFIG["TYRESYSTEM_API_USERNAME"],"test"),
            "api_modus_bestaetigt":True,"api_modus_nachweis":"synthetic-supplier-mode-proof",
            "stripe_manual_bestaetigt":True,"stripe_manual_nachweis":"synthetic-key-permission-proof",
            "preis_bestaetigt":True,"preis_nachweis":"synthetic-net-unit-and-freight-proof",
            "waehrung":"EUR","preisbasis":"net_unit","mwst_satz":"19.00","aufschlag_prozent":"10.00",
            "einzelversand_netto":"6.00","frachtfrei_ab":2,"auftragsannahme_bestaetigt":True,
            "auftragsannahme_nachweis":"synthetic-full-quantity-ack-proof","lieferprognose":"1–4 Werktage",
            "delivery_address":{"neutraldelivery":0,"salutation":"Firma","name":"Synthetic Workshop GmbH",
                "street":"Werkstattstraße 1","zipcode":"74821","city":"Mosbach","countrycode":"DE"},
            "artikel_freigaben":{SKU:{"produktart":"reifen","ean":EAN,"paketversandfaehig":True,
                "nachweis":"synthetic-parcel-approval"}}}
        legacy.portal.app.config.update(REIFEN_BESCHAFFUNG_VERFAHREN=auto.VERFAHREN,
            REIFEN_TYRESYSTEM_AUTO_POLICY=self.policy,REIFEN_TYRESYSTEM_INQUIRY=self.reader,
            REIFEN_TYRESYSTEM_ORDERS=self.orders,REIFEN_KAUF_GATEWAY=self.gateway)
        self.addCleanup(lambda: [legacy.portal.app.config.pop(key,None) for key in (
            "REIFEN_BESCHAFFUNG_VERFAHREN","REIFEN_TYRESYSTEM_AUTO_POLICY","REIFEN_TYRESYSTEM_INQUIRY","REIFEN_TYRESYSTEM_ORDERS")])

    def provider(self, query):
        result = legacy.ReifenKaufTest.provider(self, query)
        result["angebote"][0].update(artikel_id=SKU,ean=EAN,fracht_netto="6.00" if int(query["anzahl"])<2 else "0.00")
        return result

    def token(self, **query):
        with patch("itsdangerous.timed.time.time", return_value=self.clock().timestamp()):
            return erstelle_auswahl_token(legacy.portal.app.secret_key, {**legacy.QUERY,**query}, SKU)

    def draft(self, change=None, **kwargs):
        return legacy.ReifenKaufTest.draft(self, change, **kwargs)

    def authorized(self):
        order = self.ready()
        response = self.final(order)
        self.assertEqual(response.status_code,302)
        self.gateway.authorize(self.read(order)["payment_id"])
        return order

    def reconcile(self, order):
        with legacy.portal.app.test_request_context():
            self.service.reconcile(order["id"])

    def test_new_draft_freezes_flow_without_reservation_date_or_supplier_order(self):
        order = self.draft()
        row, flow = self.read(order), self.service.auto().record(order["id"])
        self.assertEqual(flow["state"],"draft")
        self.assertEqual(flow["snapshot"]["ean"],EAN)
        self.assertEqual(row["lieferdatum"],"")
        self.assertEqual(row["reservierung_json"],"{}")
        self.assertEqual(self.supplier.inspections+self.supplier.reservations+self.supplier.commits,[])
        self.assertEqual(len(self.order_http.calls),0)
        page = self.client.get(order["url"]).get_data(as_text=True)
        self.assertIn("Lieferprognose",page)
        self.assertNotIn("Bestätigte Reifenlieferung",page)
        self.assertNotIn("Den bestätigten Bereitstellungstermin sehen",page)

    def test_flow_insert_failure_rolls_back_customer_draft_too(self):
        token = self.token()
        self.client.get("/reifen/bestellen/"+token+"/kauf")
        with patch.object(auto.AutoBeschaffung,"insert",side_effect=auto.AutoBeschaffungFehler()):
            response = self.client.post("/reifen/kauf/"+token+"/entwurf",data={
                "name":"Synthetic Customer","email":"customer@example.invalid","telefon":"+491711234567",
                "strasse":"Teststraße 1","plz":"74821","ort":"Mosbach","zahlungsart":"online",
                legacy.portal.CSRF_FIELD_NAME:self.csrf()})
        self.assertEqual(response.status_code,400)
        self.assertEqual(self.rows(),[])
        self.assertEqual(self.rows("reifen_tyresystem_auto"),[])
        self.assertEqual(self.order_http.calls,[])

    def test_online_authorization_supplier_ack_capture_paid_outbox_exactly_once(self):
        order = self.authorized()
        self.assertEqual(self.read(order)["payment_status"],"offen")
        self.reconcile(order)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual(self.read(order)["payment_status"],"bezahlt")
        self.assertEqual(self.service.auto().record(order["id"])["state"],"complete")
        self.assertEqual(len(self.rows("reifen_vertrag_outbox")),1)
        self.reconcile(order)
        self.assertEqual((len(self.order_http.calls),len(self.gateway.captures)),(1,1))
        self.assertEqual(self.supplier.reservations,[])
        request = self.rows("tyresystem_order_attempts")[0]["request_json"]
        self.assertIn("Synthetic Workshop GmbH",request)
        self.assertNotIn("Synthetic Customer",request)

    def test_actual_stripe_adapter_contract_with_fake_sdk_authorizes_then_captures(self):
        import reifen_zahlung as payment
        from test_reifen_zahlung import FakeSessions, FakeIntents, FakeCharges, CONFIG as STRIPE_CONFIG
        sessions, intents, charges = FakeSessions(int(self.clock().timestamp())), FakeIntents(), FakeCharges()
        client = SimpleNamespace(v1=SimpleNamespace(checkout=SimpleNamespace(sessions=sessions),
                                                    payment_intents=intents,charges=charges))
        gateway = payment.StripeReifenGateway(STRIPE_CONFIG,client=client,clock=lambda:int(self.clock().timestamp()))
        legacy.portal.app.config["REIFEN_KAUF_GATEWAY"] = gateway
        order = self.ready()
        self.assertEqual(self.final(order).status_code,302)
        sid = self.read(order)["payment_id"]
        saved = sessions.saved[sid]
        saved.update(status="complete",payment_status="unpaid",payment_intent="pi_SyntheticSdk42")
        intents.saved["pi_SyntheticSdk42"] = {"id":"pi_SyntheticSdk42","object":"payment_intent",
            "livemode":False,"currency":"eur","amount":saved["amount_total"],"capture_method":"manual",
            "metadata":deepcopy(saved["metadata"]),"status":"requires_capture","amount_capturable":saved["amount_total"],
            "amount_received":0,"latest_charge":"ch_SyntheticSdk42"}
        charges.saved["ch_SyntheticSdk42"] = {"id":"ch_SyntheticSdk42","object":"charge","livemode":False,
            "payment_intent":"pi_SyntheticSdk42","currency":"eur","amount":saved["amount_total"],
            "captured":False,"paid":True,"amount_captured":0,"payment_method_details":{"type":"card",
                "card":{"capture_before":int(self.clock().timestamp())+3600}}}
        self.reconcile(order)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual((len(self.order_http.calls),len(intents.captures)),(1,1))
        self.assertEqual(sessions.creates[0][0]["payment_intent_data"]["capture_method"],"manual")

    def test_pickup_payment_orders_automatically_but_remains_unpaid(self):
        order = self.ready({"zahlungsart":"vor_ort"})
        self.assertEqual(self.final(order).status_code,302)
        row = self.read(order)
        self.assertEqual((row["state"],row["payment_status"]),("bestellt_zahlung_offen","vor_ort_offen"))
        self.assertEqual(len(self.order_http.calls),1)
        self.assertEqual(self.gateway.creates+self.gateway.captures,[])

    def test_real_rim_provider_uses_typed_quantity_and_never_fakes_fitment(self):
        legacy.ReifenKaufTest.setup_rims(self)
        self.addCleanup(lambda:legacy.portal.app.config.pop("FELGENANGEBOT_PROVIDER",None))
        self.rim_changes.update(Artikelnummer=SKU,ean=EAN)
        self.policy["artikel_freigaben"][SKU]["produktart"] = "felgen"
        token = legacy.erstelle_felgen_auswahl_token(legacy.portal.app.secret_key,SKU,2,clock=self.clock)
        order = self.draft({"standardmontage":""},selection_token=token)
        self.verify(order)
        self.assertEqual(self.final(order).status_code,302)
        self.gateway.authorize(self.read(order)["payment_id"])
        self.reconcile(order)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        model = self.gateway.captures[0]
        self.assertEqual((model["produktart"],model["anzahl"]),("felgen",2))
        self.assertEqual(model["produkt"]["kennungspassung"],"nicht_geprueft")
        self.assertNotIn("groesse",model["produkt"])
        self.assertNotIn("montage_brutto",model["produkt"]["preis"])
        request = json.loads(self.rows("tyresystem_order_attempts")[0]["request_json"])
        self.assertEqual((request["produktart"],request["anzahl"]),("felgen",2))

    def test_unknown_supplier_and_abandoned_sending_never_repeat_post(self):
        order = self.authorized()
        self.order_http.response = TimeoutError("synthetic supplier timeout")
        with self.assertRaises(auto.AutoBeschaffungFehler):
            self.reconcile(order)
        self.reconcile(order)
        self.assertEqual(len(self.order_http.calls),1)
        self.assertEqual(self.gateway.captures+self.gateway.cancels,[])
        self.assertEqual(self.read(order)["state"],"pruefung_noetig")

    def test_acknowledged_capture_timeout_recovers_without_supplier_resend(self):
        order = self.authorized()
        self.gateway.lose_capture_response = True
        with self.assertRaises(auto.AutoBeschaffungFehler):
            self.reconcile(order)
        self.assertEqual(self.service.auto().record(order["id"])["state"],"capture_unknown")
        self.reconcile(order)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual((len(self.order_http.calls),len(self.gateway.captures)),(1,1))

    def test_crash_before_supplier_journal_never_creates_first_post_on_restart(self):
        order = self.authorized()
        with patch.object(self.orders,"submit_once",side_effect=SystemExit("Synthetic crash")):
            with self.assertRaises(SystemExit):
                self.reconcile(order)
        self.assertEqual(self.service.auto().record(order["id"])["state"],"sending")
        record = self.service.auto().record(order["id"])
        self.assertFalse(self.service.auto_authorize_order({"reference":order["id"],
            "request":json.loads(record["request_json"]),"request_sha256":record["request_sha256"],
            "configured_mode":"test","account_fingerprint":self.policy["account_fingerprint"]}))
        self.reconcile(order)
        self.assertEqual(self.service.auto().record(order["id"])["state"],"unknown")
        self.assertEqual(self.order_http.calls,[])

    def test_supplier_journal_ack_survives_crash_before_core_ack(self):
        order = self.authorized()
        original = auto.AutoBeschaffung.change
        def crash(instance,record,state,**values):
            if state == "acknowledged":
                raise SystemExit("Synthetic crash after supplier ACK")
            return original(instance,record,state,**values)
        with patch.object(auto.AutoBeschaffung,"change",crash):
            with self.assertRaises(SystemExit):
                self.reconcile(order)
        self.assertEqual(self.rows("tyresystem_order_attempts")[0]["state"],"acknowledged")
        self.reconcile(order)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual((len(self.order_http.calls),len(self.gateway.captures)),(1,1))

    def test_concurrent_callback_is_suppressed_and_terminal_crash_does_not_reopen(self):
        order = self.authorized()
        self.order_http.callback = lambda *_:self.reconcile(order)
        original = auto.AutoBeschaffung.change
        def crash(instance,record,state,**values):
            if state == "complete":
                raise SystemExit("Synthetic crash after local acceptance")
            return original(instance,record,state,**values)
        with patch.object(auto.AutoBeschaffung,"change",crash):
            with self.assertRaises(SystemExit):
                self.reconcile(order)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.reconcile(order)
        self.assertEqual(self.read(order)["state"],"bestaetigt")
        self.assertEqual((len(self.order_http.calls),len(self.gateway.captures),len(self.rows("reifen_vertrag_outbox"))),(1,1,1))

    def test_pending_recovery_includes_only_bound_mode_and_known_safe_states(self):
        order = self.authorized()
        self.assertIn(order["id"],self.service.auto_pending_ids())
        self.gateway.lose_capture_response = True
        with self.assertRaises(auto.AutoBeschaffungFehler):
            self.reconcile(order)
        self.assertIn(order["id"],self.service.auto_pending_ids())
        self.reconcile(order)
        self.assertNotIn(order["id"],self.service.auto_pending_ids())
        with self.assertRaises(ValueError):
            self.service.auto_pending_ids(21)

    def test_price_or_stock_change_cancels_authorization_before_supplier_post(self):
        for change in ({"price":"101.00"},{"stock":3}):
            with self.subTest(change=change):
                order = self.authorized()
                self.inquiry_http.price,self.inquiry_http.stock="100.00",8
                for key,value in change.items():
                    setattr(self.inquiry_http,key,value)
                self.reconcile(order)
                self.assertEqual(self.read(order)["state"],"nicht_angenommen")
                self.assertEqual(len(self.order_http.calls),0)
                self.inquiry_http.price,self.inquiry_http.stock="100.00",8

    def test_known_supplier_rejection_cancels_not_captures_and_no_retry(self):
        order = self.authorized()
        self.order_http.response = Response(ack(orderStatus=False,error=1,ordernumber=""))
        self.reconcile(order)
        self.reconcile(order)
        self.assertEqual(self.read(order)["payment_status"],"autorisation_aufgehoben")
        self.assertEqual((len(self.order_http.calls),len(self.gateway.cancels),len(self.gateway.captures)),(1,1,0))

    def test_wrong_payment_binding_or_deadline_cannot_procure(self):
        order = self.authorized()
        self.gateway.sessions[self.read(order)["payment_id"]]["capture_before"] = int(self.clock().timestamp())
        with self.assertRaises(auto.AutoBeschaffungFehler):
            self.reconcile(order)
        self.assertEqual(len(self.order_http.calls),0)

    def test_order_mode_quote_and_snapshot_bindings_fail_before_supplier_io(self):
        order = self.authorized()
        with patch.dict(legacy.portal.app.config,{"REIFEN_KAUF_MODE":"live"}):
            with self.assertRaises(auto.AutoBeschaffungFehler):
                self.reconcile(order)
        self.assertEqual(len(self.order_http.calls),0)
        self.change(order,quote_hash="f"*64)
        with self.assertRaises(auto.AutoBeschaffungFehler):
            self.reconcile(order)
        self.assertEqual(len(self.order_http.calls),0)

    def test_inquiry_and_order_authorization_are_denied_outside_scoped_claim(self):
        self.assertIs(self.service.auto_authorize_inquiry(SKU,4),False)
        with self.assertRaises(ValueError):
            self.reader.inquire(SKU,4)
        self.assertEqual(self.inquiry_http.calls,[])
        self.assertIs(self.service.auto_authorize_order({}),False)

    def test_policy_mode_permissions_terms_and_mounting_fail_closed(self):
        with legacy.portal.app.test_request_context():
            self.assertTrue(self.service.readiness_for("reifen")["ready"])
            for field in ("api_modus_bestaetigt","stripe_manual_bestaetigt","auftragsannahme_bestaetigt"):
                with self.subTest(field=field),patch.dict(self.policy,{field:False}):
                    self.assertFalse(self.service.readiness_for("reifen")["ready"])
            with patch.dict(self.policy,{"account_fingerprint":"b"*64}):
                self.assertFalse(self.service.readiness_for("reifen")["ready"])
            with patch.dict(self.terms,{"beschaffungsverfahren":auto.VERFAHREN}):
                self.assertFalse(self.service.readiness_for("reifen")["ready"])
            query, product = self.service.fresh(self.token())
            with self.assertRaises(auto.AutoBeschaffungFehler):
                self.service.auto().preflight({**query,"montage":"ja"},product)
        self.assertEqual(self.inquiry_http.calls,[])


if __name__ == "__main__":
    unittest.main()
