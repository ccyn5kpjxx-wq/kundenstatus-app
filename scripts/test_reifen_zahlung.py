"""Stripe SDK adapter contract using mocks and locally signed fake webhooks only."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import reifen_zahlung as payment


CONFIG = {"REIFEN_STRIPE_MODE": "test", "REIFEN_STRIPE_SECRET_KEY": "sk_test_synthetic_unusable_key",
          "REIFEN_STRIPE_WEBHOOK_SECRET": "whsec_synthetic_unusable_secret",
          "REIFEN_STRIPE_PUBLIC_ORIGIN": "https://reifen.example.invalid",
          "REIFEN_STRIPE_ALLOWED_REDIRECT_HOSTS": ["reifen.example.invalid"]}


class FakeSessions:
    def __init__(self, now):
        self.now = now
        self.creates = []
        self.retrieves = []
        self.saved = {}
        self.keys = {}
        self.error = None
        self.response_changes = {}

    def create(self, params, *, options):
        self.creates.append((deepcopy(params), deepcopy(options)))
        if self.error:
            raise self.error
        key = options["idempotency_key"]
        if key in self.keys:
            previous, sid = self.keys[key]
            if previous != params:
                raise RuntimeError("Mock idempotency parameters changed")
            return deepcopy(self.saved[sid])
        sid = "cs_test_MockedSession" + str(len(self.saved) + 1)
        data = {"id": sid, "object": "checkout.session", "mode": "payment", "livemode": False,
                "status": "open", "payment_status": "unpaid", "currency": "eur",
                "amount_total": params["line_items"][0]["price_data"]["unit_amount"],
                "metadata": deepcopy(params["metadata"]), "client_reference_id": params["client_reference_id"],
                "expires_at": params["expires_at"], "payment_intent": None,
                "url": "https://checkout.stripe.com/c/pay/" + sid, **self.response_changes}
        self.keys[key] = (deepcopy(params), sid)
        self.saved[sid] = data
        return deepcopy(data)

    def retrieve(self, sid):
        self.retrieves.append(sid)
        if self.error:
            raise self.error
        return deepcopy(self.saved[sid])


class FakeIntents:
    def __init__(self):
        self.saved = {}
        self.retrieves, self.captures, self.cancels = [], [], []
        self.error = self.capture_error = self.cancel_error = None
        self.capture_response_lost = self.cancel_response_lost = False

    def retrieve(self, intent):
        self.retrieves.append(intent)
        if self.error: raise self.error
        return deepcopy(self.saved[intent])

    def capture(self, intent, params, *, options):
        self.captures.append((intent, deepcopy(params), deepcopy(options)))
        if self.capture_error: raise self.capture_error
        self.saved[intent].update(status="succeeded", amount_capturable=0, amount_received=params["amount_to_capture"])
        if self.capture_response_lost: raise TimeoutError("PRIVATE capture response lost")
        return deepcopy(self.saved[intent])

    def cancel(self, intent, params, *, options):
        self.cancels.append((intent, deepcopy(params), deepcopy(options)))
        if self.cancel_error: raise self.cancel_error
        self.saved[intent].update(status="canceled", amount_capturable=0, amount_received=0)
        if self.cancel_response_lost: raise TimeoutError("PRIVATE cancel response lost")
        return deepcopy(self.saved[intent])


class FakeCharges:
    def __init__(self):
        self.saved, self.retrieves = {}, []
        self.error = None

    def retrieve(self, charge):
        self.retrieves.append(charge)
        if self.error: raise self.error
        return deepcopy(self.saved[charge])


class StripeReifenGatewayTest(unittest.TestCase):
    def setUp(self):
        self.clock_value = int(time.time())
        self.sessions = FakeSessions(self.clock_value)
        self.intents, self.charges = FakeIntents(), FakeCharges()
        self.client = SimpleNamespace(v1=SimpleNamespace(checkout=SimpleNamespace(sessions=self.sessions),
                                                       payment_intents=self.intents, charges=self.charges))
        self.gateway = payment.StripeReifenGateway(CONFIG, client=self.client, clock=lambda: self.clock_value)
        self.order = {"id": "reifenOrder123", "quote_hash": "b" * 64, "amount_total": 62550,
                      "currency": "eur", "payment_attempt": "1", "payment_expires_at": self.clock_value + 3600,
                      "payment_created_at": self.clock_value,
                      "kunde": {"name": "Synthetic", "email": "test@example.invalid", "telefon": "+491700000000"}}
        for name in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
            blocker = patch(name, side_effect=AssertionError("No network in payment tests"))
            blocker.start()
            self.addCleanup(blocker.stop)

    def signed(self, event, *, timestamp=None, secret=None):
        body = json.dumps(event, separators=(",", ":")).encode()
        stamp = self.clock_value if timestamp is None else timestamp
        signature = hmac.new((secret or CONFIG["REIFEN_STRIPE_WEBHOOK_SECRET"]).encode(),
                             str(stamp).encode() + b"." + body, hashlib.sha256).hexdigest()
        return body, f"t={stamp},v1={signature}"

    def event(self, **changes):
        return {"id": "evt_MockedEvent123", "object": "event", "livemode": False,
                "type": "checkout.session.completed", "data": {"object": {
                    "id": "cs_test_MockedSession1", "object": "checkout.session",
                    "metadata": {"integration": payment.INTEGRATION}}}, **changes}

    def manual_order(self, **changes):
        order = {**self.order, "mode":"test", "payment_capture_method":"manual", **changes}
        created = self.gateway.create(order)
        suffix = str(len(self.sessions.saved))
        intent, charge = "pi_Manual"+suffix, "ch_Manual"+suffix
        session = self.sessions.saved[created["id"]]
        session.update(status="complete", payment_status="unpaid", payment_intent=intent, url=None)
        self.intents.saved[intent] = {"id":intent, "object":"payment_intent", "livemode":False,
            "currency":"eur", "amount":order["amount_total"], "capture_method":"manual",
            "metadata":deepcopy(session["metadata"]), "status":"requires_capture",
            "amount_capturable":order["amount_total"], "amount_received":0, "latest_charge":charge}
        self.charges.saved[charge] = {"id":charge, "object":"charge", "livemode":False,
            "payment_intent":intent, "currency":"eur", "amount":order["amount_total"],
            "paid":True, "captured":False, "amount_captured":0,
            "payment_method_details":{"type":"card", "card":{"capture_before":self.clock_value+1200}}}
        return {**order, "payment_id":created["id"], "payment_intent":intent}

    def test_manual_capture_is_server_selected_separate_from_legacy_idempotency_and_metadata(self):
        legacy = self.gateway.create(self.order)
        manual = self.gateway.create({**self.order, "payment_capture_method":"manual", "payment_id":""})
        auto_params, auto_options = self.sessions.creates[0]
        params, options = self.sessions.creates[1]
        self.assertNotEqual(legacy["id"], manual["id"])
        self.assertNotEqual(auto_options["idempotency_key"], options["idempotency_key"])
        self.assertNotIn("capture_method", auto_params["payment_intent_data"])
        self.assertEqual(params["payment_intent_data"]["capture_method"], "manual")
        self.assertEqual(params["metadata"], params["payment_intent_data"]["metadata"])
        self.assertEqual(params["metadata"]["payment_attempt"], "1")
        self.assertEqual(manual["status"], "open")
        self.assertTrue(self.gateway.supports_manual_capture)
        for method in (None, True, {}, "automatic_async", "MANUAL"):
            calls = len(self.sessions.creates)
            with self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.create({**self.order, "payment_capture_method":method})
            self.assertEqual(len(self.sessions.creates), calls)
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.create({**self.order, "payment_capture_method":"manual", "mode":"live"})
        self.assertEqual(len(self.sessions.creates), 2)

    def test_manual_authorization_reads_actual_intent_and_charge_but_never_claims_paid(self):
        order = self.manual_order()
        result = self.gateway.retrieve(order["payment_id"], expected_order=order)
        self.assertEqual(result["id"], order["payment_id"])
        self.assertEqual(result["payment_intent"], order["payment_intent"])
        self.assertEqual((result["status"], result["payment_status"]), ("authorized", "authorized"))
        self.assertEqual((result["amount_capturable"], result["amount_received"]), (62550, 0))
        self.assertEqual(result["capture_before"], self.clock_value+1200)
        self.assertEqual(len(self.charges.retrieves), 1)
        self.assertFalse(self.intents.captures or self.intents.cancels)
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.retrieve(order["payment_id"])
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.retrieve(order["payment_id"], expected_order=self.order)

    def test_manual_binding_rejects_wrong_amount_currency_mode_metadata_attempt_and_stored_ids(self):
        order = self.manual_order()
        intent = order["payment_intent"]
        original = deepcopy(self.intents.saved[intent])
        invalid = [{"amount":62551}, {"amount":True}, {"currency":"usd"}, {"livemode":True},
                   {"capture_method":"automatic"}, {"metadata":{}}, {"amount_capturable":62549},
                   {"amount_received":1}, {"object":"checkout.session"}, {"id":"pi_Foreign"}]
        for key in ("integration", "order_id", "quote_hash", "payment_attempt", "capture_method"):
            invalid.append({"metadata":{**original["metadata"], key:"PRIVATE-foreign"}})
        for change in invalid:
            self.intents.saved[intent] = {**deepcopy(original), **change}
            with self.subTest(change=change), self.assertRaises(payment.ReifenZahlungFehler) as caught:
                self.gateway.retrieve(order["payment_id"], expected_order=order)
            self.assertNotIn("PRIVATE", str(caught.exception))
        self.intents.saved[intent] = original
        for change in ({"mode":"live"}, {"payment_id":"cs_test_Foreign"}, {"payment_intent":"pi_Foreign"}):
            with self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.retrieve(order["payment_id"], expected_order={**order, **change})
        self.assertFalse(self.intents.captures or self.intents.cancels)

    def test_manual_missing_expired_or_wrong_charge_never_invents_an_authorization_deadline(self):
        order = self.manual_order()
        charge = self.intents.saved[order["payment_intent"]]["latest_charge"]
        original = deepcopy(self.charges.saved[charge])
        changes = [{"payment_method_details":{"type":"card", "card":{}}},
                   {"payment_method_details":{"type":"card", "card":{"capture_before":self.clock_value}}},
                   {"payment_intent":"pi_Foreign"}, {"captured":True}, {"amount_captured":1},
                   {"amount":1}, {"livemode":True}, {"currency":"usd"}]
        for change in changes:
            self.charges.saved[charge] = {**deepcopy(original), **change}
            with self.subTest(change=change), self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.retrieve(order["payment_id"], expected_order=order)
        self.charges.saved[charge] = original
        self.charges.error = PermissionError("PRIVATE Charges Read missing")
        with self.assertRaises(payment.ReifenZahlungFehler) as caught:
            self.gateway.retrieve(order["payment_id"], expected_order=order)
        self.assertNotIn("PRIVATE", str(caught.exception))
        self.assertFalse(self.intents.captures)

    def test_capture_uses_full_bound_amount_and_only_actual_received_funds_are_paid(self):
        order = self.manual_order()
        result = self.gateway.capture(order)
        self.assertEqual((result["status"], result["payment_status"]), ("succeeded", "paid"))
        self.assertEqual((result["amount_received"], result["amount_capturable"]), (62550, 0))
        self.assertEqual(result["id"], order["payment_intent"])
        intent, params, options = self.intents.captures[0]
        self.assertEqual(intent, order["payment_intent"])
        self.assertEqual(params, {"amount_to_capture":62550, "final_capture":True})
        self.assertNotIn(self.order["kunde"]["email"], options["idempotency_key"])
        self.assertEqual(self.gateway.capture(order), result)
        self.assertEqual(len(self.intents.captures), 1)
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.cancel_authorization(order)
        self.assertFalse(self.intents.cancels)

    def test_capture_timeout_recovery_uses_same_intent_and_never_creates_a_second_session(self):
        order = self.manual_order()
        self.intents.capture_response_lost = True
        with self.assertRaises(payment.ReifenZahlungFehler) as caught:
            self.gateway.capture(order)
        self.assertNotIn("PRIVATE", str(caught.exception))
        recovered = self.gateway.retrieve(order["payment_id"], expected_order=order)
        self.assertEqual(recovered["status"], "succeeded")
        self.assertEqual(self.gateway.capture(order)["status"], "succeeded")
        self.assertEqual(len(self.intents.captures), 1)
        self.assertEqual(len(self.sessions.saved), 1)

    def test_failed_capture_before_provider_effect_retries_with_identical_key_and_parameters(self):
        order = self.manual_order()
        self.intents.capture_error = TimeoutError("PRIVATE before capture")
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.capture(order)
        self.intents.capture_error = None
        self.assertEqual(self.gateway.capture(order)["status"], "succeeded")
        self.assertEqual(self.intents.captures[0], self.intents.captures[1])

    def test_cancel_is_only_for_actual_authorization_and_recovery_does_not_issue_refunds(self):
        order = self.manual_order()
        self.intents.cancel_response_lost = True
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.cancel_authorization(order)
        recovered = self.gateway.cancel_authorization(order)
        self.assertEqual((recovered["status"], recovered["payment_status"]), ("canceled", "canceled"))
        self.assertEqual((recovered["amount_received"], recovered["amount_capturable"]), (0, 0))
        self.assertEqual(len(self.intents.cancels), 1)
        self.assertFalse(self.intents.captures)
        self.assertEqual(self.intents.cancels[0][1], {"cancellation_reason":"abandoned"})
        for state in ("requires_action", "processing", "requires_payment_method"):
            value = self.intents.saved[order["payment_intent"]]
            value.update(status=state, amount_received=0, amount_capturable=0)
            with self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.cancel_authorization(order)
        self.assertEqual(len(self.intents.cancels), 1)

    def test_cancel_before_provider_effect_retries_with_the_same_frozen_key(self):
        order = self.manual_order()
        self.intents.cancel_error = TimeoutError("PRIVATE before cancel")
        with self.assertRaises(payment.ReifenZahlungFehler) as caught:
            self.gateway.cancel_authorization(order)
        self.assertNotIn("PRIVATE", str(caught.exception))
        self.intents.cancel_error = None
        self.assertEqual(self.gateway.cancel_authorization(order)["status"], "canceled")
        self.assertEqual(self.intents.cancels[0], self.intents.cancels[1])
        self.assertTrue(self.intents.cancels[0][2]["idempotency_key"].startswith("reifen-cancel-"))
        self.assertFalse(self.intents.captures)

    def test_manual_capture_or_cancel_requires_a_stored_manual_intent_before_sdk_io(self):
        for value in (self.order, {**self.order,"payment_capture_method":"manual"},
                      {**self.order,"payment_capture_method":"manual","payment_intent":"pi_Other","mode":"live"}):
            for method in (self.gateway.capture, self.gateway.cancel_authorization):
                with self.assertRaises(payment.ReifenZahlungFehler):
                    method(value)
        self.assertFalse(self.intents.retrieves or self.intents.captures or self.intents.cancels)

    def test_manual_success_requires_exact_received_amount_not_a_paid_charge_or_partial_result(self):
        order = self.manual_order()
        raw = self.intents.saved[order["payment_intent"]]
        for received, capturable in ((0,0), (62549,0), (62550,1)):
            raw.update(status="succeeded", amount_received=received, amount_capturable=capturable)
            with self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.retrieve(order["payment_id"], expected_order=order)
        self.assertFalse(self.intents.captures)

    def test_create_charges_exact_server_total_once_with_no_implicit_extras(self):
        result = self.gateway.create(self.order)
        params, options = self.sessions.creates[0]
        self.assertEqual(params["mode"], "payment")
        self.assertEqual(params["ui_mode"], "hosted_page")
        self.assertEqual(params["payment_method_types"], ["card"])
        self.assertEqual(params["line_items"][0]["quantity"], 1)
        self.assertEqual(params["line_items"][0]["price_data"]["unit_amount"], 62550)
        self.assertEqual(params["line_items"][0]["price_data"]["product_data"]["name"], "Reifenbestellung reifenOrder123")
        self.assertEqual(result["amount_total"], 62550)
        self.assertEqual(result["reference"], self.order["id"])
        self.assertEqual(result["quote_hash"], self.order["quote_hash"])
        self.assertEqual(params["metadata"], params["payment_intent_data"]["metadata"])
        self.assertEqual(params["metadata"]["integration"], payment.INTEGRATION)
        self.assertNotIn("subscription_data", params)
        self.assertNotIn("automatic_tax", params)
        self.assertNotIn("discounts", params)
        self.assertNotIn("shipping_options", params)
        self.assertNotIn("customer_email", params)
        self.assertNotIn("telefon", json.dumps(params))
        self.assertNotIn(self.order["kunde"]["email"], options["idempotency_key"])

    def test_rim_order_name_is_exact_while_amount_metadata_attempt_and_paid_binding_are_preserved(self):
        order = {**self.order, "id": "felgenOrder123", "produktart": "felgen",
                 "produkt": {"produktart": "felgen"}, "query": {"produktart": "felgen", "anzahl": 4}}
        result = self.gateway.create(order)
        params, options = self.sessions.creates[0]
        self.assertEqual(params["line_items"][0]["price_data"]["product_data"]["name"], "Felgenbestellung felgenOrder123")
        self.assertEqual(params["line_items"][0]["price_data"]["unit_amount"], order["amount_total"])
        self.assertEqual(params["metadata"], {"integration": payment.INTEGRATION,
            "order_id": order["id"], "quote_hash": order["quote_hash"]})
        self.assertEqual(params["payment_intent_data"], {"metadata": params["metadata"]})
        self.assertEqual(params["expires_at"], order["payment_expires_at"])
        self.gateway.create(order)
        self.assertEqual(self.sessions.creates[-1], (params, options))
        self.assertEqual(len(self.sessions.saved), 1)
        self.sessions.saved[result["id"]].update(status="complete", payment_status="paid", payment_intent="pi_RimMocked123")
        self.assertEqual(self.gateway.retrieve(result["id"], expected_order=order)["status"], "succeeded")
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.retrieve(result["id"], expected_order={**order, "quote_hash": "c"*64})

    def test_unknown_nontext_and_contradictory_product_types_are_rejected_before_checkout_io(self):
        changes = [{"produktart": value} for value in (None, True, 1, [], {}, "Felgen", "komplettraeder", "PRIVATE")]
        changes.extend(({"produktart": "felgen", "produkt": {"produktart": "reifen"}},
                        {"produktart": "reifen", "query": {"produktart": "felgen"}},
                        {"produkt": {"produktart": "felgen"}},
                        {"produktart": "felgen", "query": {"produktart": None}}))
        for change in changes:
            with self.subTest(change=change), self.assertRaises(payment.ReifenZahlungFehler) as caught:
                self.gateway.create({**self.order, **change})
            self.assertNotIn("PRIVATE", str(caught.exception))
        self.assertFalse(self.sessions.creates)

    def test_retry_reuses_frozen_expiry_parameters_and_idempotency_key(self):
        one = self.gateway.create(self.order)
        self.clock_value += 30
        two = self.gateway.create(self.order)
        self.assertEqual(one, two)
        self.assertEqual(self.sessions.creates[0], self.sessions.creates[1])
        self.assertEqual(len(self.sessions.saved), 1)
        changed = {**self.order, "payment_attempt": "2"}
        self.gateway.create(changed)
        self.assertNotEqual(self.sessions.creates[0][1], self.sessions.creates[2][1])

    def test_changed_params_on_same_attempt_never_silently_make_second_session(self):
        self.gateway.create(self.order)
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.create({**self.order, "payment_expires_at": self.order["payment_expires_at"] + 60})
        self.assertEqual(len(self.sessions.saved), 1)

    def test_idempotent_retry_remains_possible_below_thirty_minutes_remaining(self):
        for lifetime in (1800, 3600):
            with self.subTest(lifetime=lifetime):
                order = {**self.order, "payment_expires_at": self.clock_value + lifetime,
                         "payment_created_at": self.clock_value, "payment_attempt": str(lifetime)}
                initial = self.gateway.create(order)
                self.clock_value += lifetime - 1799
                retried = self.gateway.create(order)
                self.assertEqual(initial["id"], retried["id"])
                self.assertEqual(self.sessions.creates[-1], self.sessions.creates[-2])

    def test_lost_response_can_be_reconciled_but_no_blind_create_after_twenty_four_hours(self):
        original_create = self.sessions.create
        first = True
        def response_lost(params, *, options):
            nonlocal first
            result = original_create(params, options=options)
            if first:
                first = False
                raise TimeoutError("Synthetic response lost after provider accepted the request")
            return result
        self.sessions.create = response_lost
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.create(self.order)
        self.clock_value += 2000
        recovered = self.gateway.create(self.order)
        self.assertEqual(recovered["id"], "cs_test_MockedSession1")
        self.assertEqual(len(self.sessions.saved), 1)
        self.clock_value = self.order["payment_created_at"] + 86400
        calls = len(self.sessions.creates)
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.create(self.order)
        self.assertEqual(len(self.sessions.creates), calls)

    def test_invalid_order_and_expiry_fail_before_any_sdk_request(self):
        cases = ({"amount_total": 0}, {"amount_total": -1}, {"amount_total": 1.5}, {"amount_total": True},
                 {"amount_total": 10_000_001}, {"currency": "EUR"}, {"currency": "usd"},
                 {"quote_hash": "client-price"}, {"id": "../evil"},
                 {"payment_expires_at": None}, {"payment_expires_at": self.clock_value + 1799},
                 {"payment_expires_at": self.clock_value + 86401}, {"payment_attempt": "a\nb"},
                 {"payment_created_at": None}, {"payment_created_at": self.clock_value + 1})
        for change in cases:
            with self.subTest(change=change), self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.create({**self.order, **change})
        self.assertFalse(self.sessions.creates)

    def test_redirects_are_fixed_same_origin_and_ignore_client_urls(self):
        self.gateway.create({**self.order, "success_url": "https://evil.invalid", "cancel_url": "javascript:alert(1)"})
        params = self.sessions.creates[0][0]
        self.assertEqual(params["success_url"], "https://reifen.example.invalid/reifen/zahlung/erfolg/reifenOrder123?session_id={CHECKOUT_SESSION_ID}")
        self.assertEqual(params["cancel_url"], "https://reifen.example.invalid/reifen/zahlung/abgebrochen/reifenOrder123")

    def test_origin_and_path_restrictions(self):
        for origin in ("http://reifen.example.invalid", "https://evil.invalid", "https://reifen.example.invalid@evil.invalid",
                       "https://reifen.example.invalid/extra", "https://reifen.example.invalid?next=evil", "https://reifen.example.invalid:444",
                       "https://reifen.example.invalid:notaport",
                       "https://reifen.example.invalid\\@evil.invalid", "https://reifen.example.invalid\n"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                payment.StripeReifenGateway({**CONFIG, "REIFEN_STRIPE_PUBLIC_ORIGIN": origin}, client=self.client)
        for path in ("https://evil.invalid", "//evil.invalid", "/x?url=bad", "/../redirect", "/{other}", "/%2f%2fevil"):
            with self.subTest(path=path), self.assertRaises(payment.ReifenZahlungFehler):
                payment.StripeReifenGateway({**CONFIG, "REIFEN_STRIPE_SUCCESS_PATH": path}, client=self.client)

    def test_separate_keys_and_modes_required_no_rental_or_global_fallback(self):
        for mode, key in (("live", CONFIG["REIFEN_STRIPE_SECRET_KEY"]), ("test", "sk_live_synthetic_unusable_key"),
                          (None, "sk_test_synthetic_unusable_key")):
            with self.subTest(mode=mode), self.assertRaises(payment.ReifenZahlungFehler):
                payment.StripeReifenGateway({**CONFIG, "REIFEN_STRIPE_MODE": mode, "REIFEN_STRIPE_SECRET_KEY": key}, client=self.client)
        config = {key: value for key, value in CONFIG.items() if key != "REIFEN_STRIPE_SECRET_KEY"}
        config.update(MOS_PUBLIC_STRIPE_LIVE_KEY="sk_live_do_not_use_this", STRIPE_SECRET_KEY="sk_test_do_not_use_this")
        with self.assertRaises(payment.ReifenZahlungFehler):
            payment.StripeReifenGateway(config, client=self.client)

    def test_sdk_constructed_with_explicit_timeouts_and_no_automatic_retries(self):
        with patch.object(payment.stripe, "RequestsClient") as http, patch.object(payment.stripe, "StripeClient") as client:
            payment.StripeReifenGateway(CONFIG)
        http.assert_called_once_with(timeout=(5.0, 15.0))
        self.assertEqual(client.call_args.args, (CONFIG["REIFEN_STRIPE_SECRET_KEY"],))
        self.assertEqual(client.call_args.kwargs["max_network_retries"], 0)
        self.assertIs(client.call_args.kwargs["http_client"], http.return_value)

    def test_retrieve_checks_remote_paid_state_and_local_order_binding(self):
        created = self.gateway.create(self.order)
        self.sessions.saved[created["id"]].update(status="complete", payment_status="paid", payment_intent="pi_MockedIntent123", url=None)
        result = self.gateway.retrieve(created["id"], expected_order=self.order)
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["payment_status"], "paid")
        self.assertFalse(result["url"])
        for change in ({"amount_total": 1}, {"id": "anotherOrder"}, {"quote_hash": "c" * 64}):
            with self.subTest(change=change), self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.retrieve(created["id"], expected_order={**self.order, **change})

    def test_unpaid_complete_is_processing_not_paid_and_expired_not_success(self):
        created = self.gateway.create(self.order)
        self.sessions.saved[created["id"]].update(status="complete")
        self.assertEqual(self.gateway.retrieve(created["id"])["status"], "processing")
        self.sessions.saved[created["id"]].update(status="expired")
        self.assertEqual(self.gateway.retrieve(created["id"])["status"], "expired")

    def test_mismatched_session_objects_and_checkout_domains_rejected(self):
        changes = ({"livemode": True}, {"amount_total": True}, {"object": "payment_intent"}, {"mode": "subscription"},
                   {"currency": "usd"}, {"client_reference_id": "foreignOrder"}, {"metadata": {}},
                   {"status": "expired", "payment_status": "paid", "payment_intent": "pi_Test"},
                   {"status": "complete", "payment_status": "paid", "payment_intent": None},
                   {"url": "https://checkout.stripe.com.evil.invalid/pay"}, {"url": "https://evil.invalid"},
                   {"url": "https://checkout.stripe.com:notaport/pay"})
        for change in changes:
            with self.subTest(change=change):
                sessions = FakeSessions(self.clock_value)
                sessions.response_changes = change
                client = SimpleNamespace(v1=SimpleNamespace(checkout=SimpleNamespace(sessions=sessions)))
                gateway = payment.StripeReifenGateway(CONFIG, client=client, clock=lambda: self.clock_value)
                with self.assertRaises(payment.ReifenZahlungFehler):
                    gateway.create(self.order)
        with self.assertRaises(payment.ReifenZahlungFehler):
            self.gateway.retrieve("cs_live_ForeignMode")
        self.assertFalse(self.sessions.retrieves)

    def test_provider_errors_are_redacted(self):
        self.sessions.error = RuntimeError("sk_live_secret should never be displayed")
        with self.assertRaises(payment.ReifenZahlungFehler) as raised:
            self.gateway.create(self.order)
        self.assertNotIn("sk_live_secret", str(raised.exception))
        self.assertTrue(raised.exception.__suppress_context__)

    def test_real_sdk_verifies_locally_signed_fake_webhook_without_payment_claim(self):
        body, signature = self.signed(self.event())
        result = self.gateway.verify_webhook(body, signature)
        self.assertEqual(result, {"event_id": "evt_MockedEvent123", "type": "checkout.session.completed",
            "livemode": False, "ignored": False, "session_id": "cs_test_MockedSession1"})
        self.assertFalse(self.sessions.retrieves or self.sessions.creates)
        self.assertNotIn("paid", result)

    def test_webhook_rejects_tampering_unsigned_old_future_and_wrong_secret(self):
        body, signature = self.signed(self.event())
        cases = [(body+b" ", signature), (body, ""), self.signed(self.event(), timestamp=self.clock_value-301),
                 self.signed(self.event(), timestamp=self.clock_value+301), self.signed(self.event(), secret="whsec_other_secret"),
                 (body, signature+",t="+str(self.clock_value))]
        for data, header in cases:
            with self.subTest(header=header), self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.verify_webhook(data, header)

    def test_webhook_mode_and_connected_account_are_rejected(self):
        for changes in ({"livemode": True}, {"account": "acct_Foreign"}, {"context": "org_foreign"}):
            with self.subTest(changes=changes), self.assertRaises(payment.ReifenZahlungFehler):
                self.gateway.verify_webhook(*self.signed(self.event(**changes)))

    def test_signed_unrelated_events_are_ignored_and_duplicate_ids_stay_stable(self):
        unrelated = self.event(type="charge.succeeded")
        self.assertTrue(self.gateway.verify_webhook(*self.signed(unrelated))["ignored"])
        other_checkout = self.event(data={"object": {"id": "cs_test_Other", "object": "checkout.session", "metadata": {"integration": "mietwagen"}}})
        self.assertTrue(self.gateway.verify_webhook(*self.signed(other_checkout))["ignored"])
        signed = self.signed(self.event())
        self.assertEqual(self.gateway.verify_webhook(*signed), self.gateway.verify_webhook(*signed))

    def test_live_mode_uses_live_session_ids_without_any_live_request(self):
        config = {**CONFIG, "REIFEN_STRIPE_MODE": "live", "REIFEN_STRIPE_SECRET_KEY": "sk_live_synthetic_unusable_key"}
        gateway = payment.StripeReifenGateway(config, client=self.client)
        with self.assertRaises(payment.ReifenZahlungFehler):
            gateway.retrieve("cs_test_MockedSession1")
        self.assertFalse(self.sessions.creates or self.sessions.retrieves)
        event = self.event(livemode=True, data={"object": {"id": "cs_live_Mocked123", "object": "checkout.session",
                                                         "metadata": {"integration": payment.INTEGRATION}}})
        self.assertTrue(gateway.verify_webhook(*self.signed(event))["livemode"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
