"""Offline Twilio Verify regression tests. Never import the portal or send SMS."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reifen_sms import TwilioVerifySMS, ReifenSMSUnavailable, init_schema


ACCOUNT = "AC" + "a" * 32
SERVICE = "VA" + "b" * 32
VERIFICATION = "VE" + "c" * 32
TOKEN = "synthetic-not-a-real-auth-token"
PHONE = "+4915100000011"
REFERENCE = "synthetic-order-000001"


def payload(status="pending", **changes):
    return dict(sid=VERIFICATION, service_sid=SERVICE, account_sid=ACCOUNT,
                to=PHONE, channel="sms", status=status, **changes)


class Response:
    def __init__(self, data=None, status=200, raw=None):
        self.status_code = status
        self.body = raw if raw is not None else json.dumps(data).encode()
        self.closed = False

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset:offset + chunk_size]

    def close(self):
        self.closed = True


class HTTP:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.lock = threading.Lock()

    def post(self, url, **kwargs):
        with self.lock:
            self.calls.append((url, kwargs))
            response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if callable(response):
            response = response()
        return response


class TwilioSMS(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="reifen-sms-offline-")
        self.path = Path(self.temp.name) / "sms.sqlite3"
        self.now = 1000.0
        self.network = patch("socket.socket.connect", side_effect=AssertionError("No real network"))
        self.network.start()
        self.http = HTTP()
        with closing(self.db()) as db:
            init_schema(db)
            db.commit()
        self.sms = self.adapter()

    def tearDown(self):
        self.network.stop()
        self.temp.cleanup()

    def db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def adapter(self, authorize=lambda operation, reference: True):
        return TwilioVerifySMS(ACCOUNT, TOKEN, SERVICE, self.db, http=self.http,
                               authorize=authorize, clock=lambda: self.now)

    def start(self):
        self.http.responses.append(Response(payload(), status=201))
        return self.sms.start(PHONE, REFERENCE)

    def row(self):
        with closing(self.db()) as db:
            return dict(db.execute("SELECT * FROM reifen_sms_challenges ORDER BY created_at DESC LIMIT 1").fetchone())

    def test_configuration_never_sends_or_initializes_database(self):
        adapter = TwilioVerifySMS.from_env(self.db, environment={
            "REIFEN_TWILIO_ACCOUNT_SID": ACCOUNT, "REIFEN_TWILIO_AUTH_TOKEN": TOKEN,
            "REIFEN_TWILIO_VERIFY_SERVICE_SID": SERVICE,
        }, http=self.http)
        with self.assertRaises(ReifenSMSUnavailable):
            adapter.start(PHONE, REFERENCE)
        self.assertEqual(self.http.calls, [])

    def test_missing_configuration_is_generic(self):
        with self.assertRaises(ReifenSMSUnavailable) as result:
            TwilioVerifySMS.from_env(self.db, environment={})
        self.assertNotIn(TOKEN, str(result.exception))

    def test_strict_e164_rejects_ambiguous_or_injected_input_before_io(self):
        for phone in ("015100000011", "004915100000011", "+49 15100000011", "+04915100000011",
                      "+4915100000011\n", "+4915100000011000000", None, 12345678):
            with self.subTest(phone=repr(phone)), self.assertRaises(ReifenSMSUnavailable):
                self.sms.start(phone, REFERENCE)
        self.assertEqual(self.http.calls, [])

    def test_authorization_requires_exact_true_on_every_operation(self):
        for value in (None, False, 1, "yes"):
            with self.subTest(value=value), self.assertRaises(ReifenSMSUnavailable):
                self.adapter(lambda operation, reference: value).start(PHONE, REFERENCE)
        self.assertEqual(self.http.calls, [])

    def test_start_is_durable_and_idempotent_after_adapter_restart(self):
        first = self.start()
        second = self.adapter().start(PHONE, REFERENCE)
        self.assertEqual(first["verification_id"], second["verification_id"])
        self.assertTrue(second["reused"])
        self.assertEqual(len(self.http.calls), 1)

    def test_transport_is_official_https_bounded_and_does_not_redirect(self):
        self.start()
        url, options = self.http.calls[0]
        self.assertEqual(url, "https://verify.twilio.com/v2/Services/" + SERVICE + "/Verifications")
        self.assertFalse(options["allow_redirects"])
        self.assertEqual(options["timeout"], (3.05, 8))
        self.assertTrue(options["stream"])
        self.assertEqual(options["data"], {"To": PHONE, "Channel": "sms", "Locale": "de"})

    def test_no_plain_phone_reference_or_otp_is_persisted(self):
        self.start()
        self.http.responses.append(Response(payload("pending")))
        self.assertFalse(self.sms.check(PHONE, "874961", REFERENCE))
        stored = json.dumps(self.row())
        for secret in (PHONE, REFERENCE, TOKEN, "874961"):
            self.assertNotIn(secret, stored)

    def test_phone_cannot_be_changed_under_same_reference(self):
        self.start()
        with self.assertRaises(ReifenSMSUnavailable):
            self.sms.start("+4915100000022", REFERENCE)
        self.assertFalse(self.sms.check("+4915100000022", "123456", REFERENCE))
        self.assertEqual(len(self.http.calls), 1)

    def test_same_phone_cannot_verify_two_active_orders(self):
        self.start()
        with self.assertRaises(ReifenSMSUnavailable):
            self.sms.start(PHONE, "different-order-000002")
        self.assertFalse(self.sms.check(PHONE, "123456", "different-order-000002"))
        self.assertEqual(len(self.http.calls), 1)

    def test_unknown_network_outcome_is_not_automatically_resent(self):
        self.http.responses.append(TimeoutError("Secret " + PHONE + " " + TOKEN))
        with self.assertRaises(ReifenSMSUnavailable) as result:
            self.sms.start(PHONE, REFERENCE)
        self.assertNotIn(PHONE, str(result.exception))
        self.assertNotIn(TOKEN, str(result.exception))
        with self.assertRaises(ReifenSMSUnavailable):
            self.sms.start(PHONE, REFERENCE)
        self.assertEqual(len(self.http.calls), 1)
        self.assertEqual(self.row()["state"], "uncertain")

    def test_approved_checks_use_bound_sid_not_arbitrary_number(self):
        self.start()
        self.http.responses.append(Response(payload("approved")))
        self.assertTrue(self.sms.check(PHONE, "123456", REFERENCE))
        self.assertEqual(self.http.calls[-1][1]["data"], {"VerificationSid": VERIFICATION, "Code": "123456"})
        self.assertEqual(self.row()["state"], "approved")
        self.assertTrue(self.sms.check(PHONE, "123456", REFERENCE))
        self.assertEqual(len(self.http.calls), 2)

    def test_legacy_valid_true_does_not_confirm_pending(self):
        self.start()
        self.http.responses.append(Response(payload("pending", valid=True)))
        self.assertFalse(self.sms.check(PHONE, "123456", REFERENCE))

    def test_wrong_response_identity_cannot_confirm(self):
        for key, value in (("sid", "VE" + "d" * 32), ("to", "+4915100000022"),
                           ("account_sid", "AC" + "d" * 32), ("service_sid", "VA" + "d" * 32),
                           ("channel", "call")):
            with self.subTest(key=key):
                with closing(self.db()) as db:
                    db.execute("DELETE FROM reifen_sms_challenges")
                    db.commit()
                self.start()
                data = payload("approved")
                data[key] = value
                self.http.responses.append(Response(data))
                self.assertFalse(self.sms.check(PHONE, "123456", REFERENCE))
                self.assertEqual(self.row()["state"], "failed")

    def test_404_deleted_challenge_never_means_approved(self):
        self.start()
        self.http.responses.append(Response({"status": "approved"}, status=404))
        self.assertFalse(self.sms.check(PHONE, "123456", REFERENCE))

    def test_check_attempt_limit_is_enforced_before_provider(self):
        self.start()
        for _ in range(5):
            self.http.responses.append(Response(payload("pending")))
            self.assertFalse(self.sms.check(PHONE, "123456", REFERENCE))
        self.assertFalse(self.sms.check(PHONE, "123456", REFERENCE))
        self.assertEqual(len(self.http.calls), 6)
        self.assertEqual(self.row()["checks"], 5)

    def test_expiry_is_enforced_locally_even_if_provider_would_approve(self):
        self.start()
        self.now += 601
        self.assertFalse(self.sms.check(PHONE, "123456", REFERENCE))
        self.assertEqual(len(self.http.calls), 1)

    def test_approval_returning_after_deadline_is_not_committed(self):
        self.start()
        def late():
            self.now += 601
            return Response(payload("approved"))
        self.http.responses.append(late)
        self.assertFalse(self.sms.check(PHONE, "123456", REFERENCE))
        self.assertNotEqual(self.row()["state"], "approved")

    def test_oversized_malformed_or_redirected_start_stays_unconfirmed(self):
        for response in (Response(raw=b"x" * 65537), Response(raw=b"not json"),
                         Response(payload(), status=302), Response([])):
            with self.subTest(response=response):
                with closing(self.db()) as db:
                    db.execute("DELETE FROM reifen_sms_challenges")
                    db.commit()
                self.http.responses.append(response)
                with self.assertRaises(ReifenSMSUnavailable):
                    self.sms.start(PHONE, REFERENCE)
                self.assertTrue(response.closed)

    def test_parallel_starts_submit_at_most_one_sms(self):
        entered, release = threading.Event(), threading.Event()
        def blocked():
            entered.set()
            self.assertTrue(release.wait(5))
            return Response(payload())
        self.http.responses.append(blocked)
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(self.sms.start, PHONE, REFERENCE)
            self.assertTrue(entered.wait(5))
            with self.assertRaises(ReifenSMSUnavailable):
                self.adapter().start(PHONE, REFERENCE)
            release.set()
            self.assertEqual(first.result(timeout=5)["status"], "pending")
        self.assertEqual(len(self.http.calls), 1)

    def test_provider_sid_cannot_be_rebound_after_expiry(self):
        self.start()
        self.now += 601
        self.http.responses.append(Response(payload()))
        with self.assertRaises(ReifenSMSUnavailable):
            self.sms.start(PHONE, "different-order-000002")
        self.assertFalse(self.sms.check(PHONE, "123456", "different-order-000002"))


if __name__ == "__main__":
    unittest.main()
