"""Offline protocol tests for the documented, read-only TYREsystem Inquiry."""
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tyresystem_api import (TyreSystemInquiryClient, TyreSystemAPIError,
                           MAX_RESPONSE_BYTES, TIMEOUT)


CONFIG = {"TYRESYSTEM_API_USERNAME": "offline-user",
          "TYRESYSTEM_API_PASSWORD": "offline-only-not-a-real-password",
          "TYRESYSTEM_API_MODE": "test"}
NOW = datetime(2026, 11, 2, 9, 0, tzinfo=timezone.utc)


def payload(**changes):
    offer = {"idArticle": "222", "ean": "4000000000000", "price": "44.2600",
             "amount": "4", "description": "205/55 R16 DEMO-Reifen", "errorCode": 0, "stock": "20"}
    offer.update(changes)
    return {"inquiryResponse": {"offerData": offer}}


class Response:
    def __init__(self, data=None, *, raw=None, status=200, content_type="application/json", headers=None, url=None):
        self.status_code = status
        self.headers = {"Content-Type": content_type}
        self.headers.update(headers or {})
        self.raw = raw if raw is not None else json.dumps(data if data is not None else payload()).encode()
        self.closed = False
        self.url = url

    def iter_content(self, chunk_size):
        for index in range(0, len(self.raw), chunk_size):
            yield self.raw[index:index + chunk_size]

    def close(self):
        self.closed = True


class HTTP:
    def __init__(self, response=None):
        self.response = response if response is not None else Response()
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        if self.response.url is None:
            self.response.url = url
        return self.response


class InquiryClientTests(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.socket.connect", side_effect=AssertionError("No external API calls"))
        self.network.start()

    def tearDown(self):
        self.network.stop()

    def client(self, response=None, *, config=None, authorize=lambda article, amount: True, **options):
        self.http = HTTP(response)
        return TyreSystemInquiryClient(CONFIG if config is None else config,
            authorize=authorize, http=self.http, clock=lambda: NOW, **options)

    def assert_rejected(self, response, code="invalid_response"):
        client = self.client(response)
        with self.assertRaises(TyreSystemAPIError) as failure:
            client.inquire("222", 4)
        self.assertEqual(failure.exception.code, code)
        if isinstance(response, Response):
            self.assertTrue(response.closed)
        return failure.exception

    def test_documented_get_shape_with_basic_auth_and_no_redirect_or_retry(self):
        result = self.client().inquire("222", 4)
        url, options = self.http.calls[0]
        self.assertEqual(url, "https://api.tyresystem.de/Rest/Inquiry/222/4")
        self.assertEqual(options["auth"], (CONFIG["TYRESYSTEM_API_USERNAME"], CONFIG["TYRESYSTEM_API_PASSWORD"]))
        self.assertEqual(options["timeout"], TIMEOUT)
        self.assertFalse(options["allow_redirects"])
        self.assertTrue(options["verify"])
        self.assertTrue(options["stream"])
        self.assertEqual(len(self.http.calls), 1)
        self.assertEqual((result.article_id, result.amount, result.stock), ("222", 4, 20))
        self.assertEqual(result.price, Decimal("44.2600"))
        self.assertEqual(result.retrieved_at, NOW)
        self.assertEqual(result.configured_mode, "test")

    def test_constructing_configuring_or_missing_runtime_permission_never_calls_api(self):
        for authorize in (None, lambda *_: False, lambda *_: 1, lambda *_: "yes"):
            client = self.client(authorize=authorize)
            with self.assertRaises(TyreSystemAPIError) as failure:
                client.inquire("222", 4)
            self.assertEqual(failure.exception.code, "not_authorized")
            self.assertEqual(self.http.calls, [])

    def test_own_api_credentials_and_mode_required_no_shop_or_other_service_fallback(self):
        for config in ({}, {"USERNAME": "shop", "PASSWORD": "shop", "MODE": "live"},
                       {**CONFIG, "TYRESYSTEM_API_MODE": "off"},
                       {**CONFIG, "TYRESYSTEM_API_USERNAME": "name:injection"},
                       {**CONFIG, "TYRESYSTEM_API_PASSWORD": "bad\npassword"}):
            with self.subTest(config_keys=sorted(config)), self.assertRaises(TyreSystemAPIError):
                TyreSystemInquiryClient.from_env(environment=config)

    def test_endpoint_is_fixed_even_when_untrusted_url_is_supplied_in_config(self):
        client = self.client(config={**CONFIG, "TYRESYSTEM_API_BASE_URL": "http://evil.invalid/Order"})
        client.inquire(222, "4")
        self.assertEqual(self.http.calls[0][0], "https://api.tyresystem.de/Rest/Inquiry/222/4")

    def test_default_transport_disables_environment_proxies_and_retries(self):
        transport = Mock()
        transport.get.return_value = Response(url="https://api.tyresystem.de/Rest/Inquiry/222/4")
        with patch("tyresystem_api.requests.Session", return_value=transport):
            client = TyreSystemInquiryClient(CONFIG, authorize=lambda *_: True, clock=lambda: NOW)
            client.inquire("222", 4)
        self.assertIs(transport.trust_env, False)
        prefix, adapter = transport.mount.call_args.args
        self.assertEqual(prefix, "https://")
        self.assertEqual(adapter.max_retries.total, 0)
        self.assertEqual(transport.get.call_count, 1)

    def test_invalid_article_or_amount_is_rejected_before_io(self):
        for article, amount in ((True, 4), ("../Order", 4), ("222?password=x", 4),
                                ("0", 4), ("222", True), ("222", -1), ("222", 0),
                                ("222", 1001), ("222", "1.5"), (222.0, 4)):
            client = self.client()
            with self.subTest(article=article, amount=amount), self.assertRaises(TyreSystemAPIError):
                client.inquire(article, amount)
            self.assertEqual(self.http.calls, [])

    def test_known_id_and_request_quantity_must_match_response(self):
        self.assert_rejected(Response(payload(idArticle="223")))
        self.assert_rejected(Response(payload(amount="1")))

    def test_stock_shortage_is_reported_without_inventing_a_reservation(self):
        result = self.client(Response(payload(stock="0"))).inquire("222", 4)
        self.assertEqual(result.stock, 0)
        self.assertEqual(result.amount, 4)

    def test_decimal_numeric_json_preserves_exact_price_without_float(self):
        raw = json.dumps(payload()).replace('"44.2600"', "44.2600").encode()
        result = self.client(Response(raw=raw)).inquire("222", 4)
        self.assertIsInstance(result.price, Decimal)
        self.assertEqual(result.price.as_tuple().exponent, -4)

    def test_nonfinite_negative_zero_ambiguous_and_excessive_price_rejected(self):
        for price in ("NaN", "Infinity", "-1.00", "0.00", "1,20", "1e3", "1.23456", "1000001.00", True):
            with self.subTest(price=price):
                self.assert_rejected(Response(payload(price=price)))
        self.assert_rejected(Response(raw=json.dumps(payload()).replace('"44.2600"', "NaN").encode()))

    def test_nonzero_or_type_confused_error_code_never_becomes_a_price(self):
        for error in (1, -1, True, False, "1", None, 0.0):
            with self.subTest(error=error):
                self.assert_rejected(Response(payload(errorCode=error)), "provider_error")

    def test_missing_fields_wrong_shape_duplicate_keys_and_malformed_json_rejected(self):
        for response in (Response({"offerData": {}}), Response([]), Response(raw=b"broken"),
                         Response(raw=b'{"inquiryResponse":{},"inquiryResponse":{"offerData":{}}}')):
            self.assert_rejected(response)
        for field in payload()["inquiryResponse"]["offerData"]:
            data = payload()
            del data["inquiryResponse"]["offerData"][field]
            self.assert_rejected(Response(data))

    def test_invalid_ean_description_and_stock_rejected(self):
        for changes in ({"ean": 4000000000000}, {"ean": "bad"}, {"stock": True},
                        {"stock": "-1"}, {"stock": "20+"}, {"description": ""},
                        {"description": "bad\u200bdescription"}, {"description": "x" * 501}):
            self.assert_rejected(Response(payload(**changes)))

    def test_xml_example_shape_is_supported_without_external_entity_resolution(self):
        xml = b'''<?xml version="1.0" encoding="UTF-8"?><inquiryResponse><offerData>
            <idArticle>222</idArticle><ean>4000000000000</ean><price>44.26</price>
            <amount>4</amount><description>DEMO &amp; Test</description><errorCode>0</errorCode><stock>20</stock>
            </offerData></inquiryResponse>'''
        result = self.client(Response(raw=xml, content_type="text/xml; charset=utf-8")).inquire("222", 4)
        self.assertEqual(result.description, "DEMO & Test")
        self.assertEqual(result.price, Decimal("44.26"))
        self.assert_rejected(Response(raw=b'<!DOCTYPE x [<!ENTITY e SYSTEM "file:///private">]>' + xml,
                                      content_type="application/xml"))
        self.assert_rejected(Response(raw=xml.replace(b"<stock>20</stock>", b"<stock>20</stock><stock>999</stock>"),
                                      content_type="application/xml"))

    def test_xml_namespace_nested_values_and_encoding_confusion_rejected(self):
        for xml in (b'<inquiryResponse xmlns="unexpected"><offerData/></inquiryResponse>',
                    b'<inquiryResponse><offerData><stock><x>20</x></stock></offerData></inquiryResponse>',
                    b'<?xml version="1.0" encoding="UTF-16"?><inquiryResponse/>'):
            self.assert_rejected(Response(raw=xml, content_type="application/xml"))

    def test_http_auth_rate_limit_failure_and_redirect_are_safe_and_never_retried(self):
        for status, code in ((401, "authentication"), (403, "authentication"),
                             (429, "rate_limited"), (500, "unavailable"), (302, "unavailable"), (204, "unavailable")):
            with self.subTest(status=status):
                error = self.assert_rejected(Response(raw=CONFIG["TYRESYSTEM_API_PASSWORD"].encode(), status=status), code)
                self.assertNotIn(CONFIG["TYRESYSTEM_API_PASSWORD"], str(error))
                self.assertEqual(len(self.http.calls), 1)

    def test_timeout_and_unknown_transport_error_never_leak_credentials_or_raw_exception(self):
        for error in (TimeoutError(CONFIG["TYRESYSTEM_API_PASSWORD"]),
                      OSError("Authorization: Basic secret, offline-user")):
            result = self.assert_rejected(error, "unavailable")
            self.assertNotIn("offline-user", str(result))
            self.assertNotIn(CONFIG["TYRESYSTEM_API_PASSWORD"], str(result))

    def test_declared_and_streamed_oversize_wrong_content_type_and_final_host_rejected(self):
        for response in (Response(headers={"Content-Length": str(MAX_RESPONSE_BYTES + 1)}),
                         Response(raw=b" " * (MAX_RESPONSE_BYTES + 1)),
                         Response(content_type="text/html"), Response(url="https://evil.invalid/")):
            self.assert_rejected(response)

    def test_soft_total_budget_stops_more_chunks_or_parsing(self):
        ticks = iter((0.0, 16.0))
        client = self.client(monotonic=lambda: next(ticks))
        with self.assertRaises(TyreSystemAPIError) as error:
            client.inquire("222", 4)
        self.assertEqual(error.exception.code, "unavailable")
        self.assertTrue(self.http.response.closed)

    def test_late_stream_eof_cannot_return_a_result_after_budget(self):
        elapsed = {"seconds": 0.0}
        class LateEOF(Response):
            def iter_content(self, chunk_size):
                elapsed["seconds"] = 14.0
                yield self.raw
                elapsed["seconds"] = 16.0
        client = self.client(LateEOF(), monotonic=lambda: elapsed["seconds"])
        with self.assertRaises(TyreSystemAPIError) as error:
            client.inquire("222", 4)
        self.assertEqual(error.exception.code, "unavailable")
        self.assertTrue(self.http.response.closed)

    def test_clock_needs_timezone_and_response_extras_are_not_exposed(self):
        client = self.client(Response(payload(secret="not-an-output-field")))
        result = client.inquire("222", 4)
        self.assertNotIn("not-an-output-field", repr(result))
        client = self.client()
        client._clock = lambda: datetime(2026, 11, 2)
        with self.assertRaises(TyreSystemAPIError):
            client.inquire("222", 4)


if __name__ == "__main__":
    unittest.main()
