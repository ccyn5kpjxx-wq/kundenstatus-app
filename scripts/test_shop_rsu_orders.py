"""Synthetic XML responses/transports only; no provider connection or secrets."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import json
import socket
import sys
import unittest
from unittest.mock import patch
from xml.etree import ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import shop_rsu_orders as orders

NOW = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
CONFIG = {"TYRESYSTEM_API_USERNAME": "synthetic-user", "TYRESYSTEM_API_PASSWORD": "SYNTHETIC-PRIVATE-CANARY",
          "TYRESYSTEM_API_MODE": "test"}
PAYLOAD = {"reference": "SHOP-" + "a" * 32 + "-W", "positions": [
    {"pos": 1, "supplier_article_id": "123", "ean": "4019238594195", "quantity": 4, "unit_net": "68.34", "currency": "EUR"}],
    "delivery_address": {"neutraldelivery": 1, "salutation": "Firma", "name": "Synthetic & Werkstatt",
        "street": "Example 1", "zipcode": "01234", "city": "Mosbach", "countrycode": "DE"}}


def xml(rows=None):
    root = ET.Element("orderResponse")
    positions = ET.SubElement(root, "positions")
    rows = rows if rows is not None else [dict(pos=1, ordernumber="Test_1234", OrderStatus=1, error=0, errorMessage="")]
    for row in rows:
        p = ET.SubElement(positions, "position")
        for key, value in row.items():
            ET.SubElement(p, key).text = str(value)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def refused(pos, message="Error: Faulty Price. Current Tyresystem Price: 40.00 requested price:20.00"):
    return dict(pos=pos, ordernumber="", OrderStatus=0, error=1, errorMessage=message)


class Response:
    def __init__(self, raw=None, *, status=201, content_type="application/xml", chunks=None, url=orders.ORDER_URL):
        self.raw = xml() if raw is None else raw
        self.status_code, self.url = status, url
        self.headers = {"Content-Type": content_type}
        self.chunks, self.closed = chunks, False

    def iter_content(self, chunk_size):
        yield from self.chunks if self.chunks is not None else [self.raw]

    def close(self):
        self.closed = True


class HTTP:
    def __init__(self, response=None):
        self.response, self.calls = response if response is not None else Response(), []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


class OrderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.network = patch.object(socket, "socket", side_effect=AssertionError("network forbidden"))
        cls.network.start()

    @classmethod
    def tearDownClass(cls):
        cls.network.stop()

    def setUp(self):
        self.http = HTTP()
        self.authorizations = []

    def transport(self, *, policy=None, authorize=None, config=None, **kwargs):
        def allow(envelope):
            self.authorizations.append(envelope)
            self.assertEqual(envelope["request_hash"], orders.request_sha256(PAYLOAD))
            with self.assertRaises(TypeError):
                envelope["reference"] = "forged"
            return True
        return orders.RSUOrderTransport(config or CONFIG,
            policy=policy or orders.RSUOrderPolicy("test", "synthetic-mode-proof", "synthetic-full-acceptance", enabled=True),
            authorize=allow if authorize is None else authorize, http=self.http, clock=lambda: NOW, **kwargs)

    def submit(self, transport=None, payload=None):
        value = PAYLOAD if payload is None else payload
        return (transport or self.transport()).submit_order(value, idempotency_key=value["reference"])

    def test_disabled_and_live_unarmed_or_unconfirmed_modes_never_post(self):
        policies = [orders.RSUOrderPolicy("test"), orders.RSUOrderPolicy("test", enabled=True),
                    orders.RSUOrderPolicy("live", "mode-proof", "acceptance-proof", enabled=True)]
        for policy in policies:
            with self.subTest(policy=policy):
                transport = self.transport(policy=policy, config={**CONFIG, "TYRESYSTEM_API_MODE": policy.mode})
                self.assertEqual(self.submit(transport)["outcome"], "not_sent")
        self.assertEqual(self.http.calls, [])

    def test_fixed_single_position_xml_escapes_payload_and_binds_order_number(self):
        result = self.submit()
        self.assertEqual(result["outcome"], "accepted")
        self.assertEqual(len(self.http.calls), 1)
        url, kwargs = self.http.calls[0]
        self.assertEqual(url, "https://api.tyresystem.de/Rest/Order")
        self.assertFalse(kwargs["allow_redirects"])
        self.assertTrue(kwargs["verify"])
        self.assertTrue(kwargs["stream"])
        self.assertEqual(kwargs["timeout"], (3.05, 8.0))
        self.assertEqual(kwargs["headers"]["Accept"], "application/xml, text/xml")
        root = ET.fromstring(kwargs["data"])
        self.assertEqual(root.findtext("delivery_address/name"), "Synthetic & Werkstatt")
        self.assertEqual(root.findtext("delivery_address/zipcode"), "01234")
        self.assertEqual(root.findtext("ordernumber_customer"), PAYLOAD["reference"])
        self.assertEqual(root.findtext('delivery_address/neutraldelivery'), '1')
        self.assertEqual([p.findtext("price") for p in root.findall("items/item")], ["68.3400"])
        self.assertEqual([p["order_number"] for p in result["positions"]], ["Test_1234"])
        self.assertEqual(result["positions"][0]["quantity"], 4)
        self.assertEqual(result["positions"][0]["facts_source"], "submitted_request_under_external_policy")
        for key in ("echo_confirmed", "fulfillment_confirmed", "capture_authorized", "retry_authorized"):
            self.assertFalse(result[key])
        self.assertTrue(self.http.response.closed)

    def test_authorization_and_frozen_reference_are_required_before_post(self):
        for authorize in (lambda _: False, lambda _: 1, lambda _: (_ for _ in ()).throw(RuntimeError("SYNTHETIC-PRIVATE-CANARY"))):
            with self.subTest(authorize=authorize):
                self.assertEqual(self.submit(self.transport(authorize=authorize))["outcome"], "not_sent")
        result = self.transport().submit_order(PAYLOAD, idempotency_key="different")
        self.assertEqual(result["outcome"], "not_sent")
        self.assertEqual(self.http.calls, [])

    def test_canonical_request_hash_and_detached_frozen_payload(self):
        value = deepcopy(PAYLOAD)
        frozen = orders.freeze_order_request(value)
        digest = orders.request_sha256(frozen)
        value["positions"][0]["unit_net"] = "1"
        self.assertEqual(orders.request_sha256(frozen), digest)
        detached = frozen.as_dict()
        detached["positions"][0]["quantity"] = 1
        self.assertEqual(frozen.as_dict()["positions"][0]["quantity"], 4)
        same = deepcopy(PAYLOAD)
        same["positions"][0]["unit_net"] = "68.3400"
        self.assertEqual(orders.request_sha256(same), digest)
        self.assertNotIn("68.34", repr(frozen))

    def test_single_rejection_is_definitive_but_extra_partial_ack_is_unknown(self):
        self.http = HTTP(Response(xml([refused(1)]), status=404))
        result = self.submit()
        self.assertEqual(result["outcome"], "rejected")
        self.assertFalse(result["manual_review_required"])
        self.assertNotIn("Current", json.dumps(result))
        self.http = HTTP(Response(xml([dict(pos=1, ordernumber="Test_1234", OrderStatus=1, error=0, errorMessage=""), refused(2)]), status=200))
        result = self.submit()
        self.assertEqual(result["outcome"], "unknown")
        self.assertTrue(result["manual_review_required"])
        self.assertEqual(result['positions'], [])
        self.assertFalse(result["capture_authorized"])

    def test_live_404_rejection_requires_external_schema_confirmation(self):
        self.http = HTTP(Response(xml([refused(1)]), status=404))
        policy = orders.RSUOrderPolicy("live", "written-mode-proof", "written-acceptance-proof", enabled=True, live_armed=True)
        config = {**CONFIG, "TYRESYSTEM_API_MODE": "live"}
        self.assertEqual(self.submit(self.transport(policy=policy, config=config))["outcome"], "unknown")
        self.assertEqual(self.submit(self.transport(policy=replace(policy, rejection_policy_evidence="written-404-proof"), config=config))["outcome"], "rejected")

    def test_http_201_missing_malformed_and_foreign_positions_are_unknown(self):
        cases = [b"", b"<html>missing</html>", xml([refused(1)]), xml([]),
                 xml([dict(pos=1, ordernumber="Test_1234", OrderStatus=1, error=0, errorMessage=""),
                      dict(pos=1, ordernumber="Test_1234", OrderStatus=1, error=0, errorMessage="")]),
                 xml().replace(b"<pos>1</pos>", b"<pos>3</pos>"),
                 xml().replace(b"</OrderStatus>", b"</OrderStatus><orderStatus>1</orderStatus>"),
                 b'<!DOCTYPE orderResponse [<!ENTITY p "private">]><orderResponse>&p;</orderResponse>',
                 xml().replace(b'encoding=\'utf-8\'', b'encoding=\'iso-8859-1\'')]
        for raw in cases:
            with self.subTest(raw=raw):
                self.http = HTTP(Response(raw))
                result = self.submit()
                self.assertEqual(result["outcome"], "unknown")
                self.assertTrue(result["manual_review_required"])
                self.assertEqual(len(self.http.calls), 1)

    def test_unknown_transport_or_response_never_retries_post_or_leaks_errors(self):
        for value in (TimeoutError("SYNTHETIC-PRIVATE-CANARY"), Response(status=500),
                      Response(content_type="text/html"), Response(url="https://wrong.invalid/"),
                      Response(chunks=[b"x" * (orders.MAX_RESPONSE_BYTES + 1)])):
            with self.subTest(value=value):
                self.http = HTTP(value)
                transport = self.transport()
                result = self.submit(transport)
                self.assertEqual(result["outcome"], "unknown")
                self.assertEqual(len(self.http.calls), 1)
                self.assertNotIn("SYNTHETIC-PRIVATE-CANARY", json.dumps(result) + repr(transport))
                self.assertFalse(result["retry_authorized"])

    def test_invalid_commands_never_reach_post(self):
        mutations = [lambda p: p["positions"][0].update(quantity=True),
                     lambda p: p["positions"][0].update(unit_net=68.34),
                     lambda p: p["positions"][0].update(currency="USD"),
                     lambda p: p['positions'].append({**p['positions'][0], 'pos': 2, 'supplier_article_id': '456'}),
                     lambda p: p['delivery_address'].update(neutraldelivery=0),
                     lambda p: p["delivery_address"].update(neutraldelivery=True),
                     lambda p: p["delivery_address"].update(name="bad\x00name"),
                     lambda p: p.update(reference="bad reference")]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                value = deepcopy(PAYLOAD)
                mutation(value)
                result = self.transport().submit_order(value, idempotency_key=value["reference"])
                self.assertEqual(result["outcome"], "not_sent")
        self.assertEqual(self.http.calls, [])


if __name__ == "__main__":
    unittest.main()
