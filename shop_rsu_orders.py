"""Disabled XML transport; one position and one explicit POST, never a retry.

The caller durably claims a frozen shipment before authorize(envelope) allows
the POST. RSU does not document supplier idempotency or an order lookup here.
ACK fields are pos/status/error/ordernumber only. Returned SKU/quantity/net price
are explicitly bound to the submitted request under external written policy,
not echoed by RSU, delivered goods, or permission to capture a customer payment.
No credentials, environment, files, database or network are read on import.
"""
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
import re
import time
from types import MappingProxyType
import unicodedata
from xml.etree import ElementTree as ET

ORDER_URL = "https://api.tyresystem.de/Rest/Order"
MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, MAX_POSITIONS = 16384, 65536, 1
TIMEOUT, MAX_SECONDS = (3.05, 8.0), 15.0
REFERENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}")
HASH = re.compile(r"[a-f0-9]{64}")
_POSITION = {"pos", "supplier_article_id", "ean", "quantity", "unit_net", "currency"}
_ADDRESS = {"neutraldelivery", "salutation", "name", "street", "zipcode", "city", "countrycode"}
_ACK = {"ordernumber", "orderStatus", "error", "errorMessage", "pos"}
_REFUSAL404 = re.compile(
    r"Error: (?:Faulty Price\. Current Tyresystem Price: [0-9]+(?:\.[0-9]+)? requested price: ?[0-9]+(?:\.[0-9]+)?"
    r"|Article is not available Error: Requested Amount is not available\. Current Available stock: [0-9]+)"
)


class RSUOrderError(ValueError):
    def __init__(self, code="invalid_command", outcome="not_sent"):
        self.code, self.outcome = code, outcome
        super().__init__(code)


@dataclass(frozen=True)
class RSUOrderPolicy:
    mode: str
    mode_confirmed_evidence: str = ""
    acceptance_policy_evidence: str = ""
    # Production HTTP404 rejection semantics require separate confirmation.
    rejection_policy_evidence: str = ""
    enabled: bool = False
    live_armed: bool = False


@dataclass(frozen=True, repr=False)
class FrozenRSUOrder:
    _json: str

    def __repr__(self):
        return "<FrozenRSUOrder private_payload=redacted>"

    def as_dict(self):
        return json.loads(self._json)


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _hash(value):
    return sha256(_canonical(value).encode()).hexdigest()


def _text(value, maximum):
    if (not isinstance(value, str) or not 1 <= len(value) <= maximum or value != value.strip()
            or any(unicodedata.category(c).startswith("C") for c in value)):
        raise ValueError()
    return value


def _evidence(value):
    return (isinstance(value, str) and 1 <= len(value) <= 160 and "://" not in value
            and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/ -]*", value) is not None)


def _ean(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{13}", value) or set(value) == {"0"}:
        return False
    total = sum(int(c) * (3 if i % 2 == 0 else 1) for i, c in enumerate(reversed(value[:-1])))
    return (10 - total % 10) % 10 == int(value[-1])


def freeze_order_request(value):
    try:
        value = value.as_dict() if isinstance(value, FrozenRSUOrder) else value
        if (not isinstance(value, Mapping) or set(value) != {"reference", "positions", "delivery_address"}
                or not isinstance(value["reference"], str) or not REFERENCE.fullmatch(value["reference"])
                or not isinstance(value["positions"], list) or len(value["positions"]) != MAX_POSITIONS):
            raise ValueError()
        positions, seen_articles = [], set()
        for index, item in enumerate(value["positions"], 1):
            if (not isinstance(item, Mapping) or set(item) != _POSITION
                    or type(item["pos"]) is not int or item["pos"] != index
                    or not isinstance(item["supplier_article_id"], str)
                    or not re.fullmatch(r"[1-9][0-9]{0,17}", item["supplier_article_id"])
                    or item["supplier_article_id"] in seen_articles or not _ean(item["ean"])
                    or type(item["quantity"]) is not int or not 1 <= item["quantity"] <= 20
                    or item["currency"] != "EUR" or not isinstance(item["unit_net"], str)
                    or not re.fullmatch(r"(?:0|[1-9][0-9]{0,6})(?:\.[0-9]{1,4})?", item["unit_net"])
                    or not Decimal("0") < Decimal(item["unit_net"]) <= Decimal("1000000")):
                raise ValueError()
            seen_articles.add(item["supplier_article_id"])
            positions.append({**item, "unit_net": format(Decimal(item["unit_net"]), ".4f")})
        address = value["delivery_address"]
        if (not isinstance(address, Mapping) or set(address) != _ADDRESS
                or type(address["neutraldelivery"]) is not int or address["neutraldelivery"] != 1
                or address["countrycode"] != "DE" or address.get('salutation') not in {'Firma', 'Herr', 'Frau'}
                or not isinstance(address["zipcode"], str)
                or not re.fullmatch(r"[0-9]{5}", address["zipcode"])):
            raise ValueError()
        address = dict(address)
        for key, limit in (("salutation", 20), ("name", 200), ("street", 200), ("city", 120)):
            address[key] = _text(address[key], limit)
        result = FrozenRSUOrder(_canonical({"reference": value["reference"], "positions": positions,
                                            "delivery_address": address}))
        if len(_xml(result)) > MAX_REQUEST_BYTES:
            raise ValueError()
        return result
    except Exception:
        raise RSUOrderError() from None


def request_sha256(value):
    return _hash(freeze_order_request(value).as_dict())


def _xml(frozen):
    value = frozen.as_dict()
    root = ET.Element("order")
    items = ET.SubElement(root, "items")
    for position in value["positions"]:
        item = ET.SubElement(items, "item")
        for key, text in (("pos", position["pos"]), ("idArticle", position["supplier_article_id"]),
                          ("ean", position["ean"]), ("guid", ""), ("amount", position["quantity"]),
                          ("price", position["unit_net"])):
            ET.SubElement(item, key).text = str(text)
    address = ET.SubElement(root, "delivery_address")
    for key in ("neutraldelivery", "salutation", "name", "street", "zipcode", "city", "countrycode"):
        ET.SubElement(address, key).text = str(value["delivery_address"][key])
    ET.SubElement(root, "ordernumber_customer").text = value["reference"]
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _result(command, outcome, positions=(), *, code=None):
    value = command.as_dict() if command is not None else None
    return {"outcome": outcome, "reference": value["reference"] if value else None,
            "request_hash": _hash(value) if value else None, "positions": deepcopy(list(positions)),
            "code": code, "manual_review_required": outcome in {"partial", "unknown"},
            "echo_confirmed": False, "fulfillment_confirmed": False,
            "capture_authorized": False, "retry_authorized": False}


def _parse(raw, *, status, mode, command, rejection404_confirmed):
    """Strict complete position ACK; request facts are never called echoes."""
    try:
        text = raw.decode("utf-8-sig")
        if re.search(r"<!\s*(?:DOCTYPE|ENTITY)\b", text, re.I):
            raise ValueError()
        encoding = re.search(r"<\?xml\b[^>]*\bencoding\s*=\s*['\"]([^'\"]+)['\"]", text, re.I)
        if encoding and encoding[1].lower() not in {"utf-8", "utf8"}:
            raise ValueError()
        root = ET.fromstring(text)
        if root.tag != "orderResponse" or root.attrib or len(root) != 1:
            raise ValueError()
        container = root[0]
        wanted = command.as_dict()["positions"]
        if container.tag != "positions" or container.attrib or len(container) != len(wanted):
            raise ValueError()
        for node in (root, container):
            if (node.text and node.text.strip()) or (node.tail and node.tail.strip()):
                raise ValueError()
        records = {}
        for node in container:
            if (node.tag != "position" or node.attrib or (node.text and node.text.strip())
                    or (node.tail and node.tail.strip())):
                raise ValueError()
            record = {}
            for child in node:
                key = "orderStatus" if child.tag == "OrderStatus" else child.tag
                if (child.attrib or len(child) or key in record or (child.tail and child.tail.strip())):
                    raise ValueError()
                record[key] = child.text or ""
            if (set(record) != _ACK or not re.fullmatch(r"[1-9][0-9]{0,2}", record["pos"])
                    or record["pos"] != str(int(record["pos"])) or int(record["pos"]) in records
                    or record["orderStatus"] not in {"0", "1"} or record["error"] not in {"0", "1"}
                    or len(record["errorMessage"]) > 2048):
                raise ValueError()
            records[int(record["pos"])] = record
        if set(records) != set(range(1, len(wanted) + 1)):
            raise ValueError()
        results = []
        for item in wanted:
            record = records[item["pos"]]
            number, message = record["ordernumber"], record["errorMessage"]
            valid_number = (re.fullmatch(r"[1-9][0-9]{0,29}", number) is not None
                            or mode == "test" and re.fullmatch(r"Test_[0-9]{1,25}", number) is not None)
            accepted = (status in {200, 201} and record["orderStatus"] == "1" and record["error"] == "0"
                        and message == "" and valid_number)
            rejected = (record["orderStatus"] == "0" and record["error"] == "1" and number == ""
                        and (status == 200 or status == 404 and rejection404_confirmed
                             and _REFUSAL404.fullmatch(" ".join(message.split())) is not None))
            outcome = "accepted" if accepted else "rejected" if rejected else "unknown"
            results.append({**item, "outcome": outcome, "order_number": number if accepted else None,
                            "request_bound": True, "facts_source": "submitted_request_under_external_policy",
                            "echo_confirmed": False})
        outcomes = {item["outcome"] for item in results}
        overall = ("accepted" if outcomes == {"accepted"} else "rejected" if outcomes == {"rejected"}
                   else "partial" if "accepted" in outcomes else "unknown")
        return _result(command, overall, results)
    except Exception:
        return _result(command, "unknown", code="response_unconfirmed")


class RSUOrderTransport:
    def __init__(self, config, *, policy, authorize=None, http=None, clock=None, monotonic=None):
        try:
            username, password, mode = (config.get(key) for key in
                ("TYRESYSTEM_API_USERNAME", "TYRESYSTEM_API_PASSWORD", "TYRESYSTEM_API_MODE"))
            if (not isinstance(policy, RSUOrderPolicy) or policy.mode not in {"test", "live"}
                    or mode != policy.mode or type(policy.enabled) is not bool or type(policy.live_armed) is not bool
                    or not isinstance(username, str) or not 1 <= len(username) <= 200 or ":" in username
                    or not isinstance(password, str) or not 1 <= len(password) <= 500
                    or any(ord(c) < 33 or ord(c) > 126 for c in username + password)):
                raise ValueError()
        except Exception:
            raise RSUOrderError("invalid_configuration") from None
        self._username, self._password, self._policy = username, password, policy
        self._authorize, self._http = authorize, http
        self._clock, self._monotonic = clock or (lambda: datetime.now(timezone.utc)), monotonic or time.monotonic
        self.account_fingerprint = _hash({"api_username": username, "configured_mode": mode})

    def __repr__(self):
        return "<RSUOrderTransport disabled_unless_explicitly_armed private_config=redacted>"

    def submit_order(self, payload, *, idempotency_key):
        try:
            command = freeze_order_request(payload)
        except RSUOrderError:
            return _result(None, "not_sent", code="invalid_command")
        value, policy = command.as_dict(), self._policy
        if (idempotency_key != value["reference"] or not policy.enabled
                or policy.mode == "live" and not policy.live_armed
                or not _evidence(policy.mode_confirmed_evidence) or not _evidence(policy.acceptance_policy_evidence)):
            return _result(command, "not_sent", code="not_armed")
        envelope = MappingProxyType({"reference": value["reference"], "request_hash": _hash(value),
            "configured_mode": policy.mode, "account_fingerprint": self.account_fingerprint,
            "mode_evidence_id": policy.mode_confirmed_evidence,
            "acceptance_evidence_id": policy.acceptance_policy_evidence, "idempotency_key": idempotency_key})
        try:
            if not callable(self._authorize) or self._authorize(envelope) is not True:
                return _result(command, "not_sent", code="not_authorized")
            now = self._clock()
            if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
                return _result(command, "not_sent", code="invalid_clock")
        except Exception:
            return _result(command, "not_sent", code="not_authorized")
        response = owned = None
        try:
            deadline = self._monotonic() + MAX_SECONDS
            http = self._http
            if http is None:
                import requests
                owned = http = requests.Session()
                http.trust_env = False
                http.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            response = http.post(ORDER_URL, data=_xml(command), auth=(self._username, self._password),
                headers={"Content-Type": "application/xml; charset=utf-8", "Accept": "application/xml, text/xml",
                         "Accept-Encoding": "identity"}, timeout=TIMEOUT, allow_redirects=False, stream=True, verify=True)
            if (self._monotonic() >= deadline or response.status_code not in {200, 201, 404}
                    or getattr(response, "url", ORDER_URL) != ORDER_URL
                    or response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                       not in {"application/xml", "text/xml"}):
                return _result(command, "unknown", code="response_unconfirmed")
            length = response.headers.get("Content-Length")
            if length is not None and (not isinstance(length, str) or not re.fullmatch(r"[0-9]{1,9}", length)
                                       or int(length) > MAX_RESPONSE_BYTES):
                return _result(command, "unknown", code="response_unconfirmed")
            raw = bytearray()
            for chunk in response.iter_content(chunk_size=4096):
                if self._monotonic() >= deadline or not isinstance(chunk, bytes) or len(raw) + len(chunk) > MAX_RESPONSE_BYTES:
                    return _result(command, "unknown", code="response_unconfirmed")
                raw.extend(chunk)
            if self._monotonic() >= deadline:
                return _result(command, "unknown", code="response_unconfirmed")
            result = _parse(bytes(raw), status=response.status_code, mode=policy.mode, command=command,
                            rejection404_confirmed=policy.mode == "test" or _evidence(policy.rejection_policy_evidence))
            if self._monotonic() >= deadline:
                return _result(command, "unknown", code="response_unconfirmed")
            return result
        except Exception:
            return _result(command, "unknown", code="transport_unconfirmed")
        finally:
            for resource in (response, owned):
                if resource is not None:
                    try:
                        resource.close()
                    except Exception:
                        pass
