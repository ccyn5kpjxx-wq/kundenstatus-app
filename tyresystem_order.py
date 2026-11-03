"""Explicit single-position TYREsystem POST and durable at-most-one attempt.

No import IO, app, dotenv, reservation or supplier-mode inference. The account's
actual RSU test/live approval and fixed workshop address are upstream policy.
A documented position ACK does not echo SKU, quantity, price or reference.
"""
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
import re
import secrets
import time
from types import MappingProxyType
import unicodedata
from xml.etree import ElementTree as ET

import requests

from tyresystem_api import SOURCE_DOCUMENT, account_fingerprint, _unique_object, _invalid_constant


ORDER_URL = "https://api.tyresystem.de/Rest/Order"
TIMEOUT = (3.05, 8.0)
MAX_ORDER_SECONDS = 15.0
MAX_REQUEST_BYTES = 16384
MAX_RESPONSE_BYTES = 65536
BACKUP_TABLES = ("tyresystem_order_attempts",)
SCHEMA_FEATURE = "tyresystem_order_attempts_v1"
_KEYS = frozenset({"produktart", "artikel_id", "ean", "anzahl", "einkauf_netto", "waehrung", "reference", "delivery_address"})
_ADDRESS = frozenset({"neutraldelivery", "salutation", "name", "street", "zipcode", "city", "countrycode"})
_POSITION = frozenset({"ordernumber", "orderStatus", "error", "errorMessage", "pos"})
_MESSAGES = {
    "configuration": "Der eigene TYREsystem-Bestellzugang ist nicht gültig eingerichtet.",
    "input": "Die vorbereitete Lieferantenbestellung ist nicht eindeutig gültig.",
    "not_authorized": "Die Lieferantenbestellung ist für diesen Aufruf nicht freigegeben.",
    "supplier_rejected": "Der Lieferant hat die Bestellposition ausdrücklich abgelehnt.",
    "attempt_conflict": "Diese Bestellreferenz gehört zu einem anderen vorbereiteten Auftrag.",
    "attempt_unknown": "Der Lieferantenauftrag muss geprüft werden. Bitte nicht erneut bestellen.",
    "unknown": "Die Lieferantenbestellung konnte nicht eindeutig bestätigt werden. Bitte nicht erneut bestellen.",
}


class TyreSystemOrderError(ValueError):
    def __init__(self, code="unknown"):
        self.code = code if code in _MESSAGES else "unknown"
        self.outcome = ("not_sent" if self.code in {"configuration", "input", "not_authorized"}
                        else "rejected" if self.code == "supplier_rejected" else "unknown")
        super().__init__(_MESSAGES[self.code])


def _canonical(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value):
    return hashlib.sha256(value.encode("utf-8") if isinstance(value, str) else value).hexdigest()


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError()
    return value.astimezone(timezone.utc)


def _text(value, maximum):
    if (not isinstance(value, str) or not 1 <= len(value) <= maximum or value != value.strip()
            or any(unicodedata.category(c).startswith("C") for c in value)):
        raise ValueError()
    return value


def _reference(value):
    if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", value)
            or not re.search(r"[A-Za-z0-9]", value)):
        raise ValueError()
    return value


@dataclass(frozen=True, slots=True, repr=False)
class FrozenOrderRequest:
    produktart: str
    artikel_id: str
    ean: str
    anzahl: int
    einkauf_netto: str
    waehrung: str
    reference: str
    delivery_address: Mapping

    def __repr__(self):
        return "<FrozenOrderRequest private_order=redacted>"

    def as_dict(self):
        return {"produktart": self.produktart, "artikel_id": self.artikel_id, "ean": self.ean,
                "anzahl": self.anzahl, "einkauf_netto": self.einkauf_netto, "waehrung": self.waehrung,
                "reference": self.reference, "delivery_address": dict(self.delivery_address)}


def freeze_order_request(value):
    """Strict local record, with immutable nested address; never accepts VK/floats."""
    try:
        if isinstance(value, FrozenOrderRequest):
            value = value.as_dict()
        if not isinstance(value, Mapping) or set(value) != _KEYS:
            raise ValueError()
        if (value["produktart"] not in {"reifen", "felgen"} or value["waehrung"] != "EUR"
                or not isinstance(value["artikel_id"], str) or not re.fullmatch(r"[1-9][0-9]{0,17}", value["artikel_id"])
                or not isinstance(value["ean"], str) or not re.fullmatch(r"[0-9]{13}", value["ean"])
                or type(value["anzahl"]) is not int or not 1 <= value["anzahl"] <= 8):
            raise ValueError()
        price = value["einkauf_netto"]
        if (not isinstance(price, str) or not re.fullmatch(r"(?:0|[1-9][0-9]{0,6})(?:\.[0-9]{1,4})?", price)
                or not Decimal("0") < Decimal(price) <= Decimal("1000000")):
            raise ValueError()
        address = value["delivery_address"]
        if (not isinstance(address, Mapping) or set(address) != _ADDRESS
                or type(address["neutraldelivery"]) is not int or address["neutraldelivery"] not in (0, 1)
                or address["salutation"] != "Firma" or address["countrycode"] != "DE"
                or not isinstance(address["zipcode"], str) or not re.fullmatch(r"[0-9]{5}", address["zipcode"])):
            raise ValueError()
        clean_address = dict(address)
        for key, maximum in (("name", 200), ("street", 200), ("city", 120)):
            clean_address[key] = _text(address[key], maximum)
        return FrozenOrderRequest(value["produktart"], value["artikel_id"], value["ean"], value["anzahl"],
            format(Decimal(price), ".4f"), "EUR", _reference(value["reference"]), MappingProxyType(clean_address))
    except Exception:
        raise TyreSystemOrderError("input") from None


def request_sha256(value):
    """Authoritative SHA of the normalized record, excluding account/mode."""
    return _hash(_canonical(freeze_order_request(value).as_dict()))


def _xml(request):
    root = ET.Element("order")
    item = ET.SubElement(ET.SubElement(root, "items"), "item")
    for name, value in (("pos", "1"), ("idArticle", request.artikel_id), ("ean", request.ean),
                        ("guid", None), ("amount", str(request.anzahl)), ("price", request.einkauf_netto)):
        ET.SubElement(item, name).text = value
    address = ET.SubElement(root, "delivery_address")
    for name in ("neutraldelivery", "salutation", "name", "street", "zipcode", "city", "countrycode"):
        ET.SubElement(address, name).text = str(request.delivery_address[name])
    ET.SubElement(root, "ordernumber_customer").text = request.reference
    raw = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    if len(raw) > MAX_REQUEST_BYTES:
        raise TyreSystemOrderError("input")
    return raw


def _flag(value, *, boolean=False):
    if boolean and type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value in ("0", "1"):
        return value == "1"
    raise ValueError()


def _supplier_number(value):
    return isinstance(value, str) and bool(re.fullmatch(r"[1-9][0-9]{0,29}", value))


def _parse_ack(raw, content_type):
    """Accept one documented position only; do not invent missing identity echoes."""
    try:
        text = raw.decode("utf-8-sig")
        if content_type in {"application/json", "text/json"}:
            result = json.loads(text, object_pairs_hook=_unique_object, parse_float=Decimal,
                                parse_constant=_invalid_constant)
            if not isinstance(result, dict) or set(result) != {"orderResponse"}:
                raise ValueError()
            root = result["orderResponse"]
            if not isinstance(root, dict) or set(root) != {"positions"}:
                raise ValueError()
            positions = root["positions"]
            if not isinstance(positions, dict) or set(positions) != {"position"}:
                raise ValueError()
            positions = positions["position"]
            if not isinstance(positions, list) or len(positions) != 1:
                raise ValueError()
            record = positions[0]
        elif content_type in {"application/xml", "text/xml"}:
            if re.search(r"<!\s*(?:DOCTYPE|ENTITY)\b", text, re.I):
                raise ValueError()
            encoding = re.search(r"<\?xml\b[^>]*\bencoding\s*=\s*['\"]([^'\"]+)['\"]", text, re.I)
            if encoding and encoding[1].lower() not in {"utf-8", "utf8"}:
                raise ValueError()
            root = ET.fromstring(text)
            if root.tag != "orderResponse" or root.attrib or len(root) != 1:
                raise ValueError()
            positions = root[0]
            if positions.tag != "positions" or positions.attrib or len(positions) != 1:
                raise ValueError()
            position = positions[0]
            if position.tag != "position" or position.attrib:
                raise ValueError()
            for container in (root, positions, position):
                if (container.text and container.text.strip()) or (container.tail and container.tail.strip()):
                    raise ValueError()
            pairs = []
            for child in position:
                if child.attrib or len(child) or (child.tail and child.tail.strip()):
                    raise ValueError()
                pairs.append((child.tag, child.text or ""))
            record = _unique_object(pairs)
        else:
            raise ValueError()
        if not isinstance(record, Mapping) or set(record) != _POSITION:
            raise ValueError()
        if not (type(record["pos"]) is int and record["pos"] == 1 or type(record["pos"]) is str and record["pos"] == "1"):
            raise ValueError()
        status, error = _flag(record["orderStatus"], boolean=True), _flag(record["error"])
        message = record["errorMessage"]
        if message is not None and (not isinstance(message, str) or len(message) > 2048):
            raise ValueError()
        number = record["ordernumber"]
        if not status and error and number in (None, ""):
            raise TyreSystemOrderError("supplier_rejected")
        if not status or error or message not in (None, "") or not _supplier_number(number):
            raise ValueError()
        return number
    except TyreSystemOrderError:
        raise
    except Exception:
        raise TyreSystemOrderError("unknown") from None


def _receipt(request, mode, account, number, received, binding):
    return {"bestaetigt": True, "bestellnummer": number, "reference": request.reference,
            "produktart": request.produktart, "artikel_id": request.artikel_id, "ean": request.ean,
            "anzahl": request.anzahl, "einkauf_netto": request.einkauf_netto, "waehrung": request.waehrung,
            "configured_mode": mode, "account_fingerprint": account, "receiptbinding_source": binding,
            "received_at": received}


def _checked_receipt(receipt, request, mode, account, binding):
    try:
        if not isinstance(receipt, dict) or not _supplier_number(receipt.get("bestellnummer")):
            raise ValueError()
        received = _utc(datetime.fromisoformat(receipt["received_at"])).isoformat()
        expected = _receipt(request, mode, account, receipt["bestellnummer"], received, binding)
        if (set(receipt) != set(expected) or type(receipt["bestaetigt"]) is not bool
                or type(receipt["anzahl"]) is not int or receipt != expected):
            raise ValueError()
        return expected
    except Exception:
        raise TyreSystemOrderError("attempt_unknown") from None


class TyreSystemOrderClient:
    def __init__(self, config, *, authorize=None, http=None, clock=None, monotonic=None):
        try:
            if not isinstance(config, Mapping):
                raise ValueError()
            username, password, mode = (config.get(key) for key in (
                "TYRESYSTEM_API_USERNAME", "TYRESYSTEM_API_PASSWORD", "TYRESYSTEM_API_MODE"))
            if (not isinstance(username, str) or not 1 <= len(username) <= 200 or ":" in username
                    or not isinstance(password, str) or not 1 <= len(password) <= 500
                    or any(ord(c) < 33 or ord(c) > 126 for c in username + password) or mode not in {"test", "live"}):
                raise ValueError()
            self._username, self._password, self._mode = username, password, mode
            self._account = account_fingerprint(username, mode)
            self._authorize, self._http = authorize, http
            self._clock = clock or (lambda: datetime.now(timezone.utc))
            self._monotonic = monotonic or time.monotonic
        except Exception:
            raise TyreSystemOrderError("configuration") from None

    def __repr__(self):
        return "<TyreSystemOrderClient private_configuration=redacted>"

    @property
    def configured_mode(self):
        return self._mode

    @property
    def account_fingerprint(self):
        return self._account

    def now(self):
        try:
            return _utc(self._clock())
        except Exception:
            raise TyreSystemOrderError("configuration") from None

    @classmethod
    def from_env(cls, *, environment=None, **kwargs):
        values = os.environ if environment is None else environment
        if not isinstance(values, Mapping):
            raise TyreSystemOrderError("configuration")
        return cls({key: values.get(key) for key in ("TYRESYSTEM_API_USERNAME", "TYRESYSTEM_API_PASSWORD", "TYRESYSTEM_API_MODE")}, **kwargs)

    def check_authorized(self, request):
        values = request.as_dict()
        values["delivery_address"] = MappingProxyType(values["delivery_address"])
        envelope = MappingProxyType({"reference":request.reference, "request":MappingProxyType(values),
            "request_sha256":request_sha256(request), "configured_mode":self.configured_mode,
            "account_fingerprint":self.account_fingerprint})
        try:
            allowed = callable(self._authorize) and self._authorize(envelope) is True
        except Exception:
            allowed = False
        if not allowed:
            raise TyreSystemOrderError("not_authorized")

    def submit(self, frozen_request):
        request = freeze_order_request(frozen_request)
        body = _xml(request)
        self.check_authorized(request)
        self.now()  # A bad local clock must fail before a possible POST.
        response = owned_http = None
        try:
            deadline = self._monotonic() + MAX_ORDER_SECONDS
            http = self._http
            if http is None:
                owned_http = http = requests.Session()
                http.trust_env = False
                http.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            response = http.post(ORDER_URL, data=body, auth=(self._username, self._password),
                headers={"Content-Type": "application/xml; charset=utf-8", "Accept": "application/json, application/xml;q=0.9",
                         "Accept-Encoding": "identity"}, timeout=TIMEOUT, allow_redirects=False, stream=True, verify=True)
            if (self._monotonic() >= deadline or response.status_code != 200
                    or getattr(response, "url", ORDER_URL) != ORDER_URL):
                raise TyreSystemOrderError("unknown")
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            length = response.headers.get("Content-Length")
            if length is not None and (not isinstance(length, str) or not re.fullmatch(r"[0-9]{1,9}", length)
                                       or int(length) > MAX_RESPONSE_BYTES):
                raise TyreSystemOrderError("unknown")
            raw = bytearray()
            for chunk in response.iter_content(chunk_size=4096):
                if (self._monotonic() >= deadline or not isinstance(chunk, bytes)
                        or len(raw) + len(chunk) > MAX_RESPONSE_BYTES):
                    raise TyreSystemOrderError("unknown")
                raw.extend(chunk)
            if self._monotonic() >= deadline:
                raise TyreSystemOrderError("unknown")
            number = _parse_ack(bytes(raw), content_type)
            received = self.now().isoformat()
            if self._monotonic() >= deadline:
                raise TyreSystemOrderError("unknown")
            return _receipt(request, self.configured_mode, self.account_fingerprint, number, received, "submitted_request")
        except TyreSystemOrderError as error:
            if error.outcome == "not_sent":
                raise TyreSystemOrderError("unknown") from None
            raise
        except Exception:
            raise TyreSystemOrderError("unknown") from None
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass
            if owned_http is not None:
                try:
                    owned_http.close()
                except Exception:
                    pass


def init_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS tyresystem_order_attempts (
        order_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL UNIQUE, reference TEXT NOT NULL UNIQUE,
        configured_mode TEXT NOT NULL, account_fingerprint TEXT NOT NULL,
        request_hash TEXT NOT NULL, request_json TEXT NOT NULL, request_xml_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('claimed','acknowledged','unknown','rejected','blocked')),
        receipt_json TEXT NOT NULL DEFAULT '{}', receipt_hash TEXT NOT NULL DEFAULT '',
        failure_code TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )""")


class DurableTyreSystemOrders:
    def __init__(self, get_db, client):
        if not callable(get_db) or not isinstance(client, TyreSystemOrderClient):
            raise TyreSystemOrderError("configuration")
        self._get_db, self.client = get_db, client

    def __repr__(self):
        return "<DurableTyreSystemOrders private_storage=redacted>"

    @property
    def configured_mode(self):
        return self.client.configured_mode

    @property
    def account_fingerprint(self):
        return self.client.account_fingerprint

    def _identity(self, request):
        return _hash(_canonical({"request": request.as_dict(), "configured_mode": self.client.configured_mode,
                                 "account_fingerprint": self.client.account_fingerprint}))

    def _verify_identity(self, row, request, identity):
        if (row["reference"] != request.reference or row["configured_mode"] != self.client.configured_mode
                or row["account_fingerprint"] != self.client.account_fingerprint or row["request_hash"] != identity
                or row["request_json"] != _canonical(request.as_dict()) or row["request_xml_sha256"] != _hash(_xml(request))):
            raise TyreSystemOrderError("attempt_conflict")

    def _existing(self, row, request, identity):
        self._verify_identity(row, request, identity)
        if row["state"] == "rejected" and row["failure_code"] == "supplier_rejected":
            raise TyreSystemOrderError("supplier_rejected")
        if row["state"] != "acknowledged":
            raise TyreSystemOrderError("attempt_unknown")
        try:
            receipt = json.loads(row["receipt_json"], object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
            if row["receipt_hash"] != _hash(_canonical(receipt)):
                raise ValueError()
            return _checked_receipt(receipt, request, self.client.configured_mode, self.client.account_fingerprint, "persisted_request")
        except Exception:
            raise TyreSystemOrderError("attempt_unknown") from None

    def acknowledged(self, order_id, frozen_request):
        """Read an existing verified ACK only; never claim, authorize or send.

        A non-ACK is not proof that no supplier order exists. Callers must keep
        claimed/unknown cases blocked rather than falling back to submission.
        """
        request = freeze_order_request(frozen_request)
        if type(order_id) is not str or order_id != request.reference:
            raise TyreSystemOrderError("input")
        identity = self._identity(request)
        try:
            with closing(self._get_db()) as db:
                found = db.execute("SELECT * FROM tyresystem_order_attempts WHERE order_id=?", (order_id,)).fetchone()
            if found is None:
                return None
            row = dict(found)
            self._verify_identity(row, request, identity)
            if row["state"] != "acknowledged":
                return None
            return self._existing(row, request, identity)
        except TyreSystemOrderError:
            raise
        except Exception:
            raise TyreSystemOrderError("attempt_unknown") from None

    def _mark_failure(self, order_id, attempt, identity, state, code):
        try:
            with closing(self._get_db()) as db:
                db.execute("UPDATE tyresystem_order_attempts SET state=?,failure_code=?,updated_at=? "
                           "WHERE order_id=? AND attempt_id=? AND request_hash=? AND state='claimed'",
                           (state, code, self.client.now().isoformat(), order_id, attempt, identity))
                db.commit()
        except Exception:
            pass  # A durable claimed row still blocks any automatic second POST.

    def submit_once(self, order_id, frozen_request):
        request = freeze_order_request(frozen_request)
        if order_id != request.reference or type(order_id) is not str:
            raise TyreSystemOrderError("input")
        identity = self._identity(request)
        canonical = _canonical(request.as_dict())
        xml_hash = _hash(_xml(request))
        attempt = secrets.token_hex(24)
        try:
            with closing(self._get_db()) as db:
                row = db.execute("SELECT * FROM tyresystem_order_attempts WHERE order_id=?", (order_id,)).fetchone()
            if row is not None:
                return self._existing(dict(row), request, identity)
            self.client.check_authorized(request)
            stamp = self.client.now().isoformat()
            with closing(self._get_db()) as db:
                claimed = db.execute("""INSERT INTO tyresystem_order_attempts
                    (order_id,attempt_id,reference,configured_mode,account_fingerprint,request_hash,request_json,
                     request_xml_sha256,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'claimed',?,?)
                    ON CONFLICT DO NOTHING RETURNING attempt_id""",
                    (order_id,attempt,request.reference,self.client.configured_mode,self.client.account_fingerprint,
                     identity,canonical,xml_hash,stamp,stamp)).fetchone()
                db.commit()  # Mandatory durable boundary before any POST.
                row = db.execute("SELECT * FROM tyresystem_order_attempts WHERE order_id=?", (order_id,)).fetchone()
            if claimed is None:
                if row is None:
                    raise TyreSystemOrderError("attempt_unknown")
                return self._existing(dict(row), request, identity)
        except TyreSystemOrderError:
            raise
        except Exception:
            raise TyreSystemOrderError("attempt_unknown") from None
        try:
            receipt = self.client.submit(request)
            receipt = _checked_receipt(receipt, request, self.client.configured_mode, self.client.account_fingerprint, "submitted_request")
            receipt["receiptbinding_source"] = "persisted_request"
            encoded = _canonical(receipt)
            with closing(self._get_db()) as db:
                changed = db.execute("UPDATE tyresystem_order_attempts SET state='acknowledged',receipt_json=?,receipt_hash=?,updated_at=? "
                    "WHERE order_id=? AND attempt_id=? AND request_hash=? AND request_json=? AND request_xml_sha256=? "
                    "AND configured_mode=? AND account_fingerprint=? AND state='claimed'",
                    (encoded,_hash(encoded),self.client.now().isoformat(),order_id,attempt,identity,canonical,xml_hash,
                     self.client.configured_mode,self.client.account_fingerprint)).rowcount
                if changed != 1:
                    raise TyreSystemOrderError("attempt_unknown")
                db.commit()  # Do not return an accepted ACK before storage commits.
            return receipt
        except TyreSystemOrderError as error:
            state = "rejected" if error.outcome == "rejected" else "blocked" if error.outcome == "not_sent" else "unknown"
            self._mark_failure(order_id, attempt, identity, state, error.code)
            raise
        except Exception:
            self._mark_failure(order_id, attempt, identity, "unknown", "unknown")
            raise TyreSystemOrderError("attempt_unknown") from None
        except BaseException:
            self._mark_failure(order_id, attempt, identity, "unknown", "unknown")
            raise
