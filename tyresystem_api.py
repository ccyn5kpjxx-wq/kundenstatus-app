"""Internal read-only client for TYREsystem's documented article Inquiry.

No endpoint is called on import/construction. Known article IDs only: this is
neither catalogue search nor an order/reservation adapter or public price feed.
Price tax/currency/freight/validity and server test/live mode are not specified
by the public response schema and are deliberately not inferred here.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import re
import time
import unicodedata
from xml.etree import ElementTree

import requests


SOURCE_DOCUMENT = "https://www.tyresystem.de/static/Anleitung/API-REST-Architektur.pdf"
INQUIRY_ORIGIN = "https://api.tyresystem.de"
MAX_RESPONSE_BYTES = 65536
MAX_INQUIRY_AMOUNT = 1000  # Local client guard, not a documented supplier limit.
TIMEOUT = (3.05, 8.0)
MAX_INQUIRY_SECONDS = 15.0
_FIELDS = {"idArticle", "ean", "price", "amount", "description", "errorCode", "stock"}
_MESSAGES = {
    "configuration": "Der eigene TYREsystem-API-Zugang ist nicht vollständig eingerichtet.",
    "input": "Artikelnummer und Menge müssen gültige positive ganze Zahlen sein.",
    "not_authorized": "Die TYREsystem-Abfrage ist für diesen Aufruf nicht freigegeben.",
    "authentication": "Der TYREsystem-API-Zugang konnte nicht bestätigt werden.",
    "rate_limited": "Die TYREsystem-Abfrage ist derzeit begrenzt. Bitte später erneut versuchen.",
    "unavailable": "Die TYREsystem-Abfrage ist derzeit nicht verfügbar.",
    "invalid_response": "Die TYREsystem-Antwort ist unvollständig oder nicht eindeutig prüfbar.",
    "provider_error": "TYREsystem hat die Artikelabfrage nicht erfolgreich bestätigt.",
}


class TyreSystemAPIError(ValueError):
    """A bounded public-safe error code/message, never provider text or secrets."""
    def __init__(self, code):
        self.code = code if code in _MESSAGES else "unavailable"
        super().__init__(_MESSAGES[self.code])


@dataclass(frozen=True)
class ArticleInquiry:
    article_id: str
    ean: str
    amount: int
    price: Decimal
    stock: int
    description: str
    retrieved_at: datetime  # Local retrieval time, not a supplier validity promise.
    configured_mode: str  # Operator declaration only; not a provider mode flag.


def _integer(value, maximum, *, allow_zero=False, error="invalid_response"):
    if type(value) is int:
        number = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]{1,18}", value, re.ASCII):
        number = int(value)
    else:
        raise TyreSystemAPIError(error)
    if not (0 if allow_zero else 1) <= number <= maximum:
        raise TyreSystemAPIError(error)
    return number


def _price(value):
    if type(value) is int:
        value = str(value)
    elif isinstance(value, Decimal):
        value = format(value, "f")
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,7}(?:\.[0-9]{1,4})?", value, re.ASCII):
        raise TyreSystemAPIError("invalid_response")
    result = Decimal(value)
    if not 0 < result <= Decimal("1000000"):
        raise TyreSystemAPIError("invalid_response")
    return result


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate response key")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("Non-finite JSON value")


def _parse_body(raw, content_type):
    """Read either example format without entities, ambiguous keys or coercion."""
    try:
        text = raw.decode("utf-8-sig")
        if content_type in {"application/json", "text/json"}:
            data = json.loads(text, object_pairs_hook=_unique_object,
                              parse_float=Decimal, parse_constant=_invalid_constant)
            if not isinstance(data, Mapping) or not isinstance(data.get("inquiryResponse"), Mapping):
                raise ValueError()
            result = data["inquiryResponse"].get("offerData")
        elif content_type in {"application/xml", "text/xml"}:
            if re.search(r"<!\s*(?:DOCTYPE|ENTITY)\b", text, re.I):
                raise ValueError()
            encoding = re.search(r"<\?xml\b[^>]*\bencoding\s*=\s*['\"]([^'\"]+)['\"]", text, re.I)
            if encoding and encoding[1].lower() not in {"utf-8", "utf8"}:
                raise ValueError()
            root = ElementTree.fromstring(text)
            if root.tag != "inquiryResponse" or root.attrib or len(root) != 1:
                raise ValueError()
            offer = root[0]
            if offer.tag != "offerData" or offer.attrib:
                raise ValueError()
            pairs = []
            for child in offer:
                if child.attrib or len(child):
                    raise ValueError()
                pairs.append((child.tag, child.text or ""))
            result = _unique_object(pairs)
        else:
            raise ValueError()
        if not isinstance(result, Mapping) or not _FIELDS <= result.keys():
            raise ValueError()
        return result
    except Exception:
        raise TyreSystemAPIError("invalid_response") from None


class TyreSystemInquiryClient:
    """Dedicated API credentials and fixed HTTPS GET; no automatic retries.

    ``authorize(article_id, amount) is True`` must independently permit the
    explicit read. Merely providing configuration does not enable a request.
    ``http`` is an optional trusted test transport with a requests-like get().
    """
    def __init__(self, config, *, authorize=None, http=None, clock=None, monotonic=None):
        if not isinstance(config, Mapping):
            raise TyreSystemAPIError("configuration")
        username, password, mode = (config.get(key) for key in (
            "TYRESYSTEM_API_USERNAME", "TYRESYSTEM_API_PASSWORD", "TYRESYSTEM_API_MODE"))
        if (not isinstance(username, str) or not 1 <= len(username) <= 200
                or ":" in username or username != username.strip()
                or not isinstance(password, str) or not 1 <= len(password) <= 500
                or any(ord(char) < 33 or ord(char) > 126 for char in username + password)
                or mode not in {"test", "live"}):
            raise TyreSystemAPIError("configuration")
        self._username, self._password, self.configured_mode = username, password, mode
        self._authorize, self._http = authorize, http
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic or time.monotonic

    @classmethod
    def from_env(cls, *, environment=None, **kwargs):
        values = os.environ if environment is None else environment
        return cls({key: values.get(key) for key in (
            "TYRESYSTEM_API_USERNAME", "TYRESYSTEM_API_PASSWORD", "TYRESYSTEM_API_MODE")}, **kwargs)

    def inquire(self, article_id, amount):
        identifier = str(_integer(article_id, 999999999999999999, error="input"))
        quantity = _integer(amount, MAX_INQUIRY_AMOUNT, error="input")
        try:
            permitted = callable(self._authorize) and self._authorize(identifier, quantity) is True
        except Exception:
            permitted = False
        if not permitted:
            raise TyreSystemAPIError("not_authorized")
        url = INQUIRY_ORIGIN + "/Rest/Inquiry/" + identifier + "/" + str(quantity)
        response = None
        try:
            deadline = self._monotonic() + MAX_INQUIRY_SECONDS
            if self._http is None:
                self._http = requests.Session()
                self._http.trust_env = False
                self._http.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            response = self._http.get(url, auth=(self._username, self._password),
                headers={"Accept": "application/json, application/xml;q=0.9"},
                timeout=TIMEOUT, allow_redirects=False, stream=True, verify=True)
            if self._monotonic() >= deadline:
                raise TyreSystemAPIError("unavailable")
            if response.status_code in (401, 403):
                raise TyreSystemAPIError("authentication")
            if response.status_code == 429:
                raise TyreSystemAPIError("rate_limited")
            if response.status_code != 200:
                raise TyreSystemAPIError("unavailable")
            if getattr(response, "url", url) != url:
                raise TyreSystemAPIError("invalid_response")
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            length = response.headers.get("Content-Length")
            if length is not None and (not isinstance(length, str) or not re.fullmatch(r"[0-9]{1,9}", length)
                                       or int(length) > MAX_RESPONSE_BYTES):
                raise TyreSystemAPIError("invalid_response")
            raw = bytearray()
            for chunk in response.iter_content(chunk_size=4096):
                if self._monotonic() >= deadline:
                    raise TyreSystemAPIError("unavailable")
                raw.extend(chunk)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise TyreSystemAPIError("invalid_response")
            if self._monotonic() >= deadline:
                raise TyreSystemAPIError("unavailable")
            data = _parse_body(bytes(raw), content_type)
            error_code = data["errorCode"]
            if not (type(error_code) is int and error_code == 0
                    or isinstance(error_code, str) and error_code == "0"):
                raise TyreSystemAPIError("provider_error")
            returned_id = str(_integer(data["idArticle"], 999999999999999999))
            returned_amount = _integer(data["amount"], MAX_INQUIRY_AMOUNT)
            if returned_id != identifier or returned_amount != quantity:
                raise TyreSystemAPIError("invalid_response")
            ean, description = data["ean"], data["description"]
            if (not isinstance(ean, str) or not re.fullmatch(r"[0-9]{13}", ean, re.ASCII)
                    or not isinstance(description, str) or not 1 <= len(description) <= 500
                    or not description.strip()
                    or any(unicodedata.category(char).startswith("C") for char in description)):
                raise TyreSystemAPIError("invalid_response")
            retrieved_at = self._clock()
            if not isinstance(retrieved_at, datetime) or retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
                raise TyreSystemAPIError("invalid_response")
            if self._monotonic() >= deadline:
                raise TyreSystemAPIError("unavailable")
            return ArticleInquiry(identifier, ean, returned_amount, _price(data["price"]),
                _integer(data["stock"], 1000000000, allow_zero=True), description.strip(),
                retrieved_at.astimezone(timezone.utc), self.configured_mode)
        except TyreSystemAPIError:
            raise
        except Exception:
            raise TyreSystemAPIError("unavailable") from None
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    pass
