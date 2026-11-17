"""Explicit, bounded ALZURA product reads; no order or distributor capability.

Plan-only is the CLI default and does not open a token file or HTTP transport.
Actual reads require --run --reads-approved, area-specific existing tokens and
the exact endpoint/parameter allowlist copied from the inspected V1.3 specs.
This module never creates tokens, changes scopes, books packages or renders EK.
"""

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import socket
import ssl
import time
from urllib.parse import urlencode
from uuid import uuid4

import requests

WORK = Path(__file__).resolve().parent
PRIVATE = WORK / "alzura-catalog-private"
ORIGIN = "https://tyre24.alzura.com"
BASES = {"tyres": "/de/de/rest/V13/tyres", "alloys": "/de/de/rest/v13/alloys",
         "products": "/de/de/rest/v13/products", "wearparts": "/de/de/rest/V13/wearparts"}
MAX_CALLS = 30
MAX_MONTHLY_CALLS = 10000
MIN_INTERVAL = 2.1
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_RUN_RESPONSE_BYTES = 32 * 1024 * 1024
TIMEOUT = (3.05, 8.0)

# type + required. No generic path, arbitrary parameter, order (including GET),
# paid distributorList, distributorProfile, image generation or write operation.
OPERATIONS = {
    "tyres": {
        "filter": {},
        "search": {"search": ("string", True), "limit": ("integer", False),
            "filter": ("filter", False), "sorter": ("string", False), "offset": ("integer", False),
            "minStock": ("integer", False), "postcode": ("string", False), "cheapestExpressPrice": ("boolean", False)},
        "details": {"idSolr": ("string", True), "postcode": ("string", False), "minStock": ("integer", False)},
    },
    "alloys": {
        "filter": {"carID": ("integer", True)},
        "search": {"carID": ("integer", True), "limit": ("integer", False),
            "language": ("string", False), "country": ("string", False),
            "pricelistKey": ("integer", True), "threedCarID": ("integer", False)},
        "details": {"alloyRimFrontID": ("integer", True), "alloyRimRearID": ("integer", False),
            "carID": ("integer", True), "threedCarID": ("integer", False)},
        "profiSearch": {"search": ("string", False), "width": ("number", False),
            "diameter": ("integer", False), "et": ("integer", False), "holes": ("integer", False),
            "holeCircle": ("integer", False), "centerHole": ("number", False),
            "manufacturer": ("string", False), "alloyType": ("string", False),
            "minStock": ("integer", False), "ownStock": ("boolean", False), "winterfit": ("integer", False)},
        "carBrands": {},
        "carModels": {"brandID": ("integer", True)},
        "carTypes": {"brandID": ("integer", True), "modelID": ("integer", True)},
    },
    "products": {
        "areas": {"areaIds": ("integer_array", False)},
        "categories": {"productAreaId": ("integer", True), "onlyFeatured": ("boolean", False),
            "parentNodeId": ("integer", False), "hideEmpty": ("boolean", False)},
        "items": {"productAreaId": ("integer", True), "parentNodeId": ("integer", False),
            "search": ("string", False), "itemId": ("integer", False), "manufacturer": ("string_array", False),
            "filterByExpress": ("integer", False), "attributeStrings": ("string_array", False),
            "wholesalerIds": ("integer_array", False), "page": ("integer", False),
            "limit": ("integer", False), "sort": ("integer", False), "pricelistId": ("integer", False)},
    },
    "wearparts": {
        "manufacturers": {"linkingTargetType": ("string", False)},
        "modelSeries": {"manufacturerId": ("integer", True), "linkingTargetType": ("string", False)},
        "vehicles": {"manufacturerId": ("integer", True), "modelId": ("integer", True),
            "linkingTargetType": ("string", False)},
        "vehicleByKey": {"keySystemType": ("integer", True), "keySystemNumber": ("string", True)},
        "category": {"carId": ("integer", True), "parentNodeId": ("integer", False),
            "linkingTargetType": ("string", False)},
        "articles": {"assemblyGroupNodeId": ("integer", True), "carId": ("integer", True),
            "manufacturerId": ("integer", True), "modelId": ("integer", True),
            "linkingTargetType": ("string", False), "filter": ("string_array_multi", False),
            "page": ("integer", False), "showPerPage": ("integer", False), "sort": ("integer", False),
            "brandIds": ("integer_array_multi", False)},
        "articles/DIRECTSEARCH": {"articleNumber": ("string", True), "directSearch": ("integer", True),
            "searchExact": ("integer", False), "page": ("integer", False), "showPerPage": ("integer", False),
            "filterByWholesalerIds": ("integer_array_multi", False), "filterByExpress": ("integer", False),
            "sort": ("integer", False), "brandIds": ("integer_array_multi", False), "category": ("integer", False),
            "attributes": ("string_array_multi", False), "loadVehicleAttributes": ("integer", False)},
    },
}


class ReadError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ReadError("clock_invalid")
    return value.astimezone(timezone.utc)


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ReadError("duplicate_json_key")
        value[key] = item
    return value


def _json(raw):
    def invalid(_):
        raise ReadError("invalid_json_number")
    try:
        return json.loads(raw.decode("utf-8-sig"), object_pairs_hook=_unique,
                          parse_float=Decimal, parse_constant=invalid)
    except Exception:
        raise ReadError("invalid_json_response") from None


def _private_path(path):
    value = Path(path)
    resolved = value.resolve()
    if (value.is_symlink() or any(part.casefold() in {"static", "public", "templates", "outputs", "wwwroot"}
                                  for part in resolved.parts)):
        raise ReadError("private_path_required")
    return resolved


def _atomic_json(path, value):
    path = _private_path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Keep same-directory atomic replacement without repeating a long target
    # name. Windows may accept the metadata target but reject target+UUID.
    temporary = path.with_name("." + uuid4().hex + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def load_tokens(path):
    """Read only the explicitly supplied private file; never search credentials.

    Schema: {"tokens": {"tyres": "existing-token", "alloys": "existing-token",
                        "products": "existing-token", "wearparts": "existing-token"}}.
    Missing scopes stay missing. No B2B token or token from another area is used
    as a fallback. This function does not verify or alter provider permissions.
    """
    try:
        source = _private_path(path)
        with source.open("rb") as stream:
            raw = stream.read(32769)
        if len(raw) > 32768:
            raise ValueError()
        value = _json(raw)
        tokens = value.get("tokens") if isinstance(value, dict) else None
        if not isinstance(tokens, dict) or not tokens or not set(tokens) <= set(BASES):
            raise ValueError()
        return validate_tokens(tokens)
    except Exception:
        raise ReadError("token_file_invalid") from None


def validate_tokens(tokens):
    if not isinstance(tokens, dict) or not tokens or not set(tokens) <= set(BASES):
        raise ReadError("token_configuration_invalid")
    result = {}
    for area, value in tokens.items():
        if (not isinstance(value, str) or not 1 <= len(value) <= 2000
                or any(ord(char) < 33 or ord(char) > 126 for char in value)):
            raise ReadError("token_configuration_invalid")
        result[area] = value
    return result


def _string(value, maximum=200):
    if (not isinstance(value, str) or not 1 <= len(value) <= maximum or value != value.strip()
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ReadError("parameter_invalid")
    return value


def _integer(value, name, area=None):
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not re.fullmatch(r"-?[0-9]{1,18}", str(value)):
        raise ReadError("parameter_invalid")
    number = int(value)
    minimum, maximum = (0, 1000000) if name == "offset" else (0, 1000) if name == "minStock" else (-1000, 1000) if name == "et" else (1, 999999999999999999)
    if name == "limit": minimum, maximum = 1, 100
    if name == "alloyRimRearID": minimum = 0
    if name == "diameter": minimum, maximum = 8, 30
    if name == "winterfit": minimum, maximum = 0, 1
    if name == "pricelistKey" and number not in {1, 2, 3, 4, 5, 99}:
        raise ReadError("parameter_invalid")
    if area in {"products", "wearparts"}:
        if name in {"page", "parentNodeId", "sort", "category"}: minimum, maximum = 0, 1000000
        if name == "showPerPage": minimum, maximum = 1, 25
        if name in {"filterByExpress", "searchExact", "loadVehicleAttributes"}: minimum, maximum = 0, 1
        if name == "directSearch": minimum, maximum = 1, 1
        # The local shop serves Germany only. Other country's licence/plate
        # lookups are not part of this adapter's permitted data scope.
        if name == "keySystemType": minimum, maximum = 20, 20
    if not minimum <= number <= maximum:
        raise ReadError("parameter_invalid")
    return str(number)


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ReadError("parameter_invalid")
    text = str(value)
    if not re.fullmatch(r"[0-9]{1,6}(?:\.[0-9]{1,4})?", text) or not Decimal("0") < Decimal(text) <= Decimal("10000"):
        raise ReadError("parameter_invalid")
    return text


def _filters(value):
    if not isinstance(value, dict) or not 1 <= len(value) <= 16:
        raise ReadError("parameter_invalid")
    fields = []
    for identifier, entry in value.items():
        if (not re.fullmatch(r"[0-9]{1,6}", str(identifier)) or not isinstance(entry, dict)
                or set(entry) != {"name", "valueList", "multipleChoice"}
                or not isinstance(entry["valueList"], list) or not 1 <= len(entry["valueList"]) <= 16
                or type(entry["multipleChoice"]) is not bool):
            raise ReadError("parameter_invalid")
        fields.append((f"filter[{identifier}][name]", _string(entry["name"], 80)))
        fields.extend((f"filter[{identifier}][valueList][]", _string(item, 80)) for item in entry["valueList"])
        fields.append((f"filter[{identifier}][multipleChoice]", "1" if entry["multipleChoice"] else "0"))
    return fields


def _array(value, kind, name, area):
    if not isinstance(value, list) or not 1 <= len(value) <= (3 if name == "brandIds" else 16):
        raise ReadError("parameter_invalid")
    values = []
    for item in value:
        if kind.startswith("integer_"):
            values.append(_integer(item, name, area))
        else:
            text = _string(item)
            # Values are opaque provider identifiers or hashes; a comma would
            # change the meaning of a Swagger-default CSV array.
            if "," in text: raise ReadError("parameter_invalid")
            values.append(text)
    if len(set(values)) != len(values): raise ReadError("parameter_invalid")
    # Wearparts explicitly documents filter[]=.../brandIds[]=... in the
    # parameter descriptions. Products has no collectionFormat override, so
    # its arrays use Swagger 2.0's default CSV encoding.
    return [(name + "[]", item) for item in values] if kind.endswith("_multi") else [(name, ",".join(values))]


def build_url(area, operation, params=None):
    if not isinstance(area, str) or not isinstance(operation, str) or area not in OPERATIONS or operation not in OPERATIONS[area]:
        raise ReadError("operation_not_allowed")
    params = {} if params is None else params
    schema = OPERATIONS[area][operation]
    if not isinstance(params, dict) or not set(params) <= set(schema):
        raise ReadError("parameter_not_allowed")
    if any(required and name not in params for name, (_, required) in schema.items()):
        raise ReadError("required_parameter_missing")
    if operation == "profiSearch" and not set(params) - {"minStock", "ownStock", "winterfit"}:
        raise ReadError("unbounded_profisearch_not_allowed")
    if area == "products" and operation == "items" and not set(params) & {"search", "itemId", "parentNodeId"}:
        raise ReadError("required_parameter_missing")
    query = []
    for name in sorted(params):
        value, kind = params[name], schema[name][0]
        if kind == "filter": query.extend(_filters(value)); continue
        if "_array" in kind: query.extend(_array(value, kind, name, area)); continue
        if kind == "integer": encoded = _integer(value, name, area)
        elif kind == "number": encoded = _number(value)
        elif kind == "boolean":
            if type(value) is not bool: raise ReadError("parameter_invalid")
            encoded = "true" if value else "false"
        else:
            encoded = _string(value)
            if name == "postcode" and not re.fullmatch(r"[0-9]{5}", encoded): raise ReadError("parameter_invalid")
            if name == "country" and encoded != "de": raise ReadError("country_outside_de_not_allowed")
            if area == "wearparts" and name == "linkingTargetType" and encoded != "P": raise ReadError("parameter_invalid")
            if area == "wearparts" and name == "keySystemNumber" and not re.fullmatch(r"[0-9]{4}[A-Z0-9]{3,9}", encoded): raise ReadError("parameter_invalid")
        query.append((name, encoded))
    url = ORIGIN + BASES[area] + "/" + operation
    if query: url += "?" + urlencode(query)
    if len(url) > 4096: raise ReadError("request_too_long")
    return url


@contextmanager
def _ledger_lock(path):
    path = _private_path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = path.with_suffix(path.suffix + ".lock")
    if lock.is_symlink(): raise ReadError("budget_ledger_invalid")
    stream = None
    try:
        stream = lock.open("a+b")
        if stream.seek(0, os.SEEK_END) == 0:
            stream.write(b"\0"); stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    except ReadError:
        raise
    except Exception:
        raise ReadError("budget_ledger_unavailable") from None
    finally:
        if stream is not None:
            stream.close()


class BudgetLedger:
    def __init__(self, path, *, monthly_limit=MAX_MONTHLY_CALLS, run_limit=MAX_CALLS):
        if type(monthly_limit) is not int or not 1 <= monthly_limit <= MAX_MONTHLY_CALLS or type(run_limit) is not int or not 1 <= run_limit <= MAX_CALLS:
            raise ReadError("budget_configuration_invalid")
        self.path = _private_path(path)
        self.monthly_limit, self.run_limit = monthly_limit, run_limit

    def reserve(self, area, operation, run_id, now):
        now = _utc(now)
        with _ledger_lock(self.path):
            if self.path.exists():
                try:
                    raw = self.path.read_bytes()
                    if len(raw) > 8 * 1024 * 1024: raise ValueError()
                    data = _json(raw)
                    if not isinstance(data, dict) or data.get("schema_version") != 1 or not isinstance(data.get("attempts"), list): raise ValueError()
                    attempts = data["attempts"]
                    if len(attempts) > 100000: raise ValueError()
                    parsed = []
                    for entry in attempts:
                        if not isinstance(entry, dict) or set(entry) != {"at", "area", "operation", "run_id"}: raise ValueError()
                        stamp = _utc(datetime.fromisoformat(entry["at"]))
                        if entry["area"] not in OPERATIONS or entry["operation"] not in OPERATIONS[entry["area"]] or not isinstance(entry["run_id"], str): raise ValueError()
                        parsed.append((entry, stamp))
                except Exception:
                    raise ReadError("budget_ledger_invalid") from None
            else:
                attempts, parsed = [], []
            if any(stamp > now + timedelta(seconds=1) for _, stamp in parsed): raise ReadError("budget_ledger_clock_conflict")
            if sum(entry["run_id"] == run_id for entry, _ in parsed) >= self.run_limit: raise ReadError("run_call_budget_exhausted")
            month = now.strftime("%Y-%m")
            if sum(stamp.strftime("%Y-%m") == month for _, stamp in parsed) >= self.monthly_limit: raise ReadError("local_monthly_budget_exhausted")
            if sum(stamp > now - timedelta(seconds=60) for _, stamp in parsed) >= 30: raise ReadError("local_rate_limit")
            if parsed and (now - max(stamp for _, stamp in parsed)).total_seconds() < MIN_INTERVAL: raise ReadError("local_rate_spacing")
            attempts.append({"at": now.isoformat(), "area": area, "operation": operation, "run_id": run_id})
            _atomic_json(self.path, {"schema_version": 1, "attempts": attempts,
                "usage_outside_this_client_unverified": True, "monthly_counter_calendar": "UTC"})


@dataclass(repr=False)
class ReadResult:
    summary: dict
    data: object = None

    def __repr__(self):
        return f"<ALZURAReadResult ok={self.summary.get('ok', False)} private_body=redacted>"


def _row_count(data):
    if isinstance(data, list): return len(data)
    if isinstance(data, dict):
        for key in ("tyreList", "alloyRimsList", "result"):
            if isinstance(data.get(key), list): return len(data[key])
        return 1
    return 0


def _failure_code(error, phase):
    """Classify failures without retaining exception text, URLs or headers.

    requests wraps DNS/TLS errors in several levels of exception objects. Only
    inspect bounded exception objects, never stringify them or their arguments.
    The returned values are fixed local codes safe for diagnostic metadata.
    """
    pending, seen, nested = [error], set(), []
    while pending and len(nested) < 16:
        current = pending.pop()
        if not isinstance(current, BaseException) or id(current) in seen:
            continue
        seen.add(id(current)); nested.append(current)
        pending.extend(value for value in (current.__cause__, current.__context__,
                       getattr(current, "reason", None)) if isinstance(value, BaseException))
        pending.extend(value for value in current.args if isinstance(value, BaseException))
    if phase in {"private_body_storage", "private_metadata_storage"}:
        return "private_storage_error"
    if any(isinstance(item, ssl.SSLCertVerificationError) for item in nested):
        return "tls_certificate_rejected"
    if isinstance(error, requests.exceptions.SSLError) or any(isinstance(item, ssl.SSLError) for item in nested):
        return "tls_error"
    if any(isinstance(item, socket.gaierror) for item in nested):
        return "name_resolution_failed"
    if isinstance(error, requests.exceptions.ConnectTimeout):
        return "connection_timeout"
    if isinstance(error, requests.exceptions.ReadTimeout):
        return "response_read_timeout"
    if isinstance(error, requests.exceptions.Timeout) or any(isinstance(item, TimeoutError) for item in nested):
        return "transport_timeout"
    if isinstance(error, requests.exceptions.ProxyError):
        return "proxy_connection_failed"
    if any(isinstance(item, ConnectionRefusedError) for item in nested):
        return "connection_refused"
    if any(isinstance(item, ConnectionResetError) for item in nested):
        return "connection_reset"
    if isinstance(error, requests.exceptions.ConnectionError):
        return "connection_failed"
    if isinstance(error, (requests.exceptions.InvalidHeader, requests.exceptions.InvalidURL,
                          requests.exceptions.InvalidSchema, requests.exceptions.MissingSchema)):
        return "transport_configuration_invalid"
    if isinstance(error, requests.exceptions.RequestException):
        return "transport_request_failed"
    if isinstance(error, (TypeError, AttributeError)):
        return "unexpected_runtime_type_error"
    return "unexpected_local_error"


class ReadOnlyCatalogClient:
    def __init__(self, tokens, *, output_dir, ledger, authorize=None, http=None, clock=None, sleeper=None, run_id=None):
        self._tokens = validate_tokens(tokens)
        self.output_dir = _private_path(output_dir)
        self.ledger, self.authorize, self._http = ledger, authorize, http
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.sleeper = sleeper or time.sleep
        self.run_id = run_id or uuid4().hex
        self.attempts, self.response_bytes = 0, 0
        self._last_attempt = None
        self.stopped = False

    def __repr__(self):
        return f"<ALZURAReadOnlyCatalogClient attempts={self.attempts} tokens=redacted>"

    def read(self, area, operation, params=None):
        url = build_url(area, operation, params)
        if self.stopped: raise ReadError("client_stopped")
        if self.attempts >= min(MAX_CALLS, self.ledger.run_limit): raise ReadError("run_call_budget_exhausted")
        try:
            allowed = callable(self.authorize) and self.authorize(area, operation, params or {}) is True
        except Exception:
            allowed = False
        if not allowed: raise ReadError("read_not_authorized")
        if area not in self._tokens: raise ReadError("area_token_missing")
        # DIRECTSEARCH is a documented path component, not a private directory.
        operation_file = operation.replace("/", "_")
        next_body = self.output_dir / f"{self.attempts + 1:02d}-{area}-{operation_file}.body"
        if next_body.exists(): raise ReadError("private_output_already_exists")
        now = _utc(self.clock())
        if self._last_attempt is not None:
            delay = MIN_INTERVAL - (now - self._last_attempt).total_seconds()
            if delay > 0:
                self.sleeper(delay)
                now = _utc(self.clock())
        self.ledger.reserve(area, operation, self.run_id, now)
        self.attempts += 1; self._last_attempt = now
        response, owned = None, None
        summary = {"ok": False, "area": area, "operation": operation, "attempt": self.attempts,
                   "requested_at": now.isoformat(), "http_status": None, "rows": 0, "error": None,
                   "error_phase": None}
        body = b""
        phase = "transport_setup"
        try:
            transport = self._http
            if transport is None:
                owned = transport = requests.Session()
                transport.trust_env = False
                transport.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            phase = "transport_get"
            response = transport.get(url, headers={"X-AUTH-TOKEN": self._tokens[area], "Accept": "application/json", "Accept-Encoding": "identity"},
                timeout=TIMEOUT, verify=True, allow_redirects=False, stream=True)
            phase = "response_headers"
            summary["http_status"] = response.status_code
            if getattr(response, "url", url) != url: raise ReadError("response_url_changed")
            length = response.headers.get("Content-Length")
            if length is not None and (not re.fullmatch(r"[0-9]{1,10}", str(length)) or int(length) > MAX_RESPONSE_BYTES): raise ReadError("response_too_large")
            chunks = []
            phase = "response_stream"
            for chunk in response.iter_content(chunk_size=65536):
                if not isinstance(chunk, bytes): raise ReadError("invalid_response_stream")
                if len(body) + sum(map(len, chunks)) + len(chunk) > MAX_RESPONSE_BYTES or self.response_bytes + len(chunk) > MAX_RUN_RESPONSE_BYTES:
                    raise ReadError("response_byte_budget_exhausted")
                chunks.append(chunk); self.response_bytes += len(chunk)
            body = b"".join(chunks)
            phase = "private_body_storage"
            self.output_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            destination = self.output_dir / f"{self.attempts:02d}-{area}-{operation_file}.body"
            with destination.open("xb") as stream: stream.write(body)
            try: os.chmod(destination, 0o600)
            except OSError: pass
            summary.update(body_file=destination.name, body_sha256=sha256(body).hexdigest(), received_at=_utc(self.clock()).isoformat())
            phase = "response_validation"
            if 300 <= response.status_code < 400: raise ReadError("redirect_blocked")
            if response.status_code in {401, 403}: raise ReadError("authentication_or_scope_rejected")
            if response.status_code == 429: raise ReadError("provider_rate_limited")
            if response.status_code != 200: raise ReadError("provider_http_error")
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type not in {"application/json", "text/json"}: raise ReadError("response_not_json")
            data = _json(body)
            if not isinstance(data, (dict, list)): raise ReadError("response_shape_invalid")
            if isinstance(data, dict) and data.get("status") is False: raise ReadError("provider_rejected")
            summary.update(ok=True, rows=_row_count(data))
            phase = "private_metadata_storage"
            _atomic_json(self.output_dir / f"{self.attempts:02d}-{area}-{operation_file}.meta.json", summary)
            return ReadResult(summary, data)
        except ReadError as error:
            summary["ok"] = False
            summary["error"] = error.code
            summary["error_phase"] = phase
        except Exception as error:
            summary["ok"] = False
            summary["error"] = _failure_code(error, phase)
            summary["error_phase"] = phase
        finally:
            if response is not None:
                try: response.close()
                except Exception: pass
            if owned is not None:
                try: owned.close()
                except Exception: pass
        self.stopped = True  # No automatic retries or loops after any failure.
        try: _atomic_json(self.output_dir / f"{self.attempts:02d}-{area}-{operation_file}.meta.json", summary)
        except Exception: pass
        return ReadResult(summary)


def _valid_ean(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{13}", value): return False
    weighted = sum(int(char) * (3 if index % 2 == 0 else 1) for index, char in enumerate(reversed(value[:-1])))
    return (10 - weighted % 10) % 10 == int(value[-1])


def sample_plan(path, *, areas=("tyres", "alloys")):
    try:
        value = _json(Path(path).read_bytes())
        articles = value["articles"]
        if not isinstance(articles, list) or len(articles) != 20: raise ValueError()
        plan = []
        for article in articles:
            ean, kind = article["ean"], article["kind"]
            if not _valid_ean(ean): raise ValueError()
            if kind == "tyre":
                area, operation = "tyres", "search"
                params = {"search": "EAN" + ean, "limit": 5, "offset": 0, "minStock": 4}
                evidence = "documented_tyre_ean_search"
            elif kind in {"rim", "wheel"}:
                area, operation = "alloys", "profiSearch"
                d = article["dimensions"]
                params = {"width": d["width_inch"], "diameter": d["diameter_inch"], "et": d["offset_mm"],
                    "holes": d["bolt_count"], "holeCircle": d["bolt_circle_mm"], "centerHole": d["centre_bore_mm"],
                    "manufacturer": article["manufacturer"], "minStock": 4}
                evidence = "geometry_candidates_only_ean_not_documented_in_profisearch"
            else: raise ValueError()
            if area in areas:
                build_url(area, operation, params)
                plan.append({"area": area, "operation": operation, "params": params, "expected_ean": ean,
                    "sample_position": article["sample_position"], "identity_evidence": evidence})
        return plan
    except Exception:
        raise ReadError("sample_plan_invalid") from None


def validate_plan(value):
    plan = value.get("requests") if isinstance(value, dict) else value
    if not isinstance(plan, list) or not 1 <= len(plan) <= MAX_CALLS: raise ReadError("plan_invalid")
    for entry in plan:
        if (not isinstance(entry, dict) or not {"area", "operation"} <= set(entry)
                or not set(entry) <= {"area", "operation", "params", "expected_ean", "sample_position", "identity_evidence"}):
            raise ReadError("plan_invalid")
        build_url(entry["area"], entry["operation"], entry.get("params"))
    return plan


def _ean_values(data):
    if isinstance(data, list): return [value for entry in data for value in _ean_values(entry)]
    if isinstance(data, dict):
        return [value for key, value in data.items() if key in {"ean", "alloyRimFrontEAN", "alloyRimRearEAN"} and isinstance(value, str)] + [
            child for value in data.values() if isinstance(value, (dict, list)) for child in _ean_values(value)]
    return []


def _ean_matches(data, expected):
    values = _ean_values(data)
    return sum(value == expected for value in values) if expected and values else None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument("--sample", type=Path)
    choice.add_argument("--plan-file", type=Path)
    parser.add_argument("--areas", choices=("tyres", "alloys", "both"), default="both")
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--out-dir", type=Path, default=PRIVATE / ("run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:6]))
    parser.add_argument("--ledger", type=Path, default=PRIVATE / "budget-ledger.json")
    parser.add_argument("--max-calls", type=int, default=30)
    parser.add_argument("--monthly-budget", type=int, default=10000)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--reads-approved", action="store_true")
    args = parser.parse_args(argv)
    client = None
    try:
        areas = ("tyres", "alloys") if args.areas == "both" else (args.areas,)
        plan = sample_plan(args.sample, areas=areas) if args.sample else validate_plan(_json(args.plan_file.read_bytes()))
        plan = validate_plan(plan)
        if len(plan) > args.max_calls or not 1 <= args.max_calls <= MAX_CALLS: raise ReadError("run_call_budget_exhausted")
        output = _private_path(args.out_dir)
        _atomic_json(output / "plan.json", {"requests": plan, "business_api_called": False})
        if not args.run:
            print(json.dumps({"ok": True, "mode": "plan_only", "planned_requests": len(plan), "business_api_calls": 0, "token_file_read": False}))
            return 0
        if not args.reads_approved: raise ReadError("read_approval_missing")
        if args.token_file is None: raise ReadError("token_file_required")
        tokens = load_tokens(args.token_file)
        if not {entry["area"] for entry in plan} <= set(tokens): raise ReadError("area_token_missing")
        ledger = BudgetLedger(args.ledger, monthly_limit=args.monthly_budget, run_limit=args.max_calls)
        client = ReadOnlyCatalogClient(tokens, output_dir=output, ledger=ledger, authorize=lambda *_: True)
        results = []
        for entry in plan:
            result = client.read(entry["area"], entry["operation"], entry.get("params"))
            summary = dict(result.summary)
            summary["sample_position"] = entry.get("sample_position")
            summary["expected_ean_matches"] = _ean_matches(result.data, entry.get("expected_ean")) if result.summary["ok"] else None
            summary["identity_evidence"] = entry.get("identity_evidence")
            results.append(summary)
            _atomic_json(output / "results.json", {"results": results, "provider_calls": client.attempts,
                "scope": "private_product_reads_only", "orders_or_payments": False})
            if not result.summary["ok"]: break
        success = len(results) == len(plan) and all(result["ok"] for result in results)
        print(json.dumps({"ok": success, "mode": "read_only", "planned_requests": len(plan), "business_api_calls": client.attempts,
            "successful_reads": sum(result["ok"] for result in results), "rows": sum(result["rows"] for result in results)}))
        return 0 if success else 1
    except ReadError as error:
        print(json.dumps({"ok": False, "error": error.code, "business_api_calls": client.attempts if client is not None else 0}))
        return 2
    except Exception:
        print(json.dumps({"ok": False, "error": "local_read_preparation_failed", "business_api_calls": client.attempts if client is not None else 0}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
