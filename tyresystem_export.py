"""Explicit server-side GET for TYREsystem's private CSV export.

The URL is a credential and must stay in a server secret store. Import,
construction and environment loading do no IO. This client never writes files,
publishes prices, orders or reserves stock. Its result remains internal-only.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import os
import re
import time
from urllib.parse import parse_qsl, quote, urlsplit

import requests

from tyresystem_feed import FeedError, MAX_BYTES, parse_feed


EXPORT_ORIGIN = "https://www.tyresystem.de"
EXPORT_PATH = "/Services/TyresystemExport"
MAX_EXPORT_BYTES = MAX_BYTES
TIMEOUT = (3.05, 10.0)
MAX_EXPORT_SECONDS = 45.0
_MESSAGES = {
    "configuration": "Der eigene TYREsystem-Preisexport ist nicht gültig eingerichtet.",
    "not_authorized": "Der TYREsystem-Exportabruf ist für diesen Aufruf nicht freigegeben.",
    "authentication": "Der TYREsystem-Preisexport konnte nicht authentifiziert werden.",
    "rate_limited": "Der TYREsystem-Exportabruf ist derzeit begrenzt.",
    "invalid_response": "Der TYREsystem-Export ist nicht vollständig prüfbar.",
    "unavailable": "Der TYREsystem-Preisexport ist derzeit nicht verfügbar.",
}


class TyreSystemExportError(ValueError):
    """Fixed code/message only; no URL, token, provider text or response body."""
    def __init__(self, code):
        self.code = code if code in _MESSAGES else "unavailable"
        super().__init__(_MESSAGES[self.code])


def _export_url(config):
    """Return a canonical URL for exactly the observed read-only endpoint."""
    if not isinstance(config, Mapping):
        raise TyreSystemExportError("configuration")
    value = config.get("TYRESYSTEM_EXPORT_URL")
    if (not isinstance(value, str) or not 1 <= len(value) <= 2048
            or any(ord(char) < 33 or ord(char) > 126 for char in value)):
        raise TyreSystemExportError("configuration")
    try:
        parts = urlsplit(value)
        if (parts.scheme != "https" or parts.hostname != "www.tyresystem.de"
                or parts.port not in (None, 443) or parts.username is not None
                or parts.password is not None or parts.path != EXPORT_PATH
                or parts.fragment):
            raise ValueError()
        pairs = parse_qsl(parts.query, keep_blank_values=True, strict_parsing=False,
                          max_num_fields=3)
        if (len(pairs) != 2 or len({key for key, _ in pairs}) != 2
                or set(key for key, _ in pairs) != {"index", "login"}):
            raise ValueError()
        values = dict(pairs)
        token = values["login"]
        if (values["index"] != "" or not 1 <= len(token) <= 512
                or any(ord(char) < 33 or ord(char) > 126 for char in token)):
            raise ValueError()
        return EXPORT_ORIGIN + EXPORT_PATH + "?index&login=" + quote(token, safe="")
    except Exception:
        raise TyreSystemExportError("configuration") from None


class TyreSystemExportClient:
    """An explicit internal fetch with a per-call authorization callback.

    ``authorize() is True`` is required every time. ``http`` is a trusted
    requests-like test transport; production uses a new isolated session for
    each fetch, without ambient proxy, cookies, authentication or retry state.
    No external response or exception text is exposed through error messages.
    """
    __slots__ = ("_url", "_authorize", "_http", "_clock", "_monotonic")

    def __init__(self, config, *, authorize=None, http=None, clock=None, monotonic=None):
        self._url = _export_url(config)
        self._authorize = authorize
        self._http = http
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic = monotonic or time.monotonic

    def __repr__(self):
        return "<TyreSystemExportClient private_url=redacted>"

    @classmethod
    def from_env(cls, *, environment=None, **kwargs):
        values = os.environ if environment is None else environment
        if not isinstance(values, Mapping):
            raise TyreSystemExportError("configuration")
        return cls({"TYRESYSTEM_EXPORT_URL": values.get("TYRESYSTEM_EXPORT_URL")}, **kwargs)

    def fetch(self):
        try:
            permitted = callable(self._authorize) and self._authorize() is True
        except Exception:
            permitted = False
        if not permitted:
            raise TyreSystemExportError("not_authorized")

        response = None
        owned_http = None
        try:
            deadline = self._monotonic() + MAX_EXPORT_SECONDS
            http = self._http
            if http is None:
                owned_http = http = requests.Session()
                http.trust_env = False
                http.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            response = http.get(self._url,
                headers={"Accept": "text/csv, application/csv;q=0.9, application/octet-stream;q=0.8",
                         "Accept-Encoding": "identity"},
                timeout=TIMEOUT, allow_redirects=False, stream=True, verify=True)
            if self._monotonic() >= deadline:
                raise TyreSystemExportError("unavailable")
            if response.status_code in (401, 403):
                raise TyreSystemExportError("authentication")
            if response.status_code == 429:
                raise TyreSystemExportError("rate_limited")
            if response.status_code != 200:
                raise TyreSystemExportError("unavailable")
            if getattr(response, "url", self._url) != self._url:
                raise TyreSystemExportError("invalid_response")
            length = response.headers.get("Content-Length")
            if length is not None and (not isinstance(length, str)
                    or not re.fullmatch(r"[0-9]{1,10}", length)
                    or int(length) > MAX_EXPORT_BYTES):
                raise TyreSystemExportError("invalid_response")
            raw = bytearray()
            for chunk in response.iter_content(chunk_size=65536):
                if self._monotonic() >= deadline:
                    raise TyreSystemExportError("unavailable")
                if not isinstance(chunk, bytes) or len(raw) + len(chunk) > MAX_EXPORT_BYTES:
                    raise TyreSystemExportError("invalid_response")
                raw.extend(chunk)
            if self._monotonic() >= deadline:
                raise TyreSystemExportError("unavailable")
            received_at = self._clock()
            result = parse_feed(bytes(raw), abgerufen_am=received_at)
            if self._monotonic() >= deadline:
                raise TyreSystemExportError("unavailable")
            return result
        except TyreSystemExportError:
            raise
        except FeedError:
            raise TyreSystemExportError("invalid_response") from None
        except Exception:
            raise TyreSystemExportError("unavailable") from None
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
