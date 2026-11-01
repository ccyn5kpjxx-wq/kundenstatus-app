"""Order-bound Twilio Verify SMS adapter. Importing/configuring sends nothing.

The caller owns HTTP authentication, consent, order ownership and IP/budget limits.
An explicit runtime authorization callback is required before any provider call.
Only Verify's matching ``approved`` response can confirm a challenge. OTPs,
telephone numbers, credentials and provider error bodies are never persisted here.
"""
from contextlib import closing
import hashlib
import hmac
import json
import os
import re
import secrets
import time

import requests


VERIFY_ORIGIN = "https://verify.twilio.com"
CHALLENGE_TTL_SECONDS = 600
MAX_CHECKS = 5
_ACTIVE = "('starting','pending','checking','uncertain')"
_ERROR = "SMS-Prüfung derzeit nicht verfügbar. Bitte später erneut versuchen."
_E164 = re.compile(r"\+[1-9][0-9]{7,14}\Z", re.ASCII)
_REFERENCE = re.compile(r"[A-Za-z0-9_:.\-]{8,200}\Z", re.ASCII)
_SID = {prefix: re.compile(prefix + r"[0-9a-fA-F]{32}\Z", re.ASCII)
        for prefix in ("AC", "VA", "VE")}


class ReifenSMSUnavailable(ValueError):
    """Deliberately contains no provider response, PII or credential detail."""


def init_schema(db):
    """Call during the existing portal migration, not from a public request."""
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_sms_challenges (
        challenge_id TEXT PRIMARY KEY, reference_hash TEXT NOT NULL,
        phone_hash TEXT NOT NULL, service_hash TEXT NOT NULL,
        verification_sid TEXT UNIQUE, state TEXT NOT NULL,
        created_at DOUBLE PRECISION NOT NULL, expires_at DOUBLE PRECISION NOT NULL,
        updated_at DOUBLE PRECISION NOT NULL, checks INTEGER NOT NULL DEFAULT 0
    )""")
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_reifen_sms_active_reference "
               "ON reifen_sms_challenges(reference_hash) WHERE state IN " + _ACTIVE)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_reifen_sms_active_phone "
               "ON reifen_sms_challenges(phone_hash) WHERE state IN " + _ACTIVE)
    db.execute("CREATE INDEX IF NOT EXISTS idx_reifen_sms_reference_created "
               "ON reifen_sms_challenges(reference_hash,created_at)")


def _phone(value):
    if not isinstance(value, str) or not _E164.fullmatch(value):
        raise ReifenSMSUnavailable("Bitte eine gültige Telefonnummer im internationalen Format angeben.")
    return value


def _reference(value):
    if not isinstance(value, str) or not _REFERENCE.fullmatch(value):
        raise ReifenSMSUnavailable(_ERROR)
    return value


class TwilioVerifySMS:
    """Official Verify v2, with durable portal bindings and no automatic retries.

    ``authorize(operation, reference) is True`` is mandatory at runtime; a key or
    config boolean alone never enables SMS. ``get_db`` must return the existing
    SQLite/PostgreSQL portal wrapper, with schema already initialized.
    """
    demo_only = False

    def __init__(self, account_sid, auth_token, service_sid, get_db, *,
                 authorize=None, http=None, clock=time.time):
        if (not isinstance(account_sid, str) or not _SID["AC"].fullmatch(account_sid)
                or not isinstance(service_sid, str) or not _SID["VA"].fullmatch(service_sid)
                or not isinstance(auth_token, str) or not 16 <= len(auth_token) <= 256
                or auth_token != auth_token.strip()
                or any(ord(char) < 33 or ord(char) > 126 for char in auth_token)
                or not callable(get_db) or not callable(clock)):
            raise ReifenSMSUnavailable("SMS-Zugang ist nicht vollständig eingerichtet.")
        self._account, self._token, self._service = account_sid, auth_token, service_sid
        self._get_db, self._authorize, self._clock = get_db, authorize, clock
        self._base = VERIFY_ORIGIN + "/v2/Services/" + service_sid
        self._binding_key = hashlib.sha256(("reifen-sms-binding-v1:" + auth_token).encode()).digest()
        self._service_hash = self._digest("service", account_sid + ":" + service_sid)
        self._http = http

    @classmethod
    def from_env(cls, get_db, *, environment=None, **kwargs):
        values = os.environ if environment is None else environment
        return cls(values.get("REIFEN_TWILIO_ACCOUNT_SID", ""),
                   values.get("REIFEN_TWILIO_AUTH_TOKEN", ""),
                   values.get("REIFEN_TWILIO_VERIFY_SERVICE_SID", ""),
                   get_db, **kwargs)

    def _digest(self, domain, value):
        return hmac.new(self._binding_key, (domain + ":" + value).encode(), hashlib.sha256).hexdigest()

    def _identity(self, phone, reference, operation):
        phone, reference = _phone(phone), _reference(reference)
        if not callable(self._authorize) or self._authorize(operation, reference) is not True:
            raise ReifenSMSUnavailable(_ERROR)
        return phone, self._digest("reference", reference), self._digest("phone", phone)

    def _post(self, resource, fields):
        # No URL, redirect target, proxy or credentials are accepted from a client.
        if self._http is None:
            self._http = requests.Session()
            self._http.trust_env = False
        response = None
        try:
            response = self._http.post(self._base + "/" + resource,
                data=fields, auth=(self._account, self._token),
                headers={"Accept": "application/json"}, timeout=(3.05, 8),
                allow_redirects=False, stream=True)
            if response.status_code not in (200, 201):
                return None
            content = bytearray()
            for chunk in response.iter_content(chunk_size=4096):
                content.extend(chunk)
                if len(content) > 65536:
                    return None
            result = json.loads(content.decode("utf-8"))
            return result if isinstance(result, dict) else None
        except Exception:
            return None
        finally:
            if response is not None:
                response.close()

    def _matches(self, result, phone, *, sid=None, status=None):
        return (isinstance(result, dict)
            and isinstance(result.get("sid"), str) and bool(_SID["VE"].fullmatch(result["sid"]))
            and (sid is None or hmac.compare_digest(result["sid"], sid))
            and result.get("account_sid") == self._account
            and result.get("service_sid") == self._service
            and result.get("to") == phone and result.get("channel") == "sms"
            and result.get("status") == status)

    def _latest(self, db, reference_hash):
        row = db.execute("SELECT * FROM reifen_sms_challenges WHERE reference_hash=? "
                         "ORDER BY created_at DESC,challenge_id DESC LIMIT 1",
                         (reference_hash,)).fetchone()
        return dict(row) if row is not None else None

    def start(self, phone, reference):
        """Send once for a new challenge; repeat pending starts reuse its binding."""
        try:
            phone, ref_hash, phone_hash = self._identity(phone, reference, "start")
            now = self._clock()
            challenge_id = secrets.token_hex(24)
            with closing(self._get_db()) as db:
                previous = self._latest(db, ref_hash)
                if previous and (previous["phone_hash"] != phone_hash
                                 or previous["service_hash"] != self._service_hash):
                    raise ReifenSMSUnavailable(_ERROR)
                if previous and previous["expires_at"] > now:
                    if previous["state"] in {"pending", "approved"}:
                        return {"status": previous["state"],
                                "verification_id": previous["verification_sid"], "reused": True}
                    raise ReifenSMSUnavailable(_ERROR)
                db.execute("UPDATE reifen_sms_challenges SET state='expired',updated_at=? "
                           "WHERE expires_at<=? AND state IN " + _ACTIVE, (now, now))
                claimed = db.execute("""INSERT INTO reifen_sms_challenges
                    (challenge_id,reference_hash,phone_hash,service_hash,state,created_at,expires_at,updated_at)
                    VALUES(?,?,?,?,'starting',?,?,?) ON CONFLICT DO NOTHING RETURNING challenge_id""",
                    (challenge_id, ref_hash, phone_hash, self._service_hash,
                     now, now + CHALLENGE_TTL_SECONDS, now)).fetchone()
                db.commit()
                if claimed is None:
                    raise ReifenSMSUnavailable(_ERROR)
            result = self._post("Verifications", {"To": phone, "Channel": "sms", "Locale": "de"})
            valid = self._matches(result, phone, status="pending")
            with closing(self._get_db()) as db:
                if valid:
                    updated = db.execute("UPDATE reifen_sms_challenges SET verification_sid=?,"
                        "state='pending',updated_at=? WHERE challenge_id=? AND state='starting' "
                        "AND expires_at>?", (result["sid"], self._clock(), challenge_id,
                                             self._clock())).rowcount
                else:
                    updated = 0
                    db.execute("UPDATE reifen_sms_challenges SET state='uncertain',updated_at=? "
                               "WHERE challenge_id=? AND state='starting'", (self._clock(), challenge_id))
                db.commit()
            if not valid or updated != 1:
                raise ReifenSMSUnavailable(_ERROR)
            return {"status": "pending", "verification_id": result["sid"], "reused": False}
        except ReifenSMSUnavailable:
            raise
        except Exception:
            raise ReifenSMSUnavailable(_ERROR) from None

    def check(self, phone, code, reference):
        """Return True only for this persisted challenge's matching approval."""
        try:
            phone, ref_hash, phone_hash = self._identity(phone, reference, "check")
            if not isinstance(code, str) or not re.fullmatch(r"[0-9]{4,10}", code, re.ASCII):
                return False
            now = self._clock()
            with closing(self._get_db()) as db:
                row = self._latest(db, ref_hash)
                if (not row or row["phone_hash"] != phone_hash or row["service_hash"] != self._service_hash
                        or row["expires_at"] <= now):
                    return False
                if row["state"] == "approved":
                    return True
                claimed = db.execute("UPDATE reifen_sms_challenges SET state='checking',checks=checks+1,updated_at=? "
                    "WHERE challenge_id=? AND state='pending' AND checks<? AND expires_at>?",
                    (now, row["challenge_id"], MAX_CHECKS, now)).rowcount
                db.commit()
                if claimed != 1:
                    return False
            result = self._post("VerificationCheck", {"VerificationSid": row["verification_sid"], "Code": code})
            approved = self._matches(result, phone, sid=row["verification_sid"], status="approved")
            pending = self._matches(result, phone, sid=row["verification_sid"], status="pending")
            next_state = "approved" if approved else "pending" if pending or result is None else "failed"
            if not approved and row["checks"] + 1 >= MAX_CHECKS:
                next_state = "failed"
            with closing(self._get_db()) as db:
                updated = db.execute("UPDATE reifen_sms_challenges SET state=?,updated_at=? "
                    "WHERE challenge_id=? AND state='checking' AND expires_at>?",
                    (next_state, self._clock(), row["challenge_id"], self._clock())).rowcount
                db.commit()
            return approved and updated == 1
        except ReifenSMSUnavailable:
            raise
        except Exception:
            raise ReifenSMSUnavailable(_ERROR) from None
