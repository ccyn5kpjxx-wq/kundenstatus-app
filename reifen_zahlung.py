"""Dedicated Stripe Hosted Checkout transport for tire orders.

No Flask routes, database changes, rental credentials or automatic activation.
Only explicit REIFEN_STRIPE_* settings are read. Constructing the adapter does
not contact Stripe. Calls must receive a persisted, server-approved order.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
import hashlib
import math
import re
import time
from urllib.parse import urlsplit

import stripe


INTEGRATION = "reifen_checkout_v1"
MAX_AMOUNT_CENTS = 10_000_000
WEBHOOK_TOLERANCE = 300
_EVENTS = {"checkout.session.completed", "checkout.session.async_payment_succeeded",
           "checkout.session.async_payment_failed", "checkout.session.expired"}


class ReifenZahlungFehler(ValueError):
    """Safe public error; never contains a provider payload or a credential."""


def _text(value, pattern, message):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ReifenZahlungFehler(message)
    return value


def _mapping(value):
    if not isinstance(value, Mapping) and callable(getattr(value, "to_dict", None)):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise ReifenZahlungFehler("Ungültige Antwort des Zahlungsanbieters.")
    return value


def _url(value):
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ReifenZahlungFehler("Ungültige Zahlungs- oder Rückleitungsadresse.") from None
    return parsed, port


def _order(value):
    if not isinstance(value, Mapping):
        raise ReifenZahlungFehler("Die serverseitige Bestellung fehlt.")
    category = value.get("produktart", "reifen")
    if not isinstance(category, str) or category not in {"reifen", "felgen"}:
        raise ReifenZahlungFehler("Die Produktart der Bestellung ist ungültig.")
    for field in ("produkt", "query"):
        nested = value.get(field)
        if isinstance(nested, Mapping) and "produktart" in nested:
            if not isinstance(nested["produktart"], str) or nested["produktart"] != category:
                raise ReifenZahlungFehler("Die Produktart der Bestellung ist widersprüchlich.")
    order_id = _text(value.get("id"), r"[A-Za-z0-9_-]{1,100}", "Ungültige Bestellreferenz.")
    quote = _text(value.get("quote_hash"), r"[0-9a-f]{64}", "Ungültige Angebotsreferenz.")
    amount = value.get("amount_total")
    if type(amount) is not int or not 0 < amount <= MAX_AMOUNT_CENTS or value.get("currency") != "eur":
        raise ReifenZahlungFehler("Ein bestätigter positiver EUR-Gesamtbetrag in Cent ist erforderlich.")
    attempt = _text(value.get("payment_attempt", "1"), r"[A-Za-z0-9_-]{1,50}", "Ungültiger Zahlungsversuch.")
    return {"id": order_id, "quote_hash": quote, "amount_total": amount,
            "currency": "eur", "payment_attempt": attempt, "produktart": category}


class StripeReifenGateway:
    """Explicit test/live Stripe SDK client, independent of the demo checkout.

    create(order) requires frozen payment_created_at/payment_expires_at (Unix
    seconds). The core
    must persist the full attempt before IO, reuse identical arguments on retry,
    and never create blindly after Stripe's idempotency retention window.
    retrieve(sid, expected_order=order) additionally verifies local order binding.
    verify_webhook verifies raw bytes and returns only an event pointer; the
    core still retrieves the Session and atomically deduplicates fulfillment.
    """
    def __init__(self, config, *, client=None, clock=None):
        if not isinstance(config, Mapping) or config.get("REIFEN_STRIPE_MODE") not in {"test", "live"}:
            raise ReifenZahlungFehler("Ein eigener Stripe-Test- oder Livemodus für Reifen ist erforderlich.")
        self.mode = config["REIFEN_STRIPE_MODE"]
        self.livemode = self.mode == "live"
        key = _text(config.get("REIFEN_STRIPE_SECRET_KEY"),
                    rf"(?:sk|rk)_{self.mode}_[A-Za-z0-9_]{{8,256}}", "Der Reifen-Stripe-Schlüssel passt nicht zum gewählten Modus.")
        self._webhook_secret = _text(config.get("REIFEN_STRIPE_WEBHOOK_SECRET"),
                                    r"whsec_[A-Za-z0-9_]{8,256}", "Ein eigener Reifen-Webhookschlüssel ist erforderlich.")
        origin = config.get("REIFEN_STRIPE_PUBLIC_ORIGIN")
        if not isinstance(origin, str) or any(ord(c) < 33 for c in origin) or "\\" in origin:
            raise ReifenZahlungFehler("Eine feste sichere Rückleitungsadresse ist erforderlich.")
        target, port = _url(origin)
        allowed = config.get("REIFEN_STRIPE_ALLOWED_REDIRECT_HOSTS")
        if (not isinstance(allowed, (list, tuple, set)) or not allowed
                or any(not isinstance(host, str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host) for host in allowed)
                or target.scheme != "https" or target.hostname not in allowed or target.username or target.password
                or port not in (None, 443) or target.path not in ("", "/") or target.query or target.fragment):
            raise ReifenZahlungFehler("Die Rückleitungsdomain ist nicht freigegeben.")
        self.origin = origin.rstrip("/")
        self.success_path = self._path(config.get("REIFEN_STRIPE_SUCCESS_PATH", "/reifen/zahlung/erfolg/{order_id}"))
        self.cancel_path = self._path(config.get("REIFEN_STRIPE_CANCEL_PATH", "/reifen/zahlung/abgebrochen/{order_id}"))
        self.clock = clock or time.time
        # A dedicated client avoids globals, rental account/key reuse and the
        # SDK's long default timeout. Automatic retries stay off; the core owns
        # persisted retry/reconciliation decisions with stable idempotency.
        self.client = client if client is not None else stripe.StripeClient(
            key, http_client=stripe.RequestsClient(timeout=(5.0, 15.0)), max_network_retries=0)

    @staticmethod
    def _path(value):
        if (not isinstance(value, str) or not value.startswith("/") or value.startswith("//")
                or not re.fullmatch(r"/[A-Za-z0-9_/{}/.-]{1,220}", value)
                or value.count("{order_id}") > 1 or ".." in value
                or "{" in value.replace("{order_id}", "") or "}" in value.replace("{order_id}", "")):
            raise ReifenZahlungFehler("Ungültiger fest konfigurierter Rückleitungspfad.")
        return value

    def now(self):
        value = self.clock()
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ReifenZahlungFehler("Die Zahlungsuhr benötigt eine Zeitzone.")
            value = value.astimezone(timezone.utc).timestamp()
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ReifenZahlungFehler("Ungültige Zahlungsuhr.")
        return int(value)

    def _sid(self, value):
        return _text(value, rf"cs_{self.mode}_[A-Za-z0-9]{{1,200}}", "Die Stripe-Session passt nicht zum Reifen-Zahlungsmodus.")

    def _session(self, raw, *, expected_id=None, expected_order=None):
        data = _mapping(raw)
        sid = self._sid(data.get("id"))
        metadata = data.get("metadata")
        if (expected_id is not None and sid != expected_id or data.get("object") != "checkout.session"
                or data.get("livemode") is not self.livemode or data.get("mode") != "payment"
                or data.get("currency") != "eur" or type(data.get("amount_total")) is not int
                or not 0 < data["amount_total"] <= MAX_AMOUNT_CENTS
                or not isinstance(metadata, Mapping) or metadata.get("integration") != INTEGRATION
                or metadata.get("order_id") != data.get("client_reference_id")
                or data.get("status") not in {"open", "complete", "expired"}
                or data.get("payment_status") not in {"unpaid", "paid"}
                or type(data.get("expires_at")) is not int or data["expires_at"] <= 0):
            raise ReifenZahlungFehler("Die Stripe-Session stimmt nicht mit einer Reifenbestellung überein.")
        reference = _text(metadata.get("order_id"), r"[A-Za-z0-9_-]{1,100}", "Ungültige Zahlungsreferenz.")
        quote = _text(metadata.get("quote_hash"), r"[0-9a-f]{64}", "Ungültige Zahlungsreferenz.")
        payment_intent = data.get("payment_intent")
        if isinstance(payment_intent, Mapping):
            payment_intent = payment_intent.get("id")
        if payment_intent is not None:
            _text(payment_intent, r"pi_[A-Za-z0-9]{1,200}", "Ungültige Zahlungsreferenz.")
        paid = data["status"] == "complete" and data["payment_status"] == "paid" and bool(payment_intent)
        if data["payment_status"] == "paid" and not paid:
            raise ReifenZahlungFehler("Widersprüchlicher Stripe-Zahlungsstatus.")
        checkout_url = data.get("url") or ""
        if checkout_url:
            if not isinstance(checkout_url, str) or len(checkout_url) > 5000 or "\\" in checkout_url or any(ord(c) < 33 for c in checkout_url):
                raise ReifenZahlungFehler("Ungültige Stripe-Checkoutadresse.")
            parsed, port = _url(checkout_url)
            if parsed.scheme != "https" or parsed.hostname != "checkout.stripe.com" or parsed.username or parsed.password or port not in (None, 443):
                raise ReifenZahlungFehler("Unerwartete Stripe-Checkoutdomain.")
        result = {"id": sid, "url": checkout_url, "livemode": self.livemode, "expires_at": data["expires_at"],
                  "status": "succeeded" if paid else "processing" if data["status"] == "complete" else data["status"],
                  "payment_status": data["payment_status"], "amount_total": data["amount_total"], "currency": "eur",
                  "reference": reference, "quote_hash": quote, "payment_intent": payment_intent}
        if expected_order is not None:
            order = _order(expected_order)
            if (result["reference"], result["quote_hash"], result["amount_total"], result["currency"]) != (
                    order["id"], order["quote_hash"], order["amount_total"], order["currency"]):
                raise ReifenZahlungFehler("Die Zahlung passt nicht zur gespeicherten Bestellung.")
        return result

    def create(self, order):
        checked = _order(order)
        expiry = order.get("payment_expires_at")
        created = order.get("payment_created_at")
        now = self.now()
        if (type(expiry) is not int or type(created) is not int or created > now
                or not 0 <= now - created < 86400 or not 1800 <= expiry - created <= 86400):
            raise ReifenZahlungFehler("Eine eingefrorene Zahlungsfrist zwischen 30 Minuten und 24 Stunden ist erforderlich.")
        metadata = {"integration": INTEGRATION, "order_id": checked["id"], "quote_hash": checked["quote_hash"]}
        name = ("Felgenbestellung" if checked["produktart"] == "felgen" else "Reifenbestellung") + " " + checked["id"]
        success = self.origin + self.success_path.replace("{order_id}", checked["id"]) + "?session_id={CHECKOUT_SESSION_ID}"
        cancel = self.origin + self.cancel_path.replace("{order_id}", checked["id"])
        params = {"mode": "payment", "ui_mode": "hosted_page", "locale": "de",
                  "payment_method_types": ["card"], "client_reference_id": checked["id"],
                  "metadata": metadata, "payment_intent_data": {"metadata": dict(metadata)},
                  "line_items": [{"quantity": 1, "price_data": {"currency": "eur", "unit_amount": checked["amount_total"],
                      "product_data": {"name": name}}}],
                  "success_url": success, "cancel_url": cancel, "expires_at": expiry}
        key_material = "|".join((INTEGRATION, self.mode, checked["id"], checked["quote_hash"], checked["payment_attempt"]))
        key = "reifen-checkout-" + hashlib.sha256(key_material.encode()).hexdigest()
        try:
            raw = self.client.v1.checkout.sessions.create(params, options={"idempotency_key": key})
        except Exception:
            raise ReifenZahlungFehler("Stripe-Checkout konnte nicht sicher vorbereitet werden. Den Zahlungsversuch vor Wiederholung abgleichen.") from None
        result = self._session(raw, expected_order=checked)
        if result["expires_at"] != expiry or not result["url"]:
            raise ReifenZahlungFehler("Die Stripe-Zahlungsfrist oder Checkoutadresse muss geprüft werden.")
        return result

    def retrieve(self, session_id, *, expected_order=None):
        sid = self._sid(session_id)
        try:
            raw = self.client.v1.checkout.sessions.retrieve(sid)
        except Exception:
            raise ReifenZahlungFehler("Der aktuelle Stripe-Zahlungsstatus ist nicht sicher abrufbar.") from None
        return self._session(raw, expected_id=sid, expected_order=expected_order)

    def verify_webhook(self, raw_body, stripe_signature):
        if (not isinstance(raw_body, bytes) or not 0 < len(raw_body) <= 1024 * 1024
                or not isinstance(stripe_signature, str) or len(stripe_signature) > 4096):
            raise ReifenZahlungFehler("Ungültige Stripe-Ereignisübermittlung.")
        timestamps = [part[2:] for part in stripe_signature.split(",") if part.startswith("t=")]
        if len(timestamps) != 1 or not re.fullmatch(r"[0-9]{1,12}", timestamps[0]) or abs(self.now() - int(timestamps[0])) > WEBHOOK_TOLERANCE:
            raise ReifenZahlungFehler("Die Stripe-Signatur ist ungültig oder veraltet.")
        try:
            event = stripe.Webhook.construct_event(raw_body, stripe_signature, self._webhook_secret, tolerance=WEBHOOK_TOLERANCE)
        except Exception:
            raise ReifenZahlungFehler("Die Stripe-Signatur konnte nicht bestätigt werden.") from None
        event = _mapping(event)
        event_id = _text(event.get("id"), r"evt_[A-Za-z0-9]{1,200}", "Ungültige Stripe-Ereignisreferenz.")
        if event.get("livemode") is not self.livemode or event.get("account") or event.get("context"):
            raise ReifenZahlungFehler("Das Stripe-Ereignis passt nicht zum Reifen-Zahlungskonto.")
        event_type = event.get("type")
        if not isinstance(event_type, str) or len(event_type) > 120:
            raise ReifenZahlungFehler("Ungültiger Stripe-Ereignistyp.")
        result = {"event_id": event_id, "type": event_type, "livemode": self.livemode, "ignored": True, "session_id": None}
        if event_type not in _EVENTS:
            return result
        data = event.get("data")
        obj = data.get("object") if isinstance(data, Mapping) else None
        if not isinstance(obj, Mapping) or obj.get("object") != "checkout.session":
            raise ReifenZahlungFehler("Ungültiges Stripe-Checkoutereignis.")
        metadata = obj.get("metadata")
        if not isinstance(metadata, Mapping) or metadata.get("integration") != INTEGRATION:
            return result  # Other products in the same account are not tire orders.
        result.update(session_id=self._sid(obj.get("id")), ignored=False)
        return result
