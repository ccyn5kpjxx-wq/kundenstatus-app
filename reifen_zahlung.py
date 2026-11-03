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
    capture_method = value.get("payment_capture_method", "automatic")
    if not isinstance(capture_method, str) or capture_method not in {"automatic", "manual"}:
        raise ReifenZahlungFehler("Ungültiger serverseitiger Zahlungseinzug.")
    return {"id": order_id, "quote_hash": quote, "amount_total": amount,
            "currency": "eur", "payment_attempt": attempt, "produktart": category,
            "payment_capture_method": capture_method}


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
    supports_manual_capture = True  # Implemented capability, not proof of the configured key's permissions.

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
        method = metadata.get("capture_method", "automatic")
        if not isinstance(method, str) or method not in {"automatic", "manual"}:
            raise ReifenZahlungFehler("Die Zahlungsart des Stripe-Vorgangs ist widersprüchlich.")
        if method == "manual" or expected_order is not None and order["payment_capture_method"] == "manual":
            if (expected_order is None or order["payment_capture_method"] != "manual"
                    or method != "manual" or metadata.get("payment_attempt") != order["payment_attempt"]):
                raise ReifenZahlungFehler("Die Kartenautorisierung benötigt die gespeicherte Bestellbindung.")
            if expected_order.get("mode", self.mode) != self.mode:
                raise ReifenZahlungFehler("Die Bestellung gehört zu einer anderen Zahlungsumgebung.")
            stored_session = expected_order.get("payment_id")
            stored_intent = expected_order.get("payment_intent")
            if stored_session and stored_session != sid or stored_intent and stored_intent != payment_intent:
                raise ReifenZahlungFehler("Die Zahlung passt nicht zum gespeicherten Zahlungsversuch.")
            result["payment_capture_method"] = "manual"
            if data["status"] == "complete":
                if payment_intent is None:
                    raise ReifenZahlungFehler("Der abgeschlossene Checkout hat keinen bestätigbaren Zahlungsvorgang.")
                result.update(self._retrieve_manual_intent(payment_intent, order))
        return result

    def _manual_intent(self, raw, order, expected_id, *, authorization_details=True):
        data = _mapping(raw)
        metadata = data.get("metadata")
        expected_metadata = {"integration": INTEGRATION, "order_id": order["id"],
                             "quote_hash": order["quote_hash"], "payment_attempt": order["payment_attempt"],
                             "capture_method": "manual"}
        if (data.get("object") != "payment_intent" or data.get("id") != expected_id
                or data.get("livemode") is not self.livemode or data.get("currency") != "eur"
                or type(data.get("amount")) is not int or data["amount"] != order["amount_total"]
                or data.get("capture_method") != "manual" or not isinstance(metadata, Mapping)
                or any(metadata.get(key) != value for key, value in expected_metadata.items())
                or data.get("on_behalf_of") or data.get("transfer_data")):
            raise ReifenZahlungFehler("Die Stripe-Autorisierung passt nicht zur gespeicherten Bestellung.")
        state = data.get("status")
        if not isinstance(state, str) or state not in {"requires_payment_method", "requires_confirmation", "requires_action",
                         "processing", "requires_capture", "canceled", "succeeded"}:
            raise ReifenZahlungFehler("Der Stripe-Autorisierungsstatus ist nicht eindeutig.")
        capturable, received = data.get("amount_capturable"), data.get("amount_received")
        if (type(capturable) is not int or type(received) is not int
                or not 0 <= capturable <= order["amount_total"] or not 0 <= received <= order["amount_total"]):
            raise ReifenZahlungFehler("Die Stripe-Zahlungsbeträge sind widersprüchlich.")
        result = {"payment_intent": expected_id, "stripe_status": state, "payment_capture_method": "manual",
                  "livemode": self.livemode, "amount_total": order["amount_total"], "currency": "eur",
                  "reference": order["id"], "quote_hash": order["quote_hash"],
                  "amount_capturable": capturable, "amount_received": received, "capture_before": None,
                  "status": "processing", "payment_status": "unpaid"}
        if state == "succeeded":
            if received != order["amount_total"] or capturable != 0:
                raise ReifenZahlungFehler("Der tatsächliche Zahlungseingang muss abgeglichen werden.")
            result.update(status="succeeded", payment_status="paid")
        elif state == "requires_capture":
            if capturable != order["amount_total"] or received != 0:
                raise ReifenZahlungFehler("Die vollständige Kartenautorisierung fehlt.")
            result["status"] = "requires_capture"
            if authorization_details:
                result["capture_before"] = self._capture_deadline(data.get("latest_charge"), expected_id, order)
                result.update(status="authorized", payment_status="authorized")
        elif state == "canceled":
            if capturable != 0 or received != 0:
                raise ReifenZahlungFehler("Der stornierte Zahlungsvorgang muss abgeglichen werden.")
            result.update(status="canceled", payment_status="canceled")
        elif capturable != 0 or received != 0:
            raise ReifenZahlungFehler("Der offene Stripe-Zahlungsstatus ist widersprüchlich.")
        return result

    def _capture_deadline(self, charge_id, intent_id, order):
        if isinstance(charge_id, Mapping):
            charge_id = charge_id.get("id")
        charge_id = _text(charge_id, r"ch_[A-Za-z0-9]{1,200}", "Ein bestätigbarer Kartenautorisierungsbeleg fehlt.")
        try:
            charge = _mapping(self.client.v1.charges.retrieve(charge_id))
        except Exception:
            raise ReifenZahlungFehler("Die tatsächliche Autorisierungsfrist ist nicht sicher abrufbar.") from None
        details = charge.get("payment_method_details")
        card = details.get("card") if isinstance(details, Mapping) else None
        deadline = card.get("capture_before") if isinstance(card, Mapping) else None
        if (charge.get("id") != charge_id or charge.get("object") != "charge"
                or charge.get("livemode") is not self.livemode or charge.get("payment_intent") != intent_id
                or charge.get("currency") != "eur" or type(charge.get("amount")) is not int
                or charge["amount"] != order["amount_total"] or charge.get("captured") is not False
                or charge.get("paid") is not True or type(charge.get("amount_captured")) is not int
                or charge["amount_captured"] != 0 or not isinstance(details, Mapping) or details.get("type") != "card"
                or type(deadline) is not int or deadline <= self.now()):
            raise ReifenZahlungFehler("Ein gültiger, noch nicht eingezogener Kartenbetrag ist nicht bestätigt.")
        return deadline

    def _retrieve_manual_intent(self, intent_id, order, *, authorization_details=True):
        intent_id = _text(intent_id, r"pi_[A-Za-z0-9]{1,200}", "Ein gespeicherter Stripe-Zahlungsvorgang fehlt.")
        try:
            raw = self.client.v1.payment_intents.retrieve(intent_id)
        except Exception:
            raise ReifenZahlungFehler("Der aktuelle Stripe-Autorisierungsstatus ist nicht sicher abrufbar.") from None
        return self._manual_intent(raw, order, intent_id, authorization_details=authorization_details)

    def _manual_order(self, order):
        checked = _order(order)
        if checked["payment_capture_method"] != "manual" or order.get("mode", self.mode) != self.mode:
            raise ReifenZahlungFehler("Dieser Vorgang benötigt eine passende gespeicherte Kartenautorisierung.")
        intent_id = _text(order.get("payment_intent"), r"pi_[A-Za-z0-9]{1,200}", "Ein gespeicherter Stripe-Zahlungsvorgang fehlt.")
        return checked, intent_id

    def _manual_key(self, operation, order, intent_id):
        frozen = "|".join((INTEGRATION, self.mode, order["id"], order["quote_hash"], order["payment_attempt"],
                           "manual", intent_id, str(order["amount_total"])))
        if operation == "capture":
            # The single-capture request omits the former multicapture parameter.
            # Different request parameters must not reuse the former request key.
            frozen += "|single_capture_v2"
        return "reifen-" + operation + "-" + hashlib.sha256(frozen.encode()).hexdigest()

    def capture(self, order):
        """Caller must persist the supplier ACK and capture intent before this IO."""
        checked, intent_id = self._manual_order(order)
        current = self._retrieve_manual_intent(intent_id, checked)
        if current["status"] == "succeeded":
            return {"id": intent_id, **current}  # Lost capture response: no second POST.
        if current["status"] != "authorized":
            raise ReifenZahlungFehler("Ein sicher einziehbarer Kartenbetrag fehlt.")
        try:
            # Single full capture uses Stripe's default finalization. Sending
            # final_capture explicitly is rejected when multicapture is unavailable.
            raw = self.client.v1.payment_intents.capture(intent_id,
                {"amount_to_capture": checked["amount_total"]},
                options={"idempotency_key": self._manual_key("capture", checked, intent_id)})
        except Exception:
            raise ReifenZahlungFehler("Der Zahlungseinzug muss abgeglichen werden. Keine zweite Zahlung starten.") from None
        result = self._manual_intent(raw, checked, intent_id)
        if result["status"] != "succeeded":
            raise ReifenZahlungFehler("Der tatsächliche Zahlungseinzug ist noch nicht eindeutig bestätigt.")
        return {"id": intent_id, **result}

    def cancel_authorization(self, order):
        """Cancel an actually uncaptured PI; never refund a captured payment."""
        checked, intent_id = self._manual_order(order)
        current = self._retrieve_manual_intent(intent_id, checked, authorization_details=False)
        if current["status"] == "canceled":
            return {"id": intent_id, **current}
        if current["stripe_status"] != "requires_capture":
            raise ReifenZahlungFehler("Dieser Zahlungsvorgang ist keine stornierbare Kartenautorisierung.")
        try:
            raw = self.client.v1.payment_intents.cancel(intent_id, {"cancellation_reason": "abandoned"},
                options={"idempotency_key": self._manual_key("cancel", checked, intent_id)})
        except Exception:
            raise ReifenZahlungFehler("Die Freigabe des Kartenbetrags muss abgeglichen werden.") from None
        result = self._manual_intent(raw, checked, intent_id, authorization_details=False)
        if result["status"] != "canceled":
            raise ReifenZahlungFehler("Die Freigabe des Kartenbetrags ist noch nicht eindeutig bestätigt.")
        return {"id": intent_id, **result}

    def create(self, order):
        checked = _order(order)
        if checked["payment_capture_method"] == "manual" and order.get("mode", self.mode) != self.mode:
            raise ReifenZahlungFehler("Die Bestellung gehört zu einer anderen Zahlungsumgebung.")
        expiry = order.get("payment_expires_at")
        created = order.get("payment_created_at")
        now = self.now()
        if (type(expiry) is not int or type(created) is not int or created > now
                or not 0 <= now - created < 86400 or not 1800 <= expiry - created <= 86400):
            raise ReifenZahlungFehler("Eine eingefrorene Zahlungsfrist zwischen 30 Minuten und 24 Stunden ist erforderlich.")
        metadata = {"integration": INTEGRATION, "order_id": checked["id"], "quote_hash": checked["quote_hash"]}
        if checked["payment_capture_method"] == "manual":
            metadata.update(capture_method="manual", payment_attempt=checked["payment_attempt"])
        name = ("Felgenbestellung" if checked["produktart"] == "felgen" else "Reifenbestellung") + " " + checked["id"]
        success = self.origin + self.success_path.replace("{order_id}", checked["id"]) + "?session_id={CHECKOUT_SESSION_ID}"
        cancel = self.origin + self.cancel_path.replace("{order_id}", checked["id"])
        params = {"mode": "payment", "ui_mode": "hosted_page", "locale": "de",
                  "payment_method_types": ["card"], "client_reference_id": checked["id"],
                  "metadata": metadata, "payment_intent_data": {"metadata": dict(metadata)},
                  "line_items": [{"quantity": 1, "price_data": {"currency": "eur", "unit_amount": checked["amount_total"],
                      "product_data": {"name": name}}}],
                  "success_url": success, "cancel_url": cancel, "expires_at": expiry}
        if checked["payment_capture_method"] == "manual":
            params["payment_intent_data"]["capture_method"] = "manual"
        key_material = "|".join((INTEGRATION, self.mode, checked["id"], checked["quote_hash"], checked["payment_attempt"]))
        if checked["payment_capture_method"] == "manual":
            key_material += "|manual"
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
