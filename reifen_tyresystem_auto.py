"""Automatic purchase orchestration without pretending that stock is reserved.

Each draft stores its immutable flow/policy/account/quote. A durable sending
claim precedes the one supplier POST; an uncertain send never automatically
repeats. Capture follows only a durably stored, request-bound supplier receipt.
No configuration, database or provider operation occurs at import.
"""
from collections.abc import Mapping
from contextvars import ContextVar
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import re
import threading
import unicodedata

from reifenpreis_kalkulation import berechne_reifenpreis
from tyresystem_api import TyreSystemInquiryClient
from tyresystem_order import (DurableTyreSystemOrders, freeze_order_request, request_sha256)


VERFAHREN = "tyresystem_auto_v1"
BACKUP_TABLES = ("reifen_tyresystem_auto", "tyresystem_order_attempts")
_INQUIRY = ContextVar("reifen_auto_inquiry", default=None)
_ORDER = ContextVar("reifen_auto_order", default=None)
_ACTIVE_LOCK = threading.Lock()
_ACTIVE = set()
_SHA = re.compile(r"[a-f0-9]{64}")
_SKU = re.compile(r"[1-9][0-9]{0,17}")
_POLICY_KEYS = {"schema", "freigegeben", "nachweis", "configured_mode", "account_fingerprint",
    "api_modus_bestaetigt", "api_modus_nachweis", "stripe_manual_bestaetigt", "stripe_manual_nachweis",
    "preis_bestaetigt", "preis_nachweis", "waehrung", "preisbasis", "mwst_satz", "aufschlag_prozent",
    "einzelversand_netto", "frachtfrei_ab", "auftragsannahme_bestaetigt", "auftragsannahme_nachweis",
    "lieferprognose", "delivery_address", "artikel_freigaben"}


class AutoBeschaffungFehler(ValueError):
    def __init__(self, code="pruefung_noetig"):
        self.code = code
        super().__init__({"configuration": "Die automatische Beschaffung ist noch nicht vollständig freigegeben.",
            "angebot_geaendert": "Preis oder Verfügbarkeit hat sich geändert. Bitte wählen Sie den Artikel erneut.",
            "montage_nicht_freigegeben": "Für die automatische Beschaffung ist derzeit nur Abholung ohne Montagetermin möglich.",
            "abgelehnt": "Die Bestellung wurde nicht angenommen. Eine Kartenautorisierung wird aufgehoben.",
            "pruefung_noetig": "Der Bestellstand muss geprüft werden. Bitte keine weitere Bestellung oder Zahlung starten."
            }.get(code, "Der Bestellstand muss geprüft werden. Bitte wenden Sie sich an uns."))


def canonical(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":"))


def sha(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def text(value, maximum=200):
    if (not isinstance(value, str) or not value.strip() or len(value) > maximum
            or any(unicodedata.category(char).startswith("C") for char in value)):
        raise ValueError()
    return value


def validate_policy(value):
    """Actual recorded approvals are required; a catalog flag proves none of them."""
    try:
        if not isinstance(value, Mapping) or set(value) != _POLICY_KEYS:
            raise ValueError()
        policy = json.loads(canonical(dict(value)))
        if (type(policy["schema"]) is not int or policy["schema"] != 1
                or policy["configured_mode"] not in {"test", "live"}
                or not isinstance(policy["account_fingerprint"], str) or not _SHA.fullmatch(policy["account_fingerprint"])
                or any(policy[key] is not True for key in ("freigegeben", "api_modus_bestaetigt",
                    "stripe_manual_bestaetigt", "preis_bestaetigt", "auftragsannahme_bestaetigt"))
                or policy["waehrung"] != "EUR" or policy["preisbasis"] != "net_unit"
                or policy["mwst_satz"] != "19.00" or policy["aufschlag_prozent"] != "10.00"
                or policy["einzelversand_netto"] != "6.00" or type(policy["frachtfrei_ab"]) is not int
                or policy["frachtfrei_ab"] != 2):
            raise ValueError()
        for key in ("nachweis", "api_modus_nachweis", "stripe_manual_nachweis", "preis_nachweis", "auftragsannahme_nachweis"):
            text(policy[key])
        text(policy["lieferprognose"], 120)
        address = policy["delivery_address"]
        if (not isinstance(address, dict) or set(address) != {"neutraldelivery", "salutation", "name", "street", "zipcode", "city", "countrycode"}
                or type(address["neutraldelivery"]) is not int or address["neutraldelivery"] not in (0, 1)
                or address["salutation"] != "Firma" or address["countrycode"] != "DE"
                or not isinstance(address["zipcode"], str) or not re.fullmatch(r"[0-9]{5}", address["zipcode"])):
            raise ValueError()
        for key in ("name", "street", "city"):
            text(address[key], 160)
        articles = policy["artikel_freigaben"]
        if not isinstance(articles, dict) or not 1 <= len(articles) <= 100000:
            raise ValueError()
        for sku, item in articles.items():
            if (not isinstance(sku, str) or not _SKU.fullmatch(sku) or not isinstance(item, dict)
                    or set(item) != {"produktart", "ean", "paketversandfaehig", "nachweis"}
                    or item["produktart"] not in {"reifen", "felgen"} or item["paketversandfaehig"] is not True
                    or not isinstance(item["ean"], str) or not re.fullmatch(r"[0-9]{13}", item["ean"])):
                raise ValueError()
            text(item["nachweis"])
        return policy
    except Exception:
        raise AutoBeschaffungFehler("configuration") from None


def init_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS reifen_tyresystem_auto (
        order_id TEXT PRIMARY KEY, snapshot_json TEXT NOT NULL, snapshot_sha256 TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'draft', request_json TEXT NOT NULL DEFAULT '{}',
        request_sha256 TEXT NOT NULL DEFAULT '', receipt_json TEXT NOT NULL DEFAULT '{}',
        receipt_sha256 TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL
    )""")


class AutoBeschaffung:
    def __init__(self, service):
        self.s, self.app, self.p = service, service.app, service.p

    def now(self):
        return self.s.now().isoformat()

    def record(self, order_id):
        db = self.p.get_db()
        try:
            row = db.execute("SELECT * FROM reifen_tyresystem_auto WHERE order_id=?", (order_id,)).fetchone()
        finally:
            db.close()
        if row is None:
            return None
        value = dict(row)
        snapshot = json.loads(value["snapshot_json"])
        if sha(snapshot) != value["snapshot_sha256"] or snapshot.get("verfahren") != VERFAHREN:
            raise AutoBeschaffungFehler()
        value["snapshot"] = snapshot
        return value

    def change(self, record, state, **values):
        db = self.p.get_db()
        try:
            fields = {"state": state, "updated_at": self.now(), **values}
            count = db.execute("UPDATE reifen_tyresystem_auto SET " + ",".join(key+"=?" for key in fields)
                + " WHERE order_id=? AND state=? AND snapshot_sha256=?",
                (*fields.values(), record["order_id"], record["state"], record["snapshot_sha256"])).rowcount
            db.commit()
            return count == 1
        finally:
            db.close()

    def readiness(self, kind):
        try:
            policy = validate_policy(self.app.config.get("REIFEN_TYRESYSTEM_AUTO_POLICY"))
            self.clients(policy, self.app.config.get("REIFEN_KAUF_MODE"))
            if not any(item["produktart"] == kind for item in policy["artikel_freigaben"].values()):
                raise ValueError()
            terms = self.s.terms()
            if (terms.get("freigegeben") is not True or terms.get("status") != "freigegeben"
                    or not isinstance(terms.get("version"), str) or not terms["version"]
                    or terms["version"].startswith(("entwurf:", "demo:"))
                    or not isinstance(terms.get("beschaffungsverfahren"), list)
                    or VERFAHREN not in terms["beschaffungsverfahren"]
                    or terms.get("offene_entscheidungen")):
                raise ValueError()
            return []
        except Exception:
            return ["Automatische Beschaffung, Kontomodell oder Bedingungen noch nicht freigegeben"]

    def clients(self, policy, mode):
        reader, orders = (self.app.config.get(key) for key in ("REIFEN_TYRESYSTEM_INQUIRY", "REIFEN_TYRESYSTEM_ORDERS"))
        gateway = self.app.config.get("REIFEN_KAUF_GATEWAY")
        if (not isinstance(reader, TyreSystemInquiryClient) or not isinstance(orders, DurableTyreSystemOrders)
                or mode != policy["configured_mode"] or reader.configured_mode != mode or orders.configured_mode != mode
                or reader.account_fingerprint != policy["account_fingerprint"] or orders.account_fingerprint != policy["account_fingerprint"]
                or getattr(gateway, "livemode", None) is not (mode == "live")
                or getattr(gateway, "supports_manual_capture", None) is not True
                or not all(callable(getattr(gateway, name, None)) for name in ("create", "retrieve", "capture", "cancel_authorization"))):
            raise AutoBeschaffungFehler("configuration")
        return reader, orders, gateway

    def preflight(self, query, product, *, policy=None):
        if query.get("montage") == "ja":
            raise AutoBeschaffungFehler("montage_nicht_freigegeben")
        policy = validate_policy(policy if policy is not None else self.app.config.get("REIFEN_TYRESYSTEM_AUTO_POLICY"))
        reader, _, _ = self.clients(policy, policy["configured_mode"])
        sku, quantity, kind = product["artikel_id"], int(query["anzahl"]), self.s.category(query, product)
        approval = policy["artikel_freigaben"].get(sku)
        if not approval or approval["produktart"] != kind or approval["ean"] != product.get("ean"):
            raise AutoBeschaffungFehler("configuration")
        token = _INQUIRY.set((id(self.s), sku, quantity, policy["configured_mode"], policy["account_fingerprint"]))
        try:
            item = reader.inquire(sku, quantity)
        except Exception:
            raise AutoBeschaffungFehler("angebot_geaendert") from None
        finally:
            _INQUIRY.reset(token)
        if item.article_id != sku or item.ean != approval["ean"] or item.amount != quantity or item.stock < quantity:
            raise AutoBeschaffungFehler("angebot_geaendert")
        calculation = berechne_reifenpreis(einkauf_netto=item.price, menge=quantity,
            bezugskosten_netto=policy["einzelversand_netto"] if quantity < policy["frachtfrei_ab"] else "0.00",
            preise_bestaetigt=True, mwst_satz=policy["mwst_satz"])
        expected = {"einzel_brutto": calculation["einzelpreis_brutto_reifen"],
            "gesamt_felgen_brutto" if kind == "felgen" else "gesamt_reifen_brutto": calculation["gesamt_brutto_reifen"],
            "versand_brutto": calculation["versand_brutto"], "gesamt_brutto": calculation["gesamt_brutto"], "waehrung": "EUR"}
        if kind == "reifen":
            expected["montage_brutto"] = "0.00"
        if product["preis"] != expected:
            raise AutoBeschaffungFehler("angebot_geaendert")
        return policy, format(item.price, ".4f")

    def insert(self, db, order_id, query, product, quote_hash, terms, policy):
        selected = {**policy, "artikel_freigaben": {product["artikel_id"]: policy["artikel_freigaben"][product["artikel_id"]]}}
        snapshot = {"verfahren": VERFAHREN, "mode": policy["configured_mode"], "policy": selected,
            "source_policy_sha256": sha(policy), "produktart": self.s.category(query, product),
            "artikel_id": product["artikel_id"], "ean": product["ean"], "anzahl": int(query["anzahl"]),
            "quote_hash": quote_hash, "terms_sha256": terms["sha256"]}
        serialized, identity = canonical(snapshot), sha(snapshot)
        db.execute("INSERT INTO reifen_tyresystem_auto(order_id,snapshot_json,snapshot_sha256,updated_at) VALUES(?,?,?,?) "
                   "ON CONFLICT(order_id) DO NOTHING", (order_id, serialized, identity, self.now()))
        found = db.execute("SELECT snapshot_sha256 FROM reifen_tyresystem_auto WHERE order_id=?", (order_id,)).fetchone()
        if not found or found["snapshot_sha256"] != identity:
            raise AutoBeschaffungFehler()

    def bound(self, row, record):
        order, frozen = self.s.order(row), record["snapshot"]
        if (order["mode"] != frozen["mode"] or self.app.config.get("REIFEN_KAUF_MODE") != frozen["mode"]
                or order["produktart"] != frozen["produktart"]
                or order["artikel_id"] != frozen["artikel_id"] or order["anzahl"] != frozen["anzahl"]
                or order["produkt"].get("ean") != frozen["ean"] or order["quote_hash"] != frozen["quote_hash"]
                or order["rechtstexte"].get("sha256") != frozen["terms_sha256"] or row["montage_datum"] or row["montage_uhrzeit"]):
            raise AutoBeschaffungFehler()
        self.clients(frozen["policy"], frozen["mode"])
        return order

    def prepare(self, row, form):
        return self._single(row["id"], self._prepare, row, form)

    def _single(self, order_id, action, *args):
        # Suppress concurrent callbacks in this process. The durable CAS below
        # remains authoritative across processes; an abandoned sending claim
        # never causes a new POST after a restart.
        key = (id(self.app), order_id)
        with _ACTIVE_LOCK:
            if key in _ACTIVE:
                return
            _ACTIVE.add(key)
        try:
            return action(*args)
        finally:
            with _ACTIVE_LOCK:
                _ACTIVE.discard(key)

    def _prepare(self, row, form):
        self.s.require_row_ready(row)
        record = self.record(row["id"])
        if record["state"] != "draft":
            return
        order = self.bound(row, record)
        if (row["state"] not in {"entwurf", "telefon_bestaetigt", "preis_geaendert"}
                or not row["telefon_bestaetigt_am"] or row["sms_state"] != "approved"
                or form.get("bestellpflicht") != "ja" or form.get("bedingungen") != "ja"
                or form.get("quote_hash") != row["quote_hash"] or self.s.terms().get("sha256") != order["rechtstexte"].get("sha256")):
            raise AutoBeschaffungFehler()
        query, product = self.s.fresh_order(row)
        if self.s.quote_hash(query, product) != row["quote_hash"]:
            raise AutoBeschaffungFehler("angebot_geaendert")
        self.preflight(query, product, policy=record["snapshot"]["policy"])
        if not self.change(record, "authorizing" if row["zahlungsart"] == "online" else "authorized"):
            return
        declarations = canonical({"bestellpflicht": True, "bedingungen": True})
        if not self.s.cas(row, state="zahlung_wird_angelegt" if row["zahlungsart"] == "online" else "zahlung_offen",
                payment_expires_at=int(self.s.now().timestamp())+2700, payment_created_at=self.now(),
                erklaerungen_json=declarations, payment_status="unbezahlt" if row["zahlungsart"] == "online" else "vor_ort_offen"):
            self.fail(row["id"])
            return
        if row["zahlungsart"] == "vor_ort":
            return self._reconcile(row["id"])
        try:
            current = self.s.row(row["id"], private=False)
            payment = self.app.config["REIFEN_KAUF_GATEWAY"].create(self.s.order(current))
            if not self.s.cas(current, state="zahlung_offen", payment_id=payment["id"], payment_url=payment["url"], payment_status="offen"):
                raise ValueError()
            self.change(self.record(row["id"]), "waiting_authorization")
        except Exception:
            self.fail(row["id"])
            raise AutoBeschaffungFehler() from None

    def fail(self, order_id, state="unknown"):
        record = self.record(order_id)
        if record:
            self.change(record, state)
        row = self.s.row(order_id, private=False)
        if row["state"] not in {"bestaetigt", "bestellt_zahlung_offen"}:
            self.s.cas(row, state="pruefung_noetig")

    def cancel(self, row, record):
        if row["zahlungsart"] == "online":
            result = self.app.config["REIFEN_KAUF_GATEWAY"].cancel_authorization(self.s.order(row))
            self.payment_binding(result, self.s.order(row), intent=True)
            if (result.get("status") != "canceled" or result.get("payment_status") != "canceled"
                    or result.get("amount_received") != 0 or result.get("amount_capturable") != 0):
                raise AutoBeschaffungFehler()
        self.change(record, "rejected")
        self.s.cas(self.s.row(row["id"], private=False), state="nicht_angenommen", payment_status="autorisation_aufgehoben" if row["zahlungsart"] == "online" else "unbezahlt")

    def receipt(self, record, order):
        value = json.loads(record["receipt_json"])
        request = json.loads(record["request_json"])
        if (request_sha256(request) != record["request_sha256"]
                or freeze_order_request(request).as_dict() != request
                or request.get("reference") != order["id"] or request.get("produktart") != order["produktart"]
                or request.get("artikel_id") != order["artikel_id"] or request.get("ean") != record["snapshot"]["ean"]
                or type(request.get("anzahl")) is not int or request["anzahl"] != order["anzahl"]
                or request.get("delivery_address") != record["snapshot"]["policy"]["delivery_address"]
                or sha(value) != record["receipt_sha256"] or value.get("bestaetigt") is not True
                or value.get("receiptbinding_source") != "persisted_request"
                or value.get("configured_mode") != record["snapshot"]["mode"]
                or value.get("account_fingerprint") != record["snapshot"]["policy"]["account_fingerprint"]
                or any(value.get(key) != request.get(key) for key in ("reference", "produktart", "artikel_id", "ean", "anzahl", "einkauf_netto", "waehrung"))
                or not isinstance(value.get("bestellnummer"), str) or not re.fullmatch(r"[1-9][0-9]{0,29}", value["bestellnummer"])):
            raise AutoBeschaffungFehler()
        return value

    def pending_ids(self, limit=20):
        if type(limit) is not int or not 1 <= limit <= 20:
            raise ValueError("Ungültiges Abgleichslimit.")
        db = self.p.get_db()
        try:
            return [row["order_id"] for row in db.execute("SELECT a.order_id FROM reifen_tyresystem_auto a "
                "JOIN reifen_kaeufe k ON k.id=a.order_id WHERE a.state IN "
                "('authorizing','waiting_authorization','authorized','sending','acknowledged','capturing','capture_unknown') "
                "AND k.mode=? ORDER BY a.updated_at LIMIT ?", (self.app.config.get("REIFEN_KAUF_MODE"),limit)).fetchall()]
        finally:
            db.close()

    def reconcile(self, order_id):
        return self._single(order_id, self._reconcile, order_id)

    @staticmethod
    def payment_binding(payment, order, *, intent=False):
        if (not isinstance(payment, Mapping) or payment.get("reference") != order["id"]
                or payment.get("quote_hash") != order["quote_hash"]
                or payment.get("livemode") is not (order["mode"] == "live")
                or payment.get("currency") != "eur" or type(payment.get("amount_total")) is not int
                or payment["amount_total"] != order["amount_total"]
                or payment.get("payment_capture_method") != "manual"
                or not isinstance(payment.get("payment_intent"), str)
                or not re.fullmatch(r"pi_[A-Za-z0-9]{1,200}", payment["payment_intent"])
                or order.get("payment_intent") and payment["payment_intent"] != order["payment_intent"]
                or payment.get("id") != (order["payment_intent"] if intent else order["payment_id"])):
            raise AutoBeschaffungFehler()

    def _reconcile(self, order_id):
        row, record = self.s.row(order_id, private=False), self.record(order_id)
        if record["state"] in {"complete", "rejected", "unknown", "draft"}:
            return
        if record["state"] == "authorizing":
            self.fail(order_id)
            return
        order = self.bound(row, record)
        try:
            if record["state"] == "sending":
                # The supplier journal may have committed its ACK just before
                # a crash. This is a READ ONLY recovery, never submit_once().
                request = json.loads(record["request_json"])
                receipt = self.app.config["REIFEN_TYRESYSTEM_ORDERS"].acknowledged(order_id,request)
                if receipt is None:
                    self.fail(order_id)
                    return
                candidate = {**record,"receipt_json":canonical(receipt),"receipt_sha256":sha(receipt)}
                self.receipt(candidate,order)
                if not self.change(record,"acknowledged",receipt_json=candidate["receipt_json"],receipt_sha256=candidate["receipt_sha256"]):
                    return
                record = self.record(order_id)
            if row["state"] in {"bestaetigt","bestellt_zahlung_offen"}:
                receipt = self.receipt(record,order)
                if (row["supplier_order_id"] != receipt["bestellnummer"] or not row["accepted_at"]
                        or row["zahlungsart"] == "online" and row["payment_status"] != "bezahlt"):
                    raise AutoBeschaffungFehler()
                self.change(record,"complete")
                return
            if record["state"] in {"waiting_authorization", "authorized"}:
                if row["zahlungsart"] == "online":
                    payment = self.app.config["REIFEN_KAUF_GATEWAY"].retrieve(row["payment_id"], expected_order=order)
                    if payment.get("status") in {"open", "processing"}:
                        return
                    self.payment_binding(payment, order)
                    if (payment.get("status") != "authorized" or payment.get("payment_status") != "authorized"
                            or type(payment.get("amount_capturable")) is not int or payment["amount_capturable"] != order["amount_total"]
                            or type(payment.get("amount_received")) is not int or payment["amount_received"] != 0 or type(payment.get("capture_before")) is not int
                            or payment["capture_before"] <= int(self.s.now().timestamp())+60):
                        raise AutoBeschaffungFehler()
                    if not self.s.cas(row, payment_intent=payment["payment_intent"], payment_status="authorized"):
                        return
                    row = self.s.row(order_id, private=False)
                    order = self.bound(row, record)
                try:
                    _, purchase = self.preflight(order["query"], order["produkt"], policy=record["snapshot"]["policy"])
                except AutoBeschaffungFehler:
                    self.cancel(row, record)
                    return
                request = freeze_order_request({"produktart":order["produktart"], "artikel_id":order["artikel_id"],
                    "ean":order["produkt"]["ean"], "anzahl":order["anzahl"], "einkauf_netto":purchase,
                    "waehrung":"EUR", "reference":order_id, "delivery_address":record["snapshot"]["policy"]["delivery_address"]}).as_dict()
                if not self.change(record, "sending", request_json=canonical(request), request_sha256=request_sha256(request)):
                    return
                scope = _ORDER.set((id(self.s),order_id,request_sha256(request),record["snapshot"]["mode"],
                                    record["snapshot"]["policy"]["account_fingerprint"]))
                try:
                    receipt = self.app.config["REIFEN_TYRESYSTEM_ORDERS"].submit_once(order_id, request)
                except Exception as error:
                    current = self.record(order_id)
                    if getattr(error, "outcome", None) == "rejected":
                        self.cancel(row, current)
                        return
                    self.fail(order_id)
                    raise AutoBeschaffungFehler() from None
                finally:
                    _ORDER.reset(scope)
                record = self.record(order_id)
                if record["state"] != "sending":
                    raise AutoBeschaffungFehler()
                candidate = {**record, "receipt_json":canonical(receipt), "receipt_sha256":sha(receipt)}
                self.receipt(candidate, order)
                if not self.change(record, "acknowledged", receipt_json=candidate["receipt_json"], receipt_sha256=candidate["receipt_sha256"]):
                    return
                record = self.record(order_id)
            if record["state"] in {"acknowledged", "capturing", "capture_unknown"}:
                receipt = self.receipt(record, order)
                if row["zahlungsart"] == "online":
                    if record["state"] != "capturing" and not self.change(record, "capturing"):
                        return
                    paid = self.app.config["REIFEN_KAUF_GATEWAY"].capture(self.s.order(self.s.row(order_id, private=False)))
                    self.payment_binding(paid, self.s.order(self.s.row(order_id, private=False)), intent=True)
                    if (paid.get("status") != "succeeded" or paid.get("payment_status") != "paid"
                            or type(paid.get("amount_received")) is not int or paid["amount_received"] != order["amount_total"]
                            or type(paid.get("amount_capturable")) is not int or paid["amount_capturable"] != 0):
                        raise AutoBeschaffungFehler()
                    current = self.s.row(order_id, private=False)
                    if not self.s.cas(current, state="zahlung_offen", payment_status="bezahlt"):
                        raise AutoBeschaffungFehler()
                self.s.accept_auto_order(self.s.row(order_id, private=False), receipt)
                if self.s.row(order_id, private=False)["state"] not in {"bestaetigt", "bestellt_zahlung_offen"}:
                    raise AutoBeschaffungFehler()
                self.change(self.record(order_id), "complete")
        except Exception:
            if self.record(order_id)["state"] != "unknown":
                self.fail(order_id, "capture_unknown" if self.record(order_id)["state"] in {"acknowledged", "capturing"} else "unknown")
            raise AutoBeschaffungFehler() from None

    def authorize_inquiry(self, sku, quantity):
        scope = _INQUIRY.get()
        reader, orders = self.app.config.get("REIFEN_TYRESYSTEM_INQUIRY"), self.app.config.get("REIFEN_TYRESYSTEM_ORDERS")
        return (scope is not None and scope == (id(self.s), sku, quantity, getattr(reader, "configured_mode", None),
                                               getattr(orders, "account_fingerprint", None)))

    def authorize_order(self, envelope):
        try:
            if not isinstance(envelope, Mapping) or set(envelope) != {"reference", "request", "request_sha256", "configured_mode", "account_fingerprint"}:
                return False
            if _ORDER.get() != (id(self.s),envelope["reference"],envelope["request_sha256"],
                                envelope["configured_mode"],envelope["account_fingerprint"]):
                return False
            record = self.record(envelope["reference"])
            if not record or record["state"] != "sending":
                return False
            row = self.s.row(envelope["reference"], private=False)
            order = self.bound(row, record)
            request = freeze_order_request(envelope["request"]).as_dict()
            return (record["request_sha256"] == envelope["request_sha256"] == request_sha256(request)
                and canonical(request) == record["request_json"] and request["reference"] == row["id"]
                and envelope["configured_mode"] == row["mode"]
                and envelope["account_fingerprint"] == record["snapshot"]["policy"]["account_fingerprint"]
                and (row["zahlungsart"] == "vor_ort" or row["payment_status"] == "authorized" and bool(row["payment_intent"]))
                and bool(row["telefon_bestaetigt_am"]) and order["erklaerungen"].get("bestellpflicht") is True
                and order["erklaerungen"].get("bedingungen") is True)
        except Exception:
            return False
