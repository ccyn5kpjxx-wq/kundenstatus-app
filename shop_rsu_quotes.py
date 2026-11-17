"""Explicitly approved exact-SKU bindings -> fresh read-only RSU retail quotes.

Construction does not read credentials, environment, files or the network.
Candidates are not approval. Caller-owned approved_ids bind reviewed catalogue
identities to source proofs and expected EANs. Policy establishes EUR/net-unit
semantics externally; configured_mode is not a supplier-reported server mode.
No supplier POST, reservation, payment, capture or freight pricing exists here.
"""
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
import re
import threading

from shop_cart_quotes import retail_unit_gross_cent
from shop_rsu_match import CORE_FIELDS, FEATURES
from tyresystem_api import ArticleInquiry, INQUIRY_ORIGIN, TyreSystemInquiryClient


PUBLIC_ID = re.compile(r"(tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}")
HASH = re.compile(r"[a-f0-9]{64}")
ARTICLE = re.compile(r"[1-9][0-9]{0,17}")
EVIDENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/ -]{0,159}")
TTL = timedelta(minutes=15)
FAILURE_COOLDOWN = timedelta(seconds=30)
MAX_BINDINGS = 100_000
MAX_CACHE_ENTRIES = 256
_PRODUCT_CATEGORIES = {"tyre", "alloy_wheel", "steel_wheel", "body_part", "oil", "care", "accessory"}


class RSUQuoteError(ValueError):
    """Fixed local codes, never exceptions from suppliers/transports/config."""


@dataclass(frozen=True)
class RSUQuotePolicy:
    mode: str
    currency: str | None = None
    net_unit_price_confirmed: bool = False
    evidence_id: str = ""


def _hash(value):
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _stamp(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise RSUQuoteError("invalid_clock")
    return value.astimezone(timezone.utc)


def _ean(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{13}", value) or set(value) == {"0"}:
        return False
    total = sum(int(c) * (3 if i % 2 == 0 else 1) for i, c in enumerate(reversed(value[:-1])))
    return (10 - total % 10) % 10 == int(value[-1])


def _reference(value):
    return isinstance(value, str) and EVIDENCE.fullmatch(value) and "://" not in value


def _binding(identifier, value):
    """Verify the exact inspected matcher proof, not its candidate flag alone."""
    try:
        if (not isinstance(identifier, str) or not PUBLIC_ID.fullmatch(identifier)
                or not isinstance(value, Mapping)
                or set(value) != {"identity_hash", "category", "mapping", "expected_ean"}
                or not isinstance(value["identity_hash"], str) or not HASH.fullmatch(value["identity_hash"])
                or value["category"] not in _PRODUCT_CATEGORIES or not _ean(value["expected_ean"])):
            raise ValueError()
        candidate = value["mapping"]
        proof = candidate["evidence"]
        if isinstance(proof, Mapping) and proof.get("rule") == "exact_ean_manufacturer_model_colour_rim_geometry_v1":
            from shop_supplier_catalog import validate_rim_mapping
            supplier = validate_rim_mapping(candidate, product_id=identifier,
                category=value["category"], expected_ean=value["expected_ean"])
            safe = {"product_id": identifier, "identity_hash": value["identity_hash"],
                    "category": value["category"], "expected_ean": value["expected_ean"], **supplier}
            return {**safe, "evidence_id": _hash(safe)}
        if (candidate.get("status") != "exact_sku_candidate"
                or set(proof) != {"rule", "manufacturer", "mpn", "core", "alzura", "rsu"}
                or proof["rule"] != "exact_manufacturer_mpn_and_full_tyre_core_v1"
                or value["category"] != "tyre" or not identifier.startswith("tyre:")
                or not isinstance(proof["manufacturer"], str) or not 1 <= len(proof["manufacturer"]) <= 300
                or not isinstance(proof["mpn"], str) or not 1 <= len(proof["mpn"]) <= 300
                or not isinstance(proof["core"], Mapping) or set(proof["core"]) != set(CORE_FIELDS)
                or any(not isinstance(v, str) or not 1 <= len(v) <= 100 for v in proof["core"].values())
                or _hash(proof) != candidate.get("evidence_id")):
            raise ValueError()
        identity = {"manufacturer": proof["manufacturer"], "mpn": proof["mpn"], "core": proof["core"]}
        for side, namespace in (("alzura", "ALZURA_GENERAL_EXPORT"), ("rsu", "RSU_EXPORT")):
            source = proof[side]
            if (not isinstance(source, Mapping) or set(source) != {"source_sha256", "row_number", "article_id",
                    "row_fingerprint", "identity_fingerprint", "features"}
                    or any(not isinstance(source[k], str) or not HASH.fullmatch(source[k])
                           for k in ("source_sha256", "row_fingerprint", "identity_fingerprint"))
                    or not isinstance(source["article_id"], str) or not ARTICLE.fullmatch(source["article_id"])
                    or type(source["row_number"]) is not int or not 1 <= source["row_number"] <= 300_000
                    or not isinstance(source["features"], Mapping) or set(source["features"]) != set(FEATURES)
                    or any(v is not None and type(v) is not bool for v in source["features"].values())
                    or source["identity_fingerprint"] != _hash(identity)):
                raise ValueError()
            row_proof = {"namespace": namespace, "source_sha256": source["source_sha256"],
                         "row_number": source["row_number"], "article_id": source["article_id"],
                         "identity": identity, "features": source["features"],
                         "conditions_clear": True, "eligible_category": True}
            if source["row_fingerprint"] != _hash(row_proof):
                raise ValueError()
        for feature in FEATURES:
            left, right = proof["alzura"]["features"][feature], proof["rsu"]["features"][feature]
            if left is not None and right is not None and left != right:
                raise ValueError()
        safe = {"product_id": identifier, "identity_hash": value["identity_hash"],
                "category": value["category"], "supplier_article_id": proof["rsu"]["article_id"],
                "expected_ean": value["expected_ean"], "mapping_evidence_id": candidate["evidence_id"],
                "alzura_source_sha256": proof["alzura"]["source_sha256"],
                "rsu_source_sha256": proof["rsu"]["source_sha256"]}
        return {**safe, "evidence_id": _hash(safe)}
    except Exception:
        raise RSUQuoteError("invalid_mapping_binding") from None


class XMLInquiryTransport:
    """Requests-like GET-only XML transport; creates a session on first GET.

    The existing Inquiry client still enforces its fixed origin, authorization,
    response/body/deadline checks. No cookie/proxy/retry state is inherited.
    ``session_factory`` exists solely for an injected trusted test transport.
    """
    def __init__(self, *, session_factory=None):
        self._factory, self._session = session_factory, None

    def get(self, url, **kwargs):
        if (not isinstance(url, str) or not re.fullmatch(
                re.escape(INQUIRY_ORIGIN) + r"/Rest/Inquiry/[1-9][0-9]{0,17}/(?:[1-9]|1[0-9]|20)", url)
                or kwargs.get("verify") is not True or kwargs.get("allow_redirects") is not False):
            raise RSUQuoteError("inquiry_transport_guard")
        if self._session is None:
            if self._factory is None:
                import requests
                session = requests.Session()
                session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))
            else:
                session = self._factory()
            session.trust_env = False
            self._session = session
        headers = dict(kwargs.get("headers", {}))
        headers["Accept"] = "application/xml, text/xml"
        kwargs["headers"] = headers
        self._session.cookies.clear()
        return self._session.get(url, **kwargs)

    def close(self):
        if self._session is not None:
            self._session.close()
            self._session = None


class RSUQuoteProvider:
    """Two injectable CartQuoteService providers; no public request handler."""
    def __init__(self, *, bindings, approved_ids, policy, inquiry, clock=None,
                 max_cache_entries=MAX_CACHE_ENTRIES):
        if (not isinstance(bindings, Mapping) or len(bindings) > MAX_BINDINGS
                or not isinstance(approved_ids, (set, frozenset)) or len(approved_ids) > MAX_BINDINGS
                or any(not isinstance(v, str) or not PUBLIC_ID.fullmatch(v) for v in approved_ids)
                or not approved_ids <= bindings.keys()
                or not isinstance(policy, RSUQuotePolicy) or policy.mode not in {"test", "live"}
                or policy.currency not in {None, "EUR"} or type(policy.net_unit_price_confirmed) is not bool
                or policy.net_unit_price_confirmed and (policy.currency != "EUR" or not _reference(policy.evidence_id))
                or not callable(getattr(inquiry, "inquire", None))
                or type(max_cache_entries) is not int or not 1 <= max_cache_entries <= 1024):
            raise RSUQuoteError("invalid_provider_configuration")
        self._approved = frozenset(approved_ids)
        # Unapproved candidate records need not be loaded into a live process.
        self._bindings = {sid: _binding(sid, deepcopy(bindings[sid])) for sid in sorted(self._approved)}
        supplier_ids = [b["supplier_article_id"] for b in self._bindings.values()]
        if len(supplier_ids) != len(set(supplier_ids)):
            raise RSUQuoteError("duplicate_procurement_binding")
        self._policy, self._inquiry = policy, inquiry
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._cache, self._lock, self._limit = OrderedDict(), threading.RLock(), max_cache_entries

    def __repr__(self):
        return "<RSUQuoteProvider approved_bindings=private Inquiry_GET_only>"

    @classmethod
    def from_config(cls, *, config, bindings, approved_ids, policy, clock=None, transport=None, **kwargs):
        """Explicit supplied config only; never reads environment/secret files."""
        provider = cls(bindings=bindings, approved_ids=approved_ids, policy=policy,
                       inquiry=_PendingInquiry(), clock=clock, **kwargs)
        permitted = frozenset(b["supplier_article_id"] for b in provider._bindings.values())
        provider._inquiry = TyreSystemInquiryClient(config,
            authorize=lambda identifier, quantity: identifier in permitted and type(quantity) is int and 1 <= quantity <= 20,
            http=transport or XMLInquiryTransport(), clock=clock)
        if provider._inquiry.configured_mode != policy.mode:
            raise RSUQuoteError("configured_mode_mismatch")
        return provider

    def _lookup(self, product):
        if not isinstance(product, Mapping):
            return None
        identifier = product.get("id")
        if not isinstance(identifier, str):
            return None
        value = self._bindings.get(identifier)
        if (value is None or product.get("identity_hash") != value["identity_hash"]
                or product.get("category") != value["category"]):
            return None
        return value

    def rsu_mapping(self, product):
        value = self._lookup(product)
        return ({"confirmed": True, "product_id": value["product_id"], "identity_hash": value["identity_hash"],
                 "supplier": "RSU", "supplier_article_id": value["supplier_article_id"],
                 "evidence_id": value["evidence_id"]} if value else None)

    def fresh_inquiry(self, item, exact_quantity):
        """New private GET for an already frozen supplier position; no cache.

        This method is deliberately separate from public display prices. It
        returns the net amount only to the internal procurement saga.
        """
        if (not isinstance(item, Mapping) or not isinstance(item.get('id'), str)
                or type(exact_quantity) is not int or not 1 <= exact_quantity <= 20):
            return None
        binding = self._bindings.get(item.get('id'))
        mapping = item.get('mapping')
        if (binding is None or item.get('identity_hash') != binding['identity_hash']
                or not isinstance(mapping, Mapping) or mapping.get('evidence_id') != binding['evidence_id']
                or mapping.get('supplier_article_id') != binding['supplier_article_id']
                or self._policy.currency != 'EUR' or self._policy.net_unit_price_confirmed is not True):
            return None
        try:
            before = _stamp(self._clock())
            value = self._inquiry.inquire(binding['supplier_article_id'], exact_quantity)
            now = _stamp(self._clock())
            observed = _stamp(value.retrieved_at)
            if (not isinstance(value, ArticleInquiry) or value.article_id != binding['supplier_article_id']
                    or type(value.amount) is not int or value.amount != exact_quantity or value.ean != binding['expected_ean']
                    or value.configured_mode != self._policy.mode or not isinstance(value.price, Decimal)
                    or not Decimal('0') < value.price <= Decimal('100000')
                    or type(value.stock) is not int or not exact_quantity <= value.stock <= 1_000_000_000
                    or not before <= observed <= now < observed + TTL):
                return None
            return {'confirmed': True, 'product_id': binding['product_id'], 'identity_hash': binding['identity_hash'],
                    'supplier_article_id': binding['supplier_article_id'], 'mapping_evidence_id': binding['evidence_id'],
                    'expected_ean': binding['expected_ean'], 'ean': value.ean, 'quantity': exact_quantity,
                    'unit_net': format(value.price, 'f'), 'currency': 'EUR', 'mode': self._policy.mode,
                    'stock': value.stock, 'observed_at': observed.isoformat(), 'expires_at': (observed + TTL).isoformat()}
        except Exception:
            return None

    def _remember(self, key, quote, until):
        self._cache[key] = {"quote": deepcopy(quote), "until": until}
        self._cache.move_to_end(key)
        while len(self._cache) > self._limit:
            self._cache.popitem(last=False)

    def retail_quote(self, product, aggregate_quantity):
        binding = self._lookup(product)
        if (binding is None or type(aggregate_quantity) is not int or not 1 <= aggregate_quantity <= 20
                or self._policy.currency != "EUR" or self._policy.net_unit_price_confirmed is not True):
            return None
        key = (binding["evidence_id"], aggregate_quantity)
        with self._lock:
            now = _stamp(self._clock())
            cached = self._cache.get(key)
            if cached is not None and now < cached["until"]:
                quote = cached["quote"]
                if quote is None or datetime.fromisoformat(quote["observed_at"]) <= now:
                    self._cache.move_to_end(key)
                    return deepcopy(quote)
            try:
                value = self._inquiry.inquire(binding["supplier_article_id"], aggregate_quantity)
                current = _stamp(self._clock())
                if (not isinstance(value, ArticleInquiry) or value.article_id != binding["supplier_article_id"]
                        or type(value.amount) is not int or value.amount != aggregate_quantity
                        or value.ean != binding["expected_ean"] or not _ean(value.ean)
                        or value.configured_mode != self._policy.mode
                        or type(value.stock) is not int or not aggregate_quantity <= value.stock <= 1_000_000_000
                        or not isinstance(value.price, Decimal)):
                    raise RSUQuoteError("inquiry_not_confirmed")
                observed = _stamp(value.retrieved_at)
                # An injected transport cannot replay an old observation as a
                # newly retrieved quote; import/cache times do not refresh it.
                if not now <= observed <= current or current >= observed + TTL:
                    raise RSUQuoteError("inquiry_time_invalid")
                unit = retail_unit_gross_cent(value.price, binding["category"])
                quote = {"confirmed": True, "product_id": binding["product_id"],
                         "identity_hash": binding["identity_hash"], "quantity": aggregate_quantity,
                         "mode": self._policy.mode, "currency": "eur", "vat_percent": "19.00",
                         "unit_gross_cent": unit, "stock": value.stock, "observed_at": observed.isoformat(),
                         "expires_at": (observed + TTL).isoformat(), "procurement_confirmed": True}
                self._remember(key, quote, observed + TTL)
                return deepcopy(quote)
            except Exception:
                # No provider body, price, auth value or exception message is
                # retained/exposed. Cooldown also bounds repeated failed reads.
                self._remember(key, None, now + FAILURE_COOLDOWN)
                return None


class _PendingInquiry:
    def inquire(self, identifier, quantity):
        raise RSUQuoteError("inquiry_not_configured")
