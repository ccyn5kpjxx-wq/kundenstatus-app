"""Explicit ALZURA display quotes, independent from RSU purchasing approval.

Only gross public fields leave this adapter. Native readers are injected or
constructed from explicit private-root-contained paths; no env, token/file IO,
threads or network on import/construction. Actual licensed API-call budgeting
is enforced by the packaged native shared ledger (9900/month), not by a claim
that calls are free. These quotes never confirm RSU procurement or fitment.
"""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re

from shop_catalog_public import variant_fingerprint

SID = re.compile(r"(?:tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}")
WHEELS = {"tyre", "alloy_wheel", "steel_wheel"}
PARTS = {"body_part", "oil", "care", "accessory"}
PATHS = {"wheel_snapshot", "parts_snapshot", "token_file", "ledger_path", "raw_root",
         "wheel_cache_path", "parts_cache_path", "wheel_output_root", "parts_output_root"}


class AlzuraDisplayError(ValueError):
    pass


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise AlzuraDisplayError("invalid_display_timestamp") from None


class AlzuraDisplayQuoteProvider:
    def __init__(self, products, *, approved_ids, mode, wheel_quotes=None, parts_quotes=None,
                 native_ids=None, enabled=False, reads_approved=False,
                 unit1_parts_contract_confirmed=False, clock=None):
        if (not isinstance(products, (list, tuple)) or len(products) > 100_000
                or not isinstance(approved_ids, (set, frozenset)) or len(approved_ids) > 100_000
                or any(not isinstance(pid, str) or not SID.fullmatch(pid) for pid in approved_ids)
                or mode not in {"test", "live"} or type(enabled) is not bool or type(reads_approved) is not bool
                or type(unit1_parts_contract_confirmed) is not bool
                or wheel_quotes is not None and not callable(wheel_quotes)
                or parts_quotes is not None and not callable(parts_quotes)
                or native_ids is not None and not isinstance(native_ids, Mapping)):
            raise AlzuraDisplayError("invalid_display_configuration")
        self._products = {}
        aliases = native_ids or {}
        for row in products:
            if not isinstance(row, Mapping):
                raise AlzuraDisplayError("invalid_display_product")
            pid = row.get("selection_id", row.get("id"))
            if not isinstance(pid, str):
                raise AlzuraDisplayError("invalid_display_product")
            if pid not in approved_ids:
                continue
            category, native = row.get("category"), aliases.get(pid, pid)
            if (pid in self._products or category not in WHEELS | PARTS
                    or not isinstance(native, str) or not SID.fullmatch(native)
                    or not native.startswith((category if category in WHEELS else "parts") + ":")):
                raise AlzuraDisplayError("invalid_display_identity")
            self._products[pid] = {"id": pid, "identity_hash": variant_fingerprint(row), "category": category,
                                   "native_id": native, "review_pending": row.get("price_status") == "review_pending"}
        if set(self._products) != set(approved_ids):
            raise AlzuraDisplayError("released_product_missing")
        self._wheel, self._parts, self._mode = wheel_quotes, parts_quotes, mode
        self._enabled, self._approved = enabled, reads_approved
        self._parts_contract = unit1_parts_contract_confirmed
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def __repr__(self):
        return "<AlzuraDisplayQuoteProvider display_only gross_fields_only>"

    def retail_quote(self, product, aggregate_quantity):
        if not self._enabled or not self._approved or not isinstance(product, Mapping):
            return None
        pid = product.get("id")
        if not isinstance(pid, str):
            return None
        selected = self._products.get(pid)
        if (selected is None or selected["identity_hash"] != product.get("identity_hash")
                or selected["category"] != product.get("category") or selected["review_pending"]
                or type(aggregate_quantity) is not int):
            return None
        wheel = selected["category"] in WHEELS
        # Native wheel quotes contain actual1..4 tiers. Native parts' reviewed
        # public contract proves a one-SKU-unit quote only, not unseen tiers.
        if not 1 <= aggregate_quantity <= (4 if wheel else 1) or not wheel and not self._parts_contract:
            return None
        callback = self._wheel if wheel else self._parts
        if callback is None:
            return None
        try:
            result = callback([selected["native_id"]])
            quote = result.get("quotes", {}).get(selected["native_id"]) if isinstance(result, Mapping) else None
            if not isinstance(quote, Mapping):
                return None
            now, observed, expires = _stamp(self._clock()), _stamp(quote.get("observed_at")), _stamp(quote.get("expires_at"))
            if not timedelta(0) < expires - observed <= timedelta(minutes=15) or not observed <= now < expires:
                return None
            if wheel:
                if quote.get("source") != "ALZURA":
                    return None
                tier = quote.get("prices_by_quantity", {}).get(str(aggregate_quantity))
                if (not isinstance(tier, Mapping) or type(tier.get("quantity")) is not int
                        or tier["quantity"] != aggregate_quantity or tier.get("vat_percent") != "19.00"
                        or type(tier.get("items_gross_cent")) is not int
                        or type(tier.get("unit_gross_cent")) is not int
                        or tier["items_gross_cent"] != aggregate_quantity * tier["unit_gross_cent"]):
                    return None
                unit, stock = tier["unit_gross_cent"], quote.get("stock_reported")
            else:
                unit, stock = quote.get("unit_gross_cent"), quote.get("stock")
            if type(unit) is not int or not 1 <= unit <= 10_000_000 or type(stock) is not int or not aggregate_quantity <= stock <= 1_000_000:
                return None
            public = {"confirmed": True, "source": "ALZURA", "display_only": True,
                "procurement_confirmed": False, "product_id": pid, "identity_hash": selected["identity_hash"],
                "quantity": aggregate_quantity, "mode": self._mode, "currency": "eur", "vat_percent": "19.00",
                "unit_gross_cent": unit, "stock": stock, "observed_at": observed.isoformat(), "expires_at": expires.isoformat()}
            if (not wheel and quote.get("base_price_unit") == "L" and type(quote.get("base_price_gross_cent")) is int
                    and 1 <= quote["base_price_gross_cent"] <= 10_000_000):
                public.update(base_price_unit="L", base_price_gross_cent=quote["base_price_gross_cent"])
            return public
        except Exception:
            return None


def provider_factory(bundle, config, private_root):
    """Construct explicit native services and a display adapter, without reads.

    bundle: products + released_ids (or publication.released_ids).
    config: mode/enabled/reads_approved, all PATHS, optional native_ids.
    Every path resolves inside private_root; no implicit credential fallback.
    Cache configuration: source='ALZURA', require_procurement=False. Native
    ledger permits at most9900 actual calls/month shared across both services.
    """
    try:
        if not isinstance(bundle, Mapping) or not isinstance(config, Mapping) or not PATHS <= config.keys():
            raise ValueError()
        root = Path(private_root)
        if not root.is_absolute() or root.is_symlink():
            raise ValueError()
        root = root.resolve()
        paths = {}
        for name in PATHS:
            raw = config[name]
            if not isinstance(raw, (str, Path)):
                raise ValueError()
            path = Path(raw)
            path = path if path.is_absolute() else root / path
            resolved = path.resolve()
            if (path.is_symlink() or resolved == root or not resolved.is_relative_to(root)
                    or any(part.casefold() in {"static", "public", "templates", "outputs", "wwwroot"} for part in resolved.parts)):
                raise ValueError()
            paths[name] = resolved
        released = bundle.get("released_ids")
        if released is None:
            publication = bundle.get("publication")
            released = publication.get("released_ids") if isinstance(publication, Mapping) else None
        if not isinstance(released, (list, tuple, set, frozenset)):
            raise ValueError()
        from shop_partner_native.shop_live_quotes import QuoteService
        from shop_partner_native.shop_parts_provider import PartsQuoteService
        from shop_partner_native import alzura_readonly_catalog as native
        from shop_alzura_storage import LiveReadRetention
        wheel_runs=paths["wheel_output_root"] / 'shop-live-display-runs'
        parts_runs=paths["parts_output_root"] / 'shop-live-display-runs'
        retention=LiveReadRetention([wheel_runs,parts_runs])
        reader=retention.reader_factory(native.ReadOnlyCatalogClient,
            reserve_bytes=native.MAX_RESPONSE_BYTES+65536)
        wheel = QuoteService(snapshot_path=paths["wheel_snapshot"], token_file=paths["token_file"],
            ledger_path=paths["ledger_path"], cache_path=paths["wheel_cache_path"], raw_root=paths["raw_root"],
            output_root=wheel_runs,reader_factory=reader)
        parts = PartsQuoteService(catalog_path=paths["parts_snapshot"], token_file=paths["token_file"],
            ledger_path=paths["ledger_path"], cache_path=paths["parts_cache_path"],
            output_root=parts_runs,reader_factory=reader)
        return AlzuraDisplayQuoteProvider(bundle["products"], approved_ids=set(released), mode=config["mode"],
            enabled=config.get("enabled") is True, reads_approved=config.get("reads_approved") is True,
            wheel_quotes=wheel.get_quotes, parts_quotes=parts.get_quotes, native_ids=config.get("native_ids"),
            unit1_parts_contract_confirmed=True)
    except Exception:
        raise AlzuraDisplayError("explicit_private_display_configuration_required") from None
