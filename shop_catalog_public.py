"""Bounded retail catalogue projection, independent of providers and filesystem.

The caller supplies normalized rows, an explicit publication allowlist and any
released media URLs. Raw provider identities/evidence remain with the caller.
This module does not reserve stock, approve checkout or infer vehicle fitment.
"""
from collections.abc import Iterable, Mapping
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from html.parser import HTMLParser
import json
import re
import unicodedata

MAX_PRODUCTS = 100_000
MAX_PAGE_SIZE = 48
MAX_QUOTE_AGE = timedelta(minutes=15)
MAX_PUBLISHED_PRICE_AGE = timedelta(days=7)
CATEGORIES = frozenset({"tyre", "alloy_wheel", "steel_wheel", "body_part", "oil", "care", "accessory"})
DEPARTMENTS = frozenset({"tyres", "wheels", "parts", "accessories"})
PUBLIC_ID = re.compile(r"(tyre|alloy_wheel|steel_wheel|parts):[a-f0-9]{24}")
SOURCE_PHOTO = re.compile(r"bilder/[a-f0-9]{64}\.(?:jpg|jpeg|png|webp)")
SPEC_LABELS = frozenset({"Farbe", "Ausführung", "Einpresstiefe", "Lochkreis", "Herstellerartikelnummer", "EAN"})
GEOMETRY_FIELDS = ("width", "ratio", "diameter", "offset", "pcd", "bolt_count", "centre_bore",
                   "load_index", "speed_index", "season", "runflat")


class CatalogError(ValueError):
    """Fixed reason codes; never include an input row, query or provider value."""


class _Plain(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.hidden = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "iframe", "object"}:
            self.hidden += 1
        elif not self.hidden:
            self.parts.append(" ")

    def handle_endtag(self, tag):
        if tag in {"script", "style", "iframe", "object"} and self.hidden:
            self.hidden -= 1
        elif not self.hidden:
            self.parts.append(" ")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def _text(value, maximum=180):
    if not isinstance(value, str):
        return ""
    parser = _Plain()
    try:
        parser.feed(value[:10_000])
        return " ".join("".join(c for c in "".join(parser.parts)
                                 if ord(c) >= 32 and ord(c) != 127).split())[:maximum]
    except Exception:
        return ""


def _fold(value):
    text = unicodedata.normalize("NFKD", value.casefold())
    return "".join(c for c in text if not unicodedata.combining(c))


def _condition(value):
    # Conditions are safety constraints, not marketing HTML. Never shorten an
    # exclusion into an apparent approval. JSON consumers must render as text.
    if not isinstance(value, str) or not 1 <= len(value) <= 1200:
        return None
    result = " ".join("".join(c for c in value if ord(c) >= 32 and ord(c) != 127).split())
    return result if 1 <= len(result) <= 300 else None


def _stamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        if not isinstance(result, datetime) or result.tzinfo is None or result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise CatalogError("invalid_timestamp") from None


def _integer(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def _public_path(value, extensions):
    if (not isinstance(value, str) or len(value) > 250 or not value.startswith("/")
            or value.startswith("//") or not re.fullmatch(r"/[A-Za-z0-9_./-]+", value)
            or any(p in {"", ".", ".."} for p in value[1:].split("/"))):
        return None
    return value if value.rsplit(".", 1)[-1].lower() in extensions else None


def _specifications(row):
    values = {}
    details = row.get("details")
    if isinstance(details, Mapping):
        values.update({label: _text(details.get(label), 140) for label in SPEC_LABELS})
    fields = row.get("detail_fields")
    if isinstance(fields, list):
        for field in fields[:40]:
            if (isinstance(field, Mapping) and isinstance(field.get("label"), str)
                    and field["label"] in SPEC_LABELS):
                values[field["label"]] = _text(field.get("value"), 140)
    return {key: value for key, value in sorted(values.items()) if value}


def _geometry(row):
    return {key: _text(str(row[key]), 40) for key in GEOMETRY_FIELDS
            if key in row and type(row[key]) in (str, int) and _text(str(row[key]), 40)}


def _size_filter(value, minimum, maximum):
    if value is None:
        return None
    if (type(value) not in (str, int) or not re.fullmatch(r'[1-9][0-9]{0,2}', str(value))
            or not minimum <= int(value) <= maximum):
        raise CatalogError('invalid_size_filter')
    return int(value)


def _size_value(value):
    # Numeric source values only. Do not infer a size from a title/description.
    if type(value) not in (str, int) or not re.fullmatch(r'[0-9]{1,3}(?:\.0{1,4})?', str(value)):
        return None
    return int(str(value).split('.')[0])


def variant_fingerprint(row):
    """Bind reviewed fitment to an exact public variant, never just its design."""
    if not isinstance(row, Mapping):
        raise CatalogError("invalid_product")
    identity = {"id": row.get("selection_id", row.get("id")), "category": row.get("category"),
                "name": _text(row.get("name")), "size": _text(row.get("size"), 120),
                "brand": _text(row.get("manufacturer_name") or row.get("brand"), 90),
                "article_number": _text(row.get("article_number"), 100),
                "geometry": _geometry(row),
                "specifications": _specifications(row)}
    return sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _quote(row, *, allow_published_prices=False):
    """Copy canonical EUR/19% retail cents; freshness is checked on every read.

    The normalized local wheel/parts contracts establish EUR/19% even when
    those optional envelope fields are absent. Explicit conflicts are never
    converted or relabelled as a different currency/tax treatment.
    """
    wheel = row.get("category") in {"tyre", "alloy_wheel", "steel_wheel"}
    tiered = wheel or isinstance(row.get('quote_public'), Mapping)
    source = row.get("quote_public") if tiered else row.get("price")
    if not isinstance(source, Mapping) or row.get("price_status") == "review_pending":
        return None
    published = source.get('kind') == 'merchant_pricebook'
    if published and (allow_published_prices is not True or source.get('source') != 'RSU'):
        return None
    if tiered and source.get("source") not in {"ALZURA", "RSU"}:
        return None
    if ("currency" in source and source["currency"] not in ("EUR", "eur")
            or "vat_percent" in source and source["vat_percent"] != "19.00"):
        return None
    try:
        observed, expires = _stamp(source.get("observed_at")), _stamp(source.get("expires_at"))
    except CatalogError:
        return None
    stock = source.get("stock_reported") if tiered else source.get("stock")
    maximum_age = MAX_PUBLISHED_PRICE_AGE if published else MAX_QUOTE_AGE
    if not timedelta(0) < expires - observed <= maximum_age or not _integer(stock, 0, 1_000_000):
        return None
    prices = {}
    if tiered:
        supplied = source.get("prices_by_quantity")
        if not isinstance(supplied, Mapping):
            return None
        maximum_quantity = 4 if wheel else 20
        for quantity in range(1, maximum_quantity + 1):
            line = supplied.get(str(quantity))
            if (not isinstance(line, Mapping) or type(line.get("quantity")) is not int
                    or line["quantity"] != quantity or line.get("vat_percent") != "19.00"
                    or "currency" in line and line["currency"] not in ("EUR", "eur")
                    or not _integer(line.get("unit_gross_cent"), 1, 10_000_000)
                    or not _integer(line.get("items_gross_cent"), 1, maximum_quantity * 10_000_000)
                    or line["items_gross_cent"] != quantity * line["unit_gross_cent"]):
                continue
            prices[str(quantity)] = {"quantity": quantity, "unit_gross_cent": line["unit_gross_cent"],
                                     "items_gross_cent": line["items_gross_cent"]}
    elif _integer(source.get("unit_gross_cent"), 1, 10_000_000):
        unit = source["unit_gross_cent"]
        prices = {str(q): {"quantity": q, "unit_gross_cent": unit, "items_gross_cent": q * unit}
                  for q in range(1, 21)}
    if not prices:
        return None
    result = {"currency": "EUR", "vat_percent": "19.00", "observed_at": observed.isoformat(),
              "expires_at": expires.isoformat(), "stock_reported": stock, "prices_by_quantity": prices}
    if published:
        try:
            review_at = _stamp(source.get('review_at'))
            stock_observed, stock_expires = _stamp(source.get('stock_observed_at')), _stamp(source.get('stock_expires_at'))
            if (review_at != observed + timedelta(days=5)
                    or not observed <= stock_observed < stock_expires <= stock_observed + MAX_QUOTE_AGE):
                return None
        except CatalogError:
            return None
        result.update(source='RSU', kind='merchant_pricebook', review_at=review_at.isoformat(), stock_observed_at=stock_observed.isoformat(),
                      stock_expires_at=stock_expires.isoformat())
    if (not wheel and source.get("base_price_unit") == "L"
            and _integer(source.get("base_price_gross_cent"), 1, 10_000_000)):
        result.update(base_price_unit="L", base_price_gross_cent=source["base_price_gross_cent"])
    return result


class PublicCatalog:
    """Immutable publication snapshot; outputs are detached JSON-safe copies.

    ``released_ids`` is an explicit publication decision. Media/document URLs
    must be same-origin released paths supplied by the integration layer.
    ``verified_fitments`` is separate from unreviewed provider vehicle hints.
    """
    def __init__(self, products: Iterable, *, released_ids, image_urls=None,
                 document_urls=None, verified_fitments=None, clock=None, quote_lookup=None,
                 quote_lookup_authoritative=False, allow_published_prices=False):
        if (isinstance(products, (str, bytes, Mapping)) or not isinstance(products, Iterable)
                or isinstance(released_ids, (str, bytes)) or not isinstance(released_ids, (set, frozenset, list, tuple))
                or len(released_ids) > MAX_PRODUCTS
                or any(not isinstance(v, str) or not PUBLIC_ID.fullmatch(v) for v in released_ids)):
            raise CatalogError("invalid_publication_input")
        released = frozenset(released_ids)
        images = image_urls if isinstance(image_urls, Mapping) else {}
        documents = document_urls if isinstance(document_urls, Mapping) else {}
        verified = verified_fitments if isinstance(verified_fitments, Mapping) else {}
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        if quote_lookup is not None and not callable(quote_lookup):
            raise CatalogError('invalid_quote_lookup')
        if type(quote_lookup_authoritative) is not bool or type(allow_published_prices) is not bool:
            raise CatalogError('invalid_quote_lookup')
        # This callback reads an already populated public-price cache only;
        # it must never perform supplier IO in the HTTP browsing thread.
        self._quote_lookup = quote_lookup
        self._quote_lookup_authoritative = quote_lookup_authoritative
        self._allow_published_prices = allow_published_prices
        self._rows = {}
        for count, row in enumerate(products, 1):
            if count > MAX_PRODUCTS:
                raise CatalogError("catalogue_too_large")
            if not isinstance(row, Mapping):
                continue
            sid = row.get("selection_id", row.get("id"))
            category = row.get("category")
            if (not isinstance(sid, str) or sid not in released or not PUBLIC_ID.fullmatch(sid)
                    or not isinstance(category, str) or category not in CATEGORIES or row.get("dimensions_valid") is False):
                continue
            prefix = category if category in {"tyre", "alloy_wheel", "steel_wheel"} else "parts"
            if not sid.startswith(prefix + ":") or not _text(row.get("name")):
                continue
            if sid in self._rows:
                raise CatalogError("duplicate_product_id")
            if category == "tyre":
                department = "tyres"
            elif category in {"alloy_wheel", "steel_wheel"}:
                department = "wheels"
            else:
                department = row.get("department")
                if not isinstance(department, str) or department not in {"parts", "accessories"}:
                    department = "parts" if category == "body_part" else "accessories"
            product = {"id": sid, "category": category, "department": department,
                       "name": _text(row.get("name")), "brand": _text(row.get("manufacturer_name") or row.get("brand"), 90),
                       "size": _text(row.get("size"), 120), "description": _text(row.get("description"), 2000),
                       "article_number": _text(row.get("article_number"), 100),
                       "unit_label": _text(row.get("unit_label"), 50) or "Stück", "specifications": _specifications(row),
                       "geometry": _geometry(row),
                       "images": [], "fitment": {"status": "not_confirmed", "vehicles": [], "reports": []}}
            photo = row.get("photo")
            if isinstance(photo, str) and SOURCE_PHOTO.fullmatch(photo):
                url = _public_path(images.get(photo), {"jpg", "jpeg", "png", "webp"})
                if url:
                    product["images"].append(url)
            evidence = verified.get(sid)
            product["fitment"] = self._fitment(row, evidence, documents)
            self._rows[sid] = {"public": product, "quote": _quote(row, allow_published_prices=self._allow_published_prices),
                               "search": _fold(" ".join([product["name"], product["brand"], product["size"],
                                                            product["article_number"], *product["specifications"].values()]))}

    @staticmethod
    def _fitment(row, evidence, documents):
        blank = {"status": "not_confirmed", "vehicles": [], "reports": []}
        if (not isinstance(evidence, Mapping) or evidence.get("reviewed") is not True
                or evidence.get("variant_fingerprint") != variant_fingerprint(row)):
            return blank
        keys = evidence.get("report_keys")
        vehicles = evidence.get("vehicles")
        if (not isinstance(keys, list) or not 1 <= len(keys) <= 12
                or any(not isinstance(key, str) for key in keys)
                or not isinstance(vehicles, list) or not 1 <= len(vehicles) <= 128):
            return blank
        reports = [_public_path(documents.get(key), {"pdf"}) for key in keys]
        if not all(reports):
            return blank
        approved = []
        for vehicle in vehicles:
            if not isinstance(vehicle, Mapping):
                return blank
            make, model = _text(vehicle.get("make"), 80), _text(vehicle.get("model"), 120)
            start, end = vehicle.get("year_from"), vehicle.get("year_to")
            known_years = _integer(start, 1900, 2100) and _integer(end, start, 2100)
            hsn, tsn = vehicle.get("hsn", ""), vehicle.get("tsn", "")
            conditions = vehicle.get("conditions", [])
            if (not make or not model or not (known_years or start is None and end is None)
                    or not isinstance(hsn, str) or not isinstance(tsn, str)
                    or bool(hsn) != bool(tsn) or (hsn and (not re.fullmatch(r"[0-9]{4}", hsn) or not re.fullmatch(r"[A-Z0-9]{3}", tsn)))
                    or not isinstance(conditions, list) or len(conditions) > 20
                    or any(_condition(c) is None for c in conditions)):
                return blank
            approved.append({"make": make, "model": model, "year_from": start, "year_to": end,
                             "hsn": hsn, "tsn": tsn, "conditions": [_condition(c) for c in conditions]})
        return {"status": "reviewed_with_conditions", "vehicles": approved, "reports": list(dict.fromkeys(reports))}

    def _now(self, supplied):
        return _stamp(self._clock() if supplied is None else supplied)

    def _cached_entry(self, entry):
        if self._quote_lookup is None:
            return {**entry, 'quote': None} if self._quote_lookup_authoritative else entry
        try:
            value = self._quote_lookup(entry['public']['id'])
            quote = _quote({'category': entry['public']['category'], 'quote_public': value}, allow_published_prices=self._allow_published_prices)
            return {**entry, 'quote': quote if self._quote_lookup_authoritative else quote or entry['quote']}
        except Exception:
            return {**entry, 'quote': None} if self._quote_lookup_authoritative else entry

    @staticmethod
    def _project(entry, now, *, detail=True):
        product = deepcopy(entry["public"])
        if not detail:
            product["description"] = product["description"][:280]
            fitment = product["fitment"]
            fitment["more_vehicles"] = max(0, len(fitment["vehicles"]) - 3)
            fitment["vehicles"] = fitment["vehicles"][:3]
            for vehicle in fitment["vehicles"]:
                vehicle["more_conditions"] = max(0, len(vehicle["conditions"]) - 3)
                vehicle["conditions"] = vehicle["conditions"][:3]
        quote = entry["quote"]
        product.update(price_status="update_required", price=None, order_enabled=False)
        if quote and _stamp(quote["observed_at"]) <= now < _stamp(quote["expires_at"]):
            product.update(price_status="current", price=deepcopy(quote))
            if quote.get('kind') == 'merchant_pricebook':
                product['price']['stock_current'] = _stamp(quote['stock_observed_at']) <= now < _stamp(quote['stock_expires_at'])
        return product

    def get(self, product_id, *, now=None):
        if not isinstance(product_id, str) or not PUBLIC_ID.fullmatch(product_id):
            raise CatalogError("invalid_product_id")
        current = self._now(now)
        entry = self._rows.get(product_id)
        return self._project(self._cached_entry(entry), current) if entry else None

    def _browse_order(self, entry):
        return (_fold(entry['public']['name']), entry['public']['id'])

    def _offer_rank(self, entry, now):
        """Local readable-offer priority, never an order/stock authorization.

        A fixed merchant price remains readable when its stock observation is
        older than the short Inquiry TTL. Checkout rechecks the actual amount.
        A reported zero never becomes positive through ranking.
        """
        quote = entry['quote']
        if not quote or not _stamp(quote['observed_at']) <= now < _stamp(quote['expires_at']):
            return 2
        return 0 if quote['stock_reported'] > 0 else 1

    def _ranked_order(self, entry, now):
        return (self._offer_rank(entry, now), *self._browse_order(entry))

    def _page_entries(self, *, page=1, page_size=24, q="", category=None, department=None, brand=None,
             make=None, model=None, year=None, hsn=None, tsn=None,
             width=None, ratio=None, diameter=None, now=None):
        if not _integer(page, 1, 10_000) or not _integer(page_size, 1, MAX_PAGE_SIZE):
            raise CatalogError("invalid_pagination")
        if not isinstance(q, str) or len(q) > 100 or len(q.split()) > 12:
            raise CatalogError("invalid_search")
        if (category is not None and (not isinstance(category, str) or category not in CATEGORIES)
                or department is not None and (not isinstance(department, str) or department not in DEPARTMENTS)):
            raise CatalogError("invalid_category")
        for value in (brand, make, model):
            if value is not None and (not isinstance(value, str) or not value.strip() or len(value) > 120):
                raise CatalogError("invalid_filter")
        if model is not None and make is None or year is not None and (make is None or model is None or not _integer(year, 1900, 2100)):
            raise CatalogError("invalid_vehicle_filter")
        if (hsn is None) != (tsn is None) or hsn is not None and (not isinstance(hsn, str) or not isinstance(tsn, str)
                or not re.fullmatch(r"[0-9]{4}", hsn) or not re.fullmatch(r"[A-Z0-9]{3}", tsn)):
            raise CatalogError("invalid_vehicle_filter")
        sizes = {'width': _size_filter(width, 100, 455), 'ratio': _size_filter(ratio, 10, 100),
                 'diameter': _size_filter(diameter, 8, 30)}
        # Only a complete standalone nominal size is structured. Brand names,
        # load/speed indices and all other free text keep the existing search.
        nominal = re.fullmatch(r'\s*([1-9][0-9]{2})\s*/\s*([1-9][0-9]{1,2})\s*[Rr]\s*([1-9][0-9]?)\s*', q)
        size_conflict = False
        if nominal:
            parsed = {'width': _size_filter(nominal[1], 100, 455), 'ratio': _size_filter(nominal[2], 10, 100),
                      'diameter': _size_filter(nominal[3], 8, 30)}
            size_conflict = any(sizes[k] is not None and sizes[k] != parsed[k] for k in sizes)
            sizes = {k: sizes[k] if sizes[k] is not None else parsed[k] for k in sizes}
        current = self._now(now)
        tokens = [] if nominal else _fold(q).split()
        rows = []
        for entry in self._rows.values():
            public = entry["public"]
            if size_conflict:
                continue
            if category is not None and public["category"] != category or department is not None and public["department"] != department:
                continue
            if brand is not None and _fold(public["brand"]) != _fold(brand.strip()):
                continue
            if (sizes['width'] is not None or sizes['ratio'] is not None) and public['category'] != 'tyre':
                continue
            if sizes['diameter'] is not None and public['category'] not in {'tyre', 'alloy_wheel', 'steel_wheel'}:
                continue
            if any(wanted is not None and _size_value(public['geometry'].get(key)) != wanted
                   for key, wanted in sizes.items()):
                continue
            if any(token not in entry["search"] for token in tokens):
                continue
            if any(value is not None for value in (make, model, year, hsn, tsn)):
                def matches(vehicle):
                    return ((make is None or _fold(vehicle["make"]) == _fold(make.strip()))
                            and (model is None or _fold(vehicle["model"]) == _fold(model.strip()))
                            and (year is None or type(vehicle["year_from"]) is int and type(vehicle["year_to"]) is int
                                 and vehicle["year_from"] <= year <= vehicle["year_to"])
                            and (hsn is None or (vehicle["hsn"], vehicle["tsn"]) == (hsn, tsn)))
                if not any(matches(v) for v in public["fitment"]["vehicles"]):
                    continue
            # One cache-only snapshot per matching product. Grouped/family
            # callers reuse this pass instead of rescanning every source page.
            rows.append(self._cached_entry(entry))
        rows.sort(key=lambda entry: self._ranked_order(entry, current))
        return rows, current

    def _page_result(self, rows, current, *, page, page_size):
        offset = (page - 1) * page_size
        return {"items": [self._project(row, current, detail=False) for row in rows[offset:offset + page_size]],
                "page": page, "page_size": page_size, "total": len(rows),
                "has_more": offset + page_size < len(rows), "as_of": current.isoformat()}

    def page(self, *, page=1, page_size=24, q="", category=None, department=None, brand=None,
             make=None, model=None, year=None, hsn=None, tsn=None,
             width=None, ratio=None, diameter=None, now=None):
        rows, current = self._page_entries(page=page, page_size=page_size, q=q,
            category=category, department=department, brand=brand, make=make, model=model,
            year=year, hsn=hsn, tsn=tsn, width=width, ratio=ratio, diameter=diameter, now=now)
        return self._page_result(rows, current, page=page, page_size=page_size)
