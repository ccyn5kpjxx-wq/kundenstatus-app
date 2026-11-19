"""Bounded retail catalogue projection, independent of providers and filesystem.

The caller supplies normalized rows, an explicit publication allowlist and any
released media URLs. Raw provider identities/evidence remain with the caller.
This module does not reserve stock, approve checkout or infer vehicle fitment.
"""
from collections.abc import Iterable, Mapping
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from functools import cached_property
from hashlib import sha256
from html.parser import HTMLParser
import json
import re
import unicodedata
from shop_tyre_tests import ADAC_TYRE_TESTS
from shop_tyre_pricing import catalogue_tyre_price

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
TYRE_SEASON_VALUES = {"Sommerreifen": "summer", "Winterreifen": "winter",
                      "Ganzjahresreifen": "allseason", "all_season": "allseason",
                      "summer": "summer", "winter": "winter", "allseason": "allseason"}


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


def _model_terms(model):
    """Whole leading model words for browsing, plus the exact series with its type codes."""
    full = " ".join(_fold(model).split())
    words = full.partition("(")[0].split()
    return {full, *(" ".join(words[:end]) for end in range(1, len(words) + 1))}


def _matches_vehicle(vehicle, criteria, hsn, tsn, parts):
    if hsn is not None and (vehicle["hsn"], vehicle["tsn"]) != (hsn, tsn):
        return False
    make = _fold(vehicle["make"])
    models = _model_terms(vehicle["model"]) if parts else {_fold(vehicle["model"])}
    for wanted_make, wanted_model, year in criteria:
        if wanted_make != make or wanted_model is not None and wanted_model not in models:
            return False
        if year is not None and not (type(vehicle["year_from"]) is int and type(vehicle["year_to"]) is int
                                     and vehicle["year_from"] <= year <= vehicle["year_to"]):
            return False
    return True


def _wheel_vehicle_makes(row, fitment):
    """Make-level discovery only, from this exact variant's reviewed or supplier data."""
    from shop_partner_native.alzura_live_products import VEHICLE_BRANDS

    makes = {vehicle['make'] for vehicle in fitment['vehicles']}
    candidates, identifiers = row.get('vehicle_candidates'), row.get('identifiers')
    if (isinstance(identifiers, Mapping) and identifiers.get('provider') == 'ALZURA'
            and re.fullmatch(r'[1-9][0-9]{0,17}', str(identifiers.get('provider_id', '')))
            and isinstance(candidates, list) and len(candidates) <= 500):
        for candidate in candidates:
            if (isinstance(candidate, Mapping) and candidate.get('scope') == 'provider'
                    and _integer(candidate.get('car_id'), 1, 10**18-1)
                    and isinstance(candidate.get('brand'), str)):
                make = VEHICLE_BRANDS.get(candidate['brand'])
                if make:
                    makes.add(make)
                elif candidate['brand'] in VEHICLE_BRANDS.values():
                    makes.add(candidate['brand'])
    return sorted(makes, key=str.casefold)


def wheel_make_term(value):
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 80:
        raise CatalogError('invalid_filter')
    return _fold(value.strip())


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


def _wet_grip_rank(product):
    grade = product.get("quality_facts", {}).get("wet_grip")
    return grade if grade in ("A", "B", "C", "D", "E") else "Z"


def _tyre_season(geometry, source_group):
    """Use the exact variant's season or export group without changing its geometry/fingerprint."""
    explicit = geometry.get("season")
    classified = TYRE_SEASON_VALUES.get(source_group) if isinstance(source_group, str) else None
    if explicit:
        canonical = TYRE_SEASON_VALUES.get(explicit)
        return canonical if classified in (None, canonical) else None
    return classified


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


def _tyre_test_rating(row, season):
    identifiers, facts = row.get("identifiers"), row.get("quality_facts")
    if not isinstance(identifiers, Mapping) or not isinstance(facts, Mapping):
        return None
    ean = identifiers.get("ean")
    record = ADAC_TYRE_TESTS.get(ean) if isinstance(ean, str) else None
    if record is None:
        return None
    season_path = {"winter": "winterreifen", "summer": "sommerreifen", "allseason": "ganzjahresreifen"}
    label = (facts.get("fuel_efficiency"), facts.get("wet_grip"),
             str(facts.get("external_noise_db") or facts.get("noise_db")))
    if (record.source_path.partition("/")[0] != season_path.get(season) or label != record.label
            or record.fingerprint != variant_fingerprint(row) or row.get("approval")):
        return None
    grade = record.grade
    for ceiling, stars, verdict in ((1.5, 5, "sehr gut"), (2.5, 4, "gut"), (3.5, 3, "befriedigend"),
                                    (4.5, 2, "ausreichend"), (5.4, 1, "mangelhaft"), (5.5, 0, "mangelhaft")):
        if grade <= ceiling:
            return {"source": "ADAC", "year": record.year, "grade": grade, "stars": stars, "verdict": verdict,
                    "url": "https://www.adac.de/rund-ums-fahrzeug/ausstattung-technik-zubehoer/reifen/reifentest/"
                           + record.source_path}


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
                 quote_lookup_authoritative=False, allow_published_prices=False, tyre_payment_costs=False):
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
        if type(tyre_payment_costs) is not bool:
            raise CatalogError('invalid_tyre_pricing_policy')
        # This callback reads an already populated public-price cache only;
        # it must never perform supplier IO in the HTTP browsing thread.
        self._quote_lookup = quote_lookup
        self._quote_lookup_authoritative = quote_lookup_authoritative
        self._allow_published_prices = allow_published_prices
        self._tyre_payment_costs = tyre_payment_costs
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
            if category == "tyre":
                product["tyre_season"] = _tyre_season(product["geometry"], row.get("group"))
                facts = row.get("quality_facts")
                if isinstance(facts, Mapping) and facts.get("wet_grip") in ("A", "B", "C", "D", "E"):
                    product["quality_facts"] = {"wet_grip": facts["wet_grip"]}
                test_rating = _tyre_test_rating(row, product["tyre_season"])
                if test_rating is not None:
                    product["test_rating"] = test_rating
            photo = row.get("photo")
            if isinstance(photo, str) and SOURCE_PHOTO.fullmatch(photo):
                url = _public_path(images.get(photo), {"jpg", "jpeg", "png", "webp"})
                if url:
                    product["images"].append(url)
            evidence = verified.get(sid)
            product["fitment"] = self._fitment(row, evidence, documents)
            if department == 'wheels':
                product['wheel_vehicle_makes'] = _wheel_vehicle_makes(row, product['fitment'])
            self._rows[sid] = {"public": product, "quote": _quote(row, allow_published_prices=self._allow_published_prices),
                               "wheel_make_terms": frozenset(_fold(name) for name in product.get('wheel_vehicle_makes', ())),
                               "search": _fold(" ".join([product["name"], product["brand"], product["size"],
                                                            product["article_number"], *product["specifications"].values()]))}
        self._wheel_makes = sorted({name for entry in self._rows.values()
                                    for name in entry['public'].get('wheel_vehicle_makes', ())}, key=str.casefold)

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

    def _project(self, entry, now, *, detail=True):
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
        if quote and self._tyre_payment_costs and product['category'] == 'tyre':
            quote = catalogue_tyre_price(quote)
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

    @cached_property
    def _parts_vehicle_terms(self):
        # Built after source enrichment, solely from the immutable published fitments.
        terms = {}
        for entry in self._rows.values():
            product = entry["public"]
            if product["department"] == "parts":
                for vehicle in product["fitment"]["vehicles"]:
                    make = _fold(vehicle["make"])
                    for model in _model_terms(vehicle["model"]):
                        terms[make + " " + model] = (make, model)
        return terms

    def _parts_text_vehicle(self, q):
        text = " ".join(_fold(q).split())
        if text in self._parts_vehicle_terms:
            return (*self._parts_vehicle_terms[text], None)
        dated = re.fullmatch(r"(.+?)(?:\s*,\s*|\s+)([0-9]{4})", text)
        if dated and dated[1] in self._parts_vehicle_terms:
            return (*self._parts_vehicle_terms[dated[1]], int(dated[2]))
        return None

    def _page_entries(self, *, page=1, page_size=24, q="", category=None, department=None, brand=None, wheel_make=None,
             make=None, model=None, year=None, hsn=None, tsn=None,
             width=None, ratio=None, diameter=None, season=None, sort=None, now=None):
        if not _integer(page, 1, 10_000) or not _integer(page_size, 1, MAX_PAGE_SIZE):
            raise CatalogError("invalid_pagination")
        if not isinstance(q, str) or len(q) > 100 or len(q.split()) > 12:
            raise CatalogError("invalid_search")
        if season is not None and season not in ("summer", "winter", "allseason"):
            raise CatalogError("invalid_filter")
        if sort is not None and (sort not in ("rating", "wet_grip")
                or department not in (None, "tyres") or category not in (None, "tyre")
                or department is None and category is None):
            raise CatalogError("invalid_sort")
        if (category is not None and (not isinstance(category, str) or category not in CATEGORIES)
                or department is not None and (not isinstance(department, str) or department not in DEPARTMENTS)):
            raise CatalogError("invalid_category")
        for value in (brand, make, model):
            if value is not None and (not isinstance(value, str) or not value.strip() or len(value) > 120):
                raise CatalogError("invalid_filter")
        if wheel_make is not None:
            if department not in (None, 'wheels'):
                raise CatalogError("invalid_filter")
            wheel_make = wheel_make_term(wheel_make)
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
        text_vehicle = self._parts_text_vehicle(q) if q and department == "parts" else None
        tokens = [] if nominal or text_vehicle else _fold(q).split()
        criteria = []
        if make is not None:
            criteria.append((_fold(make.strip()), " ".join(_fold(model).split()) if model is not None else None, year))
        if text_vehicle:
            criteria.append(text_vehicle)
        rows = []
        for entry in self._rows.values():
            public = entry["public"]
            if size_conflict:
                continue
            if category is not None and public["category"] != category or department is not None and public["department"] != department:
                continue
            if wheel_make is not None and wheel_make not in entry['wheel_make_terms']:
                continue
            if brand is not None and _fold(public["brand"]) != _fold(brand.strip()):
                continue
            if season is not None and public.get("tyre_season") != season:
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
            if criteria or hsn is not None:
                parts = public["department"] == "parts"
                matched = [vehicle for vehicle in public["fitment"]["vehicles"]
                           if _matches_vehicle(vehicle, criteria, hsn, tsn, parts)]
                if not matched:
                    continue
                if parts and department == "parts" and criteria:
                    entry = {**entry, "matching_models": {(v["make"], v["model"]) for v in matched}}
            # One cache-only snapshot per matching product. Grouped/family
            # callers reuse this pass instead of rescanning every source page.
            rows.append(self._cached_entry(entry))
        # Keep the previous retail order within ties and for all ungraded rows.
        rows.sort(key=lambda entry: self._ranked_order(entry, current))
        if sort == "rating":
            rows.sort(key=lambda entry: entry["public"].get("test_rating", {}).get("grade", 6))
        elif sort == "wet_grip":
            rows.sort(key=lambda entry: _wet_grip_rank(entry["public"]))
        return rows, current

    def _page_result(self, rows, current, *, page, page_size, department=None):
        offset = (page - 1) * page_size
        result = {"items": [self._project(row, current, detail=False) for row in rows[offset:offset + page_size]],
                  "page": page, "page_size": page_size, "total": len(rows),
                  "has_more": offset + page_size < len(rows), "as_of": current.isoformat()}
        if rows and "matching_models" in rows[0]:
            models = sorted({pair for row in rows for pair in row["matching_models"]})
            result["vehicle_models"] = [{"make": make, "model": model} for make, model in models[:128]]
        if department == 'wheels':
            result['wheel_makes'] = self._wheel_makes.copy()
        return result

    def page(self, *, page=1, page_size=24, q="", category=None, department=None, brand=None, wheel_make=None,
             make=None, model=None, year=None, hsn=None, tsn=None,
             width=None, ratio=None, diameter=None, season=None, sort=None, now=None):
        rows, current = self._page_entries(page=page, page_size=page_size, q=q,
            category=category, department=department, brand=brand, wheel_make=wheel_make, make=make, model=model,
            year=year, hsn=hsn, tsn=tsn, width=width, ratio=ratio, diameter=diameter, season=season, sort=sort, now=now)
        return self._page_result(rows, current, page=page, page_size=page_size, department=department)
