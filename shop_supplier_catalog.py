"""Offline licensed product projection and exact non-tyre supplier evidence.

No files, credentials, provider requests or publication on import. ALZURA
display products are distinct from RSU purchase identities/quotes. An exact
rim join requires EAN, manufacturer, model/colour and all six geometry fields;
there are no aliases or inferred manufacturer numbers. Vehicle fitment remains
unconfirmed. Unsupported parts stay visible with purchase disabled.
"""
from collections import Counter, defaultdict
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re

RULE = "exact_ean_manufacturer_model_colour_rim_geometry_v1"
HASH = re.compile(r"[a-f0-9]{64}")
SID = re.compile(r"(?:alloy_wheel|steel_wheel):[a-f0-9]{24}")
RIM_FIELDS = {"width", "diameter", "offset", "bolt_count", "pcd", "centre_bore"}
PROFILE = re.compile(r"^(?P<description>.+?)\s+(?P<width>[0-9]+(?:[.,][0-9]+)?)x"
    r"(?P<diameter>[0-9]+(?:[.,][0-9]+)?)\s+ET(?P<offset>-?[0-9]+(?:[.,][0-9]+)?)\s+-\s+"
    r"LK(?P<bolt_count>[0-9]+)/(?P<pcd>[0-9]+(?:[.,][0-9]+)?)\s+ML(?P<centre_bore>[0-9]+(?:[.,][0-9]+)?)$")


class SupplierCatalogError(ValueError):
    pass


def _hash(value):
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _fold(value):
    if not isinstance(value, str) or len(value) > 500 or any(ord(c) < 32 or ord(c) == 127 for c in value):
        return None
    return " ".join(value.casefold().split()) or None


def _number(value):
    text = str(value) if type(value) in (str, int, float, Decimal) else ""
    if not re.fullmatch(r"-?[0-9]{1,4}(?:[.,][0-9]{1,4})?", text):
        return None
    try:
        number = Decimal(text.replace(",", "."))
        return format(number.normalize(), "f") if number.is_finite() else None
    except InvalidOperation:
        return None


def _ean(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{13}", value) or set(value) == {"0"}:
        return False
    total = sum(int(c) * (3 if i % 2 == 0 else 1) for i, c in enumerate(reversed(value[:-1])))
    return (10 - total % 10) % 10 == int(value[-1])


def _geometry_valid(value):
    try:
        return (set(value) == RIM_FIELDS and all(isinstance(v, str) for v in value.values())
                and Decimal("3") <= Decimal(value["width"]) <= Decimal("20")
                and Decimal("8") <= Decimal(value["diameter"]) <= Decimal("30")
                and Decimal("-100") <= Decimal(value["offset"]) <= Decimal("150")
                and value["bolt_count"] in {str(i) for i in range(3, 11)}
                and Decimal("50") <= Decimal(value["pcd"]) <= Decimal("500")
                and Decimal("20") <= Decimal(value["centre_bore"]) <= Decimal("300"))
    except (InvalidOperation, TypeError, ValueError, KeyError):
        return False


def parse_rsu_rim_profile(value):
    """Parse explicit labelled rim geometry; no search/fuzzy name parsing."""
    match = PROFILE.fullmatch(value) if isinstance(value, str) and len(value) <= 500 else None
    if match is None:
        return None
    geometry = {key: _number(match[key]) for key in RIM_FIELDS}
    return {"description": _fold(match["description"]), "geometry": geometry} if _geometry_valid(geometry) else None


def _alzura_identity(product, private):
    if not isinstance(product, Mapping) or not isinstance(private, Mapping):
        return None
    category, pid = product.get("category"), product.get("selection_id")
    ids, variant, details = product.get("identifiers", {}), private.get("variant", {}), product.get("details", {})
    if (category not in {"alloy_wheel", "steel_wheel"} or not isinstance(pid, str) or not SID.fullmatch(pid)
            or not pid.startswith(category + ":") or not isinstance(ids, Mapping)
            or not isinstance(variant, Mapping) or not isinstance(details, Mapping)):
        return None
    geometry = {key: _number(variant.get(field)) for key, field in {
        "width": "alloyRimFrontWidth", "diameter": "alloyRimFrontDiameter", "offset": "alloyRimFrontOffset",
        "bolt_count": "alloyRimFrontHoleNumber", "pcd": "alloyRimFrontHoleCircle", "centre_bore": "alloyRimFrontHubHole"}.items()}
    if not _geometry_valid(geometry):
        return None
    for field in ("width", "diameter"):
        if _number(product.get(field)) != geometry[field]:
            return None
    for field in ("alloyRimFrontOffsetFrom", "alloyRimFrontOffsetTo"):
        if variant.get(field) not in (None, "") and _number(variant[field]) != geometry["offset"]:
            return None
    for label, key in (("Einpresstiefe", "offset"), ("Lochzahl", "bolt_count"), ("Lochkreis", "pcd"), ("Mittenloch", "centre_bore")):
        if details.get(label) in (None, "", "Nicht übermittelt"):
            continue
        raw = details[label]
        if isinstance(raw, str) and raw.endswith(" mm"):
            raw = raw[:-3]
        if _number(raw) != geometry[key]:
            return None
    manufacturer, name, ean = _fold(product.get("manufacturer_name")), _fold(product.get("name")), ids.get("ean")
    if not manufacturer or not name or not _ean(ean) or not name.startswith(manufacturer + " "):
        return None
    return {"product_id": pid, "category": category, "manufacturer": manufacturer,
            "description": name[len(manufacturer) + 1:], "ean": ean, "geometry": geometry,
            "manufacturer_article_number": _fold(ids.get("manufacturer_article_number"))}


def rim_candidate(product, private, rsu_row, *, alzura_source_sha256, rsu_source_sha256, row_number):
    if (not isinstance(alzura_source_sha256, str) or not HASH.fullmatch(alzura_source_sha256)
            or not isinstance(rsu_source_sha256, str) or not HASH.fullmatch(rsu_source_sha256)
            or type(row_number) is not int or not 1 <= row_number <= 300_000):
        raise SupplierCatalogError("invalid_source_metadata")
    identity = _alzura_identity(product, private)
    parsed = parse_rsu_rim_profile(rsu_row.get("Profil"))
    if identity is None or parsed is None:
        return None
    subtype = "Alu" if identity["category"] == "alloy_wheel" else "Stahl"
    if (rsu_row.get("Artikeltyp") != "Felge" or rsu_row.get("Artikeluntertyp") != subtype
            or any(rsu_row.get(k) != "" for k in ("Dot", "Demo", "Da"))
            or identity["ean"] != rsu_row.get("ean") or identity["manufacturer"] != _fold(rsu_row.get("Hersteller"))
            or identity["geometry"] != parsed["geometry"] or identity["description"] != parsed["description"]
            or not isinstance(rsu_row.get("Artikelnummer"), str)
            or not re.fullmatch(r"[1-9][0-9]{0,17}", rsu_row["Artikelnummer"])
            or identity["manufacturer_article_number"] is not None
               and identity["manufacturer_article_number"] != _fold(rsu_row.get("Herstellerartikelnummer"))):
        return None
    rsu = {key: rsu_row[key] for key in ("Artikelnummer", "ean", "Hersteller", "Herstellerartikelnummer", "Profil",
                                         "Artikeltyp", "Artikeluntertyp", "Dot", "Demo", "Da")}
    alzura = {"source_sha256": alzura_source_sha256, "identity": identity}
    rsu = {"source_sha256": rsu_source_sha256, "row_number": row_number, "record": rsu}
    proof = {"rule": RULE, "category": identity["category"], "alzura": alzura, "rsu": rsu}
    return {"status": "exact_supplier_identity_candidate", "confirmed": False,
            "evidence_id": _hash(proof), "evidence": proof, "vehicle_fitment_confirmed": False,
            "shipping_class": "pkw_rim", "procurement_confirmed": False}


def validate_rim_mapping(candidate, *, product_id, category, expected_ean):
    """Used by RSUQuoteProvider after separate explicit approved_ids review."""
    try:
        proof = candidate["evidence"]
        if (candidate.get("status") != "exact_supplier_identity_candidate" or proof.get("rule") != RULE
                or set(proof) != {"rule", "category", "alzura", "rsu"} or proof["category"] != category
                or _hash(proof) != candidate.get("evidence_id")
                or set(proof["alzura"]) != {"source_sha256", "identity"}
                or set(proof["rsu"]) != {"source_sha256", "row_number", "record"}):
            raise ValueError()
        identity = proof["alzura"]["identity"]
        record = proof["rsu"]["record"]
        if (identity.get("product_id") != product_id or identity.get("category") != category
                or identity.get("ean") != expected_ean or not _ean(expected_ean)
                or not _geometry_valid(identity.get("geometry", {}))):
            raise ValueError()
        # Rebuild through the same strict parser/conflict rules, without
        # introducing an alias or treating supplier numbers as manufacturer IDs.
        product = {"selection_id": product_id, "category": category, "manufacturer_name": identity["manufacturer"],
            "name": identity["manufacturer"] + " " + identity["description"],
            "width": identity["geometry"]["width"], "diameter": identity["geometry"]["diameter"],
            "identifiers": {"ean": expected_ean, "manufacturer_article_number": identity["manufacturer_article_number"]}}
        variant = {field: identity["geometry"][key] for key, field in {
            "width": "alloyRimFrontWidth", "diameter": "alloyRimFrontDiameter", "offset": "alloyRimFrontOffset",
            "bolt_count": "alloyRimFrontHoleNumber", "pcd": "alloyRimFrontHoleCircle", "centre_bore": "alloyRimFrontHubHole"}.items()}
        rebuilt = rim_candidate(product, {"variant": variant}, record,
            alzura_source_sha256=proof["alzura"]["source_sha256"], rsu_source_sha256=proof["rsu"]["source_sha256"],
            row_number=proof["rsu"]["row_number"])
        if rebuilt is None or rebuilt["evidence_id"] != candidate["evidence_id"]:
            raise ValueError()
        return {"supplier_article_id": record["Artikelnummer"], "mapping_evidence_id": candidate["evidence_id"],
                "alzura_source_sha256": proof["alzura"]["source_sha256"], "rsu_source_sha256": proof["rsu"]["source_sha256"]}
    except Exception:
        raise SupplierCatalogError("invalid_rim_binding") from None


def build_supplier_catalog(rims, parts, rsu_rows, *, private_evidence, alzura_source_sha256, rsu_source_sha256):
    """Normalize all licensed local display rows; never copy supplier prices."""
    if (not isinstance(private_evidence, Mapping)
            or any(not isinstance(v, str) or not HASH.fullmatch(v) for v in (alzura_source_sha256, rsu_source_sha256))):
        raise SupplierCatalogError("invalid_source_metadata")
    index = defaultdict(list)
    for count, pair in enumerate(rsu_rows, 1):
        if count > 300_000:
            raise SupplierCatalogError("source_too_large")
        number, row = pair
        if _ean(row.get("ean")):
            index[row["ean"]].append((number, row))
    products, mappings, reports, counts = [], {}, {}, Counter()
    for original in rims:
        if len(products) >= 10_000:
            raise SupplierCatalogError("catalogue_too_large")
        product = deepcopy(original)
        pid = product.get("selection_id")
        if not isinstance(pid, str) or not SID.fullmatch(pid):
            raise SupplierCatalogError("invalid_product_id")
        product.update(quote_public=None, price=None, price_status="update_required", quote_status="rsu_mapping_missing",
                       procurement_state="rsu_mapping_missing", order_enabled=False, fitment_status="not_confirmed", approval=None)
        product["technical_reports"] = []
        private = private_evidence.get(pid, {})
        candidates = index.get(product.get("identifiers", {}).get("ean"), [])
        candidate = (rim_candidate(original, private, candidates[0][1], alzura_source_sha256=alzura_source_sha256,
            rsu_source_sha256=rsu_source_sha256, row_number=candidates[0][0]) if len(candidates) == 1 else None)
        if candidate:
            mappings[pid] = candidate
            product.update(quote_status="fresh_rsu_quote_required", procurement_state="exact_rsu_identity_pending_review")
        reports[pid] = {"unreviewed_technical_reports": deepcopy(original.get("technical_reports", [])),
                        "vehicle_fitment_confirmed": False}
        counts[product["category"]] += 1
        products.append(product)
    for original in parts:
        if len(products) >= 10_000 or original.get("category") not in {"body_part", "oil", "care", "accessory"}:
            raise SupplierCatalogError("invalid_parts_catalogue")
        category, native = original["category"], original.get("provider_id")
        if not isinstance(native, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,150}", native):
            raise SupplierCatalogError("invalid_part_identity")
        pid = "parts:" + sha256((category + ":" + native).encode()).hexdigest()[:24]
        source_request = original.get("source_request")
        # The typed catalogue area controls the department. Wearparts include
        # non-body replacement parts without changing their stable category/ID.
        department = ("parts" if original.get("department") == "parts" or category == "body_part"
                      or isinstance(source_request, Mapping) and source_request.get("area") == "wearparts"
                      else "accessories")
        product = {"selection_id": pid, "category": category, "department": department,
            "name": original.get("name", ""), "manufacturer_name": original.get("brand", ""),
            "description": original.get("description", ""), "article_number": original.get("article_number", ""),
            "unit_label": original.get("unit_label", "Stück"), "photo": original.get("photo", ""),
            "details": {"Herstellerartikelnummer": original.get("article_number", ""), "EAN": original.get("ean", "")},
            "price": None, "price_status": "review_pending" if original.get("price_review_required") is True else "update_required", "quote_status": "rsu_mapping_missing",
            "procurement_state": "rsu_mapping_missing", "order_enabled": False,
            "new_part": original.get("new_part") is True, "painting_eligible": False, "fitment_status": "not_confirmed"}
        products.append(product)
        counts[category] += 1
    ids = [p["selection_id"] for p in products]
    if len(ids) != len(set(ids)):
        raise SupplierCatalogError("duplicate_product_id")
    return {"products": products, "mappings": mappings, "private_reports": reports,
            "report": {"counts": dict(counts), "exact_rim_candidates": len(mappings), "exact_parts_candidates": 0,
                       "source_prices_reused": False, "orders_enabled": False, "vehicle_fitment_confirmed": False}}
