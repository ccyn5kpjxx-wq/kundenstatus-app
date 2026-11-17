"""Offline exact manufacturer-SKU candidates between inspected tyre schemas.

No prices, credentials, network, orders, publication or vehicle-fitment claims.
Brand/MPN normalization changes case/spacing only, never punctuation or aliases.
Unknown attributes remain unknown; independent exact SKU keys need not invent
negative runflat/XL/OE assertions. Conflicting known attributes still reject.
Candidates require separate source/release review and fresh RSU Inquiry quotes.
"""
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re


MAX_RECORDS = 300_000
HASH = re.compile(r"[a-f0-9]{64}")
SOURCE_ID = re.compile(r"[1-9][0-9]{0,17}")
CORE_FIELDS = ("width_mm", "aspect_ratio_percent", "diameter_inch", "construction",
               "load_index", "speed_index", "season")
FEATURES = ("runflat", "three_pmsf", "ms", "xl", "oe_marking")
ALZURA_FIELDS = frozenset({"id", "brandname", "manufacturer_number", "width", "height",
    "inner_diameter", "tyre_type", "load_index", "speed_index", "type", "dot", "demo",
    "da_decke", "runflat", "3pmfs", "ms"})
RSU_FIELDS = frozenset({"Artikelnummer", "Hersteller", "Herstellerartikelnummer", "Reifenbreite",
    "Reifenquerschnitt", "Reifendurchmesser", "Bauart", "Loadindex", "Speedindex", "Saison",
    "Artikeltyp", "Artikeluntertyp", "Dot", "Demo", "Da"})
SPEEDS = frozenset({"L", "M", "N", "P", "Q", "R", "S", "T", "U", "H", "V", "W", "Y"})


class MatchError(ValueError):
    """Fixed reason codes; no source values or paths in exceptions."""


def _hash(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _text(value):
    if (not isinstance(value, str) or len(value) > 300
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        return None
    return " ".join(value.casefold().split()) or None


def _number(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,8}(?:[.,][0-9]{1,4})?", value):
        return None
    try:
        number = Decimal(value.replace(",", "."))
        return format(number.normalize(), "f") if number.is_finite() else None
    except InvalidOperation:
        return None


def _complete(core):
    if any(core.get(key) is None for key in CORE_FIELDS):
        return False
    try:
        return (Decimal("100") <= Decimal(core["width_mm"]) <= Decimal("455")
                and Decimal("15") <= Decimal(core["aspect_ratio_percent"]) <= Decimal("100")
                and Decimal("10") <= Decimal(core["diameter_inch"]) <= Decimal("30")
                and Decimal("30") <= Decimal(core["load_index"]) <= Decimal("150")
                and Decimal(core["load_index"]) == Decimal(core["load_index"]).to_integral_value()
                and core["construction"] == "r" and core["speed_index"].upper() in SPEEDS
                and core["season"] in {"sommer", "winter", "ganzjahr"})
    except (InvalidOperation, ValueError, TypeError, KeyError):
        return False


@dataclass(frozen=True)
class SkuRecord:
    namespace: str
    source_sha256: str
    row_number: int
    article_id: str
    manufacturer: str | None
    mpn: str | None
    # Immutable JSON avoids mutation of an indexed source record.
    core_json: str
    features_json: str
    conditions_clear: bool
    eligible_category: bool
    source_row_fingerprint: str
    identity_fingerprint: str

    def core(self):
        return json.loads(self.core_json)

    def features(self):
        return json.loads(self.features_json)

    def sku_key(self):
        return (self.manufacturer, self.mpn) if self.manufacturer and self.mpn else None

    def complete(self):
        return self.eligible_category and self.sku_key() is not None and _complete(self.core())


def _record(row, source_sha256, row_number, *, namespace):
    required = ALZURA_FIELDS if namespace == "ALZURA_GENERAL_EXPORT" else RSU_FIELDS
    if (not isinstance(row, Mapping) or not required <= row.keys()
            or any(not isinstance(row[key], str) or len(row[key]) > 300 for key in required)
            or not isinstance(source_sha256, str) or not HASH.fullmatch(source_sha256)
            or type(row_number) is not int or not 1 <= row_number <= MAX_RECORDS):
        raise MatchError("invalid_identity_input")
    if namespace == "ALZURA_GENERAL_EXPORT":
        article_id = row["id"]
        manufacturer, mpn = row["brandname"], row["manufacturer_number"]
        raw_core = [row[key] for key in ("width", "height", "inner_diameter", "tyre_type",
                                       "load_index", "speed_index")]
        season = {"S": "sommer", "W": "winter", "G": "ganzjahr"}.get(row["type"])
        eligible = row["type"] in {"S", "W", "G"}
        clear = all(row[key] == "0" for key in ("dot", "demo", "da_decke"))
        features = {key: None for key in FEATURES}
        for field, key in (("runflat", "runflat"), ("3pmfs", "three_pmsf"), ("ms", "ms")):
            features[key] = {"0": False, "1": True}.get(row[field])
    else:
        article_id = row["Artikelnummer"]
        manufacturer, mpn = row["Hersteller"], row["Herstellerartikelnummer"]
        raw_core = [row[key] for key in ("Reifenbreite", "Reifenquerschnitt", "Reifendurchmesser",
                                       "Bauart", "Loadindex", "Speedindex")]
        season = {"s": "sommer", "w": "winter", "g": "ganzjahr"}.get(row["Saison"])
        eligible = row["Artikeltyp"] == "Reifen" and row["Artikeluntertyp"] == "PKW"
        # Empty flags mean not marked as restricted by this schema. They do
        # not prove manufacturing age, OE/XL/runflat absence or vehicle fitment.
        clear = all(row[key] == "" for key in ("Dot", "Demo", "Da"))
        features = {key: None for key in FEATURES}
    if not SOURCE_ID.fullmatch(article_id):
        raise MatchError("invalid_source_article_id")
    core = dict(zip(CORE_FIELDS, [_number(raw_core[0]), _number(raw_core[1]), _number(raw_core[2]),
                                 _text(raw_core[3]), _number(raw_core[4]), _text(raw_core[5]), season]))
    manufacturer, mpn = _text(manufacturer), _text(mpn)
    identity = {"manufacturer": manufacturer, "mpn": mpn, "core": core}
    # Field-specific row proof, excluding price/stock, tied to the complete
    # immutable source hash and CSV row number. Not an assertion of freshness.
    row_proof = {"namespace": namespace, "source_sha256": source_sha256, "row_number": row_number,
                 "article_id": article_id, "identity": identity, "features": features,
                 "conditions_clear": clear, "eligible_category": eligible}
    return SkuRecord(namespace, source_sha256, row_number, article_id, manufacturer, mpn,
                     json.dumps(core, sort_keys=True), json.dumps(features, sort_keys=True),
                     clear, eligible, _hash(row_proof), _hash(identity))


def alzura_record(row, *, archive_sha256, row_number):
    return _record(row, archive_sha256, row_number, namespace="ALZURA_GENERAL_EXPORT")


def rsu_record(row, *, csv_sha256, row_number):
    return _record(row, csv_sha256, row_number, namespace="RSU_EXPORT")


def _index(records, namespace):
    if isinstance(records, (str, bytes, Mapping)):
        raise MatchError("invalid_record_collection")
    result, ids, hashes = defaultdict(list), set(), set()
    try:
        for count, record in enumerate(records, 1):
            if count > MAX_RECORDS:
                raise MatchError("record_limit_exceeded")
            if (not isinstance(record, SkuRecord) or record.namespace != namespace
                    or not HASH.fullmatch(record.source_sha256)
                    or not HASH.fullmatch(record.source_row_fingerprint)
                    or not HASH.fullmatch(record.identity_fingerprint)
                    or not isinstance(record.core_json, str) or len(record.core_json) > 1000
                    or not isinstance(record.features_json, str) or len(record.features_json) > 500
                    or type(record.row_number) is not int or not 1 <= record.row_number <= MAX_RECORDS
                    or not isinstance(record.article_id, str) or not SOURCE_ID.fullmatch(record.article_id)
                    or type(record.conditions_clear) is not bool or type(record.eligible_category) is not bool):
                raise MatchError("invalid_identity_record")
            try:
                core, features = record.core(), record.features()
                if (not isinstance(core, dict) or set(core) != set(CORE_FIELDS)
                        or any(value is not None and (not isinstance(value, str) or len(value) > 100)
                               for value in core.values())
                        or not isinstance(features, dict) or set(features) != set(FEATURES)
                        or any(value is not None and type(value) is not bool for value in features.values())
                        or record.manufacturer != _text(record.manufacturer)
                        or record.mpn != _text(record.mpn)):
                    raise ValueError()
                identity = {"manufacturer": record.manufacturer, "mpn": record.mpn, "core": core}
                proof = {"namespace": record.namespace, "source_sha256": record.source_sha256,
                         "row_number": record.row_number, "article_id": record.article_id,
                         "identity": identity, "features": features,
                         "conditions_clear": record.conditions_clear,
                         "eligible_category": record.eligible_category}
                if record.identity_fingerprint != _hash(identity) or record.source_row_fingerprint != _hash(proof):
                    raise ValueError()
            except (ValueError, TypeError):
                raise MatchError("identity_record_binding_mismatch") from None
            if record.article_id in ids:
                raise MatchError("duplicate_source_article_id")
            ids.add(record.article_id)
            hashes.add(record.source_sha256)
            if len(hashes) > 1:
                raise MatchError("mixed_source_snapshots")
            if record.sku_key() is not None:
                result[record.sku_key()].append(record)
    except TypeError:
        raise MatchError("invalid_record_collection") from None
    return result, len(ids), next(iter(hashes), None)


def _feature_conflict(left, right):
    return any(left.get(key) is not None and right.get(key) is not None
               and left[key] != right[key] for key in FEATURES)


def match_catalogues(alzura_records, rsu_records):
    """Return private candidates and aggregate diagnostics, never live approval.

    Global brand+MPN uniqueness is checked within each supplied eligible tyre
    snapshot, including restricted/incomplete records. No fuzzy fallback, EAN
    search, minimum-LI substitution, inferred feature absence or price fallback.
    """
    left, alzura_count, alzura_hash = _index(alzura_records, "ALZURA_GENERAL_EXPORT")
    right, rsu_count, rsu_hash = _index(rsu_records, "RSU_EXPORT")
    stats, matches = Counter(), []
    missing_sku = alzura_count - sum(len(group) for group in left.values())
    if missing_sku:
        stats["identity_missing_sku"] = missing_sku
    for key, group in sorted(left.items()):
        for record in group:
            if not record.eligible_category:
                stats["excluded_category"] += 1
                continue
            if not record.complete():
                stats["identity_incomplete"] += 1
                continue
            if not record.conditions_clear:
                stats["alzura_condition_unconfirmed_or_restricted"] += 1
                continue
            # Conflicting/incomplete core facts under the same manufacturer
            # SKU require source review. A separately marked DOT/demo offer
            # does not erase a unique explicitly unrestricted source item.
            if (len({_hash(value.core()) for value in group}) != 1
                    or sum(value.conditions_clear and value.eligible_category for value in group) != 1):
                stats["alzura_sku_ambiguous"] += 1
                continue
            candidates = right.get(key, [])
            if not candidates:
                stats["no_rsu_sku"] += 1
                continue
            if len({_hash(value.core()) for value in candidates}) != 1:
                stats["rsu_sku_ambiguous"] += 1
                continue
            selectable = [value for value in candidates if value.complete() and value.conditions_clear]
            if not selectable:
                stats["rsu_identity_or_condition_unconfirmed"] += 1
                continue
            if len(selectable) != 1:
                stats["rsu_sku_ambiguous"] += 1
                continue
            candidate = selectable[0]
            if record.core() != candidate.core():
                stats["core_mismatch"] += 1
                continue
            if _feature_conflict(record.features(), candidate.features()):
                stats["explicit_feature_conflict"] += 1
                continue
            proof = {"rule": "exact_manufacturer_mpn_and_full_tyre_core_v1",
                     "manufacturer": record.manufacturer, "mpn": record.mpn, "core": record.core(),
                     "alzura": {"source_sha256": record.source_sha256, "row_number": record.row_number,
                         "article_id": record.article_id, "row_fingerprint": record.source_row_fingerprint,
                         "identity_fingerprint": record.identity_fingerprint, "features": record.features()},
                     "rsu": {"source_sha256": candidate.source_sha256, "row_number": candidate.row_number,
                         "article_id": candidate.article_id, "row_fingerprint": candidate.source_row_fingerprint,
                         "identity_fingerprint": candidate.identity_fingerprint, "features": candidate.features()}}
            matches.append({"status": "exact_sku_candidate", "confirmed": False,
                            "evidence_id": _hash(proof), "evidence": proof,
                            "procurement_confirmed": False, "vehicle_fitment_confirmed": False,
                            "supplier_quote_current": False, "public_display_ready": False})
            stats["exact_sku_candidate"] += 1
    return {"schema_version": 1, "source_hashes": {"alzura": alzura_hash, "rsu": rsu_hash},
            "source_record_counts": {"alzura": alzura_count, "rsu": rsu_count},
            "counts": dict(stats), "mappings": matches, "publication_enabled": False,
            "supplier_quotes_included": False, "supplier_orders_enabled": False,
            "payments_enabled": False}
