"""Offline, bounded reader for the inspected ALZURA general tyre export.

No downloader, credentials, supplier orders or filesystem writes live here.
The caller supplies trusted metadata binding the source observation to the
archive hash. CSV files do not establish currency, net/unit price semantics,
or freshness by themselves. Returned evidence contains partner prices and
original media references and must remain private. Products require a separate
publication allowlist and released local media before PublicCatalog exposes them.
The uninspected alloy export is deliberately unsupported.
"""
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
from html.parser import HTMLParser
from io import TextIOWrapper
from pathlib import Path
import csv
import json
import re
from urllib.parse import unquote, urlsplit
from zipfile import BadZipFile, ZipFile


MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_CSV_BYTES = 160 * 1024 * 1024
MAX_ROWS = 200_000
MAX_FIELD_CHARS = 4096
QUOTE_TTL = timedelta(minutes=15)
HEADERS = tuple((
    "id matchcode outer_diameter height width tyre_type inner_diameter brandname "
    "profile articlename description manufacturer_number usage type load_index "
    "speed_index runflat ms 3pmfs da_decke price_1 price_4 avg_price price_anonym "
    "price_rvo availability expected_delivery_date direct_link info image image_tn "
    "tyrelabel_link energy_efficiency_index wet_grip_index noise_index noise_decible "
    "vehicle_class ice_grip data_sheet demo dot dot_year price_anonym_one "
    "manufacturer_description"
).split())
IDENTITY_FIELDS = (
    "id", "matchcode", "manufacturer_number", "brandname", "profile", "articlename",
    "outer_diameter", "width", "height", "tyre_type", "inner_diameter", "type",
    "load_index", "speed_index", "runflat", "ms", "3pmfs", "da_decke", "demo", "dot", "dot_year",
)
SEASONS = {"S": "Sommerreifen", "W": "Winterreifen", "G": "Ganzjahresreifen"}
HASH = re.compile(r"[a-f0-9]{64}")
MEMBER = re.compile(r"tmp/[0-9]{1,12}\.csv")
MEDIA_HOST = re.compile(r"media[1-5]\.tyre-shopping\.com")


class FeedError(ValueError):
    """Fixed reasons only: never put a row, price, path or URL in errors."""


@dataclass(frozen=True)
class FeedMetadata:
    """Externally reviewed facts, never inferred from ZIP/file timestamps.

    ``unit_net_fields`` explicitly confirms unit-net semantics for each offered
    quantity. Only the observed 1/4 columns exist; 2/3 tiers are not invented.
    Leave this empty until the provider's field semantics have been verified.
    Evidence strings are local review references, not credentials or URLs.
    """
    archive_sha256: str
    member: str
    supplier_observed_at: str | datetime
    timestamp_evidence: str
    currency: str | None = None
    unit_net_fields: tuple[tuple[int, str], ...] = ()
    price_semantics_evidence: str = ""


@dataclass
class FeedReport:
    """Aggregate diagnostics only; safe to serialize without partner prices."""
    archive_sha256: str = ""
    csv_sha256: str = ""
    rows: int = 0
    eligible_products: int = 0
    skipped_non_passenger: int = 0
    unknown_type_rows: int = 0
    dimensions_invalid: int = 0
    image_reference_rows: int = 0
    source_id_count: int = 0
    completed: bool = False
    quote_status_counts: Counter = field(default_factory=Counter)

    def summary(self):
        return {key: dict(value) if isinstance(value, Counter) else value
                for key, value in vars(self).items()}


def _stamp(value):
    try:
        value = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise FeedError("invalid_timestamp") from None


def _reference(value):
    return (isinstance(value, str) and 1 <= len(value) <= 160
            and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _.:/-]*", value) is not None
            and "://" not in value)


def _metadata(metadata, now):
    if not isinstance(metadata, FeedMetadata):
        raise FeedError("metadata_required")
    if (not isinstance(metadata.archive_sha256, str) or not HASH.fullmatch(metadata.archive_sha256)
            or not isinstance(metadata.member, str) or not MEMBER.fullmatch(metadata.member)
            or not _reference(metadata.timestamp_evidence)):
        raise FeedError("invalid_source_metadata")
    observed = _stamp(metadata.supplier_observed_at)
    if observed > now:
        raise FeedError("source_observation_in_future")
    if metadata.currency not in (None, "EUR"):
        raise FeedError("unsupported_currency")
    if not isinstance(metadata.unit_net_fields, tuple) or len(metadata.unit_net_fields) > 2:
        raise FeedError("invalid_price_semantics")
    fields = {}
    for entry in metadata.unit_net_fields:
        if (not isinstance(entry, tuple) or len(entry) != 2 or type(entry[0]) is not int
                or not isinstance(entry[1], str)
                or (entry[0], entry[1]) not in {(1, "price_1"), (4, "price_4")}
                or entry[0] in fields):
            raise FeedError("invalid_price_semantics")
        fields[entry[0]] = entry[1]
    if fields and (metadata.currency != "EUR" or not _reference(metadata.price_semantics_evidence)):
        raise FeedError("unconfirmed_price_semantics")
    return observed, fields


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

    def handle_data(self, value):
        if not self.hidden:
            self.parts.append(value)


def _text(value, limit=300):
    parser = _Plain()
    parser.feed(value)
    return " ".join("".join(c for c in "".join(parser.parts)
                             if ord(c) >= 32 and ord(c) != 127).split())[:limit]


def _decimal(value, minimum, maximum):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,8}(?:\.[0-9]{1,8})?", value):
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        return None
    return number if number.is_finite() and minimum <= number <= maximum else None


def _number(value, minimum, maximum):
    value = _decimal(value, Decimal(str(minimum)), Decimal(str(maximum)))
    return format(value.normalize(), "f") if value is not None else ""


def _stock(value):
    return int(value) if re.fullmatch(r"[0-9]{1,7}", value) and int(value) <= 1_000_000 else None


def _media(value, *, label=False):
    """Keep inspected static media references privately; never fetch or release."""
    if not value or value != value.strip() or any(ord(c) < 32 or ord(c) == 127 for c in value):
        return None
    try:
        parsed = urlsplit(value)
        path = unquote(parsed.path)
        prefix = "/tyrelabel2/" if label else "/images/"
        if (parsed.scheme != "https" or not MEDIA_HOST.fullmatch(parsed.hostname or "")
                or parsed.username or parsed.password or parsed.port is not None
                or parsed.query or parsed.fragment or "\\" in path
                or any(part in {".", ".."} for part in path.split("/"))
                or not path.startswith(prefix)
                or not re.search(r"\.(?:jpg|jpeg|png|webp|pdf)$", path, re.I)):
            return None
        return value
    except ValueError:
        return None


def _geometry(row):
    width = _number(row["width"], 100, 500)
    ratio = _number(row["height"], 20, 95)
    diameter = _number(row["inner_diameter"], 8, 30)
    load = row["load_index"]
    valid_load = bool(re.fullmatch(r"[0-9]{1,3}(?:/[0-9]{1,3})?", load)
                      and all(40 <= int(n) <= 200 for n in load.split("/")))
    speed = row["speed_index"]
    valid_speed = bool(re.fullmatch(r"(?:[A-Z]|A[1-8]|\([A-Z]\))", speed))
    # Non-radial/outer-diameter forms remain visible only as source evidence;
    # they do not become ordinary passenger radial geometry.
    valid = bool(width and ratio and diameter and valid_load and valid_speed
                 and row["tyre_type"] == "R" and not row["outer_diameter"])
    return {"width": width, "ratio": ratio, "diameter": diameter,
            "load_index": load if valid_load else "", "speed_index": speed if valid_speed else "",
            "season": SEASONS[row["type"]],
            "size": f"{width}/{ratio} R{diameter} {load}{speed}" if valid else "Größenangabe prüfen"}, valid


def _quote(row, metadata, observed, fields, now, dimensions_valid):
    if not dimensions_valid:
        return None, "dimensions_unconfirmed"
    if any(row[key] == "1" for key in ("demo", "dot", "da_decke")):
        return None, "restricted_condition"
    if any(row[key] not in {"0", "1"} for key in ("demo", "dot", "da_decke", "runflat")):
        return None, "condition_unconfirmed"
    stock = _stock(row["availability"])
    if stock is None:
        return None, "stock_unconfirmed"
    if now >= observed + QUOTE_TTL:
        return None, "expired"
    if metadata.currency != "EUR":
        return None, "currency_unconfirmed"
    if not fields:
        return None, "price_semantics_unconfirmed"
    prices = {}
    for quantity, key in fields.items():
        net = _decimal(row[key], Decimal("0.00000001"), Decimal("1000000"))
        if net is None or quantity > stock:
            continue
        gross = int((net * Decimal("1.10") * Decimal("1.19") * 100)
                    .quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        if 1 <= gross <= 10_000_000:
            prices[str(quantity)] = {"quantity": quantity, "unit_gross_cent": gross,
                                     "items_gross_cent": gross * quantity,
                                     "currency": "EUR", "vat_percent": "19.00"}
    if not prices:
        return None, "price_or_stock_unconfirmed"
    return {"source": "ALZURA", "currency": "EUR", "vat_percent": "19.00",
            "observed_at": observed.isoformat(), "expires_at": (observed + QUOTE_TTL).isoformat(),
            "stock_reported": stock, "prices_by_quantity": prices,
            "shipping_status": "not_confirmed", "rsu_match_status": "not_confirmed"}, "current"


def normalize_row(row, *, metadata, row_number, now):
    """Return (PublicCatalog-compatible product, private evidence), or None.

    No sample importer is called. Unknown/specialty tyre categories and steel
    rims are excluded until their own geometry has been reviewed.
    """
    now = _stamp(now)
    observed, fields = _metadata(metadata, now)
    if (not isinstance(row, Mapping) or set(row) != set(HEADERS)
            or any(not isinstance(value, str) or len(value) > MAX_FIELD_CHARS for value in row.values())
            or type(row_number) is not int or not 1 <= row_number <= MAX_ROWS):
        raise FeedError("invalid_csv_record")
    if not re.fullmatch(r"[0-9]{1,12}", row["id"]):
        raise FeedError("invalid_source_id")
    if row["type"] not in SEASONS:
        return None
    geometry, valid = _geometry(row)
    identity = {key: row[key] for key in IDENTITY_FIELDS}
    identity_hash = sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()
    sid = "tyre:" + identity_hash[:24]
    quote, status = _quote(row, metadata, observed, fields, now, valid)
    brand, profile = _text(row["brandname"], 90), _text(row["profile"], 160)
    name = " ".join(filter(None, (brand, profile))) or _text(row["articlename"], 180)
    images = list(dict.fromkeys(url for key in ("image", "image_tn") if (url := _media(row[key]))))
    quality = {}
    for source, target, allowed in (
        ("energy_efficiency_index", "fuel_efficiency", set("ABCDEFG")),
        ("wet_grip_index", "wet_grip", set("ABCDEFG")),
        ("noise_index", "noise_class", set("ABC")),
        ("vehicle_class", "vehicle_class", {"C1", "C2", "C3"}),
    ):
        if row[source] in allowed:
            quality[target] = row[source]
    noise = _number(row["noise_decible"], 1, 150)
    if noise:
        quality["external_noise_db"] = noise
    product = {"selection_id": sid, "category": "tyre", "manufacturer_name": brand,
               "name": name or "Produktangaben prüfen", "description": _text(row["description"], 2000),
               "article_number": _text(row["manufacturer_number"], 100),
               **geometry, "dimensions_valid": valid, "group": SEASONS[row["type"]],
               "service_category": "standard" if valid else "individual", "unit_label": "Stück",
               "runflat": {"0": "Nein", "1": "Ja"}.get(row["runflat"], "Nicht übermittelt"),
               "details": {"Herstellerartikelnummer": _text(row["manufacturer_number"], 100)},
               "identifiers": {"ean": None, "manufacturer_article_number": _text(row["manufacturer_number"], 100)},
               "photo": "", "gallery": [], "quote_public": quote, "quote_status": status,
               "pricing_mode": "alzura_export_net_quote", "approval": None,
               "fitment_status": "not_confirmed", "technical_reports": [], "quality_facts": quality}
    evidence = {"selection_id": sid, "provider": "ALZURA", "source_namespace": "general_export",
                "provider_id": row["id"], "source_identity": identity, "identity_sha256": identity_hash,
                "source": {"archive_sha256": metadata.archive_sha256, "member": metadata.member,
                           "data_row_number": row_number, "observed_at": observed.isoformat(),
                           "timestamp_evidence": metadata.timestamp_evidence},
                "commercial_as_supplied": {key: row[key] for key in
                    ("price_1", "price_4", "availability", "expected_delivery_date")},
                "price_semantics": {"currency": metadata.currency,
                    "unit_net_fields": dict(metadata.unit_net_fields),
                    "evidence": metadata.price_semantics_evidence,
                    "net_markup_percent": "10.00", "vat_percent": "19.00",
                    "rounding": "ROUND_HALF_UP_unit_then_quantity"},
                "image_urls": images, "tyrelabel_url": _media(row["tyrelabel_link"], label=True),
                "rsu_match_status": "not_confirmed", "fitment_approval": False,
                "public_display_ready": False}
    return product, evidence


def iter_records(path, *, metadata, now, report=None):
    """Stream the validated archive without extraction or retaining raw rows.

    Consume completely before activating an import: a later duplicate, bad CSV
    or ZIP CRC must discard the candidate, never partially replace a catalogue.
    This iterator performs no write. A caller owns atomic staging/publication.
    """
    now = _stamp(now)
    _metadata(metadata, now)
    report = report if report is not None else FeedReport()
    if not isinstance(report, FeedReport) or report.rows:
        raise FeedError("invalid_report")
    try:
        with Path(path).open("rb") as raw:
            digest, size = sha256(), 0
            for chunk in iter(lambda: raw.read(1024 * 1024), b""):
                size += len(chunk)
                if size > MAX_ARCHIVE_BYTES:
                    raise FeedError("archive_too_large")
                digest.update(chunk)
            actual = digest.hexdigest()
            if actual != metadata.archive_sha256:
                raise FeedError("archive_hash_mismatch")
            report.archive_sha256 = actual
            raw.seek(0)
            with ZipFile(raw) as archive:
                infos = archive.infolist()
                if len(infos) != 1 or infos[0].filename != metadata.member:
                    raise FeedError("unexpected_archive_members")
                info = infos[0]
                if (info.flag_bits & 1 or info.file_size > MAX_CSV_BYTES
                        or info.compress_size > MAX_ARCHIVE_BYTES or info.is_dir()
                        or ((info.external_attr >> 16) & 0o170000) == 0o120000):
                    raise FeedError("unsupported_archive_member")
                # Hash the uncompressed stream in a bounded pass, then reopen
                # for CSV iteration. Both passes remain constant-memory.
                csv_digest, csv_size = sha256(), 0
                with archive.open(info) as member:
                    for chunk in iter(lambda: member.read(1024 * 1024), b""):
                        csv_size += len(chunk)
                        if csv_size > MAX_CSV_BYTES:
                            raise FeedError("csv_too_large")
                        csv_digest.update(chunk)
                if csv_size != info.file_size:
                    raise FeedError("csv_size_mismatch")
                report.csv_sha256 = csv_digest.hexdigest()
                seen = set()
                with archive.open(info) as member, TextIOWrapper(member, encoding="utf-8-sig", newline="") as text:
                    reader = csv.DictReader(text, delimiter="|", strict=True)
                    if tuple(reader.fieldnames or ()) != HEADERS:
                        raise FeedError("unexpected_csv_schema")
                    for index, row in enumerate(reader, 1):
                        if index > MAX_ROWS:
                            raise FeedError("row_limit_exceeded")
                        if None in row or None in row.values():
                            raise FeedError("invalid_csv_record")
                        # Validate all source identities, including excluded
                        # types, so an archive cannot silently collapse IDs.
                        if not re.fullmatch(r"[0-9]{1,12}", row["id"]):
                            raise FeedError("invalid_source_id")
                        if row["id"] in seen:
                            raise FeedError("duplicate_source_id")
                        seen.add(row["id"])
                        report.rows += 1
                        result = normalize_row(row, metadata=metadata, row_number=index, now=now)
                        if result is None:
                            report.skipped_non_passenger += 1
                            if row["type"] not in {"R", "M", "B", "C", "O", "L", "I", "H", "N", "U", ""}:
                                report.unknown_type_rows += 1
                            continue
                        product, evidence = result
                        report.eligible_products += 1
                        report.dimensions_invalid += not product["dimensions_valid"]
                        report.image_reference_rows += bool(evidence["image_urls"])
                        report.quote_status_counts[product["quote_status"]] += 1
                        yield result
                report.source_id_count = len(seen)
                report.completed = True
    except FeedError:
        raise
    except (OSError, ValueError, TypeError, UnicodeError, csv.Error, BadZipFile, RuntimeError,
            NotImplementedError, EOFError):
        raise FeedError("archive_read_failed") from None
