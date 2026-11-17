"""Offline, fail-closed ALZURA product snapshot for the licensed local preview.

Consumes completed read-only client run directories, never tokens or HTTP.
Only ``catalogues`` is a public renderer input. ``private_evidence`` contains
supplier purchase prices and original image references and must remain private.
ALZURA observations are never labelled as RSU quotes or vehicle approval.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit

from .alzura_readonly_catalog import build_url, validate_plan, _atomic_json

TTL = timedelta(minutes=15)
MAX_BODY = 4 * 1024 * 1024
ALLOWED = {("tyres", "search"), ("tyres", "details"),
           ("alloys", "profiSearch"), ("alloys", "search"), ("alloys", "details")}
BODY_NAME = re.compile(r"[0-9]{2}-(?:tyres|alloys)-[A-Za-z]+\.body")
MEDIA_HOST = re.compile(r"media[1-5]\.tyre-shopping\.com")
HASH = re.compile(r"[0-9a-f]{64}")
# Explicit names/aliases only; no inference from an unknown vehicle label.
VEHICLE_BRANDS = {
    "Mercedes-Benz": "Mercedes-Benz", "Mercedes Benz": "Mercedes-Benz",
    "Volkswagen": "Volkswagen", "VW": "Volkswagen", "Alfa Romeo": "Alfa Romeo",
    "Land Rover": "Land Rover", "Aston Martin": "Aston Martin", "Rolls-Royce": "Rolls-Royce",
    "Audi": "Audi", "BMW": "BMW", "Mercedes": "Mercedes-Benz", "Mini": "MINI",
    "Opel": "Opel", "Ford": "Ford", "Seat": "SEAT", "Cupra": "Cupra", "Skoda": "Skoda",
    "Škoda": "Skoda", "Toyota": "Toyota", "Lexus": "Lexus", "Nissan": "Nissan",
    "Renault": "Renault", "Peugeot": "Peugeot", "Citroen": "Citroen", "Citroën": "Citroen",
    "Fiat": "Fiat", "Abarth": "Abarth", "Dacia": "Dacia", "Hyundai": "Hyundai", "Kia": "Kia",
    "Honda": "Honda", "Mazda": "Mazda", "Suzuki": "Suzuki", "Mitsubishi": "Mitsubishi",
    "Subaru": "Subaru", "Volvo": "Volvo", "Saab": "Saab", "Porsche": "Porsche",
    "Tesla": "Tesla", "Jaguar": "Jaguar", "Jeep": "Jeep", "Chrysler": "Chrysler",
    "Dodge": "Dodge", "Chevrolet": "Chevrolet", "Smart": "Smart", "DS": "DS",
}


class SnapshotError(ValueError):
    """Fixed local reason; never includes supplier bodies or secret values."""


def stamp(value):
    try:
        value = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError()
        return value.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise SnapshotError("timestamp_invalid") from None


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise SnapshotError("duplicate_json_key")
        value[key] = item
    return value


def _json(raw):
    try:
        return json.loads(raw.decode("utf-8-sig"), object_pairs_hook=_unique,
                          parse_float=Decimal,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except Exception:
        raise SnapshotError("json_invalid") from None


def _serial(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _serial(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serial(item) for item in value]
    return value


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


def plain(value, maximum=600):
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        return ""
    parser = _Plain()
    try:
        parser.feed(str(value)[:20000])
        value = " ".join("".join(parser.parts).split())
        return "".join(c for c in value if ord(c) >= 32 and ord(c) != 127)[:maximum]
    except Exception:
        return ""


def decimal(value, *, minimum=None, maximum=None):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        return None
    text = str(value)
    if not re.fullmatch(r"-?[0-9]{1,10}(?:\.[0-9]{1,8})?", text):
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    if not number.is_finite() or (minimum is not None and number < minimum) or (maximum is not None and number > maximum):
        return None
    return number


def number(value, minimum=None, maximum=None):
    value = decimal(value, minimum=minimum, maximum=maximum)
    return format(value.normalize(), "f") if value is not None else ""


def integer(value, minimum=0, maximum=10000000):
    return value if type(value) is int and minimum <= value <= maximum else None


def valid_ean(value):
    if not isinstance(value, str) or not re.fullmatch(r"(?:[0-9]{8}|[0-9]{12,14})", value) or set(value) == {"0"}:
        return False
    total = sum(int(char) * (3 if index % 2 == 0 else 1)
                for index, char in enumerate(reversed(value[:-1])))
    return (10 - total % 10) % 10 == int(value[-1])


def image_url(value):
    """Only observed static provider media, with documented resolution expansion.

    ``%s-%s`` means width-height in the official imageUrl schema, never a view.
    There is no network operation in this module.
    """
    if not isinstance(value, str) or len(value) > 2000:
        return None
    value = value.replace("%s-%s", "w800-h800")
    if "%s" in value:
        return None
    try:
        parsed = urlsplit(value)
        decoded = unquote(parsed.path)
        if (parsed.scheme != "https" or not MEDIA_HOST.fullmatch(parsed.hostname or "")
                or parsed.username or parsed.password or parsed.port is not None
                or parsed.query or parsed.fragment or "\\" in decoded
                or any(part in {".", ".."} for part in decoded.split("/"))
                or not parsed.path.startswith(("/images/", "/images_ts/", "/dryd/", "/dryd_rims2/"))
                or not re.search(r"\.(?:jpg|jpeg|png|webp)$", parsed.path, re.I)):
            return None
        return value
    except ValueError:
        return None


def _images(row):
    candidates = [row.get("imageURL")]
    if isinstance(row.get("imageUrl"), dict):
        candidates.append(row["imageUrl"].get("URL"))
    # Known direct URLs only. No filename/view/host enumeration.
    return list(dict.fromkeys(url for value in candidates if (url := image_url(value))))


def _report_url(value):
    if not isinstance(value, str) or len(value) > 2000:
        return None
    try:
        parsed = urlsplit(value)
        if (parsed.scheme == "https" and MEDIA_HOST.fullmatch(parsed.hostname or "")
                and not parsed.username and not parsed.password and parsed.port is None
                and not parsed.query and not parsed.fragment
                and parsed.path.lower().endswith(".pdf")
                and ".." not in unquote(parsed.path).split("/")):
            return value
    except ValueError:
        pass
    return None


def _rows(data, area, operation):
    if not isinstance(data, dict) or data.get("status") is False:
        raise SnapshotError("product_response_shape_invalid")
    if area == "tyres":
        if operation == "details" and "idSolr" in data:
            return [data]
        values = data.get("tyreList")
    elif operation == "details":
        if "alloyRimFrontID" in data:
            return [data]  # Observed production shape: direct object.
        values = data.get("alloyDetailsList")
        if isinstance(values, dict):
            values = [values]
    else:
        values = data.get("alloyRimsList")
    if not isinstance(values, list) or len(values) > 20000 or any(not isinstance(row, dict) for row in values):
        raise SnapshotError("product_response_shape_invalid")
    return values


def load_run(path, *, now):
    """Read and authenticate local evidence hashes; not provider authentication."""
    root, now = Path(path).resolve(), stamp(now)
    plan = validate_plan(_json((root / "plan.json").read_bytes()))
    result = _json((root / "results.json").read_bytes())
    entries = result.get("results") if isinstance(result, dict) else None
    if not isinstance(entries, list) or len(entries) != len(plan) or result.get("provider_calls") != len(entries):
        raise SnapshotError("run_incomplete")
    records, sources = [], []
    for request, summary in zip(plan, entries):
        area, operation = request["area"], request["operation"]
        if (area, operation) not in ALLOWED:
            raise SnapshotError("non_product_source_not_allowed")
        if (not isinstance(summary, dict) or summary.get("ok") is not True
                or summary.get("http_status") != 200 or summary.get("area") != area
                or summary.get("operation") != operation):
            raise SnapshotError("run_has_failed_response")
        filename = summary.get("body_file")
        if not isinstance(filename, str) or not BODY_NAME.fullmatch(filename):
            raise SnapshotError("body_path_invalid")
        body_path = root / filename
        meta_path = root / (filename[:-5] + ".meta.json")
        if body_path.is_symlink() or meta_path.is_symlink():
            raise SnapshotError("body_path_invalid")
        meta = _json(meta_path.read_bytes())
        for key in ("ok", "area", "operation", "http_status", "body_file", "body_sha256", "requested_at", "received_at"):
            if summary.get(key) != meta.get(key):
                raise SnapshotError("response_metadata_conflict")
        if not HASH.fullmatch(str(meta.get("body_sha256", ""))):
            raise SnapshotError("body_hash_missing")
        observed, received = stamp(meta.get("requested_at")), stamp(meta.get("received_at"))
        if received < observed or observed > now or received > now:
            raise SnapshotError("response_timestamp_invalid")
        if body_path.stat().st_size > MAX_BODY:
            raise SnapshotError("body_too_large")
        raw = body_path.read_bytes()
        if sha256(raw).hexdigest() != meta["body_sha256"]:
            raise SnapshotError("body_hash_mismatch")
        data = _json(raw)
        rows = _rows(data, area, operation)
        source = {"provider": "ALZURA", "area": area, "operation": operation,
                  "run": root.name, "body_file": filename, "body_sha256": meta["body_sha256"],
                  "observed_at": observed.isoformat(), "received_at": received.isoformat(), "rows": len(rows)}
        sources.append(source)
        for row in rows:
            if area == "tyres":
                identity = row.get("idSolr")
                if not isinstance(identity, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,150}", identity):
                    raise SnapshotError("provider_identity_missing")
                if operation == "details" and identity != request.get("params", {}).get("idSolr"):
                    raise SnapshotError("details_identity_mismatch")
            else:
                identity = integer(row.get("alloyRimFrontID"), 1, 999999999999999999)
                if identity is None:
                    raise SnapshotError("provider_identity_missing")
                if operation == "details" and identity != request.get("params", {}).get("alloyRimFrontID"):
                    raise SnapshotError("details_identity_mismatch")
            records.append({"area": area, "provider_id": str(identity), "row": row,
                            "source": source, "request": request})
    return records, sources


def unit_gross_cent(net):
    """Same ROUND_HALF_UP unit-first calculation as berechne_reifenpreis."""
    value = decimal(net, minimum=Decimal("0.00000001"), maximum=Decimal("1000000"))
    if value is None:
        return None
    gross = (value * Decimal("1.10") * Decimal("1.19")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int(gross * 100) if gross > 0 else None


def _price_for(row, area, quantity):
    expected_type = "ek_front" if area == "alloys" else "ek_one" if quantity == 1 else "ek"
    values = row.get("priceList")
    if not isinstance(values, list):
        return None
    candidates = []
    for price in values:
        if not isinstance(price, dict) or price.get("type") != expected_type or price.get("country_code") != "de" or price.get("currency") != "EUR":
            continue
        minimum = integer(price.get("min_order"), 1, 10000000)
        value = decimal(price.get("value"), minimum=Decimal("0.00000001"), maximum=Decimal("1000000"))
        if minimum is None or value is None:
            return None
        if area == "tyres" and ((quantity == 1 and minimum != 1) or (quantity > 1 and minimum < 2)):
            return None
        if minimum <= quantity:
            candidates.append((value, minimum))
    if not candidates or len({value for value, _ in candidates}) != 1:
        return None  # No inferred cheapest tier or inferred set-to-unit price.
    gross = unit_gross_cent(candidates[0][0])
    if gross is None:
        return None
    return {"quantity": quantity, "unit_gross_cent": gross,
            "items_gross_cent": gross * quantity, "vat_percent": "19.00"}


def _quote(record, now, *, dimensions_valid, conflict):
    observed = stamp(record["source"]["observed_at"])
    expires = observed + TTL
    if observed > now:
        return None, "observation_in_future"
    if now >= expires:
        return None, "expired"
    if conflict:
        return None, "identity_conflict"
    if not dimensions_valid:
        return None, "dimensions_unconfirmed"
    row, area = record["row"], record["area"]
    stock = integer(row.get("quantity" if area == "tyres" else "alloyRimFrontStock"))
    if stock is None:
        return None, "stock_unconfirmed"
    prices = {str(quantity): price for quantity in range(1, 5)
              if quantity <= stock and (price := _price_for(row, area, quantity)) is not None}
    if not prices:
        return None, "price_or_stock_unconfirmed"
    return {"source": "ALZURA", "observed_at": observed.isoformat(), "expires_at": expires.isoformat(),
            "stock_reported": stock, "prices_by_quantity": prices, "shipping_status": "not_confirmed",
            "rsu_match_status": "not_confirmed"}, "current"


def _tyre_geometry(row):
    size = plain(row.get("size"), 30)
    match = re.fullmatch(r"([1-9][0-9]{2})([0-9]{2})([0-9]{2})", size)
    width, ratio, diameter = match.groups() if match else ("", "", "")
    valid = bool(match and Decimal("100") <= Decimal(width) <= Decimal("500")
                 and Decimal("20") <= Decimal(ratio) <= Decimal("95")
                 and Decimal("8") <= Decimal(diameter) <= Decimal("30"))
    if not valid:
        return {"width": "", "ratio": "", "diameter": "", "size": plain(row.get("description")) or size or "Größenangabe prüfen"}, False
    # Load index is not structured in this API search shape. Preserve the exact
    # technical description; never invent an LI or Runflat/XL absence.
    return {"width": number(width), "ratio": number(ratio), "diameter": number(diameter),
            "size": plain(row.get("description")) or f"{width}/{ratio} R{diameter}"}, True


def _steel_geometry(row):
    description = plain(row.get("description"))
    pairs = {(number(width.replace(",", ".")), number(diameter)) for width, diameter in
             re.findall(r"(?<![0-9.,])([0-9]{1,2}(?:[.,][0-9]+)?)\s*[xX×]\s*([0-9]{2})(?![0-9])", description)
             if decimal(width.replace(",", "."), minimum=Decimal("3"), maximum=Decimal("20")) is not None
             and decimal(diameter, minimum=Decimal("8"), maximum=Decimal("30")) is not None}
    if len(pairs) != 1:
        return {"width": "", "ratio": "", "diameter": "", "size": "Größenangabe prüfen"}, False
    width, diameter = next(iter(pairs))
    return {"width": width, "ratio": "", "diameter": diameter, "size": f"{width} × {diameter} Zoll"}, True


def _variant(record):
    row, area = record["row"], record["area"]
    if area == "tyres":
        return {key: _serial(row.get(key)) for key in ("ean", "manufacturerArticleNumber", "size", "speedIndex", "type",
                "isDot", "isDemo", "is3PMSF", "description", "additionalDescription")}
    return {key: _serial(row.get(key)) for key in ("alloyRimFrontEAN", "alloyRimFrontWidth", "alloyRimFrontDiameter",
            "alloyRimFrontOffset", "alloyRimFrontOffsetFrom", "alloyRimFrontOffsetTo", "alloyRimFrontHoleNumber",
            "alloyRimFrontHoleCircle", "alloyRimFrontHubHole", "typeName", "typeColor", "typeConstruction")}


def _conflicts(records):
    values = defaultdict(set)
    for record in records:
        for key, value in _variant(record).items():
            if value is not None and value != "":
                # Canonical numeric spelling does not constitute a conflict.
                numeric = number(value)
                values[key].add(numeric or json.dumps(value, ensure_ascii=False, sort_keys=True))
    return sorted(key for key, variants in values.items() if len(variants) > 1)


def _reports(records):
    results, seen = [], set()
    for record in records:
        if record["source"]["operation"] != "details":
            continue
        car_id = record["request"].get("params", {}).get("carID")
        if integer(car_id, 1, 999999999999999999) is None:
            continue
        for field in ("technicalReports", "technicalReportsAdditional"):
            values = record["row"].get(field)
            if not isinstance(values, list):
                continue
            for report in values:
                if not isinstance(report, dict) or not (url := _report_url(report.get("url"))):
                    continue
                identity = (url, car_id)
                if identity in seen:
                    continue
                seen.add(identity)
                results.append({"url": url, "report_number": plain(report.get("report_nr")),
                                "type_description": plain(report.get("type_description")), "car_id": car_id,
                                "scope_status": "provider_link_for_requested_car_not_reviewed"})
    return results


def _vehicle_candidates(records):
    candidates, observed, seen = [], [], set()
    for record in records:
        rows = record["row"].get("carInfoList")
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict) or integer(row.get("carID"), 1, 999999999999999999) is None:
                continue
            label, actual_type = plain(row.get("brand"), 250), plain(row.get("type"), 150)
            if not label:
                continue
            observed.append({"carID": row["carID"], "brand": label, "type": actual_type})
            brand, model = label, ""
            for prefix in sorted(VEHICLE_BRANDS, key=len, reverse=True):
                if label.casefold() == prefix.casefold() or label.casefold().startswith(prefix.casefold() + " "):
                    brand, model = VEHICLE_BRANDS[prefix], label[len(prefix):].strip()
                    break
            identity = (brand, model, actual_type)
            if identity in seen:
                continue
            seen.add(identity)
            if len(candidates) < 500:
                candidates.append({"brand": brand, "model": model,
                                   "variant": "Typ " + actual_type if actual_type else "",
                                   "start": "", "end": "", "keys": [], "scope": "provider",
                                   "car_id": row["carID"]})
    return candidates, len(seen) > 500, observed


def _identifiers(row, area):
    if area == "tyres":
        return {"provider": "ALZURA", "provider_id": plain(row.get("idSolr")),
                "id_t24": integer(row.get("idT24"), 1, 999999999999999999),
                "ean": row.get("ean") if valid_ean(row.get("ean")) else None,
                "manufacturer_article_number": plain(row.get("manufacturerArticleNumber")) or None,
                "wholesaler_article_number": plain(row.get("wholesalerArticleNo")) or None}
    return {"provider": "ALZURA", "provider_id": str(row["alloyRimFrontID"]),
            "ean": row.get("alloyRimFrontEAN") if valid_ean(row.get("alloyRimFrontEAN")) else None,
            "manufacturer_article_number": plain(row.get("alloyRimFrontIdent")) or None,
            "wholesaler_article_number": plain(row.get("alloyRimFrontWholesalerArticleNo")) or None}


def _format_money(cents):
    return f"{cents // 100:,}".replace(",", ".") + f",{cents % 100:02d} €"


def _quality_facts(row):
    """Keep exact article EU-label facts; never infer an overall quality tier.

    In actual responses rollingNoiseValue is the class and rollingNoiseLevel
    contains the decibels. Old numeric noise classes are not reinterpreted.
    """
    label = row.get("tyreLabel")
    if not isinstance(label, dict):
        return {}
    facts = {}
    for source, target in (("wetgripIndex", "wet_grip"), ("energyIndex", "fuel_efficiency")):
        value = label.get(source)
        if isinstance(value, str) and value in {"A", "B", "C", "D", "E"}:
            facts[target] = value
    noise_class = label.get("rollingNoiseValue")
    if isinstance(noise_class, str) and noise_class in {"A", "B", "C"}:
        facts["noise_class"] = noise_class
    noise_level = label.get("rollingNoiseLevel")
    match = re.fullmatch(r"([0-9]{2,3}) dB", noise_level) if isinstance(noise_level, str) else None
    if match and 45 <= int(match[1]) <= 100:
        facts["noise_db"] = int(match[1])
    vehicle_class = label.get("vehicleClass")
    if isinstance(vehicle_class, str) and vehicle_class in {"C1", "C2", "C3"}:
        facts["vehicle_class"] = vehicle_class
    if facts:
        facts["source"] = "ALZURA EU-Label"
    return facts


def normalize_product(records, *, now):
    """Return a fixed public product plus separate private evidence.

    Most recent actual observation supplies price/stock. More detailed older
    records may enrich identity, but never refresh their observation timestamp.
    Any contradictory known variant fact suppresses the quote.
    """
    now = stamp(now)
    ordered = sorted(records, key=lambda record: (stamp(record["source"]["observed_at"]), record["source"]["operation"] == "details"))
    latest = ordered[-1]
    row = {}
    for record in ordered:
        for key, value in record["row"].items():
            if value is not None and value != "":
                row[key] = value
    # Offers are a single observation; do not splice price from one and stock
    # from another, and do not borrow an older missing priceList.
    price_row = latest["row"]
    conflicts = _conflicts(records)
    area = latest["area"]
    identifiers = _identifiers(row, area)
    details = {"Anbieter": "ALZURA · kein bestätigtes RSU-Angebot",
               "EAN": identifiers["ean"] or "Nicht übermittelt",
               "Herstellerartikelnummer": identifiers["manufacturer_article_number"] or "Nicht übermittelt"}
    if area == "tyres":
        steel = row.get("type") == "C" and plain(row.get("description")).startswith("SF ")
        geometry, valid = _steel_geometry(row) if steel else _tyre_geometry(row)
        category = "steel_wheel" if steel else "tyre"
        group = "Stahlfelge" if steel else {"S": "Sommerreifen", "W": "Winterreifen", "G": "Ganzjahresreifen"}.get(row.get("type"), "Weitere Reifentypen")
        name = " ".join(filter(None, (plain(row.get("manufacturerName")), plain(row.get("profileName"))))) or plain(row.get("description"))
        if steel:
            name = plain(row.get("description"))
        details.update({"Artikelbezeichnung": plain(row.get("description")),
                        "Zusatzmerkmale": plain(row.get("additionalDescription")),
                        "Geschwindigkeitsindex": plain(row.get("speedIndex")),
                        "3PMSF laut Anbieter": {0: "Nein", 1: "Ja"}.get(row.get("is3PMSF"), "Nicht übermittelt")})
        if row.get("isDot") is True or type(row.get("isDot")) is int and row["isDot"] == 1:
            details["DOT-Ausführung"] = "Vom Anbieter als DOT-Artikel gemeldet; Herstellungsjahr nur soweit ausdrücklich geliefert."
        if row.get("isDemo") is True or type(row.get("isDemo")) is int and row["isDemo"] == 1:
            details["Demoware"] = "Vom Anbieter als Demo gemeldet; Zustand vor Kauf prüfen."
        service = "standard" if row.get("type") in {"S", "G", "W"} and valid else "individual"
        reports = []
    else:
        width = number(row.get("alloyRimFrontWidth"), Decimal("3"), Decimal("20"))
        diameter = number(row.get("alloyRimFrontDiameter"), Decimal("8"), Decimal("30"))
        valid = bool(width and diameter)
        geometry = {"width": width, "diameter": diameter, "ratio": "",
                    "size": f"{width} × {diameter} Zoll" if valid else "Größenangabe prüfen"}
        category, group = "alloy_wheel", "Alufelge"
        name = " ".join(filter(None, (plain(row.get("manufacturer")), plain(row.get("typeName")) or plain(row.get("type")), plain(row.get("typeColor")))))
        details.update({"Ausführung": plain(row.get("alloyRimFrontDescription")),
                        "Einpresstiefe": number(row.get("alloyRimFrontOffset")) + " mm" if number(row.get("alloyRimFrontOffset")) else "Nicht übermittelt",
                        "Lochzahl": number(row.get("alloyRimFrontHoleNumber")),
                        "Lochkreis": number(row.get("alloyRimFrontHoleCircle")),
                        "Mittenloch": number(row.get("alloyRimFrontHubHole")),
                        "Farbe": plain(row.get("typeColor")),
                        "Fahrzeugpassung": "Für das konkrete Fahrzeug und die Ausführung noch zu prüfen"})
        service, reports = "standard" if valid else "individual", _reports(records)
    merged = {**latest, "row": row}
    stable = {"provider": "ALZURA", "provider_id": latest["provider_id"], "area": area, "variant": _variant(merged)}
    digest = sha256(json.dumps(stable, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()[:24]
    quote, status = _quote({**latest, "row": price_row}, now, dimensions_valid=valid, conflict=bool(conflicts))
    prices = quote["prices_by_quantity"] if quote else {}
    basis = 4 if "4" in prices else min(map(int, prices)) if prices else None
    price = _format_money(prices[str(basis)]["unit_gross_cent"]) if basis else "Preis muss aktualisiert werden"
    vehicles, vehicles_truncated, vehicle_observations = _vehicle_candidates(records) if area == "alloys" else ([], False, [])
    product = {"selection_id": f"{category}:{digest}", "category": category, "name": name or "Produktangaben prüfen",
               "manufacturer_name": plain(row.get("manufacturerName" if area == "tyres" else "manufacturer")),
               **geometry, "dimensions_valid": valid, "group": group, "service_category": service,
               "price": price, "price_quantity_basis": basis, "sample_price_cents": None,
               "pricing_mode": "alzura_live_net_quote", "quote_public": quote, "quote_status": status,
               "photo": "", "gallery": [], "caption": "Produktbild von ALZURA · dargestellte Ausführung prüfen",
               "details": details, "identifiers": identifiers, "approval": None,
               "quality_facts": _quality_facts(price_row) if category == "tyre" and not conflicts else {},
               "technical_reports": reports, "fitment_status": "not_confirmed",
               "vehicle_candidates": vehicles, "vehicle_candidates_truncated": vehicles_truncated}
    evidence = {"selection_id": product["selection_id"], "provider_id": latest["provider_id"],
                "variant": _variant(merged), "conflicts": conflicts,
                "price_observation": latest["source"], "price_list": _serial(price_row.get("priceList")),
                "calculation": {"net_markup_percent": "10.00", "vat_percent": "19.00", "rounding": "ROUND_HALF_UP_unit_then_quantity"},
                "image_urls": list(dict.fromkeys(url for record in records for url in _images(record["row"]))),
                "sources": [record["source"] for record in records],
                "vehicle_candidate_observations": vehicle_observations,
                "rsu_match_status": "not_confirmed", "fitment_approval": False}
    return product, evidence


def build_snapshot(run_dirs, *, now=None):
    now = stamp(now or datetime.now(timezone.utc))
    if not run_dirs:
        raise SnapshotError("run_directory_required")
    grouped, sources = defaultdict(list), []
    for directory in dict.fromkeys(Path(path).resolve() for path in run_dirs):
        records, observations = load_run(directory, now=now)
        sources.extend(observations)
        for record in records:
            grouped[(record["area"], record["provider_id"])].append(record)
    catalogues, evidence = {"reifen": [], "felgen": []}, {}
    for key in sorted(grouped):
        product, private = normalize_product(grouped[key], now=now)
        catalogues["reifen" if product["category"] == "tyre" else "felgen"].append(product)
        evidence[product["selection_id"]] = private
    return {"schema_version": 1, "mode": "licensed_live_local_preview", "generated_at": now.isoformat(),
            "catalogues": catalogues, "sources": sources, "private_evidence": evidence,
            "public_display_ready": False, "orders_enabled": False, "payments_enabled": False,
            "image_cache_status": "not_requested_no_network_in_normalizer"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="Private snapshot JSON; never a public web directory")
    args = parser.parse_args(argv)
    try:
        snapshot = build_snapshot(args.run_dir)
        _atomic_json(args.output, snapshot)
        products = [product for values in snapshot["catalogues"].values() for product in values]
        print(json.dumps({"ok": True, "tyres": len(snapshot["catalogues"]["reifen"]),
                          "wheels": len(snapshot["catalogues"]["felgen"]),
                          "current_quotes": sum(product["quote_public"] is not None for product in products),
                          "network_calls": 0, "orders_or_payments": False}))
        return 0
    except Exception as error:
        code = str(error) if isinstance(error, SnapshotError) else "local_snapshot_failed"
        print(json.dumps({"ok": False, "error": code, "network_calls": 0}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
