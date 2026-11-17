"""Offline nominal tyre/rim-width proposals, never a vehicle fitment approval.

The reviewed registry is a manufacturer's dimensional table. Matching a listed
rim width and a tyre's declared nominal size does not resolve wheel contour,
actual tyre execution, load/speed suitability or any vehicle/certificate rule.
No values produced here belong in ``fitment_spec`` or a vehicle approval rule.
"""

from collections import Counter
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re
from urllib.parse import urlsplit


REGISTRY_SCOPE = "continental_group_dimensional_proposals_only"
DOCUMENTED_BRANDS = frozenset(name.casefold() for name in (
    "Continental", "Uniroyal", "Semperit", "Barum", "General Tire", "Viking",
    "Gislaved", "Mabor", "Matador", "Sportiva"))
UNRESOLVED = ("vehicleapproval", "flangecontour", "actualexecution", "loadspeedvehicleconditions")
ID = re.compile(r"(?:tyre|alloy_wheel|steel_wheel):[a-f0-9]{24}")
SIZE_SERVICE = re.compile(
    r"(?<![0-9A-Z])(?P<width>[1-9][0-9]{2})\s*/\s*(?P<ratio>[1-9][0-9]?)"
    r"\s+R\s*(?P<diameter>[1-9][0-9])\s+(?P<li>[1-9][0-9]{1,2})\s*"
    r"(?P<speed>A[1-8]|[BCDEFGHJKLMNPQRSTUVWY])(?=$|[^A-Z0-9/])", re.I)
SPECIAL = re.compile(
    r"\b(?:SSR|RFT|ROF|RUN[ -]?FLAT|RUN[ -]?ON[ -]?FLAT|DSST|ZP|RSC|EMT|HRS|CST|"
    r"CONTISEAL|CONTISILENT|SELFSEAL|SEAL[ -]?INSIDE|RETREAD(?:ED)?|"
    r"RUND[ -]?ERNEUERT|DEMO|HL)\b|(?<![A-Z])DOT(?:[ -]?[0-9]{2,4})?(?![A-Z])", re.I)


def _decimal(value, *, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    text = str(value)
    if not re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", text):
        return None
    try:
        number = Decimal(text.replace(",", "."))
    except InvalidOperation:
        return None
    return number if number.is_finite() and Decimal(str(minimum)) <= number <= Decimal(str(maximum)) else None


def _integer(value, *, minimum, maximum):
    number = _decimal(value, minimum=minimum, maximum=maximum)
    return int(number) if number is not None and number == number.to_integral_value() else None


def tyre_nominal_facts(product):
    """Return only explicit, mutually consistent nominal tyre facts or None.

    Manufacturer membership must come from the normalized native provider field,
    never the display name. No XL token means unknown, not standard-load proof.
    """
    if not isinstance(product, dict) or product.get("category") != "tyre" or product.get("dimensions_valid") is not True:
        return None
    identifier = product.get("selection_id")
    if not isinstance(identifier, str) or not ID.fullmatch(identifier) or not identifier.startswith("tyre:"):
        return None
    manufacturer = product.get("manufacturer_name")
    if not isinstance(manufacturer, str) or not manufacturer.strip() or len(manufacturer) > 150:
        return None
    details = product.get("details")
    if not isinstance(details, dict) or any(key in details for key in ("DOT-Ausführung", "Demoware")):
        return None
    description = details.get("Artikelbezeichnung")
    if not isinstance(description, str) or not description or len(description) > 2000:
        return None
    extra = details.get("Zusatzmerkmale", "")
    name = product.get("name", "")
    if not isinstance(extra, str) or not isinstance(name, str):
        return None
    observed_text = " ".join((manufacturer, name, description, extra))
    if SPECIAL.search(observed_text) or product.get("is_dot") is True or product.get("is_demo") is True:
        return None
    matches = list(SIZE_SERVICE.finditer(description))
    if len(matches) != 1:
        return None
    match = matches[0]
    facts = {"width_mm": int(match["width"]), "ratio_percent": int(match["ratio"]),
             "diameter_in": int(match["diameter"]), "load_index": int(match["li"]),
             "speed_index": match["speed"].upper()}
    if not 50 <= facts["load_index"] <= 200:
        return None
    normalized = (_integer(product.get("width"), minimum=100, maximum=999),
                  _integer(product.get("ratio"), minimum=10, maximum=99),
                  _integer(product.get("diameter"), minimum=8, maximum=30))
    if normalized != (facts["width_mm"], facts["ratio_percent"], facts["diameter_in"]):
        return None
    reported_speed = details.get("Geschwindigkeitsindex")
    if reported_speed not in (None, "") and reported_speed != facts["speed_index"]:
        return None
    facts.update({"manufacturer_name": manufacturer.strip(), "xl": True if re.search(r"\bXL\b", observed_text, re.I) else None,
                  "selection_id": identifier})
    return facts


def _registry(registry):
    if (not isinstance(registry, dict) or type(registry.get("schema_version")) is not int
            or registry["schema_version"] != 1 or registry.get("scope") != REGISTRY_SCOPE
            or registry.get("is_vehicle_approval") is not False or registry.get("construction") != "R"):
        return None
    contours = registry.get("permitted_rim_contours")
    if not isinstance(contours, list) or not contours or any(not isinstance(value, str) or value not in {"J", "JK", "JJ"} for value in contours):
        return None
    source, brands, rows = registry.get("source"), registry.get("brands"), registry.get("rows")
    if not isinstance(source, dict) or not isinstance(brands, list) or not brands or not isinstance(rows, list):
        return None
    if any(not isinstance(brand, str) or brand.strip().casefold() not in DOCUMENTED_BRANDS for brand in brands):
        return None
    url, digest = source.get("url"), source.get("sha256")
    if not isinstance(url, str) or not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
        return None
    try:
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.hostname != "www.continental-reifen.de"
                or parsed.username or parsed.password or parsed.port not in (None, 443)
                or parsed.query or parsed.fragment or not parsed.path.lower().endswith(".pdf")
                or any(ord(char) < 32 for char in url)):
            return None
    except ValueError:
        return None
    return {"url": url, "sha256": digest}, {brand.strip().casefold() for brand in brands}, rows


def _reviewed_row(row):
    if not isinstance(row, dict) or row.get("visually_reviewed") is not True:
        return None
    width = _integer(row.get("width_mm"), minimum=100, maximum=999)
    ratio = _integer(row.get("ratio_percent"), minimum=10, maximum=99)
    diameter = _integer(row.get("diameter_in"), minimum=8, maximum=30)
    page = _integer(row.get("pdf_page"), minimum=1, maximum=2000)
    printed = _integer(row.get("printed_page"), minimum=1, maximum=4000)
    if None in (width, ratio, diameter, page, printed) or row.get("size") != f"{width}/{ratio} R{diameter}":
        return None
    permitted = row.get("permitted_rim_widths_in")
    variants = row.get("load_variants")
    if not isinstance(permitted, list) or not permitted or not isinstance(variants, list) or not variants:
        return None
    widths = [_decimal(value, minimum=3, maximum=20) for value in permitted]
    if any(value is None for value in widths) or len(widths) != len(set(widths)):
        return None
    clean_variants = []
    for variant in variants:
        if not isinstance(variant, dict) or type(variant.get("xl")) is not bool:
            return None
        index = _integer(variant.get("load_index"), minimum=50, maximum=200)
        if index is None:
            return None
        clean_variants.append((index, variant["xl"]))
    return {"width_mm": width, "ratio_percent": ratio, "diameter_in": diameter,
            "widths": set(widths), "load_variants": clean_variants, "page": page, "printed_page": printed,
            "size": row["size"]}


def build_width_candidates(catalogues, reviewed_registry):
    """Map rim IDs to dimensional proposals; all full-fitment gates stay open."""
    registry = _registry(reviewed_registry)
    if registry is None or not isinstance(catalogues, dict):
        return {}
    source, brands, supplied = registry
    rows = [row for row in map(_reviewed_row, supplied) if row is not None]
    tyres, rims = catalogues.get("reifen", []), catalogues.get("felgen", [])
    if not isinstance(tyres, list) or not isinstance(rims, list):
        return {}
    tyre_ids = Counter(row.get("selection_id") for row in tyres if isinstance(row, dict) and isinstance(row.get("selection_id"), str))
    rim_ids = Counter(row.get("selection_id") for row in rims if isinstance(row, dict) and isinstance(row.get("selection_id"), str))
    eligible_tyres = []
    for tyre in tyres:
        facts = tyre_nominal_facts(tyre)
        if facts is None or tyre_ids[facts["selection_id"]] != 1 or facts["manufacturer_name"].casefold() not in brands:
            continue
        matches = [row for row in rows if (row["width_mm"], row["ratio_percent"], row["diameter_in"])
                   == (facts["width_mm"], facts["ratio_percent"], facts["diameter_in"])
                   and any(index == facts["load_index"] and (facts["xl"] is None or xl is True)
                           for index, xl in row["load_variants"])]
        # Conflicting/duplicate reviewed entries cannot silently choose a source.
        if len(matches) == 1:
            eligible_tyres.append((facts, matches[0]))
    result = {}
    for rim in rims:
        if (not isinstance(rim, dict) or not isinstance(rim.get("category"), str)
                or rim["category"] not in {"alloy_wheel", "steel_wheel"} or rim.get("dimensions_valid") is not True):
            continue
        rim_id = rim.get("selection_id")
        if not isinstance(rim_id, str) or not ID.fullmatch(rim_id) or rim_ids[rim_id] != 1 or not rim_id.startswith(rim["category"] + ":"):
            continue
        width = _decimal(rim.get("width"), minimum=3, maximum=20)
        diameter = _integer(rim.get("diameter"), minimum=8, maximum=30)
        if width is None or diameter is None:
            continue
        candidates = []
        seen = set()
        for facts, row in eligible_tyres:
            if facts["diameter_in"] != diameter or width not in row["widths"] or facts["selection_id"] in seen:
                continue
            seen.add(facts["selection_id"])
            evidence = {"sha256": source["sha256"], "page": row["page"], "size": row["size"],
                        "rim_width": str(width.normalize()), "load_index": facts["load_index"], "xl": facts["xl"]}
            evidence_id = "rim-width:" + sha256(json.dumps(evidence, sort_keys=True).encode("utf-8")).hexdigest()[:24]
            candidates.append({"tyre_selection_id": facts["selection_id"], "evidence_id": evidence_id,
                               "scope": "rim_width_candidate", "source": {**source, "page": row["page"]},
                               "unresolved": list(UNRESOLVED),
                               "label": "Nennmaß-/Breitenabgleich · Fahrzeug und genaue Ausführung noch prüfen"})
        if candidates:
            result[rim_id] = candidates
    return result


def released_width_pairs(bundle, *, approved_ids):
    """Source-bound proposals for priced exact SKUs; no vehicle approval.

    Extra source text is checked against the already validated source references
    before it is used for nominal-size and special-execution exclusions. It is
    never copied into public product descriptions or supplier order payloads.
    """
    from shop_catalog_public import variant_fingerprint
    from shop_rsu_rim_evidence import WIDTH_TABLE_SHA
    from shop_rsu_quotes import _binding

    if (not isinstance(bundle, Mapping) or not isinstance(approved_ids, (set, frozenset))
            or len(approved_ids) > 100000):
        raise ValueError('invalid_nominal_release')
    registry = bundle.get('reviewed_rim_width_registry')
    if registry is None:
        return deepcopy(bundle.get('compatible_pairs', {}))
    if (not isinstance(registry, dict) or registry.get('source', {}).get('sha256') != WIDTH_TABLE_SHA
            or not isinstance(registry.get('rows'), list) or len(registry['rows']) > 1000
            or _registry(registry) is None):
        raise ValueError('invalid_nominal_registry')
    rows = bundle.get('products')
    if not isinstance(rows, list) or len(rows) > 100000:
        raise ValueError('invalid_nominal_release')
    by = {p.get('selection_id', p.get('id')): p for p in rows}
    bindings = bundle.get('rsu_bindings', {})
    sources = bundle.get('nominal_tyre_sources', {})
    if not isinstance(sources, Mapping) or len(sources) > 10000 or not set(sources) <= approved_ids:
        raise ValueError('invalid_nominal_sources')
    fields = {'rule','product_id','identity_hash','alzura_source_sha256','alzura_row_number',
              'alzura_article_id','articlename','description','info','manufacturer_description',
              'runflat','rsu_source_sha256','rsu_row_number','rsu_profile'}
    tyres = []
    fingerprints = {identifier: variant_fingerprint(p) for identifier,p in by.items()}
    for identifier, record in sources.items():
        if (not isinstance(record, Mapping) or set(record) != {'proof','proof_sha256'}
                or not isinstance(record['proof'], Mapping) or set(record['proof']) != fields):
            raise ValueError('invalid_nominal_source')
        proof = record['proof']
        canonical = json.dumps(proof, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        product = by.get(identifier)
        binding = bindings.get(identifier)
        if (not product or product.get('category') != 'tyre' or not isinstance(binding, Mapping)
                or proof['rule'] != 'alzura_full_export_nominal_text_v1'
                or proof['product_id'] != identifier or proof['identity_hash'] != fingerprints[identifier]
                or record['proof_sha256'] != sha256(canonical.encode('utf-8')).hexdigest()
                or proof['runflat'] != '0'
                or any(not isinstance(proof[key], str) or len(proof[key]) > 2000
                       for key in ('articlename','description','info','manufacturer_description','rsu_profile'))):
            raise ValueError('invalid_nominal_source')
        _binding(identifier, binding)
        evidence = binding['mapping']['evidence']
        normalized = lambda value: ' '.join(value.casefold().split()) if isinstance(value,str) else None
        if (binding['identity_hash'] != fingerprints[identifier]
                or normalized(product.get('manufacturer_name')) != evidence['manufacturer']
                or normalized(product.get('article_number')) != evidence['mpn']):
            raise ValueError('nominal_product_source_conflict')
        left,right = evidence['alzura'],evidence['rsu']
        if (proof['alzura_source_sha256'] != left['source_sha256']
                or proof['alzura_row_number'] != left['row_number']
                or proof['alzura_article_id'] != left['article_id']
                or proof['rsu_source_sha256'] != right['source_sha256']
                or proof['rsu_row_number'] != right['row_number']
                or left['features']['runflat'] is not False):
            raise ValueError('invalid_nominal_source_reference')
        shadow = deepcopy(product)
        shadow['details'] = {**shadow.get('details', {}), 'Artikelbezeichnung': proof['articlename'],
            'Zusatzmerkmale': ' '.join(proof[key] for key in ('description','info','manufacturer_description','rsu_profile')),
            'Geschwindigkeitsindex': evidence['core']['speed_index'].upper()}
        facts = tyre_nominal_facts(shadow)
        if facts is None:
            continue
        core = evidence['core']
        if (facts['width_mm'],facts['ratio_percent'],facts['diameter_in'],facts['load_index'],facts['speed_index']) != (
                int(core['width_mm']),int(core['aspect_ratio_percent']),int(core['diameter_inch']),
                int(core['load_index']),core['speed_index'].upper()):
            raise ValueError('nominal_size_source_conflict')
        tyres.append(shadow)
    rims = [p for p in rows if p.get('category') in {'alloy_wheel','steel_wheel'}]
    proposals = build_width_candidates({'reifen':tyres,'felgen':rims}, registry)
    result = deepcopy(bundle.get('compatible_pairs', {}))
    seasons = {'Sommerreifen':'summer','Winterreifen':'winter','Ganzjahresreifen':'allseason',
               'summer':'summer','winter':'winter','allseason':'allseason'}
    for identifier, candidates in proposals.items():
        existing = result.setdefault(identifier, [])
        used = {pair.get('tyre_id') for pair in existing}
        selected = Counter()
        def rank(candidate):
            p=by[candidate['tyre_selection_id']]
            return (p.get('manufacturer_name','').casefold(),p.get('name',''),candidate['tyre_selection_id'])
        for candidate in sorted(candidates,key=rank):
            tyre_id=candidate['tyre_selection_id'];tyre=by[tyre_id]
            season=seasons.get(tyre.get('season')) or seasons.get(tyre.get('group'))
            if tyre_id in used or season is None or selected[season] >= 5:
                continue
            existing.append({'reviewed':True,'scope':'rim_width_candidate','tyre_id':tyre_id,
                'rim_identity':fingerprints[identifier],'tyre_identity':fingerprints[tyre_id],
                'source':candidate['source'],'evidence_id':candidate['evidence_id']})
            used.add(tyre_id);selected[season]+=1
    return result
