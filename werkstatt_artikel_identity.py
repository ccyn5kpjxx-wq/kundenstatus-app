"""Pure, conservative helpers for invoice-derived article proposals.

Matching is intentionally exact after Unicode/case/whitespace normalization.
Similar descriptions are candidates for review, never evidence of identity.
"""

from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import unicodedata


_UNKNOWN_SUPPLIERS = frozenset({"lieferant offen", "lieferant unbekannt", "unbekannt", "unknown"})


def _normalized_text(value):
    if not isinstance(value, str):
        return ""
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def identity_fields(supplier, article_number, product_name, packaging="", unit=""):
    """Return exact normalized identity fields, or None for incomplete data.

    Supplier aliases, SKU punctuation, numbers, units and colors are not guessed
    or removed. A missing SKU is allowed only with a supplier and full name.
    Unknown packaging remains distinct from known packaging. Different fields
    therefore stay separate proposals until someone explicitly resolves them.
    """
    fields = tuple(_normalized_text(value) for value in (
        supplier, article_number, product_name, packaging, unit,
    ))
    if not fields[0] or fields[0] in _UNKNOWN_SUPPLIERS or not fields[2]:
        return None
    return fields


def catalog_identity(supplier, article_number, product_name, packaging="", unit=""):
    """Return a stable versioned identity digest, or None for missing evidence."""
    fields = identity_fields(supplier, article_number, product_name, packaging, unit)
    if fields is None:
        return None
    serialized = json.dumps(["artikel-v1", *fields], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def parse_unit_price(value):
    """Parse a nonnegative explicit price as Decimal, without guessing units.

    Accepts DE/English decimal notation, valid thousands groups, and optional
    EUR/Euro-symbol decoration. A lone comma/dot followed by three digits is
    ambiguous (e.g. 1.234), so returns None. Invalid/ambiguous values and floats
    return None. The caller must independently establish unit-vs-line price,
    currency, tax basis, and source; this helper cannot infer those facts.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (Decimal, int)):
        result = Decimal(value)
        return result if result.is_finite() and result >= 0 else None
    if not isinstance(value, str):
        return None
    text = unicodedata.normalize("NFKC", value).strip()
    if not text or len(text) > 100:
        return None
    if re.match(r"^(?:EUR|€)", text, re.IGNORECASE):
        text = re.sub(r"^(?:EUR|€)\s*", "", text, count=1, flags=re.IGNORECASE)
    else:
        text = re.sub(r"\s*(?:EUR|€)$", "", text, count=1, flags=re.IGNORECASE)
    if not text or re.search(r"[^0-9., ]", text):
        return None

    canonical = None
    if " " in text:
        # Only proper three-digit spacing is a thousands separator.
        if re.fullmatch(r"[0-9]{1,3}(?: [0-9]{3})+(?:[.,][0-9]{1,2})?", text):
            canonical = text.replace(" ", "").replace(",", ".")
    elif "." in text and "," in text:
        if re.fullmatch(r"[0-9]{1,3}(?:\.[0-9]{3})+,[0-9]{1,2}", text):
            canonical = text.replace(".", "").replace(",", ".")
        elif re.fullmatch(r"[0-9]{1,3}(?:,[0-9]{3})+\.[0-9]{1,2}", text):
            canonical = text.replace(",", "")
    elif re.fullmatch(r"[0-9]+(?:[.,][0-9]{1,2})?", text):
        canonical = text.replace(",", ".")
    elif re.fullmatch(r"[0-9]{1,3}(?:\.[0-9]{3}){2,}", text):
        canonical = text.replace(".", "")
    elif re.fullmatch(r"[0-9]{1,3}(?:,[0-9]{3}){2,}", text):
        canonical = text.replace(",", "")
    if canonical is None:
        return None
    try:
        return Decimal(canonical)
    except InvalidOperation:
        return None
