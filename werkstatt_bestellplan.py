"""Pure validation and scheduling for explicitly requested supplier orders.

This module neither sends mail nor reads/writes a database. Callers must persist
the due time when an order is requested, select due unsent orders, and enforce
durable send idempotency separately. Recomputing an overdue due time would defer
it by another week. Invoice history is evidence for products, never an order.
"""

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
from zoneinfo import ZoneInfo


BERLIN = ZoneInfo("Europe/Berlin")
_UNKNOWN = frozenset({"unknown", "unbekannt", "offen", "none", "null"})
_MAILBOX = re.compile(
    r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
    r"@(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z]{2,63}"
)


def next_dispatch_at(now: datetime, urgent: bool) -> datetime:
    """Return an aware UTC dispatch time: now, or Monday noon in Berlin.

    Exactly Monday 12:00:00 is eligible immediately. Any later instant is due
    the following Monday. Berlin calendar arithmetic preserves local noon over
    the spring/autumn daylight-saving transitions, unlike adding 168 UTC hours.
    Urgency must have been answered explicitly; strings such as 'false' fail.
    """
    if not isinstance(now, datetime):
        raise TypeError("now muss ein datetime sein.")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now braucht eine Zeitzone.")
    if type(urgent) is not bool:
        raise TypeError("urgent muss ausdrücklich True oder False sein.")
    now_utc = now.astimezone(timezone.utc)
    if urgent:
        return now_utc
    local = now_utc.astimezone(BERLIN)
    monday = local.date() + timedelta(days=(-local.weekday()) % 7)
    cutoff = datetime.combine(monday, time(12), tzinfo=BERLIN)
    if cutoff.astimezone(timezone.utc) < now_utc:
        cutoff = datetime.combine(monday + timedelta(days=7), time(12), tzinfo=BERLIN)
    return cutoff.astimezone(timezone.utc)


def _text(value):
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or value.casefold() in _UNKNOWN or any(ord(c) < 32 for c in value):
        return None
    return value


def _identifier(value):
    if type(value) is int:
        return str(value) if value > 0 else None
    value = _text(value)
    if value is None or (re.fullmatch(r"-?[0-9]+", value) and int(value) <= 0):
        return None
    return value


def _quantity(value):
    if isinstance(value, bool) or not isinstance(value, (int, str, Decimal)):
        return None
    if isinstance(value, str):
        value = value.strip()
        # No inferred thousands separators, packaging factors or invoice totals.
        if not re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", value):
            return None
        value = value.replace(",", ".")
    try:
        quantity = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    if not quantity.is_finite() or quantity <= 0:
        return None
    fixed = format(quantity, "f")
    return fixed.rstrip("0").rstrip(".") if "." in fixed else fixed


def _recipient(value):
    value = _text(value)
    if value is None or len(value) > 254 or not _MAILBOX.fullmatch(value):
        return None
    local, domain = value.rsplit("@", 1)
    return local + "@" + domain.lower()


def group_orders(records: Iterable[Mapping]) -> dict:
    """Validate requests, then group by supplier, verified recipient and urgency.

    Required keys: id, order_requested=True, supplier_id, recipient,
    recipient_verified=True, product_id OR article_number, variant, quantity,
    unit, max_total_cents (positive integer) and urgent (explicit bool).
    A variant-free product still needs an explicit variant such as 'Standard'.
    The caller must resolve and verify catalog identity and recipient ownership;
    booleans here are attestations from that trusted application layer.

    Returns JSON-serializable {'groups': [...], 'invalid': [...]}. Groups contain
    supplier_id, recipient, urgent, orders and the sum of line max_total_cents.
    Each invalid item contains index, id, missing_fields and actionable errors.
    Duplicate request IDs invalidate ALL matching input entries (even identical
    ones); this is not a replacement for persistent cross-call idempotency.

    Quantities remain separate per request, even for identical products. Only
    the explicit quantity is considered; historical invoice quantities/prices
    and arbitrary extra input fields are never forwarded. Inputs are unchanged.
    group_orders does not select due orders or combine urgent and weekly mail.
    """
    records = list(records)
    ids = [_identifier(record.get("id")) if isinstance(record, Mapping) else None
           for record in records]
    duplicates = {key for key, count in Counter(ids).items() if key is not None and count > 1}
    grouped = {}
    invalid = []
    for index, record in enumerate(records):
        errors = {}
        if not isinstance(record, Mapping):
            invalid.append({"index": index, "id": None, "missing_fields": ["record"],
                            "errors": {"record": "Eine vollständige Bestellposition angeben."}})
            continue
        request_id = ids[index]
        if request_id is None:
            errors["id"] = "Eine eindeutige Bestellanforderungs-ID vergeben."
        elif request_id in duplicates:
            errors["id"] = "Doppelte Bestellanforderungs-ID vor dem Versand auflösen."
        if record.get("order_requested") is not True:
            errors["order_requested"] = "Die Position ausdrücklich als Bestellung anfordern."
        supplier = _identifier(record.get("supplier_id"))
        if supplier is None:
            errors["supplier_id"] = "Den zugehörigen Lieferanten auswählen."
        recipient = _recipient(record.get("recipient"))
        if recipient is None:
            errors["recipient"] = "Eine einzelne gültige Bestelladresse des Lieferanten hinterlegen."
        if record.get("recipient_verified") is not True:
            errors["recipient_verified"] = "Bestätigen, dass die Adresse Bestellungen für diesen Lieferanten annimmt."
        product = _identifier(record.get("product_id"))
        article = _identifier(record.get("article_number"))
        if product is None and article is None:
            errors["product_id_or_article_number"] = "Den genauen Katalogartikel oder die Lieferantenartikelnummer auswählen."
        variant = _text(record.get("variant"))
        if variant is None:
            errors["variant"] = "Welche Größe, Farbe oder genaue Variante soll bestellt werden?"
        quantity = _quantity(record.get("quantity"))
        if quantity is None:
            errors["quantity"] = "Welche positive Menge soll jetzt bestellt werden?"
        unit = _text(record.get("unit"))
        if unit is None:
            errors["unit"] = "In welcher Bestelleinheit, etwa Rollen oder Packungen?"
        cap = record.get("max_total_cents")
        if type(cap) is not int or cap <= 0:
            errors["max_total_cents"] = "Den erlaubten Gesamtbetrag dieser Position in Cent festlegen."
        urgent = record.get("urgent")
        if type(urgent) is not bool:
            errors["urgent"] = "Ist die Bestellung dringend?"
        if errors:
            invalid.append({"index": index, "id": request_id,
                            "missing_fields": list(errors), "errors": errors})
            continue
        key = (supplier, recipient, urgent)
        if key not in grouped:
            grouped[key] = {"supplier_id": supplier, "recipient": recipient,
                            "urgent": urgent, "orders": [], "max_total_cents": 0}
        group = grouped[key]
        group["orders"].append({"id": request_id, "product_id": product,
                                "article_number": article, "variant": variant,
                                "quantity": quantity, "unit": unit,
                                "max_total_cents": cap})
        group["max_total_cents"] += cap
    return {"groups": list(grouped.values()), "invalid": invalid}
