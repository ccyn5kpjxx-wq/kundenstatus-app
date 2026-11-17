"""Gross-only source projection extracted from reviewed local presentation. No IO."""
from datetime import datetime,timezone

def aware_datetime(value):
    if not isinstance(value, str):
        raise ValueError("An ISO timestamp is required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("An explicit timestamp timezone is required")
    return parsed.astimezone(timezone.utc)

def live_public_quote(value):
    """Whitelist public gross quotes; no supplier costs or arbitrary fields."""
    if not isinstance(value, dict) or value.get("source") != "ALZURA":
        return None
    try:
        observed = aware_datetime(value.get("observed_at"))
        expires = aware_datetime(value.get("expires_at"))
        if not 0 < (expires - observed).total_seconds() <= 900:
            return None
        stock = value.get("stock_reported")
        if type(stock) is not int or not 0 <= stock <= 1_000_000:
            return None
        supplied_prices = value.get("prices_by_quantity")
        if not isinstance(supplied_prices, dict):
            return None
        prices = {}
        for quantity in range(1, 5):
            supplied = supplied_prices.get(str(quantity))
            if not isinstance(supplied, dict):
                continue
            unit, items = supplied.get("unit_gross_cent"), supplied.get("items_gross_cent")
            if (type(supplied.get("quantity")) is not int or supplied["quantity"] != quantity
                    or type(unit) is not int or not 0 < unit <= 10_000_000
                    or type(items) is not int or items != quantity * unit
                    or supplied.get("vat_percent") != "19.00"):
                continue
            prices[str(quantity)] = {"quantity": quantity, "unit_gross_cent": unit,
                                     "items_gross_cent": items, "vat_percent": "19.00"}
        return {"source": "ALZURA", "observed_at": observed.isoformat(),
                "expires_at": expires.isoformat(), "stock_reported": stock,
                "prices_by_quantity": prices, "shipping_status": "not_confirmed",
                "rsu_match_status": "not_confirmed"}
    except (ValueError, TypeError, OverflowError):
        return None
