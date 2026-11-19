"""Customer-facing tyre pricing. Supplier quotes and the five-day book retain base prices."""
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, ROUND_CEILING


def tyre_unit_gross_cent(base_unit_gross_cent):
    """Policy v1, also used to validate stored quotes; retain it for historical orders.

    The existing gross price already includes material markup and VAT. Recover
    the fee on the final gross price as net merchant cost, plus output VAT on
    that recovery. Allocate 25 cents per reference purchase of two tyres.
    """
    if type(base_unit_gross_cent) is not int or base_unit_gross_cent <= 0:
        raise ValueError('invalid_tyre_base_price')
    vat = Decimal('1.19')
    fee_rate = Decimal('0.015')
    fixed_share_cent = Decimal('12.5')
    gross = (Decimal(base_unit_gross_cent) + fixed_share_cent * vat) / (1 - fee_rate * vat)
    return int(gross.to_integral_value(rounding=ROUND_CEILING))


def catalogue_tyre_price(quote):
    """Project each supported quantity tier from its existing base price."""
    prices = {}
    for quantity, tier in quote['prices_by_quantity'].items():
        unit = tyre_unit_gross_cent(tier['unit_gross_cent'])
        if unit <= 10_000_000:
            prices[quantity] = {**tier, 'unit_gross_cent': unit, 'items_gross_cent': unit * tier['quantity']}
    return {**quote, 'prices_by_quantity': prices} if prices else None


def cached_tyre_checkout_base(product, quantity, fresh_quote, cached_quote):
    """Bind the displayed tier's base to a fresh RSU procurement and stock check."""
    displayed = cached_quote(product['id'])
    if not isinstance(displayed, Mapping) or displayed.get('source') != 'RSU':
        return None
    tiers = displayed.get('prices_by_quantity')
    tier = tiers.get(str(quantity)) if isinstance(tiers, Mapping) else None
    base = tier.get('unit_gross_cent') if isinstance(tier, Mapping) else None
    if type(base) is not int or base <= 0:
        return None
    current = fresh_quote(product, quantity)
    if (not isinstance(current, Mapping) or type(current.get('unit_gross_cent')) is not int
            or not 0 < current['unit_gross_cent'] <= base):
        return None
    # PublicPriceCache has already freshness-checked the quote; never extend
    # its deadline when binding a more recent quantity-specific stock check.
    deadlines = (datetime.fromisoformat(value['expires_at']) for value in (current, displayed))
    return {**current, 'unit_gross_cent': base, 'expires_at': min(deadlines).isoformat()}
