"""Retail amounts shared by the wheel selector, set preview and checkout."""
from collections.abc import Mapping
from datetime import timedelta

from shop_catalog_public import _stamp


def published_wheel_tier(product, quantity, now):
    """Return the exact published tier, subject to its original source and dates."""
    if (product.get('purchase_status') != 'supplier_check_required'
            or product.get('price_status') != 'current'):
        return None
    price = product.get('price')
    if (not isinstance(price, Mapping) or price.get('kind') != 'merchant_pricebook'
            or price.get('source') != 'RSU' or price.get('currency') != 'EUR' or price.get('vat_percent') != '19.00'
            or type(price.get('stock_reported')) is not int or not quantity <= price['stock_reported'] <= 1000000):
        return None
    try:
        observed, expires = _stamp(price.get('observed_at')), _stamp(price.get('expires_at'))
        if not observed <= now < expires or not timedelta(0) < expires - observed <= timedelta(days=7):
            return None
        review = _stamp(price.get('review_at'))
        stock_observed, stock_expires = _stamp(price.get('stock_observed_at')), _stamp(price.get('stock_expires_at'))
        if (review != observed + timedelta(days=5) or not observed <= stock_observed <= now
                or not stock_observed < stock_expires <= stock_observed + timedelta(minutes=15)):
            return None
    except ValueError:
        return None
    amounts = price.get('prices_by_quantity')
    line = amounts.get(str(quantity)) if isinstance(amounts, Mapping) else None
    if (not isinstance(line, Mapping) or type(line.get('quantity')) is not int or line['quantity'] != quantity
            or type(line.get('unit_gross_cent')) is not int or not 1 <= line['unit_gross_cent'] <= 10000000
            or type(line.get('items_gross_cent')) is not int
            or line['items_gross_cent'] != quantity * line['unit_gross_cent']):
        return None
    return line


def tyre_service_gross_cent(code, diameter, quantity, options):
    """Published gross service tariff in Mosbach, applied once per tyre."""
    if (type(diameter) is not int or not 13 <= diameter <= 20
            or type(quantity) is not int or not 1 <= quantity <= 4):
        return None
    if code == 'mounting':
        if not isinstance(options, Mapping) or any(type(options.get(k, False)) is not bool for k in ('runflat', 'disposal')):
            return None
        unit = 2250 if diameter <= 16 else 2500 if diameter <= 18 else 3000
        unit += (500 if options.get('runflat') else 0) + (400 if options.get('disposal') else 0)
    elif code in {'runflat', 'disposal'}:
        unit = 500 if code == 'runflat' else 400
    else:
        return None
    return quantity * unit
