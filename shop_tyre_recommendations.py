"""Pure price comparison of already documented tyre offers.

The caller establishes exact rim compatibility, source membership and document
availability before calling this module. A comparison never grants vehicle
fitment, stock or checkout permission. Price position is not a quality rating.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import timedelta
import re
import unicodedata

from shop_catalog_public import _stamp

ID = re.compile(r'tyre:[a-f0-9]{24}')
SIZE = re.compile(r'([1-9][0-9]{2})/([1-9][0-9]{1,2}) R([1-9][0-9]?)')
MAX_OFFERS = 10000


def _norm(value):
    return ' '.join(unicodedata.normalize('NFKC', value).split()).casefold()


def _dimension(value, minimum, maximum):
    # Use structured source geometry, never a size guessed from a model title.
    if type(value) not in (str, int) or not re.fullmatch(r'[0-9]{1,3}(?:\.0{1,4})?', str(value)):
        return None
    number = int(str(value).split('.')[0])
    return number if minimum <= number <= maximum else None


def _size_numbers(value):
    if not isinstance(value, str):
        return None
    match = SIZE.fullmatch(value)
    if not match:
        return None
    numbers = tuple(map(int, match.groups()))
    return numbers if 100 <= numbers[0] <= 455 and 10 <= numbers[1] <= 100 and 8 <= numbers[2] <= 30 else None


def tyre_size_key(product):
    """Return one exact nominal-size label or None; no load/speed inference."""
    if not isinstance(product, Mapping) or product.get('category') != 'tyre':
        return None
    geometry = product.get('geometry')
    if not isinstance(geometry, Mapping):
        return None
    numbers = (_dimension(geometry.get('width'), 100, 455),
               _dimension(geometry.get('ratio'), 10, 100),
               _dimension(geometry.get('diameter'), 8, 30))
    return f'{numbers[0]}/{numbers[1]} R{numbers[2]}' if None not in numbers else None


def _offer(product, quantity, now):
    if (not isinstance(product, Mapping) or product.get('category') != 'tyre'
            or product.get('department') != 'tyres' or product.get('purchase_status') != 'supplier_check_required'
            or product.get('price_status') != 'current' or not isinstance(product.get('id'), str)
            or not ID.fullmatch(product['id']) or not isinstance(product.get('brand'), str)
            or not 1 <= len(product['brand']) <= 90 or not _norm(product['brand'])
            or not isinstance(product.get('name'), str) or not 1 <= len(product['name']) <= 180
            or not _norm(product['name'])):
        return None
    size = tyre_size_key(product)
    price = product.get('price')
    if (size is None or not isinstance(price, Mapping) or price.get('kind') != 'merchant_pricebook'
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
    family = (_norm(product['brand']), _norm(product['name']))
    return {'product': product, 'size': size, 'family': family, 'unit': line['unit_gross_cent']}


def select_priced_tyres(products, *, quantity=4, limit=5, now, size_key=None):
    """Choose a same-size, model-diverse price ladder without side effects.

    Current prices and sufficient reported quantities are mandatory. Missing
    offers are never padded with inquiry or unpriced cards. An explicit size
    request is never silently replaced by a different nominal size. Otherwise
    the size with the most distinct models wins, followed by brand diversity,
    lowest actual price and the numeric size as stable tie breakers.

    Selection preserves both price extremes and spreads remaining cards over
    rank quantiles. Neither rank nor cost creates a Budget/Premium label.
    """
    if (not isinstance(products, (list, tuple)) or len(products) > MAX_OFFERS
            or type(quantity) is not int or not 1 <= quantity <= 4
            or type(limit) is not int or not 1 <= limit <= 5
            or size_key is not None and _size_numbers(size_key) is None):
        raise ValueError('invalid_tyre_recommendation_request')
    try:
        current = _stamp(now)
    except ValueError:
        raise ValueError('invalid_tyre_recommendation_clock') from None
    # Conflicting duplicate IDs cannot choose whichever price happens to be
    # encountered first. Equivalent duplicates are harmless and count once.
    by_id, conflicts = {}, set()
    for product in products:
        if not isinstance(product, Mapping) or not isinstance(product.get('id'), str):
            continue
        identifier = product['id']
        if identifier in by_id and by_id[identifier] != product:
            conflicts.add(identifier)
        else:
            by_id[identifier] = product
    cohorts = {}
    for identifier, product in by_id.items():
        if identifier in conflicts:
            continue
        offer = _offer(product, quantity, current)
        if offer is None:
            continue
        models = cohorts.setdefault(offer['size'], {})
        previous = models.get(offer['family'])
        # A model's load/speed variants are not separate comparison cards. The
        # chosen original offer still shows its actual load/speed execution.
        if previous is None or (offer['unit'], identifier) < (previous['unit'], previous['product']['id']):
            models[offer['family']] = offer
    available = sorted(cohorts, key=_size_numbers)
    if size_key is None and cohorts:
        def rank(size):
            offers = list(cohorts[size].values())
            return (-len(offers), -len({offer['family'][0] for offer in offers}),
                    min(offer['unit'] for offer in offers), _size_numbers(size))
        selected = min(cohorts, key=rank)
    else:
        selected = size_key
    offers = sorted(cohorts.get(selected, {}).values(),
                    key=lambda offer: (offer['unit'], offer['family'], offer['product']['id']))
    if len(offers) > limit:
        if limit == 1:
            offers = offers[:1]
        else:
            # Integer nearest-rank calculation, independent of float rounding.
            denominator = limit - 1
            positions = [(2 * i * (len(offers) - 1) + denominator) // (2 * denominator) for i in range(limit)]
            offers = [offers[position] for position in positions]
    return {'items': [deepcopy(offer['product']) for offer in offers],
            'available_sizes': available, 'selected_size': selected}
