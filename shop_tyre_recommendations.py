"""Pure price comparison of already documented tyre offers.

The caller establishes exact rim compatibility, source membership and document
availability before calling this module. A comparison never grants vehicle
fitment, stock or checkout permission. Price position is not a quality rating.
"""
from collections.abc import Mapping
from copy import deepcopy
import re
import unicodedata

from shop_catalog_public import _stamp
from shop_wheel_prices import published_wheel_tier

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
            or product.get('department') != 'tyres' or not isinstance(product.get('id'), str)
            or not ID.fullmatch(product['id']) or not isinstance(product.get('brand'), str)
            or not 1 <= len(product['brand']) <= 90 or not _norm(product['brand'])
            or not isinstance(product.get('name'), str) or not 1 <= len(product['name']) <= 180
            or not _norm(product['name'])):
        return None
    size = tyre_size_key(product)
    line = published_wheel_tier(product, quantity, now)
    if size is None or line is None:
        return None
    family = (_norm(product['brand']), _norm(product['name']))
    return {'product': product, 'size': size, 'family': family, 'unit': line['unit_gross_cent']}


def _price_order(offer):
    return offer['unit'], offer['family'], offer['product']['id']


def _include_tested_offer(offers, tested, limit):
    """Keep the two price anchors and show the best tested execution in its own right."""
    if limit < 3 or any(x['product']['id'] == tested['product']['id'] for x in offers):
        return offers
    middle = range(1, len(offers) - 1)
    same_model = next((i for i in middle if offers[i]['family'] == tested['family']), None)
    if same_model is not None:
        offers[same_model] = tested
    elif len(offers) < limit:
        offers.append(tested)
    else:
        # Keep the other price points spread out by replacing the nearest interior price.
        nearest = min(middle, key=lambda i: abs(offers[i]['unit'] - tested['unit']))
        offers[nearest] = tested
    return sorted(offers, key=_price_order)


def select_priced_tyres(products, *, quantity=4, limit=5, now, size_key=None):
    """Choose a same-size, model-diverse price ladder without side effects.

    Current prices and sufficient reported quantities are mandatory. Missing
    offers are never padded with inquiry or unpriced cards. An explicit size
    request is never silently replaced by a different nominal size. Otherwise
    the size with the most distinct models wins, followed by brand diversity,
    lowest actual price and the numeric size as stable tie breakers.

    Selection preserves both model-price extremes. Between them it includes
    the best exact ADAC-tested offer and spreads other cards over price ranks.
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
    cohorts, tested_by_size = {}, {}
    for identifier, product in by_id.items():
        if identifier in conflicts:
            continue
        offer = _offer(product, quantity, current)
        if offer is None:
            continue
        rating = product.get('test_rating')
        if isinstance(rating, Mapping) and rating.get('source') == 'ADAC':
            grade = rating.get('grade')
            if type(grade) in (int, float) and 0.6 <= grade <= 5.5:
                rating_order = (grade, _price_order(offer))
                best = tested_by_size.get(offer['size'])
                if best is None or rating_order < best[0]:
                    tested_by_size[offer['size']] = rating_order, offer
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
    offers = sorted(cohorts.get(selected, {}).values(), key=_price_order)
    if len(offers) > limit:
        if limit == 1:
            offers = offers[:1]
        else:
            # Integer nearest-rank calculation, independent of float rounding.
            denominator = limit - 1
            positions = [(2 * i * (len(offers) - 1) + denominator) // (2 * denominator) for i in range(limit)]
            offers = [offers[position] for position in positions]
    if selected in tested_by_size:
        offers = _include_tested_offer(offers, tested_by_size[selected][1], limit)
    return {'items': [deepcopy(offer['product']) for offer in offers],
            'available_sizes': available, 'selected_size': selected,
            'rated_sizes': [size for size in available if size in tested_by_size]}
