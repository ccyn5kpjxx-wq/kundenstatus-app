"""Private pricing preparation for the local parts/accessories preview.

Partner costs remain in this module/private input. Only gross consumer prices
are passed to the HTML builder. No provider requests, orders or payments.
"""
import ast
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
import json
from pathlib import Path
import re

WORK = Path(__file__).resolve().parent
PRIVATE = WORK / 'shop-parts-private'
SOURCE = WORK / 'reifenshop-katalog-aktuell' / 'werkstatt_buero_preise.py'
PAINT_KEYS = {'Kotflügel': ('fender',), 'Tür': ('door',),
              'Seitenwand': ('side_panel',), 'Kofferraumdeckel': ('boot_lid',),
              'Motorhaube': ('bonnet',),
              'Stoßstange vorne oder hinten': ('front_bumper', 'rear_bumper')}


def gross_from_net(value):
    """EUR partner net -> gross cents, 15% net markup, then 19% VAT."""
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError('net_price_invalid')
    if not re.fullmatch(r'[0-9]{1,8}(?:\.[0-9]{1,6})?', str(value)):
        raise ValueError('net_price_invalid')
    try:
        net = Decimal(value)
        if not net.is_finite() or not 0 < net <= 100000:
            raise ValueError('net_price_invalid')
        return int((net * Decimal('1.15') * Decimal('1.19') * 100)
                   .quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    except InvalidOperation as exc:
        raise ValueError('net_price_invalid') from exc


def painting_tariffs(source=SOURCE):
    """Direct Neuteil partner painting tariff plus VAT; no parts resale markup."""
    source = Path(source)
    values = {}
    for statement in ast.parse(source.read_text(encoding='utf-8')).body:
        if isinstance(statement, ast.Assign):
            for target in statement.targets:
                if isinstance(target, ast.Name) and target.id in {'PAINT_PRICES', 'PDF_SHA256'}:
                    values[target.id] = ast.literal_eval(statement.value)
    pdf = source.parent / 'resources' / 'buero' / 'Interne_Preisliste_Netto_Arbeitsstand.pdf'
    if sha256(pdf.read_bytes()).hexdigest() != values.get('PDF_SHA256'):
        raise ValueError('painting_reference_changed')
    public = {}
    for name, new_partner, _new_private, _used_partner, _used_private, provisional in values['PAINT_PRICES']:
        if not provisional and name in PAINT_KEYS:
            for key in PAINT_KEYS[name]:
                public[key] = int((Decimal(new_partner) * Decimal('1.19'))
                                  .quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    if len(public) != 7:
        raise ValueError('painting_reference_incomplete')
    return public


def _text(value, maximum=200):
    return re.sub(r'[\x00-\x1f\x7f]', ' ', str(value or '')).strip()[:maximum]


def public_quote(value):
    if not isinstance(value, dict):
        return None
    try:
        observed = datetime.fromisoformat(value['observed_at'].replace('Z', '+00:00'))
        expires = datetime.fromisoformat(value['expires_at'].replace('Z', '+00:00'))
        unit, stock = value['unit_gross_cent'], value['stock']
        if (observed.utcoffset() is None or expires.utcoffset() is None
                or not 0 < (expires-observed).total_seconds() <= 900
                or type(unit) is not int or not 0 < unit <= 10_000_000
                or type(stock) is not int or not 0 <= stock <= 1_000_000):
            return None
        result = {'unit_gross_cent':unit,'observed_at':observed.isoformat(),
                  'expires_at':expires.isoformat(),'stock':stock}
        base = value.get('base_price_gross_cent')
        if value.get('base_price_unit') == 'L' and type(base) is int and 0 < base <= 10_000_000:
            result.update(base_price_gross_cent=base,base_price_unit='L')
        return result
    except (KeyError, ValueError, TypeError, OverflowError):
        return None


def _public_fitment(row):
    """Expose only labels backed by an explicit reviewed vehicle association."""
    labels, seen = [], set()

    def add(value):
        if not isinstance(value, str):
            return
        label = re.sub(r'\s+', ' ', _text(value, 300)).strip()
        if label and label.casefold() not in seen:
            seen.add(label.casefold())
            labels.append(label)

    if row.get('fitment_reviewed') is True:
        add(row.get('vehicle_label'))
    contexts = row.get('fitment_contexts', [])
    contexts = contexts if isinstance(contexts, list) else []
    source = row.get('source_context')
    for context in ([source] if isinstance(source, dict) else []) + contexts[:1000]:
        if (isinstance(context, dict) and context.get('vehicle_reviewed') is True
                and type(context.get('carId')) is int and context['carId'] > 0):
            add(context.get('vehicle_label'))
    return {'status': 'provider_vehicle' if labels else 'not_confirmed',
            'vehicle_label': labels[0] if labels else '',
            'vehicles': [{'label': label} for label in labels[:50]],
            'truncated': len(labels) > 50 or len(contexts) > 1000}


def public_product(row, *, now=None):
    """Normalize an explicitly reviewed private product record, not raw feed."""
    now = now or datetime.now(timezone.utc)
    if not isinstance(row, dict) or row.get('source') != 'ALZURA':
        raise ValueError('provider_source_required')
    category = row.get('category')
    if category not in {'body_part', 'oil', 'care', 'accessory'}:
        raise ValueError('category_invalid')
    native = _text(row.get('provider_id'))
    if not native or not _text(row.get('name')):
        raise ValueError('identity_missing')
    price = None
    # Product pricing is accepted only when explicitly identified as German EUR
    # partner net. Unknown price types, deposits and unit bases stay on request.
    if (row.get('price_review_required') is not True
            and row.get('price_type') == 'EK' and row.get('currency') == 'EUR'
            and row.get('country') == 'DE' and row.get('price_basis') == 'per_item'
            and row.get('deposit_gross_cent', 0) == 0
            and type(row.get('stock')) is int and row['stock'] >= 0):
        try:
            observed = datetime.fromisoformat(row['observed_at'].replace('Z', '+00:00'))
            if observed.utcoffset() is None or observed > now:
                raise ValueError('quote_time_invalid')
            price = {'unit_gross_cent': gross_from_net(row['unit_net_eur']),
                     'observed_at': observed.isoformat(),
                     'expires_at': (observed + timedelta(minutes=15)).isoformat(),
                     'stock': row['stock']}
            if category in {'oil','care'} and row.get('package_volume_reviewed') is True:
                volume = Decimal(str(row['package_volume_litres']))
                if volume.is_finite() and Decimal('0.001') <= volume <= 10000:
                    price.update(base_price_gross_cent=int((Decimal(price['unit_gross_cent'])/volume)
                        .quantize(Decimal('1'),rounding=ROUND_HALF_UP)),base_price_unit='L')
        except (KeyError, ValueError, TypeError, OverflowError, InvalidOperation):
            pass
    photo = row.get('photo', '')
    if not isinstance(photo, str) or not re.fullmatch(r'bilder/[a-f0-9]{64}\.(?:jpg|webp|png)', photo):
        photo = ''
    paint = row.get('paint_code') if (row.get('paint_mapping_reviewed') is True
            and row.get('new_part') is True and category == 'body_part') else None
    if paint not in {key for keys in PAINT_KEYS.values() for key in keys}:
        paint = None
    # Department follows the typed provider area, never a loose name match.
    # Keep the established category and selection ID stable for saved carts.
    department = ('parts' if row.get('source_request', {}).get('area') == 'wearparts'
                  or category == 'body_part' else 'accessories')
    result = {'id': 'parts:' + sha256((category + ':' + native).encode()).hexdigest()[:24],
              'department': department,
              'category': category, 'name': _text(row['name']),
              'brand': _text(row.get('brand')), 'article_number': _text(row.get('article_number')),
              'ean': _text(row.get('ean'), 20), 'description': _text(row.get('description'), 3000),
              'photo': photo, 'unit_label': _text(row.get('unit_label') or 'Stück', 60),
              'price': price, 'paint_code': paint, 'new_part': row.get('new_part') is True,
              'fitment': _public_fitment(row),
              'delivery': {'status': 'unknown', 'label': 'Lieferzeit wird bestätigt'}}
    if not result['photo']:
        from .media_urls import safe_image_url
        if safe_image_url(row.get('image_url')):
            result['photo'] = '/api/parts-image?article=' + result['id']
    if row.get('accessory_group') in {'oil','care','valves','adhesive','clip','tools','workshop'}:
        result['accessory_group'] = row['accessory_group']
    if row.get('price_review_required') is True:
        result['price_status'] = 'review_pending'
    return result


def load_catalog(path=PRIVATE/'products.json'):
    products = []
    if Path(path).is_file():
        value = json.loads(Path(path).read_text(encoding='utf-8'))
        if value.get('mode') != 'reviewed_alzura_parts_local':
            raise ValueError('private_parts_catalog_not_reviewed')
        for row in value.get('products', []):
            products.append(public_product(row))
    return {'products': products, 'painting_tariffs': painting_tariffs(),
            'source_status': 'Echte ALZURA-Artikel · Preise inkl. 19 % MwSt. · Versand und Einbau separat.' if products else 'Die Artikeldaten werden für die lokale Vorschau vorbereitet.'}


if __name__ == '__main__':
    from shop_parts_preview import build_parts_pages
    build_parts_pages(WORK/'alzura-vorschau-private', load_catalog())
    from shop_cart_preview import build_cart_page
    from shop_alzura_preview import load_live_snapshot
    build_cart_page(WORK/'alzura-vorschau-private', load_catalog(),
                   load_live_snapshot(WORK/'alzura-live-private'/'catalog-live.json'))
    print(json.dumps({'public_products': len(load_catalog()['products']),
                      'painting_tariffs': len(painting_tariffs()), 'markup_percent': 15,
                      'published': False, 'orders_enabled': False}))
