"""Original ALZURA partner prices for inquiry display, never procurement.

Only an explicitly pinned private snapshot plus its original articles response
and response metadata may supply a price. Existing normalization is replayed
locally; neither provider clients, mutable price books nor order APIs are used.
"""
from collections.abc import Mapping
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from shop_catalog_public import _quote, _text, variant_fingerprint
from shop_cart_quotes import retail_unit_gross_cent
from shop_parts_inquiry import HASH, NAME, ID, PartsInquiryPrices
from shop_parts_release import read, decode, _stamp
from shop_partner_native.shop_parts_provider import normalize_wearparts

MAX_SOURCE = 8 * 1024 * 1024
MAX_SOURCES_TOTAL = 32 * 1024 * 1024
NORMALIZED_FIELDS = ('source', 'provider_id', 'category', 'name', 'brand', 'article_number',
    'ean', 'unit_label', 'stock', 'observed_at', 'unit_net_eur', 'price_basis', 'price_type', 'currency', 'country')
PRICE_KEYS = {'kind', 'source', 'currency', 'vat_percent', 'observed_at', 'review_at', 'expires_at',
    'stock_reported', 'stock_current', 'stock_observed_at', 'stock_expires_at', 'prices_by_quantity'}


class PartsPartnerInquiryError(ValueError):
    pass


def _need(value, code='invalid_parts_partner_inquiry'):
    if not value:
        raise PartsPartnerInquiryError(code)


def normalize_partner_inquiry_price(price):
    """Validate only this ALZURA display type; do not broaden core pricing.

    The detached temporary envelope reuses the EUR/19% amount/date validator.
    Its RSU tags are never retained or returned, and prove no supplier rights.
    The canonical result remains stable when bootstrap and catalogue both
    validate it, with historical stock and complete typed quantity rows.
    """
    try:
        _need(isinstance(price, Mapping) and set(price) == PRICE_KEYS
            and price['kind'] == 'partner_inquiry_pricebook' and price['source'] == 'ALZURA'
            and price['currency'] == 'EUR' and price['vat_percent'] == '19.00'
            and price['stock_current'] is False)
        supplied = price['prices_by_quantity']
        _need(isinstance(supplied, Mapping) and set(supplied) == {str(q) for q in range(1, 21)}
            and all(isinstance(line, Mapping) and set(line) == {
                'quantity', 'unit_gross_cent', 'items_gross_cent', 'currency', 'vat_percent'}
                for line in supplied.values()))
        quote = _quote({'category': 'accessory', 'quote_public': {
            **price, 'kind': 'merchant_pricebook', 'source': 'RSU'}}, allow_published_prices=True)
        _need(quote is not None and len(quote['prices_by_quantity']) == 20
            and len({v['unit_gross_cent'] for v in quote['prices_by_quantity'].values()}) == 1
            and _stamp(quote['expires_at']) == _stamp(quote['observed_at']) + timedelta(days=7))
        for line in quote['prices_by_quantity'].values():
            line.update(currency='EUR', vat_percent='19.00')
        quote.update(kind='partner_inquiry_pricebook', source='ALZURA', stock_current=False)
        return quote
    except PartsPartnerInquiryError:
        raise
    except Exception:
        raise PartsPartnerInquiryError('invalid_parts_partner_inquiry_price') from None


def _projection(identity, unit, observed, stock):
    return {'identity_hash': identity, 'price': normalize_partner_inquiry_price({
        'kind': 'partner_inquiry_pricebook', 'source': 'ALZURA', 'currency': 'EUR', 'vat_percent': '19.00',
        'observed_at': observed.isoformat(), 'review_at': (observed + timedelta(days=5)).isoformat(),
        'expires_at': (observed + timedelta(days=7)).isoformat(), 'stock_reported': stock,
        'stock_current': False, 'stock_observed_at': observed.isoformat(),
        'stock_expires_at': (observed + timedelta(minutes=15)).isoformat(),
        'prices_by_quantity': {str(q): {'quantity': q, 'unit_gross_cent': unit, 'items_gross_cent': unit * q,
            'currency': 'EUR', 'vat_percent': '19.00'} for q in range(1, 21)}})}


def _ref(ref, *, row=False):
    _need(isinstance(ref, Mapping) and set(ref) == ({'file', 'sha256', 'row_number'} if row else {'file', 'sha256'})
        and isinstance(ref['file'], str) and NAME.fullmatch(ref['file'])
        and isinstance(ref['sha256'], str) and HASH.fullmatch(ref['sha256']))
    if row:
        _need(type(ref['row_number']) is int and 1 <= ref['row_number'] <= 1000)
    return ref


def load_parts_partner_prices(root, record, bundle, *, base_bundle_sha256, allowed_ids):
    """Read exact licensed originals once and return immutable display data.

    No ordering, mapping, supplier selection, freight, vehicle approval or
    mutable stock/price-book state is inferred from this display channel.
    """
    if record is None:
        return PartsInquiryPrices({})
    try:
        _need(isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256)
            and isinstance(record, Mapping) and set(record) == {'path', 'sha256', 'base_bundle_sha256'}
            and isinstance(record['path'], str) and NAME.fullmatch(record['path'])
            and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256'])
            and record['base_bundle_sha256'] == base_bundle_sha256)
        _need(isinstance(allowed_ids, (set, frozenset, list, tuple)) and len(allowed_ids) <= 100000
            and all(isinstance(v, str) and ID.fullmatch(v) for v in allowed_ids)
            and len(set(allowed_ids)) == len(allowed_ids))
        root = Path(root)
        _need(root.is_absolute() and root.is_dir() and root.resolve() == root)
        raw = read(root / record['path'], 2 * 1024 * 1024)
        _need(sha256(raw).hexdigest() == record['sha256'], 'parts_partner_manifest_hash_mismatch')
        manifest = decode(raw)
        _need(isinstance(manifest, Mapping) and set(manifest) == {
            'schema_version', 'base_bundle_sha256', 'snapshot', 'source_refs', 'display_prices'}
            and type(manifest['schema_version']) is int and manifest['schema_version'] == 1
            and manifest['base_bundle_sha256'] == base_bundle_sha256)
        prices, refs = manifest['display_prices'], manifest['source_refs']
        _need(isinstance(prices, Mapping) and 1 <= len(prices) <= 5000 and set(prices) <= set(allowed_ids)
            and isinstance(refs, list) and 1 <= len(refs) <= 128)
        _need(isinstance(bundle.get('rsu_bindings', {}), Mapping)
            and not set(prices) & set(bundle.get('rsu_bindings', {})), 'parts_partner_procurement_scope_conflict')
        sources, total = {}, 0
        for reference in refs:
            ref = _ref(reference)
            _need(ref['file'] not in sources)
            data = read(root / 'partner-price-sources' / ref['file'], MAX_SOURCE)
            _need(sha256(data).hexdigest() == ref['sha256'], 'parts_partner_source_hash_mismatch')
            total += len(data); _need(total <= MAX_SOURCES_TOTAL)
            sources[ref['file']] = (ref['sha256'], decode(data))
        def original(ref, *, row=False):
            ref = _ref(ref, row=row)
            _need(ref['file'] in sources and sources[ref['file']][0] == ref['sha256'])
            return sources[ref['file']][1]
        snapshot = original(manifest['snapshot'])
        _need(isinstance(snapshot, Mapping) and snapshot.get('mode') == 'reviewed_alzura_parts_local'
            and isinstance(snapshot.get('products'), list) and 1 <= len(snapshot['products']) <= 100000)
        products = bundle['products']
        _need(isinstance(products, list) and 1 <= len(products) <= 100000)
        index = {p.get('selection_id', p.get('id')): p for p in products if isinstance(p, Mapping)}
        _need(set(prices) <= set(bundle['publication']['released_ids']))
        result, seen_native = {}, set()
        for identifier, price in prices.items():
            _need(isinstance(price, Mapping) and set(price) == {'identity_hash', 'unit_gross_cent', 'observed_at',
                'source', 'snapshot_row_number', 'source_body', 'source_meta'} and identifier in index
                and price['source'] == 'ALZURA' and type(price['snapshot_row_number']) is int
                and 1 <= price['snapshot_row_number'] <= len(snapshot['products']))
            product = index[identifier]; identity = variant_fingerprint(product)
            _need(product.get('department') == 'parts' and product.get('category') in {'body_part', 'accessory'}
                and price['identity_hash'] == identity and type(price['unit_gross_cent']) is int)
            saved = snapshot['products'][price['snapshot_row_number'] - 1]
            _need(isinstance(saved, dict) and saved.get('source') == 'ALZURA'
                and isinstance(saved.get('provider_id'), str) and saved['provider_id'].startswith('wearparts:')
                and saved['provider_id'] not in seen_native)
            seen_native.add(saved['provider_id'])
            _need(identifier == 'parts:' + sha256((saved.get('category', '') + ':' + saved['provider_id']).encode()).hexdigest()[:24]
                and product.get('category') == saved.get('category')
                and _text(product.get('name')) == _text(saved.get('name'))
                and _text(product.get('manufacturer_name') or product.get('brand'), 90) == _text(saved.get('brand'), 90)
                and _text(product.get('article_number'), 100) == _text(saved.get('article_number'), 100)
                and product.get('details', {}).get('EAN', '') == saved.get('ean', '')
                and _text(product.get('unit_label'), 60) == _text(saved.get('unit_label'), 60), 'parts_partner_catalogue_mismatch')
            body, meta = original(price['source_body'], row=True), original(price['source_meta'])
            _need(isinstance(body, Mapping) and isinstance(body.get('result'), list)
                and 1 <= price['source_body']['row_number'] <= len(body['result'])
                and isinstance(meta, Mapping) and meta.get('ok') is True and type(meta.get('http_status')) is int
                and meta['http_status'] == 200 and meta.get('area') == 'wearparts'
                and meta.get('operation') == 'articles' and meta.get('body_sha256') == price['source_body']['sha256'])
            observed = _stamp(price['observed_at'])
            requested, received = _stamp(meta.get('requested_at')), _stamp(meta.get('received_at'))
            # Older reviewed normalizations used the request timestamp; newer
            # ones used response receipt. Both are original pinned metadata,
            # and neither may be replaced with a later publication timestamp.
            _need(observed == _stamp(saved.get('observed_at')) and requested <= received
                and observed in (requested, received), 'parts_partner_original_timestamp_mismatch')
            row = body['result'][price['source_body']['row_number'] - 1]
            _need(isinstance(row, Mapping) and isinstance(row.get('assignedTyre24Data'), Mapping))
            market = row['assignedTyre24Data']
            _need(market.get('vat') is not None and Decimal(str(market['vat'])) == Decimal('0.19')
                and market.get('depositPrice') in (None, '', '0', '0.00', 0)
                and isinstance(saved.get('source_context'), dict)
                and saved['source_context'].get('price_semantics_reviewed') is True
                and all(saved['source_context'].get(k) == v for k, v in {
                    'price_type': 'EK', 'currency': 'EUR',
                    'country': 'DE', 'price_basis': 'per_item'}.items()))
            replayed = normalize_wearparts({'result': [row]}, observed, saved.get('source_request'), saved['source_context'])
            _need(len(replayed) == 1 and all(saved.get(k) == replayed[0].get(k) for k in NORMALIZED_FIELDS),
                'parts_partner_original_row_mismatch')
            _need(saved.get('price_type') == 'EK' and saved.get('currency') == 'EUR'
                and saved.get('country') == 'DE' and saved.get('price_basis') == 'per_item'
                and Decimal(str(market.get('wholesaler_price'))) == Decimal(str(saved.get('unit_net_eur'))))
            _need(retail_unit_gross_cent(saved['unit_net_eur'], product['category']) == price['unit_gross_cent'],
                'parts_partner_material_markup_mismatch')
            _need(type(saved.get('stock')) is int and 0 <= saved['stock'] <= 1000000)
            result[identifier] = _projection(identity, price['unit_gross_cent'], observed, saved['stock'])
        return PartsInquiryPrices(result)
    except PartsPartnerInquiryError:
        raise
    except Exception:
        raise PartsPartnerInquiryError('invalid_parts_partner_inquiry') from None
