"""Original-source-verified RSU display prices for inquiry-only parts.

This channel does not merge supplier bindings, seed a database, construct a
provider, prove availability or enable an order. The explicit loader reads
only pinned private originals. The resulting object is pure, immutable price
data; retrieving it never refreshes source timestamps or performs IO.
"""
from collections.abc import Mapping
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import re

from shop_catalog_public import variant_fingerprint
from shop_cart_quotes import retail_unit_gross_cent
from shop_parts_release import read, decode, _stamp, _originals, _observation, _genuine_observation
from shop_rsu_parts_evidence import CATEGORIES, parts_source_record, validate_parts_mapping, _ean

HASH = re.compile(r'[a-f0-9]{64}')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}')
ID = re.compile(r'parts:[a-f0-9]{24}')
MAX_SOURCE = 4 * 1024 * 1024
MAX_SOURCES_TOTAL = 32 * 1024 * 1024


class PartsInquiryError(ValueError):
    pass


def _need(value, code='invalid_parts_inquiry'):
    if not value:
        raise PartsInquiryError(code)


class PartsInquiryPrices:
    """No callbacks, supplier clients, filesystem handles or mutable projections."""
    def __init__(self, records):
        self._records = deepcopy(dict(records))
        self._ids = frozenset(records)

    @property
    def ids(self):
        return self._ids

    def get(self, product_id):
        # Return the original projection even after its expiry. The normal
        # public-catalogue price validator decides visibility with its clock.
        if not isinstance(product_id, str):
            return None
        return deepcopy(self._records.get(product_id))


def _projection(identity_hash, unit, observed, stock):
    return {'identity_hash': identity_hash, 'price': {
        'kind': 'merchant_pricebook', 'source': 'RSU', 'currency': 'EUR', 'vat_percent': '19.00',
        'observed_at': observed.isoformat(), 'review_at': (observed + timedelta(days=5)).isoformat(),
        'expires_at': (observed + timedelta(days=7)).isoformat(), 'stock_reported': stock,
        'stock_current': False, 'stock_observed_at': observed.isoformat(),
        # Preserve the actual original observation window. stock_current=False
        # remains an explicit display-only fact even while that window is open.
        'stock_expires_at': (observed + timedelta(minutes=15)).isoformat(),
        'prices_by_quantity': {str(q): {'quantity': q, 'unit_gross_cent': unit,
            'items_gross_cent': unit * q, 'currency': 'EUR', 'vat_percent': '19.00'} for q in range(1, 21)}}}


def load_parts_inquiry(root, record, bundle, *, base_bundle_sha256, allowed_ids):
    """Return display-only projections after independent original-byte checks.

    The private manifest pin defines the selected SKU whitelist; allowed_ids
    additionally constrains it to the server's released, non-procurement parts
    scope. No ordering permission is inferred from an identity witness.
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
        allowed_ids = frozenset(allowed_ids)
        root = Path(root)
        _need(root.is_absolute() and root.is_dir() and root.resolve() == root)
        raw = read(root / record['path'], 2 * 1024 * 1024)
        _need(sha256(raw).hexdigest() == record['sha256'], 'parts_inquiry_hash_mismatch')
        manifest = decode(raw)
        _need(isinstance(manifest, Mapping) and set(manifest) == {
            'schema_version', 'base_bundle_sha256', 'identity_proofs', 'display_prices', 'source_refs'}
            and type(manifest['schema_version']) is int and manifest['schema_version'] == 1
            and manifest['base_bundle_sha256'] == base_bundle_sha256)
        proofs, prices = manifest['identity_proofs'], manifest['display_prices']
        _need(isinstance(proofs, Mapping) and isinstance(prices, Mapping) and 1 <= len(prices) <= 5000
              and set(proofs) == set(prices) and set(prices) <= allowed_ids)
        existing_bindings = bundle.get('rsu_bindings', {})
        _need(isinstance(existing_bindings, Mapping) and not set(prices) & set(existing_bindings),
              'parts_inquiry_procurement_scope_conflict')
        refs = manifest['source_refs']
        _need(isinstance(refs, list) and 1 <= len(refs) <= 30)
        sources, names, total = {}, set(), 0
        for ref in refs:
            _need(isinstance(ref, Mapping) and set(ref) == {'file', 'sha256'}
                  and isinstance(ref['file'], str) and NAME.fullmatch(ref['file']) and ref['file'] not in names
                  and isinstance(ref['sha256'], str) and HASH.fullmatch(ref['sha256']))
            data = read(root / 'parts-sources' / ref['file'], MAX_SOURCE)
            _need(sha256(data).hexdigest() == ref['sha256'], 'parts_inquiry_source_hash_mismatch')
            names.add(ref['file']); total += len(data)
            _need(total <= MAX_SOURCES_TOTAL)
            sources[ref['sha256']] = data
        views, inquiries, _ = _originals(sources)
        products = bundle['products']
        _need(isinstance(products, list) and 1 <= len(products) <= 100000)
        index = {p.get('selection_id', p.get('id')): (n, p) for n, p in enumerate(products, 1) if isinstance(p, Mapping)}
        _need(set(prices) <= set(bundle['publication']['released_ids']))
        result = {}
        seen_suppliers = set()
        for identifier, price in prices.items():
            _need(isinstance(identifier, str) and ID.fullmatch(identifier) and identifier in index)
            number, product = index[identifier]
            identity = variant_fingerprint(product)
            _need(product.get('category') in CATEGORIES and product.get('department') == 'parts'
                  and isinstance(price, Mapping) and set(price) == {'identity_hash', 'unit_gross_cent', 'observed_at', 'source'}
                  and price['identity_hash'] == identity and price['source'] == 'RSU'
                  and type(price['unit_gross_cent']) is int and 1 <= price['unit_gross_cent'] <= 10_000_000)
            witness = proofs[identifier]
            _need(isinstance(witness, Mapping) and set(witness) == {'identity_hash', 'category', 'mapping', 'expected_ean'}
                  and witness['identity_hash'] == identity and witness['category'] == product['category'])
            resolved = validate_parts_mapping(witness['mapping'], product_id=identifier, category=product['category'],
                identity_hash=identity, expected_ean=witness['expected_ean'])
            evidence = witness['mapping']['evidence']
            details = product.get('details', {})
            # This narrow initial display release uses exact EAN-backed rows.
            # MPN-only/packing assumptions are not introduced as a fallback.
            _need(isinstance(details, Mapping) and _ean(details.get('EAN')), 'parts_inquiry_exact_ean_required')
            catalogue = parts_source_record('catalogue', source_sha256=base_bundle_sha256, row_number=number,
                article_id=identifier, manufacturer=product.get('manufacturer_name'),
                mpn=details.get('Herstellerartikelnummer'), ean=details['EAN'])
            _need(evidence['catalogue'] == catalogue, 'parts_inquiry_catalogue_mismatch')
            supplier = evidence['rsu']
            _need(supplier['source_sha256'] in sources and resolved['supplier_article_id'] not in seen_suppliers)
            seen_suppliers.add(resolved['supplier_article_id'])
            original_normalized = sources[supplier['source_sha256']]
            _need(original_normalized.lstrip().startswith(b'{'))
            observation = _observation(original_normalized, supplier['row_number'])
            actual = parts_source_record('rsu', source_sha256=supplier['source_sha256'], row_number=supplier['row_number'],
                article_id=observation.get('article_id'), manufacturer=observation.get('manufacturer'),
                mpn=observation.get('mpn'), ean=observation.get('ean'))
            _need(supplier == actual, 'parts_inquiry_supplier_row_mismatch')
            _genuine_observation(observation, supplier, views, inquiries)
            observed = _stamp(price['observed_at'])
            matches = [item for digest, records in inquiries for item in records.values()
                if digest != supplier['source_sha256'] and item['supplier_article_id'] == resolved['supplier_article_id']
                and item['ean'] == witness['expected_ean'] and _stamp(item['retrieved_at']) == observed]
            _need(matches and len({(Decimal(item['unit_net']), item['stock']) for item in matches}) == 1,
                  'parts_inquiry_original_price_missing_or_ambiguous')
            original = matches[0]
            _need(type(original['stock']) is int and 0 <= original['stock'] <= 1_000_000
                  and retail_unit_gross_cent(original['unit_net'], product['category']) == price['unit_gross_cent'],
                  'parts_inquiry_material_markup_mismatch')
            result[identifier] = _projection(identity, price['unit_gross_cent'], observed, original['stock'])
        return PartsInquiryPrices(result)
    except PartsInquiryError:
        raise
    except Exception:
        raise PartsInquiryError('invalid_parts_inquiry') from None
