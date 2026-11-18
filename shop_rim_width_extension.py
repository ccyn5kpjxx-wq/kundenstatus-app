"""Two original-source 18-inch dimensional proposals, never vehicle approval.

Only an explicit hash-pinned local manifest may add the two reviewed table
rows and their already bound tyre texts. Prices, procurement, media, documents
and all vehicle rules remain the exact original objects.
"""
from collections.abc import Mapping
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat

from shop_catalog_public import variant_fingerprint
from shop_documents import document_asset, document_eprel_id
from shop_rim_width_candidates import released_width_pairs, tyre_nominal_facts
from shop_rsu_rim_evidence import WIDTH_TABLE_SHA

MAX_BYTES = 16 * 1024
HASH = re.compile(r'[a-f0-9]{64}')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}')
ALZ_SHA = '710376a514ebd82e562488d10e2feb0c1e95849ec1c9681fe11fa0e1e94965f7'
RSU_SHA = '5fdd13c3906d20ad76969fc6226a2190c91a32bcf8173a7d06823ebe3c04e9b3'
# Visually reviewed original Continental 2025 table: PDF pages30 and27.
REVIEWED_ROWS = (
    {'diameter_in': 18, 'load_variants': [{'load_index': 86, 'xl': True}],
     'pdf_page': 30, 'permitted_rim_widths_in': [7, 7.5, 8],
     'printed_page': 58, 'ratio_percent': 40, 'size': '205/40 R18',
     'visually_reviewed': True, 'width_mm': 205},
    {'diameter_in': 18, 'load_variants': [{'load_index': 90, 'xl': True}],
     'pdf_page': 27, 'permitted_rim_widths_in': [6.5, 7, 7.5],
     'printed_page': 52, 'ratio_percent': 45, 'size': '205/45 R18',
     'visually_reviewed': True, 'width_mm': 205},
)
# Existing released, RSU-bound source rows; no new supplier identity is created.
EXPECTED = {
    'tyre:5018e4c6ecf720c036c16bf5': (205, 40, 86, 107230, 33124),
    'tyre:089defb6b14c5b86d36c99dc': (205, 45, 90, 119014, 45258),
}


class RimWidthExtensionError(ValueError):
    """Fixed codes only, without original private data."""


def _need(value, code):
    if not value:
        raise RimWidthExtensionError(code)


def _read(root, name):
    try:
        root = Path(root)
        _need(root.is_absolute() and root.is_dir() and root.resolve() == root
              and not any(p.is_symlink() for p in (root, *root.parents)), 'width_extension_root_invalid')
        path = root / name
        _need(not path.is_symlink() and path.parent == root, 'width_extension_path_invalid')
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            meta = os.fstat(fd)
            _need(stat.S_ISREG(meta.st_mode) and 0 < meta.st_size <= MAX_BYTES, 'width_extension_size_invalid')
            with os.fdopen(fd, 'rb') as stream:
                fd = None
                raw = stream.read(MAX_BYTES + 1)
        finally:
            if fd is not None:
                os.close(fd)
        _need(0 < len(raw) <= MAX_BYTES, 'width_extension_size_invalid')
        return raw
    except RimWidthExtensionError:
        raise
    except (OSError, TypeError, ValueError):
        raise RimWidthExtensionError('width_extension_file_unavailable') from None


def _decode(raw):
    def unique(items):
        value = {}
        for key, item in items:
            _need(key not in value, 'width_extension_duplicate_key')
            value[key] = item
        return value
    try:
        return json.loads(raw, object_pairs_hook=unique,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except RimWidthExtensionError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise RimWidthExtensionError('width_extension_json_invalid') from None


def _documents(bundle, identifier, identity):
    pair = bundle.get('product_documents', {}).get(identifier)
    documents = bundle.get('documents', {})
    resources, approved = documents.get('resources'), documents.get('approved_assets')
    _need(isinstance(pair, Mapping) and pair.get('identity_hash') == identity
          and pair.get('status') == 'source_bound_document_pair'
          and isinstance(pair.get('eprel_id'), str) and re.fullmatch(r'[1-9][0-9]{0,11}', pair['eprel_id'])
          and isinstance(resources, Mapping) and isinstance(approved, list), 'width_extension_document_proof_missing')
    for key, suffix in (('tyre_label_url', ('.jpg', '.png')), ('tyre_sheet_url', ('.pdf',))):
        url = pair.get(key)
        _need(isinstance(url, str) and url.startswith('/shop/documents/'), 'width_extension_document_proof_missing')
        asset = url[len('/shop/documents/'):]
        entry = resources.get(asset)
        _need(asset.endswith(suffix) and asset in approved and isinstance(entry, Mapping)
              and isinstance(entry.get('url'), str) and document_asset(entry['url']) == asset
              and document_eprel_id(entry['url']) == pair['eprel_id'], 'width_extension_document_proof_mismatch')


def load_rim_width_extension(root, record, bundle, *, base_bundle_sha256):
    """RAM-only append of two reviewed rows and exact existing tyre proofs.

    This does not enable a vehicle fitment or skip existing assembly checks.
    Current EU label/sheet source bindings must already exist in the base.
    """
    if record is None:
        return bundle
    try:
        _need(isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256)
              and isinstance(record, Mapping) and set(record) == {'path', 'sha256', 'base_bundle_sha256'}
              and isinstance(record['path'], str) and NAME.fullmatch(record['path'])
              and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256'])
              and record['base_bundle_sha256'] == base_bundle_sha256, 'width_extension_descriptor_invalid')
        raw = _read(root, record['path'])
        _need(sha256(raw).hexdigest() == record['sha256'], 'width_extension_hash_mismatch')
        value = _decode(raw)
        _need(isinstance(value, Mapping) and set(value) == {
            'schema_version', 'base_bundle_sha256', 'registry_rows', 'nominal_tyre_sources', 'sources'}
            and type(value['schema_version']) is int and value['schema_version'] == 1
            and value['base_bundle_sha256'] == base_bundle_sha256, 'width_extension_schema_invalid')
        _need(value['sources'] == {'alzura_archive_sha256': ALZ_SHA, 'rsu_csv_sha256': RSU_SHA,
                                   'width_table_sha256': WIDTH_TABLE_SHA}, 'width_extension_original_sources_invalid')
        _need(isinstance(value['registry_rows'], list) and value['registry_rows'] == list(REVIEWED_ROWS)
              and all(row.get('visually_reviewed') is True for row in value['registry_rows']),
              'width_extension_reviewed_rows_mismatch')
        _need(isinstance(bundle, Mapping), 'width_extension_bundle_invalid')
        registry = bundle.get('reviewed_rim_width_registry')
        old, extra = bundle.get('nominal_tyre_sources'), value['nominal_tyre_sources']
        seeds, bindings = bundle.get('rsu_price_seeds', {}).get('records'), bundle.get('rsu_bindings')
        released = bundle.get('publication', {}).get('released_ids')
        _need(isinstance(registry, Mapping) and isinstance(registry.get('rows'), list)
              and registry.get('source', {}).get('sha256') == WIDTH_TABLE_SHA
              and isinstance(old, Mapping) and isinstance(extra, Mapping) and set(extra) == set(EXPECTED)
              and not set(extra) & set(old) and isinstance(seeds, Mapping) and isinstance(bindings, Mapping)
              and isinstance(released, list) and set(extra) <= set(seeds) & set(bindings) & set(released),
              'width_extension_existing_membership_invalid')
        new_sizes = {row['size'] for row in REVIEWED_ROWS}
        _need(not any(isinstance(row, Mapping) and row.get('size') in new_sizes for row in registry['rows']),
              'width_extension_registry_override')
        products = bundle.get('products')
        _need(isinstance(products, list) and 1 <= len(products) <= 100000, 'width_extension_products_invalid')
        by = {p.get('selection_id', p.get('id')): p for p in products if isinstance(p, Mapping)}
        _need(all(sum(isinstance(p, Mapping) and p.get('selection_id', p.get('id')) == identifier
                      for p in products) == 1 for identifier in EXPECTED), 'width_extension_duplicate_product')
        for identifier, (width, ratio, load, alz_row, rsu_row) in EXPECTED.items():
            product = by.get(identifier)
            _need(isinstance(product, Mapping) and product.get('category') == 'tyre', 'width_extension_product_missing')
            identity = variant_fingerprint(product)
            _documents(bundle, identifier, identity)
            item = extra[identifier]
            proof = item.get('proof') if isinstance(item, Mapping) else None
            _need(isinstance(proof, Mapping) and proof.get('identity_hash') == identity
                  and proof.get('product_id') == identifier and proof.get('alzura_source_sha256') == ALZ_SHA
                  and proof.get('rsu_source_sha256') == RSU_SHA and proof.get('alzura_row_number') == alz_row
                  and proof.get('rsu_row_number') == rsu_row, 'width_extension_original_row_mismatch')
            shadow = deepcopy(product)
            shadow['details'] = {**shadow.get('details', {}), 'Artikelbezeichnung': proof.get('articlename'),
                'Zusatzmerkmale': ' '.join(proof.get(k, '') for k in
                    ('description', 'info', 'manufacturer_description', 'rsu_profile'))}
            facts = tyre_nominal_facts(shadow)
            _need(facts is not None and (facts['width_mm'], facts['ratio_percent'], facts['diameter_in'],
                  facts['load_index'], facts['xl']) == (width, ratio, 18, load, True), 'width_extension_nominal_mismatch')
        merged = {**bundle, 'nominal_tyre_sources': {**old, **extra},
            'reviewed_rim_width_registry': {**registry, 'rows': [*registry['rows'], *deepcopy(value['registry_rows'])]}}
        independent = {**merged, 'compatible_pairs': {}, 'nominal_tyre_sources': extra}
        pairs = released_width_pairs(independent, approved_ids=set(seeds))
        proposed = {pair['tyre_id'] for rows in pairs.values() for pair in rows}
        _need(set(extra) <= proposed, 'width_extension_no_documented_proposals')
        return merged
    except RimWidthExtensionError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError):
        raise RimWidthExtensionError('width_extension_invalid') from None
