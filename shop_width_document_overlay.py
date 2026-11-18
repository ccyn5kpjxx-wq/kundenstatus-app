"""Four original EU documents for the two reviewed dimensional proposals.

The legacy local document manifest and every source URL stay intact. Optional
read-only bindings add exact local bytes; no network, pricing or vehicle rule.
"""
from collections.abc import Mapping
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

from shop_catalog_public import variant_fingerprint
from shop_document_overlay import _read, _decode, _need, HASH, NAME, MAX_MANIFEST
from shop_documents import ReleasedProductDocuments, document_asset, document_eprel_id
from shop_rim_width_extension import EXPECTED


def local_rim_width_document_resources(root, record, bundle, resources, *,
                                       base_bundle_sha256, extension_sha256):
    if record is None:
        return resources
    _need(isinstance(record, Mapping) and set(record) == {'path', 'sha256', 'base_bundle_sha256'}
        and isinstance(record['path'], str) and NAME.fullmatch(record['path'])
        and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256'])
        and isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256)
        and record['base_bundle_sha256'] == base_bundle_sha256
        and isinstance(extension_sha256, str) and HASH.fullmatch(extension_sha256),
        'width_documents_descriptor_invalid')
    root = Path(root)
    _need(root.is_absolute() and root.is_dir() and root.resolve() == root, 'width_documents_root_invalid')
    raw = _read(root / record['path'], MAX_MANIFEST)
    _need(sha256(raw).hexdigest() == record['sha256'], 'width_documents_manifest_hash_mismatch')
    manifest = _decode(raw)
    _need(isinstance(manifest, Mapping) and set(manifest) == {
        'base_bundle_sha256', 'width_extension_sha256', 'records'}
        and manifest['base_bundle_sha256'] == base_bundle_sha256
        and manifest['width_extension_sha256'] == extension_sha256
        and isinstance(manifest['records'], Mapping) and len(manifest['records']) == 4,
        'width_documents_manifest_invalid')
    _need(isinstance(bundle, Mapping) and isinstance(resources, Mapping)
        and isinstance(bundle.get('products'), list), 'width_documents_bundle_invalid')
    products = {p.get('selection_id', p.get('id')): p for p in bundle['products'] if isinstance(p, Mapping)}
    documents = bundle.get('product_documents', {})
    approved = bundle.get('documents', {}).get('approved_assets', [])
    expected = {}
    for identifier in EXPECTED:
        product, pair = products.get(identifier), documents.get(identifier)
        _need(isinstance(product, Mapping) and product.get('category') == 'tyre'
            and isinstance(pair, Mapping) and pair.get('status') == 'source_bound_document_pair'
            and pair.get('identity_hash') == variant_fingerprint(product), 'width_documents_identity_mismatch')
        for kind in ('tyre_label_url', 'tyre_sheet_url'):
            path = pair.get(kind)
            _need(isinstance(path, str) and path.startswith('/shop/documents/'), 'width_documents_source_invalid')
            asset = path[len('/shop/documents/'):]
            source = resources.get(asset)
            _need(asset in approved and isinstance(source, Mapping) and isinstance(source.get('url'), str)
                and document_asset(source['url']) == asset
                and document_eprel_id(source['url']) == pair.get('eprel_id'), 'width_documents_source_invalid')
            expected[asset] = (product, kind)
    _need(set(manifest['records']) == set(expected), 'width_documents_scope_invalid')
    result = deepcopy(resources)
    for asset, record in manifest['records'].items():
        _need(isinstance(record, Mapping) and set(record) == {'name', 'sha256', 'mime', 'size'}
            and record['name'] == asset and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256'])
            and type(record['size']) is int and 0 < record['size'] <= 4 * 1024 * 1024,
            'width_documents_record_invalid')
        source = resources[asset]
        _need(('file' not in source or source['file'] == asset)
            and ('sha256' not in source or source['sha256'] == record['sha256']), 'width_documents_resource_conflict')
        data = _read(root / 'documents' / asset, 4 * 1024 * 1024)
        _need(len(data) == record['size'] and sha256(data).hexdigest() == record['sha256'],
            'width_documents_content_hash_mismatch')
        _, mime = ReleasedProductDocuments._validate(data, asset)
        _need(record['mime'] == mime, 'width_documents_mime_mismatch')
        product, kind = expected[asset]
        if kind == 'tyre_sheet_url':
            ReleasedProductDocuments._validate_tyre_identity(data, {
                'manufacturer': product['manufacturer_name'], 'width_mm': str(product['width']),
                'aspect_ratio_percent': str(product['ratio']), 'diameter_inch': str(product['diameter']),
                'load_index': str(product['load_index']), 'speed_index': product['speed_index']})
        result[asset] = {**source, 'file': asset, 'sha256': record['sha256']}
    return result
