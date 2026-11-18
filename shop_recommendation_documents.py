"""Read-only local originals for the approved dimensional tyre suggestions.

Only file/content-hash bindings are added to already released document URLs.
No downloads, supplier access, prices, publication or vehicle approvals occur.
"""
from collections import Counter
from collections.abc import Mapping
from hashlib import sha256
import json
from pathlib import Path
import re

from shop_catalog_public import variant_fingerprint
from shop_document_overlay import _read, _decode, _need, HASH, NAME, DocumentOverlayError
from shop_documents import (ASSET, MAX_IMAGE, MAX_PDF, DocumentError,
    ReleasedProductDocuments, document_asset, document_eprel_id, _tyre_identity)

MAX_MANIFEST = 256 * 1024
MAX_ASSETS = 512
MAX_SOURCE_IDS = 256
MAX_TOTAL = 32 * 1024 * 1024


def _identity(product):
    return _tyre_identity({'manufacturer': product.get('manufacturer_name'),
        'width_mm': product.get('width'), 'aspect_ratio_percent': product.get('ratio'),
        'diameter_inch': product.get('diameter'), 'load_index': product.get('load_index'),
        'speed_index': product.get('speed_index')})


def _normal_identity(value):
    return {key: item.casefold() for key, item in _tyre_identity(value).items()}


def _source_owners(bundle, resources):
    products, proofs = bundle.get('products'), bundle.get('nominal_tyre_sources')
    pairs, docs = bundle.get('product_documents'), bundle.get('documents')
    bindings = bundle.get('rsu_bindings')
    released = bundle.get('publication', {}).get('released_ids')
    _need(isinstance(products, list) and 1 <= len(products) <= 100000
        and isinstance(proofs, Mapping) and 1 <= len(proofs) <= MAX_SOURCE_IDS
        and isinstance(pairs, Mapping) and isinstance(docs, Mapping)
        and isinstance(docs.get('resources'), Mapping) and isinstance(docs.get('approved_assets'), list)
        and isinstance(bindings, Mapping) and isinstance(released, list)
        and set(proofs) <= set(bindings) & set(released), 'recommendation_documents_bundle_invalid')
    by = {p.get('selection_id', p.get('id')): p for p in products if isinstance(p, Mapping)}
    counts = Counter(p.get('selection_id', p.get('id')) for p in products if isinstance(p, Mapping)
        and isinstance(p.get('selection_id', p.get('id')), str))
    owners = {}
    for pid, item in proofs.items():
        product, pair = by.get(pid), pairs.get(pid)
        proof = item.get('proof') if isinstance(item, Mapping) else None
        _need(counts[pid] == 1 and isinstance(product, Mapping) and product.get('category') == 'tyre'
            and isinstance(proof, Mapping) and proof.get('product_id') == pid
            and proof.get('identity_hash') == variant_fingerprint(product)
            and isinstance(bindings[pid], Mapping) and bindings[pid].get('identity_hash') == proof['identity_hash']
            and item.get('proof_sha256') == sha256(json.dumps(proof, sort_keys=True, ensure_ascii=False,
                separators=(',', ':'), allow_nan=False).encode()).hexdigest()
            and isinstance(pair, Mapping) and pair.get('status') == 'source_bound_document_pair'
            and pair.get('identity_hash') == proof['identity_hash']
            and isinstance(pair.get('eprel_id'), str) and re.fullmatch(r'[1-9][0-9]{0,11}', pair['eprel_id']),
            'recommendation_documents_identity_mismatch')
        identity = _identity(product)
        for kind, suffix in (('tyre_label_url', ('.jpg', '.png')), ('tyre_sheet_url', ('.pdf',))):
            path = pair.get(kind)
            _need(isinstance(path, str) and path.startswith('/shop/documents/'), 'recommendation_documents_source_invalid')
            asset = path[len('/shop/documents/'):]
            source, original = resources.get(asset), docs['resources'].get(asset)
            _need(ASSET.fullmatch(asset) and asset.endswith(suffix) and asset in docs['approved_assets']
                and isinstance(source, Mapping) and isinstance(original, Mapping)
                and {k: v for k, v in source.items() if k not in {'file', 'sha256'}} ==
                    {k: v for k, v in original.items() if k not in {'file', 'sha256'}}
                and isinstance(source.get('url'), str) and document_asset(source['url']) == asset,
                'recommendation_documents_source_invalid')
            eprel = document_eprel_id(source['url'])
            # Manufacturer PIS URLs carry an article number, not an EPREL ID.
            # Their existing reviewed identity chain and actual PDF are both required.
            _need(eprel == pair['eprel_id'] or kind == 'tyre_sheet_url' and eprel is None
                and _normal_identity(source.get('tyre_identity')) == _normal_identity(identity),
                'recommendation_documents_eprel_mismatch')
            owners.setdefault(asset, []).append((kind, identity))
    return owners


def local_recommendation_document_resources(root, record, bundle, resources, *,
                                             base_bundle_sha256, source_extension_sha256):
    """Bind a successful subset of existing approved source-proof document pairs.

    An absent descriptor returns the same object without file IO. Configured
    records are verified completely before a new resource mapping is returned.
    Existing local bindings cannot be replaced with a different file or hash.
    """
    if record is None:
        return resources
    try:
        _need(isinstance(record, Mapping) and set(record) == {
            'path', 'sha256', 'base_bundle_sha256', 'source_extension_sha256'}
            and isinstance(record['path'], str) and NAME.fullmatch(record['path'])
            and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256'])
            and isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256)
            and isinstance(source_extension_sha256, str) and HASH.fullmatch(source_extension_sha256)
            and record['base_bundle_sha256'] == base_bundle_sha256
            and record['source_extension_sha256'] == source_extension_sha256,
            'recommendation_documents_descriptor_invalid')
        root = Path(root)
        _need(root.is_absolute() and root.is_dir() and root.resolve() == root,
            'recommendation_documents_root_invalid')
        raw = _read(root / record['path'], MAX_MANIFEST)
        _need(sha256(raw).hexdigest() == record['sha256'], 'recommendation_documents_manifest_hash_mismatch')
        manifest = _decode(raw)
        _need(isinstance(manifest, Mapping) and set(manifest) == {
            'schema_version', 'base_bundle_sha256', 'source_extension_sha256', 'records'}
            and type(manifest['schema_version']) is int and manifest['schema_version'] == 1
            and manifest['base_bundle_sha256'] == base_bundle_sha256
            and manifest['source_extension_sha256'] == source_extension_sha256
            and isinstance(manifest['records'], Mapping) and 1 <= len(manifest['records']) <= MAX_ASSETS,
            'recommendation_documents_manifest_invalid')
        _need(isinstance(bundle, Mapping) and isinstance(resources, Mapping), 'recommendation_documents_bundle_invalid')
        owners = _source_owners(bundle, resources)
        _need(set(manifest['records']) <= set(owners), 'recommendation_documents_scope_invalid')
        docroot = root / 'documents'
        _need(docroot.is_dir() and not docroot.is_symlink() and docroot.resolve().parent == root,
            'recommendation_documents_root_invalid')
        result, total = dict(resources), 0
        for asset, entry in manifest['records'].items():
            maximum = MAX_PDF if asset.endswith('.pdf') else MAX_IMAGE
            _need(isinstance(entry, Mapping) and set(entry) == {'name', 'sha256', 'mime', 'size'}
                and entry['name'] == asset and isinstance(entry['sha256'], str) and HASH.fullmatch(entry['sha256'])
                and type(entry['size']) is int and 0 < entry['size'] <= maximum,
                'recommendation_documents_record_invalid')
            total += entry['size']
            _need(total <= MAX_TOTAL, 'recommendation_documents_total_limit')
            source = resources[asset]
            _need(('file' not in source or source['file'] == asset)
                and ('sha256' not in source or source['sha256'] == entry['sha256']),
                'recommendation_documents_resource_conflict')
            data = _read(docroot / asset, maximum)
            _need(len(data) == entry['size'] and sha256(data).hexdigest() == entry['sha256'],
                'recommendation_documents_content_hash_mismatch')
            _, mime = ReleasedProductDocuments._validate(data, asset)
            _need(mime == entry['mime'], 'recommendation_documents_mime_mismatch')
            for kind, identity in owners[asset]:
                if kind == 'tyre_sheet_url':
                    ReleasedProductDocuments._validate_tyre_identity(data, identity)
            result[asset] = {**source, 'file': asset, 'sha256': entry['sha256']}
        return result
    except DocumentOverlayError:
        raise
    except DocumentError:
        raise DocumentOverlayError('recommendation_documents_content_invalid') from None
    except Exception:
        raise DocumentOverlayError('recommendation_documents_invalid') from None
