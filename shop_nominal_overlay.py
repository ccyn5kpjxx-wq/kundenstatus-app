"""Small, hash-bound private nominal evidence; immutable catalogue stays intact.

Explicit local file reads only. No default paths, provider calls, credentials,
prices, storage mutations or vehicle approval. The existing strict exact-source
width validator remains the authority for every added dimensional candidate.
"""
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
import json
import os
import re
import stat

from shop_rim_width_candidates import released_width_pairs

MAX_BYTES = 64 * 1024
HASH = re.compile(r'[a-f0-9]{64}')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}')
ASSET = re.compile(r'[a-f0-9]{64}\.(?:jpg|png|pdf)')
SOURCE_KEYS = {'alzura_archive_sha256', 'old_rsu_csv_sha256', 'new_rsu_csv_sha256'}


class NominalOverlayError(ValueError):
    """Fixed codes only; never include source text, private paths or prices."""


def _need(ok, code):
    if not ok:
        raise NominalOverlayError(code)


def _decode(raw):
    def unique(items):
        result = {}
        for key, value in items:
            _need(key not in result, 'overlay_duplicate_key')
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except NominalOverlayError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise NominalOverlayError('overlay_invalid_json') from None


def load_nominal_tyre_overlay(root, record, bundle, *, base_bundle_sha256):
    """Return a shallow merged envelope; no base field or object is changed.

    The descriptor is the explicit deployment approval. It pins a maximum64KiB
    file and the already hash-verified base bundle. Exactly37 separately reviewed
    source rows are added; pricing, source identities and vehicle rules cannot
    be supplied by this extension. Missing record preserves legacy behavior.
    """
    if record is None:
        return bundle
    _need(isinstance(record, Mapping) and set(record) == {'path', 'sha256', 'base_bundle_sha256'},
          'overlay_descriptor_invalid')
    _need(isinstance(record['path'], str) and NAME.fullmatch(record['path']) is not None
          and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256']) is not None
          and isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256) is not None
          and record['base_bundle_sha256'] == base_bundle_sha256, 'overlay_base_or_descriptor_mismatch')
    try:
        root = Path(root)
        _need(root.is_absolute() and root.is_dir() and not root.is_symlink() and root.resolve() == root,
              'overlay_root_invalid')
        path = root / record['path']
        _need(path.parent.resolve() == root and not path.is_symlink(), 'overlay_path_invalid')
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            info = os.fstat(fd)
            _need(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= MAX_BYTES, 'overlay_size_invalid')
            with os.fdopen(fd, 'rb') as source:
                fd = None
                raw = source.read(MAX_BYTES + 1)
        finally:
            if fd is not None:
                os.close(fd)
        _need(0 < len(raw) <= MAX_BYTES and sha256(raw).hexdigest() == record['sha256'], 'overlay_hash_mismatch')
    except NominalOverlayError:
        raise
    except (OSError, ValueError, TypeError):
        raise NominalOverlayError('overlay_file_unavailable') from None
    overlay = _decode(raw)
    _need(isinstance(overlay, Mapping) and set(overlay) == {
        'schema_version', 'base_bundle_sha256', 'nominal_tyre_sources', 'sources'}
        and type(overlay['schema_version']) is int and overlay['schema_version'] == 1
        and overlay['base_bundle_sha256'] == base_bundle_sha256, 'overlay_schema_invalid')
    sources = overlay['sources']
    _need(isinstance(sources, Mapping) and set(sources) == SOURCE_KEYS
          and all(isinstance(value, str) and HASH.fullmatch(value) for value in sources.values()), 'overlay_receipts_invalid')
    _need(isinstance(bundle, Mapping), 'overlay_bundle_invalid')
    previous, extra = bundle.get('nominal_tyre_sources', {}), overlay['nominal_tyre_sources']
    seed_envelope = bundle.get('rsu_price_seeds', {})
    seeds = seed_envelope.get('records', {}) if isinstance(seed_envelope, Mapping) else None
    bindings = bundle.get('rsu_bindings', {})
    _need(isinstance(previous, Mapping) and isinstance(extra, Mapping) and len(extra) == 37
          and not set(extra) & set(previous) and isinstance(seeds, Mapping) and isinstance(bindings, Mapping)
          and set(extra) <= set(seeds) and set(extra) <= set(bindings), 'overlay_source_membership_invalid')
    try:
        for value in extra.values():
            proof = value['proof']
            _need(proof['alzura_source_sha256'] == sources['alzura_archive_sha256']
                  and proof['rsu_source_sha256'] in {sources['old_rsu_csv_sha256'], sources['new_rsu_csv_sha256']},
                  'overlay_source_receipt_conflict')
        merged = {**bundle, 'nominal_tyre_sources': {**previous, **extra}}
        # Validate the new source facts independently of legacy pairs, which
        # must not make an otherwise excluded source appear eligible.
        pairs = released_width_pairs({**merged, 'compatible_pairs': {}}, approved_ids=set(seeds))
        _need(all(isinstance(rows, list) and len(rows) <= 1000 for rows in pairs.values()), 'overlay_candidate_limit')
        proposed = {item.get('tyre_id') for rows in pairs.values() for item in rows}
        _need(set(extra) <= proposed, 'overlay_source_not_eligible')
    except NominalOverlayError:
        raise
    except (ValueError, TypeError, KeyError, AttributeError):
        raise NominalOverlayError('overlay_source_validation_failed') from None
    return merged


def unavailable_document_product_ids(bundle, unavailable_assets):
    """Deny checkout for known unavailable released mandatory tyre documents.

    Prices, publication and RSU identities stay intact. Only exact approved
    same-origin label/sheet references are consumed; no URL/filename inference.
    """
    if unavailable_assets is None:
        return frozenset()
    _need(isinstance(unavailable_assets, list) and len(unavailable_assets) <= 1000
          and all(isinstance(asset, str) and ASSET.fullmatch(asset) for asset in unavailable_assets)
          and len(set(unavailable_assets)) == len(unavailable_assets), 'unavailable_documents_invalid')
    if not unavailable_assets:
        return frozenset()
    manifest = bundle.get('documents', {})
    _need(isinstance(manifest, Mapping) and isinstance(manifest.get('resources'), Mapping)
          and isinstance(manifest.get('approved_assets'), list)
          and all(isinstance(asset, str) for asset in manifest['approved_assets']), 'unavailable_documents_manifest_invalid')
    assets = set(unavailable_assets)
    _need(assets <= set(manifest['resources']) and assets <= set(manifest['approved_assets']),
          'unavailable_document_not_approved')
    products = {p.get('selection_id', p.get('id')): p for p in bundle.get('products', []) if isinstance(p, Mapping)}
    documents = bundle.get('product_documents')
    _need(isinstance(documents, Mapping), 'unavailable_documents_product_map_missing')
    used, blocked = set(), set()
    paths = {'/shop/documents/' + asset: asset for asset in assets}
    for identifier, record in documents.items():
        if products.get(identifier, {}).get('category') != 'tyre' or not isinstance(record, Mapping):
            continue
        for key in ('tyre_label_url', 'tyre_sheet_url'):
            url = record.get(key)
            if isinstance(url, str) and url in paths:
                blocked.add(identifier)
                used.add(paths[url])
    _need(used == assets, 'unavailable_document_not_referenced')
    return frozenset(blocked)
