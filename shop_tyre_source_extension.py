"""Optional exact-source tyre text extension for existing dimensional tables.

Only original, separately reviewed nominal facts are appended in RAM. This
does not add registry rows, prices, suppliers, documents or vehicle approvals.
"""
from collections import Counter
from collections.abc import Mapping
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat

from shop_rim_width_candidates import released_width_pairs

MAX_BYTES = 256 * 1024
SOURCE_COUNT = 125
HASH = re.compile(r'[a-f0-9]{64}')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}')
SOURCES = {
    'alzura_archive_sha256': '710376a514ebd82e562488d10e2feb0c1e95849ec1c9681fe11fa0e1e94965f7',
    'old_rsu_csv_sha256': '980f372dd687fa4f772cfe83c8b844a2e2b75f31a4eda57e27a4bf772580a778',
    'new_rsu_csv_sha256': '5fdd13c3906d20ad76969fc6226a2190c91a32bcf8173a7d06823ebe3c04e9b3',
}


class TyreSourceExtensionError(ValueError):
    """Fixed codes only; no private source data in error strings."""


def _need(value, code):
    if not value:
        raise TyreSourceExtensionError(code)


def _read(root, name):
    root = Path(root)
    _need(root.is_absolute() and root.is_dir() and root.resolve() == root
        and not any(p.is_symlink() for p in (root, *root.parents)), 'tyre_source_root_invalid')
    path = root / name
    _need(not path.is_symlink() and path.parent == root, 'tyre_source_path_invalid')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    try:
        info = os.fstat(fd)
        _need(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= MAX_BYTES, 'tyre_source_size_invalid')
        with os.fdopen(fd, 'rb') as source:
            fd = None
            raw = source.read(MAX_BYTES + 1)
    finally:
        if fd is not None:
            os.close(fd)
    _need(0 < len(raw) <= MAX_BYTES, 'tyre_source_size_invalid')
    return raw


def _decode(raw):
    def unique(items):
        result = {}
        for key, value in items:
            _need(key not in result, 'tyre_source_duplicate_key')
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))


def load_tyre_source_extension(root, record, bundle, *, base_bundle_sha256):
    """Append exactly the 125 approved source facts, never other capabilities.

    The normal released-width validator independently binds every proposed
    nominal text to the original, already approved ALZURA/RSU article identity.
    Each added tyre must produce a proposal using the unchanged current table;
    old compatible pairs cannot make a new ineligible tyre appear approved.
    """
    if record is None:
        return bundle
    try:
        _need(isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256)
            and isinstance(record, Mapping) and set(record) == {'path', 'sha256', 'base_bundle_sha256'}
            and isinstance(record['path'], str) and NAME.fullmatch(record['path'])
            and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256'])
            and record['base_bundle_sha256'] == base_bundle_sha256, 'tyre_source_descriptor_invalid')
        raw = _read(root, record['path'])
        _need(sha256(raw).hexdigest() == record['sha256'], 'tyre_source_hash_mismatch')
        value = _decode(raw)
        _need(isinstance(value, Mapping) and set(value) == {
            'schema_version', 'base_bundle_sha256', 'nominal_tyre_sources', 'sources'}
            and type(value['schema_version']) is int and value['schema_version'] == 1
            and value['base_bundle_sha256'] == base_bundle_sha256
            and value['sources'] == SOURCES, 'tyre_source_schema_invalid')
        _need(isinstance(bundle, Mapping), 'tyre_source_bundle_invalid')
        old, extra = bundle.get('nominal_tyre_sources'), value['nominal_tyre_sources']
        seeds, bindings = bundle.get('rsu_price_seeds', {}).get('records'), bundle.get('rsu_bindings')
        publication = bundle.get('publication')
        _need(isinstance(old, Mapping) and isinstance(extra, Mapping) and len(extra) == SOURCE_COUNT
            and not set(extra) & set(old) and isinstance(seeds, Mapping) and isinstance(bindings, Mapping)
            and isinstance(publication, Mapping) and isinstance(publication.get('released_ids'), list)
            and set(extra) <= set(seeds) & set(bindings) & set(publication['released_ids']),
            'tyre_source_membership_invalid')
        rows = bundle.get('products')
        _need(isinstance(rows, list) and 1 <= len(rows) <= 100000, 'tyre_source_products_invalid')
        counts = Counter(p.get('selection_id', p.get('id')) for p in rows if isinstance(p, Mapping)
            and isinstance(p.get('selection_id', p.get('id')), str))
        _need(all(counts[pid] == 1 for pid in extra), 'tyre_source_duplicate_product')
        for item in extra.values():
            proof = item.get('proof') if isinstance(item, Mapping) else None
            _need(isinstance(proof, Mapping)
                and proof.get('alzura_source_sha256') == SOURCES['alzura_archive_sha256']
                and proof.get('rsu_source_sha256') in {
                    SOURCES['old_rsu_csv_sha256'], SOURCES['new_rsu_csv_sha256']},
                'tyre_source_receipt_conflict')
        # The validator checks proof shape/hash, exact identity, source row,
        # manufacturer membership, special executions and approved rim widths.
        independent = {**bundle, 'compatible_pairs': {}, 'nominal_tyre_sources': extra}
        pairs = released_width_pairs(independent, approved_ids=set(seeds))
        _need(all(isinstance(values, list) and len(values) <= 1000 for values in pairs.values()),
            'tyre_source_candidate_limit')
        proposed = {pair.get('tyre_id') for values in pairs.values() for pair in values}
        _need(set(extra) <= proposed, 'tyre_source_not_eligible')
        return {**bundle, 'nominal_tyre_sources': {**old, **extra}}
    except TyreSourceExtensionError:
        raise
    except Exception:
        raise TyreSourceExtensionError('tyre_source_validation_failed') from None
