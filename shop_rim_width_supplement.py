"""Nine exact Continental source/table additions; no purchasing authority.

This optional RAM-only supplement follows the existing two-row/125-source
extensions. It cannot alter any price, supplier binding, document or vehicle
approval, and every proposed tyre must independently match the new rows.
"""
from collections import Counter
from collections.abc import Mapping
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat

from shop_catalog_public import variant_fingerprint
from shop_rim_width_candidates import released_width_pairs, tyre_nominal_facts
from shop_rsu_rim_evidence import WIDTH_TABLE_SHA
from shop_tyre_source_extension import SOURCES as SOURCE_RECEIPTS

MAX_BYTES = 24 * 1024
HASH = re.compile(r'[a-f0-9]{64}')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}')
SOURCES = {**SOURCE_RECEIPTS, 'width_table_sha256': WIDTH_TABLE_SHA}
# Original PDF6102 pages17/18/23/28/29/36, visually reviewed in full.
# Each tuple is size, load variants, explicit J widths, PDF page, printed page.
_ROWS = (
    ('205/55 R16', ((91, False), (94, True)), (6, 6.5), 23, 44),
    ('215/55 R16', ((93, False), (97, True)), (6.5, 7), 23, 44),
    ('225/55 R16', ((95, False), (99, True)), (6.5, 7, 7.5), 23, 45),
    ('205/70 R16', ((97, False),), (5.5, 6, 6.5), 18, 35),
    ('225/75 R16', ((104, False), (108, True)), (6, 6.5, 7), 17, 33),
    ('245/45 R21', ((104, True),), (7.5, 8, 8.5, 9), 28, 55),
    ('265/45 R21', ((104, False), (108, True)), (8.5, 9, 9.5, 10), 28, 55),
    ('315/45 R21', ((116, False),), (10, 10.5, 11, 11.5, 12), 29, 56),
    ('295/30 R21', ((102, True),), (10, 10.5, 11), 36, 71),
)
REVIEWED_ROWS = tuple({
    'size': size, 'width_mm': int(size.split('/')[0]),
    'ratio_percent': int(size.split('/')[1].split()[0]),
    'diameter_in': int(size.split('R')[1]),
    'load_variants': [{'load_index': load, 'xl': xl} for load, xl in variants],
    'permitted_rim_widths_in': list(widths), 'pdf_page': page,
    'printed_page': printed, 'visually_reviewed': True,
} for size, variants, widths, page, printed in _ROWS)
OLD_RSU = SOURCE_RECEIPTS['old_rsu_csv_sha256']
NEW_RSU = SOURCE_RECEIPTS['new_rsu_csv_sha256']
# width, ratio, diameter, load, explicit XL or unknown, ALZURA row, RSU row, source.
EXPECTED = {
    'tyre:0e9ff1ea0f1802f4793a7df5': (295, 30, 21, 102, True, 122989, 56009, NEW_RSU),
    'tyre:131263519e0c8b80a780e451': (245, 45, 21, 104, True, 108777, 44881, NEW_RSU),
    'tyre:13e30c633f6d5a73210b1a4d': (215, 55, 16, 97, True, 104413, 39280, NEW_RSU),
    'tyre:2a6dece4018d4a3bc7d39754': (265, 45, 21, 108, True, 126597, 69090, NEW_RSU),
    'tyre:4edaf012b2eab9bd748dca10': (205, 70, 16, 97, None, 93280, 2431, NEW_RSU),
    'tyre:5f3b73ce77806fc5020d77a5': (225, 55, 16, 99, True, 117679, 39277, NEW_RSU),
    'tyre:76ae04cc8b7956924ec2883d': (225, 75, 16, 104, None, 79882, 77486, NEW_RSU),
    'tyre:b64fd33ed8e5b5ae0de3b1b3': (315, 45, 21, 116, None, 111968, 23916, NEW_RSU),
    'tyre:f8343bad4c8b97bb37e90751': (205, 55, 16, 91, None, 52290, 83241, OLD_RSU),
}


class RimWidthSupplementError(ValueError):
    """Fixed codes only, never private product/source values."""


def _need(value, code):
    if not value:
        raise RimWidthSupplementError(code)


def _read(root, name):
    root = Path(root)
    _need(root.is_absolute() and root.is_dir() and root.resolve() == root
          and not any(p.is_symlink() for p in (root, *root.parents)), 'width_supplement_root_invalid')
    path = root / name
    _need(not path.is_symlink() and path.parent == root, 'width_supplement_path_invalid')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    try:
        meta = os.fstat(fd)
        _need(stat.S_ISREG(meta.st_mode) and 0 < meta.st_size <= MAX_BYTES, 'width_supplement_size_invalid')
        with os.fdopen(fd, 'rb') as stream:
            fd = None
            raw = stream.read(MAX_BYTES + 1)
    finally:
        if fd is not None:
            os.close(fd)
    _need(0 < len(raw) <= MAX_BYTES, 'width_supplement_size_invalid')
    return raw


def _decode(raw):
    def unique(items):
        value = {}
        for key, item in items:
            _need(key not in value, 'width_supplement_duplicate_key')
            value[key] = item
        return value
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))


def load_rim_width_supplement(root, record, bundle, *, base_bundle_sha256):
    """Append the nine immutable rows/facts, independently proving eligibility.

    Missing/invalid EU documents are separately filtered by the public catalogue.
    This loader grants no document, stock, price, supplier or vehicle permission.
    """
    if record is None:
        return bundle
    try:
        _need(isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256)
              and isinstance(record, Mapping) and set(record) == {'path', 'sha256', 'base_bundle_sha256'}
              and isinstance(record['path'], str) and NAME.fullmatch(record['path'])
              and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256'])
              and record['base_bundle_sha256'] == base_bundle_sha256, 'width_supplement_descriptor_invalid')
        raw = _read(root, record['path'])
        _need(sha256(raw).hexdigest() == record['sha256'], 'width_supplement_hash_mismatch')
        value = _decode(raw)
        _need(isinstance(value, Mapping) and set(value) == {
            'schema_version', 'base_bundle_sha256', 'registry_rows', 'nominal_tyre_sources', 'sources'}
            and type(value['schema_version']) is int and value['schema_version'] == 1
            and value['base_bundle_sha256'] == base_bundle_sha256
            and value['sources'] == SOURCES, 'width_supplement_schema_invalid')
        # JSON equality alone would equate true/1 and false/0. Canonical bytes
        # preserve the exact reviewed row value types as well as their values.
        canonical = lambda item: json.dumps(item, sort_keys=True, separators=(',', ':'), allow_nan=False)
        _need(canonical(value['registry_rows']) == canonical(list(REVIEWED_ROWS)),
              'width_supplement_rows_mismatch')
        _need(isinstance(bundle, Mapping), 'width_supplement_bundle_invalid')
        registry = bundle.get('reviewed_rim_width_registry')
        old, extra = bundle.get('nominal_tyre_sources'), value['nominal_tyre_sources']
        seeds, bindings = bundle.get('rsu_price_seeds', {}).get('records'), bundle.get('rsu_bindings')
        released = bundle.get('publication', {}).get('released_ids')
        _need(isinstance(registry, Mapping) and isinstance(registry.get('rows'), list)
              and registry.get('source', {}).get('sha256') == WIDTH_TABLE_SHA
              and isinstance(old, Mapping) and isinstance(extra, Mapping) and set(extra) == set(EXPECTED)
              and not set(extra) & set(old) and isinstance(seeds, Mapping) and isinstance(bindings, Mapping)
              and isinstance(released, list) and set(extra) <= set(seeds) & set(bindings) & set(released),
              'width_supplement_membership_invalid')
        sizes = {row['size'] for row in REVIEWED_ROWS}
        _need(not any(isinstance(row, Mapping) and row.get('size') in sizes for row in registry['rows']),
              'width_supplement_registry_override')
        rows = bundle.get('products')
        _need(isinstance(rows, list) and 1 <= len(rows) <= 100000, 'width_supplement_products_invalid')
        counts = Counter(p.get('selection_id', p.get('id')) for p in rows if isinstance(p, Mapping)
                         and isinstance(p.get('selection_id', p.get('id')), str))
        _need(all(counts[identifier] == 1 for identifier in EXPECTED), 'width_supplement_duplicate_product')
        by = {p.get('selection_id', p.get('id')): p for p in rows if isinstance(p, Mapping)}
        for identifier, expected in EXPECTED.items():
            width, ratio, diameter, load, xl, alz_row, rsu_row, receipt = expected
            product = by.get(identifier)
            _need(isinstance(product, Mapping) and product.get('category') == 'tyre',
                  'width_supplement_product_missing')
            proof = extra[identifier].get('proof') if isinstance(extra[identifier], Mapping) else None
            _need(isinstance(proof, Mapping)
                  and proof.get('identity_hash') == variant_fingerprint(product)
                  and proof.get('product_id') == identifier
                  and proof.get('alzura_source_sha256') == SOURCE_RECEIPTS['alzura_archive_sha256']
                  and proof.get('rsu_source_sha256') == receipt
                  and type(proof.get('alzura_row_number')) is int and proof['alzura_row_number'] == alz_row
                  and type(proof.get('rsu_row_number')) is int and proof['rsu_row_number'] == rsu_row,
                  'width_supplement_original_row_mismatch')
            shadow = deepcopy(product)
            shadow['details'] = {**shadow.get('details', {}), 'Artikelbezeichnung': proof.get('articlename'),
                'Zusatzmerkmale': ' '.join(proof.get(key, '') for key in
                    ('description', 'info', 'manufacturer_description', 'rsu_profile'))}
            facts = tyre_nominal_facts(shadow)
            _need(facts is not None and (facts['width_mm'], facts['ratio_percent'], facts['diameter_in'],
                  facts['load_index'], facts['xl']) == (width, ratio, diameter, load, xl),
                  'width_supplement_nominal_mismatch')
        merged = {**bundle, 'nominal_tyre_sources': {**old, **extra},
            'reviewed_rim_width_registry': {**registry, 'rows': [*registry['rows'], *deepcopy(value['registry_rows'])]}}
        independent = {**merged, 'compatible_pairs': {}, 'nominal_tyre_sources': extra}
        pairs = released_width_pairs(independent, approved_ids=set(seeds))
        _need(all(isinstance(p, list) and len(p) <= 1000 for p in pairs.values()),
              'width_supplement_candidate_limit')
        _need(set(extra) <= {p.get('tyre_id') for values in pairs.values() for p in values},
              'width_supplement_not_eligible')
        return merged
    except RimWidthSupplementError:
        raise
    except Exception:
        raise RimWidthSupplementError('width_supplement_validation_failed') from None
