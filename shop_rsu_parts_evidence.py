"""Exact released parts identities bound to separately inspected RSU sources.

These pure codecs perform no filesystem, environment, provider or payment IO.
They establish product identity only. Price, quantity/stock, freight, vehicle
fitment and purchasing permission must pass their independent boundaries.
Source hashes refer to the inspected immutable inputs, not self-authenticated
supplier assertions. Only caller-approved records may reach RSUQuoteProvider.
"""
from collections.abc import Mapping
from copy import deepcopy
from hashlib import sha256
import json
import re
import unicodedata

RULE = 'exact_parts_identity_sources_v1'
CATEGORIES = frozenset({'body_part', 'oil', 'care', 'accessory'})
PRODUCT_ID = re.compile(r'parts:[a-f0-9]{24}')
ARTICLE_ID = re.compile(r'[1-9][0-9]{0,17}')
HASH = re.compile(r'[a-f0-9]{64}')
NAMESPACES = {'catalogue': 'SHOP_PARTS_CATALOGUE', 'rsu': 'RSU_PARTS_SOURCE'}


class PartsEvidenceError(ValueError):
    """Fixed codes only; source payloads and supplier responses stay private."""


def _require(value):
    if not value:
        raise PartsEvidenceError('invalid_parts_mapping')


def _hash(value):
    return sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                             separators=(',', ':'), allow_nan=False).encode('utf-8')).hexdigest()


def _text(value, maximum):
    _require(isinstance(value, str) and 0 < len(value) <= maximum
             and not any(unicodedata.category(c).startswith('C') for c in value))
    # Preserve punctuation, suffixes and units: no fuzzy names or maker aliases.
    value = ' '.join(unicodedata.normalize('NFKC', value).casefold().split())
    _require(0 < len(value) <= maximum)
    return value


def _ean(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{13}', value) or set(value) == {'0'}:
        return False
    total = sum(int(c) * (3 if i % 2 == 0 else 1) for i, c in enumerate(reversed(value[:-1])))
    return (10 - total % 10) % 10 == int(value[-1])


def _identity(manufacturer, mpn, ean):
    _require(ean is None or _ean(ean))
    return {'manufacturer': _text(manufacturer, 120), 'mpn': _text(mpn, 160), 'ean': ean}


def parts_source_record(side, *, source_sha256, row_number, article_id, manufacturer, mpn, ean):
    """Create a source record. Catalogue article_id is the released public ID.

    row_number is the 1-based record within the immutable inspected source,
    e.g. the released products list or one extracted RSU product observation.
    Native ALZURA IDs are never invented when that source lacks one.
    """
    _require(isinstance(side, str) and side in NAMESPACES
             and isinstance(source_sha256, str) and HASH.fullmatch(source_sha256)
             and type(row_number) is int and 1 <= row_number <= 300_000
             and isinstance(article_id, str))
    _require(PRODUCT_ID.fullmatch(article_id) if side == 'catalogue' else ARTICLE_ID.fullmatch(article_id))
    identity = _identity(manufacturer, mpn, ean)
    source = {'source_sha256': source_sha256, 'row_number': row_number,
              'article_id': article_id, 'identity': identity}
    source['row_fingerprint'] = _hash({'namespace': NAMESPACES[side], **source})
    return source


def _source(side, value):
    _require(isinstance(value, Mapping)
             and set(value) == {'source_sha256', 'row_number', 'article_id', 'identity', 'row_fingerprint'}
             and isinstance(value.get('identity'), Mapping)
             and set(value['identity']) == {'manufacturer', 'mpn', 'ean'})
    identity = value['identity']
    expected = parts_source_record(side, source_sha256=value['source_sha256'], row_number=value['row_number'],
        article_id=value['article_id'], manufacturer=identity['manufacturer'], mpn=identity['mpn'], ean=identity['ean'])
    # Only a canonical source record is accepted, including its exact row hash.
    _require(dict(value) == expected)
    return expected


def validate_parts_mapping(mapping, *, product_id, category, expected_ean, identity_hash):
    """Validate identity proof only; never choose an undocumented supplier offer."""
    try:
        _require(isinstance(product_id, str) and PRODUCT_ID.fullmatch(product_id)
                 and isinstance(category, str) and category in CATEGORIES
                 and isinstance(identity_hash, str) and HASH.fullmatch(identity_hash)
                 and _ean(expected_ean) and isinstance(mapping, Mapping)
                 and set(mapping) == {'status', 'evidence_id', 'evidence'}
                 and mapping['status'] == 'exact_sku_candidate'
                 and isinstance(mapping['evidence_id'], str) and HASH.fullmatch(mapping['evidence_id']))
        proof = mapping['evidence']
        _require(isinstance(proof, Mapping) and set(proof) == {
            'rule', 'product_id', 'identity_hash', 'category', 'match_basis', 'catalogue', 'rsu', 'supplier_offer'}
            and proof['rule'] == RULE and proof['product_id'] == product_id
            and proof['identity_hash'] == identity_hash and proof['category'] == category
            and proof['supplier_offer'] is None and _hash(proof) == mapping['evidence_id'])
        catalogue, rsu = _source('catalogue', proof['catalogue']), _source('rsu', proof['rsu'])
        left, right = catalogue['identity'], rsu['identity']
        _require(catalogue['article_id'] == product_id
                 and left['manufacturer'] == right['manufacturer'] and left['mpn'] == right['mpn']
                 and right['ean'] == expected_ean)
        if left['ean'] is None:
            _require(proof['match_basis'] == 'manufacturer_mpn')
        else:
            _require(left['ean'] == right['ean'] and proof['match_basis'] == 'ean_manufacturer_mpn')
        return {'supplier_article_id': rsu['article_id'], 'expected_ean': expected_ean,
                'mapping_evidence_id': mapping['evidence_id'],
                'catalogue_source_sha256': catalogue['source_sha256'], 'rsu_source_sha256': rsu['source_sha256']}
    except Exception:
        raise PartsEvidenceError('invalid_parts_mapping') from None


def build_parts_binding(*, product_id, identity_hash, category, catalogue, rsu, supplier_offer=None):
    """Build a private candidate; explicit approved_ids remains a separate opt-in.

    Missing supplier EAN cannot be sent through today's frozen XML contract.
    Non-null offer metadata is not discarded: the binding fails closed until
    selecting that offer is supported by a documented transport contract.
    """
    try:
        catalogue, rsu = _source('catalogue', catalogue), _source('rsu', rsu)
        evidence = {'rule': RULE, 'product_id': product_id, 'identity_hash': identity_hash, 'category': category,
                    'match_basis': 'manufacturer_mpn' if catalogue['identity']['ean'] is None else 'ean_manufacturer_mpn',
                    'catalogue': catalogue, 'rsu': rsu, 'supplier_offer': deepcopy(supplier_offer)}
        mapping = {'status': 'exact_sku_candidate', 'evidence_id': _hash(evidence), 'evidence': evidence}
        binding = {'identity_hash': identity_hash, 'category': category, 'mapping': mapping,
                   'expected_ean': rsu['identity']['ean']}
        validate_parts_mapping(mapping, product_id=product_id, category=category,
                               expected_ean=binding['expected_ean'], identity_hash=identity_hash)
        return binding
    except Exception:
        raise PartsEvidenceError('invalid_parts_mapping') from None
