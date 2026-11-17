"""Exact-variant original provider documents, never vehicle-fitment authority.

An offline builder must verify original response hashes, requested article IDs
and known geometry before creating these records. Their canonical hashes bind
that reviewed release; they do not authenticate the supplier by themselves.
No file, network, price, order or payment operations occur in this module.
"""
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import re

from shop_catalog_public import variant_fingerprint
from shop_rsu_rim_evidence import normalize_rim_geometry


RULE = 'exact_provider_variant_original_document_v1'
SCOPE = 'provider_document_not_vehicle_approval'
HASH = re.compile(r'[a-f0-9]{64}')
REPORT_URL = re.compile(
    r'https://media[1-5]\.tyre-shopping\.com/public/technical_reports/'
    r'[0-9]{1,4}/[A-Za-z0-9_.~-]{1,180}\.(?:pdf|PDF)')
PROOF_FIELDS = {'rule', 'product_id', 'identity_hash', 'provider_id', 'requested_car_id',
                'source_body_sha256', 'geometry', 'url', 'report_number', 'type_description'}


class RimReportError(ValueError):
    pass


def _hash(value):
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(',', ':'), allow_nan=False).encode('utf-8')).hexdigest()


def _label(value):
    if value is None:
        return ''
    if (not isinstance(value, str) or len(value) > 160
            or any(ord(c) < 32 or ord(c) == 127 or 0xD800 <= ord(c) <= 0xDFFF for c in value)):
        raise RimReportError('invalid_rim_report')
    return value.strip()


def rim_report_asset(url):
    if (not isinstance(url, str) or not REPORT_URL.fullmatch(url)
            or '..' in url.rsplit('/', 1)[-1]):
        raise RimReportError('invalid_rim_report_url')
    return sha256(url.encode('utf-8')).hexdigest() + '.pdf'


def rim_report_key(url):
    return 'rim-report:' + rim_report_asset(url)[:-4]


def _known_geometry(row, geometry):
    # Unknown typed fields in old provider snapshots stay unknown. The exact
    # provider article ID still binds the source; never infer missing geometry.
    for field in geometry:
        actual = row.get(field)
        if actual in (None, ''):
            if field in {'width', 'diameter'}:
                raise RimReportError('invalid_rim_report_identity')
            continue
        try:
            if isinstance(actual, bool) or Decimal(str(actual)) != Decimal(geometry[field]):
                raise ValueError()
        except (ValueError, TypeError, InvalidOperation):
            raise RimReportError('invalid_rim_report_identity') from None


def bind_provider_report(row, *, provider_id, requested_car_id, source_body_sha256,
                         geometry, report):
    """Create a detached proof after the caller verifies its retained source."""
    identifier = row.get('selection_id', row.get('id')) if isinstance(row, Mapping) else None
    native = row.get('identifiers') if isinstance(row, Mapping) else None
    if (not isinstance(identifier, str) or not re.fullmatch(r'(?:alloy_wheel|steel_wheel):[a-f0-9]{24}', identifier)
            or row.get('category') not in {'alloy_wheel', 'steel_wheel'}
            or not isinstance(native, Mapping) or native.get('provider') != 'ALZURA'
            or not isinstance(provider_id, str) or not re.fullmatch(r'[1-9][0-9]{0,17}', provider_id)
            or str(native.get('provider_id')) != provider_id
            or type(requested_car_id) is not int or not 1 <= requested_car_id <= 10**18-1
            or not isinstance(source_body_sha256, str) or not HASH.fullmatch(source_body_sha256)
            or not isinstance(report, Mapping)):
        raise RimReportError('invalid_rim_report_identity')
    try:
        dimensions = normalize_rim_geometry(geometry)
        _known_geometry(row, dimensions)
        url = report.get('url')
        rim_report_asset(url)
        proof = {'rule': RULE, 'product_id': identifier, 'identity_hash': variant_fingerprint(row),
                 'provider_id': provider_id, 'requested_car_id': requested_car_id,
                 'source_body_sha256': source_body_sha256, 'geometry': dimensions, 'url': url,
                 'report_number': _label(report.get('report_number')),
                 'type_description': _label(report.get('type_description'))}
        return {'proof': proof, 'proof_sha256': _hash(proof)}
    except (TypeError, ValueError, UnicodeError):
        raise RimReportError('invalid_rim_report') from None


def public_rim_documents(row, record, document_urls):
    """Whitelist released opaque document paths separately from fitment data.

    Input: product_documents[id] = {identity_hash, rim_report_bindings:[...]};
    publication.document_urls maps rim_report_key(url) to the exact opaque path.
    A malformed, substituted or unreleased record returns None, never a URL.
    """
    try:
        if (not isinstance(row, Mapping) or not isinstance(record, Mapping)
                or record.get('identity_hash') != variant_fingerprint(row)
                or not isinstance(document_urls, Mapping)):
            return None
        bindings = record.get('rim_report_bindings')
        if not isinstance(bindings, list) or not 1 <= len(bindings) <= 48:
            return None
        reports, seen = [], set()
        for binding in bindings:
            if not isinstance(binding, Mapping) or set(binding) != {'proof', 'proof_sha256'}:
                return None
            proof = binding['proof']
            if (not isinstance(proof, Mapping) or set(proof) != PROOF_FIELDS
                    or proof.get('rule') != RULE or binding['proof_sha256'] != _hash(proof)):
                return None
            rebuilt = bind_provider_report(row, provider_id=proof['provider_id'],
                requested_car_id=proof['requested_car_id'], source_body_sha256=proof['source_body_sha256'],
                geometry=proof['geometry'], report=proof)
            if rebuilt != binding:
                return None
            asset = rim_report_asset(proof['url'])
            public_url = '/shop/documents/' + asset
            if document_urls.get(rim_report_key(proof['url'])) != public_url:
                return None
            if public_url in seen:
                continue
            seen.add(public_url)
            reports.append({'url': public_url, 'report_number': proof['report_number'],
                            'type_description': proof['type_description'], 'scope': SCOPE})
        return {'rim_report_urls': [r['url'] for r in reports], 'rim_reports': reports,
                'rim_report_status': 'original_documents_unreviewed'}
    except (KeyError, TypeError, ValueError, UnicodeError):
        return None
