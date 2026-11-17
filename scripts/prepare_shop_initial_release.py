"""Offline reviewed identity/media release; no credentials, prices or providers.

Inputs are the hash-bound full tyre ZIP, exact manufacturer-SKU candidates and
the separately reviewed REST/EAN/local-photo crosswalk. Only an explicit review
reference arms preparation. Output is a new atomic directory; inputs and image
bytes are never modified. This is static release preparation, not publication.

The displayed source wallclock is UNVERIFIED. Its assumed Berlin offset is used
only to satisfy the existing price-disabled normalizer; that synthetic technical
timestamp is discarded. No source observation/timezone/fresh price is asserted.
"""
import argparse
from collections import Counter
from copy import deepcopy
import csv
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import TextIOWrapper
import json
import os
from pathlib import Path
import re
import shutil
import sys
from uuid import uuid4
from zipfile import ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_alzura_feed import (FeedMetadata, HEADERS, MAX_ARCHIVE_BYTES, MAX_CSV_BYTES,
                              MAX_FIELD_CHARS, MAX_ROWS, MEMBER, normalize_row)
from shop_catalog_public import PublicCatalog, SOURCE_PHOTO, variant_fingerprint
from shop_media import ReleasedMedia
from shop_rsu_match import alzura_record
from shop_rsu_quotes import RSUQuotePolicy, RSUQuoteProvider, _ean

HASH = re.compile(r'[a-f0-9]{64}')
ARTICLE = re.compile(r'[1-9][0-9]{0,17}')
MAX_JSON = 96 * 1024 * 1024
PRICE_FIELDS = ('price_1', 'price_4', 'avg_price', 'price_anonym', 'price_rvo', 'price_anonym_one', 'availability')


class ReleaseError(ValueError):
    """Fixed reason only; never echo source rows, commercial fields or paths."""


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def digest_stream(stream, maximum):
    digest, size = sha256(), 0
    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
        size += len(chunk)
        if size > maximum:
            raise ReleaseError('input_size_limit')
        digest.update(chunk)
    return digest.hexdigest(), size


def file_digest(path, maximum=MAX_JSON):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ReleaseError('invalid_input_file')
    with path.open('rb') as stream:
        return digest_stream(stream, maximum)[0]


def read_json(path, *, expected_hash=None):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ReleaseError('duplicate_json_key')
            result[key] = value
        return result
    path = Path(path)
    with path.open('rb') as stream:
        raw = stream.read(MAX_JSON + 1)
    if len(raw) > MAX_JSON:
        raise ReleaseError('input_size_limit')
    if expected_hash is not None and sha256(raw).hexdigest() != expected_hash:
        raise ReleaseError('input_changed_during_preparation')
    return json.loads(raw.decode('utf-8'), object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ReleaseError('invalid_json_number')))


class NoInquiry:
    def inquire(self, *args, **kwargs):
        raise AssertionError('No provider operation is permitted during offline preparation')


def _indices(crosswalk, mappings, catalog, hashes, expected_count):
    if (crosswalk.get('schema_version') != 1 or mappings.get('schema_version') != 1
            or crosswalk.get('source_catalog_sha256') != hashes['rest_catalog']
            or mappings.get('source_hashes', {}).get('alzura') != hashes['archive']
            or not HASH.fullmatch(mappings.get('source_hashes', {}).get('rsu', ''))
            or not isinstance(crosswalk.get('candidates'), list) or len(crosswalk['candidates']) != expected_count
            or not isinstance(mappings.get('mappings'), list) or len(mappings['mappings']) > 100000):
        raise ReleaseError('source_metadata_mismatch')
    by_evidence = {}
    for mapping in mappings['mappings']:
        evidence_id = mapping.get('evidence_id') if isinstance(mapping, dict) else None
        if (not isinstance(evidence_id, str) or not HASH.fullmatch(evidence_id)
                or evidence_id in by_evidence or sha256(canonical(mapping.get('evidence')).encode()).hexdigest() != evidence_id):
            raise ReleaseError('invalid_mapping_metadata')
        by_evidence[evidence_id] = mapping
    rest, review, supplier_ids, old_ids = {}, {}, set(), set()
    rows = catalog.get('catalogues', {}).get('reifen')
    if not isinstance(rows, list):
        raise ReleaseError('invalid_rest_catalog')
    for row in rows:
        identifier = row.get('selection_id') if isinstance(row, dict) else None
        if not isinstance(identifier, str) or identifier in rest:
            raise ReleaseError('duplicate_rest_identity')
        rest[identifier] = row
    for entry in crosswalk['candidates']:
        if not isinstance(entry, dict):
            raise ReleaseError('invalid_crosswalk')
        article, supplier = entry.get('alzura_full_id'), entry.get('rsu_article_id')
        old_id, evidence_id = entry.get('rest_selection_id'), entry.get('mapping_evidence_id')
        if (not isinstance(article, str) or not ARTICLE.fullmatch(article) or article in review
                or not isinstance(supplier, str) or not ARTICLE.fullmatch(supplier) or supplier in supplier_ids
                or old_id in old_ids or old_id not in rest or evidence_id not in by_evidence
                or entry.get('rest_catalog_sha256') != hashes['rest_catalog']
                or entry.get('same_brand_mpn_and_ean') is not True or entry.get('same_core_size') is not True
                or not isinstance(entry.get('photo'), str) or not SOURCE_PHOTO.fullmatch(entry['photo'])
                or not isinstance(entry.get('photo_sha256'), str) or not HASH.fullmatch(entry['photo_sha256'])):
            raise ReleaseError('invalid_crosswalk')
        candidate = by_evidence[evidence_id]
        proof = candidate['evidence']
        if (proof['alzura']['article_id'] != article or proof['rsu']['article_id'] != supplier
                or proof['alzura']['source_sha256'] != hashes['archive']
                or proof['rsu']['source_sha256'] != mappings['source_hashes']['rsu']):
            raise ReleaseError('mapping_crosswalk_mismatch')
        original = rest[old_id]
        identifiers = original.get('identifiers', {})
        if (original.get('category') != 'tyre' or original.get('dimensions_valid') is not True
                or str(identifiers.get('id_t24')) != article or not _ean(identifiers.get('ean'))
                or original.get('photo') != entry['photo']
                or ' '.join(str(original.get('manufacturer_name', '')).casefold().split()) != proof['manufacturer']
                or ' '.join(str(identifiers.get('manufacturer_article_number', '')).casefold().split()) != proof['mpn']
                or any(str(original.get(field)) != proof['core'][core] for field, core in
                       (('width', 'width_mm'), ('ratio', 'aspect_ratio_percent'), ('diameter', 'diameter_inch')))):
            raise ReleaseError('rest_corroboration_mismatch')
        review[article] = (entry, candidate, identifiers['ean'])
        supplier_ids.add(supplier)
        old_ids.add(old_id)
    return review


def _products(archive_path, review, archive_hash, *, now):
    products, bindings, provenance, seen, found = [], {}, [], set(), set()
    # This is a declared technical assumption, not asserted supplier chronology.
    normalization_time = datetime(2026, 11, 16, 12, 14, tzinfo=timezone(timedelta(hours=1)))
    with ZipFile(archive_path) as archive:
        infos = archive.infolist()
        if (len(infos) != 1 or not MEMBER.fullmatch(infos[0].filename) or infos[0].flag_bits & 1
                or infos[0].is_dir() or infos[0].file_size > MAX_CSV_BYTES
                or infos[0].compress_size > MAX_ARCHIVE_BYTES
                or ((infos[0].external_attr >> 16) & 0o170000) == 0o120000):
            raise ReleaseError('invalid_archive_metadata')
        info = infos[0]
        with archive.open(info) as stream:
            csv_hash, size = digest_stream(stream, MAX_CSV_BYTES)
        if size != info.file_size:
            raise ReleaseError('csv_size_mismatch')
        metadata = FeedMetadata(archive_sha256=archive_hash, member=info.filename,
            supplier_observed_at=normalization_time, timestamp_evidence='UNVERIFIED_normalization_only_assumed_Berlin',
            currency=None, unit_net_fields=())
        with archive.open(info) as binary, TextIOWrapper(binary, encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(stream, delimiter='|', strict=True)
            if tuple(reader.fieldnames or ()) != HEADERS:
                raise ReleaseError('unexpected_csv_schema')
            for number, row in enumerate(reader, 1):
                if (number > MAX_ROWS or None in row or None in row.values() or set(row) != set(HEADERS)
                        or any(not isinstance(v, str) or len(v) > MAX_FIELD_CHARS for v in row.values())
                        or not ARTICLE.fullmatch(row['id']) or row['id'] in seen):
                    raise ReleaseError('invalid_or_duplicate_csv_identity')
                seen.add(row['id'])
                if row['id'] not in review:
                    continue
                entry, candidate, expected_ean = review[row['id']]
                record = alzura_record(row, archive_sha256=archive_hash, row_number=number)
                source = candidate['evidence']['alzura']
                if (record.source_row_fingerprint != source['row_fingerprint'] or record.row_number != source['row_number']
                        or record.identity_fingerprint != source['identity_fingerprint']
                        or not record.complete() or not record.conditions_clear):
                    raise ReleaseError('full_csv_identity_mismatch')
                clean = {**row, **dict.fromkeys(PRICE_FIELDS, '')}
                normalized = normalize_row(clean, metadata=metadata, row_number=number, now=now)
                if normalized is None or normalized[0]['dimensions_valid'] is not True:
                    raise ReleaseError('invalid_reviewed_dimensions')
                product = normalized[0]
                sid = product['selection_id']
                if sid in bindings:
                    raise ReleaseError('duplicate_public_identity')
                product.update(id=sid, photo=entry['photo'], quote_public=None, quote_status='not_queried',
                               pricing_mode='rsu_fresh_quote_required')
                product['details']['EAN'] = expected_ean
                product['identifiers']['ean'] = expected_ean
                product['identity_hash'] = variant_fingerprint(product)
                products.append(product)
                bindings[sid] = {'identity_hash': product['identity_hash'], 'category': 'tyre',
                                 'mapping': deepcopy(candidate), 'expected_ean': expected_ean}
                provenance.append({'product_id': sid, 'archive_sha256': archive_hash, 'csv_row_number': number,
                    'mapping_evidence_id': entry['mapping_evidence_id'], 'rest_selection_id': entry['rest_selection_id'],
                    'rest_catalog_sha256': entry['rest_catalog_sha256'], 'expected_ean': expected_ean,
                    'source_photo': entry['photo'], 'photo_sha256': entry['photo_sha256']})
                found.add(row['id'])
    if found != set(review):
        raise ReleaseError('reviewed_product_missing')
    return sorted(products, key=lambda p: p['id']), bindings, provenance, csv_hash, info.filename, len(seen)


def _images(provenance, media_root):
    root, resources, payloads, sources, image_urls = Path(media_root).resolve(), {}, {}, {}, {}
    for entry in provenance:
        source = root / entry['source_photo']
        resolved = source.resolve()
        if (source.is_symlink() or not resolved.is_relative_to(root) or resolved == root
                or not resolved.is_file() or not 0 < resolved.stat().st_size <= 1024 * 1024):
            raise ReleaseError('invalid_local_media')
        data = resolved.read_bytes()
        digest = sha256(data).hexdigest()
        if digest != entry['photo_sha256']:
            raise ReleaseError('local_media_hash_mismatch')
        _, mime = ReleasedMedia._validate(data)
        extension = {'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp'}[mime]
        asset = digest + '.' + extension
        if asset in payloads and payloads[asset] != data:
            raise ReleaseError('media_collision')
        payloads[asset] = data
        resources[asset] = {'file': 'public-media/' + asset, 'sha256': digest}
        sources.setdefault(asset, set()).add(entry['source_photo'])
        image_urls[entry['source_photo']] = '/shop/media/' + asset
        entry['asset'] = asset
    return resources, payloads, {k: sorted(v) for k, v in sources.items()}, image_urls


def _write(path, data):
    with path.open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def prepare_release(*, archive_path, crosswalk_path, mappings_path, rest_catalog_path, media_root,
                    output_dir, review_reference, validation_policy, expected_count=481, now=None):
    """All paths explicit; validation_policy is offline constructor test data only."""
    stage = None
    output = Path(output_dir).resolve()
    try:
        if (not isinstance(review_reference, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{2,159}', review_reference)
                or type(expected_count) is not int or not 1 <= expected_count <= 2000
                or not isinstance(validation_policy, RSUQuotePolicy) or validation_policy.mode != 'test'
                or validation_policy.currency != 'EUR' or validation_policy.net_unit_price_confirmed is not True):
            raise ReleaseError('explicit_review_and_offline_validation_policy_required')
        now = now or datetime.now(timezone.utc)
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ReleaseError('invalid_preparation_clock')
        if output.exists() or not output.parent.is_dir() or output.is_symlink():
            raise ReleaseError('output_must_be_new_directory')
        hashes = {'archive': file_digest(archive_path, MAX_ARCHIVE_BYTES), 'crosswalk': file_digest(crosswalk_path),
                  'mappings': file_digest(mappings_path), 'rest_catalog': file_digest(rest_catalog_path)}
        review = _indices(read_json(crosswalk_path, expected_hash=hashes['crosswalk']),
                          read_json(mappings_path, expected_hash=hashes['mappings']),
                          read_json(rest_catalog_path, expected_hash=hashes['rest_catalog']), hashes, expected_count)
        products, bindings, provenance, csv_hash, member, rows = _products(archive_path, review, hashes['archive'], now=now)
        if file_digest(archive_path, MAX_ARCHIVE_BYTES) != hashes['archive']:
            raise ReleaseError('input_changed_during_preparation')
        resources, payloads, sources, image_urls = _images(provenance, media_root)
        approved = frozenset(bindings)
        provider = RSUQuoteProvider(bindings=bindings, approved_ids=approved, policy=validation_policy,
                                    inquiry=NoInquiry(), clock=lambda: now)
        for product in products:
            if product['identity_hash'] != variant_fingerprint(product) or provider.rsu_mapping(product) is None:
                raise ReleaseError('binding_product_validation_failed')
        catalogue = PublicCatalog(products, released_ids=approved, image_urls=image_urls, clock=lambda: now)
        for product in products:
            public = catalogue.get(product['id'])
            if public is None or len(public['images']) != 1 or public.get('price') is not None:
                raise ReleaseError('public_projection_validation_failed')
        publication = {'schema_version': 1, 'review_reference': review_reference, 'released_ids': sorted(approved),
                       'image_urls': image_urls, 'approved_assets': sorted(resources)}
        manifest = {'schema_version': 1, 'review_reference': review_reference, 'resources': resources,
                    'approved_assets': sorted(resources), 'original_source_files': sources, 'original_bytes_unchanged': True}
        files = {'products.jsonl': ''.join(canonical(p) + '\n' for p in products).encode('utf-8'),
                 'publication.json': (canonical(publication) + '\n').encode('utf-8'),
                 'bindings.private.json': (canonical(bindings) + '\n').encode('utf-8'),
                 'media_manifest.private.json': (canonical(manifest) + '\n').encode('utf-8'),
                 'provenance.private.jsonl': ''.join(canonical(p) + '\n' for p in sorted(provenance, key=lambda v: v['product_id'])).encode('utf-8')}
        files.update({'public-media/' + asset: data for asset, data in payloads.items()})
        file_manifest = {name: {'sha256': sha256(data).hexdigest(), 'bytes': len(data)} for name, data in sorted(files.items())}
        report = {'schema_version': 1, 'review_reference': review_reference, 'prepared_at': now.isoformat(),
            'counts': {'products': len(products), 'bindings': len(bindings), 'images': len(resources),
                       'csv_rows_validated': rows, 'product_images': len(provenance),
                       'seasons': dict(Counter(p['season'] for p in products))},
            'source_hashes': hashes, 'csv_sha256': csv_hash, 'csv_member': member,
            'source_time': {'original_displayed_wallclock': '16.11.2026 12:14', 'status': 'UNVERIFIED',
                'supplier_observed_at': None, 'timezone_confirmed': False,
                'normalizer_only_assumption': 'Europe/Berlin UTC+01:00; discarded; not freshness evidence'},
            'quote_public_included': False, 'buying_prices_included': False, 'provider_calls': 0,
            'live_policy_validated': False, 'provider_activation': False, 'orders_enabled': False, 'payments_enabled': False,
            'files': file_manifest, 'dataset_sha256': sha256(canonical(file_manifest).encode('utf-8')).hexdigest()}
        files['report.json'] = (canonical(report) + '\n').encode('utf-8')
        stage = output.parent / ('.' + output.name + '.staging-' + uuid4().hex)
        if stage.resolve().parent != output.parent or output.exists():
            raise ReleaseError('invalid_output_boundary')
        stage.mkdir(mode=0o700)
        (stage / 'public-media').mkdir()
        for name, data in sorted(files.items()):
            _write(stage / name, data)
        # Check both resolved targets before the Windows directory move.
        if stage.resolve().parent != output.parent or output.exists() or output.parent != output.resolve().parent:
            raise ReleaseError('invalid_output_boundary')
        os.replace(stage, output)
        stage = None
        return report
    except ReleaseError:
        raise
    except Exception:
        raise ReleaseError('initial_release_preparation_failed') from None
    finally:
        if (stage is not None and stage.exists() and not stage.is_symlink()
                and stage.resolve().parent == output.parent and stage.name.startswith('.' + output.name + '.staging-')):
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('archive', 'crosswalk', 'mappings', 'rest-catalog', 'media-root', 'output', 'review-reference', 'offline-policy-reference'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--expected-count', type=int, default=481)
    args = parser.parse_args()
    try:
        report = prepare_release(archive_path=args.archive, crosswalk_path=args.crosswalk, mappings_path=args.mappings,
            rest_catalog_path=args.rest_catalog, media_root=args.media_root, output_dir=args.output,
            review_reference=args.review_reference, expected_count=args.expected_count,
            validation_policy=RSUQuotePolicy(mode='test', currency='EUR', net_unit_price_confirmed=True,
                                             evidence_id=args.offline_policy_reference))
        print(canonical({key: report[key] for key in ('counts', 'source_hashes', 'dataset_sha256', 'provider_calls', 'buying_prices_included')}))
        return 0
    except ReleaseError as error:
        print(canonical({'error': str(error)}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
