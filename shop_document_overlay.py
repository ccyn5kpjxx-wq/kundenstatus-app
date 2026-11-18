"""Explicit hash-bound local original documents; no network or bundle mutation."""
from collections.abc import Mapping
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat

from shop_documents import (ASSET, MAX_IMAGE, MAX_PDF, DocumentError,
                            ReleasedProductDocuments, document_asset)

HASH = re.compile(r'[a-f0-9]{64}')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,99}')
MAX_MANIFEST = 64*1024
MAX_TOTAL = 8*1024*1024


class DocumentOverlayError(ValueError):
    """Fixed diagnostic codes, without private file names or source URLs."""


def _need(ok, code):
    if not ok:
        raise DocumentOverlayError(code)


def _read(path, maximum):
    try:
        _need(path.is_absolute() and '..' not in path.parts
              and not any(parent.is_symlink() for parent in (path, *path.parents)), 'document_cache_path_invalid')
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            info = os.fstat(fd)
            _need(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= maximum, 'document_cache_size_invalid')
            with os.fdopen(fd, 'rb') as source:
                fd = None
                raw = source.read(maximum+1)
        finally:
            if fd is not None:
                os.close(fd)
        _need(0 < len(raw) <= maximum, 'document_cache_size_invalid')
        return raw
    except DocumentOverlayError:
        raise
    except (OSError, ValueError, TypeError):
        raise DocumentOverlayError('document_cache_file_unavailable') from None


def _decode(raw):
    def unique(items):
        result = {}
        for key, value in items:
            _need(key not in result, 'document_cache_duplicate_key')
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=unique,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except DocumentOverlayError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise DocumentOverlayError('document_cache_json_invalid') from None


def local_product_document_resources(root, record, bundle, *, base_bundle_sha256, overlay_sha256):
    """Add only file/content-SHA bindings to 29 existing approved originals.

    No descriptor record means exactly the old resources, with no file reads.
    A configured record is fail-closed: every pinned original must be present,
    byte-identical and structurally valid before any local bindings are returned.
    Other resources, source URLs, public routes and catalogue data stay intact.
    """
    documents = bundle.get('documents') if isinstance(bundle, Mapping) else None
    resources = documents.get('resources') if isinstance(documents, Mapping) else None
    if record is None:
        return resources
    _need(isinstance(record, Mapping) and set(record) == {'path','sha256','base_bundle_sha256'}
          and isinstance(record['path'], str) and NAME.fullmatch(record['path']) is not None
          and isinstance(record['sha256'], str) and HASH.fullmatch(record['sha256']) is not None
          and isinstance(base_bundle_sha256, str) and HASH.fullmatch(base_bundle_sha256) is not None
          and record['base_bundle_sha256'] == base_bundle_sha256
          and isinstance(overlay_sha256, str) and HASH.fullmatch(overlay_sha256) is not None,
          'document_cache_descriptor_invalid')
    _need(isinstance(resources, Mapping) and isinstance(documents.get('approved_assets'), list)
          and all(isinstance(asset,str) and ASSET.fullmatch(asset) for asset in documents['approved_assets']),
          'document_cache_bundle_invalid')
    root = Path(root)
    _need(root.is_absolute() and root.is_dir() and root.resolve() == root, 'document_cache_root_invalid')
    raw = _read(root/record['path'], MAX_MANIFEST)
    _need(sha256(raw).hexdigest() == record['sha256'], 'document_cache_manifest_hash_mismatch')
    manifest = _decode(raw)
    _need(isinstance(manifest, Mapping) and set(manifest) == {'base_bundle_sha256','overlay_sha256','counts','records'}
          and manifest['base_bundle_sha256'] == base_bundle_sha256
          and manifest['overlay_sha256'] == overlay_sha256
          and isinstance(manifest['counts'], Mapping) and isinstance(manifest['records'], Mapping)
          and len(manifest['records']) == 29 and manifest['counts'].get('assets') == 29,
          'document_cache_manifest_invalid')
    docroot = root/'documents'
    _need(docroot.is_dir() and not docroot.is_symlink() and docroot.resolve().parent == root, 'document_cache_root_invalid')
    approved = set(documents['approved_assets'])
    result, total = deepcopy(resources), 0
    for asset, entry in manifest['records'].items():
        _need(isinstance(asset, str) and ASSET.fullmatch(asset) is not None and asset in approved
              and isinstance(entry, Mapping) and set(entry) == {'name','sha256','mime','size'}
              and entry['name'] == asset and isinstance(entry['sha256'], str) and HASH.fullmatch(entry['sha256']) is not None
              and type(entry['size']) is int and 0 < entry['size'] <= (MAX_PDF if asset.endswith('.pdf') else MAX_IMAGE),
              'document_cache_record_invalid')
        total += entry['size']
        _need(total <= MAX_TOTAL, 'document_cache_total_limit')
        source = resources.get(asset)
        _need(isinstance(source, Mapping) and isinstance(source.get('url'), str), 'document_cache_resource_invalid')
        try:
            _need(document_asset(source['url']) == asset, 'document_cache_resource_invalid')
        except DocumentError:
            raise DocumentOverlayError('document_cache_resource_invalid') from None
        _need(('file' not in source or source['file'] == asset)
              and ('sha256' not in source or source['sha256'] == entry['sha256']), 'document_cache_resource_conflict')
        data = _read(docroot/asset, MAX_PDF if asset.endswith('.pdf') else MAX_IMAGE)
        _need(len(data) == entry['size'] and sha256(data).hexdigest() == entry['sha256'], 'document_cache_content_hash_mismatch')
        try:
            _, mime = ReleasedProductDocuments._validate(data, asset)
            if source.get('tyre_identity') is not None:
                ReleasedProductDocuments._validate_tyre_identity(data, source['tyre_identity'])
        except DocumentError:
            raise DocumentOverlayError('document_cache_content_invalid') from None
        _need(entry['mime'] == mime, 'document_cache_mime_mismatch')
        result[asset] = {**source, 'file':asset, 'sha256':entry['sha256']}
    _need(type(manifest['counts'].get('bytes')) is int and manifest['counts']['bytes'] == total,
          'document_cache_total_mismatch')
    return result
