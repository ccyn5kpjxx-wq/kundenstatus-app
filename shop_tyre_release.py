"""Add the reviewed 23886 tyre to its exact catalogue, using ordinary RSU checkout."""
from hashlib import sha256
import json
from pathlib import Path

from shop_catalog_public import variant_fingerprint
from shop_documents import ReleasedProductDocuments
from shop_media import ReleasedMedia


ASSETS = Path(__file__).resolve().parent / 'resources' / 'shop_bridgestone_23886'
PRODUCT_ID = 'tyre:16f79c76e6956f4870cd2815'
PRIVATE_FILE = 'shop-tyre-16f79c76e6956f4870cd2815.private.json'
PRIVATE_SHA256 = '0728604a5783b22626ae9ea9e5289302b971cb7505b6758756b02b705bd09ee4'


class SupplementAssets:
    """Serve approved packaged originals at the same URLs as disk-backed originals."""
    def __init__(self, base, packaged):
        self.base = base
        self.packaged = packaged

    def read(self, asset):
        service = self.packaged if asset in self.packaged.resources else self.base
        return service.read(asset)


def add_reviewed_tyre(bundle, base_digest, release_root, *, secret_root=Path('/etc/secrets')):
    """Return the base untouched unless both the reviewed catalogue and receipt are installed."""
    release = json.loads((ASSETS / 'release.json').read_bytes())
    if base_digest != release['base_bundle_sha256']:
        return bundle, None
    # A Render secret mount may project its individual file through a symlink.
    # Only the pinned bytes of this named, bounded receipt are accepted.
    path = release_root / PRIVATE_FILE
    if path.is_symlink():
        raise ValueError('invalid_tyre_release_path')
    if not path.exists():
        path = secret_root / PRIVATE_FILE
        if not path.is_file():
            return bundle, None
    if not path.is_file() or not 0 < path.stat().st_size <= 16_384:
        raise ValueError('invalid_tyre_release_receipt')
    raw = path.read_bytes()
    if sha256(raw).hexdigest() != PRIVATE_SHA256:
        raise ValueError('invalid_tyre_release_receipt')
    private = json.loads(raw)
    product, identity = release['product'], release['identity_hash']
    binding, seed = private['binding'], private['seed']
    docs = release['product_documents']
    if not (product['selection_id'] == PRODUCT_ID and product['category'] == binding['category'] == 'tyre'
            and product['quality_facts']['vehicle_class'] == 'C1'
            and product['identifiers']['ean'] == binding['expected_ean']
            and identity == variant_fingerprint(product) == binding['identity_hash'] == seed['identity_hash'] == docs['identity_hash']
            and PRODUCT_ID not in bundle['publication']['released_ids']
            and PRODUCT_ID not in bundle['rsu_bindings'] and PRODUCT_ID not in bundle['rsu_price_seeds']['records']):
        raise ValueError('invalid_tyre_release_scope')
    images = release['media_resources']
    documents = release['document_resources']
    media = ReleasedMedia(images, approved_assets=set(images), local_root=ASSETS / 'media')
    sheets = ReleasedProductDocuments(documents, approved_assets=set(documents), enabled=True, local_root=ASSETS / 'documents')
    # Mandatory originals must work offline before granting the normal tyre release.
    for service in (media, sheets):
        for asset in service.resources:
            service.read(asset)
    image_urls = {'bilder/' + asset: '/shop/media/' + asset for asset in images}
    doc_urls = {record['url']: '/shop/documents/' + asset for asset, record in documents.items()}
    if (not all(photo in image_urls for photo in product['gallery'])
            or not all(docs[key] in doc_urls.values() for key in ('tyre_label_url', 'tyre_sheet_url'))):
        raise ValueError('invalid_tyre_release_assets')
    publication = bundle['publication']
    merged = {**bundle,
        'products': [*bundle['products'], product],
        'publication': {**publication, 'released_ids': [*publication['released_ids'], PRODUCT_ID],
                        'image_urls': {**publication['image_urls'], **image_urls},
                        'document_urls': {**publication['document_urls'], **doc_urls}},
        'browsing_evidence': {**bundle.get('browsing_evidence', {}), PRODUCT_ID: {
            'variant_fingerprint': identity, 'tyre_variant_label': 'DriveGuard Runflat (RFT) · XL',
            'gallery': [image_urls[photo] for photo in product['gallery']]}},
        'product_documents': {**bundle.get('product_documents', {}), PRODUCT_ID: docs},
        'rsu_bindings': {**bundle['rsu_bindings'], PRODUCT_ID: binding},
        'shipping_classes': {**bundle['shipping_classes'], PRODUCT_ID: 'pkw_tyre'}}
    return merged, {'seed': seed, 'media': media, 'documents': sheets}
