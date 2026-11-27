"""Add individually reviewed tyres to their exact catalogue, using ordinary RSU checkout."""
from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path

from shop_catalog_public import variant_fingerprint
from shop_documents import ReleasedProductDocuments
from shop_media import ReleasedMedia


RESOURCES = Path(__file__).resolve().parent / 'resources'


@dataclass(frozen=True)
class TyreRelease:
    product_id: str
    assets: Path
    receipt_sha256: str
    variant_label: str

    @property
    def receipt_filename(self):
        return 'shop-tyre-' + self.product_id.removeprefix('tyre:') + '.private.json'


REVIEWED_TYRES = (
    TyreRelease('tyre:16f79c76e6956f4870cd2815', RESOURCES / 'shop_bridgestone_23886',
                '0728604a5783b22626ae9ea9e5289302b971cb7505b6758756b02b705bd09ee4', 'DriveGuard Runflat (RFT) · XL'),
    TyreRelease('tyre:391657d07bef9198f20aa091', RESOURCES / 'shop_michelin_680416',
                '1992daf16c1f73e65d71092ee0475ebf8c33e4dd56b6cc6f601e8e9f696423bc', 'XL · Felgenschutzleiste (FSL)'),
)


class SupplementAssets:
    """Serve approved packaged originals at the same URLs as disk-backed originals."""
    def __init__(self, base, packaged):
        self.base = base
        self.packaged = packaged

    def read(self, asset):
        service = self.packaged if asset in self.packaged.resources else self.base
        return service.read(asset)


def add_reviewed_tyre(bundle, base_digest, release_root, item, *, secret_root=Path('/etc/secrets')):
    """Return the base untouched unless both the reviewed catalogue and receipt are installed."""
    release = json.loads((item.assets / 'release.json').read_bytes())
    if base_digest != release['base_bundle_sha256']:
        return bundle, None
    # A Render secret mount may project its individual file through a symlink.
    # Only the pinned bytes of this named, bounded receipt are accepted.
    path = release_root / item.receipt_filename
    if path.is_symlink():
        raise ValueError('invalid_tyre_release_path')
    if not path.exists():
        path = secret_root / item.receipt_filename
        if not path.is_file():
            return bundle, None
    if not path.is_file() or not 0 < path.stat().st_size <= 16_384:
        raise ValueError('invalid_tyre_release_receipt')
    raw = path.read_bytes()
    if sha256(raw).hexdigest() != item.receipt_sha256:
        raise ValueError('invalid_tyre_release_receipt')
    private = json.loads(raw)
    product, identity = release['product'], release['identity_hash']
    binding, seed = private['binding'], private['seed']
    docs = release['product_documents']
    product_id = item.product_id
    if not (product['selection_id'] == product_id and product['category'] == binding['category'] == 'tyre'
            and product['quality_facts']['vehicle_class'] == 'C1'
            and product['identifiers']['ean'] == binding['expected_ean']
            and identity == variant_fingerprint(product) == binding['identity_hash'] == seed['identity_hash'] == docs['identity_hash']
            and product_id not in bundle['publication']['released_ids']
            and product_id not in bundle['rsu_bindings'] and product_id not in bundle['rsu_price_seeds']['records']):
        raise ValueError('invalid_tyre_release_scope')
    images = release['media_resources']
    documents = release['document_resources']
    media = ReleasedMedia(images, approved_assets=set(images), local_root=item.assets / 'media')
    sheets = ReleasedProductDocuments(documents, approved_assets=set(documents), enabled=True, local_root=item.assets / 'documents')
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
        'publication': {**publication, 'released_ids': [*publication['released_ids'], product_id],
                        'image_urls': {**publication['image_urls'], **image_urls},
                        'document_urls': {**publication['document_urls'], **doc_urls}},
        'browsing_evidence': {**bundle.get('browsing_evidence', {}), product_id: {
            'variant_fingerprint': identity, 'tyre_variant_label': item.variant_label,
            'gallery': [image_urls[photo] for photo in product['gallery']]}},
        'product_documents': {**bundle.get('product_documents', {}), product_id: docs},
        'rsu_bindings': {**bundle['rsu_bindings'], product_id: binding},
        'shipping_classes': {**bundle['shipping_classes'], product_id: 'pkw_tyre'}}
    return merged, {'seed': seed, 'media': media, 'documents': sheets}
