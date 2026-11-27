import copy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_price_book import PublishedPriceBook, PRICE_TTL
from shop_tyre_release import ASSETS, PRIVATE_FILE, PRODUCT_ID, add_reviewed_tyre


class SupplementTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.release = json.loads((ASSETS / 'release.json').read_bytes())
        self.digest = self.release['base_bundle_sha256']
        self.base = {'products': [], 'publication': {'released_ids': [], 'image_urls': {}, 'document_urls': {}},
                     'rsu_bindings': {}, 'shipping_classes': {}, 'rsu_price_seeds': {'records': {}}}

    def test_exact_pinned_scope_or_no_release(self):
        for digest in (self.digest, 'f'*64):
            original, added = add_reviewed_tyre(self.base, digest, self.root, secret_root=self.root)
            self.assertIs(original, self.base)
            self.assertIsNone(added)
        (self.root / PRIVATE_FILE).write_bytes(b'{}')
        with self.assertRaisesRegex(ValueError, 'invalid_tyre_release_receipt'):
            add_reviewed_tyre(self.base, self.digest, self.root)

    def test_normal_catalogue_search_images_documents_prices_and_expiry(self):
        now = datetime(2026, 11, 27, 18, 55, tzinfo=timezone.utc)
        identity = self.release['identity_hash']
        # Synthetic private fixture: the example retail base already includes material markup and VAT.
        seed = {'identity_hash': identity, 'source': 'RSU', 'observed_at': now.isoformat(),
                'stock_reported': 20, 'unit_gross_cent': 15441}
        receipt = json.dumps({'binding': {'category': 'tyre', 'expected_ean': '3286342388618',
                                          'identity_hash': identity}, 'seed': seed}).encode()
        mounted = self.root / 'mounted'
        mounted.mkdir()
        (mounted / PRIVATE_FILE).write_bytes(receipt)
        before = copy.deepcopy(self.base)
        with patch('shop_tyre_release.PRIVATE_SHA256', sha256(receipt).hexdigest()):
            data, extra = add_reviewed_tyre(self.base, self.digest, self.root, secret_root=mounted)
        self.assertEqual(self.base, before)
        self.assertEqual(data['shipping_classes'], {PRODUCT_ID: 'pkw_tyre'})
        book = PublishedPriceBook(self.root / 'prices' / 'prices.sqlite3', products={PRODUCT_ID: {
            'id': PRODUCT_ID, 'identity_hash': identity, 'category': 'tyre'}}, approved_ids={PRODUCT_ID},
            clock=lambda: now).provision(create=True)
        book.seed({PRODUCT_ID: extra['seed']}, evidence_id='a'*64)
        catalog = EnrichedPublicCatalog(data['products'], **data['publication'],
            browsing_evidence=data['browsing_evidence'], product_documents=data['product_documents'],
            procurement_ids=set(data['rsu_bindings']), rsu_only_prices=True, quote_lookup=book.get,
            quote_lookup_authoritative=True, allow_published_prices=True, tyre_payment_costs=True, clock=lambda: now)
        for query in ('3286342388618', 'Bridgestone DriveGuard', 'RFT XL'):
            result = catalog.page(q=query, department='tyres', width='225', ratio='50', diameter='17', season='allseason')
            self.assertEqual([item['id'] for item in result['items']], [PRODUCT_ID])
        product = catalog.get(PRODUCT_ID)
        self.assertEqual(product['purchase_status'], 'supplier_check_required')
        self.assertEqual(product['tyre_variant_label'], 'DriveGuard Runflat (RFT) · XL')
        self.assertEqual(product['geometry']['runflat'], 'Ja')
        self.assertEqual(product['price']['prices_by_quantity']['1']['unit_gross_cent'], 15737)
        self.assertEqual(product['price']['prices_by_quantity']['4']['items_gross_cent'], 62948)
        self.assertEqual(len(product['images']), 2)
        for path in product['images']:
            self.assertEqual(extra['media'].read(path.rsplit('/', 1)[1])[1], 'image/jpeg')
        for field, mime in [('tyre_label_url', 'image/jpeg'), ('tyre_sheet_url', 'application/pdf')]:
            path = product['product_documents'][field]
            self.assertEqual(extra['documents'].read(path.rsplit('/', 1)[1])[1], mime)
        now += timedelta(days=1)
        self.assertTrue(book.review_due(PRODUCT_ID))
        now += PRICE_TTL
        self.assertIsNone(catalog.get(PRODUCT_ID)['price'])


if __name__ == '__main__':
    unittest.main()
