import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_price_book import PublishedPriceBook, PRICE_TTL
from shop_tyre_release import REVIEWED_TYRES, add_reviewed_tyre


class SupplementTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.digest = json.loads((REVIEWED_TYRES[0].assets / 'release.json').read_bytes())['base_bundle_sha256']
        self.base = {'products': [], 'publication': {'released_ids': [], 'image_urls': {}, 'document_urls': {}},
                     'rsu_bindings': {}, 'shipping_classes': {}, 'rsu_price_seeds': {'records': {}}}

    def test_exact_pinned_scope_or_no_release(self):
        for tyre in REVIEWED_TYRES:
            for digest in (self.digest, 'f'*64):
                original, added = add_reviewed_tyre(self.base, digest, self.root, tyre, secret_root=self.root)
                self.assertIs(original, self.base)
                self.assertIsNone(added)
            (self.root / tyre.receipt_filename).write_bytes(b'{}')
            with self.assertRaisesRegex(ValueError, 'invalid_tyre_release_receipt'):
                add_reviewed_tyre(self.base, self.digest, self.root, tyre)

    def test_normal_catalogue_search_images_documents_prices_and_expiry(self):
        now = datetime(2026, 11, 27, 18, 55, tzinfo=timezone.utc)
        mounted = self.root / 'mounted'
        mounted.mkdir()
        before = copy.deepcopy(self.base)
        data, extras, identities = self.base, {}, {}
        bridgestone, michelin = REVIEWED_TYRES
        for tyre in REVIEWED_TYRES:
            release = json.loads((tyre.assets / 'release.json').read_bytes())
            identity = release['identity_hash']
            identities[tyre.product_id] = {'id': tyre.product_id, 'identity_hash': identity, 'category': 'tyre'}
            seed = {'identity_hash': identity, 'source': 'RSU', 'observed_at': now.isoformat(),
                    'stock_reported': 20, 'unit_gross_cent': 15441 if tyre == bridgestone else 27100}
            receipt = json.dumps({'binding': {'category': 'tyre', 'expected_ean': release['product']['details']['EAN'],
                                              'identity_hash': identity}, 'seed': seed}).encode()
            (mounted / tyre.receipt_filename).write_bytes(receipt)
            spec = replace(tyre, receipt_sha256=sha256(receipt).hexdigest())
            data, extras[tyre.product_id] = add_reviewed_tyre(data, self.digest, self.root, spec, secret_root=mounted)
        self.assertEqual(self.base, before)
        self.assertEqual(data['shipping_classes'], dict.fromkeys(identities, 'pkw_tyre'))
        book = PublishedPriceBook(self.root / 'prices' / 'prices.sqlite3', products=identities,
                                  approved_ids=set(identities), clock=lambda: now).provision(create=True)
        book.seed({pid: extra['seed'] for pid, extra in extras.items()}, evidence_id='a'*64)
        catalog = EnrichedPublicCatalog(data['products'], **data['publication'],
            browsing_evidence=data['browsing_evidence'], product_documents=data['product_documents'],
            procurement_ids=set(data['rsu_bindings']), rsu_only_prices=True, quote_lookup=book.get,
            quote_lookup_authoritative=True, allow_published_prices=True, tyre_payment_costs=True, clock=lambda: now)
        cases = [(bridgestone, ('3286342388618', 'Bridgestone DriveGuard', 'RFT XL'), '225', '50', '17', 'allseason', 'Ja', 15737),
                 (michelin, ('3528706804168', 'Michelin Pilot Alpin 5', 'FSL XL'), '235', '35', '19', 'winter', 'Nein', 23810)]
        for tyre, queries, width, ratio, diameter, season, runflat, unit in cases:
            for query in queries:
                result = catalog.page(q=query, department='tyres', width=width, ratio=ratio, diameter=diameter, season=season)
                self.assertEqual([item['id'] for item in result['items']], [tyre.product_id])
            product = catalog.get(tyre.product_id)
            self.assertEqual(product['purchase_status'], 'supplier_check_required')
            self.assertEqual(product['tyre_variant_label'], tyre.variant_label)
            self.assertEqual(product['geometry']['runflat'], runflat)
            for qty in range(1, 5):
                line = product['price']['prices_by_quantity'][str(qty)]
                self.assertEqual((line['unit_gross_cent'], line['items_gross_cent']), (unit, unit * qty))
            self.assertEqual(len(product['images']), 2)
            extra = extras[tyre.product_id]
            for path in product['images']:
                self.assertEqual(extra['media'].read(path.rsplit('/', 1)[1])[1], 'image/jpeg')
            for field, mime in [('tyre_label_url', 'image/jpeg'), ('tyre_sheet_url', 'application/pdf')]:
                path = product['product_documents'][field]
                self.assertEqual(extra['documents'].read(path.rsplit('/', 1)[1])[1], mime)
        now += timedelta(days=1)
        self.assertTrue(all(book.review_due(pid) for pid in identities))
        now += PRICE_TTL
        self.assertTrue(all(catalog.get(pid)['price'] is None for pid in identities))


if __name__ == '__main__':
    unittest.main()
