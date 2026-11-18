"""Original-byte display prices, no supplier capability, DB or network access."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
import shop_parts_inquiry as inquiry
from shop_parts_inquiry import PartsInquiryError, load_parts_inquiry
from shop_catalog_public import PublicCatalog, variant_fingerprint
import test_shop_parts_release as source_fixture
import test_reconcile_rsu_parts as products_fixture


class InquiryPriceChecks(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket, 'socket', side_effect=AssertionError('No network'))
        guard.start(); self.addCleanup(guard.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.base = 'a' * 64
        self.product = products_fixture.row(); self.pid = self.product['selection_id']
        self.bundle = {'products': [self.product], 'publication': {'released_ids': [self.pid]}, 'rsu_bindings': {}}
        _, source = source_fixture.install_parts_fixture(self.root, self.bundle, self.base)
        seed = source['rsu_price_seeds']['records'][self.pid]
        self.manifest = {'schema_version': 1, 'base_bundle_sha256': self.base,
            'identity_proofs': deepcopy(source['rsu_bindings']),
            'display_prices': {self.pid: {k: seed[k] for k in ('identity_hash', 'unit_gross_cent', 'observed_at', 'source')}},
            'source_refs': source['source_refs']}
        self.record = self.save(self.manifest)

    def save(self, manifest):
        raw = source_fixture.encode(manifest); (self.root / 'inquiry.json').write_bytes(raw)
        return {'path': 'inquiry.json', 'sha256': sha256(raw).hexdigest(), 'base_bundle_sha256': self.base}

    def load(self, record=None, *, allowed=None):
        return load_parts_inquiry(self.root, self.record if record is None else record, self.bundle,
            base_bundle_sha256=self.base, allowed_ids={self.pid} if allowed is None else allowed)

    def test_absent_record_does_not_read_sources_or_mutate_bundle(self):
        with patch.object(inquiry, 'read', side_effect=AssertionError('No read')):
            prices = load_parts_inquiry(self.root, None, self.bundle, base_bundle_sha256=self.base, allowed_ids=set())
        self.assertEqual(prices.ids, frozenset()); self.assertIsNone(prices.get(self.pid))

    def test_exact_originals_produce_only_fixed_gross_display_price_with_historical_stock(self):
        before = deepcopy(self.bundle); prices = self.load(); value = prices.get(self.pid)
        self.assertEqual(self.bundle, before)
        self.assertEqual(set(value), {'identity_hash', 'price'})
        self.assertEqual(value['identity_hash'], variant_fingerprint(self.product))
        price = value['price']; self.assertEqual(price['kind'], 'merchant_pricebook')
        self.assertEqual(price['source'], 'RSU'); self.assertEqual(price['currency'], 'EUR')
        self.assertEqual(price['vat_percent'], '19.00'); self.assertFalse(price['stock_current'])
        self.assertEqual(price['stock_reported'], 2)
        self.assertEqual(set(price['prices_by_quantity']), {str(q) for q in range(1, 21)})
        self.assertEqual(price['prices_by_quantity']['1']['unit_gross_cent'], 1369)
        self.assertEqual(price['prices_by_quantity']['20']['items_gross_cent'], 27380)
        original = datetime.fromisoformat(self.manifest['display_prices'][self.pid]['observed_at'])
        self.assertEqual(datetime.fromisoformat(price['observed_at']), original)
        self.assertEqual(datetime.fromisoformat(price['review_at']), original + timedelta(days=5))
        self.assertEqual(datetime.fromisoformat(price['expires_at']), original + timedelta(days=7))
        self.assertEqual(datetime.fromisoformat(price['stock_expires_at']), original + timedelta(minutes=15))
        for secret in ('supplier_article_id', 'unit_net', 'mapping', 'source_ref', 'procurement_confirmed', 'order_enabled'):
            self.assertNotIn(secret, json.dumps(value))

    def test_get_has_no_io_callback_or_timestamp_refresh_and_returns_independent_copies(self):
        prices = self.load(); original = prices.get(self.pid)
        with patch.object(inquiry, 'read', side_effect=AssertionError('No file read')):
            first = prices.get(self.pid); first['price']['prices_by_quantity']['1']['unit_gross_cent'] = 1
            self.assertEqual(prices.get(self.pid), original)
            self.assertIsNone(prices.get('parts:' + 'b' * 24)); self.assertIsNone(prices.get([]))

    def test_regular_public_catalogue_hides_expired_prices_without_loader_retiming(self):
        prices = self.load(); original = prices.get(self.pid)
        observed = datetime.fromisoformat(original['price']['observed_at'])
        catalogue = PublicCatalog([self.product], released_ids=[self.pid], allow_published_prices=True,
            quote_lookup=lambda pid: prices.get(pid)['price'], quote_lookup_authoritative=True,
            clock=lambda: observed + timedelta(days=8))
        self.assertIsNone(catalogue.get(self.pid)['price'])
        self.assertEqual(prices.get(self.pid), original)

    def test_base_hash_manifest_hash_and_caller_scope_are_required(self):
        for change in ({'sha256': 'f' * 64}, {'base_bundle_sha256': 'f' * 64}, {'path': '../inquiry.json'}):
            with self.assertRaises(PartsInquiryError): self.load({**self.record, **change})
        with self.assertRaises(PartsInquiryError): self.load(allowed=set())
        self.bundle['rsu_bindings'][self.pid] = self.manifest['identity_proofs'][self.pid]
        with self.assertRaisesRegex(PartsInquiryError, 'procurement_scope_conflict'): self.load()

    def test_no_procurement_freight_or_runtime_enable_fields_are_admitted(self):
        for key, value in [('rsu_bindings', {}), ('shipping_classes', {}), ('order_enabled', True), ('mode', 'live')]:
            manifest = {**deepcopy(self.manifest), key: value}
            with self.subTest(key=key), self.assertRaises(PartsInquiryError): self.load(self.save(manifest))
        for key, value in [('stock_reported', 999), ('procurement_confirmed', True), ('source', 'ALZURA')]:
            manifest = deepcopy(self.manifest); manifest['display_prices'][self.pid][key] = value
            with self.subTest(key=key), self.assertRaises(PartsInquiryError): self.load(self.save(manifest))

    def test_exact_markup_cannot_be_higher_lower_market_or_boolean(self):
        for amount in (1370, 1190, 999999, True):
            manifest = deepcopy(self.manifest); manifest['display_prices'][self.pid]['unit_gross_cent'] = amount
            with self.subTest(amount=amount), self.assertRaises(PartsInquiryError): self.load(self.save(manifest))

    def test_unchanged_price_cannot_be_given_a_new_observation_timestamp(self):
        manifest = deepcopy(self.manifest)
        observed = datetime.fromisoformat(manifest['display_prices'][self.pid]['observed_at'])
        manifest['display_prices'][self.pid]['observed_at'] = (observed + timedelta(seconds=1)).isoformat()
        with self.assertRaises(PartsInquiryError): self.load(self.save(manifest))

    def test_original_dom_get_and_normalized_row_must_correlate(self):
        for name in ('supplier.dom.txt', 'rest.json', 'normalized.json'):
            path = self.root / 'parts-sources' / name; original = path.read_bytes(); path.write_bytes(b'{}')
            with self.subTest(name=name), self.assertRaises(PartsInquiryError): self.load()
            path.write_bytes(original)
        # Re-pinning a fake normalized price still cannot authenticate it.
        _, changed = source_fixture.install_parts_fixture(self.root, self.bundle, self.base, normalized_changes={'unit_net': '11'})
        manifest = deepcopy(self.manifest); manifest['identity_proofs'] = changed['rsu_bindings']; manifest['source_refs'] = changed['source_refs']
        with self.assertRaises(PartsInquiryError): self.load(self.save(manifest))

    def test_wrong_catalogue_fingerprint_ean_and_nonparts_department_cannot_borrow_price(self):
        for mutate in (lambda p: p.update(name='Different exact part'),
                       lambda p: p['details'].update(EAN='5901234123457'),
                       lambda p: p.update(department='accessories')):
            before = deepcopy(self.product); mutate(self.product)
            with self.assertRaises(PartsInquiryError): self.load()
            self.product.clear(); self.product.update(before)


if __name__ == '__main__': unittest.main(verbosity=2)
