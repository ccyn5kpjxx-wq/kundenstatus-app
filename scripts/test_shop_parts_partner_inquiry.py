"""Pinned licensed original prices; no API, database or ordering capability."""
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
sys.path.insert(0, str(ROOT))
import shop_parts_partner_inquiry as partner
from shop_parts_partner_inquiry import (PartsPartnerInquiryError, load_parts_partner_prices,
    normalize_partner_inquiry_price)
from shop_catalog_public import variant_fingerprint
from shop_partner_native.shop_parts_provider import normalize_wearparts


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


class PartnerInquiryChecks(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket, 'socket', side_effect=AssertionError('No network'))
        guard.start(); self.addCleanup(guard.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); (self.root / 'partner-price-sources').mkdir()
        self.base = 'a' * 64
        self.received = datetime.now(timezone.utc) - timedelta(seconds=2)
        self.requested = self.received - timedelta(seconds=2)
        self.raw_row = {'assignedArticle': {'articleId': '112-56543534302d4c', 'brandNo': 112,
            'articleName': 'Kotflügel', 'articleNo': 'VT540-L', 'brandName': 'EIBACH',
            'flagRemanufacturedPart': False, 'quantityPerPackingUnit': 0, 'packingUnit': 1},
            'assignedTyre24Data': {'wholesaler_price': '100.00', 'price': '200.00', 'wholesaler_quantity': 20,
                'depositPrice': None, 'vat': 0.19, 'manufacturer_number': 'VT540-L', 'manufacturer_name': 'EIBACH'},
            'articleAttributes': {'array': []}, 'eanNumber': {'array': [{'eanNumber': '4050278046336'}]}}
        self.request = {'area': 'wearparts', 'operation': 'articles', 'params': {'manufacturerId': 5,
            'modelId': 10253, 'carId': 120604, 'assemblyGroupNodeId': 16, 'linkingTargetType': 'P', 'showPerPage': 25, 'page': 0}}
        self.context = {'price_semantics_reviewed': True, 'price_type': 'EK', 'currency': 'EUR',
            'country': 'DE', 'price_basis': 'per_item'}
        self.saved = normalize_wearparts({'result': [self.raw_row]}, self.received, self.request, self.context)[0]
        self.pid = 'parts:' + sha256((self.saved['category'] + ':' + self.saved['provider_id']).encode()).hexdigest()[:24]
        self.product = {'selection_id': self.pid, 'category': self.saved['category'], 'department': 'parts',
            'name': self.saved['name'], 'manufacturer_name': self.saved['brand'], 'article_number': self.saved['article_number'],
            'unit_label': self.saved['unit_label'], 'details': {'EAN': self.saved['ean'],
                'Herstellerartikelnummer': self.saved['article_number']}, 'price': None, 'quote_public': None}
        self.bundle = {'products': [self.product], 'publication': {'released_ids': [self.pid]}, 'rsu_bindings': {}}
        self.display = {'identity_hash': variant_fingerprint(self.product), 'unit_gross_cent': 13685,
            'observed_at': self.received.isoformat(), 'source': 'ALZURA', 'snapshot_row_number': 1}
        self.save()

    def pin(self, name, value):
        raw = encode(value); (self.root / 'partner-price-sources' / name).write_bytes(raw)
        return {'file': name, 'sha256': sha256(raw).hexdigest()}

    def save(self):
        body = self.pin('body.json', {'result': [self.raw_row]})
        meta = self.pin('meta.json', {'ok': True, 'http_status': 200, 'area': 'wearparts', 'operation': 'articles',
            'requested_at': self.requested.isoformat(), 'received_at': self.received.isoformat(),
            'body_file': body['file'], 'body_sha256': body['sha256']})
        snapshot = self.pin('snapshot.json', {'mode': 'reviewed_alzura_parts_local', 'products': [self.saved]})
        self.manifest = {'schema_version': 1, 'base_bundle_sha256': self.base, 'snapshot': snapshot,
            'source_refs': [snapshot, body, meta], 'display_prices': {self.pid: {**self.display,
                'source_body': {**body, 'row_number': 1}, 'source_meta': meta}}}
        return self.save_manifest(self.manifest)

    def save_manifest(self, manifest):
        raw = encode(manifest); (self.root / 'partner-prices.json').write_bytes(raw)
        self.record = {'path': 'partner-prices.json', 'sha256': sha256(raw).hexdigest(), 'base_bundle_sha256': self.base}
        return self.record

    def load(self, *, record=None, allowed=None):
        return load_parts_partner_prices(self.root, self.record if record is None else record, self.bundle,
            base_bundle_sha256=self.base, allowed_ids={self.pid} if allowed is None else allowed)

    def test_absent_manifest_is_inert_and_needs_no_file_or_provider(self):
        with patch.object(partner, 'read', side_effect=AssertionError('No read')):
            result = load_parts_partner_prices(self.root, None, self.bundle,
                base_bundle_sha256=self.base, allowed_ids=set())
        self.assertEqual(result.ids, frozenset())

    def test_original_wholesaler_net_is_marked_up_once_and_true_provider_retained(self):
        before = deepcopy(self.bundle); source = self.load(); value = source.get(self.pid)
        self.assertEqual(before, self.bundle); self.assertEqual(source.ids, frozenset({self.pid}))
        self.assertEqual(set(value), {'identity_hash', 'price'})
        price = value['price']; self.assertEqual(price['kind'], 'partner_inquiry_pricebook')
        self.assertEqual(price['source'], 'ALZURA'); self.assertIs(price['stock_current'], False)
        self.assertEqual(price['prices_by_quantity']['1']['unit_gross_cent'], 13685)  # raw price200 is not EK100
        self.assertEqual(price['prices_by_quantity']['20']['items_gross_cent'], 273700)
        self.assertEqual(datetime.fromisoformat(price['observed_at']), self.received)
        self.assertEqual(datetime.fromisoformat(price['review_at']), self.received + timedelta(days=5))
        self.assertEqual(datetime.fromisoformat(price['expires_at']), self.received + timedelta(days=7))
        self.assertEqual(normalize_partner_inquiry_price(price), price)  # second bootstrap validation is stable
        for private in ('wholesaler', 'unit_net', 'supplier_article_id', 'mapping', 'source_body', 'provider_id'):
            self.assertNotIn(private, json.dumps(value))

    def test_original_request_timestamp_is_supported_without_retiming(self):
        self.saved['observed_at'] = self.display['observed_at'] = self.requested.isoformat(); self.save()
        self.assertEqual(self.load().get(self.pid)['price']['observed_at'], self.requested.isoformat())
        self.display['observed_at'] = (self.received + timedelta(seconds=1)).isoformat(); self.save()
        with self.assertRaises(PartsPartnerInquiryError): self.load()

    def test_retrieval_has_no_io_and_cannot_mutate_or_retime_frozen_values(self):
        source = self.load(); original = source.get(self.pid)
        with patch.object(partner, 'read', side_effect=AssertionError('No read')):
            value = source.get(self.pid); value['price']['source'] = 'RSU'
            self.assertEqual(source.get(self.pid), original); self.assertIsNone(source.get([]))

    def test_missing_ean_needs_the_same_exact_native_part_and_packing_not_a_fuzzy_match(self):
        self.raw_row['eanNumber']['array'] = []
        self.saved['ean'] = ''; self.product['details']['EAN'] = ''
        self.display['identity_hash'] = variant_fingerprint(self.product); self.save()
        self.assertEqual(self.load().get(self.pid)['price']['source'], 'ALZURA')
        self.saved['unit_label'] = 'Packung (2 Stück)'; self.product['unit_label'] = self.saved['unit_label']
        self.display['identity_hash'] = variant_fingerprint(self.product); self.save()
        with self.assertRaises(PartsPartnerInquiryError): self.load()

    def test_wrong_base_hash_caller_scope_and_procurement_scope_are_rejected(self):
        for change in ({'sha256': 'f' * 64}, {'base_bundle_sha256': 'f' * 64}, {'path': '../partner-prices.json'}):
            with self.subTest(change=change), self.assertRaises(PartsPartnerInquiryError):
                self.load(record={**self.record, **change})
        with self.assertRaises(PartsPartnerInquiryError): self.load(allowed=set())
        self.bundle['rsu_bindings'][self.pid] = {'no': 'price-only binding'}
        with self.assertRaises(PartsPartnerInquiryError): self.load()

    def test_rehashed_snapshot_claims_cannot_replace_original_net_or_identity(self):
        for key, value in [('unit_net_eur', '101.00'), ('ean', '5901234123457'), ('unit_label', 'Packung (2 Stück)')]:
            before = deepcopy(self.saved); self.saved[key] = value; self.save()
            with self.subTest(key=key), self.assertRaises(PartsPartnerInquiryError): self.load()
            self.saved = before
        self.raw_row['assignedTyre24Data'].pop('wholesaler_price'); self.save()
        with self.assertRaises(PartsPartnerInquiryError): self.load()  # bare price or UVP cannot substitute

    def test_untyped_currency_deposit_and_source_time_mismatch_never_fall_back(self):
        for key, value in [('currency', 'USD'), ('price_type', 'UVP'), ('price_semantics_reviewed', 1)]:
            before = deepcopy(self.saved); self.saved['source_context'][key] = value; self.save()
            with self.subTest(key=key), self.assertRaises(PartsPartnerInquiryError): self.load()
            self.saved = before
        self.raw_row['assignedTyre24Data']['depositPrice'] = '5.00'; self.save()
        with self.assertRaises(PartsPartnerInquiryError): self.load()

    def test_markup_cannot_be_absent_doubled_or_a_boolean(self):
        for gross in (11900, 15738, 13686, True):
            self.display['unit_gross_cent'] = gross; self.save()
            with self.subTest(gross=gross), self.assertRaises(PartsPartnerInquiryError): self.load()

    def test_whole_source_hash_row_number_and_display_only_schema_are_enforced(self):
        path = self.root / 'partner-price-sources' / 'body.json'; original = path.read_bytes(); path.write_bytes(b'{}')
        with self.assertRaises(PartsPartnerInquiryError): self.load()
        path.write_bytes(original)
        for key, value in [('shipping_classes', {}), ('rsu_bindings', {}), ('order_enabled', True)]:
            manifest = deepcopy(self.manifest); manifest[key] = value; self.save_manifest(manifest)
            with self.subTest(key=key), self.assertRaises(PartsPartnerInquiryError): self.load()
        manifest = deepcopy(self.manifest); manifest['display_prices'][self.pid]['source_body']['row_number'] = True
        self.save_manifest(manifest)
        with self.assertRaises(PartsPartnerInquiryError): self.load()

    def test_unreleased_or_renamed_part_cannot_borrow_original_net(self):
        self.bundle['publication']['released_ids'] = []
        with self.assertRaises(PartsPartnerInquiryError): self.load()
        self.bundle['publication']['released_ids'] = [self.pid]
        self.product['name'] = 'Different actual part'
        self.display['identity_hash'] = variant_fingerprint(self.product); self.save()
        with self.assertRaises(PartsPartnerInquiryError): self.load()

    def test_public_normalizer_only_accepts_explicit_alzura_display_type_and_uniform_typed_prices(self):
        good = self.load().get(self.pid)['price']
        for key, value in [('source', 'RSU'), ('kind', 'merchant_pricebook'), ('stock_current', True), ('currency', 'USD')]:
            changed = {**deepcopy(good), key: value}
            with self.subTest(key=key), self.assertRaises(PartsPartnerInquiryError): normalize_partner_inquiry_price(changed)
        for mutate in (lambda p: p['prices_by_quantity'].pop('20'),
                lambda p: p['prices_by_quantity']['2'].update(unit_gross_cent=10000, items_gross_cent=20000),
                lambda p: p.update(procurement_confirmed=True),
                lambda p: p.update(expires_at=(self.received + timedelta(days=8)).isoformat())):
            changed = deepcopy(good); mutate(changed)
            with self.assertRaises(PartsPartnerInquiryError): normalize_partner_inquiry_price(changed)


if __name__ == '__main__': unittest.main(verbosity=2)
