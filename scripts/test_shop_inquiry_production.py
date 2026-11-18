"""Real price-only production loader; no provider transport or business DB."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
import test_shop_production as production_fixture
import test_shop_parts_release as source_fixture
import test_reconcile_rsu_parts as product_fixture
import test_shop_rsu_rim_evidence as rim_fixture
from shop_catalog_public import variant_fingerprint
from shop_production import _parts_api_channel_supported
from shop_rsu_quotes import RSUQuoteProvider
from shop_runtime import register_shop_runtime
from tyresystem_api import ArticleInquiry

ORIGIN = 'https://shop.example.test'


class RimOnlyInquiry:
    def __init__(self):
        self.calls = []

    def inquire(self, article, quantity):
        # The only injected IO is a synthetic already-bound rim. Parts may
        # never reach this transport merely because a display price exists.
        if article != '42':
            raise AssertionError('Parts cannot reach the supplier transport')
        self.calls.append((article, quantity))
        return ArticleInquiry(article, rim_fixture.EAN, quantity, Decimal('100'),
            20, 'SYNTHETIC RIM', datetime.now(timezone.utc), 'live')


class InquiryProductionChecks(unittest.TestCase):
    def setUp(self):
        self.f = production_fixture.ReleaseChecks(); self.f.setUp(); self.addCleanup(self.f.doCleanups)
        self.part = product_fixture.row(); self.pid = self.part['selection_id']
        self.rim, candidate = rim_fixture.current_fixture()
        self.rid = self.rim['selection_id']; self.rim_identity = variant_fingerprint(self.rim)
        self.f.released = self.rim
        self.f.bundle['products'] = [self.rim, self.f.hidden, self.part]
        self.f.bundle['publication']['released_ids'] = [self.rid, self.pid]
        self.f.bundle['publication']['image_urls'][self.rim['photo']] = '/shop/media/' + self.f.asset
        self.binding = {'identity_hash': self.rim_identity, 'category': self.rim['category'],
            'mapping': rim_fixture._mapping(candidate['proof']), 'expected_ean': rim_fixture.EAN}
        self.f.bundle.update(rsu_bindings={self.rid: self.binding}, shipping_classes={self.rid: 'pkw_rim'},
            delivery_postcodes=['74821'], rsu_price_seeds={'schema_version': 1, 'evidence_id': 'e' * 64,
                'records': {self.rid: {'identity_hash': self.rim_identity, 'source': 'RSU',
                    'unit_gross_cent': 13685, 'stock_reported': 20,
                    'observed_at': datetime.now(timezone.utc).isoformat()}}})
        self.f.install()
        self.inquiry, self.supplier = RimOnlyInquiry(), None
        self.factory = Mock(side_effect=self.make_supplier)

    def make_supplier(self, *, config, bindings, approved_ids, policy, **kwargs):
        self.assertEqual(set(bindings), {self.rid}); self.assertEqual(approved_ids, {self.rid})
        self.supplier = RSUQuoteProvider(bindings=bindings, approved_ids=approved_ids,
            policy=policy, inquiry=self.inquiry)
        return self.supplier

    def install(self, *, fixed=False, buyers=False):
        self.f.descriptor.update(fixed_pricebook_enabled=fixed, buyer_and_quotes_enabled=buyers)
        if fixed or buyers:
            self.f.descriptor.update(rsu_reads_approved=True, initial_storage_creation=True,
                standalone_rim_sales_approved=True,
                rsu_config=production_fixture.write_json(self.f.root / 'rsu-private.json', {}))
        self.f.install()
        _, release = source_fixture.install_parts_fixture(self.f.root, self.f.bundle,
            self.f.descriptor['bundle']['sha256'])
        seed = release['rsu_price_seeds']['records'][self.pid]
        self.manifest = {'schema_version': 1, 'base_bundle_sha256': self.f.descriptor['bundle']['sha256'],
            'identity_proofs': release['rsu_bindings'],
            'display_prices': {self.pid: {key: seed[key] for key in
                ('identity_hash', 'unit_gross_cent', 'observed_at', 'source')}},
            'source_refs': [ref for ref in release['source_refs'] if ref['file'] != 'captures.json']}
        self.save()

    def save(self):
        raw = source_fixture.encode(self.manifest); (self.f.root / 'parts-inquiry.json').write_bytes(raw)
        self.f.descriptor.update(parts_inquiry_prices_approved=True, parts_inquiry_prices={
            'path': 'parts-inquiry.json', 'sha256': sha256(raw).hexdigest(),
            'base_bundle_sha256': self.f.descriptor['bundle']['sha256']})
        production_fixture.write_json(self.f.root / 'current.json', self.f.descriptor)

    def prepare(self, *, success=True):
        with patch('shop_production.RSUQuoteProvider.from_config', side_effect=self.factory):
            if success:
                result = self.f.prepare()
            else:
                with self.assertLogs(self.f.portal.app.logger, level='ERROR'):
                    result = self.f.prepare()
        self.assertIs(_parts_api_channel_supported(), False)
        self.assertIs(self.f.portal.app.config['SHOP_PARTS_DIRECT_ENABLED'], False)
        self.assertEqual(bool(result), success)
        return result

    def assert_no_provider_or_db(self):
        self.factory.assert_not_called(); self.assertEqual(self.inquiry.calls, [])
        self.assertFalse((self.f.data / 'shop-pricebook').exists())
        self.assertFalse((self.f.data / 'shop-v2').exists())

    def test_absent_display_release_preserves_legacy_and_does_not_enable_parts(self):
        dependencies = self.prepare()
        self.assertIsNone(dependencies['catalogue'].get(self.pid)['price'])
        self.assertEqual(dependencies['catalogue'].get(self.pid)['purchase_status'], 'inquiry_only')
        self.assert_no_provider_or_db()

    def test_genuine_original_price_is_visible_without_provider_or_any_pricebook(self):
        self.install(); before = deepcopy(self.f.bundle); dependencies = self.prepare()
        product = dependencies['catalogue'].get(self.pid)
        self.assertEqual(product['price']['prices_by_quantity']['1']['unit_gross_cent'], 1369)
        self.assertEqual(product['price']['prices_by_quantity']['20']['items_gross_cent'], 27380)
        self.assertIs(product['price']['stock_current'], False)
        # The original observation is still within its 15-minute window;
        # inquiry-only must nevertheless never advertise current stock.
        self.assertLess(datetime.now(timezone.utc), datetime.fromisoformat(product['price']['stock_expires_at']))
        self.assertEqual(product['price']['observed_at'], self.manifest['display_prices'][self.pid]['observed_at'])
        self.assertEqual(product['purchase_status'], 'inquiry_only'); self.assertIs(product['order_enabled'], False)
        self.assertEqual(self.f.bundle, before); self.assert_no_provider_or_db()
        self.assertNotIn('published_prices', self.f.portal.app.extensions['shop_production'])
        self.assertFalse(dependencies['catalogue_warmup']([self.pid], quantities=(1,)))
        for private in ('unit_net', 'supplier_article_id', 'mapping', 'source_refs', 'PRIVATE_CANARY', 'ALZURA_VENDOR_CANARY'):
            self.assertNotIn(private, json.dumps(product))

    def test_literal_display_approval_is_required_before_provider_and_storage(self):
        self.install(fixed=True, buyers=True)
        for value in (None, False, 1, 'true'):
            with self.subTest(value=value):
                self.f.descriptor['parts_inquiry_prices_approved'] = value
                production_fixture.write_json(self.f.root / 'current.json', self.f.descriptor)
                self.prepare(success=False); self.assert_no_provider_or_db()

    def test_wrong_pin_and_self_rehashed_wrong_markup_fail_before_supplier_and_storage(self):
        self.install(fixed=True, buyers=True)
        self.f.descriptor['parts_inquiry_prices']['sha256'] = 'f' * 64
        production_fixture.write_json(self.f.root / 'current.json', self.f.descriptor)
        self.prepare(success=False); self.assert_no_provider_or_db()
        self.manifest['display_prices'][self.pid]['unit_gross_cent'] += 1
        self.save(); self.prepare(success=False); self.assert_no_provider_or_db()

    def test_corrupted_original_and_forbidden_rights_fail_before_supplier_and_storage(self):
        self.install(fixed=True, buyers=True)
        path = self.f.root / 'parts-sources' / 'rest.json'; original = path.read_bytes(); path.write_bytes(b'{}')
        self.prepare(success=False); self.assert_no_provider_or_db(); path.write_bytes(original)
        self.manifest['shipping_classes'] = {self.pid: 'parts_standard'}
        self.save(); self.prepare(success=False); self.assert_no_provider_or_db()

    def test_fixed_pricebook_keeps_only_bound_rim_and_cannot_refresh_or_procure_display_part(self):
        self.install(fixed=True, buyers=True); dependencies = self.prepare()
        state = self.f.portal.app.extensions['shop_production']
        self.assertEqual(set(state['published_prices'].products), {self.rid})
        self.assertEqual(set(state['price_cache']._products), {self.rid})
        self.assertEqual(set(self.supplier._approved), {self.rid})
        self.assertIsNone(state['published_prices'].get(self.pid))
        self.assertEqual(dependencies['catalogue'].get(self.rid)['price']['prices_by_quantity']['1']['unit_gross_cent'], 13685)
        self.assertEqual(dependencies['catalogue'].get(self.rid)['purchase_status'], 'supplier_check_required')
        self.assertEqual(dependencies['catalogue'].get(self.pid)['price']['prices_by_quantity']['1']['unit_gross_cent'], 1369)
        self.assertIs(dependencies['catalogue'].get(self.pid)['price']['stock_current'], False)
        self.assertFalse(dependencies['catalogue_warmup']([self.pid], quantities=(1,)))
        self.assertFalse(state['price_cache'].run_once()); self.assertEqual(self.inquiry.calls, [])
        private_part = dependencies['quote_service'].catalogue_lookup(self.pid)
        self.assertIsNone(self.supplier.rsu_mapping(private_part))
        self.assertIsNone(dependencies['quote_service'].retail_quote(private_part, 1))
        self.assertEqual(self.inquiry.calls, [])
        with closing(sqlite3.connect(state['published_prices'].path)) as db:
            self.assertEqual(db.execute('SELECT product_id FROM shop_public_prices_v1').fetchall(), [(self.rid,)])
        # Independently confirmed existing wheel checkout/transport still works.
        private_rim = dependencies['quote_service'].catalogue_lookup(self.rid)
        quote = dependencies['quote_service'].retail_quote(private_rim, 1)
        self.assertTrue(quote['procurement_confirmed']); self.assertEqual(quote['unit_gross_cent'], 13685)
        self.assertEqual(self.inquiry.calls, [('42', 1)])

    def test_forged_browser_price_cannot_retain_part_quote_create_order_or_payment(self):
        self.install(fixed=True, buyers=True); dependencies = self.prepare()
        register_shop_runtime(self.f.portal, **dependencies); client = self.f.portal.app.test_client()
        response = client.get('/shop/api/v2/buyer/session', base_url=ORIGIN)
        headers = {'Origin': ORIGIN, 'X-CSRF-Token': response.get_json()['csrf_token'], 'Sec-Fetch-Site': 'same-origin'}
        address = {'recipient': 'Synthetic Buyer', 'street': 'Teststrasse 12', 'postcode': '74821',
            'city': 'Mosbach', 'country': 'DE'}
        payload = {'contact': {'name': 'Synthetic Buyer', 'email': 'buyer@example.test', 'phone': '+49 6261 12345'},
            'billing': address, 'delivery': {**address, 'salutation': 'Herr'}, 'workshop_delivery': 'collection'}
        self.assertEqual(client.put('/shop/api/v2/buyer/address', base_url=ORIGIN, json=payload, headers=headers).status_code, 200)
        cart = {'lines': [{'id': self.pid, 'quantity': 1, 'services': {}}]}
        response = client.post('/shop/api/v2/cart/quote', base_url=ORIGIN, json=cart, headers=headers)
        self.assertEqual(response.status_code, 200); value = response.get_json()
        self.assertFalse(value['quote']['ready']); self.assertFalse(value['quote_retained'])
        self.assertFalse(value['order_created']); self.assertFalse(value['payment_enabled'])
        forged = deepcopy(cart); forged['lines'][0].update(unit_gross_cent=1369, procurement_confirmed=True)
        self.assertEqual(client.post('/shop/api/v2/cart/quote', base_url=ORIGIN, json=forged, headers=headers).status_code, 400)
        # Even exposing the ordinary draft endpoint cannot make an unretained
        # display hash an order; this is not an authorization/checkout bypass.
        self.f.portal.app.config['SHOP_ORDER_DRAFT_ENABLED'] = True
        response = client.post('/shop/api/v2/orders', base_url=ORIGIN, headers=headers,
            json={'quote_hash': value['quote']['quote_hash'], 'idempotency_key': 'synthetic-inquiry-only'})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(client.post('/shop/api/v2/orders/unknown/checkout', base_url=ORIGIN, json={}, headers=headers).status_code, 404)
        path = self.f.data / 'shop-v2' / 'shop.sqlite3'
        with closing(sqlite3.connect(path)) as db:
            for table in ('shop_quotes_v2', 'shop_orders_v2', 'shop_payment_attempts_v2', 'shop_payment_events_v2'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 0)
        self.assertEqual(self.inquiry.calls, [])


if __name__ == '__main__': unittest.main(verbosity=2)
