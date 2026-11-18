"""Original ALZURA inquiry prices through real production, without ordering."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
import test_shop_inquiry_production as production_fixture
import test_shop_parts_partner_inquiry as partner_fixture
from test_shop_production import write_json
from shop_production import _parts_api_channel_supported
from shop_runtime import register_shop_runtime

ORIGIN = 'https://shop.example.test'


class PartnerInquiryProductionChecks(unittest.TestCase):
    def setUp(self):
        self.case = production_fixture.InquiryProductionChecks(); self.case.setUp(); self.addCleanup(self.case.doCleanups)
        self.partner = partner_fixture.PartnerInquiryChecks(); self.partner.setUp(); self.addCleanup(self.partner.doCleanups)
        self.f = self.case.f
        self.pid, self.rsu_pid, self.rid = self.partner.pid, self.case.pid, self.case.rid
        self.f.bundle['products'].append(deepcopy(self.partner.product))
        self.f.bundle['publication']['released_ids'].append(self.pid)

    def install(self, *, fixed=False, buyers=False):
        self.case.install(fixed=fixed, buyers=buyers)
        target = self.f.root / 'partner-price-sources'; target.mkdir(exist_ok=True)
        self.manifest = deepcopy(self.partner.manifest)
        self.manifest['base_bundle_sha256'] = self.f.descriptor['bundle']['sha256']
        for ref in self.manifest['source_refs']:
            raw = (self.partner.root / 'partner-price-sources' / ref['file']).read_bytes()
            self.assertEqual(sha256(raw).hexdigest(), ref['sha256'])
            (target / ref['file']).write_bytes(raw)
        self.save()

    def save(self):
        raw = partner_fixture.encode(self.manifest); (self.f.root / 'partner-display.json').write_bytes(raw)
        self.f.descriptor.update(parts_partner_inquiry_prices_approved=True, parts_partner_inquiry_prices={
            'path': 'partner-display.json', 'sha256': sha256(raw).hexdigest(),
            'base_bundle_sha256': self.f.descriptor['bundle']['sha256']})
        self.descriptor_save()

    def descriptor_save(self):
        write_json(self.f.root / 'current.json', self.f.descriptor)

    def prepare(self, *, success=True):
        return self.case.prepare(success=success)

    def assert_no_provider_or_storage(self):
        self.case.assert_no_provider_or_db()

    def test_missing_optional_partner_record_preserves_old_default_and_rsu_price(self):
        self.case.install(); dependencies = self.prepare()
        self.assertIsNone(dependencies['catalogue'].get(self.pid)['price'])
        self.assertEqual(dependencies['catalogue'].get(self.rsu_pid)['price']['source'], 'RSU')
        self.assert_no_provider_or_storage()

    def test_genuine_partner_price_preserves_alzura_and_existing_rsu_before_any_db_or_provider(self):
        self.install(); before = deepcopy(self.f.bundle); dependencies = self.prepare()
        product = dependencies['catalogue'].get(self.pid); price = product['price']
        self.assertEqual(price['source'], 'ALZURA'); self.assertEqual(price['kind'], 'partner_inquiry_pricebook')
        self.assertEqual(set(price['prices_by_quantity']), {str(q) for q in range(1, 21)})
        self.assertEqual(price['prices_by_quantity']['1']['unit_gross_cent'], 13685)
        self.assertEqual(price['prices_by_quantity']['20']['items_gross_cent'], 273700)
        self.assertIs(price['stock_current'], False)
        self.assertLess(datetime.now(timezone.utc), datetime.fromisoformat(price['stock_expires_at']))
        self.assertEqual(product['purchase_status'], 'inquiry_only'); self.assertIs(product['order_enabled'], False)
        rsu = dependencies['catalogue'].get(self.rsu_pid)
        self.assertEqual(rsu['price']['source'], 'RSU'); self.assertEqual(rsu['price']['kind'], 'merchant_pricebook')
        self.assertEqual(rsu['price']['prices_by_quantity']['1']['unit_gross_cent'], 1369)
        state = self.f.portal.app.extensions['shop_production']
        self.assertEqual(state['inquiry_price_count'], 2); self.assertEqual(state['partner_inquiry_price_count'], 1)
        self.assertEqual(self.f.bundle, before); self.assert_no_provider_or_storage()
        self.assertIs(_parts_api_channel_supported(), False)
        self.assertIs(self.f.portal.app.config['SHOP_PARTS_DIRECT_ENABLED'], False)
        for private in ('wholesaler_price', 'unit_net', 'supplier_article_id', 'source_refs', 'PRIVATE_CANARY'):
            self.assertNotIn(private, json.dumps(product))

    def test_invalid_partner_approval_pin_markup_or_source_closes_before_factory_and_db(self):
        self.install(fixed=True, buyers=True)
        for value in (None, False, 1, 'true'):
            self.f.descriptor['parts_partner_inquiry_prices_approved'] = value; self.descriptor_save()
            self.prepare(success=False); self.assert_no_provider_or_storage()
        self.f.descriptor['parts_partner_inquiry_prices_approved'] = True
        self.f.descriptor['parts_partner_inquiry_prices']['sha256'] = 'f' * 64; self.descriptor_save()
        self.prepare(success=False); self.assert_no_provider_or_storage()
        self.manifest['display_prices'][self.pid]['unit_gross_cent'] += 1; self.save()
        self.prepare(success=False); self.assert_no_provider_or_storage()
        self.manifest['display_prices'][self.pid]['unit_gross_cent'] -= 1; self.save()
        (self.f.root / 'partner-price-sources' / 'body.json').write_bytes(b'{}')
        self.prepare(success=False); self.assert_no_provider_or_storage()

    def test_rsu_overlap_and_procurement_fields_cannot_replace_the_rsu_display_channel(self):
        self.install(fixed=True, buyers=True)
        original = deepcopy(self.manifest)
        self.manifest['display_prices'][self.rsu_pid] = self.manifest['display_prices'].pop(self.pid)
        self.save(); self.prepare(success=False); self.assert_no_provider_or_storage()
        self.manifest = original; self.manifest['rsu_bindings'] = {self.pid: {'approved': True}}
        self.save(); self.prepare(success=False); self.assert_no_provider_or_storage()

    def test_fixed_book_cache_and_supplier_remain_rim_only_while_both_inquiry_prices_show(self):
        self.install(fixed=True, buyers=True); dependencies = self.prepare()
        state = self.f.portal.app.extensions['shop_production']
        self.assertEqual(set(state['published_prices'].products), {self.rid})
        self.assertEqual(set(state['price_cache']._products), {self.rid})
        self.assertEqual(set(self.case.supplier._approved), {self.rid})
        for pid in (self.pid, self.rsu_pid):
            self.assertIsNone(state['published_prices'].get(pid))
            self.assertFalse(dependencies['catalogue_warmup']([pid], quantities=(1,)))
            item = dependencies['quote_service'].catalogue_lookup(pid)
            self.assertIsNone(self.case.supplier.rsu_mapping(item))
            self.assertIsNone(dependencies['quote_service'].retail_quote(item, 1))
        self.assertFalse(state['price_cache'].run_once()); self.assertEqual(self.case.inquiry.calls, [])
        self.assertEqual(dependencies['catalogue'].get(self.rid)['price']['source'], 'RSU')
        with closing(sqlite3.connect(state['published_prices'].path)) as db:
            self.assertEqual(db.execute('SELECT product_id FROM shop_public_prices_v1').fetchall(), [(self.rid,)])

    def test_authenticated_http_cannot_use_visible_partner_price_as_financial_quote(self):
        self.install(fixed=True, buyers=True); dependencies = self.prepare()
        register_shop_runtime(self.f.portal, **dependencies); client = self.f.portal.app.test_client()
        session = client.get('/shop/api/v2/buyer/session', base_url=ORIGIN)
        headers = {'Origin': ORIGIN, 'X-CSRF-Token': session.get_json()['csrf_token'], 'Sec-Fetch-Site': 'same-origin'}
        address = {'recipient': 'Synthetic Buyer', 'street': 'Teststrasse 12', 'postcode': '74821',
            'city': 'Mosbach', 'country': 'DE'}
        payload = {'contact': {'name': 'Synthetic Buyer', 'email': 'buyer@example.test', 'phone': '+49 6261 12345'},
            'billing': address, 'delivery': {**address, 'salutation': 'Herr'}, 'workshop_delivery': 'collection'}
        self.assertEqual(client.put('/shop/api/v2/buyer/address', base_url=ORIGIN, json=payload, headers=headers).status_code, 200)
        public = client.get('/shop/api/v2/products/' + self.pid, base_url=ORIGIN).get_json()
        self.assertEqual(public['item']['price']['source'], 'ALZURA')
        cart = {'lines': [{'id': self.pid, 'quantity': 1, 'services': {}}]}
        response = client.post('/shop/api/v2/cart/quote', base_url=ORIGIN, json=cart, headers=headers)
        self.assertEqual(response.status_code, 200); value = response.get_json()
        self.assertFalse(value['quote']['ready']); self.assertFalse(value['quote_retained'])
        self.assertFalse(value['order_created']); self.assertFalse(value['payment_enabled'])
        forged = deepcopy(cart); forged['lines'][0].update(unit_gross_cent=13685, source='ALZURA')
        self.assertEqual(client.post('/shop/api/v2/cart/quote', base_url=ORIGIN, json=forged, headers=headers).status_code, 400)
        self.f.portal.app.config['SHOP_ORDER_DRAFT_ENABLED'] = True
        response = client.post('/shop/api/v2/orders', base_url=ORIGIN, headers=headers,
            json={'quote_hash': value['quote']['quote_hash'], 'idempotency_key': 'synthetic-partner-display'})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(client.post('/shop/api/v2/orders/unknown/checkout', base_url=ORIGIN,
            json={}, headers=headers).status_code, 404)
        with closing(sqlite3.connect(self.f.data / 'shop-v2' / 'shop.sqlite3')) as db:
            for table in ('shop_quotes_v2', 'shop_orders_v2', 'shop_payment_attempts_v2', 'shop_payment_events_v2'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 0)
        self.assertEqual(self.case.inquiry.calls, [])


if __name__ == '__main__': unittest.main(verbosity=2)
