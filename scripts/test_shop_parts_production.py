"""Real temporary production bootstrap and quote pipeline; synthetic supplier IO."""
from copy import deepcopy
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import json
import sqlite3
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
import test_shop_production as production_fixture
import test_shop_parts_release as release_fixture
import test_reconcile_rsu_parts as product_fixture
import test_shop_parts_fitment as vehicle_fixture
from shop_catalog_public import variant_fingerprint
from shop_parts_fitment import PartsVehicleFitments
from shop_parts_freight import _hash
from shop_rsu_quotes import RSUQuoteProvider
from shop_runtime import register_shop_runtime
from tyresystem_api import ArticleInquiry

ORIGIN = 'https://shop.example.test'
REF = 'alzura-vehicle:120604'


class SyntheticInquiry:
    def __init__(self, ean):
        self.ean, self.calls, self.stock, self.unit_net = ean, [], 2, Decimal('10')

    def inquire(self, article, quantity):
        self.calls.append((article, quantity))
        return ArticleInquiry(article, self.ean, quantity, self.unit_net, self.stock,
                              'SYNTHETIC PART', datetime.now(timezone.utc), 'live')


class PartsProductionChecks(unittest.TestCase):
    def setUp(self):
        self.f = production_fixture.ReleaseChecks(); self.f.setUp(); self.addCleanup(self.f.doCleanups)
        self.part = product_fixture.row()
        self.part.update(category='oil', department='accessories', price=None, unit_label='1 Liter')
        self.pid = self.part['selection_id']
        self.f.bundle['products'].append(self.part)
        self.f.bundle['publication']['released_ids'].append(self.pid)
        self.f.bundle.update(rsu_bindings={}, rsu_price_seeds={'schema_version': 1,
            'evidence_id': 'b' * 64, 'records': {}}, shipping_classes={}, delivery_postcodes=['74821'])
        self.f.descriptor.update(fixed_pricebook_enabled=True, buyer_and_quotes_enabled=True,
            initial_storage_creation=True, rsu_reads_approved=True, mainland_delivery_approved=True,
            rsu_config=production_fixture.write_json(self.f.root / 'rsu-private.json', {}))
        self.f.install()
        self.inquiry = SyntheticInquiry(product_fixture.EAN)
        self.supplier = None
        self.factory = Mock(side_effect=self.make_supplier)

    def make_supplier(self, *, config, bindings, approved_ids, policy, **kwargs):
        # Keep actual binding validation, exact-quantity pricing, private cache
        # and production policy. Only the external read operation is synthetic.
        self.supplier = RSUQuoteProvider(bindings=bindings, approved_ids=approved_ids,
                                         policy=policy, inquiry=self.inquiry)
        return self.supplier

    def install(self, *, fixed=True, **kwargs):
        self.f.install()
        record, self.release = release_fixture.install_parts_fixture(self.f.root, self.f.bundle,
            self.f.descriptor['bundle']['sha256'], fixed=fixed, **kwargs)
        self.f.descriptor.update(parts_direct_sales=record, parts_direct_sales_approved=True)
        production_fixture.write_json(self.f.root / 'current.json', self.f.descriptor)
        return record

    def prepare(self, *, success=True):
        with patch('shop_production.RSUQuoteProvider.from_config', side_effect=self.factory):
            if success:
                dependencies = self.f.prepare()
            else:
                with self.assertLogs(self.f.portal.app.logger, level='ERROR'):
                    dependencies = self.f.prepare()
        if success:
            self.assertTrue(dependencies)
        else:
            self.assertEqual(dependencies, {})
        return dependencies

    def client(self, dependencies):
        register_shop_runtime(self.f.portal, **dependencies)
        self.http = self.f.portal.app.test_client()
        response = self.http.get('/shop/api/v2/buyer/session', base_url=ORIGIN)
        self.assertEqual(response.status_code, 200)
        self.csrf = response.get_json()['csrf_token']
        self.headers = {'Origin': ORIGIN, 'X-CSRF-Token': self.csrf, 'Sec-Fetch-Site': 'same-origin'}

    def address(self, *, vehicle_ref=None, workshop_delivery='collection'):
        address = {'recipient': 'Synthetic Buyer', 'street': 'Teststrasse 12', 'postcode': '74821',
                   'city': 'Mosbach', 'country': 'DE'}
        payload = {'contact': {'name': 'Synthetic Buyer', 'email': 'buyer@example.test', 'phone': '+49 6261 12345'},
                   'billing': address, 'delivery': {**address, 'salutation': 'Herr'},
                   'workshop_delivery': workshop_delivery, 'delivery_mainland': True}
        if vehicle_ref is not None:
            payload['vehicle_ref'] = vehicle_ref
        return self.http.put('/shop/api/v2/buyer/address', base_url=ORIGIN, json=payload, headers=self.headers)

    def quote(self, quantity=1):
        response = self.http.post('/shop/api/v2/cart/quote', base_url=ORIGIN,
            json={'lines': [{'id': self.pid, 'quantity': quantity, 'services': {}}]}, headers=self.headers)
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        return response.get_json()

    def counts(self):
        path = self.f.data / 'shop-v2' / 'shop.sqlite3'
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as connection:
            return {name: connection.execute('SELECT COUNT(*) FROM ' + name).fetchone()[0] for name in
                    ('shop_quotes_v2', 'shop_orders_v2', 'shop_payment_attempts_v2', 'shop_payment_events_v2')}

    def no_early_db(self):
        self.factory.assert_not_called()
        self.assertFalse((self.f.data / 'shop-v2').exists())
        self.assertFalse((self.f.data / 'shop-pricebook').exists())
        self.assertFalse(self.f.portal.app.config['SHOP_PARTS_DIRECT_ENABLED'])
        self.assertEqual(self.f.portal.app.config['SHOP_PARTS_VEHICLE_OPTIONS'], [])

    def install_vehicle(self, *, approved=True, mutate_review=None):
        identity = variant_fingerprint(self.part)
        data = vehicle_fixture.artifact(identity)
        data['bundle_sha256'] = self.f.descriptor['bundle']['sha256']
        data['proofs'][0].update(product_id=self.pid, manufacturer=self.part['manufacturer_name'],
            mpn=self.part['details']['Herstellerartikelnummer'],
            native_provider_id='wearparts:123-' + self.part['details']['Herstellerartikelnummer'].encode().hex() + ':123')
        raw = release_fixture.encode(data)
        registry = PartsVehicleFitments(raw, artifact_sha256=sha256(raw).hexdigest(),
            base_bundle_sha256=data['bundle_sha256'], products={self.pid: {'identity_hash': identity, 'category': self.part['category']}})
        requirements = registry.review_requirements(self.pid, REF)
        keys = ('product_id', 'identity_hash', 'vehicle_ref', 'association_hash', 'conditions_hash', 'vehicle_details_hash')
        review = {**{key: requirements[key] for key in keys}, 'conditions_reviewed': True, 'vehicle_fitment_reviewed': True,
                  'unresolved_conditions': [], 'certificate_refs': [], 'review_evidence_id': 'SYNTHETIC-source-condition-review'}
        if mutate_review:
            mutate_review(review)
        source = production_fixture.write_json(self.f.root / 'parts-vehicles.json', data)
        reviews = production_fixture.write_json(self.f.root / 'parts-conditions.json',
            {'approved_conditions': [review] if approved else [], 'approved_certificate_hashes': []})
        self.f.descriptor['parts_vehicle_fitments'] = {'source': source, 'approvals': reviews}
        production_fixture.write_json(self.f.root / 'current.json', self.f.descriptor)

    def test_absent_optional_release_preserves_legacy_flags_and_unoffered_parts(self):
        # No source delta, credentials or API readers needed for the old browse release.
        for key in ('fixed_pricebook_enabled', 'buyer_and_quotes_enabled', 'rsu_reads_approved'):
            self.f.descriptor.pop(key, None)
        self.f.install()
        dependencies = self.prepare()
        self.factory.assert_not_called()
        self.assertFalse(self.f.portal.app.config['SHOP_PARTS_DIRECT_ENABLED'])
        self.assertEqual(self.f.portal.app.config['SHOP_PARTS_VEHICLE_OPTIONS'], [])
        self.assertEqual(dependencies['catalogue'].get(self.pid)['purchase_status'], 'inquiry_only')
        self.assertFalse((self.f.data / 'shop-pricebook').exists())
        self.assertFalse((self.f.data / 'shop-v2').exists())

    def test_explicit_literal_approval_is_required_before_provider_and_databases(self):
        self.install()
        for value in (None, False, 1, 'true'):
            self.f.descriptor['parts_direct_sales_approved'] = value
            production_fixture.write_json(self.f.root / 'current.json', self.f.descriptor)
            self.prepare(success=False); self.no_early_db()

    def test_original_sources_not_normalized_claims_decide_admission_before_db(self):
        self.install(normalized_changes={'unit_net': '11.00'})
        self.prepare(success=False); self.no_early_db()

    def test_bulky_or_unknown_original_freight_is_not_activated_as_standard(self):
        self.install(offers=('6.00', '60.00'))
        self.prepare(success=False); self.no_early_db()

    def test_fixed_parts_pipeline_prices_fifteen_percent_vat_shipping_and_retains_no_order(self):
        self.install(); dependencies = self.prepare()
        self.assertEqual(self.inquiry.calls, [])
        self.assertTrue(self.f.portal.app.config['SHOP_PARTS_DIRECT_ENABLED'])
        self.assertEqual(dependencies['catalogue'].get(self.pid)['price']['prices_by_quantity']['1']['unit_gross_cent'], 1369)
        self.assertEqual(self.inquiry.calls, [])  # published fixed price shown immediately
        self.client(dependencies)
        self.assertEqual(self.address().status_code, 200)
        value = self.quote(); quote = value['quote']
        self.assertTrue(quote['ready'])
        self.assertEqual(quote['lines'][0]['unit_gross_cent'], 1369)
        self.assertEqual(quote['shipments'][0]['gross_cent'], 821)
        self.assertEqual(quote['total_gross_cent'], 2190)
        self.assertTrue(value['quote_retained']); self.assertFalse(value['order_created']); self.assertFalse(value['payment_enabled'])
        self.assertEqual(self.inquiry.calls, [('123', 1)])  # retained freight adds no read
        self.assertEqual(self.counts(), {'shop_quotes_v2': 1, 'shop_orders_v2': 0,
            'shop_payment_attempts_v2': 0, 'shop_payment_events_v2': 0})
        for private in ('unit_net', 'supplier_article_id', 'source_ref', 'PRIVATE_CANARY', 'ALZURA_VENDOR_CANARY'):
            self.assertNotIn(private, json.dumps(value))

    def test_exact_quantity_and_missing_stock_are_not_inferred_from_q1(self):
        self.install(); dependencies = self.prepare(); self.client(dependencies)
        self.assertEqual(self.address().status_code, 200)
        first = self.quote(2)
        self.assertTrue(first['quote']['ready']); self.assertEqual(first['quote']['total_gross_cent'], 3559)
        self.assertEqual(self.inquiry.calls, [('123', 2)])
        failed = self.quote(3)
        self.assertFalse(failed['quote']['ready']); self.assertFalse(failed['quote_retained'])
        self.assertEqual(self.inquiry.calls, [('123', 2), ('123', 3)])
        self.assertEqual(self.counts()['shop_orders_v2'], 0)

    def test_customer_quote_requires_real_guest_csrf_and_does_not_read_for_unauthorized_post(self):
        self.install(); dependencies = self.prepare(); self.client(dependencies)
        response = self.http.post('/shop/api/v2/cart/quote', base_url=ORIGIN,
            json={'lines': [{'id': self.pid, 'quantity': 1}]}, headers={'Origin': ORIGIN})
        self.assertIn(response.status_code, (401, 403))
        self.assertEqual(self.inquiry.calls, [])
        self.assertEqual(self.counts()['shop_quotes_v2'], 0)

    def test_real_composed_tariff_supports_workshop_collection_and_reshipment_without_guessing_labor(self):
        self.install(); dependencies = self.prepare(); quotes = dependencies['quote_service']
        product = quotes.catalogue_lookup(self.pid)
        self.assertIsNotNone(quotes.retail_quote(product, 1))
        mapping = self.supplier.rsu_mapping(product)
        context = {'delivery': {'recipient': 'Synthetic Buyer', 'street': 'Teststrasse 12', 'postcode': '74821',
                    'city': 'Mosbach', 'country': 'DE', 'salutation': 'Herr'}, 'delivery_mainland': True,
                   'workshop_delivery': 'customer_reshipment'}
        rows = [{'id': self.pid, 'identity_hash': product['identity_hash'], 'quantity': 1,
                 'mapping': {k: mapping[k] for k in ('supplier_article_id', 'evidence_id')}}]
        shipment = {'id': 'workshop', 'destination': 'workshop', 'private_items': rows,
                    'items': [{'id': self.pid, 'quantity': 1}], 'context_hash': _hash(context)}
        shipment['shipment_hash'] = _hash({k: shipment[k] for k in ('id', 'destination', 'private_items', 'context_hash')})
        self.assertEqual(quotes.freight_quote(shipment, context)['gross_cent'], 1642)
        self.assertEqual(self.inquiry.calls, [('123', 1)])

    def test_panel_without_reviewed_exact_vehicle_stays_blocked_despite_price_and_freight(self):
        self.part.update(category='body_part', department='parts')
        self.install(); dependencies = self.prepare(); self.client(dependencies)
        self.assertEqual(self.address().status_code, 200)
        value = self.quote()
        self.assertFalse(value['quote']['ready']); self.assertFalse(value['quote_retained'])
        self.assertIn('fitment_unconfirmed', {r['code'] for r in value['quote']['blocked_reasons']})

    def test_reviewed_exact_registry_reaches_quote_and_wrong_vehicle_never_does(self):
        self.part.update(category='body_part', department='parts')
        self.install(); self.install_vehicle(); dependencies = self.prepare(); self.client(dependencies)
        self.assertEqual(self.f.portal.app.config['SHOP_PARTS_VEHICLE_OPTIONS'][0]['vehicle_ref'], REF)
        self.assertEqual(self.address(vehicle_ref=REF).status_code, 200)
        self.assertTrue(self.quote()['quote']['ready'])
        response = self.address(vehicle_ref='Audi all models')
        if response.status_code == 200:
            self.assertFalse(self.quote()['quote']['ready'])
        else:
            self.assertEqual(response.status_code, 400)

    def test_invalid_vehicle_approval_cannot_mutate_databases_before_failure(self):
        self.part.update(category='body_part', department='parts')
        self.install(); self.install_vehicle(mutate_review=lambda review: review.update(conditions_hash='d' * 64))
        self.prepare(success=False); self.no_early_db()

    def test_identity_bound_manufacturer_notice_is_public_without_private_review_sources(self):
        self.part.update(category='body_part', department='parts')
        self.install(); self.install_vehicle()
        path=self.f.root/'parts-conditions.json'
        profile=json.loads(path.read_bytes())
        profile['public_notices']={self.pid:{'identity_hash':variant_fingerprint(self.part),
            'notes':['Nur zur Reparatur des ursprünglichen Erscheinungsbilds.']}}
        self.f.descriptor['parts_vehicle_fitments']['approvals']=production_fixture.write_json(path,profile)
        production_fixture.write_json(self.f.root/'current.json',self.f.descriptor)
        dependencies=self.prepare()
        product=dependencies['catalogue'].get(self.pid)
        self.assertEqual(product['application_notes'],profile['public_notices'][self.pid]['notes'])
        self.assertNotIn('approved_conditions',json.dumps(product))
        self.assertEqual(self.inquiry.calls,[])

    def test_wrong_identity_notice_closes_before_any_provider_or_database(self):
        self.part.update(category='body_part', department='parts')
        self.install(); self.install_vehicle()
        path=self.f.root/'parts-conditions.json';profile=json.loads(path.read_bytes())
        profile['public_notices']={self.pid:{'identity_hash':'f'*64,'notes':['Unrelated article notice']}}
        self.f.descriptor['parts_vehicle_fitments']['approvals']=production_fixture.write_json(path,profile)
        production_fixture.write_json(self.f.root/'current.json',self.f.descriptor)
        self.prepare(success=False);self.no_early_db()


if __name__ == '__main__':
    unittest.main(verbosity=2)
