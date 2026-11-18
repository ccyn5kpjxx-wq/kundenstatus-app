"""Offline end-to-end monetary/hold boundary; no provider or production IO."""
from contextlib import closing
from copy import deepcopy
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shop_checkout import CheckoutError, ShopCheckout
from shop_complete_wheels import is_review_snapshot
from shop_order_store import OrderStore, OrderStoreError, init_schema
from test_shop_cart_quotes import Fixture, NOW, TYRE, RIM, OIL, line, digest
from test_shop_checkout import FakeGateway
from test_shop_order_store import rehash
from test_shop_catalog_enriched import catalogue, pair, tyre
from test_shop_catalog_public import wheel
from shop_catalog_public import variant_fingerprint

CONTEXT = {'postcode': '74821', 'address_version': 'synthetic-1', 'vehicle_ref': 'CAR-1'}


def fixture():
    f = Fixture()
    f.products[TYRE]['fitment_required'] = False
    f.products[RIM]['fitment'] = None
    return f


def proposal(rim, tire, vehicle):
    return {'rim_id': rim['id'], 'tyre_id': tire['id'], 'rim_identity_hash': rim['identity_hash'],
            'tyre_identity_hash': tire['identity_hash'], 'rim_diameter': rim['diameter'],
            'tyre_size': tire['tyre_size'], 'evidence_id': 'SYNTHETIC-original-dimensional-table-page'}


def make_quote(f=None, *, rows=None, vehicle='CAR-1', **changes):
    f = f or fixture()
    kwargs = dict(manual_mixed_orders=True, assembly_fitment=lambda *a: None,
                  complete_wheel_review=True, assembly_proposal=proposal)
    kwargs.update(changes)
    rows = rows or [line(RIM, 4, services={'mounting': True}, group='set-1'),
                    line(TYRE, 4, services={'mounting': True}, group='set-1')]
    return f.service_instance(**kwargs).quote({'lines': rows}, customer_id='23',
                                             vehicle_ref=vehicle, delivery_context=CONTEXT)


class CompleteWheelChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_all_in_total_is_real_and_mounting_counted_once_without_fake_fitment(self):
        f = fixture()
        q = make_quote(f)
        s, public = q.snapshot(), q.public()
        self.assertTrue(is_review_snapshot(s))
        self.assertEqual(s['version'], 2)
        self.assertTrue(public['authorization_ready'])
        self.assertFalse(public['fulfillment_ready'])
        self.assertEqual(s['total_gross_cent'], 8 * 13685 + 4 * 2500 + 700)
        self.assertEqual(len([c for c in f.calls['services'] if c[0] == 'mounting']), 1)
        self.assertEqual(s['assembly_evidence'], {})
        self.assertEqual(s['product_checks'][RIM]['fitment_evidence_id'], None)
        self.assertIn('set-1', s['assembly_proposals'])
        self.assertEqual(len(public['review_requirements']), 3)
        self.assertNotIn('PRIVATE_CANARY', str(public))

    def test_unmounted_pair_does_not_enter_mounted_complete_wheel_checkout(self):
        rows = [line(RIM, 4, group='set-1'), line(TYRE, 4, group='set-1')]
        s = make_quote(rows=rows).snapshot()
        self.assertFalse(s['ready'])
        self.assertIsNone(s['total_gross_cent'])
        self.assertTrue(all(not r['services'] for r in s['lines']))

    def test_feature_off_keeps_old_inquiry_policy(self):
        s = make_quote(complete_wheel_review=False).snapshot()
        self.assertEqual(s['version'], 1)
        self.assertFalse(s['ready'])
        self.assertIsNone(s['total_gross_cent'])

    def test_wrong_pair_geometry_counts_source_or_vehicle_cannot_be_deferred(self):
        cases = [({'rows': [line(RIM, 4, group='set-1'), line(TYRE, 3, group='set-1')]}, None),
                 ({'assembly_proposal': lambda *a: None}, None),
                 ({'vehicle': ''}, None),
                 ({'assembly_proposal': lambda *a: {**proposal(*a), 'tyre_identity_hash': 'a' * 64}}, None),
                 ({}, lambda f: f.products[TYRE].update(diameter=19)),
                 ({}, lambda f: f.products[RIM].update(fitment_required=False))]
        for kwargs, mutation in cases:
            with self.subTest(kwargs=kwargs):
                f = fixture()
                if mutation:
                    mutation(f)
                s = make_quote(f, **kwargs).snapshot()
                self.assertFalse(s['ready'])
                self.assertIsNone(s['total_gross_cent'])
                self.assertNotEqual(s['version'], 2)

    def test_unknown_money_stock_documents_and_mapping_stay_hard_blocks(self):
        for kind in ('price', 'stock', 'freight', 'service', 'mapping', 'documents'):
            with self.subTest(kind=kind):
                f, kwargs = fixture(), {}
                if kind == 'price': kwargs['retail_quote'] = lambda *a: None
                if kind == 'stock': f.stock[TYRE] = 3
                if kind == 'freight': kwargs['freight_quote'] = lambda *a: None
                if kind == 'service': kwargs['service_quote'] = lambda *a: None
                if kind == 'mapping': kwargs['rsu_mapping'] = lambda *a: None
                if kind == 'documents': kwargs['require_tyre_documents'] = True
                s = make_quote(f, **kwargs).snapshot()
                self.assertFalse(s['ready'])
                self.assertIsNone(s['total_gross_cent'])

    def test_arbitrary_mixed_cart_does_not_gain_complete_wheel_exception(self):
        rows = [line(RIM, 4, group='set-1'), line(TYRE, 4, group='set-1'), line(OIL)]
        self.assertFalse(make_quote(rows=rows).public()['ready'])

    def test_retained_v2_quote_only_starts_checkout_when_review_service_is_available(self):
        with tempfile.TemporaryDirectory() as directory:
            factory = lambda: sqlite3.connect(Path(directory) / 'synthetic.sqlite')
            with closing(factory()) as db:
                init_schema(db)
            store = OrderStore(factory, clock=lambda: NOW)
            s = make_quote().snapshot()
            store.save_quote(s, expected_quote_hash=s['quote_hash'], customer_id='23', mode='test', delivery_context=CONTEXT)
            order = store.create_order(s['quote_hash'], customer_id='23', mode='test', idempotency_key='synthetic-hold-1')
            gateway = FakeGateway()
            checkout = ShopCheckout(store, gateway, mode='test', public_origin='https://shop.example.test',
                enabled=True, clock=lambda: NOW, order_lookup=lambda identifier: {'customer_id': '23', 'mode': 'test'})
            with self.assertRaisesRegex(CheckoutError, 'order_review_unavailable'):
                checkout.start(order['id'], customer_id='23')
            self.assertEqual(gateway.created, [])
            checkout.review_service = SimpleNamespace(readiness=lambda: {'ready': True, 'mode': 'test'})
            response = checkout.start(order['id'], customer_id='23')
            self.assertTrue(response['checkout_url'].startswith('https://checkout.stripe.com/'))
            self.assertEqual(len(gateway.created), 1)
            params = gateway.created[0][0]
            self.assertEqual(params['payment_intent_data']['capture_method'], 'manual')
            self.assertEqual(sum(i['quantity'] * i['price_data']['unit_amount'] for i in params['line_items']), s['total_gross_cent'])
            self.assertFalse(response['payment_confirmed'])

    def test_version_downgrade_and_malformed_review_cannot_be_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            factory = lambda: sqlite3.connect(Path(directory) / 'synthetic.sqlite')
            with closing(factory()) as db: init_schema(db)
            store = OrderStore(factory, clock=lambda: NOW)
            for mutate in (lambda s: s.update(version=1),
                           lambda s: s['review_requirements'].append({'code': 'ignore_stock'}),
                           lambda s: s['assembly_proposals']['set-1'].update(tyre_identity_hash='0' * 64),
                           lambda s: s['lines'][1].update(quantity=3)):
                s = make_quote().snapshot()
                mutate(s)
                s = rehash(s)
                with self.assertRaises(OrderStoreError):
                    store.save_quote(s, expected_quote_hash=s['quote_hash'], customer_id='23', mode='test', delivery_context=CONTEXT)

    def test_catalogue_proposal_requires_original_source_and_exact_fingerprints(self):
        rim, tire = wheel(), tyre()
        ri, ti = rim['selection_id'], tire['selection_id']
        p = {**pair(rim, tire), 'scope': 'rim_width_candidate', 'evidence_id': 'SYNTHETIC-source-id',
             'source': {'sha256': '1' * 64, 'page': 3}}
        r = {'id': ri, 'identity_hash': variant_fingerprint(rim), 'diameter': 18}
        t = {'id': ti, 'identity_hash': variant_fingerprint(tire), 'diameter': 18, 'tyre_size': tire['size']}
        c = catalogue([rim, tire], compatible_pairs={ri: [p]})
        self.assertIsNotNone(c.assembly_proposal(r, t, 'CAR-1'))
        self.assertIsNone(c.assembly_proposal(r, {**t, 'identity_hash': '0' * 64}, 'CAR-1'))
        c = catalogue([rim, tire], compatible_pairs={ri: [pair(rim, tire)]})
        self.assertIsNone(c.assembly_proposal(r, t, 'CAR-1'))


if __name__ == '__main__':
    unittest.main()
