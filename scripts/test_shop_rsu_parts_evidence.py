"""Offline exact-part source codec, quote and persisted procurement checks."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import json
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
import shop_rsu_parts_evidence as evidence
from shop_rsu_parts_evidence import PartsEvidenceError, build_parts_binding, parts_source_record
from shop_rsu_quotes import RSUQuoteError, RSUQuoteProvider, RSUQuotePolicy
from shop_rsu_orders import freeze_order_request
from shop_order_store import OrderStore, init_schema
from shop_fulfillment import ShopFulfillment, FulfillmentJournal, init_fulfillment_schema
from tyresystem_api import ArticleInquiry
from test_shop_cart_quotes import Fixture, NOW, OIL, line
from test_shop_fulfillment import Payment, Transport, POLICY

SID, IDENTITY, EAN, ARTICLE = 'parts:' + '1' * 24, 'c' * 64, '4019238594195', '900001'


def binding(*, product_id=SID, identity_hash=IDENTITY, category='accessory', catalogue_ean=EAN,
            rsu_ean=EAN, manufacturer='Example Parts', mpn='EP-01/B', supplier_article=ARTICLE):
    catalogue = parts_source_record('catalogue', source_sha256='a' * 64, row_number=7,
        article_id=product_id, manufacturer=manufacturer, mpn=mpn, ean=catalogue_ean)
    rsu = parts_source_record('rsu', source_sha256='b' * 64, row_number=1,
        article_id=supplier_article, manufacturer=manufacturer, mpn=mpn, ean=rsu_ean)
    return build_parts_binding(product_id=product_id, identity_hash=identity_hash,
                               category=category, catalogue=catalogue, rsu=rsu)


def validate(value, *, product_id=SID, category='accessory', identity_hash=IDENTITY):
    return evidence.validate_parts_mapping(value['mapping'], product_id=product_id, category=category,
                                           expected_ean=value['expected_ean'], identity_hash=identity_hash)


def reseal(value):
    value['mapping']['evidence_id'] = evidence._hash(value['mapping']['evidence'])


class FakeInquiry:
    def __init__(self, clock):
        self.clock, self.calls, self.changes = clock, [], {}

    def inquire(self, article, quantity):
        self.calls.append((article, quantity))
        return replace(ArticleInquiry(article, EAN, quantity, Decimal('50'), 20,
                       'Synthetic inspected part', self.clock(), 'test'), **self.changes)


class EvidenceChecks(unittest.TestCase):
    def test_all_non_wheel_categories_bind_exact_identity_sources_only(self):
        for category in sorted(evidence.CATEGORIES):
            record = binding(category=category)
            verified = validate(record, category=category)
            self.assertEqual(verified['supplier_article_id'], ARTICLE)
            self.assertEqual(verified['expected_ean'], EAN)
            self.assertEqual(verified['catalogue_source_sha256'], 'a' * 64)
            encoded = json.dumps(record)
            for forbidden in ('unit_net', 'stock', 'price', 'shipping', 'fitment', 'confirmed'):
                self.assertNotIn('"' + forbidden + '"', encoded)

    def test_missing_catalogue_ean_can_match_exact_maker_mpn_but_supplier_ean_required(self):
        record = binding(catalogue_ean=None)
        self.assertEqual(record['mapping']['evidence']['match_basis'], 'manufacturer_mpn')
        self.assertEqual(validate(record)['expected_ean'], EAN)
        for missing in (None, '', '0000000000000', '4019238594194'):
            with self.subTest(ean=missing), self.assertRaises(PartsEvidenceError):
                binding(catalogue_ean=None, rsu_ean=missing)

    def test_normalization_preserves_punctuation_variant_and_no_maker_alias(self):
        source = parts_source_record('rsu', source_sha256='b' * 64, row_number=1,
            article_id=ARTICLE, manufacturer='  EXAMPLE   PARTS ', mpn='EP-01/B', ean=EAN)
        self.assertEqual(source['identity'], {'manufacturer': 'example parts', 'mpn': 'ep-01/b', 'ean': EAN})
        for changes in ({'manufacturer': 'Example'}, {'mpn': 'EP01/B'}, {'mpn': 'EP-01/C'}, {'mpn': 'EP-01/B XL'}):
            record = binding(); proof = record['mapping']['evidence']
            old = proof['rsu']; identity = {**old['identity'], **changes}
            proof['rsu'] = parts_source_record('rsu', source_sha256=old['source_sha256'], row_number=old['row_number'],
                article_id=old['article_id'], **identity)
            reseal(record)
            with self.subTest(changes=changes), self.assertRaises(PartsEvidenceError): validate(record)

    def test_conflicting_ean_is_rejected_even_when_maker_mpn_match(self):
        with self.assertRaises(PartsEvidenceError): binding(catalogue_ean='4046283759835')

    def test_recomputed_outer_hash_does_not_allow_row_or_source_tampering(self):
        for key, changed in [('source_sha256', 'd' * 64), ('row_number', 8), ('row_fingerprint', 'e' * 64),
                             ('article_id', '900002')]:
            record = binding(); record['mapping']['evidence']['rsu'][key] = changed; reseal(record)
            with self.subTest(key=key), self.assertRaises(PartsEvidenceError): validate(record)
        record = binding(); record['mapping']['evidence']['catalogue']['identity']['mpn'] = 'EP-01/B'; reseal(record)
        with self.assertRaises(PartsEvidenceError): validate(record)  # noncanonical even if logically similar

    def test_cross_product_category_identity_and_supplier_source_replays_fail(self):
        record = binding()
        for kwargs in ({'product_id': 'parts:' + '2' * 24}, {'category': 'oil'}, {'identity_hash': 'd' * 64}):
            with self.subTest(kwargs=kwargs), self.assertRaises(PartsEvidenceError): validate(record, **kwargs)
        for category in ('tyre', 'alloy_wheel', 'steel_wheel'):
            with self.assertRaises(PartsEvidenceError): binding(category=category)
        record = binding(); proof = record['mapping']['evidence']
        proof['catalogue'], proof['rsu'] = proof['rsu'], proof['catalogue']; reseal(record)
        with self.assertRaises(PartsEvidenceError): validate(record)

    def test_extra_data_bad_types_or_undocumented_offer_selector_never_becomes_binding(self):
        for key, value in [('supplier_offer', 'warehouse-123'), ('supplier_offer', {'id': '123'}),
                           ('supplier_offer', False), ('price', '50.00')]:
            record = binding(); record['mapping']['evidence'][key] = value; reseal(record)
            with self.subTest(key=key, value=value), self.assertRaises(PartsEvidenceError): validate(record)
        for row_number in (True, 0, -1, 300001, '1'):
            with self.assertRaises(PartsEvidenceError):
                parts_source_record('rsu', source_sha256='b' * 64, row_number=row_number,
                    article_id=ARTICLE, manufacturer='Example', mpn='EP-01', ean=EAN)
        for bad in ('', 'Example\x00', 'Example\nParts'):
            with self.assertRaises(PartsEvidenceError): binding(manufacturer=bad)

    def test_generated_candidates_are_detached_from_mutable_source_records(self):
        record = binding(); original = deepcopy(record); verified = validate(record)
        verified['supplier_article_id'] = '900002'
        self.assertEqual(record, original)


class ProviderChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start(); self.addCleanup(self.network.stop)
        self.inquiry = FakeInquiry(lambda: NOW)

    def provider(self, record=None, approved=None, identifier=SID):
        return RSUQuoteProvider(bindings={identifier: record or binding()},
            approved_ids={identifier} if approved is None else approved,
            policy=RSUQuotePolicy('test', 'EUR', True, 'SYNTHETIC-parts-net-unit-policy'),
            inquiry=self.inquiry, clock=lambda: NOW)

    def test_explicit_approval_and_exact_product_are_required_for_every_read(self):
        product = {'id': SID, 'identity_hash': IDENTITY, 'category': 'accessory'}
        self.assertIsNone(self.provider(approved=set()).retail_quote(product, 1))
        self.assertEqual(self.inquiry.calls, [])
        provider = self.provider()
        for changes in ({'id': 'parts:' + '2' * 24}, {'category': 'oil'}, {'identity_hash': 'd' * 64}):
            self.assertIsNone(provider.rsu_mapping({**product, **changes}))
            self.assertIsNone(provider.retail_quote({**product, **changes}, 1))
        self.assertEqual(self.inquiry.calls, [])

    def test_exact_quantity_current_net_fifteen_percent_and_vat_no_invented_stock(self):
        product = {'id': SID, 'identity_hash': IDENTITY, 'category': 'accessory'}
        provider = self.provider(); quote = provider.retail_quote(product, 2)
        self.assertEqual(quote['unit_gross_cent'], 6843)
        self.assertEqual(quote['stock'], 20)
        self.assertEqual(self.inquiry.calls, [(ARTICLE, 2)])
        for private in ('unit_net', 'supplier_article_id', 'source_sha256', 'expected_ean', 'supplier_offer'):
            self.assertNotIn(private, json.dumps(quote))
        self.inquiry.changes = {'stock': 1}
        self.assertIsNone(provider.retail_quote(product, 3))
        self.assertEqual(self.inquiry.calls[-1], (ARTICLE, 3))

    def test_fresh_inquiry_binds_exact_frozen_id_source_and_quantity(self):
        provider = self.provider(); product = {'id': SID, 'identity_hash': IDENTITY, 'category': 'accessory'}
        item = {**product, 'mapping': provider.rsu_mapping(product)}
        fresh = provider.fresh_inquiry(item, 2)
        self.assertEqual((fresh['supplier_article_id'], fresh['ean'], fresh['quantity']), (ARTICLE, EAN, 2))
        self.assertEqual(fresh['unit_net'], '50')
        for key, value in [('supplier_article_id', '900002'), ('evidence_id', 'forged')]:
            changed = deepcopy(item); changed['mapping'][key] = value
            self.assertIsNone(provider.fresh_inquiry(changed, 2))
        self.assertEqual(self.inquiry.calls, [(ARTICLE, 2)])
        for changes in ({'article_id': '900002'}, {'ean': '4046283759835'}, {'amount': 1}, {'stock': 1},
                        {'configured_mode': 'live'}, {'retrieved_at': NOW - timedelta(minutes=1)}):
            self.inquiry.changes = changes
            self.assertIsNone(provider.fresh_inquiry(item, 2))

    def test_missing_malformed_or_conflicting_mapping_cannot_construct_active_adapter(self):
        for mutate in [lambda v: v['mapping']['evidence'].update(supplier_offer={'id': 'warehouse1'}),
                       lambda v: v['mapping']['evidence']['rsu'].update(article_id='900002'),
                       lambda v: v.update(expected_ean=None), lambda v: v.update(category='body_part')]:
            record = binding(); mutate(record); reseal(record)
            with self.assertRaisesRegex(RSUQuoteError, '^invalid_mapping_binding$'): self.provider(record)
        self.assertEqual(self.inquiry.calls, [])

    def test_duplicate_supplier_id_across_different_products_is_not_silently_selected(self):
        other = 'parts:' + '2' * 24
        records = {SID: binding(), other: binding(product_id=other, identity_hash='d' * 64)}
        with self.assertRaisesRegex(RSUQuoteError, '^duplicate_procurement_binding$'):
            RSUQuoteProvider(bindings=records, approved_ids=set(records),
                policy=RSUQuotePolicy('test', 'EUR', True, 'SYNTHETIC-policy'),
                inquiry=self.inquiry, clock=lambda: NOW)
        self.assertEqual(self.inquiry.calls, [])


class PartsFulfillmentChecks(unittest.TestCase):
    """Real temporary journal + synthetic adapters: accepted exact ID precedes capture."""
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start(); self.addCleanup(self.network.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'synthetic.sqlite'
        self.factory = lambda: sqlite3.connect(self.path, timeout=5)
        with closing(self.factory()) as db: init_schema(db); init_fulfillment_schema(db)
        self.now = NOW; self.quantities = {OIL: 2}; self.f = Fixture()
        self.f.current = lambda: {'confirmed': True, 'mode': 'test', 'currency': 'eur', 'vat_percent': '19.00',
            'observed_at': self.now.isoformat(), 'expires_at': (self.now + timedelta(minutes=10)).isoformat()}
        product = self.f.products[OIL]
        self.inquiry = FakeInquiry(lambda: self.now)
        candidate = binding(product_id=OIL, identity_hash=product['identity_hash'], category='oil')
        self.provider = RSUQuoteProvider(bindings={OIL: candidate}, approved_ids={OIL},
            policy=RSUQuotePolicy('test', 'EUR', True, 'SYNTHETIC-parts-net-unit-policy'),
            inquiry=self.inquiry, clock=lambda: self.now)
        self.f.mapping, self.f.retail = self.provider.rsu_mapping, self.provider.retail_quote
        self.rows = [line(OIL, 2)]; self.context = {'postcode': '74821', 'address_reference': 'SYNTHETIC_ONLY'}
        self.store = OrderStore(self.factory, clock=lambda: self.now)
        self.journal = FulfillmentJournal(self.factory, clock=lambda: self.now)
        quote = self.f.service_instance(clock=lambda: self.now).quote({'lines': self.rows},
            customer_id='customer-a', vehicle_ref='', delivery_context=self.context)
        self.assertTrue(quote.snapshot()['ready'])
        self.store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash,
            customer_id='customer-a', mode='test', delivery_context=self.context)
        self.order = self.store.create_order(quote.quote_hash, customer_id='customer-a', mode='test',
                                             idempotency_key='SYNTHETIC-parts-order')
        self.attempt = self.store.claim_payment(self.order['id'], customer_id='customer-a', mode='test')
        self.store.bind_payment(self.order['id'], self.attempt['id'], customer_id='customer-a', mode='test',
            session_id='cs_test_SYNTHETIC', intent_id='pi_SYNTHETIC',
            amount_gross_cent=self.order['amount_gross_cent'], currency='eur')
        self.store.apply_verified_event(self.event('authorized', 'evt_SYNTHETIC_AUTH'), verified=True)
        self.payment, self.transport = Payment(self), Transport(self)
        self.saga = self.make_saga()

    def binding(self):
        return {'order_id': self.order['id'], 'customer_id': 'customer-a', 'mode': 'test', 'currency': 'eur',
                'quote_hash': self.order['quote_hash'], 'amount_gross_cent': self.order['amount_gross_cent'],
                'payment_attempt': self.attempt['id'], 'session_id': 'cs_test_SYNTHETIC', 'intent_id': 'pi_SYNTHETIC'}

    def event(self, kind, identifier):
        return {'event_id': identifier, 'order_id': self.order['id'], 'attempt_id': self.attempt['id'],
                'kind': kind, 'mode': 'test', 'session_id': 'cs_test_SYNTHETIC', 'intent_id': 'pi_SYNTHETIC',
                'amount_gross_cent': self.order['amount_gross_cent'], 'currency': 'eur', 'occurred_at': self.now.isoformat()}

    def revalidate(self, snapshot, context, *, destinations):
        return self.f.service_instance(clock=lambda: self.now).quote({'lines': self.rows},
            customer_id='customer-a', vehicle_ref='', delivery_context=context).snapshot()

    def address(self, destination, context):
        return {'neutraldelivery': 1, 'salutation': 'Firma', 'name': 'SYNTHETIC customer',
                'street': 'Teststrasse 1', 'zipcode': '74821', 'city': 'Mosbach', 'countrycode': 'DE'}

    def make_saga(self):
        return ShopFulfillment(self.store, self.journal, self.payment, self.transport, mode='test',
            supplier_policy=POLICY, fresh_inquiry=self.provider.fresh_inquiry,
            revalidate=self.revalidate, delivery_address=self.address, enabled=True, clock=lambda: self.now)

    def test_exact_parts_id_ean_quantity_and_net_transmitted_once_before_capture(self):
        result = self.saga.run(self.order['id'], customer_id='customer-a')
        self.assertEqual(result['state'], 'captured')
        self.assertEqual(self.payment.actions, ['capture'])
        self.assertEqual(len(self.transport.calls), 1)
        payload = freeze_order_request(self.transport.calls[0]).as_dict()
        self.assertEqual(payload['positions'], [{'pos': 1, 'supplier_article_id': ARTICLE, 'ean': EAN,
                          'quantity': 2, 'unit_net': '50.0000', 'currency': 'EUR'}])
        self.assertEqual(payload['delivery_address']['neutraldelivery'], 1)
        self.assertTrue(all(call == (ARTICLE, 2) for call in self.inquiry.calls))
        self.saga = self.make_saga()
        self.assertEqual(self.saga.run(self.order['id'], customer_id='customer-a')['state'], 'captured')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.payment.actions, ['capture'])

    def test_wrong_fresh_variant_never_submits_or_captures(self):
        self.inquiry.changes = {'article_id': '900002'}
        result = self.saga.run(self.order['id'], customer_id='customer-a')
        self.assertEqual(result['state'], 'cancelled')
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.payment.actions, ['cancel'])

    def test_quantity_one_stock_cannot_fulfill_two_parts_or_capture_hold(self):
        self.inquiry.changes = {'stock': 1}
        result = self.saga.run(self.order['id'], customer_id='customer-a')
        self.assertEqual(result['state'], 'cancelled')
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.payment.actions, ['cancel'])

    def test_xml_boundary_still_rejects_missing_supplier_ean_or_extra_offer_field(self):
        payload = {'reference': 'SYNTHETIC-PARTS-EXACT-1', 'positions': [{'pos': 1,
            'supplier_article_id': ARTICLE, 'ean': EAN, 'quantity': 2, 'unit_net': '50', 'currency': 'EUR'}],
            'delivery_address': self.address('customer', self.context)}
        freeze_order_request(payload)
        for changes in ({'ean': None}, {'ean': ''}, {'supplier_offer': 'warehouse123'}):
            changed = deepcopy(payload); changed['positions'][0].update(changes)
            with self.assertRaises(ValueError): freeze_order_request(changed)
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.payment.actions, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
