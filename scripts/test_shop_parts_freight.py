"""Exact-source freight observations: synthetic reads only, no app or network."""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import json
import socket
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
import shop_parts_freight as freight
from shop_parts_freight import PartsRetailFreight, PartsFreightError
from shop_freight import MainlandDeliveryEligibility, RSURetailFreight
from shop_rsu_quotes import RSUQuoteProvider, RSUQuotePolicy
from test_shop_rsu_parts_evidence import binding, FakeInquiry, SID, IDENTITY, EAN, ARTICLE
from test_shop_cart_quotes import NOW, TYRE


class PartsFreightChecks(unittest.TestCase):
    def setUp(self):
        network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        network.start(); self.addCleanup(network.stop)
        self.now = NOW
        self.binding = binding()
        self.product = {'id': SID, 'identity_hash': IDENTITY, 'category': 'accessory'}
        self.raw_inquiry = FakeInquiry(lambda: self.now)
        self.provider = RSUQuoteProvider(bindings={SID: self.binding}, approved_ids={SID},
            policy=RSUQuotePolicy('test', 'EUR', True, 'SYNTHETIC-policy'), inquiry=self.raw_inquiry, clock=lambda: self.now)
        self.mapping = self.provider.rsu_mapping(self.product)
        self.references = [{'file': 'synthetic-ui.txt', 'sha256': 'd' * 64},
                           {'file': 'synthetic-inquiry.json', 'sha256': 'e' * 64}]
        identity = self.binding['mapping']['evidence']['rsu']['identity']
        self.proof = {'rule': 'rsu_ui_unique_q1_freight_v1', 'source_ref': self.references[0], 'inquiry_ref': self.references[1],
            'observed_at': (NOW - timedelta(minutes=2)).isoformat(), 'quantity': 1, 'offer_count': 1,
            'supplier_article_id': ARTICLE, 'ean': EAN, 'manufacturer': identity['manufacturer'], 'mpn': identity['mpn'],
            'unit_net': '50', 'stock_reported': 20, 'freight_net': '6', 'currency': 'EUR', 'mainland_only': True,
            'product_id': SID, 'identity_hash': IDENTITY, 'binding_evidence_id': self.binding['mapping']['evidence_id'],
            'inquiry_observed_at': (NOW - timedelta(minutes=1)).isoformat(),
            'expires_at': (NOW + timedelta(minutes=13)).isoformat(), 'retail_gross_cent': 821}
        self.seal(self.proof)
        self.context = {'delivery': {'salutation': 'Herr', 'recipient': 'Synthetic Receiver', 'street': 'Musterstrasse 1',
            'postcode': '74821', 'city': 'Mosbach', 'country': 'DE'}, 'delivery_mainland': True, 'workshop_delivery': 'collection'}
        self.eligibility = MainlandDeliveryEligibility({'74821'}, mainland_delivery_approved=True)
        self.inquiry_calls = []

    def seal(self, value):
        value.pop('proof_sha256', None); value['proof_sha256'] = freight._hash(value)

    def fresh(self, row, quantity):
        self.inquiry_calls.append((deepcopy(row), quantity))
        return self.provider.fresh_inquiry(row, quantity)

    def tariff(self, *, proof=None, **changes):
        args = dict(bindings={SID: self.binding}, mode='test', destination_eligibility=self.eligibility,
                    fresh_inquiry=self.fresh, source_refs=self.references, clock=lambda: self.now)
        args.update(changes)
        return PartsRetailFreight({SID: self.proof if proof is None else proof}, **args)

    def shipment(self, *, rows=None, destination='customer', context=None):
        context = self.context if context is None else context
        rows = [{'id': SID, 'identity_hash': IDENTITY, 'quantity': 1,
                 'mapping': {k: self.mapping[k] for k in ('supplier_article_id', 'evidence_id')}}] if rows is None else rows
        value = {'id': destination, 'destination': destination, 'private_items': rows,
                 'items': [{'id': row['id'], 'quantity': row['quantity']} for row in rows], 'context_hash': freight._hash(context)}
        value['shipment_hash'] = freight._hash({k: value[k] for k in ('id', 'destination', 'private_items', 'context_hash')})
        return value

    def test_constructor_does_not_call_read_and_preserves_exact_proof_expiry(self):
        tariff = self.tariff(); self.assertEqual(self.inquiry_calls, []); self.assertEqual(self.raw_inquiry.calls, [])
        result = tariff(self.shipment(), self.context)
        self.assertEqual(result['gross_cent'], 821)  # 6 EUR * 1.15 * 1.19, once half-up
        self.assertEqual(result['expires_at'], self.proof['expires_at'])
        self.assertEqual(result['observed_at'], self.now.isoformat())
        self.assertEqual(result['shipment_hash'], self.shipment()['shipment_hash'])
        self.assertEqual(self.raw_inquiry.calls, [(ARTICLE, 1)])
        for private in ('unit_net', 'freight_net', 'supplier_article_id', 'source_ref', 'proof_sha256'):
            self.assertNotIn(private, json.dumps(result))

    def test_original_ui_and_inquiry_dates_never_rebase_on_current_rest_success(self):
        tariff = self.tariff(); self.now += timedelta(minutes=12)
        result = tariff(self.shipment(), self.context)
        self.assertEqual(result['expires_at'], self.proof['expires_at'])
        self.now += timedelta(minutes=1)
        self.assertIsNone(tariff(self.shipment(), self.context))
        self.assertEqual(self.raw_inquiry.calls, [(ARTICLE, 1)])

    def test_unknown_or_duplicate_source_references_and_forged_record_hash_rejected(self):
        for changes in ({'source_refs': []}, {'source_refs': self.references * 2},
                        {'source_refs': [self.references[0], {**self.references[1], 'sha256': 'f' * 64}]}):
            with self.subTest(changes=changes), self.assertRaises(PartsFreightError): self.tariff(**changes)
        proof = deepcopy(self.proof); proof['proof_sha256'] = 'f' * 64
        with self.assertRaises(PartsFreightError): self.tariff(proof=proof)
        proof = deepcopy(self.proof); proof['inquiry_ref'] = proof['source_ref']; self.seal(proof)
        with self.assertRaises(PartsFreightError): self.tariff(proof=proof)

    def test_exact_candidate_and_provider_mapping_ids_are_independent(self):
        self.assertNotEqual(self.proof['binding_evidence_id'], self.mapping['evidence_id'])
        proof = deepcopy(self.proof); proof['binding_evidence_id'] = self.mapping['evidence_id']; self.seal(proof)
        with self.assertRaises(PartsFreightError): self.tariff(proof=proof)
        shipment = self.shipment(); shipment['private_items'][0]['mapping']['evidence_id'] = self.proof['binding_evidence_id']
        shipment = self.shipment(rows=shipment['private_items'])
        self.assertIsNone(self.tariff()(shipment, self.context)); self.assertEqual(self.inquiry_calls, [])

    def test_bad_quantity_offer_identity_mainland_currency_and_extra_fields_fail(self):
        for changes in ({'quantity': 2}, {'quantity': True}, {'offer_count': 2}, {'stock_reported': 0},
                        {'mainland_only': 1}, {'currency': 'eur'}, {'supplier_article_id': '900002'},
                        {'ean': '4046283759835'}, {'manufacturer': 'another maker'}, {'mpn': 'OTHER'},
                        {'identity_hash': 'f' * 64}, {'supplier_offer': 'warehouse-1'}):
            proof = {**deepcopy(self.proof), **changes}; self.seal(proof)
            with self.subTest(changes=changes), self.assertRaises(PartsFreightError): self.tariff(proof=proof)

    def test_missing_price_not_zero_but_explicit_confirmed_zero_freight_is_allowed(self):
        for invalid in (None, '', 'unknown', False, '-1'):
            proof = deepcopy(self.proof); proof['freight_net'] = invalid; self.seal(proof)
            with self.assertRaises(PartsFreightError): self.tariff(proof=proof)
        proof = deepcopy(self.proof); proof.update(freight_net='0', retail_gross_cent=0); self.seal(proof)
        self.assertEqual(self.tariff(proof=proof)(self.shipment(), self.context)['gross_cent'], 0)

    def test_deadlines_are_bounded_by_both_observations_and_future_records_do_not_quote(self):
        proof = deepcopy(self.proof); proof['expires_at'] = (NOW + timedelta(minutes=14)).isoformat(); self.seal(proof)
        with self.assertRaises(PartsFreightError): self.tariff(proof=proof)
        proof = deepcopy(self.proof); proof['observed_at'] = (NOW + timedelta(minutes=1)).isoformat(); self.seal(proof)
        self.assertIsNone(self.tariff(proof=proof)(self.shipment(), self.context))
        self.assertEqual(self.inquiry_calls, [])

    def test_every_actual_quantity_not_one_fails_before_any_fresh_read(self):
        for quantity in (2, 4, 20):
            rows = self.shipment()['private_items']; rows[0]['quantity'] = quantity
            self.assertIsNone(self.tariff()(self.shipment(rows=rows), self.context))
        self.assertEqual(self.inquiry_calls, [])

    def test_unknown_part_hash_address_or_duplicate_item_never_gets_partial_quote(self):
        for changes in ({'id': 'parts:' + '2' * 24}, {'identity_hash': 'f' * 64}):
            rows = self.shipment()['private_items']; rows[0].update(changes)
            self.assertIsNone(self.tariff()(self.shipment(rows=rows), self.context))
        rows = self.shipment()['private_items'] * 2
        self.assertIsNone(self.tariff()(self.shipment(rows=rows), self.context))
        changed = deepcopy(self.context); changed['delivery']['street'] = 'Changed Street 2'
        self.assertIsNone(self.tariff()(self.shipment(), changed))
        self.assertEqual(self.inquiry_calls, [])

    def test_island_foreign_packstation_and_undocumented_reshipment_are_rejected(self):
        for change in ({'postcode': '25980'}, {'country': 'AT'}, {'street': 'Packstation 123'}):
            context = deepcopy(self.context); context['delivery'].update(change)
            self.assertIsNone(self.tariff()(self.shipment(context=context), context))
        context = deepcopy(self.context); context['workshop_delivery'] = 'customer_reshipment'
        self.assertIsNone(self.tariff()(self.shipment(destination='workshop', context=context), context))
        self.assertEqual(self.inquiry_calls, [])
        self.assertEqual(self.tariff()(self.shipment(destination='workshop'), self.context)['gross_cent'], 821)

    def test_changed_fresh_stock_price_sku_ean_mode_or_time_never_confirms_freight(self):
        for change in ({'stock': 0}, {'stock': 19}, {'price': Decimal('51')}, {'article_id': '900002'},
                       {'ean': '4046283759835'}, {'amount': 2}, {'configured_mode': 'live'},
                       {'retrieved_at': NOW - timedelta(seconds=1)}):
            self.raw_inquiry.changes = change
            self.assertIsNone(self.tariff()(self.shipment(), self.context), change)

    def test_fresh_callback_exceptions_and_slow_reads_do_not_leak_or_extend_deadline(self):
        def fail(row, quantity): raise RuntimeError('PRIVATE_CANARY')
        self.assertIsNone(self.tariff(fresh_inquiry=fail)(self.shipment(), self.context))
        def slow(row, quantity):
            value = self.fresh(row, quantity); self.now += timedelta(minutes=14); return value
        self.assertIsNone(self.tariff(fresh_inquiry=slow)(self.shipment(), self.context))

    def test_even_an_injected_read_cannot_forge_the_typed_fresh_binding(self):
        row = self.shipment()['private_items'][0]
        valid = self.provider.fresh_inquiry(row, 1)
        for change in ({'mapping_evidence_id': 'f' * 64}, {'expected_ean': '4046283759835'},
                       {'identity_hash': 'f' * 64}, {'quantity': True}, {'product_id': 'parts:' + '2' * 24},
                       {'mode': 'live'}, {'currency': 'eur'}, {'confirmed': 1},
                       {'observed_at': (NOW + timedelta(seconds=1)).isoformat()},
                       {'expires_at': (NOW + timedelta(minutes=16)).isoformat()}):
            result = {**deepcopy(valid), **change}
            tariff = self.tariff(fresh_inquiry=lambda *args: deepcopy(result))
            self.assertIsNone(tariff(self.shipment(), self.context), change)

    def test_mixed_fallback_uses_rehashed_subset_and_returns_only_full_sum(self):
        fallback = RSURetailFreight({TYRE: 'pkw_tyre'}, mode='test', basis='net', evidence_id='SYNTHETIC-tariff',
            checked_at=NOW.isoformat(), destination_eligibility=self.eligibility, clock=lambda: self.now)
        rows = self.shipment()['private_items'] + [{'id': TYRE, 'identity_hash': 'c' * 64, 'quantity': 1,
            'mapping': {'supplier_article_id': '123', 'evidence_id': 'f' * 64}}]
        shipment = self.shipment(rows=rows)
        self.assertEqual(self.tariff(fallback=fallback)(shipment, self.context)['gross_cent'], 1642)
        rows[-1]['id'] = 'tyre:' + 'f' * 24
        self.assertIsNone(self.tariff(fallback=fallback)(self.shipment(rows=rows), self.context))
        self.assertIsNone(self.tariff(fallback=lambda *args: None)(shipment, self.context))

    def test_nonparts_fallback_policy_is_not_replaced_by_parts_amount(self):
        fallback = RSURetailFreight({TYRE: 'pkw_tyre'}, mode='test', basis='net', evidence_id='SYNTHETIC-tariff',
            checked_at=NOW.isoformat(), destination_eligibility=self.eligibility, clock=lambda: self.now)
        rows = [{'id': TYRE, 'identity_hash': 'c' * 64, 'quantity': 4,
                 'mapping': {'supplier_article_id': '123', 'evidence_id': 'f' * 64}}]
        self.assertEqual(self.tariff(fallback=fallback)(self.shipment(rows=rows), self.context)['gross_cent'], 0)
        self.assertEqual(self.inquiry_calls, [])
        rows[0]['quantity'] = 1
        self.assertEqual(self.tariff(fallback=fallback)(self.shipment(rows=rows), self.context)['gross_cent'], 821)

    def test_original_proof_and_binding_mutation_after_construction_cannot_change_freight(self):
        tariff = self.tariff(); self.proof['retail_gross_cent'] = 0
        self.binding['mapping']['evidence']['rsu']['article_id'] = '900002'
        result = tariff(self.shipment(), self.context)
        self.assertEqual(result['gross_cent'], 821)

    def test_retained_opt_in_uses_already_validated_retail_read_without_extra_network(self):
        self.provider.retail_quote(self.product, 1)
        self.assertEqual(self.raw_inquiry.calls, [(ARTICLE, 1)])
        self.now += timedelta(minutes=1)
        default = self.tariff(fresh_inquiry=self.provider.retained_inquiry)
        self.assertIsNone(default(self.shipment(), self.context))  # default requires a new observation
        retained = self.tariff(fresh_inquiry=self.provider.retained_inquiry, allow_retained_inquiry=True)
        result = retained(self.shipment(), self.context)
        self.assertEqual(result['gross_cent'], 821)
        self.assertEqual(result['expires_at'], self.proof['expires_at'])
        self.assertEqual(self.raw_inquiry.calls, [(ARTICLE, 1)])
        for invalid in ('true', 1, None):
            with self.assertRaises(PartsFreightError): self.tariff(allow_retained_inquiry=invalid)

    def test_failed_or_changed_saga_read_invalidates_retained_shipping_confirmation(self):
        self.provider.retail_quote(self.product, 1)
        tariff = self.tariff(fresh_inquiry=self.provider.retained_inquiry, allow_retained_inquiry=True)
        self.assertIsNotNone(tariff(self.shipment(), self.context))
        self.raw_inquiry.changes = {'price': Decimal('51')}
        self.assertIsNotNone(self.provider.fresh_inquiry(self.shipment()['private_items'][0], 1))
        self.assertIsNone(tariff(self.shipment(), self.context))
        self.raw_inquiry.changes = {'stock': 0}
        self.assertIsNone(self.provider.fresh_inquiry(self.shipment()['private_items'][0], 1))
        self.assertIsNone(tariff(self.shipment(), self.context))
        self.assertEqual(len(self.raw_inquiry.calls), 3)  # freight itself did not add reads


if __name__ == '__main__':
    unittest.main(verbosity=2)
