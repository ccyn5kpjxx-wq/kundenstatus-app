"""Pinned synthetic licensed source facts and typed quote boundary, no app/IO."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
from shop_parts_fitment import PartsVehicleFitments, PartsFitmentError
from test_shop_cart_quotes import Fixture, BODY, OIL, RIM, TYRE, line

REF, BASE_SHA = 'alzura-vehicle:120604', 'f' * 64


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def artifact(identity):
    vehicle = {'carId': 120604, 'vehicleDetails': {'carId': 120604, 'typeNumber': 120604, 'manuId': 5,
        'modId': 10253, 'manuName': 'Synthetic Audi', 'modelName': 'Synthetic A3', 'typeName': '1.0 TFSI',
        'yearOfConstrFrom': 201607, 'yearOfConstrTo': 201712, 'powerKwFrom': 85, 'powerKwTo': 85},
        'kbaNumbers': {'array': [{'kbaNo': '0588BHT'}]}, 'motorCodes': {'array': [{'motorCode': 'CHZD'}]}}
    proof = {'product_id': BODY, 'identity_hash': identity, 'car_id': 120604, 'manufacturer_id': 5,
        'model_id': 10253, 'manufacturer': 'SYNTHETIC PART', 'mpn': 'PANEL-1',
        'native_provider_id': 'wearparts:123-' + 'PANEL-1'.encode().hex() + ':123',
        'scope': 'provider_vehicle_catalog_association', 'full_vehicle_fitment_approved': False, 'purchase_enabled': False,
        'article_source': {'file': 'synthetic/articles.body', 'sha256': 'a' * 64, 'row_index': 0},
        'query_evidence': {'file': 'synthetic/plan.json', 'sha256': 'b' * 64, 'request_index': 0},
        'source_query': {'assemblyGroupNodeId': 16, 'carId': 120604, 'linkingTargetType': 'P',
                         'manufacturerId': 5, 'modelId': 10253, 'page': 0, 'showPerPage': 25},
        'vehicle_source': {'file': 'synthetic/vehicles.body', 'sha256': 'c' * 64, 'row_index': 0, 'vehicle': vehicle},
        'article_attributes': {'array': []}, 'article_info': {'array': []}, 'certification_compulsory': False}
    return {'schema_version': 1, 'bundle_sha256': BASE_SHA, 'associations': 1,
            'products_with_source_query_association': 1, 'proofs': [proof]}


class PartsFitmentChecks(unittest.TestCase):
    def setUp(self):
        network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        network.start(); self.addCleanup(network.stop)
        self.fixture = Fixture(); self.product = self.fixture.products[BODY]
        self.data = artifact(self.product['identity_hash'])
        self.products = {BODY: {'identity_hash': self.product['identity_hash'], 'category': 'body_part'}}

    def resolver(self, data=None, *, reviews=(), certificates=(), **changes):
        data = self.data if data is None else data; payload = encode(data)
        args = dict(artifact_sha256=sha256(payload).hexdigest(), base_bundle_sha256=BASE_SHA,
                    products=self.products, approved_conditions=reviews, approved_certificate_hashes=certificates)
        args.update(changes)
        return PartsVehicleFitments(payload, **args)

    def review(self, *, data=None):
        requirement = self.resolver(data).review_requirements(BODY, REF)
        keys = ('product_id', 'identity_hash', 'vehicle_ref', 'association_hash', 'conditions_hash', 'vehicle_details_hash')
        return {**{k: requirement[k] for k in keys}, 'conditions_reviewed': True, 'vehicle_fitment_reviewed': True,
                'unresolved_conditions': [], 'certificate_refs': [], 'review_evidence_id': 'SYNTHETIC-original-conditions-review'}

    def test_source_association_is_not_an_approval_or_freeform_vehicle_proof(self):
        resolver = self.resolver()
        self.assertTrue(resolver.requires_vehicle(self.product))
        for ref in (REF, 'Audi', 'Audi A3 2016', 'CAR-1', 'alzura-vehicle:0120604', 'alzura-vehicle:117083', '', None):
            self.assertIsNone(resolver.lookup(self.product, ref))
        self.assertIsNone(resolver.lookup(self.fixture.products[OIL], REF))

    def test_exact_review_yields_only_hash_bound_existing_fitment_shape(self):
        resolver = self.resolver(reviews=[self.review()]); proof = resolver.lookup(self.product, REF)
        self.assertEqual(set(proof), {'confirmed', 'identity_hash', 'vehicle_ref', 'evidence_id'})
        self.assertIs(proof['confirmed'], True)
        self.assertEqual(proof['identity_hash'], self.product['identity_hash'])
        self.assertEqual(len(proof['evidence_id']), 64)
        for change in ({'id': OIL}, {'identity_hash': 'd' * 64}, {'category': 'oil'}):
            self.assertIsNone(resolver.lookup({**self.product, **change}, REF))
        proof['confirmed'] = False
        self.assertTrue(resolver.lookup(self.product, REF)['confirmed'])

    def test_public_options_preserve_month_engine_kba_and_return_copies(self):
        resolver = self.resolver(); options = resolver.vehicles()
        self.assertEqual(options[0]['vehicle_ref'], REF)
        self.assertEqual((options[0]['date_from_yyyymm'], options[0]['date_to_yyyymm']), (201607, 201712))
        self.assertEqual(options[0]['engine_codes'], ['CHZD'])
        self.assertEqual(options[0]['hsn_tsn'], ['0588BHT'])
        self.assertNotIn('file', json.dumps(options))
        options[0]['engine_codes'].append('WRONG')
        self.assertEqual(resolver.vehicles()[0]['engine_codes'], ['CHZD'])

    def test_source_hash_base_bundle_and_current_product_identity_are_mandatory(self):
        for changes in ({'artifact_sha256': 'a' * 64}, {'base_bundle_sha256': 'e' * 64},
                        {'products': {BODY: {'identity_hash': 'd' * 64, 'category': 'body_part'}}}):
            with self.subTest(changes=changes), self.assertRaises(PartsFitmentError): self.resolver(**changes)

    def test_query_car_engine_month_native_part_and_source_paths_cannot_drift(self):
        mutations = [lambda p: p['source_query'].update(carId=117083),
            lambda p: p['source_query'].update(modelId=11611),
            lambda p: p['source_query'].update(carId=True),
            lambda p: p['vehicle_source']['vehicle']['vehicleDetails'].update(typeNumber=117083),
            lambda p: p['vehicle_source']['vehicle']['vehicleDetails'].update(yearOfConstrFrom=201613),
            lambda p: p['vehicle_source']['vehicle']['motorCodes'].update(array=[]),
            lambda p: p['vehicle_source']['vehicle']['kbaNumbers'].update(array=[{'kbaNo': '0588'}]),
            lambda p: p.update(native_provider_id='wearparts:123-50414e454c2d32:123'),
            lambda p: p['article_source'].update(file='../outside.body')]
        for mutate in mutations:
            data = deepcopy(self.data); mutate(data['proofs'][0])
            with self.subTest(mutation=mutate), self.assertRaises(PartsFitmentError): self.resolver(data)

    def test_changed_article_conditions_cannot_reuse_prior_review(self):
        reviewed = self.review()
        for field, value in [('article_info', {'array': [{'infoText': 'Only PR-123'}]}),
                             ('article_attributes', {'array': [{'attrId': 1197, 'attrValue': '7X1'}]}),
                             ('certification_compulsory', True)]:
            data = deepcopy(self.data); data['proofs'][0][field] = value
            with self.subTest(field=field), self.assertRaises(PartsFitmentError): self.resolver(data, reviews=[reviewed])
        data = deepcopy(self.data)
        data['proofs'][0]['vehicle_source']['vehicle']['motorCodes']['array'][0]['motorCode'] = 'DIFFERENT'
        with self.assertRaises(PartsFitmentError): self.resolver(data, reviews=[reviewed])

    def test_unresolved_conditions_boolean_shortcuts_and_extra_keys_cannot_approve(self):
        for changes in ({'conditions_reviewed': False}, {'vehicle_fitment_reviewed': 1},
                        {'unresolved_conditions': ['PR code unknown']}, {'conditions_hash': 'd' * 64},
                        {'vehicle_ref': 'Audi A3'}, {'extra': True}):
            review = {**self.review(), **changes}
            with self.subTest(changes=changes), self.assertRaises(PartsFitmentError): self.resolver(reviews=[review])
        with self.assertRaises(PartsFitmentError): self.resolver(reviews=[self.review(), self.review()])

    def test_compulsory_certificate_requires_genuinely_supplied_reviewed_document_hash(self):
        data = deepcopy(self.data); data['proofs'][0]['certification_compulsory'] = True
        review = self.review(data=data)
        with self.assertRaises(PartsFitmentError): self.resolver(data, reviews=[review])
        review['certificate_refs'] = ['d' * 64]
        with self.assertRaises(PartsFitmentError): self.resolver(data, reviews=[review])
        self.assertTrue(self.resolver(data, reviews=[review], certificates={'d' * 64}).lookup(self.product, REF)['confirmed'])

    def test_duplicate_association_and_json_keys_fail_closed(self):
        data = deepcopy(self.data); data['proofs'].append(deepcopy(data['proofs'][0])); data['associations'] = 2
        with self.assertRaises(PartsFitmentError): self.resolver(data)
        payload = b'{"schema_version":1,"schema_version":1}'
        with self.assertRaises(PartsFitmentError):
            PartsVehicleFitments(payload, artifact_sha256=sha256(payload).hexdigest(),
                                 base_bundle_sha256=BASE_SHA, products=self.products)

    def test_default_quote_behavior_is_unchanged_and_optional_lookup_is_authoritative(self):
        ordinary = self.fixture.quote([line(BODY)])
        self.assertTrue(ordinary.snapshot()['ready'])
        denied = self.fixture.quote([line(BODY)], fitment_lookup=self.resolver().lookup)
        self.assertFalse(denied.snapshot()['ready'])
        self.assertIn('fitment_unconfirmed', {r['code'] for r in denied.public()['blocked_reasons']})
        for invalid in (True, {}, 1):
            with self.assertRaisesRegex(ValueError, 'invalid_fitment_lookup'):
                self.fixture.service_instance(fitment_lookup=invalid)

    def test_verified_selected_vehicle_is_frozen_in_quote_without_source_payload(self):
        resolver = self.resolver(reviews=[self.review()])
        self.fixture.products[BODY]['fitment'] = None
        quote = self.fixture.service_instance(fitment_lookup=resolver.lookup).quote({'lines': [line(BODY)]},
            customer_id=23, vehicle_ref=REF, delivery_context={'postcode': '74821'})
        snapshot = quote.snapshot()
        self.assertTrue(snapshot['ready'])
        self.assertEqual(snapshot['vehicle_ref'], REF)
        self.assertEqual(snapshot['product_checks'][BODY]['fitment_evidence_id'], resolver.lookup(self.product, REF)['evidence_id'])
        for private in ('synthetic/articles.body', 'article_attributes', 'article_info'):
            self.assertNotIn(private, json.dumps(snapshot))

    def test_vehicle_specific_accessory_uses_lookup_but_universal_oil_and_wheels_do_not(self):
        called = []
        def deny(product, ref):
            called.append((product['id'], ref)); return None
        self.fixture.quote([line(OIL)], fitment_lookup=deny)
        self.fixture.quote([line(TYRE)], fitment_lookup=deny)
        self.fixture.quote([line(RIM)], fitment_lookup=deny)
        self.assertEqual(called, [])
        self.fixture.products[OIL]['category'] = 'accessory'
        self.fixture.products[OIL]['fitment_required'] = True
        quote = self.fixture.quote([line(OIL)], fitment_lookup=deny)
        self.assertEqual(called, [(OIL, 'CAR-1')])
        self.assertIn('fitment_unconfirmed', {r['code'] for r in quote.public()['blocked_reasons']})

    def test_callback_cannot_mutate_server_product_and_errors_are_private(self):
        original = deepcopy(self.fixture.products[BODY])
        def failing(product, ref):
            product['identity_hash'] = 'd' * 64
            raise RuntimeError('PRIVATE_CANARY')
        quote = self.fixture.quote([line(BODY)], fitment_lookup=failing)
        self.assertEqual(self.fixture.products[BODY], original)
        self.assertFalse(quote.public()['ready'])
        self.assertNotIn('PRIVATE_CANARY', json.dumps(quote.snapshot()))


if __name__ == '__main__':
    unittest.main(verbosity=2)
