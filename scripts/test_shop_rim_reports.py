"""Synthetic exact-variant report release; no file/provider/network activity."""
from copy import deepcopy
from pathlib import Path
import json
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_rim_reports import (RimReportError, bind_provider_report, public_rim_documents,
                              rim_report_asset, rim_report_key, _hash)
from shop_catalog_public import variant_fingerprint

ID = 'alloy_wheel:' + 'a' * 24
URL = 'https://media3.tyre-shopping.com/public/technical_reports/24/RA-123~FO_8519.pdf'
GEOMETRY = {'width': '8.5', 'diameter': '19', 'offset': '37',
            'bolt_count': '5', 'pcd': '108', 'centre_bore': '72.5'}


def fixture():
    row = {'selection_id': ID, 'category': 'alloy_wheel', 'name': 'Synthetic exact rim',
           'manufacturer_name': 'SYNTHETIC', **GEOMETRY,
           'identifiers': {'provider': 'ALZURA', 'provider_id': '12345'}}
    binding = bind_provider_report(row, provider_id='12345', requested_car_id=999,
        source_body_sha256='b' * 64, geometry=GEOMETRY,
        report={'url': URL, 'report_number': 'SYNTHETIC report', 'type_description': 'Type X'})
    record = {'identity_hash': variant_fingerprint(row), 'rim_report_bindings': [binding]}
    urls = {rim_report_key(URL): '/shop/documents/' + rim_report_asset(URL)}
    return row, record, urls


class RimReportChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('No network'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_exact_original_report_is_opaque_and_never_vehicle_approval(self):
        row, record, urls = fixture()
        record.update(confirmed=True, vehicle_approved=True, private='PRIVATE_CANARY')
        result = public_rim_documents(row, record, urls)
        self.assertEqual(result['rim_report_urls'], list(urls.values()))
        self.assertEqual(result['rim_report_status'], 'original_documents_unreviewed')
        self.assertEqual(result['rim_reports'][0]['scope'], 'provider_document_not_vehicle_approval')
        text = json.dumps(result)
        for secret in ('PRIVATE_CANARY', 'media3', '12345', 'requested_car_id', 'source_body', 'geometry', 'confirmed'):
            self.assertNotIn(secret, text)
        self.assertNotIn('fitment', result)

    def test_only_observed_original_cdn_paths_are_accepted(self):
        for host in ('media1', 'media5'):
            self.assertTrue(rim_report_asset(URL.replace('media3', host)).endswith('.pdf'))
        self.assertTrue(rim_report_asset(URL[:-3] + 'PDF').endswith('.pdf'))
        for url in (URL.replace('https:', 'http:'), URL.replace('media3', 'media6'),
                    URL.replace('.com/', '.com:443/'), URL.replace('media3.', 'user:secret@media3.'),
                    URL.replace('/public/', '/elsewhere/'), URL + '?', URL + '#',
                    URL.replace('RA-123', '..'), URL.replace('RA-123', 'a%2Fb'),
                    URL.replace('RA-123', 'a%252eb'), URL.replace('.pdf', '.html')):
            with self.subTest(url=url), self.assertRaises(RimReportError):
                rim_report_asset(url)

    def test_identity_source_and_known_geometry_are_required(self):
        row, _, _ = fixture()
        for change in ({'provider_id': '54321'}, {'requested_car_id': True},
                       {'requested_car_id': 0}, {'source_body_sha256': 'x'},
                       {'geometry': {**GEOMETRY, 'pcd': '112'}}, {'geometry': {'width': '8.5'}}):
            args = dict(provider_id='12345', requested_car_id=999, source_body_sha256='b' * 64,
                        geometry=GEOMETRY, report={'url': URL})
            with self.subTest(change=change), self.assertRaises(RimReportError):
                bind_provider_report(row, **{**args, **change})

    def test_substituted_row_or_even_rehashed_proof_is_rejected(self):
        row, record, urls = fixture()
        self.assertIsNone(public_rim_documents({**row, 'offset': '40'}, record, urls))
        for field, value in (('product_id', 'alloy_wheel:' + 'c' * 24),
                             ('identity_hash', 'c' * 64), ('provider_id', '22222'),
                             ('geometry', {**GEOMETRY, 'diameter': '18'}), ('rule', 'guessed-fitment')):
            bad = deepcopy(record)
            proof = bad['rim_report_bindings'][0]['proof']
            proof[field] = value
            bad['rim_report_bindings'][0]['proof_sha256'] = _hash(proof)
            with self.subTest(field=field):
                self.assertIsNone(public_rim_documents(row, bad, urls))

    def test_missing_or_substituted_publication_never_exposes_remote_url(self):
        row, record, urls = fixture()
        for wrong in ({}, {rim_report_key(URL): 'https://evil.test/report.pdf'},
                      {rim_report_key(URL): '/shop/documents/' + 'f' * 64 + '.pdf'}):
            self.assertIsNone(public_rim_documents(row, record, wrong))

    def test_unknown_old_fields_do_not_become_invented_geometry(self):
        row, _, _ = fixture()
        row.update(offset=None, bolt_count=None, pcd=None, centre_bore=None)
        binding = bind_provider_report(row, provider_id='12345', requested_car_id=999,
            source_body_sha256='b' * 64, geometry=GEOMETRY, report={'url': URL})
        record = {'identity_hash': variant_fingerprint(row), 'rim_report_bindings': [binding]}
        self.assertIsNotNone(public_rim_documents(row, record,
            {rim_report_key(URL): '/shop/documents/' + rim_report_asset(URL)}))
        self.assertIsNone(row['centre_bore'])

    def test_repeated_document_for_different_requested_cars_is_deduplicated(self):
        row, record, urls = fixture()
        duplicate = deepcopy(record['rim_report_bindings'][0])
        duplicate['proof']['requested_car_id'] = 1000
        duplicate['proof_sha256'] = _hash(duplicate['proof'])
        record['rim_report_bindings'].append(duplicate)
        self.assertEqual(len(public_rim_documents(row, record, urls)['rim_reports']), 1)

    def test_malformed_and_unbounded_records_fail_closed(self):
        row, record, urls = fixture()
        for bad in (None, {}, {**record, 'rim_report_bindings': []},
                    {**record, 'rim_report_bindings': record['rim_report_bindings'] * 49}):
            self.assertIsNone(public_rim_documents(row, bad, urls))
        changed = deepcopy(record)
        changed['rim_report_bindings'][0]['proof']['unexpected'] = True
        self.assertIsNone(public_rim_documents(row, changed, urls))
        changed = deepcopy(record)
        changed['rim_report_bindings'][0]['proof_sha256'] = '0' * 64
        self.assertIsNone(public_rim_documents(row, changed, urls))


if __name__ == '__main__':
    unittest.main()
