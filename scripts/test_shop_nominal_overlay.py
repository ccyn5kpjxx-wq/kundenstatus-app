"""Pinned private extension and known missing-document checkout boundaries."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import base64
import json
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shop_nominal_overlay import (NominalOverlayError, load_nominal_tyre_overlay,
                                  unavailable_document_product_ids)
from shop_catalog_public import variant_fingerprint
from shop_documents import document_asset
from test_shop_released_width_pairs import fixture, rehash
from test_shop_catalog_enriched import pair, tyre
import test_shop_production as production_fixtures


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


class NominalOverlayChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network'))
        self.network.start(); self.addCleanup(self.network.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        first = fixture(index=1)
        self.extra = dict(first['nominal_tyre_sources'])
        self.bundle = {**first, 'nominal_tyre_sources': {}, 'rsu_price_seeds': {'records': {}}}
        for index in range(2, 38):
            other = fixture(index=index)
            self.bundle['products'].append(other['products'][0])
            self.bundle['rsu_bindings'].update(other['rsu_bindings'])
            self.extra.update(other['nominal_tyre_sources'])
        self.bundle['rsu_price_seeds']['records'] = {pid: {'synthetic_price_unchanged': 12345} for pid in self.extra}
        self.base_sha = sha256(encoded(self.bundle)).hexdigest()
        self.overlay = {'schema_version': 1, 'base_bundle_sha256': self.base_sha,
            'nominal_tyre_sources': self.extra, 'sources': {'alzura_archive_sha256': 'a'*64,
                'old_rsu_csv_sha256': 'b'*64, 'new_rsu_csv_sha256': 'c'*64}}
        self.record = self.save()

    def save(self, raw=None):
        raw = encoded(self.overlay) if raw is None else raw
        (self.root / 'nominal.private.json').write_bytes(raw)
        return {'path': 'nominal.private.json', 'sha256': sha256(raw).hexdigest(), 'base_bundle_sha256': self.base_sha}

    def load(self, record=None, bundle=None):
        return load_nominal_tyre_overlay(self.root, self.record if record is None else record,
            self.bundle if bundle is None else bundle, base_bundle_sha256=self.base_sha)

    def test_only_nominal_sources_are_merged_in_ram_and_legacy_absence_is_inert(self):
        before = deepcopy(self.bundle)
        merged = self.load()
        self.assertEqual(self.bundle, before)
        self.assertEqual(set(merged['nominal_tyre_sources']), set(self.extra))
        for key in self.bundle:
            if key != 'nominal_tyre_sources':
                self.assertIs(merged[key], self.bundle[key])
        self.assertEqual(merged['rsu_price_seeds'], before['rsu_price_seeds'])
        self.assertIs(load_nominal_tyre_overlay(self.root, None, self.bundle, base_bundle_sha256=self.base_sha), self.bundle)

    def test_hash_base_descriptor_shape_and_source_receipts_fail_closed(self):
        for altered in ({**self.record, 'sha256': '0'*64}, {**self.record, 'base_bundle_sha256': '1'*64},
                        {**self.record, 'file': 'unexpected.json'}, {**self.record, 'path': '../escape.json'},
                        {**self.record, 'path': str(self.root / 'nominal.private.json')}):
            with self.subTest(record=altered), self.assertRaises(NominalOverlayError):
                self.load(altered)
        self.overlay['sources']['alzura_archive_sha256'] = 'd'*64
        with self.assertRaises(NominalOverlayError): self.load(self.save())

    def test_exact_37_bound_seeded_disjoint_rows_are_required(self):
        identifier = next(iter(self.extra))
        for key in ('rsu_bindings', 'rsu_price_seeds'):
            changed = deepcopy(self.bundle)
            target = changed[key]['records'] if key == 'rsu_price_seeds' else changed[key]
            target.pop(identifier)
            with self.subTest(key=key), self.assertRaises(NominalOverlayError): self.load(bundle=changed)
        changed = deepcopy(self.bundle)
        changed['nominal_tyre_sources'][identifier] = self.extra[identifier]
        with self.assertRaises(NominalOverlayError): self.load(bundle=changed)
        self.overlay['nominal_tyre_sources'] = dict(list(self.extra.items())[1:])
        with self.assertRaises(NominalOverlayError): self.load(self.save())

    def test_rehashed_wrong_original_row_or_special_execution_cannot_be_released(self):
        identifier = next(iter(self.extra))
        self.overlay['nominal_tyre_sources'][identifier]['proof']['alzura_row_number'] += 1
        rehash(self.overlay, identifier)
        with self.assertRaises(NominalOverlayError): self.load(self.save())
        self.overlay['nominal_tyre_sources'][identifier]['proof']['alzura_row_number'] -= 1
        self.overlay['nominal_tyre_sources'][identifier]['proof']['info'] = 'SSR'
        rehash(self.overlay, identifier)
        with self.assertRaises(NominalOverlayError): self.load(self.save())

    def test_size_duplicate_nonfinite_and_unexpected_payload_fields_are_rejected(self):
        for raw in (b' ' * 65537, b'{"schema_version":1,"schema_version":1}', b'{"x":NaN}'):
            with self.subTest(size=len(raw)), self.assertRaises(NominalOverlayError): self.load(self.save(raw))
        self.overlay['rsu_price_seeds'] = {'fake': 1}
        with self.assertRaises(NominalOverlayError): self.load(self.save())

    def test_symlink_overlay_is_rejected(self):
        link = self.root / 'link.json'
        try:
            link.symlink_to(self.root / 'nominal.private.json')
        except OSError:
            self.skipTest('Platform does not grant synthetic symlink creation')
        with self.assertRaises(NominalOverlayError): self.load({**self.record, 'path': 'link.json'})

    def test_known_missing_document_must_be_approved_and_reference_a_real_tyre(self):
        identifier = next(iter(self.extra)); asset = 'f'*64 + '.pdf'
        bundle = {**self.bundle, 'documents': {'resources': {asset: {}}, 'approved_assets': [asset]},
            'product_documents': {identifier: {'tyre_sheet_url': '/shop/documents/' + asset}}}
        self.assertEqual(unavailable_document_product_ids(bundle, [asset]), {identifier})
        self.assertEqual(unavailable_document_product_ids(bundle, None), set())
        for invalid in ('true', [asset, asset], ['../x.pdf'], ['e'*64 + '.pdf']):
            with self.subTest(value=invalid), self.assertRaises(NominalOverlayError):
                unavailable_document_product_ids(bundle, invalid)
        bundle['product_documents'][identifier]['tyre_sheet_url'] += '?alias=1'
        with self.assertRaises(NominalOverlayError): unavailable_document_product_ids(bundle, [asset])


class MissingDocumentProductionChecks(unittest.TestCase):
    def test_real_bootstrap_blocks_only_known_bad_sheet_and_keeps_five_fixed_prices(self):
        release = production_fixtures.ReleaseChecks('test_approved_browse_release_exposes_only_allowlisted_projection_and_original_image')
        release.setUp(); self.addCleanup(release.doCleanups)
        rows = [tyre(letter) for letter in ('a', 'b', 'c', 'd', 'e')]
        identifiers = [p['selection_id'] for p in rows]
        resources, docs = {}, {}
        sheets = []
        for index, row in enumerate(rows):
            eprel = str(123450 + index)
            label_url = 'https://media1.tyre-shopping.com/tyrelabel2/synthetic/label.jpg?' + urlencode({
                'manufacturer': 'Synthetic', 'model': 'Example', 'size': '205/55 R18 91V',
                'qr': base64.b64encode(('https://eprel.ec.europa.eu/qr/' + eprel).encode()).decode()})
            label_asset = document_asset(label_url); resources[label_asset] = {'url': label_url}
            sheet_url = 'https://eprel.ec.europa.eu/fiches/tyres/Fiche_' + str(123450 + index) + '_DE.pdf'
            asset = document_asset(sheet_url); sheets.append(asset); resources[asset] = {'url': sheet_url}
            docs[row['selection_id']] = {'identity_hash': variant_fingerprint(row), 'eprel_id': str(123450 + index),
                'tyre_label_url': '/shop/documents/' + label_asset, 'tyre_sheet_url': '/shop/documents/' + asset}
        observed = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        release.bundle.update(products=[release.released, *rows],
            rsu_bindings={pid: {'synthetic': True} for pid in identifiers},
            shipping_classes={pid: 'pkw_tyre' for pid in identifiers}, delivery_postcodes=['74821'],
            product_documents=docs, documents={'resources': resources, 'approved_assets': list(resources)},
            compatible_pairs={release.released['selection_id']: [pair(release.released, p) for p in rows]},
            rsu_price_seeds={'schema_version': 1, 'evidence_id': 'e'*64, 'records': {
                row['selection_id']: {'source': 'RSU', 'identity_hash': variant_fingerprint(row),
                    'unit_gross_cent': 10000 + index*100, 'stock_reported': 20, 'observed_at': observed}
                for index, row in enumerate(rows)}})
        release.bundle['publication'].update(released_ids=[release.released['selection_id'], *identifiers],
            document_urls={asset: '/shop/documents/' + asset for asset in resources})
        release.descriptor.update(buyer_and_quotes_enabled=True, initial_storage_creation=True,
            rsu_reads_approved=True, fixed_pricebook_enabled=True, unavailable_product_documents=[sheets[2]],
            rsu_config=production_fixtures.write_json(release.root / 'rsu-private.json', {}))
        release.install()
        calls = []; supplier = release.synthetic_provider('RSU', calls, 9000)
        supplier.rsu_mapping = lambda p: {'confirmed': True, 'product_id': p['id'], 'identity_hash': p['identity_hash'],
            'supplier': 'RSU', 'supplier_article_id': '123', 'evidence_id': 'synthetic-mapping'}
        with patch('shop_production.RSUQuoteProvider.from_config', return_value=supplier):
            dependencies = release.prepare()
        self.assertTrue(dependencies)
        catalog = dependencies['catalogue']
        service = release.portal.app.extensions['shop_production']['quotes']
        context = {'delivery': {'salutation': 'Herr', 'recipient': 'Synthetic Buyer', 'street': 'Teststraße 12',
                               'country': 'DE', 'postcode': '74821', 'city': 'Mosbach'}}
        for index, pid in enumerate(identifiers):
            public = catalog.get(pid)
            self.assertEqual(public['price']['prices_by_quantity']['4']['unit_gross_cent'], 10000 + index*100)
            self.assertEqual(public['purchase_status'], 'inquiry_only' if index == 2 else 'supplier_check_required')
            quote = service.quote({'lines': [{'id': pid, 'quantity': 4}]},
                customer_id='synthetic-buyer', delivery_context=context).snapshot()
            self.assertIs(quote['ready'], index != 2)
            if index == 2:
                self.assertIn('product_documents_unconfirmed', [r['code'] for r in quote['blocked_reasons']])
            else:
                self.assertEqual(quote['total_gross_cent'], (10000 + index*100)*4)
        recommendations = catalog.compatible_tyres(release.released['selection_id'], quantity=4, season='summer')['items']
        self.assertEqual({p['id'] for p in recommendations}, set(identifiers) - {identifiers[2]})
        self.assertFalse(release.portal.app.config['SHOP_CHECKOUT_ENABLED'])


if __name__ == '__main__':
    unittest.main()
