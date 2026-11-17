"""Synthetic offline release preparation; network blocked, no real feed fixtures."""
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO, StringIO
import csv
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from PIL import Image
from prepare_shop_initial_release import ReleaseError, canonical, prepare_release
from shop_alzura_feed import HEADERS
from shop_catalog_public import PublicCatalog, variant_fingerprint
from shop_media import ReleasedMedia
from shop_rsu_match import alzura_record, match_catalogues, rsu_record
from shop_rsu_quotes import RSUQuotePolicy
from test_shop_alzura_feed import row
from test_shop_rsu_match import rsu

NOW = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)


class InitialReleaseChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.media_root = self.root / 'cached'
        (self.media_root / 'bilder').mkdir(parents=True)
        stream = BytesIO()
        Image.new('RGB', (2, 2), 'black').save(stream, 'PNG')
        self.image = stream.getvalue()
        self.photo = 'bilder/' + 'f' * 64 + '.png'
        (self.media_root / self.photo).write_bytes(self.image)
        self.raw = row(price_1='98765.4321', price_4='87654.3210', availability='1234')
        self.archive = self.root / 'synthetic.zip'
        self.write_archive([self.raw])
        archive_hash = sha256(self.archive.read_bytes()).hexdigest()
        matched = match_catalogues([alzura_record(self.raw, archive_sha256=archive_hash, row_number=1)],
                    [rsu_record(rsu(Herstellerartikelnummer=self.raw['manufacturer_number']), csv_sha256='b' * 64, row_number=1)])
        self.mappings = self.root / 'mappings.json'
        self.write_json(self.mappings, matched)
        sid = 'tyre:' + 'c' * 24
        product = {'selection_id': sid, 'category': 'tyre', 'name': 'Example Tour', 'manufacturer_name': 'EXAMPLE',
                   'width': '205', 'ratio': '55', 'diameter': '16', 'group': 'Sommerreifen',
                   'photo': self.photo, 'dimensions_valid': True, 'price': 'PRIVATE_OLD_PRICE_CANARY',
                   'identifiers': {'id_t24': 123456, 'ean': '4006381333931', 'manufacturer_article_number': self.raw['manufacturer_number']}}
        self.catalog = self.root / 'catalog.json'
        self.write_json(self.catalog, {'catalogues': {'reifen': [product]}, 'private_evidence': {}})
        self.crosswalk = self.root / 'crosswalk.json'
        catalog_hash = sha256(self.catalog.read_bytes()).hexdigest()
        self.cross = {'schema_version': 1, 'source_catalog_sha256': catalog_hash, 'candidates': [{
            'mapping_evidence_id': matched['mappings'][0]['evidence_id'], 'alzura_full_id': '123456', 'rsu_article_id': '654321',
            'rest_selection_id': sid, 'rest_catalog_sha256': catalog_hash, 'same_brand_mpn_and_ean': True,
            'same_core_size': True, 'photo': self.photo, 'photo_sha256': sha256(self.image).hexdigest(),
            'image_release_confirmed': False, 'mapping_approved': False, 'supplier_quote_current': False}]}
        self.write_json(self.crosswalk, self.cross)
        self.output = self.root / 'release'
        self.kwargs = dict(archive_path=self.archive, crosswalk_path=self.crosswalk, mappings_path=self.mappings,
            rest_catalog_path=self.catalog, media_root=self.media_root, output_dir=self.output,
            review_reference='SYNTHETIC-static-identity-media-review', expected_count=1, now=NOW,
            validation_policy=RSUQuotePolicy(mode='test', currency='EUR', net_unit_price_confirmed=True,
                                            evidence_id='SYNTHETIC-OFFLINE-CONSTRUCTOR-CHECK-NOT-LIVE'))

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()

    @staticmethod
    def write_json(path, data):
        path.write_text(canonical(data), encoding='utf-8')

    def write_archive(self, rows):
        stream = StringIO(newline='')
        writer = csv.DictWriter(stream, fieldnames=HEADERS, delimiter='|', lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
        with ZipFile(self.archive, 'w', compression=ZIP_DEFLATED) as archive:
            archive.writestr('tmp/123.csv', stream.getvalue().encode('utf-8-sig'))

    def prepare(self, **changes):
        return prepare_release(**{**self.kwargs, **changes})

    def test_offline_release_has_consistent_public_ids_bindings_and_unchanged_images(self):
        report = self.prepare()
        self.assertEqual(report['counts']['products'], 1)
        self.assertEqual(report['provider_calls'], 0)
        products = [json.loads(line) for line in (self.output / 'products.jsonl').read_text(encoding='utf-8').splitlines()]
        bindings = json.loads((self.output / 'bindings.private.json').read_text(encoding='utf-8'))
        publication = json.loads((self.output / 'publication.json').read_text(encoding='utf-8'))
        product = products[0]
        self.assertEqual(product['id'], product['selection_id'])
        self.assertEqual(product['identity_hash'], variant_fingerprint(product))
        self.assertEqual(bindings[product['id']]['identity_hash'], product['identity_hash'])
        self.assertIsNone(product['quote_public'])
        self.assertEqual(product['quote_status'], 'not_queried')
        catalog = PublicCatalog(products, released_ids=publication['released_ids'], image_urls=publication['image_urls'])
        public = catalog.get(product['id'])
        self.assertEqual(public['price_status'], 'update_required')
        self.assertIsNone(public['price'])
        asset = sha256(self.image).hexdigest() + '.png'
        self.assertEqual(public['images'], ['/shop/media/' + asset])
        self.assertEqual((self.output / 'public-media' / asset).read_bytes(), self.image)
        manifest = json.loads((self.output / 'media_manifest.private.json').read_text(encoding='utf-8'))
        media = ReleasedMedia(manifest['resources'], approved_assets=set(manifest['approved_assets']), local_root=self.output)
        self.assertEqual(media.read(asset), (self.image, 'image/png'))

    def test_no_buying_prices_supplier_times_or_original_media_urls_are_exported(self):
        report = self.prepare()
        for name in report['files']:
            if name.startswith('public-media/'):
                continue
            text = (self.output / name).read_text(encoding='utf-8')
            for private in ('98765.4321', '87654.3210', 'PRIVATE_OLD_PRICE_CANARY', 'https://media',
                            'observed_at', 'commercial_as_supplied', 'price_1', 'price_4'):
                self.assertNotIn(private, text)
        self.assertEqual(report['source_time']['status'], 'UNVERIFIED')
        self.assertIsNone(report['source_time']['supplier_observed_at'])
        self.assertFalse(report['source_time']['timezone_confirmed'])
        self.assertFalse(report['provider_activation'])
        self.assertFalse(report['orders_enabled'])
        self.assertFalse(report['payments_enabled'])

    def test_hash_manifest_binds_every_output_and_dataset_is_deterministic(self):
        report = self.prepare()
        for name, proof in report['files'].items():
            data = (self.output / name).read_bytes()
            self.assertEqual(len(data), proof['bytes'])
            self.assertEqual(sha256(data).hexdigest(), proof['sha256'])
        other = self.prepare(output_dir=self.root / 'release-other')
        self.assertEqual(other['dataset_sha256'], report['dataset_sha256'])

    def test_existing_directory_and_missing_explicit_review_cannot_publish(self):
        with self.assertRaises(ReleaseError):
            self.prepare(review_reference='')
        self.output.mkdir()
        sentinel = self.output / 'keep.txt'
        sentinel.write_text('keep', encoding='utf-8')
        with self.assertRaisesRegex(ReleaseError, 'output_must_be_new_directory'):
            self.prepare()
        self.assertEqual(sentinel.read_text(encoding='utf-8'), 'keep')

    def test_duplicate_crosswalk_and_wrong_catalog_source_fail_before_output(self):
        broken = deepcopy(self.cross)
        broken['candidates'].append(deepcopy(broken['candidates'][0]))
        self.write_json(self.crosswalk, broken)
        with self.assertRaises(ReleaseError):
            self.prepare(expected_count=2)
        self.assertFalse(self.output.exists())
        self.write_json(self.crosswalk, self.cross)
        value = json.loads(self.catalog.read_text(encoding='utf-8'))
        value['extra'] = 'changed source'
        self.write_json(self.catalog, value)
        with self.assertRaisesRegex(ReleaseError, 'source_metadata_mismatch'):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_wrong_ean_and_mapping_evidence_are_rejected(self):
        for mutation in ('ean', 'mapping'):
            if mutation == 'ean':
                catalog = json.loads(self.catalog.read_text(encoding='utf-8'))
                catalog['catalogues']['reifen'][0]['identifiers']['ean'] = '4006381333932'
                self.write_json(self.catalog, catalog)
                digest = sha256(self.catalog.read_bytes()).hexdigest()
                self.cross['source_catalog_sha256'] = digest
                self.cross['candidates'][0]['rest_catalog_sha256'] = digest
                self.write_json(self.crosswalk, self.cross)
            else:
                mappings = json.loads(self.mappings.read_text(encoding='utf-8'))
                mappings['mappings'][0]['evidence']['rsu']['article_id'] = '654322'
                self.write_json(self.mappings, mappings)
            with self.subTest(mutation=mutation), self.assertRaises(ReleaseError):
                self.prepare()
            self.assertFalse(self.output.exists())

    def test_media_corruption_and_path_escape_are_rejected(self):
        (self.media_root / self.photo).write_bytes(b'corrupted-image')
        with self.assertRaisesRegex(ReleaseError, 'local_media_hash_mismatch'):
            self.prepare()
        (self.media_root / self.photo).write_bytes(self.image)
        self.cross['candidates'][0]['photo'] = '../escape.png'
        self.write_json(self.crosswalk, self.cross)
        with self.assertRaises(ReleaseError):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_csv_content_binding_cannot_be_rescued_by_changing_archive_hash(self):
        self.raw['width'] = '215'
        self.write_archive([self.raw])
        mappings = json.loads(self.mappings.read_text(encoding='utf-8'))
        mappings['source_hashes']['alzura'] = sha256(self.archive.read_bytes()).hexdigest()
        self.write_json(self.mappings, mappings)
        with self.assertRaises(ReleaseError):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_atomic_failure_removes_only_owned_staging_directory(self):
        with patch('prepare_shop_initial_release._write', side_effect=OSError('PRIVATE_FILE_ERROR_CANARY')):
            with self.assertRaisesRegex(ReleaseError, '^initial_release_preparation_failed$'):
                self.prepare()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob('.release.staging-*')), [])
        self.assertTrue(self.catalog.is_file())

    def test_live_policy_is_not_accepted_as_offline_validation_activation(self):
        with self.assertRaises(ReleaseError):
            self.prepare(validation_policy=RSUQuotePolicy(mode='live', currency='EUR', net_unit_price_confirmed=True,
                                                          evidence_id='synthetic'))
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
