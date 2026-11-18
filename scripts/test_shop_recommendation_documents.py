"""Successful original subsets only; temporary synthetic bytes and no network."""
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
import shop_recommendation_documents as overlay
from shop_document_overlay import DocumentOverlayError
from shop_documents import ReleasedProductDocuments, document_asset
from test_shop_rim_width_extension import fixture, encoded
from test_shop_width_document_overlay import pdf
from test_shop_documents import image


class RecommendationDocumentChecks(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket, 'socket', side_effect=AssertionError('No network'))
        guard.start(); self.addCleanup(guard.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); (self.root / 'documents').mkdir()
        self.base, self.epoch = 'a' * 64, 'b' * 64
        self.bundle, extension = fixture()
        self.bundle['nominal_tyre_sources'] = extension['nominal_tyre_sources']
        first = next(p for p in self.bundle['products'] if p['category'] == 'tyre')
        pair = self.bundle['product_documents'][first['selection_id']]
        old_asset = pair['tyre_sheet_url'].split('/')[-1]
        source = {'url': 'https://www.contimediacenter.com/eulabel-download-pis-pdf/de_DE/01234567890/',
            'tyre_identity': overlay._identity(first)}
        asset = document_asset(source['url'])
        self.bundle['documents']['resources'].pop(old_asset)
        self.bundle['documents']['resources'][asset] = source
        self.bundle['documents']['approved_assets'].remove(old_asset)
        self.bundle['documents']['approved_assets'].append(asset)
        pair['tyre_sheet_url'] = '/shop/documents/' + asset
        self.resources = deepcopy(self.bundle['documents']['resources'])
        self.records, self.payloads = {}, {}
        for product in self.bundle['products']:
            pair = self.bundle['product_documents'].get(product['selection_id'])
            if pair is None: continue
            for kind in ('tyre_label_url', 'tyre_sheet_url'):
                asset = pair[kind].split('/')[-1]
                data = image() if kind == 'tyre_label_url' else pdf(' '.join((product['manufacturer_name'],
                    product['width'] + '/' + product['ratio'] + ' R' + product['diameter'],
                    product['load_index'], product['speed_index'])))
                self.payloads[asset] = data; (self.root / 'documents' / asset).write_bytes(data)
                self.records[asset] = {'name': asset, 'sha256': sha256(data).hexdigest(), 'size': len(data),
                    'mime': 'image/jpeg' if asset.endswith('.jpg') else 'application/pdf'}
        self.manifest = {'schema_version': 1, 'base_bundle_sha256': self.base,
            'source_extension_sha256': self.epoch, 'records': self.records}
        self.pin()

    def pin(self, raw=None):
        raw = encoded(self.manifest) if raw is None else raw
        (self.root / 'recommendation-documents.json').write_bytes(raw)
        self.record = {'path': 'recommendation-documents.json', 'sha256': sha256(raw).hexdigest(),
            'base_bundle_sha256': self.base, 'source_extension_sha256': self.epoch}

    def load(self, record=None, **kwargs):
        return overlay.local_recommendation_document_resources(self.root, self.record if record is None else record,
            self.bundle, self.resources, base_bundle_sha256=kwargs.get('base', self.base),
            source_extension_sha256=kwargs.get('epoch', self.epoch))

    def add_attestation(self):
        owners = overlay._source_owners(self.bundle, self.resources)
        self.attestation = {'schema_version': 1, 'base_bundle_sha256': self.base,
            'source_extension_sha256': self.epoch, 'document_manifest_sha256': self.record['sha256'],
            'records': {asset: {'sha256': entry['sha256'], 'owners': overlay._attestation_owners(owners[asset])}
                for asset, entry in self.manifest['records'].items() if asset.endswith('.pdf')}}
        for asset, entry in self.attestation['records'].items():
            for owner in entry['owners']:
                ReleasedProductDocuments._validate_tyre_identity(self.payloads[asset], owner['tyre_identity'])
        self.pin_attestation()

    def pin_attestation(self, raw=None):
        raw = encoded(self.attestation) if raw is None else raw
        (self.root / 'identity-attestation.json').write_bytes(raw)
        self.record['identity_attestation'] = {'path': 'identity-attestation.json', 'sha256': sha256(raw).hexdigest()}

    def test_absence_returns_exact_resources_without_io(self):
        with patch.object(overlay, '_read', side_effect=AssertionError('No IO')):
            self.assertIs(overlay.local_recommendation_document_resources(None, None, None, self.resources,
                base_bundle_sha256=None, source_extension_sha256=None), self.resources)

    def test_successful_original_subset_serve_bytes_without_network_or_other_mutation(self):
        self.manifest['records'] = {k: v for k, v in self.records.items() if k.endswith('.pdf')}; self.pin()
        before, old = deepcopy(self.bundle), deepcopy(self.resources)
        result = self.load(); self.assertEqual(self.bundle, before); self.assertEqual(self.resources, old)
        self.assertEqual(result.keys(), self.resources.keys())
        http = Mock(); http.get.side_effect = AssertionError('No HTTP')
        service = ReleasedProductDocuments(result, approved_assets=set(self.manifest['records']),
            local_root=self.root / 'documents', enabled=True, allow_network=True, http=http)
        for asset in self.manifest['records']:
            self.assertEqual(service.read(asset), (self.payloads[asset], 'application/pdf'))
        self.assertEqual(len(service.attempts), 0); http.get.assert_not_called()
        for asset in self.resources:
            if asset not in self.manifest['records']: self.assertIs(result[asset], self.resources[asset])

    def test_manifest_pin_paths_schema_limits_and_epochs_fail_closed(self):
        for delta in ({'sha256': 'f' * 64}, {'base_bundle_sha256': 'f' * 64},
                {'source_extension_sha256': 'f' * 64}, {'path': '../escape'}, {'extra': True}):
            with self.subTest(delta=delta), self.assertRaises(DocumentOverlayError): self.load({**self.record, **delta})
        for epoch in (None, True, 'bad', 'f' * 64):
            with self.subTest(epoch=epoch), self.assertRaises(DocumentOverlayError): self.load(epoch=epoch)
        old = deepcopy(self.manifest)
        for field, value in (('schema_version', True), ('records', {}), ('prices', {})):
            self.manifest = deepcopy(old); self.manifest[field] = value; self.pin()
            with self.subTest(field=field), self.assertRaises(DocumentOverlayError): self.load()
        self.manifest = deepcopy(old); self.manifest['records'] = {str(n): {} for n in range(513)}; self.pin()
        with self.assertRaisesRegex(DocumentOverlayError, 'manifest_invalid'): self.load()

    def test_source_proof_identity_eprel_approval_and_membership_required(self):
        original = deepcopy(self.bundle); pid = next(iter(self.bundle['nominal_tyre_sources']))
        for mode in ('proof_hash', 'identity', 'eprel', 'approved', 'source_url', 'released', 'binding', 'duplicate'):
            self.bundle = deepcopy(original)
            pair = self.bundle['product_documents'][pid]
            asset = pair['tyre_label_url'].split('/')[-1]
            if mode == 'proof_hash': self.bundle['nominal_tyre_sources'][pid]['proof_sha256'] = 'f' * 64
            elif mode == 'identity': pair['identity_hash'] = 'f' * 64
            elif mode == 'eprel': pair['eprel_id'] = '99999'
            elif mode == 'approved': self.bundle['documents']['approved_assets'].remove(asset)
            elif mode == 'source_url': self.bundle['documents']['resources'][asset]['url'] = 'https://invalid.example/label.jpg'
            elif mode == 'released': self.bundle['publication']['released_ids'].remove(pid)
            elif mode == 'binding': self.bundle['rsu_bindings'][pid]['identity_hash'] = 'f' * 64
            else: self.bundle['products'].append(deepcopy(next(p for p in self.bundle['products'] if p['selection_id'] == pid)))
            with self.subTest(mode=mode), self.assertRaises(DocumentOverlayError): self.load()

    def test_unproven_unknown_assets_and_changed_conti_identity_cannot_expand_scope(self):
        asset = next(iter(self.records)); self.manifest['records'] = {'f' * 64 + '.pdf': self.records[asset]}; self.pin()
        with self.assertRaisesRegex(DocumentOverlayError, 'scope_invalid'): self.load()
        self.manifest['records'] = self.records; self.pin()
        asset = next(k for k, v in self.resources.items() if 'tyre_identity' in v)
        self.resources[asset]['tyre_identity']['width_mm'] = '225'
        self.bundle['documents']['resources'][asset]['tyre_identity']['width_mm'] = '225'
        with self.assertRaises(DocumentOverlayError): self.load()

    def test_resource_file_hash_conflicts_reject_and_identical_existing_cache_is_preserved(self):
        asset = next(iter(self.records)); original = deepcopy(self.resources[asset])
        for change in ({'file': 'elsewhere.jpg'}, {'sha256': 'f' * 64}):
            self.resources[asset] = {**original, **change}
            with self.subTest(change=change), self.assertRaisesRegex(DocumentOverlayError, 'resource_conflict'): self.load()
        self.resources[asset] = {**original, 'file': asset, 'sha256': self.records[asset]['sha256']}
        self.assertEqual(self.load()[asset], self.resources[asset])

    def test_missing_corrupt_oversized_wrong_mime_or_bool_size_is_rejected(self):
        asset = next(iter(self.records)); path = self.root / 'documents' / asset
        for data in (b'', b'BROKEN', b'x' * (4 * 1024 * 1024 + 1)):
            path.write_bytes(data)
            with self.subTest(length=len(data)), self.assertRaises(DocumentOverlayError): self.load()
        path.write_bytes(self.payloads[asset]); self.records[asset]['mime'] = 'application/json'; self.pin()
        with self.assertRaisesRegex(DocumentOverlayError, 'mime_mismatch'): self.load()
        self.records[asset]['mime'] = 'image/jpeg'; self.records[asset]['size'] = True; self.pin()
        with self.assertRaisesRegex(DocumentOverlayError, 'record_invalid'): self.load()
        self.records[asset]['size'] = len(self.payloads[asset]); self.pin(); path.unlink()
        with self.assertRaises(DocumentOverlayError): self.load()

    def test_wrong_actual_pis_identity_is_rejected_even_with_matching_content_hash(self):
        for asset in (a for a in self.records if a.endswith('.pdf')):
            original = self.payloads[asset]
            wrong = pdf('Continental 225/40 R18 92 V')
            (self.root / 'documents' / asset).write_bytes(wrong)
            self.records[asset].update(sha256=sha256(wrong).hexdigest(), size=len(wrong)); self.pin()
            with self.subTest(asset=asset), self.assertRaisesRegex(DocumentOverlayError, 'content_invalid'): self.load()
            (self.root / 'documents' / asset).write_bytes(original)
            self.records[asset].update(sha256=sha256(original).hexdigest(), size=len(original)); self.pin()

    def test_duplicate_nonfinite_oversized_json_and_symlink_paths_are_rejected(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b' ' * (overlay.MAX_MANIFEST + 1)):
            self.pin(raw)
            with self.subTest(length=len(raw)), self.assertRaises(DocumentOverlayError): self.load()
        self.pin(); real = Path.is_symlink
        for name in ('recommendation-documents.json', next(iter(self.records)), 'documents'):
            with patch.object(Path, 'is_symlink', lambda p: p.name == name or real(p)):
                with self.subTest(name=name), self.assertRaises(DocumentOverlayError): self.load()

    def test_pinned_identity_attestation_skips_only_redundant_text_and_still_binds_all_bytes(self):
        self.add_attestation(); before = deepcopy(self.bundle)
        with patch.object(ReleasedProductDocuments, '_validate_tyre_identity', side_effect=AssertionError('Already attested')) as identity:
            result = self.load(); identity.assert_not_called()
        self.assertEqual(self.bundle, before)
        self.assertEqual(set(result), set(self.resources))
        asset = next(a for a in self.records if a.endswith('.pdf'))
        (self.root / 'documents' / asset).write_bytes(pdf('Continental 225/40 R18 92 V'))
        with self.assertRaisesRegex(DocumentOverlayError, 'content_hash_mismatch'): self.load()

    def test_absent_attestation_retains_original_actual_pdf_identity_validation(self):
        actual = ReleasedProductDocuments._validate_tyre_identity
        with patch.object(ReleasedProductDocuments, '_validate_tyre_identity', wraps=actual) as validate:
            self.load()
        self.assertEqual(validate.call_count, 2)

    def test_attestation_owner_product_variant_proof_eprel_geometry_and_membership_must_match(self):
        self.add_attestation(); original = deepcopy(self.attestation)
        asset = next(iter(self.attestation['records']))
        for field, value in (('product_id', 'tyre:' + 'f' * 24), ('identity_hash', 'f' * 64),
                ('nominal_proof_sha256', 'f' * 64), ('eprel_id', '99999'),
                ('tyre_identity', {'manufacturer': 'Continental', 'width_mm': '225', 'aspect_ratio_percent': '40',
                    'diameter_inch': '18', 'load_index': '92', 'speed_index': 'V'})):
            self.attestation = deepcopy(original)
            self.attestation['records'][asset]['owners'][0][field] = value; self.pin_attestation()
            with self.subTest(field=field), self.assertRaisesRegex(DocumentOverlayError, 'attestation_identity_mismatch'): self.load()
        for mode in ('missing_asset', 'unknown_asset', 'missing_owner', 'duplicate_owner', 'changed_byte_sha'):
            self.attestation = deepcopy(original)
            if mode == 'missing_asset': self.attestation['records'].pop(asset)
            elif mode == 'unknown_asset': self.attestation['records']['f' * 64 + '.pdf'] = deepcopy(self.attestation['records'][asset])
            elif mode == 'missing_owner': self.attestation['records'][asset]['owners'] = []
            elif mode == 'duplicate_owner': self.attestation['records'][asset]['owners'] *= 2
            else: self.attestation['records'][asset]['sha256'] = 'f' * 64
            self.pin_attestation()
            with self.subTest(mode=mode), self.assertRaises(DocumentOverlayError): self.load()

    def test_present_bad_attestation_never_falls_back_to_text_or_enables_changed_manifest(self):
        self.add_attestation(); original = deepcopy(self.attestation)
        for delta in ({'path': '../escape'}, {'sha256': 'f' * 64}, {'unexpected': True}):
            record = deepcopy(self.record); record['identity_attestation'].update(delta)
            with self.subTest(delta=delta), self.assertRaises(DocumentOverlayError): self.load(record)
        for field, value in (('schema_version', True), ('base_bundle_sha256', 'f' * 64),
                ('source_extension_sha256', 'f' * 64), ('document_manifest_sha256', 'f' * 64)):
            self.attestation = deepcopy(original); self.attestation[field] = value; self.pin_attestation()
            with self.subTest(field=field), self.assertRaises(DocumentOverlayError): self.load()
        self.attestation = deepcopy(original); self.pin_attestation()
        asset = next(a for a in self.records if a.endswith('.pdf'))
        wrong = pdf('Continental 225/40 R18 92 V'); (self.root / 'documents' / asset).write_bytes(wrong)
        self.records[asset].update(sha256=sha256(wrong).hexdigest(), size=len(wrong)); self.pin(); self.pin_attestation()
        with self.assertRaisesRegex(DocumentOverlayError, 'attestation_manifest_invalid'): self.load()
        self.attestation['document_manifest_sha256'] = self.record['sha256']; self.pin_attestation()
        with self.assertRaisesRegex(DocumentOverlayError, 'attestation_identity_mismatch'): self.load()
        record = deepcopy(self.record); record['identity_attestation'] = None
        with self.assertRaisesRegex(DocumentOverlayError, 'attestation_descriptor_invalid'): self.load(record)

    def test_attestation_duplicate_nonfinite_oversize_and_symlink_are_rejected(self):
        self.add_attestation()
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b' ' * (overlay.MAX_MANIFEST + 1)):
            self.pin_attestation(raw)
            with self.subTest(length=len(raw)), self.assertRaises(DocumentOverlayError): self.load()
        self.pin_attestation(); real = Path.is_symlink
        with patch.object(Path, 'is_symlink', lambda p: p.name == 'identity-attestation.json' or real(p)):
            with self.assertRaises(DocumentOverlayError): self.load()


if __name__ == '__main__': unittest.main(verbosity=2)
