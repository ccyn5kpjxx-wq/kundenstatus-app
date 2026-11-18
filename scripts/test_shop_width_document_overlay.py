"""Four scoped local originals; temporary files and no supplier/network IO."""
from copy import deepcopy
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
import shop_width_document_overlay as overlay
from shop_width_document_overlay import local_rim_width_document_resources
from shop_document_overlay import DocumentOverlayError
from shop_documents import DocumentError, ReleasedProductDocuments
import test_shop_rim_width_extension as width_fixture
import test_shop_document_overlay as old_fixture
from test_shop_documents import image
from pypdf import PdfWriter
from pypdf.generic import NameObject, DictionaryObject, DecodedStreamObject


def pdf(text):
    stream = BytesIO(); writer = PdfWriter(); page = writer.add_blank_page(width=500, height=200)
    font = DictionaryObject({NameObject('/Type'): NameObject('/Font'), NameObject('/Subtype'): NameObject('/Type1'),
        NameObject('/BaseFont'): NameObject('/Helvetica')})
    page[NameObject('/Resources')] = DictionaryObject({NameObject('/Font'): DictionaryObject({NameObject('/F1'): writer._add_object(font)})})
    content = DecodedStreamObject(); content.set_data(('BT /F1 12 Tf 10 100 Td (' + text + ') Tj ET').encode('ascii'))
    page[NameObject('/Contents')] = writer._add_object(content); writer.write(stream)
    return stream.getvalue()


class WidthDocumentChecks(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket, 'socket', side_effect=AssertionError('No network')); guard.start(); self.addCleanup(guard.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); (self.root / 'documents').mkdir()
        self.base, self.epoch = 'a' * 64, 'b' * 64
        self.bundle, _ = width_fixture.fixture()
        self.resources = deepcopy(self.bundle['documents']['resources'])
        self.records, self.payloads = {}, {}
        for product in self.bundle['products']:
            pair = self.bundle['product_documents'].get(product['selection_id'])
            if pair is None: continue
            for key in ('tyre_label_url', 'tyre_sheet_url'):
                asset = pair[key].split('/')[-1]
                data = image() if key == 'tyre_label_url' else pdf(' '.join((product['manufacturer_name'],
                    product['width'] + '/' + product['ratio'] + ' R' + product['diameter'],
                    product['load_index'], product['speed_index'])))
                self.payloads[asset] = data; (self.root / 'documents' / asset).write_bytes(data)
                self.records[asset] = {'name': asset, 'sha256': sha256(data).hexdigest(), 'size': len(data),
                    'mime': 'image/jpeg' if asset.endswith('.jpg') else 'application/pdf'}
        self.old = old_fixture.LocalDocumentChecks(); self.old.setUp(); self.addCleanup(self.old.doCleanups)
        old_resources = self.old.load()
        for asset, data in self.old.payloads.items(): (self.root / 'documents' / asset).write_bytes(data)
        self.resources.update(old_resources)
        self.bundle['documents']['resources'].update(deepcopy(self.old.resources))
        self.bundle['documents']['approved_assets'].extend(self.old.resources)
        # Keep the actual legacy manifest separate; the new loader cannot
        # rewrite it or broaden its exact29-file scope.
        self.old_manifest = (self.old.root / 'manifest.json').read_bytes()
        (self.root / 'legacy29.json').write_bytes(self.old_manifest)
        self.manifest = {'base_bundle_sha256': self.base, 'width_extension_sha256': self.epoch, 'records': self.records}
        self.pin()

    def pin(self, raw=None):
        raw = width_fixture.encoded(self.manifest) if raw is None else raw
        (self.root / 'width-documents.json').write_bytes(raw)
        self.record = {'path': 'width-documents.json', 'sha256': sha256(raw).hexdigest(), 'base_bundle_sha256': self.base}

    def load(self, record=None, **kwargs):
        return local_rim_width_document_resources(self.root, self.record if record is None else record,
            self.bundle, self.resources, base_bundle_sha256=kwargs.get('base', self.base),
            extension_sha256=kwargs.get('epoch', self.epoch))

    def test_absence_is_exact_legacy_object_and_no_io(self):
        with patch.object(overlay, '_read', side_effect=AssertionError('No IO')):
            self.assertIs(local_rim_width_document_resources(self.root, None, self.bundle, self.resources,
                base_bundle_sha256=self.base, extension_sha256=None), self.resources)

    def test_four_local_originals_serve_exact_bytes_without_network_and_preserve_legacy29_and_base(self):
        before, old = deepcopy(self.bundle), deepcopy(self.resources)
        result = self.load()
        self.assertEqual(self.bundle, before); self.assertEqual(self.resources, old)
        self.assertEqual((self.root / 'legacy29.json').read_bytes(), self.old_manifest)
        for asset in self.old.resources: self.assertEqual(result[asset], old[asset])
        http = Mock(); http.get.side_effect = AssertionError('No HTTP')
        service = ReleasedProductDocuments(result, approved_assets=set(result), local_root=self.root / 'documents',
            enabled=True, allow_network=True, http=http)
        for asset, data in self.payloads.items():
            self.assertEqual(result[asset]['file'], asset); self.assertEqual(result[asset]['sha256'], sha256(data).hexdigest())
            self.assertEqual(service.read(asset), (data, self.records[asset]['mime']))
        for asset, data in self.old.payloads.items(): self.assertEqual(service.read(asset), (data, 'application/pdf'))
        self.assertEqual(len(service.attempts), 0); http.get.assert_not_called()
        self.assertEqual(result.keys(), self.resources.keys())

    def test_wrong_hash_base_extension_epoch_path_and_types_fail_closed(self):
        for change in ({'sha256': 'f' * 64}, {'base_bundle_sha256': 'f' * 64}, {'path': '../elsewhere'}, {'extra': True}):
            with self.subTest(change=change), self.assertRaises(DocumentOverlayError): self.load({**self.record, **change})
        for epoch in (None, 'wrong', 'f' * 64, True):
            with self.subTest(epoch=epoch), self.assertRaises(DocumentOverlayError): self.load(epoch=epoch)
        with self.assertRaises(DocumentOverlayError): self.load(True)

    def test_unknown_or_missing_asset_cannot_expand_four_file_scope(self):
        first = next(iter(self.records)); original = deepcopy(self.records)
        self.manifest['records'] = {k: v for k, v in self.records.items() if k != first}; self.pin()
        with self.assertRaisesRegex(DocumentOverlayError, 'manifest_invalid'): self.load()
        self.manifest['records'] = original; value = self.manifest['records'].pop(first)
        self.manifest['records'][next(iter(self.old.records))] = value; self.pin()
        with self.assertRaisesRegex(DocumentOverlayError, 'scope_invalid'): self.load()

    def test_source_eprel_current_identity_and_approved_membership_are_required(self):
        identifier = next(iter(overlay.EXPECTED)); pair = self.bundle['product_documents'][identifier]
        original = deepcopy(pair)
        for change in ({'identity_hash': 'f' * 64}, {'eprel_id': '999999'}, {'status': 'unconfirmed'}):
            pair.update(change)
            with self.subTest(change=change), self.assertRaises(DocumentOverlayError): self.load()
            pair.clear(); pair.update(original)
        self.bundle['documents']['approved_assets'].remove(next(iter(self.records)))
        with self.assertRaises(DocumentOverlayError): self.load()

    def test_corrupt_missing_oversized_and_wrong_mime_cannot_bind_local_files(self):
        asset = next(iter(self.records)); path = self.root / 'documents' / asset
        for data in (b'', b'BROKEN', b'x' * (4 * 1024 * 1024 + 1)):
            path.write_bytes(data)
            with self.subTest(size=len(data)), self.assertRaises(DocumentOverlayError): self.load()
        path.write_bytes(self.payloads[asset]); self.records[asset]['mime'] = 'application/json'; self.pin()
        with self.assertRaisesRegex(DocumentOverlayError, 'mime_mismatch'): self.load()
        self.records[asset]['mime'] = 'image/jpeg'; self.pin(); path.unlink()
        with self.assertRaises(DocumentOverlayError): self.load()

    def test_structurally_valid_wrong_tyre_sheet_still_fails_identity_after_rehash(self):
        asset = next(a for a in self.records if a.endswith('.pdf'))
        data = pdf('Continental 225/40 R18 92 V')
        (self.root / 'documents' / asset).write_bytes(data)
        self.records[asset].update(sha256=sha256(data).hexdigest(), size=len(data)); self.pin()
        with self.assertRaisesRegex(DocumentError, 'identity_unconfirmed'): self.load()

    def test_symlink_detection_manifest_documents_and_parent_on_all_platforms(self):
        real = Path.is_symlink
        for name in ('width-documents.json', next(iter(self.records)), 'documents'):
            with patch.object(Path, 'is_symlink', lambda p: p.name == name or real(p)):
                with self.subTest(name=name), self.assertRaises(DocumentOverlayError): self.load()

    def test_duplicate_nonfinite_keys_resource_conflicts_and_boolean_sizes_rejected(self):
        for raw in (b'{"records":{},"records":{}}', b'{"records":NaN}'):
            self.pin(raw)
            with self.assertRaises(DocumentOverlayError): self.load()
        self.pin(); asset = next(iter(self.records)); self.records[asset]['size'] = True; self.pin()
        with self.assertRaisesRegex(DocumentOverlayError, 'record_invalid'): self.load()
        self.records[asset]['size'] = len(self.payloads[asset]); self.pin()
        self.resources[asset]['file'] = 'elsewhere.pdf'
        with self.assertRaisesRegex(DocumentOverlayError, 'resource_conflict'): self.load()


if __name__ == '__main__': unittest.main(verbosity=2)
