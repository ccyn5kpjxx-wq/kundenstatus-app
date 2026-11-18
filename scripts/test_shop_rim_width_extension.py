"""Synthetic offline source-chain tests; no actual approval or transport."""
import base64
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import shop_rim_width_extension as extension
from shop_rim_width_extension import RimWidthExtensionError, load_rim_width_extension
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_catalog_public import variant_fingerprint
from shop_documents import document_asset
from shop_rim_width_candidates import released_width_pairs
from shop_rsu_match import alzura_record, match_catalogues, rsu_record
from test_shop_rim_width_candidates import registry
from test_shop_catalog_public import NOW, wheel

RIM_ID = 'alloy_wheel:0dcf5de696ff9f755331e447'


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def fixture():
    rim = wheel('b', selection_id=RIM_ID, name='SYNTHETIC AUTEC VIDRON', manufacturer_name='AUTEC',
        dimensions_valid=True, width='7', diameter='18', size='7 x 18')
    table = registry(); table['source']['sha256'] = extension.WIDTH_TABLE_SHA
    products, bindings, extra, pairs, resources = [rim], {}, {}, {}, {}
    for n, (pid, (width, ratio, load, left_row, right_row)) in enumerate(extension.EXPECTED.items(), 1):
        mpn, left_id, right_id = 'SYNTHETIC-' + str(n), str(123456+n), str(654321+n)
        p = wheel('c' if n == 1 else 'd', selection_id=pid, category='tyre', name='SYNTHETIC TYRE ' + str(n),
            dimensions_valid=True, manufacturer_name='Continental', article_number=mpn,
            width=str(width), ratio=str(ratio), diameter='18', load_index=str(load), speed_index='V',
            season='Sommerreifen' if n == 1 else 'Winterreifen', size=f'{width}/{ratio} R18 {load}V',
            details={'Herstellerartikelnummer': mpn})
        p['quote_public']['source'] = 'RSU'; products.append(p)
        a = dict(id=left_id, brandname='Continental', manufacturer_number=mpn,
            width=str(width), height=str(ratio), inner_diameter='18', tyre_type='R', load_index=str(load),
            speed_index='V', type='S', dot='0', demo='0', da_decke='0', runflat='0', ms='0', **{'3pmfs': '0'})
        r = dict(Artikelnummer=right_id, Hersteller='Continental', Herstellerartikelnummer=mpn,
            Reifenbreite=str(width), Reifenquerschnitt=str(ratio), Reifendurchmesser='18', Bauart='R',
            Loadindex=str(load), Speedindex='V', Saison='s', Artikeltyp='Reifen', Artikeluntertyp='PKW',
            Dot='', Demo='', Da='')
        mapping = match_catalogues([alzura_record(a, archive_sha256=extension.ALZ_SHA, row_number=left_row)],
            [rsu_record(r, csv_sha256=extension.RSU_SHA, row_number=right_row)])['mappings'][0]
        identity = variant_fingerprint(p)
        bindings[pid] = {'identity_hash': identity, 'category': 'tyre', 'mapping': mapping, 'expected_ean': '4019238594195'}
        proof = {'rule': 'alzura_full_export_nominal_text_v1', 'product_id': pid, 'identity_hash': identity,
            'alzura_source_sha256': extension.ALZ_SHA, 'alzura_row_number': left_row, 'alzura_article_id': left_id,
            'articlename': f'SYNTHETIC {width}/{ratio} R18 {load} V XL', 'description': '', 'info': '',
            'manufacturer_description': '', 'runflat': '0', 'rsu_source_sha256': extension.RSU_SHA,
            'rsu_row_number': right_row, 'rsu_profile': 'SYNTHETIC XL'}
        extra[pid] = {'proof': proof, 'proof_sha256': sha256(encoded(proof)).hexdigest()}
        eprel = str(123450+n)
        label = 'https://media1.tyre-shopping.com/tyrelabel2/synthetic/label.jpg?' + urlencode({
            'manufacturer': 'Continental', 'model': mpn, 'size': f'{width}/{ratio}R18{load}VXL',
            'qr': base64.b64encode(('https://eprel.ec.europa.eu/qr/' + eprel).encode()).decode()})
        sheet = f'https://eprel.ec.europa.eu/fiches/tyres/Fiche_{eprel}_DE.pdf'
        for url in (label, sheet): resources[document_asset(url)] = {'url': url}
        pairs[pid] = {'identity_hash': identity, 'status': 'source_bound_document_pair', 'eprel_id': eprel,
            'tyre_label_url': '/shop/documents/' + document_asset(label),
            'tyre_sheet_url': '/shop/documents/' + document_asset(sheet)}
    bundle = {'products': products, 'rsu_bindings': bindings, 'nominal_tyre_sources': {}, 'compatible_pairs': {},
        'reviewed_rim_width_registry': table, 'rsu_price_seeds': {'records': {p: {'unchanged': 12345} for p in extra}},
        'publication': {'released_ids': [p['selection_id'] for p in products]}, 'product_documents': pairs,
        'documents': {'resources': resources, 'approved_assets': list(resources)}, 'verified_fitments': {}}
    value = {'schema_version': 1, 'base_bundle_sha256': 'a'*64, 'registry_rows': list(deepcopy(extension.REVIEWED_ROWS)),
        'nominal_tyre_sources': extra, 'sources': {'alzura_archive_sha256': extension.ALZ_SHA,
            'rsu_csv_sha256': extension.RSU_SHA, 'width_table_sha256': extension.WIDTH_TABLE_SHA}}
    return bundle, value


class WidthExtensionChecks(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket, 'socket', side_effect=AssertionError('No network')); guard.start(); self.addCleanup(guard.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup); self.root = Path(self.temp.name).resolve()
        self.bundle, self.value = fixture(); self.record = self.save()

    def save(self, raw=None):
        raw = encoded(self.value) if raw is None else raw; (self.root/'extension.json').write_bytes(raw)
        return {'path': 'extension.json', 'sha256': sha256(raw).hexdigest(), 'base_bundle_sha256': 'a'*64}

    def load(self, record=None, bundle=None):
        return load_rim_width_extension(self.root, record or self.record, self.bundle if bundle is None else bundle,
            base_bundle_sha256='a'*64)

    def rehash(self, pid):
        item = self.value['nominal_tyre_sources'][pid]; item['proof_sha256'] = sha256(encoded(item['proof'])).hexdigest()

    def test_absence_has_no_file_reads_and_preserves_object(self):
        with patch.object(extension, '_read', side_effect=AssertionError('No file read')):
            self.assertIs(load_rim_width_extension(self.root, None, self.bundle, base_bundle_sha256='a'*64), self.bundle)

    def test_real_source_contract_adds_summer_and_winter_only_and_no_rights(self):
        before = deepcopy(self.bundle); merged = self.load(); self.assertEqual(self.bundle, before)
        for key in self.bundle:
            if key not in {'nominal_tyre_sources', 'reviewed_rim_width_registry'}: self.assertIs(merged[key], self.bundle[key])
        self.assertEqual(merged['reviewed_rim_width_registry']['rows'][:-2], self.bundle['reviewed_rim_width_registry']['rows'])
        pairs = released_width_pairs(merged, approved_ids=set(merged['rsu_price_seeds']['records']))
        self.assertEqual({p['tyre_id'] for p in pairs[RIM_ID]}, set(extension.EXPECTED))
        catalog = EnrichedPublicCatalog(merged['products'], compatible_pairs=pairs,
            procurement_ids=set(merged['rsu_bindings']), released_ids=merged['publication']['released_ids'], clock=lambda: NOW)
        self.assertEqual(catalog.get(RIM_ID)['tyre_seasons'], ['summer', 'winter'])
        for season in ('summer', 'winter'):
            result = catalog.compatible_tyres(RIM_ID, season=season, quantity=4)
            self.assertEqual(len(result['items']), 1); self.assertEqual(result['scope'], 'rim_width_candidate')
            self.assertTrue(all(not item['order_enabled'] for item in result['items']))
        self.assertEqual(catalog.compatible_tyres(RIM_ID, season='allseason')['items'], [])
        self.assertEqual(merged['verified_fitments'], {})

    def test_wrong_pin_path_base_duplicate_and_oversized_input_rejected(self):
        for record in ({**self.record, 'sha256': 'f'*64}, {**self.record, 'base_bundle_sha256': 'f'*64},
                       {**self.record, 'path': '../extension.json'}):
            with self.subTest(record=record), self.assertRaises(RimWidthExtensionError): self.load(record)
        for raw in (b' '*16385, b'{"x":1,"x":2}', b'{"x":NaN}'):
            with self.assertRaises(RimWidthExtensionError): self.load(self.save(raw))

    def test_no_registry_override_borrowed_7_5_minimum_or_new_price_fields(self):
        changed = deepcopy(self.bundle); changed['reviewed_rim_width_registry']['rows'].append(deepcopy(extension.REVIEWED_ROWS[0]))
        with self.assertRaisesRegex(RimWidthExtensionError, 'registry_override'): self.load(bundle=changed)
        self.value['registry_rows'][0]['permitted_rim_widths_in'] = [7.5, 8]
        with self.assertRaisesRegex(RimWidthExtensionError, 'reviewed_rows'): self.load(self.save())
        self.value['registry_rows'] = list(deepcopy(extension.REVIEWED_ROWS)); self.value['rsu_price_seeds'] = {}
        with self.assertRaisesRegex(RimWidthExtensionError, 'schema_invalid'): self.load(self.save())

    def test_missing_released_binding_seed_or_document_proof_stops(self):
        pid = next(iter(extension.EXPECTED))
        for key in ('rsu_bindings', 'rsu_price_seeds', 'product_documents', 'publication'):
            changed = deepcopy(self.bundle)
            if key == 'rsu_price_seeds': changed[key]['records'].pop(pid)
            elif key == 'publication': changed[key]['released_ids'].remove(pid)
            else: changed[key].pop(pid)
            with self.subTest(key=key), self.assertRaises(RimWidthExtensionError): self.load(bundle=changed)
        changed = deepcopy(self.bundle); changed['product_documents'][pid]['identity_hash'] = 'f'*64
        with self.assertRaises(RimWidthExtensionError): self.load(bundle=changed)

    def test_original_source_rows_exact_identity_and_actual_xl_cannot_change(self):
        pid = next(iter(extension.EXPECTED)); original = deepcopy(self.value)
        for key, change in (('alzura_row_number', 1), ('rsu_source_sha256', 'f'*64), ('articlename', 'SYNTHETIC 225/40 R18 92 V XL'),
                            ('info', 'SSR'), ('articlename', 'SYNTHETIC 205/40 R18 86 V')):
            self.value = deepcopy(original); self.value['nominal_tyre_sources'][pid]['proof'][key] = change; self.rehash(pid)
            if change == 'SYNTHETIC 205/40 R18 86 V':
                self.value['nominal_tyre_sources'][pid]['proof']['rsu_profile'] = 'SYNTHETIC'
                self.rehash(pid)
            with self.subTest(key=key, change=change), self.assertRaises(RimWidthExtensionError): self.load(self.save())
        self.value = original; changed = deepcopy(self.bundle); changed['products'][1]['name'] = 'Wrong exact SKU'
        with self.assertRaises(RimWidthExtensionError): self.load(self.save(), bundle=changed)

    def test_existing_nominal_source_and_wrong_source_brand_cannot_be_inherited(self):
        pid = next(iter(extension.EXPECTED)); changed = deepcopy(self.bundle)
        changed['nominal_tyre_sources'][pid] = self.value['nominal_tyre_sources'][pid]
        with self.assertRaises(RimWidthExtensionError): self.load(bundle=changed)
        changed = deepcopy(self.bundle); changed['reviewed_rim_width_registry']['brands'] = ['Michelin']
        with self.assertRaises(RimWidthExtensionError): self.load(bundle=changed)
        changed = deepcopy(self.bundle); changed['products'].append(deepcopy(changed['products'][1]))
        with self.assertRaisesRegex(RimWidthExtensionError, 'duplicate_product'): self.load(bundle=changed)

    def test_mocked_symlink_is_rejected_on_all_platforms(self):
        original = Path.is_symlink
        with patch.object(Path, 'is_symlink', lambda p: p == self.root/'extension.json' or original(p)):
            with self.assertRaisesRegex(RimWidthExtensionError, 'path_invalid'): self.load()


if __name__ == '__main__': unittest.main(verbosity=2)
