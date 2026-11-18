"""Source-only extension: synthetic originals and zero network/financial IO."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'scripts'))
import shop_tyre_source_extension as extension
from shop_tyre_source_extension import TyreSourceExtensionError, load_tyre_source_extension
from shop_catalog_public import variant_fingerprint
from shop_rim_width_candidates import released_width_pairs
from shop_rsu_match import alzura_record, match_catalogues, rsu_record
from test_shop_rim_width_candidates import registry, rim
from test_shop_released_width_pairs import rehash


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def fixture():
    rows, bindings, proofs = [rim()], {}, {}
    for n in range(1, 127):
        pid = 'tyre:' + format(n, '024x'); mpn = 'SYNTHETIC-' + str(n)
        p = {'selection_id': pid, 'category': 'tyre', 'dimensions_valid': True,
            'manufacturer_name': 'Continental', 'name': 'SYNTHETIC TYRE ' + str(n),
            'article_number': mpn, 'width': '225', 'ratio': '45', 'diameter': '17',
            'load_index': '94', 'speed_index': 'V', 'season': 'Sommerreifen',
            'size': '225/45 R17 94V', 'details': {'Herstellerartikelnummer': mpn}}
        a = dict(id=str(123456+n), brandname='Continental', manufacturer_number=mpn,
            width='225', height='45', inner_diameter='17', tyre_type='R', load_index='94',
            speed_index='V', type='S', dot='0', demo='0', da_decke='0', runflat='0', ms='0', **{'3pmfs':'0'})
        r = dict(Artikelnummer=str(654321+n), Hersteller='Continental', Herstellerartikelnummer=mpn,
            Reifenbreite='225', Reifenquerschnitt='45', Reifendurchmesser='17', Bauart='R',
            Loadindex='94', Speedindex='V', Saison='s', Artikeltyp='Reifen', Artikeluntertyp='PKW', Dot='', Demo='', Da='')
        rsu_sha = extension.SOURCES['old_rsu_csv_sha256'] if n % 2 else extension.SOURCES['new_rsu_csv_sha256']
        mapping = match_catalogues([alzura_record(a, archive_sha256=extension.SOURCES['alzura_archive_sha256'], row_number=n)],
            [rsu_record(r, csv_sha256=rsu_sha, row_number=n+10)])['mappings'][0]
        identity = variant_fingerprint(p)
        bindings[pid] = {'identity_hash': identity, 'category': 'tyre', 'mapping': mapping, 'expected_ean': '4019238594195'}
        proof = {'rule': 'alzura_full_export_nominal_text_v1', 'product_id': pid, 'identity_hash': identity,
            'alzura_source_sha256': extension.SOURCES['alzura_archive_sha256'], 'alzura_row_number': n,
            'alzura_article_id': a['id'], 'articlename': 'SYNTHETIC 225/45 R17 94 V XL',
            'description': '', 'info': '', 'manufacturer_description': '', 'runflat': '0',
            'rsu_source_sha256': rsu_sha, 'rsu_row_number': n+10, 'rsu_profile': 'SYNTHETIC XL'}
        proofs[pid] = {'proof': proof, 'proof_sha256': sha256(encode(proof)).hexdigest()}; rows.append(p)
    old = {pid: item for pid, item in proofs.items() if pid.endswith(format(126, '024x'))}
    extra = {pid: item for pid, item in proofs.items() if pid not in old}
    table = registry()
    from shop_rsu_rim_evidence import WIDTH_TABLE_SHA
    table['source']['sha256'] = WIDTH_TABLE_SHA
    bundle = {'products': rows, 'rsu_bindings': bindings, 'nominal_tyre_sources': old,
        'rsu_price_seeds': {'records': {pid: {'synthetic_price_unchanged': 12000} for pid in proofs}},
        'publication': {'released_ids': [p['selection_id'] for p in rows]},
        'reviewed_rim_width_registry': table, 'compatible_pairs': {}, 'verified_fitments': {},
        'documents': {'unchanged': True}, 'shipping_classes': {'unchanged': True}}
    value = {'schema_version': 1, 'base_bundle_sha256': 'a'*64,
        'nominal_tyre_sources': extra, 'sources': dict(extension.SOURCES)}
    return bundle, value


class TyreSourceExtensionChecks(unittest.TestCase):
    def setUp(self):
        guard = patch.object(socket, 'socket', side_effect=AssertionError('No network'))
        guard.start(); self.addCleanup(guard.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.bundle, self.value = fixture(); self.record = self.save()

    def save(self, raw=None):
        raw = encode(self.value) if raw is None else raw
        (self.root/'extension.json').write_bytes(raw)
        return {'path':'extension.json','sha256':sha256(raw).hexdigest(),'base_bundle_sha256':'a'*64}

    def load(self, record=None, bundle=None):
        return load_tyre_source_extension(self.root, record or self.record,
            self.bundle if bundle is None else bundle, base_bundle_sha256='a'*64)

    def test_absence_performs_no_file_reads(self):
        with patch.object(extension, '_read', side_effect=AssertionError('No file read')):
            self.assertIs(load_tyre_source_extension(self.root, None, self.bundle, base_bundle_sha256='a'*64), self.bundle)

    def test_exact_facts_append_only_in_ram_and_never_add_other_capabilities(self):
        before = deepcopy(self.bundle); result = self.load(); self.assertEqual(self.bundle, before)
        self.assertEqual(len(result['nominal_tyre_sources']), 126)
        for key in self.bundle:
            if key != 'nominal_tyre_sources': self.assertIs(result[key], self.bundle[key])
        pairs = released_width_pairs(result, approved_ids=set(result['rsu_bindings']))
        self.assertEqual(len(next(iter(pairs.values()))), 126)
        self.assertTrue(all(p['scope']=='rim_width_candidate' and 'fitment_spec' not in p
            for values in pairs.values() for p in values))

    def test_bad_descriptor_base_hash_paths_and_payload_schema_fail(self):
        for delta in ({'path':'../escape'},{'path':str(self.root/'extension.json')},{'sha256':'f'*64},
                      {'base_bundle_sha256':'f'*64},{'unexpected':True}):
            with self.subTest(delta=delta), self.assertRaises(TyreSourceExtensionError): self.load({**self.record,**delta})
        for field in ('registry_rows','rsu_price_seeds','rsu_bindings','verified_fitments'):
            changed = deepcopy(self.value); changed[field] = {}
            old = self.value; self.value = changed
            with self.assertRaises(TyreSourceExtensionError): self.load(self.save())
            self.value = old

    def test_exact_count_existing_disjoint_released_seed_and_binding_required(self):
        pid = next(iter(self.value['nominal_tyre_sources']))
        for mode in ('seed','binding','released','existing','duplicate'):
            b = deepcopy(self.bundle)
            if mode=='seed': b['rsu_price_seeds']['records'].pop(pid)
            elif mode=='binding': b['rsu_bindings'].pop(pid)
            elif mode=='released': b['publication']['released_ids'].remove(pid)
            elif mode=='existing': b['nominal_tyre_sources'][pid] = self.value['nominal_tyre_sources'][pid]
            else: b['products'].append(deepcopy(b['products'][1]))
            with self.subTest(mode=mode), self.assertRaises(TyreSourceExtensionError): self.load(bundle=b)
        self.value['nominal_tyre_sources'].pop(pid)
        with self.assertRaises(TyreSourceExtensionError): self.load(self.save())

    def test_source_receipts_original_row_identity_and_fingerprint_cannot_be_rebased(self):
        pid = next(iter(self.value['nominal_tyre_sources'])); before = deepcopy(self.value)
        for key,value in [('alzura_row_number',9999),('rsu_source_sha256','f'*64),('identity_hash','f'*64)]:
            self.value = deepcopy(before); self.value['nominal_tyre_sources'][pid]['proof'][key] = value
            rehash(self.value,pid)
            with self.subTest(key=key), self.assertRaises(TyreSourceExtensionError): self.load(self.save())
        self.value = deepcopy(before); self.value['sources']['old_rsu_csv_sha256'] = 'f'*64
        with self.assertRaises(TyreSourceExtensionError): self.load(self.save())

    def test_ineligible_special_execution_or_borrowed_non_conti_table_does_not_pass_old_pairs(self):
        pid = next(iter(self.value['nominal_tyre_sources'])); self.value['nominal_tyre_sources'][pid]['proof']['info'] = 'SSR'
        rehash(self.value,pid); b = deepcopy(self.bundle)
        b['compatible_pairs'] = {'alloy_wheel:'+'b'*24:[{'tyre_id':pid}]}
        with self.assertRaisesRegex(TyreSourceExtensionError,'not_eligible'): self.load(self.save(),bundle=b)
        self.bundle['reviewed_rim_width_registry']['brands'] = ['Michelin']
        with self.assertRaises(TyreSourceExtensionError): self.load(self.save())

    def test_oversize_duplicate_nonfinite_and_symlink_rejected(self):
        for raw in (b' '*(extension.MAX_BYTES+1), b'{"x":1,"x":2}', b'{"x":NaN}'):
            with self.subTest(size=len(raw)), self.assertRaises(TyreSourceExtensionError): self.load(self.save(raw))
        self.record = self.save(); link = self.root/'alias.json'
        try: link.symlink_to(self.root/'extension.json')
        except OSError: self.skipTest('Synthetic symlinks unavailable')
        with self.assertRaises(TyreSourceExtensionError): self.load({**self.record,'path':'alias.json'})


if __name__=='__main__': unittest.main(verbosity=2)
