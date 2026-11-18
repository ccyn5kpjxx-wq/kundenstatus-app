"""Exact-source nominal proposals; fixtures grant no actual vehicle fitment."""
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
import json
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from shop_catalog_public import variant_fingerprint
from shop_catalog_enriched import EnrichedPublicCatalog
from shop_rim_width_candidates import released_width_pairs
from shop_rsu_match import alzura_record, match_catalogues, rsu_record
from shop_rsu_rim_evidence import WIDTH_TABLE_SHA
from test_shop_rim_width_candidates import registry, rim
from test_shop_catalog_public import NOW, wheel

TYRE_ID = 'tyre:' + 'a' * 24
RIM_ID = rim()['selection_id']


def rehash(bundle, identifier=TYRE_ID):
    entry = bundle['nominal_tyre_sources'][identifier]
    entry['proof_sha256'] = sha256(json.dumps(entry['proof'], sort_keys=True,
        ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def fixture(*, manufacturer='Continental', runflat='0', index=0):
    identifier = 'tyre:' + format(index, '024x') if index else TYRE_ID
    mpn = 'SYNTHETIC-225-17' + (str(index) if index else '')
    alz_id, rsu_id = str(123456 + index), str(654321 + index)
    left_row, right_row = 1 + index, 2 + index
    p = {'selection_id': identifier, 'category': 'tyre', 'dimensions_valid': True,
        'manufacturer_name': manufacturer, 'name': manufacturer + ' synthetic fixture' + (str(index) if index else ''),
        'article_number': mpn, 'width': '225', 'ratio': '45', 'diameter': '17',
        'load_index': '94', 'speed_index': 'V', 'season': 'Sommerreifen',
        'size': '225/45 R17 94V', 'details': {'Herstellerartikelnummer': mpn}}
    a = dict(id=alz_id, brandname=manufacturer, manufacturer_number=mpn,
        width='225', height='45', inner_diameter='17', tyre_type='R', load_index='94',
        speed_index='V', type='S', dot='0', demo='0', da_decke='0', runflat=runflat,
        ms='0', **{'3pmfs':'0'})
    r = dict(Artikelnummer=rsu_id, Hersteller=manufacturer,
        Herstellerartikelnummer=mpn, Reifenbreite='225', Reifenquerschnitt='45',
        Reifendurchmesser='17', Bauart='R', Loadindex='94', Speedindex='V', Saison='s',
        Artikeltyp='Reifen', Artikeluntertyp='PKW', Dot='', Demo='', Da='')
    mapping = match_catalogues([alzura_record(a, archive_sha256='a'*64, row_number=left_row)],
        [rsu_record(r, csv_sha256='b'*64, row_number=right_row)])['mappings'][0]
    fp = variant_fingerprint(p)
    proof = {'rule':'alzura_full_export_nominal_text_v1', 'product_id':identifier,
        'identity_hash':fp, 'alzura_source_sha256':'a'*64, 'alzura_row_number':left_row,
        'alzura_article_id':alz_id, 'articlename':'SYNTHETIC 225/45 R17 94 V XL',
        'description':'', 'info':'', 'manufacturer_description':'', 'runflat':'0',
        'rsu_source_sha256':'b'*64, 'rsu_row_number':right_row, 'rsu_profile':'SYNTHETIC XL'}
    reviewed = registry()
    reviewed['source']['sha256'] = WIDTH_TABLE_SHA
    b = {'products':[p, rim()], 'reviewed_rim_width_registry':reviewed,
        'compatible_pairs':{}, 'rsu_bindings':{identifier:{'identity_hash':fp,
        'category':'tyre', 'mapping':mapping, 'expected_ean':'4019238594195'}},
        'nominal_tyre_sources':{identifier:{'proof':proof}}}
    rehash(b, identifier)
    return b


class ReleasedWidthPairChecks(unittest.TestCase):
    def setUp(self):
        blocker = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network'))
        blocker.start()
        self.addCleanup(blocker.stop)

    def candidates(self, bundle):
        return released_width_pairs(bundle, approved_ids={TYRE_ID})

    def test_source_bound_pair_is_only_width_candidate_and_input_is_unchanged(self):
        bundle = fixture()
        before = deepcopy(bundle)
        result = self.candidates(bundle)
        self.assertEqual(bundle, before)
        pair = result[RIM_ID][0]
        self.assertEqual(pair['scope'], 'rim_width_candidate')
        self.assertEqual(pair['tyre_id'], TYRE_ID)
        self.assertEqual(pair['tyre_identity'], variant_fingerprint(bundle['products'][0]))
        self.assertEqual(pair['source']['sha256'], WIDTH_TABLE_SHA)
        for key in ('fitment_spec', 'approved', 'vehicle_id', 'certificate_reference'):
            self.assertNotIn(key, pair)

    def test_sixth_documented_offer_survives_until_quantity_check_and_display_limit(self):
        bundle = fixture(index=1)
        for index in range(2, 7):
            extra = fixture(index=index)
            bundle['products'].append(extra['products'][0])
            for key in ('rsu_bindings', 'nominal_tyre_sources'):
                bundle[key].update(extra[key])
        bundle['products'][1].update(name='SYNTHETIC rim', manufacturer_name='SYNTHETIC', size='7.5 x 17')
        tyres = [p for p in bundle['products'] if p['category'] == 'tyre']
        identifiers = {p['selection_id'] for p in tyres}
        for p in tyres:
            p['quote_public'] = deepcopy(wheel()['quote_public'])
            p['quote_public']['source'] = 'RSU'
        tyres[0]['quote_public']['stock_reported'] = 2
        pairs = released_width_pairs(bundle, approved_ids=identifiers)
        self.assertEqual(len(pairs[RIM_ID]), 6)
        catalogue = EnrichedPublicCatalog(bundle['products'],
            released_ids=[p['selection_id'] for p in bundle['products']],
            compatible_pairs=pairs, procurement_ids=identifiers, rsu_only_prices=True, clock=lambda: NOW)
        result = catalogue.compatible_tyres(RIM_ID, quantity=4, season='summer')
        self.assertEqual(len(result['items']), 5)
        self.assertEqual({p['id'] for p in result['items']}, identifiers - {tyres[0]['selection_id']})
        self.assertIn(tyres[-1]['selection_id'], {p['id'] for p in result['items']})
        self.assertEqual(result['scope'], 'rim_width_candidate')
        self.assertTrue(all(not p['order_enabled'] for p in result['items']))

    def test_exact_load_speed_may_use_compact_source_spelling(self):
        bundle = fixture()
        bundle['nominal_tyre_sources'][TYRE_ID]['proof']['articlename'] = 'SYNTHETIC 225/45 R17 94V XL'
        rehash(bundle)
        self.assertEqual(self.candidates(bundle)[RIM_ID][0]['tyre_id'], TYRE_ID)

    def test_absent_registry_preserves_detached_old_pairs_and_sources_need_price_approval(self):
        bundle = fixture()
        bundle.pop('reviewed_rim_width_registry')
        bundle['compatible_pairs'] = {RIM_ID:[{'scope':'rim_width_candidate','tyre_id':TYRE_ID}]}
        result = self.candidates(bundle)
        self.assertEqual(result, bundle['compatible_pairs'])
        result[RIM_ID].clear()
        self.assertTrue(bundle['compatible_pairs'][RIM_ID])
        with self.assertRaisesRegex(ValueError, 'invalid_nominal_sources'):
            released_width_pairs(fixture(), approved_ids=set())

    def test_tampered_hash_or_wrong_source_references_are_rejected(self):
        bundle = fixture()
        bundle['nominal_tyre_sources'][TYRE_ID]['proof']['info'] = 'changed without approval'
        with self.assertRaisesRegex(ValueError, 'invalid_nominal_source'):
            self.candidates(bundle)
        for key, value in (('alzura_source_sha256','c'*64),('alzura_row_number',3),
                ('alzura_article_id','123457'),('rsu_source_sha256','d'*64),('rsu_row_number',3)):
            bundle = fixture()
            bundle['nominal_tyre_sources'][TYRE_ID]['proof'][key] = value
            rehash(bundle)
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'invalid_nominal_source_reference'):
                self.candidates(bundle)

    def test_foreign_maker_special_execution_and_changed_nominal_size_are_not_proposed(self):
        self.assertEqual(self.candidates(fixture(manufacturer='Michelin')), {})
        for field in ('articlename','description','info','manufacturer_description','rsu_profile'):
            bundle = fixture()
            bundle['nominal_tyre_sources'][TYRE_ID]['proof'][field] += ' SSR'
            rehash(bundle)
            with self.subTest(field=field):
                self.assertEqual(self.candidates(bundle), {})
        bundle = fixture()
        bundle['nominal_tyre_sources'][TYRE_ID]['proof']['articlename'] = 'SYNTHETIC 235/45 R17 94 V XL'
        rehash(bundle)
        self.assertEqual(self.candidates(bundle), {})

    def test_known_runflat_source_cannot_be_relabeled_standard(self):
        with self.assertRaisesRegex(ValueError, 'invalid_nominal_source_reference'):
            self.candidates(fixture(runflat='1'))

    def test_changed_product_fingerprint_cannot_reuse_old_binding(self):
        bundle = fixture()
        bundle['products'][0]['name'] = 'Changed exact product'
        bundle['nominal_tyre_sources'][TYRE_ID]['proof']['identity_hash'] = variant_fingerprint(bundle['products'][0])
        rehash(bundle)
        with self.assertRaisesRegex(ValueError, 'nominal_product_source_conflict'):
            self.candidates(bundle)

    def test_recomputed_fingerprint_cannot_apply_continental_table_to_foreign_source(self):
        bundle = fixture(manufacturer='Michelin')
        bundle['products'][0]['manufacturer_name'] = 'Continental'
        fp = variant_fingerprint(bundle['products'][0])
        bundle['rsu_bindings'][TYRE_ID]['identity_hash'] = fp
        bundle['nominal_tyre_sources'][TYRE_ID]['proof']['identity_hash'] = fp
        rehash(bundle)
        with self.assertRaisesRegex(ValueError, 'nominal_product_source_conflict'):
            self.candidates(bundle)

    def test_recomputed_fingerprint_cannot_substitute_another_mpn(self):
        bundle = fixture()
        bundle['products'][0]['article_number'] = 'OTHER-MPN'
        fp = variant_fingerprint(bundle['products'][0])
        bundle['rsu_bindings'][TYRE_ID]['identity_hash'] = fp
        bundle['nominal_tyre_sources'][TYRE_ID]['proof']['identity_hash'] = fp
        rehash(bundle)
        with self.assertRaisesRegex(ValueError, 'nominal_product_source_conflict'):
            self.candidates(bundle)


if __name__ == '__main__':
    unittest.main()
