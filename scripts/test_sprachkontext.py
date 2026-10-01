"""Synthetic, offline voice-preload budget and truthfulness regressions."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_sprachkontext import (
    compact_voice_context, MAX_CONTEXT_CHARS, MAX_CONTEXT_BYTES,
    MAX_SELECTED_CHARS, MAX_MATERIAL_CHARS, MAX_ORDER_CHARS,
)


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def order(ident=156):
    return {'id': ident, 'fahrzeug': 'Testwagen', 'kennzeichen': 'TEST-AA 1',
            'status': 3, 'produktion_schritt': 'lackierung', 'lackierbereit': 0,
            'fertig_datum': '02.10.2026', 'fertig_uhrzeit': '15:30',
            'abholtermin': '2026-10-03', 'abhol_uhrzeit': '11:15',
            'transport_art': 'hol_bring', 'farbcode': 'LZ9Y', 'farbton': 'Schwarz',
            'beschreibung': 'Stoßfänger vorne rechts lackieren.',
            'analyse_text': 'Langer synthetischer Dokumenttext. ' * 1000,
            'werkstatt_angebot_text': 'Synthetische Angebotspositionen. ' * 1000}


def variant(width='30 mm', ident='TEST-30'):
    return {'variante_id': ident, 'produkt_name': 'Testband HydroGreen',
            'lieferant': 'Testlieferant', 'artikelnummer': ident,
            'groesse': width, 'farbe': '', 'gebinde': 'Karton', 've': 'VE',
            'pruefen': True, 'bestellbar': False,
            'farbabgleich': {'basis': 'produktname_alias', 'bestaetigt': False,
                            'namenshinweis': 'HydroGreen', 'hinweis': 'Ungeprüfte Farbe.'},
            'packinhalt': {'menge': '32', 'einheit': 'Stück', 'pro': 'VE',
                          'pruefen': True, 'quelle': {'art': 'einkauf', 'beleg_id': 91, 'position': 2}},
            'uebliche_menge': {'menge': '2', 'einheit': 'VE', 'pruefen': True},
            'quellen': [{'art': 'einkauf', 'beleg_id': 91, 'position': 2,
                         'datei_sha256': 'a' * 64}], 'mengenhistorie': [{'menge': '2'}]}


class VoiceContextTests(unittest.TestCase):
    def assert_budget(self, result):
        text = encoded(result)
        self.assertLessEqual(len(text), MAX_CONTEXT_CHARS)
        self.assertLessEqual(len(text.encode('utf-8')), MAX_CONTEXT_BYTES)
        for row in result.get('auftraege', []):
            self.assertLessEqual(len(encoded(row)), MAX_ORDER_CHARS)
        if 'ausgewaehlter_auftrag' in result:
            self.assertLessEqual(len(encoded(result['ausgewaehlter_auftrag'])), MAX_SELECTED_CHARS)
        if 'materialwissen' in result:
            self.assertLessEqual(len(encoded(result['materialwissen'])), MAX_MATERIAL_CHARS)

    def test_work_texts_are_reloaded_not_sliced_and_source_unchanged(self):
        context = {'stand': '2026-10-01T09:00:00+02:00', 'modus': 'live', 'native': True,
                   'kalender': {'zeitzone': 'Europe/Berlin', 'heute': '2026-10-01'},
                   'auftraege': [order()], 'next_offset': None}
        original = deepcopy(context)
        result = compact_voice_context(context)
        self.assertEqual(context, original)
        row = result['auftraege'][0]
        for field in ('id', 'status', 'fertig_datum', 'fertig_uhrzeit', 'abholtermin',
                      'abhol_uhrzeit', 'farbcode', 'farbton', 'beschreibung'):
            self.assertEqual(row[field], context['auftraege'][0][field])
        self.assertTrue(row['arbeitsdetails_abrufen'])
        self.assertIn('analyse_text', row['ausgelassene_felder'])
        self.assertNotIn('analyse_text', row)
        self.assertNotIn('werkstatt_angebot_text', row)
        self.assertEqual(result['kalender'], context['kalender'])
        self.assertFalse(result['gekuerzt'])  # All index rows are still present.
        self.assertTrue(result['sprachkontext_gekuerzt'])
        result['kalender']['heute'] = 'changed'
        row['status'] = 9
        self.assertEqual(context, original)
        self.assert_budget(result)

    def test_long_description_is_absent_with_marker_short_description_is_complete(self):
        for length in (300, 301, 100000):
            raw = {'id': 1, 'beschreibung': 'x' * length}
            result = compact_voice_context({'auftraege': [raw]})['auftraege'][0]
            if length <= 300:
                self.assertEqual(result['beschreibung'], raw['beschreibung'])
            else:
                self.assertNotIn('beschreibung', result)
                self.assertTrue(result['arbeitsdetails_abrufen'])
                self.assertIn('beschreibung', result['ausgelassene_felder'])

    def test_large_index_keeps_prefix_and_real_nonzero_cursor(self):
        rows = [order(i) for i in range(80, 300)]
        result = compact_voice_context({'auftraege': rows, 'offset': 40, 'next_offset': 260})
        ids = [row['id'] for row in result['auftraege']]
        self.assertGreater(len(ids), 0)
        self.assertLess(len(ids), len(rows))
        self.assertEqual(ids, [row['id'] for row in rows[:len(ids)]])
        self.assertEqual(result['next_offset'], 40 + len(ids))
        self.assertTrue(result['gekuerzt'])
        self.assert_budget(result)

    def test_upstream_cursor_and_partial_flag_preserved_when_all_fit(self):
        result = compact_voice_context({'auftraege': [{'id': 7}], 'next_offset': 61, 'offset': 60})
        self.assertEqual(result['next_offset'], 61)
        self.assertTrue(result['gekuerzt'])

    def test_both_order_numbers_and_bank_redaction_survive_row_pressure(self):
        raw = order(156)
        raw.update(auftragsnummer='EXTERN-TEST-9401', bankdaten_entfernt=True,
                   fahrzeug='W' * 120, kennzeichen='K' * 120,
                   farbcode='C' * 120, farbton='F' * 120, farbton_2='S' * 120)
        result = compact_voice_context({'auftraege': [raw]})
        row = result['auftraege'][0]
        self.assertEqual(row['id'], 156)
        self.assertEqual(row['auftragsnummer'], 'EXTERN-TEST-9401')
        self.assertTrue(row['bankdaten_entfernt'])
        for key in ('farbcode', 'farbton', 'farbton_2'):
            self.assertTrue(key not in row or row[key] == raw[key])
        self.assert_budget(result)
        result = compact_voice_context({'auftraege': [], 'next_offset': None, 'gekuerzt': True})
        self.assertTrue(result['gekuerzt'])

    def test_selected_order_has_priority_and_document_absence_is_not_invented(self):
        selected = order(156)
        selected.update(dokumente=[{'id': i, 'original_name': f'Test-{i}.pdf',
                                    'dokument_typ': 'Gutachten', 'text': 'never include' * 1000}
                                   for i in range(20)],
                        teile=[{'bezeichnung': 'Testteil', 'status': 'offen', 'notiz': 'x' * 9000}],
                        modus='lesestand', quelle='/admin/auftrag/156')
        source = {'auftraege': [order(i) for i in range(100)], 'ausgewaehlter_auftrag': selected}
        result = compact_voice_context(source)
        detail = result['ausgewaehlter_auftrag']
        self.assertEqual(detail['id'], 156)
        self.assertEqual(detail['modus'], 'lesestand')
        self.assertEqual(detail['quelle'], selected['quelle'])
        self.assertEqual(detail['dokumente_anzahl'], 20)
        self.assertTrue(detail['arbeitsdetails_abrufen'])
        self.assertLessEqual(len(detail['dokumente']), 3)
        self.assertNotIn('never include', encoded(result))
        for row in result['auftraege']:
            self.assertNotIn('dokumente_anzahl', row)
        self.assert_budget(result)

    def test_material_units_source_uncertainty_and_rights_remain(self):
        source = {'auftraege': [], 'materialwissen': {'pruefen': True, 'bestellbar': False,
                  'varianten': [variant()], 'varianten_gekuerzt': False,
                  'abdeckung': {'vollstaendigkeit_bestaetigt': False, 'offene_auslese': 7}}}
        original = deepcopy(source)
        result = compact_voice_context(source)
        material = result['materialwissen']
        row = material['varianten'][0]
        self.assertEqual(row['artikelnummer'], 'TEST-30')
        self.assertEqual(row['groesse'], '30 mm')
        self.assertEqual(row['ve'], 'VE')
        self.assertEqual(row['packinhalt'], source['materialwissen']['varianten'][0]['packinhalt'])
        self.assertFalse(row['farbabgleich']['bestaetigt'])
        self.assertEqual(row['farbe'], '')
        self.assertFalse(row['bestellbar'])
        self.assertTrue(row['details_abrufen'])
        self.assertEqual(material['abdeckung'], source['materialwissen']['abdeckung'])
        self.assertFalse(material['varianten_gekuerzt'])
        self.assertNotIn('datei_sha256', encoded(row))
        row['packinhalt']['quelle']['beleg_id'] = 4
        self.assertEqual(source, original)
        self.assertNotIn('materialwissen', compact_voice_context({'auftraege': []}))
        self.assertNotIn('materialfoto_auswahl', compact_voice_context({'auftraege': []}))
        self.assert_budget(result)

    def test_material_prefix_and_whole_oversized_variant_omission(self):
        source = {'auftraege': [], 'materialwissen': {'varianten': [variant(str(i) + ' mm', str(i)) for i in range(50)]}}
        result = compact_voice_context(source)
        material = result['materialwissen']
        self.assertTrue(material['varianten_gekuerzt'])
        self.assertGreater(len(material['varianten']), 0)
        self.assertLess(len(material['varianten']), 50)
        self.assert_budget(result)
        bad = variant('9' * 1000 + ' mm')
        result = compact_voice_context({'auftraege': [], 'materialwissen': {'varianten': [bad, variant()]}})
        self.assertEqual(result['materialwissen']['varianten'], [])
        self.assertTrue(result['materialwissen']['varianten_gekuerzt'])

    def test_outage_and_photo_reference_are_not_lost(self):
        photo = dict(variant(), foto_id='synthetic-photo', treffer_id='synthetic-hit')
        result = compact_voice_context({'auftraege': [order(i) for i in range(50)],
                   'materialwissen': {'verfuegbar': False, 'hinweis': 'Nicht verfügbar'},
                   'materialfoto_auswahl': photo})
        self.assertIs(result['materialwissen']['verfuegbar'], False)
        self.assertEqual(result['materialfoto_auswahl']['foto_id'], 'synthetic-photo')
        self.assertEqual(result['materialfoto_auswahl']['treffer_id'], 'synthetic-hit')
        self.assert_budget(result)

    def test_unicode_huge_fields_and_nested_junk_have_hard_global_bounds(self):
        nested = {'text': 'secret junk' * 100000}
        for _ in range(120):
            nested = {'deeper': nested}
        row = order()
        row.update(fahrzeug='🚗' * 120, kennzeichen='界' * 120, farbcode='界' * 10000,
                   beschreibung='🚗' * 300, analyse_text=nested, bauteile_override=nested)
        source = {'stand': 'x' * 100000, 'hinweis': 'y' * 100000,
                  'kalender': {'heute': nested}, 'auftraege': [dict(row, id=i) for i in range(300)],
                  'ausgewaehlter_auftrag': row, 'materialwissen': nested,
                  'materialfoto_auswahl': nested, 'unknown': nested}
        result = compact_voice_context(source)
        self.assertEqual(result['ausgewaehlter_auftrag']['id'], 156)
        self.assertNotIn('secret junk', encoded(result))
        self.assertIn('stand', result['ausgelassene_felder'])
        self.assertIn('hinweis', result['ausgelassene_felder'])
        self.assertTrue(result['gekuerzt'])
        self.assert_budget(result)

    def test_malformed_values_never_break_json_or_skip_index_holes(self):
        bad_values = [float('nan'), float('inf'), '\ud800', {'nested': [1]}, 10**5000]
        for bad in bad_values:
            result = compact_voice_context({'auftraege': [{'id': 1, 'fahrzeug': bad}, None, {'id': 3}],
                                            'native': bad, 'materialwissen': {'verfuegbar': False}})
            self.assertEqual([row['id'] for row in result['auftraege']], [1])
            self.assertEqual(result['next_offset'], 1)
            self.assertIs(result['materialwissen']['verfuegbar'], False)
            self.assert_budget(result)
        self.assert_budget(compact_voice_context(None))

    def test_all_material_metadata_is_bounded_even_without_variants(self):
        coverage_keys = ('quellen_gesamt', 'freigegebene_quellen', 'ungeklaerte_quellen',
                         'offene_auslese', 'auslese_zu_pruefen', 'positionen',
                         'gespeicherte_artikel', 'sichtbare_positionen', 'begrenzt',
                         'positionen_begrenzt', 'vollstaendigkeit_bestaetigt')
        material = {'verfuegbar': False, 'suchstatus': '🚗' * 100,
                    'abdeckung': dict.fromkeys(coverage_keys, '🚗' * 60)}
        for variants in (None, [], [variant()]):
            if variants is not None:
                material['varianten'] = variants
            result = compact_voice_context({'auftraege': [order(i) for i in range(50)],
                                            'materialwissen': material,
                                            'ausgewaehlter_auftrag': order(156)})
            self.assertIs(result['materialwissen']['verfuegbar'], False)
            self.assertLessEqual(len(encoded(result['materialwissen']).encode('utf-8')), 2400)
            self.assert_budget(result)


if __name__ == '__main__':
    unittest.main()
