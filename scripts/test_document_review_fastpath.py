"""Review-only parser optimization: identical output; uploads keep dealer matching.

Run with python scripts/test_document_review_fastpath.py. The audited smoke
harness blocks network/workers and sends writes to disposable synthetic data.
"""
from contextlib import closing
from io import BytesIO
from pathlib import Path
import json
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'perf-20261026'))
import test_navigation_readmodels as support
from werkzeug.datastructures import FileStorage

p = support.p
TEXT = '''Synthetischer Lackierauftrag
Fahrzeug: Testwagen
Kennzeichen: NAV-AB 123
FIN: WVWZZZ1JZXW000001
HSN: 0603
TSN: ABC
Auftragsnummer: NAV-REVIEW-001
Annahme: 26.10.2026
Fertig bis: 28.10.2026
Motorhaube lackieren und Stoßfänger instand setzen.
Reparaturkosten: 1234,56 EUR
'''
AI_FIELDS = {
    'fahrzeug': 'Synthetischer Testwagen', 'kennzeichen': 'NAV-AB 123',
    'fin_nummer': 'WVWZZZ1JZXW000001', 'hsn_nummer': '0603', 'tsn_nummer': 'ABC',
    'auftragsnummer': 'NAV-REVIEW-001', 'rep_max_kosten': '1234,56',
    'bauteile_override': 'Motorhaube', 'analyse_text': 'Synthetischer Arbeitsumfang',
    'beschreibung': 'Synthetische Beschreibung', 'annahme_datum': '26.10.2026',
    'fertig_datum': '28.10.2026', 'analyse_pruefen': True, 'analyse_confidence': 0.8,
    # Stored analysis_json uses the provider wire names for these fields.
    'vehicle_type': 'VW Golf', 'auftrags_datum': '26.10.2026', 'fertig_bis': '28.10.2026',
    'kurzanalyse': 'Motorhaube lackieren', 'lesefassung': 'Synthetische Beschreibung',
    'offene_bauteile': ['Motorhaube'], 'needs_review': True, 'confidence': 0.8,
}


class DocumentReviewFastpathTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.order_id = 66000
        support.insert_order(cls.order_id, kennzeichen='NAV-AB 123')
        with closing(p.get_db()) as db:
            for index, (public, category, purpose) in enumerate((
                    (0, 'standard', 'pruefen'), (1, 'standard', None),
                    (1, 'standard', 'pruefen'), (1, 'standard', 'ablage'),
                    (1, None, 'pruefen'))):
                db.execute('''INSERT INTO dateien
                    (auftrag_id, original_name, stored_name, mime_type, quelle,
                     kategorie, dokument_zweck, kunde_sichtbar, partner_sichtbar,
                     versicherung_sichtbar, sichtbarkeit_geprueft,
                     extrahierter_text, analyse_json, hochgeladen_am)
                    VALUES (?, ?, ?, 'application/pdf', 'intern', ?, ?, ?, ?, ?, 1, ?, ?, ?)''',
                    (cls.order_id, f'review-fastpath-{index}.pdf', f'review-fastpath-{index}.pdf',
                     category, purpose, public, public, public, TEXT,
                     json.dumps(AI_FIELDS), f'2026-10-{20-index:02d} 08:00:00'))
            db.commit()

    def test_review_output_sorting_visibility_and_all_fields_equal_legacy_parser_path(self):
        original_parser = p.parse_document_fields
        def legacy_parser(text, filename='', **kwargs):
            return original_parser(text, filename, include_autohaus_match=True)

        with p.app.test_request_context(f'/admin/auftrag/{self.order_id}'):
            order = p.get_auftrag(self.order_id)
            loaded = p.list_dateien(self.order_id)
            results = {}
            for audience in (None, 'kunde', 'partner', 'versicherung'):
                with self.subTest(audience=audience):
                    with patch.object(p, 'find_matching_autohaus_id', return_value=12345) as old_match:
                        with patch.object(p, 'parse_document_fields', legacy_parser):
                            expected = p.list_document_review_items(
                                self.order_id, order, audience, dateien=loaded)
                    self.assertTrue(old_match.called)
                    with patch.object(p, 'find_matching_autohaus_id',
                                      side_effect=AssertionError('review must not match dealers')):
                        actual = p.list_document_review_items(
                            self.order_id, order, audience, dateien=loaded)
                    self.assertEqual(actual, expected)
                    ids = [row['datei_id'] for row in actual]
                    self.assertEqual(ids, sorted(ids, reverse=True))
                    results[audience] = actual
            self.assertGreater(len(results[None]), len(results['kunde']))
            returned_fields = {item['key'] for row in results[None] for item in row['items']}
            self.assertEqual(returned_fields, {key for key, _ in p.DOCUMENT_REVIEW_FIELDS})

    def test_real_detail_get_never_calls_autohaus_matcher_for_review_documents(self):
        client = support.admin_client()
        with patch.object(p, 'find_matching_autohaus_id',
                          side_effect=AssertionError('detail GET must not match dealers')):
            with support.selects() as statements:
                response = client.get(f'/admin/auftrag/{self.order_id}')
        self.assertEqual(response.status_code, 200)
        self.assertIn('review-fastpath-0.pdf', response.get_data(as_text=True))
        self.assertFalse(any('SELECT id, name FROM autohaeuser' in sql for sql in statements))

    def test_default_parser_keeps_matching_and_opt_out_changes_only_autohaus_id(self):
        with patch.object(p, 'find_matching_autohaus_id', return_value=12345) as match:
            default = p.parse_document_fields(TEXT, 'synthetic.pdf')
            explicit = p.parse_document_fields(TEXT, 'synthetic.pdf', include_autohaus_match=True)
        self.assertEqual(match.call_count, 2)
        self.assertEqual(default, explicit)
        self.assertEqual(default['autohaus_id'], 12345)
        with patch.object(p, 'find_matching_autohaus_id', side_effect=AssertionError('opt-out must skip matching')):
            fast = p.parse_document_fields(TEXT, 'synthetic.pdf', include_autohaus_match=False)
        self.assertIsNone(fast.get('autohaus_id'))
        self.assertEqual({k: v for k, v in fast.items() if k != 'autohaus_id'},
                         {k: v for k, v in default.items() if k != 'autohaus_id'})

    def test_real_upload_summary_path_still_uses_default_matching(self):
        upload = FileStorage(stream=BytesIO(b'%PDF-1.4 synthetic upload'),
                             filename='synthetic-upload.pdf', content_type='application/pdf')
        # Replace OCR/provider extraction only. Real save_uploads and its summary
        # parser still run, proving upload callers retain the default matcher.
        bundle = {'text': TEXT, 'source': 'synthetic', 'structured': {},
                  'analysis_json': '', 'hint': '', 'status': 'ok'}
        with patch.object(p, 'build_document_analysis_bundle_safe', return_value=bundle):
            with patch.object(p, 'find_matching_autohaus_id', return_value=12345) as match:
                saved, _ = p.save_uploads(self.order_id, [upload], 'intern', analyze=True)
        self.assertEqual(saved, 1)
        self.assertTrue(match.called)


if __name__ == '__main__':
    unittest.main(verbosity=2)
