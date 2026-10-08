"""Historical evidence only: synthetic regressions, optional read-only archive.

Set TOPCOLOR_ARCHIVE_SAMPLE_DIR to an external invoice directory to exercise
real PDFs. No originals, prices, production DB, network or app worker in Git.
"""
from copy import deepcopy
from pathlib import Path
import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import werkstatt_rechnungsquelle as reader
from werkstatt_artikel_import import InvoiceCatalog, _price_evidence


def footer(rate='19,00', vat='19,00', total='119,00', currency='EUR'):
    def word(x, right, y, text):
        return (x, y, right, y + 10, text, 0, 0, 0)
    return {'page': 2, 'words': [
        word(405, 429, 650, 'Netto'), word(435, 449, 650, currency), word(519, 557, 650, '100,00'),
        word(405, 433, 662, 'MwSt.'), word(440, 446, 662, '%'), word(453, 478, 662, rate), word(526, 557, 662, vat),
        word(405, 489, 686, 'Rechnungs-Betrag'), word(493, 510, 686, currency), word(519, 557, 686, total)]}


class InvoiceMetadataTests(unittest.TestCase):
    def test_native_followup_page_conflicting_invoice_date_is_unknown(self):
        import tempfile
        from types import SimpleNamespace
        import fitz
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'synthetic.pdf'
            with fitz.open() as document:
                for day in ('11.09.2026', '12.09.2026'):
                    page = document.new_page()
                    page.insert_text((50, 50), 'Beleg-Datum: ' + day)
                document.save(path)
            result = reader._result('einkauf', 1)
            result['source']['supplier'] = 'TOP-Color GmbH'
            portal = SimpleNamespace(get_fitz=lambda: fitz)
            native = {'complete': True, 'positions': [], 'warnings': []}
            with patch('werkstatt_topcolor_positionen.parse_topcolor_pages', return_value=native) as parser:
                reader._read_file(portal, path, 'synthetic', result, Path(temporary))
            parser.assert_called_once()
            self.assertIsNone(result['source']['date'])
            self.assertTrue(any('Widersprüchliche' in warning for warning in result['warnings']))

    def test_document_date_in_native_topcolor_header_not_delivery_or_order_date(self):
        result = reader._result('einkauf', 1)
        reader._date_from_text('Beleg-Nr.\nBeleg-Datum\nWBAL26-RE000001\n= Leistungsdatum\n11.09.2026\n'
                               'Lieferschein-Datum\n07.09.2026\nAuftrag-Datum\n01.09.2026', result)
        self.assertEqual(result['source']['date'], '2026-09-11')

    def test_conflict_between_explicit_dates_or_known_source_stays_unknown(self):
        for source_date, text in ((None, 'Beleg-Datum: 11.09.2026\nRechnungsdatum: 12.09.2026'),
                                  ('2026-09-10', 'Beleg-Datum 11.09.2026')):
            result = reader._result('einkauf', 1)
            result['source']['date'] = source_date
            reader._date_from_text(text, result)
            reader._date_from_text('Rechnungsdatum 11.09.2026', result)
            self.assertIsNone(result['source']['date'])
            self.assertTrue(result['warnings'])

    def test_unlabeled_ingestion_or_delivery_date_never_used(self):
        for text in ('erstellt_am:2026-10-08', 'Lieferschein-Datum\n11.09.2026',
                     'Beleg-Datum\nLieferschein-Datum\n11.09.2026', 'Rechnungsdatum 31.02.2026'):
            result = reader._result('einkauf', 1)
            reader._date_from_text(text, result)
            self.assertIsNone(result['source']['date'])

    def test_explicit_summary_currency_tax_and_arithmetic_required(self):
        result = reader.topcolor_price_metadata([footer()])
        self.assertEqual((result['currency'], result['tax_basis'], result['tax_rate']), ('EUR', 'net', '19'))
        self.assertTrue(result['metadata_source']['tax_summary_reconciled'])
        other = reader.topcolor_price_metadata([footer(rate='7,00', vat='7,00', total='107,00')])
        self.assertEqual(other['tax_rate'], '7')
        for page in (footer(currency='USD'), footer(vat='18,00'), footer(total='120,00')):
            self.assertEqual(reader.topcolor_price_metadata([page]), {})

    def test_misaligned_rate_and_mixed_summaries_stay_unknown(self):
        malformed = footer()
        word = list(malformed['words'][5]); word[0] = 300; malformed['words'][5] = tuple(word)
        self.assertEqual(reader.topcolor_price_metadata([malformed]), {})
        self.assertEqual(reader.topcolor_price_metadata([footer(), footer(rate='7,00', vat='7,00', total='107,00')]), {})
        self.assertEqual(reader.topcolor_price_metadata([footer(), footer(vat='18,00')]), {})
        self.assertEqual(reader.topcolor_price_metadata([{'page': 1, 'words': [(0, 0, 20, 10, '19,00')]}]), {})

    def test_metadata_whitelist_keeps_provenance_without_verifying_price(self):
        metadata = reader.topcolor_price_metadata([footer()])
        evidence = _price_evidence(dict(metadata, value='10.00', basis='gebindepreis_netto_abgeleitet', reconciled=True))
        self.assertEqual(evidence['metadata_source']['page'], 2)
        self.assertEqual(evidence['currency'], 'EUR')
        self.assertFalse(evidence['verified'])


class HistoricalPriceTests(unittest.TestCase):
    def setUp(self):
        self.catalog = InvoiceCatalog.__new__(InvoiceCatalog)
        self.items, self.truncated = [], False
        self.catalog.knowledge_rows = Mock(side_effect=lambda **kw: {'items': deepcopy(self.items), 'truncated': self.truncated})
        self.catalog.p = Mock()

    def row(self, id=1, date='2026-09-11', price='10.00', **changes):
        row = {'vorschlag_id': id, 'lieferant': 'TOP-Color GmbH', 'artikelnummer': 'SYNTHETIC-01',
               'produkt_name': 'Testlack', 've': 'Gebinde', 'gebinde': '0.5 L',
               'historischer_preishinweis': price, 'auslese_hinweise': [],
               'quelle': {'beleg': 'SYNTHETIC-INVOICE', 'datum': date, 'seite': 2, 'position': 3},
               'price_evidence': {'value': price, 'unrounded_value': price, 'basis': 'gebindepreis_netto_abgeleitet',
                                  'reconciled': bool(price), 'currency': 'EUR', 'tax_basis': 'net', 'tax_rate': '19'}}
        row.update(changes)
        return row

    def price(self, unit='Gebinde', packaging='0.5 L', supplier='TOP-Color GmbH', sku='SYNTHETIC-01'):
        return self.catalog.historical_price(supplier, sku, unit, packaging)

    def test_latest_invoice_date_not_import_id_and_no_writes_or_authorization(self):
        self.items = [self.row(id=99, date='2026-08-01'), self.row(id=1, price='12.00')]
        price = self.price()
        self.assertEqual((price['status'], price['amount'], price['date']), ('ok', '12.00', '2026-09-11'))
        self.assertEqual(price['source']['reference'], 'SYNTHETIC-INVOICE')
        self.assertEqual(price['source']['page'], 2)
        self.assertTrue(price['historical'])
        self.assertFalse(price['verified']); self.assertFalse(price['dispatchable'])
        self.catalog.p.assert_not_called()
        self.assertEqual(self.catalog.p.mock_calls, [])

    def test_exact_supplier_sku_unit_and_packaging(self):
        self.items = [self.row()]
        for kwargs in ({'supplier': 'Other Supplier'}, {'sku': ''}, {'unit': 'Stück'}, {'packaging': '1 L'}):
            self.assertIsNone(self.price(**kwargs)['amount'])

    def test_empty_packaging_only_when_all_matching_observations_agree(self):
        self.items = [self.row(), self.row(id=2, date='2026-08-01')]
        price = self.price(packaging='')
        self.assertEqual((price['status'], price['matched_by'], price['packaging']), ('ok', 'unique_package', '0.5 L'))
        self.items.append(self.row(id=3, gebinde='1 L'))
        self.assertIsNone(self.price(packaging='')['amount'])

    def test_latest_unclear_price_or_unknown_date_never_falls_back(self):
        self.items = [self.row(date='2026-08-01'), self.row(id=2, price='')]
        result = self.price()
        self.assertIsNone(result['amount']); self.assertEqual(len(result['matches']), 2)
        self.items[1] = self.row(id=2, date=None)
        self.assertIsNone(self.price()['amount'])

    def test_same_date_conflicts_and_truncation_remain_open(self):
        self.items = [self.row(), self.row(id=2, price='12.00')]
        self.assertEqual(self.price()['status'], 'conflict')
        self.items = [self.row()]; self.truncated = True
        self.assertIsNone(self.price()['amount'])

    def test_newest_missing_unit_or_package_cannot_hide_behind_older_exact_match(self):
        for changes in ({'ve': ''}, {'gebinde': ''}):
            self.items = [self.row(date='2026-08-01'), self.row(id=2, **changes)]
            result = self.price()
            self.assertIsNone(result['amount'])
            self.assertTrue(result['matches'])

    def test_source_metadata_never_assumed_for_old_proposals(self):
        row = self.row()
        for key in ('currency', 'tax_basis', 'tax_rate'):
            row['price_evidence'].pop(key)
        self.items = [row]
        result = self.price()
        self.assertEqual((result['currency'], result['tax_basis'], result['tax_rate']), ('unknown', 'unknown', None))


class OptionalArchiveSampleTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('TOPCOLOR_ARCHIVE_SAMPLE_DIR'), 'External archive sample not selected')
    def test_external_archive_native_metadata_coverage_without_mutation(self):
        import hashlib
        import tempfile
        from types import SimpleNamespace
        import fitz
        from werkstatt_topcolor_positionen import parse_topcolor_pages
        root = Path(os.environ['TOPCOLOR_ARCHIVE_SAMPLE_DIR']).resolve(strict=True)
        files = sorted(p for p in root.iterdir() if p.suffix.lower() == '.pdf')
        read, products = 0, 0
        for path in files:
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            with fitz.open(path) as document:
                pages = [{'page': i + 1, 'height': p.rect.height, 'words': p.get_text('words')} for i, p in enumerate(document)]
                native = parse_topcolor_pages(pages, 'TOP-Color GmbH')
                if native is None:
                    continue
                read += 1; products += len(native['positions'])
                result = reader._result('einkauf', 1)
                reader._date_from_text(document[0].get_text(), result)
                self.assertIsNotNone(result['source']['date'])
                metadata = reader.topcolor_price_metadata(pages)
                self.assertEqual((metadata['currency'], metadata['tax_basis']), ('EUR', 'net'))
                self.assertIn('tax_rate', metadata)
            with tempfile.TemporaryDirectory() as temporary:
                result = reader._result('einkauf', 1)
                result['source']['supplier'] = 'TOP-Color GmbH'
                portal = SimpleNamespace(get_fitz=lambda: fitz,
                    extract_einkauf_beleg_positions=Mock(side_effect=AssertionError('Native parser only')),
                    extract_einkauf_beleg_positions_openai=Mock(side_effect=AssertionError('No network')))
                reader._read_file(portal, path, 'external-sample', result, Path(temporary))
                self.assertEqual(len(result['candidates']), len(native['positions']))
                for candidate in result['candidates']:
                    self.assertEqual(candidate['price_evidence']['currency'], 'EUR')
                    self.assertEqual(candidate['price_evidence']['tax_basis'], 'net')
                    self.assertIn('tax_rate', candidate['price_evidence'])
                    self.assertFalse(candidate['price_evidence']['verified'])
                portal.extract_einkauf_beleg_positions.assert_not_called()
                portal.extract_einkauf_beleg_positions_openai.assert_not_called()
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)
        self.assertEqual((len(files), read, products), (29, 28, 409))


if __name__ == '__main__':
    unittest.main()
