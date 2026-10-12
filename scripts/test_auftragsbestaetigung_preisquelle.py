"""Synthetic AB proposals: no production records, network, payments or orders."""
from copy import deepcopy
from contextlib import closing
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
import fitz
import test_artikel_import as import_tests
import test_topcolor_positionen as column_tests
import test_topcolor_preisstand as price_tests
import werkstatt_rechnungsquelle as reader
from werkstatt_artikel_import import InvoiceCatalog, _price_evidence
from werkstatt_cockpit_api import CockpitData, _invoice_product
from werkstatt_materialwissen import build_variants
from werkstatt_rechnungsfreigabe import classify_invoice_source
from werkstatt_topcolor_positionen import parse_topcolor_pages


AB = 'auftragsbestaetigung'


def synthetic_confirmation():
    first = column_tests.page(lines=[
        column_tests.product(position=1, article='00000071', name='Logistikpauschale', quantity='1,00', content='1,000', measure='Stück', base='2,00', discount='', total='2,00'),
        column_tests.product(position=2, name='Testlack', y=150, quantity='1,00', content='0,500', base='40,00', discount='-20,00%', total='16,00'),
        [column_tests.word(53, 165, 'WK30G'), column_tests.word(103, 165, '0,5 Liter')],
    ])
    return parse_topcolor_pages([first], 'TOP-Color GmbH', document_type=AB)


class ConfirmationPriceTests(unittest.TestCase):
    def test_confirmation_scope_is_explicit_and_does_not_expand_financial_sources(self):
        source = {'supplier': 'TOP-Color GmbH', 'beleg_typ': AB}
        self.assertFalse(classify_invoice_source(source)['allowed'])
        self.assertTrue(classify_invoice_source(source, allow_order_confirmation=True)['allowed'])
        for denied in ('Volksbank', 'AOK', 'Finanzamt', 'Insurance Unknown', 'Unknown Parts'):
            self.assertFalse(classify_invoice_source(dict(source, supplier=denied), allow_order_confirmation=True)['allowed'])
        for kind in ('angebot', 'lieferschein', 'gesperrt', 'purchasecreditnote'):
            self.assertFalse(classify_invoice_source(dict(source, beleg_typ=kind), allow_order_confirmation=True)['allowed'])
        self.assertFalse(classify_invoice_source(dict(source, voucher_type='salesinvoice'), allow_order_confirmation=True)['allowed'])

    def test_ab_stock_code_continuation_keeps_package_and_excludes_fee(self):
        parsed = synthetic_confirmation()
        self.assertTrue(parsed['complete'])
        self.assertEqual(len(parsed['positions']), 1)
        self.assertEqual(len(parsed['fees']), 1)
        item = parsed['positions'][0]
        self.assertEqual(item['produkt_name'], 'Testlack 0,5 Liter')
        self.assertEqual(item['gebinde'], '0.5 L')
        self.assertEqual(item['price_evidence']['quoted_unit_net'], '16')
        self.assertEqual(item['quantity_evidence']['basis'], 'document_line')
        self.assertNotIn('WK30G', item['produkt_name'])

    def test_quoted_amount_survives_formula_discrepancy_without_reconciliation(self):
        rows = column_tests.page(lines=[column_tests.product(name='Synthetic repair resin 1 kg', quantity='1,00', content='1,000', base='24,25', discount='-15,00%', total='20,62')])
        item = parse_topcolor_pages([rows], 'Top-Color', document_type=AB)['positions'][0]
        evidence = _price_evidence(item['price_evidence'])
        self.assertEqual(item['preis'], '20.62')
        self.assertEqual(evidence['quoted_line_total_net'], '20.62')
        self.assertEqual(evidence['quoted_unit_net'], '20.62')
        self.assertEqual(evidence['formula_unit_net'], '20.6125')
        self.assertEqual(evidence['basis'], 'auftragsbestaetigung_netto_positionswert')
        self.assertFalse(evidence['reconciled'])
        self.assertFalse(evidence['verified'])
        self.assertTrue(item['auslese_hinweise'])

    def test_quoted_multi_package_unit_is_not_list_price_or_whole_position(self):
        rows = column_tests.page(lines=[column_tests.product(name='Synthetic coating 750 ml', quantity='3,00', content='0,750', base='28,35', discount='-15,00%', total='54,22')])
        item = parse_topcolor_pages([rows], 'Top-Color', document_type=AB)['positions'][0]
        evidence = _price_evidence(item['price_evidence'])
        self.assertEqual(item['preis'], '18.07')
        self.assertEqual(evidence['quoted_line_total_net'], '54.22')
        self.assertTrue(evidence['quoted_unit_net'].startswith('18.073333'))
        self.assertEqual(evidence['base_price_per_measure'], '28.35')
        self.assertEqual(evidence['content'], '0.75')
        self.assertEqual(evidence['discount_percent'], '-15')
        self.assertEqual(evidence['measure_unit'], 'Ltr/KG')

    def test_ab_footer_requires_its_label_currency_alignment_and_reconciled_tax(self):
        footer = deepcopy(price_tests.footer())
        word = list(footer['words'][7]); word[4] = 'Betrag'; footer['words'][7] = tuple(word)
        self.assertEqual(reader.topcolor_price_metadata([footer]), {})
        metadata = reader.topcolor_price_metadata([footer], document_type=AB)
        self.assertEqual((metadata['currency'], metadata['tax_basis'], metadata['tax_rate']), ('EUR', 'net', '19'))
        malformed = deepcopy(footer)
        word = list(malformed['words'][6]); word[4] = '18,00'; malformed['words'][6] = tuple(word)
        self.assertEqual(reader.topcolor_price_metadata([malformed], document_type=AB), {})

    def test_confirmation_date_comes_from_labelled_cell_not_delivery_or_import(self):
        result = reader._result('einkauf', 1)
        page = {'page': 1, 'words': [column_tests.word(377, 304, 'Beleg-Datum'), column_tests.word(377, 316, '12.10.2026'),
            column_tests.word(298, 196, 'Liefer-Datum:'), column_tests.word(363, 196, '13.10.2026')]}
        reader._confirmation_date_from_words([page], result)
        self.assertEqual(result['source']['date'], '2026-10-12')
        conflict = deepcopy(page); row = list(conflict['words'][1]); row[4] = '14.10.2026'; conflict['words'][1] = tuple(row)
        reader._confirmation_date_from_words([conflict], result)
        self.assertIsNone(result['source']['date'])

    def test_type_declaration_needs_readable_confirmation_original(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / 'synthetic.pdf'
            with fitz.open() as document:
                page = document.new_page(); page.insert_text((40, 40), 'RECHNUNG Beleg-Datum 12.10.2026')
                document.save(path)
            result = reader._result('einkauf', 1)
            result['source'].update(supplier='TOP-Color GmbH', document_type=AB)
            with patch('werkstatt_topcolor_positionen.parse_topcolor_pages') as parse:
                with self.assertRaises(reader.SourceUnavailable):
                    reader._read_file(SimpleNamespace(get_fitz=lambda: fitz), path, 'synthetic', result, root)
                parse.assert_not_called()
            self.assertEqual(result['candidates'], [])
            self.assertTrue(reader._confirmation_header('AUFTRAGSBESTÄTIGUNG'))
            self.assertFalse(reader._confirmation_header('RECHNUNG\nBezug auf unsere Auftragsbestätigung vom 12.10.2026'))
            self.assertFalse(reader._confirmation_header('Rechnung, vormals Auftragsbestätigung'))
            self.assertFalse(reader._confirmation_header('RECHNUNG\nArtikelzeilen\nAUFTRAGSBESTÄTIGUNG'))
            self.assertFalse(reader._confirmation_header('AUFTRAGSBESTÄTIGUNG\nArtikelzeilen\nRECHNUNG'))

    def test_group_price_type_and_caption_belong_to_same_observation_in_both_orders(self):
        common = {'produkt_name': 'Synthetic coating', 'lieferant': 'TOP-Color GmbH', 'artikelnummer': '87650001',
            'produkt_beschreibung': 'Synthetic coating 0.5 Liter', 've': 'Gebinde', 'gebinde': '0.5 L'}
        invoice = dict(common, historischer_preishinweis='10.00', quelle={'art': 'einkauf', 'beleg_id': 1, 'datum': '2026-10-01'})
        confirmation = dict(common, historischer_preishinweis='20.00', document_type=AB,
            preisquellenhinweis='Preis laut Auftragsbestätigung vom 12.10.2026',
            quelle={'art': 'einkauf', 'beleg_id': 2, 'datum': '2026-10-12', 'beleg_typ': AB})
        first = build_variants([invoice, confirmation], 'Synthetic coating')[0]
        self.assertEqual(first['historischer_preishinweis'], '10.00')
        self.assertNotIn('document_type', first); self.assertNotIn('preisquellenhinweis', first)
        reverse = build_variants([confirmation, invoice], 'Synthetic coating')[0]
        self.assertEqual(reverse['historischer_preishinweis'], '20.00')
        self.assertEqual(reverse['document_type'], AB)
        self.assertEqual(reverse['preisquellenhinweis'], 'Preis laut Auftragsbestätigung vom 12.10.2026')

    def test_approved_quote_is_dated_and_never_relabelled_as_invoice(self):
        source = {'source_key': 'einkauf:99', 'source_kind': 'einkauf', 'source_id': '99', 'supplier': 'TOP-Color GmbH', 'reference': 'Synthetic AB.pdf', 'id': 5}
        item = synthetic_confirmation()['positions'][0]
        item['source'] = dict(item.pop('native_source'), date='2026-10-12', document_type=AB, sha256='a' * 64, file_id='einkauf-99')
        record, known = InvoiceCatalog._candidate_record(source, item, 1, datetime.now(timezone.utc))
        payload = json.loads(record[6])
        self.assertTrue(known)
        self.assertEqual(payload['quelle']['beleg_typ'], AB)
        self.assertEqual(payload['quelle']['datum'], '2026-10-12')
        self.assertEqual(payload['preisquellenhinweis'], 'Preis laut Auftragsbestätigung vom 12.10.2026')
        self.assertFalse(payload['preis_geprueft']); self.assertFalse(payload['bestellbar'])
        exported = _invoice_product(payload, 'TOP-Color GmbH', 99, proposal=True)
        self.assertEqual(exported['document_type'], AB)
        self.assertEqual(exported['price_evidence']['quoted_line_total_net'], '16')
        variant = build_variants([exported], 'Testlack')[0]
        self.assertEqual(variant['quellen'][0]['beleg_typ'], AB)
        self.assertEqual(variant['mengenhistorie'], [])
        self.assertIsNone(variant['uebliche_menge'])
        self.assertFalse(variant['bestellbar'])

    def test_historical_hint_exposes_ab_caption_and_preserves_exact_identity(self):
        row = {'vorschlag_id': 1, 'lieferant': 'TOP-Color GmbH', 'artikelnummer': '87650001', 'produkt_name': 'Testlack', 've': 'Gebinde', 'gebinde': '0.5 L',
            'quelle': {'beleg': 'Synthetic AB.pdf', 'beleg_typ': AB, 'datum': '2026-10-12', 'position': 2, 'seite': 1},
            'price_evidence': dict(synthetic_confirmation()['positions'][0]['price_evidence'], currency='EUR', tax_basis='net', tax_rate='19')}
        catalog = InvoiceCatalog.__new__(InvoiceCatalog)
        snapshot = {'items': [row], 'truncated': False}
        result = catalog.historical_price('TOP-Color GmbH', '87650001', 'Gebinde', '0.5 L', snapshot=snapshot)
        self.assertEqual((result['status'], result['amount']), ('ok', '16.00'))
        self.assertEqual(result['source_label'], 'Preis laut Auftragsbestätigung vom 12.10.2026')
        self.assertFalse(result['dispatchable']); self.assertFalse(result['verified'])
        for sku, pack in [('87650002', '0.5 L'), ('87650001', '1 L')]:
            self.assertIsNone(catalog.historical_price('TOP-Color GmbH', sku, 'Gebinde', pack, snapshot=snapshot)['amount'])
        row['price_evidence']['reconciled'] = False
        self.assertIsNone(catalog.historical_price('TOP-Color GmbH', '87650001', 'Gebinde', '0.5 L', snapshot=snapshot)['amount'])

    def test_targeted_catalog_import_does_not_process_other_sources_and_can_be_repeated(self):
        with tempfile.TemporaryDirectory() as temporary:
            portal = import_tests.FakePortal(str(Path(temporary) / 'synthetic.db'))
            with closing(portal.get_db()) as db:
                db.execute('INSERT INTO einkauf_belege(id,beleg_typ) VALUES(99,?)', (AB,))
                db.execute("INSERT INTO einkauf_belege(id,beleg_typ) VALUES(1,'rechnung')")
                db.commit()
            catalog = InvoiceCatalog(portal)
            catalog.prepare({'einkaufsbelege': [{'id': 99, 'lieferant': 'TOP-Color GmbH', 'original_name': 'Synthetic AB.pdf'}, {'id': 1, 'lieferant': 'TOP-Color GmbH', 'original_name': 'Other.pdf'}], 'lieferantenrechnungen': []})
            selected = next(row for row in catalog.status()['quellen'] if row['reference'] == 'Synthetic AB.pdf')['id']
            item = synthetic_confirmation()['positions'][0]
            item['source'] = dict(item.pop('native_source'), date='2026-10-12', document_type=AB)
            with patch('werkstatt_artikel_import.read_source', return_value=import_tests.extracted(item)) as read:
                catalog.process_next(source_id=selected)
                read.assert_called_once_with(portal, 'einkauf', '99')
                self.assertEqual(len(catalog.search('Testlack')), 1)
                self.assertTrue(catalog.retry_source(selected))
                catalog.process_next(source_id=selected)
            self.assertEqual(catalog.rows('SELECT count(*) AS n FROM assistent_rechnungsartikel')[0]['n'], 1)
            untouched = next(row for row in catalog.status()['quellen'] if row['reference'] == 'Other.pdf')
            self.assertEqual(untouched['state'], 'offen')
            with closing(portal.get_db()) as db:
                db.execute("UPDATE einkauf_belege SET beleg_typ='gesperrt' WHERE id=99")
                db.commit()
            self.assertEqual(catalog.search('Testlack'), [])

    def test_inventory_discovers_confirmation_without_enabling_invoice_only_callers(self):
        with tempfile.TemporaryDirectory() as temporary:
            portal = import_tests.FakePortal(str(Path(temporary) / 'synthetic.db'))
            with closing(portal.get_db()) as db:
                for column in ('lieferant', 'original_name', 'status'):
                    db.execute('ALTER TABLE einkauf_belege ADD COLUMN ' + column + ' TEXT')
                db.execute('CREATE TABLE lexware_rechnungen(voucher_id TEXT,contact_name TEXT,voucher_number TEXT,voucher_date TEXT,voucher_type TEXT,status TEXT)')
                db.execute('INSERT INTO einkauf_belege VALUES(99,?,?,?,?)', (AB, 'TOP-Color GmbH', 'Synthetic AB.pdf', 'pruefen'))
                db.execute('INSERT INTO einkauf_belege VALUES(100,?,?,?,?)', (AB, 'AOK', 'Contribution.pdf', 'pruefen'))
                db.execute('INSERT INTO einkauf_belege VALUES(101,?,?,?,?)', ('lieferschein', 'TOP-Color GmbH', 'Delivery.pdf', 'pruefen'))
                db.commit()
            service = CockpitData(portal)
            self.assertEqual([r['id'] for r in service.invoice_sources()['einkaufsbelege']], [99])
            self.assertFalse(service.material_allowed({'supplier': 'TOP-Color GmbH', 'beleg_typ': AB}))
            catalog = InvoiceCatalog(portal)
            catalog.prepare(service.invoice_sources(include_held=True))
            source = next(r for r in catalog._source_rows() if r['source_id'] == '99')
            self.assertTrue(catalog.source_rule(source)['allowed'])
            with closing(portal.get_db()) as db:
                db.execute("UPDATE einkauf_belege SET beleg_typ='gesperrt' WHERE id=99"); db.commit()
            self.assertFalse(catalog.source_rule(source | {'_current_beleg_typ': 'gesperrt'})['allowed'])

    def test_upload_preserves_selected_type_and_rejects_unallowed_type_or_rights(self):
        # Use the same isolated Flask/CSRF fixture as the existing invoice tests.
        fixture = import_tests.CatalogUploadAndApprovalTests()
        fixture.setUp(); self.addCleanup(fixture.doCleanups)
        def upload(kind=AB, csrf='synthetic-csrf'):
            return fixture.client.post('/admin/assistent-artikel/upload', data={'lieferant': 'TOP-Color GmbH', 'beleg_typ': kind, 'csrf_token': csrf,
                'rechnungen': [(io.BytesIO(b'%PDF-synthetic'), 'Synthetic AB.pdf')]}, content_type='multipart/form-data')
        self.assertEqual(upload().status_code, 302)
        self.assertEqual(fixture.portal.save_einkauf_beleg_upload.call_args.kwargs['beleg_typ'], AB)
        fixture.portal.save_einkauf_beleg_upload.reset_mock()
        self.assertEqual(upload('angebot').status_code, 302)
        fixture.portal.save_einkauf_beleg_upload.assert_not_called()
        self.assertEqual(upload(csrf='bad').status_code, 400)
        with fixture.client.session_transaction() as session:
            session['admin'] = False
        self.assertEqual(upload().status_code, 302)
        fixture.portal.save_einkauf_beleg_upload.assert_not_called()


if __name__ == '__main__':
    unittest.main()
