"""Offline material search and complete synthetic invoice-to-context checks."""
from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_materialwissen import build_variants, query_notes, rank_records
from werkstatt_cockpit_api import CockpitData, _invoice_product
from werkstatt_artikel_import import InvoiceCatalog
from werkstatt_topcolor_positionen import parse_topcolor_pages
from werkstatt_rechnungsquelle import _candidate
from test_artikel_import import FakePortal, prepare_catalog
from test_topcolor_positionen import page, product


def tape(number='T-30', width='30 mm', color='grün', supplier='Top-Color GmbH', invoice=1, quantity='6'):
    return {'produkt_name': 'Klebeband ' + color + ' ' + width,
            'produkt_beschreibung': '', 'lieferant': supplier, 'artikelnummer': number,
            'groesse': width, 'farbe': color, 'gebinde': '', 've': 'Rolle',
            'quelle': {'art': 'einkauf', 'beleg_id': invoice, 'seite': 1, 'position': 1, 'datum': '2026-09-01'},
            'quantity_evidence': {'value': quantity, 'unit': 'Rolle', 'basis': 'invoice_line', 'source_field': 'Menge', 'verified': False},
            'package_evidence': {'basis': 'unknown', 'value': None}, 'historischer_preishinweis': '2.50'}


class SearchTests(unittest.TestCase):
    def test_live_wording_and_concatenated_trade_name_match_without_fabricated_pack(self):
        row=tape(number='10000991')
        row.update(produkt_name='Mipa593250500 MP TapeHydroGreen50mRolle x30mm',groesse='',farbe='',ve='Stück',gebinde='')
        questions=('Welche Abklebebänder haben wir gekauft?',
                   'Nur Auskunft, keine Bestellung: Welche Abklebebänder haben wir bisher gekauft, und welche Breiten und Verpackungseinheiten sind belegt?',
                   'grünes Abklebeband 30 mm')
        for question in questions:
            with self.subTest(question=question):
                variants=build_variants([row],question)
                self.assertEqual(len(variants),1)
                self.assertEqual(variants[0]['artikelnummer'],'10000991')
                self.assertEqual(variants[0]['produkt_name'],row['produkt_name'])
                self.assertIn('30 mm',variants[0]['groesse'])
                self.assertIsNone(variants[0]['packinhalt'])
                self.assertEqual(variants[0]['farbe'],'')
                self.assertFalse(variants[0]['bestellbar'])

    def test_synonyms_reordered_tokens_and_natural_request(self):
        rows = [tape(), tape('T-50', '50 mm'), tape('BLUE', color='blau')]
        for query in ('Anklebeband grün 30mm', '30 mm grün Abklebeband', 'Ich möchte bitte grünes Klebeband 30 mm bestellen'):
            with self.subTest(query=query):
                self.assertEqual([x['artikelnummer'] for x in rank_records(rows, query)], ['T-30'])
        self.assertEqual([x['artikelnummer'] for x in rank_records(rows, 'TopColor grün 30 mm Klebeband')], ['T-30'])

    def test_unit_constraints_do_not_confuse_millimeters_and_metres(self):
        rows = [tape('WIDTH', '50 mm'), tape('LENGTH', '50 m'), tape('CM', '50 cm')]
        self.assertEqual([x['artikelnummer'] for x in rank_records(rows, '50 mm Klebeband')], ['WIDTH'])
        self.assertEqual([x['artikelnummer'] for x in rank_records(rows, '50 cm Klebeband')], ['CM'])

    def test_colloquial_width_shows_family_and_requires_unit(self):
        rows = [tape(), tape('T-50', '50 mm')]
        for query in ('fünfziger Abklebeband', 'dreißiger Klebeband', '50er Klebeband'):
            self.assertEqual(len(rank_records(rows, query)), 2)
            self.assertTrue(query_notes(query))

    def test_hydrogreen_is_search_alias_without_claiming_color(self):
        row = tape()
        row.update(produkt_name='Test MP Tape HydroGreen 50 m x 30 mm', farbe='')
        variant = build_variants([row], 'grünes Klebeband')[0]
        self.assertEqual(variant['farbe'], '')
        self.assertIn('Farbe separat bestätigen', variant['fehlende_angaben'])

    def test_exact_supplier_sku_name_size_color_and_pack_remain_separate(self):
        base = tape()
        changes = ({'lieferant': 'Car Parts GmbH'}, {'artikelnummer': 'DIFFERENT'},
                   {'groesse': '50 mm'}, {'farbe': 'blau'}, {'gebinde': '12 Rollen/Karton'},
                   {'produkt_name': 'Anderes Produkt bei fehlerhaft gleicher SKU'})
        rows = [base] + [dict(base, **change) for change in changes]
        self.assertEqual(len(build_variants(rows)), 7)

    def test_quantity_history_requires_independent_evidence_and_repeated_sources(self):
        rows = [tape(invoice=1), tape(invoice=2), tape(invoice=3, quantity='3')]
        result = build_variants(rows)[0]
        self.assertEqual(result['uebliche_menge']['menge'], '6')
        self.assertEqual(result['uebliche_menge']['belege'], 2)
        self.assertEqual(len(result['mengenhistorie']), 3)
        self.assertIsNone(result['packinhalt'])
        self.assertIn('Packinhalt', result['fehlende_angaben'])
        self.assertFalse(result['bestellbar'])
        self.assertTrue(result['pruefen'])
        self.assertIsNone(build_variants([rows[0], deepcopy(rows[0])])[0]['uebliche_menge'])

    def test_legacy_quantity_one_does_not_become_history_or_pack_size(self):
        row = tape(quantity='1')
        row.pop('quantity_evidence')
        row.update(menge='1', gebinde='1 Stück', ve='Stück')
        result = build_variants([row])[0]
        self.assertEqual(result['mengenhistorie'], [])
        self.assertIsNone(result['uebliche_menge'])
        self.assertIsNone(result['packinhalt'])

    def test_single_observation_is_not_usual_and_unknown_date_is_not_latest(self):
        row = tape()
        row['quelle'].pop('datum')
        result = build_variants([row])[0]
        self.assertIsNone(result['uebliche_menge'])
        self.assertIsNone(result['letzte_belegte_menge'])
        self.assertEqual(len(result['mengenhistorie']), 1)

    def test_quantity_with_unknown_unit_cannot_borrow_price_unit(self):
        rows=[tape(invoice=i) for i in (1,2)]
        for row in rows:
            row['quantity_evidence']['unit']=None
            row['ve']='Stück'
        result=build_variants(rows)[0]
        self.assertIsNone(result['uebliche_menge'])
        self.assertEqual(result['letzte_belegte_menge']['einheit'],'')
        self.assertIn('Einheit der Rechnungsmenge',result['fehlende_angaben'])

    def test_preload_prefers_repeated_evidence_and_history_stays_bounded(self):
        rows=[tape(number='NEWEST',invoice=99)]+[tape(number='REGULAR',invoice=i+1) for i in range(60)]
        result=build_variants(rows,limit=1)[0]
        self.assertEqual(result['artikelnummer'],'REGULAR')
        self.assertEqual(result['belege_anzahl'],60)
        self.assertEqual(result['uebliche_menge']['belege'],60)
        self.assertEqual(len(result['mengenhistorie']),50)
        self.assertTrue(result['historie_gekuerzt'])


class PipelineTests(unittest.TestCase):
    @contextmanager
    def db(self):
        db=self.p.get_db()
        try:
            yield db
            db.commit()
        finally:db.close()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.p = FakePortal(str(Path(self.tmp.name) / 'knowledge.db'))
        self.catalog = InvoiceCatalog(self.p)
        self.service = CockpitData(self.p); self.service.catalog = self.catalog
        with self.db() as db:
            db.execute('''CREATE TABLE einkauf_artikel(id INTEGER PRIMARY KEY,lieferant TEXT,artikelnummer TEXT,
                produkt_name TEXT,produkt_beschreibung TEXT DEFAULT '',ve TEXT DEFAULT '',gebinde TEXT DEFAULT '',
                letzter_preis TEXT DEFAULT '',letzter_preis_datum TEXT DEFAULT '',preisquelle TEXT DEFAULT '',quelle_beleg_id INTEGER)''')

    def prepare(self, sources):
        prepare_catalog(self.catalog, {'einkaufsbelege': [{'id': sid, 'lieferant': supplier, 'original_name': 'synthetic.pdf'}
                                               for sid,supplier in sources], 'lieferantenrechnungen': []})

    def test_topcolor_columns_through_reader_catalog_api_grouping(self):
        self.prepare([(1, 'Top-Color GmbH'), (2, 'Top-Color GmbH')])
        parsed = parse_topcolor_pages([page(lines=[product(name='Test-Schleifblatt 80x200mm P80 25/Pack',
                    content='1,000', measure='Pack', quantity='2,00', total='64,00')])], 'Top-Color GmbH')['positions'][0]
        def read(_portal, _kind, sid):
            source={'supplier':'Top-Color GmbH','date':'2026-09-0'+str(sid)}
            item=_candidate(parsed,source,str(sid),1,str(sid)*64,native=True)
            return {'status':'ok','candidates':[item],'coverage':{'complete':True},'warnings':[]}
        with patch('werkstatt_artikel_import.read_source', side_effect=read):
            self.catalog.process_next(); self.catalog.process_next()
        response=self.service.material_context('Schleifblatt', limit=8)
        self.assertEqual(len(response['varianten']),1)
        item=response['varianten'][0]
        self.assertEqual(item['uebliche_menge']['menge'],'2')
        self.assertEqual(item['uebliche_menge']['einheit'],'pack')
        self.assertEqual(item['packinhalt']['menge'],'25')
        self.assertEqual(item['packinhalt']['einheit'],'Stück')
        self.assertEqual(item['packinhalt']['pro'],'Pack')
        self.assertEqual(item['letzte_belegte_menge']['quelle']['datum'],'2026-09-02')
        self.assertFalse(item['bestellbar'])
        self.assertFalse(response['abdeckung']['vollstaendigkeit_bestaetigt'])

    def test_scope_revoke_and_inactive_rows_are_immediate_no_cache(self):
        self.prepare([(1,'Test Supplier'),(2,'Other Supplier'),(3,'Synthetic Bank')])
        def read(_portal, _kind, sid):
            row=tape(supplier='Test Supplier' if str(sid)=='1' else 'Other Supplier')
            row['source']={'page':1,'position':1}
            return {'status':'ok','candidates':[row],'coverage':{'complete':True},'warnings':[]}
        with patch('werkstatt_artikel_import.read_source', side_effect=read):
            self.catalog.process_next(); self.catalog.process_next()
        first=self.service.material_context()
        self.assertEqual(len(first['lieferanten']),2)
        self.assertNotIn('Synthetic Bank',json.dumps(first))
        self.p.settings['ASSISTANT_MATERIAL_SUPPLIERS']='[]'
        self.assertEqual(self.service.material_context()['varianten'],[])
        self.p.settings['ASSISTANT_MATERIAL_SUPPLIERS']='["Test Supplier"]'
        self.assertEqual(len(self.service.material_context()['varianten']),1)
        with self.db() as db:db.execute('UPDATE assistent_rechnungsartikel SET active=0')
        self.assertEqual(self.service.material_context()['varianten'],[])

    def test_saved_article_after_many_blocked_rows_still_searchable(self):
        self.prepare([(1,'Top-Color GmbH')])
        with self.db() as db:
            db.execute("INSERT INTO einkauf_artikel(id,lieferant,artikelnummer,produkt_name,quelle_beleg_id) VALUES(1,'Top-Color GmbH','T-30','Klebeband grün 30 mm',1)")
            for number in range(2, 72):
                db.execute("INSERT INTO einkauf_artikel(id,lieferant,artikelnummer,produkt_name,quelle_beleg_id) VALUES(?,'Synthetic Bank','B','Klebeband grün 30 mm',1)",(number,))
        result=self.service.articles('30mm Anklebeband grün')
        self.assertEqual([x['artikelnummer'] for x in result['artikel']],['T-30'])
        self.assertNotIn('Synthetic Bank',json.dumps(result))

    def test_product_allowlist_drops_bank_data_and_unknown_legacy_evidence(self):
        row=tape();row['bank_account']='private-secret';row['extrahierter_text']='private-full-text'
        row['quantity_evidence']['bank_data']='private-secret'
        row['quantity_evidence']['unit']='IBAN private-secret'
        item=_invoice_product(row,'Top-Color GmbH',1,True)
        self.assertNotIn('private-secret',json.dumps(item))
        self.assertNotIn('private-full-text',json.dumps(item))
        self.assertIsNone(item['quantity_evidence']['unit'])
        self.assertFalse(item['quantity_evidence']['verified'])

    def test_context_and_article_variant_limits_are_explicit(self):
        self.prepare([(1,'Top-Color GmbH')])
        with self.db() as db:
            for number in range(1,34):
                db.execute("INSERT INTO einkauf_artikel(id,lieferant,artikelnummer,produkt_name,quelle_beleg_id) VALUES(?,'Top-Color GmbH',?,'Klebeband grün 30 mm',1)",(number,'T-'+str(number)))
        compact=self.service.material_context('Abklebeband',limit=8)
        self.assertEqual(len(compact['varianten']),8)
        self.assertTrue(compact['varianten_gekuerzt'])
        self.assertFalse(compact['abdeckung']['begrenzt'],'record coverage and selected variant subset are separate')
        found=self.service.articles('Abklebeband')
        self.assertEqual(len(found['varianten']),30)
        self.assertTrue(found['varianten_gekuerzt'])

    def test_quarantined_original_hides_legacy_article_immediately_but_manual_stays(self):
        self.prepare([(1,'Top-Color GmbH')])
        with self.db() as db:
            for identifier,sku,source_id in ((1,'LINKED',1),(2,'MANUAL',None),(3,'MISSING-ORIGINAL',999),(4,'MANUAL-DEFAULT',0)):
                db.execute("INSERT INTO einkauf_artikel(id,lieferant,artikelnummer,produkt_name,quelle_beleg_id) VALUES(?,'Top-Color GmbH',?,'Klebeband grün 30 mm',?)",(identifier,sku,source_id))
        before=self.service.articles('Abklebeband')
        self.assertCountEqual([x['artikelnummer'] for x in before['artikel']],['LINKED','MANUAL','MANUAL-DEFAULT'])
        with self.db() as db:db.execute("UPDATE einkauf_belege SET beleg_typ='gesperrt' WHERE id=1")
        after=self.service.articles('Abklebeband')
        self.assertCountEqual([x['artikelnummer'] for x in after['artikel']],['MANUAL','MANUAL-DEFAULT'])
        compact=self.service.material_context('Abklebeband')
        self.assertCountEqual([x['artikelnummer'] for x in compact['varianten']],['MANUAL','MANUAL-DEFAULT'])
        self.assertNotIn('LINKED',json.dumps(compact))
        self.assertNotIn('MISSING-ORIGINAL',json.dumps(compact))


if __name__=='__main__':unittest.main()
