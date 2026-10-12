"""Real catalog/cockpit/personal preview chain, synthetic originals only."""
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
import test_materialbestellung_portal as portal_fixture
from test_materialfoto_codes import png, qr_image
from werkstatt_artikel_import import InvoiceCatalog
from werkstatt_cockpit_api import CockpitData


ENDPOINT = '/werkstatt/materialbestellung/artikelscan-vorschau'


class ScanPriceIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.f = portal_fixture.PortalTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.p = self.f.p
        self.p.get_app_setting = lambda key,default='': json.dumps(['TOP-Color GmbH'])
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.p.UPLOAD_DIR = Path(self.temp.name) / 'synthetic-preview-originals'
        self.p.UPLOAD_DIR.mkdir()
        original = self.p.UPLOAD_DIR / 'synthetic-ab.pdf'
        original.write_bytes(b'%PDF synthetic quotation, no business records')
        self.sha = hashlib.sha256(original.read_bytes()).hexdigest()
        db = self.p.get_db()
        try:
            db.executescript('''CREATE TABLE einkauf_belege(id INTEGER PRIMARY KEY,beleg_typ TEXT,lieferant TEXT,
                original_name TEXT,stored_name TEXT,status TEXT);
                CREATE TABLE einkauf_artikel(id INTEGER PRIMARY KEY,lieferant TEXT,artikelnummer TEXT,
                produkt_name TEXT,produkt_beschreibung TEXT,ve TEXT,gebinde TEXT,letzter_preis TEXT,
                letzter_preis_datum TEXT,preisquelle TEXT,quelle_beleg_id INTEGER);''')
            db.execute("INSERT INTO einkauf_belege VALUES(99,'auftragsbestaetigung','TOP-Color GmbH','Synthetic AB.pdf','synthetic-ab.pdf','importiert')")
            db.commit()
        finally:
            db.close()
        self.catalog = InvoiceCatalog(self.p)
        db = self.p.get_db()
        try:
            db.execute("INSERT INTO assistent_rechnungsimporte(id,source_key,source_kind,source_id,supplier,reference,state) VALUES(5,'einkauf:99','einkauf','99','TOP-Color GmbH','Synthetic AB.pdf','ausgelesen')")
            db.commit()
        finally:
            db.close()
        cockpit = CockpitData(self.p)
        cockpit.catalog = self.catalog
        self.p.cockpit_data = cockpit
        self.vision = self.p.assistant_material_photos.vision = Mock(return_value={
            'art':'produkt','produkt':'PPG Synthetic D8218/E2.5','artikelnummer':'D8218/E2.5'})
        self.add_product()

    def add_product(self,ident=1,*,code='D8218/E2.5',package='2.5 L',amount='12.50',formula=True,changes=None,name=None,date='2026-10-12'):
        name = name or 'PPG Synthetic '+code+' '+package
        candidate = dict(produkt_name=name,produkt_beschreibung=name,
            artikelnummer='87650001',ve='Gebinde',gebinde=package,preis=amount,
            source=dict(page=1,position=ident+1,date=date,document_type='auftragsbestaetigung',sha256=self.sha),
            price_evidence=dict(document_type='auftragsbestaetigung',basis='auftragsbestaetigung_netto_positionswert',
                value=amount,unrounded_value=amount,quantity='1',content='2.5',measure_unit='Ltr/KG',
                base_price_per_measure='5.00',discount_percent='0',line_total_net=amount,
                quoted_line_total_net=amount,quoted_unit_net=amount,formula_unit_net='12.49',
                reconciled=formula,quote_formula_reconciled=formula,currency='EUR',tax_basis='net',tax_rate='19',
                metadata_source=dict(page=2,method='topcolor_summary_columns_v1',tax_summary_reconciled=True)),
            auslese_hinweise=[] if formula else ['Positionssumme passt nicht zu Menge, Inhalt, Grundpreis und Rabatt.'])
        if changes:
            candidate['price_evidence'].update(changes)
        source = dict(source_key='einkauf:99',source_kind='einkauf',source_id='99',supplier='TOP-Color GmbH',reference='Synthetic AB.pdf',id=5)
        record,_ = InvoiceCatalog._candidate_record(source,candidate,ident,datetime.now(timezone.utc))
        self.sql('INSERT INTO assistent_rechnungsartikel(id,fingerprint,identity_key,import_id,supplier,product_name,article_number,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)',(ident,*record))

    def sql(self,query,args=()):
        db = self.p.get_db()
        try:
            result = [dict(row) for row in db.execute(query,args).fetchall()]
            db.commit()
            return result
        finally:
            db.close()

    def database(self):
        db = self.p.get_db()
        try:
            return '\n'.join(db.iterdump())
        finally:
            db.close()

    def preview(self,code='87650001',mode='code'):
        raw = portal_fixture.picture() if code is None else png(qr_image(code))
        return self.f.client.post(ENDPOINT,headers={'X-CSRF-Token':'synthetic-csrf'},
            data=dict(modus=mode,foto=(io.BytesIO(raw),'synthetic-photo.png')))

    def test_supplier_code_reads_real_dated_catalog_price_without_writes(self):
        before = self.database()
        response = self.preview()
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.json['lookup_status'],'matched')
        price = response.json['matches'][0]['price']
        self.assertEqual((price['amount'],price['date'],price['packaging']),('12.50','2026-10-12','2.5 L'))
        self.assertFalse(response.json['bestellbar'])
        self.assertEqual(response.json['matches'][0]['quelle']['beleg_typ'],'auftragsbestaetigung')
        self.vision.assert_not_called()
        self.assertEqual(self.database(),before)

    def test_complete_manufacturer_code_keeps_canonical_supplier_sku_and_package(self):
        result = self.preview('D8218/E2.5').json
        self.assertEqual(result['lookup_status'],'matched')
        self.assertEqual(result['matches'][0]['artikelnummer'],'87650001')
        self.assertEqual(result['matches'][0]['price']['packaging'],'2.5 L')
        self.assertFalse(result['bestellbar'])
        self.vision.assert_not_called()

    def test_manufacturer_prefix_other_package_and_embedded_code_are_not_exact(self):
        for code in ('D8218','D8218/E2','D8218/E0.5','D8218/E2.5-extra','8218'):
            with self.subTest(code=code):
                result = self.preview(code).json
                self.assertEqual(result['lookup_status'],'no_match')
                self.assertEqual(result['matches'],[])

    def test_exact_mipa_nine_digit_maker_number_is_not_a_numeric_or_gtin_guess(self):
        self.sql('DELETE FROM assistent_rechnungsartikel')
        self.add_product(name='Mipa 228530003 Synthetic filler white 3 L',package='3 L')
        result = self.preview('228530003','auto').json
        self.assertEqual(result['lookup_status'],'matched')
        self.assertEqual(result['matches'][0]['artikelnummer'],'87650001')
        self.assertEqual(result['matches'][0]['price']['packaging'],'3 L')
        self.vision.assert_not_called()
        for code in ('228530004','22853000','2285300030','228530003-extra','2285300030001'):
            with self.subTest(code=code):
                self.assertEqual(self.preview(code).json['matches'],[])
        self.sql('DELETE FROM assistent_rechnungsartikel')
        self.add_product(name='OtherMaker 228530003 Synthetic filler 3 L',package='3 L')
        self.assertEqual(self.preview('228530003').json['matches'],[])

    def test_same_manufacturer_code_in_different_packages_remains_ambiguous(self):
        self.sql('DELETE FROM assistent_rechnungsartikel')
        self.add_product(name='Mipa 228530003 Synthetic filler 3 L',package='3 L')
        self.add_product(2,name='Mipa 228530003 Synthetic filler 1 L',package='1 L')
        result = self.preview('228530003','auto').json
        self.assertEqual(result['lookup_status'],'ambiguous')
        self.assertEqual(len(result['matches']),2)
        self.assertEqual({hit['gebinde'] for hit in result['matches']},{'3 L','1 L'})
        self.vision.assert_not_called()

    def test_auto_uses_same_first_photo_for_one_label_read_and_no_database_changes(self):
        before = self.database()
        with patch('werkstatt_materialfoto._image',wraps=__import__('werkstatt_materialfoto')._image) as normalize:
            response = self.preview(None,'auto')
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(response.json['lookup_status'],'label')
        self.assertEqual(response.json['matches'][0]['price']['amount'],'12.50')
        self.assertEqual(self.vision.call_count,1)
        self.assertEqual(normalize.call_count,1)
        self.assertEqual(self.database(),before)
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_auto_known_code_does_not_call_vision_and_unknown_code_can_read_label(self):
        first = self.preview('D8218/E2.5','auto')
        self.assertEqual(first.status_code,200,first.text)
        self.assertEqual(first.json['lookup_status'],'matched')
        self.vision.assert_not_called()
        second = self.preview('UNKNOWN-77','auto')
        self.assertEqual(second.status_code,200,second.text)
        self.assertEqual(second.json['lookup_status'],'label')
        self.assertEqual(second.json['matches'][0]['price']['amount'],'12.50')
        self.assertEqual(self.vision.call_count,1)

    def test_printed_formula_warning_is_displayed_without_weakening_historical_price(self):
        self.sql('DELETE FROM assistent_rechnungsartikel')
        self.add_product(amount='20.62',formula=False)
        before = self.database()
        result = self.preview().json
        self.assertIn('price',result['matches'][0])
        price = result['matches'][0]['price']
        self.assertEqual(price['amount'],'20.62')
        self.assertTrue(price['review_required'])
        self.assertIn('Preisformel',price['review_notice'])
        strict = self.catalog.historical_price('TOP-Color GmbH','87650001','Gebinde','2.5 L')
        self.assertIsNone(strict['amount'])
        self.assertFalse(strict['dispatchable'])
        self.assertEqual(self.database(),before)

    def test_missing_tax_proof_changed_original_and_latest_price_conflict_have_no_price(self):
        self.sql('DELETE FROM assistent_rechnungsartikel')
        self.add_product(formula=False,changes={'metadata_source':{},'currency':'unknown'})
        self.assertNotIn('price',self.preview().json['matches'][0])
        self.sql('DELETE FROM assistent_rechnungsartikel')
        self.add_product()
        (self.p.UPLOAD_DIR/'synthetic-ab.pdf').write_bytes(b'%PDF changed synthetic original')
        self.assertNotIn('price',self.preview().json['matches'][0])
        self.sha = hashlib.sha256((self.p.UPLOAD_DIR/'synthetic-ab.pdf').read_bytes()).hexdigest()
        self.sql('DELETE FROM assistent_rechnungsartikel')
        self.add_product(amount='12.50')
        self.add_product(2,amount='13.50')
        self.assertNotIn('price',self.preview().json['matches'][0])

    def test_quarantine_or_pending_import_is_rechecked_and_never_selects_old_quote(self):
        self.assertIn('price',self.preview().json['matches'][0])
        self.sql("UPDATE assistent_rechnungsimporte SET state='offen'")
        self.assertNotIn('price',self.preview().json['matches'][0])
        self.sql("UPDATE einkauf_belege SET beleg_typ='quarantine'")
        self.assertEqual(self.preview().json['matches'],[])

    def test_field_boundaries_do_not_manufacture_bank_numbers_and_real_footer_stays_hidden(self):
        for ident,name,pack in ((2,'Synthetic primer 400 ml','400 ml'),
                                (3,'Synthetic cleaner 1 kg','1 kg'),
                                (4,'Synthetic LiftTape 10 m x 50 mm','1 Stück')):
            self.add_product(ident,name=name,package=pack)
        _,visible,coverage = self.p.cockpit_data._material_records()
        self.assertEqual(coverage['sichtbare_positionen'],4)
        self.assertEqual(len(visible),4)
        row = self.sql('SELECT payload_json FROM assistent_rechnungsartikel WHERE id=4')[0]
        payload = json.loads(row['payload_json'])
        payload['farbe'] = 'SEPA Lastschrift'
        self.sql('UPDATE assistent_rechnungsartikel SET payload_json=? WHERE id=4',(json.dumps(payload),))
        _,visible,_ = self.p.cockpit_data._material_records()
        self.assertEqual(len(visible),3)
        self.assertNotIn('SEPA',json.dumps(visible))

    def test_split_bank_number_is_rejected_without_rejoining_product_prose(self):
        from werkstatt_cockpit_api import _invoice_product
        row = dict(produkt_name='Synthetic material',produkt_beschreibung='Synthetic material',
            lieferant='TOP-Color GmbH',artikelnummer='DE8937',ve='Gebinde',gebinde='0400440532013000',
            groesse='',farbe='',menge='1')
        self.assertIsNone(_invoice_product(row,'TOP-Color GmbH',99,proposal=True))

    def test_partial_confirmation_provenance_is_never_relabelled_as_invoice_price(self):
        row = self.sql('SELECT payload_json FROM assistent_rechnungsartikel WHERE id=1')[0]
        payload = json.loads(row['payload_json'])
        payload.pop('document_type')
        payload['quelle'].pop('beleg_typ')
        self.sql('UPDATE assistent_rechnungsartikel SET payload_json=? WHERE id=1',(json.dumps(payload),))
        (self.p.UPLOAD_DIR/'synthetic-ab.pdf').write_bytes(b'%PDF changed original')
        result = self.catalog.preview_price('TOP-Color GmbH','87650001','Gebinde','2.5 L')
        self.assertIsNone(result['amount'])
        self.assertFalse(result['dispatchable'])

    def test_formula_warning_belongs_to_selected_package_not_another_same_day_variant(self):
        self.add_product(2,code='D8218/E1',package='1 L',amount='20.62',formula=False)
        standard = self.catalog.preview_price('TOP-Color GmbH','87650001','Gebinde','2.5 L')
        disputed = self.catalog.preview_price('TOP-Color GmbH','87650001','Gebinde','1 L')
        self.assertEqual(standard['status'],'ok')
        self.assertFalse(standard['review_required'])
        self.assertEqual((disputed['status'],disputed['amount']),('ok','20.62'))
        self.assertTrue(disputed['review_required'])

    def test_invalid_latest_quote_does_not_fall_back_and_malformed_preview_response_stays_empty(self):
        self.sql('DELETE FROM assistent_rechnungsartikel')
        self.add_product(date='2026-10-01')
        self.add_product(2,amount='20.62',formula=False,changes={'metadata_source':{}})
        self.assertNotIn('price',self.preview().json['matches'][0])
        with patch.object(self.catalog,'preview_price',return_value=None),patch.object(self.catalog,'historical_price') as older:
            result = self.preview().json
            self.assertEqual(len(result['matches']),1)
            self.assertNotIn('price',result['matches'][0])
            older.assert_not_called()

    def test_all_original_revocation_statuses_block_quote_without_deleting_product(self):
        for status in ('deleted','void','voided','canceled','cancelled','geloescht','gelöscht',
                       'storniert','quarantaene','quarantäne','quarantine','gesperrt'):
            with self.subTest(status=status):
                self.sql('UPDATE einkauf_belege SET status=? WHERE id=99',(status,))
                result = self.catalog.preview_price('TOP-Color GmbH','87650001','Gebinde','2.5 L')
                self.assertIsNone(result['amount'])


if __name__ == '__main__':
    unittest.main()
