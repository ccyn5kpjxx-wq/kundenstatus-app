"""Synthetic order/delivery HTTP flow. No supplier calls or operational records."""
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_bestellvergleich_ui as fixtures
from test_einkaufseingang import png
from werkstatt_liefereingang import get_delivery, analyze_delivery_text, document_position, receipt_payload, delivery_ocr_rows
from werkstatt_einkaufseingang import IntakeConflict
from werkzeug.datastructures import FileStorage

TEXT = '''TOP-COLOR
LIEFERSCHEIN
Beleg-Nr. TEST26-LS000001
Beleg-Datum 07.10.2026
Seite 1
Pos. Art.-Nr. Bezeichnung geliefert bestellt Inhalt ME Menge
0. 10004410 PPG T4000/E0.5 ENVIROBASE 1,00 1,00 0,500 Ltr/KG 0,50
WF31B CRYSTAL SILBER 0,5 Liter
1. 00000071 Logistik- 1,00 1,00 1,000 Stück 1,00
ZZZ999 /Energiekostenpauschale
'''


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PriceUITests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.p, self.client = self.fixture.p, self.fixture.client
        self.service = get_delivery(self.p)
        self.p.extract_document_text_local = lambda path, name: TEXT.replace('10004410', 'TEST-4000')

    def post(self, action, **data):
        return self.client.post('/admin/assistent-bestellungen/lieferung/' + action,
            data={'csrf_token': 'price-test', 'order_key': 'material:1', **data})

    def upload(self, color='blue'):
        return self.post('beleg', file=(BytesIO(png(color)), 'Lieferschein.png'))

    def form(self, **changes):
        return dict(group_id='1', file_id='1', page='1', position='0', quantity='1',
            supplier='Testlieferant', sku='TEST-4000', variant='50 mm', unit='Stück', reviewed='ja', **changes)

    def view(self):
        return self.client.get('/admin/assistent-bestellungen?bestellung=material:1').get_data(as_text=True)

    def rows(self, sql):
        return self.fixture.rows(sql)

    def test_intake_upload_without_selected_order_leads_to_analysis(self):
        before = self.rows('SELECT * FROM einkauf_material_dialoge')
        route = '/admin/assistent-bestellungen/eingang/ansicht'
        with patch.object(self.fixture.f.manager, 'tick', side_effect=AssertionError('No dispatch')):
            view = self.client.get(route)
            self.assertEqual(view.status_code, 200)
            html = view.get_data(as_text=True)
            self.assertIn('name="file"', html)
            self.assertIn('name="order_key"', html)
            self.assertNotIn('name="order_key" required', html)
            self.assertIn('<option value="" selected>Automatisch erkennen</option>', html)
            self.assertIn('value="material:1"', html)
            self.assertIn('Lieferschein hochladen &amp; automatisch zuordnen', html)
            result = self.post('beleg', return_to='eingang', file=(BytesIO(png()), 'Lieferschein.png'))
            self.assertEqual(result.status_code, 303)
            self.assertTrue(result.location.endswith('?bestellung=material:1#liefereingang'))
            self.assertIn('Beleg analysieren', self.client.get(result.location).get_data(as_text=True))
            original = self.client.get('/admin/assistent-bestellungen/eingang/1/dateien/1/original')
            self.assertEqual(original.data, png())
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])
        self.assertEqual(self.rows('SELECT * FROM assistent_bestellanforderungen'), [])
        self.assertEqual(self.rows('SELECT * FROM einkauf_material_dialoge'), before)

    def test_intake_upload_error_returns_to_visible_upload(self):
        for key in ('', 'material:9999', 'https://example.invalid/'):
            result = self.post('beleg', order_key=key, return_to='eingang',
                file=(BytesIO(png()), 'Lieferschein.png'))
            self.assertEqual(result.status_code, 303)
            self.assertTrue(result.location.endswith('/eingang/ansicht#lieferschein-upload'))
            view = self.client.get(result.location)
            self.assertEqual(view.status_code, 200)
            self.assertIn('Lieferschein hochladen &amp; automatisch zuordnen', view.get_data(as_text=True))
        self.assertEqual(self.rows('SELECT * FROM einkauf_eingang'), [])
        self.assertEqual(self.rows('SELECT * FROM einkauf_eingang_dateien'), [])
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])
        result = self.post('beleg', return_to='eingang')
        self.assertTrue(result.location.endswith('/eingang/ansicht#lieferschein-upload'))
        self.assertIn('Lieferschein als Foto oder PDF auswählen', self.client.get(result.location).get_data(as_text=True))

    def test_upload_address_recovers_after_failed_post_without_writes(self):
        route = '/admin/assistent-bestellungen/lieferung/'
        self.assertEqual(self.p.app.test_client().get(route + 'beleg').status_code, 403)
        before = self.rows('SELECT * FROM einkauf_material_dialoge')
        with patch.object(self.fixture.f.manager, 'tick', side_effect=AssertionError('No dispatch')):
            result = self.client.get(route + 'beleg?order_key=material:1')
            self.assertEqual(result.status_code, 303)
            self.assertTrue(result.location.endswith('/eingang/ansicht#lieferschein-upload'))
            view = self.client.get(result.location)
            self.assertEqual(view.status_code, 200)
            self.assertIn('Lieferschein und die passende Bestellung erneut auswählen', view.get_data(as_text=True))
            self.assertEqual(self.client.get(route + 'analyse').status_code, 405)
            self.assertEqual(self.client.get(route + 'zuordnen').status_code, 405)
            self.assertEqual(self.client.get(route + 'unknown').status_code, 404)
            self.assertEqual(self.client.post(route + 'beleg').status_code, 400)
            head = self.client.head(route + 'beleg', data={
                'order_key': 'material:1', 'file': (BytesIO(png()), 'Lieferschein.png')})
            self.assertEqual(head.status_code, 303)
            for action in ('analyse', 'zuordnen'):
                self.assertEqual(self.client.head(route + action, data=self.form()).status_code, 405)
        self.assertEqual(self.rows('SELECT * FROM einkauf_material_dialoge'), before)
        self.assertEqual(self.rows('SELECT * FROM einkauf_eingang'), [])
        self.assertEqual(self.rows('SELECT * FROM einkauf_eingang_dateien'), [])
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])

    def test_intake_order_choices_are_canonical_and_skip_invalid_snapshots(self):
        self.fixture.f.order('alias', request_id='material:1')
        self.fixture.f.material(2, supplier_name='Other supplier')
        with self.p.workshop_intake.db() as db:
            db.execute("UPDATE einkauf_material_dialoge SET snapshot_hash='invalid' WHERE id=2")
        from werkstatt_liefereingang import OrderDelivery
        with patch.object(OrderDelivery, 'init_schema', side_effect=AssertionError('GET mutates schema')):
            self.assertEqual([item['key'] for item in self.service.choices()], ['material:1'])
            html = self.client.get('/admin/assistent-bestellungen/eingang/ansicht').get_data(as_text=True)
        self.assertEqual(html.count('value="material:1"'), 1)
        self.assertNotIn('value="material:2"', html)

    def test_intake_choices_use_actual_noniterable_postgres_cursor_contract(self):
        import ast
        source = Path(__file__).resolve().parents[1].joinpath('app.py').read_text(encoding='utf-8')
        node = next(item for item in ast.parse(source).body
                    if isinstance(item, ast.ClassDef) and item.name == 'PostgresCursor')
        namespace = {}
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<actual-postgres-cursor>', 'exec'), namespace)
        postgres_cursor = namespace['PostgresCursor']
        get_db = self.p.get_db

        class PostgresReadTransport:
            def __init__(self):
                self.raw = get_db()
            def execute(self, sql, params=()):
                cursor = self.raw.execute(sql, params)
                return postgres_cursor(cursor.fetchall() if cursor.description else None,
                    lastrowid=cursor.lastrowid, rowcount=cursor.rowcount)
            def __getattr__(self, name):
                return getattr(self.raw, name)

        with patch.object(self.p, 'get_db', side_effect=PostgresReadTransport):
            self.assertEqual([order['key'] for order in self.service.choices()], ['material:1'])
            response = self.client.get('/admin/assistent-bestellungen/eingang/ansicht')
            self.assertEqual(response.status_code, 200)
            self.assertIn('Lieferschein hochladen &amp; automatisch zuordnen', response.get_data(as_text=True))

    def test_upload_analysis_assignment_end_to_end_no_order_changes(self):
        before = self.rows('SELECT * FROM einkauf_material_dialoge')
        with patch.object(self.fixture.f.manager, 'tick', side_effect=AssertionError('No dispatch')):
            self.assertEqual(self.upload().status_code, 303)
            self.assertEqual(self.upload().status_code, 303)
            self.assertEqual(len(self.rows('SELECT * FROM einkauf_eingang_dateien')), 1)
            self.assertEqual(self.post('analyse', group_id='1', file_id='1').status_code, 303)
            self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])
            view = self.view()
            self.assertIn('Analyse vorhanden', view)
            self.assertIn('TEST-4000', view)
            self.assertIn('Nebenkostenposition', view)
            self.assertEqual(self.post('zuordnen', **self.form()).status_code, 303)
            self.assertIn('Vollständig geliefert', self.view())
            self.assertEqual(self.rows('SELECT * FROM einkauf_material_dialoge'), before)
            self.assertEqual(self.rows('SELECT * FROM assistent_bestellanforderungen'), [])
            self.assertEqual(self.rows('SELECT * FROM assistent_bestellpreis_rechnungen'), [])
            self.assertEqual(len(self.rows('SELECT * FROM assistent_bestelllieferungen')), 1)

    def test_zero_position_idempotence_conflict_and_partial_overdelivery(self):
        self.upload()
        data = self.form(); data['quantity'] = '.5'
        self.post('zuordnen', **data)
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])
        data['quantity'] = '0,5'
        self.post('zuordnen', **data)
        self.post('zuordnen', **data)
        self.assertEqual(self.service.detail('material:1')['state'], 'teillieferung')
        self.assertEqual(len(self.rows('SELECT * FROM assistent_bestelllieferungen')), 1)
        data['quantity'] = '1'
        response = self.post('zuordnen', **data)
        self.assertIn('bereits anders zugeordnet', self.client.get(response.location).get_data(as_text=True))
        self.assertEqual(self.service.detail('material:1')['quantity'], '0.5')
        self.upload('red'); data.update(file_id='2', quantity='1')
        self.post('zuordnen', **data)
        self.assertEqual(self.service.detail('material:1')['state'], 'mehrlieferung')

    def test_authentication_csrf_and_foreign_original(self):
        route = '/admin/assistent-bestellungen/lieferung/beleg'
        self.assertEqual(self.p.app.test_client().post(route).status_code, 403)
        self.assertEqual(self.client.post(route).status_code, 400)
        self.upload()
        self.fixture.f.material(2, supplier_name='Other supplier')
        response = self.post('analyse', order_key='material:2', group_id='1', file_id='1')
        self.assertIn('nicht zur Belegsammlung', self.client.get(response.location).get_data(as_text=True))
        for field, value in [('reviewed', ''), ('sku', 'WRONG'), ('supplier', 'Other supplier'),
                             ('variant', '30 mm'), ('unit', 'Liter'), ('position', '-1'), ('page', '2')]:
            data = self.form(); data[field] = value
            self.post('zuordnen', **data)
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])

    def test_restore_lock_covers_upload_analysis_and_booking(self):
        depth = [0]
        @contextmanager
        def lock():
            depth[0] += 1
            try:
                yield
            finally:
                depth[0] -= 1
        self.p.portal_originals_operation_lock = lock
        attach = self.p.workshop_intake.attach
        def locked_attach(*args, **kwargs):
            self.assertEqual(depth[0], 1)
            return attach(*args, **kwargs)
        with patch.object(self.p.workshop_intake, 'attach', side_effect=locked_attach):
            self.upload()
        self.p.extract_document_text_local = lambda path, name: self.assertEqual(depth[0], 1) or TEXT
        self.post('analyse', group_id='1', file_id='1')
        original = self.p.workshop_intake.original
        def locked_original(*args):
            self.assertEqual(depth[0], 1)
            return original(*args)
        with patch.object(self.p.workshop_intake, 'original', side_effect=locked_original):
            self.post('zuordnen', **self.form())
        self.assertEqual(depth[0], 0)

    def test_get_overview_has_no_database_writes(self):
        self.upload()
        before = self.rows('SELECT * FROM einkauf_eingang')
        from werkstatt_liefereingang import OrderDelivery
        with patch.object(OrderDelivery, 'init_schema', side_effect=AssertionError('GET mutates schema')):
            self.assertIn('Lieferschein erfassen', self.view())
        self.assertEqual(before, self.rows('SELECT * FROM einkauf_eingang'))

    def test_analysis_failure_keeps_original_and_manual_form(self):
        self.upload()
        self.p.extract_document_text_local = lambda *args: ''
        response = self.post('analyse', group_id='1', file_id='1')
        self.assertIn('Keine lesbare Auslese', self.client.get(response.location).get_data(as_text=True))
        original = self.client.get('/admin/assistent-bestellungen/eingang/1/dateien/1/original')
        self.assertEqual(original.data, png())
        self.assertIn('Geprüfte Liefermenge zuordnen', self.view())

    def test_render_analysis_uses_bounded_reader_and_reuses_success(self):
        self.upload()
        self.p.RUNNING_ON_RENDER = True
        with patch('werkstatt_belegauslese.read_receipt', return_value=TEXT) as reader:
            with patch.object(self.p, 'extract_document_text_local', side_effect=AssertionError('Unbounded reader')):
                for _ in range(2):
                    self.assertEqual(self.post('analyse', group_id='1', file_id='1').status_code, 303)
        reader.assert_called_once()
        self.assertIn('Analyse vorhanden', self.view())
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])

    def test_render_timeout_is_visible_retryable_and_preserves_original(self):
        self.upload()
        self.p.RUNNING_ON_RENDER = True
        with patch('werkstatt_belegauslese.read_receipt', side_effect=TimeoutError):
            result = self.post('analyse', group_id='1', file_id='1')
        self.assertIn('Zeitlimit nach 20 Sekunden', self.client.get(result.location).get_data(as_text=True))
        self.assertEqual(self.p.workshop_intake.original(1, 1)[0], png())
        with patch('werkstatt_belegauslese.read_receipt', return_value=TEXT) as reader:
            self.post('analyse', group_id='1', file_id='1')
        reader.assert_called_once()
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])

    def test_concurrent_analysis_returns_without_waiting_or_mutating(self):
        self.upload()
        self.service._analysis_lock.acquire()
        try:
            with patch.object(self.service, 'source', side_effect=AssertionError('Must not wait for originals')):
                result = self.post('analyse', group_id='1', file_id='1')
        finally:
            self.service._analysis_lock.release()
        self.assertIn('Eine Beleganalyse läuft bereits', self.client.get(result.location).get_data(as_text=True))
        self.assertEqual(self.rows('SELECT extraction_status FROM einkauf_eingang_dateien')[0]['extraction_status'], 'offen')

    def test_analysis_reads_original_blob_once_and_overview_uses_metadata(self):
        self.upload()
        original_reader = self.p.workshop_intake._file
        with patch.object(self.p.workshop_intake, '_file', wraps=original_reader) as blob:
            self.post('analyse', group_id='1', file_id='1')
            self.assertEqual(blob.call_count, 1)
        with patch.object(self.p.workshop_intake, '_file', side_effect=AssertionError('Original not needed')):
            self.assertIn('Analyse vorhanden', self.view())

    def test_receipt_cannot_be_assigned_twice_to_different_order(self):
        self.upload(); self.post('zuordnen', **self.form())
        self.fixture.f.material(2, supplier_name='Testlieferant', variant='50 mm')
        self.post('beleg', order_key='material:2', file=(BytesIO(png()), 'same-original.png'))
        data = self.form(); data.update(group_id='2', file_id='2', order_key='material:2')
        response = self.post('zuordnen', **data)
        self.assertIn('bereits anders zugeordnet', self.client.get(response.location).get_data(as_text=True))
        self.assertEqual(self.service.detail('material:2')['state'], 'offen')

    def test_invoice_and_delivery_share_collection_and_remain_separate(self):
        self.fixture.upload()
        self.upload('red')
        self.assertEqual(len(self.rows('SELECT * FROM einkauf_eingang')), 1)
        self.assertEqual([r['kind'] for r in self.rows('SELECT kind FROM einkauf_eingang_dateien ORDER BY id')], ['rechnung', 'lieferschein'])

    def test_canonical_order_alias_cannot_book_the_same_position_twice(self):
        self.upload(); self.post('zuordnen', **self.form())
        self.fixture.f.order('alias', request_id='material:1')
        data = self.form(); data['reviewed'] = True
        result = self.service.record('dispatch:alias', data)
        self.assertEqual(result['state'], 'geliefert')
        self.assertEqual(len(self.rows('SELECT * FROM assistent_bestelllieferungen')), 1)

    def test_missing_or_changed_original_fails_closed(self):
        self.upload(); self.post('zuordnen', **self.form())
        with self.p.workshop_intake.db() as db:
            db.execute("UPDATE einkauf_eingang_dateien SET kind='rechnung' WHERE id=1")
        self.assertEqual(self.service.detail('material:1')['state'], 'pruefen')
        self.assertEqual(self.service.detail('material:1')['quantity'], '0')

    def test_recognized_logistics_position_is_not_material(self):
        self.upload()
        with self.p.workshop_intake.db() as db:
            db.execute('UPDATE einkauf_eingang_dateien SET draft_text=? WHERE id=1', (TEXT,))
        data = self.form(); data['position'] = '1'
        response = self.post('zuordnen', **data)
        self.assertIn('Nebenkosten oder einen anderen Artikel', self.client.get(response.location).get_data(as_text=True))
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])

    def test_admin_can_correct_ocr_typo_with_audited_reason(self):
        self.upload()
        with self.p.workshop_intake.db() as db:
            db.execute('UPDATE einkauf_eingang_dateien SET draft_text=? WHERE id=1',
                (TEXT.replace('10004410', 'TEST-400O'),))
        data = self.form()
        self.post('zuordnen', **data)
        self.assertEqual(self.rows('SELECT * FROM assistent_bestelllieferungen'), [])
        data.update(correction_confirmed='ja', correction_reason='Artikelnummer am Original kontrolliert: 0 statt O.')
        self.post('zuordnen', **data)
        self.assertEqual(self.service.detail('material:1')['state'], 'geliefert')
        self.assertIn('0 statt O', self.service.detail('material:1')['events'][0]['ocr_correction'])

    def test_pdf_pages_with_same_printed_position_are_separate(self):
        import fitz
        with fitz.open() as doc:
            doc.new_page(); doc.new_page(); raw = doc.tobytes()
        self.post('beleg', file=(BytesIO(raw), 'two-pages.pdf'))
        data = self.form(); data['quantity'] = '0.5'
        self.post('zuordnen', **data)
        data['page'] = '2'; self.post('zuordnen', **data)
        self.assertEqual(self.service.detail('material:1')['state'], 'geliefert')
        self.assertEqual(len(self.rows('SELECT * FROM assistent_bestelllieferungen')), 2)


class ParserTests(unittest.TestCase):
    def test_ocr_boxes_keep_columns_in_one_row(self):
        words = [('0.10004410PPG ENVIROBASE', 10, 20, 250), ('1,00', 400, 20, 45),
                 ('1,00', 480, 20, 45), ('0,500Ltr/KG', 560, 20, 100), ('0,50', 700, 20, 45),
                 ('CRYSTAL SILBER 0,5 Liter', 150, 55, 250)]
        boxes = [([[x,y],[x+w,y],[x+w,y+20],[x,y+20]], word, .99) for word,x,y,w in reversed(words)]
        text = 'LIEFERSCHEIN\ngeliefert bestellt Inhalt\n' + delivery_ocr_rows(boxes)
        rows = analyze_delivery_text(text)['items']
        self.assertEqual(rows[0]['sku'], '10004410')
        self.assertEqual(rows[0]['quantity'], '1.00')
        self.assertIn('CRYSTAL', rows[0]['description'])

    def test_topcolor_zero_container_content_and_fee(self):
        result = analyze_delivery_text(TEXT)
        self.assertEqual(result['number'], 'TEST26-LS000001')
        self.assertEqual(result['date'], '07.10.2026')
        self.assertEqual(len(result['items']), 2)
        paint, fee = result['items']
        self.assertEqual(paint['position'], 0)
        self.assertEqual(paint['quantity'], '1.00')
        self.assertEqual(paint['content'], '0,500 Ltr/KG')
        self.assertEqual(paint['total_content'], '0,50')
        self.assertFalse(paint['fee']); self.assertTrue(fee['fee'])

    def test_ambiguous_ocr_never_guesses_a_quantity(self):
        for text in ('invoice 0 123456 paint 1.0', TEXT.replace('geliefert', 'unknown'), TEXT.replace('1,00 1,00 0,500', 'unleserlich')):
            result = analyze_delivery_text(text)
            self.assertFalse(any(item['sku'] == '10004410' for item in result['items']))

    def test_printed_position_is_not_database_id(self):
        self.assertEqual(document_position(0), 0)
        self.assertEqual(document_position('0'), 0)
        for value in (False, -1, '0.5', '', None, 1000000):
            with self.assertRaises(ValueError):
                document_position(value)


if __name__ == '__main__':
    unittest.main()
