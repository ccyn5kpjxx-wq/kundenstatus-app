"""One-action delivery receipt flow on synthetic data and an isolated database."""
from io import BytesIO
from pathlib import Path
import json
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_bestellvergleich_ui as fixtures
from test_einkaufseingang import png
from test_liefereingang import TEXT
from werkstatt_einkaufseingang import _hash, _json
from werkstatt_liefereingang import get_delivery, analyze_delivery_text
from werkstatt_lieferautomatik import matching_item


class AutomaticDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PriceUITests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.p, self.client = self.fixture.p, self.fixture.client
        self.service = get_delivery(self.p)
        self.change_order(supplier_name='TOP-Color GmbH', article_number='10004410',
            product_name='PPG Envirobase T4000/E0.5 Crystal Silver', variant='T4000/E0.5, 1 Gebinde à 0,5 Liter')
        self.text = TEXT
        self.p.extract_document_text_local = lambda *args: self.text
        self.before = self.fixture.rows('SELECT * FROM einkauf_material_dialoge')

    def change_order(self, **values):
        with self.p.workshop_intake.db() as db:
            row = db.execute('SELECT snapshot_json FROM einkauf_material_dialoge WHERE id=1').fetchone()
            data = json.loads(row['snapshot_json']); data.update(values)
            db.execute('UPDATE einkauf_material_dialoge SET snapshot_json=?,snapshot_hash=? WHERE id=1', (_json(data), _hash(data)))

    def post(self, key='', color='blue', **data):
        return self.client.post('/admin/assistent-bestellungen/lieferung/automatisch', data={
            'csrf_token': 'price-test', 'order_key': key, 'file': (BytesIO(png(color)), 'Lieferschein.png'), **data})

    def count(self):
        return len(self.fixture.rows('SELECT * FROM assistent_bestelllieferungen'))

    def test_selected_upload_analyzes_books_and_checks_delivery_in_one_action(self):
        with patch.object(self.fixture.f.manager, 'tick', side_effect=AssertionError('No dispatch')):
            result = self.post('material:1')
            self.assertEqual(result.status_code, 303)
            self.assertEqual(self.service.detail('material:1')['state'], 'geliefert')
            view = self.client.get(result.location).get_data(as_text=True)
        self.assertIn('✓ Lieferung zugeordnet', view)
        self.assertIn('Bestellung vollständig geliefert', view)
        self.assertNotIn('name="reviewed"', view.split('id="liefereingang"')[1].split('Preisvergleich')[0])
        self.assertEqual(self.count(), 1)
        event = self.service.detail('material:1')['events'][0]
        self.assertEqual((event['position'], event['quantity'], event['assignment_method']), (0, '1', 'automatic-v1'))
        self.assertEqual(self.p.workshop_intake.original(event['group_id'], event['file_id'])[0], png('blue'))
        self.assertEqual(self.fixture.rows('SELECT * FROM einkauf_material_dialoge'), self.before)
        self.assertEqual(self.fixture.rows('SELECT * FROM assistent_bestellanforderungen'), [])
        self.assertEqual(self.fixture.rows('SELECT * FROM assistent_bestellpreis_rechnungen'), [])

    def test_without_order_selection_matches_saved_order_and_alias_once(self):
        self.fixture.f.order('alias', request_id='material:1')
        self.assertEqual(self.post().status_code, 303)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.service.detail('material:1')['state'], 'geliefert')

    def test_same_upload_and_different_photo_of_same_note_never_count_twice(self):
        self.change_order(quantity='2')
        for color in ['blue', 'blue', 'red']:
            self.assertEqual(self.post(color=color).status_code, 303)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.service.detail('material:1')['quantity'], '1')
        self.assertEqual(self.service.detail('material:1')['state'], 'teillieferung')

    def test_part_delivery_then_second_number_checks_only_when_complete(self):
        self.change_order(quantity='2')
        self.post('material:1')
        self.assertEqual(self.service.detail('material:1')['state'], 'teillieferung')
        self.text = TEXT.replace('LS000001', 'LS000002')
        self.post('material:1', color='red')
        self.assertEqual(self.service.detail('material:1')['state'], 'geliefert')
        self.assertEqual(self.count(), 2)

    def test_partial_document_keeps_review_and_retry_actions_visible(self):
        self.text = TEXT + '\n2. 10009999 OTHER ITEM 1,00 1,00 1,000 Stück 1,00\n'
        result = self.post('material:1')
        view = self.client.get(result.location).get_data(as_text=True)
        self.assertEqual(self.count(), 1)
        self.assertIn('Weitere Positionen sind noch offen', view)
        self.assertIn('Beleg analysieren & automatisch zuordnen', view)
        self.assertIn('Geprüfte Liefermenge zuordnen', view)

    def test_legacy_manual_evidence_without_number_blocks_uncertain_second_photo(self):
        from werkzeug.datastructures import FileStorage
        self.change_order(quantity='2')
        order, group, file = self.service.attach('material:1', FileStorage(stream=BytesIO(png('blue')), filename='old.png'))
        self.service.record('material:1', {'group_id':group['id'], 'file_id':file['id'], 'page':1,
            'position':0, 'quantity':'1', 'reviewed':True,
            **{name:order[name] for name in ('supplier','sku','variant','unit')}})
        result = self.post(color='red')
        self.assertIn('Älterer Liefernachweis', self.client.get(result.location).get_data(as_text=True))
        self.assertEqual(self.count(), 1)

    def test_multiple_matching_orders_require_review(self):
        order = self.service.order('material:1')
        self.fixture.f.material(2, supplier_name=order['supplier'], article_number=order['sku'],
            product_name=order['product'], variant=order['variant'])
        result = self.post()
        self.assertIn('keine eindeutige', self.client.get(result.location).get_data(as_text=True))
        self.assertEqual(self.count(), 0)
        self.assertEqual(len(self.fixture.rows('SELECT * FROM einkauf_eingang_dateien')), 1)

    def test_fully_delivered_old_order_does_not_block_a_new_open_order(self):
        self.post()
        order = self.service.order('material:1')
        self.fixture.f.material(2, supplier_name=order['supplier'], article_number=order['sku'],
            product_name=order['product'], variant=order['variant'])
        self.text = TEXT.replace('LS000001', 'LS000002')
        self.post(color='red')
        self.assertEqual(self.count(), 2)
        self.assertEqual(self.service.detail('material:2')['state'], 'geliefert')

    def test_more_than_remaining_is_not_silently_cut_or_booked(self):
        self.text = TEXT.replace('1,00 1,00 0,500 Ltr/KG 0,50', '2,00 2,00 0,500 Ltr/KG 1,00')
        result = self.post()
        self.assertIn('offenen Bestellmenge', self.client.get(result.location).get_data(as_text=True))
        self.assertEqual(self.count(), 0)

    def test_gate_requires_positive_identity_pack_and_integral_quantity(self):
        order = self.service.order('material:1')
        item = analyze_delivery_text(TEXT)['items'][0]
        self.assertTrue(matching_item(order, TEXT, item))
        for field, value in [('sku','010004410'), ('fee',True), ('quantity','0.5'),
                             ('content','1,000 Ltr/KG'), ('total_content','1,00'),
                             ('description','PPG T4000/E0.5 ENVIROBASE JET BLACK')]:
            with self.subTest(field=field):
                self.assertFalse(matching_item(order, TEXT, dict(item, **{field:value})))
        for field, value in [('supplier','Other Top-Color GmbH'), ('unit','Liter'),
                             ('dispatch_known',False), ('variant','T4000/E0.5 1 Liter'),
                             ('product','PPG Envirobase T4000/E0.5 Jet Black'),
                             ('product','PPG Envirobase T4000/E0.5 Crystal Silver 1 Liter'),
                             ('variant','T4000/E0.5 blau'), ('created_at','2026-10-09T08:00:00+00:00')]:
            with self.subTest(order_field=field):
                self.assertFalse(matching_item(dict(order, **{field:value}), TEXT, item))
        self.assertFalse(matching_item(order, TEXT.replace('TOP-COLOR','OTHER'), item))
        self.assertFalse(matching_item(order, TEXT + '\nTEST26-LS000002', item))
        self.assertFalse(matching_item(dict(order, variant='T4000/E0.5, 6 Gebinde à 0,5 Liter'), TEXT, item))
        self.assertFalse(matching_item(order, TEXT, dict(item, description=item['description']+' 1 Liter')))

    def test_unknown_or_failed_analysis_preserves_original_without_booking(self):
        for text, color in [('', 'blue'), ('Other receipt', 'red')]:
            self.text = text
            result = self.post(color=color)
            self.assertEqual(result.status_code, 303)
            self.assertEqual(self.count(), 0)
        self.assertEqual(len(self.fixture.rows('SELECT * FROM einkauf_eingang_dateien')), 2)

    def test_timeout_keeps_uploaded_original_and_books_nothing(self):
        self.p.RUNNING_ON_RENDER = True
        with patch('werkstatt_belegauslese.read_receipt', side_effect=TimeoutError):
            result = self.post()
        self.assertEqual(result.status_code, 303)
        self.assertIn('Zeitlimit', self.client.get(result.location).get_data(as_text=True))
        self.assertEqual(self.count(), 0)
        self.assertEqual(self.p.workshop_intake.original(1, 1)[0], png('blue'))

    def test_duplicate_position_and_changed_duplicate_quantity_require_review(self):
        self.text = TEXT + '\n0. 10004410 PPG T4000/E0.5 ENVIROBASE 1,00 1,00 0,500 Ltr/KG 0,50\nCRYSTAL SILBER\n'
        self.post()
        self.assertEqual(self.count(), 0)
        self.text = TEXT
        self.post(color='red')
        self.assertEqual(self.count(), 1)
        self.text = TEXT.replace('1,00 1,00 0,500 Ltr/KG 0,50', '2,00 2,00 0,500 Ltr/KG 1,00')
        result = self.post('material:1', color='green')
        self.assertIn('abweichenden Angaben', self.client.get(result.location).get_data(as_text=True))
        self.assertEqual(self.count(), 1)
        from hashlib import sha256
        file = next(file for file in self.service.files(self.service.order('material:1'))
                    if file['sha256'] == sha256(png('green')).hexdigest())
        self.assertFalse(file['assignment_complete'])

    def test_auth_csrf_and_get_head_cannot_book(self):
        route = '/admin/assistent-bestellungen/lieferung/automatisch'
        self.assertEqual(self.p.app.test_client().post(route).status_code, 403)
        self.assertEqual(self.client.post(route).status_code, 400)
        self.assertEqual(self.client.get(route).status_code, 303)
        self.assertEqual(self.client.head(route).status_code, 303)
        self.assertEqual(self.count(), 0)

    def test_incoming_upload_defaults_to_automatic_order_choice_and_neutral_retry(self):
        view = self.client.get('/admin/assistent-bestellungen/eingang/ansicht').get_data(as_text=True)
        self.assertIn('action="/admin/assistent-bestellungen/lieferung/automatisch"', view)
        self.assertIn('<option value="" selected>Automatisch erkennen</option>', view)
        self.assertNotIn('name="order_key" required', view)
        self.text = ''
        result = self.post()
        view = self.client.get(result.location).get_data(as_text=True)
        self.assertIn('Zuordnung erneut versuchen', view)

    def test_internal_automatic_record_revalidates_proof_and_quantity(self):
        order, group, file = self.service.attach('material:1', __import__('werkzeug').datastructures.FileStorage(stream=BytesIO(png()), filename='test.png'))
        self.service.analyze(order['key'], group['id'], file['id'])
        payload = {'group_id':group['id'], 'file_id':file['id'], 'page':1, 'position':0, 'quantity':'2',
            **{name:order[name] for name in ('supplier','sku','variant','unit')}}
        with self.assertRaises(ValueError):
            self.service.record(order['key'], payload, automatic=True)
        self.assertEqual(self.count(), 0)


if __name__ == '__main__':
    unittest.main()
