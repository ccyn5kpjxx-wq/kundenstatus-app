"""Vehicle creation/edit regressions on synthetic SQLite, with network disabled.

Run: python scripts/test_partner_vehicle_form.py
"""
from io import BytesIO
import unittest
from unittest.mock import patch

from flow_test import portal, with_csrf


class VehicleFormTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING'] = True
        portal.init_db()
        cls.partner = portal.get_autohaus_by_slug('kaesmann')

    def setUp(self):
        self.client = portal.app.test_client()
        with self.client.session_transaction() as session:
            session['partner_autohaus_id'] = self.partner['id']
        self.notifications = patch.object(portal, 'notify_workshop_whatsapp_for_new_order', return_value=(True, []))
        self.notifications.start()
        self.addCleanup(self.notifications.stop)

    def post(self, path, data):
        return self.client.post(path, data=with_csrf(self.client, data))

    def create(self, **extra):
        response = self.post('/partner/kaesmann/neu', {
            'aktion': 'speichern', 'fahrzeug': 'Synthetisches Testauto', 'kennzeichen': 'TEST-123',
            'farbcode': 'LB9A / Z5Z5', 'farbton': 'Weiss', 'farbton_2': 'Schwarz',
            'kunde_name': 'Testreferenz', 'analyse_text': 'Tür lackieren',
            'annahme_datum': '2026-10-01', 'abholtermin': '2026-10-02',
            'annahme_uhrzeit': '09:15', 'abhol_uhrzeit': '16:30',
            'transport_art': 'hol_und_bring', 'abhol_adresse': 'Testweg 1',
            '_draft_id': 'synthetic-draft', **extra,
        })
        self.assertEqual(response.status_code, 302)
        return int(response.location.rsplit('/', 1)[-1])

    def test_paint_code_creation_and_reopening(self):
        order_id = self.create()
        order = portal.get_auftrag(order_id)
        self.assertEqual(order['farbcode'], 'LB9A / Z5Z5')
        self.assertEqual(order['farbton_2'], 'Schwarz')
        response = self.client.get(f'/partner/kaesmann/auftrag/{order_id}')
        self.assertIn('value="LB9A / Z5Z5"', response.text)
        self.assertIn('name="annahme_uhrzeit"', response.text)
        with self.client.session_transaction() as session:
            self.assertIn('synthetic-draft', session['partner_completed_drafts'])

    def test_online_paint_order_transfers_color(self):
        response = self.post('/partner/kaesmann/lackierauftrag', {
            'aktion': 'fahrzeug_anlegen', 'typ': 'Testwagen', 'farb_nr': 'SAW',
            'kennzeichen': 'TEST-456',
        })
        self.assertEqual(response.status_code, 302)
        order = portal.get_auftrag(int(response.location.rsplit('/', 1)[-1]))
        self.assertEqual(order['farbcode'], 'SAW')

    def test_validation_and_no_file_analysis_preserve_input(self):
        for action in ('speichern', 'upload_analyze'):
            response = self.post('/partner/kaesmann/neu', {
                'aktion': action, 'farbcode': 'LB9A', 'kunde_name': 'Testreferenz',
                'annahme_uhrzeit': '09:15', 'abhol_adresse': 'Testweg 1',
                'transport_art': 'hol_und_bring', 'beschreibung': 'Text bleibt erhalten',
                '_draft_id': 'not-completed',
            })
            self.assertEqual(response.status_code, 200)
            for value in ('LB9A', 'Testreferenz', '09:15', 'Testweg 1', 'Text bleibt erhalten'):
                self.assertIn(value, response.text)
            with self.client.session_transaction() as session:
                self.assertNotIn('not-completed', session.get('partner_completed_drafts', []))

    def test_analysis_does_not_replace_manual_paint_code(self):
        result = portal.partner_new_analysis_form_values(
            {'farbcode': 'MANUELL'}, {'fields': {'farbcode': 'OCR'}, 'file_names': ['test.txt']})
        self.assertEqual(result['farbcode'], 'MANUELL')

    def test_document_color_reaches_preview_and_manual_value_wins(self):
        from werkzeug.datastructures import FileStorage
        ai = portal.normalize_openai_document_data({'farbnummer': 'LB9A'})
        self.assertEqual(ai['farbcode'], 'LB9A')
        local = portal.parse_document_fields('Farb-Nr.\nSAW\n', 'synthetisch.txt')
        self.assertEqual(local['farbcode'], 'SAW')
        with patch.object(portal, 'build_document_analysis_bundle_safe', return_value={
            'text': 'Farb-Nr.\nSAW\n', 'source': 'synthetic', 'status': 'ok',
        }):
            result = portal.analyze_partner_new_files([
                FileStorage(stream=BytesIO(b'Synthetic color'), filename='synthetisch.txt')])
        self.assertEqual(result['fields']['farbcode'], 'SAW')
        self.assertEqual(portal.partner_new_analysis_form_values({}, result)['farbcode'], 'SAW')
        self.assertEqual(portal.partner_new_analysis_form_values({'farbcode': 'LB9A'}, result)['farbcode'], 'LB9A')

    def test_missing_previously_analyzed_file_preserves_color(self):
        response = self.post('/partner/kaesmann/neu', {
            'aktion': 'speichern', 'fahrzeug': 'Testwagen', 'farbcode': 'SAW',
            'analyse_datei_erforderlich': '1',
        })
        self.assertEqual(response.status_code, 200)
        self.assertIn('value="SAW"', response.text)
        self.assertIn('erneut auswählen', response.text)

    def test_file_failure_after_creation_is_reported(self):
        with patch.object(portal, 'partner_new_analysis_token_valid', return_value=True), \
                patch.object(portal, 'save_uploads', side_effect=OSError('Synthetic disk failure')):
            order_id = self.create(analyse_abgeschlossen='1', dateien=(BytesIO(b'Test'), 'test.txt'))
        response = self.client.get(f'/partner/kaesmann/auftrag/{order_id}')
        self.assertIn('Bitte laden Sie die fehlenden Dateien hier erneut hoch', response.text)
        self.assertEqual(portal.get_auftrag(order_id)['farbcode'], 'LB9A / Z5Z5')

    def test_edit_after_creation_and_partial_post(self):
        order_id = self.create()
        path = f'/partner/kaesmann/auftrag/{order_id}'
        response = self.post(path, {'aktion': 'speichern', 'farbcode': 'SAW',
            'annahme_uhrzeit': '10:00', 'abhol_uhrzeit': '17:00', 'abhol_adresse': 'Testweg 2',
            'analyse_text': 'Zusätzlich Kotflügel', 'beschreibung': 'Nachträgliche Änderung'})
        self.assertEqual(response.status_code, 302)
        order = portal.get_auftrag(order_id)
        self.assertEqual(order['farbcode'], 'SAW')
        self.assertEqual(order['fahrzeug'], 'Synthetisches Testauto')
        self.assertEqual(order['annahme_uhrzeit'], '10:00')
        self.assertEqual(order['abhol_uhrzeit'], '17:00')
        self.assertEqual(order['abhol_adresse'], 'Testweg 2')
        self.assertEqual(order['analyse_text'], 'Zusätzlich Kotflügel')
        self.assertEqual(order['beschreibung'], 'Nachträgliche Änderung')
        html = self.client.get(path).text
        self.assertIn('name="analyse_text"', html)
        self.assertIn('Nachträgliche Änderung</textarea>', html)
        # An intentional blank clears a field; an omitted field is preserved.
        self.post(path, {'aktion': 'speichern', 'farbcode': ''})
        self.assertEqual(portal.get_auftrag(order_id)['farbcode'], '')

    def test_append_documents_without_changing_vehicle(self):
        order_id = self.create()
        path = f'/partner/kaesmann/auftrag/{order_id}'
        before = portal.get_auftrag(order_id)
        for name in ('nachtrag-a.txt', 'nachtrag-b.txt'):
            response = self.post(path, {'aktion': 'quick_upload',
                'dateien': (BytesIO(b'Synthetic attachment'), name)})
            self.assertEqual(response.status_code, 302)
        after = portal.get_auftrag(order_id)
        for field in ('farbcode', 'farbton', 'fahrzeug', 'kunde_name', 'annahme_uhrzeit', 'abhol_adresse'):
            self.assertEqual(before[field], after[field], field)
        names = [item['original_name'] for item in portal.list_dateien(order_id)]
        self.assertIn('nachtrag-a.txt', names)
        self.assertIn('nachtrag-b.txt', names)

    def test_other_partner_cannot_edit(self):
        order_id = self.create()
        with self.client.session_transaction() as session:
            session['partner_autohaus_id'] = -99
        response = self.post(f'/partner/kaesmann/auftrag/{order_id}', {'farbcode': 'WRONG'})
        self.assertNotEqual(response.status_code, 200)
        self.assertEqual(portal.get_auftrag(order_id)['farbcode'], 'LB9A / Z5Z5')


if __name__ == '__main__':
    unittest.main(verbosity=2)
