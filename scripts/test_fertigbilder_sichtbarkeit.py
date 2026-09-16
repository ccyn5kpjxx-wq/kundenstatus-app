"""Real upload/list/download regressions, using only a synthetic temporary DB."""
from io import BytesIO
from unittest.mock import patch
import unittest

from test_cockpit_safety import portal, db_execute, add_file
from cockpit_rules import document_visible


class FinishPhotoVisibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING'] = True
        portal.init_db()
        portal.schedule_change_backup = lambda *args: None
        portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, 'PHOTO-TEST')

    def setUp(self):
        self.partner = portal.get_autohaus_by_slug('kaesmann')
        self.order_id = portal.create_auftrag('autohaus', autohaus_id=self.partner['id'],
            kunde_name='Foto Test', fahrzeug='Testfahrzeug', kennzeichen='TEST-F 1',
            beschreibung='Bestätigter Umfang')
        self.order = portal.get_auftrag(self.order_id)
        self.worker = portal.app.test_client()
        self.customer = portal.app.test_client()
        self.dealer = portal.app.test_client()
        with self.worker.session_transaction() as session:
            session['werkstatt_tafel'] = portal.werkstatt_tafel_session_token()
            session['csrf_token'] = 'photo-test'
        with self.dealer.session_transaction() as session:
            session['partner_autohaus_id'] = self.partner['id']

    def upload(self, filename='finish.png'):
        return self.worker.post(f'/werkstatt/auftrag/{self.order_id}/fotos', data={
            'csrf_token': 'photo-test', 'fotos': (BytesIO(b'\x89PNG\r\n\x1a\nphoto'), filename)},
            content_type='multipart/form-data')

    def assert_visible_in_both_portals(self, item):
        response = self.dealer.get(f'/partner/kaesmann/auftrag/{self.order_id}')
        self.assertEqual(response.status_code, 200)
        self.assertTrue('1 Fertigbild(er)' in response.get_data(as_text=True), 'Partner order photo count')
        response = self.dealer.get(f'/partner/kaesmann/auftrag/{self.order_id}/dokumente')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(item['original_name'] in response.get_data(as_text=True), 'Partner document list')
        token = self.order['kunden_status_token']
        response = self.customer.get(f'/status/{token}')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(item['original_name'] in response.get_data(as_text=True), 'Customer gallery')
        for client, url in ((self.dealer, f'/partner/kaesmann/datei/{item["id"]}'),
                            (self.dealer, f'/partner/kaesmann/datei/{item["id"]}/download'),
                            (self.customer, f'/status/{token}/bild/{item["id"]}')):
            with client.get(url) as result:
                self.assertEqual(result.status_code, 200)

    def test_employee_upload_reaches_customer_and_own_dealer_without_ocr(self):
        with patch.object(portal, 'build_document_analysis_bundle_safe', side_effect=AssertionError('Photo OCR')):
            self.assertEqual(self.upload().status_code, 302)
        item = portal.list_dateien(self.order_id)[0]
        self.assertEqual((item['kunde_sichtbar'], item['partner_sichtbar'], item['versicherung_sichtbar'],
                          item['sichtbarkeit_geprueft']), (1, 1, 0, 1))
        self.assert_visible_in_both_portals(item)
        self.assertEqual(portal.get_auftrag(self.order_id)['kennzeichen'], 'TEST-F 1')
        self.assertFalse(document_visible(item, 'versicherung'))

    def test_existing_workshop_completion_photo_is_visible(self):
        file_id = add_file(self.order_id, source='werkstatt', category='fertigbild', name='existing.png')
        self.assertFalse(document_visible(portal.get_datei(file_id), 'partner'))
        db_execute("UPDATE dateien SET kunde_sichtbar=1,mime_type='image/png' WHERE id=?", (file_id,))
        self.assert_visible_in_both_portals(portal.get_datei(file_id))

    def test_revoked_completion_photo_stays_hidden(self):
        self.upload()
        item = portal.list_dateien(self.order_id)[0]
        # Explicitly deny the partner while the customer flag is still true:
        # a reviewed denial must win over the legacy completion-photo fallback.
        db_execute('UPDATE dateien SET partner_sichtbar=0,sichtbarkeit_geprueft=1 WHERE id=?', (item['id'],))
        self.assertEqual(self.dealer.get(f'/partner/kaesmann/datei/{item["id"]}').status_code, 404)
        with self.customer.get(f'/status/{self.order["kunden_status_token"]}/bild/{item["id"]}') as result:
            self.assertEqual(result.status_code, 200)
        db_execute('UPDATE dateien SET kunde_sichtbar=0,partner_sichtbar=0,sichtbarkeit_geprueft=1 WHERE id=?', (item['id'],))
        self.assertEqual(self.dealer.get(f'/partner/kaesmann/datei/{item["id"]}').status_code, 404)
        self.assertEqual(self.customer.get(f'/status/{self.order["kunden_status_token"]}/bild/{item["id"]}').status_code, 404)
        self.assertNotIn('finish.png', self.dealer.get(f'/partner/kaesmann/auftrag/{self.order_id}').get_data(as_text=True))

    def test_internal_documents_and_non_photo_files_remain_private(self):
        for source, category, name, mime in (
            ('intern', 'fertigbild', 'admin.png', 'image/png'),
            ('werkstatt', 'standard', 'receipt.png', 'image/png'),
            ('werkstatt', 'rechnung', 'invoice.png', 'image/png'),
            ('werkstatt', 'fertigbild', 'invoice.pdf', 'application/pdf'),
        ):
            with self.subTest(source=source, category=category, name=name):
                file_id = add_file(self.order_id, source=source, category=category, name=name)
                db_execute('UPDATE dateien SET kunde_sichtbar=1,mime_type=? WHERE id=?', (mime, file_id))
                self.assertFalse(document_visible(portal.get_datei(file_id), 'partner'))
                self.assertEqual(self.dealer.get(f'/partner/kaesmann/datei/{file_id}').status_code, 404)
        before = len(portal.list_dateien(self.order_id))
        self.upload('invoice.pdf')
        self.assertEqual(len(portal.list_dateien(self.order_id)), before)

    def test_upload_does_not_release_an_interleaved_internal_document(self):
        real_save = portal.save_uploads
        inserted = []
        def save_with_concurrent_document(*args, **kwargs):
            inserted.append(add_file(self.order_id, name='PRIVATE.pdf'))
            return real_save(*args, **kwargs)
        with patch.object(portal, 'save_uploads', side_effect=save_with_concurrent_document):
            self.upload()
        item = portal.get_datei(inserted[0])
        self.assertFalse(document_visible(item, 'kunde'))
        self.assertFalse(document_visible(item, 'partner'))

    def test_photo_does_not_cross_order_or_partner_boundary(self):
        self.upload()
        item = portal.list_dateien(self.order_id)[0]
        other = portal.create_auftrag('intern', fahrzeug='Anderer Auftrag')
        other_order = portal.get_auftrag(other)
        self.assertEqual(self.customer.get(f'/status/{other_order["kunden_status_token"]}/bild/{item["id"]}').status_code, 404)
        db_execute('UPDATE auftraege SET autohaus_id=NULL WHERE id=?', (self.order_id,))
        self.assertEqual(self.dealer.get(f'/partner/kaesmann/datei/{item["id"]}').status_code, 404)


if __name__ == '__main__':
    unittest.main(verbosity=2)
