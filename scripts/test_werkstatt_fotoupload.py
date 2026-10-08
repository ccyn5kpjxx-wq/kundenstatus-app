"""Shared workshop photo uploads: real templates/routes, synthetic originals only."""
import hashlib
import io
from unittest import TestCase, main
from unittest.mock import patch

from PIL import Image
import test_assistent as fixture

p, database = fixture.p, fixture.database


def original_png(size=(1250, 1250), color='#273e51'):
    output = io.BytesIO()
    Image.new('RGB', size, color).save(output, 'PNG', compress_level=0)
    return output.getvalue()


class WorkshopPhotoTests(TestCase):
    def setUp(self):
        self.fixture = fixture.AssistantTests('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.client = self.fixture.client
        self.limits = patch.dict(p.app.config, MAX_CONTENT_LENGTH=25 * 1024 * 1024)
        self.limits.start(); self.addCleanup(self.limits.stop)
        with database() as db:
            db.execute('DELETE FROM datei_backups')
            db.execute("UPDATE auftraege SET status=2, auftragsnummer='AUTOHAUS-REF-555' WHERE id=156")

    def upload(self, raw, name='original.png', **kwargs):
        return self.client.post('/werkstatt/auftrag/156/fotos', data={
            'csrf_token': 'test-csrf', 'fotos': (io.BytesIO(raw), name)
        }, follow_redirects=True, **kwargs)

    def test_tafel_and_detail_expose_same_internal_number_without_replacing_customer_reference(self):
        detail = self.client.get('/werkstatt/auftrag/156')
        self.assertEqual(detail.status_code, 200)
        self.assertIn('Auftrag 156', detail.text)
        self.assertIn('data-auftrag-id="156"', detail.text)
        self.assertIn('AUTOHAUS-REF-555', detail.text)
        self.assertIn('Autohaus-Referenz', detail.text)
        self.assertIn('werkstatt_fotoupload.js', detail.text)
        tafel = self.client.get('/werkstatt/tafel')
        self.assertEqual(tafel.status_code, 200)
        self.assertIn('Auftrag 156', tafel.text)
        with database() as db:
            self.assertEqual(db.execute('SELECT auftragsnummer FROM auftraege WHERE id=156').fetchone()[0], 'AUTOHAUS-REF-555')

    def test_series_above_25_mib_saves_each_original_exactly_once_without_document_analysis(self):
        raw = original_png()
        self.assertGreater(len(raw) * 6, 25 * 1024 * 1024)
        for index in range(6):
            response = self.upload(raw, f'original-{index}.png')
            self.assertEqual(response.status_code, 200)
            self.assertIn('data-foto-upload-gespeichert="1"', response.text)
            self.assertTrue(response.request.path.endswith('/werkstatt/auftrag/156'))
        with database() as db:
            rows = db.execute('SELECT * FROM dateien WHERE auftrag_id=156 ORDER BY id').fetchall()
            self.assertEqual(len(rows), 6)
            for row in rows:
                self.assertEqual(row['kategorie'], 'fertigbild')
                self.assertEqual(row['quelle'], 'werkstatt')
                self.assertEqual(row['size'], len(raw))
                self.assertEqual(p.upload_file_path(dict(row)).read_bytes(), raw)
                backup = db.execute('SELECT * FROM datei_backups WHERE datei_id=?', (row['id'],)).fetchone()
                self.assertIsNotNone(backup)
                self.assertEqual(backup['file_sha256'], hashlib.sha256(raw).hexdigest())
            order = db.execute('SELECT * FROM auftraege WHERE id=156').fetchone()
            self.assertEqual(order['fahrzeug'], 'Testfahrzeug')
            self.assertEqual(order['kennzeichen'], 'TEST-1')
            self.assertEqual(order['auftragsnummer'], 'AUTOHAUS-REF-555')

    def test_failed_upload_has_warning_and_no_success_marker(self):
        response = self.upload(b'not a photo', 'wrong.txt')
        self.assertEqual(response.status_code, 200)
        self.assertIn('t-warning', response.text)
        self.assertNotIn('data-foto-upload-gespeichert="1"', response.text)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM dateien WHERE auftrag_id=156').fetchone()[0], 0)

    def test_shared_request_limit_csrf_and_archived_order_still_reject(self):
        response = self.upload(b'x' * (26 * 1024 * 1024))
        self.assertEqual(response.status_code, 413)
        self.assertEqual(self.client.post('/werkstatt/auftrag/156/fotos', data={
            'fotos': (io.BytesIO(original_png((10, 10))), 'photo.png')}).status_code, 400)
        with database() as db: db.execute('UPDATE auftraege SET archiviert=1 WHERE id=156')
        self.assertEqual(self.upload(original_png((10, 10))).status_code, 404)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM dateien WHERE auftrag_id=156').fetchone()[0], 0)


if __name__ == '__main__':
    main()
