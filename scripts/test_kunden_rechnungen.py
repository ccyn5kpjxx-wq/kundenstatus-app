"""Real Flask routes, temporary synthetic SQLite/uploads, all outbound I/O denied."""
from io import BytesIO
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent))
os.environ.update({
    'GOOGLE_ADS_AUTO_SYNC_ENABLED':'0', 'ASSISTANT_ORDER_WORKER_ENABLED':'0',
    'ASSISTANT_ORDER_SEND_ENABLED':'0', 'PURCHASE_MONITOR_WORKER_ENABLED':'0',
    'MATERIAL_WHATSAPP_WORKER_ENABLED':'0', 'MATERIAL_WHATSAPP_ENABLED':'0',
})
import smoke_test as isolated

p = isolated.portal
from kunden_rechnungen import register_customer_invoices

register_customer_invoices(p)


def sql(statement, args=()):
    db = p.get_db()
    try:
        cur = db.execute(statement, args)
        result = cur.lastrowid
        db.commit()
        return result
    finally:
        db.close()


def initialize():
    p.app.config.update(TESTING=True, SERVICE_APP_ENABLED=False)
    p.init_db()
    p.schedule_change_backup = lambda reason: None
    p.lexware_request = isolated.deny_network
    p.post_whatsapp_payload = isolated.deny_network
    p.send_lead_email = isolated.deny_network
    p.notify_lead_workshop = isolated.deny_network


def partner(slug):
    return sql("""INSERT INTO autohaeuser
        (name,slug,portal_key,zugangscode,portal_titel,erstellt_am)
        VALUES (?,?,?,?,?,?)""", (f'Demo Partner {slug}', slug, uuid4().hex,
        'synthetic-not-a-live-password', f'Demo Partner {slug}', p.now_str()))


def order(partner_id, vehicle='Demo Fahrzeug', archived=False, token=None):
    token = token or uuid4().hex
    order_id = p.create_auftrag('intern', autohaus_id=partner_id,
        kunde_name='Musterkunde', kunde_email='same-address@example.test',
        fahrzeug=vehicle, kennzeichen='DEMO-A 123', kunden_status_token=token,
        beschreibung='Synthetischer Auftrag für die Rechnungsansicht')
    sql('UPDATE auftraege SET archiviert=?,status=5 WHERE id=?', (int(archived), order_id))
    return order_id, token


def pdf_bytes():
    # A valid, entirely synthetic one-page original for real browser PDF preview.
    from reportlab.pdfgen import canvas
    result = BytesIO()
    page = canvas.Canvas(result, pagesize=(595, 842))
    page.setFont('Helvetica-Bold', 22)
    page.drawString(55, 755, 'Demo-Rechnung')
    page.setFont('Helvetica', 12)
    page.drawString(55, 715, 'Synthetische Vorschau - keine echte Rechnung')
    page.drawString(55, 680, 'Diese Original-PDF kann geoeffnet und gespeichert werden.')
    page.showPage()
    page.save()
    return result.getvalue()


def document(order_id, name='Demo-Rechnung.pdf', *, released=True, customer=True,
             partner_visible=True, source='intern', category='rechnung', mime='application/pdf'):
    payload = pdf_bytes()
    filename = uuid4().hex + '.pdf'
    p.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    (p.UPLOAD_DIR / filename).write_bytes(payload)
    document_id = sql("""INSERT INTO dateien
        (auftrag_id,original_name,stored_name,mime_type,size,quelle,kategorie,
        hochgeladen_am,sichtbarkeit_geprueft,kunde_sichtbar,partner_sichtbar)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (order_id, name, filename, mime,
        len(payload), source, category, '2026-10-26 12:00:00', int(released),
        int(customer), int(partner_visible)))
    return document_id, payload


def login(client, partner_id):
    with client.session_transaction() as session:
        session['partner_autohaus_id'] = partner_id


class CustomerInvoiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        initialize()

    def setUp(self):
        self.slug = 'demo-' + uuid4().hex[:8]
        self.other_slug = 'other-' + uuid4().hex[:8]
        self.partner = partner(self.slug)
        self.other_partner = partner(self.other_slug)
        self.order, self.token = order(self.partner)
        self.archived, _ = order(self.partner, 'Archivfahrzeug', archived=True)
        self.foreign_order, self.foreign_token = order(self.other_partner, 'Fremdes Fahrzeug')
        self.client = p.app.test_client()
        self.anonymous = p.app.test_client()
        login(self.client, self.partner)
        self.index = f'/partner/{self.slug}/rechnungen'

    def file_url(self, doc, download=False):
        return f'{self.index}/{doc}' + ('/download' if download else '')

    def status_url(self, doc, token=None, download=False):
        return f'/status/{token or self.token}/rechnungen/{doc}' + ('/download' if download else '')

    def test_partner_auth_and_cross_partner_guards(self):
        own, _ = document(self.order)
        foreign, _ = document(self.foreign_order, 'Foreign-private.pdf')
        for url in (self.index, self.file_url(own), self.file_url(own, True)):
            self.assertEqual(self.anonymous.get(url).status_code, 302)
        self.assertEqual(self.client.get(f'/partner/{self.other_slug}/rechnungen').status_code, 302)
        for download in (False, True):
            self.assertEqual(self.client.get(self.file_url(foreign, download)).status_code, 404)
        html = self.client.get(self.index).get_data(as_text=True)
        self.assertNotIn('Foreign-private', html)
        self.assertIn('Demo-Rechnung.pdf', html)

    def test_only_explicitly_released_workshop_pdfs_and_archived_orders(self):
        document(self.order, 'Current-original.pdf')
        document(self.archived, 'Archived-original.pdf')
        excluded = []
        for name, options in (
            ('Hidden.pdf', {'partner_visible':False}),
            ('Legacy.pdf', {'released':False}),
            ('Customer.pdf', {'source':'kunde'}),
            ('Partner-upload.pdf', {'source':'autohaus'}),
            ('Supplier.pdf', {'source':'lieferant'}),
            ('Bonus.pdf', {'category':'bonusrechnung'}),
            ('Cost-estimate.pdf', {'category':'standard'}),
            ('Image.png', {}),
            ('Not-a-pdf.pdf', {'mime':'text/html'}),
        ):
            doc, _ = document(self.order, name, **options)
            excluded.append((name, doc))
        html = self.client.get(self.index).get_data(as_text=True)
        for name in ('Current-original.pdf', 'Archived-original.pdf', 'Archiviert', 'Archivfahrzeug'):
            self.assertIn(name, html)
        for name, doc in excluded:
            self.assertNotIn(name, html)
            self.assertEqual(self.client.get(self.file_url(doc)).status_code, 404)
            self.assertEqual(self.client.get(self.file_url(doc, True)).status_code, 404)

    def test_original_bytes_inline_and_download_headers(self):
        doc, payload = document(self.order, 'Rechnung mit Umlaut-ä.pdf')
        for client, url, attachment in (
            (self.client, self.file_url(doc), False),
            (self.client, self.file_url(doc, True), True),
            (self.anonymous, self.status_url(doc), False),
            (self.anonymous, self.status_url(doc, download=True), True),
        ):
            with client.get(url) as response:
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.data, payload)
                self.assertEqual(response.mimetype, 'application/pdf')
                self.assertTrue(response.headers['Content-Disposition'].startswith('attachment' if attachment else 'inline'))
                self.assertIn('no-store', response.headers['Cache-Control'])
                self.assertEqual(response.headers['Referrer-Policy'], 'no-referrer')
                self.assertIn('noindex', response.headers['X-Robots-Tag'])
                self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
                if not attachment:
                    self.assertIn('sandbox', response.headers['Content-Security-Policy'])
        with self.client.get(self.file_url(doc), headers={'Range':'bytes=0-31'}) as response:
            self.assertEqual(response.status_code, 206)
            self.assertEqual(response.data, payload[:32])
            self.assertTrue(response.headers['Content-Disposition'].startswith('inline'))
            self.assertIn('sandbox', response.headers['Content-Security-Policy'])

    def test_renamed_html_and_missing_original_fail_closed(self):
        doc, _ = document(self.order)
        db = p.get_db()
        stored = db.execute('SELECT stored_name FROM dateien WHERE id=?', (doc,)).fetchone()['stored_name']
        db.close()
        (p.UPLOAD_DIR / stored).write_bytes(b'<html><script>alert(1)</script></html>')
        self.assertEqual(self.client.get(self.file_url(doc)).status_code, 404)
        self.assertEqual(self.client.get(self.file_url(doc, True)).status_code, 404)
        (p.UPLOAD_DIR / stored).unlink()
        self.assertEqual(self.anonymous.get(self.status_url(doc)).status_code, 404)

    def test_revocation_and_partner_reassignment_are_immediate(self):
        doc, _ = document(self.order)
        sql('UPDATE dateien SET kunde_sichtbar=0,partner_sichtbar=0 WHERE id=?', (doc,))
        for url in (self.file_url(doc), self.file_url(doc, True), self.status_url(doc), self.status_url(doc, download=True)):
            self.assertEqual(self.client.get(url).status_code, 404)
        sql('UPDATE dateien SET kunde_sichtbar=1,partner_sichtbar=1 WHERE id=?', (doc,))
        sql('UPDATE auftraege SET autohaus_id=? WHERE id=?', (self.other_partner, self.order))
        self.assertEqual(self.client.get(self.file_url(doc)).status_code, 404)
        self.assertNotIn('Demo-Rechnung.pdf', self.client.get(self.index).get_data(as_text=True))

    def test_status_token_never_aggregates_by_name_or_email(self):
        own, _ = document(self.order)
        foreign, _ = document(self.foreign_order)
        same_partner, same_token = order(self.partner)
        same_doc, _ = document(same_partner)
        for doc in (foreign, same_doc):
            for download in (False, True):
                self.assertEqual(self.anonymous.get(self.status_url(doc, download=download)).status_code, 404)
        self.assertEqual(self.anonymous.get(self.status_url(own, token=self.foreign_token)).status_code, 404)
        sql('UPDATE auftraege SET kunden_status_aktiv=0 WHERE id=?', (self.order,))
        for download in (False, True):
            self.assertEqual(self.anonymous.get(self.status_url(own, download=download)).status_code, 404)
        self.assertEqual(self.anonymous.get(self.status_url(own, token='missing-token')).status_code, 404)

    def test_duplicate_legacy_token_cannot_expand_status_scope(self):
        own, _ = document(self.order)
        sql('UPDATE auftraege SET kunden_status_token=? WHERE id=?', (self.token, self.foreign_order))
        foreign, _ = document(self.foreign_order)
        resolved = p.get_auftrag_by_kunden_status_token(self.token)['id']
        unexpected = foreign if resolved == self.order else own
        self.assertEqual(self.anonymous.get(self.status_url(unexpected)).status_code, 404)

    def test_existing_service_guard_blocks_financial_bearer_routes(self):
        doc, _ = document(self.order)
        with patch.object(p.service_process, 'case_for_order', return_value=999999):
            for url in (self.status_url(doc), self.status_url(doc, download=True)):
                self.assertEqual(self.anonymous.get(url).status_code, 403)

    def test_real_driver_assignment_cannot_open_released_invoice(self):
        from test_service_app import ServiceAppTests
        service = ServiceAppTests('test_old_status_links_cannot_bypass_service_access')
        service.setUp()
        try:
            lead_id, case_id = service.prepared()
            self.assertEqual(service.approve(case_id).status_code, 303)
            service_order = p.get_auftrag(p.get_lead(lead_id)['auftrag_id'])
            doc, _ = document(service_order['id'])
            for download in (False, True):
                url = self.status_url(doc, token=service_order['kunden_status_token'], download=download)
                self.assertEqual(self.anonymous.get(url).status_code, 403)
                response = service.driver.get(url)
                self.assertEqual(response.status_code, 303)
                self.assertIn('/service/fahrzeug/', response.location)
                self.assertNotEqual(response.mimetype, 'application/pdf')
            service.fleet.change_vehicle(service.company, service.car, 'fahrer', {
                'name':'Neue Person', 'email':'other-person@example.test',
                'telefon':'', 'kanal':'keine', 'einwilligung':''})
            self.assertEqual(service.driver.get(url).status_code, 403)
        finally:
            service.doCleanups()

    def test_read_only_listing_and_download_do_not_analyse_or_change_data(self):
        doc, _ = document(self.order)
        db = p.get_db()
        before = dict(db.execute('SELECT * FROM auftraege WHERE id=?', (self.order,)).fetchone())
        db.close()
        with patch.object(p, 'get_auftrag', side_effect=AssertionError('Invoice read hydrated an order')), \
             patch.object(p, 'build_document_analysis_bundle_safe', side_effect=AssertionError('Invoice read ran OCR')):
            self.assertEqual(self.client.get(self.index).status_code, 200)
            with self.client.get(self.file_url(doc)) as response:
                self.assertEqual(response.status_code, 200)
        db = p.get_db()
        after = dict(db.execute('SELECT * FROM auftraege WHERE id=?', (self.order,)).fetchone())
        db.close()
        self.assertEqual(before, after)

    def test_status_navigation_panel_and_no_js_links(self):
        doc, _ = document(self.order, 'Customer-original.pdf')
        document(self.foreign_order, 'Other-original.pdf')
        response = self.anonymous.get(f'/status/{self.token}')
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('href="#rechnungen" data-section="rechnungen"', html)
        self.assertIn('id="rechnungen" data-portal-section="rechnungen"', html)
        self.assertIn(self.status_url(doc, download=True), html)
        self.assertIn('Customer-original.pdf', html)
        self.assertNotIn('Other-original.pdf', html)
        dashboard = self.client.get(f'/partner/{self.slug}/dashboard')
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn(self.index, dashboard.get_data(as_text=True))

    def test_empty_list_pagination_and_untrusted_names(self):
        self.assertIn('Noch keine Rechnung', self.client.get(self.index).get_data(as_text=True))
        document(self.order, '<img src=x onerror=alert(1)>.pdf')
        for index in range(50):
            document(self.order, f'Batch-{index:02}.pdf')
        for page in ('1', '2', 'abc', '-1', '999999'):
            response = self.client.get(self.index+'?seite='+page)
            self.assertEqual(response.status_code, 200)
            self.assertLessEqual(response.get_data(as_text=True).count('class="invoice-card"'), 50)
        html = self.client.get(self.index+'?seite=2').get_data(as_text=True)
        self.assertIn('&lt;img', html)
        self.assertNotIn('<img src=x', html)
        self.assertIn('Seite 2 von 2', html)


if __name__ == '__main__':
    unittest.main(verbosity=2)
