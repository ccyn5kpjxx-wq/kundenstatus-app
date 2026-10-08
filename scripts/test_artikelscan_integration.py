"""Combined personal scan -> originals -> admin identity tests, synthetic/offline."""
import io
import json
from pathlib import Path
import sys
import unittest

from flask import render_template
from jinja2 import FileSystemLoader

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_materialbestellung_portal as fixtures
import test_bestellungen as order_fixtures
from test_materialfoto_codes import qr_image, png
from werkstatt_bestellungen import register_orders
from werkstatt_bestelluebersicht import OrderOverview
from werkstatt_einkaufseingang import register_intake

ROOT = Path(__file__).resolve().parents[1]


class IntegratedScanTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.PortalTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.p, self.client = self.f.p, self.f.client
        self.p.admin_required = order_fixtures.FakePortal.admin_required
        self.p.get_werkstatt_smtp_config = lambda: {}
        self.p.get_werkstatt_imap_config = lambda: {}
        self.p.app.config.update(MAILBOX_SEND_ENABLED=False, ASSISTANT_ORDER_SEND_ENABLED=False,
            ASSISTANT_ORDER_WORKER_ENABLED=False,
            MAILBOX_OUTBOX_DIR=str(Path(self.f.f.f.tmp.name) / 'isolated-outbox'))
        # The existing dialog fixture has an empty dispatch stub table. Replace
        # only that disposable table with the actual admin route schema.
        self.f.sql('DROP TABLE assistent_bestellanforderungen')
        register_orders(self.p)
        register_intake(self.p)
        self.p.app.jinja_loader = FileSystemLoader(str(ROOT / 'templates'))
        self.p.app.static_folder = str(ROOT / 'static')
        self.p.app.jinja_env.globals['csrf_field'] = lambda: '<input name="csrf_token" value="synthetic-csrf">'
        for endpoint in ('reanalyze_photo', 'review', 'reserve_external', 'recheck'):
            self.p.app.add_url_rule('/fixture/' + endpoint + '/<int:draft_id>',
                'werkstatt_materialverwaltung.' + endpoint, lambda draft_id: '')
        self.f.f.hits[0]['artikelnummer'] = '10035120'
        self.raw = png(qr_image('10035120'))

    def submit(self, *, urgent=False, correction=False, raw=None):
        client_id = fixtures.uid()
        row = dict(id=client_id, menge=3, dringend=urgent)
        data = dict(request_id=fixtures.uid(), csrf_token='synthetic-csrf')
        data['foto_' + client_id] = (io.BytesIO(self.raw if raw is None else raw), 'scan.png')
        if correction:
            row.update(artikelkorrektur=True, abgelehnte_codes=['10035120'],
                       etikett_datei=True, beschreibung='Spachtel vom Etikett')
            data['etikett_' + client_id] = (io.BytesIO(fixtures.picture('red')), 'etikett.png')
        data['positionen'] = json.dumps([row])
        response = self.client.post('/werkstatt/materialbestellung/anforderungen', data=data)
        self.assertEqual(response.status_code, 200, response.text)
        item = response.json['anforderungen'][0]
        return self.f.analyze(item)

    def admin(self):
        with self.client.session_transaction() as state:
            state['admin'] = True

    def test_real_code_preview_then_quantity_and_both_urgencies_preserve_original(self):
        for urgent in (False, True):
            with self.subTest(urgent=urgent):
                preview = self.client.post('/werkstatt/materialbestellung/artikelscan-vorschau',
                    headers={'X-CSRF-Token': 'synthetic-csrf'},
                    data={'foto': (io.BytesIO(self.raw), 'scan.png')})
                self.assertEqual(preview.status_code, 200)
                self.assertEqual(preview.json['lookup_status'], 'matched')
                self.assertEqual(preview.json['product'], 'Test-Klebeband')
                self.assertEqual(self.f.sql('SELECT id FROM einkauf_material_dialoge'), [])
                view = self.submit(urgent=urgent)
                self.assertEqual(view['fields']['quantity']['value'], '3')
                self.assertEqual(view['fields']['urgent']['value'], urgent)
                source = self.f.source(view['id'])
                self.admin()
                url = f"/admin/assistent-bestellungen/eingang/{source['intake_id']}/dateien/{source['file_id']}/vorschau"
                self.assertEqual(self.client.get(url).data, self.raw)
                self.assertEqual(self.p.workshop_orders.calls, [])
                # Start the next subcase with a fresh isolated fixture.
                if not urgent:
                    self.f.sql('DELETE FROM einkauf_material_dialoge')

    def test_rejected_code_label_and_manual_assignment_are_bound_through_real_routes(self):
        view = self.submit(urgent=True, correction=True)
        source = self.f.source(view['id'])
        details = fixtures.portal_request_details(source)
        self.assertTrue(details['artikelkorrektur'])
        self.assertEqual(details['abgelehnte_codes'], ['10035120'])
        self.assertIn('article_correction', view['missing_fields'])
        self.assertNotIn('selected_article', view['fields'])
        self.assertEqual(view['review'], {})
        with self.assertRaises(ValueError):
            self.f.f.review(view)
        self.assertIsNone(self.p.material_dialog.process_next())
        self.admin()
        page = self.client.get('/admin/assistent-bestellungen/eingang/ansicht?material=' + str(view['id']))
        self.assertEqual(page.status_code, 200, page.text)
        files = self.p.workshop_intake.detail(source['intake_id'])['files']
        self.assertEqual(len(files), 2)
        for file, expected in zip(files, (self.raw, fixtures.picture('red'))):
            url = f"/admin/assistent-bestellungen/eingang/{source['intake_id']}/dateien/{file['id']}/vorschau"
            self.assertIn(url, page.text)
            opened = self.client.get(url)
            self.assertEqual(opened.status_code, 200)
            self.assertEqual(opened.data, expected)
            self.assertEqual(self.client.get(url.replace('/vorschau', '/original')).data, expected)
        assigned = self.client.post(f"/admin/assistent-bestellungen/eingang/material/{view['id']}/artikelzuordnung",
            data=dict(csrf_token='synthetic-csrf', revision=view['revision'], product_name='Geprüfter Spachtel',
                      article_number='', variant='weich', reason='Aufschrift auf Etikettfoto'))
        self.assertEqual(assigned.status_code, 303)
        current = self.p.material_dialog.status(view['id'])
        self.assertEqual(current['fields']['manual_article']['value']['product_name'], 'Geprüfter Spachtel')
        self.assertNotIn('article_correction', current['missing_fields'])
        self.assertEqual(current['review'], {})
        self.assertEqual(current['fields']['quantity']['value'], '3')
        self.assertTrue(current['fields']['urgent']['value'])
        with self.p.material_dialog.db() as db:
            raw = dict(db.execute('SELECT * FROM einkauf_material_dialoge WHERE id=?', (view['id'],)).fetchone())
        self.assertEqual(OrderOverview(self.p.get_db)._material_line(raw, {}, {})['product'], 'Geprüfter Spachtel')
        listed = self.f.get().json['anforderungen']
        self.assertEqual(next(row for row in listed if row['id'] == view['id'])['product'], 'Geprüfter Spachtel')
        self.assertEqual(self.p.workshop_orders.calls, [])

    def test_unknown_code_stays_reviewable_without_fabricated_preview_name(self):
        self.f.f.hits.clear()
        preview = self.client.post('/werkstatt/materialbestellung/artikelscan-vorschau',
            headers={'X-CSRF-Token': 'synthetic-csrf'}, data={'foto': (io.BytesIO(self.raw), 'scan.png')})
        self.assertEqual(preview.json['lookup_status'], 'no_match')
        self.assertEqual(preview.json['product'], '')
        view = self.submit()
        self.assertFalse(view['fields']['urgent']['value'])
        self.assertEqual(view['review'], {})
        self.assertEqual(self.p.workshop_orders.calls, [])

    def test_actual_template_keeps_both_personal_login_return_paths(self):
        with self.p.app.test_request_context():
            for target in ('/werkstatt/materialbestellung', '/werkstatt/mein-konto'):
                html = render_template('materialbestellung.html', who=None, can_order=False,
                                       login_next=target, csrf_token='synthetic-csrf', order_limit_cent=0)
                self.assertIn('name="next" value="' + target + '"', html)
                self.assertNotIn('<<<<<<<', html)


if __name__ == '__main__':
    unittest.main(verbosity=2)
