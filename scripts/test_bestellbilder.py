"""Original image binding, private previews and identity-only assignment, offline."""
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_materialdialog as dialogs
import test_bestellungen as orders
from test_materialkanal import png
from werkstatt_einkaufseingang import register_intake
from werkstatt_bestelluebersicht import OrderOverview
from werkstatt_bestellungen import register_orders
from werkzeug.datastructures import FileStorage


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.f = dialogs.DialogTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.s = self.f.s
        self.identity = {'product_name':'Test-Spachtel', 'article_number':'SP-2',
                         'variant':'weich', 'reason':'Aufschrift auf Originalfoto'}

    def assign(self, view, **changes):
        return self.s.assign_article(view['id'], view['revision'], dict(self.identity, **changes))

    def test_assignment_is_identity_only_and_survives_refresh(self):
        view = self.f.photo()
        before = copy.deepcopy(view['fields'])
        assigned = self.assign(view)
        self.assertEqual(assigned['fields']['manual_article']['value'], self.identity)
        self.assertNotIn('selected_article', assigned['fields'])
        self.assertEqual(assigned['review'], {})
        for key in ('quantity','unit','urgent','order_requested'):
            self.assertEqual(assigned['fields'][key], before[key])
        self.assertIn('supplier_review', assigned['missing_fields'])
        self.assertIn('price', assigned['missing_fields'])
        with self.s.db() as db:
            self.s._refresh(db, self.s._draft(db, view['id']))
        current = self.s.status(view['id'])
        self.assertEqual(current['fields']['manual_article'], assigned['fields']['manual_article'])
        self.assertEqual(current['review'], {})
        self.assertNotEqual(current['state'], 'approved')
        self.assertFalse(any(q['state']=='queued' for q in assigned['questions']))
        self.assertEqual(self.f.p.workshop_orders.calls, [])

    def test_assignment_removes_previously_approved_terms(self):
        view = self.f.review(self.f.photo())
        self.assertEqual(view['state'], 'approved')
        view = self.assign(view)
        self.assertEqual(view['review'], {})
        self.assertNotEqual(view['state'], 'approved')
        self.assertEqual(self.f.p.workshop_orders.calls, [])

    def test_unknown_code_and_variant_may_remain_empty(self):
        view = self.assign(self.f.photo(), article_number='', variant='')
        self.assertEqual(view['fields']['manual_article']['value']['article_number'], '')
        self.assertIn('price', view['missing_fields'])

    def test_stale_revision_and_employee_cannot_assign(self):
        view = self.f.photo()
        self.assign(view)
        with self.assertRaises((ValueError, LookupError)):
            self.assign(view)
        fresh = self.s.status(view['id'])
        with self.assertRaises(PermissionError):
            self.s.assign_article(fresh['id'], fresh['revision'], self.identity, actor='mitarbeiter:1')

    def test_accepted_or_cancelled_cannot_be_rewritten(self):
        view = self.f.photo()
        for state in ('accepted','cancelled','external_pending','external_sent'):
            with self.s.db() as db:
                db.execute('UPDATE einkauf_material_dialoge SET state=? WHERE id=?', (state, view['id']))
            with self.assertRaises(ValueError):
                self.assign(self.s.status(view['id']))

    def test_required_fields_and_unexpected_commercial_data_rejected(self):
        view = self.f.photo()
        for payload in (dict(self.identity, reason=''), dict(self.identity, product_name=''),
                        dict(self.identity, unit_price_cents=0), dict(self.identity, product_name='x'*301)):
            with self.assertRaises(ValueError):
                self.s.assign_article(view['id'], view['revision'], payload)

    def test_price_review_must_match_manual_identity(self):
        view = self.assign(self.f.photo())
        with self.assertRaises(ValueError):
            self.f.review(view)
        view = self.f.review(view, product_name='Test-Spachtel', article_number='SP-2', variant='weich')
        self.assertEqual(view['review']['product_name'], 'Test-Spachtel')

    def test_later_catalog_reply_cannot_reuse_terms_over_manual_identity(self):
        self.f.review(self.f.photo())
        self.f.f.time += 1200
        view = self.assign(self.f.photo(new=True))
        self.f.answer(view, 'TEST-50')
        current = self.s.status(view['id'])
        self.assertEqual(current['fields']['manual_article']['value'], self.identity)
        self.assertNotIn('selected_article', current['fields'])
        self.assertEqual(current['review'], {})
        self.assertNotEqual(current['state'], 'approved')
        self.assertEqual(self.f.p.workshop_orders.calls, [])

    def test_rejected_scan_requires_manual_assignment_before_any_approval(self):
        self.f.review(self.f.photo())
        self.f.f.time += 1200
        details = {'menge':1,'dringend':True,'vorgang':'bestellung','beschreibung':'Etikett neu prüfen',
                   'artikelkorrektur':True,'abgelehnte_codes':['TEST-50']}
        with patch('werkstatt_materialdialog.portal_request_details',return_value=details):
            view = self.f.photo(new=True)
            self.assertIn('article_correction',view['missing_fields'])
            self.assertEqual(view['review'], {})
            self.assertNotIn('selected_article',view['fields'])
            with self.assertRaises(ValueError):
                self.f.review(view)
            payload = dict(supplier_id='supplier-1',recipient='orders@example.test',product_name='Test-Klebeband',
                article_number='TEST-50',variant='grün 50 mm',subject='Test',recipient_source='Testbeleg',
                authorization_note='Interner Test',max_total_cents=10000,confirmed=True)
            with self.assertRaises(ValueError):
                self.s.reserve_external(view['id'],view['revision'],payload)
            view = self.assign(view)
            self.assertNotIn('article_correction',view['missing_fields'])
            self.assertEqual(view['review'], {})
        self.assertEqual(self.f.p.workshop_orders.calls, [])

    def test_order_guard_rejects_inconsistent_persisted_manual_identity(self):
        view = self.f.review(self.f.photo())
        fields = dict(view['fields'],manual_article={'value':self.identity})
        with self.s.db() as db:
            db.execute('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=?',(json.dumps(fields),view['id']))
        with self.assertRaises(PermissionError):
            self.s.approved_order(view['id'],view['revision'])

    def test_overview_uses_manual_name_code_and_variant(self):
        view = self.assign(self.f.photo())
        with self.s.db() as db:
            raw = dict(db.execute('SELECT * FROM einkauf_material_dialoge WHERE id=?',(view['id'],)).fetchone())
        line = OrderOverview(self.f.p.get_db)._material_line(raw, {}, {})
        self.assertEqual((line['product'],line['sku'],line['variant']), ('Test-Spachtel','SP-2','weich'))
        self.assertEqual(line['supplier'], 'Lieferant noch zuordnen')


class OriginalRouteTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.p = orders.FakePortal(Path(temp.name))
        register_orders(self.p)
        self.s = register_intake(self.p)
        self.p.workshop_intake = self.s
        self.group = self.s.create({'supplier':'Unbekannt','source_key':'synthetic-photo-1',
            'external_ref':'Testfoto','source_at':'2026-10-08T12:00:00+02:00','already_ordered':False,
            'lines':[{'product':'Artikel offen'}]})
        self.raw = png()
        self.file = self.s.attach(self.group['id'], FileStorage(stream=io.BytesIO(self.raw),filename='etikett.png'), 'materialfoto')
        self.url = f"/admin/assistent-bestellungen/eingang/{self.group['id']}/dateien/{self.file['id']}/vorschau"
        self.client = self.p.app.test_client()
        with self.client.session_transaction() as session:
            session.update(admin=True, csrf_token='test-csrf')
        self.p.material_dialog = Mock()
        self.item = dict(id=7, revision=1, message_id=1, code='M-7 R1', state='open',
            intake_id=self.group['id'], file_id=self.file['id'], source_kind='image', vorgang='bestellung',
            beschreibung='', employee_name='Test', fields={}, review={}, analysis_state='done',
            analysis={'code_erkennung':{'codes':[{'format':'QR_CODE','wert':'<b>SP-2</b>'}]}},
            questions=[], missing_fields=['article','price'], error_code='',dispatch_id='',
            internal_review_pending=False,employee_reply_required=False)
        self.p.material_dialog.status.return_value = self.item
        self.p.material_dialog.list.return_value = [self.item]
        self.p.app.add_url_rule('/material/<int:draft_id>/auslesen', 'werkstatt_materialverwaltung.reanalyze_photo', lambda draft_id: '')
        self.p.app.add_url_rule('/material/<int:draft_id>/pruefen', 'werkstatt_materialverwaltung.review', lambda draft_id: '')
        self.p.app.add_url_rule('/material/<int:draft_id>/extern', 'werkstatt_materialverwaltung.reserve_external', lambda draft_id: '')
        self.p.app.add_url_rule('/material/<int:draft_id>/nachpruefen', 'werkstatt_materialverwaltung.recheck', lambda draft_id: '')

    def test_original_opens_inline_byte_exact_with_private_headers(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, self.raw)
        self.assertEqual(response.mimetype, 'image/png')
        self.assertTrue(response.headers['Content-Disposition'].startswith('inline'))
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
        download = self.client.get(self.url.replace('/vorschau','/original'))
        self.assertTrue(download.headers['Content-Disposition'].startswith('attachment'))
        self.assertEqual(download.data, self.raw)

    def test_anonymous_employee_wrong_group_and_corrupted_original_are_denied(self):
        for identity in ({}, {'mitarbeiter_id':1}):
            with self.client.session_transaction() as session:
                session.clear(); session.update(identity)
            self.assertEqual(self.client.get(self.url).status_code, 403)
        with self.client.session_transaction() as session: session['admin']=True
        self.assertEqual(self.client.get(self.url.replace('/eingang/1/','/eingang/999/')).status_code, 404)
        db = self.p.get_db()
        db.execute("UPDATE einkauf_eingang_dateien SET sha256='invalid' WHERE id=?",(self.file['id'],))
        db.commit();db.close()
        self.assertEqual(self.client.get(self.url).status_code, 404)

    def test_material_only_url_has_photos_and_safe_code_and_assignment_form(self):
        response = self.client.get('/admin/assistent-bestellungen/eingang/ansicht?material=7')
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn(self.url, html)
        self.assertIn('Original öffnen', html)
        self.assertIn('Artikelzuordnung speichern', html)
        self.assertIn('&lt;b&gt;SP-2&lt;/b&gt;', html)
        self.assertNotIn('<b>SP-2</b>', html)

    def test_assignment_route_checks_csrf_and_does_not_process_orders(self):
        url = '/admin/assistent-bestellungen/eingang/material/7/artikelzuordnung'
        payload = dict(revision='1',product_name='Spachtel',article_number='',variant='',reason='Foto')
        self.assertEqual(self.client.post(url,data=payload).status_code, 400)
        self.p.material_dialog.assign_article.assert_not_called()
        response = self.client.post(url,data=dict(payload,csrf_token='test-csrf'))
        self.assertEqual(response.status_code, 303)
        self.p.material_dialog.assign_article.assert_called_once_with(7,1,
            dict(product_name='Spachtel',article_number='',variant='',reason='Foto'))
        self.p.material_dialog.process_next.assert_not_called()


if __name__=='__main__': unittest.main(verbosity=2)
