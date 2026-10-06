"""Private admin forms: genuine admin/CSRF, revisions and explicit gross amounts."""
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from flask import Flask, render_template

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_materialverwaltung import euro_cents, register_material_admin


class MaterialAdminTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'templates'))
        self.app.config.update(SECRET_KEY='synthetic-admin-test', TESTING=True)
        self.service = Mock()
        self.portal = SimpleNamespace(app=self.app, material_dialog=self.service)
        self.app.add_url_rule('/eingang', 'werkstatt_orders.intake_index', lambda: '')
        self.app.add_url_rule('/bestellungen', 'werkstatt_orders.index', lambda: '')
        register_material_admin(self.portal)
        self.client = self.app.test_client()
        with self.client.session_transaction() as state:
            state.update(admin=True, csrf_token='synthetic-csrf')
        self.form = dict(csrf_token='synthetic-csrf', revision='3', reviewed='ja',
            supplier_id='supplier', article_number='synthetic-50', product_name='Grünes Klebeband',
            variant='50 mm × 50 m', unit='Karton', unit_price='39,99', shipping='0', extra_costs='2.00',
            price_source='Synthetisches Angebot, heutige geprüfte Konditionen', verified_until='2026-10-31')
        self.url = '/admin/assistent-bestellungen/eingang/material/7/pruefen'

    def test_no_employee_or_anonymous_access_even_with_csrf(self):
        for identity in ({}, {'mitarbeiter_id': 7}):
            with self.client.session_transaction() as state:
                state.clear()
                state.update(identity, csrf_token='synthetic-csrf')
            self.assertEqual(self.client.post(self.url, data=self.form).status_code, 403)
        self.service.apply_admin_review.assert_not_called()

    def test_missing_or_wrong_csrf_does_not_change_review(self):
        for csrf in ('', 'wrong'):
            self.assertEqual(self.client.post(self.url, data=dict(self.form, csrf_token=csrf)).status_code, 400)
        self.service.apply_admin_review.assert_not_called()

    def test_explicit_gross_values_and_current_revision_pass_without_quantity_override(self):
        response = self.client.post(self.url, data=dict(self.form, quantity='5000', urgent='true', actor='mitarbeiter:99'))
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        args, kwargs = self.service.apply_admin_review.call_args
        self.assertEqual(args[:2], (7, 3))
        self.assertEqual(args[2]['unit_price_cents'], 3999)
        self.assertEqual(args[2]['shipping_cents'], 0)
        self.assertEqual(args[2]['extra_costs_cents'], 200)
        self.assertNotIn('quantity', args[2])
        self.assertNotIn('urgent', args[2])
        self.assertEqual(kwargs, {'actor': 'admin'})
        self.assertTrue(response.location.endswith('material=7#materialdialog'))

    def test_unknown_cost_or_missing_confirmation_or_revision_never_submits(self):
        for field, value in (('shipping', ''), ('extra_costs', ''), ('unit_price', '1.234,56'),
                             ('unit_price', '1.001'), ('unit_price', 'NaN'), ('unit_price', '-1'),
                             ('reviewed', ''), ('revision', ''), ('revision', '0')):
            response = self.client.post(self.url, data=dict(self.form, **{field: value}))
            self.assertEqual(response.status_code, 303)
        self.service.apply_admin_review.assert_not_called()

    def test_stale_review_is_shown_without_dispatch_attempt(self):
        self.service.apply_admin_review.side_effect = ValueError('Vorgang wurde geändert')
        response = self.client.post(self.url, data=self.form)
        self.assertEqual(response.status_code, 303)
        with self.client.session_transaction() as state:
            self.assertIn(('error', 'Vorgang wurde geändert'), state['_flashes'])
        self.service.process_next.assert_not_called()

    def test_photo_retry_requires_current_revision_and_does_not_claim_success_on_failure(self):
        self.service.status.return_value = dict(revision=3)
        self.service.analyze.return_value = dict(analysis_state='failed')
        url = '/admin/assistent-bestellungen/eingang/material/7/auslesen'
        self.client.post(url, data=dict(self.form, revision=2))
        self.service.analyze.assert_not_called()
        self.client.post(url, data=self.form)
        self.service.analyze.assert_called_once_with(7)
        with self.client.session_transaction() as state:
            self.assertTrue(any(category == 'error' and 'nicht abgeschlossen' in text for category, text in state['_flashes']))

    def test_user_content_escaped_and_unknown_costs_stay_empty_in_form(self):
        item = dict(id=7, message_id=5, revision=3, code='M-7 R3', state='open', review={},
                    analysis=dict(merkmale=dict(produkt='<script>bad</script>')), analysis_state='pending',
                    fields={}, questions=[], missing_fields=['quantity','price'], dispatch_id='', error_code='')
        with self.app.test_request_context():
            html = render_template('materialdialog.html', material_dialogs=[item], material_current=item,
                                   material_contacts=[], material_replies_enabled=False, csrf='synthetic')
        self.assertNotIn('<script>bad</script>', html)
        self.assertIn('&lt;script&gt;', html)
        self.assertIn('montags um 14 Uhr', html)
        self.assertIn('Rückfragen per WhatsApp sind noch nicht eingeschaltet', html)
        self.assertIn('name="shipping" inputmode="decimal" value=""', html)

    def test_recheck_is_admin_csrf_revision_bound_and_does_not_run_sender(self):
        url = '/admin/assistent-bestellungen/eingang/material/7/uebergabe-pruefen'
        self.service.recheck.return_value = dict(state='approved')
        self.assertEqual(self.client.post(url).status_code, 400)
        self.service.recheck.assert_not_called()
        response = self.client.post(url, data=self.form)
        self.assertEqual(response.status_code, 303)
        self.service.recheck.assert_called_once_with(7, 3)
        self.service.process_next.assert_not_called()
        self.service.send_question.assert_not_called()

    def test_recognized_label_is_visible_without_becoming_supplier_or_price_evidence(self):
        item = dict(id=7, revision=3, code='M-7 R3', state='review', review={},
                    analysis=dict(merkmale=dict(produkt='Test-Silber', marke='Testmarke', artikelnummer='TEST/E0.5')),
                    analysis_state='done', fields={}, questions=[], missing_fields=['supplier_review','price'],
                    dispatch_id='', error_code='', internal_review_pending=True, employee_reply_required=False)
        with self.app.test_request_context():
            html = render_template('materialdialog.html', material_dialogs=[item], material_current=item,
                                   material_contacts=[], material_replies_enabled=True, csrf='synthetic')
        self.assertIn('Interne Bestellprüfung', html)
        self.assertIn('Vom Produktetikett erkannt', html)
        self.assertIn('TEST/E0.5', html)
        self.assertIn('name="product_name" maxlength="300" value="Test-Silber"', html)
        self.assertIn('name="article_number" maxlength="300" value=""', html)
        self.assertIn('name="unit_price" inputmode="decimal" value=""', html)
        self.assertIn('noch nicht bestellt', html)

    def test_internal_review_recheck_needs_no_employee_repetition_and_does_not_send(self):
        self.service.recheck.return_value = dict(state='review', internal_review_pending=True,
                                                employee_reply_required=False)
        self.client.post('/admin/assistent-bestellungen/eingang/material/7/uebergabe-pruefen', data=self.form)
        with self.client.session_transaction() as state:
            self.assertTrue(any(category=='info' and 'keine erneute Artikelfrage' in message
                                for category,message in state['_flashes']))
        self.service.process_next.assert_not_called()
        self.service.send_question.assert_not_called()

    def test_external_reservation_uses_admin_csrf_revision_and_no_client_quantity_or_sender(self):
        url='/admin/assistent-bestellungen/eingang/material/7/extern-reservieren'
        form=dict(self.form,confirmed='ja',recipient='orders@example.test',subject='Testbestellung',
            recipient_source='Synthetischer Kontaktbeleg',authorization_note='Einmalige Freigabe',max_total='250,00',
            quantity='999',urgent='false',actor='mitarbeiter:99')
        self.assertEqual(self.client.post(url,data=dict(form,csrf_token='wrong')).status_code,400)
        for change in ({'confirmed':''},{'revision':'0'},{'max_total':''}):
            self.assertEqual(self.client.post(url,data=dict(form,**change)).status_code,303)
        self.service.reserve_external.assert_not_called()
        response=self.client.post(url,data=form)
        self.assertEqual(response.status_code,303)
        args,kwargs=self.service.reserve_external.call_args
        self.assertEqual(args[:2],(7,3))
        self.assertEqual(args[2]['max_total_cents'],25000)
        self.assertNotIn('quantity',args[2]);self.assertNotIn('urgent',args[2])
        self.assertEqual(kwargs,{'actor':'admin'})
        self.service.process_next.assert_not_called()
        self.service.send_question.assert_not_called()
        with self.client.session_transaction() as state:
            self.assertTrue(any('keine E-Mail versandt' in msg for _,msg in state['_flashes']))

    def test_external_send_proof_is_admin_only_and_never_sends(self):
        url='/admin/assistent-bestellungen/eingang/material/7/extern-versand-nachweisen'
        form=dict(csrf_token='synthetic-csrf',revision='4',confirmed='ja',reservation_id='synthetic-reservation',
            recipient='orders@example.test',subject='Testbestellung',sent_at='2026-10-06T19:00:15+02:00',
            send_evidence='Gesendet-Nachweis Test',actor='other')
        self.assertEqual(self.client.post(url,data=dict(form,csrf_token='')).status_code,400)
        self.service.record_external_sent.assert_not_called()
        response=self.client.post(url,data=form)
        self.assertEqual(response.status_code,303)
        args,kwargs=self.service.record_external_sent.call_args
        self.assertEqual(args[:2],(7,4));self.assertEqual(kwargs,{'actor':'admin'})
        self.assertEqual(args[2]['sent_at'],form['sent_at'])
        self.service.process_next.assert_not_called();self.service.send_question.assert_not_called()
        for path in ('extern-reservieren','extern-versand-nachweisen'):
            with self.client.session_transaction() as state:state.pop('admin',None)
            self.assertEqual(self.client.post('/admin/assistent-bestellungen/eingang/material/7/'+path,data=form).status_code,403)

    def test_external_reserved_template_shows_frozen_claim_and_no_automatic_review(self):
        external=dict(quantity='1',unit='Stück',product_name='<script>bad</script>',variant='Test 0,5 L',article_number='TEST',
            supplier_name='Testlieferant',recipient='orders@example.test',subject='Testbestellung',max_total_cents=25000,
            reservation_id='synthetic-reservation',reserved_at='2026-10-06T17:00:00+00:00',recipient_source='Quelle',
            authorization_note='Freigabe',body='Testtext',sent_at='2026-10-06T17:05:00+00:00',send_evidence='Gesendet-Test')
        item=dict(id=7,revision=4,code='M-7 R4',state='external_pending',review={},analysis={},analysis_state='done',
            fields={},questions=[],missing_fields=[],dispatch_id='',error_code='',external_order=external)
        for state in ('external_pending','external_sent'):
            item['state']=state
            with self.app.test_request_context():
                html=render_template('materialdialog.html',material_dialogs=[item],material_current=item,
                    material_contacts=[],material_replies_enabled=True,csrf='synthetic')
            self.assertIn('&lt;script&gt;',html);self.assertNotIn('<script>bad</script>',html)
            self.assertIn('dauerhaft für die Bestellautomatik gesperrt',html)
            self.assertNotIn('name="unit_price"',html)
            self.assertNotIn('extern-reservieren',html)
            self.assertNotIn('uebergabe-pruefen',html)
            self.assertEqual('extern-versand-nachweisen' in html,state=='external_pending')


if __name__ == '__main__':
    unittest.main()
