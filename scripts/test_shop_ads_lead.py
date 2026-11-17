"""Real AST intake and Jinja lead receipts; temporary DB and blocked network."""
import ast
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_shop_cart_inquiry as intake
from flask import render_template, session


class AdsLeadReceiptChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        intake.ActualIntakeChecks.setUpClass()

    def setUp(self):
        self.fixture = intake.ActualIntakeChecks('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.app, self.client = self.fixture.app, self.fixture.client

    def assert_receipt(self, event, kind):
        self.assertEqual(set(event), {'id', 'kind'})
        self.assertEqual(event['kind'], kind)
        self.assertRegex(event['id'], r'^lead_ads_[a-f0-9]{64}$')
        self.assertNotIn('SYNTHETIC', json.dumps(event))
        self.assertNotIn('01234567890', json.dumps(event))
        self.assertNotIn('email', json.dumps(event))

    def assert_no_pending_receipt(self):
        with self.client.session_transaction() as saved:
            self.assertNotIn('website_anfrage_ads_event', saved)

    def test_forged_success_query_without_post_never_creates_receipt_or_lead(self):
        response = self.client.get('/anfrage?gesendet=1&transaction_id=lead_ads_' + 'f' * 64 + '&value=999')
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json['submitted'])
        self.assertIsNone(response.json['ads_event'])
        self.assertEqual(self.fixture.db_calls, 0)
        self.assertEqual(self.fixture.lead_rows(), [])
        self.fixture.notices.assert_not_called()
        self.assert_no_pending_receipt()

    def test_persisted_cart_lead_creates_one_session_bound_opaque_receipt_and_refresh_does_not_repeat(self):
        response = self.client.post('/anfrage', data=self.fixture.payload())
        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(self.fixture.lead_rows()), 1)
        with self.client.session_transaction() as saved:
            event = dict(saved['website_anfrage_ads_event'])
        self.assert_receipt(event, 'shop_lead')
        success = self.client.get(response.headers['Location'])
        self.assertTrue(success.json['submitted'])
        self.assertEqual(success.json['ads_event'], event)
        self.assert_no_pending_receipt()
        for client in (self.client, self.app.test_client()):
            refreshed = client.get(response.headers['Location'])
            self.assertFalse(refreshed.json['submitted'])
            self.assertIsNone(refreshed.json['ads_event'])
        self.assertEqual(len(self.fixture.lead_rows()), 1)

    def test_ordinary_success_uses_existing_form_action_and_a_different_receipt(self):
        data = self.fixture.payload()
        data.pop('shop_anfrage'); data.pop('shop_auswahl')
        response = self.client.post('/anfrage', data=data)
        self.assertEqual(response.status_code, 302)
        success = self.client.get(response.headers['Location'])
        self.assertTrue(success.json['submitted'])
        self.assert_receipt(success.json['ads_event'], 'form')
        first = success.json['ads_event']['id']
        response = self.client.post('/anfrage', data=data)
        success = self.client.get(response.headers['Location'])
        self.assertNotEqual(success.json['ads_event']['id'], first)
        self.assertEqual(len(self.fixture.lead_rows()), 2)

    def test_csrf_honeypot_validation_and_rate_failures_create_no_measurement(self):
        invalid = self.fixture.payload(); invalid.pop('_csrf_token')
        bot = self.fixture.payload(); bot['website'] = 'SYNTHETIC BOT'
        bad = self.fixture.payload(); bad['name'] = 'X'; bad['telefon'] = ''
        for data, status in ((invalid, 400), (bot, 302), (bad, 400)):
            with self.subTest(status=status):
                response = self.client.post('/anfrage', data=data)
                self.assertEqual(response.status_code, status)
                self.assert_no_pending_receipt()
                success = self.client.get('/anfrage?gesendet=1')
                self.assertFalse(success.json['submitted'])
                self.assertIsNone(success.json['ads_event'])
        self.fixture.rate.return_value = (True, 60)
        response = self.client.post('/anfrage', data=self.fixture.payload())
        self.assertEqual(response.status_code, 429)
        self.assert_no_pending_receipt()
        self.assertEqual(self.fixture.lead_rows(), [])
        self.fixture.notices.assert_not_called()

    def test_persistence_or_notification_exception_is_not_a_success_conversion(self):
        original = self.fixture.ns['create_lead']
        for stage in ('create', 'notification'):
            with self.subTest(stage=stage):
                self.fixture.ns['create_lead'] = Mock(side_effect=RuntimeError('SYNTHETIC FAILURE')) if stage == 'create' else original
                self.fixture.notices.side_effect = RuntimeError('SYNTHETIC FAILURE') if stage == 'notification' else None
                with patch.object(self.app.logger, 'error'):
                    response = self.client.post('/anfrage', data=self.fixture.payload())
                self.assertEqual(response.status_code, 500)
                self.assert_no_pending_receipt()
                success = self.client.get('/anfrage?gesendet=1')
                self.assertFalse(success.json['submitted'])
                self.assertIsNone(success.json['ads_event'])

    def test_malformed_stale_or_extra_receipt_fields_are_not_projected(self):
        valid = {'id': 'lead_ads_' + 'a' * 64, 'kind': 'shop_lead'}
        for event in (None, 'bad', {}, {**valid, 'kind': 'purchase'}, {**valid, 'id': '1'},
                      {**valid, 'customer_email': 'PRIVATE_CANARY'}, {**valid, 'id': '<script>PRIVATE_CANARY</script>'}):
            with self.subTest(event=event):
                with self.client.session_transaction() as saved:
                    saved['website_anfrage_gesendet'] = 1
                    saved['website_anfrage_ads_event'] = event
                response = self.client.get('/anfrage?gesendet=1')
                self.assertTrue(response.json['submitted'])
                self.assertIsNone(response.json['ads_event'])
                self.assertNotIn(b'PRIVATE_CANARY', response.data)
                self.assert_no_pending_receipt()
        with self.client.session_transaction() as saved:
            saved['website_anfrage_ads_event'] = valid
        response = self.client.get('/anfrage?gesendet=1')
        self.assertFalse(response.json['submitted'])
        self.assertIsNone(response.json['ads_event'])
        self.assert_no_pending_receipt()

    def test_reused_tyre_request_does_not_issue_another_measurement_receipt(self):
        function = next(node for node in self.fixture.tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == 'bestaetige_reifenangebot_anfrage')
        function = deepcopy(function); function.decorator_list = []
        exec(compile(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])),
                     str(intake.ROOT / 'app.py'), 'exec'), self.fixture.ns)
        self.fixture.ns['get_lead'] = Mock(return_value={'ist_reifenangebot': True,
            'kunden_status_url': 'https://shop.example.test/status/SYNTHETIC'})
        self.fixture.ns['WEBSITE_ANLIEGEN']['reifenangebot'] = 'Reifenangebot'
        with self.app.test_request_context('/anfrage', method='POST'):
            self.fixture.ns['bestaetige_reifenangebot_anfrage'](1)
            event = dict(session['website_anfrage_ads_event'])
            self.assert_receipt(event, 'form')
            session.pop('website_anfrage_ads_event')
            self.fixture.ns['bestaetige_reifenangebot_anfrage'](1, ads_measurement=False)
            self.assertNotIn('website_anfrage_ads_event', session)

    def test_real_success_template_projects_only_receipt_marker_and_no_conversion_on_plain_form(self):
        self.app.jinja_env.globals['url_for'] = lambda endpoint, **kwargs: '/synthetic/' + endpoint
        self.app.jinja_env.globals['csrf_field'] = lambda: ''
        context = {'formdata': {'anliegen': 'allgemein'}, 'werkstatt_oeffentlich': {
            'name': 'SYNTHETIC WORKSHOP', 'telefon_url': 'tel:01234', 'telefon': '01234'},
            'gesendet': True, 'reifenangebot_gesendet': False, 'reifen_gesendet': False,
            'bremsen_gesendet': False, 'kurzanfrage': False, 'anliegen_optionen': {},
            'kurzanfrage_optionen': {}, 'besichtigungsarten': {}, 'ads_event': {
            'id': 'lead_ads_' + 'a' * 64, 'kind': 'shop_lead'}}
        with self.app.test_request_context('/anfrage?gesendet=1'):
            source = render_template('homepage_anfrage.html', **context)
            self.assertIn('data-shop-lead-id="lead_ads_' + 'a' * 64 + '"', source)
            self.assertIn('data-ads-event-kind="shop_lead"', source)
            self.assertNotIn('gaertnerAdsTrack(', source)
            self.assertNotIn('event_callback', source)
            context['ads_event'] = None
            self.assertNotIn('data-shop-lead-id=', render_template('homepage_anfrage.html', **context))
            context['ads_event'] = {'id': 'lead_ads_' + 'b' * 64, 'kind': 'form'}
            context['gesendet'] = False
            self.assertNotIn('data-shop-lead-id=', render_template('homepage_anfrage.html', **context))


if __name__ == '__main__':
    unittest.main()
