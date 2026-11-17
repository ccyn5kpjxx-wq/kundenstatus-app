"""Existing appointment intake link; no submissions, network or real data."""
import ast
from pathlib import Path
import socket
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from flask import Flask, request, session
from shop_service_requests import SERVICE_REQUEST_PATH, prepare_service_request_links


class ServiceRequestLinkChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.app = Flask(__name__, root_path=str(ROOT))
        self.app.secret_key = 'SYNTHETIC-TEST-ONLY'
        self.handler = Mock(return_value=('SYNTHETIC FORM', 200))
        self.app.add_url_rule('/anfrage', 'website_anfrage', lambda: self.handler(), methods=['GET', 'POST'])
        self.portal = SimpleNamespace(app=self.app, PUBLIC_SITE_ONLY=False,
            get_db=Mock(side_effect=AssertionError('No database from link adapter')),
            lead_notice=Mock(side_effect=AssertionError('No email from link adapter')))

    def test_default_and_nonliteral_activation_do_nothing(self):
        for value in (False, None, '1', 'true', 1):
            with self.subTest(value=value):
                self.assertEqual(prepare_service_request_links(self.portal, enabled=value), {})
        self.handler.assert_not_called()
        self.portal.get_db.assert_not_called()
        self.portal.lead_notice.assert_not_called()

    def test_literal_activation_returns_fixed_same_origin_path_without_io(self):
        result = prepare_service_request_links(self.portal, enabled=True)
        self.assertEqual(result, {'service_request': '/anfrage?anliegen=reifenservice',
                                  'cart_request': '/anfrage?anliegen=allgemein&shop_anfrage=1'})
        result['service_request'] = '/admin'
        self.assertEqual(prepare_service_request_links(self.portal, enabled=True)['service_request'], SERVICE_REQUEST_PATH)
        self.handler.assert_not_called()
        self.portal.get_db.assert_not_called()
        self.portal.lead_notice.assert_not_called()

    def test_stateless_or_unknown_host_mode_does_not_advertise_blocked_intake(self):
        for mode in (True, None, 'false', 0):
            with self.subTest(mode=mode):
                self.portal.PUBLIC_SITE_ONLY = mode
                self.assertEqual(prepare_service_request_links(self.portal, enabled=True), {})

    def test_missing_route_post_method_handler_or_wrong_path_fails_closed(self):
        for path, methods, endpoint in (('/anfrage', ['GET'], 'website_anfrage'),
                                        ('/other', ['GET', 'POST'], 'website_anfrage'),
                                        ('/anfrage', ['GET', 'POST'], 'different')):
            with self.subTest(path=path, methods=methods, endpoint=endpoint):
                app = Flask(__name__, root_path=str(ROOT))
                app.add_url_rule(path, endpoint, lambda: '', methods=methods)
                self.assertEqual(prepare_service_request_links(SimpleNamespace(app=app, PUBLIC_SITE_ONLY=False), enabled=True), {})
        self.app.view_functions.pop('website_anfrage')
        self.assertEqual(prepare_service_request_links(self.portal, enabled=True), {})

    def test_link_opens_existing_handler_get_only_and_does_not_submit(self):
        link = prepare_service_request_links(self.portal, enabled=True)['service_request']
        response = self.app.test_client().get(link)
        self.assertEqual(response.status_code, 200)
        self.handler.assert_called_once_with()
        self.portal.get_db.assert_not_called()
        self.portal.lead_notice.assert_not_called()

    def test_actual_portal_get_preselects_tyre_service_without_lead_or_notification(self):
        # Compile only the real handler body; importing app would start unrelated
        # portal workers. The actual GET branch is exercised with guarded sinks.
        tree = ast.parse((ROOT / 'app.py').read_text(encoding='utf-8-sig'))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'website_anfrage')
        function.decorator_list = []
        denied = Mock(side_effect=AssertionError('GET must not reserve, save or notify'))
        rendered = Mock(return_value=('SYNTHETIC FORM', 200))
        namespace = {'request': request, 'session': session,
            'clean_text': lambda value: str(value or '').strip(),
            'WEBSITE_ANLIEGEN': {'reifenservice': 'Reifenservice', 'allgemein': 'Allgemein'},
            'WEBSITE_BESICHTIGUNGSARTEN': {'werkstatt': 'Werkstatt'},
            'WEBSITE_MECHANIK_KATEGORIEN': {}, 'HOMEPAGE_LEISTUNGEN': {},
            'render_website_anfrage': rendered, 'create_lead': denied,
            'reserviere_reifen_slot': denied, 'lead_notice': denied, 'get_db': denied,
            'notify_lead_workshop': denied, 'sende_website_lead_benachrichtigung': denied}
        module = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
        exec(compile(module, str(ROOT / 'app.py'), 'exec'), namespace)
        with self.app.test_request_context(SERVICE_REQUEST_PATH, method='GET'):
            response = namespace['website_anfrage']()
        self.assertEqual(response, ('SYNTHETIC FORM', 200))
        self.assertEqual(rendered.call_args.kwargs['formdata']['anliegen'], 'reifenservice')
        self.assertFalse(rendered.call_args.kwargs['gesendet'])
        denied.assert_not_called()


if __name__ == '__main__':
    unittest.main()
