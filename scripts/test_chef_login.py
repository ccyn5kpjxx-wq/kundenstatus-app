"""Actual admin login functions extracted by AST, no app import, DB or network."""
import ast
from functools import wraps
from html.parser import HTMLParser
from pathlib import Path
import re
import secrets
import sys
import unittest
from unittest.mock import Mock

from flask import Flask, abort, flash, redirect, render_template, request, session, url_for
import hmac

ROOT = Path(__file__).resolve().parents[1]


class TokenParser(HTMLParser):
    token = None

    def handle_starttag(self, tag, attributes):
        values = dict(attributes)
        if tag == 'input' and values.get('name') == 'csrf_token':
            self.token = values['value']


class ChefLoginTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask('isolated-chef-login', template_folder=str(ROOT / 'templates'))
        self.app.config.update(TESTING=True, SECRET_KEY='synthetic-chef-login-test-only')
        names = {'admin_login_destination', 'admin_required', 'login', 'protect_csrf',
                 'get_csrf_token', 'csrf_field', 'add_csrf_fields'}
        nodes = []
        for node in ast.parse((ROOT / 'app.py').read_text(encoding='utf-8')).body:
            if isinstance(node, ast.FunctionDef) and node.name in names:
                node.decorator_list = []
                nodes.append(node)
        self.assertEqual(len(nodes), len(names))
        self.ns = dict(__name__=__name__, app=self.app, request=request, session=session,
                       redirect=redirect, url_for=url_for, flash=flash, render_template=render_template,
                       wraps=wraps, abort=abort, hmac=hmac, re=re, secrets=secrets, sys=sys,
                       CSRF_FIELD_NAME='csrf_token', progress_csrf_exempt=lambda _: False,
                       csrf_recovery_response=lambda: None,
                       login_rate_limit_status=Mock(return_value=(False, 0)),
                       login_wait_label=lambda _: 'one minute',
                       admin_password_matches=Mock(side_effect=lambda value: value == 'synthetic-valid-password'),
                       clear_login_attempts=Mock(), record_failed_login=Mock(), remember_authenticated_login=Mock())
        exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), 'isolated-actual-admin-login', 'exec'), self.ns)
        self.app.before_request(self.ns['protect_csrf'])
        self.app.after_request(self.ns['add_csrf_fields'])
        self.app.add_url_rule('/login', endpoint='login', view_func=self.ns['login'], methods=['GET', 'POST'])
        self.app.add_url_rule('/admin/mitarbeiter', endpoint='admin_mitarbeiter',
                              view_func=self.ns['admin_required'](lambda: 'chef-start'))
        self.app.add_url_rule('/admin/cockpit', endpoint='betriebs_cockpit',
                              view_func=self.ns['admin_required'](lambda: 'ordinary-cockpit'))
        self.app.add_url_rule('/admin/other', endpoint='other_admin',
                              view_func=self.ns['admin_required'](lambda: 'other-admin'))
        self.client = self.app.test_client()

    def form(self, url='/login'):
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        parser = TokenParser()
        parser.feed(response.get_data(as_text=True))
        self.assertTrue(parser.token)
        return dict(username='admin', password='synthetic-valid-password', csrf_token=parser.token)

    def test_direct_chef_entry_returns_there_after_one_normal_admin_login(self):
        response = self.client.get('/admin/mitarbeiter')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'], '/login?return_to=chef')
        url = response.headers['Location']
        form = self.form(url)
        response = self.client.post(url, data=form)
        self.assertEqual(response.headers['Location'], '/admin/mitarbeiter')
        self.assertEqual(self.client.get(response.headers['Location']).get_data(as_text=True), 'chef-start')
        self.ns['admin_password_matches'].assert_called_once_with('synthetic-valid-password')
        self.ns['remember_authenticated_login'].assert_called_once()
        with self.client.session_transaction() as state:
            self.assertTrue(state['admin'])
            self.assertNotIn('assistent_mid', state)

    def test_other_admin_entries_keep_existing_default_destination(self):
        self.assertEqual(self.client.get('/admin/other').headers['Location'], '/login')
        response = self.client.post('/login', data=self.form())
        self.assertEqual(response.headers['Location'], '/admin/cockpit')

    def test_no_open_redirect_or_ambiguous_return_marker(self):
        for query in ('return_to=https://external.invalid', 'return_to=//external.invalid',
                      'return_to=/admin/mitarbeiter', 'next=https://external.invalid',
                      'return_to=chef&return_to=https://external.invalid', 'return_to=chef%20',
                      'return_to=chef&return_to=chef'):
            self.client = self.app.test_client()
            url = '/login?' + query
            response = self.client.post(url, data=self.form(url))
            self.assertEqual(response.headers['Location'], '/admin/cockpit')

    def test_wrong_password_and_rate_limit_preserve_normal_auth(self):
        url = '/login?return_to=chef'
        form = self.form(url)
        self.assertEqual(self.client.post(url, data=dict(form, password='wrong')).status_code, 200)
        self.ns['record_failed_login'].assert_called_once_with('admin', 'admin')
        with self.client.session_transaction() as state:
            self.assertFalse(state.get('admin'))
        self.ns['login_rate_limit_status'].return_value = (True, 60)
        self.assertEqual(self.client.post(url, data=form).status_code, 429)
        self.ns['login_rate_limit_status'].return_value = (False, 0)
        self.assertEqual(self.client.post(url, data=form).headers['Location'], '/admin/mitarbeiter')

    def test_already_authenticated_admin_uses_only_fixed_destination(self):
        with self.client.session_transaction() as state:
            state['admin'] = True
        self.assertEqual(self.client.get('/login?return_to=chef').headers['Location'], '/admin/mitarbeiter')
        self.assertEqual(self.client.get('/login?return_to=https://external.invalid').headers['Location'], '/admin/cockpit')

    def test_stale_csrf_keeps_chef_target_but_requires_valid_retry(self):
        url = '/login?return_to=chef'
        self.form(url)
        response = self.client.post(url, data=dict(password='synthetic-valid-password', csrf_token='wrong'))
        self.assertEqual(response.headers['Location'], url)
        self.ns['admin_password_matches'].assert_not_called()
        response = self.client.post(url, data=self.form(url))
        self.assertEqual(response.headers['Location'], '/admin/mitarbeiter')


if __name__ == '__main__':
    unittest.main()
