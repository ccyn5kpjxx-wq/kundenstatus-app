"""Isolated employee PWA metadata/privacy checks: no app import, DB or network."""
from __future__ import annotations

import ast
from functools import lru_cache
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import sys
from types import SimpleNamespace
from unittest import TestCase, main
from urllib.parse import urlsplit

from flask import Flask, abort, request
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from werkstatt_mitarbeiter_app import register_employee_app


@lru_cache(maxsize=1)
def _host_gate_code():
    """Extract policy once; the main application is intentionally never imported."""
    tree = ast.parse((ROOT / 'app.py').read_text(encoding='utf-8-sig'))
    functions = []
    for item in tree.body:
        if isinstance(item, ast.FunctionDef) and item.name in {
                'is_public_site_request', 'restrict_public_site_service'}:
            item.decorator_list = []
            functions.append(item)
    if len(functions) != 2:
        raise AssertionError('Existing public-host policy must remain inspectable.')
    code = ast.fix_missing_locations(ast.Module(body=functions, type_ignores=[]))
    return compile(code, 'isolated-public-host-gate', 'exec')


def _host_gate(app):
    """Use the actual host policy without executing the main Flask module."""
    namespace = {
        'request': request,
        'abort': abort,
        'clean_text': lambda value: str(value or '').strip(),
        'PUBLIC_SITE_ONLY': False,
        'PUBLIC_HOSTS': {'auto-lackierzentrum.de', 'www.auto-lackierzentrum.de'},
    }
    exec(_host_gate_code(), namespace)
    app.before_request(namespace['restrict_public_site_service'])
    return namespace


class EmployeeAppTests(TestCase):
    def setUp(self):
        self.app = Flask('employee-pwa-isolated', template_folder=str(ROOT / 'templates'),
                         static_folder=str(ROOT / 'static'), static_url_path='/static')
        self.app.config.update(TESTING=True, SECRET_KEY='synthetic-pwa-test-only')
        register_employee_app(SimpleNamespace(app=self.app))
        self.host_policy = _host_gate(self.app)
        self.client = self.app.test_client()

    def get(self, path, host='kundenstatus-app.onrender.com'):
        response = self.client.get(path, base_url='https://' + host)
        self.addCleanup(response.close)
        return response

    def test_public_generic_install_page_does_not_require_an_employee(self):
        response = self.get('/werkstatt/app')
        self.assertEqual(response.status_code, 200)
        self.assertIn('text/html', response.content_type)
        body = response.get_data(as_text=True)
        self.assertIn('/werkstatt/mein-konto', body)
        self.assertIn('/static/mitarbeiter-app.webmanifest', body)
        self.assertIn('/static/mitarbeiter-app.js', body)
        self.assertNotIn('#token=', body)
        self.assertNotIn('assistent_mid', body)

    def test_install_and_worker_routes_are_read_only(self):
        for path in ('/werkstatt/app', '/werkstatt/app-sw.js'):
            with self.subTest(path=path):
                self.assertEqual(self.get(path).status_code, 200)
                response = self.client.head(path)
                self.addCleanup(response.close)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(self.client.post(path).status_code, 405)

    def test_install_action_without_javascript_targets_visible_instructions(self):
        class InstallPage(HTMLParser):
            def __init__(self):
                super().__init__()
                self.action = None
                self.details = {}
                self.platforms = []

            def handle_starttag(self, tag, attributes):
                attrs = dict(attributes)
                if 'data-app-install' in attrs:
                    self.action = (tag, attrs)
                if tag == 'details' and 'id' in attrs:
                    self.details[attrs['id']] = attrs
                if 'data-app-platform' in attrs:
                    self.platforms.append(attrs)

        page = InstallPage()
        page.feed(self.get('/werkstatt/app').get_data(as_text=True))
        tag, action = page.action
        self.assertEqual(tag, 'a')
        target = urlsplit(action['href'])
        self.assertFalse(any((target.scheme, target.netloc, target.path, target.query)))
        self.assertTrue(target.fragment)
        instructions = page.details[target.fragment]
        self.assertIn('open', instructions)
        self.assertNotIn('hidden', instructions)
        self.assertEqual({item['data-app-platform'] for item in page.platforms},
                         {'ios', 'android', 'browser'})
        self.assertTrue(all('hidden' not in item for item in page.platforms))

    def test_install_and_worker_have_privacy_and_mime_headers(self):
        for path in ('/werkstatt/app', '/werkstatt/app-sw.js'):
            with self.subTest(path=path):
                response = self.get(path)
                self.assertIn('no-store', response.headers.get('Cache-Control', ''))
                self.assertEqual(response.headers.get('X-Content-Type-Options'), 'nosniff')
                self.assertEqual(response.headers.get('Referrer-Policy'), 'no-referrer')
                self.assertIn('noindex', response.headers.get('X-Robots-Tag', ''))
        self.assertIn('javascript', self.get('/werkstatt/app-sw.js').content_type)

    def test_worker_never_intercepts_private_pages_or_replays_mutations(self):
        response = self.get('/werkstatt/app-sw.js')
        self.assertEqual(response.status_code, 200)
        source = response.get_data(as_text=True)
        # A worker with no fetch/message/sync handlers cannot cache HR pages,
        # retain bearer links, or replay an order/time POST in the background.
        self.assertNotRegex(source, r"addEventListener\s*\(\s*['\"](?:fetch|message|sync|periodicsync)['\"]")
        self.assertNotRegex(source, r'\bon(?:fetch|message|sync|periodicsync)\s*=')
        self.assertNotRegex(source, r'\b(?:caches|indexedDB|localStorage|sessionStorage)\s*[.(]')
        self.assertNotRegex(source, r'\b(?:fetch|XMLHttpRequest|importScripts)\s*\(')

    def test_worker_path_keeps_automatic_scope_inside_the_workshop(self):
        self.assertEqual(self.get('/werkstatt/app-sw.js').status_code, 200)
        self.assertEqual(self.get('/app-sw.js').status_code, 404)
        allowed = self.get('/werkstatt/app-sw.js').headers.get('Service-Worker-Allowed')
        self.assertIn(allowed, (None, '/werkstatt/'))
        script = (ROOT / 'static/mitarbeiter-app.js').read_text(encoding='utf-8')
        self.assertRegex(script, r"register\s*\(\s*['\"]/werkstatt/app-sw\.js['\"]")
        self.assertRegex(script, r"scope\s*:\s*['\"]/werkstatt/['\"]")

    def test_public_homepage_hosts_cannot_serve_app_or_worker(self):
        for host in ('auto-lackierzentrum.de', 'www.auto-lackierzentrum.de',
                     'www.auto-lackierzentrum.de:443'):
            for path in ('/werkstatt/app', '/werkstatt/app-sw.js'):
                with self.subTest(host=host, path=path):
                    self.assertEqual(self.get(path, host).status_code, 404)

    def test_public_only_service_keeps_workshop_unavailable(self):
        self.host_policy['PUBLIC_SITE_ONLY'] = True
        for path in ('/werkstatt/app', '/werkstatt/app-sw.js'):
            self.assertEqual(self.get(path).status_code, 404)

    def test_manifest_has_stable_private_free_paths(self):
        response = self.get('/static/mitarbeiter-app.webmanifest')
        self.assertEqual(response.status_code, 200)
        manifest = json.loads(response.get_data(as_text=True))
        self.assertEqual(manifest['id'], '/werkstatt/mitarbeiter-app')
        self.assertEqual(manifest['start_url'], '/werkstatt/mein-konto')
        self.assertEqual(manifest['scope'], '/werkstatt/')
        self.assertEqual(manifest['display'], 'standalone')
        paths = [manifest['id'], manifest['start_url'], manifest['scope']]
        paths.extend(item['src'] for item in manifest['icons'])
        paths.extend(item['url'] for item in manifest.get('shortcuts', []))
        for path in paths:
            with self.subTest(path=path):
                parts = urlsplit(path)
                self.assertTrue(path.startswith('/'))
                self.assertFalse(any((parts.scheme, parts.netloc, parts.query, parts.fragment)))
        for shortcut in manifest.get('shortcuts', []):
            self.assertTrue(shortcut['url'].startswith(manifest['scope']))

    def test_install_icons_are_real_pngs_with_declared_dimensions(self):
        for size in (180, 192, 512):
            with self.subTest(size=size), Image.open(ROOT / f'static/mitarbeiter-app-icon-{size}.png') as icon:
                self.assertEqual(icon.format, 'PNG')
                self.assertEqual(icon.size, (size, size))
                icon.verify()

    def test_shared_head_has_no_personalized_manifest_or_install_token(self):
        template = self.app.jinja_env.get_template('_mitarbeiter_app_head.html')
        with self.app.test_request_context('/werkstatt/app'):
            body = template.render()
        self.assertIn('/static/mitarbeiter-app.webmanifest', body)
        self.assertIn('/static/mitarbeiter-app-icon-180.png', body)
        self.assertNotIn('#token=', body)
        self.assertNotRegex(body, r'(?:mitarbeiter_id|assistent_mid)\s*=')

    def test_changed_portal_templates_compile_in_the_existing_jinja_environment(self):
        for filename in ('mitarbeiter_app.html', '_mitarbeiter_app_head.html',
                         '_mitarbeiter_app_card.html', '_mitarbeiter_portal_nav.html',
                         'mitarbeiter_portal.html', 'mitarbeiter_auftraege.html',
                         'materialbestellung.html', 'assistent_arbeitszeit.html',
                         'assistent_urlaub.html', 'assistent.html'):
            with self.subTest(template=filename):
                self.app.jinja_env.get_template(filename)


if __name__ == '__main__':
    main()
