"""Public UI cache boundaries and request-local timing; no live data/network."""
import re
import unittest
from unittest.mock import patch

import test_navigation_readmodels as support
from portal_request_timing import start_request_timing, timed_database_call, server_timing_header
from portal_static_assets import PUBLIC_UI_ASSETS

p = support.p


class NavigationCacheHeadersTests(unittest.TestCase):
    def setUp(self):
        self.admin = support.admin_client()

    def test_versioned_public_assets_are_private_cacheable_and_byte_identical(self):
        with p.app.test_request_context('/'):
            urls = {name:p.static_asset_url(name) for name in PUBLIC_UI_ASSETS}
        for name, url in urls.items():
            with self.subTest(name=name):
                self.assertRegex(url, r'[?&]v=[a-f0-9]{20}(?:&|$)')
                result = self.admin.get(url)
                self.assertEqual(result.status_code, 200)
                self.assertEqual(result.headers['Cache-Control'], 'private, max-age=31536000, immutable')
                self.assertNotIn('public', result.headers['Cache-Control'])
                self.assertEqual(result.data, (p.pathlib.Path(p.app.static_folder) / name).read_bytes())
                result.close()

    def test_unversioned_wrong_duplicate_unknown_and_error_assets_do_not_get_long_cache(self):
        with p.app.test_request_context('/'):
            valid = p.static_asset_url('admin-sidebar.css')
        for url in ('/static/admin-sidebar.css', '/static/admin-sidebar.css?v=old',
                    valid + '&v=old', '/static/logo.png?v=anything', '/static/missing.css?v=anything'):
            with self.subTest(url=url):
                response = self.admin.get(url)
                self.assertNotIn('immutable', response.headers.get('Cache-Control', ''))
                self.assertNotIn('max-age=31536000', response.headers.get('Cache-Control', ''))
                response.close()
        with p.app.test_request_context('/'):
            self.assertEqual(p.static_asset_url('logo.png'), '/static/logo.png')

    def test_version_changes_with_asset_content_and_conditional_requests_keep_policy(self):
        asset = support.SCRATCH / 'assets'
        asset.mkdir(exist_ok=True)
        path = asset / 'admin-sidebar.css'
        path.write_bytes(b'body { color: red; }')
        with patch.object(p.app, '_static_folder', str(asset)):
            with p.app.test_request_context('/'):
                first = p.static_asset_url('admin-sidebar.css')
            response = self.admin.get(first)
            conditional = self.admin.get(first, headers={'If-None-Match':response.headers['ETag']})
            self.assertEqual(conditional.status_code, 304)
            self.assertIn('immutable', conditional.headers['Cache-Control'])
            response.close()
            conditional.close()
            path.write_bytes(b'body { color: blue; padding: 1px; }')
            with p.app.test_request_context('/'):
                second = p.static_asset_url('admin-sidebar.css')
            self.assertNotEqual(first, second)
            with self.admin.get(first) as stale:
                self.assertNotIn('immutable', stale.headers['Cache-Control'])
            with self.admin.get(second) as fresh:
                self.assertIn('immutable', fresh.headers['Cache-Control'])

    def test_html_status_and_original_downloads_keep_existing_private_policy(self):
        html = self.admin.get('/admin')
        self.assertEqual(html.status_code, 200)
        self.assertIn('no-store', html.headers['Cache-Control'])
        self.assertIn('Server-Timing', html.headers)
        for url in ('/status/not-a-valid-token', '/admin/datei/99999999', '/admin/datei/99999999/download'):
            with self.subTest(url=url):
                result = self.admin.get(url)
                self.assertEqual(result.status_code, 404)
                self.assertNotIn('immutable', result.headers['Cache-Control'])
                self.assertIn('no-store', result.headers['Cache-Control'])
        public = p.app.test_client().get('/homepage')
        self.assertNotIn('Server-Timing', public.headers)

    def test_actual_template_uses_fresh_content_versions_for_common_assets(self):
        html = self.admin.get('/admin').get_data(as_text=True)
        for name in ('admin-sidebar.css', 'admin-sidebar.js', 'werkstatt_theme.css', 'logo-nav-192.webp'):
            self.assertRegex(html, re.escape('/static/' + name) + r'\?v=[a-f0-9]{20}')


class NavigationRequestTimingTests(unittest.TestCase):
    def test_timing_contains_only_durations_and_statement_count(self):
        calls = []
        @timed_database_call('db')
        def query(sql, params):
            calls.append((sql, params))
            return 42
        @timed_database_call('connect')
        def connect():
            return 'connection'
        with p.app.test_request_context('/'):
            with patch('portal_request_timing.perf_counter', side_effect=[1,2,2.002,3,3.005,4]):
                start_request_timing()
                self.assertEqual(query('SELECT sensitive_name FROM private_table', ('secret-token',)), 42)
                self.assertEqual(connect(), 'connection')
                header = server_timing_header()
            self.assertEqual(header, 'app;dur=3000.00, db;dur=2.00, connect;dur=5.00, sql;desc="1 statements"')
            self.assertNotIn('secret', header)
            self.assertNotIn('private_table', header)
        self.assertEqual(calls, [('SELECT sensitive_name FROM private_table', ('secret-token',))])

    def test_errors_propagate_and_following_request_is_fresh(self):
        failure = ValueError('synthetic failure')
        @timed_database_call('db')
        def operation():
            raise failure
        with p.app.test_request_context('/'):
            start_request_timing()
            with self.assertRaises(ValueError) as raised:
                operation()
            self.assertIs(raised.exception, failure)
            self.assertIn('1 statements', server_timing_header())
        with p.app.test_request_context('/'):
            start_request_timing()
            self.assertIn('0 statements', server_timing_header())

    def test_background_and_uninstrumented_context_keep_plain_behavior(self):
        @timed_database_call('db')
        def operation(value):
            return value
        marker = object()
        self.assertIs(operation(marker), marker)
        with p.app.test_request_context('/'):
            self.assertIs(operation(marker), marker)
            self.assertIsNone(server_timing_header())


if __name__ == '__main__':
    unittest.main(verbosity=2)
