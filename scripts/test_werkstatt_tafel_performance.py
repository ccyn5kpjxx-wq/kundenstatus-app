"""Protected board HEAD budget; synthetic SQLite, network/workers forbidden.

Run separately with python scripts/test_werkstatt_tafel_performance.py.
The navigation harness sets isolation before importing the actual application.
"""
from contextlib import contextmanager, ExitStack
import sqlite3
import unittest
from unittest.mock import patch

import test_navigation_readmodels as isolated

p = isolated.p


class WorkshopBoardHeadTests(unittest.TestCase):
    def setUp(self):
        self.code = 'TAFEL-PERF-SYNTHETIC'
        p.set_app_setting(p.WERKSTATT_TAFEL_CODE_SETTING, self.code)
        self.addCleanup(self.remove_orders)

    @staticmethod
    def remove_orders():
        db = p.get_db()
        try:
            db.execute("DELETE FROM auftraege WHERE token LIKE 'tafel-performance-%'")
            db.commit()
        finally:
            db.close()

    def client(self, **session):
        client = p.app.test_client()
        if session:
            with client.session_transaction() as state:
                state.update(session)
        return client

    def worker(self):
        return self.client(werkstatt_tafel=p.werkstatt_tafel_session_token(self.code))

    @contextmanager
    def no_board_work(self):
        targets = ((p, 'list_auftraege'), (p, 'list_reifentermine'),
                   (p.inspektionscheckliste, 'load_many'), (p, 'render_template'),
                   (p.check24_bookings, 'review_count'))
        with ExitStack() as stack:
            mocks = [stack.enter_context(patch.object(target, name,
                side_effect=AssertionError('HEAD must not calculate/render the board: ' + name)))
                for target, name in targets]
            yield
            for mocked in mocks:
                mocked.assert_not_called()

    def assert_head(self, response, status):
        self.assertEqual(response.status_code, status)
        self.assertEqual(response.data, b'')
        if status == 200:
            self.assertNotIn('Content-Length', response.headers)
            self.assertEqual(response.headers['Content-Type'], 'text/html; charset=utf-8')
        else:
            self.assertEqual(response.headers['Location'], '/werkstatt')

    def test_anonymous_and_foreign_sessions_still_require_board_login(self):
        for session in ({}, {'partner_autohaus_id': 1}, {'versicherung_id': 1},
                        {'werkstatt_tafel': 'invalid-synthetic-token'}):
            with self.subTest(session=list(session)), self.no_board_work():
                self.assert_head(self.client(**session).head('/werkstatt/tafel'), 302)

    def test_admin_head_skips_all_board_loading_and_rendering(self):
        with self.no_board_work():
            response = self.client(admin=True).head('/werkstatt/tafel')
        self.assert_head(response, 200)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertIn('Cookie', response.headers['Vary'])
        self.assertTrue(response.headers.getlist('Set-Cookie'))

    def test_current_shared_session_head_skips_all_board_work(self):
        with self.no_board_work():
            response = self.worker().head('/werkstatt/tafel')
        self.assert_head(response, 200)
        self.assertEqual(response.headers['Cache-Control'], 'private, no-cache')

    def test_code_rotation_revokes_head_and_get_without_cached_authorization(self):
        worker = self.worker()
        with self.no_board_work():
            self.assert_head(worker.head('/werkstatt/tafel'), 200)
            p.set_app_setting(p.WERKSTATT_TAFEL_CODE_SETTING, self.code + '-ROTATED')
            self.assert_head(worker.head('/werkstatt/tafel'), 302)
            self.assertEqual(worker.get('/werkstatt/tafel').status_code, 302)

    def test_head_sql_budget_is_only_current_auth_even_with_100_tokenized_orders(self):
        clients = ((self.client(admin=True), 0), (self.worker(), 1))
        original_connect = sqlite3.connect
        statements = []
        forbidden = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
                     sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_DROP_TABLE}
        def traced(*args, **kwargs):
            db = original_connect(*args, **kwargs)
            db.set_trace_callback(lambda sql: statements.append(' '.join(sql.split())))
            db.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action in forbidden else sqlite3.SQLITE_OK)
            return db
        for count in (0, 100):
            if count:
                for index in range(count):
                    isolated.insert_order(927600 + index, token='tafel-performance-' + str(index),
                        kunden_status_token='synthetic-board-token-' + str(index), status=2 if index < 20 else 5)
            for client, expected in clients:
                statements.clear()
                with self.subTest(added_orders=count, auth_selects=expected), patch('sqlite3.connect', traced):
                    self.assert_head(client.head('/werkstatt/tafel'), 200)
                selects = [sql for sql in statements if sql.upper().startswith('SELECT')]
                self.assertEqual(len(selects), expected, selects)
                self.assertTrue(all('FROM app_settings WHERE key=' in sql for sql in selects), selects)

    def test_get_remains_fresh_and_head_preserves_its_security_headers(self):
        isolated.insert_order(927700, token='tafel-performance-fresh', fahrzeug='TAFEL-PERF Stand A')
        headers = ('Content-Type', 'Cache-Control', 'Content-Security-Policy',
                   'X-Content-Type-Options', 'X-Frame-Options', 'Referrer-Policy',
                   'Permissions-Policy', 'Vary')
        for role, client in (('admin', self.client(admin=True)), ('worker', self.worker())):
            with self.subTest(role=role):
                get = client.get('/werkstatt/tafel')
                self.assertEqual(get.status_code, 200)
                self.assertIn('TAFEL-PERF Stand A', get.get_data(as_text=True))
                self.assertGreater(len(get.data), 0)
                with self.no_board_work():
                    head = client.head('/werkstatt/tafel')
                self.assert_head(head, 200)
                for header in headers:
                    self.assertIn(header, get.headers)
                    self.assertEqual(head.headers[header], get.headers[header], header)
        db = p.get_db()
        try:
            db.execute("UPDATE auftraege SET fahrzeug='TAFEL-PERF Stand B' WHERE id=927700")
            db.commit()
        finally:
            db.close()
        get = self.worker().get('/werkstatt/tafel')
        self.assertEqual(get.status_code, 200)
        self.assertIn('TAFEL-PERF Stand B', get.get_data(as_text=True))
        self.assertNotIn('TAFEL-PERF Stand A', get.get_data(as_text=True))


if __name__ == '__main__':
    unittest.main(verbosity=2)
