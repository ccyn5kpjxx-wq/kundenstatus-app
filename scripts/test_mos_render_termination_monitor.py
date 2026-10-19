"""Synthetic checks for the inactive Render termination alarm starter."""

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import builtins
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import patch

from scripts.run_mos_render_termination_monitor import check_read_only, preflight, run


def environment():
    return {
        'RENDER': 'true', 'MOS_TERMINATION_ENABLED': '1',
        'MOS_TERMINATION_EMAIL_ENABLED': '1',
        'MOS_TERMINATION_ORIGIN': 'https://booking.example.invalid',
        'REQUIRE_POSTGRES_ON_RENDER': 'true',
        'DATABASE_URL': 'postgresql://synthetic:synthetic@db.example.invalid:5432/test',
        'FLASK_SECRET_KEY': 'synthetic-only-' + 'x' * 48,
    }


class MonitorTests(TestCase):
    def test_preflight_requires_live_https_and_postgres(self):
        preflight(environment())
        for change in ({'RENDER': ''}, {'MOS_TERMINATION_ENABLED': '0'},
                       {'MOS_TERMINATION_ORIGIN': 'http://booking.example.invalid'},
                       {'DATABASE_URL': 'postgresql://test:pass@127.0.0.1/test'},
                       {'FLASK_SECRET_KEY': 'short'},
                       {'SESSION_COOKIE_SECURE': 'false'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                preflight({**environment(), **change})

    def test_alarm_exit_is_preserved_without_running_real_command(self):
        calls = []
        def fake_runner(command, **kwargs):
            calls.append((command, kwargs))
            return SimpleNamespace(returncode=1)
        self.assertEqual(run(environment(), runner=fake_runner), 1)
        self.assertEqual(calls[0][0][-1], 'mos-termination-check')
        self.assertEqual(calls[0][1]['env']['MAILBOX_SEND_ENABLED'], 'false')


class ReadOnlyMonitorTests(TestCase):
    def environment(self):
        return {'RENDER': 'true', 'MOS_TERMINATION_MONITOR_ENABLED': '1',
                'REQUIRE_POSTGRES_ON_RENDER': 'true',
                'DATABASE_URL': 'postgresql://synthetic:synthetic@db.example.invalid/test'}

    def exercise(self, *, count=0, query_error=None, connect_error=None):
        statements, connections, closed = [], [], []

        class Cursor:
            def execute(self, sql):
                statements.append(sql)
                if sql.startswith('SELECT') and query_error:
                    raise query_error

            def fetchone(self):
                return (count,)

        connection = SimpleNamespace(cursor=lambda: Cursor(), rollback=lambda: closed.append('rollback'),
                                     close=lambda: closed.append('close'))

        def fake_connect(*args, **kwargs):
            connections.append((args, kwargs))
            if connect_error:
                raise connect_error
            return connection

        original_import = builtins.__import__
        def safe_import(name, *args, **kwargs):
            if name == 'app':
                raise AssertionError('App import forbidden in read-only monitor')
            return original_import(name, *args, **kwargs)

        output, errors = StringIO(), StringIO()
        with patch('builtins.__import__', safe_import), redirect_stdout(output), redirect_stderr(errors):
            code = check_read_only(self.environment(), connect=fake_connect)
        return code, statements, connections, closed, output.getvalue() + errors.getvalue()

    def test_zero_or_open_count_uses_only_read_only_transaction_and_no_smtp_or_app(self):
        for count, expected in ((0, 0), (2, 1)):
            with self.subTest(count=count):
                code, sql, connections, closed, output = self.exercise(count=count)
                self.assertEqual(code, expected)
                self.assertEqual(sql, ['BEGIN READ ONLY', "SELECT COUNT(*) FROM miet_checkout_terminations WHERE mail_status!='sent'"])
                options = connections[0][1]
                self.assertEqual(options['connect_timeout'], 10)
                self.assertIn('default_transaction_read_only=on', options['options'])
                self.assertIn('statement_timeout=10000', options['options'])
                self.assertIn('lock_timeout=3000', options['options'])
                self.assertTrue(options['autocommit'])
                self.assertEqual(closed, ['rollback', 'close'])
                self.assertIn(str(count), output)

    def test_missing_table_or_query_timeout_fails_closed_without_details(self):
        for error in (RuntimeError('relation missing; synthetic-secret'), TimeoutError('synthetic-secret')):
            with self.subTest(error=type(error).__name__):
                code, _, _, closed, output = self.exercise(query_error=error)
                self.assertNotEqual(code, 0)
                self.assertEqual(closed, ['rollback', 'close'])
                self.assertNotIn('synthetic-secret', output)
                self.assertNotIn('relation missing', output)

    def test_connection_failure_has_no_sensitive_error_details(self):
        code, _, _, closed, output = self.exercise(connect_error=TimeoutError('synthetic-secret'))
        self.assertNotEqual(code, 0)
        self.assertEqual(closed, [])
        self.assertNotIn('synthetic-secret', output)

    def test_missing_or_local_database_never_connects(self):
        for change in ({'DATABASE_URL': ''}, {'DATABASE_URL': 'postgresql://test:test@localhost/test'},
                       {'MOS_TERMINATION_MONITOR_ENABLED': '0'}, {'REQUIRE_POSTGRES_ON_RENDER': 'false'}):
            with self.subTest(change=change), patch('sys.stderr', StringIO()):
                connect = lambda *a, **k: self.fail('Unsafe runtime must not connect')
                self.assertEqual(check_read_only({**self.environment(), **change}, connect=connect), 2)


if __name__ == '__main__':
    main()
