"""Offline startup concurrency/cleanup checks; no real app, database or provider."""

from contextlib import redirect_stderr
import importlib.abc
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_mos_render_cli as runner


ENV = {'RENDER': 'true', 'REQUIRE_POSTGRES_ON_RENDER': 'true',
       'DATABASE_URL': 'postgresql://user:SYNTHETIC_SECRET@db.example.invalid/mos'}


class Connection:
    def __init__(self, events, *, acquire=True, unlock=True):
        self.events = events
        self.acquire = acquire
        self.unlock = unlock
        self.closed = False

    def execute(self, sql, args):
        self.events.append('lock' if 'try_advisory' in sql else 'unlock')
        self.args = args
        value = self.acquire if 'try_advisory' in sql else self.unlock
        if isinstance(value, Exception):
            raise value
        return Mock(fetchone=lambda: (value,))

    def close(self):
        self.closed = True
        self.events.append('close')


class BootstrapTests(unittest.TestCase):
    def test_import_is_locked_but_actual_command_runs_after_unlock_and_close(self):
        events = []
        connection = Connection(events)
        connect = Mock(return_value=connection)
        def imported(name):
            self.assertEqual(name, 'app')
            events.append('import')
        def invoked(command):
            self.assertEqual(command, 'mos-booking-reconcile')
            events.append('command')
            self.assertTrue(connection.closed)
            return 7
        self.assertEqual(runner.run('mos-booking-reconcile', ENV, connect=connect,
                                    importer=imported, invoke=invoked), 7)
        self.assertEqual(events, ['lock', 'import', 'unlock', 'close', 'command'])
        self.assertEqual(connection.args, (runner.APP_IMPORT_LOCK_KEY,))
        kwargs = connect.call_args.kwargs
        self.assertIs(kwargs['autocommit'], True)
        self.assertEqual(kwargs['connect_timeout'], runner.CONNECT_TIMEOUT_SECONDS)
        self.assertIn(f'statement_timeout={runner.QUERY_TIMEOUT_MS}', kwargs['options'])
        self.assertIn(f'lock_timeout={runner.QUERY_TIMEOUT_MS}', kwargs['options'])

    def test_busy_lock_timeout_never_imports_or_invokes_and_emits_no_secret(self):
        events = []
        connection = Connection(events, acquire=False)
        importer, invoke = Mock(), Mock()
        stderr = io.StringIO()
        ticks = iter((0, runner.LOCK_WAIT_SECONDS))
        with redirect_stderr(stderr):
            result = runner.run('mos-contract-delivery', ENV,
                                connect=lambda *a, **k: connection, importer=importer,
                                invoke=invoke, clock=lambda: next(ticks), sleeper=Mock())
        self.assertEqual(result, 2)
        importer.assert_not_called()
        invoke.assert_not_called()
        self.assertEqual(events, ['lock', 'close'])
        self.assertNotIn('SYNTHETIC_SECRET', stderr.getvalue())

    def test_connect_lock_import_and_unlock_failures_are_closed_without_command(self):
        for stage in ('connect', 'lock', 'import', 'unlock'):
            with self.subTest(stage=stage):
                events = []
                error = RuntimeError(ENV['DATABASE_URL'])
                connection = Connection(events, acquire=error if stage == 'lock' else True,
                                        unlock=error if stage == 'unlock' else True)
                connect = Mock(side_effect=error) if stage == 'connect' else Mock(return_value=connection)
                importer = Mock(side_effect=error) if stage == 'import' else Mock()
                invoke = Mock()
                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    self.assertEqual(runner.run('mos-contract-delivery', ENV, connect=connect,
                                                importer=importer, invoke=invoke), 2)
                invoke.assert_not_called()
                self.assertNotIn('SYNTHETIC_SECRET', stderr.getvalue())
                self.assertNotIn('postgresql://', stderr.getvalue())
                if stage != 'connect':
                    self.assertTrue(connection.closed)
                if stage in ('import', 'unlock'):
                    self.assertIn('unlock', events)

    def test_invalid_command_or_database_never_connects(self):
        for command, updates in (('other-command', {}), ('mos-booking-reconcile', {'RENDER': ''}),
                                  ('mos-contract-delivery', {'DATABASE_URL': 'sqlite:///tmp/test'})):
            with self.subTest(command=command, updates=updates), redirect_stderr(io.StringIO()):
                connect, importer, invoke = Mock(), Mock(), Mock()
                self.assertEqual(runner.run(command, {**ENV, **updates}, connect=connect,
                                            importer=importer, invoke=invoke), 2)
                connect.assert_not_called()
                importer.assert_not_called()
                invoke.assert_not_called()

    def test_overlapping_startups_never_import_together(self):
        guard = threading.Lock()
        state = threading.Lock()
        first_import = threading.Event()
        second_attempt = threading.Event()
        active = 0
        imports = []
        results = []
        errors = []

        class SharedConnection:
            def execute(self, sql, args):
                if 'try_advisory' in sql:
                    acquired = guard.acquire(blocking=False)
                    if not acquired:
                        second_attempt.set()
                    return Mock(fetchone=lambda: (acquired,))
                guard.release()
                return Mock(fetchone=lambda: (True,))
            def close(self):
                pass

        def imported(name):
            nonlocal active
            with state:
                active += 1
                if active != 1:
                    errors.append('concurrent import')
                imports.append(name)
            first_import.set()
            if len(imports) == 1 and not second_attempt.wait(2):
                errors.append('second contender did not attempt lock')
            with state:
                active -= 1

        def work():
            results.append(runner.run('mos-booking-reconcile', ENV,
                                      connect=lambda *a, **k: SharedConnection(),
                                      importer=imported, invoke=lambda command: 0,
                                      sleeper=lambda seconds: threading.Event().wait(0.001)))
        one = threading.Thread(target=work)
        two = threading.Thread(target=work)
        one.start()
        self.assertTrue(first_import.wait(2))
        two.start()
        one.join(3)
        two.join(3)
        self.assertFalse(one.is_alive() or two.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [0, 0])
        self.assertEqual(imports, ['app', 'app'])

    def test_real_flask_reuses_cached_synthetic_app_for_both_exact_commands(self):
        from flask import Flask
        import click
        module = ModuleType('app')
        module.app = Flask('synthetic-mos-bootstrap-test')
        events = []
        connection = Connection(events)
        def callback():
            self.assertTrue(connection.closed)
            events.append('command')
        for command in runner.COMMANDS:
            module.app.cli.add_command(click.Command(command, callback=callback))
        class RejectAppSource(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == 'app':
                    raise AssertionError('Flask attempted to reload product app source')
        finder = RejectAppSource()
        def imported(name):
            events.append('import')
            sys.modules[name] = module
        with patch.dict(sys.modules), patch.dict('os.environ', {}, clear=False):
            sys.modules.pop('app', None)
            sys.meta_path.insert(0, finder)
            try:
                for command in sorted(runner.COMMANDS):
                    events.clear()
                    self.assertEqual(runner.run(command, ENV, connect=lambda *a, **k: connection,
                                                importer=imported), 0)
                    self.assertEqual(events, ['lock', 'import', 'unlock', 'close', 'command'])
            finally:
                sys.meta_path.remove(finder)

    def test_direct_and_module_entrypoints_bootstrap_from_foreign_cwd_without_app(self):
        with tempfile.TemporaryDirectory(prefix='mos-cli-bootstrap-test-') as temp:
            for mode in ('direct', 'module'):
                code = ('import os,runpy,sys; '
                        f'sys.path.insert(0,{str(ROOT)!r}); '
                        'os.environ.pop("RENDER",None); '
                        'sys.argv=["mos-cli","unsupported-command"]; '
                        + (f'runpy.run_path({str(ROOT / "scripts/run_mos_render_cli.py")!r},run_name="__main__")'
                           if mode == 'direct' else
                           'runpy.run_module("scripts.run_mos_render_cli",run_name="__main__")'))
                with self.subTest(mode=mode):
                    result = subprocess.run([sys.executable, '-I', '-c', code], cwd=temp,
                                            capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertNotIn('ModuleNotFoundError', result.stderr)
                    self.assertNotIn('Traceback', result.stderr)

    def test_real_flask_command_failure_preserves_nonzero_exit_status(self):
        from flask import Flask
        import click
        module = ModuleType('app')
        module.app = Flask('synthetic-mos-bootstrap-failure')
        @module.app.cli.command('mos-contract-delivery')
        def command():
            error = click.ClickException('Synthetic unresolved delivery')
            error.exit_code = 9
            raise error
        events = []
        connection = Connection(events)
        with patch.dict(sys.modules, {'app': module}), patch.dict('os.environ', {}, clear=False), \
                redirect_stderr(io.StringIO()) as stderr:
            self.assertEqual(runner.run('mos-contract-delivery', ENV,
                                        connect=lambda *a, **k: connection,
                                        importer=lambda name: module), 9)
        self.assertTrue(connection.closed)
        self.assertIn('Synthetic unresolved delivery', stderr.getvalue())


if __name__ == '__main__':
    unittest.main()
