"""Offline startup-gate boundaries; no app import, database or provider."""
from contextlib import ExitStack
from pathlib import Path
import sys
import threading
import traceback
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import portal_startup_lock as gate
from scripts import run_mos_render_cli as legacy


ENV = {'RENDER': 'true',
       'DATABASE_URL': 'postgresql://synthetic:SYNTHETIC_SECRET@db.example.invalid/portal'}


class Connection:
    def __init__(self, events, *, acquire=(True,), unlock=(True,), close_error=None):
        self.events = events
        self.acquire = acquire
        self.unlock = unlock
        self.close_error = close_error
        self.closed = False

    def execute(self, sql, params):
        self.events.append(('lock' if 'try_advisory' in sql else 'unlock', params))
        value = self.acquire if 'try_advisory' in sql else self.unlock
        if isinstance(value, Exception):
            raise value
        return Mock(fetchone=lambda: value)

    def close(self):
        self.events.append(('close',))
        self.closed = True
        if self.close_error:
            raise self.close_error


class StartupLockTests(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        for name in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.create_connection'):
            stack.enter_context(patch(name, side_effect=AssertionError('External network forbidden')))

    def test_key_is_identical_to_unchanged_mos_cli(self):
        self.assertEqual(gate.APP_IMPORT_LOCK_KEY, legacy.APP_IMPORT_LOCK_KEY)
        self.assertEqual(gate.LOCK_WAIT_SECONDS, legacy.LOCK_WAIT_SECONDS)

    def test_local_nonrender_and_explicit_sqlite_path_remain_unmodified(self):
        for env in ({}, {'DATABASE_URL': ENV['DATABASE_URL']},
                    {'RENDER': 'true', 'REQUIRE_POSTGRES_ON_RENDER': 'false'},
                    {'RENDER': 'true', 'REQUIRE_POSTGRES_ON_RENDER': '0', 'DATABASE_URL': 'sqlite:///synthetic.db'}):
            with self.subTest(env=env):
                connect = Mock()
                with gate.portal_app_import_lock(env, connect=connect):
                    pass
                connect.assert_not_called()
        error = ValueError('synthetic local import')
        with self.assertRaises(ValueError) as caught:
            with gate.portal_app_import_lock({}, connect=Mock()):
                raise error
        self.assertIs(caught.exception, error)

    def test_render_default_true_and_explicit_flags_hold_entire_import_then_close(self):
        for requirement in (None, '1', 'true', 'yes', 'on', 'false'):
            with self.subTest(requirement=requirement):
                env = dict(ENV)
                if requirement is not None:
                    env['REQUIRE_POSTGRES_ON_RENDER'] = requirement
                events = []
                connection = Connection(events)
                connect = Mock(return_value=connection)
                with gate.portal_app_import_lock(env, connect=connect):
                    events.append(('import-start',))
                    self.assertFalse(connection.closed)
                    events.append(('registration-complete',))
                self.assertEqual([event[0] for event in events],
                                 ['lock', 'import-start', 'registration-complete', 'unlock', 'close'])
                self.assertEqual(events[0][1], (gate.APP_IMPORT_LOCK_KEY,))
                self.assertEqual(events[-2][1], (gate.APP_IMPORT_LOCK_KEY,))
                kwargs = connect.call_args.kwargs
                self.assertIs(kwargs['autocommit'], True)
                self.assertEqual(kwargs['connect_timeout'], gate.CONNECT_TIMEOUT_SECONDS)
                self.assertIn(f'statement_timeout={gate.QUERY_TIMEOUT_MS}', kwargs['options'])
                self.assertIn(f'lock_timeout={gate.QUERY_TIMEOUT_MS}', kwargs['options'])

    def test_invalid_required_configuration_never_connects_or_enters_import(self):
        for updates in ({'DATABASE_URL': ''}, {'DATABASE_URL': 'sqlite:///synthetic.db'},
                        {'DATABASE_URL': 'postgresql://host'}, {'DATABASE_URL': 'postgresql:///dbname'},
                        {'DATABASE_URL': 'postgresql://[broken/portal'}):
            with self.subTest(updates=updates):
                connect, imported = Mock(), Mock()
                with self.assertRaises(gate.StartupImportLockError):
                    with gate.portal_app_import_lock({**ENV, **updates}, connect=connect):
                        imported()
                connect.assert_not_called()
                imported.assert_not_called()

    def test_busy_timeout_closes_without_entering_import_or_unlocking_another_owner(self):
        events = []
        connection = Connection(events, acquire=(False,))
        imported = Mock()
        clock = Mock(side_effect=(0, gate.LOCK_WAIT_SECONDS))
        with self.assertRaises(gate.StartupImportLockError):
            with gate.portal_app_import_lock(ENV, connect=lambda *a, **k: connection,
                                             clock=clock, sleeper=Mock()):
                imported()
        imported.assert_not_called()
        self.assertEqual([event[0] for event in events], ['lock', 'close'])

    def test_busy_poll_may_acquire_after_previous_owner_releases(self):
        events = []
        connection = Connection(events)
        execute = connection.execute
        calls = 0
        def try_then_acquire(sql, params):
            nonlocal calls
            if 'try_advisory' in sql:
                calls += 1
                connection.acquire = (calls > 1,)
            return execute(sql, params)
        connection.execute = try_then_acquire
        sleeper = Mock()
        with gate.portal_app_import_lock(ENV, connect=lambda *a, **k: connection,
                                         clock=Mock(side_effect=(0, 0.1)), sleeper=sleeper):
            events.append(('import',))
        self.assertEqual([event[0] for event in events], ['lock', 'lock', 'import', 'unlock', 'close'])
        sleeper.assert_called_once_with(gate.POLL_SECONDS)

    def test_connect_lock_import_unlock_and_close_errors_fail_closed_without_secrets(self):
        for stage in ('connect', 'lock', 'import', 'unlock', 'close'):
            with self.subTest(stage=stage):
                events = []
                error = RuntimeError(ENV['DATABASE_URL'])
                connection = Connection(events, acquire=error if stage == 'lock' else (True,),
                                        unlock=error if stage == 'unlock' else (True,),
                                        close_error=error if stage == 'close' else None)
                connect = Mock(side_effect=error) if stage == 'connect' else Mock(return_value=connection)
                published = False
                try:
                    with gate.portal_app_import_lock(ENV, connect=connect):
                        if stage == 'import':
                            raise error
                        events.append(('import',))
                    published = True
                except gate.StartupImportLockError as caught:
                    trace = ''.join(traceback.format_exception(caught))
                    self.assertNotIn('SYNTHETIC_SECRET', trace)
                    self.assertNotIn('postgresql://', trace)
                else:
                    self.fail('Startup failure was suppressed')
                self.assertFalse(published)
                if stage != 'connect':
                    self.assertTrue(connection.closed)
                if stage in ('import', 'unlock', 'close'):
                    self.assertIn('unlock', [event[0] for event in events])

    def test_malformed_lock_and_unlock_results_fail_closed(self):
        for value in (None, (), (1,), ('true',)):
            for stage in ('lock', 'unlock'):
                with self.subTest(value=value, stage=stage):
                    events = []
                    connection = Connection(events, acquire=value if stage == 'lock' else (True,),
                                            unlock=value if stage == 'unlock' else (True,))
                    imported = Mock()
                    with self.assertRaises(gate.StartupImportLockError):
                        with gate.portal_app_import_lock(ENV, connect=lambda *a, **k: connection):
                            imported()
                    if stage == 'lock':
                        imported.assert_not_called()
                    self.assertTrue(connection.closed)

    def test_interrupt_still_releases_and_closes_without_suppressing_interrupt(self):
        events = []
        connection = Connection(events)
        with self.assertRaises(KeyboardInterrupt):
            with gate.portal_app_import_lock(ENV, connect=lambda *a, **k: connection):
                raise KeyboardInterrupt
        self.assertEqual([event[0] for event in events], ['lock', 'unlock', 'close'])

    def test_web_and_legacy_cli_share_the_same_exclusion_gate(self):
        lock = threading.Lock()
        entered = threading.Event()
        contender = threading.Event()
        finished = []
        failures = []

        class SharedConnection:
            held = False
            def execute(self, sql, params):
                if params != (gate.APP_IMPORT_LOCK_KEY,):
                    raise AssertionError('Different startup lock namespace')
                if 'try_advisory' in sql:
                    self.held = lock.acquire(blocking=False)
                    if not self.held:
                        contender.set()
                    return Mock(fetchone=lambda: (self.held,))
                lock.release()
                self.held = False
                return Mock(fetchone=lambda: (True,))
            def close(self):
                if self.held:
                    lock.release()
                    self.held = False

        def web():
            try:
                with gate.portal_app_import_lock(ENV, connect=lambda *a, **k: SharedConnection()):
                    entered.set()
                    if not contender.wait(2):
                        raise AssertionError('CLI did not contend')
                finished.append('web')
            except BaseException as error:
                failures.append(type(error).__name__)

        def cli():
            try:
                def imported(name):
                    if not lock.locked():
                        raise AssertionError('Import ran outside the shared gate')
                    return None
                result = legacy.run('mos-booking-reconcile', {**ENV, 'REQUIRE_POSTGRES_ON_RENDER': 'true'},
                                    connect=lambda *a, **k: SharedConnection(), importer=imported,
                                    invoke=lambda command: 0, sleeper=lambda seconds: threading.Event().wait(0.001))
                if result != 0:
                    raise AssertionError('Legacy CLI failed')
                finished.append('cli')
            except BaseException as error:
                failures.append(type(error).__name__)

        first = threading.Thread(target=web)
        second = threading.Thread(target=cli)
        first.start()
        self.assertTrue(entered.wait(2))
        second.start()
        first.join(3)
        second.join(3)
        self.assertFalse(first.is_alive() or second.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(finished, ['web', 'cli'])
        self.assertFalse(lock.locked())


if __name__ == '__main__':
    unittest.main(verbosity=2)
