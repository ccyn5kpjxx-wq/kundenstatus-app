"""Offline orchestration checks: failures must not hide the other MOS alarm."""

from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import patch

from scripts.run_mos_render_operations import reconciliation, termination_monitor, run


class OperationsTests(TestCase):
    def setUp(self):
        def deny(*args, **kwargs):
            raise AssertionError('External network forbidden')
        for name in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.create_connection'):
            block = patch(name, deny)
            block.start()
            self.addCleanup(block.stop)

    def exercise(self, environment, results):
        calls = []
        output = StringIO()

        def fake_runner(kind, index):
            def invoke(environment):
                calls.append((kind, dict(environment)))
                outcome = results[index]
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome
            return invoke

        with redirect_stderr(output):
            code = run(environment, reconcile_runner=fake_runner('reconciliation', 0),
                       termination_runner=fake_runner('termination', 1))
        return code, calls, output.getvalue()

    def test_disabled_or_nonexplicit_flag_runs_only_reconciliation(self):
        for value in (None, '', '0', 'false', 'true', '01'):
            with self.subTest(value=value):
                environment = {} if value is None else {'MOS_TERMINATION_MONITOR_ENABLED': value}
                code, calls, _ = self.exercise(environment, [0])
                self.assertEqual(code, 0)
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0][0], 'reconciliation')

    def test_enabled_runs_both_without_mutating_or_logging_environment(self):
        environment = {'MOS_TERMINATION_MONITOR_ENABLED': '1', 'FLASK_SECRET_KEY': 'synthetic-secret'}
        before = dict(environment)
        code, calls, output = self.exercise(environment, [0, 0])
        self.assertEqual(code, 0)
        self.assertEqual([call[0] for call in calls], ['reconciliation', 'termination'])
        self.assertEqual(calls[0][1], {**environment, 'MOS_TERMINATION_ENABLED': '0'})
        self.assertEqual(calls[1][1], environment)
        self.assertEqual(environment, before)
        self.assertEqual(output, '')

    def test_only_reconciliation_uses_flask_and_public_route_is_disabled_in_its_child(self):
        calls = []

        def fake_process(command, **kwargs):
            calls.append((command, kwargs))
            return SimpleNamespace(returncode=0)

        with patch.object(reconciliation, 'preflight'), \
                patch.object(reconciliation.subprocess, 'run', side_effect=fake_process), \
                patch.object(termination_monitor, 'check_read_only', return_value=0) as monitor:
            code = run({'MOS_TERMINATION_MONITOR_ENABLED': '1', 'MOS_TERMINATION_ENABLED': '1'})
        self.assertEqual(code, 0)
        self.assertEqual([call[0][-1] for call in calls], ['mos-booking-reconcile'])
        self.assertEqual([call[1]['timeout'] for call in calls], [900])
        self.assertTrue(all(call[0][1:4] == ['-m', 'flask', '--app'] for call in calls))
        self.assertEqual(calls[0][1]['env']['MOS_TERMINATION_ENABLED'], '0')
        monitor.assert_called_once()
        self.assertEqual(monitor.call_args.args[0]['MOS_TERMINATION_MONITOR_ENABLED'], '1')

    def test_real_reconciliation_timeout_still_invokes_real_monitor_starter(self):
        calls = []

        def fake_process(command, **kwargs):
            calls.append(command[-1])
            if command[-1] == 'mos-booking-reconcile':
                raise subprocess.TimeoutExpired('flask', kwargs['timeout'])
            return SimpleNamespace(returncode=0)

        with patch.object(reconciliation, 'preflight'), \
                patch.object(reconciliation.subprocess, 'run', side_effect=fake_process), \
                patch.object(termination_monitor, 'check_read_only', side_effect=lambda env: calls.append('readonly-monitor') or 0), \
                redirect_stderr(StringIO()):
            code = run({'MOS_TERMINATION_MONITOR_ENABLED': '1'})
        self.assertEqual(code, 124)
        self.assertEqual(calls, ['mos-booking-reconcile', 'readonly-monitor'])

    def test_direct_and_module_import_reach_real_paused_preflight_without_app_import(self):
        root = Path(__file__).resolve().parents[1]
        probe = r'''
import json, pathlib, runpy, sys, tempfile
from unittest.mock import patch
root=pathlib.Path(sys.argv[1]); mode=sys.argv[2]
def deny(*a,**k): raise AssertionError('Network forbidden')
with patch('socket.socket.connect',deny), patch('socket.create_connection',deny):
 if mode=='direct':
  sys.path.insert(0,str(root/'scripts'))
  loaded=runpy.run_path(str(root/'scripts/run_mos_render_operations.py'),run_name='import_probe')
 else:
  sys.path.insert(0,str(root))
  loaded=runpy.run_module('scripts.run_mos_render_operations',run_name='import_probe')
 with tempfile.TemporaryDirectory(prefix='mos-ops-preflight-') as td:
  secret_root=pathlib.Path(td); config_file=secret_root/'mos-booking.json'
  config={'mode':'live','enabled':False,'live_enabled':True,'origin':'https://example.invalid',
          'deposit_method':'card_authorization_at_booking'}
  config_file.write_text(json.dumps(config),encoding='utf-8')
  env={'RENDER':'true','PUBLIC_SITE_ONLY':'false','REQUIRE_POSTGRES_ON_RENDER':'true',
       'DATABASE_URL':'postgresql://synthetic:synthetic@db.example.invalid/test',
       'MOS_BOOKING_CONFIG_FILE':str(config_file),'MOS_STRIPE_LIVE_KEY':'rk_live_SYNTHETIC',
       'MOS_STRIPE_WEBHOOK_SECRET':'whsec_SYNTHETIC','FLASK_SECRET_KEY':'synthetic-'+'x'*40}
  assert loaded['reconciliation'].preflight(env,secret_root=secret_root)==config
 assert 'app' not in sys.modules
 print('real paused preflight passed; app not imported')
'''
        for mode in ('direct', 'module'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix='mos-ops-cwd-') as directory:
                result = subprocess.run([sys.executable, '-I', '-c', probe, str(root), mode],
                                        cwd=directory, capture_output=True, text=True, timeout=30, check=False)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('real paused preflight passed', result.stdout)

    def test_reconciliation_failure_still_runs_termination_monitor(self):
        code, calls, output = self.exercise({'MOS_TERMINATION_MONITOR_ENABLED': '1'}, [2, 0])
        self.assertEqual(code, 2)
        self.assertEqual(len(calls), 2)
        self.assertIn('Zahlungsabgleich', output)

    def test_termination_failure_reaches_scheduler_after_successful_reconciliation(self):
        code, calls, output = self.exercise({'MOS_TERMINATION_MONITOR_ENABLED': '1'}, [0, 1])
        self.assertEqual(code, 1)
        self.assertEqual(len(calls), 2)
        self.assertIn('Kündigungsalarm', output)

    def test_reconciliation_timeout_or_os_error_never_skips_second_check_or_leaks_error(self):
        failures = [(subprocess.TimeoutExpired('synthetic-secret', 900), 124),
                    (OSError('synthetic-secret'), 127), (ValueError('synthetic-secret'), 1)]
        for failure, expected in failures:
            with self.subTest(error=type(failure).__name__):
                code, calls, output = self.exercise({'MOS_TERMINATION_MONITOR_ENABLED': '1'}, [failure, 0])
                self.assertEqual(code, expected)
                self.assertEqual(len(calls), 2)
                self.assertNotIn('synthetic-secret', output)

    def test_termination_timeout_keeps_first_check_and_returns_failure(self):
        code, calls, _ = self.exercise({'MOS_TERMINATION_MONITOR_ENABLED': '1'},
                                      [0, subprocess.TimeoutExpired('monitor', 90)])
        self.assertEqual(code, 124)
        self.assertEqual(len(calls), 2)

    def test_two_failures_are_both_reported_and_first_status_is_preserved(self):
        code, calls, output = self.exercise({'MOS_TERMINATION_MONITOR_ENABLED': '1'}, [2, 1])
        self.assertEqual(code, 2)
        self.assertEqual(len(calls), 2)
        self.assertIn('Zahlungsabgleich', output)
        self.assertIn('Kündigungsalarm', output)


if __name__ == '__main__':
    main()
