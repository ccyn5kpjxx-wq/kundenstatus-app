"""Synthetic process tests; never connect to a Stripe account."""

import contextlib
import io
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from scripts import run_mos_stripe_operator as operator


TEST_KEY = 'sk_test_synthetic_not_a_real_key'
PUBLISHABLE = 'pk_test_synthetic_not_a_real_key'
SIGNING = 'whsec_synthetic_not_a_real_secret'


class FakeProcess:
    def __init__(self, output=b''):
        self.stdout = io.BytesIO(output) if output is not None else None
        self.running = True
        self.terminated = False

    def poll(self):
        return None if self.running else 1

    def terminate(self):
        self.terminated = True
        self.running = False

    def kill(self):
        self.terminated = True
        self.running = False

    def wait(self, timeout=None):
        return 1


class FakeJob:
    def __init__(self):
        self.children = []
        self.closed = False

    def assign(self, child):
        self.children.append(child)

    def close(self):
        self.closed = True


class OperatorTests(unittest.TestCase):
    def test_only_explicit_test_credentials_are_accepted(self):
        for invalid in ('sk_live_synthetic', 'rk_live_synthetic', 'sk_test_',
                        'sk_test_space here', ''):
            with self.assertRaises(ValueError):
                operator._test_value('server', invalid, ('sk_test_', 'rk_test_'))
        self.assertEqual(operator._test_value('server', 'rk_test_synthetic',
                                              ('sk_test_', 'rk_test_')), 'rk_test_synthetic')
        self.assertEqual(operator._test_value('publishable', PUBLISHABLE,
                                              ('pk_test_',)), PUBLISHABLE)

    def test_cli_command_has_only_loopback_and_four_checkout_events(self):
        args = operator._listener_command(Path('synthetic-config.toml'))
        self.assertEqual(args[-1], operator.WEBHOOK_URL)
        self.assertIn('127.0.0.1:5086/mietwagen-test/webhook', args[-1])
        self.assertEqual(set(args[args.index('--events') + 1].split(',')), set(operator.EVENTS))
        self.assertEqual(args[args.index('--color') + 1], 'off')
        self.assertEqual(args[args.index('--config') + 1], 'synthetic-config.toml')
        self.assertNotIn('--live', args)
        self.assertNotIn('--api-key', args)
        self.assertNotIn('--print-secret', args)

    def test_ambient_live_database_and_mail_secrets_are_not_inherited(self):
        inherited = {'SystemRoot': 'C:\\Windows', 'PATH': 'safe-path',
                     'MOS_STRIPE_LIVE_KEY': 'sk_live_synthetic',
                     'DATABASE_URL': 'postgresql://synthetic',
                     'MAIL_SMTP_PASS': 'synthetic-mail-secret'}
        self.assertEqual(operator._safe_base_env(inherited),
                         {'SystemRoot': 'C:\\Windows', 'PATH': 'safe-path'})

    def test_listener_ready_line_is_consumed_without_display(self):
        child = FakeProcess(b'Listening for events\nReady! Your webhook signing secret is '
                            + SIGNING.encode() + b' (^C to quit)\n')
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(operator._listener_secret(child, timeout=1), SIGNING)
        self.assertEqual(out.getvalue(), '')

    def test_listener_without_signing_secret_fails_closed(self):
        child = FakeProcess(b'Authentication failed\n')
        with self.assertRaisesRegex(RuntimeError, 'nicht sicher bereitgestellt'):
            operator._listener_secret(child, timeout=0.2)

    def test_supervisor_passes_secrets_only_in_child_environments_and_stops_both(self):
        cli = FakeProcess(b'Ready! Your webhook signing secret is ' + SIGNING.encode() + b'\n')
        server = FakeProcess(None)
        started = []
        job = FakeJob()

        def fake_popen(args, **kwargs):
            started.append((args, kwargs))
            return cli if len(started) == 1 else server

        inherited = {'SystemRoot': 'C:\\Windows', 'PATH': 'safe-path',
                     'DATABASE_URL': 'postgresql://synthetic',
                     'MAIL_SMTP_PASS': 'synthetic-mail-secret'}
        with (patch.object(operator, '_port_available'),
              patch.object(operator, '_server_ready'),
              patch.object(operator.time, 'sleep', side_effect=KeyboardInterrupt),
              contextlib.redirect_stdout(io.StringIO()) as out):
            with self.assertRaises(KeyboardInterrupt):
                operator.run_with_keys(TEST_KEY, PUBLISHABLE, ambient=inherited,
                                       popen=fake_popen, job_factory=lambda: job)

        self.assertEqual(job.children, [cli, server])
        self.assertTrue(job.closed)
        self.assertTrue(cli.terminated)
        self.assertTrue(server.terminated)
        self.assertEqual(len(started), 2)
        cli_args, cli_options = started[0]
        server_args, server_options = started[1]
        config_dir = Path(cli_options['env']['XDG_CONFIG_HOME'])
        self.assertEqual(config_dir.parent, operator._private_config_root())
        self.assertEqual(Path(cli_args[cli_args.index('--config') + 1]),
                         config_dir / 'config.toml')
        self.assertFalse(config_dir.exists())
        self.assertEqual(cli_options['env']['TEMP'], str(config_dir))
        self.assertEqual(cli_options['env']['TMP'], str(config_dir))
        self.assertEqual(server_options['env']['TEMP'], str(config_dir))
        self.assertEqual(server_options['env']['TMP'], str(config_dir))
        self.assertEqual(cli_options['env']['STRIPE_API_KEY'], TEST_KEY)
        self.assertEqual(server_options['env']['MOS_STRIPE_TEST_KEY'], TEST_KEY)
        self.assertEqual(server_options['env']['MOS_STRIPE_PUBLISHABLE_KEY'], PUBLISHABLE)
        self.assertEqual(server_options['env']['MOS_STRIPE_WEBHOOK_SECRET'], SIGNING)
        self.assertNotIn('STRIPE_API_KEY', server_options['env'])
        self.assertNotIn('DATABASE_URL', server_options['env'])
        self.assertNotIn('MAIL_SMTP_PASS', server_options['env'])
        self.assertNotIn('MOS_STRIPE_WEBHOOK_SECRET', cli_options['env'])
        self.assertEqual(cli_options['stdout'], subprocess.PIPE)
        self.assertEqual(server_options['stdout'], subprocess.DEVNULL)
        self.assertEqual(server_options['stderr'], subprocess.DEVNULL)
        for value in (TEST_KEY, PUBLISHABLE, SIGNING):
            self.assertNotIn(value, repr(cli_args) + repr(server_args) + out.getvalue())

    @unittest.skipUnless(os.name == 'nt', 'Windows Job Objects only')
    def test_job_close_terminates_a_synthetic_child(self):
        job = operator._WindowsJob()
        child = subprocess.Popen(
            [sys.executable, '-c', 'import time; time.sleep(30)'],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            job.assign(child)
            job.close()
            self.assertIsNotNone(child.wait(timeout=5))
        finally:
            operator._stop(child)
            job.close()


if __name__ == '__main__':
    unittest.main()
