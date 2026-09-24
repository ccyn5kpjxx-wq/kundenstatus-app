"""Synthetic checks for the inactive Render termination alarm starter."""

from types import SimpleNamespace
from unittest import TestCase, main

from scripts.run_mos_render_termination_monitor import preflight, run


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


if __name__ == '__main__':
    main()
