"""The continuous worker is inert without an explicit Render live preflight."""

from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_mos_render_contract_delivery_worker import POLL_SECONDS, work


class WorkerTests(unittest.TestCase):
    def test_disabled_environment_never_starts_cycle(self):
        calls = []
        self.assertEqual(work({}, one_cycle=lambda _env: calls.append('sent'),
                              max_cycles=1), 2)
        self.assertEqual(calls, [])

    def test_success_polls_and_failure_exits_for_alert(self):
        with patch('run_mos_render_contract_delivery_worker.preflight'):
            cycles, sleeps = [], []

            def cycle(_env):
                cycles.append(1)
                return 0 if len(cycles) == 1 else 4

            self.assertEqual(work({'RENDER': 'true'}, one_cycle=cycle,
                                  sleeper=sleeps.append), 4)
            self.assertEqual(len(cycles), 2)
            self.assertEqual(sleeps, [POLL_SECONDS])


if __name__ == '__main__':
    unittest.main()
