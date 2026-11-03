"""Offline lifecycle checks: fake processes/threads, no downloads or cache writes."""
import importlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import tyresystem_refresh_worker as refresh


class Clock:
    def __init__(self): self.value = 0.0
    def __call__(self): return self.value
    def advance(self, seconds): self.value += seconds


class Event:
    def __init__(self): self.stopped = False; self.waits = []
    def is_set(self): return self.stopped
    def set(self): self.stopped = True
    def wait(self, seconds):
        self.waits.append(seconds)
        self.set()
        return True


class Process:
    def __init__(self, clock, *, exit_code=0, terminate_exits=True, kill_exits=True):
        self.clock, self.exit_code = clock, exit_code
        self.terminate_exits, self.kill_exits = terminate_exits, kill_exits
        self.code = None
        self.terminations = self.kills = 0
        self.waits = []
    def poll(self): return self.code
    def terminate(self):
        self.terminations += 1
        if self.terminate_exits: self.code = -15
    def kill(self):
        self.kills += 1
        if self.kill_exits: self.code = -9
    def wait(self, timeout):
        self.waits.append(timeout)
        if self.code is not None: return self.code
        if self.exit_code is not None:
            self.code = self.exit_code
            return self.code
        self.clock.advance(timeout)
        raise subprocess.TimeoutExpired("synthetic-process", timeout)


class Thread:
    def __init__(self, *, target, name, daemon, clock=None, forever=False, fail=False):
        self.target, self.name, self.daemon = target, name, daemon
        self.clock, self.forever, self.fail = clock, forever, fail
        self.started = False
        self.joins = []
    def start(self):
        if self.fail: raise RuntimeError("PRIVATE startup text")
        self.started = True  # Deliberately never execute a real thread.
    def is_alive(self):
        return self.started and (self.forever or not self.target.__self__._stopping.is_set())
    def join(self, timeout):
        if not self.started: raise RuntimeError("Cannot join thread before it is started")
        self.joins.append(timeout)
        if self.clock: self.clock.advance(timeout)


class RefreshWorkerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="refresh-worker-offline-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.env = {"TYRESYSTEM_REFRESH_ENABLED":"1", "TYRESYSTEM_PRIVATE_ROOT":str(self.root),
                    "FELGEN_PRIVATE_ROOT":str(self.root), "TYRESYSTEM_CATALOG_DIR":str(self.root/"tires"),
                    "FELGEN_CATALOG_DIR":str(self.root/"rims"),
                    "TYRESYSTEM_EXPORT_URL":"https://www.tyresystem.de/PRIVATE-SYNTHETIC-URL",
                    "UNRELATED_SECRET":"PRIVATE-UNRELATED", "PATH":os.environ.get("PATH", "")}
        self.app = SimpleNamespace(config={"PUBLIC_SITE_ONLY":False, "REIFEN_KAUF_ENABLED":False,
                                           "TYRESYSTEM_CATALOG_PUBLICATION_APPROVED":False},
                                   extensions={}, logger=Mock())
        self.portal = SimpleNamespace(app=self.app, PUBLIC_SITE_ONLY=False)
        for target in ("socket.create_connection", "socket.socket.connect", "socket.getaddrinfo"):
            blocked = patch(target, side_effect=AssertionError("No network"))
            blocked.start()
            self.addCleanup(blocked.stop)
        blocked = patch("subprocess.Popen", side_effect=AssertionError("No real process"))
        blocked.start()
        self.addCleanup(blocked.stop)
        self.clock, self.event = Clock(), Event()

    def worker(self, process=None, spawn=None):
        self.process = process or Process(self.clock)
        self.spawn = spawn or Mock(return_value=self.process)
        return refresh.TyreSystemRefreshWorker(refresh._settings(self.env), process_factory=self.spawn,
            event=self.event, monotonic=self.clock,
            thread_factory=lambda **kwargs: Thread(**kwargs, clock=self.clock))

    def test_import_does_not_read_paths_start_threads_or_spawn_even_when_enabled(self):
        with patch("pathlib.Path.resolve", side_effect=AssertionError("No import path IO")), \
             patch("threading.Thread", side_effect=AssertionError("No import thread")), \
             patch.dict(os.environ, self.env, clear=True):
            importlib.reload(refresh)

    def test_default_off_and_public_or_maintenance_instances_do_not_even_validate_paths(self):
        cases = [({}, {}), ({**self.env, "TYRESYSTEM_REFRESH_ENABLED":"true"}, {}),
                 ({**self.env, "TYRESYSTEM_REFRESH_ENABLED":True}, {}),
                 (self.env, {"PUBLIC_SITE_ONLY":True}),
                 (self.env, {"STORAGE_PRESSURE_MAINTENANCE":True}),
                 ({**self.env, "STORAGE_PRESSURE_MAINTENANCE":"1"}, {})]
        for env, config in cases:
            with self.subTest(config=config, enabled=env.get("TYRESYSTEM_REFRESH_ENABLED")):
                self.app.config.update(PUBLIC_SITE_ONLY=False, STORAGE_PRESSURE_MAINTENANCE=False)
                self.app.config.update(config)
                with patch.object(refresh, "_settings", side_effect=AssertionError("No config IO")) as settings:
                    self.assertIsNone(refresh.start_refresh_worker(self.portal, environment=env))
                settings.assert_not_called()
                self.assertEqual(self.app.extensions[refresh.STATUS_KEY], "disabled")

    def test_start_is_singleton_does_not_fetch_and_preserves_purchase_and_publication_gates(self):
        before = dict(self.app.config)
        threads = []
        def factory(**kwargs):
            item = Thread(**kwargs); threads.append(item); return item
        with patch("threading.Thread", side_effect=factory):
            first = refresh.start_refresh_worker(self.portal, environment=self.env)
            second = refresh.start_refresh_worker(self.portal, environment=self.env)
        self.assertIs(first, second)
        self.assertEqual(len(threads), 1)
        self.assertTrue(threads[0].daemon)
        self.assertTrue(threads[0].started)
        self.assertEqual(first.status["runs"], 0)
        self.assertEqual(self.app.config, before)
        self.assertFalse((self.root/"tires").exists())
        self.assertFalse((self.root/"rims").exists())
        self.assertTrue(refresh.stop_refresh_worker(self.portal))
        self.assertTrue(refresh.stop_refresh_worker(self.portal))
        self.assertNotIn(refresh.EXTENSION_KEY, self.app.extensions)

    def test_real_portal_public_flag_is_mandatory_and_overrides_false_app_config(self):
        for value in (True, None, "false", 0):
            self.portal.PUBLIC_SITE_ONLY = value
            with self.subTest(public=value), patch.object(refresh, "_settings") as settings:
                self.assertIsNone(refresh.start_refresh_worker(self.portal, environment=self.env))
                settings.assert_not_called()
        del self.portal.PUBLIC_SITE_ONLY
        with patch.object(refresh, "_settings") as settings:
            self.assertIsNone(refresh.start_refresh_worker(self.portal, environment=self.env))
            settings.assert_not_called()

    def test_invalid_private_paths_or_missing_secret_are_sanitized_without_starting(self):
        changes = [{"TYRESYSTEM_CATALOG_DIR":"relative"}, {"FELGEN_CATALOG_DIR":str(self.root/"tires")},
                   {"FELGEN_CATALOG_DIR":str(self.root/"tires"/"nested")},
                   {"TYRESYSTEM_CATALOG_DIR":str(self.root)},
                   {"TYRESYSTEM_PRIVATE_ROOT":"//PRIVATE.invalid/share"},
                   {"TYRESYSTEM_CATALOG_DIR":str(self.root/"static"/"catalog")},
                   {"FELGEN_PRIVATE_ROOT":str(self.root/"missing")},
                   {"TYRESYSTEM_EXPORT_URL":""}, {"TYRESYSTEM_EXPORT_URL":"PRIVATE\ninvalid"}]
        for change in changes:
            with self.subTest(fields=list(change)), patch("threading.Thread") as thread:
                self.assertIsNone(refresh.start_refresh_worker(self.portal, environment={**self.env, **change}))
                thread.assert_not_called()
                self.assertEqual(self.app.extensions[refresh.STATUS_KEY], "configuration_error")
        self.assertNotIn("PRIVATE", str(self.app.logger.mock_calls))

    def test_reconfiguration_off_stops_previous_daemon_and_bad_start_does_not_break_web(self):
        with patch("threading.Thread", side_effect=lambda **kwargs: Thread(**kwargs)):
            old = refresh.start_refresh_worker(self.portal, environment=self.env)
            self.assertIsNotNone(old)
            self.assertIsNone(refresh.start_refresh_worker(self.portal, environment={}))
        self.assertEqual(old.status["state"], "stopped")
        with patch("threading.Thread", side_effect=lambda **kwargs: Thread(**kwargs, fail=True)):
            self.assertIsNone(refresh.start_refresh_worker(self.portal, environment=self.env))
        self.assertNotIn(refresh.EXTENSION_KEY, self.app.extensions)
        self.assertEqual(self.app.extensions[refresh.STATUS_KEY], "startup_failed")
        self.assertNotIn("PRIVATE", str(self.app.logger.mock_calls))

    def test_child_receives_only_env_url_exact_dual_cli_and_no_shell_or_output_pipes(self):
        worker = self.worker()
        self.assertTrue(worker.run_once())
        args, options = self.spawn.call_args
        command = args[0]
        self.assertEqual(command[:2], [sys.executable, "-B"])
        self.assertTrue(command[2].endswith("refresh_tyresystem_catalog.py"))
        self.assertIn("--run", command)
        for flag, expected in (("--cache-dir",self.root/"tires"), ("--private-root",self.root),
                               ("--felgen-cache-dir",self.root/"rims"), ("--felgen-private-root",self.root)):
            self.assertEqual(command[command.index(flag)+1], str(expected))
        self.assertNotIn("PRIVATE", str(command))
        self.assertFalse(options["shell"])
        self.assertTrue(options["close_fds"])
        for name in ("stdin", "stdout", "stderr"):
            self.assertEqual(options[name], subprocess.DEVNULL)
        self.assertEqual(options["env"]["TYRESYSTEM_EXPORT_URL"], self.env["TYRESYSTEM_EXPORT_URL"])
        self.assertNotIn("UNRELATED_SECRET", options["env"])
        self.assertNotIn("PRIVATE", repr(worker))
        self.assertNotIn("PRIVATE", str(worker.status))
        self.assertEqual(worker.status["state"], "updated")
        self.assertFalse((self.root/"tires").exists())

    def test_loop_fetches_off_request_then_waits_300_seconds_and_stops(self):
        worker = self.worker()
        worker._run()
        self.assertEqual(self.spawn.call_count, 1)
        self.assertEqual(self.event.waits, [300])
        self.assertEqual(worker.status["state"], "stopped")

    def test_timeout_terminates_inside_240_second_ceiling_and_keeps_original_caches_untouched(self):
        process = Process(self.clock, exit_code=None)
        worker = self.worker(process)
        self.assertFalse(worker.run_once())
        self.assertEqual(process.terminations, 1)
        self.assertEqual(process.kills, 0)
        self.assertLessEqual(self.clock(), 240)
        self.assertEqual(worker.status["state"], "timeout")
        self.assertFalse((self.root/"tires").exists())

    def test_unresponsive_child_is_killed_reaped_and_cache_check_flagged(self):
        process = Process(self.clock, exit_code=None, terminate_exits=False)
        worker = self.worker(process)
        self.assertFalse(worker.run_once())
        self.assertEqual((process.terminations, process.kills), (1, 1))
        self.assertEqual(process.poll(), -9)
        self.assertLessEqual(self.clock(), 240)
        self.assertTrue(worker.status["cache_must_be_checked"])
        self.assertIsNone(worker._current_child())

    def test_sigterm_exit_one_without_kill_still_marks_possible_partial_publication(self):
        process = Process(self.clock, exit_code=None)
        def terminate():
            process.terminations += 1
            process.code = 1  # CLI handled SIGTERM between category publications.
        process.terminate = terminate
        worker = self.worker(process)
        self.assertFalse(worker.run_once())
        self.assertEqual((process.terminations, process.kills), (1, 0))
        self.assertTrue(worker.status["cache_must_be_checked"])
        self.assertEqual(worker.status["state"], "timeout")
        self.assertIsNone(worker._current_child())

    def test_stopped_during_spawn_terminates_late_child_and_never_launches_again(self):
        process = Process(self.clock, exit_code=None)
        def spawn(*args, **kwargs):
            self.event.set()
            return process
        worker = self.worker(process, Mock(side_effect=spawn))
        self.assertFalse(worker.run_once())
        self.assertEqual(process.terminations, 1)
        self.assertFalse(worker.run_once())
        self.assertEqual(self.spawn.call_count, 1)
        self.assertTrue(worker.stop())

    def test_stop_during_unfinished_spawn_keeps_instance_until_late_child_is_reaped(self):
        process = Process(self.clock, exit_code=None)
        observations = []
        def spawn(*args, **kwargs):
            observations.append(refresh.stop_refresh_worker(self.portal))
            self.assertIs(self.app.extensions[refresh.EXTENSION_KEY], worker)
            self.assertIs(refresh.start_refresh_worker(self.portal, environment=self.env), worker)
            worker._thread.forever = False
            return process
        worker = self.worker(process, Mock(side_effect=spawn))
        worker._thread = Thread(target=worker._run, name="fake", daemon=True, clock=self.clock, forever=True)
        worker._thread.start()
        self.app.extensions[refresh.EXTENSION_KEY] = worker
        self.assertFalse(worker.run_once())
        self.assertEqual(observations, [False])
        self.assertEqual(process.terminations, 1)
        self.assertIsNone(worker._current_child())
        self.assertTrue(refresh.stop_refresh_worker(self.portal))
        self.assertNotIn(refresh.EXTENSION_KEY, self.app.extensions)

    def test_os_spawn_returning_after_deadline_is_aborted_without_another_grace_wait(self):
        process = Process(self.clock, exit_code=None, terminate_exits=False)
        def spawn(*args, **kwargs):
            self.clock.advance(refresh.MAX_RUN_SECONDS + 1)
            return process
        worker = self.worker(process, Mock(side_effect=spawn))
        self.assertFalse(worker.run_once())
        self.assertEqual((process.terminations, process.kills), (1, 1))
        self.assertTrue(all(timeout == 0 for timeout in process.waits))
        self.assertTrue(worker.status["cache_must_be_checked"])
        self.assertIsNone(worker._current_child())

    def test_stop_interrupts_child_without_long_join_and_repeated_stop_is_safe(self):
        worker = self.worker(Process(self.clock, exit_code=None, terminate_exits=False))
        worker._child = self.process
        worker.start()
        self.assertTrue(worker.stop())
        self.assertEqual((self.process.terminations, self.process.kills), (1, 1))
        self.assertLessEqual(self.clock(), refresh.MAX_STOP_SECONDS)
        self.assertTrue(all(wait <= .25 for wait in worker._thread.joins))
        self.assertTrue(worker.stop())

    def test_incomplete_shutdown_is_bounded_and_never_allows_second_child(self):
        process = Process(self.clock, exit_code=None, terminate_exits=False, kill_exits=False)
        worker = self.worker(process)
        worker._child = process
        self.assertFalse(worker.stop())
        self.assertLessEqual(self.clock(), refresh.MAX_STOP_SECONDS)
        self.assertEqual(worker.status["state"], "stop_incomplete")
        self.assertTrue(worker.status["cache_must_be_checked"])
        self.assertFalse(worker.run_once())
        self.spawn.assert_not_called()

    def test_unfinished_thread_join_has_an_eight_second_budget(self):
        worker = self.worker()
        worker._thread = Thread(target=worker._run, name="fake", daemon=True, clock=self.clock, forever=True)
        worker._thread.start()
        self.assertFalse(worker.stop())
        self.assertLessEqual(self.clock(), refresh.MAX_STOP_SECONDS)
        self.assertEqual(worker.status["state"], "stop_incomplete")

    def test_child_failures_are_generic_do_not_retry_immediately_or_change_gates(self):
        worker = self.worker(Process(self.clock, exit_code=1))
        self.assertFalse(worker.run_once())
        self.assertEqual(worker.status["state"], "refresh_failed")
        self.assertTrue(worker.status["cache_must_be_checked"])
        self.assertEqual(self.spawn.call_count, 1)
        broken = self.worker(spawn=Mock(side_effect=RuntimeError("PRIVATE URL secret")))
        self.assertFalse(broken.run_once())
        self.assertEqual(broken.status["state"], "worker_failed")
        self.assertNotIn("PRIVATE", str(broken.status))
        self.assertFalse(self.app.config["REIFEN_KAUF_ENABLED"])
        self.assertFalse(self.app.config["TYRESYSTEM_CATALOG_PUBLICATION_APPROVED"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
