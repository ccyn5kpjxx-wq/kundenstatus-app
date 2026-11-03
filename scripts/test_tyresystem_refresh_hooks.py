"""Offline Gunicorn lifecycle checks; no portal import, process or network."""
import os
from pathlib import Path
import runpy
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]


class CatalogHooks(unittest.TestCase):
    def setUp(self):
        self.config = runpy.run_path(str(ROOT / "gunicorn.conf.py"))
        self.portal = ModuleType("app")
        self.portal.PUBLIC_SITE_ONLY = False
        self.production = ModuleType("tomorrowworks_production")
        self.production.STORAGE_PRESSURE_MAINTENANCE = False
        self.manager = ModuleType("tyresystem_refresh_worker")
        self.manager.start_refresh_worker = Mock(return_value=object())
        self.manager.stop_refresh_worker = Mock(return_value=True)
        self.worker = SimpleNamespace(cfg=SimpleNamespace(workers=1), log=Mock())
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {}, clear=True).start()
        patch.dict(sys.modules, {"app":self.portal, "tomorrowworks_production":self.production,
                                "tyresystem_refresh_worker":self.manager}).start()

    def start(self):
        self.config["post_worker_init"](self.worker)

    def stop(self):
        self.config["worker_exit"](object(), self.worker)

    def test_default_off_and_nonexplicit_flags_do_not_start_or_stop_anything(self):
        for flag in (None, "0", "true", "yes", ""):
            with self.subTest(flag=flag):
                if flag is not None: os.environ["TYRESYSTEM_REFRESH_ENABLED"] = flag
                self.start()
                self.stop()
        self.manager.start_refresh_worker.assert_not_called()
        self.manager.stop_refresh_worker.assert_not_called()

    def test_existing_worker_and_signal_configuration_is_preserved(self):
        self.assertEqual(self.config["workers"], 1)
        self.assertEqual(self.config["worker_class"], "portal_gunicorn.PortalThreadWorker")
        self.assertEqual(self.config["threads"], 4)
        self.assertEqual(self.config["keepalive"], 0)
        self.assertTrue(callable(self.config["post_fork"]))
        self.assertNotIn("worker_int", self.config)
        self.assertNotIn("worker_abort", self.config)

    def test_enabled_starts_after_initialization_and_stops_same_portal(self):
        os.environ["TYRESYSTEM_REFRESH_ENABLED"] = "1"
        self.start()
        self.manager.start_refresh_worker.assert_called_once_with(self.portal)
        self.stop()
        self.manager.stop_refresh_worker.assert_called_once_with(self.portal)
        self.worker.log.warning.assert_not_called()

    def test_missing_portal_public_homepage_or_storage_pressure_never_starts(self):
        os.environ["TYRESYSTEM_REFRESH_ENABLED"] = "1"
        with patch.dict(sys.modules, {"app":None}): self.start()
        self.portal.PUBLIC_SITE_ONLY = True
        self.start()
        del self.portal.PUBLIC_SITE_ONLY
        self.start()
        self.portal.PUBLIC_SITE_ONLY = False
        self.production.STORAGE_PRESSURE_MAINTENANCE = True
        self.start()
        self.manager.start_refresh_worker.assert_not_called()

    def test_multiple_workers_cannot_enable_multiple_schedulers(self):
        os.environ["TYRESYSTEM_REFRESH_ENABLED"] = "1"
        self.worker.cfg.workers = 2
        self.start()
        self.manager.start_refresh_worker.assert_not_called()
        self.worker.log.warning.assert_called_once()

    def test_optional_start_failure_is_redacted_and_keeps_cleanup_target(self):
        os.environ["TYRESYSTEM_REFRESH_ENABLED"] = "1"
        self.manager.start_refresh_worker.side_effect = RuntimeError("private-export-credential")
        self.start()
        self.stop()
        self.manager.stop_refresh_worker.assert_called_once_with(self.portal)
        self.assertNotIn("private-export-credential", str(self.worker.log.mock_calls))

    def test_failed_or_uncertain_stop_reports_only_fixed_safe_warning(self):
        os.environ["TYRESYSTEM_REFRESH_ENABLED"] = "1"
        self.start()
        self.manager.stop_refresh_worker.return_value = False
        self.stop()
        self.assertEqual(self.worker.log.warning.call_count, 1)
        self.manager.stop_refresh_worker.side_effect = RuntimeError("private-export-credential")
        self.stop()
        self.assertEqual(self.worker.log.warning.call_count, 2)
        self.assertNotIn("private-export-credential", str(self.worker.log.mock_calls))

    def test_hooks_connect_to_actual_manager_without_any_thread_or_child(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("synthetic_real_refresh_manager", ROOT / "tyresystem_refresh_worker.py")
        actual = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {spec.name:actual}):
            spec.loader.exec_module(actual)
        self.portal.app = SimpleNamespace(config={}, extensions={}, logger=Mock())
        with tempfile.TemporaryDirectory(prefix="catalog-hooks-") as folder:
            private_root = Path(folder).resolve()
            env = {"TYRESYSTEM_REFRESH_ENABLED":"1", "TYRESYSTEM_PRIVATE_ROOT":str(private_root),
                   "FELGEN_PRIVATE_ROOT":str(private_root), "TYRESYSTEM_CATALOG_DIR":str(private_root / "tires"),
                   "FELGEN_CATALOG_DIR":str(private_root / "rims"),
                   "TYRESYSTEM_EXPORT_URL":"https://synthetic.invalid/private-export"}
            fake_thread = Mock()
            fake_thread.is_alive.return_value = False
            with patch.dict(sys.modules, {"tyresystem_refresh_worker":actual}), patch.dict(os.environ, env, clear=True), \
                    patch.object(actual.threading, "Thread", return_value=fake_thread), \
                    patch.object(actual.subprocess, "Popen") as process:
                self.start()
                active = self.portal.app.extensions[actual.EXTENSION_KEY]
                self.assertIsInstance(active, actual.TyreSystemRefreshWorker)
                self.assertIn("--run", active._settings.command)
                fake_thread.start.assert_called_once()
                self.stop()
                self.assertNotIn(actual.EXTENSION_KEY, self.portal.app.extensions)
                process.assert_not_called()


if __name__ == "__main__":
    unittest.main()
