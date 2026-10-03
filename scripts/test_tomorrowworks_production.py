import os
import runpy
import sqlite3
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from flask import Flask
from werkzeug.test import Client
from werkzeug.wrappers import Response


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = PROJECT_ROOT / "tomorrowworks_production.py"


class TomorrowWorksProductionTests(unittest.TestCase):
    def load_entrypoint(self, error=None, *, free_bytes=1024**4):
        werkstatt = Flask("test_werkstatt")

        @werkstatt.get("/")
        def index():
            return "Werkstatt erreichbar"

        @werkstatt.get("/healthz")
        def healthz():
            return "ok"

        @werkstatt.get("/admin/dateiarchiv")
        def dateiarchiv():
            return "Archiv"

        app_module = types.ModuleType("app")
        app_module.app = werkstatt
        dashboard_module = types.ModuleType("tomorrowworks_dashboard")
        create_calls = []

        def failing_create_app(_config):
            create_calls.append(_config)
            if error is not None:
                raise error
            dashboard = Flask("test_dashboard")

            @dashboard.get("/")
            def dashboard_index():
                return "TomorrowWorks"

            return dashboard

        dashboard_module.create_app = failing_create_app
        with (
            patch.dict(
                sys.modules,
                {
                    "app": app_module,
                    "tomorrowworks_dashboard": dashboard_module,
                },
            ),
            patch.dict(
                os.environ,
                {
                    "TW_DASHBOARD_MAINTENANCE": "0",
                    "AUTO_BACKUP_ENABLED": "true",
                },
            ),
            patch("shutil.disk_usage", return_value=types.SimpleNamespace(free=free_bytes)),
        ):
            namespace = runpy.run_path(str(ENTRYPOINT))
            namespace["_test_auto_backup_enabled"] = os.getenv("AUTO_BACKUP_ENABLED")
        return namespace, create_calls

    def test_low_disk_skips_sqlite_and_keeps_export_online(self):
        namespace, create_calls = self.load_entrypoint(free_bytes=16 * 1024)
        client = Client(namespace["application"], Response)

        self.assertEqual(create_calls, [])
        self.assertEqual(namespace["_test_auto_backup_enabled"], "false")
        self.assertEqual(client.get("/healthz").status_code, 200)
        self.assertEqual(client.get("/admin/dateiarchiv").status_code, 200)
        for method, path in (
            ("GET", "/agentur/"),
            ("GET", "/agentur/projekte/1"),
            ("POST", "/agentur/api/aktion"),
        ):
            response = client.open(path, method=method)
            self.assertEqual(response.status_code, 503)
            self.assertNotIn(b"disk I/O error", response.data)

    def test_disk_error_keeps_werkstatt_online(self):
        namespace, create_calls = self.load_entrypoint(sqlite3.OperationalError("disk I/O error"))
        client = Client(namespace["application"], Response)

        self.assertEqual(len(create_calls), 1)
        self.assertEqual(client.get("/").status_code, 200)
        response = client.get("/agentur/")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.headers["Retry-After"], "300")

    def test_unrelated_database_error_still_stops_startup(self):
        with self.assertRaisesRegex(sqlite3.OperationalError, "malformed"):
            self.load_entrypoint(sqlite3.OperationalError("database disk image is malformed"))


if __name__ == "__main__":
    unittest.main()
