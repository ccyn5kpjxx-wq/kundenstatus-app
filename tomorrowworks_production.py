"""Gemeinsamer Render-Einstieg fuer Werkstatt-App und Tomorrow-Works-Cockpit."""

from __future__ import annotations

import os

from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.middleware.proxy_fix import ProxyFix

from app import app as werkstatt_app
from tomorrowworks_dashboard import create_app


def _mount_path() -> str:
    value = os.getenv("TW_APPLICATION_ROOT", "/agentur").strip() or "/agentur"
    if not value.startswith("/"):
        value = f"/{value}"
    return value.rstrip("/") or "/agentur"


MOUNT_PATH = _mount_path()
dashboard_app = create_app(
    {
        "APPLICATION_ROOT": MOUNT_PATH,
        "SESSION_COOKIE_PATH": MOUNT_PATH,
        "SESSION_COOKIE_SECURE": os.getenv("TW_SESSION_COOKIE_SECURE", "1") == "1",
        "GIT_MONITOR_ENABLED": os.getenv("TW_GIT_MONITOR", "0") == "1",
    }
)
dashboard_app.wsgi_app = ProxyFix(
    dashboard_app.wsgi_app,
    x_for=1,
    x_proto=1,
    x_host=1,
)

application = DispatcherMiddleware(werkstatt_app, {MOUNT_PATH: dashboard_app})
