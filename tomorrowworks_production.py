"""Gemeinsamer Render-Einstieg fuer Werkstatt-App und Tomorrow-Works-Cockpit."""

from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path

from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.middleware.proxy_fix import ProxyFix


STARTUP_STORAGE_RESERVE_BYTES = 64 * 1024 * 1024


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _persistent_storage_free_bytes() -> int | None:
    default_data_dir = Path("/var/data") if os.getenv("RENDER") else Path(__file__).resolve().parent / "data"
    probe = Path(os.getenv("DATA_DIR", "").strip() or default_data_dir)
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    try:
        return int(shutil.disk_usage(probe).free)
    except OSError:
        return None


PERSISTENT_STORAGE_FREE_BYTES = _persistent_storage_free_bytes()
STORAGE_PRESSURE_MAINTENANCE = (
    PERSISTENT_STORAGE_FREE_BYTES is not None
    and PERSISTENT_STORAGE_FREE_BYTES < STARTUP_STORAGE_RESERVE_BYTES
)
if STORAGE_PRESSURE_MAINTENANCE:
    # The main app uses PostgreSQL, but its automatic ZIP backup and the
    # TomorrowWorks SQLite startup both write to the persistent disk. During a
    # rescue export neither may compete for the last bytes.
    os.environ["AUTO_BACKUP_ENABLED"] = "false"
    os.environ["TW_DASHBOARD_MAINTENANCE"] = "true"


from app import app as werkstatt_app
from tomorrowworks_dashboard import create_app


def _mount_path() -> str:
    value = os.getenv("TW_APPLICATION_ROOT", "/agentur").strip() or "/agentur"
    if not value.startswith("/"):
        value = f"/{value}"
    return value.rstrip("/") or "/agentur"


MOUNT_PATH = _mount_path()


def _is_sqlite_storage_error(error: sqlite3.OperationalError) -> bool:
    message = str(error).strip().lower()
    return "disk i/o error" in message or "database or disk is full" in message


def _storage_unavailable_wsgi(environ, start_response):
    body = (
        "TomorrowWorks ist wegen eines Speicherengpasses voruebergehend nicht verfuegbar."
    ).encode("utf-8")
    start_response(
        "503 Service Unavailable",
        [
            ("Content-Type", "text/plain; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Cache-Control", "no-store"),
            ("Retry-After", "300"),
        ],
    )
    return [body]


dashboard_wsgi = None
if _env_flag("TW_DASHBOARD_MAINTENANCE"):
    werkstatt_app.logger.error(
        "TomorrowWorks bleibt wegen zu geringer freier Disk-Kapazitaet im Wartungsmodus "
        "(frei=%s, benoetigt=%s); die Werkstatt-App bleibt fuer die kontrollierte "
        "Datensicherung verfuegbar.",
        PERSISTENT_STORAGE_FREE_BYTES,
        STARTUP_STORAGE_RESERVE_BYTES,
    )
    dashboard_wsgi = _storage_unavailable_wsgi
else:
    try:
        dashboard_app = create_app(
            {
                "APPLICATION_ROOT": MOUNT_PATH,
                "SESSION_COOKIE_PATH": MOUNT_PATH,
                "SESSION_COOKIE_SECURE": os.getenv("TW_SESSION_COOKIE_SECURE", "1") == "1",
                "GIT_MONITOR_ENABLED": os.getenv("TW_GIT_MONITOR", "0") == "1",
            }
        )
        dashboard_wsgi = dashboard_app.wsgi_app
    except sqlite3.OperationalError as exc:
        if not _is_sqlite_storage_error(exc):
            raise
        werkstatt_app.logger.exception(
            "TomorrowWorks konnte wegen eines SQLite-Speicherfehlers nicht gestartet werden; "
            "die Werkstatt-App bleibt fuer die kontrollierte Datensicherung verfuegbar."
        )
        dashboard_wsgi = _storage_unavailable_wsgi

dashboard_wsgi = ProxyFix(
    dashboard_wsgi,
    x_for=1,
    x_proto=1,
    x_host=1,
)

application = DispatcherMiddleware(werkstatt_app, {MOUNT_PATH: dashboard_wsgi})
