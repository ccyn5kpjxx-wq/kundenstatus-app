"""Shared Render configuration; also loaded by Gunicorn's default discovery."""

from pathlib import Path
import faulthandler
import os
import signal
import sys

# App startup includes singleton background services and backups.
workers = 1
worker_class = "portal_gunicorn.PortalThreadWorker"
threads = 4
timeout = 180

# Render terminates client connections at its proxy. Closing upstream HTTP
# connections avoids retaining idle sockets and unread request bodies here.
keepalive = 0

# Avoid disk-backed heartbeat metadata operations blocking the worker loop.
if Path("/dev/shm").is_dir():
    worker_tmp_dir = "/dev/shm"

# No local control interface is needed in Render's managed service.
control_socket_disable = True

# With one worker, request-count recycling would briefly stop serving traffic.
max_requests = 0


def post_fork(server, worker):
    # Diagnostic signal for this worker only; stacks contain no frame locals.
    faulthandler.register(signal.SIGURG, file=sys.stderr, all_threads=True)


def post_worker_init(worker):
    """Start the opt-in catalog job only after the existing portal is loaded."""
    if os.environ.get("TYRESYSTEM_REFRESH_ENABLED", "").strip() != "1":
        return
    portal = sys.modules.get("app")
    production = sys.modules.get("tomorrowworks_production")
    if (portal is None or getattr(portal, "PUBLIC_SITE_ONLY", True)
            or getattr(production, "STORAGE_PRESSURE_MAINTENANCE", False)):
        return
    if getattr(worker.cfg, "workers", None) != 1:
        worker.log.warning("Katalogaktualisierung benötigt genau einen Portal-Worker und bleibt deaktiviert.")
        return
    # Keep the cleanup target even if optional initialization fails partway.
    worker._tyresystem_refresh_portal = portal
    try:
        from tyresystem_refresh_worker import start_refresh_worker
        start_refresh_worker(portal)
    except Exception:
        # Credentials and export URLs must never appear in a startup traceback.
        worker.log.warning("Katalogaktualisierung konnte nicht gestartet werden; Portal bleibt verfügbar.")


def worker_exit(server, worker):
    """Bounded child cleanup in the exiting worker; preserve Gunicorn signals."""
    portal = getattr(worker, "_tyresystem_refresh_portal", None)
    if portal is None:
        return
    try:
        from tyresystem_refresh_worker import stop_refresh_worker
        if stop_refresh_worker(portal) is not True:
            worker.log.warning("Katalogaktualisierung konnte nicht vollständig beendet werden; Schreibstatus prüfen.")
    except Exception:
        worker.log.warning("Katalogaktualisierung benötigt nach dem Worker-Ende eine Statusprüfung.")
