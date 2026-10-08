"""Shared Render configuration; also loaded by Gunicorn's default discovery."""

from pathlib import Path

# App startup includes singleton background services and backups.
workers = 1
worker_class = "gthread"
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
