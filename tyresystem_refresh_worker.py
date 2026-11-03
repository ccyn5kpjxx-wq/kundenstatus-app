"""Opt-in catalogue refresh in the web worker which owns its private disk.

Import never starts a thread, loads configuration, touches a cache or downloads.
Gunicorn lifecycle hooks own start/stop; this module installs no signal handler.
"""
from collections.abc import Mapping
from dataclasses import dataclass, field
import os
from pathlib import Path
import subprocess
import sys
import threading
import time


EXTENSION_KEY = "tyresystem_refresh_worker"
STATUS_KEY = "tyresystem_refresh_status"
INTERVAL_SECONDS = 300
MAX_RUN_SECONDS = 240
MAX_STOP_SECONDS = 8
_TERM_SECONDS = 3
_KILL_SECONDS = 2
_LIFECYCLE_LOCK = threading.Lock()
_PUBLIC_PARTS = {"static", "public", "templates", "outputs", "wwwroot"}
_PROCESS_ENV = {"PATH", "SystemRoot", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "TMPDIR",
                "LANG", "LC_ALL", "LC_CTYPE", "LD_LIBRARY_PATH", "SSL_CERT_FILE", "SSL_CERT_DIR"}


@dataclass(frozen=True, repr=False)
class _Settings:
    command: tuple
    cwd: str
    environment: Mapping = field(repr=False)


def _private_path(value):
    if not isinstance(value, str) or not value or any(ord(char) < 32 for char in value):
        raise ValueError()
    text = value.replace("\\", "/")
    path = Path(value)
    if text.startswith("//") or "://" in text or not path.is_absolute():
        raise ValueError()
    resolved = path.resolve()
    if (path != resolved or resolved == Path(resolved.anchor)
            or any(part.lower() in _PUBLIC_PARTS for part in resolved.parts)
            or path.is_symlink()):
        raise ValueError()
    return resolved


def _settings(environment):
    roots = [_private_path(environment.get(name)) for name in ("TYRESYSTEM_PRIVATE_ROOT", "FELGEN_PRIVATE_ROOT")]
    caches = [_private_path(environment.get(name)) for name in ("TYRESYSTEM_CATALOG_DIR", "FELGEN_CATALOG_DIR")]
    for cache, root in zip(caches, roots):
        if not root.is_dir() or cache == root or not cache.is_relative_to(root):
            raise ValueError()
        if cache.exists() and not cache.is_dir():
            raise ValueError()
    if caches[0] == caches[1] or caches[0].is_relative_to(caches[1]) or caches[1].is_relative_to(caches[0]):
        raise ValueError()
    url = environment.get("TYRESYSTEM_EXPORT_URL")
    if not isinstance(url, str) or not 1 <= len(url) <= 8192 or any(ord(char) < 33 for char in url):
        raise ValueError()
    project = Path(__file__).resolve().parent
    script = project / "scripts" / "refresh_tyresystem_catalog.py"
    if not script.is_file() or script.is_symlink() or not sys.executable:
        raise ValueError()
    child_environment = {key: value for key, value in environment.items()
                         if key in _PROCESS_ENV and isinstance(value, str)}
    child_environment.update(TYRESYSTEM_EXPORT_URL=url, PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1")
    return _Settings((sys.executable, "-B", str(script), "--run", "--cache-dir", str(caches[0]),
                      "--private-root", str(roots[0]), "--felgen-cache-dir", str(caches[1]),
                      "--felgen-private-root", str(roots[1])), str(project), child_environment)


def _maintenance(value):
    return value is True or isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "ja"}


def _status(app, code):
    app.extensions[STATUS_KEY] = code
    if code in {"configuration_error", "startup_failed", "stop_incomplete", "worker_failed"}:
        try:
            app.logger.warning("TYREsystem refresh: %s", code)
        except Exception:
            pass


class TyreSystemRefreshWorker:
    """One daemon and at most one bounded child; contains no purchase policy."""
    def __init__(self, settings, *, process_factory=None, event=None, thread_factory=None, monotonic=None):
        self._settings = settings
        self._spawn = process_factory or subprocess.Popen
        self._stopping = event if event is not None else threading.Event()
        self._thread_factory = thread_factory or threading.Thread
        self._clock = monotonic or time.monotonic
        self._state_lock = threading.Lock()
        self._run_lock = threading.Lock()
        self._termination_lock = threading.Lock()
        self._thread = None
        self._child = None
        self._state = "created"
        self._runs = 0
        self._cache_check = False

    def __repr__(self):
        return "<TyreSystemRefreshWorker private_configuration=redacted>"

    @property
    def status(self):
        with self._state_lock:
            return {"state": self._state, "runs": self._runs, "cache_must_be_checked": self._cache_check}

    def _set_state(self, state):
        with self._state_lock:
            self._state = state

    def start(self):
        if self._thread is not None or self._stopping.is_set():
            return
        self._thread = self._thread_factory(target=self._run, name="tyresystem-web-refresh", daemon=True)
        self._set_state("starting")
        self._thread.start()

    @staticmethod
    def _alive(process):
        try:
            return process is not None and process.poll() is None
        except Exception:
            return True  # An unknown process state never permits a second spawn.

    def _current_child(self):
        with self._state_lock:
            return self._child

    def _clear_reaped(self, process):
        if not self._alive(process):
            with self._state_lock:
                if self._child is process:
                    self._child = None

    def _terminate(self, process, budget):
        if not self._alive(process):
            self._clear_reaped(process)
            return True
        if not self._termination_lock.acquire(blocking=False):
            return False
        deadline = self._clock() + max(0, min(budget, _TERM_SECONDS + _KILL_SECONDS))
        try:
            with self._state_lock:
                self._cache_check = True  # Even a graceful SIGTERM can interrupt between the two atomic writes.
            try:
                process.terminate()
                process.wait(timeout=max(0, min(_TERM_SECONDS, deadline - self._clock())))
            except Exception:
                pass
            if self._alive(process):
                try:
                    process.kill()
                    process.wait(timeout=max(0, min(_KILL_SECONDS, deadline - self._clock())))
                except Exception:
                    pass
            self._clear_reaped(process)
            return not self._alive(process)
        finally:
            self._termination_lock.release()

    def run_once(self):
        """Only the daemon/explicit offline tests call this, never a web request."""
        if self._stopping.is_set() or not self._run_lock.acquire(blocking=False):
            return False
        process = None
        deadline = self._clock() + MAX_RUN_SECONDS
        try:
            if self._stopping.is_set() or self._alive(self._current_child()):
                return False
            options = {"shell": False, "cwd": self._settings.cwd,
                       "env": dict(self._settings.environment), "stdin": subprocess.DEVNULL,
                       "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL, "close_fds": True}
            if os.name == "nt":
                options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
            else:
                options["start_new_session"] = True
            self._set_state("refreshing")
            process = self._spawn(list(self._settings.command), **options)
            with self._state_lock:
                self._child = process
                self._runs += 1
            # Leave a termination budget inside the 240-second call ceiling.
            wait_until = deadline - (_TERM_SECONDS + _KILL_SECONDS)
            while not self._stopping.is_set() and self._clock() < wait_until:
                try:
                    code = process.wait(timeout=min(0.25, wait_until - self._clock()))
                    if code != 0:
                        with self._state_lock:
                            self._cache_check = True  # CLI can report a partial multi-file update only through failure.
                    self._set_state("updated" if code == 0 else "refresh_failed")
                    return code == 0
                except subprocess.TimeoutExpired:
                    continue
            reason = "stopping" if self._stopping.is_set() else "timeout"
            self._set_state(reason)
            self._terminate(process, max(0, deadline - self._clock()))
            return False
        except Exception:
            self._set_state("worker_failed")
            if process is not None:
                self._terminate(process, max(0, deadline - self._clock()))
            return False
        finally:
            if process is not None:
                self._clear_reaped(process)
            self._run_lock.release()

    def _run(self):
        try:
            while not self._stopping.is_set():
                self.run_once()
                if self._stopping.wait(INTERVAL_SECONDS):
                    break
        finally:
            if not self._alive(self._current_child()):
                self._set_state("stopped")

    def stop(self):
        """Bounded to eight seconds; caller logs failure, never joins for 240s."""
        deadline = self._clock() + MAX_STOP_SECONDS
        self._stopping.set()
        while self._clock() < deadline:
            process = self._current_child()
            if self._alive(process):
                self._terminate(process, max(0, deadline - self._clock()))
            thread = self._thread
            if thread is not None and thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=max(0, min(0.25, deadline - self._clock())))
            thread_alive = thread is not None and thread.is_alive()
            if not thread_alive and not self._alive(self._current_child()):
                self._set_state("stopped")
                return True
            if thread is None or thread is threading.current_thread():
                break
        self._set_state("stop_incomplete")
        return False


def start_refresh_worker(portal, *, environment=None):
    """Call after loading the WSGI app, paired with stop in worker_exit.

    Exactly '1' opts in. Public/maintenance instances are always denied. Repeated
    starts are idempotent; stop the old instance before changing its settings.
    Startup/configuration failures never take the website down.
    """
    app = portal.app
    env = os.environ if environment is None else environment
    try:
        if (not isinstance(env, Mapping) or env.get("TYRESYSTEM_REFRESH_ENABLED") != "1"
                or getattr(portal, "PUBLIC_SITE_ONLY", None) is not False
                or _maintenance(app.config.get("PUBLIC_SITE_ONLY"))
                or _maintenance(app.config.get("STORAGE_PRESSURE_MAINTENANCE"))
                or _maintenance(env.get("STORAGE_PRESSURE_MAINTENANCE"))):
            if stop_refresh_worker(portal):
                _status(app, "disabled")
            return None
        settings = _settings(env)
        with _LIFECYCLE_LOCK:
            existing = app.extensions.get(EXTENSION_KEY)
            if isinstance(existing, TyreSystemRefreshWorker):
                return existing
            if existing is not None:
                raise ValueError()
            worker = TyreSystemRefreshWorker(settings)
            app.extensions[EXTENSION_KEY] = worker
            try:
                worker.start()
            except Exception:
                worker.stop()
                if worker.status["state"] == "stopped":
                    app.extensions.pop(EXTENSION_KEY, None)
                _status(app, "startup_failed")
                return None
        _status(app, "started")
        return worker
    except Exception:
        stop_refresh_worker(portal)
        _status(app, "configuration_error")
        return None


def stop_refresh_worker(portal):
    """Explicit SIGTERM/normal-exit hook target. No signal handler is replaced."""
    app = portal.app
    with _LIFECYCLE_LOCK:
        worker = app.extensions.get(EXTENSION_KEY)
    if worker is None:
        return True
    if not isinstance(worker, TyreSystemRefreshWorker):
        _status(app, "stop_incomplete")
        return False
    try:
        complete = worker.stop()
    except Exception:
        complete = False
    if complete:
        with _LIFECYCLE_LOCK:
            if app.extensions.get(EXTENSION_KEY) is worker:
                app.extensions.pop(EXTENSION_KEY, None)
        _status(app, "stopped")
    else:
        _status(app, "stop_incomplete")
    return complete
