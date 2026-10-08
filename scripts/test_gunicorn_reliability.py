"""Linux socket regressions against an isolated, synthetic Gunicorn server.

No portal imports, databases, credentials, or existing services are used.
Examples (use separate package directories when comparing versions)::

    python test_gunicorn_reliability.py --suite legacy --gunicorn-path /tmp/gunicorn23
    python test_gunicorn_reliability.py --suite corrected --gunicorn-path /tmp/gunicorn26
    python test_gunicorn_reliability.py --suite production --config /repo/gunicorn.conf.py

``legacy`` succeeds only when BOTH historical stalls are reproduced.
The corrected and production suites require successful, bounded responses.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


WSGI_SOURCE = '''import os
from pathlib import Path
import time

def application(environ, start_response):
    # Deliberately do not read wsgi.input: early rejection has the same shape.
    path = environ.get("PATH_INFO", "")
    if path.startswith("/hold"):
        deadline = time.monotonic() + 15
        while not Path(__file__).with_name("release").exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("synthetic release barrier expired")
            time.sleep(0.005)
    body = b"z" * 262144 if path.endswith("/large") else b"ok\\n"
    start_response("200 OK", [("Content-Type", "text/plain"),
                              ("Content-Length", str(len(body))),
                              ("X-Worker-Pid", str(os.getpid()))])
    return [body]
'''


def remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("request deadline exceeded")
    return value


def receive_response(sock, deadline, *, expect_closed=False, expected_pid=None,
                     expected_body=b"ok\n"):
    data = bytearray()
    while b"\r\n\r\n" not in data:
        sock.settimeout(remaining(deadline))
        part = sock.recv(4096)
        if not part:
            raise AssertionError("connection closed before response headers")
        data.extend(part)
        if len(data) > 65536:
            raise AssertionError("unexpectedly large synthetic response")
    raw_headers, body = bytes(data).split(b"\r\n\r\n", 1)
    lines = raw_headers.decode("latin1").split("\r\n")
    if lines[0].split(" ", 2)[1] != "200":
        raise AssertionError("unexpected status: " + lines[0])
    headers = {}
    for line in lines[1:]:
        key, value = line.split(":", 1)
        headers[key.lower()] = value.strip()
    size = int(headers["content-length"])
    while len(body) < size:
        sock.settimeout(remaining(deadline))
        part = sock.recv(size - len(body))
        if not part:
            raise AssertionError("connection closed before response body")
        body += part
    if body != expected_body:
        raise AssertionError("unexpected synthetic response body")
    worker_pid = int(headers["x-worker-pid"])
    if expected_pid is not None and worker_pid != expected_pid:
        raise AssertionError("worker restarted during the regression")
    if expect_closed:
        if headers.get("connection", "").lower() != "close":
            raise AssertionError("production did not announce Connection: close")
        sock.settimeout(remaining(deadline))
        try:
            extra = sock.recv(1)
        except ConnectionResetError:
            extra = b""  # An already completed response followed by closure.
        if extra:
            raise AssertionError("unexpected bytes after completed response")
    return worker_pid


def send_request(sock, deadline, *, body=b"", close=False, path=None):
    method, default_path = ("POST", "/ignore") if body else ("GET", "/health")
    path = path or default_path
    connection = "close" if close else "keep-alive"
    headers = (
        f"{method} {path} HTTP/1.1\r\nHost: synthetic.invalid\r\n"
        f"Connection: {connection}\r\nContent-Length: {len(body)}\r\n\r\n"
    ).encode("ascii")
    sock.settimeout(remaining(deadline))
    sock.sendall(headers + body)


def socket_count(pid):
    count = 0
    for entry in Path(f"/proc/{pid}/fd").iterdir():
        try:
            count += os.readlink(entry).startswith("socket:")
        except FileNotFoundError:
            pass
    return count


class SyntheticServer:
    def __init__(self, args, *, threads=4, connections=1000, keepalive=120,
                 production=False):
        self.args = args
        self.threads = threads
        self.connections = connections
        self.keepalive = keepalive
        self.production = production
        self.process = None
        self.directory = None
        self.log = None
        self.worker_pid = None

    def __enter__(self):
        self.directory = tempfile.TemporaryDirectory(prefix="gunicorn-reliability-")
        directory = Path(self.directory.name)
        (directory / "synthetic_wsgi.py").write_text(WSGI_SOURCE, encoding="utf-8")
        self.log = (directory / "gunicorn.log").open("w+b")
        # Pass an already bound socket, avoiding a free-port selection race.
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        self.port = listener.getsockname()[1]
        command = [sys.executable, "-m", "gunicorn"]
        if self.production and self.args.config:
            command += ["--config", str(self.args.config)]
        else:
            command += ["--config", "/dev/null", "--worker-class", "gthread",
                        "--threads", str(self.threads),
                        "--worker-connections", str(self.connections),
                        "--keep-alive", str(0 if self.production else self.keepalive)]
            if self.args.version_tuple >= (25, 1):
                command += ["--no-control-socket"]
        command += ["--workers", "1", "--bind", f"fd://{listener.fileno()}",
                    "--timeout", "20", "--graceful-timeout", "2",
                    "--max-requests", "0", "--error-logfile", "-",
                    "--access-logfile", "-", "synthetic_wsgi:application"]
        try:
            self.process = subprocess.Popen(
                command, cwd=directory, env=self.args.child_env,
                stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log,
                start_new_session=True, pass_fds=(listener.fileno(),),
            )
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise
        finally:
            listener.close()
        try:
            deadline = time.monotonic() + 12
            while True:
                if self.process.poll() is not None:
                    raise AssertionError("synthetic server exited during startup")
                try:
                    self.worker_pid = self.request(timeout=min(0.4, remaining(deadline)),
                                                   close=True, expected_pid=None)
                    break
                except (TimeoutError, ConnectionError, OSError):
                    remaining(deadline)
                    time.sleep(0.05)
            # Allow the startup probe's closed connection to leave the poller.
            time.sleep(0.15)
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def request(self, *, timeout=None, body=b"", close=True, expect_closed=False,
                expected_pid="current"):
        deadline = time.monotonic() + (timeout or self.args.deadline)
        if expected_pid == "current":
            expected_pid = self.worker_pid
        with socket.create_connection(("127.0.0.1", self.port),
                                      timeout=remaining(deadline)) as sock:
            send_request(sock, deadline, body=body, close=close)
            return receive_response(sock, deadline, expect_closed=expect_closed,
                                    expected_pid=expected_pid)

    def __exit__(self, error_type, error, traceback):
        try:
            if self.process is not None:
                # This process group was created solely for this subprocess.
                try:
                    os.killpg(self.process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    self.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=3)
            if error_type is not None and self.log is not None:
                self.log.flush()
                self.log.seek(0)
                print(self.log.read()[-6000:].decode("utf-8", "replace"),
                      file=sys.stderr)
        finally:
            if self.log is not None:
                self.log.close()
            if self.directory is not None:
                self.directory.cleanup()


def unread_body(args, legacy):
    with SyntheticServer(args) as server, ExitStack() as stack:
        # Small unread bodies are the control case: they fit in the parser buffer.
        with ExitStack() as control:
            for _ in range(4):
                deadline = time.monotonic() + args.deadline
                sock = control.enter_context(socket.create_connection(
                    ("127.0.0.1", server.port), timeout=remaining(deadline)))
                send_request(sock, deadline, body=b"x" * 32)
                receive_response(sock, deadline, expected_pid=server.worker_pid)
            server.request()
        held = []
        for _ in range(4):
            deadline = time.monotonic() + args.deadline
            sock = stack.enter_context(socket.create_connection(
                ("127.0.0.1", server.port), timeout=remaining(deadline)))
            held.append(sock)
            send_request(sock, deadline, body=b"x" * 16384)
            receive_response(sock, deadline, expected_pid=server.worker_pid)
        time.sleep(0.2)
        try:
            server.request()
        except TimeoutError:
            if not legacy:
                raise
        else:
            if legacy:
                raise AssertionError("legacy unread-body stall was not reproduced")
        if legacy:
            for sock in held:
                sock.close()
            # Exactly one recovery probe; failed requests are never retried.
            server.request()
        else:
            for sock in held:
                deadline = time.monotonic() + args.deadline
                send_request(sock, deadline, close=True)
                receive_response(sock, deadline, expected_pid=server.worker_pid)
                sock.close()
            server.request()
    return {"case": "unread_16k_body", "expected_stall_reproduced": legacy,
            "recovery": "passed"}


def idle_connection_cap(args, legacy):
    with SyntheticServer(args, threads=3, connections=4) as server, ExitStack() as stack:
        baseline = socket_count(server.worker_pid)
        held = [stack.enter_context(socket.create_connection(
            ("127.0.0.1", server.port), timeout=args.deadline)) for _ in range(4)]
        accepted_deadline = time.monotonic() + args.deadline
        while socket_count(server.worker_pid) < baseline + len(held):
            remaining(accepted_deadline)
            time.sleep(0.01)
        # Let the loop reach the connection-limit branch before any data arrives.
        time.sleep(0.15)
        deadline = time.monotonic() + args.deadline
        for sock in held:
            send_request(sock, deadline, close=True)

        def receive(sock):
            try:
                receive_response(sock, deadline, expected_pid=server.worker_pid)
                return "answered"
            except TimeoutError:
                return "timed_out"
            finally:
                # Connection: close is answered before Gunicorn drains/ closes
                # the client; do not artificially retain a client after FIN.
                sock.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(receive, held))
        expected = "timed_out" if legacy else "answered"
        if results != [expected] * 4:
            raise AssertionError(f"connection-cap expected {expected}: {results}")
        if not legacy:
            server.request()
    return {"case": "idle_connection_cap", "responses": results,
            "expected_stall_reproduced": legacy}


def production_load(args):
    with SyntheticServer(args, production=True) as server:
        server.request(close=False, expect_closed=True)
        results = []
        for wave in range(3):
            gate = threading.Barrier(32)

            def one(index):
                gate.wait(timeout=5)
                start = time.monotonic()
                server.request(close=False, expect_closed=True,
                               body=b"x" * 16384 if index % 2 else b"")
                return time.monotonic() - start

            with ThreadPoolExecutor(max_workers=32) as pool:
                times = list(pool.map(one, range(32)))
            server.request(close=False, expect_closed=True)
            results.append({"wave": wave + 1, "passed": len(times),
                            "max_seconds": round(max(times), 4)})
    return {"case": "production_keepalive_zero", "parallel_clients": 32,
            "waves": results, "connection_close_verified": True}


def delayed_client_close(args):
    """Clients retain their write halves after a complete response and FIN."""
    baseline = args.suite == "close-baseline"
    with SyntheticServer(args, production=True) as server:
        baseline_fds = len(list(Path(f"/proc/{server.worker_pid}/fd").iterdir()))
        baseline_sockets = socket_count(server.worker_pid)
        waves = []
        for wave in range(1 if baseline else 3):
            release = Path(server.directory.name) / "release"
            release.unlink(missing_ok=True)
            with ExitStack() as stack:
                held = []
                for index in range(32):
                    sock = stack.enter_context(socket.create_connection(
                        ("127.0.0.1", server.port), timeout=args.deadline))
                    send_request(sock, time.monotonic() + args.deadline,
                                 body=b"x" * 16384 if index % 2 else b"",
                                 path="/hold/large" if index % 2 else "/hold")
                    held.append(sock)
                accepted_deadline = time.monotonic() + args.deadline
                while socket_count(server.worker_pid) < baseline_sockets + len(held):
                    remaining(accepted_deadline)
                    time.sleep(0.01)
                release.touch()
                # Read all bodies but intentionally retain client sockets. Even
                # receiving FIN does not close the clients' write half.
                deadline = time.monotonic() + args.deadline

                def receive(item):
                    index, sock = item
                    expected = b"z" * 262144 if index % 2 else b"ok\n"
                    receive_response(sock, deadline, expected_pid=server.worker_pid,
                                     expected_body=expected)

                with ThreadPoolExecutor(max_workers=32) as pool:
                    list(pool.map(receive, enumerate(held)))
                thread_count = len(list(Path(f"/proc/{server.worker_pid}/task").iterdir()))
                expected_threads = 5 if baseline else 6
                if thread_count != expected_threads:
                    raise AssertionError(f"expected {expected_threads} worker threads, got {thread_count}")
                started = time.monotonic()
                try:
                    server.request()
                except TimeoutError:
                    if not baseline:
                        raise
                else:
                    if baseline:
                        raise AssertionError("upstream graceful-close stall was not reproduced")
                duration = time.monotonic() - started
            # ExitStack releases all clients before one recovery request.
            server.request()
            cleanup_deadline = time.monotonic() + args.deadline
            while len(list(Path(f"/proc/{server.worker_pid}/fd").iterdir())) > baseline_fds:
                remaining(cleanup_deadline)
                time.sleep(0.01)
            waves.append({"wave": wave + 1, "complete_responses": 32,
                          "health_seconds": round(duration, 4),
                          "expected_stall_reproduced": baseline,
                          "worker_threads": thread_count, "fd_count_restored": True})
        if not baseline:
            os.kill(server.worker_pid, signal.SIGURG)
            time.sleep(0.1)
            server.request()
            if b"portal_gunicorn.py" not in Path(server.log.name).read_bytes():
                raise AssertionError("worker diagnostic signal did not dump the closer stack")
    return {"case": "delayed_client_close", "waves": waves,
            "same_worker": True, "diagnostic_signal_verified": not baseline}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("legacy", "corrected", "production",
                                           "close-baseline", "close-corrected"),
                        default="corrected")
    parser.add_argument("--config", type=lambda value: Path(value).resolve(),
                        help="Gunicorn config for production or close-corrected suite")
    parser.add_argument("--gunicorn-path", type=lambda value: Path(value).resolve(),
                        help="Optional isolated pip --target package directory")
    parser.add_argument("--deadline", type=float, default=3.0,
                        help="Seconds per request after readiness; default: 3")
    args = parser.parse_args()
    if sys.platform != "linux" or not Path("/proc/self/fd").is_dir():
        parser.error("Linux with /proc is required; no portal service was touched")
    if not 1 <= args.deadline <= 10:
        parser.error("--deadline must be between 1 and 10 seconds")
    if args.config and args.suite not in {"production", "close-corrected"}:
        parser.error("--config requires --suite production or close-corrected")
    if args.suite == "close-corrected" and not args.config:
        parser.error("close-corrected requires --config with PortalThreadWorker")
    if args.config and not args.config.is_file():
        parser.error("--config must name an existing file")
    if args.gunicorn_path and not args.gunicorn_path.is_dir():
        parser.error("--gunicorn-path must name an existing directory")
    # Whitelist runtime essentials; do not pass live application secrets/config.
    allowed = {"PATH", "LANG", "LC_ALL", "LC_CTYPE", "LD_LIBRARY_PATH",
               "SSL_CERT_FILE", "SSL_CERT_DIR", "SYSTEMROOT"}
    args.child_env = {key: value for key, value in os.environ.items() if key in allowed}
    args.child_env["PYTHONUNBUFFERED"] = "1"
    package_paths = [str(args.gunicorn_path)] if args.gunicorn_path else []
    if args.config:
        package_paths.append(str(args.config.parent))
    if package_paths:
        args.child_env["PYTHONPATH"] = os.pathsep.join(package_paths)
    version = subprocess.check_output(
        [sys.executable, "-c", "import gunicorn; print(gunicorn.__version__)"],
        cwd=tempfile.gettempdir(), env=args.child_env, text=True, timeout=10,
    ).strip()
    args.version_tuple = tuple(int(part) for part in version.split(".")[:3])
    if args.suite == "legacy" and args.version_tuple != (23, 0, 0):
        parser.error("legacy suite requires exactly Gunicorn 23.0.0")
    if args.suite != "legacy" and args.version_tuple < (26, 2, 0):
        parser.error("this suite requires Gunicorn >= 26.2.0")
    print(json.dumps({"suite": args.suite, "gunicorn": version,
                      "python": sys.version.split()[0], "deadline": args.deadline}),
          flush=True)
    results = []
    if args.suite.startswith("close-"):
        cases = [delayed_client_close]
    elif args.suite == "production":
        cases = [production_load]
    else:
        cases = [lambda options: idle_connection_cap(options, args.suite == "legacy"),
                 lambda options: unread_body(options, args.suite == "legacy")]
    for case in cases:
        result = case(args)
        results.append(result)
        print(json.dumps(result), flush=True)
    print(json.dumps({"ok": True, "suite": args.suite, "results": results}), flush=True)


if __name__ == "__main__":
    main()
