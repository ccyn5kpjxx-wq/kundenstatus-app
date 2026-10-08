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

def application(environ, start_response):
    # Deliberately do not read wsgi.input: early rejection has the same shape.
    body = b"ok\\n"
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


def receive_response(sock, deadline, *, expect_closed=False, expected_pid=None):
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
    if body != b"ok\n":
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


def send_request(sock, deadline, *, body=b"", close=False):
    method, path = ("POST", "/ignore") if body else ("GET", "/health")
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("legacy", "corrected", "production"),
                        default="corrected")
    parser.add_argument("--config", type=lambda value: Path(value).resolve(),
                        help="Production Gunicorn config, used only by production suite")
    parser.add_argument("--gunicorn-path", type=lambda value: Path(value).resolve(),
                        help="Optional isolated pip --target package directory")
    parser.add_argument("--deadline", type=float, default=3.0,
                        help="Seconds per request after readiness; default: 3")
    args = parser.parse_args()
    if sys.platform != "linux" or not Path("/proc/self/fd").is_dir():
        parser.error("Linux with /proc is required; no portal service was touched")
    if not 1 <= args.deadline <= 10:
        parser.error("--deadline must be between 1 and 10 seconds")
    if args.config and args.suite != "production":
        parser.error("--config is only supported with --suite production")
    if args.config and not args.config.is_file():
        parser.error("--config must name an existing file")
    if args.gunicorn_path and not args.gunicorn_path.is_dir():
        parser.error("--gunicorn-path must name an existing directory")
    # Whitelist runtime essentials; do not pass live application secrets/config.
    allowed = {"PATH", "LANG", "LC_ALL", "LC_CTYPE", "LD_LIBRARY_PATH",
               "SSL_CERT_FILE", "SSL_CERT_DIR", "SYSTEMROOT"}
    args.child_env = {key: value for key, value in os.environ.items() if key in allowed}
    args.child_env["PYTHONUNBUFFERED"] = "1"
    if args.gunicorn_path:
        args.child_env["PYTHONPATH"] = str(args.gunicorn_path)
    version = subprocess.check_output(
        [sys.executable, "-c", "import gunicorn; print(gunicorn.__version__)"],
        cwd=tempfile.gettempdir(), env=args.child_env, text=True, timeout=10,
    ).strip()
    args.version_tuple = tuple(int(part) for part in version.split(".")[:3])
    if args.suite == "legacy" and args.version_tuple != (23, 0, 0):
        parser.error("legacy suite requires exactly Gunicorn 23.0.0")
    if args.suite != "legacy" and args.version_tuple < (26, 2, 0):
        parser.error("corrected/production suites require Gunicorn >= 26.2.0")
    print(json.dumps({"suite": args.suite, "gunicorn": version,
                      "python": sys.version.split()[0], "deadline": args.deadline}),
          flush=True)
    results = []
    cases = [production_load] if args.suite == "production" else [
        lambda options: idle_connection_cap(options, args.suite == "legacy"),
        lambda options: unread_body(options, args.suite == "legacy"),
    ]
    for case in cases:
        result = case(args)
        results.append(result)
        print(json.dumps(result), flush=True)
    print(json.dumps({"ok": True, "suite": args.suite, "results": results}), flush=True)


if __name__ == "__main__":
    main()
