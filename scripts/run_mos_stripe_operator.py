"""Run the isolated MOS Stripe TEST preview without recording credentials.

Run this from an interactive Windows terminal. Existing Stripe TEST credentials
are requested with hidden input. The Stripe CLI listener's signing secret is
read from its private output pipe and passed only to the local Flask child.
The wrapper passes credentials only through child environments, never as
arguments or files, and discards both child outputs. It never enables the
public MOS booking route.
"""

from __future__ import annotations

import ctypes
from ctypes import wintypes
import getpass
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.request import ProxyHandler, Request, build_opener
import warnings


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / '.agent-hub/stripe-cli/node_modules/@stripe/cli-win32-x64/bin/stripe.exe'
PYTHON = ROOT / '.agent-hub/setup-venv/Scripts/python.exe'
STARTER = ROOT / 'scripts/run_mos_stripe_staging.py'
HOST = '127.0.0.1'
PORT = 5086
TEST_URL = f'http://{HOST}:{PORT}/mietwagen-test/'
WEBHOOK_URL = f'http://{HOST}:{PORT}/mietwagen-test/webhook'
EVENTS = (
    'checkout.session.completed',
    'checkout.session.expired',
    'checkout.session.async_payment_succeeded',
    'checkout.session.async_payment_failed',
)
SECRET_RE = re.compile(rb'(?<![A-Za-z0-9_-])whsec_[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])')
SAFE_ENV = (
    'SystemRoot', 'WINDIR', 'PATH', 'PATHEXT', 'TEMP', 'TMP',
    'USERPROFILE', 'APPDATA', 'LOCALAPPDATA', 'PROGRAMDATA',
)


class _BasicLimit(ctypes.Structure):
    _fields_ = [
        ('PerProcessUserTimeLimit', ctypes.c_int64),
        ('PerJobUserTimeLimit', ctypes.c_int64),
        ('LimitFlags', wintypes.DWORD),
        ('MinimumWorkingSetSize', ctypes.c_size_t),
        ('MaximumWorkingSetSize', ctypes.c_size_t),
        ('ActiveProcessLimit', wintypes.DWORD),
        ('Affinity', ctypes.c_size_t),
        ('PriorityClass', wintypes.DWORD),
        ('SchedulingClass', wintypes.DWORD),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in (
        'ReadOperationCount', 'WriteOperationCount', 'OtherOperationCount',
        'ReadTransferCount', 'WriteTransferCount', 'OtherTransferCount')]


class _ExtendedLimit(ctypes.Structure):
    _fields_ = [
        ('BasicLimitInformation', _BasicLimit),
        ('IoInfo', _IoCounters),
        ('ProcessMemoryLimit', ctypes.c_size_t),
        ('JobMemoryLimit', ctypes.c_size_t),
        ('PeakProcessMemoryUsed', ctypes.c_size_t),
        ('PeakJobMemoryUsed', ctypes.c_size_t),
    ]


class _WindowsJob:
    """Close the job to kill both children, including on supervisor exit."""

    def __init__(self):
        if os.name != 'nt':
            raise RuntimeError('Dieser Starter benötigt Windows.')
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateJobObjectW.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
        kernel.CreateJobObjectW.restype = wintypes.HANDLE
        kernel.SetInformationJobObject.argtypes = (
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD)
        kernel.SetInformationJobObject.restype = wintypes.BOOL
        kernel.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
        kernel.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel.CloseHandle.restype = wintypes.BOOL
        self.kernel = kernel
        self.handle = kernel.CreateJobObjectW(None, None)
        if not self.handle:
            raise RuntimeError('Prozessschutz konnte nicht eingerichtet werden.')
        info = _ExtendedLimit()
        info.BasicLimitInformation.LimitFlags = 0x00002000  # KILL_ON_JOB_CLOSE
        if not kernel.SetInformationJobObject(self.handle, 9, ctypes.byref(info), ctypes.sizeof(info)):
            self.close()
            raise RuntimeError('Prozessschutz konnte nicht eingerichtet werden.')

    def assign(self, child):
        if not self.kernel.AssignProcessToJobObject(self.handle, wintypes.HANDLE(child._handle)):
            raise RuntimeError('Testprozess konnte nicht abgesichert werden.')

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def _test_value(name, value, prefixes):
    if (not isinstance(value, str) or not any(value.startswith(prefix) and len(value) > len(prefix)
            for prefix in prefixes) or any(char.isspace() for char in value)):
        raise ValueError(f'{name} fehlt oder ist kein Stripe-TEST-Wert.')
    return value


def _hidden_input(prompt):
    with warnings.catch_warnings():
        warnings.simplefilter('error', getpass.GetPassWarning)
        return getpass.getpass(prompt)


def _safe_base_env(source):
    return {name: source[name] for name in SAFE_ENV if name in source}


def _listener_command(config_file):
    return [str(CLI), 'listen', '--color', 'off', '--config', str(config_file),
            '--events', ','.join(EVENTS), '--forward-to', WEBHOOK_URL]


def _private_config_root():
    """Keep CLI config and recursive temp cleanup inside this ignored workspace area."""
    workspace = ROOT.resolve(strict=True)
    hub = (ROOT / '.agent-hub').resolve(strict=True)
    if hub.parent != workspace:
        raise RuntimeError('Lokaler Agenten-Hub liegt außerhalb des Workspaces.')
    temporary = hub / 'stripe-operator-temp'
    temporary.mkdir(exist_ok=True)
    temporary = temporary.resolve(strict=True)
    if temporary.parent != hub:
        raise RuntimeError('Temporärer CLI-Ordner liegt außerhalb des Agenten-Hubs.')
    return temporary


def _listener_secret(child, timeout=45):
    """Consume all CLI output privately; accept only its ready-line secret."""
    ready = threading.Event()
    secret = {}

    def drain():
        try:
            for line in iter(child.stdout.readline, b''):
                if b'webhook signing secret' in line.lower():
                    match = SECRET_RE.search(line)
                    if match and 'value' not in secret:
                        secret['value'] = match.group().decode('ascii')
                        ready.set()
        finally:
            ready.set()

    threading.Thread(target=drain, daemon=True, name='stripe-test-output-drain').start()
    deadline = time.monotonic() + timeout
    while not ready.wait(0.1):
        if child.poll() is not None or time.monotonic() >= deadline:
            break
    if child.poll() is not None or 'value' not in secret:
        raise RuntimeError('Stripe-TEST-Listener wurde nicht sicher bereitgestellt.')
    return secret['value']


def _port_available():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, PORT))


def _server_ready(child, timeout=60):
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if child.poll() is not None:
            break
        try:
            with opener.open(Request(TEST_URL), timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.2)
    raise RuntimeError('Isolierter MOS-Testserver wurde nicht bereit.')


def _stop(child):
    if child is None:
        return
    try:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
    except OSError:
        pass
    finally:
        if child.stdout:
            child.stdout.close()


def run_with_keys(test_key, publishable_key, *, ambient=None, popen=subprocess.Popen,
                  job_factory=_WindowsJob):
    _test_value('Stripe-Serverkey', test_key, ('sk_test_', 'rk_test_'))
    _test_value('Stripe-Publishable-Key', publishable_key, ('pk_test_',))
    if os.name != 'nt' or not CLI.is_file() or not PYTHON.is_file() or not STARTER.is_file():
        raise RuntimeError('Vorbereitete lokale Windows-Testwerkzeuge fehlen.')
    ambient = os.environ if ambient is None else ambient
    if ambient.get('MOS_STRIPE_LIVE_KEY') or ambient.get('MOS_PUBLIC_STRIPE_LIVE_KEY'):
        raise RuntimeError('Live-Schlüssel im Testprozess nicht zulässig.')
    _port_available()
    config_root = _private_config_root()
    with tempfile.TemporaryDirectory(prefix='run-', dir=config_root) as config_dir:
        config_dir = Path(config_dir).resolve(strict=True)
        if config_dir.parent != config_root:
            raise RuntimeError('Temporärer CLI-Ordner liegt außerhalb des Agenten-Hubs.')
        cli = server = None
        job = job_factory()
        try:
            base = _safe_base_env(ambient)
            cli_env = {**base, 'STRIPE_API_KEY': test_key,
                       'XDG_CONFIG_HOME': str(config_dir),
                       'TEMP': str(config_dir), 'TMP': str(config_dir)}
            cli = popen(_listener_command(config_dir / 'config.toml'), cwd=ROOT,
                        env=cli_env, stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        creationflags=subprocess.CREATE_NO_WINDOW)
            job.assign(cli)
            signing_secret = _listener_secret(cli)
            server_env = {**base, 'TEMP': str(config_dir), 'TMP': str(config_dir),
                          'MOS_STRIPE_TEST_KEY': test_key,
                          'MOS_STRIPE_PUBLISHABLE_KEY': publishable_key,
                          'MOS_STRIPE_WEBHOOK_SECRET': signing_secret,
                          'MOS_STRIPE_LIVE_KEY': ''}
            server = popen([str(PYTHON), str(STARTER)], cwd=ROOT, env=server_env,
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW)
            job.assign(server)
            _server_ready(server)
            print(f'Nur Stripe-TEST: {TEST_URL}  (Strg+C beendet beide Prozesse.)')
            while cli.poll() is None and server.poll() is None:
                time.sleep(0.3)
            raise RuntimeError('Ein Testprozess wurde beendet; beide Prozesse werden gestoppt.')
        finally:
            job.close()
            try:
                _stop(server)
            finally:
                _stop(cli)


def main():
    if os.name != 'nt' or not sys.stdin.isatty() or not sys.stderr.isatty():
        print('Nur aus einem interaktiven Windows-Terminal starten.', file=sys.stderr)
        return 2
    try:
        test_key = _hidden_input('Vorhandener Stripe-TEST-Serverkey (verdeckt): ')
        publishable_key = _hidden_input('Passender pk_test_-Key (verdeckt): ')
        run_with_keys(test_key, publishable_key)
    except KeyboardInterrupt:
        print('\nLokaler Stripe-TEST beendet.')
        return 0
    except Exception:
        # Never render an exception: third-party errors can embed credentials.
        print('Lokaler Stripe-TEST konnte nicht sicher gestartet werden oder wurde gestoppt.',
              file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
