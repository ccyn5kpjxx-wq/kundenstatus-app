"""Serialize the two MOS background app imports, then run their unchanged CLI.

The portal initializes schemas during import.  A dedicated PostgreSQL session
lock prevents the mail worker and reconciliation cron importing it together.
The lock is released before mail delivery or Stripe reconciliation starts.
Web deployments must still stop these background jobs while importing app.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

COMMANDS = frozenset({'mos-contract-delivery', 'mos-booking-reconcile'})
# Dedicated namespace; unrelated to the destructive portal-originals lock.
APP_IMPORT_LOCK_KEY = 6572746_20261019
CONNECT_TIMEOUT_SECONDS = 10
QUERY_TIMEOUT_MS = 5000
LOCK_WAIT_SECONDS = 90
POLL_SECONDS = 0.2


def _invoke(command):
    from flask.cli import cli
    import click

    try:
        return cli.main(args=['--app', 'app', command], prog_name='flask',
                        standalone_mode=False) or 0
    except click.ClickException as exc:
        exc.show()
        return exc.exit_code
    except click.Abort:
        print('Aborted!', file=sys.stderr)
        return 1


def run(command, environment=None, *, connect=None, importer=importlib.import_module,
        invoke=_invoke, clock=time.monotonic, sleeper=time.sleep):
    runtime = os.environ if environment is None else environment
    try:
        database = urlsplit(runtime.get('DATABASE_URL', ''))
        if (command not in COMMANDS or not runtime.get('RENDER')
                or runtime.get('REQUIRE_POSTGRES_ON_RENDER', '').lower() not in {'1', 'true'}
                or database.scheme not in {'postgres', 'postgresql'} or not database.hostname
                or database.hostname in {'localhost', '127.0.0.1', '::1'}
                or not database.path.strip('/')):
            raise ValueError
    except (TypeError, ValueError):
        print('MOS-CLI nicht gestartet: sichere PostgreSQL-Konfiguration fehlt.', file=sys.stderr)
        return 2

    connection = None
    acquired = False
    ready = False
    try:
        if connect is None:
            import psycopg
            connect = psycopg.connect
        connection = connect(
            runtime['DATABASE_URL'], connect_timeout=CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
            options=f'-c statement_timeout={QUERY_TIMEOUT_MS} -c lock_timeout={QUERY_TIMEOUT_MS}',
        )
        deadline = clock() + LOCK_WAIT_SECONDS
        while True:
            row = connection.execute('SELECT pg_try_advisory_lock(%s)',
                                     (APP_IMPORT_LOCK_KEY,)).fetchone()
            if row and row[0] is True:
                acquired = True
                break
            if clock() >= deadline:
                raise TimeoutError
            sleeper(POLL_SECONDS)
        importer('app')
        ready = True
    except Exception:
        # Driver/import errors may contain connection strings: emit no details.
        print('MOS-CLI nicht gestartet: geschützter App-Import fehlgeschlagen.', file=sys.stderr)
    finally:
        if connection is not None:
            if acquired:
                try:
                    row = connection.execute('SELECT pg_advisory_unlock(%s)',
                                             (APP_IMPORT_LOCK_KEY,)).fetchone()
                    if not row or row[0] is not True:
                        raise ValueError
                except Exception:
                    ready = False
                    print('MOS-CLI nicht gestartet: Importsperre konnte nicht freigegeben werden.',
                          file=sys.stderr)
            try:
                connection.close()
            except Exception:
                ready = False
                print('MOS-CLI nicht gestartet: Sperrverbindung konnte nicht geschlossen werden.',
                      file=sys.stderr)
    if not ready:
        return 2
    return invoke(command)


if __name__ == '__main__':
    raise SystemExit(run(sys.argv[1] if len(sys.argv) == 2 else ''))
