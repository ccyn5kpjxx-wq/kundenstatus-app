"""Coordinate complete Render web imports with the existing MOS import gate."""
from contextlib import contextmanager
import os
import time
from urllib.parse import urlsplit


# Keep identical to scripts.run_mos_render_cli; its focused test checks parity.
APP_IMPORT_LOCK_KEY = 6572746_20261019
CONNECT_TIMEOUT_SECONDS = 10
QUERY_TIMEOUT_MS = 5000
LOCK_WAIT_SECONDS = 90
POLL_SECONDS = 0.2


class StartupImportLockError(RuntimeError):
    """Safe startup failure that does not expose a driver error or DSN."""


@contextmanager
def portal_app_import_lock(environment=None, *, connect=None,
                           clock=time.monotonic, sleeper=time.sleep):
    """Hold the shared gate around the caller's entire application import.

    Local/non-Render imports are unchanged. Render PostgreSQL imports use a
    dedicated autocommit session, with bounded connection/query/lock acquisition.
    The import itself remains supervised by the web process startup timeout.
    An explicitly allowed Render SQLite path also retains its previous behavior.
    No app, environment file or provider is imported to obtain this lock.
    """
    runtime = os.environ if environment is None else environment
    if not runtime.get('RENDER'):
        yield
        return
    database_url = runtime.get('DATABASE_URL', '')
    uses_postgres = isinstance(database_url, str) and database_url.startswith(('postgres://', 'postgresql://'))
    if not uses_postgres:
        required = runtime.get('REQUIRE_POSTGRES_ON_RENDER')
        if required is not None and str(required).strip().lower() not in {'1', 'true', 'yes', 'on'}:
            yield
            return
        raise StartupImportLockError('Web-App nicht gestartet: PostgreSQL-Konfiguration fehlt.') from None
    try:
        database = urlsplit(database_url)
        if not database.hostname or not database.path.strip('/'):
            raise ValueError
    except (TypeError, ValueError):
        raise StartupImportLockError('Web-App nicht gestartet: PostgreSQL-Konfiguration ist ungültig.') from None

    connection = None
    acquired = False
    failure = None
    stage = 'Verbindung'
    try:
        if connect is None:
            import psycopg
            connect = psycopg.connect
        connection = connect(
            database_url, connect_timeout=CONNECT_TIMEOUT_SECONDS, autocommit=True,
            options=f'-c statement_timeout={QUERY_TIMEOUT_MS} -c lock_timeout={QUERY_TIMEOUT_MS}',
        )
        stage = 'Importsperre'
        deadline = clock() + LOCK_WAIT_SECONDS
        while True:
            row = connection.execute('SELECT pg_try_advisory_lock(%s)',
                                     (APP_IMPORT_LOCK_KEY,)).fetchone()
            if row and row[0] is True:
                acquired = True
                break
            if not row or row[0] is not False:
                raise ValueError
            remaining = deadline - clock()
            if remaining <= 0:
                raise TimeoutError
            sleeper(min(POLL_SECONDS, remaining))
        stage = 'App-Import'
        yield
    except Exception:
        # Both driver and import exceptions may contain connection strings.
        failure = StartupImportLockError(f'Web-App nicht gestartet: geschützter App-Import fehlgeschlagen ({stage}).')
    finally:
        if connection is not None:
            cleanup_failed = False
            if acquired:
                try:
                    row = connection.execute('SELECT pg_advisory_unlock(%s)',
                                             (APP_IMPORT_LOCK_KEY,)).fetchone()
                    if not row or row[0] is not True:
                        raise ValueError
                except Exception:
                    cleanup_failed = True
            try:
                connection.close()
            except Exception:
                cleanup_failed = True
            if cleanup_failed:
                failure = StartupImportLockError('Web-App nicht gestartet: Importsperre konnte nicht sauber freigegeben werden.')
    if failure is not None:
        raise failure from None
