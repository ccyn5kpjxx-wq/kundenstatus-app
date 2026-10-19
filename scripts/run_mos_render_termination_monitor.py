"""Fail-closed Render check for unanswered MOS termination declarations.

This prepares a scheduled alarm. It neither creates a Render service nor sends
mail, changes a rental, or accesses Stripe.
"""

import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
TIMEOUT_SECONDS = 90
READ_ONLY_CONNECT_TIMEOUT_SECONDS = 10
READ_ONLY_QUERY_TIMEOUT_MS = 10000
READ_ONLY_LOCK_TIMEOUT_MS = 3000


def check_read_only(environment=None, *, connect=None):
    """Count existing declarations without Flask/app import, mail access or writes.

    Used by the shared operations cron, which intentionally has no SMTP access.
    Missing tables and database failures are alarms, never an assumed empty queue.
    """
    runtime = os.environ if environment is None else environment
    try:
        if (not runtime.get('RENDER') or runtime.get('MOS_TERMINATION_MONITOR_ENABLED') != '1'
                or runtime.get('REQUIRE_POSTGRES_ON_RENDER', '').lower() not in {'1', 'true'}):
            raise ValueError
        dsn = runtime.get('DATABASE_URL', '')
        database = urlsplit(dsn)
        if (database.scheme not in {'postgres', 'postgresql'} or not database.hostname
                or database.hostname in {'localhost', '127.0.0.1', '::1'}
                or not database.path.strip('/')):
            raise ValueError
    except (TypeError, ValueError):
        print('MOS-Kündigungsalarm: sichere PostgreSQL-Monitor-Konfiguration fehlt.', file=sys.stderr)
        return 2
    connection = None
    try:
        if connect is None:
            import psycopg
            connect = psycopg.connect
        connection = connect(
            dsn, connect_timeout=READ_ONLY_CONNECT_TIMEOUT_SECONDS,
            options=('-c default_transaction_read_only=on '
                     f'-c statement_timeout={READ_ONLY_QUERY_TIMEOUT_MS} '
                     f'-c lock_timeout={READ_ONLY_LOCK_TIMEOUT_MS}'),
            autocommit=True,
        )
        cursor = connection.cursor()
        cursor.execute('BEGIN READ ONLY')
        cursor.execute("SELECT COUNT(*) FROM miet_checkout_terminations WHERE mail_status!='sent'")
        row = cursor.fetchone()
        if not row or type(row[0]) is not int or row[0] < 0:
            raise ValueError
        count = row[0]
        print(f'Offene MOS-Kündigungsbestätigungen: {count}')
        return 1 if count else 0
    except Exception:
        # Driver exceptions can include credentials/connection strings: never emit details.
        print('MOS-Kündigungsalarm: Datenbankprüfung fehlgeschlagen.', file=sys.stderr)
        return 2
    finally:
        if connection is not None:
            try:
                connection.rollback()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass


def preflight(environment):
    if not environment.get('RENDER'):
        raise ValueError('Nur auf Render ausführen.')
    if environment.get('MOS_TERMINATION_ENABLED') != '1':
        raise ValueError('Öffentlicher Kündigungsweg ist nicht aktiviert.')
    if environment.get('MOS_TERMINATION_EMAIL_ENABLED') != '1':
        raise ValueError('Sofortige E-Mail-Bestätigung ist nicht aktiviert.')
    if environment.get('REQUIRE_POSTGRES_ON_RENDER', '').lower() not in {'1', 'true'}:
        raise ValueError('PostgreSQL-Pflicht fehlt.')
    try:
        database = urlsplit(environment.get('DATABASE_URL', ''))
        origin = urlsplit(environment.get('MOS_TERMINATION_ORIGIN', ''))
    except (TypeError, ValueError) as exc:
        raise ValueError('Datenbank oder HTTPS-Origin fehlt.') from exc
    if (database.scheme not in {'postgres', 'postgresql'} or not database.hostname
            or database.hostname in {'localhost', '127.0.0.1', '::1'}
            or not database.path.strip('/')):
        raise ValueError('Autoritative PostgreSQL-Datenbank fehlt.')
    if (origin.scheme != 'https' or not origin.hostname or origin.path not in ('', '/')
            or origin.query or origin.fragment or origin.username or origin.password):
        raise ValueError('Öffentliche HTTPS-Origin fehlt.')
    if environment.get('SESSION_COOKIE_SECURE', '').lower() in {'0', 'false', 'no'}:
        raise ValueError('Sichere Sitzungscookies sind ausgeschaltet.')
    secret = environment.get('FLASK_SECRET_KEY', '')
    if len(secret) < 32 or secret in {'change-me', 'gaertner-autohaus-2026'}:
        raise ValueError('Dauerhafter Flask-Schlüssel fehlt.')


def run(environment=None, *, runner=subprocess.run):
    runtime = dict(os.environ if environment is None else environment)
    try:
        preflight(runtime)
    except ValueError as exc:
        print(f'MOS-Kündigungsalarm nicht gestartet: {exc}', file=sys.stderr)
        return 2
    runtime.update({
        'DATA_DIR': '/tmp/mos-termination-monitor',
        'UPLOAD_DIR': '/tmp/mos-termination-monitor/uploads',
        'BACKUP_DIR': '/tmp/mos-termination-monitor/backups',
        'DELETED_UPLOAD_DIR': '/tmp/mos-termination-monitor/deleted',
        'AUTO_BACKUP_ON_STARTUP': 'false',
        'AUTO_BACKUP_ENABLED': 'false',
        'AUTO_CHANGE_BACKUP_ENABLED': 'false',
        'MAILBOX_SEND_ENABLED': 'false',
        'MAILBOX_WRITE_ENABLED': 'false',
        'MAILBOX_MOVE_ENABLED': 'false',
        'MAILBOX_FLAGS_ENABLED': 'false',
        'GOOGLE_ADS_AUTO_SYNC_ENABLED': 'false',
        'LEXWARE_API_KEY': '',
        'CODEX_BRIDGE_ENABLED': 'false',
    })
    try:
        result = runner([sys.executable, '-m', 'flask', '--app', 'app', 'mos-termination-check'],
                        cwd=ROOT, env=runtime, timeout=TIMEOUT_SECONDS, check=False)
    except subprocess.TimeoutExpired:
        print('MOS-Kündigungsalarm hat die maximale Laufzeit überschritten.', file=sys.stderr)
        return 124
    except OSError:
        print('MOS-Kündigungsalarm konnte nicht gestartet werden.', file=sys.stderr)
        return 127
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(run())
