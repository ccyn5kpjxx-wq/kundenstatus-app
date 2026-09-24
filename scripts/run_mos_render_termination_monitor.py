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
