"""Fail-closed entry point for a future Render MOS reconciliation cron job.

This module does not create a Render service or enable customer bookings.  The
private MOS config must be a Render Secret File shared with the portal service.
Its nonzero exit status is intended for Render's cron failure notification.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
SECRET_ROOT = Path('/etc/secrets')
TIMEOUT_SECONDS = 900


class ReconcilePreflightError(ValueError):
    """Unsafe or incomplete Render runtime; never launch the Flask command."""


def _required_value(environment: dict[str, str], name: str, prefixes: tuple[str, ...]) -> None:
    value = environment.get(name, '')
    if not isinstance(value, str) or not value.startswith(prefixes) or value != value.strip() or '\n' in value:
        raise ReconcilePreflightError(f'{name} fehlt oder hat den falschen Modus.')


def preflight(environment: dict[str, str], *, secret_root: Path = SECRET_ROOT) -> dict:
    """Check configuration without importing the portal or contacting Stripe/PG."""
    if not environment.get('RENDER'):
        raise ReconcilePreflightError('Nur auf Render ausführen.')
    if environment.get('PUBLIC_SITE_ONLY', '').lower() not in {'0', 'false'}:
        raise ReconcilePreflightError('Portalmodus ist erforderlich.')
    if environment.get('REQUIRE_POSTGRES_ON_RENDER', '').lower() not in {'1', 'true'}:
        raise ReconcilePreflightError('PostgreSQL-Pflicht ist erforderlich.')
    try:
        database = urlsplit(environment.get('DATABASE_URL', ''))
    except (TypeError, ValueError) as exc:
        raise ReconcilePreflightError('Autoritative PostgreSQL-Verbindung fehlt.') from exc
    if database.scheme not in {'postgres', 'postgresql'} or not database.hostname or not database.path.strip('/'):
        raise ReconcilePreflightError('Autoritative PostgreSQL-Verbindung fehlt.')
    if database.hostname in {'localhost', '127.0.0.1', '::1'}:
        raise ReconcilePreflightError('Lokale Testdatenbank ist auf Render unzulässig.')

    path_value = environment.get('MOS_BOOKING_CONFIG_FILE', '')
    if not path_value:
        raise ReconcilePreflightError('MOS_BOOKING_CONFIG_FILE fehlt.')
    try:
        config_path = Path(path_value).resolve()
        if not config_path.is_relative_to(secret_root.resolve()) or not config_path.is_file():
            raise ReconcilePreflightError('MOS-Konfiguration muss als Render Secret File vorliegen.')
        if config_path.stat().st_size > 1_000_000:
            raise ReconcilePreflightError('MOS-Konfiguration ist zu groß.')
    except OSError as exc:
        raise ReconcilePreflightError('MOS-Konfiguration ist nicht lesbar.') from exc
    try:
        config = json.loads(config_path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReconcilePreflightError('MOS-Konfiguration ist nicht lesbar oder ungültig.') from exc
    if not isinstance(config, dict) or config.get('mode') != 'live' or config.get('enabled') is not True:
        raise ReconcilePreflightError('Freigegebene Live-Konfiguration fehlt.')
    try:
        origin = urlsplit(config.get('origin', ''))
    except (TypeError, ValueError) as exc:
        raise ReconcilePreflightError('HTTPS-Origin der Livebuchung fehlt.') from exc
    if origin.scheme != 'https' or not origin.hostname or origin.path or origin.query or origin.fragment:
        raise ReconcilePreflightError('HTTPS-Origin der Livebuchung fehlt.')

    from mos_booking.production import launch_errors

    try:
        missing = launch_errors(config)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ReconcilePreflightError('Buchungsfreigaben sind ungültig.') from exc
    if missing:
        raise ReconcilePreflightError('Buchungsfreigaben unvollständig: ' + '; '.join(missing))
    _required_value(environment, 'MOS_STRIPE_LIVE_KEY', ('sk_live_', 'rk_live_'))
    _required_value(environment, 'MOS_STRIPE_PUBLISHABLE_KEY', ('pk_live_',))
    _required_value(environment, 'MOS_STRIPE_WEBHOOK_SECRET', ('whsec_',))
    if environment.get('MOS_STRIPE_TEST_KEY'):
        raise ReconcilePreflightError('Testschlüssel darf nicht in der Live-Umgebung stehen.')
    secret = environment.get('FLASK_SECRET_KEY', '')
    if len(secret) < 32 or secret in {'change-me', 'gaertner-autohaus-2026'}:
        raise ReconcilePreflightError('Dauerhafter Flask-Schlüssel fehlt.')
    return config


def run(environment: dict[str, str] | None = None, *, secret_root: Path = SECRET_ROOT) -> int:
    runtime = dict(os.environ if environment is None else environment)
    try:
        preflight(runtime, secret_root=secret_root)
    except ReconcilePreflightError as exc:
        print(f'MOS-Abgleich nicht gestartet: {exc}', file=sys.stderr)
        return 2

    # Importing app initializes local directories.  The cron instance has no
    # persistent disk; payment state remains exclusively in Render Postgres.
    runtime.update({
        'DATA_DIR': '/tmp/mos-reconcile',
        'UPLOAD_DIR': '/tmp/mos-reconcile/uploads',
        'BACKUP_DIR': '/tmp/mos-reconcile/backups',
        'DELETED_UPLOAD_DIR': '/tmp/mos-reconcile/deleted',
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
        result = subprocess.run(
            [sys.executable, '-m', 'flask', '--app', 'app', 'mos-booking-reconcile'],
            cwd=ROOT, env=runtime, timeout=TIMEOUT_SECONDS, check=False,
        )
    except subprocess.TimeoutExpired:
        print('MOS-Abgleich hat die maximale Laufzeit überschritten.', file=sys.stderr)
        return 124
    except OSError:
        print('MOS-Abgleich konnte nicht gestartet werden.', file=sys.stderr)
        return 127
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(run())
