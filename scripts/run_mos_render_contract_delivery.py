"""Fail-closed entry point for a future Render MOS contract-delivery worker.

This does not create a Render service or enable mail delivery by itself.
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


class DeliveryPreflightError(ValueError):
    pass


def _on(value):
    return str(value).lower() in {'1', 'true'}


def preflight(environment: dict[str, str], *, secret_root: Path = SECRET_ROOT) -> dict:
    """Validate the live destination and explicit mail authority before app import."""
    if not environment.get('RENDER'):
        raise DeliveryPreflightError('Nur auf Render ausführen.')
    if environment.get('PUBLIC_SITE_ONLY', '').lower() not in {'0', 'false'}:
        raise DeliveryPreflightError('Portalmodus ist erforderlich.')
    if not _on(environment.get('REQUIRE_POSTGRES_ON_RENDER')):
        raise DeliveryPreflightError('PostgreSQL-Pflicht ist erforderlich.')
    try:
        database = urlsplit(environment.get('DATABASE_URL', ''))
    except (TypeError, ValueError) as exc:
        raise DeliveryPreflightError('Autoritative PostgreSQL-Verbindung fehlt.') from exc
    if (database.scheme not in {'postgres', 'postgresql'} or not database.hostname
            or not database.path.strip('/') or database.hostname in {'localhost', '127.0.0.1', '::1'}):
        raise DeliveryPreflightError('Autoritative PostgreSQL-Verbindung fehlt.')
    path_value = environment.get('MOS_BOOKING_CONFIG_FILE', '')
    if not path_value:
        raise DeliveryPreflightError('MOS_BOOKING_CONFIG_FILE fehlt.')
    try:
        path = Path(path_value).resolve()
        if not path.is_relative_to(secret_root.resolve()) or not path.is_file() or path.stat().st_size > 1_000_000:
            raise DeliveryPreflightError('MOS-Konfiguration muss als Render Secret File vorliegen.')
        config = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DeliveryPreflightError('MOS-Konfiguration ist nicht lesbar oder ungültig.') from exc
    if not isinstance(config, dict) or config.get('mode') != 'live':
        raise DeliveryPreflightError('Live-Konfiguration fehlt.')
    try:
        origin = urlsplit(config.get('origin', ''))
    except (TypeError, ValueError) as exc:
        raise DeliveryPreflightError('HTTPS-Origin fehlt.') from exc
    if (origin.scheme != 'https' or not origin.hostname or origin.path
            or origin.query or origin.fragment or origin.username or origin.password):
        raise DeliveryPreflightError('HTTPS-Origin fehlt.')
    if environment.get('MOS_CONTRACT_EMAIL_ENABLED') != '1':
        raise DeliveryPreflightError('MOS-Vertragsversand ist nicht ausdrücklich aktiviert.')
    if environment.get('MOS_STRIPE_TEST_KEY'):
        raise DeliveryPreflightError('Testschlüssel darf nicht in der Live-Umgebung stehen.')
    secret = environment.get('FLASK_SECRET_KEY', '')
    if len(secret) < 32 or secret in {'change-me', 'gaertner-autohaus-2026'}:
        raise DeliveryPreflightError('Dauerhafter Flask-Schlüssel fehlt.')
    account = environment.get('MAIL_SMTP_USER', '')
    if (not account or '@' not in account or account != environment.get('MAIL_IMAP_USER')
            or not environment.get('MAIL_SMTP_HOST') or not environment.get('MAIL_SMTP_PASS')):
        raise DeliveryPreflightError('Expliziter Werkstatt-SMTP-Zugang fehlt.')
    if _on(environment.get('MAIL_SMTP_SSL')) == _on(environment.get('MAIL_SMTP_TLS')):
        raise DeliveryPreflightError('Genau eine verschlüsselte SMTP-Verbindung ist erforderlich.')
    try:
        port = int(environment.get('MAIL_SMTP_PORT', ''))
    except (TypeError, ValueError) as exc:
        raise DeliveryPreflightError('SMTP-Port fehlt.') from exc
    if not 1 <= port <= 65535:
        raise DeliveryPreflightError('SMTP-Port ist ungültig.')
    return config


def run(environment: dict[str, str] | None = None, *, secret_root: Path = SECRET_ROOT) -> int:
    runtime = dict(os.environ if environment is None else environment)
    try:
        preflight(runtime, secret_root=secret_root)
    except DeliveryPreflightError as exc:
        print(f'MOS-Vertragsversand nicht gestartet: {exc}', file=sys.stderr)
        return 2
    runtime.update({
        'DATA_DIR': '/tmp/mos-contract-delivery',
        'UPLOAD_DIR': '/tmp/mos-contract-delivery/uploads',
        'BACKUP_DIR': '/tmp/mos-contract-delivery/backups',
        'DELETED_UPLOAD_DIR': '/tmp/mos-contract-delivery/deleted',
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
    for key in ('MOS_STRIPE_LIVE_KEY', 'MOS_STRIPE_PUBLISHABLE_KEY', 'MOS_STRIPE_WEBHOOK_SECRET'):
        runtime.pop(key, None)  # The mail worker never contacts Stripe.
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'flask', '--app', 'app', 'mos-contract-delivery'],
            cwd=ROOT, env=runtime, timeout=TIMEOUT_SECONDS, check=False,
        )
    except subprocess.TimeoutExpired:
        print('MOS-Vertragsversand hat die maximale Laufzeit überschritten.', file=sys.stderr)
        return 124
    except OSError:
        print('MOS-Vertragsversand konnte nicht gestartet werden.', file=sys.stderr)
        return 127
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(run())
