"""Local HTTP acceptance with a fresh PostgreSQL database and Stripe TEST only.

Use the private ``.agent-hub/postgres-runtime/connection.json`` for the dedicated
loopback cluster. Every invocation creates a NEW database and leaves it intact
for inspection; this launcher never opens, reuses, or deletes an existing rental
database. It does not make a Stripe request until a test user starts a checkout.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import sys
import tempfile
from unittest.mock import patch
from urllib.parse import quote
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from run_mos_stripe_staging import require_test_credentials, staging_configuration


TEST_HOST = '127.0.0.1'
TEST_PORT = 5087
TEST_ORIGIN = f'http://{TEST_HOST}:{TEST_PORT}'
PG_HOST = '127.0.0.1'
PG_PORT = 55439
PG_USER = 'mos_test_admin'
PG_ADMIN_DB = 'postgres'
PG_NAME_PREFIX = 'mos_stripe_acceptance_'
PRIVATE_ROOT = ROOT / '.agent-hub' / 'postgres-runtime'


def require_cluster_config(cfg):
    if (type(cfg) is not dict or set(cfg) != {'host', 'port', 'user', 'password', 'dbname'}
            or cfg['host'] != PG_HOST or type(cfg['port']) is not int or cfg['port'] != PG_PORT
            or cfg['user'] != PG_USER or cfg['dbname'] != PG_ADMIN_DB
            or not isinstance(cfg['password'], str) or not cfg['password']):
        raise ValueError('Nur 127.0.0.1:55439/mos_test_admin/postgres ist zulässig.')
    return cfg


def load_cluster_config(connection_file):
    """Fail before network access if the private file does not name our cluster."""
    path = Path(connection_file).resolve()
    if not path.is_relative_to(PRIVATE_ROOT.resolve()) or path.suffix != '.json':
        raise ValueError('Nur die private MOS-Testcluster-Verbindungsdatei ist zulässig.')
    if path.stat().st_size > 4096:
        raise ValueError('Ungültige MOS-Testcluster-Verbindungsdatei.')
    return require_cluster_config(json.loads(path.read_text(encoding='utf-8')))


def database_url(cfg, name):
    require_cluster_config(cfg)
    if not re.fullmatch(r'mos_stripe_acceptance_[0-9a-f]{32}', name):
        raise ValueError('Ungültiger neuer Testdatenbankname.')
    return (f"postgresql://{PG_USER}:{quote(cfg['password'], safe='')}@{PG_HOST}:{PG_PORT}/{name}")


def fresh_test_database(cfg):
    """CREATE only, with a random name. A collision fails; no DROP/overwrite path."""
    require_cluster_config(cfg)
    import psycopg
    from psycopg import sql

    name = PG_NAME_PREFIX + secrets.token_hex(16)
    with psycopg.connect(**cfg, autocommit=True, connect_timeout=5) as admin:
        row = admin.execute('SELECT current_user, current_database(), inet_server_addr()::text, inet_server_port()').fetchone()
        try:
            server_ip = str(ipaddress.ip_interface(row[2]).ip)
        except (TypeError, ValueError):
            server_ip = None
        if row[0] != PG_USER or row[1] != PG_ADMIN_DB or server_ip != PG_HOST or row[3] != PG_PORT:
            raise RuntimeError('Der lokale PostgreSQL-Testcluster stimmt nicht überein.')
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    return name


def clean_environment(directory, db_url):
    """Only OS process essentials and explicit inert app settings survive."""
    directory = Path(directory).resolve()
    keep = {key: os.environ[key] for key in ('SystemRoot', 'WINDIR', 'PATH', 'TEMP', 'TMP') if key in os.environ}
    keep.update({
        'RENDER': 'isolated-mos-pg-test', 'DATABASE_URL': db_url,
        'REQUIRE_POSTGRES_ON_RENDER': '1', 'DATA_DIR': str(directory),
        'UPLOAD_DIR': str(directory / 'uploads'), 'BACKUP_DIR': str(directory / 'backups'),
        'DELETED_UPLOAD_DIR': str(directory / 'deleted'),
        'AUTO_BACKUP_ENABLED': '0', 'AUTO_CHANGE_BACKUP_ENABLED': '0',
        'GOOGLE_ADS_AUTO_SYNC_ENABLED': '0', 'MAILBOX_SEND_ENABLED': '0',
        'MAILBOX_WRITE_ENABLED': '0', 'MAILBOX_MOVE_ENABLED': '0',
        'PUBLIC_SITE_INDEXABLE': '0', 'ADMIN_PASS': secrets.token_urlsafe(32),
        'FLASK_SECRET_KEY': secrets.token_urlsafe(48),
        'MOS_BOOKING_CONFIG_FILE': '', 'MOS_STRIPE_TEST_KEY': '',
        'MOS_STRIPE_LIVE_KEY': '', 'MOS_STRIPE_WEBHOOK_SECRET': '',
    })
    return keep


@contextmanager
def isolated_process_environment(values):
    """Prevent inherited live app/PG/mail/Stripe variables and local dotenv reads."""
    exists = Path.exists
    with patch.dict(os.environ, values, clear=True), patch.object(
            Path, 'exists', lambda p: False if p in (ROOT / '.env', ROOT / '.env.local') else exists(p)):
        yield


def build_staging_app(directory, cfg, name, credentials):
    """Import the real portal against the newly created database, then seed fake cars."""
    if 'app' in sys.modules:
        raise RuntimeError('PostgreSQL-Stripe-Teststart benötigt einen frischen Python-Prozess.')
    directory = Path(directory).resolve()
    if any(directory.iterdir()):
        raise ValueError('Stripe-PostgreSQL-Testverzeichnis muss leer sein.')
    url = database_url(cfg, name)
    with isolated_process_environment(clean_environment(directory, url)):
        import app as portal

        if not portal.USE_POSTGRES or portal.DATABASE_URL != url:
            raise RuntimeError('PostgreSQL-Testdatenbank wurde nicht isoliert geladen.')
        db = portal.get_db()
        try:
            fleet = {}
            for slug, label, cents in [('kona', 'Hyundai KONA N Line X', 5900),
                                       ('i10', 'Hyundai i10', 3900)]:
                vehicle_id = db.execute('''INSERT INTO mietfahrzeuge
                    (kennzeichen,bezeichnung,erstellt_am,geaendert_am)
                    VALUES (?,?,?,?) RETURNING id''',
                    ('TEST-' + slug, 'TESTDATENSATZ ' + label,
                     portal.now_str(), portal.now_str())).fetchone()['id']
                fleet[slug] = {'id': vehicle_id, 'daily_cents': cents}
            db.commit()
        finally:
            db.close()
        fleet['kona'].update(discount_after_days=3, discount_cents=4900)
        first = (datetime.now(ZoneInfo('Europe/Berlin')) + timedelta(days=2)).replace(
            hour=9, minute=0, second=0, microsecond=0)
        offline = {
            'enabled': True, 'test_configuration': True, 'mode': 'offline',
            'origin': TEST_ORIGIN, 'fleet': fleet,
            'slots': [(first + timedelta(days=n)).isoformat() for n in range(5)],
            'terms_version': 'draft:ENTWURF-2026-09-23-02',
            'vat_included': True, 'day_rule': 'elapsed_24h_ceil',
            'included_km_day': 150, 'extra_km_cents': 25, 'max_days': 30,
            'deposit_cents': 50000, 'deductible_cents': 100000,
            'terms_text': ('TESTENTWURF, keine verbindlichen Mietbedingungen. '
                           'Persönliche Übergabe, voll/voll, 500 Euro Kautionsautorisierung, '
                           '1.000 Euro vertragliche Selbstbeteiligung. Kein Livebetrieb.'),
        }
        booking = staging_configuration(offline)
        booking.update(origin=TEST_ORIGIN, postgres_test_database=name)
        portal.app.config['MOS_PUBLIC_BOOKING'] = booking
        portal.app.config.update(
            MOS_PUBLIC_STRIPE_TEST_KEY=credentials['MOS_STRIPE_TEST_KEY'],
            MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY=credentials['MOS_STRIPE_PUBLISHABLE_KEY'],
            MOS_PUBLIC_WEBHOOK_SECRET=credentials['MOS_STRIPE_WEBHOOK_SECRET'],
            MOS_PUBLIC_STRIPE_LIVE_KEY='', MAILBOX_SEND_ENABLED=False,
        )
        from mos_public_booking import isolated_postgres_stripe_test
        if not isolated_postgres_stripe_test(portal, booking):
            raise RuntimeError('PostgreSQL-Stripe-Testgate abgelehnt.')
        return portal


def main(argv=None):
    parser = argparse.ArgumentParser(description='Isolierte MOS PostgreSQL+Stripe-Testseite')
    parser.add_argument('--connection-file', required=True)
    args = parser.parse_args(argv)
    credentials = require_test_credentials(os.environ)
    cfg = load_cluster_config(args.connection_file)
    directory = Path(tempfile.mkdtemp(prefix='mos-stripe-pg-staging-'))
    # The environment stays clean for the whole HTTP server lifetime.
    with isolated_process_environment(clean_environment(directory, '')):
        name = fresh_test_database(cfg)
        with isolated_process_environment(clean_environment(directory, database_url(cfg, name))):
            portal = build_staging_app(directory, cfg, name, credentials)
            print(f'NUR TESTDATEN: {TEST_ORIGIN}/mietwagen-test/ – Datenbank {name}')
            portal.app.run(host=TEST_HOST, port=TEST_PORT, debug=False, use_reloader=False)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Database and Stripe connection errors may contain secret-bearing DSNs.
        print('MOS PostgreSQL-Stripe-Teststart fehlgeschlagen:', type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
