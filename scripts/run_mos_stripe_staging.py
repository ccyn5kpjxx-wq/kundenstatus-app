"""Isolated localhost Stripe TEST preview with synthetic rental vehicles only.

Set the three MOS_STRIPE_* test credentials in the process environment before
starting this script. A local Stripe CLI listener may forward test events to
``http://127.0.0.1:5086/mietwagen-test/webhook`` and supply its signing secret.
The webhook secret must belong to a Stripe test endpoint:
Stripe uses the same ``whsec_`` prefix for test and live webhook secrets, so
the endpoint's mode cannot be inferred from the string alone.
"""

from copy import deepcopy
from pathlib import Path
import os
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from run_mos_public_test import build_test_app


TEST_HOST = '127.0.0.1'
TEST_PORT = 5086
TEST_ORIGIN = f'http://{TEST_HOST}:{TEST_PORT}'
TEST_DB_NAME = 'portal.mos-public-test.sqlite3'


def require_test_credentials(environ):
    """Return in-memory test credentials, failing before any app/DB setup."""
    if environ.get('MOS_STRIPE_LIVE_KEY'):
        raise ValueError('Live-Schlüssel im Testprozess nicht zulässig.')
    expected = {
        'MOS_STRIPE_TEST_KEY': ('sk_test_', 'rk_test_'),
        'MOS_STRIPE_PUBLISHABLE_KEY': ('pk_test_',),
        'MOS_STRIPE_WEBHOOK_SECRET': ('whsec_',),
    }
    values = {}
    for name, prefixes in expected.items():
        value = environ.get(name, '')
        if (not isinstance(value, str) or not any(value.startswith(prefix) and len(value) > len(prefix)
                for prefix in prefixes) or any(char.isspace() for char in value)):
            raise ValueError(f'{name} fehlt oder passt nicht zur Testkonfiguration.')
        values[name] = value
    return values


def staging_configuration(offline_config):
    """Preserve the synthetic fleet and draft terms from the offline starter."""
    if (offline_config.get('mode') != 'offline' or offline_config.get('test_configuration') is not True
            or set(offline_config.get('fleet', {})) != {'i10', 'kona'}):
        raise ValueError('Isolierte Offline-Testflotte erforderlich.')
    config = deepcopy(offline_config)
    config.update(enabled=True, mode='stripe_test', origin=TEST_ORIGIN,
                  deposit_method='card_authorization_at_booking',
                  cancellation_policy='free_48h_then_10pct_rent')
    return config


def build_staging_app(directory, credentials):
    """Create a fresh disposable portal; no Stripe request occurs here."""
    if 'app' in sys.modules:
        raise RuntimeError('Stripe-Teststart benötigt einen frischen Python-Prozess.')
    directory = Path(directory).resolve()
    if any(directory.iterdir()):
        raise ValueError('Stripe-Testverzeichnis muss leer sein.')
    portal = build_test_app(directory, origin=TEST_ORIGIN)
    if (portal.USE_POSTGRES or Path(portal.DB).resolve() != directory / TEST_DB_NAME
            or not portal.app.config['MOS_PUBLIC_BOOKING'].get('test_configuration')):
        raise RuntimeError('Testdatenbank oder Testkonfiguration ist nicht isoliert.')
    portal.app.config['MOS_PUBLIC_BOOKING'] = staging_configuration(
        portal.app.config['MOS_PUBLIC_BOOKING'])
    portal.app.config.update(
        MOS_PUBLIC_STRIPE_TEST_KEY=credentials['MOS_STRIPE_TEST_KEY'],
        MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY=credentials['MOS_STRIPE_PUBLISHABLE_KEY'],
        MOS_PUBLIC_WEBHOOK_SECRET=credentials['MOS_STRIPE_WEBHOOK_SECRET'],
        MOS_PUBLIC_STRIPE_LIVE_KEY='',
    )
    return portal


def main():
    credentials = require_test_credentials(os.environ)
    directory = Path(tempfile.mkdtemp(prefix='mos-stripe-staging-'))
    portal = build_staging_app(directory, credentials)
    print(f'NUR STRIPE-TESTDATEN: {TEST_ORIGIN}/mietwagen-test/ – {directory}')
    portal.app.run(host=TEST_HOST, port=TEST_PORT, debug=False, use_reloader=False)


if __name__ == '__main__':
    main()
