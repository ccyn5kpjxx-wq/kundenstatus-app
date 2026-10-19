"""Synthetic context-boundary tests, never real payment or PostgreSQL acceptance."""

from copy import deepcopy
import inspect
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase, main
from unittest.mock import patch

from flask import Flask

from mos_public_booking import register
from scripts.test_mos_render_reconcile import fixture_config


ROOT = Path(__file__).resolve().parents[1]


class PublicTerminationGateTests(TestCase):
    def setUp(self):
        def deny(*args, **kwargs):
            raise AssertionError('External network forbidden')
        for name in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.create_connection'):
            blocked = patch(name, deny)
            blocked.start()
            self.addCleanup(blocked.stop)
        self.tmp = TemporaryDirectory(prefix='mos-http-termination-gate-')
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'synthetic.sqlite3'
        cfg = fixture_config()  # Explicit synthetic approvals, not production evidence.
        cfg['launch']['termination_review']['applies'] = True
        cfg.update(vat_included=True, day_rule='elapsed_24h_ceil', deposit_cents=50000,
                   deductible_cents=100000, included_km_day=150, extra_km_cents=25,
                   max_days=5, slots=[])
        cfg['fleet']['kona']['daily_cents'] = 5900
        cfg['fleet']['i10']['daily_cents'] = 3900
        self.app = Flask(__name__, template_folder=str(ROOT / 'templates'))
        self.app.secret_key = 'synthetic-context-gate-' + 'x' * 40
        self.app.config.update(TESTING=True, MOS_PUBLIC_BOOKING=cfg, MOS_TERMINATION_ENABLED=False,
                               MOS_PUBLIC_STRIPE_LIVE_KEY='rk_live_SYNTHETIC',
                               MOS_PUBLIC_STRIPE_PUBLISHABLE_KEY='pk_live_SYNTHETIC',
                               MOS_PUBLIC_WEBHOOK_SECRET='whsec_SYNTHETIC')
        self.app.jinja_env.globals['csrf_field'] = lambda: ''
        self.app.add_url_rule('/impressum', 'impressum_seite', lambda: '')
        self.app.add_url_rule('/datenschutz', 'datenschutz_seite', lambda: '')
        self.app.add_url_rule('/mieten/kuendigen', 'mos_termination.form', lambda: '')
        def get_db():
            db = sqlite3.connect(self.path)
            db.row_factory = sqlite3.Row
            return db
        # Minimal DB/gateway seams test setup routing only; not a PG integration claim.
        self.portal = SimpleNamespace(app=self.app, USE_POSTGRES=True, DB=str(self.path), get_db=get_db,
                                      USING_EPHEMERAL_SECRET_KEY=False, USING_GENERATED_FLASK_SECRET_KEY=False,
                                      admin_required=lambda function: function)
        with patch.dict('os.environ', {'MOS_BOOKING_CONFIG_FILE': ''}):
            register(self.portal)
        guard = self.app.before_request_funcs['mos_public'][0]
        self.setup = inspect.getclosurevars(guard).nonlocals['setup']
        client_patch = patch('stripe.StripeClient')
        client_patch.start()
        self.addCleanup(client_patch.stop)
        self.client = self.app.test_client()

    def test_cli_can_load_shared_checkout_with_public_route_off(self):
        with self.app.app_context():
            state = self.setup()
        self.assertEqual(type(state['service']).__name__, 'SharedCheckout')
        self.assertFalse(self.app.config['MOS_TERMINATION_ENABLED'])
        self.assertTrue(state['cfg']['launch']['termination_review']['applies'])

    def test_http_get_and_post_without_route_flag_are_blocked_before_initialization(self):
        for method, path in (('GET', '/mieten/'), ('POST', '/mieten/quote')):
            with self.subTest(method=method):
                response = self.client.open(path, method=method, base_url='https://booking.example.invalid')
                self.assertEqual(response.status_code, 409)
                self.assertIn('Öffentlicher Kündigungsweg muss', response.get_data(as_text=True))
                self.assertNotIn('mos_public_booking', self.app.extensions)

    def test_http_get_and_post_without_route_flag_stay_blocked_after_cli_cache(self):
        with self.app.app_context():
            cached = self.setup()
        for method, path in (('GET', '/mieten/'), ('POST', '/mieten/quote')):
            with self.subTest(method=method):
                response = self.client.open(path, method=method, base_url='https://booking.example.invalid')
                self.assertEqual(response.status_code, 409)
                self.assertIn('Öffentlicher Kündigungsweg muss', response.get_data(as_text=True))
                self.assertIs(self.app.extensions['mos_public_booking'], cached)

    def test_http_with_route_flag_reaches_normal_page_post_setup_and_signature_validation(self):
        self.app.config['MOS_TERMINATION_ENABLED'] = True
        response = self.client.get('/mieten/', base_url='https://booking.example.invalid')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Auto wählen', response.get_data(as_text=True))
        with self.app.test_request_context('/mieten/quote', method='POST', base_url='https://booking.example.invalid'):
            self.assertIs(self.setup(), self.app.extensions['mos_public_booking'])
        response = self.client.post('/mieten/webhook', data=b'{}', base_url='https://booking.example.invalid')
        self.assertEqual(response.status_code, 400)
        self.assertNotIn('Öffentlicher Kündigungsweg muss', response.get_data(as_text=True))

    def test_cli_keeps_terms_insurance_key_and_postgres_gates(self):
        original = deepcopy(self.app.config['MOS_PUBLIC_BOOKING'])
        for case in ('terms', 'insurance', 'key', 'postgres'):
            with self.subTest(case=case):
                self.app.config['MOS_PUBLIC_BOOKING'] = deepcopy(original)
                self.app.extensions.pop('mos_public_booking', None)
                self.app.config['MOS_PUBLIC_STRIPE_LIVE_KEY'] = 'rk_live_SYNTHETIC'
                self.portal.USE_POSTGRES = True
                if case == 'terms':
                    self.app.config['MOS_PUBLIC_BOOKING']['terms_text'] = 'changed synthetic terms'
                elif case == 'insurance':
                    self.app.config['MOS_PUBLIC_BOOKING']['launch']['insurance'].pop('i10')
                elif case == 'key':
                    self.app.config['MOS_PUBLIC_STRIPE_LIVE_KEY'] = 'sk_test_SYNTHETIC'
                else:
                    self.portal.USE_POSTGRES = False
                with self.app.app_context(), self.assertRaises(ValueError):
                    self.setup()


if __name__ == '__main__':
    main()
