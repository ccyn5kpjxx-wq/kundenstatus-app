"""Synthetic guest sessions, private delivery context and durable SQLite CAS."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from flask import Flask, request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_buyer import (BuyerError, COOKIE_NAME, SQLiteBuyerBackend, ShopBuyer,
                        create_shop_buyer_blueprint, init_buyer_schema, validate_context)

ORIGIN = 'https://shop.example.test'
ADDRESS = {'recipient': 'Synthetic Buyer', 'street': 'Teststraße 12', 'postcode': '74821',
           'city': 'Mosbach', 'country': 'DE'}
PAYLOAD = {'contact': {'name': 'Synthetic Buyer', 'email': 'buyer@example.test', 'phone': '+49 6261 12345'},
           'billing': ADDRESS, 'delivery': None}


class BuyerChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'buyer-synthetic.sqlite'
        self.now = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
        self.factory = lambda: sqlite3.connect(self.path, timeout=5)
        with closing(self.factory()) as connection:
            init_buyer_schema(connection)
        self.backend = SQLiteBuyerBackend(self.factory, clock=lambda: self.now)
        self.buyer = ShopBuyer(self.backend, public_origin=ORIGIN, mode='test', enabled=True, clock=lambda: self.now)
        self.app = self.make_app(self.buyer)
        self.client = self.app.test_client()

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()

    def make_app(self, buyer):
        app = Flask('synthetic-shop-buyer')
        app.config.update(TESTING=True, SECRET_KEY='synthetic-portal-unrelated', SHOP_BUYER_ENABLED=True)
        app.register_blueprint(create_shop_buyer_blueprint(buyer))

        @app.post('/callback-check')
        def callbacks():
            principal = buyer.authorize_quote(request)
            if principal is None:
                return {'error': 'authorization_required'}, 403
            try:
                return {'principal': principal, 'context': buyer.quote_context(request, principal)}
            except BuyerError as error:
                return {'error': error.code}, 400
        return app

    def establish(self, client=None):
        response = (client or self.client).get('/shop/api/v2/buyer/session', base_url=ORIGIN)
        self.assertEqual(response.status_code, 200)
        return response

    def save(self, csrf, payload=None, client=None, **changes):
        params = {'base_url': ORIGIN, 'json': deepcopy(PAYLOAD if payload is None else payload),
                  'headers': {'Origin': ORIGIN, 'X-CSRF-Token': csrf}}
        params.update(changes)
        return (client or self.client).put('/shop/api/v2/buyer/address', **params)

    def callback(self, csrf, client=None):
        return (client or self.client).post('/callback-check', base_url=ORIGIN,
               headers={'Origin': ORIGIN, 'X-CSRF-Token': csrf})

    def backend_records(self):
        with closing(self.factory()) as connection:
            return connection.execute('SELECT session_key,record_json FROM shop_buyer_sessions_v2').fetchall()

    def test_construction_and_disabled_routes_do_not_open_any_database(self):
        calls = []
        backend = SQLiteBuyerBackend(lambda: calls.append('opened'))
        buyer = ShopBuyer(backend, public_origin=ORIGIN, mode='test')
        app = self.make_app(buyer)
        app.config['SHOP_BUYER_ENABLED'] = False
        self.assertEqual(app.test_client().get('/shop/api/v2/buyer/session', base_url=ORIGIN).status_code, 404)
        app.config['SHOP_BUYER_ENABLED'] = True
        self.assertEqual(app.test_client().get('/shop/api/v2/buyer/session', base_url=ORIGIN).status_code, 503)
        self.assertEqual(calls, [])

    def test_guest_cookie_is_opaque_secure_and_public_projection_contains_no_pii_or_identity(self):
        response = self.establish()
        body, cookie = response.get_json(), response.headers['Set-Cookie']
        self.assertEqual(set(body), {'guest', 'address_ready', 'csrf_token', 'expires_at'})
        self.assertFalse(body['address_ready'])
        for part in ('Secure', 'HttpOnly', 'SameSite=Lax', 'Path=/'):
            self.assertIn(part, cookie)
        self.assertNotIn('Domain=', cookie)
        token = self.client.get_cookie(COOKIE_NAME, domain='shop.example.test').value
        self.assertEqual(len(token), 43)
        records = self.backend_records()
        self.assertEqual(records[0][0], sha256(token.encode('ascii')).hexdigest())
        self.assertNotIn(token, records[0][1])
        self.assertNotIn('sg_', response.get_data(as_text=True))
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(self.establish().get_json()['csrf_token'], body['csrf_token'])
        self.assertEqual(len(self.backend_records()), 1)

    def test_portal_admin_session_never_becomes_buyer_identity(self):
        with self.client.session_transaction() as portal:
            portal.update(admin=True, kunde_id=123, user_id='admin')
        self.assertEqual(self.callback('x' * 43).status_code, 403)
        csrf = self.establish().get_json()['csrf_token']
        self.assertEqual(self.save(csrf).status_code, 200)
        response = self.callback(csrf).get_json()
        self.assertRegex(response['principal'], r'^sg_[a-f0-9]{32}$')
        self.assertNotIn('admin', response['principal'])
        self.assertNotEqual(response['principal'], '123')

    def test_address_normalizes_private_context_and_other_guest_cannot_read_or_use_it(self):
        csrf = self.establish().get_json()['csrf_token']
        payload = deepcopy(PAYLOAD)
        payload['contact']['email'] = 'buyer@EXAMPLE.TEST'
        response = self.save(csrf, payload)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()['address_ready'])
        text = response.get_data(as_text=True)
        for private in ('buyer@example.test', 'Teststraße', '74821', 'Synthetic Buyer', 'sg_'):
            self.assertNotIn(private, text)
        result = self.callback(csrf).get_json()
        expected = validate_context(PAYLOAD)
        self.assertEqual(result['context'], {'vehicle_ref': '', 'delivery_context': expected})
        other = self.app.test_client()
        other_csrf = self.establish(other).get_json()['csrf_token']
        self.assertEqual(self.callback(csrf, other).status_code, 403)
        self.assertEqual(self.callback(other_csrf, other).get_json()['error'], 'buyer_address_required')

    def test_fixed_https_host_origin_and_fetch_site_checks_precede_session_creation(self):
        for base, headers in (('http://shop.example.test', {}), ('https://attacker.invalid', {}),
                              (ORIGIN, {'Origin': 'https://attacker.invalid'}),
                              (ORIGIN, {'Sec-Fetch-Site': 'cross-site'})):
            with self.subTest(base=base, headers=headers):
                result = self.client.get('/shop/api/v2/buyer/session', base_url=base, headers=headers)
                self.assertEqual(result.status_code, 403)
        self.assertEqual(self.backend_records(), [])

    def test_mutations_require_cookie_exact_origin_and_own_csrf(self):
        csrf = self.establish().get_json()['csrf_token']
        for headers in ({}, {'Origin': ORIGIN}, {'Origin': ORIGIN, 'X-CSRF-Token': 'x' * 43},
                        {'Origin': 'https://attacker.invalid', 'X-CSRF-Token': csrf},
                        {'Origin': ORIGIN, 'X-CSRF-Token': csrf, 'Sec-Fetch-Site': 'cross-site'}):
            with self.subTest(headers=headers):
                self.assertEqual(self.save(csrf, headers=headers).status_code, 403)
        self.assertFalse(self.establish().get_json()['address_ready'])
        self.assertEqual(self.save(csrf, client=self.app.test_client()).status_code, 403)

    def test_rejects_duplicate_json_nonfinite_oversized_unknown_and_client_identity_fields(self):
        csrf = self.establish().get_json()['csrf_token']
        headers = {'Origin': ORIGIN, 'X-CSRF-Token': csrf}
        for raw in ('{"contact":{},"contact":{},"billing":{},"delivery":null}',
                    '{"contact":NaN,"billing":{},"delivery":null}', 'x' * 17000,
                    json.dumps({**PAYLOAD, 'customer_id': 'attacker'})):
            with self.subTest(raw=raw[:30]):
                response = self.client.put('/shop/api/v2/buyer/address', base_url=ORIGIN,
                        data=raw, content_type='application/json', headers=headers)
                self.assertEqual(response.status_code, 400)
        self.assertEqual(self.client.put('/shop/api/v2/buyer/address', base_url=ORIGIN,
                data=json.dumps(PAYLOAD), content_type='text/plain', headers=headers).status_code, 400)
        self.assertEqual(self.client.get('/shop/api/v2/buyer/session?email=private', base_url=ORIGIN).status_code, 400)

    def test_germany_address_rules_and_structured_contact_validation(self):
        csrf = self.establish().get_json()['csrf_token']
        edits = [('billing', 'postcode', '00000'), ('billing', 'postcode', '7482'),
                 ('billing', 'country', 'AT'), ('billing', 'street', 'Packstation 123'),
                 ('billing', 'street', 'Teststraße'), ('contact', 'email', 'not-an-email'),
                 ('contact', 'name', 'Bad\r\nInjected'), ('contact', 'phone', 'private<script>')]
        for section, key, value in edits:
            with self.subTest(section=section, key=key, value=value):
                payload = deepcopy(PAYLOAD)
                payload[section][key] = value
                self.assertEqual(self.save(csrf, payload).status_code, 400)
        self.assertFalse(self.establish().get_json()['address_ready'])

    def test_expiry_restarts_guest_without_reusing_old_identity_or_csrf(self):
        csrf = self.establish().get_json()['csrf_token']
        self.save(csrf)
        first = self.callback(csrf).get_json()['principal']
        self.now += timedelta(days=2)
        self.assertEqual(self.callback(csrf).status_code, 403)
        fresh = self.establish().get_json()
        self.assertNotEqual(fresh['csrf_token'], csrf)
        self.assertFalse(fresh['address_ready'])
        self.save(fresh['csrf_token'])
        self.assertNotEqual(self.callback(fresh['csrf_token']).get_json()['principal'], first)

    def test_durable_restart_preserves_same_guest_address_and_csrf(self):
        csrf = self.establish().get_json()['csrf_token']
        self.save(csrf)
        original = self.callback(csrf).get_json()
        token = self.client.get_cookie(COOKIE_NAME, domain='shop.example.test').value
        backend = SQLiteBuyerBackend(self.factory, clock=lambda: self.now)
        buyer = ShopBuyer(backend, public_origin=ORIGIN, mode='test', enabled=True, clock=lambda: self.now)
        restarted = self.make_app(buyer).test_client()
        restarted.set_cookie(COOKIE_NAME, token, domain='shop.example.test')
        self.assertEqual(self.callback(csrf, restarted).get_json(), original)

    def test_test_and_live_guests_cannot_cross_modes(self):
        csrf = self.establish().get_json()['csrf_token']
        self.save(csrf)
        token = self.client.get_cookie(COOKIE_NAME, domain='shop.example.test').value
        live = ShopBuyer(self.backend, public_origin=ORIGIN, mode='live', enabled=True, clock=lambda: self.now)
        client = self.make_app(live).test_client()
        client.set_cookie(COOKIE_NAME, token, domain='shop.example.test')
        self.assertEqual(self.callback(csrf, client).status_code, 403)
        self.assertFalse(self.establish(client).get_json()['address_ready'])

    def test_logout_revokes_server_token_and_cookie(self):
        csrf = self.establish().get_json()['csrf_token']
        self.save(csrf)
        token = self.client.get_cookie(COOKIE_NAME, domain='shop.example.test').value
        response = self.client.delete('/shop/api/v2/buyer/session', base_url=ORIGIN,
                   headers={'Origin': ORIGIN, 'X-CSRF-Token': csrf})
        self.assertEqual(response.get_json(), {'signed_out': True})
        self.assertEqual(self.backend_records(), [])
        self.client.set_cookie(COOKIE_NAME, token, domain='shop.example.test')
        self.assertEqual(self.callback(csrf).status_code, 403)

    def test_concurrent_cas_allows_exactly_one_address_revision_and_keeps_identity(self):
        self.establish()
        key, encoded = self.backend_records()[0]
        initial = json.loads(encoded)
        records = []
        for city in ('Mosbach', 'Heidelberg'):
            value = deepcopy(initial)
            value['revision'] += 1
            value['context'] = validate_context(PAYLOAD)
            value['context']['delivery']['city'] = city
            records.append(value)
        barrier, results = threading.Barrier(2), []
        def write(value):
            barrier.wait(5)
            results.append(self.backend.compare_and_swap(key, 1, value))
        workers = [threading.Thread(target=write, args=(record,)) for record in records]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(5)
            self.assertFalse(worker.is_alive())
        self.assertEqual(sorted(results), [False, True])
        stored = self.backend.load(key)
        self.assertEqual(stored['revision'], 2)
        self.assertEqual(stored['customer_id'], initial['customer_id'])
        tampered = deepcopy(stored)
        tampered['revision'] += 1
        tampered['customer_id'] = 'sg_' + 'f' * 32
        with self.assertRaisesRegex(BuyerError, '^buyer_unavailable$'):
            self.backend.compare_and_swap(key, 2, tampered)

    def test_context_copy_cannot_mutate_retained_address_and_wrong_principal_is_denied(self):
        csrf = self.establish().get_json()['csrf_token']
        self.save(csrf)
        cookie = self.client.get_cookie(COOKIE_NAME, domain='shop.example.test').value
        with self.app.test_request_context('/cart', base_url=ORIGIN, method='POST',
                headers={'Origin': ORIGIN, 'X-CSRF-Token': csrf, 'Cookie': COOKIE_NAME + '=' + cookie}):
            principal = self.buyer.authorize_quote(request)
            context = self.buyer.quote_context(request, principal)
            context['delivery_context']['delivery']['postcode'] = '99999'
            self.assertEqual(self.buyer.quote_context(request, principal)['delivery_context']['delivery']['postcode'], '74821')
            with self.assertRaisesRegex(BuyerError, 'buyer_address_required'):
                self.buyer.quote_context(request, 'sg_' + 'f' * 32)

    def test_backend_failure_has_no_raw_details_and_extra_delivery_rule_fails_closed(self):
        csrf = self.establish().get_json()['csrf_token']
        self.buyer.delivery_rule = lambda context: False
        self.assertEqual(self.save(csrf).status_code, 400)
        self.buyer.delivery_rule = None
        with patch.object(self.backend, 'load', side_effect=RuntimeError('PRIVATE_DB_CREDENTIAL_CANARY')):
            result = self.client.get('/shop/api/v2/buyer/session', base_url=ORIGIN)
            self.assertEqual(result.status_code, 503)
            self.assertNotIn('PRIVATE_DB_CREDENTIAL_CANARY', result.get_data(as_text=True))

    def test_bad_public_origins_and_sqlite_replacement_are_rejected(self):
        for origin in ('http://shop.example.test', 'https://user@shop.example.test',
                       'https://shop.example.test/path', 'https://shop.example.test:444',
                       'https://shop.example.test/?next=evil'):
            with self.subTest(origin=origin), self.assertRaises(BuyerError):
                ShopBuyer(self.backend, public_origin=origin, mode='test')
        backend = SQLiteBuyerBackend(lambda: object())
        with self.assertRaisesRegex(BuyerError, '^buyer_unavailable$'):
            backend.load('a' * 64)


if __name__ == '__main__':
    unittest.main()
