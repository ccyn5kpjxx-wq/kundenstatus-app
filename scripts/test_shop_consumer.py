"""Synthetic immutable terms and no-login withdrawal; no real mail/payment."""
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import re
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask
from shop_buyer import ShopBuyer, SQLiteBuyerBackend, create_shop_buyer_blueprint, init_buyer_schema
from shop_consumer import (COOKIE, DOCS, ConsumerError, ShopConsumer, create_consumer_blueprint, init_consumer_schema)
from shop_order_store import OrderStore, init_schema
from test_shop_order_store import CONTEXT, NOW, quote

ORIGIN = 'https://shop.example.test'


def approved_pack(version='SYNTHETIC-APPROVED-1'):
    return {'version': version, 'approved': True, 'status': 'approved', 'scope': 'mixed_retail',
            'texts': {key: 'SYNTHETIC terms ' + key + ' <unsafe markup>' for key in DOCS},
            'attachments': [{'filename': 'SYNTHETIC.txt', 'mimetype': 'text/plain', 'content': b'SYNTHETIC legal annex'}]}


class ConsumerChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'synthetic.sqlite'
        self.now = NOW
        self.factory = lambda: sqlite3.connect(self.path, timeout=5)
        with closing(self.factory()) as db:
            init_schema(db)
            init_buyer_schema(db)
            init_consumer_schema(db)
        self.store = OrderStore(self.factory, clock=lambda: self.now)
        self.snapshot = quote()
        self.store.save_quote(self.snapshot, expected_quote_hash=self.snapshot['quote_hash'],
                              customer_id='customer-a', mode='test', delivery_context=CONTEXT)
        self.pack = approved_pack()
        self.outbox = Mock()
        self.outbox.send.side_effect = lambda token, message, cfg, **kw: {
            'token': token, 'state': 'sent', 'accepted': [message['To']]}
        self.service = self.consumer()
        self.app = Flask(__name__, root_path=str(ROOT))
        self.app.config['SHOP_CONSUMER_ENABLED'] = True
        self.app.register_blueprint(create_consumer_blueprint(self.service))
        self.client = self.app.test_client()

    def consumer(self, **changes):
        options = dict(mode='test', public_origin=ORIGIN, pack=self.pack, enabled=True,
            withdrawals_enabled=True, clock=lambda: self.now, outbox=self.outbox,
            smtp_config=lambda: {'from_address': 'info@auto-lackierzentrum.de'})
        options.update(changes)
        return ShopConsumer(self.store, self.factory, **options)

    def accept(self, service=None):
        service = self.service if service is None else service
        return service.accept_quote(self.snapshot['quote_hash'], customer_id='customer-a',
            pack_hash=service.pack_hash, terms_accepted=True, consumer=True)

    def open(self, client=None):
        client = self.client if client is None else client
        response = client.get('/shop/widerruf/', base_url=ORIGIN)
        self.assertEqual(response.status_code, 200)
        return re.search('name="csrf" value="([A-Za-z0-9_-]+)"', response.text).group(1), response

    def review(self, csrf, client=None, **changes):
        value = {'csrf': csrf, 'name': 'SYNTHETIC Buyer', 'email': 'synthetic@example.invalid',
                 'contract_reference': 'EXTERNAL REFERENCE NOT IN DATABASE', 'scope': 'Only one selected article'}
        value.update(changes)
        client = self.client if client is None else client
        response = client.post('/shop/widerruf/pruefen', base_url=ORIGIN, headers={'Origin': ORIGIN}, data=value)
        self.assertEqual(response.status_code, 200, response.text)
        token = re.search('name="confirm" value="([A-Za-z0-9_-]+)"', response.text).group(1)
        return token

    def confirm(self, csrf, token, client=None):
        client = self.client if client is None else client
        return client.post('/shop/widerruf/bestaetigen', base_url=ORIGIN, headers={'Origin': ORIGIN},
                           data={'csrf': csrf, 'confirm': token})

    def withdrawal_row(self):
        with closing(self.factory()) as db:
            db.row_factory = sqlite3.Row
            return dict(db.execute('SELECT * FROM shop_withdrawals_v2').fetchone())

    def test_default_does_not_touch_storage_or_activate_checkout(self):
        factory = Mock(side_effect=AssertionError('No construction IO'))
        value = ShopConsumer(self.store, factory, mode='test', public_origin=ORIGIN)
        self.assertFalse(value.readiness()['checkout_ready'])
        self.assertFalse(value.deliver_once())
        factory.assert_not_called()
        self.outbox.send.assert_not_called()

    def test_draft_and_tyres_only_pack_cannot_activate_legal_or_checkout(self):
        for changes in ({'approved': False}, {'status': 'entwurf'}, {'scope': 'tyres_pickup'},
                        {'version': 'entwurf:unapproved'}, {'texts': {}}, {'attachments': [{'content': 'wrong'}]}):
            with self.subTest(changes=changes):
                service = self.consumer(pack={**self.pack, **changes}, delivery_enabled=True)
                self.assertFalse(service.readiness()['legal_ready'])
                self.assertFalse(service.readiness()['checkout_ready'])
                with self.assertRaises(ConsumerError):
                    service.terms()
        self.assertFalse(self.service.readiness()['checkout_ready'])
        self.assertTrue(self.consumer(delivery_enabled=True).readiness()['checkout_ready'])

    def test_acceptance_freezes_pack_once_for_exact_owner_quote_and_mode(self):
        first = self.accept()
        self.now += timedelta(seconds=1)
        self.assertEqual(self.accept(), first)
        self.assertTrue(self.service.require_acceptance(self.snapshot['quote_hash'], customer_id='customer-a'))
        for changes in ({'customer_id': 'different-customer'}, {'pack_hash': 'f' * 64},
                        {'terms_accepted': False}, {'consumer': False}):
            arguments = dict(customer_id='customer-a', pack_hash=self.service.pack_hash, terms_accepted=True, consumer=True)
            arguments.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ConsumerError):
                self.service.accept_quote(self.snapshot['quote_hash'], **arguments)
        with self.assertRaises(ConsumerError):
            self.consumer(mode='live').accept_quote(self.snapshot['quote_hash'], customer_id='customer-a',
                pack_hash=self.service.pack_hash, terms_accepted=True)

    def test_receipt_callback_retains_old_pack_after_change_and_checks_order_owner(self):
        self.accept()
        order = self.store.create_order(self.snapshot['quote_hash'], customer_id='customer-a', mode='test',
                                       idempotency_key='synthetic-consumer-order')
        newer = self.consumer(pack=approved_pack('SYNTHETIC-APPROVED-2'))
        with self.assertRaises(ConsumerError):
            newer.require_acceptance(self.snapshot['quote_hash'], customer_id='customer-a')
        frozen = newer.contract_pack(order['id'], customer_id='customer-a', mode='test')
        self.assertEqual(frozen['version'], self.pack['version'])
        self.assertEqual(frozen['attachments'][0]['content'], b'SYNTHETIC legal annex')
        self.pack['texts']['agb'] = 'LATER CALLER MUTATION'
        self.assertNotIn('LATER CALLER MUTATION', json.dumps(frozen['texts']))
        for customer, mode in (('different-customer', 'test'), ('customer-a', 'live')):
            with self.assertRaises(ConsumerError):
                newer.contract_pack(order['id'], customer_id=customer, mode=mode)
        with closing(self.factory()) as db, self.assertRaises(sqlite3.IntegrityError):
            db.execute("UPDATE shop_consumer_acceptances_v2 SET pack_hash=?", ('f' * 64,))

    def test_expired_quote_does_not_accept(self):
        self.now += timedelta(minutes=20)
        with self.assertRaises(ConsumerError):
            self.accept()

    def test_legal_pages_escape_content_and_use_no_store(self):
        response = self.client.get('/shop/recht/agb', base_url=ORIGIN)
        self.assertEqual(response.status_code, 200)
        self.assertIn('&lt;unsafe markup&gt;', response.text)
        self.assertNotIn('<unsafe markup>', response.text)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(self.client.get('/shop/recht/arbitrary', base_url=ORIGIN).status_code, 404)

    def test_public_two_step_withdrawal_records_exact_content_once_without_login_or_order_lookup(self):
        csrf, response = self.open()
        self.assertIn('Secure', response.headers['Set-Cookie'])
        self.assertIn('HttpOnly', response.headers['Set-Cookie'])
        token = self.review(csrf)
        with closing(self.factory()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_withdrawals_v2').fetchone()[0], 0)
        with patch.object(self.store, 'get_order', side_effect=AssertionError('No order enumeration')):
            first = self.confirm(csrf, token)
            self.assertEqual(first.status_code, 200)
            self.assertEqual(self.confirm(csrf, token).status_code, 200)
        row = self.withdrawal_row()
        self.assertEqual(row['state'], 'manual_review')
        self.assertEqual(row['received_at'], NOW.isoformat())
        declaration = json.loads(row['declaration_json'])
        self.assertEqual(declaration['contract_reference'], 'EXTERNAL REFERENCE NOT IN DATABASE')
        self.assertEqual(declaration['scope'], 'Only one selected article')
        with closing(self.factory()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_withdrawals_v2').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_orders_v2').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_payment_attempts_v2').fetchone()[0], 0)
        self.outbox.send.assert_not_called()

    def test_csrf_origin_cookie_mode_and_expiry_prevent_cross_user_or_forged_declarations(self):
        csrf, _ = self.open()
        token = self.review(csrf)
        other = self.app.test_client()
        self.assertEqual(self.confirm(csrf, token, client=other).status_code, 403)
        self.assertEqual(self.confirm('a' * 43, token).status_code, 403)
        self.assertEqual(self.confirm(csrf, 'b' * 43).status_code, 403)
        response = self.client.post('/shop/widerruf/bestaetigen', base_url=ORIGIN,
            headers={'Origin': 'https://evil.example'}, data={'csrf': csrf, 'confirm': token})
        self.assertEqual(response.status_code, 403)
        self.now += timedelta(minutes=31)
        self.assertEqual(self.confirm(csrf, token).status_code, 403)

    def test_public_withdrawal_can_be_opened_from_an_external_email_or_search_link(self):
        response = self.client.get('/shop/widerruf/', base_url=ORIGIN,
            headers={'Sec-Fetch-Site': 'cross-site', 'Sec-Fetch-Mode': 'navigate'})
        self.assertEqual(response.status_code, 200)
        self.assertIn('Vertrag widerrufen', response.text)

    def test_confirm_uses_server_reviewed_content_not_client_replacement(self):
        csrf, _ = self.open()
        token = self.review(csrf, name='<script>SYNTHETIC</script>')
        self.assertEqual(self.client.post('/shop/widerruf/bestaetigen', base_url=ORIGIN, headers={'Origin': ORIGIN},
            data={'csrf': csrf, 'confirm': token, 'email': 'attacker@example.invalid'}).status_code, 400)
        self.assertEqual(self.confirm(csrf, token).status_code, 200)
        self.assertEqual(json.loads(self.withdrawal_row()['declaration_json'])['email'], 'synthetic@example.invalid')

    def test_real_buyer_csrf_guards_http_terms_acceptance(self):
        app = Flask(__name__, root_path=str(ROOT))
        app.config.update(SHOP_CONSUMER_ENABLED=True, SHOP_BUYER_ENABLED=True)
        buyer = ShopBuyer(SQLiteBuyerBackend(self.factory, clock=lambda: self.now), mode='test',
                          public_origin=ORIGIN, enabled=True, clock=lambda: self.now)
        app.register_blueprint(create_shop_buyer_blueprint(buyer))
        app.register_blueprint(create_consumer_blueprint(self.service, buyer=buyer))
        client = app.test_client()
        state = client.get('/shop/api/v2/buyer/session', base_url=ORIGIN).get_json()
        with closing(self.factory()) as db:
            principal = json.loads(db.execute('SELECT record_json FROM shop_buyer_sessions_v2').fetchone()[0])['customer_id']
        value = quote(customer=principal)
        self.store.save_quote(value, expected_quote_hash=value['quote_hash'], customer_id=principal, mode='test', delivery_context=CONTEXT)
        payload = {'quote_hash': value['quote_hash'], 'pack_hash': self.service.pack_hash, 'terms_accepted': True, 'consumer': True}
        self.assertEqual(client.post('/shop/api/v2/consumer/acceptance', base_url=ORIGIN,
            headers={'Origin': ORIGIN}, json=payload).status_code, 403)
        response = client.post('/shop/api/v2/consumer/acceptance', base_url=ORIGIN,
            headers={'Origin': ORIGIN, 'X-CSRF-Token': state['csrf_token']}, json=payload)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.get_json()['accepted'])
        self.assertNotIn(principal, response.text)

    def test_durable_confirmation_uses_same_token_exact_recipient_and_never_sends_during_intake(self):
        csrf, _ = self.open()
        self.assertEqual(self.confirm(csrf, self.review(csrf)).status_code, 200)
        self.assertFalse(self.service.deliver_once())
        active = self.consumer(delivery_enabled=True)
        self.assertTrue(active.deliver_once())
        self.assertFalse(active.deliver_once())
        self.outbox.send.assert_called_once()
        token, message, cfg = self.outbox.send.call_args.args
        self.assertEqual(token, self.withdrawal_row()['mail_token'])
        self.assertEqual(message['To'], 'synthetic@example.invalid')
        self.assertIn(NOW.isoformat(), message.get_content())
        self.assertIn('EXTERNAL REFERENCE NOT IN DATABASE', message.get_content())
        self.assertFalse(self.outbox.send.call_args.kwargs['retry_not_sent'])
        self.assertEqual(self.withdrawal_row()['mail_state'], 'submitted')

    def test_unknown_or_wrong_recipient_mail_result_is_not_retried_or_called_delivered(self):
        csrf, _ = self.open()
        self.assertEqual(self.confirm(csrf, self.review(csrf)).status_code, 200)
        self.outbox.send.side_effect = TimeoutError('SYNTHETIC uncertain SMTP')
        active = self.consumer(delivery_enabled=True)
        self.assertTrue(active.deliver_once())
        self.assertEqual(self.withdrawal_row()['mail_state'], 'unknown')
        self.assertFalse(active.deliver_once())
        self.outbox.send.assert_called_once()
        row = self.withdrawal_row()
        self.assertEqual(active._mail_state({'token': row['mail_token'], 'state': 'sent',
            'accepted': ['wrong@example.invalid']}, row), 'unknown')
        self.outbox.status.return_value = {'token': row['mail_token'], 'state': 'copy_pending',
                                           'accepted': ['synthetic@example.invalid']}
        self.assertEqual(active.reconcile_confirmation(row['id'])['confirmation_state'], 'submitted')
        self.outbox.send.assert_called_once()
        self.assertEqual(active.pending_withdrawals()[0]['confirmation_state'], 'submitted')


if __name__ == '__main__':
    unittest.main()
