"""Synthetic measurement boundary; temporary SQLite, no providers or network."""
from contextlib import closing
from copy import deepcopy
from pathlib import Path
import json
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flask import Flask
from shop_order_store import OrderStore, OrderStoreError, init_schema
from shop_payment_actions import PaymentActionJournal, init_payment_action_schema
from shop_public_api import create_shop_public_blueprint
from test_shop_order_store import NOW, CONTEXT, quote


class ShopPurchaseMeasurementChecks(unittest.TestCase):
    origin = 'https://shop.example.test'

    def setUp(self):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'synthetic-measurement.sqlite'
        self.factory = lambda: sqlite3.connect(self.path)
        with closing(self.factory()) as db:
            init_schema(db)
            init_payment_action_schema(db)
        self.store = OrderStore(self.factory, clock=lambda: NOW)
        self.actions = PaymentActionJournal(self.factory, clock=lambda: NOW)
        self.auth = Mock(return_value='customer-a')
        self.checkout = Mock()
        self.app = Flask(__name__)
        self.app.config.update(SHOP_PUBLIC_CATALOGUE_ENABLED=True, SHOP_ORDER_DRAFT_ENABLED=True,
                               SHOP_ORDER_MODE='live', SHOP_PUBLIC_ORIGIN=self.origin)
        self.app.register_blueprint(create_shop_public_blueprint(order_store=self.store,
            checkout_service=self.checkout, authorize_quote=self.auth))
        self.client = self.app.test_client()
        self.order, self.attempt, self.binding = self.create()

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()

    def create(self, mode='live', customer='customer-a'):
        snapshot = quote(mode=mode, customer=customer)
        self.store.save_quote(snapshot, expected_quote_hash=snapshot['quote_hash'], customer_id=customer,
                              mode=mode, delivery_context=CONTEXT)
        order = self.store.create_order(snapshot['quote_hash'], customer_id=customer,
                                       mode=mode, idempotency_key='synthetic-measure-' + customer + mode)
        attempt = self.store.claim_payment(order['id'], customer_id=customer, mode=mode)
        binding = {'order_id': order['id'], 'customer_id': customer, 'mode': mode,
                   'currency': 'eur', 'amount_gross_cent': order['amount_gross_cent'],
                   'quote_hash': order['quote_hash'], 'payment_attempt': attempt['id'],
                   'session_id': 'cs_' + mode + '_synthetic' + customer.replace('-', ''),
                   'intent_id': 'pi_synthetic' + customer.replace('-', '') + mode}
        self.store.bind_payment(order['id'], attempt['id'], customer_id=customer, mode=mode,
            session_id=binding['session_id'], intent_id=binding['intent_id'],
            amount_gross_cent=order['amount_gross_cent'], currency='eur')
        return order, attempt, binding

    def event(self, kind='paid', *, order=None, attempt=None, binding=None):
        order, attempt, binding = order or self.order, attempt or self.attempt, binding or self.binding
        self.store.apply_verified_event({'event_id': 'evt_synthetic_' + kind + attempt['id'],
            'order_id': order['id'], 'attempt_id': attempt['id'], 'kind': kind,
            'occurred_at': NOW.isoformat(), 'mode': binding['mode'], 'currency': 'eur',
            'amount_gross_cent': binding['amount_gross_cent'], 'session_id': binding['session_id'],
            'intent_id': binding['intent_id']}, verified=True)

    def captured(self, binding=None):
        binding = binding or self.binding
        action = self.actions.claim(binding, 'capture', {'verified': True, 'persisted': True})
        self.actions.settle(action['id'], binding, {'state': 'captured', 'observed_at': NOW.isoformat()})

    def purchase(self, order=None):
        order = order or self.order
        return self.store.purchase_conversion(order['id'], customer_id=order['customer_id'], mode=order['mode'])

    def get(self, *, suffix='', headers=None):
        return self.client.get('/shop/api/v2/orders/' + self.order['id'] + '/measurement' + suffix,
                               base_url=self.origin, headers=headers or {})

    def test_live_full_capture_has_exact_server_amount_and_opaque_stable_dedup_key(self):
        self.event()
        self.captured()
        before = self.purchase()
        self.assertRegex(before['transaction_id'], r'^shop_ads_[a-f0-9]{64}$')
        self.assertEqual(before['value'], 532.0)
        self.assertEqual(before['currency'], 'EUR')
        self.assertEqual(before, self.purchase())
        self.assertNotIn(self.order['id'], json.dumps(before))
        self.assertNotIn('customer-a', json.dumps(before))
        self.assertNotIn('cs_live', json.dumps(before))
        other, attempt, binding = self.create(customer='customer-b')
        self.event(order=other, attempt=attempt, binding=binding)
        self.captured(binding)
        self.assertNotEqual(before['transaction_id'], self.purchase(other)['transaction_id'])

    def test_unpaid_authorized_failed_and_return_alone_never_project(self):
        self.assertIsNone(self.purchase())
        self.event('authorized')
        self.assertIsNone(self.purchase())
        self.event('failed')
        self.assertIsNone(self.purchase())

    def test_paid_flag_or_paid_event_alone_is_insufficient_without_capture_proof(self):
        with closing(self.factory()) as db:
            db.execute("UPDATE shop_orders_v2 SET status='paid' WHERE id=?", (self.order['id'],))
            db.commit()
        self.assertIsNone(self.purchase())
        self.event()
        self.assertIsNone(self.purchase())

    def test_capture_alone_is_insufficient_without_verified_paid_event(self):
        self.captured()
        with closing(self.factory()) as db:
            db.execute("UPDATE shop_orders_v2 SET status='paid' WHERE id=?", (self.order['id'],))
            db.execute("UPDATE shop_payment_attempts_v2 SET state='paid' WHERE id=?", (self.attempt['id'],))
            db.commit()
        self.assertIsNone(self.purchase())

    def test_test_mode_never_projects_even_if_full_paid_and_captured(self):
        order, attempt, binding = self.create(mode='test')
        self.event(order=order, attempt=attempt, binding=binding)
        self.captured(binding)
        self.assertIsNone(self.purchase(order))

    def test_cancelled_or_refunded_retained_status_never_projects(self):
        self.event()
        self.captured()
        for status in ('cancelled', 'refunded', 'payment_failed', 'authorized'):
            with self.subTest(status=status), closing(self.factory()) as db:
                db.execute('UPDATE shop_orders_v2 SET status=? WHERE id=?', (status, self.order['id']))
                db.commit()
                self.assertIsNone(self.purchase())

    def test_wrong_owner_mode_or_unknown_id_do_not_reveal_purchase(self):
        self.event()
        self.captured()
        for owner, mode, oid in [('customer-b', 'live', self.order['id']),
                                  ('customer-a', 'test', self.order['id']),
                                  ('customer-a', 'live', 'sho_' + 'e' * 32)]:
            with self.assertRaisesRegex(OrderStoreError, 'order_not_found'):
                self.store.purchase_conversion(oid, customer_id=owner, mode=mode)

    def test_capture_binding_mismatch_or_unsettled_result_fail_closed(self):
        for change in ({'amount_gross_cent': 1}, {'customer_id': 'customer-b'},
                       {'mode': 'test', 'session_id': 'cs_test_synthetic'}, {'payment_attempt': 'spa_' + 'f' * 32}):
            with self.subTest(change=change):
                with closing(self.factory()) as db:
                    db.execute('DELETE FROM shop_payment_actions_v2')
                    db.commit()
                binding = {**self.binding, **change}
                self.captured(binding)
                self.event()
                self.assertIsNone(self.purchase())

    def test_uninitialized_capture_journal_fails_closed_without_schema_mutation(self):
        self.event()
        with closing(self.factory()) as db:
            db.execute('DROP TABLE shop_payment_actions_v2')
            db.commit()
        self.assertIsNone(self.purchase())

    def test_unknown_or_malformed_capture_result_never_projects(self):
        self.event()
        self.captured()
        for result in ('{', 'null', '{"state":"cancelled","observed_at":"2026-11-16T20:00:00Z"}',
                       '{"state":"captured","observed_at":"2026-11-16T20:00:00Z","refunded":true}'):
            with self.subTest(result=result), closing(self.factory()) as db:
                db.execute('UPDATE shop_payment_actions_v2 SET result_json=? WHERE order_id=?',
                           (result, self.order['id']))
                db.commit()
                self.assertIsNone(self.purchase())

    def test_measurement_does_not_mutate_any_order_payment_or_journal_record(self):
        self.event()
        self.captured()
        def dump():
            with closing(self.factory()) as db:
                return list(db.iterdump())
        before = dump()
        self.assertIsNotNone(self.purchase())
        self.assertIsNotNone(self.get().get_json()['purchase'])
        self.assertEqual(before, dump())

    def test_bad_service_projection_or_private_fields_cannot_escape_boundary(self):
        with patch.object(self.store, 'purchase_conversion') as project:
            for value in ({'transaction_id': 'PRIVATE_CANARY', 'value': 532, 'currency': 'EUR'},
                          {'transaction_id': 'shop_ads_' + 'a' * 64, 'value': True, 'currency': 'EUR'},
                          {'transaction_id': 'shop_ads_' + 'a' * 64, 'value': float('nan'), 'currency': 'EUR'},
                          {'transaction_id': 'shop_ads_' + 'a' * 64, 'value': 532, 'currency': 'EUR',
                           'email': 'PRIVATE_CANARY'}):
                project.return_value = value
                response = self.get()
                self.assertEqual(response.status_code, 503)
                self.assertNotIn(b'PRIVATE_CANARY', response.data)
            project.side_effect = RuntimeError('PRIVATE_CANARY')
            response = self.get()
            self.assertEqual(response.status_code, 503)
            self.assertNotIn(b'PRIVATE_CANARY', response.data)

    def test_http_is_session_scoped_read_only_no_store_and_minimal(self):
        self.event()
        self.captured()
        response = self.get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {'purchase': self.purchase()})
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
        self.checkout.assert_not_called()
        self.checkout.start.assert_not_called()
        self.checkout.reconcile.assert_not_called()
        self.auth.assert_called_once()

    def test_http_unknown_owner_missing_auth_cross_origin_and_caller_values_are_rejected(self):
        self.auth.return_value = 'customer-b'
        self.assertEqual(self.get().status_code, 404)
        self.auth.return_value = None
        self.assertEqual(self.get().status_code, 403)
        self.auth.return_value = 'customer-a'
        self.assertEqual(self.get(headers={'Origin': 'https://attacker.test'}).status_code, 403)
        self.assertEqual(self.get(headers={'Sec-Fetch-Site': 'cross-site'}).status_code, 403)
        self.assertEqual(self.get(suffix='?value=1&mode=live').status_code, 400)

    def test_http_disabled_and_test_mode_do_not_measure(self):
        self.app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = False
        self.assertEqual(self.get().status_code, 404)
        self.app.config['SHOP_PUBLIC_CATALOGUE_ENABLED'] = True
        self.app.config['SHOP_ORDER_MODE'] = 'test'
        self.assertEqual(self.get().status_code, 404)


if __name__ == '__main__':
    unittest.main()
