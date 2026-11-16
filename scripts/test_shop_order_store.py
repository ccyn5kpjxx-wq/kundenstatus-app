"""Synthetic persistent-order tests. Temporary SQLite only; no network/APIs."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_order_store import OrderStore, OrderStoreError, init_schema

NOW = datetime(2026, 11, 16, 20, 0, tzinfo=timezone.utc)
TYRE, OIL = 'tyre:' + 'a' * 24, 'parts:' + 'b' * 24
CONTEXT = {'customer': {'country': 'DE', 'postcode': '74821', 'address_reference': 'SYNTHETIC'}}


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                             separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def rehash(value):
    value = deepcopy(value)
    value.pop('quote_hash', None)
    return {**value, 'quote_hash': digest(value)}


def quote(customer='customer-a', mode='test', context=None):
    context = CONTEXT if context is None else context
    items = []
    for product, quantity, unit, destination, mounting in (
            (TYRE, 4, 10000, 'workshop', True), (OIL, 2, 2000, 'customer', False)):
        items.append({'id': product, 'identity_hash': digest(product), 'name': 'SYNTHETIC',
                      'category': 'tyre' if product == TYRE else 'oil', 'quantity': quantity,
                      'destination': destination, 'unit_gross_cent': unit, 'items_gross_cent': unit * quantity,
                      'services': [{'code': 'mounting', 'gross_cent': 8000}] if mounting else []})
    shipments = []
    for index, item in enumerate(items):
        group = {'id': item['destination'], 'destination': item['destination'],
                 'items': [{'id': item['id'], 'quantity': item['quantity']}],
                 'private_items': [{'id': item['id'], 'quantity': item['quantity'], 'identity_hash': item['identity_hash'],
                                   'mapping': {'supplier_article_id': str(index + 100), 'evidence_id': 'SYNTHETIC'}}],
                 'context_hash': digest(context), 'gross_cent': 500 if index == 0 else 700}
        group['shipment_hash'] = digest({key: group[key] for key in ('id', 'destination', 'private_items', 'context_hash')})
        shipments.append(group)
    return rehash({'version': 1, 'mode': mode, 'currency': 'eur', 'vat_percent': '19.00',
                   'customer_id': customer, 'vehicle_ref': 'SYNTHETIC-CAR',
                   'request': [{'id': TYRE, 'quantity': 4, 'services': {'mounting': True}, 'assembly_group': ''},
                               {'id': OIL, 'quantity': 2, 'services': {}, 'assembly_group': ''}],
                   'created_at': (NOW - timedelta(minutes=1)).isoformat(),
                   'expires_at': (NOW + timedelta(minutes=14)).isoformat(),
                   'ready': True, 'known_subtotal_gross_cent': 53200, 'total_gross_cent': 53200,
                   'blocked_reasons': [], 'lines': items, 'shipments': shipments, 'assembly_evidence': {},
                   'product_checks': {TYRE: {'required_fees': []}, OIL: {'required_fees': []}}})


class OrderStoreChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'synthetic.sqlite'
        self.now = NOW
        self.factory = lambda: sqlite3.connect(self.path, timeout=10)
        with closing(self.factory()) as db:
            init_schema(db)
        self.store = OrderStore(self.factory, clock=lambda: self.now)
        self.snapshot = quote()

    def tearDown(self):
        self.temp.cleanup()

    def save(self, value=None, context=None):
        value = self.snapshot if value is None else value
        return self.store.save_quote(value, expected_quote_hash=value['quote_hash'],
                    customer_id=value['customer_id'], mode=value['mode'],
                    delivery_context=CONTEXT if context is None else context)

    def order(self, value=None, key='synthetic-key-01'):
        value = self.snapshot if value is None else value
        self.save(value)
        return self.store.create_order(value['quote_hash'], customer_id=value['customer_id'],
                                       mode=value['mode'], idempotency_key=key)

    def claim(self, order=None):
        order = self.order() if order is None else order
        return order, self.store.claim_payment(order['id'], customer_id=order['customer_id'], mode=order['mode'])

    def bind(self, order=None, suffix='one'):
        order, attempt = self.claim(order)
        binding = dict(customer_id=order['customer_id'], mode=order['mode'],
                       session_id='cs_' + order['mode'] + '_' + suffix, intent_id='pi_' + suffix,
                       amount_gross_cent=order['amount_gross_cent'], currency='eur')
        self.store.bind_payment(order['id'], attempt['id'], **binding)
        return order, attempt, binding

    def event(self, order, attempt, binding, kind='paid', identifier='evt_one'):
        return {'event_id': identifier, 'order_id': order['id'], 'attempt_id': attempt['id'],
                'mode': binding['mode'], 'currency': binding['currency'],
                'amount_gross_cent': binding['amount_gross_cent'], 'session_id': binding['session_id'],
                'intent_id': binding['intent_id'], 'kind': kind, 'occurred_at': NOW.isoformat()}

    def test_internal_webhook_owner_lookup_is_minimal_and_mode_bound(self):
        order = self.order()
        self.assertEqual(self.store.lookup_payment_owner(order['id'], mode='test'),
                         {'customer_id': 'customer-a', 'mode': 'test'})
        for identifier, mode in ((order['id'], 'live'), ('sho_' + 'f' * 32, 'test'),
                                 ('../anything', 'test'), (order['id'], 'off')):
            with self.assertRaises(OrderStoreError):
                self.store.lookup_payment_owner(identifier, mode=mode)

    def test_server_quote_ledger_is_required_and_frozen(self):
        with self.assertRaisesRegex(OrderStoreError, 'quote_not_found'):
            self.store.create_order(self.snapshot['quote_hash'], customer_id='customer-a', mode='test', idempotency_key='synthetic-key-01')
        self.save()
        original = deepcopy(self.snapshot)
        self.snapshot['lines'][0]['items_gross_cent'] = 1
        saved = self.store.lookup_server_quote(original['quote_hash'], customer_id='customer-a', mode='test')
        self.assertEqual(saved, original)
        saved['lines'][0]['items_gross_cent'] = 2
        self.assertEqual(self.store.lookup_server_quote(original['quote_hash'], customer_id='customer-a', mode='test'), original)

    def test_tampering_and_unknown_prices_services_freight_or_mapping_never_save(self):
        tampered = deepcopy(self.snapshot)
        tampered['total_gross_cent'] = 1
        with self.assertRaisesRegex(OrderStoreError, 'quote_binding_mismatch'):
            self.save(tampered)
        changes = [lambda v: v.update(ready=False), lambda v: v.update(total_gross_cent=None),
                   lambda v: v['shipments'][0].update(gross_cent=None),
                   lambda v: v['shipments'][0]['private_items'][0].update(mapping=None),
                   lambda v: v['lines'][0]['services'][0].update(gross_cent=None),
                   lambda v: v['lines'][0].update(services=[]),
                   lambda v: v['shipments'][0]['items'][0].update(quantity=3)]
        for change in changes:
            with self.subTest(change=change):
                value = deepcopy(self.snapshot)
                change(value)
                with self.assertRaises(OrderStoreError):
                    self.save(rehash(value))
        with closing(self.factory()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_quotes_v2').fetchone()[0], 0)

    def test_future_expired_long_or_naive_quotes_are_rejected(self):
        for start, expiry in ((NOW + timedelta(seconds=1), NOW + timedelta(minutes=10)),
                              (NOW - timedelta(minutes=15), NOW),
                              (NOW, NOW + timedelta(minutes=16)),
                              (NOW.replace(tzinfo=None), NOW + timedelta(minutes=10))):
            value = deepcopy(self.snapshot)
            value.update(created_at=start.isoformat(), expires_at=expiry.isoformat())
            with self.subTest(start=start), self.assertRaises(OrderStoreError):
                self.save(rehash(value))

    def test_same_idempotency_key_reuses_one_order_but_conflicting_bindings_fail(self):
        first = self.order()
        again = self.store.create_order(self.snapshot['quote_hash'], customer_id='customer-a', mode='test', idempotency_key='synthetic-key-01')
        self.assertEqual(first, again)
        for value in (quote(customer='customer-b'), quote(mode='live')):
            self.save(value)
            with self.assertRaisesRegex(OrderStoreError, 'idempotency_conflict'):
                self.store.create_order(value['quote_hash'], customer_id=value['customer_id'], mode=value['mode'], idempotency_key='synthetic-key-01')
        with self.assertRaisesRegex(OrderStoreError, 'quote_already_ordered'):
            self.store.create_order(self.snapshot['quote_hash'], customer_id='customer-a', mode='test', idempotency_key='synthetic-key-02')

    def test_customer_mode_and_frozen_delivery_context_are_bound(self):
        wrong = deepcopy(CONTEXT)
        wrong['customer']['postcode'] = '00000'
        with self.assertRaisesRegex(OrderStoreError, 'delivery_context_mismatch'):
            self.save(context=wrong)
        order = self.order()
        wrong['customer']['postcode'] = '11111'
        self.assertEqual(self.store.delivery_context_for_order(order['id'], customer_id='customer-a', mode='test'), CONTEXT)
        for identity in ({'customer_id': 'customer-b', 'mode': 'test'}, {'customer_id': 'customer-a', 'mode': 'live'}):
            with self.assertRaisesRegex(OrderStoreError, 'order_not_found'):
                self.store.get_order(order['id'], **identity)

    def test_expired_existing_order_is_retrievable_but_cannot_start_payment(self):
        order = self.order()
        self.now += timedelta(minutes=20)
        self.assertEqual(self.store.create_order(self.snapshot['quote_hash'], customer_id='customer-a', mode='test', idempotency_key='synthetic-key-01')['id'], order['id'])
        with self.assertRaisesRegex(OrderStoreError, 'quote_expired_or_order_closed'):
            self.store.claim_payment(order['id'], customer_id='customer-a', mode='test')

    def test_freshness_is_checked_after_acquiring_the_database_connection_and_lock(self):
        def delayed_factory():
            self.now = NOW + timedelta(minutes=30)
            return self.factory()
        delayed = OrderStore(delayed_factory, clock=lambda: self.now)
        with self.assertRaisesRegex(OrderStoreError, 'quote_expired'):
            delayed.save_quote(self.snapshot, expected_quote_hash=self.snapshot['quote_hash'],
                               customer_id='customer-a', mode='test', delivery_context=CONTEXT)
        self.now = NOW
        self.save()
        with self.assertRaisesRegex(OrderStoreError, 'quote_expired'):
            delayed.create_order(self.snapshot['quote_hash'], customer_id='customer-a', mode='test', idempotency_key='synthetic-key-01')
        self.now = NOW
        order = self.order()
        with self.assertRaisesRegex(OrderStoreError, 'quote_expired_or_order_closed'):
            delayed.claim_payment(order['id'], customer_id='customer-a', mode='test')

    def test_concurrent_duplicate_submissions_create_one_order_and_one_payment_claim(self):
        self.save()
        def submit(_):
            store = OrderStore(self.factory, clock=lambda: NOW)
            order = store.create_order(self.snapshot['quote_hash'], customer_id='customer-a', mode='test', idempotency_key='synthetic-key-01')
            attempt = store.claim_payment(order['id'], customer_id='customer-a', mode='test')
            return order['id'], attempt['id'], attempt['can_create']
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(submit, range(8)))
        self.assertEqual(len({row[0] for row in results}), 1)
        self.assertEqual(len({row[1] for row in results}), 1)
        self.assertEqual(sum(row[2] for row in results), 1)

    def test_lost_response_requires_same_durable_operation_reconciliation(self):
        order, first = self.claim()
        self.assertTrue(first['can_create'])
        recovered = OrderStore(self.factory, clock=lambda: NOW)
        second = recovered.claim_payment(order['id'], customer_id='customer-a', mode='test')
        self.assertFalse(second['can_create'])
        self.assertTrue(second['reconcile_required'])
        self.assertEqual(first['idempotency_key'], second['idempotency_key'])
        recovered.mark_payment_unknown(order['id'], first['id'], customer_id='customer-a', mode='test')
        self.assertEqual(recovered.claim_payment(order['id'], customer_id='customer-a', mode='test')['state'], 'unknown')

    def test_payment_binding_rejects_amount_mode_and_cross_order_session_reuse(self):
        order, attempt = self.claim()
        values = dict(customer_id='customer-a', mode='test', session_id='cs_test_one', intent_id='pi_one', amount_gross_cent=53200, currency='eur')
        for changed in ({'amount_gross_cent': 1}, {'session_id': 'cs_live_one'}, {'currency': 'usd'}):
            with self.subTest(changed=changed), self.assertRaises(OrderStoreError):
                self.store.bind_payment(order['id'], attempt['id'], **{**values, **changed})
        self.store.bind_payment(order['id'], attempt['id'], **values)
        self.store.bind_payment(order['id'], attempt['id'], **values)
        other = self.order(quote(customer='customer-b'), key='synthetic-key-02')
        other_attempt = self.store.claim_payment(other['id'], customer_id='customer-b', mode='test')
        with self.assertRaisesRegex(OrderStoreError, 'payment_binding_conflict'):
            self.store.bind_payment(other['id'], other_attempt['id'], **{**values, 'customer_id': 'customer-b'})

    def test_only_verified_exact_bound_events_change_payment_status(self):
        order, attempt, binding = self.bind()
        event = self.event(order, attempt, binding)
        with self.assertRaisesRegex(OrderStoreError, 'unverified_payment_event'):
            self.store.apply_verified_event(event)
        for change in ({'amount_gross_cent': 1}, {'currency': 'usd'}, {'mode': 'live'}, {'intent_id': 'pi_other'}, {'kind': 'browser_return'}):
            with self.subTest(change=change), self.assertRaises(OrderStoreError):
                self.store.apply_verified_event({**event, **change}, verified=True)
        self.assertEqual(self.store.get_order(order['id'], customer_id='customer-a', mode='test')['status'], 'payment_pending')
        accepted = self.store.apply_verified_event(event, verified=True)
        self.assertEqual(accepted['status'], 'paid')
        self.assertEqual(self.store.apply_verified_event(event, verified=True)['status'], 'paid')
        with closing(self.factory()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM shop_payment_events_v2').fetchone()[0], 1)

    def test_open_checkout_session_can_learn_its_exact_intent_once(self):
        order, attempt = self.claim()
        values = dict(customer_id='customer-a', mode='test', session_id='cs_test_open',
                      intent_id=None, amount_gross_cent=53200, currency='eur')
        initial = self.store.bind_payment(order['id'], attempt['id'], **values)
        self.assertIsNone(initial['intent_id'])
        pending = self.store.claim_payment(order['id'], customer_id='customer-a', mode='test')
        self.assertFalse(pending['can_create'])
        self.assertTrue(pending['reconcile_required'])
        event = self.event(order, attempt, {**values, 'intent_id': 'pi_learned'})
        with self.assertRaisesRegex(OrderStoreError, 'payment_event_mismatch'):
            self.store.apply_verified_event(event, verified=True)
        self.store.bind_payment(order['id'], attempt['id'], **{**values, 'intent_id': 'pi_learned'})
        with self.assertRaisesRegex(OrderStoreError, 'payment_binding_mismatch'):
            self.store.bind_payment(order['id'], attempt['id'], **{**values, 'intent_id': 'pi_other'})
        self.assertEqual(self.store.apply_verified_event(event, verified=True)['status'], 'paid')

    def test_expired_open_checkout_can_close_without_an_intent_but_cannot_become_paid(self):
        order, attempt = self.claim()
        values = dict(customer_id='customer-a', mode='test', session_id='cs_test_expired',
                      intent_id=None, amount_gross_cent=53200, currency='eur')
        self.store.bind_payment(order['id'], attempt['id'], **values)
        event = self.event(order, attempt, values, 'cancelled', 'evt_expired')
        for kind in ('paid', 'authorized'):
            with self.assertRaisesRegex(OrderStoreError, 'payment_event_mismatch'):
                self.store.apply_verified_event({**event, 'kind': kind}, verified=True)
        self.assertEqual(self.store.apply_verified_event(event, verified=True)['status'], 'cancelled')
        self.assertEqual(self.store.apply_verified_event(event, verified=True)['status'], 'cancelled')
        retry = self.store.claim_payment(order['id'], customer_id='customer-a', mode='test')
        self.assertFalse(retry['can_create'])
        self.assertFalse(retry['reconcile_required'])
        with self.assertRaisesRegex(OrderStoreError, 'payment_binding_mismatch'):
            self.store.bind_payment(order['id'], attempt['id'], **{**values, 'intent_id': 'pi_late'})

    def test_late_events_do_not_downgrade_paid_and_duplicate_ids_cannot_change_binding(self):
        order, attempt, binding = self.bind()
        event = self.event(order, attempt, binding)
        self.store.apply_verified_event(event, verified=True)
        self.assertEqual(self.store.apply_verified_event(self.event(order, attempt, binding, 'cancelled', 'evt_late'), verified=True)['status'], 'paid')
        with self.assertRaisesRegex(OrderStoreError, 'payment_event_conflict'):
            self.store.apply_verified_event({**event, 'kind': 'failed'}, verified=True)

    def test_new_authorization_can_recover_failure_but_old_failures_do_not_downgrade_it(self):
        order, attempt, binding = self.bind()
        failed = self.event(order, attempt, binding, 'failed', 'evt_failed')
        self.assertEqual(self.store.apply_verified_event(failed, verified=True)['status'], 'payment_failed')
        self.now = NOW + timedelta(seconds=30)
        recovered = {**self.event(order, attempt, binding, 'authorized', 'evt_retry'), 'occurred_at': self.now.isoformat()}
        self.assertEqual(self.store.apply_verified_event(recovered, verified=True)['status'], 'authorized')
        old_failure = {**failed, 'event_id': 'evt_old_failure'}
        self.assertEqual(self.store.apply_verified_event(old_failure, verified=True)['status'], 'authorized')
        future = {**recovered, 'event_id': 'evt_future', 'occurred_at': (self.now + timedelta(minutes=3)).isoformat()}
        with self.assertRaisesRegex(OrderStoreError, 'payment_event_mismatch'):
            self.store.apply_verified_event(future, verified=True)

    def test_snapshot_and_delivery_context_cannot_be_updated_in_storage(self):
        order = self.order()
        for column, value in (('amount_gross_cent', 1), ('snapshot_json', '{}'), ('delivery_context_json', '{}')):
            with closing(self.factory()) as db, self.assertRaises(sqlite3.IntegrityError):
                db.execute('UPDATE shop_orders_v2 SET ' + column + '=? WHERE id=?', (value, order['id']))
        self.assertEqual(self.store.snapshot_for_order(order['id'], customer_id='customer-a', mode='test')['total_gross_cent'], 53200)

    def test_failed_storage_and_invalid_mode_do_not_leak_raw_errors(self):
        def broken():
            raise RuntimeError('PRIVATE_CONNECTION_CANARY')
        with self.assertRaisesRegex(OrderStoreError, '^storage_unavailable$'):
            OrderStore(broken).lookup_server_quote('a' * 64, customer_id='customer-a', mode='test')
        with self.assertRaises(OrderStoreError):
            self.store.bind_payment('bad', 'bad', customer_id='customer-a', mode=None,
                                    session_id='bad', intent_id='bad', amount_gross_cent=1, currency='eur')


if __name__ == '__main__':
    unittest.main()
