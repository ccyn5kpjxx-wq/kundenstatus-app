"""Synthetic persisted procurement saga; no actual orders, payments or network."""
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import json
import socket
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
from shop_fulfillment import ShopFulfillment, FulfillmentJournal, FulfillmentError, init_fulfillment_schema
from shop_cart_quotes import retail_unit_gross_cent
from shop_order_store import OrderStore, init_schema
from shop_rsu_orders import request_sha256, RSUOrderTransport, RSUOrderPolicy
from test_shop_cart_quotes import Fixture, NOW, TYRE, RIM, OIL, BODY, line

ARTICLES = {TYRE: '101', RIM: '102', OIL: '103', BODY: '104'}
POLICY = {'account_fingerprint': 'e' * 64, 'mode_evidence_id': 'SYNTHETIC-mode-policy',
          'acceptance_evidence_id': 'SYNTHETIC-all-qty-net-policy'}


def ean(number):
    digits = str(number).zfill(12)
    total = sum(int(c) * (3 if i % 2 == 0 else 1) for i, c in enumerate(reversed(digits)))
    return digits + str((10 - total % 10) % 10)


class Payment:
    def __init__(self, fixture):
        self.f = fixture
        self.actions = []
        self.authorizations = 0
        self.result_state = None
        self.expiry = timedelta(hours=3)

    def authorization(self, order_id):
        self.authorizations += 1
        binding = self.f.binding()
        assert order_id == binding['order_id']
        return {**binding, 'verified': True, 'state': 'authorized',
                'observed_at': self.f.now.isoformat(), 'authorized_until': (self.f.now + self.expiry).isoformat()}

    def action(self, order_id, operation):
        proof = self.f.saga.proof_resolver(order_id, operation=operation)
        assert proof['verified'] is True and proof['persisted'] is True
        assert proof['all_goods_accepted'] is (operation == 'capture')
        if operation == 'capture':
            assert proof['accepted_lines'] == [{'id': key, 'quantity': n} for key, n in sorted(self.f.quantities.items())]
        else:
            assert proof['cancel_allowed'] is True
        self.actions.append(operation)
        state = self.result_state or ('captured' if operation == 'capture' else 'cancelled')
        if state in {'captured', 'cancelled'}:
            self.f.store.apply_verified_event(self.f.event('paid' if state == 'captured' else 'cancelled', 'evt_SYNTHETIC_' + operation), verified=True)
        return {'order_id': order_id, 'operation': operation, 'state': state, 'mode': 'test',
                'currency': 'eur', 'amount_gross_cent': self.f.order['amount_gross_cent'],
                'payment_confirmed': state == 'captured', 'authorization_released': state == 'cancelled'}

    def capture(self, order_id):
        return self.action(order_id, 'capture')

    def cancel(self, order_id):
        return self.action(order_id, 'cancel')


class Transport:
    def __init__(self, fixture):
        self.f = fixture
        self.calls = []
        self.outcomes = []
        self.after_grant = None
        self.before_grant = None
        self.change_receipt = None

    def submit_order(self, payload, *, idempotency_key):
        envelope = {**POLICY, 'configured_mode': 'test', 'reference': payload['reference'],
                    'request_hash': request_sha256(payload), 'idempotency_key': idempotency_key}
        if self.before_grant:
            self.before_grant()
        if self.f.saga.authorize_submission(envelope) is not True:
            return {'outcome': 'not_sent', 'reference': payload['reference'], 'request_hash': request_sha256(payload),
                    'positions': [], 'echo_confirmed': False, 'fulfillment_confirmed': False,
                    'capture_authorized': False, 'retry_authorized': False}
        assert self.f.saga.authorize_submission(envelope) is False  # one-use transport grant
        self.calls.append(deepcopy(payload))
        if self.after_grant:
            self.after_grant()
        outcome = self.outcomes.pop(0) if self.outcomes else 'accepted'
        if outcome == 'timeout':
            raise TimeoutError('PRIVATE_PROVIDER_CANARY')
        positions = []
        for index, item in enumerate(payload['positions']):
            kind = 'rejected' if outcome == 'rejected' or outcome == 'partial' and index > 0 else 'accepted'
            positions.append({**item, 'outcome': kind, 'order_number': 'Test_1234' if kind == 'accepted' else None,
                'request_bound': True, 'echo_confirmed': False, 'facts_source': 'submitted_request_under_external_policy'})
        result = {'outcome': outcome, 'reference': payload['reference'], 'request_hash': request_sha256(payload),
                  'positions': positions, 'echo_confirmed': False, 'fulfillment_confirmed': False,
                  'capture_authorized': False, 'retry_authorized': False}
        if self.change_receipt:
            self.change_receipt(result)
        return result


class SagaFixture:
    def setUp(self, rows=None):
        self.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        self.network.start()
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'synthetic.sqlite'
        self.factory = lambda: sqlite3.connect(self.path, timeout=5)
        with closing(self.factory()) as db:
            init_schema(db)
            init_fulfillment_schema(db)
        self.now = NOW
        self.store = OrderStore(self.factory, clock=lambda: self.now)
        self.journal = FulfillmentJournal(self.factory, clock=lambda: self.now)
        self.f = Fixture()
        self.f.current = lambda: {'confirmed': True, 'mode': 'test', 'currency': 'eur', 'vat_percent': '19.00',
            'observed_at': self.now.isoformat(), 'expires_at': (self.now + timedelta(minutes=10)).isoformat()}
        self.f.mapping = lambda product: {'confirmed': True, 'supplier': 'RSU', 'product_id': product['id'],
            'identity_hash': product['identity_hash'], 'supplier_article_id': ARTICLES[product['id']], 'evidence_id': 'SYNTHETIC-mapping-proof'}
        self.context = {'postcode': '74821', 'address_reference': 'SYNTHETIC_ONLY'}
        self.rows = rows or [line(TYRE, 4, services={'mounting': True}, group='set-1'),
                     line(RIM, 4, services={'mounting': True}, group='set-1'), line(OIL, 2),
                     line(BODY, services={'painting': True, 'paint_color': 'LY7W'})]
        self.quantities = {row['id']: row['quantity'] for row in self.rows}
        quote = self.f.service_instance(clock=lambda: self.now).quote({'lines': self.rows},
            customer_id='customer-a', vehicle_ref='CAR-1', delivery_context=self.context)
        self.store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash,
            customer_id='customer-a', mode='test', delivery_context=self.context)
        self.order = self.store.create_order(quote.quote_hash, customer_id='customer-a', mode='test',
                                             idempotency_key='SYNTHETIC-checkout-order')
        self.attempt = self.store.claim_payment(self.order['id'], customer_id='customer-a', mode='test')
        self.store.bind_payment(self.order['id'], self.attempt['id'], customer_id='customer-a', mode='test',
            session_id='cs_test_SYNTHETIC', intent_id='pi_SYNTHETIC', amount_gross_cent=self.order['amount_gross_cent'], currency='eur')
        self.store.apply_verified_event(self.event('authorized', 'evt_SYNTHETIC_AUTH'), verified=True)
        self.payment = Payment(self)
        self.transport = Transport(self)
        self.net_override, self.inquiry_edits = None, None
        self.inquiry_calls = []
        self.saga = self.make_saga()

    def tearDown(self):
        self.temp.cleanup()
        self.network.stop()

    def binding(self):
        return {'order_id': self.order['id'], 'customer_id': 'customer-a', 'mode': 'test', 'currency': 'eur',
                'quote_hash': self.order['quote_hash'], 'amount_gross_cent': self.order['amount_gross_cent'],
                'payment_attempt': self.attempt['id'], 'session_id': 'cs_test_SYNTHETIC', 'intent_id': 'pi_SYNTHETIC'}

    def event(self, kind, identifier):
        return {'event_id': identifier, 'order_id': self.order['id'], 'attempt_id': self.attempt['id'],
                'mode': 'test', 'currency': 'eur', 'amount_gross_cent': self.order['amount_gross_cent'],
                'session_id': 'cs_test_SYNTHETIC', 'intent_id': 'pi_SYNTHETIC', 'kind': kind, 'occurred_at': self.now.isoformat()}

    def inquiry(self, item, quantity):
        self.inquiry_calls.append((item['id'], quantity))
        result = {'confirmed': True, 'product_id': item['id'], 'identity_hash': item['identity_hash'],
                  'supplier_article_id': item['mapping']['supplier_article_id'],
                  'mapping_evidence_id': item['mapping']['evidence_id'],
                  'expected_ean': ean(item['mapping']['supplier_article_id']), 'ean': ean(item['mapping']['supplier_article_id']),
                  'quantity': quantity, 'currency': 'EUR', 'mode': 'test', 'stock': 100,
                  'unit_net': self.net_override or ('100' if item['id'] in {TYRE, RIM} else '50'),
                  'observed_at': self.now.isoformat(), 'expires_at': (self.now + timedelta(minutes=10)).isoformat()}
        if self.inquiry_edits:
            self.inquiry_edits(result)
        return result

    def revalidate(self, snapshot, context, *, destinations):
        requests = [request for request, line in zip(snapshot['request'], snapshot['lines']) if line['destination'] in destinations]
        return self.f.service_instance(clock=lambda: self.now).quote({'lines': requests},
            customer_id='customer-a', vehicle_ref=snapshot['vehicle_ref'], delivery_context=context).snapshot()

    def address(self, destination, context):
        return {'neutraldelivery': 1, 'salutation': 'Firma', 'name': 'SYNTHETIC ' + destination,
                'street': 'Teststraße 12', 'zipcode': '74821', 'city': 'Mosbach', 'countrycode': 'DE'}

    def make_saga(self, **changes):
        values = dict(mode='test', supplier_policy=POLICY, fresh_inquiry=self.inquiry, revalidate=self.revalidate,
                      delivery_address=self.address, enabled=True, clock=lambda: self.now)
        values.update(changes)
        return ShopFulfillment(self.store, self.journal, self.payment, self.transport, **values)

    def run_saga(self):
        return self.saga.run(self.order['id'], customer_id='customer-a')


class SagaChecks(SagaFixture, unittest.TestCase):
    def test_disabled_default_has_no_database_or_adapter_calls(self):
        calls = []
        journal = FulfillmentJournal(lambda: calls.append('db'))
        saga = ShopFulfillment(None, journal, mode='test')
        with self.assertRaisesRegex(FulfillmentError, '^fulfillment_disabled$'):
            saga.run(self.order['id'], customer_id='customer-a')
        self.assertEqual(calls, [])
        self.assertEqual(self.transport.calls, [])

    def test_all_shipments_bound_once_then_full_capture_and_restart_safe(self):
        result = self.run_saga()
        self.assertEqual(result['state'], 'captured')
        self.assertTrue(result['payment_confirmed'])
        self.assertFalse(result['delivery_confirmed'])
        self.assertEqual(self.payment.actions, ['capture'])
        self.assertEqual(len(self.transport.calls), 4)
        self.assertEqual({p['delivery_address']['name'] for p in self.transport.calls}, {'SYNTHETIC customer', 'SYNTHETIC workshop'})
        self.assertEqual(len({p['reference'] for p in self.transport.calls}), 4)
        for payload in self.transport.calls:
            self.assertTrue(payload['reference'].startswith('SHOP-' + self.order['id'][4:] + '-'))
            self.assertEqual(len(payload['positions']), 1)
            self.assertEqual(payload['positions'][0]['pos'], 1)
            self.assertEqual(payload['delivery_address']['neutraldelivery'], 1)
            self.assertTrue(all(item['unit_net'] in {'100.0000', '50.0000'} for item in payload['positions']))
        self.journal = FulfillmentJournal(self.factory, clock=lambda: self.now)
        self.saga = self.make_saga()
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(len(self.transport.calls), 4)
        self.assertEqual(self.payment.actions, ['capture'])
        text = json.dumps(result)
        for private in ('unit_net', 'Teststraße', 'supplier_article_id', 'SYNTHETIC customer'):
            self.assertNotIn(private, text)

    def test_original_quote_may_expire_but_requires_new_all_cost_reads(self):
        self.now += timedelta(hours=1)
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(len(self.inquiry_calls), 10)  # 4+3+2+1 remaining single-position reads

    def test_higher_partner_cost_rejects_before_post_and_releases_hold(self):
        self.net_override = '101'
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.payment.actions, ['cancel'])

    def test_wrong_currency_never_relabels_the_buying_cost(self):
        self.inquiry_edits = lambda r: r.update(currency='USD')
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertFalse(self.transport.calls)

    def test_wrong_mapping_evidence_never_submits(self):
        self.inquiry_edits = lambda r: r.update(mapping_evidence_id='wrong')
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertFalse(self.transport.calls)

    def test_insufficient_stock_never_partially_orders(self):
        self.inquiry_edits = lambda r: r.update(stock=1)
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertFalse(self.transport.calls)

    def test_ean_mismatch_is_not_a_same_identity_substitute(self):
        self.inquiry_edits = lambda r: r.update(ean=ean(999))
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertFalse(self.transport.calls)

    def test_stale_cached_inquiry_is_not_a_fresh_read(self):
        self.inquiry_edits = lambda r: r.update(observed_at=(self.now - timedelta(seconds=1)).isoformat())
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertFalse(self.transport.calls)

    def test_typed_all_rejection_releases_only_when_nothing_accepted(self):
        self.transport.outcomes = ['rejected']
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.payment.actions, ['cancel'])

    def test_accepted_first_then_rejected_second_is_manual_no_capture_cancel(self):
        self.transport.outcomes = ['accepted', 'rejected']
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 2)
        self.assertEqual(self.payment.actions, [])
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 2)

    def test_lost_response_or_partial_ack_never_resubmits_or_moves_funds(self):
        self.transport.outcomes = ['timeout']
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.saga = self.make_saga()
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.payment.actions, [])
        with self.assertRaises(FulfillmentError):
            self.saga.proof_resolver(self.order['id'], operation='cancel')
        with self.assertRaises(FulfillmentError):
            self.saga.proof_resolver(self.order['id'], operation='capture')

    def test_conflicting_ack_quantities_are_unknown_even_if_transport_says_accepted(self):
        self.transport.change_receipt = lambda result: result['positions'][0].update(quantity=20)
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.payment.actions, [])

    def test_partial_ack_for_one_position_is_unknown_not_full_acceptance(self):
        self.transport.outcomes = ['accepted', 'partial']
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.payment.actions, [])
        private = self.journal.load(self.order['id'], customer_id='customer-a', mode='test')
        partial = next(json.loads(c['receipt_json']) for c in private['children'] if c['state'] == 'unknown')
        self.assertEqual(partial['outcome'], 'unknown')
        self.assertTrue(any(c['state'] == 'accepted' for c in private['children']))

    def test_cost_failure_after_first_acceptance_never_releases_hold(self):
        def after_first():
            self.inquiry_edits = lambda r: r.update(stock=0)
        self.transport.after_grant = after_first
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.payment.actions, [])

    def test_accepted_customer_only_stock_is_not_demanded_again(self):
        def after_first():
            self.f.stock[OIL] = 0
            self.inquiry_edits = lambda result: result.update(stock=0) if result['product_id'] == OIL else None
        self.transport.after_grant = after_first
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(sum(identifier == OIL for identifier, _ in self.inquiry_calls), 1)
        self.assertEqual(self.payment.actions, ['capture'])

    def test_same_product_split_destinations_uses_exact_child_price_tier_and_remaining_stock(self):
        # Different, correctly accepted per-order tiers must not be compared
        # against the lower price of the other destination.
        original_retail = self.f.retail
        self.f.retail = lambda product, quantity: {**original_retail(product, quantity),
            'unit_gross_cent': retail_unit_gross_cent('50' if quantity == 2 else '40', product['category'])}
        self.rows = [line(OIL, 2), line(OIL, 3, services={'oil_service': True})]
        self.quantities = {OIL: 5}
        self.f.stock[OIL] = 5
        quote = self.f.service_instance(clock=lambda: self.now).quote({'lines': self.rows},
            customer_id='customer-a', delivery_context=self.context)
        self.store.save_quote(quote.snapshot(), expected_quote_hash=quote.quote_hash,
            customer_id='customer-a', mode='test', delivery_context=self.context)
        self.order = self.store.create_order(quote.quote_hash, customer_id='customer-a', mode='test',
                                            idempotency_key='SYNTHETIC-split-identical-product')
        self.attempt = self.store.claim_payment(self.order['id'], customer_id='customer-a', mode='test')
        self.store.bind_payment(self.order['id'], self.attempt['id'], customer_id='customer-a', mode='test',
            session_id='cs_test_SYNTHETICSPLIT', intent_id='pi_SYNTHETICSPLIT', amount_gross_cent=self.order['amount_gross_cent'], currency='eur')
        original_binding = self.binding
        self.binding = lambda: {**original_binding(), 'session_id': 'cs_test_SYNTHETICSPLIT', 'intent_id': 'pi_SYNTHETICSPLIT'}
        original_event = self.event
        self.event = lambda kind, identifier: {**original_event(kind, identifier), 'session_id': 'cs_test_SYNTHETICSPLIT', 'intent_id': 'pi_SYNTHETICSPLIT'}
        self.store.apply_verified_event(self.event('authorized', 'evt_SYNTHETIC_SPLIT_AUTH'), verified=True)
        self.inquiry_edits = lambda result: result.update(unit_net='50' if result['quantity'] == 2 else '40')
        def after_first():
            self.f.stock[OIL] = 3
            self.inquiry_edits = lambda result: result.update(stock=3, unit_net='50' if result['quantity'] == 2 else '40')
        self.transport.after_grant = after_first
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(self.inquiry_calls, [(OIL, 2), (OIL, 3), (OIL, 3)])
        self.assertEqual([p['positions'][0]['quantity'] for p in self.transport.calls], [2, 3])
        self.assertEqual([p['positions'][0]['unit_net'] for p in self.transport.calls], ['50.0000', '40.0000'])
        self.assertEqual(self.payment.actions, ['capture'])

    def test_every_single_position_ack_is_required_before_capture(self):
        observed = []
        self.transport.after_grant = lambda: observed.append((len(self.transport.calls), list(self.payment.actions)))
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(observed, [(1, []), (2, []), (3, []), (4, [])])
        private = self.journal.load(self.order['id'], customer_id='customer-a', mode='test')
        self.assertEqual(len(private['children']), 4)
        self.assertTrue(all(c['state'] == 'accepted' for c in private['children']))
        self.assertEqual(len({c['reference'] for c in private['children']}), 4)

    def test_partial_acceptance_between_two_positions_for_same_workshop_never_moves_funds(self):
        self.transport.outcomes = ['accepted', 'accepted', 'rejected']
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 3)
        self.assertEqual([p['delivery_address']['name'] for p in self.transport.calls[1:]],
                         ['SYNTHETIC workshop', 'SYNTHETIC workshop'])
        self.assertEqual(self.payment.actions, [])
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 3)
        for operation in ('capture', 'cancel'):
            with self.assertRaises(FulfillmentError):
                self.saga.proof_resolver(self.order['id'], operation=operation)

    def test_legacy_shipment_claims_are_never_reinterpreted_as_new_position_grants(self):
        with closing(self.factory()) as connection:
            connection.execute('CREATE TABLE shop_fulfillment_shipments_v2 (order_id TEXT,destination TEXT,state TEXT)')
            connection.execute('INSERT INTO shop_fulfillment_shipments_v2 VALUES (?,?,?)',
                               (self.order['id'], 'workshop', 'claimed'))
            connection.commit()
        with self.assertRaisesRegex(FulfillmentError, '^legacy_supplier_claim_requires_review$'):
            self.run_saga()
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.payment.actions, [])
        with closing(self.factory()) as connection:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM shop_fulfillment_positions_v3').fetchone()[0], 0)
            self.assertEqual(connection.execute('SELECT state FROM shop_fulfillment_shipments_v2').fetchone()[0], 'claimed')

    def test_workshop_also_requires_typed_neutral_delivery(self):
        original = self.address
        self.saga = self.make_saga(delivery_address=lambda destination, context: {
            **original(destination, context), 'neutraldelivery': 1 if destination == 'customer' else True})
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.payment.actions, [])

    def test_hold_near_deadline_never_orders(self):
        self.payment.expiry = timedelta(seconds=119)
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.payment.actions, [])

    def test_procurement_expiry_during_post_claim_wait_cannot_gain_a_grant(self):
        self.transport.before_grant = lambda: setattr(self, 'now', self.now + timedelta(minutes=11))
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.payment.actions, [])
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.transport.calls, [])

    def test_higher_freight_is_not_absorbed_without_review(self):
        original = self.f.freight
        self.f.freight = lambda shipment, context: {**original(shipment, context), 'gross_cent': 50000}
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertFalse(self.transport.calls)

    def test_higher_mandatory_service_cost_is_not_hidden_in_frozen_total(self):
        original = self.f.service
        self.f.service = lambda code, product, quantity, options: {**original(code, product, quantity, options), 'gross_cent': 50000}
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertFalse(self.transport.calls)

    def test_customer_address_must_request_neutral_delivery(self):
        original = self.address
        self.saga = self.make_saga(delivery_address=lambda destination, context: {**original(destination, context), 'neutraldelivery': 0})
        self.assertEqual(self.run_saga()['state'], 'cancelled')
        self.assertFalse(self.transport.calls)

    def test_crash_after_supplier_post_before_receipt_commit_remains_claimed(self):
        with patch.object(self.journal, 'settle', side_effect=FulfillmentError('fulfillment_storage_unavailable')):
            self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.payment.actions, [])

    def test_simultaneous_workers_get_only_one_supplier_submission_per_destination(self):
        entered, release = threading.Event(), threading.Event()
        def wait():
            entered.set()
            self.assertTrue(release.wait(5))
        self.transport.after_grant = wait
        results = []
        worker = threading.Thread(target=lambda: results.append(self.run_saga()))
        worker.start()
        self.assertTrue(entered.wait(5))
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        release.set()
        worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(results[0]['state'], 'captured')
        self.assertEqual(len(self.transport.calls), 4)
        self.assertEqual(self.payment.actions, ['capture'])

    def test_wrong_owner_or_mode_cannot_use_another_orders_frozen_record(self):
        with self.assertRaisesRegex(FulfillmentError, '^frozen_order_unavailable$'):
            self.saga.run(self.order['id'], customer_id='customer-b')
        live = self.make_saga(mode='live')
        with self.assertRaisesRegex(FulfillmentError, '^frozen_order_unavailable$'):
            live.run(self.order['id'], customer_id='customer-a')
        self.assertEqual(self.transport.calls, [])
        self.assertEqual(self.payment.actions, [])

    def test_capture_uncertainty_never_resends_supplier_orders(self):
        self.payment.result_state = 'unknown'
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 4)
        self.assertEqual(self.payment.actions, ['capture', 'capture'])  # action service owns read-only reconciliation

    def test_delayed_uncertain_payment_result_cannot_erase_capture(self):
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.journal.action_result(self.order['id'], customer_id='customer-a', mode='test', state='unknown')
        self.assertEqual(self.saga.status(self.order['id'], customer_id='customer-a')['state'], 'captured')

    def test_actual_xml_adapter_acceptance_contract_and_consumed_grant(self):
        from xml.etree import ElementTree as ET
        sent = []
        class Response:
            status_code = 201
            headers = {'Content-Type': 'application/xml'}
            def __init__(self, xml):
                self.xml = xml
            def iter_content(self, **kwargs):
                yield self.xml
            def close(self):
                pass
        class HTTP:
            def post(self, url, **kwargs):
                command = ET.fromstring(kwargs['data'])
                sent.append(command)
                root = ET.Element('orderResponse')
                container = ET.SubElement(root, 'positions')
                for item in command.find('items'):
                    position = ET.SubElement(container, 'position')
                    for key, value in (('ordernumber', 'Test_1234'), ('OrderStatus', '1'), ('error', '0'), ('errorMessage', ''), ('pos', item.findtext('pos'))):
                        ET.SubElement(position, key).text = value
                return Response(ET.tostring(root, encoding='utf-8'))
        policy = RSUOrderPolicy(mode='test', mode_confirmed_evidence=POLICY['mode_evidence_id'],
            acceptance_policy_evidence=POLICY['acceptance_evidence_id'], enabled=True)
        self.transport = RSUOrderTransport({'TYRESYSTEM_API_USERNAME': 'SYNTHETIC_ONLY',
            'TYRESYSTEM_API_PASSWORD': 'SYNTHETIC_ONLY', 'TYRESYSTEM_API_MODE': 'test'}, policy=policy,
            authorize=lambda envelope: self.saga.authorize_submission(envelope), http=HTTP(), clock=lambda: self.now)
        self.saga = self.make_saga(supplier_policy={**POLICY, 'account_fingerprint': self.transport.account_fingerprint})
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(len(sent), 4)
        self.assertTrue(all(len(command.findall('items/item')) == 1 for command in sent))
        self.assertTrue(all(command.findtext('delivery_address/neutraldelivery') == '1' for command in sent))
        self.assertEqual(self.payment.actions, ['capture'])
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(len(sent), 4)


class CombinedPurchaseChecks(SagaFixture, unittest.TestCase):
    def setUp(self):
        from shop_production_payments import requote_remaining_goods
        super().setUp(rows=[line(RIM), line(TYRE)])
        self.saga = self.make_saga(revalidate_remaining=lambda *args: requote_remaining_goods(
            self.f.service_instance(clock=lambda: self.now), *args))
        self.f.stock.update({RIM: 1, TYRE: 1})

    def sell_last_unit(self):
        purchased = self.transport.calls[-1]['positions'][0]['supplier_article_id']
        identifier = next(key for key, article in ARTICLES.items() if article == purchased)
        self.f.stock[identifier] = 0

    def test_rim_and_tyre_consuming_last_units_yield_one_full_payment(self):
        self.transport.after_grant = self.sell_last_unit
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(self.run_saga()['state'], 'captured')
        self.assertEqual(self.payment.actions, ['capture'])
        posts = self.transport.calls
        self.assertEqual(len(posts), 2)
        self.assertEqual({p['positions'][0]['supplier_article_id'] for p in posts}, {ARTICLES[TYRE], ARTICLES[RIM]})
        self.assertEqual(len({p['reference'] for p in posts}), 2)
        self.assertTrue(all(len(p['positions']) == 1 for p in posts))

    def test_full_freight_increase_after_first_acceptance_is_checked_before_ordering_second(self):
        original = self.f.freight
        def after_first():
            self.sell_last_unit()
            self.f.freight = lambda shipment, context: {**original(shipment, context),
                'gross_cent': 900 if len(shipment['items']) == 2 else 500}
        self.transport.after_grant = after_first
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(self.payment.actions, [])

    def test_uncertain_second_item_never_retries_or_captures_either_item(self):
        self.transport.after_grant = self.sell_last_unit
        self.transport.outcomes = ['accepted', 'timeout']
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(self.run_saga()['state'], 'manual_review')
        self.assertEqual(len(self.transport.calls), 2)
        self.assertEqual(self.payment.actions, [])


if __name__ == '__main__':
    unittest.main()
