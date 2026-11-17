"""Real temporary journals, synthetic saga/SMTP only, network forbidden."""
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import json
import sqlite3
import sys
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mailbox_outbox import MailOutbox
from shop_order_store import OrderStore, init_schema
from shop_fulfillment import FulfillmentJournal, init_fulfillment_schema
from shop_order_receipts import ReceiptError, ShopOrderReceipts, init_order_receipt_schema
from shop_order_workflow import WorkflowError, WorkflowJournal, ShopOrderWorkflow, WorkflowCheckout, init_order_workflow_schema
import test_shop_fulfillment as fulfillment_fixtures

ADDRESS = {'recipient': 'Synthetic Customer', 'street': 'Teststraße 12', 'postcode': '74821', 'city': 'Mosbach', 'country': 'DE'}
CONTEXT = {'contact': {'name': 'Synthetic Customer', 'email': 'customer@example.test'},
           'billing': ADDRESS, 'delivery': ADDRESS}
LEGAL = {'version': 'SYNTHETIC-v1', 'texts': {key: 'SYNTHETIC ' + key for key in ('agb', 'widerruf', 'lieferung', 'datenschutz')},
         'attachments': [{'filename': 'Synthetic.pdf', 'mimetype': 'application/pdf', 'content': b'%PDF-1.7\nSYNTHETIC'}]}


class FakeOutbox:
    def __init__(self):
        self.sent, self.retained = [], {}
        self.fail = False

    def send(self, token, message, config, *, retry_not_sent):
        assert retry_not_sent is False
        self.sent.append((token, message))
        result = {'token': token, 'state': 'sent', 'accepted': [str(message['To'])]}
        self.retained[token] = result
        if self.fail:
            raise TimeoutError('PRIVATE_SMTP_CANARY')
        return result

    def status(self, token):
        return self.retained.get(token)


class WorkflowChecks(unittest.TestCase):
    def setUp(self):
        self.f = fulfillment_fixtures.SagaChecks('test_all_shipments_bound_once_then_full_capture_and_restart_safe')
        self.f.setUp()
        self.f.path = Path(self.f.temp.name) / 'workflow.sqlite'
        self.f.factory = lambda: sqlite3.connect(self.f.path, timeout=5)
        with closing(self.f.factory()) as db:
            init_schema(db)
            init_fulfillment_schema(db)
            init_order_receipt_schema(db)
            init_order_workflow_schema(db)
        self.f.store = OrderStore(self.f.factory, clock=lambda: self.f.now)
        self.f.journal = FulfillmentJournal(self.f.factory, clock=lambda: self.f.now)
        self.f.context = deepcopy(CONTEXT)
        q = self.f.f.service_instance(clock=lambda: self.f.now).quote({'lines': self.f.rows},
            customer_id='customer-a', vehicle_ref='CAR-1', delivery_context=self.f.context)
        self.f.store.save_quote(q.snapshot(), expected_quote_hash=q.quote_hash, customer_id='customer-a',
                               mode='test', delivery_context=self.f.context)
        self.f.order = self.f.store.create_order(q.quote_hash, customer_id='customer-a', mode='test', idempotency_key='SYNTHETIC-workflow-order')
        self.f.attempt = self.f.store.claim_payment(self.f.order['id'], customer_id='customer-a', mode='test')
        self.f.store.bind_payment(self.f.order['id'], self.f.attempt['id'], customer_id='customer-a', mode='test',
            session_id='cs_test_SYNTHETIC', intent_id='pi_SYNTHETIC', amount_gross_cent=self.f.order['amount_gross_cent'], currency='eur')
        self.f.store.apply_verified_event(self.f.event('authorized', 'evt_SYNTHETIC_AUTH'), verified=True)
        self.f.saga = self.f.make_saga()
        self.outbox = FakeOutbox()
        self.receipts = ShopOrderReceipts(self.f.store, self.f.factory, mode='test', sender_address='shop@example.test',
            enabled=True, delivery_enabled=True, outbox=self.outbox, smtp_config=lambda: {'from_address': 'shop@example.test'},
            legal_snapshot=lambda oid, **kwargs: deepcopy(LEGAL), clock=lambda: self.f.now)
        self.journal = WorkflowJournal(self.f.factory, clock=lambda: self.f.now, lease_seconds=60)
        self.workflow = ShopOrderWorkflow(self.f.store, self.journal, self.f.saga, mode='test', enabled=True,
                                          receipt_service=self.receipts, clock=lambda: self.f.now)
        self.order_id = self.f.order['id']

    def tearDown(self):
        self.workflow.stop()
        self.f.tearDown()

    def count(self, table):
        with closing(self.f.factory()) as db:
            return db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]

    def test_default_off_construction_and_start_do_not_open_db_or_adapters(self):
        calls = []
        journal = WorkflowJournal(lambda: calls.append('db'))
        service = ShopOrderWorkflow(None, journal, None, mode='test')
        with self.assertRaisesRegex(WorkflowError, 'workflow_disabled'):
            service.start()
        self.assertEqual(calls, [])
        self.assertEqual(self.f.transport.calls, [])

    def test_queue_and_initial_receipt_commit_atomically_once_after_verified_ledger_auth(self):
        one = self.workflow.consider(self.order_id)
        two = self.workflow.consider(self.order_id)
        self.assertEqual(one['order_id'], two['order_id'])
        self.assertEqual(self.count('shop_order_workflow_v2'), 1)
        self.assertEqual(self.count('shop_order_receipts_v2'), 1)
        self.assertEqual(self.outbox.sent, [])
        self.assertEqual(self.f.transport.calls, [])

    def test_authorized_status_without_verified_event_cannot_create_job_or_receipt(self):
        with closing(self.f.factory()) as db:
            db.execute('DELETE FROM shop_payment_events_v2')
            db.commit()
        self.assertIsNone(self.workflow.consider(self.order_id))
        self.assertEqual(self.count('shop_order_workflow_v2'), 0)
        self.assertEqual(self.count('shop_order_receipts_v2'), 0)

    def test_receipt_insert_failure_rolls_back_workflow_job(self):
        with patch.object(self.receipts, 'enqueue_in_transaction', side_effect=ReceiptError('synthetic-failure')):
            with self.assertRaises(WorkflowError):
                self.workflow.consider(self.order_id)
        self.assertEqual(self.count('shop_order_workflow_v2'), 0)
        self.assertEqual(self.count('shop_order_receipts_v2'), 0)

    def test_repair_scan_recovers_auth_commit_before_webhook_enqueue(self):
        self.assertEqual(self.workflow.repair(), 1)
        self.assertEqual(self.workflow.repair(), 0)
        self.assertEqual(self.count('shop_order_workflow_v2'), 1)

    def test_full_saga_then_confirmed_receipt_once_and_restart_no_supplier_resubmit(self):
        self.assertEqual(self.workflow.run_once()['state'], 'fulfilled')
        self.assertEqual(len(self.f.transport.calls), 4)
        self.assertEqual(self.f.payment.actions, ['capture'])
        self.assertEqual(self.count('shop_order_receipts_v2'), 2)
        self.assertEqual(self.workflow.run_once()['state'], 'idle')
        self.assertEqual(len(self.f.transport.calls), 4)
        self.assertEqual(self.f.payment.actions, ['capture'])
        self.assertEqual(len(self.outbox.sent), 2)
        customer = self.receipts.customer_receipt(self.order_id, customer_id='customer-a')
        self.assertEqual(customer['status'], 'paid')
        self.assertEqual(customer['invoice_status'], 'not_issued')
        self.assertFalse(customer['returns_enabled'])
        self.assertEqual(customer['email_state'], {'order_received': 'submitted', 'order_confirmed': 'submitted'})

    def test_partial_supplier_prefix_becomes_manual_without_confirmation_or_funds_move(self):
        self.f.transport.outcomes = ['accepted', 'rejected']
        self.assertEqual(self.workflow.run_once()['state'], 'manual_review')
        self.assertEqual(self.f.payment.actions, [])
        self.assertEqual(self.count('shop_order_receipts_v2'), 1)
        self.assertEqual(self.workflow.run_once()['state'], 'idle')
        self.assertEqual(len(self.f.transport.calls), 2)

    def test_stale_queue_lease_recovers_without_regranting_uncertain_supplier_post(self):
        self.workflow.consider(self.order_id)
        old = self.journal.claim(mode='test')
        self.f.transport.outcomes = ['timeout']
        self.assertEqual(self.f.run_saga()['state'], 'manual_review')
        self.f.now += timedelta(seconds=61)
        self.assertEqual(self.workflow.run_once()['state'], 'manual_review')
        self.assertEqual(len(self.f.transport.calls), 1)
        self.assertFalse(self.journal.finish(old, state='fulfilled'))

    def test_missing_confirmation_receipt_after_capture_recovers_without_another_capture(self):
        prepare = self.receipts.prepare
        def temporary_failure(order_id, **kwargs):
            if kwargs['kind'] == 'order_confirmed':
                raise ReceiptError('synthetic-temporary-legal-unavailable')
            return prepare(order_id, **kwargs)
        with patch.object(self.receipts, 'prepare', temporary_failure):
            self.assertEqual(self.workflow.run_once()['state'], 'deferred')
        self.assertEqual(self.f.payment.actions, ['capture'])
        self.f.now += timedelta(seconds=61)
        self.assertEqual(self.workflow.run_once()['state'], 'fulfilled')
        self.assertEqual(self.f.payment.actions, ['capture'])
        self.assertEqual(len(self.f.transport.calls), 4)

    def test_unknown_mail_is_not_automatically_submitted_again(self):
        self.workflow.consider(self.order_id)
        self.outbox.fail = True
        self.assertEqual(self.receipts.deliver_once()['state'], 'unknown')
        self.assertEqual(self.receipts.deliver_once()['state'], 'idle')
        self.assertEqual(len(self.outbox.sent), 1)
        self.assertEqual(self.receipts.reconcile(self.order_id, customer_id='customer-a', kind='order_received')['state'], 'submitted')
        self.assertEqual(len(self.outbox.sent), 1)

    def test_payment_failure_after_queue_does_not_repeatedly_run_procurement(self):
        self.workflow.consider(self.order_id)
        self.f.now += timedelta(seconds=30)
        self.f.store.apply_verified_event(self.f.event('failed', 'evt_SYNTHETIC_LATER_FAILED'), verified=True)
        self.assertEqual(self.workflow.run_once()['state'], 'manual_review')
        self.assertEqual(self.f.transport.calls, [])
        self.assertEqual(self.workflow.run_once()['state'], 'idle')

    def test_delivery_disabled_never_calls_smtp_or_reads_smtp_config(self):
        self.workflow.consider(self.order_id)
        self.receipts.delivery_enabled = False
        self.receipts.smtp_config = lambda: (_ for _ in ()).throw(AssertionError('Must not read SMTP config'))
        self.assertEqual(self.receipts.deliver_once()['state'], 'disabled')
        self.assertEqual(self.outbox.sent, [])

    def test_receipt_ownership_privacy_frozen_totals_and_legal_attachment(self):
        self.workflow.consider(self.order_id)
        self.receipts.deliver_once()
        message = self.outbox.sent[0][1]
        self.assertEqual(str(message['To']), 'customer@example.test')
        body = message.get_body(preferencelist=('plain',)).get_content()
        self.assertIn('SYNTHETIC agb', body)
        self.assertIn('keine Rechnung', body)
        self.assertNotIn('unit_net', body)
        self.assertNotIn('supplier_article_id', body)
        self.assertEqual(next(message.iter_attachments()).get_payload(decode=True), LEGAL['attachments'][0]['content'])
        public = self.receipts.customer_receipt(self.order_id, customer_id='customer-a')
        self.assertNotIn('customer@example.test', json.dumps(public))
        self.assertNotIn('Teststraße', json.dumps(public))
        with self.assertRaises(ReceiptError):
            self.receipts.customer_receipt(self.order_id, customer_id='customer-b')
        with self.assertRaises(Exception):
            self.workflow.customer_status(self.order_id, customer_id='customer-b')

    def test_mutating_legal_snapshot_cannot_replace_already_queued_receipt(self):
        self.workflow.consider(self.order_id)
        self.receipts.legal_snapshot = lambda oid, **kwargs: {**deepcopy(LEGAL), 'version': 'changed'}
        with self.assertRaises(ReceiptError):
            self.receipts.enqueue(self.order_id, customer_id='customer-a', kind='order_received')
        self.assertEqual(self.count('shop_order_receipts_v2'), 1)

    def test_concurrent_workers_have_one_queue_lease(self):
        self.workflow.consider(self.order_id)
        barrier, outcomes = threading.Barrier(2), []
        def claim():
            barrier.wait(5)
            outcomes.append(self.journal.claim(mode='test'))
        workers = [threading.Thread(target=claim) for _ in range(2)]
        for worker in workers: worker.start()
        for worker in workers:
            worker.join(5)
            self.assertFalse(worker.is_alive())
        self.assertEqual(sum(value is not None for value in outcomes), 1)

    def test_real_existing_mail_outbox_is_reused_with_fake_smtp_and_no_retry(self):
        self.workflow.consider(self.order_id)
        def mail_db():
            db = self.f.factory()
            db.row_factory = sqlite3.Row
            return db
        outbox = MailOutbox(mail_db, Path(self.f.temp.name) / 'mail-outbox', object())
        smtp = []
        def submit(row, config, raw):
            smtp.append(raw)
            outbox._record(row, state='copy_pending', accepted=[row['recipients'][0]], refused={}, copy_done=0)
        def copy(row):
            outbox._record(row, state='sent', copy_done=1)
        self.receipts.outbox = outbox
        with patch.object(outbox, '_preflight', lambda row, config: row.update(sent_folder='Sent')), \
             patch.object(outbox, '_smtp', submit), patch.object(outbox, '_copy', copy):
            self.assertEqual(self.receipts.deliver_once()['state'], 'submitted')
            self.assertEqual(self.receipts.deliver_once()['state'], 'idle')
        self.assertEqual(len(smtp), 1)

    def test_checkout_facade_uses_raw_verifier_and_status_never_enqueues(self):
        calls = []
        class Checkout:
            mode = 'test'
            def readiness(inner): return {'ready': True, 'mode': 'test', 'capture_method': 'manual', 'payment_methods': ['card']}
            def status(inner, oid, **kwargs): return {'status': 'authorized'}
            def handle_webhook(inner, raw, signature):
                calls.append((raw, signature))
                if signature != 'synthetic-verified': raise ValueError('invalid-signature')
                return {'ignored': False, 'order_id': self.order_id, 'status': 'authorized'}
        facade = WorkflowCheckout(Checkout(), self.workflow)
        facade.status(self.order_id, customer_id='customer-a')
        self.assertEqual(self.count('shop_order_workflow_v2'), 0)
        with self.assertRaises(ValueError): facade.handle_webhook(b'raw', 'invalid')
        self.assertEqual(self.count('shop_order_workflow_v2'), 0)
        facade.handle_webhook(b'raw', 'synthetic-verified')
        facade.handle_webhook(b'raw', 'synthetic-verified')
        self.assertEqual(self.count('shop_order_workflow_v2'), 1)
        self.assertEqual(calls[0], (b'raw', 'invalid'))


if __name__ == '__main__':
    unittest.main()
