"""Invoice integration on an isolated shop ledger. All provider effects are in memory."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import json
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'scripts')]

from shop_fulfillment import init_fulfillment_schema
from shop_invoice_service import ShopInvoiceError, ShopInvoiceService
from shop_lexware_invoices import ShopLexwareError
from shop_order_receipts import ReceiptError, ShopOrderReceipts, init_order_receipt_schema
from shop_order_store import OrderStore, init_schema
from shop_order_workflow import init_order_workflow_schema
from shop_payment_actions import PaymentActionJournal, init_payment_action_schema
from test_shop_order_store import NOW, quote, rehash


class InMemoryLexware:
    def __init__(self):
        self.records = {}
        self.writes = []
        self.lookups = []
        self.pdf_reads = []
        self.voucher_number = 'RE-TEST-1'
        self.lose_reply = False

    def configured(self):
        return True

    def create_invoice(self, payload, *, finalize):
        identifier = str(uuid4())
        self.writes.append((deepcopy(payload), finalize))
        self.records[identifier] = (deepcopy(payload), finalize)
        if self.lose_reply:
            raise ShopLexwareError('lexware_invoice_response_unavailable')
        return identifier

    def recover(self, payload):
        self.lookups.append(deepcopy(payload))
        matching = [key for key, (record, _) in self.records.items() if record == payload]
        if len(matching) > 1:
            raise ShopLexwareError('lexware_invoice_search_ambiguous')
        return matching[0] if matching else None

    def inspect(self, identifier, payload):
        record, final = self.records[identifier]
        assert record == payload
        return {'state': 'final' if final else 'draft', 'voucher_status': 'open' if final else 'draft',
                'voucher_number': self.voucher_number if final else ''}

    def pdf(self, identifier, payload, *, voucher_number=None):
        remote = self.inspect(identifier, payload)
        if remote['state'] != 'final' or remote['voucher_number'] != voucher_number:
            raise ShopLexwareError('lexware_invoice_binding_mismatch')
        self.pdf_reads.append(identifier)
        return b'%PDF-1.7\nSYNTHETIC\n%%EOF'


class InvoiceServiceTests(unittest.TestCase):
    def setUp(self):
        network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden'))
        network.start()
        self.addCleanup(network.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        db_path = Path(temporary.name) / 'invoice-integration.sqlite'
        self.factory = lambda: sqlite3.connect(db_path, timeout=10)
        with closing(self.factory()) as db:
            init_schema(db)
            init_payment_action_schema(db)
            init_fulfillment_schema(db)
            init_order_workflow_schema(db)
            init_order_receipt_schema(db)
        self.now = NOW
        self.store = OrderStore(self.factory, clock=lambda: self.now)
        self.actions = PaymentActionJournal(self.factory, clock=lambda: self.now)
        self.adapter = InMemoryLexware()
        self.serial = 0

    def service(self):
        return ShopInvoiceService(self.store, self.adapter, clock=lambda: self.now.timestamp())

    def order(self, *, mode='live', capture=True, paid=True, fulfilled=True, positions=('accepted', 'accepted')):
        self.serial += 1
        customer = 'buyer-' + str(self.serial)
        address = {'recipient': 'Synthetic Example', 'street': 'Teststraße 12', 'postcode': '74821',
                   'city': 'Mosbach', 'country': 'DE'}
        context = {'contact': {'name': 'Synthetic Example', 'email': 'buyer@example.test'},
                   'billing': address, 'delivery': {**address, 'street': 'Anderer Testweg 8'}}
        snapshot = quote(mode=mode, customer=customer, context=context)
        snapshot['created_at'] = (self.now - timedelta(seconds=5)).isoformat()
        snapshot['expires_at'] = (self.now + timedelta(minutes=10)).isoformat()
        snapshot = rehash(snapshot)
        digest = snapshot['quote_hash']
        self.store.save_quote(snapshot, expected_quote_hash=digest, customer_id=customer, mode=mode,
                              delivery_context=context)
        order = self.store.create_order(digest, customer_id=customer, mode=mode, idempotency_key='receipt-' + customer)
        oid = order['id']
        attempt = self.store.claim_payment(oid, customer_id=customer, mode=mode)
        binding = {'order_id': oid, 'customer_id': customer, 'mode': mode, 'currency': 'eur',
                   'amount_gross_cent': order['amount_gross_cent'], 'quote_hash': digest, 'payment_attempt': attempt['id'],
                   'session_id': f'cs_{mode}_synthetic{self.serial}', 'intent_id': f'pi_synthetic{self.serial}'}
        self.store.bind_payment(oid, attempt['id'], customer_id=customer, mode=mode, session_id=binding['session_id'],
                                intent_id=binding['intent_id'], amount_gross_cent=binding['amount_gross_cent'], currency='eur')
        stamp = self.now.isoformat()
        if paid:
            self.store.apply_verified_event({'event_id': 'evt_synthetic' + str(self.serial), 'order_id': oid,
                'attempt_id': attempt['id'], 'kind': 'paid', 'occurred_at': stamp, 'mode': mode, 'currency': 'eur',
                'amount_gross_cent': binding['amount_gross_cent'], 'session_id': binding['session_id'],
                'intent_id': binding['intent_id']}, verified=True)
        if capture:
            action = self.actions.claim(binding, 'capture', {'verified': True, 'persisted': True})
            self.actions.settle(action['id'], binding, {'state': 'captured', 'observed_at': stamp})
        with closing(self.factory()) as db:
            encoded = json.dumps(binding)
            db.execute('''INSERT INTO shop_order_workflow_v2
                (order_id,customer_id,mode,binding_json,state,created_at,updated_at) VALUES (?,?,?,?,?,?,?)''',
                (oid, customer, mode, encoded, 'fulfilled' if fulfilled else 'manual_review', stamp, stamp))
            db.execute('''INSERT INTO shop_fulfillment_v2
                (order_id,customer_id,mode,binding_json,plan_hash,lines_json,policy_json,state,action_state,created_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (oid, customer, mode, encoded, digest, json.dumps(snapshot['lines']), '{}', 'accepted',
                 'captured' if capture else None, stamp, stamp))
            for index, state in enumerate(positions):
                item = snapshot['lines'][index]
                db.execute('''INSERT INTO shop_fulfillment_positions_v3
                    (order_id,child_key,destination,item_json,reference,state,updated_at) VALUES (?,?,?,?,?,?,?)''',
                    (oid, item['id'], item['destination'], json.dumps(item), oid + str(index), state, stamp))
            db.commit()
        return oid

    def test_new_paid_order_finalized_exactly_once_including_services_and_both_shipments(self):
        service = self.service()
        self.now += timedelta(minutes=2)
        oid = self.order()
        self.assertTrue(service.process_next())
        self.assertFalse(self.service().process_next())
        self.assertEqual(len(self.adapter.writes), 1)
        payload, final = self.adapter.writes[0]
        self.assertTrue(final)
        self.assertEqual(payload['address']['street'], 'Teststraße 12')
        self.assertEqual(payload['taxConditions']['taxType'], 'gross')
        total = sum(Decimal(str(item['unitPrice']['grossAmount'])) * item['quantity'] for item in payload['lineItems'])
        self.assertEqual(total, Decimal('532.00'))
        names = [item['name'] for item in payload['lineItems']]
        for term in ('Montage', 'Versand zur Werkstatt', 'Direktversand zum Kunden'):
            self.assertTrue(any(term in name for name in names), names)
        self.assertEqual(service.status(oid)['state'], 'ready')
        self.assertEqual(service.statuses([oid])[oid]['voucher_number'], 'RE-TEST-1')
        document, filename = service.pdf(oid)
        self.assertTrue(document.startswith(b'%PDF-'))
        self.assertTrue(filename.endswith('.pdf'))
        self.assertEqual(len(self.adapter.writes), 1)

    def test_existing_order_never_issued_by_scanner_or_restart(self):
        oid = self.order()
        self.now += timedelta(minutes=2)
        service = self.service()
        self.assertFalse(service.process_next())
        self.assertEqual(service.status(oid)['state'], 'available')
        self.now += timedelta(minutes=3)
        self.assertFalse(self.service().process_next())
        self.assertFalse(self.adapter.writes)
        self.assertEqual(service.prepare(oid)['state'], 'ready')
        self.assertEqual(service.prepare(oid)['state'], 'ready')
        self.assertEqual(len(self.adapter.writes), 1)

    def test_missing_evidence_or_test_mode_never_opens_an_invoice(self):
        service = self.service()
        self.now += timedelta(seconds=3)
        ids = [self.order(**flags) for flags in ({'mode': 'test'}, {'capture': False}, {'paid': False},
               {'fulfilled': False}, {'positions': ('accepted', 'rejected')}, {'positions': ()})]
        for oid in ids:
            self.assertEqual(service.status(oid)['state'], 'ineligible')
            with self.assertRaises(ShopInvoiceError):
                service.prepare(oid)
        self.assertFalse(service.process_next())
        self.assertFalse(self.adapter.writes)

    def test_lost_create_reply_can_be_reconciled_but_never_posts_again(self):
        service = self.service()
        self.now += timedelta(minutes=1)
        oid = self.order()
        self.adapter.lose_reply = True
        service.process_next()
        self.assertEqual(service.status(oid)['state'], 'unknown')
        self.assertEqual(len(self.adapter.writes), 1)
        self.assertFalse(self.service().process_next())
        service.refresh(oid)
        self.assertEqual(service.status(oid)['state'], 'ready')
        self.assertEqual(len(self.adapter.writes), 1)
        self.assertTrue(self.adapter.lookups)

    def test_absent_result_after_ambiguous_create_never_grants_second_write(self):
        service = self.service()
        self.now += timedelta(minutes=1)
        oid = self.order()
        with patch.object(self.adapter, 'create_invoice', side_effect=ShopLexwareError('lexware_invoice_response_unavailable')) as attempt:
            self.assertEqual(service.prepare(oid)['state'], 'unknown')
            self.now += timedelta(days=2)
            self.assertTrue(self.service().process_next())
            service.refresh(oid)
            self.assertEqual(service.prepare(oid)['state'], 'unknown')
            self.assertEqual(attempt.call_count, 1)
        self.assertGreaterEqual(len(self.adapter.lookups), 2)
        self.assertFalse(self.adapter.writes)

    def test_only_failed_read_before_post_can_retry_and_keeps_original_invoice_date(self):
        service = self.service()
        self.now += timedelta(minutes=1)
        oid = self.order()
        with patch.object(self.adapter, 'create_invoice', side_effect=ShopLexwareError('lexware_invoice_preflight_unavailable')) as preflight:
            self.assertTrue(service.process_next())
            frozen = deepcopy(preflight.call_args.args[0])
        self.assertFalse(self.adapter.writes)
        self.assertFalse(service.process_next())
        self.now += timedelta(days=1)
        self.assertTrue(self.service().process_next())
        self.assertEqual(service.status(oid)['state'], 'ready')
        self.assertEqual(self.adapter.writes, [(frozen, True)])

    def test_existing_draft_keeps_same_external_id_until_finalized_in_lexware(self):
        service = self.service()
        self.now += timedelta(minutes=1)
        oid = self.order()
        identifier = str(uuid4())

        def find_existing_draft(payload, *, finalize):
            # Adapter preflight finds an earlier persisted draft for this reference.
            self.adapter.records[identifier] = (deepcopy(payload), False)
            return identifier

        with patch.object(self.adapter, 'create_invoice', side_effect=find_existing_draft) as request:
            self.assertEqual(service.prepare(oid)['state'], 'draft')
            self.assertIn(identifier, service.status(oid)['edit_url'])
            self.assertEqual(service.prepare(oid)['state'], 'draft')
            with self.assertRaises(ShopInvoiceError):
                service.pdf(oid)
            payload, _ = self.adapter.records[identifier]
            self.adapter.records[identifier] = (payload, True)
            self.assertEqual(self.service().refresh(oid)['state'], 'ready')
            self.assertEqual(request.call_count, 1)
        service.pdf(oid)
        self.assertEqual(self.adapter.pdf_reads, [identifier])
        self.assertFalse(self.adapter.writes)

    def test_invalid_provider_capture_evidence_cannot_starve_next_verified_order(self):
        service = self.service()
        self.now += timedelta(minutes=1)
        damaged = self.order()
        with closing(self.factory()) as db:
            db.execute("UPDATE shop_payment_actions_v2 SET result_json='{}' WHERE order_id=?", (damaged,))
            db.commit()
        self.now += timedelta(minutes=1)
        unaffected = self.order()
        self.assertTrue(service.process_next())
        self.assertTrue(service.process_next())
        self.assertEqual(service.status(damaged)['state'], 'blocked')
        self.assertEqual(service.status(unaffected)['state'], 'ready')
        self.assertEqual(len(self.adapter.writes), 1)

    def test_two_workers_cannot_create_parallel_documents_for_one_order(self):
        workers = [self.service(), self.service()]
        self.now += timedelta(minutes=1)
        oid = self.order()
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda worker: worker.process_next(), workers))
        self.assertEqual(len(self.adapter.writes), 1)
        self.assertEqual(workers[0].status(oid)['state'], 'ready')

    def test_pdf_rechecks_number_against_original_final_invoice(self):
        service = self.service()
        oid = self.order()
        self.assertEqual(service.prepare(oid)['voucher_number'], 'RE-TEST-1')
        self.adapter.voucher_number = 'RE-UNRELATED'
        with patch.object(self.adapter, 'pdf', wraps=self.adapter.pdf) as download:
            with self.assertRaises(ShopInvoiceError):
                service.pdf(oid)
            self.assertEqual(download.call_args.kwargs, {'voucher_number': 'RE-TEST-1'})
        self.assertFalse(self.adapter.pdf_reads)
        self.assertEqual(service.status(oid)['voucher_number'], 'RE-TEST-1')
        self.assertEqual(len(self.adapter.writes), 1)

    def test_customer_receipt_shows_only_own_issued_invoice_number_without_provider_access(self):
        oid = self.order()
        service = self.service()
        receipts = ShopOrderReceipts(self.store, self.factory, mode='live', sender_address='shop@example.test')
        before = receipts.customer_receipt(oid, customer_id='buyer-1')
        self.assertEqual((before['invoice_status'], before['invoice']), ('not_issued', None))
        service.prepare(oid)
        after = receipts.customer_receipt(oid, customer_id='buyer-1')
        self.assertEqual((after['invoice_status'], after['invoice']), ('issued', {'number': 'RE-TEST-1'}))
        with self.assertRaises(ReceiptError):
            receipts.customer_receipt(oid, customer_id='buyer-unrelated')
        self.assertFalse(self.adapter.pdf_reads)
        self.assertEqual(len(self.adapter.writes), 1)


if __name__ == '__main__':
    unittest.main()
