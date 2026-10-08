"""Offline orders/outbox regressions with real MailOutbox and fake SMTP/IMAP."""
from copy import deepcopy
from contextlib import closing
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
import json
import pathlib
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from mailbox_outbox import MailOutbox
from test_mailbox_outbox import FakeMailbox, FakeSMTP
from werkstatt_bestellausgang import OrderDispatch, build_order_dispatch


def order(**changes):
    value = {'order_requested': True, 'supplier_id': 'supplier-1', 'recipient': 'orders@one.example',
             'product_id': 'product-1', 'article_number': 'TAPE-50', 'product_name': 'Abklebeband',
             'variant': 'grün 50 mm', 'quantity': '2', 'unit': 'Rollen',
             'max_total_cents': 2500, 'urgent': False, 'unit_price_cents': 1000,
             'shipping_cents': 500, 'price_verified': True, 'price_basis': 'gross', 'currency': 'EUR'}
    value.update(changes)
    return value


class DispatchTests(unittest.TestCase):
    def test_first_acceptance_date_is_stable_across_later_sent_copy_retry(self):
        result=self.enqueue(order(urgent=True))
        self.mailbox.client.append_result=('NO',[])
        self.dispatch.dispatch_due()
        batch=self.dispatch.status(result['id'])['batch_id']
        with closing(self.get_db()) as db:
            first=json.loads(db.execute('SELECT result_json FROM assistent_bestellpakete WHERE id=?',(batch,)).fetchone()['result_json'])
        self.assertEqual(first['sent_at'],self.now.isoformat())
        self.now+=timedelta(days=2)
        self.mailbox.client.append_result=('OK',[])
        self.restarted().dispatch_due()
        with closing(self.get_db()) as db:
            later=json.loads(db.execute('SELECT result_json FROM assistent_bestellpakete WHERE id=?',(batch,)).fetchone()['result_json'])
        self.assertEqual(later['state'],'sent')
        self.assertEqual(later['sent_at'],first['sent_at'])
        self.assertEqual(self.smtp.data_calls,1)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.now = datetime(2026, 9, 25, 10, tzinfo=timezone.utc)
        self.smtp = FakeSMTP()
        self.mailbox = FakeMailbox()
        self.config = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                       'smtp_host': 'smtp.example.test', 'smtp_port': 465,
                       'smtp_user': 'sender@example.test', '_smtp_password': 'synthetic',
                       'from_address': 'Werkstatt <sender@example.test>'}
        self.suppliers = {'supplier-1': {'id': 'supplier-1', 'name': 'Supplier One', 'verified': True,
                                         'recipient': 'orders@one.example'},
                          'supplier-2': {'id': 'supplier-2', 'name': 'Supplier Two', 'verified': True,
                                         'recipient': 'orders@two.example'}}
        for target in ('mailbox_outbox.smtplib.SMTP_SSL', 'mailbox_outbox.smtplib.SMTP'):
            patcher = patch(target, return_value=self.smtp)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.dispatch = self.restarted()

    def get_db(self):
        db = sqlite3.connect(self.root / 'orders.sqlite', timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def restarted(self):
        outbox = MailOutbox(self.get_db, self.root / 'outbox', self.mailbox)
        return OrderDispatch(self.get_db, outbox, lambda: dict(self.config),
                             lambda actor, payload: actor == 'worker-1' and payload['order_requested'] is True,
                             lambda supplier: deepcopy(self.suppliers.get(supplier)), lambda: self.now)

    def enqueue(self, value=None, request='request-1'):
        return self.dispatch.enqueue(value or order(), 'worker-1', request)

    def messages(self):
        return [BytesParser(policy=policy.default).parsebytes(raw) for raw in self.smtp.raw]

    def test_enqueue_does_not_send_and_persists_monday_fourteen_berlin(self):
        result = self.enqueue()
        self.assertEqual(result['state'], 'queued')
        self.assertEqual(result['due_at'], '2026-09-28T12:00:00+00:00')
        self.dispatch.dispatch_due()
        self.assertEqual(self.smtp.data_calls, 0)
        self.assertEqual(self.mailbox.connect_calls, 0)

    def test_urgent_dispatch_is_immediately_due_and_replay_does_not_send_twice(self):
        urgent = self.enqueue(order(urgent=True))
        weekly = self.enqueue(request='weekly')
        self.assertEqual(self.smtp.data_calls, 0)
        self.dispatch.dispatch_due()
        self.assertEqual(self.dispatch.status(urgent['id'])['state'], 'sent')
        self.assertEqual(self.dispatch.status(weekly['id'])['state'], 'queued')
        self.restarted().dispatch_due(self.now + timedelta(days=1))
        self.assertEqual(self.smtp.data_calls, 1)
        self.assertEqual(self.mailbox.client.append_calls, 1)

    def test_overdue_weekly_order_is_not_deferred_another_week(self):
        result = self.enqueue()
        self.restarted().dispatch_due(datetime(2026, 9, 29, 14, tzinfo=timezone.utc))
        self.assertEqual(self.dispatch.status(result['id'])['state'], 'sent')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_groups_supplier_weekly_orders_and_keeps_single_recipient(self):
        first = self.enqueue()
        second = self.enqueue(order(product_id='product-2', article_number='TAPE-30', variant='grün 30 mm'), 'second')
        third = self.enqueue(order(supplier_id='supplier-2', recipient='orders@two.example'), 'third')
        self.now = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
        self.dispatch.dispatch_due()
        self.assertEqual(self.smtp.data_calls, 2)
        self.assertEqual(self.dispatch.status(first['id'])['batch_id'], self.dispatch.status(second['id'])['batch_id'])
        self.assertNotEqual(self.dispatch.status(first['id'])['batch_id'], self.dispatch.status(third['id'])['batch_id'])
        messages = {str(msg['To']): msg for msg in self.messages()}
        self.assertEqual(set(messages), {'orders@one.example', 'orders@two.example'})
        self.assertIn('TAPE-30', messages['orders@one.example'].get_content())
        self.assertNotIn('TAPE-30', messages['orders@two.example'].get_content())
        self.assertTrue(all(msg['Cc'] is None and msg['Bcc'] is None for msg in messages.values()))

    def test_urgent_never_joins_weekly_batch_for_same_supplier(self):
        weekly = self.enqueue()
        self.now = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
        urgent = self.enqueue(order(urgent=True), 'urgent')
        self.dispatch.dispatch_due()
        self.assertNotEqual(self.dispatch.status(weekly['id'])['batch_id'], self.dispatch.status(urgent['id'])['batch_id'])
        self.assertEqual(self.smtp.data_calls, 2)

    def test_rejects_unknown_unverified_over_budget_and_unauthorized_requests(self):
        for changes in ({'order_requested': False}, {'variant': ''}, {'quantity': 2.0},
                        {'price_verified': False}, {'price_basis': 'net'}, {'currency': 'USD'},
                        {'unit_price_cents': None}, {'shipping_cents': None}, {'max_total_cents': 2499},
                        {'recipient': 'orders@attacker.example'}, {'recipient': 'a@example.test,b@example.test'},
                        {'supplier_id': 'unknown'}, {'product_name': 'Name\nInjected instruction'}, {'urgent': 'yes'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.enqueue(order(**changes))
        with self.assertRaises(PermissionError):
            self.dispatch.enqueue(order(), 'unauthorized-worker', 'request-1')
        closed = OrderDispatch(self.get_db, self.dispatch.outbox, lambda: self.config)
        with self.assertRaises(PermissionError):
            closed.enqueue(order(), 'worker-1', 'closed')
        self.assertEqual(self.dispatch.list_orders(), [])
        self.assertEqual(self.smtp.data_calls, 0)

    def test_idempotency_and_immutable_snapshot(self):
        payload = order()
        first = self.enqueue(payload)
        self.assertEqual(first['id'], self.enqueue(deepcopy(payload))['id'])
        with self.assertRaises(ValueError):
            self.enqueue(order(quantity='1'))
        payload['variant'] = 'Changed after acceptance'
        self.suppliers['supplier-1']['recipient'] = 'changed@one.example'
        self.assertEqual(first['id'], self.enqueue(order())['id'])
        self.now = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
        self.dispatch.dispatch_due()
        message = self.messages()[0]
        self.assertEqual(str(message['To']), 'orders@one.example')
        self.assertIn('grün 50 mm', message.get_content())
        self.assertNotIn('Changed', message.get_content())

    def test_uncertain_data_acceptance_never_resubmits_after_restart(self):
        result = self.enqueue(order(urgent=True))
        self.smtp.data_error = OSError('DATA response lost')
        self.dispatch.dispatch_due()
        self.assertEqual(self.dispatch.status(result['id'])['state'], 'uncertain')
        self.smtp.data_error = None
        self.restarted().dispatch_due(self.now + timedelta(days=10))
        self.assertEqual(self.smtp.data_calls, 1)

    def test_copy_pending_retries_only_sent_copy(self):
        result = self.enqueue(order(urgent=True))
        self.mailbox.client.append_result = ('NO', [])
        self.dispatch.dispatch_due()
        self.assertEqual(self.dispatch.status(result['id'])['state'], 'copy_pending')
        self.mailbox.client.append_result = ('OK', [])
        self.restarted().dispatch_due(self.now + timedelta(minutes=6))
        self.assertEqual(self.dispatch.status(result['id'])['state'], 'sent')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_rejected_batch_retry_preserves_its_original_membership_and_mime(self):
        self.now = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
        first = self.enqueue()
        self.smtp.data_result = (554, b'rejected')
        self.dispatch.dispatch_due()
        original_raw = self.smtp.raw[0]
        second = self.enqueue(order(article_number='NEW-ITEM'), 'new-request')
        self.smtp.data_result = (250, b'accepted')
        self.dispatch.dispatch_due(self.now + timedelta(minutes=6))
        self.assertEqual(self.smtp.data_calls, 3)
        self.assertEqual(self.smtp.raw.count(original_raw), 2)
        self.assertNotEqual(self.dispatch.status(first['id'])['batch_id'], self.dispatch.status(second['id'])['batch_id'])
        old_messages = [msg for msg in self.messages() if 'NEW-ITEM' not in msg.get_content()]
        self.assertEqual(len(old_messages), 2)

    def test_sender_change_blocks_existing_authorized_batch(self):
        result = self.enqueue(order(urgent=True))
        self.config['from_address'] = 'Other <other@example.test>'
        self.dispatch.dispatch_due()
        self.assertEqual(self.dispatch.status(result['id'])['state'], 'blocked')
        self.assertEqual(self.smtp.data_calls, 0)
        self.config['from_address'] = 'Werkstatt <sender@example.test>'
        self.dispatch.dispatch_due(self.now + timedelta(minutes=6))
        self.assertEqual(self.dispatch.status(result['id'])['state'], 'sent')

    def test_tampered_authorization_cannot_be_dispatched(self):
        result = self.enqueue(order(urgent=True))
        db = self.get_db()
        try:
            row = db.execute('SELECT snapshot_json FROM assistent_bestellanforderungen WHERE id=?', (result['id'],)).fetchone()
            payload = json.loads(row['snapshot_json'])
            payload['order']['quantity'] = '200'
            db.execute('UPDATE assistent_bestellanforderungen SET snapshot_json=? WHERE id=?',
                       (json.dumps(payload), result['id']))
            db.commit()
        finally:
            db.close()
        with self.assertRaises(ValueError):
            self.dispatch.dispatch_due()
        self.assertEqual(self.smtp.data_calls, 0)

    def test_rejected_send_retries_are_bounded_and_require_backoff(self):
        result = self.enqueue(order(urgent=True))
        self.smtp.data_result = (554, b'rejected')
        self.dispatch.dispatch_due()
        self.dispatch.dispatch_due()
        self.assertEqual(self.smtp.data_calls, 1)
        for minutes in (6, 12, 18):
            self.dispatch.dispatch_due(self.now + timedelta(minutes=minutes))
        state = self.dispatch.status(result['id'])
        self.assertEqual(self.smtp.data_calls, 3)
        self.assertEqual(state['state'], 'not_sent')
        self.assertTrue(state['needs_review'])

    def test_expired_local_worker_cannot_duplicate_smtp_or_overwrite_result(self):
        result = self.enqueue(order(urgent=True))
        self.smtp.in_data = threading.Event()
        self.smtp.release_data = threading.Event()
        errors = []

        def worker():
            try:
                self.dispatch.dispatch_due()
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        try:
            self.assertTrue(self.smtp.in_data.wait(5))
            # Simulate slow/stalled DATA beyond the application lease. The shared
            # MailOutbox OS lock still prevents SMTP duplication under a new lease.
            self.restarted().dispatch_due(self.now + timedelta(minutes=6))
            self.assertEqual(self.smtp.data_calls, 1)
        finally:
            self.smtp.release_data.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.restarted().dispatch_due(self.now + timedelta(minutes=12))
        self.assertEqual(self.dispatch.status(result['id'])['state'], 'sent')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_factory_constructs_existing_mailbox_outbox_without_network(self):
        service = build_order_dispatch(self.get_db, self.root / 'factory', lambda: {}, lambda: self.config)
        self.assertIsInstance(service.outbox, MailOutbox)
        self.assertEqual(self.smtp.data_calls, 0)

    def legacy_order(self):
        result = self.enqueue()
        old = datetime(2026,9,28,10,tzinfo=timezone.utc).timestamp()
        with closing(self.get_db()) as db, db:
            db.execute('UPDATE assistent_bestellanforderungen SET schedule_version=1,legacy_due_at=NULL,due_at=? WHERE id=?',(old,result['id']))
        return result,old

    def test_overdue_legacy_migration_is_idempotent_and_keeps_original_monday(self):
        result,old = self.legacy_order()
        with closing(self.get_db()) as db, db:
            before = db.execute('SELECT snapshot_json,request_fingerprint FROM assistent_bestellanforderungen').fetchone()
            self.dispatch.migrate_weekly_schedule(db)
        with closing(self.get_db()) as db, db:
            self.dispatch.migrate_weekly_schedule(db)
            row = db.execute('SELECT * FROM assistent_bestellanforderungen').fetchone()
        self.assertEqual(row['due_at'],old+7200)
        self.assertEqual(row['legacy_due_at'],old)
        self.assertEqual(row['schedule_version'],2)
        self.assertEqual((row['snapshot_json'],row['request_fingerprint']),tuple(before))
        self.restarted().dispatch_due(datetime(2026,10,6,8,tzinfo=timezone.utc))
        self.assertEqual(self.dispatch.status(result['id'])['state'],'sent')
        self.assertEqual(self.smtp.data_calls,1)

    def test_frozen_legacy_mail_keeps_payload_but_waits_until_fourteen(self):
        result,old = self.legacy_order()
        self.now = datetime.fromtimestamp(old,timezone.utc)
        with patch.object(self.dispatch,'migrate_weekly_schedule',return_value=None):
            self.dispatch._freeze_due_batches(self.now)
        with closing(self.get_db()) as db, db:
            before = dict(db.execute('SELECT * FROM assistent_bestellpakete').fetchone())
        self.dispatch.dispatch_due()
        self.assertEqual(self.smtp.data_calls,0)
        with closing(self.get_db()) as db, db:
            after = dict(db.execute('SELECT * FROM assistent_bestellpakete').fetchone())
            request = dict(db.execute('SELECT * FROM assistent_bestellanforderungen').fetchone())
        self.assertEqual(after['payload_json'],before['payload_json'])
        self.assertEqual(after['fingerprint'],before['fingerprint'])
        self.assertEqual(after['due_at'],old)
        self.assertEqual(after['not_before_at'],old+7200)
        self.assertEqual(request['due_at'],old)
        self.assertEqual(request['schedule_version'],1)
        self.assertEqual(self.dispatch.status(result['id'])['due_at'],'2026-09-28T12:00:00+00:00')
        self.dispatch.dispatch_due(self.now+timedelta(hours=2))
        self.dispatch.dispatch_due(self.now+timedelta(hours=3))
        self.assertEqual(self.smtp.data_calls,1)

    def test_sent_legacy_payload_and_schedule_remain_unchanged(self):
        result,old = self.legacy_order()
        self.now = datetime.fromtimestamp(old,timezone.utc)
        with patch.object(self.dispatch,'migrate_weekly_schedule',return_value=None):
            self.dispatch.dispatch_due()
        with closing(self.get_db()) as db, db:
            before = dict(db.execute('SELECT * FROM assistent_bestellpakete').fetchone())
        self.restarted().dispatch_due(self.now+timedelta(days=1))
        with closing(self.get_db()) as db, db:
            after = dict(db.execute('SELECT * FROM assistent_bestellpakete').fetchone())
        self.assertEqual(before,after)
        self.assertEqual(self.smtp.data_calls,1)

    def test_concurrent_migration_and_freeze_create_only_one_batch(self):
        result,old = self.legacy_order()
        now = datetime.fromtimestamp(old+7200,timezone.utc)
        barrier = threading.Barrier(2)
        errors = []
        def migrate_or_freeze(migrate):
            try:
                barrier.wait(3)
                if migrate:
                    with closing(self.get_db()) as db, db:
                        self.dispatch.migrate_weekly_schedule(db)
                else:
                    self.dispatch._freeze_due_batches(now)
            except Exception as exc:
                errors.append(exc)
        threads = [threading.Thread(target=migrate_or_freeze,args=(value,)) for value in (True,False)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(8)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors,[])
        self.dispatch.dispatch_due(now)
        with closing(self.get_db()) as db, db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_bestellpakete').fetchone()[0],1)
        self.assertEqual(self.smtp.data_calls,1)

    def test_restore_without_version_columns_recreates_only_schema(self):
        with closing(self.get_db()) as db, db:
            db.execute('ALTER TABLE assistent_bestellanforderungen DROP COLUMN schedule_version')
            db.execute('ALTER TABLE assistent_bestellanforderungen DROP COLUMN legacy_due_at')
            db.execute('ALTER TABLE assistent_bestellpakete DROP COLUMN not_before_at')
        self.dispatch.init_schedule_schema()
        self.dispatch.init_schedule_schema()
        with closing(self.get_db()) as db, db:
            columns = [row['name'] for row in db.execute('PRAGMA table_info(assistent_bestellanforderungen)')]
        self.assertIn('schedule_version',columns)
        self.assertIn('legacy_due_at',columns)
        self.assertEqual(self.smtp.data_calls,0)


if __name__ == '__main__':
    unittest.main()
