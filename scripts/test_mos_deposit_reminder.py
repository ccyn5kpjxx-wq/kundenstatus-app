"""Synthetic deposit reminders and scoped access; no SMTP or provider traffic."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import json
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from mos_deposit_reminder import (access_url, create_access_token, deliver_one,
    enqueue_due, init_schema, pending_ids, run_cycle, unresolved_count,
    validate_access_token, missing_deposit_overdue)


class Portal:
    USE_POSTGRES = False

    def __init__(self, path):
        self.path = path
        self.app = SimpleNamespace(secret_key='synthetic-secret-key-' * 3,
            config={'MOS_PUBLIC_BOOKING': {'origin': 'https://mos.example.test', 'mode': 'live'}})

    def get_db(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db


class DepositReminderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='mos-deposit-reminder-')
        self.addCleanup(self.tmp.cleanup)
        self.portal = Portal(str(Path(self.tmp.name) / 'synthetic.sqlite3'))
        self.start = datetime(2027, 5, 17, 8, tzinfo=timezone.utc)
        self.now = self.start - timedelta(hours=12)
        self.hold_id = 'synthetic-reminder-hold'
        self.payload = {'customer': {'email': 'customer@example.test'}, 'quote': {
            'test_only': False, 'amount_cents': 19500, 'currency': 'eur',
            'deposit_method': 'card_authorization_at_booking',
            'deposit_timing': 'before_pickup', 'owner_hash': 'a' * 64,
            'start_slot': self.start.isoformat(),
            'end_slot': (self.start + timedelta(days=5)).isoformat()}}
        self.cfg = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                    'smtp_host': 'smtp.example.test', 'smtp_port': 465,
                    'smtp_user': 'workshop@example.test', '_smtp_password': 'synthetic',
                    'from_address': 'workshop@example.test', 'display_name': 'Werkstatt'}
        db = self.portal.get_db()
        db.execute('''CREATE TABLE miet_checkout_holds (id TEXT PRIMARY KEY,
            payload TEXT,fingerprint TEXT,session_id TEXT,status TEXT,payment_intent TEXT,
            mietvorgang_id INTEGER)''')
        db.execute('''CREATE TABLE mietvorgaenge (id INTEGER PRIMARY KEY,
            status TEXT,rueckgabe_datum TEXT)''')
        db.execute('CREATE TABLE miet_checkout_cancellations (id TEXT PRIMARY KEY)')
        db.execute('''CREATE TABLE miet_checkout_deposit_auths
            (hold_id TEXT PRIMARY KEY,status TEXT,capture_before TEXT)''')
        db.execute("INSERT INTO mietvorgaenge VALUES (7,'aktiv',NULL)")
        db.execute('INSERT INTO miet_checkout_holds VALUES (?,?,?,?,?,?,?)',
            (self.hold_id, json.dumps(self.payload, sort_keys=True), 'synthetic-fingerprint',
             'cs_live_synthetic', 'confirmed', 'pi_synthetic', 7))
        init_schema(db)
        db.commit()
        db.close()

    def hold(self):
        db = self.portal.get_db()
        try:
            return dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (self.hold_id,)).fetchone())
        finally:
            db.close()

    def row(self):
        db = self.portal.get_db()
        try:
            row = db.execute('SELECT * FROM miet_checkout_deposit_reminders WHERE hold_id=?', (self.hold_id,)).fetchone()
            return dict(row) if row else None
        finally:
            db.close()

    def execute(self, sql, params=()):
        db = self.portal.get_db()
        try:
            db.execute(sql, params)
            db.commit()
        finally:
            db.close()

    def deliver(self, now=None):
        return deliver_one(self.portal, self.hold_id, self.cfg,
                           live=True, enabled=True, now=now or self.now)

    def test_due_window_queues_once_and_sends_scoped_personal_link(self):
        self.assertEqual(enqueue_due(self.portal, self.now - timedelta(seconds=1))['queued'], 0)
        self.assertEqual(enqueue_due(self.portal, self.now)['queued'], 1)
        self.assertEqual(enqueue_due(self.portal, self.now)['queued'], 0)
        self.assertEqual(pending_ids(self.portal, now=self.now), [self.hold_id])
        sent = []
        with patch('mos_deposit_reminder._smtp_send', side_effect=lambda m, _: sent.append(m) or 'sent'):
            self.assertEqual(self.deliver(), 'sent')
            self.assertEqual(self.deliver(), 'sent')
        self.assertEqual(len(sent), 1)
        body = sent[0].get_content()
        self.assertIn('500,00 EUR', body)
        self.assertIn('https://mos.example.test/mieten/buchung-oeffnen#', body)
        self.assertNotIn('customer@example.test', body)
        self.assertNotIn('synthetic-secret', body)
        self.assertEqual(self.row()['attempts'], 1)
        self.assertEqual(unresolved_count(self.portal), 0)

    def test_reentrant_callback_cannot_send_duplicate(self):
        enqueue_due(self.portal, self.now)
        seen = []
        def callback(message, cfg):
            seen.append(message)
            self.assertEqual(self.deliver(), 'sending')
            return 'sent'
        with patch('mos_deposit_reminder._smtp_send', side_effect=callback):
            self.assertEqual(self.deliver(), 'sent')
        self.assertEqual(len(seen), 1)

    def test_canceled_after_queue_and_returned_bookings_are_skipped(self):
        enqueue_due(self.portal, self.now)
        token = create_access_token(self.portal, self.hold(), self.now)
        self.execute('INSERT INTO miet_checkout_cancellations VALUES (?)', (self.hold_id,))
        with patch('mos_deposit_reminder._smtp_send') as smtp:
            self.assertEqual(self.deliver(), 'skipped')
            smtp.assert_not_called()
        with self.assertRaises(ValueError):
            validate_access_token(self.portal, token, self.now)
        self.assertEqual(unresolved_count(self.portal), 0)

    def test_late_reminders_are_monitored_without_sending(self):
        self.assertEqual(enqueue_due(self.portal, self.start)['overdue'], 1)
        self.assertEqual(self.row()['status'], 'overdue')
        self.assertEqual(unresolved_count(self.portal), 1)
        with patch('mos_deposit_reminder._smtp_send') as smtp:
            self.assertEqual(self.deliver(self.start), 'overdue')
            smtp.assert_not_called()

    def test_queue_that_misses_pickup_is_overdue(self):
        enqueue_due(self.portal, self.now)
        with patch('mos_deposit_reminder._smtp_send') as smtp:
            self.assertEqual(self.deliver(self.start), 'overdue')
            smtp.assert_not_called()

    def test_valid_authorization_and_historic_or_unpaid_quotes_need_no_reminder(self):
        self.execute('INSERT INTO miet_checkout_deposit_auths VALUES (?,?,?)',
            (self.hold_id, 'authorized', (self.start + timedelta(days=7)).isoformat()))
        self.assertEqual(enqueue_due(self.portal, self.now)['queued'], 0)
        self.execute('DELETE FROM miet_checkout_deposit_auths')
        self.execute("UPDATE miet_checkout_holds SET status='pending'")
        self.assertEqual(enqueue_due(self.portal, self.now)['queued'], 0)
        self.execute("UPDATE miet_checkout_holds SET status='confirmed'")
        self.payload['quote'].pop('deposit_timing')
        self.execute('UPDATE miet_checkout_holds SET payload=?', (json.dumps(self.payload),))
        self.assertEqual(enqueue_due(self.portal, self.now)['queued'], 0)

    def test_short_authorization_still_needs_action_but_released_is_not_retried(self):
        self.execute('INSERT INTO miet_checkout_deposit_auths VALUES (?,?,?)',
            (self.hold_id, 'authorized', (self.start + timedelta(days=1)).isoformat()))
        self.assertEqual(enqueue_due(self.portal, self.now)['queued'], 1)
        self.execute("UPDATE miet_checkout_deposit_auths SET status='released'")
        with patch('mos_deposit_reminder._smtp_send') as smtp:
            self.assertEqual(self.deliver(), 'skipped')
            smtp.assert_not_called()

    def test_uncertain_smtp_is_review_and_never_retried(self):
        enqueue_due(self.portal, self.now)
        with patch('mos_deposit_reminder._smtp_send', return_value='uncertain') as smtp:
            self.assertEqual(self.deliver(), 'uncertain')
            self.assertEqual(self.deliver(), 'review')
            self.assertEqual(smtp.call_count, 1)
        self.assertEqual(unresolved_count(self.portal), 1)
        detail = self.row()['detail']
        self.assertNotIn('customer@example.test', detail)
        self.assertNotIn('buchung-oeffnen', detail)

    def test_explicit_no_acceptance_can_retry_after_delay(self):
        enqueue_due(self.portal, self.now)
        with patch('mos_deposit_reminder._smtp_send', return_value='not_sent') as smtp:
            self.assertEqual(self.deliver(), 'not_sent')
            self.assertEqual(self.deliver(), 'not_due')
            self.assertEqual(smtp.call_count, 1)
        with patch('mos_deposit_reminder._smtp_send', return_value='sent'):
            self.assertEqual(self.deliver(self.now + timedelta(seconds=61)), 'sent')

    def test_tokens_bind_scope_owner_payload_expiry_and_live_confirmed_booking(self):
        token = create_access_token(self.portal, self.hold(), self.now)
        data = validate_access_token(self.portal, token, self.now)
        self.assertEqual(data['scope'], 'deposit')
        self.assertEqual(data['hold_id'], self.hold_id)
        self.assertEqual(data['owner_hash'], 'a' * 64)
        self.assertEqual(data['expires_at'], int((self.start + timedelta(days=1)).timestamp()))
        with self.assertRaises(ValueError):
            validate_access_token(self.portal, token[:-5] + 'wrong', self.now)
        with self.assertRaises(ValueError):
            validate_access_token(self.portal, token, self.start + timedelta(days=1))
        self.payload['quote']['owner_hash'] = 'b' * 64
        self.execute('UPDATE miet_checkout_holds SET payload=?', (json.dumps(self.payload),))
        with self.assertRaises(ValueError):
            validate_access_token(self.portal, token, self.now)

    def test_access_url_rejects_untrusted_origin_shape_and_token_uses_no_email(self):
        with self.assertRaises(ValueError):
            access_url(self.portal, self.hold(), origin='https://mos.example.test@evil.example/path', now=self.now)
        token = create_access_token(self.portal, self.hold(), self.now)
        from itsdangerous import URLSafeSerializer
        data = URLSafeSerializer(self.portal.app.secret_key, salt='mos-deposit-access-v1').loads(token)
        self.assertNotIn('customer@example.test', json.dumps(data))
        self.assertNotIn('recipient', data)
        from urllib.parse import urlsplit
        url = urlsplit(access_url(self.portal, self.hold(), now=self.now))
        self.assertEqual(url.path, '/mieten/buchung-oeffnen')
        self.assertEqual(url.query, '')
        self.assertEqual(url.fragment, token)

    def test_disabled_cycle_never_queues_or_sends(self):
        with patch('mos_deposit_reminder._smtp_send') as smtp:
            result = run_cycle(self.portal, self.cfg, live=True, enabled=False, now=self.now)
            smtp.assert_not_called()
        self.assertEqual(result['queued'], 0)
        self.assertIsNone(self.row())

    def test_cycle_counts_delivery(self):
        with patch('mos_deposit_reminder._smtp_send', return_value='sent'):
            result = run_cycle(self.portal, self.cfg, live=True, enabled=True, now=self.now)
        self.assertEqual(result['queued'], 1)
        self.assertEqual(result['sent'], 1)
        self.assertEqual(result['unresolved'], 0)

    def test_changed_snapshot_is_reviewed_without_delivery(self):
        enqueue_due(self.portal, self.now)
        self.payload['quote']['amount_cents'] = 5900
        self.execute('UPDATE miet_checkout_holds SET payload=?', (json.dumps(self.payload),))
        with patch('mos_deposit_reminder._smtp_send') as smtp:
            self.assertEqual(self.deliver(), 'review')
            smtp.assert_not_called()
        self.assertEqual(unresolved_count(self.portal), 1)

    def test_tokens_reject_unpaid_pending_returned_and_other_scope(self):
        token = create_access_token(self.portal, self.hold(), self.now)
        self.execute("UPDATE miet_checkout_holds SET payment_intent=NULL,status='pending'")
        with self.assertRaises(ValueError):
            validate_access_token(self.portal, token, self.now)
        self.execute("UPDATE miet_checkout_holds SET payment_intent='pi_synthetic',status='confirmed'")
        from itsdangerous import URLSafeSerializer
        signer = URLSafeSerializer(self.portal.app.secret_key, salt='mos-deposit-access-v1')
        data = signer.loads(token)
        data['scope'] = 'cancel'
        with self.assertRaises(ValueError):
            validate_access_token(self.portal, signer.dumps(data), self.now)
        self.execute("UPDATE mietvorgaenge SET rueckgabe_datum='2027-05-19'")
        with self.assertRaises(ValueError):
            validate_access_token(self.portal, token, self.now)

    def test_sent_reminder_without_card_still_alarms_at_pickup_without_resending(self):
        with patch('mos_deposit_reminder._smtp_send', return_value='sent'):
            run_cycle(self.portal, self.cfg, live=True, enabled=True, now=self.now)
        audit = self.row()
        self.assertEqual(audit['status'], 'sent')
        self.assertEqual(missing_deposit_overdue(self.portal, self.start - timedelta(seconds=1)), 0)
        with patch('mos_deposit_reminder._smtp_send') as smtp:
            result = run_cycle(self.portal, self.cfg, live=True, enabled=True, now=self.start)
            smtp.assert_not_called()
        self.assertEqual(result['missing_deposit_overdue'], 1)
        self.assertEqual(result['unresolved'], 0)
        self.assertEqual(self.row(), audit)

    def test_missing_card_is_monitored_without_any_reminder_or_intent_row(self):
        self.assertEqual(missing_deposit_overdue(self.portal, self.start), 1)
        self.assertIsNone(self.row())
        self.assertEqual(missing_deposit_overdue(self.portal, self.now), 0)

    def test_failed_released_and_expired_card_remain_overdue_until_resolved(self):
        for status in ('released', 'releasing', 'review', 'awaiting_card', 'creating'):
            with self.subTest(status=status):
                self.execute('DELETE FROM miet_checkout_deposit_auths')
                self.execute('INSERT INTO miet_checkout_deposit_auths VALUES (?,?,NULL)', (self.hold_id, status))
                self.assertEqual(missing_deposit_overdue(self.portal, self.start), 1)
        self.execute('DELETE FROM miet_checkout_deposit_auths')
        self.execute('INSERT INTO miet_checkout_deposit_auths VALUES (?,?,?)',
                     (self.hold_id, 'authorized', (self.start + timedelta(days=7)).isoformat()))
        self.assertEqual(missing_deposit_overdue(self.portal, self.start), 0)
        self.assertEqual(missing_deposit_overdue(self.portal, self.start + timedelta(days=8)), 1)

    def test_cancellation_resolves_reminder_alarms_and_preserves_delivery_audit(self):
        enqueue_due(self.portal, self.start)
        for status in ('overdue', 'review', 'sending'):
            with self.subTest(status=status):
                self.execute('DELETE FROM miet_checkout_cancellations')
                self.execute("UPDATE miet_checkout_deposit_reminders SET status=?,detail='synthetic audit evidence'", (status,))
                audit = self.row()
                self.assertEqual(unresolved_count(self.portal), 1)
                self.assertEqual(missing_deposit_overdue(self.portal, self.start), 1)
                self.execute('INSERT INTO miet_checkout_cancellations VALUES (?)', (self.hold_id,))
                self.assertEqual(unresolved_count(self.portal), 0)
                self.assertEqual(missing_deposit_overdue(self.portal, self.start), 0)
                self.assertEqual(self.row(), audit)

    def test_returned_or_canceled_rental_resolves_alarms_without_cancellation_record(self):
        enqueue_due(self.portal, self.start)
        audit = self.row()
        for status, returned in [('zurueck', ''), ('storniert', ''), ('aktiv', '2027-05-19')]:
            with self.subTest(status=status, returned=returned):
                self.execute('UPDATE mietvorgaenge SET status=?,rueckgabe_datum=?', (status, returned))
                self.assertEqual(unresolved_count(self.portal), 0)
                self.assertEqual(missing_deposit_overdue(self.portal, self.start), 0)
                self.assertEqual(self.row(), audit)

    def test_invalid_deferred_time_is_an_error_instead_of_silent_green_monitor(self):
        self.payload['quote']['start_slot'] = 'invalid-time'
        self.execute('UPDATE miet_checkout_holds SET payload=?', (json.dumps(self.payload),))
        with self.assertRaisesRegex(ValueError, 'Kautionsüberwachung'):
            missing_deposit_overdue(self.portal, self.start)
        with patch('mos_deposit_reminder._smtp_send') as smtp:
            result = run_cycle(self.portal, self.cfg, live=True, enabled=True, now=self.start)
            smtp.assert_not_called()
        self.assertEqual(result['errors'], 1)

    def test_offline_deferred_quotes_do_not_enter_the_live_monitor(self):
        self.payload['quote']['test_only'] = True
        self.execute('UPDATE miet_checkout_holds SET payload=?', (json.dumps(self.payload),))
        self.assertEqual(missing_deposit_overdue(self.portal, self.start), 0)

    def test_actual_reminder_insert_passes_postgres_adapter_without_nonexistent_id(self):
        # Capture SQL from the real enqueue path, then pass it through the
        # production adapter with its driver boundary faked. This catches the
        # adapter's implicit RETURNING id on a table whose key is hold_id.
        statements = []
        original = self.portal.get_db

        def traced_connection():
            db = original()
            db.set_trace_callback(statements.append)
            return db

        with patch.object(self.portal, 'get_db', side_effect=traced_connection):
            self.assertEqual(enqueue_due(self.portal, self.now)['queued'], 1)
        sql = next(statement for statement in statements
                   if statement.lstrip().upper().startswith('INSERT INTO MIET_CHECKOUT_DEPOSIT_REMINDERS'))
        sys.path.insert(0, str(ROOT / 'scripts'))
        from run_mos_public_test import build_test_app
        from test_postgres_contract_insert_adapter import FakeConnection
        with tempfile.TemporaryDirectory(prefix='mos-reminder-pg-adapter-') as directory:
            real_portal = build_test_app(directory)
            fake = FakeConnection()
            cursor = real_portal.PostgresConnection(fake).execute(sql)
        converted = fake.statements[0][0]
        self.assertIn('RETURNING hold_id', converted)
        self.assertNotIn('RETURNING id', converted)
        self.assertIsNone(cursor.lastrowid)


if __name__ == '__main__':
    unittest.main()
