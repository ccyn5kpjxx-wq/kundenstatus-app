"""Synthetic MOS order-receipt tests; no Stripe or real SMTP request."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import json
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mos_order_receipt import deliver_one, enqueue, init_schema, pending_ids, unresolved_count


class Portal:
    USE_POSTGRES = False

    def __init__(self, path):
        self.path = path

    def get_db(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db


class OrderReceiptTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='mos-order-receipt-')
        self.addCleanup(self.tmp.cleanup)
        self.portal = Portal(str(Path(self.tmp.name) / 'synthetic.sqlite3'))
        self.hold_id = 'synthetic-hold-1'
        self.payload = json.dumps({'customer': {'email': 'customer@example.test'},
                                   'quote': {'test_only': False, 'amount_cents': 3900,
                                             'currency': 'eur'}}, sort_keys=True)
        self.session = {'id': 'cs_live_synthetic', 'payment_intent': 'pi_synthetic',
                        'livemode': True, 'mode': 'payment', 'status': 'complete',
                        'payment_status': 'paid', 'client_reference_id': self.hold_id,
                        'metadata': {'hold_id': self.hold_id, 'quote_hash': 'synthetic-fingerprint'},
                        'amount_total': 3900, 'currency': 'eur'}
        self.cfg = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                    'smtp_host': 'smtp.example.test', 'smtp_port': 465,
                    'smtp_user': 'workshop@example.test', '_smtp_password': 'synthetic',
                    'from_address': 'workshop@example.test', 'display_name': 'Werkstatt'}
        db = self.portal.get_db()
        db.execute('''CREATE TABLE miet_checkout_holds (id TEXT PRIMARY KEY,mietfahrzeug_id INTEGER,
            payload TEXT,fingerprint TEXT,session_id TEXT,status TEXT,payment_intent TEXT,
            mietvorgang_id INTEGER)''')
        db.execute('CREATE TABLE mietfahrzeuge (id INTEGER PRIMARY KEY)')
        db.execute('INSERT INTO mietfahrzeuge (id) VALUES (1)')
        db.execute('''CREATE TABLE miet_checkout_events
            (id TEXT PRIMARY KEY,hold_id TEXT,session_id TEXT,kind TEXT)''')
        db.execute('INSERT INTO miet_checkout_holds VALUES (?,?,?,?,?,?,?,?)',
                   (self.hold_id, 1, self.payload, 'synthetic-fingerprint',
                    self.session['id'], 'confirmed', self.session['payment_intent'], 7))
        init_schema(db)
        db.commit()
        db.close()

    def row(self):
        db = self.portal.get_db()
        try:
            found = db.execute('SELECT * FROM miet_checkout_order_receipts WHERE hold_id=?',
                               (self.hold_id,)).fetchone()
            return dict(found) if found else None
        finally:
            db.close()

    def queue(self, *, session=None, event='evt_synthetic'):
        db = self.portal.get_db()
        try:
            hold = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?',
                                   (self.hold_id,)).fetchone())
            value = enqueue(db, hold, self.session if session is None else session,
                            source_event_id=event,
                            observed_at=datetime(2026, 9, 24, 12, tzinfo=timezone.utc))
            db.commit()
            return value
        finally:
            db.close()

    def test_paid_confirmed_is_queued_once_and_delivered_neutrally(self):
        self.assertTrue(self.queue())
        self.assertFalse(self.queue(event='evt_duplicate'))
        self.assertEqual(pending_ids(self.portal), [self.hold_id])
        self.assertEqual(unresolved_count(self.portal), 1)
        sent = []
        with patch('mos_order_receipt._smtp_send', side_effect=lambda m, _: sent.append(m) or 'sent'):
            self.assertEqual(deliver_one(self.portal, self.hold_id, self.cfg,
                                         live=True, enabled=True), 'sent')
        self.assertEqual(len(sent), 1)
        text = sent[0].get_content()
        self.assertIn('Eingang Ihrer Online-Bestellung', text)
        self.assertIn('39,00 EUR', text)
        self.assertIn('keine zusätzliche Annahme', text)
        self.assertNotIn('Ihre Buchung ist bestätigt', text)
        self.assertEqual(self.row()['status'], 'sent')
        self.assertEqual(unresolved_count(self.portal), 0)
        self.assertEqual(deliver_one(self.portal, self.hold_id, self.cfg,
                                     live=True, enabled=True), 'sent')

    def test_paid_review_gets_receipt_even_without_rental_or_saved_payment_intent(self):
        db = self.portal.get_db()
        db.execute("UPDATE miet_checkout_holds SET status='review',payment_intent=NULL,mietvorgang_id=NULL")
        db.commit()
        db.close()
        self.assertTrue(self.queue())
        with patch('mos_order_receipt._smtp_send', return_value='sent') as smtp:
            self.assertEqual(deliver_one(self.portal, self.hold_id, self.cfg,
                                         live=True, enabled=True), 'sent')
        smtp.assert_called_once()

    def test_signed_event_paid_review_enqueues_once_and_replay_is_idempotent(self):
        from mietwagen_checkout import SharedCheckout
        from mos_booking.gateway import StripeTestGateway

        db = self.portal.get_db()
        db.execute("UPDATE miet_checkout_holds SET status='review',payment_intent=NULL,mietvorgang_id=NULL")
        db.commit()
        db.close()
        gateway = StripeTestGateway.__new__(StripeTestGateway)
        gateway.livemode = True
        gateway.retrieve = lambda sid: self.session
        service = SharedCheckout(self.portal, gateway)
        event = {'id': 'evt_synthetic', 'livemode': True,
                 'type': 'checkout.session.completed',
                 'data': {'object': {'id': self.session['id']}}}
        with patch('mos_booking.gateway.verified_event', return_value=event) as verify:
            self.assertIsNone(service.handle_signed_event(b'synthetic signed body', 'synthetic signature',
                                                           'synthetic webhook secret'))
            self.assertIsNone(service.handle_signed_event(b'synthetic signed body', 'synthetic signature',
                                                           'synthetic webhook secret'))
        self.assertEqual(verify.call_count, 2)
        db = self.portal.get_db()
        try:
            self.assertEqual(db.execute('SELECT COUNT(*) AS n FROM miet_checkout_events').fetchone()['n'], 1)
            self.assertEqual(db.execute('SELECT COUNT(*) AS n FROM miet_checkout_order_receipts').fetchone()['n'], 1)
            hold = db.execute('SELECT status,mietvorgang_id FROM miet_checkout_holds').fetchone()
            self.assertEqual(hold['status'], 'review')
            self.assertIsNone(hold['mietvorgang_id'])
        finally:
            db.close()

    def test_unpaid_test_mode_and_mismatches_are_rejected(self):
        cases = [
            {'payment_status': 'unpaid'},
            {'status': 'open'},
            {'livemode': False},
            {'amount_total': 4000},
            {'currency': 'usd'},
            {'client_reference_id': 'other'},
            {'metadata': {'hold_id': self.hold_id, 'quote_hash': 'wrong'}},
        ]
        for change in cases:
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.queue(session={**self.session, **change})
        db = self.portal.get_db()
        payload = json.loads(self.payload)
        payload['quote']['test_only'] = True
        db.execute('UPDATE miet_checkout_holds SET payload=?', (json.dumps(payload),))
        db.commit()
        db.close()
        with self.assertRaises(ValueError):
            self.queue()
        self.assertIsNone(self.row())

    def test_duplicate_with_changed_provider_data_is_rejected(self):
        self.queue()
        with self.assertRaisesRegex(ValueError, 'kollidiert'):
            self.queue(session={**self.session, 'payment_intent': 'pi_other'})
        self.assertEqual(self.row()['payment_intent'], 'pi_synthetic')

    def test_ambiguous_smtp_is_fenced_for_manual_review(self):
        self.queue()
        with patch('mos_order_receipt._smtp_send', return_value='uncertain') as smtp:
            self.assertEqual(deliver_one(self.portal, self.hold_id, self.cfg,
                                         live=True, enabled=True), 'uncertain')
            self.assertEqual(deliver_one(self.portal, self.hold_id, self.cfg,
                                         live=True, enabled=True), 'review')
        smtp.assert_called_once()
        self.assertEqual(self.row()['status'], 'review')

    def test_definite_smtp_rejection_retries_and_disabled_worker_does_not_send(self):
        self.queue()
        with patch('mos_order_receipt._smtp_send', return_value='not_sent') as smtp:
            self.assertEqual(deliver_one(self.portal, self.hold_id, self.cfg,
                                         live=False, enabled=True), 'disabled')
            self.assertEqual(deliver_one(self.portal, self.hold_id, self.cfg,
                                         live=True, enabled=True), 'not_sent')
        smtp.assert_called_once()
        self.assertEqual(self.row()['status'], 'queued')
        self.assertEqual(pending_ids(self.portal), [])

    def test_mutated_payload_fails_closed_before_smtp(self):
        self.queue()
        db = self.portal.get_db()
        db.execute('UPDATE miet_checkout_holds SET payload=?', (self.payload + ' ',))
        db.commit()
        db.close()
        with patch('mos_order_receipt._smtp_send') as smtp:
            self.assertEqual(deliver_one(self.portal, self.hold_id, self.cfg,
                                         live=True, enabled=True), 'review')
        smtp.assert_not_called()


if __name__ == '__main__':
    unittest.main()
