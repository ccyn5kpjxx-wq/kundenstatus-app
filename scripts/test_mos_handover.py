"""Synthetic handover checks: no SMTP, Stripe request or operational database."""

from base64 import b64encode
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
import json
import sqlite3
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mietwagen_checkout import SharedCheckout
from mos_contract_delivery import init_schema as init_delivery_schema
from mos_handover import init_schema, record
from mos_booking.production import RefundLedger


class HandoverTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.now = datetime(2026, 9, 24, 12, tzinfo=timezone.utc)
        self.start = self.now - timedelta(hours=1)
        self.end = self.now + timedelta(days=1)
        self.until = (self.end + timedelta(days=2)).isoformat()
        self.contract = {'test_only': False, 'customer_email': 'customer@example.test'}
        self.quote = {'test_only': False, 'start_slot': self.start.isoformat(),
                      'end_slot': self.end.isoformat(),
                      'deposit_method': 'card_authorization_at_booking',
                      'deposit_authorized_cents': 50000}
        self.db.execute('''CREATE TABLE miet_checkout_holds (id TEXT PRIMARY KEY,
            mietfahrzeug_id INTEGER, status TEXT, payment_intent TEXT,
            mietvorgang_id INTEGER, payload TEXT, fingerprint TEXT)''')
        self.db.execute('''CREATE TABLE miet_checkout_contracts (hold_id TEXT PRIMARY KEY,
            contract_json TEXT, contract_sha256 TEXT, pdf_base64 TEXT, pdf_sha256 TEXT,
            signed_at TEXT, signature_png_base64 TEXT)''')
        self.db.execute('CREATE TABLE miet_checkout_cancellations (id TEXT PRIMARY KEY)')
        self.db.execute('''CREATE TABLE miet_checkout_deposit_auths
            (hold_id TEXT PRIMARY KEY,intent_id TEXT,status TEXT,capture_before TEXT)''')
        self.db.execute('''CREATE TABLE mietvorgaenge
            (id INTEGER PRIMARY KEY,mietfahrzeug_id INTEGER,status TEXT,rueckgabe_datum TEXT)''')
        init_delivery_schema(self.db)
        init_schema(self.db)
        self.db.execute('INSERT INTO miet_checkout_holds VALUES (?,?,?,?,?,?,?)',
                        ('h1', 3, 'confirmed', 'pi_payment', 7,
                         json.dumps({'quote': self.quote}), 'quote-digest'))
        self.db.execute("INSERT INTO mietvorgaenge VALUES (7,3,'aktiv','')")
        self.db.execute("INSERT INTO miet_checkout_deposit_auths VALUES ('h1','pi_deposit','authorized',?)",
                        (self.until,))
        canonical = json.dumps(self.contract, ensure_ascii=False, sort_keys=True,
                               separators=(',', ':'))
        pdf = b'%PDF-1.4\nsynthetic contract\n%%EOF'
        pdf_hash = sha256(pdf).hexdigest()
        self.db.execute('INSERT INTO miet_checkout_contracts VALUES (?,?,?,?,?,?,?)',
                        ('h1', canonical, sha256(canonical.encode()).hexdigest(),
                         b64encode(pdf).decode(), pdf_hash,
                         (self.now-timedelta(hours=3)).isoformat(), 'synthetic-signature'))
        self.db.execute('''INSERT INTO miet_checkout_contract_delivery
            (hold_id,pdf_sha256,recipient,status,enqueued_at,accepted_at)
            VALUES (?,?,?,'sent',?,?)''',
            ('h1', pdf_hash, self.contract['customer_email'],
             (self.now-timedelta(hours=2, minutes=30)).isoformat(),
             (self.now-timedelta(hours=2)).isoformat()))
        self.service = SharedCheckout.__new__(SharedCheckout)
        self.service.gateway = SimpleNamespace(livemode=True)
        self.intent = {'id': 'pi_deposit', 'livemode': True, 'amount': 50000,
                       'currency': 'eur', 'metadata': {'hold_id': 'h1', 'quote_hash': 'quote-digest'},
                       'status': 'requires_capture', 'amount_capturable': 50000,
                       'capture_before': self.until, 'card_funding': 'credit'}

    def handover(self, **overrides):
        arguments = dict(operator_name='Test Person', odometer_km=12345,
                         protocol_ref='TEST-123', receipt_confirmed=True,
                         license_checked=True, fuel_full=True,
                         condition_recorded=True, now=self.now)
        arguments.update(overrides)
        return record(self.db, self.service, 'h1', self.intent, **arguments)

    def assert_blocked(self):
        with self.assertRaises(ValueError):
            self.handover()
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM miet_checkout_handovers').fetchone()[0], 0)

    def test_records_once_and_preserves_first_audit(self):
        first = self.handover()
        self.assertEqual(first['mietvorgang_id'], 7)
        self.assertEqual(first['customer_receipt_confirmed'], 1)
        second = self.handover(operator_name='Another Operator')
        self.assertEqual(second['operator_name'], 'Test Person')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM miet_checkout_handovers').fetchone()[0], 1)

    def test_admin_no_show_cannot_cancel_after_handover(self):
        self.handover()

        @contextmanager
        def locked(_vehicle_id):
            yield self.db, None

        fake_service = SimpleNamespace(
            read=lambda _hold_id: dict(self.db.execute(
                "SELECT * FROM miet_checkout_holds WHERE id='h1'").fetchone()),
            locked=locked)
        with self.assertRaisesRegex(ValueError, 'Schlüsselübergabe'):
            RefundLedger(fake_service).cancel('h1', requested_at=self.now,
                                              admin=True, no_show=True)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM miet_checkout_cancellations').fetchone()[0], 0)

    def test_rejects_unconfirmed_customer_receipt(self):
        with self.assertRaisesRegex(ValueError, 'Empfang'):
            self.handover(receipt_confirmed=False)

    def test_rejects_unsent_mail_or_pdf_tampering(self):
        self.db.execute("UPDATE miet_checkout_contract_delivery SET status='review' WHERE hold_id='h1'")
        self.assert_blocked()
        self.db.execute("UPDATE miet_checkout_contract_delivery SET status='sent' WHERE hold_id='h1'")
        self.db.execute("UPDATE miet_checkout_contracts SET pdf_sha256='tampered' WHERE hold_id='h1'")
        self.assert_blocked()

    def test_rejects_cancellation_and_test_marker(self):
        self.db.execute("INSERT INTO miet_checkout_cancellations VALUES ('h1')")
        self.assert_blocked()
        self.db.execute("DELETE FROM miet_checkout_cancellations")
        self.quote['test_only'] = True
        self.db.execute('UPDATE miet_checkout_holds SET payload=? WHERE id=?',
                        (json.dumps({'quote': self.quote}), 'h1'))
        self.assert_blocked()

    def test_rejects_expired_or_non_credit_deposit(self):
        self.intent['card_funding'] = 'debit'
        self.assert_blocked()
        self.intent['card_funding'] = 'credit'
        self.intent['capture_before'] = (self.now-timedelta(minutes=1)).isoformat()
        self.assert_blocked()
        self.intent['capture_before'] = self.until
        self.db.execute("UPDATE miet_checkout_deposit_auths SET status='released' WHERE hold_id='h1'")
        self.assert_blocked()

    def test_rejects_early_handover_or_late_mail_acceptance(self):
        with self.assertRaises(ValueError):
            self.handover(now=self.start-timedelta(minutes=1))
        self.db.execute('UPDATE miet_checkout_contract_delivery SET accepted_at=? WHERE hold_id=?',
                        ((self.now+timedelta(minutes=1)).isoformat(), 'h1'))
        self.assert_blocked()


if __name__ == '__main__':
    unittest.main()
