"""Offline contract-delivery tests. SMTP is always replaced by a fake."""

from base64 import b64encode
from hashlib import sha256
from pathlib import Path
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mos_contract_delivery import deliver_one, enqueue, init_schema, pending_ids, unfinished_live_ids, unresolved_count


class Portal:
    def __init__(self, path):
        self.path = path

    def get_db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        return db


class FakeSMTP:
    calls = 0
    data_result = (250, b'ok')
    data_error = None
    before_data_error = None
    entered_data = None
    release_data = None
    raw = None

    def __init__(self, *args, **kwargs):
        pass

    def login(self, *args):
        pass

    def mail(self, *args):
        if self.before_data_error:
            raise self.before_data_error
        return 250, b'ok'

    def rcpt(self, *args):
        return 250, b'ok'

    def data(self, raw):
        type(self).calls += 1
        type(self).raw = raw
        if self.entered_data:
            self.entered_data.set()
            if not self.release_data.wait(5):
                raise AssertionError('fake SMTP was not released')
        if self.data_error:
            raise self.data_error
        if b'MOS-Mietvertrag-' not in raw:
            raise AssertionError('PDF attachment missing')
        return self.data_result

    def quit(self):
        pass


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='mos-delivery-tests-')
        self.addCleanup(self.tmp.cleanup)
        self.portal = Portal(str(Path(self.tmp.name) / 'outbox.sqlite3'))
        self.cfg = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                    'smtp_host': 'smtp.example.test', 'smtp_port': 465,
                    'smtp_user': 'workshop@example.test', '_smtp_password': 'synthetic',
                    'from_address': 'workshop@example.test', 'display_name': 'Werkstatt'}
        self.hold = 'hold-synthetic-123'
        self.pdf = b'%PDF-1.4 synthetic signed rental copy\n%%EOF'
        self.contract = {'customer_email': 'customer@example.test', 'test_only': False}
        db = self.portal.get_db()
        db.execute('''CREATE TABLE miet_checkout_holds (id TEXT PRIMARY KEY,status TEXT,
            payment_intent TEXT,mietvorgang_id INTEGER,payload TEXT)''')
        db.execute('''CREATE TABLE miet_checkout_contracts (hold_id TEXT PRIMARY KEY,
            contract_json TEXT,contract_sha256 TEXT,pdf_base64 TEXT,pdf_sha256 TEXT,signed_at TEXT,
            signature_png_base64 TEXT)''')
        init_schema(db)
        db.execute('CREATE TABLE miet_checkout_cancellations (id TEXT PRIMARY KEY)')
        db.execute('INSERT INTO miet_checkout_holds VALUES (?,?,?,?,?)',
                   (self.hold, 'confirmed', 'pi_synthetic', 7,
                    json.dumps({'quote': {'test_only': False}})))
        canonical = json.dumps(self.contract, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        db.execute('INSERT INTO miet_checkout_contracts VALUES (?,?,?,?,?,?,?)',
                   (self.hold, canonical, sha256(canonical.encode()).hexdigest(), b64encode(self.pdf).decode(),
                    sha256(self.pdf).hexdigest(), '2026-09-24T12:00:00+00:00', 'signature'))
        db.commit()
        db.close()
        FakeSMTP.calls = 0
        FakeSMTP.data_result = (250, b'ok')
        FakeSMTP.data_error = None
        FakeSMTP.before_data_error = None
        FakeSMTP.entered_data = None
        FakeSMTP.release_data = None
        FakeSMTP.raw = None

    def row(self):
        db = self.portal.get_db()
        try:
            found = db.execute('SELECT * FROM miet_checkout_contract_delivery WHERE hold_id=?',
                               (self.hold,)).fetchone()
            return dict(found) if found else None
        finally:
            db.close()

    def queue(self):
        db = self.portal.get_db()
        try:
            hold = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (self.hold,)).fetchone())
            self.assertTrue(enqueue(db, hold, self.contract, sha256(self.pdf).hexdigest()))
            db.commit()
        finally:
            db.close()

    def send_fake(self, *, live=True, enabled=True):
        with patch('mos_contract_delivery.smtplib.SMTP_SSL', FakeSMTP):
            return deliver_one(self.portal, self.hold, self.cfg, live=live, enabled=enabled)

    def test_only_confirmed_live_contract_enters_outbox(self):
        db = self.portal.get_db()
        try:
            hold = dict(db.execute('SELECT * FROM miet_checkout_holds WHERE id=?', (self.hold,)).fetchone())
            self.assertFalse(enqueue(db, hold, {**self.contract, 'test_only': True}, sha256(self.pdf).hexdigest()))
            self.assertFalse(enqueue(db, hold, {'customer_email': self.contract['customer_email']}, sha256(self.pdf).hexdigest()))
            self.assertFalse(enqueue(db, {**hold, 'status': 'review'}, self.contract, sha256(self.pdf).hexdigest()))
            self.assertFalse(enqueue(db, {**hold, 'status': 'released'}, self.contract, sha256(self.pdf).hexdigest()))
            db.commit()
        finally:
            db.close()
        self.assertIsNone(self.row())
        self.queue()
        self.queue()
        self.assertEqual(unresolved_count(self.portal), 1)
        self.assertEqual(pending_ids(self.portal), [self.hold])

    def test_backfill_skips_test_holds_but_reports_real_missing_delivery(self):
        self.assertEqual(unfinished_live_ids(self.portal), [self.hold])
        db = self.portal.get_db()
        db.execute('UPDATE miet_checkout_holds SET payload=? WHERE id=?',
                   (json.dumps({'quote': {'test_only': True}}), self.hold))
        db.commit()
        db.close()
        self.assertEqual(unfinished_live_ids(self.portal), [])

    def test_cancelled_booking_gets_historical_contract_mail(self):
        self.queue()
        db = self.portal.get_db()
        db.execute('INSERT INTO miet_checkout_cancellations VALUES (?)', (self.hold,))
        db.commit()
        db.close()
        self.assertEqual(self.send_fake(), 'sent')
        self.assertIn(b'inzwischen storniert', FakeSMTP.raw)
        self.assertNotIn(b'Ihre Fahrzeugbuchung wurde bestaetigt', FakeSMTP.raw)

    def test_no_send_without_explicit_live_enablement(self):
        self.queue()
        self.assertEqual(self.send_fake(live=False), 'disabled')
        self.assertEqual(self.send_fake(enabled=False), 'disabled')
        self.assertEqual(FakeSMTP.calls, 0)
        self.assertEqual(self.row()['status'], 'queued')

    def test_single_accepted_send_is_persisted_and_not_repeated(self):
        self.queue()
        self.assertEqual(self.send_fake(), 'sent')
        self.assertEqual(self.send_fake(), 'sent')
        self.assertEqual(FakeSMTP.calls, 1)
        self.assertEqual(self.row()['status'], 'sent')
        self.assertEqual(unresolved_count(self.portal), 0)

    def test_parallel_workers_do_not_submit_twice(self):
        self.queue()
        FakeSMTP.entered_data = threading.Event()
        FakeSMTP.release_data = threading.Event()
        outcome = []
        worker = threading.Thread(target=lambda: outcome.append(self.send_fake()))
        worker.start()
        try:
            self.assertTrue(FakeSMTP.entered_data.wait(5))
            self.assertEqual(self.send_fake(), 'sending')
        finally:
            FakeSMTP.release_data.set()
            worker.join(5)
        self.assertEqual(outcome, ['sent'])
        self.assertEqual(FakeSMTP.calls, 1)

    def test_provider_reply_loss_requires_manual_review_without_retry(self):
        self.queue()
        FakeSMTP.data_error = OSError('DATA reply lost')
        self.assertEqual(self.send_fake(), 'uncertain')
        FakeSMTP.data_error = None
        self.assertEqual(self.send_fake(), 'review')
        self.assertEqual(FakeSMTP.calls, 1)
        self.assertEqual(self.row()['status'], 'review')

    def test_known_pre_data_failure_is_retryable_after_backoff(self):
        self.queue()
        FakeSMTP.before_data_error = OSError('connect failed')
        self.assertEqual(self.send_fake(), 'not_sent')
        self.assertEqual(self.row()['status'], 'queued')
        self.assertEqual(FakeSMTP.calls, 0)
        self.assertEqual(pending_ids(self.portal), [])

    def test_changed_payment_or_pdf_blocks_sending(self):
        for change in ('hold', 'pdf', 'test_contract'):
            with self.subTest(change=change):
                self.setUp()
                self.queue()
                db = self.portal.get_db()
                if change == 'hold':
                    db.execute("UPDATE miet_checkout_holds SET status='review' WHERE id=?", (self.hold,))
                else:
                    if change == 'pdf':
                        db.execute("UPDATE miet_checkout_contracts SET pdf_base64=? WHERE hold_id=?",
                                   (b64encode(b'changed').decode(), self.hold))
                    else:
                        db.execute('UPDATE miet_checkout_contracts SET contract_json=? WHERE hold_id=?',
                                   (json.dumps({**self.contract, 'test_only': True}), self.hold))
                db.commit()
                db.close()
                self.assertEqual(self.send_fake(), 'review')
                self.assertEqual(FakeSMTP.calls, 0)


if __name__ == '__main__':
    unittest.main()
