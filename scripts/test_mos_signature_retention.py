from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from mos_signature_retention import parse_cutoff, retention_report
from scripts.report_mos_signature_retention import main


def make_db(path=':memory:'):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript('''
        CREATE TABLE miet_checkout_holds (
            id TEXT PRIMARY KEY, status TEXT, payload TEXT, session_id TEXT,
            payment_intent TEXT, mietvorgang_id INTEGER);
        CREATE TABLE miet_checkout_contracts (hold_id TEXT);
        CREATE TABLE miet_checkout_deposit_auths (hold_id TEXT);
        CREATE TABLE miet_checkout_creation_attempts (hold_id TEXT);
        CREATE TABLE miet_checkout_events (hold_id TEXT);
        CREATE TABLE miet_checkout_refunds (hold_id TEXT);
        CREATE TABLE miet_checkout_cancellations (id TEXT);
    ''')
    return db


def add(db, ident, status='released', signed_at='2020-01-01T00:00:00+00:00',
        signature='private-drawn-signature', **changes):
    payload = json.dumps({'customer': {'name': 'Private Customer'},
                          'quote': {'signature_png_base64': signature, 'signed_at': signed_at}})
    row = {'id': ident, 'status': status, 'payload': payload, 'session_id': None,
           'payment_intent': None, 'mietvorgang_id': None}
    row.update(changes)
    db.execute('''INSERT INTO miet_checkout_holds
        (id,status,payload,session_id,payment_intent,mietvorgang_id)
        VALUES (:id,:status,:payload,:session_id,:payment_intent,:mietvorgang_id)''', row)
    return payload


class RetentionReportTests(unittest.TestCase):
    def test_only_provider_untouched_released_signature_is_technical_candidate(self):
        db = make_db()
        add(db, 'isolated')
        add(db, 'pending', status='pending')
        add(db, 'review', status='review')
        add(db, 'confirmed', status='confirmed')
        add(db, 'payment', payment_intent='pi_test_placeholder')
        add(db, 'session', session_id='cs_test_placeholder')
        add(db, 'deposit')
        add(db, 'attempt')
        add(db, 'event')
        add(db, 'contract')
        add(db, 'refund')
        add(db, 'cancellation')
        add(db, 'unsigned', signature='')
        add(db, 'invalid', signed_at='not-a-date')
        add(db, 'recent', signed_at='2024-01-01T00:00:00+00:00')
        for table, ident, column in (
            ('miet_checkout_deposit_auths', 'deposit', 'hold_id'),
            ('miet_checkout_creation_attempts', 'attempt', 'hold_id'),
            ('miet_checkout_events', 'event', 'hold_id'),
            ('miet_checkout_contracts', 'contract', 'hold_id'),
            ('miet_checkout_refunds', 'refund', 'hold_id'),
            ('miet_checkout_cancellations', 'cancellation', 'id'),
        ):
            db.execute(f'INSERT INTO {table} ({column}) VALUES (?)', (ident,))
        db.commit()
        before = db.execute("SELECT payload FROM miet_checkout_holds WHERE id='isolated'").fetchone()[0]
        report = retention_report(db, datetime(2021, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(report['classes'], {
            'contract_or_payment_record': 3,
            'invalid_signed_record': 1,
            'isolated_released_before_cutoff': 1,
            'isolated_released_newer_than_cutoff': 1,
            'manual_review': 1,
            'no_signature': 1,
            'pending': 1,
            'provider_or_financial_history': 6,
        })
        self.assertEqual(report['total_holds'], 15)
        self.assertFalse(report['automatic_deletion'])
        self.assertEqual(before, db.execute("SELECT payload FROM miet_checkout_holds WHERE id='isolated'").fetchone()[0])
        self.assertNotIn('Private Customer', json.dumps(report))
        self.assertNotIn('private-drawn-signature', json.dumps(report))
        self.assertNotIn('isolated"', json.dumps(report))
        db.close()

    def test_without_policy_cutoff_even_isolated_record_is_not_ready(self):
        db = make_db()
        add(db, 'a')
        self.assertEqual(retention_report(db)['classes'], {'isolated_released_no_policy_cutoff': 1})
        db.close()

    def test_cutoff_must_be_explicit_utc_and_past(self):
        self.assertIsNone(parse_cutoff(None))
        self.assertEqual(parse_cutoff('2020-01-01T00:00:00Z').tzinfo, timezone.utc)
        for value in ('2020-01-01', '2020-01-01T01:00:00+01:00', 'bad',
                      '2999-01-01T00:00:00Z'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_cutoff(value)

    def test_cli_opens_only_explicit_sqlite_copy_read_only(self):
        with tempfile.TemporaryDirectory(prefix='mos-retention-test-') as directory:
            path = Path(directory) / 'copy.sqlite3'
            db = make_db(path)
            payload = add(db, 'a')
            db.commit()
            db.close()
            output = StringIO()
            with redirect_stdout(output):
                main(['--sqlite-copy', str(path), '--signed-before-utc', '2021-01-01T00:00:00Z'])
            report = json.loads(output.getvalue())
            self.assertEqual(report['classes'], {'isolated_released_before_cutoff': 1})
            self.assertNotIn('Private Customer', output.getvalue())
            self.assertNotIn('private-drawn-signature', output.getvalue())
            verification = sqlite3.connect(path)
            try:
                self.assertEqual(verification.execute('SELECT payload FROM miet_checkout_holds').fetchone()[0], payload)
            finally:
                verification.close()

    def test_schema_gap_fails_closed(self):
        db = sqlite3.connect(':memory:')
        with self.assertRaises(sqlite3.OperationalError):
            retention_report(db)
        db.close()


if __name__ == '__main__':
    unittest.main()
