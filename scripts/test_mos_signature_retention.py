from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from mos_signature_retention import RetentionPolicy, block_hold, cleanup, init_schema, parse_cutoff, retention_report
from scripts.report_mos_signature_retention import main
from scripts.run_mos_signature_retention import load_policy, main as cleanup_main


def make_db(path=':memory:'):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.executescript('''
        CREATE TABLE miet_checkout_holds (
            id TEXT PRIMARY KEY, status TEXT, payload TEXT, session_id TEXT,
            payment_intent TEXT, mietvorgang_id INTEGER, expires_at INTEGER);
        CREATE TABLE miet_checkout_contracts (hold_id TEXT);
        CREATE TABLE miet_checkout_contract_delivery (hold_id TEXT);
        CREATE TABLE miet_checkout_order_receipts (hold_id TEXT);
        CREATE TABLE miet_checkout_deposit_auths (hold_id TEXT);
        CREATE TABLE miet_checkout_creation_attempts (hold_id TEXT);
        CREATE TABLE miet_checkout_events (hold_id TEXT);
        CREATE TABLE miet_checkout_refunds (hold_id TEXT);
        CREATE TABLE miet_checkout_cancellations (id TEXT);
        CREATE TABLE miet_checkout_handovers (hold_id TEXT);
    ''')
    init_schema(db)
    db.commit()
    return db


def add(db, ident, status='released', signed_at='2020-01-01T00:00:00+00:00',
        signature='private-drawn-signature', **changes):
    payload = json.dumps({'customer': {'name': 'Private Customer'},
                          'quote': {'signature_png_base64': signature, 'signed_at': signed_at,
                                    'deposit_method': 'card_authorization_at_booking',
                                    'slot_policy': 'db_open_slots_v1', 'test_only': False,
                                    'signed_contract_hash': 'a' * 64,
                                    'signature_record_hash': 'b' * 64}})
    row = {'id': ident, 'status': status, 'payload': payload, 'session_id': None,
           'payment_intent': None, 'mietvorgang_id': None, 'expires_at': 1577836800}
    row.update(changes)
    db.execute('''INSERT INTO miet_checkout_holds
        (id,status,payload,session_id,payment_intent,mietvorgang_id,expires_at)
        VALUES (:id,:status,:payload,:session_id,:payment_intent,:mietvorgang_id,:expires_at)''', row)
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
        add(db, 'delivery')
        add(db, 'order_receipt')
        add(db, 'refund')
        add(db, 'cancellation')
        add(db, 'handover')
        add(db, 'legal')
        add(db, 'unsigned', signature='')
        add(db, 'invalid', signed_at='not-a-date')
        add(db, 'recent', signed_at='2024-01-01T00:00:00+00:00', expires_at=1704067500)
        for table, ident, column in (
            ('miet_checkout_deposit_auths', 'deposit', 'hold_id'),
            ('miet_checkout_creation_attempts', 'attempt', 'hold_id'),
            ('miet_checkout_events', 'event', 'hold_id'),
            ('miet_checkout_contracts', 'contract', 'hold_id'),
            ('miet_checkout_contract_delivery', 'delivery', 'hold_id'),
            ('miet_checkout_order_receipts', 'order_receipt', 'hold_id'),
            ('miet_checkout_refunds', 'refund', 'hold_id'),
            ('miet_checkout_cancellations', 'cancellation', 'id'),
            ('miet_checkout_handovers', 'handover', 'hold_id'),
        ):
            db.execute(f'INSERT INTO {table} ({column}) VALUES (?)', (ident,))
        db.execute('INSERT INTO miet_checkout_retention_blocks VALUES (?,?,?)', ('legal','dispute','2020-01-01'))
        db.execute('INSERT INTO miet_checkout_retention_seen VALUES (?,?)', ('isolated','2020-01-01T00:00:00+00:00'))
        db.execute('INSERT INTO miet_checkout_retention_seen VALUES (?,?)', ('recent','2020-01-01T00:00:00+00:00'))
        db.commit()
        before = db.execute("SELECT payload FROM miet_checkout_holds WHERE id='isolated'").fetchone()[0]
        report = retention_report(db, datetime(2021, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(report['classes'], {
            'contract_or_payment_record': 3,
            'invalid_or_legacy_signed_record': 1,
            'isolated_released_before_cutoff': 1,
            'isolated_released_newer_than_cutoff': 1,
            'legal_hold': 1,
            'manual_review': 1,
            'no_signature': 1,
            'pending': 1,
            'provider_or_financial_history': 9,
        })
        self.assertEqual(report['total_holds'], 19)
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
            self.assertEqual(report['classes'], {'isolated_released_not_observed': 1})
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

    def test_apply_requires_approval_then_observes_full_period_before_delete(self):
        db = make_db()
        add(db, 'isolated')
        db.commit()
        first = datetime(2021, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(cleanup(db, RetentionPolicy(30), now=first)['report']['classes'],
                         {'isolated_released_not_observed': 1})
        self.assertEqual(db.execute('SELECT COUNT(*) FROM miet_checkout_retention_seen').fetchone()[0], 0)
        with self.assertRaises(ValueError):
            cleanup(db, RetentionPolicy(30), apply=True, now=first)
        approved = RetentionPolicy(30, approval_ref='approved-test-policy')
        one = cleanup(db, approved, apply=True, now=first)
        self.assertEqual((one['observed'], one['deleted']), (1, 0))
        two = cleanup(db, approved, apply=True, now=datetime(2021, 1, 30, tzinfo=timezone.utc))
        self.assertEqual((two['observed'], two['deleted']), (0, 0))
        three = cleanup(db, approved, apply=True, now=datetime(2021, 2, 1, tzinfo=timezone.utc))
        self.assertEqual((three['observed'], three['deleted']), (0, 1))
        self.assertEqual(db.execute('SELECT COUNT(*) FROM miet_checkout_holds').fetchone()[0], 0)
        db.close()

    def test_legal_hold_and_provider_history_never_purged(self):
        db = make_db()
        add(db, 'legal')
        add(db, 'provider', session_id='cs_test_placeholder')
        add(db, 'paid', status='confirmed')
        add(db, 'review', status='review')
        db.commit()
        block_hold(db, 'legal', 'Customer dispute', now=datetime(2021, 1, 1, tzinfo=timezone.utc))
        result = cleanup(db, RetentionPolicy(1, 'approved-test-policy'), apply=True,
                         now=datetime(2021, 1, 2, tzinfo=timezone.utc))
        self.assertEqual((result['observed'], result['deleted']), (0, 0))
        self.assertEqual(db.execute('SELECT COUNT(*) FROM miet_checkout_holds').fetchone()[0], 4)
        db.close()

    def test_legal_hold_added_after_observation_blocks_due_cleanup(self):
        db = make_db()
        add(db, 'legal')
        db.execute('INSERT INTO miet_checkout_retention_seen VALUES (?,?)',
                   ('legal','2020-01-01T00:00:00+00:00'))
        db.commit()
        block_hold(db, 'legal', 'Pending dispute', now=datetime(2021, 1, 1, tzinfo=timezone.utc))
        result = cleanup(db, RetentionPolicy(30, 'approved-test-policy'), apply=True,
                         now=datetime(2021, 2, 1, tzinfo=timezone.utc))
        self.assertEqual(result['deleted'], 0)
        self.assertEqual(result['report']['classes'], {'legal_hold': 1})
        db.close()

    def test_delete_rechecks_new_provider_event_and_rolls_back(self):
        db = make_db()
        add(db, 'isolated')
        db.execute('INSERT INTO miet_checkout_retention_seen VALUES (?,?)',
                   ('isolated','2020-01-01T00:00:00+00:00'))
        db.commit()

        class RaceDb:
            def execute(self, sql, params=()):
                if sql.startswith('DELETE FROM miet_checkout_holds'):
                    db.execute('INSERT INTO miet_checkout_events VALUES (?)', ('isolated',))
                return db.execute(sql, params)
            def commit(self):db.commit()
            def rollback(self):db.rollback()

        with self.assertRaises(RuntimeError):
            cleanup(RaceDb(), RetentionPolicy(30, 'approved-test-policy'), apply=True,
                    now=datetime(2021, 2, 1, tzinfo=timezone.utc))
        self.assertEqual(db.execute('SELECT COUNT(*) FROM miet_checkout_holds').fetchone()[0], 1)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM miet_checkout_events').fetchone()[0], 0)
        db.close()

    def test_cleanup_cli_migration_dry_run_and_apply_guard(self):
        with tempfile.TemporaryDirectory(prefix='mos-retention-cli-test-') as directory:
            path = Path(directory) / 'copy.sqlite3'
            db = make_db(path)
            add(db, 'isolated')
            db.commit();db.close()
            output = StringIO()
            with redirect_stdout(output):
                cleanup_main(['--sqlite-db', str(path), '--migrate'])
            self.assertTrue(json.loads(output.getvalue())['schema_migrated'])
            output = StringIO()
            with redirect_stdout(output):
                cleanup_main(['--sqlite-db', str(path)])
            self.assertEqual(json.loads(output.getvalue())['deleted'], 0)
            policy_path = Path(directory) / 'approved-policy.json'
            policy_path.write_text(json.dumps({'scope': 'provider_untouched_released_holds_v1',
                'retention_days': 30, 'approval_ref': 'synthetic-test-approval',
                'approved_at': '2020-01-01T00:00:00Z'}), encoding='utf-8')
            self.assertEqual(load_policy(policy_path).days, 30)
            with patch.dict('os.environ', {'MOS_SIGNATURE_RETENTION_APPLY_ENABLED': ''}):
                with redirect_stderr(StringIO()), self.assertRaises(SystemExit):
                    cleanup_main(['--sqlite-db', str(path), '--apply', '--policy-file', str(policy_path)])
            with patch.dict('os.environ', {'MOS_SIGNATURE_RETENTION_APPLY_ENABLED': '1'}):
                output = StringIO()
                with redirect_stdout(output):
                    cleanup_main(['--sqlite-db', str(path), '--apply', '--policy-file', str(policy_path)])
                first = json.loads(output.getvalue())
            self.assertEqual((first['observed'], first['deleted']), (1, 0))
            block_path = Path(directory) / 'legal-hold.json'
            block_path.write_text(json.dumps({'hold_id': 'isolated', 'reason': 'Synthetic dispute'}),
                                  encoding='utf-8')
            output = StringIO()
            with redirect_stdout(output):
                cleanup_main(['--sqlite-db', str(path), '--block-file', str(block_path)])
            self.assertTrue(json.loads(output.getvalue())['legal_hold_recorded'])
            verification = sqlite3.connect(path)
            try:
                self.assertEqual(verification.execute('SELECT COUNT(*) FROM miet_checkout_holds').fetchone()[0], 1)
                self.assertEqual(verification.execute('SELECT COUNT(*) FROM miet_checkout_retention_blocks').fetchone()[0], 1)
            finally:verification.close()


if __name__ == '__main__':
    unittest.main()
