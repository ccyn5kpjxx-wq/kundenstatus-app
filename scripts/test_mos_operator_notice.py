"""Exercise the rental station outbox without external mail, payment or inventory."""

from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mietwagen_checkout import init_schema
from mos_operator_notice import STATION_ADDRESS, enqueue, run_cycle


class StationNoticeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.portal = Mock(get_db=self.db)
        self.now = datetime.now(timezone.utc)
        self.quote = {
            'test_only': False, 'signed_at': self.now.isoformat(), 'vehicle_name': 'Hyundai i10',
            'start_slot': (self.now + timedelta(days=1)).isoformat(),
            'end_slot': (self.now + timedelta(days=2)).isoformat(),
        }
        self.sender = {'from_address': 'smtp@example.test'}
        self.smtp = self.enterContext(patch('mos_operator_notice._smtp_send', return_value='sent'))
        self.enterContext(patch.dict('os.environ', {'MOS_OPERATOR_NOTIFICATION_EMAIL': ''}))
        with self.transaction() as db:
            init_schema(db)
            db.execute('CREATE TABLE miet_checkout_cancellations (id TEXT PRIMARY KEY)')
            db.execute('CREATE TABLE mietvorgaenge (id INTEGER PRIMARY KEY,status TEXT)')

    def db(self):
        conn = sqlite3.connect(Path(self.tmp.name) / 'station.sqlite3')
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def transaction(self):
        with closing(self.db()) as db, db:
            yield db

    def hold(self, id_, q, status='pending', payment_intent=None):
        with self.transaction() as db:
            rental = None
            if status == 'confirmed':
                rental = db.execute("INSERT INTO mietvorgaenge(status) VALUES ('aktiv')").lastrowid
            payload = json.dumps({'quote': q, 'customer': {'name': 'Beispiel', 'email': 'kunde@example.test'}})
            db.execute('''INSERT INTO miet_checkout_holds
                (id,request_key,mietfahrzeug_id,start_datum,end_datum,payload,fingerprint,status,
                 expires_at,payment_intent,mietvorgang_id)
                VALUES (?,?,1,?,?,?,?,?,1234,?,?)''',
                (id_, id_, q['start_slot'][:10], q['end_slot'][:10], payload, 'fingerprint', status,
                 payment_intent, rental))

    def cycle(self):
        return run_cycle(self.portal, self.sender, 'https://mos.example.test')

    def notices(self):
        with self.transaction() as db:
            return [tuple(r) for r in db.execute('''SELECT hold_id,event_type,status,attempts
                FROM miet_checkout_operator_notices ORDER BY hold_id,event_type''')]

    def test_recent_confirmations_and_pending_requests_reach_station_once(self):
        self.hold('paid', self.quote, 'confirmed', 'pi_synthetic')
        onsite = dict(self.quote, payment_method='onsite')
        self.hold('onsite', onsite, 'confirmed')
        self.hold('unpaid', self.quote)
        old = dict(self.quote, signed_at=(self.now - timedelta(days=3)).isoformat())
        self.hold('old', old, 'confirmed', 'pi_old')
        self.hold('test', dict(self.quote, test_only=True), 'confirmed', 'pi_test')
        self.hold('cancelled', self.quote, 'confirmed', 'pi_cancelled')
        with self.transaction() as db:
            db.execute("INSERT INTO miet_checkout_cancellations VALUES ('cancelled')")
            enqueue(db, 'paid', 'payment_pending', self.quote)
            enqueue(db, 'unpaid', 'payment_pending', self.quote)
            enqueue(db, 'unpaid', 'payment_pending', self.quote)
            enqueue(db, 'onsite', 'confirmed', onsite)
            enqueue(db, 'test', 'payment_pending', dict(self.quote, test_only=True))
        self.assertEqual(self.cycle(), 0)
        self.assertEqual(self.cycle(), 0)
        self.assertEqual(self.notices(), [('onsite', 'confirmed', 'sent', 1),
                                         ('paid', 'confirmed', 'sent', 1),
                                         ('paid', 'payment_pending', 'superseded', 0),
                                         ('unpaid', 'payment_pending', 'sent', 1)])
        self.assertEqual(self.smtp.call_count, 3)
        messages = [call.args[0] for call in self.smtp.call_args_list]
        self.assertTrue(all(m['To'] == STATION_ADDRESS for m in messages))
        self.assertTrue(all('Abholung:' in m.get_content() and 'Rückgabe:' in m.get_content() for m in messages))
        bodies = ' '.join(m.get_content() for m in messages)
        for text in ('Zahlung bei Abholung', 'Onlinezahlung eingegangen', 'Onlinezahlung steht noch aus'):
            self.assertIn(text, bodies)
        self.assertEqual(len({m['Message-ID'] for m in messages}), 3)

    def test_safe_retry_and_ambiguous_delivery(self):
        self.hold('online', self.quote)
        with self.transaction() as db:
            enqueue(db, 'online', 'payment_pending', self.quote)
        self.smtp.side_effect = ['not_sent', 'uncertain']
        self.assertEqual(self.cycle(), 0)
        self.assertEqual(self.cycle(), 0)
        self.assertEqual(self.notices(), [('online', 'payment_pending', 'queued', 1)])
        with self.transaction() as db:
            db.execute("UPDATE miet_checkout_operator_notices SET next_attempt_at=''")
        self.assertEqual(self.cycle(), 1)
        self.assertEqual(self.cycle(), 1)
        self.assertEqual(self.notices(), [('online', 'payment_pending', 'review', 2)])
        self.assertEqual(self.smtp.call_count, 2)
        self.assertEqual(self.smtp.call_args_list[0].args[0]['Message-ID'],
                         self.smtp.call_args_list[1].args[0]['Message-ID'])


if __name__ == '__main__':
    unittest.main()
