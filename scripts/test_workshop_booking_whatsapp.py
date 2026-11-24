"""Offline checks using temporary databases and a recording WhatsApp transport."""

from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import workshop_booking_whatsapp as notices


class Portal:
    PORTAL_BASE_URL = 'https://portal.example'

    def __init__(self, path):
        self.path = path
        self.phones = ['+491111', '+492222']
        self.config_errors = []
        self.calls = []
        self.responses = []

    def open_fresh_db(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db

    def whatsapp_workshop_numbers(self):
        return self.phones

    def whatsapp_bridge_config_errors(self):
        return self.config_errors

    def whatsapp_message_template_enabled(self):
        return True

    def whatsapp_payload_phone(self, phone):
        return phone.lstrip('+')

    def build_whatsapp_template_payload(self, phone, order, message, sender, *, reference):
        return {'type': 'template', 'to': phone, 'reference': reference, 'detail': message}

    def build_whatsapp_text_payload(self, phone, message):
        return {'type': 'text', 'to': phone, 'detail': message}

    def post_whatsapp_payload(self, payload, *, include_http_status):
        assert include_http_status
        self.calls.append(payload)
        return self.responses.pop(0) if self.responses else (True, 'receipt-' + payload['to'], '', 200)


class TestBookingWhatsApp(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.portal = Portal(Path(scratch.name) / 'notifications.db')
        self.db = self.portal.open_fresh_db()
        self.addCleanup(self.db.close)
        notices.init_schema(self.db)
        self.db.commit()

    def queue(self):
        notices.enqueue(self.db, event_key='rental:42', reference='M-42', title='Miete bestätigt',
                        message='Hyundai i10, Abholung am Montag', path='/admin/mietfahrzeuge')

    def states(self):
        return [tuple(r) for r in self.db.execute("""SELECT phone,state,attempts FROM
                workshop_booking_wa_deliveries ORDER BY phone""")]

    def test_transaction_and_repeated_transition(self):
        self.queue()
        self.db.rollback()
        self.assertEqual(notices.run_cycle(self.portal), 0)
        self.queue()
        self.db.commit()
        self.assertEqual(notices.run_cycle(self.portal), 2)
        self.queue()
        self.db.commit()
        self.assertEqual(notices.run_cycle(self.portal), 0)
        self.assertEqual(self.states(), [('491111','sent',1), ('492222','sent',1)])
        self.assertTrue(all(p['reference'] == 'M-42' and p['detail'].endswith(
            'https://portal.example/admin/mietfahrzeuge') for p in self.portal.calls))

    def test_configured_later(self):
        self.queue()
        self.db.commit()
        self.portal.config_errors = ['unavailable']
        self.assertEqual(notices.run_cycle(self.portal), 0)
        self.portal.config_errors.clear()
        self.assertEqual(notices.run_cycle(self.portal), 2)

    def test_explicit_rejection_can_fall_back_per_recipient(self):
        self.queue()
        self.db.commit()
        self.portal.responses = [(False, '', 'Template not found', 400),
                                 (True, 'text-id', '', 200), (False, '', 'Rate limited', 429)]
        notices.run_cycle(self.portal)
        self.assertEqual([p['type'] for p in self.portal.calls], ['template','text','template'])
        self.assertEqual(self.states(), [('491111','sent',1), ('492222','pending',1)])
        self.db.execute('UPDATE workshop_booking_wa_deliveries SET retry_at=0')
        self.db.commit()
        notices.run_cycle(self.portal)
        self.assertEqual(self.states(), [('491111','sent',1), ('492222','sent',2)])

    def test_ambiguous_submission_never_repeats(self):
        self.queue()
        self.db.commit()
        self.portal.responses = [(False, '', 'Timeout', None), (False, '', 'Upstream unavailable', 502)]
        notices.run_cycle(self.portal)
        self.assertEqual(notices.run_cycle(self.portal), 0)
        self.assertEqual(len(self.portal.calls), 2)
        self.assertEqual(self.states(), [('491111','review',1), ('492222','review',1)])

    def test_abandoned_claim_is_quarantined(self):
        self.queue()
        self.db.commit()
        notices._prepare(self.db, ['491111'], 300, 1)
        notices._claim(self.db, 300)
        self.assertEqual(notices.run_cycle(self.portal), 0)
        self.assertEqual(self.states(), [('491111','review',1)])
        self.assertFalse(self.portal.calls)

    def test_parallel_workers_send_once_per_recipient(self):
        self.queue()
        self.db.commit()
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: notices.run_cycle(self.portal), range(4)))
        self.assertEqual(sum(results), 2)
        self.assertEqual(self.states(), [('491111','sent',1), ('492222','sent',1)])

    def test_sqlite_file_restore_requires_current_receipts(self):
        incoming = self.portal.path.parent / 'old.db'
        sqlite3.connect(incoming).close()
        notices.check_sqlite_replacement(self.portal.path, incoming)
        self.queue()
        self.db.commit()
        with closing(sqlite3.connect(incoming)) as target:
            self.db.backup(target)
        notices.run_cycle(self.portal)
        with self.assertRaisesRegex(ValueError, 'Versandhistorie'):
            notices.check_sqlite_replacement(self.portal.path, incoming)
        with closing(sqlite3.connect(incoming)) as target:
            self.db.backup(target)
        notices.check_sqlite_replacement(self.portal.path, incoming)

    def test_empty_target_never_activates_archived_pending_work(self):
        empty = self.portal.path.parent / 'empty.db'
        self.queue()
        self.db.commit()
        with self.assertRaisesRegex(ValueError, 'erneut aktivieren'):
            notices.check_sqlite_replacement(empty, self.portal.path)
        notices._prepare(self.db, ['491111'], 300, 1)
        with self.assertRaisesRegex(ValueError, 'erneut aktivieren'):
            notices.check_sqlite_replacement(empty, self.portal.path)
        notices.run_cycle(self.portal)
        notices.check_sqlite_replacement(empty, self.portal.path)


if __name__ == '__main__':
    unittest.main()
