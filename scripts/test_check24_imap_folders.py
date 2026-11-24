"""Offline IMAP folder discovery and exactly-once booking across folders."""
from contextlib import closing, nullcontext
from datetime import datetime
import hashlib
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scripts.test_check24_buchungen import BODY, synthetic_mail


class FolderServer:
    """Minimal IMAP server: equal UIDs and UIDVALIDITY are intentional."""

    def __init__(self, folders):
        self.folders = folders
        self.selected = None
        self.selections = []
        self.fetches = []

    def list(self):
        return 'OK', [f'(\\HasNoChildren) "." "{name}"'.encode() for name in self.folders]

    def select(self, name, readonly):
        assert readonly is True
        self.selected = name.strip('"')
        assert self.selected in self.folders
        self.selections.append(self.selected)
        return 'OK', [b'1']

    def response(self, name):
        assert name == 'UIDVALIDITY'
        return name, [b'73']

    def uid(self, action, *args):
        messages = self.folders[self.selected]
        if action == 'SEARCH':
            assert args == (None, 'SINCE', '11-Sep-2026', 'FROM', '"montagepartner@check24.de"')
            return 'OK', [' '.join(messages).encode()]
        assert action == 'FETCH'
        uid, fields = args
        raw = messages[uid]
        if fields == '(RFC822.SIZE)':
            return 'OK', [f'(RFC822.SIZE {len(raw)})'.encode()]
        assert fields == '(FLAGS BODY.PEEK[])'
        self.fetches.append((self.selected, uid))
        return 'OK', [(b'(FLAGS (\\Seen))', raw)]


def verified_booking(number, hour):
    header = (b'Authentication-Results: kundenserver.de; dkim=pass header.i=@check24.de; '
              b'spf=pass smtp.mailfrom=montagepartner@check24.de; dmarc=pass header.from=check24.de\n'
              b'Received: from example.invalid by mx.kundenserver.de (test); Mon, 26 Oct 2026 11:29:01 +0100\n')
    body = BODY.replace('TEST9001', number).replace('08:00 - 08:30', f'{hour}:00 - {hour}:30')
    return header + synthetic_mail(body, subject=f'Neue CHECK24 Terminbuchung (Bestellnummer: {number})')


class FolderSyncTest(unittest.TestCase):
    def test_filtered_seen_mail_and_copies_keep_one_order_each(self):
        import smoke_test

        portal = smoke_test.portal
        cfg = {'configured': True, 'ssl': True, 'host': 'imap.ionos.de',
               'user': 'werkstatt@example.invalid', 'folder': 'INBOX'}
        primary = verified_booking('FOLDER123', '13')
        filtered = verified_booking('FOLDER124', '14')
        server = FolderServer({'INBOX': {'1': primary}, 'INBOX.Check24': {'1': filtered, '2': primary},
                               'Trash.CHECK24': {'1': primary}})
        frozen = datetime(2026, 10, 26, 12, tzinfo=ZoneInfo('Europe/Berlin'))
        with patch('mailbox_client.Mailbox.connect', return_value=nullcontext(server)), \
             patch.object(portal, 'get_werkstatt_imap_config', return_value=cfg), \
             patch('check24_buchungen.datetime', wraps=datetime) as clock:
            clock.now.return_value = frozen
            # The inbox already has a receipt written by the previous scanner.
            inbox_hash = hashlib.sha256(b'imap.ionos.de/werkstatt@example.invalid/INBOX').hexdigest()
            portal.check24_bookings.ingest(primary, inbox_hash + '/73/1', verified=True, auto_apply=True)
            with patch('check24_buchungen.SYNC_BATCH_LIMIT', 2):
                self.assertEqual(portal.check24_bookings.sync(auto_apply=True), 2)
            self.assertEqual(server.fetches, [('INBOX.Check24', '1'), ('INBOX.Check24', '2')])
            self.assertEqual(portal.check24_bookings.sync(auto_apply=True), 0)
            with closing(portal.get_db()) as db:
                bookings = db.execute("SELECT order_id FROM check24_bookings WHERE provider_id IN (?,?)",
                                      ('FOLDER123', 'FOLDER124')).fetchall()
                self.assertEqual(len({row['order_id'] for row in bookings}), 2)
                event = db.execute('SELECT raw_hash FROM check24_events WHERE provider_id=?', ('FOLDER123',)).fetchall()
                self.assertEqual(len(event), 1)
                copies = db.execute('SELECT source_key FROM check24_receipts WHERE raw_hash=?',
                                    (event[0]['raw_hash'],)).fetchall()
                self.assertEqual(len(copies), 2)
            del server.folders['INBOX.Check24']
            server.selections.clear()
            self.assertEqual(portal.check24_bookings.sync(), 0)
            self.assertEqual(server.selections, ['INBOX'])

    def test_backlog_reserves_batch_capacity_for_filtered_mail(self):
        import smoke_test

        portal = smoke_test.portal
        cfg = {'configured': True, 'ssl': True, 'host': 'imap.ionos.de',
               'user': 'batch@example.invalid', 'folder': 'INBOX'}
        raw = synthetic_mail()
        server = FolderServer({'INBOX': {str(i): raw for i in range(1, 7)},
                               'CHECK24': {'1': raw, '2': raw}})
        frozen = datetime(2026, 10, 26, 12, tzinfo=ZoneInfo('Europe/Berlin'))
        with patch('mailbox_client.Mailbox.connect', return_value=nullcontext(server)), \
             patch.object(portal, 'get_werkstatt_imap_config', return_value=cfg), \
             patch('check24_buchungen.SYNC_BATCH_LIMIT', 4), \
             patch('check24_buchungen.datetime', wraps=datetime) as clock:
            clock.now.return_value = frozen
            self.assertEqual(portal.check24_bookings.sync(), 4)
            self.assertEqual(server.fetches, [('INBOX', '1'), ('INBOX', '2'), ('CHECK24', '1'), ('CHECK24', '2')])
            del server.folders['CHECK24']
            self.assertEqual(portal.check24_bookings.sync(), 4)
            self.assertEqual(portal.check24_bookings.sync(), 0)

    def test_configured_folder_and_inbox_children_are_selected_once(self):
        from check24_buchungen import Check24Bookings
        from mailbox_client import Mailbox

        server = FolderServer({'Anfragen': {}, 'INBOX': {}, 'Anfragen/CHECK24': {},
                               'INBOX.Check24': {}, 'CHECK24': {}, 'Trash/CHECK24': {}})
        box = Mailbox(lambda: {})
        self.assertEqual(Check24Bookings._scan_folders(box, server, 'Anfragen'),
                         ['Anfragen', 'INBOX', 'Anfragen/CHECK24', 'INBOX.Check24', 'CHECK24'])
        self.assertEqual(Check24Bookings._scan_folders(box, server, 'INBOX.Check24'),
                         ['INBOX.Check24', 'INBOX', 'CHECK24'])
        server.folders = {'INBOX': {}}
        self.assertEqual(Check24Bookings._scan_folders(box, server, 'fehlend'), ['INBOX'])


if __name__ == '__main__':
    unittest.main()
