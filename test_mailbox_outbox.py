"""Isolated SMTP/IMAP regressions; never connects to a real mail server."""
import ast
from contextlib import contextmanager
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
import re
import smtplib
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid

from mailbox_outbox import MailOutbox


def message(body='Hallo mit Signatur', attachment=b'attachment bytes', to='one@example.test'):
    msg = EmailMessage()
    msg['From'] = 'Werkstatt <sender@example.test>'
    msg['To'] = to
    msg['Subject'] = 'Test mit Umlaut: Grüße'
    msg['Date'] = 'Sun, 06 Sep 2026 22:00:00 +0200'
    msg['Message-ID'] = make_msgid(domain='example.test')
    msg.set_content(body)
    msg.add_alternative('<p>' + body + '</p>', subtype='html')
    msg.add_attachment(attachment, maintype='application', subtype='octet-stream', filename='Nachweis.pdf')
    return msg


class FakeSMTP:
    def __init__(self):
        self.data_calls = 0
        self.raw = []
        self.login_error = None
        self.mail_error = None
        self.rcpt_error = None
        self.rcpt_results = {}
        self.data_error = None
        self.data_result = (250, b'accepted')
        self.quit_error = None
        self.in_data = None
        self.release_data = None
        self.after_data = None

    def starttls(self, **kwargs):
        pass

    def login(self, *args):
        if self.login_error:
            raise self.login_error

    def ehlo_or_helo_if_needed(self):
        pass

    def has_extn(self, name):
        return False

    def mail(self, sender, options=()):
        if self.mail_error:
            raise self.mail_error
        return 250, b'ok'

    def rcpt(self, address):
        if self.rcpt_error:
            raise self.rcpt_error
        return self.rcpt_results.get(address, (250, b'ok'))

    def data(self, raw):
        self.data_calls += 1
        self.raw.append(raw)
        if self.in_data:
            self.in_data.set()
            if not self.release_data.wait(10):
                raise TimeoutError('test barrier')
        if self.after_data:
            self.after_data()
        if self.data_error:
            raise self.data_error
        return self.data_result

    def quit(self):
        if self.quit_error:
            raise self.quit_error

    def close(self):
        pass


class FakeIMAP:
    def __init__(self):
        self.append_calls = 0
        self.search_calls = 0
        self.messages = []
        self.append_result = ('OK', [b'done'])
        self.lost_response = False
        self.search_result = None
        self.in_append = None
        self.release_append = None

    def uid(self, command, *args):
        assert command == 'SEARCH'
        self.search_calls += 1
        if self.search_result is not None:
            return self.search_result
        sought = args[-1].strip('"').encode()
        return 'OK', [b'1' if any(sought in raw for raw in self.messages) else b'']

    def append(self, folder, flags, when, raw):
        assert flags == r'(\Seen)'
        self.append_calls += 1
        if self.in_append:
            self.in_append.set()
            if not self.release_append.wait(10):
                raise TimeoutError('test barrier')
        if self.append_result[0] == 'OK':
            self.messages.append(raw)
        if self.lost_response:
            raise OSError('APPEND accepted, response lost')
        return self.append_result


class FakeMailbox:
    def __init__(self):
        self.client = FakeIMAP()
        self.user = 'sender@example.test'
        self.folder_list = [{'id': 'Sent Folder', 'label': 'Gesendete Objekte', 'flags': ''}]
        self.connect_calls = 0

    def config(self):
        return {'user': self.user}

    @contextmanager
    def connect(self):
        self.connect_calls += 1
        yield self.client

    def folders(self, client):
        return self.folder_list

    def select(self, client, folder, readonly=True):
        assert readonly is True
        assert folder in {f['id'] for f in self.folder_list}
        return '1'


class OutboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dbpath = self.root / 'test.sqlite3'
        self.service = FakeMailbox()
        self.smtp = FakeSMTP()
        self.cfg = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                    'smtp_host': 'smtp.example.test', 'smtp_port': 465,
                    'smtp_user': 'sender@example.test', '_smtp_password': 'synthetic',
                    'from_address': 'Werkstatt <sender@example.test>'}
        self.token = str(uuid.uuid4())
        self.outbox = MailOutbox(self.get_db, self.root / 'private', self.service)
        self.ssl_patch = patch('mailbox_outbox.smtplib.SMTP_SSL', return_value=self.smtp)
        self.smtp_constructor = self.ssl_patch.start()
        self.addCleanup(self.ssl_patch.stop)
        self.tls_patch = patch('mailbox_outbox.smtplib.SMTP', return_value=self.smtp)
        self.tls_constructor = self.tls_patch.start()
        self.addCleanup(self.tls_patch.stop)

    def get_db(self):
        db = sqlite3.connect(self.dbpath, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def restarted(self):
        return MailOutbox(self.get_db, self.root / 'private', self.service)

    def test_success_and_replay_with_regenerated_mime_is_single_submission(self):
        msg = message()
        result = self.outbox.send(self.token, msg, self.cfg)
        self.assertEqual('sent', result['state'])
        self.assertFalse(result['can_retry'])
        self.assertFalse(result['copy_pending'])
        self.assertFalse(result['payload_available'])
        changed_framing = message()
        changed_framing.replace_header('Date', 'Mon, 07 Sep 2026 22:00:00 +0200')
        changed_framing.as_bytes()
        self.assertEqual(result, self.restarted().send(self.token, changed_framing, self.cfg))
        self.assertEqual(1, self.smtp.data_calls)
        self.assertEqual(1, self.service.client.append_calls)
        self.assertEqual(msg['Message-ID'], result['message_id'])
        with self.assertRaises(FileNotFoundError):
            self.outbox.payload(self.token)
        self.assertEqual(['one@example.test'], result['accepted'])

    def test_same_token_changed_body_attachment_or_recipient_is_rejected(self):
        self.service.client.append_result = ('NO', [])
        self.outbox.send(self.token, message(), self.cfg)
        for changed in (message(body='Anderer Text'), message(attachment=b'changed'),
                        message(to='other@example.test')):
            with self.assertRaisesRegex(ValueError, 'anderen Nachrichteninhalt'):
                self.outbox.send(self.token, changed, self.cfg)
        self.assertEqual(1, self.smtp.data_calls)

    def test_safe_predata_failures_are_retryable_and_keep_payload(self):
        for failure in ('connect', 'login', 'mail', 'rcpt', 'all_refused', 'data_rejected', 'data_command_rejected'):
            with self.subTest(failure=failure):
                token = str(uuid.uuid4())
                self.smtp.login_error = self.smtp.mail_error = self.smtp.rcpt_error = None
                self.smtp.rcpt_results = {}
                self.smtp.data_result = (250, b'ok')
                self.smtp.data_error = None
                self.smtp_constructor.side_effect = None
                if failure == 'connect':
                    self.smtp_constructor.side_effect = OSError('connection failed')
                elif failure == 'login':
                    self.smtp.login_error = smtplib.SMTPAuthenticationError(535, b'no')
                elif failure == 'mail':
                    self.smtp.mail_error = OSError('MAIL response lost')
                elif failure == 'rcpt':
                    self.smtp.rcpt_error = OSError('RCPT response lost')
                elif failure == 'all_refused':
                    self.smtp.rcpt_results = {'one@example.test': (550, b'no')}
                elif failure == 'data_rejected':
                    self.smtp.data_result = (554, b'rejected')
                else:
                    self.smtp.data_error = smtplib.SMTPDataError(554, b'no DATA')
                result = self.outbox.send(token, message(), self.cfg)
                self.assertEqual('not_sent', result['state'])
                self.assertTrue(result['can_retry'])
                payload = self.outbox.payload(token)
                self.assertIn(b'Nachweis.pdf', payload)
                self.assertIn(b'Hallo mit Signatur', payload)
                self.assertEqual('not_sent', self.restarted().status(token)['state'])
        self.assertEqual(0, self.service.client.append_calls)

    def test_explicit_retry_of_known_rejection_reuses_original_payload(self):
        self.smtp.login_error = OSError('failed')
        self.outbox.send(self.token, message(), self.cfg)
        raw = self.outbox.payload(self.token)
        self.smtp.login_error = None
        self.assertEqual('sent', self.restarted().send(self.token, message(), self.cfg)['state'])
        self.assertEqual([raw], self.smtp.raw)

    def test_data_response_loss_blocks_smtp_even_after_restart(self):
        self.smtp.data_error = OSError('DATA response lost')
        initial = self.outbox.send(self.token, message(), self.cfg)
        self.assertEqual('uncertain', initial['state'])
        self.assertFalse(initial['can_retry'])
        self.smtp.data_error = None
        self.assertEqual('uncertain', self.restarted().send(self.token, message(), self.cfg)['state'])
        self.assertEqual('uncertain', self.restarted().retry_copy(self.token)['state'])
        self.assertEqual(1, self.smtp.data_calls)
        self.assertEqual(0, self.service.client.append_calls)
        self.assertTrue(initial['payload_available'])

    def test_quit_failure_does_not_reverse_acceptance(self):
        self.smtp.quit_error = OSError('QUIT response lost')
        self.assertEqual('sent', self.outbox.send(self.token, message(), self.cfg)['state'])
        self.assertEqual(1, self.service.client.append_calls)

    def test_partial_recipients_never_offer_general_retry(self):
        self.smtp.rcpt_results = {'two@example.test': (550, b'no')}
        msg = message(to='one@example.test,two@example.test')
        result = self.outbox.send(self.token, msg, self.cfg)
        self.assertEqual('partial', result['state'])
        self.assertEqual(['one@example.test'], result['accepted'])
        self.assertEqual(550, result['refused']['two@example.test']['code'])
        self.assertFalse(result['can_retry'])
        self.assertFalse(result['copy_pending'])
        self.outbox.send(self.token, msg, self.cfg)
        self.assertEqual(1, self.smtp.data_calls)

    def test_copy_failure_and_restart_retry_only_append(self):
        self.service.client.append_result = ('NO', [])
        result = self.outbox.send(self.token, message(), self.cfg)
        self.assertEqual('copy_pending', result['state'])
        self.assertTrue(result['can_retry_copy'])
        self.assertFalse(result['can_retry'])
        raw = self.outbox.payload(self.token)
        self.service.client.append_result = ('OK', [])
        result = self.restarted().retry_copy(self.token)
        self.assertEqual('sent', result['state'])
        self.assertEqual(1, self.smtp.data_calls)
        self.assertEqual([raw], self.service.client.messages)
        self.assertFalse(result['payload_available'])

    def test_lost_append_response_recovers_by_message_id_without_duplicate(self):
        self.service.client.lost_response = True
        result = self.outbox.send(self.token, message(), self.cfg)
        self.assertEqual('copy_pending', result['state'])
        self.assertTrue(result['payload_available'])
        self.service.client.lost_response = False
        self.assertEqual('sent', self.restarted().retry_copy(self.token)['state'])
        self.assertEqual(1, self.service.client.append_calls)
        self.assertEqual(1, self.smtp.data_calls)
        self.assertEqual(2, self.service.client.search_calls)

    def test_status_and_recent_never_touch_network(self):
        self.service.client.append_result = ('NO', [])
        self.outbox.send(self.token, message(), self.cfg)
        connects = self.service.connect_calls
        self.assertEqual('copy_pending', self.restarted().status(self.token)['state'])
        recent = self.restarted().recent()
        self.assertEqual(1, len(recent))
        self.assertEqual('Test mit Umlaut: Grüße', recent[0]['subject'])
        self.assertEqual('one@example.test', recent[0]['to'])
        self.assertGreater(recent[0]['created_at'], 0)
        self.assertEqual(connects, self.service.connect_calls)
        self.assertEqual(1, self.smtp.data_calls)

    def test_sent_folder_and_account_checked_before_smtp(self):
        for missing, wrong_account in ((True, False), (False, True)):
            self.service.folder_list = [] if missing else [{'id': 'Sent', 'flags': r'\Sent', 'label': 'Whatever'}]
            self.service.user = 'wrong@example.test' if wrong_account else 'sender@example.test'
            result = self.outbox.send(str(uuid.uuid4()), message(), self.cfg)
            self.assertEqual('not_sent', result['state'])
            self.assertTrue(result['payload_available'])
        self.smtp_constructor.assert_not_called()

    def test_sent_flags_and_aliases(self):
        for folder in ({'id': 'Custom', 'flags': r'\HasNoChildren \Sent', 'label': 'Custom'},
                       {'id': 'Gesendete Objekte', 'flags': '', 'label': 'Gesendete Objekte'},
                       {'id': 'Sent Items', 'flags': '', 'label': 'Sent Items'}):
            self.service.folder_list = [folder]
            self.assertEqual('sent', self.outbox.send(str(uuid.uuid4()), message(), self.cfg)['state'])

    def test_search_failure_never_appends_blindly(self):
        for response in (('NO', []), ('OK', [None]), ('OK', [])):
            self.service.client.search_result = response
            self.assertEqual('copy_pending', self.outbox.send(str(uuid.uuid4()), message(), self.cfg)['state'])
        self.assertEqual(0, self.service.client.append_calls)

    def test_simultaneous_requests_submit_only_once(self):
        self.smtp.in_data = threading.Event()
        self.smtp.release_data = threading.Event()
        results, errors = [], []
        def send_one():
            try:
                results.append(self.outbox.send(self.token, message(), self.cfg))
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=send_one)
        thread.start()
        try:
            self.assertTrue(self.smtp.in_data.wait(10))
            self.assertEqual('sending', self.restarted().send(self.token, message(), self.cfg)['state'])
            self.assertEqual('sending', self.restarted().status(self.token)['state'])
            with self.assertRaises(ValueError):
                self.restarted().send(self.token, message(body='changed'), self.cfg)
        finally:
            self.smtp.release_data.set()
            thread.join(10)
        self.assertFalse(errors)
        self.assertEqual('sent', results[0]['state'])
        self.assertEqual(1, self.smtp.data_calls)

    def test_simultaneous_copy_retries_append_only_once(self):
        self.service.client.append_result = ('NO', [])
        self.outbox.send(self.token, message(), self.cfg)
        self.service.client.append_result = ('OK', [])
        self.service.client.in_append = threading.Event()
        self.service.client.release_append = threading.Event()
        results = []
        thread = threading.Thread(target=lambda: results.append(self.restarted().retry_copy(self.token)))
        thread.start()
        try:
            self.assertTrue(self.service.client.in_append.wait(10))
            self.assertEqual('copy_pending', self.restarted().retry_copy(self.token)['state'])
        finally:
            self.service.client.release_append.set()
            thread.join(10)
        self.assertEqual('sent', results[0]['state'])
        self.assertEqual(2, self.service.client.append_calls)
        self.assertEqual(1, self.smtp.data_calls)

    def test_interrupted_process_is_uncertain_and_never_resubmitted(self):
        class Crash(BaseException):
            pass
        def crash():
            raise Crash()
        self.smtp.after_data = crash
        with self.assertRaises(Crash):
            self.outbox.send(self.token, message(), self.cfg)
        self.assertEqual('uncertain', self.restarted().status(self.token)['state'])
        self.assertEqual('uncertain', self.restarted().send(self.token, message(), self.cfg)['state'])
        self.assertEqual(1, self.smtp.data_calls)
        self.assertTrue(self.restarted().status(self.token)['payload_available'])

    def test_database_failure_after_smtp_recovers_from_journal(self):
        original_sync = self.outbox._sync_db
        def fail_after_smtp(row):
            if row['state'] != 'sending':
                raise sqlite3.OperationalError('synthetic DB outage')
            return original_sync(row)
        with patch.object(self.outbox, '_sync_db', side_effect=fail_after_smtp):
            result = self.outbox.send(self.token, message(), self.cfg)
        self.assertEqual('copy_pending', result['state'])
        self.assertEqual('copy_pending', self.restarted().status(self.token)['state'])
        self.assertEqual('copy_pending', self.restarted().send(self.token, message(), self.cfg)['state'])
        self.assertEqual('sent', self.restarted().retry_copy(self.token)['state'])
        self.assertEqual(1, self.smtp.data_calls)
        self.assertEqual(1, self.service.client.append_calls)

    def test_journal_failure_can_recover_from_database(self):
        original_write = self.outbox._atomic_write
        def fail_journal(path, raw):
            if path.suffix == '.json':
                raise OSError('synthetic journal failure')
            return original_write(path, raw)
        with patch.object(self.outbox, '_atomic_write', side_effect=fail_journal):
            self.assertEqual('sent', self.outbox.send(self.token, message(), self.cfg)['state'])
        self.assertEqual('sent', self.restarted().status(self.token)['state'])

    def test_database_failure_after_copy_keeps_durable_sent_result(self):
        original_sync = self.outbox._sync_db
        def fail_final(row):
            if row['state'] == 'sent':
                raise sqlite3.OperationalError('DB final update failed')
            return original_sync(row)
        with patch.object(self.outbox, '_sync_db', side_effect=fail_final):
            self.assertEqual('sent', self.outbox.send(self.token, message(), self.cfg)['state'])
        self.assertEqual('sent', self.restarted().status(self.token)['state'])
        self.assertEqual('sent', self.restarted().retry_copy(self.token)['state'])
        self.assertEqual(1, self.service.client.append_calls)

    def test_no_submission_if_neither_store_can_record_sending(self):
        with patch.object(self.outbox, '_record', return_value=False):
            result = self.outbox.send(self.token, message(), self.cfg)
        self.assertEqual('not_sent', result['state'])
        self.assertEqual(0, self.smtp.data_calls)
        self.assertTrue(result['payload_available'])

    def test_legacy_guard_tokens_remain_blocked(self):
        db = self.get_db()
        try:
            db.execute('CREATE TABLE mailbox_send_guard(token TEXT PRIMARY KEY, status TEXT)')
            db.execute('INSERT INTO mailbox_send_guard VALUES (?,?)', (self.token, 'accepted'))
            db.commit()
        finally:
            db.close()
        self.assertTrue(self.outbox.status(self.token)['legacy'])
        self.assertEqual('uncertain', self.outbox.send(self.token, message(), self.cfg)['state'])
        self.smtp_constructor.assert_not_called()

    def test_bcc_is_envelope_only(self):
        msg = message()
        msg['Bcc'] = 'hidden@example.test'
        result = self.outbox.send(self.token, msg, self.cfg)
        self.assertEqual(['one@example.test', 'hidden@example.test'], result['accepted'])
        self.assertNotIn(b'Bcc:', self.smtp.raw[0])
        self.assertNotIn(b'hidden@example.test', self.service.client.messages[0])

    def test_starttls_path(self):
        self.cfg.update(smtp_ssl=False, smtp_tls=True, smtp_port=587)
        self.assertEqual('sent', self.outbox.send(self.token, message(), self.cfg)['state'])
        self.tls_constructor.assert_called_once()
        self.smtp_constructor.assert_not_called()

    def test_actual_postgres_adapter_returning_id_works(self):
        names = {'DbRow', 'PostgresCursor', 'PostgresConnection',
                 'convert_sqlite_sql_to_postgres', 'get_insert_table_name'}
        tree = ast.parse(Path(__file__).with_name('app.py').read_text(encoding='utf-8'))
        nodes = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
        namespace = {'re': re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'app.py (adapter only)', 'exec'), namespace)
        sql_seen = []
        class Cursor:
            def __init__(self, connection):
                self.cursor = connection.cursor()
                self.description = None
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.cursor.close()
            def execute(self, sql, params):
                sql_seen.append(sql)
                sql = sql.replace('%s', '?').replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT')
                self.cursor.execute(sql, params)
                self.rows = self.cursor.fetchall() if self.cursor.description else []
                self.rowcount = self.cursor.rowcount
                self.description = [type('Column', (), {'name': item[0]}) for item in self.cursor.description] if self.cursor.description else None
            def fetchall(self):
                return self.rows
        class Connection:
            def __init__(self):
                self.connection = sqlite3.connect(self_db, timeout=10)
            def cursor(self):
                return Cursor(self.connection)
            def commit(self):
                self.connection.commit()
            def rollback(self):
                self.connection.rollback()
            def close(self):
                self.connection.close()
        self_db = self.root / 'pg-shim.sqlite3'
        pg = MailOutbox(lambda: namespace['PostgresConnection'](Connection()), self.root / 'pg-private', self.service)
        self.assertEqual('sent', pg.send(self.token, message(), self.cfg)['state'])
        self.assertTrue(any('SERIAL PRIMARY KEY' in sql for sql in sql_seen))
        self.assertTrue(any('INSERT INTO mailbox_outbox' in sql and sql.endswith('RETURNING id') for sql in sql_seen))
        self.assertEqual('sent', pg.status(self.token)['state'])


if __name__ == '__main__':
    unittest.main()
