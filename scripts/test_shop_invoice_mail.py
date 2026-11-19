"""Exercise the invoice transport with a temporary MailOutbox and fake SMTP/IMAP."""
from email import policy
from email.parser import BytesParser
from pathlib import Path
import smtplib
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mailbox_outbox import MailOutbox
from shop_invoice_mail import ShopInvoiceMailer
from test_mailbox_outbox import FakeMailbox, FakeSMTP


class InvoiceMailTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.smtp = FakeSMTP()
        self.imap = FakeMailbox()
        self.recipient = 'kunde+shop@example.test'
        self.config = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                       'smtp_host': 'smtp.example.test', 'smtp_port': 465, 'smtp_user': self.imap.user,
                       '_smtp_password': 'fake', 'from_address': self.imap.user}

        def connect():
            db = sqlite3.connect(root / 'test.sqlite3')
            db.row_factory = sqlite3.Row
            return db

        self.outbox = MailOutbox(connect, root / 'outbox', self.imap)
        self.mailer = ShopInvoiceMailer(outbox=self.outbox, smtp_config=lambda: self.config,
                                        sender_address=self.imap.user)
        self.args = dict(order_id='sho_' + 'a' * 32, recipient=self.recipient, voucher_number='RE/123',
                         content=b'%PDF-1.7\ninvoice fixture\n%%EOF\n', filename='Rechnung-RE-123.pdf')
        for name in ('SMTP_SSL', 'SMTP'):
            patched = patch('mailbox_outbox.smtplib.' + name, return_value=self.smtp)
            patched.start()
            self.addCleanup(patched.stop)

    def status(self):
        return self.mailer.status(order_id=self.args['order_id'], recipient=self.recipient)

    def test_one_message_one_customer_exact_pdf_and_replay(self):
        self.assertEqual('unknown', self.status())
        self.assertEqual('submitted', self.mailer.send(**self.args))
        self.assertEqual('submitted', self.status())
        self.assertEqual('submitted', self.mailer.send(**self.args))
        self.assertEqual(1, self.smtp.data_calls)
        mail = BytesParser(policy=policy.default).parsebytes(self.smtp.raw[0])
        self.assertEqual(self.recipient, str(mail['To']))
        self.assertEqual(self.imap.user, str(mail['From']))
        self.assertNotIn('Cc', mail)
        self.assertNotIn('Bcc', mail)
        self.assertIn('RE/123', str(mail['Subject']))
        self.assertIn(self.args['order_id'], mail.get_body().get_content())
        (part,) = mail.iter_attachments()
        self.assertEqual('application/pdf', part.get_content_type())
        self.assertEqual(self.args['filename'], part.get_filename())
        self.assertEqual(self.args['content'], part.get_payload(decode=True))

    def test_replay_changed_recipient_and_attachment_cannot_be_sent(self):
        self.assertEqual('submitted', self.mailer.send(**self.args))
        for changes in ({'recipient': 'other@example.test'}, {'content': b'%PDF-1.7\nother\n%%EOF\n'}):
            self.assertEqual('unknown', self.mailer.send(**(self.args | changes)))
        self.assertEqual(1, self.smtp.data_calls)
        self.assertEqual('unknown', self.mailer.status(order_id=self.args['order_id'], recipient='other@example.test'))

    def test_invalid_inputs_and_configuration_stay_before_delivery(self):
        for changes in ({'recipient': 'a@example.test, b@example.test'}, {'recipient': 'a@example.test\r\nCc: b@example.test'},
                        {'voucher_number': 'RE\nBcc: b@example.test'}, {'filename': '../Rechnung-RE-123.pdf'},
                        {'filename': 'Rechnung-456.pdf'}, {'content': b'<html>Login</html>'},
                        {'content': b'%PDF-1.7\ntruncated'}):
            with self.subTest(changes=changes):
                self.assertEqual('not_sent', self.mailer.send(**(self.args | changes)))
        self.config['from_address'] = 'other@example.test'
        self.assertEqual('not_sent', self.mailer.send(**self.args))
        self.assertEqual([], self.outbox.recent())
        self.assertEqual(0, self.smtp.data_calls)

    def test_known_rejection_can_retry(self):
        self.smtp.rcpt_results[self.recipient] = (451, b'try later')
        self.assertEqual('not_sent', self.mailer.send(**self.args))
        self.assertEqual('not_sent', self.status())
        self.assertEqual(0, self.smtp.data_calls)
        self.smtp.rcpt_results.clear()
        self.assertEqual('submitted', self.mailer.send(**self.args))
        self.assertEqual(1, self.smtp.data_calls)

    def test_lost_smtp_reply_never_submits_again(self):
        self.smtp.data_error = smtplib.SMTPServerDisconnected('connection lost')
        self.assertEqual('unknown', self.mailer.send(**self.args))
        self.assertEqual('unknown', self.status())
        self.smtp.data_error = None
        self.assertEqual('unknown', self.mailer.send(**self.args))
        self.assertEqual(1, self.smtp.data_calls)

    def test_accepted_when_sent_folder_copy_fails(self):
        self.imap.client.append_result = ('NO', [])
        self.assertEqual('submitted', self.mailer.send(**self.args))
        self.assertEqual('submitted', self.status())
        self.assertEqual(1, self.smtp.data_calls)


if __name__ == '__main__':
    unittest.main()
