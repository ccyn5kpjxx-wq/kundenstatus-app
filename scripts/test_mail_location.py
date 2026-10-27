"""Synthetic rendered-mail checks; sockets and real providers are disabled."""
from email.message import EmailMessage
from hashlib import sha256
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import smoke_test as isolated
from flask import Flask
from mailbox_client import register_mailbox
from werkstatt_mail_identity import BUSINESS_ADDRESS, STREET, add_workshop_mail_footer
from werkstatt_kontakt import PHONE
import mos_contract_delivery
import mos_order_receipt
import mos_termination

p = isolated.portal
OLD_ADDRESS = 'Binauer Höhe 4, 74821 Mosbach-Lohrbach'


class MailLocationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        p.init_db()

    def assert_current(self, text):
        self.assertIn(STREET, text)
        self.assertIn('74821 Mosbach', text)
        self.assertNotIn('Binauer Höhe', text)

    def test_plain_html_bodies_are_current_idempotent_and_keep_attachments(self):
        msg = EmailMessage()
        msg['To'] = 'customer@example.invalid'
        msg['Subject'] = 'Synthetic service message'
        original_text = 'Annahme 11:00 Uhr, 120,00 EUR netto.\n'
        msg.set_content(original_text)
        msg.add_alternative('<html><body><p>Annahme 11:00 Uhr, 120,00 EUR netto.</p></body></html>', subtype='html')
        msg.add_attachment(b'Historical original: ' + OLD_ADDRESS.encode('utf-8'),
                           maintype='application', subtype='pdf', filename='original.pdf')
        old_mail = EmailMessage()
        old_mail.set_content('Historical original mail: ' + OLD_ADDRESS)
        msg.add_attachment(old_mail)
        attachments = [item.as_bytes() for item in msg.iter_attachments()]
        add_workshop_mail_footer(msg)
        self.assertTrue(msg.get_body(('plain',)).get_content().startswith(original_text))
        self.assert_current(msg.get_body(('plain',)).get_content())
        html = msg.get_body(('html',)).get_content()
        self.assert_current(html)
        self.assertLess(html.index(STREET), html.index('</body>'))
        self.assertEqual([item.as_bytes() for item in msg.iter_attachments()], attachments)
        before = msg.as_bytes()
        add_workshop_mail_footer(msg)
        self.assertEqual(msg.as_bytes(), before)
        self.assertEqual(msg['To'], 'customer@example.invalid')
        self.assertEqual(msg['Subject'], 'Synthetic service message')

    def test_three_endcustomer_mail_builders_and_html_signature(self):
        order = {'id': 1, 'kunde_name': 'Synthetic customer', 'fahrzeug': 'Synthetic car',
                 'kennzeichen': 'TEST 1', 'annahme_datum': '2026-11-02', 'annahme_uhrzeit': '11:00',
                 'kunden_status_url': 'https://example.invalid/status/synthetic'}
        with p.app.test_request_context('/'):
            for builder in (p.baue_endkunden_fertig_mail, p.baue_endkunden_danke_mail,
                            p.baue_endkunden_terminbestaetigung_mail):
                with self.subTest(builder=builder.__name__):
                    self.assert_current(builder(order))
                    self.assertIn(PHONE, builder(order))
            self.assert_current(p.render_template('_mail_signature.html'))
            self.assertIn(PHONE, p.render_template('_mail_signature.html'))

    def test_common_system_mail_footer_and_foreign_tenant_opt_out(self):
        for enabled in (True, False):
            with self.subTest(workshop=enabled):
                msg = EmailMessage()
                msg['From'] = 'info@auto-lackierzentrum.de'
                msg.set_content('Synthetic customer confirmation, unchanged amount 142,80 EUR.')
                p.add_standard_mail_headers(msg, werkstatt_footer=enabled)
                text = msg.get_content()
                if enabled:
                    self.assert_current(text)
                else:
                    self.assertNotIn(STREET, text)
                self.assertIn('142,80 EUR', text)

    def test_lead_sender_respects_brand_even_with_shared_from_address(self):
        for website, expected in (('auto-lackierzentrum', True), ('autovermietung-mos', True),
                                  ('tomorrowworks', False)):
            cfg = {'website': website, 'configured': True, 'display_name': 'Synthetic sender',
                   'address': 'info@auto-lackierzentrum.de', 'host': 'smtp.example.invalid',
                   'port': 465, 'ssl': True, 'tls': False, 'user': 'synthetic', 'password': 'synthetic'}
            with self.subTest(website=website), \
                 patch.object(p, 'get_lead_mail_config', return_value=cfg), \
                 patch.object(p.smtplib, 'SMTP_SSL') as smtp, \
                 patch.object(p, 'append_lead_message_to_sent', return_value='synthetic'), \
                 patch.object(p, 'log_lead_mail'):
                p.send_lead_email({'id': 999, 'website': website}, 'customer@example.invalid',
                                  'Synthetic subject', 'Unchanged synthetic message and 142,80 EUR.')
                msg = smtp.return_value.__enter__.return_value.send_message.call_args.args[0]
                self.assertEqual(STREET in msg.get_content(), expected)
                self.assertEqual(msg['To'], 'customer@example.invalid')
                draft = p.fallback_lead_mail_draft({'id': 999, 'website': website})
                self.assertEqual(STREET in draft['email_text'], expected)

    def mailbox_message(self, quoted=''):
        captured = []

        class CaptureOnlyOutbox:
            def __init__(self, *args, **kwargs):
                pass

            def status(self, token):
                return None

            def send(self, token, msg, cfg):
                captured.append(msg)
                return {'state': 'synthetic_capture_only'}

        app = Flask('synthetic-mail-location', template_folder=str(ROOT / 'templates'),
                    static_folder=str(ROOT / 'static'))
        app.config.update(TESTING=True, MAILBOX_SEND_ENABLED=True)
        register_mailbox(app, lambda fn: fn, lambda: {'user': 'chef@example.invalid'},
                         lambda: {'from_address': 'chef@example.invalid'},
                         lambda: self.fail('No mailbox database access allowed'))
        with patch('mailbox_outbox.MailOutbox', CaptureOnlyOutbox):
            response = app.test_client().post('/admin/mail/send', data={
                'send_token': str(uuid4()), 'to': 'customer@example.invalid',
                'subject': 'Synthetic preview', 'body': 'Unchanged synthetic body 142,80 EUR.',
                'quoted_body': quoted})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(captured), 1)
        return captured[0]

    def test_actual_mailbox_plain_and_html_composer_use_current_signature(self):
        msg = self.mailbox_message()
        self.assert_current(msg.get_body(('plain',)).get_content())
        self.assert_current(msg.get_body(('html',)).get_content())
        self.assertIn(PHONE, msg.get_body(('plain',)).get_content())
        self.assertIn(PHONE, msg.get_body(('html',)).get_content())
        images = [part for part in msg.walk() if part.get_content_maintype() == 'image']
        self.assertEqual({str(part['Content-ID']) for part in images},
                         {'<signature-logo>', '<signature-portrait>'})
        self.assertEqual(len(images), 2)
        for image in images:
            self.assertEqual(image.get_content_disposition(), 'inline')
            self.assertIn('cid:' + str(image['Content-ID']).strip('<>'),
                          msg.get_body(('html',)).get_content())

    def test_historical_quote_is_retained_in_new_mail_bodies(self):
        quote = 'Historical quoted mail: ' + OLD_ADDRESS
        msg = self.mailbox_message(quote)
        for kind in ('plain', 'html'):
            body = msg.get_body((kind,)).get_content()
            self.assertIn(STREET, body)
            self.assertIn(quote, body)

    def test_mos_receipt_and_contract_envelope_keep_original_pdf_and_price(self):
        receipt = mos_order_receipt._message({'recipient': 'customer@example.invalid',
                                            'amount_cents': 12345, 'hold_id': 'synthetic'})
        self.assert_current(receipt.get_content())
        self.assertIn('123,45 EUR', receipt.get_content())
        original = b'%PDF historical snapshot ' + OLD_ADDRESS.encode('utf-8')
        msg = mos_contract_delivery._message('synthetic', 'customer@example.invalid', original, False)
        self.assert_current(msg.get_body(('plain',)).get_content())
        self.assertEqual(list(msg.iter_attachments())[0].get_payload(decode=True), original)

    def test_termination_declaration_snapshot_remains_exact(self):
        declaration = 'Historical declaration with pickup address: ' + OLD_ADDRESS
        row = {'id': 'synthetic', 'customer_email': 'customer@example.invalid',
               'declaration_text': declaration, 'declaration_sha256': sha256(declaration.encode()).hexdigest(),
               'declaration_json': json.dumps({'email': 'customer@example.invalid'})}
        original = dict(row)
        msg = mos_termination._message(row, {'from_address': 'info@example.invalid'})
        self.assertIn(BUSINESS_ADDRESS, msg.get_content())
        self.assertIn(declaration, msg.get_content())
        self.assertEqual(row, original)

    def test_preupdate_offer_replay_keeps_history_without_smtp_retry(self):
        import smtplib
        from test_assistent_angebote import OfferTests

        for state in ('sent', 'copy_pending', 'uncertain', 'not_sent'):
            with self.subTest(state=state):
                fixture = OfferTests()
                fixture.setUp()
                try:
                    fixture.action(attachment_ids=[1])
                    if state == 'uncertain':
                        fixture.smtp.data_error = smtplib.SMTPServerDisconnected('synthetic')
                    elif state == 'not_sent':
                        fixture.smtp.data_result = (554, b'synthetic rejection')
                    elif state == 'copy_pending':
                        fixture.mailbox.client.append_result = ('NO', [])
                    # Simulate the exact composer before the postal-footer update.
                    with patch('werkstatt_assistent_angebote.add_workshop_mail_footer'):
                        first = fixture.call(fixture.service.submit_approved_action, 'quote-1')
                    self.assertEqual(first['state'], state)
                    token = fixture.service._token('admin', 'quote-1')
                    with fixture.p.workshop_orders.db() as db:
                        saved = dict(db.execute('SELECT * FROM mailbox_outbox WHERE token=?', (token,)).fetchone())
                    files_before = {path.name: path.read_bytes() for path in
                                    fixture.service.outbox.storage_dir.glob('*.eml')}
                    smtp_calls = fixture.smtp.data_calls
                    replay = fixture.call(fixture.service.submit_approved_action, 'quote-1')
                    self.assertEqual(replay['state'], state)
                    self.assertEqual(fixture.smtp.data_calls, smtp_calls)
                    with fixture.p.workshop_orders.db() as db:
                        after = dict(db.execute('SELECT * FROM mailbox_outbox WHERE token=?', (token,)).fetchone())
                    self.assertEqual(after, saved)
                    self.assertEqual({path.name: path.read_bytes() for path in
                                      fixture.service.outbox.storage_dir.glob('*.eml')}, files_before)
                finally:
                    fixture.doCleanups()


if __name__ == '__main__':
    unittest.main(verbosity=2)
