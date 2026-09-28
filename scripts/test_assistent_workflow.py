"""HTTP integration: synthetic orders/uploads, genuine services, fake SMTP/IMAP."""
import io
import json
import tempfile
import unittest
from email import policy
from email.parser import BytesParser
from pathlib import Path
from unittest.mock import Mock, patch

import test_assistent as fixture
from PIL import Image
from mailbox_outbox import MailOutbox
from test_mailbox_outbox import FakeMailbox, FakeSMTP

p = fixture.p
database = fixture.database
BASE = '/werkstatt/assistent'


class AssistantWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.legacy = fixture.AssistantTests(methodName='runTest')
        self.legacy.setUp()
        self.addCleanup(self.legacy.tearDown)
        self.client = self.legacy.client
        self.enterContext(patch.dict(p.app.config, ASSISTANT_READ_ONLY=True, ASSISTANT_NATIVE_COCKPIT=True))
        self.enterContext(patch('requests.sessions.Session.request', side_effect=AssertionError('Network forbidden')))
        self.smtp, self.mailbox = FakeSMTP(), FakeMailbox()
        for target in ('SMTP', 'SMTP_SSL'):
            self.enterContext(patch('mailbox_outbox.smtplib.' + target, return_value=self.smtp))
        self.enterContext(patch('imaplib.IMAP4_SSL', side_effect=AssertionError('Real IMAP forbidden')))
        self.enterContext(patch('imaplib.IMAP4', side_effect=AssertionError('Real IMAP forbidden')))
        self.enterContext(patch.object(p, 'get_werkstatt_smtp_config', return_value={
            'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
            'smtp_host': 'smtp.example.test', 'smtp_port': 465,
            'smtp_user': 'sender@example.test', '_smtp_password': 'synthetic',
            'from_address': 'sender@example.test'}))
        self.enterContext(patch.object(p.workshop_orders, 'availability', return_value={'can_send': True}))
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.outbox = MailOutbox(p.get_db, Path(self.temporary.name), self.mailbox)
        self.outbox._ensure_schema()
        self.enterContext(patch.object(p.assistant_offers, 'outbox', self.outbox))
        self.ocr = self.enterContext(patch.object(p, 'build_document_analysis_bundle_safe', return_value={
            'text': '', 'structured': {'fahrzeug': 'Audi A4', 'kennzeichen': 'NEU-100',
                                     'fin_nummer': 'WAUZZZ8K9AA000001', 'farbcode': 'LY9B'}, 'status': 'ai_ready'}))
        with database() as db:
            for table in ('assistent_uploads', 'datei_backups', 'assistent_fortschritt_audit', 'status_log',
                          'assistent_bestellkontakte', 'mailbox_outbox'):
                db.execute('DELETE FROM ' + table)
            db.execute("UPDATE auftraege SET status=3,kennzeichen='TEST-' || id,farbcode='',farbton='',farbton_2='',kunde_name='Testkunde',kunde_email='customer@example.test',kontakt_telefon='',geaendert_am='before',werkstatt_angebot_preis='500 netto',angebot_status='angefragt' WHERE id IN (156,157)")
            db.execute("INSERT INTO assistent_bestellkontakte(id,name,recipient,source_note,verified_at,verified_by) VALUES('synthetic-supplier','Testlieferant','quote@supplier.example','Synthetisch bestätigt','2026-09-28','admin')")
        p.set_app_setting('ASSISTANT_OPERATIONS_ENABLED', '1')
        p.workshop_orders.set_setting('max_total_cents', 25000)
        image = io.BytesIO()
        Image.new('RGB', (40, 30), 'blue').save(image, format='PNG')
        self.image = image.getvalue()

    def post(self, route, data=None, client=None):
        return self.legacy.post(route, data, client)

    def rows(self, sql, args=()):
        with database() as db:
            return [dict(row) for row in db.execute(sql, args).fetchall()]

    def order(self, order_id=156):
        return self.rows('SELECT * FROM auftraege WHERE id=?', (order_id,))[0]

    def action(self, action_id):
        return self.rows('SELECT * FROM assistent_aktionen WHERE id=?', (action_id,))[0]

    def upload(self, purpose='schaden', client=None, csrf=True, key='synthetic-upload-123456789', analyze=True):
        client = client or self.client
        response = client.post(BASE + '/unterlagen', data={
            'file': (io.BytesIO(self.image), 'synthetic.png', 'image/png'),
            'purpose': purpose, 'request_id': key}, headers={'X-CSRF-Token': 'test-csrf'} if csrf else {})
        if response.status_code == 200 and analyze:
            return self.post('/unterlagen/' + response.json['id'] + '/analyse', client=client)
        return response

    def proposal(self, kind, **args):
        response = self.post('/vorschlag', dict(art=kind, **args))
        self.assertEqual(response.status_code, 200, response.text)
        return response.json

    def confirm(self, action, client=None):
        return self.post('/bestaetigen/' + action['id'], client=client)

    def create_fields(self, **changes):
        return dict(kunde_name='Synthetic Client', fahrzeug='Audi A4', kennzeichen='NEU-100',
                    fin_nummer='WAUZZZ8K9AA000001', kontakt_telefon='', **changes)

    def test_upload_routes_enforce_identity_csrf_current_rights_and_operations(self):
        anonymous = p.app.test_client()
        self.assertEqual(anonymous.get(BASE + '/unterlagen').status_code, 401)
        self.assertIn(self.upload(client=anonymous).status_code, (400, 401))
        self.assertEqual(self.upload(csrf=False).status_code, 400)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET dokumentieren=0 WHERE mitarbeiter_id=1')
        self.assertIn(self.upload().status_code, (400, 403))
        with database() as db:
            db.execute('UPDATE assistent_rechte SET dokumentieren=1,version=2 WHERE mitarbeiter_id=1')
        self.assertEqual(self.upload().status_code, 401)
        admin = self.legacy.make_client(admin=True)
        p.set_app_setting('ASSISTANT_OPERATIONS_ENABLED', '0')
        self.assertEqual(self.upload(client=admin).status_code, 403)
        self.assertEqual(self.rows('SELECT id FROM assistent_uploads'), [])
        self.assertEqual(self.post('/foto/156', client=admin).status_code, 403)

    def test_own_preview_is_private_and_foreign_staging_ids_never_work(self):
        uploaded = self.upload()
        self.assertEqual(uploaded.status_code, 200, uploaded.text)
        identifier = uploaded.json['id']
        self.assertEqual(self.client.get(BASE + '/unterlagen').json[0]['id'], identifier)
        response = self.client.get(BASE + '/unterlagen/' + identifier + '/original')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, self.image)
        self.assertIn('no-store', response.headers.get('Cache-Control', ''))
        self.assertEqual(response.headers['X-Content-Type-Options'], 'nosniff')
        foreign = self.legacy.make_client(admin=True)
        for suffix in ('/original',):
            self.assertIn(foreign.get(BASE + '/unterlagen/' + identifier + suffix).status_code, (400, 404))
        self.assertIn(self.post('/unterlagen/' + identifier + '/analyse', client=foreign).status_code, (400, 404))
        self.assertEqual(foreign.get(BASE + '/unterlagen').json, [])
        denied = self.post('/vorschlag', {'art': 'datei', 'auftrag_id': 156, 'upload_id': identifier}, foreign)
        self.assertIn(denied.status_code, (400, 404))

    def test_color_preview_is_exact_then_confirm_and_replay_keep_status(self):
        before = self.order()
        item = self.proposal('farbe', auftrag_id=156, felder={'farbcode': 'LY9B', 'farbton': 'Brillantschwarz'})
        self.assertEqual(self.order(), before)
        self.assertIn('LY9B', item['daten']['text'])
        confirmation = self.confirm(item)
        self.assertEqual(confirmation.status_code, 200, confirmation.text)
        self.assertEqual(confirmation.json['auftrag']['id'], 156)
        self.assertEqual(self.order()['farbcode'], 'LY9B')
        self.assertEqual(self.order()['farbton'], 'Brillantschwarz')
        self.assertEqual(self.order()['status'], before['status'])
        replay = self.confirm(item)
        self.assertEqual(replay.status_code, 200)
        self.assertTrue(replay.json['wiederholt'])
        self.assertEqual(len(self.rows("SELECT * FROM assistent_fortschritt_audit WHERE action='farbe'")), 1)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_color_and_contact_cas_reject_newer_state_even_same_timestamp(self):
        for kind, fields, column in (('farbe', {'farbcode': 'LY9B'}, 'farbcode'),
                                     ('kontakt', {'kunde_email': 'new@example.test'}, 'kunde_email')):
            with self.subTest(kind=kind):
                item = self.proposal(kind, auftrag_id=156, felder=fields)
                with database() as db:
                    db.execute('UPDATE auftraege SET ' + column + '=? WHERE id=156', ('changed',))
                response = self.confirm(item)
                self.assertEqual(response.status_code, 400, response.text)
                self.assertEqual(self.order()[column], 'changed')
                self.assertEqual(self.action(item['id'])['status'], 'vorschlag')

    def test_contact_change_requires_own_explicit_confirmation_and_rechecks_rights(self):
        item = self.proposal('kontakt', auftrag_id=156, felder={'kunde_email': 'NEW@EXAMPLE.TEST', 'kontakt_telefon': '+49 123 456789'})
        self.assertEqual(self.order()['kunde_email'], 'customer@example.test')
        self.assertEqual(self.confirm(item, self.legacy.make_client(admin=True)).status_code, 404)
        self.assertEqual(self.client.post(BASE + '/bestaetigen/' + item['id'], json={}).status_code, 400)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET dokumentieren=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.confirm(item).status_code, 403)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET dokumentieren=1 WHERE mitarbeiter_id=1')
        result = self.confirm(item)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(self.order()['kunde_email'], 'new@example.test')
        self.assertEqual(self.order()['kontakt_telefon'], '+49 123 456789')
        self.assertEqual(self.smtp.data_calls, 0)

    def test_new_order_gets_database_id_and_atomic_original_once_after_confirmation(self):
        upload = self.upload(purpose='fahrzeugschein').json
        before = len(self.rows('SELECT id FROM auftraege'))
        item = self.proposal('auftrag_neu', upload_id=upload['id'], felder=self.create_fields())
        self.assertEqual(item['auftrag_id'], 0)
        self.assertEqual(len(self.rows('SELECT id FROM auftraege')), before)
        self.assertEqual(self.rows('SELECT id FROM dateien'), [])
        response = self.confirm(item)
        self.assertEqual(response.status_code, 200, response.text)
        identifier = response.json['auftrag']['id']
        self.assertEqual(self.action(item['id'])['auftrag_id'], identifier)
        self.assertNotIn(identifier, (0, 156, 157))
        order = self.order(identifier)
        self.assertEqual(order['status'], 1)
        self.assertEqual(order['kunden_status_aktiv'], 0)
        self.assertEqual(order['kunden_status_token'], '')
        file = self.rows('SELECT * FROM dateien')[0]
        self.assertEqual(file['auftrag_id'], identifier)
        self.assertEqual(file['kategorie'], 'assistent')
        self.assertEqual((p.UPLOAD_DIR / file['stored_name']).read_bytes(), self.image)
        self.assertEqual(self.confirm(item).json['auftrag']['id'], identifier)
        self.assertEqual(len(self.rows('SELECT id FROM auftraege')), before + 1)
        self.assertEqual(len(self.rows('SELECT id FROM dateien')), 1)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_later_visit_of_archived_vehicle_is_a_new_intake_not_old_confirmation(self):
        first = self.proposal('auftrag_neu', felder=self.create_fields())
        order_id = self.confirm(first).json['auftrag']['id']
        with database() as db:
            db.execute('UPDATE auftraege SET archiviert=1,status=5 WHERE id=?', (order_id,))
        second = self.proposal('auftrag_neu', felder=self.create_fields())
        self.assertNotEqual(second['id'], first['id'])
        self.assertEqual(second['status'], 'vorschlag')
        self.assertEqual(self.proposal('auftrag_neu', felder=self.create_fields())['id'], second['id'])
        new_id = self.confirm(second).json['auftrag']['id']
        self.assertNotEqual(new_id, order_id)
        self.assertEqual(self.confirm(second).json['auftrag']['id'], new_id)

    def test_source_attachment_failure_rolls_back_entire_intake_and_can_retry(self):
        upload = self.upload(purpose='fahrzeugschein').json
        item = self.proposal('auftrag_neu', upload_id=upload['id'], felder=self.create_fields())
        before = len(self.rows('SELECT id FROM auftraege'))
        original_attach = p.assistant_uploads.attach
        def attach_then_fail(*args, **kwargs):
            original_attach(*args, **kwargs)
            raise ValueError('synthetic attach failure')
        with patch.object(p.assistant_uploads, 'attach', side_effect=attach_then_fail):
            self.assertEqual(self.confirm(item).status_code, 400)
        self.assertEqual(len(self.rows('SELECT id FROM auftraege')), before)
        self.assertEqual(self.rows('SELECT id FROM dateien'), [])
        self.assertEqual(self.rows('SELECT id FROM assistent_fortschritt_audit'), [])
        self.assertIsNone(p.assistant_uploads.get('mitarbeiter:1', upload['id'])['datei_id'])
        self.assertEqual(self.action(item['id'])['status'], 'vorschlag')
        self.assertEqual(self.confirm(item).status_code, 200)

    def test_existing_order_file_confirmation_does_not_accept_ocr_fields(self):
        uploaded = self.upload().json
        before = self.order()
        item = self.proposal('datei', auftrag_id=156, upload_id=uploaded['id'])
        self.assertEqual(self.rows('SELECT id FROM dateien'), [])
        self.assertEqual(self.confirm(item).status_code, 200)
        self.assertEqual(self.order(), before)
        self.assertEqual(self.confirm(item).status_code, 200)
        self.assertEqual(len(self.rows('SELECT id FROM dateien')), 1)
        other = self.post('/vorschlag', {'art': 'datei', 'auftrag_id': 157, 'upload_id': uploaded['id']})
        self.assertEqual(other.status_code, 400)

    def test_supplier_mail_is_reviewed_nonbinding_with_exact_selected_attachment_and_no_resend(self):
        uploaded = self.upload().json
        attached = self.proposal('datei', auftrag_id=156, upload_id=uploaded['id'])
        self.assertEqual(self.confirm(attached).status_code, 200)
        file_id = self.rows('SELECT id FROM dateien')[0]['id']
        item = self.proposal('lieferantenanfrage', auftrag_id=156, supplier_id='synthetic-supplier',
                             text='Bitte Stoßfänger vorne rechts anbieten.', attachment_ids=[file_id])
        self.assertEqual(self.smtp.data_calls, 0)
        self.assertEqual(item['daten']['recipient'], 'quote@supplier.example')
        self.assertIn('keine Bestellung', item['daten']['body'])
        self.assertEqual(item['daten']['attachments'][0]['id'], file_id)
        response = self.confirm(item)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json['versandstatus']['state'], 'sent')
        self.assertEqual(self.confirm(item).json['versandstatus']['state'], 'sent')
        self.assertEqual(self.smtp.data_calls, 1)
        mail = BytesParser(policy=policy.default).parsebytes(self.smtp.raw[0])
        self.assertEqual(mail['To'], 'quote@supplier.example')
        self.assertEqual(len(list(mail.iter_attachments())), 1)
        self.assertEqual(list(mail.iter_attachments())[0].get_payload(decode=True), self.image)
        self.assertEqual(self.rows('SELECT id FROM assistent_bestellanforderungen'), [])

    def test_uploaded_quote_is_readable_after_assignment_with_source_and_without_bank_footers(self):
        self.ocr.return_value = {'text': 'Angebot QT-100\nStoßfänger 123,00 EUR netto\nIBAN DE89370400440532013000', 'structured': {}}
        upload = self.upload(purpose='angebot').json
        self.assertIn('123,00 EUR', upload['analyse']['angebotsinhalt'])
        before = self.client.get(BASE + '/angebote/156')
        self.assertEqual(before.status_code, 200, before.text)
        self.assertNotIn('QT-100', before.text)
        action = self.proposal('datei', auftrag_id=156, upload_id=upload['id'])
        self.assertEqual(self.confirm(action).status_code, 200)
        response = self.client.get(BASE + '/angebote/156')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn('123,00 EUR', response.text)
        self.assertIn('QT-100', response.text)
        self.assertNotIn('DE893704', response.text)
        self.assertIn('source', response.text)
        self.assertIn('pruefen', response.text)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_missing_mail_configuration_returns_to_proposal_and_requires_fresh_confirmation(self):
        item = self.proposal('lieferantenanfrage', auftrag_id=156, supplier_id='synthetic-supplier', text='Bitte Preis nennen.')
        with patch.object(p.workshop_orders, 'availability', return_value={'can_send': False}):
            response = self.confirm(item)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json['versandstatus']['state'], 'blocked')
        self.assertEqual(self.action(item['id'])['status'], 'vorschlag')
        self.assertEqual(self.rows('SELECT id FROM mailbox_outbox'), [])
        self.client.get(BASE + '/aktionen')
        self.assertEqual(self.smtp.data_calls, 0)
        self.assertEqual(self.confirm(item).json['versandstatus']['state'], 'sent')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_customer_quote_uses_separate_sales_price_over_purchase_cap_without_acceptance(self):
        with database() as db:
            db.execute('UPDATE assistent_rechte SET limit_cent=0 WHERE mitarbeiter_id=1')
        item = self.proposal('kundenangebot', auftrag_id=156, text='Lackierung Stoßfänger vorne rechts.',
                             gesamt_brutto='1200.00', attachment_ids=[])
        self.assertEqual(item['daten']['gross_total_cents'], 120000)
        self.assertEqual(self.smtp.data_calls, 0)
        response = self.confirm(item)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json['versandstatus']['state'], 'sent')
        mail = BytesParser(policy=policy.default).parsebytes(self.smtp.raw[0])
        self.assertEqual(mail['To'], 'customer@example.test')
        self.assertIn('1200,00 EUR brutto', mail.get_content())
        self.assertEqual(self.order()['werkstatt_angebot_preis'], '500 netto')
        self.assertEqual(self.order()['angebot_status'], 'angefragt')

    def test_mail_missing_recipient_or_price_never_sends_and_revoked_rights_deny_confirm(self):
        with database() as db:
            db.execute("UPDATE auftraege SET kunde_email='' WHERE id=156")
        item = self.proposal('kundenangebot', auftrag_id=156, text='Lackierung', attachment_ids=[])
        self.assertIn('recipient', item['daten']['missing_fields'])
        self.assertIn('gross_total_cents', item['daten']['missing_fields'])
        self.assertEqual(self.confirm(item).status_code, 400)
        good = self.proposal('lieferantenanfrage', auftrag_id=156, supplier_id='synthetic-supplier', text='Bitte Preis nennen.')
        with database() as db:
            db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.confirm(good).status_code, 403)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_spoken_workflow_confirmation_requires_exact_actor_bound_challenge(self):
        item = self.proposal('farbe', auftrag_id=156, felder={'farbcode': 'LY9B'})
        response = self.post('/vorlesen/' + item['id'])
        self.assertEqual(response.status_code, 200, response.text)
        challenge = response.json
        self.assertIn('LY9B', challenge['text'])
        self.assertEqual(self.post('/sprache-bestaetigen', {'nonce': challenge['nonce'], 'text': 'Ja'}).status_code, 400)
        foreign = self.legacy.make_client(admin=True)
        self.assertEqual(self.post('/sprache-bestaetigen', {'nonce': challenge['nonce'], 'text': challenge['phrase']}, foreign).status_code, 400)
        self.assertEqual(self.order()['farbcode'], '')
        confirmed = self.post('/sprache-bestaetigen', {'nonce': challenge['nonce'], 'text': challenge['phrase']})
        self.assertEqual(confirmed.status_code, 200, confirmed.text)
        self.assertEqual(self.order()['farbcode'], 'LY9B')

    def test_realtime_model_proposes_color_and_file_without_confirmation_tool(self):
        from werkstatt_assistent_workflow import TOOLS
        color_tool = next(tool for tool in TOOLS if tool['name'] == 'farbton_vorschlagen')
        self.assertFalse(color_tool['strict'], 'Partial edits must not be normalized into required fields by Responses')
        self.assertNotIn('required', color_tool['parameters']['properties']['felder'])
        before = self.order()
        response = self.post('/realtime/werkzeug', {'name': 'farbton_vorschlagen', 'arguments': {'auftrag_id': 156, 'felder': {'farbcode': 'LY9B'}}})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json['event']['data']['status'], 'vorschlag')
        self.assertEqual(self.order(), before)
        config_call = Mock(text='v=0\r\nanswer')
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-test'), patch('werkstatt_assistent.requests.post', return_value=config_call) as call:
            session = self.post('/realtime/start', {'sdp': 'v=0\r\noffer'})
            self.assertEqual(session.status_code, 200, session.text)
            tools = {tool['name'] for tool in json.loads(call.call_args.kwargs['files']['session'][1])['tools']}
        self.assertTrue({'farbton_vorschlagen', 'kontakt_vorschlagen', 'auftrag_vorschlagen', 'unterlagen_lesen', 'angebotsanfrage_vorschlagen', 'kundenangebot_vorschlagen'} <= tools)
        self.assertFalse({'bestaetigen', 'senden', 'auftrag_anlegen', 'mail_senden'} & tools)
        self.assertEqual(self.smtp.data_calls, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
