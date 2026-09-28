"""Quotation workflow using temporary data and fake SMTP/IMAP exclusively."""
import hashlib
import json
import smtplib
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from email import policy
from email.parser import BytesParser
from flask import session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_bestellungen import FakePortal
from test_mailbox_outbox import FakeMailbox, FakeSMTP
from werkstatt_bestellungen import register_orders
from werkstatt_assistent_angebote import OfferService, _hash


class OfferTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.p = FakePortal(Path(self.temp.name))
        self.p.workshop_orders = register_orders(self.p)
        self.p.workshop_orders.configure_operations(True, 25000)
        self.p.app.config['MAILBOX_SEND_ENABLED'] = True
        self.p.get_app_setting = self.setting
        self.p.werkstatt_datei_sichtbar = lambda row: True
        self.smtp, self.mailbox = FakeSMTP(), FakeMailbox()
        self.p.workshop_orders.dispatch.outbox.service = self.mailbox
        for target in ('SMTP', 'SMTP_SSL'):
            patcher = patch('mailbox_outbox.smtplib.' + target, return_value=self.smtp)
            patcher.start(); self.addCleanup(patcher.stop)
        self.images = {1: b'synthetic image', 2: b'other synthetic image'}
        self.service = OfferService(self.p, self.attachment)
        with self.p.workshop_orders.db() as db:
            db.executescript('''
                CREATE TABLE auftraege(id INTEGER PRIMARY KEY,fahrzeug TEXT,kunde_email TEXT,fin_nummer TEXT,
                  archiviert INTEGER,geaendert_am TEXT,werkstatt_angebot_preis TEXT,angebot_status TEXT,notiz_intern TEXT);
                INSERT INTO auftraege VALUES(1,'Audi Test','customer@example.test','VIN-TEST',0,'before','500 netto','angefragt','');
                INSERT INTO auftraege VALUES(2,'Other','','',0,'before','','entwurf','');
                ALTER TABLE auftraege ADD COLUMN hsn_nummer TEXT;
                ALTER TABLE auftraege ADD COLUMN tsn_nummer TEXT;
                CREATE TABLE assistent_aktionen(id TEXT PRIMARY KEY,actor TEXT,auftrag_id INTEGER,art TEXT,payload TEXT,status TEXT);
                CREATE TABLE assistent_audit(id INTEGER PRIMARY KEY AUTOINCREMENT,actor TEXT,auftrag_id INTEGER,aktion TEXT,details TEXT,zeit TEXT);
                CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,aktiv INTEGER);
                CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,lesen INTEGER,einkaufen INTEGER,version INTEGER,limit_cent INTEGER);
                INSERT INTO mitarbeiter VALUES(7,1);INSERT INTO assistent_rechte VALUES(7,1,1,2,0);
                CREATE TABLE werkstatt_emails(id INTEGER PRIMARY KEY,auftrag_id INTEGER,zuordnung_manuell INTEGER,absender_email TEXT,betreff TEXT,nachricht TEXT,empfangen_am TEXT,kategorie TEXT);
                CREATE TABLE dateien(id INTEGER PRIMARY KEY,auftrag_id INTEGER,original_name TEXT,dokument_typ TEXT,kategorie TEXT,extrahierter_text TEXT,hochgeladen_am TEXT);
            ''');db.commit()
        self.supplier = self.p.workshop_orders.propose_contact('K-Parts', 'quotes@supplier.example', 'Confirmed synthetic contact')
        self.p.workshop_orders.verify_contact(self.supplier, 1)

    def setting(self, key, default=''):
        with self.p.workshop_orders.db() as db:
            row = db.execute('SELECT value FROM app_settings WHERE key=?', (key,)).fetchone()
        return row['value'] if row else default

    def attachment(self, actor, order, did, kind):
        if order != 1 or did not in self.images or kind not in {'kunde', 'lieferant'}:
            raise ValueError('Wrong attachment owner/order')
        content = self.images[did]
        return {'datei_id': did, 'content': content, 'original_name': 'damage.jpg', 'mime_type': 'image/jpeg',
                'size': len(content), 'sha256': hashlib.sha256(content).hexdigest()}

    def call(self, fn, *args, actor='admin', csrf=True, method='POST', **kwargs):
        headers = {'X-CSRF-Token': 'test-csrf'} if csrf else {}
        with self.p.app.test_request_context('/synthetic', method=method, headers=headers):
            session['csrf_token'] = 'test-csrf'
            if actor == 'admin': session['admin'] = True
            else: session.update(assistent_mid=7, assistent_version=2)
            return fn(actor, *args, **kwargs)

    def action(self, kind='lieferantenanfrage', *, action_id='quote-1', actor='admin', approved=True, **changes):
        payload = self.call(self.service.prepare, kind, changes.pop('order_id', 1), 'Bitte Stoßfänger anbieten.',
                            supplier_id=self.supplier, actor=actor, **changes)
        with self.p.workshop_orders.db() as db:
            db.execute('INSERT INTO assistent_aktionen VALUES(?,?,?,?,?,?)',
                       (action_id, actor, payload['order_id'], kind, json.dumps(payload), 'intern_freigegeben' if approved else 'vorschlag'))
            db.commit()
        return payload

    def test_supplier_quote_is_nonbinding_and_durable_idempotent(self):
        payload = self.action(attachment_ids=[1])
        self.assertEqual(self.smtp.data_calls, 0)
        self.assertEqual(payload['recipient'], 'quotes@supplier.example')
        self.assertIn('keine Bestellung', payload['body'])
        first = self.call(self.service.submit_approved_action, 'quote-1')
        self.assertEqual(first['state'], 'sent')
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'sent')
        self.assertEqual(self.smtp.data_calls, 1)
        message = BytesParser(policy=policy.default).parsebytes(self.smtp.raw[0])
        self.assertEqual(len(list(message.iter_attachments())), 1)
        self.assertNotIn('customer@example', message.get_body().get_content())
        with self.p.workshop_orders.db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_audit').fetchone()[0], 1)

    def test_customer_gross_price_is_explicit_not_limited_as_purchase_and_never_overwrites_net(self):
        self.action('kundenangebot', actor='mitarbeiter:7', gross_total_cents=120000)
        result = self.call(self.service.submit_approved_action, 'quote-1', actor='mitarbeiter:7')
        self.assertEqual(result['state'], 'sent', 'zero purchasing limit does not forbid a sales quote')
        message = BytesParser(policy=policy.default).parsebytes(self.smtp.raw[0])
        self.assertEqual(message['To'], 'customer@example.test')
        self.assertIn('1200,00 EUR brutto', message.get_content())
        with self.p.workshop_orders.db() as db:
            order = db.execute('SELECT * FROM auftraege WHERE id=1').fetchone()
        self.assertEqual(order['werkstatt_angebot_preis'], '500 netto')
        self.assertEqual(order['angebot_status'], 'angefragt')
        self.assertIn('1200.00 EUR brutto', order['notiz_intern'])
        self.assertIn('Kundenannahme offen', order['notiz_intern'])

    def test_missing_recipient_or_customer_total_stays_reviewable_draft(self):
        draft = self.action('kundenangebot', order_id=2)
        self.assertCountEqual(draft['missing_fields'], ['recipient', 'gross_total_cents'])
        self.assertEqual(draft['recipient'], '')
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'blocked')
        self.assertEqual(self.smtp.data_calls, 0)

    def test_unapproved_foreign_csrf_revoked_rights_cannot_send(self):
        self.action(approved=False)
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'blocked')
        for kwargs in ({'csrf': False}, {'method': 'GET'}, {'actor': 'mitarbeiter:7'}):
            with self.assertRaises(PermissionError): self.call(self.service.submit_approved_action, 'quote-1', **kwargs)
        with self.p.workshop_orders.db() as db:
            db.execute('UPDATE assistent_rechte SET einkaufen=0');db.commit()
        with self.assertRaises(PermissionError):
            self.call(self.service.source_offers, 1, actor='mitarbeiter:7', method='GET')
        self.assertEqual(self.smtp.data_calls, 0)

    def test_changed_recipient_or_attachment_requires_fresh_review(self):
        self.action(attachment_ids=[1])
        self.images[1] = b'changed after review'
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'blocked')
        self.action('kundenangebot', action_id='customer', gross_total_cents=10000)
        with self.p.workshop_orders.db() as db:
            db.execute("UPDATE auftraege SET kunde_email='new@example.test' WHERE id=1");db.commit()
        self.assertEqual(self.call(self.service.submit_approved_action, 'customer')['state'], 'blocked')
        self.assertEqual(self.smtp.data_calls, 0)

    def test_vehicle_papers_foreign_uploads_and_bank_text_are_rejected(self):
        for changes in ({'order_id': 2, 'attachment_ids': [1]}, {'attachment_ids': [999]}):
            with self.assertRaises(ValueError): self.action(**changes)
        original = self.service.attachment_resolver
        def paper(*args):
            value = original(*args);value['original_name'] = 'Fahrzeugschein.jpg';return value
        self.service.attachment_resolver = paper
        with self.assertRaises(ValueError): self.action(attachment_ids=[1])
        with self.assertRaises(ValueError):
            self.call(self.service.prepare, 'kundenangebot', 1, 'IBAN DE12345678901234567890', gross_total_cents=100)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_uncertain_delivery_never_retries_smtp(self):
        self.action();self.smtp.data_error = OSError('synthetic lost DATA reply')
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'uncertain')
        self.smtp.data_error = None
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'uncertain')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_known_rejection_replay_does_not_retry_delivery(self):
        self.action()
        self.smtp.data_error = smtplib.SMTPDataError(554, b'synthetic rejection')
        first = self.call(self.service.submit_approved_action, 'quote-1')
        self.assertEqual(first['state'], 'not_sent')
        self.assertTrue(first['needs_review'])
        self.smtp.data_error = None
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'not_sent')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_concurrent_stale_confirmations_cannot_retry_known_rejection(self):
        self.action()
        self.smtp.data_error = smtplib.SMTPDataError(554, b'synthetic rejection')
        send = self.service.outbox.send
        arrived, finished = threading.Barrier(2), threading.Event()
        counter, counter_lock = [0], threading.Lock()
        def overlapping_send(*args, **kwargs):
            with counter_lock:
                index = counter[0]
                counter[0] += 1
            # Both confirmations have observed no outbox history. The second
            # reaches the durable lock only after the first rejection exists.
            arrived.wait(5)
            if index:
                if not finished.wait(5):
                    raise AssertionError('First synthetic send did not finish')
                return send(*args, **kwargs)
            try:
                return send(*args, **kwargs)
            finally:
                finished.set()
        with patch.object(self.service.outbox, 'send', side_effect=overlapping_send):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(self.call, self.service.submit_approved_action, 'quote-1') for _ in range(2)]
                self.assertEqual([f.result(timeout=10)['state'] for f in futures], ['not_sent', 'not_sent'])
        self.assertEqual(self.smtp.data_calls, 1)

    def test_changed_order_snapshot_requires_new_review_even_without_timestamp_update(self):
        for field, value in (('fahrzeug', 'Changed vehicle'), ('fin_nummer', 'CHANGED-VIN'),
                             ('hsn_nummer', '1234'), ('tsn_nummer', 'ABC'), ('geaendert_am', 'later')):
            with self.subTest(field=field):
                action_id = 'drift-' + field
                self.action(action_id=action_id)
                with self.p.workshop_orders.db() as db:
                    db.execute('UPDATE auftraege SET ' + field + '=? WHERE id=1', (value,))
                    db.commit()
                result = self.call(self.service.submit_approved_action, action_id)
                self.assertEqual(result['state'], 'blocked')
                self.assertIn('order', result['missing_fields'])
        self.assertEqual(self.smtp.data_calls, 0)

    def test_sent_replay_survives_order_change_and_archiving(self):
        self.action()
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'sent')
        with self.p.workshop_orders.db() as db:
            db.execute("UPDATE auftraege SET fahrzeug='Later vehicle',archiviert=1,geaendert_am='later' WHERE id=1")
            db.commit()
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'sent')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_audit_failure_rolls_back_borrowed_connection_without_resending(self):
        self.action()
        connection = self.p.get_db()
        self.addCleanup(connection.close)
        class BorrowedConnection:
            def execute(self, sql, params=()):
                if sql.startswith('INSERT INTO assistent_audit'):
                    raise RuntimeError('Synthetic audit failure')
                return connection.execute(sql, params)
            def commit(self): connection.commit()
            def rollback(self): connection.rollback()
            def close(self): pass
        with patch.object(self.p, 'get_db', return_value=BorrowedConnection()):
            result = self.call(self.service.submit_approved_action, 'quote-1')
        self.assertEqual(result['state'], 'sent')
        self.assertTrue(result['audit_pending'])
        self.assertFalse(connection.in_transaction, 'request-shared connection must remain usable')
        replay = self.call(self.service.submit_approved_action, 'quote-1')
        self.assertEqual(replay['state'], 'sent')
        self.assertNotIn('audit_pending', replay)
        self.assertEqual(self.smtp.data_calls, 1)

    def test_unhashable_and_noninteger_attachment_ids_are_validation_errors(self):
        for attachment_ids in ([{}], [[]], [True], ['1'], [0], [1, 1]):
            with self.subTest(attachment_ids=attachment_ids):
                with self.assertRaises(ValueError):
                    self.call(self.service.prepare, 'lieferantenanfrage', 1, 'Synthetic scope',
                              supplier_id=self.supplier, attachment_ids=attachment_ids)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_mutated_stored_payload_and_altered_already_sent_content_rejected(self):
        payload = self.action();self.call(self.service.submit_approved_action, 'quote-1')
        payload['mail']['body'] = 'Changed body'
        with self.p.workshop_orders.db() as db:
            db.execute('UPDATE assistent_aktionen SET payload=?', (json.dumps(payload),));db.commit()
        with self.assertRaises(ValueError): self.call(self.service.submit_approved_action, 'quote-1')
        payload.pop('review_hash');payload['review_hash'] = _hash(payload)
        with self.p.workshop_orders.db() as db:
            db.execute('UPDATE assistent_aktionen SET payload=?', (json.dumps(payload),));db.commit()
        self.assertEqual(self.call(self.service.submit_approved_action, 'quote-1')['state'], 'blocked')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_sources_require_existing_manual_order_link_and_remove_bank_lines(self):
        with self.p.workshop_orders.db() as db:
            for identifier, order, manual, title in ((1,1,1,'Teileangebot'), (2,1,0,'Teileangebot'), (3,2,1,'Teileangebot'), (4,1,1,'Bankangebot')):
                db.execute('INSERT INTO werkstatt_emails VALUES(?,?,?,?,?,?,?,?)',
                           (identifier,order,manual,'quotes@supplier.example',title,'Artikel A 100 EUR\nIBAN DE12345678901234567890','today','angebot'))
            db.execute("INSERT INTO dateien VALUES(1,1,'Angebot.pdf','angebot','teileangebot','Position A 80 EUR\nBIC EXAMPLE','today')")
            db.commit()
        result = self.call(self.service.source_offers, 1, method='GET')
        self.assertEqual([(s['type'],s['id']) for s in result['sources']], [('email',1),('document',1)])
        self.assertNotIn('IBAN', json.dumps(result));self.assertNotIn('BIC', json.dumps(result))
        self.assertEqual(self.smtp.data_calls, 0)


if __name__ == '__main__': unittest.main()
