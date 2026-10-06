"""Real intake/dialog/order/outbox chain with synthetic catalog, Meta, SMTP, IMAP."""
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_materialdialog as dialog_tests
from test_mailbox_outbox import FakeSMTP, FakeMailbox
from werkstatt_bestellungen import OrderManagement
from werkstatt_bestellplan import BERLIN


class MaterialPurchaseEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.f = dialog_tests.DialogTests('runTest')
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.p = self.f.p
        self.f.f.time = datetime(2026, 10, 9, 10, tzinfo=timezone.utc).timestamp()
        self.f.f.message['timestamp'] = str(int(self.f.f.time))
        clock = patch('werkstatt_bestellungen._now', side_effect=lambda: datetime.fromtimestamp(self.f.f.time, timezone.utc))
        clock.start()
        self.addCleanup(clock.stop)
        self.f.f.sql('DROP TABLE assistent_bestellanforderungen')
        self.f.f.sql('CREATE TABLE IF NOT EXISTS app_settings(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT)')
        self.p.get_werkstatt_smtp_config = lambda: dict(smtp_configured=True, smtp_ssl=True, smtp_tls=False,
            smtp_host='smtp.example.test', smtp_port=465, smtp_user='sender@example.test',
            _smtp_password='synthetic', from_address='sender@example.test')
        self.p.get_werkstatt_imap_config = lambda: dict(configured=True, ssl=True, user='sender@example.test')
        self.p.app.config.update(TESTING=True, MAILBOX_OUTBOX_DIR=str(Path(self.f.f.tmp.name) / 'outbox'),
            MAILBOX_SEND_ENABLED=True, ASSISTANT_ORDER_SEND_ENABLED=True, ASSISTANT_ORDER_WORKER_ENABLED=True)
        self.manager = OrderManagement(self.p)
        self.p.workshop_orders = self.manager
        self.manager.configure_operations(True, 25000)
        self.manager.set_setting('worker_last_ok', self.f.f.time)
        self.contact = self.manager.propose_contact('Testlieferant', 'orders@supplier-a.example', 'Synthetic verified source')
        self.manager.verify_contact(self.contact, 1)
        self.smtp = FakeSMTP()
        self.manager.dispatch.outbox.service = FakeMailbox()
        for name in ('SMTP', 'SMTP_SSL'):
            mock = patch('mailbox_outbox.smtplib.' + name, return_value=self.smtp)
            mock.start()
            self.addCleanup(mock.stop)

    def demand(self, caption, *, supplier=None, price=1000, shipping=0, extras=0):
        self.f.f.message['timestamp'] = str(int(self.f.f.time))
        view = self.f.photo(caption, new=True)
        return self.f.review(view, supplier_id=supplier or self.contact,
                             unit_price_cents=price, shipping_cents=shipping, extra_costs_cents=extras)

    def test_photo_to_real_urgent_outbox_exactly_once_and_same_key_on_replay(self):
        view = self.demand('Klebeband grün 50 mm, ein Karton, dringend', shipping=300, extras=200)
        result = self.f.s.process_next()
        self.assertEqual(result['state'], 'sent')
        self.assertEqual(self.smtp.data_calls, 1)
        row = self.f.s.status(view['id'])
        order = self.manager.dispatch.status(row['dispatch_id'])
        self.assertEqual(order['order']['max_total_cents'], 1500)
        self.assertEqual(order['order']['id'], 'material:' + str(view['id']))
        mail = BytesParser(policy=policy.default).parsebytes(self.smtp.raw[0])
        self.assertEqual(str(mail['To']), 'orders@supplier-a.example')
        self.assertIn('50 mm', mail.get_body(preferencelist=('plain',)).get_content())
        self.manager.submit_material_request(view['id'], row['revision'])
        self.manager.tick()
        self.f.s.process_next()
        self.assertEqual(self.smtp.data_calls, 1)

    def test_weekly_flow_waits_until_14_and_keeps_supplier_mails_separate(self):
        a = self.demand('Bitte ein Karton bestellen, nicht dringend')
        self.assertEqual(self.f.s.process_next()['state'], 'queued')
        b = self.demand('Bitte zwei Karton bestellen, nicht dringend', price=1500)
        self.assertEqual(self.f.s.process_next()['state'], 'queued')
        supplier_b = self.manager.propose_contact('Testlieferant B', 'orders@supplier-b.example', 'Synthetic source B')
        self.manager.verify_contact(supplier_b, 1)
        self.f.hits[0]['lieferant'] = 'Testlieferant B'
        self.demand('Bitte ein Karton bestellen, nicht dringend', supplier=supplier_b)
        self.assertEqual(self.f.s.process_next()['state'], 'queued')
        self.assertEqual(self.smtp.data_calls, 0)
        self.f.f.time = datetime(2026, 10, 12, 13, 59, tzinfo=BERLIN).timestamp()
        self.manager.tick(worker=True)
        self.assertEqual(self.smtp.data_calls, 0)
        self.f.f.time = datetime(2026, 10, 12, 14, 0, tzinfo=BERLIN).timestamp()
        self.manager.tick(worker=True)
        self.assertEqual(self.smtp.data_calls, 2)
        mails = [BytesParser(policy=policy.default).parsebytes(raw) for raw in self.smtp.raw]
        self.assertEqual({str(mail['To']) for mail in mails}, {'orders@supplier-a.example', 'orders@supplier-b.example'})
        first = self.manager.dispatch.status(self.f.s.status(a['id'])['dispatch_id'])
        second = self.manager.dispatch.status(self.f.s.status(b['id'])['dispatch_id'])
        self.assertEqual(first['batch_id'], second['batch_id'])
        self.manager.tick(worker=True)
        self.assertEqual(self.smtp.data_calls, 2)

    def test_photo_budget_including_extras_accepts_250_and_blocks_one_cent_more(self):
        self.demand('Ein Karton, dringend', price=24000, shipping=800, extras=200)
        self.assertEqual(self.f.s.process_next()['state'], 'sent')
        over = self.demand('Ein Karton, dringend', price=24000, shipping=800, extras=201)
        self.assertIn('budget', over['missing_fields'])
        self.f.s.process_next()
        self.assertEqual(self.smtp.data_calls, 1)

    def test_revoked_employee_before_monday_prevents_actual_smtp(self):
        self.demand('Bitte ein Karton bestellen, nicht dringend')
        self.assertEqual(self.f.s.process_next()['state'], 'queued')
        self.f.f.sql('UPDATE assistent_rechte SET einkaufen=0,version=version+1 WHERE mitarbeiter_id=1')
        self.f.f.time = datetime(2026, 10, 12, 14, 0, tzinfo=BERLIN).timestamp()
        self.manager.tick(worker=True)
        self.assertEqual(self.smtp.data_calls, 0)


if __name__ == '__main__':
    unittest.main()
