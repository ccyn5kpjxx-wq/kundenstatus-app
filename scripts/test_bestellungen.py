"""Admin UI/worker regressions using temporary DB and fake SMTP/IMAP only."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from flask import Flask, abort, session

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_mailbox_outbox import FakeMailbox, FakeSMTP
from werkstatt_bestellungen import register_orders, run_worker


class FakePortal:
    def __init__(self, root):
        self.path = root / 'test.sqlite'
        self.app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / 'templates'))
        self.app.config.update(SECRET_KEY='offline-test-only', TESTING=True,
                               MAILBOX_OUTBOX_DIR=str(root / 'private-outbox'), MAILBOX_SEND_ENABLED=False,
                               ASSISTANT_ORDER_SEND_ENABLED=False, ASSISTANT_ORDER_WORKER_ENABLED=False)
        self.smtp = {'smtp_configured': True, 'smtp_ssl': True, 'smtp_tls': False,
                     'smtp_host': 'smtp.example.test', 'smtp_port': 465,
                     'smtp_user': 'sender@example.test', '_smtp_password': 'synthetic',
                     'from_address': 'sender@example.test'}
        self.imap = {'configured': True, 'ssl': True, 'user': 'sender@example.test'}

    def get_db(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        return db

    def get_werkstatt_smtp_config(self):
        return deepcopy(self.smtp)

    def get_werkstatt_imap_config(self):
        return deepcopy(self.imap)

    @staticmethod
    def admin_required(func):
        @wraps(func)
        def guarded(*args, **kwargs):
            if not session.get('admin'):
                abort(403)
            return func(*args, **kwargs)
        return guarded


class ManagementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = datetime(2026, 9, 27, 10, tzinfo=timezone.utc)
        clock = patch('werkstatt_bestellungen._now', side_effect=lambda: self.now)
        clock.start()
        self.addCleanup(clock.stop)
        self.portal = FakePortal(Path(self.temp.name))
        self.manager = register_orders(self.portal)
        self.smtp, self.mailbox = FakeSMTP(), FakeMailbox()
        self.manager.dispatch.outbox.service = self.mailbox
        for name in ('SMTP', 'SMTP_SSL'):
            mock = patch('mailbox_outbox.smtplib.' + name, return_value=self.smtp)
            mock.start()
            self.addCleanup(mock.stop)
        self.client = self.portal.app.test_client()
        with self.client.session_transaction() as state:
            state['admin'] = True
            state['csrf_token'] = 'test-csrf'
        self.client.get('/admin/assistent-bestellungen')

    def post(self, path, values=None):
        return self.client.post('/admin/assistent-bestellungen' + path, data={'csrf_token': 'test-csrf', **(values or {})})

    def nonce(self):
        with self.client.session_transaction() as state:
            return list(state['assistant_order_requests'])[-1]

    def ready(self, *, worker=False):
        self.portal.app.config.update(ASSISTANT_ORDER_SEND_ENABLED=True, MAILBOX_SEND_ENABLED=True,
                                      ASSISTANT_ORDER_WORKER_ENABLED=worker)
        self.assertEqual(self.post('/kontakt', {'name': 'Supplier', 'recipient': 'orders@supplier.example',
                                                'source_note': 'Rechnung A'}).status_code, 303)
        contact = self.manager.contacts()[0]
        self.assertEqual(self.post('/kontakt/' + contact['id'] + '/bestaetigen',
                                   {'contact_confirmed': 'ja', 'revision': contact['revision']}).status_code, 303)
        self.assertEqual(self.post('/kostenrahmen', {'max_total': '100,00'}).status_code, 303)
        if worker:
            run_worker(self.manager, once=True)
        return contact['id']

    def order_form(self, supplier_id, **changes):
        form = {'request_id': self.nonce(), 'action': 'bestellen', 'price_confirmed': 'ja',
                'supplier_id': supplier_id, 'product_name': 'Abklebeband', 'article_number': 'AB-50',
                'variant': 'grün 50 mm', 'quantity': '2', 'unit': 'Rolle', 'unit_price': '10,00',
                'shipping': '5,00', 'max_total': '25,00', 'urgency': 'urgent'}
        form.update(changes)
        return form

    def test_registration_is_reentrant_and_defaults_to_no_sends(self):
        self.assertIs(register_orders(self.portal), self.manager)
        response = self.client.get('/admin/assistent-bestellungen')
        self.assertIn('Bestellversand deaktiviert', response.get_data(as_text=True))
        self.assertNotIn('synthetic', response.get_data(as_text=True))
        self.assertEqual(self.smtp.data_calls, 0)
        self.assertEqual(self.mailbox.connect_calls, 0)
        with self.assertRaises(ValueError):
            self.manager.tick()

    def test_admin_auth_and_csrf_apply_to_every_write(self):
        anonymous = self.portal.app.test_client()
        self.assertEqual(anonymous.get('/admin/assistent-bestellungen').status_code, 403)
        self.assertEqual(anonymous.post('/admin/assistent-bestellungen/kontakt').status_code, 403)
        for path in ('/kontakt', '/kontakt/id/bestaetigen', '/kostenrahmen', '/bestellen'):
            with self.subTest(path=path):
                self.assertEqual(self.client.post('/admin/assistent-bestellungen' + path, data={}).status_code, 400)
        self.assertEqual(self.manager.contacts(), [])
        self.assertEqual(self.smtp.data_calls, 0)

    def test_invoice_contacts_are_proposals_and_model_flags_cannot_verify(self):
        self.post('/kontakt', {'name': 'Supplier', 'recipient': 'orders@supplier.example',
                              'source_note': 'Rechnung A', 'verified': 'true', 'verified_at': 'now'})
        contact = self.manager.contacts()[0]
        self.assertFalse(self.manager.resolve_supplier(contact['id'])['verified'])
        response = self.post('/kontakt/' + contact['id'] + '/bestaetigen',
                             {'revision': contact['revision'], 'recipient_verified': 'true'})
        self.assertEqual(response.status_code, 400)
        self.assertFalse(self.manager.resolve_supplier(contact['id'])['verified'])

    def test_contact_edit_invalidates_verification_and_stale_confirmation(self):
        contact_id = self.ready()
        previous = self.manager.contacts()[0]
        self.assertTrue(self.manager.resolve_supplier(contact_id)['verified'])
        self.post('/kontakt', {'contact_id': contact_id, 'name': 'Supplier', 'recipient': 'changed@supplier.example'})
        self.assertFalse(self.manager.resolve_supplier(contact_id)['verified'])
        response = self.post('/kontakt/' + contact_id + '/bestaetigen',
                             {'contact_confirmed': 'ja', 'revision': previous['revision']})
        self.assertEqual(response.status_code, 400)

    def test_missing_limit_price_shipping_or_deliberate_action_prevents_queueing(self):
        contact_id = self.ready()
        for changes in ({'unit_price': ''}, {'shipping': ''}, {'unit_price': '1.234'}, {'quantity': ''},
                        {'variant': ''}, {'article_number': ''}, {'price_confirmed': ''}, {'action': ''},
                        {'request_id': 'forged'}, {'max_total': '101,00'}, {'max_total': '0'}, {'urgency': ''}):
            with self.subTest(changes=changes):
                self.assertEqual(self.post('/bestellen', self.order_form(contact_id, **changes)).status_code, 400)
        self.assertEqual(self.manager.dispatch.list_orders(), [])
        self.assertEqual(self.smtp.data_calls, 0)

    def test_urgent_request_sends_once_with_trusted_recipient_and_actor(self):
        contact_id = self.ready()
        form = self.order_form(contact_id, recipient='attacker@example.test', actor_id='model', price_verified='true')
        self.assertEqual(self.post('/bestellen', form).status_code, 303)
        self.assertEqual(self.post('/bestellen', form).status_code, 303)
        self.assertEqual(self.smtp.data_calls, 1)
        saved = self.manager.dispatch.list_orders()[0]
        self.assertEqual(saved['actor_id'], 'admin')
        self.assertEqual(saved['order']['recipient'], 'orders@supplier.example')
        self.assertEqual(saved['state'], 'sent')

    def test_weekly_requires_live_worker_then_dispatches_at_monday_noon(self):
        contact_id = self.ready()
        form = self.order_form(contact_id, urgency='weekly')
        self.assertEqual(self.post('/bestellen', form).status_code, 400)
        self.portal.app.config['ASSISTANT_ORDER_WORKER_ENABLED'] = True
        run_worker(self.manager, once=True)
        self.assertTrue(self.manager.availability()['worker_live'])
        self.assertEqual(self.post('/bestellen', form).status_code, 303)
        self.assertEqual(self.smtp.data_calls, 0)
        self.now = datetime(2026, 9, 28, 10, tzinfo=timezone.utc)
        run_worker(self.manager, once=True)
        self.assertEqual(self.smtp.data_calls, 1)
        self.assertEqual(self.manager.dispatch.list_orders()[0]['state'], 'sent')

    def test_worker_heartbeat_expires_and_old_queue_remains_durable(self):
        contact_id = self.ready(worker=True)
        self.assertEqual(self.post('/bestellen', self.order_form(contact_id, urgency='weekly')).status_code, 303)
        self.now += timedelta(minutes=4)
        self.assertFalse(self.manager.availability()['worker_live'])
        self.assertEqual(len(self.manager.dispatch.list_orders()), 1)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_worker_command_is_opt_in_and_uses_no_flask_thread(self):
        runner = self.portal.app.test_cli_runner()
        blocked = runner.invoke(args=['werkstatt-bestellungen-worker', '--once'])
        self.assertNotEqual(blocked.exit_code, 0)
        self.assertEqual(self.smtp.data_calls, 0)
        self.ready(worker=True)
        success = runner.invoke(args=['werkstatt-bestellungen-worker', '--once'])
        self.assertEqual(success.exit_code, 0, success.output)
        self.assertTrue(self.manager.availability()['worker_live'])

    def test_changed_or_missing_durable_outbox_path_blocks_sending(self):
        contact_id = self.ready()
        self.portal.app.config.pop('MAILBOX_OUTBOX_DIR')
        self.assertFalse(self.manager.availability()['can_send'])
        self.assertEqual(self.post('/bestellen', self.order_form(contact_id)).status_code, 400)
        self.assertEqual(self.smtp.data_calls, 0)
        self.assertEqual(self.manager.dispatch.list_orders(), [])

    def test_sent_copy_pending_and_uncertain_are_displayed_truthfully(self):
        contact_id = self.ready()
        self.mailbox.client.append_result = ('NO', [])
        self.post('/bestellen', self.order_form(contact_id))
        response = self.client.get('/admin/assistent-bestellungen')
        self.assertIn('Nur die Gesendet-Kopie fehlt', response.get_data(as_text=True))
        self.assertEqual(self.smtp.data_calls, 1)
        self.assertEqual(self.manager.dispatch.list_orders()[0]['state'], 'copy_pending')

    def test_direct_service_calls_do_not_obtain_admin_route_authorization(self):
        self.ready()
        self.assertFalse(self.manager.authorize_order('admin', {'id': 'fake', 'max_total_cents': 1}))
        with self.portal.app.test_request_context('/admin/assistent-bestellungen', method='POST'):
            session['admin'] = True
            self.assertFalse(self.manager.authorize_order('admin', {'id': 'fake', 'max_total_cents': 1}))


if __name__ == '__main__':
    unittest.main()
