"""Admin UI/worker regressions using temporary DB and fake SMTP/IMAP only."""
from copy import deepcopy
from contextlib import nullcontext
import ast
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from functools import wraps
import json
import re
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from flask import Flask, abort, session, render_template
from werkzeug.routing import Rule

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_mailbox_outbox import FakeMailbox, FakeSMTP
from werkstatt_bestellungen import OrderManagement, register_orders, run_worker, start_order_worker


class FakePortal:
    portal_originals_operation_lock = staticmethod(nullcontext)
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
        db = self.get_db()
        try:
            db.execute("CREATE TABLE app_settings(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT)")
            db.commit()
        finally:
            db.close()

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

    def test_mutation_failure_is_not_hidden_by_optional_historical_price_fallback(self):
        with patch.object(self.manager, 'propose_contact', side_effect=RuntimeError('synthetic mutation failure')):
            with self.assertRaisesRegex(RuntimeError, 'synthetic mutation failure'):
                self.post('/kontakt', {'name': 'Supplier', 'recipient': 'orders@supplier.example',
                                       'source_note': 'synthetic invoice'})

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
                'shipping': '5,00', 'extra_costs': '0,00', 'price_source': 'Geprüftes Angebot',
                'max_total': '25,00', 'urgency': 'urgent'}
        form.update(changes)
        return form

    def approved_action(self, contact_id, *, action_id='action-1', actor='admin', status='intern_freigegeben', art='bestellung', **changes):
        payload = {'order_requested': True, 'supplier_id': contact_id, 'recipient': 'orders@supplier.example',
                   'article_number': 'AB-50', 'product_name': 'Abklebeband', 'variant': 'grün 50 mm',
                   'quantity': '2', 'unit': 'Rolle', 'urgent': True, 'max_total_cents': 2500,
                   'unit_price_cents': 1000, 'shipping_cents': 300, 'extra_costs_cents': 200,
                   'price_verified': True, 'price_basis': 'gross', 'currency': 'EUR', 'price_source': 'Geprüftes Lieferantenangebot A'}
        payload.update(changes)
        with self.manager.db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS assistent_aktionen (
                id TEXT PRIMARY KEY, actor TEXT, auftrag_id INTEGER, art TEXT, payload TEXT, status TEXT)''')
            db.execute('INSERT INTO assistent_aktionen VALUES(?,?,?,?,?,?)',
                       (action_id, actor, 0, art, json.dumps({'versand': payload}), status))
            db.commit()
        return action_id

    def submit_action(self, action_id='action-1', actor='admin', *, method='POST', csrf=True, employee=False):
        headers = {'X-CSRF-Token': 'test-csrf'} if csrf else {}
        with self.portal.app.test_request_context('/werkstatt/assistent/bestellen/' + action_id, method=method, headers=headers):
            session['csrf_token'] = 'test-csrf'
            if employee:
                session.update(assistent_mid=7, assistent_version=2)
            else:
                session['admin'] = True
            return self.manager.submit_approved_action(actor, action_id)

    def test_approved_avatar_action_sends_once_and_preserves_price_provenance(self):
        self.approved_action(self.ready())
        first = self.submit_action()
        self.assertEqual(first['state'], 'sent')
        self.assertEqual(first['order']['expected_total_cents'], 2500)
        self.assertEqual(self.smtp.data_calls, 1)
        message = BytesParser(policy=policy.default).parsebytes(self.smtp.raw[0]).get_content()
        self.assertIn('Geprüftes Lieferantenangebot A', message)
        self.assertIn('Freigegebene Nebenkosten: 2,00 EUR', message)
        self.assertIn('einschließlich aller Versand- und Nebenkosten: 25,00 EUR', message)
        self.assertEqual(first['id'], self.submit_action()['id'])
        self.assertEqual(self.smtp.data_calls, 1)
        self.portal.app.config['ASSISTANT_ORDER_SEND_ENABLED'] = False
        self.assertEqual(self.submit_action()['state'], 'sent', 'past delivery remains truthful when transport disabled')

    def test_avatar_bridge_rejects_legacy_unapproved_foreign_or_csrf_less_actions(self):
        contact = self.ready()
        self.approved_action(contact, action_id='legacy', art='einkauf')
        self.approved_action(contact, action_id='draft', status='vorschlag')
        self.approved_action(contact, action_id='foreign', actor='mitarbeiter:7')
        self.approved_action(contact)
        for action in ('legacy', 'foreign'):
            with self.assertRaises(PermissionError):
                self.submit_action(action)
        self.assertEqual(self.submit_action('draft')['state'], 'blocked')
        for options in ({'method': 'GET'}, {'csrf': False}):
            with self.assertRaises(PermissionError):
                self.submit_action(**options)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_avatar_requires_explicit_shipping_extra_price_source_and_fresh_employee_limit(self):
        contact = self.ready()
        for number, changes in enumerate(({'shipping_cents': None}, {'extra_costs_cents': None},
                                          {'price_source': ''}, {'urgent': None}, {'price_verified': False},
                                          {'variant': ''}, {'max_total_cents': 2499})):
            action = self.approved_action(contact, action_id='missing-' + str(number), **changes)
            self.assertEqual(self.submit_action(action)['state'], 'blocked')
        with self.manager.db() as db:
            db.execute('CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY, aktiv INTEGER)')
            db.execute('CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY, lesen INTEGER,einkaufen INTEGER,limit_cent INTEGER,version INTEGER)')
            db.execute('INSERT INTO mitarbeiter VALUES(7,1)')
            db.execute('INSERT INTO assistent_rechte VALUES(7,1,1,0,2)')
            db.commit()
        self.approved_action(contact, actor='mitarbeiter:7')
        self.assertEqual(self.submit_action(actor='mitarbeiter:7', employee=True)['state'], 'blocked')
        with self.manager.db() as db:
            db.execute('UPDATE assistent_rechte SET limit_cent=2500,version=3');db.commit()
        with self.assertRaises(PermissionError):
            self.submit_action(actor='mitarbeiter:7', employee=True)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_weekly_supplier_cap_cannot_be_split_across_requests_or_mail_batches(self):
        contact = self.ready(worker=True)
        self.manager.set_setting('max_total_cents', 25000)
        self.approved_action(contact, urgent=False, quantity='12', max_total_cents=12500)
        self.approved_action(contact, action_id='action-2', urgent=False, quantity='12', max_total_cents=12500)
        self.approved_action(contact, action_id='action-3', urgent=False, quantity='1', max_total_cents=1500)
        first, second = self.submit_action(), self.submit_action('action-2')
        self.assertEqual(first['state'], 'queued');self.assertEqual(second['state'], 'queued')
        blocked = self.submit_action('action-3')
        self.assertEqual(blocked['state'], 'blocked');self.assertIn('Nicht in weitere', blocked['message'])
        self.now = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
        self.manager.tick(worker=True)
        self.assertEqual(self.smtp.data_calls, 1)
        message = BytesParser(policy=policy.default).parsebytes(self.smtp.raw[0]).get_content()
        self.assertIn('einschließlich aller Versand- und Nebenkosten: 250,00 EUR', message)
        self.assertEqual(self.manager.dispatch.status(first['id'])['batch_id'], self.manager.dispatch.status(second['id'])['batch_id'])
        # An additional exact-cutoff request cannot create a second mail to evade the same weekly limit.
        self.assertEqual(self.submit_action('action-3')['state'], 'blocked')
        self.assertEqual(self.smtp.data_calls, 1)

    def test_weekly_budget_reservation_serializes_competing_actors(self):
        contact = self.ready(worker=True)
        self.manager.set_setting('max_total_cents', 25000)
        for number in range(2):
            self.approved_action(contact, action_id='concurrent-' + str(number), urgent=False,
                                 quantity='15', max_total_cents=15500)
        results, failures = [], []
        def submit(number):
            try: results.append(self.submit_action('concurrent-' + str(number)))
            except Exception as exc: failures.append(exc)
        threads = [threading.Thread(target=submit, args=(number,)) for number in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10)
        self.assertFalse(any(thread.is_alive() for thread in threads));self.assertEqual(failures, [])
        self.assertCountEqual([result['state'] for result in results], ['queued', 'blocked'])
        self.assertEqual(len(self.manager.dispatch.list_orders()), 1)

    def test_worker_rechecks_revoked_contacts_and_lowered_batch_budget(self):
        contact = self.ready(worker=True)
        self.approved_action(contact, urgent=False)
        queued = self.submit_action()
        self.manager.propose_contact('Supplier', 'changed@supplier.example', contact_id=contact)
        self.now = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
        self.manager.tick(worker=True)
        self.assertEqual(self.manager.dispatch.status(queued['id'])['state'], 'blocked')
        self.assertIn('Bestellkontakt', self.manager.dispatch.status(queued['id'])['message'])
        self.assertEqual(self.smtp.data_calls, 0)
        self.manager.propose_contact('Supplier', 'orders@supplier.example', contact_id=contact)
        self.manager.verify_contact(contact, self.manager.contacts()[0]['revision'])
        self.manager.set_setting('max_total_cents', 2000)
        self.now += timedelta(minutes=6);self.manager.tick(worker=True)
        self.assertEqual(self.manager.dispatch.status(queued['id'])['state'], 'blocked')
        self.assertIn('Kostenrahmen', self.manager.dispatch.status(queued['id'])['message'])
        self.assertEqual(self.smtp.data_calls, 0)

    def test_new_contact_block_remains_visible_after_a_previous_smtp_rejection(self):
        contact = self.ready()
        self.approved_action(contact)
        self.smtp.data_result = (554, b'synthetic rejection')
        result = self.submit_action()
        self.assertEqual(result['state'], 'not_sent')
        self.manager.propose_contact('Supplier', 'changed@supplier.example', contact_id=contact)
        self.now += timedelta(minutes=6)
        self.manager.tick()
        blocked = self.manager.dispatch.status(result['id'])
        self.assertEqual(blocked['state'], 'blocked')
        self.assertIn('Bestellkontakt', blocked['message'])
        self.assertEqual(self.smtp.data_calls, 1)

    def test_background_worker_retries_failure_without_a_busy_loop(self):
        self.manager.set_setting('worker_enabled', '1')
        self.portal.app.config['TESTING'] = False
        class StopAfterTwo:
            def __init__(self): self.waits = []
            def is_set(self): return len(self.waits) >= 2
            def wait(self, seconds): self.waits.append(seconds)
        stop = StopAfterTwo()
        with patch('werkstatt_bestellungen.threading.Event', return_value=stop), \
             patch('werkstatt_bestellungen.threading.Thread') as thread, \
             patch.object(self.manager, 'tick', side_effect=[ValueError('synthetic failure'), {}]) as tick, \
             patch.object(self.manager.app.logger, 'warning') as warning:
            self.assertTrue(start_order_worker(self.manager))
            thread.call_args.kwargs['target']()
        self.assertEqual(tick.call_count, 2)
        self.assertEqual(stop.waits, [30, 30])
        self.assertEqual(warning.call_count, 1)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_persistent_flags_and_explicit_worker_start_do_not_start_during_registration(self):
        self.manager.set_setting('send_enabled', '1');self.manager.set_setting('worker_enabled', '1')
        self.portal.app.config['MAILBOX_SEND_ENABLED'] = True
        self.assertTrue(self.manager.availability()['can_send'])
        self.assertTrue(self.manager.availability()['worker_enabled'])
        self.assertFalse(start_order_worker(self.manager), 'tests never start a live background sender')
        self.portal.app.config['TESTING'] = False
        with patch('werkstatt_bestellungen.threading.Thread') as thread:
            thread.return_value.is_alive.return_value = True
            self.assertTrue(start_order_worker(self.manager))
            self.assertFalse(start_order_worker(self.manager))
            self.assertEqual(thread.return_value.start.call_count, 1)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_admin_operation_activation_is_persistent_bounded_and_does_not_order(self):
        result = self.post('/betrieb', {'action': 'activate', 'max_total': '250,00'})
        self.assertEqual(result.status_code, 303)
        with self.manager.db() as db:
            flag = db.execute("SELECT value FROM app_settings WHERE key='ASSISTANT_OPERATIONS_ENABLED'").fetchone()
        self.assertEqual(flag['value'], '1')
        self.assertEqual(self.manager.cap(), 25000)
        self.assertEqual(self.manager.setting('send_enabled'), '1')
        self.assertEqual(self.manager.setting('worker_enabled'), '1')
        self.assertFalse(self.manager.availability()['can_send'], 'mailbox transport switch remains independent')
        restarted = OrderManagement(self.portal)
        self.assertEqual(restarted.cap(), 25000)
        self.assertTrue(restarted.availability()['worker_enabled'])
        self.assertIsNone(restarted._worker, 'constructing manager still cannot start a sender')
        self.assertEqual(self.smtp.data_calls, 0)
        self.assertEqual(self.manager.dispatch.list_orders(), [])
        for value in ('250,01', '1000', '0', '-1', 'NaN', ''):
            self.assertEqual(self.post('/betrieb', {'action': 'activate', 'max_total': value}).status_code, 400)
            self.assertEqual(self.post('/kostenrahmen', {'max_total': value}).status_code, 400)
        self.assertEqual(self.manager.cap(), 25000)

    def test_admin_pause_overrides_environment_flags_and_preserves_budget(self):
        self.post('/betrieb', {'action': 'activate', 'max_total': '250,00'})
        self.portal.app.config.update(ASSISTANT_ORDER_SEND_ENABLED=True, ASSISTANT_ORDER_WORKER_ENABLED=True,
                                      MAILBOX_SEND_ENABLED=True)
        self.assertTrue(self.manager.availability()['can_send'])
        self.assertEqual(self.post('/betrieb', {'action': 'deactivate'}).status_code, 303)
        self.assertFalse(self.manager.availability()['can_send'])
        self.assertFalse(self.manager.availability()['worker_enabled'])
        self.assertTrue(self.manager._worker_stop.is_set())
        self.assertEqual(self.manager.cap(), 25000)
        with self.manager.db() as db:
            self.assertEqual(db.execute("SELECT value FROM app_settings WHERE key='ASSISTANT_OPERATIONS_ENABLED'").fetchone()['value'], '0')
        self.assertFalse(OrderManagement(self.portal).availability()['can_send'])

    def test_worker_restarts_after_explicit_pause_and_process_restart(self):
        self.manager.configure_operations(True, 25000)
        self.portal.app.config['TESTING'] = False
        with patch('werkstatt_bestellungen.threading.Thread') as thread:
            thread.return_value.is_alive.return_value = True
            self.assertTrue(start_order_worker(self.manager))
            old_stop = self.manager._worker_stop
            self.manager.configure_operations(False)
            self.assertTrue(old_stop.is_set())
            self.assertFalse(start_order_worker(self.manager))
            self.manager.configure_operations(True, 25000)
            self.assertTrue(start_order_worker(self.manager), 'a winding-down old loop must not suppress the new enabled loop')
            self.assertIsNot(self.manager._worker_stop, old_stop)
            restarted = OrderManagement(self.portal)
            self.assertTrue(start_order_worker(restarted))
            self.assertEqual(thread.return_value.start.call_count, 3)
        self.assertEqual(self.smtp.data_calls, 0)

    def test_real_postgres_adapter_accepts_settings_contacts_and_worker_heartbeat(self):
        names = {'DbRow', 'PostgresCursor', 'PostgresConnection', 'convert_sqlite_sql_to_postgres', 'get_insert_table_name'}
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'app.py').read_text(encoding='utf-8'))
        nodes = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
        namespace = {'re': re}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'app.py (adapter only)', 'exec'), namespace)
        statements, database_path = [], self.portal.path
        rollbacks = []
        with self.manager.db() as db:
            db.execute('ALTER TABLE assistent_bestellanforderungen DROP COLUMN schedule_version')
            db.execute('ALTER TABLE assistent_bestellanforderungen DROP COLUMN legacy_due_at')
            db.execute('ALTER TABLE assistent_bestellpakete DROP COLUMN not_before_at')
            db.commit()
        class Cursor:
            def __init__(self, connection):
                self.owner = connection
                self.cursor = connection.connection.cursor()
            def __enter__(self): return self
            def __exit__(self, *args): self.cursor.close()
            def execute(self, sql, params):
                statements.append(sql)
                if self.owner.failed:
                    raise RuntimeError('PostgreSQL transaction is aborted until rollback')
                try:
                    self.cursor.execute(sql.replace('%s', '?').replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT'), params)
                except sqlite3.DatabaseError:
                    self.owner.failed = True
                    raise
                self.rows = self.cursor.fetchall() if self.cursor.description else []
                self.rowcount = self.cursor.rowcount
                self.description = [type('Column', (), {'name': item[0]}) for item in self.cursor.description] if self.cursor.description else None
            def fetchall(self): return self.rows
        class Connection:
            def __init__(self):
                self.connection = sqlite3.connect(database_path)
                self.failed = False
            def cursor(self): return Cursor(self)
            def commit(self): self.connection.commit()
            def rollback(self):
                rollbacks.append(True)
                self.connection.rollback()
                self.failed = False
            def close(self): self.connection.close()
        with patch.object(self.portal, 'get_db', side_effect=lambda: namespace['PostgresConnection'](Connection())):
            manager = OrderManagement(self.portal)
            manager.configure_operations(True, 25000)
            manager.set_setting('worker_last_ok', self.now.timestamp())
            contact = manager.propose_contact('Supplier PG', 'orders@pg.example', 'Synthetic source')
            manager.verify_contact(contact, 1)
            self.assertEqual(manager.cap(), 25000)
            self.assertTrue(manager.resolve_supplier(contact)['verified'])
            self.assertTrue(manager.availability()['worker_live'])
            manager.configure_operations(False)
        self.assertGreaterEqual(len(rollbacks),3)
        self.assertEqual(len([sql for sql in statements if sql.startswith('ALTER TABLE')]),3)
        settings = [sql for sql in statements if sql.startswith('INSERT INTO assistent_bestellkonfiguration')]
        self.assertTrue(settings)
        self.assertTrue(all(sql.endswith('RETURNING setting_key') for sql in settings))
        self.assertTrue(any(sql.startswith('INSERT INTO app_settings') and sql.endswith('RETURNING key') for sql in statements))
        self.assertTrue(any(sql.startswith('INSERT INTO assistent_bestellkontakte') and sql.endswith('RETURNING id') for sql in statements))
        self.assertEqual(self.smtp.data_calls, 0)

    def test_operation_route_requires_admin_csrf_and_explicit_action(self):
        self.assertEqual(self.portal.app.test_client().post('/admin/assistent-bestellungen/betrieb',
                          data={'action': 'activate', 'max_total': '250'}).status_code, 403)
        self.assertEqual(self.client.post('/admin/assistent-bestellungen/betrieb',
                          data={'action': 'activate', 'max_total': '250'}).status_code, 400)
        self.assertEqual(self.post('/betrieb', {'action': 'unexpected', 'max_total': '250'}).status_code, 400)
        self.assertEqual(self.manager.cap(), 0)

    def test_rights_template_offers_new_access_defaults_without_increasing_existing_limits(self):
        for endpoint, path in (('assistent.page', '/werkstatt/assistent'), ('admin_mitarbeiter', '/admin/mitarbeiter'),
                               ('assistent.vacation_admin', '/admin/assistent-urlaub'),
                               ('arbeitszeit_admin.index', '/admin/arbeitszeit')):
            self.portal.app.url_map.add(Rule(path, endpoint=endpoint))
        employees = [{'id': 1, 'name': 'Neu', 'lesen': None, 'einkaufen': None, 'dokumentieren': None, 'limit_cent': None},
                     {'id': 2, 'name': 'Bestehend', 'lesen': 1, 'einkaufen': 1, 'dokumentieren': 0, 'limit_cent': 0},
                     {'id': 3, 'name': 'Begrenzt', 'lesen': 1, 'einkaufen': 1, 'dokumentieren': 1, 'limit_cent': 4000}]
        with self.portal.app.test_request_context():
            html = render_template('assistent_rechte.html', employees=employees, events=[], read_only=True,
                                   operations_enabled=True, order_cap=25000, order_availability=self.manager.availability(),
                                   csrf_field=lambda: '')
        new = html.split('aria-labelledby="employee-1"')[1].split('</section>')[0]
        old = html.split('aria-labelledby="employee-2"')[1].split('</section>')[0]
        lower = html.split('aria-labelledby="employee-3"')[1].split('</section>')[0]
        self.assertIn('name="dokumentieren" type="checkbox" checked', new)
        self.assertIn('name="limit" inputmode="decimal" value="250.00"', new)
        self.assertIn('name="limit" inputmode="decimal" value="0.00"', old)
        self.assertIn('name="limit" inputmode="decimal" value="40.00"', lower)
        self.assertNotIn('readonly required', html)
        self.assertIn('Bilder und Unterlagen intern zuordnen', html)
        self.assertIn('/admin/assistent-bestellungen/betrieb', html)

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

    def test_weekly_requires_live_worker_then_dispatches_at_monday_fourteen(self):
        contact_id = self.ready()
        form = self.order_form(contact_id, urgency='weekly')
        self.assertEqual(self.post('/bestellen', form).status_code, 400)
        self.portal.app.config['ASSISTANT_ORDER_WORKER_ENABLED'] = True
        run_worker(self.manager, once=True)
        self.assertTrue(self.manager.availability()['worker_live'])
        self.assertEqual(self.post('/bestellen', form).status_code, 303)
        self.assertEqual(self.smtp.data_calls, 0)
        self.now = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
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
