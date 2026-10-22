"""Real cost policy + employee progress in a synthetic miniapp, without providers."""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import sys
import tempfile
from threading import RLock
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4

from flask import Flask, abort, jsonify, request, session

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from werkstatt_fortschritt import WorkshopProgress, ProgressError
from werkstatt_serviceprozess import ServiceProcess, ensure_schema


class ServiceProgressGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'synthetic.db'
        self.app = Flask(__name__)
        self.app.secret_key = 'synthetic-miniapp-no-production-data'
        self.app.config['TESTING'] = True
        self.lock = RLock()
        self.p = SimpleNamespace(get_db=self.db, USE_POSTGRES=False,
            portal_originals_operation_lock=lambda: self.lock, LEXWARE_TAX_RATE=19)

        @contextmanager
        def connection():
            db = self.db()
            try:
                yield db
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()
        self.fleet = SimpleNamespace(db=connection)
        self.p.service_process = ServiceProcess(self.p, self.fleet)
        with connection() as db:
            db.executescript('''CREATE TABLE auftraege(
                id INTEGER PRIMARY KEY,fahrzeug TEXT,kennzeichen TEXT,fin_nummer TEXT,
                werkstatt_angebot_text TEXT,werkstatt_angebot_preis TEXT,angebot_status TEXT,preisstand_json TEXT,
                status INTEGER,archiviert INTEGER DEFAULT 0,produktion_schritt TEXT DEFAULT '',
                lackierbereit INTEGER DEFAULT 0,lackierbereit_am TEXT DEFAULT '',geaendert_am TEXT,
                start_datum TEXT DEFAULT '',fertig_datum TEXT DEFAULT '',
                fahrzeug_abholbereit INTEGER DEFAULT 0,fahrzeug_abholbereit_am TEXT DEFAULT '');
                CREATE TABLE status_log(id INTEGER PRIMARY KEY,auftrag_id INTEGER,status INTEGER,zeitstempel TEXT);
                CREATE TABLE assistent_fortschritt_audit(id INTEGER PRIMARY KEY,actor TEXT,request_id TEXT,
                payload_fingerprint TEXT,order_id INTEGER,action TEXT,expected_status INTEGER,
                expected_changed_at TEXT,created_at TEXT,result_json TEXT DEFAULT '',UNIQUE(actor,request_id));
                CREATE TABLE kundenflotten_fahrzeuge(id TEXT PRIMARY KEY,firma_id TEXT,kennzeichen TEXT,fin TEXT);
                INSERT INTO kundenflotten_fahrzeuge VALUES('vehicle','company','TEST 1','');
                INSERT INTO auftraege(id,fahrzeug,kennzeichen,fin_nummer,werkstatt_angebot_text,
                werkstatt_angebot_preis,angebot_status,preisstand_json,status,geaendert_am)
                VALUES(1,'Synthetic car','TEST 1','','Bremsen vorne erneuern','200 EUR netto','angenommen','{}',2,'2026-10-21T09:00:00');
                INSERT INTO auftraege(id,fahrzeug,kennzeichen,status,geaendert_am)
                VALUES(2,'Legacy synthetic car','TEST 2',3,'2026-10-21T09:00:00');''')
            ensure_schema(db)
        self.progress = WorkshopProgress(self.p, initialize=False)
        self.admin = {'role': 'werkstatt', 'id': 'synthetic-admin'}
        self.manager = {'role': 'fuhrparkleitung', 'id': 'manager', 'company_id': 'company'}
        self.case = self.p.service_process.bind_case(order_id=1, company_id='company', vehicle_id='vehicle', actor=self.admin)
        parties = {k: {'name': 'Synthetic party'} for k in ('auftraggeber','freigebender','rechnungsempfaenger','zahler')}
        parties['freigebender']['identity_id'] = 'manager'
        parties['rechnungsempfaenger'].update(strasse='Testweg 1', plz='74821', ort='Mosbach', land='DE')
        parties.update(zahlungsweg='firmenrechnung', fallart='firma')
        self.p.service_process.save_parties(self.case, parties, self.admin, expected_revision=1)
        self.offer = self.p.service_process.sync_offer(self.case, self.admin)
        self.employee = {'actor': 'employee:synthetic', 'dokumentieren': 1}
        self.forms = {}

        @self.app.post('/employee/order/<int:oid>/status')
        def employee_status(oid):
            if session.get('employee') != 'synthetic':
                abort(403)
            if request.form.get('csrf_token') != session.get('csrf_token'):
                abort(400)
            form = self.forms[request.form['request_id']]
            if form['auftrag_id'] != oid:
                abort(403)
            try:
                with self.p.portal_originals_operation_lock():
                    result = self.progress.confirm(form, self.employee, request.form['request_id'])
                return jsonify(result)
            except ProgressError as exc:
                return jsonify(error=exc.code, message=str(exc)), exc.status_code
        self.client = self.app.test_client()
        with self.client.session_transaction() as state:
            state.update(employee='synthetic', csrf_token='synthetic-csrf')

    def db(self):
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        return db

    def sql(self, query, values=()):
        with self.fleet.db() as db:
            return db.execute(query, values).fetchall()

    def approve(self):
        self.offer = self.p.service_process.sync_offer(self.case, self.admin)
        self.p.service_process.approve(self.case, self.manager,
            expected_revision=self.offer['revision'], snapshot_hash=self.offer['snapshot_hash'], request_id=uuid4().hex)

    def form(self, action='in_arbeit_starten', oid=1):
        request_id = uuid4().hex
        self.forms[request_id] = self.progress.preview(oid, action, self.employee)
        return {'csrf_token': 'synthetic-csrf', 'request_id': request_id}

    def test_native_employee_start_cannot_bypass_missing_cost_approval(self):
        form = self.form()
        response = self.client.post('/employee/order/1/status', data=form)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json['error'], 'cost_approval_required')
        self.assertEqual(self.progress.read(1)['status'], 2)
        self.assertEqual(len(self.sql('SELECT * FROM assistent_fortschritt_audit')), 0)
        self.assertEqual(len(self.sql('SELECT * FROM status_log')), 0)

    def test_approved_native_start_and_finish_succeed(self):
        self.approve()
        response = self.client.post('/employee/order/1/status', data=self.form())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.progress.read(1)['status'], 3)
        response = self.client.post('/employee/order/1/status', data=self.form('fertig_melden'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.progress.read(1)['status'], 4)

    def test_saved_employee_finish_form_rejects_changed_price_without_status_change(self):
        self.approve()
        self.client.post('/employee/order/1/status', data=self.form())
        form = self.form('fertig_melden')
        self.sql("UPDATE auftraege SET werkstatt_angebot_preis='300 EUR netto' WHERE id=1")
        response = self.client.post('/employee/order/1/status', data=form)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.progress.read(1)['status'], 3)
        self.assertEqual(len(self.sql('SELECT * FROM assistent_fortschritt_audit')), 1)

    def test_replay_also_rechecks_current_approval(self):
        self.approve()
        form = self.form()
        self.assertEqual(self.client.post('/employee/order/1/status', data=form).status_code, 200)
        self.sql("UPDATE auftraege SET werkstatt_angebot_text='Additional repair' WHERE id=1")
        response = self.client.post('/employee/order/1/status', data=form)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(len(self.sql('SELECT * FROM assistent_fortschritt_audit')), 1)

    def test_existing_progress_endpoint_cannot_bypass_invalidated_approval(self):
        self.approve()
        self.client.post('/employee/order/1/status', data=self.form())
        state = self.progress.read(1)
        self.sql("UPDATE auftraege SET werkstatt_angebot_preis='250 EUR netto' WHERE id=1")
        with self.assertRaises(ProgressError) as error:
            self.progress.update(1, 'lackierung_starten', state['status'], state['geaendert_am'], 'employee:synthetic', uuid4().hex)
        self.assertEqual(error.exception.status_code, 409)
        self.assertEqual(error.exception.code, 'cost_approval_required')
        self.assertEqual(self.progress.read(1)['produktion_schritt'], '')

    def test_unbound_legacy_order_and_portal_without_policy_keep_behavior(self):
        state = self.progress.read(2)
        self.assertEqual(self.progress.update(2, 'lackierbereit', state['status'], state['geaendert_am'], 'employee:synthetic', uuid4().hex)['fortschritt']['lackierbereit'], True)
        del self.p.service_process
        self.assertEqual(self.client.post('/employee/order/1/status', data=self.form()).status_code, 200)
        self.assertEqual(self.progress.read(1)['status'], 3)

    def test_missing_approval_is_checked_under_gate_before_database_access(self):
        held = []

        @contextmanager
        def gate():
            with self.lock:
                held.append(True)
                try:
                    yield
                finally:
                    held.pop()

        def guarded_db():
            self.assertTrue(held, 'Service row locks must follow the shared operation gate')
            return self.db()

        state = self.progress.read(1)
        with patch.object(self.p, 'portal_originals_operation_lock', gate), patch.object(self.p, 'get_db', guarded_db):
            with self.assertRaises(ProgressError) as error:
                self.progress.update(1, 'lackierbereit', state['status'], state['geaendert_am'], 'employee:test', uuid4().hex)
        self.assertEqual(error.exception.code, 'cost_approval_required')
        self.assertEqual(held, [])
        self.assertEqual(self.progress.read(1)['status'], 2)
        self.assertEqual(self.sql('SELECT * FROM assistent_fortschritt_audit'), [])

    def test_authorization_callback_takes_order_locks_inside_shared_gate(self):
        held = []

        @contextmanager
        def gate():
            with self.lock:
                held.append(True)
                try:
                    yield
                finally:
                    held.pop()

        def authorize(db, order_id):
            self.assertTrue(held, 'Employee authorization must not lock an order before the gate')
            db.execute('UPDATE auftraege SET id=id WHERE id=?', (order_id,))

        state = self.progress.read(2)
        with patch.object(self.p, 'portal_originals_operation_lock', gate):
            result = self.progress._update(2, 'lackierbereit', state['status'], state['geaendert_am'],
                                           'employee:test', uuid4().hex, authorize=authorize)
        self.assertTrue(result['fortschritt']['lackierbereit'])
        self.assertEqual(held, [])

    def test_standalone_legacy_adapter_without_shared_gate_remains_compatible(self):
        del self.p.portal_originals_operation_lock
        state = self.progress.read(2)
        result = self.progress.update(2, 'lackierbereit', state['status'], state['geaendert_am'],
                                      'standalone:test', uuid4().hex)
        self.assertTrue(result['fortschritt']['lackierbereit'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
