"""Offline regressions for the PostgreSQL failures found in the portal audit."""
from io import BytesIO
from pathlib import Path
import base64
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import smoke_test as isolated  # Configures temporary data and denies all network.
from werkzeug.datastructures import FileStorage

p = isolated.portal
p.app.config['TESTING'] = True


class PgCursor:
    """Exercise the real adapter while enforcing PG's aborted-transaction rule."""
    def __init__(self, connection):
        self.connection = connection
        self.raw = connection.raw.cursor()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.raw.close()

    def execute(self, sql, params):
        self.connection.statements.append(sql)
        if sql.startswith('ROLLBACK TO SAVEPOINT'):
            self.connection.aborted = False
        elif self.connection.aborted:
            raise RuntimeError('current transaction is aborted')
        if 'INSERT OR REPLACE' in sql or (
            self.connection.fail_backup and sql.startswith('INSERT INTO mietbild_backups')
        ):
            self.connection.aborted = True
            raise RuntimeError('synthetic backup SQL failure')
        self.raw.execute(sql.replace('%s', '?'), params)
        self.rowcount = self.raw.rowcount
        self.description = ([SimpleNamespace(name=c[0]) for c in self.raw.description]
                            if self.raw.description else None)

    def fetchall(self):
        return self.raw.fetchall()


class PgConnection:
    def __init__(self, raw, statements, fail_backup=False):
        self.raw, self.statements, self.fail_backup = raw, statements, fail_backup
        self.aborted = False

    def cursor(self):
        return PgCursor(self)

    def commit(self):
        if self.aborted:
            # PostgreSQL COMMIT in an aborted transaction rolls back instead.
            self.raw.rollback()
        else:
            self.raw.commit()

    def rollback(self):
        self.aborted = False
        self.raw.rollback()

    def close(self):
        self.raw.close()


class SystemAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        p.init_db()

    def setUp(self):
        self.original_get_db = p.get_db
        db = p.get_db()
        try:
            db.execute('DELETE FROM erinnerungen')
            db.execute('DELETE FROM mietbild_backups')
            db.execute('DELETE FROM mietfahrzeug_bilder')
            db.execute('DELETE FROM mietfahrzeuge')
            db.execute("INSERT INTO mietfahrzeuge(id,kennzeichen,erstellt_am,geaendert_am) VALUES(1,'TEST','2026-10-08','2026-10-08')")
            db.commit()
        finally:
            db.close()
        backup_patcher = patch.object(p, 'schedule_change_backup')
        self.backup = backup_patcher.start()
        self.addCleanup(backup_patcher.stop)

    def pg_factory(self, statements, fail_backup=False):
        return p.PostgresConnection(PgConnection(self.original_get_db(), statements, fail_backup))

    def test_reminder_pg_route_returns_success_after_committed_update(self):
        db = p.get_db()
        db.execute("INSERT INTO erinnerungen(id,text,status,erstellt_am,erledigt_am) VALUES(1,'Synthetic','offen','2026-10-08','')")
        db.commit(); db.close()
        client = p.app.test_client()
        with client.session_transaction() as session:
            session['admin'] = True
            session['csrf_token'] = 'synthetic-audit-csrf'
        statements = []
        with patch.object(p, 'get_db', side_effect=lambda: self.pg_factory(statements)):
            response = client.post('/admin/erinnerungen/1/erledigt', data={'csrf_token': 'synthetic-audit-csrf'})
        self.assertEqual(response.status_code, 302)
        db = p.get_db()
        row = db.execute('SELECT status,erledigt_am FROM erinnerungen WHERE id=1').fetchone()
        db.close()
        self.assertEqual(row['status'], 'erledigt')
        self.assertTrue(row['erledigt_am'])
        self.backup.assert_called_once_with('erinnerung-erledigt')

    def test_missing_reminder_pg_does_not_report_change_or_backup(self):
        with patch.object(p, 'get_db', side_effect=lambda: self.pg_factory([])):
            self.assertFalse(p.mark_erinnerung_erledigt(999))
        self.backup.assert_not_called()

    def test_rental_image_pg_adapter_keeps_metadata_and_original_backup(self):
        raw = b'synthetic original image bytes'
        statements = []
        with patch.object(p, 'get_db', side_effect=lambda: self.pg_factory(statements)):
            saved = p.save_mietfahrzeug_bilder(1, [FileStorage(stream=BytesIO(raw), filename='synthetic.jpg')])
        self.assertEqual(saved, 1)
        db = p.get_db()
        row = db.execute('SELECT b.stored_name, s.file_base64 FROM mietfahrzeug_bilder b JOIN mietbild_backups s ON s.bild_id=b.id').fetchone()
        db.close()
        self.assertIsNotNone(row)
        self.assertEqual(base64.b64decode(row['file_base64']), raw)
        self.assertEqual((p.UPLOAD_DIR / row['stored_name']).read_bytes(), raw)
        self.assertTrue(any(sql.rstrip().endswith('RETURNING bild_id') for sql in statements))

    def test_optional_backup_sql_failure_does_not_rollback_rental_metadata(self):
        raw = b'synthetic original retained if optional backup fails'
        statements = []
        with patch.object(p, 'get_db', side_effect=lambda: self.pg_factory(statements, True)):
            saved = p.save_mietfahrzeug_bilder(1, [FileStorage(stream=BytesIO(raw), filename='synthetic.jpg')])
        self.assertEqual(saved, 1)
        db = p.get_db()
        row = db.execute('SELECT stored_name FROM mietfahrzeug_bilder').fetchone()
        self.assertEqual(db.execute('SELECT COUNT(*) FROM mietbild_backups').fetchone()[0], 0)
        db.close()
        self.assertIsNotNone(row)
        self.assertEqual((p.UPLOAD_DIR / row['stored_name']).read_bytes(), raw)
        self.assertIn('ROLLBACK TO SAVEPOINT mietbild_backup', statements)
        self.assertIn('RELEASE SAVEPOINT mietbild_backup', statements)

    def test_badge_count_is_shared_within_render_and_fresh_next_request(self):
        count = Mock(side_effect=[7, 8])
        with patch.object(p, 'mahnungen_faellig_anzahl', count):
            with p.app.test_request_context('/admin/cockpit'):
                helpers = p.inject_csrf_helpers()
                self.assertEqual(helpers['mahnungen_faellig_count'](), 7)
                self.assertEqual(helpers['mahnungen_faellig_count'](), 7)
                self.assertEqual(count.call_count, 1)
            with p.app.test_request_context('/admin/cockpit'):
                self.assertEqual(p.inject_csrf_helpers()['mahnungen_faellig_count'](), 8)
        self.assertEqual(count.call_count, 2)

    def test_four_http_lock_waiters_return_busy_and_health_remains_usable(self):
        entered, release = threading.Event(), threading.Event()
        def owner():
            with p.portal_originals_operation_lock():
                entered.set()
                release.wait(5)
        thread = threading.Thread(target=owner)
        thread.start()
        self.assertTrue(entered.wait(2))
        def waiter():
            with p.app.test_request_context('/werkstatt/materialbestellung'):
                with p.portal_originals_operation_lock():
                    self.fail('Contended request acquired a lock still owned elsewhere')
        try:
            start = time.monotonic()
            with patch.object(p, 'PORTAL_ORIGINALS_REQUEST_WAIT_SECONDS', 0.05):
                with ThreadPoolExecutor(max_workers=4) as pool:
                    futures = [pool.submit(waiter) for _ in range(4)]
                    for future in futures:
                        with self.assertRaises(p.ServiceUnavailable) as caught:
                            future.result(timeout=2)
                        response = caught.exception.get_response()
                        self.assertEqual(response.status_code, 503)
                        self.assertEqual(response.headers['Retry-After'], '2')
            self.assertLess(time.monotonic() - start, 1)
            self.assertEqual(p.app.test_client().get('/healthz').status_code, 200)
        finally:
            release.set(); thread.join(2)
        self.assertFalse(thread.is_alive())
        with p.app.test_request_context('/'):
            with p.portal_originals_operation_lock(), p.portal_originals_operation_lock():
                self.assertEqual(p._portal_originals_lock_state.depth, 2)
        self.assertEqual(p._portal_originals_lock_state.depth, 0)

    def test_pg_lock_contention_is_bounded_and_connection_released(self):
        connection = Mock()
        connection.execute.return_value.fetchone.return_value = (False,)
        with patch.object(p, 'USE_POSTGRES', True), patch.object(p, 'open_fresh_db', return_value=connection), patch.object(p, 'PORTAL_ORIGINALS_REQUEST_WAIT_SECONDS', 0.02):
            with p.app.test_request_context('/'):
                with self.assertRaises(p.ServiceUnavailable):
                    with p.portal_originals_operation_lock():
                        self.fail('Foreign PG lock must not be entered')
        self.assertTrue(all('pg_try_advisory_lock' in call.args[0] for call in connection.execute.call_args_list))
        connection.close.assert_called_once()
        # A timed-out owner must also release the process-local RLock.
        with p.app.test_request_context('/'):
            with p.portal_originals_operation_lock():
                self.assertEqual(p._portal_originals_lock_state.depth, 1)

    def test_pg_connection_deadline_and_request_reuse(self):
        connector = Mock()
        with patch.object(p, 'USE_POSTGRES', True), patch.object(p, 'psycopg', connector):
            with p.app.test_request_context('/'):
                first = p.get_db()
                self.assertIs(p.get_db(), first)
                self.assertEqual(connector.connect.call_count, 1)
                p.open_fresh_db().close()
            with p.app.app_context():
                p.get_db().close()
        self.assertEqual(connector.connect.call_count, 3)
        self.assertTrue(all(call.kwargs.get('connect_timeout') == 5 for call in connector.connect.call_args_list))


if __name__ == '__main__':
    unittest.main(verbosity=2)
