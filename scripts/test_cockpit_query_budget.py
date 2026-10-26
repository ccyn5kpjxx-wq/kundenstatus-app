"""Real portal regressions with disposable data and the smoke harness network ban."""
from contextlib import closing
from pathlib import Path
import hashlib
import re
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from smoke_test import portal as p


class ServicePathCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'paths.sqlite3'
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('CREATE TABLE serviceprozesse (auftrag_id INTEGER, lead_id INTEGER, fahrzeug_id TEXT)')
            db.executemany('INSERT INTO serviceprozesse VALUES (?,?,?)',
                           [(7, 8, 'order-car'), (9, 7, 'lead-car')])
            db.commit()
        self.selects = []

        def connect():
            db = sqlite3.connect(self.path)
            db.row_factory = sqlite3.Row
            db.set_trace_callback(self.selects.append)
            return db

        self.connect = connect
        patcher = patch.object(p, 'get_db', connect)
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = patch.object(p, 'service_process', object())
        patcher.start()
        self.addCleanup(patcher.stop)

    def change(self, value):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('UPDATE serviceprozesse SET fahrzeug_id=? WHERE auftrag_id=7', (value,))
            db.commit()

    def test_get_caches_positive_and_missing_paths_and_separates_ids(self):
        with p.app.test_request_context('/admin/cockpit'):
            for _ in range(5):
                self.assertEqual(p.service_status_path(order_id=7), '/service/fahrzeug/order-car')
                self.assertEqual(p.service_status_path(lead_id=7), '/service/fahrzeug/lead-car')
                self.assertEqual(p.service_status_path(order_id=99), '')
        self.assertEqual(len(self.selects), 3)

    def test_new_get_reads_changed_positive_and_previously_missing_paths(self):
        with p.app.test_request_context('/admin/cockpit'):
            self.assertTrue(p.service_status_path(order_id=7).endswith('/order-car'))
            self.assertEqual(p.service_status_path(order_id=99), '')
        self.change('changed-car')
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('INSERT INTO serviceprozesse VALUES (99,99,?)', ('new-car',))
            db.commit()
        with p.app.test_request_context('/admin/cockpit'):
            self.assertTrue(p.service_status_path(order_id=7).endswith('/changed-car'))
            self.assertTrue(p.service_status_path(order_id=99).endswith('/new-car'))
        self.assertEqual(len(self.selects), 4)

    def test_post_and_background_calls_observe_changes_immediately(self):
        with p.app.test_request_context('/anfrage', method='POST'):
            self.assertTrue(p.service_status_path(order_id=7).endswith('/order-car'))
            self.change('post-car')
            self.assertTrue(p.service_status_path(order_id=7).endswith('/post-car'))
        self.assertTrue(p.service_status_path(order_id=7).endswith('/post-car'))
        self.change('background-car')
        self.assertTrue(p.service_status_path(order_id=7).endswith('/background-car'))
        self.assertEqual(len(self.selects), 4)

    def test_failed_lookup_is_closed_and_not_cached(self):
        broken = unittest.mock.Mock()
        broken.execute.side_effect = sqlite3.OperationalError('synthetic failure')
        with p.app.test_request_context('/admin/cockpit'):
            with patch.object(p, 'get_db', return_value=broken):
                with self.assertRaises(sqlite3.OperationalError):
                    p.service_status_path(order_id=7)
            broken.close.assert_called_once()
            self.assertTrue(p.service_status_path(order_id=7).endswith('/order-car'))

    def test_cached_paths_preserve_public_and_request_host_urls(self):
        order = {'id': 7, 'kunden_status_token': 'synthetic-token', 'kunden_status_aktiv': True}
        with patch.object(p, 'PORTAL_BASE_URL', 'https://portal.example.invalid'):
            with p.app.test_request_context('/admin/cockpit', base_url='http://localhost:5000'):
                self.assertEqual(p.kunden_status_url(order),
                                 'https://portal.example.invalid/service/fahrzeug/order-car')
                self.assertEqual(p.kunden_status_url(order, force_request_host=True),
                                 'http://localhost:5000/service/fahrzeug/order-car')
        self.assertEqual(len(self.selects), 1)


class CockpitQueryBudgetTests(unittest.TestCase):
    def test_realistic_archive_is_not_hydrated_and_remains_searchable(self):
        p.app.config['TESTING'] = True
        db = p.get_db()
        try:
            rows = []
            for index in range(322):
                archive_flag = None if index == 19 else int(index >= 20)
                rows.append((f'BUDGET-{index:03}', f'BUDGET-{index:03}', archive_flag,
                             1 if index < 20 else 6, '2026-10-01 08:00:00',
                             '2026-10-01 08:00:00', f'budget-order-{index}', f'budget-status-{index}'))
            db.executemany('''INSERT INTO auftraege
                (fahrzeug,kennzeichen,archiviert,status,erstellt_am,geaendert_am,token,kunden_status_token)
                VALUES (?,?,?,?,?,?,?,?)''', rows)
            db.commit()
            budget_ids = {row['id'] for row in db.execute(
                "SELECT id FROM auftraege WHERE token LIKE 'budget-order-%'").fetchall()}
        finally:
            db.close()
        self.addCleanup(self.remove_orders)
        client = p.app.test_client()
        with client.session_transaction() as session:
            session['admin'] = True

        queries = []
        original_get_db = p.get_db
        original_row = p.row_to_auftrag
        original_list = p.list_auftraege

        def counting_db():
            db = original_get_db()
            db.set_trace_callback(lambda sql: queries.append(' '.join(sql.split())))
            return db

        with patch.object(p, 'get_db', counting_db), patch.object(p, 'row_to_auftrag', wraps=original_row) as hydrate:
            response = client.get('/admin/cockpit')
            self.assertEqual(response.status_code, 200)
            budget_rows = [dict(call.args[0]) for call in hydrate.call_args_list
                           if str(call.args[0]['token']).startswith('budget-order-')]
            self.assertEqual(len(budget_rows), 20)
            self.assertTrue(any(row['archiviert'] is None for row in budget_rows))
            self.assertFalse(any(row['archiviert'] == 1 for row in budget_rows))
        selects = [sql for sql in queries if sql.upper().startswith('SELECT')]
        service_reads = [sql for sql in selects if sql.startswith('SELECT fahrzeug_id FROM serviceprozesse')]
        budget_reads = [sql for sql in service_reads
                        if (match := re.search(r'WHERE auftrag_id\s*=\s*(\d+)', sql))
                        and int(match[1]) in budget_ids]
        self.assertEqual(len(budget_reads), 0)
        batch_reads = [sql for sql in selects if sql.startswith('SELECT auftrag_id, fahrzeug_id FROM serviceprozesse')]
        self.assertEqual(len(batch_reads), 1)
        self.assertLessEqual(len(selects), 50)
        print(f'Cockpit: {len(selects)} SELECTs for 20 active and 302 archived tokenized orders')
        html = response.get_data(as_text=True)
        self.assertTrue('BUDGET-020' not in html, 'Archived vehicle must not appear in the cockpit')

        def legacy_cockpit_orders(*args, **kwargs):
            return [order for order in original_list(include_archived=True) if not order['archiviert']]

        with patch.object(p, 'list_auftraege', legacy_cockpit_orders):
            legacy = client.get('/admin/cockpit')
        self.assertEqual(legacy.status_code, 200)

        def normalize(value):
            value = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', '<CLOCK>', value)
            return re.sub(r'\b\d{2}:\d{2}:\d{2}\b', '<TIME>', value)

        self.assertEqual(hashlib.sha256(normalize(html).encode()).hexdigest(),
                         hashlib.sha256(normalize(legacy.get_data(as_text=True)).encode()).hexdigest(),
                         'Cockpit HTML must match the previous archive filtering behavior')
        search = client.get('/admin/cockpit/auftraege-suche?q=BUDGET-020')
        self.assertEqual(search.status_code, 200)
        self.assertTrue(any(item['kennzeichen'] == 'BUDGET-020' for item in search.json['items']))
        dashboard = client.get('/admin')
        self.assertEqual(dashboard.status_code, 200)
        self.assertNotIn('BUDGET-020', dashboard.get_data(as_text=True))
        archive = client.get('/admin?archiv=1')
        self.assertEqual(archive.status_code, 200)
        self.assertIn('BUDGET-020', archive.get_data(as_text=True), 'Archive remains explicitly accessible')

    @staticmethod
    def remove_orders():
        db = p.get_db()
        try:
            db.execute("DELETE FROM auftraege WHERE token LIKE 'budget-order-%'")
            db.commit()
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
