"""Real legacy-count equivalence on synthetic, network-isolated fixtures."""
from datetime import datetime
from itertools import product
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from smoke_test import portal as p  # Isolated app import, external sockets denied.
from portal_navigation_snapshot import NAVIGATION_SQL_COUNT_KEYS, load_navigation_sql_counts


SCHEMA = """
CREATE TABLE mitarbeiter_aufgaben (datum TEXT, status TEXT);
CREATE TABLE leads (status TEXT);
CREATE TABLE bewerbungen (status TEXT);
CREATE TABLE fahrzeugsuchen (status TEXT);
CREATE TABLE fahrzeugverkaeufe (status TEXT);
CREATE TABLE fahrzeugeinkauf_fahrzeuge (interessant INTEGER, status TEXT);
CREATE TABLE mietwagen_anfragen (status TEXT);
CREATE TABLE auftraege (versicherung_id INTEGER, archiviert INTEGER, quelle TEXT, angebotsphase INTEGER);
CREATE TABLE werkstatt_emails (status TEXT);
"""


class ReadConnection:
    def __init__(self, db):
        self.db = db
        self.statements = []
        self.closes = 0
        self.commits = 0
        self.rollbacks = 0

    def execute(self, sql, params=()):
        if not sql.lstrip().upper().startswith('SELECT'):
            raise AssertionError('The count snapshot must remain read-only')
        self.statements.append((sql, tuple(params)))
        return self.db.execute(sql, params)

    def close(self):
        # Legacy helpers close their borrowed connection; the new helper must not.
        self.closes += 1

    def commit(self):
        self.commits += 1
        raise AssertionError('No count should commit')

    def rollback(self):
        self.rollbacks += 1
        raise AssertionError('No count should roll back')


class FrozenDateTime:
    current = datetime(2026, 10, 26, 12, 0)

    @classmethod
    def now(cls):
        return cls.current


class NavigationSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.addCleanup(self.db.close)
        FrozenDateTime.current = datetime(2026, 10, 26, 12, 0)

    def snapshot(self, today_text='26.10.2026'):
        read_db = ReadConnection(self.db)
        values = load_navigation_sql_counts(read_db, today_text)
        self.assertEqual(len(read_db.statements), 1)
        self.assertEqual(read_db.statements[0][1], (today_text,))
        self.assertEqual((read_db.closes, read_db.commits, read_db.rollbacks), (0, 0, 0))
        self.assertEqual(set(values), set(NAVIGATION_SQL_COUNT_KEYS))
        self.assertTrue(all(type(value) is int for value in values.values()))
        return values

    def compare_legacy(self):
        read_db = ReadConnection(self.db)
        # The integration retains this sync BEFORE loading. Its side effect is
        # outside the read-only count contract and is not repeated by the module.
        with patch.object(p, 'get_db', return_value=read_db), \
             patch.object(p, 'sync_mietwagen_anfragen_to_leads') as sync, \
             patch.object(p, 'datetime', FrozenDateTime):
            old = {key: getattr(p, key)() for key in NAVIGATION_SQL_COUNT_KEYS}
        sync.assert_called_once_with()
        self.assertEqual(len(read_db.statements), 9)
        actual = self.snapshot(FrozenDateTime.current.strftime(p.DATE_FMT))
        self.assertEqual(actual, old)
        return actual

    def test_empty_tables_match_all_nine_legacy_counts(self):
        self.assertEqual(self.compare_legacy(), dict.fromkeys(NAVIGATION_SQL_COUNT_KEYS, 0))

    def test_status_null_empty_case_and_whitespace_predicates_match(self):
        statuses = (None, '', 'neu', ' neu ', 'NEU', '\nneu\t', 'offen', 'gewonnen',
                    'verloren', 'abgeschlossen', 'abgelehnt', 'verkauft', 'gekauft',
                    'verworfen', 'erledigt', 'ERLEDIGT', 'in_zahlung', 'sonstiges')
        for table in ('leads', 'bewerbungen', 'fahrzeugsuchen', 'fahrzeugverkaeufe',
                      'mietwagen_anfragen', 'werkstatt_emails'):
            self.db.executemany(f'INSERT INTO {table} (status) VALUES (?)', [(status,) for status in statuses])
        self.db.executemany('INSERT INTO fahrzeugeinkauf_fahrzeuge VALUES (?,?)',
                            list(product((None, 0, 1, 2), statuses)))
        result = self.compare_legacy()
        self.assertEqual(result['admin_leads_count'], len(statuses)-3)  # NULL is excluded.
        self.assertEqual(result['fahrzeugverkauf_count'], len(statuses)-1)  # NULL is included.
        self.assertEqual(result['admin_bewerbungen_count'], 1)
        self.assertEqual(result['admin_email_count'], 1)
        self.assertEqual(result['mietwagen_anfragen_count'], 1)
        self.assertEqual(result['fahrzeugeinkauf_count'], len(statuses)-2)

    def test_insurance_source_archive_and_offer_matrix_matches(self):
        rows = list(product((None, -1, 0, 1, 2), (None, 0, 1, 2),
                            (None, '', 'autohaus', ' autohaus ', 'intern', 'werkstatt'),
                            (None, 0, 1, 2)))
        self.db.executemany('INSERT INTO auftraege VALUES (?,?,?,?)', rows)
        result = self.compare_legacy()
        # Positive insurance IDs, unset/zero archive flags, and all source/phase
        # combinations except explicit autohaus offers: 2 * 2 * (6 * 4 - 2).
        self.assertEqual(result['admin_versicherung_count'], 88)

    def test_tasks_use_the_supplied_exact_legacy_day_text(self):
        dates = (None, '', '26.10.2026', '27.10.2026', '25.10.2026',
                 '2026-10-26', ' 26.10.2026 ', '26.10.2026 12:00:00')
        statuses = (None, '', 'offen', 'erledigt', 'ERLEDIGT', ' erledigt ')
        self.db.executemany('INSERT INTO mitarbeiter_aufgaben VALUES (?,?)', list(product(dates, statuses)))
        self.assertEqual(self.compare_legacy()['aufgaben_offen_heute_anzahl'], 5)
        FrozenDateTime.current = datetime(2026, 10, 27, 0, 1)
        self.assertEqual(self.compare_legacy()['aufgaben_offen_heute_anzahl'], 5)
        self.assertEqual(self.snapshot('28.10.2026')['aufgaben_offen_heute_anzahl'], 0)

    def test_fresh_calls_observe_writes_without_mutating_prior_result(self):
        before = self.snapshot()
        self.db.execute("INSERT INTO leads VALUES ('neu')")
        self.db.execute("INSERT INTO werkstatt_emails VALUES ('neu')")
        after = self.snapshot()
        self.assertEqual(before['admin_leads_count'], 0)
        self.assertEqual(after['admin_leads_count'], 1)
        self.assertEqual(after['admin_email_count'], 1)
        self.db.execute("UPDATE leads SET status='gewonnen'")
        self.assertEqual(self.snapshot()['admin_leads_count'], 0)

    def test_date_is_bound_not_interpolated_and_no_sync_is_started(self):
        exact = "26.10.2026' OR 1=1 --"
        self.db.execute('INSERT INTO mitarbeiter_aufgaben VALUES (?,?)', (exact, 'offen'))
        self.db.execute("INSERT INTO mitarbeiter_aufgaben VALUES ('other','offen')")
        with patch.object(p, 'sync_mietwagen_anfragen_to_leads') as sync:
            result = self.snapshot(exact)
        sync.assert_not_called()
        self.assertEqual(result['aufgaben_offen_heute_anzahl'], 1)

    def test_database_errors_propagate_without_connection_lifecycle_changes(self):
        db = Mock()
        db.execute.side_effect = sqlite3.OperationalError('synthetic failure')
        with self.assertRaises(sqlite3.OperationalError):
            load_navigation_sql_counts(db, '26.10.2026')
        db.close.assert_not_called()
        db.commit.assert_not_called()
        db.rollback.assert_not_called()

    def test_real_portal_postgres_adapter_converts_parameter_and_row_shape(self):
        self.db.execute("INSERT INTO mitarbeiter_aufgaben VALUES ('26.10.2026','offen')")
        self.db.execute("INSERT INTO leads VALUES ('neu')")
        converted = []
        raw = self.db

        class CursorBridge:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.cursor.close()

            def execute(self, sql, params):
                converted.append((sql, params))
                self.cursor = raw.execute(sql.replace('%s', '?'), params)

            @property
            def description(self):
                return [SimpleNamespace(name=column[0]) for column in self.cursor.description]

            @property
            def rowcount(self):
                return self.cursor.rowcount

            def fetchall(self):
                return self.cursor.fetchall()

        adapter = p.PostgresConnection(SimpleNamespace(cursor=CursorBridge))
        result = load_navigation_sql_counts(adapter, '26.10.2026')
        self.assertEqual(result, self.snapshot())
        self.assertEqual(len(converted), 1)
        self.assertEqual(converted[0][1], ('26.10.2026',))
        self.assertIn('datum=%s', converted[0][0])
        self.assertNotIn('?', converted[0][0])


if __name__ == '__main__':
    unittest.main()
