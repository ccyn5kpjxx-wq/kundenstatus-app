"""Actual portal PG adapter SQL against an isolated natural-key counter table.

No portal import or production DB; executing generated SQL makes a spurious
RETURNING id fail, rather than merely comparing implementation strings.
"""
import ast
from datetime import datetime, timedelta
from pathlib import Path
import re
import socket
import sqlite3
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from portal_request_timing import timed_database_call

SOURCE = ast.parse((ROOT / 'app.py').read_text(encoding='utf-8-sig'))
NAMES = {'DbRow', 'PostgresCursor', 'PostgresConnection', 'get_insert_table_name',
         'convert_sqlite_sql_to_postgres', 'split_sql_script', 'log_shop_klick',
         'list_shop_klick_statistik'}
NODES = [node for node in SOURCE.body
         if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in NAMES]


class Cursor:
    def __init__(self, db, statements):
        self.cursor = db.cursor()
        self.statements = statements

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cursor.close()

    def execute(self, sql, params):
        self.statements.append((sql, params))
        self.cursor.execute(sql.replace('%s', '?'), params)
        self.rowcount = self.cursor.rowcount
        self.description = ([SimpleNamespace(name=item[0]) for item in self.cursor.description]
                            if self.cursor.description else None)

    def fetchall(self):
        return self.cursor.fetchall()


class PostgresShopAnalyticsChecks(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.execute('''CREATE TABLE shop_klick_tageswerte (
            datum TEXT NOT NULL, seite TEXT NOT NULL, element TEXT NOT NULL,
            anzahl INTEGER NOT NULL, PRIMARY KEY (datum,seite,element))''')
        self.db.execute('CREATE TABLE besucher_events (id INTEGER PRIMARY KEY AUTOINCREMENT, seite TEXT)')
        self.statements = []
        self.labels = {'/shop/felgen': {'product-view': 'Artikel ansehen', 'cart-add': 'Warenkorb'},
                       '/': {'home-nav-shop': 'Shop öffnen'}}
        self.namespace = {'re': re, 'datetime': datetime, 'timedelta': timedelta, 'ZoneInfo': ZoneInfo,
                          'timed_database_call': timed_database_call, 'SHOP_KLICK_ELEMENTE': self.labels,
                          'besucher_client_klassen': lambda agent: ('Desktop', 'Chrome', agent == 'SYNTHETIC_BOT'),
                          'besucher_seiten_label': lambda website, page: page}
        exec(compile(ast.Module(body=NODES, type_ignores=[]), str(ROOT / 'app.py'), 'exec'), self.namespace)
        connection = SimpleNamespace(cursor=lambda: Cursor(self.db, self.statements),
                                     commit=self.db.commit, rollback=self.db.rollback, close=lambda: None)
        self.pg = self.namespace['PostgresConnection'](connection)
        self.namespace['get_db'] = lambda: self.pg
        self.network = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_actual_shop_counter_insert_repeat_and_report_work_with_no_id_column(self):
        today = datetime.now(ZoneInfo('Europe/Berlin')).date()
        expired = (today - timedelta(days=90)).isoformat()
        self.db.execute("INSERT INTO shop_klick_tageswerte VALUES (?,'/shop/felgen','product-view',99)", (expired,))
        click = self.namespace['log_shop_klick']
        click('/shop/felgen', 'product-view', 'SYNTHETIC_BROWSER')
        click('/shop/felgen', 'product-view', 'SYNTHETIC_BROWSER')
        click('/shop/felgen', 'cart-add', 'SYNTHETIC_BROWSER')
        rows = self.db.execute('SELECT datum,seite,element,anzahl FROM shop_klick_tageswerte ORDER BY element').fetchall()
        self.assertEqual(rows, [(today.isoformat(), '/shop/felgen', 'cart-add', 1),
                                (today.isoformat(), '/shop/felgen', 'product-view', 2)])
        report = self.namespace['list_shop_klick_statistik'](self.pg, 7)
        self.assertEqual(report['gesamt'], 3)
        self.assertEqual(report['items'][0]['element_label'], 'Artikel ansehen')
        self.assertEqual(report['items'][0]['klicks'], 2)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM besucher_events').fetchone()[0], 0)
        inserts = [sql for sql, params in self.statements if sql.lstrip().upper().startswith('INSERT')]
        self.assertEqual(len(inserts), 3)
        self.assertTrue(all('%s' in sql and 'RETURNING' not in sql.upper() for sql in inserts))

    def test_generated_ids_still_return_and_explicit_natural_key_returning_remains_intact(self):
        cursor = self.pg.execute('INSERT INTO besucher_events (seite) VALUES (?)', ('/shop/felgen',))
        self.assertEqual(cursor.lastrowid, 1)
        self.assertEqual(self.pg.execute('SELECT LAST_INSERT_ROWID()').fetchone()[0], 1)
        value = self.pg.execute('''INSERT INTO shop_klick_tageswerte VALUES (?,?,?,1)
            ON CONFLICT (datum,seite,element) DO UPDATE SET anzahl=shop_klick_tageswerte.anzahl+1
            RETURNING anzahl''', ('2026-11-17', '/shop/felgen', 'product-view')).fetchone()
        self.assertEqual(value['anzahl'], 1)
        self.assertNotIn('RETURNING id', self.statements[-1][0])

    def test_invalid_or_bot_events_do_not_open_or_mutate_the_counter(self):
        with patch.dict(self.namespace, {'get_db': lambda: (_ for _ in ()).throw(AssertionError('No DB'))}):
            for page, element, agent in (('/unreviewed', 'product-view', 'SYNTHETIC_BROWSER'),
                                         ('/shop/felgen', 'customer@example.invalid', 'SYNTHETIC_BROWSER'),
                                         ('/shop/felgen', 'product-view', 'SYNTHETIC_BOT')):
                self.namespace['log_shop_klick'](page, element, agent)
        self.assertEqual(self.statements, [])


if __name__ == '__main__':
    unittest.main()
