"""Stored mounting wishes must represent a purchased local workshop service."""
import json
import sqlite3
import sys
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shop_admin import ShopAdminReader
from shop_workshop_queue import mark_handled
from test_shop_order_store import quote


class LegacyWishChecks(TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            CREATE TABLE shop_orders_v2(id TEXT PRIMARY KEY, mode TEXT, status TEXT,
                                        snapshot_json TEXT, delivery_context_json TEXT);
            CREATE TABLE shop_order_appointment_requests_v2(order_id TEXT, requested_date TEXT, requested_time TEXT);
            CREATE TABLE shop_payment_attempts_v2(order_id TEXT, state TEXT);
        ''')
        self.tables = {'shop_orders_v2', 'shop_order_appointment_requests_v2', 'shop_payment_attempts_v2'}

    def add(self, order_id, snapshot, delivery):
        self.db.execute("INSERT INTO shop_orders_v2 VALUES (?,'live','paid',?,?)", (order_id, snapshot, delivery))
        self.db.execute("INSERT INTO shop_order_appointment_requests_v2 VALUES (?,'2099-01-07','10:00')", (order_id,))

    def test_legacy_wishes_do_not_display_confirm_button_or_block_processing(self):
        service = quote()
        goods = quote()
        goods['request'][0]['services'] = {}
        goods['lines'][0]['services'] = [{'code': 'required:rdks', 'gross_cent': 2000}]
        self.add('goods', json.dumps(goods), '{}')
        self.add('reship', json.dumps(service), '{"workshop_delivery":"customer_reshipment"}')
        self.add('bad_snapshot', '{broken', '{}')
        self.add('bad_delivery', json.dumps(service), 'null')
        self.add('local', json.dumps(service), '{}')
        rows = self.db.execute('SELECT id FROM shop_orders_v2').fetchall()
        related = ShopAdminReader._related(self.db, self.tables, rows)
        calendar = Mock(return_value={})
        for order_id in ('goods', 'reship', 'bad_snapshot', 'bad_delivery'):
            with self.subTest(order_id=order_id):
                self.assertNotIn('appointment', related[order_id])
                self.assertIsNone(mark_handled(self.db, self.tables, order_id, calendar))
        calendar.assert_not_called()
        self.assertEqual(related['local']['appointment'], '07.01.2099, 10:00 Uhr')
        self.assertIn('bestätigen', mark_handled(self.db, self.tables, 'local', calendar))
        calendar.assert_called_once_with(['local'])


if __name__ == '__main__':
    main()
