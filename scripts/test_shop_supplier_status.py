"""Internal order detail views over the offline, synthetic RSU procurement journal."""
from contextlib import closing
from copy import deepcopy
import json
import unittest

from test_shop_fulfillment import SagaFixture
from shop_supplier_status import supplier_status_by_line


TABLES = {'shop_fulfillment_v2', 'shop_fulfillment_positions_v3'}


class SupplierStatusChecks(SagaFixture, unittest.TestCase):
    def status(self, lines=None, *, tables=TABLES, mode='test', order=None):
        with closing(self.factory()) as db:
            db.execute('PRAGMA query_only=ON')
            return supplier_status_by_line(db, tables, order or self.order['id'], mode,
                                           lines if lines is not None else self.snapshot_lines())

    def snapshot_lines(self):
        return self.store.snapshot_for_order(self.order['id'], customer_id='customer-a', mode='test')['lines']

    def test_visible_receipts_only_belong_to_this_order_and_mode(self):
        self.assertTrue(all(s['state'] == 'undocumented' for s in self.status()))
        self.run_saga()
        positions = self.status()
        self.assertEqual({s['order_number'] for s in positions}, {'Test_1234'})
        self.assertTrue(all(s['state'] == 'accepted' for s in positions))
        self.assertEqual(len({s['position_key'] for s in positions}), 4)
        self.assertTrue(all(set(s) == {'order_number', 'label', 'state', 'position_key'} for s in positions))
        for query in ({'tables': set()}, {'mode': 'live'}, {'order': 'other-order'}):
            self.assertTrue(all(s['state'] == 'undocumented' for s in self.status(**query)))

    def test_mixed_receipts_keep_separate_statuses_after_timeout(self):
        self.transport.outcomes = ['accepted', 'timeout']
        self.run_saga()
        positions = self.status()
        self.assertCountEqual([s['state'] for s in positions], ['accepted', 'uncertain', 'pending', 'pending'])
        self.assertTrue(all(s['order_number'] is None for s in positions if s['state'] != 'accepted'))

    def test_rejection_has_no_number(self):
        self.transport.outcomes = ['rejected']
        self.run_saga()
        positions = self.status()
        self.assertIn('rejected', [s['state'] for s in positions])
        self.assertTrue(all(s['order_number'] is None for s in positions))

    def test_aggregated_supplier_position_maps_to_each_matching_shop_line(self):
        self.run_saga()
        lines = deepcopy(self.snapshot_lines())
        index = next(i for i, line in enumerate(lines) if line['quantity'] >= 2)
        extra = dict(lines[index], quantity=1)
        lines[index]['quantity'] -= 1
        lines.append(extra)
        statuses = self.status(lines)
        self.assertEqual(statuses[index], statuses[-1])
        self.assertEqual(statuses[-1]['state'], 'accepted')
        extra['quantity'] += 1
        self.assertEqual(self.status(lines)[-1]['state'], 'uncertain')

    def test_legacy_unidentified_line_does_not_hide_confirmed_siblings(self):
        self.run_saga()
        lines = self.snapshot_lines()
        del lines[0]['destination']
        positions = self.status(lines)
        self.assertEqual(positions[0]['state'], 'undocumented')
        self.assertTrue(all(s['state'] == 'accepted' for s in positions[1:]))

    def test_unbound_malformed_or_other_supplier_response_is_not_a_confirmation(self):
        self.run_saga()
        with closing(self.factory()) as db:
            key, raw = db.execute('SELECT child_key, receipt_json FROM shop_fulfillment_positions_v3 LIMIT 1').fetchone()
            receipt = json.loads(raw)
            cases = ['{', json.dumps(dict(receipt, reference='other-position')),
                     json.dumps(dict(receipt, request_hash='another-hash'))]
            for edits in ({'supplier_article_id': 'another-article'}, {'order_number': '<private-data>'},
                          {'quantity': 999}, {'request_bound': False}):
                cases.append(json.dumps(dict(receipt, positions=[dict(receipt['positions'][0], **edits)])))
            for raw_receipt in cases:
                db.execute('UPDATE shop_fulfillment_positions_v3 SET receipt_json=? WHERE child_key=?',
                           (raw_receipt, key))
                db.commit()
                status = next(s for s in self.status() if s['position_key'] == key)
                self.assertEqual((status['state'], status['order_number']), ('uncertain', None))


if __name__ == '__main__':
    unittest.main()
