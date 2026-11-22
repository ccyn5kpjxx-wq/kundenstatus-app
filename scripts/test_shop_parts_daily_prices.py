"""Only local fixture files. No checkout, order, payment or supplier connections."""
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scripts'))
import stage_partner_daily_prices as staging
import test_shop_partner_inquiry_production as fixtures
from shop_parts_daily_prices import FORMAT, POINTER


class PartnerDailyChecks(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.PartnerInquiryProductionChecks()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.install()
        self.root, self.pid = self.fixture.f.root, self.fixture.pid
        self.record = self.fixture.f.descriptor['parts_partner_inquiry_prices']
        self.now = datetime.now(timezone.utc) - timedelta(seconds=2)
        self.data = dict(format=FORMAT, bundle_sha256=self.record['base_bundle_sha256'],
                         manifest_sha256=self.record['sha256'], sources=[['a'*64, 'b'*64, 'c'*64, self.now.isoformat()]],
                         rows={self.pid: [0, 1234, 3]})

    def stage(self, *, dry=False):
        raw = staging.encode(self.data)
        source = self.root / 'testinput.json'
        source.write_bytes(raw)
        staging.stage(SimpleNamespace(release_root=self.root, stdin_b64=False, file=source,
                                      sha256=sha256(raw).hexdigest(), dry_run=dry))

    def test_hot_update_list_and_inquiry_detail_with_rsu_precedence(self):
        catalogue = self.fixture.prepare()['catalogue']
        lookup = catalogue.partner_inquiry_price_lookup.__self__
        self.stage(dry=True)
        self.assertFalse((self.root / POINTER).exists())
        self.stage()
        def selected():
            return catalogue.get(self.pid, now=self.now)
        detail = selected()
        rows = catalogue.page(department='parts', q=detail['name'], now=self.now)['items']
        shown = next(x for x in rows if x['id'] == self.pid)
        self.assertEqual(detail['price'], shown['price'])
        self.assertEqual(detail['purchase_status'], 'inquiry_only')
        self.assertFalse(detail['order_enabled'])
        self.assertFalse(detail['price']['stock_current'])
        gross = detail['price']['prices_by_quantity']
        self.assertEqual((gross['1']['unit_gross_cent'], gross['4']['items_gross_cent']), (1689, 6756))
        self.assertEqual(datetime.fromisoformat(detail['price']['review_at']) - self.now, timedelta(days=1))
        rsu = catalogue.get(self.fixture.rsu_pid)
        self.assertEqual(rsu['price']['source'], 'RSU')
        self.data['sources'][0][3] = (self.now + timedelta(seconds=1)).isoformat()
        self.data['rows'][self.pid] = None
        self.stage()
        lookup.next_read = 0
        self.assertIsNone(selected()['price'])
        self.assertEqual(selected()['purchase_status'], 'inquiry_only')
        self.fixture.assert_no_provider_or_storage()

    def test_old_future_conflicting_incomplete_or_different_scope_cannot_replace_prices(self):
        self.stage()
        path = self.root / POINTER
        saved = path.read_bytes()
        valid = json.dumps(self.data)
        changes = [
            ('rows', {}), ('rows', {self.fixture.rsu_pid: [0, 1000, 2]}),
            ('rows', {self.pid: [0, True, 1]}), ('manifest_sha256', 'd'*64),
            ('sources', [['a'*64, 'b'*64, 'c'*64, (self.now + timedelta(hours=1)).isoformat()]]),
            ('sources', [['a'*64, 'b'*64, 'c'*64, (self.now - timedelta(days=2)).isoformat()]]),
            ('rows', {self.pid: [0, 1235, 3]}),
        ]
        for key, value in changes:
            with self.subTest(key=key):
                self.data = json.loads(valid)
                self.data[key] = value
                with self.assertRaises(ValueError):
                    self.stage()
                self.assertEqual(path.read_bytes(), saved)


if __name__ == '__main__':
    unittest.main()
