"""Offline MOS onsite pickup, cancellation and deposit repayment checks."""
from contextlib import contextmanager
from datetime import timedelta
from hashlib import sha256
import json
from types import SimpleNamespace
import unittest

import test_mos_handover as handover_fixtures
import mos_return
from mos_booking.production import RefundLedger, init_refund_schema


class OnsiteOperations(unittest.TestCase):
    def setUp(self):
        self.fixture = handover_fixtures.HandoverTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        self.now = self.fixture.now
        self.db.execute('ALTER TABLE miet_checkout_holds ADD COLUMN session_id TEXT')
        self.db.execute('ALTER TABLE miet_checkout_holds ADD COLUMN grund TEXT')
        self.db.execute('ALTER TABLE mietvorgaenge ADD COLUMN geaendert_am TEXT')
        self.db.execute('DROP TABLE miet_checkout_cancellations')
        init_refund_schema(self.db)
        mos_return.init_schema(self.db)
        self.quote = dict(self.fixture.quote, days=2, daily_cents=3900, rental_cents=7800,
                          deposit_cents=50000, deposit_authorized_cents=0, deposit_charged_cents=0,
                          amount_cents=0, checkout_deposit=False, payment_method='onsite',
                          deposit_method='pay_at_pickup', included_km=300, extra_km_cents=25,
                          cancellation_policy='free_48h_then_10pct_rent')
        self.db.execute("UPDATE miet_checkout_holds SET payment_intent=NULL, payload=?",
                        (json.dumps({'quote': self.quote}),))
        self.db.execute('DELETE FROM miet_checkout_deposit_auths')
        contract = dict(self.fixture.contract, **self.quote)
        canonical = json.dumps(contract, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        self.db.execute('UPDATE miet_checkout_contracts SET contract_json=?,contract_sha256=?',
                        (canonical, sha256(canonical.encode()).hexdigest()))

        @contextmanager
        def locked(vehicle_id):
            self.assertEqual(vehicle_id, 3)
            with self.db:
                yield self.db, None

        self.ledger = RefundLedger(SimpleNamespace(
            read=lambda hid: dict(self.db.execute('SELECT * FROM miet_checkout_holds WHERE id=?',(hid,)).fetchone()),
            locked=locked, p=SimpleNamespace(now_str=lambda: self.now.isoformat())))
        self.db.commit()

    def collect(self, **overrides):
        fields = dict(onsite_rent_received=True, onsite_deposit_received=True, onsite_receipt_ref='PAY-123')
        fields.update(overrides)
        with self.db:
            return self.fixture.handover(**fields)

    def repay(self, **overrides):
        fields = dict(confirmed=True, operator_name='Test Team', receipt_ref='REF-123',
                      now=self.fixture.end + timedelta(hours=1))
        fields.update(overrides)
        with self.db:
            return mos_return.record_onsite_repayment(self.db,'h1',**fields)

    def record_return(self, clear):
        returned_at = self.fixture.end
        return mos_return.record(self.db,'h1', returned_at=returned_at, operator_name='Test Team',
            odometer_km=12400, fuel_full=True, condition_recorded=True, damage_free=clear,
            charges_resolved=clear, no_objection=clear, protocol_ref='RET-123',
            note='' if clear else 'Zustand prüfen', now=returned_at)

    def test_both_receipts_before_keys_and_one_audited_payout(self):
        for invalid in [dict(onsite_rent_received=False), dict(onsite_deposit_received=False), dict(onsite_receipt_ref='')]:
            with self.assertRaises(ValueError):
                self.collect(**invalid)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM miet_checkout_onsite_payments').fetchone()[0], 0)
        self.collect()
        self.collect(onsite_receipt_ref='SECOND')
        pay = self.db.execute('SELECT * FROM miet_checkout_onsite_payments').fetchone()
        self.assertEqual((pay['receipt_ref'], pay['rental_cents'], pay['deposit_cents']), ('PAY-123',7800,50000))
        with self.assertRaises(ValueError):
            self.ledger.cancel('h1',requested_at=self.now)
        with self.assertRaises(ValueError):
            self.repay()
        self.record_return(True)
        self.db.commit()
        with self.assertRaises(ValueError):
            self.repay(confirmed=False)
        payout = self.repay()
        self.assertEqual(payout['amount_cents'],50000)
        self.assertEqual(self.repay(receipt_ref='SECOND'), payout)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM miet_checkout_refunds').fetchone()[0],0)

    def test_disputed_return_requires_separate_clearance(self):
        self.collect()
        self.record_return(False)
        self.db.commit()
        with self.assertRaises(ValueError):
            self.repay()
        mos_return.record_clearance(self.db,'h1', operator_name='Test Team',evidence_ref='CHECK-1',
            note='Schaden vollständig geklärt',fuel_resolved=True,damage_resolved=True,
            time_km_resolved=True,now=self.fixture.end + timedelta(minutes=15))
        self.assertEqual(self.repay()['receipt_ref'],'REF-123')

    def test_unpaid_cancellation_releases_vehicle_and_no_provider_refund(self):
        before = self.fixture.start - timedelta(hours=1)
        for _ in range(2):
            self.assertIsNone(self.ledger.cancel('h1',requested_at=before))
        self.assertEqual(tuple(self.db.execute('SELECT status,grund FROM miet_checkout_holds').fetchone()),
                         ('released','onsite_cancelled'))
        self.assertEqual(self.db.execute('SELECT status FROM mietvorgaenge').fetchone()[0],'storniert')
        self.assertEqual(self.db.execute('SELECT fee_cents FROM miet_checkout_cancellations').fetchone()[0],780)
        for _ in range(2):
            self.assertIsNone(self.ledger.credit('h1',180,'Ersparnis','REQ-1'))
        self.assertEqual(self.db.execute('SELECT credit_cents FROM miet_checkout_cancellations').fetchone()[0],180)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM miet_checkout_refunds').fetchone()[0],0)


if __name__ == '__main__':
    unittest.main()
