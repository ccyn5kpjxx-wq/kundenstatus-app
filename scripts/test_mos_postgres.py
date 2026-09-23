"""Real PostgreSQL acceptance; always creates a new synthetic database on loopback.

Pass a private JSON connection file for a dedicated local test cluster. Never
accept DATABASE_URL or connect to a remote/operational database. Leaves the test
database for inspection; no existing database is dropped or modified.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--connection-file', required=True)
    args = parser.parse_args()
    cfg = json.loads(Path(args.connection_file).read_text(encoding='utf-8'))
    if (set(cfg) != {'host', 'port', 'user', 'password', 'dbname'}
        or cfg['host'] != '127.0.0.1' or cfg['port'] != 55439
        or cfg['user'] != 'mos_test_admin' or cfg['dbname'] != 'postgres'):
        raise SystemExit('Requires the dedicated loopback MOS test cluster on port 55439.')
    import psycopg
    from psycopg import sql
    name = 'mos_acceptance_' + uuid.uuid4().hex
    with psycopg.connect(**cfg, autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    cfg['dbname'] = name
    # Reuse the actual portal integration suite. Its bootstrap is isolated and
    # blocks external Python sockets; libpq only targets the validated loopback.
    spec = importlib.util.spec_from_file_location('inventory_suite', ROOT/'scripts/test_mietwagen_checkout.py')
    suite_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite_module)
    portal = suite_module.portal
    portal.USE_POSTGRES = True
    portal.get_db = lambda: portal.PostgresConnection(psycopg.connect(**cfg))
    portal.init_db()
    from mos_booking.production import RefundLedger, init_refund_schema
    from mos_public_booking import init_slot_schema
    db = portal.get_db()
    init_refund_schema(db)
    db.commit()
    db.close()

    class RefundTests(suite_module.InventoryTests):
        # Only load the methods defined below; stock tests run separately.
        def test_postgres_public_slot_close_blocks_reserve_and_checkout(self):
            start = (datetime.now(timezone.utc)+timedelta(days=5)).replace(second=0,microsecond=0)
            end = start+timedelta(days=1)
            a,b=start.isoformat(),end.isoformat()
            q=dict(self.quote,start_slot=a,end_slot=b,slot_policy='db_open_slots_v1')
            db=portal.get_db()
            init_slot_schema(db,[a,b]);db.commit();db.close()
            db=portal.get_db()
            db.execute('UPDATE miet_checkout_slots SET active=0 WHERE slot=?',(a,));db.commit();db.close()
            with self.assertRaisesRegex(ValueError,'nicht mehr freigegeben'):
                self.s.reserve(uuid.uuid4().hex,self.vid,start.date().isoformat(),
                               end.date().isoformat(),self.customer,q)
            db=portal.get_db()
            db.execute('UPDATE miet_checkout_slots SET active=1 WHERE slot=?',(a,));db.commit();db.close()
            h=self.s.reserve(uuid.uuid4().hex,self.vid,start.date().isoformat(),
                             end.date().isoformat(),self.customer,q)
            db=portal.get_db()
            db.execute('UPDATE miet_checkout_slots SET active=0 WHERE slot=?',(a,));db.commit();db.close()
            with self.assertRaisesRegex(ValueError,'nicht mehr freigegeben'):
                self.s.create_checkout(h['id'])
            self.assertIsNone(self.s.read(h['id'])['session_id'])

        def test_postgres_atomic_rollback(self):
            h=self.hold(); session=self.s.create_checkout(h['id'])
            self.gateway.pay(session['id'])
            body,signature=self.gateway.signed_event(session['id'])
            db=portal.get_db()
            db.execute("""CREATE FUNCTION reject_confirmation() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN
                IF NEW.status='confirmed' THEN RAISE EXCEPTION 'test rollback'; END IF;
                RETURN NEW; END $$""")
            db.execute('CREATE TRIGGER fail_confirmation BEFORE UPDATE ON miet_checkout_holds FOR EACH ROW EXECUTE FUNCTION reject_confirmation()')
            db.commit();db.close()
            try:
                with self.assertRaises(psycopg.errors.RaiseException):
                    self.s.handle_signed_event(body,signature,self.secret)
                self.assertEqual(self.count(),0)
                db=portal.get_db()
                self.assertEqual(db.execute('SELECT COUNT(*) AS n FROM miet_checkout_events WHERE hold_id=?',(h['id'],)).fetchone()['n'],0)
                db.close()
            finally:
                db=portal.get_db()
                db.execute('DROP TRIGGER fail_confirmation ON miet_checkout_holds')
                db.execute('DROP FUNCTION reject_confirmation()')
                db.commit();db.close()
            self.s.handle_signed_event(body,signature,self.secret)
            self.assertEqual(self.count(),1)

        def test_postgres_refund_after_cancel(self):
            start = datetime.now(timezone.utc) + timedelta(days=10)
            q = dict(self.quote, amount_cents=64700, rental_cents=14700,
                     deposit_charged_cents=50000, daily_cents=4900,
                     start_slot=start.isoformat(), end_slot=(start+timedelta(days=3)).isoformat())
            h = self.s.reserve(uuid.uuid4().hex, self.vid, start.date().isoformat(),
                               (start+timedelta(days=3)).date().isoformat(), self.customer, q)
            self.event(h)
            ledger = RefundLedger(self.s)
            rid = ledger.cancel(h['id'], start-timedelta(days=2))
            self.assertEqual(ledger.process(rid), 'succeeded')
            self.assertEqual(ledger.process(rid), 'succeeded')
            self.assertEqual(ledger.cancel(h['id'], start-timedelta(days=2)), rid)
            db = portal.get_db()
            row = db.execute('SELECT amount_cents,status FROM miet_checkout_refunds WHERE id=?', (rid,)).fetchone()
            self.assertEqual(row['amount_cents'], 64700)
            self.assertEqual(row['status'], 'succeeded')
            db.close()

    names = unittest.defaultTestLoader.getTestCaseNames(suite_module.InventoryTests)
    # This one fixture directly uses SQLite PRAGMA; the separate competing
    # request acceptance test still exercises the real acceptance writer.
    names.remove('test_acceptance_route_cannot_bypass_hold')
    names.remove('test_fulfilment_failure_rolls_back_rental_and_event')
    suite = unittest.TestSuite(suite_module.InventoryTests(n) for n in names)
    suite.addTest(RefundTests('test_postgres_refund_after_cancel'))
    suite.addTest(RefundTests('test_postgres_atomic_rollback'))
    suite.addTest(RefundTests('test_postgres_public_slot_close_blocks_reserve_and_checkout'))
    print('Synthetic acceptance database:', name, flush=True)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        # Connection exceptions can contain connection data. Print no DSNs.
        print('Acceptance failed:', type(exc).__name__, file=sys.stderr)
        import traceback
        for frame in traceback.extract_tb(exc.__traceback__):
            print(Path(frame.filename).name, frame.lineno, frame.name, file=sys.stderr)
        if type(exc).__name__ in {'UndefinedTable','UndefinedColumn','SyntaxError'}:
            print(exc.diag.message_primary, file=sys.stderr)
        raise SystemExit(1)
