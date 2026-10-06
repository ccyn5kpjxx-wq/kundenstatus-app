"""Material handoff transaction/replay tests: temporary DB and fake mail only."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import unittest
from unittest.mock import patch

import test_bestellungen as fixtures
from test_bestellausgang import order
from werkstatt_bestellungen import _material_permit


class StoredDialog:
    """Contract double whose approval and revocation live in the fixture DB."""
    def __init__(self, manager):
        self.manager = manager
        self.after_approve = None
        self.fail_ack = False
        self.guarded = []
        with manager.db() as db:
            db.execute('CREATE TABLE test_material(id INTEGER PRIMARY KEY,revision INTEGER,actor TEXT,payload TEXT,active INTEGER,dispatch_id TEXT)')
            db.commit()

    def add(self, payload, draft=1, actor='mitarbeiter:7'):
        with self.manager.db() as db:
            db.execute('INSERT INTO test_material VALUES(?,1,?,?,1,?)',(draft,actor,json.dumps(payload),''))
            db.commit()

    def revoke(self):
        with self.manager.db() as db:
            db.execute('UPDATE test_material SET active=0')
            db.commit()

    def approved_order(self, draft, revision):
        with self.manager.db() as db:
            result = self.guard_order(db,draft,revision)
        if self.after_approve:
            self.after_approve()
        return result

    def guard_order(self, db, draft_id, revision=None, actor=None, request_key=None, intent=None):
        self.guarded.append((id(db),db.in_transaction,intent is not None))
        row = db.execute('SELECT * FROM test_material WHERE id=?',(draft_id,)).fetchone()
        if not row or not row['active'] or (revision is not None and revision != row['revision']):
            raise PermissionError('Materialfreigabe widerrufen oder geändert.')
        key = 'material:'+str(draft_id)
        payload = json.loads(row['payload'])
        if actor is not None and actor != row['actor'] or request_key is not None and request_key != key:
            raise PermissionError('Falscher Besteller oder Schlüssel.')
        if intent is not None and self.manager.dispatch._intent(payload,key) != intent:
            raise PermissionError('Materialinhalt geändert.')
        return dict(draft_id=draft_id,revision=row['revision'],actor=row['actor'],request_key=key,payload=payload,fingerprint='fixture')

    def order_attempt(self, draft, revision, result):
        if self.fail_ack:
            raise OSError('Simulierter Absturz nach Queue-Commit')
        with self.manager.db() as db:
            row = db.execute('SELECT * FROM test_material WHERE id=?',(draft,)).fetchone()
            if row['dispatch_id'] and row['dispatch_id'] != result.get('id'):
                raise ValueError('Neue Bestell-ID verboten.')
            db.execute('UPDATE test_material SET dispatch_id=? WHERE id=?',(result.get('id') or '',draft))
            db.commit()


class MaterialBridgeTests(unittest.TestCase):
    post = fixtures.ManagementTests.post
    nonce = fixtures.ManagementTests.nonce
    ready = fixtures.ManagementTests.ready
    def setUp(self):
        fixtures.ManagementTests.setUp(self)
        self.contact = self.ready(worker=True)
        self.manager.set_setting('max_total_cents',25000)
        with self.manager.db() as db:
            db.execute('CREATE TABLE mitarbeiter(id INTEGER PRIMARY KEY,aktiv INTEGER)')
            db.execute('INSERT INTO mitarbeiter VALUES(7,1)')
            db.execute('CREATE TABLE assistent_rechte(mitarbeiter_id INTEGER PRIMARY KEY,lesen INTEGER,einkaufen INTEGER,limit_cent INTEGER,version INTEGER)')
            db.execute('INSERT INTO assistent_rechte VALUES(7,1,1,25000,1)')
            db.commit()
        self.dialog = self.portal.material_dialog = StoredDialog(self.manager)
        self.portal.workshop_orders = self.manager

    def payload(self, **changes):
        values = dict(supplier_id=self.contact,recipient='orders@supplier.example',extra_costs_cents=0,
                      price_source='Aktuell geprüftes Angebot 2026-10-05')
        values.update(changes)
        return order(**values)

    def add(self,draft=1,**changes):
        self.dialog.add(self.payload(**changes),draft)

    def rows(self):
        with self.manager.db() as db:
            return [dict(row) for row in db.execute('SELECT * FROM assistent_bestellanforderungen')]

    def test_server_snapshot_sends_without_request_or_fake_session_once(self):
        self.add(urgent=True)
        first = self.manager.submit_material_request(1,1)
        self.assertEqual(first['state'],'sent')
        self.assertEqual(first['order']['expected_total_cents'],2500)
        self.assertEqual(self.smtp.data_calls,1)
        self.assertIsNone(_material_permit.get())
        self.assertEqual(first['id'],self.manager.submit_material_request(1,1)['id'])
        self.assertEqual(self.smtp.data_calls,1)
        self.assertTrue(any(in_tx and has_intent for _,in_tx,has_intent in self.dialog.guarded))
        self.assertEqual(self.rows()[0]['request_id'],'material:1')

    def test_crash_after_queue_commit_replays_same_id_and_sends_once(self):
        self.add(urgent=True)
        self.dialog.fail_ack = True
        first = self.manager.submit_material_request(1,1)
        self.assertTrue(first['id'])
        self.assertTrue(first['needs_review'])
        self.assertEqual(self.smtp.data_calls,0)
        self.dialog.fail_ack = False
        recovered = self.manager.submit_material_request(1,1)
        self.assertEqual(recovered['id'],first['id'])
        self.assertEqual(recovered['state'],'sent')
        self.assertEqual(len(self.rows()),1)
        self.assertEqual(self.smtp.data_calls,1)

    def test_crash_then_revoked_source_preserves_durable_order_identity(self):
        self.add(urgent=True)
        self.dialog.fail_ack = True
        first = self.manager.submit_material_request(1,1)
        self.dialog.fail_ack = False
        self.dialog.revoke()
        replay = self.manager.submit_material_request(1,1)
        self.assertEqual(replay['id'],first['id'])
        self.assertTrue(replay['needs_review'])
        self.assertEqual(len(self.rows()),1)
        self.manager.tick()
        self.assertEqual(self.manager.dispatch.status(first['id'])['state'],'blocked')
        self.assertEqual(self.smtp.data_calls,0)

    def test_changed_revision_cannot_create_another_order(self):
        self.add(urgent=True)
        self.dialog.fail_ack = True
        first = self.manager.submit_material_request(1,1)
        with self.manager.db() as db:
            db.execute('UPDATE test_material SET revision=2,payload=? WHERE id=1',(json.dumps(self.payload(urgent=True,article_number='OTHER')),))
            db.commit()
        self.dialog.fail_ack = False
        replay = self.manager.submit_material_request(1,2)
        self.assertEqual(replay['id'],first['id'])
        self.assertTrue(replay['needs_review'])
        self.assertEqual(len(self.rows()),1)
        self.assertEqual(self.rows()[0]['id'],first['id'])
        self.assertEqual(self.smtp.data_calls,0)

    def test_revocation_between_snapshot_and_reservation_rolls_back(self):
        self.add(urgent=True)
        self.dialog.after_approve = self.dialog.revoke
        result = self.manager.submit_material_request(1,1)
        self.assertEqual(result['state'],'blocked')
        self.assertEqual(self.rows(),[])
        self.assertIsNone(_material_permit.get())
        self.assertEqual(self.smtp.data_calls,0)

    def test_rights_revoked_between_snapshot_and_reservation(self):
        self.add(urgent=True)
        def revoke():
            with self.manager.db() as db:
                db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=7')
                db.commit()
        self.dialog.after_approve = revoke
        self.assertEqual(self.manager.submit_material_request(1,1)['state'],'blocked')
        self.assertEqual(self.rows(),[])

    def test_rights_or_draft_revoked_after_acceptance_prevent_smtp(self):
        for mode in ('rights','draft'):
            with self.subTest(mode=mode):
                self.add(draft=1 if mode=='rights' else 2,urgent=False)
                result = self.manager.submit_material_request(1 if mode=='rights' else 2,1)
                if mode=='rights':
                    with self.manager.db() as db:
                        db.execute('UPDATE assistent_rechte SET einkaufen=0')
                        db.commit()
                else:
                    self.dialog.revoke()
                self.now = datetime(2026,9,28,12,tzinfo=timezone.utc)
                self.manager.tick()
                self.assertEqual(self.manager.dispatch.status(result['id'])['state'],'blocked')
                self.assertEqual(self.smtp.data_calls,0)
                with self.manager.db() as db:
                    db.execute('UPDATE assistent_rechte SET einkaufen=1')
                    db.execute('UPDATE test_material SET active=1')
                    db.commit()
                self.now = datetime(2026,9,27,10,tzinfo=timezone.utc)
                self.manager.set_setting('worker_last_ok',self.now.timestamp())

    def test_verified_price_and_explicit_costs_are_required(self):
        for index,changes in enumerate(({'price_verified':False},{'price_basis':'net'}, {'shipping_cents':None},
                       {'extra_costs_cents':None},{'price_source':''},{'max_total_cents':2499}),1):
            self.add(draft=index,urgent=True,**changes)
            self.assertEqual(self.manager.submit_material_request(index,1)['state'],'blocked',changes)
        self.assertEqual(self.rows(),[])
        self.assertEqual(self.smtp.data_calls,0)

    def test_shared_monday_cap_counts_frozen_legacy_noon_requests(self):
        self.add(urgent=False,unit_price_cents=7500,shipping_cents=0,max_total_cents=15000)
        first = self.manager.submit_material_request(1,1)
        old = datetime(2026,9,28,10,tzinfo=timezone.utc).timestamp()
        with self.manager.db() as db:
            db.execute('UPDATE assistent_bestellanforderungen SET schedule_version=1,legacy_due_at=NULL,due_at=? WHERE id=?',(old,first['id']))
            db.commit()
        with patch.object(self.manager.dispatch,'migrate_weekly_schedule',return_value=None):
            self.manager.dispatch._freeze_due_batches(datetime.fromtimestamp(old,timezone.utc))
        self.add(draft=2,urgent=False,unit_price_cents=6000,shipping_cents=0,max_total_cents=12000)
        second = self.manager.submit_material_request(2,1)
        self.assertEqual(second['state'],'blocked')
        self.assertEqual(len(self.rows()),1)
        self.assertEqual(self.smtp.data_calls,0)

    def test_already_sent_legacy_noon_order_still_counts_for_same_monday(self):
        self.add(urgent=False,unit_price_cents=7500,shipping_cents=0,max_total_cents=15000)
        first = self.manager.submit_material_request(1,1)
        self.now = datetime(2026,9,28,10,tzinfo=timezone.utc)
        with self.manager.db() as db:
            db.execute('UPDATE assistent_bestellanforderungen SET schedule_version=1,legacy_due_at=NULL,due_at=? WHERE id=?',(self.now.timestamp(),first['id']))
            db.commit()
        with patch.object(self.manager.dispatch,'migrate_weekly_schedule',return_value=None):
            self.manager.tick()
        self.assertEqual(self.manager.dispatch.status(first['id'])['state'],'sent')
        self.manager.set_setting('worker_last_ok',self.now.timestamp())
        self.add(draft=2,urgent=False,unit_price_cents=6000,shipping_cents=0,max_total_cents=12000)
        self.assertEqual(self.manager.submit_material_request(2,1)['state'],'blocked')
        self.assertEqual(len(self.rows()),1)
        self.assertEqual(self.smtp.data_calls,1)

    def test_all_in_250_eur_cap_and_personal_cap(self):
        self.add(urgent=True,unit_price_cents=12000,shipping_cents=1000,max_total_cents=25000)
        self.assertEqual(self.manager.submit_material_request(1,1)['state'],'sent')
        self.add(draft=2,urgent=True,unit_price_cents=12000,shipping_cents=1001,max_total_cents=25001)
        self.assertEqual(self.manager.submit_material_request(2,1)['state'],'blocked')
        with self.manager.db() as db:
            db.execute('UPDATE assistent_rechte SET limit_cent=1000')
            db.commit()
        self.add(draft=3,urgent=True)
        self.assertEqual(self.manager.submit_material_request(3,1)['state'],'blocked')
        self.assertEqual(self.smtp.data_calls,1)

    def test_weekly_waits_for_fourteen_and_unverified_sender_never_sends(self):
        self.add(urgent=False)
        result = self.manager.submit_material_request(1,1)
        self.assertEqual(result['due_at'],'2026-09-28T12:00:00+00:00')
        self.now = datetime(2026,9,28,10,tzinfo=timezone.utc)
        self.manager.tick()
        self.assertEqual(self.smtp.data_calls,0)
        self.portal.smtp['from_address'] = 'different@example.test'
        self.now += timedelta(hours=2)
        self.manager.tick()
        self.assertEqual(self.manager.dispatch.status(result['id'])['state'],'blocked')
        self.assertEqual(self.smtp.data_calls,0)

    def test_context_does_not_authorize_unrelated_dispatch(self):
        self.add(urgent=True)
        self.assertEqual(self.manager.submit_material_request(1,1)['state'],'sent')
        with self.assertRaises(PermissionError):
            self.manager.dispatch.enqueue(self.payload(urgent=True),'mitarbeiter:7','material:2')
        self.assertEqual(len(self.rows()),1)

    def test_restore_without_any_order_tables_is_safe_and_idempotent(self):
        with self.manager.db() as db:
            for table in ('assistent_bestellanforderungen','assistent_bestellpakete','assistent_bestellkontakte','assistent_bestellkonfiguration'):
                db.execute('DROP TABLE '+table)
            db.commit()
        self.portal.workshop_orders_init_schema()
        self.portal.workshop_orders_init_schema()
        self.assertEqual(self.manager.dispatch.list_orders(),[])
        self.assertEqual(self.manager.contacts(),[])
        self.assertEqual(self.manager.cap(),0)
        self.assertEqual(self.smtp.data_calls,0)


if __name__ == '__main__':
    unittest.main()
