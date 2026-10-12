"""Witnessed retrospective WhatsApp batches; synthetic data, no transport."""
from datetime import datetime, timezone
import copy
import json
import sqlite3
import unittest

import test_material_external as external
import test_material_external_restore as restore
import test_materialverwaltung as admin


class ExternalBatchTests(unittest.TestCase):
    def setUp(self):
        self.base = external.ExternalOrderTests('runTest')
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.s,self.p,self.f = self.base.s,self.base.p,self.base.f
        self.views = [self.base.base.photo('Test-Klebeband, ein Karton, nicht dringend'),
                      self.base.base.photo('Test-Klebeband, 3 Stück, nicht dringend',new=True)]
        self.f.time += 60
        self.payload = dict(supplier_id='supplier-1',recipient='+491701234567',subject='Montagsbestellung mit zwei Positionen',
                            sent_at=datetime.fromtimestamp(self.f.time-30,timezone.utc).isoformat(),
                            send_evidence='WhatsApp zeigt zugestellt: synthetische-bestellung.pdf, zwei Seiten',
                            source_key='synthetic-whatsapp-document-1',max_total_cents=25000,confirmed=True)

    def select(self):
        return [(view['id'],view['revision']) for view in self.views]

    def raw(self):
        return [dict(row) for row in self.f.sql('SELECT * FROM einkauf_material_dialoge ORDER BY id')]

    def record(self,**changes):
        return self.s.record_external_batch(self.select(),dict(self.payload,**changes))

    def test_preserves_quantity_identity_originals_and_unknown_prices_without_reservation(self):
        before = self.raw()
        self.p.workshop_orders.supplier['verified'] = False
        result = self.record()
        self.assertEqual((result['count'],result['replayed']),(2,False))
        after = self.raw()
        for original,row in zip(before,after):
            claim = json.loads(row['snapshot_json'])
            self.assertEqual(row['state'],'external_sent')
            self.assertEqual(row['revision'],original['revision']+1)
            for key in ('fields_json','review_json','analysis_json','message_id','created_at','dispatch_id'):
                self.assertEqual(row[key],original[key],key)
            self.assertEqual(claim['kind'],'recorded_external_order')
            self.assertEqual(claim['cap_scope'],'batch')
            self.assertEqual(claim['batch_id'],result['batch_id'])
            self.assertEqual(claim['max_total_cents'],25000)
            fields = json.loads(original['fields_json'])
            for key in ('quantity','unit','urgent'):
                self.assertEqual(claim[key],fields[key]['value'])
            self.assertEqual(claim['article_number'],'')
            self.assertLess(claim['sent_at'],claim['recorded_at'])
            self.assertFalse({'reserved_at','reserved_by','reservation_id','body','unit_price_cents','shipping_cents'} & set(claim))
            self.assertTrue(self.s.status(row['id'])['external_order'])
        self.assertEqual(self.p.workshop_orders.calls,[])
        self.assertEqual(self.f.sql('SELECT * FROM assistent_bestellanforderungen'),[])
        self.assertFalse(self.p.workshop_orders.supplier['verified'])
        audit = self.f.sql('SELECT * FROM assistent_audit')
        self.assertEqual(len(audit),2)
        self.assertTrue(all(row['aktion']=='material_external_sent' for row in audit))

    def test_unknown_shared_limit_is_not_invented(self):
        self.record(max_total_cents=None)
        self.assertTrue(all(json.loads(row['snapshot_json'])['max_total_cents'] is None for row in self.raw()))

    def test_exact_retry_is_idempotent_and_changed_receipt_or_selection_is_rejected(self):
        result = self.record()
        rows = self.raw()
        audits = self.f.sql('SELECT * FROM assistent_audit')
        retry = self.s.record_external_batch(list(reversed(self.select())),self.payload)
        self.assertEqual(retry['batch_id'],result['batch_id'])
        self.assertTrue(retry['replayed'])
        self.assertEqual(self.raw(),rows)
        self.assertEqual(self.f.sql('SELECT * FROM assistent_audit'),audits)
        for changes in ({'recipient':'+491701111111'},{'send_evidence':'Other document'},
                        {'subject':'Other subject'},{'max_total_cents':None},{'source_key':'second-send'}):
            with self.subTest(changes=changes),self.assertRaises(ValueError):
                self.record(**changes)
        with self.assertRaises(ValueError):
            self.s.record_external_batch(self.select()[:1],self.payload)
        self.assertEqual(self.raw(),rows)

    def test_bad_selection_stale_revision_bound_or_removed_member_leave_whole_batch_unchanged(self):
        before = self.raw()
        selections = [[],self.select()+self.select()[:1],[(True,1)],[(self.views[0]['id'],1)],
                      [self.select()[0],(self.views[1]['id'],self.views[1]['revision']+1)]]
        for selected in selections:
            with self.subTest(selection=selected),self.assertRaises(ValueError):
                self.s.record_external_batch(selected,self.payload)
            self.assertEqual(self.raw(),before)
        self.f.sql('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?)',
                   ('synthetic-existing','mitarbeiter:1','material:'+str(self.views[1]['id'])))
        with self.assertRaises(ValueError):self.record()
        self.assertEqual(self.raw(),before)
        self.f.sql('DELETE FROM assistent_bestellanforderungen')
        self.f.sql("UPDATE einkauf_material_dialoge SET state='cancelled' WHERE id=?",(self.views[1]['id'],))
        before = self.raw()
        with self.assertRaises(ValueError):self.record()
        self.assertEqual(self.raw(),before)
        self.assertEqual(self.f.sql('SELECT * FROM assistent_audit'),[])

    def test_audit_failure_rolls_back_every_row(self):
        before = self.raw()
        self.f.sql("""CREATE TRIGGER fail_second_audit BEFORE INSERT ON assistent_audit
            WHEN (SELECT COUNT(*) FROM assistent_audit)>0 BEGIN SELECT RAISE(ABORT,'synthetic audit failure'); END""")
        with self.assertRaises(sqlite3.IntegrityError):self.record()
        self.assertEqual(self.raw(),before)
        self.assertEqual(self.f.sql('SELECT * FROM assistent_audit'),[])

    def test_changed_frozen_content_never_passes_an_idempotent_retry(self):
        self.record()
        row = self.raw()[0]
        fields = json.loads(row['fields_json'])
        fields['quantity']['value'] = '100'
        self.f.sql('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=?',(json.dumps(fields),row['id']))
        before = self.raw()
        with self.assertRaises(ValueError):self.record()
        self.assertEqual(self.raw(),before)

    def test_forwarded_source_or_missing_personal_quantity_rejects_whole_batch(self):
        second = self.raw()[1]
        self.f.sql('UPDATE einkauf_material_nachrichten SET forwarded=1 WHERE id=?',(second['message_id'],))
        before = self.raw()
        with self.assertRaises(PermissionError):self.record()
        self.assertEqual(self.raw(),before)
        self.f.sql('UPDATE einkauf_material_nachrichten SET forwarded=0 WHERE id=?',(second['message_id'],))
        fields = json.loads(second['fields_json'])
        fields.pop('quantity')
        self.f.sql('UPDATE einkauf_material_dialoge SET fields_json=? WHERE id=?',(json.dumps(fields),second['id']))
        before = self.raw()
        with self.assertRaises(ValueError):self.record()
        self.assertEqual(self.raw(),before)
        self.assertEqual(self.f.sql('SELECT * FROM assistent_audit'),[])

    def test_admin_only_timestamp_recipient_cap_and_contact_validation(self):
        before = self.raw()
        for changes in ({'confirmed':False},{'recipient':'orders@example.test'},{'recipient':'491701234567'},
                        {'recipient':'+49170\n12345'},{'supplier_id':'missing'},{'send_evidence':''},
                        {'source_key':'spaces forbidden'},{'max_total_cents':25001},{'max_total_cents':0},
                        {'max_total_cents':True},{'sent_at':'2026-10-12T15:08'},
                        {'sent_at':datetime.fromtimestamp(self.f.time+61,timezone.utc).isoformat()},
                        {'sent_at':datetime.fromtimestamp(before[1]['created_at']-1,timezone.utc).isoformat()},
                        {'quantity':'99'}):
            with self.subTest(changes=changes),self.assertRaises((ValueError,PermissionError)):
                self.record(**changes)
            self.assertEqual(self.raw(),before)
        with self.assertRaises(PermissionError):
            self.s.record_external_batch(self.select(),self.payload,actor='mitarbeiter:1')
        self.assertEqual(self.f.sql('SELECT * FROM assistent_audit'),[])

    def test_already_sent_is_recordable_after_rights_revocation_and_cannot_be_enqueued_or_changed(self):
        self.f.sql('UPDATE assistent_rechte SET einkaufen=0,limit_cent=0 WHERE mitarbeiter_id=1')
        self.record()
        before = self.raw()
        for original in self.views:
            current = self.s.status(original['id'])
            for change in (lambda:self.s.recheck(current['id'],current['revision']),
                           lambda:self.s.approved_order(current['id'],current['revision']),
                           lambda:self.s.remove_requests([(current['id'],current['revision'])],'Doppelt')):
                with self.assertRaises((ValueError,PermissionError)):change()
            with self.assertRaises(PermissionError):self.s.analyze(current['id'])
            self.s.order_attempt(current['id'],current['revision'],{'id':'late','state':'sent'})
        self.s.init_schema()
        for _ in range(3):self.s.process_next()
        self.assertEqual(self.raw(),before)
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_originals_restore_lock_is_held_before_opening_transaction(self):
        original = self.p.get_db
        def guarded():
            self.assertTrue(self.base.lock_held)
            return original()
        self.p.get_db = guarded
        self.addCleanup(setattr,self.p,'get_db',original)
        self.record()


class ExternalBatchRouteTests(unittest.TestCase):
    def setUp(self):
        self.base = admin.MaterialAdminTests('runTest')
        self.base.setUp()
        self.client,self.service = self.base.client,self.base.service
        self.url = '/admin/assistent-bestellungen/eingang/material/extern-sammelbestellung-nachtragen'
        self.form = dict(csrf_token='synthetic-csrf',selection=['7:3','8:2'],supplier_id='supplier',
                         recipient='+491701234567',subject='Batch',sent_at='2026-10-12T15:08',
                         send_evidence='Synthetic document delivered',source_key='document-1',max_total='250,00',confirmed='ja')
        self.service.record_external_batch.return_value = dict(count=2,batch_id='a'*32,sent_at='2026-10-12T13:08:00+00:00')

    def test_authenticated_csrf_bound_route_uses_berlin_and_never_sends(self):
        self.assertEqual(self.client.post(self.url,data=dict(self.form,csrf_token='wrong')).status_code,400)
        self.service.record_external_batch.assert_not_called()
        response = self.client.post(self.url,data=dict(self.form,actor='mitarbeiter:99',quantity='999'))
        args,kwargs = self.service.record_external_batch.call_args
        self.assertEqual(args[0],[(7,3),(8,2)])
        self.assertEqual(args[1]['sent_at'],'2026-10-12T15:08:00+02:00')
        self.assertEqual(args[1]['max_total_cents'],25000)
        self.assertNotIn('actor',args[1])
        self.assertNotIn('quantity',args[1])
        self.assertEqual(kwargs,{'actor':'admin'})
        self.assertEqual(response.status_code,303)
        self.assertIn('archive_day=sent:2026-10-12',response.location)
        self.assertTrue(response.location.endswith('#versandte-bestellungen'))
        self.assertEqual(response.headers['Cache-Control'],'no-store')
        self.service.process_next.assert_not_called()
        self.service.send_question.assert_not_called()
        with self.client.session_transaction() as state:
            state.clear();state['csrf_token']='synthetic-csrf'
        self.assertEqual(self.client.post(self.url,data=self.form).status_code,403)

    def test_invalid_selection_confirmation_date_or_limit_does_not_submit(self):
        for changes in ({'selection':[]},{'selection':['7:bad']},{'confirmed':''},{'sent_at':'not a date'},
                        {'sent_at':'2026-10-25T02:30'},{'max_total':'0x250'}):
            with self.subTest(changes=changes):
                self.assertEqual(self.client.post(self.url,data=dict(self.form,**changes)).status_code,303)
        self.service.record_external_batch.assert_not_called()
        self.client.post(self.url,data=dict(self.form,max_total=''))
        self.assertIsNone(self.service.record_external_batch.call_args.args[1]['max_total_cents'])


class ExternalBatchRestoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        restore.ExternalRestoreTests.setUpClass()

    def test_old_backup_cannot_remove_retrospective_batch_or_receipt(self):
        base = restore.ExternalRestoreTests('runTest')
        base.setUp()
        self.addCleanup(base.doCleanups)
        before = base.export()
        base.seed_claim('external_sent')
        with restore.connection(base.db_path) as db:
            snapshot = dict(kind='recorded_external_order',batch_id='a'*32,source_key='document-1',
                            sent_at='2026-10-12T13:08:00+00:00',recorded_at='2026-10-12T14:00:00+00:00',
                            send_evidence='Synthetic WhatsApp receipt')
            db.execute('UPDATE einkauf_material_dialoge SET snapshot_json=?',(json.dumps(snapshot),))
        base.assert_json_rejected_unchanged(before)
        current = base.export()
        missing_audit = copy.deepcopy(current)
        missing_audit['tables']['assistent_audit'] = missing_audit['tables']['assistent_audit'][:1]
        base.assert_json_rejected_unchanged(missing_audit)


if __name__ == '__main__':
    unittest.main()
