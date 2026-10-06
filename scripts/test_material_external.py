"""One external mail claim, synthetic DB only; no transport or app workers."""
from contextlib import contextmanager
from datetime import datetime, timezone
import copy
import json
import sqlite3
import unittest
from unittest.mock import patch

import test_materialdialog as fixtures


class ExternalOrderTests(unittest.TestCase):
    def setUp(self):
        self.base = fixtures.DialogTests('runTest')
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.s,self.p,self.f = self.base.s,self.base.p,self.base.f
        self.lock_held = False
        @contextmanager
        def operation_lock():
            self.assertFalse(self.lock_held)
            self.lock_held = True
            try:
                yield
            finally:
                self.lock_held = False
        self.p.portal_originals_operation_lock = operation_lock
        self.f.sql('''CREATE TABLE assistent_audit(id INTEGER PRIMARY KEY AUTOINCREMENT,actor TEXT NOT NULL,
            auftrag_id INTEGER,aktion TEXT NOT NULL,details TEXT NOT NULL,zeit TEXT NOT NULL)''')
        self.payload = dict(supplier_id='supplier-1',recipient='orders@example.test',
            product_name='Test Crystal Silver',article_number='TEST-4000',variant='TEST/E0.5, 0,5 Liter',
            subject='Dringende Bestellung M-1: Test Crystal Silver',recipient_source='Synthetische Lieferantenkorrespondenz',
            authorization_note='Expliziter Einzelauftrag bis 250 Euro einschließlich aller Nebenkosten',
            max_total_cents=25000,confirmed=True)

    def reserve(self,view=None):
        view = view or self.base.crystal_photo()
        return self.s.reserve_external(view['id'],view['revision'],self.payload)

    def raw(self,view):
        return self.f.sql('SELECT * FROM einkauf_material_dialoge WHERE id=?',(view['id'],))[0]

    def proof(self,view):
        external=view['external_order']
        return dict(reservation_id=external['reservation_id'],recipient=external['recipient'],subject=external['subject'],
            sent_at=datetime.fromtimestamp(self.f.time,timezone.utc).isoformat(),
            send_evidence='Synthetischer Gesendet-Beleg mail-123',confirmed=True)

    def test_reservation_is_atomic_price_free_and_only_confirms_this_contact(self):
        self.p.workshop_orders.supplier['verified']=False
        view=self.base.crystal_photo()
        original=self.raw(view)
        original_db=self.p.get_db
        def guarded_db():
            self.assertTrue(self.lock_held, 'Restore mutex must precede DB opening')
            return original_db()
        with patch.object(self.p,'get_db',side_effect=guarded_db):
            reserved=self.reserve(view)
        row=self.raw(reserved)
        self.assertEqual(reserved['state'],'external_pending')
        self.assertEqual(reserved['revision'],view['revision']+1)
        self.assertEqual(row['fields_json'],original['fields_json'])
        self.assertEqual(row['review_json'],'{}')
        self.assertEqual(row['dispatch_id'],'')
        self.assertFalse(self.p.workshop_orders.supplier['verified'])
        self.assertEqual(self.f.sql('SELECT * FROM assistent_bestellanforderungen'),[])
        external=reserved['external_order']
        self.assertEqual((external['quantity'],external['unit'],external['urgent']),('1','Stück',True))
        for clause in ('250,00 EUR','Mehrwertsteuer, Versand und aller Nebenkosten','nicht ausführen','Keine Ersatzartikel','Liefertermin bestätigen'):
            self.assertIn(clause,external['body'])
        self.assertNotIn('unit_price_cents',external)
        audit=self.f.sql('SELECT * FROM assistent_audit')
        self.assertEqual(len(audit),1)
        self.assertEqual(json.loads(audit[0]['details'])['snapshot_hash'],row['snapshot_hash'])
        self.assertTrue(all(q['state']=='superseded' for q in reserved['questions']))

    def test_missing_authorization_wrong_recipient_invalid_cap_or_overridden_quantity_leave_no_claim(self):
        view=self.base.crystal_photo()
        original=self.raw(view)
        for change in ({'confirmed':False},{'recipient':'other@example.test'},{'recipient':'orders@example.test\r\nBcc:x@y.test'},
                       {'max_total_cents':25001},{'max_total_cents':0},{'max_total_cents':True},
                       {'authorization_note':''},{'quantity':'100'},{'supplier_id':'missing'}):
            with self.subTest(change=change),self.assertRaises((ValueError,PermissionError)):
                self.s.reserve_external(view['id'],view['revision'],dict(self.payload,**change))
            self.assertEqual(self.raw(view),original)
        with self.assertRaises(PermissionError):
            self.s.reserve_external(view['id'],view['revision'],self.payload,actor='mitarbeiter:1')
        self.assertEqual(self.f.sql('SELECT * FROM assistent_audit'),[])

    def test_stale_employee_missing_fields_and_personal_limit_cannot_reserve(self):
        view=self.base.crystal_photo('Dringend')
        with self.assertRaises(ValueError):self.reserve(view)
        self.base.answer(view,'Ein Stück')
        with self.assertRaises(ValueError):self.reserve(view)
        current=self.s.status(view['id'])
        self.f.sql('UPDATE assistent_rechte SET limit_cent=24999 WHERE mitarbeiter_id=1')
        with self.assertRaises(PermissionError):self.reserve(current)
        self.f.sql('UPDATE assistent_rechte SET limit_cent=25000,einkaufen=0 WHERE mitarbeiter_id=1')
        with self.assertRaises(PermissionError):self.reserve(current)
        self.assertEqual(self.f.sql('SELECT * FROM assistent_audit'),[])

    def test_automatic_durable_queue_wins_before_manual_claim(self):
        view=self.base.photo()
        view=self.base.review(view)
        snapshot=self.s.approved_order(view['id'],view['revision'])
        self.f.sql('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?)',('synthetic-existing',snapshot['actor'],snapshot['request_key']))
        with self.assertRaises(ValueError):self.reserve(view)
        self.assertEqual(self.s.status(view['id'])['state'],'approved')
        self.assertEqual(self.f.sql('SELECT * FROM assistent_audit'),[])

    def test_external_claim_blocks_every_automatic_or_employee_change_and_survives_init(self):
        view=self.reserve()
        for completed in (False,True):
            if completed:
                view=self.s.record_external_sent(view['id'],view['revision'],self.proof(view))
            original=self.raw(view)
            with self.assertRaises(ValueError):self.reserve(view)
            with self.assertRaises(ValueError):self.base.review(view)
            with self.assertRaises(ValueError):self.s.recheck(view['id'],view['revision'])
            with self.assertRaises(PermissionError):self.s.approved_order(view['id'],view['revision'])
            self.assertEqual(self.base.answer(view,'Zwei Stück, nicht dringend')['state'],'review')
            self.s.analyze(view['id'])
            self.s.order_attempt(view['id'],1,{'state':'blocked'})
            self.s.order_attempt(view['id'],1,{'id':'fictional','state':'sent'})
            self.s.ensure_draft(view['message_id'])
            self.s.init_schema()
            for _ in range(3):self.s.process_next()
            self.assertEqual(self.raw(view),original)
            self.assertEqual(self.p.workshop_orders.calls,[])
            self.assertEqual(self.f.sql('SELECT * FROM assistent_bestellanforderungen'),[])

    def test_inflight_analysis_cannot_replace_external_claim(self):
        self.f.message['image']['caption']='Ein Stück, dringend'
        self.f.ingest();self.f.replies()
        source=self.base.channel.process_next()
        view=self.s.ensure_draft(source['id'])
        stored=[]
        def during_vision(*args):
            reserved=self.reserve(self.s.status(view['id']))
            stored.append(self.raw(reserved))
            return {'art':'produkt','produkt':'Späteres Ergebnis'}
        self.p.assistant_material_photos.vision=during_vision
        self.s.analyze(view['id'])
        self.assertEqual(len(stored),1)
        self.assertEqual(self.raw(view),stored[0])
        self.s.process_next()
        self.assertEqual(self.raw(view),stored[0])

    def test_selected_worker_failure_cannot_reset_external_claim(self):
        view=self.base.crystal_photo()
        self.f.sql("UPDATE einkauf_material_dialoge SET state='open',analysis_state='pending' WHERE id=?",(view['id'],))
        stored=[]
        def selected_then_reserved(draft_id):
            reserved=self.reserve(self.s.status(draft_id))
            stored.append(self.raw(reserved))
            raise ValueError('Synthetic worker lost race')
        with patch.object(self.s,'analyze',side_effect=selected_then_reserved):
            self.s.process_next()
        self.assertEqual(self.raw(view),stored[0])

    def test_selected_automatic_dispatch_cannot_reset_manual_claim_or_enqueue(self):
        view=self.base.review(self.base.photo())
        stored=[]
        original_submit=self.p.workshop_orders.submit_material_request
        def selected_then_reserved(draft_id,revision):
            reserved=self.reserve(self.s.status(draft_id))
            stored.append(self.raw(reserved))
            return original_submit(draft_id,revision)
        with patch.object(self.p.workshop_orders,'submit_material_request',side_effect=selected_then_reserved):
            self.s.process_next()
        self.assertEqual(self.raw(view),stored[0])
        self.assertEqual(self.f.sql('SELECT * FROM assistent_bestellanforderungen'),[])

    def test_employee_selected_article_cannot_silently_change_for_external_mail(self):
        view=self.base.photo()
        self.base.answer(view,'Ja')
        view=self.s.status(view['id'])
        self.assertIn('selected_article',view['fields'])
        with self.assertRaises(ValueError):self.reserve(view)
        matching=dict(self.payload,article_number='TEST-50')
        self.assertEqual(self.s.reserve_external(view['id'],view['revision'],matching)['state'],'external_pending')

    def test_audit_failure_rolls_back_reservation_and_queued_notifications(self):
        view=self.base.crystal_photo()
        original=self.raw(view)
        original_questions=copy.deepcopy(view['questions'])
        self.f.sql("CREATE TRIGGER fail_audit BEFORE INSERT ON assistent_audit BEGIN SELECT RAISE(ABORT,'synthetic'); END")
        with self.assertRaises(sqlite3.IntegrityError):self.reserve(view)
        self.assertEqual(self.raw(view),original)
        self.assertEqual(self.s.status(view['id'])['questions'],original_questions)

    def test_evidence_must_match_claim_and_actual_time_then_is_idempotent_after_revocation(self):
        view=self.reserve()
        original=self.raw(view)
        evidence=self.proof(view)
        for change in ({'reservation_id':'wrong'},{'recipient':'other@example.test'},{'subject':'other'},
                       {'send_evidence':''},{'sent_at':'2026-10-05T12:00:00'},
                       {'sent_at':datetime.fromtimestamp(self.f.time-1,timezone.utc).isoformat()},
                       {'sent_at':datetime.fromtimestamp(self.f.time+3600,timezone.utc).isoformat()},
                       {'confirmed':False}):
            with self.subTest(change=change),self.assertRaises((ValueError,PermissionError)):
                self.s.record_external_sent(view['id'],view['revision'],dict(evidence,**change))
            self.assertEqual(self.raw(view),original)
        with self.assertRaises(ValueError):self.s.record_external_sent(view['id'],view['revision']-1,evidence)
        self.f.sql('UPDATE assistent_rechte SET einkaufen=0,version=2 WHERE mitarbeiter_id=1')
        complete=self.s.record_external_sent(view['id'],view['revision'],evidence)
        self.assertEqual(complete['state'],'external_sent')
        self.assertEqual(self.s.record_external_sent(view['id'],view['revision'],evidence),complete)
        with self.assertRaises(ValueError):self.s.record_external_sent(view['id'],complete['revision'],dict(evidence,send_evidence='other'))
        self.assertEqual(len(self.f.sql('SELECT * FROM assistent_audit')),2)
        self.assertEqual(self.f.sql('SELECT * FROM assistent_bestellanforderungen'),[])

    def test_corrupt_reservation_is_never_reopened_or_confirmed(self):
        view=self.reserve()
        self.f.sql("UPDATE einkauf_material_dialoge SET snapshot_hash='corrupt' WHERE id=?",(view['id'],))
        self.assertEqual(self.s.status(view['id'])['external_order'],{})
        with self.assertRaises(ValueError):self.s.record_external_sent(view['id'],view['revision'],self.proof(view))
        with self.assertRaises(ValueError):self.s.recheck(view['id'],view['revision'])
        self.s.order_attempt(view['id'],None,{'state':'blocked'})
        self.assertEqual(self.s.status(view['id'])['state'],'external_pending')


if __name__=='__main__':
    unittest.main()
