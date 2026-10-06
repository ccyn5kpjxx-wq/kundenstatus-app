"""Isolated end-to-end material dialogs; synthetic images, catalog and dispatch."""
import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import test_materialkanal as fixtures
from werkstatt_bestellausgang import OrderDispatch
from werkstatt_materialdialog import MaterialDialog, register_material_dialog, parse_request, article_query, TABLES


class Orders:
    def __init__(self,p):
        self.p,self.calls=p,[]
        self.dispatch=SimpleNamespace(_intent=OrderDispatch._intent)
        self.supplier={'id':'supplier-1','name':'Testlieferant','recipient':'orders@example.test','verified':True}
        db=p.get_db()
        db.execute('CREATE TABLE assistent_bestellanforderungen(id TEXT PRIMARY KEY,actor_id TEXT,request_id TEXT UNIQUE)')
        db.commit();db.close()
    def cap(self): return 25000
    def resolve_supplier(self,key): return dict(self.supplier) if key==self.supplier['id'] else None
    def submit_material_request(self,draft_id,revision):
        snapshot=self.p.material_dialog.approved_order(draft_id,revision)
        with self.p.material_dialog.db() as db:
            self.p.material_dialog.guard_order(db,draft_id,revision,snapshot['actor'],snapshot['request_key'],
                self.dispatch._intent(snapshot['payload'],snapshot['request_key']))
            db.execute('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?) ON CONFLICT(request_id) DO NOTHING',
                ('synthetic-'+str(draft_id),snapshot['actor'],snapshot['request_key']))
        self.calls.append(copy.deepcopy(snapshot))
        result={'id':'synthetic-'+str(draft_id),'state':'sent' if snapshot['payload']['urgent'] else 'queued'}
        self.p.material_dialog.order_attempt(draft_id,revision,result)
        return result


class DialogTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.MaterialChannelTests('runTest');self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.p,self.channel=self.f.p,self.f.s
        self.p.material_channel=self.channel
        self.f.sql('ALTER TABLE assistent_rechte ADD COLUMN limit_cent INTEGER NOT NULL DEFAULT 25000')
        self.p.workshop_orders=Orders(self.p)
        self.hits=[{'produkt_name':'Test-Klebeband','lieferant':'Testlieferant','artikelnummer':'TEST-50','groesse':'50 mm','farbe':'grün',
                    'gebinde':'6 Rollen','ve':'Karton','quellen':[{'art':'einkauf','beleg_id':1,'position':1}]}]
        self.p.cockpit_data=SimpleNamespace(articles=lambda query:{'varianten':copy.deepcopy(self.hits),'abdeckung':{}})
        self.vision_calls=[]
        def vision(*args):
            self.vision_calls.append(True)
            return {'art':'produkt','produkt':'Test-Klebeband','breite':'50 mm','farbe':'grün'}
        self.p.assistant_material_photos.vision=vision
        self.s=register_material_dialog(self.p)
        self.sequence=0
        self.guard=patch('socket.socket.connect',side_effect=AssertionError('No real network in tests'))
        self.guard.start();self.addCleanup(self.guard.stop)

    def photo(self,caption='Test-Klebeband grün 50 mm, ein Karton, dringend', new=False):
        if new:
            self.f.message['id']='wamid.photo-'+str(self.sequence+10)
            self.sequence+=1
        self.f.message['image']['caption']=caption
        self.f.ingest();self.f.replies()
        source=self.channel.process_next()
        view=self.s.ensure_draft(source['id'])
        self.s.analyze(view['id'])
        return self.s.status(view['id'])

    def answer(self,view,body,*,sender='491701111111',quote='',explicit=True):
        self.sequence+=1
        self.f.time+=2
        message={'id':'wamid.answer-'+str(self.sequence),'from':sender,'timestamp':str(int(self.f.time)),'type':'text',
                 'text':{'body':(view['code']+': ' if explicit and view else '')+body}}
        if quote: message['context']={'id':quote}
        self.f.ingest(self.f.envelope(message))
        result=self.s.process_text()
        return result

    def review(self,view,**changes):
        payload={'supplier_id':'supplier-1','article_number':'TEST-50','product_name':'Test-Klebeband','variant':'grün 50 mm',
            'unit':'Karton','unit_price_cents':10000,'shipping_cents':500,'extra_costs_cents':0,
            'price_source':'Aktuelles geprüftes Testangebot, Karton mit 6 Rollen','reviewed':True,'verified_until':'2026-12-31'}
        payload.update(changes)
        return self.s.apply_admin_review(view['id'],view['revision'],payload)

    def test_photo_urgent_implies_request_analyzes_but_does_not_invent_price(self):
        view=self.photo()
        self.assertEqual(len(self.vision_calls),1)
        self.assertTrue(view['fields']['order_requested']['value'])
        self.assertEqual(view['fields']['quantity']['value'],'1')
        self.assertTrue(view['fields']['urgent']['value'])
        self.assertEqual(view['analysis_state'],'done')
        self.assertIn('price',view['missing_fields'])
        self.assertEqual(self.p.workshop_orders.calls,[])
        view=self.review(view)
        self.assertEqual(view['state'],'approved')
        result=self.s.process_next()
        self.assertEqual(result['state'],'sent')
        self.assertEqual(len(self.p.workshop_orders.calls),1)
        self.s.process_next()
        self.assertEqual(len(self.p.workshop_orders.calls),1)

    def crystal_photo(self,caption='Ein Stück, dringend',new=False):
        self.hits=[]
        self.p.assistant_material_photos.vision=lambda *args: {
            'art':'produkt','marke':'PPG','produkt':'T4000 Crystal Silver','artikelnummer':'T4000/E0.5'}
        return self.photo(caption,new=new)

    def test_recognized_photo_without_catalog_match_needs_internal_review_not_article_repeat(self):
        view=self.crystal_photo()
        self.assertEqual(view['state'],'review')
        self.assertEqual(view['missing_fields'],['supplier_review','price'])
        self.assertTrue(view['internal_review_pending'])
        self.assertFalse(view['employee_reply_required'])
        notice=view['questions'][0]
        self.assertEqual(notice['field'],'internal_review')
        for text in ('T4000 Crystal Silver','1 Stück','dringend','Noch nicht bestellt'):
            self.assertIn(text,notice['body'])
        self.assertNotIn('T4000/E0.5',notice['body'])
        self.assertNotIn('Antworte',notice['body'])
        self.assertNotIn('?',notice['body'])
        self.assertEqual(view['review'],{})
        self.assertNotIn('selected_article',view['fields'])
        with self.assertRaises(PermissionError):self.s.approved_order(view['id'],view['revision'])
        self.s.process_next()
        self.assertEqual(self.p.workshop_orders.calls,[])
        self.assertEqual(self.f.sql('SELECT * FROM assistent_bestellanforderungen'),[])

    def test_recognized_photo_only_asks_missing_quantity_then_keeps_bound_answer(self):
        view=self.crystal_photo('dringend')
        self.assertTrue(view['employee_reply_required'])
        question=view['questions'][0]
        self.assertEqual(question['field'],'quantity')
        self.assertIn('T4000 Crystal Silver',question['body'])
        self.assertIn('dringend',question['body'])
        self.assertIn('Antworte',question['body'])
        self.assertEqual(self.answer(view,'Ein Stück')['state'],'applied')
        updated=self.s.status(view['id'])
        self.assertEqual(updated['fields']['quantity']['value'],'1')
        self.assertEqual(updated['fields']['unit']['value'],'Stück')
        self.assertTrue(updated['fields']['urgent']['value'])
        self.assertEqual(updated['state'],'review')
        self.assertEqual(updated['questions'][0]['field'],'internal_review')
        self.assertNotIn('Antworte',updated['questions'][0]['body'])
        self.assertEqual(updated['questions'][1]['state'],'superseded')

    def test_internal_status_can_be_sent_once_but_quoted_yes_grants_nothing(self):
        view=self.crystal_photo()
        self.p.app.config['MATERIAL_WHATSAPP_REPLIES_ENABLED']=True
        calls=[]
        def post(url,**kwargs):
            calls.append(kwargs['json'])
            return fixtures.Response(json.dumps({'messages':[{'id':'wamid.internal-status'}]}).encode())
        self.channel.transport.post=post
        self.assertEqual(self.s.send_question()['state'],'sent')
        self.assertIsNone(self.s.send_question())
        self.assertEqual(len(calls),1)
        self.assertNotIn('Antworte',calls[0]['text']['body'])
        self.assertEqual(self.answer(None,'ja',quote='wamid.internal-status',explicit=False)['state'],'review')
        updated=self.s.status(view['id'])
        self.assertNotIn('selected_article',updated['fields'])
        self.assertEqual(updated['review'],{})
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_selected_catalog_article_is_not_asked_again_when_only_terms_are_missing(self):
        view=self.photo()
        self.assertEqual(self.answer(view,'ja')['state'],'applied')
        updated=self.s.status(view['id'])
        self.assertEqual(updated['state'],'review')
        self.assertEqual(updated['missing_fields'],['supplier_review','price'])
        self.assertEqual(updated['questions'][0]['field'],'internal_review')
        self.assertIn('Test-Klebeband',updated['questions'][0]['body'])
        self.assertNotIn('Meinst du',updated['questions'][0]['body'])
        self.assertEqual(updated['review'],{})

    def test_unclear_correction_prevents_queued_internal_status_from_being_sent(self):
        view=self.crystal_photo()
        self.assertEqual(self.answer(view,'Das anders machen')['state'],'review')
        self.assertFalse(self.s.status(view['id'])['internal_review_pending'])
        self.p.app.config['MATERIAL_WHATSAPP_REPLIES_ENABLED']=True
        with patch.object(self.channel.transport,'post',create=True) as post:
            self.assertEqual(self.s.send_question()['state'],'review')
        post.assert_not_called()

    def test_internal_status_rechecks_correction_after_claim_before_transport(self):
        view=self.crystal_photo()
        self.p.app.config['MATERIAL_WHATSAPP_REPLIES_ENABLED']=True
        original=self.s._draft
        calls=0
        def changed(dbase,*args,**kwargs):
            nonlocal calls
            calls+=1
            if calls==2:
                dbase.execute("UPDATE einkauf_material_dialoge SET error_code='antwort_unverstaendlich' WHERE id=?",(view['id'],))
            return original(dbase,*args,**kwargs)
        with patch.object(self.s,'_draft',side_effect=changed),patch.object(self.channel.transport,'post',create=True) as post:
            self.s.send_question()
        self.assertEqual(calls,2)
        post.assert_not_called()

    def crystal_catalog_hit(self):
        return dict(self.hits[0] if self.hits else {},produkt_name='PPG T4000/E0.5 ENVIROBASE CRYSTAL SILBER',
                    lieferant='Testlieferant',artikelnummer='SYNTHETIC-SILVER-05',groesse='0,5 Liter',
                    gebinde='0,5 Liter',ve='Dose',quellen=[{'art':'einkauf','beleg_id':1,'position':1}])

    def test_recheck_refreshes_stored_label_catalog_without_vision_selection_or_order(self):
        view=self.crystal_photo()
        self.hits=[self.crystal_catalog_hit()]
        with patch.object(self.p.assistant_material_photos,'vision',side_effect=AssertionError('No new vision')):
            updated=self.s.recheck(view['id'],view['revision'])
        self.assertEqual(len(updated['analysis']['treffer']),1)
        self.assertEqual(updated['revision'],view['revision']+1)
        self.assertEqual(updated['state'],'review')
        self.assertEqual(updated['missing_fields'],['supplier_review','price'])
        self.assertEqual(updated['questions'][0]['field'],'internal_review')
        self.assertNotIn('Meinst du',updated['questions'][0]['body'])
        self.assertEqual(updated['review'],{})
        self.assertNotIn('selected_article',updated['fields'])
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_recheck_preserves_genuine_catalog_ambiguity(self):
        view=self.crystal_photo()
        hit=self.crystal_catalog_hit()
        self.hits=[hit,dict(hit,artikelnummer='SYNTHETIC-SILVER-1',groesse='1 Liter',gebinde='1 Liter')]
        updated=self.s.recheck(view['id'],view['revision'])
        self.assertEqual(updated['state'],'open')
        self.assertTrue(updated['employee_reply_required'])
        self.assertEqual(updated['questions'][0]['field'],'article')
        self.assertIn('0,5 Liter',updated['questions'][0]['body'])
        self.assertIn('1 Liter',updated['questions'][0]['body'])
        self.assertNotIn('selected_article',updated['fields'])

    def test_recheck_catalog_outage_preserves_previous_analysis_and_revision(self):
        view=self.crystal_photo()
        self.p.cockpit_data.articles=lambda query: (_ for _ in ()).throw(ValueError('Synthetic unavailable'))
        with self.assertRaises(ValueError):self.s.recheck(view['id'],view['revision'])
        updated=self.s.status(view['id'])
        self.assertEqual(updated['analysis'],view['analysis'])
        self.assertEqual(updated['revision'],view['revision'])

    def test_recheck_rechecks_rights_and_same_revision_corrections_after_lookup(self):
        for mutation in ('rights','correction','revision','accepted'):
            with self.subTest(mutation=mutation):
                view=self.crystal_photo() if mutation=='rights' else self.photo('Ein Stück, dringend',new=True)
                initial=self.s.status(view['id'])
                old_lookup=self.p.cockpit_data.articles
                mutated=False
                def changed_lookup(query):
                    nonlocal mutated
                    if not mutated:
                        if mutation=='rights':
                            self.f.sql('UPDATE assistent_rechte SET version=version+1 WHERE mitarbeiter_id=1')
                        elif mutation=='correction':
                            self.f.sql("UPDATE einkauf_material_dialoge SET state='review',error_code='antwort_unverstaendlich' WHERE id=?",(view['id'],))
                        elif mutation=='revision':
                            self.f.sql('UPDATE einkauf_material_dialoge SET revision=revision+1 WHERE id=?',(view['id'],))
                        else:
                            self.f.sql('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?)',('durable','mitarbeiter:1','material:'+str(view['id'])))
                        mutated=True
                    return {'varianten':[]}
                self.p.cockpit_data.articles=changed_lookup
                with self.assertRaises((ValueError,PermissionError)):
                    self.s.recheck(view['id'],view['revision'])
                self.p.cockpit_data.articles=old_lookup
                updated=self.s.status(view['id'])
                self.assertEqual(updated['revision'],initial['revision']+(mutation=='revision'))
                self.assertEqual(updated['analysis'],initial['analysis'])
                if mutation=='rights':
                    self.f.sql('UPDATE assistent_rechte SET version=version-1 WHERE mitarbeiter_id=1')

    def test_recheck_does_not_even_search_cancelled_or_accepted_requests(self):
        view=self.crystal_photo()
        self.answer(view,'abbrechen')
        cancelled=self.s.status(view['id'])
        with patch.object(self.p.assistant_material_photos,'status') as lookup:
            with self.assertRaises(ValueError):self.s.recheck(cancelled['id'],cancelled['revision'])
        lookup.assert_not_called()
        other=self.photo(new=True)
        self.f.sql('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?)',('durable','mitarbeiter:1','material:'+str(other['id'])))
        with patch.object(self.p.assistant_material_photos,'status') as lookup:
            with self.assertRaises(ValueError):self.s.recheck(other['id'],other['revision'])
        lookup.assert_not_called()

    def test_recheck_does_not_repeat_sent_uncertain_or_inflight_internal_status(self):
        for state in ('sent','uncertain','sending'):
            with self.subTest(state=state):
                view=self.crystal_photo(new=True)
                notice_id=view['questions'][0]['id']
                self.f.sql('UPDATE einkauf_material_rueckfragen SET state=? WHERE id=?',(state,notice_id))
                updated=self.s.recheck(view['id'],view['revision'])
                updated=self.s.recheck(updated['id'],updated['revision'])
                self.assertEqual(updated['questions'][0]['id'],notice_id)
                self.assertEqual(self.f.sql("SELECT id FROM einkauf_material_rueckfragen WHERE draft_id=? AND state='queued'",(view['id'],)),[])

    def test_recheck_replaces_unsent_status_and_changed_quantity_gets_new_status(self):
        view=self.crystal_photo()
        previous=view['questions'][0]['id']
        updated=self.s.recheck(view['id'],view['revision'])
        self.assertEqual(updated['questions'][1]['id'],previous)
        self.assertEqual(updated['questions'][1]['state'],'superseded')
        self.assertEqual(updated['questions'][0]['state'],'queued')
        notice_id=updated['questions'][0]['id']
        self.f.sql("UPDATE einkauf_material_rueckfragen SET state='sent' WHERE id=?",(notice_id,))
        self.assertEqual(self.answer(updated,'Zwei Stück')['state'],'applied')
        updated=self.s.status(view['id'])
        self.assertNotEqual(updated['questions'][0]['id'],notice_id)
        self.assertEqual(updated['questions'][0]['state'],'queued')
        self.assertIn('2 Stück',updated['questions'][0]['body'])

    def test_multiple_catalog_variants_still_need_a_specific_employee_answer(self):
        self.hits.append(dict(self.hits[0],artikelnummer='TEST-30',groesse='30 mm'))
        view=self.photo()
        self.assertEqual(view['state'],'open')
        self.assertTrue(view['employee_reply_required'])
        self.assertFalse(view['internal_review_pending'])
        question=view['questions'][0]
        self.assertEqual(question['field'],'article')
        for text in ('50 mm','30 mm','1 Karton','dringend','Antworte'):
            self.assertIn(text,question['body'])

    def test_unreadable_product_is_not_misrepresented_as_identified(self):
        self.hits=[]
        self.p.assistant_material_photos.vision=lambda *args:{'art':'unklar'}
        view=self.photo('Ein Stück, dringend')
        self.assertEqual(view['state'],'open')
        self.assertIn('article',view['missing_fields'])
        self.assertNotIn('supplier_review',view['missing_fields'])
        self.assertEqual(view['questions'][0]['field'],'article')
        self.assertIn('noch nicht eindeutig lesbar',view['questions'][0]['body'])

    def test_price_expiry_and_budget_limit_are_internal_blockers_not_employee_questions(self):
        view=self.review(self.photo(),unit_price_cents=24501,shipping_cents=500)
        self.assertEqual(view['state'],'review')
        self.assertEqual(view['missing_fields'],['budget'])
        self.assertEqual(view['questions'][0]['field'],'internal_review')
        self.assertIn('250 Euro',view['questions'][0]['body'])
        self.assertNotIn('Antworte',view['questions'][0]['body'])
        with self.assertRaises(PermissionError):self.s.approved_order(view['id'],view['revision'])
        view=self.review(view,verified_until='2026-10-05')
        self.f.time+=86400
        view=self.s.recheck(view['id'],view['revision'])
        self.assertEqual(view['state'],'review')
        self.assertEqual(view['missing_fields'],['price'])
        self.assertIn('abgelaufen',view['questions'][0]['body'])
        self.assertNotIn('Antworte',view['questions'][0]['body'])
        with self.assertRaises(PermissionError):self.s.approved_order(view['id'],view['revision'])
        self.s.process_next()
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_nonurgent_is_not_cancelled_and_uses_weekly_bridge(self):
        for text in ('Bitte 1 Karton bestellen, nicht dringend','Nicht dringend, bitte 1 Karton bestellen'):
            parsed=parse_request(text)
            self.assertNotIn('cancelled',parsed)
            self.assertFalse(parsed['urgent'])
        view=self.review(self.photo('Bitte 1 Karton bestellen, nicht dringend'))
        self.assertEqual(self.s.process_next()['state'],'queued')

    def test_negated_or_conflicting_urgency_never_becomes_immediate(self):
        for text in ('Bitte 1 Karton nicht sofort bestellen, erst am Montag',
                     'Nicht so dringend, 1 Karton bestellen',
                     'Bitte 1 Karton bestellen, keinesfalls sofort'):
            parsed=parse_request(text)
            self.assertFalse(parsed['urgent'])
            self.assertNotIn('cancelled',parsed)
        self.assertNotIn('urgent',parse_request('1 Karton dringend bestellen, aber erst am Montag'))

    def test_ve_caption_and_ocr_never_become_quantity(self):
        view=self.photo('Test-Klebeband, VE 96 Stück, dringend')
        self.assertNotIn('quantity',view['fields'])
        self.assertIn('quantity',view['missing_fields'])
        self.assertEqual(self.p.workshop_orders.calls,[])
        self.assertNotIn('quantity',parse_request('Inhalt: 96 Stück, dringend'))

    def test_bound_quantity_reply_then_no_extra_final_confirmation(self):
        view=self.photo('Klebeband, dringend')
        self.assertEqual(self.answer(view,'ein Karton')['state'],'applied')
        view=self.review(self.s.status(view['id']))
        self.assertEqual(view['state'],'approved')
        self.assertEqual(self.s.process_next()['state'],'sent')

    def test_stale_code_foreign_employee_and_unbound_yes_cannot_authorize(self):
        view=self.photo('Klebeband, dringend')
        self.channel.verify_sender(2,'491702222222','persönlich geprüft',confirmed=True)
        self.assertEqual(self.answer(view,'ein Karton',sender='491702222222')['state'],'review')
        self.assertNotIn('quantity',self.s.status(view['id'])['fields'])
        self.answer(view,'ein Karton')
        self.assertEqual(self.answer(view,'zwei Kartons')['state'],'review')
        self.assertEqual(self.answer(None,'ja',explicit=False)['state'],'review')
        self.assertEqual(self.s.status(view['id'])['fields']['quantity']['value'],'1')

    def test_quote_exact_source_never_uses_latest_other_photo(self):
        first=self.photo('Klebeband dringend')
        second=self.photo('Anderes Material dringend',new=True)
        self.assertEqual(self.answer(None,'ein Karton',quote='wamid.synthetic-1',explicit=False)['state'],'applied')
        self.assertEqual(self.s.status(first['id'])['fields']['quantity']['value'],'1')
        self.assertNotIn('quantity',self.s.status(second['id'])['fields'])

    def test_variant_answer_and_valid_terms_are_reusable_without_recheck(self):
        first=self.photo()
        self.assertEqual(self.answer(first,'50 mm')['state'],'applied')
        first=self.review(self.s.status(first['id']))
        self.assertTrue(first['review'].get('match_identity'))
        self.s.process_next()
        second=self.photo(new=True)
        self.assertEqual(self.answer(second,'ja')['state'],'applied')
        second=self.s.status(second['id'])
        self.assertEqual(second['review']['reused_from'],first['id'])
        self.assertEqual(second['state'],'approved')
        self.assertEqual(self.s.process_next()['state'],'sent')

    def test_expired_terms_and_changed_supplier_are_not_orderable(self):
        view=self.review(self.photo(),verified_until='2026-10-05')
        self.f.time+=86400
        with self.assertRaises(PermissionError): self.s.approved_order(view['id'],view['revision'])
        self.f.time-=86400
        self.p.workshop_orders.supplier['recipient']='changed@example.test'
        with self.assertRaises(PermissionError): self.s.approved_order(view['id'],view['revision'])

    def test_text_only_request_gets_own_source_without_photo_or_vision(self):
        self.assertEqual(self.answer(None,'Bitte ein Karton Klebeband bestellen, nicht dringend',explicit=False)['state'],'applied')
        view=self.s.list()[0]
        self.assertEqual(view['source_kind'],'text')
        self.assertEqual(view['employee_name'],'Testperson Eins')
        self.assertEqual(self.f.sql('SELECT id FROM einkauf_eingang_dateien'),[])
        self.s.process_next()
        self.assertEqual(self.vision_calls,[])
        self.assertTrue(self.s.status(view['id'])['analysis']['treffer'])

    def test_revocation_inside_vision_cannot_commit_labels(self):
        def revoke(*args):
            self.f.sql('UPDATE assistent_rechte SET version=version+1 WHERE mitarbeiter_id=1')
            return {'art':'produkt','produkt':'Never store this'}
        self.p.assistant_material_photos.vision=revoke
        view=self.photo()
        self.assertEqual(view['analysis_state'],'failed')
        self.assertNotIn('Never store this',json.dumps(view))

    def test_replay_same_text_and_same_photo_have_one_draft(self):
        view=self.photo('Klebeband dringend')
        self.assertEqual(self.f.ingest()['duplicates'],1)
        self.s.ensure_draft(view['message_id'])
        self.assertEqual(len(self.s.list()),1)

    def test_price_quantity_shipping_and_personal_caps_all_rechecked(self):
        view=self.review(self.photo(),unit_price_cents=24500,shipping_cents=500)
        self.assertEqual(view['state'],'approved')
        self.assertEqual(self.s.approved_order(view['id'],view['revision'])['payload']['max_total_cents'],25000)
        self.f.sql('UPDATE assistent_rechte SET limit_cent=24999 WHERE mitarbeiter_id=1')
        with self.assertRaises(PermissionError): self.s.approved_order(view['id'],view['revision'])
        with self.assertRaises(ValueError): self.review(view,shipping_cents=None)

    def test_unknown_send_outcome_and_crashed_sending_are_never_retried(self):
        self.photo('Klebeband dringend')
        self.p.app.config['MATERIAL_WHATSAPP_REPLIES_ENABLED']=True
        calls=[]
        def timeout(*args,**kwargs):
            calls.append((args,kwargs))
            import requests
            raise requests.Timeout('synthetic')
        self.channel.transport.post=timeout
        self.assertEqual(self.s.send_question()['state'],'uncertain')
        self.assertIsNone(self.s.send_question())
        self.assertEqual(len(calls),1)
        self.f.sql("UPDATE einkauf_material_rueckfragen SET state='sending',updated_at=? WHERE state='uncertain'",(self.f.time-121,))
        self.assertIsNone(self.s.send_question())
        self.assertEqual(len(calls),1)
        self.assertEqual(self.s.list()[0]['questions'][0]['state'],'uncertain')

    def test_question_uses_same_receiver_and_sent_question_binds_yes(self):
        view=self.photo('Klebeband ein Karton bestellen')
        self.p.app.config['MATERIAL_WHATSAPP_REPLIES_ENABLED']=True
        calls=[]
        def post(url,**kwargs):
            calls.append((url,kwargs))
            return fixtures.Response(json.dumps({'messages':[{'id':'wamid.question-1'}]}).encode())
        self.channel.transport.post=post
        self.assertEqual(self.s.send_question()['state'],'sent')
        self.assertTrue(calls[0][0].endswith('/123456/messages'))
        self.assertEqual(calls[0][1]['json']['to'],'491701111111')
        self.assertFalse(calls[0][1]['allow_redirects'])
        self.assertEqual(self.answer(None,'ja',quote='wamid.question-1',explicit=False)['state'],'applied')
        self.assertTrue(self.s.status(view['id'])['fields']['urgent']['value'])

    def test_crash_after_enqueue_blocks_edits_before_order_attempt(self):
        view=self.review(self.photo())
        self.f.sql('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?)',('durable','mitarbeiter:1','material:'+str(view['id'])))
        with self.assertRaises(ValueError): self.review(view,unit_price_cents=1)
        self.assertEqual(self.answer(view,'zwei Kartons')['state'],'review')
        self.s.order_attempt(view['id'],view['revision'],{'id':'durable','state':'queued'})
        self.assertEqual(self.s.status(view['id'])['state'],'accepted')

    def test_admin_only_review_cannot_override_signed_quantity_or_intent(self):
        view=self.photo('Test-Klebeband')
        with self.assertRaises(PermissionError): self.s.apply_admin_review(view['id'],view['revision'],{},actor='mitarbeiter:1')
        view=self.review(view)
        self.assertIn('order_requested',view['missing_fields'])
        self.assertIn('quantity',view['missing_fields'])
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_text_search_keeps_product_dimensions_not_requested_amount(self):
        queries=[]
        self.p.cockpit_data.articles=lambda query: queries.append(query) or {'varianten':[]}
        self.answer(None,'Bitte 1 Karton Klebeband 50 mm bestellen, dringend',explicit=False)
        self.s.process_next()
        self.assertEqual(queries,['Klebeband 50 mm'])
        self.assertEqual(article_query('Mipa D8115 bitte 2 Dosen bestellen, nicht dringend'),'Mipa D8115')

    def test_variant_change_invalidates_price_and_guard_checks_identity(self):
        self.hits.append(dict(self.hits[0],artikelnummer='TEST-30',groesse='30 mm'))
        view=self.photo()
        self.answer(view,'50 mm')
        view=self.review(self.s.status(view['id']))
        self.answer(view,'30 mm')
        updated=self.s.status(view['id'])
        self.assertEqual(updated['state'],'review')
        self.assertEqual(updated['review'],{})
        with self.assertRaises(PermissionError): self.s.approved_order(updated['id'],updated['revision'])
        # Even an inconsistent stored snapshot cannot bypass the final fence.
        self.f.sql('UPDATE einkauf_material_dialoge SET state=?,review_json=? WHERE id=?',('approved',json.dumps(view['review']),view['id']))
        with self.assertRaises(PermissionError): self.s.approved_order(updated['id'],updated['revision'])

    def test_revoked_old_ready_and_pending_requests_do_not_starve_next(self):
        self.f.ingest();self.f.replies()
        oldest=self.channel.process_next()
        self.s.ensure_draft(oldest['id'])
        self.f.message['id']='wamid.unclaimed'
        self.f.ingest();self.f.replies()
        unclaimed=self.channel.process_next()
        self.channel.revoke_sender(self.f.sender['id'],self.f.sender['revision'])
        self.channel.verify_sender(2,'+491702222222','Personally verified second synthetic sender',confirmed=True)
        self.f.message.update(id='wamid.valid',**{'from':'491702222222'})
        self.f.ingest();self.f.replies()
        valid=self.channel.process_next()
        self.s.process_next()
        self.assertEqual(self.f.sql('SELECT state FROM einkauf_material_nachrichten WHERE id=?',(unclaimed['id'],))[0]['state'],'review')
        views={view['message_id']:view for view in self.s.list()}
        self.assertEqual(views[oldest['id']]['state'],'review')
        self.assertEqual(views[valid['id']]['analysis_state'],'done')
        self.assertEqual(views[valid['id']]['employee_id'],2)

    def test_crash_then_revoke_preserves_durable_order_without_authorizing_send(self):
        view=self.review(self.photo())
        self.f.sql('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?)',('durable','mitarbeiter:1','material:'+str(view['id'])))
        self.channel.revoke_sender(self.f.sender['id'],self.f.sender['revision'])
        result=self.s.process_next()
        self.assertEqual(result['state'],'blocked')
        view=self.s.status(view['id'])
        self.assertEqual(view['dispatch_id'],'durable')
        self.assertEqual(view['state'],'accepted')
        self.assertEqual(self.p.workshop_orders.calls,[])
        with self.assertRaises(PermissionError): self.s.approved_order(view['id'],view['revision'])

    def test_recheck_restores_blocked_handoff_but_never_sends(self):
        view=self.review(self.photo())
        self.s.order_attempt(view['id'],view['revision'],{'state':'blocked'})
        view=self.s.recheck(view['id'],view['revision'])
        self.assertEqual(view['state'],'approved')
        self.assertEqual(self.p.workshop_orders.calls,[])
        self.channel.revoke_sender(self.f.sender['id'],self.f.sender['revision'])
        with self.assertRaises(PermissionError): self.s.recheck(view['id'],view['revision'])

    def test_unclear_correction_pauses_existing_approval_and_needs_clarification(self):
        view=self.review(self.photo())
        self.assertEqual(self.answer(view,'Das anders machen')['state'],'review')
        view=self.s.status(view['id'])
        self.assertEqual(view['state'],'review')
        with self.assertRaises(ValueError): self.s.recheck(view['id'],view['revision'])
        self.answer(view,'dringend, aber erst am Montag')
        view=self.s.status(view['id'])
        self.assertIn('urgent',view['missing_fields'])
        self.assertNotIn('urgent',view['fields'])
        self.assertEqual(view['state'],'open')


if __name__=='__main__': unittest.main(verbosity=2)
