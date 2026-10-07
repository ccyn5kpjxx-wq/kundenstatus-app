"""Isolated end-to-end material dialogs; synthetic images, catalog and dispatch."""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import test_materialkanal as fixtures
from werkstatt_bestellausgang import OrderDispatch
from werkstatt_bestellplan import BERLIN, next_dispatch_at
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
            self.f.message['timestamp']=str(int(self.f.time))
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

    def test_photo_quantity_is_request_with_monday_default_and_internal_terms_check(self):
        view=self.photo('ein Stück')
        self.assertEqual(view['state'],'review')
        self.assertFalse(view['employee_reply_required'])
        self.assertTrue(view['fields']['order_requested']['value'])
        self.assertEqual(view['fields']['order_requested']['proof']['basis'],'quantity_in_personal_material_channel')
        self.assertEqual(view['fields']['quantity']['value'],'1')
        self.assertEqual(view['fields']['unit']['value'],'Stück')
        self.assertFalse(view['fields']['urgent']['value'])
        self.assertEqual(view['fields']['urgent']['proof']['basis'],'owner_default_monday_14')
        self.assertEqual(view['fields']['selected_article']['value']['artikelnummer'],'TEST-50')
        self.assertEqual(view['missing_fields'],['supplier_review','price'])
        self.assertEqual(view['questions'][0]['field'],'internal_review')
        self.assertIn('Noch nicht bestellt',view['questions'][0]['body'])
        self.assertNotIn('?',view['questions'][0]['body'])
        self.s.process_next()
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_exact_photo_with_valid_same_unit_terms_needs_no_repeat_or_final_yes(self):
        prior=self.review(self.photo('ein Stück'),unit='Stück')
        self.assertEqual(self.s.process_next()['state'],'queued')
        for caption,expected in (('ein Stück','queued'),('ein Stück, dringend','sent')):
            with self.subTest(caption=caption):
                self.f.time+=601
                view=self.photo(caption,new=True)
                self.assertEqual(view['state'],'approved')
                self.assertEqual(view['review']['reused_from'],prior['id'])
                self.assertFalse(view['employee_reply_required'])
                self.assertEqual(self.s.process_next()['state'],expected)
                snapshot=self.p.workshop_orders.calls[-1]
                self.assertEqual(snapshot['payload']['quantity'],'1')
                now=datetime.fromtimestamp(self.f.time,timezone.utc)
                due=next_dispatch_at(now,snapshot['payload']['urgent'])
                if expected=='sent':
                    self.assertEqual(due,now)
                else:
                    self.assertEqual((due.astimezone(BERLIN).weekday(),due.astimezone(BERLIN).hour),(0,14))
                count=len(self.p.workshop_orders.calls)
                self.s.process_next()
                self.assertEqual(len(self.p.workshop_orders.calls),count)
                prior=view

    def test_bare_quantity_binds_one_recent_personal_photo_preserving_urgency(self):
        view=self.photo('dringend')
        self.assertEqual(self.answer(None,'ein Stück',explicit=False)['state'],'applied')
        updated=self.s.status(view['id'])
        self.assertEqual(len(self.s.list()),1)
        self.assertEqual(updated['fields']['quantity']['value'],'1')
        self.assertTrue(updated['fields']['urgent']['value'])
        self.assertEqual(updated['state'],'review')
        self.assertFalse(updated['employee_reply_required'])
        self.assertEqual(updated['questions'][0]['field'],'internal_review')
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_bare_quantity_binds_blank_photo_with_monday_default(self):
        view=self.photo('')
        self.assertEqual(self.answer(None,'ein Stück',explicit=False)['state'],'applied')
        updated=self.s.status(view['id'])
        self.assertTrue(updated['fields']['order_requested']['value'])
        self.assertFalse(updated['fields']['urgent']['value'])
        self.assertFalse(updated['employee_reply_required'])

    def test_free_order_then_number_and_unicode_unit_stay_with_same_photo(self):
        self.hits=[]
        self.p.assistant_material_photos.vision=lambda *args:{'art':'produkt','marke':'Top-Color','farbe':'gelb'}
        view=self.photo('')
        self.assertEqual(view['questions'][0]['field'],'order_requested')
        reply=self.answer(None,'Bestellen',explicit=False)
        self.assertEqual(reply['state'],'applied')
        self.assertEqual(reply['draft_id'],view['id'])
        self.assertEqual(len(self.s.list()),1)
        view=self.s.status(view['id'])
        self.assertEqual(view['questions'][0]['field'],'quantity')
        self.assertEqual(self.answer(None,'1',explicit=False)['state'],'applied')
        view=self.s.status(view['id'])
        self.assertEqual(view['fields']['quantity']['value'],'1')
        self.assertNotIn('unit',view['fields'])
        self.assertEqual(view['questions'][0]['field'],'unit')
        self.assertEqual(self.answer(None,'1 stűck',explicit=False)['state'],'applied')
        view=self.s.status(view['id'])
        self.assertEqual(view['fields']['unit']['value'],'Stück')
        self.assertFalse(view['fields']['urgent']['value'])
        self.assertEqual(len(self.s.list()),1)
        self.assertEqual(self.f.sql('SELECT body FROM einkauf_material_texte ORDER BY id')[-1]['body'],'1 stűck')
        self.assertEqual(self.p.workshop_orders.calls,[])
        self.assertIn('article',view['missing_fields'])

    def test_unicode_unit_spelling_is_only_normalized_for_parsing(self):
        for text in ('1 stűck','1 STŰCK','1 stu\u030bck','1 stu\u0308ck','1 Stück','1 stueck'):
            with self.subTest(text=text):
                self.assertEqual(parse_request(text),{'quantity':'1','unit':'Stück'})
                self.assertEqual(article_query(text),'')
        self.assertEqual(parse_request('Stűck',question='unit'),{'unit':'Stück'})

    def test_bare_number_needs_quantity_question_and_never_invents_unit(self):
        view=self.photo('')
        self.assertEqual(self.answer(None,'1',explicit=False)['state'],'review')
        self.assertNotIn('quantity',self.s.status(view['id'])['fields'])
        self.answer(None,'Bestellen',explicit=False)
        view=self.s.status(view['id'])
        self.assertEqual(self.answer(view,'1')['state'],'applied')
        view=self.s.status(view['id'])
        self.assertNotIn('unit',view['fields'])
        self.assertEqual(self.answer(None,'Stűck',explicit=False)['state'],'applied')
        self.assertEqual(self.s.status(view['id'])['fields']['unit']['value'],'Stück')

    def test_short_order_without_photo_or_with_two_photos_creates_no_text_order(self):
        self.assertEqual(self.answer(None,'Bestellen',explicit=False)['state'],'review')
        self.assertEqual(self.s.list(),[])
        first=self.photo('')
        second=self.photo('',new=True)
        self.assertEqual(self.answer(None,'Bestellen',explicit=False)['state'],'review')
        self.assertEqual(len(self.s.list()),2)
        for view in (first,second):
            self.assertNotIn('order_requested',self.s.status(view['id'])['fields'])

    def test_short_order_counts_unprocessed_photos_and_personal_sender(self):
        first=self.photo('')
        self.channel.verify_sender(2,'491702222222','persönlich geprüft',confirmed=True)
        self.assertEqual(self.answer(None,'Bestellen',sender='491702222222',explicit=False)['state'],'review')
        self.f.message['id']='wamid.second-unprocessed'
        self.f.message['image']['caption']=''
        self.f.ingest()
        self.assertEqual(self.answer(None,'Bestellen',explicit=False)['state'],'review')
        self.assertNotIn('order_requested',self.s.status(first['id'])['fields'])

    def test_short_order_waits_for_single_photo_and_keeps_source_binding(self):
        self.f.message['image']['caption']=''
        self.f.ingest()
        self.assertEqual(self.answer(None,'Bestellen',explicit=False)['state'],'waiting_for_photo')
        self.f.replies()
        source=self.channel.process_next()
        view=self.s.ensure_draft(source['id'])
        self.s.analyze(view['id'])
        reply=self.s.process_text()
        self.assertEqual(reply['state'],'applied')
        self.assertEqual(reply['draft_id'],view['id'])
        self.assertEqual(len(self.s.list()),1)

    def test_pure_brand_and_visual_category_are_hints_not_product_identity(self):
        view=self.photo('Ein Stück')
        analysis=dict(view['analysis'],merkmale={'marke':'Top-Color','produkt':'Top-Color','farbe':'gelb',
            'materialtyp':'Folie','masse':'5 x 120 m'},treffer=[])
        fields=dict(view['fields']);fields.pop('selected_article')
        self.f.sql('UPDATE einkauf_material_dialoge SET fields_json=?,analysis_json=?,revision=revision+1 WHERE id=?',
                   (json.dumps(fields),json.dumps(analysis),view['id']))
        with self.s.db() as db:self.s._refresh(db,self.s._draft(db,view['id']))
        view=self.s.status(view['id'])
        self.assertIn('article',view['missing_fields'])
        self.assertNotIn('supplier_review',view['missing_fields'])
        self.assertIn('5 x 120 m',view['questions'][0]['body'])
        self.assertIn('Folie',view['questions'][0]['body'])

    def test_bare_quantity_never_chooses_latest_of_multiple_photos(self):
        first=self.photo('')
        second=self.photo('',new=True)
        self.assertEqual(self.answer(None,'ein Stück',explicit=False)['state'],'review')
        for view in (first,second):
            self.assertNotIn('quantity',self.s.status(view['id'])['fields'])
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_unprocessed_second_photo_also_makes_bare_quantity_ambiguous(self):
        first=self.photo('')
        self.f.message['id']='wamid.second-unprocessed'
        self.f.message['image']['caption']=''
        self.f.ingest()
        self.assertEqual(self.answer(None,'ein Stück',explicit=False)['state'],'review')
        self.assertNotIn('quantity',self.s.status(first['id'])['fields'])
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_bare_quantity_waits_for_its_only_photo_to_finish_intake(self):
        self.f.message['image']['caption']=''
        self.f.ingest()
        self.assertEqual(self.answer(None,'ein Stück',explicit=False)['state'],'waiting_for_photo')
        self.assertEqual(self.s.list(),[])
        self.f.replies()
        source=self.channel.process_next()
        view=self.s.ensure_draft(source['id'])
        self.s.analyze(view['id'])
        self.assertEqual(self.s.process_text()['state'],'applied')
        self.assertEqual(self.s.status(view['id'])['fields']['quantity']['value'],'1')

    def test_bare_quantity_does_not_bind_old_photo_or_other_employee(self):
        view=self.photo('')
        self.channel.verify_sender(2,'491702222222','persönlich geprüft',confirmed=True)
        self.assertEqual(self.answer(None,'ein Stück',sender='491702222222',explicit=False)['state'],'review')
        self.f.time+=901
        self.assertEqual(self.answer(None,'ein Stück',explicit=False)['state'],'review')
        self.assertNotIn('quantity',self.s.status(view['id'])['fields'])
        self.assertEqual(self.answer(None,'ein Stück',quote='wamid.synthetic-1',explicit=False)['state'],'applied')

    def test_negated_quantity_stock_and_conditional_captions_are_not_purchase_requests(self):
        for caption in ('Nicht ein Stück','nicht 1 Stück','ein Stück vorhanden','wir haben ein Stück auf Lager',
                        'ein Stück geliefert','vielleicht ein Stück','ein Stück?','VE 96 Stück','nicht bestellen, ein Stück'):
            with self.subTest(caption=caption):
                view=self.photo(caption,new=True)
                self.assertNotIn('order_requested',view['fields'])
                self.assertNotEqual(view['state'],'approved')
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_negated_bare_quantity_does_not_order_with_valid_reusable_terms(self):
        self.review(self.photo('ein Stück'),unit='Stück')
        self.s.process_next()
        view=self.photo('',new=True)
        self.assertEqual(self.answer(None,'nicht 1 Stück',explicit=False)['state'],'review')
        updated=self.s.status(view['id'])
        self.assertNotIn('quantity',updated['fields'])
        self.assertNotIn('order_requested',updated['fields'])
        self.s.process_next()
        self.assertEqual(len(self.p.workshop_orders.calls),1)

    def test_conflicting_urgency_still_needs_answer_not_monday_default(self):
        view=self.photo('ein Stück dringend, aber erst am Montag')
        self.assertIn('urgent',view['missing_fields'])
        self.assertNotIn('urgent',view['fields'])
        self.assertTrue(view['employee_reply_required'])
        self.assertEqual(view['questions'][0]['field'],'urgent')

    def test_exact_photo_match_preserves_decimal_dimensions_and_missing_variants(self):
        for width,color in (('5.0 mm','grün'),('5,0 mm','grün'),('50 mm','blau'),('50 mm','')):
            with self.subTest(width=width,color=color):
                self.hits[0]['groesse']=width
                self.hits[0]['farbe']=color
                view=self.photo('ein Stück',new=True)
                self.assertNotIn('selected_article',view['fields'])
                self.assertNotEqual(view['state'],'approved')
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_recheck_discards_automatic_selection_if_current_catalog_loses_uniqueness(self):
        view=self.review(self.photo('ein Stück'),unit='Stück')
        self.assertEqual(view['fields']['selected_article']['proof']['basis'],'exact_photo_catalog_match')
        self.hits.append(dict(self.hits[0],artikelnummer='TEST-50-ALT',gebinde='12 Rollen'))
        updated=self.s.recheck(view['id'],view['revision'])
        self.assertNotIn('selected_article',updated['fields'])
        self.assertEqual(updated['review'],{})
        self.assertEqual(updated['state'],'open')
        self.assertEqual(updated['questions'][0]['field'],'article')
        self.s.process_next()
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_matching_name_and_variant_do_not_override_different_photo_sku(self):
        self.p.assistant_material_photos.vision=lambda *args: {
            'art':'produkt','produkt':'Test-Klebeband','artikelnummer':'TEST-30','breite':'50 mm','farbe':'grün'}
        view=self.photo('ein Stück')
        self.assertNotIn('selected_article',view['fields'])
        self.assertEqual(view['review'],{})
        self.assertEqual(view['state'],'review')
        self.s.process_next()
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_possible_double_order_requires_bound_additional_yes_and_persists(self):
        first=self.review(self.photo('ein Stück, dringend'),unit='Stück')
        self.assertEqual(self.s.process_next()['state'],'sent')
        second=self.photo('ein Stück, dringend',new=True)
        self.assertEqual(second['state'],'open')
        self.assertEqual(second['duplicate_of'],first['id'])
        self.assertEqual(second['questions'][0]['field'],'possible_duplicate')
        self.assertIn('zusätzlich',second['questions'][0]['body'])
        self.assertEqual(self.answer(None,'ja',explicit=False)['state'],'review')
        self.f.time+=3600
        restarted=MaterialDialog(self.p)
        self.p.material_dialog=restarted
        second=restarted.recheck(second['id'],second['revision'])
        self.assertEqual(second['duplicate_of'],first['id'])
        self.assertIn('possible_duplicate',second['missing_fields'])
        self.s=restarted
        self.assertEqual(self.answer(second,'Ja')['state'],'applied')
        second=self.s.status(second['id'])
        self.assertEqual(second['state'],'approved')
        self.assertEqual(self.s.process_next()['state'],'sent')
        self.s.process_next()
        self.assertEqual(len(self.p.workshop_orders.calls),2)

    def test_duplicate_no_cancels_only_new_request_and_wrong_employee_cannot_confirm(self):
        first=self.review(self.photo('ein Stück, dringend'),unit='Stück')
        self.s.process_next()
        second=self.photo('ein Stück, dringend',new=True)
        self.channel.verify_sender(2,'491702222222','persönlich geprüft',confirmed=True)
        self.assertEqual(self.answer(second,'Ja',sender='491702222222')['state'],'review')
        self.assertEqual(self.s.status(second['id'])['state'],'open')
        self.assertEqual(self.answer(second,'Nein')['state'],'applied')
        self.assertEqual(self.s.status(second['id'])['state'],'cancelled')
        self.assertEqual(self.s.status(first['id'])['state'],'accepted')
        self.s.process_next()
        self.assertEqual(len(self.p.workshop_orders.calls),1)

    def test_changed_urgency_does_not_silently_order_same_article_twice(self):
        first=self.review(self.photo('Ein Stück'),unit='Stück')
        self.assertEqual(self.s.process_next()['state'],'queued')
        second=self.photo('Ein Stück, dringend',new=True)
        self.assertEqual(second['duplicate_of'],first['id'])
        self.assertEqual(second['questions'][0]['field'],'possible_duplicate')
        self.s.process_next()
        self.assertEqual(len(self.p.workshop_orders.calls),1)
        self.assertEqual(self.answer(second,'Ja')['state'],'applied')
        self.assertEqual(self.s.process_next()['state'],'sent')
        self.assertEqual(len(self.p.workshop_orders.calls),2)

    def test_duplicate_confirmation_does_not_apply_after_changed_quantity(self):
        self.review(self.photo('ein Stück, dringend'),unit='Stück')
        self.s.process_next()
        second=self.photo('ein Stück, dringend',new=True)
        self.answer(second,'Ja')
        second=self.s.status(second['id'])
        self.answer(second,'Zwei Stück')
        changed=self.s.status(second['id'])
        self.assertIsNone(changed['duplicate_of'])
        self.assertEqual(changed['fields']['quantity']['value'],'2')
        self.assertNotIn('duplicate_confirmation',changed['fields'])
        self.answer(changed,'Ein Stück')
        restored=self.s.status(second['id'])
        self.assertIn('possible_duplicate',restored['missing_fields'])
        self.assertNotIn('duplicate_confirmation',restored['fields'])

    def test_late_duplicate_before_dispatch_persists_an_answerable_question(self):
        first=self.photo('Ein Stück, dringend')
        fields=copy.deepcopy(first['fields'])
        fields.pop('selected_article')
        self.f.sql("UPDATE einkauf_material_dialoge SET fields_json=?,analysis_json='{}',analysis_state='pending' WHERE id=?",
                   (json.dumps(fields),first['id']))
        second=self.review(self.photo('Ein Stück, dringend',new=True),unit='Stück')
        self.assertEqual(second['state'],'approved')
        self.s.analyze(first['id'])
        with self.assertRaises(PermissionError):self.p.workshop_orders.submit_material_request(second['id'],second['revision'])
        self.s.order_attempt(second['id'],second['revision'],{'state':'blocked'})
        second=self.s.status(second['id'])
        self.assertEqual(second['state'],'open')
        self.assertEqual(second['duplicate_of'],first['id'])
        self.assertEqual(second['questions'][0]['field'],'possible_duplicate')
        self.assertEqual(self.answer(second,'Ja')['state'],'applied')
        self.assertEqual(self.s.status(second['id'])['state'],'approved')

    def test_duplicate_signature_changes_with_size_colour_and_packaging(self):
        first=self.review(self.photo('Ein Stück, dringend'),unit='Stück')
        signature=self.s._duplicate_signature(first['fields'],first['review'])
        for key,value in (('groesse','30 mm'),('farbe','blau'),('gebinde','12 Rollen'),('ve','Packung')):
            with self.subTest(key=key):
                fields=copy.deepcopy(first['fields'])
                fields['selected_article']['value'][key]=value
                self.assertNotEqual(self.s._duplicate_signature(fields,first['review']),signature)

    def test_external_reservation_does_not_bypass_additional_duplicate_confirmation(self):
        from test_material_external import ExternalOrderTests
        external=ExternalOrderTests('runTest')
        external.setUp()
        self.addCleanup(external.doCleanups)
        first=external.base.review(external.base.photo('Ein Stück, dringend'),unit='Stück')
        external.s.process_next()
        second=external.base.photo('Ein Stück, dringend',new=True)
        external.payload.update(article_number='TEST-50',product_name='Test-Klebeband',variant='grün 50 mm')
        with self.assertRaises(ValueError):external.reserve(second)
        self.assertEqual(external.s.status(first['id'])['state'],'accepted')
        self.assertEqual(external.base.answer(second,'Ja')['state'],'applied')
        second=external.s.status(second['id'])
        self.assertEqual(external.reserve(second)['state'],'external_pending')
        self.assertEqual(len(external.p.workshop_orders.calls),1)

    def test_late_duplicate_also_blocks_external_reservation_before_marker_exists(self):
        from test_material_external import ExternalOrderTests
        external=ExternalOrderTests('runTest')
        external.setUp()
        self.addCleanup(external.doCleanups)
        first=external.base.photo('Ein Stück, dringend')
        fields=copy.deepcopy(first['fields']);fields.pop('selected_article')
        external.f.sql("UPDATE einkauf_material_dialoge SET fields_json=?,analysis_json='{}',analysis_state='pending' WHERE id=?",
                       (json.dumps(fields),first['id']))
        second=external.base.review(external.base.photo('Ein Stück, dringend',new=True),unit='Stück')
        external.s.analyze(first['id'])
        external.payload.update(article_number='TEST-50',product_name='Test-Klebeband',variant='grün 50 mm')
        self.assertIsNone(external.s.status(second['id'])['duplicate_of'])
        with self.assertRaises(ValueError):external.reserve(second)
        self.assertEqual(external.p.workshop_orders.calls,[])

    def test_catalog_article_detects_prior_manual_order_with_free_text_variant(self):
        from test_material_external import ExternalOrderTests
        external=ExternalOrderTests('runTest')
        external.setUp()
        self.addCleanup(external.doCleanups)
        first=external.base.crystal_photo()
        first=external.reserve(first)
        external.base.hits=[{'produkt_name':'Test Crystal Silver','lieferant':'Testlieferant','artikelnummer':'TEST-4000',
                            'groesse':'0,5 Liter','farbe':'silber','gebinde':'Dose','ve':'Stück',
                            'quellen':[{'art':'einkauf','beleg_id':1,'position':1}]}]
        external.p.assistant_material_photos.vision=lambda *args: {
            'art':'produkt','produkt':'Test Crystal Silver','artikelnummer':'TEST-4000'}
        second=external.base.photo('Ein Stück, dringend',new=True)
        self.assertEqual(second['duplicate_of'],first['id'])
        self.assertIn('possible_duplicate',second['missing_fields'])
        self.assertEqual(external.p.workshop_orders.calls,[])

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

    def test_reanalyze_photo_rereads_original_clears_old_terms_and_keeps_employee_amount(self):
        view=self.review(self.photo('Ein Stück'),unit='Stück')
        original_photo=self.f.sql('SELECT assistant_photo_id,expected_sha256 FROM einkauf_material_nachrichten WHERE id=?',
                                  (view['message_id'],))[0]
        self.hits=[]
        calls=[]
        def vision(*args):
            current=self.s.status(view['id'])
            self.assertEqual(current['review'],{})
            self.assertNotIn('selected_article',current['fields'])
            self.assertNotEqual(current['state'],'approved')
            calls.append(True)
            return {'art':'produkt','marke':'Top-Color','farbe':'gelb','materialtyp':'Folie','masse':'5 x 120 m'}
        self.p.assistant_material_photos.vision=vision
        updated=self.s.reanalyze_photo(view['id'],view['revision'])
        self.assertEqual(calls,[True])
        self.assertEqual(updated['analysis_state'],'done')
        self.assertEqual(updated['analysis']['merkmale']['masse'],'5 x 120 m')
        self.assertEqual(updated['fields']['quantity']['value'],'1')
        self.assertEqual(updated['fields']['unit']['value'],'Stück')
        self.assertEqual(updated['review'],{})
        self.assertIn('article',updated['missing_fields'])
        self.assertIn('5 x 120 m',updated['questions'][0]['body'])
        self.assertEqual(self.f.sql('SELECT assistant_photo_id,expected_sha256 FROM einkauf_material_nachrichten WHERE id=?',
                                  (view['message_id'],))[0],original_photo)
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_reanalyze_rejects_stale_cancelled_text_and_inflight_requests(self):
        view=self.photo()
        with patch.object(self.p.assistant_material_photos,'vision') as vision:
            with self.assertRaises(ValueError):self.s.reanalyze_photo(view['id'],view['revision']-1)
            self.f.sql('UPDATE einkauf_material_dialoge SET analysis_until=? WHERE id=?',(self.f.time+60,view['id']))
            with self.assertRaises(ValueError):self.s.reanalyze_photo(view['id'],view['revision'])
            self.f.sql('UPDATE einkauf_material_dialoge SET analysis_until=0 WHERE id=?',(view['id'],))
            self.answer(view,'Abbrechen')
            view=self.s.status(view['id'])
            with self.assertRaises(ValueError):self.s.reanalyze_photo(view['id'],view['revision'])
            self.answer(None,'Ein Stück Klebeband bestellen',explicit=False)
            text=self.s.list()[0]
            with self.assertRaises(ValueError):self.s.reanalyze_photo(text['id'],text['revision'])
        vision.assert_not_called()

    def test_reanalyze_loses_old_selection_on_failure_without_ordering(self):
        view=self.review(self.photo())
        self.p.assistant_material_photos.vision=lambda *args: (_ for _ in ()).throw(ValueError('synthetic vision failed'))
        updated=self.s.reanalyze_photo(view['id'],view['revision'])
        self.assertEqual(updated['analysis_state'],'failed')
        self.assertNotIn('selected_article',updated['fields'])
        self.assertEqual(updated['review'],{})
        self.assertEqual(self.p.workshop_orders.calls,[])

    def test_reanalyze_never_overwrites_employee_change_or_revoked_rights(self):
        for change in ('reply','rights','durable'):
            with self.subTest(change=change):
                view=self.photo('Ein Stück',new=True)
                before_durable=[]
                def vision(*args):
                    if change=='reply':
                        current=self.s.status(view['id'])
                        self.assertEqual(self.answer(current,'Zwei Stück')['state'],'applied')
                    elif change=='rights':
                        self.f.sql('UPDATE assistent_rechte SET version=version+1 WHERE mitarbeiter_id=1')
                    else:
                        self.f.sql('INSERT INTO assistent_bestellanforderungen VALUES(?,?,?)',
                                   ('durable-'+str(view['id']),'mitarbeiter:1','material:'+str(view['id'])))
                        before_durable.append(self.f.sql('SELECT * FROM einkauf_material_dialoge WHERE id=?',(view['id'],))[0])
                    return {'art':'produkt','produkt':'Must not replace','artikelnummer':'NEW'}
                self.p.assistant_material_photos.vision=vision
                updated=self.s.reanalyze_photo(view['id'],view['revision'])
                self.assertNotEqual(updated['analysis'].get('merkmale',{}).get('produkt'),'Must not replace')
                if change=='reply':
                    self.assertEqual(updated['fields']['quantity']['value'],'2')
                elif change=='rights':
                    self.f.sql('UPDATE assistent_rechte SET version=version-1 WHERE mitarbeiter_id=1')
                else:
                    self.assertEqual(self.f.sql('SELECT * FROM einkauf_material_dialoge WHERE id=?',(view['id'],))[0],before_durable[0])
                self.p.assistant_material_photos.vision=lambda *args: {'art':'produkt','produkt':'Test-Klebeband','breite':'50 mm','farbe':'grün'}

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
        self.p.assistant_material_photos.vision=lambda *args: {
            'art':'produkt','produkt':'Test-Klebeband','farbe':'grün'}
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
        view=self.photo('Klebeband ein Karton dringend bestellen, aber erst am Montag')
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
        self.p.assistant_material_photos.vision=lambda *args: {
            'art':'produkt','produkt':'Test-Klebeband','farbe':'grün'}
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


class PhotoQuantityEndToEndTests(unittest.TestCase):
    def test_ten_different_photos_keep_each_quantity_and_replayed_webhook_never_resends(self):
        from test_materialbestellung_e2e import MaterialPurchaseEndToEndTests
        fixture=MaterialPurchaseEndToEndTests('runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        employee=fixture.f
        def catalog(index):
            name='Synthetischer Testartikel '+str(index)
            employee.hits[0].update(produkt_name=name,artikelnummer='TEST-'+str(index))
            employee.p.assistant_material_photos.vision=lambda *args: {
                'art':'produkt','produkt':name,'breite':'50 mm','farbe':'grün'}
        for index in range(10):
            catalog(index)
            old=employee.photo('Zwei Stück',new=True)
            old=employee.review(old,supplier_id=fixture.contact,article_number='TEST-'+str(index),
                product_name='Synthetischer Testartikel '+str(index),unit='Stück',unit_price_cents=1000,shipping_cents=0)
            employee.answer(old,'Abbrechen')
        fixture.f.f.time+=601
        fixture.manager.set_setting('worker_last_ok',fixture.f.f.time)
        drafts=[]
        for index in range(10):
            catalog(index)
            view=employee.photo('Ein Stück'+(', dringend' if index%2==0 else ''),new=True)
            self.assertEqual(view['state'],'approved')
            self.assertFalse(view['employee_reply_required'])
            self.assertIsNone(view['duplicate_of'])
            drafts.append(view['id'])
            self.assertEqual(employee.s.process_next()['state'],'sent' if index%2==0 else 'queued')
        self.assertEqual(len(set(drafts)),10)
        self.assertEqual(fixture.smtp.data_calls,5)
        fixture.f.f.ingest()
        employee.s.process_next()
        self.assertEqual(fixture.smtp.data_calls,5)
        for index,draft_id in enumerate(drafts):
            stored=employee.s.status(draft_id)
            self.assertEqual(stored['fields']['selected_article']['value']['artikelnummer'],'TEST-'+str(index))
            self.assertEqual(stored['fields']['quantity']['value'],'1')
        fixture.f.f.time=datetime(2026,10,12,13,59,tzinfo=BERLIN).timestamp()
        fixture.manager.tick(worker=True)
        self.assertEqual(fixture.smtp.data_calls,5)
        fixture.f.f.time=datetime(2026,10,12,14,0,tzinfo=BERLIN).timestamp()
        fixture.manager.tick(worker=True)
        self.assertEqual(fixture.smtp.data_calls,6)
        fixture.manager.tick(worker=True)
        employee.s.process_next()
        self.assertEqual(fixture.smtp.data_calls,6)

    def test_photo_then_one_piece_dispatches_urgent_once_otherwise_monday_14(self):
        from test_materialbestellung_e2e import MaterialPurchaseEndToEndTests
        fixture=MaterialPurchaseEndToEndTests('runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        employee=fixture.f
        prior=employee.photo('ein Stück',new=True)
        employee.review(prior,supplier_id=fixture.contact,unit='Stück',unit_price_cents=1000,shipping_cents=0)
        self.assertEqual(employee.s.process_next()['state'],'queued')
        fixture.f.f.time+=601
        fixture.manager.set_setting('worker_last_ok',fixture.f.f.time)
        employee.photo('dringend',new=True)
        self.assertEqual(employee.answer(None,'ein Stück',explicit=False)['state'],'applied')
        self.assertEqual(employee.s.process_next()['state'],'sent')
        self.assertEqual(fixture.smtp.data_calls,1)
        fixture.f.f.time+=601
        fixture.manager.set_setting('worker_last_ok',fixture.f.f.time)
        employee.photo('',new=True)
        self.assertEqual(employee.answer(None,'ein Stück',explicit=False)['state'],'applied')
        self.assertEqual(employee.s.process_next()['state'],'queued')
        fixture.f.f.time=datetime(2026,10,12,13,59,tzinfo=BERLIN).timestamp()
        fixture.manager.tick(worker=True)
        self.assertEqual(fixture.smtp.data_calls,1)
        fixture.f.f.time=datetime(2026,10,12,14,0,tzinfo=BERLIN).timestamp()
        fixture.manager.tick(worker=True)
        self.assertEqual(fixture.smtp.data_calls,2)
        fixture.manager.tick(worker=True)
        employee.s.process_next()
        self.assertEqual(fixture.smtp.data_calls,2)


if __name__=='__main__': unittest.main(verbosity=2)
