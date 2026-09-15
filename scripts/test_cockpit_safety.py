"""Isolated behavioural tests: never reads customer DB or calls providers."""
from pathlib import Path
from io import BytesIO
from datetime import date, timedelta
from unittest.mock import patch
import os
import sys
import tempfile
import unittest
import json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP = Path(tempfile.mkdtemp(prefix='cockpit-safety-'))
os.environ.update({'RENDER':'cockpit-test', 'DATABASE_URL':'', 'REQUIRE_POSTGRES_ON_RENDER':'0',
    'DATA_DIR':str(TEMP), 'SQLITE_DB_PATH':str(TEMP/'test.db'), 'UPLOAD_DIR':str(TEMP/'uploads'),
    'BACKUP_DIR':str(TEMP/'backups'), 'DELETED_UPLOAD_DIR':str(TEMP/'deleted'),
    'AUTO_BACKUP_ENABLED':'0', 'AUTO_CHANGE_BACKUP_ENABLED':'0', 'OPENAI_API_KEY':'',
    'FLASK_SECRET_KEY':'cockpit-test', 'ADMIN_PASS':'cockpit-test', 'SCHADEN_SMTP_PASS':'',
    'LEXWARE_API_KEY':'', 'SMTP_PASSWORD':''})
import app as portal
from cockpit_rules import document_visible, exact_contact, price_record, decimal_input
from werkzeug.datastructures import FileStorage


def db_execute(sql, args=()):
    db = portal.get_db()
    cursor = db.execute(sql, args)
    result = cursor.lastrowid
    db.commit(); db.close()
    return result


def add_file(order_id, source='intern', category='standard', name='INTERN.pdf', analysis=None):
    path = portal.UPLOAD_DIR / f'{order_id}-{name}'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'%PDF-1.4\n%%EOF')
    return db_execute('INSERT INTO dateien (auftrag_id,original_name,stored_name,mime_type,size,quelle,kategorie,hochgeladen_am,analyse_json) VALUES (?,?,?,?,?,?,?,?,?)',
        (order_id,name,path.name,'application/pdf',path.stat().st_size,source,category,portal.now_str(),json.dumps(analysis or {})))


class CockpitSafety(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING'] = True
        portal.init_db()
        portal.schedule_change_backup = lambda reason: None
        # An accidental provider call fails the suite instead of sending anything.
        def denied(*args, **kwargs):
            raise AssertionError('External provider called by isolated test')
        portal.lexware_request = denied
        portal.post_whatsapp_payload = denied
        portal.send_lead_email = denied
        portal.notify_lead_workshop = lambda *args, **kwargs: None

    def setUp(self):
        self.admin = portal.app.test_client()
        self.public = portal.app.test_client()
        with self.admin.session_transaction() as session:
            session['admin'] = True
            session['csrf_token'] = 'test-csrf'
        self.order_id = portal.create_auftrag('intern', kunde_name='Test Kunde', kunde_email='kunde@example.test',
            fahrzeug='Testfahrzeug', kennzeichen='TEST-X 1', beschreibung='Bestätigter Umfang')
        self.order = portal.get_auftrag(self.order_id)

    def post(self, url, data=None):
        return self.admin.post(url, data={'csrf_token':'test-csrf', **(data or {})})

    def test_money_separation_and_rounding(self):
        record = price_record('2.750,00','19','Angebot')
        self.assertEqual(record['brutto'], '3272.50')
        self.assertEqual(record['mwst'], '522.50')
        for value in ('-1','NaN','Infinity','1.999','1e9','Preis 300','10000001'):
            with self.subTest(value=value), self.assertRaises(ValueError): decimal_input(value)

    def test_price_save_keeps_supplier_out_of_customer_amount(self):
        result = self.post(f'/admin/auftrag/{self.order_id}/preise', {'preisstand_version':'{}',
            'lieferant_netto':'890,41','lieferant_steuer':'19','lieferant_quelle':'Lieferant',
            'kunde_netto':'1400','kunde_steuer':'19','kunde_quelle':'Vereinbarung'})
        self.assertEqual(result.status_code,302)
        order = portal.get_auftrag(self.order_id)
        self.assertEqual(order['rep_max_kosten'], '')
        context = portal.build_lexware_rechnung_context(order)
        self.assertEqual(context['netto_betrag'],1400)
        self.assertNotIn('890',str(context['positionen']))
        # A stale form cannot replace a newer price agreement.
        self.post(f'/admin/auftrag/{self.order_id}/preise', {'preisstand_version':'{}','kunde_netto':'12','kunde_steuer':'19','kunde_quelle':'Alt'})
        self.assertEqual(portal.build_lexware_rechnung_context(portal.get_auftrag(self.order_id))['netto_betrag'],1400)

    def test_price_source_must_belong_to_order(self):
        foreign = portal.create_auftrag('intern',fahrzeug='Andere Bestellung')
        file_id = add_file(foreign)
        self.post(f'/admin/auftrag/{self.order_id}/preise', {'preisstand_version':'{}','kunde_netto':'300','kunde_steuer':'19','kunde_quelle':'Quelle','kunde_datei':str(file_id)})
        self.assertEqual(portal.get_auftrag(self.order_id)['preisstand_json'],'{}')

    def test_archive_does_not_analyse_or_reset_review(self):
        db_execute('UPDATE auftraege SET analyse_werkstatt_geprueft=1, analyse_pruefen=0 WHERE id=?',(self.order_id,))
        with patch.object(portal,'build_document_analysis_bundle_safe',side_effect=AssertionError('Archive analysed')):
            portal.save_uploads(self.order_id,[FileStorage(stream=BytesIO(b'%PDF-1.4\n%%EOF'), filename='SEPA.pdf')], 'intern', dokument_zweck='ablage')
        order=portal.get_auftrag(self.order_id)
        self.assertEqual(order['analyse_werkstatt_geprueft'],1)
        self.assertEqual(order['analyse_pruefen'],0)
        self.assertEqual(portal.list_document_review_items(self.order_id),[])

    def test_read_and_upload_never_adopt_recognized_values(self):
        data={'fahrzeug':'FALSCHER BMW','kennzeichen':'TEST-F 999','rep_max_kosten':'890,41','beschreibung':'Lieferantenrechnung'}
        with patch.object(portal,'build_document_analysis_bundle_safe',return_value={'analysis_json':json.dumps(data),'text':'','source':'test','status':'ok'}):
            portal.save_uploads(self.order_id,[FileStorage(stream=BytesIO(b'%PDF-1.4\n%%EOF'),filename='angebot.pdf')],'intern',apply_analysis=True)
        self.assertEqual(portal.get_auftrag(self.order_id)['fahrzeug'],'Testfahrzeug')
        self.assertEqual(portal.get_auftrag(self.order_id)['rep_max_kosten'],'')
        self.assertEqual(portal.get_auftrag(self.order_id)['beschreibung'],'Bestätigter Umfang')
        self.assertEqual(portal.apply_document_data_to_auftrag(self.order_id,True),{})

    def test_explicit_one_field_adoption_and_stale_rejection(self):
        file_id=add_file(self.order_id,analysis={'fahrzeug':'VW Tiguan','kennzeichen':'TEST-B 2','rep_max_kosten':'750'})
        self.assertTrue(portal.list_document_review_items(self.order_id))
        self.post(f'/admin/auftrag/{self.order_id}/auslese', {'datei_id':str(file_id),'feld':'kennzeichen','alter_wert':'TEST-X 1','neuer_wert':'TEST-B 2'})
        order=portal.get_auftrag(self.order_id)
        self.assertEqual(order['fahrzeug'],'Testfahrzeug');self.assertEqual(order['kennzeichen'],'TEST-B 2')
        self.post(f'/admin/auftrag/{self.order_id}/auslese', {'datei_id':str(file_id),'feld':'kennzeichen','alter_wert':'TEST-X 1','neuer_wert':'TEST-B 2'})
        self.assertEqual(portal.get_auftrag(self.order_id)['kennzeichen'],'TEST-B 2')
        self.assertEqual(self.post(f'/admin/auftrag/{self.order_id}/auslese', {'datei_id':str(file_id),'feld':'rep_max_kosten'}).status_code,400)

    def test_customer_direct_routes_enforce_revoke_and_ownership(self):
        lead=portal.create_lead({'website':'auto-lackierzentrum','quelle':'website','kunde_name':'Test'})
        db_execute('UPDATE leads SET auftrag_id=? WHERE id=?',(self.order_id,lead))
        file_id=add_file(self.order_id,source='kunde')
        token=self.order['kunden_status_token']
        routes=[f'/status/{token}/bild/{file_id}',f'/status/{token}/dokument/{file_id}']
        for url in routes:
            with self.public.get(url) as response: self.assertEqual(response.status_code,200)
        with portal.app.test_request_context(): url=portal.url_for('admin_datei_kunde_sichtbar',datei_id=file_id)
        # force known visible state, then existing toggle must revoke effectively
        db_execute('UPDATE dateien SET kunde_sichtbar=1,sichtbarkeit_geprueft=1 WHERE id=?',(file_id,))
        self.post(url)
        for url in routes: self.assertEqual(self.public.get(url).status_code,404)
        self.post(f'/admin/datei/{file_id}/freigaben',{'kunde':'1'})
        for url in routes:
            with self.public.get(url) as response: self.assertEqual(response.status_code,200)
        foreign=portal.create_auftrag('intern',fahrzeug='Andere')
        foreign_file=add_file(foreign,source='kunde')
        self.assertEqual(self.public.get(f'/status/{token}/bild/{foreign_file}').status_code,404)

    def test_lead_privacy_and_conversion(self):
        lead_id=portal.create_lead({'website':'auto-lackierzentrum','quelle':'website','kunde_name':'Test'})
        token=portal.get_lead(lead_id)['kunden_status_token']
        path=portal.UPLOAD_DIR/'lead.pdf';path.write_bytes(b'%PDF-1.4\n%%EOF')
        private=db_execute("INSERT INTO lead_dateien (lead_id,original_name,stored_name,mime_type,size,quelle,erstellt_am) VALUES (?,?,?,?,?,'intern',?)", (lead_id,'PRIVAT.pdf',path.name,'application/pdf',len(path.read_bytes()),portal.now_str()))
        self.assertEqual(self.public.get(f'/status/{token}/bild/{private}').status_code,404)
        portal.copy_lead_attachments_to_order(lead_id,self.order_id)
        files=portal.list_dateien(self.order_id)
        self.assertFalse(document_visible(files[0],'kunde'))
        self.post(f'/admin/lead/{lead_id}/datei/{private}/freigabe',{'kunde':'1'})
        with self.public.get(f'/status/{token}/bild/{private}') as response: self.assertEqual(response.status_code,200)

    def test_customer_history_omits_internal_and_survives_conversion(self):
        lead=portal.create_lead({'website':'auto-lackierzentrum','quelle':'website','kunde_name':'Test'})
        db_execute('UPDATE leads SET auftrag_id=? WHERE id=?',(self.order_id,lead))
        for visible,msg in [(1,'Anfrage vor Auftrag'),(0,'VERTRAULICHER EINKAUF')]:
            db_execute("INSERT INTO lead_portal_log (lead_id,quelle,titel,nachricht,kunden_sichtbar,erstellt_am) VALUES (?,'werkstatt','Info',?,?,?)",(lead,msg,visible,portal.now_str()))
        portal.add_benachrichtigung(self.order_id,'Intern','Rabatt geheim',quelle='intern')
        portal.add_benachrichtigung(self.order_id,'Kundentext','Hallo',quelle='kunde')
        self.post(f'/admin/auftrag/{self.order_id}/kundenantwort',{'nachricht':'Freigegebene Antwort','veroeffentlichen':'1'})
        history=str(portal.customer_history(auftrag_id=self.order_id))
        self.assertIn('Anfrage vor Auftrag',history);self.assertIn('Hallo',history);self.assertIn('Freigegebene Antwort',history)
        self.assertNotIn('VERTRAULICH',history);self.assertNotIn('Rabatt geheim',history)

    def test_ambiguous_or_conflicting_contact_rejected(self):
        person={'name':'Max Beispiel','email':'max@example.test','plz':'12345','ort':'Berlin'}
        contact={'id':'one','person':{'firstName':'Max','lastName':'Beispiel'},'addresses':{'billing':[{'street':'A','zip':'99999','city':'Hamburg'}]},'emailAddresses':{'business':['max@example.test']}}
        with self.assertRaises(ValueError):exact_contact([contact],person)
        contact['addresses']['billing'][0].update(zip='12345',city='Berlin')
        self.assertEqual(exact_contact([contact],person)['id'],'one')
        with self.assertRaises(ValueError):exact_contact([contact,contact],person)
        with self.assertRaises(ValueError):exact_contact([contact],{'name':'Ganz anderer Name'})
        contact['emailAddresses']['business']=['anderer@example.test']
        with self.assertRaises(ValueError):exact_contact([contact],person)

    def test_invoice_existing_attachment_blocks_creation(self):
        db_execute('UPDATE auftraege SET status=5 WHERE id=?',(self.order_id,))
        add_file(self.order_id,category='rechnung')
        with patch.object(portal,'create_lexware_invoice_draft') as create:
            self.post(f'/admin/auftrag/{self.order_id}/rechnung/lexware',{'netto_betrag':'300','rechnung_pruefung_bestaetigt':'1'})
            create.assert_not_called()

    def test_invoice_confirmation_and_amount_required(self):
        db_execute('UPDATE auftraege SET status=5 WHERE id=?',(self.order_id,))
        with patch.object(portal,'create_lexware_invoice_draft') as create:
            self.post(f'/admin/auftrag/{self.order_id}/rechnung/lexware',{'netto_betrag':'300'})
            self.post(f'/admin/auftrag/{self.order_id}/rechnung/lexware',{'netto_betrag':'-300','rechnung_pruefung_bestaetigt':'1'})
            create.assert_not_called()

    def test_uncertain_invoice_never_retries_automatically(self):
        db_execute('UPDATE auftraege SET status=5 WHERE id=?',(self.order_id,))
        with patch.object(portal,'create_lexware_invoice_draft',side_effect=TimeoutError('Test timeout')) as create:
            for i in range(2):self.post(f'/admin/auftrag/{self.order_id}/rechnung/lexware',{'netto_betrag':'300','rechnung_pruefung_bestaetigt':'1'})
            self.assertEqual(create.call_count,1)
        self.assertEqual(portal.get_auftrag(self.order_id)['rechnung_erstellung_status'],'pruefen')

    def test_confirmed_tax_rate_used_in_payload(self):
        item=portal.build_lexware_invoice_line_item({'bezeichnung':'Lackieren'},'Test',100,7)
        self.assertEqual(item['unitPrice']['taxRatePercentage'],7)

    def test_running_invoice_cannot_be_reset_or_reentered(self):
        db_execute('UPDATE auftraege SET status=5 WHERE id=?',(self.order_id,))
        def in_flight(*args):
            self.assertEqual(self.post(f'/admin/auftrag/{self.order_id}/rechnung/pruefung',{'kein_beleg_bestaetigt':'1'}).status_code,400)
            self.post(f'/admin/auftrag/{self.order_id}/rechnung/lexware',{'netto_betrag':'300','rechnung_pruefung_bestaetigt':'1'})
            raise TimeoutError('Isolated test')
        with patch.object(portal,'create_lexware_invoice_draft',side_effect=in_flight) as create:
            self.post(f'/admin/auftrag/{self.order_id}/rechnung/lexware',{'netto_betrag':'300','rechnung_pruefung_bestaetigt':'1'})
            self.assertEqual(create.call_count,1)

    def test_customer_update_date_across_month_boundary(self):
        events=[{'erstellt_am':'31.08.2026 10:00'}, {'erstellt_am':'01.09.2026 10:00'}]
        self.assertEqual(portal.customer_updated_at(events,[]),'01.09.2026 10:00')
        self.assertEqual(portal.customer_updated_at(events,[{'hochgeladen_am':'02.09.2026 08:00'}]),'02.09.2026 08:00')

    def test_planning_excludes_future_locked_and_assigned(self):
        today=date.today()
        base={'id':1,'status':3,'versicherung_id':0,'fahrzeug':'Test'}
        entries=[base,{**base,'id':2,'status':2,'start_datum':(today+timedelta(days=2)).isoformat()},
            {**base,'id':3,'versicherung_id':1,'versicherung_freigabe_status':'offen'},
            {**base,'id':4}, {**base,'id':5,'archiviert':1}]
        selected=portal.zuweisbare_tagesauftraege(entries,[{'auftrag_id':4}],today)
        self.assertEqual([e['id'] for e in selected],[1])

    def test_document_actions_count_unique_orders(self):
        for _ in range(2):add_file(self.order_id, name=f'doc-{_}.pdf',analysis={'fahrzeug':'BMW'})
        db_execute('UPDATE auftraege SET analyse_pruefen=1,analyse_werkstatt_geprueft=0 WHERE id=?',(self.order_id,))
        with portal.app.test_request_context():
            actions=portal.cockpit_aktionsuebersicht([portal.get_auftrag(self.order_id)])
        self.assertEqual(len(actions['groups']['dokumente']),1)
        self.assertIn('2 Unterlage',actions['groups']['dokumente'][0]['detail'])
        self.assertEqual(actions['total'],sum(map(len,actions['groups'].values())))

    def test_new_mutations_require_admin_and_csrf(self):
        self.assertNotEqual(self.public.post(f'/admin/auftrag/{self.order_id}/preise').status_code,200)
        self.assertEqual(self.admin.post(f'/admin/auftrag/{self.order_id}/preise',data={}).status_code,400)

    def test_actual_order_invoice_and_actions_render(self):
        add_file(self.order_id)
        for url in (f'/admin/auftrag/{self.order_id}',f'/admin/auftrag/{self.order_id}/rechnung','/admin/cockpit','/admin/aufgaben'):
            with self.subTest(url=url):self.assertEqual(self.admin.get(url).status_code,200)


if __name__ == '__main__':
    unittest.main(verbosity=2)
