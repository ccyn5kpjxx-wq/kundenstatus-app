"""Isolated tests for the production cockpit API; no network or real customer data."""
import hashlib
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
TMP=tempfile.TemporaryDirectory()
os.environ.update(DATABASE_URL='',RENDER='1',DATA_DIR=TMP.name,
    SQLITE_DB_PATH=str(Path(TMP.name)/'api.db'),UPLOAD_DIR=str(Path(TMP.name)/'uploads'),
    AUTO_BACKUP_ENABLED='0',AUTO_CHANGE_BACKUP_ENABLED='0',FLASK_SECRET_KEY='isolated-api-test')
os.environ.update(GOOGLE_ADS_AUTO_SYNC_ENABLED='0',MAILBOX_SEND_ENABLED='0',ASSISTANT_ORDER_SEND_ENABLED='0',OPENAI_API_KEY='')
import app as p
p.app.config.update(TESTING=True,SESSION_COOKIE_SECURE=False)


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.client=p.app.test_client()
        self.headers={'Authorization':'Bearer synthetic-api-secret'}
        self.grant(['auftraege:lesen','dokumente:lesen','einkauf:lesen'])
        p.set_app_setting('ASSISTANT_MATERIAL_SUPPLIERS','["Supplier Test"]')
        db=p.get_db()
        try:
            db.execute('PRAGMA foreign_keys=OFF') if not p.USE_POSTGRES else None
            for table in ('auftraege','dateien','autohaeuser','einkauf_artikel','einkauf_belege'):
                db.execute('DELETE FROM '+table)
            db.execute("INSERT INTO autohaeuser(id,name,slug,zugangscode,erstellt_am) VALUES(901,'Testpartner','testpartner','test-access','2026-09-27')")
            for oid,transport in [(901,'hol_und_bring'),(902,'standard')]:
                db.execute("INSERT INTO auftraege(id,autohaus_id,fahrzeug,kennzeichen,status,annahme_datum,fertig_datum,abholtermin,transport_art,erstellt_am,geaendert_am,token) VALUES(?,901,'Test-Auto','TEST',2,'2026-09-28','2026-09-28','29.09.2026',?,?,?,'must-not-leak')",(oid,transport,p.now_str(),p.now_str()))
            db.commit()
        finally:db.close()

    def grant(self,scopes):
        p.set_app_setting('ASSISTANT_API_GRANT',json.dumps({'hash':hashlib.sha256(b'synthetic-api-secret').hexdigest(),'scopes':scopes}))

    def get(self,path):return self.client.get('/api/werkstatt/v1/'+path,headers=self.headers)

    def update_order(self,oid,**values):
        db=p.get_db()
        try:
            db.execute('UPDATE auftraege SET '+','.join(key+'=?' for key in values)+' WHERE id=?',(*values.values(),oid))
            db.commit()
        finally:db.close()

    def paint_get(self,period='woche'):
        # Stable Monday fixture: tests do not depend on the developer's clock.
        with patch('werkstatt_cockpit_api.datetime',wraps=datetime) as clock:
            clock.now.return_value=datetime(2026,9,28,8,0,tzinfo=ZoneInfo('Europe/Berlin'))
            return self.get('lackplan?zeitraum='+period)

    def test_access_scope_and_revocation(self):
        self.assertEqual(self.client.get('/api/werkstatt/v1/auftraege').status_code,401)
        self.assertEqual(self.get('status').status_code,200)
        self.grant(['auftraege:lesen'])
        self.assertEqual(self.get('artikel?q=test').status_code,403)
        p.set_app_setting('ASSISTANT_API_GRANT','')
        self.assertEqual(self.get('auftraege').status_code,401)

    def test_pagination_search_and_secret_exclusion(self):
        result=self.get('auftraege?limit=1')
        self.assertEqual(result.status_code,200)
        self.assertEqual(result.json['next_offset'],1)
        self.assertEqual(result.headers['Cache-Control'],'no-store')
        self.assertNotIn('must-not-leak',result.text)
        self.assertEqual(len(self.get('auftraege?q=Testpartner').json['auftraege']),2)
        self.assertEqual(self.get('auftraege?limit=9999').status_code,400)
        self.assertEqual(self.get('auftraege/99999').status_code,400)

    def test_schedule_distinguishes_transport_and_customer(self):
        today=self.get('termine?datum=2026-09-28').json
        self.assertEqual(today['zeitzone'],'Europe/Berlin')
        kinds=[x['art'] for x in today['ereignisse']]
        self.assertEqual(kinds.count('fertig'),2)
        self.assertIn('abholen',kinds);self.assertIn('kunde_bringt',kinds)
        future=self.get('termine?datum=2026-09-29').json
        self.assertEqual({x['art'] for x in future['ereignisse']},{'zurueckbringen','kunde_holt'})
        self.assertEqual(self.get('termine?datum=tomorrow').status_code,400)

    def test_briefing_and_paint_plan_require_order_read_scope(self):
        paths=('briefing?datum=2026-09-28','lackplan?zeitraum=heute')
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(self.client.get('/api/werkstatt/v1/'+path).status_code,401)
        self.grant(['dokumente:lesen','einkauf:lesen'])
        for path in paths:self.assertEqual(self.get(path).status_code,403)
        self.grant(['auftraege:lesen'])
        for path in paths:
            result=self.get(path)
            self.assertEqual(result.status_code,200)
            self.assertEqual(result.headers['Cache-Control'],'no-store')
            self.assertNotIn('must-not-leak',result.text)
        self.assertEqual(self.get('briefing?datum=not-a-day').status_code,400)
        self.assertEqual(self.get('lackplan?zeitraum=jahr').status_code,400)
        p.set_app_setting('ASSISTANT_API_GRANT','')
        for path in paths:self.assertEqual(self.get(path).status_code,401)

    def test_briefing_and_schedule_keep_finish_and_transport_times_separate(self):
        self.update_order(901,annahme_uhrzeit='08:15',fertig_uhrzeit='14:30',
                          abholtermin='2026-09-28',abhol_uhrzeit='17:45')
        # A pickup time must never fill an absent completion hour.
        self.update_order(902,annahme_uhrzeit='09:00',fertig_uhrzeit='',
                          abholtermin='2026-09-28',abhol_uhrzeit='18:00')
        with patch.object(p,'get_auftrag',side_effect=AssertionError('Read API must not hydrate/write orders')):
            briefing=self.get('briefing?datum=2026-09-28')
            schedule=self.get('termine?datum=2026-09-28')
        self.assertEqual(briefing.status_code,200)
        self.assertEqual(schedule.status_code,200)
        events={(item['auftrag_id'],item['art']):item for item in briefing.json['ereignisse']}
        self.assertEqual(events[901,'heute_faellig']['uhrzeit'],'14:30')
        self.assertEqual(events[901,'heute_faellig']['uhrzeit_feld'],'fertig_uhrzeit')
        self.assertEqual(events[901,'abholung_durch_werkstatt_heute']['uhrzeit'],'08:15')
        self.assertEqual(events[901,'rueckbringung_heute']['uhrzeit'],'17:45')
        self.assertIsNone(events[902,'heute_faellig']['uhrzeit'])
        self.assertEqual(events[902,'heute_faellig']['uhrzeit_status'],'unbekannt')
        self.assertEqual(events[902,'anlieferung_heute']['uhrzeit'],'09:00')
        self.assertEqual(events[902,'kundenabholung_heute']['uhrzeit'],'18:00')
        self.assertIn('Uhrzeit unbekannt',briefing.json['speech_text'])
        self.assertEqual(events[901,'heute_faellig']['quelle'],'/admin/auftrag/901')
        times={(item['auftrag_id'],item['art']):item['uhrzeit'] for item in schedule.json['ereignisse']}
        self.assertEqual(times[901,'fertig'],'14:30')
        self.assertEqual(times[901,'abholen'],'08:15')
        self.assertEqual(times[901,'zurueckbringen'],'17:45')
        self.assertIn(times[902,'fertig'],('',None))

    def test_briefing_finished_order_only_retains_return_transport(self):
        self.update_order(901,status=4,fertig_datum='2026-09-27',abholtermin='2026-09-28')
        self.update_order(902,archiviert=1)
        report=self.get('briefing?datum=2026-09-28').json
        self.assertEqual([(item['auftrag_id'],item['art']) for item in report['ereignisse']],[(901,'rueckbringung_heute')])
        self.assertEqual(report['kategorien']['ueberfaellig'],[])
        self.assertEqual(report['kategorien']['heute_faellig'],[])
        self.update_order(901,status=5)
        self.assertEqual(self.get('briefing?datum=2026-09-28').json['ereignisse'],[])

    def test_paint_plan_preserves_colors_and_labels_finish_time_as_non_paint_schedule(self):
        self.update_order(901,beschreibung='Stoßfänger lackieren',farbcode='LY7W',farbton='Silber',
                          farbton_2='Schwarz',fertig_uhrzeit='14:30',abhol_uhrzeit='18:00')
        self.update_order(902,beschreibung='Tür lackieren',farbcode='',farbton='',farbton_2='',
                          annahme_datum='',start_datum='2026-10-02',fertig_datum='2026-10-02')
        today=self.paint_get('heute')
        week=self.paint_get('woche')
        self.assertEqual(today.status_code,200)
        self.assertEqual(today.json['datum'],'2026-09-28')
        self.assertEqual(today.json['bis'],'2026-09-28')
        self.assertEqual(week.json['bis'],'2026-10-04')
        self.assertEqual([item['auftrag_id'] for item in today.json['eintraege']],[901])
        items={item['auftrag_id']:item for item in week.json['eintraege']}
        self.assertEqual(set(items),{901,902})
        self.assertEqual((items[901]['farbcode'],items[901]['farbton'],items[901]['farbton_2']),('LY7W','Silber','Schwarz'))
        self.assertEqual(items[901]['uhrzeit'],'14:30')
        self.assertIn('Fertigfrist',items[901]['hinweis'])
        self.assertIn('eigener Lackiertermin ist nicht hinterlegt',items[901]['hinweis'])
        self.assertIn('Kabinen- oder Personalplan',week.json['hinweis'])
        self.assertIn('fehlende Farbcodes bleiben unbekannt',week.json['hinweis'])
        self.assertEqual(items[902]['farbcode'],'')
        self.assertEqual(items[901]['quelle'],'/admin/auftrag/901')
        order=self.get('auftraege/901').json
        self.assertEqual((order['farbcode'],order['farbton'],order['farbton_2']),('LY7W','Silber','Schwarz'))
        self.assertEqual(order['fertig_uhrzeit'],'14:30')
        self.assertNotIn('must-not-leak',week.text)

    def test_paint_plan_excludes_finish_complete_and_archived_but_keeps_active_paint(self):
        self.update_order(902,beschreibung='Räder wechseln',farbcode='',farbton='',farbton_2='')
        self.update_order(901,status=3,produktion_schritt='lackierung',fertig_datum='',start_datum='',fertig_uhrzeit='')
        active=self.paint_get('heute').json['eintraege']
        self.assertEqual([item['auftrag_id'] for item in active],[901])
        self.assertEqual(active[0]['art'],'Lackierung aktiv')
        self.assertEqual(active[0]['datum'],'')
        self.assertIsNone(active[0]['uhrzeit'])
        for status,stage,archived in ((3,'finish',0),(4,'lackierung',0),(5,'lackierung',0),(3,'lackierung',1)):
            with self.subTest(status=status,stage=stage,archived=archived):
                self.update_order(901,status=status,produktion_schritt=stage,archiviert=archived)
                self.assertEqual(self.paint_get('heute').json['eintraege'],[])

    def test_documents_and_invoice_products_preserve_sources(self):
        db=p.get_db()
        try:
            db.execute("INSERT INTO dateien(id,auftrag_id,original_name,stored_name,extrahierter_text,hochgeladen_am) VALUES(901,901,'test.txt','private-path','Originaltest',?)",(p.now_str(),))
            db.execute("INSERT INTO einkauf_belege(id,lieferant,original_name,extrahierter_text,erstellt_am) VALUES(901,'Top-Color GmbH','testrechnung.pdf','Artikel TEST-123',?)",(p.now_str(),))
            db.execute("INSERT INTO einkauf_artikel(id,lieferant,artikelnummer,produkt_name,quelle_beleg_id,erstellt_am,geaendert_am) VALUES(901,'Top-Color GmbH','TEST-123','Testmaterial',901,?,?)",(p.now_str(),p.now_str()))
            db.commit()
        finally:db.close()
        order=self.get('auftraege/901').json
        self.assertEqual(order['dokumente'][0]['id'],901)
        document=self.get('dokumente/901')
        self.assertEqual(document.json['extrahierter_text'],'Originaltest')
        self.assertNotIn('private-path',document.text)
        self.assertEqual(self.get('artikel?q=TEST-123').json['artikel'][0]['quelle_beleg_id'],901)
        invoice=self.get('belege/901').json
        self.assertEqual(invoice['artikel'][0]['artikelnummer'],'TEST-123')
        self.assertEqual(invoice['artikel'][0]['quelle']['beleg_id'],901)
        self.assertNotIn('extrahierter_text',invoice)

    def test_invoice_only_exposes_structured_products_and_price_provenance(self):
        secret='DE89370400440532013000'
        db=p.get_db()
        try:
            db.execute("INSERT INTO einkauf_belege(id,lieferant,original_name,extrahierter_text,erstellt_am) VALUES(903,'Top-Color GmbH','topcolor.pdf',?,?)",('Klebeband\nIBAN '+secret+'\nGesamtsumme 98765,43',p.now_str()))
            db.execute("INSERT INTO einkauf_artikel(id,lieferant,artikelnummer,produkt_name,gebinde,ve,letzter_preis,preisquelle,quelle_beleg_id,erstellt_am,geaendert_am) VALUES(903,'Top-Color GmbH','TAPE-50','Klebeband grün 50 mm','6 Rollen','Rolle','12,50 EUR',?,903,?,?)",('Bankverbindung '+secret,p.now_str(),p.now_str()))
            # Contaminated historical rows are excluded too, not relabeled as products.
            for oid,name,description in ((904,'Gesamtsumme','98765,43'),(905,'Klebeband','IBAN '+secret),(906,'Lastschrift','Konto belastet')):
                db.execute("INSERT INTO einkauf_artikel(id,lieferant,artikelnummer,produkt_name,produkt_beschreibung,quelle_beleg_id,erstellt_am,geaendert_am) VALUES(?,'Top-Color GmbH','FOOTER',?,?,903,?,?)",(oid,name,description,p.now_str(),p.now_str()))
            db.execute("INSERT INTO assistent_rechnungsimporte(id,source_key,source_kind,source_id,supplier,reference,state) VALUES(9903,'test-safe-903','einkauf','903','Top-Color GmbH','topcolor.pdf','pruefen')")
            payload={'lieferant':'Top-Color GmbH','produkt_name':'Klebeband grün 30 mm','artikelnummer':'TAPE-30','groesse':'30 mm','farbe':'grün','menge':'6','historischer_preishinweis':'10,25','bank_account':secret,'invoice_total':'98765,43','extrahierter_text':'IBAN '+secret,'quelle':{'art':'einkauf','beleg_id':'903','seite':2,'seiten':[2,3,True,0,-1,secret],'methode':'topcolor_word_columns_v1','position':3,'bank_data':secret},'price_evidence':{'value':'10.25','basis':'unknown','bank_account':secret,'invoice_total':'98765.43'}}
            db.execute("INSERT INTO assistent_rechnungsartikel(fingerprint,identity_key,import_id,supplier,product_name,article_number,payload_json,created_at) VALUES('safe903','safe903',9903,'Top-Color GmbH','Klebeband grün 30 mm','TAPE-30',?,?)",(json.dumps(payload),p.now_str()))
            db.commit()
        finally:db.close()
        response=self.get('belege/903')
        self.assertEqual(response.status_code,200)
        self.assertEqual([x['artikelnummer'] for x in response.json['artikel']],['TAPE-50'])
        item=response.json['artikel'][0]
        self.assertEqual((item['gebinde'],item['historischer_preishinweis']),('6 Rollen','12.50'))
        self.assertFalse(item['preis_geprueft'])
        self.assertEqual(item['quelle'],{'art':'einkauf','artikel_id':903,'beleg_id':903})
        candidate=response.json['artikelvorschlaege'][0]
        self.assertEqual((candidate['artikelnummer'],candidate['groesse'],candidate['farbe']),('TAPE-30','30 mm','grün'))
        self.assertEqual(candidate['quelle']['seite'],2)
        self.assertEqual(candidate['quelle']['seiten'],[2,3])
        self.assertEqual(candidate['quelle']['methode'],'topcolor_word_columns_v1')
        self.assertEqual(candidate['price_evidence']['value'],'10.25')
        self.assertFalse(candidate['bestellbar'])
        for forbidden in (secret,'98765','bank_account','invoice_total','extrahierter_text'):
            self.assertNotIn(forbidden,response.text)
        # The article-search tool must not reopen the full-text/footer path.
        search=self.get('artikel?q=Klebeband')
        self.assertEqual(search.status_code,200)
        self.assertEqual([x['artikelnummer'] for x in search.json['artikel']],['TAPE-50'])
        self.assertEqual(search.json['artikel'][0]['letzter_preis'],'12.50')
        self.assertEqual(search.json['artikelvorschlaege'][0]['quelle']['seiten'],[2,3])
        for forbidden in (secret,'98765','bank_account','invoice_total','extrahierter_text'):
            self.assertNotIn(forbidden,search.text)

    def test_order_text_parts_and_document_metadata_cannot_bypass_bank_filter(self):
        secret='DE89370400440532013000'
        self.update_order(901,beschreibung='Stoßfänger lackieren\nIBAN '+secret,
                          analyse_text='Klebeband 12,50 EUR\nBIC TESTDEFFXXX',
                          werkstatt_angebot_text='Lackierung freigegeben\nKontonummer 123456789')
        db=p.get_db()
        try:
            db.execute("INSERT INTO dateien(id,auftrag_id,original_name,stored_name,analyse_hinweis,hochgeladen_am) VALUES(921,901,'arbeit.pdf','private',?,?)",('OCR unsicher\nBankverbindung '+secret,p.now_str()))
            db.execute("INSERT INTO versicherung_teile(auftrag_id,bezeichnung,notiz,erstellt_am,geaendert_am) VALUES(901,'Klebeband',?,?,?)",('Geliefert\nIBAN '+secret,p.now_str(),p.now_str()))
            db.commit()
        finally:db.close()
        for path in ('auftraege','auftraege/901','briefing?datum=2026-09-28'):
            response=self.get(path)
            self.assertEqual(response.status_code,200)
            for forbidden in (secret,'TESTDEFFXXX','123456789'):
                self.assertNotIn(forbidden,response.text)
        detail=self.get('auftraege/901').json
        self.assertEqual(detail['beschreibung'],'Stoßfänger lackieren')
        self.assertEqual(detail['analyse_text'],'Klebeband 12,50 EUR')
        self.assertEqual(detail['teile'][0]['notiz'],'Geliefert')
        self.assertEqual(detail['dokumente'][0]['analyse_hinweis'],'OCR unsicher')
        self.assertTrue(detail['bankdaten_entfernt'])

    def test_invoice_without_structured_positions_does_not_fall_back_to_text(self):
        db=p.get_db()
        try:
            db.execute("INSERT INTO einkauf_belege(id,lieferant,original_name,extrahierter_text,erstellt_am) VALUES(904,'Car-Parts','rechnung.pdf','IBAN DE89370400440532013000\nMaterial ABC-123',?)",(p.now_str(),))
            db.commit()
        finally:db.close()
        response=self.get('belege/904')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['auslese_status'],'keine_strukturierten_artikel')
        self.assertEqual(response.json['artikel'],[])
        self.assertNotIn('ABC-123',response.text)
        self.assertNotIn('DE89370400440532013000',response.text)
        db=p.get_db()
        try:
            db.execute("UPDATE einkauf_belege SET lieferant='Volksbank' WHERE id=904")
            db.commit()
        finally:db.close()
        self.assertEqual(self.get('belege/904').status_code,400)

    def test_documents_block_commercial_bypass_but_preserve_work_and_prices(self):
        secret='DE89370400440532013000'
        db=p.get_db()
        try:
            for did,kind,name,category in ((910,'Rechnung','scan.pdf','standard'),(911,'','lieferantenrechnung.pdf','standard'),(912,'','scan.pdf','rechnung')):
                db.execute("INSERT INTO dateien(id,auftrag_id,original_name,stored_name,dokument_typ,kategorie,extrahierter_text,hochgeladen_am) VALUES(?,901,?,'private',?,?,'Nicht freigegebener Rechnungstext',?)",(did,name,kind,category,p.now_str()))
            db.execute("INSERT INTO dateien(id,auftrag_id,original_name,stored_name,dokument_typ,extrahierter_text,extrakt_kurz,analyse_json,analyse_hinweis,hochgeladen_am) VALUES(913,901,'dat.pdf','private','DAT-Kalkulation',?,?,?,?,?)",('Stoßfänger vorne lackieren\nArtikel Klebeband 12,50 EUR\nSumme Reparatur 100 EUR\nIBAN '+secret,'Lackierauftrag\nBIC TESTDEFFXXX',json.dumps({'arbeit':'lackieren','iban':secret}),'OCR prüfen\nBankverbindung geheim',p.now_str()))
            db.commit()
        finally:db.close()
        for did in (910,911,912):self.assertEqual(self.get('dokumente/'+str(did)).status_code,400)
        result=self.get('dokumente/913')
        self.assertEqual(result.status_code,200)
        self.assertIn('Stoßfänger vorne lackieren',result.json['extrahierter_text'])
        self.assertIn('12,50 EUR',result.json['extrahierter_text'])
        self.assertIn('Summe Reparatur 100 EUR',result.json['extrahierter_text'])
        self.assertTrue(result.json['bankdaten_entfernt'])
        self.assertIn('unvollständig',result.json['hinweis'])
        for forbidden in (secret,'TESTDEFFXXX','geheim','private'):
            self.assertNotIn(forbidden,result.text)

    def test_invoice_inventory_excludes_accounting_and_bank_fields(self):
        db=p.get_db()
        try:
            db.execute("DELETE FROM lexware_rechnungen")
            for kind,vid in [('purchaseinvoice','supplier-test'),('salesinvoice','customer-test')]:
                db.execute("INSERT INTO lexware_rechnungen(voucher_id,voucher_type,contact_name,voucher_number,status,total_amount,raw_json,erstellt_am,geaendert_am) VALUES(?,?,?,'INV-1','offen',98765,'bank-secret',?,?)",(vid,kind,'Supplier Test',p.now_str(),p.now_str()))
            db.commit()
        finally:db.close()
        result=self.get('belege')
        self.assertEqual(result.status_code,200)
        self.assertEqual([r['voucher_id'] for r in result.json['lieferantenrechnungen']],['supplier-test'])
        self.assertNotIn('bank-secret',result.text)
        self.assertNotIn('98765',result.text)
        db=p.get_db()
        try:
            db.execute("UPDATE lexware_rechnungen SET contact_name='Volksbank' WHERE voucher_id='supplier-test'")
            db.commit()
        finally:db.close()
        self.assertEqual(self.get('belege').json['lieferantenrechnungen'],[])
        self.grant(['auftraege:lesen'])
        self.assertEqual(self.get('belege').status_code,403)

    def test_invoice_catalog_requires_admin_csrf_and_keeps_read_grants_readonly(self):
        self.assertNotEqual(self.client.get('/admin/assistent-artikel').status_code,200)
        self.assertNotEqual(self.client.post('/admin/assistent-artikel/start',headers=self.headers).status_code,200)
        with self.client.session_transaction() as session:session.update(admin=True,csrf_token='test-csrf')
        self.assertEqual(self.client.get('/admin/assistent-artikel').status_code,200)
        self.assertEqual(self.client.post('/admin/assistent-artikel/start').status_code,400)
        result=self.client.post('/admin/assistent-artikel/start',headers={'X-CSRF-Token':'test-csrf'})
        self.assertEqual(result.status_code,200)
        self.assertIn('quellen',result.json)

    def test_key_management_requires_admin_and_csrf(self):
        self.assertNotEqual(self.client.get('/admin/assistent-api').status_code,200)
        with self.client.session_transaction() as session:session.update(admin=True,csrf_token='test-csrf')
        self.assertEqual(self.client.post('/admin/assistent-api',data={'aktion':'erstellen'}).status_code,400)
        result=self.client.post('/admin/assistent-api',data={'aktion':'erstellen','csrf_token':'test-csrf'})
        self.assertEqual(result.status_code,200)
        self.assertEqual(self.get('status').status_code,401,'rotation revokes old token')
        stored=json.loads(p.get_app_setting('ASSISTANT_API_GRANT'))
        self.assertEqual(len(stored['hash']),64)
        self.assertEqual(stored['scopes'],['auftraege:lesen'])
        result=self.client.post('/admin/assistent-api',data={'aktion':'erstellen','csrf_token':'test-csrf','scopes':['dokumente:lesen','banking:lesen','finanzen:lesen']})
        self.assertEqual(result.status_code,200)
        stored=json.loads(p.get_app_setting('ASSISTANT_API_GRANT'))
        self.assertEqual(stored['scopes'],['auftraege:lesen','dokumente:lesen'])


if __name__=='__main__':unittest.main(verbosity=2)
