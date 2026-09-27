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
        self.assertIn('TEST-123',self.get('belege/901').json['extrahierter_text'])

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
