"""Isolated tests for the production cockpit API; no network or real customer data."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

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

    def test_documents_and_invoice_products_preserve_sources(self):
        db=p.get_db()
        try:
            db.execute("INSERT INTO dateien(id,auftrag_id,original_name,stored_name,extrahierter_text,hochgeladen_am) VALUES(901,901,'test.txt','private-path','Originaltest',?)",(p.now_str(),))
            db.execute("INSERT INTO einkauf_belege(id,original_name,extrahierter_text,erstellt_am) VALUES(901,'testrechnung.pdf','Artikel TEST-123',?)",(p.now_str(),))
            db.execute("INSERT INTO einkauf_artikel(id,artikelnummer,produkt_name,quelle_beleg_id,erstellt_am,geaendert_am) VALUES(901,'TEST-123','Testmaterial',901,?,?)",(p.now_str(),p.now_str()))
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
