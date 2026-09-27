"""Offline regression tests: synthetic data, no supplier or OpenAI calls."""
import concurrent.futures
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from contextlib import contextmanager
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP = tempfile.TemporaryDirectory()
os.environ.update(DATABASE_URL="", RENDER="1", DATA_DIR=TEMP.name,
                  SQLITE_DB_PATH=str(Path(TEMP.name) / "test.db"),
                  UPLOAD_DIR=str(Path(TEMP.name) / "uploads"),
                  AUTO_BACKUP_ENABLED="0", AUTO_CHANGE_BACKUP_ENABLED="0",
                  GOOGLE_ADS_AUTO_SYNC_ENABLED="0", MAILBOX_SEND_ENABLED="0",
                  ASSISTANT_ORDER_SEND_ENABLED="0", ASSISTANT_READ_ONLY="1",
                  ASSISTANT_COCKPIT_API_TOKEN="", ASSISTANT_COCKPIT_SNAPSHOT="",
                  OPENAI_API_KEY="", FLASK_SECRET_KEY="offline-assistant-test-secret")
import app as p
from PIL import Image
from werkzeug.security import generate_password_hash
from werkstatt_assistent import cents

p.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)

@contextmanager
def database():
    db=p.get_db()
    try:
        with db:
            yield db
    finally:
        db.close()


class AssistantTests(unittest.TestCase):
    def setUp(self):
        # Legacy write regressions remain opt-in; deployed registration defaults to read-only.
        self.mode = patch.dict(p.app.config, ASSISTANT_READ_ONLY=False, ASSISTANT_NATIVE_COCKPIT=True)
        self.mode.start()
        self.net = patch("werkstatt_assistent.requests.post", side_effect=AssertionError("Network forbidden"))
        self.net.start()
        self.key = patch.object(p, "get_openai_api_key", return_value="")
        self.key.start()
        self.guard = patch.object(p, "werkstatt_tafel_session_ok", return_value=True)
        self.guard.start()
        with database() as db:
            db.execute('PRAGMA foreign_keys=OFF')
            for table in ("assistent_audit", "assistent_dialog", "assistent_aktionen", "assistent_rechte", "assistent_profile", "dateien", "auftraege", "mitarbeiter"):
                db.execute("DELETE FROM " + table)
            db.execute("INSERT INTO mitarbeiter(id,name,aktiv,erstellt_am,geaendert_am) VALUES(1,'Testperson',1,?,?)", (p.now_str(),p.now_str()))
            db.execute("INSERT INTO assistent_rechte(mitarbeiter_id,passwort_hash,lesen,dokumentieren,einkaufen,limit_cent) VALUES(1,?,1,1,1,30000)", (generate_password_hash("test-passwort-123"),))
            for order in (156, 157):
                db.execute("INSERT INTO auftraege(id,fahrzeug,kennzeichen,beschreibung,erstellt_am,geaendert_am) VALUES(?, 'Testfahrzeug','TEST-1','Keine Freigabe',?,?)", (order, p.now_str(), p.now_str()))
        self.client = self.make_client()

    def tearDown(self):
        self.net.stop(); self.key.stop(); self.guard.stop()
        self.mode.stop()

    def make_client(self, admin=False):
        client = p.app.test_client()
        with client.session_transaction() as s:
            s.update(csrf_token="test-csrf", assistent_mid=1, assistent_version=1)
            if admin:
                s["admin"] = True
        return client

    def post(self, path, data=None, client=None):
        return (client or self.client).post('/werkstatt/assistent' + path, json=data or {}, headers={"X-CSRF-Token": "test-csrf"})

    def purchase(self, **overrides):
        return self.post('/vorschlag', dict(auftrag_id=156, art='einkauf', lieferant='K-Parts', teilenummer='TEST-123', bezeichnung='Testteil', menge=2, stueckpreis_brutto='140.00', versand_brutto='10.00', nebenkosten_brutto='10.00', **overrides))

    def test_pages_and_media_policy(self):
        r = self.client.get('/werkstatt/assistent')
        self.assertEqual(r.status_code, 200)
        self.assertIn('KI noch nicht eingerichtet', r.text)
        self.assertIn('camera=(self)', r.headers['Permissions-Policy'])
        self.assertIn("media-src 'self' blob:", r.headers['Content-Security-Policy'])
        self.assertEqual(self.make_client(admin=True).get('/werkstatt/assistent/rechte').status_code, 200)

    def test_auth_csrf_and_employee_forgery(self):
        anon = p.app.test_client()
        self.assertEqual(anon.get('/werkstatt/assistent/auftrag/156').status_code, 401)
        self.assertEqual(self.client.post('/werkstatt/assistent/vorschlag', json={}).status_code, 400)
        self.assertEqual(self.post('/vorschlag', {'actor':'admin','auftrag_id':999,'art':'notiz','text':'X'}).status_code, 400)
        self.assertEqual(self.client.get('/werkstatt/assistent/rechte').status_code, 302)

    def test_login_and_revocation(self):
        client=p.app.test_client()
        client.get('/werkstatt/assistent')
        with client.session_transaction() as s: token=s['csrf_token']
        r=client.post('/werkstatt/assistent/login', data={'csrf_token':token,'mitarbeiter_id':'1','password':'test-passwort-123'})
        self.assertEqual(r.status_code,302)
        self.assertEqual(client.get('/werkstatt/assistent/auftrag/156').status_code,200)
        with database() as db: db.execute('UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1')
        self.assertEqual(client.get('/werkstatt/assistent/auftrag/156').status_code,401)

    def test_note_requires_confirmation_and_idempotent(self):
        action=self.post('/vorschlag',{'auftrag_id':156,'art':'notiz','text':'Kotflügel demontiert'}).json
        with database() as db: self.assertEqual(db.execute('SELECT notiz_intern FROM auftraege WHERE id=156').fetchone()[0], '')
        for _ in range(2): self.assertEqual(self.post('/bestaetigen/'+action['id']).status_code,200)
        with database() as db:
            row=db.execute('SELECT notiz_intern,status FROM auftraege WHERE id=156').fetchone()
            self.assertEqual(row[0].count('Kotflügel demontiert'),1)
            self.assertEqual(row[1],1)

    def test_concurrent_confirm_only_once(self):
        action=self.post('/vorschlag',{'auftrag_id':156,'art':'notiz','text':'Einmal'}).json
        def confirm(_): return self.post('/bestaetigen/'+action['id'],client=self.make_client()).status_code
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool: self.assertEqual(list(pool.map(confirm,range(2))),[200,200])
        with database() as db: self.assertEqual(db.execute('SELECT notiz_intern FROM auftraege WHERE id=156').fetchone()[0].count('Einmal'),1)

    def test_purchase_exact_limit_duplicate_and_no_supplier(self):
        action=self.purchase().json
        self.assertEqual(action['daten']['gesamt_cent'],30000)
        self.assertEqual(self.purchase().json['id'],action['id'])
        self.assertEqual(self.post('/bestaetigen/'+action['id']).status_code,200)
        self.assertEqual(self.post('/bestellen/'+action['id']).status_code,503)

    def test_budget_includes_extras_and_current_rights(self):
        action=self.purchase().json
        with database() as db: db.execute('UPDATE assistent_rechte SET limit_cent=29999 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/bestaetigen/'+action['id']).status_code,400)
        self.assertEqual(self.post('/bestellen/'+action['id']).status_code,403)
        with database() as db: db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.purchase().status_code,400)

    def test_invalid_prices(self):
        for value in ('NaN','Infinity','-1','0.001',None,'unbekannt','100001'):
            with self.subTest(value=value):
                with self.assertRaises(ValueError): cents(value)
        self.assertEqual(cents('0,01'),1)

    @unittest.skipUnless(hasattr(p, 'assistent_datei_intern_sichtbar'), 'Legacy photo privacy guards absent; uploads are fail-closed')
    def test_photo_internal_and_duplicate(self):
        image=io.BytesIO(); Image.new('RGB',(10,10),'red').save(image,'JPEG'); raw=image.getvalue()
        for _ in range(2):
            r=self.client.post('/werkstatt/assistent/foto/156',data={'foto':(io.BytesIO(raw),'x.jpg')},headers={'X-CSRF-Token':'test-csrf'})
            self.assertEqual(r.status_code,200)
        with database() as db:
            rows=db.execute('SELECT * FROM dateien WHERE auftrag_id=156').fetchall()
            self.assertEqual(len(rows),1)
            row=dict(rows[0]); self.assertEqual(row['kategorie'],'assistent'); self.assertEqual(row['analyse_json'],'')
            for k,v in row.items():
                if 'freigabe' in k: self.assertFalse(v, k)
            self.assertEqual(db.execute('SELECT status FROM auftraege WHERE id=156').fetchone()[0],1)

    def test_invalid_photo_and_missing_key(self):
        r=self.client.post('/werkstatt/assistent/foto/156',data={'foto':(io.BytesIO(b'<script>'),'x.jpg')},headers={'X-CSRF-Token':'test-csrf'})
        self.assertEqual(r.status_code,400 if hasattr(p, 'assistent_datei_intern_sichtbar') else 403)
        self.assertEqual(self.post('/dialog',{'text':'Hallo'}).status_code,400)

    @unittest.skipUnless(hasattr(p, 'assistent_datei_intern_sichtbar'), 'Legacy photo privacy guards absent; uploads are fail-closed')
    def test_internal_photo_cannot_be_sent_or_downloaded_externally(self):
        photo={'kategorie':'assistent','quelle':'intern','auftrag_id':156}
        with p.app.test_request_context('/partner/kaesmann/datei/1'):
            self.assertFalse(p.assistent_datei_intern_sichtbar(photo))
            from werkzeug.exceptions import NotFound
            with self.assertRaises(NotFound): p.send_upload_file(photo)
        with p.app.test_request_context('/admin/auftrag/156'):
            from flask import session
            session['admin']=True
            self.assertTrue(p.assistent_datei_intern_sichtbar(photo))
        self.assertEqual(p.versicherung_mail_attachments([photo]), ([], [], 0))

    def test_audio_and_speech_provider_contract(self):
        self.key.stop(); self.key=patch.object(p,'get_openai_api_key',return_value='offline-test');self.key.start()
        class Response:
            content=b'fake-test-audio'
            def raise_for_status(self): pass
            def json(self): return {'text':'Auftrag 156'}
        with patch('werkstatt_assistent.requests.post', return_value=Response()) as mock:
            r=self.client.post('/werkstatt/assistent/audio',data={'audio':(io.BytesIO(b'test-audio'),'audio.webm','audio/webm')},headers={'X-CSRF-Token':'test-csrf'})
            self.assertEqual(r.json['text'],'Auftrag 156')
            self.assertTrue(mock.call_args.args[0].endswith('/audio/transcriptions'))
            r=self.post('/sprechen',{'text':'Testantwort'})
            self.assertEqual(r.mimetype,'audio/mpeg')
            self.assertEqual(mock.call_args.kwargs['json']['voice'],'coral')

    @unittest.skipUnless(hasattr(p, 'assistent_datei_intern_sichtbar'), 'Legacy photo privacy guards absent; uploads are fail-closed')
    def test_vision_failure_does_not_hide_saved_photo(self):
        self.key.stop(); self.key=patch.object(p,'get_openai_api_key',return_value='offline-test');self.key.start()
        image=io.BytesIO();Image.new('RGB',(10,10),'blue').save(image,'JPEG')
        import requests
        with patch('werkstatt_assistent.requests.post',side_effect=requests.Timeout):
            r=self.client.post('/werkstatt/assistent/foto/156',data={'foto':(io.BytesIO(image.getvalue()),'x.jpg'),'analyse':'1'},headers={'X-CSRF-Token':'test-csrf'})
        self.assertEqual(r.status_code,200)
        self.assertIn('Foto gespeichert',r.json['sichtung'])

    def test_document_permission_denied_and_archived_order(self):
        with database() as db: db.execute('UPDATE assistent_rechte SET dokumentieren=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/vorschlag',{'auftrag_id':156,'art':'notiz','text':'test'}).status_code,400)
        self.assertEqual(self.client.post('/werkstatt/assistent/foto/156',headers={'X-CSRF-Token':'test-csrf'}).status_code,403)
        with database() as db: db.execute('UPDATE auftraege SET archiviert=1 WHERE id=156')
        self.assertEqual(self.client.get('/werkstatt/assistent/auftrag/156').status_code,400)

    def test_dialog_executes_read_tool_and_separates_users(self):
        self.key.stop(); self.key=patch.object(p,'get_openai_api_key',return_value='offline-test');self.key.start()
        first={'output':[{'type':'function_call','name':'auftrag_lesen','arguments':'{"auftrag_id":156}','call_id':'c1'}]}
        second={'output':[{'type':'message','content':[{'type':'output_text','text':'Auftrag 156; Freigabe unbekannt.'}]}]}
        class Response:
            def __init__(self,data): self.data=data
            def raise_for_status(self): pass
            def json(self): return self.data
        with patch('werkstatt_assistent.requests.post',side_effect=[Response(first),Response(second)]) as mock:
            r=self.post('/dialog',{'text':'Auftrag 156 bitte'})
            self.assertEqual(r.status_code,200)
            self.assertEqual(r.json['events'][0]['data']['id'],156)
            self.assertFalse(mock.call_args.kwargs['json']['store'])
        self.assertEqual(self.post('/dialog/leeren').status_code,200)

    def test_profile_allowlist(self):
        self.assertEqual(self.post('/profil',{'name':'Chris','stil':'ruhig','stimme':'coral'}).status_code,200)
        self.assertEqual(self.post('/profil',{'name':'Chris','stil':'ruhig','stimme':'unbekannt'}).status_code,400)

    def test_voice_confirmation_requires_exact_readback_and_token(self):
        action=self.post('/vorschlag',{'auftrag_id':156,'art':'notiz','text':'Sprachtest'}).json
        self.assertEqual(self.post('/sprache-bestaetigen',{'text':'Auftrag 156 bestätigen'}).status_code,400)
        challenge=self.post('/vorlesen/'+action['id']).json
        self.assertIn('Sprachtest',challenge['text'])
        for text in ('Ja','Auftrag 157 bestätigen','Auftrag 156 bestätigen und etwas anderes'):
            self.assertEqual(self.post('/sprache-bestaetigen',{'nonce':challenge['nonce'],'text':text}).status_code,400)
        self.assertEqual(self.post('/sprache-bestaetigen',{'nonce':'falsch','text':challenge['phrase']}).status_code,400)
        self.assertEqual(self.post('/sprache-bestaetigen',{'nonce':challenge['nonce'],'text':challenge['phrase']}).status_code,200)
        self.assertEqual(self.post('/sprache-bestaetigen',{'nonce':challenge['nonce'],'text':challenge['phrase']}).status_code,400)

    def test_voice_confirmation_expires_and_rechecks_budget(self):
        action=self.purchase().json
        challenge=self.post('/vorlesen/'+action['id']).json
        with self.client.session_transaction() as s:
            c=dict(s['assistent_bestaetigung']);c['expires']=0;s['assistent_bestaetigung']=c
        self.assertEqual(self.post('/sprache-bestaetigen',{'nonce':challenge['nonce'],'text':challenge['phrase']}).status_code,400)
        challenge=self.post('/vorlesen/'+action['id']).json
        with database() as db:db.execute('UPDATE assistent_rechte SET limit_cent=1 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/sprache-bestaetigen',{'nonce':challenge['nonce'],'text':challenge['phrase']}).status_code,400)

    def test_unpriced_inquiry_email_is_draft_and_private(self):
        action=self.post('/vorschlag',{'auftrag_id':156,'art':'anfrage','lieferant':'K-Parts','teilenummer':'TEST-42','bezeichnung':'Testteil','menge':1}).json
        self.assertNotIn('gesamt_cent',action['daten'])
        response=self.client.get('/werkstatt/assistent/email/'+action['id'])
        self.assertEqual(response.status_code,200)
        from email import policy
        from email.parser import BytesParser
        message=BytesParser(policy=policy.default).parsebytes(response.data)
        self.assertEqual(message['X-Unsent'],'1')
        self.assertIsNone(message['To'])
        self.assertIn('keine Bestellung',message.get_content())
        self.assertEqual(self.make_client(admin=True).get('/werkstatt/assistent/email/'+action['id']).status_code,404)

    def test_avatar_choice_and_home_screen_manifest(self):
        self.assertEqual(self.post('/profil',{'name':'Chris','stil':'ruhig','stimme':'coral','avatar':'kupfer'}).status_code,200)
        html=self.client.get('/werkstatt/assistent').text
        self.assertIn('data-avatar="kupfer"',html)
        self.assertIn('Gespräch starten',html)
        self.assertIn('assistent.webmanifest',html)
        self.assertEqual(self.post('/profil',{'name':'Chris','stil':'ruhig','stimme':'coral','avatar':'unknown'}).status_code,400)


    def test_realtime_preload_and_session_contract(self):
        import json
        from unittest.mock import Mock
        with database() as db:
            db.execute("UPDATE auftraege SET archiviert=1 WHERE id=157")
        response=self.client.get('/werkstatt/assistent/realtime/kontext')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.headers['Cache-Control'],'no-store')
        self.assertIn('156',response.json['instructions'])
        self.assertNotIn('157',response.json['instructions'])
        self.assertEqual(self.post('/realtime/start',{'sdp':'invalid'}).status_code,400)
        provider=Mock(text='v=0\r\nanswer')
        with patch.object(p,'get_openai_api_key',return_value='test-key'), patch('werkstatt_assistent.requests.post',return_value=provider) as post:
            result=self.post('/realtime/start',{'sdp':'v=0\r\nsynthetic-offer','auftrag_id':156})
        self.assertEqual(result.status_code,200)
        self.assertNotIn('test-key',result.text)
        args=post.call_args
        self.assertTrue(args.args[0].endswith('/realtime/calls'))
        config=json.loads(args.kwargs['files']['session'][1])
        vad=config['audio']['input']['turn_detection']
        self.assertTrue(vad['interrupt_response'])
        self.assertLessEqual(vad['silence_duration_ms'],500)
        self.assertIn('156',config['instructions'])
        self.assertNotIn('bestaetigen',[t['name'] for t in config['tools']])

    def test_realtime_tools_recheck_rights_and_only_propose(self):
        result=self.post('/realtime/werkzeug',{'name':'auftrag_lesen','arguments':{'auftrag_id':156}})
        self.assertEqual(result.json['event']['data']['id'],156)
        result=self.post('/realtime/werkzeug',{'name':'aktion_vorschlagen','arguments':{'auftrag_id':156,'art':'notiz','text':'Realtime Test'}})
        self.assertEqual(result.json['event']['data']['status'],'vorschlag')
        with database() as db:
            self.assertEqual(db.execute('SELECT notiz_intern FROM auftraege WHERE id=156').fetchone()[0],'')
        self.assertEqual(self.post('/realtime/werkzeug',{'name':'bestaetigen','arguments':{}}).status_code,400)
        with database() as db:db.execute('UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1')
        self.assertEqual(self.client.get('/werkstatt/assistent/realtime/kontext').status_code,401)
        self.assertEqual(self.post('/realtime/werkzeug',{'name':'auftrag_lesen','arguments':{'auftrag_id':156}}).status_code,401)

    @patch.dict(p.app.config, ASSISTANT_NATIVE_COCKPIT=False)
    def test_cockpit_snapshot_readonly_no_demo_fallback(self):
        import json
        from datetime import datetime, timezone
        snapshot=Path(TEMP.name)/'cockpit.json'
        snapshot.write_text(json.dumps({'source':'https://kundenstatus-app.onrender.com','captured_at':datetime.now(timezone.utc).isoformat(),'orders':[{'id':900,'fahrzeug':'Synthetischer Audi','beschreibung':'Stoßfänger hinten lackieren','quelle':'https://kundenstatus-app.onrender.com/admin/auftrag/900'}]}),encoding='utf-8')
        with patch.dict(os.environ,{'ASSISTANT_COCKPIT_SNAPSHOT':str(snapshot)}):
            admin=self.make_client(admin=True)
            self.assertEqual(self.client.get('/werkstatt/assistent/quelle').status_code,403)
            source=admin.get('/werkstatt/assistent/quelle').json
            self.assertEqual(source['modus'],'lesestand')
            self.assertEqual(len(source['auftraege']),1)
            self.assertEqual(admin.get('/werkstatt/assistent/auftrag/900').json['beschreibung'],'Stoßfänger hinten lackieren')
            self.assertEqual(admin.get('/werkstatt/assistent/auftrag/156').status_code,400)
            self.assertEqual(admin.get('/werkstatt/assistent/aktionen').json,[])
            for route in ('/vorschlag','/bestaetigen/test','/foto/900','/bestellen/test','/sprache-bestaetigen'):
                self.assertEqual(self.post(route,client=admin).status_code,403)
            self.assertEqual(self.post('/realtime/werkzeug',{'name':'aktion_vorschlagen','arguments':{'auftrag_id':900,'art':'notiz','text':'X'}},client=admin).status_code,400)
            self.assertEqual(self.post('/realtime/werkzeug',{'name':'auftrag_lesen','arguments':{'auftrag_id':900}},client=admin).status_code,200)
            self.assertNotIn('Keine Freigabe',admin.get('/werkstatt/assistent/realtime/kontext').json['instructions'])

    @patch.dict(p.app.config, ASSISTANT_NATIVE_COCKPIT=False)
    def test_cockpit_snapshot_stale_or_foreign_source_fails_closed(self):
        import json
        from datetime import datetime, timezone
        import assistent_cockpit as cockpit
        snapshot=Path(TEMP.name)/'invalid-cockpit.json'
        for data in [
            {'source':'https://kundenstatus-app.onrender.com','captured_at':'2020-01-01T00:00:00Z','orders':[]},
            {'source':'https://example.org','captured_at':datetime.now(timezone.utc).isoformat(),'orders':[]}]:
            snapshot.write_text(json.dumps(data),encoding='utf-8')
            with patch.dict(os.environ,{'ASSISTANT_COCKPIT_SNAPSHOT':str(snapshot)}):
                with self.assertRaises(ValueError):cockpit.load_snapshot()
                self.assertEqual(self.make_client(admin=True).get('/werkstatt/assistent/auftrag/156').status_code,400)

    def test_direct_api_is_fail_closed_and_never_redirects_token(self):
        import assistent_cockpit as cockpit
        from unittest.mock import Mock
        with patch.dict(os.environ,{'ASSISTANT_COCKPIT_API_TOKEN':'synthetic-token'}):
            with patch('assistent_cockpit.requests.get',return_value=Mock(status_code=302,content=b'')) as call:
                with self.assertRaises(ValueError):cockpit.order_context(156)
                self.assertFalse(call.call_args.kwargs['allow_redirects'])
                self.assertTrue(call.call_args.args[0].startswith(cockpit.ORIGIN+'/api/werkstatt/v1/'))
            result=Mock(status_code=200,content=b'{}');result.json.return_value={'id':900,'fahrzeug':'Live-Test','dokumente':[]}
            with patch('assistent_cockpit.requests.get',return_value=result):
                self.assertEqual(cockpit.order_context(900)['modus'],'live')

    @patch.dict(p.app.config, ASSISTANT_NATIVE_COCKPIT=False)
    def test_overview_filters_live_events_and_preserves_paint_codes(self):
        admin=self.make_client(admin=True)
        events=[{'art':kind,'auftrag_id':index} for index,kind in enumerate(
            ('heute_faellig','anlieferung_heute','rueckbringung_heute','abholung_durch_werkstatt_heute'),1)]
        with patch('assistent_cockpit.api_enabled',return_value=True), patch('assistent_cockpit.api_read',return_value={'ereignisse':events,'speech_text':'Alle Termine'}) as remote:
            self.assertEqual([x['auftrag_id'] for x in admin.get('/werkstatt/assistent/ueberblick?ansicht=rein').json['eintraege']],[2,4])
            outgoing=admin.get('/werkstatt/assistent/ueberblick?ansicht=raus').json
            self.assertEqual([x['auftrag_id'] for x in outgoing['eintraege']],[1,3])
            self.assertNotEqual(outgoing['hinweis'],'Alle Termine')
            remote.return_value={'eintraege':[{'auftrag_id':1,'farbcode':'LY7W'}]}
            self.assertEqual(admin.get('/werkstatt/assistent/ueberblick?ansicht=lack&zeitraum=heute').json['eintraege'][0]['farbcode'],'LY7W')
            remote.assert_called_with('lackplan',{'zeitraum':'heute'})
            self.assertEqual(admin.get('/werkstatt/assistent/ueberblick?ansicht=lack&zeitraum=irgendwann').status_code,400)
        with patch('assistent_cockpit.api_enabled',return_value=True), patch('assistent_cockpit.api_read',side_effect=ValueError('Nicht erreichbar')):
            self.assertEqual(admin.get('/werkstatt/assistent/ueberblick').status_code,400)

    def test_assistant_has_schedule_document_and_invoice_tools(self):
        response=self.post('/realtime/werkzeug',{'name':'tagesplan','arguments':{'datum':'2026-09-28'}})
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['result']['datum'],'2026-09-28')
        with database() as db:db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/realtime/werkzeug',{'name':'artikel_suchen','arguments':{'suche':'TEST'}}).status_code,400)
        self.assertEqual(self.post('/realtime/werkzeug',{'name':'auftraege_suchen','arguments':{'suche':'Testfahrzeug'}}).status_code,200)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_native_readonly_is_server_enforced_for_employees_and_admin(self):
        with database() as db:
            before = tuple(db.execute('SELECT notiz_intern,status FROM auftraege WHERE id=156').fetchone())
        for client in (self.client, self.make_client(admin=True)):
            for route in ('/vorschlag','/bestaetigen/fake','/bestellen/fake','/foto/156','/vorlesen/fake','/sprache-bestaetigen'):
                self.assertEqual(self.post(route, {'auftrag_id':156,'art':'notiz','text':'Nicht speichern'}, client=client).status_code,403,route)
            self.assertEqual(client.get('/werkstatt/assistent/email/fake').status_code,403)
            for tool in ('aktion_vorschlagen','kamera','fortschritt_speichern'):
                self.assertEqual(self.post('/realtime/werkzeug',{'name':tool,'arguments':{'auftrag_id':156}},client=client).status_code,400)
            self.assertEqual(client.get('/werkstatt/assistent/aktionen').json,[])
        with database() as db:
            self.assertEqual(tuple(db.execute('SELECT notiz_intern,status FROM auftraege WHERE id=156').fetchone()),before)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM dateien').fetchone()[0],0)
        self.assertEqual(self.post('/profil',{'name':'Chris','stil':'knapp','stimme':'coral'}).status_code,200)
        self.assertIn('data-read-only="true"',self.client.get('/werkstatt/assistent').text)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_native_employee_reads_ignore_remote_tokens_and_recheck_rights(self):
        with patch.dict(os.environ, ASSISTANT_COCKPIT_API_TOKEN='secret-must-not-leak', ASSISTANT_COCKPIT_SNAPSHOT='missing-file'), patch('assistent_cockpit.requests.get',side_effect=AssertionError('No remote API allowed')):
            source=self.client.get('/werkstatt/assistent/quelle')
            self.assertEqual(source.status_code,200)
            self.assertTrue(source.json['native'])
            self.assertEqual(source.json['modus'],'live')
            self.assertEqual({o['id'] for o in source.json['auftraege']},{156,157})
            self.assertEqual(self.client.get('/werkstatt/assistent/auftrag/156').json['modus'],'live')
            for view in ('morgen','rein','raus','lack'):
                self.assertEqual(self.client.get('/werkstatt/assistent/ueberblick?ansicht='+view).status_code,200)
            for name in ('morgenueberblick','lackierplan'):
                self.assertEqual(self.post('/realtime/werkzeug',{'name':name,'arguments':{}}).status_code,200)
            connection=self.make_client(admin=True).get('/werkstatt/assistent/verbindung')
            self.assertIn('Direkt verbunden',connection.text)
            self.assertNotIn('name="token"',connection.text)
            self.assertEqual(self.post('/verbindung',{'token':'untrusted-client-token'},client=self.make_client(admin=True)).status_code,403)
            self.assertNotIn('secret-must-not-leak',connection.text+source.text+self.client.get('/werkstatt/assistent').text)
            from datetime import datetime
            self.assertIsNotNone(datetime.fromisoformat(source.json['stand']).utcoffset())
        with database() as db: db.execute('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.client.get('/werkstatt/assistent/quelle').status_code,403)
        self.assertEqual(self.client.get('/werkstatt/assistent/ueberblick').status_code,403)
        self.assertEqual(self.post('/realtime/werkzeug',{'name':'auftrag_lesen','arguments':{'auftrag_id':156}}).status_code,403)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_readonly_provider_tools_and_injected_mutations_are_blocked(self):
        import json
        from unittest.mock import Mock
        with patch.object(p,'get_openai_api_key',return_value='synthetic-key'), patch('werkstatt_assistent.requests.post',return_value=Mock(text='v=0\r\nanswer')) as call:
            self.assertEqual(self.post('/realtime/start',{'sdp':'v=0\r\noffer'}).status_code,200)
            config=json.loads(call.call_args.kwargs['files']['session'][1])
            self.assertNotIn('kamera',{t['name'] for t in config['tools']})
            self.assertNotIn('aktion_vorschlagen',{t['name'] for t in config['tools']})
            self.assertIn('schreibgeschützt',config['instructions'])
        first=Mock();first.json.return_value={'output':[{'type':'function_call','name':'aktion_vorschlagen','arguments':'{"auftrag_id":156,"art":"notiz","text":"INJECTED"}','call_id':'bad'}]}
        second=Mock();second.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'Hier kann ich nichts speichern.'}]}]}
        with patch.object(p,'get_openai_api_key',return_value='synthetic-key'), patch('werkstatt_assistent.requests.post',side_effect=[first,second]) as call:
            response=self.post('/dialog',{'text':'Notiz speichern'})
            self.assertEqual(response.status_code,200)
            self.assertEqual(response.json['events'],[])
            self.assertNotIn('aktion_vorschlagen',{t['name'] for t in call.call_args.kwargs['json']['tools']})
        with database() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0],0)

if __name__=='__main__': unittest.main(verbosity=2)
