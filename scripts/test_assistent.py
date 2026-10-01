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

    def rendered_profile(self, client=None):
        # Inspect the server contract independently of the figure artwork/UI.
        with patch('werkstatt_assistent.render_template',return_value='synthetic-profile') as render:
            self.assertEqual((client or self.client).get('/werkstatt/assistent').status_code,200)
            return render.call_args.kwargs['profile']

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

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_native_personal_login_without_workshop_code_or_privilege_escalation(self):
        self.guard.stop()  # Exercise the real workshop gate: no shared code/session.
        client=p.app.test_client()
        response=client.get('/werkstatt/assistent')
        self.assertEqual(response.status_code,200)
        self.assertIn('name="mitarbeiter_id"',response.text)
        self.assertNotIn('Keine Freigabe',response.text)
        with client.session_transaction() as s:
            token=s['csrf_token']
            self.assertFalse(s.get('werkstatt_tafel'))
            self.assertFalse(s.get('admin'))
        data={'mitarbeiter_id':'1','password':'test-passwort-123'}
        self.assertEqual(client.post('/werkstatt/assistent/login',data=data).status_code,400)
        data['csrf_token']=token
        self.assertEqual(client.post('/werkstatt/assistent/login',data=dict(data,password='falsch')).status_code,401)
        response=client.post('/werkstatt/assistent/login',data=data)
        self.assertEqual(response.status_code,302)
        self.assertEqual(response.location,'/werkstatt/assistent')
        with client.session_transaction() as s:
            self.assertEqual(s['assistent_mid'],1)
            self.assertEqual(s['assistent_version'],1)
            self.assertTrue(s.permanent)
            self.assertFalse(s.get('werkstatt_tafel'))
            self.assertFalse(s.get('admin'))
        self.assertEqual(client.get('/werkstatt/assistent/auftrag/156').status_code,200)
        self.assertEqual(client.get('/werkstatt/assistent/ueberblick').status_code,200)
        self.assertNotEqual(client.get('/admin/mitarbeiter').status_code,200)
        self.assertEqual(client.get('/werkstatt/tafel').status_code,302)
        self.assertEqual(client.post('/werkstatt/assistent/vorschlag',json={'auftrag_id':156,'art':'notiz','text':'Keine Änderung'},headers={'X-CSRF-Token':token}).status_code,403)
        with database() as db:db.execute('UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1')
        self.assertEqual(client.get('/werkstatt/assistent/auftrag/156').status_code,401)

    def test_native_login_keeps_activity_rights_ratelimit_and_own_logout(self):
        self.guard.stop()
        client=p.app.test_client()
        client.get('/werkstatt/assistent')
        with client.session_transaction() as s: token=s['csrf_token']
        data={'mitarbeiter_id':'1','password':'test-passwort-123','csrf_token':token}
        with patch.object(p,'login_rate_limit_status',return_value=(True,60)):
            self.assertEqual(client.post('/werkstatt/assistent/login',data=data).status_code,429)
        with database() as db:db.execute('UPDATE mitarbeiter SET aktiv=0 WHERE id=1')
        self.assertEqual(client.post('/werkstatt/assistent/login',data=data).status_code,401)
        with database() as db:db.execute('UPDATE mitarbeiter SET aktiv=1 WHERE id=1')
        self.assertEqual(client.post('/werkstatt/assistent/login',data=data).status_code,302)
        with database() as db:db.execute('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(client.get('/werkstatt/assistent/auftrag/156').status_code,403)
        with client.session_transaction() as s:
            s['werkstatt_tafel']='independent-existing-workshop-session'
            s['other_preference']='keep'
        self.assertEqual(client.post('/werkstatt/assistent/logout',data={'csrf_token':token}).status_code,302)
        with client.session_transaction() as s:
            self.assertNotIn('assistent_mid',s)
            self.assertNotIn('assistent_version',s)
            self.assertEqual(s['werkstatt_tafel'],'independent-existing-workshop-session')
            self.assertEqual(s['other_preference'],'keep')
        self.assertEqual(client.get('/werkstatt/assistent/auftrag/156').status_code,401)

    @patch.dict(p.app.config, ASSISTANT_NATIVE_COCKPIT=False)
    def test_remote_comparison_still_requires_workshop_gate(self):
        self.guard.stop()
        client=self.make_client()
        self.assertEqual(client.get('/werkstatt/assistent').status_code,302)
        self.assertEqual(client.get('/werkstatt/assistent/auftrag/156').status_code,401)
        self.assertEqual(client.post('/werkstatt/assistent/login',data={'mitarbeiter_id':'1','password':'test-passwort-123','csrf_token':'test-csrf'}).status_code,403)

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

    def test_character_defaults_and_invalid_stored_values_have_safe_fallback(self):
        self.assertEqual(self.rendered_profile()['character'],'drache')
        with database() as db:
            db.execute("INSERT INTO assistent_profile(actor,name,stil,stimme) VALUES('mitarbeiter:1','Existing name','knapp','ash')")
        self.assertEqual(self.rendered_profile()['character'],'drache')
        for stored in ('', 'removed-figure', '../../arbitrary.svg'):
            with database() as db:db.execute("UPDATE assistent_profile SET character=? WHERE actor='mitarbeiter:1'",(stored,))
            visible=self.rendered_profile()
            self.assertEqual((visible['character'],visible['name'],visible['stimme']),('drache','Existing name','ash'))
            with database() as db:self.assertEqual(db.execute("SELECT character FROM assistent_profile WHERE actor='mitarbeiter:1'").fetchone()[0],stored)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_fantasy_characters_and_legacy_choices_persist_in_both_profile_routes(self):
        preferences={'name':'Eigener Rufname','stil':'ruhig','stimme':'ash','avatar':'blau'}
        self.assertEqual(self.post('/profil',preferences).status_code,200)
        before=self.rendered_profile()
        self.assertEqual(before['character'],'drache')
        with database() as db:
            rights_before=dict(db.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=1').fetchone())
        for character in ('drache','zauberfuchs','einhorn','phoenix','greif','waldgeist','chris','mila','robot'):
            with self.subTest(character=character):
                self.assertEqual(self.post('/profil',dict(preferences,character=character)).status_code,200)
                self.assertEqual(self.post('/avatar',{'character':character}).json,{'ok':True,'character':character})
                self.assertEqual(self.rendered_profile(),dict(before,character=character))
                # Old clients may save names/voices without knowing new figures.
                self.assertEqual(self.post('/profil',preferences).status_code,200)
                self.assertEqual(self.rendered_profile(),dict(before,character=character))
                with database() as db:
                    self.assertEqual(db.execute("SELECT character FROM assistent_profile WHERE actor='mitarbeiter:1'").fetchone()[0],character)
                    self.assertEqual(dict(db.execute('SELECT * FROM assistent_rechte WHERE mitarbeiter_id=1').fetchone()),rights_before)

    def test_profile_character_and_old_color_are_independent_and_invalid_is_atomic(self):
        first={'name':'Werkstatt','stil':'knapp','stimme':'ash','avatar':'kupfer','character':'robot'}
        self.assertEqual(self.post('/profil',first).status_code,200)
        saved=self.rendered_profile()
        self.assertEqual((saved['character'],saved['avatar']),('robot','kupfer'))
        old_client={'name':'Legacy client','stil':'ruhig','stimme':'coral','avatar':'blau'}
        self.assertEqual(self.post('/profil',old_client).status_code,200)
        saved=self.rendered_profile()
        self.assertEqual((saved['character'],saved['avatar'],saved['name']),('robot','blau','Legacy client'))
        for invalid in ('unknown','../../image.svg',None,True,['mila'],{'id':'mila'}):
            response=self.post('/profil',dict(first,name='Must not save',character=invalid))
            self.assertEqual(response.status_code,400)
            self.assertEqual(self.rendered_profile(),saved)
        self.assertEqual(self.post('/profil',dict(first,avatar='invalid',character='mila')).status_code,400)
        self.assertEqual(self.rendered_profile(),saved)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_character_picker_changes_only_own_character_and_preserves_preferences(self):
        first={'name':'Individueller Name','stil':'knapp','stimme':'ash','avatar':'kupfer','character':'chris'}
        self.assertEqual(self.post('/profil',first).status_code,200)
        before=self.rendered_profile()
        admin=self.make_client(admin=True)
        self.assertEqual(self.post('/avatar',{'character':'robot'},client=admin).json,{'ok':True,'character':'robot'})
        admin_before=self.rendered_profile(admin)
        response=self.post('/avatar',{'character':'mila'})
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json,{'ok':True,'character':'mila'})
        self.assertEqual(self.rendered_profile(),dict(before,character='mila'))
        self.assertEqual(self.rendered_profile(admin),admin_before)
        self.assertEqual((admin_before['name'],admin_before['stil'],admin_before['stimme']),('Chris','kollegial','coral'))
        # Reject attempts to target another actor or modify unrelated preferences.
        for extra in ({'actor':'admin'},{'name':'Another name'},{'stimme':'coral'},{'avatar':'mint'}):
            self.assertEqual(self.post('/avatar',dict(extra,character='robot')).status_code,400)
        self.assertEqual(self.rendered_profile(),dict(before,character='mila'))
        self.assertEqual(self.rendered_profile(admin),admin_before)
        # A later save from an old profile form cannot undo the picker.
        first.pop('character')
        self.assertEqual(self.post('/profil',first).status_code,200)
        self.assertEqual(self.rendered_profile()['character'],'mila')

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_character_picker_rechecks_auth_csrf_rights_and_validation(self):
        anon=p.app.test_client()
        with anon.session_transaction() as s:s['csrf_token']='test-csrf'
        self.assertEqual(self.post('/avatar',{'character':'mila'},client=anon).status_code,401)
        self.assertEqual(self.client.post('/werkstatt/assistent/avatar',json={'character':'mila'}).status_code,400)
        for invalid in ({},{'character':None},{'character':[]},{'character':'unknown'}):
            self.assertEqual(self.post('/avatar',invalid).status_code,400)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_profile').fetchone()[0],0)
            db.execute('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/avatar',{'character':'mila'}).status_code,403)
        with database() as db:db.execute('UPDATE assistent_rechte SET lesen=1,version=2 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/avatar',{'character':'mila'}).status_code,401)

    def test_profile_and_character_reject_nonobject_json_without_changes(self):
        import json
        initial={'name':'Keep this name','stil':'ruhig','stimme':'ash','avatar':'kupfer','character':'mila'}
        self.assertEqual(self.post('/profil',initial).status_code,200)
        before=self.rendered_profile()
        for route in ('/avatar','/profil'):
            for value in ([],['mila'],'mila',True,7,None):
                with self.subTest(route=route,value=value):
                    response=self.client.post('/werkstatt/assistent'+route,data=json.dumps(value),content_type='application/json',headers={'X-CSRF-Token':'test-csrf'})
                    self.assertEqual(response.status_code,400)
                    self.assertEqual(response.json['error'],'JSON-Objekt erforderlich.')
                    self.assertEqual(self.rendered_profile(),before)


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

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_voice_start_and_refresh_share_bounded_context_and_full_order_tool(self):
        import copy
        import json
        import time
        from datetime import datetime, timezone
        from unittest.mock import Mock
        description = 'Lackierauftrag ÄÖÜ: Stoßfänger rechts bearbeiten. ' * 1000
        selected = {'id': 156, 'fahrzeug': 'Testfahrzeug', 'kennzeichen': 'TEST-1',
            'beschreibung': description, 'analyse_text': description,
            'werkstatt_angebot_text': description, 'status': 2,
            'farbcode': 'SYNTHETIC-COLOR', 'stand': '2026-10-01T12:00:00+02:00',
            'dokumente': [{'id': 800, 'original_name': 'synthetischer-auftrag.pdf'}],
            'quelle': '/admin/auftrag/156'}
        index = {'auftraege': [{**selected, 'id': number} for number in range(200, 140, -1)], 'next_offset': 60}
        before_index, before_selected = copy.deepcopy(index), copy.deepcopy(selected)
        self.assertGreater(len(json.dumps(index)), 77000)
        provider = Mock(text='v=0\r\nsynthetic-answer')
        provider.json.return_value = {'value': 'ek_synthetic_short_lived_credential', 'expires_at': int(time.time()) + 60}
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-key'), \
             patch('werkstatt_assistent.workshop_now', return_value=datetime(2026,10,1,10,tzinfo=timezone.utc)), \
             patch.object(p.cockpit_data, 'orders', return_value=index), \
             patch.object(p.cockpit_data, 'order', return_value=selected) as full_order, \
             patch.object(p.cockpit_data, 'material_context', return_value={'varianten': []}), \
             patch('werkstatt_assistent.requests.post', return_value=provider) as call:
            refresh = self.client.get('/werkstatt/assistent/realtime/kontext?auftrag_id=156')
            self.assertEqual(refresh.status_code, 200)
            instructions = refresh.json['instructions']
            raw_context = instructions.rsplit('\nAKTENSTAND ', 1)[1].split(': ', 1)[1]
            context = json.loads(raw_context)
            self.assertLessEqual(len(raw_context), 8000)
            self.assertLessEqual(len(raw_context.encode('utf-8')), 12000)
            self.assertTrue(context['gekuerzt'])
            self.assertEqual(context['ausgewaehlter_auftrag']['id'], 156)
            self.assertTrue(context['ausgewaehlter_auftrag']['arbeitsdetails_abrufen'])
            self.assertIn('ausgelassene_felder', context['ausgewaehlter_auftrag'])
            self.assertIn('dokument_lesen', instructions)
            self.assertIn('Weggelassene oder gekürzte Angaben bedeuten nicht', instructions)
            for transport in ('server', 'browser'):
                response = self.post('/realtime/start', {'transport': transport, 'sdp': 'v=0\r\nsynthetic-offer', 'auftrag_id': 156})
                self.assertEqual(response.status_code, 200, response.text)
                config = (call.call_args.kwargs['json']['session'] if transport == 'browser'
                          else json.loads(call.call_args.kwargs['files']['session'][1]))
                self.assertEqual(config['instructions'], instructions)
                self.assertNotIn('bestaetigen', {item['name'] for item in config['tools']})
            result = self.post('/realtime/werkzeug', {'name': 'auftrag_lesen', 'arguments': {'auftrag_id': 156}})
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json['result']['beschreibung'], description)
            self.assertEqual(result.json['result']['analyse_text'], description)
            self.assertEqual(result.json['result']['dokumente'], selected['dokumente'])
            full_order.reset_mock()
            with database() as db:
                db.execute('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1')
            self.assertEqual(self.post('/realtime/werkzeug', {'name': 'auftrag_lesen', 'arguments': {'auftrag_id': 156}}).status_code, 403)
            self.assertEqual(self.client.get('/werkstatt/assistent/realtime/kontext?auftrag_id=156').status_code, 403)
            full_order.assert_not_called()
        self.assertEqual(index, before_index)
        self.assertEqual(selected, before_selected)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_voice_compaction_does_not_change_text_dialog_context(self):
        import copy
        import json
        from unittest.mock import Mock
        index = {'auftraege': [{'id': 156, 'beschreibung': 'FULL_TEXT_CONTEXT ' * 5000}], 'next_offset': None}
        before = copy.deepcopy(index)
        provider = Mock()
        provider.json.return_value = {'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'Synthetische Antwort.'}]}]}
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-key'), \
             patch.object(p.cockpit_data, 'orders', return_value=index), \
             patch('werkstatt_sprachkontext.compact_voice_context', side_effect=AssertionError('Text must remain unchanged')), \
             patch('werkstatt_assistent.requests.post', return_value=provider) as call:
            response = self.post('/dialog', {'text': 'Was steht in Auftrag 156?'})
        self.assertEqual(response.status_code, 200)
        instructions = call.call_args.kwargs['json']['instructions']
        context = json.loads(instructions.rsplit('\nAKTENSTAND ', 1)[1].split(': ', 1)[1])
        self.assertEqual(context['auftraege'], index['auftraege'])
        self.assertNotIn('Der Sprachkontext ist eine gekürzte Übersicht.', instructions)
        self.assertEqual(index, before)

    def test_browser_realtime_uses_same_trusted_session_and_only_ephemeral_response(self):
        import json
        import time
        from datetime import datetime, timezone
        from unittest.mock import Mock
        now = int(time.time())
        secret = 'ek_synthetic_short_lived_credential'
        provider = Mock(text='v=0\r\nsynthetic-answer')
        provider.json.return_value = {'value': secret, 'expires_at': now + 60,
            'session': {'instructions': 'PRIVATE_PROVIDER_SESSION'}, 'api_key': 'PRIVATE_PROVIDER_KEY'}
        with patch.object(p, 'get_openai_api_key', return_value='sk-synthetic-server-only'), \
             patch('werkstatt_assistent.time.time', return_value=now), \
             patch('werkstatt_assistent.workshop_now', return_value=datetime(2026,10,1,tzinfo=timezone.utc)), \
             patch.object(p.cockpit_data, 'orders', return_value={'auftraege': [], 'next_offset': None}), \
             patch.object(p.cockpit_data, 'material_context', return_value={'varianten': []}), \
             patch('werkstatt_assistent.requests.post', return_value=provider) as call:
            old = self.post('/realtime/start', {'sdp': 'v=0\r\nsynthetic-offer'})
            old_config = json.loads(call.call_args.kwargs['files']['session'][1])
            self.assertEqual(old.status_code, 200)
            self.assertEqual(old.json, {'sdp': 'v=0\r\nsynthetic-answer'})
            result = self.post('/realtime/start', {'sdp': 'v=0\r\nsynthetic-offer', 'transport': 'browser',
                'session': {'instructions': 'CLIENT_OVERRIDE', 'tools': []}, 'model': 'CLIENT_MODEL',
                'expires_after': {'seconds': 7200}})
            sent = call.call_args
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json, {'client_secret': secret, 'expires_at': now + 60})
        self.assertEqual(result.headers['Cache-Control'], 'no-store')
        self.assertEqual(sent.args[0], 'https://api.openai.com/v1/realtime/client_secrets')
        self.assertEqual(sent.kwargs['json'], {'session': old_config,
            'expires_after': {'anchor': 'created_at', 'seconds': 60}})
        self.assertNotIn('files', sent.kwargs)
        self.assertEqual(sent.kwargs['headers'], {'Authorization': 'Bearer sk-synthetic-server-only'})
        self.assertNotIn('CLIENT_OVERRIDE', json.dumps(sent.kwargs['json']))
        for private in ('PRIVATE_PROVIDER', 'sk-synthetic-server-only', 'instructions', 'tools'):
            self.assertNotIn(private, result.text)
        with self.client.session_transaction() as state:
            self.assertNotIn(secret, str(dict(state)))
        with database() as db:
            for table in ('assistent_dialog', 'assistent_audit', 'assistent_aktionen'):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 0)

    def test_browser_realtime_validates_transport_sdp_auth_csrf_and_current_rights(self):
        body = {'transport': 'browser', 'sdp': 'v=0\r\nsynthetic-offer'}
        # setUp's provider mock rejects every unexpected network invocation.
        for transport in ('unknown', '', None, [], {}, True):
            self.assertEqual(self.post('/realtime/start', {**body, 'transport': transport}).status_code, 400)
        for sdp in ('invalid', 'v=0' + 'x' * 64000, None, []):
            self.assertEqual(self.post('/realtime/start', {**body, 'sdp': sdp}).status_code, 400)
        self.assertEqual(self.client.post('/werkstatt/assistent/realtime/start', json=body).status_code, 400)
        anonymous = p.app.test_client()
        with anonymous.session_transaction() as state:
            state['csrf_token'] = 'test-csrf'
        self.assertEqual(self.post('/realtime/start', body, anonymous).status_code, 401)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/realtime/start', body).status_code, 403)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET lesen=1,version=2 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/realtime/start', body).status_code, 401)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET version=1 WHERE mitarbeiter_id=1')
            db.execute('UPDATE mitarbeiter SET aktiv=0 WHERE id=1')
        self.assertEqual(self.post('/realtime/start', body).status_code, 401)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_browser_realtime_tools_follow_rights_and_model_is_trimmed(self):
        import time
        from unittest.mock import Mock
        with database() as db:
            db.execute('UPDATE assistent_rechte SET einkaufen=0,dokumentieren=0 WHERE mitarbeiter_id=1')
        provider = Mock()
        provider.json.return_value = {'value': 'ek_synthetic_short_lived_credential', 'expires_at': int(time.time()) + 60}
        for configured, expected in [('  gpt-realtime  ', 'gpt-realtime'), (' \t ', 'gpt-realtime')]:
            with patch.dict(os.environ, {'ASSISTANT_REALTIME_MODEL': configured}), \
                 patch.object(p, 'get_openai_api_key', return_value='sk-synthetic-server-only'), \
                 patch('werkstatt_assistent.requests.post', return_value=provider) as call:
                result = self.post('/realtime/start', {'transport': 'browser', 'sdp': 'v=0\r\nsynthetic-offer'})
            self.assertEqual(result.status_code, 200)
            config = call.call_args.kwargs['json']['session']
            self.assertEqual(config['model'], expected)
            names = {tool['name'] for tool in config['tools']}
            self.assertIn('auftrag_lesen', names)
            self.assertTrue(names.isdisjoint({'beleg_lesen', 'bestellung_vorschlagen', 'status_vorschlagen', 'bestaetigen'}))
            self.assertNotIn('"materialwissen":', config['instructions'])

    def test_browser_realtime_invalid_credentials_fail_closed_without_provider_data(self):
        import time
        from unittest.mock import Mock
        now = int(time.time())
        valid = {'value': 'ek_synthetic_short_lived_credential', 'expires_at': now + 60}
        cases = [None, [], {}, {'client_secret': valid},
                 {**valid, 'value': 'sk-synthetic-server-only'}, {**valid, 'value': ''},
                 {**valid, 'value': 'ek_INVALID\r\nPRIVATE_PROVIDER'}, {**valid, 'value': ['PRIVATE_PROVIDER']},
                 {**valid, 'value': 'ek_' + 'x' * 2049},
                 {**valid, 'expires_at': now - 1}, {**valid, 'expires_at': now},
                 {**valid, 'expires_at': now + 600}, {**valid, 'expires_at': str(now + 60)},
                 {**valid, 'expires_at': True}, {**valid, 'expires_at': None}]
        for payload in cases:
            with self.subTest(payload=payload):
                provider = Mock()
                provider.json.return_value = payload
                with patch.object(p, 'get_openai_api_key', return_value='sk-synthetic-server-only'), \
                     patch('werkstatt_assistent.time.time', return_value=now), \
                     patch('werkstatt_assistent.requests.post', return_value=provider):
                    result = self.post('/realtime/start', {'transport': 'browser', 'sdp': 'v=0\r\nsynthetic-offer'})
                self.assertEqual(result.status_code, 400)
                self.assertEqual(set(result.json), {'error'})
                self.assertIn('keinen gültigen kurzlebigen Sprachzugang', result.json['error'])
                self.assertEqual(result.headers['Cache-Control'], 'no-store')
                for private in ('sk-synthetic-server-only', 'ek_', 'PRIVATE_PROVIDER'):
                    self.assertNotIn(private, result.text)
        provider = Mock()
        provider.json.side_effect = ValueError('PRIVATE_PROVIDER sk-synthetic-server-only')
        with patch.object(p, 'get_openai_api_key', return_value='sk-synthetic-server-only'), \
             patch('werkstatt_assistent.requests.post', return_value=provider):
            result = self.post('/realtime/start', {'transport': 'browser', 'sdp': 'v=0\r\nsynthetic-offer'})
        self.assertEqual(result.status_code, 400)
        self.assertNotIn('PRIVATE_PROVIDER', result.text)
        self.assertNotIn('sk-synthetic-server-only', result.text)

    def provider_failure(self, status, payload, *, raw=False):
        """Exercise real requests.HTTPError, with hostile synthetic data only."""
        import json
        import requests
        response = requests.Response()
        response.status_code = status
        response._content = (payload if raw else json.dumps(payload)).encode('utf-8')
        response.headers.update({'X-Private-Provider': 'PRIVATE_CUSTOMER',
                                 'Retry-After': 'PRIVATE_CUSTOMER',
                                 'Set-Cookie': 'provider_secret=PRIVATE_CUSTOMER'})
        response.url = 'https://api.openai.com/v1/realtime/calls?private=PRIVATE_CUSTOMER'
        response.request = requests.Request('POST', response.url,
            headers={'Authorization': 'Bearer synthetic-private-key'},
            data='PRIVATE_CUSTOMER').prepare()
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-private-key'), \
             patch('werkstatt_assistent.requests.post', return_value=response) as provider:
            result = self.post('/realtime/start', {'sdp': 'v=0\r\nsynthetic-offer'})
        self.assertEqual(provider.call_count, 1, 'No automatic provider retry')
        self.assertEqual(result.status_code, 400)
        self.assertEqual(set(result.json), {'error'})
        self.assertEqual(result.headers['Cache-Control'], 'no-store')
        self.assertNotIn('X-Private-Provider', result.headers)
        self.assertNotIn('Retry-After', result.headers)
        visible = result.text + str(list(result.headers))
        for secret in ('synthetic-private-key', 'PRIVATE_CUSTOMER', 'provider_secret'):
            self.assertNotIn(secret, visible)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0], 0)
        return result.json['error']

    def test_provider_http_failures_have_safe_distinct_german_categories(self):
        cases = [
            (401, {}, 'KI-Zugang abgelehnt'),
            (403, {}, 'KI-Zugriff nicht erlaubt'),
            (404, {}, 'Modell oder Endpunkt nicht verfügbar'),
            (429, {'code': 'insufficient_quota'}, 'KI-Kontingent oder Kostenlimit'),
            (429, {'code': 'credit_balance_exhausted'}, 'KI-Kontingent oder Kostenlimit'),
            (429, {'code': 'organization_spend_limit_exceeded'}, 'KI-Kontingent oder Kostenlimit'),
            (429, {'code': 'project_spend_limit_exceeded'}, 'KI-Kontingent oder Kostenlimit'),
            (429, {'code': 'organization_usage_limit_exceeded'}, 'KI-Kontingent oder Kostenlimit'),
            (429, {'type': 'insufficient_quota'}, 'KI-Kontingent oder Kostenlimit'),
            (429, {'code': 'rate_limit_exceeded'}, 'Zu viele KI-Anfragen'),
            (429, {'code': 'slow_down'}, 'Zu viele KI-Anfragen'),
            (429, {'type': 'rate_limit_error'}, 'Zu viele KI-Anfragen'),
            (429, {}, 'ist unbekannt'),
            (400, {'code': 'invalid_request_error', 'param': 'session.audio.output.voice'}, 'Betroffener Bereich: Stimme'),
            (500, {}, 'vorübergehende Serverstörung'),
            (503, {}, 'vorübergehende Serverstörung'),
        ]
        for status, fields, expected in cases:
            with self.subTest(status=status, fields=fields):
                message = self.provider_failure(status, {'error': {**fields,
                    'message': 'PRIVATE_CUSTOMER synthetic-private-key'}})
                self.assertIn(expected, message)
                self.assertIn('HTTP ' + str(status), message)
                self.assertIn('Keine Aktion automatisch ausgeführt.', message)

    def test_provider_error_metadata_is_untrusted_and_never_reflected(self):
        cases = [(400, {'error': {'code': 'PRIVATE_CUSTOMER', 'param': 'session.tools.PRIVATE_CUSTOMER', 'message': 'PRIVATE_CUSTOMER'}}),
                 (400, {'error': {'param': ['PRIVATE_CUSTOMER']}}),
                 (429, {'error': {'code': {'PRIVATE_CUSTOMER': 1}, 'type': ['PRIVATE_CUSTOMER']}}),
                 (429, ['PRIVATE_CUSTOMER']), (400, {'error': 'PRIVATE_CUSTOMER'}),
                 (400, None), (418, {'error': {'message': 'PRIVATE_CUSTOMER'}})]
        for status, payload in cases:
            with self.subTest(status=status, payload=payload):
                message = self.provider_failure(status, payload)
                self.assertNotIn('Betroffener Bereich', message)
        self.provider_failure(400, '<html>PRIVATE_CUSTOMER synthetic-private-key</html>', raw=True)

    def test_provider_400_reason_categories_do_not_echo_messages(self):
        cases = [({'code': 'context_length_exceeded'}, 'Sitzungskontext'),
                 ({'message': 'Maximum context length PRIVATE_CUSTOMER'}, 'Sitzungskontext'),
                 ({'code': 'string_above_max_length'}, 'Zeichenanzahl'),
                 ({'message': 'Instructions are too long: PRIVATE_CUSTOMER'}, 'Sitzungsanweisungen'),
                 ({'message': 'Instructions cannot exceed PRIVATE_CUSTOMER'}, 'Sitzungsanweisungen'),
                 ({'message': 'Instructions cannot be longer than 16384 tokens, you have provided 29523 tokens.'}, 'Sitzungsanweisungen'),
                 ({'code': 'invalid_function_parameters'}, 'Werkzeugformat'),
                 ({'message': 'Invalid schema for function PRIVATE_CUSTOMER'}, 'Werkzeugformat'),
                 ({'code': 'model_not_found'}, 'Modell ist unbekannt'),
                 ({'message': 'Unknown model PRIVATE_CUSTOMER'}, 'Modell ist unbekannt'),
                 ({'message': 'Unsupported voice PRIVATE_CUSTOMER'}, 'Stimme wurde abgelehnt'),
                 ({'code': 'unknown_parameter'}, 'Konfigurationsparameter wird nicht unterstützt')]
        for fields, expected in cases:
            with self.subTest(fields=fields):
                message = self.provider_failure(400, {'error': {'message': 'PRIVATE_CUSTOMER synthetic-private-key', **fields}})
                self.assertIn(expected, message)
                self.assertNotIn('Diagnose:', message)
        for value in (None, ['PRIVATE_CUSTOMER'], {'PRIVATE_CUSTOMER': 1}, 3,
                      'PRIVATE_CUSTOMER ' * 500 + 'instructions too long'):
            self.assertNotIn('Hinweis:', self.provider_failure(400, {'error': {'message': value}}))

    def test_realtime_provider400_summary_is_admin_only_bounded_and_content_free(self):
        import json
        import requests
        admin = self.make_client(admin=True)
        provider = requests.Response()
        provider.status_code = 400
        provider._content = json.dumps({'error': {'code': 'context_length_exceeded',
            'message': 'PRIVATE_PROVIDER_CONTENT sk-synthetic-private-key'}}).encode()
        private_context = {'auftraege': [{'beschreibung': 'PRIVATE_CUSTOMER_CONTENT ' * 3000}], 'next_offset': None}
        for transport in ('server', 'browser'):
            for client, model, label in ((self.client, 'gpt-realtime', None),
                                          (admin, 'gpt-realtime', 'gpt-realtime'),
                                          (admin, 'sk-synthetic-private-key', 'anderes Modell'),
                                          (admin, 'gpt-' + 'x' * 81, 'anderes Modell')):
                with self.subTest(transport=transport, label=label), \
                     patch.object(p, 'get_openai_api_key', return_value='sk-synthetic-private-key'), \
                     patch.dict(os.environ, {'ASSISTANT_REALTIME_MODEL': model}), \
                     patch.object(p.cockpit_data, 'orders', return_value=private_context), \
                     patch('werkstatt_assistent.requests.post', return_value=provider) as call:
                    result = self.post('/realtime/start', {'transport': transport, 'sdp': 'v=0\r\nsynthetic-offer', 'actor': 'admin'}, client)
                self.assertEqual(result.status_code, 400)
                message = result.json['error']
                config = (call.call_args.kwargs['json']['session'] if transport == 'browser'
                          else json.loads(call.call_args.kwargs['files']['session'][1]))
                self.assertLess(len(config['instructions']), 24000)
                if label:
                    self.assertIn(f"Diagnose: {len(config['instructions'])} Anweisungszeichen, {len(config['tools'])} Werkzeuge, Modell {label}.", message)
                else:
                    self.assertNotIn('Diagnose:', message)
                    self.assertNotIn('Anweisungszeichen', message)
                self.assertEqual(set(result.json), {'error'})
                self.assertEqual(result.headers['Cache-Control'], 'no-store')
                self.assertLess(len(message), 650)
                for private in ('PRIVATE_PROVIDER_CONTENT', 'PRIVATE_CUSTOMER_CONTENT', 'sk-synthetic-private-key'):
                    self.assertNotIn(private, result.text)
        provider.status_code = 503
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-key'), \
             patch('werkstatt_assistent.requests.post', return_value=provider):
            result = self.post('/realtime/start', {'transport': 'browser', 'sdp': 'v=0\r\nsynthetic-offer'}, admin)
        self.assertNotIn('Diagnose:', result.json['error'])

    def test_provider_transport_errors_are_distinct_and_redacted(self):
        import requests
        for error, expected in [(requests.Timeout, 'Zeitüberschreitung'),
                                (requests.ConnectTimeout, 'Zeitüberschreitung'),
                                (requests.ConnectionError, 'Verbindung zum KI-Dienst fehlgeschlagen'),
                                (requests.RequestException, 'Verbindung zum KI-Dienst fehlgeschlagen')]:
            with self.subTest(error=error.__name__), \
                 patch.object(p, 'get_openai_api_key', return_value='synthetic-private-key'), \
                 patch('werkstatt_assistent.requests.post', side_effect=error('PRIVATE_CUSTOMER synthetic-private-key')) as provider:
                result = self.post('/realtime/start', {'sdp': 'v=0\r\nsynthetic-offer'})
                self.assertEqual(result.status_code, 400)
                self.assertIn(expected, result.json['error'])
                self.assertNotIn('PRIVATE_CUSTOMER', result.text)
                self.assertNotIn('synthetic-private-key', result.text)
                self.assertEqual(result.headers['Cache-Control'], 'no-store')
                self.assertEqual(provider.call_count, 1)

    def test_provider_authorization_stays_server_side_and_multipart_boundary_is_automatic(self):
        from unittest.mock import Mock
        provider = Mock(text='v=0\r\nsynthetic-answer')
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-private-key'), \
             patch('werkstatt_assistent.requests.post', return_value=provider) as call:
            result = self.post('/realtime/start', {'sdp': 'v=0\r\nsynthetic-offer'})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(call.call_args.kwargs['headers'], {'Authorization': 'Bearer synthetic-private-key'})
        self.assertEqual(call.call_args.kwargs['timeout'], (10, 60))
        self.assertIn('files', call.call_args.kwargs)
        self.assertNotIn('synthetic-private-key', result.text + str(list(result.headers)))

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

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_readonly_followup_keeps_own_conversation_but_refreshes_facts(self):
        from unittest.mock import Mock
        with database() as db:
            for actor, role, text in [
                ('mitarbeiter:1','user','Welche Breiten vom grünen Klebeband wurden bestellt?'),
                ('mitarbeiter:1','assistant','Im Vorschlag stehen 30 mm und 50 mm.'),
                ('admin','assistant','OTHER-ACTOR-PRIVATE-CONTEXT'),
                ('mitarbeiter:1','system','STORED-SYSTEM-ROLE-MUST-NOT-BE-USED'),
            ]:
                db.execute('INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES(?,?,?,?)',(actor,role,text,p.now_str()))
        result=Mock();result.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'50 mm, verstanden.'}]}]}
        with patch.object(p,'get_openai_api_key',return_value='synthetic-key'), patch('werkstatt_assistent.requests.post',return_value=result) as provider:
            response=self.post('/dialog',{'text':'Die 50 mm bitte.'})
        self.assertEqual(response.status_code,200)
        sent=provider.call_args.kwargs['json']
        history=[item for item in sent['input'] if 'role' in item]
        self.assertEqual([item['content'] for item in history],[
            'Welche Breiten vom grünen Klebeband wurden bestellt?',
            'Im Vorschlag stehen 30 mm und 50 mm.', 'Die 50 mm bitte.'])
        self.assertIn('Frühere Antworten sind kein aktueller Aktennachweis',sent['instructions'])
        self.assertIn('kalender',sent['instructions'])
        self.assertFalse(sent['store'])
        self.assertEqual(provider.call_count,1)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_missing_article_rights_remove_model_tools_and_previous_price_answers(self):
        import json
        from unittest.mock import Mock
        with database() as db:
            db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
            db.execute("INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES('mitarbeiter:1','assistant','OLDER-ARTICLE-PRICE',?)",(p.now_str(),))
        result=Mock(text='v=0\r\nanswer');result.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'Auftragsauskunft ist möglich.'}]}]}
        with patch.object(p,'get_openai_api_key',return_value='synthetic-key'), patch('werkstatt_assistent.requests.post',return_value=result) as provider:
            self.assertEqual(self.post('/realtime/start',{'sdp':'v=0\r\noffer'}).status_code,200)
            realtime=json.loads(provider.call_args.kwargs['files']['session'][1])
            self.assertEqual(self.post('/dialog',{'text':'Was steht im Auftrag?'}).status_code,200)
            dialog=provider.call_args.kwargs['json']
        for request_data in (realtime,dialog):
            names={tool['name'] for tool in request_data['tools']}
            self.assertTrue({'auftrag_lesen','tagesplan'} <= names)
            self.assertFalse({'artikel_suchen','beleg_lesen','aktion_vorschlagen','kamera'} & names)
        self.assertNotIn('OLDER-ARTICLE-PRICE',json.dumps(dialog['input']))
        with patch.object(p.cockpit_data,'articles') as articles:
            self.assertEqual(self.post('/realtime/werkzeug',{'name':'artikel_suchen','arguments':{'suche':'Klebeband'}}).status_code,400)
            articles.assert_not_called()

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_berlin_calendar_and_relative_dates_cross_utc_midnight_and_dst(self):
        import json
        from datetime import datetime,timezone
        from unittest.mock import Mock
        frozen=datetime(2026,10,24,22,30,tzinfo=timezone.utc)
        class Clock(datetime):
            @classmethod
            def now(cls,tz=None):return frozen.astimezone(tz) if tz else frozen.replace(tzinfo=None)
        with patch('werkstatt_assistent.datetime',Clock):
            response=self.client.get('/werkstatt/assistent/realtime/kontext')
            text=response.json['instructions']
            context=json.loads(text.rsplit('\nAKTENSTAND ',1)[1].split(': ',1)[1])
            self.assertEqual(context['kalender'],{'zeitzone':'Europe/Berlin','heute':'2026-10-25','morgen':'2026-10-26','uebermorgen':'2026-10-27'})
            self.assertEqual(context['stand'],'2026-10-25T00:30:00+02:00')
            with patch.object(p.cockpit_data,'schedule',side_effect=lambda day:{'datum':day}) as schedule:
                for word, expected in [(None,'2026-10-25'),('heute','2026-10-25'),('morgen','2026-10-26'),('übermorgen','2026-10-27'),('2026-11-02','2026-11-02')]:
                    result=self.post('/realtime/werkzeug',{'name':'tagesplan','arguments':{'datum':word}})
                    self.assertEqual(result.status_code,200)
                    self.assertEqual(result.json['result']['datum'],expected)
                self.assertEqual(schedule.call_count,5)
            with patch.object(p.cockpit_data,'briefing',side_effect=lambda day:{'datum':day}):
                result=self.post('/realtime/werkzeug',{'name':'morgenueberblick','arguments':{'datum':'morgen'}})
            self.assertEqual(result.json['result']['datum'],'2026-10-26')

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_material_prefetch_is_ready_before_first_model_request(self):
        from unittest.mock import Mock
        result=Mock(); result.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'Welche belegte Breite?'}]}]}
        evidence={'varianten':[{'produkt_name':'SYNTHETIC BAND','groesse':'30 mm','packinhalt':None}], 'bestellbar':False}
        with patch.object(p.cockpit_data,'material_context',create=True,return_value=evidence) as materials, patch.object(p,'get_openai_api_key',return_value='synthetic-key'), patch('werkstatt_assistent.requests.post',return_value=result) as provider:
            response=self.post('/dialog',{'text':'Ich brauche Abklebeband'})
            self.assertEqual(response.status_code,200)
            materials.assert_called_once_with(query='Abklebeband',limit=8)
            self.assertEqual(provider.call_count,1)
            self.assertIn('SYNTHETIC BAND',provider.call_args.kwargs['json']['instructions'])
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0],0)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_natural_invoice_question_preloads_concatenated_tape_before_model(self):
        from unittest.mock import Mock
        product='Mipa593250500 MP TapeHydroGreen50mRolle x30mm'
        with database() as db:
            db.execute("INSERT INTO einkauf_artikel(lieferant,artikelnummer,produkt_name,ve,gebinde,erstellt_am,geaendert_am) VALUES('Top-Color GmbH','10000991',?,'Stück','',?,?)",(product,p.now_str(),p.now_str()))
        result=Mock();result.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'Ein belegter Artikel ist mit 30 mm hinterlegt; Packinhalt unbekannt.'}]}]}
        question='Nur Auskunft, keine Bestellung: Welche Abklebebänder haben wir bisher gekauft, und welche Breiten und Verpackungseinheiten sind belegt?'
        with patch.object(p,'get_openai_api_key',return_value='synthetic-key'), patch('werkstatt_assistent.requests.post',return_value=result) as provider:
            response=self.post('/dialog',{'text':question})
            self.assertEqual(response.status_code,200)
            self.assertEqual(provider.call_count,1)
            instructions=provider.call_args.kwargs['json']['instructions']
            self.assertIn(product,instructions)
            self.assertIn('30 mm',instructions)
            self.assertIn('"packinhalt": null',instructions)
            self.assertIn('Keine Treffer für einen Suchtext bedeuten nicht',instructions)
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0],0)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_green_tape_question_preloads_widths_and_pack_despite_unconfirmed_color(self):
        import json
        from unittest.mock import Mock
        from werkstatt_cockpit_api import _invoice_product
        records=[]
        for width, count in ((25,36),(30,32),(50,24)):
            row={'produkt_name':f'Test MP TapeHydroGreen50mRolle x{width}mm',
                 'artikelnummer':f'SYNTH-{width}', 'lieferant':'Top-Color GmbH', 've':'Stück',
                 'quelle':{'art':'einkauf','beleg_id':1,'seite':1,'position':width},
                 'package_evidence':{'value':str(count),'unit':'Stück','per_unit':'VE',
                     'basis':'explicit_description','text':f'{count} Stück/VE'}}
            records.append(_invoice_product(row,'Top-Color GmbH',1,True))
        with database() as db:
            db.execute("INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES('mitarbeiter:1','assistant',?,?)",
                       ('Frühere Antwort: Die Farbe ist unbekannt, deshalb kann ich keine Breiten nennen.',p.now_str()))
        reply=Mock(); reply.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'Passend dazu finde ich HydroGreen.'}]}]}
        question='Nur Auskunft: Welche Breiten und Verpackungseinheiten sind bei unserem grünen Abklebeband belegt?'
        with patch.object(p.cockpit_data,'_material_records',return_value=([],records,{'begrenzt':True})), \
             patch.object(p,'get_openai_api_key',return_value='synthetic-key'), \
             patch('werkstatt_assistent.requests.post',return_value=reply) as provider:
            response=self.post('/dialog',{'text':question})
            self.assertEqual(response.status_code,200)
            self.assertEqual(provider.call_count,1)
            instructions=provider.call_args.kwargs['json']['instructions']
        context=json.loads(instructions.rsplit('\nAKTENSTAND ',1)[1].split(': ',1)[1])
        material=context['materialwissen']
        self.assertEqual(material['suchstatus'],'treffer')
        self.assertEqual(len(material['varianten']),3)
        for variant in material['varianten']:
            width=int(variant['artikelnummer'].split('-')[1])
            self.assertIn(f'{width} mm',variant['groesse'])
            self.assertEqual(variant['packinhalt']['menge'],str({25:36,30:32,50:24}[width]))
            self.assertEqual(variant['packinhalt']['pro'],'VE')
            self.assertEqual(variant['packinhalt']['quelle']['seite'],1)
            self.assertEqual(variant['farbe'],'')
            self.assertEqual(variant['farbabgleich']['basis'],'produktname_alias')
            self.assertEqual(variant['farbabgleich']['namenshinweis'],'HydroGreen')
            self.assertFalse(variant['farbabgleich']['bestaetigt'])
            self.assertFalse(variant['bestellbar'])
            self.assertNotIn('Packinhalt',variant['fehlende_angaben'])
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0],0)

        # The complete fresh view can answer the exact same pure question
        # without a language-model round; old dialogue cannot alter its facts.
        with patch.object(p.cockpit_data,'_material_records',return_value=([],records,{})), \
             patch('werkstatt_assistent.requests.post',side_effect=AssertionError('No provider call')):
            direct=self.post('/dialog',{'text':question})
        self.assertEqual(direct.status_code,200)
        self.assertEqual(direct.json['events'],[])
        self.assertIn('25 mm mit 36, 30 mm mit 32 und 50 mm mit 24 Stück je Verkaufseinheit',direct.json['text'])
        self.assertIn('Farbzuordnung sind ungeprüft',direct.json['text'])
        with database() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_aktionen').fetchone()[0],0)
            self.assertEqual(db.execute("SELECT text FROM assistent_dialog WHERE actor='mitarbeiter:1' AND role='assistant' ORDER BY id DESC LIMIT 1").fetchone()[0],direct.json['text'])

        with database() as db:db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        with patch.object(p.cockpit_data,'material_context') as material, \
             patch.object(p,'get_openai_api_key',return_value='synthetic-key'), \
             patch('werkstatt_assistent.requests.post',return_value=reply) as provider:
            self.assertEqual(self.post('/dialog',{'text':question}).status_code,200)
            material.assert_not_called()
            self.assertEqual(provider.call_count,1)
            self.assertNotIn('36 Stück',provider.call_args.kwargs['json']['instructions'])

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_material_prefetch_respects_revoked_rights_and_outage(self):
        import json
        with database() as db: db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        with patch.object(p.cockpit_data,'material_context',create=True) as materials:
            response=self.client.get('/werkstatt/assistent/realtime/kontext')
            self.assertEqual(response.status_code,200)
            materials.assert_not_called()
            self.assertNotIn('"materialwissen":',response.json['instructions'])
        with database() as db: db.execute('UPDATE assistent_rechte SET einkaufen=1 WHERE mitarbeiter_id=1')
        with patch.object(p.cockpit_data,'material_context',create=True,side_effect=ValueError('private technical data')):
            response=self.client.get('/werkstatt/assistent/realtime/kontext')
            self.assertEqual(response.status_code,200)
            context=json.loads(response.json['instructions'].rsplit('\nAKTENSTAND ',1)[1].split(': ',1)[1])
            self.assertFalse(context['materialwissen']['verfuegbar'])
            self.assertNotIn('private technical data',response.text)
            self.assertIn('Testfahrzeug',response.json['instructions'])

    def test_material_followups_reuse_product_without_stale_variant_constraints(self):
        from werkstatt_assistent import material_query
        history=[{'role':'assistant','text':'Fremde Antwort kein Produktbezug'},
                 {'role':'user','text':'Bitte grünes Abklebeband 30 mm bestellen'}]
        self.assertEqual(material_query('Einen Karton davon bitte',history),'Abklebeband')
        self.assertEqual(material_query('Was bestellen wir davon?',history),'Abklebeband')
        self.assertEqual(material_query('Doch 50 mm bitte',history),'Abklebeband 50 mm')
        self.assertEqual(material_query('Dann blau bitte',history),'Abklebeband blau')
        self.assertEqual(material_query('Davon 2 Rollen',history),'Abklebeband')
        self.assertEqual(material_query('50',history),'Abklebeband 50')
        self.assertEqual(material_query('Handschuhe bitte',history),'Handschuhe')
        self.assertIsNone(material_query('Was muss ich an Auftrag 156 machen?',history))
        question='Nur Auskunft, keine Bestellung: Welche Abklebebänder haben wir bisher gekauft, und welche Breiten und Verpackungseinheiten sind belegt?'
        self.assertEqual(material_query(question),'Abklebebänder')
        self.assertEqual(material_query('Welche grünen Abklebebänder in 30 mm haben wir gekauft?'),'Abklebebänder grünen 30 mm')
        self.assertEqual(material_query('Ich brauche Klebeband für Auftrag 156'),'Klebeband')

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_job_text_skips_material_preload_but_realtime_start_retains_it(self):
        from unittest.mock import Mock
        result=Mock();result.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'Laut Cockpit: Testarbeit.'}]}]}
        with patch.object(p.cockpit_data,'material_context',return_value={'varianten':[]}) as materials, patch.object(p,'get_openai_api_key',return_value='synthetic-key'), patch('werkstatt_assistent.requests.post',return_value=result) as provider:
            response=self.post('/dialog',{'text':'Was muss ich an Auftrag 156 machen?'})
            self.assertEqual(response.status_code,200)
            materials.assert_not_called()
            self.assertNotIn('"materialwissen":',provider.call_args.kwargs['json']['instructions'])
            self.assertEqual(provider.call_count,1)
            response=self.client.get('/werkstatt/assistent/realtime/kontext')
            self.assertEqual(response.status_code,200)
            materials.assert_called_once_with(query='',limit=12)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_followup_width_prefetch_is_resolved_before_first_model_round(self):
        from unittest.mock import Mock
        with database() as db:
            db.execute("INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES('mitarbeiter:1','user','Ich brauche Abklebeband grün 30 mm',?)",(p.now_str(),))
        result=Mock();result.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'Die 50-mm-Variante ist als Vorschlag hinterlegt.'}]}]}
        with patch.object(p.cockpit_data,'material_context',return_value={'varianten':[]}) as materials, patch.object(p,'get_openai_api_key',return_value='synthetic-key'), patch('werkstatt_assistent.requests.post',return_value=result) as provider:
            response=self.post('/dialog',{'text':'Doch 50 mm bitte'})
            self.assertEqual(response.status_code,200)
            materials.assert_called_once_with(query='Abklebeband 50 mm',limit=8)
            self.assertEqual(provider.call_count,1)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_preload_marks_partial_list_and_explains_status_and_deadlines(self):
        import json
        with patch.object(p.cockpit_data,'orders',return_value={'auftraege':[{'id':156,'status':3}],'next_offset':60}):
            text=self.client.get('/werkstatt/assistent/realtime/kontext').json['instructions']
        context=json.loads(text.rsplit('\nAKTENSTAND ',1)[1].split(': ',1)[1])
        self.assertTrue(context['gekuerzt'])
        self.assertEqual(context['next_offset'],60)
        self.assertIn('Erst nach erfolgloser Abfrage nicht gefunden sagen',text)
        self.assertIn('geplante Fertigfrist, keine bestätigte Fertigstellung',text)
        self.assertIn('4 fertig, 5 zurückgegeben',text)
        self.assertNotIn('hinterlegten Arbeiten und Freigabe',text)
        result=self.post('/realtime/werkzeug',{'name':'auftrag_lesen','arguments':{'auftrag_id':157}})
        self.assertEqual(result.json['result']['id'],157)

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_read_tool_bad_types_return_400_without_reading_other_sources(self):
        cases=[
            ([],{}), (None,{}), ('auftrag_lesen',{'auftrag_id':True}),
            ('auftrag_lesen',{'auftrag_id':[156]}), ('auftrag_lesen',{'auftrag_id':156.5}),
            ('dokument_lesen',{'dokument_id':True}), ('beleg_lesen',{'beleg_id':{}}),
            ('auftraege_suchen',{'suche':[]}), ('auftraege_suchen',{'suche':'Test','offset':True}),
            ('tagesplan',{'datum':[]}), ('tagesplan',{'datum':'2026-02-30'}),
            ('artikel_suchen',{'suche':['Test']}), ('lackierplan',{'zeitraum':['heute']}),
        ]
        with patch.object(p.cockpit_data,'order') as order, patch.object(p.cockpit_data,'document') as document, patch.object(p.cockpit_data,'invoice') as invoice:
            for name,args in cases:
                with self.subTest(name=name,args=args):
                    response=self.post('/realtime/werkzeug',{'name':name,'arguments':args})
                    self.assertEqual(response.status_code,400)
            order.assert_not_called();document.assert_not_called();invoice.assert_not_called()

    @patch.dict(p.app.config, ASSISTANT_READ_ONLY=True)
    def test_document_tool_rejects_archived_parent_before_reading_content(self):
        with database() as db:
            db.execute("INSERT INTO dateien(id,auftrag_id,original_name,stored_name,hochgeladen_am) VALUES(801,156,'synthetic-job.pdf','synthetic-unused.pdf',?)",(p.now_str(),))
        with patch.object(p.cockpit_data,'document',return_value={'id':801,'auftrag_id':156,'extrahierter_text':'SYNTHETIC WORK'}) as document:
            result=self.post('/realtime/werkzeug',{'name':'dokument_lesen','arguments':{'dokument_id':801}})
            self.assertEqual(result.status_code,200)
            document.assert_called_once_with(801)
            document.reset_mock()
            with database() as db:db.execute('UPDATE auftraege SET archiviert=1 WHERE id=156')
            result=self.post('/realtime/werkzeug',{'name':'dokument_lesen','arguments':{'dokument_id':801}})
            self.assertEqual(result.status_code,400)
            self.assertNotIn('SYNTHETIC WORK',result.text)
            document.assert_not_called()

if __name__=='__main__': unittest.main(verbosity=2)
