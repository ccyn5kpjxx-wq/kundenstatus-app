"""Personal avatar HTTP integration, synthetic import capsule, no live HR/network.

The existing fixture is composed, never inherited: unrelated 52 tests are not
rerun. All employee/calendar/time data below are synthetic and isolated.
"""
from datetime import date, datetime, timezone
from pathlib import Path
import json
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_assistent as fixture

p, database = fixture.p, fixture.database
PERSON = {'actor': 'mitarbeiter:1', 'mitarbeiter_id': 1, 'lesen': 1}
ADMIN = {'actor': 'admin'}


class PersonalHTTPTests(unittest.TestCase):
    def setUp(self):
        self.legacy = fixture.AssistantTests(methodName='runTest')
        self.legacy.setUp(); self.addCleanup(self.legacy.tearDown)
        self.client = self.legacy.client
        self.enterContext(patch.dict(p.app.config, ASSISTANT_READ_ONLY=True, ASSISTANT_NATIVE_COCKPIT=True))
        self.enterContext(patch('requests.sessions.Session.request', side_effect=AssertionError('No network')))
        self.enterContext(patch('smtplib.SMTP', side_effect=AssertionError('No SMTP')))
        self.enterContext(patch('smtplib.SMTP_SSL', side_effect=AssertionError('No SMTP')))
        self.enterContext(patch.object(p, 'schedule_change_backup', return_value=None))
        self.enterContext(patch('werkstatt_mitarbeiter_selfservice._today', return_value=date(2026,9,29)))
        self.enterContext(patch.object(p.assistant_time, 'now', return_value=datetime(2026,9,29,7,tzinfo=timezone.utc)))
        with database() as db:
            for table in ('mitarbeiter_urlaub_audit','mitarbeiter_urlaubsantraege','mitarbeiter_urlaubskonten',
                          'mitarbeiter_urlaub','mitarbeiter_zeitstempel','mitarbeiter_zeitstatus'):
                db.execute('DELETE FROM ' + table)
            db.execute("DELETE FROM app_settings WHERE key='ASSISTANT_OPERATIONS_ENABLED'")
            db.execute('UPDATE assistent_rechte SET dokumentieren=0,einkaufen=0,limit_cent=0 WHERE mitarbeiter_id=1')
            db.execute("INSERT INTO mitarbeiter(id,name,aktiv,erstellt_am,geaendert_am) VALUES(2,'Other synthetic employee',1,?,?)", (p.now_str(),p.now_str()))
            db.execute("INSERT INTO assistent_rechte(mitarbeiter_id,passwort_hash,lesen,dokumentieren,einkaufen,limit_cent) VALUES(2,'not-for-login',1,0,0,0)")
        self.other = p.app.test_client()
        with self.other.session_transaction() as s:
            s.update(csrf_token='test-csrf',assistent_mid=2,assistent_version=1)
        self.admin = self.legacy.make_client(admin=True)

    def post(self, path, data=None, client=None):
        return self.legacy.post(path, data, client)

    def tool(self, name, args=None, client=None):
        return self.post('/realtime/werkzeug', {'name':name,'arguments':args or {}}, client)

    def rows(self, sql, args=()):
        with database() as db:
            return [dict(row) for row in db.execute(sql,args).fetchall()]

    def count(self, table):
        return self.rows('SELECT COUNT(*) AS n FROM '+table)[0]['n']

    def leave(self, client=None, **changes):
        data = {'art':'urlaub','start_datum':'2026-10-05','end_datum':'2026-10-09'}
        data.update(changes)
        return self.post('/vorschlag', data, client)

    def challenge(self, action, client=None):
        result = self.post('/vorlesen/'+action['id'], client=client)
        self.assertEqual(result.status_code, 200, result.text)
        return result.json

    def spoken(self, challenge, client=None, **changes):
        data = {'nonce':challenge['nonce'],'text':challenge['phrase']}
        data.update(changes)
        return self.post('/sprache-bestaetigen', data, client)

    def test_personal_tools_read_only_own_data_without_purchase_rights(self):
        other_who={'actor':'mitarbeiter:2','mitarbeiter_id':2,'lesen':1}
        p.assistant_selfservice.apply(other_who,'2026-11-02','2026-11-03','other-only-request-001')
        own=self.tool('mein_urlaub_lesen',{'jahr':2026})
        self.assertEqual(own.status_code,200,own.text)
        self.assertIsNone(own.json['result']['resttage'])
        self.assertEqual(own.json['result']['antraege'],[])
        self.assertNotIn('Other synthetic',own.text)
        own_time=self.tool('meine_arbeitszeit_lesen',{'monat':'2026-09'})
        self.assertEqual(own_time.status_code,200,own_time.text)
        self.assertEqual(own_time.json['result']['mitarbeiter']['id'],1)
        for name,args in [('mein_urlaub_lesen',{'jahr':2026,'mitarbeiter_id':2}),
                          ('meine_arbeitszeit_lesen',{'monat':'2026-09','actor':'mitarbeiter:2'}),
                          ('urlaub_beantragen_vorschlagen',{'start_datum':'2026-10-05','end_datum':'2026-10-09','mitarbeiter_id':2}),
                          ('arbeitszeit_vorschlagen',{'aktion':'kommen','zeit':'2026-09-01T08:00:00Z'})]:
            self.assertEqual(self.tool(name,args).status_code,400)
        self.assertEqual(self.count('assistent_aktionen'),0)

    def test_leave_requires_separate_actor_bound_confirmation_and_never_auto_approves(self):
        response=self.tool('urlaub_beantragen_vorschlagen',{'start_datum':'2026-10-05','end_datum':'2026-10-09'})
        self.assertEqual(response.status_code,200,response.text)
        action=response.json['event']['data']
        self.assertEqual(action['art'],'urlaub'); self.assertEqual(action['status'],'vorschlag')
        self.assertEqual(self.count('mitarbeiter_urlaubsantraege'),0)
        challenge=self.challenge(action)
        self.assertEqual(challenge['phrase'],'Urlaub beantragen')
        self.assertIn('2026-10-05',challenge['text']); self.assertIn('2026-10-09',challenge['text'])
        self.assertEqual(self.count('mitarbeiter_urlaubsantraege'),0)
        self.assertEqual(self.spoken(challenge,text='Ja').status_code,400)
        self.assertEqual(self.spoken(challenge,nonce='wrong').status_code,400)
        self.assertIn(self.post('/bestaetigen/'+action['id'],client=self.other).status_code,(403,404))
        # Even a copied challenge in a different employee's session is not theirs.
        with self.client.session_transaction() as state: saved=dict(state['assistent_bestaetigung'])
        with self.other.session_transaction() as state: state['assistent_bestaetigung']=saved
        self.assertIn(self.spoken(challenge,client=self.other).status_code,(400,403))
        good=self.spoken(challenge)
        self.assertEqual(good.status_code,200,good.text)
        self.assertIn('Noch nicht genehmigt',good.json['hinweis'])
        rows=self.rows('SELECT mitarbeiter_id,status FROM mitarbeiter_urlaubsantraege')
        self.assertEqual(rows,[{'mitarbeiter_id':1,'status':'beantragt'}])
        self.assertEqual(self.count('mitarbeiter_urlaub'),0)
        self.assertEqual(self.count('mitarbeiter_urlaubskonten'),0)
        again=self.post('/bestaetigen/'+action['id'])
        self.assertEqual(again.status_code,200,again.text)
        self.assertEqual(self.count('mitarbeiter_urlaubsantraege'),1)

    def test_expired_challenge_and_csrf_cannot_submit(self):
        action=self.leave().json
        self.assertEqual(self.client.post('/werkstatt/assistent/bestaetigen/'+action['id'],json={}).status_code,400)
        challenge=self.challenge(action)
        with self.client.session_transaction() as state:
            value=dict(state['assistent_bestaetigung']);value['expires']=0;state['assistent_bestaetigung']=value
        self.assertEqual(self.spoken(challenge).status_code,400)
        self.assertEqual(self.count('mitarbeiter_urlaubsantraege'),0)

    def test_hr_allowed_with_operations_off_does_not_unlock_order_writes(self):
        self.assertTrue(p.app.config['ASSISTANT_READ_ONLY'])
        self.assertEqual(p.get_app_setting('ASSISTANT_OPERATIONS_ENABLED',''),'')
        self.assertEqual(self.leave().status_code,200)
        for payload in ({'art':'status','auftrag_id':156,'aktion':'fertig_melden'},
                        {'art':'bestellung','auftrag_id':0},
                        {'art':'notiz','auftrag_id':156,'text':'must-not-write'},
                        {'art':'kontakt','auftrag_id':156,'kontakt_telefon':'000'}):
            self.assertEqual(self.post('/vorschlag',payload).status_code,403)
        self.assertEqual(self.post('/foto/156').status_code,403)
        self.assertEqual(self.rows('SELECT beschreibung FROM auftraege WHERE id=156')[0]['beschreibung'],'Keine Freigabe')
        self.assertEqual({r['art'] for r in self.rows('SELECT art FROM assistent_aktionen')},{'urlaub'})

    def test_clock_only_after_confirm_replay_no_duplicate_and_server_time(self):
        response=self.tool('arbeitszeit_vorschlagen',{'aktion':'kommen'})
        self.assertEqual(response.status_code,200,response.text)
        action=response.json['event']['data']
        self.assertEqual(self.count('mitarbeiter_zeitstempel'),0)
        self.assertEqual(self.count('mitarbeiter_zeitstatus'),0)
        challenge=self.challenge(action)
        self.assertEqual(challenge['phrase'],'Zeitstempel bestätigen')
        result=self.spoken(challenge)
        self.assertEqual(result.status_code,200,result.text)
        self.assertEqual(self.rows('SELECT mitarbeiter_id,aktion,zeit FROM mitarbeiter_zeitstempel'),
                         [{'mitarbeiter_id':1,'aktion':'kommen','zeit':'2026-09-29T07:00:00+00:00'}])
        self.assertEqual(self.post('/bestaetigen/'+action['id']).status_code,200)
        self.assertEqual(self.count('mitarbeiter_zeitstempel'),1)
        self.assertEqual(self.tool('meine_arbeitszeit_lesen',{'monat':'2026-09'}).json['result']['status']['zustand'],'arbeitet')
        self.assertEqual(self.tool('meine_arbeitszeit_lesen',{'monat':'2026-09'},self.other).json['result']['status']['zustand'],'abwesend')

    def test_admin_has_boss_links_and_no_personal_hr_actor(self):
        own=self.client.get('/werkstatt/assistent')
        self.assertEqual(own.status_code,200,own.text)
        for link in ('/werkstatt/assistent/urlaub','/werkstatt/assistent/arbeitszeit'):
            self.assertIn('href="'+link+'"',own.text)
        self.assertNotIn('href="/admin/arbeitszeit"',own.text)
        self.assertNotIn('href="/werkstatt/assistent/urlaub/verwaltung"',own.text)
        admin=self.admin.get('/werkstatt/assistent')
        self.assertEqual(admin.status_code,200)
        self.assertIn('href="/admin/arbeitszeit"',admin.text)
        self.assertIn('href="/werkstatt/assistent/urlaub/verwaltung"',admin.text)
        self.assertNotIn('data-personal-enabled="true"',admin.text)
        for name in ('mein_urlaub_lesen','meine_arbeitszeit_lesen'):
            self.assertEqual(self.tool(name,client=self.admin).status_code,400)
        self.assertEqual(self.leave(client=self.admin).status_code,403)
        self.assertNotEqual(self.client.get('/admin/arbeitszeit').status_code,200)
        self.assertNotEqual(self.client.get('/werkstatt/assistent/urlaub/verwaltung').status_code,200)

    def test_revoked_rights_version_or_activity_prevents_hr_confirm(self):
        action=self.leave().json
        for sql,expected in [('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1',403),
                             ('UPDATE assistent_rechte SET lesen=1,version=2 WHERE mitarbeiter_id=1',401),
                             ('UPDATE assistent_rechte SET version=1 WHERE mitarbeiter_id=1',None),
                             ('UPDATE mitarbeiter SET aktiv=0 WHERE id=1',401)]:
            with database() as db: db.execute(sql)
            if expected:
                self.assertEqual(self.post('/bestaetigen/'+action['id']).status_code,expected)
                self.assertEqual(self.tool('mein_urlaub_lesen').status_code,expected)
        self.assertEqual(self.count('mitarbeiter_urlaubsantraege'),0)

    def test_dialog_tool_contract_and_greeting_do_not_preload_or_stamp_hr(self):
        provider=Mock();provider.json.return_value={'output':[{'type':'message','content':[{'type':'output_text','text':'Guten Morgen.'}]}]}
        with patch.object(p,'get_openai_api_key',return_value='synthetic'), \
             patch('werkstatt_assistent.requests.post',return_value=provider) as call, \
             patch.object(p.assistant_selfservice,'summary',side_effect=AssertionError('No HR preload')), \
             patch.object(p.assistant_time,'summary',side_effect=AssertionError('No HR preload')):
            response=self.post('/dialog',{'text':'Guten Morgen'})
        self.assertEqual(response.status_code,200,response.text)
        tools={t['name']:t for t in call.call_args.kwargs['json']['tools']}
        for name in ('mein_urlaub_lesen','meine_arbeitszeit_lesen','urlaub_beantragen_vorschlagen','arbeitszeit_vorschlagen'):
            self.assertIn(name,tools)
            self.assertFalse(tools[name]['parameters']['additionalProperties'])
            self.assertNotIn('mitarbeiter_id',tools[name]['parameters']['properties'])
        self.assertNotIn('status_vorschlagen',tools)
        self.assertEqual(self.count('mitarbeiter_urlaubsantraege'),0)
        self.assertEqual(self.count('mitarbeiter_zeitstempel'),0)

    def test_new_leave_request_after_withdrawal_or_rejection_has_new_action(self):
        for decision in ('zurueckgezogen','abgelehnt'):
            with self.subTest(decision=decision):
                first=self.leave().json
                confirmed=self.post('/bestaetigen/'+first['id'])
                self.assertEqual(confirmed.status_code,200,confirmed.text)
                row=self.rows("SELECT * FROM mitarbeiter_urlaubsantraege WHERE status='beantragt'")[0]
                if decision=='zurueckgezogen':
                    p.assistant_selfservice.withdraw(PERSON,row['id'],row['version'])
                else:
                    p.assistant_selfservice.review(ADMIN,row['id'],'abgelehnt',row['version'])
                count_before=self.count('mitarbeiter_urlaubsantraege')
                replay=self.post('/bestaetigen/'+first['id'])
                self.assertEqual(replay.status_code,200,replay.text)
                self.assertEqual(replay.json['status'],decision)
                self.assertIn('Es wurde kein neuer Antrag eingereicht.',replay.json['hinweis'])
                self.assertEqual(self.count('mitarbeiter_urlaubsantraege'),count_before)
                self.assertEqual(self.rows("SELECT id FROM mitarbeiter_urlaubsantraege WHERE status='beantragt'"),[])
                second=self.leave()
                self.assertEqual(second.status_code,200,second.text)
                self.assertNotEqual(second.json['id'],first['id'])
                self.assertEqual(second.json['status'],'vorschlag')
                self.assertEqual(self.leave().json['id'],second.json['id'],'Repeat preparation does not multiply open proposals')
                submitted=self.spoken(self.challenge(second.json))
                self.assertEqual(submitted.status_code,200,submitted.text)
                newest=self.rows("SELECT * FROM mitarbeiter_urlaubsantraege WHERE status='beantragt'")[0]
                self.assertNotEqual(newest['id'],row['id'])
                p.assistant_selfservice.withdraw(PERSON,newest['id'],newest['version'])
        self.assertEqual(self.count('mitarbeiter_urlaub'),0)


if __name__ == '__main__':
    unittest.main()
