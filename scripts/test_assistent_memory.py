"""Isolated personal-memory integration tests; synthetic employees and no network.

Compose the established fixture without inheriting its unrelated test cases.
The assertions cover actor boundaries, forgetting races and prompt limits.
"""
import concurrent.futures
import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_assistent as fixture

p, database = fixture.p, fixture.database
BASE = '/werkstatt/assistent'
PERSON = {'actor': 'mitarbeiter:1', 'mitarbeiter_id': 1, 'lesen': 1,
          'einkaufen': 1, 'dokumentieren': 1}


class MemoryHTTPTests(unittest.TestCase):
    def setUp(self):
        self.legacy = fixture.AssistantTests(methodName='runTest')
        self.legacy.setUp()
        self.addCleanup(self.legacy.tearDown)
        self.client = self.legacy.client
        self.enterContext(patch.dict(p.app.config, ASSISTANT_READ_ONLY=True,
                                     ASSISTANT_NATIVE_COCKPIT=True))
        self.enterContext(patch('requests.sessions.Session.request',
                                side_effect=AssertionError('Network forbidden')))
        self.enterContext(patch('smtplib.SMTP', side_effect=AssertionError('SMTP forbidden')))
        self.enterContext(patch('smtplib.SMTP_SSL', side_effect=AssertionError('SMTP forbidden')))
        self.enterContext(patch.object(p, 'schedule_change_backup', return_value=None))
        with database() as db:
            db.execute("DELETE FROM app_settings WHERE key='ASSISTANT_OPERATIONS_ENABLED'")
            db.execute("INSERT INTO mitarbeiter(id,name,aktiv,erstellt_am,geaendert_am) VALUES(2,'Other synthetic employee',1,?,?)",
                       (p.now_str(), p.now_str()))
            db.execute("INSERT INTO assistent_rechte(mitarbeiter_id,passwort_hash,lesen,dokumentieren,einkaufen,limit_cent) VALUES(2,'not-for-login',1,1,1,10000)")
        self.other = p.app.test_client()
        with self.other.session_transaction() as state:
            state.update(csrf_token='test-csrf', assistent_mid=2, assistent_version=1)
        self.admin = self.legacy.make_client(admin=True)
        self.serial = 0
        # Preserve shared test fixture semantics while clearing new tables too.
        for client in (self.client, self.other, self.admin):
            response = self.delete('/gedaechtnis', {'generation': self.state(client)['generation']}, client)
            self.assertEqual(response.status_code, 200, response.text)

    def post(self, path, data=None, client=None):
        return self.legacy.post(path, data, client)

    def delete(self, path, data=None, client=None):
        return (client or self.client).delete(BASE + path, json=data or {},
                                               headers={'X-CSRF-Token': 'test-csrf'})

    def state(self, client=None, query=''):
        response = (client or self.client).get(BASE + '/gedaechtnis' + query)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn('no-store', response.headers.get('Cache-Control', ''))
        return response.json

    def voice(self, text, role='user', client=None, **changes):
        self.serial += 1
        data = {'generation': self.state(client)['generation'],
                'event_id': f'synthetic-memory-event-{self.serial:06d}',
                'role': role, 'text': text}
        data.update(changes)
        return self.post('/gedaechtnis/gespraech', data, client)

    def note(self, text, client=None, **changes):
        data = {'generation': self.state(client)['generation'], 'text': text}
        data.update(changes)
        return self.post('/gedaechtnis/notiz', data, client)

    def tool(self, name, arguments=None, client=None):
        return self.post('/realtime/werkzeug', {'name': name, 'arguments': arguments or {}}, client)

    def rows(self, sql, args=()):
        with database() as db:
            return [dict(row) for row in db.execute(sql, args).fetchall()]

    def answer(self, text='Synthetische Antwort.'):
        response = Mock()
        response.json.return_value = {'output': [{'type': 'message', 'content': [
            {'type': 'output_text', 'text': text}]}]}
        return response

    def test_read_only_employee_can_manage_own_memory_without_business_writes(self):
        with database() as db:
            db.execute('UPDATE assistent_rechte SET dokumentieren=0,einkaufen=0,limit_cent=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.note('Ich bevorzuge kurze Antworten.').status_code, 200)
        generation = self.state()['generation']
        self.assertEqual(self.voice('Morgen möchte ich die Vorbereitung besprechen.').status_code, 200)
        current = self.state()
        self.assertEqual(current['generation'], generation, 'Appending must not invalidate the live conversation')
        self.assertEqual(current['entries'][0]['source'], 'voice')
        self.assertEqual(self.rows('SELECT id FROM assistent_aktionen'), [])
        self.assertEqual(self.rows('SELECT status FROM auftraege WHERE id=156'), [{'status': 1}])

    def test_actor_isolation_in_ui_search_prompts_and_foreign_ids(self):
        own = 'EIGENES_MERKWORT Lackierplanung morgen'
        foreign = 'FREMDES_MERKWORT Persönliche Notiz anderer Mitarbeiter'
        self.assertEqual(self.note(own).status_code, 200)
        self.assertEqual(self.note(foreign, self.other).status_code, 200)
        self.assertEqual(self.voice('FREMDER_DIALOG', client=self.other).status_code, 200)
        foreign_state = self.state(self.other)
        for query in ('', '?actor=mitarbeiter:2', '?mitarbeiter_id=2', '?suche=FREMDES_MERKWORT'):
            response = self.client.get(BASE + '/gedaechtnis' + query)
            self.assertIn(response.status_code, (200, 400))
            self.assertNotIn(foreign, response.text)
            self.assertNotIn('FREMDER_DIALOG', response.text)
        self.assertNotIn(own, json.dumps(self.state(self.other), ensure_ascii=False))
        self.assertNotIn(own, json.dumps(self.state(self.admin), ensure_ascii=False))
        generation = self.state()['generation']
        for kind, row in [('notiz', foreign_state['notes'][0]), ('eintrag', foreign_state['entries'][0])]:
            response = self.delete('/gedaechtnis/' + kind + '/' + str(row['id']), {'generation': generation})
            self.assertIn(response.status_code, (400, 404))
        response = self.note('Foreign replacement', id=foreign_state['notes'][0]['id'])
        self.assertIn(response.status_code, (400, 404))
        self.assertEqual(self.state(self.other), foreign_state)
        for route in ('/realtime/kontext',):
            response = self.client.get(BASE + route)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertIn(own, response.text)
            self.assertNotIn(foreign, response.text)

    def test_auth_csrf_and_current_revocation_apply_to_every_memory_route(self):
        self.assertEqual(self.note('Berechtigte Notiz').status_code, 200)
        self.assertEqual(self.voice('Berechtigtes Gespräch').status_code, 200)
        state = self.state()
        operations = [
            ('get', '/gedaechtnis', None),
            ('post', '/gedaechtnis/gespraech', {'generation': state['generation'], 'event_id': 'auth-test-event', 'role': 'user', 'text': 'test'}),
            ('post', '/gedaechtnis/notiz', {'generation': state['generation'], 'text': 'test'}),
            ('delete', '/gedaechtnis/notiz/' + str(state['notes'][0]['id']), {'generation': state['generation']}),
            ('delete', '/gedaechtnis/eintrag/' + str(state['entries'][0]['id']), {'generation': state['generation']}),
            ('delete', '/gedaechtnis', {'generation': state['generation']}),
        ]
        for method, path, body in operations:
            anon = p.app.test_client()
            with anon.session_transaction() as session:
                session['csrf_token'] = 'test-csrf'
            self.assertEqual(getattr(anon, method)(BASE + path, json=body,
                              headers={'X-CSRF-Token': 'test-csrf'}).status_code, 401)
            if method != 'get':
                self.assertEqual(getattr(self.client, method)(BASE + path, json=body).status_code, 400)
        for mutation, expected in [('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1', 403),
                                   ('UPDATE assistent_rechte SET lesen=1,version=2 WHERE mitarbeiter_id=1', 401),
                                   ('UPDATE assistent_rechte SET version=1 WHERE mitarbeiter_id=1', 200),
                                   ('UPDATE mitarbeiter SET aktiv=0 WHERE id=1', 401)]:
            with database() as db:
                db.execute(mutation)
            if expected == 200:
                continue
            for method, path, body in operations:
                response = getattr(self.client, method)(BASE + path, json=body,
                                                       headers={'X-CSRF-Token': 'test-csrf'})
                self.assertEqual(response.status_code, expected, (method, path, response.text))
            self.assertEqual(self.tool('gedaechtnis_suchen', {'suche': 'Berechtigte'}).status_code, expected)

    def test_generation_rotation_blocks_stale_edits_and_delayed_transcripts(self):
        old = self.state()['generation']
        self.assertEqual(self.note('Erste Fassung').status_code, 200)
        current = self.state()
        self.assertNotEqual(old, current['generation'])
        note_id = current['notes'][0]['id']
        self.assertEqual(self.note('Zweite Fassung', id=note_id).status_code, 200)
        after_edit = self.state()
        self.assertNotEqual(after_edit['generation'], current['generation'])
        stale = current['generation']
        self.assertEqual(self.voice('Verspätete alte Stimme', generation=stale).status_code, 409)
        self.assertEqual(self.note('Alte Fassung', generation=stale, id=note_id).status_code, 409)
        self.assertEqual(self.delete('/gedaechtnis/notiz/' + str(note_id), {'generation': stale}).status_code, 409)
        self.assertEqual(self.delete('/gedaechtnis', {'generation': stale}).status_code, 409)
        self.assertEqual(self.state(), after_edit)
        self.assertEqual(self.delete('/gedaechtnis', {'generation': after_edit['generation']}).status_code, 200)
        self.assertEqual(self.voice('Nicht wiederbeleben', generation=after_edit['generation']).status_code, 409)
        cleared = self.state()
        self.assertEqual(cleared['entries'], [])
        self.assertEqual(cleared['notes'], [])

    def test_duplicate_voice_events_are_idempotent_and_scoped_per_actor(self):
        self.assertEqual(self.voice('Einmal speichern', event_id='same-event-unique').status_code, 200)
        self.assertEqual(self.voice('Einmal speichern', event_id='same-event-unique').status_code, 200)
        self.assertEqual(len(self.state()['entries']), 1)
        self.assertEqual(self.voice('Andere Person', event_id='same-event-unique', client=self.other).status_code, 200)
        self.assertEqual(len(self.state(self.other)['entries']), 1)
        # Conflicting retries cannot overwrite an already persisted event.
        response = self.voice('Anderer Inhalt', event_id='same-event-unique')
        self.assertIn(response.status_code, (200, 400, 409))
        self.assertEqual(self.state()['entries'][0]['text'], 'Einmal speichern')

    def test_legacy_text_history_survives_migration_and_search_paginates_without_leaking(self):
        with database() as db:
            for number in range(55):
                db.execute('INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES(?,?,?,?)',
                           ('mitarbeiter:1', 'user', f'Altgespräch {number:03d} SUCHWORT', p.now_str()))
            db.execute('INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES(?,?,?,?)',
                       ('mitarbeiter:2', 'user', 'SUCHWORT FREMD', p.now_str()))
        first = self.state(query='?suche=SUCHWORT')
        self.assertTrue(first['next_before_id'])
        ids = [row['id'] for row in first['entries']]
        second = self.state(query='?suche=SUCHWORT&before_id=' + str(first['next_before_id']))
        self.assertTrue(second['entries'])
        self.assertTrue(set(ids).isdisjoint(row['id'] for row in second['entries']))
        self.assertNotIn('FREMD', json.dumps(first) + json.dumps(second))
        self.assertTrue(all(row['source'] == 'text' for row in first['entries']))

    def test_text_response_arriving_after_clear_does_not_restore_forgotten_context(self):
        self.assertEqual(self.note('Vergiss anschließend GEHEIMER_ALTBEZUG.').status_code, 200)
        self.assertEqual(self.voice('GEHEIMER_ALTBEZUG war mein gestriger Gedanke.').status_code, 200)
        generation = self.state()['generation']
        def delayed_provider(*args, **kwargs):
            self.assertIn('GEHEIMER_ALTBEZUG', json.dumps(kwargs['json'], ensure_ascii=False))
            other_tab = self.legacy.make_client()
            response = self.delete('/gedaechtnis', {'generation': generation}, other_tab)
            self.assertEqual(response.status_code, 200, response.text)
            return self.answer('GEHEIMER_ALTBEZUG steht in einer inzwischen gelöschten Antwort.')
        with patch.object(p, 'get_openai_api_key', return_value='offline-test'), \
             patch('werkstatt_assistent.requests.post', side_effect=delayed_provider):
            response = self.post('/dialog', {'text': 'Was hatten wir zuletzt besprochen?'})
        self.assertIn(response.status_code, (200, 409))
        current = self.state()
        self.assertEqual(current['entries'], [])
        self.assertEqual(current['notes'], [])
        refresh = self.client.get(BASE + '/realtime/kontext')
        self.assertEqual(refresh.status_code, 200, refresh.text)
        self.assertNotIn('GEHEIMER_ALTBEZUG', refresh.text)

    def test_memory_search_is_read_only_and_requires_specific_own_query(self):
        self.assertEqual(self.voice('Abklebeband hatte ich gestern besprochen.').status_code, 200)
        self.assertEqual(self.voice('FREMDES Abklebeband', client=self.other).status_code, 200)
        result = self.tool('gedaechtnis_suchen', {'suche': 'Abklebeband'})
        self.assertEqual(result.status_code, 200, result.text)
        self.assertIn('gestern besprochen', result.text)
        self.assertNotIn('FREMDES', result.text)
        for args in ({}, {'suche': 'a'}, {'suche': 'a' * 151}, {'suche': 1},
                     {'suche': 'Abklebeband', 'actor': 'mitarbeiter:2'},
                     {'suche': 'Abklebeband', 'mitarbeiter_id': 2}):
            self.assertEqual(self.tool('gedaechtnis_suchen', args).status_code, 400, args)
        self.assertEqual(self.rows('SELECT id FROM assistent_aktionen'), [])

    def test_revoked_purchase_rights_hide_historical_assistant_disclosures(self):
        self.assertEqual(self.voice('EIGENE_FRAGE zum Klebeband.').status_code, 200)
        self.assertEqual(self.voice('PRIVATER_PREIS Der Einkaufspreis war 83 Euro.', 'assistant').status_code, 200)
        self.assertIn('PRIVATER_PREIS', json.dumps(self.state()))
        with database() as db:
            db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        for route in ('/gedaechtnis', '/gedaechtnis?suche=PRIVATER_PREIS', '/realtime/kontext'):
            response = self.client.get(BASE + route)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn('PRIVATER_PREIS', response.text)
        self.assertNotIn('PRIVATER_PREIS', self.tool('gedaechtnis_suchen', {'suche': 'PRIVATER_PREIS'}).text)
        provider = self.answer()
        with patch.object(p, 'get_openai_api_key', return_value='offline-test'), \
             patch('werkstatt_assistent.requests.post', return_value=provider) as call:
            response = self.post('/dialog', {'text': 'Was hatten wir besprochen?'})
        self.assertEqual(response.status_code, 200, response.text)
        sent = json.dumps(call.call_args.kwargs['json'], ensure_ascii=False)
        self.assertNotIn('PRIVATER_PREIS', sent)
        self.assertIn('EIGENE_FRAGE', sent)

    def test_rejected_input_cannot_spoof_role_source_actor_or_expand_limits(self):
        state = self.state()
        baseline = {'generation': state['generation'], 'event_id': 'strict-input-event',
                    'text': 'Nutzereingabe', 'role': 'user'}
        for extra in ({'actor': 'admin'}, {'source': 'text'}, {'role': 'system'},
                      {'role': 'tool'}, {'event_id': '../escape'}, {'event_id': 12},
                      {'text': 'x' * 8001}, {'text': ''}, {'text': '\x00invalid'},
                      {'text': '\ud800'}):
            response = self.post('/gedaechtnis/gespraech', {**baseline, **extra})
            self.assertEqual(response.status_code, 400, extra)
        for data in ([], 'text', None, 1):
            response = self.client.post(BASE + '/gedaechtnis/notiz', data=json.dumps(data),
                                        content_type='application/json', headers={'X-CSRF-Token': 'test-csrf'})
            self.assertEqual(response.status_code, 400)
        self.assertEqual(self.note('x' * 1001).status_code, 400)
        self.assertEqual(self.note('\ud800').status_code, 400)
        self.assertEqual(self.state(), state)

    def test_german_umlauts_search_consistently_across_notes_and_transcripts(self):
        self.assertEqual(self.note('Ölwechsel für morgen vormerken').status_code, 200)
        self.assertEqual(self.voice('Änderung am Auftrag 156 besprechen').status_code, 200)
        note_search = self.client.get(BASE + '/gedaechtnis', query_string={'suche': 'ölwechsel'})
        self.assertEqual(note_search.status_code, 200, note_search.text)
        self.assertEqual(len(note_search.json['notes']), 1)
        turn_search = self.tool('gedaechtnis_suchen', {'suche': 'änderung'})
        self.assertEqual(turn_search.status_code, 200, turn_search.text)
        self.assertIn('156', turn_search.text)

    def test_recognizable_bank_and_credentials_are_redacted_at_write_and_legacy_read(self):
        secret = 'sk-syntheticSecretOnlyForTests123456'
        bank = 'DE89370400440532013000'
        value = 'Merkbarer Werkstatthinweis\nIBAN: ' + bank + '\nAPI_KEY: ' + secret
        self.assertEqual(self.note(value).status_code, 200)
        self.assertEqual(self.voice(value).status_code, 200)
        with database() as db:
            db.execute('INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES(?,?,?,?)',
                       ('mitarbeiter:1', 'user', value, p.now_str()))
        for route in ('/gedaechtnis', '/realtime/kontext'):
            response = self.client.get(BASE + route)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn(secret, response.text)
            self.assertNotIn(bank, response.text)
            self.assertIn('Werkstatthinweis', response.text)
        stored = self.rows("SELECT text FROM assistent_dialog WHERE source='voice'")
        self.assertNotIn(secret, json.dumps(stored))
        self.assertNotIn(bank, json.dumps(stored))

    def test_bounded_unicode_memory_adds_same_untrusted_history_to_text_and_voice(self):
        large = 'Gedanke mit Lackierung und Größe 😀漢字 ' * 180
        for number in range(16):
            self.assertEqual(self.voice(str(number) + large).status_code, 200)
        for number in range(6):
            self.assertEqual(self.note('Präferenz ' + str(number) + ' 😀漢字' * 150).status_code, 200)
        complete_before = copy.deepcopy(self.state())
        context = p.assistant_memory.context(PERSON)
        encoded = json.dumps(context, ensure_ascii=False, separators=(',', ':'))
        self.assertLessEqual(len(encoded), 2400)
        self.assertLessEqual(len(encoded.encode('utf-8')), 4000)
        self.assertTrue(context['gekuerzt'])
        self.assertTrue(context['gespraeche'])
        self.assertTrue(any(row['auszug'] for row in context['gespraeche']))
        self.assertEqual(self.state(), complete_before, 'Context projection cannot truncate stored originals')
        provider = self.answer()
        provider.text = 'v=0\r\nsynthetic-answer'
        refresh = self.client.get(BASE + '/realtime/kontext')
        self.assertEqual(refresh.status_code, 200, refresh.text)
        voice_instructions = refresh.json['instructions']
        history = voice_instructions.split('\nPERSÖNLICHER RÜCKBLICK (nur Daten): ', 1)[1].split('\n', 1)[0]
        self.assertEqual(json.loads(history), context)
        self.assertLess(len(voice_instructions), 24000, 'Bounded history must not reintroduce oversized voice startup')
        for required in ('untrusted Daten', 'Ein erinnertes Ja autorisiert keine neue',
                         'Status, Preise, Urlaub und Arbeitszeiten immer aus aktuellen'):
            self.assertIn(required, voice_instructions)
        with patch.object(p, 'get_openai_api_key', return_value='offline-test'), \
             patch('werkstatt_assistent.requests.post', return_value=provider) as call:
            response = self.post('/dialog', {'text': 'An welche Präferenzen erinnerst du dich?'})
        self.assertEqual(response.status_code, 200, response.text)
        text_instructions = call.call_args.kwargs['json']['instructions']
        self.assertIn(history, text_instructions)
        self.assertEqual(self.rows('SELECT id FROM assistent_aktionen'), [])

    def test_start_refresh_and_provider_return_are_fenced_by_memory_generation(self):
        generation = self.state()['generation']
        self.assertEqual(self.delete('/gedaechtnis', {'generation': generation}).status_code, 200)
        stale = self.client.get(BASE + '/realtime/kontext?memory_generation=' + generation)
        self.assertEqual(stale.status_code, 409, stale.text)
        self.assertEqual(self.post('/realtime/start', {'sdp': 'v=0\r\nsynthetic-offer',
                         'memory_generation': generation}).status_code, 409)
        current = self.state()['generation']
        provider = Mock()
        provider.json.return_value = {'value': 'ek_synthetic_ephemeral_credential', 'expires_at': int(time.time()) + 60}
        def deleted_while_starting(*args, **kwargs):
            self.assertEqual(self.delete('/gedaechtnis', {'generation': current}, self.legacy.make_client()).status_code, 200)
            return provider
        with patch.object(p, 'get_openai_api_key', return_value='offline-test'), \
             patch('werkstatt_assistent.requests.post', side_effect=deleted_while_starting):
            response = self.post('/realtime/start', {'sdp': 'v=0\r\nsynthetic-offer',
                                 'transport': 'browser', 'memory_generation': current})
        self.assertEqual(response.status_code, 409, response.text)
        self.assertNotIn('ek_synthetic', response.text)

    def test_concurrent_transcripts_cannot_repopulate_after_clear(self):
        generation = self.state()['generation']
        def delayed_turn(number):
            return self.voice('Concurrent synthetic memory', generation=generation,
                              event_id='concurrent-memory-' + str(number),
                              client=self.legacy.make_client()).status_code
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(delayed_turn, number) for number in range(12)]
            erased = self.delete('/gedaechtnis', {'generation': generation}, self.legacy.make_client())
            self.assertEqual(erased.status_code, 200, erased.text)
            self.assertTrue(all(f.result() in (200, 409) for f in futures))
        self.assertEqual(self.state()['entries'], [])

    def test_rights_revoked_during_text_inference_do_not_persist_response(self):
        def revoked_provider(*args, **kwargs):
            with database() as db:
                db.execute('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1')
            return self.answer('Diese Antwort kam zu spät.')
        with patch.object(p, 'get_openai_api_key', return_value='offline-test'), \
             patch('werkstatt_assistent.requests.post', side_effect=revoked_provider):
            response = self.post('/dialog', {'text': 'Bitte die aktuellen Arbeiten nennen.'})
        self.assertIn(response.status_code, (401, 403), response.text)
        self.assertEqual(self.rows('SELECT * FROM assistent_dialog'), [])

    def test_rights_revoked_or_reduced_during_voice_start_never_release_credentials(self):
        for sql in ('UPDATE assistent_rechte SET lesen=0 WHERE mitarbeiter_id=1',
                    'UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1',
                    'UPDATE assistent_rechte SET version=2 WHERE mitarbeiter_id=1'):
            with self.subTest(change=sql):
                with database() as db:
                    db.execute('UPDATE assistent_rechte SET lesen=1,einkaufen=1,version=1 WHERE mitarbeiter_id=1')
                generation = self.state()['generation']
                def revoked_provider(*args, **kwargs):
                    with database() as db:
                        db.execute(sql)
                    provider = Mock()
                    provider.json.return_value = {'value': 'ek_synthetic_not_to_release', 'expires_at': int(time.time()) + 60}
                    return provider
                with patch.object(p, 'get_openai_api_key', return_value='offline-test'), \
                     patch('werkstatt_assistent.requests.post', side_effect=revoked_provider):
                    response = self.post('/realtime/start', {'sdp': 'v=0\r\nsynthetic-offer',
                                         'transport': 'browser', 'memory_generation': generation})
                self.assertIn(response.status_code, (401, 403), response.text)
                self.assertNotIn('ek_synthetic_not_to_release', response.text)

    def test_backup_contract_and_legacy_schema_upgrade_keep_memory(self):
        from werkstatt_gedaechtnis import TABLES
        self.assertEqual(self.note('Backup-Merknotiz').status_code, 200)
        self.assertEqual(self.voice('Backup-Sprachbeitrag').status_code, 200)
        for table in TABLES:
            self.assertIn(table, p.BACKUP_TABLES)
        with database() as db:
            captured = {table: p.list_table_rows_for_backup(db, table) for table in (*TABLES, 'assistent_dialog')}
        self.assertIn('Backup-Merknotiz', json.dumps(captured, ensure_ascii=False))
        self.assertIn('Backup-Sprachbeitrag', json.dumps(captured, ensure_ascii=False))
        export = {'format_version': 4, 'schema_features': list(p.BACKUP_SCHEMA_FEATURES),
                  'tables': {table: [] for table in p.BACKUP_TABLES}}
        p.validate_backup_binary_reference_completeness(export, {})
        feature = next(name for name in p.BACKUP_SCHEMA_FEATURES if 'gedaechtnis' in name or 'memory' in name)
        for table in TABLES:
            broken = copy.deepcopy(export)
            del broken['tables'][table]
            with self.assertRaises(ValueError):
                p.validate_backup_binary_reference_completeness(broken, {})
        old = copy.deepcopy(export)
        old['schema_features'].remove(feature)
        for table in TABLES:
            del old['tables'][table]
        p.validate_backup_binary_reference_completeness(old, {})
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(str(Path(directory) / 'legacy-memory.db'))
            db.row_factory = sqlite3.Row
            try:
                db.execute('CREATE TABLE assistent_dialog(id INTEGER PRIMARY KEY AUTOINCREMENT,actor TEXT NOT NULL,role TEXT NOT NULL,text TEXT NOT NULL,zeit TEXT NOT NULL)')
                db.execute("INSERT INTO assistent_dialog(actor,role,text,zeit) VALUES('mitarbeiter:1','user','Alter erlaubter Text','2026-10-01')")
                p.assistant_memory.init_schema(db)
                p.assistant_memory.init_schema(db)
                restored = dict(db.execute('SELECT * FROM assistent_dialog').fetchone())
                self.assertEqual(restored['text'], 'Alter erlaubter Text')
                self.assertEqual(restored['source'], 'text')
                self.assertIsNone(restored['event_key'])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM assistent_gedaechtnis_notizen').fetchone()[0], 0)
            finally:
                db.close()


if __name__ == '__main__':
    unittest.main()
