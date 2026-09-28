"""Native avatar operations over real HTTP routes, synthetic SQLite, no sends.

Reuse the existing app import capsule and fixture methods by composition, so
this suite neither inherits nor reruns the unrelated legacy assistant tests.
"""
import json
import unittest
from unittest.mock import Mock, patch

import test_assistent as fixture

p = fixture.p
database = fixture.database


class AssistantOperationTests(unittest.TestCase):
    def setUp(self):
        self.legacy = fixture.AssistantTests(methodName='runTest')
        self.legacy.setUp()
        self.addCleanup(self.legacy.tearDown)
        self.client = self.legacy.client
        self.enterContext(patch.dict(p.app.config, ASSISTANT_READ_ONLY=True,
                                     ASSISTANT_NATIVE_COCKPIT=True))
        self.enterContext(patch('requests.sessions.Session.request', side_effect=AssertionError('Network forbidden')))
        self.enterContext(patch('smtplib.SMTP', side_effect=AssertionError('SMTP forbidden')))
        self.enterContext(patch('smtplib.SMTP_SSL', side_effect=AssertionError('SMTP forbidden')))
        self.enterContext(patch.object(p.workshop_orders.dispatch, 'enqueue', side_effect=AssertionError('Real outbox forbidden')))
        self.enterContext(patch.object(p.workshop_orders.dispatch, 'dispatch_due', side_effect=AssertionError('Real dispatch forbidden')))
        self.enterContext(patch.object(p.workshop_orders, 'availability', return_value={
            'can_send': True, 'worker_live': True, 'enabled': True,
            'worker_enabled': True, 'mailbox_ready': True, 'storage_ready': True}))
        self.bridge = self.enterContext(patch.object(p.workshop_orders, 'submit_approved_action', side_effect=self.submit))
        self.enterContext(patch.object(p.workshop_orders, 'approved_action_status', side_effect=lambda actor, action: self.delivery.get((actor, action))))
        self.delivery = {}
        self.submitted = []
        with database() as db:
            for table in ('assistent_fortschritt_audit', 'status_log', 'assistent_bestellkontakte',
                          'assistent_bestellkonfiguration'):
                db.execute('DELETE FROM ' + table)
            db.execute("DELETE FROM app_settings WHERE key='ASSISTANT_OPERATIONS_ENABLED'")
            db.execute("UPDATE auftraege SET status=3,produktion_schritt='vorarbeit',lackierbereit=0,geaendert_am='28.09.2026 09:30' WHERE id IN (156,157)")
            db.execute("INSERT INTO assistent_bestellkontakte(id,name,recipient,source_note,verified_at,verified_by) VALUES('supplier-test','Testlieferant','orders@example.invalid','Rechnungsvorschlag','2026-09-28','admin')")
            db.execute("INSERT INTO assistent_bestellkontakte(id,name,recipient,source_note) VALUES('unverified','Ungeprüft','unknown@example.invalid','ungeprüfte Rechnung')")
        p.set_app_setting('ASSISTANT_OPERATIONS_ENABLED', '1')
        p.workshop_orders.set_setting('max_total_cents', 25000)

    def post(self, route, data=None, client=None):
        return self.legacy.post(route, data, client)

    def rows(self, sql, args=()):
        with database() as db:
            return [dict(row) for row in db.execute(sql, args).fetchall()]

    def count(self, table):
        return self.rows('SELECT COUNT(*) AS n FROM ' + table)[0]['n']

    def order(self):
        return self.rows('SELECT * FROM auftraege WHERE id=156')[0]

    def status_proposal(self, **overrides):
        return self.post('/vorschlag', dict(art='status', auftrag_id=156, aktion='lackierbereit', **overrides))

    def order_args(self, **overrides):
        result = dict(art='bestellung', auftrag_id=0, supplier_id='supplier-test',
                      teilenummer='TAPE-50', bezeichnung='Grünes Klebeband', variante='grün, 50 mm',
                      einheit='Rollen', preisquelle='Vom Mitarbeiter geprüftes Angebot TEST-1',
                      menge=2, dringend=False, stueckpreis_brutto='10.00',
                      versand_brutto='5.00', nebenkosten_brutto='0.00')
        result.update(overrides)
        return result

    def purchase_proposal(self, **overrides):
        return self.post('/vorschlag', self.order_args(**overrides))

    def saved_action(self, action_id):
        return self.rows('SELECT * FROM assistent_aktionen WHERE id=?', (action_id,))[0]

    def submit(self, actor, action_id):
        saved = self.saved_action(action_id)
        self.assertEqual(saved['actor'], actor)
        self.assertEqual(saved['status'], 'intern_freigegeben')
        payload = json.loads(saved['payload'])
        self.assertIs(payload['versand']['price_verified'], True)
        self.assertIs(payload['versand']['order_requested'], True)
        self.submitted.append((actor, action_id, payload))
        return self.delivery.setdefault((actor, action_id), {
            'id': 'synthetic-order', 'state': 'queued',
            'message': 'Für Montag eingeplant. Noch nicht versendet.'})

    def test_operations_switch_keeps_readonly_and_legacy_photo_writes_closed(self):
        self.assertTrue(p.app.config['ASSISTANT_READ_ONLY'])
        with patch('werkstatt_assistent.render_template', return_value='page') as render:
            self.assertEqual(self.client.get('/werkstatt/assistent').status_code, 200)
            self.assertEqual(render.call_args.kwargs['capabilities'], {'status': True, 'bestellen': True})
            self.assertTrue(render.call_args.kwargs['read_only'])
        for client in (self.client, self.legacy.make_client(admin=True)):
            for kind in ('notiz', 'einkauf', 'anfrage'):
                self.assertEqual(self.post('/vorschlag', {'art': kind, 'auftrag_id': 156, 'text': 'Nicht speichern'}, client).status_code, 400)
            for route in ('/foto/156', '/bestellen/not-real'):
                self.assertEqual(self.post(route, client=client).status_code, 403)
        p.set_app_setting('ASSISTANT_OPERATIONS_ENABLED', '0')
        self.assertEqual(self.status_proposal().status_code, 403)
        self.assertEqual(self.purchase_proposal().status_code, 403)
        self.assertEqual(self.client.get('/werkstatt/assistent/aktionen').json, [])
        self.assertEqual(self.count('assistent_aktionen'), 0)

    def test_status_preview_is_readonly_then_confirmation_and_replay_are_durable(self):
        before = self.order()
        response = self.status_proposal()
        self.assertEqual(response.status_code, 200, response.text)
        action = response.json
        self.assertEqual(action['status'], 'vorschlag')
        self.assertIn('nicht fertiggemeldet', action['daten']['text'])
        self.assertEqual(self.order(), before)
        self.assertEqual(self.count('assistent_fortschritt_audit'), 0)
        self.assertEqual(self.status_proposal().json['id'], action['id'])
        first = self.post('/bestaetigen/' + action['id'])
        self.assertEqual(first.status_code, 200, first.text)
        self.assertTrue(first.json['ok'])
        self.assertEqual(first.json['auftrag']['id'], 156)
        self.assertEqual(self.order()['lackierbereit'], 1)
        self.assertEqual(self.order()['status'], 3, 'paint-ready is not vehicle finished')
        second = self.post('/bestaetigen/' + action['id'])
        self.assertEqual(second.status_code, 200, second.text)
        self.assertTrue(second.json['wiederholt'])
        self.assertEqual(self.count('assistent_fortschritt_audit'), 1)
        self.assertEqual(len(self.rows("SELECT * FROM assistent_audit WHERE aktion='status_geaendert'")), 1)
        self.bridge.assert_not_called()

    def test_stale_status_snapshot_cannot_overwrite_newer_work(self):
        action = self.status_proposal().json
        with database() as db:
            # The timestamp deliberately stays equal: compare the full saved snapshot.
            db.execute("UPDATE auftraege SET produktion_schritt='finish' WHERE id=156")
        response = self.post('/bestaetigen/' + action['id'])
        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(self.order()['produktion_schritt'], 'finish')
        self.assertEqual(self.order()['lackierbereit'], 0)
        self.assertEqual(self.saved_action(action['id'])['status'], 'vorschlag')
        self.assertEqual(self.count('assistent_fortschritt_audit'), 0)

    def test_status_confirmation_rechecks_owner_role_version_and_csrf(self):
        action = self.status_proposal().json
        route = '/bestaetigen/' + action['id']
        self.assertEqual(self.post(route, client=self.legacy.make_client(admin=True)).status_code, 404)
        self.assertEqual(self.client.post('/werkstatt/assistent' + route, json={}).status_code, 400)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET dokumentieren=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post(route).status_code, 403)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET dokumentieren=1,version=2 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post(route).status_code, 401)
        self.assertEqual(self.order()['lackierbereit'], 0)

    def test_spoken_confirmation_is_bound_to_exact_phrase_nonce_and_one_action(self):
        action = self.status_proposal().json
        challenge = self.post('/vorlesen/' + action['id']).json
        self.assertIn('Lackierbereit', challenge['text'])
        self.assertEqual(self.post('/sprache-bestaetigen', {'nonce': challenge['nonce'], 'text': 'Ja'}).status_code, 400)
        self.assertEqual(self.post('/sprache-bestaetigen', {'nonce': 'wrong', 'text': challenge['phrase']}).status_code, 400)
        confirmed = self.post('/sprache-bestaetigen', {'nonce': challenge['nonce'], 'text': challenge['phrase']})
        self.assertEqual(confirmed.status_code, 200, confirmed.text)
        self.assertEqual(self.post('/sprache-bestaetigen', {'nonce': challenge['nonce'], 'text': challenge['phrase']}).status_code, 400)
        self.assertEqual(self.count('assistent_fortschritt_audit'), 1)

    def test_material_proposal_has_verified_supplier_but_no_approval_or_send(self):
        response = self.purchase_proposal(actor='admin', recipient='attacker@example.invalid',
                                          price_verified=True, order_requested=True)
        self.assertEqual(response.status_code, 200, response.text)
        action = response.json
        self.assertEqual(action['auftrag_id'], 0)
        self.assertEqual(action['daten']['gesamt_cent'], 2500)
        shipping = action['daten']['versand']
        self.assertEqual(shipping['recipient'], 'orders@example.invalid')
        self.assertEqual(shipping['variant'], 'grün, 50 mm')
        self.assertIs(shipping['price_verified'], False)
        self.assertFalse(shipping.get('order_requested', False))
        self.assertEqual(self.saved_action(action['id'])['actor'], 'mitarbeiter:1')
        self.assertEqual(self.purchase_proposal().json['id'], action['id'])
        self.bridge.assert_not_called()

    def test_order_missing_ambiguous_costs_recipient_and_budget_are_rejected(self):
        cases = [dict(supplier_id='unverified'), dict(supplier_id='missing'),
                 dict(variante=''), dict(einheit=''), dict(preisquelle=''), dict(dringend='yes'),
                 dict(menge=True), dict(menge=0), dict(menge=101),
                 dict(stueckpreis_brutto='123.00'), dict(versand_brutto=None),
                 dict(nebenkosten_brutto=''), dict(stueckpreis_brutto='1.234,56')]
        for args in cases:
            with self.subTest(args=args):
                response = self.purchase_proposal(**args)
                self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(self.count('assistent_aktionen'), 0)
        self.bridge.assert_not_called()

    def test_order_only_human_confirmation_sets_approval_and_keeps_replay_identity(self):
        action = self.purchase_proposal().json
        first = self.post('/bestaetigen/' + action['id'])
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json['status'], 'queued')
        self.assertIn('Noch nicht versendet', first.json['hinweis'])
        second = self.post('/bestaetigen/' + action['id'])
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual({(actor, identifier) for actor, identifier, _ in self.submitted}, {('mitarbeiter:1', action['id'])})
        self.assertEqual(self.submitted[0][2], self.submitted[1][2], 'authorized order is unchanged on retry')
        self.assertEqual(len(self.rows("SELECT * FROM assistent_audit WHERE aktion='bestellung_bestaetigt'")), 1)
        listed = self.client.get('/werkstatt/assistent/aktionen').json
        self.assertEqual(listed[0]['versandstatus']['state'], 'queued')

    def test_order_rechecks_current_limit_rights_and_keeps_blocked_delivery_truthful(self):
        action = self.purchase_proposal().json
        with database() as db:
            db.execute('UPDATE assistent_rechte SET limit_cent=100 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/bestaetigen/' + action['id']).status_code, 400)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET limit_cent=30000,einkaufen=0 WHERE mitarbeiter_id=1')
        self.assertEqual(self.post('/bestaetigen/' + action['id']).status_code, 403)
        self.bridge.assert_not_called()
        with database() as db:
            db.execute('UPDATE assistent_rechte SET einkaufen=1 WHERE mitarbeiter_id=1')
        self.bridge.side_effect = None
        self.bridge.return_value = {'state': 'blocked', 'message': 'Bestellversand noch nicht eingerichtet. Keine E-Mail gesendet.'}
        response = self.post('/bestaetigen/' + action['id'])
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json['status'], 'blocked')
        self.assertIn('Keine E-Mail gesendet', response.json['hinweis'])
        saved = self.saved_action(action['id'])
        self.assertEqual(saved['status'], 'vorschlag')
        pending = json.loads(saved['payload'])['versand']
        self.assertIs(pending['price_verified'], False)
        self.assertIs(pending['order_requested'], False)
        self.assertEqual(self.count('assistent_bestellanforderungen'), 0)
        self.assertEqual(self.post('/vorlesen/' + action['id']).status_code, 200)
        self.bridge.side_effect = self.submit
        retry = self.post('/bestaetigen/' + action['id'])
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertEqual(retry.json['status'], 'queued')
        self.assertEqual(self.saved_action(action['id'])['status'], 'intern_freigegeben')

    def test_order_spoken_readback_names_costs_and_expiry_prevents_approval(self):
        action = self.purchase_proposal().json
        challenge = self.post('/vorlesen/' + action['id']).json
        for expected in ('Werkstattmaterial', 'orders@example.invalid', '50 mm', '25.00 Euro', 'Preisquelle'):
            self.assertIn(expected, challenge['text'])
        with self.client.session_transaction() as session:
            expired = dict(session['assistent_bestaetigung'])
            expired['expires'] = 0
            session['assistent_bestaetigung'] = expired
        self.assertEqual(self.post('/sprache-bestaetigen', {'nonce': challenge['nonce'], 'text': challenge['phrase']}).status_code, 400)
        self.assertIs(json.loads(self.saved_action(action['id'])['payload'])['versand']['price_verified'], False)
        self.bridge.assert_not_called()

    def test_repeat_prepares_new_unapproved_order_only_after_smtp_acceptance(self):
        for delivery_state in ('sent', 'copy_pending'):
            with self.subTest(state=delivery_state):
                action = self.purchase_proposal(bezeichnung='Testartikel ' + delivery_state).json
                self.assertEqual(self.post('/bestaetigen/' + action['id']).status_code, 200)
                self.delivery[('mitarbeiter:1', action['id'])] = {'state': delivery_state, 'message': 'SMTP angenommen'}
                self.bridge.reset_mock()
                route = '/erneut-vorbereiten/' + action['id']
                key = 'synthetic-repeat-request-123456789'
                first = self.post(route, {'request_id': key})
                self.assertEqual(first.status_code, 200, first.text)
                repeated = first.json
                self.assertNotEqual(repeated['id'], action['id'])
                self.assertEqual(repeated['status'], 'vorschlag')
                self.assertIs(repeated['daten']['versand']['price_verified'], False)
                self.assertIs(repeated['daten']['versand']['order_requested'], False)
                self.assertEqual(repeated['daten']['gesamt_cent'], action['daten']['gesamt_cent'])
                self.assertEqual(self.post(route, {'request_id': key}).json['id'], repeated['id'], 'lost HTTP reply does not create another order proposal')
                self.assertIs(json.loads(self.saved_action(action['id'])['payload'])['versand']['price_verified'], True)
                self.bridge.assert_not_called()

    def test_repeat_blocks_uncertain_queued_foreign_and_changed_contact_orders(self):
        action = self.purchase_proposal().json
        self.assertEqual(self.post('/bestaetigen/' + action['id']).status_code, 200)
        self.bridge.reset_mock()
        route = '/erneut-vorbereiten/' + action['id']
        args = {'request_id': 'synthetic-repeat-request-123456789'}
        for state in ('queued', 'uncertain', 'blocked'):
            with self.subTest(state=state):
                self.delivery[('mitarbeiter:1', action['id'])] = {'state': state}
                self.assertEqual(self.post(route, args).status_code, 400)
        self.delivery[('mitarbeiter:1', action['id'])] = {'state': 'sent'}
        self.assertEqual(self.post(route, {'request_id': 'short'}).status_code, 400)
        self.assertEqual(self.post(route, args, client=self.legacy.make_client(admin=True)).status_code, 404)
        with database() as db:
            db.execute("UPDATE assistent_bestellkontakte SET recipient='changed@example.invalid' WHERE id='supplier-test'")
        self.assertEqual(self.post(route, args).status_code, 400)
        self.assertEqual(self.count('assistent_aktionen'), 1)
        self.bridge.assert_not_called()

    def realtime_config(self):
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-test'), patch('werkstatt_assistent.requests.post', return_value=Mock(text='v=0\r\nanswer')) as call:
            response = self.post('/realtime/start', {'sdp': 'v=0\r\noffer'})
            self.assertEqual(response.status_code, 200, response.text)
            return json.loads(call.call_args.kwargs['files']['session'][1])

    def test_model_tool_contract_offers_only_scoped_proposals_and_verified_contacts(self):
        config = self.realtime_config()
        tools = {tool['name']: tool for tool in config['tools']}
        self.assertTrue({'status_vorschlagen', 'bestellung_vorschlagen', 'lieferanten_lesen'} <= tools.keys())
        self.assertFalse({'bestaetigen', 'bestellen', 'erneut_vorbereiten', 'repeat_order', 'kamera', 'aktion_vorschlagen'} & tools.keys())
        self.assertIn('Keine Aktionen selbst bestätigen', config['instructions'])
        self.assertEqual(tools['bestellung_vorschlagen']['parameters']['properties']['dringend']['type'], 'boolean')
        for field in ('stueckpreis_brutto', 'versand_brutto', 'nebenkosten_brutto'):
            self.assertEqual(tools['bestellung_vorschlagen']['parameters']['properties'][field]['type'], 'string')
        contacts = self.post('/realtime/werkzeug', {'name': 'lieferanten_lesen', 'arguments': {}})
        self.assertEqual(contacts.status_code, 200, contacts.text)
        self.assertEqual([r['id'] for r in contacts.json['result']['lieferanten']], ['supplier-test'])
        self.assertEqual(contacts.json['result']['limit_cent'], 25000)
        self.assertNotIn('Rechnungsvorschlag', contacts.text)
        with database() as db:
            db.execute('UPDATE assistent_rechte SET einkaufen=0 WHERE mitarbeiter_id=1')
        tools = {tool['name'] for tool in self.realtime_config()['tools']}
        self.assertIn('status_vorschlagen', tools)
        self.assertFalse({'bestellung_vorschlagen', 'lieferanten_lesen', 'artikel_suchen', 'beleg_lesen'} & tools)

    def test_realtime_model_can_propose_but_cannot_confirm_or_send(self):
        before = self.order()
        status = self.post('/realtime/werkzeug', {'name': 'status_vorschlagen', 'arguments': {'auftrag_id': 156, 'aktion': 'lackierbereit'}})
        self.assertEqual(status.status_code, 200, status.text)
        self.assertEqual(status.json['event']['data']['status'], 'vorschlag')
        args = self.order_args(price_verified=True, order_requested=True)
        args.pop('art')
        order = self.post('/realtime/werkzeug', {'name': 'bestellung_vorschlagen', 'arguments': args})
        self.assertEqual(order.status_code, 200, order.text)
        self.assertIs(order.json['event']['data']['daten']['versand']['price_verified'], False)
        for name in ('bestaetigen', 'bestellen', 'sprache-bestaetigen'):
            self.assertEqual(self.post('/realtime/werkzeug', {'name': name, 'arguments': {'id': order.json['event']['data']['id']}}).status_code, 400)
        self.assertEqual(self.order(), before)
        self.bridge.assert_not_called()

    def test_text_model_emits_proposals_without_operational_side_effects(self):
        args = self.order_args()
        args.pop('art')
        first = Mock()
        first.json.return_value = {'output': [{'type': 'function_call', 'name': 'bestellung_vorschlagen', 'arguments': json.dumps(args), 'call_id': 'proposal-call'}]}
        second = Mock()
        second.json.return_value = {'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'Bestellvorschlag zur Prüfung bereit.'}]}]}
        with patch.object(p, 'get_openai_api_key', return_value='synthetic-test'), patch('werkstatt_assistent.requests.post', side_effect=[first, second]):
            response = self.post('/dialog', {'text': 'Bitte zwei Rollen bestellen.'})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json['events'][0]['type'], 'vorschlag')
        self.assertIs(response.json['events'][0]['data']['daten']['versand']['price_verified'], False)
        self.assertEqual(self.count('assistent_aktionen'), 1)
        self.bridge.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
