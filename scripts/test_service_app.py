"""Integrated first service-app flow; disposable data, sockets denied, providers mocked."""
from contextlib import closing
from datetime import date, timedelta
from io import BytesIO
import re
import unittest
from unittest.mock import patch
from uuid import uuid4

from werkzeug.datastructures import MultiDict, FileStorage
import fitz
import smoke_test as isolated

p = isolated.portal


class ServiceAppTests(unittest.TestCase):
    def setUp(self):
        self.flags = patch.dict(p.app.config, TESTING=True, SERVICE_APP_ENABLED=True,
            SERVICE_ACCESS_ENABLED=True, FLOTTENPORTAL_ENABLED=True,
            SERVICE_APP_TEST_TRANSPORT_ENABLED=True)
        self.flags.start(); self.addCleanup(self.flags.stop)
        for name in ('lead_notice', 'notify_lead_workshop', 'sende_website_lead_benachrichtigung',
                     'sende_autohaus_benachrichtigung_mail', 'sende_endkunden_mail',
                     'notify_customer_whatsapp_fertig', 'schedule_change_backup'):
            mock = patch.object(p, name); mock.start(); self.addCleanup(mock.stop)
        with p.PUBLIC_FORM_ATTEMPTS_LOCK:
            p.PUBLIC_FORM_ATTEMPTS.clear()
        self.fleet = p.kundenflotten
        self.company = self.fleet.create_company({'name': 'Beispiel Flotte ' + uuid4().hex[:8],
                                                  'konditionen': 'INTERNAL-DO-NOT-DISCLOSE'})
        self.other = self.fleet.create_company({'name': 'Andere Firma ' + uuid4().hex[:8]})
        self.car = self.make_car(self.company)
        self.foreign_car = self.make_car(self.other)
        self.email = 'fahrer-' + uuid4().hex[:8] + '@example.invalid'
        self.fleet.change_vehicle(self.company, self.car, 'fahrer', {'name': 'Alex Beispiel',
            'email': self.email, 'telefon': '0171 0000000', 'kanal': 'email',
            'einwilligung': 'Dokumentierte synthetische Testzustimmung'})
        invite = p.service_access.prepare_invitation({'name': 'Alex Beispiel', 'email': self.email,
            'firma_id': self.company, 'fahrzeug_ids': [self.car], 'bestaetigt': '1', 'request_id': uuid4().hex})
        self.password = 'Nur-Lokal-Service-2026'
        p.service_access.accept(invite['token'], self.password, self.password)
        self.driver = p.app.test_client(); self.manager = p.app.test_client(); self.admin = p.app.test_client()
        self.driver.get('/service/anmelden')
        result = self.driver.post('/service/anmelden', data={'csrf_token': self.csrf(self.driver),
            'email': self.email, 'passwort': self.password})
        self.assertEqual(result.status_code, 303)
        self.manager_user = 'firma-' + uuid4().hex[:12]
        credentials = {'benutzername': self.manager_user, 'passwort': self.password,
                       'passwort_wiederholen': self.password}
        p.flottenportal.configure_access(self.company, credentials)
        self.manager.get('/flotte/anmelden')
        self.assertEqual(self.manager.post('/flotte/anmelden', data={**credentials,
            'csrf_token': self.csrf(self.manager)}).status_code, 303)
        with self.admin.session_transaction() as state:
            state.update(admin=True, csrf_token='test-admin-csrf')

    def make_car(self, company):
        return self.fleet.create_vehicle(company, {'kennzeichen': 'MOS S ' + uuid4().hex[:6].upper(),
            'modell': 'Beispiel Kombi', 'km': '42000', 'km_datum': date.today().isoformat()})

    @staticmethod
    def csrf(client):
        with client.session_transaction() as state:
            return state.get('csrf_token')

    @staticmethod
    def sql(query, args=()):
        with closing(p.get_db()) as db:
            rows = db.execute(query, args).fetchall(); db.commit(); return rows

    def request(self, intent='reparaturanfrage', **changes):
        response = self.driver.get(f'/service/fahrzeug/{self.car}/anfrage?anliegen={intent}')
        self.assertEqual(response.status_code, 200)
        token = re.search(r'name="service_context" value="([^"]+)"', response.text).group(1)
        day = date.today() + timedelta(days=14)
        while day.weekday() == 6: day += timedelta(days=1)
        data = MultiDict({'csrf_token': self.csrf(self.driver), 'anliegen': intent,
            'name': 'Alex Beispiel', 'email': self.email, 'telefon': '0171 0000000',
            'fahrzeug': 'UNTRUSTED OTHER CAR', 'kennzeichen': 'WRONG',
            'nachricht': 'Beschädigung vorne prüfen, Foto folgt.', 'service_context': token,
            'wunschdatum': day.isoformat(), 'wunschzeit': '11:00', 'reifen_leistung': 'radwechsel'})
        for key, value in changes.items():
            data.setlist(key, value if isinstance(value, list) else [value])
        with p.PUBLIC_FORM_ATTEMPTS_LOCK: p.PUBLIC_FORM_ATTEMPTS.clear()
        return self.driver.post('/anfrage', data=data), token

    def case(self):
        response, _ = self.request()
        self.assertEqual(response.status_code, 302)
        lead_id = self.sql('SELECT MAX(id) FROM leads')[0][0]
        return lead_id, p.service_process.case_for_lead(lead_id)

    def prepared(self):
        lead_id, case_id = self.case()
        self.sql("UPDATE leads SET angebot_status='angebot_abgegeben',angebot_text='Stoßfänger prüfen und instand setzen',angebot_preis='120,00 EUR netto' WHERE id=?", (lead_id,))
        view = p.service_process.view(case_id, {'role': 'werkstatt', 'id': 'test-admin'})
        p.service_process.save_parties(case_id, {'auftraggeber': {'name': 'Beispiel Flotte'},
            'freigebender': {'name': 'Fuhrparkleitung', 'identity_id': p.flottenportal.access(self.company)['id']},
            'rechnungsempfaenger': {'name': 'Beispiel Rechnungsempfänger', 'strasse': 'Teststraße 8',
                                  'plz': '74821', 'ort': 'Mosbach', 'land': 'DE'},
            'zahler': {'name': 'Abweichende Test-Kostenstelle'}, 'zahlungsweg': 'firmenrechnung',
            'fallart': 'firma', 'selbstbeteiligung': '', 'selbstbeteiligung_quelle': ''},
            {'role': 'werkstatt', 'id': 'test-admin'}, expected_revision=view['revision'])
        p.service_process.sync_offer(case_id, {'role': 'werkstatt', 'id': 'test-admin'})
        return lead_id, case_id

    def approve(self, case_id, **overrides):
        self.manager.get('/service')
        with self.manager.session_transaction() as state:
            actor = {'role': 'fuhrparkleitung', 'id': state['flotte_zugang'],
                     'version': state['flotte_version'], 'company_id': self.company}
        view = p.service_process.view(case_id, actor)
        return self.manager.post(f'/service/fall/{case_id}/freigeben', data={
            'csrf_token': self.csrf(self.manager), 'revision': str(view['revision']),
            'snapshot_hash': view['snapshot_hash'], 'request_id': uuid4().hex, 'bestaetigt': '1', **overrides})

    def test_public_entry_needs_no_account_and_does_not_reveal_fleet(self):
        public = p.app.test_client()
        page = public.get('/service')
        self.assertEqual(page.status_code, 200)
        self.assertIn('keine App-Installation', page.text)
        self.assertNotIn(self.company, page.text)
        self.assertEqual(public.get(f'/service/fahrzeug/{self.car}').status_code, 403)
        with patch.dict(p.app.config, SERVICE_APP_ENABLED=False):
            self.assertEqual(public.get('/service').status_code, 404)

    def test_driver_sees_only_current_vehicle_and_no_internal_or_financial_data(self):
        lead_id, case_id = self.prepared()
        page = self.driver.get(f'/service/fahrzeug/{self.car}')
        self.assertEqual(page.status_code, 200)
        self.assertNotIn('INTERNAL-DO-NOT-DISCLOSE', page.text)
        self.assertNotIn('120.00', page.text)
        self.assertNotIn('Teststraße 8', page.text)
        self.assertNotIn(p.get_lead(lead_id)['kunden_status_token'], page.text)
        self.assertEqual(self.driver.get(f'/service/fahrzeug/{self.foreign_car}').status_code, 404)
        self.assertEqual(self.driver.post(f'/service/fall/{case_id}/freigeben',
            data={'csrf_token': self.csrf(self.driver)}).status_code, 403)

    def test_signed_request_uses_database_vehicle_and_existing_lead_only(self):
        response, _ = self.request()
        self.assertEqual(response.status_code, 302)
        lead_id = self.sql('SELECT MAX(id) FROM leads')[0][0]
        lead = p.get_lead(lead_id)
        car = self.fleet.vehicle(self.company, self.car)['vehicle']
        self.assertEqual(lead['kennzeichen'], car['kennzeichen'])
        self.assertEqual(lead['fahrzeug'], car['modell'])
        self.assertEqual(lead['portal_data']['service_company_id'], self.company)
        self.assertTrue(p.service_process.case_for_lead(lead_id))
        self.assertFalse(lead['auftrag_id'])
        success = self.driver.get(response.location)
        self.assertIn('/service/fahrzeug/' + self.car, success.text)
        self.assertNotIn(lead['kunden_status_token'], success.text)
        self.assertNotIn(lead['kunden_status_token'], str(p.lead_notice.call_args))

    def test_tampered_or_foreign_context_cannot_create_lead(self):
        before = self.sql('SELECT COUNT(*) FROM leads')[0][0]
        response, _ = self.request(service_context='invalid-context')
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.sql('SELECT COUNT(*) FROM leads')[0][0], before)

    def test_vehicle_change_revokes_open_page_and_signed_request(self):
        page = self.driver.get(f'/service/fahrzeug/{self.car}/anfrage')
        token = re.search(r'name="service_context" value="([^"]+)"', page.text).group(1)
        csrf = self.csrf(self.driver)
        self.fleet.change_vehicle(self.company, self.car, 'fahrer', {'name': 'Neue Person',
            'email': 'neu@example.invalid', 'telefon': '', 'kanal': 'keine', 'einwilligung': ''})
        self.assertEqual(self.driver.get(f'/service/fahrzeug/{self.car}').status_code, 403)
        self.assertEqual(self.driver.post('/anfrage', data={'csrf_token': csrf,
            'service_context': token, 'anliegen': 'reparaturanfrage'}).status_code, 403)

    def test_old_status_links_cannot_bypass_service_access(self):
        lead_id, _ = self.prepared()
        lead = p.get_lead(lead_id)
        token = lead['kunden_status_token']
        self.assertIn('/service/fahrzeug/' + self.car, lead['kunden_status_url'])
        anon = p.app.test_client()
        for path in (f'/status/{token}', f'/status/{token}/qr.svg',
                     f'/status/{token}/bild/1', f'/status/{token}/dokument/1',
                     f'/kunden-app/fall/{token}', f'/kunden-app/fall/{token}/status'):
            self.assertEqual(anon.get(path).status_code, 403, path)
            self.assertEqual(self.driver.get(path).status_code, 303, path)
        self.assertEqual(self.driver.post(f'/status/{token}/angebot-annehmen',
            data={'csrf_token': self.csrf(self.driver)}).status_code, 409)
        self.fleet.change_vehicle(self.company, self.car, 'fahrer', {'name': 'Neue Person',
            'email': 'neu@example.invalid', 'telefon': 'SECRET-NEW-PHONE', 'kanal': 'keine', 'einwilligung': ''})
        self.assertEqual(self.driver.get(f'/status/{token}').status_code, 403)
        with patch.dict(p.app.config, SERVICE_APP_ENABLED=False):
            self.assertEqual(anon.get(f'/status/{token}').status_code, 403)

    def test_driver_change_during_request_render_returns_no_new_contact(self):
        render = p.render_website_anfrage
        def changed(**kwargs):
            self.fleet.change_vehicle(self.company, self.car, 'fahrer', {'name': 'Neue Person',
                'email': 'new-person@example.invalid', 'telefon': 'SECRET-NEW-PHONE', 'kanal': 'keine', 'einwilligung': ''})
            return render(**kwargs)
        with patch.object(p, 'render_website_anfrage', side_effect=changed):
            response = self.driver.get(f'/service/fahrzeug/{self.car}/anfrage')
        self.assertEqual(response.status_code, 403)
        self.assertNotIn('SECRET-NEW-PHONE', response.text)

    def test_approval_to_existing_order_status_and_explicit_invoice_recipient(self):
        lead_id, case_id = self.prepared()
        with self.assertRaises(ValueError):
            p.create_order_from_accepted_lead(p.get_lead(lead_id), p.get_lead(lead_id)['portal_data'])
        approved = self.approve(case_id)
        self.assertEqual(approved.status_code, 303)
        order_id = p.get_lead(lead_id)['auftrag_id']
        self.assertTrue(order_id)
        self.assertEqual(p.service_process.case_for_order(order_id), case_id)
        self.assertEqual(len(self.sql('SELECT * FROM kundenflotten_auftraege WHERE auftrag_id=?', (order_id,))), 1)
        started = self.admin.post(f'/admin/status/{order_id}/3', data={'csrf_token': self.csrf(self.admin)})
        self.assertEqual(started.status_code, 302)
        context = p.build_lexware_rechnung_context(p.get_auftrag(order_id), 120)
        self.assertEqual(context['kunde']['name'], 'Beispiel Rechnungsempfänger')
        self.assertEqual(context['kunde']['strasse'], 'Teststraße 8')
        self.assertNotEqual(context['kunde']['name'], 'Abweichende Test-Kostenstelle')
        with self.assertRaises(ValueError):
            p.build_lexware_rechnung_context(p.get_auftrag(order_id), 150)
        self.sql("UPDATE auftraege SET werkstatt_angebot_preis='150 EUR netto' WHERE id=?", (order_id,))
        blocked = self.admin.post(f'/admin/status/{order_id}/4', data={'csrf_token': self.csrf(self.admin)})
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(p.get_auftrag(order_id)['status'], 3)
        self.sql('UPDATE auftraege SET status=5 WHERE id=?', (order_id,))
        with patch.object(p, 'create_lexware_invoice_draft') as provider:
            for response in (self.admin.get(f'/admin/auftrag/{order_id}/rechnung'),
                self.admin.post(f'/admin/auftrag/{order_id}/rechnung/lexware', data={
                    'csrf_token': self.csrf(self.admin), 'rechnung_pruefung_bestaetigt': '1', 'netto_betrag': '120'})):
                self.assertEqual(response.status_code, 303)
                self.assertIn('/admin/service/fall/', response.location)
            provider.assert_not_called()

    def test_approval_csrf_and_explicit_confirmation_required(self):
        _, case_id = self.prepared()
        self.assertEqual(self.approve(case_id, csrf_token='bad').status_code, 400)
        self.assertEqual(self.approve(case_id, bestaetigt='').status_code, 400)
        self.assertFalse(p.service_process.view(case_id, {'role': 'werkstatt', 'id': 'test'})['freigegeben'])

    def test_legacy_acceptance_does_not_record_false_success(self):
        from werkzeug.exceptions import Conflict
        lead_id, _ = self.prepared()
        before = len(p.list_lead_portal_events(lead_id))
        with p.app.test_request_context('/status/test/angebot-annehmen', method='POST', data={
            'angebot_annehmen_bestaetigt': '1', 'wunsch_annahme_datum': (date.today()+timedelta(days=14)).isoformat(),
            'transport_art': 'standard', 'ersatzfahrzeug': 'nein'}):
            with self.assertRaises(Conflict):
                p.accept_lead_offer_from_portal('synthetic-token', p.get_lead(lead_id))
        self.assertEqual(len(p.list_lead_portal_events(lead_id)), before)
        self.assertFalse(p.get_lead(lead_id)['auftrag_id'])

    def test_invoice_uses_current_approved_scope_and_no_driver_contacts(self):
        lead_id, case_id = self.prepared()
        self.assertEqual(self.approve(case_id).status_code, 303)
        order_id = p.get_lead(lead_id)['auftrag_id']
        stale = p.get_auftrag(order_id)
        self.sql('UPDATE auftraege SET werkstatt_angebot_text=? WHERE id=?', ('Bremsen hinten prüfen', order_id))
        p.service_process.sync_offer(case_id, {'role': 'werkstatt', 'id': 'test'})
        self.assertEqual(self.approve(case_id).status_code, 303)
        context = p.build_lexware_rechnung_context(stale, 120)
        self.assertEqual(context['positionen'][0]['bezeichnung'], 'Bremsen hinten prüfen')
        self.assertEqual(context['kunde']['email'], '')
        self.assertEqual(context['kunde']['telefon'], '')

    def test_invoice_provider_holds_approval_transaction_and_checks_before_writes(self):
        import sqlite3
        lead_id, _ = self.prepared()
        case_id = p.service_process.case_for_lead(lead_id)
        self.assertEqual(self.approve(case_id).status_code, 303)
        order = p.get_auftrag(p.get_lead(lead_id)['auftrag_id'])
        context = p.build_lexware_rechnung_context(order, 120)
        def locked_contact(customer):
            self.assertEqual(customer['email'], '')
            with closing(p.get_db()) as other:
                other.execute('PRAGMA busy_timeout=10')
                with self.assertRaises(sqlite3.OperationalError):
                    other.execute('UPDATE serviceprozesse SET revision=revision+1 WHERE id=?', (case_id,))
                other.rollback()
            return 'synthetic-contact', False, True
        with patch.object(p, 'ensure_lexware_contact', side_effect=locked_contact) as contact, \
             patch.object(p, 'lexware_request', return_value={'id': 'synthetic-invoice'}) as provider:
            result = p.create_lexware_invoice_draft(order, context, 120)
            self.assertEqual(result['invoice_id'], 'synthetic-invoice')
            payload = provider.call_args.args[2]
            self.assertEqual(payload['address']['name'], 'Beispiel Rechnungsempfänger')
            self.assertNotIn('contactId', payload['address'])
            contact.reset_mock(); provider.reset_mock()
            self.sql("UPDATE auftraege SET werkstatt_angebot_preis='160 EUR netto' WHERE id=?", (order['id'],))
            with self.assertRaises(ValueError):
                p.create_lexware_invoice_draft(order, context, 120)
            contact.assert_not_called(); provider.assert_not_called()

    def test_tire_reservation_keeps_hour_guard_and_links_service_case(self):
        self.sql('DELETE FROM reifen_termin_slots')
        first, _ = self.request('reifenservice')
        self.assertEqual(first.status_code, 302)
        lead_id = self.sql('SELECT MAX(id) FROM leads')[0][0]
        self.assertTrue(p.service_process.case_for_lead(lead_id))
        page = self.driver.get(f'/service/fahrzeug/{self.car}')
        self.assertIn('11:00', page.text)
        self.assertIn('Reifentermin gebucht', page.text)
        reminder = p.service_reminders.vehicle_inbox(self.company, self.car)
        self.assertTrue(any(item['id'] == f'reifen:{lead_id}' and item['due_time'] == '11:00' for item in reminder))
        self.assertEqual(self.request('reifenservice')[0].status_code, 409)
        with self.assertRaises(ValueError): p.reifenauftrag_aus_buchung(lead_id)
        self.assertFalse(p.get_lead(lead_id)['auftrag_id'])

    def test_only_explicitly_visible_vehicle_receipts_download(self):
        with fitz.open() as pdf:
            pdf.new_page().insert_text((40, 50), 'Synthetic service invoice')
            raw = pdf.tobytes()
        doc = p.flottenportal.upload(self.company, self.car, {'art': 'rechnung', 'titel': 'Testrechnung',
            'belegdatum': date.today().isoformat(), 'betrag': '142,80', 'sichtbar': '1',
            'request_id': uuid4().hex}, [FileStorage(stream=BytesIO(raw), filename='testrechnung.pdf')], internal=True)
        response = self.driver.get(f'/service/fahrzeug/{self.car}/beleg/{doc}')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, raw)
        self.assertEqual(self.driver.get(f'/service/fahrzeug/{self.foreign_car}/beleg/{doc}').status_code, 404)

    def test_reminder_preview_test_and_withdrawn_assignment(self):
        key = uuid4().hex
        self.sql('INSERT INTO kundenflotten_plaene VALUES(?,?,?,?,?,NULL,NULL,NULL,?,\'offen\',1,?)',
            (key, self.car, 'hu', 'HU laut Testunterlage', date.today().isoformat(),
             'Synthetischer dokumentierter Termin', date.today().isoformat()))
        endpoint = f'/service/fahrzeug/{self.car}/erinnerung/plan:{key}'
        preview = self.driver.get(endpoint)
        self.assertEqual(preview.status_code, 200)
        snapshot = re.search(r'name="snapshot" value="([^"]+)"', preview.text).group(1)
        response = self.driver.post(endpoint + '/test', data={'csrf_token': self.csrf(self.driver),
            'snapshot': snapshot, 'request_id': uuid4().hex})
        self.assertEqual(response.status_code, 303)
        self.assertEqual(self.sql('SELECT status FROM kundenflotten_erinnerung_tests WHERE fahrzeug_id=?', (self.car,))[0][0], 'lokal_getestet')
        p.lead_notice.assert_not_called()
        with patch.dict(p.app.config, SERVICE_APP_TEST_TRANSPORT_ENABLED=False):
            response = self.driver.post(endpoint + '/test', data={'csrf_token': self.csrf(self.driver),
                'snapshot': snapshot, 'request_id': uuid4().hex})
            self.assertEqual(response.status_code, 409)
        self.assertEqual(self.driver.post(endpoint + '/test', data={'csrf_token': 'bad',
            'snapshot': snapshot, 'request_id': uuid4().hex}).status_code, 400)
        self.assertEqual(self.driver.get(f'/service/fahrzeug/{self.foreign_car}/erinnerung/plan:{key}').status_code, 404)
        self.fleet.change_vehicle(self.company, self.car, 'fahrer', {'name': 'Neue Person',
            'email': 'new@example.invalid', 'telefon': '', 'kanal': 'keine', 'einwilligung': ''})
        self.assertEqual(self.driver.post(endpoint + '/test', data={'csrf_token': self.csrf(self.driver),
            'snapshot': snapshot, 'request_id': uuid4().hex}).status_code, 403)
        self.assertEqual(len(self.sql('SELECT * FROM kundenflotten_erinnerung_tests WHERE fahrzeug_id=?', (self.car,))), 1)

    def test_admin_parties_screen_and_online_payment_disabled(self):
        _, case_id = self.prepared()
        page = self.admin.get('/admin/service/fall/' + case_id)
        self.assertEqual(page.status_code, 200)
        self.assertIn('Verantwortlichkeiten festlegen', page.text)
        self.assertIn('Rechnungsempfänger', page.text)
        self.assertFalse(p.service_process.view(case_id, {'role': 'werkstatt', 'id': 'test'})['online_payment_enabled'])
        self.assertEqual(self.driver.get('/admin/service').status_code, 302)


if __name__ == '__main__':
    unittest.main(verbosity=2)
