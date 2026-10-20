"""Public tire request -> protected workshop views, using disposable test data."""
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from werkzeug.datastructures import MultiDict

sys.path.insert(0, str(Path(__file__).resolve().parent))
import smoke_test as isolated

portal = isolated.portal


class ReifentermineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING'] = True
        portal.init_db()

    def setUp(self):
        self.client = portal.app.test_client()
        self.client.get('/anfrage?anliegen=reifenservice')
        with self.client.session_transaction() as session:
            self.csrf = session[portal.CSRF_FIELD_NAME]
        self.patches = [patch.object(portal, name) for name in (
            'sende_website_lead_benachrichtigung', 'lead_notice', 'notify_lead_workshop'
        )]
        for provider in self.patches:
            provider.start()
            self.addCleanup(provider.stop)

    def count(self, table):
        db = portal.get_db()
        try:
            return db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
        finally:
            db.close()

    def book(self, changes=None):
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        data = MultiDict({
            portal.CSRF_FIELD_NAME: self.csrf, 'anliegen': 'reifenservice',
            'name': 'Reifen Testkunde', 'email': 'reifen-test@example.invalid',
            'telefon': '0171 1234567', 'fahrzeug': 'VW Golf', 'kennzeichen': 'mos test 1',
            'nachricht': '', 'wunschdatum': (date.today() + timedelta(days=7)).isoformat(),
            'wunschzeit': '09:30',
        })
        data.setlist('reifen_leistung', ['radwechsel', 'einlagerung', 'radwechsel'])
        for key, value in (changes or {}).items():
            data.setlist(key, value if isinstance(value, list) else [value])
        response = self.client.post('/anfrage', data=data)
        return response

    def latest(self):
        db = portal.get_db()
        try:
            lead_id = db.execute('SELECT MAX(id) FROM leads').fetchone()[0]
        finally:
            db.close()
        return portal.get_lead(lead_id)

    def login_admin(self):
        with self.client.session_transaction() as session:
            session['admin'] = True

    def test_form_and_entry_button(self):
        response = self.client.get('/anfrage?anliegen=reifenservice')
        self.assertIn('type="time"', response.text)
        self.assertIn('id="kennzeichen"', response.text)
        self.assertIn('erst nach unserer Bestätigung', response.text)
        self.assertIn('Terminanfrage senden', response.text)
        detail = self.client.get('/leistungen/reifenservice')
        self.assertEqual(detail.status_code, 200)
        self.assertIn('Reifentermin buchen', detail.text)

    def test_request_is_structured_without_order_or_price_commitment(self):
        before = self.count('auftraege')
        response = self.book()
        self.assertEqual(response.status_code, 302)
        lead = self.latest()
        self.assertEqual(lead['kennzeichen'], 'MOS TEST 1')
        self.assertEqual(lead['portal_data']['reifen_leistungen'], ['radwechsel', 'einlagerung'])
        self.assertEqual(lead['portal_data']['kunden_wunsch_zeitfenster'], '09:30')
        self.assertEqual(self.count('auftraege'), before)
        self.assertFalse(lead['auftrag_id'])
        self.assertIn('noch keine Termin-, Preis- oder Auftragsbestätigung', self.client.get(response.location).text)
        portal.notify_lead_workshop.assert_called_once()

    def test_invalid_dates_times_and_services_do_not_create_leads(self):
        cases = [
            {'wunschdatum': ''}, {'wunschdatum': '2026-02-30'},
            {'wunschdatum': '20261025'}, {'wunschdatum': '2026-W43-1'},
            {'wunschdatum': (date.today() + timedelta(days=7)).isoformat() + 'extra'},
            {'wunschdatum': (date.today() - timedelta(days=1)).isoformat()},
            {'wunschzeit': ''}, {'wunschzeit': '25:01'}, {'wunschzeit': '9:30'},
            {'wunschzeit': '09:61'}, {'wunschzeit': 'vormittags'}, {'wunschzeit': '0١:30'},
            {'reifen_leistung': []}, {'reifen_leistung': ['hacking']},
            {portal.CSRF_FIELD_NAME: 'invalid'},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                before = self.count('leads')
                self.assertEqual(self.book(changes).status_code, 400)
                self.assertEqual(self.count('leads'), before)

    def test_past_time_today_is_rejected_in_berlin(self):
        today = date.today().isoformat()
        fixed = datetime.fromisoformat(today + 'T14:00').replace(tzinfo=ZoneInfo('Europe/Berlin'))
        with patch.object(portal, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = fixed
            response = self.book({'wunschdatum': today, 'wunschzeit': '13:59'})
        self.assertEqual(response.status_code, 400)
        self.assertIn('Wunschtermin darf nicht in der Vergangenheit', response.text)

    def test_public_and_hall_access_cannot_open_admin_queue(self):
        self.assertEqual(self.client.get('/admin/reifentermine').status_code, 302)
        self.assertEqual(self.client.get('/werkstatt/tafel').status_code, 302)
        portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, 'REIFEN-TEST')
        with self.client.session_transaction() as session:
            session['werkstatt_tafel'] = portal.werkstatt_tafel_session_token()
        self.assertEqual(self.client.get('/admin/reifentermine').status_code, 302)

    def test_workshop_views_show_request_and_escape_user_input(self):
        self.book({'name': '<script>TEST-REIFEN</script>'})
        lead = self.latest()
        self.login_admin()
        overview = self.client.get('/admin/reifentermine')
        self.assertEqual(overview.status_code, 200)
        self.assertEqual(overview.headers['Cache-Control'], 'no-store')
        self.assertIn('Bestätigung offen', overview.text)
        self.assertIn('MOS TEST 1', overview.text)
        self.assertIn('Reifeneinlagerung', overview.text)
        self.assertIn('reifen-test@example.invalid', overview.text)
        self.assertIn('&lt;script&gt;TEST-REIFEN&lt;/script&gt;', overview.text)
        self.assertNotIn('<script>TEST-REIFEN</script>', overview.text)
        hall = self.client.get('/werkstatt/tafel')
        self.assertEqual(hall.status_code, 200)
        self.assertIn('Reifen-Terminwünsche', hall.text)
        self.assertNotIn('reifen-test@example.invalid', hall.text)
        self.assertNotIn('0171 1234567', hall.text)
        with portal.app.test_request_context('/admin/cockpit'):
            actions = portal.cockpit_aktionsuebersicht([])
        self.assertTrue(any(item.get('lead_id') == lead['id'] for item in actions['groups']['termine']))
        self.assertFalse(any(item.get('lead_id') == lead['id'] for item in actions['groups']['angebote']))
        cockpit = self.client.get('/admin/cockpit')
        self.assertEqual(cockpit.status_code, 200)
        self.assertIn('/admin/reifentermine', cockpit.text)

    def test_customer_reschedule_is_reflected_without_duplicate_request(self):
        self.book()
        lead = self.latest()
        before = self.count('leads')
        new_date = (date.today() + timedelta(days=11)).isoformat()
        response = self.client.post('/status/' + lead['kunden_status_token'] + '/termin', data={
            portal.CSRF_FIELD_NAME: self.csrf, 'wunschtermin': new_date, 'zeitfenster': '14:15'
        })
        self.assertEqual(response.status_code, 302)
        current = next(item for item in portal.list_reifentermine() if item['id'] == lead['id'])
        self.assertEqual(current['datum_iso'], new_date)
        self.assertEqual(current['zeit'], '14:15')
        self.assertEqual(self.count('leads'), before)

    def test_closed_requests_are_not_pending_and_malformed_data_is_safe(self):
        self.book()
        lead = self.latest()
        db = portal.get_db()
        db.execute("UPDATE leads SET status='verloren' WHERE id=?", (lead['id'],))
        db.commit()
        db.close()
        self.assertFalse(any(item['id'] == lead['id'] for item in portal.list_reifentermine()))
        for value in ('{broken', '[]', 'null', '{"anliegen":"reifenservice","reifen_leistungen":[{},null]}'):
            self.assertIsNone(portal.reifen_termin_daten({'portal_json': value}))

    def test_queue_reads_do_not_create_messages_or_modify_leads(self):
        self.book()
        db = portal.get_db()
        before = [tuple(row) for row in db.execute('SELECT * FROM leads').fetchall()]
        db.close()
        notifications = self.count('benachrichtigungen')
        with patch.object(portal, 'sync_mietwagen_anfragen_to_leads', side_effect=AssertionError('No sync on GET')):
            portal.list_reifentermine()
            portal.list_reifentermine()
        db = portal.get_db()
        after = [tuple(row) for row in db.execute('SELECT * FROM leads').fetchall()]
        db.close()
        self.assertEqual(before, after)
        self.assertEqual(notifications, self.count('benachrichtigungen'))


if __name__ == '__main__':
    unittest.main(verbosity=2)
