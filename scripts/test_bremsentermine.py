"""Brake enquiries preserve parts information without reserving a workshop slot."""
from datetime import date, timedelta
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parent))
import smoke_test as isolated
portal=isolated.portal


class BremsenAnfrageTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING']=True
        portal.init_db()

    def setUp(self):
        self.client=portal.app.test_client()
        self.client.get('/anfrage?anliegen=bremsenservice')
        with self.client.session_transaction() as sess:
            self.csrf=sess[portal.CSRF_FIELD_NAME]
            sess['admin']=True
        for name in ('sende_website_lead_benachrichtigung','lead_notice','notify_lead_workshop',
                     'schedule_change_backup','sende_autohaus_benachrichtigung_mail','sende_endkunden_mail'):
            mock=patch.object(portal,name);mock.start();self.addCleanup(mock.stop)

    def post(self,**changes):
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK: portal.PUBLIC_FORM_ATTEMPTS.clear()
        data={portal.CSRF_FIELD_NAME:self.csrf,'anliegen':'bremsenservice','name':'Bremsen Beispiel',
              'telefon':'0171 0000000','fahrzeug':'Testfahrzeug','kennzeichen':'MOS TEST',
              'fin':'WVWZZZ1JZXW000001','bremsen_achse':'vorne','teile_qualitaet':'marke'}
        data.update(changes)
        return self.client.post('/anfrage',data=data)

    def counts(self):
        db=portal.get_db()
        try:return {t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0]
                    for t in ('leads','auftraege','reifen_termin_slots')}
        finally:db.close()

    def latest(self):
        db=portal.get_db();lid=db.execute('SELECT MAX(id) FROM leads').fetchone()[0];db.close()
        return portal.get_lead(lid)

    def test_request_stores_parts_choices_without_booking_or_order(self):
        before=self.counts()
        wish=(date.today()+timedelta(days=7)).isoformat()
        response=self.post(wunschdatum=wish,wunschzeit='09:00')
        self.assertEqual(response.status_code,302)
        after=self.counts()
        self.assertEqual(after['leads'],before['leads']+1)
        self.assertEqual(after['auftraege'],before['auftraege'])
        self.assertEqual(after['reifen_termin_slots'],before['reifen_termin_slots'])
        lead=self.latest();data=lead['portal_data']
        self.assertEqual(data['fin'],'WVWZZZ1JZXW000001')
        self.assertEqual(data['bremsen_achse'],'vorne')
        self.assertEqual(data['teile_qualitaet'],'marke')
        self.assertEqual(data['kunden_wunsch_zeitfenster'],'')
        self.assertFalse(lead['auftrag_id'])
        self.assertIn('persönlich',lead['naechste_aktion'])

    def test_bad_fin_missing_vehicle_phone_and_unknown_choices_rejected(self):
        for fields in ({'fin':''},{'fin':'WVWZZZ1JZXW00000'},{'fin':'WVWZZZ1JZXW0000012'},
                       {'fin':'WVWZZZ1JZXW00000I'},{'bremsen_achse':''},{'bremsen_achse':'alle'},
                       {'teile_qualitaet':''},{'teile_qualitaet':'<script>'},
                       {'fahrzeug':''},{'telefon':''}):
            with self.subTest(fields=fields):
                before=self.counts()
                self.assertEqual(self.post(**fields).status_code,400)
                self.assertEqual(self.counts(),before)

    def test_accepted_offer_preserves_fin_and_brake_category(self):
        self.assertEqual(self.post(bremsen_achse='beide',teile_qualitaet='premium').status_code,302)
        lead=self.latest();db=portal.get_db()
        db.execute("UPDATE leads SET angebot_status='angebot_abgegeben' WHERE id=?",(lead['id'],))
        db.commit();db.close();lead=portal.get_lead(lead['id'])
        order=portal.get_auftrag(portal.create_order_from_accepted_lead(lead,dict(lead['portal_data'])))
        self.assertEqual(order['fin_nummer'],'WVWZZZ1JZXW000001')
        self.assertEqual(order['leistungsart'],'bremsen')
        self.assertEqual(order['schaden_aufnahme']['bremsen_achse'],'beide')
        self.assertEqual(order['schaden_aufnahme']['teile_qualitaet'],'premium')
        self.assertEqual(order['produktion_schritte'],[])
        self.assertFalse(order['hat_reifenbuchung'])

    def test_other_mechanic_enquiries_do_not_gain_brake_requirements(self):
        self.assertEqual(self.post(anliegen='mechanikanfrage',fin='',bremsen_achse='',
            teile_qualitaet='',mechanik_kategorie='oelwechsel',nachricht='Bitte Ölwechsel anbieten.').status_code,302)
        self.assertEqual(self.latest()['portal_data']['anliegen'],'mechanikanfrage')


if __name__=='__main__':unittest.main(verbosity=2)
