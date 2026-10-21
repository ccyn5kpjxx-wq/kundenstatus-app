"""Service classification across intake, orders, workshop views and tire guards."""
from datetime import date, timedelta
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import smoke_test as isolated
portal = isolated.portal


class AuftragsartenTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING'] = True
        portal.init_db()

    def setUp(self):
        self.client = portal.app.test_client()
        self.client.get('/anfrage?anliegen=mechanikanfrage')
        with self.client.session_transaction() as sess:
            self.csrf = sess[portal.CSRF_FIELD_NAME]
            sess['admin'] = True
        for name in ('schedule_change_backup', 'lead_notice', 'notify_lead_workshop',
                     'sende_website_lead_benachrichtigung', 'sende_autohaus_benachrichtigung_mail',
                     'sende_endkunden_mail', 'notify_customer_whatsapp_fertig'):
            obj = patch.object(portal, name)
            obj.start()
            self.addCleanup(obj.stop)
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()

    def order(self, art='', **values):
        return portal.get_auftrag(portal.create_auftrag('intern', leistungsart=art,
            kunde_name='Max Beispiel', fahrzeug='Testfahrzeug Kategorie', **values))

    def post_order(self, order, **extra):
        values = {portal.CSRF_FIELD_NAME: self.csrf, 'fahrzeug':order['fahrzeug'],
                  'kunde_name': order['kunde_name'], 'beschreibung':order['beschreibung']}
        values.update(extra)
        return self.client.post('/admin/auftrag/'+str(order['id']), data=values)

    def test_manual_admin_hall_and_partner_intake(self):
        cases = [('/admin/neu', {'fahrzeug':'Admin Test','leistungsart':'bremsen'}),
                 ('/werkstatt/auftrag-anlegen', {'kunde_name':'Hallen Test','kontakt_telefon':'0171 0000000','fahrzeug':'Halle Test','leistungsart':'oelwechsel'})]
        partner = portal.list_autohaeuser()[0]
        with self.client.session_transaction() as sess:
            sess['partner_autohaus_id'] = partner['id']
        cases.append(('/partner/'+partner['slug']+'/neu', {'fahrzeug':'Partner Test','leistungsart':'klima'}))
        for url, values in cases:
            with self.subTest(url=url):
                values[portal.CSRF_FIELD_NAME] = self.csrf
                response = self.client.post(url, data=values)
                self.assertEqual(response.status_code, 302)
                saved = next(a for a in portal.list_auftraege(include_angebote=True) if a['fahrzeug']==values['fahrzeug'])
                self.assertEqual(saved['leistungsart'], values['leistungsart'])

    def test_invalid_type_and_legacy_form_preservation(self):
        order = self.order('bremsen')
        self.assertEqual(self.post_order(order, leistungsart='<script>').status_code, 400)
        self.assertEqual(portal.get_auftrag(order['id'])['leistungsart'], 'bremsen')
        self.assertEqual(self.post_order(order).status_code, 302)
        self.assertEqual(portal.get_auftrag(order['id'])['leistungsart'], 'bremsen')
        self.assertEqual(self.post_order(order, leistungsart='inspektion').status_code, 302)
        self.assertEqual(portal.get_auftrag(order['id'])['auftragsart_label'], 'Inspektion & Wartung')
        invalid = self.client.post('/admin/neu',data={portal.CSRF_FIELD_NAME:self.csrf,'fahrzeug':'Invalid Type','leistungsart':'unknown'})
        self.assertEqual(invalid.status_code,400)
        self.assertFalse(any(a['fahrzeug']=='Invalid Type' for a in portal.list_auftraege()))

    def test_structured_customer_selection_survives_offer_acceptance(self):
        response = self.client.post('/anfrage',data={portal.CSRF_FIELD_NAME:self.csrf,
            'anliegen':'mechanikanfrage','mechanik_kategorie':['bremsen'], 'name':'Kategorien Test',
            'telefon':'0171 0000000','fahrzeug':'Bremsen Test','nachricht':'Bitte Bremsen prüfen.'})
        self.assertEqual(response.status_code,302)
        db=portal.get_db()
        row=db.execute('SELECT * FROM leads ORDER BY id DESC LIMIT 1').fetchone()
        intake=json.loads(row['portal_json'])
        self.assertEqual(intake['mechanik_kategorien'],['bremsen'])
        db.execute("UPDATE leads SET angebot_status='angebot_abgegeben' WHERE id=?",(row['id'],));db.commit();db.close()
        order_id=portal.create_order_from_accepted_lead(portal.get_lead(row['id']),intake)
        self.assertEqual(portal.get_auftrag(order_id)['leistungsart'],'bremsen')
        self.assertEqual(portal.leistungsart_aus_anfrage({'anliegen':'mechanikanfrage','mechanik_kategorien':['bremsen','oelwechsel']}),'mechanik')
        for anliegen, expected in [('glasschaden','autoglas'),('fahrzeugpflege','fahrzeugpflege'),('dellenreparatur','dellen_smart_repair')]:
            self.assertEqual(portal.leistungsart_aus_anfrage({'anliegen':anliegen}),expected)

    def test_workshop_and_cockpit_show_services_without_paint_steps(self):
        today=date.today().strftime(portal.DATE_FMT)
        order=self.order('bremsen',annahme_datum=today,beschreibung='Test Bremsauftrag')
        db=portal.get_db();db.execute('UPDATE auftraege SET status=3 WHERE id=?',(order['id'],));db.commit();db.close()
        order=portal.get_auftrag(order['id'])
        self.assertEqual(order['produktion_schritte'],[])
        self.assertNotIn('Lackierung',str(portal.kunden_produktion_steps(order)))
        self.assertIn('Bremsarbeiten',self.client.get('/werkstatt/tafel').text)
        self.assertIn('Bremsarbeiten',self.client.get('/werkstatt/auftrag/'+str(order['id'])).text)
        self.assertIn('Bremsarbeiten',self.client.get('/admin/cockpit').text)
        self.assertEqual(self.client.post('/werkstatt/auftrag/'+str(order['id'])+'/produktion/lackierung',data={portal.CSRF_FIELD_NAME:self.csrf}).status_code,400)
        search=self.client.get('/admin/cockpit/auftraege-suche?q=Bremsarbeiten').get_json()
        self.assertTrue(any(x['id']==order['id'] and x['auftragsart_label']=='Bremsarbeiten' for x in search['items']))

    def test_service_page_origin_preserves_smart_repair_and_cannot_override_brakes(self):
        form=self.client.get('/anfrage?anliegen=reparaturanfrage&leistungsart=dellen-smart-repair')
        # The hidden origin is a hint; the server validates its service/anliegen pair.
        for anliegen, hint, categories, expected in (
            ('reparaturanfrage','dellen-smart-repair',[], 'dellen_smart_repair'),
            ('mechanikanfrage','dellen-smart-repair',['bremsen'], 'bremsen'),
        ):
            response=self.client.post('/anfrage',data={portal.CSRF_FIELD_NAME:self.csrf,'anliegen':anliegen,
                'leistungsart':hint,'mechanik_kategorie':categories,'name':'Herkunft Test','telefon':'0171 0000000',
                'nachricht':'Bitte die angefragte Leistung prüfen.'})
            self.assertEqual(response.status_code,302)
            db=portal.get_db();row=db.execute('SELECT portal_json FROM leads ORDER BY id DESC LIMIT 1').fetchone();db.close()
            self.assertEqual(portal.leistungsart_aus_anfrage(json.loads(row['portal_json'])),expected)

    def test_inbox_and_delay_projections_preserve_order_service(self):
        partner=portal.list_autohaeuser()[0]
        order=self.order('bremsen',autohaus_id=partner['id'])
        db=portal.get_db()
        db.execute("UPDATE auftraege SET quelle='autohaus', status=1 WHERE id=?",(order['id'],))
        db.execute('INSERT INTO verzoegerungen (auftrag_id,quelle,meldung,erstellt_am) VALUES (?,?,?,?)',
                   (order['id'],'autohaus','Lokale Testmeldung',portal.now_str()))
        db.commit();db.close()
        with portal.app.test_request_context('/admin/cockpit'):
            data=portal.dashboard_daten(portal.list_auftraege())
            inbox=[x for x in data['postfach_items'] if x.get('ziel_url')=='/admin/auftrag/'+str(order['id'])]
            self.assertTrue(any(x['typ']=='Fahrzeug' for x in inbox))
            self.assertTrue(any(x['typ']=='Termin' for x in inbox))
            self.assertTrue(all(x['auftragsart_label']=='Bremsarbeiten' for x in inbox))
            delay=next(x for x in data['offene_verzoegerungen'] if x['auftrag_id']==order['id'])
            self.assertEqual(delay['auftragsart_label'],'Bremsarbeiten')

    def test_paint_statistics_exclude_explicit_nonpaint_types(self):
        orders=[]
        for art in ('lackierung','bremsen','oelwechsel','reifenservice','fahrzeugpflege','autoglas','dellen_smart_repair'):
            order=self.order(art);order['status']=4;order['fertig_datum_obj']=date.today();orders.append(order)
        stats=portal.build_lackier_statistik(orders,fertigmeldungen={})
        self.assertEqual(stats['gesamt'],1)
        historical=self.order('',beschreibung='Bremsen Öl Reifen lackieren')
        self.assertEqual(historical['leistungsart'],'')
        self.assertIn('Art prüfen',historical['auftragsart_label'])

    def test_booked_and_billed_tires_cannot_be_reclassified(self):
        intake=json.dumps({'anliegen':'reifenservice','kunden_wunsch_annahme_datum':'29.10.2026','kunden_wunsch_zeitfenster':'09:00'})
        booked=self.order('reifenservice',schaden_aufnahme_json=intake,annahme_datum='29.10.2026')
        response=self.post_order(booked,leistungsart='bremsen',annahme_datum='29.10.2026',annahme_uhrzeit='')
        self.assertEqual(response.status_code,400)
        self.assertEqual(portal.get_auftrag(booked['id'])['leistungsart'],'reifenservice')
        billed=self.order('reifenservice')
        db=portal.get_db();db.execute('UPDATE auftraege SET reifen_rechnung_json=? WHERE id=?',('{"schema":2}',billed['id']));db.commit();db.close()
        self.assertEqual(self.post_order(billed,leistungsart='mechanik').status_code,400)
        # A manually created tire job has no online reservation to alter.
        manual=self.order('reifenservice')
        self.assertFalse(manual['hat_reifenbuchung'])
        self.assertEqual(self.post_order(manual,leistungsart='reifenservice',annahme_datum='29.10.2026').status_code,302)
        self.assertEqual(portal.get_auftrag(manual['id'])['annahme_datum'],'29.10.2026')

    def test_concurrent_type_change_prevents_tire_snapshot_on_brake_order(self):
        order=self.order('reifenservice')
        original=portal.berechne_reifenservice
        def change_type_before_save(*args, **kwargs):
            db=portal.get_db()
            db.execute("UPDATE auftraege SET leistungsart='bremsen' WHERE id=?",(order['id'],))
            db.commit();db.close()
            return original(*args, **kwargs)
        with patch.object(portal,'berechne_reifenservice',side_effect=change_type_before_save):
            response=self.client.post('/admin/auftrag/'+str(order['id'])+'/reifen-rechnung',data={
                portal.CSRF_FIELD_NAME:self.csrf,'revision':portal.reifen_rechnungsentwurf(order)['revision'],
                'leistung':['radwechsel_bis17'],'menge_radwechsel_bis17':'1','leistungsdatum':date.today().isoformat(),
                'rechnung_name':'Max Beispiel'})
        self.assertEqual(response.status_code,400)
        saved=portal.get_auftrag(order['id'])
        self.assertEqual(saved['leistungsart'],'bremsen')
        self.assertEqual(saved['reifen_rechnung_json'],'{}')


if __name__=='__main__':
    unittest.main(verbosity=2)
