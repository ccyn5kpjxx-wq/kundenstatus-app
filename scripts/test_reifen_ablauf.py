"""Isolated booking -> scheduled work -> actual services -> Lexware draft/export."""
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0,str(Path(__file__).resolve().parent))
import smoke_test as isolated
portal = isolated.portal


class ReifenAblaufTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        portal.app.config['TESTING'] = True
        portal.init_db()

    def setUp(self):
        db = portal.get_db()
        db.execute('DELETE FROM reifen_termin_slots')
        db.commit(); db.close()
        self.client = portal.app.test_client()
        self.client.get('/anfrage?anliegen=reifenservice')
        with self.client.session_transaction() as sess:
            self.csrf = sess[portal.CSRF_FIELD_NAME]
            sess['admin'] = True
        for name in ['sende_website_lead_benachrichtigung','lead_notice','notify_lead_workshop',
                     'sende_autohaus_benachrichtigung_mail','sende_endkunden_mail',
                     'notify_customer_whatsapp_fertig','schedule_change_backup']:
            provider = patch.object(portal,name)
            mocked = provider.start()
            if name == 'notify_customer_whatsapp_fertig':
                mocked.return_value = (False,'Test ohne Versand')
            self.addCleanup(provider.stop)
        self.day = date.today()+timedelta(days=7)
        while self.day.weekday()==6:
            self.day += timedelta(days=1)

    def book(self,time='09:00'):
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        response = self.client.post('/anfrage',data={
            portal.CSRF_FIELD_NAME:self.csrf,'anliegen':'reifenservice','name':'Max <Beispiel>',
            'telefon':'0171 1234567','wunschdatum':self.day.isoformat(),'wunschzeit':time,
            'reifen_leistung':['radwechsel','einlagerung'],'fahrzeug':'Golf Test','kennzeichen':'MOS TEST',
        })
        self.assertEqual(response.status_code,302)
        db = portal.get_db()
        lead_id = db.execute('SELECT MAX(id) FROM leads').fetchone()[0]
        db.close()
        return portal.get_lead(lead_id)

    def order(self):
        lead = self.book()
        response = self.client.post(f'/admin/reifenportal/{lead["id"]}/auftrag',data={portal.CSRF_FIELD_NAME:self.csrf})
        self.assertEqual(response.status_code,302)
        return portal.get_auftrag(portal.get_lead(lead['id'])['auftrag_id']),lead

    def save(self,order,**changes):
        data = {
            portal.CSRF_FIELD_NAME:self.csrf,'revision':portal.reifen_rechnungsentwurf(order)['revision'],
            'leistung':['radwechsel_bis17','einlagerung'],'menge_radwechsel_bis17':'1','menge_einlagerung':'1',
            'rechnung_name':'Max Beispiel','rechnung_strasse':'Teststraße 1','rechnung_plz':'74821',
            'rechnung_ort':'Mosbach','leistungsdatum':date.today().isoformat(),
        }
        data.update(changes)
        return self.client.post(f'/admin/auftrag/{order["id"]}/reifen-rechnung',data=data)

    def finished(self,order):
        db=portal.get_db()
        db.execute('UPDATE auftraege SET status=4 WHERE id=?',(order['id'],))
        db.commit();db.close()
        return portal.get_auftrag(order['id'])

    def invoice(self,order):
        return self.client.post(f'/admin/auftrag/{order["id"]}/rechnung/lexware',data={
            portal.CSRF_FIELD_NAME:self.csrf,'rechnung_pruefung_bestaetigt':'1',
            'reifen_revision':portal.reifen_rechnungsentwurf(order)['revision'],'netto_betrag':'0.01',
        })

    def test_booking_to_work_without_offer_and_duplicate_takeover(self):
        order,lead=self.order()
        self.assertTrue(order['ist_reifenauftrag'])
        self.assertEqual(order['status'],2)
        self.assertEqual(order['annahme_uhrzeit'],'09:00')
        self.assertEqual(order['annahme_datum'],portal.format_date(self.day.isoformat()))
        self.assertNotEqual(order['angebot_status'],'angenommen')
        with ThreadPoolExecutor(max_workers=2) as pool:
            ids=list(pool.map(lambda _:portal.reifenauftrag_aus_buchung(lead['id']),range(2)))
        self.assertEqual(ids,[order['id'],order['id']])
        self.assertNotIn('09:00',portal.freie_reifen_slots(self.day.isoformat()))
        page=self.client.get('/admin/reifenportal')
        self.assertIn('Fertig · Rechnung erstellen',page.text)
        self.assertIn(f'data-reifenauftrag="{order["id"]}"',page.text)
        self.assertEqual(self.client.get('/werkstatt/tafel').status_code,200)

    def test_takeover_requires_owned_slot_and_cancel_protects_order(self):
        lead=self.book()
        db=portal.get_db();db.execute('DELETE FROM reifen_termin_slots WHERE lead_id=?',(lead['id'],));db.commit();db.close()
        with self.assertRaises(ValueError):
            portal.reifenauftrag_aus_buchung(lead['id'])
        self.client.post(f'/admin/reifenportal/{lead["id"]}/termin',data={
            portal.CSRF_FIELD_NAME:self.csrf,'datum':self.day.isoformat(),'uhrzeit':'10:00'})
        order_id=portal.reifenauftrag_aus_buchung(lead['id'])
        with self.assertRaises(ValueError):portal.delete_lead(lead['id'])
        with self.assertRaises(ValueError):portal.set_lead_status(lead['id'],'verloren')
        response=self.client.post(f'/admin/reifenportal/{lead["id"]}/termin',data={portal.CSRF_FIELD_NAME:self.csrf,'aktion':'stornieren'})
        self.assertEqual(response.status_code,302)
        self.assertTrue(portal.get_auftrag(order_id)['archiviert'])
        self.assertIn('10:00',portal.freie_reifen_slots(self.day.isoformat()))

    def test_office_reschedule_updates_hold_and_order_atomically(self):
        order,lead=self.order();self.book('10:00')
        url=f'/admin/reifenportal/{lead["id"]}/termin'
        self.client.post(url,data={portal.CSRF_FIELD_NAME:self.csrf,'datum':self.day.isoformat(),'uhrzeit':'10:00'})
        self.assertEqual(portal.get_auftrag(order['id'])['annahme_uhrzeit'],'09:00')
        self.client.post(url,data={portal.CSRF_FIELD_NAME:self.csrf,'datum':self.day.isoformat(),'uhrzeit':'11:00'})
        self.assertEqual(portal.get_auftrag(order['id'])['annahme_uhrzeit'],'11:00')
        self.assertIn('09:00',portal.freie_reifen_slots(self.day.isoformat()))
        self.assertNotIn('11:00',portal.freie_reifen_slots(self.day.isoformat()))
        self.client.post(f'/admin/auftrag/{order["id"]}',data={portal.CSRF_FIELD_NAME:self.csrf,'annahme_datum':self.day.isoformat(),'annahme_uhrzeit':'12:00'})
        self.assertEqual(portal.get_auftrag(order['id'])['annahme_uhrzeit'],'11:00')

    def test_actual_prices_save_export_and_draft_print(self):
        order,_=self.order()
        self.assertEqual(self.save(order).status_code,302)
        order=portal.get_auftrag(order['id'])
        calc=portal.reifen_rechnungsentwurf(order)['kalkulation']
        self.assertEqual(calc['gesamt'],{'brutto_cent':8000,'netto_cent':6723,'mwst_cent':1277})
        page=self.client.get(f'/admin/auftrag/{order["id"]}/reifen-rechnung')
        self.assertEqual(page.status_code,200)
        self.assertIn('80,00',page.text)
        xlsx=self.client.get(f'/admin/auftrag/{order["id"]}/reifen-rechnung/excel')
        self.assertEqual(xlsx.status_code,200)
        from openpyxl import load_workbook
        wb=load_workbook(BytesIO(xlsx.data))
        self.assertIn('ENTWURF', ' '.join(str(c.value or '') for row in wb.active for c in row))
        printed=self.client.get(f'/admin/auftrag/{order["id"]}/reifen-rechnung/druck')
        self.assertEqual(printed.status_code,200)
        self.assertIn('kein Rechnungsbeleg',printed.text)
        self.assertEqual(self.client.get(f'/admin/auftrag/{order["id"]}/rechnung').status_code,302)
        self.assertEqual(self.save(order,revision='stale').status_code,400)

    def test_discounted_units_reach_lexware_with_matching_vat_rounding(self):
        order,_=self.order()
        self.save(order,leistung=['montage_19_20'],menge_montage_19_20='3',lose_raeder='1')
        order=self.finished(portal.get_auftrag(order['id']))
        calc=portal.reifen_rechnungsentwurf(order)['kalkulation']
        self.assertEqual(calc['gesamt'],{'brutto_cent':7500,'netto_cent':6303,'mwst_cent':1197})
        with patch.object(portal,'LEXWARE_API_KEY','test-only'),patch.object(portal,'ensure_lexware_contact',return_value=('contact-test',False,False)),patch.object(portal,'lexware_request',return_value={'id':'invoice-test'}) as provider:
            self.invoice(order)
            items=provider.call_args.args[2]['lineItems']
            self.assertEqual(len(items),1)
            self.assertEqual(items[0]['unitPrice']['grossAmount'],25)
            self.assertEqual(items[0]['quantity'],3)

    def test_ocr_and_customer_delay_cannot_bypass_tire_slot(self):
        order,_=self.order()
        response=self.client.post(f'/admin/auftrag/{order["id"]}/auslese',data={portal.CSRF_FIELD_NAME:self.csrf,'feld':'annahme_datum'})
        self.assertEqual(response.status_code,302)
        self.assertIn('reifenportal',response.location)
        self.client.post('/status/'+order['kunden_status_token']+'/verzoegerung',data={portal.CSRF_FIELD_NAME:self.csrf,'termin_art':'annahme','neuer_termin':self.day.isoformat(),'meldung':'Test'})
        self.assertEqual(portal.get_auftrag(order['id'])['annahme_uhrzeit'],'09:00')

    def test_backup_contains_reservation_and_saved_service_snapshot(self):
        order,lead=self.order();self.save(order)
        saved=portal.get_auftrag(order['id'])['reifen_rechnung_json']
        archive=portal.create_backup_package('isolated-reifen-test')
        with zipfile.ZipFile(archive) as z:
            export=json.loads(z.read('backup.json'))
        rows=export['tables']['auftraege']
        self.assertEqual(next(r for r in rows if r['id']==order['id'])['reifen_rechnung_json'],saved)
        self.assertEqual(next(r for r in export['tables']['reifen_termin_slots'] if r['lead_id']==lead['id'])['uhrzeit'],'09:00')

    def test_failed_takeover_rolls_back_new_order(self):
        lead=self.book()
        real=portal.create_auftrag
        def fail_after_insert(*a,**kw):
            real(*a,**kw)
            raise RuntimeError('isolated transaction failure')
        db=portal.get_db();before=db.execute('SELECT COUNT(*) FROM auftraege').fetchone()[0];db.close()
        with patch.object(portal,'create_auftrag',side_effect=fail_after_insert):
            with self.assertRaises(RuntimeError):portal.reifenauftrag_aus_buchung(lead['id'])
        db=portal.get_db();after=db.execute('SELECT COUNT(*) FROM auftraege').fetchone()[0];db.close()
        self.assertEqual(before,after)
        self.assertFalse(portal.get_lead(lead['id'])['auftrag_id'])

    def test_invalid_service_combinations_and_missing_address_block_invoice(self):
        order,_=self.order()
        self.assertEqual(self.save(order,leistung=['saison_radwechsel_bis17','einlagerung']).status_code,400)
        self.assertEqual(self.save(order,rechnung_strasse='').status_code,302)
        order=self.finished(portal.get_auftrag(order['id']))
        with patch.object(portal,'LEXWARE_API_KEY','test-only'),patch.object(portal,'create_lexware_invoice_draft') as provider:
            self.invoice(order);provider.assert_not_called()

    def test_final_service_selection_reaches_lexware_once_on_gross_basis(self):
        order,_=self.order();self.save(order);order=self.finished(portal.get_auftrag(order['id']))
        with patch.object(portal,'LEXWARE_API_KEY','test-only'),patch.object(portal,'ensure_lexware_contact',return_value=('contact-test',False,False)),patch.object(portal,'lexware_request',return_value={'id':'invoice-test'}) as provider:
            first=self.invoice(order)
            self.assertEqual(first.status_code,302)
            self.assertEqual(provider.call_count,1)
            payload=provider.call_args.args[2]
            self.assertEqual(payload['taxConditions'],{'taxType':'gross'})
            self.assertEqual([p['unitPrice']['grossAmount'] for p in payload['lineItems']],[35,45])
            self.assertEqual(payload['address']['street'],'Teststraße 1')
            self.assertEqual(payload['shippingConditions']['shippingType'],'service')
            self.invoice(portal.get_auftrag(order['id']))
            self.assertEqual(provider.call_count,1)
            self.assertEqual(self.save(portal.get_auftrag(order['id'])).status_code,400)

    def test_uncertain_lexware_result_stays_locked_and_auth_is_required(self):
        order,_=self.order();self.save(order);order=self.finished(portal.get_auftrag(order['id']))
        with patch.object(portal,'LEXWARE_API_KEY','test-only'),patch.object(portal,'create_lexware_invoice_draft',side_effect=RuntimeError('timeout')) as provider:
            self.invoice(order);self.invoice(portal.get_auftrag(order['id']))
            self.assertEqual(provider.call_count,1)
        self.assertEqual(portal.get_auftrag(order['id'])['rechnung_erstellung_status'],'pruefen')
        anon=portal.app.test_client()
        for suffix in ('','/excel','/druck'):
            self.assertEqual(anon.get(f'/admin/auftrag/{order["id"]}/reifen-rechnung'+suffix).status_code,302)


if __name__=='__main__':unittest.main(verbosity=2)
