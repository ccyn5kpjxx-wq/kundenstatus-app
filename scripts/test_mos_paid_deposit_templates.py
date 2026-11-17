"""Charged long-rental deposits stay distinct from short-rental card holds."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest

from jinja2 import Environment, FileSystemLoader

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from mos_order_receipt import _message as order_receipt_message
from werkstatt_kontakt import public_workshop_contact


class PaidDepositTemplateTests(unittest.TestCase):
    def setUp(self):
        self.env=Environment(loader=FileSystemLoader(ROOT/'templates'),autoescape=True)
        self.env.filters['mos_slot']=lambda value:value
        self.env.globals.update(url_for=lambda endpoint,**values:'/'+endpoint+
            ('/'+values['filename'] if 'filename' in values else ''),
            csrf_field=lambda:'<input name="csrf_token" value="synthetic-csrf">')
        self.cfg={'enabled':True,'max_days':30,'deposit_method':'card_authorization_at_booking',
                  'deposit_timing':'before_pickup','privacy_url':'/privacy',
                  'long_rental_deposit_method':'charge_with_rent_refund_after_return'}
        self.quote={'vehicle_name':'Hyundai i10','portal_vehicle_name':'Synthetic i10','slug':'i10',
                    'vehicle_id':1,'vehicle_plate':'TEST-1','vehicle_vin':'',
                    'start_slot':'2027-09-20T14:00:00+02:00','end_slot':'2027-09-30T14:00:00+02:00',
                    'days':10,'daily_cents':3900,'rental_cents':39000,'amount_cents':89000,
                    'deposit_cents':50000,'deposit_charged_cents':50000,'deposit_authorized_cents':0,
                    'deposit_method':'charge_with_rent_refund_after_return',
                    'long_rental_deposit_policy':'paid_at_booking_v1','checkout_deposit':True,
                    'deductible_cents':100000,'included_km':1500,'extra_km_cents':25,
                    'rules_version':'SYNTHETIC-PAID-DEPOSIT','terms_text':'SYNTHETIC TERMS ONLY',
                    'lessor_name':'Synthetic Lessor','lessor_address':'Synthetic Address',
                    'lessor_email':'test@example.invalid','lessor_phone':'+491234567890',
                    'test_only':False,'cancellation_policy':'free_48h_then_10pct_rent'}
        self.hold={'id':'synthetic-hold','status':'confirmed','session_id':'cs_live_synthetic',
                   'mietvorgang_id':1,'grund':''}
        self.account={'cancellation':None,'refunds':[]}

    def render(self,name,**values):
        defaults={'live':True,'config':{},'booking_config':self.cfg,
                  'werkstatt_oeffentlich':public_workshop_contact(),
                  'q':self.quote,'h':self.hold,'account':self.account,
                  'contract':None,'signed':None,'presigned':None,'deposit':None,
                  'prefill':{},'token':'synthetic-token','error':None,
                  'listings':{'kona':'Hyundai KONA N Line X','i10':'Hyundai i10'}}
        if name in ('index.html','admin.html'):defaults.pop('q')
        defaults.update(values)
        return self.env.get_template('mos_public/'+name).render(**defaults)

    def contract(self):
        return {**self.quote,'customer_name':'Synthetic Customer',
                'customer_email':'customer@example.invalid','terms_version':self.quote['rules_version']}

    def test_selection_explains_short_hold_and_long_payment_before_price_button(self):
        slots=[self.quote['start_slot'],'2027-09-21T14:00:00+02:00',self.quote['end_slot']]
        text=self.render('index.html',slots=slots,default_end=slots[1],weekly_schedule=True,
                         max_days=30,selection={'vehicle':'i10'})
        self.assertIn('Bis fünf berechnete Miettage:',text)
        self.assertIn('Sechs bis 30 berechnete Miettage:',text)
        self.assertIn('500 € Kaution bei der Buchung zusammen mit der Miete bezahlen',text)
        self.assertIn('frühestens zwölf Stunden vor der Abholung',text)
        self.assertIn('innerhalb von zwei Werktagen',text)
        self.assertLess(text.index('So funktioniert die Kaution'),text.index('<button>'))
        self.assertNotIn('28 Miettage',text)

    def test_long_quote_separates_rental_deposit_total_and_names_the_payment_button(self):
        text=self.render('quote.html')
        self.assertIn('890,00 €',text)
        self.assertIn('390,00 € Miete inkl. MwSt. + 500,00 € rückzahlbare Kaution',text)
        self.assertIn('Weiter zur Zahlung von Miete und Kaution',text)
        self.assertIn('Miete und Kaution bezahlen',text)
        self.assertIn('innerhalb von zwei Werktagen',text)
        self.assertNotIn('Kaution vor Abholung online reservieren',text)
        self.assertNotIn('Weiter zur Kautionsreservierung und Onlinezahlung',text)

    def test_short_quote_keeps_the_later_card_hold_with_only_rent_payable(self):
        quote=deepcopy(self.quote)
        quote.pop('long_rental_deposit_policy')
        quote.update(days=3,rental_cents=11700,amount_cents=11700,deposit_charged_cents=0,
                     deposit_authorized_cents=50000,deposit_method='card_authorization_at_booking',
                     deposit_timing='before_pickup',checkout_deposit=False)
        text=self.render('quote.html',q=quote)
        self.assertIn('Zahlbetrag: nur die Miete',text)
        self.assertIn('Weiter zur Mietpreiszahlung',text)
        self.assertIn('frühestens zwölf Stunden vor der Abholung',text)
        self.assertNotIn('Weiter zur Zahlung von Miete und Kaution',text)
        self.assertNotIn('Bei dieser längeren Miete',text)

    def test_historical_charged_quote_without_new_flag_keeps_its_original_explanation(self):
        quote=deepcopy(self.quote)
        quote.pop('long_rental_deposit_policy')
        quote.update(days=2,rental_cents=7800,amount_cents=57800)
        text=self.render('quote.html',q=quote)
        self.assertIn('nach Rückgabe entsprechend den vereinbarten Bedingungen zurückerstattet',text)
        self.assertNotIn('Bei dieser längeren Miete',text)
        self.assertNotIn('innerhalb von zwei Werktagen',text)

    def test_paid_long_status_shows_total_split_without_any_card_hold_action(self):
        text=self.render('status.html')
        self.assertIn('Bezahlter Gesamtbetrag: 390,00 € Miete inkl. MwSt. + 500,00 € rückzahlbare Kaution',text)
        self.assertIn('500,00 € Kaution hast du zusammen mit dem Mietpreis bezahlt',text)
        self.assertNotIn('500 € Kaution online reservieren</a>',text)
        self.assertNotIn('Kaution vor Abholung online reservieren',text)

    def test_refund_status_distinguishes_queued_submitted_succeeded_and_failed(self):
        labels={'queued':'Kautionserstattung vorgemerkt:',
                'submitted':'Kautionserstattung in Bearbeitung:',
                'succeeded':'Kautionserstattung bestätigt:',
                'failed':'Kautionserstattung fehlgeschlagen:'}
        for status,label in labels.items():
            with self.subTest(status=status):
                account={'cancellation':None,'refunds':[{'kind':'deposit_return','amount_cents':50000,'status':status}]}
                text=self.render('status.html',account=account,rental_returned=True)
                self.assertIn(label,text)
                for other,other_label in labels.items():
                    if other!=status:self.assertNotIn(other_label,text)
                if status!='succeeded':self.assertNotIn('hat die Erstattung von 500,00 € bestätigt',text)
                self.assertNotIn('Erstattung 500,00 €:',text)  # No duplicate generic entry.

    def test_contract_distinguishes_proposed_payment_from_paid_total(self):
        contract=self.contract()
        for status,word in [('pending','zu bezahlen'),('confirmed','bezahlt')]:
            with self.subTest(status=status):
                text=self.render('contract.html',contract=contract,h={**self.hold,'status':status},signed_local='synthetic')
                self.assertIn('bei Buchung '+word,text)
                self.assertIn('390,00 €',text)
                self.assertIn('500,00 €',text)
                self.assertIn('890,00 €',text)
                self.assertIn('innerhalb von zwei Werktagen',text)

    def test_order_receipt_labels_total_payment_and_gives_split_for_new_policy(self):
        row={'hold_id':self.hold['id'],'recipient':'customer@example.invalid',
             'amount_cents':89000,'payload':json.dumps({'quote':self.quote})}
        body=order_receipt_message(row).get_content()
        self.assertIn('Zahlbetrag laut Bestellung: 890,00 EUR',body)
        self.assertIn('Mietpreis inkl. MwSt.: 390,00 EUR',body)
        self.assertIn('Rückzahlbare Kaution: 500,00 EUR',body)
        self.assertNotIn('Mietpreis laut Bestellung: 890,00 EUR',body)
        self.assertIn('keine zusätzliche Annahme',body)

    def test_legacy_receipt_still_works_without_new_policy_or_payload(self):
        row={'hold_id':'synthetic-old','recipient':'customer@example.invalid','amount_cents':3900}
        body=order_receipt_message(row).get_content()
        self.assertIn('Zahlbetrag laut Bestellung: 39,00 EUR',body)
        self.assertNotIn('Rückzahlbare Kaution:',body)

    def test_offline_simulator_discloses_the_synthetic_deposit_in_the_total(self):
        text=self.render('simulate.html',live=False)
        self.assertIn('390,00 € Miete + 500,00 € rückzahlbare Kaution',text)
        self.assertIn('ebenfalls nur simuliert',text)
        self.assertNotIn('Kaution 500 € separat, hier nicht eingezogen',text)

    def staff_hold(self):
        return {**self.hold,'q':deepcopy(self.quote),'account':deepcopy(self.account),
                'signed_at':'synthetic-signature-time','order_receipt_status':'sent',
                'order_receipt_delay_seconds':None,'contract_delivery_status':'sent',
                'contract_delivery_delay_seconds':None,'handed_at':None,
                'deposit_status':None,'rental_status':'aktiv'}

    def test_staff_can_request_fresh_payment_and_handover_check_for_new_paid_policy(self):
        text=self.render('admin.html',holds=[self.staff_hold()],open_slots=[],closed_slots=[],
                         request_id='synthetic',weekly_schedule=False)
        self.assertIn('Mietzahlung und Kaution prüfen, dann Schlüssel übergeben',text)
        self.assertIn('action="/mos_public.admin_handover"',text)
        self.assertIn('Zahlung des Mietpreises einschließlich der 500 € Kaution frisch beim Zahlungsanbieter',text)
        self.assertIn('890,00 € insgesamt: 390,00 € Miete und 500,00 €',text)

    def test_staff_handover_does_not_treat_a_missing_flag_or_wrong_method_as_paid_policy(self):
        for changed in ('missing_flag','wrong_method'):
            with self.subTest(changed=changed):
                hold=self.staff_hold()
                if changed=='missing_flag':hold['q'].pop('long_rental_deposit_policy')
                else:hold['q']['deposit_method']='card_authorization_at_booking'
                text=self.render('admin.html',holds=[hold],open_slots=[],closed_slots=[],
                                 request_id='synthetic',weekly_schedule=False)
                self.assertNotIn('action="/mos_public.admin_handover"',text)

    def test_paid_unconfirmed_staff_review_refunds_both_components(self):
        hold=self.staff_hold()
        hold.update(status='review',mietvorgang_id=None)
        text=self.render('admin.html',holds=[hold],open_slots=[],closed_slots=[],
                         request_id='synthetic',weekly_schedule=False)
        self.assertIn('Die Gesamtzahlung aus Mietpreis und 500 € Kaution wird vollständig erstattet.',text)
        self.assertIn('Ich beauftrage die vollständige Erstattung von Mietpreis und Kaution dieses Prüffalls.',text)
        self.assertIn('value="review_full_refund"',text)


if __name__=='__main__':unittest.main()
