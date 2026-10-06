"""Admin price forms, provenance and unchanged dispatch evidence on a temporary DB."""
from contextlib import nullcontext, contextmanager
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_bestelluebersicht import OverviewTests
from test_einkaufseingang import png
from werkstatt_einkaufseingang import register_intake
from werkstatt_bestellvergleich import OrderPriceComparison
from werkstatt_rechnungsfreigabe import classify_invoice_source

class PriceUITests(unittest.TestCase):
    def setUp(self):
        self.f=OverviewTests(); self.f.setUp(); self.addCleanup(self.f.doCleanups)
        self.f.material(supplier_name='Testlieferant',variant='50 mm')
        self.p=self.f.p
        self.p.portal_originals_operation_lock=nullcontext
        self.p.workshop_intake=register_intake(self.p)
        self.p.cockpit_data=SimpleNamespace(catalog=SimpleNamespace(
            knowledge_rows=lambda **kw: {'items':[]},
            source_rule=lambda source: classify_invoice_source(source,['Testlieferant'])))
        self.s=self.p.order_price_comparison=OrderPriceComparison(self.p)
        self.client=self.f.client
        with self.client.session_transaction() as sess: sess['csrf_token']='price-test'
        self.before=self.rows('SELECT * FROM einkauf_material_dialoge')
    def rows(self,sql):
        with self.s.db() as db: return [dict(row) for row in db.execute(sql)]
    def post(self,action,**values):
        return self.client.post('/admin/assistent-bestellungen/preisvergleich/'+action,
            data={'csrf_token':'price-test','order_key':'material:1',**values})
    def upload(self):
        return self.post('beleg',file=(BytesIO(png()),'Rechnung.png'))
    def form(self,**changes):
        return dict(original='1:1',source_date='2026-09-28',page='1',position='1',amount='10,00',
            tax_basis='net',tax_rate='19',discount_basis='nach Rabatt, Versand separat',
            unit='Stück',pack='50 mm',supplier='Testlieferant',sku='TEST-4000',variant='50 mm',reviewed='ja',**changes)
    def test_readonly_overview_includes_external_shipment_and_comparison(self):
        response=self.client.get('/admin/assistent-bestellungen?bestellung=material:1')
        self.assertEqual(response.status_code,200)
        html=response.get_data(as_text=True)
        for text in ('Bestellungen','Testperson A','Extern versandt','Preisvergleich zur Bestellung',
                     'Rechnung hochladen & zuordnen'):
            self.assertIn(text,html)
        self.assertEqual(self.rows('SELECT * FROM einkauf_material_dialoge'),self.before)
    def test_admin_csrf_and_no_price_write_from_invalid_requests(self):
        anonymous=self.p.app.test_client()
        self.assertEqual(anonymous.post('/admin/assistent-bestellungen/preisvergleich/beleg').status_code,403)
        employee=self.p.app.test_client()
        with employee.session_transaction() as sess: sess['assistent_mid']=1
        self.assertEqual(employee.post('/admin/assistent-bestellungen/preisvergleich/beleg').status_code,403)
        self.assertEqual(self.client.post('/admin/assistent-bestellungen/preisvergleich/beleg',data={'order_key':'material:1'}).status_code,400)
        self.assertEqual(self.rows('SELECT * FROM assistent_bestellpreis_basis'),[])
    def test_upload_estimate_invoice_flow_is_deduplicated_and_never_dispatches(self):
        with patch.object(self.f.manager,'tick',side_effect=AssertionError('no dispatch')):
            self.assertEqual(self.upload().status_code,303)
            self.assertEqual(self.upload().status_code,303)
            self.assertEqual(len(self.rows('SELECT * FROM einkauf_eingang_dateien')),1)
            self.assertEqual(self.post('schaetzung',**self.form()).status_code,303)
            self.assertEqual(self.post('schaetzung',**self.form()).status_code,303)
            self.assertEqual(len(self.rows('SELECT * FROM assistent_bestellpreis_basis')),1)
            view=self.client.get('/admin/assistent-bestellungen?bestellung=material:1').get_data(as_text=True)
            self.assertIn('Wartet auf Rechnung',view)
            self.assertIn('Preisnachtrag',view)
            form=self.form();form.update(source_date='2026-09-30',position='2',amount='12,00',quantity='1')
            self.post('rechnung',**form)
            view=self.client.get('/admin/assistent-bestellungen?bestellung=material:1').get_data(as_text=True)
            self.assertIn('Preisabweichung',view)
            self.assertIn('20,00 %',view)
            self.assertEqual(self.rows('SELECT * FROM einkauf_material_dialoge'),self.before)
            self.assertEqual(self.rows('SELECT * FROM assistent_bestellanforderungen'),[])
    def test_mismatched_invoice_is_not_saved_and_error_remains_visible(self):
        self.upload(); form=self.form(); form['variant']='30 mm'
        response=self.post('schaetzung',**form)
        self.assertEqual(response.status_code,303)
        html=self.client.get(response.location).get_data(as_text=True)
        self.assertIn('passt nicht',html)
        self.assertEqual(self.rows('SELECT * FROM assistent_bestellpreis_basis'),[])
    def test_open_send_states_remain_in_review_counter(self):
        self.f.material(2,state='external_pending')
        self.f.material(3,state='accepted')
        self.f.batch('partial-batch',state='ready',outbox='partial')
        self.f.order('partial-order',batch='partial-batch')
        html=self.client.get('/admin/assistent-bestellungen').get_data(as_text=True)
        self.assertIn('<span>Zu prüfen</span><strong>3</strong>',html)

    def test_receipt_upload_holds_restore_lock_through_attachment(self):
        depth=[0]
        @contextmanager
        def lock():
            depth[0]+=1
            try: yield
            finally: depth[0]-=1
        attach=self.p.workshop_intake.attach
        def locked_attach(*args,**kwargs):
            self.assertEqual(depth[0],1)
            return attach(*args,**kwargs)
        self.p.portal_originals_operation_lock=lock
        with patch.object(self.p.workshop_intake,'attach',side_effect=locked_attach) as spy:
            self.upload()
            spy.assert_called_once()
        self.assertEqual(depth[0],0)

    def test_receipt_supplier_is_derived_from_order_not_form(self):
        self.post('beleg',supplier='Anderer Lieferant',file=(BytesIO(png()),'Beleg.png'))
        groups=self.rows('SELECT supplier FROM einkauf_eingang')
        self.assertEqual(groups,[{'supplier':'Testlieferant'}])
        self.assertEqual(self.rows('SELECT * FROM einkauf_material_dialoge'),self.before)

if __name__=='__main__': unittest.main()
