"""Explicit AB-to-order links on synthetic canonical sources, without transport."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from unittest.mock import patch
import unittest
from flask import Blueprint, abort, request, session
from jinja2 import FileSystemLoader

import test_bestellvergleich_external_batch as frozen
from werkstatt_artikel_import import InvoiceCatalog
from werkstatt_bestellbestaetigung import get_order_confirmations,TABLE
from werkstatt_einkaufseingang import IntakeConflict
from werkstatt_bestellvergleich_ui import register_comparison_forms,comparison_context


class ConfirmationFixture:
    def setUp(self):
        self.base = frozen.RecordedBatchComparisonTests('runTest')
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.p,self.comparison = self.base.p,self.base.s
        self.sql = self.base.base.sql
        self.p.UPLOAD_DIR = Path(self.base.f.temporary.name)/'ab-originals'
        self.p.UPLOAD_DIR.mkdir()
        self.original = self.p.UPLOAD_DIR/'synthetic-ab.pdf'
        self.original.write_bytes(b'%PDF synthetic AB original, no business data')
        self.sha = hashlib.sha256(self.original.read_bytes()).hexdigest()
        self.p.get_app_setting = lambda key,default='':json.dumps(['Testlieferant'])
        self.sql('CREATE TABLE einkauf_belege(id INTEGER PRIMARY KEY,beleg_typ TEXT,lieferant TEXT,original_name TEXT,stored_name TEXT,status TEXT)')
        self.sql("INSERT INTO einkauf_belege VALUES(99,'auftragsbestaetigung','Testlieferant','SyntheticAB.pdf','synthetic-ab.pdf','importiert')")
        self.catalog = self.p.cockpit_data.catalog = InvoiceCatalog(self.p)
        self.sql("INSERT INTO assistent_rechnungsimporte(id,source_key,source_kind,source_id,supplier,reference,state) VALUES(5,'einkauf:99','einkauf','99','Testlieferant','SyntheticAB.pdf','ausgelesen')")
        self.add_proposal(1,position=2)
        self.links = get_order_confirmations(self.p,self.comparison)

    def add_proposal(self,ident,*,position=2,quantity='2',total='25',sku='TEST-50',name='Testband 50 mm',formula=True):
        candidate = dict(produkt_name=name,artikelnummer=sku,ve='Karton',gebinde='6 Rollen',groesse='50 mm',preis='12.50',
            source=dict(page=1,position=position,date='2026-10-06',document_type='auftragsbestaetigung',sha256=self.sha,file_id='einkauf-99'),
            price_evidence=dict(document_type='auftragsbestaetigung',basis='auftragsbestaetigung_netto_positionswert',
                quoted_line_total_net=total,quoted_unit_net=str(Decimal(total)/Decimal(quantity)),
                value='12.50',quantity=quantity,content='1',measure_unit='Pack',base_price_per_measure='12.50',discount_percent='0',
                reconciled=formula,quote_formula_reconciled=formula,currency='EUR',tax_basis='net',tax_rate='19',
                metadata_source=dict(page=2,method='topcolor_summary_columns_v1',tax_summary_reconciled=True)))
        source = dict(source_key='einkauf:99',source_kind='einkauf',source_id='99',supplier='Testlieferant',reference='SyntheticAB.pdf',id=5)
        record,_ = InvoiceCatalog._candidate_record(source,candidate,position,datetime.now(timezone.utc))
        self.sql('INSERT INTO assistent_rechnungsartikel(id,fingerprint,identity_key,import_id,supplier,product_name,article_number,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)',(ident,*record))

    def assignment(self,ident=1,key='material:1',reason=''):
        order = self.comparison.detail(key)['order']
        quote = self.links._proposals()[ident]
        return dict(order_key=key,order_fingerprint=order['fingerprint'],proposal_id=ident,
                    proposal_fingerprint=quote['proposal_fingerprint'],review_reason=reason,reviewed=True)

class ConfirmationLinkTests(ConfirmationFixture,unittest.TestCase):
    def test_quote_is_separate_frozen_dated_and_idempotent_without_invoice_or_approval(self):
        before = self.sql('SELECT * FROM einkauf_material_dialoge')
        item = self.assignment(reason='Am AB-Original zugeordnet')
        self.links.record_batch([item])
        after = self.sql('SELECT * FROM '+TABLE)
        self.links.record_batch([item])
        self.assertEqual(self.sql('SELECT * FROM '+TABLE),after)
        view = self.comparison.detail('material:1')
        quote = view['confirmation']
        self.assertTrue(quote['available'])
        self.assertFalse(quote['review_required'])
        self.assertEqual((quote['amount'],quote['unit'],quote['packaging']),('12.50','Karton','6 Rollen'))
        self.assertEqual(quote['date_display'],'06.10.2026')
        self.assertFalse(quote['can_dispatch']);self.assertFalse(quote['price_approved'])
        self.assertEqual(quote['original_order']['quantity'],'2')
        self.assertIsNone(view['estimate']);self.assertEqual(view['invoices'],[])
        self.assertEqual(self.sql('SELECT * FROM einkauf_material_dialoge'),before)
        self.assertEqual(self.sql('SELECT * FROM assistent_bestellanforderungen'),[])

    def test_replacement_and_carton_disagreement_need_reason_and_remain_explicitly_different(self):
        self.add_proposal(2,position=3,sku='OTHER',name='Other-brand LiftTape 10 m')
        item = self.assignment(2)
        with self.assertRaises(ValueError):self.links.record_batch([item])
        self.assertEqual(self.sql('SELECT * FROM '+TABLE),[])
        item['review_reason']='AB nennt anderen Artikel; Ersatz noch nicht freigegeben'
        self.links.record_batch([item])
        quote = self.links.summaries(['material:1'])['material:1']
        self.assertTrue(quote['review_required'])
        self.assertEqual(quote['sku'],'OTHER')
        self.assertEqual(quote['original_order']['sku'],'TEST-50')
        self.assertEqual(quote['amount'],'12.50')
        self.assertFalse(quote['price_approved'])

    def test_formula_discrepancy_preserves_printed_quote_and_separate_unit_basis(self):
        self.add_proposal(2,position=3,quantity='3',total='54.22',formula=False)
        item = self.assignment(2,reason='Formelsumme und abweichende Menge noch prüfen')
        self.links.record_batch([item])
        quote = self.links.summaries(['material:1'])['material:1']
        self.assertEqual((quote['amount'],quote['line_amount']),('18.07','54.22'))
        self.assertTrue(quote['formula_warning']);self.assertTrue(quote['review_required'])
        self.assertEqual(quote['original_order']['quantity'],'2')
        self.assertEqual(quote['quantity'],'3')

    def test_stale_order_candidate_or_duplicate_line_leaves_atomic_batch_unchanged(self):
        item = self.assignment()
        for change in ({'order_fingerprint':'bad'},{'proposal_fingerprint':'bad'},{'reviewed':False}):
            with self.subTest(change=change),self.assertRaises(ValueError):
                self.links.record_batch([dict(item,**change)])
        with self.assertRaises(ValueError):self.links.record_batch([item,item])
        self.assertEqual(self.sql('SELECT * FROM '+TABLE),[])
        self.links.record_batch([item])
        changed = dict(item,review_reason='Different assignment')
        with self.assertRaises(IntakeConflict):self.links.record_batch([changed])
        self.assertEqual(len(self.sql('SELECT * FROM '+TABLE)),1)

    def test_source_revocation_tamper_and_original_replacement_hide_without_deleting(self):
        self.links.record_batch([self.assignment()])
        rows = self.sql('SELECT * FROM '+TABLE)
        for change in ("UPDATE assistent_rechnungsartikel SET active=0", "UPDATE einkauf_belege SET beleg_typ='gesperrt'", "UPDATE einkauf_belege SET status='quarantine'", "UPDATE assistent_rechnungsimporte SET state='offen'"):
            with self.subTest(change=change):
                self.sql(change)
                quote = self.links.summaries(['material:1'])['material:1']
                self.assertFalse(quote['available'])
                self.sql("UPDATE assistent_rechnungsartikel SET active=1")
                self.sql("UPDATE einkauf_belege SET beleg_typ='auftragsbestaetigung',status='importiert'")
                self.sql("UPDATE assistent_rechnungsimporte SET state='ausgelesen'")
        self.original.write_bytes(b'%PDF different AB original')
        self.assertFalse(self.links.summaries(['material:1'])['material:1']['available'])
        self.assertEqual(self.sql('SELECT * FROM '+TABLE),rows)

    def test_atomic_rollback_on_source_change_before_final_publication(self):
        item = self.assignment()
        real = self.links._fresh_proposals
        calls=[]
        def changed():
            calls.append(True)
            return {} if len(calls)==3 else real()
        with patch.object(self.links,'_fresh_proposals',side_effect=changed),self.assertRaises(IntakeConflict):
            self.links.record_batch([item])
        self.assertEqual(self.sql('SELECT * FROM '+TABLE),[])

    def test_admin_supplier_quoted_tax_and_known_document_only(self):
        with self.assertRaises(PermissionError):self.links.record_batch([self.assignment()],actor='mitarbeiter:1')
        self.sql("UPDATE einkauf_belege SET lieferant='Volksbank'")
        self.assertEqual(self.links._proposals(),{})
        self.sql("UPDATE einkauf_belege SET lieferant='Testlieferant',beleg_typ='rechnung'")
        self.assertEqual(self.links._proposals(),{})

    def test_request_snapshot_is_single_catalog_read_and_alias_binding_uses_original_source(self):
        with patch.object(self.catalog,'knowledge_rows',wraps=self.catalog.knowledge_rows) as read:
            with self.links.request_snapshot():
                form=self.links.batch_form([{'source':'material:1'}])
                self.links.summaries(['material:1'])
                self.comparison.detail('material:1')
            self.assertEqual(read.call_count,1)
        self.assertEqual(form['orders'][0]['source'],'material:1')
        self.assertEqual(form['orders'][0]['proposals'][0]['id'],1)

    def test_manufacturer_code_match_requires_delimited_token(self):
        order={'sku':'T400','unit':'Karton','quantity':'2','product':'Test','variant':''}
        quote=self.links._proposals()[1]
        quote=dict(quote,product='PPG T4000/E0.5',sku='10004410')
        self.assertTrue(self.links._review(order,quote,''))

    def test_berlin_order_day_not_utc_or_recording_day(self):
        self.base.claim.update(sent_at='2026-10-05T22:01:00+00:00',recorded_at='2026-10-06T10:00:00+00:00')
        self.base.save()
        self.links.record_batch([self.assignment()])
        self.assertTrue(self.links.summaries(['material:1'])['material:1']['available'])

    def test_dispatch_alias_bindings_preserve_the_overview_source_key(self):
        self.base.base.regular(ident='regular',state='sent')
        batch=self.links.batch_form([{'source':'dispatch:regular'}])
        self.assertEqual(batch['orders'][0]['source'],'dispatch:regular')
        self.assertEqual(batch['orders'][0]['order_key'],'order:regular')

    def test_optional_catalog_failure_hides_quote_and_recovers_next_request_without_private_error(self):
        self.links.record_batch([self.assignment()])
        with patch.object(self.catalog,'knowledge_rows',side_effect=RuntimeError('PRIVATE_DATABASE_DETAIL')):
            with self.assertLogs('werkstatt_bestellbestaetigung',level='WARNING') as logs:
                with self.links.request_snapshot():
                    self.assertFalse(self.comparison.detail('material:1')['confirmation']['available'])
            self.assertNotIn('PRIVATE_DATABASE_DETAIL',' '.join(logs.output))
        with self.links.request_snapshot():
            self.assertTrue(self.comparison.detail('material:1')['confirmation']['available'])


class ConfirmationRouteTests(ConfirmationFixture,unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.app=self.p.app
        self.app.testing=True
        bp=Blueprint('werkstatt_orders',__name__,url_prefix='/admin/assistent-bestellungen')
        @bp.before_request
        def admin_csrf():
            if not session.get('admin'):abort(403)
            if request.form.get('csrf_token') != session.get('csrf_token'):abort(400)
        bp.add_url_rule('',endpoint='index',view_func=lambda:'ok')
        register_comparison_forms(bp,self.p)
        self.app.register_blueprint(bp)
        self.client=self.app.test_client()
        with self.client.session_transaction() as state:state.update(admin=True,csrf_token='confirmation-test')

    def test_normal_batch_form_is_admin_csrf_bound_blank_skip_and_archive_preserving(self):
        item=self.assignment()
        form={'csrf_token':'confirmation-test','reviewed':'ja','confirmation_order':['material:1','material:22'],
              'order_fingerprint_material:1':item['order_fingerprint'],
              'proposal_ref_material:1':f"1:{item['proposal_fingerprint']}",'proposal_ref_material:22':'',
              'review_reason_material:1':'','archive_day':'sent:2026-10-06',
              'amount':'0.01','actor':'mitarbeiter:999'}
        url='/admin/assistent-bestellungen/preisvergleich/bestaetigung'
        self.assertEqual(self.client.post(url,data=dict(form,csrf_token='wrong')).status_code,400)
        self.assertEqual(self.sql('SELECT * FROM '+TABLE),[])
        response=self.client.post(url,data=form)
        self.assertEqual(response.status_code,303)
        self.assertIn('archive_day=sent:2026-10-06',response.location)
        self.assertEqual(self.links.summaries(['material:1'])['material:1']['amount'],'12.50')
        with self.client.session_transaction() as state:state.pop('admin')
        self.assertEqual(self.client.post(url,data=form).status_code,403)

    def test_duplicate_normal_form_and_failed_batch_do_not_create_partial_link(self):
        item=self.assignment()
        form={'csrf_token':'confirmation-test','reviewed':'ja','confirmation_order':['material:1','material:1'],
              'order_fingerprint_material:1':item['order_fingerprint'],'proposal_ref_material:1':f"1:{item['proposal_fingerprint']}"}
        self.assertEqual(self.client.post('/admin/assistent-bestellungen/preisvergleich/bestaetigung',data=form).status_code,303)
        self.assertEqual(self.sql('SELECT * FROM '+TABLE),[])

    def test_saved_quote_renders_actual_overview_price_cell_and_normal_form(self):
        self.links.record_batch([self.assignment()])
        entry=dict(id='material:1',source='material:1',draft=False,product='Original-Testband',person='Synthetic Employee',
                   quantity='2',unit='Karton',supplier='Testlieferant',cap='250,00 €',editable=False,restorable=False,
                   state='external_sent',state_label='Bestellt',warnings=[],overdue=False)
        self.app.jinja_loader=FileSystemLoader(str(Path(__file__).resolve().parents[1]/'templates'))
        source=(Path(__file__).resolve().parents[1]/'templates/assistent_bestellungen.html').read_text(encoding='utf-8')
        url_macro=source[source.index('{% macro overview_url'):source.index('{%- endmacro %}')+len('{%- endmacro %}')]
        table_start=source.index('{% macro order_table')
        table_macro=source[table_start:source.index('{% endmacro %}',table_start)+len('{% endmacro %}')]
        context=dict(overview=dict(page=1,draft_page=1,open=dict(page=1),removed=dict(page=1),archive_blocks_page=1,filters={}),
                     entries=[entry],confirmation_entries=[entry],csrf='confirmation-test',group=dict(key='sent:2026-10-06'),
                     confirmation_summaries=self.links.summaries(['material:1']),
                     confirmation_orders={'material:1':self.links.batch_form(['material:1'])['orders'][0]},
                     confirmation_candidates=list(self.links._proposals().values()))
        with self.app.test_request_context('/admin/assistent-bestellungen'):
            html=self.app.jinja_env.from_string(url_macro+table_macro+'{{order_table(entries)}}{% include "_bestellbestaetigung.html" %}').render(**context)
        self.assertIn('12,50',html)
        self.assertIn('netto',html)
        self.assertIn('06.10.2026',html)
        self.assertIn('Original-Testband',html)
        self.assertIn('SyntheticAB.pdf',html)
        self.assertIn('proposal_ref_material:1',html)
        self.assertIn('value="sent:2026-10-06"',html)


if __name__ == '__main__':
    unittest.main()
