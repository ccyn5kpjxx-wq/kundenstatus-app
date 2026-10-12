"""Frozen WhatsApp batches remain valid price/delivery identities, never prices."""
from copy import deepcopy
import json
import unittest
from unittest.mock import patch

import test_bestellvergleich as fixtures
from werkstatt_bestellvergleich import OrderPriceComparison
from werkstatt_einkaufseingang import _hash, _json
from werkstatt_liefereingang import OrderDelivery, receipt_payload


class RecordedBatchComparisonTests(unittest.TestCase):
    def setUp(self):
        self.base = fixtures.OrderPriceTests('runTest')
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.s,self.p,self.f = self.base.s,self.base.p,self.base.f
        self.p.order_price_comparison = self.s
        self.base.sql("ALTER TABLE einkauf_material_dialoge ADD COLUMN fields_json TEXT DEFAULT '{}'")
        self.base.sql('ALTER TABLE einkauf_material_dialoge ADD COLUMN message_id INTEGER')
        self.base.sql('CREATE TABLE einkauf_material_nachrichten(id INTEGER PRIMARY KEY,employee_id INTEGER)')
        self.base.sql('INSERT INTO einkauf_material_nachrichten VALUES(7,1)')
        self.fields = {'quantity':{'value':'2'},'unit':{'value':'Karton'},'urgent':{'value':False},'order_requested':{'value':True}}
        self.claim = dict(kind='recorded_external_order',channel='whatsapp',cap_scope='batch',batch_id='a'*32,
            source_revision=7,batch_selection=[{'id':1,'revision':7}],source_key='synthetic-shared-document',
            supplier_id='supplier-1',supplier_name='Testlieferant',recipient='+491701234567',subject='Synthetische Montagsliste',
            sent_at='2026-10-06T12:01:00+00:00',recorded_at='2026-10-07T12:00:00+00:00',recorded_by='admin',
            send_evidence='Synthetische WhatsApp-PDF, zwei Seiten, zugestellt',max_total_cents=None,
            draft_id=1,employee_id=1,quantity='2',unit='Karton',urgent=False,
            article_number='TEST-50',product_name='Testband',variant='50 mm')
        self.save()
        self.delivery = OrderDelivery(self.p)

    def save(self):
        shared = {key:self.claim.get(key) for key in ('supplier_id','recipient','subject','sent_at',
                  'send_evidence','source_key','max_total_cents','channel','cap_scope')}
        self.claim['batch_payload_hash'] = _hash(dict(shared,selection=self.claim['batch_selection']))
        self.base.sql('UPDATE einkauf_material_dialoge SET snapshot_json=?,snapshot_hash=?,fields_json=?,message_id=7 WHERE id=1',
                      (_json(self.claim),_hash(self.claim),_json(self.fields)))

    def receipt(self):
        order = self.delivery.order('material:1')
        group = self.f.service.create(receipt_payload(order))
        file = self.f.attach(group,kind='lieferschein',color='green')
        return dict(group_id=group['id'],file_id=file['id'],page=1,position=0,quantity='1',
                    supplier='Testlieferant',sku=self.claim['article_number'],variant=self.claim['variant'],
                    unit='Karton',reviewed=True)

    def test_historical_estimate_and_invoice_work_without_reservation_or_fake_shared_price(self):
        before = self.base.sql('SELECT * FROM einkauf_material_dialoge')
        estimate = self.s.record_estimate('material:1',self.base.price)
        self.assertEqual(estimate['order']['kind'],'external_recorded')
        self.assertEqual(estimate['order']['created_at'],self.claim['sent_at'])
        self.assertTrue(estimate['estimate']['after_dispatch'])
        self.assertEqual(estimate['estimate']['estimated_article_total'],'20.00')
        self.assertEqual(self.s.record_estimate('material:1',self.base.price),estimate)
        actual = self.s.record_invoice('material:1',self.base.invoice())
        self.assertEqual(actual['status'],'difference')
        self.assertEqual(actual['invoices'][0]['comparison']['difference'],'2')
        self.assertFalse(actual['can_dispatch'])
        self.assertEqual(self.base.sql('SELECT * FROM einkauf_material_dialoge'),before)
        self.assertEqual(self.base.sql('SELECT * FROM assistent_bestellanforderungen'),[])

    def test_frozen_known_identity_accepts_delivery_and_keeps_order_unmodified(self):
        before = self.base.sql('SELECT * FROM einkauf_material_dialoge')
        payload = self.receipt()
        view = self.delivery.record('material:1',payload)
        self.assertEqual(view['state'],'teillieferung')
        self.assertEqual((view['quantity'],view['ordered']),('1','2'))
        self.assertEqual(self.delivery.record('material:1',payload),view)
        self.assertEqual(self.base.sql('SELECT * FROM einkauf_material_dialoge'),before)
        self.assertEqual(len(self.base.sql('SELECT * FROM assistent_bestelllieferungen')),1)

    def test_missing_sku_or_variant_is_not_comparable_but_original_receipt_can_be_attached(self):
        original = deepcopy(self.claim)
        for name in ('article_number','variant'):
            self.claim = dict(original,**{name:''})
            self.save()
            view = self.s.detail('material:1')
            self.assertEqual(view['status'],'not_comparable')
            self.assertFalse(view['order']['identity_complete'])
            self.assertIsNone(view['estimate'])
            with patch.object(self.f.service,'original') as read,self.assertRaisesRegex(ValueError,'nicht vergleichbar'):
                self.s.record_estimate('material:1',self.base.price)
            read.assert_not_called()
            with self.assertRaisesRegex(ValueError,'nicht vergleichbar'):
                self.s.record_invoice('material:1',self.base.invoice())
        payload = self.receipt()
        self.assertEqual(len(self.f.service.detail(payload['group_id'])['files']),1)
        with self.assertRaisesRegex(ValueError,'nicht vergleichbar'):
            self.delivery.record('material:1',payload)
        self.assertEqual(self.base.sql('SELECT * FROM assistent_bestellpreis_basis'),[])
        self.assertEqual(self.base.sql('SELECT * FROM assistent_bestellpreis_rechnungen'),[])
        self.assertEqual(self.base.sql('SELECT * FROM assistent_bestelllieferungen'),[])

    def test_rehashed_tampered_owner_receipt_batch_or_frozen_quantity_is_rejected(self):
        original = deepcopy(self.claim)
        for changes in ({'draft_id':2},{'employee_id':2},{'channel':'email'},
                        {'send_evidence':''},{'cap_scope':'line'},{'batch_id':'bad'},
                        {'source_revision':8},{'sent_at':'2026-10-08T12:00:00+00:00'},
                        {'quantity':'99'},{'recipient':'orders@example.invalid'},{'variant':123}):
            with self.subTest(changes=changes):
                self.claim = dict(original,**changes)
                self.save()
                with self.assertRaises(ValueError):self.s.record_estimate('material:1',self.base.price)
                with self.assertRaises(ValueError):self.delivery.order('material:1')
        self.claim = original
        self.save()
        self.base.sql('UPDATE einkauf_material_nachrichten SET employee_id=2 WHERE id=7')
        with self.assertRaises(ValueError):self.s.detail('material:1')
        self.assertEqual(self.base.sql('SELECT * FROM assistent_bestellpreis_basis'),[])

    def test_shared_cap_and_record_time_never_become_estimated_price_or_order_day(self):
        self.claim.update(max_total_cents=25000,recorded_at='2026-10-12T12:00:00+00:00')
        self.save()
        order = self.s.detail('material:1')['order']
        self.assertNotIn('unit_price',order)
        self.assertNotIn('max_total_cents',order)
        self.assertEqual(order['created_at'],'2026-10-06T12:01:00+00:00')
        self.assertIsNone(self.s.detail('material:1')['estimate'])
        # The witnessed send day, not the later recording day, separates a
        # historical estimate from a new invoice.
        with self.assertRaisesRegex(ValueError,'spätere Rechnung'):
            self.s.record_estimate('material:1',dict(self.base.price,source_date='2026-10-07'))


if __name__ == '__main__':
    unittest.main()
