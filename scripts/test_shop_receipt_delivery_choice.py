"""Real temporary receipt/order stores, synthetic authorization, no network."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import test_shop_order_workflow as fixtures
from shop_order_receipts import ReceiptError, WORKSHOP_DELIVERY_LABELS


class ReceiptDeliveryChecks(unittest.TestCase):
    def fixture(self, choice=None):
        context=deepcopy(fixtures.CONTEXT)
        if choice is not None:context['workshop_delivery']=choice
        case=fixtures.WorkflowChecks('test_queue_and_initial_receipt_commit_atomically_once_after_verified_ledger_auth')
        with patch.object(fixtures,'CONTEXT',context):case.setUp()
        self.addCleanup(case.tearDown)
        return case

    def test_selected_collection_or_reshipment_survives_private_context_and_owner_receipt(self):
        for choice in ('collection','customer_reshipment'):
            with self.subTest(choice=choice):
                f=self.fixture(choice)
                prepared=f.receipts.prepare(f.order_id,customer_id='customer-a',kind='order_received')
                self.assertIn(WORKSHOP_DELIVERY_LABELS[choice],prepared['body'])
                self.assertIn('Kartenbetrag reserviert',prepared['body'])
                self.assertNotIn('bereits versendet',prepared['body'])
                self.assertNotIn('wurde zugestellt',prepared['body'])
                view=f.receipts.customer_receipt(f.order_id,customer_id='customer-a')
                self.assertEqual(view['workshop_delivery'],choice)
                self.assertEqual(view['workshop_delivery_label'],WORKSHOP_DELIVERY_LABELS[choice])
                self.assertNotIn('billing',view)
                private=f.receipts.customer_receipt(f.order_id,customer_id='customer-a',include_addresses=True)
                self.assertEqual(private['delivery'],fixtures.ADDRESS)
                self.assertEqual(private['workshop_delivery'],choice)
                self.assertEqual(f.outbox.sent,[])
                self.assertEqual(f.f.transport.calls,[])

    def test_legacy_receipt_keeps_existing_text_and_immutable_replay(self):
        f=self.fixture()
        one=f.receipts.prepare(f.order_id,customer_id='customer-a',kind='order_received')
        self.assertIn('Versand zur Werkstatt:',one['body'])
        self.assertNotIn('Abholung nach Werkstattleistung',one['body'])
        self.assertNotIn('Weiterversand',one['body'])
        f.receipts.enqueue(f.order_id,customer_id='customer-a',kind='order_received')
        f.receipts.enqueue(f.order_id,customer_id='customer-a',kind='order_received')
        self.assertEqual(f.count('shop_order_receipts_v2'),1)
        two=f.receipts.prepare(f.order_id,customer_id='customer-a',kind='order_received')
        self.assertEqual(one['fingerprint'],two['fingerprint'])
        self.assertNotIn('workshop_delivery',f.receipts.customer_receipt(f.order_id,customer_id='customer-a'))

    def test_accepted_order_receipt_repeats_the_wish_without_reserving(self):
        original = fixtures.OrderStore.create_order
        def with_wish(store, *args, **kwargs):
            return original(store, *args, **kwargs, appointment_preference={'date':'2026-11-20','time':'10:00'},
                            appointment_calendar=lambda day: ['10:00'])
        with patch.object(fixtures.OrderStore, 'create_order', with_wish):
            f = self.fixture('collection')
        prepared = f.receipts.prepare(f.order_id,customer_id='customer-a',kind='order_received')
        self.assertIn('Wunschtermin für die Reifenmontage ist der 20.11.2026 um 10:00 Uhr',prepared['body'])
        self.assertIn('sobald die Reifenlieferung geklärt ist',prepared['body'])
        self.assertEqual(f.outbox.sent,[])

    def test_invalid_saved_enum_never_enters_customer_or_queued_receipt(self):
        f=self.fixture('collection');order,snapshot,context=f.receipts._load(f.order_id,'customer-a')
        context['workshop_delivery']='already-shipped'
        with patch.object(f.receipts,'_load',return_value=(order,snapshot,context)):
            with self.assertRaises(ReceiptError):f.receipts.prepare(f.order_id,customer_id='customer-a',kind='order_received')
            with self.assertRaises(ReceiptError):f.receipts.customer_receipt(f.order_id,customer_id='customer-a')
        self.assertEqual(f.count('shop_order_receipts_v2'),0)

    def test_paid_customer_sees_same_number_and_items_as_the_confirmation(self):
        f = self.fixture('collection')
        order, snapshot, context = f.receipts._load(f.order_id, 'customer-a')
        order.update(mode='live', status='paid', appointment_preference={'date': '2026-11-20', 'time': '10:00'})
        lookup = Mock(return_value={f.order_id: 582})
        with patch.object(f.receipts, 'mode', 'live'), patch.object(f.receipts, 'workshop_order_lookup', lookup), \
                patch.object(f.receipts, '_load', return_value=(order, snapshot, context)):
            view = f.receipts.customer_workshop_order(f.order_id, customer_id='customer-a')
            message = f.receipts.prepare(f.order_id, customer_id='customer-a', kind='order_confirmed')
            self.assertEqual(view['number'], 582)
            self.assertTrue(view['mounting'])
            self.assertIn('Montageauftrag Nr. 582', message['subject'])
            self.assertIn('Montageauftrag Nr. 582', message['body'])
            self.assertIn('20.11.2026 um 10:00 Uhr', message['body'])
            self.assertEqual(len(view['items']), len(snapshot['lines']))
            self.assertTrue(any('Komplettradmontage' in item['services'] for item in view['items']))
            for item in view['items']:
                self.assertEqual(set(item), {'name', 'quantity', 'services'})
                self.assertIn(f"{item['quantity']} × {item['name']}", message['body'])
            lookup.return_value = {}
            self.assertEqual(f.receipts.customer_workshop_order(f.order_id, customer_id='customer-a'), {'pending': True})
            with self.assertRaisesRegex(ReceiptError, 'workshop_order_pending'):
                f.receipts.prepare(f.order_id, customer_id='customer-a', kind='order_confirmed')
        self.assertFalse(f.outbox.sent)

    def test_goods_order_has_order_number_and_only_its_purchased_goods(self):
        f = self.fixture()
        order, snapshot, context = f.receipts._load(f.order_id, 'customer-a')
        order.update(mode='live', status='paid')
        oil = next(i for i, row in enumerate(snapshot['lines']) if row['category'] == 'oil')
        snapshot.update(lines=[snapshot['lines'][oil]], request=[snapshot['request'][oil]])
        lookup = Mock(return_value={f.order_id: 583})
        with patch.object(f.receipts, 'mode', 'live'), patch.object(f.receipts, 'workshop_order_lookup', lookup), \
                patch.object(f.receipts, '_load', return_value=(order, snapshot, context)):
            view = f.receipts.customer_workshop_order(f.order_id, customer_id='customer-a')
            self.assertFalse(view['mounting'])
            self.assertEqual(len(view['items']), 1)
            message = f.receipts.prepare(f.order_id, customer_id='customer-a', kind='order_confirmed')
            self.assertIn('Auftrag Nr. 583', message['body'])
            self.assertNotIn('Montagetermin', message['body'])
            order['status'] = 'authorized'
            lookup.reset_mock()
            receipt = f.receipts.prepare(f.order_id, customer_id='customer-a', kind='order_received')
            self.assertNotIn('Auftrag Nr.', receipt['body'])
            self.assertIsNone(f.receipts.customer_workshop_order(f.order_id, customer_id='customer-a'))
            lookup.assert_not_called()


if __name__=='__main__':unittest.main()
