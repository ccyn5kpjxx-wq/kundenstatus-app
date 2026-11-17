"""Offline contract checks; synthetic catalogues and no network/payment IO."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_cart_quotes import CartQuoteService, QuoteInputError, retail_unit_gross_cent, restore_quote

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
TYRE, RIM, OIL, BODY, RIM2 = ('tyre:' + 'a'*24, 'alloy_wheel:' + 'b'*24,
                            'parts:' + 'c'*24, 'parts:' + 'd'*24, 'alloy_wheel:' + 'e'*24)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                          separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def line(identifier, quantity=1, *, services=None, group=''):
    return {'id': identifier, 'quantity': quantity, 'services': services or {}, 'assembly_group': group}


class Fixture:
    def __init__(self):
        self.products, self.calls = {}, {'retail': [], 'mapping': [], 'services': [], 'freight': []}
        self.stock = {}
        for identifier, category in ((TYRE, 'tyre'), (RIM, 'alloy_wheel'), (OIL, 'oil'),
                                     (BODY, 'body_part'), (RIM2, 'alloy_wheel')):
            identity = digest([identifier, 'exact-variant'])
            self.products[identifier] = {'id': identifier, 'category': category, 'name': 'Synthetic ' + category,
                'identity_hash': identity, 'fitment_required': category != 'oil',
                'fitment': {'confirmed': True, 'identity_hash': identity, 'vehicle_ref': 'CAR-1', 'evidence_id': 'reviewed-proof'},
                'required_fees_confirmed': True, 'required_fees': [], 'new_part': True,
                'painting_eligible': category == 'body_part', 'diameter': 18,
                'tyre_size': '225/40 R18 92Y XL', 'supplier_private_token': 'PRIVATE_CANARY'}

    def current(self):
        return {'confirmed': True, 'mode': 'test', 'currency': 'eur', 'vat_percent': '19.00',
                'observed_at': (NOW-timedelta(seconds=10)).isoformat(),
                'expires_at': (NOW+timedelta(seconds=600)).isoformat()}

    def catalogue(self, identifier):
        return deepcopy(self.products.get(identifier))

    def retail(self, product, quantity):
        self.calls['retail'].append((product['id'], quantity))
        return {**self.current(), 'product_id': product['id'], 'identity_hash': product['identity_hash'],
            'quantity': quantity, 'unit_gross_cent': retail_unit_gross_cent('100' if product['category'] in {'tyre','alloy_wheel'} else '50', product['category']),
            'stock': self.stock.get(product['id'], 100), 'procurement_confirmed': True,
            'net_purchase_price': 'PRIVATE_CANARY', 'provider_response': {'password': 'PRIVATE_CANARY'}}

    def mapping(self, product):
        self.calls['mapping'].append(product['id'])
        return {'confirmed': True, 'supplier': 'RSU', 'product_id': product['id'],
                'identity_hash': product['identity_hash'], 'supplier_article_id': 'RSU-' + product['id'], 'evidence_id': 'mapping-proof'}

    def service(self, code, product, quantity, options):
        self.calls['services'].append((code, product['id'], quantity, deepcopy(options)))
        unit = {'mounting':2500, 'painting':12000, 'installation':1500, 'oil_service':3000,
                'care':4500, 'required:rdks':2000, 'required:fasteners':500}[code]
        if code == 'mounting':
            unit += 500 if options['runflat'] else 0
            unit += 400 if options['disposal'] else 0
        return {**self.current(), 'product_id': product['id'], 'identity_hash': product['identity_hash'],
                'quantity': quantity, 'options_hash': digest(options), 'gross_cent': unit*quantity}

    def freight(self, shipment, context):
        self.calls['freight'].append(deepcopy(shipment))
        return {**self.current(), 'shipment_hash': shipment['shipment_hash'], 'context_hash': shipment['context_hash'],
                'gross_cent': 500 if shipment['destination'] == 'customer' else 700}

    def assembly(self, rim, tyre, vehicle):
        return {'confirmed': True, 'rim_identity_hash': rim['identity_hash'], 'tyre_identity_hash': tyre['identity_hash'],
                'vehicle_ref': vehicle, 'tyre_size': tyre['tyre_size'], 'rim_diameter': rim['diameter'], 'evidence_id': 'exact-size-and-car-proof'}

    def service_instance(self, **changes):
        providers = dict(catalogue_lookup=self.catalogue, retail_quote=self.retail, rsu_mapping=self.mapping,
                         service_quote=self.service, freight_quote=self.freight, assembly_fitment=self.assembly,
                         mode='test', clock=lambda: NOW)
        providers.update(changes)
        return CartQuoteService(**providers)

    def quote(self, rows, **changes):
        return self.service_instance(**changes).quote({'lines': rows}, customer_id=23, vehicle_ref='CAR-1',
                    delivery_context={'postcode': '74821', 'address_version': 'synthetic-1'})


class CartQuoteChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.network = patch.object(socket, 'socket', side_effect=AssertionError('Network forbidden in quote tests'))
        cls.network.start()

    @classmethod
    def tearDownClass(cls):
        cls.network.stop()

    def setUp(self):
        self.f = Fixture()

    def codes(self, quote):
        return {reason['code'] for reason in quote.public()['blocked_reasons']}

    def test_mixed_cart_prices_mounting_once_and_splits_delivery(self):
        q = self.f.quote([line(TYRE,4,services={'mounting':True},group='set-1'),
                          line(RIM,4,services={'mounting':True},group='set-1'), line(OIL,2),
                          line(BODY,services={'painting':True,'paint_color':'LY7W'})])
        p = q.public()
        self.assertTrue(p['ready'])
        self.assertEqual(p['total_gross_cent'], 148449)
        self.assertEqual(p['total_gross_cent'], p['known_subtotal_gross_cent'])
        self.assertEqual(len([v for v in self.f.calls['services'] if v[0]=='mounting']), 1)
        self.assertEqual([v[1:3] for v in self.f.calls['services'] if v[0]=='mounting'], [(TYRE,4)])
        self.assertEqual({row['id']:row['destination'] for row in p['lines']},
                         {TYRE:'workshop',RIM:'workshop',OIL:'customer',BODY:'workshop'})
        self.assertEqual(len(p['shipments']), 2)
        self.assertFalse(p['order_enabled'])
        self.assertFalse(p['payment_enabled'])

    def test_trusted_markup_is_exact_per_unit_then_quantity(self):
        self.assertEqual(retail_unit_gross_cent('50', 'tyre'), 6545)
        self.assertEqual(retail_unit_gross_cent('50', 'oil'), 6843)
        self.assertEqual(retail_unit_gross_cent(Decimal('100.0055'), 'alloy_wheel'), 13091)
        for value in ('NaN','Infinity','-1','0','0.00001',True,1.25):
            with self.subTest(value=value), self.assertRaises(ValueError):
                retail_unit_gross_cent(value,'oil')
        q = self.f.quote([line(OIL,3)]).public()
        self.assertEqual(q['lines'][0]['items_gross_cent'],6843*3)

    def test_manual_mixed_order_policy_blocks_payment_without_losing_real_totals(self):
        result = self.f.quote([line(TYRE, 4), line(OIL)], manual_mixed_orders=True).public()
        self.assertFalse(result['ready'])
        self.assertIsNone(result['total_gross_cent'])
        self.assertIsInstance(result['known_subtotal_gross_cent'], int)
        self.assertIn('mixed_cart_manual_review_required', {r['code'] for r in result['blocked_reasons']})
        self.assertTrue(self.f.quote([line(TYRE, 4)], manual_mixed_orders=True).public()['ready'])
        split = self.f.quote([line(TYRE, 2), line(TYRE, 2, services={'mounting': True})], manual_mixed_orders=True)
        self.assertIn('mixed_cart_manual_review_required', self.codes(split))

    def test_live_default_requires_manual_mixed_offer_but_synthetic_test_mode_can_exercise_saga(self):
        self.assertTrue(CartQuoteService(mode='live').manual_mixed_orders)
        self.assertFalse(CartQuoteService(mode='test').manual_mixed_orders)
        with self.assertRaisesRegex(ValueError, 'invalid_mixed_order_policy'):
            CartQuoteService(manual_mixed_orders='false')

    def test_direct_shipping_needs_supported_explicit_salutation_before_payment(self):
        for salutation in (None, '', 'An', True, 'Herr', 'Frau', 'Firma'):
            context = {'delivery': {'salutation': salutation}}
            service = self.f.service_instance(require_delivery_salutation=True)
            result = service.quote({'lines': [line(OIL)]}, customer_id=23, vehicle_ref='CAR-1', delivery_context=context)
            self.assertEqual('delivery_salutation_required' in self.codes(result), salutation not in {'Herr', 'Frau', 'Firma'})
        result = self.f.quote([line(TYRE, 4, services={'mounting': True})], require_delivery_salutation=True)
        self.assertNotIn('delivery_salutation_required', self.codes(result))

    def test_required_tyre_documents_cannot_be_asserted_through_cart_payload(self):
        result = self.f.quote([line(TYRE)], require_tyre_documents=True)
        self.assertIn('product_documents_unconfirmed', self.codes(result))
        self.f.products[TYRE]['required_documents_confirmed'] = True
        self.assertNotIn('product_documents_unconfirmed', self.codes(self.f.quote([line(TYRE)], require_tyre_documents=True)))

    def test_provider_observation_after_slow_io_uses_current_clock_and_final_creation(self):
        current = [NOW]
        def retail(p, count):
            current[0] += timedelta(seconds=2)
            return {**self.f.retail(p, count), 'observed_at': current[0].isoformat(),
                    'expires_at': (current[0] + timedelta(minutes=15)).isoformat()}
        def freight(group, context):
            current[0] += timedelta(seconds=3)
            return {**self.f.freight(group, context), 'observed_at': current[0].isoformat(),
                    'expires_at': (current[0] + timedelta(minutes=15)).isoformat()}
        q = self.f.quote([line(OIL)], retail_quote=retail, freight_quote=freight,
                         clock=lambda: current[0]).public()
        self.assertTrue(q['ready'])
        self.assertEqual(q['created_at'], (NOW + timedelta(seconds=5)).isoformat())
        self.assertEqual(q['expires_at'], (NOW + timedelta(minutes=15)).isoformat())

    def test_later_slow_dependency_cannot_make_expired_inquiry_payable(self):
        current = [NOW]
        def retail(p, count):
            current[0] += timedelta(seconds=1)
            return {**self.f.retail(p, count), 'observed_at': current[0].isoformat(),
                    'expires_at': (current[0] + timedelta(seconds=2)).isoformat()}
        def freight(group, context):
            current[0] += timedelta(seconds=3)
            return {**self.f.freight(group, context), 'observed_at': current[0].isoformat(),
                    'expires_at': (current[0] + timedelta(minutes=15)).isoformat()}
        q = self.f.quote([line(OIL)], retail_quote=retail, freight_quote=freight,
                         clock=lambda: current[0]).public()
        self.assertFalse(q['ready'])
        self.assertIsNone(q['total_gross_cent'])
        self.assertIn('quote_expired', {r['code'] for r in q['blocked_reasons']})
        self.assertEqual(q['created_at'], (NOW + timedelta(seconds=4)).isoformat())
        self.assertEqual(q['expires_at'], (NOW + timedelta(seconds=3)).isoformat())

    def test_clock_moving_backwards_or_losing_timezone_after_io_fails_closed(self):
        for target, reason in ((NOW - timedelta(seconds=1), 'clock_moved_backwards'),
                               (NOW.replace(tzinfo=None), 'aware_clock_required')):
            with self.subTest(target=target):
                current = [NOW]
                def retail(p, count):
                    current[0] = target
                    return self.f.retail(p, count)
                with self.assertRaisesRegex(ValueError, reason):
                    self.f.quote([line(OIL)], retail_quote=retail, clock=lambda: current[0])

    def test_browser_price_and_unknown_fields_cannot_enter_quote(self):
        service = self.f.service_instance()
        values = [ {'lines':[line(OIL)],'total_gross_cent':1},
                   {'lines':[{**line(OIL),'unit_gross_cent':1}]},
                   {'lines':[{**line(OIL),'supplier_article_id':'forged'}]},
                   {'lines':[line(OIL,True)]}, {'lines':[line(OIL,'1')]},
                   {'lines':[line('https://attacker.invalid/')]},
                   {'lines':[line(OIL,services={'care':{}})]},
                   {'lines':[line(OIL,services={'mounting':'true'})]},
                   {'lines':[line(BODY,services={'painting':True,'paint_color':'<script>'})]} ]
        for value in values:
            with self.subTest(value=value), self.assertRaises(QuoteInputError):
                service.quote(value,customer_id='customer')
        self.assertEqual(self.f.calls['retail'],[])

    def test_exact_duplicate_configuration_is_rejected(self):
        with self.assertRaisesRegex(QuoteInputError,'duplicate_line'):
            self.f.quote([line(OIL),line(OIL)])

    def test_same_sku_destinations_use_exact_position_quantities_and_global_stock(self):
        self.f.stock[OIL]=4
        q = self.f.quote([line(OIL,2),line(OIL,3,services={'oil_service':True})])
        self.assertIn('stock_insufficient_or_unconfirmed',self.codes(q))
        self.assertEqual(self.f.calls['retail'],[(OIL,2),(OIL,3)])
        self.assertIsNone(q.public()['total_gross_cent'])
        self.f.stock[OIL]=5
        q = self.f.quote([line(OIL,2),line(OIL,3,services={'oil_service':True})])
        self.assertTrue(q.public()['ready'])
        self.assertEqual({g['id']:g['items'] for g in q.public()['shipments']},
                         {'customer':[{'id':OIL,'quantity':2}],'workshop':[{'id':OIL,'quantity':3}]})

    def test_split_destinations_keep_their_different_price_tiers(self):
        def retail(product, quantity):
            self.assertIn(quantity, (2,3))
            return {**self.f.retail(product,quantity), 'unit_gross_cent':{2:8211,3:7527}[quantity], 'stock':5}
        q=self.f.quote([line(OIL,2),line(OIL,3,services={'oil_service':True})],retail_quote=retail)
        public=q.public()
        self.assertTrue(public['ready'])
        self.assertEqual(self.f.calls['retail'],[(OIL,2),(OIL,3)])
        self.assertEqual([(v['destination'],v['quantity'],v['unit_gross_cent'],v['items_gross_cent']) for v in public['lines']],
                         [('customer',2,8211,16422),('workshop',3,7527,22581)])
        self.assertEqual(public['total_gross_cent'],49203)
        private=q.snapshot()['shipments']
        self.assertEqual({v['destination']:v['private_items'][0]['quantity'] for v in private},
                         {'customer':2,'workshop':3})

    def test_minimum_reported_stock_must_cover_both_destinations(self):
        for stock_by_quantity in ({2:4,3:50},{2:50,3:4}):
            with self.subTest(stock_by_quantity=stock_by_quantity):
                self.f=Fixture()
                def retail(product,quantity):
                    return {**self.f.retail(product,quantity),'stock':stock_by_quantity[quantity]}
                q=self.f.quote([line(OIL,2),line(OIL,3,services={'oil_service':True})],retail_quote=retail)
                self.assertEqual(self.f.calls['retail'],[(OIL,2),(OIL,3)])
                self.assertIn('stock_insufficient_or_unconfirmed',self.codes(q))
                self.assertFalse(q.public()['ready'])
                self.assertIsNone(q.public()['total_gross_cent'])

    def test_same_sku_configurations_in_one_destination_share_one_exact_price_read(self):
        rows=[line(BODY,services={'painting':True,'paint_color':'RAL 9005'}),
              line(BODY,services={'painting':True,'paint_color':'RAL 9010'})]
        q=self.f.quote(rows)
        self.assertTrue(q.public()['ready'])
        self.assertEqual(self.f.calls['retail'],[(BODY,2)])
        self.assertEqual([v['unit_gross_cent'] for v in q.public()['lines']],[6843,6843])
        self.assertEqual(q.snapshot()['shipments'][0]['private_items'][0]['quantity'],2)
        self.assertEqual(len(q.snapshot()['shipments'][0]['private_items']),1)

    def test_a_cart_wide_price_echo_cannot_replace_an_exact_destination_quote(self):
        def retail(product,quantity):
            return {**self.f.retail(product,quantity),'quantity':5}
        q=self.f.quote([line(OIL,2),line(OIL,3,services={'oil_service':True})],retail_quote=retail)
        self.assertEqual(self.f.calls['retail'],[(OIL,2),(OIL,3)])
        self.assertIn('price_identity_unconfirmed',self.codes(q))
        self.assertIsNone(q.public()['total_gross_cent'])

    def test_two_complete_sets_share_tyre_stock_and_charge_each_mounting_and_freight_once(self):
        rows=[line(TYRE,4,services={'mounting':True},group='first'),line(RIM,4,group='first'),
              line(TYRE,4,services={'mounting':True},group='second'),line(RIM2,4,group='second')]
        self.f.stock[TYRE]=7
        self.assertIn('stock_insufficient_or_unconfirmed',self.codes(self.f.quote(rows)))
        self.f=Fixture();self.f.stock[TYRE]=8
        q=self.f.quote(rows).public()
        self.assertTrue(q['ready'])
        self.assertEqual(q['total_gross_cent'],230140)
        self.assertEqual([call for call in self.f.calls['retail'] if call[0]==TYRE],[(TYRE,8)])
        self.assertEqual(len([call for call in self.f.calls['services'] if call[0]=='mounting']),2)
        self.assertEqual(len(q['shipments']),1)
        self.assertEqual(next(item['quantity'] for item in q['shipments'][0]['items'] if item['id']==TYRE),8)

    def test_missing_mapping_and_alias_supplier_identity_are_blocked(self):
        self.assertIn('rsu_mapping_unconfirmed',self.codes(self.f.quote([line(OIL)],rsu_mapping=None)))
        def alias(p):
            return {**self.f.mapping(p),'supplier_article_id':'same-supplier-sku'}
        q = self.f.quote([line(TYRE),line(RIM)],rsu_mapping=alias)
        self.assertIn('duplicate_procurement_identity',self.codes(q))

    def test_price_binding_time_mode_currency_stock_and_procurement_are_required(self):
        changes = [({'identity_hash':'f'*64},'price_identity_unconfirmed'),
                   ({'quantity':2},'price_identity_unconfirmed'),({'stock':True},'stock_insufficient_or_unconfirmed'),
                   ({'currency':'usd'},'currency_mismatch'),({'mode':'live'},'mode_mismatch'),
                   ({'vat_percent':'7.00'},'vat_unconfirmed'),
                   ({'procurement_confirmed':False},'procurement_price_unconfirmed'),
                   ({'expires_at':NOW.isoformat()},'quote_expired'),
                   ({'observed_at':(NOW+timedelta(seconds=1)).isoformat()},'quote_from_future'),
                   ({'unit_gross_cent':False},'price_amount_unconfirmed'),
                   ({'unit_gross_cent':float('nan')},'price_amount_unconfirmed')]
        for alteration,code in changes:
            def price(p,q):
                return {**self.f.retail(p,q),**alteration}
            with self.subTest(alteration=alteration):
                result=self.f.quote([line(OIL)],retail_quote=price)
                self.assertIn(code,self.codes(result))
                self.assertIsNone(result.public()['total_gross_cent'])

    def test_unknown_freight_is_not_zero_but_explicit_confirmed_zero_is_valid(self):
        q=self.f.quote([line(OIL)],freight_quote=None)
        self.assertFalse(q.public()['ready'])
        self.assertIsNone(q.public()['shipments'][0]['gross_cent'])
        self.assertEqual(q.public()['known_subtotal_gross_cent'],6843)
        def free(group,context):
            return {**self.f.freight(group,context),'gross_cent':0}
        q=self.f.quote([line(OIL)],freight_quote=free)
        self.assertTrue(q.public()['ready'])
        self.assertEqual(q.public()['total_gross_cent'],6843)
        self.assertEqual(q.public()['shipments'][0]['gross_cent'],0)

    def test_freight_is_bound_to_address_and_exact_shipping_group(self):
        for key in ('context_hash','shipment_hash'):
            def wrong(group,context):
                return {**self.f.freight(group,context),key:'f'*64}
            with self.subTest(key=key):
                q=self.f.quote([line(OIL)],freight_quote=wrong)
                self.assertIn('freight_unconfirmed',self.codes(q))

    def test_partial_dependency_failures_never_publish_a_payable_total_or_error_secret(self):
        def partial(group,context):
            if group['destination']=='customer':
                raise RuntimeError('PRIVATE_CANARY')
            return self.f.freight(group,context)
        q=self.f.quote([line(OIL),line(BODY,services={'painting':True,'paint_color':'RAL 9005'})],freight_quote=partial)
        self.assertIsNone(q.public()['total_gross_cent'])
        self.assertEqual({g['id']:g['gross_cent'] for g in q.public()['shipments']},{'customer':None,'workshop':700})
        self.assertNotIn('PRIVATE_CANARY',json.dumps(q.snapshot()))
        empty=CartQuoteService(clock=lambda:NOW).quote({'lines':[line(OIL)]},customer_id='synthetic')
        self.assertFalse(empty.public()['ready'])
        self.assertIsNone(empty.public()['known_subtotal_gross_cent'])

    def test_vehicle_fitment_cannot_be_asserted_by_browser_or_missing_metadata(self):
        for alteration in ({'fitment':{'confirmed':True}},{'fitment_required':None}):
            self.f = Fixture()
            self.f.products[TYRE].update(alteration)
            self.assertIn('fitment_unconfirmed',self.codes(self.f.quote([line(TYRE)])))
        self.f=Fixture()
        q=self.f.service_instance().quote({'lines':[line(TYRE)]},customer_id=23,vehicle_ref='other-car')
        self.assertIn('fitment_unconfirmed',self.codes(q))

    def test_complete_wheels_require_exact_size_proof_and_matching_quantities(self):
        rows=[line(TYRE,4,services={'mounting':True},group='set'),line(RIM,4,group='set')]
        self.assertTrue(self.f.quote(rows).public()['ready'])
        self.assertIn('assembly_fitment_unconfirmed',self.codes(self.f.quote(rows,assembly_fitment=None)))
        bad=deepcopy(rows);bad[1]['quantity']=3
        self.assertIn('assembly_fitment_unconfirmed',self.codes(self.f.quote(bad)))
        self.f.products[TYRE]['tyre_size']='225/40 R17 92Y XL'
        self.assertIn('assembly_fitment_unconfirmed',self.codes(self.f.quote(rows)))
        self.f=Fixture()
        self.f.products[RIM]['diameter']=19
        self.assertIn('assembly_fitment_unconfirmed',self.codes(self.f.quote(rows)))

    def test_group_without_mounting_confirmation_cannot_hide_a_missing_montage_fee(self):
        q=self.f.quote([line(TYRE,group='set'),line(RIM,group='set')])
        self.assertIn('assembly_mounting_unconfirmed',self.codes(q))
        self.assertEqual(self.f.calls['services'],[])

    def test_painting_and_other_services_require_eligible_product_and_confirmed_fee(self):
        self.f.products[BODY]['painting_eligible']=False
        q=self.f.quote([line(BODY,services={'painting':True,'paint_color':'LY7W'})])
        self.assertIn('service_not_applicable',self.codes(q))
        self.f=Fixture()
        q=self.f.quote([line(BODY,services={'painting':True,'paint_color':'LY7W'})],service_quote=None)
        self.assertFalse(q.public()['ready'])
        self.assertIsNone(q.public()['lines'][0]['services'][0]['gross_cent'])
        def wrong_options(code,p,n,options):
            return {**self.f.service(code,p,n,options),'options_hash':digest({'paint_color':'different'})}
        q=self.f.quote([line(BODY,services={'painting':True,'paint_color':'LY7W'})],service_quote=wrong_options)
        self.assertIn('service_quote_unconfirmed',self.codes(q))

    def test_required_costs_are_confirmed_even_when_the_list_is_empty(self):
        self.f.products[RIM]['required_fees_confirmed']=False
        self.assertIn('required_fees_unconfirmed',self.codes(self.f.quote([line(RIM)])))
        self.f.products[RIM]['required_fees_confirmed']=True
        self.f.products[RIM]['required_fees']=['rdks','fasteners']
        q=self.f.quote([line(RIM,4)])
        self.assertTrue(q.public()['ready'])
        self.assertEqual(q.public()['total_gross_cent'],13090*4+2000*4+500*4+500)
        self.assertFalse(self.f.quote([line(RIM,4)],service_quote=None).public()['ready'])
        self.f.products[RIM]['required_fees']=[{}]
        self.assertIn('required_fees_unconfirmed',self.codes(self.f.quote([line(RIM)])))

    def test_snapshot_is_immutable_public_whitelist_and_hash_rejects_tampering(self):
        rows=[line(OIL)]
        q=self.f.quote(rows)
        before=q.snapshot()
        rows[0]['quantity']=20
        self.f.products[OIL]['name']='changed afterwards'
        public=q.public();public['lines'][0]['items_gross_cent']=1
        self.assertEqual(q.snapshot(),before)
        self.assertNotIn('PRIVATE_CANARY',json.dumps(q.snapshot()))
        for key in ('private_items','supplier_article_id','customer_id','vehicle_ref','identity_hash'):
            self.assertNotIn(key,json.dumps(q.public()))
        restored=restore_quote(before,q.quote_hash,mode='test',now=NOW)
        self.assertEqual(restored.public(),q.public())
        tampered=deepcopy(before);tampered['total_gross_cent']+=1
        with self.assertRaisesRegex(QuoteInputError,'quote_binding_mismatch'):
            restore_quote(tampered,q.quote_hash,mode='test',now=NOW)
        with self.assertRaises(QuoteInputError):
            restore_quote(before,q.quote_hash,mode='live',now=NOW)

    def test_earliest_dependency_expiry_limits_the_quote(self):
        def short_service(code,p,n,options):
            return {**self.f.service(code,p,n,options),'expires_at':(NOW+timedelta(seconds=60)).isoformat()}
        q=self.f.quote([line(OIL,services={'oil_service':True})],service_quote=short_service)
        self.assertEqual(q.public()['expires_at'],(NOW+timedelta(seconds=60)).isoformat())
        restore_quote(q.snapshot(),q.quote_hash,mode='test',now=NOW+timedelta(seconds=59))
        with self.assertRaisesRegex(QuoteInputError,'quote_expired'):
            restore_quote(q.snapshot(),q.quote_hash,mode='test',now=NOW+timedelta(seconds=60))

    def test_missing_owner_bad_mode_or_naive_clock_cannot_prepare_a_quote(self):
        with self.assertRaises(ValueError):
            self.f.service_instance(mode='off')
        for owner in ('',None,True):
            with self.subTest(owner=owner),self.assertRaises(QuoteInputError):
                self.f.service_instance().quote({'lines':[line(OIL)]},customer_id=owner)
        with self.assertRaises(ValueError):
            self.f.quote([line(OIL)],clock=lambda:NOW.replace(tzinfo=None))

    def test_lone_unicode_surrogates_cannot_crash_canonical_quote_encoding(self):
        service=self.f.service_instance()
        with self.assertRaises(QuoteInputError):
            service.quote({'lines':[line(OIL)]},customer_id='\ud800')
        with self.assertRaises(QuoteInputError):
            service.quote({'lines':[line(OIL)]},customer_id='customer',vehicle_ref='\ud800')
        with self.assertRaises(QuoteInputError):
            service.quote({'lines':[line(BODY,services={'painting':True,'paint_color':'\ud800'})]},customer_id='customer')
        self.f.products[OIL]['name']='\ud800'
        quote=self.f.quote([line(OIL)])
        self.assertIn('catalogue_identity_unconfirmed',self.codes(quote))


if __name__=='__main__':
    unittest.main(verbosity=2)
