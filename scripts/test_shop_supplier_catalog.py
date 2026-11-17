"""Synthetic rim evidence and display products, without provider access."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_supplier_catalog import build_supplier_catalog, rim_candidate, validate_rim_mapping, SupplierCatalogError
from shop_rsu_quotes import RSUQuoteProvider, RSUQuotePolicy, RSUQuoteError
from tyresystem_api import ArticleInquiry

PID = 'alloy_wheel:' + 'a' * 24
EAN = '4019238594195'
NOW = datetime(2026, 11, 17, tzinfo=timezone.utc)


def sources():
    product = {'selection_id': PID, 'category': 'alloy_wheel', 'manufacturer_name': 'Example',
        'name': 'Example V Crystal Silver', 'width': '7', 'diameter': '17', 'details': {'Einpresstiefe': '40 mm'},
        'identifiers': {'ean': EAN, 'manufacturer_article_number': None},
        'price': 'PRIVATE_CANARY', 'quote_public': {'private': 'PRIVATE_CANARY'}, 'technical_reports': ['unreviewed']}
    private = {'variant': {'alloyRimFrontWidth': '7.00', 'alloyRimFrontDiameter': 17, 'alloyRimFrontOffset': 40,
        'alloyRimFrontOffsetFrom': 40, 'alloyRimFrontOffsetTo': 40, 'alloyRimFrontHoleNumber': 5,
        'alloyRimFrontHoleCircle': 112, 'alloyRimFrontHubHole': '57.1'}}
    rsu = {'Artikelnummer': '12345', 'ean': EAN, 'Hersteller': 'EXAMPLE', 'Herstellerartikelnummer': 'MPN1',
        'Profil': 'V crystal silver 7x17 ET40 - LK5/112 ML57.1', 'Artikeltyp': 'Felge', 'Artikeluntertyp': 'Alu',
        'Dot': '', 'Demo': '', 'Da': ''}
    return product, private, rsu


def candidate():
    p, e, r = sources()
    return rim_candidate(p, e, r, alzura_source_sha256='a' * 64, rsu_source_sha256='b' * 64, row_number=1)


class SupplierTests(unittest.TestCase):
    def test_exact_ean_manufacturer_model_colour_and_every_geometry_field(self):
        value = candidate()
        self.assertFalse(value['confirmed'])
        self.assertFalse(value['vehicle_fitment_confirmed'])
        self.assertEqual(value['shipping_class'], 'pkw_rim')
        self.assertEqual(validate_rim_mapping(value, product_id=PID, category='alloy_wheel', expected_ean=EAN)['supplier_article_id'], '12345')
        for field, changed in (('Hersteller', 'Alias'), ('Profil', 'V black 7x17 ET40 - LK5/112 ML57.1'),
            ('Profil', 'V crystal silver 7x17 ET45 - LK5/112 ML57.1'), ('Demo', 'Demo')):
            p, e, r = sources()
            r[field] = changed
            with self.subTest(field=field, changed=changed):
                self.assertIsNone(rim_candidate(p, e, r, alzura_source_sha256='a' * 64, rsu_source_sha256='b' * 64, row_number=1))

    def test_conflicting_public_private_geometry_or_known_mpn_blocks(self):
        for modify in (lambda p,e: p['details'].update(Einpresstiefe='45 mm'),
                       lambda p,e: e['variant'].update(alloyRimFrontOffsetTo=45),
                       lambda p,e: p['identifiers'].update(manufacturer_article_number='OTHER')):
            p,e,r=sources()
            modify(p,e)
            self.assertIsNone(rim_candidate(p,e,r,alzura_source_sha256='a'*64,rsu_source_sha256='b'*64,row_number=1))

    def test_mapping_is_usable_only_after_independent_approval_and_fresh_inquiry(self):
        class Fake:
            calls = 0
            def inquire(self, article, quantity):
                self.calls += 1
                return ArticleInquiry(article, EAN, quantity, Decimal('100'), 20, 'Synthetic rim', NOW, 'test')
        inquiry = Fake()
        bindings = {PID: {'identity_hash': 'c'*64, 'category': 'alloy_wheel', 'mapping': candidate(), 'expected_ean': EAN}}
        policy = RSUQuotePolicy('test','EUR',True,'synthetic-net-eur-policy')
        denied = RSUQuoteProvider(bindings=bindings,approved_ids=set(),policy=policy,inquiry=inquiry,clock=lambda:NOW)
        product = {'id':PID,'identity_hash':'c'*64,'category':'alloy_wheel'}
        self.assertIsNone(denied.retail_quote(product,4))
        self.assertEqual(inquiry.calls,0)
        approved = RSUQuoteProvider(bindings=bindings,approved_ids={PID},policy=policy,inquiry=inquiry,clock=lambda:NOW)
        self.assertEqual(approved.retail_quote(product,4)['unit_gross_cent'],13685)
        tampered = deepcopy(bindings)
        tampered[PID]['mapping']['evidence']['rsu']['record']['Artikelnummer']='999'
        with self.assertRaises(RSUQuoteError):
            RSUQuoteProvider(bindings=tampered,approved_ids={PID},policy=policy,inquiry=inquiry)

    def test_full_display_keeps_unmapped_parts_but_never_source_prices_or_fitment(self):
        p,e,r=sources()
        parts=[{'provider_id':'products:1','category':'oil','name':'Example oil','brand':'Example',
                'article_number':'OIL1','ean':EAN,'unit_label':'5 L','unit_net_eur':'PRIVATE_CANARY'}]
        result=build_supplier_catalog([p],parts,[(1,r)],private_evidence={PID:e},
                                     alzura_source_sha256='a'*64,rsu_source_sha256='b'*64)
        self.assertEqual(len(result['products']),2)
        self.assertEqual(result['report']['exact_rim_candidates'],1)
        self.assertEqual(result['report']['exact_parts_candidates'],0)
        self.assertNotIn('PRIVATE_CANARY',json.dumps(result))
        self.assertTrue(all(not row['order_enabled'] for row in result['products']))
        self.assertEqual(result['products'][0]['technical_reports'],[])
        self.assertEqual(result['private_reports'][PID]['unreviewed_technical_reports'],['unreviewed'])
        ambiguous=build_supplier_catalog([p],parts,[(1,r),(2,{**r,'Artikelnummer':'999'})],private_evidence={PID:e},
                                         alzura_source_sha256='a'*64,rsu_source_sha256='b'*64)
        self.assertEqual(ambiguous['mappings'],{})

    def test_typed_parts_department_and_unreviewed_price_survive_normalization(self):
        rows = [
            {'provider_id':'wearparts:1','category':'accessory','source_request':{'area':'wearparts'}},
            {'provider_id':'products:2','category':'accessory','department':'parts'},
            {'provider_id':'products:3','category':'oil','price_review_required':True},
        ]
        result=build_supplier_catalog([],rows,[],private_evidence={},
                                     alzura_source_sha256='a'*64,rsu_source_sha256='b'*64)
        self.assertEqual([r['department'] for r in result['products']],['parts','parts','accessories'])
        self.assertEqual(result['products'][2]['price_status'],'review_pending')


if __name__=='__main__':
    unittest.main()
