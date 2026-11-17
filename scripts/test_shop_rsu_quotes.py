"""Injected RSU Inquiry only: synthetic identities/prices, no HTTP or secrets."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import json
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shop_cart_quotes import CartQuoteService, retail_unit_gross_cent
from shop_rsu_match import alzura_record, match_catalogues, rsu_record
from shop_rsu_quotes import RSUQuoteError, RSUQuotePolicy, RSUQuoteProvider, XMLInquiryTransport
from tyresystem_api import ArticleInquiry

NOW = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
SID = "tyre:" + "1" * 24
IDENTITY = "c" * 64
EAN = "4019238594195"


def bindings():
    a = dict(id="123456", brandname="Example", manufacturer_number="EX-205/16", width="205", height="55",
             inner_diameter="16", tyre_type="R", load_index="91", speed_index="V", type="S", dot="0",
             demo="0", da_decke="0", runflat="0", ms="0", **{"3pmfs": "0"})
    r = dict(Artikelnummer="654321", Hersteller="EXAMPLE", Herstellerartikelnummer="EX-205/16",
             Reifenbreite="205", Reifenquerschnitt="55", Reifendurchmesser="16", Bauart="R", Loadindex="91",
             Speedindex="V", Saison="s", Artikeltyp="Reifen", Artikeluntertyp="PKW", Dot="", Demo="", Da="")
    candidate = match_catalogues([alzura_record(a, archive_sha256="a" * 64, row_number=1)],
                                 [rsu_record(r, csv_sha256="b" * 64, row_number=2)])["mappings"][0]
    return {SID: {"identity_hash": IDENTITY, "category": "tyre", "mapping": candidate, "expected_ean": EAN}}


def product():
    return {"id": SID, "identity_hash": IDENTITY, "category": "tyre"}


class FakeInquiry:
    def __init__(self, clock):
        self.clock, self.calls, self.changes, self.fail = clock, [], {}, False

    def inquire(self, identifier, quantity):
        self.calls.append((identifier, quantity))
        if self.fail:
            raise RuntimeError("SENSITIVE_PROVIDER_BODY_PASSWORD_and_price")
        value = ArticleInquiry(identifier, EAN, quantity, {1: Decimal("100"), 2: Decimal("95"),
                               4: Decimal("90")}.get(quantity, Decimal("100")), 20, "Example tyre",
                               self.clock(), "test")
        return replace(value, **self.changes)


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.now = NOW
        self.inquiry = FakeInquiry(lambda: self.now)

    def provider(self, *, approved=None, policy=None, data=None, **kwargs):
        return RSUQuoteProvider(bindings=data or bindings(), approved_ids={SID} if approved is None else approved,
            policy=policy or RSUQuotePolicy("test", "EUR", True, "synthetic-RSU-net-unit-review"),
            inquiry=self.inquiry, clock=lambda: self.now, **kwargs)

    def test_approval_is_separate_from_candidates_and_identity_is_bound(self):
        data = bindings()
        self.assertFalse(data[SID]["mapping"]["confirmed"])
        denied = self.provider(approved=set(), data=data)
        self.assertIsNone(denied.rsu_mapping(product()))
        self.assertIsNone(denied.retail_quote(product(), 1))
        self.assertEqual(self.inquiry.calls, [])
        provider = self.provider(data=data)
        mapping = provider.rsu_mapping(product())
        self.assertTrue(mapping["confirmed"])
        self.assertEqual(mapping["supplier_article_id"], "654321")
        for change in ({"identity_hash": "d" * 64}, {"id": []}, {"category": "alloy_wheel"}):
            with self.subTest(change=change):
                self.assertIsNone(provider.rsu_mapping({**product(), **change}))
                self.assertIsNone(provider.retail_quote({**product(), **change}, 1))
        self.assertFalse(data[SID]["mapping"]["confirmed"])

    def test_exact_quantity_rounding_and_no_quantity_tier_inference(self):
        provider = self.provider()
        for quantity, expected in ((1, 13090), (4, 11781), (2, 12436)):
            quote = provider.retail_quote(product(), quantity)
            self.assertEqual(quote["unit_gross_cent"], expected)
            self.assertEqual(quote["quantity"], quantity)
            self.assertTrue(quote["procurement_confirmed"])
            self.assertEqual(quote["expires_at"], (NOW + timedelta(minutes=15)).isoformat())
        self.assertEqual(self.inquiry.calls, [("654321", 1), ("654321", 4), ("654321", 2)])
        quote = provider.retail_quote(product(), 1)
        encoded = json.dumps(quote)
        for secret in ("expected_ean", "supplier_article_id", "price_1", "unit_net", "provider_id"):
            self.assertNotIn(secret, encoded)

    def test_goods_markup_is_unchanged_and_shipping_is_not_charged_here(self):
        for category in ("tyre", "alloy_wheel", "steel_wheel"):
            self.assertEqual(retail_unit_gross_cent(Decimal("100"), category), 13090)
        for category in ("body_part", "oil", "care", "accessory"):
            self.assertEqual(retail_unit_gross_cent(Decimal("100"), category), 13685)
        self.assertNotIn("shipping", json.dumps(self.provider().retail_quote(product(), 1)))

    def test_policy_does_not_guess_currency_or_net_unit_semantics(self):
        for policy in (RSUQuotePolicy("test"), RSUQuotePolicy("test", "EUR")):
            with self.subTest(policy=policy):
                self.assertIsNone(self.provider(policy=policy).retail_quote(product(), 1))
        self.assertEqual(self.inquiry.calls, [])
        for policy in (RSUQuotePolicy("test", "USD", True, "review"), RSUQuotePolicy("test", "EUR", True, "")):
            with self.subTest(policy=policy), self.assertRaises(RSUQuoteError):
                self.provider(policy=policy)

    def test_response_identity_mode_time_stock_and_price_are_all_checked(self):
        changes = ({"article_id": "654322"}, {"ean": "4000000000006"}, {"amount": 2},
                   {"configured_mode": "live"}, {"stock": 0}, {"stock": True}, {"price": 100.0},
                   {"price": Decimal("-1")}, {"price": Decimal("NaN")},
                   {"retrieved_at": NOW - timedelta(seconds=1)}, {"retrieved_at": NOW + timedelta(seconds=1)},
                   {"retrieved_at": NOW.replace(tzinfo=None)})
        for change in changes:
            with self.subTest(change=change):
                self.inquiry.changes = change
                self.assertIsNone(self.provider().retail_quote(product(), 1))

    def test_private_cache_is_exact_quantity_detached_bounded_and_not_redated(self):
        provider = self.provider(max_cache_entries=1)
        quote = provider.retail_quote(product(), 1)
        quote["unit_gross_cent"] = 1
        self.assertEqual(provider.retail_quote(product(), 1)["unit_gross_cent"], 13090)
        self.assertEqual(len(self.inquiry.calls), 1)
        self.now += timedelta(minutes=15)
        renewed = provider.retail_quote(product(), 1)
        self.assertEqual(len(self.inquiry.calls), 2)
        self.assertEqual(renewed["observed_at"], self.now.isoformat())
        provider.retail_quote(product(), 2)
        self.assertEqual(len(provider._cache), 1)
        provider.retail_quote(product(), 1)
        self.assertEqual(len(self.inquiry.calls), 4)

    def test_failures_are_bounded_and_do_not_reveal_supplier_messages(self):
        self.inquiry.fail = True
        provider = self.provider()
        self.assertIsNone(provider.retail_quote(product(), 1))
        self.assertIsNone(provider.retail_quote(product(), 1))
        self.assertEqual(len(self.inquiry.calls), 1)
        self.assertNotIn("SENSITIVE", repr(provider))
        self.assertNotIn("SENSITIVE", repr(provider._cache))
        self.now += timedelta(seconds=30)
        self.assertIsNone(provider.retail_quote(product(), 1))
        self.assertEqual(len(self.inquiry.calls), 2)
        for quantity in (0, 21, True, 1.0, "1"):
            self.assertIsNone(provider.retail_quote(product(), quantity))
        self.assertEqual(len(self.inquiry.calls), 2)

    def test_mapping_proofs_and_expected_ean_cannot_be_modified(self):
        mutations = [lambda d: d[SID].update(expected_ean="4019238594194"),
                     lambda d: d[SID]["mapping"]["evidence"]["rsu"].update(article_id="654322"),
                     lambda d: d[SID]["mapping"].update(evidence_id="d" * 64),
                     lambda d: d[SID]["mapping"]["evidence"]["alzura"].update(source_sha256="d" * 64)]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                value = bindings()
                mutation(value)
                with self.assertRaisesRegex(RSUQuoteError, "invalid_mapping_binding"):
                    self.provider(data=value)
        self.assertEqual(self.inquiry.calls, [])

    def test_xml_transport_is_get_only_guarded_lazy_and_sets_accept(self):
        calls = []
        class Session:
            trust_env = True
            cookies = SimpleNamespace(clear=lambda: calls.append("clear"))
            def get(self, url, **kwargs):
                calls.append((url, kwargs))
                return "synthetic-response"
            def close(self):
                calls.append("close")
        transport = XMLInquiryTransport(session_factory=lambda: Session())
        self.assertEqual(calls, [])
        self.assertFalse(hasattr(transport, "post"))
        for url in ("https://api.tyresystem.de/Rest/Order", "https://evil.invalid/Rest/Inquiry/654321/1",
                    "https://api.tyresystem.de/Rest/Inquiry/654321/21"):
            with self.subTest(url=url), self.assertRaises(RSUQuoteError):
                transport.get(url, verify=True, allow_redirects=False)
        self.assertEqual(calls, [])
        self.assertEqual(transport.get("https://api.tyresystem.de/Rest/Inquiry/654321/1",
                                      verify=True, allow_redirects=False, headers={"Accept": "application/json"}),
                         "synthetic-response")
        self.assertEqual(calls[-1][1]["headers"]["Accept"], "application/xml, text/xml")
        self.assertFalse(transport._session.trust_env)
        transport.close()

    def test_factory_constructs_real_client_without_network_and_guards_ids(self):
        provider = RSUQuoteProvider.from_config(config={"TYRESYSTEM_API_USERNAME": "synthetic-user",
                "TYRESYSTEM_API_PASSWORD": "synthetic-password", "TYRESYSTEM_API_MODE": "test"},
            bindings=bindings(), approved_ids={SID},
            policy=RSUQuotePolicy("test", "EUR", True, "synthetic-review"), clock=lambda: self.now)
        self.assertTrue(provider.rsu_mapping(product())["confirmed"])
        self.assertTrue(provider._inquiry._authorize("654321", 20))
        self.assertFalse(provider._inquiry._authorize("654322", 1))
        self.assertFalse(provider._inquiry._authorize("654321", 21))
        self.assertIsNone(provider._inquiry._http._session)

    def frozen_item(self,provider):
        return {**product(),'mapping':provider.rsu_mapping(product())}

    def test_fresh_private_inquiry_is_exact_bound_and_never_uses_retail_cache(self):
        provider=self.provider()
        provider.retail_quote(product(),1)
        item=self.frozen_item(provider)
        for quantity in (1,2,1):
            result=provider.fresh_inquiry(item,quantity)
            self.assertTrue(result['confirmed'])
            self.assertEqual(result['quantity'],quantity)
            self.assertEqual(result['supplier_article_id'],'654321')
            self.assertEqual(result['mapping_evidence_id'],item['mapping']['evidence_id'])
            self.assertEqual((result['expected_ean'],result['ean']),(EAN,EAN))
            self.assertEqual(result['currency'],'EUR')
            self.assertEqual(result['unit_net'],'95' if quantity==2 else '100')
            self.assertEqual(result['observed_at'],NOW.isoformat())
            self.assertEqual(result['expires_at'],(NOW+timedelta(minutes=15)).isoformat())
            result['unit_net']='MUTATED'
        self.assertEqual(self.inquiry.calls,[('654321',1),('654321',1),('654321',2),('654321',1)])

    def test_fresh_private_inquiry_rejects_unapproved_changed_evidence_before_get(self):
        provider=self.provider()
        item=self.frozen_item(provider)
        mutations=({'identity_hash':'f'*64},{'id':'tyre:'+'f'*24},
                   {'mapping':{**item['mapping'],'evidence_id':'wrong'}},
                   {'mapping':{**item['mapping'],'supplier_article_id':'123'}},{'mapping':None})
        for change in mutations:
            self.assertIsNone(provider.fresh_inquiry({**item,**change},1))
        for quantity in (0,21,True,'1',1.0):self.assertIsNone(provider.fresh_inquiry(item,quantity))
        self.assertIsNone(self.provider(approved=set()).fresh_inquiry(item,1))
        self.assertIsNone(self.provider(policy=RSUQuotePolicy('test')).fresh_inquiry(item,1))
        self.assertEqual(self.inquiry.calls,[])

    def test_fresh_private_inquiry_rejects_stale_stock_and_response_identity(self):
        provider=self.provider()
        item=self.frozen_item(provider)
        for change in ({'article_id':'654322'},{'ean':'4000000000006'},{'amount':2},{'configured_mode':'live'},
                       {'stock':0},{'stock':True},{'price':Decimal('NaN')},{'price':Decimal('-1')},{'price':100.0},
                       {'retrieved_at':NOW-timedelta(seconds=1)},{'retrieved_at':NOW+timedelta(seconds=1)},
                       {'retrieved_at':NOW.replace(tzinfo=None)}):
            with self.subTest(change=change):
                self.inquiry.changes=change
                self.assertIsNone(provider.fresh_inquiry(item,1))
        self.inquiry.changes={}
        old=self.inquiry.inquire
        def slow(identifier,quantity):
            result=old(identifier,quantity)
            self.now+=timedelta(minutes=15)
            return result
        self.inquiry.inquire=slow
        self.assertIsNone(provider.fresh_inquiry(item,1))

    def integrated_cart(self, *, freight_delay):
        provider = self.provider()
        old_inquire = self.inquiry.inquire
        def slow_inquiry(identifier, quantity):
            self.now += timedelta(seconds=2)
            return old_inquire(identifier, quantity)
        self.inquiry.inquire = slow_inquiry
        def catalogue(identifier):
            return {**product(), 'name': 'Synthetic exact tyre', 'fitment_required': True,
                    'fitment': {'confirmed': True, 'identity_hash': IDENTITY,
                                'vehicle_ref': 'CAR-1', 'evidence_id': 'synthetic-fitment'},
                    'diameter': 16, 'tyre_size': '205/55 R16 91V',
                    'required_fees_confirmed': True, 'required_fees': []}
        def freight(shipment, context):
            self.now += freight_delay
            return {'confirmed': True, 'mode': 'test', 'currency': 'eur', 'vat_percent': '19.00',
                    'observed_at': self.now.isoformat(), 'expires_at': (self.now + timedelta(minutes=15)).isoformat(),
                    'shipment_hash': shipment['shipment_hash'], 'context_hash': shipment['context_hash'], 'gross_cent': 0}
        service = CartQuoteService(catalogue_lookup=catalogue, rsu_mapping=provider.rsu_mapping,
            retail_quote=provider.retail_quote, freight_quote=freight, clock=lambda: self.now)
        return service.quote({'lines': [{'id': SID, 'quantity': 1}]}, customer_id='synthetic', vehicle_ref='CAR-1')

    def test_real_adapter_contract_with_slow_inquiry_preserves_actual_observation(self):
        quote = self.integrated_cart(freight_delay=timedelta(seconds=3)).public()
        self.assertTrue(quote['ready'])
        self.assertEqual(quote['created_at'], (NOW + timedelta(seconds=5)).isoformat())
        self.assertEqual(quote['expires_at'], (NOW + timedelta(minutes=15)).isoformat())
        self.assertEqual(self.inquiry.calls, [('654321', 1)])
        self.assertEqual(quote['total_gross_cent'], 13090)

    def test_slow_later_freight_cannot_pay_for_expired_inquiry_or_fetch_new_price(self):
        quote = self.integrated_cart(freight_delay=timedelta(minutes=15)).public()
        self.assertFalse(quote['ready'])
        self.assertIsNone(quote['total_gross_cent'])
        self.assertIn('quote_expired', {reason['code'] for reason in quote['blocked_reasons']})
        self.assertEqual(self.inquiry.calls, [('654321', 1)])


if __name__ == "__main__":
    unittest.main()
