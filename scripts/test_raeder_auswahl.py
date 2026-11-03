"""Product-aware selection against synthetic providers; no app/DB/dotenv/IO."""
import ast
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from itsdangerous import URLSafeTimedSerializer
from felgen_katalog import FelgenCatalogProvider, parse_felgen_feed
from raeder_auswahl import (FELGEN_TOKEN_PREFIX, FELGEN_TOKEN_SALT, RaederAuswahlNichtVerfuegbar,
                           RaederAuswahlResolver, erstelle_felgen_auswahl_token)
from reifen_suche import parse_reifen_suche
from scripts.test_felgen_katalog import POLICY, raw_feed


SECRET = "synthetic-raeder-selection-only-secret"
QUERY = {"groesse": "205/55 R16 91V", "saison": "sommer", "qualitaet": "alle", "anzahl": "4", "montage": "nein"}


class SelectionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Execute the actual existing token helper without importing its Flask demo.
        tree = ast.parse((ROOT / "reifen_bestellung.py").read_text(encoding="utf-8-sig"))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "erstelle_auswahl_token")
        namespace = {"parse_reifen_suche": parse_reifen_suche, "URLSafeTimedSerializer": URLSafeTimedSerializer,
                     "re": re, "_QUERY_FIELDS": tuple(QUERY), "_TOKEN_SALT": "reifen-auswahl-v1"}
        exec(compile(ast.Module(body=[node], type_ignores=[]), "existing-tire-token-helper", "exec"), namespace)
        cls._legacy_token = staticmethod(namespace["erstelle_auswahl_token"])

    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            block = patch(target, side_effect=AssertionError("External network forbidden"))
            block.start()
            self.addCleanup(block.stop)
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.snapshot = parse_felgen_feed(raw_feed(), abgerufen_am=self.now)
        self.cache = SimpleNamespace(load=Mock(side_effect=lambda: self.snapshot))
        self.rims = FelgenCatalogProvider(self.cache, policy=deepcopy(POLICY), private_view_authorized=lambda: True,
                                         clock=lambda: self.now)
        self.tires = Mock(side_effect=lambda _: {"angebote": [self.tire()]})
        self.tires.lookup = None  # Real legacy functions have no lookup attribute.
        self.config = {"FELGENANGEBOT_PROVIDER": self.rims, "REIFENANGEBOT_PROVIDER": self.tires,
                       "REIFEN_KAUF_ENABLED": False, "REIFEN_KAUF_MODE": "off"}
        self.resolver = RaederAuswahlResolver(self.config, SECRET, clock=lambda: self.now)

    def tire(self, **changes):
        value = {"artikel_id": "1", "ean": "4000000000042", "bezeichnung": "Synthetic tire",
                 "breite": 205, "querschnitt": 55, "zoll": 16, "lastindex": 91, "geschwindigkeitsindex": "V",
                 "saison": "sommer", "qualitaet": None, "netto_einkauf": "50.00", "fracht_netto": "0.00",
                 "waehrung": "EUR", "bestand": 8, "preis_bestaetigt": True,
                 "abgerufen_am": self.now.isoformat(), "gueltig_bis": (self.now+timedelta(minutes=15)).isoformat(),
                 "lieferzeit": "Unconfirmed local synthetic delivery text", "montage_standard_bestaetigt": True}
        return {**value, **changes}

    def rim_token(self, sku="1", quantity=4):
        return erstelle_felgen_auswahl_token(SECRET, sku, quantity, clock=lambda: self.now)

    def old_tire_token(self, *args):
        with patch("itsdangerous.timed.TimestampSigner.get_timestamp", return_value=int(self.now.timestamp())):
            return self._legacy_token(*args)

    def expect_failure(self, invoke, code=None):
        with self.assertRaises(RaederAuswahlNichtVerfuegbar) as caught:
            invoke()
        if code is not None:
            self.assertEqual(caught.exception.code, code)
        self.assertNotIn("PRIVATE", str(caught.exception))
        return caught.exception

    def test_existing_real_tire_helper_tokens_keep_query_and_exact_server_price(self):
        token = self.old_tire_token(SECRET, dict(QUERY), "1")
        result = self.resolver.from_token(token)
        self.assertEqual(result.mutable_query(), QUERY)
        self.assertEqual(dict(result.core), {"produktart": "reifen", "artikel_id": "1", "anzahl": 4, "montage": False})
        self.assertEqual(result.produkt["preis"]["gesamt_brutto"], "261.80")
        self.assertNotIn("netto_einkauf", json.dumps(result.as_dict()))
        self.assertNotIn("produktart", result.query)
        self.cache.load.assert_not_called()

    def test_rim_purchase_token_is_distinct_and_does_not_invent_tire_or_fitment_fields(self):
        token = self.rim_token()
        self.assertTrue(token.startswith("felgen."))
        result = self.resolver.from_token(token, produktart="felgen")
        self.assertEqual(dict(result.core), {"produktart": "felgen", "artikel_id": "1", "anzahl": 4})
        self.assertEqual(result.mutable_query(), {"produktart": "felgen", "anzahl": 4})
        self.assertIsNone(result.montage)
        for key in ("groesse", "saison", "montage", "fahrzeug", "abe"):
            self.assertNotIn(key, result.produkt)
        self.assertNotIn("montage_brutto", result.produkt["preis"])
        self.assertEqual(result.produkt["kennungspassung"], "nicht_geprueft")
        self.assertEqual(result.produkt["preis"]["gesamt_brutto"], "523.60")
        self.tires.assert_not_called()

    def test_tire_mounting_uses_confirmed_search_calculation_and_keeps_the_original_query(self):
        query = {**QUERY, "montage": "ja"}
        result = self.resolver.from_token(self.old_tire_token(SECRET, query, "1"))
        self.assertTrue(result.montage)
        self.assertIs(result.core["montage"], True)
        self.assertEqual(result.mutable_query(), query)
        self.assertGreater(float(result.produkt["preis"]["montage_brutto"]), 0)
        self.tires.side_effect = lambda _: {"angebote": [self.tire(montage_standard_bestaetigt=False)]}
        self.expect_failure(lambda: self.resolver.refresh_owned(query, "1"))

    def test_future_tampered_and_malformed_initial_tokens_do_not_invoke_providers(self):
        future = erstelle_felgen_auswahl_token(SECRET, "1", 4, clock=lambda: self.now+timedelta(minutes=1))
        for token in (future, "felgen.", "felgen.PRIVATE-invalid", "felgen."+"X"*2000, None):
            self.expect_failure(lambda: self.resolver.from_token(token, produktart="felgen"), "auswahl_ungueltig")
        for sku, quantity in (("01", 4), ("../PRIVATE", 4), ("1", True), ("1", "4"), ("1", 9)):
            self.expect_failure(lambda: erstelle_felgen_auswahl_token(SECRET, sku, quantity))
        self.tires.assert_not_called()
        self.cache.load.assert_not_called()

    def test_types_same_sku_and_salt_prefix_collisions_are_rejected_before_provider_io(self):
        tire = self.old_tire_token(SECRET, dict(QUERY), "1")
        rim = self.rim_token()
        readonly = URLSafeTimedSerializer(SECRET, salt="felgen-auswahl-v1").dumps({"artikel_id": "1", "anzahl": 4})
        wrong_legacy_type = URLSafeTimedSerializer(SECRET, salt="reifen-auswahl-v1").dumps(
            {"query": {"produktart": "felgen", "anzahl": 4}, "artikel_id": "1"})
        cases = [(rim, "reifen"), (rim.removeprefix("felgen."), "felgen"), (tire, "felgen"),
                 ("felgen."+tire, "felgen"), ("felgen."+readonly, "felgen"), (readonly, "felgen"),
                 (wrong_legacy_type, "reifen")]
        for token, kind in cases:
            with self.subTest(kind=kind):
                self.expect_failure(lambda: self.resolver.from_token(token, produktart=kind), "auswahl_ungueltig")
        self.tires.assert_not_called()
        self.cache.load.assert_not_called()
        self.assertNotEqual(self.resolver.quote_identity(self.resolver.from_token(tire))["core"],
                            self.resolver.quote_identity(self.resolver.from_token(rim, produktart="felgen"))["core"])

    def test_client_price_quantity_and_category_hacks_cannot_change_authoritative_selection(self):
        serializer = URLSafeTimedSerializer(SECRET, salt=FELGEN_TOKEN_SALT)
        base = {"produktart": "felgen", "artikel_id": "1", "anzahl": 4}
        invalid = [{**base, "preis": "0.01"}, {**base, "produktart": "reifen"}, {**base, "anzahl": True},
                   {**base, "anzahl": "4"}, {**base, "anzahl": 0}, {**base, "anzahl": 9},
                   {**base, "artikel_id": "../PRIVATE"}, {**base, "montage": True}]
        for payload in invalid:
            token = FELGEN_TOKEN_PREFIX+serializer.dumps(payload)
            self.expect_failure(lambda: self.resolver.from_token(token, produktart="felgen"))
        signed = self.rim_token()
        body, signature = signed.rsplit(".", 1)
        tampered = body+"."+("A" if signature[0] != "A" else "B")+signature[1:]
        self.expect_failure(lambda: self.resolver.from_token(tampered, produktart="felgen"))
        self.cache.load.assert_not_called()
        self.tires.assert_not_called()

    def test_expired_initial_tokens_fail_but_owned_draft_can_refresh_real_current_data(self):
        tire_token, rim_token = self.old_tire_token(SECRET, dict(QUERY), "1"), self.rim_token()
        self.now += timedelta(minutes=16)
        self.snapshot = parse_felgen_feed(raw_feed({"Einkaufspreis": "110,00"}), abgerufen_am=self.now)
        self.expect_failure(lambda: self.resolver.from_token(tire_token), "auswahl_ungueltig")
        self.expect_failure(lambda: self.resolver.from_token(rim_token, produktart="felgen"), "auswahl_ungueltig")
        self.cache.load.assert_not_called()
        self.tires.assert_not_called()
        refreshed = self.resolver.refresh_owned({"produktart": "felgen", "anzahl": 4}, "1")
        self.assertEqual(refreshed.produkt["preis"]["gesamt_brutto"], "575.96")
        self.assertEqual(self.resolver.refresh_owned(QUERY, "1").produktart, "reifen")

    def test_deep_immutable_result_and_independent_json_exports_cannot_mutate_quote(self):
        result = self.resolver.from_token(self.rim_token(), produktart="felgen")
        with self.assertRaises(FrozenInstanceError):
            result.produktart = "reifen"
        for operation in (lambda: result.produkt["preis"].__setitem__("gesamt_brutto", "0.01"),
                          lambda: result.query.__setitem__("anzahl", 1), lambda: result.core.__setitem__("produktart", "reifen")):
            with self.assertRaises((TypeError, AttributeError)):
                operation()
        exported = result.as_dict()
        exported["produkt"]["preis"]["gesamt_brutto"] = "0.01"
        exported["query"]["anzahl"] = 1
        self.assertEqual(result.produkt["preis"]["gesamt_brutto"], "523.60")
        self.assertEqual(result.anzahl, 4)
        self.assertEqual(json.loads(json.dumps(result.as_dict()))["produktart"], "felgen")

    def test_quote_binding_changes_with_price_or_quantity_but_not_receipt_or_stock(self):
        old = self.resolver.from_token(self.rim_token(), produktart="felgen")
        binding = self.resolver.quote_identity(old)
        self.now += timedelta(minutes=1)
        self.snapshot = parse_felgen_feed(raw_feed({"Lagerbestand": "7"}), abgerufen_am=self.now)
        fresh = self.resolver.refresh_owned(old.query, old.artikel_id)
        self.assertEqual(self.resolver.quote_identity(fresh), binding)
        self.snapshot = parse_felgen_feed(raw_feed({"Einkaufspreis": "110,00"}), abgerufen_am=self.now)
        changed = self.resolver.refresh_owned(old.query, old.artikel_id)
        self.assertNotEqual(self.resolver.quote_identity(changed), binding)
        self.assertNotEqual(self.resolver.quote_identity(self.resolver.refresh_owned({"produktart": "felgen", "anzahl": 1}, "1")), binding)
        self.assertEqual(changed.produkt["preis"]["gesamt_brutto"], "575.96")
        json.dumps(binding)

    def test_exact_sku_revalidation_survives_ranking_change_for_both_provider_contracts(self):
        self.snapshot = parse_felgen_feed(raw_feed({"Einkaufspreis": "110,00"},
            *({"Artikelnummer": str(i), "Einkaufspreis": "99,00"} for i in range(2, 42))), abgerufen_am=self.now)
        self.assertNotIn("1", [item["artikel_id"] for item in self.rims.search({})["produkte"]])
        self.assertEqual(self.resolver.from_token(self.rim_token(), produktart="felgen").artikel_id, "1")
        rows = [self.tire(artikel_id=str(i), netto_einkauf="40.00") for i in range(2, 42)]+[self.tire()]
        self.tires.side_effect = lambda _: {"angebote": rows}
        self.assertEqual(self.resolver.from_token(self.old_tire_token(SECRET, dict(QUERY), "1")).artikel_id, "1")
        class Lookup:
            def __call__(_self, _query):
                raise AssertionError("Ranking search must not be used for exact SKU")
            def lookup(_self, query, sku):
                return {"angebote": [self.tire(artikel_id=sku)]}
        self.config["REIFENANGEBOT_PROVIDER"] = Lookup()
        self.assertEqual(self.resolver.refresh_owned(QUERY, "1").artikel_id, "1")

    def test_stale_missing_stock_and_incomplete_catalogue_remain_unavailable(self):
        self.snapshot = parse_felgen_feed(raw_feed({"Lagerbestand": "3"}), abgerufen_am=self.now)
        self.expect_failure(lambda: self.resolver.from_token(self.rim_token(), produktart="felgen"), "artikel_nicht_verfuegbar")
        self.snapshot = parse_felgen_feed(raw_feed(), abgerufen_am=self.now-timedelta(minutes=16))
        self.expect_failure(lambda: self.resolver.refresh_owned({"produktart": "felgen", "anzahl": 4}, "1"), "katalog_veraltet")
        self.config["FELGENANGEBOT_PROVIDER"] = lambda _: {"PRIVATE": True}
        self.expect_failure(lambda: self.resolver.refresh_owned({"produktart": "felgen", "anzahl": 4}, "1"), "katalog_nicht_bereit")
        self.config["REIFENANGEBOT_PROVIDER"] = None
        self.expect_failure(lambda: self.resolver.refresh_owned(QUERY, "1"), "katalog_nicht_bereit")

    def test_owned_query_cannot_smuggle_prices_filters_or_tire_values_into_rims(self):
        for query in ({**QUERY, "preis": "0.01"}, {**QUERY, "produktart": "felgen"},
                      {"produktart": "felgen", "anzahl": "4"}, {"produktart": "felgen", "anzahl": 4, "montage": False},
                      {"produktart": "felgen", "anzahl": 4, "groesse": "205/55 R16"}, {**QUERY, "seite": "2"}):
            self.expect_failure(lambda: self.resolver.refresh_owned(query, "1"))
        self.tires.assert_not_called()
        self.cache.load.assert_not_called()

    def test_rim_public_model_drops_raw_fields_and_rejects_inconsistent_prices_or_fitment(self):
        actual = self.rims.lookup("1")
        exposed = {**actual, "netto_einkauf": "PRIVATE", "source_url": "https://PRIVATE.invalid"}
        with patch.object(self.rims, "lookup", return_value=exposed):
            result = self.resolver.refresh_owned({"produktart": "felgen", "anzahl": 4}, "1")
        self.assertNotIn("PRIVATE", json.dumps(result.as_dict()))
        invalid = [{**actual, "artikel_id": "2"}, {**actual, "anzahl": 1}, {**actual, "kennungspassung": "geprueft"},
                   {**actual, "preis": {**actual["preis"], "gesamt_brutto": "0.01"}},
                   {**actual, "preis": {**actual["preis"], "einzel_brutto": 130.90}}]
        for raw in invalid:
            with patch.object(self.rims, "lookup", return_value=raw):
                self.expect_failure(lambda: self.resolver.refresh_owned({"produktart": "felgen", "anzahl": 4}, "1"))

    def test_unknown_provider_errors_are_generic_and_never_change_purchase_gates(self):
        before = dict(self.config)
        self.cache.load.side_effect = RuntimeError("PRIVATE credentials and provider body")
        self.expect_failure(lambda: self.resolver.refresh_owned({"produktart": "felgen", "anzahl": 4}, "1"), "katalog_nicht_verfuegbar")
        self.tires.side_effect = RuntimeError("PRIVATE credentials and provider body")
        self.expect_failure(lambda: self.resolver.refresh_owned(QUERY, "1"), "katalog_nicht_verfuegbar")
        self.assertEqual(self.config, before)
        self.assertNotIn(SECRET, repr(self.resolver))

    def test_complete_wheels_and_unknown_types_cannot_be_created_from_parts_or_generic_functions(self):
        fake = Mock(return_value={"bestaetigt": True, "teile": [], "gesamt_brutto": "100.00"})
        self.config["KOMPLETTRADANGEBOT_PROVIDER"] = fake
        for kind, code in (("komplettraeder", "komplettrad_nicht_konfiguriert"), ("Felgen", "produktart_ungueltig"),
                           (None, "produktart_ungueltig"), (True, "produktart_ungueltig")):
            self.expect_failure(lambda: self.resolver.from_token(self.rim_token(), produktart=kind), code)
            self.expect_failure(lambda: self.resolver.refresh_owned({"produktart": kind, "anzahl": 4}, "1"), code)
        fake.assert_not_called()
        self.tires.assert_not_called()
        self.cache.load.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
