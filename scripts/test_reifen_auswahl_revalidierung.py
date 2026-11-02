"""Selected-SKU regression with real search, signed tokens and synthetic cache.

No application/dotenv/database import. Every external socket is blocked and
catalogue writes stay in a disposable private directory.
"""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reifen_bestellung import ReifenBestellung, erstelle_auswahl_token
from reifen_kauf import ReifenKauf
from reifen_suche import ReifenSucheEingabefehler, suche_reifenangebote
from scripts.test_tyresystem_provider import NOW, POLICY, QUERY, feed
from tyresystem_provider import TyreSystemCatalogCache, TyreSystemCatalogProvider


ARGS = {"groesse": "225/40 R18 92V", "saison": "winter", "qualitaet": "alle",
        "anzahl": "4", "montage": "nein"}
SKU = "999"


def offer(**changes):
    item = {"artikel_id": SKU, "bezeichnung": "Synthetic selected tyre", "ean": None,
            "breite": 225, "querschnitt": 40, "zoll": 18, "lastindex": 92,
            "geschwindigkeitsindex": "V", "saison": "winter", "qualitaet": None,
            "netto_einkauf": "100.00", "fracht_netto": "0.00", "waehrung": "EUR",
            "bestand": 8, "preis_bestaetigt": True, "abgerufen_am": NOW.isoformat(),
            "gueltig_bis": (NOW + timedelta(minutes=15)).isoformat(), "lieferzeit": "Ohne Reservierung.",
            "runflat": False, "montage_standard_bestaetigt": True}
    item.update(changes)
    return item


class SelectionRevalidationTest(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex",
                       "socket.create_connection", "socket.getaddrinfo"):
            blocker = patch(target, side_effect=AssertionError("External network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="tyre-selection-offline-")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.now = NOW
        self.cache = TyreSystemCatalogCache(root / "catalogue", private_root=root, clock=lambda: self.now)
        self.secret = "synthetic-selection-test-only"
        self.token = erstelle_auswahl_token(self.secret, ARGS, SKU)

    def search(self, provider, **kwargs):
        return suche_reifenangebote(ARGS, provider, now=self.now, **kwargs)

    def selected_in_both_checkouts(self, provider):
        service = SimpleNamespace(app=SimpleNamespace(secret_key=self.secret,
            config={"REIFENANGEBOT_PROVIDER": provider}), now=lambda: self.now)
        for checkout in (ReifenKauf, ReifenBestellung):
            with self.subTest(checkout=checkout.__name__):
                query, product = checkout.fresh(service, self.token)
                self.assertEqual(query, ARGS)
                self.assertEqual(product["artikel_id"], SKU)
                self.assertEqual(product["preis"]["einzel_brutto"], "130.90")

    def catalogue_provider(self):
        return TyreSystemCatalogProvider(self.cache, publication_approved=True,
            publication_reference="synthetic-approved-release", policy=deepcopy(POLICY), clock=lambda: self.now)

    def test_existing_function_provider_keeps_selection_after_rank_drops_to_100(self):
        offers = [offer()]
        def provider(query):
            self.assertEqual(query, QUERY)
            return {"angebote": list(offers)}
        self.selected_in_both_checkouts(provider)
        offers[:] = [offer(artikel_id=str(i), netto_einkauf="99.00") for i in range(1, 100)] + [offer()]
        public, status = self.search(provider)
        self.assertEqual(status, 200)
        self.assertEqual(len(public["produkte"]), 12)
        self.assertNotIn(SKU, [p["artikel_id"] for p in public["produkte"]])
        self.selected_in_both_checkouts(provider)

    def test_catalogue_selection_beyond_provider_and_public_caps(self):
        rows = [{"Artikelnummer": str(i), "Einkaufspreis": "99,00"} for i in range(1, 122)]
        self.cache.replace(feed(*rows, {"Artikelnummer": SKU, "Einkaufspreis": "100,00"}))
        provider = self.catalogue_provider()
        self.assertTrue(provider.publication_ready)
        self.assertEqual(len(provider(QUERY)["angebote"]), 100)
        self.assertNotIn(SKU, [p["artikel_id"] for p in provider(QUERY)["angebote"]])
        self.assertEqual([p["artikel_id"] for p in provider.lookup(QUERY, SKU)["angebote"]], [SKU])
        public, status = self.search(provider, artikel_id=SKU)
        self.assertEqual(status, 200)
        self.assertEqual(len(public["produkte"]), 1)
        self.selected_in_both_checkouts(provider)

    def test_lookup_only_exposes_exact_validated_sku_and_current_price(self):
        class Provider:
            def __call__(self, query):
                raise AssertionError("Search fallback must not be called")
            def lookup(self, query, sku):
                self.query, self.sku = query, sku
                return {"angebote": [offer(artikel_id="123", netto_einkauf="1.00"),
                                     offer(netto_einkauf="120.00", internal_secret="PRIVATE-MARKER")]}
        provider = Provider()
        result, status = self.search(provider, artikel_id=SKU)
        self.assertEqual((provider.query, provider.sku), (QUERY, SKU))
        self.assertEqual(status, 200)
        self.assertEqual([p["artikel_id"] for p in result["produkte"]], [SKU])
        self.assertEqual(result["produkte"][0]["preis"]["einzel_brutto"], "157.08")
        self.assertNotIn("PRIVATE-MARKER", str(result))
        self.assertNotIn("netto_einkauf", str(result))

    def test_lookup_errors_never_fall_back_or_disclose_provider_details(self):
        class Provider:
            def __call__(self, query):
                raise AssertionError("No automatic fallback")
            def lookup(self, query, sku):
                raise RuntimeError("PRIVATE-MARKER https://private.invalid/?key=secret")
        result, status = self.search(Provider(), artikel_id=SKU)
        self.assertEqual(status, 503)
        self.assertEqual(result["produkte"], [])
        self.assertNotIn("PRIVATE-MARKER", str(result))

    def test_targeted_result_still_needs_all_normal_offer_checks(self):
        for changes in ({"bestand": 3}, {"breite": 205}, {"saison": "sommer"},
                        {"preis_bestaetigt": False}, {"fracht_netto": None},
                        {"abgerufen_am": (NOW-timedelta(minutes=16)).isoformat()},
                        {"gueltig_bis": NOW.isoformat()}, {"qualitaet": "alle"}):
            with self.subTest(changes=changes):
                class Provider:
                    def __call__(self, query):
                        raise AssertionError("No search fallback")
                    def lookup(self, query, sku):
                        return {"angebote": [offer(**changes)]}
                result, status = self.search(Provider(), artikel_id=SKU)
                self.assertEqual(status, 200)
                self.assertEqual(result["produkte"], [])

    def test_catalogue_lookup_keeps_stock_dimensions_metadata_and_age_gates(self):
        self.cache.replace(feed({"Artikelnummer": SKU, "Lagerbestand": "3"}))
        provider = self.catalogue_provider()
        self.assertEqual(provider.lookup(QUERY, SKU), {"angebote": []})
        self.cache.replace(feed({"Artikelnummer": SKU}))
        self.assertEqual(provider.lookup({**QUERY, "breite": 205}, SKU), {"angebote": []})
        self.assertEqual(provider.lookup({**QUERY, "qualitaet": "premium"}, SKU), {"angebote": []})
        self.assertEqual(provider.lookup({**QUERY, "montage": True}, SKU), {"angebote": []})
        self.now += timedelta(minutes=15)
        result, status = self.search(provider, artikel_id=SKU)
        self.assertEqual(status, 503)
        self.assertEqual(result["produkte"], [])

    def test_unreleased_lookup_never_reads_cache(self):
        with patch.object(self.cache, "load") as load:
            for settings in ({}, {"publication_approved": True, "publication_reference": "test"},
                             {"policy": POLICY}):
                provider = TyreSystemCatalogProvider(self.cache, **settings)
                self.assertFalse(provider.publication_ready)
                self.assertEqual(provider.lookup(QUERY, SKU), {"angebote": []})
            load.assert_not_called()

    def test_internal_sku_never_becomes_a_public_query_field(self):
        calls = []
        def provider(query):
            calls.append(query)
            return {"angebote": [offer()]}
        with self.assertRaises(ReifenSucheEingabefehler):
            suche_reifenangebote({**ARGS, "artikel_id": SKU}, provider, now=NOW)
        for sku in ("", "bad\nvalue", "X" * 101, 999, True):
            with self.subTest(sku=sku), self.assertRaises(ReifenSucheEingabefehler):
                self.search(provider, artikel_id=sku)
        self.assertEqual(calls, [])

    def test_lookup_response_bounds_still_apply(self):
        class Provider:
            def __call__(self, query):
                raise AssertionError("No search fallback")
            def lookup(self, query, sku):
                return {"angebote": [offer()] * 101}
        result, status = self.search(Provider(), artikel_id=SKU)
        self.assertEqual(status, 503)
        self.assertEqual(result["produkte"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
