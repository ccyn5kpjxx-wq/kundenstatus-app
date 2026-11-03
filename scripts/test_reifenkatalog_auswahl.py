"""Synthetic facets, private-view authorization, paging and actual GET route.

Application route code is AST-extracted into a disposable Flask app; no portal,
dotenv, database, real catalogue, credentials or provider network is loaded.
"""
import ast
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask, abort, jsonify, request
from reifen_bestellung import erstelle_auswahl_token, lade_auswahl_token
from reifen_suche import parse_reifen_suche, suche_reifenangebote, suche_reifenkatalog_auswahl, ReifenSucheEingabefehler
from scripts.test_tyresystem_provider import POLICY, QUERY, feed
from tyresystem_provider import TyreSystemCatalogCache, TyreSystemCatalogProvider, TyreSystemCatalogError


NOW = datetime(2026, 11, 3, 1, tzinfo=timezone.utc)
ARGS = {"groesse": "225/40 R18 92V", "saison": "winter", "qualitaet": "alle", "anzahl": "4", "montage": "nein"}


class CatalogueSelectionTest(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            blocker = patch(target, side_effect=AssertionError("External network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="real-catalogue-contract-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = NOW
        self.cache = TyreSystemCatalogCache(self.root / "catalogue", private_root=self.root, clock=lambda: self.now)

    def provider(self, **settings):
        return TyreSystemCatalogProvider(self.cache, policy=deepcopy(POLICY), clock=lambda: self.now, **settings)

    def publish(self, *rows):
        self.cache.replace(feed(*rows, received=self.now))

    def public(self, provider, **options):
        return suche_reifenangebote({**ARGS, **options}, provider, now=self.now)

    def test_optional_defaults_keep_exact_original_normalized_query_and_token_core(self):
        self.assertEqual(parse_reifen_suche(ARGS), QUERY)
        self.assertEqual(parse_reifen_suche({**ARGS, "hersteller": "", "sortierung": "preis_auf", "seite": "1"}), QUERY)
        extra = {**ARGS, "hersteller": "Alpha", "sortierung": "preis_ab", "seite": "2"}
        self.assertEqual(parse_reifen_suche(extra), {**QUERY, "hersteller": "Alpha", "sortierung": "preis_ab", "seite": 2})
        token = erstelle_auswahl_token("synthetic-only", extra, "1")
        self.assertEqual(lade_auswahl_token("synthetic-only", token)["query"], ARGS)
        for key, value in (("hersteller", "X" * 101), ("hersteller", "\t"), ("hersteller", "A\u202eB"),
                           ("hersteller", "<script>"), ("sortierung", "unknown"), ("seite", "0"),
                           ("seite", "10001"), ("seite", "01"), ("seite", True)):
            with self.subTest(key=key, value=value), self.assertRaises(ReifenSucheEingabefehler) as caught:
                parse_reifen_suche({**ARGS, key: value})
            self.assertEqual(caught.exception.feld, key)

    def test_private_callback_is_strict_does_not_change_publication_or_touch_cache_when_denied(self):
        for callback in (None, True, lambda: 1, lambda: "yes", lambda: False, Mock(side_effect=RuntimeError("PRIVATE"))):
            with self.subTest(callback=type(callback).__name__), patch.object(self.cache, "load") as load:
                provider = self.provider(private_view_authorized=callback)
                self.assertFalse(provider.publication_ready)
                self.assertEqual(provider(QUERY), {"angebote": []})
                with self.assertRaises(TyreSystemCatalogError):
                    provider.facets()
                load.assert_not_called()
        with patch.object(self.cache, "load") as load:
            provider = TyreSystemCatalogProvider(self.cache, private_view_authorized=lambda: True)
            self.assertEqual(provider(QUERY), {"angebote": []})
            load.assert_not_called()

    def test_private_read_view_has_no_implicit_refresh_and_same_fifteen_minute_ttl(self):
        self.publish({})
        provider = self.provider(private_view_authorized=lambda: True)
        self.assertFalse(provider.publication_ready)
        self.assertEqual(provider(QUERY)["gesamt"], 1)
        before = provider.facets()["preisstand"]
        self.now += timedelta(minutes=10)
        self.assertEqual(provider.facets()["preisstand"], before)
        self.now += timedelta(minutes=5)
        for operation in (lambda: provider(QUERY), provider.facets):
            with self.assertRaises(TyreSystemCatalogError) as caught:
                operation()
            self.assertEqual(caught.exception.code, "cache_stale")
        self.assertEqual(self.cache.load().snapshot.abgerufen_am, NOW)

    def test_manufacturer_filter_precedes_paging_and_all_sorts_are_global(self):
        rows = [{"Artikelnummer": str(i), "Hersteller": "Bravo" if i <= 30 else "Alpha",
                 "Profil": f"Profile {i:02}", "Einkaufspreis": f"{i},00"} for i in range(1, 47)]
        self.publish(*rows)
        provider = self.provider(private_view_authorized=lambda: True)
        body, status = self.public(provider, hersteller="Alpha", seite="2")
        self.assertEqual(status, 200)
        self.assertEqual((body["treffer_gesamt"], body["seite"], body["seiten"]), (16, 2, 2))
        self.assertEqual([p["artikel_id"] for p in body["produkte"]], ["43", "44", "45", "46"])
        self.assertTrue(all(p["hersteller"] == "Alpha" for p in body["produkte"]))
        self.assertNotIn("netto_einkauf", str(body))
        descending, _ = self.public(provider, sortierung="preis_ab")
        self.assertEqual([p["artikel_id"] for p in descending["produkte"]], [str(i) for i in range(46, 34, -1)])
        names, _ = self.public(provider, sortierung="name")
        self.assertTrue(all(p["hersteller"] == "Alpha" for p in names["produkte"]))
        empty, _ = self.public(provider, hersteller="alpha")
        self.assertEqual((empty["treffer_gesamt"], empty["produkte"]), (0, []))
        lookup = provider.lookup({**QUERY, "seite": 10000, "sortierung": "preis_ab"}, "1")
        self.assertEqual([p["artikel_id"] for p in lookup["angebote"]], ["1"])

    def test_legacy_function_provider_is_filtered_sorted_and_paged_before_public_cap(self):
        self.publish(*[{"Artikelnummer": str(i), "Hersteller": "Alpha", "Einkaufspreis": f"{i},00"} for i in range(1, 31)])
        source = self.provider(private_view_authorized=lambda: True)
        all_items = [item for page in (1, 2, 3) for item in source({**QUERY, "seite": page})["angebote"]]
        def legacy(query):
            return {"angebote": all_items}
        body, status = self.public(legacy, hersteller="Alpha", sortierung="preis_ab", seite="2")
        self.assertEqual(status, 200)
        self.assertEqual([p["artikel_id"] for p in body["produkte"]], [str(i) for i in range(18, 6, -1)])
        self.assertNotIn("treffer_gesamt", body)

    def test_facets_combine_seasons_without_ranking_cap_and_exclude_unsupported_stock(self):
        self.publish({"Hersteller": "Alpha"}, {"Hersteller": "Beta", "Saison": "s"},
                     {"Hersteller": "Alpha", "Saison": "g"},
                     {"Hersteller": "Available", "Reifenbreite": "205", "Reifenquerschnitt": "55", "Reifendurchmesser": "16", "Lagerbestand": "1"},
                     {"Hersteller": "NoStock", "Lagerbestand": "0"}, {"Hersteller": "Old", "Dot": "DOT2020"},
                     {"Hersteller": "Demo", "Demo": "Demo"}, {"Hersteller": "Half", "Reifendurchmesser": "17.5"},
                     {"Hersteller": "Aspect100", "Reifenquerschnitt": "100"})
        provider = self.provider(private_view_authorized=lambda: True)
        raw = provider.facets()
        self.assertEqual(raw["dimensionen"], [{"breite": 205, "querschnitt": 55, "zoll": 16, "saisons": ["winter"]},
                          {"breite": 225, "querschnitt": 40, "zoll": 18, "saisons": ["ganzjahr", "sommer", "winter"]}])
        self.assertEqual(raw["hersteller"], ["Alpha", "Available", "Beta"])
        body, status = suche_reifenkatalog_auswahl(provider, now=self.now)
        self.assertEqual(status, 200)
        self.assertEqual(body["dimensionen"], raw["dimensionen"])
        self.assertEqual(set(body), {"status", "code", "dimensionen", "hersteller", "preisstand", "gueltig_bis"})
        self.assertNotIn("93.0600", str(body))
        self.assertNotIn("eprel", str(body))

    def test_facet_response_is_whitelisted_and_bad_bounds_are_not_partly_accepted(self):
        self.publish({})
        raw = self.provider(private_view_authorized=lambda: True).facets()
        class Facets:
            def facets(self):
                return raw
        raw["private_key"] = "PRIVATE-MARKER"
        raw["dimensionen"][0]["raw"] = "PRIVATE-MARKER"
        body, status = suche_reifenkatalog_auswahl(Facets(), now=self.now)
        self.assertEqual(status, 200)
        self.assertNotIn("PRIVATE-MARKER", str(body))
        original = deepcopy(raw)
        for mutate in (lambda r: r["dimensionen"][0].update(zoll=True),
                       lambda r: r["dimensionen"][0].update(saisons=["not-real"]),
                       lambda r: r.update(hersteller=["bad\nbrand"]),
                       lambda r: r.update(preisstand="2026-11-03T01:00:00"),
                       lambda r: r.update(gueltig_bis=NOW.isoformat())):
            raw = deepcopy(original)
            mutate(raw)
            body, status = suche_reifenkatalog_auswahl(Facets(), now=self.now)
            self.assertEqual(status, 503)
            self.assertEqual(body["dimensionen"], [])

    def test_pagination_metadata_is_bounded_and_optional(self):
        self.publish({})
        item = self.provider(private_view_authorized=lambda: True)(QUERY)["angebote"][0]
        for meta in ({"gesamt": True}, {"gesamt": -1}, {"gesamt": 300001}, {"paginiert": True}, {"paginiert": "yes"}):
            with self.subTest(meta=meta):
                body, status = self.public(lambda _: {"angebote": [item], **meta})
                self.assertEqual((status, body["code"]), (503, "angebote_nicht_pruefbar"))


class FacetHttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = Path(__file__).resolve().parents[1] / "app.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                     and node.name in {"restrict_public_site_service", "reifenkatalog_auswahl"}]
        cls.app = Flask("synthetic-facet-route")
        cls.app.testing = True
        cls.env = {"app": cls.app, "jsonify": jsonify, "request": request, "abort": abort,
                   "is_public_site_request": lambda: True, "PUBLIC_SITE_ONLY": False,
                   "public_form_rate_limit_status": Mock(return_value=(False, 0)),
                   "record_public_form_attempt": Mock(), "REIFEN_SUCHE_RATE_LIMIT_MAX": 60,
                   "REIFEN_SUCHE_RATE_LIMIT_WINDOW_SECONDS": 60}
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), cls.env)

    def test_actual_public_get_has_no_order_side_effect_and_private_metadata_never_leaks(self):
        for target in ("socket.socket.connect", "socket.create_connection"):
            blocker = patch(target, side_effect=AssertionError("Network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        now = datetime.now(timezone.utc)
        class Provider:
            def facets(self):
                return {"dimensionen": [{"breite": 225, "querschnitt": 40, "zoll": 18, "saisons": ["winter"]}],
                        "hersteller": ["Alpha"], "preisstand": now.isoformat(),
                        "gueltig_bis": (now+timedelta(minutes=15)).isoformat(), "private": "PRIVATE-MARKER"}
        self.app.config["REIFENANGEBOT_PROVIDER"] = Provider()
        client = self.app.test_client()
        response = client.get("/api/reifenkatalog/auswahl")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["hersteller"], ["Alpha"])
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertNotIn("PRIVATE-MARKER", response.text)
        self.assertEqual(client.get("/api/reifenkatalog/auswahl?key=PRIVATE").status_code, 400)
        self.assertEqual(client.post("/api/reifenkatalog/auswahl").status_code, 404)
        self.app.config["REIFENANGEBOT_PROVIDER"] = None
        self.assertEqual(client.get("/api/reifenkatalog/auswahl").json["code"], "katalog_nicht_bereit")
        with patch.dict(self.env, {"public_form_rate_limit_status": lambda *args, **kwargs: (True, 10)}):
            response = client.get("/api/reifenkatalog/auswahl")
            self.assertEqual(response.status_code, 429)
            self.assertEqual(response.headers["Retry-After"], "10")


if __name__ == "__main__":
    unittest.main(verbosity=2)
