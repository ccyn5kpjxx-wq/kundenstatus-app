"""Synthetic sourced-grade/store/filter regressions. No portal or real sources."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reifen_bewertungen import ReifenBewertungsStore, public_test_rating
from reifen_bestellung import erstelle_auswahl_token, lade_auswahl_token
from reifen_suche import parse_reifen_suche, suche_reifenangebote, ReifenSucheEingabefehler
from scripts.test_tyresystem_provider import NOW, POLICY, QUERY, feed
from tyresystem_provider import IndexedCatalog, TyreSystemCatalogProvider

ARGS = {"groesse": "225/40 R18 92V", "saison": "winter", "qualitaet": "alle", "anzahl": "4", "montage": "nein"}
GRADE = {"quelle": "ADAC", "note": "2.3", "urteil": "gut", "jahr": 2021,
         "testgroesse": "225/40 R18 92V", "quelle_url": "https://www.adac.de/rund-ums-fahrzeug/ausstattung-technik-zubehoer/reifen/reifentest/winterreifen/225-40-r18/synthetic-winter-id-1/"}


class TireRatingsTest(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            blocker = patch(target, side_effect=AssertionError("External network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="tire-ratings-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "private-ledger.json"
        self.article = feed().artikel[0]
        self.record = {**GRADE, "artikel_id": "1", "hersteller": "Synthetic", "profil": "Winter",
            "breite": 225, "querschnitt": 40, "zoll": "18", "lastindex": 92, "speedindex": "V", "saison": "winter",
            "vergleichsgruppe": "synthetic-adac-winter-2021-225-40-r18", "nachweis": "Synthetic exact test fixture, checked"}
        self.ledger([self.record])

    def ledger(self, rows):
        self.path.write_text(json.dumps({"schema": 1, "ratings": rows}), encoding="utf-8")

    def store(self, **options):
        return ReifenBewertungsStore(self.path, clock=lambda: NOW, **options)

    def provider(self, rows=None, **options):
        catalog = IndexedCatalog.build(feed(*(rows or [{}])))
        class Cache:
            def load(_):
                return catalog
        return TyreSystemCatalogProvider(Cache(), policy=POLICY, clock=lambda: NOW,
            private_view_authorized=lambda: True, **options)

    def test_default_deny_and_only_true_callback_or_separate_public_release(self):
        for callback in (None, True, lambda: 1, lambda: "true", Mock(side_effect=RuntimeError("PRIVATE"))):
            self.assertIsNone(self.store(private_view_authorized=callback).describe(self.article))
        self.assertIsNone(self.store(publication_approved=True).describe(self.article))
        self.assertEqual(self.store(private_view_authorized=lambda: True).describe(self.article), GRADE)
        self.assertEqual(self.store(publication_approved=True, publication_reference="synthetic publication rights").describe(self.article), GRADE)

    def test_exact_article_model_dimension_indices_and_season_binding_without_lookup_io(self):
        store = self.store(private_view_authorized=lambda: True)
        with patch.object(Path, "open", side_effect=AssertionError("No request-time file IO")):
            self.assertEqual(store.describe(self.article), GRADE)
            for field, value in (("artikel_id", "2"), ("hersteller", "Other"), ("profil", "Winter EVO"),
                    ("breite", 215), ("querschnitt", 45), ("zoll", Decimal(17)), ("lastindex", 95),
                    ("speedindex", "W"), ("saison", "sommer")):
                self.assertIsNone(store.describe(replace(self.article, **{field: value})))
        self.assertEqual(store.describe(replace(self.article, einkaufspreis=Decimal("123"))), GRADE)

    def test_historical_note_has_no_price_ttl_but_future_year_and_inconsistent_facts_are_unknown(self):
        self.assertEqual(self.store(private_view_authorized=lambda: True).describe(self.article)["jahr"], 2021)
        for change in ({"jahr": 2027}, {"jahr": True}, {"note": "0.5"}, {"note": "5.6"}, {"note": 2.3},
                       {"note": "2.30"}, {"urteil": "sehr gut"}, {"testgroesse": "225/40 R18 95V"},
                       {"quelle": "TYREsystem"}, {"source": "PRIVATE"}):
            self.ledger([{**self.record, **change}])
            self.assertIsNone(self.store(private_view_authorized=lambda: True).describe(self.article))

    def test_private_or_malicious_source_addresses_and_extra_public_fields_are_rejected(self):
        for url in (GRADE["quelle_url"].replace("https:", "http:"),
                    GRADE["quelle_url"].replace("www.adac.de", "www.adac.de.evil.invalid"),
                    GRADE["quelle_url"]+"?token=PRIVATE", GRADE["quelle_url"]+"#PRIVATE",
                    GRADE["quelle_url"].replace("www.adac.de", "user@www.adac.de"),
                    GRADE["quelle_url"].replace("225-40-r18", "195-65-r15")):
            self.assertIsNone(public_test_rating({**GRADE, "quelle_url": url}, current_year=2026))
        self.assertIsNone(public_test_rating({**GRADE, "nachweis": "PRIVATE"}))
        self.assertIsNone(public_test_rating({**GRADE, "review_count": 10}))
        self.assertIsNone(public_test_rating(GRADE, groesse="225/40 R18 95V"))
        self.assertIsNone(public_test_rating(GRADE, saison="sommer"))

    def test_ambiguous_corrupt_oversized_and_nonprivate_ledgers_fail_closed(self):
        self.ledger([self.record, self.record])
        self.assertIsNone(self.store(private_view_authorized=lambda: True).describe(self.article))
        self.path.write_text('{"schema":1,"schema":1,"ratings":[]}', encoding="utf-8")
        self.assertIsNone(self.store(private_view_authorized=lambda: True).describe(self.article))
        self.path.write_bytes(b"x"*(4*1024*1024+1))
        self.assertIsNone(self.store(private_view_authorized=lambda: True).describe(self.article))
        self.assertIsNone(ReifenBewertungsStore(Path("relative.json"), private_view_authorized=lambda: True).describe(self.article))
        folder = self.path.parent / "static"
        folder.mkdir()
        unsafe = folder / "ledger.json"
        unsafe.write_text(json.dumps({"schema":1,"ratings":[self.record]}), encoding="utf-8")
        self.assertIsNone(ReifenBewertungsStore(unsafe, private_view_authorized=lambda: True).describe(self.article))

    def test_optional_resolver_failure_or_bad_rating_never_discards_a_default_price(self):
        provider = self.provider(rating_resolver=lambda _: GRADE)
        body, status = suche_reifenangebote(ARGS, provider, now=NOW)
        self.assertEqual((status, body["produkte"][0]["testbewertung"]), (200, GRADE))
        price = body["produkte"][0]["preis"]
        for resolver in (Mock(side_effect=RuntimeError("PRIVATE")), lambda _: {**GRADE, "quelle": "fake"}):
            provider._rating_resolver = resolver
            body, status = suche_reifenangebote(ARGS, provider, now=NOW)
            self.assertEqual(status, 200)
            self.assertEqual(body["produkte"][0]["preis"], price)
            self.assertNotIn("testbewertung", body["produkte"][0])
            self.assertNotIn("PRIVATE", str(body))

    def test_filter_happens_before_page_cap_and_total_and_selection_sku_remains_exact(self):
        rows = [{"Artikelnummer":str(i), "Einkaufspreis":str(i)+",00"} for i in range(1, 31)]
        def resolve(article):
            number = int(article.artikel_id)
            return {**GRADE, "note":"2.3" if number < 25 else "3.5", "urteil":"gut" if number < 25 else "befriedigend"} if number >= 13 else None
        provider = self.provider(rows, rating_resolver=resolve)
        body, status = suche_reifenangebote({**ARGS, "testnote":"gut"}, provider, now=NOW)
        self.assertEqual((status, body["treffer_gesamt"], body["seiten"]), (200, 12, 1))
        self.assertEqual([p["artikel_id"] for p in body["produkte"]], [str(i) for i in range(13,25)])
        body, _ = suche_reifenangebote({**ARGS, "testnote":"mit_note", "seite":"2"}, provider, now=NOW)
        self.assertEqual(body["treffer_gesamt"], 18)
        self.assertEqual([p["artikel_id"] for p in body["produkte"]], [str(i) for i in range(25,31)])
        body, _ = suche_reifenangebote(ARGS, provider, now=NOW, artikel_id="29")
        self.assertEqual(body["produkte"][0]["artikel_id"], "29")

    def test_rating_sort_is_lowest_first_unknown_last_with_price_ties(self):
        rows = [{"Artikelnummer":str(i), "Einkaufspreis":str(price)+",00"} for i,price in ((1,10),(2,30),(3,20),(4,40),(5,15))]
        def resolve(article):
            return {**GRADE, "note":"3.5", "urteil":"befriedigend"} if article.artikel_id=="4" else GRADE if article.artikel_id in {"2","3"} else None
        body, _ = suche_reifenangebote({**ARGS, "sortierung":"testnote_auf"}, self.provider(rows, rating_resolver=resolve), now=NOW)
        self.assertEqual([p["artikel_id"] for p in body["produkte"]], ["3","2","4","1","5"])

    def test_legacy_nonpaginated_provider_filters_before_total_and_page_slicing(self):
        original = self.provider().lookup(QUERY, "1")["angebote"][0]
        rows = []
        for number in range(1, 27):
            row = {**original, "artikel_id":str(number)}
            if number >= 13:
                row["artikel_testbewertung"] = GRADE if number < 26 else {**GRADE,"note":"3.5","urteil":"befriedigend"}
            rows.append(row)
        body, status = suche_reifenangebote({**ARGS,"testnote":"gut","seite":"2"}, lambda _: {"angebote":rows}, now=NOW)
        self.assertEqual((status,body["treffer_gesamt"],body["seiten"]), (200,13,2))
        self.assertEqual([p["artikel_id"] for p in body["produkte"]], ["25"])

    def test_query_defaults_and_base_selection_token_are_unchanged(self):
        self.assertEqual(parse_reifen_suche({**ARGS,"testnote":"alle"}), parse_reifen_suche(ARGS))
        for invalid in ("excellent", "2.5", True, None):
            with self.assertRaises(ReifenSucheEingabefehler) as stopped:
                parse_reifen_suche({**ARGS, "testnote":invalid})
            self.assertEqual((stopped.exception.feld, stopped.exception.code), ("testnote", "testnote_ungueltig"))
        token = erstelle_auswahl_token("synthetic-key", {**ARGS,"testnote":"gut","sortierung":"testnote_auf"}, "1")
        self.assertEqual(lade_auswahl_token("synthetic-key", token)["query"], ARGS)

    def test_no_notes_is_no_match_not_provider_failure_and_stale_price_still_expires(self):
        body, status = suche_reifenangebote({**ARGS, "testnote":"gut"}, self.provider(), now=NOW)
        self.assertEqual((status, body["status"], body["treffer_gesamt"]), (200,"keine_treffer",0))
        body, status = suche_reifenangebote(ARGS, self.provider(rating_resolver=lambda _: GRADE), now=NOW+timedelta(minutes=16))
        self.assertFalse(body["produkte"])
        self.assertEqual(body["code"], "angebote_nicht_pruefbar")


if __name__ == "__main__":
    unittest.main(verbosity=2)
