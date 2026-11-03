"""Synthetic rim parsing/cache/pricing/search. No app, dotenv or provider IO."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import csv
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from felgen_katalog import (FelgenCatalogCache, FelgenCatalogProvider, FelgenEingabefehler,
                           FelgenKatalogError, parse_felgen_feed, parse_felgen_suche)
from tyresystem_feed import HEADERS


NOW = datetime(2026, 11, 3, 12, tzinfo=timezone.utc)
POLICY = {"currency": "EUR", "price_basis": "net_unit", "single_item_shipping_net": "6.00",
          "free_shipping_from": 2, "verified_on": "2026-11-03", "sources": {
              "price": "https://www.tyresystem.de/agb", "shipping": "https://www.tyresystem.de/lieferbedingungen"}}


def raw_feed(*changes):
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=";", lineterminator="\n")
    writer.writerow(HEADERS)
    for index, change in enumerate(changes or ({},), 1):
        row = dict.fromkeys(HEADERS, "")
        row.update(Artikelnummer=str(index), Artikeltyp="Felge", Artikeluntertyp="Alu", Lagerbestand="8",
                   Einkaufspreis="100,00", Hersteller="Synthetic", Profil="Wheel Model", ean="0000000012345",
                   Herstellerartikelnummer=f"MPN-{index}")
        row.update(change)
        writer.writerow(row[key] for key in HEADERS)
    return output.getvalue().encode("utf-8")


class RimCatalogueTest(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            blocker = patch(target, side_effect=AssertionError("External network forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)
        self.temp = tempfile.TemporaryDirectory(prefix="rim-catalogue-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = NOW
        self.cache = FelgenCatalogCache(self.root / "rims", private_root=self.root, clock=lambda: self.now)

    def snapshot(self, *rows, received=NOW):
        return parse_felgen_feed(raw_feed(*rows), abgerufen_am=received)

    def publish(self, *rows):
        self.cache.replace(self.snapshot(*rows, received=self.now))

    def provider(self, **options):
        return FelgenCatalogProvider(self.cache, policy=deepcopy(POLICY), private_view_authorized=lambda: True,
                                    clock=lambda: self.now, **options)

    def test_parser_uses_only_supported_physical_rims_without_inventing_geometry(self):
        result = self.snapshot({}, {"Artikeluntertyp": "Stahl", "ean": "", "Herstellerartikelnummer": ""},
            {"Artikeluntertyp": "Stahl Agrar"}, {"Artikeltyp": "GenFelge"}, {"Artikeltyp": "GenKomplettrad"},
            {"Artikeltyp": "Reifen", "Artikeluntertyp": "PKW"}, {"Fahrzeugtyp": "LKW"})
        self.assertEqual([a.artikel_id for a in result.artikel], ["1", "2"])
        self.assertEqual([a.material for a in result.artikel], ["alu", "stahl"])
        self.assertIsNone(result.artikel[1].ean)
        self.assertIsNone(result.artikel[1].hersteller_artikelnummer)
        self.assertEqual(result.artikel[0].ean, "0000000012345")
        self.assertEqual(result.artikel[0].abgerufen_am, NOW)
        self.assertFalse(hasattr(result.artikel[0], "einpresstiefe"))
        with self.assertRaises(FrozenInstanceError):
            result.artikel[0].material = "fake"
        with self.assertRaises(TypeError):
            result.statistik["accepted_rims"] = 100
        self.assertNotIn("100.00", repr(result))

    def test_global_conflicting_ids_are_quarantined_across_category_and_malformed_rows(self):
        one = {"Artikelnummer": "1", "Herstellerartikelnummer": "MPN-1"}
        other = {"Artikelnummer": "1", "Artikeltyp": "Reifen"}
        for rows in ((one, other), (other, one), (one, {**one, "Einkaufspreis": "101,00"}, one)):
            snapshot = self.snapshot(*rows)
            self.assertEqual(snapshot.artikel, ())
            self.assertEqual(snapshot.statistik["conflicting_ids"], 1)
        duplicate = self.snapshot(one, one)
        self.assertEqual(len(duplicate.artikel), 1)
        self.assertEqual(duplicate.statistik["duplicate_rows"], 1)
        raw = raw_feed(one)
        bad = raw + b"1;malformed\n"
        self.assertEqual(parse_felgen_feed(bad, abgerufen_am=NOW).artikel, ())

    def test_demo_da_and_unknown_special_flags_are_never_normal_rim_offers(self):
        snapshot = self.snapshot({}, {"Demo": "Demo"}, {"Da": "Da"}, {"Demo": "unknown"}, {"Da": "unknown"})
        self.assertEqual([a.artikel_id for a in snapshot.artikel], ["1"])
        self.cache.replace(snapshot)
        result = self.provider().search({})
        self.assertEqual((result["treffer_gesamt"], [p["artikel_id"] for p in result["produkte"]]), (1, ["1"]))

    def test_header_encoding_resource_and_field_boundaries_fail_safely(self):
        for raw in (raw_feed().replace(b"Artikelnummer", b"SKU", 1), b"\xff", b"", raw_feed({"Profil": "x"*2049})):
            if b"x"*2049 in raw:
                self.assertEqual(parse_felgen_feed(raw, abgerufen_am=NOW).artikel, ())
            else:
                with self.assertRaises(FelgenKatalogError) as caught:
                    parse_felgen_feed(raw, abgerufen_am=NOW)
                self.assertEqual(caught.exception.code, "feed_invalid")
        with patch("felgen_katalog.MAX_ROWS", 1), self.assertRaises(FelgenKatalogError):
            self.snapshot({}, {})
        with patch("felgen_katalog.MAX_BYTES", 1), self.assertRaises(FelgenKatalogError):
            self.snapshot()
        with self.assertRaises(FelgenKatalogError):
            parse_felgen_feed(raw_feed(), abgerufen_am=NOW.replace(tzinfo=None))

    def test_cache_roundtrip_hash_corruption_and_read_never_renew_receipt(self):
        original = self.snapshot()
        self.cache.replace(original)
        fresh = FelgenCatalogCache(self.cache.directory, private_root=self.root, clock=lambda: self.now)
        loaded = fresh.load()
        self.assertEqual(loaded.artikel, original.artikel)
        self.assertEqual((loaded.sha256, loaded.abgerufen_am), (original.sha256, NOW))
        self.now += timedelta(minutes=10)
        self.assertEqual(fresh.load().abgerufen_am, NOW)
        self.cache.path.write_text("PRIVATE-MARKER invalid", encoding="utf-8")
        with self.assertRaises(FelgenKatalogError) as caught:
            fresh.load()
        self.assertEqual(caught.exception.code, "cache_invalid")
        self.assertNotIn("PRIVATE", str(caught.exception))

    def test_rechecksummed_wrong_cache_fields_or_duplicate_ids_are_rejected(self):
        self.publish()
        original = json.loads(self.cache.path.read_text())
        for mutate in (lambda p: p["articles"][0].update(bestand=True),
                       lambda p: p["articles"][0].update(einkaufspreis="NaN"),
                       lambda p: p["articles"][0].update(unknown="PRIVATE"),
                       lambda p: p["articles"].append(dict(p["articles"][0]))):
            value = deepcopy(original)
            mutate(value["payload"])
            canonical = json.dumps(value["payload"], ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
            value["sha256"] = hashlib.sha256(canonical).hexdigest()
            self.cache.path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(FelgenKatalogError):
                self.cache.load()

    def test_atomic_failure_writer_lock_and_old_or_false_receipts_do_not_replace(self):
        self.publish()
        before = self.cache.path.read_bytes()
        self.now += timedelta(minutes=1)
        newer = self.snapshot({"Einkaufspreis": "120,00"}, received=self.now)
        with patch("felgen_katalog.os.replace", side_effect=OSError("PRIVATE")), self.assertRaises(FelgenKatalogError):
            self.cache.replace(newer)
        self.assertEqual(self.cache.path.read_bytes(), before)
        self.assertEqual(self.cache.load().abgerufen_am, NOW)
        self.assertEqual(list(self.cache.directory.glob("*.tmp")), [])
        lock = self.cache.directory / ".felgen-write.lock"
        lock.write_text("other writer")
        with self.assertRaises(FelgenKatalogError):
            self.cache.replace(newer)
        self.assertEqual(lock.read_text(), "other writer")
        lock.unlink()
        for candidate in (self.snapshot(received=NOW-timedelta(seconds=1)), self.snapshot(received=self.now+timedelta(minutes=1)),
                          replace(self.snapshot(), abgerufen_am=self.now)):
            with self.assertRaises(FelgenKatalogError):
                self.cache.replace(candidate)
            self.assertEqual(self.cache.path.read_bytes(), before)

    def test_unsafe_cache_roots_are_rejected_before_network_or_writes(self):
        for folder in ("relative", "//server/share", self.root / "static", self.root.parent / "outside"):
            with self.assertRaises(FelgenKatalogError):
                FelgenCatalogCache(folder, private_root=self.root)

    def test_private_authorization_and_complete_policy_are_required_before_cache_io(self):
        for auth in (None, True, lambda: 1, lambda: "true", Mock(side_effect=RuntimeError("PRIVATE"))):
            with patch.object(self.cache, "load") as read:
                provider = FelgenCatalogProvider(self.cache, policy=POLICY, private_view_authorized=auth)
                with self.assertRaises(FelgenKatalogError):
                    provider.search({})
                self.assertFalse(provider.publication_ready)
                read.assert_not_called()
        for field, value in (("single_item_shipping_net", None), ("single_item_shipping_net", "0.00"),
                             ("price_basis", "gross"), ("currency", "USD"), ("free_shipping_from", True), ("sources", {})):
            with patch.object(self.cache, "load") as read:
                provider = FelgenCatalogProvider(self.cache, policy={**POLICY, field: value}, private_view_authorized=lambda: True)
                with self.assertRaises(FelgenKatalogError):
                    provider.facets()
                read.assert_not_called()

    def test_real_price_calculator_markup_freight_and_public_whitelist(self):
        self.publish()
        provider = self.provider()
        four = provider.search({})["produkte"][0]
        self.assertEqual(four["preis"], {"einzel_brutto": "130.90", "gesamt_felgen_brutto": "523.60",
                                      "versand_brutto": "0.00", "gesamt_brutto": "523.60", "waehrung": "EUR"})
        one = provider.lookup("1", anzahl=1)
        self.assertEqual((one["preis"]["versand_brutto"], one["preis"]["gesamt_brutto"]), ("7.14", "138.04"))
        self.assertEqual(four["kennungspassung"], "nicht_geprueft")
        self.assertEqual(set(four), {"artikel_id", "ean", "hersteller_artikelnummer", "hersteller", "profil", "bezeichnung",
                                   "material", "bestand", "anzahl", "preisstand", "gueltig_bis", "kennungspassung", "preis"})
        for private in ("einkaufspreis", "netto_einkauf", "source_sha256", "http", "einpresstiefe", "gutachten"):
            self.assertNotIn(private, str(four))

    def test_exact_identity_priority_stock_sorts_paging_and_lookup_independence(self):
        rows = [{"Artikelnummer": str(i), "Hersteller": "Beta" if i < 20 else "Alpha", "Profil": f"Model{i:02}",
                 "Einkaufspreis": f"{i},00", "Herstellerartikelnummer": f"Part{i}", "ean": f"{i:013}"} for i in range(1, 45)]
        self.publish(*rows)
        provider = self.provider()
        page = provider.search({"hersteller": "Alpha", "seite": "2"})
        self.assertEqual((page["treffer_gesamt"], page["seiten"], page["seite"]), (25, 3, 2))
        self.assertEqual([p["artikel_id"] for p in page["produkte"]], [str(i) for i in range(32, 44)])
        self.assertEqual(provider.search({"sortierung": "preis_ab"})["produkte"][0]["artikel_id"], "44")
        self.assertEqual(provider.search({"sortierung": "name"})["produkte"][0]["hersteller"], "Alpha")
        for needle in ("44", "PART44", "0000000000044"):
            self.assertEqual([p["artikel_id"] for p in provider.search({"suche": needle})["produkte"]], ["44"])
        self.assertEqual(provider.lookup("1")["artikel_id"], "1")
        self.assertIsNone(provider.lookup("999"))
        self.assertEqual(provider.search({"suche": "alpha model44"})["produkte"][0]["artikel_id"], "44")
        self.publish({"Artikelnummer": "1", "Lagerbestand": "3"}, {"Artikelnummer": "2", "Profil": "Model 1"})
        self.assertEqual(provider.search({"suche": "1"})["produkte"], [])
        self.assertIsNone(provider.lookup("1", anzahl=4))

    def test_facets_and_exact_expiry_preserve_actual_receipt(self):
        self.publish({}, {"Hersteller": "Alpha", "Artikeluntertyp": "Stahl", "Lagerbestand": "1"}, {"Hersteller": "NoStock", "Lagerbestand": "0"})
        provider = self.provider()
        facets = provider.facets()
        self.assertEqual((facets["hersteller"], facets["materialien"], facets["artikel_gesamt"]), (["Alpha", "Synthetic"], ["alu", "stahl"], 2))
        self.assertNotIn("einkaufspreis", str(facets))
        self.now += timedelta(minutes=10)
        self.assertEqual(provider.facets()["preisstand"], facets["preisstand"])
        self.now += timedelta(minutes=5)
        for method in (lambda: provider.search({}), lambda: provider.lookup("1"), provider.facets):
            with self.assertRaises(FelgenKatalogError) as caught:
                method()
            self.assertEqual(caught.exception.code, "cache_stale")

    def test_only_page_is_materialized_and_gross_rounding_ties_use_sku(self):
        self.publish(*[{"Artikelnummer": str(i), "Einkaufspreis": "100,00"} for i in range(1, 41)])
        provider = self.provider()
        with patch.object(provider, "_product", wraps=provider._product) as products:
            result = provider.search({"seite": "2"})
            self.assertEqual((result["treffer_gesamt"], len(result["produkte"]), products.call_count), (40, 12, 12))
        self.publish({"Artikelnummer": "1", "Einkaufspreis": "1,0001"}, {"Artikelnummer": "2", "Einkaufspreis": "1,0000"})
        for sort in ("preis_auf", "preis_ab"):
            result = provider.search({"sortierung": sort})
            self.assertEqual([p["artikel_id"] for p in result["produkte"]], ["1", "2"])
            self.assertEqual({p["preis"]["einzel_brutto"] for p in result["produkte"]}, {"1.31"})

    def test_bad_inputs_and_invalid_calculation_never_become_free_or_unbounded(self):
        for field, value in (("suche", "x"*121), ("hersteller", "bad\nbrand"), ("material", "Agrar"),
                             ("anzahl", "0"), ("anzahl", "9"), ("seite", "10001"), ("seite", True), ("sortierung", "unknown")):
            with self.subTest(field=field), self.assertRaises(FelgenEingabefehler) as caught:
                parse_felgen_suche({field: value})
            self.assertEqual(caught.exception.feld, field)
        with self.assertRaises(FelgenEingabefehler):
            parse_felgen_suche({"netto_einkauf": "1"})
        self.publish({"Einkaufspreis": "1000000,00"}, {"Einkaufspreis": "0,0001"})
        self.assertEqual(self.provider().search({})["treffer_gesamt"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
