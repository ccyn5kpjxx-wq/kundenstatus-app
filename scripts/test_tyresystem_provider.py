"""Synthetic catalogue/cache/search tests. No portal, dotenv or external IO."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import csv
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tyresystem_feed import HEADERS, parse_feed
from tyresystem_provider import TyreSystemCatalogCache, TyreSystemCatalogProvider, TyreSystemCatalogError
from scripts.refresh_tyresystem_catalog import refresh_catalog, main as refresh_main


NOW = datetime(2026, 11, 2, 12, tzinfo=timezone.utc)
POLICY = {"currency": "EUR", "price_basis": "net_unit", "single_tyre_shipping_net": "6.00",
          "free_shipping_from": 2, "delivery_text": "Standardlieferung 1–4 Werktage; keine Reservierung.",
          "verified_on": "2026-11-02", "sources": {
              "price": "https://www.tyresystem.de/agb", "shipping": "https://www.tyresystem.de/lieferbedingungen",
              "delivery": "https://www.tyresystem.de/leistungen"}}
QUERY = {"breite": 225, "querschnitt": 40, "zoll": 18, "lastindex": 92,
         "geschwindigkeitsindex": "V", "saison": "winter", "qualitaet": "alle", "anzahl": 4, "montage": False}


def feed(*changes, received=NOW):
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=";", lineterminator="\n")
    writer.writerow(HEADERS)
    for index, changeset in enumerate(changes or ({},), 1):
        row = dict.fromkeys(HEADERS, "")
        row.update(Artikelnummer=str(index), ean="4000000000001", Lagerbestand="8", Einkaufspreis="93,0600",
                   Hersteller="Synthetic", Profil="Winter", Reifenbreite="225", Reifenquerschnitt="40",
                   Reifendurchmesser="18", Loadindex="92", Speedindex="V", Saison="w", Fahrzeugtyp="PKW",
                   Bauart="R", Artikeltyp="Reifen", Artikeluntertyp="PKW", LabelKraftstoffeffizienz="B",
                   LabelNasshaftung="A", LabelRollgeraeusch="71", LabelRollgeraeuschklasse="B",
                   **{"EPREL-URL": "https://eprel.ec.europa.eu/qr/12345"})
        row.update(changeset)
        writer.writerow(row[key] for key in HEADERS)
    return parse_feed(output.getvalue().encode(), abgerufen_am=received)


class CatalogueTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tyresystem-catalog-offline-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = NOW
        self.cache = TyreSystemCatalogCache(self.root / "private-catalog", private_root=self.root, clock=lambda: self.now)
        self.addCleanup(patch.stopall)
        for name in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            patch(name, side_effect=AssertionError("External network forbidden")).start()

    def provider(self, **changes):
        return TyreSystemCatalogProvider(self.cache, publication_approved=True, publication_reference="synthetic-written-release",
            policy=deepcopy(POLICY), clock=lambda: self.now, **changes)

    def publish(self, snapshot=None):
        self.cache.replace(snapshot or feed())

    def test_default_and_unproved_release_perform_no_cache_read(self):
        for options in ({}, {"publication_approved": 1, "publication_reference": "reference"},
                        {"publication_approved": True}, {"publication_approved": True, "publication_reference": "  "},
                        {"publication_approved": True, "publication_reference": "bad\nref"}):
            with self.subTest(options=options), patch.object(self.cache, "load") as load:
                provider = TyreSystemCatalogProvider(self.cache, policy=POLICY, **options)
                self.assertEqual(provider(QUERY), {"angebote": []})
                load.assert_not_called()

    def test_missing_or_incomplete_price_and_freight_policy_never_implies_zero(self):
        for change in ({"single_tyre_shipping_net": None}, {"single_tyre_shipping_net": "0.00"},
                       {"price_basis": None}, {"currency": "USD"}, {"free_shipping_from": True},
                       {"sources": {}}, {"verified_on": "not-a-date"}, {"delivery_text": "Garantiert morgen reserviert"}):
            policy = {**POLICY, **change}
            with self.subTest(change=change), patch.object(self.cache, "load") as load:
                provider = TyreSystemCatalogProvider(self.cache, publication_approved=True, publication_reference="test", policy=policy)
                self.assertEqual(provider(QUERY), {"angebote": []})
                load.assert_not_called()

    def test_roundtrip_preserves_receipt_decimal_and_dimension_index(self):
        original = feed({}, {"Artikelnummer": "2", "Reifenbreite": "205", "Reifenquerschnitt": "55", "Reifendurchmesser": "16"})
        self.publish(original)
        second = TyreSystemCatalogCache(self.cache.directory, private_root=self.root, clock=lambda: self.now)
        loaded = second.load()
        self.assertEqual(loaded.snapshot.abgerufen_am, NOW)
        self.assertEqual(loaded.snapshot.sha256, original.sha256)
        self.assertEqual(str(loaded.snapshot.artikel[0].einkaufspreis), "93.0600")
        self.assertEqual(len(loaded.dimensions[(225, 40, 18, "winter")]), 1)
        self.assertIs(loaded.snapshot.public_ready, False)
        with self.assertRaises(TypeError):
            loaded.dimensions[(1, 1, 1, "winter")] = ()

    def test_receipt_expiry_does_not_extend_when_file_is_read_again(self):
        self.publish()
        provider = self.provider()
        first = provider(QUERY)["angebote"][0]
        self.now += timedelta(minutes=10)
        second = provider(QUERY)["angebote"][0]
        self.assertEqual(first["abgerufen_am"], second["abgerufen_am"])
        self.assertEqual(first["gueltig_bis"], second["gueltig_bis"])
        self.now = NOW + timedelta(minutes=15)
        with self.assertRaises(TyreSystemCatalogError) as stopped:
            provider(QUERY)
        self.assertEqual(stopped.exception.code, "cache_stale")

    def test_corrupt_changed_cache_never_reuses_warm_memory(self):
        self.publish()
        self.cache.load()
        self.cache.path.write_text('not-json https://secret.invalid/login=private', encoding="utf-8")
        with self.assertRaises(TyreSystemCatalogError) as stopped:
            self.provider()(QUERY)
        self.assertNotIn("private", str(stopped.exception))
        self.assertEqual(stopped.exception.code, "cache_invalid")

    def test_checksums_unknown_fields_and_bad_types_reject_complete_cache(self):
        self.publish()
        original = json.loads(self.cache.path.read_text())
        for mutate in (lambda p: p.update(unexpected="credential"),
                       lambda p: p["articles"][0].update(gemeldeter_bestand=True),
                       lambda p: p["articles"].append(deepcopy(p["articles"][0])),
                       lambda p: p["articles"][0].update(eprel_url="https://secret.invalid/private")):
            value = deepcopy(original)
            mutate(value["payload"])
            encoded = json.dumps(value["payload"], ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
            value["sha256"] = hashlib.sha256(encoded).hexdigest()
            self.cache.path.write_text(json.dumps(value))
            with self.assertRaises(TyreSystemCatalogError):
                self.cache.load()
        value = deepcopy(original)
        value["payload"]["articles"][0]["einkaufspreis"] = "1.00"
        self.cache.path.write_text(json.dumps(value))
        with self.assertRaises(TyreSystemCatalogError):
            self.cache.load()

    def test_failed_atomic_swap_preserves_exact_old_bytes_and_time(self):
        self.publish()
        before, receipt = self.cache.path.read_bytes(), self.cache.load().snapshot.abgerufen_am
        self.now += timedelta(minutes=1)
        replacement = feed({"Einkaufspreis": "100,00"}, received=self.now)
        with patch("tyresystem_provider.os.replace", side_effect=OSError("sensitive fake path")):
            with self.assertRaises(TyreSystemCatalogError):
                self.cache.replace(replacement)
        self.assertEqual(self.cache.path.read_bytes(), before)
        self.assertEqual(self.cache.load().snapshot.abgerufen_am, receipt)
        self.assertEqual([p.name for p in self.cache.directory.iterdir()], ["catalog.json"])

    def test_invalid_stale_future_and_older_refreshes_do_not_replace_catalog(self):
        self.publish()
        original = self.cache.path.read_bytes()
        bad = replace(feed(), artikel=(replace(feed().artikel[0], gemeldeter_bestand=-1),))
        for snapshot in (bad, feed(received=NOW-timedelta(minutes=15)), feed(received=NOW+timedelta(seconds=31))):
            with self.assertRaises(TyreSystemCatalogError):
                self.cache.replace(snapshot)
            self.assertEqual(self.cache.path.read_bytes(), original)
        self.now += timedelta(minutes=1)
        self.cache.replace(feed(received=self.now))
        newer = self.cache.path.read_bytes()
        with self.assertRaises(TyreSystemCatalogError):
            self.cache.replace(feed())
        self.assertEqual(self.cache.path.read_bytes(), newer)

    def test_existing_writer_lock_is_not_removed_or_overridden(self):
        self.publish()
        previous = self.cache.path.read_bytes()
        lock = self.cache.directory / ".catalog-write.lock"
        lock.write_text("synthetic owner")
        with self.assertRaises(TyreSystemCatalogError):
            self.cache.replace(feed())
        self.assertEqual(self.cache.path.read_bytes(), previous)
        self.assertEqual(lock.read_text(), "synthetic owner")

    def test_complete_new_snapshot_becomes_visible_only_at_atomic_rename(self):
        self.publish()
        self.now += timedelta(minutes=1)
        replacement = feed({"Einkaufspreis": "120,00"}, received=self.now)
        import os
        actual_replace = os.replace
        observed = []
        def atomic_swap(source, target):
            observed.append(self.cache.load().snapshot.abgerufen_am)
            return actual_replace(source, target)
        with patch("tyresystem_provider.os.replace", side_effect=atomic_swap):
            self.cache.replace(replacement)
        self.assertEqual(observed, [NOW])
        self.assertEqual(self.cache.load().snapshot.abgerufen_am, self.now)
        self.assertEqual(str(self.cache.load().snapshot.artikel[0].einkaufspreis), "120.00")

    def test_cache_resource_limit_and_duplicate_json_keys_fail_closed(self):
        self.publish()
        with patch("tyresystem_provider.MAX_CACHE_BYTES", 16), self.assertRaises(TyreSystemCatalogError):
            self.cache.load()
        self.cache.path.write_text('{"payload":{},"payload":{},"sha256":"irrelevant"}')
        with self.assertRaises(TyreSystemCatalogError):
            self.cache.load()

    def test_cheapest_eligible_articles_are_selected_before_the_result_cap(self):
        rows = [{"Artikelnummer": str(index), "Einkaufspreis": "100,00"} for index in range(1, 102)]
        rows[-1]["Einkaufspreis"] = "1,00"
        self.publish(feed(*rows))
        offers = self.provider()(QUERY)["angebote"]
        self.assertEqual(len(offers), 12)
        self.assertEqual(offers[0]["artikel_id"], "101")

    def test_public_outside_relative_and_unc_locations_are_rejected(self):
        for folder in (self.root / "static" / "catalog", self.root.parent / "outside-catalog", "relative", "//server/share"):
            with self.subTest(path=str(folder)), self.assertRaises(TyreSystemCatalogError):
                TyreSystemCatalogCache(folder, private_root=self.root)

    def test_quantity_dimensions_indices_season_and_special_goods_are_filtered(self):
        self.publish(feed({}, {"Artikelnummer": "2", "Lagerbestand": "3", "Einkaufspreis": "40,00"},
            {"Artikelnummer": "3", "Loadindex": "91", "Speedindex": "H", "Einkaufspreis": "30,00"},
            {"Artikelnummer": "4", "Demo": "Demo", "Einkaufspreis": "20,00"},
            {"Artikelnummer": "5", "Saison": "s", "Einkaufspreis": "10,00"},
            {"Artikelnummer": "6", "Reifenbreite": "205", "Einkaufspreis": "10,00"}))
        self.assertEqual([p["artikel_id"] for p in self.provider()(QUERY)["angebote"]], ["1"])
        optional = {**QUERY, "lastindex": None, "geschwindigkeitsindex": None}
        offers = self.provider()(optional)["angebote"]
        self.assertEqual([p["artikel_id"] for p in offers], ["3", "1"])
        self.assertEqual(offers[0]["lastindex"], 91)
        self.assertEqual(offers[0]["geschwindigkeitsindex"], "H")
        self.assertIs(type(offers[0]["zoll"]), int)
        self.assertEqual(self.provider()({**QUERY, "anzahl": 8})["angebote"][0]["bestand"], 8)
        with self.assertRaises(TyreSystemCatalogError):
            self.provider()({**QUERY, "anzahl": True})

    def test_all_brands_works_without_invented_quality_or_profile_features(self):
        self.publish(feed({"Profil": "Winter XL Runflat 3PMSF"}))
        offer = self.provider()(QUERY)["angebote"][0]
        for key in ("qualitaet", "runflat", "xl", "schneeflocke"):
            self.assertIsNone(offer[key])
        self.assertFalse(offer["montage_standard_bestaetigt"])
        self.assertEqual(self.provider()({**QUERY, "qualitaet": "premium"}), {"angebote": [], "gesamt": 0, "paginiert": True})
        self.assertEqual(self.provider()({**QUERY, "montage": True}), {"angebote": [], "gesamt": 0, "paginiert": True})

    def test_only_confirmed_article_metadata_enables_class_and_standard_mounting(self):
        self.publish()
        meta = {"1": {"bestaetigt": True, "nachweis": "synthetic supplier sheet", "qualitaet": "premium",
                      "runflat": False, "montage_standard_bestaetigt": True, "xl": True, "schneeflocke": True}}
        provider = self.provider(article_metadata=meta)
        item = provider({**QUERY, "montage": True, "qualitaet": "premium"})["angebote"][0]
        self.assertTrue(item["montage_standard_bestaetigt"])
        self.assertTrue(item["xl"] and item["schneeflocke"])
        for change in ({"runflat": True}, {"runflat": None}, {"bestaetigt": 1}, {"nachweis": ""}, {"nachweis": "  "}, {"qualitaet": []}):
            modified = {"1": {**meta["1"], **change}}
            self.assertEqual(self.provider(article_metadata=modified)({**QUERY, "montage": True}), {"angebote": [], "gesamt": 0, "paginiert": True})

    def test_freight_is_six_for_one_and_once_zero_from_two(self):
        self.publish()
        for quantity, expected in ((1, "6.00"), (2, "0.00"), (4, "0.00"), (8, "0.00")):
            item = self.provider()({**QUERY, "anzahl": quantity})["angebote"][0]
            self.assertEqual(item["fracht_netto"], expected)
            self.assertEqual(item["abgerufen_am"], NOW.isoformat())
            self.assertEqual(item["gueltig_bis"], (NOW+timedelta(minutes=15)).isoformat())
            self.assertIn("keine Reservierung", item["lieferzeit"])

    def test_actual_search_contract_calculates_ten_percent_without_public_ek(self):
        from reifen_suche import suche_reifenangebote
        self.publish()
        provider = self.provider(article_metadata={"1": {"bestaetigt": True, "nachweis": "test", "qualitaet": "premium"}})
        payload, code = suche_reifenangebote({"groesse": "225/40 R18 92V", "saison": "winter", "qualitaet": "premium",
                                            "anzahl": "4", "montage": "nein"}, provider, now=NOW)
        self.assertEqual(code, 200)
        self.assertEqual(payload["produkte"][0]["preis"]["einzel_brutto"], "121.82")
        self.assertEqual(payload["produkte"][0]["preis"]["gesamt_brutto"], "487.28")
        serialized = json.dumps(payload)
        self.assertNotIn("netto_einkauf", serialized)
        self.assertNotIn("93.0600", serialized)
        self.assertNotIn("source_sha256", serialized)

    def test_failed_refresh_does_not_relabel_existing_data_as_fresh(self):
        self.publish()
        before = self.cache.path.read_bytes()
        client = Mock()
        client.fetch.side_effect = RuntimeError("https://private.invalid/?login=do-not-print")
        result, code = refresh_catalog(self.cache, client)
        self.assertEqual(code, 1)
        self.assertFalse(result["updated"])
        self.assertEqual(self.cache.path.read_bytes(), before)
        self.assertNotIn("do-not-print", json.dumps(result))

    def test_cli_requires_run_before_constructing_client_or_reading_environment(self):
        with patch("tyresystem_export.TyreSystemExportClient.from_env") as factory, redirect_stdout(io.StringIO()) as output:
            code = refresh_main(["--cache-dir", str(self.cache.directory), "--private-root", str(self.root)])
        self.assertEqual(code, 2)
        factory.assert_not_called()
        self.assertFalse(self.cache.directory.exists())
        self.assertEqual(json.loads(output.getvalue()), {"updated": False, "run_authorized": False})

    def test_explicit_cli_performs_one_injected_fetch_without_releasing_publication(self):
        client = Mock()
        client.fetch.return_value = feed()
        with patch("scripts.refresh_tyresystem_catalog.TyreSystemCatalogCache", return_value=self.cache), \
                patch("tyresystem_export.TyreSystemExportClient.from_env", return_value=client) as factory, \
                redirect_stdout(io.StringIO()) as output:
            code = refresh_main(["--run", "--cache-dir", str(self.cache.directory), "--private-root", str(self.root)])
        self.assertEqual(code, 0)
        self.assertIs(factory.call_args.kwargs["authorize"](), True)
        client.fetch.assert_called_once_with()
        self.assertFalse(json.loads(output.getvalue())["public_release_changed"])
        self.assertEqual(self.cache.load().snapshot.abgerufen_am, NOW)


if __name__ == "__main__":
    unittest.main(verbosity=2)
