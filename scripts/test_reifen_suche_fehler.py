"""Safe search error contract and size normalization, with offline providers."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkzeug.datastructures import MultiDict
from reifen_suche import ReifenSucheEingabefehler, parse_reifen_suche, suche_reifenangebote
from tyresystem_provider import TyreSystemCatalogError


NOW = datetime(2026, 11, 3, tzinfo=timezone.utc)
QUERY = {"groesse": "225/40 R18 92V", "saison": "winter", "qualitaet": "alle", "anzahl": "4", "montage": "nein"}


def article(**changes):
    value = {"artikel_id": "1", "bezeichnung": "Synthetic tyre", "breite": 225, "querschnitt": 40,
             "zoll": 18, "lastindex": 92, "geschwindigkeitsindex": "V", "qualitaet": None,
             "saison": "winter", "bestand": 8, "waehrung": "EUR", "preis_bestaetigt": True,
             "netto_einkauf": "100.00", "fracht_netto": "0.00", "abgerufen_am": NOW.isoformat(),
             "gueltig_bis": (NOW+timedelta(minutes=15)).isoformat(), "lieferzeit": "Ohne Reservierung."}
    value.update(changes)
    return value


class SearchErrorsTest(unittest.TestCase):
    def setUp(self):
        for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection", "socket.getaddrinfo"):
            blocker = patch(target, side_effect=AssertionError("External IO forbidden"))
            blocker.start()
            self.addCleanup(blocker.stop)

    def search(self, provider=None, **query):
        return suche_reifenangebote({**QUERY, **query}, provider, now=NOW)

    def test_unambiguous_dimension_spellings_without_invented_indices(self):
        for size in ("225/40R18", "225 / 40 R 18", "225 40 18", "225/40 18", "2254018"):
            with self.subTest(size=size):
                parsed = parse_reifen_suche({**QUERY, "groesse": size})
                self.assertEqual([parsed[k] for k in ("breite", "querschnitt", "zoll")], [225, 40, 18])
                self.assertIsNone(parsed["lastindex"])
                self.assertIsNone(parsed["geschwindigkeitsindex"])
        for size in ("225/40R18 92V", "225 40 18 92 V", "225/40ZR18 92V", "225/40R1892V"):
            parsed = parse_reifen_suche({**QUERY, "groesse": size})
            self.assertEqual((parsed["lastindex"], parsed["geschwindigkeitsindex"]), (92, "V"))
        parsed = parse_reifen_suche({**QUERY, "groesse": "225/40 R18 95H"})
        self.assertEqual((parsed["lastindex"], parsed["geschwindigkeitsindex"]), (95, "H"))

    def test_field_specific_errors_remain_value_errors_with_readable_text(self):
        for field, value in (("groesse", "225/40 R18 92V XL"), ("groesse", ""),
                             ("saison", ""), ("qualitaet", "unknown"), ("anzahl", "9"), ("montage", ""),
                             ("saison", []), ("qualitaet", []), ("montage", [])):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ReifenSucheEingabefehler) as caught:
                    self.search(lambda _: self.fail("Provider called before validation"), **{field: value})
                self.assertIsInstance(caught.exception, ValueError)
                self.assertEqual(caught.exception.feld, field)
                self.assertEqual(caught.exception.code, field + "_ungueltig")
                self.assertTrue(str(caught.exception))
        self.assertEqual(str(ReifenSucheEingabefehler("Existing caller text")), "Existing caller text")

    def test_ambiguous_or_unsupported_indices_are_never_silently_removed(self):
        for size in ("225401892V", "225/40R18 50V", "225/40R18 92Z", "225/40R18 92(V)", "225/40R18\n92V"):
            with self.subTest(size=size), self.assertRaises(ReifenSucheEingabefehler) as caught:
                parse_reifen_suche({**QUERY, "groesse": size})
            self.assertEqual(caught.exception.feld, "groesse")

    def test_unknown_and_duplicate_search_fields_stay_invalid(self):
        duplicate = MultiDict(QUERY)
        duplicate.add("groesse", "205/55R16")
        for args in ({**QUERY, "api_key": "PRIVATE-MARKER"}, duplicate):
            with self.assertRaises(ReifenSucheEingabefehler) as caught:
                parse_reifen_suche(args)
            self.assertEqual(caught.exception.code, "suchfelder_ungueltig")
            self.assertNotIn("PRIVATE-MARKER", str(caught.exception))

    def test_catalogue_missing_stale_and_generic_error_codes_are_distinct(self):
        expected = {"cache_stale": "katalog_veraltet", "cache_missing": "katalog_nicht_bereit",
                    "cache_invalid": "katalog_nicht_verfuegbar", "configuration": "katalog_nicht_verfuegbar",
                    "query": "katalog_nicht_verfuegbar", "write": "katalog_nicht_verfuegbar"}
        for internal, code in expected.items():
            with self.subTest(internal=internal):
                def provider(_):
                    error = TyreSystemCatalogError(internal)
                    error.args = ("PRIVATE-MARKER https://private.invalid/?key=secret",)
                    raise error
                body, status = self.search(provider)
                self.assertEqual((status, body["code"]), (503, code))
                self.assertEqual(body["produkte"], [])
                self.assertNotIn("PRIVATE-MARKER", str(body))
                self.assertNotIn("private.invalid", str(body))

    def test_unrecognized_provider_errors_cannot_spoof_public_catalogue_code(self):
        def provider(_):
            error = RuntimeError("PRIVATE-MARKER")
            error.code = "cache_stale"
            raise error
        body, status = self.search(provider)
        self.assertEqual((status, body["code"]), (503, "katalog_nicht_verfuegbar"))
        self.assertNotIn("PRIVATE-MARKER", str(body))
        body, status = self.search()
        self.assertEqual((status, body["code"]), (200, "katalog_nicht_bereit"))

    def test_no_matches_and_unverifiable_articles_are_not_conflated(self):
        body, status = self.search(lambda _: {"angebote": []})
        self.assertEqual((status, body["status"], body["code"]), (200, "keine_treffer", "keine_treffer"))
        for item in (article(bestand=3), article(fracht_netto=None), article(abgerufen_am=(NOW-timedelta(minutes=16)).isoformat())):
            body, status = self.search(lambda _: {"angebote": [item]})
            self.assertEqual((status, body["code"]), (200, "angebote_nicht_pruefbar"))
            self.assertNotEqual(body["status"], "keine_treffer")
            self.assertEqual(body["produkte"], [])
        for raw in (None, {"angebote": "PRIVATE-MARKER"}, {"angebote": [article()]*101}):
            body, status = self.search(lambda _: raw)
            self.assertEqual((status, body["code"]), (503, "angebote_nicht_pruefbar"))
            self.assertNotIn("PRIVATE-MARKER", str(body))

    def test_supported_offer_keeps_real_price_and_montage_limits_have_their_own_code(self):
        body, status = self.search(lambda _: {"angebote": [article(bestand=0), article()]})
        self.assertEqual((status, body["code"]), (200, "ok"))
        self.assertEqual(body["produkte"][0]["preis"]["gesamt_brutto"], "523.60")
        body, status = self.search(lambda _: self.fail("Unsupported montage queried provider"), montage="ja", anzahl="8")
        self.assertEqual((status, body["code"]), (200, "montage_nicht_online"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
