"""Synthetic manufacturer-SKU matching; no real feed data or provider IO."""
from dataclasses import replace
from pathlib import Path
import json
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import shop_rsu_match as match


def alzura(**changes):
    row = dict(id="123456", brandname="Example", manufacturer_number="EX-205/16",
               width="205", height="55", inner_diameter="16", tyre_type="R",
               load_index="91", speed_index="V", type="S", dot="0", demo="0",
               da_decke="0", runflat="1", ms="0", **{"3pmfs": "0"})
    row.update(changes)
    return row


def rsu(**changes):
    row = dict(Artikelnummer="654321", Hersteller="EXAMPLE", Herstellerartikelnummer="ex-205/16",
               Reifenbreite="205", Reifenquerschnitt="55", Reifendurchmesser="16.0",
               Bauart="R", Loadindex="91", Speedindex="V", Saison="s", Artikeltyp="Reifen",
               Artikeluntertyp="PKW", Dot="", Demo="", Da="")
    row.update(changes)
    return row


def a(row=None, n=1):
    return match.alzura_record(row or alzura(), archive_sha256="a" * 64, row_number=n)


def r(row=None, n=1):
    return match.rsu_record(row or rsu(), csv_sha256="b" * 64, row_number=n)


class MatchTests(unittest.TestCase):
    def test_exact_sku_candidate_preserves_unknowns_and_never_activates(self):
        result = match.match_catalogues([a()], [r()])
        self.assertEqual(result["counts"], {"exact_sku_candidate": 1})
        value = result["mappings"][0]
        self.assertEqual(value["evidence"]["rsu"]["article_id"], "654321")
        self.assertIsNone(value["evidence"]["rsu"]["features"]["runflat"])
        self.assertTrue(value["evidence"]["alzura"]["features"]["runflat"])
        for key in ("confirmed", "procurement_confirmed", "vehicle_fitment_confirmed",
                    "supplier_quote_current", "public_display_ready"):
            self.assertFalse(value[key])
        self.assertFalse(result["publication_enabled"])
        self.assertFalse(result["supplier_quotes_included"])
        encoded = json.dumps(result)
        for private in ("price_1", "Einkaufspreis", "unit_net", "image", "ean"):
            self.assertNotIn(private, encoded)
        self.assertEqual(sum(result["counts"].values()), result["source_record_counts"]["alzura"])

    def test_case_space_and_numeric_spelling_only_are_normalized(self):
        self.assertEqual(match.match_catalogues([a(alzura(brandname="  Example  "))], [r()])
                         ["counts"].get("exact_sku_candidate"), 1)
        for field, value in (("manufacturer_number", "EX20516"), ("brandname", "Other"),
                             ("width", "215"), ("load_index", "94"), ("speed_index", "W"), ("type", "W")):
            with self.subTest(field=field):
                self.assertEqual(match.match_catalogues([a(alzura(**{field: value}))], [r()])["mappings"], [])

    def test_global_duplicate_sku_geometry_or_ids_cannot_be_rescued(self):
        for candidates in ([r(), r(rsu(Artikelnummer="654322"), n=2)],
                           [r(), r(rsu(Artikelnummer="654322", Loadindex="94"), n=2)],
                           [r(), r(rsu(Artikelnummer="654322", Loadindex=""), n=2)]):
            with self.subTest(candidates=candidates):
                self.assertEqual(match.match_catalogues([a()], candidates)["mappings"], [])
        self.assertEqual(match.match_catalogues([a(), a(alzura(id="123457"), n=2)], [r()])["mappings"], [])

    def test_conditions_must_be_clear_without_ignoring_incomplete_core(self):
        for field in ("dot", "demo", "da_decke"):
            for value in ("1", "", "unknown"):
                with self.subTest(field=field, value=value):
                    self.assertEqual(match.match_catalogues([a(alzura(**{field: value}))], [r()])["mappings"], [])
        for field, value in (("Dot", "DOT2022"), ("Demo", "Demo"), ("Da", "Da")):
            with self.subTest(field=field):
                self.assertEqual(match.match_catalogues([a()], [r(rsu(**{field: value}))])["mappings"], [])
        for field, value in (("type", "C"), ("width", ""), ("speed_index", "?"), ("load_index", "91/89")):
            with self.subTest(field=field):
                self.assertEqual(match.match_catalogues([a(alzura(**{field: value}))], [r()])["mappings"], [])

    def test_known_feature_conflict_rejects_unknown_does_not_mean_false(self):
        self.assertTrue(match._feature_conflict({"runflat": True}, {"runflat": False}))
        self.assertFalse(match._feature_conflict({"runflat": True}, {"runflat": None}))
        actual = r()
        altered = replace(actual, features_json=json.dumps({**actual.features(), "runflat": False}))
        with self.assertRaisesRegex(match.MatchError, "identity_record_binding_mismatch"):
            match.match_catalogues([a()], [altered])

    def test_evidence_is_deterministic_and_binds_both_sources(self):
        first = match.match_catalogues([a()], [r()])["mappings"][0]
        again = match.match_catalogues([a()], [r()])["mappings"][0]
        self.assertEqual(first["evidence_id"], again["evidence_id"])
        moved = match.match_catalogues([a(n=2)], [r()])["mappings"][0]
        self.assertNotEqual(first["evidence_id"], moved["evidence_id"])
        self.assertEqual(first["evidence"]["alzura"]["identity_fingerprint"],
                         moved["evidence"]["alzura"]["identity_fingerprint"])
        repriced = alzura(price_1="999.99", availability="0")
        self.assertEqual(a().identity_fingerprint, a(repriced).identity_fingerprint)

    def test_invalid_sources_duplicates_and_limits_fail_closed(self):
        with self.assertRaisesRegex(match.MatchError, "duplicate_source_article_id"):
            match.match_catalogues([a(), a(n=2)], [r()])
        changed_source = match.alzura_record(alzura(id="123457"), archive_sha256="c" * 64, row_number=2)
        with self.assertRaisesRegex(match.MatchError, "mixed_source_snapshots"):
            match.match_catalogues([a(), changed_source], [r()])
        too_many = [a(), a(alzura(id="123457"), n=2)]
        with patch.object(match, "MAX_RECORDS", 1), self.assertRaisesRegex(match.MatchError, "record_limit_exceeded"):
            match.match_catalogues(too_many, [r()])
        for wrong in ("bad", [], None):
            with self.subTest(wrong=wrong), self.assertRaises(match.MatchError):
                match.alzura_record(alzura(), archive_sha256=wrong, row_number=1)
        with self.assertRaisesRegex(match.MatchError, "invalid_source_article_id"):
            a(alzura(id="123_456"))

    def test_separate_restricted_offer_never_replaces_clear_item(self):
        result = match.match_catalogues([a(), a(alzura(id="123457", dot="1"), n=2)],
                                       [r(), r(rsu(Artikelnummer="654322", Dot="DOT2022"), n=2)])
        self.assertEqual(len(result["mappings"]), 1)
        self.assertEqual(result["mappings"][0]["evidence"]["rsu"]["article_id"], "654321")

    def test_every_record_is_accounted_for_including_missing_manufacturer_number(self):
        result = match.match_catalogues([a(), a(alzura(id="123457", manufacturer_number=""), n=2)], [r()])
        self.assertEqual(result["counts"]["identity_missing_sku"], 1)
        self.assertEqual(sum(result["counts"].values()), 2)


if __name__ == "__main__":
    unittest.main()
