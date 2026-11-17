"""Offline synthetic product tests; these fixtures grant no actual fitment."""

from copy import deepcopy
import unittest
from unittest.mock import patch

from shop_rim_width_candidates import build_width_candidates, tyre_nominal_facts


def registry():
    return {"schema_version": 1, "scope": "continental_group_dimensional_proposals_only",
            "is_vehicle_approval": False, "construction": "R", "permitted_rim_contours": ["J", "JK", "JJ"],
            "brands": ["Continental"],
            "source": {"url": "https://www.continental-reifen.de/fixture.pdf", "sha256": "1" * 64},
            "rows": [{"size": "225/45 R17", "width_mm": 225, "ratio_percent": 45, "diameter_in": 17,
                      "load_variants": [{"load_index": 91, "xl": False}, {"load_index": 94, "xl": True}],
                      "permitted_rim_widths_in": [7, 7.5, 8, 8.5], "pdf_page": 27,
                      "printed_page": 52, "visually_reviewed": True}]}


def tyre(**extra):
    return {"selection_id": "tyre:" + "a" * 24, "category": "tyre", "dimensions_valid": True,
            "manufacturer_name": "CONTINENTAL", "name": "SYNTHETIC TEST FIXTURE",
            "width": "225", "ratio": "45", "diameter": "17",
            "details": {"Artikelbezeichnung": "SYNTHETIC FIXTURE 225/45 R17 94 V XL",
                        "Geschwindigkeitsindex": "V", "Zusatzmerkmale": ""}, **extra}


def rim(**extra):
    return {"selection_id": "alloy_wheel:" + "b" * 24, "category": "alloy_wheel",
            "dimensions_valid": True, "width": "7.5", "diameter": "17", **extra}


def run_candidates(tyre_row=None, rim_row=None, reviewed=None):
    return build_width_candidates({"reifen": [tyre_row or tyre()], "felgen": [rim_row or rim()]},
                                  registry() if reviewed is None else reviewed)


class NominalWidthChecks(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.socket", side_effect=AssertionError("Network forbidden"))
        self.network.start()

    def tearDown(self):
        self.network.stop()

    def test_positive_synthetic_product_is_only_nominal_candidate_with_open_gates(self):
        result = run_candidates()
        candidate = result[rim()["selection_id"]][0]
        self.assertEqual(candidate["tyre_selection_id"], tyre()["selection_id"])
        self.assertEqual(candidate["scope"], "rim_width_candidate")
        self.assertEqual(candidate["source"], {**registry()["source"], "page": 27})
        self.assertEqual(set(candidate["unresolved"]), {"vehicleapproval", "flangecontour", "actualexecution", "loadspeedvehicleconditions"})
        self.assertIn("Nennmaß", candidate["label"])
        for forbidden in ("fitment_spec", "approved", "vehicle_id", "certificate_reference"):
            self.assertNotIn(forbidden, candidate)
        self.assertRegex(candidate["evidence_id"], r"^rim-width:[a-f0-9]{24}$")

    def test_same_diameter_alone_never_suffices_and_boolean_dimensions_are_invalid(self):
        for extra in ({"width": "6.5"}, {"diameter": "18"}, {"width": True},
                      {"width": "NaN"}, {"diameter": "17e0"}, {"dimensions_valid": False}):
            with self.subTest(extra=extra):
                self.assertEqual(run_candidates(rim_row=rim(**extra)), {})

    def test_brand_requires_exact_native_manufacturer_and_does_not_parse_display_name(self):
        for brand in (None, "", "MICHELIN", "CONTINENTAL OTHER", ["Continental"]):
            with self.subTest(brand=brand):
                self.assertEqual(run_candidates(tyre_row=tyre(manufacturer_name=brand,
                    name="CONTINENTAL SYNTHETIC TEST")), {})
        missing = tyre()
        del missing["manufacturer_name"]
        self.assertEqual(run_candidates(tyre_row=missing), {})

    def test_description_and_normalized_size_must_agree_and_fragment_must_be_unique(self):
        for description in ("SYNTHETIC 235/45 R17 94 V XL", "SYNTHETIC 225/40 R17 94 V XL",
                            "SYNTHETIC 225/45 R18 94 V XL", "SYNTHETIC 225/45 R17",
                            "225/45 R17 94 V XL / 225/45 R17 94 V XL", "225/45 R17 94/92 V"):
            product = tyre()
            product["details"]["Artikelbezeichnung"] = description
            with self.subTest(description=description):
                self.assertIsNone(tyre_nominal_facts(product))
                self.assertEqual(run_candidates(tyre_row=product), {})

    def test_exact_load_index_and_explicit_xl_variant_are_required_when_given(self):
        for service in ("92 V XL", "95 V XL", "91 V XL"):
            product = tyre()
            product["details"]["Artikelbezeichnung"] = "SYNTHETIC 225/45 R17 " + service
            with self.subTest(service=service):
                self.assertEqual(run_candidates(tyre_row=product), {})
        product = tyre()
        product["details"]["Artikelbezeichnung"] = "SYNTHETIC 225/45 R17 91 V"
        facts = tyre_nominal_facts(product)
        self.assertIsNone(facts["xl"])
        self.assertTrue(run_candidates(tyre_row=product))
        self.assertIn("actualexecution", run_candidates(tyre_row=product)[rim()["selection_id"]][0]["unresolved"])

    def test_dot_demo_retread_runflat_and_special_execution_are_not_proposed(self):
        for marker in ("DOT", "DOT2022", "DEMO", "RETREAD", "runderneuert", "SSR", "RFT", "Runflat",
                       "Run-on-flat", "ZP", "ContiSeal", "ContiSilent", "HL"):
            product = tyre()
            product["details"]["Zusatzmerkmale"] = marker
            with self.subTest(marker=marker):
                self.assertEqual(run_candidates(tyre_row=product), {})
        for flag in ("DOT-Ausführung", "Demoware"):
            product = tyre()
            product["details"][flag] = "Vom Anbieter gemeldet"
            self.assertEqual(run_candidates(tyre_row=product), {})

    def test_speed_metadata_conflict_or_missing_explicit_dimensions_is_rejected(self):
        product = tyre()
        product["details"]["Geschwindigkeitsindex"] = "H"
        self.assertEqual(run_candidates(tyre_row=product), {})
        for field in ("width", "ratio", "diameter"):
            product = tyre()
            del product[field]
            self.assertIsNone(tyre_nominal_facts(product))

    def test_unreviewed_or_inconsistent_source_rows_cannot_create_candidates(self):
        for change in ({"visually_reviewed": False}, {"size": "235/45 R17"}, {"pdf_page": True},
                       {"permitted_rim_widths_in": [7.5, "bad"]}, {"load_variants": [{"load_index": 94, "xl": "yes"}]}):
            reviewed = registry()
            reviewed["rows"][0].update(change)
            with self.subTest(change=change):
                self.assertEqual(run_candidates(reviewed=reviewed), {})
        reviewed = registry()
        reviewed["rows"].append(deepcopy(reviewed["rows"][0]))
        self.assertEqual(run_candidates(reviewed=reviewed), {})

    def test_registry_metadata_cannot_expand_manufacturer_or_approval_scope(self):
        for key, value in (("brands", ["Continental", "Michelin"]), ("is_vehicle_approval", True),
                           ("construction", "ZR"), ("scope", "approved_all_vehicles"),
                           ("permitted_rim_contours", ["B"]), ("permitted_rim_contours", [{}]), ("schema_version", True)):
            reviewed = registry()
            reviewed[key] = value
            with self.subTest(key=key):
                self.assertEqual(run_candidates(reviewed=reviewed), {})
        for source in ({"url": "https://evil.invalid/fixture.pdf", "sha256": "1" * 64},
                       {"url": registry()["source"]["url"] + "?token=secret", "sha256": "1" * 64},
                       {"url": "https://user:pw@www.continental-reifen.de/fixture.pdf", "sha256": "1" * 64},
                       {"url": registry()["source"]["url"], "sha256": "not-a-hash"}):
            reviewed = registry()
            reviewed["source"] = source
            self.assertEqual(run_candidates(reviewed=reviewed), {})

    def test_missing_or_bad_catalogues_return_no_fallback(self):
        self.assertEqual(build_width_candidates({}, registry()), {})
        self.assertEqual(build_width_candidates({"reifen": "bad", "felgen": [rim()]}, registry()), {})
        self.assertEqual(build_width_candidates({"reifen": [tyre()], "felgen": [rim()]}, {}), {})
        self.assertEqual(build_width_candidates({"reifen": [tyre()], "felgen": [rim(category={})]}, registry()), {})

    def test_duplicate_product_id_cannot_choose_between_inconsistent_executions(self):
        tyres = [tyre(), tyre(width="235")]
        self.assertEqual(build_width_candidates({"reifen": tyres, "felgen": [rim()]}, registry()), {})
        rims = [rim(), rim(width="6")]
        self.assertEqual(build_width_candidates({"reifen": [tyre()], "felgen": rims}, registry()), {})


if __name__ == "__main__":
    unittest.main()
