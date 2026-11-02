"""Draft snapshots must not silently become approved or share mutable state."""
import hashlib
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reifen_rechtstexte import reifen_rechtstexte, rechtstexte_entwurf_text


class ReifenRechtstexteTest(unittest.TestCase):
    def test_default_and_demo_are_both_unapproved_drafts(self):
        for demo in (False, True):
            with self.subTest(demo=demo):
                data = reifen_rechtstexte(demo=demo)
                self.assertEqual(data["status"], "entwurf")
                self.assertIs(data["freigegeben"], False)
                self.assertTrue(data["version"].startswith("demo:" if demo else "entwurf:"))
                self.assertTrue(data["offene_entscheidungen"])
                self.assertTrue(all(data[key] for key in ("agb", "lieferung", "datenschutz", "widerruf", "widerrufsformular")))
        with self.assertRaises(ValueError):
            reifen_rechtstexte(demo="true")

    def test_independent_snapshots_preserve_previous_contract_copy(self):
        first = reifen_rechtstexte()
        original = json.loads(json.dumps(first))
        second = reifen_rechtstexte()
        second["anbieter"]["firma"] = "falscher Anbieter"
        second["quellen"][0]["url"] = "https://example.invalid"
        second["offene_entscheidungen"].clear()
        self.assertEqual(first, original)
        self.assertEqual(reifen_rechtstexte(), original)

    def test_digest_binds_exact_json_snapshot_and_demo_variant_differs(self):
        data = reifen_rechtstexte()
        digest = data.pop("sha256")
        encoded = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.assertEqual(hashlib.sha256(encoded).hexdigest(), digest)
        self.assertNotEqual(reifen_rechtstexte(demo=True)["sha256"], digest)

    def test_human_review_copy_contains_same_version_and_complete_core_texts(self):
        data = reifen_rechtstexte()
        rendered = rechtstexte_entwurf_text()
        for key in ("version", "sha256", "agb", "lieferung", "widerruf", "widerrufsformular", "datenschutz"):
            self.assertIn(data[key], rendered)
        self.assertIn("INTERN – VOR EINER VERÖFFENTLICHUNG OFFEN", rendered)
        self.assertNotIn("ec.europa.eu/consumers/odr", rendered)

    def test_return_policy_snapshot_matches_operator_decision_and_catalog_scope(self):
        data = reifen_rechtstexte()
        self.assertIn("Sie tragen die unmittelbaren Kosten der Rücksendung.", data["widerruf_waren"])
        self.assertIn("persönlich", data["widerruf_waren"])
        self.assertIn("in Mosbach", data["widerruf_waren"])
        self.assertIn("nicht paketversandfähige Ausführungen können hier nicht bestellt", data["lieferung"])
        self.assertNotIn("Kosten der Rücksendung übernehmen wir", rechtstexte_entwurf_text())

    def test_official_form_uses_current_seller_and_leaves_individual_fields_blank(self):
        data = reifen_rechtstexte()
        form = data["widerrufsformular"]
        self.assertEqual(data["anbieter"]["firma"], "Gärtner GmbH Karosserie & Lack")
        for expected in ("Muster-Widerrufsformular", data["anbieter"]["firma"],
                         data["anbieter"]["strasse"], data["anbieter"]["email"],
                         "Bestellt am (*)/erhalten am (*)", "Name des/der Verbraucher(s)",
                         "Anschrift des/der Verbraucher(s)",
                         "Unterschrift des/der Verbraucher(s) (nur bei Mitteilung auf Papier)",
                         "(*) Unzutreffendes streichen."):
            self.assertIn(expected, form)
        self.assertIn("______________________________________________________________________", form)
        self.assertNotIn("Gärtner GmbH Karosserie + Lack", rechtstexte_entwurf_text())
        self.assertIn("https://auto-lackierzentrum.de/impressum", rechtstexte_entwurf_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
