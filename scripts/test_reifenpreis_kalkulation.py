"""Real price invariants, cent boundaries and missing-price rejection; no app IO."""
from copy import deepcopy
from decimal import Decimal, localcontext
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from reifenpreis_kalkulation import berechne_reifenpreis
from reifenservice import berechne_reifenservice, berechne_check24_reifenservice


class ReifenpreisKalkulationTest(unittest.TestCase):
    def calc(self, **changes):
        args = {"einkauf_netto": "100.00", "menge": 4,
                "bezugskosten_netto": "10.00", "preise_bestaetigt": True}
        args.update(changes)
        return berechne_reifenpreis(**args)

    def test_ten_percent_markup_then_vat_and_one_shipping_charge(self):
        result = self.calc()
        self.assertEqual(result["aufschlag_prozent"], "10.00")
        self.assertEqual(Decimal(result["reifen_kalkulationsbasis_einzel_netto"]), Decimal("110"))
        self.assertEqual(result["einzelpreis_brutto_reifen"], "130.90")
        self.assertEqual(result["gesamt_brutto_reifen"], "523.60")
        self.assertEqual(result["versand_brutto"], "11.90")
        self.assertEqual(result["montage_brutto"], "0.00")
        self.assertEqual(result["gesamt_brutto"], "535.50")
        self.assertEqual(result["gesamt_netto_cent"], 45000)
        self.assertEqual(result["gesamt_mwst_cent"], 8550)

    def test_markup_is_not_a_ten_percent_margin(self):
        result = self.calc(einkauf_netto="90", menge=1, bezugskosten_netto="0", mwst_satz="0")
        self.assertEqual(result["einzelpreis_brutto_reifen"], "99.00")
        self.assertNotEqual(result["einzelpreis_brutto_reifen"], "100.00")

    def test_unit_price_rounding_and_total_always_agree(self):
        result = self.calc(einkauf_netto="0.05", menge=3, bezugskosten_netto="0")
        self.assertEqual(result["einzelpreis_brutto_reifen_cent"], 7)
        self.assertEqual(result["gesamt_brutto_reifen_cent"], 21)
        self.assertEqual(result["gesamt_brutto_reifen_cent"], 3 * result["einzelpreis_brutto_reifen_cent"])
        self.assertEqual(result["gesamt_brutto_cent"], result["gesamt_netto_cent"] + result["gesamt_mwst_cent"])

    def test_four_decimal_supplier_price_is_retained_before_markup(self):
        result = self.calc(einkauf_netto=Decimal("80.1234"), menge="2", bezugskosten_netto=Decimal("0"))
        self.assertEqual(result["einkauf_einzel_netto"], "80.1234")
        self.assertEqual(Decimal(result["reifen_kalkulationsbasis_einzel_netto"]), Decimal("88.135740"))
        self.assertEqual(result["einzelpreis_brutto_reifen"], "104.88")
        self.assertEqual(result["gesamt_brutto"], "209.76")

    def test_shipping_is_not_multiplied_by_quantity_or_marked_up(self):
        one = self.calc(menge=1)
        eight = self.calc(menge=8)
        self.assertEqual(one["versand_brutto_cent"], eight["versand_brutto_cent"])
        self.assertEqual(eight["versand_brutto_cent"], 1190)
        self.assertTrue(eight["bezugskosten_einmal_pro_angebot"])

    def test_explicit_confirmed_free_shipping_is_allowed(self):
        result = self.calc(bezugskosten_netto="0.00")
        self.assertEqual(result["versand_brutto_cent"], 0)
        self.assertEqual(result["gesamt_brutto"], "523.60")
        with self.assertRaises(TypeError):
            berechne_reifenpreis(einkauf_netto="100", menge=4, preise_bestaetigt=True)

    def test_missing_or_unconfirmed_prices_never_create_zero_fallback(self):
        for field in ("einkauf_netto", "bezugskosten_netto"):
            for invalid in (None, "", "unbekannt", "auf Anfrage", False):
                with self.subTest(field=field, invalid=invalid), self.assertRaises(ValueError):
                    self.calc(**{field: invalid})
        for flag in (False, None, 1, "true", "ja"):
            with self.subTest(flag=flag), self.assertRaises(ValueError):
                self.calc(preise_bestaetigt=flag)

    def test_negative_nonfinite_float_and_ambiguous_money_is_rejected(self):
        for field in ("einkauf_netto", "bezugskosten_netto", "mwst_satz"):
            for invalid in ("-1", Decimal("-0.01"), float("nan"), float("inf"), 19.0,
                            Decimal("NaN"), Decimal("Infinity"), "100,00", "1e2", "1 000", True):
                with self.subTest(field=field, invalid=invalid), self.assertRaises(ValueError):
                    self.calc(**{field: invalid})

    def test_price_precision_and_hard_limits_are_enforced(self):
        cases = ({"einkauf_netto": "0"}, {"einkauf_netto": "0.0001"},
                 {"einkauf_netto": "1.00001"}, {"einkauf_netto": "10000.0001"},
                 {"bezugskosten_netto": "1.001"}, {"bezugskosten_netto": "10000.01"},
                 {"mwst_satz": "30.01"}, {"mwst_satz": "19.001"},
                 {"einkauf_netto": "10000", "menge": 8})
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.calc(**case)

    def test_only_whole_quantities_one_to_eight(self):
        for invalid in (None, 0, -1, 9, 4.0, Decimal("4"), True, "04", "4.0", "٤"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.calc(menge=invalid)
        self.assertEqual(self.calc(menge="8")["menge"], 8)

    def test_approved_mounting_added_once_without_markup_or_second_vat(self):
        mounting = berechne_reifenservice([{"preis_id": "montage_13_16", "menge": 4}])
        original = deepcopy(mounting)
        result = self.calc(montage_kalkulation=mounting)
        self.assertEqual(result["montage_brutto"], "90.00")
        self.assertEqual(result["montage_mwst_cent"], 1437)
        self.assertEqual(result["gesamt_brutto"], "625.50")
        self.assertEqual(result["gesamt_netto_cent"], 52563)
        self.assertEqual(result["gesamt_mwst_cent"], 9987)
        self.assertEqual(mounting, original)

    def test_mounting_keeps_own_tax_when_tire_tax_differs(self):
        mounting = berechne_reifenservice([{"preis_id": "montage_13_16", "menge": 4}])
        result = self.calc(mwst_satz="7", montage_kalkulation=mounting)
        self.assertEqual(result["mwst_satz"], "7.00")
        self.assertEqual(result["montage_mwst_satz"], "19.00")
        self.assertEqual(result["montage_brutto_cent"], 9000)
        self.assertEqual(result["montage_mwst_cent"], 1437)
        self.assertEqual(result["gesamt_brutto"], "571.50")
        self.assertEqual(result["gesamt_netto_cent"] + result["gesamt_mwst_cent"], 57150)

    def test_tampered_external_wrong_quantity_and_non_mounting_snapshots_rejected(self):
        valid = berechne_reifenservice([{"preis_id": "montage_13_16", "menge": 4}])
        tampered = deepcopy(valid)
        tampered["gesamt"]["brutto_cent"] = 1
        cases = [tampered, 9000, {"gesamt": {"brutto_cent": 9000}},
                 berechne_check24_reifenservice("AB123456", "Montage", "90.00"),
                 berechne_reifenservice([{"preis_id": "radwechsel_bis17", "menge": 1}]),
                 berechne_reifenservice([{"preis_id": "montage_13_16", "menge": 2}])]
        for snapshot in cases:
            with self.subTest(snapshot=snapshot), self.assertRaises(ValueError):
                self.calc(montage_kalkulation=snapshot)
        with self.assertRaises(ValueError):
            self.calc(menge=8, montage_kalkulation=valid)

    def test_mixed_mounting_sizes_and_addons_preserve_catalogue_total(self):
        mounting = berechne_reifenservice([{"preis_id": "montage_17_18", "menge": 2},
            {"preis_id": "montage_19_20", "menge": 2}, {"preis_id": "runflat", "menge": 4},
            {"preis_id": "entsorgung", "menge": 4}], lose_raeder=True)
        result = self.calc(montage_kalkulation=mounting)
        self.assertEqual(result["montage_brutto_cent"], mounting["gesamt"]["brutto_cent"])
        self.assertEqual(result["montage_brutto"], "126.00")
        self.assertEqual(result["gesamt_brutto"], "661.50")

    def test_result_is_json_safe_and_cent_sums_reconcile(self):
        result = self.calc(einkauf_netto="89.99", menge=3, bezugskosten_netto="5.99")
        self.assertEqual(json.loads(json.dumps(result)), result)
        self.assertFalse(any(isinstance(value, float) for value in result.values()))
        self.assertEqual(result["gesamt_brutto_cent"], result["gesamt_brutto_reifen_cent"]
                         + result["versand_brutto_cent"] + result["montage_brutto_cent"])
        self.assertEqual(result["gesamt_brutto_cent"], result["gesamt_netto_cent"] + result["gesamt_mwst_cent"])

    def test_calculation_has_own_sufficient_decimal_precision(self):
        expected = self.calc(einkauf_netto="999.9999", menge=8, bezugskosten_netto="9999.99")
        with localcontext() as context:
            context.prec = 5
            actual = self.calc(einkauf_netto="999.9999", menge=8, bezugskosten_netto="9999.99")
        self.assertEqual(actual, expected)

    def test_mounting_validation_also_uses_own_decimal_precision(self):
        mounting = berechne_reifenservice([{"preis_id": "montage_13_16", "menge": 4}])
        expected = self.calc(einkauf_netto="999.9999", bezugskosten_netto="9999.99", montage_kalkulation=mounting)
        with localcontext() as context:
            context.prec = 3
            actual = self.calc(einkauf_netto="999.9999", bezugskosten_netto="9999.99", montage_kalkulation=mounting)
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main(verbosity=2)
