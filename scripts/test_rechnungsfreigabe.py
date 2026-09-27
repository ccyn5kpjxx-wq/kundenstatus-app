"""Offline policy tests: supplier metadata only, never invoices or credentials."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from werkstatt_rechnungsfreigabe import classify_invoice_source, normalize_supplier


class SourceApprovalTests(unittest.TestCase):
    def test_main_supplier_spelling_variants_preserve_original_name(self):
        for supplier in ("Topcolor", "TOP-COLOR GmbH", "Top Color Autolackierbedarf GmbH",
                         "Top-Color Gmbh-Hermannstr.", "Top-Color Gmbh-", "Car-Parts GmbH", "CarParts Automotive GmbH",
                         "Car Parts Gmbh + Co. KG", "Car Parts Gmbh&Co.kg",
                         "Tech-Masters Deutschland GmbH", "TECH MASTERS GmbH"):
            with self.subTest(supplier=supplier):
                result = classify_invoice_source({"contact_name": supplier, "voucher_type": "purchaseinvoice"})
                self.assertEqual(result["decision"], "allow")
                self.assertTrue(result["allowed"])
                self.assertEqual(result["supplier"], supplier)

    def test_other_workshop_supplier_names_require_explicit_classification(self):
        for supplier in ("Adolf Würth GmbH & Co. KG", "Albert Berner Deutschland GmbH", "Theodor Förch GmbH & Co. KG",
                         "Stahlgruber GmbH", "WM SE", "PV Automotive GmbH", "Johannes J. Matthies GmbH & Co. KG"):
            with self.subTest(supplier=supplier):
                result = classify_invoice_source(supplier)
                self.assertEqual(result["decision"], "review")

    def test_generic_tech_material_parts_and_unknown_marketplace_require_review(self):
        for supplier in ("Tech Solutions GmbH", "Master Finance", "Car Parts Consulting GmbH", "Topcolor Partner XYZ",
                         "Materialhandel Muster", "Amazon", "Unknown GmbH", "Super Würth Services", "WM GmbH"):
            with self.subTest(supplier=supplier):
                result = classify_invoice_source(supplier)
                self.assertEqual(result["decision"], "review")
                self.assertFalse(result["allowed"])
                self.assertTrue(result["requires_review"])

    def test_sensitive_sources_block_even_with_explicit_allowlist(self):
        groups = {
            "banking": ("Volksbank Mosbach eG", "Sparkasse Neckartal", "Deutsche Bank AG", "PayPal", "Topcolor Bankgebühren"),
            "insurance": ("Allianz Versicherung AG", "HUK-Coburg", "R+V Allgemeine Versicherung AG"),
            "contributions": ("Deutsche Rentenversicherung", "AOK", "Handwerkskammer", "IHK", "BGHM", "Rundfunkbeitrag"),
            "tax_authority": ("Finanzamt Mosbach", "Steuerberater Muster", "Landratsamt", "Stadtkasse"),
            "medical": ("Arztpraxis Muster", "Zahnarzt Müller", "Medizinisches Labor", "Apotheke"),
            "collection": ("Inkasso GmbH", "Muster Forderungsmanagement", "Gerichtsvollzieher"),
        }
        for group, names in groups.items():
            for supplier in names:
                with self.subTest(group=group, supplier=supplier):
                    result = classify_invoice_source(supplier, allowed_suppliers=[supplier])
                    self.assertEqual(result["decision"], "block")
                    self.assertFalse(result["allowed"])

    def test_sensitive_filename_blocks_even_known_supplier(self):
        for key in ("reference", "original_name", "voucher_number"):
            result = classify_invoice_source({"supplier": "Topcolor GmbH", key: "Kontoauszug September.pdf"})
            self.assertEqual(result["decision"], "block")
            self.assertEqual(result["rule"], "banking")

    def test_unknown_supplier_needs_explicit_exact_server_allowlist(self):
        source = {"lieferant": "Muster Lackierbedarf GmbH"}
        self.assertEqual(classify_invoice_source(source)["decision"], "review")
        result = classify_invoice_source(source, allowed_suppliers=["MUSTER-LACKIERBEDARF GmbH"])
        self.assertEqual(result["decision"], "allow")
        self.assertEqual(result["rule"], "explicit_supplier")
        for invalid in (["Muster*"], ["Lackierbedarf"], "Muster Lackierbedarf GmbH", {"Muster Lackierbedarf GmbH": True}):
            self.assertEqual(classify_invoice_source(source, allowed_suppliers=invalid)["decision"], "review")

    def test_missing_or_conflicting_supplier_is_not_guessed(self):
        for source in ({}, {"supplier": ""}, {"supplier": "Sammellieferant"}, None, 123,
                       {"supplier": "Topcolor", "contact_name": "CarParts"}):
            self.assertEqual(classify_invoice_source(source)["decision"], "review")
        self.assertEqual(classify_invoice_source({"supplier": "Topcolor", "lieferant": "TOP-COLOR"})["decision"], "allow")

    def test_non_invoice_metadata_is_blocked(self):
        for data in ({"voucher_type": "salesinvoice"}, {"voucherType": "purchasecreditnote"}, {"beleg_typ": "lieferschein"}):
            self.assertEqual(classify_invoice_source({"supplier": "Topcolor", **data})["decision"], "block")

    def test_raw_invoice_and_financial_data_cannot_authorize_unknown_supplier(self):
        result = classify_invoice_source({"supplier": "Muster", "raw_json": '{"supplier":"Topcolor"}',
                                          "extrahierter_text": "Tech-Masters Artikel", "total_amount": 98765,
                                          "allowed_suppliers": ["Muster"]})
        self.assertEqual(result["decision"], "review")
        self.assertNotIn("98765", str(result))
        self.assertNotIn("raw_json", result)

    def test_normalization_handles_umlauts_legal_punctuation_and_length(self):
        self.assertEqual(normalize_supplier("  Würth GmbH & Co. KG "), "wuerth gmbh co kg")
        self.assertEqual(normalize_supplier("x" * 501), "")
        self.assertEqual(classify_invoice_source("x" * 501)["decision"], "review")
        self.assertEqual(classify_invoice_source("Topcolor" + " " * 500 + "Bank")["decision"], "review")


if __name__ == "__main__":
    unittest.main()
