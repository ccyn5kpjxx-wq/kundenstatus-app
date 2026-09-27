"""Run with python scripts/test_artikel_identity.py; no app/DB/network imports."""

from decimal import Decimal
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from werkstatt_artikel_identity import catalog_identity, identity_fields, parse_unit_price


class ArticleIdentityTests(unittest.TestCase):
    def test_normalizes_case_unicode_and_whitespace_only(self):
        expected = catalog_identity("Supplier", "AB-12", "Klebeband grün 30 mm", "6 Rollen", "Rolle")
        self.assertEqual(expected, catalog_identity(
            " SUPPLIER ", " ab-12 ", " Klebeband  gru\u0308n 30 mm ", "6\u00a0Rollen", "ROLLE",
        ))
        self.assertEqual(len(expected), 64)
        self.assertEqual(identity_fields("Supplier", "AB-12", "Band 30 mm"),
                         ("supplier", "ab-12", "band 30 mm", "", ""))

    def test_supplier_sku_and_variants_cannot_merge(self):
        base = ("Supplier", "AB-12", "Klebeband grün 30 mm", "6 Rollen", "Rolle")
        original = catalog_identity(*base)
        variants = [
            (0, "Other supplier"), (1, "AB12"), (1, "AB/12"),
            (1, "AB-13"), (1, ""), (2, "Klebeband grün 50 mm"),
            (2, "Klebeband rot 30 mm"), (2, "Klebeband grün 30 cm"),
            (3, "12 Rollen"), (3, ""), (4, "Packung"), (4, ""),
        ]
        for field, value in variants:
            with self.subTest(field=field, value=value):
                changed = list(base)
                changed[field] = value
                self.assertNotEqual(original, catalog_identity(*changed))

    def test_same_supplier_and_name_without_sku_can_be_proposed(self):
        key = catalog_identity("Supplier", "", "Klebeband grün 30 mm")
        self.assertIsNotNone(key)
        self.assertNotEqual(key, catalog_identity("Supplier", "", "Klebeband grün 50 mm"))

    def test_requires_supplier_and_product_name(self):
        for supplier in ("", " ", None, "Lieferant offen", "UNKNOWN"):
            with self.subTest(supplier=supplier):
                self.assertIsNone(catalog_identity(supplier, "AB-12", "Band"))
        self.assertIsNone(catalog_identity("Supplier", "AB-12", ""))

    def test_field_boundaries_do_not_collide(self):
        self.assertNotEqual(catalog_identity("A|B", "C", "Band"),
                            catalog_identity("A", "B|C", "Band"))


class UnitPriceTests(unittest.TestCase):
    def test_explicit_decimal_prices(self):
        cases = {
            "12,50": "12.50", "12.50": "12.50", "12,5": "12.5",
            "12.5 EUR": "12.5", " EUR 12,50 ": "12.50", "12,50 €": "12.50",
            "1.234,56": "1234.56", "1,234.56": "1234.56",
            "1 234,56": "1234.56", "1\u202f234.56": "1234.56",
            "1.234.567": "1234567", "1,234,567": "1234567",
            "1 234": "1234", "1234": "1234", "0": "0", "0,00": "0.00",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(parse_unit_price(value), Decimal(expected))

    def test_ambiguous_and_invalid_prices_are_unknown(self):
        cases = (
            "1.234", "12,345", "0.125", "1234.567", "1,234,56", "1.234.56",
            "12 34,50", "1  234,56", "1,23.45", "1.23,45", "1.234,567",
            "12.50 netto", "2 x 12,50", "12,50 / 25,00", "-12,50", "+12.50",
            "12,50 USD", "EUR 12,50 €", "NaN", "Infinity", "1e2", "", " ",
            None, True, False, 12.50, Decimal("NaN"), Decimal("Infinity"), -1,
        )
        for value in cases:
            with self.subTest(value=value):
                self.assertIsNone(parse_unit_price(value))

    def test_exact_numeric_types_stay_exact(self):
        self.assertEqual(parse_unit_price(1250), Decimal("1250"))
        self.assertEqual(parse_unit_price(Decimal("12.50")), Decimal("12.50"))
        self.assertEqual(parse_unit_price("0,10") + parse_unit_price("0.20"), Decimal("0.30"))


if __name__ == "__main__":
    unittest.main()
