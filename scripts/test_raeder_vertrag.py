"""Offline category and immutable contract-copy regressions; no provider IO."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import reifen_vertrag as contract
from scripts.test_reifen_vertrag import order


def rim_order():
    value = order()
    value.update(produktart="felgen", montage_datum="", montage_uhrzeit="",
                 amount_total=25012)
    value["rechtstexte"]["produktarten"] = ["reifen", "felgen"]
    value["produkt"] = {
        "artikel_id": "414140", "bezeichnung": "Borbet XR", "hersteller": "Borbet",
        "profil": "XR", "hersteller_artikelnummer": "XR-EXACT-001", "ean": None,
        "material": "alu", "kennungspassung": "nicht_geprueft",
        "preis": {"einzel_brutto": "62.53", "gesamt_felgen_brutto": "250.12",
                  "versand_brutto": "0.00", "gesamt_brutto": "250.12"},
        "einkaufspreis": "secret-never-in-contract", "internal": "secret-provider-field"}
    return value


class WheelContract(unittest.TestCase):
    def test_existing_tire_order_remains_a_tire_contract(self):
        _, subject, body, saved = contract._order_body(order())
        self.assertIn("Reifenbestellung", subject)
        self.assertIn("Größe: 205/55 R16 91V", body)
        self.assertEqual(saved["produktart"], "reifen")

    def test_rim_contract_preserves_exact_identity_quantity_price_and_scope(self):
        value = rim_order()
        before = deepcopy(value)
        recipient, subject, body, saved = contract._order_body(value)
        self.assertEqual(value, before)
        self.assertEqual(recipient, "anna@example.invalid")
        self.assertIn("Felgenbestellung", subject)
        self.assertIn("Artikel: 414140", body)
        self.assertIn("Herstellerartikelnummer: XR-EXACT-001", body)
        self.assertIn("Menge: 4", body)
        self.assertIn("250,12 EUR", body)
        self.assertIn("Fahrzeugpassung: nicht geprüft", body)
        self.assertNotIn("Reifenbestellung", body)
        self.assertNotIn("Größe:", body)
        self.assertNotIn("secret-", body)
        self.assertNotIn("gesamt_reifen_brutto", saved["preis"])
        self.assertEqual(saved["produktart"], "felgen")
        self.assertEqual(saved["produkt"]["ean"], "")
        self.assertEqual(saved["rechtstexte"]["produktarten"], ["reifen", "felgen"])

    def test_nontext_product_identity_cannot_silently_become_empty_text(self):
        for invalid in (False, 0, [], {}):
            with self.subTest(invalid=invalid):
                value = rim_order()
                value["produkt"]["hersteller_artikelnummer"] = invalid
                with self.assertRaises(ValueError):
                    contract._order_body(value)

    def test_rims_cannot_use_legacy_tire_terms_or_malformed_scope(self):
        for scope in (None, [], ["reifen"], "felgen", ["felgen", "felgen"], ["felgen", {}]):
            with self.subTest(scope=scope):
                value = rim_order()
                value["rechtstexte"]["produktarten"] = scope
                with self.assertRaises(ValueError):
                    contract._order_body(value)

    def test_declared_scope_also_binds_existing_tire_orders(self):
        value = order()
        value["rechtstexte"]["produktarten"] = ["felgen"]
        with self.assertRaises(ValueError):
            contract._order_body(value)

    def test_unknown_or_conflicting_product_type_is_rejected(self):
        for category in ("komplettraeder", "accessories", None, {}, True):
            with self.subTest(category=category):
                value = rim_order()
                value["produktart"] = category
                with self.assertRaises(ValueError):
                    contract._order_body(value)
        value = rim_order()
        value["produkt"]["produktart"] = "reifen"
        with self.assertRaises(ValueError):
            contract._order_body(value)

    def test_rim_copy_rejects_inconsistent_price_or_missing_identity(self):
        for key, changed in (("gesamt_felgen_brutto", "250.13"), ("gesamt_brutto", "250.13"),
                             ("einzel_brutto", "-62.53"), ("versand_brutto", "1.00")):
            with self.subTest(key=key):
                value = rim_order()
                value["produkt"]["preis"][key] = changed
                with self.assertRaises(ValueError):
                    contract._order_body(value)
        for change in ("amount", "quantity", "identity"):
            with self.subTest(change=change):
                value = rim_order()
                if change == "amount": value["amount_total"] += 1
                if change == "quantity": value["anzahl"] = 2
                if change == "identity": value["produkt"]["artikel_id"] = ""
                with self.assertRaises(ValueError):
                    contract._order_body(value)

    def test_unconfigured_rim_assembly_cannot_enter_the_contract(self):
        value = rim_order()
        value["montage_datum"] = "2026-11-07"
        with self.assertRaises(ValueError):
            contract._order_body(value)

    def test_unconfirmed_online_payment_never_becomes_an_acceptance(self):
        for status in ("offen", "autorisiert", "requires_capture"):
            with self.subTest(status=status):
                value = rim_order()
                value.update(zahlungsart="online", status="bestaetigt", zahlung_status=status)
                with self.assertRaises(ValueError):
                    contract._order_body(value)


if __name__ == "__main__":
    unittest.main()
