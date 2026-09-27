"""Run with python scripts/test_bestellplan.py; no app, DB or network imports."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import pathlib
import sys
import unittest
from zoneinfo import ZoneInfo

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from werkstatt_bestellplan import group_orders, next_dispatch_at


BERLIN = ZoneInfo("Europe/Berlin")
UTC = timezone.utc


def order(**changes):
    result = {"id": 1, "order_requested": True, "supplier_id": "topcolor",
              "recipient": "bestellung@example.com", "recipient_verified": True,
              "product_id": 12, "article_number": "GR-50", "variant": "grün 50 mm",
              "quantity": "2", "unit": "Rolle", "max_total_cents": 1500, "urgent": False}
    result.update(changes)
    return result


class DispatchTimeTests(unittest.TestCase):
    def test_monday_before_exactly_and_after_noon(self):
        cutoff = datetime(2026, 9, 28, 12, tzinfo=BERLIN)
        for now in (cutoff - timedelta(microseconds=1), cutoff):
            self.assertEqual(next_dispatch_at(now, False), datetime(2026, 9, 28, 10, tzinfo=UTC))
        self.assertEqual(next_dispatch_at(cutoff + timedelta(microseconds=1), False),
                         datetime(2026, 10, 5, 10, tzinfo=UTC))

    def test_weekend_and_other_timezone_use_berlin_cutoff(self):
        self.assertEqual(next_dispatch_at(datetime(2026, 9, 27, 23, 59, tzinfo=UTC), False),
                         datetime(2026, 9, 28, 10, tzinfo=UTC))
        self.assertEqual(next_dispatch_at(datetime(2026, 9, 28, 11, tzinfo=UTC), False),
                         datetime(2026, 10, 5, 10, tzinfo=UTC))

    def test_spring_dst_preserves_monday_noon(self):
        self.assertEqual(next_dispatch_at(datetime(2026, 3, 23, 12, 1, tzinfo=BERLIN), False),
                         datetime(2026, 3, 30, 10, tzinfo=UTC))

    def test_autumn_dst_preserves_monday_noon(self):
        self.assertEqual(next_dispatch_at(datetime(2026, 10, 19, 12, 1, tzinfo=BERLIN), False),
                         datetime(2026, 10, 26, 11, tzinfo=UTC))
        for fold in (0, 1):
            self.assertEqual(next_dispatch_at(datetime(2026, 10, 25, 2, 30, tzinfo=BERLIN, fold=fold), False),
                             datetime(2026, 10, 26, 11, tzinfo=UTC))

    def test_urgent_is_immediate_and_utc(self):
        now = datetime(2026, 12, 28, 17, 32, 10, 123456, tzinfo=BERLIN)
        actual = next_dispatch_at(now, True)
        self.assertEqual(actual, now)
        self.assertIs(actual.tzinfo, UTC)

    def test_year_rollover(self):
        self.assertEqual(next_dispatch_at(datetime(2026, 12, 29, 8, tzinfo=BERLIN), False),
                         datetime(2027, 1, 4, 11, tzinfo=UTC))

    def test_naive_time_or_implicit_urgency_fail(self):
        with self.assertRaises(ValueError):
            next_dispatch_at(datetime(2026, 9, 28, 12), False)
        for urgent in (None, "false", 0, 1):
            with self.subTest(urgent=urgent), self.assertRaises(TypeError):
                next_dispatch_at(datetime.now(UTC), urgent)


class OrderGroupingTests(unittest.TestCase):
    def test_group_suppliers_recipients_and_urgency_separately(self):
        records = [order(), order(id=2, quantity=3), order(id=3, supplier_id="master"),
                   order(id=4, recipient="other@example.com"), order(id=5, urgent=True)]
        result = group_orders(records)
        self.assertEqual(result["invalid"], [])
        self.assertEqual(len(result["groups"]), 4)
        first = result["groups"][0]
        self.assertEqual([line["id"] for line in first["orders"]], ["1", "2"])
        self.assertEqual([line["quantity"] for line in first["orders"]], ["2", "3"])
        self.assertEqual(first["max_total_cents"], 3000)

    def test_variants_are_not_merged_and_input_is_not_modified(self):
        records = [order(), order(id=2, article_number="GR-30", variant="grün 30 mm"),
                   order(id=3, variant="rot 50 mm")]
        previous = deepcopy(records)
        result = group_orders(records)
        self.assertEqual(records, previous)
        self.assertEqual([line["variant"] for line in result["groups"][0]["orders"]],
                         ["grün 50 mm", "grün 30 mm", "rot 50 mm"])
        json.dumps(result)

    def test_invoice_fields_are_never_order_requests_or_quantity_fallbacks(self):
        source = order(quantity=None, invoice_quantity=48, historic_quantity=12)
        result = group_orders([source, order(id=2, order_requested=False)])
        self.assertFalse(result["groups"])
        self.assertIn("quantity", result["invalid"][0]["missing_fields"])
        self.assertIn("order_requested", result["invalid"][1]["missing_fields"])
        accepted = group_orders([order(invoice_quantity=100, invoice_total=90000)])
        line = accepted["groups"][0]["orders"][0]
        self.assertEqual(line["quantity"], "2")
        self.assertNotIn("invoice_quantity", line)
        self.assertNotIn("invoice_total", line)

    def test_missing_fields_are_actionable(self):
        invalid = group_orders([{}])["invalid"][0]
        self.assertEqual(set(invalid["missing_fields"]), {
            "id", "order_requested", "supplier_id", "recipient", "recipient_verified",
            "product_id_or_article_number", "variant", "quantity", "unit", "max_total_cents", "urgent"})
        self.assertTrue(all(invalid["errors"].values()))

    def test_unknown_unverified_or_multi_recipient_is_rejected(self):
        for address in ("", "unknown", "rechnung@example.com,other@example.com",
                        "A <rechnung@example.com>", "rechnung@example.com\r\nBcc: other@example.com"):
            with self.subTest(address=address):
                result = group_orders([order(recipient=address)])
                self.assertFalse(result["groups"])
                self.assertIn("recipient", result["invalid"][0]["errors"])
        result = group_orders([order(recipient_verified=False)])
        self.assertFalse(result["groups"])
        self.assertIn("recipient_verified", result["invalid"][0]["errors"])

    def test_all_duplicate_ids_are_rejected_including_cross_supplier(self):
        result = group_orders([order(), order(id="1", supplier_id="master"), order(id=3)])
        self.assertEqual(len(result["invalid"]), 2)
        self.assertTrue(all("id" in item["errors"] for item in result["invalid"]))
        self.assertEqual([line["id"] for line in result["groups"][0]["orders"]], ["3"])

    def test_exact_product_or_supplier_article_is_required(self):
        for changes in ({"product_id": None}, {"article_number": None}):
            self.assertTrue(group_orders([order(**changes)])["groups"])
        result = group_orders([order(product_id=None, article_number=None, product_name="Klebeband")])
        self.assertFalse(result["groups"])
        self.assertIn("product_id_or_article_number", result["invalid"][0]["errors"])

    def test_cap_and_flags_cannot_be_inferred_or_truthy_strings(self):
        for changes, field in (({"max_total_cents": 0}, "max_total_cents"),
                               ({"max_total_cents": True}, "max_total_cents"),
                               ({"max_total_cents": "1500"}, "max_total_cents"),
                               ({"urgent": "false"}, "urgent"),
                               ({"order_requested": 1}, "order_requested"),
                               ({"recipient_verified": "yes"}, "recipient_verified")):
            with self.subTest(changes=changes):
                result = group_orders([order(**changes)])
                self.assertFalse(result["groups"])
                self.assertIn(field, result["invalid"][0]["errors"])

    def test_positive_exact_quantities_only(self):
        for value, expected in ((2, "2"), ("2,50", "2.5"), (Decimal("0.250"), "0.25")):
            self.assertEqual(group_orders([order(quantity=value)])["groups"][0]["orders"][0]["quantity"], expected)
        for value in (0, -1, True, 1.5, "2 Rollen", "1,000.50", Decimal("NaN"), Decimal("Infinity")):
            with self.subTest(value=value):
                self.assertFalse(group_orders([order(quantity=value)])["groups"])

    def test_non_mapping_and_empty_input(self):
        self.assertEqual(group_orders([]), {"groups": [], "invalid": []})
        self.assertIn("record", group_orders([None])["invalid"][0]["errors"])


if __name__ == "__main__":
    unittest.main()
