"""Synthetic offline tests: no account, provider feed, credentials or HTTP."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import StringIO
from pathlib import Path
import csv
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import shop_alzura_feed as feed
from shop_catalog_public import PublicCatalog


NOW = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)


def row(**changes):
    value = dict.fromkeys(feed.HEADERS, "")
    value.update(id="123456", type="S", brandname="Example", profile="Tour",
                 articlename="Example Tour 205/55 R16 91V", manufacturer_number="EX-205-16",
                 width="205", height="55", inner_diameter="16", tyre_type="R",
                 load_index="91", speed_index="V", runflat="0", ms="0", **{"3pmfs": "0"},
                 da_decke="0", demo="0", dot="0", price_1="100.005", price_4="95.00",
                 availability="8", image="https://media1.tyre-shopping.com/images/example.jpg",
                 image_tn="https://media1.tyre-shopping.com/images/example-small.jpg",
                 tyrelabel_link="https://media2.tyre-shopping.com/tyrelabel2/example.pdf")
    value.update(changes)
    return value


def metadata(**changes):
    value = feed.FeedMetadata(archive_sha256="a" * 64, member="tmp/123.csv",
                              supplier_observed_at=NOW - timedelta(minutes=1),
                              timestamp_evidence="synthetic-source-review",
                              currency="EUR", unit_net_fields=((1, "price_1"), (4, "price_4")),
                              price_semantics_evidence="synthetic-schema-review")
    return replace(value, **changes)


class FeedTests(unittest.TestCase):
    def normalize(self, value=None, meta=None, now=NOW):
        return feed.normalize_row(value or row(), metadata=meta or metadata(), row_number=1, now=now)

    def archive(self, folder, rows, *, headers=None, extra=None):
        text = StringIO(newline="")
        writer = csv.DictWriter(text, fieldnames=headers or feed.HEADERS, delimiter="|", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        path = Path(folder) / "synthetic.zip"
        with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
            archive.writestr("tmp/123.csv", text.getvalue().encode("utf-8-sig"))
            if extra:
                archive.writestr(extra, b"unwanted")
        return path, metadata(archive_sha256=sha256(path.read_bytes()).hexdigest())

    def test_explicit_unit_net_prices_round_unit_then_quantity(self):
        product, evidence = self.normalize()
        self.assertEqual(product["quote_status"], "current")
        prices = product["quote_public"]["prices_by_quantity"]
        self.assertEqual(set(prices), {"1", "4"})
        self.assertEqual(prices["1"]["unit_gross_cent"], 13091)
        self.assertEqual(prices["4"]["unit_gross_cent"], 12436)
        self.assertEqual(prices["4"]["items_gross_cent"], 49744)
        self.assertEqual(evidence["price_semantics"]["net_markup_percent"], "10.00")
        self.assertFalse(evidence["public_display_ready"])
        self.assertEqual(evidence["rsu_match_status"], "not_confirmed")
        self.assertIsNone(product["identifiers"]["ean"])

    def test_missing_semantics_or_currency_does_not_invent_quotes(self):
        product, _ = self.normalize(meta=metadata(unit_net_fields=(), price_semantics_evidence=""))
        self.assertEqual(product["quote_status"], "price_semantics_unconfirmed")
        self.assertIsNone(product["quote_public"])
        product, _ = self.normalize(meta=metadata(currency=None, unit_net_fields=(), price_semantics_evidence=""))
        self.assertEqual(product["quote_status"], "currency_unconfirmed")
        for changes in ({"currency": "USD"}, {"unit_net_fields": ((2, "price_1"),)},
                        {"price_semantics_evidence": ""}, {"unit_net_fields": ((1, []),)}):
            with self.subTest(changes=changes), self.assertRaises(feed.FeedError):
                self.normalize(meta=metadata(**changes))

    def test_timestamps_never_come_from_import_time_and_expired_is_not_current(self):
        product, evidence = self.normalize(meta=metadata(supplier_observed_at=NOW - timedelta(days=1)))
        self.assertEqual(product["quote_status"], "expired")
        self.assertIsNone(product["quote_public"])
        self.assertEqual(evidence["source"]["observed_at"], (NOW - timedelta(days=1)).isoformat())
        for value in (NOW.replace(tzinfo=None), "bad", NOW + timedelta(seconds=1)):
            with self.subTest(value=value), self.assertRaises(feed.FeedError):
                self.normalize(meta=metadata(supplier_observed_at=value))

    def test_conditions_geometry_stock_and_invalid_price_fail_closed(self):
        cases = [({"demo": "1"}, "restricted_condition"), ({"dot": "1"}, "restricted_condition"),
                 ({"da_decke": "1"}, "restricted_condition"), ({"runflat": ""}, "condition_unconfirmed"),
                 ({"height": ""}, "dimensions_unconfirmed"), ({"width": "999"}, "dimensions_unconfirmed"),
                 ({"outer_diameter": "20"}, "dimensions_unconfirmed"), ({"tyre_type": "B"}, "dimensions_unconfirmed"),
                 ({"load_index": "?"}, "dimensions_unconfirmed"), ({"speed_index": "?"}, "dimensions_unconfirmed"),
                 ({"availability": "unknown"}, "stock_unconfirmed"), ({"availability": "0"}, "price_or_stock_unconfirmed"),
                 ({"price_1": "NaN", "price_4": "-1"}, "price_or_stock_unconfirmed")]
        for changes, status in cases:
            with self.subTest(changes=changes):
                product, _ = self.normalize(row(**changes))
                self.assertEqual(product["quote_status"], status)
                self.assertIsNone(product["quote_public"])
        product, _ = self.normalize(row(availability="2"))
        self.assertEqual(set(product["quote_public"]["prices_by_quantity"]), {"1"})

    def test_identity_is_stable_across_quotes_but_not_variant_changes(self):
        product, _ = self.normalize()
        later, _ = self.normalize(row(price_1="200", availability="20"))
        self.assertEqual(product["selection_id"], later["selection_id"])
        variant, _ = self.normalize(row(width="215"))
        self.assertNotEqual(product["selection_id"], variant["selection_id"])
        self.assertIsNone(self.normalize(row(type="C")))
        self.assertIsNone(self.normalize(row(type="2055516")))

    def test_public_projection_requires_allowlist_media_and_checks_ongoing_expiry(self):
        product, evidence = self.normalize()
        sid = product["selection_id"]
        hidden = PublicCatalog([product], released_ids=[])
        self.assertEqual(hidden.page(now=NOW)["total"], 0)
        catalog = PublicCatalog([product], released_ids=[sid])
        current = catalog.get(sid, now=NOW)
        self.assertEqual(current["price_status"], "current")
        self.assertEqual(current["images"], [])
        self.assertFalse(current["order_enabled"])
        stale = catalog.get(sid, now=NOW + timedelta(minutes=15))
        self.assertEqual(stale["price_status"], "update_required")
        self.assertIsNone(stale["price"])
        encoded = json.dumps(current)
        for key in ("commercial_as_supplied", "price_1", "price_4", "provider_id", "source_identity", "image_urls"):
            self.assertNotIn(key, encoded)
        self.assertIn("commercial_as_supplied", evidence)

    def test_media_references_remain_private_and_reject_nonstatic_paths(self):
        product, evidence = self.normalize()
        self.assertEqual(len(evidence["image_urls"]), 2)
        self.assertEqual(product["photo"], "")
        for url in ("http://media1.tyre-shopping.com/images/a.jpg", "https://evil.example/images/a.jpg",
                    "https://media1.tyre-shopping.com/images/a.jpg?token=secret",
                    "https://u:p@media1.tyre-shopping.com/images/a.jpg",
                    "https://media1.tyre-shopping.com/images/%2e%2e/a.jpg"):
            with self.subTest(url=url):
                _, private = self.normalize(row(image=url, image_tn=""))
                self.assertEqual(private["image_urls"], [])

    def test_stream_reader_validates_hash_schema_unique_ids_and_records(self):
        with tempfile.TemporaryDirectory() as folder:
            path, meta = self.archive(folder, [row(), row(id="123457", type="M")])
            report = feed.FeedReport()
            records = list(feed.iter_records(path, metadata=meta, now=NOW, report=report))
            self.assertEqual(len(records), 1)
            self.assertEqual(report.rows, 2)
            self.assertEqual(report.source_id_count, 2)
            self.assertEqual(report.skipped_non_passenger, 1)
            self.assertTrue(report.completed)
            self.assertRegex(report.csv_sha256, "^[a-f0-9]{64}$")
            self.assertNotIn("price_1", json.dumps(report.summary()))
            with self.assertRaisesRegex(feed.FeedError, "archive_hash_mismatch"):
                list(feed.iter_records(path, metadata=replace(meta, archive_sha256="b" * 64), now=NOW))
            for rows, kwargs, error in (
                ([row(), row()], {}, "duplicate_source_id"),
                ([row()], {"headers": tuple(reversed(feed.HEADERS))}, "unexpected_csv_schema"),
                ([row()], {"extra": "../secret.csv"}, "unexpected_archive_members"),
                ([row(id="unsafe")], {}, "invalid_source_id"),
            ):
                with self.subTest(error=error):
                    path, meta = self.archive(folder, rows, **kwargs)
                    with self.assertRaisesRegex(feed.FeedError, error):
                        list(feed.iter_records(path, metadata=meta, now=NOW))

    def test_limits_and_incomplete_stream_report_fail_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            path, meta = self.archive(folder, [row(), row(id="123457")])
            report = feed.FeedReport()
            records = feed.iter_records(path, metadata=meta, now=NOW, report=report)
            next(records)
            records.close()
            self.assertFalse(report.completed)
            with patch.object(feed, "MAX_ROWS", 1), self.assertRaisesRegex(feed.FeedError, "row_limit_exceeded"):
                list(feed.iter_records(path, metadata=meta, now=NOW))
            with patch.object(feed, "MAX_CSV_BYTES", 1), self.assertRaisesRegex(feed.FeedError, "unsupported_archive_member"):
                list(feed.iter_records(path, metadata=meta, now=NOW))
            with patch.object(feed, "MAX_ARCHIVE_BYTES", 1), self.assertRaisesRegex(feed.FeedError, "archive_too_large"):
                list(feed.iter_records(path, metadata=meta, now=NOW))
        incomplete = row()
        del incomplete["width"]
        with self.assertRaisesRegex(feed.FeedError, "invalid_csv_record"):
            self.normalize(incomplete)

    def test_unclosed_quote_must_not_swallow_the_next_record(self):
        with tempfile.TemporaryDirectory() as folder:
            text = "|".join(feed.HEADERS) + "\n"
            value = row()
            # All preceding fields are ordinary cells; the last field starts
            # a malformed quote that must not consume a second record.
            text += "|".join(value[key] for key in feed.HEADERS[:-1]) + '|"unclosed\n'
            text += "|".join(row(id="123457")[key] for key in feed.HEADERS) + "\n"
            path = Path(folder) / "malformed.zip"
            with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
                archive.writestr("tmp/123.csv", text.encode())
            meta = metadata(archive_sha256=sha256(path.read_bytes()).hexdigest())
            report = feed.FeedReport()
            with self.assertRaisesRegex(feed.FeedError, "archive_read_failed"):
                list(feed.iter_records(path, metadata=meta, now=NOW, report=report))
            self.assertFalse(report.completed)


if __name__ == "__main__":
    unittest.main()
