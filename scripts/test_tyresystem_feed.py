"""Offline feed protocol, error isolation and query regression tests."""
from datetime import datetime, timezone
from decimal import Decimal
import csv
import io
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import tyresystem_feed as feed


STAMP = datetime(2026, 11, 2, 13, 0, tzinfo=timezone.utc)


def row(**changes):
    result = dict.fromkeys(feed.HEADERS, "")
    result.update(Artikelnummer="123", ean="0012345678905", Lagerbestand="20",
                  Einkaufspreis="68,3400", Hersteller="Example", Profil="Winter XL 3PMSF",
                  Herstellerartikelnummer="00123", Reifenbreite="225", Reifenquerschnitt="40",
                  Reifendurchmesser="18", Loadindex="92", Speedindex="V", Saison="w",
                  Fahrzeugtyp="PKW", Artikeltyp="Reifen", Artikeluntertyp="PKW", Bauart="R")
    result.update(changes)
    return result


def data(*rows, headers=feed.HEADERS, bom=False):
    output = io.StringIO(newline="")
    writer = csv.writer(output, delimiter=";", lineterminator="\r\n")
    writer.writerow(headers)
    for record in rows:
        writer.writerow([record.get(h, "") for h in headers] if isinstance(record, dict) else record)
    return (("\ufeff" if bom else "") + output.getvalue()).encode("utf-8")


def query(snapshot, **changes):
    args = dict(breite=225, querschnitt=40, zoll="18", lastindex=92,
                speedindex="V", saison="winter", menge=4)
    args.update(changes)
    return snapshot.query(**args)


class FeedTests(unittest.TestCase):
    def setUp(self):
        self.sockets = patch.object(socket, "socket", side_effect=AssertionError("Network forbidden"))
        self.sockets.start()
        self.addCleanup(self.sockets.stop)

    def parse(self, *rows, **options):
        return feed.parse_feed(data(*rows, **options), abgerufen_am=STAMP)

    def test_decimal_and_identifier_preservation(self):
        article = self.parse(row()).artikel[0]
        self.assertEqual(article.einkaufspreis, Decimal("68.3400"))
        self.assertEqual(article.ean, "0012345678905")
        self.assertEqual(article.hersteller_artikelnummer, "00123")
        self.assertEqual(article.internal_dict()["einkaufspreis"], "68.3400")

    def test_bom_quoted_profile_and_non_ascii(self):
        article = self.parse(row(Profil='Winter; "Spezial" ä'), bom=True).artikel[0]
        self.assertEqual(article.profil, 'Winter; "Spezial" ä')

    def test_no_implicit_commerce_metadata(self):
        snapshot = self.parse(row())
        article = snapshot.artikel[0]
        self.assertFalse(snapshot.public_ready)
        self.assertFalse(article.public_ready)
        for field in ("fracht_netto", "waehrung_bestaetigt", "steuerbasis_bestaetigt",
                      "lieferzusage", "xl_bestaetigt", "runflat_bestaetigt", "schneeflocke_bestaetigt"):
            self.assertIsNone(getattr(article, field))
        self.assertIsNone(snapshot.erzeugt_am_lieferant)

    def test_aware_receipt_time_required(self):
        for stamp in (None, "2026-11-02", datetime(2026, 11, 2)):
            with self.subTest(stamp=stamp), self.assertRaises(feed.FeedError):
                feed.parse_feed(data(row()), abgerufen_am=stamp)

    def test_receipt_time_is_normalized_and_snapshot_frozen(self):
        snap = self.parse(row())
        self.assertEqual(snap.abgerufen_am, STAMP)
        self.assertEqual(len(snap.sha256), 64)
        with self.assertRaises(TypeError):
            snap.statistik["rows_read"] = 0
        with self.assertRaises(AttributeError):
            snap.public_ready = True

    def test_exact_header_required(self):
        for headers in (feed.HEADERS[:-1], tuple(reversed(feed.HEADERS)),
                        ("unknown",) + feed.HEADERS[1:], (feed.HEADERS[1],) + feed.HEADERS[1:]):
            with self.subTest(headers=headers), self.assertRaises(feed.FeedError):
                self.parse(row(), headers=headers)

    def test_empty_and_invalid_encoding_fail_whole_snapshot(self):
        for content in (b"", b"\xff", data(row()) + b"\xff"):
            with self.subTest(content=content[:10]), self.assertRaises(feed.FeedError):
                feed.parse_feed(content, abgerufen_am=STAMP)

    def test_reject_non_bytes_and_file_url_without_exposing_it(self):
        secret = "https://vendor.invalid/private?login=SECRET"
        for call in (lambda: feed.parse_feed(secret, abgerufen_am=STAMP),
                     lambda: feed.read_feed(secret, abgerufen_am=STAMP)):
            with self.assertRaises(feed.FeedError) as result:
                call()
            self.assertNotIn("SECRET", str(result.exception))

    def test_byte_and_row_limits(self):
        with patch.object(feed, "MAX_BYTES", 20), self.assertRaises(feed.FeedError):
            self.parse(row())
        with patch.object(feed, "MAX_ROWS", 1), self.assertRaises(feed.FeedError):
            self.parse(row(), row(Artikelnummer="124"))

    def test_structural_csv_corruption_fails_whole_snapshot(self):
        corrupt = data(row()) + b'"unterminated\r\n'
        with self.assertRaises(feed.FeedError):
            feed.parse_feed(corrupt, abgerufen_am=STAMP)

    def test_bad_column_count_isolated(self):
        snapshot = self.parse(["short"], row(), ["extra"] * 29)
        self.assertEqual(len(snapshot.artikel), 1)
        self.assertEqual(snapshot.statistik["invalid_rows"], 2)
        self.assertEqual(snapshot.fehlerkategorien["spaltenzahl"], 2)

    def test_empty_lines_do_not_create_malformed_products(self):
        snapshot = feed.parse_feed(data(row()) + b"\r\n\r\n", abgerufen_am=STAMP)
        self.assertEqual(snapshot.statistik["rows_read"], 1)
        self.assertEqual(snapshot.statistik["invalid_rows"], 0)

    def test_non_pkw_and_other_product_types_are_filtered(self):
        snapshot = self.parse(row(), row(Artikelnummer="124", Artikeltyp="Felge"),
                              row(Artikelnummer="125", Fahrzeugtyp="Motorrad"),
                              row(Artikelnummer="126", Artikeluntertyp="Schlauch"))
        self.assertEqual(snapshot.statistik["ignored_non_pkw"], 3)
        self.assertEqual(len(snapshot.artikel), 1)

    def test_only_explicit_radial_construction_is_eligible(self):
        snapshot = self.parse(row(), *(row(Artikelnummer=str(200+i), Bauart=kind)
                                       for i, kind in enumerate(("D", "B", "Z", "ZR", ""))))
        self.assertEqual(len(snapshot.artikel), 1)
        self.assertEqual(snapshot.fehlerkategorien["bauart"], 5)
        self.assertEqual(snapshot.artikel[0].bauart, "R")

    def test_invalid_numbers_do_not_poison_other_rows(self):
        invalid = [row(Artikelnummer=str(200+i), **changes) for i, changes in enumerate([
            {"Einkaufspreis":"0,00"}, {"Einkaufspreis":"-3,50"},
            {"Einkaufspreis":"NaN"}, {"Einkaufspreis":"1.234,56"},
            {"Einkaufspreis":"1e3"}, {"Lagerbestand":"-1"},
            {"Lagerbestand":">20"}, {"Loadindex":"92/90"},
            {"Speedindex":"Z"}, {"Reifenbreite":"6.5"},
            {"Reifendurchmesser":"nan"}, {"Saison":"?"},
        ])]
        snapshot = self.parse(row(), *invalid)
        self.assertEqual(len(snapshot.artikel), 1)
        self.assertEqual(snapshot.statistik["invalid_rows"], len(invalid))
        self.assertLessEqual(len(snapshot.fehlerkategorien), 8)

    def test_invalid_id_ean_and_text_are_safe(self):
        snapshot = self.parse(row(), row(Artikelnummer="00123"),
                              row(Artikelnummer="124", ean="not-an-ean"),
                              row(Artikelnummer="125", Profil="secret\x00value"),
                              row(Artikelnummer="126", Profil="x" * (feed.MAX_FIELD_CHARS + 1)))
        self.assertEqual(len(snapshot.artikel), 1)
        self.assertEqual(snapshot.statistik["invalid_rows"], 4)
        self.assertNotIn("secret", str(snapshot.fehlerkategorien))

    def test_missing_optional_ean_is_unknown(self):
        article = self.parse(row(ean="")).artikel[0]
        self.assertIsNone(article.ean)

    def test_identical_duplicate_never_overwrites(self):
        snapshot = self.parse(row(), row())
        self.assertEqual(len(snapshot.artikel), 1)
        self.assertEqual(snapshot.statistik["duplicate_rows"], 1)
        self.assertEqual(snapshot.statistik["conflicting_ids"], 0)

    def test_conflicting_duplicate_quarantines_id_regardless_of_order(self):
        for records in ((row(), row(Einkaufspreis="1,00")), (row(Einkaufspreis="1,00"), row())):
            snapshot = self.parse(*records, row(Artikelnummer="124"))
            self.assertEqual([a.artikel_id for a in snapshot.artikel], ["124"])
            self.assertEqual(snapshot.statistik["conflicting_ids"], 1)

    def test_duplicate_product_type_or_invalid_row_conflict_quarantines(self):
        for duplicate in (row(Artikeltyp="Felge"), row(Einkaufspreis="NaN"), row(Profil="a\x00b")):
            snapshot = self.parse(row(), duplicate, row())
            self.assertEqual(len(snapshot.artikel), 0)
            self.assertEqual(snapshot.statistik["conflicting_ids"], 1)

    def test_eprel_only_accepted_host_never_followed(self):
        good = "https://eprel.ec.europa.eu/qr/123"
        self.assertEqual(self.parse(row(**{"EPREL-URL":good})).artikel[0].eprel_url, good)
        for url in ("http://eprel.ec.europa.eu/qr/1", "https://eprel.ec.europa.eu.evil/qr/1",
                    "https://secret@eprel.ec.europa.eu/qr/1", "https://eprel.ec.europa.eu:99/qr/1",
                    "https://eprel.ec.europa.eu/qr/1?secret=1", "javascript:alert(1)"):
            with self.subTest(url=url):
                self.assertIsNone(self.parse(row(**{"EPREL-URL":url})).artikel[0].eprel_url)

    def test_load_speed_stock_season_and_dimensions_query(self):
        snapshot = self.parse(row(), row(Artikelnummer="124", Loadindex="94", Speedindex="W"),
                              row(Artikelnummer="125", Speedindex="H"),
                              row(Artikelnummer="126", Lagerbestand="3"),
                              row(Artikelnummer="127", Saison="s"),
                              row(Artikelnummer="128", Reifenquerschnitt="45"))
        self.assertEqual([a.artikel_id for a in query(snapshot)], ["123", "124"])

    def test_special_stock_excluded_by_default(self):
        snapshot = self.parse(row(), row(Artikelnummer="124", Dot="DOT22"),
                              row(Artikelnummer="125", Demo="Demo"), row(Artikelnummer="126", Da="Da"))
        self.assertEqual(len(query(snapshot)), 1)
        self.assertEqual(len(query(snapshot, sonderware=True)), 4)

    def test_cheapest_order_is_decimal_and_ties_are_stable(self):
        snapshot = self.parse(row(Einkaufspreis="100,00"), row(Artikelnummer="124", Einkaufspreis="9,50"),
                              row(Artikelnummer="125", Einkaufspreis="9,50"))
        self.assertEqual([a.artikel_id for a in query(snapshot, limit=2)], ["124", "125"])

    def test_query_rejects_bad_types_and_unsupported_meanings(self):
        snapshot = self.parse(row())
        for changes in ({"menge":0}, {"menge":True}, {"menge":1.0}, {"limit":101},
                        {"zoll":18.0}, {"speedindex":"ZR"}, {"saison":"w"},
                        {"sonderware":"yes"}, {"lastindex":"92/90"}):
            with self.subTest(changes=changes), self.assertRaises(feed.FeedError):
                query(snapshot, **changes)

    def test_local_path_and_bytes_entry_points(self):
        content = data(row())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "example.csv"
            path.write_bytes(content)
            self.assertEqual(feed.load_feed(path, abgerufen_am=STAMP).sha256,
                             feed.load_feed(content, abgerufen_am=STAMP).sha256)
            with self.assertRaises(feed.FeedError) as result:
                feed.read_feed(Path(directory) / "SECRET-missing.csv", abgerufen_am=STAMP)
            self.assertNotIn("SECRET", str(result.exception))

    def test_unc_device_and_uri_paths_rejected_before_io(self):
        for path in (r"\\server\share\feed.csv", r"\\?\C:\feed.csv", r"\\.\pipe\feed",
                     "//server/share/feed.csv", "ftp:private", Path(r"\\server\share\feed.csv")):
            with self.subTest(path=path), patch.object(Path, "open") as open_file:
                with self.assertRaises(feed.FeedError):
                    feed.read_feed(path, abgerufen_am=STAMP)
                open_file.assert_not_called()


if __name__ == "__main__":
    unittest.main()
