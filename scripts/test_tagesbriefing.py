"""Run with python scripts/test_tagesbriefing.py; no app, DB or network imports."""

from copy import deepcopy
from datetime import date, datetime, timezone
import json
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from werkstatt_tagesbriefing import build_briefing


TODAY = date(2026, 9, 28)


def order(**values):
    row = {"id": 156, "fahrzeug": "Audi A4", "kennzeichen": "TEST-1", "autohaus": "Partner",
           "status": 3, "archiviert": 0, "transport_art": "standard"}
    row.update(values)
    return row


class DailyBriefingTests(unittest.TestCase):
    def test_german_and_iso_dates_classify_today_and_overdue(self):
        report = build_briefing([order(fertig_datum="28.09.2026"),
                                 order(id=157, fertig_datum="2026-09-27"),
                                 order(id=158, fertig_datum="2026-09-29")], TODAY)
        self.assertEqual([event["auftrag_id"] for event in report["ereignisse"]], [157, 156])
        self.assertEqual(len(report["kategorien"]["heute_faellig"]), 1)
        self.assertEqual(len(report["kategorien"]["ueberfaellig"]), 1)
        self.assertEqual(report["anzahl_auftraege"], 2)

    def test_finish_time_only_comes_from_dedicated_field(self):
        report = build_briefing([order(fertig_datum="28.09.2026", abhol_uhrzeit="16:00")], TODAY)
        event = report["ereignisse"][0]
        self.assertIsNone(event["uhrzeit"])
        self.assertEqual(event["uhrzeit_feld"], "fertig_uhrzeit")
        self.assertEqual(event["uhrzeit_status"], "unbekannt")
        self.assertIn("Uhrzeit unbekannt", report["speech_text"])
        self.assertNotIn("16:00", report["speech_text"])
        report = build_briefing([order(fertig_datum="28.09.2026", fertig_uhrzeit="9:30", abhol_uhrzeit="16:00")], TODAY)
        self.assertEqual(report["ereignisse"][0]["uhrzeit"], "09:30")

    def test_transport_has_correct_direction_and_own_time(self):
        report = build_briefing([
            order(annahme_datum="28.09.2026", annahme_uhrzeit="08:00", abholtermin="28.09.2026", abhol_uhrzeit="17:00"),
            order(id=157, transport_art="hol_und_bring", annahme_datum="2026-09-28", annahme_uhrzeit="10:00",
                  abholtermin="2026-09-28", abhol_uhrzeit="18:00")], TODAY)
        expected = {"anlieferung_heute": (156, "08:00"), "kundenabholung_heute": (156, "17:00"),
                    "abholung_durch_werkstatt_heute": (157, "10:00"), "rueckbringung_heute": (157, "18:00")}
        for kind, values in expected.items():
            event = report["kategorien"][kind][0]
            self.assertEqual((event["auftrag_id"], event["uhrzeit"]), values)

    def test_finished_keeps_return_but_has_no_unfinished_deadline(self):
        report = build_briefing([order(status=4, fertig_datum="27.09.2026", annahme_datum="28.09.2026", abholtermin="28.09.2026")], TODAY)
        self.assertEqual([event["art"] for event in report["ereignisse"]], ["kundenabholung_heute"])
        today_done = build_briefing([order(status="4", fertig_datum="28.09.2026")], TODAY)
        self.assertFalse(today_done["ereignisse"])

    def test_returned_and_archived_are_excluded(self):
        for changes in ({"status": 5}, {"archiviert": 1}, {"archiviert": "1"}):
            with self.subTest(changes=changes):
                report = build_briefing([order(fertig_datum="27.09.2026", abholtermin="28.09.2026", **changes)], TODAY)
                self.assertFalse(report["ereignisse"])

    def test_unknown_transport_is_not_assumed_to_be_customer_transport(self):
        report = build_briefing([order(transport_art=None, annahme_datum="28.09.2026")], TODAY)
        self.assertEqual(len(report["kategorien"]["transport_ungeklaert"]), 1)
        self.assertFalse(report["kategorien"]["anlieferung_heute"])
        self.assertIn("Transportart ungeklärt", report["speech_text"])

    def test_internal_id_is_source_not_external_order_number(self):
        report = build_briefing([order(id="156", auftragsnummer="EXTERNAL-999", fertig_datum="28.09.2026",
                                      quelle="https://unrelated.invalid/")], TODAY)
        event = report["ereignisse"][0]
        self.assertEqual(event["auftrag_id"], 156)
        self.assertEqual(event["quelle"], "/admin/auftrag/156")
        self.assertNotIn("EXTERNAL", report["speech_text"])

    def test_invalid_dates_times_status_and_duplicates_are_reported(self):
        report = build_briefing([order(id=1, fertig_datum="31.09.2026"),
                                 order(id=2, fertig_datum="28.09.2026", fertig_uhrzeit="25:90"),
                                 order(id=3, status="unknown", fertig_datum="28.09.2026"),
                                 order(id=4, fertig_datum="28.09.2026"),
                                 order(id="4", fertig_datum="27.09.2026"),
                                 order(id=None, fertig_datum="28.09.2026")], TODAY)
        self.assertEqual([event["auftrag_id"] for event in report["ereignisse"]], [2])
        self.assertIsNone(report["ereignisse"][0]["uhrzeit"])
        self.assertEqual(len(report["datenhinweise"]), 6)

    def test_speech_is_limited_but_structure_retains_every_event(self):
        report = build_briefing([order(id=i, fertig_datum="28.09.2026") for i in range(1, 9)], TODAY)
        self.assertEqual(len(report["ereignisse"]), 8)
        self.assertEqual(report["speech_text"].count("Auftrag "), 5)
        self.assertIn("3 weitere Termine", report["speech_text"])

    def test_today_aware_datetime_uses_berlin_calendar(self):
        report = build_briefing([order(fertig_datum="28.09.2026")], datetime(2026, 9, 27, 23, 30, tzinfo=timezone.utc))
        self.assertEqual(report["datum"], "2026-09-28")
        self.assertEqual(report["anzahl_ereignisse"], 1)
        with self.assertRaises(ValueError):
            build_briefing([], datetime(2026, 9, 28))

    def test_default_today_uses_berlin_timezone(self):
        with patch("werkstatt_tagesbriefing.datetime", wraps=datetime) as clock:
            clock.now.return_value = datetime(2026, 9, 28, 1, tzinfo=timezone.utc)
            # Empty data avoids isinstance calls against the patched class.
            report = build_briefing([])
            self.assertEqual(report["datum"], "2026-09-28")
            self.assertEqual(str(clock.now.call_args.args[0]), "Europe/Berlin")

    def test_descriptions_releases_and_quantities_are_not_interpreted(self):
        rows = [order(fertig_datum="28.09.2026", beschreibung="Ungeprüfter Text 50 Stück",
                      analyse_text="Fremder Inhalt", versicherung_freigabe_status="offen")]
        before = deepcopy(rows)
        report = build_briefing(rows, TODAY)
        self.assertEqual(rows, before)
        self.assertNotIn("Ungeprüfter", report["speech_text"])
        self.assertNotIn("50 Stück", json.dumps(report, ensure_ascii=False))
        self.assertNotIn("freigegeben", report["speech_text"])

    def test_stable_sort_and_same_order_multiple_events(self):
        rows = [order(id=2, fertig_datum="28.09.2026", fertig_uhrzeit="09:00"),
                order(id=1, fertig_datum="28.09.2026", fertig_uhrzeit="09:00", abholtermin="28.09.2026")]
        report = build_briefing(rows, "2026-09-28")
        self.assertEqual(report, build_briefing(list(reversed(rows)), "28.09.2026"))
        self.assertEqual(report["anzahl_ereignisse"], 3)
        self.assertEqual(report["anzahl_auftraege"], 2)


if __name__ == "__main__":
    unittest.main()
