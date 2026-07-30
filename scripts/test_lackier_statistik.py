from datetime import date
from pathlib import Path
import json
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def main():
    auftraege = [
        {"id": 1, "status": 4, "fertig_datum": "30.07.2026", "angebotsphase": False},
        {"id": 2, "status": 5, "fertig_datum": "12.06.2026", "angebotsphase": False},
        {"id": 3, "status": 2, "fertig_datum": "08.05.2026", "angebotsphase": False},
        {"id": 4, "status": 4, "fertig_datum": "18.07.2026", "angebotsphase": True},
        {"id": 5, "status": 2, "fertig_datum": "14.07.2026", "angebotsphase": False},
        {"id": 6, "status": 5, "fertig_datum": "01.04.2026", "angebotsphase": False},
        {"id": 7, "status": 4, "fertig_datum": "", "angebotsphase": False},
        {
            "id": 8,
            "status": 5,
            "fertig_datum": "19.01.2026",
            "angebotsphase": False,
            "fahrzeug": "VW Golf",
            "kennzeichen": "MOS-L 26",
            "autohaus_name": "Test Autohaus",
        },
    ]
    fertigmeldungen = {
        1: date(2026, 7, 30),
        2: date(2026, 6, 12),
        3: date(2026, 5, 8),
        4: date(2026, 7, 18),
        6: date(2026, 4, 1),
        7: date(2026, 6, 29),
        8: date(2026, 1, 19),
    }

    statistik = portal.build_lackier_statistik(
        auftraege,
        reference_date=date(2026, 7, 30),
        fertigmeldungen=fertigmeldungen,
    )
    counts = [monat["count"] for monat in statistik["monate"]]
    assert counts == [1, 2, 1], counts
    assert statistik["gesamt"] == 4, statistik
    assert [monat["label"] for monat in statistik["monate"]] == [
        "Juli 2026",
        "Juni 2026",
        "Mai 2026",
    ]
    assert statistik["alle_monate"] == statistik["monate"]
    assert "historie" not in statistik
    assert all(
        item["id"] != 8
        for monat in statistik["monate"]
        for item in monat["items"]
    )
    json.dumps(statistik, ensure_ascii=False)
    assert any(rule.endpoint == "admin_lackier_statistik" for rule in portal.app.url_map.iter_rules())

    jahreswechsel = portal.build_lackier_statistik(
        [],
        reference_date=date(2026, 1, 2),
        fertigmeldungen={},
    )
    assert [monat["label"] for monat in jahreswechsel["monate"]] == [
        "Januar 2026",
        "Dezember 2025",
        "November 2025",
    ]
    print("[OK] Lackierstatistik zeigt ausschließlich die letzten drei Monate.")


if __name__ == "__main__":
    main()
