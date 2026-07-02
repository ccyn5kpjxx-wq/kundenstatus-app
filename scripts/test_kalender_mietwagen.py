"""Feature-Test: Mietwagen-Vermietungen im Cockpit-Monatsblick (Markierung + Legende)."""
from datetime import date, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402

TEST_KENNZEICHEN = "TEST-KAL-MIETE"


def tage_im_monat(kalender):
    """Alle Tag-Dicts des Monats als {date: day} einsammeln."""
    return {day["datum"]: day for week in kalender["weeks"] for day in week}


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    ok = True

    def check(label, cond):
        nonlocal ok
        print(("[OK] " if cond else "[FEHLER] ") + label)
        ok &= bool(cond)

    heute = date.today()
    # Zielmonat = nächster Monat -> Vorgang liegt in der Zukunft = Phase "reserviert"
    monat_start = portal.shift_month(date(heute.year, heute.month, 1), 1)
    von = monat_start + timedelta(days=9)   # 10. des Monats
    bis = monat_start + timedelta(days=11)  # 12. des Monats
    monat_param = monat_start.strftime("%Y-%m")

    with portal.app.test_request_context("/"):
        db = portal.get_db()
        cursor = db.execute(
            "INSERT INTO mietfahrzeuge (kennzeichen, bezeichnung, erstellt_am, geaendert_am)"
            " VALUES (?, 'Testwagen', ?, ?)",
            (TEST_KENNZEICHEN, portal.now_str(), portal.now_str()),
        )
        fahrzeug_id = cursor.lastrowid
        db.execute(
            "INSERT INTO mietvorgaenge (mietfahrzeug_id, kunde_name, kunde_telefon, start_datum, end_datum, status, erstellt_am, geaendert_am)"
            " VALUES (?, 'Test Kalenderkunde', '0000', ?, ?, 'aktiv', ?, ?)",
            (fahrzeug_id, von.strftime(portal.DATE_FMT), bis.strftime(portal.DATE_FMT), portal.now_str(), portal.now_str()),
        )
        db.commit()
        db.close()

    try:
        # 1) Intern (Admin): Tage markiert, Eintrag im Tages-Popover, Legende an
        with portal.app.test_request_context("/"):
            kalender = portal.build_mini_monatskalender(
                [], monat_param, endpoint="betriebs_cockpit", include_internal_notes=True
            )
        tage = tage_im_monat(kalender)
        check("Mietbeginn (10.) markiert", tage[von]["has_miete"] is True)
        check("Miet-Mitteltag (11.) markiert", tage[von + timedelta(days=1)]["has_miete"] is True)
        check("Miet-Ende (12.) markiert", tage[bis]["has_miete"] is True)
        check("Tag danach (13.) nicht markiert", tage[bis + timedelta(days=1)]["has_miete"] is False)
        eintrag = next((i for i in tage[von]["items"] if i["label"] == "Mietwagen"), None)
        check("Tages-Popover hat Mietwagen-Eintrag", eintrag is not None)
        check("Eintrag nennt Kennzeichen + reserviert",
              eintrag and TEST_KENNZEICHEN in eintrag["title"] and "reserviert" in eintrag["title"])
        check("Eintrag nennt Kunde + Zeitraum",
              eintrag and "Test Kalenderkunde" in eintrag["subtitle"] and von.strftime(portal.DATE_FMT) in eintrag["subtitle"])
        check("Eintrag verlinkt auf Mietfahrzeuge", eintrag and "/admin/mietfahrzeuge" in (eintrag["url"] or ""))
        check("Tooltip nennt Mietwagen", "Mietwagen" in tage[von]["tooltip"])
        check("show_miete an (intern)", kalender.get("show_miete") is True)
        check("miete_count = 3 Tage", kalender.get("miete_count") == 3)

        # 2) Partner-Kontext: Vermietungen bleiben unsichtbar (Kundennamen intern!)
        with portal.app.test_request_context("/"):
            partner_kal = portal.build_mini_monatskalender(
                [], monat_param, include_internal_notes=False
            )
        partner_tage = tage_im_monat(partner_kal)
        check("Partner sieht keine Miet-Markierung", partner_tage[von]["has_miete"] is False)
        check("Partner: show_miete aus", partner_kal.get("show_miete") is False)

        # 3) Cockpit-Seite rendert Marker + Legende
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        response = client.get(f"/admin/cockpit?monat={monat_param}")
        check("Cockpit lädt (200)", response.status_code == 200)
        html = response.get_data(as_text=True)
        check("Legende 'Mietwagen vermietet/reserviert'", "Mietwagen vermietet/reserviert" in html)
        check("Miet-Marker im Kalender gerendert", 'class="mini-miete-marker"' in html)

        # 4) Offene Miete ohne End-Datum markiert bis heute
        with portal.app.test_request_context("/"):
            db = portal.get_db()
            db.execute(
                "INSERT INTO mietvorgaenge (mietfahrzeug_id, kunde_name, start_datum, end_datum, status, erstellt_am, geaendert_am)"
                " VALUES (?, 'Test Offen', ?, '', 'aktiv', ?, ?)",
                (fahrzeug_id, (heute - timedelta(days=1)).strftime(portal.DATE_FMT), portal.now_str(), portal.now_str()),
            )
            db.commit()
            db.close()
        with portal.app.test_request_context("/"):
            kalender_jetzt = portal.build_mini_monatskalender(
                [], heute.strftime("%Y-%m"), endpoint="betriebs_cockpit", include_internal_notes=True
            )
        tage_jetzt = tage_im_monat(kalender_jetzt)
        offen_heute = any(i["label"] == "Mietwagen" and "Test Offen" in i["subtitle"] for i in tage_jetzt[heute]["items"])
        check("Offene Miete (ohne Ende) markiert heute", tage_jetzt[heute]["has_miete"] is True and offen_heute)
    finally:
        # Aufräumen (nur eigene Testdaten)
        with portal.app.test_request_context("/"):
            db = portal.get_db()
            db.execute("DELETE FROM mietvorgaenge WHERE mietfahrzeug_id = ?", (fahrzeug_id,))
            db.execute("DELETE FROM mietfahrzeuge WHERE id = ?", (fahrzeug_id,))
            db.commit()
            db.close()

    print("\nErgebnis:", "ALLE CHECKS GRÜN" if ok else "MINDESTENS EIN CHECK ROT")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
