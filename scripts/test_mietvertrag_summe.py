"""Feature-Test: Mietvertrag — Gesamtbetrag (Vorkasse), nur Vollkasko-SB, Kostenübersicht."""
from datetime import date, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402

KZ = "TEST-VERTRAG-SUM"


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    ok = True

    def check(label, cond):
        nonlocal ok
        print(("[OK] " if cond else "[FEHLER] ") + label)
        ok &= bool(cond)

    # 1) Helper
    check("parse_euro_de 1.234,56", portal.parse_euro_de("1.234,56") == 1234.56)
    check("parse_euro_de 30", portal.parse_euro_de("30") == 30.0)
    check("parse_euro_de 0,25", portal.parse_euro_de("0,25") == 0.25)
    check("parse_euro_de 500,00 €", portal.parse_euro_de("500,00 €") == 500.0)
    check("parse_euro_de leer/kaputt", portal.parse_euro_de("") == 0.0 and portal.parse_euro_de("quatsch") == 0.0)
    check("euro_de_label 1234.5", portal.euro_de_label(1234.5) == "1.234,50")
    check("miettage 1 Tag (15.->16.)", portal.mietvertrag_miettage({"start_datum": "15.09.2026", "end_datum": "16.09.2026"}) == 1)
    check("miettage gleicher Tag = 1", portal.mietvertrag_miettage({"start_datum": "15.09.2026", "end_datum": "15.09.2026"}) == 1)
    check("miettage 3 Tage (10.->13.)", portal.mietvertrag_miettage({"start_datum": "10.09.2026", "end_datum": "13.09.2026"}) == 3)
    check("miettage ohne Daten = 1", portal.mietvertrag_miettage({}) == 1)

    # 2) Kein Teilkasko-Feld mehr, Platzhalter sauber
    feldnamen = [f["name"] for f in portal.MIETVERTRAG_FELDER]
    check("Feld selbstbeteiligung_teilkasko_euro entfernt", "selbstbeteiligung_teilkasko_euro" not in feldnamen)
    check("Feld selbstbeteiligung_euro bleibt", "selbstbeteiligung_euro" in feldnamen)
    alle_texte = " ".join(a["text"] for a in portal.MIETVERTRAG_ABSCHNITTE)
    check("Kein Teilkasko-SB-Platzhalter mehr im Vertragstext", "{selbstbeteiligung_teilkasko_euro}" not in alle_texte)
    check("Einheitliche Vollkasko-SB auch für Teilkasko-Schäden", "einheitlich" in alle_texte and "gesonderter Teilkaskoschutz" in alle_texte)
    check("Vorkasse im § 4", "Vorkasse" in alle_texte and "vor Fahrzeugübergabe zur Zahlung fällig" in alle_texte)

    # 3) Testdaten: Fahrzeug 30 €/Tag, Miete morgen -> übermorgen (1 Tag)
    morgen = date.today() + timedelta(days=1)
    with portal.app.test_request_context("/"):
        db = portal.get_db()
        cur = db.execute(
            "INSERT INTO mietfahrzeuge (kennzeichen, bezeichnung, tagessatz, erstellt_am, geaendert_am) VALUES (?, 'Hyundai i10 Test', 30, ?, ?)",
            (KZ, portal.now_str(), portal.now_str()),
        )
        fahrzeug_id = cur.lastrowid
        cur = db.execute(
            "INSERT INTO mietvorgaenge (mietfahrzeug_id, kunde_name, start_datum, end_datum, status, erstellt_am, geaendert_am)"
            " VALUES (?, 'Test Summenkunde', ?, ?, 'aktiv', ?, ?)",
            (fahrzeug_id, morgen.strftime(portal.DATE_FMT), (morgen + timedelta(days=1)).strftime(portal.DATE_FMT), portal.now_str(), portal.now_str()),
        )
        vorgang_id = cur.lastrowid
        db.commit()
        db.close()

    try:
        vorgang = portal.get_mietvorgang(vorgang_id)
        fahrzeug = portal.get_mietfahrzeug(fahrzeug_id)

        # 4) Kontext: 1 Tag × 30 € + 500 € Kaution = 530 €
        with portal.app.test_request_context("/"):
            kontext = portal.mietvertrag_kontext(vorgang, fahrzeug)
        check("Kontext: 1 Miettag", kontext["miettage_text"] == "1 Miettag")
        check("Kontext: Mietsumme 30,00", kontext["mietsumme_euro"] == "30,00")
        check("Kontext: Gesamtbetrag 530,00", kontext["gesamtbetrag_euro"] == "530,00")
        with portal.app.test_request_context("/"):
            abschnitte = portal.mietvertrag_abschnitte_gefuellt(kontext)
        p4 = next(a for a in abschnitte if a["nummer"] == "§ 4")
        check("§ 4 nennt Gesamtbetrag 530,00 EUR", "Gesamtbetrag von 530,00 EUR" in p4["text"])
        check("Keine offenen Platzhalter im ganzen Vertrag", not any("{" in a["text"] and "}" in a["text"] for a in abschnitte))

        # 5) Alte gespeicherte Teilkasko-Werte stören nicht (Migration bestehender Verträge)
        with portal.app.test_request_context("/"):
            db = portal.get_db()
            db.execute(
                "UPDATE mietvorgaenge SET vertrag_felder_json = ? WHERE id = ?",
                ('{"selbstbeteiligung_teilkasko_euro": "500,00", "kaution_euro": "500,00", "mietpreis_tag": "30,00"}', vorgang_id),
            )
            db.commit()
            db.close()
        vorgang = portal.get_mietvorgang(vorgang_id)
        with portal.app.test_request_context("/"):
            kontext2 = portal.mietvertrag_kontext(vorgang, fahrzeug)
            abschnitte2 = portal.mietvertrag_abschnitte_gefuellt(kontext2)
        check("Alt-Vertrag: Gesamtbetrag weiter 530,00", kontext2["gesamtbetrag_euro"] == "530,00")
        check("Alt-Vertrag: kein Teilkasko-Platzhalter offen", not any("selbstbeteiligung_teilkasko" in a["text"] for a in abschnitte2))

        # 6) Vertragsseite rendert Kostenübersicht + Live-Rechner
        client = portal.app.test_client()
        with client.session_transaction() as session:
            session["admin"] = True
        response = client.get(f"/admin/mietvorgang/{vorgang_id}/vertrag")
        check("Vertragsseite lädt (200)", response.status_code == 200)
        html = response.get_data(as_text=True)
        check("Kostenübersicht sichtbar", "Vor Fahrzeugübergabe zu zahlen" in html)
        check("Gesamtbetrag 530,00 auf der Seite", 'id="kbGesamt">530,00' in html)
        check("Live-Rechner eingebunden", "neuRechnen" in html)
        check("Kein Teilkasko-Eingabefeld mehr", 'name="selbstbeteiligung_teilkasko_euro"' not in html)

        # 7) PDF baut ohne Fehler und enthält Inhalt
        with portal.app.test_request_context("/"):
            pdf = portal.make_mietvertrag_pdf(vorgang, fahrzeug)
        pdf_bytes = pdf.getvalue() if hasattr(pdf, "getvalue") else (pdf or b"")
        check("PDF wird erzeugt (>10 KB)", len(pdf_bytes) > 10000 and pdf_bytes[:4] == b"%PDF")
    finally:
        with portal.app.test_request_context("/"):
            db = portal.get_db()
            db.execute("DELETE FROM mietvorgaenge WHERE id = ?", (vorgang_id,))
            db.execute("DELETE FROM mietfahrzeuge WHERE id = ?", (fahrzeug_id,))
            db.commit()
            db.close()

    print("\nErgebnis:", "ALLE CHECKS GRÜN" if ok else "MINDESTENS EIN CHECK ROT")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
