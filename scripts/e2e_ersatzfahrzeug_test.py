# -*- coding: utf-8 -*-
"""End-to-End-Test Ersatzfahrzeug-Selbstauswahl: Verfügbarkeit über Zeitraum,
Klassen+Preise, Buchung blockt das Fahrzeug, Überschneidungs-Schutz."""
import sys
import tempfile
from pathlib import Path
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import app as portal  # noqa: E402

_tmp = tempfile.mkdtemp(prefix="ersatz_test_")
portal.DB = Path(_tmp) / "test.db"
portal.UPLOAD_DIR = Path(_tmp) / "uploads"

RESULTS = []


def report(label, ok, detail=""):
    RESULTS.append((label, ok))
    print(f"[{'OK  ' if ok else 'FEHLER'}] {label}" + (f" - {detail}" if detail else ""))
    return ok


def with_csrf(client, data=None):
    payload = dict(data or {})
    with client.session_transaction() as s:
        payload["csrf_token"] = s.get("csrf_token")
    return payload


def iso(d):
    return d.isoformat()


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    client = portal.app.test_client()
    with client.session_transaction() as s:
        s["admin"] = True
    client.get("/admin/mietfahrzeuge")

    heute = date.today()
    a_start, a_end = heute, heute + timedelta(days=5)
    overlap_s, overlap_e = heute + timedelta(days=2), heute + timedelta(days=4)
    frei_s, frei_e = heute + timedelta(days=20), heute + timedelta(days=25)

    klein = portal.create_mietfahrzeug(kennzeichen="MOS-K 1", bezeichnung="Hyundai i10", fahrzeugklasse="Kleinwagen", tagessatz="35")
    kompakt = portal.create_mietfahrzeug(kennzeichen="MOS-K 2", bezeichnung="VW Golf", fahrzeugklasse="Kompaktklasse", tagessatz="49")
    kombi = portal.create_mietfahrzeug(kennzeichen="MOS-K 3", bezeichnung="Skoda Octavia", fahrzeugklasse="Kombi", tagessatz="59")

    # --- Engine: anfangs alle frei ---
    report("Engine: Fahrzeug frei (noch keine Buchung)", portal.mietfahrzeug_zeitraum_frei(klein, iso(a_start), iso(a_end)))
    gruppen = portal.verfuegbare_mietfahrzeuge_nach_klasse(iso(a_start), iso(a_end))
    report("3 Klassen verfügbar, nach Preis sortiert (Kleinwagen zuerst)",
           len(gruppen) == 3 and gruppen[0]["klasse"] == "Kleinwagen" and gruppen[-1]["klasse"] == "Kombi",
           f"{[g['klasse'] for g in gruppen]}")
    report("Ab-Preis je Klasse korrekt", gruppen[0]["ab_preis"] == 35.0 and gruppen[1]["ab_preis"] == 49.0)

    # --- Auswahl-Seite zeigt freie Fahrzeuge ---
    r = client.get(f"/admin/ersatzfahrzeug?start={iso(a_start)}&end={iso(a_end)}")
    seite = r.get_data(as_text=True)
    report("Auswahl-Seite zeigt alle 3 Fahrzeuge", r.status_code == 200
           and "MOS-K 1" in seite and "MOS-K 2" in seite and "MOS-K 3" in seite)
    report("Preise auf der Seite sichtbar", "35,00 €" in seite and "49,00 €" in seite)

    # --- Buchen (blockt) -> Redirect zum Vertrag ---
    r = client.post("/admin/ersatzfahrzeug/buchen", data=with_csrf(client, {
        "fahrzeug_id": str(klein), "start_datum": iso(a_start), "end_datum": iso(a_end),
        "kunde_name": "Max Mustermann", "kunde_telefon": "0176 1234567",
    }), follow_redirects=False)
    report("Buchung leitet zum Vertrag weiter", r.status_code == 302 and "/vertrag" in (r.headers.get("Location") or ""),
           r.headers.get("Location"))

    # --- Engine: jetzt im überlappenden Zeitraum belegt, im freien frei ---
    report("Gebuchtes Fahrzeug im Überschneidungs-Zeitraum NICHT frei",
           not portal.mietfahrzeug_zeitraum_frei(klein, iso(overlap_s), iso(overlap_e)))
    report("Gebuchtes Fahrzeug im späteren Zeitraum wieder frei",
           portal.mietfahrzeug_zeitraum_frei(klein, iso(frei_s), iso(frei_e)))

    # --- Auswahl-Seite: im Überschneidungs-Zeitraum fehlt das gebuchte Auto ---
    seite2 = client.get(f"/admin/ersatzfahrzeug?start={iso(overlap_s)}&end={iso(overlap_e)}").get_data(as_text=True)
    report("Belegtes Fahrzeug nicht mehr buchbar in der Auswahl",
           f'name="fahrzeug_id" value="{klein}"' not in seite2 and f'name="fahrzeug_id" value="{kompakt}"' in seite2)
    gruppen2 = portal.verfuegbare_mietfahrzeuge_nach_klasse(iso(overlap_s), iso(overlap_e))
    report("Kleinwagen-Klasse jetzt nicht mehr frei", all(g["klasse"] != "Kleinwagen" for g in gruppen2))

    # --- Doppelbuchung im selben Zeitraum wird verhindert ---
    n_vorher = len([v for v in portal.list_mietvorgaenge(klein) if not v["abgeschlossen"]])
    r = client.post("/admin/ersatzfahrzeug/buchen", data=with_csrf(client, {
        "fahrzeug_id": str(klein), "start_datum": iso(overlap_s), "end_datum": iso(overlap_e),
        "kunde_name": "Zweiter Kunde",
    }), follow_redirects=True)
    n_nachher = len([v for v in portal.list_mietvorgaenge(klein) if not v["abgeschlossen"]])
    report("Doppelbuchung im Überschneidungs-Zeitraum abgelehnt", n_nachher == n_vorher, f"{n_vorher}->{n_nachher}")

    # --- Buchen im freien späteren Zeitraum klappt (Reservierung) ---
    r = client.post("/admin/ersatzfahrzeug/buchen", data=with_csrf(client, {
        "fahrzeug_id": str(klein), "start_datum": iso(frei_s), "end_datum": iso(frei_e),
        "kunde_name": "Dritter Kunde",
    }), follow_redirects=False)
    report("Buchung im freien Zeitraum erlaubt (zweite, nicht überlappende)", r.status_code == 302 and "/vertrag" in (r.headers.get("Location") or ""))

    # --- Vermieten-Route nutzt denselben Überschneidungs-Schutz ---
    r = client.post(f"/admin/mietfahrzeuge/{klein}/vermieten", data=with_csrf(client, {
        "kunde_name": "Overlap", "start_datum": iso(overlap_s), "end_datum": iso(overlap_e),
    }), follow_redirects=True)
    report("Vermieten-Route lehnt Überschneidung ebenfalls ab",
           "bereits belegt" in r.get_data(as_text=True))

    print()
    fails = [x for x in RESULTS if not x[1]]
    print(f"== ERGEBNIS: {len(RESULTS) - len(fails)}/{len(RESULTS)} Checks bestanden ==")
    for label, ok in fails:
        print(f"  - {label}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
