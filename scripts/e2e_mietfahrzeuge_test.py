# -*- coding: utf-8 -*-
"""End-to-End-Test fuer die Mietfahrzeug-Verwaltung.

Spielt den kompletten Admin-Ablauf gegen einen Flask-Testclient durch:
Fahrzeug anlegen -> in der Liste sehen -> an Kunden vergeben ->
Status/Belegung pruefen -> Doppelvergabe verhindern -> zuruecknehmen ->
wieder verfuegbar -> bearbeiten -> Nav-Badge -> loeschen.

Laeuft auf einer eigenen Test-DB, keine Live-Daten.
"""
import io
import os
import re
import sys
import tempfile
from pathlib import Path
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402

# Eigene Test-DB + Upload-Ordner im Temp (nicht im OneDrive-Worktree),
# damit nichts Echtes angefasst wird und unlink sofort greift.
_tmpdir = tempfile.mkdtemp(prefix="mietfz_test_")
portal.DB = Path(_tmpdir) / "test.db"
portal.UPLOAD_DIR = Path(_tmpdir) / "uploads"

RESULTS = []


def report(label, ok, detail=""):
    RESULTS.append((label, ok, detail))
    print(f"[{'OK  ' if ok else 'FEHLER'}] {label}" + (f" - {detail}" if detail else ""))
    return ok


def with_csrf(client, data=None):
    payload = dict(data or {})
    with client.session_transaction() as session:
        token = session.get("csrf_token")
    if token:
        payload["csrf_token"] = token
    return payload


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    client = portal.app.test_client()
    with client.session_transaction() as session:
        session["admin"] = True

    heute = date.today()
    in_5_tagen = (heute + timedelta(days=5)).isoformat()

    # 1) Leere Seite laedt
    r = client.get("/admin/mietfahrzeuge")
    report("Mietfahrzeug-Seite laedt (leer)", r.status_code == 200
           and "Mietfahrzeuge" in r.get_data(as_text=True))

    # 2) Fahrzeug anlegen
    r = client.post("/admin/mietfahrzeuge/neu", data=with_csrf(client, {
        "kennzeichen": "mos-gk 100",
        "bezeichnung": "VW Polo 1.0 TSI",
        "fahrzeugklasse": "Kleinwagen",
        "farbe": "Weiß",
        "baujahr": "2023",
        "kraftstoff": "Benzin",
        "tagessatz": "39,00",
        "standort": "Hof",
        "status": "verfuegbar",
        "notiz": "Winterreifen",
    }), follow_redirects=True)
    seite = r.get_data(as_text=True)
    report("Fahrzeug anlegen + sichtbar (Kennzeichen großgeschrieben)",
           r.status_code == 200 and "MOS-GK 100" in seite and "VW Polo 1.0 TSI" in seite)
    report("Tagessatz korrekt formatiert", "39,00" in seite)
    report("Fahrzeug zeigt Status Verfügbar",
           re.search(r"MOS-GK 100.*?Verfügbar", seite, re.S) is not None)

    fz = portal.list_mietfahrzeuge()
    report("Genau 1 Fahrzeug in DB", len(fz) == 1, f"{len(fz)} gefunden")
    fid = fz[0]["id"]
    report("Effektiver Status = verfuegbar", fz[0]["effektiver_status"] == "verfuegbar",
           fz[0]["effektiver_status"])

    # 3) Nav-Badge: noch niemand unterwegs
    report("Badge-Zähler 0 vor Vermietung", portal.mietfahrzeuge_unterwegs_anzahl() == 0,
           str(portal.mietfahrzeuge_unterwegs_anzahl()))

    # 4) Vergeben an Kunden (heute -> in 5 Tagen)
    r = client.post(f"/admin/mietfahrzeuge/{fid}/vermieten", data=with_csrf(client, {
        "kunde_name": "Erika Muster-TEST",
        "kunde_telefon": "06261 111",
        "start_datum": heute.isoformat(),
        "end_datum": in_5_tagen,
        "notiz": "Ersatzwagen während Lackierung",
    }), follow_redirects=True)
    seite = r.get_data(as_text=True)
    report("Fahrzeug vergeben -> Status Vermietet",
           "Erika Muster-TEST" in seite and "Vermietet" in seite)
    report("Geplante Rückgabe sichtbar im Belegungsplan",
           "Erika Muster-TEST" in seite and "Unterwegs" in seite)

    fz = portal.list_mietfahrzeuge()[0]
    report("Effektiver Status jetzt = vermietet", fz["effektiver_status"] == "vermietet",
           fz["effektiver_status"])
    report("Badge-Zähler 1 nach Vermietung", portal.mietfahrzeuge_unterwegs_anzahl() == 1,
           str(portal.mietfahrzeuge_unterwegs_anzahl()))
    vorgang_id = fz["aktiver_vorgang"]["id"]

    # 5) Doppelvergabe muss verhindert werden
    r = client.post(f"/admin/mietfahrzeuge/{fid}/vermieten", data=with_csrf(client, {
        "kunde_name": "Zweiter Kunde",
        "start_datum": heute.isoformat(),
    }), follow_redirects=True)
    report("Doppelvergabe wird abgelehnt",
           "bereits an" in r.get_data(as_text=True))
    report("Trotz Versuch nur 1 aktiver Vorgang",
           len([v for v in portal.list_mietvorgaenge(fid) if not v["abgeschlossen"]]) == 1)

    # 6) Zuruecknehmen
    r = client.post(f"/admin/mietvorgang/{vorgang_id}/zurueck", data=with_csrf(client, {
        "rueckgabe_datum": heute.isoformat(),
    }), follow_redirects=True)
    seite = r.get_data(as_text=True)
    report("Nach Rückgabe wieder Verfügbar",
           re.search(r"MOS-GK 100.*?Verfügbar", seite, re.S) is not None)
    fz = portal.list_mietfahrzeuge()[0]
    report("Effektiver Status zurück auf verfuegbar", fz["effektiver_status"] == "verfuegbar",
           fz["effektiver_status"])
    report("Badge-Zähler wieder 0", portal.mietfahrzeuge_unterwegs_anzahl() == 0)
    report("Rückgabe steht im Verlauf", len(fz["historie"]) == 1, f"{len(fz['historie'])} im Verlauf")

    # 7) Reservierung in der Zukunft -> reserviert
    r = client.post(f"/admin/mietfahrzeuge/{fid}/vermieten", data=with_csrf(client, {
        "kunde_name": "Zukunfts-Kunde",
        "start_datum": (heute + timedelta(days=3)).isoformat(),
        "end_datum": (heute + timedelta(days=6)).isoformat(),
    }), follow_redirects=True)
    fz = portal.list_mietfahrzeuge()[0]
    report("Zukünftiger Start = reserviert (nicht vermietet)",
           fz["effektiver_status"] == "reserviert", fz["effektiver_status"])
    report("Reservierung zählt NICHT als unterwegs",
           portal.mietfahrzeuge_unterwegs_anzahl() == 0)
    # Reservierung wieder entfernen
    res_id = fz["naechste_reservierung"]["id"]
    client.post(f"/admin/mietvorgang/{res_id}/loeschen", data=with_csrf(client), follow_redirects=True)

    # 8) Bearbeiten
    r = client.post(f"/admin/mietfahrzeuge/{fid}/bearbeiten", data=with_csrf(client, {
        "kennzeichen": "MOS-GK 100",
        "bezeichnung": "VW Polo 1.0 TSI Comfortline",
        "fahrzeugklasse": "Kleinwagen",
        "status": "wartung",
        "tagessatz": "42,00",
        "aktiv": "1",
    }), follow_redirects=True)
    seite = r.get_data(as_text=True)
    report("Bearbeiten übernimmt Modell + Wartungs-Status",
           "Comfortline" in seite and "In Wartung" in seite)

    # 8b) Fotos hochladen, ausliefern, Titelbild, loeschen
    def png_bytes(color):
        from PIL import Image
        buf = io.BytesIO()
        Image.new("RGB", (240, 160), color).save(buf, "PNG")
        buf.seek(0)
        return buf

    r = client.post(
        f"/admin/mietfahrzeuge/{fid}/bilder",
        data=with_csrf(client, {
            "bilder": [
                (png_bytes((200, 30, 30)), "front.png"),
                (png_bytes((30, 30, 200)), "heck.png"),
            ],
        }),
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    seite = r.get_data(as_text=True)
    report("2 Fotos hochgeladen", "Fotos (2)" in seite or "/mietfahrzeuge/bild/" in seite)
    bilder = portal.list_mietfahrzeug_bilder(fid)
    report("2 Bilder in DB", len(bilder) == 2, f"{len(bilder)}")
    report("Erstes Bild ist automatisch Titelbild",
           sum(1 for b in bilder if b["ist_titelbild"]) == 1)

    bild1, bild2 = bilder[0], bilder[1]
    r = client.get(f"/admin/mietfahrzeuge/bild/{bild1['id']}")
    report("Bild wird ausgeliefert (Bytes + image-Typ)",
           r.status_code == 200 and r.headers.get("Content-Type", "").startswith("image/")
           and len(r.get_data()) > 100, f"Status {r.status_code}, {r.headers.get('Content-Type')}")
    # Antwort schließen, sonst hält der Test-Client unter Windows das Datei-Handle
    # offen und das spätere unlink schlägt fehl (auf Render/Linux irrelevant).
    r.close()

    # Titelbild auf das zweite umstellen
    nicht_titel = next(b for b in bilder if not b["ist_titelbild"])
    client.post(f"/admin/mietfahrzeuge/bild/{nicht_titel['id']}/titelbild",
                data=with_csrf(client), follow_redirects=True)
    neu = {b["id"]: b for b in portal.list_mietfahrzeug_bilder(fid)}
    report("Titelbild umgestellt", neu[nicht_titel["id"]]["ist_titelbild"] == 1)

    # Datei liegt physisch auf der Platte?
    pfad = portal.upload_file_path(bild1)
    report("Bilddatei liegt im Upload-Ordner", pfad is not None and pfad.exists())

    # Ein Bild loeschen -> Datei muss weg sein, Titelbild rueckt nach
    geloescht = neu[bild2["id"]] if bild2["id"] in neu else bilder[1]
    pfad_del = portal.upload_file_path(geloescht)
    client.post(f"/admin/mietfahrzeuge/bild/{geloescht['id']}/loeschen",
                data=with_csrf(client), follow_redirects=True)
    rest = portal.list_mietfahrzeug_bilder(fid)
    report("Bild gelöscht (DB)", len(rest) == 1, f"{len(rest)} übrig")
    report("Bilddatei physisch entfernt", not (pfad_del and pfad_del.exists()))
    report("Verbleibendes Bild ist Titelbild", rest and rest[0]["ist_titelbild"] == 1)

    # Merke die Datei des verbleibenden Bildes -> muss beim Fahrzeug-Loeschen mit weg
    rest_pfad = portal.upload_file_path(rest[0])

    # 9) Loeschen (Anker-Karte muss verschwinden; Kennzeichen steht auch als
    #    Formular-Platzhalter auf der Seite -> deshalb auf die Karte pruefen)
    r = client.post(f"/admin/mietfahrzeuge/{fid}/loeschen", data=with_csrf(client), follow_redirects=True)
    seite = r.get_data(as_text=True)
    report("Fahrzeug-Karte gelöscht",
           f'id="fahrzeug-{fid}"' not in seite and "Comfortline" not in seite)
    report("DB wieder leer", len(portal.list_mietfahrzeuge()) == 0)
    rest_bilder_n = len(portal.list_mietfahrzeug_bilder(fid))
    datei_weg = not (rest_pfad and rest_pfad.exists())
    report("Bilder mit dem Fahrzeug entfernt (DB + Platte)",
           rest_bilder_n == 0 and datei_weg,
           f"DB-Bilder={rest_bilder_n}, Datei-weg={datei_weg}, Pfad={rest_pfad}")

    # Abschluss
    print()
    fails = [r for r in RESULTS if not r[1]]
    print(f"== ERGEBNIS: {len(RESULTS) - len(fails)}/{len(RESULTS)} Checks bestanden ==")
    if fails:
        for label, _, detail in fails:
            print(f"  - {label}" + (f" ({detail})" if detail else ""))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
