# -*- coding: utf-8 -*-
"""Test: Admin-Mietfahrzeugseite zeigt immer ein Bild.

Faelle:
1. Foto-Datei fehlt auf Disk UND es gibt kein DB-Backup (Altbestand vor
   mietbild_backups) -> /admin/mietfahrzeuge/bild/<id> liefert Symbolfoto
   statt 404 (vorher: kaputte Thumbnails auf der Flotten-Seite).
2. Fahrzeug ganz ohne Fotos -> /admin/mietfahrzeuge/<id>/symbolfoto liefert
   ein Bild, Karte rendert den Symbolfoto-Platzhalter.
3. Unbekannte IDs -> weiterhin 404.

Laeuft auf einer eigenen Test-DB, keine Live-Daten.
"""
import io
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402

_tmpdir = tempfile.mkdtemp(prefix="mietbild_fb_test_")
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
    client.get("/admin/mietfahrzeuge")  # legt csrf_token in der Session an

    # Fahrzeug mit Foto anlegen (Kona -> Symbolfoto kona.jpg vorhanden)
    client.post("/admin/mietfahrzeuge/neu", data=with_csrf(client, {
        "kennzeichen": "IN-ZULAUF-2",
        "bezeichnung": "Hyundai Kona 1.6 T-GDI N Line X",
        "fahrzeugklasse": "SUV / Geländewagen",
        "tagessatz": "59,00",
        "status": "verfuegbar",
    }))
    fz = portal.list_mietfahrzeuge()
    report("Fahrzeug angelegt", len(fz) == 1)
    fid = fz[0]["id"]

    png = (b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
           b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01"
           b"\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82")
    client.post(f"/admin/mietfahrzeuge/{fid}/bilder", data=with_csrf(client, {
        "bilder": (io.BytesIO(png), "kona_front.png"),
    }), content_type="multipart/form-data")
    bilder = portal.list_mietfahrzeug_bilder(fid)
    report("Foto hochgeladen", len(bilder) == 1)
    bild = bilder[0]

    # Intaktes Foto wird normal ausgeliefert (kein Symbolfoto)
    r = client.get(f"/admin/mietfahrzeuge/bild/{bild['id']}")
    report("Intaktes Foto: Original wird geliefert",
           r.status_code == 200 and r.data.startswith(b"\x89PNG"),
           f"Status {r.status_code}")
    r.close()  # Windows: send_file haelt die Datei sonst offen -> unlink schlaegt fehl

    # Deploy-Verlust simulieren: Datei weg + kein DB-Backup (Altbestand)
    (portal.UPLOAD_DIR / Path(bild["stored_name"]).name).unlink()
    with portal.app.app_context():
        db = portal.get_db()
        db.execute("DELETE FROM mietbild_backups WHERE bild_id=?", (bild["id"],))
        db.commit()
        db.close()

    r = client.get(f"/admin/mietfahrzeuge/bild/{bild['id']}")
    kona_bytes = (Path(portal.app.static_folder) / "mietwagen" / "kona.jpg").read_bytes()
    report("Verlorenes Foto: Symbolfoto statt 404",
           r.status_code == 200 and r.data == kona_bytes,
           f"Status {r.status_code}, {len(r.data)} Bytes")

    # Flotten-Seite: Thumbnail-URL vorhanden und lieferbar (kein kaputtes Bild)
    seite = client.get("/admin/mietfahrzeuge").get_data(as_text=True)
    report("Flotten-Karte referenziert Bildroute",
           f"/admin/mietfahrzeuge/bild/{bild['id']}" in seite)

    # Fahrzeug ohne Fotos -> Symbolfoto-Platzhalter auf der Karte
    client.post("/admin/mietfahrzeuge/neu", data=with_csrf(client, {
        "kennzeichen": "IN-ZULAUF-3",
        "bezeichnung": "Fiat Doblo",
        "fahrzeugklasse": "Transporter",
        "tagessatz": "55,00",
        "status": "verfuegbar",
    }))
    fz2 = [f for f in portal.list_mietfahrzeuge() if f["kennzeichen"] == "IN-ZULAUF-3"][0]
    r = client.get(f"/admin/mietfahrzeuge/{fz2['id']}/symbolfoto")
    doblo_bytes = (Path(portal.app.static_folder) / "mietwagen" / "doblo.jpg").read_bytes()
    report("Fahrzeug ohne Fotos: Symbolfoto-Route liefert Doblo-Bild",
           r.status_code == 200 and r.data == doblo_bytes,
           f"Status {r.status_code}")
    seite = client.get("/admin/mietfahrzeuge").get_data(as_text=True)
    report("Karte ohne Fotos zeigt Symbolfoto + Hinweis",
           f"/admin/mietfahrzeuge/{fz2['id']}/symbolfoto" in seite and "Symbolfoto" in seite)

    # Kaputte IDs bleiben 404
    r1 = client.get("/admin/mietfahrzeuge/bild/999999")
    r2 = client.get("/admin/mietfahrzeuge/999999/symbolfoto")
    report("Unbekannte IDs -> 404", r1.status_code == 404 and r2.status_code == 404,
           f"{r1.status_code}/{r2.status_code}")

    ok = sum(1 for _, o, _ in RESULTS if o)
    print(f"\n== ERGEBNIS: {ok}/{len(RESULTS)} Checks bestanden ==")
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
