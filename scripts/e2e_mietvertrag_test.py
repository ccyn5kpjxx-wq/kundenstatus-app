# -*- coding: utf-8 -*-
"""End-to-End-Test Mietvertrag: Daten -> Unterschrift -> Bestätigen -> PDF/QR -> Versandweg.

Deckt beide Fälle ab: Reparaturfall (Mietvorgang mit Auftrag -> QR/Statuslink)
und reine Vermietung (kein Auftrag -> kein QR). Läuft auf Test-DB, ohne Live-Mails.
"""
import base64
import io
import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import app as portal  # noqa: E402

_tmp = tempfile.mkdtemp(prefix="mietvertrag_test_")
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


def png_dataurl():
    from PIL import Image, ImageDraw
    buf = io.BytesIO()
    bild = Image.new("RGB", (200, 80), (255, 255, 255))
    zeichner = ImageDraw.Draw(bild)
    zeichner.line([(20, 55), (55, 25), (90, 60), (135, 22), (180, 50)], fill=(20, 32, 48), width=4)
    bild.save(buf, "PNG")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def draft_meta(vorgang_id):
    vorgang = portal.get_mietvorgang(vorgang_id)
    fahrzeug = portal.get_mietfahrzeug(vorgang["mietfahrzeug_id"])
    auftrag = portal.mietvertrag_auftrag(vorgang)
    return {
        "expected_draft_hash": portal.mietvertrag_entwurf_hash(vorgang, fahrzeug, auftrag),
        "expected_version": str(max(1, int(vorgang.get("vertrag_version") or 1))),
        "expected_text_version": portal.MIETVERTRAG_TEXT_VERSION,
        "kunde_name": str(vorgang.get("kunde_name") or ""),
        "kunde_telefon": str(vorgang.get("kunde_telefon") or ""),
        "start_datum": portal.iso_date(vorgang.get("start_datum")),
        "end_datum": portal.iso_date(vorgang.get("end_datum")),
    }


def version_meta(vorgang_id, include_target=False):
    vorgang = portal.get_mietvorgang(vorgang_id)
    version_nummer = max(1, int(vorgang.get("vertrag_version") or 1))
    version = portal.get_mietvertrag_version(vorgang_id, version_nummer)
    meta = {
        "expected_version": str(version_nummer),
        "expected_snapshot_hash": str((version or {}).get("snapshot_sha256") or ""),
        "expected_pdf_hash": str((version or {}).get("pdf_sha256") or ""),
    }
    if include_target:
        weg, ziel = portal.mietvertrag_versandweg(vorgang, portal.mietvertrag_auftrag(vorgang))
        meta.update(
            {
                "expected_versandweg": str(weg or ""),
                "expected_versandziel": str(ziel or ""),
            }
        )
    return meta


def main():
    portal.app.config["TESTING"] = True
    # Produktionsstandard bleibt gesperrt. Für den isolierten E2E-Test wird ein
    # ausdrücklich versionierter, rechtlich freigegebener Testtext simuliert.
    portal.MIETVERTRAG_RECHTLICH_FREIGEGEBEN = True
    portal.MIETVERTRAG_TEXT_VERSION = "phase1-e2e-v1"
    portal.init_db()
    client = portal.app.test_client()
    with client.session_transaction() as s:
        s["admin"] = True
    client.get("/admin/mietfahrzeuge")  # CSRF etablieren
    heute = date.today().isoformat()

    # --- Reparaturfall: Auftrag mit Status-Token anlegen ---
    aid = portal.create_auftrag("kunde", kunde_name="Max Mustermann", fahrzeug="Audi A4",
                                kennzeichen="MOS-AB 123", kontakt_telefon="0176 12345678")
    db = sqlite3.connect(portal.DB)
    db.execute("UPDATE auftraege SET kunden_status_token='reptoken123', kunden_status_aktiv=1 WHERE id=?", (aid,))
    db.commit()
    db.close()
    report("Reparatur-Auftrag angelegt (mit Status-Token)", aid is not None)

    # --- Fahrzeug + Vermietung (Reparaturfall, mit E-Mail) ---
    fid = portal.create_mietfahrzeug(kennzeichen="MOS-GR 90", bezeichnung="Hyundai i10",
                                     fahrzeugklasse="Kleinwagen", tagessatz="35")
    r = client.post(f"/admin/mietfahrzeuge/{fid}/vermieten", data=with_csrf(client, {
        "kunde_name": "Max Mustermann",
        "kunde_telefon": "0176 12345678",
        "kunde_email": "max@example.de",
        "auftrag_id": str(aid),
        "start_datum": heute,
        "end_datum": heute,
    }), follow_redirects=True)
    vorgang = portal.list_mietfahrzeuge()[0]["aktiver_vorgang"]
    report("Vermietung im Reparaturfall angelegt", vorgang is not None and vorgang["auftrag_id"] == aid)
    vid = vorgang["id"]
    report("kunde_email gespeichert", portal.get_mietvorgang(vid)["kunde_email"] == "max@example.de")

    # --- Vertragsseite lädt, enthält §§ + QR (Reparaturfall) ---
    r = client.get(f"/admin/mietvorgang/{vid}/vertrag")
    seite = r.get_data(as_text=True)
    report("Vertragsseite lädt", r.status_code == 200 and "§ 1" in seite and "§ 9" in seite)
    report("QR/Statuslink im Reparaturfall sichtbar", "reptoken123/qr.svg" in seite and "/status/reptoken123" in seite)

    # --- Vertragsdaten speichern (eigene Selbstbeteiligung) ---
    felder = {f["name"]: f["default"] for f in portal.MIETVERTRAG_FELDER}
    felder["selbstbeteiligung_euro"] = "1.500,00"
    felder["kunde_adresse"] = "Musterstr. 12, 74821 Mosbach"
    felder["geburtsdatum"] = "1985-04-12"
    felder["ausweis_nr"] = "T22000129"
    felder["fuehrerschein_nr"] = "MUST85ABC123"
    felder["fuehrerschein_ausstellungsdatum"] = "2010-06-15"
    felder["fuehrerschein_behoerde"] = "Landratsamt Neckar-Odenwald-Kreis"
    felder["km_stand_uebergabe"] = "42150"
    felder["mietpreis_tag"] = "35,00"
    r = client.post(f"/admin/mietvorgang/{vid}/vertrag/speichern",
                    data=with_csrf(client, {
                        **draft_meta(vid), **felder, "kunde_email": "max@example.de"
                    }), follow_redirects=True)
    gespeichert = json.loads(portal.get_mietvorgang(vid)["vertrag_felder_json"] or "{}")
    report("Vertragsdaten gespeichert (SB 1.500)", gespeichert.get("selbstbeteiligung_euro") == "1.500,00")
    report("Status jetzt 'entwurf'", portal.get_mietvorgang(vid)["vertrag_status"] == "entwurf")

    # --- Security-Fix: ungültige E-Mail wird abgelehnt (alter Wert bleibt) ---
    client.post(f"/admin/mietvorgang/{vid}/vertrag/speichern",
                data=with_csrf(client, {
                    **draft_meta(vid), **felder, "kunde_email": "kein-at-zeichen"
                }), follow_redirects=True)
    report("Ungültige E-Mail wird abgelehnt", portal.get_mietvorgang(vid)["kunde_email"] == "max@example.de")

    # --- Security-Fix: Nicht-PNG-Unterschrift wird abgelehnt ---
    client.post(f"/admin/mietvorgang/{vid}/vertrag/unterschrift",
                data=with_csrf(client, {
                    **draft_meta(vid), "unterschrift_data": "data:image/png;base64,Zm9vYmFy"
                }), follow_redirects=True)
    v_nope = portal.get_mietvorgang(vid)
    report("Nicht-PNG-Unterschrift abgelehnt (Status bleibt entwurf)",
           not v_nope["unterschrift_stored"] and v_nope["vertrag_status"] == "entwurf")

    # --- Unterschrift speichern ---
    r = client.post(f"/admin/mietvorgang/{vid}/vertrag/unterschrift",
                    data=with_csrf(client, {**draft_meta(vid), "unterschrift_data": png_dataurl(),
                                            "unterschrift_name": "Max Mustermann", "unterschrift_ort": "Mosbach"}),
                    follow_redirects=True)
    v = portal.get_mietvorgang(vid)
    report("Unterschrift gespeichert + Status 'unterschrieben'",
           bool(v["unterschrift_stored"]) and v["vertrag_status"] == "unterschrieben")
    report("Snapshot eingefroren", bool(v["vertrag_snapshot_json"]))
    versionen = portal.list_mietvertrag_versionen(vid)
    report("Fixierte Vertragsversion mit Prüfsummen gespeichert",
           len(versionen) == 1 and bool(versionen[0]["snapshot_sha256"])
           and bool(versionen[0]["pdf_sha256"]) and bool(versionen[0]["pdf_base64"]))

    rimg = client.get(f"/admin/mietvorgang/{vid}/unterschrift.png")
    report("Unterschrift-Bild wird ausgeliefert",
           rimg.status_code == 200 and rimg.headers.get("Content-Type", "").startswith("image/"))
    rimg.close()

    # --- PDF (Reparaturfall, mit QR) ---
    rpdf = client.get(f"/admin/mietvorgang/{vid}/vertrag.pdf")
    pdf_bytes = rpdf.get_data()
    report("Vertrag-PDF (Reparaturfall) erzeugt",
           rpdf.status_code == 200 and rpdf.headers.get("Content-Type") == "application/pdf"
           and pdf_bytes[:4] == b"%PDF" and len(pdf_bytes) > 2000,
           f"{len(pdf_bytes)} Bytes")
    rpdf.close()

    # --- Bestätigen ---
    r = client.post(f"/admin/mietvorgang/{vid}/vertrag/bestaetigen",
                    data=with_csrf(client, version_meta(vid)), follow_redirects=True)
    report("Vertrag bestätigt", portal.get_mietvorgang(vid)["vertrag_status"] == "bestaetigt")

    # --- Versandweg E-Mail (SMTP nicht konfiguriert -> Warnung, kein Crash) ---
    weg, ziel = portal.mietvertrag_versandweg(portal.get_mietvorgang(vid), portal.get_auftrag(aid))
    report("Versandweg = E-Mail (E-Mail hinterlegt)", weg == "email" and ziel == "max@example.de")
    r = client.post(f"/admin/mietvorgang/{vid}/vertrag/senden",
                    data=with_csrf(client, version_meta(vid, include_target=True)), follow_redirects=True)
    report("Senden-Route ohne Crash (SMTP aus)", r.status_code == 200)

    # --- Reine Vermietung: kein Auftrag, nur Handynummer ---
    fid2 = portal.create_mietfahrzeug(kennzeichen="MOS-GR 91", bezeichnung="VW Up", fahrzeugklasse="Kleinwagen", tagessatz="30")
    client.post(f"/admin/mietfahrzeuge/{fid2}/vermieten", data=with_csrf(client, {
        "kunde_name": "Erika Beispiel", "kunde_telefon": "0151 99887766",
        "whatsapp_erlaubt": "1", "start_datum": heute, "end_datum": heute,
    }), follow_redirects=True)
    vid2 = portal.get_mietfahrzeug(fid2)["aktiver_vorgang"]["id"]
    seite2 = client.get(f"/admin/mietvorgang/{vid2}/vertrag").get_data(as_text=True)
    report("Reine Vermietung: kein QR/Statuslink", "qr.svg" not in seite2 and "Reine Vermietung" in seite2)
    weg2, _ = portal.mietvertrag_versandweg(portal.get_mietvorgang(vid2), None)
    report("Versandweg = WhatsApp (Handynummer + ausdrücklicher Wunsch)", weg2 == "whatsapp")
    rpdf2 = client.get(f"/admin/mietvorgang/{vid2}/vertrag.pdf")
    pb2 = rpdf2.get_data()
    report("Vertrag-PDF (reine Vermietung, ohne QR) erzeugt",
           rpdf2.status_code == 200 and pb2[:4] == b"%PDF" and len(pb2) > 2000)
    rpdf2.close()

    print()
    fails = [x for x in RESULTS if not x[1]]
    print(f"== ERGEBNIS: {len(RESULTS) - len(fails)}/{len(RESULTS)} Checks bestanden ==")
    for label, ok in fails:
        print(f"  - {label}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
