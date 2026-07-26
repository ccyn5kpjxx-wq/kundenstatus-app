# -*- coding: utf-8 -*-
"""Isolierter Integrationstest fuer Homepage-Anfragen und mehrere Lead-Bilder."""

from __future__ import annotations

import base64
from datetime import date, timedelta
from io import BytesIO
import os
import pathlib
import sys
import tempfile

from werkzeug.datastructures import MultiDict


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TEMP_DIR = pathlib.Path(tempfile.mkdtemp(prefix="homepage_kurzanfrage_"))
os.environ.update(
    {
        "RENDER": "local-homepage-kurzanfrage-test",
        "DATABASE_URL": "",
        "REQUIRE_POSTGRES_ON_RENDER": "0",
        "DATA_DIR": str(TEMP_DIR),
        "SQLITE_DB_PATH": str(TEMP_DIR / "test.db"),
        "UPLOAD_DIR": str(TEMP_DIR / "uploads"),
        "BACKUP_DIR": str(TEMP_DIR / "backups"),
        "DELETED_UPLOAD_DIR": str(TEMP_DIR / "deleted"),
        "AUTO_BACKUP_ENABLED": "0",
        "AUTO_CHANGE_BACKUP_ENABLED": "0",
        "LEXWARE_API_KEY": "",
        "OPENAI_API_KEY": "",
        "WHATSAPP_ACCESS_TOKEN": "",
        "WHATSAPP_PHONE_NUMBER_ID": "",
        "SCHADEN_VORSCHAU_AKTIV": "0",
        "FLASK_SECRET_KEY": "homepage-kurzanfrage-test",
        "ADMIN_PASS": "homepage-kurzanfrage-test",
    }
)

import app as portal  # noqa: E402


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


def check(label, condition):
    passed = bool(condition)
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")
    return passed


def scalar(sql, params=()):
    db = portal.get_db()
    try:
        row = db.execute(sql, params).fetchone()
        return row[0] if row else None
    finally:
        db.close()


def main():
    portal.app.config["TESTING"] = True
    client = portal.app.test_client()
    checks = []

    bestand_id = portal.create_auftrag(
        "intern",
        kunde_name="Bestand Kunde",
        fahrzeug="Bestandsfahrzeug",
        kennzeichen="MOS-ALT 1",
        beschreibung="Nicht verändern",
    )
    bestand_vorher = dict(portal.get_auftrag(bestand_id))

    homepage = client.get("/homepage")
    homepage_html = homepage.get_data(as_text=True)
    auswahl = client.get("/anfrage?anliegen=schaden")
    auswahl_html = auswahl.get_data(as_text=True)
    lack = client.get("/anfrage?anliegen=lackanfrage")
    lack_html = lack.get_data(as_text=True)
    pflege = client.get("/anfrage?anliegen=fahrzeugpflege")
    pflege_html = pflege.get_data(as_text=True)
    mechanik = client.get("/anfrage?anliegen=mechanikanfrage")
    mechanik_html = mechanik.get_data(as_text=True)
    mietwagen = client.get("/anfrage?anliegen=mietwagenanfrage")
    mietwagen_html = mietwagen.get_data(as_text=True)
    weitere = client.get("/anfrage?anliegen=weitere_anfrage")
    weitere_html = weitere.get_data(as_text=True)
    dellen = client.get("/anfrage?anliegen=dellenreparatur")
    dellen_html = dellen.get_data(as_text=True)

    checks += [
        check("Homepage verlinkt neue Auswahl", "/anfrage?anliegen=schaden" in homepage_html),
        check("Alle Anfragewege sind auswählbar", auswahl.status_code == 200 and all(
            text in auswahl_html for text in (
                "Lackanfrage", "Reparaturanfrage", "Mechanik &amp; Wartung", "Fahrzeugpflege", "Mietwagen",
                "Fahrzeugcheck", "Weitere Anfrage", "Versicherungsschaden",
            )
        )),
        check("Versicherung bleibt eigener Prozess", 'href="/privat/schaden"' in auswahl_html),
        check("Kurzanfrage bleibt kurz", lack.status_code == 200 and all(
            marker in lack_html for marker in (
                "Nur das Nötigste", 'name="name"', 'name="telefon"',
                'name="nachricht"', 'name="bilder"', 'enctype="multipart/form-data"',
            )
        )),
        check("Kurzanfrage erlaubt mehrere Bilder", "Bilder hinzufügen" in lack_html and 'multiple' in lack_html),
        check("Fahrzeugpflege bietet Kategorien", pflege.status_code == 200 and all(
            marker in pflege_html for marker in (
                'name="pflege_kategorie"', "Innenaufbereitung", "Außenaufbereitung",
                "1-Step-Politur", "2-Step-Politur", "Komplettaufbereitung",
            )
        )),
        check("Mechanik bietet Wartungs- und Problemkategorien", mechanik.status_code == 200 and all(
            marker in mechanik_html for marker in (
                'name="mechanik_kategorie"', "Bremsen prüfen / wechseln", "Ölwechsel",
                "Inspektion / Wartung", "Motor / Antrieb", "Elektrik / Batterie",
                "Sonstiges Mechanikproblem",
            )
        )),
        check("Mietwagen zeigt den auswählbaren Fuhrpark", mietwagen.status_code == 200 and all(
            marker in mietwagen_html for marker in (
                'name="mietwagen_fahrzeug"', "Hyundai i10", "Hyundai KONA N Line X",
                "Fiat Doblò", "Fiat Doblò Maxi", "39 € / Tag", "59 € / Tag",
                "i10_symbolfoto.jpg", "kona_symbolfoto.jpg", "doblo_symbolfoto.jpg",
                "doblo_maxi_symbolfoto.jpg", "Noch unsicher oder anderer Fahrzeugwunsch",
                'name="mietbeginn"', 'name="mietende"', 'name="nachricht"', 'name="bilder"',
            )
        )),
        check("Weitere Anfrage deckt übrige Kategorien ab", weitere.status_code == 200 and all(
            marker in weitere_html for marker in (
                'name="weitere_anfrage_art"', "Werkstatttermin", "Farbton- und Lackberatung",
                "Felgenservice", "Dellenreparatur", "Allgemeine Beratung",
            )
        )),
        check("Normale Anfrage erlaubt mehrere Bilder", dellen.status_code == 200 and all(
            marker in dellen_html for marker in (
                'name="bilder"', 'multiple', 'enctype="multipart/form-data"', "Bis zu 5 Bilder",
            )
        )),
    ]

    with client.session_transaction() as session:
        csrf_token = session.get(portal.CSRF_FIELD_NAME)
    response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "lackanfrage"),
            ("besichtigungsart", "werkstatt"),
            ("name", "Kurzanfrage Kunde"),
            ("telefon", "0171 1234567"),
            ("email", ""),
            ("fahrzeug", "VW Golf"),
            ("nachricht", "Kratzer an der hinteren Tür"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "schaden-vorne.png")),
            ("bilder", (BytesIO(PNG_1X1), "schaden-seite.png")),
            ("bilder", (BytesIO(PNG_1X1), "schaden-detail.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    lead = portal.get_lead(lead_id) if lead_id else None
    lead_dateien = portal.list_lead_dateien(lead_id) if lead_id else []
    upload_paths = [portal.UPLOAD_DIR / item["stored_name"] for item in lead_dateien]
    checks += [
        check("Kurzanfrage erzeugt Lead", response.status_code == 302 and lead and lead["quelle"] == "website"),
        check("Anliegen eindeutig gespeichert", lead and "Lackanfrage" in lead["beschreibung"]),
        check("Drei Bilder sicher gespeichert", len(lead_dateien) == 3 and all(
            path.is_file() and path.read_bytes() == PNG_1X1 for path in upload_paths
        )),
        check("Bilder starten keine OCR", lead and not lead.get("ki_analyse_text")),
        check("Lead ist dem Werkstatt-Cockpit zugeordnet", lead and lead["quelle"] == "website"),
    ]

    pflege_response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "fahrzeugpflege"),
            ("name", "Pflegeanfrage Kunde"),
            ("telefon", "0171 2223344"),
            ("email", ""),
            ("fahrzeug", "Audi A4"),
            ("pflege_kategorie", "innen"),
            ("pflege_kategorie", "2-step"),
            ("nachricht", "Innenraum stark verschmutzt und feine Kratzer im Lack"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "pflege-innen.png")),
            ("bilder", (BytesIO(PNG_1X1), "pflege-lack.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    pflege_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    pflege_lead = portal.get_lead(pflege_lead_id) if pflege_lead_id else None
    pflege_dateien = portal.list_lead_dateien(pflege_lead_id) if pflege_lead_id else []
    checks += [
        check("Fahrzeugpflege erzeugt Lead", pflege_response.status_code == 302 and pflege_lead_id > lead_id),
        check("Pflegekategorien stehen im Lead", pflege_lead and all(
            text in pflege_lead["beschreibung"] for text in ("Innenaufbereitung", "2-Step-Politur")
        )),
        check("Fahrzeugpflege speichert mehrere Bilder", len(pflege_dateien) == 2),
    ]

    mechanik_response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "mechanikanfrage"),
            ("name", "Mechanikanfrage Kunde"),
            ("telefon", "0171 7778899"),
            ("email", ""),
            ("fahrzeug", "BMW 320d"),
            ("mechanik_kategorie", "bremsen"),
            ("mechanik_kategorie", "oelwechsel"),
            ("nachricht", "Bremsen quietschen und der nächste Ölwechsel ist fällig"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "bremse-vorne.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    mechanik_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    mechanik_lead = portal.get_lead(mechanik_lead_id) if mechanik_lead_id else None
    mechanik_dateien = portal.list_lead_dateien(mechanik_lead_id) if mechanik_lead_id else []
    checks += [
        check("Mechanikanfrage erzeugt Werkstatt-Lead", mechanik_response.status_code == 302 and mechanik_lead_id > pflege_lead_id),
        check("Mechanikkategorien stehen im Lead", mechanik_lead and all(
            text in mechanik_lead["beschreibung"] for text in ("Bremsen prüfen / wechseln", "Ölwechsel")
        )),
        check("Mechanikanfrage speichert Bild", len(mechanik_dateien) == 1),
    ]

    mietbeginn = (date.today() + timedelta(days=7)).isoformat()
    mietende = (date.today() + timedelta(days=10)).isoformat()
    mietwagen_response = client.post(
        "/anfrage",
        data={
            portal.CSRF_FIELD_NAME: csrf_token,
            "anliegen": "mietwagenanfrage",
            "name": "Mietwagenanfrage Kunde",
            "telefon": "0171 3334455",
            "email": "",
            "mietwagen_fahrzeug": "hyundai-kona",
            "fahrzeug": "",
            "mietbeginn": mietbeginn,
            "mietende": mietende,
            "nachricht": "Mietwagen für einen Werkstattaufenthalt benötigt",
            "website": "",
        },
        follow_redirects=False,
    )
    mietwagen_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    mietwagen_lead = portal.get_lead(mietwagen_lead_id) if mietwagen_lead_id else None
    checks += [
        check("Mietwagenanfrage erzeugt Werkstatt-Lead", mietwagen_response.status_code == 302 and mietwagen_lead_id > mechanik_lead_id),
        check("Mietfahrzeug, Kurzprofil und Tarif stehen im Lead", mietwagen_lead and all(
            text in mietwagen_lead["beschreibung"] for text in (
                "Hyundai KONA N Line X", "Kompakt-SUV · Automatik", "59 € / Tag",
                "Ab 3 Tagen 49 € / Tag", "Jetzt verfügbar",
            )
        )),
        check("Mietzeitraum steht im Lead", mietwagen_lead and all(
            text in mietwagen_lead["beschreibung"] for text in (mietbeginn, mietende, "Mietwagenanfrage")
        )),
    ]

    normale_response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "dellenreparatur"),
            ("name", "Dellenanfrage Kunde"),
            ("telefon", "0171 7654321"),
            ("email", ""),
            ("fahrzeug", "Seat Leon"),
            ("wunschdatum", ""),
            ("nachricht", "Delle in der Beifahrertür"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "delle-gesamt.png")),
            ("bilder", (BytesIO(PNG_1X1), "delle-detail.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    normale_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    normale_dateien = portal.list_lead_dateien(normale_lead_id) if normale_lead_id else []
    checks += [
        check("Normale Anfrage mit Bildern erzeugt Lead", normale_response.status_code == 302 and normale_lead_id > lead_id),
        check("Normale Anfrage speichert beide Bilder", len(normale_dateien) == 2),
    ]

    with client.session_transaction() as session:
        session["admin"] = True
        admin_csrf = session.get(portal.CSRF_FIELD_NAME)
    detail = client.get(f"/admin/leads/{lead_id}")
    datei_antworten = [
        client.get(f"/admin/leads/{lead_id}/anhaenge/{item['id']}") for item in lead_dateien
    ]
    detail_html = detail.get_data(as_text=True)
    cockpit = client.get("/admin/leads")
    cockpit_html = cockpit.get_data(as_text=True)
    checks += [
        check("Alle Bilder im Lead sichtbar", detail.status_code == 200 and all(
            item["original_name"] in detail_html for item in lead_dateien
        )),
        check("Geschützte Bild-Routen liefern Originale", all(
            antwort.status_code == 200 and antwort.data == PNG_1X1 for antwort in datei_antworten
        )),
        check("Neue Anfragearten erscheinen im Lead-Cockpit", cockpit.status_code == 200 and all(
            name in cockpit_html for name in ("Pflegeanfrage Kunde", "Mechanikanfrage Kunde", "Mietwagenanfrage Kunde")
        )),
    ]

    umwandlung = client.post(
        f"/admin/leads/{lead_id}/auftrag",
        data={portal.CSRF_FIELD_NAME: admin_csrf},
        follow_redirects=False,
    )
    lead_nachher = portal.get_lead(lead_id)
    auftrag_id = int(lead_nachher["auftrag_id"] or 0)
    leadbild_count = int(
        scalar(
            "SELECT COUNT(*) FROM dateien WHERE auftrag_id=? AND kategorie='leadbild'",
            (auftrag_id,),
        )
        or 0
    )
    bestand_nachher = dict(portal.get_auftrag(bestand_id))
    lead_dateien_nachher = portal.list_lead_dateien(lead_id)
    checks += [
        check("Lead wird normaler Auftrag", umwandlung.status_code == 302 and auftrag_id > 0),
        check("Alle Bilder bleiben am Auftrag erhalten", leadbild_count == 3 and all(path.is_file() for path in upload_paths)),
        check("Bildhistorie bleibt auch am Lead erhalten", len(lead_dateien_nachher) == 3),
        check("Bestehender Auftrag bleibt unverändert", bestand_nachher == bestand_vorher),
    ]

    print(f"Temporäre Testdaten: {TEMP_DIR}")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
