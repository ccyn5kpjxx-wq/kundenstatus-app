# -*- coding: utf-8 -*-
"""Regressionstest fuer die Phase-1-Sicherheitskorrekturen der Vermietung.

Der Test arbeitet ausschliesslich auf einer Temp-Datenbank samt Temp-Uploads.
Er prueft insbesondere Eingabevalidierung, kollisionsfreie Vermietung,
versionierte Vertragsnachweise mit Integritätsprüfung und den exakten Partner-Zugangscode.
"""

from __future__ import annotations

import base64
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import traceback
import zipfile
from datetime import date, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Beim Import initialisiert app.py seine Datenbank. Externe Dienste und Backups
# deshalb schon vor dem Import sicher deaktivieren und alle Datenpfade auf einen
# eigenen Temp-Ordner legen.
TMP_DIR = Path(tempfile.mkdtemp(prefix="phase1_sicherheit_"))
os.environ["RENDER"] = "local-test"
os.environ["DATABASE_URL"] = ""
os.environ["REQUIRE_POSTGRES_ON_RENDER"] = "false"
os.environ["AUTO_BACKUP_ENABLED"] = "false"
os.environ["AUTO_CHANGE_BACKUP_ENABLED"] = "false"
os.environ["DATA_DIR"] = str(TMP_DIR)
os.environ["SQLITE_DB_PATH"] = str(TMP_DIR / "phase1-test.db")
os.environ["UPLOAD_DIR"] = str(TMP_DIR / "uploads")
os.environ["DELETED_UPLOAD_DIR"] = str(TMP_DIR / "deleted_uploads")
os.environ["BACKUP_DIR"] = str(TMP_DIR / "backups")
os.environ["LEXWARE_API_KEY"] = ""
os.environ["OPENAI_API_KEY"] = ""
os.environ["WHATSAPP_ACCESS_TOKEN"] = ""
os.environ["WHATSAPP_PHONE_NUMBER_ID"] = ""
os.environ["MIETVERTRAG_RECHTLICH_FREIGEGEBEN"] = "false"
os.environ["MIETVERTRAG_TEXT_VERSION"] = ""

import app as portal  # noqa: E402
from werkzeug.datastructures import FileStorage  # noqa: E402


portal.DB = TMP_DIR / "phase1-test.db"
portal.DATA_DIR = TMP_DIR
portal.UPLOAD_DIR = TMP_DIR / "uploads"
portal.DELETED_UPLOAD_DIR = TMP_DIR / "deleted_uploads"
portal.BACKUP_DIR = TMP_DIR / "backups"
portal.USE_POSTGRES = False
portal.DATABASE_URL = ""


class Suite:
    def __init__(self):
        self.results: list[tuple[str, bool, str]] = []

    def check(self, label: str, condition: object, detail: str = "") -> bool:
        ok = bool(condition)
        self.results.append((label, ok, detail))
        suffix = f" - {detail}" if detail else ""
        print(f"[{'OK  ' if ok else 'FEHLER'}] {label}{suffix}")
        return ok

    def expect_value_error(self, label: str, callback, text: str = "") -> bool:
        try:
            callback()
        except ValueError as exc:
            message = str(exc)
            return self.check(
                label,
                not text or text.lower() in message.lower(),
                message,
            )
        except Exception as exc:  # pragma: no cover - Diagnosepfad
            return self.check(label, False, f"{type(exc).__name__}: {exc}")
        return self.check(label, False, "kein ValueError ausgeloest")

    def run_section(self, label: str, callback) -> None:
        print(f"\n== {label} ==")
        try:
            callback()
        except Exception as exc:  # Ein Backendfehler soll klar sichtbar sein.
            detail = f"{type(exc).__name__}: {exc}"
            self.check(f"Backendfehler in Abschnitt '{label}'", False, detail)
            traceback.print_exc()

    def exit_code(self) -> int:
        fehler = [result for result in self.results if not result[1]]
        print()
        print(
            f"== ERGEBNIS: {len(self.results) - len(fehler)}/{len(self.results)} "
            "Checks bestanden =="
        )
        if fehler:
            print("Blockierende Backend-/Regressionsergebnisse:")
            for label, _ok, detail in fehler:
                print(f"  - {label}" + (f" ({detail})" if detail else ""))
        return 1 if fehler else 0


def db_scalar(sql: str, params=()):
    db = portal.get_db()
    try:
        row = db.execute(sql, params).fetchone()
        return row[0] if row else None
    finally:
        db.close()


def db_execute(sql: str, params=()) -> None:
    db = portal.get_db()
    try:
        db.execute(sql, params)
        db.commit()
    finally:
        db.close()


def csrf_payload(client, data=None) -> dict:
    payload = dict(data or {})
    with client.session_transaction() as session:
        token = session.get("csrf_token")
    if not token:
        client.get("/")
        with client.session_transaction() as session:
            token = session.get("csrf_token")
    payload["csrf_token"] = token
    return payload


def admin_client():
    client = portal.app.test_client()
    with client.session_transaction() as session:
        session["admin"] = True
    client.get("/admin/mietfahrzeuge")
    return client


def png_bytes() -> bytes:
    from PIL import Image, ImageDraw

    stream = io.BytesIO()
    bild = Image.new("RGB", (240, 90), (255, 255, 255))
    zeichner = ImageDraw.Draw(bild)
    zeichner.line([(20, 65), (60, 25), (105, 68), (160, 20), (220, 58)], fill=(20, 32, 48), width=4)
    bild.save(stream, "PNG")
    return stream.getvalue()


def png_data_url() -> str:
    return "data:image/png;base64," + base64.b64encode(png_bytes()).decode("ascii")


def leere_png_data_url() -> str:
    from PIL import Image

    stream = io.BytesIO()
    Image.new("RGBA", (240, 90), (255, 255, 255, 0)).save(stream, "PNG")
    return "data:image/png;base64," + base64.b64encode(stream.getvalue()).decode("ascii")


def vollstaendige_vertragsfelder(*, mietpreis="49,90") -> dict[str, str]:
    werte: dict[str, str] = {}
    for feld in portal.MIETVERTRAG_FELDER:
        wert = str(feld.get("default") or "").strip()
        if not wert:
            typ = feld.get("typ")
            if typ == "datum":
                wert = "2015-06-15"
            elif typ == "zahl":
                wert = "1"
            elif typ == "euro":
                wert = "10,00"
            else:
                wert = "Testangabe"
        werte[feld["name"]] = wert
    werte.update(
        {
            "kunde_adresse": "Musterstrasse 12, 74821 Mosbach",
            "geburtsdatum": "1985-03-12",
            "ausweis_nr": "TEST-AUSWEIS-123",
            "fuehrerschein_nr": "TEST-FS-987",
            "fuehrerschein_klasse": "B",
            "fuehrerschein_ausstellungsdatum": "2010-05-20",
            "fuehrerschein_behoerde": "Landratsamt Testkreis",
            "km_stand_uebergabe": "12345",
            "mietpreis_tag": mietpreis,
            "kaution_euro": "500,00",
            "selbstbeteiligung_euro": "1.000,00",
        }
    )
    return werte


def vertragsentwurf_meta(vorgang_id: int) -> dict[str, str]:
    """Hidden-Formwerte der aktuell angezeigten Entwurfsseite nachbilden."""
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


def vertragsversion_meta(vorgang_id: int) -> dict[str, str]:
    """Hidden-Formwerte einer fixierten Vertragsversion nachbilden."""
    vorgang = portal.get_mietvorgang(vorgang_id)
    version_nummer = max(1, int(vorgang.get("vertrag_version") or 1))
    version = portal.get_mietvertrag_version(vorgang_id, version_nummer)
    return {
        "expected_version": str(version_nummer),
        "expected_snapshot_hash": str((version or {}).get("snapshot_sha256") or ""),
        "expected_pdf_hash": str((version or {}).get("pdf_sha256") or ""),
    }


def vertragsversand_meta(vorgang_id: int) -> dict[str, str]:
    meta = vertragsversion_meta(vorgang_id)
    vorgang = portal.get_mietvorgang(vorgang_id)
    weg, ziel = portal.mietvertrag_versandweg(vorgang, portal.mietvertrag_auftrag(vorgang))
    meta.update(
        {
            "expected_versandweg": str(weg or ""),
            "expected_versandziel": str(ziel or ""),
        }
    )
    return meta


def test_eingaben(suite: Suite) -> None:
    fid = portal.create_mietfahrzeug(
        kennzeichen="MOS-P1 10",
        bezeichnung="Phase-1 Eingabetest",
        fahrzeugklasse="Kleinwagen",
        tagessatz="39,00",
    )
    start = date.today() + timedelta(days=10)
    ende = start + timedelta(days=4)

    suite.expect_value_error(
        "Admin-Vermietung: fehlendes Abholdatum wird abgelehnt",
        lambda: portal.create_mietvorgang(
            fid,
            kunde_name="Test Kunde",
            kunde_telefon="0176 12345678",
            start_datum="",
            end_datum=ende.isoformat(),
        ),
        "Abholdatum",
    )
    suite.expect_value_error(
        "Admin-Vermietung: fehlendes Rueckgabedatum wird abgelehnt",
        lambda: portal.create_mietvorgang(
            fid,
            kunde_name="Test Kunde",
            kunde_telefon="0176 12345678",
            start_datum=start.isoformat(),
            end_datum="",
        ),
        "gabedatum",
    )
    suite.expect_value_error(
        "Admin-Vermietung: Rueckgabe vor Abholung wird abgelehnt",
        lambda: portal.create_mietvorgang(
            fid,
            kunde_name="Test Kunde",
            kunde_telefon="0176 12345678",
            start_datum=ende.isoformat(),
            end_datum=start.isoformat(),
        ),
        "nicht vor",
    )
    suite.expect_value_error(
        "Admin-Vermietung: ungueltige Telefonnummer wird abgelehnt",
        lambda: portal.create_mietvorgang(
            fid,
            kunde_name="Test Kunde",
            kunde_telefon="x",
            start_datum=start.isoformat(),
            end_datum=ende.isoformat(),
        ),
        "Telefonnummer",
    )
    suite.check(
        "Fehlversuche erzeugen keinen Mietvorgang",
        db_scalar("SELECT COUNT(*) FROM mietvorgaenge WHERE mietfahrzeug_id=?", (fid,)) == 0,
    )

    # Die urspruengliche Sicherheitsluecke lag auch im oeffentlichen Formular.
    public = portal.app.test_client()
    public.get("/mietwagen")
    faelle = [
        (
            "Oeffentliche Anfrage: Rueckgabe vor Abholung",
            {"start_datum": ende.isoformat(), "end_datum": start.isoformat(), "telefon": "0176 12345678"},
            "nicht vor",
        ),
        (
            "Oeffentliche Anfrage: fehlendes Abholdatum",
            {"start_datum": "", "end_datum": ende.isoformat(), "telefon": "0176 12345678"},
            "Abholdatum",
        ),
        (
            "Oeffentliche Anfrage: fehlendes Rueckgabedatum",
            {"start_datum": start.isoformat(), "end_datum": "", "telefon": "0176 12345678"},
            "gabedatum",
        ),
        (
            "Oeffentliche Anfrage: ungueltige Telefonnummer",
            {"start_datum": start.isoformat(), "end_datum": ende.isoformat(), "telefon": "x"},
            "Telefonnummer",
        ),
    ]
    for label, abweichung, meldung in faelle:
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        vorher = db_scalar("SELECT COUNT(*) FROM mietwagen_anfragen")
        daten = {
            "name": "Test Kunde",
            "telefon": "0176 12345678",
            "email": "test@example.de",
            "start_datum": start.isoformat(),
            "end_datum": ende.isoformat(),
            "mietfahrzeug_id": str(fid),
            "klasse_wunsch": "Kleinwagen",
        }
        daten.update(abweichung)
        response = public.post(
            "/mietwagen",
            data=csrf_payload(public, daten),
            follow_redirects=True,
        )
        html = response.get_data(as_text=True)
        nachher = db_scalar("SELECT COUNT(*) FROM mietwagen_anfragen")
        suite.check(
            label,
            response.status_code == 200 and vorher == nachher and meldung.lower() in html.lower(),
            f"HTTP {response.status_code}, Anfragen {vorher}->{nachher}",
        )


def test_doppelbuchung(suite: Suite) -> None:
    fid = portal.create_mietfahrzeug(
        kennzeichen="MOS-P1 20",
        bezeichnung="Phase-1 Doppelbuchung",
        fahrzeugklasse="Kompaktklasse",
        tagessatz="45,00",
    )
    start = date.today() + timedelta(days=20)
    ende = start + timedelta(days=3)
    first_id = portal.create_mietvorgang(
        fid,
        kunde_name="Erste Buchung",
        kunde_telefon="0176 11111111",
        start_datum=start.isoformat(),
        end_datum=ende.isoformat(),
    )
    suite.check("Erste Buchung wird angelegt", bool(first_id))
    suite.expect_value_error(
        "Ueberlappende Doppelbuchung wird abgelehnt",
        lambda: portal.create_mietvorgang(
            fid,
            kunde_name="Zweite Buchung",
            kunde_telefon="0176 22222222",
            start_datum=(start + timedelta(days=1)).isoformat(),
            end_datum=(ende + timedelta(days=1)).isoformat(),
        ),
        "bereits belegt",
    )
    suite.check(
        "Nach Doppelbuchungsversuch existiert genau ein offener Vorgang",
        len(portal.list_mietvorgaenge(fid, only_open=True)) == 1,
    )

    # Gleichzeitige Requests duerfen die vorherige Pruefung nicht umgehen.
    fid_parallel = portal.create_mietfahrzeug(
        kennzeichen="MOS-P1 21",
        bezeichnung="Phase-1 Paralleltest",
        fahrzeugklasse="Kompaktklasse",
        tagessatz="46,00",
    )
    barrier = threading.Barrier(3)
    resultate: list[tuple[str, object]] = []
    lock = threading.Lock()

    def buchen(name: str) -> None:
        barrier.wait()
        try:
            value = portal.create_mietvorgang(
                fid_parallel,
                kunde_name=name,
                kunde_telefon="0176 33333333",
                start_datum=start.isoformat(),
                end_datum=ende.isoformat(),
            )
            result = ("ok", value)
        except Exception as exc:  # Ergebnis wird unten exakt bewertet.
            result = ("error", exc)
        with lock:
            resultate.append(result)

    threads = [threading.Thread(target=buchen, args=(f"Parallel {nr}",)) for nr in (1, 2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=20)

    erfolge = [value for status, value in resultate if status == "ok"]
    fehler = [value for status, value in resultate if status == "error"]
    suite.check(
        "Parallele Buchung beendet ohne haengenden Thread",
        all(not thread.is_alive() for thread in threads),
    )
    suite.check(
        "Parallele Doppelbuchung: exakt eine Buchung gewinnt",
        len(erfolge) == 1
        and len(fehler) == 1
        and isinstance(fehler[0], ValueError)
        and "belegt" in str(fehler[0]).lower()
        and len(portal.list_mietvorgaenge(fid_parallel, only_open=True)) == 1,
        ", ".join(f"{status}:{value}" for status, value in resultate),
    )


def test_anfrage_backup_und_datenschutz(suite: Suite) -> None:
    benoetigte_tabellen = {
        "mietfahrzeuge",
        "mietvorgaenge",
        "mietvertrag_versionen",
        "mietfahrzeug_bilder",
        "mietbild_backups",
        "mietwagen_anfragen",
    }
    suite.check(
        "PostgreSQL-/JSON-Backup umfasst den vollstaendigen Mietbetrieb",
        benoetigte_tabellen.issubset(set(portal.BACKUP_TABLES)),
        ", ".join(sorted(benoetigte_tabellen - set(portal.BACKUP_TABLES))),
    )

    legacy_fid = portal.create_mietfahrzeug(
        kennzeichen="MOS-P1 24",
        bezeichnung="Phase-1 Legacy",
        fahrzeugklasse="Kleinwagen",
        tagessatz="35,00",
    )
    legacy_vorgang = portal.create_mietvorgang(
        legacy_fid,
        kunde_name="Legacy Testkunde",
        kunde_telefon="06261 987654",
        kunde_email="legacy@example.test",
        start_datum=(date.today() + timedelta(days=70)).isoformat(),
        end_datum=(date.today() + timedelta(days=72)).isoformat(),
    )
    db = portal.get_db()
    try:
        db.execute(
            """
            UPDATE mietvorgaenge
            SET vertrag_status='bestaetigt', vertrag_snapshot_json=?,
                vertrag_bestaetigt_am=?, vertrag_gesendet_am=?, vertrag_versandweg='email'
            WHERE id=?
            """,
            ('{"legacy":true}', portal.now_str(), portal.now_str(), legacy_vorgang),
        )
        portal.backfill_legacy_mietvertrag_versionen(db)
        db.commit()
    finally:
        db.close()
    legacy_version = portal.get_mietvertrag_version(legacy_vorgang, 1)
    suite.check(
        "Legacy-Migration bewahrt alten Vertrags- und Versandhinweis ohne falsche Freigabe",
        legacy_version is not None
        and legacy_version["quelle"] == "legacy_backfill"
        and not int(legacy_version["rechtlich_freigegeben"] or 0)
        and legacy_version["versandweg"] == "email"
        and legacy_version["versandziel"] == "legacy@example.test",
    )

    wartung_id = portal.create_mietfahrzeug(
        kennzeichen="MOS-P1 25",
        bezeichnung="Phase-1 Wartung",
        fahrzeugklasse="Kleinwagen",
        tagessatz="39,00",
        status="wartung",
    )
    suite.check(
        "Wartungsfahrzeuge werden nicht oeffentlich angeboten",
        wartung_id not in {f["id"] for f in portal.mietwagen_public_fahrzeuge()},
    )

    public = portal.app.test_client()
    gefaelschte_seite = public.get("/mietwagen?ok=1&anfrage=999999").get_data(as_text=True)
    suite.check(
        "Erfolgsmeldung und Anfragenummer lassen sich nicht per URL erfinden",
        "Anfrage angekommen" not in gefaelschte_seite and "MW-999999" not in gefaelschte_seite,
    )

    start = date.today() + timedelta(days=20)
    ende = start + timedelta(days=3)
    with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
        portal.PUBLIC_FORM_ATTEMPTS.clear()
    response = public.post(
        "/mietwagen",
        data=csrf_payload(
            public,
            {
                "name": "Anfrage Testkunde",
                "telefon": "06261 123456",
                "email": "anfrage@example.test",
                "start_datum": start.isoformat(),
                "end_datum": ende.isoformat(),
                "geplante_km": "600",
                "klasse_wunsch": "Kleinwagen",
                "nachricht": "Bitte telefonisch melden.",
            },
        ),
        follow_redirects=True,
    )
    anfrage = None
    db = portal.get_db()
    try:
        row = db.execute(
            "SELECT * FROM mietwagen_anfragen WHERE name=? ORDER BY id DESC LIMIT 1",
            ("Anfrage Testkunde",),
        ).fetchone()
        anfrage = dict(row) if row else None
    finally:
        db.close()
    suite.check(
        "Echte Formularantwort zeigt nur die sessiongebundene Anfragenummer",
        response.status_code == 200
        and anfrage is not None
        and "Anfrage angekommen" in response.get_data(as_text=True)
        and f"MW-{anfrage['id']}" in response.get_data(as_text=True),
    )
    suite.check(
        "Ohne WhatsApp-Wunsch werden Daten nicht an die WhatsApp-Benachrichtigung gegeben",
        anfrage is not None
        and not int(anfrage["whatsapp_erlaubt"] or 0)
        and anfrage["whatsapp_status"] == "nicht angefordert",
    )
    suite.check(
        "Geplante Kilometer werden mit der Mietwagenanfrage gespeichert",
        anfrage is not None and int(anfrage.get("geplante_km") or 0) == 600,
        f"gespeichert: {(anfrage or {}).get('geplante_km')}",
    )

    fahrzeug_id = portal.create_mietfahrzeug(
        kennzeichen="MOS-P1 26",
        bezeichnung="Phase-1 Anfrage",
        fahrzeugklasse="Kleinwagen",
        tagessatz="45,00",
    )
    clients = [admin_client(), admin_client()]
    payloads = [
        csrf_payload(
            client,
            {
                "mietfahrzeug_id": str(fahrzeug_id),
                "start_datum": start.isoformat(),
                "end_datum": ende.isoformat(),
            },
        )
        for client in clients
    ]
    barriere = threading.Barrier(2)
    resultate: list[tuple[int, str]] = []
    ergebnis_lock = threading.Lock()

    def uebernehmen(index: int) -> None:
        barriere.wait(timeout=5)
        antwort = clients[index].post(
            f"/admin/mietanfrage/{anfrage['id']}/uebernehmen",
            data=payloads[index],
            follow_redirects=True,
        )
        with ergebnis_lock:
            resultate.append((antwort.status_code, antwort.get_data(as_text=True)))

    threads = [threading.Thread(target=uebernehmen, args=(i,), daemon=True) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    anfrage_nachher = None
    db = portal.get_db()
    try:
        row = db.execute(
            "SELECT * FROM mietwagen_anfragen WHERE id=?", (int(anfrage["id"]),)
        ).fetchone()
        anfrage_nachher = dict(row) if row else None
    finally:
        db.close()
    suite.check(
        "Parallele Anfrageuebernahme erzeugt exakt einen Mietvorgang",
        all(not thread.is_alive() for thread in threads)
        and len(resultate) == 2
        and anfrage_nachher is not None
        and anfrage_nachher["status"] == "uebernommen"
        and int(anfrage_nachher["mietvorgang_id"] or 0) > 0
        and db_scalar(
            "SELECT COUNT(*) FROM mietvorgaenge WHERE mietfahrzeug_id=?",
            (fahrzeug_id,),
        )
        == 1,
        ", ".join(str(status) for status, _body in resultate),
    )

    auswahl_client = admin_client()
    auswahl_seite = auswahl_client.get(
        "/admin/ersatzfahrzeug?kunde_name=URL-Leak&kunde_telefon=01760000000"
    ).get_data(as_text=True)
    suite.check(
        "Ersatzfahrzeug-Auswahl ignoriert personenbezogene GET-Parameter",
        "URL-Leak" not in auswahl_seite
        and "01760000000" not in auswahl_seite
        and 'method="post"' in auswahl_seite.lower(),
    )


def test_storno_und_archiv(suite: Suite) -> None:
    start = date.today() + timedelta(days=30)
    ende = start + timedelta(days=2)
    fid = portal.create_mietfahrzeug(
        kennzeichen="MOS-P1 30",
        bezeichnung="Phase-1 Storno",
        fahrzeugklasse="Mittelklasse",
        tagessatz="55,00",
    )
    vorgang_id = portal.create_mietvorgang(
        fid,
        kunde_name="Storno Kunde",
        kunde_telefon="0176 44444444",
        start_datum=start.isoformat(),
        end_datum=ende.isoformat(),
    )
    suite.expect_value_error(
        "Storno ohne nachvollziehbaren Grund wird abgelehnt",
        lambda: portal.storniere_mietvorgang(vorgang_id, ""),
        "Stornogrund",
    )
    suite.check(
        "Abgelehntes Storno laesst Vorgang aktiv",
        portal.get_mietvorgang(vorgang_id)["status"] == "aktiv",
    )
    grund = "Kunde hat den Termin abgesagt"
    portal.storniere_mietvorgang(vorgang_id, grund)
    storniert = portal.get_mietvorgang(vorgang_id)
    suite.check(
        "Storno behaelt Datensatz, Grund und Zeitstempel",
        storniert is not None
        and storniert["status"] == "storniert"
        and storniert["storno_grund"] == grund
        and bool(storniert["storniert_am"]),
    )
    storno_zeitpunkt = storniert["storniert_am"]
    suite.expect_value_error(
        "Wiederholtes Storno ueberschreibt den Auditnachweis nicht",
        lambda: portal.storniere_mietvorgang(vorgang_id, "anderer Grund"),
        "bereits storniert",
    )
    suite.expect_value_error(
        "Stornierter Vorgang kann nicht als zurueckgegeben umgebucht werden",
        lambda: portal.mietvorgang_zuruecknehmen(vorgang_id),
        "storniert",
    )
    storniert_unveraendert = portal.get_mietvorgang(vorgang_id)
    suite.check(
        "Stornozeitpunkt und -grund bleiben nach Fehlversuchen erhalten",
        storniert_unveraendert["storniert_am"] == storno_zeitpunkt
        and storniert_unveraendert["storno_grund"] == grund
        and storniert_unveraendert["status"] == "storniert",
    )
    suite.check(
        "Storno gibt denselben Zeitraum wieder frei",
        portal.mietfahrzeug_zeitraum_frei(fid, start.isoformat(), ende.isoformat()),
    )
    ersatz_id = portal.create_mietvorgang(
        fid,
        kunde_name="Ersatzbuchung",
        kunde_telefon="0176 55555555",
        start_datum=start.isoformat(),
        end_datum=ende.isoformat(),
    )
    suite.check(
        "Freigegebener Zeitraum kann neu gebucht werden; Storno bleibt erhalten",
        bool(ersatz_id)
        and db_scalar("SELECT COUNT(*) FROM mietvorgaenge WHERE mietfahrzeug_id=?", (fid,)) == 2
        and portal.get_mietvorgang(vorgang_id)["status"] == "storniert",
    )
    suite.expect_value_error(
        "Ungueltiges tatsaechliches Rueckgabedatum wird abgelehnt",
        lambda: portal.mietvorgang_zuruecknehmen(ersatz_id, "kein-datum"),
        "gültiges tatsächliches Rückgabedatum",
    )
    suite.expect_value_error(
        "Zukuenftiges tatsaechliches Rueckgabedatum wird abgelehnt",
        lambda: portal.mietvorgang_zuruecknehmen(
            ersatz_id, (date.today() + timedelta(days=1)).isoformat()
        ),
        "Zukunft",
    )
    suite.check(
        "Abgelehnte Rueckgabe veraendert den Mietvorgang nicht",
        portal.get_mietvorgang(ersatz_id)["status"] == "aktiv"
        and not portal.get_mietvorgang(ersatz_id)["rueckgabe_datum"],
    )

    archiv_fid = portal.create_mietfahrzeug(
        kennzeichen="MOS-P1 31",
        bezeichnung="Phase-1 Archiv",
        fahrzeugklasse="Kombi",
        tagessatz="59,00",
    )
    bild_datei = FileStorage(
        stream=io.BytesIO(png_bytes()),
        filename="archiv-nachweis.png",
        content_type="image/png",
    )
    gespeichert = portal.save_mietfahrzeug_bilder(archiv_fid, [bild_datei])
    bilder_vorher = portal.list_mietfahrzeug_bilder(archiv_fid)
    bild_pfad = portal.upload_file_path(bilder_vorher[0]) if bilder_vorher else None
    offen_id = portal.create_mietvorgang(
        archiv_fid,
        kunde_name="Archiv Kunde",
        kunde_telefon="0176 66666666",
        start_datum=start.isoformat(),
        end_datum=ende.isoformat(),
    )
    client = admin_client()
    response = client.post(
        f"/admin/mietfahrzeuge/{archiv_fid}/bearbeiten",
        data=csrf_payload(
            client,
            {
                "kennzeichen": "MOS-P1 31",
                "bezeichnung": "Phase-1 Archiv",
                "fahrzeugklasse": "Kombi",
                "tagessatz": "59,00",
                "status": "verfuegbar",
                "aktiv": "0",
            },
        ),
        follow_redirects=True,
    )
    suite.check(
        "Bearbeiten-Route verweigert aktiv=0 bei offenem Mietvorgang",
        response.status_code == 200
        and bool(portal.get_mietfahrzeug(archiv_fid)["aktiv"])
        and "Archivierung" in response.get_data(as_text=True),
    )
    response = client.post(
        f"/admin/mietfahrzeuge/{archiv_fid}/bearbeiten",
        data=csrf_payload(
            client,
            {
                "kennzeichen": "MOS-P1 31",
                "bezeichnung": "Phase-1 Archiv",
                "fahrzeugklasse": "Kombi",
                "tagessatz": "59,00",
                "status": "inaktiv",
                "aktiv": "1",
            },
        ),
        follow_redirects=True,
    )
    fahrzeug_nach_status_bypass = portal.get_mietfahrzeug(archiv_fid)
    suite.check(
        "Bearbeiten-Route verweigert inaktiv-Status als Archivierungs-Bypass",
        response.status_code == 200
        and bool(fahrzeug_nach_status_bypass["aktiv"])
        and fahrzeug_nach_status_bypass["basis_status"] != "inaktiv"
        and "Archivierung" in response.get_data(as_text=True),
    )
    suite.expect_value_error(
        "Fahrzeugarchiv verweigert offenen Mietvorgang",
        lambda: portal.archiviere_mietfahrzeug(archiv_fid, "Flottenwechsel"),
        "laufende Vermietung",
    )
    suite.check(
        "Nach verweigertem Archiv bleibt Fahrzeug aktiv",
        bool(portal.get_mietfahrzeug(archiv_fid)["aktiv"]),
    )
    portal.storniere_mietvorgang(offen_id, "Archivierung nach Flottenwechsel")
    portal.archiviere_mietfahrzeug(archiv_fid, "Flottenwechsel")
    archiv = portal.get_mietfahrzeug(archiv_fid)
    suite.check(
        "Archivierung behaelt Fahrzeugzeile und dokumentiert Grund",
        archiv is not None
        and not archiv["aktiv"]
        and archiv["status"] == "inaktiv"
        and archiv["archivgrund"] == "Flottenwechsel"
        and bool(archiv["archiviert_am"]),
    )
    suite.check(
        "Archivierung bewahrt alle Mietvorgaenge",
        portal.get_mietvorgang(offen_id) is not None
        and db_scalar("SELECT COUNT(*) FROM mietvorgaenge WHERE mietfahrzeug_id=?", (archiv_fid,)) == 1,
    )
    suite.check(
        "Archivierung bewahrt Bildzeile, Backup und Datei",
        gespeichert == 1
        and len(portal.list_mietfahrzeug_bilder(archiv_fid)) == len(bilder_vorher) == 1
        and db_scalar(
            "SELECT COUNT(*) FROM mietbild_backups WHERE bild_id=?",
            (bilder_vorher[0]["id"],),
        )
        == 1
        and bool(bild_pfad and bild_pfad.exists()),
    )
    archivzeit = archiv["archiviert_am"]
    portal.update_mietfahrzeug(
        archiv_fid,
        kennzeichen=archiv["kennzeichen"],
        bezeichnung=archiv["bezeichnung"],
        fahrzeugklasse=archiv["fahrzeugklasse"],
        farbe=archiv["farbe"],
        fin_nummer=archiv["fin_nummer"],
        baujahr=archiv["baujahr"],
        kraftstoff=archiv["kraftstoff"],
        tagessatz=archiv["tagessatz"],
        standort=archiv["standort"],
        status="verfuegbar",
        notiz=archiv["notiz"],
        aktiv=True,
    )
    reaktiviert = portal.get_mietfahrzeug(archiv_fid)
    suite.check(
        "Reaktivierung bewahrt den letzten Archivierungsnachweis",
        reaktiviert["aktiv"]
        and reaktiviert["archiviert_am"] == archivzeit
        and reaktiviert["archivgrund"] == "Flottenwechsel",
    )


def test_partner_code(suite: Suite) -> None:
    portal_key = "phase1mixedcaseportal"
    mixed_code = "MiXeD-Case9"
    db = portal.get_db()
    try:
        cursor = db.execute(
            """
            INSERT INTO autohaeuser
            (name, slug, portal_key, zugangscode, erstellt_am)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("Phase 1 Testautohaus", "phase1-testautohaus", portal_key, mixed_code, portal.now_str()),
        )
        autohaus_id = cursor.lastrowid
        db.commit()
    finally:
        db.close()

    exakt = portal.app.test_client()
    exakt.get(f"/portal/{portal_key}")
    response = exakt.post(
        f"/portal/{portal_key}",
        data=csrf_payload(exakt, {"password": mixed_code}),
        follow_redirects=False,
    )
    with exakt.session_transaction() as session:
        angemeldet_id = session.get("partner_autohaus_id")
    suite.check(
        "Partner-MixedCase-Code meldet mit exakt gleicher Schreibweise an",
        response.status_code == 302 and angemeldet_id == autohaus_id,
        f"HTTP {response.status_code}, Session-ID {angemeldet_id}",
    )

    falsch = portal.app.test_client()
    falsch.get(f"/portal/{portal_key}")
    response = falsch.post(
        f"/portal/{portal_key}",
        data=csrf_payload(falsch, {"password": mixed_code.upper()}),
        follow_redirects=True,
    )
    with falsch.session_transaction() as session:
        falsche_session = session.get("partner_autohaus_id")
    suite.check(
        "Partner-Code bleibt exakt und ist nicht ungewollt case-insensitive",
        response.status_code == 200
        and falsche_session is None
        and "Falscher Zugangscode" in response.get_data(as_text=True),
        f"HTTP {response.status_code}, Session-ID {falsche_session}",
    )


def version_fingerprint(version: dict) -> dict:
    schluessel = (
        "version",
        "text_version",
        "status",
        "snapshot_json",
        "snapshot_sha256",
        "pdf_base64",
        "pdf_sha256",
        "unterschrift_base64",
        "unterschrift_sha256",
        "unterschrift_name",
        "unterschrift_ort",
        "unterschrift_am",
        "aenderungsgrund",
        "quelle",
        "rechtlich_freigegeben",
    )
    return {key: version.get(key) for key in schluessel}


def test_vertragsintegritaet(suite: Suite) -> None:
    original_freigabe = portal.MIETVERTRAG_RECHTLICH_FREIGEGEBEN
    original_textversion = portal.MIETVERTRAG_TEXT_VERSION
    original_mailversand = portal.send_mietvertrag_mail
    try:
        client = admin_client()
        start = date.today() + timedelta(days=45)
        ende = start + timedelta(days=4)
        fid = portal.create_mietfahrzeug(
            kennzeichen="MOS-P1 40",
            bezeichnung="Phase-1 Vertrag",
            fahrzeugklasse="SUV / Gelaendewagen",
            tagessatz="69,00",
            fin_nummer="WVWZZZTESTPHASE1001",
        )
        vorgang_id = portal.create_mietvorgang(
            fid,
            kunde_name="Vertrag Testkunde",
            kunde_telefon="0176 77777777",
            kunde_email="",
            whatsapp_erlaubt=True,
            start_datum=start.isoformat(),
            end_datum=ende.isoformat(),
        )
        felder_v1 = vollstaendige_vertragsfelder(mietpreis="69,00")
        save_v1 = {
            **vertragsentwurf_meta(vorgang_id),
            **felder_v1,
            "kunde_email": "",
            "whatsapp_erlaubt": "1",
        }
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/speichern",
            data=csrf_payload(client, save_v1),
            follow_redirects=True,
        )
        gespeichert = json.loads(portal.get_mietvorgang(vorgang_id)["vertrag_felder_json"])
        suite.check(
            "Vollstaendige Vertragsfelder werden fuer den Entwurf gespeichert",
            response.status_code == 200
            and set(gespeichert) == {feld["name"] for feld in portal.MIETVERTRAG_FELDER}
            and all(str(value).strip() for value in gespeichert.values()),
        )

        portal.MIETVERTRAG_RECHTLICH_FREIGEGEBEN = False
        portal.MIETVERTRAG_TEXT_VERSION = ""
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/unterschrift",
            data=csrf_payload(
                client,
                {
                    **vertragsentwurf_meta(vorgang_id),
                    "unterschrift_data": png_data_url(),
                    "unterschrift_name": "Vertrag Testkunde",
                    "unterschrift_ort": "Mosbach",
                },
            ),
            follow_redirects=True,
        )
        vorgang = portal.get_mietvorgang(vorgang_id)
        suite.check(
            "Rechtliche Freigabe aus: Signatur ist technisch gesperrt",
            response.status_code == 200
            and "Signieren ist gesperrt" in response.get_data(as_text=True)
            and vorgang["vertrag_status"] == "entwurf"
            and not vorgang["unterschrift_stored"]
            and not portal.list_mietvertrag_versionen(vorgang_id),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/senden",
            data=csrf_payload(client),
            follow_redirects=True,
        )
        vorgang = portal.get_mietvorgang(vorgang_id)
        suite.check(
            "Rechtliche Freigabe aus: Versand ist technisch gesperrt",
            response.status_code == 200
            and "Versand ist gesperrt" in response.get_data(as_text=True)
            and not vorgang["vertrag_gesendet_am"],
        )

        portal.MIETVERTRAG_RECHTLICH_FREIGEGEBEN = True
        portal.MIETVERTRAG_TEXT_VERSION = "phase1-test-2026-07-11"
        vorgang = portal.get_mietvorgang(vorgang_id)
        fahrzeug = portal.get_mietfahrzeug(fid)
        suite.check(
            "Freigabe plus Textversion und Pflichtfelder machen Vertrag signierbar",
            portal.mietvertrag_rechtlich_freigegeben()
            and portal.mietvertrag_signatur_fehler(vorgang, fahrzeug) == [],
            ", ".join(portal.mietvertrag_signatur_fehler(vorgang, fahrzeug)),
        )
        unplausible_felder = dict(felder_v1)
        unplausible_felder.update(
            {
                "geburtsdatum": "abc",
                "fuehrerschein_ausstellungsdatum": "morgen",
                "km_stand_uebergabe": "viele",
                "km_limit": "unbegrenzt",
                "kaution_euro": "abc",
                "selbstbeteiligung_euro": "--",
                "mehrkm_preis": "kostenlos?",
                "reinigungspauschale_euro": "unbekannt",
            }
        )
        unplausibler_vorgang = dict(vorgang)
        unplausibler_vorgang["vertrag_felder_json"] = json.dumps(unplausible_felder)
        plausibilitaetsfehler = portal.mietvertrag_signatur_fehler(
            unplausibler_vorgang, fahrzeug
        )
        suite.check(
            "Unplausible Datums-, Kilometer- und Euro-Werte blockieren die Signatur",
            any("Geburtsdatum" in f for f in plausibilitaetsfehler)
            and any("Ausstellungsdatum" in f for f in plausibilitaetsfehler)
            and any("Kilometerstand" in f for f in plausibilitaetsfehler)
            and any("Freikilometer" in f for f in plausibilitaetsfehler)
            and any("Kaution" in f for f in plausibilitaetsfehler)
            and any("Selbstbeteiligung" in f for f in plausibilitaetsfehler)
            and any("Mehrkilometer" in f for f in plausibilitaetsfehler)
            and any("Reinigungspauschale" in f for f in plausibilitaetsfehler),
            ", ".join(plausibilitaetsfehler),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/unterschrift",
            data=csrf_payload(
                client,
                {
                    **vertragsentwurf_meta(vorgang_id),
                    "unterschrift_data": leere_png_data_url(),
                    "unterschrift_name": "Vertrag Testkunde",
                    "unterschrift_ort": "Mosbach",
                },
            ),
            follow_redirects=True,
        )
        suite.check(
            "Leere oder transparente PNG-Datei gilt nicht als Unterschrift",
            portal.get_mietvorgang(vorgang_id)["vertrag_status"] == "entwurf"
            and not portal.list_mietvertrag_versionen(vorgang_id)
            and "sichtbare PNG-Unterschrift" in response.get_data(as_text=True),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/unterschrift",
            data=csrf_payload(
                client,
                {
                    **vertragsentwurf_meta(vorgang_id),
                    "unterschrift_data": png_data_url(),
                    "unterschrift_name": "Vertrag Testkunde",
                    "unterschrift_ort": "Mosbach",
                },
            ),
            follow_redirects=True,
        )
        vorgang_v1 = portal.get_mietvorgang(vorgang_id)
        version_1 = portal.get_mietvertrag_version(vorgang_id, 1)
        pdf_v1 = portal.mietvertrag_version_pdf_bytes(version_1)
        snapshot_v1 = portal.mietvertrag_version_snapshot(version_1)
        suite.check(
            "Signatur erzeugt Version 1 mit Snapshot-, Signatur- und PDF-Hashes",
            response.status_code == 200
            and vorgang_v1["vertrag_status"] == "unterschrieben"
            and int(vorgang_v1["vertrag_version"]) == 1
            and version_1 is not None
            and version_1["text_version"] == portal.MIETVERTRAG_TEXT_VERSION
            and version_1["quelle"] == "normal"
            and int(version_1["rechtlich_freigegeben"]) == 1
            and len(version_1["snapshot_sha256"]) == 64
            and len(version_1["unterschrift_sha256"]) == 64
            and len(version_1["pdf_sha256"]) == 64
            and portal.sha256_text(version_1["snapshot_json"]) == version_1["snapshot_sha256"]
            and portal.sha256_bytes(pdf_v1) == version_1["pdf_sha256"]
            and pdf_v1.startswith(b"%PDF")
            and snapshot_v1.get("text_version") == portal.MIETVERTRAG_TEXT_VERSION
            and not portal.mietvertrag_version_integritaetsfehler(version_1),
        )
        backup_path = portal.create_backup_package("phase1-vertragsblob-test")
        backup_roundtrip_ok = False
        backup_detail = ""
        try:
            with zipfile.ZipFile(backup_path, "r") as archive:
                names = set(archive.namelist())
                export = json.loads(archive.read("backup.json").decode("utf-8"))
                json_version = next(
                    row
                    for row in export["tables"]["mietvertrag_versionen"]
                    if int(row["mietvorgang_id"]) == vorgang_id
                    and int(row["version"]) == 1
                )
                refs = [
                    ref
                    for ref in export.get("binary_blobs", [])
                    if ref.get("table") == "mietvertrag_versionen"
                    and int(ref.get("row_id") or 0) == int(version_1["id"])
                ]
                with tempfile.TemporaryDirectory(
                    prefix="phase1_backup_restore_", dir=TMP_DIR
                ) as restore_dir:
                    imported_db, _uploads, _export = portal.extract_import_package_files(
                        archive, names, Path(restore_dir)
                    )
                    restored = sqlite3.connect(imported_db)
                    try:
                        restored_row = restored.execute(
                            """
                            SELECT pdf_base64, unterschrift_base64
                            FROM mietvertrag_versionen
                            WHERE mietvorgang_id=? AND version=1
                            """,
                            (vorgang_id,),
                        ).fetchone()
                    finally:
                        restored.close()
                backup_roundtrip_ok = (
                    int(export.get("format_version") or 0) == portal.BACKUP_FORMAT_VERSION
                    and json_version.get("pdf_base64") == ""
                    and json_version.get("unterschrift_base64") == ""
                    and len(refs) == 2
                    and all(ref.get("zip_path") in names for ref in refs)
                    and restored_row is not None
                    and restored_row[0] == version_1["pdf_base64"]
                    and restored_row[1] == version_1["unterschrift_base64"]
                )
        except Exception as exc:
            backup_detail = f"{type(exc).__name__}: {exc}"
        suite.check(
            "Backup externalisiert Vertragsdateien einmalig und stellt sie verlustfrei wieder her",
            backup_roundtrip_ok,
            backup_detail,
        )
        frozen_v1 = version_fingerprint(version_1)
        raw_felder_v1 = vorgang_v1["vertrag_felder_json"]
        v1_action_meta = vertragsversion_meta(vorgang_id)

        geaenderte_felder = dict(felder_v1)
        geaenderte_felder["mietpreis_tag"] = "999,99"
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/speichern",
            data=csrf_payload(client, dict(geaenderte_felder, kunde_email="neu@example.de")),
            follow_redirects=True,
        )
        nach_save = portal.get_mietvorgang(vorgang_id)
        suite.check(
            "Speichern nach Signatur ist blockiert und aendert keine Vertragsdaten",
            response.status_code == 200
            and "bearbeitung gesperrt" in response.get_data(as_text=True).lower()
            and nach_save["vertrag_status"] == "unterschrieben"
            and nach_save["vertrag_felder_json"] == raw_felder_v1
            and not nach_save["kunde_email"],
        )

        # Simuliert eine spaetere Aenderung der operativen Live-Zeile. Die
        # signierte Version muss weiterhin exakt dieselben PDF-Bytes liefern.
        db = portal.get_db()
        try:
            db.execute(
                "UPDATE mietvorgaenge SET vertrag_felder_json=? WHERE id=?",
                (json.dumps({"mietpreis_tag": "777,77"}), vorgang_id),
            )
            db.execute("UPDATE mietfahrzeuge SET tagessatz=777.77 WHERE id=?", (fid,))
            db.commit()
        finally:
            db.close()
        response = client.get(f"/admin/mietvorgang/{vorgang_id}/vertrag.pdf")
        suite.check(
            "Aenderung der Live-DB aendert das gespeicherte signierte PDF nicht",
            response.status_code == 200
            and response.get_data() == pdf_v1
            and version_fingerprint(portal.get_mietvertrag_version(vorgang_id, 1)) == frozen_v1,
        )

        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/neue-version",
            data=csrf_payload(
                client,
                {
                    **v1_action_meta,
                    "aenderungsgrund": "Preis nach Kundenwunsch korrigiert",
                },
            ),
            follow_redirects=True,
        )
        entwurf_v2 = portal.get_mietvorgang(vorgang_id)
        suite.check(
            "Neue Version erzeugt V2-Entwurf und bewahrt V1 unveraendert",
            response.status_code == 200
            and int(entwurf_v2["vertrag_version"]) == 2
            and entwurf_v2["vertrag_status"] == "entwurf"
            and version_fingerprint(portal.get_mietvertrag_version(vorgang_id, 1)) == frozen_v1
            and client.get(f"/admin/mietvorgang/{vorgang_id}/vertrag/version/1.pdf").get_data() == pdf_v1,
        )

        felder_v2 = vollstaendige_vertragsfelder(mietpreis="79,00")
        client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/speichern",
            data=csrf_payload(
                client,
                {
                    **vertragsentwurf_meta(vorgang_id),
                    **felder_v2,
                    "kunde_email": "",
                    "whatsapp_erlaubt": "1",
                },
            ),
            follow_redirects=True,
        )
        stale_entwurf_meta = vertragsentwurf_meta(vorgang_id)
        felder_v2_gespeichert = dict(felder_v2)
        felder_v2_gespeichert["mietpreis_tag"] = "80,00"
        client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/speichern",
            data=csrf_payload(
                client,
                {
                    **stale_entwurf_meta,
                    **felder_v2_gespeichert,
                    "kunde_email": "",
                    "whatsapp_erlaubt": "1",
                },
            ),
            follow_redirects=True,
        )
        stale_felder = dict(felder_v2)
        stale_felder["mietpreis_tag"] = "999,00"
        response_stale_entwurf = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/speichern",
            data=csrf_payload(
                client,
                {
                    **stale_entwurf_meta,
                    **stale_felder,
                    "kunde_email": "",
                    "whatsapp_erlaubt": "1",
                },
            ),
            follow_redirects=True,
        )
        gespeicherter_v2_entwurf = json.loads(
            portal.get_mietvorgang(vorgang_id)["vertrag_felder_json"]
        )
        suite.check(
            "Ein alter Entwurfs-Tab kann neuere Vertragsdaten nicht ueberschreiben",
            gespeicherter_v2_entwurf.get("mietpreis_tag") == "80,00"
            and "nicht mehr aktuell" in response_stale_entwurf.get_data(as_text=True),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/unterschrift",
            data=csrf_payload(
                client,
                {
                    **vertragsentwurf_meta(vorgang_id),
                    "unterschrift_data": png_data_url(),
                    "unterschrift_name": "Vertrag Testkunde",
                    "unterschrift_ort": "Mosbach",
                },
            ),
            follow_redirects=True,
        )
        version_2 = portal.get_mietvertrag_version(vorgang_id, 2)
        suite.check(
            "V2 kann separat signiert werden; V1-PDF und V1-Nachweis bleiben erhalten",
            response.status_code == 200
            and portal.get_mietvorgang(vorgang_id)["vertrag_status"] == "unterschrieben"
            and version_2 is not None
            and len(portal.list_mietvertrag_versionen(vorgang_id)) == 2
            and version_fingerprint(portal.get_mietvertrag_version(vorgang_id, 1)) == frozen_v1
            and portal.mietvertrag_version_pdf_bytes(portal.get_mietvertrag_version(vorgang_id, 1)) == pdf_v1,
        )

        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/bestaetigen",
            data=csrf_payload(client, v1_action_meta),
            follow_redirects=True,
        )
        suite.check(
            "Ein alter Versions-Tab kann eine neuere Vertragsversion nicht bestaetigen",
            portal.get_mietvorgang(vorgang_id)["vertrag_status"] == "unterschrieben"
            and "nicht mehr aktuell" in response.get_data(as_text=True),
        )

        original_v2_snapshot = version_2["snapshot_json"]
        original_v2_snapshot_hash = version_2["snapshot_sha256"]
        db = portal.get_db()
        try:
            db.execute(
                """
                UPDATE mietvertrag_versionen
                SET snapshot_json=?, snapshot_sha256=?
                WHERE mietvorgang_id=? AND version=2
                """,
                (version_1["snapshot_json"], version_1["snapshot_sha256"], vorgang_id),
            )
            db.commit()
        finally:
            db.close()
        vertauschte_version = portal.get_mietvertrag_version(vorgang_id, 2)
        suite.check(
            "Intakter Snapshot einer anderen Version wird nicht falsch zugeordnet",
            "nicht zur selben Vertragsfassung"
            in portal.mietvertrag_version_integritaetsfehler(vertauschte_version),
        )
        db = portal.get_db()
        try:
            db.execute(
                """
                UPDATE mietvertrag_versionen
                SET snapshot_json=?, snapshot_sha256=?
                WHERE mietvorgang_id=? AND version=2
                """,
                (original_v2_snapshot, original_v2_snapshot_hash, vorgang_id),
            )
            db.commit()
        finally:
            db.close()

        # Beschadigter PDF-Hash: Bestaetigung muss verweigert werden.
        db_execute(
            "UPDATE mietvertrag_versionen SET pdf_sha256=? WHERE mietvorgang_id=? AND version=2",
            ("0" * 64, vorgang_id),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/bestaetigen",
            data=csrf_payload(client, vertragsversion_meta(vorgang_id)),
            follow_redirects=True,
        )
        suite.check(
            "Bestaetigung verweigert eine Vertragsversion mit Integritaetsfehler",
            response.status_code == 200
            and portal.get_mietvorgang(vorgang_id)["vertrag_status"] == "unterschrieben"
            and "beschädigt" in response.get_data(as_text=True).lower(),
        )

        db_execute(
            "UPDATE mietvertrag_versionen SET pdf_sha256=? WHERE mietvorgang_id=? AND version=2",
            (version_2["pdf_sha256"], vorgang_id),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/bestaetigen",
            data=csrf_payload(client, vertragsversion_meta(vorgang_id)),
            follow_redirects=True,
        )
        bestaetigt = portal.get_mietvorgang(vorgang_id)
        version_2_bestaetigt = portal.get_mietvertrag_version(vorgang_id, 2)
        suite.check(
            "Bestaetigung akzeptiert nur die wieder vollstaendig integre Version",
            response.status_code == 200
            and bestaetigt["vertrag_status"] == "bestaetigt"
            and bool(bestaetigt["vertrag_bestaetigt_am"])
            and version_2_bestaetigt["status"] == "bestaetigt"
            and not portal.mietvertrag_version_integritaetsfehler(version_2_bestaetigt),
        )

        # Nach Bestaetigung separater Versandtest: manipulierter Snapshot muss
        # vor der Erzeugung eines Versandnachweises stoppen.
        snapshot_raw_v2 = version_2_bestaetigt["snapshot_json"]
        db_execute(
            "UPDATE mietvertrag_versionen SET snapshot_json=? WHERE mietvorgang_id=? AND version=2",
            (snapshot_raw_v2 + "MANIPULIERT", vorgang_id),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/senden",
            data=csrf_payload(client, vertragsversand_meta(vorgang_id)),
            follow_redirects=True,
        )
        nach_blockiertem_versand = portal.get_mietvorgang(vorgang_id)
        suite.check(
            "Versand verweigert eine bestaetigte, aber manipulierte Version",
            response.status_code == 200
            and not nach_blockiertem_versand["vertrag_gesendet_am"]
            and not nach_blockiertem_versand["vertrag_versandweg"]
            and "beschädigt" in response.get_data(as_text=True).lower(),
        )

        db_execute(
            "UPDATE mietvertrag_versionen SET snapshot_json=? WHERE mietvorgang_id=? AND version=2",
            (snapshot_raw_v2, vorgang_id),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/senden",
            data=csrf_payload(client, vertragsversand_meta(vorgang_id)),
            follow_redirects=False,
        )
        nur_vorbereitet = portal.get_mietvorgang(vorgang_id)
        version_nur_vorbereitet = portal.get_mietvertrag_version(vorgang_id, 2)
        suite.check(
            "WhatsApp-Vorbereitung wird nicht faelschlich als Versand protokolliert",
            response.status_code == 302
            and response.headers.get("Location", "").startswith("https://wa.me/")
            and not nur_vorbereitet["vertrag_gesendet_am"]
            and not version_nur_vorbereitet["versendet_am"],
        )

        stale_versand_meta = vertragsversand_meta(vorgang_id)
        db_execute(
            "UPDATE mietvorgaenge SET kunde_email=? WHERE id=?",
            ("vertrag@example.test", vorgang_id),
        )
        mail_aufrufe = []

        def fake_mail(*args, **kwargs):
            mail_aufrufe.append((args, kwargs))
            return {"sent": True}

        portal.send_mietvertrag_mail = fake_mail
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/senden",
            data=csrf_payload(client, stale_versand_meta),
            follow_redirects=True,
        )
        suite.check(
            "Geaendertes Versandziel verlangt erneute sichtbare Pruefung",
            not mail_aufrufe
            and not portal.get_mietvorgang(vorgang_id)["vertrag_gesendet_am"]
            and "Empfänger hat sich geändert" in response.get_data(as_text=True),
        )
        response = client.post(
            f"/admin/mietvorgang/{vorgang_id}/vertrag/senden",
            data=csrf_payload(client, vertragsversand_meta(vorgang_id)),
            follow_redirects=True,
        )
        versendet = portal.get_mietvorgang(vorgang_id)
        versendete_version = portal.get_mietvertrag_version(vorgang_id, 2)
        suite.check(
            "Tatsaechlicher E-Mail-Versand wird an der exakten Version protokolliert",
            response.status_code == 200
            and bool(versendet["vertrag_gesendet_am"])
            and versendet["vertrag_versandweg"] == "email"
            and versendete_version["versandweg"] == "email"
            and versendete_version["versandziel"] == "vertrag@example.test"
            and bool(versendete_version["versendet_am"])
            and len(mail_aufrufe) == 1
            and not portal.mietvertrag_version_integritaetsfehler(versendete_version),
        )
        aktuelle_meta = vertragsversion_meta(vorgang_id)
        portal.neue_mietvertrag_version(
            vorgang_id,
            "Versandhistorie erhalten testen",
            aktuelle_meta["expected_version"],
            aktuelle_meta["expected_snapshot_hash"],
        )
        version_nach_versionswechsel = portal.get_mietvertrag_version(vorgang_id, 2)
        vorgang_nach_versionswechsel = portal.get_mietvorgang(vorgang_id)
        suite.check(
            "Versionswechsel bewahrt den Versandnachweis der vorherigen Fassung",
            version_nach_versionswechsel["versandweg"] == "email"
            and version_nach_versionswechsel["versandziel"] == "vertrag@example.test"
            and bool(version_nach_versionswechsel["versendet_am"])
            and int(vorgang_nach_versionswechsel["vertrag_version"]) == 3
            and not vorgang_nach_versionswechsel["vertrag_gesendet_am"],
        )
        original_v1_pdf_hash = portal.get_mietvertrag_version(vorgang_id, 1)["pdf_sha256"]
        db_execute(
            "UPDATE mietvertrag_versionen SET pdf_sha256=? WHERE mietvorgang_id=? AND version=1",
            ("0" * 64, vorgang_id),
        )
        historie_seite = client.get(
            f"/admin/mietvorgang/{vorgang_id}/vertrag"
        ).get_data(as_text=True)
        suite.check(
            "Auch eine beschaedigte historische Version wird sichtbar rot markiert",
            "Integritätsprüfung fehlgeschlagen" in historie_seite
            and "Fixierte PDF-Fassung mit gültiger Prüfsumme" in historie_seite,
        )
        db_execute(
            "UPDATE mietvertrag_versionen SET pdf_sha256=? WHERE mietvorgang_id=? AND version=1",
            (original_v1_pdf_hash, vorgang_id),
        )
    finally:
        portal.MIETVERTRAG_RECHTLICH_FREIGEGEBEN = original_freigabe
        portal.MIETVERTRAG_TEXT_VERSION = original_textversion
        portal.send_mietvertrag_mail = original_mailversand


def main() -> int:
    portal.app.config.update(
        TESTING=True,
        SESSION_COOKIE_SECURE=False,
    )
    portal.init_db()
    with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
        portal.PUBLIC_FORM_ATTEMPTS.clear()
    with portal.LOGIN_ATTEMPTS_LOCK:
        portal.LOGIN_ATTEMPTS.clear()

    suite = Suite()
    try:
        suite.run_section("Eingabevalidierung", lambda: test_eingaben(suite))
        suite.run_section("Doppelbuchung und Transaktion", lambda: test_doppelbuchung(suite))
        suite.run_section(
            "Anfragen, Datenschutz und Backup",
            lambda: test_anfrage_backup_und_datenschutz(suite),
        )
        suite.run_section("Storno und Fahrzeugarchiv", lambda: test_storno_und_archiv(suite))
        suite.run_section("Exakter Partner-Zugangscode", lambda: test_partner_code(suite))
        suite.run_section("Rechtliche Freigabe und Vertragsintegritaet", lambda: test_vertragsintegritaet(suite))
        return suite.exit_code()
    finally:
        shutil.rmtree(TMP_DIR, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
