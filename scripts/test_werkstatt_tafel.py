import atexit
import io
import os
from pathlib import Path
import shutil
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="kundenstatus-werkstatt-tafel-test-"))
os.environ["DATABASE_URL"] = ""
os.environ["RENDER"] = "1"
os.environ["REQUIRE_POSTGRES_ON_RENDER"] = "0"
os.environ["SQLITE_DB_PATH"] = str(TEST_ROOT / "auftraege.db")
os.environ["UPLOAD_DIR"] = str(TEST_ROOT / "uploads")
atexit.register(shutil.rmtree, TEST_ROOT, ignore_errors=True)

import app as portal  # noqa: E402


FEHLER = []


def check(label, ok, detail=""):
    if ok:
        print(f"[OK] {label}")
    else:
        print(f"[FEHLER] {label} {detail}")
        FEHLER.append(label)


def csrf_data(client, data=None):
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

    # Ohne Login: Tafel und Detail leiten zur Anmeldung um
    response = client.get("/werkstatt/tafel")
    check("Tafel ohne Login leitet zum Login um", response.status_code == 302 and "/werkstatt" in response.headers.get("Location", ""))
    response = client.get("/werkstatt/auftrag/1")
    check("Auftrags-Detail ohne Login leitet zum Login um", response.status_code == 302)
    response = client.get("/werkstatt/auftrag-anlegen")
    check("Werkstatt-Anlage ohne Login leitet zum Login um", response.status_code == 302)

    # Login-Seite laedt (und setzt CSRF-Token in die Session)
    response = client.get("/werkstatt")
    check("Login-Seite laedt", response.status_code == 200 and "Werkstatt-Tafel" in response.get_data(as_text=True))

    # Code setzen wie ueber Admin -> Zugaenge
    portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "TEST99")

    # Falscher Code wird abgelehnt
    response = client.post("/werkstatt", data=csrf_data(client, {"password": "FALSCH"}))
    check("Falscher Code abgelehnt", response.status_code == 200 and "Falscher Werkstatt-Code" in response.get_data(as_text=True))

    # Richtiger Code meldet an
    response = client.post("/werkstatt", data=csrf_data(client, {"password": "TEST99"}))
    check("Richtiger Code leitet zur Tafel", response.status_code == 302 and "/werkstatt/tafel" in response.headers.get("Location", ""))

    response = client.get("/werkstatt/tafel")
    html = response.get_data(as_text=True)
    check("Tafel laedt mit Spalten", response.status_code == 200 and "In Arbeit" in html and "Geplant" in html)
    check(
        "Tafel-Kopf verlinkt PPG-Messungen als Uebergang",
        "🎨 PPG-Messungen" in html
        and 'href="https://emea.ppglinq.com/rapid-match"' in html
        and 'target="_blank"' in html
        and 'rel="noopener noreferrer"' in html
        and "data-messungen-dialog" not in html,
    )
    check(
        "Tafel hat Button zum Auftrag-Anlegen",
        "＋ Anlegen" in html and 'data-auftrag-anlegen' in html and "/werkstatt/auftrag-anlegen" in html,
    )

    response = client.get("/werkstatt/auftrag-anlegen")
    anlegen_html = response.get_data(as_text=True)
    check(
        "Werkstatt-Anlage zeigt Kunden-, FIN-, Foto- und Preisfelder",
        response.status_code == 200
        and 'name="kunde_name"' in anlegen_html
        and 'name="kontakt_telefon"' in anlegen_html
        and 'name="fin_nummer"' in anlegen_html
        and 'name="fotos"' in anlegen_html
        and 'capture="environment"' in anlegen_html
        and 'name="werkstatt_preisvorschlag"' in anlegen_html
        and "Nur das Büro" in anlegen_html,
    )

    response = client.post(
        "/werkstatt/auftrag-anlegen",
        data=csrf_data(
            client,
            {
                "kunde_name": "Werkstatt Testkunde",
                "kontakt_telefon": "0176 12345678",
                "fahrzeug": "Volkswagen Golf",
                "kennzeichen": "mos wt 42",
                "fin_nummer": "WVWZZZ1JZXW000001",
                "beschreibung": "Stoßfänger vorne prüfen",
                "werkstatt_preisvorschlag": "599,00",
                "fotos": (
                    io.BytesIO(b"\x89PNG\r\n\x1a\nWerkstatt-Aufnahme"),
                    "werkstatt-aufnahme.png",
                ),
            },
        ),
        content_type="multipart/form-data",
    )
    db = portal.get_db()
    werkstatt_row = db.execute(
        "SELECT * FROM auftraege WHERE quelle='werkstatt' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    werkstatt_neu_id = int(werkstatt_row["id"]) if werkstatt_row else 0
    werkstatt_fotos = db.execute(
        "SELECT * FROM dateien WHERE auftrag_id=? ORDER BY id",
        (werkstatt_neu_id,),
    ).fetchall()
    db.close()
    check(
        "Werkstatt kann Auftrag mit Foto und Preisvorschlag anlegen",
        response.status_code == 302
        and werkstatt_row is not None
        and int(werkstatt_row["status"]) == 1
        and int(werkstatt_row["werkstatt_neu"]) == 1
        and werkstatt_row["kunde_name"] == "Werkstatt Testkunde"
        and werkstatt_row["kontakt_telefon"] == "0176 12345678"
        and werkstatt_row["fin_nummer"] == "WVWZZZ1JZXW000001"
        and werkstatt_row["kennzeichen"] == "MOS WT 42"
        and werkstatt_row["werkstatt_preisvorschlag"] == "599,00 € netto"
        and len(werkstatt_fotos) == 1
        and werkstatt_fotos[0]["quelle"] == "werkstatt"
        and werkstatt_fotos[0]["kategorie"] == "standard",
    )
    response = client.post(
        f"/werkstatt/auftrag/{werkstatt_neu_id}/status/3",
        data=csrf_data(client),
    )
    check(
        "Werkstatt-Annahme bleibt bis zur Büroprüfung gesperrt",
        response.status_code == 400 and portal.get_auftrag(werkstatt_neu_id)["status"] == 1,
    )

    # Eingeloggt: Login-Seite leitet direkt zur Tafel weiter
    response = client.get("/werkstatt")
    check("Login-Seite leitet Angemeldete zur Tafel", response.status_code == 302)

    # Auftrags-Detail eines echten Auftrags
    auftraege = [a for a in portal.list_auftraege() if int(a.get("status") or 1) <= 4]
    if auftraege:
        auftrag = auftraege[0]
        auftrag_id = auftrag["id"]

        response = client.get(f"/werkstatt/auftrag/{auftrag_id}")
        html = response.get_data(as_text=True)
        check(
            f"Auftrags-Detail laedt (#{auftrag_id})",
            response.status_code == 200 and "Was zu machen ist" in html and "Zur Tafel" in html,
        )
        # Preis-FELDER der App (Bonus, Lexware, Angebotspreis) bleiben draussen;
        # Kalkulations-/KVA-PDFs sind als Unterlage bewusst sichtbar (GF 11.06.2026).
        check("Detail zeigt keine Preisfelder der App", "bonus" not in html.lower() and "lexware" not in html.lower())
        check("Detail zeigt Verschieben-Buttons", "Auftrag verschieben" in html)
        check("Detail zeigt Unterlagen-Bereich", "Auftragsunterlagen" in html)

        # Tafel zeigt Schnell-Knopf und Drop-Ziele
        response = client.get("/werkstatt/tafel")
        tafel_html = response.get_data(as_text=True)
        check("Tafel hat Drop-Ziele", 'data-ziel-status="3"' in tafel_html)
        check("Tafel hat Schnell-Knoepfe", "data-status-knopf" in tafel_html)

        # Startzustand deterministisch setzen (Wiederholungslaeufe auf derselben DB:
        # der Idempotenz-Check wuerde einen POST auf den bereits aktiven Status zum No-Op machen)
        db = portal.get_db()
        db.execute("UPDATE auftraege SET status=2 WHERE id=?", (auftrag_id,))
        db.commit()
        db.close()

        # Statuswechsel: Formular-Weg (wie Detail-Seite) auf "In Arbeit"
        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/status/3",
            data=csrf_data(client, {"next": f"/werkstatt/auftrag/{auftrag_id}"}),
        )
        nach_wechsel = portal.get_auftrag(auftrag_id)
        log = portal.get_status_log(auftrag_id)
        check(
            "Verschieben auf In Arbeit (Formular)",
            response.status_code == 302
            and nach_wechsel["status"] == 3
            and bool(nach_wechsel["start_datum"])
            and log and int(log[-1]["status"]) == 3,
        )

        # Statuswechsel: fetch-Weg (wie Tafel-Knopf/Drag&Drop) auf "Fertig"
        with client.session_transaction() as session:
            csrf = session.get("csrf_token")
        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/status/4",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        nach_wechsel = portal.get_auftrag(auftrag_id)
        check(
            "Verschieben auf Fertig (fetch/Drag&Drop)",
            response.status_code == 200
            and response.is_json
            and response.get_json().get("ok") is True
            and nach_wechsel["status"] == 4
            and bool(nach_wechsel["fertig_datum"]),
        )

        # Rueckwaerts auf Eingeplant ist erlaubt
        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/status/2",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check("Zurueck auf Eingeplant erlaubt", response.status_code == 200 and portal.get_auftrag(auftrag_id)["status"] == 2)

        # Nicht erlaubte Zielstatus werden abgelehnt
        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/status/5",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check("Zurueckgegeben (5) fuer Werkstatt gesperrt", response.status_code == 400)
        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/status/1",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check("Angelegt (1) fuer Werkstatt gesperrt", response.status_code == 400)

        # PDF-Unterlage anlegen und ueber die Werkstatt-Route oeffnen
        import uuid as _uuid

        stored = f"test_werkstatt_{_uuid.uuid4().hex}.pdf"
        portal.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        (portal.UPLOAD_DIR / stored).write_bytes(b"%PDF-1.4\n%Test Werkstatt-Tafel\n")
        db = portal.get_db()
        db.execute(
            "INSERT INTO dateien (auftrag_id, original_name, stored_name, mime_type, size, hochgeladen_am)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (auftrag_id, "Reparaturauftrag_Test.pdf", stored, "application/pdf", 30, portal.now_str()),
        )
        db.commit()
        datei_row = db.execute("SELECT id FROM dateien WHERE stored_name=?", (stored,)).fetchone()
        db.close()
        response = client.get(f"/werkstatt/auftrag/{auftrag_id}")
        check("Unterlagen-Liste zeigt PDF", "Reparaturauftrag_Test.pdf" in response.get_data(as_text=True))
        response = client.get(f"/werkstatt/datei/{datei_row['id']}")
        check(
            "PDF laesst sich ueber Werkstatt-Route oeffnen",
            response.status_code == 200 and response.data.startswith(b"%PDF"),
        )

        # Rechnungen duerfen NICHT auf den Hallen-Bildschirm (weder Liste noch Direktzugriff)
        stored_re = f"test_rechnung_{_uuid.uuid4().hex}.pdf"
        (portal.UPLOAD_DIR / stored_re).write_bytes(b"%PDF-1.4\n%Rechnung Test\n")
        db = portal.get_db()
        db.execute(
            "INSERT INTO dateien (auftrag_id, original_name, stored_name, mime_type, size, dokument_typ, hochgeladen_am)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (auftrag_id, "Rechnung_RE0299_Test.pdf", stored_re, "application/pdf", 30, "Rechnung", portal.now_str()),
        )
        db.commit()
        re_row = db.execute("SELECT id FROM dateien WHERE stored_name=?", (stored_re,)).fetchone()
        db.close()
        response = client.get(f"/werkstatt/auftrag/{auftrag_id}")
        check("Rechnung erscheint nicht in Unterlagen", "Rechnung_RE0299_Test.pdf" not in response.get_data(as_text=True))
        response = client.get(f"/werkstatt/datei/{re_row['id']}")
        check("Rechnung auch per Direktzugriff gesperrt", response.status_code == 404)

        # KVA/DAT-Kalkulation bleibt sichtbar — Mitarbeiter sollen den Auftrag lesen koennen
        stored_kva = f"test_kva_{_uuid.uuid4().hex}.pdf"
        (portal.UPLOAD_DIR / stored_kva).write_bytes(b"%PDF-1.4\n%KVA Test\n")
        db = portal.get_db()
        db.execute(
            "INSERT INTO dateien (auftrag_id, original_name, stored_name, mime_type, size, dokument_typ, hochgeladen_am)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (auftrag_id, "KVA_DAT_Test.pdf", stored_kva, "application/pdf", 30, "DAT-Kalkulation", portal.now_str()),
        )
        db.commit()
        kva_row = db.execute("SELECT id FROM dateien WHERE stored_name=?", (stored_kva,)).fetchone()
        db.close()
        response = client.get(f"/werkstatt/auftrag/{auftrag_id}")
        check("KVA/Kalkulation bleibt als Unterlage sichtbar", "KVA_DAT_Test.pdf" in response.get_data(as_text=True))
        response = client.get(f"/werkstatt/datei/{kva_row['id']}")
        check("KVA laesst sich oeffnen", response.status_code == 200)

        # Zurueckgegebene Auftraege (Status 5) sind fuer die Werkstatt gesperrt
        db = portal.get_db()
        db.execute("UPDATE auftraege SET status=5 WHERE id=?", (auftrag_id,))
        db.commit()
        db.close()
        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/status/3",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check(
            "Zurueckgegebener Auftrag nicht reaktivierbar",
            response.status_code == 409 and portal.get_auftrag(auftrag_id)["status"] == 5,
        )

        # Gleicher Status nochmal: keine doppelte Mail/Benachrichtigung/Log-Zeile
        db = portal.get_db()
        db.execute("UPDATE auftraege SET status=3 WHERE id=?", (auftrag_id,))
        db.commit()
        db.close()
        log_vorher = len(portal.get_status_log(auftrag_id))
        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/status/3",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check(
            "Gleicher Status loest keine doppelte Aktion aus",
            response.status_code == 200
            and response.get_json().get("unveraendert") is True
            and len(portal.get_status_log(auftrag_id)) == log_vorher,
        )

        # --- Produktionsschritte (Vorarbeit -> Karosserie -> Lackierung -> Finish) ---
        db = portal.get_db()
        db.execute("UPDATE auftraege SET status=3, produktion_schritt='' WHERE id=?", (auftrag_id,))
        db.commit()
        db.close()
        check("Spalte produktion_schritt existiert", "produktion_schritt" in portal.get_auftrag(auftrag_id))

        tafel_html = client.get("/werkstatt/tafel").get_data(as_text=True)
        check(
            "Tafel zeigt Produktions-Stepper",
            "data-prod-knopf" in tafel_html and "Vorarbeit" in tafel_html and "Lackierung" in tafel_html,
        )

        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/produktion/lackierung",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check(
            "Produktionsschritt auf Lackierung gesetzt",
            response.status_code == 200
            and response.get_json().get("produktion_schritt") == "lackierung"
            and portal.get_auftrag(auftrag_id)["produktion_schritt"] == "lackierung",
        )

        steps = portal.produktion_schritt_steps(portal.get_auftrag(auftrag_id))
        zustand = {s["key"]: s["state"] for s in steps}
        check(
            "Stepper-Zustaende stimmen (vor erledigt, lack aktiv, finish offen)",
            zustand.get("vorarbeit") == "erledigt"
            and zustand.get("karosserie") == "erledigt"
            and zustand.get("lackierung") == "aktiv"
            and zustand.get("finish") == "offen",
        )

        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/produktion/lackierung",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check(
            "Toggle nimmt aktuellen Schritt zurueck",
            response.status_code == 200 and portal.get_auftrag(auftrag_id)["produktion_schritt"] == "",
        )

        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/produktion/quatsch",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check("Ungueltiger Schritt abgelehnt (400)", response.status_code == 400)

        # Schlanke Kundenansicht: Status 3 + Lackierung -> "In der Lackierung" aktiv
        db = portal.get_db()
        db.execute("UPDATE auftraege SET status=3, produktion_schritt='lackierung' WHERE id=?", (auftrag_id,))
        db.commit()
        db.close()
        kunde_steps = portal.kunden_status_timeline_kurz(portal.get_auftrag(auftrag_id))
        labels = [s["label"] for s in kunde_steps]
        aktiv = [s["label"] for s in kunde_steps if s["state"] == "active"]
        check(
            "Kunde sieht schlanke Werkstatt-Schritte",
            "In Vorbereitung" in labels and "In der Lackierung" in labels and "Endkontrolle & Politur" in labels,
        )
        check("Kunde: aktive Stufe = In der Lackierung", aktiv == ["In der Lackierung"])

        # Recycling: faellt der Auftrag unter "In Arbeit" zurueck, darf kein veralteter Schritt bleiben
        db = portal.get_db()
        db.execute("UPDATE auftraege SET status=3, produktion_schritt='finish' WHERE id=?", (auftrag_id,))
        db.commit()
        db.close()
        response = client.post(
            f"/werkstatt/auftrag/{auftrag_id}/status/2",
            headers={"X-CSRF-Token": csrf, "X-Requested-With": "fetch"},
        )
        check(
            "Zurueck auf Eingeplant setzt Produktionsschritt zurueck",
            response.status_code == 200 and portal.get_auftrag(auftrag_id)["produktion_schritt"] == "",
        )

        kein_login = portal.app.test_client()
        response = kein_login.post(f"/werkstatt/auftrag/{auftrag_id}/produktion/vorarbeit")
        check("Produktionsschritt ohne Login abgewiesen", response.status_code in (302, 400))
    else:
        print("[HINWEIS] Keine offenen Auftraege in der Test-DB — Detail-/Status-Tests uebersprungen")

    response = client.get("/werkstatt/auftrag/999999")
    check("Unbekannter Auftrag liefert 404", response.status_code == 404)

    # Statuswechsel ohne Login wird abgewiesen
    fremd_client = portal.app.test_client()
    response = fremd_client.post("/werkstatt/auftrag/1/status/3")
    check("Statuswechsel ohne Login abgewiesen", response.status_code in (302, 400))

    # Code-Rotation wirft angemeldete Bildschirme raus
    portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "NEU777")
    response = client.get("/werkstatt/tafel")
    check("Code-Aenderung meldet alte Sitzungen ab", response.status_code == 302)

    # Admin sieht Tafel ohne Werkstatt-Code
    admin_client = portal.app.test_client()
    with admin_client.session_transaction() as session:
        session["admin"] = True
    response = admin_client.get("/werkstatt/tafel")
    check("Admin sieht Tafel ohne Extra-Login", response.status_code == 200)
    response = admin_client.get(f"/admin/auftrag/{werkstatt_neu_id}")
    admin_html = response.get_data(as_text=True)
    check(
        "Büro sieht offenen Prüfstatus und Mitarbeiter-Preisvorschlag",
        response.status_code == 200
        and "Büro-Prüfung offen" in admin_html
        and "Noch nicht freigegeben" in admin_html
        and "599,00 € netto" in admin_html,
    )

    response = admin_client.get("/admin/zugaenge")
    html = response.get_data(as_text=True)
    check("Zugaenge-Seite zeigt Werkstatt-Block", response.status_code == 200 and "Werkstatt-Tafel" in html and "NEU777" in html)

    # Admin: Code aendern ueber das Formular
    admin_client.get("/admin/zugaenge")
    response = admin_client.post("/admin/werkstatt-tafel/zugang", data=csrf_data(admin_client, {"zugangscode": "halle1"}))
    check("Admin-Code-Aenderung leitet zurueck", response.status_code == 302)
    check("Code wird gross geschrieben gespeichert", portal.get_werkstatt_tafel_code() == "HALLE1")

    # Aufraeumen: frischen Zufallscode hinterlassen
    portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, portal.generate_werkstatt_tafel_code())

    if FEHLER:
        print(f"\n{len(FEHLER)} Test(s) fehlgeschlagen.")
        sys.exit(1)
    print("\nWerkstatt-Tafel-Test erfolgreich.")


if __name__ == "__main__":
    main()
