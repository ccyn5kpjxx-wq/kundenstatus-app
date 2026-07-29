import atexit
from io import BytesIO
import os
from pathlib import Path
import shutil
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="kundenstatus-admin-neu-analyse-test-"))
os.environ["DATABASE_URL"] = ""
os.environ["RENDER"] = "1"
os.environ["REQUIRE_POSTGRES_ON_RENDER"] = "0"
os.environ["SQLITE_DB_PATH"] = str(TEST_ROOT / "auftraege.db")
os.environ["UPLOAD_DIR"] = str(TEST_ROOT / "uploads")
atexit.register(shutil.rmtree, TEST_ROOT, ignore_errors=True)

import app as portal  # noqa: E402


FEHLER = []
PDF_BYTES = b"%PDF-1.4\n% Interne Formularanalyse\n"
PDF_NAME = "interner-lackierauftrag.pdf"


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


def auftrag_count():
    db = portal.get_db()
    row = db.execute("SELECT COUNT(*) AS anzahl FROM auftraege").fetchone()
    db.close()
    return int(row["anzahl"] or 0)


def fake_analysis(files):
    return {
        "files_analyzed": len(files),
        "fields": {
            "fahrzeug": "Audi A4",
            "kennzeichen": "MOS GA 42",
            "fin_nummer": "WAUZZZ8K9DA000001",
            "beschreibung": "Stoßfänger hinten instandsetzen",
        },
        "review_hint": "Automatisch erkannte Werte bitte prüfen.",
        "needs_review": True,
        "confidence": 0.83,
        "sources": ["test"],
        "file_names": [file.filename for file in files],
        "file_hashes": [portal.partner_new_file_hash(file) for file in files],
        "errors": [],
    }


def main():
    portal.app.config["TESTING"] = True
    portal.init_db()
    client = portal.app.test_client()
    with client.session_transaction() as session:
        session["admin"] = True

    response = client.get("/admin/neu")
    html = response.get_data(as_text=True)
    check(
        "Admin-Formular erklärt die reine Analyse vor dem Speichern",
        response.status_code == 200
        and "Dabei wird noch kein Auftrag angelegt" in html
        and "Datei analysieren &amp; Felder eintragen" in html
        and "Auftrag jetzt anlegen" in html
        and "/admin/neu/analysieren" in html,
    )

    original_analysis = portal.analyze_partner_new_files
    portal.analyze_partner_new_files = fake_analysis
    try:
        before = auftrag_count()
        response = client.post(
            "/admin/neu/analysieren",
            data=csrf_data(
                client,
                {"dateien": (BytesIO(PDF_BYTES), PDF_NAME)},
            ),
            content_type="multipart/form-data",
        )
        payload = response.get_json() or {}
        check(
            "Analyse-Endpunkt füllt den Entwurf ohne Auftragserzeugung",
            response.status_code == 200
            and payload.get("ok") is True
            and payload.get("fields", {}).get("fahrzeug") == "Audi A4"
            and bool(payload.get("analysis_token"))
            and auftrag_count() == before,
        )

        response = client.post(
            "/admin/neu",
            data=csrf_data(
                client,
                {
                    "aktion": "upload_analyze",
                    "kunde_name": "Entwurf bleibt erhalten",
                    "dateien": (BytesIO(PDF_BYTES), PDF_NAME),
                },
            ),
            content_type="multipart/form-data",
        )
        fallback_html = response.get_data(as_text=True)
        check(
            "Server-Fallback analysiert ebenfalls ohne Auftrag",
            response.status_code == 200
            and "Die Datei wurde nur analysiert" in fallback_html
            and 'value="Audi A4"' in fallback_html
            and 'value="Entwurf bleibt erhalten"' in fallback_html
            and auftrag_count() == before,
        )

        response = client.post(
            "/admin/neu",
            data=csrf_data(
                client,
                {
                    "aktion": "speichern",
                    "analyse_abgeschlossen": "1",
                    "analyse_datei_erforderlich": "1",
                    "analyse_dateisignatur": PDF_NAME,
                    "analyse_token": payload.get("analysis_token", ""),
                    "fahrzeug": "Audi A4",
                },
            ),
        )
        check(
            "Analysierte Originaldatei muss beim Abschluss mitgespeichert werden",
            response.status_code == 200
            and "zuvor analysierte Datei erneut auswählen" in response.get_data(as_text=True)
            and auftrag_count() == before,
        )

        response = client.post(
            "/admin/neu",
            data=csrf_data(
                client,
                {
                    "aktion": "speichern",
                    "fahrzeug": "Audi A4",
                    "dateien": (BytesIO(PDF_BYTES), PDF_NAME),
                },
            ),
            content_type="multipart/form-data",
        )
        check(
            "Datei kann nicht ohne vorherige Analyse gespeichert werden",
            response.status_code == 200
            and "Bitte die ausgewählte Datei zuerst analysieren" in response.get_data(as_text=True)
            and auftrag_count() == before,
        )

        response = client.post(
            "/admin/neu",
            data=csrf_data(
                client,
                {
                    "aktion": "speichern",
                    "analyse_abgeschlossen": "1",
                    "analyse_datei_erforderlich": "1",
                    "analyse_dateisignatur": PDF_NAME,
                    "analyse_token": payload.get("analysis_token", ""),
                    "kunde_name": "Interner Testkunde",
                    "fahrzeug": "Audi A4 geprüft",
                    "kennzeichen": "mos ga 42",
                    "fin_nummer": "WAUZZZ8K9DA000001",
                    "beschreibung": "Vom Büro geprüft",
                    "transport_art": "standard",
                    "dateien": (BytesIO(PDF_BYTES), PDF_NAME),
                },
            ),
            content_type="multipart/form-data",
        )
        db = portal.get_db()
        row = db.execute(
            "SELECT * FROM auftraege WHERE quelle='intern' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        file_row = db.execute(
            "SELECT * FROM dateien WHERE auftrag_id=? ORDER BY id DESC LIMIT 1",
            (int(row["id"]) if row else 0,),
        ).fetchone()
        db.close()
        check(
            "Erst die geprüfte Abschlussaktion legt Auftrag und Originaldatei an",
            response.status_code == 302
            and row is not None
            and auftrag_count() == before + 1
            and row["fahrzeug"] == "Audi A4 geprüft"
            and row["kennzeichen"] == "MOS GA 42"
            and file_row is not None
            and file_row["original_name"] == PDF_NAME
            and not file_row["analyse_quelle"],
        )
    finally:
        portal.analyze_partner_new_files = original_analysis

    if FEHLER:
        print(f"\n{len(FEHLER)} Test(s) fehlgeschlagen.")
        sys.exit(1)
    print("\nAdmin-Neuanlage-Analyse-Test erfolgreich.")


if __name__ == "__main__":
    main()
