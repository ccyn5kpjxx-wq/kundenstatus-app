from io import BytesIO
from pathlib import Path
import shutil
import sys
import tempfile

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def check(label, condition):
    if not condition:
        raise AssertionError(label)
    print(f"[OK] {label}")


def main():
    with tempfile.TemporaryDirectory(prefix="fahrzeugverkauf-pdf-") as temp_name:
        runtime = Path(temp_name)
        portal.DATA_DIR = runtime
        portal.DB = runtime / "auftraege.db"
        portal.UPLOAD_DIR = runtime / "uploads"
        portal.DELETED_UPLOAD_DIR = runtime / "deleted_uploads"
        portal.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        portal.app.config.update(TESTING=True)
        portal.init_db()

        verkauf_id = portal.create_fahrzeugverkauf(
            {
                "kunde_name": "Eigenes Fahrzeug",
                "kontakt_telefon": "",
                "fahrzeug": "Citroën C4 1.2 Benzin 131 PS",
                "fin_nummer": "",
                "kilometerstand": "74.597 km",
                "baujahr": "01/2021",
                "status": "inseriert",
                "preis": "11.900 €",
                "preis_basis": "VB",
                "ausstattung": "Leichtmetallfelgen\nLED-Scheinwerfer\nEinparkhilfe\nKlimaanlage",
                "beschreibung": (
                    "Top-Zustand - Bremsen neu. Bekannter Vorschaden: Ein Blechschaden an der rechten "
                    "Fahrzeugseite wurde fachmännisch instand gesetzt."
                ),
                "markt_recherche": "Kleinanzeigen: 11.900 € VB",
                "kontakt_schild": "",
                "verkauft_am": "",
                "kaeufer": "",
                "notiz": "",
            }
        )

        source_image = ROOT / "static" / "mietwagen" / "kona.jpg"
        stored_name = "verkaufsbild.jpg"
        target_image = portal.UPLOAD_DIR / stored_name
        shutil.copyfile(source_image, target_image)
        db = portal.get_db()
        cursor = db.execute(
            """
            INSERT INTO fahrzeugverkauf_dateien
            (verkauf_id, original_name, stored_name, mime_type, size, kategorie, quelle, hochgeladen_am)
            VALUES (?, ?, ?, ?, ?, 'bild', 'test', ?)
            """,
            (
                verkauf_id,
                "citroen-c4.jpg",
                stored_name,
                "image/jpeg",
                target_image.stat().st_size,
                portal.now_str(),
            ),
        )
        db.execute(
            "UPDATE fahrzeugverkaeufe SET bild_datei_id=? WHERE id=?",
            (int(cursor.lastrowid), verkauf_id),
        )
        db.commit()
        db.close()

        client = portal.app.test_client()
        protected = client.get(f"/admin/fahrzeugverkauf/{verkauf_id}/verkaufsschild.pdf")
        check("PDF-Download bleibt ohne Admin-Sitzung geschützt", protected.status_code == 302)

        with client.session_transaction() as session:
            session["admin"] = True

        schild = client.get(f"/admin/fahrzeugverkauf/{verkauf_id}/verkaufsschild")
        schild_html = schild.get_data(as_text=True)
        check("Verkaufsschild lädt", schild.status_code == 200)
        check(
            "Download-Button steht oben im Verkaufsschild",
            "PDF herunterladen" in schild_html
            and f"/admin/fahrzeugverkauf/{verkauf_id}/verkaufsschild.pdf" in schild_html,
        )

        response = client.get(f"/admin/fahrzeugverkauf/{verkauf_id}/verkaufsschild.pdf")
        disposition = response.headers.get("Content-Disposition", "")
        check("PDF-Route liefert eine PDF", response.status_code == 200 and response.mimetype == "application/pdf")
        check("PDF wird als Download angeboten", "attachment" in disposition and ".pdf" in disposition)
        check("PDF besitzt eine gültige Signatur", response.data.startswith(b"%PDF-"))

        reader = PdfReader(BytesIO(response.data))
        pdf_text = "\n".join(page.extract_text() or "" for page in reader.pages)
        check("Verkaufsschild bleibt eine A4-Seite", len(reader.pages) == 1)
        check("Fahrzeug und Preis stehen in der PDF", "Citroën C4" in pdf_text and "11.900,00" in pdf_text)
        check("Bekannter Vorschaden steht in der PDF", "Bekannter Vorschaden" in pdf_text)


if __name__ == "__main__":
    main()
