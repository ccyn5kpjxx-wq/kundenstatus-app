"""Workshop printing through the real routes, with local synthetic orders and uploads."""

from contextlib import closing
from io import BytesIO
import unittest

import fitz
from PIL import Image
from werkzeug.datastructures import FileStorage

import smoke_test


portal = smoke_test.portal


def original_pdf(*pages):
    with fitz.open() as doc:
        for text in pages:
            doc.new_page().insert_text((40, 50), text)
        return doc.tobytes()


class OrderPrintTest(unittest.TestCase):
    def setUp(self):
        self.oid = portal.create_auftrag("autohaus", autohaus_id=portal.get_autohaus_by_slug("kaesmann")["id"],
            leistungsart="lackierung", fahrzeug="Audi A6", kennzeichen="TEST AB 123", farbcode="LY7W",
            beschreibung="Tür hinten rechts instand setzen und lackieren.", auftragsnummer="9617")
        self.client = portal.app.test_client()
        with self.client.session_transaction() as sess:
            sess["admin"] = True
        self.admin = f"/admin/auftrag/{self.oid}/ausdrucken"
        self.halle = f"/werkstatt/auftrag/{self.oid}/ausdrucken"

    def upload(self, name, raw, *, order=None, kategorie="standard"):
        saved, _ = portal.save_uploads(order or self.oid, [FileStorage(stream=BytesIO(raw), filename=name)],
                                       "autohaus", kategorie=kategorie, analyze=False)
        self.assertEqual(saved, 1)

    def test_a4_cover_and_all_order_pages_exclude_other_jobs_and_invoices(self):
        self.upload("Lackauftrag9617.pdf", original_pdf("TUER HINTEN RECHTS", "LACKIERFREIGABE"))
        self.upload("Rechnung_Auftrag_9617.pdf", original_pdf("PRIVATE INVOICE"), kategorie="rechnung")
        other = portal.create_auftrag("intern", fahrzeug="Testwagen")
        self.upload("Lackauftrag7777.pdf", original_pdf("OTHER ORDER"), order=other)
        result = self.client.get(self.admin)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.mimetype, "application/pdf")
        self.assertIn("inline", result.headers["Content-Disposition"])
        self.assertIn("no-store", result.headers["Cache-Control"])
        with fitz.open(stream=result.data) as pdf:
            self.assertEqual(len(pdf), 3)
            pages = [p.get_text() for p in pdf]
            for text in (f"Lackierauftrag {self.oid}", "TEST AB 123", "9617", "LY7W", "Tür hinten rechts"):
                self.assertIn(text, pages[0])
            self.assertAlmostEqual(pdf[0].rect.width, 841.89, places=1)
            self.assertIn("TUER HINTEN RECHTS", pages[1])
            self.assertIn("LACKIERFREIGABE", pages[2])
            self.assertNotIn("PRIVATE INVOICE", "".join(pages))
            self.assertNotIn("OTHER ORDER", "".join(pages))

    def test_rack_header_checklist_and_scissor_strip(self):
        with closing(portal.get_db()) as db:
            db.execute("""UPDATE auftraege SET start_datum='15.11.2026', start_uhrzeit='08:00',
                annahme_datum='14.11.2026', fertig_datum='17.11.2026', fertig_uhrzeit='16:30',
                beschreibung=? WHERE id=?""",
                       ("Tür vorne rechts: Neuteil lackieren; Tür hinten rechts instand setzen", self.oid))
            db.commit()
        with fitz.open(stream=self.client.get(self.admin).data) as pdf:
            self.assertEqual(len(pdf), 1)
            page = pdf[0]
            header = page.get_text(clip=fitz.Rect(0, 0, page.rect.width, 62))
            for value in ("ARBEITSBEGINN", "15.11.2026", "08:00", "17.11.2026", "16:30", "Lackierauftrag"):
                self.assertIn(value, header)
            self.assertNotIn("14.11.2026", header)
            strip = fitz.Rect(0, page.rect.height - 70, page.rect.width, page.rect.height)
            self.assertEqual(page.get_text(clip=strip).strip(), f"Auftrag {self.oid}")
            text = page.get_text()
            self.assertIn("Auftrag erledigt", text)
            self.assertIn("Rückgabe ans Büro", text)
            self.assertIn("Tür hinten rechts instand setzen", text)
            spans = [span for block in page.get_text("dict")["blocks"] if "lines" in block
                     for line in block["lines"] for span in line["spans"]]
            self.assertTrue(any("NEUTEIL" in span["text"] and "Bold" in span["font"] for span in spans))
            squares = [path for path in page.get_drawings() if abs(path["rect"].width - 11) < .1
                       and abs(path["rect"].height - 11) < .1]
            self.assertEqual(len(squares), 3)

    def test_image_and_unreadable_original_show_what_will_print(self):
        photo = BytesIO()
        Image.new("RGB", (500, 330), "white").save(photo, "PNG")
        self.upload("Lackierauftrag_Seite_2.png", photo.getvalue())
        self.upload("Lackauftrag_defekt.pdf", b"%PDF-unreadable")
        with fitz.open(stream=self.client.get(self.admin).data) as pdf:
            self.assertEqual(len(pdf), 2)
            self.assertTrue(pdf[1].get_images())
            self.assertGreater(pdf[1].rect.width, pdf[1].rect.height)
            self.assertIn("Lackauftrag_defekt.pdf", pdf[0].get_text())
            self.assertIn("Fehlende Druckseiten", pdf[0].get_text())

    def test_permissions_archives_and_orders_without_upload(self):
        partner = portal.app.test_client()
        with partner.session_transaction() as sess:
            sess["partner_autohaus_id"] = portal.get_autohaus_by_slug("kaesmann")["id"]
        for route in (self.admin, self.halle):
            self.assertNotEqual(partner.get(route).mimetype, "application/pdf")
        portal.set_app_setting(portal.WERKSTATT_TAFEL_CODE_SETTING, "TEST-PRINT")
        hall = portal.app.test_client()
        with hall.session_transaction() as sess:
            sess["werkstatt_tafel"] = portal.werkstatt_tafel_session_token()
        with fitz.open(stream=hall.get(self.halle).data) as pdf:
            self.assertEqual(len(pdf), 1)
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET archiviert=1 WHERE id=?", (self.oid,))
            db.commit()
        self.assertEqual(hall.get(self.halle).status_code, 404)
        self.assertEqual(self.client.get(self.admin).mimetype, "application/pdf")

    def test_long_work_instructions_are_preserved_across_sheets(self):
        words = "\n".join(f"Position {i} bearbeiten." for i in range(140))
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET beschreibung=?, leistungsart='mechanik' WHERE id=?", (words, self.oid))
            db.commit()
        with fitz.open(stream=self.client.get(self.admin).data) as pdf:
            self.assertGreater(len(pdf), 1)
            whole = "".join(page.get_text() for page in pdf)
            self.assertIn("Werkstattauftrag", whole)
            self.assertIn("Position 139 bearbeiten.", whole)


if __name__ == "__main__":
    unittest.main()
