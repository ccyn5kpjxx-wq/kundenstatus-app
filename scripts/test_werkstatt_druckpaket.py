"""Workshop printing through the real routes, with local synthetic orders and uploads."""

from contextlib import closing
from io import BytesIO
import json
import unittest

import fitz
from PIL import Image
from reportlab.lib.pagesizes import A4, landscape
from werkzeug.datastructures import FileStorage

import smoke_test


portal = smoke_test.portal


def quarter_cut_lines(page):
    cut_y = page.rect.height * .75
    lines = []
    for drawing in page.get_drawings():
        for item in drawing["items"]:
            if item[0] != "l":
                continue
            start, end = item[1:3]
            if (abs(start.y - cut_y) <= .75 and abs(end.y - cut_y) <= .75
                    and abs(end.x - start.x) > page.rect.width * .75):
                lines.append((start, end))
    return lines


def original_pdf(*pages):
    width, height = landscape(A4)
    with fitz.open() as doc:
        for text in pages:
            doc.new_page(width=width, height=height).insert_text((40, 50), text)
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

    def assert_landscape_package(self, pdf):
        for page in pdf:
            with self.subTest(printed_page=page.number + 1):
                self.assertAlmostEqual(page.rect.width, 841.89, delta=.5)
                self.assertAlmostEqual(page.rect.height, 595.28, delta=.5)

    def assert_first_page_tearoff(self, page, number):
        self.assertEqual(len(quarter_cut_lines(page)), 1, "Schnittlinie bei 75 % der A4-Höhe erwartet")
        cut_y = page.rect.height * .75
        lower = [s for block in page.get_text("dict")["blocks"] for line in block.get("lines", [])
                 for s in line["spans"] if s["text"].strip() and s["bbox"][3] > cut_y]
        self.assertEqual(" ".join(s["text"].strip() for s in lower), f"Auftrag {number}")
        self.assertGreaterEqual(min(s["size"] for s in lower), 29)
        bounds = fitz.Rect(lower[0]["bbox"])
        for span in lower[1:]:
            bounds.include_rect(span["bbox"])
        self.assertAlmostEqual((bounds.x0 + bounds.x1) / 2, page.rect.width / 2, delta=2)
        self.assertAlmostEqual((bounds.y0 + bounds.y1) / 2, (cut_y + page.rect.height) / 2, delta=10)

    def assert_print_hierarchy(self, page, title, number, service, start, end, identification):
        spans = [s for block in page.get_text("dict")["blocks"] for line in block.get("lines", [])
                 for s in line["spans"] if s["text"].strip()]
        first_baseline = min(s["origin"][1] for s in spans)
        first = sorted((s for s in spans if abs(s["origin"][1] - first_baseline) < 1.5),
                       key=lambda s: s["bbox"][0])
        title_text = " ".join(s["text"].strip() for s in first)
        self.assertRegex(title_text, rf"(?i)^{title}\s+(?:Nr\.?\s*)?{number}$")
        self.assertTrue(any("bold" in s["font"].lower() for s in first))

        def find(text, after):
            matches = [s for s in spans if s["origin"][1] > after + 1.5 and text.casefold() in s["text"].casefold()]
            self.assertTrue(matches, f"{text!r} fehlt unterhalb der PDF-Zeile bei {after:.1f} pt")
            return min(matches, key=lambda s: s["origin"][1])

        work = find("Arbeiten abhaken", first_baseline)
        service_row = find(service, first_baseline)
        start_row = find(start[1], service_row["origin"][1])
        end_row = find(end[1], start_row["origin"][1])
        for day, row in ((start[0], start_row), (end[0], end_row)):
            same_row = " ".join(s["text"] for s in spans if abs(s["origin"][1] - row["origin"][1]) < 1.5)
            self.assertIn(day, same_row)
        fields = [find(text, end_row["origin"][1]) for text in identification]
        self.assertLess(max(s["origin"][1] for s in fields), work["origin"][1])
        self.assertLess(max(s["size"] for s in fields), min(s["size"] for s in (service_row, start_row, end_row)))
        self.assertGreater(max(s["size"] for s in first), max(s["size"] for s in (service_row, start_row, end_row)))

        previous = [s["bbox"][3] for s in spans if s["origin"][1] < work["origin"][1] - 1.5]
        self.assertGreaterEqual(work["bbox"][1] - max(previous), 9, "Vor der Checkliste fehlt eine freie Textzeile")

    def test_a4_cover_and_all_order_pages_exclude_other_jobs_and_invoices(self):
        original = original_pdf("TUER HINTEN RECHTS", "LACKIERFREIGABE")
        self.upload("Lackauftrag9617.pdf", original)
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
            self.assertRegex(pages[0], rf"(?i)Lackierauftrag\s+(?:Nr\.?\s*)?{self.oid}\b")
            for text in ("TEST AB 123", "9617", "LY7W", "Tür hinten rechts"):
                self.assertIn(text, pages[0])
            self.assert_landscape_package(pdf)
            self.assertIn("TUER HINTEN RECHTS", pages[1])
            self.assertIn("LACKIERFREIGABE", pages[2])
            with fitz.open(stream=original) as source:
                for index, source_page in enumerate(source):
                    self.assertEqual(source_page.get_pixmap().digest, pdf[index + 1].get_pixmap().digest)
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
            self.assert_landscape_package(pdf)
            page = pdf[0]
            self.assert_print_hierarchy(page, "Lackierauftrag", self.oid, "Lackier",
                ("15.11.2026", "08:00"), ("17.11.2026", "16:30"),
                (portal.get_autohaus_by_slug("kaesmann")["name"], "Audi A6", "TEST AB 123"))
            self.assert_first_page_tearoff(page, self.oid)
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

    def test_tyre_order_uses_the_same_header_hierarchy_and_checklist_spacing(self):
        order_id = portal.create_auftrag("intern", leistungsart="reifenservice", kunde_name="Tina Beispiel",
            fahrzeug="Opel Corsa", kennzeichen="TEST EF 456", start_datum="20.11.2026", fertig_datum="20.11.2026",
            schaden_aufnahme_json=json.dumps({"anliegen": "reifenservice", "reifen_leistungen": ["radwechsel"]}))
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET start_uhrzeit='10:15', fertig_uhrzeit='11:00' WHERE id=?", (order_id,))
            db.commit()
        with fitz.open(stream=self.client.get(f"/admin/auftrag/{order_id}/ausdrucken").data) as pdf:
            self.assertEqual(len(pdf), 1)
            self.assert_landscape_package(pdf)
            page = pdf[0]
            self.assert_print_hierarchy(page, "Werkstattauftrag", order_id, "Radwechsel",
                                        ("20.11.2026", "10:15"), ("20.11.2026", "11:00"),
                                        ("Tina Beispiel", "Opel Corsa", "TEST EF 456"))
            self.assert_first_page_tearoff(page, order_id)

    def test_image_and_unreadable_original_show_what_will_print(self):
        photo = BytesIO()
        Image.new("RGB", (500, 330), "white").save(photo, "PNG")
        self.upload("Lackierauftrag_Seite_2.png", photo.getvalue())
        self.upload("Lackauftrag_defekt.pdf", b"%PDF-unreadable")
        with fitz.open(stream=self.client.get(self.admin).data) as pdf:
            self.assertEqual(len(pdf), 2)
            self.assert_landscape_package(pdf)
            images = pdf[1].get_image_info()
            self.assertEqual(len(images), 1)
            image_box = fitz.Rect(images[0]["bbox"])
            self.assertTrue(pdf[1].rect.contains(image_box))
            self.assertAlmostEqual(image_box.width / image_box.height, 500 / 330, places=3)
            self.assertIn("Lackauftrag_defekt.pdf", pdf[0].get_text())
            self.assertIn("Fehlende Druckseiten", pdf[0].get_text())

    def test_mixed_and_rotated_originals_stay_upright_and_complete(self):
        with fitz.open() as attachment:
            for width, height, rotation in ((842, 595, 0), (595, 842, 0),
                                            (595, 842, 90), (842, 595, 90)):
                page = attachment.new_page(width=width, height=height)
                page.set_rotation(rotation)
                # Write upright at opposite edges in the orientation of the displayed original.
                for x, y, label in ((12, 20, "LINKS OBEN"),
                                    (page.rect.width - 112, page.rect.height - 12, "RECHTS UNTEN")):
                    point = fitz.Point(x, y) * page.derotation_matrix
                    page.insert_text(point, label, rotate=rotation)
            self.upload("Lackauftrag.pdf", attachment.tobytes())
        with fitz.open(stream=self.client.get(self.admin).data) as pdf:
            self.assertEqual(len(pdf), 5)
            self.assert_landscape_package(pdf)
            for index in range(1, len(pdf)):
                page = pdf[index]
                for label in ("LINKS OBEN", "RECHTS UNTEN"):
                    with self.subTest(original_page=index, label=label):
                        hits = page.search_for(label, quads=True)
                        self.assertEqual(len(hits), 1)
                        quad = hits[0] * page.rotation_matrix
                        self.assertTrue(page.rect.contains(quad.rect))
                        self.assertGreater(quad.ur.x, quad.ul.x)
                        self.assertAlmostEqual(quad.ur.y, quad.ul.y, delta=.2)
                        if label == "LINKS OBEN":
                            self.assertLess(quad.rect.x1, page.rect.width / 2)
                            self.assertLess(quad.rect.y1, page.rect.height / 4)
                        else:
                            self.assertGreater(quad.rect.x0, page.rect.width / 2)
                            self.assertGreater(quad.rect.y0, page.rect.height * .75)

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
            self.assert_landscape_package(pdf)
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
            self.assert_landscape_package(pdf)
            self.assert_first_page_tearoff(pdf[0], self.oid)
            continuation = [pdf[index] for index in range(1, len(pdf))]
            for page in continuation:
                self.assertFalse(quarter_cut_lines(page), "Es gibt nur auf Blatt 1 einen Abrissstreifen")
            self.assertTrue(any("Position " in page.get_text(clip=fitz.Rect(0, page.rect.height * .75,
                                page.rect.width, page.rect.height)) for page in continuation),
                            "Folgeseiten müssen das untere Viertel für Arbeitspositionen nutzen")
            whole = "".join(page.get_text() for page in pdf)
            self.assertIn("Werkstattauftrag", whole)
            for index in range(140):
                self.assertEqual(whole.count(f"Position {index} bearbeiten."), 1)


if __name__ == "__main__":
    unittest.main()
