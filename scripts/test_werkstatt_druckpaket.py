"""Workshop printing through the real routes, with local synthetic orders and uploads."""

from contextlib import closing
from io import BytesIO
import json
import unittest

import fitz
from PIL import Image
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
            self.assertAlmostEqual(pdf[0].rect.width, 595.28, places=1)
            self.assertAlmostEqual(pdf[0].rect.height, 841.89, places=1)
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
            self.assertTrue(pdf[1].get_images())
            self.assertLess(pdf[1].rect.width, pdf[1].rect.height)
            self.assertIn("Lackauftrag_defekt.pdf", pdf[0].get_text())
            self.assertIn("Fehlende Druckseiten", pdf[0].get_text())

    def test_wide_and_rotated_originals_print_in_portrait_without_losing_edges(self):
        with fitz.open() as attachment:
            page = attachment.new_page(width=842, height=595)
            page.insert_text((12, 20), "LINKS OBEN")
            page.insert_text((723, 572), "RECHTS UNTEN")
            page = attachment.new_page(width=595, height=842)
            page.insert_text((14, 24), "GEDREHTES BLATT")
            page.set_rotation(90)
            self.upload("Lackauftrag.pdf", attachment.tobytes())
        with fitz.open(stream=self.client.get(self.admin).data) as pdf:
            self.assertEqual(len(pdf), 3)
            for page in pdf:
                self.assertAlmostEqual(page.rect.width, 595.28, places=1)
                self.assertAlmostEqual(page.rect.height, 841.89, places=1)
            for page, text in ((pdf[1], "LINKS OBEN"), (pdf[1], "RECHTS UNTEN"),
                               (pdf[2], "GEDREHTES BLATT")):
                hits = page.search_for(text)
                self.assertEqual(len(hits), 1)
                self.assertTrue(page.rect.contains(hits[0]))

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
