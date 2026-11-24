"""Workshop printing through the real routes, with local synthetic orders and uploads."""

from contextlib import closing
from io import BytesIO
import json
import unittest

import fitz
from PIL import Image
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from werkzeug.datastructures import FileStorage

import smoke_test


portal = smoke_test.portal


def cut_lines(page):
    lines = []
    for drawing in page.get_drawings():
        dashes = drawing["dashes"]
        if not dashes or dashes.replace(" ", "").startswith("[]"):
            continue
        for item in drawing["items"]:
            if item[0] != "l":
                continue
            start, end = item[1:3]
            if abs(end.y - start.y) > page.rect.height * .85 or abs(end.x - start.x) > page.rect.width * .75:
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
        cuts = cut_lines(page)
        self.assertEqual(len(cuts), 1, "Senkrechte Schnittlinie am rechten Rand erwartet")
        start, end = cuts[0]
        cut_x = page.rect.width - 40 * mm
        self.assertAlmostEqual(start.x, cut_x, delta=.75)
        self.assertAlmostEqual(end.x, cut_x, delta=.75)
        tag_spans = []
        for block in page.get_text("dict")["blocks"]:
            for line in block.get("lines", []):
                for span in line["spans"]:
                    if span["text"].strip() and span["bbox"][2] > cut_x:
                        self.assertGreater(span["bbox"][0], cut_x, "Die Schnittlinie kreuzt Text")
                        self.assertAlmostEqual(abs(line["dir"][1]), 1, places=2)
                        self.assertGreaterEqual(span["size"], 29)
                        tag_spans.append(span)
        self.assertEqual(" ".join(s["text"].strip() for s in tag_spans), f"Auftrag {number}")
        bounds = fitz.Rect(tag_spans[0]["bbox"])
        for span in tag_spans[1:]:
            bounds.include_rect(span["bbox"])
        self.assertAlmostEqual((bounds.x0 + bounds.x1) / 2, (cut_x + page.rect.width) / 2, delta=10)
        self.assertAlmostEqual((bounds.y0 + bounds.y1) / 2, page.rect.height / 2, delta=10)

    def assert_print_hierarchy(self, page, title, number, service, start, end, identification):
        retained_width = page.rect.width - 40 * mm
        rack_window = fitz.Rect(0, 0, retained_width, 21 * mm)
        titles = page.search_for(f"{title} {number}", clip=rack_window)
        self.assertTrue(titles, "Titel und Nummer müssen über der Fachkante stehen")
        title_right = max(rect.x1 for rect in titles)
        schedule_window = fitz.Rect(title_right, 0, retained_width, rack_window.y1)
        for day, time in (start, end):
            self.assertTrue(page.search_for(f"{day} · {time}", clip=schedule_window),
                            f"Termin {day} {time} fehlt oben rechts auf dem bleibenden Blatt")
        work, = page.search_for("Arbeiten abhaken")
        body_header = fitz.Rect(0, 0, retained_width, work.y0)
        for text in (service, *identification):
            self.assertTrue(page.search_for(text, clip=body_header), f"{text!r} fehlt über der Checkliste")

        spans = [s for block in page.get_text("dict", clip=body_header)["blocks"]
                 for line in block.get("lines", []) for s in line["spans"] if s["text"].strip()]
        self.assertTrue(any(title in s["text"] and "bold" in s["font"].lower() for s in spans))
        self.assertGreaterEqual(work.y0 - max(s["bbox"][3] for s in spans), 9,
                                "Vor der Checkliste fehlt eine freie Textzeile")

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
            squares = [path for path in page.get_drawings() if 8 <= path["rect"].width <= 14
                       and abs(path["rect"].height - path["rect"].width) < .1]
            self.assertEqual(len(squares), 3)
            done, = page.search_for("Auftrag erledigt")
            self.assertGreater(done.y0, page.rect.height - 25 * mm)
            self.assertTrue(any(abs(path["rect"].y0 - done.y0) < 5 and 0 < done.x0 - path["rect"].x1 < 15
                                for path in squares), "Erledigt-Feld braucht eine Checkbox zum Abhaken")

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

    def test_check24_booking_and_tyre_details_fit_on_one_worksheet(self):
        intake = {"anliegen": "reifenservice", "reifen_leistungen": ["montage"],
                  "check24_bestellnummer": "TEST532", "check24_start": "2026-12-05T11:00",
                  "check24_ende": "2026-12-05T12:00"}
        notes = "\n".join(("CHECK24 · Bestellung TEST532", "Reifenmontage für 4 Reifen",
                           "Termin: 05.12.2026 11:00–12:00 Uhr", "4x Test-Winterreifen 245/40 R18 97 V",
                           "Voraussichtliche Reifenlieferung: 2026-12-02", "Gebuchter Gesamtpreis: 120 EUR"))
        oid = portal.create_auftrag("check24", leistungsart="reifenservice", kunde_name="Tina Beispiel",
                                    fahrzeug="Reifenservice", auftragsnummer="TEST532", beschreibung=notes,
                                    annahme_datum="05.12.2026", schaden_aufnahme_json=json.dumps(intake))
        with fitz.open(stream=self.client.get(f"/admin/auftrag/{oid}/ausdrucken").data) as pdf:
            self.assertEqual(len(pdf), 1, "Die Fertigmeldung darf keine zusätzliche Seite erzeugen")
            page = pdf[0]
            self.assert_first_page_tearoff(page, oid)
            self.assert_print_hierarchy(page, "Werkstattauftrag", oid, "Reifenmontage",
                                        ("05.12.2026", "11:00"), ("05.12.2026", "12:00"),
                                        ("Tina Beispiel", "Reifenservice", "TEST532"))
            rack_window = fitz.Rect(0, 0, page.rect.width - 40 * mm, 21 * mm)
            spans = {s["text"]: s for block in page.get_text("dict", clip=rack_window)["blocks"]
                     for line in block.get("lines", []) for s in line["spans"]}
            schedule_text = fitz.Rect()
            for text, minimum_size in (("Arbeitsbeginn", 13), ("Gebuchtes Ende", 13),
                                       ("05.12.2026 · 11:00", 19), ("05.12.2026 · 12:00", 19)):
                self.assertIn(text, spans)
                span = spans[text]
                self.assertEqual(span["color"], 0xFFFFFF)
                self.assertIn("bold", span["font"].lower())
                self.assertGreaterEqual(span["size"], minimum_size)
                self.assertTrue(rack_window.contains(span["bbox"]))
                schedule_text.include_rect(span["bbox"])
            self.assertTrue(any(path["fill"] == (0, 0, 0) and path["fill_opacity"] == 1
                                and path["rect"].contains(schedule_text) for path in page.get_drawings()),
                            "Beide Termine müssen im selben schwarzen Balken stehen")
            for text in ("Fahrzeugübergabe", "Angaben zum Auftrag", "Voraussichtliche Reifenlieferung",
                         "245/40 R18 97 V", "Auftrag erledigt"):
                self.assertTrue(page.search_for(text), text)

        del intake["check24_ende"]
        with closing(portal.get_db()) as db:
            db.execute("UPDATE auftraege SET schaden_aufnahme_json=? WHERE id=?", (json.dumps(intake), oid))
            db.commit()
        with fitz.open(stream=self.client.get(f"/admin/auftrag/{oid}/ausdrucken").data) as pdf:
            self.assertTrue(pdf[0].search_for("Offen", clip=rack_window))
            self.assertTrue(pdf[0].search_for("05.12.2026 · 11:00", clip=rack_window))

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
                self.assertFalse(cut_lines(page), "Es gibt nur auf Blatt 1 einen Abrissstreifen")
                self.assertNotIn("Auftrag erledigt", page.get_text())
                self.assertIn("Position ", page.get_text(), "Ein Folgeblatt braucht echte Arbeitspositionen")
            self.assertTrue(any("Position " in page.get_text(clip=fitz.Rect(0, page.rect.height * .75,
                                page.rect.width, page.rect.height)) for page in continuation),
                            "Folgeseiten müssen das untere Viertel für Arbeitspositionen nutzen")
            whole = "".join(page.get_text() for page in pdf)
            self.assertIn("Werkstattauftrag", whole)
            for index in range(140):
                self.assertEqual(whole.count(f"Position {index} bearbeiten."), 1)


if __name__ == "__main__":
    unittest.main()
