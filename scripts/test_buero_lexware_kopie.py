"""Review-copy tests use generated documents only; never call a provider or app."""
import hashlib
import io
from pathlib import Path
import random
import sys
import unittest
from unittest.mock import patch

import fitz
from PIL import Image
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen.canvas import Canvas

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from werkstatt_buero_lexware_kopie import prepare_lexware_review_copy, MAX_SOURCE_BYTES
import werkstatt_buero_lexware_kopie as review_copy


def pdf_bytes(pages=2):
    output = io.BytesIO()
    canvas = Canvas(output, invariant=1)
    for index in range(pages):
        canvas.drawString(40, 780, f"SYNTHETIC ORIGINAL PAGE {index + 1}")
        canvas.setFillColorRGB(index % 2, 0.25, 0.65)
        canvas.rect(30, 50, 200 + index % 3, 100, fill=1)
        canvas.showPage()
    canvas.save()
    return output.getvalue()


def metadata(raw, **changes):
    result = dict(id="a" * 32, revision=4, sha256=hashlib.sha256(raw).hexdigest(),
                  typ="zahlungsauftrag", kategorie="miete", bereich="geschaeftlich",
                  belegnummer="", belegdatum="", betrag_cent=171003, faelligkeit="",
                  zahlungsempfaenger="", iban="", bic="", verwendungszweck="",
                  notiz="Synthetische Quelle; keine Zahlungsbestätigung.")
    result.update(changes)
    return result


def cover_text(result):
    return PdfReader(io.BytesIO(result["bytes"])).pages[0].extract_text()


def noisy_pdf_sizes(sizes):
    image = Image.frombytes("L", (400, 400), random.Random(41).randbytes(400 * 400))
    output = io.BytesIO()
    image.save(output, format="PNG")
    source = fitz.open()
    for width, height in sizes:
        page = source.new_page(width=width, height=height)
        page.insert_image(page.rect, stream=output.getvalue())
    raw = source.tobytes(no_new_id=True)
    source.close()
    return raw


class LexwareReviewCopyTests(unittest.TestCase):
    def test_repeatable_and_different_beleg_ids_for_same_original(self):
        raw = pdf_bytes()
        before = bytes(raw)
        first = prepare_lexware_review_copy(metadata(raw), raw)
        second = prepare_lexware_review_copy(metadata(raw), raw)
        other = prepare_lexware_review_copy(metadata(raw, id="b" * 32, kategorie="kaution"), raw)
        self.assertEqual(first, second)
        self.assertNotEqual(first["sha256"], other["sha256"])
        self.assertEqual(first["original_sha256"], other["original_sha256"])
        self.assertEqual(raw, before)
        self.assertEqual(first["sha256"], hashlib.sha256(first["bytes"]).hexdigest())
        self.assertEqual(first["mime"], "application/pdf")
        self.assertTrue(first["transformed"])
        self.assertLessEqual(len(first["bytes"]), 5_000_000)

    def test_canonically_equivalent_id_spellings_still_have_distinct_pdf_bytes(self):
        raw = pdf_bytes(1)
        first = prepare_lexware_review_copy(metadata(raw, id="Öl"), raw)
        second = prepare_lexware_review_copy(metadata(raw, id="O\u0308l"), raw)
        self.assertNotEqual(first["bytes"], second["bytes"])

    def test_every_original_page_follows_cover_without_visual_change(self):
        raw = pdf_bytes(4)
        result = prepare_lexware_review_copy(metadata(raw), raw)
        self.assertEqual(result["source_pages"], 4)
        with fitz.open(stream=raw, filetype="pdf") as source, fitz.open(stream=result["bytes"], filetype="pdf") as copy:
            self.assertEqual(len(copy), 5)
            for index, page in enumerate(source):
                self.assertEqual(page.get_text(), copy[index + 1].get_text())
                self.assertEqual(page.get_pixmap().samples, copy[index + 1].get_pixmap().samples)

    def test_deck_preserves_real_number_unicode_xml_and_newlines(self):
        raw = pdf_bytes(1)
        number = '<font color="red">ORIGINAL & 42</font>'
        result = prepare_lexware_review_copy(metadata(raw, belegnummer=number,
            lieferant="Müller & Söhne\nΩ Ж 张", verwendungszweck="Zweck\n<unverändert>",
            notiz="Quelle <b>ungeprüft</b> & original"), raw)
        text = cover_text(result)
        self.assertIn(number, text.replace("\n", " "))
        self.assertIn("Müller & Söhne", text)
        self.assertIn("Ω [U+0416] [U+5F20]", text)
        self.assertIn("<unverändert>", text)
        self.assertIn("<b>ungeprüft</b>", text)
        self.assertIn("1.710,03 EUR", text)
        self.assertIn("Keine automatische Buchung oder Bankzahlung", text)
        self.assertIn(metadata(raw)["sha256"], text.replace("\n", ""))

    def test_all_five_missing_transfer_fields_are_visible_and_no_fake_number(self):
        raw = pdf_bytes(1)
        result = prepare_lexware_review_copy(metadata(raw), raw)
        text = cover_text(result)
        for name in ("Zahlungsempfänger", "IBAN", "BIC", "Verwendungszweck", "Fälligkeit"):
            self.assertIn(name, text)
        self.assertEqual(text.count("Nicht angegeben"), 8)
        self.assertIn("Zahlungsauftrag - keine Rechnung", text)
        self.assertIn("Keine neue Rechnung", " ".join(text.split()))

    def test_private_and_unclear_are_not_represented_as_business(self):
        raw = pdf_bytes(1)
        private = prepare_lexware_review_copy(metadata(raw, typ="post", kategorie="privat", bereich="unklar", betrag_cent=None), raw)
        text = cover_text(private)
        self.assertIn("PRIVATE UNTERLAGE", text)
        self.assertIn("Keine automatische Betriebsausgabe", text)
        self.assertIn("Sonstige Post", text)
        unclear = prepare_lexware_review_copy(metadata(raw, bereich="unklar"), raw)
        self.assertIn("Noch zuzuordnen", cover_text(unclear))
        self.assertNotIn("Geschäftlich", cover_text(unclear))

    def test_hash_required_and_conflicting_original_hash_hard_fails(self):
        raw = pdf_bytes(1)
        for changes in ({"sha256": "0" * 64}, {"sha256": None}, {"original_sha256": "0" * 64}):
            with self.subTest(changes=list(changes)), self.assertRaisesRegex(ValueError, "SHA256"):
                prepare_lexware_review_copy(metadata(raw, **changes), raw)
        detail = metadata(raw)
        detail["original_sha256"] = detail.pop("sha256").upper()
        self.assertEqual(prepare_lexware_review_copy(detail, raw)["original_sha256"], hashlib.sha256(raw).hexdigest())

    def test_invalid_source_and_source_size_do_not_leak_partial_result(self):
        for raw in (b"", b"not a pdf", b"%PDF-invalid", b"x" * (MAX_SOURCE_BYTES + 1)):
            with self.subTest(size=len(raw)), self.assertRaises(ValueError):
                prepare_lexware_review_copy(metadata(raw), raw)

    def test_one_hundred_pages_accepted_and_101_rejected(self):
        raw = pdf_bytes(100)
        self.assertEqual(prepare_lexware_review_copy(metadata(raw), raw)["source_pages"], 100)
        raw = pdf_bytes(101)
        with self.assertRaisesRegex(ValueError, "1 bis 100"):
            prepare_lexware_review_copy(metadata(raw), raw)

    def test_encrypted_pdf_is_not_silently_unlocked(self):
        source = pdf_bytes(1)
        writer = PdfWriter()
        writer.append(io.BytesIO(source))
        writer.encrypt("synthetic-password")
        output = io.BytesIO()
        writer.write(output)
        raw = output.getvalue()
        with self.assertRaisesRegex(ValueError, "verschlüsselt"):
            prepare_lexware_review_copy(metadata(raw), raw)

    def test_metadata_limits_safe_filename_and_revision(self):
        raw = pdf_bytes(1)
        result = prepare_lexware_review_copy(metadata(raw, id="Öl / Prüfbeleg " + "x" * 80,
                                                    notiz="lange Quelle " * 1000), raw)
        self.assertRegex(result["filename"], r"^[A-Za-z0-9_-]+\.pdf$")
        self.assertLess(len(result["filename"]), 120)
        self.assertIn("[gekürzt]", cover_text(result))
        for changes in ({"id": "bad\nID"}, {"revision": True}, {"revision": 0},
                        {"betrag_cent": 1.0}, {"lieferant": "x" * 181}):
            with self.subTest(fields=list(changes)), self.assertRaises(ValueError):
                prepare_lexware_review_copy(metadata(raw, **changes), raw)

    def test_upload_limit_is_exact_and_fails_without_dropping_pages(self):
        raw = pdf_bytes(2)
        for maximum in (True, 0, -1, 5_000_001):
            with self.subTest(maximum=maximum), self.assertRaises(ValueError):
                prepare_lexware_review_copy(metadata(raw), raw, maximum)
        with self.assertRaisesRegex(ValueError, "Alle Originalseiten passen nicht gut lesbar"):
            prepare_lexware_review_copy(metadata(raw), raw, 1000)

    def test_png_and_jpeg_have_one_complete_source_page(self):
        for format in ("PNG", "JPEG"):
            image = Image.new("RGB", (700, 1000), "white")
            image.paste("red", (0, 0, 100, 100))
            image.paste("blue", (600, 900, 700, 1000))
            output = io.BytesIO()
            image.save(output, format=format)
            raw = output.getvalue()
            result = prepare_lexware_review_copy(metadata(raw), raw)
            self.assertEqual(result["source_pages"], 1)
            with fitz.open(stream=result["bytes"], filetype="pdf") as copy:
                self.assertEqual(len(copy), 2)
                pixels = copy[1].get_pixmap()
                rgb = Image.frombytes("RGB", (pixels.width, pixels.height), pixels.samples)
                self.assertGreater(rgb.getpixel((5, 5))[0], 200)
                self.assertGreater(rgb.getpixel((rgb.width - 5, rgb.height - 5))[2], 200)

    def test_palette_and_rgb_png_transparency_composite_on_white(self):
        for mode in ("P", "RGB"):
            with self.subTest(mode=mode):
                if mode == "P":
                    image = Image.new("P", (120, 180), 0)
                    image.putpalette([0, 0, 0, 255, 0, 0] + [0] * (768 - 6))
                    image.paste(1, (30, 40, 90, 140))
                    transparency = 0
                else:
                    image = Image.new("RGB", (120, 180), "black")
                    image.paste("red", (30, 40, 90, 140))
                    transparency = (0, 0, 0)
                output = io.BytesIO()
                image.save(output, format="PNG", transparency=transparency)
                raw = output.getvalue()
                with Image.open(io.BytesIO(raw)) as original:
                    self.assertNotIn("A", original.getbands())
                result = prepare_lexware_review_copy(metadata(raw), raw)
                with fitz.open(stream=result["bytes"], filetype="pdf") as document:
                    pix = document[1].get_pixmap()
                    rendered = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                    self.assertEqual(rendered.getpixel((5, 5)), (255, 255, 255))
                    red = rendered.getpixel((pix.width // 2, pix.height // 2))
                    self.assertGreater(red[0], 245)
                    self.assertLess(max(red[1:]), 10)

    def test_image_exif_orientation_is_retained_when_compositing(self):
        image = Image.new("RGB", (80, 120), "white")
        exif = Image.Exif()
        exif[274] = 6  # The visible document is rotated 90 degrees clockwise.
        output = io.BytesIO()
        image.save(output, format="JPEG", exif=exif)
        raw = output.getvalue()
        result = prepare_lexware_review_copy(metadata(raw), raw)
        with fitz.open(stream=result["bytes"], filetype="pdf") as document:
            self.assertGreater(document[1].rect.width, document[1].rect.height)
            self.assertAlmostEqual(document[1].rect.width / document[1].rect.height, 1.5, places=4)

    def test_oversized_profiles_are_skipped_for_all_pages_before_rendering(self):
        # The later 36x48-inch page exceeds 40MP at 180/170/160dpi;
        # its complete 150dpi rendition is 38.88MP and is still admissible.
        raw = noisy_pdf_sizes(((70, 70), (36 * 72, 48 * 72)))
        with patch.object(review_copy, "_raster_pdf", return_value=pdf_bytes(2)) as render:
            result = prepare_lexware_review_copy(metadata(raw), raw, 80_000)
        self.assertEqual(render.call_count, 1)
        self.assertEqual(render.call_args.args[1:], (150, 65))
        self.assertEqual(result["source_pages"], 2)
        self.assertEqual(len(PdfReader(io.BytesIO(result["bytes"])).pages), 3)

    def test_skipped_profiles_can_reach_a_real_smaller_raster(self):
        # Scale the same geometry and budget down 1000-fold for a fast real
        # rendering regression, rather than allocate a 39MP test pixmap.
        scale = 1000 ** .5
        raw = noisy_pdf_sizes(((70, 70), (36 * 72 / scale, 48 * 72 / scale)))
        with patch.object(review_copy, "MAX_IMAGE_PIXELS", 40_000), \
                patch.object(review_copy, "_raster_pdf", wraps=review_copy._raster_pdf) as render:
            result = prepare_lexware_review_copy(metadata(raw), raw, 80_000)
        self.assertEqual(render.call_count, 1)
        self.assertEqual(render.call_args.args[1:], (150, 65))
        self.assertEqual(len(PdfReader(io.BytesIO(result["bytes"])).pages), 3)
        self.assertLessEqual(len(result["bytes"]), 80_000)

    def test_no_admissible_raster_profile_fails_before_allocating_pixels(self):
        raw = noisy_pdf_sizes(((70, 70), (40 * 72, 50 * 72)))
        with patch.object(review_copy, "_raster_pdf") as render:
            with self.assertRaisesRegex(ValueError, "Alle Originalseiten passen nicht gut lesbar"):
                prepare_lexware_review_copy(metadata(raw), raw, 80_000)
        render.assert_not_called()

    def test_big_scan_raster_fallback_is_stable_and_preserves_both_pages(self):
        source = fitz.open()
        generator = random.Random(7331)
        for index in range(2):
            image = Image.frombytes("L", (1600, 2200), generator.randbytes(1600 * 2200))
            image.paste(255, (0, 0, 1600, 180))
            image.paste(index * 180, (0, 2050, 1600, 2200))
            output = io.BytesIO()
            image.save(output, format="PNG")
            page = source.new_page(width=595, height=842)
            page.insert_image(page.rect, stream=output.getvalue())
        raw = source.tobytes(no_new_id=True)
        source.close()
        self.assertGreater(len(raw), 5_000_000)
        self.assertLessEqual(len(raw), MAX_SOURCE_BYTES)
        result = prepare_lexware_review_copy(metadata(raw), raw)
        repeated = prepare_lexware_review_copy(metadata(raw), raw)
        self.assertEqual(result["bytes"], repeated["bytes"])
        self.assertLessEqual(len(result["bytes"]), 5_000_000)
        self.assertIn("Raster-Lesekopie", cover_text(result))
        with fitz.open(stream=result["bytes"], filetype="pdf") as copy:
            self.assertEqual(len(copy), 3)
            self.assertEqual(result["source_pages"], 2)
            for index in range(2):
                pix = copy[index + 1].get_pixmap()
                image = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                self.assertGreater(min(image.getpixel((pix.width // 2, 15))), 245)
                self.assertLess(abs(image.getpixel((pix.width // 2, pix.height - 15))[0] - index * 180), 15)


if __name__ == "__main__":
    unittest.main(verbosity=2)
