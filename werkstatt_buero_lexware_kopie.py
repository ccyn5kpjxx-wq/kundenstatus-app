"""Local, deterministic review PDFs for an explicitly requested Lexware file handoff.

The caller must freeze the returned bytes before its first POST and reuse them on
retries. This module has no database, network, booking, or payment side effects.
Original bytes are only read; a review copy is never represented as an invoice.
"""
from __future__ import annotations

import hashlib
import hmac
import io
import math
from pathlib import Path
import re
import threading
import unicodedata
from xml.sax.saxutils import escape

import fitz
from PIL import Image, ImageOps
from pypdf import PdfReader, PdfWriter
import reportlab
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


MAX_SOURCE_BYTES = 10 * 1024 * 1024
MAX_SOURCE_PAGES = 100
MAX_IMAGE_PIXELS = 40_000_000
# Never reduce below 150 dpi / JPEG 65 merely to satisfy an upload limit.
# The caller must surface a clear failure when this legibility floor is too big.
_RASTER_PROFILES = ((180, 84), (170, 78), (160, 72), (150, 65))
_FONT_LOCK = threading.Lock()
_FONT = "LexwareReviewVera"
_BOLD = "LexwareReviewVeraBold"
_KINDS = {"rechnung": "Rechnung", "bescheid": "Bescheid", "post": "Sonstige Post",
          "lieferschein": "Lieferschein", "zahlungsauftrag": "Zahlungsauftrag - keine Rechnung"}
_CATEGORIES = {"rechnung": "Rechnungen", "gebuehren": "Gebühren", "steuern": "Steuern",
               "sonstige": "Sonstige Post", "privat": "Privat", "miete": "Miete", "kaution": "Kaution"}


def _fonts():
    with _FONT_LOCK:
        if _FONT not in pdfmetrics.getRegisteredFontNames():
            folder = Path(reportlab.__file__).resolve().parent / "fonts"
            pdfmetrics.registerFont(TTFont(_FONT, str(folder / "Vera.ttf")))
            pdfmetrics.registerFont(TTFont(_BOLD, str(folder / "VeraBd.ttf")))


def _text(value, maximum=400, *, shorten=False):
    if value is None:
        return "Nicht angegeben"
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise ValueError("Ungültige Angaben für die Prüfkopie.")
    value = unicodedata.normalize("NFC", str(value))
    value = "".join(" " if unicodedata.category(char).startswith("C") and char != "\n" else char
                    for char in value).strip()
    value = value.replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", "-")
    if len(value) > maximum:
        if not shorten:
            raise ValueError("Angaben für die Prüfkopie sind zu lang.")
        value = value[:maximum].rstrip() + " [gekürzt]"
    return value or "Nicht angegeben"


def _visible(value):
    """Keep unsupported glyphs readable as Unicode codes, never empty squares."""
    supported = pdfmetrics.getFont(_FONT).face.charToGlyph
    return "".join(char if char == "\n" or ord(char) in supported else f"[U+{ord(char):04X}]"
                   for char in value)


def _paragraph(value, style):
    return Paragraph(escape(_visible(value)).replace("\n", "<br/>"), style)


def _amount(value):
    if value is None:
        return "Nicht angegeben"
    if isinstance(value, bool) or not isinstance(value, int) or abs(value) > 999_999_999_999:
        raise ValueError("Ungültiger Betrag für die Prüfkopie.")
    sign = "-" if value < 0 else ""
    euros, cents = divmod(abs(value), 100)
    return f"{sign}{euros:,}".replace(",", ".") + f",{cents:02d} EUR"


def _cover(detail, digest, pages, *, raster=False):
    _fonts()
    body = ParagraphStyle("review-body", fontName=_FONT, fontSize=9, leading=12,
                          alignment=TA_LEFT, splitLongWords=True)
    label = ParagraphStyle("review-label", parent=body, fontName=_BOLD, fontSize=8.5)
    heading = ParagraphStyle("review-heading", parent=body, fontName=_BOLD, fontSize=20, leading=25)
    small = ParagraphStyle("review-small", parent=body, fontSize=7.5, leading=10)
    private = detail.get("bereich") == "privat" or detail.get("kategorie") == "privat"
    area = "Privat" if private else {"geschaeftlich": "Geschäftlich", "unklar": "Noch zuzuordnen"}.get(
        detail.get("bereich"), "Noch zuzuordnen")
    kind = _text(detail.get("typ"), 100)
    category = _text(detail.get("kategorie"), 100)
    title = "PRIVATE UNTERLAGE" if private else "PRÜFBELEG"
    stream = io.BytesIO()
    document = SimpleDocTemplate(stream, pagesize=A4, leftMargin=40, rightMargin=40,
                                 topMargin=34, bottomMargin=34, pageCompression=1,
                                 invariant=1, title="Manuelle Lexware-Prüfkopie",
                                 author="Werkstatt-Büro", creator="Werkstatt-Büro Prüfkopie v1")
    story = [_paragraph(title, heading), Spacer(1, 7),
             _paragraph("Prüfbeleg zur manuellen Ablage. Keine automatische Buchung oder Bankzahlung.", body),
             Spacer(1, 4), _paragraph("Keine automatische Betriebsausgabe." if private else
                 "Buchung, Steuerbehandlung und Zahlungsstand sind gesondert zu prüfen.", body), Spacer(1, 14)]
    rows = [
        ("Belegart", _KINDS.get(kind, kind)),
        ("Kategorie", _CATEGORIES.get(category, category)),
        ("Zuordnung", area),
        ("Portal-Beleg-ID / Revision", _text(detail["id"], 160) + " / " + str(detail["revision"])),
        ("Original-SHA256", digest),
        ("Betrag laut Belegerfassung", _amount(detail.get("betrag_cent"))),
        ("Belegdatum", _text(detail.get("belegdatum"), 80)),
        ("Original-Belegnummer", _text(detail.get("belegnummer"), 180)),
        ("Absender / Lieferant", _text(detail.get("lieferant"), 180)),
    ]
    payment_rows = [
        ("Zahlungsempfänger", _text(detail.get("zahlungsempfaenger"), 180)),
        ("IBAN", _text(detail.get("iban"), 80)),
        ("BIC", _text(detail.get("bic"), 40)),
        ("Verwendungszweck", _text(detail.get("verwendungszweck"), 340)),
        ("Fälligkeit", _text(detail.get("faelligkeit"), 80)),
    ]

    def table(items):
        result = Table([[_paragraph(left, label), _paragraph(right, body)] for left, right in items],
                       colWidths=(149, A4[0] - 229), hAlign="LEFT")
        result.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"),
                                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                                    ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.HexColor("#f0f3f5"), colors.white])]))
        return result

    story.extend([table(rows), Spacer(1, 12), _paragraph("Überweisungsdaten laut Belegerfassung", label),
                  Spacer(1, 5), table(payment_rows), Spacer(1, 12),
                  _paragraph("Quellnotiz: " + _text(detail.get("notiz"), 360, shorten=True), small),
                  Spacer(1, 8), _paragraph(
                      f"Es folgen alle {pages} Originalseiten in unveränderter Reihenfolge. "
                      + ("Für die Uploadgrenze als komprimierte Raster-Lesekopie wiedergegeben. " if raster else "")
                      + "Diese Kopie ersetzt das separat erhaltene Original nicht. "
                      "Keine neue Rechnung, keine ergänzten Steuerwerte, Raten oder Zahlungsbestätigung.", small)])
    document.build(story)
    cover = stream.getvalue()
    if len(PdfReader(io.BytesIO(cover), strict=True).pages) != 1:
        raise ValueError("Die Angaben sind für eine übersichtliche Prüfdeckseite zu lang; bitte Quellnotiz kürzen.")
    return cover


def _combine(cover, source, identity):
    writer = PdfWriter()
    for raw in (cover, source):
        reader = PdfReader(io.BytesIO(raw), strict=True)
        for page in reader.pages:
            writer.add_page(page)
    writer.add_metadata({"/Title": "Manuelle Lexware-Prüfkopie", "/Author": "Werkstatt-Büro", "/Subject": identity,
                         "/Creator": "Werkstatt-Büro Prüfkopie v1", "/Producer": "Werkstatt-Büro Prüfkopie v1"})
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def _raster_profile_fits(source, dpi):
    # MuPDF rounds transformed page bounds outwards, so use its actual bounding
    # boxes rather than rounded width/height estimates. Check every page before
    # allocating a pixmap; a later oversized page must skip the whole profile.
    for page in source:
        bounds = (page.rect * fitz.Matrix(dpi / 72, dpi / 72)).irect
        if min(bounds.width, bounds.height) < 1 or bounds.width * bounds.height > MAX_IMAGE_PIXELS:
            return False
    return True


def _raster_pdf(source, dpi, quality):
    if not _raster_profile_fits(source, dpi):
        raise ValueError("Originalseite ist für eine sichere, lesbare Prüfkopie zu groß.")
    output = fitz.open()
    try:
        for page in source:
            rect = page.rect
            pixmap = page.get_pixmap(dpi=dpi, colorspace=fitz.csRGB, alpha=False, annots=True)
            image = Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)
            jpeg = io.BytesIO()
            image.save(jpeg, format="JPEG", quality=quality, optimize=True, progressive=False, subsampling=0)
            target = output.new_page(width=rect.width, height=rect.height)
            target.insert_image(target.rect, stream=jpeg.getvalue())
        # no_new_id avoids a clock/random-based document ID.
        return output.tobytes(garbage=4, deflate=True, no_new_id=True)
    finally:
        output.close()


def _image_pdf(raw):
    try:
        with Image.open(io.BytesIO(raw)) as original:
            if original.format not in {"JPEG", "PNG"} or getattr(original, "n_frames", 1) != 1:
                raise ValueError("Nur einseitige PNG- oder JPEG-Bilder sind als Original zulässig.")
            if not 0 < original.width * original.height <= MAX_IMAGE_PIXELS:
                raise ValueError("Originalbild ist für eine sichere Prüfkopie zu groß.")
            original.load()
            image = ImageOps.exif_transpose(original)
            # RGBA expansion applies PNG tRNS keys as well as existing alpha
            # channels, including palette and RGB PNGs with no A band.
            rgba = image.convert("RGBA")
            background = Image.new("RGB", rgba.size, "white")
            background.paste(rgba, mask=rgba.getchannel("A"))
            encoded = io.BytesIO()
            background.save(encoded, format="PNG", optimize=True)
            document = fitz.open()
            try:
                # A4-sized sheet, aspect preserved. Never infer money or metadata from pixels.
                scale = min(A4[0] / background.width, A4[1] / background.height)
                page = document.new_page(width=background.width * scale, height=background.height * scale)
                page.insert_image(page.rect, stream=encoded.getvalue())
                return document.tobytes(garbage=4, deflate=True, no_new_id=True)
            finally:
                document.close()
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Originalbild kann nicht sicher als Prüfkopie gelesen werden.") from exc


def prepare_lexware_review_copy(detail: dict, raw: bytes, max_bytes=5_000_000) -> dict:
    """Return a deterministic, document-specific PDF; reject incomplete/invalid sources.

    ``detail`` requires id, positive revision and sha256 (or original_sha256).
    Missing banking fields are displayed as ``Nicht angegeben`` and never block.
    All original pages follow one review cover. Raster fallback retains all pages
    at >=150 dpi and JPEG quality >=65, otherwise this operation fails clearly.
    """
    if not isinstance(detail, dict) or not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_SOURCE_BYTES:
        raise ValueError("Original fehlt oder überschreitet die Grenze von 10 MiB.")
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or not 0 < max_bytes <= 5_000_000:
        raise ValueError("Ungültige Uploadgrenze für die Prüfkopie (höchstens 5.000.000 Bytes).")
    key = detail.get("id")
    revision = detail.get("revision")
    if not isinstance(key, str) or not key.strip() or len(key) > 160 or any(unicodedata.category(c).startswith("C") for c in key):
        raise ValueError("Stabile Portal-Beleg-ID für die Prüfkopie fehlt oder ist ungültig.")
    if isinstance(revision, bool) or not isinstance(revision, int) or not 0 < revision <= 2**63 - 1:
        raise ValueError("Gültige Belegrevision für die Prüfkopie fehlt.")
    digest = hashlib.sha256(raw).hexdigest()
    expected = [detail[name] for name in ("sha256", "original_sha256") if name in detail]
    if not expected or any(not isinstance(value, str) or not re.fullmatch(r"[a-fA-F0-9]{64}", value)
                           or not hmac.compare_digest(digest, value.lower()) for value in expected):
        raise ValueError("Original-SHA256 stimmt nicht mit den erhaltenen Originalbytes überein.")
    try:
        if raw.startswith(b"%PDF-"):
            source_bytes = raw
        elif raw.startswith(b"\x89PNG\r\n\x1a\n") or raw.startswith(b"\xff\xd8\xff"):
            source_bytes = _image_pdf(raw)
        else:
            raise ValueError("Original ist kein lesbares PDF-, PNG- oder JPEG-Dokument.")
        with fitz.open(stream=source_bytes, filetype="pdf") as source:
            if source.needs_pass or source.is_encrypted or source.is_repaired:
                raise ValueError("Original-PDF ist verschlüsselt oder beschädigt; geprüfte Lesekopie erforderlich.")
            pages = source.page_count
            if not 1 <= pages <= MAX_SOURCE_PAGES:
                raise ValueError("Eine Prüfkopie benötigt 1 bis 100 vollständige Originalseiten.")
            # Walk every page before composing; malformed later pages must not vanish.
            for page in source:
                if not all(math.isfinite(value) for value in page.rect) or page.rect.is_empty:
                    raise ValueError("Original enthält eine ungültige Seite.")
            cover = _cover(detail, digest, pages)
            # Preserve exact identity even when printable Unicode normalisation
            # makes two unusual IDs look alike on the cover.
            identity = f"Portal-Beleg-ID: {key}; Revision: {revision}"
            combined = _combine(cover, source_bytes, identity)
            if len(combined) > max_bytes:
                cover = _cover(detail, digest, pages, raster=True)
                for dpi, quality in _RASTER_PROFILES:
                    if not _raster_profile_fits(source, dpi):
                        continue
                    combined = _combine(cover, _raster_pdf(source, dpi, quality), identity)
                    if len(combined) <= max_bytes:
                        break
                else:
                    raise ValueError("Alle Originalseiten passen nicht gut lesbar unter die Uploadgrenze. "
                                     "Bitte eine besser komprimierte vollständige Lesekopie bereitstellen.")
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Original kann nicht vollständig als Prüfkopie verarbeitet werden.") from exc
    if len(PdfReader(io.BytesIO(combined), strict=True).pages) != pages + 1:
        raise ValueError("Prüfkopie enthält nicht alle Originalseiten.")
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", unicodedata.normalize("NFKD", key).encode("ascii", "ignore").decode()).strip("-")[:60]
    token = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
    filename = f"Pruefkopie-{slug or 'Beleg'}-{token}-r{revision}.pdf"
    return {"bytes": combined, "mime": "application/pdf", "filename": filename,
            "sha256": hashlib.sha256(combined).hexdigest(), "original_sha256": digest,
            "transformed": True, "source_pages": pages}
