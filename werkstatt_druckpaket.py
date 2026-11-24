"""Printable workshop order followed by its original work instructions."""

from io import BytesIO
import re
from xml.sax.saxutils import escape

import fitz
from PIL import Image, ImageOps
from reportlab.lib.colors import HexColor
from reportlab.graphics.shapes import Drawing, Rect
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import getAscentDescent
from reportlab.platypus import BaseDocTemplate, Frame, PageTemplate, Paragraph, Table, TableStyle

from auftragsarten import AUFTRAGSARTEN, auftrag_leistungsart
from reifen_auftragsetikett import reifenetikett
from werkstatt_auftrag_ausdruck import _FIELDS, _event, _sheet_data, _work_text
from werkstatt_druck_checkliste import checklist_rows
from werkstatt_druckquellen import druckquellen, lade_druckquelle


_PAGE_SIZE = landscape(A4)
_STRIP_WIDTH = 40 * mm


def _order(portal, order_id, tafel):
    fields = ", ".join("a." + name for name in _FIELDS)
    db = portal.get_db()
    try:
        row = db.execute(f"""SELECT {fields}, a.leistungsart, a.schaden_aufnahme_json,
            a.quelle, a.kunde_name, a.start_uhrzeit,
            a.angebotsphase, a.angebot_status, a.werkstatt_angebot_text, a.versicherung_id,
            a.versicherung_freigabe_status, a.schaden_eigenauftrag, h.name AS autohaus
            FROM auftraege a LEFT JOIN autohaeuser h ON h.id=a.autohaus_id WHERE a.id=?""",
                         (order_id,)).fetchone()
    finally:
        db.close()
    if row is None:
        return None
    order = dict(row)
    if tafel and (order["archiviert"] or order["angebotsphase"]
                  or not portal.werkstatt_tafel_auftrag_sichtbar(order)):
        return None
    return order


def _paragraph(text, style):
    return Paragraph("<br/>".join(escape(line) for line in str(text).splitlines()), style)


def _date_text(event):
    return " · ".join(filter(None, (event["datum"], event["uhrzeit"]))) or "Offen"


def _work_positions(sheet, order, tyre):
    scope = sheet["arbeit"]
    if order["quelle"] == "shop" and scope.startswith("Bestellte Artikel\n"):
        scope = "\n".join(line for line in scope.splitlines()[1:] if " · " in line)
    rows = checklist_rows(scope)
    if tyre and tyre["service_items"]:
        # Booking data is authoritative; keep other instructions below the checklist.
        rows = [row for item in tyre["service_items"] for row in checklist_rows(_work_text(item))]
    return rows


def _boxes(rows, width, style):
    entries = []
    for row in rows:
        mark = Drawing(14, 14)
        mark.add(Rect(1, 1, 11, 11, strokeWidth=1.2, fillColor=None))
        item = escape(row["text"])
        if row["new_part"]:
            item = f"<b>NEUTEIL · {item}</b>"
        entries.append([mark, Paragraph(item, style)])
    table = Table(entries, colWidths=[21, width-21], hAlign="LEFT", splitInRow=1)
    table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"),
                               ("LEFTPADDING", (0, 0), (-1, -1), 0),
                               ("TOPPADDING", (0, 0), (-1, -1), 5),
                               ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
    return table


def _draw_tearoff(canvas, width, height, number):
    cut_x = width - _STRIP_WIDTH
    canvas.saveState()
    canvas.setLineWidth(0.8)
    canvas.setDash(4, 3)
    canvas.line(cut_x, 6*mm, cut_x, height - 13*mm)
    canvas.setDash()
    for dx in (-3, 3):
        canvas.circle(cut_x + dx, height - 7*mm, 2.2)
        canvas.line(cut_x + dx, height - 7*mm - 2, cut_x - dx, height - 12*mm)
    tag = f"Auftrag {number}"
    font = "Helvetica-Bold"
    pts = min(54, (height - 24*mm) / canvas.stringWidth(tag, font, 1))
    asc, desc = getAscentDescent(font, pts)
    canvas.translate(cut_x + _STRIP_WIDTH / 2, height / 2)
    canvas.rotate(90)
    canvas.setFont(font, pts)
    canvas.drawCentredString(0, -(asc + desc) / 2, tag)
    canvas.restoreState()


def _cover(sheet, order, printed, omitted, tyre=None, finished=None):
    stream = BytesIO()
    size = _PAGE_SIZE
    doc = BaseDocTemplate(stream, pagesize=size, title=f"Auftrag {sheet['nummer']}",
                          author="Gärtner Karosserie & Lack", leftMargin=10*mm, rightMargin=_STRIP_WIDTH + 8*mm,
                          allowSplitting=True)
    ink, green = HexColor("#172f25"), HexColor("#37654f")
    heading = ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=24, leading=33, textColor=ink)
    label = ParagraphStyle("label", fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=green,
                           spaceBefore=8, spaceAfter=3, keepWithNext=True)
    checklist_label = ParagraphStyle("checklist", parent=label, spaceBefore=17)
    value = ParagraphStyle("value", fontName="Helvetica", fontSize=13, leading=17, textColor=ink)
    note = ParagraphStyle("note", parent=value, fontSize=11, leading=15)
    service = auftrag_leistungsart(order)
    if not service and any(re.search(r"lack(?:ier)?[ _-]*auftrag", name, re.I) for name in printed + omitted):
        service = "lackierung"
    title = "Lackierauftrag" if service == "lackierung" else "Werkstattauftrag"
    vehicle = f"{sheet['fahrzeug']} · {sheet['kennzeichen']}"
    story = []
    service_name = AUFTRAGSARTEN.get(service, {}).get("label", "Werkstattauftrag")
    start = _event("Arbeitsbeginn", order["start_datum"], order["start_uhrzeit"])
    end = sheet["fertig"]
    start_name, start_value = "Arbeitsbeginn", _date_text(start)
    end_name, end_value = "Planende", _date_text(end)
    if not end["datum"] and finished:
        end_name, end_value = "Fertig gemeldet", finished.strftime("%d.%m.%Y")
    if tyre:
        start_name, start_value = tyre["start_label"], tyre["start"]
        end_name, end_value = tyre["end_label"], tyre["end"]
        service_name = tyre["service"]

    def section(name, content, style=value):
        story.extend([_paragraph(name, label), _paragraph(content, style)])

    customer = order["autohaus"] or order["kunde_name"] or ""
    identification = [customer] if customer else []
    identification.append(vehicle)
    if sheet["externe_referenz"] and sheet["externe_referenz"] != str(sheet["nummer"]):
        identification.append("Auftragsreferenz " + sheet["externe_referenz"])
    # The rack exposes only the upper edge. Keep the two appointments beside
    # the order number, even when the service or vehicle description wraps.
    key_style = ParagraphStyle("field", fontName="Helvetica-Bold", fontSize=10, leading=12, textColor=green)
    service_style = ParagraphStyle("service", parent=value, fontName="Helvetica-Bold", fontSize=15, leading=19)
    time_style = ParagraphStyle("time", parent=service_style, leading=18)
    date_width = 63*mm
    rack = Table([[
        Paragraph(f"{title} <font size='32'>{sheet['nummer']}</font>", heading),
        [_paragraph(start_name, key_style), _paragraph(start_value, time_style)],
        [_paragraph(end_name, key_style), _paragraph(end_value, time_style)],
    ]], colWidths=[doc.width - 2*date_width, date_width, date_width])
    rack.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"),
                              ("LEFTPADDING", (0, 0), (-1, -1), 0),
                              ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                              ("TOPPADDING", (0, 0), (-1, -1), 0),
                              ("BOTTOMPADDING", (0, 0), (-1, -1), 0)]))
    top = [
        (rack, 5),
        (Paragraph(f"<font size='11' color='#37654f'>Service:</font>   {escape(service_name)}", service_style), 3),
        (_paragraph("   ·   ".join(identification), note), 5),
    ]
    header = [(item, item.wrap(doc.width, size[1])[1], gap) for item, gap in top]
    header_top = size[1] - 6*mm
    body_top = header_top - sum(height + gap for _, height, gap in header) - 4*mm
    if sheet["archiviert"] or order["angebotsphase"]:
        section("Status", "Archiviert" if sheet["archiviert"] else "Angebotsphase")
    dates = []
    arrival, _, departure = sheet["termine"]
    for event in (arrival, departure):
        if event["datum"]:
            dates.append(" ".join(filter(None, (event["label"], event["datum"], event["uhrzeit"]))))
    if dates:
        section("Fahrzeugübergabe", "   ·   ".join(dates))
    if sheet["lackdaten"]:
        section("Lackdaten", "   ·   ".join(f"{item['label']} {item['wert']}" for item in sheet["lackdaten"]))
    rows = _work_positions(sheet, order, tyre)
    if order["angebot_status"] == "angenommen":
        known = {r["text"].casefold() for r in rows}
        for row in checklist_rows(_work_text(order["werkstatt_angebot_text"])):
            if row["text"].casefold() not in known:
                rows.append(row)
                known.add(row["text"].casefold())
    story.append(_paragraph("Arbeiten abhaken", checklist_label))
    if not rows:
        story.append(_paragraph("Positionen aus der beigefügten Auftragsunterlage übernehmen." if printed
                                else "Arbeitsumfang vor Beginn klären und hier eintragen.", note))
        rows = [{"text": "_" * 56, "new_part": False} for _ in range(2)]
    story.append(_boxes(rows, doc.width, value))
    if tyre and tyre["service_items"] and sheet["arbeit"] and not all(
            item.casefold() in tyre["service"].casefold() for item in sheet["arbeit"].splitlines()):
        section("Angaben zum Auftrag", sheet["arbeit"], note)
    if printed:
        section("Beigefügte Auftragsunterlagen", "\n".join(printed), note)
    if omitted:
        section("Fehlende Druckseiten · Original im Auftrag prüfen", "\n".join(omitted), note)

    def chrome(canvas, document):
        width, height = size
        left, right = doc.leftMargin, width - doc.rightMargin
        y = header_top
        for item, item_height, gap in header:
            y -= item_height
            item.drawOn(canvas, left, y)
            y -= gap
        canvas.setStrokeColor(green)
        canvas.setFillColor(ink)
        canvas.line(left, y, right, y)
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(right, 6*mm, f"Gärtner Karosserie & Lack   ·   Auftrag {sheet['nummer']}   ·   Blatt {document.page}")
        if document.page == 1:
            # A fixed sign-off cannot spill onto a sheet of its own.
            baseline = 15*mm
            canvas.rect(left, baseline - 1, 11, 11)
            canvas.setFont("Helvetica-Bold", 11)
            canvas.drawString(left + 17, baseline, "Auftrag erledigt")
            canvas.setFont("Helvetica", 11)
            canvas.drawString(left + 38*mm, baseline, "·  Rückgabe ans Büro")
            for caption, offset in (("Datum", 112*mm), ("Kürzel", 170*mm)):
                x = left + offset
                canvas.drawString(x, baseline, caption)
                canvas.line(x + 15*mm, baseline - 1, x + 46*mm, baseline - 1)
            _draw_tearoff(canvas, width, height, sheet["nummer"])

    for name, bottom in (("Auftragsblatt", 23*mm), ("Fortsetzung", 13*mm)):
        frame = Frame(doc.leftMargin, bottom, doc.width, body_top - bottom, id=name,
                      leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
        doc.addPageTemplates(PageTemplate(id=name, frames=[frame], onPage=chrome,
                                          autoNextPageTemplate="Fortsetzung"))
    doc.build(story)
    return stream.getvalue()


def _image_pages(raw):
    """Fit each photo/scan to A4 while preserving orientation and aspect ratio."""
    with Image.open(BytesIO(raw)) as source, fitz.open() as pages:
        for frame in range(getattr(source, "n_frames", 1)):
            source.seek(frame)
            if source.width * source.height > Image.MAX_IMAGE_PIXELS:
                raise ValueError("Bild zu groß")
            image = ImageOps.exif_transpose(source)
            image.thumbnail((3300, 3300))
            rgba = image.convert("RGBA")
            white = Image.new("RGB", rgba.size, "white")
            white.paste(rgba, mask=rgba.getchannel("A"))
            jpeg = BytesIO()
            white.save(jpeg, "JPEG", quality=90)
            width, height = _PAGE_SIZE
            page = pages.new_page(width=width, height=height)
            page.insert_image(fitz.Rect(12, 12, width-12, height-12), stream=jpeg.getvalue())
        return pages.tobytes()


def _append_landscape_pages(attachments, original):
    """Place each original upright on landscape A4 without cropping."""
    width, height = _PAGE_SIZE
    # Normalize before copying: PyMuPDF caches source objects as soon as printing begins.
    for page in original:
        if page.rotation:
            page.remove_rotation()
    for index, page in enumerate(original):
        if abs(page.rect.width - width) < 1 and abs(page.rect.height - height) < 1:
            attachments.insert_pdf(original, from_page=index, to_page=index, links=False,
                                   annots=False, widgets=False, final=index == len(original) - 1)
        else:
            target = attachments.new_page(width=width, height=height)
            if page.get_contents():
                margin = 5*mm
                frame = fitz.Rect(margin, margin, width - margin, height - margin)
                target.show_pdf_page(frame, original, index, keep_proportion=True)


def druckpaket(portal, order_id, *, tafel=False):
    order = _order(portal, order_id, tafel)
    if order is None:
        return None
    sheet = _sheet_data(portal, order)
    tyre = (reifenetikett(portal, order_id, tafel=tafel, nur_arbeitsdaten=True)
            if auftrag_leistungsart(order) == "reifenservice" else None)
    finished = (portal.list_erste_fertigmeldungen([order_id]).get(order_id)
                if not sheet["fertig"]["datum"] and int(order["status"] or 1) >= 4 and not tyre else None)
    printed, omitted = [], []
    with fitz.open() as attachments:
        for source in druckquellen(portal, order_id, tafel=tafel):
            start = attachments.page_count
            try:
                raw = lade_druckquelle(portal, order_id, source["id"], tafel=tafel)
                if not raw:
                    raise ValueError("Original fehlt")
                if source["suffix"].lower() != ".pdf":
                    raw = _image_pages(raw)
                with fitz.open(stream=raw, filetype="pdf") as original:
                    if original.needs_pass or not original.page_count:
                        raise ValueError("PDF gesperrt oder leer")
                    original.bake()
                    _append_landscape_pages(attachments, original)
                printed.append(source["original_name"])
            except (OSError, ValueError, RuntimeError, fitz.mupdf.FzErrorBase, Image.DecompressionBombError):
                if attachments.page_count > start:
                    attachments.delete_pages(from_page=start, to_page=attachments.page_count-1)
                omitted.append(source["original_name"])
        with fitz.open(stream=_cover(sheet, order, printed, omitted, tyre, finished), filetype="pdf") as result:
            if attachments.page_count:
                result.insert_pdf(attachments)
            return BytesIO(result.tobytes(garbage=3, deflate=True))
