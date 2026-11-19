"""Printable workshop order followed by its original work instructions."""

from io import BytesIO
import re
from xml.sax.saxutils import escape

import fitz
from PIL import Image, ImageOps
from reportlab.lib.colors import HexColor
from reportlab.graphics.shapes import Drawing, Rect
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Table, TableStyle

from auftragsarten import AUFTRAGSARTEN, auftrag_leistungsart
from reifen_auftragsetikett import reifenetikett
from werkstatt_auftrag_ausdruck import _FIELDS, _event, _sheet_data, _work_text
from werkstatt_druck_checkliste import checklist_rows
from werkstatt_druckquellen import druckquellen, lade_druckquelle


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


def _cover(sheet, order, printed, omitted, tyre=None, finished=None):
    stream = BytesIO()
    size = A4
    doc = SimpleDocTemplate(stream, pagesize=size, title=f"Auftrag {sheet['nummer']}",
                            author="Gärtner Karosserie & Lack", leftMargin=12*mm, rightMargin=12*mm,
                            bottomMargin=32*mm, allowSplitting=True)
    ink, green = HexColor("#172f25"), HexColor("#37654f")
    heading = ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=25, leading=42, textColor=ink)
    label = ParagraphStyle("label", fontName="Helvetica-Bold", fontSize=11, leading=14, textColor=green,
                           spaceBefore=12, spaceAfter=4, keepWithNext=True)
    checklist_label = ParagraphStyle("checklist", parent=label, spaceBefore=22)
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
    # Lay out the same stack on each worksheet page. Its height follows the
    # complete service name, including a booking with several services.
    key_style = ParagraphStyle("field", parent=label, fontSize=12, leading=20)
    service_style = ParagraphStyle("service", parent=value, fontName="Helvetica-Bold", fontSize=16, leading=21)
    time_style = ParagraphStyle("time", parent=service_style, fontSize=17, leading=23)
    fields = Table([
        [_paragraph("Service:", key_style), _paragraph(service_name, service_style)],
        [_paragraph(start_name, key_style), _paragraph(start_value, time_style)],
        [_paragraph(end_name, key_style), _paragraph(end_value, time_style)],
    ], colWidths=[46*mm, doc.width - 12 - 46*mm], hAlign="LEFT")
    fields.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                                ("TOPPADDING", (0, 0), (-1, -1), 3),
                                ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
    top = [
        (Paragraph(f"{title} <font size='36'>{sheet['nummer']}</font>", heading), 5),
        (fields, 8),
        (_paragraph("   ·   ".join(identification), note), 7),
    ]
    header = [(item, item.wrap(doc.width - 12, size[1])[1], gap) for item, gap in top]
    doc.topMargin = 12*mm + sum(height + gap for _, height, gap in header)
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
    story.append(_boxes(rows, doc.width - 12, value))
    if tyre and tyre["service_items"] and sheet["arbeit"] and not all(
            item.casefold() in tyre["service"].casefold() for item in sheet["arbeit"].splitlines()):
        section("Angaben zum Auftrag", sheet["arbeit"], note)
    if printed:
        section("Beigefügte Auftragsunterlagen", "\n".join(printed), note)
    if omitted:
        section("Fehlende Druckseiten · Original im Auftrag prüfen", "\n".join(omitted), note)
    story.append(_paragraph("Abschluss und Rückgabe ans Büro", label))
    story.append(_boxes([{"text": "Auftrag erledigt   ·   Datum __________________   ·   Kürzel __________________",
                          "new_part": False}], doc.width - 12, value))

    def chrome(canvas, document):
        width, height = size
        y = height - 8*mm
        for item, item_height, gap in header:
            y -= item_height
            item.drawOn(canvas, doc.leftMargin + 6, y)
            y -= gap
        canvas.setStrokeColor(green)
        canvas.line(doc.leftMargin + 6, y, width-doc.rightMargin - 6, y)
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(width-14*mm, 30*mm, f"Gärtner Karosserie & Lack   ·   Auftrag {sheet['nummer']}   ·   Blatt {document.page}")
        if document.page == 1:
            cut = 26*mm
            canvas.setLineWidth(0.8)
            canvas.setDash(4, 3)
            canvas.line(14*mm, cut, width-7*mm, cut)
            canvas.setDash()
            for dy in (-3, 3):
                canvas.circle(8*mm, cut+dy, 2.2)
                canvas.line(8*mm+2, cut+dy, 13*mm, cut-dy)
            canvas.setFont("Helvetica-Bold", 29)
            canvas.drawString(14*mm, 8*mm, f"Auftrag {sheet['nummer']}")

    doc.build(story, onFirstPage=chrome, onLaterPages=chrome)
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
            width, height = A4
            page = pages.new_page(width=width, height=height)
            page.insert_image(fitz.Rect(12, 12, width-12, height-12), stream=jpeg.getvalue())
        return pages.tobytes()


def _append_portrait_pages(attachments, original):
    """Keep A4 originals at full size; fit other paper sizes onto upright A4."""
    for index, page in enumerate(original):
        if abs(page.rect.width - A4[0]) < 1 and abs(page.rect.height - A4[1]) < 1:
            attachments.insert_pdf(original, from_page=index, to_page=index, links=False,
                                   annots=False, widgets=False, final=index == len(original) - 1)
        else:
            target = attachments.new_page(width=A4[0], height=A4[1])
            if page.get_contents():
                page.remove_rotation()
                margin = 5*mm
                frame = fitz.Rect(margin, margin, A4[0] - margin, A4[1] - margin)
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
                    _append_portrait_pages(attachments, original)
                printed.append(source["original_name"])
            except (OSError, ValueError, RuntimeError, fitz.mupdf.FzErrorBase, Image.DecompressionBombError):
                if attachments.page_count > start:
                    attachments.delete_pages(from_page=start, to_page=attachments.page_count-1)
                omitted.append(source["original_name"])
        with fitz.open(stream=_cover(sheet, order, printed, omitted, tyre, finished), filetype="pdf") as result:
            if attachments.page_count:
                result.insert_pdf(attachments)
            return BytesIO(result.tobytes(garbage=3, deflate=True))
