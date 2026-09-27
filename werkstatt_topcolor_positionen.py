"""Offline positional parser for the known German Topcolor invoice table.

Input is PyMuPDF word data, not OCR prose or instructions. Requires both a known
supplier name and the expected headers/column ordering. It reads product rows
only, not account, payment or document totals. All results remain proposals.
"""

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re


_SUPPLIER = re.compile(r"(?<![a-z])(?:top[ -]?colou?r|auto[ -]?color)(?![a-z])", re.I)
_FEE_SKUS = {"00000071"}
_HEADERS = ("Pos.", "Art.-Nr.", "Bezeichnung", "Menge", "Inhalt", "Preis", "Gesamt")


def _lines(words):
    rows = []
    for word in sorted(words, key=lambda w: (w[1], w[0])):
        if len(word) < 5 or not isinstance(word[4], str):
            continue
        if rows and abs(rows[-1][0] - word[1]) <= 2:
            rows[-1][1].append(word)
        else:
            rows.append([word[1], [word]])
    return [(y, sorted(row, key=lambda w: w[0])) for y, row in rows]


def _layout(rows):
    for y, words in rows:
        by_name = {word[4]: word for word in words}
        if not all(header in by_name for header in _HEADERS):
            continue
        x = [by_name[header][0] for header in _HEADERS]
        units = [word[0] for word in words if word[4] == "ME" and x[4] < word[0] < x[5]]
        if x != sorted(x) or not units:
            continue
        return {"y": y, "position": x[0], "article": x[1], "name": x[2],
                "quantity": x[3], "content": x[4], "measure": units[0],
                "price": x[5], "total": x[6]}
    return None


def _decimal(raw, allow_negative=False):
    raw = raw.strip().replace("%", "")
    pattern = r"-?\d+(?:\.\d{3})*,\d{1,3}|-?\d+" if allow_negative else r"\d+(?:\.\d{3})*,\d{1,3}|\d+"
    if not re.fullmatch(pattern, raw):
        return None
    try:
        return Decimal(raw.replace(".", "").replace(",", "."))
    except InvalidOperation:
        return None


def _plain(value):
    if value is None:
        return ""
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _cell(words, left, right):
    return " ".join(word[4] for word in words if left <= word[0] < right).strip()


def _packaging(name, content, measure):
    if measure == "Ltr/KG":
        matches = re.findall(r"\b(\d+(?:[.,]\d+)?)\s*(Liter|Ltr|ml|kg|l)\b", name, re.I)
        if matches:
            size, unit = matches[-1]
            unit = {"liter": "L", "ltr": "L", "l": "L", "ml": "ml", "kg": "kg"}[unit.lower()]
            return size.replace(",", ".") + " " + unit, "Gebinde"
        return _plain(content) + " Ltr/KG" if content else "", "Gebinde"
    counts = re.findall(r"\b(\d+)\s*/\s*(Pack|KP|ROL)\b", name, re.I)
    if counts:
        count, unit = counts[-1]
        return count + " Stück/" + ("Rolle" if unit.upper() == "ROL" else "Pack"), measure
    counts = re.findall(r"\b(?:VE\s*=\s*)?(\d+)\s*(?:Stück|Stueck)\b", name, re.I)
    if counts and measure == "Pack":
        return counts[-1] + " Stück/Pack", measure
    return (_plain(content) + " " + measure).strip() if content else "", measure


def _candidate(row):
    name = " ".join(row["description"]).strip()
    values = row["values"]
    quantity, content = _decimal(values["quantity"]), _decimal(values["content"])
    base, total = _decimal(values["base"]), _decimal(values["total"])
    discount = _decimal(values["discount"], True) if values["discount"] else Decimal(0)
    problems = []
    if quantity is None or quantity <= 0:
        problems.append("Bestellte Rechnungsmenge unklar; keine Bestellmenge daraus ableiten.")
        quantity = None
    if content is None or content <= 0:
        problems.append("Gebindeinhalt unklar.")
        content = None
    if values["measure"] not in {"Ltr/KG", "Stück", "Stueck", "Pack"}:
        problems.append("Maßeinheit unklar.")
    if discount is None or not Decimal(-100) <= discount <= 0:
        problems.append("Rabatt unklar.")
        discount = None
    price = ""
    derived = None
    if all(value is not None for value in (quantity, content, base, total, discount)):
        derived = content * base * (1 + discount / 100)
        expected = (quantity * derived).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if expected != total:
            problems.append("Positionssumme passt nicht zu Menge, Inhalt, Grundpreis und Rabatt.")
        elif not problems:
            price = format(derived.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")
    else:
        problems.append("Preisberechnung unvollständig; kein Gebindepreis übernommen.")
    packaging, unit = _packaging(name, content, values["measure"])
    if values["measure"] == "Ltr/KG":
        explicit_size = re.fullmatch(r"(\d+(?:\.\d+)?) (L|ml|kg)", packaging)
        if explicit_size is None:
            problems.append("Liter-/Kilogramm-Gebinde in der Produktbezeichnung nicht eindeutig.")
            price = ""
        elif content is not None:
            described = Decimal(explicit_size[1]) / (1000 if explicit_size[2] == "ml" else 1)
            if described != content:
                problems.append("Gebindeangabe der Beschreibung widerspricht der Inhaltsspalte.")
                price = ""
    dimensions = re.findall(r"(?:Ø\s*)?\d+(?:[.,]\d+)?\s*(?:[x×]\s*\d+(?:[.,]\d+)?\s*)?(?:mm|mtr|µm)\b", name, re.I)
    grits = re.findall(r"\bP\d{2,4}\b", name)
    return {
        "produkt_name": name, "artikelnummer": row["article"],
        "produkt_beschreibung": name, "stueckzahl": _plain(quantity) or None,
        "ve": unit, "gebinde": packaging, "groesse": " / ".join(dimensions + grits),
        "preis": price, "kategorie": "Material", "verified": False,
        "price_verified": False, "preis_geprueft": False, "pruefen": True,
        "auslese_hinweise": problems,
        "price_evidence": {
            "value": price, "basis": "gebindepreis_netto_abgeleitet" if price else "unknown",
            "verified": False, "reconciled": bool(price),
            "unrounded_value": _plain(derived) if price else "",
            "quantity": _plain(quantity), "content": _plain(content),
            "measure_unit": values["measure"], "base_price_per_measure": _plain(base),
            "discount_percent": _plain(discount), "line_total_net": _plain(total),
            "calculation": "Inhalt × Grundpreis je Maßeinheit × (1 + Rabatt / 100)",
        },
        "native_source": {"page": row["pages"][0], "pages": row["pages"],
                          "position": row["position"], "method": "topcolor_word_columns_v1"},
    }


def parse_topcolor_pages(pages, supplier):
    """Return positions/fees/coverage, or None for unrecognized supplier/layout.

    pages is [{page: one-based integer, height: number, words: fitz words}, ...].
    Description-only continuations can cross a page boundary, preserving both
    page references. Fees are accounted for without becoming catalog products.
    """
    if not isinstance(supplier, str) or not _SUPPLIER.search(supplier):
        return None
    prepared = [(page, _lines(page["words"])) for page in pages]
    layouts = [_layout(rows) for _, rows in prepared]
    if not any(layouts):
        return None
    warnings, records, fees, positions_seen, page_complete = [], [], [], [], {}
    current = None
    for (page, rows), layout in zip(prepared, layouts):
        page_number = page["page"]
        page_complete[page_number] = bool(layout)
        if not layout:
            warnings.append(f"Seite {page_number}: bekanntes Artikeltabellenlayout fehlt; prüfen.")
            current = None
            continue
        for y, words in rows:
            if y <= layout["y"] + 3:
                continue
            if y > page["height"] * 0.85:
                lower_article = _cell(words, layout["article"] - 3, layout["name"] - 3)
                if re.fullmatch(r"\d{8}", lower_article):
                    warnings.append(f"Seite {page_number}: mögliche weitere Artikelzeile außerhalb des erkannten Tabellenbereichs.")
                    page_complete[page_number] = False
                continue
            joined = " ".join(word[4] for word in words)
            # Nothing after these table terminators belongs to product rows.
            if re.search(r"\b(?:ACHTUNG|Gesamtbetrag|Zahlungsbedingungen|Zahlbar|Bankverbindung|Nettobetrag|Mehrwertsteuer|MwSt)\b", joined, re.I):
                current = None
                break
            position_raw = _cell(words, layout["position"] - 3, layout["article"] - 3)
            article = _cell(words, layout["article"] - 3, layout["name"] - 3)
            description = _cell(words, layout["name"] - 2, layout["quantity"] - 4)
            if re.fullmatch(r"\d+\.", position_raw) and re.fullmatch(r"\d{8}", article):
                position = int(position_raw[:-1])
                positions_seen.append(position)
                numeric_words = [word for word in words if word[0] >= layout["price"] - 3]
                base = " ".join(word[4] for word in numeric_words if word[0] < layout["total"] - 3 and "%" not in word[4])
                discount = " ".join(word[4] for word in numeric_words if "%" in word[4])
                total = " ".join(word[4] for word in numeric_words if word[0] >= layout["total"] - 3 and re.fullmatch(r"[\d.,]+", word[4]))
                current = {"position": position, "article": article, "description": [description],
                           "pages": [page_number], "values": {
                    "quantity": _cell(words, layout["quantity"] - 4, layout["content"] - 3),
                    "content": _cell(words, layout["content"] - 3, layout["measure"] - 3),
                    "measure": _cell(words, layout["measure"] - 3, layout["price"] - 3),
                    "base": base, "discount": discount, "total": total,
                }}
                if article in _FEE_SKUS or re.search(r"\b(?:Logistik|Energiekostenpauschale)\b", description, re.I):
                    fees.append({"position": position, "page": page_number, "article_number": article,
                                 "reason": "Logistik-/Energiekostenpauschale, kein Katalogprodukt"})
                    current = None
                else:
                    records.append(current)
                continue
            if re.fullmatch(r"\d+\.", position_raw) or re.fullmatch(r"\d{8}", article):
                warnings.append(f"Seite {page_number}: Artikelposition mit unklarer Artikelnummer.")
                page_complete[page_number] = False
                current = None
                continue
            # Only continuation words entirely within the description column.
            # AUFTRAG/LIEFERSCHEIN are outside that column and cannot contaminate it.
            if current and description and all(layout["name"] - 2 <= word[0] < layout["quantity"] - 4 for word in words):
                current["description"].append(description)
                if page_number not in current["pages"]:
                    current["pages"].append(page_number)
    if positions_seen != list(range(1, len(positions_seen) + 1)):
        warnings.append("Positionsnummern sind unvollständig oder doppelt; Rechnung prüfen.")
        page_complete = {page: False for page in page_complete}
    candidates = []
    for row in records:
        if not " ".join(row["description"]).strip():
            warnings.append(f"Position {row['position']}: Produktbezeichnung fehlt.")
            page_complete[row["pages"][0]] = False
            continue
        item = _candidate(row)
        if item["auslese_hinweise"]:
            page_complete[row["pages"][0]] = False
        candidates.append(item)
    if not candidates:
        warnings.append("Keine Produktpositionen im erkannten Tabellenlayout; Rechnung prüfen.")
        page_complete = {page: False for page in page_complete}
    if fees:
        warnings.append(f"{len(fees)} Gebührenposition(en) wurden vom Artikelstamm ausgeschlossen.")
    return {"positions": candidates, "fees": fees, "warnings": warnings,
            "page_complete": page_complete, "complete": bool(candidates) and all(page_complete.values())}
